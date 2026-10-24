//! Storage thread: the only pack writer and the only sender of catalog ops.
//!
//! Ops are buffered and forwarded to the catalog writer only at commit
//! barriers, right after the open pack is fsynced. So the catalog never holds
//! (committed or pending) a unit row whose pack bytes are not durable.
//!
//! Foreign-key order is kept here: a pack row precedes its units, units
//! precede their content, and a file entry whose content is still in flight
//! (claimed by another worker) is parked until that content is inserted.

use super::{Halt, Shared, Stage};
use crate::catalog::writer::CatalogOp;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::{ContentRow, EntryKind, EntryRow, PackRow, UnitRow};
use crate::pack::{PackWriter, RecordHeader, RECORD_HEADER_LEN};
use crate::repo::Repo;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

/// Commit barrier: after this many buffered ops, or this much time.
const BARRIER_OPS: usize = 10_000;
const BARRIER_INTERVAL: Duration = Duration::from_secs(1);

pub(super) enum StorageMsg {
    /// An entry row. `content_known` is set when the worker found the
    /// entry's content already committed in the catalog.
    Entry { row: EntryRow, content_known: bool },
    /// A content new to the repository, with its units in `seq` order.
    NewContent {
        content: ContentRow,
        units: Vec<(RecordHeader, Vec<u8>)>,
        media: Option<(u32, u32)>,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct StorageStats {
    pub entries: u64,
    pub files: u64,
    pub file_bytes: u64,
    pub new_units: u64,
    pub new_bytes_stored: u64,
}

struct OpenPack {
    id: Id,
    writer: PackWriter,
    /// Appended to since the last sync.
    dirty: bool,
}

pub(super) struct Storage {
    packs_dir: PathBuf,
    max_size: u64,
    /// Read-only view of committed units from earlier runs.
    cat: Catalog,
    out: SyncSender<CatalogOp>,
    pack: Option<OpenPack>,
    /// Units appended during this run.
    written_units: HashSet<Hash32>,
    /// Contents inserted during this run.
    inserted_contents: HashSet<Hash32>,
    /// File entries waiting for their content's `NewContent`.
    pending: HashMap<Hash32, Vec<EntryRow>>,
    /// Ops not yet forwarded; flushed at the next barrier.
    buf: Vec<CatalogOp>,
    stats: StorageStats,
}

impl Storage {
    pub fn open(repo: &Repo, out: SyncSender<CatalogOp>) -> Result<Storage> {
        Ok(Storage {
            packs_dir: repo.packs_dir(),
            max_size: repo.config.packs.max_size,
            cat: Catalog::open_readonly(&repo.catalog_path())?,
            out,
            pack: None,
            written_units: HashSet::new(),
            inserted_contents: HashSet::new(),
            pending: HashMap::new(),
            buf: Vec::new(),
            stats: StorageStats::default(),
        })
    }

    /// Consume messages until every sender has gone, then seal and commit.
    pub fn run(mut self, rx: &Receiver<StorageMsg>, shared: &Shared) -> Stage<StorageStats> {
        let mut last_barrier = Instant::now();
        loop {
            if shared.aborted() {
                return Err(Halt::Aborted);
            }
            let wait = BARRIER_INTERVAL.saturating_sub(last_barrier.elapsed());
            match rx.recv_timeout(wait) {
                Ok(msg) => self.handle(msg)?,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            if self.buf.len() >= BARRIER_OPS || last_barrier.elapsed() >= BARRIER_INTERVAL {
                self.barrier()?;
                last_barrier = Instant::now();
            }
        }
        // Senders also disconnect when they stop on an abort.
        if shared.aborted() {
            return Err(Halt::Aborted);
        }
        self.finish()
    }

    fn handle(&mut self, msg: StorageMsg) -> Result<()> {
        match msg {
            StorageMsg::Entry { row, content_known } => {
                match row.content_hash {
                    Some(h) if !content_known && !self.inserted_contents.contains(&h) => {
                        self.pending.entry(h).or_default().push(row);
                    }
                    _ => self.push_entry(row),
                }
                Ok(())
            }
            StorageMsg::NewContent {
                content,
                units,
                media,
            } => self.new_content(content, units, media),
        }
    }

    fn push_entry(&mut self, row: EntryRow) {
        self.stats.entries += 1;
        if row.kind == EntryKind::File {
            self.stats.files += 1;
            self.stats.file_bytes += row.size.and_then(|s| u64::try_from(s).ok()).unwrap_or(0);
        }
        self.buf.push(CatalogOp::InsertEntry(row));
    }

    fn new_content(
        &mut self,
        content: ContentRow,
        units: Vec<(RecordHeader, Vec<u8>)>,
        media: Option<(u32, u32)>,
    ) -> Result<()> {
        let mut hashes = Vec::with_capacity(units.len());
        for (header, payload) in units {
            hashes.push(header.unit_hash);
            if !self.written_units.contains(&header.unit_hash)
                && !self.cat.has_unit(&header.unit_hash)?
            {
                self.append(&header, &payload)?;
            }
        }
        let hash = content.hash;
        self.buf.push(CatalogOp::InsertContent(content, hashes));
        if let Some((width, height)) = media {
            self.buf.push(CatalogOp::InsertMedia {
                content: hash,
                width,
                height,
            });
        }
        self.inserted_contents.insert(hash);
        for row in self.pending.remove(&hash).unwrap_or_default() {
            self.push_entry(row);
        }
        Ok(())
    }

    fn append(&mut self, header: &RecordHeader, payload: &[u8]) -> Result<()> {
        let record_len = RECORD_HEADER_LEN as u64 + header.stored_len;
        if let Some(p) = &self.pack {
            if p.writer.len() + record_len > self.max_size {
                self.seal()?;
            }
        }
        let mut pack = match self.pack.take() {
            Some(p) => p,
            None => self.create_pack()?,
        };
        let offset = pack.writer.append(header, payload);
        let id = pack.id;
        pack.dirty = true;
        self.pack = Some(pack);
        let offset = offset?;
        self.buf.push(CatalogOp::InsertUnit(UnitRow {
            hash: header.unit_hash,
            kind: header.kind,
            raw_size: super::to_i64(header.raw_len, "unit raw size")?,
            stored_size: super::to_i64(header.stored_len, "unit stored size")?,
            compression: header.compression,
            pack_id: id,
            pack_offset: super::to_i64(offset, "pack offset")?,
        }));
        self.written_units.insert(header.unit_hash);
        self.stats.new_units += 1;
        self.stats.new_bytes_stored += header.stored_len;
        Ok(())
    }

    /// Packs are created lazily, so none is ever left empty.
    fn create_pack(&mut self) -> Result<OpenPack> {
        let id = Id::new_v7();
        let writer = PackWriter::create(&self.packs_dir, id)?;
        self.buf.push(CatalogOp::UpsertPack(PackRow {
            id,
            size: super::to_i64(writer.len(), "pack size")?,
            sealed: false,
            blake3: None,
        }));
        Ok(OpenPack {
            id,
            writer,
            dirty: false,
        })
    }

    /// Seal the open pack (which fsyncs it), if there is one.
    fn seal(&mut self) -> Result<()> {
        if let Some(p) = self.pack.take() {
            let sealed = p.writer.seal()?;
            self.buf.push(CatalogOp::UpsertPack(PackRow {
                id: sealed.id,
                size: super::to_i64(sealed.size, "pack size")?,
                sealed: true,
                blake3: Some(sealed.blake3),
            }));
        }
        Ok(())
    }

    /// fsync the open pack, record its length, then forward every buffered
    /// op and commit. Packs sealed since the last barrier were fsynced by
    /// `seal`.
    fn barrier(&mut self) -> Stage<()> {
        if let Some(p) = &mut self.pack {
            if p.dirty {
                p.writer.sync()?;
                p.dirty = false;
                let size = super::to_i64(p.writer.len(), "pack size")?;
                self.buf.push(CatalogOp::UpsertPack(PackRow {
                    id: p.id,
                    size,
                    sealed: false,
                    blake3: None,
                }));
            }
        }
        if self.buf.is_empty() {
            return Ok(());
        }
        for op in self.buf.drain(..).chain([CatalogOp::Commit]) {
            self.out.send(op).map_err(|_| Halt::Aborted)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Stage<StorageStats> {
        if !self.pending.is_empty() {
            let n: usize = self.pending.values().map(Vec::len).sum();
            return Err(Error::Corrupt(format!(
                "{n} entries reference content that was never stored"
            ))
            .into());
        }
        self.seal()?;
        self.barrier()?;
        Ok(self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::Compression;
    use crate::model::EntryStatus;
    use crate::pack::UnitKind;
    use crate::sniff::{Format, ProfileKind};
    use std::sync::mpsc::{sync_channel, Receiver};

    fn file_entry(content: Hash32) -> EntryRow {
        EntryRow {
            id: Id::new_v7(),
            snapshot_id: Id::new_v7(),
            parent_id: None,
            name: b"f".to_vec(),
            path: b"f".to_vec(),
            kind: EntryKind::File,
            mode: 0o644,
            mtime_ns: 0,
            size: Some(3),
            content_hash: Some(content),
            link_target: None,
            hardlink_group: None,
            status: EntryStatus::Ok,
        }
    }

    fn new_content(data: &[u8]) -> StorageMsg {
        let hash = Hash32::of(data);
        StorageMsg::NewContent {
            content: ContentRow {
                hash,
                size: data.len() as i64,
                format: Some(Format::Other),
                profile: ProfileKind::Binary,
                unit_count: 1,
            },
            units: vec![(
                RecordHeader {
                    kind: UnitKind::RawChunk,
                    compression: Compression::None,
                    unit_hash: hash,
                    raw_len: data.len() as u64,
                    stored_len: data.len() as u64,
                },
                data.to_vec(),
            )],
            media: None,
        }
    }

    fn storage() -> (tempfile::TempDir, Storage, Receiver<CatalogOp>) {
        let tmp = tempfile::tempdir().unwrap();
        let repo = Repo::init(&tmp.path().join("r")).unwrap();
        let (tx, rx) = sync_channel(1024);
        (tmp, Storage::open(&repo, tx).unwrap(), rx)
    }

    fn kind(op: &CatalogOp) -> &'static str {
        match op {
            CatalogOp::UpsertPack(p) if p.sealed => "seal",
            CatalogOp::UpsertPack(_) => "pack",
            CatalogOp::InsertUnit(_) => "unit",
            CatalogOp::InsertContent(..) => "content",
            CatalogOp::InsertEntry(_) => "entry",
            CatalogOp::Commit => "commit",
            _ => "other",
        }
    }

    #[test]
    fn parked_entry_follows_its_content() {
        let (_tmp, mut st, rx) = storage();
        let h = Hash32::of(b"abc");
        // The entry overtakes the content claimed by another worker.
        st.handle(StorageMsg::Entry {
            row: file_entry(h),
            content_known: false,
        })
        .unwrap();
        assert_eq!(st.pending.len(), 1);
        st.handle(new_content(b"abc")).unwrap();
        assert!(st.pending.is_empty());
        // A later entry for the same content goes straight through.
        st.handle(StorageMsg::Entry {
            row: file_entry(h),
            content_known: false,
        })
        .unwrap();
        let stats = match st.finish() {
            Ok(s) => s,
            Err(_) => panic!("finish failed"),
        };
        assert_eq!((stats.entries, stats.new_units), (2, 1));
        let ops: Vec<&str> = rx.try_iter().map(|op| kind(&op)).collect();
        assert_eq!(
            ops,
            ["pack", "unit", "content", "entry", "entry", "seal", "commit"]
        );
    }

    #[test]
    fn content_that_never_arrives_is_corrupt() {
        let (_tmp, mut st, _rx) = storage();
        st.handle(StorageMsg::Entry {
            row: file_entry(Hash32::of(b"missing")),
            content_known: false,
        })
        .unwrap();
        assert!(matches!(st.finish(), Err(Halt::Failed(Error::Corrupt(_)))));
    }
}
