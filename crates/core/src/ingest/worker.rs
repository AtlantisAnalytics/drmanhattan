//! Reader pool: read, sniff, chunk and hash each file; compress and emit the
//! units of contents that are new to the repository.
//!
//! Stability (spec §7 step 4): `(size, mtime)` from the walk, from an fstat
//! before the read and from an fstat after it must all agree. Otherwise the
//! file is re-read once from offset 0 between a fresh pair of fstats. If
//! those disagree too, the ingest fails with `Error::Unstable`, or, with
//! `allow_unstable`, the retry's bytes are stored with `status = unstable`.
//! Either way the entry's size is the number of bytes read and its mtime is
//! the final fstat's. Open and read errors are never retried.

use super::screen::{Screener, Verdict};
use super::storage::StorageMsg;
use super::{entry_row, to_i64, Halt, Shared, Stage};
use crate::catalog::Catalog;
use crate::chunk::{chunk_reader, FileDigest};
use crate::compress::encode;
use crate::config::Config;
use crate::display::display_path;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::{ContentRow, EntryStatus};
use crate::pack::{RecordHeader, UnitKind};
use crate::sniff::{detect, profile_kind, Format, ProfileKind};
use crate::walk::{mtime_ns, open_walked_file, WalkEntry};
use crate::xattrs::read_file_xattrs;
use crossbeam_channel::{Receiver, Sender};
use std::borrow::Cow;
use std::collections::HashSet;
use std::fs::{File, Metadata};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Bytes read up front for format sniffing and image dimensions.
const SNIFF_LEN: u64 = 8192;

pub(super) struct FileJob {
    pub id: Id,
    pub parent: Option<Id>,
    pub abs: PathBuf,
    pub entry: WalkEntry,
    /// Set by the walker for files with `nlink > 1`.
    pub hardlink_group: Option<i64>,
}

/// `(size, mtime_ns)` as seen by one stat.
type Stamp = (u64, i64);

/// One read of a whole file from offset 0.
struct ReadPass {
    head: Vec<u8>,
    format: Format,
    profile: ProfileKind,
    chunks: Vec<(Hash32, Vec<u8>)>,
    digest: FileDigest,
}

/// State shared by all workers.
pub(super) struct Worker<'a> {
    pub snapshot: Id,
    pub config: &'a Config,
    pub catalog_path: PathBuf,
    /// Contents some worker has claimed for emission during this run.
    pub claimed: &'a Mutex<HashSet<Hash32>>,
    pub screener: &'a dyn Screener,
    pub shared: &'a Shared,
    pub allow_unstable: bool,
}

impl Worker<'_> {
    pub fn run(&self, jobs: &Receiver<FileJob>, out: &Sender<StorageMsg>) -> Stage<()> {
        let cat = Catalog::open_readonly(&self.catalog_path)?;
        for job in jobs {
            if self.shared.aborted() {
                return Err(Halt::Aborted);
            }
            let (content, entry) = self.process(&cat, job)?;
            for msg in content.into_iter().chain([entry]) {
                out.send(msg).map_err(|_| Halt::Aborted)?;
            }
        }
        Ok(())
    }

    /// Returns the `NewContent` message (if this worker claimed the content)
    /// and the entry message, in the order they must be sent.
    fn process(&self, cat: &Catalog, job: FileJob) -> Result<(Option<StorageMsg>, StorageMsg)> {
        let FileJob {
            id,
            parent,
            abs,
            entry,
            hardlink_group,
        } = job;
        fail::fail_point!("ingest::after_walk");
        let (mut file, meta) = open_walked_file(&abs, &entry)?;
        let walked: Stamp = (entry.size, entry.mtime_ns);
        let before = stamp(&meta, &abs)?;
        let pass = self.read_pass(&mut file, before.0, &entry.name, &abs)?;
        fail::fail_point!("ingest::after_read");
        let after = fstamp(&file, &abs)?;
        let (pass, after, status) = if walked == before && before == after {
            (pass, after, EntryStatus::Ok)
        } else {
            // Free the first read's buffers before reading the file again.
            drop(pass);
            let before = fstamp(&file, &abs)?;
            let pass = self.read_pass(&mut file, before.0, &entry.name, &abs)?;
            fail::fail_point!("ingest::after_read");
            let after = fstamp(&file, &abs)?;
            if before == after {
                (pass, after, EntryStatus::Ok)
            } else if self.allow_unstable {
                (pass, after, EntryStatus::Unstable)
            } else {
                return Err(Error::Unstable(abs));
            }
        };
        let xattrs = read_file_xattrs(&file, &abs)?;
        let ReadPass {
            head,
            format,
            profile: profile_kind,
            chunks,
            digest,
        } = pass;

        let content_known = cat.has_content(&digest.hash)?;
        let new_content = if !content_known && self.claim(digest.hash) {
            match self
                .screener
                .screen(&entry.path, &digest.hash, digest.size, format)
            {
                Verdict::Pass => {}
                // Withholding arrives with real screeners (Plan 3); until
                // then any other verdict fails the ingest (fail closed).
                v => {
                    return Err(Error::Corrupt(format!(
                        "screener returned {v:?} for {}; withholding is not supported yet",
                        display_path(&abs)
                    )))
                }
            }
            let content = ContentRow {
                hash: digest.hash,
                size: to_i64(digest.size, "file size")?,
                format: Some(format),
                profile: profile_kind,
                unit_count: to_i64(chunks.len() as u64, "unit count")?,
            };
            let level = self.config.compression.level;
            let units = chunks
                .into_iter()
                .map(|(hash, raw)| compress_unit(hash, raw, format.is_precompressed(), level))
                .collect::<Result<Vec<_>>>()?;
            Some(StorageMsg::NewContent {
                content,
                units,
                media: image_dims(format, &head),
            })
        } else {
            None
        };
        let size = to_i64(digest.size, "file size")?;
        let mut row = entry_row(
            self.snapshot,
            id,
            parent,
            entry,
            Some(size),
            Some(digest.hash),
        );
        row.mtime_ns = after.1;
        row.hardlink_group = hardlink_group;
        row.status = status;
        let msg = StorageMsg::Entry {
            row,
            content_known,
            xattrs,
        };
        Ok((new_content, msg))
    }

    /// Read the whole file from offset 0: sniff its head, then chunk it with
    /// the profile its format and `size` (from the preceding fstat) select.
    fn read_pass(&self, file: &mut File, size: u64, name: &[u8], abs: &Path) -> Result<ReadPass> {
        let io = |e| Error::io(abs, e);
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        let mut head = Vec::with_capacity(SNIFF_LEN as usize);
        (&mut *file)
            .take(SNIFF_LEN)
            .read_to_end(&mut head)
            .map_err(io)?;
        let format = detect(name, &head);
        let profile = profile_kind(format, size);
        let cdc = match profile {
            ProfileKind::Text => &self.config.chunking.text,
            ProfileKind::Binary => &self.config.chunking.binary,
        };
        // Plan 1 buffers whole files; Plan 3 adds a two-pass read for large ones.
        let mut chunks: Vec<(Hash32, Vec<u8>)> = Vec::new();
        let digest = chunk_reader(cdc, Cursor::new(&head[..]).chain(&mut *file), |c, data| {
            chunks.push((c.hash, data.to_vec()));
            Ok(())
        })
        .map_err(|e| with_path(e, abs))?;
        Ok(ReadPass {
            head,
            format,
            profile,
            chunks,
            digest,
        })
    }

    /// True if this call claimed `h` for emission (nobody had before).
    fn claim(&self, h: Hash32) -> bool {
        self.claimed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(h)
    }
}

fn stamp(meta: &Metadata, abs: &Path) -> Result<Stamp> {
    Ok((meta.len(), mtime_ns(meta, abs)?))
}

fn fstamp(file: &File, abs: &Path) -> Result<Stamp> {
    let meta = file.metadata().map_err(|e| Error::io(abs, e))?;
    stamp(&meta, abs)
}

/// `chunk_reader` reports read errors with an empty path.
fn with_path(e: Error, path: &Path) -> Error {
    match e {
        Error::Io { path: p, source } if p.as_os_str().is_empty() => Error::io(path, source),
        e => e,
    }
}

fn compress_unit(
    hash: Hash32,
    raw: Vec<u8>,
    precompressed: bool,
    level: i32,
) -> Result<(RecordHeader, Vec<u8>)> {
    let (compression, stored) = encode(&raw, precompressed, level)?;
    let compressed = match stored {
        Cow::Owned(z) => Some(z),
        Cow::Borrowed(_) => None,
    };
    let raw_len = raw.len() as u64;
    let payload = compressed.unwrap_or(raw);
    let header = RecordHeader {
        kind: UnitKind::RawChunk,
        compression,
        unit_hash: hash,
        raw_len,
        stored_len: payload.len() as u64,
    };
    Ok((header, payload))
}

/// Width and height from the image header, without decoding.
fn image_dims(format: Format, head: &[u8]) -> Option<(u32, u32)> {
    if !matches!(
        format,
        Format::Jpeg | Format::Png | Format::Webp | Format::Gif | Format::Avif
    ) {
        return None;
    }
    let s = imagesize::blob_size(head).ok()?;
    Some((u32::try_from(s.width).ok()?, u32::try_from(s.height).ok()?))
}
