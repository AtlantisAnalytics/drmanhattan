//! The snapshot record (spec §6.3): everything `recover` needs to rebuild a
//! snapshot's catalog rows from packs alone.
//!
//! The document is a CBOR sequence (RFC 8742) of `Item`s, written in this
//! order and streamed, so a snapshot of any size never sits in memory:
//!
//! 1. one `header` (`version`, the final snapshot row with `record_unit`
//!    unset: a row cannot name its own record),
//! 2. one `entry` per entry,
//! 3. one `content` per distinct content the snapshot references, with its
//!    unit hashes in `seq` order,
//! 4. one `xattr` per xattr row.
//!
//! Each item is a CBOR map tagged by its `t` field. Byte strings (names,
//! paths, ids, hashes) are CBOR byte strings. The document is chunked with
//! the binary profile into `raw_chunk` units; a `snapshot_record` unit then
//! holds the CBOR `RecordIndex` naming the chunks.

use super::{Halt, Stage};
use crate::catalog::Catalog;
use crate::chunk::chunk_reader;
use crate::compress::encode;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::{EntryKind, EntryRow, EntryStatus, SnapshotRow, SnapshotStatus, UnitRow};
use crate::pack::{RecordHeader, UnitKind};
use crate::unitread::UnitReader;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Version of the record document format.
const VERSION: u32 = 1;

/// A decoded snapshot record, held in memory (tests and Plan 1 callers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub snapshot: SnapshotRow,
    pub entries: Vec<EntryRow>,
    pub contents: Vec<(Hash32, Vec<Hash32>)>,
    pub xattrs: Vec<(Id, Vec<u8>, Vec<u8>)>,
}

// ---- wire types -----------------------------------------------------------

/// Raw bytes, encoded as a CBOR byte string.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Bin(Vec<u8>);

/// A 16-byte id as a byte string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct B16(Id);

/// A 32-byte hash as a byte string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct B32(Hash32);

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a byte string")
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> std::result::Result<Vec<u8>, E> {
        Ok(v.to_vec())
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> std::result::Result<Vec<u8>, E> {
        Ok(v)
    }
}

fn de_fixed<'de, D: Deserializer<'de>, const N: usize>(
    d: D,
) -> std::result::Result<[u8; N], D::Error> {
    let v = d.deserialize_byte_buf(BytesVisitor)?;
    let len = v.len();
    v.try_into()
        .map_err(|_| de::Error::invalid_length(len, &"a fixed-length byte string"))
}

impl Serialize for Bin {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Bin {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Bin, D::Error> {
        d.deserialize_byte_buf(BytesVisitor).map(Bin)
    }
}

impl Serialize for B16 {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_bytes(self.0.as_bytes())
    }
}

impl<'de> Deserialize<'de> for B16 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<B16, D::Error> {
        de_fixed::<D, 16>(d).map(|b| B16(Id::from_bytes(b)))
    }
}

impl Serialize for B32 {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_bytes(&self.0 .0)
    }
}

impl<'de> Deserialize<'de> for B32 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<B32, D::Error> {
        de_fixed::<D, 32>(d).map(|b| B32(Hash32(b)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WireSnapshot {
    id: B16,
    status: String,
    source_root: Bin,
    host: String,
    tool_version: String,
    started_at: i64,
    completed_at: Option<i64>,
    note: Option<String>,
    manifest_hash: Option<B32>,
    ext_manifest_hash: Option<B32>,
    source_manifest_hash: Option<B32>,
    record_unit: Option<B32>,
    entry_count: Option<i64>,
    file_bytes: Option<i64>,
    screened_out: i64,
    label_pattern: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WireEntry {
    id: B16,
    snapshot_id: B16,
    parent_id: Option<B16>,
    name: Bin,
    path: Bin,
    kind: String,
    mode: u32,
    mtime_ns: i64,
    size: Option<i64>,
    content_hash: Option<B32>,
    link_target: Option<Bin>,
    hardlink_group: Option<i64>,
    status: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
enum Item {
    Header {
        version: u32,
        snapshot: WireSnapshot,
    },
    Entry(WireEntry),
    Content {
        hash: B32,
        units: Vec<B32>,
    },
    Xattr {
        entry: B16,
        name: Bin,
        value: Bin,
    },
}

impl Item {
    /// Position of the item's section in the document.
    fn section(&self) -> u8 {
        match self {
            Item::Header { .. } => 0,
            Item::Entry(_) => 1,
            Item::Content { .. } => 2,
            Item::Xattr { .. } => 3,
        }
    }
}

/// Payload of the `snapshot_record` unit.
#[derive(Debug, Serialize, Deserialize)]
struct RecordIndex {
    snapshot_id: B16,
    document_blake3: B32,
    document_size: u64,
    chunks: Vec<B32>,
}

fn corrupt(why: impl std::fmt::Display) -> Error {
    Error::Corrupt(format!("snapshot record: {why}"))
}

impl From<SnapshotRow> for WireSnapshot {
    fn from(s: SnapshotRow) -> WireSnapshot {
        WireSnapshot {
            id: B16(s.id),
            status: s.status.as_str().to_string(),
            source_root: Bin(s.source_root),
            host: s.host,
            tool_version: s.tool_version,
            started_at: s.started_at,
            completed_at: s.completed_at,
            note: s.note,
            manifest_hash: s.manifest_hash.map(B32),
            ext_manifest_hash: s.ext_manifest_hash.map(B32),
            source_manifest_hash: s.source_manifest_hash.map(B32),
            record_unit: s.record_unit.map(B32),
            entry_count: s.entry_count,
            file_bytes: s.file_bytes,
            screened_out: s.screened_out,
            label_pattern: s.label_pattern,
        }
    }
}

impl TryFrom<WireSnapshot> for SnapshotRow {
    type Error = Error;

    fn try_from(s: WireSnapshot) -> Result<SnapshotRow> {
        Ok(SnapshotRow {
            id: s.id.0,
            status: SnapshotStatus::parse(&s.status)
                .ok_or_else(|| corrupt(format!("unknown snapshot status {:?}", s.status)))?,
            source_root: s.source_root.0,
            host: s.host,
            tool_version: s.tool_version,
            started_at: s.started_at,
            completed_at: s.completed_at,
            note: s.note,
            manifest_hash: s.manifest_hash.map(|h| h.0),
            ext_manifest_hash: s.ext_manifest_hash.map(|h| h.0),
            source_manifest_hash: s.source_manifest_hash.map(|h| h.0),
            record_unit: s.record_unit.map(|h| h.0),
            entry_count: s.entry_count,
            file_bytes: s.file_bytes,
            screened_out: s.screened_out,
            label_pattern: s.label_pattern,
        })
    }
}

impl From<EntryRow> for WireEntry {
    fn from(e: EntryRow) -> WireEntry {
        WireEntry {
            id: B16(e.id),
            snapshot_id: B16(e.snapshot_id),
            parent_id: e.parent_id.map(B16),
            name: Bin(e.name),
            path: Bin(e.path),
            kind: e.kind.as_str().to_string(),
            mode: e.mode,
            mtime_ns: e.mtime_ns,
            size: e.size,
            content_hash: e.content_hash.map(B32),
            link_target: e.link_target.map(Bin),
            hardlink_group: e.hardlink_group,
            status: e.status.as_str().to_string(),
        }
    }
}

impl TryFrom<WireEntry> for EntryRow {
    type Error = Error;

    fn try_from(e: WireEntry) -> Result<EntryRow> {
        Ok(EntryRow {
            id: e.id.0,
            snapshot_id: e.snapshot_id.0,
            parent_id: e.parent_id.map(|p| p.0),
            name: e.name.0,
            path: e.path.0,
            kind: EntryKind::parse(&e.kind)
                .ok_or_else(|| corrupt(format!("unknown entry kind {:?}", e.kind)))?,
            mode: e.mode,
            mtime_ns: e.mtime_ns,
            size: e.size,
            content_hash: e.content_hash.map(|h| h.0),
            link_target: e.link_target.map(|t| t.0),
            hardlink_group: e.hardlink_group,
            status: EntryStatus::parse(&e.status)
                .ok_or_else(|| corrupt(format!("unknown entry status {:?}", e.status)))?,
        })
    }
}

// ---- writing --------------------------------------------------------------

/// Removes the temp document when dropped, on success and on error.
struct TempFile(PathBuf);

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn to_cbor(item: &impl Serialize, out: impl Write, path: &Path) -> Result<()> {
    ciborium::into_writer(item, out).map_err(|e| match e {
        ciborium::ser::Error::Io(e) => Error::io(path, e),
        ciborium::ser::Error::Value(m) => corrupt(format!("cannot encode: {m}")),
    })
}

/// Compress `raw` like any other unit and build its record header.
fn unit(kind: UnitKind, raw: Vec<u8>, level: i32) -> Result<(RecordHeader, Vec<u8>)> {
    let hash = Hash32::of(&raw);
    let raw_len = raw.len() as u64;
    let (compression, stored) = encode(&raw, false, level)?;
    let payload = match stored {
        Cow::Owned(z) => z,
        Cow::Borrowed(_) => raw,
    };
    let header = RecordHeader {
        kind,
        compression,
        unit_hash: hash,
        raw_len,
        stored_len: payload.len() as u64,
    };
    Ok((header, payload))
}

/// Write the record of `snapshot` (its final row, `record_unit` unset) from
/// the committed catalog: the document goes to a temp file under `tmp_dir`,
/// is chunked with the binary profile, and each chunk and then the index
/// unit are handed to `emit` in order. Returns the index unit's hash.
pub(super) fn write_record(
    cat: &Catalog,
    snapshot: &SnapshotRow,
    tmp_dir: &Path,
    config: &Config,
    mut emit: impl FnMut(RecordHeader, Vec<u8>) -> Stage<()>,
) -> Stage<Hash32> {
    std::fs::create_dir_all(tmp_dir).map_err(|e| Error::io(tmp_dir, e))?;
    let path = tmp_dir.join(format!("record-{}.cbor", snapshot.id));
    let tmp = TempFile(path.clone());
    write_document(cat, snapshot, &path)?;

    let file = File::open(&tmp.0).map_err(|e| Error::io(&tmp.0, e))?;
    let level = config.compression.level;
    let mut chunks = Vec::new();
    let mut halted: Option<Halt> = None;
    let digest = chunk_reader(
        &config.chunking.binary,
        BufReader::with_capacity(1 << 20, file),
        |c, data| {
            let (header, payload) = unit(UnitKind::RawChunk, data.to_vec(), level)?;
            chunks.push(B32(c.hash));
            emit(header, payload).map_err(|h| {
                halted = Some(h);
                corrupt("storage stopped")
            })
        },
    );
    if let Some(h) = halted {
        return Err(h);
    }
    let digest = digest.map_err(|e| match e {
        Error::Io { path, source } if path.as_os_str().is_empty() => Error::io(&tmp.0, source),
        e => e,
    })?;
    drop(tmp);

    let index = RecordIndex {
        snapshot_id: B16(snapshot.id),
        document_blake3: B32(digest.hash),
        document_size: digest.size,
        chunks,
    };
    let mut raw = Vec::new();
    to_cbor(&index, &mut raw, &path)?;
    let (header, payload) = unit(UnitKind::SnapshotRecord, raw, level)?;
    let hash = header.unit_hash;
    emit(header, payload)?;
    Ok(hash)
}

fn write_document(cat: &Catalog, snapshot: &SnapshotRow, path: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|e| Error::io(path, e))?;
    let mut w = BufWriter::with_capacity(1 << 20, file);
    let header = Item::Header {
        version: VERSION,
        snapshot: WireSnapshot::from(snapshot.clone()),
    };
    to_cbor(&header, &mut w, path)?;
    cat.for_each_entry(snapshot.id, |e| {
        to_cbor(&Item::Entry(e.into()), &mut w, path)
    })?;
    cat.for_each_snapshot_content(snapshot.id, |hash, units| {
        let item = Item::Content {
            hash: B32(hash),
            units: units.into_iter().map(B32).collect(),
        };
        to_cbor(&item, &mut w, path)
    })?;
    cat.for_each_snapshot_xattr(snapshot.id, |entry, name, value| {
        let item = Item::Xattr {
            entry: B16(entry),
            name: Bin(name),
            value: Bin(value),
        };
        to_cbor(&item, &mut w, path)
    })?;
    w.flush().map_err(|e| Error::io(path, e))
}

// ---- reading --------------------------------------------------------------

/// Streams the document by reading its chunk units in order, hashing and
/// counting the bytes as they are loaded.
struct DocReader<'a> {
    reader: &'a UnitReader,
    cat: &'a Catalog,
    chunks: std::vec::IntoIter<B32>,
    cur: Arc<Vec<u8>>,
    pos: usize,
    hasher: blake3::Hasher,
    size: u64,
}

impl DocReader<'_> {
    fn next_chunk(&mut self, h: &Hash32) -> Result<()> {
        let row: UnitRow = self.cat.unit(h)?;
        if row.kind != UnitKind::RawChunk {
            return Err(corrupt(format!(
                "chunk {h} is a {} unit",
                row.kind.as_str()
            )));
        }
        self.cur = self.reader.read_unit(&row)?;
        self.pos = 0;
        self.hasher.update(&self.cur);
        self.size += self.cur.len() as u64;
        Ok(())
    }
}

impl Read for DocReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.pos == self.cur.len() {
            match self.chunks.next() {
                None => return Ok(0),
                Some(B32(h)) => self.next_chunk(&h).map_err(std::io::Error::other)?,
            }
        }
        let n = buf.len().min(self.cur.len() - self.pos);
        buf[..n].copy_from_slice(&self.cur[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Recover a typed error that `DocReader` passed through an `io::Error`.
fn from_io(e: std::io::Error) -> Error {
    if e.get_ref().is_some_and(|inner| inner.is::<Error>()) {
        if let Some(inner) = e.into_inner() {
            if let Ok(err) = inner.downcast::<Error>() {
                return *err;
            }
        }
        return corrupt("unreadable document");
    }
    corrupt(format!("read failed: {e}"))
}

fn from_cbor<T: serde::de::DeserializeOwned>(r: impl Read) -> Result<T> {
    ciborium::from_reader(r).map_err(|e| match e {
        ciborium::de::Error::Io(e) => from_io(e),
        ciborium::de::Error::Syntax(at) => corrupt(format!("invalid CBOR at offset {at}")),
        ciborium::de::Error::Semantic(_, m) => corrupt(m),
        ciborium::de::Error::RecursionLimitExceeded => corrupt("nesting too deep"),
    })
}

/// Parse a whole document from `r`, enforcing the section order.
fn parse_document(r: &mut impl BufRead) -> Result<SnapshotRecord> {
    let mut snapshot = None;
    let mut entries = Vec::new();
    let mut contents = Vec::new();
    let mut xattrs = Vec::new();
    let mut section = 0u8;
    loop {
        if r.fill_buf().map_err(from_io)?.is_empty() {
            break;
        }
        let item: Item = from_cbor(&mut *r)?;
        let s = item.section();
        if (snapshot.is_none() && s != 0) || (snapshot.is_some() && (s == 0 || s < section)) {
            return Err(corrupt("items out of order"));
        }
        section = s;
        match item {
            Item::Header {
                version,
                snapshot: w,
            } => {
                if version != VERSION {
                    return Err(corrupt(format!("unsupported version {version}")));
                }
                snapshot = Some(SnapshotRow::try_from(w)?);
            }
            Item::Entry(w) => entries.push(EntryRow::try_from(w)?),
            Item::Content { hash, units } => {
                contents.push((hash.0, units.into_iter().map(|u| u.0).collect()))
            }
            Item::Xattr { entry, name, value } => xattrs.push((entry.0, name.0, value.0)),
        }
    }
    let snapshot = snapshot.ok_or_else(|| corrupt("empty document"))?;
    if let Some(e) = entries.iter().find(|e| e.snapshot_id != snapshot.id) {
        return Err(corrupt(format!(
            "entry {} belongs to snapshot {}",
            e.id, e.snapshot_id
        )));
    }
    Ok(SnapshotRecord {
        snapshot,
        entries,
        contents,
        xattrs,
    })
}

/// Read back the snapshot record whose `snapshot_record` unit is `unit`,
/// verifying every chunk, the document's BLAKE3 and size, and that the
/// document describes the snapshot the index names.
pub fn read_record(reader: &UnitReader, cat: &Catalog, unit: &Hash32) -> Result<SnapshotRecord> {
    let row = cat.unit(unit)?;
    if row.kind != UnitKind::SnapshotRecord {
        return Err(corrupt(format!(
            "unit {unit} is a {} unit",
            row.kind.as_str()
        )));
    }
    let index: RecordIndex = from_cbor(&reader.read_unit(&row)?[..])?;
    let doc = DocReader {
        reader,
        cat,
        chunks: index.chunks.into_iter(),
        cur: Arc::new(Vec::new()),
        pos: 0,
        hasher: blake3::Hasher::new(),
        size: 0,
    };
    let mut buf = BufReader::with_capacity(1 << 16, doc);
    let rec = parse_document(&mut buf)?;
    // `parse_document` stops only at end of input: every chunk was read.
    let doc = buf.into_inner();
    let hash = Hash32(*doc.hasher.finalize().as_bytes());
    if doc.size != index.document_size || hash != index.document_blake3.0 {
        return Err(corrupt(format!(
            "document is {} bytes with BLAKE3 {hash}, index says {} bytes with {}",
            doc.size, index.document_size, index.document_blake3.0
        )));
    }
    if rec.snapshot.id != index.snapshot_id.0 {
        return Err(corrupt(format!(
            "index names snapshot {}, document holds {}",
            index.snapshot_id.0, rec.snapshot.id
        )));
    }
    Ok(rec)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> SnapshotRow {
        SnapshotRow {
            id: Id::new_v7(),
            status: SnapshotStatus::Complete,
            source_root: b"/src\xff".to_vec(),
            host: "h".into(),
            tool_version: "0".into(),
            started_at: -5,
            completed_at: Some(7),
            note: None,
            manifest_hash: Some(Hash32::of(b"m")),
            ext_manifest_hash: None,
            source_manifest_hash: None,
            record_unit: None,
            entry_count: Some(1),
            file_bytes: Some(0),
            screened_out: 0,
            label_pattern: None,
        }
    }

    fn entry(snap: Id) -> EntryRow {
        EntryRow {
            id: Id::new_v7(),
            snapshot_id: snap,
            parent_id: None,
            name: Vec::new(),
            path: Vec::new(),
            kind: EntryKind::Dir,
            mode: 0o755,
            mtime_ns: -1,
            size: None,
            content_hash: None,
            link_target: None,
            hardlink_group: None,
            status: EntryStatus::Ok,
        }
    }

    fn encode_items(items: &[Item]) -> Vec<u8> {
        let mut out = Vec::new();
        for i in items {
            to_cbor(i, &mut out, Path::new("")).unwrap();
        }
        out
    }

    fn header(s: &SnapshotRow) -> Item {
        Item::Header {
            version: VERSION,
            snapshot: s.clone().into(),
        }
    }

    #[test]
    fn document_roundtrips() {
        let s = snapshot();
        let e = entry(s.id);
        let big = vec![0xA5u8; 10_000]; // longer than ciborium's scratch buffer
        let doc = encode_items(&[
            header(&s),
            Item::Entry(e.clone().into()),
            Item::Content {
                hash: B32(Hash32::of(b"c")),
                units: vec![B32(Hash32::of(b"u1")), B32(Hash32::of(b"u2"))],
            },
            Item::Xattr {
                entry: B16(e.id),
                name: Bin(b"user.x".to_vec()),
                value: Bin(big.clone()),
            },
        ]);
        let rec = parse_document(&mut &doc[..]).unwrap();
        assert_eq!(rec.snapshot, s);
        assert_eq!(rec.entries, vec![e.clone()]);
        assert_eq!(
            rec.contents,
            vec![(Hash32::of(b"c"), vec![Hash32::of(b"u1"), Hash32::of(b"u2")])]
        );
        assert_eq!(rec.xattrs, vec![(e.id, b"user.x".to_vec(), big)]);
    }

    #[test]
    fn misordered_or_damaged_documents_are_corrupt() {
        let s = snapshot();
        let e = entry(s.id);
        let content = || Item::Content {
            hash: B32(Hash32::of(b"c")),
            units: Vec::new(),
        };
        let bad = [
            encode_items(&[Item::Entry(e.clone().into())]),
            encode_items(&[header(&s), content(), Item::Entry(e.clone().into())]),
            encode_items(&[header(&s), header(&s)]),
            encode_items(&[header(&s), Item::Entry(entry(Id::new_v7()).into())]),
            Vec::new(),
        ];
        for doc in bad {
            assert!(matches!(
                parse_document(&mut &doc[..]),
                Err(Error::Corrupt(_))
            ));
        }
        // Truncated mid-item.
        let doc = encode_items(&[header(&s), Item::Entry(e.into())]);
        let cut = &doc[..doc.len() - 3];
        assert!(matches!(
            parse_document(&mut &cut[..]),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn temp_document_removed_when_storage_stops() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::repo::Repo::init(&tmp.path().join("r")).unwrap();
        let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
        let s = snapshot();
        let mut emitted = 0;
        let r = write_record(&cat, &s, &repo.tmp_dir(), &repo.config, |_, _| {
            emitted += 1;
            Err(Halt::Aborted)
        });
        assert!(matches!(r, Err(Halt::Aborted)));
        assert_eq!(emitted, 1);
        let left = std::fs::read_dir(repo.tmp_dir()).unwrap().count();
        assert_eq!(left, 0);
    }
}
