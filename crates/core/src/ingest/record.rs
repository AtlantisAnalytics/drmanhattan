//! The snapshot record (spec §6.3): everything `recover` needs to rebuild a
//! snapshot's catalog rows from packs alone.
//!
//! The document is a CBOR sequence (RFC 8742) of `Item`s, written in this
//! order and streamed, so a snapshot of any size never sits in memory:
//!
//! 1. one `header` (`version`, the final snapshot row with `record_unit`
//!    unset: a row cannot name its own record),
//! 2. one `entry` per entry,
//! 3. one `content` per distinct content the snapshot references: its row
//!    and its unit hashes in `seq` order,
//! 4. one `xattr` per xattr row,
//! 5. one `provenance` per provenance row (optional; Plan 1 writes none),
//! 6. one `screen_hit` per screen hit (optional; Plan 1 writes none).
//!
//! Each item is a CBOR map tagged by its `t` field; spec §6.3 lists every
//! field. Byte strings (names, paths, ids, hashes) are CBOR byte strings.
//! The document is chunked with the binary profile into `raw_chunk` units;
//! a `snapshot_record` unit then holds the CBOR `RecordIndex` naming them.
//!
//! The item kinds and their fields are permanent for `VERSION` 1. Adding any
//! other item kind, adding a vocabulary value (e.g. a `Format`), or changing
//! a field requires bumping `VERSION`. Decoding is strict: unknown item
//! kinds, unknown fields, missing required fields, and unknown enum values
//! are corruption. Optional fields are always written (null when absent),
//! and a missing optional field reads as null.

use super::{Halt, Stage};
use crate::catalog::Catalog;
use crate::chunk::chunk_reader;
use crate::compress::encode;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::{
    ContentRow, EntryKind, EntryRow, EntryStatus, SnapshotRow, SnapshotStatus, UnitRow,
};
use crate::pack::{RecordHeader, UnitKind};
use crate::sniff::{Format, ProfileKind};
use crate::unitread::UnitReader;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Version of the record document format. Bump it for any new item kind
/// or field change (see the module doc).
const VERSION: u32 = 1;

/// A decoded snapshot record, held in memory (tests and Plan 1 callers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub snapshot: SnapshotRow,
    pub entries: Vec<EntryRow>,
    /// Each content row with its unit hashes in `seq` order.
    pub contents: Vec<(ContentRow, Vec<Hash32>)>,
    pub xattrs: Vec<(Id, Vec<u8>, Vec<u8>)>,
    /// Empty in Plan 1.
    pub provenance: Vec<ProvenanceRecord>,
    /// Empty in Plan 1.
    pub screen_hits: Vec<ScreenHitRecord>,
}

/// A `provenance` row (spec §6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvenanceRecord {
    pub entry_id: Id,
    pub source: String,
    pub url: Option<String>,
    pub fetched_at: Option<i64>,
    pub last_modified: Option<i64>,
    pub server_ip: Option<String>,
    pub http_headers: Option<String>,
}

/// A `screen_hit` row (spec §6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScreenHitRecord {
    pub id: i64,
    pub snapshot_id: Id,
    pub entry_id: Id,
    pub verdict: String,
    pub screener: String,
    pub list_version: Option<String>,
    pub blake3: Hash32,
    pub md5: Option<Vec<u8>>,
    pub sha1: Option<Vec<u8>>,
    pub sha256: Option<Vec<u8>>,
    pub detail: Option<String>,
    pub detected_at: i64,
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
struct WireProvenance {
    entry_id: B16,
    source: String,
    url: Option<String>,
    fetched_at: Option<i64>,
    last_modified: Option<i64>,
    server_ip: Option<String>,
    http_headers: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireScreenHit {
    id: i64,
    snapshot_id: B16,
    entry_id: B16,
    verdict: String,
    screener: String,
    list_version: Option<String>,
    blake3: B32,
    md5: Option<Bin>,
    sha1: Option<Bin>,
    sha256: Option<Bin>,
    detail: Option<String>,
    detected_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case", deny_unknown_fields)]
enum Item {
    Header {
        version: u32,
        snapshot: WireSnapshot,
    },
    Entry(WireEntry),
    Content {
        hash: B32,
        size: i64,
        /// `Format::as_str`, or null when the catalog has none.
        format: Option<String>,
        /// `ProfileKind::as_str`: the chunk profile actually used.
        profile: String,
        unit_count: i64,
        units: Vec<B32>,
    },
    Xattr {
        entry: B16,
        name: Bin,
        value: Bin,
    },
    Provenance(WireProvenance),
    ScreenHit(WireScreenHit),
}

impl Item {
    /// Position of the item's section in the document.
    fn section(&self) -> u8 {
        match self {
            Item::Header { .. } => 0,
            Item::Entry(_) => 1,
            Item::Content { .. } => 2,
            Item::Xattr { .. } => 3,
            Item::Provenance(_) => 4,
            Item::ScreenHit(_) => 5,
        }
    }
}

/// Payload of the `snapshot_record` unit.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// The closed vocabularies of §6.1, checked on read (and on write, once a
/// writer builds these items).
const PROVENANCE_SOURCES: &[&str] = &["wget_layout", "jsonl"];
const SCREEN_VERDICTS: &[&str] = &["match", "error"];

fn check_vocab(what: &str, v: &str, allowed: &[&str]) -> Result<()> {
    if allowed.contains(&v) {
        Ok(())
    } else {
        Err(corrupt(format!("unknown {what} {v:?}")))
    }
}

impl TryFrom<WireProvenance> for ProvenanceRecord {
    type Error = Error;

    fn try_from(p: WireProvenance) -> Result<ProvenanceRecord> {
        check_vocab("provenance source", &p.source, PROVENANCE_SOURCES)?;
        Ok(ProvenanceRecord {
            entry_id: p.entry_id.0,
            source: p.source,
            url: p.url,
            fetched_at: p.fetched_at,
            last_modified: p.last_modified,
            server_ip: p.server_ip,
            http_headers: p.http_headers,
        })
    }
}

impl TryFrom<WireScreenHit> for ScreenHitRecord {
    type Error = Error;

    fn try_from(h: WireScreenHit) -> Result<ScreenHitRecord> {
        check_vocab("screen_hit verdict", &h.verdict, SCREEN_VERDICTS)?;
        Ok(ScreenHitRecord {
            id: h.id,
            snapshot_id: h.snapshot_id.0,
            entry_id: h.entry_id.0,
            verdict: h.verdict,
            screener: h.screener,
            list_version: h.list_version,
            blake3: h.blake3.0,
            md5: h.md5.map(|b| b.0),
            sha1: h.sha1.map(|b| b.0),
            sha256: h.sha256.map(|b| b.0),
            detail: h.detail,
            detected_at: h.detected_at,
        })
    }
}

/// Fails rather than write a content item no reader would accept.
fn content_item(c: ContentRow, units: Vec<Hash32>) -> Result<Item> {
    if usize::try_from(c.unit_count).ok() != Some(units.len()) {
        return Err(corrupt(format!(
            "catalog content {} has unit_count {} but {} units",
            c.hash,
            c.unit_count,
            units.len()
        )));
    }
    Ok(Item::Content {
        hash: B32(c.hash),
        size: c.size,
        format: c.format.map(|f| f.as_str().to_string()),
        profile: c.profile.as_str().to_string(),
        unit_count: c.unit_count,
        units: units.into_iter().map(B32).collect(),
    })
}

fn content_row(
    hash: B32,
    size: i64,
    format: Option<String>,
    profile: String,
    unit_count: i64,
    units: Vec<B32>,
) -> Result<(ContentRow, Vec<Hash32>)> {
    if usize::try_from(unit_count).ok() != Some(units.len()) {
        return Err(corrupt(format!(
            "content {} has unit_count {unit_count} but lists {} units",
            hash.0,
            units.len()
        )));
    }
    let format = match format {
        Some(f) => {
            Some(Format::from_str(&f).ok_or_else(|| corrupt(format!("unknown format {f:?}")))?)
        }
        None => None,
    };
    let profile = ProfileKind::parse(&profile)
        .ok_or_else(|| corrupt(format!("unknown profile {profile:?}")))?;
    let row = ContentRow {
        hash: hash.0,
        size,
        format,
        profile,
        unit_count,
    };
    Ok((row, units.into_iter().map(|u| u.0).collect()))
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
    cat.for_each_snapshot_content(snapshot.id, |content, units| {
        to_cbor(&content_item(content, units)?, &mut w, path)
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
    let mut provenance = Vec::new();
    let mut screen_hits = Vec::new();
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
            Item::Content {
                hash,
                size,
                format,
                profile,
                unit_count,
                units,
            } => contents.push(content_row(hash, size, format, profile, unit_count, units)?),
            Item::Xattr { entry, name, value } => xattrs.push((entry.0, name.0, value.0)),
            Item::Provenance(p) => provenance.push(ProvenanceRecord::try_from(p)?),
            Item::ScreenHit(h) => screen_hits.push(ScreenHitRecord::try_from(h)?),
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
        provenance,
        screen_hits,
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
    use ciborium::Value;

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

    fn content(units: &[&[u8]]) -> (ContentRow, Vec<Hash32>) {
        let row = ContentRow {
            hash: Hash32::of(b"c"),
            size: 12,
            format: Some(Format::Html),
            profile: ProfileKind::Text,
            unit_count: units.len() as i64,
        };
        (row, units.iter().map(|u| Hash32::of(u)).collect())
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
            content_item(content(&[b"u1", b"u2"]).0, content(&[b"u1", b"u2"]).1).unwrap(),
            Item::Xattr {
                entry: B16(e.id),
                name: Bin(b"user.x".to_vec()),
                value: Bin(big.clone()),
            },
        ]);
        let rec = parse_document(&mut &doc[..]).unwrap();
        assert_eq!(rec.snapshot, s);
        assert_eq!(rec.entries, vec![e.clone()]);
        assert_eq!(rec.contents, vec![content(&[b"u1", b"u2"])]);
        assert_eq!(rec.xattrs, vec![(e.id, b"user.x".to_vec(), big)]);
        assert!(rec.provenance.is_empty() && rec.screen_hits.is_empty());
    }

    #[test]
    fn optional_provenance_and_screen_hit_sections_roundtrip() {
        let s = snapshot();
        let e = entry(s.id);
        let doc = encode_items(&[
            header(&s),
            Item::Entry(e.clone().into()),
            Item::Provenance(WireProvenance {
                entry_id: B16(e.id),
                source: "wget_layout".into(),
                url: Some("https://example.com/a".into()),
                fetched_at: Some(-3),
                last_modified: None,
                server_ip: Some("192.0.2.1".into()),
                http_headers: Some("{\"etag\":\"x\"}".into()),
            }),
            Item::ScreenHit(WireScreenHit {
                id: 9,
                snapshot_id: B16(s.id),
                entry_id: B16(e.id),
                verdict: "match".into(),
                screener: "list-a".into(),
                list_version: None,
                blake3: B32(Hash32::of(b"hit")),
                md5: Some(Bin(vec![1; 16])),
                sha1: None,
                sha256: Some(Bin(vec![2; 32])),
                detail: Some("d".into()),
                detected_at: 42,
            }),
        ]);
        let rec = parse_document(&mut &doc[..]).unwrap();
        assert_eq!(
            rec.provenance,
            vec![ProvenanceRecord {
                entry_id: e.id,
                source: "wget_layout".into(),
                url: Some("https://example.com/a".into()),
                fetched_at: Some(-3),
                last_modified: None,
                server_ip: Some("192.0.2.1".into()),
                http_headers: Some("{\"etag\":\"x\"}".into()),
            }]
        );
        assert_eq!(
            rec.screen_hits,
            vec![ScreenHitRecord {
                id: 9,
                snapshot_id: s.id,
                entry_id: e.id,
                verdict: "match".into(),
                screener: "list-a".into(),
                list_version: None,
                blake3: Hash32::of(b"hit"),
                md5: Some(vec![1; 16]),
                sha1: None,
                sha256: Some(vec![2; 32]),
                detail: Some("d".into()),
                detected_at: 42,
            }]
        );
    }

    #[test]
    fn content_unit_count_must_match() {
        let s = snapshot();
        let (row, units) = content(&[b"u1", b"u2"]);
        let bad = ContentRow {
            unit_count: 3,
            ..row
        };
        // The writer refuses to emit it ...
        assert!(matches!(
            content_item(bad.clone(), units.clone()),
            Err(Error::Corrupt(_))
        ));
        // ... and the reader rejects it if one turns up anyway.
        let item = Item::Content {
            hash: B32(bad.hash),
            size: bad.size,
            format: None,
            profile: "text".into(),
            unit_count: 3,
            units: units.into_iter().map(B32).collect(),
        };
        let doc = encode_items(&[header(&s), item]);
        let err = parse_document(&mut &doc[..]).unwrap_err();
        assert!(
            matches!(&err, Error::Corrupt(m) if m.contains("unit_count")),
            "{err:?}"
        );
    }

    #[test]
    fn content_without_format_roundtrips() {
        let s = snapshot();
        let (row, units) = content(&[b"u1"]);
        let row = ContentRow {
            format: None,
            profile: ProfileKind::Binary,
            ..row
        };
        let doc = encode_items(&[
            header(&s),
            content_item(row.clone(), units.clone()).unwrap(),
        ]);
        let rec = parse_document(&mut &doc[..]).unwrap();
        assert_eq!(rec.contents, vec![(row, units)]);
    }

    /// `item` as a CBOR value, edited by `f`, then encoded after a header.
    fn edited(s: &SnapshotRow, item: &Item, f: impl FnOnce(&mut Vec<(Value, Value)>)) -> Vec<u8> {
        let mut v = Value::serialized(item).unwrap();
        let Value::Map(m) = &mut v else {
            panic!("items are maps")
        };
        f(m);
        let mut doc = encode_items(&[header(s)]);
        ciborium::into_writer(&v, &mut doc).unwrap();
        doc
    }

    fn content_value() -> Item {
        let (row, units) = content(&[b"u1"]);
        content_item(row, units).unwrap()
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let s = snapshot();
        let doc = edited(&s, &content_value(), |m| {
            m.push((Value::Text("extra".into()), Value::Integer(1.into())))
        });
        assert!(matches!(
            parse_document(&mut &doc[..]),
            Err(Error::Corrupt(_))
        ));
        let mut snap = Value::serialized(&header(&s)).unwrap();
        if let Value::Map(m) = &mut snap {
            for (k, v) in m.iter_mut() {
                if k.as_text() == Some("snapshot") {
                    if let Value::Map(inner) = v {
                        inner.push((Value::Text("extra".into()), Value::Null));
                    }
                }
            }
        }
        let mut doc = Vec::new();
        ciborium::into_writer(&snap, &mut doc).unwrap();
        assert!(matches!(
            parse_document(&mut &doc[..]),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn absent_optional_field_reads_as_null() {
        let s = snapshot();
        let doc = edited(&s, &content_value(), |m| {
            m.retain(|(k, _)| k.as_text() != Some("format"))
        });
        let rec = parse_document(&mut &doc[..]).unwrap();
        assert_eq!(rec.contents[0].0.format, None);
        // A required field may not be left out.
        let doc = edited(&s, &content_value(), |m| {
            m.retain(|(k, _)| k.as_text() != Some("profile"))
        });
        assert!(matches!(
            parse_document(&mut &doc[..]),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn unknown_vocabulary_is_rejected() {
        let s = snapshot();
        let e = entry(s.id);
        let prov = Item::Provenance(WireProvenance {
            entry_id: B16(e.id),
            source: "carrier_pigeon".into(),
            url: None,
            fetched_at: None,
            last_modified: None,
            server_ip: None,
            http_headers: None,
        });
        let hit = Item::ScreenHit(WireScreenHit {
            id: 1,
            snapshot_id: B16(s.id),
            entry_id: B16(e.id),
            verdict: "maybe".into(),
            screener: "x".into(),
            list_version: None,
            blake3: B32(Hash32::of(b"h")),
            md5: None,
            sha1: None,
            sha256: None,
            detail: None,
            detected_at: 0,
        });
        let (row, units) = content(&[b"u1"]);
        let bad_format = Item::Content {
            hash: B32(row.hash),
            size: row.size,
            format: Some("bmp".into()),
            profile: "text".into(),
            unit_count: 1,
            units: units.into_iter().map(B32).collect(),
        };
        for item in [prov, hit, bad_format] {
            let doc = encode_items(&[header(&s), item]);
            let err = parse_document(&mut &doc[..]).unwrap_err();
            assert!(
                matches!(&err, Error::Corrupt(m) if m.contains("unknown")),
                "{err:?}"
            );
        }
    }

    #[test]
    fn misordered_or_damaged_documents_are_corrupt() {
        let s = snapshot();
        let e = entry(s.id);
        let content = || {
            let (row, units) = content(&[]);
            content_item(row, units).unwrap()
        };
        let screen_hit = || {
            Item::ScreenHit(WireScreenHit {
                id: 1,
                snapshot_id: B16(s.id),
                entry_id: B16(e.id),
                verdict: "error".into(),
                screener: "x".into(),
                list_version: None,
                blake3: B32(Hash32::of(b"h")),
                md5: None,
                sha1: None,
                sha256: None,
                detail: None,
                detected_at: 0,
            })
        };
        let provenance = || {
            Item::Provenance(WireProvenance {
                entry_id: B16(e.id),
                source: "jsonl".into(),
                url: None,
                fetched_at: None,
                last_modified: None,
                server_ip: None,
                http_headers: None,
            })
        };
        let bad = [
            encode_items(&[Item::Entry(e.clone().into())]),
            encode_items(&[header(&s), content(), Item::Entry(e.clone().into())]),
            encode_items(&[header(&s), screen_hit(), provenance()]),
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
