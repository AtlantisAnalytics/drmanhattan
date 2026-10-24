//! Self-describing pack files (spec §6.2). All integers are little-endian.
//!
//! ```text
//! header : "WMKPACK\0" (8) | format u16 = 1 | pack_id (16)              = 26 bytes
//! record : marker u32 = 0x574D4B52 | kind u8 | compression u8 | unit_hash (32)
//!          | raw_len u64 | stored_len u64 | header_crc32c u32 | payload
//! footer : count u64 | count x (unit_hash 32 | offset u64) | pack_blake3 (32)
//!          | footer_offset u64 | "WMKPEND\0" (8)
//! ```
//!
//! The CRC32C covers the 50 bytes from `kind` through `stored_len`.
//! `footer_offset` is the file offset where the footer starts and `pack_blake3`
//! covers `[0, footer_offset)`. The footer is end-anchored so a sealed pack is
//! recognised from its last 16 bytes, independent of any mid-file damage.

use crate::compress::{self, Compression};
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const FILE_MAGIC: &[u8; 8] = b"WMKPACK\0";
const FOOTER_MAGIC: &[u8; 8] = b"WMKPEND\0";
const FORMAT: u16 = 1;
const RECORD_MARKER: u32 = 0x574D_4B52;
const FILE_HEADER_LEN: u64 = 26;
/// Bytes covered by the CRC: kind, compression, hash, raw_len, stored_len.
const CRC_SPAN: usize = 50;
/// Marker + CRC span + CRC.
const RECORD_HEADER_LEN: usize = 4 + CRC_SPAN + 4;
/// count (8) + pack_blake3 (32) + footer_offset (8) + magic (8): footer with no entries.
const FOOTER_FIXED_LEN: u64 = 8 + 32 + 8 + 8;

/// Upper bound on a unit's raw and stored length (fastcdc's maximum chunk size).
pub const MAX_UNIT_LEN: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UnitKind {
    RawChunk = 0,
    SnapshotRecord = 1,
    PurgeRecord = 2,
}

impl UnitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            UnitKind::RawChunk => "raw_chunk",
            UnitKind::SnapshotRecord => "snapshot_record",
            UnitKind::PurgeRecord => "purge_record",
        }
    }

    pub fn from_u8(v: u8) -> Option<UnitKind> {
        match v {
            0 => Some(UnitKind::RawChunk),
            1 => Some(UnitKind::SnapshotRecord),
            2 => Some(UnitKind::PurgeRecord),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub kind: UnitKind,
    pub compression: Compression,
    pub unit_hash: Hash32,
    pub raw_len: u64,
    pub stored_len: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct SealedPack {
    pub id: Id,
    pub size: u64,
    pub blake3: Hash32,
}

#[derive(Debug)]
pub struct PackScan {
    pub id: Id,
    /// (offset of the record marker, header) for each valid record, sorted by
    /// offset. Unsealed: the sequential prefix. Sealed: the union of the
    /// records the sequential parse validated and the index entries whose
    /// record validates on its own.
    pub records: Vec<(u64, RecordHeader)>,
    pub sealed: bool,
    /// End of the valid prefix: the file length if sealed, else the offset
    /// where the first invalid record (or torn tail) begins.
    pub valid_len: u64,
    /// `Some` only when sealed: whether the stored pack hash matches.
    pub blake3_ok: Option<bool>,
    /// Sealed packs only: sorted offsets where something claims a record but
    /// no valid record exists (failing index entries, and the sequential stop
    /// position if it is before the footer).
    pub damaged: Vec<u64>,
    /// True iff the pack is sealed, the footer index lists exactly the
    /// sequentially parsed records (same hashes and offsets), and that parse
    /// reached the footer. Always false for unsealed packs.
    pub index_ok: bool,
}

/// `<packs_dir>/<last 2 hex chars of id>/<id hyphenated>.pack`
pub fn pack_path(packs_dir: &Path, id: Id) -> PathBuf {
    let s = id.to_string();
    packs_dir.join(&s[s.len() - 2..]).join(format!("{s}.pack"))
}

/// Length invariants every record must satisfy.
fn check_sizes(
    compression: Compression,
    raw_len: u64,
    stored_len: u64,
) -> std::result::Result<(), String> {
    if raw_len > MAX_UNIT_LEN {
        return Err(format!("raw_len {raw_len} exceeds {MAX_UNIT_LEN}"));
    }
    if stored_len > MAX_UNIT_LEN {
        return Err(format!("stored_len {stored_len} exceeds {MAX_UNIT_LEN}"));
    }
    match compression {
        Compression::None if stored_len != raw_len => Err(format!(
            "uncompressed record has stored_len {stored_len} != raw_len {raw_len}"
        )),
        Compression::Zstd if stored_len >= raw_len => Err(format!(
            "zstd record has stored_len {stored_len} >= raw_len {raw_len}"
        )),
        _ => Ok(()),
    }
}

fn encode_record_header(h: &RecordHeader) -> [u8; RECORD_HEADER_LEN] {
    let mut b = [0u8; RECORD_HEADER_LEN];
    b[0..4].copy_from_slice(&RECORD_MARKER.to_le_bytes());
    b[4] = h.kind as u8;
    b[5] = h.compression as u8;
    b[6..38].copy_from_slice(&h.unit_hash.0);
    b[38..46].copy_from_slice(&h.raw_len.to_le_bytes());
    b[46..54].copy_from_slice(&h.stored_len.to_le_bytes());
    let crc = crc32c::crc32c(&b[4..4 + CRC_SPAN]);
    b[54..58].copy_from_slice(&crc.to_le_bytes());
    b
}

enum HeaderError {
    /// Bad marker, CRC or lengths: damage or a torn tail.
    Invalid(String),
    /// Valid CRC but a kind or compression this build does not know.
    Unsupported(String),
}

fn decode_record_header(
    b: &[u8; RECORD_HEADER_LEN],
) -> std::result::Result<RecordHeader, HeaderError> {
    let marker = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
    if marker != RECORD_MARKER {
        return Err(HeaderError::Invalid(format!(
            "bad record marker {marker:#010x}"
        )));
    }
    let crc = u32::from_le_bytes([b[54], b[55], b[56], b[57]]);
    if crc != crc32c::crc32c(&b[4..4 + CRC_SPAN]) {
        return Err(HeaderError::Invalid("record header CRC mismatch".into()));
    }
    let kind = UnitKind::from_u8(b[4])
        .ok_or_else(|| HeaderError::Unsupported(format!("unsupported unit kind {}", b[4])))?;
    let compression = Compression::from_u8(b[5])
        .ok_or_else(|| HeaderError::Unsupported(format!("unsupported compression {}", b[5])))?;
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&b[6..38]);
    let h = RecordHeader {
        kind,
        compression,
        unit_hash: Hash32(hash),
        raw_len: u64_at(b, 38),
        stored_len: u64_at(b, 46),
    };
    check_sizes(h.compression, h.raw_len, h.stored_len).map_err(HeaderError::Invalid)?;
    Ok(h)
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}

fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| Error::io(dir, e))
}

pub struct PackWriter {
    file: File,
    path: PathBuf,
    id: Id,
    hasher: blake3::Hasher,
    len: u64,
    entries: Vec<(Hash32, u64)>,
    /// Set on any I/O error: file, hasher and index may then disagree.
    failed: bool,
}

impl PackWriter {
    /// Create a new pack file (failing if it already exists) and write its header.
    pub fn create(packs_dir: &Path, id: Id) -> Result<PackWriter> {
        let path = pack_path(packs_dir, id);
        let parent = path.parent().unwrap_or(packs_dir).to_path_buf();
        std::fs::create_dir_all(&parent).map_err(|e| Error::io(&parent, e))?;
        // Make the fanout dir's own directory entry durable.
        fsync_dir(packs_dir)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| Error::io(&path, e))?;
        let mut header = Vec::with_capacity(FILE_HEADER_LEN as usize);
        header.extend_from_slice(FILE_MAGIC);
        header.extend_from_slice(&FORMAT.to_le_bytes());
        header.extend_from_slice(id.as_bytes());
        let mut w = PackWriter {
            file,
            path,
            id,
            hasher: blake3::Hasher::new(),
            len: 0,
            entries: Vec::new(),
            failed: false,
        };
        w.write_hashed(&header)?;
        fsync_dir(&parent)?;
        Ok(w)
    }

    fn poisoned(&self) -> Error {
        Error::Corrupt(format!(
            "pack writer poisoned by earlier I/O error: {}",
            self.path.display()
        ))
    }

    fn write_hashed(&mut self, bytes: &[u8]) -> Result<()> {
        if self.failed {
            return Err(self.poisoned());
        }
        if let Err(e) = self.file.write_all(bytes) {
            self.failed = true;
            return Err(Error::io(&self.path, e));
        }
        self.hasher.update(bytes);
        self.len += bytes.len() as u64;
        Ok(())
    }

    /// Append a record; returns the offset of its marker.
    pub fn append(&mut self, h: &RecordHeader, payload: &[u8]) -> Result<u64> {
        if self.failed {
            return Err(self.poisoned());
        }
        if h.stored_len != payload.len() as u64 {
            return Err(Error::Corrupt(format!(
                "stored_len {} != payload length {}",
                h.stored_len,
                payload.len()
            )));
        }
        check_sizes(h.compression, h.raw_len, h.stored_len).map_err(Error::Corrupt)?;
        let offset = self.len;
        self.write_hashed(&encode_record_header(h))?;
        self.write_hashed(payload)?;
        self.entries.push((h.unit_hash, offset));
        Ok(offset)
    }

    /// Bytes written so far, including the file header.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// True when no records have been appended.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn sync(&mut self) -> Result<()> {
        if self.failed {
            return Err(self.poisoned());
        }
        if let Err(e) = self.file.sync_all() {
            self.failed = true;
            return Err(Error::io(&self.path, e));
        }
        Ok(())
    }

    /// Test seam: swap the file handle for a read-only one so the next write
    /// fails with a real OS error.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn __break_file_for_test(&mut self) -> Result<()> {
        self.file = File::open(&self.path).map_err(|e| Error::io(&self.path, e))?;
        Ok(())
    }

    /// Write the footer, then fsync the file and its parent directory.
    pub fn seal(mut self) -> Result<SealedPack> {
        if self.failed {
            return Err(self.poisoned());
        }
        let footer_offset = self.len;
        let blake3 = Hash32(*self.hasher.finalize().as_bytes());
        let mut footer = Vec::with_capacity(8 + 40 * self.entries.len() + 48);
        footer.extend_from_slice(&(self.entries.len() as u64).to_le_bytes());
        for (hash, off) in &self.entries {
            footer.extend_from_slice(&hash.0);
            footer.extend_from_slice(&off.to_le_bytes());
        }
        footer.extend_from_slice(&blake3.0);
        footer.extend_from_slice(&footer_offset.to_le_bytes());
        footer.extend_from_slice(FOOTER_MAGIC);
        if let Err(e) = self.file.write_all(&footer) {
            self.failed = true;
            return Err(Error::io(&self.path, e));
        }
        self.len += footer.len() as u64;
        self.sync()?;
        if let Some(parent) = self.path.parent() {
            fsync_dir(parent)?;
        }
        Ok(SealedPack {
            id: self.id,
            size: self.len,
            blake3,
        })
    }
}

pub struct PackReader {
    file: File,
    path: PathBuf,
    len: u64,
}

impl PackReader {
    pub fn open(path: &Path) -> Result<PackReader> {
        let file = File::open(path).map_err(|e| Error::io(path, e))?;
        let len = file.metadata().map_err(|e| Error::io(path, e))?.len();
        Ok(PackReader {
            file,
            path: path.to_path_buf(),
            len,
        })
    }

    fn corrupt(&self, offset: u64, why: impl std::fmt::Display) -> Error {
        Error::Corrupt(format!(
            "pack {} at offset {offset}: {why}",
            self.path.display()
        ))
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.file
            .read_exact_at(buf, offset)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::UnexpectedEof => self.corrupt(offset, "truncated"),
                _ => Error::io(&self.path, e),
            })
    }

    /// Parse the record header at `pos`, requiring header and payload to fit
    /// below `limit`. The outer `Err` is an I/O error or an unsupported kind or
    /// compression; the inner `Err` is a reason the record is invalid or torn.
    fn parse_record_at(
        &self,
        pos: u64,
        limit: u64,
    ) -> Result<std::result::Result<RecordHeader, String>> {
        let body = pos + RECORD_HEADER_LEN as u64;
        if body > limit {
            return Ok(Err("truncated record header".into()));
        }
        let mut hb = [0u8; RECORD_HEADER_LEN];
        self.read_exact_at(&mut hb, pos)?;
        let h = match decode_record_header(&hb) {
            Ok(h) => h,
            Err(HeaderError::Invalid(why)) => return Ok(Err(why)),
            Err(HeaderError::Unsupported(why)) => return Err(self.corrupt(pos, why)),
        };
        if body.checked_add(h.stored_len).is_none_or(|end| end > limit) {
            return Ok(Err("payload extends past end of data".into()));
        }
        Ok(Ok(h))
    }

    /// Read and validate a record header and return it with its stored payload.
    pub fn read_record(&self, offset: u64) -> Result<(RecordHeader, Vec<u8>)> {
        let h = self
            .parse_record_at(offset, self.len)?
            .map_err(|why| self.corrupt(offset, why))?;
        let mut payload = vec![0u8; h.stored_len as usize];
        self.read_exact_at(&mut payload, offset + RECORD_HEADER_LEN as u64)?;
        Ok((h, payload))
    }

    /// Read, decompress and verify a unit against `expect` and the stored header hash.
    pub fn read_unit(&self, offset: u64, expect: &Hash32) -> Result<Vec<u8>> {
        let (h, payload) = self.read_record(offset)?;
        let raw = compress::decode(h.compression, &payload, h.raw_len)
            .map_err(|e| self.corrupt(offset, e))?;
        let actual = Hash32::of(&raw);
        if actual != *expect || actual != h.unit_hash {
            return Err(self.corrupt(
                offset,
                format!(
                    "unit hash mismatch (computed {actual}, expected {expect}, header {})",
                    h.unit_hash
                ),
            ));
        }
        Ok(raw)
    }

    /// BLAKE3 of file bytes `[0, end)`.
    fn hash_prefix(&self, end: u64) -> Result<Hash32> {
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; 1 << 20];
        let mut at = 0u64;
        while at < end {
            let want = buf.len().min((end - at) as usize);
            self.read_exact_at(&mut buf[..want], at)?;
            hasher.update(&buf[..want]);
            at += want as u64;
        }
        Ok(Hash32(*hasher.finalize().as_bytes()))
    }

    /// Parse records sequentially from the file header up to `limit`; returns
    /// the records and the position where parsing stopped.
    fn parse_sequential(&self, limit: u64) -> Result<(Vec<(u64, RecordHeader)>, u64)> {
        let mut records = Vec::new();
        let mut pos = FILE_HEADER_LEN;
        while pos < limit {
            match self.parse_record_at(pos, limit)? {
                Ok(h) => {
                    records.push((pos, h));
                    pos += RECORD_HEADER_LEN as u64 + h.stored_len;
                }
                Err(_) => break,
            }
        }
        Ok((records, pos))
    }

    /// If the file ends with a consistent end-anchored footer, return
    /// `(footer_offset, record_count)`.
    fn find_footer(&self) -> Result<Option<(u64, u64)>> {
        if self.len < FILE_HEADER_LEN + FOOTER_FIXED_LEN {
            return Ok(None);
        }
        let mut tail = [0u8; 16];
        self.read_exact_at(&mut tail, self.len - 16)?;
        if &tail[8..] != FOOTER_MAGIC {
            return Ok(None);
        }
        let footer_offset = u64_at(&tail, 0);
        if footer_offset < FILE_HEADER_LEN || footer_offset > self.len - FOOTER_FIXED_LEN {
            return Ok(None);
        }
        let mut cb = [0u8; 8];
        self.read_exact_at(&mut cb, footer_offset)?;
        let count = u64::from_le_bytes(cb);
        // Every record takes at least a header, so this bounds the index size
        // (and the allocation for it) by the file's own size.
        if count > (footer_offset - FILE_HEADER_LEN) / RECORD_HEADER_LEN as u64 {
            return Ok(None);
        }
        let total = count
            .checked_mul(40)
            .and_then(|n| n.checked_add(FOOTER_FIXED_LEN))
            .and_then(|n| n.checked_add(footer_offset));
        if total != Some(self.len) {
            return Ok(None);
        }
        Ok(Some((footer_offset, count)))
    }
}

/// Scan a pack. A pack ending in a consistent end-anchored footer is sealed:
/// each indexed record is validated individually and damage is reported in
/// `damaged`. Otherwise it is unsealed and parsing stops at the first torn or
/// invalid record. Unsupported kinds/compressions and I/O errors are errors.
pub fn scan(path: &Path) -> Result<PackScan> {
    let r = PackReader::open(path)?;
    let bad = |why: &str| Error::Corrupt(format!("pack {}: {why}", path.display()));
    if r.len < FILE_HEADER_LEN {
        return Err(bad("file shorter than pack header"));
    }
    let mut fh = [0u8; FILE_HEADER_LEN as usize];
    r.read_exact_at(&mut fh, 0)?;
    if &fh[0..8] != FILE_MAGIC {
        return Err(bad("bad pack magic"));
    }
    let format = u16::from_le_bytes([fh[8], fh[9]]);
    if format != FORMAT {
        return Err(bad(&format!("unsupported pack format {format}")));
    }
    let mut idb = [0u8; 16];
    idb.copy_from_slice(&fh[10..26]);
    let id = Id::from_bytes(idb);

    let Some((footer_offset, count)) = r.find_footer()? else {
        let (records, pos) = r.parse_sequential(r.len)?;
        return Ok(PackScan {
            id,
            records,
            sealed: false,
            valid_len: pos,
            blake3_ok: None,
            damaged: Vec::new(),
            index_ok: false,
        });
    };

    let mut index = vec![0u8; (r.len - footer_offset) as usize];
    r.read_exact_at(&mut index, footer_offset)?;
    let (sequential, stop) = r.parse_sequential(footer_offset)?;
    let mut records = sequential.clone();
    let mut damaged = Vec::new();
    let mut index_pairs = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let e = &index[8 + 40 * i..8 + 40 * (i + 1)];
        let off = u64_at(e, 32);
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&e[..32]);
        index_pairs.push((Hash32(hash), off));
        let valid = if off < FILE_HEADER_LEN || off >= footer_offset {
            None
        } else {
            r.parse_record_at(off, footer_offset)?.ok()
        };
        match valid {
            Some(h) => records.push((off, h)),
            None => damaged.push(off),
        }
    }
    if stop != footer_offset {
        damaged.push(stop);
    }
    records.sort_unstable_by_key(|(off, _)| *off);
    records.dedup_by_key(|(off, _)| *off);
    damaged.sort_unstable();
    damaged.dedup();
    let index_ok = stop == footer_offset
        && index_pairs.len() == sequential.len()
        && index_pairs
            .iter()
            .zip(&sequential)
            .all(|((hash, off), (soff, h))| off == soff && *hash == h.unit_hash);

    let at = 8 + 40 * count as usize;
    let stored = &index[at..at + 32];
    let blake3_ok = r.hash_prefix(footer_offset)?.0[..] == *stored;
    Ok(PackScan {
        id,
        records,
        sealed: true,
        valid_len: r.len,
        blake3_ok: Some(blake3_ok),
        damaged,
        index_ok,
    })
}
