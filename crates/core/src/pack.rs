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
use crate::display::display_path;
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
/// Bytes of a record before its payload: marker + CRC span + CRC.
pub const RECORD_HEADER_LEN: usize = 4 + CRC_SPAN + 4;
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

    pub fn parse(s: &str) -> Option<UnitKind> {
        match s {
            "raw_chunk" => Some(UnitKind::RawChunk),
            "snapshot_record" => Some(UnitKind::SnapshotRecord),
            "purge_record" => Some(UnitKind::PurgeRecord),
            _ => None,
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

pub(crate) fn fsync_dir(dir: &Path) -> Result<()> {
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
            display_path(&self.path)
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
            display_path(&self.path)
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
        match self.parse_record_classified(pos, limit)? {
            Ok(h) => Ok(Ok(h)),
            Err(HeaderError::Invalid(why)) => Ok(Err(why)),
            Err(HeaderError::Unsupported(why)) => Err(self.corrupt(pos, why)),
        }
    }

    /// Like `parse_record_at`, but an unsupported kind or compression is
    /// reported in the inner `Err` rather than as an outer error.
    fn parse_record_classified(
        &self,
        pos: u64,
        limit: u64,
    ) -> Result<std::result::Result<RecordHeader, HeaderError>> {
        let body = pos + RECORD_HEADER_LEN as u64;
        if body > limit {
            return Ok(Err(HeaderError::Invalid("truncated record header".into())));
        }
        let mut hb = [0u8; RECORD_HEADER_LEN];
        self.read_exact_at(&mut hb, pos)?;
        let h = match decode_record_header(&hb) {
            Ok(h) => h,
            Err(e) => return Ok(Err(e)),
        };
        if body.checked_add(h.stored_len).is_none_or(|end| end > limit) {
            return Ok(Err(HeaderError::Invalid(
                "payload extends past end of data".into(),
            )));
        }
        Ok(Ok(h))
    }

    /// Call `f` with the offset of every record marker in `[from, limit)`, in
    /// order. Reads `buf.len()`-byte blocks that overlap by the marker length
    /// minus one, so a marker straddling a block edge is found exactly once.
    fn for_each_marker(
        &self,
        from: u64,
        limit: u64,
        buf: &mut [u8],
        mut f: impl FnMut(u64) -> Result<()>,
    ) -> Result<()> {
        let marker = RECORD_MARKER.to_le_bytes();
        let mut at = from;
        while at + marker.len() as u64 <= limit {
            let want = buf.len().min((limit - at) as usize);
            self.read_exact_at(&mut buf[..want], at)?;
            for (i, w) in buf[..want].windows(marker.len()).enumerate() {
                if w == marker {
                    f(at + i as u64)?;
                }
            }
            if at + want as u64 >= limit {
                break;
            }
            at += (want - (marker.len() - 1)) as u64;
        }
        Ok(())
    }

    /// Read and validate the record header at `offset`, without reading its
    /// payload. The payload must still fit within the pack.
    pub fn read_header(&self, offset: u64) -> Result<RecordHeader> {
        self.parse_record_at(offset, self.len)?
            .map_err(|why| self.corrupt(offset, why))
    }

    /// Read and validate a record header and return it with its stored payload.
    pub fn read_record(&self, offset: u64) -> Result<(RecordHeader, Vec<u8>)> {
        let h = self.read_header(offset)?;
        // Lets tests count chunk payload reads (compiled out without the
        // `failpoints` feature).
        fail::fail_point!(
            "pack::read_chunk_payload",
            h.kind == UnitKind::RawChunk,
            |_| Err(self.corrupt(offset, "failpoint pack::read_chunk_payload"))
        );
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

/// Block size for the salvage resync search.
const SCAN_BLOCK: usize = 1 << 20;

/// Result of `salvage`: every record of a pack whose header still validates.
#[derive(Debug)]
#[non_exhaustive]
pub struct Salvage {
    /// From the file header; None if the header is unreadable (file skipped).
    pub id: Option<Id>,
    /// (offset, header) of every candidate record: each offset whose header
    /// validates, in strictly increasing offset order. Candidates may overlap
    /// and may include false positives (a header embedded in some payload), so
    /// the caller must verify each payload against its hash. Only verified
    /// records establish what is intact; see `coverage_gaps`.
    pub records: Vec<(u64, RecordHeader)>,
    /// Consistent end-anchored footer whose BLAKE3 matches the bytes before it.
    pub sealed_blake3: Option<Hash32>,
    /// End of the data region: the footer offset when a consistent footer
    /// exists, else the file length.
    pub data_end: u64,
    /// Greatest end offset of any candidate (26 if none).
    pub valid_end: u64,
    /// `(offset, unit hash)` of every entry in a consistent footer's index,
    /// in index order; empty without one. Whether each entry names a valid
    /// record is reported in `problems`.
    pub index: Vec<(u64, Hash32)>,
    /// Structural problems only: unreadable header, id/name mismatch,
    /// unsupported kinds, index entries without a valid record, an
    /// inconsistent footer, a footer hash mismatch. Damage is not judged here.
    pub problems: Vec<String>,
}

/// Find every candidate record in a pack: parse sequentially while records
/// validate, then test every record marker in the rest of the data region.
/// Headers (marker, CRC, size invariants, payload fits) are validated, payloads
/// are not read. Errs only on I/O errors.
pub fn salvage(path: &Path) -> Result<Salvage> {
    let shown = display_path(path);
    let r = PackReader::open(path)?;
    let mut out = Salvage {
        id: None,
        records: Vec::new(),
        sealed_blake3: None,
        data_end: FILE_HEADER_LEN,
        valid_end: FILE_HEADER_LEN,
        index: Vec::new(),
        problems: Vec::new(),
    };
    let unreadable = |out: &mut Salvage, why: String| {
        out.problems
            .push(format!("pack {shown}: {why}; skipping file"));
    };
    if r.len < FILE_HEADER_LEN {
        unreadable(&mut out, "file shorter than pack header".into());
        return Ok(out);
    }
    let mut fh = [0u8; FILE_HEADER_LEN as usize];
    r.read_exact_at(&mut fh, 0)?;
    if &fh[0..8] != FILE_MAGIC {
        unreadable(&mut out, "bad pack magic".into());
        return Ok(out);
    }
    let format = u16::from_le_bytes([fh[8], fh[9]]);
    if format != FORMAT {
        unreadable(&mut out, format!("unsupported pack format {format}"));
        return Ok(out);
    }
    let mut idb = [0u8; 16];
    idb.copy_from_slice(&fh[10..26]);
    let id = Id::from_bytes(idb);
    out.id = Some(id);
    if path.file_stem().and_then(|s| s.to_str()) != Some(id.to_string().as_str()) {
        out.problems.push(format!(
            "pack {shown}: header id {id} does not match the file name"
        ));
    }

    let footer = r.find_footer()?;
    let data_end = footer.map_or(r.len, |(off, _)| off);
    out.data_end = data_end;

    // Offsets of records with a valid CRC but an unsupported kind (already reported).
    let mut unsupported: Vec<u64> = Vec::new();
    let mut pos = FILE_HEADER_LEN;
    while pos < data_end {
        match r.parse_record_classified(pos, data_end)? {
            Ok(h) => {
                out.records.push((pos, h));
                pos += RECORD_HEADER_LEN as u64 + h.stored_len;
                out.valid_end = out.valid_end.max(pos);
            }
            Err(e) => {
                if let HeaderError::Unsupported(why) = e {
                    out.problems
                        .push(format!("pack {shown} at offset {pos}: {why}; skipped"));
                    unsupported.push(pos);
                }
                break;
            }
        }
    }
    if pos < data_end {
        // The sequential parse stopped early. From here, test every marker
        // and keep every header that validates, without skipping over spans:
        // a header embedded in a damaged or valid payload must not hide a
        // genuine record behind it.
        let mut buf = vec![0u8; SCAN_BLOCK];
        r.for_each_marker(pos + 1, data_end, &mut buf, |off| {
            match r.parse_record_classified(off, data_end)? {
                Ok(h) => {
                    out.records.push((off, h));
                    out.valid_end = out
                        .valid_end
                        .max(off + RECORD_HEADER_LEN as u64 + h.stored_len);
                }
                Err(HeaderError::Unsupported(why)) => {
                    out.problems
                        .push(format!("pack {shown} at offset {off}: {why}; skipped"));
                    unsupported.push(off);
                }
                Err(HeaderError::Invalid(_)) => {}
            }
            Ok(())
        })?;
    }
    if footer.is_none() && r.len >= FILE_HEADER_LEN + 8 {
        let mut tail = [0u8; 8];
        r.read_exact_at(&mut tail, r.len - 8)?;
        if &tail == FOOTER_MAGIC {
            out.problems.push(format!(
                "pack {shown}: footer at end of pack is inconsistent"
            ));
        }
    }

    if let Some((footer_offset, count)) = footer {
        let mut index = vec![0u8; (r.len - footer_offset) as usize];
        r.read_exact_at(&mut index, footer_offset)?;
        // As `scan` does, also try every indexed offset. The marker scan
        // already tests these, so this is cheap insurance.
        let mut extra = Vec::new();
        for i in 0..count as usize {
            let off = u64_at(&index[8 + 40 * i..8 + 40 * (i + 1)], 32);
            if off >= FILE_HEADER_LEN
                && off < footer_offset
                && out.records.binary_search_by_key(&off, |(o, _)| *o).is_err()
            {
                match r.parse_record_classified(off, footer_offset)? {
                    Ok(h) => {
                        extra.push((off, h));
                        out.valid_end = out
                            .valid_end
                            .max(off + RECORD_HEADER_LEN as u64 + h.stored_len);
                    }
                    Err(HeaderError::Unsupported(why)) if !unsupported.contains(&off) => {
                        out.problems
                            .push(format!("pack {shown} at offset {off}: {why}; skipped"));
                        unsupported.push(off);
                    }
                    Err(_) => {}
                }
            }
        }
        if !extra.is_empty() {
            out.records.extend(extra);
            out.records.sort_unstable_by_key(|(off, _)| *off);
            out.records.dedup_by_key(|(off, _)| *off);
        }
        for i in 0..count as usize {
            let e = &index[8 + 40 * i..8 + 40 * (i + 1)];
            let off = u64_at(e, 32);
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&e[..32]);
            out.index.push((off, Hash32(hash)));
            let ok = out
                .records
                .binary_search_by_key(&off, |(o, _)| *o)
                .is_ok_and(|k| out.records[k].1.unit_hash.0[..] == e[..32]);
            if !ok && !unsupported.contains(&off) {
                out.problems.push(format!(
                    "pack {shown}: index entry for offset {off} has no valid record"
                ));
            }
        }
        let at = 8 + 40 * count as usize;
        let mut stored = [0u8; 32];
        stored.copy_from_slice(&index[at..at + 32]);
        if r.hash_prefix(footer_offset)?.0 == stored {
            out.sealed_blake3 = Some(Hash32(stored));
        } else {
            out.problems.push(format!(
                "pack {shown}: footer BLAKE3 does not match the pack contents"
            ));
        }
    }
    Ok(out)
}

/// Byte ranges of a pack's data region not covered by verified records.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Gaps {
    /// Uncovered `[start, end)` ranges: damage.
    pub damaged: Vec<(u64, u64)>,
    /// Unsealed packs only: where a final uncovered range reaching the end of
    /// the data begins. That is a crash leftover, not damage.
    pub torn_tail: Option<u64>,
}

/// Compute the uncovered ranges of `[data_start, data_end)`. `verified` holds
/// `(offset, total record length including the header)` of records whose
/// header CRC and payload hash were both checked, so a verified span never
/// contains damaged bytes and every gap in their union is exact. Spans may
/// overlap and need not be sorted. On an unsealed pack a final gap reaching
/// `data_end` is a `torn_tail`; on a sealed pack every gap is damage.
pub fn coverage_gaps(
    verified: &[(u64, u64)],
    data_start: u64,
    data_end: u64,
    sealed: bool,
) -> Gaps {
    let mut spans: Vec<(u64, u64)> = verified
        .iter()
        .map(|&(off, len)| (off, off.saturating_add(len)))
        .collect();
    spans.sort_unstable();
    let mut gaps = Gaps {
        damaged: Vec::new(),
        torn_tail: None,
    };
    let mut cursor = data_start;
    for (start, end) in spans {
        if cursor >= data_end {
            break;
        }
        if start > cursor {
            gaps.damaged.push((cursor, start.min(data_end)));
        }
        cursor = cursor.max(end);
    }
    if cursor < data_end {
        if sealed {
            gaps.damaged.push((cursor, data_end));
        } else {
            gaps.torn_tail = Some(cursor);
        }
    }
    gaps
}

/// Scan a pack. A pack ending in a consistent end-anchored footer is sealed:
/// each indexed record is validated individually and damage is reported in
/// `damaged`. Otherwise it is unsealed and parsing stops at the first torn or
/// invalid record. Unsupported kinds/compressions and I/O errors are errors.
pub fn scan(path: &Path) -> Result<PackScan> {
    let r = PackReader::open(path)?;
    let bad = |why: &str| Error::Corrupt(format!("pack {}: {why}", display_path(path)));
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

#[cfg(test)]
mod salvage_tests {
    use super::*;

    fn raw(payload: &[u8]) -> (RecordHeader, Vec<u8>) {
        (
            RecordHeader {
                kind: UnitKind::RawChunk,
                compression: Compression::None,
                unit_hash: Hash32::of(payload),
                raw_len: payload.len() as u64,
                stored_len: payload.len() as u64,
            },
            payload.to_vec(),
        )
    }

    /// A pack of five records; returns its path, id and record offsets.
    fn build(dir: &Path, seal: bool) -> (PathBuf, Id, Vec<u64>) {
        let id = Id::new_v7();
        let mut w = PackWriter::create(dir, id).unwrap();
        let mut offs = Vec::new();
        for i in 0..5u8 {
            let (h, p) = raw(&vec![i + 1; 100 + i as usize]);
            offs.push(w.append(&h, &p).unwrap());
        }
        if seal {
            w.seal().unwrap();
        } else {
            w.sync().unwrap();
        }
        (pack_path(dir, id), id, offs)
    }

    fn corrupt_crc(path: &Path, record_offset: u64) {
        let mut b = std::fs::read(path).unwrap();
        b[record_offset as usize + 54] ^= 0xFF;
        std::fs::write(path, b).unwrap();
    }

    fn found(s: &Salvage) -> Vec<u64> {
        s.records.iter().map(|(o, _)| *o).collect()
    }

    #[test]
    fn salvage_intact_sealed_pack_matches_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, id, _) = build(tmp.path(), true);
        let s = salvage(&path).unwrap();
        let sc = scan(&path).unwrap();
        assert_eq!(s.id, Some(id));
        assert_eq!(s.records, sc.records);
        let bytes = std::fs::read(&path).unwrap();
        let stored = Hash32(
            bytes[bytes.len() - 48..bytes.len() - 16]
                .try_into()
                .unwrap(),
        );
        assert_eq!(s.sealed_blake3, Some(stored));
        assert!(s.problems.is_empty(), "{:?}", s.problems);
    }

    #[test]
    fn salvage_finds_records_after_mid_pack_damage_unsealed() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, _, offs) = build(tmp.path(), false);
        corrupt_crc(&path, offs[2]);
        let s = salvage(&path).unwrap();
        assert_eq!(found(&s), vec![offs[0], offs[1], offs[3], offs[4]]);
        // Damage is judged from verified payloads (`coverage_gaps`), not here.
        assert!(s.problems.is_empty(), "{:?}", s.problems);
        assert_eq!(s.sealed_blake3, None);
    }

    #[test]
    fn salvage_finds_records_after_mid_pack_damage_sealed() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, _, offs) = build(tmp.path(), true);
        corrupt_crc(&path, offs[2]);
        let s = salvage(&path).unwrap();
        assert_eq!(found(&s), vec![offs[0], offs[1], offs[3], offs[4]]);
        assert_eq!(s.sealed_blake3, None);
        assert!(!s.problems.is_empty());
    }

    #[test]
    fn salvage_skips_unknown_kind_with_valid_crc() {
        let tmp = tempfile::tempdir().unwrap();
        let id = Id::new_v7();
        let mut w = PackWriter::create(tmp.path(), id).unwrap();
        let (h, p) = raw(b"first");
        let o1 = w.append(&h, &p).unwrap();
        w.sync().unwrap();
        let path = pack_path(tmp.path(), id);
        drop(w);
        let (h2, p2) = raw(b"second");
        let mut bad = encode_record_header(&h2);
        bad[4] = 9;
        let crc = crc32c::crc32c(&bad[4..4 + CRC_SPAN]);
        bad[54..58].copy_from_slice(&crc.to_le_bytes());
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&bad).unwrap();
        f.write_all(&p2).unwrap();
        let o3 = std::fs::metadata(&path).unwrap().len();
        f.write_all(&encode_record_header(&h2)).unwrap();
        f.write_all(&p2).unwrap();
        let s = salvage(&path).unwrap();
        assert_eq!(found(&s), vec![o1, o3]);
        assert_eq!(s.problems.len(), 1, "{:?}", s.problems);
        assert!(s.problems[0].contains("kind 9"), "{:?}", s.problems);
    }

    #[test]
    fn salvage_sealed_pack_with_unknown_kind_still_salvages() {
        let tmp = tempfile::tempdir().unwrap();
        let id = Id::new_v7();
        let mut w = PackWriter::create(tmp.path(), id).unwrap();
        let (h, p) = raw(b"first");
        let o1 = w.append(&h, &p).unwrap();
        // A valid-CRC record of an unknown kind, listed in the index.
        let (h2, p2) = raw(b"second");
        let mut bad = encode_record_header(&h2);
        bad[4] = 9;
        let crc = crc32c::crc32c(&bad[4..4 + CRC_SPAN]);
        bad[54..58].copy_from_slice(&crc.to_le_bytes());
        let ob = w.len();
        w.write_hashed(&bad).unwrap();
        w.write_hashed(&p2).unwrap();
        w.entries.push((h2.unit_hash, ob));
        let o3 = w.append(&h2, &p2).unwrap();
        w.seal().unwrap();
        let s = salvage(&pack_path(tmp.path(), id)).unwrap();
        assert_eq!(found(&s), vec![o1, o3]);
        assert_eq!(s.problems.len(), 1, "{:?}", s.problems);
        assert!(s.problems[0].contains("kind 9"), "{:?}", s.problems);
        assert!(s.sealed_blake3.is_some());
    }

    #[test]
    fn salvage_sealed_finds_record_stepped_over_by_embedded_header() {
        // The damaged record embeds a valid header whose payload ends exactly at
        // the record after the next real one R1; a chain that skipped spans would
        // step over R1. Every marker is tested, so R1 is still found.
        let tmp = tempfile::tempdir().unwrap();
        let id = Id::new_v7();
        let mut w = PackWriter::create(tmp.path(), id).unwrap();
        let (h, p) = raw(b"a");
        w.append(&h, &p).unwrap();
        let damaged_at = w.len();
        let r1_total = RECORD_HEADER_LEN as u64 + 100;
        let stored = 40 + r1_total;
        let embedded = RecordHeader {
            kind: UnitKind::RawChunk,
            compression: Compression::None,
            unit_hash: Hash32::of(b"embedded"),
            raw_len: stored,
            stored_len: stored,
        };
        let mut payload = vec![0u8; 10];
        payload.extend_from_slice(&encode_record_header(&embedded));
        payload.extend_from_slice(&[0u8; 40]);
        let (hd, pd) = raw(&payload);
        w.append(&hd, &pd).unwrap();
        let (h1, p1) = raw(&[7u8; 100]);
        let o1 = w.append(&h1, &p1).unwrap();
        let (h2, p2) = raw(&[8u8; 50]);
        let o2 = w.append(&h2, &p2).unwrap();
        w.seal().unwrap();
        let path = pack_path(tmp.path(), id);
        corrupt_crc(&path, damaged_at);
        let s = salvage(&path).unwrap();
        let offs = found(&s);
        assert!(offs.contains(&o1) && offs.contains(&o2), "{offs:?}");
        assert!(offs.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn salvage_inconsistent_footer_is_a_problem() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, _, offs) = build(tmp.path(), true);
        let mut b = std::fs::read(&path).unwrap();
        let footer_offset = u64::from_le_bytes(b[b.len() - 16..b.len() - 8].try_into().unwrap());
        b[footer_offset as usize] ^= 0xFF; // footer count
        std::fs::write(&path, b).unwrap();
        let s = salvage(&path).unwrap();
        assert_eq!(found(&s).len(), offs.len());
        assert!(
            s.problems.iter().any(|p| p.contains("footer")),
            "{:?}",
            s.problems
        );
        assert_eq!(s.sealed_blake3, None);
    }

    #[test]
    fn salvage_torn_tail_sets_valid_end() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, _, offs) = build(tmp.path(), false);
        let end = std::fs::metadata(&path).unwrap().len();
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0xAB; 30]).unwrap();
        let s = salvage(&path).unwrap();
        assert_eq!(found(&s).len(), offs.len());
        assert_eq!(s.valid_end, end);
        assert_eq!(s.data_end, end + 30);
        assert!(s.problems.is_empty(), "{:?}", s.problems);
        let gaps = coverage_gaps(&verified(&path, &s), FILE_HEADER_LEN, s.data_end, false);
        assert_eq!(
            gaps,
            Gaps {
                damaged: vec![],
                torn_tail: Some(end)
            }
        );
    }

    #[test]
    fn salvage_resync_does_not_shadow_a_real_record() {
        // A damaged record embeds a valid header whose payload reaches past the
        // next real record's header; the real record must still be found.
        let tmp = tempfile::tempdir().unwrap();
        let id = Id::new_v7();
        let mut w = PackWriter::create(tmp.path(), id).unwrap();
        let (h, p) = raw(b"a");
        let oa = w.append(&h, &p).unwrap();
        let damaged_at = w.len();
        let inner_in_d = 40u64; // bytes of the embedded record's payload inside D
        let stored = inner_in_d + RECORD_HEADER_LEN as u64 + 2;
        let embedded = RecordHeader {
            kind: UnitKind::RawChunk,
            compression: Compression::None,
            unit_hash: Hash32::of(b"embedded"),
            raw_len: stored,
            stored_len: stored,
        };
        let mut payload = vec![0u8; 10];
        payload.extend_from_slice(&encode_record_header(&embedded));
        payload.extend_from_slice(&[0u8; 40]);
        let (hd, pd) = raw(&payload);
        w.append(&hd, &pd).unwrap();
        let (hr, pr) = raw(&[7u8; 100]);
        let or = w.append(&hr, &pr).unwrap();
        w.sync().unwrap();
        let path = pack_path(tmp.path(), id);
        corrupt_crc(&path, damaged_at);
        let s = salvage(&path).unwrap();
        let offs = found(&s);
        assert!(offs.contains(&oa) && offs.contains(&or), "{offs:?}");
    }

    struct Layout {
        path: PathBuf,
        x: u64,
        p: u64,
        d: u64,
        r: u64,
        r2_end: u64,
    }

    /// Unsealed pack: A, X (CRC-corrupt), P (intact; its payload embeds a valid
    /// header E whose span runs past D into R), D (CRC-corrupt), R, R2.
    fn overshoot_layout(dir: &Path) -> Layout {
        let id = Id::new_v7();
        let mut w = PackWriter::create(dir, id).unwrap();
        let (h, p) = raw(b"a");
        w.append(&h, &p).unwrap();
        let (hx, px) = raw(&[3u8; 40]);
        let x = w.append(&hx, &px).unwrap();
        let (m, d_payload) = (30u64, 64u64);
        let d_total = RECORD_HEADER_LEN as u64 + d_payload;
        let stored = m + d_total + RECORD_HEADER_LEN as u64 + 20;
        let embedded = RecordHeader {
            kind: UnitKind::RawChunk,
            compression: Compression::None,
            unit_hash: Hash32::of(b"embedded"),
            raw_len: stored,
            stored_len: stored,
        };
        let mut payload = vec![0u8; 10];
        payload.extend_from_slice(&encode_record_header(&embedded));
        payload.extend_from_slice(&vec![0u8; m as usize]);
        let (hp, pp) = raw(&payload);
        let p_off = w.append(&hp, &pp).unwrap();
        let (hd, pd) = raw(&vec![5u8; d_payload as usize]);
        let d = w.append(&hd, &pd).unwrap();
        let (hr, pr) = raw(&[7u8; 100]);
        let r = w.append(&hr, &pr).unwrap();
        let (h2, p2) = raw(&[8u8; 50]);
        let r2 = w.append(&h2, &p2).unwrap();
        w.sync().unwrap();
        let path = pack_path(dir, id);
        corrupt_crc(&path, x);
        corrupt_crc(&path, d);
        Layout {
            path,
            x,
            p: p_off,
            d,
            r,
            r2_end: r2 + RECORD_HEADER_LEN as u64 + 50,
        }
    }

    /// (offset, total length) of every candidate whose payload really verifies.
    fn verified(path: &Path, s: &Salvage) -> Vec<(u64, u64)> {
        let rd = PackReader::open(path).unwrap();
        s.records
            .iter()
            .filter(|(off, h)| rd.read_unit(*off, &h.unit_hash).is_ok())
            .map(|(off, h)| (*off, RECORD_HEADER_LEN as u64 + h.stored_len))
            .collect()
    }

    #[test]
    fn salvage_finds_real_records_behind_overshooting_embedded_header() {
        let tmp = tempfile::tempdir().unwrap();
        let l = overshoot_layout(tmp.path());
        let s = salvage(&l.path).unwrap();
        let offs = found(&s);
        assert!(offs.contains(&l.r), "{offs:?}");
        assert!(offs.contains(&(l.r2_end - RECORD_HEADER_LEN as u64 - 50)));
        assert!(offs.windows(2).all(|w| w[0] < w[1]));
        assert!(s.problems.is_empty(), "{:?}", s.problems);
    }

    #[test]
    fn coverage_gaps_end_to_end_reports_exactly_the_damage() {
        let tmp = tempfile::tempdir().unwrap();
        let l = overshoot_layout(tmp.path());
        let s = salvage(&l.path).unwrap();
        let gaps = coverage_gaps(&verified(&l.path, &s), FILE_HEADER_LEN, s.data_end, false);
        assert_eq!(gaps.damaged, vec![(l.x, l.p), (l.d, l.r)]);
        assert_eq!(gaps.torn_tail, None);
        assert!(gaps.damaged.iter().all(|&(_, e)| e <= l.r));
        assert_eq!(s.data_end, l.r2_end);
    }

    #[test]
    fn coverage_gaps_cases() {
        let g = |v: &[(u64, u64)], sealed| coverage_gaps(v, 26, 200, sealed);
        let none = Gaps {
            damaged: vec![],
            torn_tail: None,
        };
        // No gaps, with overlapping and unsorted spans.
        assert_eq!(g(&[(100, 100), (26, 80), (60, 60)], true), none);
        assert_eq!(g(&[(26, 174)], false), none);
        // Mid-file gap on an unsealed pack is damage.
        assert_eq!(
            g(&[(26, 34), (100, 100)], false),
            Gaps {
                damaged: vec![(60, 100)],
                torn_tail: None
            }
        );
        // Final gap: torn tail when unsealed, damage when sealed.
        assert_eq!(
            g(&[(26, 74)], false),
            Gaps {
                damaged: vec![],
                torn_tail: Some(100)
            }
        );
        assert_eq!(
            g(&[(26, 74)], true),
            Gaps {
                damaged: vec![(100, 200)],
                torn_tail: None
            }
        );
        // Nothing verified: the whole region is a gap.
        assert_eq!(
            g(&[], true),
            Gaps {
                damaged: vec![(26, 200)],
                torn_tail: None
            }
        );
    }

    #[test]
    fn salvage_reports_id_name_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, id, offs) = build(tmp.path(), true);
        let other = tmp.path().join(format!("{}.pack", Id::new_v7()));
        std::fs::rename(&path, &other).unwrap();
        let s = salvage(&other).unwrap();
        assert_eq!(s.id, Some(id));
        assert_eq!(found(&s).len(), offs.len());
        assert!(
            s.problems.iter().any(|p| p.contains("name")),
            "{:?}",
            s.problems
        );
    }

    #[test]
    fn salvage_bad_magic_skips_file() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, _, _) = build(tmp.path(), false);
        let mut b = std::fs::read(&path).unwrap();
        b[0] ^= 0xFF;
        std::fs::write(&path, b).unwrap();
        let s = salvage(&path).unwrap();
        assert_eq!(s.id, None);
        assert!(s.records.is_empty());
        assert_eq!(s.problems.len(), 1);
    }

    #[test]
    fn salvage_resyncs_across_block_boundary() {
        // A damaged record whose payload embeds a valid record whose marker
        // straddles the end of the first SCAN_BLOCK-byte search block.
        let tmp = tempfile::tempdir().unwrap();
        let id = Id::new_v7();
        let mut w = PackWriter::create(tmp.path(), id).unwrap();
        let (h, p) = raw(b"a");
        let oa = w.append(&h, &p).unwrap();
        let damaged_at = w.len();
        let (hz, pz) = raw(b"z");
        let mut payload = vec![0u8; SCAN_BLOCK - 59];
        payload.extend_from_slice(&encode_record_header(&hz));
        payload.extend_from_slice(&pz);
        let (ho, po) = raw(&payload);
        w.append(&ho, &po).unwrap();
        w.sync().unwrap();
        let path = pack_path(tmp.path(), id);
        corrupt_crc(&path, damaged_at);
        let s = salvage(&path).unwrap();
        assert_eq!(found(&s), vec![oa, damaged_at + SCAN_BLOCK as u64 - 1]);
    }
}
