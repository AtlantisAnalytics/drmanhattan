//! Self-describing pack files (spec §6.2). All integers are little-endian.
//!
//! ```text
//! header : "WMKPACK\0" (8) | format u16 = 1 | pack_id (16)              = 26 bytes
//! record : marker u32 = 0x574D4B52 | kind u8 | compression u8 | unit_hash (32)
//!          | raw_len u64 | stored_len u64 | header_crc32c u32 | payload
//! footer : count u64 | count x (unit_hash 32 | offset u64) | pack_blake3 (32) | "WMKPEND\0"
//! ```
//!
//! The CRC32C covers the 50 bytes from `kind` through `stored_len`.

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
    /// (offset of the record marker, header) for each valid record, in order.
    pub records: Vec<(u64, RecordHeader)>,
    pub sealed: bool,
    /// End of the valid prefix: the file length if sealed, else the offset
    /// where the first invalid record (or torn tail) begins.
    pub valid_len: u64,
    /// `Some` only when sealed: whether the stored pack hash matches.
    pub blake3_ok: Option<bool>,
}

/// `<packs_dir>/<last 2 hex chars of id>/<id hyphenated>.pack`
pub fn pack_path(packs_dir: &Path, id: Id) -> PathBuf {
    let s = id.to_string();
    packs_dir.join(&s[s.len() - 2..]).join(format!("{s}.pack"))
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

/// Parse and validate a record header. `Err(reason)` on bad marker, CRC, kind or compression.
fn decode_record_header(b: &[u8; RECORD_HEADER_LEN]) -> std::result::Result<RecordHeader, String> {
    let marker = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
    if marker != RECORD_MARKER {
        return Err(format!("bad record marker {marker:#010x}"));
    }
    let crc = u32::from_le_bytes([b[54], b[55], b[56], b[57]]);
    if crc != crc32c::crc32c(&b[4..4 + CRC_SPAN]) {
        return Err("record header CRC mismatch".into());
    }
    let kind = UnitKind::from_u8(b[4]).ok_or_else(|| format!("unknown unit kind {}", b[4]))?;
    let compression =
        Compression::from_u8(b[5]).ok_or_else(|| format!("unknown compression {}", b[5]))?;
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&b[6..38]);
    Ok(RecordHeader {
        kind,
        compression,
        unit_hash: Hash32(hash),
        raw_len: u64_at(b, 38),
        stored_len: u64_at(b, 46),
    })
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
}

impl PackWriter {
    /// Create a new pack file (failing if it already exists) and write its header.
    pub fn create(packs_dir: &Path, id: Id) -> Result<PackWriter> {
        let path = pack_path(packs_dir, id);
        let parent = path.parent().unwrap_or(packs_dir).to_path_buf();
        std::fs::create_dir_all(&parent).map_err(|e| Error::io(&parent, e))?;
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
        };
        w.write_hashed(&header)?;
        fsync_dir(&parent)?;
        Ok(w)
    }

    fn write_hashed(&mut self, bytes: &[u8]) -> Result<()> {
        self.file
            .write_all(bytes)
            .map_err(|e| Error::io(&self.path, e))?;
        self.hasher.update(bytes);
        self.len += bytes.len() as u64;
        Ok(())
    }

    /// Append a record; returns the offset of its marker.
    pub fn append(&mut self, h: &RecordHeader, payload: &[u8]) -> Result<u64> {
        if h.stored_len != payload.len() as u64 {
            return Err(Error::Corrupt(format!(
                "stored_len {} != payload length {}",
                h.stored_len,
                payload.len()
            )));
        }
        let offset = self.len;
        self.write_hashed(&encode_record_header(h))?;
        self.write_hashed(payload)?;
        self.entries.push((h.unit_hash, offset));
        Ok(offset)
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all().map_err(|e| Error::io(&self.path, e))
    }

    /// Write the footer, then fsync the file and its parent directory.
    pub fn seal(mut self) -> Result<SealedPack> {
        let blake3 = Hash32(*self.hasher.finalize().as_bytes());
        let mut footer = Vec::with_capacity(8 + 40 * self.entries.len() + 40);
        footer.extend_from_slice(&(self.entries.len() as u64).to_le_bytes());
        for (hash, off) in &self.entries {
            footer.extend_from_slice(&hash.0);
            footer.extend_from_slice(&off.to_le_bytes());
        }
        footer.extend_from_slice(&blake3.0);
        footer.extend_from_slice(FOOTER_MAGIC);
        self.file
            .write_all(&footer)
            .map_err(|e| Error::io(&self.path, e))?;
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

    /// Read and validate a record header and return it with its stored payload.
    pub fn read_record(&self, offset: u64) -> Result<(RecordHeader, Vec<u8>)> {
        let mut hb = [0u8; RECORD_HEADER_LEN];
        self.read_exact_at(&mut hb, offset)?;
        let h = decode_record_header(&hb).map_err(|why| self.corrupt(offset, why))?;
        let start = offset + RECORD_HEADER_LEN as u64;
        if start
            .checked_add(h.stored_len)
            .is_none_or(|end| end > self.len)
        {
            return Err(self.corrupt(offset, "payload extends past end of file"));
        }
        let mut payload = vec![0u8; h.stored_len as usize];
        self.read_exact_at(&mut payload, start)?;
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
}

/// Scan a pack from the start, stopping at the first record that cannot be parsed,
/// then check whether what remains is a valid footer.
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

    let mut records = Vec::new();
    let mut pos = FILE_HEADER_LEN;
    loop {
        let mut hb = [0u8; RECORD_HEADER_LEN];
        if pos + RECORD_HEADER_LEN as u64 > r.len || r.file.read_exact_at(&mut hb, pos).is_err() {
            break;
        }
        let Ok(h) = decode_record_header(&hb) else {
            break;
        };
        let end = pos + RECORD_HEADER_LEN as u64;
        match end.checked_add(h.stored_len) {
            Some(next) if next <= r.len => {
                records.push((pos, h));
                pos = next;
            }
            _ => break,
        }
    }

    let unsealed = |valid_len| PackScan {
        id,
        records: Vec::new(),
        sealed: false,
        valid_len,
        blake3_ok: None,
    };
    let Some(blake3_ok) = check_footer(&r, pos, &records)? else {
        return Ok(PackScan {
            records,
            ..unsealed(pos)
        });
    };
    Ok(PackScan {
        id,
        records,
        sealed: true,
        valid_len: r.len,
        blake3_ok: Some(blake3_ok),
    })
}

/// If a valid footer starts at `pos`, return whether the pack hash matches.
fn check_footer(r: &PackReader, pos: u64, records: &[(u64, RecordHeader)]) -> Result<Option<bool>> {
    let remaining = r.len - pos;
    if remaining < 8 + 32 + 8 {
        return Ok(None);
    }
    let mut cb = [0u8; 8];
    r.read_exact_at(&mut cb, pos)?;
    let count = u64::from_le_bytes(cb);
    if count != records.len() as u64 {
        return Ok(None);
    }
    let expected = 40u64
        .checked_mul(count)
        .and_then(|n| n.checked_add(8 + 32 + 8));
    if expected != Some(remaining) {
        return Ok(None);
    }
    let mut footer = vec![0u8; remaining as usize];
    r.read_exact_at(&mut footer, pos)?;
    let n = records.len();
    if &footer[footer.len() - 8..] != FOOTER_MAGIC {
        return Ok(None);
    }
    for (i, (off, h)) in records.iter().enumerate() {
        let e = &footer[8 + 40 * i..8 + 40 * (i + 1)];
        if e[..32] != h.unit_hash.0 || u64_at(e, 32) != *off {
            return Ok(None);
        }
    }
    let stored = &footer[8 + 40 * n..8 + 40 * n + 32];

    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut at = 0u64;
    while at < pos {
        let want = buf.len().min((pos - at) as usize);
        r.read_exact_at(&mut buf[..want], at)?;
        hasher.update(&buf[..want]);
        at += want as u64;
    }
    Ok(Some(hasher.finalize().as_bytes() == stored))
}
