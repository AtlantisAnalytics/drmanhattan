//! Shared, thread-safe unit reads: pack handles stay open and decoded units
//! are kept in a byte-bounded LRU cache. Every unit read from a pack is
//! verified against its hash before it is returned or cached.

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::UnitRow;
use crate::pack::{pack_path, PackReader};
use crate::repo::Repo;
use lru::LruCache;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

/// Default cache budget: 256 MiB of decoded unit bytes.
pub const DEFAULT_CACHE_BYTES: usize = 256 << 20;

struct Cache {
    lru: LruCache<Hash32, Arc<Vec<u8>>>,
    /// Sum of the lengths of the cached units.
    bytes: usize,
    cap: usize,
}

impl Cache {
    fn insert(&mut self, hash: Hash32, data: Arc<Vec<u8>>) {
        if data.len() > self.cap {
            return;
        }
        self.bytes += data.len();
        if let Some(old) = self.lru.put(hash, data) {
            self.bytes -= old.len();
        }
        while self.bytes > self.cap {
            match self.lru.pop_lru() {
                Some((_, evicted)) => self.bytes -= evicted.len(),
                None => break,
            }
        }
    }
}

pub struct UnitReader {
    packs_dir: PathBuf,
    packs: Mutex<HashMap<Id, Arc<PackReader>>>,
    cache: Mutex<Cache>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl UnitReader {
    /// Units name their pack and offset, so reads need no catalog lookups;
    /// `_cat` ties the reader to the catalog its `UnitRow`s come from.
    /// `cache_bytes` bounds the decoded bytes kept cached; a unit larger
    /// than that is never cached.
    pub fn new(repo: &Repo, _cat: &Catalog, cache_bytes: usize) -> UnitReader {
        UnitReader {
            packs_dir: repo.packs_dir(),
            packs: Mutex::new(HashMap::new()),
            cache: Mutex::new(Cache {
                lru: LruCache::unbounded(),
                bytes: 0,
                cap: cache_bytes,
            }),
        }
    }

    fn pack(&self, id: Id) -> Result<Arc<PackReader>> {
        let mut packs = lock(&self.packs);
        if let Some(p) = packs.get(&id) {
            return Ok(p.clone());
        }
        let p = Arc::new(PackReader::open(&pack_path(&self.packs_dir, id))?);
        packs.insert(id, p.clone());
        Ok(p)
    }

    /// The raw bytes of `unit`, verified against its hash.
    pub fn read_unit(&self, unit: &UnitRow) -> Result<Arc<Vec<u8>>> {
        if let Some(hit) = lock(&self.cache).lru.get(&unit.hash) {
            return Ok(hit.clone());
        }
        let offset = u64::try_from(unit.pack_offset)
            .map_err(|_| Error::Corrupt(format!("unit {} has a negative offset", unit.hash)))?;
        let raw = self.pack(unit.pack_id)?.read_unit(offset, &unit.hash)?;
        if i64::try_from(raw.len()).ok() != Some(unit.raw_size) {
            return Err(Error::Corrupt(format!(
                "unit {} is {} bytes, catalog says {}",
                unit.hash,
                raw.len(),
                unit.raw_size
            )));
        }
        let raw = Arc::new(raw);
        lock(&self.cache).insert(unit.hash, raw.clone());
        Ok(raw)
    }

    /// Write `units` to `out` in order and return the BLAKE3 of all the
    /// bytes written. Write errors carry an empty path; callers that know
    /// the destination should re-wrap them.
    pub fn write_content(&self, units: &[UnitRow], out: &mut impl Write) -> Result<Hash32> {
        let mut hasher = blake3::Hasher::new();
        for u in units {
            let data = self.read_unit(u)?;
            hasher.update(&data);
            out.write_all(&data)
                .map_err(|e| Error::io(PathBuf::new(), e))?;
        }
        Ok(Hash32(*hasher.finalize().as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::Compression;
    use crate::pack::{PackWriter, RecordHeader, UnitKind};

    fn assert_send_sync<T: Send + Sync>() {}

    struct Fixture {
        _tmp: tempfile::TempDir,
        repo: Repo,
        cat: Catalog,
        units: Vec<(UnitRow, Vec<u8>)>,
    }

    /// A repo with one sealed pack holding a unit per payload.
    fn fixture(payloads: &[Vec<u8>]) -> Fixture {
        let tmp = tempfile::tempdir().unwrap();
        let repo = Repo::init(&tmp.path().join("r")).unwrap();
        let cat = Catalog::open(&repo.catalog_path()).unwrap();
        let id = Id::new_v7();
        let mut w = PackWriter::create(&repo.packs_dir(), id).unwrap();
        let mut units = Vec::new();
        for p in payloads {
            let h = RecordHeader {
                kind: UnitKind::RawChunk,
                compression: Compression::None,
                unit_hash: Hash32::of(p),
                raw_len: p.len() as u64,
                stored_len: p.len() as u64,
            };
            let offset = w.append(&h, p).unwrap();
            let row = UnitRow {
                hash: h.unit_hash,
                kind: UnitKind::RawChunk,
                raw_size: p.len() as i64,
                stored_size: p.len() as i64,
                compression: Compression::None,
                pack_id: id,
                pack_offset: offset as i64,
            };
            units.push((row, p.clone()));
        }
        w.seal().unwrap();
        Fixture {
            _tmp: tmp,
            repo,
            cat,
            units,
        }
    }

    #[test]
    fn reads_verified_units_and_streams_content() {
        assert_send_sync::<UnitReader>();
        let f = fixture(&[b"hello ".to_vec(), b"world".to_vec()]);
        let r = UnitReader::new(&f.repo, &f.cat, DEFAULT_CACHE_BYTES);
        for (row, bytes) in &f.units {
            assert_eq!(*r.read_unit(row).unwrap(), *bytes);
        }
        let rows: Vec<UnitRow> = f.units.iter().map(|(u, _)| u.clone()).collect();
        let mut out = Vec::new();
        let h = r.write_content(&rows, &mut out).unwrap();
        assert_eq!(out, b"hello world");
        assert_eq!(h, Hash32::of(b"hello world"));
    }

    #[test]
    fn hash_mismatch_is_corrupt() {
        let f = fixture(&[b"payload".to_vec()]);
        let r = UnitReader::new(&f.repo, &f.cat, DEFAULT_CACHE_BYTES);
        let mut row = f.units[0].0.clone();
        row.hash = Hash32::of(b"something else");
        assert!(matches!(r.read_unit(&row), Err(Error::Corrupt(_))));
        let mut row = f.units[0].0.clone();
        row.raw_size += 1;
        assert!(matches!(r.read_unit(&row), Err(Error::Corrupt(_))));
    }

    #[test]
    fn cache_is_bounded_by_bytes() {
        let payloads: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i; 100]).collect();
        let mut payloads = payloads;
        payloads.push(vec![9u8; 500]);
        let f = fixture(&payloads);
        let r = UnitReader::new(&f.repo, &f.cat, 250);
        for (row, _) in &f.units[..4] {
            r.read_unit(row).unwrap();
            let c = lock(&r.cache);
            assert!(c.bytes <= 250, "cache holds {} bytes", c.bytes);
        }
        {
            let c = lock(&r.cache);
            assert_eq!((c.lru.len(), c.bytes), (2, 200));
            // The two most recent units survive.
            assert!(c.lru.contains(&f.units[2].0.hash) && c.lru.contains(&f.units[3].0.hash));
        }
        // Larger than the whole budget: returned, never cached.
        let big = r.read_unit(&f.units[4].0).unwrap();
        assert_eq!(big.len(), 500);
        let c = lock(&r.cache);
        assert!(!c.lru.contains(&f.units[4].0.hash));
        assert_eq!(c.bytes, 200);
    }

    #[test]
    fn cached_hit_avoids_the_pack() {
        let f = fixture(&[b"cached".to_vec()]);
        let r = UnitReader::new(&f.repo, &f.cat, DEFAULT_CACHE_BYTES);
        let row = &f.units[0].0;
        r.read_unit(row).unwrap();
        lock(&r.packs).clear();
        std::fs::remove_file(pack_path(&f.repo.packs_dir(), row.pack_id)).unwrap();
        assert_eq!(*r.read_unit(row).unwrap(), b"cached");
    }
}
