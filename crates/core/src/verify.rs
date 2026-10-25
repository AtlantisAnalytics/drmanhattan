//! Repository verification (spec §9). A damaged repository is reported as
//! `problems`, never as an `Err`; `Err` is reserved for failures to even
//! look (e.g. the catalog will not open).
//!
//! `quick` checks, for every unit a selected complete snapshot references
//! (its contents' units, its record unit, and the chunks the record index
//! names): the pack file exists, the record header at the unit's offset
//! parses, and its hash and lengths equal the unit row; plus the snapshot's
//! stored manifest hashes. Full adds decompress-and-hash of every unit, the
//! whole-pack BLAKE3 of each sealed pack touched, and a full read of the
//! snapshot record.

use std::collections::HashSet;

use crate::catalog::Catalog;
use crate::display::display_path;
use crate::error::Result;
use crate::ids::{Hash32, Id};
use crate::ingest::record;
use crate::manifest::{manifest_snapshot, ManifestOptions};
use crate::model::{SnapshotRow, SnapshotStatus};
use crate::pack::{pack_path, scan};
use crate::repo::Repo;
use crate::unitread::UnitReader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyReport {
    pub snapshots: u64,
    pub units_checked: u64,
    pub packs_checked: u64,
    /// Empty means the repository verified OK.
    pub problems: Vec<String>,
}

struct Checker<'a> {
    repo: &'a Repo,
    cat: &'a Catalog,
    quick: bool,
    /// Also the only source of pack handles, so verify holds at most its
    /// bounded number open.
    reader: UnitReader,
    seen: HashSet<Hash32>,
    touched: HashSet<Id>,
    problems: Vec<String>,
}

impl Checker<'_> {
    /// Check one unit by hash, once per run.
    fn unit(&mut self, snap: Id, hash: &Hash32) {
        if !self.seen.insert(*hash) {
            return;
        }
        let row = match self.cat.unit(hash) {
            Ok(r) => r,
            Err(e) => {
                self.problems
                    .push(format!("snapshot {snap}: unit {hash}: {e}"));
                return;
            }
        };
        self.touched.insert(row.pack_id);
        let loc = format!(
            "pack {} offset {}",
            display_path(&pack_path(&self.repo.packs_dir(), row.pack_id)),
            row.pack_offset
        );
        let offset = match u64::try_from(row.pack_offset) {
            Ok(o) => o,
            Err(_) => {
                self.problems.push(format!(
                    "snapshot {snap}: unit {hash}: negative offset ({loc})"
                ));
                return;
            }
        };
        let header = match self.reader.pack(row.pack_id) {
            Err(e) => {
                let path = pack_path(&self.repo.packs_dir(), row.pack_id);
                self.problems.push(format!(
                    "snapshot {snap}: unit {hash}: pack {} unreadable: {e}",
                    display_path(&path)
                ));
                return;
            }
            Ok(p) => p.read_record(offset).map(|(h, _)| h),
        };
        let h = match header {
            Ok(h) => h,
            Err(e) => {
                self.problems
                    .push(format!("snapshot {snap}: unit {hash}: {e} ({loc})"));
                return;
            }
        };
        if h.unit_hash != row.hash
            || i64::try_from(h.raw_len).ok() != Some(row.raw_size)
            || i64::try_from(h.stored_len).ok() != Some(row.stored_size)
        {
            self.problems.push(format!(
                "snapshot {snap}: unit {hash}: record header (hash {}, raw {}, stored {}) \
                 disagrees with catalog (raw {}, stored {}) ({loc})",
                h.unit_hash, h.raw_len, h.stored_len, row.raw_size, row.stored_size
            ));
            return;
        }
        if !self.quick {
            if let Err(e) = self.reader.read_unit(&row) {
                self.problems
                    .push(format!("snapshot {snap}: unit {hash}: {e} ({loc})"));
            }
        }
    }

    fn snapshot(&mut self, s: &SnapshotRow) -> Result<()> {
        let id = s.id;
        let opts = ManifestOptions {
            mtime_precision_ns: 1,
            cross_fs: false,
        };
        match manifest_snapshot(self.cat, id, b"", &opts) {
            Ok(m) => {
                if Some(m.core) != s.manifest_hash {
                    self.problems.push(format!(
                        "snapshot {id}: manifest hash {} does not match stored {:?}",
                        m.core,
                        s.manifest_hash.map(|h| h.to_hex())
                    ));
                }
                if Some(m.ext) != s.ext_manifest_hash {
                    self.problems.push(format!(
                        "snapshot {id}: extended manifest hash {} does not match stored {:?}",
                        m.ext,
                        s.ext_manifest_hash.map(|h| h.to_hex())
                    ));
                }
            }
            Err(e) => self
                .problems
                .push(format!("snapshot {id}: cannot compute manifest: {e}")),
        }

        let mut hashes: Vec<Hash32> = Vec::new();
        self.cat.for_each_snapshot_content(id, |_, units| {
            hashes.extend(units);
            Ok(())
        })?;
        for h in &hashes {
            self.unit(id, h);
        }

        match &s.record_unit {
            None => self
                .problems
                .push(format!("snapshot {id}: complete but has no record unit")),
            Some(ru) => {
                self.unit(id, ru);
                match record::record_chunks(&self.reader, self.cat, ru) {
                    Ok(chunks) => {
                        for c in &chunks {
                            self.unit(id, c);
                        }
                    }
                    Err(e) => self
                        .problems
                        .push(format!("snapshot {id}: record unit {ru}: {e}")),
                }
                if !self.quick {
                    if let Err(e) = record::read_record(&self.reader, self.cat, ru) {
                        self.problems
                            .push(format!("snapshot {id}: record unit {ru}: {e}"));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Verify `snapshot`, or every complete snapshot when `None`.
pub fn run(repo: &Repo, snapshot: Option<Id>, quick: bool) -> Result<VerifyReport> {
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let selected: Vec<SnapshotRow> = match snapshot {
        Some(id) => vec![cat.snapshot(id)?],
        None => cat
            .snapshots()?
            .into_iter()
            .filter(|s| s.status == SnapshotStatus::Complete)
            .collect(),
    };
    let mut c = Checker {
        repo,
        cat: &cat,
        quick,
        reader: UnitReader::new(repo, 64 << 20),
        seen: HashSet::new(),
        touched: HashSet::new(),
        problems: Vec::new(),
    };
    let mut snapshots = 0;
    for s in &selected {
        if s.status != SnapshotStatus::Complete {
            c.problems.push(format!(
                "snapshot {}: status is {}, not complete",
                s.id,
                s.status.as_str()
            ));
            continue;
        }
        snapshots += 1;
        c.snapshot(s)?;
    }

    let mut touched: Vec<Id> = c.touched.iter().copied().collect();
    touched.sort();
    if !quick {
        for id in &touched {
            let path = pack_path(&repo.packs_dir(), *id);
            if !path.exists() {
                continue; // already reported per unit
            }
            match scan(&path) {
                Ok(s) if s.sealed && s.blake3_ok != Some(true) => c
                    .problems
                    .push(format!("pack {}: BLAKE3 mismatch", display_path(&path))),
                Ok(_) => {}
                Err(e) => c
                    .problems
                    .push(format!("pack {}: {e}", display_path(&path))),
            }
        }
    }
    Ok(VerifyReport {
        snapshots,
        units_checked: c.seen.len() as u64,
        packs_checked: touched.len() as u64,
        problems: c.problems,
    })
}
