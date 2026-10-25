//! Repository verification (spec §9). A damaged repository is reported as
//! `problems`, never as an `Err`; `Err` is reserved for failures to even
//! look (e.g. the catalog will not open).
//!
//! `quick` checks, for every unit a selected complete snapshot references
//! (its contents' units, its record unit, and the chunks the record index
//! names): the pack file exists, the record header at the unit's offset
//! parses, and its hash, kind and lengths equal the unit row; plus the
//! snapshot's stored manifest hashes. Full adds decompress-and-hash of
//! every unit, a streaming BLAKE3 over each content's units in order
//! (which must equal the content hash, so a content mapped to the wrong
//! units is caught), the whole-pack BLAKE3 of each sealed pack touched,
//! and a full read of the snapshot record.

use std::collections::HashSet;

use crate::catalog::Catalog;
use crate::display::display_path;
use crate::error::Result;
use crate::ids::{Hash32, Id};
use crate::ingest::record;
use crate::manifest::{manifest_snapshot, ManifestOptions};
use crate::model::{ContentRow, SnapshotRow, SnapshotStatus};
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
    /// Units with a problem already reported.
    bad: HashSet<Hash32>,
    /// Contents whose units were already hashed (full only).
    hashed: HashSet<Hash32>,
    touched: HashSet<Id>,
    problems: Vec<String>,
}

impl Checker<'_> {
    fn problem(&mut self, hash: &Hash32, msg: String) {
        self.bad.insert(*hash);
        self.problems.push(msg);
    }

    /// Check one unit by hash, once per run. With `read` (and not quick),
    /// also decompress and hash it; content units are read by `content`.
    fn unit(&mut self, snap: Id, hash: &Hash32, read: bool) {
        if !self.seen.insert(*hash) {
            return;
        }
        let row = match self.cat.unit(hash) {
            Ok(r) => r,
            Err(e) => {
                self.problem(hash, format!("snapshot {snap}: unit {hash}: {e}"));
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
                let msg = format!("snapshot {snap}: unit {hash}: negative offset ({loc})");
                self.problem(hash, msg);
                return;
            }
        };
        let header = match self.reader.pack(row.pack_id) {
            Err(e) => {
                let path = pack_path(&self.repo.packs_dir(), row.pack_id);
                let msg = format!(
                    "snapshot {snap}: unit {hash}: pack {} unreadable: {e}",
                    display_path(&path)
                );
                self.problem(hash, msg);
                return;
            }
            Ok(p) => p.read_record(offset).map(|(h, _)| h),
        };
        let h = match header {
            Ok(h) => h,
            Err(e) => {
                self.problem(hash, format!("snapshot {snap}: unit {hash}: {e} ({loc})"));
                return;
            }
        };
        if h.kind != row.kind {
            let msg = format!(
                "snapshot {snap}: unit {hash}: record header kind {} disagrees with \
                 catalog kind {} ({loc})",
                h.kind.as_str(),
                row.kind.as_str()
            );
            self.problem(hash, msg);
            return;
        }
        if h.unit_hash != row.hash
            || i64::try_from(h.raw_len).ok() != Some(row.raw_size)
            || i64::try_from(h.stored_len).ok() != Some(row.stored_size)
        {
            let msg = format!(
                "snapshot {snap}: unit {hash}: record header (hash {}, raw {}, stored {}) \
                 disagrees with catalog (raw {}, stored {}) ({loc})",
                h.unit_hash, h.raw_len, h.stored_len, row.raw_size, row.stored_size
            );
            self.problem(hash, msg);
            return;
        }
        if read && !self.quick {
            if let Err(e) = self.reader.read_unit(&row) {
                self.problem(hash, format!("snapshot {snap}: unit {hash}: {e} ({loc})"));
            }
        }
    }

    /// Check a content's units; when full, also read them in order (each
    /// verified against its own hash) and require their concatenation to
    /// hash to the content hash.
    fn content(&mut self, snap: Id, c: &ContentRow, units: &[Hash32]) {
        for h in units {
            self.unit(snap, h, false);
        }
        if self.quick || !self.hashed.insert(c.hash) {
            return;
        }
        let mut hasher = blake3::Hasher::new();
        for h in units {
            if self.bad.contains(h) {
                return; // already reported
            }
            let Ok(row) = self.cat.unit(h) else {
                return; // `unit` reported it
            };
            match self.reader.read_unit(&row) {
                Ok(data) => {
                    hasher.update(&data);
                }
                Err(e) => {
                    let loc = format!(
                        "pack {} offset {}",
                        display_path(&pack_path(&self.repo.packs_dir(), row.pack_id)),
                        row.pack_offset
                    );
                    self.problem(h, format!("snapshot {snap}: unit {h}: {e} ({loc})"));
                    return;
                }
            }
        }
        let got = Hash32(*hasher.finalize().as_bytes());
        if got != c.hash {
            self.problems.push(format!(
                "snapshot {snap}: content {}: its {} unit(s) hash to {got}",
                c.hash,
                units.len()
            ));
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

        let cat = self.cat;
        cat.for_each_snapshot_content(id, |content, units| {
            self.content(id, &content, &units);
            Ok(())
        })?;

        match &s.record_unit {
            None => self
                .problems
                .push(format!("snapshot {id}: complete but has no record unit")),
            Some(ru) => {
                self.unit(id, ru, true);
                match record::record_chunks(&self.reader, self.cat, ru) {
                    Ok(chunks) => {
                        for c in &chunks {
                            self.unit(id, c, true);
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
        bad: HashSet::new(),
        hashed: HashSet::new(),
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
