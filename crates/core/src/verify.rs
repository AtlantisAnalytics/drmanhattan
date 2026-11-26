//! Repository verification (spec §9). A damaged repository is reported as
//! `problems`, never as an `Err`; `Err` is reserved for failures to even
//! look (e.g. the catalog will not open).
//!
//! `quick` checks, for every unit a selected complete or damaged snapshot references
//! (its contents' units, its record unit, and the chunks the record index
//! names): the pack file exists, the record header at the unit's offset
//! parses, and its hash, kind and lengths equal the unit row; plus the
//! snapshot's stored manifest hashes. Full adds decompress-and-hash of
//! every unit, a streaming BLAKE3 over each content's units in order
//! (which must equal the content hash, so a content mapped to the wrong
//! units is caught), the whole-pack BLAKE3 of each sealed pack touched,
//! and a full read of the snapshot record.
//!
//! The calling thread walks the catalog and makes every check except the
//! content reads, which run on worker threads. Each step of the walk is a
//! numbered job, and a merger emits the results in job order, so the
//! report is the same for any worker count.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use crossbeam_channel::{Receiver, Sender};

use crate::catalog::Catalog;
use crate::display::{display_bytes, display_path};
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::ingest::record;
use crate::manifest::{manifest_snapshot, ManifestOptions};
use crate::model::{ContentRow, EntryKind, SnapshotRow, SnapshotStatus, UnitRow};
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

#[derive(Debug, Clone, Default)]
pub struct VerifyOptions {
    /// Check record headers only, reading no unit payloads (see the module
    /// doc).
    pub quick: bool,
    /// Content-reading threads; 0 means `std::thread::available_parallelism()`.
    pub workers: usize,
    /// Test seam: at most this many jobs between sent and merged instead
    /// of `REORDER_WINDOW`. `None` (the default) is the production value.
    #[doc(hidden)]
    pub window: Option<usize>,
}

/// Content-reading threads for `n`; 0 means one per available CPU.
fn worker_count(n: usize) -> usize {
    match n {
        0 => std::thread::available_parallelism().map_or(1, |n| n.get()),
        n => n,
    }
}

/// Hash one pack file whole. `None` means it is fine, or missing (a missing
/// pack is already reported per unit).
fn check_pack(path: &Path) -> Option<String> {
    if !path.exists() {
        return None;
    }
    match scan(path) {
        Ok(s) if s.sealed && s.blake3_ok != Some(true) => {
            Some(format!("pack {}: BLAKE3 mismatch", display_path(path)))
        }
        Ok(_) => None,
        Err(e) => Some(format!("pack {}: {e}", display_path(path))),
    }
}

/// Run `check_pack` over `touched` on `workers` threads. Problems come back
/// in `touched` order, whatever the worker count.
fn check_packs(repo: &Repo, touched: &[Id], workers: usize) -> Vec<String> {
    let next = AtomicUsize::new(0);
    let results: Vec<Mutex<Option<String>>> = touched.iter().map(|_| Mutex::new(None)).collect();
    std::thread::scope(|s| {
        for _ in 0..workers.min(touched.len()) {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(id) = touched.get(i) else { break };
                let found = check_pack(&pack_path(&repo.packs_dir(), *id));
                *results[i].lock().unwrap() = found;
            });
        }
    });
    results
        .into_iter()
        .filter_map(|m| m.into_inner().unwrap())
        .collect()
}

fn unit_location(repo: &Repo, row: &UnitRow) -> String {
    format!(
        "pack {} offset {}",
        display_path(&pack_path(&repo.packs_dir(), row.pack_id)),
        row.pack_offset
    )
}

/// What reading one content's units found.
enum ContentOutcome {
    Ok,
    /// `unit` failed to read; the merger reports this only for the first
    /// content (in walk order) that hits it.
    UnitFailed {
        unit: Hash32,
        problem: String,
    },
    /// Every unit read, but together they do not hash to the content hash.
    HashMismatch(String),
}

/// Read `units` in order into `hasher`, each verified against its own hash,
/// stopping at the first that fails.
fn hash_units(
    reader: &UnitReader,
    repo: &Repo,
    snap: Id,
    units: &[UnitRow],
    hasher: &mut blake3::Hasher,
) -> std::result::Result<(), ContentOutcome> {
    for u in units {
        match reader.read_unit(u) {
            Ok(data) => {
                hasher.update(&data);
            }
            Err(e) => {
                let loc = unit_location(repo, u);
                return Err(ContentOutcome::UnitFailed {
                    unit: u.hash,
                    problem: format!("snapshot {snap}: unit {}: {e} ({loc})", u.hash),
                });
            }
        }
    }
    Ok(())
}

/// Read `content`'s `units` (all of them, in order) and require their
/// concatenation to hash to the content hash.
fn check_content(
    reader: &UnitReader,
    repo: &Repo,
    snap: Id,
    content: &ContentRow,
    units: &[UnitRow],
) -> ContentOutcome {
    let mut hasher = blake3::Hasher::new();
    if let Err(failed) = hash_units(reader, repo, snap, units, &mut hasher) {
        return failed;
    }
    let got = Hash32(*hasher.finalize().as_bytes());
    if got == content.hash {
        ContentOutcome::Ok
    } else {
        ContentOutcome::HashMismatch(format!(
            "snapshot {snap}: content {}: its {} unit(s) hash to {got}",
            content.hash,
            units.len()
        ))
    }
}

/// A content for a worker to read.
struct ContentWork {
    snap: Id,
    content: ContentRow,
    units: Vec<UnitRow>,
    /// `units` stops before a unit the walk already reported: read them,
    /// but there is no content hash to compare.
    partial: bool,
}

impl ContentWork {
    fn check(&self, reader: &UnitReader, repo: &Repo) -> ContentOutcome {
        if !self.partial {
            return check_content(reader, repo, self.snap, &self.content, &self.units);
        }
        let mut hasher = blake3::Hasher::new();
        match hash_units(reader, repo, self.snap, &self.units, &mut hasher) {
            Ok(()) => ContentOutcome::Ok,
            Err(failed) => failed,
        }
    }
}

/// One step of the walk: the problems found since the previous job, then
/// (full only) a content to read.
struct Job {
    seq: u64,
    before: Vec<String>,
    work: Option<ContentWork>,
}

struct Done {
    seq: u64,
    before: Vec<String>,
    outcome: ContentOutcome,
}

/// Most jobs sent but not yet merged: at about 150 B per buffered result,
/// about 10 MB, so workers run far ahead of one large content in bounded memory.
const REORDER_WINDOW: usize = 65_536;

/// The error for a pipeline thread that is gone, which only a panic does;
/// the scope then re-raises that panic.
fn stopped() -> Error {
    Error::Corrupt("verify thread panicked".into())
}

/// Sends a placeholder result for the job a panicking worker held, so the
/// merger, and with it the producer's window, moves past it. The scope
/// still re-raises the panic, so the placeholder is never reported.
struct Placeholder<'a> {
    seq: u64,
    done: &'a Sender<Done>,
}

impl Drop for Placeholder<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let _ = self.done.send(Done {
                seq: self.seq,
                before: Vec::new(),
                outcome: ContentOutcome::Ok,
            });
        }
    }
}

fn content_worker(reader: &UnitReader, repo: &Repo, jobs: &Receiver<Job>, done: &Sender<Done>) {
    for job in jobs {
        let _placeholder = Placeholder { seq: job.seq, done };
        let outcome = match &job.work {
            Some(w) => w.check(reader, repo),
            None => ContentOutcome::Ok,
        };
        let d = Done {
            seq: job.seq,
            before: job.before,
            outcome,
        };
        if done.send(d).is_err() {
            return; // the merger panicked; the scope reports it
        }
    }
}

/// Collect the workers' results in job order. A unit that fails to read is
/// reported once, for the first content that hits it, as later contents
/// holding it would only repeat the failure. Each job merged returns its
/// credit to the producer's window.
fn merge(done: &Receiver<Done>, credits: &Sender<()>) -> Vec<String> {
    let mut problems = Vec::new();
    let mut waiting = BTreeMap::new();
    let mut read_bad = HashSet::new();
    let mut next = 0u64;
    for d in done {
        waiting.insert(d.seq, d);
        while let Some(d) = waiting.remove(&next) {
            next += 1;
            // Never blocks: at most the window's credits exist. Fails only
            // once the producer is gone, which needs none.
            let _ = credits.send(());
            problems.extend(d.before);
            match d.outcome {
                ContentOutcome::Ok => {}
                ContentOutcome::UnitFailed { unit, problem } => {
                    if read_bad.insert(unit) {
                        problems.push(problem);
                    }
                }
                ContentOutcome::HashMismatch(p) => problems.push(p),
            }
        }
    }
    problems
}

/// The walk, on the calling thread: every check but the content reads,
/// which it hands to the workers as jobs.
struct Checker<'a> {
    repo: &'a Repo,
    cat: &'a Catalog,
    quick: bool,
    /// Also the only source of pack handles, so verify holds at most its
    /// bounded number open.
    reader: &'a UnitReader,
    seen: HashSet<Hash32>,
    /// Units with a problem already reported by the walk.
    bad: HashSet<Hash32>,
    /// Contents whose units were already sent to be read (full only).
    hashed: HashSet<Hash32>,
    touched: HashSet<Id>,
    /// Problems found since the last job was sent.
    pending: Vec<String>,
    jobs: Sender<Job>,
    /// One per job the producer may send before earlier ones are merged.
    credits: Receiver<()>,
    seq: u64,
}

impl Checker<'_> {
    fn problem(&mut self, hash: &Hash32, msg: String) {
        self.bad.insert(*hash);
        self.pending.push(msg);
    }

    /// Send the pending problems and `work` as the next job. Steps that
    /// found nothing and have nothing to read send no job.
    fn send(&mut self, work: Option<ContentWork>) -> Result<()> {
        if work.is_none() && self.pending.is_empty() {
            return Ok(());
        }
        if self.credits.try_recv().is_err() {
            // Lets tests see the window fill (compiled out without the
            // `failpoints` feature).
            fail::fail_point!("verify::wait_credit");
            self.credits.recv().map_err(|_| stopped())?;
        }
        let job = Job {
            seq: self.seq,
            before: std::mem::take(&mut self.pending),
            work,
        };
        self.seq += 1;
        // Fails only once every worker is gone, which only a panic does.
        self.jobs.send(job).map_err(|_| stopped())
    }

    /// Check one unit by hash, once per run. With `read` (and not quick),
    /// also decompress and hash it; content units are read by the workers.
    /// Returns the unit's row whenever it loads; for a unit already seen,
    /// only when full, the only case that needs it again.
    fn unit(&mut self, snap: Id, hash: &Hash32, read: bool) -> Option<UnitRow> {
        if !self.seen.insert(*hash) {
            return if self.quick {
                None
            } else {
                self.cat.unit(hash).ok()
            };
        }
        let row = match self.cat.unit(hash) {
            Ok(r) => r,
            Err(e) => {
                self.problem(hash, format!("snapshot {snap}: unit {hash}: {e}"));
                return None;
            }
        };
        self.touched.insert(row.pack_id);
        self.check_unit(snap, hash, &row, read);
        Some(row)
    }

    fn check_unit(&mut self, snap: Id, hash: &Hash32, row: &UnitRow, read: bool) {
        let loc = unit_location(self.repo, row);
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
            Ok(p) => p.read_header(offset),
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
            if let Err(e) = self.reader.read_unit(row) {
                self.problem(hash, format!("snapshot {snap}: unit {hash}: {e} ({loc})"));
            }
        }
    }

    /// Check a content's units; when full, also send them to be read in
    /// order (each verified against its own hash) and their concatenation
    /// checked against the content hash. As ever, a unit the walk already
    /// reported ends the read there, and the hash is not compared.
    fn content(&mut self, snap: Id, c: ContentRow, units: &[Hash32]) -> Result<()> {
        let mut rows = Vec::with_capacity(units.len());
        for h in units {
            rows.push(self.unit(snap, h, false));
        }
        // An incomplete content is reported per file by `snapshot`; its
        // present units are still read, but there is no content hash to
        // compare.
        let incomplete = (units.len() as i64) < c.unit_count;
        if self.quick || !self.hashed.insert(c.hash) {
            return self.send(None);
        }
        let mut prefix = Vec::with_capacity(units.len());
        for (h, row) in units.iter().zip(rows) {
            match row {
                Some(r) if !self.bad.contains(h) => prefix.push(r),
                _ => break,
            }
        }
        let partial = prefix.len() < units.len() || incomplete;
        if partial && prefix.is_empty() {
            return self.send(None);
        }
        self.send(Some(ContentWork {
            snap,
            content: c,
            units: prefix,
            partial,
        }))
    }

    fn snapshot(&mut self, s: &SnapshotRow) -> Result<()> {
        let id = s.id;
        let opts = ManifestOptions {
            mtime_precision_ns: 1,
            cross_fs: false,
        };
        match manifest_snapshot(self.cat, id, b"", &opts) {
            Ok(m) => {
                let stored = |h: Option<Hash32>| h.map_or("(none)".into(), |h| h.to_hex());
                if Some(m.core) != s.manifest_hash {
                    self.pending.push(format!(
                        "snapshot {id}: manifest hash {} does not match stored {}",
                        m.core,
                        stored(s.manifest_hash)
                    ));
                }
                if Some(m.ext) != s.ext_manifest_hash {
                    self.pending.push(format!(
                        "snapshot {id}: extended manifest hash {} does not match stored {}",
                        m.ext,
                        stored(s.ext_manifest_hash)
                    ));
                }
            }
            Err(e) => self
                .pending
                .push(format!("snapshot {id}: cannot compute manifest: {e}")),
        }

        let cat = self.cat;
        // Contents with fewer units than they declare: hash -> (have, want).
        let mut incomplete: HashMap<Hash32, (i64, i64)> = HashMap::new();
        cat.for_each_snapshot_content(id, |content, units| {
            let have = units.len() as i64;
            if have < content.unit_count {
                incomplete.insert(content.hash, (have, content.unit_count));
            }
            self.content(id, content, &units)
        })?;
        if !incomplete.is_empty() {
            cat.for_each_entry(id, |e| {
                let missing = e.content_hash.and_then(|h| incomplete.get(&h));
                if let (EntryKind::File, Some(&(have, want))) = (e.kind, missing) {
                    self.pending.push(format!(
                        "snapshot {id}: {}: missing {} of {want} units",
                        display_bytes(&e.path),
                        want - have
                    ));
                }
                Ok(())
            })?;
        }

        match &s.record_unit {
            None => self
                .pending
                .push(format!("snapshot {id}: complete but has no record unit")),
            Some(ru) => {
                self.unit(id, ru, true);
                match record::record_chunks(self.reader, self.cat, ru) {
                    Ok(chunks) => {
                        for c in &chunks {
                            self.unit(id, c, true);
                        }
                    }
                    Err(e) => self
                        .pending
                        .push(format!("snapshot {id}: record unit {ru}: {e}")),
                }
                if !self.quick {
                    if let Err(e) = record::read_record(self.reader, self.cat, ru) {
                        self.pending
                            .push(format!("snapshot {id}: record unit {ru}: {e}"));
                    }
                }
            }
        }
        self.send(None)
    }

    /// Walk `selected` in report order; returns how many were checked.
    fn walk(&mut self, selected: &[SnapshotRow]) -> Result<u64> {
        let mut snapshots = 0;
        for s in selected {
            if !matches!(s.status, SnapshotStatus::Complete | SnapshotStatus::Damaged) {
                self.pending.push(format!(
                    "snapshot {}: status is {}, not complete",
                    s.id,
                    s.status.as_str()
                ));
                continue;
            }
            snapshots += 1;
            self.snapshot(s)?;
        }
        self.send(None)?;
        Ok(snapshots)
    }
}

/// Verify `snapshot`, or every complete or damaged snapshot when `None`, reading
/// contents on one thread per CPU.
pub fn run(repo: &Repo, snapshot: Option<Id>, quick: bool) -> Result<VerifyReport> {
    run_with(
        repo,
        snapshot,
        &VerifyOptions {
            quick,
            workers: 0,
            ..VerifyOptions::default()
        },
    )
}

/// Verify `snapshot`, or every complete or damaged snapshot when `None`. The report
/// does not depend on `opts.workers`.
pub fn run_with(repo: &Repo, snapshot: Option<Id>, opts: &VerifyOptions) -> Result<VerifyReport> {
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let selected: Vec<SnapshotRow> = match snapshot {
        Some(id) => vec![cat.snapshot(id)?],
        None => cat
            .snapshots()?
            .into_iter()
            .filter(|s| matches!(s.status, SnapshotStatus::Complete | SnapshotStatus::Damaged))
            .collect(),
    };
    let reader = UnitReader::new(repo, 64 << 20);
    let workers = worker_count(opts.workers);
    let (cat, reader) = (&cat, &reader);
    // Workers and merger start before the walk, so on an `Err` from the
    // walk its job sender drops and every thread is joined before return.
    let (snapshots, seen, touched, mut problems) = std::thread::scope(|s| {
        let (job_tx, job_rx) = crossbeam_channel::bounded::<Job>(4 * workers);
        let (done_tx, done_rx) = crossbeam_channel::unbounded::<Done>();
        // At least one credit, or the producer could never send.
        let window = opts.window.unwrap_or(REORDER_WINDOW).max(1);
        let (credit_tx, credit_rx) = crossbeam_channel::bounded::<()>(window);
        for _ in 0..window {
            credit_tx.send(()).map_err(|_| stopped())?;
        }
        for _ in 0..workers {
            let (jobs, done) = (job_rx.clone(), done_tx.clone());
            s.spawn(move || content_worker(reader, repo, &jobs, &done));
        }
        drop((job_rx, done_tx));
        let merger = s.spawn(move || merge(&done_rx, &credit_tx));

        let mut c = Checker {
            repo,
            cat,
            quick: opts.quick,
            reader,
            seen: HashSet::new(),
            bad: HashSet::new(),
            hashed: HashSet::new(),
            touched: HashSet::new(),
            pending: Vec::new(),
            jobs: job_tx,
            credits: credit_rx,
            seq: 0,
        };
        let snapshots = c.walk(&selected)?;
        let Checker {
            seen,
            touched,
            jobs,
            ..
        } = c;
        // Closing the job queue lets the workers, then the merger, finish.
        drop(jobs);
        let problems = merger
            .join()
            .unwrap_or_else(|p| std::panic::resume_unwind(p));
        Ok::<_, Error>((snapshots, seen, touched, problems))
    })?;

    let mut touched: Vec<Id> = touched.into_iter().collect();
    touched.sort();
    if !opts.quick {
        problems.extend(check_packs(repo, &touched, workers));
    }
    Ok(VerifyReport {
        snapshots,
        units_checked: seen.len() as u64,
        packs_checked: touched.len() as u64,
        problems,
    })
}
