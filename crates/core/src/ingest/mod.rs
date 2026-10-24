//! Ingest pipeline (spec §7).
//!
//! ```text
//! walker (caller's thread) ──FileJob──► workers ──StorageMsg──► storage ──CatalogOp──► catalog writer
//!        └──────────── dir and symlink entries ────────────────────┘
//! ```
//!
//! The storage thread is the only writer of pack bytes and the only sender of
//! catalog ops. It forwards ops to the catalog writer only at commit
//! barriers, after fsyncing the open pack, so the writer's open transaction
//! never holds a row referencing unsynced pack bytes (the §7 ordering
//! invariant), even when the pipeline aborts and the writer commits on close.
//!
//! Finish runs while storage is still alive: once the walker and workers are
//! done, a `Flush` makes every row durable in the catalog; the manifest
//! hashes are computed from it with `manifest_snapshot` (the same code
//! `verify` uses); the snapshot record is built from it and its units go
//! through storage like any other; a second `Flush` commits them. Only
//! then is storage closed (sealing the open pack), every thread joined, and
//! the snapshot row marked complete with its `record_unit`.

pub mod record;
pub mod screen;
mod storage;
mod worker;

use crate::catalog::writer::{spawn_catalog_writer, CatalogOp};
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::manifest::{manifest_snapshot, ManifestOptions};
use crate::model::{EntryKind, EntryRow, EntryStatus, SnapshotRow, SnapshotStatus};
use crate::repo::Repo;
use crate::walk::{walk, SkipReason, WalkEntry, WalkItem, WalkOptions};
use crate::xattrs::read_xattrs;
use screen::{NoopScreener, Screener};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use storage::{Storage, StorageMsg, StorageStats};
use worker::{FileJob, Worker};

/// Capacity of the storage -> catalog writer queue (ops carry no payloads).
const CATALOG_QUEUE: usize = 1024;

#[derive(Debug, Clone, Default)]
pub struct IngestOptions {
    pub note: Option<String>,
    pub allow_unstable: bool,
    pub cross_fs: bool,
    /// Reader threads; 0 means `std::thread::available_parallelism()`.
    pub workers: usize,
    /// Test seam: commit barrier after this many buffered catalog ops
    /// instead of 10_000. `None` (the default) is the production value.
    #[doc(hidden)]
    pub barrier_ops: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct IngestReport {
    pub snapshot: Id,
    pub entries: u64,
    pub files: u64,
    /// Logical bytes of all file entries.
    pub bytes_read: u64,
    pub new_units: u64,
    /// Sum of `stored_size` over the units newly appended by this run.
    pub new_bytes_stored: u64,
    /// `bytes_read / new_bytes_stored`; 0.0 when nothing new was stored.
    pub dedup_ratio: f64,
    pub skipped: Vec<(Vec<u8>, SkipReason)>,
    pub unstable: Vec<Vec<u8>>,
    pub withheld: u64,
    pub manifest_hash: Hash32,
    pub elapsed_ms: u64,
}

/// Snapshot `source` into `repo`.
pub fn run(repo: &Repo, source: &Path, opts: &IngestOptions) -> Result<IngestReport> {
    let started = Instant::now();
    let _lock = repo.lock_writer()?;
    let source = canonical(source)?;
    let root = canonical(&repo.root)?;
    // Component-wise, so `/x/r2` is not inside `/x/r`.
    if source.starts_with(&root) || root.starts_with(&source) {
        return Err(Error::Overlap);
    }
    let snap = SnapshotRow {
        id: Id::new_v7(),
        status: SnapshotStatus::InProgress,
        source_root: source.as_os_str().as_bytes().to_vec(),
        host: hostname(),
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        started_at: now_ns()?,
        completed_at: None,
        note: opts.note.clone(),
        manifest_hash: None,
        ext_manifest_hash: None,
        source_manifest_hash: None,
        record_unit: None,
        entry_count: None,
        file_bytes: None,
        screened_out: 0,
        label_pattern: None,
    };
    let result = pipeline(repo, &source, opts, &snap, &NoopScreener)
        .and_then(|out| finish(repo, out, started));
    if result.is_err() {
        mark_failed(repo, &snap);
    }
    result
}

fn canonical(p: &Path) -> Result<PathBuf> {
    p.canonicalize().map_err(|e| Error::io(p, e))
}

/// Best effort: the caller gets the original error either way, so a failure
/// to record the failure is reported on stderr rather than returned.
fn mark_failed(repo: &Repo, snap: &SnapshotRow) {
    let row = SnapshotRow {
        status: SnapshotStatus::Failed,
        completed_at: now_ns().ok(),
        ..snap.clone()
    };
    if let Err(e) = Catalog::open(&repo.catalog_path()).and_then(|c| c.upsert_snapshot(&row)) {
        eprintln!(
            "warning: could not mark snapshot {} as failed: {e}",
            snap.id
        );
    }
}

struct PipelineOutput {
    /// Stats as of the first flush: the snapshot's entries and the file
    /// units it added, excluding its record's units.
    stats: StorageStats,
    skipped: Vec<(Vec<u8>, SkipReason)>,
    /// The final snapshot row, `record_unit` included.
    row: SnapshotRow,
    manifest_hash: Hash32,
}

/// What the finish phase produced before storage was closed.
struct Sealed {
    stats: StorageStats,
    row: SnapshotRow,
    manifest_hash: Hash32,
}

/// Why a pipeline stage stopped before finishing.
#[derive(Debug)]
pub(crate) enum Halt {
    /// Another stage failed (abort flag set, or a channel closed).
    Aborted,
    Failed(Error),
}

impl From<Error> for Halt {
    fn from(e: Error) -> Halt {
        Halt::Failed(e)
    }
}

pub(crate) type Stage<T> = std::result::Result<T, Halt>;

/// Abort flag plus the first real error any stage hit.
pub(crate) struct Shared {
    abort: AtomicBool,
    first: Mutex<Option<Error>>,
}

impl Shared {
    fn new() -> Shared {
        Shared {
            abort: AtomicBool::new(false),
            first: Mutex::new(None),
        }
    }

    pub(crate) fn aborted(&self) -> bool {
        self.abort.load(Ordering::SeqCst)
    }

    fn fail(&self, e: Error) {
        let mut first = self.first.lock().unwrap_or_else(|p| p.into_inner());
        if first.is_none() {
            *first = Some(e);
        }
        self.abort.store(true, Ordering::SeqCst);
    }

    /// Record why a stage stopped. Stages call this while still holding
    /// their channel ends, so peers see the abort flag before a disconnect.
    fn halt(&self, h: Halt) {
        match h {
            Halt::Aborted => self.abort.store(true, Ordering::SeqCst),
            Halt::Failed(e) => self.fail(e),
        }
    }

    fn take_error(&self) -> Option<Error> {
        self.first.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

fn panicked(stage: &str) -> Error {
    Error::Corrupt(format!("ingest {stage} thread panicked"))
}

/// Records a panic as soon as unwinding passes through the stage, so peers
/// abort at once instead of when the panicked thread is joined. Declare it
/// after the stage's channel ends so it drops (and fires) before them.
struct PanicGuard<'a> {
    shared: &'a Shared,
    stage: &'static str,
}

impl Drop for PanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.shared.fail(panicked(self.stage));
        }
    }
}

/// Run walker, workers, storage and catalog writer to completion. Every
/// thread is joined before this returns, on success and on failure.
fn pipeline(
    repo: &Repo,
    source: &Path,
    opts: &IngestOptions,
    snap: &SnapshotRow,
    screener: &dyn Screener,
) -> Result<PipelineOutput> {
    let workers = match opts.workers {
        0 => std::thread::available_parallelism().map_or(1, |n| n.get()),
        n => n,
    };
    let shared = Shared::new();
    let mut skipped = Vec::new();
    let mut sealed = None;

    let (cat_tx, cat_rx) = mpsc::sync_channel(CATALOG_QUEUE);
    let writer = spawn_catalog_writer(repo.catalog_path(), cat_rx);
    // If the writer is already gone, its join below reports why.
    let opened = cat_tx.send(CatalogOp::UpsertSnapshot(snap.clone())).is_ok()
        && cat_tx.send(CatalogOp::Commit).is_ok();

    if opened {
        let claimed = Mutex::new(HashSet::new());
        let worker = Worker {
            snapshot: snap.id,
            config: &repo.config,
            catalog_path: repo.catalog_path(),
            claimed: &claimed,
            screener,
            shared: &shared,
            allow_unstable: opts.allow_unstable,
        };
        let worker = &worker;
        let shared = &shared;
        std::thread::scope(|s| {
            let (job_tx, job_rx) = crossbeam_channel::bounded::<FileJob>(4 * workers);
            let (st_tx, st_rx) = crossbeam_channel::bounded::<StorageMsg>(4 * workers);

            let barrier_ops = opts.barrier_ops.unwrap_or(storage::BARRIER_OPS);
            let storage = s.spawn(move || {
                let st_rx = st_rx;
                let _guard = PanicGuard {
                    shared,
                    stage: "storage",
                };
                // The report's stats come from the finish phase's flush.
                let run = Storage::open(repo, cat_tx, barrier_ops)
                    .map_err(Halt::from)
                    .and_then(|st| st.run(&st_rx, shared));
                if let Err(h) = run {
                    shared.halt(h);
                }
            });
            let pool: Vec<_> = (0..workers)
                .map(|_| {
                    let (jobs, out) = (job_rx.clone(), st_tx.clone());
                    s.spawn(move || {
                        let (jobs, out) = (jobs, out);
                        let _guard = PanicGuard {
                            shared,
                            stage: "worker",
                        };
                        if let Err(h) = worker.run(&jobs, &out) {
                            shared.halt(h);
                        }
                    })
                })
                .collect();
            drop(job_rx);

            walk_source(source, opts, snap.id, &job_tx, &st_tx, shared, &mut skipped);
            drop(job_tx);

            for h in pool {
                if h.join().is_err() {
                    shared.fail(panicked("worker"));
                }
            }
            if !shared.aborted() {
                match seal_snapshot(repo, snap, &st_tx) {
                    Ok(s) => sealed = Some(s),
                    Err(h) => shared.halt(h),
                }
            }
            // Closing the last sender lets storage seal its pack and stop.
            drop(st_tx);
            if storage.join().is_err() {
                shared.fail(panicked("storage"));
            }
        });
    } else {
        drop(cat_tx);
    }

    match writer.join() {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => shared.fail(e),
        Err(_) => shared.fail(panicked("catalog writer")),
    }
    if let Some(e) = shared.take_error() {
        return Err(e);
    }
    let sealed = sealed.ok_or_else(|| Error::Corrupt("ingest stopped without an error".into()))?;
    Ok(PipelineOutput {
        stats: sealed.stats,
        skipped,
        row: sealed.row,
        manifest_hash: sealed.manifest_hash,
    })
}

/// Commit everything storage has received and return its stats. Fails with
/// `Aborted` if storage or the catalog writer stopped (they record why).
fn flush(storage: &crossbeam_channel::Sender<StorageMsg>) -> Stage<StorageStats> {
    let (reply, replied) = mpsc::channel();
    storage
        .send(StorageMsg::Flush(reply))
        .map_err(|_| Halt::Aborted)?;
    replied.recv().map_err(|_| Halt::Aborted)
}

/// The finish phase, run with storage still alive once the walker and
/// workers are done: commit, hash, then write and commit the record.
fn seal_snapshot(
    repo: &Repo,
    snap: &SnapshotRow,
    storage: &crossbeam_channel::Sender<StorageMsg>,
) -> Stage<Sealed> {
    let stats = flush(storage)?;
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let h = manifest_snapshot(
        &cat,
        snap.id,
        b"",
        &ManifestOptions {
            mtime_precision_ns: 1,
            cross_fs: false,
        },
    )?;
    // The record holds this row; a row cannot contain its own record unit.
    let row = SnapshotRow {
        status: SnapshotStatus::Complete,
        completed_at: Some(now_ns()?),
        manifest_hash: Some(h.core),
        ext_manifest_hash: Some(h.ext),
        // Plan 1 ingests with no filtering, so the source manifest is the core one.
        source_manifest_hash: Some(h.core),
        record_unit: None,
        entry_count: Some(to_i64(stats.entries, "entry count")?),
        file_bytes: Some(to_i64(stats.file_bytes, "file bytes")?),
        ..snap.clone()
    };
    let unit = record::write_record(
        &cat,
        &row,
        &repo.tmp_dir(),
        &repo.config,
        |header, payload| {
            storage
                .send(StorageMsg::Unit(header, payload))
                .map_err(|_| Halt::Aborted)
        },
    )?;
    flush(storage)?;
    Ok(Sealed {
        stats,
        row: SnapshotRow {
            record_unit: Some(unit),
            ..row
        },
        manifest_hash: h.core,
    })
}

/// Walk on the caller's thread: files go to the workers, dirs and symlinks
/// straight to storage (so every parent precedes its children there).
fn walk_source(
    source: &Path,
    opts: &IngestOptions,
    snapshot: Id,
    jobs: &crossbeam_channel::Sender<FileJob>,
    storage: &crossbeam_channel::Sender<StorageMsg>,
    shared: &Shared,
    skipped: &mut Vec<(Vec<u8>, SkipReason)>,
) {
    // Ids of the directories on the current path, indexed by depth.
    let mut parents: Vec<Id> = Vec::new();
    // Hard-link groups by (dev, ino), numbered from 1 per snapshot.
    let mut groups: HashMap<(u64, u64), i64> = HashMap::new();
    let mut halted = false;
    let wopts = WalkOptions {
        cross_fs: opts.cross_fs,
    };
    let res = walk(source, &wopts, |item| {
        let entry = match item {
            WalkItem::Skipped { path, reason } => {
                skipped.push((path, reason));
                return Ok(());
            }
            WalkItem::Entry(e) => e,
        };
        let id = Id::new_v7();
        parents.truncate(entry.depth as usize);
        let parent = parents.last().copied();
        if entry.kind == EntryKind::Dir {
            parents.push(id);
        }
        if shared.aborted() {
            halted = true;
            return Err(Error::Corrupt("walk halted".into()));
        }
        let abs = if entry.path.is_empty() {
            source.to_path_buf()
        } else {
            source.join(OsStr::from_bytes(&entry.path))
        };
        let sent = if entry.kind == EntryKind::File {
            let hardlink_group = (entry.nlink > 1).then(|| {
                let next = groups.len() as i64 + 1;
                *groups.entry((entry.dev, entry.ino)).or_insert(next)
            });
            jobs.send(FileJob {
                id,
                parent,
                abs,
                entry,
                hardlink_group,
            })
            .is_ok()
        } else {
            // Files' xattrs are read by the worker that reads the file.
            let xattrs = read_xattrs(&abs, entry.kind)?;
            let row = entry_row(snapshot, id, parent, entry, None, None);
            storage
                .send(StorageMsg::Entry {
                    row,
                    content_known: false,
                    xattrs,
                })
                .is_ok()
        };
        if sent {
            Ok(())
        } else {
            halted = true;
            Err(Error::Corrupt("walk halted".into()))
        }
    });
    match res {
        _ if halted => shared.halt(Halt::Aborted),
        Err(e) => shared.fail(e),
        Ok(()) => {}
    }
}

/// Build an entry row from a walk entry. `size` and `content` are set for
/// files only.
fn entry_row(
    snapshot: Id,
    id: Id,
    parent: Option<Id>,
    e: WalkEntry,
    size: Option<i64>,
    content: Option<Hash32>,
) -> EntryRow {
    EntryRow {
        id,
        snapshot_id: snapshot,
        parent_id: parent,
        name: e.name,
        path: e.path,
        kind: e.kind,
        mode: e.mode,
        mtime_ns: e.mtime_ns,
        size,
        content_hash: content,
        link_target: e.link_target,
        hardlink_group: None,
        status: EntryStatus::Ok,
    }
}

fn finish(repo: &Repo, out: PipelineOutput, started: Instant) -> Result<IngestReport> {
    let PipelineOutput {
        stats,
        skipped,
        row,
        manifest_hash,
    } = out;
    Catalog::open(&repo.catalog_path())?.upsert_snapshot(&row)?;
    let dedup_ratio = if stats.new_bytes_stored == 0 {
        0.0
    } else {
        stats.file_bytes as f64 / stats.new_bytes_stored as f64
    };
    Ok(IngestReport {
        snapshot: row.id,
        entries: stats.entries,
        files: stats.files,
        bytes_read: stats.file_bytes,
        new_units: stats.new_units,
        new_bytes_stored: stats.new_bytes_stored,
        dedup_ratio,
        skipped,
        unstable: stats.unstable,
        withheld: 0,
        manifest_hash,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
}

pub(crate) fn to_i64(v: u64, what: &str) -> Result<i64> {
    i64::try_from(v).map_err(|_| Error::Corrupt(format!("{what} {v} exceeds i64")))
}

fn now_ns() -> Result<i64> {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Corrupt(format!("system clock before the Unix epoch: {e}")))?;
    i64::try_from(d.as_nanos()).map_err(|_| Error::Corrupt("system clock out of range".into()))
}

fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is valid for writes of `buf.len()` bytes.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return String::new();
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}
