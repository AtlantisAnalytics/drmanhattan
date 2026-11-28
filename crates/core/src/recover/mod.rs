//! `wmk recover` (spec "Design → Steps"): rebuild a complete catalog from the
//! packs alone.
//!
//! `build` writes a new catalog at a path of the caller's choosing and never
//! writes packs, `catalog.sqlite` or `wmk.toml`. In order:
//!
//! 1. **Scan** (`scan`): salvage every pack, verify every candidate record's
//!    payload, insert `pack` rows and one `unit` row per intact unit.
//! 2. **Replay**: stream each `snapshot_record` unit into rows, one
//!    transaction per snapshot, committed only once the record verified.
//!    A record that fails is rolled back and reported lost.
//! 3. **Media**: recompute `media` rows from each image's leading bytes.
//! 4. **Damage**: a snapshot referencing a content with missing units is
//!    marked `damaged`; its entry rows stay exactly as recorded.
//! 5. **Check**: SQLite integrity and foreign keys, and every recovered
//!    snapshot's manifest hash against its stored one.
//! 6. Checkpoint, so no `-wal` is left beside the new catalog.
//!
//! `run` wraps `build`: it holds the writer lock, builds into `tmp/`, keeps
//! the old catalog as `catalog.sqlite.pre-recover-<UTC time>`, swaps the new
//! one in and records `format_version = 2`.

mod scan;

use crate::catalog::writer::{apply, CatalogOp};
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::ingest::record::{record_snapshot_id, stream_record, RecordItem};
use crate::manifest::{manifest_snapshot, ManifestOptions};
use crate::model::{ContentRow, UnitRow};
use crate::repo::Repo;
use crate::sniff::{image_dims, Format, SNIFF_LEN};
use crate::unitread::UnitReader;
use crate::verify::worker_count;
use rusqlite::Connection;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct RecoverOptions {
    /// Pack-scanning threads; 0 means one per available CPU.
    pub workers: usize,
}

/// A recovered snapshot whose files are not all intact.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DamagedSnapshot {
    pub id: Id,
    /// Paths of the file entries whose content lost units, sorted bytewise.
    pub files: Vec<Vec<u8>>,
}

/// A `snapshot_record` unit whose snapshot could not be rebuilt.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LostRecord {
    pub unit: Hash32,
    /// The snapshot, when the record's index unit names it.
    pub snapshot: Option<Id>,
    pub why: String,
}

/// Damaged pack bytes `[start, end)` that hold no record recover could even
/// try (no valid record header), and that may have held data: what they
/// held, a snapshot record included, is unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UnaccountedRange {
    pub pack: PathBuf,
    pub pack_id: Id,
    pub start: u64,
    pub end: u64,
}

/// The end `[start, end)` of an unsealed pack after its last intact record:
/// an unfinished write, ignored and left in place.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TornTail {
    pub pack: PathBuf,
    pub pack_id: Id,
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct RecoverReport {
    /// Pack files given a `pack` row.
    pub packs: u64,
    /// `unit` rows: distinct intact units.
    pub units: u64,
    /// Bytes read from pack files (the length of each pack given a row).
    pub bytes_read: u64,
    /// Recovered snapshots, damaged included, in id order.
    pub snapshots: Vec<Id>,
    pub damaged: Vec<DamagedSnapshot>,
    pub lost: Vec<LostRecord>,
    /// Damaged bytes that may have held a unit or snapshot record that
    /// nothing else accounts for, in pack path and offset order.
    pub unaccounted: Vec<UnaccountedRange>,
    /// Unfinished writes ignored at the end of unsealed packs (information:
    /// a crash leaves these, and they cost nothing).
    pub torn_tails: Vec<TornTail>,
    /// Problems that cost no file.
    pub warnings: Vec<String>,
    /// Snapshots the old catalog held that were not recovered (filled by
    /// the caller that swaps the catalog in).
    pub not_recovered: Vec<Id>,
    /// Where the old catalog was preserved (filled by the same caller).
    pub old_catalog: Option<PathBuf>,
    /// Wall-clock time of each step of the rebuild.
    pub steps: StepTimes,
    pub elapsed_ms: u64,
}

/// Wall-clock milliseconds of each step of the rebuild, in order. The
/// total (`RecoverReport::elapsed_ms`) also covers setup and the final
/// checkpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StepTimes {
    /// Salvaging and verifying every pack (parallel).
    pub scan_ms: u64,
    /// Rebuilding snapshots from their records.
    pub replay_ms: u64,
    /// Measuring image dimensions.
    pub media_ms: u64,
    /// Marking damaged snapshots.
    pub damage_ms: u64,
    /// Integrity, foreign-key and manifest checks of the new catalog.
    pub check_ms: u64,
}

/// Milliseconds since `*mark`, moving `*mark` to now.
fn lap(mark: &mut Instant) -> u64 {
    let now = Instant::now();
    let ms = now.duration_since(*mark).as_millis() as u64;
    *mark = now;
    ms
}

impl RecoverReport {
    /// Every recovered snapshot is intact, no snapshot record was lost, and
    /// no damaged bytes could have held one.
    pub fn intact(&self) -> bool {
        self.damaged.is_empty() && self.lost.is_empty() && self.unaccounted.is_empty()
    }
}

/// Image formats ingest measures (`image_dims` returns `None` for others).
const MEDIA_FORMATS: [Format; 5] = [
    Format::Jpeg,
    Format::Png,
    Format::Webp,
    Format::Gif,
    Format::Avif,
];

/// Build a complete, checked catalog at `dest` (which must not exist, nor
/// its `-wal`, `-shm` or `-journal`) from the packs. Does not touch
/// `catalog.sqlite` or `wmk.toml`, and never writes packs.
///
/// On `Err`, `dest` may exist, partly written and unchecked: the caller
/// must delete it (and its `-wal` / `-shm`) and must not use it.
pub fn build(repo: &Repo, dest: &Path, opts: &RecoverOptions) -> Result<RecoverReport> {
    build_with(repo, dest, opts, &HashMap::new())
}

/// `build`, knowing each pack's size in the previous catalog (`old_sizes`,
/// possibly empty): an unsealed pack's bytes below it were committed.
fn build_with(
    repo: &Repo,
    dest: &Path,
    opts: &RecoverOptions,
    old_sizes: &HashMap<Id, u64>,
) -> Result<RecoverReport> {
    let start = Instant::now();
    check_dest(repo, dest)?;
    let mut report = RecoverReport::default();
    // Before `dest` is created: a misplaced pack stops recover outright.
    let packs = scan::list_packs(&repo.packs_dir(), &mut report.warnings)?;
    let cat = Catalog::open(dest)?;
    let mut mark = Instant::now();
    let scanned = scan::scan(
        &cat,
        &packs,
        worker_count(opts.workers),
        old_sizes,
        &mut report.warnings,
    )?;
    report.packs = scanned.packs;
    report.units = scanned.units;
    report.bytes_read = scanned.bytes_read;
    report.unaccounted = scanned.unaccounted;
    report.torn_tails = scanned.torn_tails;
    for (unit, why) in scanned.lost_records {
        report.lost.push(LostRecord {
            unit,
            snapshot: None,
            why: format!("record unit is corrupt: {why}"),
        });
    }

    // No cache: each unit is read about once (a record's small index twice).
    report.steps.scan_ms = lap(&mut mark);
    let reader = UnitReader::new(repo, 0);
    replay(&reader, &cat, &mut report)?;
    report.steps.replay_ms = lap(&mut mark);
    media(&reader, &cat, worker_count(opts.workers))?;
    report.steps.media_ms = lap(&mut mark);
    mark_damage(&cat, &mut report)?;
    report.steps.damage_ms = lap(&mut mark);
    check(&cat, &report.snapshots)?;
    report.steps.check_ms = lap(&mut mark);
    let busy: i64 = cat
        .conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
    if busy != 0 {
        return Err(Error::Corrupt(
            "rebuilt catalog could not be fully checkpointed".into(),
        ));
    }
    // Closing the last connection removes the (now empty) -wal and -shm.
    cat.conn.close().map_err(|(_, e)| e)?;
    report.elapsed_ms = start.elapsed().as_millis() as u64;
    Ok(report)
}

/// Rebuild the catalog from the packs and swap it in, keeping the old one.
///
/// Holds the writer lock throughout. The new catalog is built in `tmp/`;
/// until the final rename, any error removes it and leaves `catalog.sqlite`
/// (with its `-wal` / `-shm`) and `wmk.toml` as they were. The old catalog is
/// never deleted: it is hard-linked to `catalog.sqlite.pre-recover-<stamp>`.
/// Once the rename has happened, later problems are warnings in the report.
pub fn run(repo: &Repo, opts: &RecoverOptions) -> Result<RecoverReport> {
    let _lock = repo.lock_writer()?;
    repo.sweep_tmp()?;
    let tmp_dir = repo.tmp_dir();
    sweep_stale_catalogs(&tmp_dir)?;
    let (old_ids, old_sizes) = read_old(repo);

    std::fs::create_dir_all(&tmp_dir).map_err(|e| Error::io(&tmp_dir, e))?;
    let tmp = tmp_dir.join(format!("recover-{}.sqlite", uuid::Uuid::now_v7()));
    let built = build_with(repo, &tmp, opts, &old_sizes).and_then(|report| {
        // Only now: a failed build must not have touched the old catalog.
        checkpoint_old(&repo.catalog_path());
        Ok((swap_in(repo, &tmp)?, report))
    });
    let ((old_catalog, warnings), mut report) = match built {
        Ok(v) => v,
        Err(e) => {
            remove_sqlite_files(&tmp);
            return Err(e);
        }
    };
    report.old_catalog = old_catalog;
    report.warnings.extend(warnings);
    report.not_recovered = old_ids
        .into_iter()
        .filter(|id| !report.snapshots.contains(id))
        .collect();

    let mut config = repo.config.clone();
    config.format_version = 2;
    if let Err(e) = config.save(&repo.root.join("wmk.toml")) {
        report.warnings.push(format!(
            "new catalog is in place; could not set wmk.toml format_version to 2: {e}; \
             run `wmk recover` again"
        ));
    }
    Ok(report)
}

/// Delete `recover-*.sqlite` (and sidecars) left in `tmp/` by a crashed run.
/// The caller must hold the writer lock.
pub(crate) fn sweep_stale_catalogs(dir: &Path) -> Result<()> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::io(dir, e)),
    };
    for entry in rd {
        let entry = entry.map_err(|e| Error::io(dir, e))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let stale = name.starts_with("recover-")
            && ["", "-wal", "-shm", "-journal"]
                .iter()
                .any(|s| name.ends_with(&format!(".sqlite{s}")));
        if stale {
            let p = entry.path();
            std::fs::remove_file(&p).map_err(|e| Error::io(&p, e))?;
        }
    }
    Ok(())
}

/// Snapshot ids in the old catalog, and each pack's committed size: the
/// largest `pack.size` that the old catalog or any kept
/// `catalog.sqlite.pre-recover-*` records for it. All best effort: an
/// unreadable catalog contributes nothing.
///
/// The backups matter because a rebuilt catalog records an unsealed pack
/// only up to its last intact record: read alone on a later run, it would
/// turn committed bytes lost before into a torn tail. Packs are append-only
/// and backups are never deleted, so the maximum is safe.
fn read_old(repo: &Repo) -> (Vec<Id>, HashMap<Id, u64>) {
    let path = repo.catalog_path();
    let old = path
        .exists()
        .then(|| Catalog::open_readonly(&path).ok())
        .flatten();
    let ids = old
        .as_ref()
        .and_then(|c| c.snapshots().ok())
        .map_or_else(Vec::new, |rows| rows.into_iter().map(|r| r.id).collect());
    let mut sizes = HashMap::new();
    if let Some(cat) = &old {
        add_pack_sizes(&cat.conn, &mut sizes);
    }
    for backup in backup_catalogs(repo) {
        if let Ok(conn) = open_backup(&backup) {
            add_pack_sizes(&conn, &mut sizes);
        }
    }
    (ids, sizes)
}

/// Open a kept catalog read-only without creating files beside it: as
/// immutable, unless it has a non-empty `-wal` of its own, whose rows count,
/// or its path is not UTF-8, which a URI cannot carry. An ordinary read-only
/// open then adds a `-shm` beside it.
fn open_backup(path: &Path) -> Result<Connection> {
    use rusqlite::OpenFlags;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let has_wal = std::fs::metadata(with_suffix(path, "-wal")).is_ok_and(|m| m.len() > 0);
    match immutable_uri(path) {
        Some(uri) if !has_wal => Ok(Connection::open_with_flags(
            uri,
            flags | OpenFlags::SQLITE_OPEN_URI,
        )?),
        _ => Ok(Connection::open_with_flags(path, flags)?),
    }
}

/// `file:<path>?immutable=1`, percent-encoding what a URI path would
/// misread; `None` when `path` is not UTF-8.
fn immutable_uri(path: &Path) -> Option<String> {
    let mut uri = String::from("file:");
    for c in path.to_str()?.chars() {
        match c {
            '%' => uri.push_str("%25"),
            '?' => uri.push_str("%3f"),
            '#' => uri.push_str("%23"),
            c => uri.push(c),
        }
    }
    uri.push_str("?immutable=1");
    Some(uri)
}

/// Raise each pack's entry in `sizes` to the size `conn`'s catalog records,
/// if larger.
fn add_pack_sizes(conn: &Connection, sizes: &mut HashMap<Id, u64>) {
    let _ = (|| -> Result<()> {
        let mut stmt = conn.prepare("SELECT id, size FROM pack")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, Id>(0)?, r.get::<_, i64>(1)?)))?;
        for row in rows {
            let (id, size) = row?;
            let size = u64::try_from(size).unwrap_or(0);
            let e = sizes.entry(id).or_insert(0);
            *e = (*e).max(size);
        }
        Ok(())
    })();
}

/// The kept `catalog.sqlite.pre-recover-*` catalogs in the repository root
/// (their `-wal`, `-shm` and `-journal` sidecars excluded), best effort.
fn backup_catalogs(repo: &Repo) -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(&repo.root) else {
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = rd
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.starts_with("catalog.sqlite.pre-recover-")
                && !SIDECARS.iter().any(|s| name.ends_with(s))
                && e.file_type().is_ok_and(|t| t.is_file())
        })
        .map(|e| e.path())
        .collect();
    out.sort();
    out
}

/// Fold the old catalog's WAL into it, best effort; never creates the file.
/// An empty file is left alone: SQLite deletes the `-wal` of an empty
/// database it opens read-write, and `swap_in` keeps that `-wal` aside.
fn checkpoint_old(path: &Path) {
    if !std::fs::metadata(path).is_ok_and(|m| m.len() > 0) {
        return;
    }
    if let Ok(conn) = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
    {
        let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
        let _ = conn.close();
    }
}

fn with_suffix(p: &Path, suffix: &str) -> PathBuf {
    let mut s = p.as_os_str().to_os_string();
    s.push(suffix);
    PathBuf::from(s)
}

fn remove_sqlite_files(p: &Path) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(with_suffix(p, suffix));
    }
}

/// Sidecar files SQLite keeps beside a database.
const SIDECARS: [&str; 3] = ["-wal", "-shm", "-journal"];

/// Preserve whatever is left of the old catalog and rename `tmp` over
/// `catalog.sqlite`. Returns the backup path (`None` when there was no
/// `catalog.sqlite`) and warnings: sidecars kept aside without a main file,
/// and a failed directory fsync.
///
/// Every old sidecar is moved aside, even with no main file: a stale `-wal`
/// beside the new catalog would be replayed into it. On error before the
/// rename, everything is put back.
fn swap_in(repo: &Repo, tmp: &Path) -> Result<(Option<PathBuf>, Vec<String>)> {
    let cat = repo.catalog_path();
    let has_main = match std::fs::symlink_metadata(&cat) {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(Error::io(&cat, e)),
    };
    let sidecars: Vec<&str> = SIDECARS
        .into_iter()
        .filter(|s| std::fs::symlink_metadata(with_suffix(&cat, s)).is_ok())
        .collect();
    let base = if has_main || !sidecars.is_empty() {
        Some(reserve_backup(&cat, &utc_stamp(), has_main)?)
    } else {
        None
    };

    let mut moved: Vec<&str> = Vec::new();
    let swapped = (|| -> Result<()> {
        if let Some(b) = &base {
            for suffix in &sidecars {
                let from = with_suffix(&cat, suffix);
                std::fs::rename(&from, with_suffix(b, suffix)).map_err(|e| Error::io(&from, e))?;
                moved.push(suffix);
            }
        }
        fail::fail_point!("recover::before_rename", |_| Err(Error::io(
            &cat,
            std::io::Error::other("failpoint recover::before_rename")
        )));
        std::fs::rename(tmp, &cat).map_err(|e| Error::io(&cat, e))
    })();
    if let Err(e) = swapped {
        let mut stuck = Vec::new();
        if let Some(b) = &base {
            for suffix in moved {
                if let Err(u) = std::fs::rename(with_suffix(b, suffix), with_suffix(&cat, suffix)) {
                    stuck.push(format!("{}: {u}", with_suffix(b, suffix).display()));
                }
            }
            // Keep the link if anything could not be put back.
            if has_main && stuck.is_empty() {
                let _ = std::fs::remove_file(b);
            }
        }
        return Err(if stuck.is_empty() {
            e
        } else {
            Error::Corrupt(format!(
                "{e}; additionally could not move back: {}",
                stuck.join("; ")
            ))
        });
    }
    let mut warnings = Vec::new();
    if let (Some(b), false) = (&base, has_main) {
        let kept: Vec<String> = sidecars
            .iter()
            .map(|s| crate::display::display_path(&with_suffix(b, s)))
            .collect();
        warnings.push(format!(
            "catalog.sqlite was missing but its leftover files were kept aside, unused: {}",
            kept.join(", ")
        ));
    }
    if let Err(e) = sync_root(repo) {
        warnings.push(format!(
            "new catalog is in place but the repository directory could not be synced: {e}"
        ));
    }
    let backup = base.filter(|_| has_main);
    Ok((backup, warnings))
}

/// Make the rename of the new catalog durable.
fn sync_root(repo: &Repo) -> Result<()> {
    fail::fail_point!("recover::after_rename", |_| Err(Error::io(
        &repo.root,
        std::io::Error::other("failpoint recover::after_rename")
    )));
    crate::pack::fsync_dir(&repo.root)
}

/// Pick the first `<cat>.pre-recover-<stamp>[-n]` whose name and sidecar
/// names are all free and, when `link_main`, hard-link `cat` to it.
fn reserve_backup(cat: &Path, stamp: &str, link_main: bool) -> Result<PathBuf> {
    let base = with_suffix(cat, &format!(".pre-recover-{stamp}"));
    for n in 1u32.. {
        let target = if n == 1 {
            base.clone()
        } else {
            with_suffix(&base, &format!("-{n}"))
        };
        let taken = std::iter::once("")
            .chain(SIDECARS)
            .any(|s| std::fs::symlink_metadata(with_suffix(&target, s)).is_ok());
        if taken {
            continue;
        }
        if !link_main {
            return Ok(target);
        }
        match std::fs::hard_link(cat, &target) {
            Ok(()) => return Ok(target),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(Error::io(
                    &target,
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "cannot preserve the old catalog: filesystem refused a hard link: {e}"
                        ),
                    ),
                ))
            }
        }
    }
    unreachable!("the suffix counter does not run out")
}

/// Now as `YYYYMMDDTHHMMSSZ` (UTC).
fn utc_stamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn check_dest(repo: &Repo, dest: &Path) -> Result<()> {
    if dest == repo.catalog_path() {
        return Err(Error::Corrupt(
            "recover must build its catalog beside catalog.sqlite, not over it".into(),
        ));
    }
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut p = dest.as_os_str().to_os_string();
        p.push(suffix);
        let p = PathBuf::from(p);
        match std::fs::symlink_metadata(&p) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::io(&p, e)),
            Ok(_) => {
                return Err(Error::io(
                    &p,
                    std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        "recover builds a new catalog and will not reuse an existing file",
                    ),
                ))
            }
        }
    }
    Ok(())
}

/// Errors that say nothing about the record itself (the disk, the catalog
/// file): these fail the whole build rather than lose a snapshot.
fn fatal(e: &Error) -> bool {
    match e {
        Error::Io { .. } => true,
        Error::Sqlite(e) => e.sqlite_error_code() != Some(rusqlite::ErrorCode::ConstraintViolation),
        _ => false,
    }
}

fn why(e: Error) -> String {
    match e {
        Error::Corrupt(m) => m,
        e => e.to_string(),
    }
}

/// Step 2: rebuild each snapshot from its record.
fn replay(reader: &UnitReader, cat: &Catalog, report: &mut RecoverReport) -> Result<()> {
    let records: Vec<Hash32> = {
        let mut stmt = cat
            .conn
            .prepare("SELECT hash FROM unit WHERE kind = 'snapshot_record' ORDER BY hash")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    // Group by snapshot id first: two different records for one snapshot is
    // corruption, and neither may be used.
    let mut by_id: BTreeMap<Id, Vec<Hash32>> = BTreeMap::new();
    for unit in records {
        match record_snapshot_id(reader, cat, &unit) {
            Ok(id) => by_id.entry(id).or_default().push(unit),
            Err(e) if fatal(&e) => return Err(e),
            Err(e) => report.lost.push(LostRecord {
                unit,
                snapshot: None,
                why: why(e),
            }),
        }
    }
    for (id, units) in by_id {
        if units.len() > 1 {
            for unit in &units {
                report.lost.push(LostRecord {
                    unit: *unit,
                    snapshot: Some(id),
                    why: format!(
                        "snapshot {id} has {} different records; none is used",
                        units.len()
                    ),
                });
            }
            continue;
        }
        match replay_one(reader, cat, units[0]) {
            Ok(()) => report.snapshots.push(id),
            Err(e) if fatal(&e) => return Err(e),
            Err(e) => report.lost.push(LostRecord {
                unit: units[0],
                snapshot: Some(id),
                why: why(e),
            }),
        }
    }
    Ok(())
}

/// Insert one record's rows in one transaction, committed only after the
/// record fully verified (and its rows satisfy every constraint); on any
/// error everything is rolled back.
fn replay_one(reader: &UnitReader, cat: &Catalog, unit: Hash32) -> Result<()> {
    let conn = &cat.conn;
    // Entries' parents and contents may follow the rows that reference them.
    conn.execute_batch("BEGIN; PRAGMA defer_foreign_keys = ON;")?;
    let r = stream_record(reader, cat, &unit, |item| insert_item(cat, unit, item))
        .and_then(|_| Ok(conn.execute_batch("COMMIT")?));
    if r.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    r
}

fn insert_item(cat: &Catalog, unit: Hash32, item: RecordItem) -> Result<()> {
    let conn = &cat.conn;
    match item {
        RecordItem::Header(mut row) => {
            if row.manifest_hash.is_none() {
                return Err(Error::Corrupt(
                    "record has no manifest hash to check the rebuilt rows against".into(),
                ));
            }
            row.record_unit = Some(unit);
            apply(conn, CatalogOp::UpsertSnapshot(row))
        }
        RecordItem::Entry(e) => apply(conn, CatalogOp::InsertEntry(e)),
        RecordItem::Content(c, units) => insert_content(cat, c, &units),
        RecordItem::Xattr(entry, name, value) => {
            apply(conn, CatalogOp::InsertXattr(entry, name, value))
        }
        RecordItem::Provenance(_) | RecordItem::ScreenHit(_) => Err(Error::Corrupt(
            "provenance/screening records are not supported yet".into(),
        )),
    }
}

/// The content row, then `content_unit` rows for the units present, so a
/// damaged content lists fewer units than its `unit_count`. A content
/// another snapshot already inserted keeps that snapshot's unit list.
fn insert_content(cat: &Catalog, c: ContentRow, units: &[Hash32]) -> Result<()> {
    let conn = &cat.conn;
    let inserted = conn
        .prepare_cached(
            "INSERT OR IGNORE INTO content(hash, size, format, profile, unit_count) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?
        .execute(rusqlite::params![
            c.hash,
            c.size,
            c.format.map(|f| f.as_str()),
            c.profile.as_str(),
            c.unit_count,
        ])?;
    if inserted == 0 {
        return Ok(());
    }
    let hash = c.hash;
    let mut stmt = conn.prepare_cached(
        "INSERT INTO content_unit(content_hash, seq, unit_hash) VALUES (?1, ?2, ?3)",
    )?;
    for (seq, u) in units.iter().enumerate() {
        if cat.has_unit(u)? {
            stmt.execute(rusqlite::params![hash, seq as i64, u])?;
        }
    }
    Ok(())
}

/// An image `media` measures, with the units holding its leading bytes.
struct MediaJob {
    content: Hash32,
    format: Format,
    units: Vec<UnitRow>,
}

/// Step 3: a `media` row for each image whose leading bytes are present,
/// measured exactly as ingest measures it. The catalog is read and written
/// on this thread; the units are read and measured on `workers` threads.
fn media(reader: &UnitReader, cat: &Catalog, workers: usize) -> Result<()> {
    let jobs = media_jobs(cat)?;
    let measured = measure(reader, &jobs, workers)?;
    let conn = &cat.conn;
    conn.execute_batch("BEGIN")?;
    let r = (|| {
        for (content, width, height) in measured {
            apply(
                conn,
                CatalogOp::InsertMedia {
                    content,
                    width,
                    height,
                },
            )?;
        }
        Ok(conn.execute_batch("COMMIT")?)
    })();
    if r.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    r
}

/// Every image content whose leading units are all present.
fn media_jobs(cat: &Catalog) -> Result<Vec<MediaJob>> {
    let formats: Vec<String> = MEDIA_FORMATS
        .iter()
        .map(|f| format!("'{}'", f.as_str()))
        .collect();
    let mut stmt = cat.conn.prepare(&format!(
        "SELECT hash, format, unit_count FROM content WHERE format IN ({})",
        formats.join(",")
    ))?;
    let mut rows = stmt.query([])?;
    let mut jobs = Vec::new();
    while let Some(row) = rows.next()? {
        let content: Hash32 = row.get(0)?;
        let format: String = row.get(1)?;
        let unit_count: i64 = row.get(2)?;
        let Some(format) = Format::from_str(&format) else {
            continue;
        };
        if let Some(units) = head_units(cat, &content, unit_count)? {
            jobs.push(MediaJob {
                content,
                format,
                units,
            });
        }
    }
    Ok(jobs)
}

/// The units holding the first `SNIFF_LEN` bytes of `content` (all of it
/// if shorter), or `None` if one of them is missing.
fn head_units(cat: &Catalog, content: &Hash32, unit_count: i64) -> Result<Option<Vec<UnitRow>>> {
    let mut stmt = cat.conn.prepare_cached(
        "SELECT seq, unit_hash FROM content_unit WHERE content_hash = ?1 ORDER BY seq",
    )?;
    let mut rows = stmt.query([content])?;
    let mut units = Vec::new();
    let mut len = 0u64;
    let mut seq = 0i64;
    while len < SNIFF_LEN && seq < unit_count {
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        if row.get::<_, i64>(0)? != seq {
            return Ok(None);
        }
        let unit = cat.unit(&row.get(1)?)?;
        len += u64::try_from(unit.raw_size).unwrap_or(0);
        units.push(unit);
        seq += 1;
    }
    Ok(Some(units))
}

/// `(content, width, height)` of each job `image_dims` can measure, in
/// content order. Stops at the first read error, which it returns.
fn measure(
    reader: &UnitReader,
    jobs: &[MediaJob],
    workers: usize,
) -> Result<Vec<(Hash32, u32, u32)>> {
    let next = AtomicUsize::new(0);
    let abort = AtomicBool::new(false);
    let per_worker: Vec<Result<Vec<(Hash32, u32, u32)>>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers.clamp(1, jobs.len().max(1)))
            .map(|_| {
                let (next, abort) = (&next, &abort);
                s.spawn(move || {
                    let mut out = Vec::new();
                    while !abort.load(Ordering::Relaxed) {
                        let Some(job) = jobs.get(next.fetch_add(1, Ordering::Relaxed)) else {
                            break;
                        };
                        match head(reader, &job.units) {
                            Ok(head) => {
                                if let Some((w, h)) = image_dims(job.format, &head) {
                                    out.push((job.content, w, h));
                                }
                            }
                            Err(e) => {
                                abort.store(true, Ordering::Relaxed);
                                return Err(e);
                            }
                        }
                    }
                    Ok(out)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
            .collect()
    });
    let mut all = Vec::new();
    for r in per_worker {
        all.extend(r?);
    }
    all.sort_unstable_by_key(|(content, _, _)| *content);
    Ok(all)
}

/// The first `SNIFF_LEN` bytes held by `units`, each read and verified.
fn head(reader: &UnitReader, units: &[UnitRow]) -> Result<Vec<u8>> {
    let mut head = Vec::new();
    for unit in units {
        let data = reader.read_unit(unit)?;
        let want = (SNIFF_LEN as usize - head.len()).min(data.len());
        head.extend_from_slice(&data[..want]);
    }
    Ok(head)
}

/// Step 4: mark each snapshot with an incomplete content `damaged` and list
/// the files that lost units.
fn mark_damage(cat: &Catalog, report: &mut RecoverReport) -> Result<()> {
    let conn = &cat.conn;
    for &id in &report.snapshots {
        let incomplete = cat.incomplete_contents(id)?;
        if incomplete.is_empty() {
            continue;
        }
        conn.execute("UPDATE snapshot SET status = 'damaged' WHERE id = ?1", [id])?;
        let mut stmt = conn.prepare_cached(
            "SELECT path FROM entry WHERE snapshot_id = ?1 AND kind = 'file' \
             AND content_hash = ?2",
        )?;
        let mut files = Vec::new();
        for h in &incomplete {
            let rows = stmt.query_map(rusqlite::params![id, h], |r| r.get::<_, Vec<u8>>(0))?;
            for p in rows {
                files.push(p?);
            }
        }
        files.sort();
        report.damaged.push(DamagedSnapshot { id, files });
    }
    Ok(())
}

/// Step 5: the rebuilt catalog is sound and reproduces every recovered
/// snapshot's manifest hash.
fn check(cat: &Catalog, snapshots: &[Id]) -> Result<()> {
    let failed = |what: String| Error::Corrupt(format!("rebuilt catalog failed its check: {what}"));
    let integrity = column_strings(&cat.conn, "PRAGMA integrity_check")?;
    if integrity != ["ok"] {
        return Err(failed(format!("integrity_check: {}", integrity.join("; "))));
    }
    let fk = column_strings(&cat.conn, "PRAGMA foreign_key_check")?;
    if !fk.is_empty() {
        return Err(failed(format!(
            "foreign_key_check: {} rows violate a foreign key (tables {})",
            fk.len(),
            fk.join(", ")
        )));
    }
    let opts = ManifestOptions {
        mtime_precision_ns: 1,
        cross_fs: false,
    };
    for &id in snapshots {
        let stored = cat.snapshot(id)?.manifest_hash;
        let computed = manifest_snapshot(cat, id, b"", &opts)?.core;
        if stored != Some(computed) {
            return Err(failed(format!(
                "snapshot {id}: manifest {computed} does not match the recorded hash"
            )));
        }
    }
    Ok(())
}

/// The first column of every row `sql` returns, as text.
fn column_strings(conn: &Connection, sql: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_uri_encodes_specials_and_refuses_non_utf8() {
        assert_eq!(
            immutable_uri(Path::new("/r/a%b?c#d/catalog.sqlite.pre-recover-x")).as_deref(),
            Some("file:/r/a%25b%3fc%23d/catalog.sqlite.pre-recover-x?immutable=1")
        );
        use std::os::unix::ffi::OsStrExt;
        let odd = Path::new(std::ffi::OsStr::from_bytes(b"/r/\xff/catalog.sqlite"));
        assert_eq!(immutable_uri(odd), None);
    }

    #[test]
    fn same_stamp_gets_distinct_backups_and_skips_taken_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let cat = dir.path().join("catalog.sqlite");
        std::fs::write(&cat, b"db").unwrap();
        let stamp = "20260101T000000Z";
        let a = reserve_backup(&cat, stamp, true).unwrap();
        let b = reserve_backup(&cat, stamp, true).unwrap();
        assert_ne!(a, b);
        assert!(b.to_string_lossy().ends_with("Z-2"));
        // A base whose sidecar name exists is taken too.
        std::fs::write(with_suffix(&with_suffix(&a, "-3"), "-wal"), b"w").unwrap();
        let c = reserve_backup(&cat, stamp, true).unwrap();
        assert!(c.to_string_lossy().ends_with("Z-4"), "{c:?}");
        // Without a main file the base is only reserved by name.
        let d = reserve_backup(&cat, stamp, false).unwrap();
        assert!(d.to_string_lossy().ends_with("Z-5"), "{d:?}");
        assert!(!d.exists());
    }

    #[test]
    fn utc_stamp_has_the_documented_shape() {
        let s = utc_stamp();
        assert_eq!(s.len(), 16);
        assert!(s.ends_with('Z') && s.as_bytes()[8] == b'T');
    }
}
