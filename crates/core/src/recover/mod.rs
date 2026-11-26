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
use crate::model::ContentRow;
use crate::repo::Repo;
use crate::sniff::{image_dims, Format, SNIFF_LEN};
use crate::unitread::UnitReader;
use crate::verify::worker_count;
use rusqlite::Connection;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct RecoverOptions {
    /// Pack-scanning threads; 0 means one per available CPU.
    pub workers: usize,
}

/// A recovered snapshot whose files are not all intact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DamagedSnapshot {
    pub id: Id,
    /// Paths of the file entries whose content lost units, sorted bytewise.
    pub files: Vec<Vec<u8>>,
}

/// A `snapshot_record` unit whose snapshot could not be rebuilt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostRecord {
    pub unit: Hash32,
    /// The snapshot, when the record's index unit names it.
    pub snapshot: Option<Id>,
    pub why: String,
}

#[derive(Debug, Clone, Default)]
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
    /// Problems that cost no file.
    pub warnings: Vec<String>,
    /// Snapshots the old catalog held that were not recovered (filled by
    /// the caller that swaps the catalog in).
    pub not_recovered: Vec<Id>,
    /// Where the old catalog was preserved (filled by the same caller).
    pub old_catalog: Option<PathBuf>,
    pub elapsed_ms: u64,
}

impl RecoverReport {
    /// Every recovered snapshot is intact and no snapshot record was lost.
    pub fn intact(&self) -> bool {
        self.damaged.is_empty() && self.lost.is_empty()
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
    let start = Instant::now();
    check_dest(repo, dest)?;
    let mut report = RecoverReport::default();
    // Before `dest` is created: a misplaced pack stops recover outright.
    let packs = scan::list_packs(&repo.packs_dir(), &mut report.warnings)?;
    let cat = Catalog::open(dest)?;
    let scanned = scan::scan(
        &cat,
        &packs,
        worker_count(opts.workers),
        &mut report.warnings,
    )?;
    report.packs = scanned.packs;
    report.units = scanned.units;
    report.bytes_read = scanned.bytes_read;
    for (unit, why) in scanned.lost_records {
        report.lost.push(LostRecord {
            unit,
            snapshot: None,
            why: format!("record unit is corrupt: {why}"),
        });
    }

    // No cache: each unit is read about once (a record's small index twice).
    let reader = UnitReader::new(repo, 0);
    replay(&reader, &cat, &mut report)?;
    media(&reader, &cat)?;
    mark_damage(&cat, &mut report)?;
    check(&cat, &report.snapshots)?;
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
pub fn run(repo: &Repo, opts: &RecoverOptions) -> Result<RecoverReport> {
    let _lock = repo.lock_writer()?;
    repo.sweep_tmp()?;
    let old_ids = old_snapshot_ids(&repo.catalog_path());
    checkpoint_old(&repo.catalog_path());

    let tmp_dir = repo.tmp_dir();
    std::fs::create_dir_all(&tmp_dir).map_err(|e| Error::io(&tmp_dir, e))?;
    let tmp = tmp_dir.join(format!("recover-{}.sqlite", uuid::Uuid::now_v7()));
    let built = build(repo, &tmp, opts).and_then(|report| Ok((swap_in(repo, &tmp)?, report)));
    let (old_catalog, mut report) = match built {
        Ok(v) => v,
        Err(e) => {
            remove_sqlite_files(&tmp);
            return Err(e);
        }
    };
    report.old_catalog = old_catalog;
    report.not_recovered = old_ids
        .into_iter()
        .filter(|id| !report.snapshots.contains(id))
        .collect();

    let mut config = repo.config.clone();
    config.format_version = 2;
    config.save(&repo.root.join("wmk.toml"))?;
    Ok(report)
}

/// Snapshot ids in the old catalog, or none when it cannot be read.
fn old_snapshot_ids(path: &Path) -> Vec<Id> {
    if !path.exists() {
        return Vec::new();
    }
    match Catalog::open_readonly(path).and_then(|c| c.snapshots()) {
        Ok(rows) => rows.into_iter().map(|r| r.id).collect(),
        Err(_) => Vec::new(),
    }
}

/// Fold the old catalog's WAL into it, best effort; never creates the file.
fn checkpoint_old(path: &Path) {
    if !path.exists() {
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

/// Preserve the old catalog (if any) and rename `tmp` over `catalog.sqlite`.
/// Returns the backup path. On error the old catalog is back in place.
fn swap_in(repo: &Repo, tmp: &Path) -> Result<Option<PathBuf>> {
    let cat = repo.catalog_path();
    let backup = match std::fs::symlink_metadata(&cat) {
        Ok(_) => Some(backup_link(&cat)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(Error::io(&cat, e)),
    };
    let mut moved: Vec<&str> = Vec::new();
    let swapped = (|| -> Result<()> {
        if let Some(b) = &backup {
            for suffix in ["-wal", "-shm"] {
                let from = with_suffix(&cat, suffix);
                if from.exists() {
                    let to = with_suffix(b, suffix);
                    std::fs::rename(&from, &to).map_err(|e| Error::io(&from, e))?;
                    moved.push(suffix);
                }
            }
        }
        std::fs::rename(tmp, &cat).map_err(|e| Error::io(&cat, e))?;
        crate::pack::fsync_dir(&repo.root)
    })();
    match swapped {
        Ok(()) => Ok(backup),
        Err(e) => {
            // Undo only while the new catalog has not replaced the old one.
            if let Some(b) = &backup {
                if std::fs::symlink_metadata(tmp).is_ok() {
                    for suffix in moved {
                        let _ = std::fs::rename(with_suffix(b, suffix), with_suffix(&cat, suffix));
                    }
                    let _ = std::fs::remove_file(b);
                }
            }
            Err(e)
        }
    }
}

/// Hard-link `cat` to the first free `<cat>.pre-recover-<stamp>[-n]`.
fn backup_link(cat: &Path) -> Result<PathBuf> {
    let base = with_suffix(cat, &format!(".pre-recover-{}", utc_stamp()));
    for n in 1u32.. {
        let target = if n == 1 {
            base.clone()
        } else {
            with_suffix(&base, &format!("-{n}"))
        };
        match std::fs::hard_link(cat, &target) {
            Ok(()) => return Ok(target),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(Error::io(&target, e)),
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

/// Step 3: a `media` row for each image whose leading bytes are present,
/// measured exactly as ingest measures it.
fn media(reader: &UnitReader, cat: &Catalog) -> Result<()> {
    let conn = &cat.conn;
    let formats: Vec<String> = MEDIA_FORMATS
        .iter()
        .map(|f| format!("'{}'", f.as_str()))
        .collect();
    conn.execute_batch("BEGIN")?;
    let r = (|| {
        let mut stmt = conn.prepare(&format!(
            "SELECT hash, format, unit_count FROM content WHERE format IN ({})",
            formats.join(",")
        ))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let hash: Hash32 = row.get(0)?;
            let format: String = row.get(1)?;
            let unit_count: i64 = row.get(2)?;
            let Some(format) = Format::from_str(&format) else {
                continue;
            };
            let Some(head) = head(reader, cat, &hash, unit_count)? else {
                continue;
            };
            if let Some((width, height)) = image_dims(format, &head) {
                apply(
                    conn,
                    CatalogOp::InsertMedia {
                        content: hash,
                        width,
                        height,
                    },
                )?;
            }
        }
        Ok(conn.execute_batch("COMMIT")?)
    })();
    if r.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    r
}

/// The first `SNIFF_LEN` bytes of `content` (all of it if shorter), or
/// `None` if a unit they need is missing.
fn head(
    reader: &UnitReader,
    cat: &Catalog,
    content: &Hash32,
    unit_count: i64,
) -> Result<Option<Vec<u8>>> {
    let mut stmt = cat.conn.prepare_cached(
        "SELECT seq, unit_hash FROM content_unit WHERE content_hash = ?1 ORDER BY seq",
    )?;
    let mut rows = stmt.query([content])?;
    let mut head = Vec::new();
    let mut seq = 0i64;
    while (head.len() as u64) < SNIFF_LEN && seq < unit_count {
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        if row.get::<_, i64>(0)? != seq {
            return Ok(None);
        }
        let data = reader.read_unit(&cat.unit(&row.get(1)?)?)?;
        let want = (SNIFF_LEN as usize - head.len()).min(data.len());
        head.extend_from_slice(&data[..want]);
        seq += 1;
    }
    Ok(Some(head))
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
