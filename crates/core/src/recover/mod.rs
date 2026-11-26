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
use std::time::Instant;

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
