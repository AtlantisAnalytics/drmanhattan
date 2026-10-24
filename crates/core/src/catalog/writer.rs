//! Single catalog writer thread. Ops arrive over a channel and are applied
//! inside one open transaction, which commits only on `Commit` or when the
//! channel closes. Callers must send rows in foreign-key order (pack, unit,
//! content, snapshot, entry parents before children, then xattr/media).

use super::Catalog;
use crate::error::Result;
use crate::ids::{Hash32, Id};
use crate::model::{ContentRow, EntryRow, PackRow, SnapshotRow, UnitRow};
use rusqlite::{params, Connection};
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::thread::JoinHandle;

#[derive(Debug)]
pub enum CatalogOp {
    UpsertSnapshot(SnapshotRow),
    InsertEntry(EntryRow),
    /// Content row plus its unit hashes in `seq` order.
    InsertContent(ContentRow, Vec<Hash32>),
    InsertUnit(UnitRow),
    UpsertPack(PackRow),
    InsertXattr(Id, Vec<u8>, Vec<u8>),
    InsertMedia {
        content: Hash32,
        width: u32,
        height: u32,
    },
    Commit,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CatalogStats {
    /// Data ops applied (every op except `Commit`).
    pub ops: u64,
    /// Transactions committed. Only non-empty transactions count: a `Commit`
    /// with no pending ops does nothing.
    pub commits: u64,
}

pub fn spawn_catalog_writer(
    path: PathBuf,
    rx: Receiver<CatalogOp>,
) -> JoinHandle<Result<CatalogStats>> {
    std::thread::spawn(move || run(path, rx))
}

fn run(path: PathBuf, rx: Receiver<CatalogOp>) -> Result<CatalogStats> {
    let cat = Catalog::open(&path)?;
    let conn = &cat.conn;
    let mut stats = CatalogStats::default();
    let mut open = false;
    for op in rx {
        if let CatalogOp::Commit = op {
            if open {
                conn.execute_batch("COMMIT")?;
                open = false;
                stats.commits += 1;
            }
            continue;
        }
        if !open {
            conn.execute_batch("BEGIN")?;
            open = true;
        }
        if let Err(e) = apply(conn, op) {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
        stats.ops += 1;
    }
    if open {
        conn.execute_batch("COMMIT")?;
        stats.commits += 1;
    }
    Ok(stats)
}

fn apply(conn: &Connection, op: CatalogOp) -> Result<()> {
    match op {
        CatalogOp::UpsertSnapshot(s) => upsert_snapshot(conn, &s)?,
        CatalogOp::InsertEntry(e) => {
            conn.prepare_cached(
                "INSERT INTO entry(id, snapshot_id, parent_id, name, path, kind, mode, \
                 mtime_ns, size, content_hash, link_target, hardlink_group, status) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            )?
            .execute(params![
                e.id,
                e.snapshot_id,
                e.parent_id,
                e.name,
                e.path,
                e.kind.as_str(),
                e.mode,
                e.mtime_ns,
                e.size,
                e.content_hash,
                e.link_target,
                e.hardlink_group,
                e.status.as_str(),
            ])?;
        }
        CatalogOp::InsertContent(c, units) => {
            conn.prepare_cached(
                "INSERT OR IGNORE INTO content(hash, size, format, profile, unit_count) \
                 VALUES (?1,?2,?3,?4,?5)",
            )?
            .execute(params![
                c.hash,
                c.size,
                c.format.map(|f| f.as_str()),
                c.profile.as_str(),
                c.unit_count,
            ])?;
            let mut stmt = conn.prepare_cached(
                "INSERT OR IGNORE INTO content_unit(content_hash, seq, unit_hash) \
                 VALUES (?1,?2,?3)",
            )?;
            for (seq, u) in units.iter().enumerate() {
                stmt.execute(params![c.hash, seq as i64, u])?;
            }
        }
        CatalogOp::InsertUnit(u) => {
            conn.prepare_cached(
                "INSERT OR IGNORE INTO unit(hash, kind, raw_size, stored_size, compression, \
                 pack_id, pack_offset) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            )?
            .execute(params![
                u.hash,
                u.kind.as_str(),
                u.raw_size,
                u.stored_size,
                u.compression.as_str(),
                u.pack_id,
                u.pack_offset,
            ])?;
        }
        CatalogOp::UpsertPack(p) => {
            conn.prepare_cached(
                "INSERT INTO pack(id, size, sealed, blake3) VALUES (?1,?2,?3,?4) \
                 ON CONFLICT(id) DO UPDATE SET size=excluded.size, sealed=excluded.sealed, \
                 blake3=excluded.blake3",
            )?
            .execute(params![p.id, p.size, p.sealed as i64, p.blake3])?;
        }
        CatalogOp::InsertXattr(entry, name, value) => {
            conn.prepare_cached("INSERT INTO xattr(entry_id, name, value) VALUES (?1,?2,?3)")?
                .execute(params![entry, name, value])?;
        }
        CatalogOp::InsertMedia {
            content,
            width,
            height,
        } => {
            conn.prepare_cached(
                "INSERT OR IGNORE INTO media(content_hash, width, height) VALUES (?1,?2,?3)",
            )?
            .execute(params![content, width, height])?;
        }
        CatalogOp::Commit => {}
    }
    Ok(())
}

/// Insert or fully replace a snapshot row. Shared by the writer thread's
/// `UpsertSnapshot` op and `Catalog::upsert_snapshot`.
pub(super) fn upsert_snapshot(conn: &Connection, s: &SnapshotRow) -> Result<()> {
    conn.prepare_cached(
        "INSERT INTO snapshot(id, status, source_root, host, tool_version, started_at, \
         completed_at, note, manifest_hash, ext_manifest_hash, source_manifest_hash, \
         record_unit, entry_count, file_bytes, screened_out, label_pattern) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16) \
         ON CONFLICT(id) DO UPDATE SET status=excluded.status, \
         source_root=excluded.source_root, host=excluded.host, \
         tool_version=excluded.tool_version, started_at=excluded.started_at, \
         completed_at=excluded.completed_at, note=excluded.note, \
         manifest_hash=excluded.manifest_hash, \
         ext_manifest_hash=excluded.ext_manifest_hash, \
         source_manifest_hash=excluded.source_manifest_hash, \
         record_unit=excluded.record_unit, entry_count=excluded.entry_count, \
         file_bytes=excluded.file_bytes, screened_out=excluded.screened_out, \
         label_pattern=excluded.label_pattern",
    )?
    .execute(params![
        s.id,
        s.status.as_str(),
        s.source_root,
        s.host,
        s.tool_version,
        s.started_at,
        s.completed_at,
        s.note,
        s.manifest_hash,
        s.ext_manifest_hash,
        s.source_manifest_hash,
        s.record_unit,
        s.entry_count,
        s.file_bytes,
        s.screened_out,
        s.label_pattern,
    ])?;
    Ok(())
}
