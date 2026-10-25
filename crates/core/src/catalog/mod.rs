//! SQLite catalog: connection setup, schema bootstrap and read queries.

pub mod schema;
pub mod writer;

use crate::compress::Compression;
use crate::display::display_bytes;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::{
    ContentRow, EntryKind, EntryRow, EntryStatus, PackRow, SnapshotRow, SnapshotStatus, UnitRow,
};
use crate::pack::UnitKind;
use crate::sniff::{Format, ProfileKind};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row};
use std::path::Path;
use std::time::Duration;

const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

impl ToSql for Hash32 {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Blob(&self.0)))
    }
}

impl FromSql for Hash32 {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let b = value.as_blob()?;
        let arr: [u8; 32] = b.try_into().map_err(|_| FromSqlError::InvalidBlobSize {
            expected_size: 32,
            blob_size: b.len(),
        })?;
        Ok(Hash32(arr))
    }
}

impl ToSql for Id {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Blob(&self.0)))
    }
}

impl FromSql for Id {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let b = value.as_blob()?;
        let arr: [u8; 16] = b.try_into().map_err(|_| FromSqlError::InvalidBlobSize {
            expected_size: 16,
            blob_size: b.len(),
        })?;
        Ok(Id(arr))
    }
}

fn bad(what: &str, v: &str) -> Error {
    Error::Corrupt(format!("catalog has unknown {what} {v:?}"))
}

const SNAPSHOT_COLS: &str = "id, status, source_root, host, tool_version, started_at, \
    completed_at, note, manifest_hash, ext_manifest_hash, source_manifest_hash, record_unit, \
    entry_count, file_bytes, screened_out, label_pattern";

const ENTRY_COLS: &str = "id, snapshot_id, parent_id, name, path, kind, mode, mtime_ns, size, \
    content_hash, link_target, hardlink_group, status";

const CONTENT_COLS: &str = "hash, size, format, profile, unit_count";

const UNIT_COLS: &str = "hash, kind, raw_size, stored_size, compression, pack_id, pack_offset";

fn snapshot_from_row(r: &Row<'_>) -> Result<SnapshotRow> {
    let status: String = r.get(1)?;
    Ok(SnapshotRow {
        id: r.get(0)?,
        status: SnapshotStatus::parse(&status).ok_or_else(|| bad("snapshot status", &status))?,
        source_root: r.get(2)?,
        host: r.get(3)?,
        tool_version: r.get(4)?,
        started_at: r.get(5)?,
        completed_at: r.get(6)?,
        note: r.get(7)?,
        manifest_hash: r.get(8)?,
        ext_manifest_hash: r.get(9)?,
        source_manifest_hash: r.get(10)?,
        record_unit: r.get(11)?,
        entry_count: r.get(12)?,
        file_bytes: r.get(13)?,
        screened_out: r.get(14)?,
        label_pattern: r.get(15)?,
    })
}

fn entry_from_row(r: &Row<'_>) -> Result<EntryRow> {
    let kind: String = r.get(5)?;
    let status: String = r.get(12)?;
    Ok(EntryRow {
        id: r.get(0)?,
        snapshot_id: r.get(1)?,
        parent_id: r.get(2)?,
        name: r.get(3)?,
        path: r.get(4)?,
        kind: EntryKind::parse(&kind).ok_or_else(|| bad("entry kind", &kind))?,
        mode: r.get(6)?,
        mtime_ns: r.get(7)?,
        size: r.get(8)?,
        content_hash: r.get(9)?,
        link_target: r.get(10)?,
        hardlink_group: r.get(11)?,
        status: EntryStatus::parse(&status).ok_or_else(|| bad("entry status", &status))?,
    })
}

fn content_from_row(r: &Row<'_>) -> Result<ContentRow> {
    let format: Option<String> = r.get(2)?;
    let profile: String = r.get(3)?;
    let format = match format {
        Some(s) => Some(Format::from_str(&s).ok_or_else(|| bad("content format", &s))?),
        None => None,
    };
    Ok(ContentRow {
        hash: r.get(0)?,
        size: r.get(1)?,
        format,
        profile: ProfileKind::parse(&profile).ok_or_else(|| bad("content profile", &profile))?,
        unit_count: r.get(4)?,
    })
}

fn unit_from_row(r: &Row<'_>) -> Result<UnitRow> {
    let kind: String = r.get(1)?;
    let compression: String = r.get(4)?;
    Ok(UnitRow {
        hash: r.get(0)?,
        kind: UnitKind::parse(&kind).ok_or_else(|| bad("unit kind", &kind))?,
        raw_size: r.get(2)?,
        stored_size: r.get(3)?,
        compression: Compression::parse(&compression)
            .ok_or_else(|| bad("unit compression", &compression))?,
        pack_id: r.get(5)?,
        pack_offset: r.get(6)?,
    })
}

/// Repository-wide totals, as reported by `wmk stats`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RepoStats {
    pub snapshots: u64,
    pub entries: u64,
    pub contents: u64,
    pub units: u64,
    pub packs: u64,
    /// Sum of file sizes over every readable (ok/unstable) file entry in all snapshots.
    pub logical_bytes: u64,
    /// Sum of `content.size`.
    pub unique_bytes: u64,
    /// Sum of `unit.stored_size`.
    pub stored_bytes: u64,
}

pub struct Catalog {
    pub(crate) conn: Connection,
}

impl Catalog {
    /// Open read-write, creating the schema in a fresh database.
    pub fn open(path: &Path) -> Result<Catalog> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let cat = Catalog { conn };
        match cat.user_version()? {
            schema::USER_VERSION => {}
            0 => {
                let tables: i64 = cat.conn.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",
                    [],
                    |r| r.get(0),
                )?;
                if tables != 0 {
                    return Err(Error::Corrupt(
                        "catalog has tables but user_version is 0".into(),
                    ));
                }
                cat.conn
                    .execute_batch(&format!("BEGIN IMMEDIATE;{}COMMIT;", schema::SCHEMA_SQL))?;
            }
            v => return Err(unsupported_version(v)),
        }
        Ok(cat)
    }

    /// Open an existing catalog read-only.
    pub fn open_readonly(path: &Path) -> Result<Catalog> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let cat = Catalog { conn };
        let v = cat.user_version()?;
        if v != schema::USER_VERSION {
            return Err(unsupported_version(v));
        }
        Ok(cat)
    }

    fn user_version(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))?)
    }

    fn one<T>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        f: fn(&Row<'_>) -> Result<T>,
    ) -> Result<Option<T>> {
        let mut stmt = self.conn.prepare_cached(sql)?;
        let mut rows = stmt.query(params)?;
        match rows.next()? {
            Some(r) => Ok(Some(f(r)?)),
            None => Ok(None),
        }
    }

    fn all<T>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        f: fn(&Row<'_>) -> Result<T>,
    ) -> Result<Vec<T>> {
        let mut stmt = self.conn.prepare_cached(sql)?;
        let mut rows = stmt.query(params)?;
        let mut out = Vec::new();
        while let Some(r) = rows.next()? {
            out.push(f(r)?);
        }
        Ok(out)
    }

    fn exists(&self, sql: &str, h: &Hash32) -> Result<bool> {
        let mut stmt = self.conn.prepare_cached(sql)?;
        Ok(stmt.query_row([h], |_| Ok(())).optional()?.is_some())
    }

    pub fn has_content(&self, h: &Hash32) -> Result<bool> {
        self.exists("SELECT 1 FROM content WHERE hash = ?1", h)
    }

    pub fn has_unit(&self, h: &Hash32) -> Result<bool> {
        self.exists("SELECT 1 FROM unit WHERE hash = ?1", h)
    }

    pub fn snapshot(&self, id: Id) -> Result<SnapshotRow> {
        self.one(
            &format!("SELECT {SNAPSHOT_COLS} FROM snapshot WHERE id = ?1"),
            [id],
            snapshot_from_row,
        )?
        .ok_or_else(|| Error::NotFound(format!("snapshot {id}")))
    }

    /// All snapshots, oldest first.
    pub fn snapshots(&self) -> Result<Vec<SnapshotRow>> {
        self.all(
            &format!("SELECT {SNAPSHOT_COLS} FROM snapshot ORDER BY started_at, id"),
            [],
            snapshot_from_row,
        )
    }

    /// Resolve `latest` (newest complete snapshot), a full UUID, or a hex
    /// prefix of at least 8 characters (hyphens ignored, case-insensitive).
    pub fn resolve_snapshot(&self, spec: &str) -> Result<Id> {
        if spec == "latest" {
            return self
                .one(
                    "SELECT id FROM snapshot WHERE status = 'complete' \
                     ORDER BY started_at DESC, id DESC LIMIT 1",
                    [],
                    |r| Ok(r.get::<_, Id>(0)?),
                )?
                .ok_or_else(|| Error::NotFound("no complete snapshot".into()));
        }
        let norm: String = spec
            .chars()
            .filter(|c| *c != '-')
            .collect::<String>()
            .to_lowercase();
        if norm.len() < 8 {
            return Err(Error::NotFound(format!(
                "snapshot prefix {spec:?} is too short (need at least 8 hex characters)"
            )));
        }
        if norm.len() > 32 || !norm.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::NotFound(format!("snapshot {spec:?}")));
        }
        let ids = self.all(
            "SELECT id FROM snapshot WHERE substr(lower(hex(id)), 1, ?1) = ?2 ORDER BY id",
            rusqlite::params![norm.len() as i64, norm],
            |r| Ok(r.get::<_, Id>(0)?),
        )?;
        match ids.len() {
            0 => Err(Error::NotFound(format!("snapshot {spec:?}"))),
            1 => Ok(ids[0]),
            n => Err(Error::Ambiguous(format!(
                "snapshot prefix {spec:?} matches {n} snapshots"
            ))),
        }
    }

    /// Children of `parent` (or the root entries when `None`), ordered by
    /// raw name bytes.
    pub fn children(&self, snapshot: Id, parent: Option<Id>) -> Result<Vec<EntryRow>> {
        self.all(
            &format!(
                "SELECT {ENTRY_COLS} FROM entry WHERE snapshot_id = ?1 AND parent_id IS ?2 \
                 ORDER BY name"
            ),
            rusqlite::params![snapshot, parent],
            entry_from_row,
        )
    }

    pub fn entry(&self, id: Id) -> Result<EntryRow> {
        self.one(
            &format!("SELECT {ENTRY_COLS} FROM entry WHERE id = ?1"),
            [id],
            entry_from_row,
        )?
        .ok_or_else(|| Error::NotFound(format!("entry {id}")))
    }

    pub fn entry_by_path(&self, snapshot: Id, path: &[u8]) -> Result<EntryRow> {
        self.one(
            &format!("SELECT {ENTRY_COLS} FROM entry WHERE snapshot_id = ?1 AND path = ?2"),
            rusqlite::params![snapshot, path],
            entry_from_row,
        )?
        .ok_or_else(|| {
            Error::NotFound(format!(
                "path {} in snapshot {snapshot}",
                display_bytes(path)
            ))
        })
    }

    pub fn content(&self, h: &Hash32) -> Result<ContentRow> {
        self.one(
            &format!("SELECT {CONTENT_COLS} FROM content WHERE hash = ?1"),
            [h],
            content_from_row,
        )?
        .ok_or_else(|| Error::NotFound(format!("content {h}")))
    }

    /// Units of a content, in `seq` order.
    pub fn content_units(&self, h: &Hash32) -> Result<Vec<UnitRow>> {
        self.all(
            "SELECT u.hash, u.kind, u.raw_size, u.stored_size, u.compression, u.pack_id, \
             u.pack_offset FROM content_unit cu JOIN unit u ON u.hash = cu.unit_hash \
             WHERE cu.content_hash = ?1 ORDER BY cu.seq",
            [h],
            unit_from_row,
        )
    }

    pub fn unit(&self, h: &Hash32) -> Result<UnitRow> {
        self.one(
            &format!("SELECT {UNIT_COLS} FROM unit WHERE hash = ?1"),
            [h],
            unit_from_row,
        )?
        .ok_or_else(|| Error::NotFound(format!("unit {h}")))
    }

    pub fn pack(&self, id: Id) -> Result<PackRow> {
        self.one(
            "SELECT id, size, sealed, blake3 FROM pack WHERE id = ?1",
            [id],
            |r| {
                Ok(PackRow {
                    id: r.get(0)?,
                    size: r.get(1)?,
                    sealed: r.get::<_, i64>(2)? != 0,
                    blake3: r.get(3)?,
                })
            },
        )?
        .ok_or_else(|| Error::NotFound(format!("pack {id}")))
    }

    pub fn entries_with_content(&self, h: &Hash32) -> Result<Vec<EntryRow>> {
        self.all(
            &format!("SELECT {ENTRY_COLS} FROM entry WHERE content_hash = ?1 ORDER BY id"),
            [h],
            entry_from_row,
        )
    }

    /// Repository totals; one aggregate query per field.
    pub fn stats(&self) -> Result<RepoStats> {
        let n = |sql: &str| -> Result<u64> {
            let v: i64 = self.conn.query_row(sql, [], |r| r.get(0))?;
            u64::try_from(v).map_err(|_| Error::Corrupt(format!("negative total from {sql}")))
        };
        Ok(RepoStats {
            snapshots: n("SELECT count(*) FROM snapshot")?,
            entries: n("SELECT count(*) FROM entry")?,
            contents: n("SELECT count(*) FROM content")?,
            units: n("SELECT count(*) FROM unit")?,
            packs: n("SELECT count(*) FROM pack")?,
            logical_bytes: n("SELECT coalesce(sum(size), 0) FROM entry \
                 WHERE kind = 'file' AND status IN ('ok', 'unstable')")?,
            unique_bytes: n("SELECT coalesce(sum(size), 0) FROM content")?,
            stored_bytes: n("SELECT coalesce(sum(stored_size), 0) FROM unit")?,
        })
    }

    pub fn xattrs(&self, entry: Id) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.all(
            "SELECT name, value FROM xattr WHERE entry_id = ?1 ORDER BY name",
            [entry],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
    }

    /// Stream every entry of `snapshot`, ordered by path, without loading
    /// them all.
    pub fn for_each_entry(
        &self,
        snapshot: Id,
        mut f: impl FnMut(EntryRow) -> Result<()>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {ENTRY_COLS} FROM entry WHERE snapshot_id = ?1 ORDER BY path"
        ))?;
        let mut rows = stmt.query([snapshot])?;
        while let Some(r) = rows.next()? {
            f(entry_from_row(r)?)?;
        }
        Ok(())
    }

    /// Stream each distinct content referenced by `snapshot`: its row and
    /// its unit hashes in `seq` order. Only one content's unit list is held
    /// at a time.
    pub fn for_each_snapshot_content(
        &self,
        snapshot: Id,
        mut f: impl FnMut(ContentRow, Vec<Hash32>) -> Result<()>,
    ) -> Result<()> {
        // LEFT JOIN: an empty file's content has no units but is still listed.
        // Columns 0..5 are CONTENT_COLS, so `content_from_row` applies.
        let mut stmt = self.conn.prepare(
            "SELECT c.hash, c.size, c.format, c.profile, c.unit_count, cu.unit_hash \
             FROM content c LEFT JOIN content_unit cu ON cu.content_hash = c.hash \
             WHERE c.hash IN (SELECT content_hash FROM entry \
             WHERE snapshot_id = ?1 AND content_hash IS NOT NULL) \
             ORDER BY c.hash, cu.seq",
        )?;
        let mut rows = stmt.query([snapshot])?;
        let mut cur: Option<(ContentRow, Vec<Hash32>)> = None;
        while let Some(r) = rows.next()? {
            let unit: Option<Hash32> = r.get(5)?;
            let hash: Hash32 = r.get(0)?;
            match &mut cur {
                Some((c, units)) if c.hash == hash => units.extend(unit),
                _ => {
                    let next = (content_from_row(r)?, unit.into_iter().collect());
                    if let Some((c, units)) = cur.replace(next) {
                        f(c, units)?;
                    }
                }
            }
        }
        if let Some((c, units)) = cur {
            f(c, units)?;
        }
        Ok(())
    }

    /// Stream every xattr row of `snapshot`'s entries as
    /// `(entry, name, value)`, ordered by entry id then name.
    pub fn for_each_snapshot_xattr(
        &self,
        snapshot: Id,
        mut f: impl FnMut(Id, Vec<u8>, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(
            "SELECT x.entry_id, x.name, x.value FROM xattr x \
             JOIN entry e ON e.id = x.entry_id WHERE e.snapshot_id = ?1 \
             ORDER BY x.entry_id, x.name",
        )?;
        let mut rows = stmt.query([snapshot])?;
        while let Some(r) = rows.next()? {
            f(r.get(0)?, r.get(1)?, r.get(2)?)?;
        }
        Ok(())
    }

    /// Insert or replace a snapshot row directly (autocommit), for writes made
    /// after the writer thread has finished. Same SQL as
    /// `CatalogOp::UpsertSnapshot`.
    pub fn upsert_snapshot(&self, s: &SnapshotRow) -> Result<()> {
        writer::upsert_snapshot(&self.conn, s)
    }
}

fn unsupported_version(v: i64) -> Error {
    Error::Corrupt(format!(
        "unsupported catalog user_version {v} (this build supports {})",
        schema::USER_VERSION
    ))
}
