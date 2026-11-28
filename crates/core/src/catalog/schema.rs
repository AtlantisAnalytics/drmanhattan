//! Catalog schema (spec §6.1). Table names, column names and value strings
//! are a published contract for external readers.

/// The version new catalogs are created at.
pub const USER_VERSION: i64 = 2;
/// Versions this build opens; an existing catalog keeps its version.
pub const SUPPORTED_VERSIONS: [i64; 2] = [1, 2];

pub const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS snapshot(
  id BLOB PRIMARY KEY,
  status TEXT NOT NULL,
  source_root BLOB NOT NULL,
  host TEXT NOT NULL,
  tool_version TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  completed_at INTEGER,
  note TEXT,
  manifest_hash BLOB,
  ext_manifest_hash BLOB,
  source_manifest_hash BLOB,
  record_unit BLOB,
  entry_count INTEGER, file_bytes INTEGER,
  screened_out INTEGER NOT NULL DEFAULT 0,
  label_pattern TEXT
);

CREATE TABLE IF NOT EXISTS content(
  hash BLOB PRIMARY KEY,
  size INTEGER NOT NULL,
  format TEXT,
  profile TEXT NOT NULL,
  unit_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pack(
  id BLOB PRIMARY KEY,
  size INTEGER NOT NULL,
  sealed INTEGER NOT NULL,
  blake3 BLOB
);

CREATE TABLE IF NOT EXISTS unit(
  hash BLOB PRIMARY KEY,
  kind TEXT NOT NULL,
  raw_size INTEGER NOT NULL,
  stored_size INTEGER NOT NULL,
  compression TEXT NOT NULL,
  pack_id BLOB NOT NULL REFERENCES pack(id),
  pack_offset INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS content_unit(
  content_hash BLOB NOT NULL REFERENCES content(hash),
  seq INTEGER NOT NULL,
  unit_hash BLOB NOT NULL REFERENCES unit(hash),
  PRIMARY KEY(content_hash, seq)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS entry(
  id BLOB PRIMARY KEY,
  snapshot_id BLOB NOT NULL REFERENCES snapshot(id),
  parent_id BLOB REFERENCES entry(id),
  name BLOB NOT NULL,
  path BLOB NOT NULL,
  kind TEXT NOT NULL,
  mode INTEGER NOT NULL,
  mtime_ns INTEGER NOT NULL,
  size INTEGER,
  content_hash BLOB REFERENCES content(hash),
  link_target BLOB,
  hardlink_group INTEGER,
  status TEXT NOT NULL,
  UNIQUE(snapshot_id, path)
);

CREATE TABLE IF NOT EXISTS xattr(
  entry_id BLOB NOT NULL REFERENCES entry(id),
  name BLOB NOT NULL,
  value BLOB NOT NULL,
  PRIMARY KEY(entry_id, name)
);

CREATE TABLE IF NOT EXISTS media(
  content_hash BLOB PRIMARY KEY REFERENCES content(hash),
  width INTEGER, height INTEGER,
  phash INTEGER,
  phash_algo TEXT,
  analyzed_at INTEGER
);

CREATE TABLE IF NOT EXISTS label(
  entry_id BLOB NOT NULL REFERENCES entry(id),
  scheme TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY(entry_id, scheme)
);

CREATE TABLE IF NOT EXISTS provenance(
  entry_id BLOB PRIMARY KEY REFERENCES entry(id),
  source TEXT NOT NULL,
  url TEXT, fetched_at INTEGER, last_modified INTEGER,
  server_ip TEXT, http_headers TEXT
);

CREATE TABLE IF NOT EXISTS screen_hit(
  id INTEGER PRIMARY KEY,
  snapshot_id BLOB NOT NULL REFERENCES snapshot(id),
  entry_id BLOB NOT NULL REFERENCES entry(id),
  verdict TEXT NOT NULL,
  screener TEXT NOT NULL,
  list_version TEXT,
  blake3 BLOB NOT NULL, md5 BLOB, sha1 BLOB, sha256 BLOB,
  detail TEXT,
  detected_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS purge_log(
  id INTEGER PRIMARY KEY,
  content_hash BLOB NOT NULL,
  reason TEXT NOT NULL,
  units_removed INTEGER NOT NULL,
  packs_rewritten INTEGER NOT NULL,
  affected_snapshots TEXT NOT NULL,
  purged_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS entry_snapshot_parent ON entry(snapshot_id, parent_id);
CREATE INDEX IF NOT EXISTS entry_parent ON entry(parent_id);
CREATE INDEX IF NOT EXISTS entry_content_hash ON entry(content_hash);
CREATE INDEX IF NOT EXISTS screen_hit_snapshot ON screen_hit(snapshot_id);
CREATE INDEX IF NOT EXISTS screen_hit_entry ON screen_hit(entry_id);
CREATE INDEX IF NOT EXISTS content_unit_unit_hash ON content_unit(unit_hash);
CREATE INDEX IF NOT EXISTS unit_pack_offset ON unit(pack_id, pack_offset);
CREATE INDEX IF NOT EXISTS label_scheme_value ON label(scheme, value);
CREATE INDEX IF NOT EXISTS media_phash ON media(phash);

PRAGMA user_version = 2;
";

#[cfg(test)]
mod tests {
    use super::SCHEMA_SQL;
    use rusqlite::{Connection, StatementStatus};

    fn fresh() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        conn
    }

    /// SQLite looks child rows up by their foreign-key column whenever a
    /// parent row is deleted, or inserted while deferred violations are
    /// outstanding. Without an index led by that column, each lookup scans
    /// the whole child table.
    #[test]
    fn every_foreign_key_column_leads_an_index() {
        let conn = fresh();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let mut missing = Vec::new();
        for t in &tables {
            let fks: Vec<String> = conn
                .prepare("SELECT \"from\" FROM pragma_foreign_key_list(?1)")
                .unwrap()
                .query_map([t], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            let leading: Vec<String> = conn
                .prepare(
                    "SELECT ii.name FROM pragma_index_list(?1) il, pragma_index_info(il.name) ii \
                     WHERE ii.seqno = 0",
                )
                .unwrap()
                .query_map([t], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            for col in fks {
                if !leading.contains(&col) {
                    missing.push(format!("{t}.{col}"));
                }
            }
        }
        assert!(missing.is_empty(), "no index leads with {missing:?}");
    }

    /// Recover replays a snapshot record in one transaction with deferred
    /// foreign keys, and entries may precede the rows they reference. Each
    /// entry insert must then stay an index lookup, not a scan of `entry`.
    #[test]
    fn deferred_entry_inserts_do_not_scan_the_entry_table() {
        let conn = fresh();
        conn.execute_batch("BEGIN; PRAGMA defer_foreign_keys = ON;")
            .unwrap();
        conn.execute(
            "INSERT INTO snapshot(id, status, source_root, host, tool_version, started_at) \
             VALUES (x'01', 'complete', x'2f', 'h', '0', 1)",
            [],
        )
        .unwrap();
        let mut ins = conn
            .prepare(
                "INSERT INTO entry(id, snapshot_id, parent_id, name, path, kind, mode, \
                 mtime_ns, status) VALUES (?1, x'01', ?2, ?3, ?3, 'dir', 493, 0, 'ok')",
            )
            .unwrap();
        // A child whose parent is not in yet: a deferred violation stays open.
        ins.execute(rusqlite::params![
            vec![0xffu8; 16],
            vec![0xfeu8; 16],
            b"orphan".to_vec()
        ])
        .unwrap();
        for i in 0u32..500 {
            let id = i.to_be_bytes().repeat(4);
            ins.execute(rusqlite::params![
                id,
                None::<Vec<u8>>,
                format!("d{i}").into_bytes()
            ])
            .unwrap();
        }
        assert_eq!(ins.get_status(StatementStatus::FullscanStep), 0);
        drop(ins);
        conn.execute_batch("ROLLBACK").unwrap();
    }
}
