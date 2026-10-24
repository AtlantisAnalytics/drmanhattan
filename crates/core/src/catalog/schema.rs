//! Catalog schema (spec §6.1). Table names, column names and value strings
//! are a published contract for external readers.

pub const USER_VERSION: i64 = 1;

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
CREATE INDEX IF NOT EXISTS entry_content_hash ON entry(content_hash);
CREATE INDEX IF NOT EXISTS content_unit_unit_hash ON content_unit(unit_hash);
CREATE INDEX IF NOT EXISTS unit_pack_offset ON unit(pack_id, pack_offset);
CREATE INDEX IF NOT EXISTS label_scheme_value ON label(scheme, value);
CREATE INDEX IF NOT EXISTS media_phash ON media(phash);

PRAGMA user_version = 1;
";
