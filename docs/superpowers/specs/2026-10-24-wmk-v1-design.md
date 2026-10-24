# wmk v1 — Design Spec

- **Project:** drmanhattan (CLI binary: `wmk`, "watchmaker")
- **Date:** 2026-10-24
- **Status:** Draft, awaiting review

## 1. Purpose

`wmk` inspects a directory, records its exact structure, assigns a UUID alias to every entry, and decomposes file contents into deduplicated, content-addressed storage units. Any snapshot can be rebuilt byte-for-byte from a repository on demand.

The primary use case is **ML datasets built from scraped websites**, including existing hand-labelled datasets (folder-as-class) that represent hundreds of hours of annotation and must survive storage and rebuild exactly.

Key measures, in priority order where they conflict:

1. **Bulletproof reproducibility**: a restore is provably identical to what was ingested.
2. **Speed**: ingest is bound by I/O, not by CPU or catalog work.
3. **Storage efficiency**: identical content is stored once.

## 2. Scope

### In scope (v1)

- Inventory: walk a directory and record tree, names, metadata, UUIDs.
- Decompose: content-defined chunking, dedup, compression, and pack storage.
- Rebuild: exact restore (full or by path prefix) with verification.
- Integrity tooling: `manifest`, `verify`, `diff`, `recover`.
- Lightweight media metadata: image dimensions at ingest; perceptual hash via `analyze`.
- Folder-as-class labels via path patterns.
- Hash-list screening at ingest, provenance capture, evidence reports, and `purge`.
- A published SQLite schema as the contract for Python consumers.

### Out of scope (later specs)

- Dataset recombination and export beyond `restore --path`.
- Sidecar labels (CSV/JSON/COCO), and Label Studio/CVAT imports.
- Format-aware storage units (pixel data split from metadata, HTML boilerplate split from content, WARC ingestion). The data model reserves room for these (see `unit.kind`).
- Perceptual dedup and variant collapsing (keep the highest-resolution original), and crop-robust clustering such as outpainting to a nominal shape before embedding.
- Snapshot deletion (`forget`) and general garbage collection. These will reuse `purge`'s pack-rewrite machinery.
- Encryption at rest. Use filesystem encryption (FileVault, LUKS) in v1.
- PyO3 bindings.
- Distributed or object-storage backends.

### Scale targets

- **Comfortable:** up to about 1M files and 1 TB per snapshot.
- **Designed toward:** 1M–100M files and multiple TB per repository, without changing the format.

### Platforms

macOS (APFS, including case-insensitive volumes) and Linux (ext4, xfs, btrfs).

## 3. Terminology

| Term | Meaning |
|---|---|
| **repo** | A wmk repository directory: config, catalog, and packs |
| **snapshot** | One ingest of one source directory; immutable once complete |
| **entry** | One file, directory, or symlink within a snapshot. Its UUIDv7 is the **file alias** |
| **content** | Unique file bytes, identified by BLAKE3-256 of the whole file |
| **unit** | One stored blob, identified by BLAKE3-256 of its raw (uncompressed) bytes. v1 has three kinds: `raw_chunk`, `snapshot_record`, `purge_record` |
| **pack** | An append-only container file of units, sealed when full |
| **manifest hash** | A deterministic hash of a tree's restorable state (see §6). It excludes UUIDs and derived data |

## 4. Fidelity contract

| Aspect | Recorded | Restored by default | In `manifest_hash` |
|---|---|---|---|
| File bytes | yes | yes | yes, via content hash |
| Directory tree, including empty dirs | yes | yes | yes |
| Names as raw bytes (never normalized) | yes | yes | yes |
| Permission bits `mode & 0o1777` | yes | yes | yes |
| setuid/setgid bits | yes | no | no |
| mtime, files and dirs (ns) | yes | yes | yes |
| Symlinks (target bytes; never followed) | yes | yes | target only |
| Symlink mtime/mode | yes | best effort (`lutimes`) | no |
| Hard links (`hardlink_group`) | yes | with `--hardlinks` | `ext_manifest_hash` only |
| Extended attributes | yes | with `--xattrs` | `ext_manifest_hash` only |
| uid/gid, atime, ctime, ACLs | no | — | — |
| Sockets, FIFOs, device files | listed in the ingest report, skipped | — | excluded, on both sides |

The same rules apply whether a manifest is computed from a live directory (`wmk manifest`) or from a snapshot. That is what makes the hashes comparable.

## 5. Repository layout

```
repo/
  wmk.toml                    # format version, chunk profiles, compression, pack size, screening
  wmk.lock                    # advisory flock; one writer process at a time
  catalog.sqlite              # WAL mode; derived index (rebuildable from packs)
  packs/<hh>/<pack-id>.pack   # hh = last 2 hex chars of the pack UUIDv7 (random bits)
```

The chunk profiles, compression settings and pack size are fixed at `wmk init` and cannot be changed afterwards, because changing them would break dedup stability. To use different settings, create a new repo.

```toml
format_version = 1

[chunking.text]      # used for sniffed text formats ≤ 4 MiB
min = 2048
avg = 8192
max = 65536

[chunking.binary]    # used for everything else
min = 262144
avg = 1048576
max = 4194304

[compression]
algo = "zstd"
level = 3

[packs]
max_size = 268435456 # 256 MiB
```

**Why two chunk profiles:** at a 1 MiB average, typical HTML pages (50–200 KiB) would never be split, so shared site templates would not dedup. Small chunks for text-class files catch shared boilerplate. Large chunks for binaries keep the unit count manageable. Estimate for a 1 TB scrape that is 10% text: about 14M units, which is fine for SQLite. Which profile a file uses is decided deterministically from its sniffed format and size, and the decision is recorded on its `content` row.

Files smaller than their profile's `min` become a single chunk.

## 6. Data model

### 6.1 Catalog schema (SQLite, `PRAGMA user_version = 1`)

Hashes are stored as 32-byte BLOBs and UUIDs as 16-byte BLOBs. Times are integer nanoseconds since the Unix epoch, and may be negative.

```sql
snapshot(
  id BLOB PRIMARY KEY,            -- UUIDv7
  status TEXT NOT NULL,           -- in_progress | complete | failed
  source_root BLOB NOT NULL,      -- absolute path bytes at ingest
  host TEXT NOT NULL,
  tool_version TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  completed_at INTEGER,
  note TEXT,
  manifest_hash BLOB,             -- core fidelity, tree as stored
  ext_manifest_hash BLOB,         -- core + hardlinks + xattrs
  source_manifest_hash BLOB,      -- tree as seen, including screened-out entries
  record_unit BLOB,               -- unit hash of the snapshot_record
  entry_count INTEGER, file_bytes INTEGER,
  screened_out INTEGER NOT NULL DEFAULT 0,
  label_pattern TEXT
)

entry(
  id BLOB PRIMARY KEY,            -- UUIDv7: the file alias
  snapshot_id BLOB NOT NULL REFERENCES snapshot(id),
  parent_id BLOB REFERENCES entry(id),  -- NULL for the root entry
  name BLOB NOT NULL,             -- raw bytes; '' for root
  path BLOB NOT NULL,             -- raw relative path bytes, '/'-joined; '' for root
  kind TEXT NOT NULL,             -- file | dir | symlink
  mode INTEGER NOT NULL,          -- full st_mode & 0o7777
  mtime_ns INTEGER NOT NULL,
  size INTEGER,                   -- files only
  content_hash BLOB REFERENCES content(hash),  -- files only; NULL when not stored
  link_target BLOB,               -- symlinks only
  hardlink_group INTEGER,         -- per-snapshot; shared by entries with the same (dev, ino)
  status TEXT NOT NULL,           -- ok | unstable | screened_out | screen_error | purged
  UNIQUE(snapshot_id, path)
)

content(
  hash BLOB PRIMARY KEY,          -- BLAKE3-256 of the whole file
  size INTEGER NOT NULL,
  format TEXT,                    -- sniffed: jpeg, png, webp, gif, html, css, js, json, ...
  profile TEXT NOT NULL,          -- text | binary
  unit_count INTEGER NOT NULL
)

content_unit(
  content_hash BLOB NOT NULL REFERENCES content(hash),
  seq INTEGER NOT NULL,
  unit_hash BLOB NOT NULL REFERENCES unit(hash),
  PRIMARY KEY(content_hash, seq)
) WITHOUT ROWID

unit(
  hash BLOB PRIMARY KEY,          -- BLAKE3-256 of raw bytes
  kind TEXT NOT NULL,             -- raw_chunk | snapshot_record | purge_record (future: format-aware kinds)
  raw_size INTEGER NOT NULL,
  stored_size INTEGER NOT NULL,
  compression TEXT NOT NULL,      -- none | zstd
  pack_id BLOB NOT NULL REFERENCES pack(id),
  pack_offset INTEGER NOT NULL
) WITHOUT ROWID

pack(
  id BLOB PRIMARY KEY,            -- UUIDv7
  size INTEGER NOT NULL,
  sealed INTEGER NOT NULL,        -- 0 | 1
  blake3 BLOB                     -- whole-pack hash, set on seal
)

xattr(entry_id BLOB NOT NULL REFERENCES entry(id), name BLOB NOT NULL, value BLOB NOT NULL,
      PRIMARY KEY(entry_id, name))

media(
  content_hash BLOB PRIMARY KEY REFERENCES content(hash),
  width INTEGER, height INTEGER,  -- from header, at ingest
  phash INTEGER,                  -- 64-bit, set by `analyze`
  phash_algo TEXT,                -- e.g. "image_hasher:dct-mean:8x8"
  analyzed_at INTEGER
)

label(entry_id BLOB NOT NULL REFERENCES entry(id), scheme TEXT NOT NULL, value TEXT NOT NULL,
      PRIMARY KEY(entry_id, scheme))

provenance(
  entry_id BLOB PRIMARY KEY REFERENCES entry(id),
  source TEXT NOT NULL,           -- wget_layout | jsonl
  url TEXT, fetched_at INTEGER, last_modified INTEGER,
  server_ip TEXT, http_headers TEXT  -- JSON object
)

screen_hit(
  id INTEGER PRIMARY KEY,
  snapshot_id BLOB NOT NULL REFERENCES snapshot(id),
  entry_id BLOB NOT NULL REFERENCES entry(id),
  verdict TEXT NOT NULL,          -- match | error
  screener TEXT NOT NULL,         -- configured name
  list_version TEXT,              -- e.g. list file BLAKE3 + mtime
  blake3 BLOB NOT NULL, md5 BLOB, sha1 BLOB, sha256 BLOB,
  detail TEXT,
  detected_at INTEGER NOT NULL
)

purge_log(
  id INTEGER PRIMARY KEY,
  content_hash BLOB NOT NULL,
  reason TEXT NOT NULL,
  units_removed INTEGER NOT NULL,
  packs_rewritten INTEGER NOT NULL,
  affected_snapshots TEXT NOT NULL,  -- JSON: [{id, old_manifest_hash, new_manifest_hash}]
  purged_at INTEGER NOT NULL
)
```

Indexes: `entry(snapshot_id, parent_id)`, `entry(content_hash)`, `content_unit(unit_hash)`, `unit(pack_id, pack_offset)`, `label(scheme, value)`, `media(phash)`.

Identity is kept separate from content. The same image at 40 paths has 40 entries (40 UUIDs), 1 `content` row, and its units stored once.

### 6.2 Pack format

All integers are little-endian.

```
header : "WMKPACK\0" (8) | format u16 | pack_id (16)
record : marker u32 = 0x574D4B52 ("WMKR") | kind u8 | compression u8
         | unit_hash (32) | raw_len u64 | stored_len u64
         | header_crc32c u32 (over the preceding header fields) | payload (stored_len)
footer : (only on seal) record count u64 | [unit_hash, offset u64]* | pack_blake3 (32)
         | "WMKPEND\0" (8)
```

- Every record is self-describing, so the catalog can be rebuilt by scanning packs (`recover`).
- `header_crc32c` lets the scanner detect a torn tail on an unsealed pack and truncate it.
- `pack_blake3` covers every byte before the footer.
- Unit integrity is checked by recomputing BLAKE3 over the decompressed payload and comparing it with `unit_hash`.

### 6.3 Snapshot record unit

When an ingest finishes, wmk writes one `snapshot_record` unit. It is a zstd-compressed CBOR document containing:

- the snapshot row
- every entry, with its UUID
- the `content → [unit_hash]` lists for contents this snapshot references
- xattrs, provenance, and screen hits

This record makes the packs the source of truth. Labels and media are derived data, so they are recomputed rather than recovered.

### 6.4 Canonical manifest encoding

`manifest_hash = BLAKE3(encoding)`. The encoding is the sequence of entries sorted bytewise by `path`, where each entry is:

```
path_len u32 | path bytes | kind u8 (0 dir, 1 file, 2 symlink)
dir:     mode u32 (& 0o1777) | mtime_ns i64
file:    mode u32 (& 0o1777) | mtime_ns i64 | size u64 | content_blake3 (32)
symlink: target_len u32 | target bytes
```

The root entry has `path = ""` and is included.

`ext_manifest_hash` uses the same encoding, followed after each entry by:

- the canonical hardlink group: the index of the group's lexicographically first path, or `u64::MAX`
- the sorted xattrs: `name_len | name | value_len | value`

UUIDs, labels, media, provenance, and snapshot metadata are never hashed.

## 7. Ingest (`wmk ingest <dir>`)

### Pipeline

```
walker ──► reader/chunker pool (rayon) ──► pack writer (1 thread) ──► catalog writer (1 thread)
```

1. **Lock.** Take `wmk.lock`, create the snapshot row with `status=in_progress`, and assign UUIDv7s as entries are discovered.
2. **Walk.**
   - Use `lstat` only. Never follow symlinks. Stay on one filesystem (`--cross-fs` to override).
   - Names are captured as raw `OsStr` bytes.
   - Entries with `st_nlink > 1` are grouped by `(dev, ino)` into `hardlink_group`.
   - Special files are skipped and listed in the report.
3. **Read, hash and chunk.**
   - Read each file once. In the same pass: compute whole-file BLAKE3, find FastCDC (2020) boundaries using the file's profile, and compute a BLAKE3 hash per chunk. MD5, SHA-1 and SHA-256 are computed in the same pass only if a configured hash list needs them.
   - Sniff the format using magic bytes (`infer`), plus file extensions for text classes (html, htm, css, js, mjs, json, xml, svg, txt, csv, md).
   - Read image dimensions from the header only (`imagesize`), without decoding.
   - If the content hash is already in the catalog or in flight, skip chunk emission. The entry still references the content.
4. **Stability check.** Run `stat` before and after reading. If size or mtime changed, retry once. If it changed again, fail the ingest (exit 1). With `--allow-unstable`, the bytes read are stored instead, and the entry gets `status=unstable`.
5. **Screen** each new content (§8) before emitting any of its units.
6. **Compress.** Use `none` for already-compressed formats (jpeg, png, webp, gif, avif, heic, mp4, webm, zip, gz, bz2, xz, zst, 7z). Otherwise use zstd level 3. If the result is ≥ 97% of the raw size, store it uncompressed. The decision is deterministic per unit.
7. **Pack writer.**
   - This is the only authority on whether a unit is new. It checks the in-memory set of in-flight hashes and the catalog, so concurrent duplicates are written once.
   - New units are appended to the open pack. At `max_size` the pack is fsynced, its footer written, and it is sealed.
8. **Catalog writer.** Commits batched transactions of about 10k rows or 1 s, whichever comes first.
9. **Finish.**
   - Fsync all packs and write the `snapshot_record` unit.
   - Compute `manifest_hash`, `ext_manifest_hash` and `source_manifest_hash`.
   - Set `status=complete`.

**Ordering invariant:** pack bytes are fsynced before any catalog row that references them is committed.

**Memory:** queues between stages are bounded, so the number of files in flight stays in the thousands.

### Options

| Option | Effect |
|---|---|
| `--labels <pattern>` | Derive labels from paths (§10) |
| `--provenance wget\|<file.jsonl>` | Capture provenance (§8.4) |
| `--note <text>` | Free-text note on the snapshot |
| `--allow-unstable` | Store files that changed during the read, flagged `unstable` |
| `--screen-fail-open` | Store files the screener couldn't check, instead of withholding them |
| `--cross-fs` | Allow the walk to cross filesystem boundaries |

### Report

Printed, and emitted as JSON with `--json`:

- counts, bytes read, new bytes stored, dedup ratio, throughput
- skipped special files
- unstable files
- screening results

## 8. Screening, provenance and evidence

### 8.1 Placement

Each new content is screened once, in the reader pool, after hashing and before any of its units reach the pack writer.

- Files up to 64 MiB are held in memory until a verdict is returned.
- Larger files are read twice: first to hash and screen, then to chunk.

### 8.2 Screeners

```toml
[[screen.hashlist]]
name = "ncmec-2026-10"
algo = "md5"              # md5 | sha1 | sha256 | blake3
path = "/secure/lists/ncmec.md5"   # one lowercase hex hash per line; '#' comments

[[screen.external]]
name = "photodna"
command = ["python", "-m", "my_screener"]
timeout_ms = 5000
```

- **Hash lists** are loaded into in-memory hash sets at ingest start. `list_version` is the BLAKE3 of the list file plus its mtime.
- **External screeners** run as long-lived child processes speaking JSON Lines over stdio.
  - Request: `{"id", "path", "content_blake3", "size", "format"}`
  - Response: `{"id", "verdict": "pass"|"match"|"error", "detail"}`
  - A timeout or crash counts as `error`. A crashed process is restarted once.

### 8.3 Outcomes

- **Match.**
  - No units are stored.
  - The entry is recorded with `status=screened_out` and `content_hash=NULL`, and a `screen_hit` row is written with every hash that was computed.
  - The source is never modified.
  - wmk never copies matched bytes anywhere.
- **Error.** The screener fails closed. Units are not stored, the entry gets `status=screen_error`, and a `screen_hit` row is written with `verdict=error`. `--screen-fail-open` stores the content instead, and the hit is still recorded.
- **After the ingest:**
  - `manifest_hash` covers the tree as stored.
  - `source_manifest_hash` covers the tree as seen, including withheld entries.
  - The exit code is 3 if any entry was withheld.
- **On restore and diff:** `restore` skips withheld entries and reports their count. `wmk diff <snapshot> <source>` reports them as *expected missing*, not as differences.

### 8.4 Provenance

Optional. Records where each entry came from.

- **`--provenance wget`**
  - Derives `url = "https://" + <first path component as host> + "/" + <rest of path>`; scheme overridable with `--provenance-scheme`.
  - Sets `last_modified` from mtime (wget sets mtime from the `Last-Modified` header).
  - Leaves `fetched_at` unknown.
- **`--provenance <file.jsonl>`**
  - Each line: `{"path", "url", "fetched_at", "server_ip", "http_headers"}`, where `path` is relative to the source root.
  - Lines whose path doesn't exist are reported. Entries without a line simply get no provenance.

### 8.5 Rescreening, evidence and purge

- **`wmk screen [<snapshot>]`** rescreens stored contents against the currently configured lists and records new `screen_hit` rows. Stored bytes are hashed while streaming from packs; they are not restored to disk.
- **`wmk screen report <snapshot> [--json]`** writes one evidence record per hit:
  - entry path, UUID, url, fetched_at, server_ip, http_headers
  - md5, sha1, sha256 and blake3
  - screener name, list name, list version, verdict, detected_at
  - snapshot id, host, tool version
  - the fixed statement "flagged by automated hash match; not viewed"

  It never includes file bytes.
- **`wmk purge <content-hash> --reason <text>`**
  1. Find the units referenced **only** by this content. Shared units are kept.
  2. For each affected pack, write a new pack without those units, verify it fully, update the catalog in one transaction, and then delete the old pack.
  3. Mark affected entries `status=purged` and set `content_hash=NULL`.
  4. Recompute `manifest_hash` for every affected snapshot, and write a new `snapshot_record` that replaces the old one. The old record unit is dropped in the same pack rewrite.
  5. Write a `purge_log` row recording old and new manifest hashes. Also write a `purge_record` unit so the purge survives `recover`.

  The operation can be killed at any point and the repo stays consistent: the old pack is removed only after the new one is verified and committed.

Legal and process note (not a software requirement): the reporting protocol, including whether any evidence beyond metadata must be preserved, should be signed off by counsel (US federal and California law) before large-scale scraping. v1 defaults to never retaining matched bytes.

## 9. Restore and verification

### `wmk restore <snapshot> <dest> [--path <prefix>] [--hardlinks] [--xattrs]`

1. **Pre-flight checks.** Nothing is written until these pass.
   - `dest` must not exist.
   - Probe the destination filesystem for case sensitivity and name-byte preservation by creating and checking test files in the parent directory.
   - If any two entries collide on a case-insensitive destination, abort and list the collisions.
   - If any name would not round-trip byte-exact (non-UTF-8 names on APFS, or normalization on HFS+), abort and list the names.
   - Probe mtime precision and record any expected precision loss.
2. **Write.**
   - Write everything into `<dest>.wmk-partial-<snapshot-id>` and rename it to `dest` at the end.
   - Create directories with temporary mode `0o700`.
   - Write files in parallel, sorted by `(pack_id, pack_offset)` of each file's first unit so pack reads are mostly sequential.
   - An LRU cache of decompressed units serves heavily shared chunks.
   - Check every unit's BLAKE3 on read, and check every file's whole BLAKE3 before setting its metadata. Any mismatch aborts the restore and leaves the partial directory for inspection.
3. **Finalize, in this order:**
   1. symlinks
   2. hard links (`--hardlinks`; otherwise independent copies)
   3. xattrs (`--xattrs`)
   4. file modes and mtimes
   5. directory modes and mtimes, deepest first
   6. rename into place
4. **Report:** counts, any precision loss, and withheld or purged entries.

### Verification commands

| Command | Behavior |
|---|---|
| `wmk manifest <dir> [--ext] [--mtime-precision <ns>]` | Computes the manifest hash of a live directory using §4 and §6.4 rules, without ingesting |
| `wmk verify [<snapshot>] [--quick]` | `--quick`: catalog/pack consistency (every referenced unit exists in a pack at the recorded offset with a valid header). Default: also decompress and BLAKE3 every referenced unit and each sealed pack |
| `wmk diff <snapshot> <dir> [--path <prefix>] [--mtime-precision <ns>]` | Path-level differences: content, mode, mtime, kind, link target, missing, extra, expected-missing |
| `wmk recover` | Rebuilds `catalog.sqlite` from packs. Truncates torn tails of unsealed packs, re-indexes all units, replays snapshot and purge records, and recomputes labels |

**Core guarantee:** `manifest(D) == snapshot.manifest_hash == manifest(restore(snapshot))`. This holds for any directory D with no screened-out entries, on any destination that passes the pre-flight checks.

## 10. Labels

Pattern grammar (applies to file entries only):

- `{name}` captures exactly one path component.
- `*` matches one component without capturing it.
- `**` matches zero or more components, and is only allowed at the end.

```
wmk ingest ./scrape --labels '{split}/{class}/**'
# train/explicit/0001.jpg → (split, "train"), (class, "explicit")
```

- Files that don't match the pattern get no labels.
- A captured component that isn't valid UTF-8 is reported and not labelled.
- `wmk label <snapshot> --pattern <p>` replaces that snapshot's labels. Labels are derived data and are never part of manifest hashes.

## 11. CLI summary

The repo is selected with `--repo <path>` or `WMK_REPO`. All commands support `--json`, and progress is shown on stderr.

| Command | Purpose |
|---|---|
| `init <repo>` | Create a repo with fixed config |
| `ingest <dir>` | Snapshot a directory (§7) |
| `analyze [<snapshot>]` | Compute perceptual hashes for un-analyzed image contents. Streams from packs; can be rerun safely |
| `ls` | List snapshots |
| `show <snapshot> [<path>]` | Browse a snapshot's tree |
| `stats` | Repo totals: raw vs stored bytes, dedup ratio, units per pack |
| `resolve <uuid>` | UUID → snapshot, path, content |
| `find --content <hash>` | Every entry with that content |
| `cat <uuid\|content-hash>` | Stream file bytes to stdout |
| `label <snapshot> --pattern <p>` | Replace a snapshot's labels |
| `restore`, `manifest`, `verify`, `diff`, `recover` | §9 |
| `screen`, `screen report`, `purge` | §8.5 |

**Exit codes:**

| Code | Meaning |
|---|---|
| 0 | OK |
| 1 | Error |
| 2 | Verification failure, or differences found |
| 3 | Completed, but some entries were withheld by screening |

**Concurrency:** one writer process holds `wmk.lock`. Readers can open the catalog at any time.

## 12. Python contract

- `docs/SCHEMA.md` documents every table and column in §6.1, its semantics, and example queries:
  - "all files labelled class=X"
  - "every entry for a content"
  - "images ≥ 1024 px wide"
  - "near-duplicates by phash Hamming distance"
- The schema version is kept in `PRAGMA user_version` and in `wmk.toml` `format_version`.
- Python opens `catalog.sqlite` read-only (`mode=ro` URI). All writes go through `wmk`.
- Bytes are obtained via `wmk cat` or `wmk restore --path`, so Python never parses packs.

## 13. Implementation structure

```
Cargo.toml                  # workspace
crates/core/                # drmanhattan-core (lib)
  config, model, walk, chunk, sniff, pack, catalog, manifest,
  ingest, restore, verify, screen, provenance, labels, purge, recover, analyze
crates/cli/                 # drmanhattan (bin: wmk) — clap, output formatting, exit codes
python/tests/               # pytest schema-contract tests
docs/SCHEMA.md
```

Each core module has one job and a narrow interface. For example, `pack` knows nothing about SQLite, and `manifest` is a pure function from a sorted entry iterator to a hash, so the same code serves both live directories and snapshots.

**Main dependencies:** `fastcdc`, `blake3`, `zstd`, `rusqlite` (bundled), `rayon`, `crossbeam-channel`, `uuid` (v7), `clap`, `serde`, `ciborium`, `toml`, `indicatif`, `infer`, `imagesize`, `image` + `image_hasher` (analyze), `md-5`, `sha1`, `sha2`, `crc32c`, `xattr`, `fs4`.

**Test-only dependencies:** `proptest`, `fail`, `tempfile`, `criterion`.

## 14. Testing

1. **Round-trip property tests (proptest).** Generate trees containing:
   - raw-byte names: non-UTF-8 on Linux, NFC and NFD on both platforms
   - empty files and directories
   - sizes at `min - 1`, `min`, `min + 1`, `max`, and `max + 1` for both profiles
   - symlinks, including dangling, absolute, and pointing outside the tree
   - hard links
   - unusual permission bits
   - mtimes before 1970, at 0, and with nanosecond values

   Assert:
   - `manifest(D) == manifest_hash == manifest(restore)`
   - `diff` is empty
   - re-ingesting the same tree yields the same `manifest_hash` and zero new units
2. **Format stability.**
   - A committed fixture repo, with expected manifest hashes, must restore byte-exact on every future version.
   - The chunker gets known-input → known-boundary vectors for both profiles.
   - Golden canonical-manifest encodings.
3. **Fault injection (`fail` failpoints).**
   - Kill the ingest at every stage boundary. `verify --quick` must pass, a fresh ingest must succeed, and no catalog row may reference missing data.
   - Flip one byte in a pack: `verify` fails and `restore` refuses.
   - Delete the catalog, run `recover`, then restore: the result is byte-identical.
   - Kill `purge` at each step: the repo stays consistent.
4. **Platform (CI on macOS and Linux).**
   - Case-collision pre-flight on case-insensitive APFS.
   - Rejection of non-UTF-8 names on macOS.
   - mtime precision probing.
   - xattr round trip with `--xattrs`.
   - Hard links with `--hardlinks`.
5. **Screening.** Uses only synthetic, harmless files and test lists built from their hashes:
   - matches for each of the four algorithms
   - a mock external screener covering pass, match, error, timeout, and crash-restart
   - fail-closed by default, and `--screen-fail-open`
   - evidence report contents
   - rescreening after a list update
   - `purge`, then `verify`, then `recover`
6. **Python contract.** Pytest runs every query in `docs/SCHEMA.md` against a freshly ingested fixture repo.
7. **Benchmarks.**
   - `criterion` microbenchmarks: chunking, hashing, compression, pack append.
   - End-to-end benchmark on a generated corpus: about 100k files mixing HTML (sharing templates), CSS/JS, and JPEG/PNG, with planted exact duplicates.
   - Track files/s, MB/s, dedup ratio, and stored/raw ratio.
   - Optional `--corpus <dir>` to run against a real scrape.

## 15. Error handling principles

- Never modify the source directory.
- Never overwrite an existing restore destination.
- Fail closed, both on screening and on any integrity mismatch.
- Every withheld, skipped, or unstable entry is listed in the command's report. Nothing is dropped silently.
- A crash or kill at any point leaves the repo consistent (§7 ordering invariant, §8.5 purge ordering). The worst case is unreferenced bytes in a pack.

## 16. Future work (informs v1 constraints)

- **Dataset recombination:** select by labels, media, or provenance across snapshots and materialize new trees. This builds on `restore`'s writer and the catalog schema.
- **Format-aware units:** new `unit.kind` values with per-format decompose/recompose code, each gated by round-trip property tests. WARC ingestion is a strong candidate, because it also yields complete provenance.
- **Perceptual dedup and variant collapsing:** keep the highest-resolution original. This uses `media.phash` and dimensions.
- **Crop-robust clustering:** a pluggable normalize-before-embed stage (for example, outpainting to a nominal shape), plus an evaluation harness that scores how consistent the subject is within each cluster, against a baseline.
- **Sidecar label import**, and **zstd dictionary compression** for small similar text files.
- **`forget` and GC**, reusing purge's pack-rewrite path.
- **PyO3 bindings.**
