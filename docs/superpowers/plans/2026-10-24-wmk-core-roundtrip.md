# watchmaker (`wmk`) Plan 1 of 4: Core Round Trip — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A working `wmk` that can `init` a repo, `ingest` a directory into deduplicated chunk packs plus a SQLite catalog, `restore` it byte-exact, and prove that with `manifest`, `verify`, and `diff`. The inspection commands (`ls`, `show`, `stats`, `resolve`, `find`, `cat`) are included.

**Architecture:** A Cargo workspace with a library crate (`drmanhattan-core`) and a CLI crate (`drmanhattan`, binary `wmk`). Ingest is a pipeline: a sequential pre-order walker feeds a pool of reader/chunker workers. The workers feed a single storage thread, which appends units to packs, fsyncs, and forwards catalog ops with commit barriers. The storage thread feeds a single catalog-writer thread. Restore reads units through a shared `UnitReader` (pack handles plus an LRU cache) and writes into a partial directory that is renamed into place at the end.

**Tech Stack:** Rust (stable ≥ 1.85, edition 2021). Libraries: `fastcdc` (v2020), `blake3`, `zstd`, `rusqlite` (bundled), `rayon`, `crossbeam-channel`, `uuid` (v7), `ciborium`, `serde`, `toml`, `thiserror`, `fs4`, `filetime`, `xattr`, `imagesize`, `infer`, `lru`, `unicode-normalization`, `libc`, `fail`. CLI: `clap` (derive, env), `anyhow`, `serde_json`, `indicatif`. Tests: `tempfile`, `proptest`, `assert_cmd`, `predicates`.

**Spec:** `docs/superpowers/specs/2026-10-24-wmk-v1-design.md`

**Plan series:**

| Plan | Contents |
|---|---|
| 1 (this one) | Core round trip |
| 2 | `recover`, plus the fault-injection suite |
| 3 | Screening, provenance, evidence report, `screen`, `purge` |
| 4 | Labels, `analyze`, `docs/SCHEMA.md` with the Python contract tests, benchmarks |

Plan 1 creates the **full** §6.1 schema and the `Screener` hook point, so later plans add behavior without migrations.

## Global Constraints

- **Hashing:** BLAKE3-256 for all content, unit, pack, and manifest hashes. Stored as 32-byte BLOBs and printed as lowercase hex.
- **IDs:** UUIDv7, stored as 16-byte BLOBs, printed hyphenated.
- **Time:** integer nanoseconds since the Unix epoch, signed `i64`.
- **Chunk profiles** (fixed at `init`):
  - text: min 2048 / avg 8192 / max 65536
  - binary: min 262144 / avg 1048576 / max 4194304

  Text profile applies to sniffed text formats with size ≤ 4 MiB (4194304).
- **Compression:** zstd level 3. Use `none` for precompressed formats, or when the zstd output is ≥ 97% of the raw size.
- **Pack size:** `max_size = 268435456`. Pack path: `packs/<last 2 hex chars of pack id>/<pack-id hyphenated>.pack`.
- **Pack format:** exactly as spec §6.2, little-endian, record marker `0x574D4B52`, header magic `WMKPACK\0`, footer magic `WMKPEND\0`.
- **Manifest encoding:** spec §6.4, canonical order = depth-first pre-order with children sorted bytewise by name. Modes are masked with `& 0o1777`.
- **Naming:**
  - user-facing name: "watchmaker (`wmk`)"
  - config `wmk.toml`, lock `wmk.lock`, env `WMK_REPO`
  - partial restore dir `<dest>.wmk-partial-<snapshot-id>`
- **Exit codes:** 0 ok, 1 error, 2 verification failure or differences found, 3 entries withheld by screening.
- **Help text:** `wmk --help` first line: `wmk (watchmaker): disassemble directories into storage units and rebuild them exactly`
- **Platforms:** macOS and Linux only (`#[cfg(unix)]`). Use `std::os::unix::ffi::OsStrExt` for raw-byte names everywhere. Never use `to_string_lossy` on a stored path.
- **Errors:** `thiserror` in core, `anyhow` in the CLI. Never `unwrap()` on I/O in non-test code.
- **CI gate per task:** `cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`

## Review Focus

1. **Repo and source overlap.** `wmk ingest` of a directory containing the repo, or a source inside the repo, must refuse before writing anything. Otherwise it ingests its own packs as they grow. *Test in Task 8.*
2. **Unreadable or vanishing files.** A `chmod 000` file, or a file deleted after the walk, must fail the ingest (exit 1), name the path, and leave the snapshot `failed`. It must never be skipped silently. *Test in Task 9.*
3. **Zero-byte files and exact chunk-boundary sizes.** An empty file must restore as an empty file with `unit_count = 0`. Sizes `min-1`, `min`, `max`, and `max+1` must round-trip. *Tests in Tasks 2 and 13.*
4. **Hostile names and long paths.** These must ingest, restore, and diff byte-exact, or fail preflight with a list:
   - names with `\n`, `\\`, leading `-`, a literal `..` inside a longer name, or 255 bytes
   - restored paths longer than `PATH_MAX`

   *Tests in Tasks 6 and 10.*
5. **Read-only directories in the source** (mode `0o555`, with children). Restore must still write the children, then apply `0o555` last. *Test in Task 10.*

---

## File Structure

```
Cargo.toml                         # workspace: members = ["crates/core", "crates/cli"]
rust-toolchain.toml                # channel = "stable"
.github/workflows/ci.yml           # Task 13
crates/core/Cargo.toml             # name = "drmanhattan-core"; feature "failpoints" = ["fail/failpoints"]
crates/core/src/
  lib.rs                           # pub mod declarations only
  error.rs                         # Error enum, Result alias
  ids.rs                           # Hash32, Id
  config.rs                        # Config (wmk.toml)
  repo.rs                          # Repo::init/open, WriterLock
  chunk.rs                         # Profile, Chunk, chunk_reader
  sniff.rs                         # Format detection, profile choice
  compress.rs                      # Compression, encode/decode
  pack.rs                          # PackWriter, PackReader, scan
  model.rs                         # EntryKind, EntryStatus, *Row structs
  catalog/mod.rs                   # Catalog (open, queries)
  catalog/schema.rs                # SCHEMA_SQL, user_version
  catalog/writer.rs                # CatalogOp, spawn_catalog_writer
  walk.rs                          # pre-order walker
  manifest.rs                      # ManifestHasher, manifest_dir, manifest_snapshot
  unitread.rs                      # UnitReader (pack handles + LRU), read_content
  ingest/mod.rs                    # ingest::run, IngestOptions, IngestReport
  ingest/worker.rs                 # per-file read/hash/chunk/compress
  ingest/storage.rs                # storage thread (pack append, fsync barriers)
  ingest/sequencer.rs              # reorder buffer for manifest hashing
  ingest/record.rs                 # snapshot record encode/decode
  ingest/screen.rs                 # Screener trait + NoopScreener (hook only)
  restore/mod.rs                   # restore::run, RestoreOptions, RestoreReport
  restore/preflight.rs             # FsProbe, collisions, name/path checks
  verify.rs                        # verify::run
  diff.rs                          # diff::run, Difference
crates/core/tests/common/mod.rs    # TreeBuilder fixture helper
crates/core/tests/*.rs             # one file per area (named in tasks)
crates/cli/Cargo.toml              # name = "drmanhattan"; [[bin]] name = "wmk"
crates/cli/src/main.rs             # clap root, global --repo/--json, exit codes
crates/cli/src/cmd/<command>.rs    # one file per subcommand
crates/cli/src/output.rs           # print human or JSON
crates/cli/tests/cli.rs            # assert_cmd end-to-end tests
tests/fixtures/repo-v1/            # Task 13 committed fixture repo
```

---

### Task 1: Workspace, config, repo init/open, writer lock, `wmk init`

**Files:**
- Create: `Cargo.toml`, `rust-toolchain.toml`, `.gitignore` (`/target`)
- Create: `crates/core/{Cargo.toml, src/lib.rs, src/error.rs, src/ids.rs, src/config.rs, src/repo.rs}`
- Create: `crates/cli/{Cargo.toml, src/main.rs, src/output.rs, src/cmd/mod.rs, src/cmd/init.rs}`
- Test: `crates/core/tests/repo.rs`, `crates/cli/tests/cli.rs`

**Interfaces:**
- Produces:
  - `ids::Hash32([u8; 32])`: `Hash32::of(&[u8]) -> Hash32`, `to_hex() -> String`, `from_hex(&str) -> Result<Hash32>`, `Display` as hex. Also derives `Copy, Eq, Hash, Ord, Serialize, Deserialize`.
  - `ids::Id([u8; 16])`: `Id::new_v7() -> Id`, `from_bytes`, `as_bytes`, `parse(&str) -> Result<Id>`, `Display` hyphenated.
  - `config::Profile { min: u32, avg: u32, max: u32 }`
  - `config::Config { format_version: u32, chunking: Chunking { text: Profile, binary: Profile }, compression: CompressionCfg { algo: String, level: i32 }, packs: PacksCfg { max_size: u64 } }`, with `Config::default()` set to the Global Constraints values, `Config::load(&Path)` and `Config::save(&Path)`.
  - `repo::Repo { root: PathBuf, config: Config }`: `Repo::init(&Path) -> Result<Repo>`, `Repo::open(&Path) -> Result<Repo>`, `catalog_path()`, `packs_dir()`, `lock_writer() -> Result<WriterLock>` (non-blocking `fs4` exclusive lock on `wmk.lock`; fails with `Error::Locked`).
  - `error::Error` variants used by later tasks: `Io { path: PathBuf, source }`, `Locked`, `NotARepo`, `RepoNotEmpty`, `Corrupt(String)`, `NotFound(String)`, `Ambiguous(String)`, `Preflight(Vec<String>)`, `Unstable(PathBuf)`, `Overlap`, `Sqlite`, `Config`.
  - CLI root: `wmk [--repo <path> | env WMK_REPO] [--json] <command>`, and `fn exit_code(&anyhow::Error) -> i32`, which maps core errors to 1 (later tasks add 2 and 3).

- [ ] **Step 0: Toolchain.** Run `rustup --version`. If missing, stop and ask the user to install rustup; the Homebrew rustc 1.79 is too old. Then `rustup toolchain install stable`.
- [ ] **Step 1: Write failing tests in `crates/core/tests/repo.rs`:**
  - `init_creates_layout`: after `Repo::init(tmp/"r")`, `r/wmk.toml`, `r/packs/`, and `r/catalog.sqlite` exist, and `Config::load` equals `Config::default()`, with text avg 8192 and pack max 268435456. (The catalog file is created by Task 5; until then, assert only on `wmk.toml` and `packs/`, and add the catalog assertion in Task 5.)
  - `init_refuses_nonempty_dir`: `Repo::init` on a dir containing any file → `Err(Error::RepoNotEmpty)`.
  - `open_non_repo_fails`: `Repo::open(tmp)` → `Err(Error::NotARepo)`.
  - `second_writer_lock_fails`: hold `lock_writer()`, then call `lock_writer()` again on a second `Repo::open` → `Err(Error::Locked)`. After the first lock is dropped, the call succeeds.
- [ ] **Step 2:** Run `cargo test -p drmanhattan-core --test repo`. Expected: FAIL (does not compile).
- [ ] **Step 3: Implement** `ids`, `config`, `repo`, and `error`. `wmk.toml` is the spec §5 TOML, serialized with serde.
- [ ] **Step 4: Write failing CLI tests in `crates/cli/tests/cli.rs`:**
  - `help_first_line`: `wmk --help` stdout starts with the exact help text above.
  - `init_then_init_fails`: `wmk init <tmp>/r` exits 0. A second identical call exits 1, and its stderr contains `not empty`.
- [ ] **Step 5: Implement the CLI root and `cmd/init.rs`.** `output.rs` exposes `fn emit<T: Serialize + std::fmt::Display>(json: bool, value: &T)`.
- [ ] **Step 6:** Run the CI gate. Expected: PASS.
- [ ] **Step 7: Commit** with message `feat: workspace, config, repo init and writer lock`.

---

### Task 2: Content-defined chunking

**Files:**
- Create: `crates/core/src/chunk.rs`
- Test: `crates/core/tests/chunk.rs`, `crates/core/tests/vectors/chunk_vectors.json` (generated on the first passing run, then committed)

**Interfaces:**
- Consumes: `config::Profile`, `ids::Hash32`.
- Produces:
  - `chunk::Chunk { offset: u64, len: u32, hash: Hash32 }`
  - `chunk::FileDigest { hash: Hash32, size: u64 }`
  - `chunk::chunk_reader<R: Read>(p: &Profile, r: R, mut on_chunk: impl FnMut(Chunk, &[u8]) -> Result<()>) -> Result<FileDigest>`, which computes the whole-file BLAKE3 and per-chunk BLAKE3 in one pass, using `fastcdc::v2020::StreamCDC::new(r, min, avg, max)`.

- [ ] **Step 1: Write failing tests:**
  - `empty_input_zero_chunks`: on `&[]`, no callback fires, `size == 0`, and `hash == Hash32::of(b"")`.
  - `below_min_is_single_chunk`: 2047 bytes with the text profile → exactly 1 chunk of len 2047.
  - `chunks_reassemble_and_hash`: on 5 MiB of seeded pseudo-random bytes (`rand_chacha` with seed 42, as a dev-dependency), the concatenated chunk slices equal the input. Every `chunk.hash == Hash32::of(slice)`. The digest equals `Hash32::of(input)`. For the binary profile, all `len` values are ≤ 4194304 and all except the last are ≥ 262144.
  - `boundaries_stable_after_prefix_edit`: insert 10 bytes at offset 1000 in the 5 MiB input. At least 80% of the chunk hashes from the original are still present.
  - `golden_vectors`: for both profiles, the chunk `(offset, len)` lists over seeded inputs of 1 MiB and 9 MiB must equal `vectors/chunk_vectors.json`. If the file is absent, the test writes it and fails with "vectors generated; commit them". Once committed, the test asserts equality, so later changes cannot move chunk boundaries.
- [ ] **Step 2:** Run `cargo test -p drmanhattan-core --test chunk`. Expected: FAIL.
- [ ] **Step 3: Implement `chunk_reader`.**
- [ ] **Step 4:** Run the tests twice: the first run generates the vectors, the second must PASS. Then run the CI gate.
- [ ] **Step 5: Commit** `feat: content-defined chunking with golden vectors`, including `vectors/chunk_vectors.json`.

---

### Task 3: Format sniffing and compression policy

**Files:**
- Create: `crates/core/src/sniff.rs`, `crates/core/src/compress.rs`
- Test: `crates/core/tests/sniff_compress.rs`

**Interfaces:**
- Produces:
  - `sniff::Format`: an enum with variants `Jpeg, Png, Webp, Gif, Avif, Heic, Mp4, Webm, Zip, Gzip, Bzip2, Xz, Zstd, SevenZ, Html, Css, Js, Json, Xml, Svg, Txt, Csv, Md, Other`. Methods: `as_str()` returns lowercase names (`"jpeg"`, `"seven_z"`, …), plus `from_str`, `is_text()`, and `is_precompressed()`.
  - `sniff::detect(name: &[u8], head: &[u8]) -> Format`. Magic bytes are checked first, via `infer` on `head`, which holds at most the first 8192 bytes. If that finds nothing, the lowercase extension of `name` is matched against `html, htm, css, js, mjs, json, xml, svg, txt, csv, md`. If neither matches, the result is `Other`.
  - `sniff::ProfileKind { Text, Binary }`, with `as_str()` returning `"text"` or `"binary"`.
  - `sniff::profile_kind(f: Format, size: u64) -> ProfileKind`, which returns `Text` iff `f.is_text() && size <= 4194304`.
  - `compress::Compression { None = 0, Zstd = 1 }`, with `as_str()`, `from_u8`.
  - `compress::encode(raw: &[u8], precompressed: bool, level: i32) -> Result<(Compression, Cow<[u8]>)>`
  - `compress::decode(c: Compression, stored: &[u8], raw_len: u64) -> Result<Vec<u8>>`

- [ ] **Step 1: Write failing tests:**
  - `jpeg_by_magic_ignores_extension`: head `FF D8 FF E0`, name `x.html` → `Jpeg`.
  - `html_by_extension`: head `<!doctype html>`, name `a/index.HTM` → `Html`.
  - `unknown_is_other`.
  - `text_profile_cutoff`: `profile_kind(Html, 4194304) == Text`, and `profile_kind(Html, 4194305) == Binary`. A JPEG of any size → `Binary`.
  - `precompressed_skips_zstd`: `encode(data, true, 3)` → `None`.
  - `incompressible_stays_raw`: 64 KiB of random bytes → `None`.
  - `compressible_uses_zstd`: 64 KiB of repeated HTML → `Zstd`, and `decode(...)` round-trips.
  - `decode_wrong_len_errors`: `decode(Zstd, stored, raw_len + 1)` → `Err(Corrupt)`.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement.** In `encode`, use `None` when `precompressed`, or when `zstd_len * 100 >= raw_len * 97`.
- [ ] **Step 4:** Run the CI gate. Expected: PASS.
- [ ] **Step 5: Commit** `feat: format sniffing and compression policy`.

---

### Task 4: Pack format

**Files:**
- Create: `crates/core/src/pack.rs`
- Test: `crates/core/tests/pack.rs`

**Interfaces:**
- Consumes: `Id`, `Hash32`, `Compression`.
- Produces:
  - `pack::UnitKind { RawChunk = 0, SnapshotRecord = 1, PurgeRecord = 2 }`, with `as_str()` returning `raw_chunk`, `snapshot_record`, or `purge_record`.
  - `pack::RecordHeader { kind: UnitKind, compression: Compression, unit_hash: Hash32, raw_len: u64, stored_len: u64 }`
  - `pack::pack_path(packs_dir: &Path, id: Id) -> PathBuf`
  - `PackWriter::create(packs_dir, id) -> Result<PackWriter>`, which writes the header.
  - `append(&mut self, h: &RecordHeader, payload: &[u8]) -> Result<u64>`, which returns the record's offset.
  - `len(&self) -> u64`, `sync(&mut self) -> Result<()>` (`fsync`), and `seal(self) -> Result<SealedPack { id: Id, size: u64, blake3: Hash32 }>`. `seal` writes the footer and fsyncs both the file and its parent dir.
  - `PackReader::open(path) -> Result<PackReader>`
  - `read_record(&self, offset) -> Result<(RecordHeader, Vec<u8>)>`, using `pread` via `FileExt::read_at`.
  - `read_unit(&self, offset, expect: &Hash32) -> Result<Vec<u8>>`, which decodes and checks BLAKE3, returning `Err(Corrupt)` on mismatch.
  - `pack::scan(path) -> Result<PackScan { id: Id, records: Vec<(u64, RecordHeader)>, sealed: bool, valid_len: u64, blake3_ok: Option<bool> }>`, which stops at the first bad marker or CRC.

- [ ] **Step 1: Write failing tests:**
  - `append_read_roundtrip`: 3 units (zstd and none). `read_unit` returns the raw bytes for each.
  - `seal_footer_and_hash`: after `seal`, `scan` reports `sealed = true`, 3 records, and `blake3_ok = Some(true)`.
  - `flipped_payload_byte_detected`: flip one payload byte on disk → `read_unit` returns `Err(Corrupt)`, and `scan` reports `blake3_ok = Some(false)`.
  - `flipped_header_byte_stops_scan`: flip a byte inside record 2's header → `scan` returns 1 record, and `valid_len` equals record 2's offset.
  - `torn_tail_unsealed`: append 2 units, `sync`, then truncate the file mid-way through record 2 → `scan` reports `sealed = false`, 1 record, and `valid_len` at record 2's offset.
  - `path_fanout`: `pack_path` uses the last 2 hex chars of the id as the subdirectory.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement.** Compute `crc32c` over the 50 header bytes that follow the marker: kind (1) + compression (1) + hash (32) + raw_len (8) + stored_len (8). The footer's `pack_blake3` is computed incrementally while writing.
- [ ] **Step 4:** Run the CI gate. Expected: PASS.
- [ ] **Step 5: Commit** `feat: self-describing pack format`.

---

### Task 5: Catalog schema, writer thread, and queries

**Files:**
- Create: `crates/core/src/model.rs`, `crates/core/src/catalog/{mod.rs, schema.rs, writer.rs}`
- Modify: `crates/core/src/repo.rs` (`init` creates the catalog)
- Test: `crates/core/tests/catalog.rs`

**Interfaces:**
- Produces:
  - `model::EntryKind { Dir, File, Symlink }`, where `as_str` and `u8` give `dir = 0`, `file = 1`, `symlink = 2`, matching the §6.4 kind byte.
  - `model::EntryStatus { Ok, Unstable, ScreenedOut, ScreenError, Purged }`
  - `model::SnapshotStatus { InProgress, Complete, Failed }`
  - `model::EntryRow`, `ContentRow`, `UnitRow`, `PackRow`, and `SnapshotRow`, with fields exactly as the §6.1 columns. Rust types: `Id`, `Hash32`, `Vec<u8>` for BLOB paths and names, `i64` for times, and `Option` for nullable columns.
  - `catalog::schema::SCHEMA_SQL` creates **every** table and index in §6.1 and sets `PRAGMA user_version = 1`.
  - `Catalog::open(path) -> Result<Catalog>`: read-write. Sets WAL, `synchronous=NORMAL`, and `foreign_keys=ON`, and creates the schema if `user_version == 0`. If `user_version` is anything other than 0 or 1, it fails with `Corrupt`.
  - `Catalog::open_readonly(path)`
  - Queries:
    - `has_content(&Hash32) -> Result<bool>`, `has_unit(&Hash32) -> Result<bool>`
    - `snapshot(Id) -> Result<SnapshotRow>`, `snapshots() -> Result<Vec<SnapshotRow>>`
    - `resolve_snapshot(&str) -> Result<Id>`: accepts `latest` (the newest complete snapshot), a full UUID, or a hex prefix of at least 8 chars with hyphens ignored. Returns `NotFound` or `Ambiguous`.
    - `children(snapshot: Id, parent: Option<Id>) -> Result<Vec<EntryRow>>`: ordered by `name` bytewise. SQLite BLOB comparison is memcmp, which is correct.
    - `entry(Id) -> Result<EntryRow>`, `entry_by_path(snapshot, &[u8]) -> Result<EntryRow>`
    - `content(&Hash32) -> Result<ContentRow>`
    - `content_units(&Hash32) -> Result<Vec<UnitRow>>`, in `seq` order
    - `unit(&Hash32) -> Result<UnitRow>`
    - `entries_with_content(&Hash32) -> Result<Vec<EntryRow>>`
    - `xattrs(Id) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`
  - `catalog::writer::CatalogOp`, an enum with variants `UpsertSnapshot(SnapshotRow)`, `InsertEntry(EntryRow)`, `InsertContent(ContentRow, Vec<Hash32>)`, `InsertUnit(UnitRow)`, `UpsertPack(PackRow)`, `InsertXattr(Id, Vec<u8>, Vec<u8>)`, `InsertMedia { content: Hash32, width: u32, height: u32 }`, and `Commit`.
  - `spawn_catalog_writer(path: PathBuf, rx: Receiver<CatalogOp>) -> JoinHandle<Result<CatalogStats>>`. It holds one open transaction and commits **only** on `Commit` or on channel close. Inserts use `INSERT OR IGNORE` for content, unit, and media.

- [ ] **Step 1: Write failing tests:**
  - `schema_has_all_tables`: the table names include all 14 tables from §6.1, and `user_version == 1`.
  - `blob_order_is_bytewise`: children named `b"a-c"`, `b"a"`, `b"\xff"`, and `b"B"` come back in the order `B, a, a-c, \xff`.
  - `writer_commits_only_on_commit`: send an `InsertEntry` without `Commit`. A separate read-only connection sees 0 rows. After `Commit`, it sees 1 row.
  - `resolve_snapshot_forms`: covers `latest`, full UUID, an 8-char prefix, an ambiguous prefix → `Ambiguous`, and an unknown prefix → `NotFound`.
  - `content_units_in_seq_order`.
  - `future_user_version_rejected`: set `user_version = 2` → `open` returns `Err(Corrupt)`.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement.** Also add the catalog assertion to `init_creates_layout` from Task 1.
- [ ] **Step 4:** Run the CI gate. Expected: PASS.
- [ ] **Step 5: Commit** `feat: catalog schema, batched writer, queries`.

---

### Task 6: Walker and test tree builder

**Files:**
- Create: `crates/core/src/walk.rs`, `crates/core/tests/common/mod.rs`
- Test: `crates/core/tests/walk.rs`

**Interfaces:**
- Produces:
  - `walk::WalkOptions { cross_fs: bool }`
  - `walk::WalkItem`, an enum:
    - `Entry(WalkEntry { depth: u32, path: Vec<u8>, name: Vec<u8>, kind: EntryKind, mode: u32, mtime_ns: i64, size: u64, dev: u64, ino: u64, nlink: u64, link_target: Option<Vec<u8>> })`
    - `Skipped { path: Vec<u8>, reason: SkipReason }`, where `SkipReason` is `Socket | Fifo | CharDevice | BlockDevice | OtherFilesystem`.
  - `walk::walk(root: &Path, opts: &WalkOptions, sink: impl FnMut(WalkItem) -> Result<()>) -> Result<()>`. Visits in canonical order (pre-order, children sorted bytewise by name) using `lstat` only. The root is emitted first with `path = b""` and `name = b""`. Paths are relative and `/`-joined. Errors carry the path.
  - `common::TreeBuilder` (tests only):
    - `new(root)`
    - `file(path, bytes)`, `dir(path)`, `symlink(path, target)`, `hardlink(path, existing)`
    - `mode(path, u32)`, `mtime(path, i64 ns)`, `fifo(path)`
    - `finish()`, which applies mtimes deepest-first so directory mtimes stick.

- [ ] **Step 1: Write failing tests:**
  - `canonical_order`: the tree `a/x`, `a-c`, `a/y`, and `b` walks as `"", "a", "a/x", "a/y", "a-c", "b"`.
  - `symlinks_not_followed`: a symlink to a dir is emitted as a `Symlink` with its target bytes, and its children are never visited. A dangling symlink is still emitted.
  - `fifo_skipped`: a FIFO is reported as `Skipped(Fifo)`.
  - `mtime_ns_preserved`: an mtime of `1_700_000_000_123_456_789` reads back exactly. Skip the test if the filesystem lacks ns precision, detected by probing.
  - `negative_mtime`: an mtime of `-86_400_000_000_000` reads back exactly.
  - `hostile_names` (Review Focus 4): names `"line\nbreak"`, `"back\\slash"`, `"-rf"`, `"a..b"`, and `"x".repeat(255)` all walk with exact bytes.
  - `non_utf8_name` (`#[cfg(target_os = "linux")]`): name `b"\xff\xfe.jpg"` walks with exact bytes.
  - `hardlink_identity`: two hard-linked paths share `(dev, ino)` and have `nlink == 2`.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement.** Use `std::fs::read_dir`, collect each directory's names, sort them, and recurse. `OtherFilesystem` applies when `dev != root dev && !cross_fs`; such dirs are emitted as Skipped and not descended. `mode` is `st_mode & 0o7777`. `mtime_ns` is `meta.mtime() * 1_000_000_000 + meta.mtime_nsec()`, in i64 integer arithmetic via `MetadataExt`.
- [ ] **Step 4:** Run the CI gate. Expected: PASS.
- [ ] **Step 5: Commit** `feat: canonical pre-order walker and test tree builder`.

---

### Task 7: Manifest hashing and `wmk manifest`

**Files:**
- Create: `crates/core/src/manifest.rs`, `crates/cli/src/cmd/manifest.rs`
- Test: `crates/core/tests/manifest.rs`, `crates/core/tests/vectors/manifest_golden.hex`

**Interfaces:**
- Consumes: `walk`. File bytes are hashed with `blake3::Hasher` over a buffered read, with no chunking.
- Produces:
  - `manifest::ManifestEntry<'a> { path: &'a [u8], kind: EntryKind, mode: u32, mtime_ns: i64, size: u64, content: Option<Hash32>, link_target: Option<&'a [u8]>, hardlink_first: Option<u64>, xattrs: &'a [(Vec<u8>, Vec<u8>)] }`
  - `manifest::OwnedManifestEntry`: the same fields, owned (`Vec<u8>` for path, link target, and xattrs), with `as_entry(&self) -> ManifestEntry<'_>`. Used by the ingest sequencer.
  - `ManifestHasher::new(mtime_precision_ns: u64)`, `push(&mut self, e: &ManifestEntry)`, `finish(self) -> ManifestHashes { core: Hash32, ext: Hash32, entries: u64 }`.
    - The mode is masked with `& 0o1777`.
    - mtime is floored to the precision: `m - m.rem_euclid(p)`.
    - Entries must be pushed in canonical order.
  - `manifest::manifest_dir(root: &Path, opts: &ManifestOptions { mtime_precision_ns: u64, cross_fs: bool }) -> Result<ManifestHashes>`. Hard links are assigned `hardlink_first` = the canonical index of the first entry with the same `(dev, ino)`, for files with `nlink > 1`. Xattrs are read with the `xattr` crate and sorted by name.
  - `manifest::manifest_snapshot(cat: &Catalog, snapshot: Id, root_path: &[u8], opts) -> Result<ManifestHashes>`. Recursive `children()` traversal from the entry at `root_path`, where `b""` means the whole snapshot. Paths are re-based relative to that entry. Skips entries whose status is not `Ok` or `Unstable`.
  - CLI: `wmk manifest <dir> [--ext] [--mtime-precision <ns>]` prints the hex of `core`, or of `ext` with `--ext`. With `--json`, it prints `{core, ext, entries}`.

- [ ] **Step 1: Write failing tests:**
  - `golden_encoding`: a fixed tree is built in a tempdir with explicit modes and mtimes:
    - root `0o755`
    - `a.txt` = "hello", `0o644`, mtime 1_000_000_000_000_000_001
    - `d/` `0o700`
    - `d/l` → symlink to `"../a.txt"`

    `core` must equal the hex in `vectors/manifest_golden.hex`, generated on the first passing run and committed. A second assertion hand-builds the expected byte stream per §6.4 and checks `Hash32::of(stream) == core`.
  - `uuid_independent`: the same tree in two different temp roots gives an identical `core`.
  - `mode_and_mtime_sensitive`: changing one file's mode, one mtime by 1 ns, one byte of content, or one symlink target changes `core`.
  - `setuid_ignored`: adding `0o4000` to a file's mode leaves `core` unchanged.
  - `precision_floor`: with precision 1_000_000_000, mtimes `…_123` and `…_999` in the same second give the same hash.
  - `ext_includes_hardlinks`: two independent copies and one hard-linked pair have the same `core` but a different `ext`.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement.** Encoding per §6.4: `path_len u32 LE | path | kind u8 | …`. The `ext` stream is the core encoding of each entry followed by `hardlink_first` (u64 LE, `u64::MAX` when absent), `xattr_count u32`, then for each xattr `name_len u32 | name | value_len u32 | value`.
- [ ] **Step 4:** Run the tests twice (the first run generates the golden file), then run the CI gate. Expected: PASS.
- [ ] **Step 5: Commit** `feat: canonical manifest hashing and wmk manifest`.

---

### Task 8: Ingest pipeline (happy path) and `wmk ingest`

**Files:**
- Create: `crates/core/src/ingest/{mod.rs, worker.rs, storage.rs, sequencer.rs, screen.rs}`, `crates/cli/src/cmd/ingest.rs`
- Test: `crates/core/tests/ingest.rs`

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces:
  - `ingest::IngestOptions { note: Option<String>, allow_unstable: bool, cross_fs: bool, workers: usize /* 0 = num cpus */ }`
  - `ingest::IngestReport { snapshot: Id, entries: u64, files: u64, bytes_read: u64, new_units: u64, new_bytes_stored: u64, dedup_ratio: f64, skipped: Vec<(Vec<u8>, SkipReason)>, unstable: Vec<Vec<u8>>, withheld: u64, manifest_hash: Hash32, elapsed_ms: u64 }`
  - `ingest::run(repo: &Repo, source: &Path, opts: &IngestOptions) -> Result<IngestReport>`
  - `ingest::screen::Screener` trait: `fn screen(&self, path: &[u8], content: &Hash32, size: u64, format: Format) -> Verdict`, where `Verdict` is `Pass | Match(String) | Error(String)`. Also `NoopScreener` (always `Pass`). `run` uses `NoopScreener`; Plan 3 adds a parameter.
  - `ingest::sequencer::Sequencer`: `new()`, `insert(seq: u64, entry: OwnedManifestEntry)`, `drain_ready(&mut self, f: impl FnMut(&OwnedManifestEntry))`. A reorder buffer that releases entries in `seq` order.

**Pipeline** (the decisions the implementer can't infer):
1. `lock_writer()`. Canonicalize `source` and `repo.root`. If either contains the other, return `Err(Error::Overlap)`.
2. Insert `SnapshotRow { status: InProgress, … }` and `Commit`.
3. **Walker** (main thread). Assigns `seq` (canonical index) and `Id::new_v7()` to each entry, keeping a `Vec<Id>` stack by depth to supply `parent_id`.
   - Dirs and symlinks go straight to the storage channel as `StorageMsg::Entry(EntryRow, seq)`.
   - Files go as `FileJob { seq, id, parent, abs_path, walk_entry }` on a bounded channel (capacity `4 * workers`) to the worker pool.
4. **Workers** (`std::thread` × N).
   - Open and stat the file, then run `chunk_reader` with the profile from `sniff::profile_kind`, sniffing on the first 8192 bytes.
   - Buffer the chunks in memory (Plan 3 handles files over 64 MiB with a two-pass read).
   - Look up the content hash in the shared `Mutex<HashSet<Hash32>>` of in-flight contents, then via `has_content` on the worker's own read-only `Catalog`.
   - If the content is new: compress each chunk with `encode(…, format.is_precompressed(), level)`, and send `StorageMsg::NewContent(ContentRow, Vec<(RecordHeader, Vec<u8>)>)`.
   - Always send `StorageMsg::Entry(EntryRow, seq)` afterwards.
   - Image dims: if `format` is jpeg, png, webp, gif, or avif, call `imagesize::blob_size(head)` and send `InsertMedia` on success.
5. **Storage thread** (`ingest/storage.rs`).
   - Owns the open `PackWriter` and a `HashSet<Hash32>` of units written this run, and checks `has_unit` on its own read-only connection.
   - New units are appended; units already present are skipped. A pack is sealed at `max_size`, and a new one is created.
   - Forwards `InsertUnit`, `InsertContent`, `UpsertPack`, `InsertEntry`, and `InsertMedia` to the catalog channel in arrival order.
   - Every 10_000 forwarded ops, or every 1 s: `pack.sync()`, then send `Commit`. This is the ordering invariant: fsync before commit.
   - Also feeds each entry's `OwnedManifestEntry` to the `Sequencer` and pushes released entries into a `ManifestHasher` (precision 1).
6. **Finish.** After the walker and workers finish:
   - Seal the open pack (if non-empty).
   - Write the snapshot record (Task 9; in this task, leave `record_unit = NULL`).
   - Set `manifest_hash`, `ext_manifest_hash`, and `source_manifest_hash` (equal to `manifest_hash` in Plan 1), plus `status = Complete`, `completed_at`, `entry_count`, and `file_bytes`.
   - Final `Commit`.
7. **On any error:** set the snapshot to `status = Failed`, commit, and return the error. Pack bytes already written are harmless.

CLI: `wmk ingest <dir> [--note] [--allow-unstable] [--cross-fs] [--workers N]` prints a report (human, or JSON with `--json`) and exits 0. Exit 3 is used only when `withheld > 0`, which Plan 3 makes possible.

- [ ] **Step 1: Write failing tests in `ingest.rs`:**
  - `ingest_manifest_matches_dir`: on a tree of 20 files (mixed html, jpeg-magic, random), 3 dirs, and 1 symlink, `report.manifest_hash == manifest_dir(source).core`, and the snapshot status is `Complete`.
  - `exact_duplicates_stored_once`: the same 300 KiB file at 5 paths gives 5 entries sharing one `content_hash`. The `unit` row count equals the chunk count of a single copy.
  - `reingest_zero_new_units`: a second ingest of the same tree gives `new_units == 0` and an equal `manifest_hash`, under a different snapshot Id with different entry Ids.
  - `shared_template_dedups`: 50 HTML files of about 100 KiB each, sharing 80 KiB of identical prefix with unique tails, give `new_bytes_stored` < 25% of `bytes_read`.
  - `overlap_refused` (Review Focus 1): a repo inside the source, or a source inside the repo → `Err(Overlap)`. No snapshot row is created and `packs/` is empty.
  - `concurrent_ingest_locked`: hold `lock_writer()` → `ingest::run` returns `Err(Locked)`.
  - `fsync_before_commit`: no unit row references a pack offset past the pack's fsynced length. Assert by opening the catalog after the run and checking `unit.pack_offset < pack.size` for every unit.
- [ ] **Step 2:** Run `cargo test -p drmanhattan-core --test ingest`. Expected: FAIL.
- [ ] **Step 3: Implement** the pipeline above, plus the CLI command.
- [ ] **Step 4: Add the CLI test** `ingest_reports_snapshot`: after `wmk --repo r ingest src --json`, stdout parses as JSON with `snapshot` and `manifest_hash`, the exit code is 0, and `wmk manifest src` prints the same hash.
- [ ] **Step 5:** Run the CI gate. Expected: PASS.
- [ ] **Step 6: Commit** `feat: parallel ingest pipeline with fsync-before-commit`.

---

### Task 9: Ingest fidelity (hard links, xattrs, stability, unreadable files, snapshot record)

**Files:**
- Create: `crates/core/src/ingest/record.rs`, `crates/core/src/unitread.rs`
- Modify: `crates/core/src/ingest/{mod.rs, worker.rs, storage.rs}`
- Test: `crates/core/tests/ingest_fidelity.rs` (with failpoint tests behind `#[cfg(feature = "failpoints")]`)

**Interfaces:**
- Consumes: Task 8.
- Produces:
  - Hard links: during the walk, `HashMap<(dev, ino), i64>` assigns `hardlink_group` (a per-snapshot counter starting at 1) to files with `nlink > 1`.
  - Xattrs are read for every entry with `xattr::list` and `xattr::get`, which do not follow symlinks; never use the `_deref` variants. They are sent as `InsertXattr` and feed `ext_manifest_hash`.
  - `hardlink_first` for the manifest: the walker keeps `HashMap<(dev, ino), u64>` mapping each group to the `seq` of its first member, and passes it with each file entry to the storage thread.
  - Stability:
    - The worker compares `(size, mtime_ns)` from the walk with an `fstat` before the read and an `fstat` after it.
    - On a mismatch, it retries once by re-statting, re-reading, and re-comparing.
    - If it mismatches again: with `allow_unstable` set, the entry is stored with `status = Unstable` and added to `report.unstable`. Otherwise it fails with `Error::Unstable(path)`.
    - Failpoint `ingest::after_read` sits between the read and the second stat.
  - Unreadable files: an open/read error fails the ingest with `Error::Io { path }` (no retry).
  - `ingest::record::SnapshotRecord { snapshot: SnapshotRow, entries: Vec<EntryRow>, contents: Vec<(Hash32, Vec<Hash32>)>, xattrs: Vec<(Id, Vec<u8>, Vec<u8>)> }`, serialized with `ciborium`. It is built at finish by reading back from the catalog after the last `Commit`.
  - `record::write_record(storage, &SnapshotRecord) -> Result<Hash32 /* snapshot_record unit hash */>`: the CBOR bytes go through `chunk_reader` with the **binary** profile, and each chunk is stored as a `RawChunk` unit. Then a `SnapshotRecord`-kind unit is stored, holding the CBOR of `{snapshot_id, document_blake3, document_size, chunks: Vec<Hash32>}`.
  - `unitread::UnitReader::new(repo: &Repo, cat: &Catalog, cache_bytes: usize /* default 256 MiB */) -> UnitReader` (`Send + Sync`)
  - `read_unit(&self, &UnitRow) -> Result<Arc<Vec<u8>>>`: verifies the hash, keeps pack readers open in a `Mutex<HashMap<Id, Arc<PackReader>>>`, and caches in a `Mutex<LruCache>` bounded by bytes.
  - `write_content(&self, units: &[UnitRow], out: &mut impl Write) -> Result<Hash32>`: streams the units and returns the BLAKE3 of the whole.
  - `record::read_record(reader: &UnitReader, cat: &Catalog, unit: &Hash32) -> Result<SnapshotRecord>` is used by tests now and by `recover` in Plan 2.

- [ ] **Step 1: Write failing tests:**
  - `hardlinks_grouped`: two hard-linked files share a non-null `hardlink_group`. An unlinked file has `NULL`.
  - `xattrs_recorded`: a file with xattr `user.test` (Linux) or `com.example.test` (macOS) = `b"v"` gives an `xattr` row. `ext_manifest_hash` matches `manifest_dir`'s `ext`, and differs from it when the xattr is absent.
  - `unreadable_file_fails` (Review Focus 2): a `chmod 000` file → `Err(Io)` naming the path, and the snapshot status is `Failed`. Skip when running as root.
  - `vanished_file_fails` (failpoints; Review Focus 2): a callback at `ingest::after_walk` deletes a file → `Err` naming the path, and status is `Failed`.
  - `unstable_fails_by_default` (failpoints): a callback at `ingest::after_read` appends a byte on both attempts → `Err(Unstable(path))`.
  - `unstable_allowed_flags` (failpoints): the same with `allow_unstable` → `Ok`, with the entry status `Unstable` and listed in the report.
  - `fifo_reported_skipped`: the report's `skipped` contains the FIFO path. The manifest excludes it and still equals `manifest_dir`.
  - `snapshot_record_roundtrip`: `snapshot.record_unit` is set, and `read_record` returns entries equal to the catalog rows (ids, paths, content hashes) and contents with unit lists equal to `content_units`.
- [ ] **Step 2:** Run `cargo test -p drmanhattan-core --test ingest_fidelity --features failpoints`. Expected: FAIL.
- [ ] **Step 3: Implement.** Add failpoints `ingest::after_walk` (just before the worker opens a file) and `ingest::after_read`, via `fail::fail_point!`.
- [ ] **Step 4:** Run the CI gate. Expected: PASS.
- [ ] **Step 5: Commit** `feat: ingest fidelity, stability checks, snapshot record`.

---

### Task 10: Restore and `wmk restore`

**Files:**
- Create: `crates/core/src/restore/{mod.rs, preflight.rs}`, `crates/cli/src/cmd/restore.rs`
- Test: `crates/core/tests/restore.rs`

**Interfaces:**
- Consumes: `unitread::UnitReader` (Task 9), `manifest::manifest_dir` and `manifest_snapshot` (Task 7).
- Produces:
  - `restore::RestoreOptions { path: Vec<u8> /* b"" = whole */, hardlinks: bool, xattrs: bool }`
  - `restore::RestoreReport { files: u64, dirs: u64, symlinks: u64, bytes: u64, withheld: u64, mtime_precision_ns: u64, elapsed_ms: u64 }`
  - `restore::run(repo, snapshot: Id, dest: &Path, opts) -> Result<RestoreReport>`, which calls `run_with_probe(…, None)`.
  - `#[doc(hidden)] pub fn restore::run_with_probe(repo, snapshot, dest, opts, probe: Option<FsProbe>) -> Result<RestoreReport>`. `Some` replaces the real probe. This is the test seam for simulating other filesystems.
  - `preflight::FsProbe { case_insensitive: bool, accepts_non_utf8: bool, normalizes_names: bool, mtime_precision_ns: u64, path_max: usize }`
  - `preflight::probe(dir: &Path) -> Result<FsProbe>`: creates `.wmk-probe-<uuid>` in the dest's parent and always removes it.
  - `preflight::check(entries: &[EntryRow], probe: &FsProbe, dest_abs_len: usize) -> Vec<String>`: an empty result means OK. Messages: `case collision: <a> vs <b>`, `name not representable: <path, escaped>`, `path too long (<n> > <max>): <path>`.

**Decisions:**
- **Probing:**
  - *Case:* create `wmkProbe` and check whether `WMKPROBE` exists.
  - *Non-UTF-8:* try creating `b"\xff"`.
  - *Normalization:* create the NFC name `"é"` (`b"\xc3\xa9"`), then check that `read_dir` returns the same bytes.
  - *mtime precision:* set the mtime to `…_123_456_789`, read it back, and take the precision as the largest power of 10 that divides the difference, or 1.
  - *`path_max`:* `libc::pathconf(dir, _PC_PATH_MAX)`.
- **Collision key:** per parent, `name` → NFD → lowercase (`str::to_lowercase`). This applies only to UTF-8 names; non-UTF-8 names are already rejected on filesystems where that matters.
- **Write order** (spec §9):
  1. Create the partial dir `<dest>.wmk-partial-<snapshot>`.
  2. Create all dirs in pre-order with mode `0o700`.
  3. Write files in parallel with rayon, sorted by the `(pack_id, pack_offset)` of their first unit. Each file is created with mode `0o600`, written via `write_content`, and its BLAKE3 compared with `content_hash` (on mismatch, `Err(Corrupt)` and the partial dir is left in place).
  4. Create symlinks.
  5. Hard links (`--hardlinks`): link every group member to the first-in-canonical-order member's restored path. Without the flag, members are independent copies.
  6. Set xattrs (`--xattrs`).
  7. Set file modes, then file mtimes (with `filetime`, and `set_symlink_file_times` for symlinks).
  8. Set dir modes and mtimes, deepest first.
  9. `rename(partial, dest)`.
- **`--path`:** the entry at `path` becomes `dest`, and descendants are re-based. If the entry is a file, `dest` is that file.
- Entries whose status is not `Ok` or `Unstable` are skipped and counted in `withheld`.

CLI: `wmk restore <snapshot-ref> <dest> [--path <p>] [--hardlinks] [--xattrs]`. Preflight failures exit 1 and print each message.

- [ ] **Step 1: Write failing tests:**
  - `roundtrip_manifest_equal`: for the Task 8 tree, `manifest_dir(dest) == snapshot.manifest_hash`, and the bytes of every file are equal.
  - `roundtrip_ext_with_flags`: the Task 9 hardlink and xattr tree, restored with `hardlinks` and `xattrs`, gives `manifest_dir(dest).ext == ext_manifest_hash`. Without the flags, `core` still matches.
  - `dest_exists_refused`: an existing `dest` → `Err`, with nothing written.
  - `readonly_dirs_restored` (Review Focus 5): a source dir with mode `0o555` containing 2 files restores successfully with manifests equal.
  - `empty_file_and_empty_dir` (Review Focus 3): both restore. The empty file has size 0, and its `content.unit_count == 0`.
  - `subpath_restore`: `--path b"d"` gives `manifest_dir(dest) == manifest_snapshot(cat, s, b"d")`.
  - `corrupt_unit_aborts`: flip a payload byte in a pack → `Err(Corrupt)`, `dest` does not exist, and the partial dir exists.
  - `preflight_case_collision` (unit test on `check`): entries `Foo.jpg` and `foo.jpg` under the same parent, with `case_insensitive = true` → 1 message. With `false` → none.
  - `preflight_path_too_long` (Review Focus 4): `path_max = 1024` and a 1100-byte path → a message is produced.
  - `preflight_non_utf8_on_strict_fs`: `accepts_non_utf8 = false` and name `b"\xff"` → a message.
  - `hostile_names_roundtrip` (Review Focus 4): the Task 6 hostile names round-trip with manifests equal.
  - `linux_tree_on_macos_collides` (`#[cfg(target_os = "linux")]`, integration): ingest `Foo` and `foo`. Restoring to a dir where a fake `FsProbe { case_insensitive: true, .. }` is injected via `restore::run_with_probe` → `Err(Preflight)` listing the pair, and nothing is written.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Add the CLI test** `restore_roundtrip_cli`: after `init`, `ingest`, and `restore latest out`, `wmk manifest out` equals the ingest's `manifest_hash`.
- [ ] **Step 5:** Run the CI gate. Expected: PASS.
- [ ] **Step 6: Commit** `feat: verified atomic restore with filesystem preflight`.

---

### Task 11: `verify` and `diff`

**Files:**
- Create: `crates/core/src/verify.rs`, `crates/core/src/diff.rs`, `crates/cli/src/cmd/{verify.rs, diff.rs}`
- Test: `crates/core/tests/verify_diff.rs`

**Interfaces:**
- Produces:
  - `verify::VerifyReport { snapshots: u64, units_checked: u64, packs_checked: u64, problems: Vec<String> }`, where an empty `problems` means OK.
  - `verify::run(repo, snapshot: Option<Id>, quick: bool) -> Result<VerifyReport>`
    - **quick:** for every unit referenced (through `content_unit` and `record_unit`) by the selected complete snapshots, the pack file exists, and `read_record(offset)` gives a header whose hash and lengths match the unit row. Also, `manifest_snapshot(...) == snapshot.manifest_hash`.
    - **full:** everything in quick, plus `read_unit` (decompress and hash) for each unit, plus `scan().blake3_ok == Some(true)` for each sealed pack those units live in.
  - `diff::Difference`, an enum with variants `ContentDiffers, ModeDiffers { snap: u32, dir: u32 }, MtimeDiffers { snap: i64, dir: i64 }, KindDiffers, LinkTargetDiffers, Missing, Extra, ExpectedMissing(EntryStatus)`. Each variant carries `path: Vec<u8>`.
  - `diff::run(repo, snapshot: Id, dir: &Path, opts: DiffOptions { path: Vec<u8>, mtime_precision_ns: u64 }) -> Result<Vec<Difference>>`. It loads the snapshot subtree into a `HashMap<Vec<u8>, EntryRow>`, walks `dir`, and compares on the §4 fidelity rules (mode `& 0o1777`, mtime floored). Entries with status `ScreenedOut`, `ScreenError`, or `Purged` that are present in `dir` are not reported as `Extra`; they are reported as `ExpectedMissing` only when also absent from `dir`.
  - CLI:
    - `wmk verify [<snapshot-ref>] [--quick]`: exit 2 if `problems` is non-empty.
    - `wmk diff <snapshot-ref> <dir> [--path] [--mtime-precision]`: prints one line per difference (`~content path`, `~mode path 644→600`, `-missing path`, `+extra path`, `=expected-missing path (screened_out)`). Exit 2 if any difference other than `ExpectedMissing` is found.

- [ ] **Step 1: Write failing tests:**
  - `verify_clean_repo`: both quick and full give empty `problems`.
  - `verify_detects_flipped_byte`: full reports a problem naming the pack. Quick does not, because the header is intact (assert this explicitly, to document what quick covers).
  - `verify_detects_missing_pack`: delete a pack file → quick reports it.
  - `diff_clean_after_restore`: diff against the restored dir → empty.
  - `diff_reports_each_kind`: edit content, chmod, touch an mtime, delete one file, and add one file in a restored copy → exactly those 5 differences, sorted by path.
  - `diff_precision`: in a restore where mtimes are floored to the second, diff with `mtime_precision_ns = 1e9` is empty, and with 1 it reports `MtimeDiffers`.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement**, including the CLI exit-code mapping (`exit_code` gains code 2 for a `VerifyFailed` or `DiffFound` sentinel error).
- [ ] **Step 4:** Run the CI gate. Expected: PASS.
- [ ] **Step 5: Commit** `feat: verify and diff`.

---

### Task 12: Inspection commands (`ls`, `show`, `stats`, `resolve`, `find`, `cat`)

**Files:**
- Create: `crates/cli/src/cmd/{ls.rs, show.rs, stats.rs, resolve.rs, find.rs, cat.rs}`
- Modify: `crates/core/src/catalog/mod.rs` (add `stats() -> Result<RepoStats>`)
- Test: `crates/cli/tests/cli.rs`

**Interfaces:**
- Produces:
  - `catalog::RepoStats { snapshots: u64, entries: u64, contents: u64, units: u64, packs: u64, logical_bytes: u64 /* sum of file sizes over all entries */, unique_bytes: u64 /* sum of content.size */, stored_bytes: u64 /* sum of unit.stored_size */ }`
  - Output contract, as JSON keys:
    - `ls`: an array of `{id, status, source_root, started_at, completed_at, entry_count, file_bytes, manifest_hash, note}`. `source_root` is shown escaped if it is not UTF-8.
    - `show <snap> [path]`: the children of `path`, as `{id, name, kind, size, mode, mtime_ns, content_hash, status}`.
    - `stats`: the `RepoStats` fields, plus `dedup_ratio = logical/unique` and `storage_ratio = stored/logical`.
    - `resolve <uuid>`: `{entry_id, snapshot_id, path, kind, content_hash, status}`.
    - `find --content <hex>`: an array of `{snapshot_id, entry_id, path}`.
    - `cat <uuid|hex>`: raw bytes to stdout (no JSON), streamed via `UnitReader::write_content` and verified. An entry that is not a file, or whose status is withheld, exits 1.
  - **Escaping rule** for non-UTF-8 path bytes in human and JSON output: `\xHH` for each invalid byte. Valid UTF-8 is printed as-is. Implement it once in `output.rs` as `fn display_bytes(&[u8]) -> String`.

- [ ] **Step 1: Write failing CLI tests:**
  - `ls_shows_snapshot`: after one ingest, `wmk ls --json` returns 1 element with status `complete`.
  - `show_root_children`: lists the expected names in canonical order.
  - `stats_dedup`: ingesting 3 identical 100 KiB files gives `dedup_ratio` within 0.01 of 3.0.
  - `resolve_and_cat`: the entry id of a known file is obtained via `show --json`. `resolve` returns its path, and the `cat` output bytes equal the source file.
  - `find_by_content`: the hash from `show` → `find` returns all 3 duplicate paths.
  - `display_bytes_escapes` (unit test in `output.rs`): `b"a\xffb"` → `"a\\xffb"`.
- [ ] **Step 2:** Run `cargo test -p drmanhattan --test cli`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run the CI gate. Expected: PASS.
- [ ] **Step 5: Commit** `feat: inspection commands`.

---

### Task 13: Round-trip property tests, fixture repo, and CI

**Files:**
- Create: `crates/core/tests/roundtrip_prop.rs`, `crates/core/tests/fixture_repo.rs`, `tests/fixtures/repo-v1/` (generated), `tests/fixtures/repo-v1.expected.toml`, `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the property test `roundtrip_any_tree`** (`proptest`, 64 cases by default, with `PROPTEST_CASES` respected).
  - **Strategy `TreeSpec`:** depth ≤ 4, ≤ 40 entries.
    - *Names:* `[a-zA-Z0-9._ -]{1,20}`, plus NFC and NFD `é` variants (on both OSes), plus on Linux only raw bytes from `0x80..=0xFF` and `\n`. Names never equal `.` or `..`, contain no `/` or NUL, and are case-unique per parent on macOS.
    - *Kinds:* file, dir, and symlink (target: a random relative string, often dangling).
    - *File sizes:* drawn from `{0, 1, 2047, 2048, 2049, 65535, 65536, 65537, 262143, 262144, 262145, 4194304, 4194305}` ∪ `0..300_000`. The large sizes are capped to at most 2 per tree.
    - *Modes:* `0o400..=0o777` plus an optional `0o1000`. Dirs always include `0o700`, so the test can clean up.
    - *mtimes:* from `-2_000_000_000_000_000_000..=4_000_000_000_000_000_000`, including 0.
    - *Hard links:* 0–2 pairs.
  - **Assertions:**
    - (a) `ingest(D).manifest_hash == manifest_dir(D).core`
    - (b) `manifest_dir(restore(S)).core == manifest_hash`
    - (c) `diff(S, restore(S))` is empty
    - (d) a second ingest has `new_units == 0`, and its `manifest_hash` is equal
    - (e) `restore` with `hardlinks` gives equal `ext` (xattrs omitted from the generator)
  - Skip the mtime parts of the assertions on filesystems the probe reports as coarser than 1 ns, using the probed precision.
- [ ] **Step 2:** Run `cargo test -p drmanhattan-core --test roundtrip_prop --release`. Expected: PASS. On failure, reproduce the shrunk case as a named regression test in the owning task's test file, fix it, and rerun.
- [ ] **Step 3: Write `fixture_repo.rs`.**
  - An `#[ignore]` test `generate_fixture` builds a deterministic tree with:
    - 30 files: html with shared templates, 2 tiny PNG-magic files, 1 empty file, and one exactly 2048 bytes
    - nested dirs, 2 symlinks, and 1 hard-link pair
    - fixed modes and mtimes

    It ingests the tree into `tests/fixtures/repo-v1/` and writes `repo-v1.expected.toml` with `manifest_hash`, `ext_manifest_hash`, and the per-file BLAKE3 values. The total size must stay under 1 MiB.
  - Test `fixture_restores_exactly`: copies the fixture repo to a tempdir, then:
    - `verify` (full) is clean
    - `restore` with `hardlinks` gives a `manifest_dir` matching both expected hashes
    - every file's BLAKE3 matches the expected values
- [ ] **Step 4:** Run `cargo test -p drmanhattan-core --test fixture_repo -- --ignored generate_fixture`, then `cargo test -p drmanhattan-core --test fixture_repo`. Expected: PASS. Commit the generated fixture: from now on it is a format-stability gate, and any later change that breaks it is a format break.
- [ ] **Step 5: Write `.github/workflows/ci.yml`.**
  - Matrix: `ubuntu-latest` and `macos-latest`.
  - Steps: checkout, `dtolnay/rust-toolchain@stable` with components `rustfmt` and `clippy`, `Swatinem/rust-cache@v2`, then the CI gate with `--all-features`, then `cargo test --release -p drmanhattan-core --test roundtrip_prop` with `PROPTEST_CASES=256`.
- [ ] **Step 6:** Run the full CI gate locally, plus `roundtrip_prop` with `PROPTEST_CASES=256`. Expected: PASS.
- [ ] **Step 7: Commit** `test: round-trip property tests, format fixture, CI`.

---

## Done criteria for Plan 1

- `wmk init`, `ingest`, `restore`, `manifest`, `verify`, `diff`, `ls`, `show`, `stats`, `resolve`, `find`, and `cat` all work end-to-end on macOS and Linux.
- The core guarantee holds under property testing.
- The committed fixture repo restores exactly.
- CI is green on both platforms.
