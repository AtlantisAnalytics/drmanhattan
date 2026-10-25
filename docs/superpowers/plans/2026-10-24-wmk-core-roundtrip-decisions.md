# Plan 1 (core round trip): decisions and open items

This file records the decisions the implementation controller made while executing
`2026-10-24-wmk-core-roundtrip.md`, along with the findings deferred to later work.
Each decision has the form *what was decided, why, and what it costs if it is wrong*.
The spec (`../specs/2026-10-24-wmk-v1-design.md`) was updated wherever a decision changed the
documented behaviour.

## Decisions

### Setup and process
- **Branch.** Work on `feat/wmk-core-roundtrip` in place rather than in a separate worktree. *Why:* the repo was fresh. *Cost if wrong:* none.
- **Toolchain.** Use rustup stable rather than Homebrew rustc 1.79; the toolchain is now pinned to 1.99.1, with `rust-version` 1.87. *Why:* the libraries need a newer compiler, and a pinned toolchain means a new clippy lint can't break CI unannounced. *Cost if wrong:* MSRV 1.87 is checked by clippy only, not by an actual 1.87 build.
- **Failpoint tests.** Serialize them on a file-local mutex with `FailScenario`, in their own test binary (`tests/ingest_failpoints.rs`). *Why:* the failpoint registry is process-global. *Cost if wrong:* flaky tests, which would be visible.
- **Fixture commit.** Generate the fixture with `wal_checkpoint(TRUNCATE)` and commit no `-wal`, `-shm`, lock or `tmp` files. *Cost if wrong:* the fixture test fails, which is visible.

### Config and chunking
- **Config validation.** `Config::validate` runs on every load. It checks the profile bounds against fastcdc's limits, `format_version`, `compression.algo` and `compression.level`, and `packs.max_size` (1 MiB to 4 GiB). *Why:* `wmk.toml` is user-editable, and fastcdc panics on bad bounds.

### Pack format (spec §6.2)
- **End-anchored footer.** The footer is `count | index | pack_blake3 | footer_offset u64 | "WMKPEND\0"`. *Why:* the old spec gave no way to locate the footer, and `recover` must never truncate intact sealed data. *Cost if wrong:* 8 bytes per pack.
- **Sealed scan.** `records` is the union of sequentially validated records and index-validated records. `damaged` holds only offsets that contain no valid record. `index_ok` reports whether the index agrees. *Why:* `pack_blake3` doesn't cover the index, so the index must not override verified bytes.
- **Unit size bounds.** `MAX_UNIT_LEN` is 16 MiB. A header with `None` compression and stored ≠ raw, or with `Zstd` and stored ≥ raw, is Corrupt. *Cost if wrong:* a future codec needs a format bump, which it would need anyway.
- **Pack creation.** Packs are created with `create_new`, and both the fanout dir and `packs/` are fsynced.

### Catalog (spec §6.1)
- **Table count.** There are 12 tables. The plan's "14" was a typo.
- **Foreign keys.** FKs are immediate, not deferred, and callers send ops in FK order. *Why:* this catches pipeline-ordering bugs. *Cost if wrong:* the ingest pipeline must keep the order, which it does by design.
- **Closed format vocabulary (Ruling F).** `content.format` uses a closed list of values. Adding one means bumping both the catalog and the record versions. Record decoding is strict (`deny_unknown_fields`). *Why:* old binaries should refuse a newer repo rather than misread it. *Cost if wrong:* adding a new sniffed format forces a repo format bump. The final reviewer called this "questionable, not wrong"; it is kept.

### Walking and manifest (spec §4, §6.4)
- **Walker.** The walk is iterative, the root must be a directory, and the cross-fs check applies to directories only. *Cost if wrong:* a bind-mounted single file could cross filesystems silently.
- **Canonical order.** Entries are ordered depth-first pre-order, with each directory's children sorted bytewise by name. *Why:* this order can be produced while streaming.
- **Hard links in the ext hash.** A hard-link group counts only when at least 2 of its members are inside the hashed tree. *Why:* otherwise a file whose other links are outside the tree (or outside a `--path` subtree) can never round-trip.
- **Xattr namespace policy.**
  - Linux records `user.*` only; `security.*`, `system.*` (ACLs) and `trusted.*` are ignored.
  - macOS ignores `com.apple.provenance` and `com.apple.macl`, and keeps `com.apple.quarantine`.

  *Why:* OS-managed attributes can't round-trip for an unprivileged user. *Cost if wrong:* anyone who needs SELinux labels has to wait for an opt-in flag.
- **Fallible hashing.** `ManifestHasher::push` returns a `Result`: lengths are checked, and an entry missing its target or content is Corrupt. *Why:* a frozen format must never produce silent colliding encodings.
- **Precision floor.** An mtime precision of 0 is treated as 1. Flooring saturates instead of overflowing.
- **`wmk manifest --cross-fs`** is kept. *Why:* it is needed to match a source that was ingested with `--cross-fs`.

### Ingest (spec §7)
- **R1: hashes after commit.** Manifest hashes are computed after the catalog commit, by `manifest_snapshot`, rather than by a streaming sequencer. *Why:* ingest and verify then share one code path. *Cost if wrong:* one extra catalog traversal per ingest.
- **Ordering.** Storage parks entries whose content is still in flight. Catalog ops are buffered and forwarded only at a barrier, in the order sync, then the buffered ops, then `UpsertPack(len)`, then `Commit`. *Why:* the writer commits when its channel closes. *Cost if wrong:* at most about 10k ops held in memory.
- **Packs.** Packs are created lazily and sealed at `max_size` and at finish.
- **Worker file safety.** Workers open files with `O_NOFOLLOW|O_NONBLOCK` and require a regular file with the same dev/ino as the walk saw. `manifest_dir` and `diff` share this check. *Why:* a FIFO swapped in after the walk must not cause a hang.
- **Stability.**
  - `entry.size` is the number of bytes actually read, and the mtime comes from the post-read fstat.
  - A changed file gets one retry.
  - A hard-link group whose members end up with divergent content is treated as unstable.
- **Snapshot record.** It is a CBOR sequence: Header, Entry\*, Content{full row + units}\*, Xattr\*, then optional Provenance\*/ScreenHit\* sections that are reserved for Plan 3. It is streamed to `tmp/`, chunked with the binary profile, and indexed by a single `snapshot_record` unit. *Why:* this scales to 100M entries, and `recover` must be able to rebuild content rows from packs alone.
- **Finish sequence.** Flush (acked after commit), compute hashes, write the record, flush again, seal, join, then run the final upsert under `synchronous=FULL`. *Why:* a successful ingest must survive power loss.
- **Fault testing.** The `failpoints` feature is wired up, and a storage failure is injected mid-run to test fsync-before-commit. The barrier thresholds are configurable for tests.
- **Fixture identity.** A test-only `host_override`/`source_root_override` (feature `test-seams`) keeps local machine details out of the committed fixture. The two fixture commits were squashed, so no branch commit contains the hostname.

### Restore (spec §9)
- **Path validation.** A single validated traversal (`snaptree`) is the only way any code walks catalog entries. It checks that `path == parent_path/name`, rejects bad names (`.`, `..`, `/`, NUL), requires a single root, and detects cycles. *Why:* a crafted catalog could otherwise write outside the destination.
- **Preflight probe.** The probe detects case and normalization insensitivity. Collision keys are NFD+lowercase or NFD, depending on what the probe finds. A path is refused when its length is ≥ `PATH_MAX`. *Why:* APFS treats NFC and NFD spellings as the same name even on case-sensitive volumes.
- **Partial directory.** An existing partial dir is an error and is never deleted automatically. Every write is exclusive, and the final rename refuses to replace an existing path.
- **Ordering.** Xattrs are set before modes. The restored root's mode and mtime are applied after the rename. Hard links are created only for groups with at least 2 members in the restored set.
- **Pack handles.** Open pack handles are bounded by an LRU of 64 (`Arc<PackReader>`). `verify` shares it.
- **`UnitReader::new(repo, cache_bytes)`** takes no catalog parameter.

### Verify, diff and CLI (spec §9, §11)
- **Verify scope.** Verify covers content units, the record index, and the record chunk units. Full mode also streams each content's units to check the content hash, and checks that each unit's kind matches. Corruption is reported as problems, not errors (exit 2).
- **Diff scope.** `diff` compares core fidelity only. Use `wmk manifest --ext` for xattr and hard-link parity. `diff` has `--cross-fs`.
- **Output escaping.** Every user-visible path goes through one renderer, which escapes C0/C1 control bytes, DEL, `\` and invalid UTF-8. *Why:* this prevents terminal-escape injection from hostile names.
- **`cat` argument.** A 64-hex argument is a content hash; anything that parses as an Id is an entry. On a hash mismatch, `cat` writes to stderr and exits 1. Bytes already streamed can't be recalled, and this is documented.

## Open items

### Residual from the final review (not yet fixed)
- **`restore --xattrs` doesn't filter by the namespace policy.** On Linux, running as root, a crafted catalog could set `security.capability` or `trusted.*`. This predates the plan and is roughly a 3-line fix: refuse or skip names that `xattr_recorded` would reject.
- **Unreachable rows are skipped silently.** A full traversal doesn't report catalog rows it can't reach. `verify` catches this through the manifest mismatch.
- **Cosmetic issues:**
  - `verify` prints `Some("…")`.
  - Config error text is not escaped.
  - The README example omits `wmk init`.

### For Plan 2 (`recover` and fault injection)
- **Ambiguous seal state.** Treat "sealed with `blake3_ok = false`" as ambiguous: a crafted tail ending in `WMKPEND` can mimic a seal.
- **Payload verification.** A sealed scan validates headers only, so `recover` must verify payload hashes itself.
- **Bad index entries.** An index entry that points inside another record adds an overlapping record. An index entry that hits an unsupported kind makes the scan fail with `Err`. Consider treating the latter as damaged.
- **Stale temp files.** Sweep stale `tmp/record-*.cbor` files left behind by a crash.
- **Streaming record reads.** `read_record` holds the whole record in memory and checks its hash after parsing. `recover` needs a streaming visitor that defers commit until the hash has been verified.
- **Conflicting status.** Decide which wins when the catalog says Failed but the record says Complete (a failure during the final upsert).
- **Pack length.** `UnitReader` fixes a pack's length at open time.
- **Pack id check.** Cross-check the header `pack_id` against the filename.
- **Partial create.** A partial pack file can be left behind if create fails mid-header.

### For Plan 3 (screening and large files)
- **Memory.** Whole files are buffered in memory, about 5× the worker count in flight. Large-file streaming is needed before scraping multi-GB media.
- **Screener errors.** A non-Pass screener verdict currently maps to `Error::Corrupt`; give it a proper variant.

### For Plan 4 (labels, Python contract, benchmarks)
- **Read-only open.** Apple's system `sqlite3` couldn't open the fixture catalog with `mode=ro` when no `-shm` file was present, and Python may hit the same problem.
- **Progress output.** Spec §11 says progress goes to stderr, but nothing implements it yet.

### Other deferred polish (no data at risk)
- **Restore and preflight:**
  - `manifest` and `diff` can't take a single-file root (needed after `restore --path <file>`).
  - There is no xattr support probe in preflight.
  - The case-fold uses `to_lowercase`, which has final-sigma and ligature quirks.
  - The normalization probe checks NFC only.
  - The sticky bit on regular files fails late on macOS.
  - There is a window after the rename before the root's metadata is applied.
- **Tests and cleanup:**
  - Assorted test-coverage gaps, listed per task in the review history.
  - Duplicated helpers in restore.
  - `ingest/mod.rs` could split the walker into its own file.
  - `fail` is a non-optional dependency.
- **Behaviour notes:**
  - A content's format, and therefore its chunk profile, can depend on the name of the first file seen with that content. Content dedup is unaffected.
  - `Config::save` isn't atomic.
- **CI:** CI uses `actions/checkout@v4`, which runs on Node 20.
