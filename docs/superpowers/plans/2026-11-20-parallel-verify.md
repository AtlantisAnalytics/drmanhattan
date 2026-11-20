# Parallel Full Verify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a full `wmk verify` run at disk speed across worker threads, with every check kept and a report that does not depend on the worker count.

**Architecture:** `verify::run_with` runs a three-stage pipeline inside `std::thread::scope`. One producer (the calling thread) walks snapshots in today's order and checks headers. N workers decompress and hash contents. One merger restores the producer's order and removes duplicate reports. The whole-pack BLAKE3 pass then runs on the same number of workers, and its results are reported in sorted pack-id order. The per-content and per-pack checks are pure functions, so a future one-pass-per-pack mode can reuse them.

**Tech Stack:** Rust 1.99.1 (pinned), `crossbeam-channel` and scoped `std::thread`, as ingest uses them, plus the existing `UnitReader`, `PackReader` and `pack::scan`.

**Spec:** `docs/superpowers/specs/2026-11-20-parallel-verify-design.md`

## Global Constraints

- No new dependencies. `crossbeam-channel` and `rayon` are already in `crates/core/Cargo.toml`.
- CI must pass: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-features`.
- On the user's Mac, prefix every cargo command with `export PATH="$HOME/.cargo/bin:$PATH";`. `sed` is GNU sed, so don't use heredocs. Write files with the editor tools and commit with `git commit -F <file>`.
- `verify::run(repo, snapshot, quick)` keeps its signature and behaviour, at the default worker count.
- Workers: 0 means `std::thread::available_parallelism()`, as `IngestOptions.workers` does.
- Problem messages are unchanged, byte for byte.
- Commit messages end with the attribution lines the session gives.
- Spec deviation: the design doc says "a dedicated rayon pool". This plan uses scoped threads with channels instead, matching ingest. That keeps the pool private and sized to `workers` with no global state, which is the intent of that line. Task 3 updates the design doc to match.

## Review Focus

1. **The producer fails partway (catalog error):** `run_with` returns `Err` promptly, with no deadlock, because every thread is joined. Pinned by `catalog_error_returns_err_without_hanging` (Task 1).
2. **More workers than contents** (for example 64 workers on a tiny repo): the report is identical. Pinned by `report_is_identical_for_any_worker_count`, which runs with 0, 1, 2, 4 and 64 workers (Task 1).
3. **A snapshot with no file contents** (only directories and empty files): verifies clean with 4 workers. Pinned by `snapshot_without_contents_verifies_clean` (Task 1).
4. **A missing pack file during the parallel pack pass:** the report is identical across worker counts and names the pack. Pinned by `missing_pack_report_is_identical_for_any_worker_count` (Task 2).
5. **The same content in two snapshots:** checked once, and the unit counts are equal across worker counts. Covered by the two-snapshot fixture in `report_is_identical_for_any_worker_count` (Task 1).

---

### Task 1: Pipelined content pass and `run_with`

**Files:**
- Modify: `crates/core/src/verify.rs`
- Create: `crates/core/tests/verify_parallel.rs`
- Test (unchanged, must pass): `crates/core/tests/verify_diff.rs`, `crates/core/tests/verify_failpoints.rs`, `crates/core/tests/fixture_repo.rs`

**Interfaces:**
- Produces, all public in `drmanhattan_core::verify`:
  - `#[derive(Debug, Clone, Default)] pub struct VerifyOptions { pub quick: bool, pub workers: usize }`
  - `pub fn run_with(repo: &Repo, snapshot: Option<Id>, opts: &VerifyOptions) -> Result<VerifyReport>`
  - `pub fn run(repo: &Repo, snapshot: Option<Id>, quick: bool) -> Result<VerifyReport>`, which now delegates to `run_with` with `workers: 0`.
  - `VerifyReport` already derives `PartialEq, Eq`; the tests compare whole reports with `assert_eq!`.
- Produces (private, in `verify.rs`; Task 2 and a future per-pack mode rely on them):
  - `fn check_content(reader: &UnitReader, repo: &Repo, snap: Id, content: &ContentRow, units: &[UnitRow]) -> ContentOutcome`
  - `enum ContentOutcome { Ok, UnitFailed { unit: Hash32, problem: String }, HashMismatch(String) }`
  - `fn worker_count(n: usize) -> usize` (0 means available parallelism).

- [ ] **Step 1: Write the characterization fixture and test, which pass on current code**

In `verify_parallel.rs`, write a fixture `fn corrupted_repo() -> (TempDir, Repo)`:
- `Repo::init`, then `repo.config.packs.max_size = 1 << 20` before ingesting, so the corruption lands in several packs. This is the same pattern as `tests/ingest.rs:291`.
- Snapshot 1 source:
  - `r.bin` = `noise(1, 6_000_000)`
  - `r_plus.bin` = `r.bin` followed by `noise(2, 1_000_000)`, which shares its leading chunks with `r.bin`
  - `u.bin` = `noise(3, 3_000_000)`
  - `a.txt` = `b"alpha\n"`
  - `empty.txt` = `b""`
  - an empty directory `d/`
- Snapshot 2: the same tree plus `later.bin` = `noise(4, 2_000_000)`, ingested again into the same repo. Use `workers: 2`.
- Guard: assert that the first unit of `r.bin`'s content is also among `r_plus.bin`'s units. Use `Catalog::entry_by_path` and `content_units`, as `verify_diff.rs::content_pack` does.
- Corruptions:
  1. Flip one payload byte of the shared first unit (`offset + RECORD_HEADER_LEN + 10`), as `verify_detects_flipped_byte` does.
  2. Flip one payload byte of `u.bin`'s last unit.
  3. Run `UPDATE unit SET stored_size = stored_size + 1 WHERE hash = ?` for `later.bin`'s first unit.

`noise` is the same generator as in `verify_failpoints.rs`. Copy it, don't share it: test binaries don't share non-`common` code.

Write `fn normalize(repo: &Repo, problems: &[String]) -> Vec<String>`, which replaces the repo's root path with `<repo>`.

Write the test `report_matches_serial_order`: `verify::run(&repo, None, false)`, then assert that `normalize(...)` equals a literal `Vec<&str>` pinned from a run of the current code. Capture it by running the test once with a temporary `println!`, paste the output in, then remove the `println!`. Also pin `snapshots`, `units_checked` and `packs_checked` as literals.

- [ ] **Step 2: Run it. Expect PASS on current code. If it fails, the fixture is nondeterministic: fix the fixture**

Run: `cargo test -p drmanhattan-core --all-features --test verify_parallel report_matches_serial_order`
Expected: `1 passed`.

- [ ] **Step 3: Write the failing tests for the new API**

In `verify_parallel.rs`:

```rust
#[test]
fn report_is_identical_for_any_worker_count() {
    let (_tmp, repo) = corrupted_repo();
    for quick in [false, true] {
        let base = verify::run_with(&repo, None, &VerifyOptions { quick, workers: 1 }).unwrap();
        assert!(!base.problems.is_empty() || quick);
        for workers in [0, 2, 4, 64] {
            let r = verify::run_with(&repo, None, &VerifyOptions { quick, workers }).unwrap();
            assert_eq!(r, base, "quick={quick} workers={workers}");
        }
    }
}

#[test]
fn corruption_in_shared_chunk_is_reported_once() {
    // full, workers 4: exactly one problem contains the shared unit's hex hash;
    // at least one problem contains "BLAKE3 mismatch"; one problem contains
    // u.bin's corrupted unit hash; one problem contains later.bin's unit hash
    // with "disagrees with catalog".
}

#[test]
fn snapshot_without_contents_verifies_clean() {
    // tree: empty.txt (0 bytes) and an empty directory only; ingest; full verify
    // with workers 4 -> problems empty, snapshots == 1.
}

#[test]
fn catalog_error_returns_err_without_hanging() {
    // corrupted_repo(); open the catalog read-write with rusqlite and run
    // `DROP TABLE content_unit`; run_with(full, workers 4) must return Err.
    // The test harness timeout is the hang detector: no sleeps.
}
```

- [ ] **Step 4: Run them. Expect a compile failure: `run_with` and `VerifyOptions` don't exist**

Run: `cargo test -p drmanhattan-core --all-features --test verify_parallel`
Expected: `error[E0425]` / `E0412` for `run_with` and `VerifyOptions`.

- [ ] **Step 5: Implement the pipeline in `crates/core/src/verify.rs`**

How it works (this is the one algorithm the tests don't determine):

- **Producer.** The calling thread owns `Checker` (catalog, `seen`, `touched`, header-`bad`). It walks exactly today's order: status problems, then for each snapshot the manifest, its contents in `for_each_snapshot_content` order, and its record checks.
  - `Checker::problem` now appends to `pending: Vec<String>` instead of `problems`.
  - After each content's header checks, it sends `Job { seq, before: take(pending), work }`. `work` is `Some((content, Vec<UnitRow>))` only if all of these hold: it's a full verify, `hashed.insert(content.hash)` is new, no unit is in header-`bad`, and every unit row loaded. Otherwise `work` is `None`.
  - `unit()` returns `Option<UnitRow>` (the catalog row whenever it loaded, even for an already-seen unit), so the producer builds `Vec<UnitRow>` without a second lookup path.
  - The snapshot-record checks stay in the producer: they're small, and they still read via the shared `UnitReader`. Their problems go out as a `Job` with `work: None`.
  - At the end, any remaining `pending` goes out the same way, and then the job sender is dropped.
- **Workers** (`worker_count(opts.workers)` threads, `crossbeam_channel::bounded(4 * workers)` for jobs): each runs `check_content` on `work` and sends `Done { seq, before, outcome }` on an unbounded results channel.
  - `check_content` streams the units through one `blake3::Hasher` via `UnitReader::read_unit`. It stops at the first unit error (`UnitFailed`). After all units, it compares against `content.hash` (`HashMismatch`). The messages are exactly today's (`"snapshot {snap}: unit {h}: {e} ({loc})"` and `"snapshot {snap}: content {}: its {} unit(s) hash to {got}"`).
- **Merger** (its own scoped thread): keeps a `BTreeMap<u64, Done>` and emits in `seq` order. It appends `before`, then the outcome:
  - `UnitFailed`: report it only if `read_bad.insert(unit)` is new. Otherwise drop it silently. This matches today's `bad.contains` early return.
  - `HashMismatch`: report it.
  - The merger returns the ordered `Vec<String>`.
- **Errors.** If the producer hits `Err`, it drops the job sender, and `thread::scope` joins the workers and the merger before the `Err` is returned. Worker panics propagate out of `scope`.
- **Pack pass.** Stays serial in this task (Task 2 parallelizes it), and its problems are appended after the merged list, as today.
- **`run` delegates:** `run(r, s, quick)` calls `run_with(r, s, &VerifyOptions { quick, workers: 0 })`.

- [ ] **Step 6: Run the new tests and the existing verify tests. Expect all to pass**

Run: `cargo test -p drmanhattan-core --all-features --test verify_parallel --test verify_diff --test verify_failpoints --test fixture_repo`
Expected: all pass, including `report_matches_serial_order` unchanged.

- [ ] **Step 7: Run the full CI set**

Run: `cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`
Expected: fmt clean, `No issues found`, all tests pass, with only the 2 pre-existing ignored tests.

- [ ] **Step 8: Commit** with the message `feat(verify): check contents on parallel workers`.

---

### Task 2: Parallel whole-pack hash pass

**Files:**
- Modify: `crates/core/src/verify.rs`
- Test: `crates/core/tests/verify_parallel.rs`

**Interfaces:**
- Consumes: `VerifyOptions`, `worker_count` (Task 1).
- Produces (private): `fn check_pack(path: &Path) -> Option<String>`, which returns today's message for `pack::scan` errors or a sealed BLAKE3 mismatch, and `None` for a missing file (already reported per unit). Also `fn check_packs(repo: &Repo, touched: &[Id], workers: usize) -> Vec<String>`.

- [ ] **Step 1: Write the failing-order guard test**

`missing_pack_report_is_identical_for_any_worker_count`: use `corrupted_repo()`, then delete the pack holding `u.bin`'s first unit. For quick and for full, compare `run_with` at 1 and at 4 workers: the reports must be equal, and some problem must contain the pack's file name.

This test passes before and after this task. It guards the refactor; it isn't a red test, because parallelism in the pack pass is invisible except as speed. Task 3 measures the speed.

- [ ] **Step 2: Run it. Expect PASS**

Run: `cargo test -p drmanhattan-core --all-features --test verify_parallel missing_pack`

- [ ] **Step 3: Implement `check_packs`**

Use scoped threads (`worker_count` of them) taking indices from an `AtomicUsize` over the sorted `touched` ids. Write results into a `Vec<Option<String>>` by index, then flatten in index order. Replace the serial loop in `run_with` with it. Quick verify still skips the pack pass.

- [ ] **Step 4: Run the full CI set** (the same command as Task 1 Step 7). Expected: all green.

- [ ] **Step 5: Commit** with the message `feat(verify): hash packs on parallel workers`.

---

### Task 3: CLI flag, docs, and measurement

**Files:**
- Modify: `crates/cli/src/cmd/verify.rs`, `crates/cli/tests/verify_diff.rs`, `README.md:73`, `docs/superpowers/specs/2026-11-20-parallel-verify-design.md` (the Threads line, plus a Results section)

**Interfaces:**
- Consumes: `verify::run_with`, `VerifyOptions` (Task 1).

- [ ] **Step 1: Write the failing CLI test**

In `crates/cli/tests/verify_diff.rs::verify_and_diff_exit_codes`, add `&["verify", "--workers", "2"]` and `&["verify", "--quick", "--workers", "1"]` to the list of commands expected to exit 0.

- [ ] **Step 2: Run it. Expect FAIL** (clap: unexpected argument `--workers`)

Run: `cargo test -p drmanhattan --all-features --test verify_diff`

- [ ] **Step 3: Add the flag**

In `VerifyArgs`, add this field with the doc comment as shown:

```rust
/// Worker threads for decompressing and hashing (0 = one per CPU; use 1 for spinning disks)
#[arg(long, default_value_t = 0)]
pub workers: usize,
```

Call `verify::run_with(&repo, snapshot, &VerifyOptions { quick: args.quick, workers: args.workers })`. In `README.md:73`, add `--workers N` to the verify row's description.

- [ ] **Step 4: Run the full CI set. Expect all green. Then commit** with the message `feat(cli): add verify --workers`.

- [ ] **Step 5: Measure on the test server (over ssh, using a real repository of 1.58 TB with 2 snapshots)**

- **Ship the branch:** the branch isn't pushed, so `git bundle create <scratch>/verify.bundle main..HEAD`, copy it to the test server with `scp`, then in the server's clone of this repo: `git fetch <bundle> HEAD:verify-parallel && git checkout verify-parallel`. Then `$HOME/.cargo/bin/cargo build --release -p drmanhattan`.
- **Run in `tmux`, logging with `/usr/bin/time -f '%e s wall, %U s user, %S s sys, %M KB max RSS'`:**
  1. `wmk verify --quick`
  2. `numactl --cpunodebind=0 wmk verify` (full, default workers)
- **Also record:** each run's `read_bytes` from `/proc/<pid>/io` at the end. The disk throughput is `read_bytes / wall`.
- **Baseline:** 3,826.63 s for the full verify on 2026-11-17, with one snapshot and the pre-fix code. Note in the results that the repo now has 2 snapshots with essentially the same units.
- **Expected:** full verify roughly 20–25 min, limited by the disk; quick verify a few minutes. Both must report 0 problems.
- **Header pass check:** quick verify's wall time approximates the serial header pass. If it's more than 5% of the full verify's wall time, record "parallelize the header pass" as a follow-up in the design doc's Results, as the design specifies.
- **Afterwards:** put the test server's clone back on `main`.

- [ ] **Step 6: Record the results**

Add a short `## Results` section to the design doc (wall time, CPU, throughput, worker count) and fix its Threads line to say scoped threads. Commit with the message `docs: parallel verify results`.
