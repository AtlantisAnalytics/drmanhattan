# Ingest Memory Budget Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the file data `wmk ingest` holds in memory by a byte budget (default a quarter of physical RAM, `--max-memory` to override).

**Architecture:** A first-come-first-served byte semaphore (`Budget`) shared by the ingest workers. A worker reserves a file's size before reading it; the returned `Permit` travels inside the `NewContent` message to the storage thread, or is dropped by the worker when the content needs no storing. Dropping a permit releases its bytes.

**Tech Stack:** Rust 1.87, std `Mutex` + `Condvar`, `libc` (already a dependency), clap, crossbeam-channel.

**Spec:** `docs/superpowers/specs/2026-11-24-ingest-memory-budget-design.md`

## Global Constraints

- Default budget: a quarter of physical RAM; `sysctlbyname("hw.memsize")` on macOS, `sysconf(_SC_PHYS_PAGES) x sysconf(_SC_PAGESIZE)` on Linux; 4 GiB (4 * 1024^3) if detection fails.
- A request larger than the budget is granted only when nothing else is held; peak held bytes are at most max(budget, largest request), plus re-read growth.
- Requests are granted first come, first served.
- Re-read growth never waits.
- A worker waiting on the budget wakes and stops when the pipeline aborts. No hang, ever.
- Ingest output (snapshot, catalog rows, packs) is unchanged by the budget.
- `--max-memory <SIZE>`: bytes, or digits followed by one of `K`, `M`, `G`, `T` (case-insensitive, powers of 1024). Zero, overflow and anything else are rejected by clap with an error. Help text: it bounds buffered file data, not the whole process.
- No new dependencies. The repo is public: no hostnames, user names or home paths in any committed file.
- Every commit message ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_014XuLEBgd3XjPqDATaEvquq
  ```
- Run cargo with `PATH="$HOME/.cargo/bin:$PATH"`. Gate for every task: `cargo test --all-features`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --check`.

## Review Focus

- An abort (any stage fails) while workers are blocked on the budget: ingest returns `Err` promptly, no hang. Pinned in Task 2.
- A file larger than the budget among smaller files: completes, alone. Pinned in Task 2.
- A file that grows during the read (the unstable retry) with a budget smaller than the file: completes or fails as today, never deadlocks. Pinned in Task 2.
- Re-ingest of already-stored content with a tiny budget: permits released by the worker, run completes. Pinned in Task 2.
- Empty files with a fully used budget: a 0-byte request is granted in turn and does not wait for bytes. Pinned in Task 1.

---

### Task 1: The `Budget` semaphore

**Files:**
- Create: `crates/core/src/ingest/budget.rs` (with `#[cfg(test)] mod tests`)
- Modify: `crates/core/src/ingest/mod.rs` (add `mod budget;`)

**Interfaces:**
- Produces (all `pub(crate)`):
  - `struct Budget` with `fn new(limit: u64) -> Arc<Budget>`, `fn acquire(self: &Arc<Self>, bytes: u64, stop: &dyn Fn() -> bool) -> Option<Permit>` (blocks; `None` once `stop()` is true), `fn peak(&self) -> u64`, `fn held(&self) -> u64`.
  - `struct Permit` (owns an `Arc<Budget>`; `Send`) with `fn grow_to(&mut self, bytes: u64)` (raises the held amount to `bytes` if larger, never waits) and `Drop` that releases its bytes and wakes waiters.
  - `fn default_limit() -> u64` (a quarter of `physical_memory()`, else 4 GiB) and `fn physical_memory() -> Option<u64>`.

- [ ] **Step 1: Write the failing tests** in `budget.rs`:
  - `grants_within_limit`: `new(100)`; acquire 60 then 40 both return at once; `held() == 100`.
  - `waits_until_release`: holding 60 of 100, a thread acquiring 50 is still blocked after 100 ms; dropping the 60 lets it finish; `peak() == 60`.
  - `first_come_first_served`: holding 90 of 100, thread A requests 50 (blocks); after A is queued, thread B requests 5 (would fit) and must not be granted before A. Record grant order in a shared `Vec`; drop the 90; order is `[A, B]`.
  - `oversize_runs_alone`: `new(100)`, holding 10; a request of 250 blocks until the 10 is dropped, then is granted; `peak() == 250`; while it is held, a request of 1 blocks.
  - `zero_byte_request_does_not_wait_for_bytes`: holding 100 of 100 with no queue ahead, `acquire(0)` returns at once.
  - `grow_to_never_waits`: holding 100 of 100, `grow_to(150)` returns at once; `held() == 150`; drop releases all 150.
  - `stop_wakes_a_waiter`: holding 100 of 100, a waiter with a `stop` flag set from another thread after 50 ms returns `None` within 1 s.
  - `physical_memory_is_detected`: `physical_memory()` is `Some(n)` with `n >= 256 MiB`; `default_limit() == n / 4`.
- [ ] **Step 2: Run** `cargo test -p drmanhattan-core --lib budget`. Expected: compile failure (types missing).
- [ ] **Step 3: Implement.** One `Mutex<State>` (`held`, `peak`, `next_ticket`, `serving`) and one `Condvar`. A request takes a ticket and is granted when `ticket == serving` and (`held + bytes <= limit` or `held == 0`); granting bumps `serving` and calls `notify_all`. Waiting uses `wait_timeout` of 50 ms and re-checks `stop()`; a waiter that stops returns `None` without bumping `serving` (every waiter checks `stop`, so the queue drains on abort). `physical_memory` uses `libc::sysctlbyname` under `cfg(target_os = "macos")` and `libc::sysconf` under `cfg(target_os = "linux")`, `None` elsewhere or on failure. Lock poisoning: recover with `into_inner`, as `worker.rs` does.
- [ ] **Step 4: Run** the Step 2 command. Expected: all 8 pass. Then the task gate.
- [ ] **Step 5: Commit** `feat(ingest): add a byte budget semaphore`.

### Task 2: Wire the budget into ingest

**Files:**
- Modify: `crates/core/src/ingest/mod.rs` (`IngestOptions`, `IngestReport`, `pipeline`, `PipelineOutput`, `finish`)
- Modify: `crates/core/src/ingest/worker.rs` (`Worker`, `process`)
- Modify: `crates/core/src/ingest/storage.rs` (`StorageMsg::NewContent`, its test helper `new_content`)
- Create: `crates/core/tests/ingest_budget.rs`
- Modify: `crates/core/tests/ingest_failpoints.rs`

**Interfaces:**
- Consumes: Task 1's `Budget`, `Permit`, `default_limit`.
- Produces:
  - `IngestOptions::max_memory: Option<u64>` — budget in bytes; `None` = `default_limit()`; `Some(0)` makes `ingest::run` return `Err(Error::Config(..))` before any snapshot row is written.
  - `IngestReport::peak_buffered: u64` marked `#[doc(hidden)]` — the budget's `peak()` for the run (test seam; not printed by the CLI).
  - `StorageMsg::NewContent` gains `permit: Option<Permit>` (`None` in storage's own unit tests); storage drops it after `new_content` has appended the units.

- [ ] **Step 1: Write the failing tests** in `tests/ingest_budget.rs` (use `common::TreeBuilder` and a seeded `rand_chacha::ChaCha8Rng` so data is incompressible and distinct):
  - `budget_bounds_buffered_bytes`: 40 files of 16 KiB to 48 KiB plus one 300 KiB file; `workers: 8`, `max_memory: Some(64 * 1024)`. Assert `report.peak_buffered <= 300 * 1024`, `report.peak_buffered > 0`, the snapshot restores byte-identical (`restore::run` + compare), and `verify::run` reports no problems.
  - `output_is_identical_with_and_without_budget`: ingest the same tree into two repos, one with `max_memory: Some(64 * 1024)` and one with `None`; equal `manifest_hash`, `new_units` and `new_bytes_stored`.
  - `reingest_with_tiny_budget_completes`: ingest a 20-file tree twice into one repo with `workers: 8`, `max_memory: Some(1)`; both runs return `Ok`, the second stores no new file content (`new_bytes_stored` of the second run equals that of a re-ingest of the same tree under `None` into a copy of the first repo, since the snapshot record's own units are always new), and `peak_buffered <=` the largest file.
  - `zero_max_memory_is_rejected`: `max_memory: Some(0)` returns `Err(Error::Config(_))`.
- [ ] **Step 2: Add failpoint tests** in `tests/ingest_failpoints.rs`:
  - `abort_while_waiting_on_budget_does_not_hang`: 20 files of 32 KiB, `workers: 8`, `max_memory: Some(32 * 1024)`, `barrier_ops: Some(2)` (so storage syncs early, while workers still queue on the budget), failpoint `ingest::storage_before_sync` (existing) set to `return`. Run ingest on a thread and `recv_timeout(Duration::from_secs(30))` its result: it must be `Err`, not a timeout.
  - `grow_during_read_with_small_budget`: reuse `grow_after_read(path, 1)` on a 64 KiB file with `max_memory: Some(16 * 1024)` and `allow_unstable: false`; the file grows once then is stable, so ingest succeeds as `unstable_once_then_stable_is_ok` does; result within 30 s.
- [ ] **Step 3: Run** `cargo test -p drmanhattan-core --all-features --test ingest_budget --test ingest_failpoints`. Expected: compile failure on `max_memory` / `peak_buffered`.
- [ ] **Step 4: Implement.**
  - `run`: reject `Some(0)` first. `pipeline` builds `Budget::new(opts.max_memory.unwrap_or_else(default_limit))` and hands it to `Worker` (new field `budget: Arc<Budget>`); `peak()` flows out through `PipelineOutput` into `IngestReport::peak_buffered`.
  - `Worker::process`: after `before` is stamped, `acquire(max(entry.size, before.0), &|| self.shared.aborted())`; `None` → return the abort `Halt` the worker already uses. After each `read_pass`, `permit.grow_to(pass.digest.size)`. Move the permit into `NewContent` when emitting it; otherwise let it drop at the end of `process`. Keep the existing `drop(pass)` before the re-read; the permit stays held across the re-read.
  - The abort flag must be set before peers can block forever: `stop` reads `Shared::aborted`, which `halt`/`fail` already set, and `acquire` re-checks it every 50 ms.
- [ ] **Step 5: Run** the Step 3 command, then the task gate. Expected: all pass, including every existing ingest test.
- [ ] **Step 6: Commit** `feat(ingest): bound buffered file data with a memory budget`.

### Task 3: `wmk ingest --max-memory`

**Files:**
- Modify: `crates/cli/src/cmd/ingest.rs`
- Modify: `crates/cli/tests/ingest.rs`
- Modify: `README.md:69` (ingest row)

**Interfaces:**
- Consumes: `IngestOptions::max_memory` from Task 2.
- Produces: `fn parse_size(s: &str) -> Result<u64, String>` in `crates/cli/src/cmd/ingest.rs`, used as the clap `value_parser`.

- [ ] **Step 1: Write the failing tests.** Unit tests on `parse_size`: `"1024" -> 1024`, `"512M" -> 512 * 1024^2`, `"8g" -> 8 * 1024^3`, `"2T" -> 2 * 1024^4`, `"1K" -> 1024`; errors for `"0"`, `"0G"`, `""`, `"G"`, `"1.5G"`, `"-1"`, `"8GB"`, `"99999999999T"` (overflow). CLI test in `tests/ingest.rs`: `ingest_with_max_memory_succeeds` (`--max-memory 1M` on the tree from `ingest_reports_snapshot`, exit 0, same `manifest_hash` as `wmk manifest`) and `ingest_rejects_zero_max_memory` (`--max-memory 0` exits non-zero, stderr mentions `max-memory`).
- [ ] **Step 2: Run** `cargo test -p drmanhattan`. Expected: compile failure.
- [ ] **Step 3: Implement** the flag as `max_memory: Option<u64>` with `#[arg(long, value_name = "SIZE", value_parser = parse_size)]`, doc comment: `Cap on file data buffered in memory, e.g. 8G (default: a quarter of RAM; not a whole-process limit)`. Pass it into `IngestOptions`. README ingest row: `Snapshot a directory into the repository (`--workers N`; `--max-memory SIZE` caps buffered file data, default a quarter of RAM)`.
- [ ] **Step 4: Run** the Step 2 command, then the task gate. Expected: pass.
- [ ] **Step 5: Commit** `feat(cli): add ingest --max-memory`.

### Task 4: Measure and record (controller, not a subagent)

**Files:**
- Modify: `docs/superpowers/specs/2026-11-24-ingest-memory-budget-design.md` (append `## Results`)

- [ ] **Step 1:** In the scratchpad, generate 8 incompressible files of 512 MiB (`head -c 536870912 /dev/urandom`) and 2,000 files of 64 KiB. Build release binaries of `main` (in a temp worktree) and this branch.
- [ ] **Step 2:** For each binary, `wmk init` a fresh repo in the scratchpad and run `/usr/bin/time -l wmk ingest <src> --workers 8`; on the branch, run it with the default budget and with `--max-memory 1G`. Record wall time and maximum resident set size.
- [ ] **Step 3:** Expected: `main` peaks well above 2 GiB; the branch with `--max-memory 1G` peaks near 1 GiB plus fixed overhead; wall times within noise for the default budget. Append the table and raw `time -l` lines' key fields to the spec's `## Results`, with no host or path details. Delete the scratch data.
- [ ] **Step 4: Commit** `docs: ingest memory budget results`.
