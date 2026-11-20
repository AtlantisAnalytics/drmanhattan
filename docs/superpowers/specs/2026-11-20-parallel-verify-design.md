# Parallel full verify: design

Status: approved 2026-11-20.

## Why

A full `wmk verify` of a 1.58 TB repository took 63.8 minutes on a dual Xeon
test server. One core ran at 97% while the NVMe drive, which reads at about
3 GB/s, sat mostly idle. Verify is CPU-bound on a single thread, so it scales
badly as repositories grow. The goal is the fastest full verify that keeps
every check it makes today.

## Goals

- A full verify that is limited by the disk, not by one core.
- The same guarantee: every unit decompressed and hashed, every content's
  units hashed in order against the content hash, and every touched sealed
  pack's whole-pack BLAKE3 checked.
- The same report for any worker count: the same problems, in the same order,
  and the same counts.
- Bounded memory, independent of repository size.

## Non-goals

- Reading the data only once (see the later option below).
- Changing `--quick`. It already reads only record headers (commit 26d4151).
- Changing `restore` or `ingest`.

## Anti-goals

- Any check removed or weakened to gain speed. Dropping the whole-pack hash,
  for example, would leave pack bytes outside units unchecked.
- Reports that depend on thread scheduling.

## Approaches considered

1. **Parallelize the content and pack passes (chosen).** Content checks run
   across worker threads in bounded batches, and whole-pack hashes run across
   threads. The data is still read twice from disk, but at disk speed.
   Expected on the test server: 64 minutes down to about 20-25, to be
   measured.
2. **One pass per pack (later option).** Read each pack once and check its
   BLAKE3 and every unit in it from that one read. A content's units can sit
   in any packs and in any order, so checking content hashes needs units
   buffered or re-read. This halves disk reads at most, for much more
   complexity. The design must leave room for it: keep the per-content and
   per-pack checks as separate functions, with their results merged in a
   fixed order, so a per-pack pass could replace both later.
3. **Drop the whole-pack hash.** Rejected: it weakens the guarantee.

## Design

- **Worker count.** Add `workers` to verify's options. 0 means one per CPU,
  as `ingest --workers` does. The CLI gains `wmk verify --workers N`, and
  `--workers 1` suits spinning disks. Keep `verify::run(repo, snapshot,
  quick)` working for existing callers, at the default worker count.
- **Header pass.** Unchanged and serial at first: it finds the distinct units
  each snapshot references, checks every unit's catalog row against its
  record header, and records which units are bad. If measurement shows it
  takes more than about 5% of a full verify, parallelize it too, keeping its
  report order.
- **Content pass (full only).** A producer walks the catalog in order and
  sends each content's unit rows as a numbered job to N workers over a
  bounded channel. Workers never touch the catalog, because the rows travel
  with the job. They share one `UnitReader` and stream each content's units
  through BLAKE3 without holding the whole content. A merger emits the
  results in job order. A fixed reorder window of 65,536 jobs bounds the
  results waiting to be merged. Report a unit's read failure only once, for the
  first content in catalog order that hits it. Skip contents whose units the
  header pass already reported.
- **Snapshot record checks.** Unchanged and serial. They are small.
- **Pack pass (full only).** Hash the touched packs in parallel and report
  them in sorted pack-id order, as today.
- **Threads.** Use scoped std threads (`std::thread::scope`) and crossbeam
  channels, `workers` threads per pass, not a rayon pool: a producer walks
  the snapshots and checks headers serially, the workers check contents, and
  a merger restores catalog order through a fixed reorder window.

## Testing

- The existing verify tests pass unchanged.
- New: on a repository with corruption of several kinds (a flipped payload
  byte, a chunk shared by two contents, and a bad pack hash), 1 worker and
  4 workers give identical reports.
- The read-count tests from commit 26d4151 still pass: full verify reads
  each chunk payload once.
- Measured on the test server before and after: wall time, CPU, and disk
  read throughput for a full verify of the 1.58 TB repository.

## Left to the builder

The batch size and how batches are formed; whether the `UnitReader` cache
helps the content pass or should be bypassed there; and how the worker
count is threaded through the API.

## Results

Measured on 2026-11-20 on the test server (2 x Intel Xeon Gold 5215,
repository on an NVMe drive on a PCIe 3.0 x4 link), on a 1.58 TB
photo-archive repository (2 snapshots, 1,309,171 units, 5,897 packs), with
the release build. The full run used `numactl --cpunodebind=0`; the quick
run was plain. Both runs reported 0 problems.

| Run | Wall | User CPU | Sys CPU | Max RSS | Disk read |
| --- | --- | --- | --- | --- | --- |
| Baseline full verify (2026-11-17, 1 snapshot, before the change) | 3,826.63 s | 1,523.12 s | 2,158.89 s | 246,276 KB | about 3.3 TB (estimated) |
| Parallel full verify, default workers | 1,235.14 s (20.6 min) | 2,299.68 s | 3,372.32 s | 977,128 KB | about 3.34 TB |
| `verify --quick` | 109.96 s | 14.91 s | 13.68 s | 114,084 KB | about 5.7 GB |

- **Sources.** Baseline wall, user, sys and RSS are from the 2026-11-17 run
  log of that verify. Its read volume was not logged: it is estimated from a
  `/proc/<pid>/io` sample of 3.21 TB taken about 100 s before the run ended.
  The 2026-11-19 ingest log of the second snapshot says "6 new units,
  2580627 new bytes stored". For the new runs,
  disk read is the last `read_bytes` sample from `/proc/<pid>/io`, taken
  within a few seconds of the process exiting, so it and the throughput
  below are approximate.
- **Speedup:** 3.1x (63.8 min down to 20.6 min). The repository now has a
  second snapshot; the two verify outputs report 1,309,131 units and
  5,896 packs (baseline) against 1,309,171 units and 5,897 packs (now), so
  the second snapshot added 40 units and the stored data is essentially
  unchanged.
- **Disk throughput:** about 3.34 TB / 1,235 s = about 2.7 GB/s on average.
  The nominal limit of a PCIe 3.0 x4 link is about 3.9 GB/s; the drive's
  real limit was not measured here, and this run was not shown to be at it.
- **Worker count:** default (0), which is 20 here: `numactl
  --cpunodebind=0 nproc` reports 20 on the test server, and the process
  showed 21 to 22 threads (the workers plus the merger and the main thread,
  which is the producer: 20 + 2 = 22; the pack pass is 20 workers + main =
  21).
- **CPU use:** ten samples, one every two minutes. The process used 327%
  to 718% of one core (top), about 3 to 7 of the 20 available CPUs.
  Whole-machine idle was 52.5% to 82.5%. So the workers did not keep the
  CPUs busy, and sys time (3,372 s) exceeded user time (2,300 s). In the
  first six samples (to about 10 minutes) system iowait was 24.0% in the
  first and 2.9% to 4.2% in the other five; in the last four it was 30.8%
  to 34.3%. The raw sample lines are retained in the measurement log. The data does not
  say what limits the run: idle workers are consistent with contention on
  the shared 64-handle pack cache or with I/O waits, but neither was
  isolated.
- **Memory:** max RSS rose from 246 MB to 977 MB, a rise the
  reorder window (about 10 MB) cannot explain. The cause of the rise was not
  measured; per-worker decode buffers, the 64 MB unit cache and per-thread
  allocator arenas are candidates. Memory is bounded by worker count, window
  and cache, but the walk's sets of seen, hashed, bad and touched units
  still grow with unit count.
- **Header pass rule:** quick verify (the serial header pass) took 109.96 s, 8.9% of the full wall time, which is over the 5%
  threshold. Follow-up: parallelize the header pass, keeping its report
  order.
