# Plan

Working notes for the next session. For the design, see
`docs/superpowers/specs/2026-10-24-wmk-v1-design.md`. For open items from
Plan 1, see `docs/superpowers/plans/2026-10-24-wmk-core-roundtrip-decisions.md`.

## Status (2026-11-18)

The first test on a real archive passed. The archive is about 2 TiB of photos
and video: 157k files, mostly JPEG, with some camera RAW and video. The largest
file is 3.73 GiB. It sits on an exFAT drive, mounted read-only, attached to a
Linux test server. The repository is on a separate NVMe drive and is kept.

| Step | Result |
|---|---|
| Ingest, 4 workers | 47.3 min, 1,970 GiB at 0.69 GiB/s, peak RSS 16.6 GB |
| `verify` (full) | 63.8 min, 0 problems (1,309,131 units, 5,896 packs) |
| Contents | 117,500 distinct for 156,776 files |
| Repository | 1.58 TB for 2.12 TB logical: dedup 1.33, storage ratio 0.746 |
| Restore of 3 folders (141 GB) | byte-identical (`diff -r`); names, sizes, mtimes (ns) and modes identical |

Restore ran at 1.1-1.4 GiB/s cold, with a peak RSS of about 1.1 GB. The
restored folders included 54 GB of GoPro video and the filenames with accented
characters.

When comparing metadata between exFAT and ext4, leave out directory sizes:
exFAT reports 1 MiB for every directory.

## Next

- Decide what to do with the test repository. Keep it to try a second ingest
  and measure incremental dedup, or delete it to free the space.
- Work on the code follow-ups below, starting with the `verify` read pattern.

## How the full ingest was run

1. Mount the source drive read-only. It is deliberately not in fstab.
2. Check that nothing else needs the machine for about 2.5 hours, and that the
   repository drive has at least 2 TB free.
3. Run the ingest in `tmux`, so a dropped SSH connection cannot stop it:

   ```
   WMK_REPO=<repo> numactl --cpunodebind=<source drive's NUMA node> \
     wmk ingest <source> --workers 4
   ```

4. Run `wmk verify` and `wmk stats`, and record the dedup ratio and repo size.
5. Restore a few folders with `wmk restore latest <dest> --path <folder>` and
   compare them byte for byte with the source. A full restore needs as much
   free space as the source, so it may not fit beside the repository.

Expect about 40-90 minutes for the ingest and up to an hour for `verify`.

Stopping an ingest partway means deleting the repository and starting over,
because `recover` does not exist yet. The source is not affected.

exFAT has no symlinks or xattrs and reports synthetic modes, so this test does
not exercise those parts of the fidelity guarantee.

## Measurements

Test server (dual Xeon Gold 5215, 128 GB RAM), cold cache, one socket:

| Sample | Workers | Speed | Peak RSS |
|---|---|---|---|
| 53.5 GiB, mostly large video | 2 | 0.55 GiB/s | 6.2 GB |
| | 4 | 0.62 GiB/s | 10.8 GB |
| | 8 | 0.64 GiB/s | 20.5 GB |
| 21.6 GiB of JPEGs | 4 | 0.88 GiB/s | 0.3 GB |
| | 8 | 0.95 GiB/s | 0.4 GB |

Full `verify` ran at 0.58 GiB/s, possibly with the repository still cached.
The JPEG sample stored 12.5 GiB of 21.6 GiB because of exact duplicates.

Camera RAW files were in neither sample. They are the largest group that goes
through zstd, so they may run at a different speed.

M4 Max (14 cores, 36 GiB), warm cache, 20 MiB incompressible files: new data
levels off at 1.6 GiB/s from 4 workers up, while already-stored data reaches
7.2 GiB/s at 14 workers. Raw disk write speed was 6.3 GiB/s. Peak RSS was about
6 x workers x file size there, because writes were the bottleneck and the
queues stayed full. On the test server's slower cold reads, it was far lower.

## Code follow-ups

Found during this testing. None of them block the ingest.

- **Whole-file buffering.** `crates/core/src/ingest/worker.rs` holds each file
  in memory, and the storage queue can hold many more. The default of one worker
  per core is unsafe for runs of multi-GB files on smaller machines. Streaming
  large files, or a byte budget on the queue, would fix it.
- **New-data throughput.** Ingest of new data stops scaling at about 4 workers.
  The single thread in `crates/core/src/ingest/storage.rs` is the likely limit,
  but this has not been profiled.
- **`verify` reads every unit twice.** `Checker::unit` in
  `crates/core/src/verify.rs` checks headers with `PackReader::read_record`,
  which reads the whole payload. `Checker::content` then reads the same units
  again. This happens in `--quick` mode too, so `--quick` still reads the
  entire repository. The second read mostly hits the page cache: on 1.58 TB
  stored, a full `verify` read about 3.3 TB from disk (units, then the
  whole-pack hashes) and spent more time in the kernel than in `wmk` itself
  (2,159 s against 1,523 s). A header-only read would fix both modes.
- **Serial `verify`.** `crates/core/src/verify.rs` uses one thread. On the full
  archive it checked 1,469 GiB stored in 63.8 min (0.38 GiB/s).
- **Format mapping.** `from_mime` in `crates/core/src/sniff.rs` does not know
  `.mov`, `.mkv` or camera RAW, so they get a zstd attempt. The measured cost
  was small on random bytes, so this is low priority.
- **`recover`.** Still to be built (Plan 2).
