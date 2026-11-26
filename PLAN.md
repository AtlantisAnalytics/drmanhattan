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

## Second ingest: incremental dedup (2026-11-19)

A second machine held a superset of the same archive: the same 156,774 paths
and sizes, plus 12,456 extra files (65.7 GiB). It was ingested into the same
repository as a second snapshot, read over NFS 4.2 (read-only, `nconnect=4`)
on a direct 10 GbE link.

| Step | Result |
|---|---|
| Ingest, 4 workers | 36.3 min, 2,035 GiB at 0.94 GiB/s, peak RSS 14.7 GB |
| Newly stored | 6 units, 2.58 MB (5 new contents) |
| Repository after | 2 snapshots, 4.30 TB logical in 1.58 TB stored: dedup 2.71, storage ratio 0.367 |
| Restore of the largest extra folder (12,567 files, 63 GiB) | 26.6 s; byte-identical, metadata identical |

All of the extra files were copies of content already stored under other
names, which only content-level dedup can find. Ingest of already-stored
content ran faster than the first ingest (0.94 against 0.69 GiB/s), because
known content is hashed but not written.

Notes for repeating this:

- The link measured 9.4 Gb/s raw TCP and 0.6-0.7 GB/s for a single cold file
  over NFS, so 4 workers kept the ingest from waiting on the network.
- NFS 4.2 preserves sub-second mtimes. `sshfs` (SFTP v3) rounds them to whole
  seconds, so it is not suitable for snapshots that must keep mtimes exact.
- Over 1 GbE the same ingest would have taken about 6 hours.

## Parallel verify (2026-11-20)

`verify --quick` now reads only record headers, and a full `verify` checks
contents and pack hashes on worker threads (`wmk verify --workers N`, 0 = one
per CPU). Released in 0.1.2. On the same 1.58 TB repository:

| Run | Before | After |
|---|---|---|
| Full `verify` | 63.8 min | 20.6 min (about 2.7 GB/s from disk), 0 problems |
| `verify --quick` | read the whole repository | 110 s |

The design, plan and measurement details are in
`docs/superpowers/specs/2026-11-20-parallel-verify-design.md`.

## Next

- Work on the code follow-ups below.
- The test repository is kept for further tests.

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

Expect about 40-90 minutes for the ingest and about 20 minutes for `verify`
(0.1.2 or later; 0.1.1 took about an hour).

If an ingest is stopped partway, run `wmk recover` to rebuild the catalog from
the packs, then ingest again; stored units are reused. The source is not
affected.

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
- **`verify` header pass.** Still serial. It is what `--quick` runs, and it took
  8.9% of a full `verify`, over the design's 5% threshold. Parallelize it,
  keeping the report order.
- **`verify` CPU use.** A full `verify` kept only 3 to 7 of 20 CPUs busy, and
  max RSS rose from 246 MB to 977 MB. Contention on the shared 64-handle pack
  cache or the unit-cache lock are candidates; neither was isolated.
- **`verify` small gaps.** A failed catalog lookup for an already-seen unit
  skips that check silently instead of reporting a problem (this predates the
  parallel work). A content's check stops at its first bad unit. `--workers`
  is not validated.
- **One pass per pack.** A full `verify` still reads the data twice (units,
  then whole-pack hashes). Reading each pack once would halve disk reads, for
  much more complexity; the design leaves room for it.
- **Format mapping.** `from_mime` in `crates/core/src/sniff.rs` does not know
  `.mov`, `.mkv` or camera RAW, so they get a zstd attempt. The measured cost
  was small on random bytes, so this is low priority.
- **`recover`.** Built: `wmk recover` rebuilds the catalog from the packs.
