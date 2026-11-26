# `wmk recover` and crash testing: design

Status: approved 2026-11-25.

## Why

The catalog (`catalog.sqlite`) is a fast index, not the only copy of
anything. That was a v1 design commitment (spec §6.2, §6.3, §9): every pack
record describes itself, and each finished ingest stores a snapshot record
inside the packs, so the catalog can be rebuilt from the packs alone. v1
shipped without the tool that does it. Today a lost or corrupt catalog
means the repository is unusable, and PLAN.md tells people to delete a
repository if an ingest is stopped partway.

The owner's measures are speed, storage efficiency and bulletproof
reproducibility. Hand-labelled datasets must survive. The concrete
situations Plan 2 serves:

- **The catalog is lost or corrupt** (disk error, accidental deletion, a
  copy taken mid-write or without its `-wal` file).
- **Moving a repository** to another machine.
- **Backing up a repository.** Sealed packs never change, so incremental
  backups copy only new packs, while the catalog changes every ingest.
  With `recover`, `wmk.toml` plus `packs/` is a complete backup.
- **Stopping an ingest partway** (Ctrl-C, a crash, a power cut) must never
  cost the repository.

**Next, after Plan 2:** combining repositories ingested on two machines
(pool both `packs/` folders, then `recover`). The owner wants it soon after
Plan 2. Plan 2 does not build or test it, but must not rule it out (see
Constraints).

## Goals

1. `wmk recover` rebuilds the catalog from `wmk.toml` and `packs/` alone.
   Every snapshot whose snapshot record was durably written comes back,
   with the same rows the original catalog held for it.
2. Recover reads every byte and checks every unit against its hash.
3. **Damage (the owner chose this: "option A"):** recover brings back
   everything that checks out. A snapshot with files that cannot be
   rebuilt is kept and marked `damaged`, the exact files are named, and
   the command exits 2. Fully intact snapshots are unaffected, and their
   undamaged files restore normally even from a damaged snapshot.
4. The old catalog is never destroyed. The new one is built beside it,
   checked, then swapped in atomically, and the old one is kept.
5. Recover never modifies or deletes anything in `packs/`.
6. Derived data is recomputed: image dimensions (`media.width`,
   `media.height`), exactly as ingest computes them.
7. Crash tests prove that killing an ingest at any point leaves a usable
   repository: `verify` passes, the next ingest works, and recover
   restores every finished snapshot byte-exact.
8. The README documents moving and backing up a repository.

## Non-goals

- Combining repositories (next update; see Why).
- Bringing back snapshots whose snapshot record was never written. Their
  stored units stay in the packs, are indexed by recover, and are reused
  by the next ingest.
- A fast header-only mode (`--quick`). It can come later if a full rebuild
  proves too slow.
- Repairing a catalog in place by comparing it with the packs.
- Simulating power loss (unsynced data vanishing). The crash tests kill the
  process; the fsync ordering is already covered by the Plan 1 failpoint
  test.
- Garbage collection of unreferenced, duplicate or corrupt units.
- Labels: not implemented yet (`label_pattern` is always null). Recover
  recomputes them once Plan 4 adds them.
- Purge records (Plan 3). No current repository contains one.

## Anti-goals

- Recover making anything worse: losing the old catalog, writing to packs,
  or leaving a half-built catalog in place.
- Hiding a failed unit. Every unit that fails its hash is in the report,
  even when it costs no file (see exit codes).
- Silently dropping a finished snapshot. Every snapshot recover cannot
  bring back is named in its report.

## Approaches considered

1. **Full rebuild reading every byte (chosen).** Scan all packs in
   parallel, hash every unit, replay every snapshot record into a new
   catalog, check it, swap it in. Takes about as long as a full `verify`
   (about 20 minutes for the 1.58 TB test repository). It is the only
   approach that can name damaged files, which goal 3 requires. It is one
   code path, and combining repositories builds on it directly.
2. **Header-only rebuild.** Like `verify --quick` (about 2 minutes on the
   same repository), but a corrupt unit looks fine until a later `verify`.
   Rejected for now: it breaks goal 3. Possible later as `--quick`.
3. **Repair in place.** Compare packs with the existing catalog and fix
   the differences. Rejected: it needs a trustworthy catalog to compare
   with, which is what recover's situations lack, and it is a second,
   harder code path.

## Design

### Command

`wmk recover [--workers N] [--json]`, with the usual `--repo`. It takes the
writer lock, so it cannot run beside an ingest. `--workers` works as in
`verify` (0 = one per CPU).

Exit codes:
- **0:** every recovered snapshot is fully intact and no snapshot record
  was lost. Problems that cost no file (a corrupt duplicate of a unit
  that has an intact copy, a corrupt unit no snapshot references, a sealed
  pack whose whole-file BLAKE3 fails while every record in it validates,
  stray files) are still listed in the report, as warnings.
- **2:** at least one snapshot is `damaged`, or a snapshot record could
  not be recovered.
- **1:** error; nothing was swapped in.

It requires `wmk.toml` (chunking and compression settings are not stored
in the packs). It works whether `catalog.sqlite` is present, corrupt, or
missing.

### Steps

1. **Sweep.** Delete stale `tmp/record-*.cbor` files (the writer lock
   proves no ingest owns them). Ingest will also do this sweep when it
   starts (it does not today).
2. **Scan and hash.** Every `*.pack` file under `packs/`, in parallel,
   with a new salvage scanner (the existing `pack::scan` stops or fails
   too early for this):
   - It never fails a whole pack over one bad record. After a record that
     does not validate, it searches forward for the next valid record
     header (marker, CRC and size invariants), so damage in the middle of
     a pack, sealed or not, costs only the damaged records. A record with
     a valid CRC but an unknown kind or compression is reported and
     skipped, not an error.
   - Every validated record is decompressed and its BLAKE3 compared with
     its `unit_hash`. Good records become `unit` rows.
   - A unit found intact in more than one pack (possible after a crash and
     re-ingest) is indexed once. A corrupt copy is ignored when an intact
     copy exists, and listed as a warning.
   - An unsealed pack's torn tail is ignored, not truncated (this
     deliberately supersedes v1 spec §9, which said recover truncates).
   - Each pack gets a `pack` row. A pack whose footer is consistent and
     whose BLAKE3 matches is `sealed = 1` with its `blake3`. Otherwise it
     is `sealed = 0` with `size` = the end of its last valid record.
   - A bad file magic, an unknown pack format, a file too short for a
     header, and a header `pack_id` that differs from the file name are
     reported. Records in such a file are still salvaged where they
     validate, except that a file whose header is unreadable is skipped.
     Files under `packs/` not named like packs are reported and ignored.
3. **Replay snapshot records.** Each `snapshot_record` unit is read with a
   streaming reader that inserts rows as it parses, inside one transaction
   per snapshot, committed only after the document's BLAKE3 and size check
   out. A record that fails (a missing chunk, a hash mismatch, a strict
   decoding error) rolls back, and its snapshot is reported as lost, by id
   when the index unit is readable. The snapshot row comes from the record
   header, with `record_unit` set to the record unit's hash.
   - The record wins over any other source. Ingest writes the record
     durably before it seals the last pack and before the final catalog
     update, so a snapshot killed in that window, or marked `failed` after
     it, comes back `complete`.
   - Two different records for the same snapshot id is corruption,
     reported, and neither is used.
4. **Recompute media.** For each content whose format is an image format
   ingest measures, read its first bytes (the same sniff length ingest
   uses) and call the same dimension function ingest uses. Contents whose
   first bytes are missing get no `media` row.
5. **Mark damage.** A content whose units are not all present is damaged:
   its `content` row is kept, and its `content_unit` rows exist only for
   the units that are present, so damage is visible as fewer `content_unit`
   rows than `unit_count`. Entry rows stay exactly as recorded (status
   included), so manifest hashes are unchanged. A snapshot with any entry
   referencing a damaged content gets `status = 'damaged'`.
6. **Check the new catalog.** SQLite `integrity_check` and
   `foreign_key_check`. For every recovered snapshot, damaged or not, the
   manifest hash computed from the rebuilt rows (as `verify` does) must
   equal the stored `manifest_hash`. A failure here is an error: nothing
   is swapped.
7. **Swap.** The new catalog is built in `tmp/` and fully checkpointed
   (no `-wal` left). The old catalog is preserved first under
   `catalog.sqlite.pre-recover-<UTC timestamp>` in the repository root,
   together with any `-wal` and `-shm` files, which are moved out of the
   way so none can sit beside the new catalog. The new catalog is then
   renamed over `catalog.sqlite` in one atomic rename, and the directory
   is fsynced. There is no moment without a catalog once the old one is
   preserved; if the old one was missing, there was none to begin with.
8. **Report.** Counts (packs, units, snapshots recovered, bytes read,
   time); damaged snapshots with each damaged file's path; lost snapshot
   records; warnings (as listed under exit codes); and, when the old
   catalog was readable, the snapshots it held that recover did not bring
   back (normally unfinished ingests). Human output may abbreviate long
   file lists; `--json` lists everything.

### Catalog and format version

- One new value: `snapshot.status = 'damaged'`. Entry statuses do not
  change. The snapshot record format does not change (recover never
  writes records).
- The schema version lives in two places that must agree (v1 spec §12):
  the catalog's `user_version` and `wmk.toml`'s `format_version`. Both
  become 2 in repositories that `init` creates and that `recover`
  rebuilds. Recover rewrites `wmk.toml` atomically (write to a temporary
  file, fsync, rename) and only when it swaps in the new catalog. wmk 0.3
  opens version 1 and 2 repositories and leaves an existing version 1
  repository at version 1 until it is recovered. wmk 0.2 refuses a
  version 2 repository, as it refuses any unknown version today.

### Missing catalog guard

`Catalog::open` creates an empty catalog when none exists. After a lost
catalog, that would let an ingest start a fresh, empty catalog beside
packs full of data, re-storing everything. So every command that opens
the catalog for writing (ingest, and anything else that does) refuses
when `catalog.sqlite` is missing but `packs/` holds any pack, and says to
run `wmk recover`. `init` is unaffected (it creates an empty repository).

### Behaviour of other commands

- **`ls`** shows `damaged` as the status.
- **`restore`** accepts a `damaged` snapshot. It restores everything that
  is intact, withholds every file whose content is damaged, lists them,
  and exits 2. `restore --path` of a folder with no damaged files exits 0.
  The withholding is a restore-time check on content completeness, kept
  separate from the existing status-based withholding (`ok` and
  `unstable` only), so the manifest computation is untouched. `latest`
  keeps meaning the newest `complete` snapshot.
- **`verify`** reports a damaged snapshot's damaged files and exits 2. Its
  manifest check passes, since entry rows are unchanged.
- **`ingest`** must not treat a damaged content as already stored: when a
  file with that content is ingested again, its missing units are stored
  again, which repairs the content for every snapshot that uses it. (The
  old snapshots keep `damaged` until the next `recover`; clearing it
  during ingest is out of scope.)

### Crash testing

Tests run the real `wmk` binary with failpoints set through the
`FAILPOINTS` environment variable. That needs work in the CLI: a
`failpoints` feature that enables `drmanhattan-core/failpoints`, and fail
scenario setup in `main`. The `fail` crate has no abort action, so the
failpoints use a closure that calls `std::process::abort` when a test
asks for it, ending the process with no destructors or cleanup.

Kill points cover every point in ingest between two durable steps,
including: after the walk; after units are written but before the fsync;
after the fsync but before the catalog commit; after a barrier's catalog
commit; while the snapshot record's units are being written; after the
record is durable but before the final seal; after the final seal but
before the final snapshot upsert; after the final upsert. After each kill:

- `verify --quick` passes and no catalog row references missing data.
- A fresh ingest of the same tree succeeds and restores byte-exact.
- Deleting the catalog and running `recover` brings back every snapshot
  whose record was durably written before the kill (including the killed
  one when the kill came after its record), restorable byte-exact.

### Other tests

- **Round trip.** Ingest several snapshots (sharing content, with images,
  xattrs, hard links, symlinks, empty dirs and odd names), delete the
  catalog, recover: every table recover rebuilds (`snapshot`, `entry`,
  `content`, `content_unit`, `unit`, `pack`, `xattr`, `media`) holds the
  same rows as the original catalog, except that unit locations may
  differ where a unit is stored twice and `analyzed_at`-style timestamps
  may differ. Every snapshot restores byte-exact.
- **Recover over a healthy catalog** gives an equivalent catalog, keeps
  the old one, and exits 0.
- **Damage.** Flip a byte in a data unit: that file is named, its snapshot
  is `damaged`, exit 2, restore withholds exactly that file and restores
  the rest byte-exact, and the manifest check passes. Flip a byte in a
  snapshot record chunk: that snapshot is reported lost, others recover.
  Corrupt a pack header, damage a record in the middle of an unsealed
  pack, truncate a sealed pack, and corrupt a footer: each is reported,
  and every intact record elsewhere, including after the damage in the
  same pack, is used. A unit stored twice with one copy corrupt: no
  damaged snapshot, a warning, exit 0.
- **Healing.** After damage, re-ingesting the source stores the missing
  units again, and a second recover clears `damaged`.
- **Missing catalog guard.** Delete the catalog: ingest refuses and names
  `wmk recover`; after recover, ingest works.
- **Moving.** Copy a repository to another path and restore from it.
  Delete the copy's catalog and recover it.
- **Torn tail.** An unsealed pack with a torn tail recovers without the
  file being changed (its length and bytes are compared before and after).
- **Versions.** A recovered repository has version 2 in both places; a
  version 1 repository still opens and ingests without being upgraded.
- Recover refuses to run while the writer lock is held, and fails cleanly
  without `wmk.toml`.

## Constraints

- Packs are never written by recover. The old catalog is never deleted.
- Memory is bounded independently of repository size: the snapshot record
  is streamed, and hashing holds at most a few units per worker (units are
  at most 16 MiB).
- Speed: within roughly 1.5x of a full `verify` on the same repository.
- Must not rule out combining repositories: recover accepts any pack ids,
  duplicate units across packs, and snapshots from different hosts, and
  never assumes one ingest history. (Known issue for that update:
  `screen_hit.id` is a repository-wide integer copied into records, so
  pooled repositories could collide once screening writes hits.)
- Public repository: no host names, user names or personal paths in
  committed files or tests.
- New public API and a format version change: the release is 0.3.0.

## Left to the builder

- The exact failpoint names and placement, given the list above.
- How the streaming record reader is structured, and whether `read_record`
  becomes a thin wrapper over it.
- How the salvage scanner relates to `pack::scan` (shared parsing is
  expected; `scan`'s current behaviour for its existing callers stays).
- Batching and transaction sizes for unit inserts, and how the scan
  parallelizes across packs (verify has no pack-scanning pool to reuse;
  its `check_packs` only hashes whole files).
- The human report layout and the `--json` field names, following the
  existing commands.
- Which copy of a duplicated intact unit is indexed (any choice is
  correct).
- The PLAN.md and README updates (moving and backing up a repository:
  copy the whole folder while nothing is writing, run `wmk verify` on
  the other side; for backups, `wmk.toml` plus `packs/` is enough with
  `recover`; never run wmk over a network file system).
