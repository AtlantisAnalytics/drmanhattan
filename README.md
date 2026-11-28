<p align="center">
  <img src="docs/assets/logo.png" alt="drmanhattan logo: an atom whose nucleus breaks apart into storage blocks" width="256">
</p>

# watchmaker (`wmk`)

watchmaker (`wmk`) disassembles directories into storage units and rebuilds
them exactly.

It walks a directory, records its exact structure (names as raw bytes,
modes, nanosecond mtimes, symlinks, hard links, extended attributes), gives
every entry a UUID, and splits file contents into content-defined chunks.
Identical chunks are stored once, compressed, in append-only pack files,
indexed by a SQLite catalog. Any snapshot can then be rebuilt byte for byte.

It is built for ML datasets made from scraped websites, including
hand-labelled folder-as-class datasets that must survive storage and come
back exactly.

## The guarantee

A restore is provably identical to what was ingested. Every snapshot has a
canonical manifest hash over its tree, names, modes, mtimes, sizes, content
hashes and link targets. `wmk manifest` computes the same hash from a live
directory, so you can check a restore yourself:

```sh
wmk init r
wmk --repo r ingest data/                 # prints the manifest hash
wmk --repo r restore latest out/
wmk manifest out/                         # same hash
```

Restore verifies every unit and every whole file against its BLAKE3 hash,
builds the tree in a partial directory and renames it into place only when
everything checked out. It refuses up front (writing nothing) if the
destination filesystem would alter the tree, for example by folding case or
normalizing Unicode names.

## Install

```sh
cargo install drmanhattan    # installs the `wmk` binary
```

The library is published separately as
[`drmanhattan-core`](https://crates.io/crates/drmanhattan-core).

## Build

Requires Rust stable at or above the crates' `rust-version` (1.87),
installed with [rustup](https://rustup.rs). The repository pins its
toolchain in `rust-toolchain.toml`.

```sh
cargo build --release        # binary: target/release/wmk
cargo test --all-features    # full test suite
```

Platforms: macOS (APFS) and Linux (ext4, xfs, btrfs).

## Commands

Every command takes `--repo <dir>` (or `WMK_REPO`) and `--json` for
machine-readable output.

| Command | What it does |
|---|---|
| `wmk init <dir>` | Create a new repository |
| `wmk ingest <dir>` | Snapshot a directory into the repository (`--workers N`; `--max-memory SIZE` caps buffered file data, default a quarter of RAM) |
| `wmk manifest <dir>` | Compute the manifest hash of a live directory |
| `wmk restore <snapshot> <dest>` | Rebuild a snapshot byte-exact in a new directory (`--path`, `--hardlinks`, `--xattrs`) |
| `wmk verify [snapshot]` | Check that stored data is intact (`--quick` skips decompression; `--workers N` sets worker threads, 0 = one per CPU) |
| `wmk recover` | Rebuild the catalog from the pack files (`--workers N`); the old catalog is kept beside the new one. Exit 2 if any file or snapshot record is damaged |
| `wmk diff <snapshot> <dir>` | Show path-level differences between a snapshot and a live directory |
| `wmk ls` | List snapshots |
| `wmk show <snapshot> [path]` | Browse a snapshot's tree |
| `wmk stats` | Repository totals and dedup ratios |
| `wmk resolve <entry-id>` | Look up an entry id: snapshot, path and content |
| `wmk find --content <hash>` | Find every entry with a given content hash |
| `wmk cat <entry-id or hash>` | Stream file bytes to stdout, verified as they stream |

A snapshot is named by `latest`, its full UUID, or a prefix of at least
8 hex characters.

Exit codes: 0 success, 1 error, 2 verification failure or differences found
(also: `restore` withheld damaged files, or `recover` found damage or lost
snapshot records), 3 entries withheld by screening.

## v1 limits

- **Memory:** ingest still reads each file whole into memory, but the total
  held at once is capped (`--max-memory`, default a quarter of RAM). A
  single file larger than the cap is read on its own while other workers
  wait, so it still needs that much RAM. Streaming large files comes later
  (Plan 3).
- **No screening yet:** hash-list screening, provenance capture and `purge`
  are Plan 3.

## Moving or backing up a repository

Copy the whole repository folder while nothing is writing to it, then run
`wmk verify` on the other side. `wmk.toml` plus `packs/` is a complete backup:
if `catalog.sqlite` is lost or damaged, `wmk recover` rebuilds it from the
packs, keeping the old catalog beside the new one. Never run `wmk` over a
network file system.

Each `wmk recover` keeps the catalog it replaced as
`catalog.sqlite.pre-recover-<UTC time>`, with any `-wal` beside it. Keep
these files. A later recover reads them to tell bytes a pack once held
from an unfinished write, so without them, data lost from a pack can be
reported as a harmless torn tail. Each is about the size of the catalog.

`wmk recover` upgrades a repository to format version 2, which wmk 0.2
cannot open.

## Design

The full design, including the catalog schema, pack format and manifest
encoding, is in
[docs/superpowers/specs/2026-10-24-wmk-v1-design.md](docs/superpowers/specs/2026-10-24-wmk-v1-design.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
