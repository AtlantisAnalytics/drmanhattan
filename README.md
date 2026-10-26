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
| `wmk ingest <dir>` | Snapshot a directory into the repository |
| `wmk manifest <dir>` | Compute the manifest hash of a live directory |
| `wmk restore <snapshot> <dest>` | Rebuild a snapshot byte-exact in a new directory (`--path`, `--hardlinks`, `--xattrs`) |
| `wmk verify [snapshot]` | Check that stored data is intact (`--quick` skips decompression) |
| `wmk diff <snapshot> <dir>` | Show path-level differences between a snapshot and a live directory |
| `wmk ls` | List snapshots |
| `wmk show <snapshot> [path]` | Browse a snapshot's tree |
| `wmk stats` | Repository totals and dedup ratios |
| `wmk resolve <entry-id>` | Look up an entry id: snapshot, path and content |
| `wmk find --content <hash>` | Find every entry with a given content hash |
| `wmk cat <entry-id or hash>` | Stream file bytes to stdout, verified as they stream |

A snapshot is named by `latest`, its full UUID, or a prefix of at least
8 hex characters.

Exit codes: 0 success, 1 error, 2 verification failure or differences found,
3 entries withheld by screening.

## v1 limits

- **Memory:** v1 buffers each file in memory during ingest; very large files
  (multi-GB) need correspondingly large RAM. Streaming large files comes
  later (Plan 3).
- **No `recover` yet:** rebuilding the catalog from the packs alone is
  Plan 2. The packs and snapshot records already carry everything it needs.
- **No screening yet:** hash-list screening, provenance capture and `purge`
  are Plan 3.

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
