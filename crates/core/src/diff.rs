//! Path-level diff between a snapshot and a live directory, under exactly the
//! manifest's fidelity rules (spec §4, §6.4): kind; `mode & 0o1777` and
//! floored mtime for dirs and files; size then content hash for files; link
//! target for symlinks. Skipped walk items (FIFOs and the like) are ignored,
//! as manifests ignore them.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::ids::Id;
use crate::manifest::hash_file;
use crate::model::{EntryKind, EntryRow, EntryStatus};
use crate::repo::Repo;
use crate::walk::{walk, WalkEntry, WalkItem, WalkOptions};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Difference {
    ContentDiffers { path: Vec<u8> },
    ModeDiffers { path: Vec<u8>, snap: u32, dir: u32 },
    MtimeDiffers { path: Vec<u8>, snap: i64, dir: i64 },
    KindDiffers { path: Vec<u8> },
    LinkTargetDiffers { path: Vec<u8> },
    Missing { path: Vec<u8> },
    Extra { path: Vec<u8> },
    ExpectedMissing { path: Vec<u8>, status: EntryStatus },
}

impl Difference {
    pub fn path(&self) -> &[u8] {
        match self {
            Difference::ContentDiffers { path }
            | Difference::ModeDiffers { path, .. }
            | Difference::MtimeDiffers { path, .. }
            | Difference::KindDiffers { path }
            | Difference::LinkTargetDiffers { path }
            | Difference::Missing { path }
            | Difference::Extra { path }
            | Difference::ExpectedMissing { path, .. } => path,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DiffOptions {
    /// Snapshot subtree compared against `dir`'s root; empty is the whole tree.
    pub path: Vec<u8>,
    /// Both sides' mtimes are floored to this (0 is treated as 1).
    pub mtime_precision_ns: u64,
    /// Descend into other filesystems (match how the snapshot was ingested).
    pub cross_fs: bool,
}

fn withheld(s: EntryStatus) -> bool {
    matches!(
        s,
        EntryStatus::ScreenedOut | EntryStatus::ScreenError | EntryStatus::Purged
    )
}

/// `path` relative to `root` (`root` itself is the empty path), if under it.
fn rebase(root: &[u8], path: &[u8]) -> Option<Vec<u8>> {
    if root.is_empty() {
        return Some(path.to_vec());
    }
    if path == root {
        return Some(Vec::new());
    }
    path.strip_prefix(root)
        .and_then(|p| p.strip_prefix(b"/"))
        .map(<[u8]>::to_vec)
}

/// True if a strict ancestor of `path` is in `withheld_paths`.
fn under_withheld(withheld_paths: &std::collections::HashSet<Vec<u8>>, path: &[u8]) -> bool {
    path.iter()
        .enumerate()
        .filter(|(_, b)| **b == b'/')
        .any(|(i, _)| withheld_paths.contains(&path[..i]))
}

fn floor(ns: i64, precision: i64) -> i64 {
    ns - ns.rem_euclid(precision)
}

fn compare(
    out: &mut Vec<Difference>,
    row: &EntryRow,
    e: &WalkEntry,
    abs: &Path,
    precision: i64,
) -> Result<()> {
    let path = e.path.clone();
    if row.kind != e.kind {
        out.push(Difference::KindDiffers { path });
        return Ok(());
    }
    match e.kind {
        EntryKind::Symlink => {
            if row.link_target != e.link_target {
                out.push(Difference::LinkTargetDiffers { path });
            }
        }
        EntryKind::Dir | EntryKind::File => {
            let (sm, dm) = (row.mode & 0o1777, e.mode & 0o1777);
            if sm != dm {
                out.push(Difference::ModeDiffers {
                    path: path.clone(),
                    snap: sm,
                    dir: dm,
                });
            }
            let (st, dt) = (floor(row.mtime_ns, precision), floor(e.mtime_ns, precision));
            if st != dt {
                out.push(Difference::MtimeDiffers {
                    path: path.clone(),
                    snap: st,
                    dir: dt,
                });
            }
            if e.kind == EntryKind::File {
                let same_size = row.size.and_then(|s| u64::try_from(s).ok()) == Some(e.size);
                // Hash only when the sizes match; a row without a hash never matches.
                if !same_size || Some(hash_file(abs)?) != row.content_hash {
                    out.push(Difference::ContentDiffers { path });
                }
            }
        }
    }
    Ok(())
}

pub fn run(repo: &Repo, snapshot: Id, dir: &Path, opts: &DiffOptions) -> Result<Vec<Difference>> {
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let precision = i64::try_from(opts.mtime_precision_ns.max(1)).unwrap_or(i64::MAX);

    // The snapshot subtree, keyed by path relative to the subtree root.
    let mut snap: HashMap<Vec<u8>, EntryRow> = HashMap::new();
    if opts.path.is_empty() {
        cat.for_each_entry(snapshot, |row| {
            snap.insert(row.path.clone(), row);
            Ok(())
        })?;
    } else {
        cat.entry_by_path(snapshot, &opts.path)?;
        cat.for_each_entry(snapshot, |row| {
            if let Some(rel) = rebase(&opts.path, &row.path) {
                snap.insert(rel, row);
            }
            Ok(())
        })?;
    }
    let withheld_paths: std::collections::HashSet<Vec<u8>> = snap
        .iter()
        .filter(|(_, r)| withheld(r.status))
        .map(|(p, _)| p.clone())
        .collect();

    let mut out = Vec::new();
    walk(
        dir,
        &WalkOptions {
            cross_fs: opts.cross_fs,
        },
        |item| {
            let WalkItem::Entry(e) = item else {
                return Ok(());
            };
            match snap.remove(&e.path) {
                None => {
                    if !under_withheld(&withheld_paths, &e.path) {
                        out.push(Difference::Extra {
                            path: e.path.clone(),
                        });
                    }
                }
                // Present although withheld: nothing to compare, nothing to report.
                Some(row) if withheld(row.status) => {}
                Some(row) => {
                    let abs = if e.path.is_empty() {
                        dir.to_path_buf()
                    } else {
                        dir.join(OsStr::from_bytes(&e.path))
                    };
                    compare(&mut out, &row, &e, &abs, precision)?;
                }
            }
            Ok(())
        },
    )?;

    for (path, row) in snap {
        if withheld(row.status) {
            out.push(Difference::ExpectedMissing {
                path,
                status: row.status,
            });
        } else if !under_withheld(&withheld_paths, &path) {
            out.push(Difference::Missing { path });
        }
    }
    out.sort_by(|a, b| a.path().cmp(b.path()));
    Ok(out)
}
