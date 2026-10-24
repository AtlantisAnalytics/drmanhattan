//! Canonical manifest hashing (spec §6.4). The byte encoding is permanent:
//! all integers are little-endian, entries are fed in canonical order.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::BufReader;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::{EntryKind, EntryRow, EntryStatus};
use crate::walk::{walk, WalkEntry, WalkItem, WalkOptions};

#[derive(Debug, Clone, Copy)]
pub struct ManifestOptions {
    pub mtime_precision_ns: u64,
    /// Used by `manifest_dir` only; `manifest_snapshot` ignores it.
    pub cross_fs: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestHashes {
    pub core: Hash32,
    pub ext: Hash32,
    pub entries: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct ManifestEntry<'a> {
    pub path: &'a [u8],
    pub kind: EntryKind,
    pub mode: u32,
    pub mtime_ns: i64,
    pub size: u64,
    pub content: Option<Hash32>,
    pub link_target: Option<&'a [u8]>,
    /// Canonical index of the first member of a hard-link group that has at
    /// least two members inside the hashed tree; `None` otherwise.
    pub hardlink_first: Option<u64>,
    pub xattrs: &'a [(Vec<u8>, Vec<u8>)],
}

#[derive(Debug, Clone)]
pub struct OwnedManifestEntry {
    pub path: Vec<u8>,
    pub kind: EntryKind,
    pub mode: u32,
    pub mtime_ns: i64,
    pub size: u64,
    pub content: Option<Hash32>,
    pub link_target: Option<Vec<u8>>,
    pub hardlink_first: Option<u64>,
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl OwnedManifestEntry {
    pub fn as_entry(&self) -> ManifestEntry<'_> {
        ManifestEntry {
            path: &self.path,
            kind: self.kind,
            mode: self.mode,
            mtime_ns: self.mtime_ns,
            size: self.size,
            content: self.content,
            link_target: self.link_target.as_deref(),
            hardlink_first: self.hardlink_first,
            xattrs: &self.xattrs,
        }
    }
}

pub struct ManifestHasher {
    core: blake3::Hasher,
    ext: blake3::Hasher,
    precision: i64,
    entries: u64,
}

fn put_u32(buf: &mut Vec<u8>, v: usize) {
    // Lengths beyond u32 cannot occur for names, link targets or xattr values
    // on supported filesystems; saturate rather than panic.
    buf.extend_from_slice(&u32::try_from(v).unwrap_or(u32::MAX).to_le_bytes());
}

impl ManifestHasher {
    /// `mtime_precision_ns == 0` is treated as 1 (no flooring).
    pub fn new(mtime_precision_ns: u64) -> ManifestHasher {
        let precision = i64::try_from(mtime_precision_ns.max(1)).unwrap_or(i64::MAX);
        ManifestHasher {
            core: blake3::Hasher::new(),
            ext: blake3::Hasher::new(),
            precision,
            entries: 0,
        }
    }

    /// Entries must be pushed in canonical order.
    pub fn push(&mut self, e: &ManifestEntry) {
        let mut buf = Vec::with_capacity(e.path.len() + 64);
        put_u32(&mut buf, e.path.len());
        buf.extend_from_slice(e.path);
        buf.push(e.kind.as_u8());
        let mode = e.mode & 0o1777;
        let mtime = e.mtime_ns - e.mtime_ns.rem_euclid(self.precision);
        match e.kind {
            EntryKind::Dir => {
                buf.extend_from_slice(&mode.to_le_bytes());
                buf.extend_from_slice(&mtime.to_le_bytes());
            }
            EntryKind::File => {
                buf.extend_from_slice(&mode.to_le_bytes());
                buf.extend_from_slice(&mtime.to_le_bytes());
                buf.extend_from_slice(&e.size.to_le_bytes());
                buf.extend_from_slice(&e.content.map(|h| h.0).unwrap_or([0; 32]));
            }
            EntryKind::Symlink => {
                let t = e.link_target.unwrap_or(&[]);
                put_u32(&mut buf, t.len());
                buf.extend_from_slice(t);
            }
        }
        self.core.update(&buf);
        self.ext.update(&buf);

        let mut x = Vec::new();
        x.extend_from_slice(&e.hardlink_first.unwrap_or(u64::MAX).to_le_bytes());
        let mut xs: Vec<&(Vec<u8>, Vec<u8>)> = e.xattrs.iter().collect();
        xs.sort_by(|a, b| a.0.cmp(&b.0));
        put_u32(&mut x, xs.len());
        for (name, value) in xs {
            put_u32(&mut x, name.len());
            x.extend_from_slice(name);
            put_u32(&mut x, value.len());
            x.extend_from_slice(value);
        }
        self.ext.update(&x);
        self.entries += 1;
    }

    pub fn finish(self) -> ManifestHashes {
        ManifestHashes {
            core: Hash32(*self.core.finalize().as_bytes()),
            ext: Hash32(*self.ext.finalize().as_bytes()),
            entries: self.entries,
        }
    }
}

fn hash_file(path: &Path) -> Result<Hash32> {
    let f = File::open(path).map_err(|e| Error::io(path, e))?;
    let mut h = blake3::Hasher::new();
    h.update_reader(BufReader::with_capacity(1 << 20, f))
        .map_err(|e| Error::io(path, e))?;
    Ok(Hash32(*h.finalize().as_bytes()))
}

fn read_xattrs(path: &Path, kind: EntryKind) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    // `xattr::list` / `xattr::get` do not follow symlinks.
    let names = match xattr::list(path) {
        Ok(n) => n,
        Err(e) => {
            let code = e.raw_os_error();
            let unsupported = code == Some(libc::ENOTSUP);
            let link_perm = kind == EntryKind::Symlink && code == Some(libc::EPERM);
            if unsupported || link_perm {
                return Ok(Vec::new());
            }
            return Err(Error::io(path, e));
        }
    };
    let mut out = Vec::new();
    for name in names {
        match xattr::get(path, &name) {
            Ok(Some(v)) => out.push((name.into_vec(), v)),
            Ok(None) => {}
            Err(e) => return Err(Error::io(path, e)),
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

pub fn manifest_dir(root: &Path, opts: &ManifestOptions) -> Result<ManifestHashes> {
    let wopts = WalkOptions {
        cross_fs: opts.cross_fs,
    };
    // Pass 1: metadata only. Count (dev, ino) members of nlink > 1 files.
    let mut counts: HashMap<(u64, u64), u32> = HashMap::new();
    walk(root, &wopts, |item| {
        if let WalkItem::Entry(e) = item {
            if e.kind == EntryKind::File && e.nlink > 1 {
                *counts.entry((e.dev, e.ino)).or_insert(0) += 1;
            }
        }
        Ok(())
    })?;

    // Pass 2: hash.
    let mut hasher = ManifestHasher::new(opts.mtime_precision_ns);
    let mut firsts: HashMap<(u64, u64), u64> = HashMap::new();
    let mut index: u64 = 0;
    walk(root, &wopts, |item| {
        let WalkItem::Entry(e) = item else {
            return Ok(());
        };
        let abs = if e.path.is_empty() {
            root.to_path_buf()
        } else {
            root.join(OsStr::from_bytes(&e.path))
        };
        push_walk_entry(&mut hasher, &e, &abs, &counts, &mut firsts, index)?;
        index += 1;
        Ok(())
    })?;
    Ok(hasher.finish())
}

fn push_walk_entry(
    hasher: &mut ManifestHasher,
    e: &WalkEntry,
    abs: &Path,
    counts: &HashMap<(u64, u64), u32>,
    firsts: &mut HashMap<(u64, u64), u64>,
    index: u64,
) -> Result<()> {
    let key = (e.dev, e.ino);
    let hardlink_first = if e.kind == EntryKind::File
        && e.nlink > 1
        && counts.get(&key).copied().unwrap_or(0) >= 2
    {
        Some(*firsts.entry(key).or_insert(index))
    } else {
        None
    };
    let content = if e.kind == EntryKind::File {
        Some(hash_file(abs)?)
    } else {
        None
    };
    let xattrs = read_xattrs(abs, e.kind)?;
    hasher.push(&ManifestEntry {
        path: &e.path,
        kind: e.kind,
        mode: e.mode,
        mtime_ns: e.mtime_ns,
        size: e.size,
        content,
        link_target: e.link_target.as_deref(),
        hardlink_first,
        xattrs: &xattrs,
    });
    Ok(())
}

fn included(row: &EntryRow) -> bool {
    matches!(row.status, EntryStatus::Ok | EntryStatus::Unstable)
}

/// Pre-order traversal of the included entries under `start` (inclusive).
fn traverse(
    cat: &Catalog,
    snapshot: Id,
    start: EntryRow,
    mut visit: impl FnMut(&EntryRow) -> Result<()>,
) -> Result<()> {
    if !included(&start) {
        return Ok(());
    }
    visit(&start)?;
    if start.kind != EntryKind::Dir {
        return Ok(());
    }
    // Stack of pending sibling lists, each reversed so `pop` yields the first.
    let mut stack: Vec<Vec<EntryRow>> = vec![reversed(cat.children(snapshot, Some(start.id))?)];
    while let Some(top) = stack.last_mut() {
        let Some(row) = top.pop() else {
            stack.pop();
            continue;
        };
        if !included(&row) {
            continue;
        }
        visit(&row)?;
        if row.kind == EntryKind::Dir {
            stack.push(reversed(cat.children(snapshot, Some(row.id))?));
        }
    }
    Ok(())
}

fn reversed(mut v: Vec<EntryRow>) -> Vec<EntryRow> {
    v.reverse();
    v
}

pub fn manifest_snapshot(
    cat: &Catalog,
    snapshot: Id,
    root_path: &[u8],
    opts: &ManifestOptions,
) -> Result<ManifestHashes> {
    let start = cat.entry_by_path(snapshot, root_path)?;

    let mut counts: HashMap<i64, u32> = HashMap::new();
    traverse(cat, snapshot, start.clone(), |row| {
        if row.kind == EntryKind::File {
            if let Some(g) = row.hardlink_group {
                *counts.entry(g).or_insert(0) += 1;
            }
        }
        Ok(())
    })?;

    let mut hasher = ManifestHasher::new(opts.mtime_precision_ns);
    let mut firsts: HashMap<i64, u64> = HashMap::new();
    let mut index: u64 = 0;
    traverse(cat, snapshot, start, |row| {
        let rel: &[u8] = if root_path.is_empty() {
            &row.path
        } else if row.path == root_path {
            &[]
        } else {
            row.path
                .strip_prefix(root_path)
                .and_then(|p| p.strip_prefix(b"/"))
                .ok_or_else(|| {
                    Error::Corrupt(format!(
                        "entry path not under {root_path:?}: {:?}",
                        row.path
                    ))
                })?
        };
        let hardlink_first = match (row.kind, row.hardlink_group) {
            (EntryKind::File, Some(g)) if counts.get(&g).copied().unwrap_or(0) >= 2 => {
                Some(*firsts.entry(g).or_insert(index))
            }
            _ => None,
        };
        let (size, content) = if row.kind == EntryKind::File {
            let content = row.content_hash.ok_or_else(|| {
                Error::Corrupt(format!("file entry {:?} has no content hash", row.path))
            })?;
            let size = row
                .size
                .and_then(|s| u64::try_from(s).ok())
                .ok_or_else(|| Error::Corrupt(format!("file entry {:?} has no size", row.path)))?;
            (size, Some(content))
        } else {
            (0, None)
        };
        let xattrs = cat.xattrs(row.id)?;
        hasher.push(&ManifestEntry {
            path: rel,
            kind: row.kind,
            mode: row.mode,
            mtime_ns: row.mtime_ns,
            size,
            content,
            link_target: row.link_target.as_deref(),
            hardlink_first,
            xattrs: &xattrs,
        });
        index += 1;
        Ok(())
    })?;
    Ok(hasher.finish())
}
