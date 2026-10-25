//! Canonical manifest hashing (spec §6.4). The byte encoding is permanent:
//! all integers are little-endian, entries are fed in canonical order.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::EntryKind;
use crate::snaptree;
use crate::walk::{walk, WalkEntry, WalkItem, WalkOptions};
use crate::xattrs::read_xattrs;

/// OS-managed, per-process xattrs that are neither recorded at ingest nor
/// hashed (spec §4). Ingest must filter with this same list.
/// `com.apple.quarantine` is deliberately NOT listed: it is meaningful data.
pub const IGNORED_XATTRS: &[&[u8]] = &[b"com.apple.provenance"];

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

fn put_u32(buf: &mut Vec<u8>, v: usize) -> Result<()> {
    let v = u32::try_from(v)
        .map_err(|_| Error::Corrupt("manifest field length exceeds u32".to_string()))?;
    buf.extend_from_slice(&v.to_le_bytes());
    Ok(())
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
    ///
    /// Fails with `Corrupt` (leaving the hasher unchanged) if a length
    /// exceeds `u32`, a symlink has no target, or a file has no content hash.
    pub fn push(&mut self, e: &ManifestEntry) -> Result<()> {
        let mut buf = Vec::with_capacity(e.path.len() + 64);
        put_u32(&mut buf, e.path.len())?;
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
                let c = e.content.ok_or_else(|| {
                    Error::Corrupt(format!("file entry {:?} has no content hash", e.path))
                })?;
                buf.extend_from_slice(&c.0);
            }
            EntryKind::Symlink => {
                let t = e.link_target.ok_or_else(|| {
                    Error::Corrupt(format!("symlink entry {:?} has no target", e.path))
                })?;
                put_u32(&mut buf, t.len())?;
                buf.extend_from_slice(t);
            }
        }

        let mut x = Vec::new();
        x.extend_from_slice(&e.hardlink_first.unwrap_or(u64::MAX).to_le_bytes());
        let mut xs: Vec<&(Vec<u8>, Vec<u8>)> = e.xattrs.iter().collect();
        xs.sort_by(|a, b| a.0.cmp(&b.0));
        put_u32(&mut x, xs.len())?;
        for (name, value) in xs {
            put_u32(&mut x, name.len())?;
            x.extend_from_slice(name);
            put_u32(&mut x, value.len())?;
            x.extend_from_slice(value);
        }

        self.core.update(&buf);
        self.ext.update(&buf);
        self.ext.update(&x);
        self.entries += 1;
        Ok(())
    }

    pub fn finish(self) -> ManifestHashes {
        ManifestHashes {
            core: Hash32(*self.core.finalize().as_bytes()),
            ext: Hash32(*self.ext.finalize().as_bytes()),
            entries: self.entries,
        }
    }
}

pub(crate) fn hash_file(path: &Path) -> Result<Hash32> {
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| Error::io(path, e))?;
    let mut h = blake3::Hasher::new();
    h.update_reader(BufReader::with_capacity(1 << 20, f))
        .map_err(|e| Error::io(path, e))?;
    Ok(Hash32(*h.finalize().as_bytes()))
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
    })
}

pub fn manifest_snapshot(
    cat: &Catalog,
    snapshot: Id,
    root_path: &[u8],
    opts: &ManifestOptions,
) -> Result<ManifestHashes> {
    let withheld = || {
        Error::NotFound(format!(
            "path {root_path:?} is withheld in snapshot {snapshot}"
        ))
    };
    let mut counts: HashMap<i64, u32> = HashMap::new();
    let mut first = true;
    snaptree::traverse(cat, snapshot, root_path, |n| {
        if std::mem::take(&mut first) && !n.included {
            return Err(withheld());
        }
        if n.included && n.row.kind == EntryKind::File {
            if let Some(g) = n.row.hardlink_group {
                *counts.entry(g).or_insert(0) += 1;
            }
        }
        Ok(())
    })?;

    let mut hasher = ManifestHasher::new(opts.mtime_precision_ns);
    let mut firsts: HashMap<i64, u64> = HashMap::new();
    let mut index: u64 = 0;
    snaptree::traverse(cat, snapshot, root_path, |n| {
        if !n.included {
            return Ok(());
        }
        let row = &n.row;
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
            path: &n.rel,
            kind: row.kind,
            mode: row.mode,
            mtime_ns: row.mtime_ns,
            size,
            content,
            link_target: row.link_target.as_deref(),
            hardlink_first,
            xattrs: &xattrs,
        })?;
        index += 1;
        Ok(())
    })?;
    Ok(hasher.finish())
}
