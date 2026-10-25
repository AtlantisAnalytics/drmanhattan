//! Canonical manifest hashing (spec §6.4). The byte encoding is permanent:
//! all integers are little-endian, entries are fed in canonical order.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::io::BufReader;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::catalog::Catalog;
use crate::display::display_bytes;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::EntryKind;
use crate::snaptree;
use crate::walk::{open_walked_file, walk, WalkEntry, WalkItem, WalkOptions};
use crate::xattrs::read_xattrs;

/// macOS: OS-managed xattrs that are neither recorded at ingest nor hashed
/// (spec §4). `com.apple.quarantine` is deliberately NOT listed: it is
/// meaningful data.
pub const IGNORED_XATTRS: &[&[u8]] = &[b"com.apple.provenance", b"com.apple.macl"];

/// Whether the xattr `name` is recorded at ingest and hashed (spec §4).
/// On Linux only the `user.` namespace is: `security.*`, `system.*` (POSIX
/// ACLs) and `trusted.*` are not. Elsewhere (macOS) every name is, except
/// `IGNORED_XATTRS`. Ingest and `manifest_dir` both filter with this, via
/// the shared xattr reader.
pub fn xattr_recorded(name: &[u8]) -> bool {
    recorded_on(cfg!(target_os = "linux"), name)
}

fn recorded_on(linux: bool, name: &[u8]) -> bool {
    if linux {
        name.starts_with(b"user.")
    } else {
        !IGNORED_XATTRS.contains(&name)
    }
}

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

/// `ns` floored to a multiple of `precision` (which must be at least 1):
/// `ns - ns.rem_euclid(precision)`, saturating at `i64::MIN` where the
/// true floor is not representable.
pub(crate) fn floor_mtime(ns: i64, precision: i64) -> i64 {
    ns.saturating_sub(ns.rem_euclid(precision))
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
        let mtime = floor_mtime(e.mtime_ns, self.precision);
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
                    Error::Corrupt(format!(
                        "file entry {} has no content hash",
                        display_bytes(e.path)
                    ))
                })?;
                buf.extend_from_slice(&c.0);
            }
            EntryKind::Symlink => {
                let t = e.link_target.ok_or_else(|| {
                    Error::Corrupt(format!(
                        "symlink entry {} has no target",
                        display_bytes(e.path)
                    ))
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

/// BLAKE3 of the regular file the walk saw as `entry` at `path`, opened
/// with the ingest worker's checks (`open_walked_file`): a path swapped
/// for a FIFO, a symlink or another file since the walk is an error, never
/// a hang or a hash of something else.
pub(crate) fn hash_file(path: &Path, entry: &WalkEntry) -> Result<Hash32> {
    let (f, _) = open_walked_file(path, entry)?;
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
        Some(hash_file(abs, e)?)
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
            "path {} is withheld in snapshot {snapshot}",
            display_bytes(root_path)
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
                Error::Corrupt(format!(
                    "file entry {} has no content hash",
                    display_bytes(&row.path)
                ))
            })?;
            let size = row
                .size
                .and_then(|s| u64::try_from(s).ok())
                .ok_or_else(|| {
                    Error::Corrupt(format!(
                        "file entry {} has no size",
                        display_bytes(&row.path)
                    ))
                })?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_file_refuses_a_fifo_swapped_in_after_the_walk_without_hanging() {
        use std::os::unix::ffi::OsStrExt;
        let tmp = tempfile::tempdir().unwrap();
        let abs = tmp.path().join("f");
        std::fs::write(&abs, b"hello").unwrap();
        let mut entry = None;
        walk(tmp.path(), &WalkOptions { cross_fs: false }, |item| {
            if let WalkItem::Entry(e) = item {
                if e.path == b"f" {
                    entry = Some(e);
                }
            }
            Ok(())
        })
        .unwrap();
        let entry = entry.unwrap();
        assert_eq!(hash_file(&abs, &entry).unwrap(), Hash32::of(b"hello"));
        std::fs::remove_file(&abs).unwrap();
        let c = std::ffi::CString::new(abs.as_os_str().as_bytes()).unwrap();
        // SAFETY: c is a valid NUL-terminated path for the duration of the call.
        assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o644) }, 0);
        let (tx, rx) = std::sync::mpsc::channel();
        let p = abs.clone();
        std::thread::spawn(move || {
            let _ = tx.send(hash_file(&p, &entry).is_err());
        });
        let refused = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("hash_file hung opening a FIFO");
        assert!(refused);
    }

    #[test]
    fn mtime_floor_saturates_instead_of_overflowing() {
        assert_eq!(floor_mtime(1_234, 100), 1_200);
        assert_eq!(floor_mtime(-1_234, 100), -1_300);
        assert_eq!(floor_mtime(i64::MAX, 1), i64::MAX);
        // i64::MIN is 2 above a multiple of 10: the floor is below i64::MIN.
        assert_eq!(floor_mtime(i64::MIN, 10), i64::MIN);
        let mut h = ManifestHasher::new(10);
        let e = ManifestEntry {
            path: b"",
            kind: EntryKind::Dir,
            mode: 0o755,
            mtime_ns: i64::MIN,
            size: 0,
            content: None,
            link_target: None,
            hardlink_first: None,
            xattrs: &[],
        };
        h.push(&e).unwrap();
        assert_eq!(h.finish().entries, 1);
    }

    #[test]
    fn linux_records_only_the_user_namespace() {
        for name in [&b"user.wmk"[..], b"user.xdg.origin.url"] {
            assert!(recorded_on(true, name), "{name:?}");
        }
        for name in [
            &b"security.selinux"[..],
            b"security.capability",
            b"system.posix_acl_access",
            b"system.posix_acl_default",
            b"trusted.overlay.opaque",
            b"user",
        ] {
            assert!(!recorded_on(true, name), "{name:?}");
        }
    }

    #[test]
    fn macos_drops_only_the_os_managed_names() {
        assert!(!recorded_on(false, b"com.apple.provenance"));
        assert!(!recorded_on(false, b"com.apple.macl"));
        for name in [
            &b"com.apple.quarantine"[..],
            b"com.apple.metadata:kMDItemWhereFroms",
            b"com.example.test",
        ] {
            assert!(recorded_on(false, name), "{name:?}");
        }
    }
}
