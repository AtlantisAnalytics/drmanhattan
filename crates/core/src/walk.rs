//! Canonical walker: depth-first pre-order, children sorted bytewise by raw
//! name, `lstat` only (symlinks are never followed). Iterative, so very deep
//! trees cannot overflow the thread stack.

use std::ffi::OsStr;
use std::fs::{self, Metadata};
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::model::EntryKind;

#[derive(Debug, Clone, Copy)]
pub struct WalkOptions {
    pub cross_fs: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    Socket,
    Fifo,
    CharDevice,
    BlockDevice,
    OtherFilesystem,
}

impl SkipReason {
    pub fn as_str(self) -> &'static str {
        match self {
            SkipReason::Socket => "socket",
            SkipReason::Fifo => "fifo",
            SkipReason::CharDevice => "char_device",
            SkipReason::BlockDevice => "block_device",
            SkipReason::OtherFilesystem => "other_filesystem",
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalkEntry {
    pub depth: u32,
    pub path: Vec<u8>,
    pub name: Vec<u8>,
    pub kind: EntryKind,
    pub mode: u32,
    pub mtime_ns: i64,
    pub size: u64,
    pub dev: u64,
    pub ino: u64,
    pub nlink: u64,
    pub link_target: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum WalkItem {
    Entry(WalkEntry),
    Skipped { path: Vec<u8>, reason: SkipReason },
}

/// Pending children of one directory. `names` is sorted descending so that
/// `pop` yields the smallest name next.
struct Frame {
    abs: PathBuf,
    rel: Vec<u8>,
    depth: u32,
    names: Vec<Vec<u8>>,
}

fn mtime_ns(meta: &Metadata, abs: &Path) -> Result<i64> {
    meta.mtime()
        .checked_mul(1_000_000_000)
        .and_then(|s| s.checked_add(meta.mtime_nsec()))
        .ok_or_else(|| {
            Error::io(
                abs,
                io::Error::new(io::ErrorKind::InvalidData, "mtime out of range"),
            )
        })
}

fn read_children(abs: &Path) -> Result<Vec<Vec<u8>>> {
    let rd = fs::read_dir(abs).map_err(|e| Error::io(abs, e))?;
    let mut names = Vec::new();
    for ent in rd {
        let ent = ent.map_err(|e| Error::io(abs, e))?;
        names.push(ent.file_name().into_vec());
    }
    names.sort_unstable_by(|a, b| b.cmp(a));
    Ok(names)
}

pub fn walk(
    root: &Path,
    opts: &WalkOptions,
    mut sink: impl FnMut(WalkItem) -> Result<()>,
) -> Result<()> {
    let root_meta = fs::symlink_metadata(root).map_err(|e| Error::io(root, e))?;
    if !root_meta.is_dir() {
        return Err(Error::io(
            root,
            io::Error::new(io::ErrorKind::InvalidInput, "walk root is not a directory"),
        ));
    }
    let root_dev = root_meta.dev();

    sink(WalkItem::Entry(WalkEntry {
        depth: 0,
        path: Vec::new(),
        name: Vec::new(),
        kind: EntryKind::Dir,
        mode: root_meta.mode() & 0o7777,
        mtime_ns: mtime_ns(&root_meta, root)?,
        size: root_meta.size(),
        dev: root_dev,
        ino: root_meta.ino(),
        nlink: root_meta.nlink(),
        link_target: None,
    }))?;

    let mut stack = vec![Frame {
        abs: root.to_path_buf(),
        rel: Vec::new(),
        depth: 0,
        names: read_children(root)?,
    }];

    while let Some(top) = stack.last_mut() {
        let Some(name) = top.names.pop() else {
            stack.pop();
            continue;
        };
        let abs = top.abs.join(OsStr::from_bytes(&name));
        let mut rel = top.rel.clone();
        if !rel.is_empty() {
            rel.push(b'/');
        }
        rel.extend_from_slice(&name);
        let depth = top.depth + 1;

        let meta = fs::symlink_metadata(&abs).map_err(|e| Error::io(&abs, e))?;
        let ft = meta.file_type();
        let kind = if ft.is_dir() {
            EntryKind::Dir
        } else if ft.is_file() {
            EntryKind::File
        } else if ft.is_symlink() {
            EntryKind::Symlink
        } else {
            let reason = if ft.is_fifo() {
                SkipReason::Fifo
            } else if ft.is_socket() {
                SkipReason::Socket
            } else if ft.is_char_device() {
                SkipReason::CharDevice
            } else {
                SkipReason::BlockDevice
            };
            sink(WalkItem::Skipped { path: rel, reason })?;
            continue;
        };

        if kind == EntryKind::Dir && !opts.cross_fs && meta.dev() != root_dev {
            sink(WalkItem::Skipped {
                path: rel,
                reason: SkipReason::OtherFilesystem,
            })?;
            continue;
        }

        let link_target = if kind == EntryKind::Symlink {
            let t = fs::read_link(&abs).map_err(|e| Error::io(&abs, e))?;
            Some(t.as_os_str().as_bytes().to_vec())
        } else {
            None
        };

        sink(WalkItem::Entry(WalkEntry {
            depth,
            path: rel.clone(),
            name,
            kind,
            mode: meta.mode() & 0o7777,
            mtime_ns: mtime_ns(&meta, &abs)?,
            size: meta.size(),
            dev: meta.dev(),
            ino: meta.ino(),
            nlink: meta.nlink(),
            link_target,
        }))?;

        if kind == EntryKind::Dir {
            let names = read_children(&abs)?;
            stack.push(Frame {
                abs,
                rel,
                depth,
                names,
            });
        }
    }
    Ok(())
}
