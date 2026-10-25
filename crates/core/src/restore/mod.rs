//! Restore (spec §9): rebuild a snapshot byte-exact.
//!
//! Nothing is written until the preflight passes. Everything is then built
//! inside `<dest>.wmk-partial-<snapshot-id>` (dirs at `0o700`, files at
//! `0o600`), every unit and every whole file is verified against its BLAKE3,
//! metadata is applied in the §9 order, and the partial is renamed into
//! place. The restored root's own mode and mtime are applied last, after
//! the rename. Any failure leaves the partial for inspection.

pub mod preflight;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::{EntryKind, EntryRow, SnapshotStatus, UnitRow};
use crate::repo::Repo;
use crate::snaptree;
use crate::unitread::{UnitReader, DEFAULT_CACHE_BYTES};
use filetime::FileTime;
use preflight::FsProbe;
use rayon::prelude::*;
use std::collections::HashMap;
use std::ffi::{CString, OsStr};
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
    /// The snapshot path to restore as `dest`; empty restores the whole tree.
    pub path: Vec<u8>,
    /// Re-create hard-link groups with two or more members in the restored
    /// set; otherwise every member is an independent copy.
    pub hardlinks: bool,
    /// Restore extended attributes.
    pub xattrs: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    /// Logical bytes of all restored file entries.
    pub bytes: u64,
    /// Entries skipped because their status is neither `ok` nor `unstable`,
    /// counting every entry beneath a withheld directory.
    pub withheld: u64,
    /// The destination's mtime granularity; mtimes finer than this were
    /// floored by the filesystem.
    pub mtime_precision_ns: u64,
    pub elapsed_ms: u64,
}

/// Restore `snapshot` (or its subtree at `opts.path`) to `dest`, which must
/// not exist; its parent must.
pub fn run(repo: &Repo, snapshot: Id, dest: &Path, opts: &RestoreOptions) -> Result<RestoreReport> {
    run_with_probe(repo, snapshot, dest, opts, None)
}

/// `run`, with `Some(probe)` standing in for probing the destination: the
/// test seam for simulating other filesystems.
#[doc(hidden)]
pub fn run_with_probe(
    repo: &Repo,
    snapshot: Id,
    dest: &Path,
    opts: &RestoreOptions,
    probe: Option<FsProbe>,
) -> Result<RestoreReport> {
    let started = Instant::now();
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let snap = cat.snapshot(snapshot)?;
    if snap.status != SnapshotStatus::Complete {
        return Err(Error::NotFound(format!(
            "complete snapshot {snapshot} (it is {})",
            snap.status.as_str()
        )));
    }

    // Preflight: nothing is written until every check passes.
    let (parent, dest) = locate(dest)?;
    if exists(&dest)? {
        return Err(Error::Preflight(vec![format!(
            "destination exists: {}",
            dest.display()
        )]));
    }
    let partial = partial_path(&dest, snapshot);
    if exists(&partial)? {
        return Err(Error::Preflight(vec![format!(
            "partial restore {} exists from an earlier attempt; inspect or remove it",
            partial.display()
        )]));
    }
    let (entries, withheld) = collect(&cat, snapshot, &opts.path)?;
    let probe = match probe {
        Some(p) => p,
        None => preflight::probe(&parent)?,
    };
    // Everything is written under the partial path, the longer of the two.
    let msgs = preflight::check(&entries, &probe, partial.as_os_str().len());
    if !msgs.is_empty() {
        return Err(Error::Preflight(msgs));
    }
    let plan = plan(&cat, &entries, opts.hardlinks)?;

    // Write.
    let at = |rel: &[u8]| -> PathBuf {
        if rel.is_empty() {
            partial.clone()
        } else {
            partial.join(OsStr::from_bytes(rel))
        }
    };
    for e in entries.iter().filter(|e| e.kind == EntryKind::Dir) {
        let p = at(&e.path);
        DirBuilder::new()
            .mode(0o700)
            .create(&p)
            .map_err(|err| Error::io(&p, err))?;
    }
    let reader = UnitReader::new(repo, DEFAULT_CACHE_BYTES);
    plan.files
        .par_iter()
        .try_for_each(|job| write_file(&reader, &at(&entries[job.entry].path), job))?;

    // Finalize, in the §9 order.
    for e in entries.iter().filter(|e| e.kind == EntryKind::Symlink) {
        let p = at(&e.path);
        let target = e.link_target.as_deref().unwrap_or_default();
        std::os::unix::fs::symlink(OsStr::from_bytes(target), &p)
            .map_err(|err| Error::io(&p, err))?;
    }
    for &(member, first) in &plan.links {
        let p = at(&entries[member].path);
        fs::hard_link(at(&entries[first].path), &p).map_err(|err| Error::io(&p, err))?;
    }
    if opts.xattrs {
        // Before modes: setting an xattr can need write permission.
        let index: HashMap<Id, usize> =
            entries.iter().enumerate().map(|(i, e)| (e.id, i)).collect();
        cat.for_each_snapshot_xattr(snapshot, |id, name, value| {
            let Some(&i) = index.get(&id) else {
                return Ok(());
            };
            let p = at(&entries[i].path);
            xattr::set(&p, OsStr::from_bytes(&name), &value).map_err(|err| Error::io(&p, err))
        })?;
    }
    // The root (index 0) is finished after the rename.
    for e in entries[1..].iter().filter(|e| e.kind != EntryKind::Dir) {
        set_meta(&at(&e.path), e)?;
    }
    // Reverse pre-order puts every directory after all of its descendants.
    for e in entries[1..]
        .iter()
        .rev()
        .filter(|e| e.kind == EntryKind::Dir)
    {
        set_meta(&at(&e.path), e)?;
    }
    rename_into_place(&partial, &dest)?;
    set_meta(&dest, &entries[0])?;

    let count = |k: EntryKind| entries.iter().filter(|e| e.kind == k).count() as u64;
    Ok(RestoreReport {
        files: count(EntryKind::File),
        dirs: count(EntryKind::Dir),
        symlinks: count(EntryKind::Symlink),
        bytes: plan.bytes,
        withheld,
        mtime_precision_ns: probe.mtime_precision_ns,
        elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

/// The canonical parent of `dest` and the absolute destination path.
fn locate(dest: &Path) -> Result<(PathBuf, PathBuf)> {
    let name = dest.file_name().ok_or_else(|| {
        Error::Preflight(vec![format!(
            "destination has no final name: {}",
            dest.display()
        )])
    })?;
    let parent = match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let parent = parent.canonicalize().map_err(|e| Error::io(parent, e))?;
    let dest = parent.join(name);
    Ok((parent, dest))
}

fn partial_path(dest: &Path, snapshot: Id) -> PathBuf {
    let mut name = dest.as_os_str().to_os_string();
    name.push(format!(".wmk-partial-{snapshot}"));
    PathBuf::from(name)
}

/// Whether anything, a dangling symlink included, is at `p`.
fn exists(p: &Path) -> Result<bool> {
    match fs::symlink_metadata(p) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(Error::io(p, e)),
    }
}

/// The restored set in canonical pre-order, with paths re-based onto the
/// entry at `root_path` (which gets path `""`), and the withheld count:
/// every entry that is withheld or beneath a withheld directory.
fn collect(cat: &Catalog, snapshot: Id, root_path: &[u8]) -> Result<(Vec<EntryRow>, u64)> {
    let mut withheld = 0u64;
    let mut out = Vec::new();
    snaptree::traverse(cat, snapshot, root_path, |n| {
        if !n.included {
            if out.is_empty() {
                return Err(Error::NotFound(format!(
                    "path {} is withheld in snapshot {snapshot}",
                    root_path.escape_ascii()
                )));
            }
            withheld += 1;
            return Ok(());
        }
        let mut e = n.row;
        e.path = n.rel;
        validate(&e)?;
        out.push(e);
        Ok(())
    })?;
    Ok((out, withheld))
}

/// Reject rows the restore cannot reproduce exactly.
fn validate(e: &EntryRow) -> Result<()> {
    let bad = |what: &str| {
        Err(Error::Corrupt(format!(
            "entry {} {what}",
            e.path.escape_ascii()
        )))
    };
    match e.kind {
        EntryKind::File if e.content_hash.is_none() => bad("has no content hash"),
        EntryKind::File if e.size.is_none_or(|s| s < 0) => bad("has no size"),
        EntryKind::Symlink if e.link_target.is_none() => bad("has no link target"),
        _ => Ok(()),
    }
}

struct FileJob {
    /// Index into the restored entries.
    entry: usize,
    content: Hash32,
    units: Arc<Vec<UnitRow>>,
}

struct Plan {
    /// Files to write, in pack order.
    files: Vec<FileJob>,
    /// `(member, first)`: hard-link `member` to the already written `first`.
    links: Vec<(usize, usize)>,
    bytes: u64,
}

fn plan(cat: &Catalog, entries: &[EntryRow], hardlinks: bool) -> Result<Plan> {
    let mut members: HashMap<i64, u32> = HashMap::new();
    if hardlinks {
        for e in entries.iter().filter(|e| e.kind == EntryKind::File) {
            if let Some(g) = e.hardlink_group {
                *members.entry(g).or_insert(0) += 1;
            }
        }
    }
    let mut firsts: HashMap<i64, usize> = HashMap::new();
    let mut units: HashMap<Hash32, Arc<Vec<UnitRow>>> = HashMap::new();
    let mut plan = Plan {
        files: Vec::new(),
        links: Vec::new(),
        bytes: 0,
    };
    for (i, e) in entries.iter().enumerate() {
        if e.kind != EntryKind::File {
            continue;
        }
        let (Some(content), Some(size)) = (e.content_hash, e.size) else {
            continue; // `validate` already refused these
        };
        plan.bytes += size as u64;
        let group = e
            .hardlink_group
            .filter(|g| members.get(g).copied().unwrap_or(0) >= 2);
        if let Some(g) = group {
            if let Some(&first) = firsts.get(&g) {
                if entries[first].content_hash != Some(content) {
                    return Err(Error::Corrupt(format!(
                        "hard-link group {g} members {} and {} have different content",
                        entries[first].path.escape_ascii(),
                        e.path.escape_ascii()
                    )));
                }
                plan.links.push((i, first));
                continue;
            }
            firsts.insert(g, i);
        }
        let units = match units.get(&content) {
            Some(u) => u.clone(),
            None => {
                let u = Arc::new(cat.content_units(&content)?);
                units.insert(content, u.clone());
                u
            }
        };
        plan.files.push(FileJob {
            entry: i,
            content,
            units,
        });
    }
    // Empty files (no units) first, then by the first unit's pack position.
    plan.files
        .sort_by_key(|j| j.units.first().map(|u| (u.pack_id, u.pack_offset)));
    Ok(plan)
}

fn write_file(reader: &UnitReader, path: &Path, job: &FileJob) -> Result<()> {
    let f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| Error::io(path, e))?;
    let mut w = BufWriter::with_capacity(1 << 20, f);
    let got = reader
        .write_content(&job.units, &mut w)
        .map_err(|e| match e {
            Error::Io { path: p, source } if p.as_os_str().is_empty() => Error::io(path, source),
            e => e,
        })?;
    w.flush().map_err(|e| Error::io(path, e))?;
    if got != job.content {
        return Err(Error::Corrupt(format!(
            "restored file {} hashes to {got}, catalog says {}",
            path.display(),
            job.content
        )));
    }
    Ok(())
}

fn file_time(ns: i64) -> FileTime {
    FileTime::from_unix_time(
        ns.div_euclid(1_000_000_000),
        ns.rem_euclid(1_000_000_000) as u32,
    )
}

/// Mode (`& 0o1777`, so never setuid/setgid) then mtime. A symlink gets its
/// times only, best effort: atime = mtime, skipped where unsupported.
fn set_meta(path: &Path, e: &EntryRow) -> Result<()> {
    let t = file_time(e.mtime_ns);
    if e.kind == EntryKind::Symlink {
        return match filetime::set_symlink_file_times(path, t, t) {
            Err(err) if is_unsupported(&err) => Ok(()),
            r => r.map_err(|err| Error::io(path, err)),
        };
    }
    fs::set_permissions(path, fs::Permissions::from_mode(e.mode & 0o1777))
        .map_err(|err| Error::io(path, err))?;
    filetime::set_file_mtime(path, t).map_err(|err| Error::io(path, err))
}

fn is_unsupported(e: &io::Error) -> bool {
    e.raw_os_error()
        .is_some_and(|c| [libc::ENOSYS, libc::ENOTSUP, libc::EOPNOTSUPP].contains(&c))
}

/// `rename(from, to)` that never replaces an existing `to`. Falls back to
/// check-then-rename only where the filesystem lacks the no-replace form.
fn rename_into_place(from: &Path, to: &Path) -> Result<()> {
    match rename_noreplace(from, to) {
        Ok(()) => Ok(()),
        Err(e) if is_unsupported(&e) || e.raw_os_error() == Some(libc::EINVAL) => {
            if exists(to)? {
                return Err(Error::io(to, io::Error::from(io::ErrorKind::AlreadyExists)));
            }
            fs::rename(from, to).map_err(|e| Error::io(to, e))
        }
        Err(e) => Err(Error::io(to, e)),
    }
}

fn cstring(p: &Path) -> io::Result<CString> {
    CString::new(p.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}

#[cfg(target_os = "linux")]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    let (f, t) = (cstring(from)?, cstring(to)?);
    // SAFETY: both are valid NUL-terminated paths for the duration of the call.
    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            f.as_ptr(),
            libc::AT_FDCWD,
            t.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    let (f, t) = (cstring(from)?, cstring(to)?);
    // SAFETY: both are valid NUL-terminated paths for the duration of the call.
    let rc = unsafe { libc::renamex_np(f.as_ptr(), t.as_ptr(), libc::RENAME_EXCL) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
