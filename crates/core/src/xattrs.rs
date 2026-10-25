//! Extended attributes as recorded at ingest and hashed by the manifest.
//! Ingest and `manifest_dir` share this logic so they cannot disagree.

use crate::error::{Error, Result};
use crate::manifest::xattr_recorded;
use crate::model::EntryKind;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;
use xattr::{FileExt, XAttrs};

/// The xattrs of `path`, without following a symlink (`xattr::list` and
/// `xattr::get`, never the `_deref` variants). See `collect` for the rules.
pub(crate) fn read_xattrs(path: &Path, kind: EntryKind) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    collect(xattr::list(path), |n| xattr::get(path, n), kind, path)
}

/// The xattrs of an open regular file, read through its descriptor so they
/// come from the inode that was checked and read. `path` names the file in
/// errors only.
pub(crate) fn read_file_xattrs(file: &File, path: &Path) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    collect(
        file.list_xattr(),
        |n| file.get_xattr(n),
        EntryKind::File,
        path,
    )
}

/// Sorted bytewise by name, keeping only the names `xattr_recorded`
/// accepts (the spec §4 namespace policy). A filesystem without xattr
/// support (`ENOTSUP`, or `EOPNOTSUPP`, which differs from it on macOS),
/// and a symlink whose xattrs the OS refuses to list (`EPERM`), both yield
/// none.
fn collect(
    names: io::Result<XAttrs>,
    get: impl Fn(&OsStr) -> io::Result<Option<Vec<u8>>>,
    kind: EntryKind,
    path: &Path,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let names = match names {
        Ok(n) => n,
        Err(e) => {
            let code = e.raw_os_error();
            let unsupported = code == Some(libc::ENOTSUP) || code == Some(libc::EOPNOTSUPP);
            let link_perm = kind == EntryKind::Symlink && code == Some(libc::EPERM);
            if unsupported || link_perm {
                return Ok(Vec::new());
            }
            return Err(Error::io(path, e));
        }
    };
    let mut out = Vec::new();
    for name in names {
        if !xattr_recorded(name.as_bytes()) {
            continue;
        }
        match get(&name) {
            Ok(Some(v)) => out.push((name.into_vec(), v)),
            Ok(None) => {}
            Err(e) => return Err(Error::io(path, e)),
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}
