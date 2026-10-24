//! Extended attributes as recorded at ingest and hashed by the manifest.
//! Ingest and `manifest_dir` share this reader so they cannot disagree.

use crate::error::{Error, Result};
use crate::manifest::IGNORED_XATTRS;
use crate::model::EntryKind;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;

/// The xattrs of `path`, sorted bytewise by name, without following a
/// symlink (`xattr::list` and `xattr::get`, never the `_deref` variants).
/// `IGNORED_XATTRS` are dropped. A filesystem without xattr support, and a
/// symlink whose xattrs the OS refuses to list (`EPERM`), both yield none.
pub(crate) fn read_xattrs(path: &Path, kind: EntryKind) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
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
        if IGNORED_XATTRS.contains(&name.as_bytes()) {
            continue;
        }
        match xattr::get(path, &name) {
            Ok(Some(v)) => out.push((name.into_vec(), v)),
            Ok(None) => {}
            Err(e) => return Err(Error::io(path, e)),
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}
