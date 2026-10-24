//! Restore preflight (spec §9 step 1): probe the destination filesystem and
//! refuse any tree it would silently alter: names that collide or would be
//! rewritten, and paths longer than it allows.

use crate::error::{Error, Result};
use crate::ids::Id;
use crate::model::EntryRow;
use std::collections::HashMap;
use std::ffi::{CString, OsStr};
use std::fs::{self, File};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use unicode_normalization::UnicodeNormalization;

/// What the destination filesystem does to names, times and paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsProbe {
    /// `wmkProbe` and `WMKPROBE` name the same file.
    pub case_insensitive: bool,
    /// The NFC and NFD spellings of a name are the same file (APFS, even
    /// when case-sensitive).
    pub normalization_insensitive: bool,
    /// A name that is not UTF-8 can be created and reads back unchanged.
    pub accepts_non_utf8: bool,
    /// Names read back in a different form than written (HFS+ stores NFD).
    pub normalizes_names: bool,
    /// The granularity of stored mtimes, a power of 10 from 1 to 10^9.
    pub mtime_precision_ns: u64,
    /// `pathconf(_PC_PATH_MAX)`: the longest path in bytes, counting the NUL.
    pub path_max: usize,
}

const NFC_E: &[u8] = b"\xc3\xa9";
const NFD_E: &[u8] = b"e\xcc\x81";
const NON_UTF8: &[u8] = b"\xff";
/// An mtime with a nonzero digit at every sub-second place.
const PROBE_MTIME_NS: i64 = 1_700_000_000_123_456_789;

/// Removes the probe directory when dropped, so errors cannot leave it.
struct ProbeDir {
    path: PathBuf,
    armed: bool,
}

impl ProbeDir {
    /// Remove the directory now, reporting failure.
    fn remove(mut self) -> Result<()> {
        self.armed = false;
        fs::remove_dir_all(&self.path).map_err(|e| Error::io(&self.path, e))
    }
}

impl Drop for ProbeDir {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn names(dir: &Path) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    for e in fs::read_dir(dir).map_err(|e| Error::io(dir, e))? {
        out.push(
            e.map_err(|e| Error::io(dir, e))?
                .file_name()
                .as_bytes()
                .to_vec(),
        );
    }
    Ok(out)
}

fn create(path: &Path) -> std::io::Result<File> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}

/// Probe the filesystem holding `dir` (the destination's parent, which must
/// exist) inside a `.wmk-probe-<uuid>` directory that is always removed.
pub fn probe(dir: &Path) -> Result<FsProbe> {
    let path = dir.join(format!(".wmk-probe-{}", Id::new_v7()));
    fs::create_dir(&path).map_err(|e| Error::io(&path, e))?;
    let pd = ProbeDir { path, armed: true };
    let d = pd.path.as_path();

    let lower = d.join("wmkProbe");
    create(&lower).map_err(|e| Error::io(&lower, e))?;
    let case_insensitive = fs::symlink_metadata(d.join("WMKPROBE")).is_ok();

    // Any refusal (EILSEQ on APFS, EINVAL elsewhere) means "not accepted",
    // as does a name that reads back altered.
    let raw = d.join(OsStr::from_bytes(NON_UTF8));
    let accepts_non_utf8 = create(&raw).is_ok() && names(d)?.iter().any(|n| n == NON_UTF8);

    let nfc = d.join(OsStr::from_bytes(NFC_E));
    create(&nfc).map_err(|e| Error::io(&nfc, e))?;
    let normalization_insensitive = fs::symlink_metadata(d.join(OsStr::from_bytes(NFD_E))).is_ok();
    let listed = names(d)?;
    let normalizes_names = !listed.iter().any(|n| n == NFC_E);
    if normalizes_names && !listed.iter().any(|n| n == NFD_E) {
        // `check` can only predict an NFD rewrite.
        return Err(Error::Preflight(vec![format!(
            "destination filesystem rewrites names in an unknown way: {} reads back as {}",
            NFC_E.escape_ascii(),
            listed
                .iter()
                .map(|n| n.escape_ascii().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )]));
    }

    let mtime_precision_ns = probe_mtime_precision(&lower)?;
    let path_max = path_max(dir)?;

    pd.remove()?;
    Ok(FsProbe {
        case_insensitive,
        normalization_insensitive,
        accepts_non_utf8,
        normalizes_names,
        mtime_precision_ns,
        path_max,
    })
}

/// Set an mtime with a nonzero digit at every sub-second place and read it
/// back: the precision is the largest power of 10 (at most 10^9) that
/// divides the stored sub-second part, or 1 when it was kept exactly.
fn probe_mtime_precision(file: &Path) -> Result<u64> {
    let set = filetime::FileTime::from_unix_time(
        PROBE_MTIME_NS.div_euclid(1_000_000_000),
        PROBE_MTIME_NS.rem_euclid(1_000_000_000) as u32,
    );
    filetime::set_file_mtime(file, set).map_err(|e| Error::io(file, e))?;
    let md = fs::metadata(file).map_err(|e| Error::io(file, e))?;
    let got = filetime::FileTime::from_last_modification_time(&md);
    Ok(precision_of(got.nanoseconds()))
}

fn precision_of(subsec_ns: u32) -> u64 {
    let sub = u64::from(subsec_ns);
    let mut p = 1u64;
    while p < 1_000_000_000 && sub % (p * 10) == 0 {
        p *= 10;
    }
    p
}

fn path_max(dir: &Path) -> Result<usize> {
    let c = CString::new(dir.as_os_str().as_bytes())
        .map_err(|_| Error::Preflight(vec![format!("path contains NUL: {}", dir.display())]))?;
    // SAFETY: `c` is a valid NUL-terminated path for the duration of the call.
    let v = unsafe { libc::pathconf(c.as_ptr(), libc::_PC_PATH_MAX) };
    // -1: no limit reported (the probe has just written in `dir`, so it
    // exists); fall back to the OS constant.
    Ok(if v > 0 {
        v as usize
    } else {
        libc::PATH_MAX as usize
    })
}

/// The key under which a filesystem considers two sibling names the same,
/// or `None` when it compares raw bytes (no collision is possible).
fn collision_key(name: &[u8], probe: &FsProbe) -> Option<Vec<u8>> {
    if !probe.case_insensitive && !probe.normalization_insensitive {
        return None;
    }
    let Ok(s) = std::str::from_utf8(name) else {
        return Some(name.to_vec());
    };
    let nfd: String = s.nfd().collect();
    Some(if probe.case_insensitive {
        nfd.to_lowercase().into_bytes()
    } else {
        nfd.into_bytes()
    })
}

/// Whether the filesystem would store `name` under different bytes.
fn not_representable(name: &[u8], probe: &FsProbe) -> bool {
    match std::str::from_utf8(name) {
        Err(_) => !probe.accepts_non_utf8,
        // `probe` only accepts a normalizing filesystem that rewrites to NFD.
        Ok(s) => probe.normalizes_names && !s.nfd().eq(s.chars()),
    }
}

/// Check the restored set against `probe`. `entries` carry their restored
/// paths relative to the destination (the destination itself has path
/// `""` and is skipped: its name is the caller's). `dest_abs_len` is the
/// byte length of the absolute directory the entries are written under.
/// An empty result means the tree can be restored byte-exact.
pub fn check(entries: &[EntryRow], probe: &FsProbe, dest_abs_len: usize) -> Vec<String> {
    let mut msgs = Vec::new();
    let mut seen: HashMap<(Option<Id>, Vec<u8>), &[u8]> = HashMap::new();
    for e in entries {
        if e.path.is_empty() {
            continue;
        }
        if let Some(key) = collision_key(&e.name, probe) {
            match seen.get(&(e.parent_id, key.clone())) {
                Some(first) => msgs.push(format!(
                    "case collision: {} vs {}",
                    first.escape_ascii(),
                    e.path.escape_ascii()
                )),
                None => {
                    seen.insert((e.parent_id, key), &e.path);
                }
            }
        }
        if not_representable(&e.name, probe) {
            msgs.push(format!("name not representable: {}", e.path.escape_ascii()));
        }
        let len = dest_abs_len + 1 + e.path.len();
        if len >= probe.path_max {
            msgs.push(format!(
                "path too long ({len} > {}): {}",
                probe.path_max.saturating_sub(1),
                e.path.escape_ascii()
            ));
        }
    }
    msgs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precision_is_the_largest_dividing_power_of_ten() {
        assert_eq!(precision_of(123_456_789), 1);
        assert_eq!(precision_of(123_456_700), 100);
        assert_eq!(precision_of(123_456_000), 1_000);
        assert_eq!(precision_of(0), 1_000_000_000);
    }
}
