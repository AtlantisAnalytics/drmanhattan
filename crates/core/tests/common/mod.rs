#![allow(dead_code)]
//! Test-only helper that builds a directory tree with exact modes and mtimes.

use std::ffi::OsStr;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};

use filetime::FileTime;

pub struct TreeBuilder {
    root: PathBuf,
    modes: Vec<(Vec<u8>, u32)>,
    mtimes: Vec<(Vec<u8>, i64)>,
}

fn join(root: &Path, rel: &[u8]) -> PathBuf {
    root.join(OsStr::from_bytes(rel))
}

impl TreeBuilder {
    pub fn new(root: &Path) -> TreeBuilder {
        TreeBuilder {
            root: root.to_path_buf(),
            modes: Vec::new(),
            mtimes: Vec::new(),
        }
    }

    pub fn file(&mut self, path: &[u8], bytes: &[u8]) -> &mut Self {
        let p = join(&self.root, path);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        fs::write(&p, bytes).expect("write file");
        self
    }

    pub fn dir(&mut self, path: &[u8]) -> &mut Self {
        fs::create_dir_all(join(&self.root, path)).expect("create dir");
        self
    }

    pub fn symlink(&mut self, path: &[u8], target: &[u8]) -> &mut Self {
        let p = join(&self.root, path);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        symlink(OsStr::from_bytes(target), &p).expect("symlink");
        self
    }

    pub fn hardlink(&mut self, path: &[u8], existing: &[u8]) -> &mut Self {
        let p = join(&self.root, path);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        fs::hard_link(join(&self.root, existing), &p).expect("hard_link");
        self
    }

    pub fn fifo(&mut self, path: &[u8]) -> &mut Self {
        let p = join(&self.root, path);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        let c = std::ffi::CString::new(p.as_os_str().as_bytes()).expect("cstring");
        // SAFETY: c is a valid NUL-terminated path for the duration of the call.
        let rc = unsafe { libc::mkfifo(c.as_ptr(), 0o644) };
        assert_eq!(rc, 0, "mkfifo: {}", std::io::Error::last_os_error());
        self
    }

    pub fn mode(&mut self, path: &[u8], mode: u32) -> &mut Self {
        self.modes.push((path.to_vec(), mode));
        self
    }

    pub fn mtime(&mut self, path: &[u8], ns: i64) -> &mut Self {
        self.mtimes.push((path.to_vec(), ns));
        self
    }

    /// Applies queued modes and mtimes deepest-first (mode before mtime per
    /// path), so directory mtimes are not disturbed by later child changes.
    pub fn finish(&mut self) {
        let mut paths: Vec<Vec<u8>> = self
            .modes
            .iter()
            .map(|(p, _)| p.clone())
            .chain(self.mtimes.iter().map(|(p, _)| p.clone()))
            .collect();
        paths.sort();
        paths.dedup();
        let depth = |p: &Vec<u8>| p.iter().filter(|&&b| b == b'/').count();
        paths.sort_by_key(|p| std::cmp::Reverse(depth(p)));
        for p in paths {
            let full = join(&self.root, &p);
            if let Some((_, m)) = self.modes.iter().rev().find(|(q, _)| *q == p) {
                fs::set_permissions(&full, fs::Permissions::from_mode(*m)).expect("chmod");
            }
            if let Some((_, ns)) = self.mtimes.iter().rev().find(|(q, _)| *q == p) {
                let ft = FileTime::from_unix_time(
                    ns.div_euclid(1_000_000_000),
                    ns.rem_euclid(1_000_000_000) as u32,
                );
                let is_link = fs::symlink_metadata(&full)
                    .expect("lstat")
                    .file_type()
                    .is_symlink();
                if is_link {
                    filetime::set_symlink_file_times(&full, ft, ft).expect("set link times");
                } else {
                    filetime::set_file_times(&full, ft, ft).expect("set times");
                }
            }
        }
        self.modes.clear();
        self.mtimes.clear();
    }
}
