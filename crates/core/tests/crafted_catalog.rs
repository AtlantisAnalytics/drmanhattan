//! A crafted catalog must never steer a traversal (spec §6.4 paths): every
//! catalog walk derives each path from its parent's and rejects names that
//! could escape the tree, stored paths that disagree, and cycles. Restore,
//! `manifest_snapshot` and diff all fail with `Corrupt`, and restore writes
//! nothing at all.

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::diff::{self, DiffOptions};
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::Id;
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::manifest::{manifest_snapshot, ManifestOptions};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::restore::{self, RestoreOptions};
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

struct Case {
    _tmp: tempfile::TempDir,
    repo_dir: PathBuf,
    src: PathBuf,
    /// Empty directory that restore writes its destination into.
    out: PathBuf,
    snapshot: Id,
    /// A path outside the destination that a crafted row points at.
    escape: PathBuf,
}

/// Ingest `a/f`, `g` and an empty `a/d/`, then apply `craft` to the catalog.
fn case(craft: impl FnOnce(&rusqlite::Connection, &Case)) -> Case {
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("r");
    let repo = Repo::init(&repo_dir).unwrap();
    let src = tmp.path().join("src");
    TreeBuilder::new(&src)
        .file(b"a/f", b"payload")
        .file(b"g", b"other")
        .dir(b"a/d");
    let r = ingest::run(&repo, &src, &IngestOptions::default()).unwrap();
    let out = tmp.path().join("out");
    fs::create_dir(&out).unwrap();
    let escape = tmp.path().join("escaped");
    let c = Case {
        _tmp: tmp,
        repo_dir,
        src,
        out,
        snapshot: r.snapshot,
        escape,
    };
    let conn = rusqlite::Connection::open(c.repo_dir.join("catalog.sqlite")).unwrap();
    craft(&conn, &c);
    c
}

/// `UPDATE entry SET <set> WHERE path = <path>` with `?1` = the new value.
fn set(conn: &rusqlite::Connection, path: &[u8], set: &str, value: &[u8]) {
    let n = conn
        .execute(
            &format!("UPDATE entry SET {set} WHERE path = ?2"),
            rusqlite::params![value, path],
        )
        .unwrap();
    assert_eq!(n, 1, "no row at {}", path.escape_ascii());
}

fn id_of(conn: &rusqlite::Connection, path: &[u8]) -> Vec<u8> {
    conn.query_row("SELECT id FROM entry WHERE path = ?1", [path], |r| r.get(0))
        .unwrap()
}

/// Run `f` on another thread; a traversal that loops fails the test
/// instead of hanging it.
fn within<T: Send + 'static>(what: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| panic!("{what} did not finish: the traversal loops"))
}

fn assert_corrupt<T: std::fmt::Debug>(what: &str, r: Result<T, Error>) {
    match r {
        Err(Error::Corrupt(m)) => assert!(m.contains("snapshot "), "{what}: message {m:?}"),
        other => panic!("{what}: expected Corrupt, got {other:?}"),
    }
}

/// Restore, `manifest_snapshot` and diff (of `path`) all refuse the
/// crafted catalog, and restore leaves no trace anywhere.
fn assert_refused(c: &Case, path: &[u8]) {
    let (repo_dir, out, snap, p) = (c.repo_dir.clone(), c.out.clone(), c.snapshot, path.to_vec());
    let r = within("restore", move || {
        let repo = Repo::open(&repo_dir).unwrap();
        let opts = RestoreOptions {
            path: p,
            hardlinks: true,
            xattrs: false,
        };
        restore::run(&repo, snap, &out.join("dest"), &opts)
    });
    assert_corrupt("restore", r);
    let left: Vec<_> = fs::read_dir(&c.out)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert!(left.is_empty(), "restore wrote {left:?}");
    assert!(!c.escape.exists(), "restore wrote outside its destination");
    let mut parent: Vec<_> = fs::read_dir(c.out.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().as_bytes().to_vec())
        .collect();
    parent.sort();
    assert_eq!(
        parent,
        [&b"out"[..], b"r", b"src"],
        "restore wrote beside out/"
    );

    let (repo_dir, snap, p) = (c.repo_dir.clone(), c.snapshot, path.to_vec());
    let r = within("manifest_snapshot", move || {
        let cat = Catalog::open_readonly(&repo_dir.join("catalog.sqlite")).unwrap();
        let opts = ManifestOptions {
            mtime_precision_ns: 1,
            cross_fs: false,
        };
        manifest_snapshot(&cat, snap, &p, &opts)
    });
    assert_corrupt("manifest_snapshot", r);

    let (repo_dir, src, snap, p) = (c.repo_dir.clone(), c.src.clone(), c.snapshot, path.to_vec());
    let r = within("diff", move || {
        let repo = Repo::open(&repo_dir).unwrap();
        let dir = if p.is_empty() {
            src
        } else {
            src.join(std::ffi::OsStr::from_bytes(&p))
        };
        let opts = DiffOptions {
            path: p,
            mtime_precision_ns: 1,
            cross_fs: false,
        };
        diff::run(&repo, snap, &dir, &opts)
    });
    assert_corrupt("diff", r);
}

fn abs(p: &Path) -> Vec<u8> {
    p.as_os_str().as_bytes().to_vec()
}

#[test]
fn absolute_path_is_corrupt() {
    let c = case(|conn, c| set(conn, b"a/f", "path = ?1", &abs(&c.escape)));
    assert_refused(&c, b"");
}

#[test]
fn dotdot_in_path_is_corrupt() {
    let c = case(|conn, _| set(conn, b"a/f", "path = ?1", b"a/../../escaped"));
    assert_refused(&c, b"");
}

#[test]
fn name_with_slash_is_corrupt() {
    // The stored path agrees with parent path + "/" + name: only the name
    // check can catch it.
    let c = case(|conn, _| {
        set(conn, b"a/f", "name = ?1", b"d/f");
        set(conn, b"a/f", "path = ?1", b"a/d/f");
    });
    assert_refused(&c, b"");
}

#[test]
fn dotdot_name_is_corrupt() {
    let c = case(|conn, _| {
        set(conn, b"a/d", "name = ?1", b"..");
        set(conn, b"a/d", "path = ?1", b"a/..");
    });
    assert_refused(&c, b"");
}

#[test]
fn nul_and_empty_names_are_corrupt() {
    let c = case(|conn, _| {
        set(conn, b"g", "name = ?1", b"g\0");
        set(conn, b"g", "path = ?1", b"g\0");
    });
    assert_refused(&c, b"");
    let c = case(|conn, _| {
        set(conn, b"a/d", "name = ?1", b"");
        set(conn, b"a/d", "path = ?1", b"a/");
    });
    assert_refused(&c, b"");
}

#[test]
fn path_disagreeing_with_parent_and_name_is_corrupt() {
    let c = case(|conn, _| set(conn, b"a/f", "path = ?1", b"a/elsewhere"));
    assert_refused(&c, b"");
}

#[test]
fn subtree_start_path_disagreeing_with_its_ancestry_is_corrupt() {
    // `a` is reached by its stored path, so only checking its ancestry
    // (root path + "/" + name) can tell the path is wrong.
    let c = case(|conn, _| {
        set(conn, b"a/f", "path = ?1", b"x/f");
        set(conn, b"a/d", "path = ?1", b"x/d");
        set(conn, b"a", "path = ?1", b"x");
    });
    assert_refused(&c, b"x");
}

#[test]
fn root_parented_by_its_child_is_corrupt() {
    // The cycle the walk would follow forever: root -> a -> root -> ...
    let c = case(|conn, _| {
        let a = id_of(conn, b"a");
        set(conn, b"", "parent_id = ?1", &a);
    });
    assert_refused(&c, b"");
}

#[test]
fn directory_parented_by_itself_is_corrupt() {
    let c = case(|conn, _| {
        let a = id_of(conn, b"a");
        set(conn, b"a", "parent_id = ?1", &a);
    });
    assert_refused(&c, b"a");
}

#[test]
fn second_root_is_corrupt() {
    let c = case(|conn, _| {
        let n = conn
            .execute(
                "UPDATE entry SET parent_id = NULL WHERE path = ?1",
                [&b"g"[..]],
            )
            .unwrap();
        assert_eq!(n, 1);
    });
    assert_refused(&c, b"");
}

#[test]
fn root_with_a_name_is_corrupt() {
    let c = case(|conn, _| set(conn, b"", "name = ?1", b"root"));
    assert_refused(&c, b"");
}

/// A name the namespace policy refuses to record on this OS.
#[cfg(target_os = "linux")]
const UNRECORDED_XATTR: &str = "security.capability";
#[cfg(not(target_os = "linux"))]
const UNRECORDED_XATTR: &str = "com.apple.provenance";

/// An honest catalog never holds an xattr outside the policy, so restore
/// refuses one before writing anything, with or without `--xattrs`.
#[test]
fn xattr_outside_namespace_policy_is_corrupt() {
    for xattrs in [false, true] {
        let c = case(|conn, _| {
            conn.execute(
                "INSERT INTO xattr(entry_id, name, value) VALUES (?1, ?2, ?3)",
                rusqlite::params![id_of(conn, b"a/f"), UNRECORDED_XATTR.as_bytes(), &b"v"[..]],
            )
            .unwrap();
        });
        let (repo_dir, out, snap) = (c.repo_dir.clone(), c.out.clone(), c.snapshot);
        let r = within("restore", move || {
            let repo = Repo::open(&repo_dir).unwrap();
            let opts = RestoreOptions {
                path: Vec::new(),
                hardlinks: true,
                xattrs,
            };
            restore::run(&repo, snap, &out.join("dest"), &opts)
        });
        match r {
            Err(Error::Corrupt(m)) => {
                assert!(m.contains("snapshot "), "xattrs={xattrs}: {m:?}");
                assert!(m.contains("a/f"), "xattrs={xattrs}: {m:?}");
                assert!(m.contains(UNRECORDED_XATTR), "xattrs={xattrs}: {m:?}");
            }
            other => panic!("xattrs={xattrs}: expected Corrupt, got {other:?}"),
        }
        let left: Vec<_> = fs::read_dir(&c.out)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(left.is_empty(), "xattrs={xattrs}: restore wrote {left:?}");
    }
}
