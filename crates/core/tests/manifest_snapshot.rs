mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::writer::{spawn_catalog_writer, CatalogOp};
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::manifest::{manifest_dir, manifest_snapshot, ManifestOptions};
use drmanhattan_core::model::{
    ContentRow, EntryKind, EntryRow, EntryStatus, SnapshotRow, SnapshotStatus,
};
use drmanhattan_core::sniff::ProfileKind;
use drmanhattan_core::walk::{walk, WalkItem, WalkOptions};
use std::collections::HashMap;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::mpsc::channel;

const T_ROOT: i64 = 1_100_000_000_000_000_000;
const T_FILE: i64 = 1_000_000_000_000_000_001;

fn opts() -> ManifestOptions {
    ManifestOptions {
        mtime_precision_ns: 1,
        cross_fs: false,
    }
}

/// Builds a catalog from a live tree (ingest does not exist yet).
/// `tweak` may alter rows before insertion.
fn catalog_of(root: &Path, db: &Path, tweak: impl Fn(&mut EntryRow)) -> Id {
    let sid = Id::new_v7();
    let (tx, rx) = channel();
    let h = spawn_catalog_writer(db.to_path_buf(), rx);
    tx.send(CatalogOp::UpsertSnapshot(SnapshotRow {
        id: sid,
        status: SnapshotStatus::Complete,
        source_root: b"/src".to_vec(),
        host: "h".into(),
        tool_version: "0".into(),
        started_at: 1,
        completed_at: None,
        note: None,
        manifest_hash: None,
        ext_manifest_hash: None,
        source_manifest_hash: None,
        record_unit: None,
        entry_count: None,
        file_bytes: None,
        screened_out: 0,
        label_pattern: None,
    }))
    .unwrap();
    let mut ids: HashMap<Vec<u8>, Id> = HashMap::new();
    let mut groups: HashMap<(u64, u64), i64> = HashMap::new();
    let mut seen_content = std::collections::HashSet::new();
    walk(root, &WalkOptions { cross_fs: false }, |item| {
        let WalkItem::Entry(e) = item else {
            return Ok(());
        };
        let id = Id::new_v7();
        let parent = if e.path.is_empty() {
            None
        } else {
            let p = e.path.iter().rposition(|&b| b == b'/');
            let pp = p.map(|i| e.path[..i].to_vec()).unwrap_or_default();
            Some(ids[&pp])
        };
        let content_hash = if e.kind == EntryKind::File {
            let bytes = std::fs::read(root.join(std::ffi::OsStr::from_bytes(&e.path)))
                .map_err(|x| Error::io(root, x))?;
            let ch = Hash32::of(&bytes);
            if seen_content.insert(ch) {
                tx.send(CatalogOp::InsertContent(
                    ContentRow {
                        hash: ch,
                        size: bytes.len() as i64,
                        format: None,
                        profile: ProfileKind::Binary,
                        unit_count: 0,
                    },
                    vec![],
                ))
                .unwrap();
            }
            Some(ch)
        } else {
            None
        };
        let n = groups.len() as i64 + 1;
        let hardlink_group = (e.kind == EntryKind::File && e.nlink > 1)
            .then(|| *groups.entry((e.dev, e.ino)).or_insert(n));
        let mut row = EntryRow {
            id,
            snapshot_id: sid,
            parent_id: parent,
            name: e.name.clone(),
            path: e.path.clone(),
            kind: e.kind,
            mode: e.mode,
            mtime_ns: e.mtime_ns,
            size: (e.kind == EntryKind::File).then_some(e.size as i64),
            content_hash,
            link_target: e.link_target.clone(),
            hardlink_group,
            status: EntryStatus::Ok,
        };
        tweak(&mut row);
        ids.insert(e.path.clone(), id);
        tx.send(CatalogOp::InsertEntry(row)).unwrap();
        // Live files may carry xattrs (e.g. com.apple.provenance on macOS,
        // security.selinux on Linux).
        let abs = root.join(std::ffi::OsStr::from_bytes(&e.path));
        for name in xattr::list(&abs).unwrap_or_default() {
            // Ingest filters with the same policy.
            if !drmanhattan_core::manifest::xattr_recorded(name.as_bytes()) {
                continue;
            }
            if let Ok(Some(v)) = xattr::get(&abs, &name) {
                tx.send(CatalogOp::InsertXattr(id, name.as_bytes().to_vec(), v))
                    .unwrap();
            }
        }
        Ok(())
    })
    .unwrap();
    drop(tx);
    h.join().unwrap().unwrap();
    sid
}

fn tree(root: &Path) {
    let mut t = TreeBuilder::new(root);
    t.file(b"a/x", b"xx")
        .hardlink(b"a/y", b"a/x")
        .hardlink(b"b", b"a/x")
        .file(b"sub/q", b"qq")
        .hardlink(b"sub/q2", b"sub/q")
        .file(b"z-last", b"z")
        .file(b"a-c", b"c")
        .symlink(b"a/l", b"x")
        .mtime(b"a/x", T_FILE)
        .mtime(b"", T_ROOT);
    t.finish();
}

#[test]
fn matches_manifest_dir_whole_and_subtrees() {
    let d = tempfile::tempdir().unwrap();
    let live = d.path().join("live");
    std::fs::create_dir(&live).unwrap();
    tree(&live);
    let db = d.path().join("c.sqlite");
    let sid = catalog_of(&live, &db, |_| {});
    let cat = Catalog::open_readonly(&db).unwrap();
    for sub in ["", "a", "sub"] {
        let dir = if sub.is_empty() {
            live.clone()
        } else {
            live.join(sub)
        };
        let want = manifest_dir(&dir, &opts()).unwrap();
        let got = manifest_snapshot(&cat, sid, sub.as_bytes(), &opts()).unwrap();
        assert_eq!(got, want, "subtree {sub:?}");
    }
    assert_eq!(
        manifest_snapshot(&cat, sid, b"", &opts()).unwrap().entries,
        11
    );
}

#[test]
fn withheld_entry_before_hardlink_group_matches_tree_without_it() {
    let d = tempfile::tempdir().unwrap();
    let full = d.path().join("full");
    let without = d.path().join("without");
    for p in [&full, &without] {
        std::fs::create_dir(p).unwrap();
    }
    let mut t = TreeBuilder::new(&full);
    t.file(b"0-withheld", b"w")
        .file(b"x", b"same")
        .hardlink(b"y", b"x")
        .mtime(b"", T_ROOT);
    t.finish();
    let mut t = TreeBuilder::new(&without);
    t.file(b"x", b"same")
        .hardlink(b"y", b"x")
        .mtime(b"", T_ROOT);
    t.finish();
    // Match mtimes of the shared entries so only the withheld entry differs.
    for n in ["x", "y"] {
        let m = std::fs::metadata(full.join(n)).unwrap();
        let ft = filetime::FileTime::from_last_modification_time(&m);
        filetime::set_file_times(without.join(n), ft, ft).unwrap();
    }
    let db = d.path().join("c.sqlite");
    let sid = catalog_of(&full, &db, |r| {
        if r.path == b"0-withheld" {
            r.status = EntryStatus::ScreenedOut;
        }
    });
    let cat = Catalog::open_readonly(&db).unwrap();
    let got = manifest_snapshot(&cat, sid, b"", &opts()).unwrap();
    let want = manifest_dir(&without, &opts()).unwrap();
    assert_eq!(got.entries, want.entries);
    assert_eq!(got.core, want.core);
    assert_eq!(got.ext, want.ext);
}

#[test]
fn withheld_or_missing_root_path_is_not_found() {
    let d = tempfile::tempdir().unwrap();
    let live = d.path().join("live");
    std::fs::create_dir(&live).unwrap();
    let mut t = TreeBuilder::new(&live);
    t.file(b"keep", b"k").file(b"drop", b"d");
    t.finish();
    let db = d.path().join("c.sqlite");
    let sid = catalog_of(&live, &db, |r| {
        if r.path == b"drop" {
            r.status = EntryStatus::ScreenedOut;
        }
    });
    let cat = Catalog::open_readonly(&db).unwrap();
    for p in [&b"drop"[..], b"nope"] {
        let err = manifest_snapshot(&cat, sid, p, &opts()).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    }
}

#[test]
fn skips_withheld_and_rejects_missing_content() {
    let d = tempfile::tempdir().unwrap();
    let live = d.path().join("live");
    std::fs::create_dir(&live).unwrap();
    let mut t = TreeBuilder::new(&live);
    t.file(b"keep", b"k").file(b"drop", b"d");
    t.finish();
    let db = d.path().join("c.sqlite");
    let sid = catalog_of(&live, &db, |r| {
        if r.path == b"drop" {
            r.status = EntryStatus::ScreenedOut;
        }
    });
    let cat = Catalog::open_readonly(&db).unwrap();
    assert_eq!(
        manifest_snapshot(&cat, sid, b"", &opts()).unwrap().entries,
        2
    );

    let db2 = d.path().join("c2.sqlite");
    let sid2 = catalog_of(&live, &db2, |r| {
        if r.path == b"keep" {
            r.content_hash = None;
        }
    });
    let cat2 = Catalog::open_readonly(&db2).unwrap();
    let err = manifest_snapshot(&cat2, sid2, b"", &opts()).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err:?}");
}
