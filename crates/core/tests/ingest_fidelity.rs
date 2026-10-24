//! Ingest fidelity: hard links, xattrs, unreadable and special files, and the
//! snapshot record. Failpoint-driven cases live in `ingest_failpoints.rs`.

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::Hash32;
use drmanhattan_core::ingest::record::read_record;
use drmanhattan_core::ingest::{self, IngestOptions, IngestReport};
use drmanhattan_core::manifest::{manifest_dir, ManifestOptions};
use drmanhattan_core::model::{EntryRow, SnapshotStatus};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::unitread::{UnitReader, DEFAULT_CACHE_BYTES};
use drmanhattan_core::walk::SkipReason;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[cfg(target_os = "macos")]
const XATTR: &str = "com.example.test";
#[cfg(not(target_os = "macos"))]
const XATTR: &str = "user.test";

fn opts() -> IngestOptions {
    IngestOptions {
        workers: 4,
        ..IngestOptions::default()
    }
}

fn mopts() -> ManifestOptions {
    ManifestOptions {
        mtime_precision_ns: 1,
        cross_fs: false,
    }
}

fn setup() -> (TempDir, Repo, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir(&src).unwrap();
    (tmp, repo, src)
}

fn ingest(repo: &Repo, src: &Path) -> IngestReport {
    ingest::run(repo, src, &opts()).unwrap()
}

fn only_snapshot_status(repo: &Repo) -> SnapshotStatus {
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let snaps = cat.snapshots().unwrap();
    assert_eq!(snaps.len(), 1);
    snaps[0].status
}

fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() == 0 }
}

#[test]
fn hardlinks_grouped() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"a", b"linked body")
        .hardlink(b"d/b", b"a")
        .file(b"c", b"linked body")
        .file(b"e", b"other");
    let r = ingest(&repo, &src);
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let group = |p: &[u8]| cat.entry_by_path(r.snapshot, p).unwrap().hardlink_group;
    assert!(group(b"a").is_some());
    assert_eq!(group(b"a"), group(b"d/b"));
    assert_eq!(group(b"c"), None);
    assert_eq!(group(b"e"), None);

    let live = manifest_dir(&src, &mopts()).unwrap();
    let s = cat.snapshot(r.snapshot).unwrap();
    assert_eq!(s.manifest_hash, Some(live.core));
    assert_eq!(s.ext_manifest_hash, Some(live.ext));
}

#[test]
fn xattrs_recorded() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"f", b"body").dir(b"d").symlink(b"l", b"f");
    xattr::set(src.join("f"), XATTR, b"v").unwrap();
    xattr::set(src.join("d"), XATTR, b"dir value").unwrap();
    let r = ingest(&repo, &src);

    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let f = cat.entry_by_path(r.snapshot, b"f").unwrap();
    assert_eq!(
        cat.xattrs(f.id).unwrap(),
        vec![(XATTR.as_bytes().to_vec(), b"v".to_vec())]
    );
    let d = cat.entry_by_path(r.snapshot, b"d").unwrap();
    assert_eq!(
        cat.xattrs(d.id).unwrap(),
        vec![(XATTR.as_bytes().to_vec(), b"dir value".to_vec())]
    );

    let s = cat.snapshot(r.snapshot).unwrap();
    let live = manifest_dir(&src, &mopts()).unwrap();
    assert_eq!(s.ext_manifest_hash, Some(live.ext));

    xattr::remove(src.join("f"), XATTR).unwrap();
    let without = manifest_dir(&src, &mopts()).unwrap();
    assert_eq!(without.core, live.core);
    assert_ne!(s.ext_manifest_hash, Some(without.ext));
}

#[test]
fn unreadable_file_fails() {
    if is_root() {
        return; // root can read mode-000 files
    }
    use std::os::unix::fs::PermissionsExt;
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"ok", b"fine").file(b"sub/locked", b"secret");
    let locked = src.join("sub/locked");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let err = ingest::run(&repo, &src, &opts()).unwrap_err();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o644)).unwrap();
    match err {
        Error::Io { path, source } => {
            assert!(path.ends_with("sub/locked"), "{}", path.display());
            assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
        }
        e => panic!("unexpected error {e:?}"),
    }
    assert_eq!(only_snapshot_status(&repo), SnapshotStatus::Failed);
}

#[test]
fn fifo_reported_skipped() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"a", b"x").fifo(b"d/p").file(b"d/q", b"y");
    let r = ingest(&repo, &src);
    assert_eq!(r.skipped, vec![(b"d/p".to_vec(), SkipReason::Fifo)]);
    let live = manifest_dir(&src, &mopts()).unwrap();
    assert_eq!(r.manifest_hash, live.core);
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    assert!(cat.entry_by_path(r.snapshot, b"d/p").is_err());
    assert_eq!(cat.snapshot(r.snapshot).unwrap().entry_count, Some(4));
}

/// Every entry of the snapshot, keyed by path.
fn catalog_entries(cat: &Catalog, snap: drmanhattan_core::ids::Id) -> BTreeMap<Vec<u8>, EntryRow> {
    let mut out = BTreeMap::new();
    let mut stack = vec![cat.entry_by_path(snap, b"").unwrap()];
    while let Some(e) = stack.pop() {
        stack.extend(cat.children(snap, Some(e.id)).unwrap());
        out.insert(e.path.clone(), e);
    }
    out
}

fn assert_record_matches_catalog(repo: &Repo, r: &IngestReport) {
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let snap = cat.snapshot(r.snapshot).unwrap();
    assert_eq!(snap.status, SnapshotStatus::Complete);
    let unit = snap.record_unit.expect("record_unit set");
    let reader = UnitReader::new(repo, &cat, DEFAULT_CACHE_BYTES);
    let rec = read_record(&reader, &cat, &unit).unwrap();

    // The record holds the final row, minus its own record unit.
    assert_eq!(
        rec.snapshot,
        drmanhattan_core::model::SnapshotRow {
            record_unit: None,
            ..snap.clone()
        }
    );

    let expected = catalog_entries(&cat, r.snapshot);
    let got: BTreeMap<Vec<u8>, EntryRow> = rec
        .entries
        .iter()
        .map(|e| (e.path.clone(), e.clone()))
        .collect();
    assert_eq!(rec.entries.len(), got.len(), "duplicate entry paths");
    assert_eq!(got, expected);

    let mut want: BTreeMap<Hash32, Vec<Hash32>> = BTreeMap::new();
    for e in expected.values() {
        if let Some(h) = e.content_hash {
            let units = cat.content_units(&h).unwrap();
            want.insert(h, units.iter().map(|u| u.hash).collect());
        }
    }
    let have: BTreeMap<Hash32, Vec<Hash32>> = rec.contents.iter().cloned().collect();
    assert_eq!(rec.contents.len(), have.len(), "duplicate contents");
    assert_eq!(have, want);

    let mut want_x = Vec::new();
    for e in expected.values() {
        for (n, v) in cat.xattrs(e.id).unwrap() {
            want_x.push((e.id, n, v));
        }
    }
    let mut have_x = rec.xattrs.clone();
    want_x.sort();
    have_x.sort();
    assert_eq!(have_x, want_x);
}

#[test]
fn snapshot_record_roundtrip() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"a.html", b"<html>hello</html>")
        .file(b"dup1", &[7u8; 5000])
        .file(b"d/dup2", &[7u8; 5000])
        .hardlink(b"d/hl", b"a.html")
        .file(b"d/e/big.bin", &vec![3u8; 600_000])
        .file(b"empty", b"")
        .symlink(b"d/l", b"../a.html");
    xattr::set(src.join("dup1"), XATTR, b"x1").unwrap();
    xattr::set(src.join("d"), XATTR, &vec![0xAB; 9000]).unwrap();
    let r = ingest(&repo, &src);
    assert_record_matches_catalog(&repo, &r);
}

#[test]
fn snapshot_record_spans_chunks() {
    let (_tmp, mut repo, src) = setup();
    // A tiny binary profile, so the record document is split into many units.
    repo.config.chunking.binary.min = 256;
    repo.config.chunking.binary.avg = 1024;
    repo.config.chunking.binary.max = 4096;
    let mut t = TreeBuilder::new(&src);
    for i in 0..60u32 {
        t.file(
            format!("dir{}/file-{i:03}.txt", i % 5).as_bytes(),
            &i.to_le_bytes(),
        );
    }
    let r = ingest(&repo, &src);
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    let doc_chunks: i64 = conn
        .query_row(
            "SELECT count(*) FROM unit WHERE kind = 'raw_chunk' \
             AND hash NOT IN (SELECT unit_hash FROM content_unit)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(doc_chunks > 1, "record stored in {doc_chunks} chunk(s)");
    let records: i64 = conn
        .query_row(
            "SELECT count(*) FROM unit WHERE kind = 'snapshot_record'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(records, 1);
    assert_record_matches_catalog(&repo, &r);
}

#[test]
fn record_temp_file_removed() {
    let (_tmp, repo, src) = setup();
    std::fs::write(src.join("f"), b"x").unwrap();
    ingest(&repo, &src);
    let tmp_dir = repo.root.join("tmp");
    let left: Vec<_> = match std::fs::read_dir(&tmp_dir) {
        Ok(rd) => rd.map(|e| e.unwrap().file_name()).collect(),
        Err(_) => Vec::new(),
    };
    assert!(left.is_empty(), "left behind: {left:?}");
}

#[test]
fn reingest_counts_exclude_record_units() {
    let (_tmp, repo, src) = setup();
    std::fs::write(src.join("f"), b"same bytes").unwrap();
    let r1 = ingest(&repo, &src);
    let r2 = ingest(&repo, &src);
    assert_eq!(r1.new_units, 1);
    assert_eq!(r2.new_units, 0);
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let (s1, s2) = (
        cat.snapshot(r1.snapshot).unwrap(),
        cat.snapshot(r2.snapshot).unwrap(),
    );
    assert!(s1.record_unit.is_some() && s2.record_unit.is_some());
    assert_ne!(s1.record_unit, s2.record_unit);
}
