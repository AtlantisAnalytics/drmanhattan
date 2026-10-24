//! Failpoint tests for the ingest pipeline. The failpoint registry is
//! process-global (a configured failpoint fires on every ingest in the
//! process), so these live in their own test binary and serialize on
//! `FAIL_LOCK`.
#![cfg(feature = "failpoints")]

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::manifest::{manifest_dir, ManifestOptions};
use drmanhattan_core::model::{EntryStatus, SnapshotStatus};
use drmanhattan_core::pack::pack_path;
use drmanhattan_core::repo::Repo;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

static FAIL_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn storage_failure_after_commits_keeps_units_within_synced_packs() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario = fail::FailScenario::setup();
    // The first barrier with appended bytes commits; the second fails before
    // its sync, leaving an unsealed pack with committed units and unsynced
    // bytes beyond them.
    fail::cfg("ingest::storage_before_sync", "1*off->return").unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    let mut t = TreeBuilder::new(&src);
    for i in 0..40u32 {
        let body: Vec<u8> = (0..20_000u32)
            .map(|j| (j.wrapping_mul(2_654_435_761) ^ i.wrapping_mul(40_503)) as u8)
            .collect();
        t.file(format!("f{i:02}").as_bytes(), &body);
    }
    let opts = IngestOptions {
        workers: 2,
        barrier_ops: Some(1),
        ..IngestOptions::default()
    };
    let err = ingest::run(&repo, &src, &opts).unwrap_err();
    scenario.teardown();

    assert!(
        matches!(&err, Error::Corrupt(m) if m.contains("ingest::storage_before_sync")),
        "{err:?}"
    );
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let snaps = cat.snapshots().unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].status, SnapshotStatus::Failed);
    let unsealed = common::assert_units_within_packs(&repo);
    assert!(unsealed > 0, "no committed units in an unsealed pack");

    // The failed barrier had appended bytes it never committed, so the
    // unsealed pack's committed size is strictly below its on-disk length.
    // Equality would mean a barrier committed before its fsync.
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    let mut stmt = conn
        .prepare("SELECT id, size FROM pack WHERE sealed = 0")
        .unwrap();
    let packs: Vec<(Vec<u8>, i64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(packs.len(), 1);
    let (id, size) = &packs[0];
    let id = Id::from_bytes(id.clone().try_into().unwrap());
    let on_disk = std::fs::metadata(pack_path(&repo.packs_dir(), id))
        .unwrap()
        .len();
    assert!(
        (*size as u64) < on_disk,
        "committed size {size} not below on-disk length {on_disk}"
    );
}

/// A repo plus a source holding the single file `src/f`, returned as the
/// canonical path ingest reports in errors. Failpoint callbacks fire for
/// every file, so these tests use exactly one.
fn one_file(body: &[u8]) -> (tempfile::TempDir, Repo, PathBuf, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    TreeBuilder::new(&src).file(b"f", body);
    let f = src.join("f").canonicalize().unwrap();
    (tmp, repo, src, f)
}

/// At `ingest::after_read`, append a byte to `path` on the first `times`
/// reads. Returns the number of reads seen.
fn grow_after_read(path: &Path, times: usize) -> Arc<AtomicUsize> {
    let seen = Arc::new(AtomicUsize::new(0));
    let (p, n) = (path.to_path_buf(), seen.clone());
    fail::cfg_callback("ingest::after_read", move || {
        if n.fetch_add(1, Ordering::SeqCst) < times {
            let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
            f.write_all(b"+").unwrap();
        }
    })
    .unwrap();
    seen
}

fn opts() -> IngestOptions {
    IngestOptions {
        workers: 2,
        ..IngestOptions::default()
    }
}

fn first_status(repo: &Repo) -> SnapshotStatus {
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let snaps = cat.snapshots().unwrap();
    assert_eq!(snaps.len(), 1);
    snaps[0].status
}

#[test]
fn vanished_file_fails() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario = fail::FailScenario::setup();
    let (_tmp, repo, src, f) = one_file(b"here at walk time");
    let target = f.clone();
    fail::cfg_callback("ingest::after_walk", move || {
        let _ = std::fs::remove_file(&target);
    })
    .unwrap();
    let err = ingest::run(&repo, &src, &opts()).unwrap_err();
    scenario.teardown();
    match err {
        Error::Io { path, source } => {
            assert_eq!(path, f);
            assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
        }
        e => panic!("unexpected error {e:?}"),
    }
    assert_eq!(first_status(&repo), SnapshotStatus::Failed);
}

#[test]
fn unstable_fails_by_default() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario = fail::FailScenario::setup();
    let (_tmp, repo, src, f) = one_file(b"growing");
    let seen = grow_after_read(&f, usize::MAX);
    let err = ingest::run(&repo, &src, &opts()).unwrap_err();
    scenario.teardown();
    match err {
        Error::Unstable(path) => assert_eq!(path, f),
        e => panic!("unexpected error {e:?}"),
    }
    assert_eq!(seen.load(Ordering::SeqCst), 2, "read once, retried once");
    assert_eq!(first_status(&repo), SnapshotStatus::Failed);
}

#[test]
fn unstable_allowed_flags() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario = fail::FailScenario::setup();
    let (_tmp, repo, src, f) = one_file(b"growing");
    let seen = grow_after_read(&f, usize::MAX);
    let opts = IngestOptions {
        allow_unstable: true,
        ..opts()
    };
    let r = ingest::run(&repo, &src, &opts).unwrap();
    scenario.teardown();
    assert_eq!(seen.load(Ordering::SeqCst), 2);
    assert_eq!(r.unstable, vec![b"f".to_vec()]);
    assert_eq!(first_status(&repo), SnapshotStatus::Complete);

    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let e = cat.entry_by_path(r.snapshot, b"f").unwrap();
    assert_eq!(e.status, EntryStatus::Unstable);
    // The retry's bytes: the original plus the first appended byte.
    assert_eq!(e.content_hash, Some(Hash32::of(b"growing+")));
    let content = cat.content(&e.content_hash.unwrap()).unwrap();
    assert_eq!(e.size, Some(content.size));
    assert_eq!(e.size, Some(b"growing+".len() as i64));
}

#[test]
fn unstable_once_then_stable_is_ok() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario = fail::FailScenario::setup();
    let (_tmp, repo, src, f) = one_file(b"growing");
    let seen = grow_after_read(&f, 1);
    let r = ingest::run(&repo, &src, &opts()).unwrap();
    scenario.teardown();
    assert_eq!(seen.load(Ordering::SeqCst), 2);
    assert!(r.unstable.is_empty());

    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let e = cat.entry_by_path(r.snapshot, b"f").unwrap();
    assert_eq!(e.status, EntryStatus::Ok);
    assert_eq!(e.size, Some(b"growing+".len() as i64));
    // Size and mtime come from the stable re-read, so the snapshot matches
    // the tree as it now is.
    let live = manifest_dir(
        &src,
        &ManifestOptions {
            mtime_precision_ns: 1,
            cross_fs: false,
        },
    )
    .unwrap();
    assert_eq!(r.manifest_hash, live.core);
}
