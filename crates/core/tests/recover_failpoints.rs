//! Failpoint tests for the catalog swap in `recover::run`. The failpoint
//! registry is process-global, so these serialize on `FAIL_LOCK`.
#![cfg(feature = "failpoints")]

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::Id;
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::recover::{self, RecoverOptions};
use drmanhattan_core::repo::Repo;
use std::fs;
use std::sync::Mutex;

static FAIL_LOCK: Mutex<()> = Mutex::new(());

fn setup() -> (tempfile::TempDir, Repo) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    fs::create_dir(&src).unwrap();
    TreeBuilder::new(&src)
        .file(b"a", b"hello")
        .file(b"b/c", b"x");
    ingest::run(&repo, &src, &IngestOptions::default()).unwrap();
    (tmp, repo)
}

fn root_names(repo: &Repo) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(&repo.root)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

#[test]
fn failure_before_rename_puts_everything_back() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_tmp, repo) = setup();
    // A reader whose transaction began before the insert pins the WAL: the
    // checkpoint recover runs on the old catalog can copy none of it, so
    // the main file and the WAL must come back byte for byte.
    let writer = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    let reader = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM snapshot;")
        .unwrap();
    writer
        .execute(
            "INSERT INTO snapshot(id, status, source_root, host, tool_version, started_at) \
             VALUES (?1, 'in_progress', x'2f', 'h', 't', 1)",
            [Id::new_v7().as_bytes().to_vec()],
        )
        .unwrap();
    let names_before = root_names(&repo);
    assert!(names_before.contains(&"catalog.sqlite-wal".to_string()));
    let cfg_before = fs::read(repo.root.join("wmk.toml")).unwrap();
    let main_before = fs::read(repo.catalog_path()).unwrap();
    let wal_before = fs::read(repo.root.join("catalog.sqlite-wal")).unwrap();
    assert!(!wal_before.is_empty());

    let scenario = fail::FailScenario::setup();
    fail::cfg("recover::before_rename", "return").unwrap();
    let r = recover::run(&repo, &RecoverOptions::default());
    scenario.teardown();

    assert!(matches!(r, Err(Error::Io { .. })), "{r:?}");
    assert_eq!(root_names(&repo), names_before);
    assert!(fs::read(repo.catalog_path()).unwrap() == main_before);
    assert!(fs::read(repo.root.join("catalog.sqlite-wal")).unwrap() == wal_before);
    assert!(fs::read_dir(repo.tmp_dir()).map_or(true, |mut d| d.next().is_none()));
    assert_eq!(fs::read(repo.root.join("wmk.toml")).unwrap(), cfg_before);
    // The old catalog, WAL included, still shows the uncheckpointed row.
    drop(reader);
    drop(writer);
    assert_eq!(
        Catalog::open_readonly(&repo.catalog_path())
            .unwrap()
            .snapshots()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn failure_after_rename_is_a_warning_not_an_error() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_tmp, repo) = setup();
    let scenario = fail::FailScenario::setup();
    fail::cfg("recover::after_rename", "return").unwrap();
    let r = recover::run(&repo, &RecoverOptions::default());
    scenario.teardown();

    let report = r.unwrap();
    assert!(report.intact(), "{report:#?}");
    assert!(report.old_catalog.is_some());
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("new catalog is in place")),
        "{:?}",
        report.warnings
    );
    assert_eq!(
        Catalog::open_readonly(&repo.catalog_path())
            .unwrap()
            .snapshots()
            .unwrap()
            .len(),
        1
    );
}
