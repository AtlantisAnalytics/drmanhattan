//! Failpoint tests for the ingest pipeline. The failpoint registry is
//! process-global, so these live in their own test binary and serialize on
//! `FAIL_LOCK`.
#![cfg(feature = "failpoints")]

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::error::Error;
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::model::SnapshotStatus;
use drmanhattan_core::repo::Repo;
use std::sync::Mutex;

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
}
