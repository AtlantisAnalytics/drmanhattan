//! The ingest memory budget: buffered file data stays within the budget (or
//! the largest file, if larger) and the stored result does not change.

mod common;

use common::TreeBuilder;
use drmanhattan_core::error::Error;
use drmanhattan_core::ingest::{self, IngestOptions, IngestReport};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::restore::{self, RestoreOptions};
use drmanhattan_core::verify;
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};
use std::path::Path;

const KIB: usize = 1024;

fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    ChaCha8Rng::seed_from_u64(seed).fill_bytes(&mut v);
    v
}

/// 40 files of 16 to 48 KiB plus `big.bin` of 300 KiB, all incompressible
/// and distinct. Returns the files as (name, bytes).
fn tree(src: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<(String, Vec<u8>)> = (0..40u64)
        .map(|i| {
            (
                format!("f{i:02}.bin"),
                noise(i, (16 + (i as usize % 33)) * KIB),
            )
        })
        .collect();
    files.push(("big.bin".into(), noise(999, 300 * KIB)));
    let mut t = TreeBuilder::new(src);
    for (name, body) in &files {
        t.file(name.as_bytes(), body);
    }
    files
}

fn ingest_with(repo: &Repo, src: &Path, max_memory: Option<u64>) -> IngestReport {
    let opts = IngestOptions {
        workers: 8,
        max_memory,
        ..IngestOptions::default()
    };
    ingest::run(repo, src, &opts).unwrap()
}

#[test]
fn budget_bounds_buffered_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    let files = tree(&src);

    let r = ingest_with(&repo, &src, Some(64 * KIB as u64));
    assert!(r.peak_buffered > 0);
    assert!(
        r.peak_buffered <= 300 * KIB as u64,
        "peak {} over the largest file",
        r.peak_buffered
    );

    let dest = tmp.path().join("out");
    restore::run(&repo, r.snapshot, &dest, &RestoreOptions::default()).unwrap();
    for (name, body) in &files {
        assert_eq!(&std::fs::read(dest.join(name)).unwrap(), body, "{name}");
    }
    let v = verify::run(&repo, None, false).unwrap();
    assert!(v.problems.is_empty(), "{:?}", v.problems);
}

#[test]
fn output_is_identical_with_and_without_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    tree(&src);
    let a = Repo::init(&tmp.path().join("a")).unwrap();
    let b = Repo::init(&tmp.path().join("b")).unwrap();
    let ra = ingest_with(&a, &src, Some(64 * KIB as u64));
    let rb = ingest_with(&b, &src, None);
    assert_eq!(ra.manifest_hash, rb.manifest_hash);
    assert_eq!(ra.new_units, rb.new_units);
    assert_eq!(ra.new_bytes_stored, rb.new_bytes_stored);
}

#[test]
fn reingest_with_tiny_budget_completes() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let mut t = TreeBuilder::new(&src);
    for i in 0..20u64 {
        t.file(format!("f{i:02}").as_bytes(), &noise(i, 24 * KIB));
    }
    let a = Repo::init(&tmp.path().join("a")).unwrap();
    let b = Repo::init(&tmp.path().join("b")).unwrap();
    ingest_with(&a, &src, Some(1));
    let again = ingest_with(&a, &src, Some(1));
    ingest_with(&b, &src, None);
    let unbudgeted = ingest_with(&b, &src, None);
    // Only the snapshot record's own units are new the second time.
    assert_eq!(again.new_units, unbudgeted.new_units);
    assert!(again.peak_buffered <= 24 * KIB as u64);
}

#[test]
fn zero_max_memory_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    TreeBuilder::new(&src).file(b"f", b"x");
    let opts = IngestOptions {
        max_memory: Some(0),
        ..IngestOptions::default()
    };
    let err = ingest::run(&repo, &src, &opts).unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err:?}");
}
