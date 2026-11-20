//! Failpoint tests for how much `verify` reads. The failpoint registry is
//! process-global, so these live in their own test binary and serialize on
//! `FAIL_LOCK`.
#![cfg(feature = "failpoints")]

mod common;

use common::TreeBuilder;
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::verify::{self, VerifyOptions, VerifyReport};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use tempfile::TempDir;

static FAIL_LOCK: Mutex<()> = Mutex::new(());

const CHUNK_READS: &str = "pack::read_chunk_payload";

/// Deterministic bytes that zstd cannot shrink much.
fn noise(seed: u32, len: usize) -> Vec<u8> {
    (0..len as u32)
        .map(|j| {
            (j.wrapping_mul(2_654_435_761) ^ seed.wrapping_mul(40_503)).rotate_left(seed % 7) as u8
        })
        .collect()
}

/// A repo with one snapshot: small text files, a duplicated file, and a file
/// large enough to span several binary-profile chunks.
fn setup() -> (TempDir, Repo) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    let mut t = TreeBuilder::new(&src);
    let big = noise(1, 6_000_000);
    t.file(b"a.txt", b"alpha alpha alpha\n")
        .file(b"sub/b.txt", b"bravo\n")
        .file(b"big.bin", &big)
        .file(b"sub/big-copy.bin", &big);
    t.finish();
    let opts = IngestOptions {
        workers: 2,
        ..IngestOptions::default()
    };
    ingest::run(&repo, &src, &opts).unwrap();
    (tmp, repo)
}

fn chunk_units(repo: &Repo) -> usize {
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM unit WHERE kind = 'raw_chunk'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    usize::try_from(n).unwrap()
}

/// Run `verify` over every snapshot, counting chunk payloads read from packs.
fn verify_counting_chunk_reads(repo: &Repo, quick: bool) -> (VerifyReport, usize) {
    let reads = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&reads);
    fail::cfg_callback(CHUNK_READS, move || {
        counter.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    let report = verify::run(repo, None, quick).unwrap();
    fail::remove(CHUNK_READS);
    (report, reads.load(Ordering::SeqCst))
}

#[test]
fn quick_verify_reads_no_chunk_payloads() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_tmp, repo) = setup();
    let scenario = fail::FailScenario::setup();
    let (report, reads) = verify_counting_chunk_reads(&repo, true);
    scenario.teardown();

    assert!(report.problems.is_empty(), "{:?}", report.problems);
    assert!(report.units_checked > 0);
    assert_eq!(reads, 0, "quick verify must check record headers only");
}

#[test]
fn full_verify_reads_each_chunk_payload_once() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_tmp, repo) = setup();
    let chunks = chunk_units(&repo);
    assert!(
        chunks > 3,
        "fixture should span several chunks, has {chunks}"
    );
    let scenario = fail::FailScenario::setup();
    let (report, reads) = verify_counting_chunk_reads(&repo, false);
    scenario.teardown();

    assert!(report.problems.is_empty(), "{:?}", report.problems);
    assert_eq!(
        reads, chunks,
        "each chunk payload should be read from its pack once"
    );
}

/// A gate the first chunk read waits on, so its content stalls while the
/// other worker and the producer run on.
#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    opened: Condvar,
}

impl Gate {
    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.opened.notify_all();
    }

    fn is_open(&self) -> bool {
        *self.open.lock().unwrap()
    }

    fn wait(&self) {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.opened.wait(open).unwrap();
        }
    }
}

/// A repo with one snapshot of `n` small distinct files (one chunk each).
fn many_small_files(n: u32) -> (TempDir, Repo) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    let mut t = TreeBuilder::new(&src);
    for i in 0..n {
        t.file(format!("f{i:03}").as_bytes(), &noise(i + 1, 4096));
    }
    t.finish();
    let opts = IngestOptions {
        workers: 2,
        ..IngestOptions::default()
    };
    ingest::run(&repo, &src, &opts).unwrap();
    (tmp, repo)
}

#[test]
fn stalled_content_holds_back_the_producer() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    const FILES: u32 = 120;
    const WINDOW: usize = 8;
    // With the stalled job among the first two sent, at most WINDOW reads
    // happen while it waits; without the window, nearly all FILES do.
    const LIMIT: usize = 2 * WINDOW;
    let (_tmp, repo) = many_small_files(FILES);
    let scenario = fail::FailScenario::setup();

    let gate = Arc::new(Gate::default());
    let reads = Arc::new(AtomicUsize::new(0));
    let overran = Arc::new(AtomicBool::new(false));
    let waited = Arc::new(AtomicBool::new(false));
    let (g, r, o) = (gate.clone(), reads.clone(), overran.clone());
    fail::cfg_callback(CHUNK_READS, move || {
        let n = r.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            g.wait();
        } else if n > LIMIT && !g.is_open() {
            o.store(true, Ordering::SeqCst);
            g.open();
        }
    })
    .unwrap();
    let (g, w) = (gate.clone(), waited.clone());
    fail::cfg_callback("verify::wait_credit", move || {
        w.store(true, Ordering::SeqCst);
        g.open();
    })
    .unwrap();
    let opts = VerifyOptions {
        quick: false,
        workers: 2,
        window: Some(WINDOW),
    };
    let report = verify::run_with(&repo, None, &opts).unwrap();
    fail::remove(CHUNK_READS);
    fail::remove("verify::wait_credit");
    scenario.teardown();

    assert!(report.problems.is_empty(), "{:?}", report.problems);
    assert!(
        !overran.load(Ordering::SeqCst),
        "more than {LIMIT} contents were read while the first was stalled"
    );
    assert!(
        waited.load(Ordering::SeqCst),
        "the producer never waited for the window"
    );
}

#[test]
fn worker_panic_propagates_without_hanging() {
    let _serial = FAIL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_tmp, repo) = many_small_files(120);
    let scenario = fail::FailScenario::setup();
    fail::cfg(CHUNK_READS, "1*panic(injected worker panic)").unwrap();
    // A window smaller than the job count, so the producer must wait on
    // the panicked job's slot.
    let opts = VerifyOptions {
        quick: false,
        workers: 2,
        window: Some(8),
    };
    let run = std::panic::catch_unwind(|| verify::run_with(&repo, None, &opts));
    fail::remove(CHUNK_READS);
    scenario.teardown();
    // The test harness timeout is the hang detector.
    assert!(run.is_err(), "a worker panic must surface as a panic");
}
