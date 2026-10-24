mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::chunk::chunk_reader;
use drmanhattan_core::config::Config;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::Id;
use drmanhattan_core::ingest::{self, IngestOptions, IngestReport};
use drmanhattan_core::manifest::{manifest_dir, ManifestOptions};
use drmanhattan_core::model::SnapshotStatus;
use drmanhattan_core::pack::{pack_path, scan, RECORD_HEADER_LEN};
use drmanhattan_core::repo::Repo;
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn opts() -> IngestOptions {
    IngestOptions {
        note: None,
        allow_unstable: false,
        cross_fs: false,
        workers: 4,
    }
}

fn mopts() -> ManifestOptions {
    ManifestOptions {
        mtime_precision_ns: 1,
        cross_fs: false,
    }
}

fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    ChaCha8Rng::seed_from_u64(seed).fill_bytes(&mut v);
    v
}

/// A temp dir holding a fresh repo at `r/` and an empty source dir at `src/`.
fn setup() -> (TempDir, Repo, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir(&src).unwrap();
    (tmp, repo, src)
}

fn db(repo: &Repo) -> rusqlite::Connection {
    rusqlite::Connection::open(repo.catalog_path()).unwrap()
}

fn count(repo: &Repo, sql: &str) -> i64 {
    db(repo).query_row(sql, [], |r| r.get(0)).unwrap()
}

fn entry_ids(repo: &Repo, snapshot: Id) -> HashSet<Vec<u8>> {
    let conn = db(repo);
    let mut stmt = conn
        .prepare("SELECT id FROM entry WHERE snapshot_id = ?1")
        .unwrap();
    let rows = stmt
        .query_map([snapshot.as_bytes().to_vec()], |r| r.get::<_, Vec<u8>>(0))
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn ingest(repo: &Repo, src: &Path) -> IngestReport {
    ingest::run(repo, src, &opts()).unwrap()
}

fn png(width: u32, height: u32) -> Vec<u8> {
    let mut v = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR".to_vec();
    v.extend_from_slice(&width.to_be_bytes());
    v.extend_from_slice(&height.to_be_bytes());
    v.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    v.extend_from_slice(&random(99, 4000));
    v
}

#[test]
fn ingest_manifest_matches_dir() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.dir(b"a").dir(b"a/b").dir(b"c");
    let dirs: [&str; 4] = ["", "a/", "a/b/", "c/"];
    for i in 0..20u64 {
        let dir = dirs[i as usize % 4];
        match i % 3 {
            0 => {
                let html = format!(
                    "<html><body>{}</body></html>",
                    "<p>hi</p>".repeat(i as usize * 50)
                );
                t.file(format!("{dir}page{i}.html").as_bytes(), html.as_bytes());
            }
            1 => {
                let mut jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0];
                jpeg.extend_from_slice(&random(i, 5000 + i as usize * 100));
                t.file(format!("{dir}img{i}.jpg").as_bytes(), &jpeg);
            }
            _ => {
                t.file(
                    format!("{dir}blob{i}.bin").as_bytes(),
                    &random(i, 1000 * i as usize),
                );
            }
        }
    }
    t.symlink(b"c/link", b"../a/page4.html");
    t.mtime(b"a/b", 1_000_000_000_123_456_789).finish();

    let live = manifest_dir(&src, &mopts()).unwrap();
    let r = ingest(&repo, &src);
    assert_eq!(r.manifest_hash, live.core);
    assert_eq!(r.entries, 25);
    assert_eq!(r.files, 20);
    assert!(r.skipped.is_empty() && r.unstable.is_empty());
    assert_eq!(r.withheld, 0);

    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let s = cat.snapshot(r.snapshot).unwrap();
    assert_eq!(s.status, SnapshotStatus::Complete);
    assert_eq!(s.manifest_hash, Some(live.core));
    assert_eq!(s.source_manifest_hash, Some(live.core));
    assert!(s.ext_manifest_hash.is_some());
    assert_eq!(s.entry_count, Some(25));
    assert!(s.completed_at.is_some());
    assert_eq!(
        cat.entry_by_path(r.snapshot, b"c/link")
            .unwrap()
            .link_target,
        Some(b"../a/page4.html".to_vec())
    );
}

#[test]
fn exact_duplicates_stored_once() {
    let (_tmp, repo, src) = setup();
    let data = random(7, 300 * 1024);
    let mut t = TreeBuilder::new(&src);
    for p in ["a", "b", "d/c", "d/e/f", "g"] {
        t.file(p.as_bytes(), &data);
    }
    let r = ingest(&repo, &src);

    let mut chunks = 0;
    chunk_reader(&Config::default().chunking.binary, &data[..], |_, _| {
        chunks += 1;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        count(&repo, "SELECT count(*) FROM entry WHERE kind = 'file'"),
        5
    );
    assert_eq!(
        count(
            &repo,
            "SELECT count(DISTINCT content_hash) FROM entry WHERE kind = 'file'"
        ),
        1
    );
    assert_eq!(count(&repo, "SELECT count(*) FROM content"), 1);
    assert_eq!(count(&repo, "SELECT count(*) FROM unit"), chunks);
    assert_eq!(r.new_units, chunks as u64);
    assert_eq!(r.bytes_read, 5 * data.len() as u64);
}

#[test]
fn reingest_zero_new_units() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"x.html", b"<html>hello</html>")
        .file(b"d/y.bin", &random(1, 700_000))
        .symlink(b"d/l", b"x");
    let r1 = ingest(&repo, &src);
    let r2 = ingest(&repo, &src);
    assert!(r1.new_units > 0);
    assert_eq!(r2.new_units, 0);
    assert_eq!(r2.new_bytes_stored, 0);
    assert_eq!(r2.dedup_ratio, 0.0);
    assert_eq!(r1.manifest_hash, r2.manifest_hash);
    assert_ne!(r1.snapshot, r2.snapshot);
    let (e1, e2) = (entry_ids(&repo, r1.snapshot), entry_ids(&repo, r2.snapshot));
    assert_eq!(e1.len(), 5);
    assert_eq!(e2.len(), 5);
    assert!(e1.is_disjoint(&e2));
}

#[test]
fn shared_template_dedups() {
    let (_tmp, repo, src) = setup();
    // ~80 KiB of identical HTML prefix (pseudo-random hex, so CDC finds
    // boundaries), then ~20 KiB of numbered lines unique to each file.
    let mut template = String::from("<html><head><title>t</title></head><body>\n");
    let mut i = 0u64;
    while template.len() < 80 * 1024 {
        template.push_str(&format!(
            "<div class=\"r{i}\">{}</div>\n",
            blake3::hash(&i.to_le_bytes()).to_hex()
        ));
        i += 1;
    }
    let mut t = TreeBuilder::new(&src);
    for f in 0..50 {
        let mut page = template.clone();
        let mut j = 0;
        while page.len() < 100 * 1024 {
            page.push_str(&format!("<p>file {f} line {j}: unique tail text</p>\n"));
            j += 1;
        }
        page.push_str("</body></html>\n");
        t.file(format!("p{f:02}.html").as_bytes(), page.as_bytes());
    }
    let r = ingest(&repo, &src);
    assert!(r.bytes_read > 50 * 100 * 1024);
    assert!(
        r.new_bytes_stored * 4 < r.bytes_read,
        "stored {} of {} read",
        r.new_bytes_stored,
        r.bytes_read
    );
    assert!(r.dedup_ratio > 4.0);
}

#[test]
fn overlap_refused() {
    // Repo inside the source.
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("f"), b"x").unwrap();
    let repo = Repo::init(&src.join("repo")).unwrap();
    assert!(matches!(
        ingest::run(&repo, &src, &opts()),
        Err(Error::Overlap)
    ));
    assert_no_snapshot_no_packs(&repo);

    // Source inside the repo, reached through a non-canonical path.
    let (tmp, repo, _) = setup();
    let inner = repo.root.join("sub");
    std::fs::create_dir(&inner).unwrap();
    std::fs::write(inner.join("f"), b"x").unwrap();
    let odd = tmp.path().join("src/../r/./sub");
    assert!(matches!(
        ingest::run(&repo, &odd, &opts()),
        Err(Error::Overlap)
    ));
    assert_no_snapshot_no_packs(&repo);

    // A sibling whose name merely shares a prefix is not an overlap.
    let sibling = tmp.path().join("r2");
    std::fs::create_dir(&sibling).unwrap();
    ingest::run(&repo, &sibling, &opts()).unwrap();
}

fn assert_no_snapshot_no_packs(repo: &Repo) {
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    assert!(cat.snapshots().unwrap().is_empty());
    assert_eq!(std::fs::read_dir(repo.packs_dir()).unwrap().count(), 0);
}

#[test]
fn concurrent_ingest_locked() {
    let (_tmp, repo, src) = setup();
    std::fs::write(src.join("f"), b"x").unwrap();
    let _held = repo.lock_writer().unwrap();
    assert!(matches!(
        ingest::run(&repo, &src, &opts()),
        Err(Error::Locked)
    ));
}

#[test]
fn fsync_before_commit() {
    let (_tmp, mut repo, src) = setup();
    // Small packs so the run seals several of them.
    repo.config.packs.max_size = 1 << 20;
    let mut t = TreeBuilder::new(&src);
    for i in 0..12u64 {
        t.file(format!("f{i}").as_bytes(), &random(100 + i, 512 * 1024));
    }
    ingest(&repo, &src);

    let conn = db(&repo);
    let bad: i64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM unit u JOIN pack p ON p.id = u.pack_id \
                 WHERE u.pack_offset >= p.size OR u.pack_offset + {RECORD_HEADER_LEN} + u.stored_size > p.size"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bad, 0);

    let mut stmt = conn
        .prepare("SELECT id, size, sealed, blake3 IS NOT NULL FROM pack")
        .unwrap();
    let packs: Vec<(Vec<u8>, i64, bool, bool)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(
        packs.len() >= 2,
        "expected several packs, got {}",
        packs.len()
    );
    for (id, size, sealed, has_hash) in packs {
        assert!(sealed && has_hash);
        let id = Id::from_bytes(id.try_into().unwrap());
        let path = pack_path(&repo.packs_dir(), id);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), size as u64);
        let s = scan(&path).unwrap();
        assert!(s.sealed && s.index_ok);
        assert_eq!(s.blake3_ok, Some(true));
        assert!(size as u64 <= repo.config.packs.max_size + 4096);
    }
}

#[test]
fn image_dimensions_recorded() {
    let (_tmp, repo, src) = setup();
    std::fs::write(src.join("pic.png"), png(3, 7)).unwrap();
    ingest(&repo, &src);
    let (w, h): (i64, i64) = db(&repo)
        .query_row("SELECT width, height FROM media", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!((w, h), (3, 7));
}

#[test]
fn walk_error_marks_failed() {
    let (_tmp, repo, src) = setup();
    let file = src.join("not-a-dir");
    std::fs::write(&file, b"x").unwrap();
    assert!(matches!(
        ingest::run(&repo, &file, &opts()),
        Err(Error::Io { .. })
    ));
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let snaps = cat.snapshots().unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].status, SnapshotStatus::Failed);
}

#[test]
fn worker_error_aborts_cleanly() {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        return; // root can read mode-000 files
    }
    use std::os::unix::fs::PermissionsExt;
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    for i in 0..200 {
        t.file(format!("d{}/f{i}", i % 7).as_bytes(), &random(i, 3000));
    }
    let locked = src.join("d2/f100");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let err = ingest::run(&repo, &src, &opts()).unwrap_err();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o644)).unwrap();
    match err {
        Error::Io { path, .. } => assert!(path.ends_with("d2/f100"), "{}", path.display()),
        e => panic!("unexpected error {e:?}"),
    }
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let snaps = cat.snapshots().unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].status, SnapshotStatus::Failed);
}
