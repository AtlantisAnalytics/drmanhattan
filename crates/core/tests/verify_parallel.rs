//! Parallel `verify`: the report is the same for any worker count, and in
//! the same order as the serial checker produced.

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::display::display_path;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::model::UnitRow;
use drmanhattan_core::pack::{pack_path, RECORD_HEADER_LEN};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::verify::{self, VerifyOptions};
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tempfile::TempDir;

/// Deterministic bytes that zstd cannot shrink, so a file's units fill
/// several small packs.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    ChaCha8Rng::seed_from_u64(seed).fill_bytes(&mut v);
    v
}

/// One ingest worker, so units reach packs in walk order and pack ids sort
/// the same way every run.
fn ingest_into(repo: &Repo, src: &Path) -> Id {
    let opts = IngestOptions {
        workers: 1,
        ..IngestOptions::default()
    };
    ingest::run(repo, src, &opts).unwrap().snapshot
}

fn units_of(repo: &Repo, snap: Id, path: &[u8]) -> Vec<UnitRow> {
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let c = cat.entry_by_path(snap, path).unwrap().content_hash.unwrap();
    cat.content_units(&c).unwrap()
}

/// Flip one payload byte of `unit` in its pack.
fn flip(repo: &Repo, unit: &UnitRow) {
    let pack = pack_path(&repo.packs_dir(), unit.pack_id);
    fs::set_permissions(&pack, fs::Permissions::from_mode(0o644)).unwrap();
    let mut bytes = fs::read(&pack).unwrap();
    bytes[unit.pack_offset as usize + RECORD_HEADER_LEN + 10] ^= 0x01;
    fs::write(&pack, &bytes).unwrap();
}

/// Corruption of several kinds over two snapshots and many small packs.
/// Returns the fixture and the hashes of the shared corrupted unit,
/// `u.bin`'s corrupted unit and `later.bin`'s mis-cataloged unit.
fn corrupted_fixture() -> (TempDir, Repo, [Hash32; 3]) {
    let tmp = tempfile::tempdir().unwrap();
    let mut repo = Repo::init(&tmp.path().join("r")).unwrap();
    // Small packs, so the corruption lands in several of them.
    repo.config.packs.max_size = 1 << 20;
    let src = tmp.path().join("src");
    let r = noise(1, 6_000_000);
    let mut r_plus = r.clone();
    r_plus.extend(noise(2, 1_000_000));
    let mut t = TreeBuilder::new(&src);
    t.file(b"r.bin", &r)
        .file(b"r_plus.bin", &r_plus)
        .file(b"u.bin", &noise(3, 3_000_000))
        .file(b"a.txt", b"alpha\n")
        .file(b"empty.txt", b"")
        .dir(b"d");
    t.finish();
    let snap1 = ingest_into(&repo, &src);
    t.file(b"later.bin", &noise(4, 2_000_000));
    t.finish();
    let snap2 = ingest_into(&repo, &src);

    let shared = units_of(&repo, snap1, b"r.bin")[0].clone();
    assert!(
        units_of(&repo, snap1, b"r_plus.bin")
            .iter()
            .any(|u| u.hash == shared.hash),
        "r.bin and r_plus.bin should share their first unit"
    );
    let u_last = units_of(&repo, snap1, b"u.bin").pop().unwrap();
    let later = units_of(&repo, snap2, b"later.bin")[0].clone();

    flip(&repo, &shared);
    flip(&repo, &u_last);
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    let n = conn
        .execute(
            "UPDATE unit SET stored_size = stored_size + 1 WHERE hash = ?1",
            [&later.hash.0[..]],
        )
        .unwrap();
    assert_eq!(n, 1);
    drop(conn);
    (tmp, repo, [shared.hash, u_last.hash, later.hash])
}

fn corrupted_repo() -> (TempDir, Repo) {
    let (tmp, repo, _) = corrupted_fixture();
    (tmp, repo)
}

fn is_id(s: &[u8]) -> bool {
    s.len() == 36
        && s.iter().enumerate().all(|(i, &b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_digit() || (b'a'..=b'f').contains(&b),
        })
}

/// Make `problems` independent of the run: the repo root becomes `<repo>`,
/// and each UUID (snapshot and pack ids, which are time-based) becomes
/// `<idN>`, numbered by first appearance across the whole list. A pack's
/// shard directory (its id's last two hex digits) goes with its id.
fn normalize(repo: &Repo, problems: &[String]) -> Vec<String> {
    let root = display_path(&repo.root);
    let mut ids: HashMap<String, usize> = HashMap::new();
    problems
        .iter()
        .map(|p| {
            let p = p.replace(&root, "<repo>");
            let b = p.as_bytes();
            let mut out = String::new();
            let mut i = 0;
            while i < b.len() {
                let boundary = |j: usize| j >= b.len() || !b[j].is_ascii_alphanumeric();
                let starts = i == 0 || !b[i - 1].is_ascii_alphanumeric();
                if starts && i + 36 <= b.len() && is_id(&b[i..i + 36]) && boundary(i + 36) {
                    let id = &p[i..i + 36];
                    let shard = format!("/{}/", &id[34..]);
                    if out.ends_with(&shard) {
                        out.truncate(out.len() - 3);
                    }
                    let n = ids.len();
                    let n = *ids.entry(id.to_string()).or_insert(n);
                    out.push_str(&format!("<id{n}>"));
                    i += 36;
                } else {
                    let c = p[i..].chars().next().unwrap();
                    out.push(c);
                    i += c.len_utf8();
                }
            }
            out
        })
        .collect()
}

#[test]
fn report_matches_serial_order() {
    let (_tmp, repo) = corrupted_repo();
    let r = verify::run(&repo, None, false).unwrap();
    // Pinned from the serial checker (commit 34918e1).
    let expected = vec![
        "snapshot <id0>: unit 3e8712cf5cddf4f72ecbe4a68624673dc5ab10bd4cca135085763abdacee4f15: \
         repository is corrupt: pack <repo>/packs/<id1>.pack at offset 26: unit hash mismatch \
         (computed 61c22e7af03b4a27807075d91ddfd378d01614372284fce0bb8422e12fe94549, \
         expected 3e8712cf5cddf4f72ecbe4a68624673dc5ab10bd4cca135085763abdacee4f15, \
         header 3e8712cf5cddf4f72ecbe4a68624673dc5ab10bd4cca135085763abdacee4f15) \
         (pack <repo>/packs/<id1>.pack offset 26)",
        "snapshot <id0>: unit a7da1343a0a1510e70eee81f9ba392eaa44bd92defbacf547c7a40d475c2884b: \
         repository is corrupt: pack <repo>/packs/<id2>.pack at offset 26: unit hash mismatch \
         (computed 976ab7b4944e20679041bd90aeb856341b40659ae733a871ef7e45e3bff5b473, \
         expected a7da1343a0a1510e70eee81f9ba392eaa44bd92defbacf547c7a40d475c2884b, \
         header a7da1343a0a1510e70eee81f9ba392eaa44bd92defbacf547c7a40d475c2884b) \
         (pack <repo>/packs/<id2>.pack offset 26)",
        "snapshot <id3>: unit 6361c0b2375d79d326452477f9b7747647345c5e7a2ff17787d0725f9d4b6613: \
         record header (hash 6361c0b2375d79d326452477f9b7747647345c5e7a2ff17787d0725f9d4b6613, \
         raw 399046, stored 399046) disagrees with catalog (raw 399046, stored 399047) \
         (pack <repo>/packs/<id4>.pack offset 26)",
        "pack <repo>/packs/<id2>.pack: BLAKE3 mismatch",
        "pack <repo>/packs/<id1>.pack: BLAKE3 mismatch",
    ];
    assert_eq!(normalize(&repo, &r.problems), expected);
    assert_eq!((r.snapshots, r.units_checked, r.packs_checked), (2, 20, 14));
}

#[test]
fn report_is_identical_for_any_worker_count() {
    let (_tmp, repo) = corrupted_repo();
    for quick in [false, true] {
        let base = verify::run_with(
            &repo,
            None,
            &VerifyOptions {
                quick,
                workers: 1,
                ..VerifyOptions::default()
            },
        )
        .unwrap();
        assert!(!base.problems.is_empty() || quick);
        for workers in [0, 2, 4, 64] {
            let r = verify::run_with(
                &repo,
                None,
                &VerifyOptions {
                    quick,
                    workers,
                    ..VerifyOptions::default()
                },
            )
            .unwrap();
            assert_eq!(r, base, "quick={quick} workers={workers}");
        }
    }
}

#[test]
fn corruption_in_shared_chunk_is_reported_once() {
    let (_tmp, repo, [shared, u_last, later]) = corrupted_fixture();
    let opts = VerifyOptions {
        quick: false,
        workers: 4,
        ..VerifyOptions::default()
    };
    let r = verify::run_with(&repo, None, &opts).unwrap();
    let count = |f: &dyn Fn(&String) -> bool| r.problems.iter().filter(|p| f(p)).count();
    let (shared, u_last, later) = (shared.to_hex(), u_last.to_hex(), later.to_hex());
    assert_eq!(count(&|p| p.contains(&shared)), 1, "{:?}", r.problems);
    assert!(
        count(&|p| p.contains("BLAKE3 mismatch")) >= 1,
        "{:?}",
        r.problems
    );
    assert_eq!(count(&|p| p.contains(&u_last)), 1, "{:?}", r.problems);
    assert_eq!(
        count(&|p| p.contains(&later) && p.contains("disagrees with catalog")),
        1,
        "{:?}",
        r.problems
    );
}

#[test]
fn snapshot_without_contents_verifies_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    TreeBuilder::new(&src)
        .file(b"empty.txt", b"")
        .dir(b"d")
        .finish();
    ingest_into(&repo, &src);
    let opts = VerifyOptions {
        quick: false,
        workers: 4,
        ..VerifyOptions::default()
    };
    let r = verify::run_with(&repo, None, &opts).unwrap();
    assert!(r.problems.is_empty(), "{:?}", r.problems);
    assert_eq!(r.snapshots, 1);
}

#[test]
fn catalog_error_returns_err_without_hanging() {
    let (_tmp, repo) = corrupted_repo();
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    conn.execute_batch("DROP TABLE content_unit").unwrap();
    drop(conn);
    // The test harness timeout is the hang detector.
    let opts = VerifyOptions {
        quick: false,
        workers: 4,
        ..VerifyOptions::default()
    };
    assert!(verify::run_with(&repo, None, &opts).is_err());
}

/// A content whose last unit fails the header pass still has the units
/// before it read, as the serial checker did (verified on commit 34918e1).
#[test]
fn unit_before_a_header_bad_unit_is_still_read() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    TreeBuilder::new(&src)
        .file(b"x.bin", &noise(5, 3_000_000))
        .finish();
    let snap = ingest_into(&repo, &src);
    let units = units_of(&repo, snap, b"x.bin");
    assert!(units.len() >= 2, "x.bin should span several units");
    let (first, last) = (&units[0], units.last().unwrap());
    flip(&repo, first);
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    conn.execute(
        "UPDATE unit SET stored_size = stored_size + 1 WHERE hash = ?1",
        [&last.hash.0[..]],
    )
    .unwrap();
    drop(conn);

    let base = verify::run_with(
        &repo,
        None,
        &VerifyOptions {
            quick: false,
            workers: 1,
            ..VerifyOptions::default()
        },
    )
    .unwrap();
    let p = &base.problems;
    assert_eq!(p.len(), 3, "{p:?}");
    assert!(
        p[0].contains(&last.hash.to_hex()) && p[0].contains("disagrees with catalog"),
        "{p:?}"
    );
    assert!(
        p[1].contains(&first.hash.to_hex()) && p[1].contains("unit hash mismatch"),
        "{p:?}"
    );
    assert!(p[2].ends_with("BLAKE3 mismatch"), "{p:?}");
    let opts = VerifyOptions {
        quick: false,
        workers: 4,
        ..VerifyOptions::default()
    };
    assert_eq!(verify::run_with(&repo, None, &opts).unwrap(), base);
}

#[test]
fn missing_pack_report_is_identical_for_any_worker_count() {
    let (_tmp, repo) = corrupted_repo();
    let snap = Catalog::open_readonly(&repo.catalog_path())
        .unwrap()
        .snapshots()
        .unwrap()[0]
        .id;
    let pack = pack_path(
        &repo.packs_dir(),
        units_of(&repo, snap, b"u.bin")[0].pack_id,
    );
    fs::remove_file(&pack).unwrap();
    let name = pack.file_name().unwrap().to_str().unwrap().to_owned();
    for quick in [false, true] {
        let run = |workers| {
            let opts = VerifyOptions {
                quick,
                workers,
                ..VerifyOptions::default()
            };
            verify::run_with(&repo, None, &opts).unwrap()
        };
        let base = run(1);
        assert!(base.problems.iter().any(|p| p.contains(&name)), "{base:?}");
        assert_eq!(run(4), base);
    }
}
