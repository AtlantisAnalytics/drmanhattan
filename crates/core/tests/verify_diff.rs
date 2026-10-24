//! `verify` and `diff` (spec §4, §9).

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::diff::{self, DiffOptions, Difference};
use drmanhattan_core::ids::Id;
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::pack::{pack_path, RECORD_HEADER_LEN};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::restore::{self, RestoreOptions};
use drmanhattan_core::verify;
use filetime::FileTime;
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const T0: i64 = 1_700_000_000_123_456_789;

fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    ChaCha8Rng::seed_from_u64(seed).fill_bytes(&mut v);
    v
}

/// A repo with one snapshot of a small tree, and the snapshot id.
fn setup() -> (TempDir, Repo, Id) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    fs::create_dir(&src).unwrap();
    let mut t = TreeBuilder::new(&src);
    t.file(b"a.txt", b"alpha alpha alpha\n")
        .file(b"m.txt", b"mode me\n")
        .file(b"t.txt", b"touch me\n")
        .file(b"sub/gone.txt", b"to be deleted\n")
        .file(b"sub/keep.bin", &random(7, 100_000))
        .symlink(b"link", b"a.txt")
        .dir(b"empty");
    for (i, p) in [
        &b"a.txt"[..],
        b"m.txt",
        b"t.txt",
        b"sub/gone.txt",
        b"sub/keep.bin",
        b"link",
        b"sub",
        b"empty",
        b"",
    ]
    .iter()
    .enumerate()
    {
        t.mtime(p, T0 + i as i64 * 1_000_000_007);
    }
    t.finish();
    let opts = IngestOptions {
        workers: 2,
        ..IngestOptions::default()
    };
    let snap = ingest::run(&repo, &src, &opts).unwrap().snapshot;
    (tmp, repo, snap)
}

fn restore_to(repo: &Repo, snap: Id, dest: &Path) {
    restore::run(repo, snap, dest, &RestoreOptions::default()).unwrap();
}

fn set_mtime(p: &Path, ns: i64) {
    let ft = FileTime::from_unix_time(
        ns.div_euclid(1_000_000_000),
        ns.rem_euclid(1_000_000_000) as u32,
    );
    filetime::set_file_times(p, ft, ft).unwrap();
}

fn mtime(p: &Path) -> i64 {
    let m = fs::symlink_metadata(p).unwrap();
    m.mtime() * 1_000_000_000 + m.mtime_nsec()
}

fn dopts(precision: u64) -> DiffOptions {
    DiffOptions {
        path: Vec::new(),
        mtime_precision_ns: precision,
        cross_fs: false,
    }
}

fn content_pack(repo: &Repo, snap: Id, path: &[u8]) -> (PathBuf, i64) {
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let e = cat.entry_by_path(snap, path).unwrap();
    let u = cat.content_units(&e.content_hash.unwrap()).unwrap()[0].clone();
    (pack_path(&repo.packs_dir(), u.pack_id), u.pack_offset)
}

#[test]
fn verify_clean_repo() {
    let (_tmp, repo, _) = setup();
    for quick in [true, false] {
        let r = verify::run(&repo, None, quick).unwrap();
        assert!(r.problems.is_empty(), "{:?}", r.problems);
        assert_eq!(r.snapshots, 1);
        assert!(r.units_checked > 0 && r.packs_checked > 0);
    }
}

#[test]
fn verify_detects_flipped_byte() {
    let (_tmp, repo, snap) = setup();
    let (pack, offset) = content_pack(&repo, snap, b"sub/keep.bin");
    fs::set_permissions(&pack, fs::Permissions::from_mode(0o644)).unwrap();
    let mut bytes = fs::read(&pack).unwrap();
    bytes[offset as usize + RECORD_HEADER_LEN + 10] ^= 0x01;
    fs::write(&pack, &bytes).unwrap();

    // Quick looks only at headers, which are intact.
    let q = verify::run(&repo, Some(snap), true).unwrap();
    assert!(q.problems.is_empty(), "{:?}", q.problems);

    let f = verify::run(&repo, Some(snap), false).unwrap();
    let name = pack.file_name().unwrap().to_str().unwrap();
    assert!(
        f.problems.iter().any(|p| p.contains(name)),
        "{:?}",
        f.problems
    );
}

#[test]
fn verify_detects_missing_pack() {
    let (_tmp, repo, snap) = setup();
    let (pack, _) = content_pack(&repo, snap, b"sub/keep.bin");
    fs::remove_file(&pack).unwrap();
    let q = verify::run(&repo, None, true).unwrap();
    let name = pack.file_name().unwrap().to_str().unwrap();
    assert!(
        q.problems.iter().any(|p| p.contains(name)),
        "{:?}",
        q.problems
    );
}

#[test]
fn diff_clean_after_restore() {
    let (tmp, repo, snap) = setup();
    let out = tmp.path().join("out");
    restore_to(&repo, snap, &out);
    assert_eq!(diff::run(&repo, snap, &out, &dopts(1)).unwrap(), vec![]);
}

#[test]
fn diff_reports_each_kind() {
    let (tmp, repo, snap) = setup();
    let out = tmp.path().join("out");
    restore_to(&repo, snap, &out);
    let sub_mtime = mtime(&out.join("sub"));
    let root_mtime = mtime(&out);

    // Same-size edit, so the content hash is what catches it; keep the mtime.
    let a = out.join("a.txt");
    let a_mtime = mtime(&a);
    fs::write(&a, b"ALPHA alpha alpha\n").unwrap();
    set_mtime(&a, a_mtime);
    fs::set_permissions(out.join("m.txt"), fs::Permissions::from_mode(0o600)).unwrap();
    set_mtime(&out.join("t.txt"), T0 + 5_000_000_000);
    fs::remove_file(out.join("sub/gone.txt")).unwrap();
    fs::write(out.join("sub/new.txt"), b"new\n").unwrap();
    set_mtime(&out.join("sub"), sub_mtime);
    set_mtime(&out, root_mtime);

    let d = diff::run(&repo, snap, &out, &dopts(1)).unwrap();
    let paths: Vec<&[u8]> = d.iter().map(|x| x.path()).collect();
    assert_eq!(
        paths,
        vec![
            &b"a.txt"[..],
            b"m.txt",
            b"sub/gone.txt",
            b"sub/new.txt",
            b"t.txt"
        ],
        "{d:?}"
    );
    assert!(matches!(d[0], Difference::ContentDiffers { .. }));
    assert!(
        matches!(&d[1], Difference::ModeDiffers { snap, dir, .. } if *snap == 0o644 && *dir == 0o600),
        "{:?}",
        d[1]
    );
    assert!(matches!(d[2], Difference::Missing { .. }));
    assert!(matches!(d[3], Difference::Extra { .. }));
    assert!(matches!(d[4], Difference::MtimeDiffers { .. }));
}

#[test]
fn diff_kind_and_link_target() {
    let (tmp, repo, snap) = setup();
    let out = tmp.path().join("out");
    restore_to(&repo, snap, &out);
    fs::remove_file(out.join("link")).unwrap();
    std::os::unix::fs::symlink("elsewhere", out.join("link")).unwrap();
    fs::remove_dir(out.join("empty")).unwrap();
    fs::write(out.join("empty"), b"x").unwrap();
    let d = diff::run(&repo, snap, &out, &dopts(u64::MAX)).unwrap();
    assert!(
        d.contains(&Difference::LinkTargetDiffers {
            path: b"link".to_vec()
        }),
        "{d:?}"
    );
    assert!(
        d.contains(&Difference::KindDiffers {
            path: b"empty".to_vec()
        }),
        "{d:?}"
    );
}

#[test]
fn diff_subtree_rebases_path() {
    let (tmp, repo, snap) = setup();
    let out = tmp.path().join("sub-out");
    let opts = RestoreOptions {
        path: b"sub".to_vec(),
        ..RestoreOptions::default()
    };
    restore::run(&repo, snap, &out, &opts).unwrap();
    let d = diff::run(
        &repo,
        snap,
        &out,
        &DiffOptions {
            path: b"sub".to_vec(),
            mtime_precision_ns: 1,
            cross_fs: false,
        },
    )
    .unwrap();
    assert_eq!(d, vec![]);
}

/// Mark entries as withheld directly in the catalog (ingest cannot yet).
fn withhold(repo: &Repo, paths: &[&[u8]]) {
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    for p in paths {
        let n = conn
            .execute(
                "UPDATE entry SET status = 'screened_out', content_hash = NULL WHERE path = ?1",
                [p],
            )
            .unwrap();
        assert_eq!(n, 1);
    }
}

/// Mtimes are floored away so only the structure is compared.
fn structural() -> DiffOptions {
    dopts(u64::MAX)
}

#[test]
fn diff_withheld_absent_is_expected_missing() {
    let (tmp, repo, snap) = setup();
    let out = tmp.path().join("out");
    restore_to(&repo, snap, &out);
    withhold(&repo, &[b"t.txt"]);
    fs::remove_file(out.join("t.txt")).unwrap();
    let d = diff::run(&repo, snap, &out, &structural()).unwrap();
    assert_eq!(
        d,
        vec![Difference::ExpectedMissing {
            path: b"t.txt".to_vec(),
            status: drmanhattan_core::model::EntryStatus::ScreenedOut
        }]
    );
}

#[test]
fn diff_withheld_present_is_not_reported() {
    let (tmp, repo, snap) = setup();
    let out = tmp.path().join("out");
    restore_to(&repo, snap, &out);
    withhold(&repo, &[b"t.txt"]);
    // Present, and even with different content: nothing to compare against.
    fs::write(out.join("t.txt"), b"anything at all").unwrap();
    assert_eq!(diff::run(&repo, snap, &out, &structural()).unwrap(), vec![]);
}

#[test]
fn diff_under_withheld_dir_is_neither_extra_nor_missing() {
    let (tmp, repo, snap) = setup();
    // Absent: the dir is expected missing; its children are not Missing.
    let out = tmp.path().join("out");
    restore_to(&repo, snap, &out);
    withhold(&repo, &[b"sub"]);
    fs::remove_dir_all(out.join("sub")).unwrap();
    let d = diff::run(&repo, snap, &out, &structural()).unwrap();
    assert_eq!(d.len(), 1, "{d:?}");
    assert!(matches!(&d[0], Difference::ExpectedMissing { path, .. } if path == b"sub"));

    // Present with an unknown child: not Extra.
    let out2 = tmp.path().join("out2");
    restore_to(&repo, snap, &out2);
    fs::create_dir(out2.join("sub")).unwrap();
    fs::write(out2.join("sub/surprise.txt"), b"?").unwrap();
    assert_eq!(
        diff::run(&repo, snap, &out2, &structural()).unwrap(),
        vec![]
    );
}

#[test]
fn diff_dir_replaced_by_file_cascades_missing() {
    let (tmp, repo, snap) = setup();
    let out = tmp.path().join("out");
    restore_to(&repo, snap, &out);
    fs::remove_dir_all(out.join("sub")).unwrap();
    fs::write(out.join("sub"), b"now a file").unwrap();
    let d = diff::run(&repo, snap, &out, &structural()).unwrap();
    let got: Vec<(&[u8], &str)> = d
        .iter()
        .map(|x| {
            let k = match x {
                Difference::KindDiffers { .. } => "kind",
                Difference::Missing { .. } => "missing",
                other => panic!("unexpected {other:?}"),
            };
            (x.path(), k)
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (&b"sub"[..], "kind"),
            (b"sub/gone.txt", "missing"),
            (b"sub/keep.bin", "missing")
        ]
    );
}

#[test]
fn diff_file_replaced_by_dir_cascades_extra() {
    let (tmp, repo, snap) = setup();
    let out = tmp.path().join("out");
    restore_to(&repo, snap, &out);
    fs::remove_file(out.join("a.txt")).unwrap();
    fs::create_dir(out.join("a.txt")).unwrap();
    fs::write(out.join("a.txt/child"), b"c").unwrap();
    let d = diff::run(&repo, snap, &out, &structural()).unwrap();
    assert_eq!(
        d,
        vec![
            Difference::KindDiffers {
                path: b"a.txt".to_vec()
            },
            Difference::Extra {
                path: b"a.txt/child".to_vec()
            }
        ]
    );
}

#[test]
fn diff_precision() {
    let (tmp, repo, snap) = setup();
    let out = tmp.path().join("out");
    restore_to(&repo, snap, &out);
    // Floor every restored mtime to the second, dirs deepest-first.
    let mut all: Vec<PathBuf> = Vec::new();
    fn collect(p: &Path, all: &mut Vec<PathBuf>) {
        all.push(p.to_path_buf());
        if fs::symlink_metadata(p).unwrap().is_dir() {
            for e in fs::read_dir(p).unwrap() {
                collect(&e.unwrap().path(), all);
            }
        }
    }
    collect(&out, &mut all);
    all.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
    for p in &all {
        if fs::symlink_metadata(p).unwrap().file_type().is_symlink() {
            continue;
        }
        let m = mtime(p);
        set_mtime(p, m - m.rem_euclid(1_000_000_000));
    }
    assert_eq!(
        diff::run(&repo, snap, &out, &dopts(1_000_000_000)).unwrap(),
        vec![]
    );
    let d = diff::run(&repo, snap, &out, &dopts(1)).unwrap();
    assert!(!d.is_empty());
    assert!(
        d.iter()
            .all(|x| matches!(x, Difference::MtimeDiffers { .. })),
        "{d:?}"
    );
}
