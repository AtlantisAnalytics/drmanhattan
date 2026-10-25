//! Restore (spec §9): byte-exact, verified, atomic, behind a preflight.

mod common;

use common::{assert_units_within_packs, TreeBuilder};
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::ingest::{self, IngestOptions, IngestReport};
use drmanhattan_core::manifest::{manifest_dir, manifest_snapshot, ManifestOptions};
use drmanhattan_core::model::{EntryKind, EntryRow, EntryStatus};
use drmanhattan_core::pack::{pack_path, RECORD_HEADER_LEN};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::restore::preflight::{self, check, FsProbe};
use drmanhattan_core::restore::{self, RestoreOptions, RestoreReport};
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};
use std::ffi::OsStr;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[cfg(target_os = "macos")]
const XATTR: &str = "com.example.test";
#[cfg(not(target_os = "macos"))]
const XATTR: &str = "user.test";

fn iopts() -> IngestOptions {
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

fn whole() -> RestoreOptions {
    RestoreOptions {
        path: Vec::new(),
        hardlinks: false,
        xattrs: false,
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
    fs::create_dir(&src).unwrap();
    (tmp, repo, src)
}

fn ingest(repo: &Repo, src: &Path) -> IngestReport {
    let r = ingest::run(repo, src, &iopts()).unwrap();
    assert_units_within_packs(repo);
    r
}

fn catalog(repo: &Repo) -> Catalog {
    Catalog::open_readonly(&repo.catalog_path()).unwrap()
}

fn restore(repo: &Repo, snapshot: Id, dest: &Path, opts: &RestoreOptions) -> RestoreReport {
    restore::run(repo, snapshot, dest, opts).unwrap()
}

fn partial(dest: &Path, snapshot: Id) -> PathBuf {
    let mut name = dest.file_name().unwrap().to_os_string();
    name.push(format!(".wmk-partial-{snapshot}"));
    dest.with_file_name(name)
}

/// Every file under `a` has the same bytes under `b`, and every symlink the
/// same target, and both trees hold the same names.
fn assert_same_bytes(a: &Path, b: &Path) {
    let names = |d: &Path| {
        let mut v: Vec<Vec<u8>> = fs::read_dir(d)
            .unwrap()
            .map(|e| e.unwrap().file_name().as_bytes().to_vec())
            .collect();
        v.sort();
        v
    };
    let (na, nb) = (names(a), names(b));
    assert_eq!(na, nb, "names differ under {}", a.display());
    for n in na {
        let (pa, pb) = (a.join(OsStr::from_bytes(&n)), b.join(OsStr::from_bytes(&n)));
        let ft = fs::symlink_metadata(&pa).unwrap().file_type();
        if ft.is_symlink() {
            assert_eq!(fs::read_link(&pa).unwrap(), fs::read_link(&pb).unwrap());
        } else if ft.is_dir() {
            assert_same_bytes(&pa, &pb);
        } else {
            assert_eq!(fs::read(&pa).unwrap(), fs::read(&pb).unwrap(), "{pa:?}");
        }
    }
}

/// Make every dir under `root` writable again so the temp dir can be removed.
fn unlock(root: &Path) {
    if let Ok(md) = fs::symlink_metadata(root) {
        if md.is_dir() {
            fs::set_permissions(root, fs::Permissions::from_mode(0o755)).unwrap();
            for e in fs::read_dir(root).unwrap() {
                unlock(&e.unwrap().path());
            }
        }
    }
}

/// The Task 8 ingest tree: nested dirs, text and binary files, a symlink
/// and an exact directory mtime.
fn task8_tree(src: &Path) {
    let mut t = TreeBuilder::new(src);
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
    // Large enough for several binary-profile chunks.
    t.file(b"a/big.bin", &random(500, 3 << 20));
    t.symlink(b"c/link", b"../a/page4.html");
    t.mode(b"c/page3.html", 0o640)
        .mtime(b"c/page3.html", 1_200_000_000_000_000_001)
        .mtime(b"c/link", 1_300_000_000_000_000_007)
        .mtime(b"a/b", 1_000_000_000_123_456_789)
        .mtime(b"", 1_400_000_000_987_654_321)
        .finish();
}

#[test]
fn roundtrip_manifest_equal() {
    let (tmp, repo, src) = setup();
    task8_tree(&src);
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("out");
    let rep = restore(&repo, r.snapshot, &dest, &whole());

    let s = catalog(&repo).snapshot(r.snapshot).unwrap();
    assert_eq!(
        Some(manifest_dir(&dest, &mopts()).unwrap().core),
        s.manifest_hash
    );
    assert_same_bytes(&src, &dest);
    assert_eq!((rep.files, rep.dirs, rep.symlinks), (21, 4, 1));
    assert_eq!(rep.bytes, r.bytes_read);
    assert_eq!(rep.withheld, 0);
    assert!(rep.mtime_precision_ns >= 1);
    assert!(!partial(&dest, r.snapshot).exists());
}

#[test]
fn roundtrip_ext_with_flags() {
    let (tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"a", b"linked body")
        .hardlink(b"d/b", b"a")
        .hardlink(b"d/e/c", b"a")
        .file(b"c", b"linked body")
        .file(b"f", b"other")
        .symlink(b"l", b"f");
    xattr::set(src.join("f"), XATTR, b"file value").unwrap();
    xattr::set(src.join("d"), XATTR, b"dir value").unwrap();
    xattr::set(&src, XATTR, b"root value").unwrap();
    t.mode(b"f", 0o444).mode(b"a", 0o640).finish();
    let r = ingest(&repo, &src);
    let s = catalog(&repo).snapshot(r.snapshot).unwrap();
    let live = manifest_dir(&src, &mopts()).unwrap();
    assert_eq!(s.ext_manifest_hash, Some(live.ext));

    let with = tmp.path().join("with");
    let opts = RestoreOptions {
        path: Vec::new(),
        hardlinks: true,
        xattrs: true,
    };
    restore(&repo, r.snapshot, &with, &opts);
    let got = manifest_dir(&with, &mopts()).unwrap();
    assert_eq!(Some(got.ext), s.ext_manifest_hash);
    assert_eq!(Some(got.core), s.manifest_hash);
    let ino = |root: &Path, p: &str| fs::metadata(root.join(p)).unwrap().ino();
    assert_eq!(ino(&with, "a"), ino(&with, "d/b"));
    assert_eq!(ino(&with, "a"), ino(&with, "d/e/c"));
    assert_ne!(ino(&with, "a"), ino(&with, "c"));

    let without = tmp.path().join("without");
    restore(&repo, r.snapshot, &without, &whole());
    let got = manifest_dir(&without, &mopts()).unwrap();
    assert_eq!(Some(got.core), s.manifest_hash);
    assert_ne!(Some(got.ext), s.ext_manifest_hash);
    assert_ne!(ino(&without, "a"), ino(&without, "d/b"));
    assert!(xattr::get(without.join("f"), XATTR).unwrap().is_none());
    assert_same_bytes(&src, &without);
}

#[test]
fn dest_exists_refused() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"x", b"body");
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("out");
    fs::create_dir(&dest).unwrap();
    let err = restore::run(&repo, r.snapshot, &dest, &whole()).unwrap_err();
    assert!(matches!(err, Error::Preflight(_)), "{err}");
    assert_eq!(fs::read_dir(&dest).unwrap().count(), 0);
    assert!(!partial(&dest, r.snapshot).exists());

    // A dangling symlink counts as existing too.
    let link = tmp.path().join("dangling");
    std::os::unix::fs::symlink("nowhere", &link).unwrap();
    assert!(restore::run(&repo, r.snapshot, &link, &whole()).is_err());
    assert!(!partial(&link, r.snapshot).exists());
}

#[test]
fn stale_partial_refused() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"x", b"body");
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("out");
    let stale = partial(&dest, r.snapshot);
    fs::create_dir(&stale).unwrap();
    fs::write(stale.join("evidence"), b"keep me").unwrap();
    let err = restore::run(&repo, r.snapshot, &dest, &whole()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains(&stale.display().to_string()), "{msg}");
    assert!(msg.contains("inspect or remove"), "{msg}");
    assert_eq!(fs::read(stale.join("evidence")).unwrap(), b"keep me");
    assert!(!dest.exists());
}

#[test]
fn readonly_dirs_restored() {
    let (tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"ro/one", b"1")
        .file(b"ro/two", b"2")
        .file(b"top", b"t");
    t.mode(b"ro", 0o555)
        .mode(b"ro/one", 0o444)
        .mode(b"", 0o555)
        .mtime(b"ro", 1_100_000_000_000_000_003)
        .mtime(b"", 1_100_000_000_000_000_005)
        .finish();
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("out");
    restore(&repo, r.snapshot, &dest, &whole());
    let s = catalog(&repo).snapshot(r.snapshot).unwrap();
    assert_eq!(
        Some(manifest_dir(&dest, &mopts()).unwrap().core),
        s.manifest_hash
    );
    assert_eq!(fs::metadata(&dest).unwrap().mode() & 0o7777, 0o555);
    assert_eq!(
        fs::metadata(dest.join("ro")).unwrap().mode() & 0o7777,
        0o555
    );
    assert_same_bytes(&src, &dest);
    unlock(&src);
    unlock(&dest);
}

#[test]
fn empty_file_and_empty_dir() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src)
        .file(b"empty", b"")
        .dir(b"void")
        .file(b"full", b"x");
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("out");
    let rep = restore(&repo, r.snapshot, &dest, &whole());
    assert_eq!(fs::metadata(dest.join("empty")).unwrap().len(), 0);
    assert!(fs::metadata(dest.join("void")).unwrap().is_dir());
    assert_eq!(fs::read_dir(dest.join("void")).unwrap().count(), 0);
    let cat = catalog(&repo);
    let e = cat.entry_by_path(r.snapshot, b"empty").unwrap();
    assert_eq!(cat.content(&e.content_hash.unwrap()).unwrap().unit_count, 0);
    let s = cat.snapshot(r.snapshot).unwrap();
    assert_eq!(
        Some(manifest_dir(&dest, &mopts()).unwrap().core),
        s.manifest_hash
    );
    assert_eq!((rep.files, rep.dirs), (2, 2));
}

#[test]
fn subpath_restore() {
    let (tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"d/x", b"x body")
        .file(b"d/e/y", &random(3, 70_000))
        .symlink(b"d/e/l", b"../x")
        .file(b"d-sibling", b"not in d")
        .file(b"other", b"o");
    t.mode(b"d", 0o750)
        .mtime(b"d", 1_100_000_000_000_000_009)
        .finish();
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("val");
    let opts = RestoreOptions {
        path: b"d".to_vec(),
        ..whole()
    };
    let rep = restore(&repo, r.snapshot, &dest, &opts);
    let cat = catalog(&repo);
    let want = manifest_snapshot(&cat, r.snapshot, b"d", &mopts()).unwrap();
    assert_eq!(manifest_dir(&dest, &mopts()).unwrap().core, want.core);
    assert_same_bytes(&src.join("d"), &dest);
    assert_eq!((rep.files, rep.dirs, rep.symlinks), (2, 2, 1));
    assert_eq!(fs::metadata(&dest).unwrap().mode() & 0o7777, 0o750);
}

#[test]
fn subpath_file_restore() {
    let (tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"d/x", b"x body");
    t.mode(b"d/x", 0o604)
        .mtime(b"d/x", 1_100_000_000_000_000_011)
        .finish();
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("x-copy");
    let opts = RestoreOptions {
        path: b"d/x".to_vec(),
        ..whole()
    };
    let rep = restore(&repo, r.snapshot, &dest, &opts);
    let md = fs::symlink_metadata(&dest).unwrap();
    assert!(md.is_file());
    assert_eq!(fs::read(&dest).unwrap(), b"x body");
    assert_eq!(md.mode() & 0o7777, 0o604);
    assert_eq!(md.mtime(), 1_100_000_000);
    assert_eq!(md.mtime_nsec(), 11);
    // `manifest_dir` hashes directories only; the checks above cover the
    // single-file manifest entry (bytes, mode, mtime).
    assert_eq!((rep.files, rep.dirs, rep.symlinks), (1, 0, 0));
    assert_eq!(rep.bytes, 6);
    assert!(!partial(&dest, r.snapshot).exists());
}

#[test]
fn corrupt_unit_aborts() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src)
        .file(b"ok", b"fine")
        .file(b"victim.bin", &random(11, 100_000));
    let r = ingest(&repo, &src);
    let cat = catalog(&repo);
    let e = cat.entry_by_path(r.snapshot, b"victim.bin").unwrap();
    let unit = cat.content_units(&e.content_hash.unwrap()).unwrap()[0].clone();
    let pack = pack_path(&repo.packs_dir(), unit.pack_id);
    fs::set_permissions(&pack, fs::Permissions::from_mode(0o644)).unwrap();
    let mut bytes = fs::read(&pack).unwrap();
    bytes[unit.pack_offset as usize + RECORD_HEADER_LEN + 10] ^= 0x01;
    fs::write(&pack, &bytes).unwrap();

    let dest = tmp.path().join("out");
    let err = restore::run(&repo, r.snapshot, &dest, &whole()).unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    assert!(!dest.exists());
    assert!(partial(&dest, r.snapshot).is_dir());
}

#[test]
fn withheld_entries_skipped() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src)
        .file(b"keep", b"k")
        .file(b"bad/x", b"x")
        .file(b"bad/y", b"y")
        .file(b"gone", b"g");
    let r = ingest(&repo, &src);
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    conn.execute(
        "UPDATE entry SET status = 'screened_out' WHERE path IN (x'676f6e65', x'626164')",
        [],
    )
    .unwrap();
    drop(conn);
    let dest = tmp.path().join("out");
    let rep = restore(&repo, r.snapshot, &dest, &whole());
    // `gone`, plus the `bad` dir and its two files.
    assert_eq!(rep.withheld, 4);
    assert!(!dest.join("gone").exists() && !dest.join("bad").exists());
    let want = manifest_snapshot(&catalog(&repo), r.snapshot, b"", &mopts()).unwrap();
    assert_eq!(manifest_dir(&dest, &mopts()).unwrap().core, want.core);
}

fn probe_with(f: impl FnOnce(&mut FsProbe)) -> FsProbe {
    let mut p = FsProbe {
        case_insensitive: false,
        normalization_insensitive: false,
        accepts_non_utf8: true,
        normalizes_names: false,
        mtime_precision_ns: 1,
        path_max: 4096,
    };
    f(&mut p);
    p
}

fn row(parent: Option<Id>, name: &[u8], path: &[u8], kind: EntryKind) -> EntryRow {
    EntryRow {
        id: Id::new_v7(),
        snapshot_id: Id::from_bytes([0; 16]),
        parent_id: parent,
        name: name.to_vec(),
        path: path.to_vec(),
        kind,
        mode: 0o644,
        mtime_ns: 0,
        size: (kind == EntryKind::File).then_some(0),
        content_hash: (kind == EntryKind::File).then(|| Hash32::of(b"")),
        link_target: None,
        hardlink_group: None,
        status: EntryStatus::Ok,
    }
}

/// A root dir holding one file per name.
fn flat(names: &[&[u8]]) -> Vec<EntryRow> {
    let root = row(None, b"", b"", EntryKind::Dir);
    let mut v = vec![root.clone()];
    for n in names {
        v.push(row(Some(root.id), n, n, EntryKind::File));
    }
    v
}

#[test]
fn preflight_case_collision() {
    let mut es = flat(&[b"Foo.jpg", b"bar", b"foo.jpg"]);
    // Same names under a different parent do not collide with the root's.
    let sub = row(Some(es[0].id), b"sub", b"sub", EntryKind::Dir);
    es.push(row(
        Some(sub.id),
        b"FOO.JPG",
        b"sub/FOO.JPG",
        EntryKind::File,
    ));
    es.push(sub);
    let msgs = check(&es, &probe_with(|p| p.case_insensitive = true), 10);
    assert_eq!(msgs, vec!["case collision: Foo.jpg vs foo.jpg".to_string()]);
    assert!(check(&es, &probe_with(|_| {}), 10).is_empty());
}

#[test]
fn preflight_normalization_collision() {
    let (nfc, nfd): (&[u8], &[u8]) = (b"caf\xc3\xa9", b"cafe\xcc\x81");
    let es = flat(&[nfc, nfd]);
    // Case-sensitive but normalization-insensitive (APFS case-sensitive).
    let msgs = check(&es, &probe_with(|p| p.normalization_insensitive = true), 10);
    assert_eq!(msgs.len(), 1, "{msgs:?}");
    assert!(msgs[0].starts_with("case collision: "));
    // Case-insensitive applies NFD before lowercasing.
    let es = flat(&[b"CAF\xc3\x89", nfd]);
    assert_eq!(
        check(&es, &probe_with(|p| p.case_insensitive = true), 10).len(),
        1
    );
    // Raw-byte filesystems hold both.
    assert!(check(&flat(&[nfc, nfd]), &probe_with(|_| {}), 10).is_empty());
}

#[test]
fn preflight_normalizing_fs_rejects_rewritten_names() {
    let (nfc, nfd): (&[u8], &[u8]) = (b"caf\xc3\xa9", b"cafe\xcc\x81");
    let es = flat(&[nfc, nfd, b"plain", b"\xff"]);
    let msgs = check(&es, &probe_with(|p| p.normalizes_names = true), 10);
    assert_eq!(
        msgs,
        // Printable UTF-8 is shown as is (the one display renderer).
        vec!["name not representable: caf\u{e9}".to_string()]
    );
}

#[test]
fn preflight_path_too_long() {
    let long = vec![b'x'; 1100];
    let es = flat(&[&long]);
    let msgs = check(&es, &probe_with(|p| p.path_max = 1024), 10);
    assert_eq!(msgs.len(), 1, "{msgs:?}");
    assert!(
        msgs[0].starts_with("path too long (1111 > 1023): xxx"),
        "{}",
        msgs[0]
    );

    // PATH_MAX counts the NUL: dest + '/' + path must stay below it.
    let es = flat(&[b"abcd"]);
    let p = probe_with(|p| p.path_max = 16);
    assert_eq!(check(&es, &p, 11).len(), 1); // 11 + 1 + 4 = 16
    assert!(check(&es, &p, 10).is_empty()); // 15 bytes + NUL fits
}

#[test]
fn preflight_non_utf8_on_strict_fs() {
    let es = flat(&[b"\xff", b"ok"]);
    let msgs = check(&es, &probe_with(|p| p.accepts_non_utf8 = false), 10);
    assert_eq!(msgs, vec!["name not representable: \\xff".to_string()]);
    assert!(check(&es, &probe_with(|_| {}), 10).is_empty());
}

#[test]
fn probe_reports_host_fs_and_cleans_up() {
    let tmp = tempfile::tempdir().unwrap();
    let p = preflight::probe(tmp.path()).unwrap();
    assert_eq!(
        fs::read_dir(tmp.path()).unwrap().count(),
        0,
        "probe dir left"
    );
    assert!(p.mtime_precision_ns >= 1 && p.mtime_precision_ns <= 1_000_000_000);
    assert!(p.path_max >= 1024);
    assert!(!p.normalizes_names);
    #[cfg(target_os = "macos")]
    {
        // APFS: always normalization-insensitive, never non-UTF-8.
        assert!(p.normalization_insensitive);
        assert!(!p.accepts_non_utf8);
        assert_eq!(p.path_max, 1024);
    }
    #[cfg(target_os = "linux")]
    {
        assert!(!p.case_insensitive && !p.normalization_insensitive);
        assert!(p.accepts_non_utf8);
    }
    assert!(preflight::probe(&tmp.path().join("missing")).is_err());
}

#[test]
fn injected_probe_refuses_before_writing() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"some/deeper/file.txt", b"x");
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("out");
    let partial_len = partial(&dest, r.snapshot).as_os_str().len();
    let probe = probe_with(|p| p.path_max = partial_len + 8);
    let err = restore::run_with_probe(&repo, r.snapshot, &dest, &whole(), Some(probe)).unwrap_err();
    let Error::Preflight(msgs) = err else {
        panic!("expected preflight error, got {err}");
    };
    assert!(
        msgs.iter().any(|m| m.contains("some/deeper/file.txt")),
        "{msgs:?}"
    );
    assert!(!dest.exists());
    assert!(!partial(&dest, r.snapshot).exists());
}

/// The names `walk::hostile_names` covers, plus non-UTF-8 where the
/// filesystem allows it.
#[test]
fn hostile_names_roundtrip() {
    let (tmp, repo, src) = setup();
    let mut names: Vec<Vec<u8>> = vec![
        b"line\nbreak".to_vec(),
        b"back\\slash".to_vec(),
        b"-rf".to_vec(),
        b"a..b".to_vec(),
        "x".repeat(255).into_bytes(),
        b" lead and trail ".to_vec(),
        "caf\u{e9}".into(),
    ];
    if cfg!(target_os = "linux") {
        names.push(b"\xff\xfe.jpg".to_vec());
    }
    let mut t = TreeBuilder::new(&src);
    for n in &names {
        t.file(n, n);
        let mut sub = b"dir-".to_vec();
        sub.extend_from_slice(&n[..n.len().min(200)]);
        sub.extend_from_slice(b"/inner");
        t.file(&sub, b"inner");
    }
    t.finish();
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("out");
    restore(&repo, r.snapshot, &dest, &whole());
    let s = catalog(&repo).snapshot(r.snapshot).unwrap();
    assert_eq!(
        Some(manifest_dir(&dest, &mopts()).unwrap().core),
        s.manifest_hash
    );
    assert_same_bytes(&src, &dest);
}

#[cfg(target_os = "linux")]
#[test]
fn linux_tree_on_macos_collides() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"Foo", b"1").file(b"foo", b"2");
    let r = ingest(&repo, &src);
    let dest = tmp.path().join("out");
    let probe = probe_with(|p| p.case_insensitive = true);
    let err = restore::run_with_probe(&repo, r.snapshot, &dest, &whole(), Some(probe)).unwrap_err();
    let Error::Preflight(msgs) = err else {
        panic!("expected preflight error, got {err}");
    };
    assert_eq!(msgs, vec!["case collision: Foo vs foo".to_string()]);
    assert!(!dest.exists());
    assert!(!partial(&dest, r.snapshot).exists());
}
