//! Format-stability gate (spec §14.2): the committed repository at
//! `tests/fixtures/repo-v1/` must restore byte-exact on every future version.
//!
//! `generate_fixture` (ignored, and refusing to run unless
//! `WMK_REGENERATE_FIXTURE=1` is set) rebuilds it. Regenerating is a
//! deliberate format change: unless only the test inputs changed, it needs a
//! `format_version` bump. Commit the result together with
//! `tests/fixtures/repo-v1.expected.toml`:
//!
//! ```text
//! WMK_REGENERATE_FIXTURE=1 cargo test -p drmanhattan-core --test fixture_repo -- --ignored generate_fixture
//! ```

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::ingest::record::read_record;
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::manifest::{manifest_dir, manifest_snapshot, ManifestOptions};
use drmanhattan_core::model::{EntryKind, SnapshotStatus};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::restore::{self, preflight, RestoreOptions};
use drmanhattan_core::unitread::{UnitReader, DEFAULT_CACHE_BYTES};
use drmanhattan_core::verify;
use drmanhattan_core::walk::{walk, WalkItem, WalkOptions};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

const MIB: u64 = 1 << 20;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Expected {
    manifest_hash: String,
    ext_manifest_hash: String,
    /// Every regular file of the tree (hard-link members included): path to
    /// BLAKE3 hex.
    files: BTreeMap<String, String>,
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests/fixtures")
}

fn fixture_repo() -> PathBuf {
    fixtures_dir().join("repo-v1")
}

fn expected_path() -> PathBuf {
    fixtures_dir().join("repo-v1.expected.toml")
}

fn mopts(p: u64) -> ManifestOptions {
    ManifestOptions {
        mtime_precision_ns: p,
        cross_fs: false,
    }
}

/// A `[files]` key: the path itself, or escaped if it is not UTF-8.
fn path_key(path: &[u8]) -> String {
    match std::str::from_utf8(path) {
        Ok(s) => s.to_string(),
        Err(_) => path.escape_ascii().to_string(),
    }
}

/// BLAKE3 of every regular file under `root`, keyed by `path_key`.
fn file_hashes(root: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    walk(root, &WalkOptions { cross_fs: false }, |item| {
        if let WalkItem::Entry(e) = item {
            if e.kind == EntryKind::File {
                let bytes = fs::read(root.join(OsStr::from_bytes(&e.path))).unwrap();
                out.insert(path_key(&e.path), blake3::hash(&bytes).to_hex().to_string());
            }
        }
        Ok(())
    })
    .unwrap();
    out
}

fn dir_size(p: &Path) -> u64 {
    fs::read_dir(p)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            let md = e.metadata().unwrap();
            if md.is_dir() {
                dir_size(&e.path())
            } else {
                md.len()
            }
        })
        .sum()
}

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dest = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_tree(&e.path(), &dest);
        } else {
            fs::copy(e.path(), &dest).unwrap();
        }
    }
}

// ---- the deterministic tree ----

const T: i64 = 1_600_000_000_000_000_000;

fn html(title: &str, i: usize) -> Vec<u8> {
    let mut s = String::from(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <link rel=\"stylesheet\" href=\"/assets/style.css\">\n",
    );
    s.push_str(&format!("<title>{title}</title>\n</head>\n<body>\n<nav>\n"));
    // Shared header template.
    for n in 0..60 {
        s.push_str(&format!(
            "  <a class=\"nav-item\" href=\"/section-{n:02}.html\">Section {n:02}: movements and complications</a>\n"
        ));
    }
    s.push_str("</nav>\n<main>\n");
    // Unique body.
    for p in 0..(12 + i % 5) {
        s.push_str(&format!(
            "<p>Page {i} paragraph {p}: the escapement releases the gear train \
             {} times, and the balance wheel swings {} degrees.</p>\n",
            (i + 3) * (p + 7),
            (i * 31 + p * 17) % 360
        ));
    }
    s.push_str("</main>\n<footer>\n");
    // Shared footer template.
    for n in 0..40 {
        s.push_str(&format!(
            "  <span class=\"footer-link\">Archive {n:02} &middot; watchmaker fixture</span>\n"
        ));
    }
    s.push_str("</footer>\n</body>\n</html>\n");
    s.into_bytes()
}

/// A PNG signature and IHDR chunk, then `tag`.
fn png(tag: u8) -> Vec<u8> {
    let mut v = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00".to_vec();
    v.extend_from_slice(&[tag; 13]);
    v
}

/// 30 files (25 HTML sharing templates, a stylesheet, two PNG-magic files,
/// one empty file, one of exactly 2048 bytes), nested dirs, two symlinks
/// and one hard-link pair, with fixed modes and mtimes.
fn fixture_tree(root: &Path) {
    let mut t = TreeBuilder::new(root);
    let mut files: Vec<(String, Vec<u8>, u32)> = Vec::new();
    for i in 1..=10 {
        files.push((format!("site/page-{i:02}.html"), html("Page", i), 0o644));
    }
    for i in 1..=8 {
        files.push((
            format!("site/blog/post-{i:02}.html"),
            html("Post", 10 + i),
            0o644,
        ));
    }
    for i in 1..=7 {
        files.push((
            format!("site/blog/2019/archive-{i:02}.html"),
            html("Archive", 20 + i),
            0o600,
        ));
    }
    let css: String = (0..80)
        .map(|n| {
            format!(
                ".nav-item-{n} {{ margin: {n}px; color: #{:06x}; }}\n",
                n * 4099
            )
        })
        .collect();
    files.push(("assets/style.css".into(), css.into_bytes(), 0o644));
    files.push(("assets/img/dial.png".into(), png(1), 0o444));
    files.push(("assets/img/hand.png".into(), png(2), 0o444));
    files.push(("shared/empty.txt".into(), Vec::new(), 0o600));
    let blob: Vec<u8> = (0..2048u32).map(|i| (i * 31 + 7) as u8).collect();
    files.push(("assets/blob-2048.bin".into(), blob, 0o755));
    assert_eq!(files.len(), 30);

    for (i, (path, bytes, mode)) in files.iter().enumerate() {
        let ns = T + i as i64 * 1_000_000_007;
        t.file(path.as_bytes(), bytes)
            .mode(path.as_bytes(), *mode)
            .mtime(path.as_bytes(), ns);
    }
    // Before 1970, with nanoseconds.
    t.mtime(b"shared/empty.txt", -1_234_567_890_123_456_789);
    t.symlink(b"site/home.html", b"page-01.html")
        .symlink(b"assets/latest", b"../site/blog/post-08.html")
        .hardlink(b"shared/index-copy.html", b"site/page-01.html");
    for (dir, mode, ns) in [
        (&b"site"[..], 0o755, T + 900_000_000_001),
        (b"site/blog", 0o755, T + 900_000_000_002),
        (b"site/blog/2019", 0o750, T + 900_000_000_003),
        (b"assets", 0o755, T + 900_000_000_004),
        (b"assets/img", 0o700, T + 900_000_000_005),
        (b"shared", 0o1777, T + 900_000_000_006),
        (b"", 0o755, T + 900_000_000_007),
    ] {
        t.mode(dir, mode).mtime(dir, ns);
    }
    t.finish();
}

// ---- tests ----

#[test]
#[ignore = "regenerates the committed fixture; run only for a deliberate format break"]
fn generate_fixture() {
    assert_eq!(
        std::env::var("WMK_REGENERATE_FIXTURE").as_deref(),
        Ok("1"),
        "regenerating the fixture is a deliberate format change; set WMK_REGENERATE_FIXTURE=1"
    );
    let dir = fixture_repo();
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    fs::create_dir(&src).unwrap();
    fixture_tree(&src);

    let repo = Repo::init(&dir).unwrap();
    let opts = IngestOptions {
        note: Some("format fixture repo-v1".into()),
        // Keep this machine's name and paths out of the committed fixture.
        host_override: Some("fixture-host".into()),
        source_root_override: Some(b"/fixture/src".to_vec()),
        ..IngestOptions::default()
    };
    let r = ingest::run(&repo, &src, &opts).unwrap();
    let live = manifest_dir(&src, &mopts(1)).unwrap();
    assert_eq!(r.manifest_hash, live.core);
    let snap = Catalog::open_readonly(&repo.catalog_path())
        .unwrap()
        .snapshot(r.snapshot)
        .unwrap();
    assert_eq!(snap.ext_manifest_hash, Some(live.ext));

    // Fold the WAL into the database file and close every connection.
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    let busy: i64 = conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
        .unwrap();
    assert_eq!(busy, 0, "checkpoint blocked");
    conn.close().unwrap();
    for name in ["catalog.sqlite-wal", "catalog.sqlite-shm"] {
        let p = dir.join(name);
        if let Ok(md) = fs::metadata(&p) {
            assert!(name.ends_with("-shm") || md.len() == 0, "{name} not empty");
            fs::remove_file(&p).unwrap();
        }
    }
    let _ = fs::remove_file(dir.join("wmk.lock"));
    let _ = fs::remove_dir_all(dir.join("tmp"));
    fs::write(
        dir.join(".gitignore"),
        "# Only wmk.toml, catalog.sqlite and packs/ are part of the fixture.\n\
         *-wal\n*-shm\nwmk.lock\ntmp/\n",
    )
    .unwrap();

    let expected = Expected {
        manifest_hash: live.core.to_hex(),
        ext_manifest_hash: live.ext.to_hex(),
        files: file_hashes(&src),
    };
    assert_eq!(expected.files.len(), 31);
    fs::write(expected_path(), toml::to_string(&expected).unwrap()).unwrap();
    let size = dir_size(&dir);
    assert!(size < MIB, "fixture is {size} bytes");
}

#[test]
fn fixture_restores_exactly() {
    let expected: Expected = toml::from_str(&fs::read_to_string(expected_path()).unwrap()).unwrap();
    // A copy: opening the catalog may write WAL files next to it.
    let tmp = tempfile::tempdir().unwrap();
    let copy = tmp.path().join("repo-v1");
    copy_tree(&fixture_repo(), &copy);
    let repo = Repo::open(&copy).unwrap();

    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let snaps = cat.snapshots().unwrap();
    assert_eq!(snaps.len(), 1);
    let snap = &snaps[0];
    assert_eq!(snap.status, SnapshotStatus::Complete);
    assert_eq!(snap.host, "fixture-host");
    assert_eq!(snap.source_root, b"/fixture/src");
    // The snapshot record in the pack (zstd-compressed) carries the same.
    let reader = UnitReader::new(&repo, DEFAULT_CACHE_BYTES);
    let rec = read_record(&reader, &cat, &snap.record_unit.unwrap()).unwrap();
    assert_eq!(rec.snapshot.host, "fixture-host");
    assert_eq!(rec.snapshot.source_root, b"/fixture/src");
    assert_eq!(
        snap.manifest_hash.map(|h| h.to_hex()),
        Some(expected.manifest_hash.clone())
    );
    assert_eq!(
        snap.ext_manifest_hash.map(|h| h.to_hex()),
        Some(expected.ext_manifest_hash.clone())
    );
    let from_catalog = manifest_snapshot(&cat, snap.id, b"", &mopts(1)).unwrap();
    assert_eq!(from_catalog.core.to_hex(), expected.manifest_hash);
    assert_eq!(from_catalog.ext.to_hex(), expected.ext_manifest_hash);

    let v = verify::run(&repo, None, false).unwrap();
    assert!(v.problems.is_empty(), "verify: {:?}", v.problems);
    assert_eq!(v.snapshots, 1);

    let out = tmp.path().join("out");
    let opts = RestoreOptions {
        path: Vec::new(),
        hardlinks: true,
        xattrs: false,
    };
    restore::run(&repo, snap.id, &out, &opts).unwrap();
    let p = preflight::probe(tmp.path()).unwrap().mtime_precision_ns;
    if cfg!(any(target_os = "macos", target_os = "linux")) {
        // APFS and ext4 keep nanoseconds: a coarser result is a probe bug.
        assert_eq!(p, 1, "probed mtime precision of the temp filesystem");
    }
    let restored = manifest_dir(&out, &mopts(p)).unwrap();
    if p == 1 {
        assert_eq!(restored.core.to_hex(), expected.manifest_hash);
        assert_eq!(restored.ext.to_hex(), expected.ext_manifest_hash);
    } else {
        // A coarser filesystem floored the mtimes: compare at its precision.
        let floored = manifest_snapshot(&cat, snap.id, b"", &mopts(p)).unwrap();
        assert_eq!(restored.core, floored.core);
        assert_eq!(restored.ext, floored.ext);
    }
    assert_eq!(file_hashes(&out), expected.files);
}
