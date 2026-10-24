mod common;

use common::TreeBuilder;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::Hash32;
use drmanhattan_core::manifest::{manifest_dir, ManifestEntry, ManifestHasher, ManifestOptions};
use drmanhattan_core::model::EntryKind;
use std::path::Path;

const T_ROOT: i64 = 1_100_000_000_000_000_000;
const T_FILE: i64 = 1_000_000_000_000_000_001;
const T_DIR: i64 = 1_200_000_000_000_000_000;

fn opts(p: u64) -> ManifestOptions {
    ManifestOptions {
        mtime_precision_ns: p,
        cross_fs: false,
    }
}

fn core_hex(root: &Path) -> String {
    manifest_dir(root, &opts(1))
        .expect("manifest")
        .core
        .to_hex()
}

fn golden_tree(root: &Path) {
    let mut t = TreeBuilder::new(root);
    t.file(b"a.txt", b"hello")
        .dir(b"d")
        .symlink(b"d/l", b"../a.txt")
        .mode(b"", 0o755)
        .mode(b"a.txt", 0o644)
        .mode(b"d", 0o700)
        .mtime(b"a.txt", T_FILE)
        .mtime(b"d", T_DIR)
        .mtime(b"", T_ROOT);
    t.finish();
}

fn u32le(v: &mut Vec<u8>, x: u32) {
    v.extend_from_slice(&x.to_le_bytes());
}

#[test]
fn golden_encoding() {
    let d = tempfile::tempdir().unwrap();
    golden_tree(d.path());
    let h = manifest_dir(d.path(), &opts(1)).unwrap();
    assert_eq!(h.entries, 4);

    let mut s = Vec::new();
    // root
    u32le(&mut s, 0);
    s.push(0);
    u32le(&mut s, 0o755);
    s.extend_from_slice(&T_ROOT.to_le_bytes());
    // a.txt
    u32le(&mut s, 5);
    s.extend_from_slice(b"a.txt");
    s.push(1);
    u32le(&mut s, 0o644);
    s.extend_from_slice(&T_FILE.to_le_bytes());
    s.extend_from_slice(&5u64.to_le_bytes());
    s.extend_from_slice(&Hash32::of(b"hello").0);
    // d
    u32le(&mut s, 1);
    s.extend_from_slice(b"d");
    s.push(0);
    u32le(&mut s, 0o700);
    s.extend_from_slice(&T_DIR.to_le_bytes());
    // d/l
    u32le(&mut s, 3);
    s.extend_from_slice(b"d/l");
    s.push(2);
    u32le(&mut s, 8);
    s.extend_from_slice(b"../a.txt");
    assert_eq!(Hash32::of(&s), h.core);

    assert_eq!(
        include_str!("vectors/manifest_golden.hex").trim(),
        h.core.to_hex()
    );
}

/// One-off generator for the committed golden vectors. Only run it when the
/// encoding is intentionally changed (which breaks every stored hash):
/// `cargo test -p drmanhattan-core --test manifest -- --ignored --nocapture print_golden`
#[test]
#[ignore]
fn print_golden() {
    let d = tempfile::tempdir().unwrap();
    golden_tree(d.path());
    println!(
        "manifest_golden.hex: {}",
        manifest_dir(d.path(), &opts(1)).unwrap().core.to_hex()
    );
    println!("manifest_ext_golden.hex: {}", synthetic_ext().0.to_hex());
}

fn dir_entry<'a>(path: &'a [u8], mode: u32, mtime_ns: i64) -> ManifestEntry<'a> {
    ManifestEntry {
        path,
        kind: EntryKind::Dir,
        mode,
        mtime_ns,
        size: 0,
        content: None,
        link_target: None,
        hardlink_first: None,
        xattrs: &[],
    }
}

/// Synthetic entry list (no filesystem): returns (ext, hand-built stream).
fn synthetic_ext() -> (Hash32, Vec<u8>) {
    let hello = Hash32::of(b"hello");
    let file = |path| ManifestEntry {
        path,
        kind: EntryKind::File,
        mode: 0o644,
        mtime_ns: T_FILE,
        size: 5,
        content: Some(hello),
        link_target: None,
        hardlink_first: None,
        xattrs: &[],
    };
    let xattrs = vec![
        (b"user.b".to_vec(), b"2".to_vec()),
        (b"user.a".to_vec(), b"11".to_vec()),
    ];
    let mut h = ManifestHasher::new(1);
    h.push(&dir_entry(b"", 0o755, T_ROOT)).unwrap();
    h.push(&ManifestEntry {
        hardlink_first: Some(1),
        ..file(b"f1")
    })
    .unwrap();
    h.push(&ManifestEntry {
        hardlink_first: Some(1),
        ..file(b"f2")
    })
    .unwrap();
    h.push(&ManifestEntry {
        xattrs: &xattrs,
        ..file(b"g")
    })
    .unwrap();
    let ext = h.finish().ext;

    let mut s = Vec::new();
    // root
    u32le(&mut s, 0);
    s.push(0);
    u32le(&mut s, 0o755);
    s.extend_from_slice(&T_ROOT.to_le_bytes());
    s.extend_from_slice(&u64::MAX.to_le_bytes());
    u32le(&mut s, 0);
    let file_bytes = |s: &mut Vec<u8>, name: &[u8], first: u64| {
        u32le(s, name.len() as u32);
        s.extend_from_slice(name);
        s.push(1);
        u32le(s, 0o644);
        s.extend_from_slice(&T_FILE.to_le_bytes());
        s.extend_from_slice(&5u64.to_le_bytes());
        s.extend_from_slice(&hello.0);
        s.extend_from_slice(&first.to_le_bytes());
    };
    file_bytes(&mut s, b"f1", 1);
    u32le(&mut s, 0);
    file_bytes(&mut s, b"f2", 1);
    u32le(&mut s, 0);
    file_bytes(&mut s, b"g", u64::MAX);
    u32le(&mut s, 2);
    for (n, v) in [(&b"user.a"[..], &b"11"[..]), (b"user.b", b"2")] {
        u32le(&mut s, n.len() as u32);
        s.extend_from_slice(n);
        u32le(&mut s, v.len() as u32);
        s.extend_from_slice(v);
    }
    (ext, s)
}

#[test]
fn golden_ext_encoding() {
    let (ext, stream) = synthetic_ext();
    assert_eq!(Hash32::of(&stream), ext);
    assert_eq!(
        include_str!("vectors/manifest_ext_golden.hex").trim(),
        ext.to_hex()
    );
}

#[test]
fn push_rejects_incomplete_entries() {
    let mut h = ManifestHasher::new(1);
    let mut e = dir_entry(b"s", 0o777, 0);
    e.kind = EntryKind::Symlink;
    assert!(matches!(h.push(&e), Err(Error::Corrupt(_))));
    e.kind = EntryKind::File;
    assert!(matches!(h.push(&e), Err(Error::Corrupt(_))));
    assert_eq!(h.finish().entries, 0);
}

#[test]
fn ignored_xattr_not_hashed_but_quarantine_is() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    golden_tree(a.path());
    golden_tree(b.path());
    let f = b.path().join("a.txt");
    if xattr::set(&f, "com.apple.provenance", b"x").is_err() {
        eprintln!("skip: xattrs unsupported");
        return;
    }
    assert_eq!(
        manifest_dir(a.path(), &opts(1)).unwrap(),
        manifest_dir(b.path(), &opts(1)).unwrap()
    );
    if xattr::set(&f, "com.apple.quarantine", b"q").is_ok() {
        assert_ne!(
            manifest_dir(a.path(), &opts(1)).unwrap().ext,
            manifest_dir(b.path(), &opts(1)).unwrap().ext
        );
    }
}

#[test]
fn uuid_independent() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    golden_tree(a.path());
    golden_tree(b.path());
    assert_eq!(core_hex(a.path()), core_hex(b.path()));
}

#[test]
fn mode_and_mtime_sensitive() {
    let base = tempfile::tempdir().unwrap();
    golden_tree(base.path());
    let want = core_hex(base.path());

    type Variant = Box<dyn Fn(&mut TreeBuilder)>;
    let variants: Vec<Variant> = vec![
        Box::new(|t| {
            t.mode(b"a.txt", 0o600);
        }),
        Box::new(|t| {
            t.mtime(b"a.txt", T_FILE + 1);
        }),
        Box::new(|t| {
            t.file(b"a.txt", b"hellO");
            t.mtime(b"a.txt", T_FILE).mode(b"a.txt", 0o644);
        }),
    ];
    for v in variants {
        let d = tempfile::tempdir().unwrap();
        golden_tree(d.path());
        let mut t = TreeBuilder::new(d.path());
        v(&mut t);
        t.finish();
        assert_ne!(core_hex(d.path()), want);
    }

    let d = tempfile::tempdir().unwrap();
    golden_tree(d.path());
    std::fs::remove_file(d.path().join("d/l")).unwrap();
    let mut t = TreeBuilder::new(d.path());
    t.symlink(b"d/l", b"../b.txt").mtime(b"d", T_DIR);
    t.finish();
    assert_ne!(core_hex(d.path()), want);
}

#[test]
fn setuid_ignored() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    golden_tree(a.path());
    golden_tree(b.path());
    let mut t = TreeBuilder::new(b.path());
    t.mode(b"a.txt", 0o4644);
    t.finish();
    assert_eq!(core_hex(a.path()), core_hex(b.path()));
}

#[test]
fn precision_floor() {
    let mk = |ns: i64| {
        let d = tempfile::tempdir().unwrap();
        let mut t = TreeBuilder::new(d.path());
        t.file(b"f", b"x").mtime(b"f", ns).mtime(b"", T_ROOT);
        t.finish();
        let h = manifest_dir(d.path(), &opts(1_000_000_000)).unwrap().core;
        let exact = manifest_dir(d.path(), &opts(1)).unwrap().core;
        (h, exact)
    };
    let (a, ea) = mk(1_000_000_000_000_000_123);
    let (b, eb) = mk(1_000_000_000_000_000_999);
    assert_eq!(a, b);
    assert_ne!(ea, eb);
}

#[test]
fn precision_zero_is_one_and_floor_is_euclidean() {
    let e = |m: i64| drmanhattan_core::manifest::ManifestEntry {
        path: b"",
        kind: drmanhattan_core::model::EntryKind::Dir,
        mode: 0o755,
        mtime_ns: m,
        size: 0,
        content: None,
        link_target: None,
        hardlink_first: None,
        xattrs: &[],
    };
    let run = |p: u64, m: i64| {
        let mut h = ManifestHasher::new(p);
        h.push(&e(m)).unwrap();
        h.finish().core
    };
    assert_eq!(run(0, 7), run(1, 7));
    // -1_500_000_000 floors to -2_000_000_000 at 1s precision.
    assert_eq!(run(1_000_000_000, -1_500_000_000), run(1, -2_000_000_000));
}

#[test]
fn ext_includes_hardlinks() {
    let copies = tempfile::tempdir().unwrap();
    let linked = tempfile::tempdir().unwrap();
    for (root, link) in [(copies.path(), false), (linked.path(), true)] {
        let mut t = TreeBuilder::new(root);
        t.file(b"one", b"same");
        if link {
            t.hardlink(b"two", b"one");
        } else {
            t.file(b"two", b"same");
        }
        t.mtime(b"one", T_FILE)
            .mtime(b"two", T_FILE)
            .mtime(b"", T_ROOT);
        t.finish();
    }
    let a = manifest_dir(copies.path(), &opts(1)).unwrap();
    let b = manifest_dir(linked.path(), &opts(1)).unwrap();
    assert_eq!(a.core, b.core);
    assert_ne!(a.ext, b.ext);
}

#[test]
fn single_member_hardlink_group_ignored() {
    // `outer` is linked to a file outside the hashed tree: only one member inside.
    let d = tempfile::tempdir().unwrap();
    let plain = tempfile::tempdir().unwrap();
    std::fs::create_dir(d.path().join("in")).unwrap();
    std::fs::write(d.path().join("outer"), b"same").unwrap();
    std::fs::hard_link(d.path().join("outer"), d.path().join("in/f")).unwrap();
    let mut t = TreeBuilder::new(plain.path());
    t.file(b"f", b"same");
    t.finish();
    let root = d.path().join("in");
    let mut t = TreeBuilder::new(plain.path());
    t.mtime(b"", T_ROOT).mtime(b"f", T_FILE);
    t.finish();
    let mut t = TreeBuilder::new(&root);
    t.mtime(b"", T_ROOT).mtime(b"f", T_FILE);
    t.finish();
    let a = manifest_dir(&root, &opts(1)).unwrap();
    let b = manifest_dir(plain.path(), &opts(1)).unwrap();
    assert_eq!(a.ext, b.ext);
}

#[test]
fn xattrs_in_ext_only() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    golden_tree(a.path());
    golden_tree(b.path());
    let name = if cfg!(target_os = "linux") {
        "user.wmk"
    } else {
        "wmk.test"
    };
    if xattr::set(b.path().join("a.txt"), name, b"v").is_err() {
        eprintln!("skip: xattrs unsupported");
        return;
    }
    // Setting an xattr may bump nothing we hash; restore file mtime to be safe.
    let mut t = TreeBuilder::new(b.path());
    t.mtime(b"a.txt", T_FILE);
    t.finish();
    let ha = manifest_dir(a.path(), &opts(1)).unwrap();
    let hb = manifest_dir(b.path(), &opts(1)).unwrap();
    assert_eq!(ha.core, hb.core);
    assert_ne!(ha.ext, hb.ext);
}
