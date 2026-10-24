mod common;

use common::TreeBuilder;
use drmanhattan_core::model::EntryKind;
use drmanhattan_core::walk::{walk, SkipReason, WalkEntry, WalkItem, WalkOptions};

fn collect(root: &std::path::Path) -> Vec<WalkItem> {
    let mut v = Vec::new();
    walk(root, &WalkOptions { cross_fs: false }, |i| {
        v.push(i);
        Ok(())
    })
    .expect("walk");
    v
}

fn entries(root: &std::path::Path) -> Vec<WalkEntry> {
    collect(root)
        .into_iter()
        .filter_map(|i| match i {
            WalkItem::Entry(e) => Some(e),
            WalkItem::Skipped { .. } => None,
        })
        .collect()
}

fn find<'a>(es: &'a [WalkEntry], path: &[u8]) -> &'a WalkEntry {
    es.iter().find(|e| e.path == path).expect("entry present")
}

#[test]
fn canonical_order() {
    let d = tempfile::tempdir().unwrap();
    let mut t = TreeBuilder::new(d.path());
    t.file(b"a/x", b"1")
        .file(b"a-c", b"2")
        .file(b"a/y", b"3")
        .file(b"b", b"4");
    t.finish();
    let es = entries(d.path());
    let paths: Vec<&[u8]> = es.iter().map(|e| e.path.as_slice()).collect();
    let want: Vec<&[u8]> = vec![b"", b"a", b"a/x", b"a/y", b"a-c", b"b"];
    assert_eq!(paths, want);
    assert_eq!(es[0].depth, 0);
    assert_eq!(es[0].kind, EntryKind::Dir);
    assert_eq!(es[0].name, b"");
    assert_eq!(find(&es, b"a/x").depth, 2);
    assert_eq!(find(&es, b"a/x").name, b"x");
}

#[test]
fn symlinks_not_followed() {
    let d = tempfile::tempdir().unwrap();
    let mut t = TreeBuilder::new(d.path());
    t.file(b"real/inner", b"x")
        .symlink(b"link", b"real")
        .symlink(b"dangling", b"nowhere/at/all");
    t.finish();
    let es = entries(d.path());
    let link = find(&es, b"link");
    assert_eq!(link.kind, EntryKind::Symlink);
    assert_eq!(link.link_target.as_deref(), Some(&b"real"[..]));
    assert!(es.iter().all(|e| !e.path.starts_with(b"link/")));
    let dg = find(&es, b"dangling");
    assert_eq!(dg.kind, EntryKind::Symlink);
    assert_eq!(dg.link_target.as_deref(), Some(&b"nowhere/at/all"[..]));
    assert_eq!(find(&es, b"real/inner").link_target, None);
}

#[test]
fn fifo_skipped() {
    let d = tempfile::tempdir().unwrap();
    let mut t = TreeBuilder::new(d.path());
    t.file(b"f", b"x").fifo(b"pipe");
    t.finish();
    let items = collect(d.path());
    assert!(items.iter().any(|i| matches!(
        i,
        WalkItem::Skipped { path, reason: SkipReason::Fifo } if path == b"pipe"
    )));
    assert!(items
        .iter()
        .all(|i| !matches!(i, WalkItem::Entry(e) if e.path == b"pipe")));
}

/// True when the scratch filesystem round-trips a ns-precision mtime.
fn ns_precision(dir: &std::path::Path) -> bool {
    let mut t = TreeBuilder::new(dir);
    t.file(b"probe", b"p")
        .mtime(b"probe", 1_700_000_000_123_456_789);
    t.finish();
    let es = entries(dir);
    find(&es, b"probe").mtime_ns == 1_700_000_000_123_456_789
}

#[test]
fn mtime_ns_preserved() {
    let d = tempfile::tempdir().unwrap();
    if !ns_precision(d.path()) {
        eprintln!("skip: filesystem lacks ns mtime precision");
        return;
    }
    let mut t = TreeBuilder::new(d.path());
    t.file(b"d/f", b"x")
        .mtime(b"d/f", 1_700_000_000_123_456_789)
        .mtime(b"d", 1_600_000_000_987_654_321);
    t.finish();
    let es = entries(d.path());
    assert_eq!(find(&es, b"d/f").mtime_ns, 1_700_000_000_123_456_789);
    assert_eq!(find(&es, b"d").mtime_ns, 1_600_000_000_987_654_321);
}

#[test]
fn negative_mtime() {
    let d = tempfile::tempdir().unwrap();
    let mut t = TreeBuilder::new(d.path());
    t.file(b"old", b"x").mtime(b"old", -86_400_000_000_000);
    t.finish();
    let es = entries(d.path());
    assert_eq!(find(&es, b"old").mtime_ns, -86_400_000_000_000);
}

#[test]
fn hostile_names() {
    let d = tempfile::tempdir().unwrap();
    let long = "x".repeat(255);
    let names: Vec<Vec<u8>> = vec![
        b"line\nbreak".to_vec(),
        b"back\\slash".to_vec(),
        b"-rf".to_vec(),
        b"a..b".to_vec(),
        long.into_bytes(),
    ];
    let mut t = TreeBuilder::new(d.path());
    for n in &names {
        t.file(n, b"x");
    }
    t.finish();
    let es = entries(d.path());
    for n in &names {
        let e = find(&es, n);
        assert_eq!(&e.name, n);
        assert_eq!(e.kind, EntryKind::File);
    }
    assert_eq!(es.len(), names.len() + 1);
}

#[cfg(target_os = "linux")]
#[test]
fn non_utf8_name() {
    let d = tempfile::tempdir().unwrap();
    let mut t = TreeBuilder::new(d.path());
    t.file(b"\xff\xfe.jpg", b"x");
    t.finish();
    let es = entries(d.path());
    assert_eq!(find(&es, b"\xff\xfe.jpg").name, b"\xff\xfe.jpg");
}

#[test]
fn hardlink_identity() {
    let d = tempfile::tempdir().unwrap();
    let mut t = TreeBuilder::new(d.path());
    t.file(b"one", b"x").hardlink(b"two", b"one");
    t.finish();
    let es = entries(d.path());
    let (a, b) = (find(&es, b"one"), find(&es, b"two"));
    assert_eq!((a.dev, a.ino), (b.dev, b.ino));
    assert_eq!(a.nlink, 2);
    assert_eq!(b.nlink, 2);
}

#[test]
fn mode_masked_and_sink_error_propagates() {
    let d = tempfile::tempdir().unwrap();
    let mut t = TreeBuilder::new(d.path());
    t.file(b"f", b"abc").mode(b"f", 0o4755);
    t.finish();
    let es = entries(d.path());
    assert_eq!(find(&es, b"f").mode, 0o4755);
    assert_eq!(find(&es, b"f").size, 3);

    let mut n = 0;
    let err = walk(d.path(), &WalkOptions { cross_fs: false }, |_| {
        n += 1;
        Err(drmanhattan_core::error::Error::Locked)
    })
    .unwrap_err();
    assert!(matches!(err, drmanhattan_core::error::Error::Locked));
    assert_eq!(n, 1);
}

#[test]
fn root_must_be_directory() {
    let d = tempfile::tempdir().unwrap();
    let mut t = TreeBuilder::new(d.path());
    t.file(b"f", b"x");
    t.finish();
    let err = walk(
        &d.path().join("f"),
        &WalkOptions { cross_fs: false },
        |_| Ok(()),
    )
    .unwrap_err();
    match err {
        drmanhattan_core::error::Error::Io { source, .. } => {
            assert_eq!(source.kind(), std::io::ErrorKind::InvalidInput)
        }
        e => panic!("unexpected {e:?}"),
    }
}
