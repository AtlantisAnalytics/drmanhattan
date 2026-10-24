use drmanhattan_core::catalog::writer::{spawn_catalog_writer, CatalogOp, CatalogStats};
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::compress::Compression;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::model::{
    ContentRow, EntryKind, EntryRow, EntryStatus, PackRow, SnapshotRow, SnapshotStatus, UnitRow,
};
use drmanhattan_core::pack::UnitKind;
use drmanhattan_core::sniff::{Format, ProfileKind};
use std::sync::mpsc::channel;

fn snap(id: Id, status: SnapshotStatus, started_at: i64) -> SnapshotRow {
    SnapshotRow {
        id,
        status,
        source_root: b"/src".to_vec(),
        host: "h".into(),
        tool_version: "0".into(),
        started_at,
        completed_at: None,
        note: None,
        manifest_hash: None,
        ext_manifest_hash: None,
        source_manifest_hash: None,
        record_unit: None,
        entry_count: None,
        file_bytes: None,
        screened_out: 0,
        label_pattern: None,
    }
}

fn entry(snapshot: Id, parent: Option<Id>, name: &[u8], path: &[u8]) -> EntryRow {
    EntryRow {
        id: Id::new_v7(),
        snapshot_id: snapshot,
        parent_id: parent,
        name: name.to_vec(),
        path: path.to_vec(),
        kind: EntryKind::File,
        mode: 0o644,
        mtime_ns: -5,
        size: Some(0),
        content_hash: None,
        link_target: None,
        hardlink_group: None,
        status: EntryStatus::Ok,
    }
}

fn unit(h: Hash32, pack: Id, off: i64) -> UnitRow {
    UnitRow {
        hash: h,
        kind: UnitKind::RawChunk,
        raw_size: 10,
        stored_size: 8,
        compression: Compression::Zstd,
        pack_id: pack,
        pack_offset: off,
    }
}

fn setup() -> (tempfile::TempDir, std::path::PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("catalog.sqlite");
    Catalog::open(&path).unwrap();
    (tmp, path)
}

#[test]
fn schema_has_all_tables() {
    let (_t, path) = setup();
    let conn = rusqlite::Connection::open(&path).unwrap();
    let mut names: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    names.sort();
    let mut want = vec![
        "snapshot",
        "entry",
        "content",
        "content_unit",
        "unit",
        "pack",
        "xattr",
        "media",
        "label",
        "provenance",
        "screen_hit",
        "purge_log",
    ];
    want.sort();
    assert_eq!(names, want);
    let v: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 1);
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
}

#[test]
fn blob_order_is_bytewise() {
    let (_t, path) = setup();
    let sid = Id::new_v7();
    let (tx, rx) = channel();
    let h = spawn_catalog_writer(path.clone(), rx);
    tx.send(CatalogOp::UpsertSnapshot(snap(
        sid,
        SnapshotStatus::InProgress,
        1,
    )))
    .unwrap();
    for n in [&b"a-c"[..], b"a", b"\xff", b"B"] {
        tx.send(CatalogOp::InsertEntry(entry(sid, None, n, n)))
            .unwrap();
    }
    drop(tx);
    let stats = h.join().unwrap().unwrap();
    assert_eq!(stats, CatalogStats { ops: 5, commits: 1 });
    let cat = Catalog::open_readonly(&path).unwrap();
    let names: Vec<Vec<u8>> = cat
        .children(sid, None)
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(
        names,
        vec![
            b"B".to_vec(),
            b"a".to_vec(),
            b"a-c".to_vec(),
            b"\xff".to_vec()
        ]
    );
}

#[test]
fn writer_commits_only_on_commit() {
    let (_t, path) = setup();
    let sid = Id::new_v7();
    let count = |p: &std::path::Path| -> i64 {
        let c = rusqlite::Connection::open(p).unwrap();
        c.query_row("SELECT count(*) FROM entry", [], |r| r.get(0))
            .unwrap()
    };
    let (tx, rx) = channel();
    let h = spawn_catalog_writer(path.clone(), rx);
    tx.send(CatalogOp::UpsertSnapshot(snap(
        sid,
        SnapshotStatus::InProgress,
        1,
    )))
    .unwrap();
    tx.send(CatalogOp::Commit).unwrap();
    tx.send(CatalogOp::InsertEntry(entry(sid, None, b"", b"")))
        .unwrap();
    // Wait until the writer has consumed the entry op: a later Commit-free
    // round trip is unavailable, so poll the writer's progress via a lock-free
    // check that the snapshot (committed) is visible, then give it time.
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert_eq!(count(&path), 0);
    tx.send(CatalogOp::Commit).unwrap();
    let mut seen = 0;
    for _ in 0..100 {
        seen = count(&path);
        if seen == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(seen, 1);
    drop(tx);
    assert_eq!(h.join().unwrap().unwrap().commits, 2);
}

#[test]
fn writer_error_rolls_back_and_returns() {
    let (_t, path) = setup();
    let sid = Id::new_v7();
    let (tx, rx) = channel();
    let h = spawn_catalog_writer(path.clone(), rx);
    tx.send(CatalogOp::UpsertSnapshot(snap(
        sid,
        SnapshotStatus::InProgress,
        1,
    )))
    .unwrap();
    let e = entry(sid, None, b"x", b"x");
    tx.send(CatalogOp::InsertEntry(e.clone())).unwrap();
    tx.send(CatalogOp::InsertEntry(e)).unwrap();
    let r = h.join().unwrap();
    assert!(matches!(r, Err(Error::Sqlite(_))));
    let cat = Catalog::open_readonly(&path).unwrap();
    assert!(cat.snapshots().unwrap().is_empty());
}

#[test]
fn resolve_snapshot_forms() {
    let (_t, path) = setup();
    let mk = |first: [u8; 4], tail: u8| {
        let mut b = [0u8; 16];
        b[..4].copy_from_slice(&first);
        b[15] = tail;
        Id(b)
    };
    let a = mk([0xAB, 0xCD, 0xEF, 0x01], 1);
    let b = mk([0xAB, 0xCD, 0xEF, 0x01], 2);
    let c = mk([0x11, 0x22, 0x33, 0x44], 3);
    let d = mk([0x55, 0x66, 0x77, 0x88], 4);
    let (tx, rx) = channel();
    let h = spawn_catalog_writer(path.clone(), rx);
    tx.send(CatalogOp::UpsertSnapshot(snap(
        a,
        SnapshotStatus::Complete,
        10,
    )))
    .unwrap();
    tx.send(CatalogOp::UpsertSnapshot(snap(
        b,
        SnapshotStatus::Complete,
        20,
    )))
    .unwrap();
    tx.send(CatalogOp::UpsertSnapshot(snap(
        c,
        SnapshotStatus::Complete,
        30,
    )))
    .unwrap();
    // Newer but not complete: must not be "latest".
    tx.send(CatalogOp::UpsertSnapshot(snap(
        d,
        SnapshotStatus::InProgress,
        40,
    )))
    .unwrap();
    drop(tx);
    h.join().unwrap().unwrap();
    let cat = Catalog::open_readonly(&path).unwrap();
    assert_eq!(cat.resolve_snapshot("latest").unwrap(), c);
    assert_eq!(cat.resolve_snapshot(&c.to_string()).unwrap(), c);
    assert_eq!(cat.resolve_snapshot("11223344").unwrap(), c);
    assert_eq!(cat.resolve_snapshot("1122-3344").unwrap(), c);
    assert_eq!(cat.resolve_snapshot("55667788").unwrap(), d);
    assert!(matches!(
        cat.resolve_snapshot("abcdef01"),
        Err(Error::Ambiguous(_))
    ));
    assert!(matches!(
        cat.resolve_snapshot("ABCDEF01"),
        Err(Error::Ambiguous(_))
    ));
    // A longer prefix disambiguates.
    assert_eq!(cat.resolve_snapshot(&b.to_string()).unwrap(), b);
    assert!(matches!(
        cat.resolve_snapshot("deadbeef"),
        Err(Error::NotFound(_))
    ));
    match cat.resolve_snapshot("abc") {
        Err(Error::NotFound(m)) => assert!(m.contains("too short")),
        other => panic!("{other:?}"),
    }
    assert_eq!(cat.snapshot(a).unwrap().status, SnapshotStatus::Complete);
    assert!(matches!(
        cat.snapshot(Id::new_v7()),
        Err(Error::NotFound(_))
    ));
}

#[test]
fn content_units_in_seq_order() {
    let (_t, path) = setup();
    let pack = Id::new_v7();
    let ch = Hash32::of(b"whole");
    // Unit hashes deliberately not in hash order relative to seq.
    let u: Vec<Hash32> = [b"z", b"a", b"m"].iter().map(|b| Hash32::of(*b)).collect();
    let (tx, rx) = channel();
    let h = spawn_catalog_writer(path.clone(), rx);
    tx.send(CatalogOp::UpsertPack(PackRow {
        id: pack,
        size: 1,
        sealed: false,
        blake3: None,
    }))
    .unwrap();
    for (i, x) in u.iter().enumerate() {
        tx.send(CatalogOp::InsertUnit(unit(*x, pack, i as i64 * 100)))
            .unwrap();
    }
    let crow = ContentRow {
        hash: ch,
        size: 30,
        format: Some(Format::Png),
        profile: ProfileKind::Binary,
        unit_count: 3,
    };
    tx.send(CatalogOp::InsertContent(crow.clone(), u.clone()))
        .unwrap();
    // Re-sending is a no-op.
    tx.send(CatalogOp::InsertContent(crow.clone(), u.clone()))
        .unwrap();
    tx.send(CatalogOp::InsertMedia {
        content: ch,
        width: 3,
        height: 4,
    })
    .unwrap();
    drop(tx);
    h.join().unwrap().unwrap();
    let cat = Catalog::open_readonly(&path).unwrap();
    let got: Vec<Hash32> = cat
        .content_units(&ch)
        .unwrap()
        .into_iter()
        .map(|r| r.hash)
        .collect();
    assert_eq!(got, u);
    assert_eq!(cat.content(&ch).unwrap(), crow);
    assert!(cat.has_content(&ch).unwrap());
    assert!(!cat.has_content(&Hash32::of(b"nope")).unwrap());
    assert!(cat.has_unit(&u[0]).unwrap());
    assert_eq!(cat.unit(&u[1]).unwrap(), unit(u[1], pack, 100));
    assert!(matches!(
        cat.unit(&Hash32::of(b"nope")),
        Err(Error::NotFound(_))
    ));
}

#[test]
fn entry_queries_and_xattrs() {
    let (_t, path) = setup();
    let sid = Id::new_v7();
    let ch = Hash32::of(b"c");
    let root = entry(sid, None, b"", b"");
    let mut f = entry(sid, Some(root.id), b"f", b"f");
    f.content_hash = Some(ch);
    let (tx, rx) = channel();
    let h = spawn_catalog_writer(path.clone(), rx);
    tx.send(CatalogOp::UpsertSnapshot(snap(
        sid,
        SnapshotStatus::InProgress,
        1,
    )))
    .unwrap();
    tx.send(CatalogOp::InsertContent(
        ContentRow {
            hash: ch,
            size: 0,
            format: None,
            profile: ProfileKind::Text,
            unit_count: 0,
        },
        vec![],
    ))
    .unwrap();
    tx.send(CatalogOp::InsertEntry(root.clone())).unwrap();
    tx.send(CatalogOp::InsertEntry(f.clone())).unwrap();
    tx.send(CatalogOp::InsertXattr(
        f.id,
        b"user.b".to_vec(),
        b"2".to_vec(),
    ))
    .unwrap();
    tx.send(CatalogOp::InsertXattr(
        f.id,
        b"user.a".to_vec(),
        b"1".to_vec(),
    ))
    .unwrap();
    drop(tx);
    h.join().unwrap().unwrap();
    let cat = Catalog::open_readonly(&path).unwrap();
    assert_eq!(cat.entry(f.id).unwrap(), f);
    assert_eq!(cat.entry_by_path(sid, b"f").unwrap(), f);
    assert_eq!(cat.children(sid, Some(root.id)).unwrap(), vec![f.clone()]);
    assert_eq!(cat.children(sid, None).unwrap(), vec![root]);
    assert_eq!(cat.entries_with_content(&ch).unwrap(), vec![f.clone()]);
    assert_eq!(
        cat.xattrs(f.id).unwrap(),
        vec![
            (b"user.a".to_vec(), b"1".to_vec()),
            (b"user.b".to_vec(), b"2".to_vec())
        ]
    );
    assert!(matches!(
        cat.entry_by_path(sid, b"missing"),
        Err(Error::NotFound(_))
    ));
}

#[test]
fn snapshot_upsert_updates_all_columns() {
    let (_t, path) = setup();
    let sid = Id::new_v7();
    let mut s = snap(sid, SnapshotStatus::InProgress, 1);
    let (tx, rx) = channel();
    let h = spawn_catalog_writer(path.clone(), rx);
    tx.send(CatalogOp::UpsertSnapshot(s.clone())).unwrap();
    s.status = SnapshotStatus::Complete;
    s.completed_at = Some(9);
    s.manifest_hash = Some(Hash32::of(b"m"));
    s.entry_count = Some(3);
    s.note = Some("n".into());
    tx.send(CatalogOp::UpsertSnapshot(s.clone())).unwrap();
    drop(tx);
    h.join().unwrap().unwrap();
    let cat = Catalog::open_readonly(&path).unwrap();
    assert_eq!(cat.snapshot(sid).unwrap(), s);
    assert_eq!(cat.snapshots().unwrap().len(), 1);
}

#[test]
fn future_user_version_rejected() {
    let (_t, path) = setup();
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.pragma_update(None, "user_version", 2).unwrap();
    }
    assert!(matches!(Catalog::open(&path), Err(Error::Corrupt(_))));
    assert!(matches!(
        Catalog::open_readonly(&path),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn readonly_connection_cannot_write() {
    let (_t, path) = setup();
    let cat = Catalog::open_readonly(&path).unwrap();
    assert!(cat.snapshots().unwrap().is_empty());
    let c =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert!(c.execute("DELETE FROM snapshot", []).is_err());
}
