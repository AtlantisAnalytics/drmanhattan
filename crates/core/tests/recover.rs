//! `recover::build`: a complete, checked catalog rebuilt from the packs alone.

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::compress::Compression;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::ingest::record::record_chunks;
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::pack::RECORD_HEADER_LEN;
use drmanhattan_core::pack::{pack_path, PackReader, PackWriter, RecordHeader, UnitKind};
use drmanhattan_core::recover::{self, RecoverOptions, RecoverReport};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::unitread::UnitReader;
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[cfg(target_os = "macos")]
const XATTR: &str = "com.example.test";
#[cfg(not(target_os = "macos"))]
const XATTR: &str = "user.test";

fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    ChaCha8Rng::seed_from_u64(seed).fill_bytes(&mut v);
    v
}

/// A 2 KiB PNG whose header gives `width` x `height`.
fn png(width: u32, height: u32) -> Vec<u8> {
    let mut v = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR".to_vec();
    v.extend_from_slice(&width.to_be_bytes());
    v.extend_from_slice(&height.to_be_bytes());
    v.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    v.extend_from_slice(&random(99, 2048 - v.len()));
    v
}

/// A JPEG with an APP0 and a SOF0 segment giving `width` x `height`.
fn jpeg(width: u16, height: u16) -> Vec<u8> {
    let mut v = vec![0xFF, 0xD8];
    v.extend_from_slice(&[0xFF, 0xE0, 0x00, 0x10]);
    v.extend_from_slice(b"JFIF\0\x01\x01\0\0\x01\0\x01\0\0");
    v.extend_from_slice(&[0xFF, 0xC0, 0x00, 0x11, 0x08]);
    v.extend_from_slice(&height.to_be_bytes());
    v.extend_from_slice(&width.to_be_bytes());
    v.extend_from_slice(&[0x03, 0x01, 0x22, 0x00, 0x02, 0x11, 0x01, 0x03, 0x11, 0x01]);
    v.extend_from_slice(&random(98, 3000));
    v.extend_from_slice(&[0xFF, 0xD9]);
    v
}

fn setup() -> (TempDir, Repo, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let src = tmp.path().join("src");
    fs::create_dir(&src).unwrap();
    (tmp, repo, src)
}

fn ingest(repo: &Repo, src: &Path) -> Id {
    let opts = IngestOptions {
        workers: 4,
        ..IngestOptions::default()
    };
    ingest::run(repo, src, &opts).unwrap().snapshot
}

fn catalog(repo: &Repo) -> Catalog {
    Catalog::open_readonly(&repo.catalog_path()).unwrap()
}

/// Length and BLAKE3 of every file under `dir`, recursively.
fn digests(dir: &Path) -> BTreeMap<PathBuf, (u64, Hash32)> {
    let mut out = BTreeMap::new();
    let Ok(rd) = fs::read_dir(dir) else {
        return out;
    };
    for e in rd {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(digests(&p));
        } else {
            let bytes = fs::read(&p).unwrap();
            out.insert(p, (bytes.len() as u64, Hash32::of(&bytes)));
        }
    }
    out
}

/// Bytes of the repository files `build` must never change.
fn untouchable(repo: &Repo) -> BTreeMap<PathBuf, (u64, Hash32)> {
    let mut out = digests(&repo.packs_dir());
    for name in ["catalog.sqlite", "wmk.toml"] {
        let p = repo.root.join(name);
        if let Ok(bytes) = fs::read(&p) {
            out.insert(p, (bytes.len() as u64, Hash32::of(&bytes)));
        }
    }
    out
}

/// Build into `<tmp>/<name>` with `workers` threads, checking that no pack,
/// `catalog.sqlite` or `wmk.toml` changed and that no `-wal` is left.
fn build_with(repo: &Repo, name: &str, workers: usize) -> (PathBuf, RecoverReport) {
    let before = untouchable(repo);
    let dest = repo.root.parent().unwrap().join(name);
    let report = recover::build(repo, &dest, &RecoverOptions { workers }).unwrap();
    assert_eq!(untouchable(repo), before, "build changed repository files");
    let mut wal = dest.as_os_str().to_os_string();
    wal.push("-wal");
    assert!(
        !Path::new(&wal).exists(),
        "-wal left beside the new catalog"
    );
    (dest, report)
}

fn build(repo: &Repo) -> (PathBuf, RecoverReport) {
    build_with(repo, "x.sqlite", 0)
}

/// Every row `sql` returns, rendered so rows can be compared as a set.
fn table_rows(path: &Path, sql: &str) -> BTreeSet<Vec<String>> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    let rows = stmt
        .query_map([], |r| {
            (0..n)
                .map(|i| {
                    r.get::<_, rusqlite::types::Value>(i)
                        .map(|v| format!("{v:?}"))
                })
                .collect::<rusqlite::Result<Vec<String>>>()
        })
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn status(path: &Path, snapshot: Id) -> String {
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT status FROM snapshot WHERE id = ?1",
            [snapshot.as_bytes().to_vec()],
            |r| r.get(0),
        )
        .unwrap()
}

/// XOR one byte of the pack file holding `pack` at `offset`.
fn flip(repo: &Repo, pack: Id, offset: u64) {
    let path = pack_path(&repo.packs_dir(), pack);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    let mut bytes = fs::read(&path).unwrap();
    bytes[offset as usize] ^= 0x01;
    fs::write(&path, &bytes).unwrap();
}

/// XOR a payload byte of the unit `hash`.
fn flip_unit(repo: &Repo, hash: &Hash32) {
    let u = catalog(repo).unit(hash).unwrap();
    flip(
        repo,
        u.pack_id,
        (u.pack_offset as usize + RECORD_HEADER_LEN + 3) as u64,
    );
}

fn first_unit(repo: &Repo, snapshot: Id, path: &[u8]) -> Hash32 {
    let cat = catalog(repo);
    let content = cat
        .entry_by_path(snapshot, path)
        .unwrap()
        .content_hash
        .unwrap();
    cat.content_units(&content).unwrap()[0].hash
}

#[test]
fn rebuilt_catalog_matches_original() {
    let (_tmp, repo, src) = setup();
    let shared = random(1, 300_000);
    let mut t = TreeBuilder::new(&src);
    t.file(b"shared.bin", &shared)
        .file(b"pic.png", &png(3, 7))
        .file(b"photo.jpg", &jpeg(64, 48))
        .file(b"empty", b"")
        .file(b"odd\x01 name\t", b"odd bytes")
        .file("cafe\u{301}".as_bytes(), b"decomposed")
        .file(b"sub/deep/f.txt", b"text body\n")
        .hardlink(b"sub/linked", b"shared.bin")
        .symlink(b"link", b"sub/deep/f.txt")
        .dir(b"empty_dir");
    if cfg!(target_os = "linux") {
        // APFS refuses names that are not UTF-8.
        t.file(b"\xff\xfe.bin", b"not utf-8");
    }
    if let Err(e) = xattr::set(src.join("pic.png"), XATTR, b"v") {
        eprintln!("xattrs unsupported here: {e}");
    }
    let s1 = ingest(&repo, &src);
    t.file(b"more.bin", &random(2, 200_000));
    let s2 = ingest(&repo, &src);
    fs::remove_file(src.join("photo.jpg")).unwrap();
    t.file(b"shared copy", &shared);
    let s3 = ingest(&repo, &src);

    let (dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert!(report.warnings.is_empty(), "{:?}", report.warnings);
    let mut ids = vec![s1, s2, s3];
    ids.sort();
    assert_eq!(report.snapshots, ids);
    assert!(report.packs >= 1 && report.units >= 1 && report.bytes_read > 0);

    let orig = repo.catalog_path();
    for table in [
        "snapshot",
        "entry",
        "content",
        "content_unit",
        "unit",
        "pack",
        "xattr",
    ] {
        let sql = format!("SELECT * FROM {table}");
        let want = table_rows(&orig, &sql);
        assert!(!want.is_empty() || table == "xattr", "{table} is empty");
        assert_eq!(table_rows(&dest, &sql), want, "table {table}");
    }
    let media = "SELECT content_hash, width, height FROM media";
    assert_eq!(table_rows(&orig, media).len(), 2, "png and jpeg measured");
    assert_eq!(table_rows(&dest, media), table_rows(&orig, media));
    assert_eq!(
        report.units as usize,
        table_rows(&orig, "SELECT hash FROM unit").len()
    );

    // The worker count changes nothing.
    let (one, _) = build_with(&repo, "one.sqlite", 1);
    for table in [
        "snapshot",
        "entry",
        "content",
        "content_unit",
        "unit",
        "pack",
        "media",
    ] {
        let sql = format!("SELECT * FROM {table}");
        assert_eq!(
            table_rows(&one, &sql),
            table_rows(&dest, &sql),
            "table {table}"
        );
    }
}

#[test]
fn rebuilt_catalog_is_version_2_and_opens() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s = ingest(&repo, &src);
    let (dest, _) = build(&repo);
    let v: i64 = rusqlite::Connection::open(&dest)
        .unwrap()
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 2);
    let cat = Catalog::open_readonly(&dest).unwrap();
    assert_eq!(
        cat.snapshot(s).unwrap().record_unit,
        catalog(&repo).snapshot(s).unwrap().record_unit
    );
}

#[test]
fn existing_dest_is_refused() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    ingest(&repo, &src);
    let dest = tmp.path().join("x.sqlite");
    fs::write(&dest, b"keep me").unwrap();
    assert!(recover::build(&repo, &dest, &RecoverOptions::default()).is_err());
    assert_eq!(fs::read(&dest).unwrap(), b"keep me");
    let wal_only = tmp.path().join("y.sqlite");
    fs::write(tmp.path().join("y.sqlite-wal"), b"stale").unwrap();
    assert!(recover::build(&repo, &wal_only, &RecoverOptions::default()).is_err());
    assert!(!wal_only.exists());
}

#[test]
fn empty_file_is_not_damage() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"e1", b"").file(b"d/e2", b"");
    let s = ingest(&repo, &src);
    let (dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert_eq!(report.snapshots, vec![s]);
    assert_eq!(status(&dest, s), "complete");
}

#[test]
fn flipped_data_byte_damages_one_file() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"a", &random(3, 100_000)).file(b"b", b"bee");
    let s1 = ingest(&repo, &src);
    fs::remove_file(src.join("a")).unwrap();
    t.file(b"c", b"sea");
    let s2 = ingest(&repo, &src);
    flip_unit(&repo, &first_unit(&repo, s1, b"a"));

    let (dest, report) = build(&repo);
    assert!(!report.intact());
    assert!(report.lost.is_empty(), "{:?}", report.lost);
    assert_eq!(report.damaged.len(), 1, "{:?}", report.damaged);
    assert_eq!(report.damaged[0].id, s1);
    assert_eq!(report.damaged[0].files, vec![b"a".to_vec()]);
    assert!(!report.warnings.is_empty());
    let mut ids = vec![s1, s2];
    ids.sort();
    assert_eq!(report.snapshots, ids);
    assert_eq!(status(&dest, s1), "damaged");
    assert_eq!(status(&dest, s2), "complete");
    // The damaged content keeps its row; only the missing unit is absent.
    let cat = Catalog::open_readonly(&dest).unwrap();
    let a = cat.entry_by_path(s1, b"a").unwrap().content_hash.unwrap();
    assert!(cat.has_content(&a).unwrap());
    assert!(!cat.content_complete(&a).unwrap());
}

#[test]
fn flipped_record_byte_loses_that_snapshot_only() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"one", b"1");
    let s1 = ingest(&repo, &src);
    t.file(b"two", b"2");
    let s2 = ingest(&repo, &src);
    t.file(b"three", b"3");
    let s3 = ingest(&repo, &src);
    let cat = catalog(&repo);
    let record = cat.snapshot(s2).unwrap().record_unit.unwrap();
    let chunk = record_chunks(&UnitReader::new(&repo, 0), &cat, &record).unwrap()[0];
    drop(cat);
    flip_unit(&repo, &chunk);

    let (dest, report) = build(&repo);
    assert_eq!(report.lost.len(), 1, "{:?}", report.lost);
    assert_eq!(report.lost[0].snapshot, Some(s2));
    assert_eq!(report.lost[0].unit, record);
    assert!(report.damaged.is_empty(), "{:?}", report.damaged);
    let mut ids = vec![s1, s3];
    ids.sort();
    assert_eq!(report.snapshots, ids);
    assert_eq!(status(&dest, s1), "complete");
    assert_eq!(status(&dest, s3), "complete");
    let cat = Catalog::open_readonly(&dest).unwrap();
    assert!(cat.snapshot(s2).is_err());
    let others = format!(
        "SELECT * FROM entry WHERE snapshot_id <> x'{}'",
        s2.to_string().replace('-', "")
    );
    assert_eq!(
        table_rows(&dest, "SELECT * FROM entry"),
        table_rows(&repo.catalog_path(), &others),
        "no row of the lost snapshot remains"
    );
}

#[test]
fn corrupt_duplicate_copy_is_a_warning() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"x", &random(4, 50_000));
    let s = ingest(&repo, &src);
    let unit = catalog(&repo).unit(&first_unit(&repo, s, b"x")).unwrap();
    let reader = PackReader::open(&pack_path(&repo.packs_dir(), unit.pack_id)).unwrap();
    let (header, payload) = reader.read_record(unit.pack_offset as u64).unwrap();
    drop(reader);
    let copy = Id::new_v7();
    let mut w = PackWriter::create(&repo.packs_dir(), copy).unwrap();
    w.append(&header, &payload).unwrap();
    w.seal().unwrap();
    flip_unit(&repo, &unit.hash);

    let (dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert!(!report.warnings.is_empty());
    assert_eq!(status(&dest, s), "complete");
    let rebuilt = Catalog::open_readonly(&dest)
        .unwrap()
        .unit(&unit.hash)
        .unwrap();
    assert_eq!(rebuilt.pack_id, copy);
}

#[test]
fn failed_status_comes_back_complete_when_record_exists() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s = ingest(&repo, &src);
    rusqlite::Connection::open(repo.catalog_path())
        .unwrap()
        .execute(
            "UPDATE snapshot SET status = 'failed' WHERE id = ?1",
            [s.as_bytes().to_vec()],
        )
        .unwrap();
    let (dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert_eq!(status(&dest, s), "complete");
}

#[test]
fn torn_tail_is_ignored_and_pack_untouched() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s = ingest(&repo, &src);
    // An unsealed pack as a crash leaves it: one synced record, then a
    // partly written one.
    let raw = b"orphan unit bytes".to_vec();
    let header = RecordHeader {
        kind: UnitKind::RawChunk,
        compression: Compression::None,
        unit_hash: Hash32::of(&raw),
        raw_len: raw.len() as u64,
        stored_len: raw.len() as u64,
    };
    let id = Id::new_v7();
    let mut w = PackWriter::create(&repo.packs_dir(), id).unwrap();
    let offset = w.append(&header, &raw).unwrap();
    let end = w.len();
    w.sync().unwrap();
    drop(w);
    let path = pack_path(&repo.packs_dir(), id);
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend_from_slice(&[0x52, 0x4B, 0x4D, 0x57, 1, 0, 9, 9, 9]);
    fs::write(&path, &bytes).unwrap();

    let (dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert!(report.warnings.is_empty(), "{:?}", report.warnings);
    assert_eq!(report.snapshots, vec![s]);
    let cat = Catalog::open_readonly(&dest).unwrap();
    let p = cat.pack(id).unwrap();
    assert!(!p.sealed);
    assert_eq!(p.size as u64, end);
    assert_eq!(p.blake3, None);
    let u = cat.unit(&header.unit_hash).unwrap();
    assert_eq!((u.pack_id, u.pack_offset as u64), (id, offset));
}

#[test]
fn stray_files_under_packs_are_warnings() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s = ingest(&repo, &src);
    fs::write(repo.packs_dir().join("notes.txt"), b"hello").unwrap();
    let (_dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert_eq!(report.snapshots, vec![s]);
    assert_eq!(report.warnings.len(), 1, "{:?}", report.warnings);
    assert!(
        report.warnings[0].contains("notes.txt"),
        "{:?}",
        report.warnings
    );
}

#[test]
fn corrupt_record_unit_is_reported_lost() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s1 = ingest(&repo, &src);
    TreeBuilder::new(&src).file(b"g", b"more");
    let s2 = ingest(&repo, &src);
    let record = catalog(&repo).snapshot(s1).unwrap().record_unit.unwrap();
    flip_unit(&repo, &record);

    let (_dest, report) = build(&repo);
    assert!(!report.intact());
    assert_eq!(report.lost.len(), 1, "{:?}", report.lost);
    assert_eq!(report.lost[0].unit, record);
    assert_eq!(report.lost[0].snapshot, None);
    assert_eq!(report.snapshots, vec![s2]);
}

/// Store, in a new sealed pack, a second index unit for `snapshot`'s record:
/// the same chunks, but a `document_size` one byte too large, so replaying
/// it inserts every row and then fails the final size check. Returns the
/// original index unit and the variant's hash.
fn store_size_variant(repo: &Repo, snapshot: Id) -> (Hash32, Hash32) {
    let cat = catalog(repo);
    let record = cat.snapshot(snapshot).unwrap().record_unit.unwrap();
    let row = cat.unit(&record).unwrap();
    drop(cat);
    let raw = PackReader::open(&pack_path(&repo.packs_dir(), row.pack_id))
        .unwrap()
        .read_unit(row.pack_offset as u64, &record)
        .unwrap();
    let mut index: ciborium::Value = ciborium::from_reader(&raw[..]).unwrap();
    let ciborium::Value::Map(fields) = &mut index else {
        panic!("index is not a map");
    };
    let size = fields
        .iter_mut()
        .find(|(k, _)| k.as_text() == Some("document_size"))
        .unwrap();
    let n: u64 = size.1.as_integer().unwrap().try_into().unwrap();
    size.1 = ciborium::Value::Integer((n + 1).into());
    let mut other = Vec::new();
    ciborium::into_writer(&index, &mut other).unwrap();
    let header = RecordHeader {
        kind: UnitKind::SnapshotRecord,
        compression: Compression::None,
        unit_hash: Hash32::of(&other),
        raw_len: other.len() as u64,
        stored_len: other.len() as u64,
    };
    let mut w = PackWriter::create(&repo.packs_dir(), Id::new_v7()).unwrap();
    w.append(&header, &other).unwrap();
    w.seal().unwrap();
    (record, header.unit_hash)
}

#[test]
fn two_records_for_one_snapshot_are_both_lost() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s1 = ingest(&repo, &src);
    TreeBuilder::new(&src).file(b"g", b"more");
    let s2 = ingest(&repo, &src);
    let (record, variant) = store_size_variant(&repo, s1);

    let (dest, report) = build(&repo);
    let mut lost: Vec<_> = report.lost.iter().map(|l| (l.unit, l.snapshot)).collect();
    lost.sort();
    let mut want = vec![(record, Some(s1)), (variant, Some(s1))];
    want.sort();
    assert_eq!(lost, want, "{:?}", report.lost);
    assert!(report
        .lost
        .iter()
        .all(|l| l.why.contains("different records")));
    assert_eq!(report.snapshots, vec![s2]);
    assert!(Catalog::open_readonly(&dest).unwrap().snapshot(s1).is_err());
}

/// `x'<hex>'` for an id or hash, for SQL in `table_rows`.
fn sql_blob(bytes: &[u8]) -> String {
    format!("x'{}'", hex::encode(bytes))
}

#[test]
fn record_failing_after_its_rows_leaves_none_behind() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"unique.bin", &random(5, 120_000)).dir(b"d");
    let has_xattr = match xattr::set(src.join("unique.bin"), XATTR, b"v") {
        Ok(()) => true,
        Err(e) => {
            eprintln!("xattrs unsupported here: {e}");
            false
        }
    };
    let s1 = ingest(&repo, &src);
    fs::remove_file(src.join("unique.bin")).unwrap();
    t.file(b"other", b"s2 only");
    let s2 = ingest(&repo, &src);
    let orig = repo.catalog_path();
    let content = catalog(&repo)
        .entry_by_path(s1, b"unique.bin")
        .unwrap()
        .content_hash
        .unwrap();
    let s1_entries = format!(
        "SELECT id FROM entry WHERE snapshot_id = {}",
        sql_blob(s1.as_bytes())
    );
    let s1_xattrs = format!("SELECT * FROM xattr WHERE entry_id IN ({s1_entries})");
    if has_xattr {
        assert!(!table_rows(&orig, &s1_xattrs).is_empty());
    }
    // Only the variant is replayed: it inserts all of s1's rows, then fails.
    let (record, variant) = store_size_variant(&repo, s1);
    flip_unit(&repo, &record);

    let (dest, report) = build(&repo);
    let mut lost: Vec<_> = report.lost.iter().map(|l| (l.unit, l.snapshot)).collect();
    lost.sort();
    // The corrupt original is reported too, by unit (its index is unreadable).
    let mut want = vec![(record, None), (variant, Some(s1))];
    want.sort();
    assert_eq!(lost, want, "{:?}", report.lost);
    let why = &report.lost.iter().find(|l| l.unit == variant).unwrap().why;
    assert!(why.contains("index says"), "{why}");
    assert_eq!(report.snapshots, vec![s2]);

    let c = sql_blob(&content.0);
    let mut s1_rows = vec![
        format!(
            "SELECT * FROM snapshot WHERE id = {}",
            sql_blob(s1.as_bytes())
        ),
        s1_entries,
        format!("SELECT * FROM content WHERE hash = {c}"),
        format!("SELECT * FROM content_unit WHERE content_hash = {c}"),
    ];
    if has_xattr {
        s1_rows.push(s1_xattrs);
    }
    for sql in s1_rows {
        assert!(!table_rows(&orig, &sql).is_empty(), "no such rows: {sql}");
        assert!(table_rows(&dest, &sql).is_empty(), "rows remain: {sql}");
    }
}

#[test]
fn records_embedded_in_a_payload_are_not_units() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s1 = ingest(&repo, &src);
    let units_before = catalog(&repo).stats().unwrap().units;

    // Another repository's sealed pack, holding its snapshot record.
    let other = Repo::init(&tmp.path().join("other")).unwrap();
    let other_src = tmp.path().join("other_src");
    TreeBuilder::new(&other_src).file(b"g", b"foreign file");
    let foreign = ingest(&other, &other_src);
    let foreign_record = catalog(&other)
        .snapshot(foreign)
        .unwrap()
        .record_unit
        .unwrap();
    let foreign_pack = catalog(&other).unit(&foreign_record).unwrap().pack_id;
    let inner = fs::read(pack_path(&other.packs_dir(), foreign_pack)).unwrap();

    // A record whose header is then damaged (breaking the sequential
    // parse), followed by an intact record whose payload is that pack.
    let unit = |raw: &[u8]| RecordHeader {
        kind: UnitKind::RawChunk,
        compression: Compression::None,
        unit_hash: Hash32::of(raw),
        raw_len: raw.len() as u64,
        stored_len: raw.len() as u64,
    };
    let victim = b"victim".to_vec();
    let id = Id::new_v7();
    let mut w = PackWriter::create(&repo.packs_dir(), id).unwrap();
    let victim_at = w.append(&unit(&victim), &victim).unwrap();
    let outer_at = w.append(&unit(&inner), &inner).unwrap();
    w.seal().unwrap();
    flip(&repo, id, victim_at + 10);

    let (dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert_eq!(report.snapshots, vec![s1]);
    assert_eq!(
        report.units,
        units_before + 1,
        "only the outer record is new"
    );
    let cat = Catalog::open_readonly(&dest).unwrap();
    assert!(!cat.has_unit(&foreign_record).unwrap());
    assert_eq!(
        cat.unit(&Hash32::of(&inner)).unwrap().pack_offset as u64,
        outer_at
    );
    assert!(
        !report.warnings.iter().any(|w| w.contains("no intact copy")),
        "{:?}",
        report.warnings
    );
}

#[test]
fn misplaced_pack_is_an_error() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s = ingest(&repo, &src);
    let cat = catalog(&repo);
    let record = cat.snapshot(s).unwrap().record_unit.unwrap();
    let home = pack_path(&repo.packs_dir(), cat.unit(&record).unwrap().pack_id);
    drop(cat);
    let name = home.file_name().unwrap().to_owned();
    let dest = tmp.path().join("x.sqlite");
    for wrong in [
        repo.packs_dir().join(&name),
        repo.packs_dir().join("zz").join(&name),
    ] {
        fs::create_dir_all(wrong.parent().unwrap()).unwrap();
        fs::rename(&home, &wrong).unwrap();
        let err = recover::build(&repo, &dest, &RecoverOptions::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains(&wrong.display().to_string()), "{err}");
        assert!(
            err.contains(&format!("move it to {}", home.display())),
            "{err}"
        );
        assert!(!dest.exists(), "nothing is built");
        fs::rename(&wrong, &home).unwrap();
    }
    let (_dest, report) = build(&repo);
    assert_eq!(report.snapshots, vec![s]);
}

#[test]
fn bad_footer_hash_is_a_warning() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", &random(6, 40_000));
    let s = ingest(&repo, &src);
    let cat = catalog(&repo);
    let record = cat.snapshot(s).unwrap().record_unit.unwrap();
    let id = cat.unit(&record).unwrap().pack_id;
    let units = cat.stats().unwrap().units;
    drop(cat);
    // End of the pack's last record: where its footer starts.
    let data_end: i64 = rusqlite::Connection::open(repo.catalog_path())
        .unwrap()
        .query_row(
            &format!(
                "SELECT max(pack_offset + {RECORD_HEADER_LEN} + stored_size) FROM unit \
                 WHERE pack_id = ?1"
            ),
            [id.as_bytes().to_vec()],
            |r| r.get(0),
        )
        .unwrap();
    let len = fs::metadata(pack_path(&repo.packs_dir(), id))
        .unwrap()
        .len();
    // The stored BLAKE3 sits just before footer_offset (8) and the magic (8).
    flip(&repo, id, len - 16 - 5);

    let (dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert_eq!(report.snapshots, vec![s]);
    assert_eq!(report.units, units);
    assert!(
        report.warnings.iter().any(|w| w.contains("footer BLAKE3")),
        "{:?}",
        report.warnings
    );
    let p = Catalog::open_readonly(&dest).unwrap().pack(id).unwrap();
    assert!(!p.sealed);
    assert_eq!(p.blake3, None);
    assert_eq!(p.size, data_end);
    assert_eq!(status(&dest, s), "complete");
}
