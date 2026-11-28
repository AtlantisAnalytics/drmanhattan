//! `recover::build`: a complete, checked catalog rebuilt from the packs alone.

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::compress::Compression;
use drmanhattan_core::config::Config;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::ingest::record::record_chunks;
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::manifest::{manifest_dir, ManifestOptions};
use drmanhattan_core::pack::RECORD_HEADER_LEN;
use drmanhattan_core::pack::{pack_path, PackReader, PackWriter, RecordHeader, UnitKind};
use drmanhattan_core::recover::{self, RecoverOptions, RecoverReport};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::restore::{self, RestoreOptions};
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
fn image_with_a_damaged_head_gets_no_media_row() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"bad.png", &png(640, 480))
        .file(b"good.jpg", &jpeg(320, 200));
    for i in 0..20u32 {
        t.file(format!("p{i}.png").as_bytes(), &png(100 + i, 50 + i));
    }
    let s = ingest(&repo, &src);
    flip_unit(&repo, &first_unit(&repo, s, b"bad.png"));
    // (width, height) of the image at `path`, if it has a media row.
    let dims = |dest: &Path, path: &[u8]| -> Option<(i64, i64)> {
        let hash = Catalog::open_readonly(dest)
            .unwrap()
            .entry_by_path(s, path)
            .unwrap()
            .content_hash
            .unwrap();
        let conn = rusqlite::Connection::open(dest).unwrap();
        conn.query_row(
            "SELECT width, height FROM media WHERE content_hash = ?1",
            [hash],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok()
    };

    for workers in [1, 8] {
        let (dest, report) = build_with(&repo, &format!("w{workers}.sqlite"), workers);
        assert_eq!(report.damaged.len(), 1, "{:?}", report.damaged);
        let rows = table_rows(&dest, "SELECT content_hash FROM media");
        assert_eq!(rows.len(), 21, "every intact image measured");
        assert_eq!(dims(&dest, b"bad.png"), None, "damaged image measured");
        assert_eq!(dims(&dest, b"good.jpg"), Some((320, 200)));
        assert_eq!(dims(&dest, b"p7.png"), Some((107, 57)));
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
    assert!(report.unaccounted.is_empty(), "{:?}", report.unaccounted);
    assert_eq!(report.torn_tails.len(), 1, "{:?}", report.torn_tails);
    let tail = &report.torn_tails[0];
    assert_eq!(
        (tail.pack_id, tail.start, tail.end),
        (id, end, bytes.len() as u64)
    );
    assert_eq!(report.snapshots, vec![s]);
    let cat = Catalog::open_readonly(&dest).unwrap();
    let p = cat.pack(id).unwrap();
    assert!(!p.sealed);
    assert_eq!(p.size as u64, end);
    assert_eq!(p.blake3, None);
    let u = cat.unit(&header.unit_hash).unwrap();
    assert_eq!((u.pack_id, u.pack_offset as u64), (id, offset));
}

/// Break the record header of the unit `hash` (its CRC no longer matches).
fn break_header(repo: &Repo, hash: &Hash32) -> (Id, u64) {
    let u = catalog(repo).unit(hash).unwrap();
    flip(repo, u.pack_id, u.pack_offset as u64 + 54);
    (u.pack_id, u.pack_offset as u64)
}

#[test]
fn broken_header_of_only_record_copy_is_unaccounted() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"one", b"1");
    let s1 = ingest(&repo, &src);
    t.file(b"two", b"2");
    let s2 = ingest(&repo, &src);
    let record = catalog(&repo).snapshot(s2).unwrap().record_unit.unwrap();
    let span = RECORD_HEADER_LEN as u64 + catalog(&repo).unit(&record).unwrap().stored_size as u64;
    let (pack, offset) = break_header(&repo, &record);

    let (_dest, report) = build(&repo);
    assert!(!report.intact(), "{report:#?}");
    assert_eq!(report.snapshots, vec![s1]);
    assert_eq!(report.unaccounted.len(), 1, "{:?}", report.unaccounted);
    let r = &report.unaccounted[0];
    assert_eq!((r.pack_id, r.start, r.end), (pack, offset, offset + span));
    assert_eq!(r.pack, pack_path(&repo.packs_dir(), pack));
}

#[test]
fn broken_header_with_an_intact_copy_is_a_warning() {
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
    break_header(&repo, &unit.hash);

    let (dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert!(report.unaccounted.is_empty(), "{:?}", report.unaccounted);
    assert!(
        report.warnings.iter().any(|w| w.contains("intact copy")),
        "{:?}",
        report.warnings
    );
    assert_eq!(status(&dest, s), "complete");
}

#[test]
fn committed_bytes_lost_from_an_unsealed_pack_are_unaccounted() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s = ingest(&repo, &src);
    // An unsealed pack holding two committed units, as the old catalog says.
    let unit = |raw: &[u8]| RecordHeader {
        kind: UnitKind::RawChunk,
        compression: Compression::None,
        unit_hash: Hash32::of(raw),
        raw_len: raw.len() as u64,
        stored_len: raw.len() as u64,
    };
    let (a, b) = (b"first unit".to_vec(), b"second unit".to_vec());
    let id = Id::new_v7();
    let mut w = PackWriter::create(&repo.packs_dir(), id).unwrap();
    w.append(&unit(&a), &a).unwrap();
    let second = w.append(&unit(&b), &b).unwrap();
    let end = w.len();
    w.sync().unwrap();
    drop(w);
    rusqlite::Connection::open(repo.catalog_path())
        .unwrap()
        .execute(
            "INSERT INTO pack(id, size, sealed, blake3) VALUES (?1, ?2, 0, NULL)",
            rusqlite::params![id.as_bytes().to_vec(), end as i64],
        )
        .unwrap();
    // The final record's header breaks: without the old catalog this looks
    // like a torn tail, but those bytes were committed.
    flip(&repo, id, second + 54);

    let report = recover::run(&repo, &RecoverOptions::default()).unwrap();
    assert!(!report.intact(), "{report:#?}");
    assert_eq!(report.snapshots, vec![s]);
    assert_eq!(report.unaccounted.len(), 1, "{:?}", report.unaccounted);
    let r = &report.unaccounted[0];
    assert_eq!((r.pack_id, r.start, r.end), (id, second, end));
    assert!(report.torn_tails.is_empty(), "{:?}", report.torn_tails);
}

/// Leave `snapshot`'s record as an ingest killed right after the record was
/// durable leaves it: the last record of an unsealed pack (footer cut off)
/// whose catalog row says those bytes were committed. Returns the pack id,
/// the record's offset and the pack's data end.
fn unseal_record_pack(repo: &Repo, snapshot: Id) -> (Id, u64, u64) {
    let cat = catalog(repo);
    let record = cat.snapshot(snapshot).unwrap().record_unit.unwrap();
    let unit = cat.unit(&record).unwrap();
    drop(cat);
    let path = pack_path(&repo.packs_dir(), unit.pack_id);
    let bytes = fs::read(&path).unwrap();
    let footer = u64::from_le_bytes(bytes[bytes.len() - 16..bytes.len() - 8].try_into().unwrap());
    let offset = unit.pack_offset as u64;
    assert_eq!(
        offset + RECORD_HEADER_LEN as u64 + unit.stored_size as u64,
        footer,
        "the record is the pack's last record"
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(&path, &bytes[..footer as usize]).unwrap();
    rusqlite::Connection::open(repo.catalog_path())
        .unwrap()
        .execute(
            "UPDATE pack SET sealed = 0, blake3 = NULL, size = ?2 WHERE id = ?1",
            rusqlite::params![unit.pack_id.as_bytes().to_vec(), footer as i64],
        )
        .unwrap();
    (unit.pack_id, offset, footer)
}

#[test]
fn second_recover_still_reports_lost_committed_bytes() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"one", b"1");
    let s1 = ingest(&repo, &src);
    t.file(b"two", b"2");
    let s2 = ingest(&repo, &src);
    let (pack, offset, end) = unseal_record_pack(&repo, s2);
    flip(&repo, pack, offset + 54);

    for run in 1..=2 {
        let report = recover::run(&repo, &RecoverOptions::default()).unwrap();
        assert!(!report.intact(), "run {run}: {report:#?}");
        assert_eq!(report.snapshots, vec![s1], "run {run}");
        assert_eq!(report.unaccounted.len(), 1, "run {run}: {report:#?}");
        let r = &report.unaccounted[0];
        assert_eq!(
            (r.pack_id, r.start, r.end),
            (pack, offset, end),
            "run {run}"
        );
        assert!(report.torn_tails.is_empty(), "run {run}: {report:#?}");
    }
}

#[test]
fn torn_tail_stays_harmless_across_recovers() {
    let (_tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s = ingest(&repo, &src);
    let raw = b"committed unit".to_vec();
    let header = RecordHeader {
        kind: UnitKind::RawChunk,
        compression: Compression::None,
        unit_hash: Hash32::of(&raw),
        raw_len: raw.len() as u64,
        stored_len: raw.len() as u64,
    };
    let id = Id::new_v7();
    let mut w = PackWriter::create(&repo.packs_dir(), id).unwrap();
    w.append(&header, &raw).unwrap();
    let end = w.len();
    w.sync().unwrap();
    drop(w);
    rusqlite::Connection::open(repo.catalog_path())
        .unwrap()
        .execute(
            "INSERT INTO pack(id, size, sealed, blake3) VALUES (?1, ?2, 0, NULL)",
            rusqlite::params![id.as_bytes().to_vec(), end as i64],
        )
        .unwrap();
    // Bytes past every recorded size: an unfinished write.
    let path = pack_path(&repo.packs_dir(), id);
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend_from_slice(&[0x52, 0x4B, 0x4D, 0x57, 1, 0, 9, 9, 9]);
    fs::write(&path, &bytes).unwrap();

    for run in 1..=2 {
        let report = recover::run(&repo, &RecoverOptions::default()).unwrap();
        assert!(report.intact(), "run {run}: {report:#?}");
        assert_eq!(report.snapshots, vec![s], "run {run}");
        assert_eq!(report.torn_tails.len(), 1, "run {run}: {report:#?}");
        let tail = &report.torn_tails[0];
        assert_eq!(
            (tail.pack_id, tail.start, tail.end),
            (id, end, bytes.len() as u64),
            "run {run}"
        );
    }
    // Reading the kept catalogs left nothing beside them.
    let sidecars: Vec<_> = fs::read_dir(&repo.root)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("catalog.sqlite.pre-recover-") && n.ends_with("-shm"))
        .collect();
    assert!(sidecars.is_empty(), "{sidecars:?}");
}

#[test]
fn symlinked_shard_directory_is_followed() {
    let (tmp, repo, src) = setup();
    TreeBuilder::new(&src).file(b"f", b"body");
    let s = ingest(&repo, &src);
    let cat = catalog(&repo);
    let record = cat.snapshot(s).unwrap().record_unit.unwrap();
    let home = pack_path(&repo.packs_dir(), cat.unit(&record).unwrap().pack_id);
    drop(cat);
    let shard = home.parent().unwrap().to_path_buf();
    let elsewhere = tmp.path().join("shard-elsewhere");
    fs::rename(&shard, &elsewhere).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &shard).unwrap();

    let (_dest, report) = build(&repo);
    assert!(report.intact(), "{report:#?}");
    assert!(report.warnings.is_empty(), "{:?}", report.warnings);
    assert_eq!(report.snapshots, vec![s]);
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
    // The victim had no other copy: its bytes are unaccounted.
    assert!(!report.intact(), "{report:#?}");
    assert_eq!(report.unaccounted.len(), 1, "{:?}", report.unaccounted);
    let r = &report.unaccounted[0];
    assert_eq!((r.pack_id, r.start, r.end), (id, victim_at, outer_at));
    assert!(report.damaged.is_empty() && report.lost.is_empty());
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

// ---- recover::run ----

fn mopts() -> ManifestOptions {
    ManifestOptions {
        mtime_precision_ns: 1,
        cross_fs: false,
    }
}

fn two_snapshots(repo: &Repo, src: &Path) -> (Id, Id) {
    let mut t = TreeBuilder::new(src);
    t.file(b"a.bin", &random(5, 100_000))
        .file(b"sub/f.txt", b"text\n")
        .symlink(b"l", b"sub/f.txt");
    let s1 = ingest(repo, src);
    t.file(b"b.bin", &random(6, 50_000));
    let s2 = ingest(repo, src);
    (s1, s2)
}

/// Restore `snapshot` from `repo` and check it against the stored manifest.
fn assert_restores(repo: &Repo, snapshot: Id, out: &Path) {
    restore::run(
        repo,
        snapshot,
        out,
        &RestoreOptions {
            path: Vec::new(),
            hardlinks: false,
            xattrs: false,
        },
    )
    .unwrap();
    let want = catalog(repo).snapshot(snapshot).unwrap().manifest_hash;
    assert_eq!(Some(manifest_dir(out, &mopts()).unwrap().core), want);
}

fn user_version(path: &Path) -> i64 {
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap()
}

fn backups(repo: &Repo) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(&repo.root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("catalog.sqlite.pre-recover-")
                && !p.to_string_lossy().ends_with("-wal")
                && !p.to_string_lossy().ends_with("-shm")
        })
        .collect();
    v.sort();
    v
}

fn remove_catalog(repo: &Repo) {
    for s in ["", "-wal", "-shm"] {
        let mut p = repo.catalog_path().into_os_string();
        p.push(s);
        let _ = fs::remove_file(p);
    }
}

fn no_tmp_leftovers(repo: &Repo) {
    let left: Vec<PathBuf> = fs::read_dir(repo.tmp_dir())
        .map(|rd| rd.map(|e| e.unwrap().path()).collect())
        .unwrap_or_default();
    assert!(left.is_empty(), "left in tmp/: {left:?}");
}

#[test]
fn recover_after_deleting_catalog_restores_byte_exact() {
    let (tmp, repo, src) = setup();
    let (s1, s2) = two_snapshots(&repo, &src);
    remove_catalog(&repo);

    let report = recover::run(&repo, &RecoverOptions::default()).unwrap();
    assert!(report.intact(), "{report:#?}");
    assert_eq!(report.old_catalog, None);
    assert!(report.not_recovered.is_empty());
    assert_eq!(report.snapshots, vec![s1, s2]);
    assert_eq!(user_version(&repo.catalog_path()), 2);
    assert_eq!(Repo::open(&repo.root).unwrap().config.format_version, 2);
    assert!(backups(&repo).is_empty());
    no_tmp_leftovers(&repo);
    for (i, s) in [s1, s2].into_iter().enumerate() {
        assert_restores(&repo, s, &tmp.path().join(format!("out{i}")));
    }
}

#[test]
fn recover_over_healthy_catalog_keeps_old_one() {
    let (_tmp, repo, src) = setup();
    let (s1, s2) = two_snapshots(&repo, &src);
    let before = fs::read(repo.catalog_path()).unwrap();

    let report = recover::run(&repo, &RecoverOptions::default()).unwrap();
    assert!(report.intact(), "{report:#?}");
    assert!(report.not_recovered.is_empty());
    let old = report.old_catalog.clone().unwrap();
    assert!(old
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("catalog.sqlite.pre-recover-"));
    assert_eq!(fs::read(&old).unwrap(), before);
    let old_cat = Catalog::open_readonly(&old).unwrap();
    assert_eq!(old_cat.snapshots().unwrap().len(), 2);
    assert_eq!(catalog(&repo).snapshots().unwrap().len(), 2);
    assert_eq!(report.snapshots, vec![s1, s2]);
    assert_eq!(backups(&repo), vec![old]);
}

#[test]
fn recover_twice_keeps_both_old_catalogs() {
    let (_tmp, repo, src) = setup();
    two_snapshots(&repo, &src);
    let a = recover::run(&repo, &RecoverOptions::default()).unwrap();
    let b = recover::run(&repo, &RecoverOptions::default()).unwrap();
    let (a, b) = (a.old_catalog.unwrap(), b.old_catalog.unwrap());
    assert_ne!(a, b);
    assert!(a.exists() && b.exists());
    assert_eq!(backups(&repo).len(), 2);
}

#[test]
fn recover_empty_repository() {
    let (_tmp, repo, _src) = setup();
    let report = recover::run(&repo, &RecoverOptions::default()).unwrap();
    assert!(report.intact(), "{report:#?}");
    assert!(report.snapshots.is_empty());
    assert!(catalog(&repo).snapshots().unwrap().is_empty());
    assert!(report.old_catalog.is_some());
}

#[test]
fn unreadable_pack_is_an_error_and_changes_nothing() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("running as root: permissions are not enforced");
        return;
    }
    let (_tmp, repo, src) = setup();
    let (s1, _s2) = two_snapshots(&repo, &src);
    let unit = first_unit(&repo, s1, b"a.bin");
    let pack = catalog(&repo).unit(&unit).unwrap().pack_id;
    let before = untouchable(&repo);
    let path = pack_path(&repo.packs_dir(), pack);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
    let result = recover::run(&repo, &RecoverOptions::default());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(result, Err(Error::Io { .. })), "{result:?}");
    assert_eq!(untouchable(&repo), before);
    assert!(backups(&repo).is_empty());
    no_tmp_leftovers(&repo);
}

#[test]
fn unfinished_snapshot_is_listed_as_not_recovered() {
    let (_tmp, repo, src) = setup();
    two_snapshots(&repo, &src);
    let id = Id::new_v7();
    rusqlite::Connection::open(repo.catalog_path())
        .unwrap()
        .execute(
            "INSERT INTO snapshot(id, status, source_root, host, tool_version, started_at) \
             VALUES (?1, 'in_progress', x'2f', 'h', 't', 1)",
            [id.as_bytes().to_vec()],
        )
        .unwrap();
    let report = recover::run(&repo, &RecoverOptions::default()).unwrap();
    assert_eq!(report.not_recovered, vec![id]);
    assert_eq!(report.snapshots.len(), 2);
}

#[test]
fn recover_refuses_while_locked() {
    let (_tmp, repo, src) = setup();
    two_snapshots(&repo, &src);
    let before = untouchable(&repo);
    let _lock = repo.lock_writer().unwrap();
    let r = recover::run(&repo, &RecoverOptions::default());
    assert!(matches!(r, Err(Error::Locked)), "{r:?}");
    assert_eq!(untouchable(&repo), before);
}

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dest = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_tree(&e.path(), &dest);
        } else {
            fs::copy(e.path(), dest).unwrap();
        }
    }
}

#[test]
fn moved_repository_recovers() {
    let (tmp, repo, src) = setup();
    let (s1, s2) = two_snapshots(&repo, &src);
    let moved = tmp.path().join("moved");
    copy_tree(&repo.root, &moved);
    let copy = Repo::open(&moved).unwrap();
    remove_catalog(&copy);
    let report = recover::run(&copy, &RecoverOptions::default()).unwrap();
    assert!(report.intact(), "{report:#?}");
    assert_eq!(report.snapshots, vec![s1, s2]);
    assert_restores(&copy, s2, &tmp.path().join("out"));
}

#[test]
fn damage_then_reingest_then_recover_clears_damage() {
    let (_tmp, repo, src) = setup();
    let mut t = TreeBuilder::new(&src);
    t.file(b"big.bin", &random(7, 100_000)).file(b"ok", b"fine");
    let s = ingest(&repo, &src);
    let unit = first_unit(&repo, s, b"big.bin");
    flip_unit(&repo, &unit);

    let first = recover::run(&repo, &RecoverOptions::default()).unwrap();
    assert!(!first.intact(), "{first:#?}");
    assert_eq!(status(&repo.catalog_path(), s), "damaged");

    // Re-ingesting the intact source stores the lost units again.
    let again = ingest(&repo, &src);
    let second = recover::run(&repo, &RecoverOptions::default()).unwrap();
    assert!(second.intact(), "{second:#?}");
    assert_eq!(second.snapshots, vec![s, again]);
    assert_eq!(status(&repo.catalog_path(), s), "complete");
    assert_eq!(status(&repo.catalog_path(), again), "complete");
}

/// A non-empty WAL for a catalog: copied while a connection holds
/// uncheckpointed writes. Returns its bytes.
fn live_wal_bytes(repo: &Repo) -> Vec<u8> {
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn.execute(
        "INSERT INTO snapshot(id, status, source_root, host, tool_version, started_at) \
         VALUES (?1, 'in_progress', x'2f', 'h', 't', 1)",
        [Id::new_v7().as_bytes().to_vec()],
    )
    .unwrap();
    let mut wal = repo.catalog_path().into_os_string();
    wal.push("-wal");
    let bytes = fs::read(&wal).unwrap();
    assert!(!bytes.is_empty());
    bytes
}

#[test]
fn stale_wal_without_main_file_is_kept_aside_not_replayed() {
    let (_tmp, repo, src) = setup();
    two_snapshots(&repo, &src);
    let wal = live_wal_bytes(&repo);
    remove_catalog(&repo);
    let mut wal_path = repo.catalog_path().into_os_string();
    wal_path.push("-wal");
    fs::write(&wal_path, &wal).unwrap();

    let report = recover::run(&repo, &RecoverOptions::default()).unwrap();
    assert!(report.intact(), "{report:#?}");
    assert_eq!(report.old_catalog, None);
    assert_eq!(report.snapshots.len(), 2);
    let conn = rusqlite::Connection::open(repo.catalog_path()).unwrap();
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    // A replayed WAL would bring back its in_progress row.
    let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
    assert_eq!(count("SELECT count(*) FROM snapshot"), 2);
    assert_eq!(
        count("SELECT count(*) FROM snapshot WHERE status = 'in_progress'"),
        0
    );
    let kept: Vec<_> = fs::read_dir(&repo.root)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("catalog.sqlite.pre-recover-") && n.ends_with("-wal"))
        .collect();
    assert_eq!(kept.len(), 1, "{kept:?}");
    assert_eq!(fs::read(repo.root.join(&kept[0])).unwrap(), wal);
    assert!(
        report.warnings.iter().any(|w| w.contains(&kept[0])),
        "{:?}",
        report.warnings
    );
}

#[test]
fn recover_upgrades_a_version_1_repository() {
    let (_tmp, repo, src) = setup();
    two_snapshots(&repo, &src);
    rusqlite::Connection::open(repo.catalog_path())
        .unwrap()
        .pragma_update(None, "user_version", 1)
        .unwrap();
    let cfg = Config {
        format_version: 1,
        ..Config::default()
    };
    cfg.save(&repo.root.join("wmk.toml")).unwrap();
    let repo = Repo::open(&repo.root).unwrap();
    assert_eq!(repo.config.format_version, 1);

    let report = recover::run(&repo, &RecoverOptions::default()).unwrap();
    assert!(report.intact(), "{report:#?}");
    assert_eq!(user_version(&repo.catalog_path()), 2);
    assert_eq!(
        Config::load(&repo.root.join("wmk.toml"))
            .unwrap()
            .format_version,
        2
    );
    assert_eq!(user_version(backups(&repo)[0].as_path()), 1);
}

#[test]
fn stale_recover_temp_files_are_swept() {
    let (_tmp, repo, src) = setup();
    two_snapshots(&repo, &src);
    fs::create_dir_all(repo.tmp_dir()).unwrap();
    for n in ["recover-old.sqlite", "recover-old.sqlite-wal"] {
        fs::write(repo.tmp_dir().join(n), b"junk").unwrap();
    }
    recover::run(&repo, &RecoverOptions::default()).unwrap();
    no_tmp_leftovers(&repo);
}
