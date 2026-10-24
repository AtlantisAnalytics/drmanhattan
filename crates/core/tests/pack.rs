use drmanhattan_core::compress::{self, Compression};
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::pack::{
    pack_path, scan, PackReader, PackWriter, RecordHeader, UnitKind, MAX_UNIT_LEN,
};
use std::path::Path;

/// On-disk size of one record header.
const REC_HDR: usize = 58;

fn units() -> Vec<Vec<u8>> {
    vec![
        vec![7u8; 5000],                                    // compresses with zstd
        (0..=255u8).cycle().take(300).collect::<Vec<u8>>(), // stored raw (precompressed)
        b"tiny".to_vec(),
    ]
}

fn append_unit(w: &mut PackWriter, raw: &[u8], precompressed: bool) -> (u64, Hash32) {
    let (c, stored) = compress::encode(raw, precompressed, 3).unwrap();
    let h = RecordHeader {
        kind: UnitKind::RawChunk,
        compression: c,
        unit_hash: Hash32::of(raw),
        raw_len: raw.len() as u64,
        stored_len: stored.len() as u64,
    };
    (w.append(&h, &stored).unwrap(), h.unit_hash)
}

fn write_three(dir: &Path, seal: bool) -> (Id, Vec<(u64, Hash32)>) {
    let id = Id::new_v7();
    let mut w = PackWriter::create(dir, id).unwrap();
    let mut out = vec![];
    for (i, u) in units().iter().enumerate() {
        out.push(append_unit(&mut w, u, i == 1));
    }
    if seal {
        w.seal().unwrap();
    } else {
        w.sync().unwrap();
    }
    (id, out)
}

fn flip(path: &Path, at: usize) {
    let mut bytes = std::fs::read(path).unwrap();
    bytes[at] ^= 0xFF;
    std::fs::write(path, &bytes).unwrap();
}

fn file_header(id: Id) -> Vec<u8> {
    let mut v = b"WMKPACK\0".to_vec();
    v.extend_from_slice(&1u16.to_le_bytes());
    v.extend_from_slice(id.as_bytes());
    v
}

/// A record header with a correct CRC, for crafting hostile files.
fn rec_header(kind: u8, comp: u8, raw_len: u64, stored_len: u64) -> Vec<u8> {
    let mut v = 0x574D_4B52u32.to_le_bytes().to_vec();
    v.push(kind);
    v.push(comp);
    v.extend_from_slice(&[0xAB; 32]);
    v.extend_from_slice(&raw_len.to_le_bytes());
    v.extend_from_slice(&stored_len.to_le_bytes());
    let crc = crc32c::crc32c(&v[4..]);
    v.extend_from_slice(&crc.to_le_bytes());
    v
}

fn crafted(dir: &Path, rec: &[u8]) -> std::path::PathBuf {
    let p = dir.join("crafted.pack");
    let mut bytes = file_header(Id::new_v7());
    bytes.extend_from_slice(rec);
    std::fs::write(&p, bytes).unwrap();
    p
}

#[test]
fn append_read_roundtrip() {
    let t = tempfile::tempdir().unwrap();
    let (id, recs) = write_three(t.path(), true);
    let r = PackReader::open(&pack_path(t.path(), id)).unwrap();
    for ((off, hash), raw) in recs.iter().zip(units()) {
        assert_eq!(r.read_unit(*off, hash).unwrap(), raw);
        let (h, payload) = r.read_record(*off).unwrap();
        assert_eq!(h.stored_len as usize, payload.len());
    }
    assert!(matches!(
        r.read_unit(recs[0].0, &recs[1].1),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn seal_footer_and_hash() {
    let t = tempfile::tempdir().unwrap();
    let id = Id::new_v7();
    let mut w = PackWriter::create(t.path(), id).unwrap();
    assert!(w.is_empty());
    for u in units() {
        append_unit(&mut w, &u, false);
    }
    assert!(!w.is_empty());
    let sealed = w.seal().unwrap();
    let p = pack_path(t.path(), id);
    let s = scan(&p).unwrap();
    assert_eq!(s.id, id);
    assert!(s.sealed);
    assert_eq!(s.records.len(), 3);
    assert_eq!(s.blake3_ok, Some(true));
    assert!(s.damaged.is_empty());
    let len = std::fs::metadata(&p).unwrap().len();
    assert_eq!(s.valid_len, len);
    assert_eq!(sealed.size, len);
    let bytes = std::fs::read(&p).unwrap();
    let footer = 8 + 40 * 3 + 32 + 8 + 8;
    assert_eq!(sealed.blake3, Hash32::of(&bytes[..bytes.len() - footer]));
    // footer_offset is stored just before the trailing magic.
    let fo = u64::from_le_bytes(bytes[bytes.len() - 16..bytes.len() - 8].try_into().unwrap());
    assert_eq!(fo as usize, bytes.len() - footer);
}

#[test]
fn create_refuses_existing() {
    let t = tempfile::tempdir().unwrap();
    let id = Id::new_v7();
    PackWriter::create(t.path(), id).unwrap();
    assert!(PackWriter::create(t.path(), id).is_err());
}

#[test]
fn flipped_payload_byte_detected() {
    let t = tempfile::tempdir().unwrap();
    let (id, recs) = write_three(t.path(), true);
    let p = pack_path(t.path(), id);
    flip(&p, recs[0].0 as usize + REC_HDR + 1);
    let r = PackReader::open(&p).unwrap();
    assert!(matches!(
        r.read_unit(recs[0].0, &recs[0].1),
        Err(Error::Corrupt(_))
    ));
    let s = scan(&p).unwrap();
    assert!(s.sealed);
    assert_eq!(s.blake3_ok, Some(false));
    assert!(s.damaged.is_empty());
}

#[test]
fn sealed_flipped_header_reports_damage() {
    let t = tempfile::tempdir().unwrap();
    let (id, recs) = write_three(t.path(), true);
    let p = pack_path(t.path(), id);
    flip(&p, recs[1].0 as usize + 10);
    let s = scan(&p).unwrap();
    assert!(s.sealed);
    assert_eq!(s.damaged, vec![recs[1].0]);
    let offs: Vec<u64> = s.records.iter().map(|(o, _)| *o).collect();
    assert_eq!(offs, vec![recs[0].0, recs[2].0]);
    assert_eq!(s.blake3_ok, Some(false));
    assert_eq!(s.valid_len, std::fs::metadata(&p).unwrap().len());
    let r = PackReader::open(&p).unwrap();
    let e = r.read_record(recs[1].0).unwrap_err();
    assert!(matches!(e, Error::Corrupt(ref m) if m.contains(&recs[1].0.to_string())));
}

#[test]
fn unsealed_flipped_header_stops_scan() {
    let t = tempfile::tempdir().unwrap();
    let (id, recs) = write_three(t.path(), false);
    let p = pack_path(t.path(), id);
    flip(&p, recs[1].0 as usize + 10);
    let s = scan(&p).unwrap();
    assert!(!s.sealed);
    assert_eq!(s.records.len(), 1);
    assert_eq!(s.valid_len, recs[1].0);
    assert_eq!(s.blake3_ok, None);
    assert!(s.damaged.is_empty());
}

#[test]
fn torn_tail_unsealed() {
    let t = tempfile::tempdir().unwrap();
    let (id, recs) = write_three(t.path(), false);
    let p = pack_path(t.path(), id);
    let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
    f.set_len(recs[1].0 + REC_HDR as u64 + 2).unwrap();
    let s = scan(&p).unwrap();
    assert!(!s.sealed);
    assert_eq!(s.records.len(), 1);
    assert_eq!(s.valid_len, recs[1].0);
    assert_eq!(s.blake3_ok, None);
}

#[test]
fn torn_inside_record_header() {
    let t = tempfile::tempdir().unwrap();
    let (id, recs) = write_three(t.path(), false);
    let p = pack_path(t.path(), id);
    let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
    f.set_len(recs[1].0 + 20).unwrap();
    let s = scan(&p).unwrap();
    assert!(!s.sealed);
    assert_eq!(s.records.len(), 1);
    assert_eq!(s.valid_len, recs[1].0);
}

#[test]
fn torn_footer_is_unsealed() {
    let t = tempfile::tempdir().unwrap();
    let id = Id::new_v7();
    let mut w = PackWriter::create(t.path(), id).unwrap();
    for u in units() {
        append_unit(&mut w, &u, false);
    }
    let sealed = w.seal().unwrap();
    let footer_offset = sealed.size - (8 + 40 * 3 + 32 + 8 + 8);
    let p = pack_path(t.path(), id);
    let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
    f.set_len(footer_offset + 30).unwrap();
    let s = scan(&p).unwrap();
    assert!(!s.sealed);
    assert_eq!(s.records.len(), 3);
    assert_eq!(s.valid_len, footer_offset);
    assert_eq!(s.blake3_ok, None);
}

#[test]
fn footer_entry_mismatch_is_damage() {
    let t = tempfile::tempdir().unwrap();
    let id = Id::new_v7();
    let mut w = PackWriter::create(t.path(), id).unwrap();
    for u in units() {
        append_unit(&mut w, &u, false);
    }
    let sealed = w.seal().unwrap();
    let footer_offset = (sealed.size - (8 + 40 * 3 + 32 + 8 + 8)) as usize;
    let p = pack_path(t.path(), id);
    // Corrupt the hash of index entry 1.
    flip(&p, footer_offset + 8 + 40);
    let s = scan(&p).unwrap();
    assert!(s.sealed);
    assert_eq!(s.records.len(), 2);
    assert_eq!(s.damaged.len(), 1);
    assert_eq!(s.blake3_ok, Some(true));
}

#[test]
fn huge_stored_len_is_rejected_without_allocating() {
    let t = tempfile::tempdir().unwrap();
    // Far beyond MAX_UNIT_LEN.
    let p = crafted(t.path(), &rec_header(0, 0, u64::MAX, u64::MAX));
    let r = PackReader::open(&p).unwrap();
    assert!(matches!(r.read_record(26), Err(Error::Corrupt(_))));
    let s = scan(&p).unwrap();
    assert!(s.records.is_empty());
    assert_eq!(s.valid_len, 26);
    // Within bounds but the payload is not in the file.
    let p = crafted(t.path(), &rec_header(0, 0, MAX_UNIT_LEN, MAX_UNIT_LEN));
    let r = PackReader::open(&p).unwrap();
    assert!(matches!(r.read_record(26), Err(Error::Corrupt(_))));
    assert_eq!(scan(&p).unwrap().valid_len, 26);
}

#[test]
fn inconsistent_lengths_rejected() {
    let t = tempfile::tempdir().unwrap();
    // none with stored != raw
    let p = crafted(t.path(), &rec_header(0, 0, 10, 9));
    assert!(PackReader::open(&p).unwrap().read_record(26).is_err());
    // zstd with stored >= raw
    let p = crafted(t.path(), &rec_header(0, 1, 10, 10));
    assert!(PackReader::open(&p).unwrap().read_record(26).is_err());
    // append rejects the same
    let mut w = PackWriter::create(t.path(), Id::new_v7()).unwrap();
    let h = RecordHeader {
        kind: UnitKind::RawChunk,
        compression: Compression::Zstd,
        unit_hash: Hash32::of(b"x"),
        raw_len: 1,
        stored_len: 1,
    };
    assert!(matches!(w.append(&h, b"x"), Err(Error::Corrupt(_))));
}

#[test]
fn unknown_kind_with_valid_crc_is_error() {
    let t = tempfile::tempdir().unwrap();
    let p = crafted(t.path(), &rec_header(9, 0, 0, 0));
    match scan(&p) {
        Err(Error::Corrupt(m)) => assert!(m.contains("unsupported unit kind"), "{m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
    let p = crafted(t.path(), &rec_header(0, 7, 0, 0));
    match scan(&p) {
        Err(Error::Corrupt(m)) => assert!(m.contains("unsupported compression"), "{m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[test]
fn writer_is_poisoned_after_io_error() {
    let t = tempfile::tempdir().unwrap();
    let mut w = PackWriter::create(t.path(), Id::new_v7()).unwrap();
    append_unit(&mut w, b"first", true);
    // Test seam: swap in a read-only handle so the next write fails for real.
    w.__break_file_for_test().unwrap();
    let h = RecordHeader {
        kind: UnitKind::RawChunk,
        compression: Compression::None,
        unit_hash: Hash32::of(b"second"),
        raw_len: 6,
        stored_len: 6,
    };
    assert!(matches!(w.append(&h, b"second"), Err(Error::Io { .. })));
    match w.append(&h, b"second") {
        Err(Error::Corrupt(m)) => assert!(m.contains("poisoned"), "{m}"),
        other => panic!("expected poisoned, got {other:?}"),
    }
    assert!(w.sync().is_err());
    match w.seal() {
        Err(Error::Corrupt(m)) => assert!(m.contains("poisoned"), "{m}"),
        other => panic!("expected poisoned, got {other:?}"),
    }
}

#[test]
fn bad_file_header_is_error() {
    let t = tempfile::tempdir().unwrap();
    let p = t.path().join("x.pack");
    std::fs::write(&p, b"nope").unwrap();
    assert!(matches!(scan(&p), Err(Error::Corrupt(_))));
    std::fs::write(&p, [0u8; 40]).unwrap();
    assert!(matches!(scan(&p), Err(Error::Corrupt(_))));
}

#[test]
fn path_fanout() {
    let id = Id::from_bytes([
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ]);
    assert_eq!(
        pack_path(Path::new("/r/packs"), id),
        Path::new("/r/packs/10/01020304-0506-0708-090a-0b0c0d0e0f10.pack")
    );
}
