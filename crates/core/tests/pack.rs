use drmanhattan_core::compress;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::pack::{pack_path, scan, PackReader, PackWriter, RecordHeader, UnitKind};
use std::path::Path;

fn units() -> Vec<Vec<u8>> {
    vec![
        vec![7u8; 5000],                                    // compresses
        (0..=255u8).cycle().take(300).collect::<Vec<u8>>(), // small
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
    assert!(r.read_unit(recs[0].0, &recs[1].1).is_err());
}

#[test]
fn seal_footer_and_hash() {
    let t = tempfile::tempdir().unwrap();
    let id = Id::new_v7();
    let mut w = PackWriter::create(t.path(), id).unwrap();
    for u in units() {
        append_unit(&mut w, &u, false);
    }
    let sealed = w.seal().unwrap();
    let p = pack_path(t.path(), id);
    let s = scan(&p).unwrap();
    assert_eq!(s.id, id);
    assert!(s.sealed);
    assert_eq!(s.records.len(), 3);
    assert_eq!(s.blake3_ok, Some(true));
    let len = std::fs::metadata(&p).unwrap().len();
    assert_eq!(s.valid_len, len);
    assert_eq!(sealed.size, len);
    let bytes = std::fs::read(&p).unwrap();
    let footer = 8 + 40 * 3 + 32 + 8;
    assert_eq!(sealed.blake3, Hash32::of(&bytes[..bytes.len() - footer]));
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
    let mut bytes = std::fs::read(&p).unwrap();
    bytes[recs[0].0 as usize + 58 + 1] ^= 0xFF;
    std::fs::write(&p, &bytes).unwrap();
    let r = PackReader::open(&p).unwrap();
    assert!(matches!(
        r.read_unit(recs[0].0, &recs[0].1),
        Err(Error::Corrupt(_))
    ));
    let s = scan(&p).unwrap();
    assert!(s.sealed);
    assert_eq!(s.blake3_ok, Some(false));
}

#[test]
fn flipped_header_byte_stops_scan() {
    let t = tempfile::tempdir().unwrap();
    let (id, recs) = write_three(t.path(), true);
    let p = pack_path(t.path(), id);
    let mut bytes = std::fs::read(&p).unwrap();
    bytes[recs[1].0 as usize + 10] ^= 0x01;
    std::fs::write(&p, &bytes).unwrap();
    let s = scan(&p).unwrap();
    assert_eq!(s.records.len(), 1);
    assert_eq!(s.valid_len, recs[1].0);
    assert!(!s.sealed);
    assert_eq!(s.blake3_ok, None);
    let r = PackReader::open(&p).unwrap();
    let e = r.read_record(recs[1].0).unwrap_err();
    assert!(matches!(e, Error::Corrupt(ref m) if m.contains(&recs[1].0.to_string())));
}

#[test]
fn torn_tail_unsealed() {
    let t = tempfile::tempdir().unwrap();
    let (id, recs) = write_three(t.path(), false);
    let p = pack_path(t.path(), id);
    let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
    f.set_len(recs[1].0 + 60).unwrap();
    let s = scan(&p).unwrap();
    assert!(!s.sealed);
    assert_eq!(s.records.len(), 1);
    assert_eq!(s.valid_len, recs[1].0);
    assert_eq!(s.blake3_ok, None);
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
    let id = Id::new_v7();
    let s = id.to_string();
    let p = pack_path(Path::new("/r/packs"), id);
    assert_eq!(
        p,
        Path::new("/r/packs")
            .join(&s[s.len() - 2..])
            .join(format!("{s}.pack"))
    );
}
