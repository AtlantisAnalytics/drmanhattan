use drmanhattan_core::chunk::{chunk_reader, Chunk, FileDigest};
use drmanhattan_core::config::Profile;
use drmanhattan_core::ids::Hash32;
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

const TEXT: Profile = Profile {
    min: 2048,
    avg: 8192,
    max: 65536,
};
const BINARY: Profile = Profile {
    min: 262_144,
    avg: 1_048_576,
    max: 4_194_304,
};

fn seeded(len: usize) -> Vec<u8> {
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut v = vec![0u8; len];
    rng.fill_bytes(&mut v);
    v
}

fn run(p: &Profile, data: &[u8]) -> (Vec<Chunk>, Vec<u8>, FileDigest) {
    let mut chunks = Vec::new();
    let mut joined = Vec::new();
    let digest = chunk_reader(p, data, |c, bytes| {
        assert_eq!(c.len as usize, bytes.len());
        chunks.push(c);
        joined.extend_from_slice(bytes);
        Ok(())
    })
    .expect("chunking in-memory data");
    (chunks, joined, digest)
}

#[test]
fn empty_input_zero_chunks() {
    let (chunks, _, digest) = run(&TEXT, &[]);
    assert!(chunks.is_empty());
    assert_eq!(digest.size, 0);
    assert_eq!(digest.hash, Hash32::of(b""));
}

#[test]
fn below_min_is_single_chunk() {
    let data = seeded(2047);
    let (chunks, _, digest) = run(&TEXT, &data);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].offset, 0);
    assert_eq!(chunks[0].len, 2047);
    assert_eq!(digest.size, 2047);
}

#[test]
fn chunks_reassemble_and_hash() {
    let data = seeded(5 * 1024 * 1024);
    for p in [TEXT, BINARY] {
        let (chunks, joined, digest) = run(&p, &data);
        assert_eq!(joined, data);
        assert_eq!(digest.hash, Hash32::of(&data));
        assert_eq!(digest.size, data.len() as u64);
        let mut off = 0u64;
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.offset, off);
            off += u64::from(c.len);
            let slice = &data[c.offset as usize..(c.offset + u64::from(c.len)) as usize];
            assert_eq!(c.hash, Hash32::of(slice));
            assert!(c.len <= p.max);
            if i + 1 < chunks.len() {
                assert!(c.len >= p.min);
            }
        }
        assert_eq!(off, data.len() as u64);
    }
}

#[test]
fn read_error_maps_to_io() {
    struct Bad;
    impl std::io::Read for Bad {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("boom"))
        }
    }
    let err = chunk_reader(&TEXT, Bad, |_, _| Ok(())).unwrap_err();
    assert!(matches!(err, drmanhattan_core::error::Error::Io { .. }));
}

#[test]
fn boundaries_stable_after_prefix_edit() {
    let data = seeded(5 * 1024 * 1024);
    let mut edited = data.clone();
    edited.splice(1000..1000, [0xAB; 10]);
    let (orig, _, _) = run(&TEXT, &data);
    let (new, _, _) = run(&TEXT, &edited);
    let new_hashes: HashSet<_> = new.iter().map(|c| c.hash).collect();
    let kept = orig.iter().filter(|c| new_hashes.contains(&c.hash)).count();
    assert!(
        kept * 100 >= orig.len() * 80,
        "only {kept}/{} chunks survived",
        orig.len()
    );
}

type Vectors = BTreeMap<String, BTreeMap<String, Vec<(u64, u32)>>>;

#[test]
fn golden_vectors() {
    let mut actual = Vectors::new();
    for (name, p) in [("text", TEXT), ("binary", BINARY)] {
        for size in [1usize << 20, 9 << 20] {
            let (chunks, _, _) = run(&p, &seeded(size));
            actual.entry(name.to_string()).or_default().insert(
                size.to_string(),
                chunks.iter().map(|c| (c.offset, c.len)).collect(),
            );
        }
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/chunk_vectors.json");
    if !path.exists() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
        panic!("vectors generated; commit them");
    }
    let expected: Vectors = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        actual, expected,
        "chunk boundaries moved; dedup would break"
    );
}
