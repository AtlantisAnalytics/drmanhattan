//! Kill `wmk ingest` at every point between two durable steps, then check
//! that the repository is still usable: it verifies, a fresh ingest works,
//! and `recover` from the packs alone brings back every snapshot whose
//! record was durable before the kill.
#![cfg(feature = "failpoints")]

use assert_cmd::Command;
use drmanhattan_core::ids::Id;
use drmanhattan_core::pack::{pack_path, RECORD_HEADER_LEN};
use rusqlite::OpenFlags;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Output;

/// Exit code of a kill point (`libc::_exit(137)`, not a signal).
const KILL_CODE: i32 = 137;

/// Kill points, and whether the killed ingest's snapshot record is durable.
const KILL_POINTS: [(&str, bool); 8] = [
    ("ingest::kill::after_walk", false),
    ("ingest::kill::before_sync", false),
    ("ingest::kill::after_sync", false),
    ("catalog::kill::after_commit", false),
    ("ingest::kill::during_record", false),
    ("ingest::kill::record_durable", true),
    ("ingest::kill::before_upsert", true),
    ("ingest::kill::after_upsert", true),
];

/// T2's shape: enough small files that its ingest buffers over twice the
/// default commit barrier (10_000 ops; 3 per file: unit, content, entry),
/// so it makes at least two data barrier commits before the finish phase.
/// The CLI has no way to lower the barrier.
const T2_DIRS: usize = 70;
const T2_FILES_PER_DIR: usize = 100;

fn wmk() -> Command {
    let mut cmd = Command::cargo_bin("wmk").unwrap();
    cmd.env_remove("WMK_REPO").env_remove("FAILPOINTS");
    cmd
}

fn run(repo: &Path, args: &[&str], extra: &[&Path]) -> Output {
    wmk()
        .arg("--repo")
        .arg(repo)
        .args(args)
        .args(extra)
        .output()
        .unwrap()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn assert_code(out: &Output, code: i32, what: &str) {
    assert_eq!(out.status.code(), Some(code), "{what}: {}", stderr(out));
}

/// Seeded xorshift bytes: incompressible and distinct per seed.
struct Noise(u64);

impl Noise {
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n)
            .map(|_| {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                (self.0 >> 24) as u8
            })
            .collect()
    }
}

fn write_tree(root: &Path, seed: u64, dirs: usize, files: usize, size: usize) {
    let mut noise = Noise(seed);
    for d in 0..dirs {
        let dir = root.join(format!("d{d:03}"));
        std::fs::create_dir_all(&dir).unwrap();
        for f in 0..files {
            std::fs::write(dir.join(format!("f{f:03}.bin")), noise.bytes(size)).unwrap();
        }
    }
}

fn manifest(dir: &Path) -> String {
    let out = wmk().arg("manifest").arg(dir).output().unwrap();
    assert_code(&out, 0, "manifest");
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

struct Tree {
    path: PathBuf,
    manifest: String,
}

/// Ingest `tree` and return the snapshot id.
fn ingest(repo: &Path, tree: &Tree) -> String {
    let out = run(repo, &["--json", "ingest"], &[&tree.path]);
    assert_code(&out, 0, "ingest");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["manifest_hash"], tree.manifest.as_str());
    v["snapshot"].as_str().unwrap().to_string()
}

fn assert_restores(repo: &Path, snapshot: &str, tree: &Tree, dest: &Path) {
    let out = run(repo, &["restore", snapshot], &[dest]);
    assert_code(&out, 0, "restore");
    assert_eq!(manifest(dest), tree.manifest, "snapshot {snapshot}");
}

fn read_only_catalog(repo: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open_with_flags(
        repo.join("catalog.sqlite"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap()
}

/// Every `unit` row points at bytes inside an existing pack file.
fn assert_units_in_packs(repo: &Path) {
    let conn = read_only_catalog(repo);
    let mut stmt = conn
        .prepare("SELECT pack_id, pack_offset, stored_size FROM unit")
        .unwrap();
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })
        .unwrap();
    let mut lens: HashMap<Vec<u8>, u64> = HashMap::new();
    for row in rows {
        let (pack, offset, stored) = row.unwrap();
        let len = *lens.entry(pack.clone()).or_insert_with(|| {
            let id = Id::from_bytes(pack.clone().try_into().unwrap());
            let path = pack_path(&repo.join("packs"), id);
            std::fs::metadata(&path)
                .unwrap_or_else(|e| panic!("unit row references {}: {e}", path.display()))
                .len()
        });
        let end = offset as u64 + RECORD_HEADER_LEN as u64 + stored as u64;
        assert!(
            end <= len,
            "unit ends at {end}, past its pack's {len} bytes"
        );
    }
}

/// Entry rows the killed (still `in_progress`) snapshot has committed.
fn in_progress_entries(repo: &Path) -> i64 {
    read_only_catalog(repo)
        .query_row(
            "SELECT count(*) FROM entry e JOIN snapshot s ON s.id = e.snapshot_id \
             WHERE s.status = 'in_progress'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

fn remove_catalog(repo: &Path) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(repo.join(format!("catalog.sqlite{suffix}")));
    }
}

fn kill_case(tmp: &Path, point: &str, record_durable: bool, t1: &Tree, t2: &Tree) {
    let repo = tmp.join("r");
    wmk().arg("init").arg(&repo).assert().success();
    ingest(&repo, t1);

    // Fire on the third commit: the first is the in-progress snapshot row,
    // so this is the second data barrier commit, mid-ingest.
    let spec = if point == "catalog::kill::after_commit" {
        format!("{point}=2*off->return")
    } else {
        format!("{point}=return")
    };
    let out = wmk()
        .env("FAILPOINTS", &spec)
        .arg("--repo")
        .arg(&repo)
        .arg("ingest")
        .arg(&t2.path)
        .output()
        .unwrap();
    assert_code(&out, KILL_CODE, &format!("{point}: killed ingest"));
    if point == "catalog::kill::after_commit" {
        let total = (T2_DIRS * (T2_FILES_PER_DIR + 1) + 1) as i64;
        let n = in_progress_entries(&repo);
        assert!(
            0 < n && n < total,
            "{point}: {n} of {total} entries committed"
        );
    }

    // 1. The catalog is consistent with the packs.
    assert_code(
        &run(&repo, &["verify", "--quick"], &[]),
        0,
        &format!("{point}: verify --quick"),
    );
    assert_units_in_packs(&repo);

    // 2. A fresh ingest of the same tree works and restores byte-exact.
    let again = ingest(&repo, t2);
    assert_restores(&repo, &again, t2, &tmp.join("again"));

    // 3. Recover from the packs alone.
    remove_catalog(&repo);
    assert_code(
        &run(&repo, &["recover"], &[]),
        0,
        &format!("{point}: recover"),
    );
    let out = run(&repo, &["--json", "ls"], &[]);
    assert_code(&out, 0, "ls");
    let snaps: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).unwrap();
    let expected = 2 + usize::from(record_durable);
    assert_eq!(snaps.len(), expected, "{point}: snapshots after recover");
    let mut t2_snaps = 0;
    for (i, s) in snaps.iter().enumerate() {
        let id = s["id"].as_str().unwrap();
        let root = s["source_root"].as_str().unwrap();
        let tree = if root.ends_with("/t1") {
            t1
        } else {
            assert!(root.ends_with("/t2"), "{point}: unexpected source {root}");
            t2_snaps += 1;
            t2
        };
        assert_eq!(s["status"], "complete", "{point}: snapshot {id}");
        assert_restores(&repo, id, tree, &tmp.join(format!("rec{i}")));
    }
    assert_eq!(t2_snaps, expected - 1, "{point}: T2 snapshots");
}

#[test]
fn every_kill_point_leaves_a_usable_repository() {
    let tmp = tempfile::tempdir().unwrap();
    let tree = |name: &str, seed, dirs, files, size| {
        let path = tmp.path().join(name);
        write_tree(&path, seed, dirs, files, size);
        Tree {
            manifest: manifest(&path),
            path,
        }
    };
    let t1 = tree("t1", 0x1234_5678_9abc_def1, 1, 5, 100_000);
    let t2 = tree("t2", 0x0fed_cba9_8765_4321, T2_DIRS, T2_FILES_PER_DIR, 256);

    // Each case has its own repository; the trees are only read. (Running
    // the cases on threads saves little: the time is mostly filesystem.)
    for (i, (point, record_durable)) in KILL_POINTS.into_iter().enumerate() {
        let case = tmp.path().join(format!("case{i}"));
        std::fs::create_dir(&case).unwrap();
        kill_case(&case, point, record_durable, &t1, &t2);
    }
}
