use assert_cmd::Command;
use drmanhattan_core::ids::Id;
use drmanhattan_core::pack::pack_path;
use predicates::prelude::*;
use std::path::{Path, PathBuf};

fn wmk() -> Command {
    let mut cmd = Command::cargo_bin("wmk").unwrap();
    cmd.env_remove("WMK_REPO");
    cmd
}

/// init + ingest of a few files, one of them incompressible; returns the
/// repo path and the ingest's manifest hash.
fn ingested(tmp: &Path) -> (PathBuf, String) {
    let repo = tmp.join("r");
    let src = tmp.join("src");
    std::fs::create_dir_all(src.join("d")).unwrap();
    std::fs::write(src.join("a.html"), b"<html>hi</html>").unwrap();
    let mut x = 0x1234_5678_9abc_def1u64;
    let noise: Vec<u8> = (0..200_000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect();
    std::fs::write(src.join("d/noise.bin"), noise).unwrap();
    wmk().arg("init").arg(&repo).assert().success();
    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("--json")
        .arg("ingest")
        .arg(&src)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    (repo, v["manifest_hash"].as_str().unwrap().to_string())
}

fn remove_catalog(repo: &Path) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(repo.join(format!("catalog.sqlite{suffix}")));
    }
}

/// Flip one payload byte of the unit holding `d/noise.bin`.
fn damage_noise(repo: &Path) {
    let conn = rusqlite::Connection::open(repo.join("catalog.sqlite")).unwrap();
    let (pack_id, offset): (Vec<u8>, i64) = conn
        .query_row(
            "SELECT u.pack_id, u.pack_offset FROM unit u \
             JOIN content_unit cu ON cu.unit_hash = u.hash \
             JOIN entry e ON e.content_hash = cu.content_hash \
             WHERE e.path = CAST('d/noise.bin' AS BLOB) AND cu.seq = 0",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    drop(conn);
    let id = Id::from_bytes(pack_id.try_into().unwrap());
    let path = pack_path(&repo.join("packs"), id);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[offset as usize + 58 + 1] ^= 0xFF;
    std::fs::write(&path, bytes).unwrap();
}

#[test]
fn recover_rebuilds_deleted_catalog() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo, hash) = ingested(tmp.path());
    remove_catalog(&repo);
    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("recover")
        .assert()
        .code(0)
        .stdout(predicate::str::contains("OK: 1 snapshots"));
    let out = tmp.path().join("out");
    wmk()
        .arg("--repo")
        .arg(&repo)
        .args(["restore", "latest"])
        .arg(&out)
        .assert()
        .code(0);
    let m = wmk().arg("manifest").arg(&out).output().unwrap();
    assert!(m.status.success());
    assert!(String::from_utf8(m.stdout).unwrap().contains(&hash));
}

#[test]
fn recover_reports_time_per_step() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo, _) = ingested(tmp.path());
    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .args(["--json", "recover"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let steps = v["steps_ms"].as_object().expect("steps_ms object");
    let names: Vec<&str> = steps.keys().map(|k| k.as_str()).collect();
    for step in ["scan", "replay", "media", "damage", "check"] {
        assert!(names.contains(&step), "missing {step} in {names:?}");
    }
    let sum: u64 = steps.values().map(|t| t.as_u64().unwrap()).sum();
    assert!(sum <= v["elapsed_ms"].as_u64().unwrap());
    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("recover")
        .assert()
        .code(0)
        .stdout(predicate::str::contains("steps: scan "));
}

#[test]
fn recover_reports_damage_with_exit_2() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo, _) = ingested(tmp.path());
    damage_noise(&repo);
    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("recover")
        .assert()
        .code(2)
        .stdout(predicate::str::contains("d/noise.bin"))
        .stdout(predicate::str::contains("DAMAGED: 1 snapshots"));
    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("ls")
        .assert()
        .code(0)
        .stdout(predicate::str::contains("damaged"));
}

#[test]
fn recover_json_lists_damaged_files() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo, _) = ingested(tmp.path());
    damage_noise(&repo);
    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .args(["--json", "recover"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(v["damaged"].as_array().unwrap().len(), 1);
    assert_eq!(v["damaged"][0]["files"][0], "d/noise.bin");
    assert!(v["old_catalog"].is_string());
}

#[test]
fn recover_with_a_broken_record_header_exits_2() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo, _) = ingested(tmp.path());
    std::fs::write(tmp.path().join("src/more.txt"), b"second ingest").unwrap();
    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("ingest")
        .arg(tmp.path().join("src"))
        .assert()
        .code(0);
    // Break the header CRC of the newest snapshot's record (its only copy).
    let conn = rusqlite::Connection::open(repo.join("catalog.sqlite")).unwrap();
    let (pack_id, offset): (Vec<u8>, i64) = conn
        .query_row(
            "SELECT u.pack_id, u.pack_offset FROM snapshot s \
             JOIN unit u ON u.hash = s.record_unit ORDER BY s.started_at DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    drop(conn);
    let id = Id::from_bytes(pack_id.try_into().unwrap());
    let path = pack_path(&repo.join("packs"), id);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[offset as usize + 54] ^= 0xFF;
    std::fs::write(&path, bytes).unwrap();
    remove_catalog(&repo);

    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("recover")
        .assert()
        .code(2)
        .stdout(predicate::str::contains(format!("bytes {offset}..")))
        .stdout(predicate::str::contains("DAMAGED: 1 snapshots"));
    remove_catalog(&repo);
    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .args(["--json", "recover"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["ok"], false);
    let u = v["unaccounted"].as_array().unwrap();
    assert_eq!(u.len(), 1, "{v}");
    assert_eq!(u[0]["start"], offset);
    assert_eq!(u[0]["pack_id"], id.to_string());
}

#[test]
fn cat_of_a_damaged_content_exits_2_without_output() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo, _) = ingested(tmp.path());
    let hash: String = rusqlite::Connection::open(repo.join("catalog.sqlite"))
        .unwrap()
        .query_row(
            "SELECT lower(hex(content_hash)) FROM entry \
             WHERE path = CAST('d/noise.bin' AS BLOB)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    damage_noise(&repo);
    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("recover")
        .assert()
        .code(2);
    wmk()
        .arg("--repo")
        .arg(&repo)
        .args(["cat", &hash])
        .assert()
        .code(2)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("missing 1 of"));
}

#[test]
fn ingest_without_catalog_suggests_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let (repo, _) = ingested(tmp.path());
    remove_catalog(&repo);
    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("ingest")
        .arg(tmp.path().join("src"))
        .assert()
        .code(1)
        .stderr(predicate::str::contains("wmk recover"));
}
