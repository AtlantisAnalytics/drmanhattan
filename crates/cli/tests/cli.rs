use assert_cmd::Command;
use predicates::prelude::*;

fn wmk() -> Command {
    let mut cmd = Command::cargo_bin("wmk").unwrap();
    cmd.env_remove("WMK_REPO");
    cmd
}

#[test]
fn help_first_line() {
    wmk()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::starts_with(
        "wmk (watchmaker): disassemble directories into storage units and rebuild them exactly\n",
    ));
}

#[test]
fn init_then_init_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("r");
    wmk().arg("init").arg(&repo).assert().success();
    assert!(repo.join("wmk.toml").is_file());
    wmk()
        .arg("init")
        .arg(&repo)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("not empty"));
}

// ---- inspection commands ----

use serde_json::Value;
use std::path::Path;

fn run_json(repo: &Path, args: &[&str]) -> Value {
    let out = wmk()
        .arg("--repo")
        .arg(repo)
        .arg("--json")
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

/// init + ingest of a small tree with three identical 100 KiB files.
fn inspect_repo(tmp: &Path) -> std::path::PathBuf {
    let repo = tmp.join("r");
    let src = tmp.join("src");
    std::fs::create_dir_all(src.join("sub")).unwrap();
    let big: Vec<u8> = (0..100 * 1024u32).map(|i| (i * 7 % 251) as u8).collect();
    std::fs::write(src.join("a.bin"), &big).unwrap();
    std::fs::write(src.join("b.bin"), &big).unwrap();
    std::fs::write(src.join("sub/c.bin"), &big).unwrap();
    wmk().arg("init").arg(&repo).assert().success();
    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("ingest")
        .arg(&src)
        .assert()
        .success();
    repo
}

#[test]
fn ls_shows_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = inspect_repo(tmp.path());
    let v = run_json(&repo, &["ls"]);
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["status"], "complete");
    for k in [
        "id",
        "source_root",
        "started_at",
        "completed_at",
        "entry_count",
        "file_bytes",
        "manifest_hash",
        "note",
    ] {
        assert!(arr[0].get(k).is_some(), "missing key {k}");
    }
    let human = wmk().arg("--repo").arg(&repo).arg("ls").output().unwrap();
    assert!(human.status.success());
    assert!(String::from_utf8_lossy(&human.stdout).contains("complete"));
}

#[test]
fn show_root_children() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = inspect_repo(tmp.path());
    let v = run_json(&repo, &["show", "latest"]);
    let names: Vec<&str> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["a.bin", "b.bin", "sub"]);
    assert_eq!(v[2]["kind"], "dir");
    assert_eq!(v[0]["size"], 100 * 1024);
    let sub = run_json(&repo, &["show", "latest", "sub"]);
    assert_eq!(sub[0]["name"], "c.bin");
}

#[test]
fn stats_dedup() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = inspect_repo(tmp.path());
    let v = run_json(&repo, &["stats"]);
    let ratio = v["dedup_ratio"].as_f64().unwrap();
    assert!((ratio - 3.0).abs() < 0.01, "dedup_ratio {ratio}");
    assert_eq!(v["snapshots"], 1);
    assert_eq!(v["logical_bytes"], 3 * 100 * 1024);
    assert!(v["storage_ratio"].as_f64().unwrap() > 0.0);
}

#[test]
fn resolve_and_cat() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = inspect_repo(tmp.path());
    let shown = run_json(&repo, &["show", "latest"]);
    let id = shown[0]["id"].as_str().unwrap().to_string();
    let r = run_json(&repo, &["resolve", &id]);
    assert_eq!(r["path"], "a.bin");
    assert_eq!(r["entry_id"], id.as_str());
    assert_eq!(r["kind"], "file");
    let want = std::fs::read(tmp.path().join("src/a.bin")).unwrap();
    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .args(["cat", &id])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(out.stdout, want);
    // by content hash
    let hash = shown[0]["content_hash"].as_str().unwrap();
    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .args(["cat", hash])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(out.stdout, want);
}

#[test]
fn cat_rejects_dir_unknown_and_garbage() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = inspect_repo(tmp.path());
    let shown = run_json(&repo, &["show", "latest"]);
    let dir_id = shown[2]["id"].as_str().unwrap();
    for arg in [dir_id, "not-an-id", &"0".repeat(64)] {
        let out = wmk()
            .arg("--repo")
            .arg(&repo)
            .args(["cat", arg])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1), "cat {arg}");
        assert!(out.stdout.is_empty());
    }
    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .args(["resolve", "00000000-0000-7000-8000-000000000000"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn find_by_content() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = inspect_repo(tmp.path());
    let shown = run_json(&repo, &["show", "latest"]);
    let hash = shown[0]["content_hash"].as_str().unwrap();
    let v = run_json(&repo, &["find", "--content", hash]);
    let mut paths: Vec<&str> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    paths.sort();
    assert_eq!(paths, ["a.bin", "b.bin", "sub/c.bin"]);
    let none = run_json(&repo, &["find", "--content", &"0".repeat(64)]);
    assert_eq!(none.as_array().unwrap().len(), 0);
}

#[test]
fn cat_withheld_entry_exits_1() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = inspect_repo(tmp.path());
    let shown = run_json(&repo, &["show", "latest"]);
    assert_eq!(shown[0]["name"], "a.bin");
    let id = shown[0]["id"].as_str().unwrap().to_string();
    let conn = rusqlite::Connection::open(repo.join("catalog.sqlite")).unwrap();
    let n = conn
        .execute(
            "UPDATE entry SET status = 'screened_out', content_hash = NULL \
             WHERE path = CAST('a.bin' AS BLOB)",
            [],
        )
        .unwrap();
    assert_eq!(n, 1);
    drop(conn);
    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .args(["cat", &id])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("withheld"), "{stderr}");
}

#[test]
fn stats_empty_repo_is_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("r");
    wmk().arg("init").arg(&repo).assert().success();
    let v = run_json(&repo, &["stats"]);
    for k in [
        "snapshots",
        "entries",
        "contents",
        "units",
        "packs",
        "logical_bytes",
        "unique_bytes",
        "stored_bytes",
    ] {
        assert_eq!(v[k], 0, "{k}");
    }
    assert_eq!(v["dedup_ratio"].as_f64(), Some(0.0));
    assert_eq!(v["storage_ratio"].as_f64(), Some(0.0));
}

#[cfg(target_os = "linux")]
#[test]
fn show_non_utf8_name_escaped() {
    use std::os::unix::ffi::OsStrExt;
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("r");
    let src = tmp.path().join("src");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join(std::ffi::OsStr::from_bytes(b"bad\xff")), b"x").unwrap();
    wmk().arg("init").arg(&repo).assert().success();
    wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("ingest")
        .arg(&src)
        .assert()
        .success();
    let v = run_json(&repo, &["show", "latest"]);
    let names: Vec<&str> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["bad\\xff"]);
}
