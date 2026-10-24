use assert_cmd::Command;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

fn wmk() -> Command {
    let mut cmd = Command::cargo_bin("wmk").unwrap();
    cmd.env_remove("WMK_REPO");
    cmd
}

#[test]
fn verify_and_diff_exit_codes() {
    let tmp = tempfile::tempdir().unwrap();
    let t = tmp.path();
    std::fs::create_dir(t.join("src")).unwrap();
    std::fs::write(t.join("src/a.txt"), b"hello\n").unwrap();
    wmk().current_dir(t).args(["init", "r"]).assert().success();
    wmk()
        .current_dir(t)
        .args(["--repo", "r", "ingest", "src"])
        .assert()
        .success();
    wmk()
        .current_dir(t)
        .args(["--repo", "r", "restore", "latest", "out"])
        .assert()
        .success();

    for args in [
        &["verify"][..],
        &["verify", "--quick"],
        &["verify", "latest"],
    ] {
        let out = wmk()
            .current_dir(t)
            .args(["--repo", "r"])
            .args(args)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(0), "{args:?}");
    }
    let out = wmk()
        .current_dir(t)
        .args(["--repo", "r", "diff", "latest", "out"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert!(out.stdout.is_empty());

    std::fs::write(t.join("out").join(OsStr::from_bytes(b"x\xc3\xa9y")), b"?").unwrap();
    let out = wmk()
        .current_dir(t)
        .args(["--repo", "r", "diff", "latest", "out"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains("+extra x\u{e9}y"), "{s}");

    let out = wmk()
        .current_dir(t)
        .args(["--repo", "r", "--json", "diff", "latest", "out"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(v["differences"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["kind"] == "extra" && d["path"] == "x\u{e9}y"));

    // Break a pack: full verify exits 2 and prints the problem.
    let pack = walk_packs(&t.join("r/packs"));
    let mut bytes = std::fs::read(&pack).unwrap();
    std::fs::set_permissions(&pack, std::os::unix::fs::PermissionsExt::from_mode(0o644)).unwrap();
    let n = bytes.len() / 2;
    bytes[n] ^= 0xff;
    std::fs::write(&pack, bytes).unwrap();
    let out = wmk()
        .current_dir(t)
        .args(["--repo", "r", "verify"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8(out.stdout).unwrap().contains("FAILED"));
}

#[test]
fn diff_expected_missing_only_exits_0() {
    let tmp = tempfile::tempdir().unwrap();
    let t = tmp.path();
    std::fs::create_dir(t.join("src")).unwrap();
    std::fs::write(t.join("src/a.txt"), b"hello\n").unwrap();
    std::fs::write(t.join("src/b.txt"), b"withheld\n").unwrap();
    wmk().current_dir(t).args(["init", "r"]).assert().success();
    wmk()
        .current_dir(t)
        .args(["--repo", "r", "ingest", "src"])
        .assert()
        .success();
    wmk()
        .current_dir(t)
        .args(["--repo", "r", "restore", "latest", "out"])
        .assert()
        .success();
    let conn = rusqlite::Connection::open(t.join("r/catalog.sqlite")).unwrap();
    conn.execute(
        "UPDATE entry SET status = 'screened_out', content_hash = NULL WHERE path = CAST('b.txt' AS BLOB)",
        [],
    )
    .unwrap();
    drop(conn);
    std::fs::remove_file(t.join("out/b.txt")).unwrap();
    let out = wmk()
        .current_dir(t)
        .args([
            "--repo",
            "r",
            "diff",
            "latest",
            "out",
            "--mtime-precision",
            "9223372036854775807",
        ])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert_eq!(
        String::from_utf8(out.stdout).unwrap().trim(),
        "=expected-missing b.txt (screened_out)"
    );
}

fn walk_packs(dir: &std::path::Path) -> std::path::PathBuf {
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            return walk_packs(&p);
        }
        if p.extension().is_some_and(|x| x == "pack") {
            return p;
        }
    }
    panic!("no pack under {}", dir.display());
}
