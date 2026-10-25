use assert_cmd::Command;
use std::path::Path;

fn wmk() -> Command {
    let mut cmd = Command::cargo_bin("wmk").unwrap();
    cmd.env_remove("WMK_REPO");
    cmd
}

/// `init` + `ingest` in `tmp`; returns the ingest's manifest hash.
fn ingested(tmp: &Path) -> String {
    let repo = tmp.join("r");
    let src = tmp.join("src");
    std::fs::create_dir_all(src.join("d/e")).unwrap();
    std::fs::write(src.join("a.html"), b"<html>hi</html>").unwrap();
    std::fs::write(src.join("d/b.bin"), vec![7u8; 300_000]).unwrap();
    std::fs::write(src.join("d/e/empty"), b"").unwrap();
    std::os::unix::fs::symlink("../a.html", src.join("d/link")).unwrap();
    wmk().arg("init").arg(&repo).assert().success();
    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("ingest")
        .arg(&src)
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    v["manifest_hash"].as_str().unwrap().to_string()
}

#[test]
fn restore_roundtrip_cli() {
    let tmp = tempfile::tempdir().unwrap();
    let hash = ingested(tmp.path());
    let out = wmk()
        .current_dir(tmp.path())
        .args(["--repo", "r", "restore", "latest", "out"])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8(out.stdout)
        .unwrap()
        .starts_with("restored snapshot "));
    let m = wmk()
        .arg("manifest")
        .arg(tmp.path().join("out"))
        .output()
        .unwrap();
    assert!(m.status.success());
    assert_eq!(String::from_utf8(m.stdout).unwrap().trim(), hash);

    // A subtree, as JSON.
    let out = wmk()
        .current_dir(tmp.path())
        .args([
            "--repo", "r", "--json", "restore", "latest", "sub", "--path", "d/",
        ])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        (v["files"].as_u64(), v["dirs"].as_u64()),
        (Some(2), Some(2))
    );
    assert_eq!(v["symlinks"], 1);
    assert_eq!(
        std::fs::read(tmp.path().join("sub/b.bin")).unwrap(),
        vec![7u8; 300_000]
    );
}

#[test]
fn restore_preflight_failure_exits_1() {
    let tmp = tempfile::tempdir().unwrap();
    ingested(tmp.path());
    std::fs::create_dir(tmp.path().join("out")).unwrap();
    let out = wmk()
        .current_dir(tmp.path())
        .args(["--repo", "r", "restore", "latest", "out"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8(out.stderr).unwrap();
    let mut lines = err.lines();
    assert!(
        lines.next().unwrap().starts_with("destination exists: "),
        "{err}"
    );
    assert!(
        lines.next().unwrap().starts_with("error: preflight failed"),
        "{err}"
    );
}
