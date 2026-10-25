use assert_cmd::Command;

fn wmk() -> Command {
    let mut cmd = Command::cargo_bin("wmk").unwrap();
    cmd.env_remove("WMK_REPO");
    cmd
}

#[test]
fn ingest_reports_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("r");
    let src = tmp.path().join("src");
    std::fs::create_dir_all(src.join("d")).unwrap();
    std::fs::write(src.join("a.html"), b"<html>hi</html>").unwrap();
    std::fs::write(src.join("d/b.bin"), vec![7u8; 100_000]).unwrap();
    wmk().arg("init").arg(&repo).assert().success();

    let out = wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("ingest")
        .arg(&src)
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["snapshot"].as_str().unwrap().len(), 36);
    assert_eq!(v["entries"], 4);
    let hash = v["manifest_hash"].as_str().unwrap();

    let m = wmk().arg("manifest").arg(&src).output().unwrap();
    assert!(m.status.success());
    assert_eq!(String::from_utf8(m.stdout).unwrap().trim(), hash);

    // Human output names the snapshot too.
    let human = wmk()
        .arg("--repo")
        .arg(&repo)
        .arg("ingest")
        .arg(&src)
        .output()
        .unwrap();
    assert!(human.status.success());
    let text = String::from_utf8(human.stdout).unwrap();
    assert!(text.starts_with("snapshot "));
    assert!(text.contains(&format!("manifest {hash}")));
    assert!(text.contains("0 new units"));
}

#[test]
fn ingest_without_repo_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let out = wmk().arg("ingest").arg(tmp.path()).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("no repository"));
}
