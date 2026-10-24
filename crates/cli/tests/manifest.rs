use assert_cmd::Command;

fn wmk() -> Command {
    let mut cmd = Command::cargo_bin("wmk").unwrap();
    cmd.env_remove("WMK_REPO");
    cmd
}

#[test]
fn manifest_prints_core_ext_and_json() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("f"), b"hello").unwrap();

    let core = wmk().arg("manifest").arg(tmp.path()).output().unwrap();
    assert!(core.status.success());
    let core = String::from_utf8(core.stdout).unwrap().trim().to_string();
    assert_eq!(core.len(), 64);

    let ext = wmk()
        .args(["manifest", "--ext"])
        .arg(tmp.path())
        .output()
        .unwrap();
    let ext = String::from_utf8(ext.stdout).unwrap().trim().to_string();
    assert_eq!(ext.len(), 64);
    assert_ne!(core, ext);

    let j = wmk()
        .args(["--json", "manifest", "--mtime-precision", "1000000000"])
        .arg(tmp.path())
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&j.stdout).unwrap();
    assert_eq!(v["entries"], 2);
    assert_eq!(v["core"].as_str().unwrap().len(), 64);
    assert_eq!(v["ext"].as_str().unwrap().len(), 64);
}
