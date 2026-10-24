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
