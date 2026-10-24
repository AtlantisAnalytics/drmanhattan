use drmanhattan_core::config::Config;
use drmanhattan_core::error::Error;
use drmanhattan_core::repo::Repo;

fn open_with(edit: impl FnOnce(&mut Config)) -> Result<Repo, Error> {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    Repo::init(&root).unwrap();
    let mut cfg = Config::default();
    edit(&mut cfg);
    cfg.save(&root.join("wmk.toml")).unwrap();
    Repo::open(&root)
}

#[test]
fn default_config_validates() {
    Config::default().validate().unwrap();
}

#[test]
fn config_rejects_out_of_bounds_profile() {
    match open_with(|c| c.chunking.text.min = 1) {
        Err(Error::Config(m)) => assert!(m.contains("text.min"), "{m}"),
        other => panic!("expected Config error, got {:?}", other.map(|_| ())),
    }
}

#[test]
fn config_rejects_min_gt_avg() {
    let r = open_with(|c| c.chunking.binary.min = 2_000_000);
    assert!(matches!(r, Err(Error::Config(_))));
}

#[test]
fn config_rejects_unknown_format_version() {
    match open_with(|c| c.format_version = 2) {
        Err(Error::Config(m)) => assert_eq!(m, "unsupported format_version 2"),
        other => panic!("expected Config error, got {:?}", other.map(|_| ())),
    }
}
