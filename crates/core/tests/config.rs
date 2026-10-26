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
fn config_parse_error_escapes_terminal_controls() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    Repo::init(&root).unwrap();
    std::fs::write(root.join("wmk.toml"), "format_version = \x1b[2J\n").unwrap();
    match Repo::open(&root) {
        Err(Error::Config(m)) => {
            assert!(!m.contains('\x1b'), "{m:?}");
            assert!(m.contains("\\x1b[2J"), "{m:?}");
            // Line breaks in the parser's multi-line report survive.
            assert!(m.contains('\n'), "{m:?}");
        }
        other => panic!("expected Config error, got {:?}", other.map(|_| ())),
    }
}

#[test]
fn config_rejects_unknown_format_version() {
    match open_with(|c| c.format_version = 2) {
        Err(Error::Config(m)) => assert_eq!(m, "unsupported format_version 2"),
        other => panic!("expected Config error, got {:?}", other.map(|_| ())),
    }
}

fn config_error(edit: impl FnOnce(&mut Config)) -> String {
    match open_with(edit) {
        Err(Error::Config(m)) => m,
        other => panic!("expected Config error, got {:?}", other.map(|_| ())),
    }
}

#[test]
fn config_rejects_unknown_compression_algo() {
    let m = config_error(|c| c.compression.algo = "lz4".into());
    assert!(m.contains("compression.algo"), "{m}");
}

#[test]
fn config_rejects_out_of_range_compression_level() {
    for level in [-200_000, 23] {
        let m = config_error(|c| c.compression.level = level);
        assert!(m.contains("compression.level"), "{level}: {m}");
    }
    for level in [1, 19, 22] {
        let mut c = Config::default();
        c.compression.level = level;
        c.validate().unwrap();
    }
}

#[test]
fn config_rejects_out_of_range_pack_size() {
    for size in [0, (1 << 20) - 1, (4 << 30) + 1] {
        let m = config_error(|c| c.packs.max_size = size);
        assert!(m.contains("packs.max_size"), "{size}: {m}");
    }
    for size in [1 << 20, 4 << 30] {
        let mut c = Config::default();
        c.packs.max_size = size;
        c.validate().unwrap();
    }
}
