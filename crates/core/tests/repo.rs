use drmanhattan_core::config::Config;
use drmanhattan_core::error::Error;
use drmanhattan_core::repo::Repo;

#[test]
fn init_creates_layout() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    let repo = Repo::init(&root).unwrap();
    assert_eq!(repo.root, root);
    assert!(root.join("wmk.toml").is_file());
    assert!(root.join("packs").is_dir());
    let cat_path = root.join("catalog.sqlite");
    assert!(cat_path.is_file());
    let cat = drmanhattan_core::catalog::Catalog::open_readonly(&cat_path).unwrap();
    assert!(cat.snapshots().unwrap().is_empty());
    let loaded = Config::load(&root.join("wmk.toml")).unwrap();
    assert_eq!(loaded, Config::default());
    assert_eq!(loaded.chunking.text.avg, 8192);
    assert_eq!(loaded.packs.max_size, 268_435_456);
}

#[test]
fn init_refuses_nonempty_dir() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("x"), b"x").unwrap();
    assert!(matches!(Repo::init(tmp.path()), Err(Error::RepoNotEmpty)));
}

#[test]
fn open_non_repo_fails() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(matches!(Repo::open(tmp.path()), Err(Error::NotARepo)));
}

#[test]
fn second_writer_lock_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    Repo::init(&root).unwrap();
    let a = Repo::open(&root).unwrap();
    let b = Repo::open(&root).unwrap();
    let lock = a.lock_writer().unwrap();
    assert!(matches!(b.lock_writer(), Err(Error::Locked)));
    drop(lock);
    assert!(b.lock_writer().is_ok());
}
