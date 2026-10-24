use crate::config::Config;
use crate::error::{Error, Result};
use fs4::{FileExt, TryLockError};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

const CONFIG_FILE: &str = "wmk.toml";
const LOCK_FILE: &str = "wmk.lock";
const CATALOG_FILE: &str = "catalog.sqlite";
const PACKS_DIR: &str = "packs";

#[derive(Debug)]
pub struct Repo {
    pub root: PathBuf,
    pub config: Config,
}

/// Holds the exclusive writer lock until dropped.
#[derive(Debug)]
pub struct WriterLock {
    file: File,
}

impl Drop for WriterLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

impl Repo {
    /// Create a new repository in `root` (which must be absent or empty).
    pub fn init(root: &Path) -> Result<Repo> {
        match std::fs::read_dir(root) {
            Ok(mut entries) => {
                if entries.next().is_some() {
                    return Err(Error::RepoNotEmpty);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::io(root, e)),
        }
        let packs = root.join(PACKS_DIR);
        std::fs::create_dir_all(&packs).map_err(|e| Error::io(&packs, e))?;
        let config = Config::default();
        config.save(&root.join(CONFIG_FILE))?;
        Ok(Repo {
            root: root.to_path_buf(),
            config,
        })
    }

    pub fn open(root: &Path) -> Result<Repo> {
        let cfg_path = root.join(CONFIG_FILE);
        if !cfg_path.is_file() {
            return Err(Error::NotARepo);
        }
        let config = Config::load(&cfg_path)?;
        Ok(Repo {
            root: root.to_path_buf(),
            config,
        })
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.root.join(CATALOG_FILE)
    }

    pub fn packs_dir(&self) -> PathBuf {
        self.root.join(PACKS_DIR)
    }

    /// Take the non-blocking exclusive writer lock on `wmk.lock`.
    pub fn lock_writer(&self) -> Result<WriterLock> {
        let path = self.root.join(LOCK_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|e| Error::io(&path, e))?;
        match FileExt::try_lock(&file) {
            Ok(()) => Ok(WriterLock { file }),
            Err(TryLockError::WouldBlock) => Err(Error::Locked),
            Err(TryLockError::Error(e)) => Err(Error::io(&path, e)),
        }
    }
}
