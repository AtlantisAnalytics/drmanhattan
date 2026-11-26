use crate::catalog::Catalog;
use crate::config::Config;
use crate::error::{Error, Result};
use fs4::{FileExt, TryLockError};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

const CONFIG_FILE: &str = "wmk.toml";
const LOCK_FILE: &str = "wmk.lock";
const CATALOG_FILE: &str = "catalog.sqlite";
const PACKS_DIR: &str = "packs";
const TMP_DIR: &str = "tmp";

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
        Catalog::open(&root.join(CATALOG_FILE))?;
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

    /// Scratch space for files that never outlive a command (created on
    /// demand).
    pub fn tmp_dir(&self) -> PathBuf {
        self.root.join(TMP_DIR)
    }

    /// Whether any `*.pack` file exists under `packs/<2 hex>/`.
    pub fn has_packs(&self) -> Result<bool> {
        let dir = self.packs_dir();
        let top = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(Error::io(&dir, e)),
        };
        for shard in top {
            let shard = shard.map_err(|e| Error::io(&dir, e))?.path();
            if !shard.is_dir() {
                continue;
            }
            for f in std::fs::read_dir(&shard).map_err(|e| Error::io(&shard, e))? {
                let f = f.map_err(|e| Error::io(&shard, e))?.path();
                if f.extension().is_some_and(|x| x == "pack") {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Delete stale `record-*.cbor` files directly in `tmp/`; returns how
    /// many. The caller must hold the writer lock.
    pub fn sweep_tmp(&self) -> Result<u64> {
        let dir = self.tmp_dir();
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(Error::io(&dir, e)),
        };
        let mut n = 0;
        for entry in rd {
            let entry = entry.map_err(|e| Error::io(&dir, e))?;
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_file = entry
                .file_type()
                .map_err(|e| Error::io(&path, e))?
                .is_file();
            if is_file && name.starts_with("record-") && name.ends_with(".cbor") {
                std::fs::remove_file(&path).map_err(|e| Error::io(&path, e))?;
                n += 1;
            }
        }
        Ok(n)
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
