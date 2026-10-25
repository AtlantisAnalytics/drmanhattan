use crate::display::display_path;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error on {}: {source}", display_path(path))]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("repository is locked by another writer")]
    Locked,
    #[error("not a watchmaker repository")]
    NotARepo,
    #[error("directory is not empty; refusing to initialize a repository there")]
    RepoNotEmpty,
    #[error("repository is corrupt: {0}")]
    Corrupt(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("ambiguous: {0}")]
    Ambiguous(String),
    #[error("preflight failed: {}", .0.join("; "))]
    Preflight(Vec<String>),
    #[error("tree changed while reading: {}", display_path(.0))]
    Unstable(PathBuf),
    #[error("source and repository paths overlap")]
    Overlap,
    #[error("catalog error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("config error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Error {
        Error::Io {
            path: path.into(),
            source,
        }
    }
}
