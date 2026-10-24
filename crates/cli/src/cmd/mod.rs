pub mod ingest;
pub mod init;
pub mod manifest;

use crate::Cli;
use anyhow::{anyhow, Result};
use drmanhattan_core::repo::Repo;

/// Open the repository named by `--repo` / `WMK_REPO`.
pub fn open_repo(cli: &Cli) -> Result<Repo> {
    let path = cli
        .repo
        .as_ref()
        .ok_or_else(|| anyhow!("no repository given (use --repo or WMK_REPO)"))?;
    Ok(Repo::open(path)?)
}
