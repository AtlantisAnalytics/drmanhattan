use crate::output::{display_path, emit};
use crate::Cli;
use anyhow::{anyhow, Result};
use clap::Args;
use drmanhattan_core::repo::Repo;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Args)]
pub struct InitArgs {
    /// Directory to create the repository in (defaults to --repo / WMK_REPO)
    pub path: Option<PathBuf>,
}

#[derive(Serialize)]
struct InitReport {
    path: String,
    format_version: u32,
}

impl std::fmt::Display for InitReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "initialized repository at {}", self.path)
    }
}

pub fn run(cli: &Cli, args: &InitArgs) -> Result<()> {
    let path = args
        .path
        .clone()
        .or_else(|| cli.repo.clone())
        .ok_or_else(|| anyhow!("no repository path given (use <path>, --repo, or WMK_REPO)"))?;
    let repo = Repo::init(&path)?;
    let report = InitReport {
        path: display_path(&repo.root),
        format_version: repo.config.format_version,
    };
    emit(cli.json, &report)
}
