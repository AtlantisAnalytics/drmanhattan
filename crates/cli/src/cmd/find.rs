use crate::cmd::open_repo;
use crate::output::{display_bytes, emit};
use crate::Cli;
use anyhow::Result;
use clap::Args;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::ids::Hash32;
use serde::Serialize;
use std::fmt;

#[derive(Args)]
pub struct FindArgs {
    /// Content hash (64 hex characters)
    #[arg(long)]
    pub content: String,
}

#[derive(Serialize)]
struct Hit {
    snapshot_id: String,
    entry_id: String,
    path: String,
}

#[derive(Serialize)]
#[serde(transparent)]
struct Hits(Vec<Hit>);

impl fmt::Display for Hits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, h) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{} {} {}", h.snapshot_id, h.entry_id, h.path)?;
        }
        Ok(())
    }
}

pub fn run(cli: &Cli, args: &FindArgs) -> Result<()> {
    let repo = open_repo(cli)?;
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let hash = Hash32::from_hex(&args.content)?;
    let hits = cat
        .entries_with_content(&hash)?
        .into_iter()
        .map(|e| Hit {
            snapshot_id: e.snapshot_id.to_string(),
            entry_id: e.id.to_string(),
            path: display_bytes(&e.path),
        })
        .collect();
    let hits = Hits(hits);
    if !cli.json && hits.0.is_empty() {
        return Ok(());
    }
    emit(cli.json, &hits)
}
