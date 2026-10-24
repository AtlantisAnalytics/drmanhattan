use crate::cmd::open_repo;
use crate::output::{display_bytes, emit};
use crate::Cli;
use anyhow::Result;
use clap::Args;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::error::Error;
use drmanhattan_core::ids::Id;
use serde::Serialize;
use std::fmt;

#[derive(Args)]
pub struct ResolveArgs {
    /// Entry id (UUID)
    pub id: String,
}

#[derive(Serialize)]
struct Report {
    entry_id: String,
    snapshot_id: String,
    path: String,
    kind: &'static str,
    content_hash: Option<String>,
    status: &'static str,
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "snapshot: {}", self.snapshot_id)?;
        writeln!(f, "path:     {}", self.path)?;
        writeln!(f, "kind:     {}", self.kind)?;
        writeln!(f, "status:   {}", self.status)?;
        write!(
            f,
            "content:  {}",
            self.content_hash.as_deref().unwrap_or("-")
        )
    }
}

pub fn run(cli: &Cli, args: &ResolveArgs) -> Result<()> {
    let repo = open_repo(cli)?;
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let id = Id::parse(&args.id).map_err(|_| Error::NotFound(format!("entry {:?}", args.id)))?;
    let e = cat.entry(id)?;
    emit(
        cli.json,
        &Report {
            entry_id: e.id.to_string(),
            snapshot_id: e.snapshot_id.to_string(),
            path: display_bytes(&e.path),
            kind: e.kind.as_str(),
            content_hash: e.content_hash.map(|h| h.to_hex()),
            status: e.status.as_str(),
        },
    )
}
