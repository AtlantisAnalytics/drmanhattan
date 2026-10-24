use crate::cmd::open_repo;
use crate::output::emit;
use crate::{Cli, VerifyFailed};
use anyhow::Result;
use clap::Args;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::verify;
use serde::Serialize;
use std::fmt;

#[derive(Args)]
pub struct VerifyArgs {
    /// Snapshot to verify: `latest`, a full id, or an id prefix (default: all complete snapshots)
    pub snapshot: Option<String>,
    /// Check pack files and record headers only; skip decompressing and hashing units
    #[arg(long)]
    pub quick: bool,
}

#[derive(Serialize)]
struct Report {
    ok: bool,
    snapshots: u64,
    units_checked: u64,
    packs_checked: u64,
    problems: Vec<String>,
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for p in &self.problems {
            writeln!(f, "{p}")?;
        }
        write!(
            f,
            "{}: {} snapshots, {} units, {} packs, {} problems",
            if self.ok { "OK" } else { "FAILED" },
            self.snapshots,
            self.units_checked,
            self.packs_checked,
            self.problems.len()
        )
    }
}

pub fn run(cli: &Cli, args: &VerifyArgs) -> Result<i32> {
    let repo = open_repo(cli)?;
    let snapshot = match &args.snapshot {
        Some(s) => Some(Catalog::open_readonly(&repo.catalog_path())?.resolve_snapshot(s)?),
        None => None,
    };
    let r = verify::run(&repo, snapshot, args.quick)?;
    let n = r.problems.len();
    emit(
        cli.json,
        &Report {
            ok: n == 0,
            snapshots: r.snapshots,
            units_checked: r.units_checked,
            packs_checked: r.packs_checked,
            problems: r.problems,
        },
    )?;
    if n > 0 {
        return Err(VerifyFailed(n).into());
    }
    Ok(0)
}
