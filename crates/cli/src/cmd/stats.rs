use crate::cmd::open_repo;
use crate::output::emit;
use crate::Cli;
use anyhow::Result;
use clap::Args;
use drmanhattan_core::catalog::{Catalog, RepoStats};
use serde::Serialize;
use std::fmt;

#[derive(Args)]
pub struct StatsArgs {}

#[derive(Serialize)]
struct Report {
    #[serde(flatten)]
    totals: RepoStatsJson,
    dedup_ratio: f64,
    storage_ratio: f64,
}

#[derive(Serialize)]
struct RepoStatsJson {
    snapshots: u64,
    entries: u64,
    contents: u64,
    units: u64,
    packs: u64,
    logical_bytes: u64,
    unique_bytes: u64,
    stored_bytes: u64,
}

fn ratio(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

impl Report {
    fn new(s: RepoStats) -> Report {
        Report {
            totals: RepoStatsJson {
                snapshots: s.snapshots,
                entries: s.entries,
                contents: s.contents,
                units: s.units,
                packs: s.packs,
                logical_bytes: s.logical_bytes,
                unique_bytes: s.unique_bytes,
                stored_bytes: s.stored_bytes,
            },
            dedup_ratio: ratio(s.logical_bytes, s.unique_bytes),
            storage_ratio: ratio(s.stored_bytes, s.logical_bytes),
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let t = &self.totals;
        writeln!(f, "snapshots:     {}", t.snapshots)?;
        writeln!(f, "entries:       {}", t.entries)?;
        writeln!(f, "contents:      {}", t.contents)?;
        writeln!(f, "units:         {}", t.units)?;
        writeln!(f, "packs:         {}", t.packs)?;
        writeln!(f, "logical bytes: {}", t.logical_bytes)?;
        writeln!(f, "unique bytes:  {}", t.unique_bytes)?;
        writeln!(f, "stored bytes:  {}", t.stored_bytes)?;
        writeln!(f, "dedup ratio:   {:.2}", self.dedup_ratio)?;
        write!(f, "storage ratio: {:.3}", self.storage_ratio)
    }
}

pub fn run(cli: &Cli, _args: &StatsArgs) -> Result<()> {
    let repo = open_repo(cli)?;
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    emit(cli.json, &Report::new(cat.stats()?))
}
