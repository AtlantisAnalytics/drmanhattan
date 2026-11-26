use crate::cmd::open_repo;
use crate::output::{display_bytes, display_path, emit};
use crate::Cli;
use anyhow::Result;
use clap::Args;
use drmanhattan_core::recover::{self, RecoverOptions, RecoverReport};
use serde::Serialize;
use std::fmt;

/// Damaged files listed per snapshot in human output (JSON lists all).
const MAX_LISTED: usize = 20;

#[derive(Args)]
pub struct RecoverArgs {
    /// Worker threads for scanning packs (0 = one per CPU; use 1 for spinning disks)
    #[arg(long, default_value_t = 0)]
    pub workers: usize,
}

#[derive(Serialize)]
struct DamagedSnapshot {
    id: String,
    files: Vec<String>,
}

#[derive(Serialize)]
struct LostRecord {
    unit: String,
    snapshot: Option<String>,
    why: String,
}

#[derive(Serialize)]
struct Report {
    ok: bool,
    packs: u64,
    units: u64,
    bytes_read: u64,
    snapshots: Vec<String>,
    damaged: Vec<DamagedSnapshot>,
    lost: Vec<LostRecord>,
    warnings: Vec<String>,
    not_recovered: Vec<String>,
    old_catalog: Option<String>,
    elapsed_ms: u64,
}

impl Report {
    fn new(r: &RecoverReport) -> Report {
        Report {
            ok: r.intact(),
            packs: r.packs,
            units: r.units,
            bytes_read: r.bytes_read,
            snapshots: r.snapshots.iter().map(|s| s.to_string()).collect(),
            damaged: r
                .damaged
                .iter()
                .map(|d| DamagedSnapshot {
                    id: d.id.to_string(),
                    files: d.files.iter().map(|p| display_bytes(p)).collect(),
                })
                .collect(),
            lost: r
                .lost
                .iter()
                .map(|l| LostRecord {
                    unit: l.unit.to_string(),
                    snapshot: l.snapshot.map(|s| s.to_string()),
                    why: l.why.clone(),
                })
                .collect(),
            warnings: r.warnings.clone(),
            not_recovered: r.not_recovered.iter().map(|s| s.to_string()).collect(),
            old_catalog: r.old_catalog.as_deref().map(display_path),
            elapsed_ms: r.elapsed_ms,
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for d in &self.damaged {
            for p in d.files.iter().take(MAX_LISTED) {
                writeln!(f, "damaged: {} {p}", d.id)?;
            }
            if d.files.len() > MAX_LISTED {
                writeln!(
                    f,
                    "damaged: {} … and {} more",
                    d.id,
                    d.files.len() - MAX_LISTED
                )?;
            }
        }
        for l in &self.lost {
            let snap = l.snapshot.as_deref().unwrap_or("unknown snapshot");
            writeln!(f, "lost snapshot record {} ({snap}): {}", l.unit, l.why)?;
        }
        for w in &self.warnings {
            writeln!(f, "warning: {w}")?;
        }
        for s in &self.not_recovered {
            writeln!(f, "not recovered: snapshot {s} was in the previous catalog")?;
        }
        if let Some(p) = &self.old_catalog {
            writeln!(f, "previous catalog kept at {p}")?;
        }
        write!(
            f,
            "{}: {} snapshots, {} units, {} packs, {:.2} GiB read in {:.1}s",
            if self.ok { "OK" } else { "DAMAGED" },
            self.snapshots.len(),
            self.units,
            self.packs,
            self.bytes_read as f64 / (1u64 << 30) as f64,
            self.elapsed_ms as f64 / 1000.0
        )
    }
}

/// Returns 0 when every recovered snapshot is intact, else 2. Needs only
/// `wmk.toml`: the catalog may be missing or unusable.
pub fn run(cli: &Cli, args: &RecoverArgs) -> Result<i32> {
    let repo = open_repo(cli)?;
    let report = recover::run(
        &repo,
        &RecoverOptions {
            workers: args.workers,
        },
    )?;
    emit(cli.json, &Report::new(&report))?;
    Ok(if report.intact() { 0 } else { 2 })
}
