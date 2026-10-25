use crate::cmd::open_repo;
use crate::output::{display_bytes, emit};
use crate::Cli;
use anyhow::Result;
use clap::Args;
use drmanhattan_core::ingest::{self, IngestOptions, IngestReport};
use serde::Serialize;
use std::fmt;
use std::path::PathBuf;

#[derive(Args)]
pub struct IngestArgs {
    /// Directory to snapshot
    pub dir: PathBuf,
    /// Free-text note stored on the snapshot
    #[arg(long)]
    pub note: Option<String>,
    /// Store files that change while being read, flagged unstable
    #[arg(long)]
    pub allow_unstable: bool,
    /// Descend into other filesystems
    #[arg(long)]
    pub cross_fs: bool,
    /// Reader threads (0 = one per CPU)
    #[arg(long, default_value_t = 0)]
    pub workers: usize,
}

#[derive(Serialize)]
struct Skipped {
    path: String,
    reason: &'static str,
}

#[derive(Serialize)]
struct Report {
    snapshot: String,
    manifest_hash: String,
    entries: u64,
    files: u64,
    bytes_read: u64,
    new_units: u64,
    new_bytes_stored: u64,
    /// bytes_read / new_bytes_stored; 0 when nothing new was stored.
    dedup_ratio: f64,
    throughput_bytes_per_sec: u64,
    elapsed_ms: u64,
    skipped: Vec<Skipped>,
    unstable: Vec<String>,
    withheld: u64,
}

impl Report {
    fn new(r: &IngestReport) -> Report {
        Report {
            snapshot: r.snapshot.to_string(),
            manifest_hash: r.manifest_hash.to_hex(),
            entries: r.entries,
            files: r.files,
            bytes_read: r.bytes_read,
            new_units: r.new_units,
            new_bytes_stored: r.new_bytes_stored,
            dedup_ratio: r.dedup_ratio,
            throughput_bytes_per_sec: r.bytes_read.saturating_mul(1000) / r.elapsed_ms.max(1),
            elapsed_ms: r.elapsed_ms,
            skipped: r
                .skipped
                .iter()
                .map(|(p, reason)| Skipped {
                    path: display_bytes(p),
                    reason: reason.as_str(),
                })
                .collect(),
            unstable: r.unstable.iter().map(|p| display_bytes(p)).collect(),
            withheld: r.withheld,
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "snapshot {}", self.snapshot)?;
        writeln!(f, "manifest {}", self.manifest_hash)?;
        write!(
            f,
            "{} entries ({} files), {} bytes read, {} new units, {} new bytes stored, \
             dedup ratio {:.2}, {} ms",
            self.entries,
            self.files,
            self.bytes_read,
            self.new_units,
            self.new_bytes_stored,
            self.dedup_ratio,
            self.elapsed_ms
        )?;
        for s in &self.skipped {
            write!(f, "\nskipped ({}): {}", s.reason, s.path)?;
        }
        for p in &self.unstable {
            write!(f, "\nunstable: {p}")?;
        }
        if self.withheld > 0 {
            write!(f, "\nwithheld: {}", self.withheld)?;
        }
        Ok(())
    }
}

/// Returns the exit code: 3 when entries were withheld by screening, else 0.
pub fn run(cli: &Cli, args: &IngestArgs) -> Result<i32> {
    let repo = open_repo(cli)?;
    let report = ingest::run(
        &repo,
        &args.dir,
        &IngestOptions {
            note: args.note.clone(),
            allow_unstable: args.allow_unstable,
            cross_fs: args.cross_fs,
            workers: args.workers,
            ..IngestOptions::default()
        },
    )?;
    emit(cli.json, &Report::new(&report))?;
    Ok(if report.withheld > 0 { 3 } else { 0 })
}
