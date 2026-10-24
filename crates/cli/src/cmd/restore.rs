use crate::cmd::open_repo;
use crate::output::emit;
use crate::Cli;
use anyhow::{anyhow, Result};
use clap::Args;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::error::Error;
use drmanhattan_core::restore::{self, RestoreOptions, RestoreReport};
use serde::Serialize;
use std::ffi::OsString;
use std::fmt;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

#[derive(Args)]
pub struct RestoreArgs {
    /// Snapshot to restore: `latest`, a full id, or an id prefix
    pub snapshot: String,
    /// Destination; must not exist (its parent must)
    pub dest: PathBuf,
    /// Restore only the entry at this snapshot path, as the destination itself
    #[arg(long)]
    pub path: Option<OsString>,
    /// Re-create hard links (otherwise members are independent copies)
    #[arg(long)]
    pub hardlinks: bool,
    /// Restore extended attributes
    #[arg(long)]
    pub xattrs: bool,
}

#[derive(Serialize)]
struct Report {
    snapshot: String,
    dest: String,
    files: u64,
    dirs: u64,
    symlinks: u64,
    bytes: u64,
    withheld: u64,
    mtime_precision_ns: u64,
    elapsed_ms: u64,
}

impl Report {
    fn new(snapshot: String, dest: String, r: &RestoreReport) -> Report {
        Report {
            snapshot,
            dest,
            files: r.files,
            dirs: r.dirs,
            symlinks: r.symlinks,
            bytes: r.bytes,
            withheld: r.withheld,
            mtime_precision_ns: r.mtime_precision_ns,
            elapsed_ms: r.elapsed_ms,
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "restored snapshot {} to {}", self.snapshot, self.dest)?;
        write!(
            f,
            "{} files, {} dirs, {} symlinks, {} bytes, {} ms",
            self.files, self.dirs, self.symlinks, self.bytes, self.elapsed_ms
        )?;
        if self.mtime_precision_ns > 1 {
            write!(
                f,
                "\nmtime precision: {} ns (finer mtimes were floored by the destination)",
                self.mtime_precision_ns
            )?;
        }
        if self.withheld > 0 {
            write!(f, "\nwithheld: {}", self.withheld)?;
        }
        Ok(())
    }
}

/// The snapshot path as stored: raw bytes, no trailing `/`.
fn snapshot_path(p: &Option<OsString>) -> Vec<u8> {
    let mut v = p
        .as_ref()
        .map(|p| p.as_bytes().to_vec())
        .unwrap_or_default();
    while v.last() == Some(&b'/') {
        v.pop();
    }
    v
}

/// Returns the exit code: 3 when entries were withheld by screening, else 0.
/// Each preflight problem is printed on its own line before failing.
pub fn run(cli: &Cli, args: &RestoreArgs) -> Result<i32> {
    let repo = open_repo(cli)?;
    let snapshot =
        Catalog::open_readonly(&repo.catalog_path())?.resolve_snapshot(&args.snapshot)?;
    let opts = RestoreOptions {
        path: snapshot_path(&args.path),
        hardlinks: args.hardlinks,
        xattrs: args.xattrs,
    };
    let report = match restore::run(&repo, snapshot, &args.dest, &opts) {
        Ok(r) => r,
        Err(Error::Preflight(msgs)) => {
            for m in &msgs {
                eprintln!("{m}");
            }
            return Err(anyhow!(
                "preflight failed: {} problem(s); nothing was written",
                msgs.len()
            ));
        }
        Err(e) => return Err(e.into()),
    };
    emit(
        cli.json,
        &Report::new(
            snapshot.to_string(),
            args.dest.display().to_string(),
            &report,
        ),
    )?;
    Ok(if report.withheld > 0 { 3 } else { 0 })
}
