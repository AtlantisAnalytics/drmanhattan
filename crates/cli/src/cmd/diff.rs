use crate::cmd::open_repo;
use crate::output::{display_bytes, emit};
use crate::{Cli, DiffFound};
use anyhow::Result;
use clap::Args;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::diff::{self, DiffOptions, Difference};
use serde::Serialize;
use std::ffi::OsString;
use std::fmt;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

#[derive(Args)]
pub struct DiffArgs {
    /// Snapshot to compare: `latest`, a full id, or an id prefix
    pub snapshot: String,
    /// Live directory to compare against
    pub dir: PathBuf,
    /// Compare only the snapshot subtree at this path, against the root of `dir`
    #[arg(long)]
    pub path: Option<OsString>,
    /// Floor mtimes to this many nanoseconds before comparing (0 and 1 mean exact)
    #[arg(long, default_value_t = 1)]
    pub mtime_precision: u64,
}

#[derive(Serialize)]
struct Item {
    kind: &'static str,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'static str>,
}

impl Item {
    fn new(d: &Difference) -> Item {
        let mut it = Item {
            kind: "",
            path: display_bytes(d.path()),
            snapshot: None,
            dir: None,
            status: None,
        };
        match d {
            Difference::ContentDiffers { .. } => it.kind = "content",
            Difference::ModeDiffers { snap, dir, .. } => {
                it.kind = "mode";
                it.snapshot = Some(format!("{snap:o}"));
                it.dir = Some(format!("{dir:o}"));
            }
            Difference::MtimeDiffers { snap, dir, .. } => {
                it.kind = "mtime";
                it.snapshot = Some(snap.to_string());
                it.dir = Some(dir.to_string());
            }
            Difference::KindDiffers { .. } => it.kind = "kind",
            Difference::LinkTargetDiffers { .. } => it.kind = "link-target",
            Difference::Missing { .. } => it.kind = "missing",
            Difference::Extra { .. } => it.kind = "extra",
            Difference::ExpectedMissing { status, .. } => {
                it.kind = "expected-missing";
                it.status = Some(status.as_str());
            }
        }
        it
    }
}

impl fmt::Display for Item {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let p = &self.path;
        match self.kind {
            "content" => write!(f, "~content {p}"),
            "mode" => write!(
                f,
                "~mode {p} {}\u{2192}{}",
                self.snapshot.as_deref().unwrap_or(""),
                self.dir.as_deref().unwrap_or("")
            ),
            "mtime" => write!(
                f,
                "~mtime {p} {}\u{2192}{}",
                self.snapshot.as_deref().unwrap_or(""),
                self.dir.as_deref().unwrap_or("")
            ),
            "kind" => write!(f, "~kind {p}"),
            "link-target" => write!(f, "~link-target {p}"),
            "missing" => write!(f, "-missing {p}"),
            "extra" => write!(f, "+extra {p}"),
            _ => write!(f, "=expected-missing {p} ({})", self.status.unwrap_or("")),
        }
    }
}

#[derive(Serialize)]
struct Report {
    differences: Vec<Item>,
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, d) in self.differences.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{d}")?;
        }
        Ok(())
    }
}

pub fn run(cli: &Cli, args: &DiffArgs) -> Result<i32> {
    let repo = open_repo(cli)?;
    let snapshot =
        Catalog::open_readonly(&repo.catalog_path())?.resolve_snapshot(&args.snapshot)?;
    let mut path = args
        .path
        .as_ref()
        .map(|p| p.as_bytes().to_vec())
        .unwrap_or_default();
    while path.last() == Some(&b'/') {
        path.pop();
    }
    let opts = DiffOptions {
        path,
        mtime_precision_ns: args.mtime_precision,
    };
    let diffs = diff::run(&repo, snapshot, &args.dir, &opts)?;
    let real = diffs
        .iter()
        .filter(|d| !matches!(d, Difference::ExpectedMissing { .. }))
        .count();
    let report = Report {
        differences: diffs.iter().map(Item::new).collect(),
    };
    if cli.json || !report.differences.is_empty() {
        emit(cli.json, &report)?;
    }
    if real > 0 {
        return Err(DiffFound(real).into());
    }
    Ok(0)
}
