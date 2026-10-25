use crate::cmd::open_repo;
use crate::output::{display_bytes, emit};
use crate::Cli;
use anyhow::{bail, Result};
use clap::Args;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::model::EntryKind;
use serde::Serialize;
use std::ffi::OsString;
use std::fmt;
use std::os::unix::ffi::OsStrExt;

#[derive(Args)]
pub struct ShowArgs {
    /// Snapshot: `latest`, a full id, or an id prefix
    pub snapshot: String,
    /// Directory within the snapshot whose children to list (default: the root)
    pub path: Option<OsString>,
}

#[derive(Serialize)]
struct Row {
    id: String,
    name: String,
    kind: &'static str,
    size: Option<i64>,
    mode: u32,
    mtime_ns: i64,
    content_hash: Option<String>,
    status: &'static str,
}

#[derive(Serialize)]
#[serde(transparent)]
struct Rows(Vec<Row>);

impl fmt::Display for Rows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, r) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(
                f,
                "{:<7} {:>06o} {:>12} {}  {}",
                r.kind,
                r.mode & 0o7777,
                r.size.map_or("-".to_string(), |s| s.to_string()),
                r.id,
                r.name
            )?;
            if r.status != "ok" {
                write!(f, "  [{}]", r.status)?;
            }
        }
        Ok(())
    }
}

pub fn run(cli: &Cli, args: &ShowArgs) -> Result<()> {
    let repo = open_repo(cli)?;
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let snap = cat.resolve_snapshot(&args.snapshot)?;
    let mut path = args
        .path
        .as_ref()
        .map(|p| p.as_bytes().to_vec())
        .unwrap_or_default();
    while path.last() == Some(&b'/') {
        path.pop();
    }
    // The root is the entry with the empty path; its children are the listing.
    let dir = cat.entry_by_path(snap, &path)?;
    if dir.kind != EntryKind::Dir {
        bail!("{} is not a directory", display_bytes(&path));
    }
    let rows = cat
        .children(snap, Some(dir.id))?
        .into_iter()
        .map(|e| Row {
            id: e.id.to_string(),
            name: display_bytes(&e.name),
            kind: e.kind.as_str(),
            size: e.size,
            mode: e.mode,
            mtime_ns: e.mtime_ns,
            content_hash: e.content_hash.map(|h| h.to_hex()),
            status: e.status.as_str(),
        })
        .collect();
    emit(cli.json, &Rows(rows))
}
