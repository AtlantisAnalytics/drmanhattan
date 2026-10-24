use crate::cmd::open_repo;
use crate::output::{display_bytes, emit};
use crate::Cli;
use anyhow::Result;
use clap::Args;
use drmanhattan_core::catalog::Catalog;
use serde::Serialize;
use std::fmt;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Args)]
pub struct LsArgs {}

#[derive(Serialize)]
struct Row {
    id: String,
    status: String,
    source_root: String,
    started_at: i64,
    completed_at: Option<i64>,
    entry_count: Option<i64>,
    file_bytes: Option<i64>,
    manifest_hash: Option<String>,
    note: Option<String>,
}

/// Nanoseconds since the Unix epoch as RFC 3339 UTC (seconds precision).
fn rfc3339(ns: i64) -> String {
    let secs = u64::try_from(ns.div_euclid(1_000_000_000)).unwrap_or(0);
    humantime::format_rfc3339_seconds(UNIX_EPOCH + Duration::from_secs(secs)).to_string()
}

#[derive(Serialize)]
#[serde(transparent)]
struct Rows(Vec<Row>);

impl fmt::Display for Rows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:<8}  {:<11}  {:<20}  {:>9}  {:>12}  NOTE",
            "ID", "STATUS", "STARTED", "ENTRIES", "BYTES"
        )?;
        for r in &self.0 {
            let dash = |v: Option<i64>| v.map_or("-".to_string(), |v| v.to_string());
            write!(
                f,
                "\n{:<8}  {:<11}  {:<20}  {:>9}  {:>12}  {}",
                &r.id[..8],
                r.status,
                rfc3339(r.started_at),
                dash(r.entry_count),
                dash(r.file_bytes),
                r.note.as_deref().unwrap_or("")
            )?;
        }
        Ok(())
    }
}

pub fn run(cli: &Cli, _args: &LsArgs) -> Result<()> {
    let repo = open_repo(cli)?;
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let rows = cat
        .snapshots()?
        .into_iter()
        .map(|s| Row {
            id: s.id.to_string(),
            status: s.status.as_str().into(),
            source_root: display_bytes(&s.source_root),
            started_at: s.started_at,
            completed_at: s.completed_at,
            entry_count: s.entry_count,
            file_bytes: s.file_bytes,
            manifest_hash: s.manifest_hash.map(|h| h.to_hex()),
            note: s.note,
        })
        .collect();
    emit(cli.json, &Rows(rows))
}
