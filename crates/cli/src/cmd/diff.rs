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
    /// Descend into other filesystems (use if the snapshot was taken with --cross-fs)
    #[arg(long)]
    pub cross_fs: bool,
}

/// JSON shape of one difference (stable): `{"kind", "path"}` plus, for
/// `mode` and `mtime`, `"snapshot"` and `"dir"` values (octal mode string /
/// decimal ns string), and for `expected-missing`, `"status"`. Kinds:
/// content, mode, mtime, kind, link-target, missing, extra, expected-missing.
#[derive(Serialize)]
struct ItemJson {
    kind: &'static str,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'static str>,
}

struct Item<'a>(&'a Difference);

impl Serialize for Item<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let path = display_bytes(self.0.path());
        let plain = |kind| ItemJson {
            kind,
            path: display_bytes(self.0.path()),
            snapshot: None,
            dir: None,
            status: None,
        };
        let j = match self.0 {
            Difference::ContentDiffers { .. } => plain("content"),
            Difference::ModeDiffers { snap, dir, .. } => ItemJson {
                snapshot: Some(format!("{snap:o}")),
                dir: Some(format!("{dir:o}")),
                ..plain("mode")
            },
            Difference::MtimeDiffers { snap, dir, .. } => ItemJson {
                snapshot: Some(snap.to_string()),
                dir: Some(dir.to_string()),
                ..plain("mtime")
            },
            Difference::KindDiffers { .. } => plain("kind"),
            Difference::LinkTargetDiffers { .. } => plain("link-target"),
            Difference::Missing { .. } => plain("missing"),
            Difference::Extra { .. } => plain("extra"),
            Difference::ExpectedMissing { status, .. } => ItemJson {
                status: Some(status.as_str()),
                ..plain("expected-missing")
            },
        };
        debug_assert_eq!(j.path, path);
        j.serialize(s)
    }
}

impl fmt::Display for Item<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let p = display_bytes(self.0.path());
        match self.0 {
            Difference::ContentDiffers { .. } => write!(f, "~content {p}"),
            Difference::ModeDiffers { snap, dir, .. } => {
                write!(f, "~mode {p} {snap:o}\u{2192}{dir:o}")
            }
            Difference::MtimeDiffers { snap, dir, .. } => {
                write!(f, "~mtime {p} {snap}\u{2192}{dir}")
            }
            Difference::KindDiffers { .. } => write!(f, "~kind {p}"),
            Difference::LinkTargetDiffers { .. } => write!(f, "~link-target {p}"),
            Difference::Missing { .. } => write!(f, "-missing {p}"),
            Difference::Extra { .. } => write!(f, "+extra {p}"),
            Difference::ExpectedMissing { status, .. } => {
                write!(f, "=expected-missing {p} ({})", status.as_str())
            }
        }
    }
}

struct Report<'a>(&'a [Difference]);

impl Serialize for Report<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("Report", 1)?;
        let items: Vec<Item> = self.0.iter().map(Item).collect();
        st.serialize_field("differences", &items)?;
        st.end()
    }
}

impl fmt::Display for Report<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, d) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{}", Item(d))?;
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
        cross_fs: args.cross_fs,
    };
    let diffs = diff::run(&repo, snapshot, &args.dir, &opts)?;
    let real = diffs
        .iter()
        .filter(|d| !matches!(d, Difference::ExpectedMissing { .. }))
        .count();
    if cli.json || !diffs.is_empty() {
        emit(cli.json, &Report(&diffs))?;
    }
    if real > 0 {
        return Err(DiffFound(real).into());
    }
    Ok(0)
}
