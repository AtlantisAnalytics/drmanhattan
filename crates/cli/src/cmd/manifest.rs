use crate::output::emit;
use crate::Cli;
use anyhow::Result;
use clap::Args;
use drmanhattan_core::manifest::{manifest_dir, ManifestOptions};
use serde::Serialize;
use std::path::PathBuf;

#[derive(Args)]
pub struct ManifestArgs {
    /// Directory to hash
    pub dir: PathBuf,
    /// Print the extended hash (core + hard links + xattrs)
    #[arg(long)]
    pub ext: bool,
    /// Floor mtimes to this many nanoseconds before hashing (0 and 1 mean exact)
    #[arg(long, default_value_t = 1)]
    pub mtime_precision: u64,
    /// Descend into other filesystems
    #[arg(long)]
    pub cross_fs: bool,
}

#[derive(Serialize)]
struct ManifestReport {
    core: String,
    ext: String,
    entries: u64,
    #[serde(skip)]
    show_ext: bool,
}

impl std::fmt::Display for ManifestReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.show_ext { &self.ext } else { &self.core })
    }
}

pub fn run(cli: &Cli, args: &ManifestArgs) -> Result<()> {
    let h = manifest_dir(
        &args.dir,
        &ManifestOptions {
            mtime_precision_ns: args.mtime_precision,
            cross_fs: args.cross_fs,
        },
    )?;
    emit(
        cli.json,
        &ManifestReport {
            core: h.core.to_hex(),
            ext: h.ext.to_hex(),
            entries: h.entries,
            show_ext: args.ext,
        },
    )
}
