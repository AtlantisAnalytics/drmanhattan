mod cmd;
mod output;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "wmk",
    about = "wmk (watchmaker): disassemble directories into storage units and rebuild them exactly",
    version
)]
pub struct Cli {
    /// Repository path (or set WMK_REPO)
    #[arg(long, global = true, env = "WMK_REPO")]
    pub repo: Option<PathBuf>,
    /// Emit machine-readable JSON
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Create a new repository
    Init(cmd::init::InitArgs),
    /// Snapshot a directory into the repository
    Ingest(cmd::ingest::IngestArgs),
    /// Compute the manifest hash of a live directory
    Manifest(cmd::manifest::ManifestArgs),
    /// Rebuild a snapshot byte-exact in a new directory
    Restore(cmd::restore::RestoreArgs),
    /// Check that stored data is intact
    Verify(cmd::verify::VerifyArgs),
    /// Show path-level differences between a snapshot and a live directory
    Diff(cmd::diff::DiffArgs),
}

/// `verify` found problems (exit code 2).
#[derive(Debug)]
pub struct VerifyFailed(pub usize);

impl std::fmt::Display for VerifyFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "verification failed: {} problem(s)", self.0)
    }
}

impl std::error::Error for VerifyFailed {}

/// `diff` found differences other than expected-missing (exit code 2).
#[derive(Debug)]
pub struct DiffFound(pub usize);

impl std::fmt::Display for DiffFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} difference(s) found", self.0)
    }
}

impl std::error::Error for DiffFound {}

/// Map an error to a process exit code: 2 for verification failure or
/// differences found, otherwise 1. (3, entries withheld, is a success-path code.)
pub fn exit_code(err: &anyhow::Error) -> i32 {
    if err.is::<VerifyFailed>() || err.is::<DiffFound>() {
        2
    } else {
        1
    }
}

fn main() {
    let cli = Cli::parse();
    let result = match &cli.command {
        Command::Init(args) => cmd::init::run(&cli, args).map(|()| 0),
        Command::Ingest(args) => cmd::ingest::run(&cli, args),
        Command::Manifest(args) => cmd::manifest::run(&cli, args).map(|()| 0),
        Command::Restore(args) => cmd::restore::run(&cli, args),
        Command::Verify(args) => cmd::verify::run(&cli, args),
        Command::Diff(args) => cmd::diff::run(&cli, args),
    };
    match result {
        Ok(0) => {}
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(exit_code(&err));
        }
    }
}
