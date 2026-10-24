mod cmd;
mod output;

use clap::{Parser, Subcommand};
use drmanhattan_core::error::Error;
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
}

/// Map an error to a process exit code. Core errors are 1; later tasks add 2 and 3.
pub fn exit_code(err: &anyhow::Error) -> i32 {
    match err.downcast_ref::<Error>() {
        Some(_) => 1,
        None => 1,
    }
}

fn main() {
    let cli = Cli::parse();
    let result = match &cli.command {
        Command::Init(args) => cmd::init::run(&cli, args).map(|()| 0),
        Command::Ingest(args) => cmd::ingest::run(&cli, args),
        Command::Manifest(args) => cmd::manifest::run(&cli, args).map(|()| 0),
        Command::Restore(args) => cmd::restore::run(&cli, args),
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
