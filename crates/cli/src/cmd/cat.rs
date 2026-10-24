use crate::cmd::open_repo;
use crate::Cli;
use anyhow::{anyhow, bail, Result};
use clap::Args;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::ids::{Hash32, Id};
use drmanhattan_core::model::{EntryKind, EntryStatus};
use drmanhattan_core::unitread::UnitReader;
use std::io::{BufWriter, Write};

/// Decoded-unit cache for streaming one content.
const CACHE_BYTES: usize = 64 << 20;

/// Stream file bytes to stdout.
///
/// The bytes are verified against the content hash as they stream, but a
/// mismatch can only be reported after the bytes were written: they cannot
/// be recalled, so on a nonzero exit treat the output as untrusted.
#[derive(Args)]
pub struct CatArgs {
    /// Entry id (UUID) of a file, or a content hash (64 hex characters)
    pub target: String,
}

pub fn run(cli: &Cli, args: &CatArgs) -> Result<()> {
    let repo = open_repo(cli)?;
    let cat = Catalog::open_readonly(&repo.catalog_path())?;
    let t = args.target.as_str();
    let expected = if t.len() == 64 && t.bytes().all(|b| b.is_ascii_hexdigit()) {
        Hash32::from_hex(t)?
    } else if let Ok(id) = Id::parse(t) {
        let e = cat.entry(id)?;
        if e.kind != EntryKind::File {
            bail!("entry {id} is a {}, not a file", e.kind.as_str());
        }
        if !matches!(e.status, EntryStatus::Ok | EntryStatus::Unstable) {
            bail!("entry {id} is withheld (status {})", e.status.as_str());
        }
        e.content_hash
            .ok_or_else(|| anyhow!("entry {id} has no content"))?
    } else {
        bail!("{t:?} is neither an entry id nor a 64-character content hash");
    };
    let units = cat.content_units(&expected)?;
    // An unknown hash has no content row; an empty content has no units.
    cat.content(&expected)?;
    let reader = UnitReader::new(&repo, CACHE_BYTES);
    let stdout = std::io::stdout().lock();
    let mut out = BufWriter::new(stdout);
    let got = reader.write_content(&units, &mut out)?;
    out.flush()?;
    if got != expected {
        bail!("content hash mismatch: expected {expected}, read {got}; output is corrupt");
    }
    Ok(())
}
