//! Screening hook (spec §8). Every new content is screened before any of its
//! units is emitted. Plan 1 ships only `NoopScreener`.

use crate::ids::Hash32;
use crate::sniff::Format;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    /// Matched a screening list; the detail names the match.
    Match(String),
    /// The screener could not check the content.
    Error(String),
}

pub trait Screener: Send + Sync {
    fn screen(&self, path: &[u8], content: &Hash32, size: u64, format: Format) -> Verdict;
}

/// Passes everything.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopScreener;

impl Screener for NoopScreener {
    fn screen(&self, _path: &[u8], _content: &Hash32, _size: u64, _format: Format) -> Verdict {
        Verdict::Pass
    }
}
