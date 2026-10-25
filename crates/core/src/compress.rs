//! Per-unit compression policy (spec §7.6).

use crate::error::{Error, Result};
use std::borrow::Cow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Zstd = 1,
}

impl Compression {
    pub fn as_str(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Zstd => "zstd",
        }
    }

    pub fn parse(s: &str) -> Option<Compression> {
        match s {
            "none" => Some(Compression::None),
            "zstd" => Some(Compression::Zstd),
            _ => None,
        }
    }

    pub fn from_u8(v: u8) -> Option<Compression> {
        match v {
            0 => Some(Compression::None),
            1 => Some(Compression::Zstd),
            _ => None,
        }
    }
}

/// Choose and apply compression. Stores raw when `precompressed`, or when
/// zstd output is >= 97% of the raw size. Deterministic for a given input.
pub fn encode(raw: &[u8], precompressed: bool, level: i32) -> Result<(Compression, Cow<'_, [u8]>)> {
    if precompressed {
        return Ok((Compression::None, Cow::Borrowed(raw)));
    }
    let z = zstd::bulk::compress(raw, level)
        .map_err(|e| Error::Corrupt(format!("zstd compress failed: {e}")))?;
    if (z.len() as u64) * 100 >= (raw.len() as u64) * 97 {
        Ok((Compression::None, Cow::Borrowed(raw)))
    } else {
        Ok((Compression::Zstd, Cow::Owned(z)))
    }
}

pub fn decode(c: Compression, stored: &[u8], raw_len: u64) -> Result<Vec<u8>> {
    let out = match c {
        Compression::None => stored.to_vec(),
        Compression::Zstd => {
            // Callers must bound raw_len first (pack::MAX_UNIT_LEN); this cap of
            // raw_len + 1 only lets an over-long stream be detected, it does
            // not protect against an unbounded raw_len.
            let cap = usize::try_from(raw_len)
                .map_err(|_| Error::Corrupt("raw_len too large".into()))?
                .saturating_add(1);
            zstd::bulk::decompress(stored, cap)
                .map_err(|e| Error::Corrupt(format!("zstd decompress failed: {e}")))?
        }
    };
    if out.len() as u64 != raw_len {
        return Err(Error::Corrupt(format!(
            "unit length {} != expected {}",
            out.len(),
            raw_len
        )));
    }
    Ok(out)
}
