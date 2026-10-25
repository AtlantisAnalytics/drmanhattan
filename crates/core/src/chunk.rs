//! Content-defined chunking (FastCDC 2020) with one-pass BLAKE3 hashing.
//!
//! Chunk boundaries must never change for a given profile: dedup depends on
//! them. `tests/vectors/chunk_vectors.json` pins them.

use crate::config::Profile;
use crate::error::{Error, Result};
use crate::ids::Hash32;
use fastcdc::v2020::{Error as CdcError, StreamCDC};
use std::io::Read;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chunk {
    pub offset: u64,
    pub len: u32,
    pub hash: Hash32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileDigest {
    pub hash: Hash32,
    pub size: u64,
}

/// Split `r` into content-defined chunks, calling `on_chunk` for each with its
/// bytes. Returns the whole-stream BLAKE3 and size, computed in the same pass.
///
/// The profile must have passed `Config::validate`; fastcdc panics on
/// out-of-bounds sizes.
///
/// Read errors become `Error::Io` with an empty path; callers that know the
/// path should re-wrap it.
pub fn chunk_reader<R: Read>(
    p: &Profile,
    r: R,
    mut on_chunk: impl FnMut(Chunk, &[u8]) -> Result<()>,
) -> Result<FileDigest> {
    debug_assert!(p.min <= p.avg && p.avg <= p.max, "profile not validated");
    let mut whole = blake3::Hasher::new();
    let mut size = 0u64;
    for item in StreamCDC::new(r, p.min as usize, p.avg as usize, p.max as usize) {
        let cd = match item {
            Ok(cd) => cd,
            Err(CdcError::IoError(e)) => return Err(Error::io(PathBuf::new(), e)),
            Err(e) => {
                return Err(Error::io(
                    PathBuf::new(),
                    std::io::Error::other(e.to_string()),
                ))
            }
        };
        whole.update(&cd.data);
        size += cd.data.len() as u64;
        let chunk = Chunk {
            offset: cd.offset,
            len: cd.data.len() as u32,
            hash: Hash32::of(&cd.data),
        };
        on_chunk(chunk, &cd.data)?;
    }
    Ok(FileDigest {
        hash: Hash32(*whole.finalize().as_bytes()),
        size,
    })
}
