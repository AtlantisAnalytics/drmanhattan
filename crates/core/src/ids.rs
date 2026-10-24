use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// BLAKE3-256 digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Hash32(pub [u8; 32]);

impl Hash32 {
    pub fn of(data: &[u8]) -> Hash32 {
        Hash32(*blake3::hash(data).as_bytes())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> Result<Hash32> {
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out)
            .map_err(|e| Error::Corrupt(format!("bad hash hex {s:?}: {e}")))?;
        Ok(Hash32(out))
    }
}

impl fmt::Display for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// UUIDv7 identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Id(pub [u8; 16]);

impl Id {
    pub fn new_v7() -> Id {
        Id(*uuid::Uuid::now_v7().as_bytes())
    }

    pub fn from_bytes(b: [u8; 16]) -> Id {
        Id(b)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn parse(s: &str) -> Result<Id> {
        uuid::Uuid::parse_str(s)
            .map(|u| Id(*u.as_bytes()))
            .map_err(|e| Error::Corrupt(format!("bad id {s:?}: {e}")))
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", uuid::Uuid::from_bytes(self.0).hyphenated())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_hex_roundtrip() {
        let h = Hash32::of(b"abc");
        assert_eq!(Hash32::from_hex(&h.to_hex()).unwrap(), h);
        assert_eq!(h.to_string().len(), 64);
        assert!(Hash32::from_hex("zz").is_err());
    }

    #[test]
    fn id_roundtrip() {
        let id = Id::new_v7();
        let s = id.to_string();
        assert_eq!(s.len(), 36);
        assert_eq!(Id::parse(&s).unwrap(), id);
        assert_eq!(Id::from_bytes(*id.as_bytes()), id);
    }
}
