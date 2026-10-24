use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Profile {
    pub min: u32,
    pub avg: u32,
    pub max: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunking {
    pub text: Profile,
    pub binary: Profile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionCfg {
    pub algo: String,
    pub level: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PacksCfg {
    pub max_size: u64,
}

/// Contents of `wmk.toml`; fixed at `wmk init`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub format_version: u32,
    pub chunking: Chunking,
    pub compression: CompressionCfg,
    pub packs: PacksCfg,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            format_version: 1,
            chunking: Chunking {
                text: Profile {
                    min: 2048,
                    avg: 8192,
                    max: 65536,
                },
                binary: Profile {
                    min: 262_144,
                    avg: 1_048_576,
                    max: 4_194_304,
                },
            },
            compression: CompressionCfg {
                algo: "zstd".to_string(),
                level: 3,
            },
            packs: PacksCfg {
                max_size: 268_435_456,
            },
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let text = std::fs::read_to_string(path).map_err(|e| Error::io(path, e))?;
        toml::from_str(&text).map_err(|e| Error::Config(format!("{}: {e}", path.display())))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let text = toml::to_string(self).map_err(|e| Error::Config(e.to_string()))?;
        std::fs::write(path, text).map_err(|e| Error::io(path, e))
    }
}
