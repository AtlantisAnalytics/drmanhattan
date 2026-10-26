use crate::display::{display_path, display_text};
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

/// Smallest allowed `packs.max_size`: 1 MiB.
pub const MIN_PACK_SIZE: u64 = 1 << 20;
/// Largest allowed `packs.max_size`: 4 GiB.
pub const MAX_PACK_SIZE: u64 = 4 << 30;

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
        let cfg: Config = toml::from_str(&text).map_err(|e| {
            // The parser's report quotes the offending line verbatim.
            Error::Config(format!(
                "{}: {}",
                display_path(path),
                display_text(&e.to_string())
            ))
        })?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Check that the config is usable: supported format version, chunk
    /// profiles within fastcdc's bounds (so chunking can never panic), zstd
    /// at a level zstd accepts, and a pack size from `MIN_PACK_SIZE` to
    /// `MAX_PACK_SIZE`.
    pub fn validate(&self) -> Result<()> {
        if self.format_version != 1 {
            return Err(Error::Config(format!(
                "unsupported format_version {}",
                self.format_version
            )));
        }
        validate_profile("text", &self.chunking.text)?;
        validate_profile("binary", &self.chunking.binary)?;
        if self.compression.algo != "zstd" {
            return Err(Error::Config(format!(
                "compression.algo = {:?} is not supported (only \"zstd\")",
                self.compression.algo
            )));
        }
        let levels = zstd::compression_level_range();
        if !levels.contains(&self.compression.level) {
            return Err(Error::Config(format!(
                "compression.level = {} out of range {}..={}",
                self.compression.level,
                levels.start(),
                levels.end()
            )));
        }
        let size = self.packs.max_size;
        if !(MIN_PACK_SIZE..=MAX_PACK_SIZE).contains(&size) {
            return Err(Error::Config(format!(
                "packs.max_size = {size} out of range {MIN_PACK_SIZE}..={MAX_PACK_SIZE}"
            )));
        }
        Ok(())
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let text = toml::to_string(self).map_err(|e| Error::Config(e.to_string()))?;
        std::fs::write(path, text).map_err(|e| Error::io(path, e))
    }
}

fn validate_profile(name: &str, p: &Profile) -> Result<()> {
    use fastcdc::v2020::{
        AVERAGE_MAX, AVERAGE_MIN, MAXIMUM_MAX, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN,
    };
    let fields = [
        ("min", p.min, MINIMUM_MIN, MINIMUM_MAX),
        ("avg", p.avg, AVERAGE_MIN, AVERAGE_MAX),
        ("max", p.max, MAXIMUM_MIN, MAXIMUM_MAX),
    ];
    for (field, v, lo, hi) in fields {
        let v = v as usize;
        if v < lo || v > hi {
            return Err(Error::Config(format!(
                "chunking.{name}.{field} = {v} out of range {lo}..={hi}"
            )));
        }
        if !v.is_multiple_of(2) {
            return Err(Error::Config(format!(
                "chunking.{name}.{field} = {v} must be even"
            )));
        }
    }
    if !(p.min <= p.avg && p.avg <= p.max) {
        return Err(Error::Config(format!(
            "chunking.{name}: require min <= avg <= max"
        )));
    }
    Ok(())
}
