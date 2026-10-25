//! Catalog row types. Enums here own the persisted strings; they convert to
//! and from SQL text at the catalog boundary.

use crate::compress::Compression;
use crate::ids::{Hash32, Id};
use crate::pack::UnitKind;
use crate::sniff::{Format, ProfileKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryKind {
    Dir = 0,
    File = 1,
    Symlink = 2,
}

impl EntryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryKind::Dir => "dir",
            EntryKind::File => "file",
            EntryKind::Symlink => "symlink",
        }
    }

    /// The §6.4 manifest kind byte.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn parse(s: &str) -> Option<EntryKind> {
        match s {
            "dir" => Some(EntryKind::Dir),
            "file" => Some(EntryKind::File),
            "symlink" => Some(EntryKind::Symlink),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    Ok,
    Unstable,
    ScreenedOut,
    ScreenError,
    Purged,
}

impl EntryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryStatus::Ok => "ok",
            EntryStatus::Unstable => "unstable",
            EntryStatus::ScreenedOut => "screened_out",
            EntryStatus::ScreenError => "screen_error",
            EntryStatus::Purged => "purged",
        }
    }

    pub fn parse(s: &str) -> Option<EntryStatus> {
        match s {
            "ok" => Some(EntryStatus::Ok),
            "unstable" => Some(EntryStatus::Unstable),
            "screened_out" => Some(EntryStatus::ScreenedOut),
            "screen_error" => Some(EntryStatus::ScreenError),
            "purged" => Some(EntryStatus::Purged),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotStatus {
    InProgress,
    Complete,
    Failed,
}

impl SnapshotStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SnapshotStatus::InProgress => "in_progress",
            SnapshotStatus::Complete => "complete",
            SnapshotStatus::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<SnapshotStatus> {
        match s {
            "in_progress" => Some(SnapshotStatus::InProgress),
            "complete" => Some(SnapshotStatus::Complete),
            "failed" => Some(SnapshotStatus::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRow {
    pub id: Id,
    pub status: SnapshotStatus,
    pub source_root: Vec<u8>,
    pub host: String,
    pub tool_version: String,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub note: Option<String>,
    pub manifest_hash: Option<Hash32>,
    pub ext_manifest_hash: Option<Hash32>,
    pub source_manifest_hash: Option<Hash32>,
    pub record_unit: Option<Hash32>,
    pub entry_count: Option<i64>,
    pub file_bytes: Option<i64>,
    pub screened_out: i64,
    pub label_pattern: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryRow {
    pub id: Id,
    pub snapshot_id: Id,
    pub parent_id: Option<Id>,
    pub name: Vec<u8>,
    pub path: Vec<u8>,
    pub kind: EntryKind,
    pub mode: u32,
    pub mtime_ns: i64,
    pub size: Option<i64>,
    pub content_hash: Option<Hash32>,
    pub link_target: Option<Vec<u8>>,
    pub hardlink_group: Option<i64>,
    pub status: EntryStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentRow {
    pub hash: Hash32,
    pub size: i64,
    pub format: Option<Format>,
    pub profile: ProfileKind,
    pub unit_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitRow {
    pub hash: Hash32,
    pub kind: UnitKind,
    pub raw_size: i64,
    pub stored_size: i64,
    pub compression: Compression,
    pub pack_id: Id,
    pub pack_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackRow {
    pub id: Id,
    pub size: i64,
    pub sealed: bool,
    pub blake3: Option<Hash32>,
}
