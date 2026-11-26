//! Step 1 of `build`: salvage every pack on a pool of threads, verify each
//! candidate record's payload against its hash, and index the intact units
//! on the caller's connection.
//!
//! Workers send rows over a bounded channel; the caller is the only writer.
//! Per pack, a placeholder `pack` row goes first (units reference it), then
//! its units, then the final `pack` row. Memory per worker is one pack's
//! candidate list and one unit.

use crate::catalog::writer::{apply, CatalogOp};
use crate::catalog::Catalog;
use crate::display::display_path;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::{PackRow, UnitRow};
use crate::pack::{coverage_gaps, pack_path, salvage, PackReader, UnitKind, RECORD_HEADER_LEN};
use crossbeam_channel::{Receiver, Sender};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Length of a pack's file header: where its data region starts.
const PACK_HEADER_LEN: u64 = 26;

/// Rows applied per transaction.
const BATCH: u64 = 10_000;

/// A pack file at the location its id names (`pack_path(packs_dir, id)`).
pub(super) struct PackFile {
    pub path: PathBuf,
    pub id: Id,
}

/// What `scan` found.
pub(super) struct Scanned {
    /// Packs given a `pack` row.
    pub packs: u64,
    /// Sum of those packs' file lengths: salvage reads every byte of each.
    pub bytes_read: u64,
    /// `unit` rows inserted.
    pub units: u64,
    /// `snapshot_record` units found only corrupt (hash, why), each once:
    /// their snapshots are lost, and nothing else would say so.
    pub lost_records: Vec<(Hash32, String)>,
}

enum Msg {
    /// First message of a pack: a placeholder row its units can reference.
    Pack(PackRow),
    Unit(UnitRow),
    /// A candidate record whose payload does not match its hash.
    Corrupt(Hash32, UnitKind, String),
    Warning(String),
    /// Last message of a pack: its final row and file length.
    Done(PackRow, u64),
}

/// Every pack file under `packs_dir`, in path order. Anything else there
/// (stray files, misplaced or misnamed packs, whose units could not be
/// located by id) is reported in `warnings` and ignored.
pub(super) fn list_packs(packs_dir: &Path, warnings: &mut Vec<String>) -> Result<Vec<PackFile>> {
    let mut out = Vec::new();
    for shard in sorted_dir(packs_dir)? {
        if !is_dir(&shard)? {
            warnings.push(format!(
                "{}: not a pack file; ignored",
                display_path(&shard)
            ));
            continue;
        }
        for path in sorted_dir(&shard)? {
            let id = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".pack"))
                .and_then(|stem| Id::parse(stem).ok())
                .filter(|id| pack_path(packs_dir, *id) == path);
            match id {
                Some(id) if !is_dir(&path)? => out.push(PackFile { path, id }),
                _ => warnings.push(format!("{}: not a pack file; ignored", display_path(&path))),
            }
        }
    }
    Ok(out)
}

fn sorted_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(Error::io(dir, e)),
    };
    let mut v = rd
        .map(|e| e.map(|e| e.path()).map_err(|e| Error::io(dir, e)))
        .collect::<Result<Vec<_>>>()?;
    v.sort();
    Ok(v)
}

/// Does not follow symlinks: a symlinked directory is not a shard.
fn is_dir(path: &Path) -> Result<bool> {
    Ok(std::fs::symlink_metadata(path)
        .map_err(|e| Error::io(path, e))?
        .is_dir())
}

/// Salvage `packs` on `workers` threads and insert their `pack` and `unit`
/// rows into `cat`, keeping the first intact copy of each unit. Every
/// structural problem, uncovered (damaged) byte range and corrupt record is
/// appended to `warnings`, sorted.
pub(super) fn scan(
    cat: &Catalog,
    packs: &[PackFile],
    workers: usize,
    warnings: &mut Vec<String>,
) -> Result<Scanned> {
    let next = AtomicUsize::new(0);
    let abort = AtomicBool::new(false);
    let mut corrupt = Vec::new();
    let (written, worker_results) = std::thread::scope(|s| {
        let (tx, rx) = crossbeam_channel::bounded::<Msg>(1024);
        let handles: Vec<_> = (0..workers.min(packs.len()))
            .map(|_| {
                let tx = tx.clone();
                let (next, abort) = (&next, &abort);
                s.spawn(move || -> Result<()> {
                    while !abort.load(Ordering::Relaxed) {
                        let Some(p) = packs.get(next.fetch_add(1, Ordering::Relaxed)) else {
                            break;
                        };
                        if let Err(e) = scan_pack(p, &tx) {
                            abort.store(true, Ordering::Relaxed);
                            return Err(e);
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        drop(tx);
        // Returning drops `rx`, so workers blocked on a send stop too.
        let written = write(cat, rx, warnings, &mut corrupt);
        if written.is_err() {
            abort.store(true, Ordering::Relaxed);
        }
        let results: Vec<Result<()>> = handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
            .collect();
        (written, results)
    });
    // A worker fails on its own (I/O) or because the writer stopped; the
    // writer's error is then the cause.
    let mut scanned = written?;
    for r in worker_results {
        r?;
    }
    // Whether an intact copy exists is known only once every pack is in.
    for (hash, kind, why) in corrupt {
        if cat.has_unit(&hash)? {
            warnings.push(format!("{why}; an intact copy is used"));
            continue;
        }
        warnings.push(format!("{why}; no intact copy was found"));
        if kind == UnitKind::SnapshotRecord && !scanned.lost_records.iter().any(|(h, _)| *h == hash)
        {
            scanned.lost_records.push((hash, why));
        }
    }
    scanned.lost_records.sort();
    warnings.sort();
    Ok(scanned)
}

/// Apply the workers' rows in batched transactions until every sender is
/// gone, collecting their warnings and corrupt records.
fn write(
    cat: &Catalog,
    rx: Receiver<Msg>,
    warnings: &mut Vec<String>,
    corrupt: &mut Vec<(Hash32, UnitKind, String)>,
) -> Result<Scanned> {
    let conn = &cat.conn;
    let mut out = Scanned {
        packs: 0,
        bytes_read: 0,
        units: 0,
        lost_records: Vec::new(),
    };
    let mut pending = 0u64;
    conn.execute_batch("BEGIN")?;
    let r = (|| -> Result<()> {
        for msg in rx {
            match msg {
                Msg::Pack(row) => apply(conn, CatalogOp::UpsertPack(row))?,
                Msg::Unit(row) => {
                    apply(conn, CatalogOp::InsertUnit(row))?;
                    out.units += conn.changes();
                }
                Msg::Corrupt(hash, kind, why) => corrupt.push((hash, kind, why)),
                Msg::Warning(w) => warnings.push(w),
                Msg::Done(row, len) => {
                    apply(conn, CatalogOp::UpsertPack(row))?;
                    out.packs += 1;
                    out.bytes_read += len;
                }
            }
            pending += 1;
            if pending >= BATCH {
                conn.execute_batch("COMMIT; BEGIN")?;
                pending = 0;
            }
        }
        Ok(conn.execute_batch("COMMIT")?)
    })();
    if r.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    r?;
    Ok(out)
}

fn send(tx: &Sender<Msg>, m: Msg) -> Result<()> {
    tx.send(m)
        .map_err(|_| Error::Corrupt("recover: catalog writer stopped".into()))
}

fn to_i64(v: u64, what: &str) -> Result<i64> {
    i64::try_from(v).map_err(|_| Error::Corrupt(format!("{what} {v} out of range")))
}

/// Salvage one pack and send its rows. Errs only on I/O errors (or when the
/// writer has stopped).
fn scan_pack(p: &PackFile, tx: &Sender<Msg>) -> Result<()> {
    let shown = display_path(&p.path);
    let s = salvage(&p.path)?;
    for w in s.problems {
        send(tx, Msg::Warning(w))?;
    }
    if s.id.is_none() {
        // Header unreadable: the file is skipped (salvage said so).
        return Ok(());
    }
    let len = std::fs::metadata(&p.path)
        .map_err(|e| Error::io(&p.path, e))?
        .len();
    let placeholder = PackRow {
        id: p.id,
        size: to_i64(PACK_HEADER_LEN, "pack size")?,
        sealed: false,
        blake3: None,
    };
    send(tx, Msg::Pack(placeholder))?;

    let reader = PackReader::open(&p.path)?;
    let mut verified: Vec<(u64, u64)> = Vec::new();
    let mut end = PACK_HEADER_LEN;
    for (offset, h) in &s.records {
        match reader.read_unit(*offset, &h.unit_hash) {
            Ok(_) => {
                let span = RECORD_HEADER_LEN as u64 + h.stored_len;
                verified.push((*offset, span));
                end = end.max(offset + span);
                let row = UnitRow {
                    hash: h.unit_hash,
                    kind: h.kind,
                    raw_size: to_i64(h.raw_len, "unit raw size")?,
                    stored_size: to_i64(h.stored_len, "unit stored size")?,
                    compression: h.compression,
                    pack_id: p.id,
                    pack_offset: to_i64(*offset, "pack offset")?,
                };
                send(tx, Msg::Unit(row))?;
            }
            Err(Error::Corrupt(why)) => send(tx, Msg::Corrupt(h.unit_hash, h.kind, why))?,
            Err(e) => return Err(e),
        }
    }

    // Behind a consistent footer every uncovered range is damage, even when
    // the footer's BLAKE3 fails (that is reported separately). Without one,
    // a final uncovered range is a torn tail: a crash leftover, not damage.
    let has_footer = s.data_end < len;
    let gaps = coverage_gaps(&verified, PACK_HEADER_LEN, s.data_end, has_footer);
    for (from, to) in gaps.damaged {
        send(
            tx,
            Msg::Warning(format!(
                "pack {shown}: bytes {from}..{to} hold no intact record"
            )),
        )?;
    }
    let row = match s.sealed_blake3 {
        Some(b) => PackRow {
            id: p.id,
            size: to_i64(len, "pack size")?,
            sealed: true,
            blake3: Some(b),
        },
        None => PackRow {
            id: p.id,
            size: to_i64(end, "pack size")?,
            sealed: false,
            blake3: None,
        },
    };
    send(tx, Msg::Done(row, len))
}
