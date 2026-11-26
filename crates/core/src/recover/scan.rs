//! Step 1 of `build`: salvage every pack on a pool of threads, verify each
//! candidate record's payload against its hash, and index the intact units
//! on the caller's connection.
//!
//! Workers send rows over a bounded channel; the caller is the only writer.
//! Per pack, a placeholder `pack` row goes first (units reference it), then
//! its units, then the final `pack` row. Memory per worker is one pack's
//! candidate list and footer index, and one unit.
//!
//! Damaged bytes (`coverage_gaps`) that a header-valid candidate covers are
//! judged by that candidate (a corrupt unit: a warning, a damaged file or a
//! lost record). Damaged bytes no candidate covers could have held anything,
//! a snapshot record included, so they are unaccounted (fatal to
//! `RecoverReport::intact`) unless a consistent footer's index shows that
//! every unit it places there has an intact copy.

use super::{TornTail, UnaccountedRange};
use crate::catalog::writer::{apply, CatalogOp};
use crate::catalog::Catalog;
use crate::display::display_path;
use crate::error::{Error, Result};
use crate::ids::{Hash32, Id};
use crate::model::{PackRow, UnitRow};
use crate::pack::{coverage_gaps, pack_path, salvage, PackReader, UnitKind, RECORD_HEADER_LEN};
use crossbeam_channel::{Receiver, Sender};
use std::collections::HashMap;
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
    /// Damaged bytes that may have held data, by pack path and offset.
    pub unaccounted: Vec<UnaccountedRange>,
    /// Unfinished writes ignored at the end of unsealed packs, likewise.
    pub torn_tails: Vec<TornTail>,
}

enum Msg {
    /// First message of a pack: a placeholder row its units can reference.
    Pack(PackRow),
    Unit(UnitRow),
    /// A candidate record whose payload does not match its hash.
    Corrupt(Hash32, UnitKind, String),
    /// Damaged bytes no candidate covers, with the unit hashes a consistent
    /// footer's index places inside them (`None` without such a footer).
    Unaccounted(UnaccountedRange, Option<Vec<Hash32>>),
    TornTail(TornTail),
    Warning(String),
    /// Last message of a pack: its final row and file length.
    Done(PackRow, u64),
}

/// What can be judged only once every pack's units are in.
#[derive(Default)]
struct Deferred {
    corrupt: Vec<(Hash32, UnitKind, String)>,
    unaccounted: Vec<(UnaccountedRange, Option<Vec<Hash32>>)>,
}

/// Every pack file under `packs_dir`, in path order. Files not named like
/// packs are reported in `warnings` and ignored. A file named like a pack
/// but not at `pack_path` of its id (wrong shard, directly in `packs/`, a
/// non-canonical spelling) is an error: its units could not be read by id,
/// so a snapshot record in it would vanish, and recover never moves files.
pub(super) fn list_packs(packs_dir: &Path, warnings: &mut Vec<String>) -> Result<Vec<PackFile>> {
    let mut out = Vec::new();
    for shard in sorted_dir(packs_dir)? {
        if is_dir(&shard)? {
            for path in sorted_dir(&shard)? {
                classify(packs_dir, path, &mut out, warnings)?;
            }
        } else {
            classify(packs_dir, shard, &mut out, warnings)?;
        }
    }
    Ok(out)
}

fn classify(
    packs_dir: &Path,
    path: PathBuf,
    out: &mut Vec<PackFile>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let id = path
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix(".pack"))
        .and_then(|stem| Id::parse(stem).ok());
    match id {
        Some(id) if !is_dir(&path)? => {
            let home = pack_path(packs_dir, id);
            if home != path {
                return Err(Error::Corrupt(format!(
                    "pack file {} is not where its name places it; move it to {} and run \
                     recover again",
                    display_path(&path),
                    display_path(&home)
                )));
            }
            out.push(PackFile { path, id });
        }
        _ => warnings.push(format!("{}: not a pack file; ignored", display_path(&path))),
    }
    Ok(())
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

/// Follows symlinks, as ingest and the unit reader do: a symlink to a
/// directory is a shard. A dangling symlink is not a directory.
fn is_dir(path: &Path) -> Result<bool> {
    match std::fs::metadata(path) {
        Ok(m) => Ok(m.is_dir()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(Error::io(path, e)),
    }
}

/// Salvage `packs` on `workers` threads and insert their `pack` and `unit`
/// rows into `cat`, keeping the first intact copy of each unit. Every
/// structural problem, cost-free damaged byte range and corrupt record is
/// appended to `warnings`, sorted.
///
/// `old_sizes` holds the `pack.size` of each pack in the previous catalog
/// (empty when there is none): an unsealed pack's bytes below that size
/// were committed, so they cannot be a torn tail.
pub(super) fn scan(
    cat: &Catalog,
    packs: &[PackFile],
    workers: usize,
    old_sizes: &HashMap<Id, u64>,
    warnings: &mut Vec<String>,
) -> Result<Scanned> {
    let next = AtomicUsize::new(0);
    let abort = AtomicBool::new(false);
    let mut deferred = Deferred::default();
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
                        if let Err(e) = scan_pack(p, old_sizes.get(&p.id).copied(), &tx) {
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
        let written = write(cat, rx, warnings, &mut deferred);
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
    for (range, index) in deferred.unaccounted {
        let mut spare = index.as_ref().is_some_and(|h| !h.is_empty());
        for h in index.iter().flatten() {
            if !cat.has_unit(h)? {
                spare = false;
                break;
            }
        }
        if spare {
            warnings.push(format!(
                "pack {}: bytes {}..{} hold no readable record, but every unit its footer \
                 index places there has an intact copy",
                display_path(&range.pack),
                range.start,
                range.end
            ));
        } else {
            scanned.unaccounted.push(range);
        }
    }
    for (hash, kind, why) in deferred.corrupt {
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
    scanned
        .unaccounted
        .sort_by(|a, b| (&a.pack, a.start).cmp(&(&b.pack, b.start)));
    scanned
        .torn_tails
        .sort_by(|a, b| (&a.pack, a.start).cmp(&(&b.pack, b.start)));
    warnings.sort();
    Ok(scanned)
}

/// Apply the workers' rows in batched transactions until every sender is
/// gone, collecting their warnings, torn tails and what must be judged later.
fn write(
    cat: &Catalog,
    rx: Receiver<Msg>,
    warnings: &mut Vec<String>,
    deferred: &mut Deferred,
) -> Result<Scanned> {
    let conn = &cat.conn;
    let mut out = Scanned {
        packs: 0,
        bytes_read: 0,
        units: 0,
        lost_records: Vec::new(),
        unaccounted: Vec::new(),
        torn_tails: Vec::new(),
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
                Msg::Corrupt(hash, kind, why) => deferred.corrupt.push((hash, kind, why)),
                Msg::Unaccounted(range, index) => deferred.unaccounted.push((range, index)),
                Msg::TornTail(t) => out.torn_tails.push(t),
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

/// The parts of `[start, end)` that no span in `spans` (`(start, end)`,
/// sorted by start, possibly overlapping) covers.
fn uncovered((start, end): (u64, u64), spans: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut cursor = start;
    for &(s, e) in spans {
        if s >= end {
            break;
        }
        if e <= cursor {
            continue;
        }
        if s > cursor {
            out.push((cursor, s));
        }
        cursor = e;
    }
    if cursor < end {
        out.push((cursor, end));
    }
    out
}

/// Salvage one pack and send its rows. `old_size` is its size in the
/// previous catalog, if known. Errs only on I/O errors (or when the writer
/// has stopped).
fn scan_pack(p: &PackFile, old_size: Option<u64>, tx: &Sender<Msg>) -> Result<()> {
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
    // `(start, end)` of candidates whose payload failed its hash, in order.
    let mut failed: Vec<(u64, u64)> = Vec::new();
    // End of the verified spans so far. Genuine records never overlap and
    // candidates come in offset order, so a candidate starting inside a
    // verified record is a header embedded in its payload (a pack file
    // stored uncompressed, say): never a unit of this pack, intact or not.
    let mut covered_end = PACK_HEADER_LEN;
    for (offset, h) in &s.records {
        if *offset < covered_end {
            continue;
        }
        match reader.read_unit(*offset, &h.unit_hash) {
            Ok(_) => {
                let span = RECORD_HEADER_LEN as u64 + h.stored_len;
                verified.push((*offset, span));
                covered_end = offset + span;
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
            Err(Error::Corrupt(why)) => {
                failed.push((*offset, offset + RECORD_HEADER_LEN as u64 + h.stored_len));
                send(tx, Msg::Corrupt(h.unit_hash, h.kind, why))?
            }
            Err(e) => return Err(e),
        }
    }

    // Behind a consistent footer every uncovered range is damage, even when
    // the footer's BLAKE3 fails (that is reported separately). Without one,
    // a final uncovered range is a torn tail: a crash leftover, not damage.
    let has_footer = s.data_end < len;
    let gaps = coverage_gaps(&verified, PACK_HEADER_LEN, s.data_end, has_footer);
    let range = |start, end| UnaccountedRange {
        pack: p.path.clone(),
        pack_id: p.id,
        start,
        end,
    };
    for (from, to) in gaps.damaged {
        let pieces = uncovered((from, to), &failed);
        if pieces.is_empty() {
            // Each failed candidate here is reported on its own.
            send(
                tx,
                Msg::Warning(format!(
                    "pack {shown}: bytes {from}..{to} hold no intact record"
                )),
            )?;
        }
        for (a, b) in pieces {
            let index = has_footer.then(|| {
                s.index
                    .iter()
                    .filter(|(off, _)| (a..b).contains(off))
                    .map(|(_, h)| *h)
                    .collect()
            });
            send(tx, Msg::Unaccounted(range(a, b), index))?;
        }
    }
    if !has_footer {
        // Bytes below the size the previous catalog recorded were committed:
        // missing or unreadable there, they are damage, not an unfinished
        // write (a truncated file included).
        let tail = gaps.torn_tail.unwrap_or(s.data_end);
        let committed = old_size.unwrap_or(0).max(tail);
        for (a, b) in uncovered((tail, committed), &failed) {
            send(tx, Msg::Unaccounted(range(a, b), None))?;
        }
        if committed < s.data_end {
            send(
                tx,
                Msg::TornTail(TornTail {
                    pack: p.path.clone(),
                    pack_id: p.id,
                    start: committed,
                    end: s.data_end,
                }),
            )?;
        }
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
            size: to_i64(covered_end, "pack size")?,
            sealed: false,
            blake3: None,
        },
    };
    send(tx, Msg::Done(row, len))
}

#[cfg(test)]
mod tests {
    use super::uncovered;

    #[test]
    fn uncovered_subtracts_overlapping_spans() {
        assert_eq!(uncovered((10, 100), &[]), vec![(10, 100)]);
        assert_eq!(
            uncovered((10, 100), &[(0, 20), (15, 30), (40, 50), (90, 200)]),
            vec![(30, 40), (50, 90)]
        );
        assert_eq!(uncovered((10, 100), &[(5, 100)]), vec![]);
        assert_eq!(uncovered((50, 50), &[]), vec![]);
        assert_eq!(uncovered((50, 40), &[]), vec![]);
    }
}
