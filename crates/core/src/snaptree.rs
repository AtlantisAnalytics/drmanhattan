//! The one validated walk over a snapshot's catalog entries. Restore,
//! `manifest_snapshot` and diff read the entry tree only through
//! `traverse`, so a crafted catalog can neither name a path outside the
//! tree nor make a walk loop.
//!
//! Every entry's path is derived, not trusted: the root's is `""` and a
//! child's is its parent's path, then `/` (unless the parent is the root),
//! then its name (spec §6.4). The stored `path` must equal the derived one.

use std::collections::HashSet;

use crate::catalog::Catalog;
use crate::display::display_bytes;
use crate::error::{Error, Result};
use crate::ids::Id;
use crate::model::{EntryKind, EntryRow, EntryStatus};

/// One entry, as `traverse` yields it.
pub(crate) struct Node {
    pub row: EntryRow,
    /// The path relative to the traversal's start, which itself has `""`.
    pub rel: Vec<u8>,
    /// This entry and every ancestor up to the start are readable
    /// (`ok` or `unstable`).
    pub included: bool,
}

/// Whether an entry's bytes are readable (`ok` or `unstable`), as opposed
/// to withheld.
pub(crate) fn readable(row: &EntryRow) -> bool {
    matches!(row.status, EntryStatus::Ok | EntryStatus::Unstable)
}

/// `parent`, then `/` unless `parent` is the root's empty path, then `name`.
pub(crate) fn join(parent: &[u8], name: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(parent.len() + 1 + name.len());
    p.extend_from_slice(parent);
    if !parent.is_empty() {
        p.push(b'/');
    }
    p.extend_from_slice(name);
    p
}

fn corrupt(snapshot: Id, row: &EntryRow, why: &str) -> Error {
    Error::Corrupt(format!(
        "snapshot {snapshot}: entry {} at {}: {why}",
        row.id,
        display_bytes(&row.path)
    ))
}

/// A name that is one path component and nothing else.
fn check_name(snapshot: Id, row: &EntryRow) -> Result<()> {
    let n = &row.name[..];
    let why = if n.is_empty() {
        "has an empty name"
    } else if n == b"." || n == b".." {
        "has a name that is a dot component"
    } else if n.contains(&b'/') {
        "has a name containing '/'"
    } else if n.contains(&0) {
        "has a name containing NUL"
    } else {
        return Ok(());
    };
    Err(corrupt(snapshot, row, why))
}

/// `row` is a valid child of a directory whose stored path is `parent`.
fn check_child(snapshot: Id, parent: &[u8], row: &EntryRow) -> Result<()> {
    if row.snapshot_id != snapshot {
        return Err(corrupt(snapshot, row, "belongs to another snapshot"));
    }
    check_name(snapshot, row)?;
    if row.path != join(parent, &row.name) {
        return Err(corrupt(
            snapshot,
            row,
            &format!(
                "stored path disagrees with its parent's path {} and its name {}",
                display_bytes(parent),
                display_bytes(&row.name)
            ),
        ));
    }
    Ok(())
}

/// The snapshot's single root: no parent, empty name, empty path.
fn root(cat: &Catalog, snapshot: Id) -> Result<EntryRow> {
    let mut roots = cat.children(snapshot, None)?;
    match roots.len() {
        1 => {}
        0 => {
            cat.snapshot(snapshot)?; // NotFound for an unknown snapshot
            return Err(Error::Corrupt(format!(
                "snapshot {snapshot}: has no root entry"
            )));
        }
        n => {
            return Err(Error::Corrupt(format!(
                "snapshot {snapshot}: has {n} root entries"
            )))
        }
    }
    let root = roots.remove(0);
    if !root.name.is_empty() || !root.path.is_empty() {
        return Err(corrupt(
            snapshot,
            &root,
            "is the root but has a nonempty name or path",
        ));
    }
    Ok(root)
}

/// Walk from `start` up to the root, checking each step like a child.
fn check_ancestry(cat: &Catalog, snapshot: Id, start: &EntryRow, root: Id) -> Result<()> {
    let mut seen = HashSet::new();
    let mut cur = start.clone();
    loop {
        if !seen.insert(cur.id) {
            return Err(corrupt(snapshot, start, "has a cycle among its ancestors"));
        }
        let Some(pid) = cur.parent_id else {
            return if cur.id == root {
                Ok(())
            } else {
                Err(corrupt(snapshot, &cur, "has no parent but is not the root"))
            };
        };
        let parent = match cat.entry(pid) {
            Ok(p) => p,
            Err(Error::NotFound(_)) => return Err(corrupt(snapshot, &cur, "has a missing parent")),
            Err(e) => return Err(e),
        };
        if parent.kind != EntryKind::Dir {
            return Err(corrupt(
                snapshot,
                &cur,
                "has a parent that is not a directory",
            ));
        }
        check_child(snapshot, &parent.path, &cur)?;
        cur = parent;
    }
}

struct Frame {
    /// Children not yet visited, reversed so `pop` yields the first.
    pending: Vec<EntryRow>,
    /// The directory's stored (validated) path.
    path: Vec<u8>,
    rel: Vec<u8>,
    included: bool,
}

/// Visit `start_path` (the whole tree when empty) and every entry beneath
/// it in canonical pre-order (children sorted bytewise by name), withheld
/// subtrees included. Fails with `NotFound` if there is no entry at
/// `start_path`, and with `Corrupt`, before or during the visits, at the
/// first entry that breaks the rules in the module doc, a second root, or
/// a cycle.
pub(crate) fn traverse(
    cat: &Catalog,
    snapshot: Id,
    start_path: &[u8],
    mut visit: impl FnMut(Node) -> Result<()>,
) -> Result<()> {
    let root = root(cat, snapshot)?;
    let start = if start_path.is_empty() {
        root
    } else {
        let s = cat.entry_by_path(snapshot, start_path)?;
        check_ancestry(cat, snapshot, &s, root.id)?;
        s
    };
    let mut dirs: HashSet<Id> = HashSet::new();
    let mut stack = Vec::new();
    let included = readable(&start);
    if start.kind == EntryKind::Dir {
        dirs.insert(start.id);
        stack.push(Frame {
            pending: reversed(cat.children(snapshot, Some(start.id))?),
            path: start.path.clone(),
            rel: Vec::new(),
            included,
        });
    }
    visit(Node {
        row: start,
        rel: Vec::new(),
        included,
    })?;
    while let Some(top) = stack.last_mut() {
        let Some(row) = top.pending.pop() else {
            stack.pop();
            continue;
        };
        check_child(snapshot, &top.path, &row)?;
        let rel = join(&top.rel, &row.name);
        let included = top.included && readable(&row);
        if row.kind == EntryKind::Dir {
            if !dirs.insert(row.id) {
                return Err(corrupt(snapshot, &row, "is reached twice (a cycle)"));
            }
            stack.push(Frame {
                pending: reversed(cat.children(snapshot, Some(row.id))?),
                path: row.path.clone(),
                rel: rel.clone(),
                included,
            });
        }
        visit(Node { row, rel, included })?;
    }
    Ok(())
}

fn reversed(mut v: Vec<EntryRow>) -> Vec<EntryRow> {
    v.reverse();
    v
}
