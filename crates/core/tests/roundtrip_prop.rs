//! Round-trip property tests (spec §14.1): for any generated tree `D`,
//! `manifest_dir(D) == snapshot.manifest_hash == manifest_dir(restore(snapshot))`.
//!
//! `PROPTEST_CASES` overrides the default of 64 cases.

mod common;

use common::TreeBuilder;
use drmanhattan_core::catalog::Catalog;
use drmanhattan_core::diff::{self, DiffOptions};
use drmanhattan_core::ids::Hash32;
use drmanhattan_core::ingest::{self, IngestOptions};
use drmanhattan_core::manifest::{
    manifest_dir, manifest_snapshot, ManifestHashes, ManifestOptions,
};
use drmanhattan_core::repo::Repo;
use drmanhattan_core::restore::{self, preflight, RestoreOptions};
use proptest::prelude::*;
use proptest::sample::Index;
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::path::Path;
use std::sync::OnceLock;
use unicode_normalization::UnicodeNormalization;

/// Chunk-profile boundaries (text min/max, binary min/max, each ±1).
const SPECIAL_SIZES: [u64; 13] = [
    0, 1, 2047, 2048, 2049, 65535, 65536, 65537, 262143, 262144, 262145, 4194304, 4194305,
];
/// Sizes at or above this count against `MAX_LARGE`.
const LARGE: u64 = 4_194_304;
const MAX_LARGE: usize = 2;
const MAX_DEPTH: usize = 4;
const MAX_ENTRIES: usize = 40;
const MAX_LINKS: usize = 2;
const MTIME_MIN: i64 = -2_000_000_000_000_000_000;
const MTIME_MAX: i64 = 4_000_000_000_000_000_000;

// ---- the concrete tree a case builds ----

/// Bytes, shown as an escaped byte-string literal.
#[derive(Clone, PartialEq, Eq)]
struct B(Vec<u8>);

impl fmt::Debug for B {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "b\"{}\"", self.0.escape_ascii())
    }
}

#[derive(Debug, Clone, Copy)]
struct Content {
    /// HTML-like text (text chunk profile) rather than random bytes.
    text: bool,
    /// One repeated byte (after the HTML prefix for text), so FastCDC finds
    /// no boundary and must force a cut at the profile's max size.
    low_entropy: bool,
    /// Few seeds, so contents and prefixes repeat across files.
    seed: u8,
}

#[derive(Debug, Clone)]
enum Kind {
    File {
        size: u64,
        content: Content,
        mode: u32,
        mtime: i64,
    },
    Dir {
        mode: u32,
        mtime: i64,
    },
    Symlink {
        target: B,
    },
    /// A second name for the earlier file at `to`.
    HardLink {
        to: B,
    },
}

#[derive(Debug, Clone)]
struct Node {
    path: B,
    kind: Kind,
}

/// Nodes are in creation order: every parent dir and hard-link target
/// precedes its use.
#[derive(Debug, Clone)]
struct Plan {
    root_mode: u32,
    root_mtime: i64,
    nodes: Vec<Node>,
}

fn content_bytes(c: Content, size: u64) -> Vec<u8> {
    const WORDS: &[&[u8]] = &[
        b"<div class=\"row\">",
        b"</div>",
        b"<p>",
        b"</p>",
        b"<a href=\"/gear\">",
        b"</a>",
        b"escapement",
        b"mainspring",
        b"balance wheel",
        b"\n",
    ];
    let size = usize::try_from(size).unwrap();
    let mut rng = ChaCha8Rng::seed_from_u64(u64::from(c.seed));
    let mut out = Vec::with_capacity(size + 32);
    if c.low_entropy {
        // Zeros for binary seed 0; letters for text.
        let fill = if c.text { b'a' + c.seed } else { c.seed };
        if c.text {
            out.extend_from_slice(b"<!DOCTYPE html>\n<html><body>\n");
        }
        out.resize(size.max(out.len()), fill);
        out.truncate(size);
    } else if c.text {
        out.extend_from_slice(b"<!DOCTYPE html>\n<html><body>\n");
        while out.len() < size {
            out.extend_from_slice(WORDS[rng.next_u32() as usize % WORDS.len()]);
            out.push(b' ');
        }
        out.truncate(size);
    } else {
        out.resize(size, 0);
        rng.fill_bytes(&mut out);
    }
    out
}

fn build(root: &Path, plan: &Plan) {
    let mut t = TreeBuilder::new(root);
    for n in &plan.nodes {
        let p = n.path.0.as_slice();
        match &n.kind {
            Kind::File {
                size,
                content,
                mode,
                mtime,
            } => {
                t.file(p, &content_bytes(*content, *size))
                    .mode(p, *mode)
                    .mtime(p, *mtime);
            }
            Kind::Dir { mode, mtime } => {
                t.dir(p).mode(p, *mode).mtime(p, *mtime);
            }
            Kind::Symlink { target } => {
                t.symlink(p, &target.0);
            }
            Kind::HardLink { to } => {
                t.hardlink(p, &to.0);
            }
        }
    }
    t.mode(b"", plan.root_mode).mtime(b"", plan.root_mtime);
    t.finish();
}

// ---- the property ----

/// The temp filesystem's mtime granularity (R25: floor when coarser than 1 ns).
fn precision() -> u64 {
    static P: OnceLock<u64> = OnceLock::new();
    *P.get_or_init(|| {
        let tmp = tempfile::tempdir().unwrap();
        let p = preflight::probe(tmp.path()).unwrap().mtime_precision_ns;
        if cfg!(any(target_os = "macos", target_os = "linux")) {
            // APFS and ext4 keep nanoseconds: a coarser result is a probe
            // bug, which must not silently switch to floored comparisons.
            assert_eq!(p, 1, "probed mtime precision of the temp filesystem");
        }
        p
    })
}

fn mopts(p: u64) -> ManifestOptions {
    ManifestOptions {
        mtime_precision_ns: p,
        cross_fs: false,
    }
}

fn restore_to(repo: &Repo, snap: drmanhattan_core::ids::Id, dest: &Path, hardlinks: bool) {
    let opts = RestoreOptions {
        path: Vec::new(),
        hardlinks,
        xattrs: false,
    };
    restore::run(repo, snap, dest, &opts).unwrap();
}

/// Builds `plan`, then checks assertions (a)-(e) of the task brief.
fn check_roundtrip(plan: &Plan) -> Result<(), TestCaseError> {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    fs::create_dir(&src).unwrap();
    build(&src, plan);
    let repo = Repo::init(&tmp.path().join("r")).unwrap();
    let p = precision();
    let live = manifest_dir(&src, &mopts(p)).unwrap();

    let r = ingest::run(&repo, &src, &IngestOptions::default()).unwrap();
    let cat = Catalog::open_readonly(&repo.catalog_path()).unwrap();
    let snap = cat.snapshot(r.snapshot).unwrap();
    prop_assert_eq!(snap.manifest_hash, Some(r.manifest_hash));
    // The snapshot's hashes, mtimes floored to `p` when the filesystem is coarser.
    let want: (Hash32, Hash32) = if p == 1 {
        (r.manifest_hash, snap.ext_manifest_hash.unwrap())
    } else {
        let h: ManifestHashes = manifest_snapshot(&cat, r.snapshot, b"", &mopts(p)).unwrap();
        (h.core, h.ext)
    };

    // (a) ingest(D).manifest_hash == manifest_dir(D).core
    prop_assert_eq!(live.core, want.0, "(a) ingest vs live source");
    prop_assert_eq!(live.ext, want.1, "(a) ext: ingest vs live source");

    // (b) manifest_dir(restore(S)).core == manifest_hash
    let out = tmp.path().join("out");
    restore_to(&repo, r.snapshot, &out, false);
    let restored = manifest_dir(&out, &mopts(p)).unwrap();
    prop_assert_eq!(restored.core, want.0, "(b) restore vs snapshot");

    // (c) diff(S, restore(S)) is empty
    let d = diff::run(
        &repo,
        r.snapshot,
        &out,
        &DiffOptions {
            path: Vec::new(),
            mtime_precision_ns: p,
            cross_fs: false,
        },
    )
    .unwrap();
    prop_assert!(d.is_empty(), "(c) diff after restore: {:?}", d);

    // (d) re-ingest: zero new units, same manifest hash
    let r2 = ingest::run(&repo, &src, &IngestOptions::default()).unwrap();
    prop_assert_eq!(r2.new_units, 0, "(d) re-ingest stored new units");
    prop_assert_eq!(r2.manifest_hash, r.manifest_hash, "(d) re-ingest hash");

    // (e) restore with hard links reproduces the ext manifest
    let out_hl = tmp.path().join("out-hl");
    restore_to(&repo, r.snapshot, &out_hl, true);
    let hl = manifest_dir(&out_hl, &mopts(p)).unwrap();
    prop_assert_eq!(hl.core, want.0, "(e) core with hard links");
    prop_assert_eq!(hl.ext, want.1, "(e) ext with hard links");
    prop_assert_eq!(hl.ext, live.ext, "(e) ext vs live source");
    Ok(())
}

// ---- the generator ----

#[derive(Debug, Clone)]
enum KindSpec {
    File {
        size: u64,
        content: Content,
        mode: u32,
        mtime: i64,
    },
    Dir {
        mode: u32,
        mtime: i64,
    },
    Symlink {
        target: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
struct EntrySpec {
    /// Picks the parent among the directories that may still have children.
    parent: Index,
    name: Vec<u8>,
    kind: KindSpec,
}

#[derive(Debug, Clone)]
struct LinkSpec {
    file: Index,
    parent: Index,
    name: Vec<u8>,
}

#[derive(Debug, Clone)]
struct TreeSpec {
    root_mode: u32,
    root_mtime: i64,
    entries: Vec<EntrySpec>,
    links: Vec<LinkSpec>,
}

/// The key under which the host filesystem treats two sibling names as the
/// same (R25): NFD then lowercase on macOS (APFS), raw bytes on Linux.
fn collision_key(name: &[u8]) -> Vec<u8> {
    if !cfg!(target_os = "macos") {
        return name.to_vec();
    }
    match std::str::from_utf8(name) {
        Ok(s) => s.nfd().collect::<String>().to_lowercase().into_bytes(),
        Err(_) => name.to_vec(),
    }
}

fn join(parent: &[u8], name: &[u8]) -> Vec<u8> {
    if parent.is_empty() {
        return name.to_vec();
    }
    let mut p = parent.to_vec();
    p.push(b'/');
    p.extend_from_slice(name);
    p
}

/// Resolve a spec into a buildable tree: entries whose name collides with a
/// sibling are dropped, as are large files beyond `MAX_LARGE` (downsized).
fn plan(spec: &TreeSpec) -> Plan {
    // (path, depth) of every directory, the root first.
    let mut dirs: Vec<(Vec<u8>, usize)> = vec![(Vec::new(), 0)];
    let mut taken: HashSet<(Vec<u8>, Vec<u8>)> = HashSet::new();
    let mut files: Vec<Vec<u8>> = Vec::new();
    let mut large = 0;
    let mut nodes = Vec::new();

    let mut place = |dirs: &[(Vec<u8>, usize)], parent: Index, name: &[u8]| {
        let open: Vec<&(Vec<u8>, usize)> = dirs.iter().filter(|(_, d)| *d < MAX_DEPTH).collect();
        let (ppath, pdepth) = open[parent.index(open.len())];
        taken
            .insert((ppath.clone(), collision_key(name)))
            .then(|| (join(ppath, name), pdepth + 1))
    };

    for e in &spec.entries {
        let Some((path, depth)) = place(&dirs, e.parent, &e.name) else {
            continue;
        };
        let kind = match &e.kind {
            KindSpec::File {
                size,
                content,
                mode,
                mtime,
            } => {
                let mut size = *size;
                if size >= LARGE {
                    if large == MAX_LARGE {
                        size %= 300_000;
                    } else {
                        large += 1;
                    }
                }
                files.push(path.clone());
                Kind::File {
                    size,
                    content: *content,
                    mode: *mode,
                    mtime: *mtime,
                }
            }
            KindSpec::Dir { mode, mtime } => {
                dirs.push((path.clone(), depth));
                Kind::Dir {
                    mode: *mode,
                    mtime: *mtime,
                }
            }
            KindSpec::Symlink { target } => Kind::Symlink {
                target: B(target.clone()),
            },
        };
        nodes.push(Node {
            path: B(path),
            kind,
        });
    }

    for l in &spec.links {
        if files.is_empty() {
            break;
        }
        // Each file joins at most one pair.
        let to = files.remove(l.file.index(files.len()));
        let Some((path, _)) = place(&dirs, l.parent, &l.name) else {
            continue;
        };
        nodes.push(Node {
            path: B(path),
            kind: Kind::HardLink { to: B(to) },
        });
    }

    Plan {
        root_mode: spec.root_mode,
        root_mtime: spec.root_mtime,
        nodes,
    }
}

fn not_dots(mut b: Vec<u8>) -> Vec<u8> {
    if b == b"." || b == b".." {
        b.insert(0, b'x');
    }
    b
}

fn name() -> BoxedStrategy<Vec<u8>> {
    let ascii = prop_oneof![
        4 => "[a-zA-Z0-9._ -]{1,20}",
        // Short names, so case variants meet as siblings.
        1 => "[abAB.]{1,2}",
    ]
    .prop_map(|s| not_dots(s.into_bytes()));
    let accented = ("[a-c]{0,3}", any::<bool>(), "[a-c]{0,3}").prop_map(|(a, nfd, b)| {
        let e = if nfd { "e\u{301}" } else { "\u{e9}" };
        format!("{a}{e}{b}").into_bytes()
    });
    if cfg!(target_os = "linux") {
        let raw =
            prop::collection::vec(prop_oneof![0x80u8..=0xFF, Just(b'\n'), b'a'..=b'z'], 1..=20);
        prop_oneof![6 => ascii, 2 => accented, 2 => raw].boxed()
    } else {
        prop_oneof![6 => ascii, 2 => accented].boxed()
    }
}

/// Relative (often dangling), absolute, or climbing out of the tree.
fn link_target() -> impl Strategy<Value = Vec<u8>> {
    let comp = prop_oneof![
        4 => "[a-zA-Z0-9._ -]{1,12}".prop_map(String::into_bytes),
        1 => Just(b"..".to_vec()),
    ];
    (0u8..4, prop::collection::vec(comp, 1..=4)).prop_map(|(prefix, comps)| {
        let mut t = match prefix {
            0 => b"/".to_vec(),
            1 => b"../".to_vec(),
            _ => Vec::new(),
        };
        t.extend(comps.join(&b'/'));
        t
    })
}

fn mtime() -> impl Strategy<Value = i64> {
    prop_oneof![
        1 => Just(0i64),
        // Either side of the epoch, with nanoseconds.
        1 => -10_000_000_000i64..10_000_000_000,
        6 => MTIME_MIN..=MTIME_MAX,
    ]
}

fn file_mode() -> impl Strategy<Value = u32> {
    0o400u32..=0o777
}

/// Always `0o700` so the test can clean up; sticky optional (dirs only, R25).
fn dir_mode() -> impl Strategy<Value = u32> {
    (0o400u32..=0o777, any::<bool>())
        .prop_map(|(m, sticky)| m | 0o700 | if sticky { 0o1000 } else { 0 })
}

fn size() -> impl Strategy<Value = u64> {
    prop_oneof![
        1 => prop::sample::select(SPECIAL_SIZES.to_vec()),
        2 => 0u64..300_000,
    ]
}

fn content() -> impl Strategy<Value = Content> {
    (any::<bool>(), prop::bool::weighted(0.25), 0u8..4).prop_map(|(text, low_entropy, seed)| {
        Content {
            text,
            low_entropy,
            seed,
        }
    })
}

fn kind() -> impl Strategy<Value = KindSpec> {
    prop_oneof![
        5 => (size(), content(), file_mode(), mtime()).prop_map(|(size, content, mode, mtime)| {
            KindSpec::File { size, content, mode, mtime }
        }),
        3 => (dir_mode(), mtime()).prop_map(|(mode, mtime)| KindSpec::Dir { mode, mtime }),
        2 => link_target().prop_map(|target| KindSpec::Symlink { target }),
    ]
}

fn tree() -> impl Strategy<Value = TreeSpec> {
    let entry = (any::<Index>(), name(), kind()).prop_map(|(parent, name, kind)| EntrySpec {
        parent,
        name,
        kind,
    });
    let link = (any::<Index>(), any::<Index>(), name()).prop_map(|(file, parent, name)| LinkSpec {
        file,
        parent,
        name,
    });
    (
        dir_mode(),
        mtime(),
        prop::collection::vec(entry, 0..=MAX_ENTRIES - MAX_LINKS),
        prop::collection::vec(link, 0..=MAX_LINKS),
    )
        .prop_map(|(root_mode, root_mtime, entries, links)| TreeSpec {
            root_mode,
            root_mtime,
            entries,
            links,
        })
}

fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    ProptestConfig {
        cases,
        // Shrunk failures become named regression tests instead.
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn roundtrip_any_tree(spec in tree()) {
        let plan = plan(&spec);
        check_roundtrip(&plan).map_err(|e| TestCaseError::fail(format!("{e}\nplan: {plan:#?}")))?;
    }
}
