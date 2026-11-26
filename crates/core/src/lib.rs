pub mod catalog;
pub mod chunk;
pub mod compress;
pub mod config;
pub mod diff;
pub mod display;
pub mod error;
pub mod ids;
pub mod ingest;
pub mod manifest;
pub mod model;
pub mod pack;
pub mod recover;
pub mod repo;
pub mod restore;
pub(crate) mod snaptree;
pub mod sniff;
pub mod unitread;
pub mod verify;
pub mod walk;
pub(crate) mod xattrs;

/// Crash-test kill point action: end the process at once, with no
/// destructors, atexit handlers or stdio flush. `_exit` rather than `abort`
/// so macOS raises no crash report; the effect on the repository is the
/// same. Used as `fail::fail_point!("<name>", |_| kill())`.
#[cfg(feature = "failpoints")]
pub(crate) fn kill() -> ! {
    // SAFETY: `_exit` takes no pointers and does not return.
    unsafe { libc::_exit(137) }
}
