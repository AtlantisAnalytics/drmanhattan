use anyhow::Result;
use serde::Serialize;

/// Print `value` as JSON when `json` is set, otherwise as its human `Display` form.
pub fn emit<T: Serialize + std::fmt::Display>(json: bool, value: &T) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(value)?);
    } else {
        println!("{value}");
    }
    Ok(())
}

/// The one renderer for every user-visible path, name and link target, in
/// human and JSON output alike (core's error messages use it too): printable
/// UTF-8 as is, `\` as `\\`, and each control or invalid byte as `\xHH`.
pub use drmanhattan_core::display::{display_bytes, display_path};

#[cfg(test)]
mod tests {
    use super::display_bytes;

    #[test]
    fn display_bytes_escapes() {
        assert_eq!(display_bytes(b"a\xffb"), "a\\xffb");
    }

    #[test]
    fn controls_and_backslash_are_escaped() {
        assert_eq!(display_bytes(b"a\x1b[2Jb\nfake"), "a\\x1b[2Jb\\x0afake");
        assert_eq!(display_bytes(b"\\xff"), "\\\\xff");
    }

    #[test]
    fn invalid_bytes_are_escaped() {
        assert_eq!(display_bytes(b"a/\xffb\xc3\xa9"), "a/\\xffb\u{e9}");
    }
}
