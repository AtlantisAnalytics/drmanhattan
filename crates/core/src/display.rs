//! The one renderer for raw bytes shown to a person: paths, names and link
//! targets in CLI output (human and JSON) and in error messages.
//!
//! Valid, printable UTF-8 passes through unchanged. Everything a terminal
//! could act on or that would be ambiguous is escaped: `\` becomes `\\`,
//! and each byte of a C0 control (U+0000 to U+001F), DEL (U+007F), a C1
//! control (U+0080 to U+009F, two bytes in UTF-8) or invalid UTF-8 becomes
//! `\xHH` (lowercase hex). The rendering is lossless: a literal `\xff` in
//! a name reads `\\xff`, the byte 0xFF reads `\xff`.

use std::fmt::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

fn hex(out: &mut String, bytes: &[u8]) {
    for b in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "\\x{b:02x}");
    }
}

/// Render raw bytes for display (see the module doc).
pub fn display_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for chunk in bytes.utf8_chunks() {
        for c in chunk.valid().chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '\u{0}'..='\u{1f}' | '\u{7f}'..='\u{9f}' => {
                    let mut buf = [0u8; 4];
                    hex(&mut out, c.encode_utf8(&mut buf).as_bytes());
                }
                c => out.push(c),
            }
        }
        hex(&mut out, chunk.invalid());
    }
    out
}

/// `display_bytes` of each line of `text`, keeping the `\n` between lines:
/// for multi-line messages that quote untrusted input, such as a parser's
/// report on a config file.
pub fn display_text(text: &str) -> String {
    text.split('\n')
        .map(|line| display_bytes(line.as_bytes()))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `display_bytes` of a path's raw bytes.
pub fn display_path(path: &Path) -> String {
    display_bytes(path.as_os_str().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn printable_utf8_is_unchanged() {
        assert_eq!(
            display_bytes("a/b c/\u{e9}\u{1f600}".as_bytes()),
            "a/b c/\u{e9}\u{1f600}"
        );
    }

    #[test]
    fn invalid_bytes_are_escaped() {
        assert_eq!(display_bytes(b"a\xffb"), "a\\xffb");
        assert_eq!(display_bytes(b"a/\xffb\xc3\xa9"), "a/\\xffb\u{e9}");
    }

    #[test]
    fn controls_are_escaped() {
        assert_eq!(display_bytes(b"a\x1b[2Jb\nfake"), "a\\x1b[2Jb\\x0afake");
        assert_eq!(display_bytes(b"\x00\x1f\x7f"), "\\x00\\x1f\\x7f");
        // C1 controls (here U+0085 and U+009B) are escaped byte by byte.
        assert_eq!(
            display_bytes("x\u{85}y\u{9b}z".as_bytes()),
            "x\\xc2\\x85y\\xc2\\x9bz"
        );
        // U+00A0 is the first code point after the C1 range.
        assert_eq!(display_bytes("\u{a0}".as_bytes()), "\u{a0}");
    }

    #[test]
    fn backslash_is_doubled_so_rendering_is_unambiguous() {
        assert_eq!(display_bytes(b"\\xff"), "\\\\xff");
        assert_ne!(display_bytes(b"\\xff"), display_bytes(b"\xff"));
    }

    #[test]
    fn text_keeps_line_breaks_only() {
        assert_eq!(display_text("a\n\x1b[2Jb\r\n"), "a\n\\x1b[2Jb\\x0d\n");
        assert_eq!(display_text(""), "");
    }

    #[test]
    fn paths_render_the_same() {
        use std::ffi::OsStr;
        let p = Path::new(OsStr::from_bytes(b"/tmp/a\nb\xff"));
        assert_eq!(display_path(p), "/tmp/a\\x0ab\\xff");
    }
}
