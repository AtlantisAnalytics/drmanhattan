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

/// Render stored bytes for display: valid UTF-8 as is, each invalid byte as `\xHH`.
pub fn display_bytes(bytes: &[u8]) -> String {
    let mut out = String::new();
    for chunk in bytes.utf8_chunks() {
        out.push_str(chunk.valid());
        for b in chunk.invalid() {
            out.push_str(&format!("\\x{b:02x}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::display_bytes;

    #[test]
    fn display_bytes_escapes() {
        assert_eq!(display_bytes(b"a\xffb"), "a\\xffb");
    }

    #[test]
    fn invalid_bytes_are_escaped() {
        assert_eq!(display_bytes(b"a/\xffb\xc3\xa9"), "a/\\xffb\u{e9}");
    }
}
