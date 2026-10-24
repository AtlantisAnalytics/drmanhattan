//! Format sniffing and chunk-profile selection.
//!
//! The `as_str()` names are persisted in the catalog's `content.format`
//! column and are part of the on-disk contract.

/// Text files larger than this use the binary chunk profile.
pub const TEXT_PROFILE_MAX: u64 = 4_194_304;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    Jpeg,
    Png,
    Webp,
    Gif,
    Avif,
    Heic,
    Mp4,
    Webm,
    Zip,
    Gzip,
    Bzip2,
    Xz,
    Zstd,
    SevenZ,
    Html,
    Css,
    Js,
    Json,
    Xml,
    Svg,
    Txt,
    Csv,
    Md,
    Other,
}

impl Format {
    pub fn as_str(self) -> &'static str {
        match self {
            Format::Jpeg => "jpeg",
            Format::Png => "png",
            Format::Webp => "webp",
            Format::Gif => "gif",
            Format::Avif => "avif",
            Format::Heic => "heic",
            Format::Mp4 => "mp4",
            Format::Webm => "webm",
            Format::Zip => "zip",
            Format::Gzip => "gzip",
            Format::Bzip2 => "bzip2",
            Format::Xz => "xz",
            Format::Zstd => "zstd",
            Format::SevenZ => "seven_z",
            Format::Html => "html",
            Format::Css => "css",
            Format::Js => "js",
            Format::Json => "json",
            Format::Xml => "xml",
            Format::Svg => "svg",
            Format::Txt => "txt",
            Format::Csv => "csv",
            Format::Md => "md",
            Format::Other => "other",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Format> {
        Some(match s {
            "jpeg" => Format::Jpeg,
            "png" => Format::Png,
            "webp" => Format::Webp,
            "gif" => Format::Gif,
            "avif" => Format::Avif,
            "heic" => Format::Heic,
            "mp4" => Format::Mp4,
            "webm" => Format::Webm,
            "zip" => Format::Zip,
            "gzip" => Format::Gzip,
            "bzip2" => Format::Bzip2,
            "xz" => Format::Xz,
            "zstd" => Format::Zstd,
            "seven_z" => Format::SevenZ,
            "html" => Format::Html,
            "css" => Format::Css,
            "js" => Format::Js,
            "json" => Format::Json,
            "xml" => Format::Xml,
            "svg" => Format::Svg,
            "txt" => Format::Txt,
            "csv" => Format::Csv,
            "md" => Format::Md,
            "other" => Format::Other,
            _ => return None,
        })
    }

    pub fn is_text(self) -> bool {
        matches!(
            self,
            Format::Html
                | Format::Css
                | Format::Js
                | Format::Json
                | Format::Xml
                | Format::Svg
                | Format::Txt
                | Format::Csv
                | Format::Md
        )
    }

    /// Already-compressed formats; zstd is skipped for these (spec §7.6).
    pub fn is_precompressed(self) -> bool {
        matches!(
            self,
            Format::Jpeg
                | Format::Png
                | Format::Webp
                | Format::Gif
                | Format::Avif
                | Format::Heic
                | Format::Mp4
                | Format::Webm
                | Format::Zip
                | Format::Gzip
                | Format::Bzip2
                | Format::Xz
                | Format::Zstd
                | Format::SevenZ
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileKind {
    Text,
    Binary,
}

impl ProfileKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ProfileKind::Text => "text",
            ProfileKind::Binary => "binary",
        }
    }
}

pub fn profile_kind(f: Format, size: u64) -> ProfileKind {
    if f.is_text() && size <= TEXT_PROFILE_MAX {
        ProfileKind::Text
    } else {
        ProfileKind::Binary
    }
}

/// Map an `infer` MIME type to a `Format`.
///
/// | MIME                                   | Format  |
/// |----------------------------------------|---------|
/// | image/jpeg                             | Jpeg    |
/// | image/png                              | Png     |
/// | image/webp                             | Webp    |
/// | image/gif                              | Gif     |
/// | image/avif                             | Avif    |
/// | image/heic, image/heif                 | Heic    |
/// | video/mp4                              | Mp4     |
/// | video/webm                             | Webm    |
/// | application/zip                        | Zip     |
/// | application/gzip                       | Gzip    |
/// | application/x-bzip2                    | Bzip2   |
/// | application/x-xz                       | Xz      |
/// | application/zstd                       | Zstd    |
/// | application/x-7z-compressed            | SevenZ  |
/// | text/html                              | Html    |
/// | text/xml, application/xml              | Xml     |
///
/// Anything else `infer` recognizes is ignored and the extension decides.
fn from_mime(mime: &str) -> Option<Format> {
    Some(match mime {
        "image/jpeg" => Format::Jpeg,
        "image/png" => Format::Png,
        "image/webp" => Format::Webp,
        "image/gif" => Format::Gif,
        "image/avif" => Format::Avif,
        "image/heic" | "image/heif" => Format::Heic,
        "video/mp4" => Format::Mp4,
        "video/webm" => Format::Webm,
        "application/zip" => Format::Zip,
        "application/gzip" => Format::Gzip,
        "application/x-bzip2" => Format::Bzip2,
        "application/x-xz" => Format::Xz,
        "application/zstd" => Format::Zstd,
        "application/x-7z-compressed" => Format::SevenZ,
        "text/html" => Format::Html,
        "text/xml" | "application/xml" => Format::Xml,
        _ => return None,
    })
}

fn from_extension(name: &[u8]) -> Option<Format> {
    let dot = name.iter().rposition(|&b| b == b'.')?;
    let ext = name[dot + 1..].to_ascii_lowercase();
    Some(match ext.as_slice() {
        b"html" | b"htm" => Format::Html,
        b"css" => Format::Css,
        b"js" | b"mjs" => Format::Js,
        b"json" => Format::Json,
        b"xml" => Format::Xml,
        b"svg" => Format::Svg,
        b"txt" => Format::Txt,
        b"csv" => Format::Csv,
        b"md" => Format::Md,
        _ => return None,
    })
}

/// Detect a file's format. `head` is at most the first 8192 bytes.
/// Magic bytes win; the extension is the fallback.
pub fn detect(name: &[u8], head: &[u8]) -> Format {
    let head = &head[..head.len().min(8192)];
    infer::get(head)
        .and_then(|t| from_mime(t.mime_type()))
        .or_else(|| from_extension(name))
        .unwrap_or(Format::Other)
}
