use drmanhattan_core::compress::{decode, encode, Compression};
use drmanhattan_core::error::Error;
use drmanhattan_core::sniff::{detect, profile_kind, Format, ProfileKind};
use rand_chacha::ChaCha8Rng;
use rand_core::{Rng, SeedableRng};

const ALL: [Format; 24] = [
    Format::Jpeg,
    Format::Png,
    Format::Webp,
    Format::Gif,
    Format::Avif,
    Format::Heic,
    Format::Mp4,
    Format::Webm,
    Format::Zip,
    Format::Gzip,
    Format::Bzip2,
    Format::Xz,
    Format::Zstd,
    Format::SevenZ,
    Format::Html,
    Format::Css,
    Format::Js,
    Format::Json,
    Format::Xml,
    Format::Svg,
    Format::Txt,
    Format::Csv,
    Format::Md,
    Format::Other,
];

#[test]
fn jpeg_by_magic_ignores_extension() {
    assert_eq!(detect(b"x.html", &[0xFF, 0xD8, 0xFF, 0xE0]), Format::Jpeg);
}

#[test]
fn html_by_extension() {
    assert_eq!(detect(b"a/index.HTM", b"<!doctype html>"), Format::Html);
}

#[test]
fn unknown_is_other() {
    assert_eq!(detect(b"blob.bin", b"\x01\x02\x03\x04"), Format::Other);
    assert_eq!(detect(b"noext", b""), Format::Other);
}

#[test]
fn extension_table() {
    for (n, f) in [
        ("a.css", Format::Css),
        ("a.js", Format::Js),
        ("a.mjs", Format::Js),
        ("a.json", Format::Json),
        ("a.xml", Format::Xml),
        ("a.svg", Format::Svg),
        ("a.txt", Format::Txt),
        ("a.csv", Format::Csv),
        ("a.md", Format::Md),
        ("a.html", Format::Html),
    ] {
        assert_eq!(detect(n.as_bytes(), b"plain"), f, "{n}");
    }
}

#[test]
fn magic_table() {
    assert_eq!(detect(b"a", b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR"), Format::Png);
    assert_eq!(detect(b"a", b"GIF89a\x01\0\x01\0"), Format::Gif);
    assert_eq!(
        detect(b"a", &[0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 3]),
        Format::Gzip
    );
    assert_eq!(detect(b"a", b"PK\x03\x04\x14\0\0\0\0\0"), Format::Zip);
    assert_eq!(detect(b"a", &[0x28, 0xB5, 0x2F, 0xFD, 0, 0]), Format::Zstd);
    assert_eq!(
        detect(b"a", &[0xFD, b'7', b'z', b'X', b'Z', 0, 0]),
        Format::Xz
    );
    assert_eq!(detect(b"a", b"BZh91AY&SY"), Format::Bzip2);
    assert_eq!(
        detect(b"a", &[b'7', b'z', 0xBC, 0xAF, 0x27, 0x1C, 0, 4]),
        Format::SevenZ
    );
}

#[test]
fn format_str_roundtrip() {
    for f in ALL {
        assert_eq!(Format::from_str(f.as_str()), Some(f), "{}", f.as_str());
    }
    assert_eq!(Format::SevenZ.as_str(), "seven_z");
    assert_eq!(Format::from_str("nope"), None);
}

#[test]
fn classification_sets() {
    let pre: Vec<Format> = ALL
        .iter()
        .copied()
        .filter(|f| f.is_precompressed())
        .collect();
    assert_eq!(pre.len(), 14);
    assert!(pre.iter().all(|f| !f.is_text()));
    let text: Vec<Format> = ALL.iter().copied().filter(|f| f.is_text()).collect();
    assert_eq!(text.len(), 9);
    assert!(!Format::Other.is_text() && !Format::Other.is_precompressed());
}

#[test]
fn text_profile_cutoff() {
    assert_eq!(profile_kind(Format::Html, 4194304), ProfileKind::Text);
    assert_eq!(profile_kind(Format::Html, 4194305), ProfileKind::Binary);
    assert_eq!(profile_kind(Format::Jpeg, 10), ProfileKind::Binary);
    assert_eq!(ProfileKind::Text.as_str(), "text");
    assert_eq!(ProfileKind::Binary.as_str(), "binary");
}

#[test]
fn compression_u8() {
    assert_eq!(Compression::from_u8(0), Some(Compression::None));
    assert_eq!(Compression::from_u8(1), Some(Compression::Zstd));
    assert_eq!(Compression::from_u8(2), None);
    assert_eq!(Compression::None.as_str(), "none");
    assert_eq!(Compression::Zstd.as_str(), "zstd");
}

#[test]
fn precompressed_skips_zstd() {
    let data = b"<p>hello</p>".repeat(1000);
    let (c, out) = encode(&data, true, 3).unwrap();
    assert_eq!(c, Compression::None);
    assert_eq!(&*out, &data[..]);
}

#[test]
fn incompressible_stays_raw() {
    let mut data = vec![0u8; 64 * 1024];
    ChaCha8Rng::seed_from_u64(7).fill_bytes(&mut data);
    let (c, out) = encode(&data, false, 3).unwrap();
    assert_eq!(c, Compression::None);
    assert_eq!(&*out, &data[..]);
}

#[test]
fn compressible_uses_zstd() {
    let data = b"<div class=\"x\">hello</div>\n".repeat(64 * 1024 / 27);
    let (c, out) = encode(&data, false, 3).unwrap();
    assert_eq!(c, Compression::Zstd);
    assert!(out.len() < data.len());
    let back = decode(c, &out, data.len() as u64).unwrap();
    assert_eq!(back, data);
}

#[test]
fn decode_wrong_len_errors() {
    let data = b"<div>hello</div>\n".repeat(4000);
    let (c, out) = encode(&data, false, 3).unwrap();
    assert_eq!(c, Compression::Zstd);
    let r = decode(c, &out, data.len() as u64 + 1);
    assert!(matches!(r, Err(Error::Corrupt(_))));
    let r = decode(Compression::None, b"abc", 4);
    assert!(matches!(r, Err(Error::Corrupt(_))));
    assert_eq!(decode(Compression::None, b"abc", 3).unwrap(), b"abc");
}

#[test]
fn decode_garbage_zstd_errors() {
    let r = decode(Compression::Zstd, b"not zstd at all", 10);
    assert!(matches!(r, Err(Error::Corrupt(_))));
}
