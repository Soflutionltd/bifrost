use super::*;

// ===== Task 7: decode_q_encoding and hex_digit tests (RFC 2047 Section 4.2) =====

/// Simple hex-encoded byte: =E9 -> 0xE9 (RFC 2047 Section 4.2).
#[test]
fn q_encoding_simple_hex() {
    let result = decode_q_encoding("=E9");
    assert_eq!(result, vec![0xE9]);
}

/// Underscore maps to space in Q-encoding (RFC 2047 Section 4.2).
#[test]
fn q_encoding_underscore_to_space() {
    let result = decode_q_encoding("Hello_World");
    assert_eq!(result, b"Hello World");
}

/// Mixed literal and encoded characters (RFC 2047 Section 4.2).
#[test]
fn q_encoding_mixed_literal_and_encoded() {
    let result = decode_q_encoding("caf=E9");
    assert_eq!(result, b"caf\xE9");
}

/// Consecutive encoded bytes (RFC 2047 Section 4.2).
#[test]
fn q_encoding_consecutive_encoded() {
    let result = decode_q_encoding("=C3=A9");
    assert_eq!(result, vec![0xC3, 0xA9]);
}

/// Trailing incomplete =E  -  should emit '=' literally (RFC 2047 Section 4.2).
#[test]
fn q_encoding_trailing_incomplete() {
    let result = decode_q_encoding("test=E");
    // '=' is not followed by two hex digits, so it stays literal
    assert_eq!(result, b"test=E");
}

/// Invalid hex digits =GG  -  should emit '=' literally (RFC 2047 Section 4.2).
#[test]
fn q_encoding_invalid_hex_digits() {
    let result = decode_q_encoding("test=GG");
    assert_eq!(result, b"test=GG");
}

/// Empty input produces empty output (RFC 2047 Section 4.2).
#[test]
fn q_encoding_empty_input() {
    let result = decode_q_encoding("");
    assert!(result.is_empty());
}

/// All-encoded input (RFC 2047 Section 4.2).
#[test]
fn q_encoding_all_encoded() {
    let result = decode_q_encoding("=48=65=6C=6C=6F");
    assert_eq!(result, b"Hello");
}

/// Lowercase hex digits in Q-encoding (RFC 2047 Section 4.2).
#[test]
fn q_encoding_lowercase_hex() {
    let result = decode_q_encoding("=e9=c3");
    assert_eq!(result, vec![0xE9, 0xC3]);
}

/// Underscore and encoded mixed (RFC 2047 Section 4.2).
#[test]
fn q_encoding_underscore_and_encoded_mixed() {
    let result = decode_q_encoding("=E9l=E8ve_du_coll=E8ge");
    assert_eq!(
        result,
        vec![
            0xE9, b'l', 0xE8, b'v', b'e', b' ', b'd', b'u', b' ', b'c', b'o', b'l', b'l', 0xE8,
            b'g', b'e'
        ]
    );
}

/// `hex_digit` with valid digits (RFC 2047 Section 4.2).
#[test]
fn hex_digit_valid() {
    assert_eq!(hex_digit(b'0'), Some(0));
    assert_eq!(hex_digit(b'5'), Some(5));
    assert_eq!(hex_digit(b'9'), Some(9));
    assert_eq!(hex_digit(b'A'), Some(10));
    assert_eq!(hex_digit(b'F'), Some(15));
    assert_eq!(hex_digit(b'a'), Some(10));
    assert_eq!(hex_digit(b'f'), Some(15));
}

/// `hex_digit` with invalid characters (RFC 2047 Section 4.2).
#[test]
fn hex_digit_invalid() {
    assert_eq!(hex_digit(b'G'), None);
    assert_eq!(hex_digit(b'g'), None);
    assert_eq!(hex_digit(b'z'), None);
    assert_eq!(hex_digit(b' '), None);
    assert_eq!(hex_digit(b'!'), None);
    assert_eq!(hex_digit(b'\x00'), None);
}

/// `hex_digit` boundary values (RFC 2047 Section 4.2).
#[test]
fn hex_digit_boundaries() {
    // Just below '0'
    assert_eq!(hex_digit(b'/'), None);
    // Just above '9'
    assert_eq!(hex_digit(b':'), None);
    // Just below 'A'
    assert_eq!(hex_digit(b'@'), None);
    // Just above 'F'
    assert_eq!(hex_digit(b'G'), None);
    // Just below 'a'
    assert_eq!(hex_digit(b'`'), None);
    // Just above 'f'
    assert_eq!(hex_digit(b'g'), None);
}

// ===== Q-encoding soft line break stripping (Postel's law leniency) =====
// RFC 2047 Section 4.2 Q-encoding does NOT define soft line breaks.
// Soft line breaks are a Quoted-Printable concept from RFC 2045 Section 6.7.
// We strip them as leniency for non-conformant encoders.

/// `=\r\n` stripped as Postel's-law leniency (not in RFC 2047 Section 4.2;
/// borrowed from RFC 2045 Section 6.7 Quoted-Printable).
#[test]
fn q_encoding_soft_line_break_crlf() {
    let result = decode_q_encoding("Hel=\r\nlo");
    assert_eq!(result, b"Hello");
}

/// `=\n` (bare LF) stripped as Postel's-law leniency (not in RFC 2047
/// Section 4.2; borrowed from RFC 2045 Section 6.7 Quoted-Printable).
#[test]
fn q_encoding_soft_line_break_lf() {
    let result = decode_q_encoding("Hel=\nlo");
    assert_eq!(result, b"Hello");
}

/// `=\r\n` at end of input stripped as Postel's-law leniency.
#[test]
fn q_encoding_soft_line_break_at_end() {
    let result = decode_q_encoding("Hello=\r\n");
    assert_eq!(result, b"Hello");
}

/// `=\r\n` mixed with hex-encoded bytes, stripped as Postel's-law leniency.
#[test]
fn q_encoding_soft_break_with_hex() {
    let result = decode_q_encoding("caf=\r\n=E9");
    assert_eq!(result, b"caf\xE9");
}

/// RFC 2047 Section 4.2 Q-encoding defines only three transformations:
/// `=XX` hex-encoded bytes, `_` as space, and printable ASCII pass-through.
/// It does NOT define soft line breaks (`=\r\n`). Soft line breaks are a
/// Quoted-Printable concept from RFC 2045 Section 6.7. Since encoded-words
/// cannot contain CR/LF (RFC 2047 Section 2), `=\r\n` inside one is already
/// malformed. We strip it as a Postel's-law leniency for non-conformant
/// encoders  -  this test documents that intentional behavior.
#[test]
fn spec_audit_q_encoding_soft_line_break_is_postel_leniency() {
    let result = decode_q_encoding("Hello=\r\nWorld");
    assert_eq!(result, b"HelloWorld");
}
