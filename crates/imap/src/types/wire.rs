//! Modified UTF-7 encoding for IMAP mailbox names.
//!
//! RFC 3501 Section 5.1.3 defines a modified UTF-7 encoding for mailbox names.
//! Printable US-ASCII characters (0x20-0x7E) except `&` are sent as-is.
//! All other characters are encoded in modified Base64 (shifted with `&`...`-`).
//! The `&` character itself is represented as `&-`.
//!
//! The modified Base64 alphabet replaces `/` with `,` and omits padding `=`.
//!
//! When the server advertises `UTF8=ACCEPT` (RFC 6855) and the client has
//! enabled it, mailbox names are sent as raw UTF-8 instead.

use base64::Engine;
use base64::alphabet::Alphabet;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use std::sync::LazyLock;

/// Negotiated mailbox wire encoding. Conversion and canonicality live here so
/// display decoding never silently becomes a new server address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireMode {
    ModifiedUtf7,
    Utf8,
}

impl From<bool> for WireMode {
    fn from(utf8: bool) -> Self {
        if utf8 { Self::Utf8 } else { Self::ModifiedUtf7 }
    }
}

impl WireMode {
    pub(crate) fn encode(self, name: &str) -> String {
        // Only the exact INBOX token is case-insensitive. An Outlook user
        // selecting an Inbox child expects the server's exact LIST address.
        if name.eq_ignore_ascii_case("INBOX") {
            return "INBOX".to_owned();
        }
        self.encode_component(name)
    }

    pub(crate) fn encode_component(self, name: &str) -> String {
        match self {
            Self::Utf8 => name.to_owned(),
            Self::ModifiedUtf7 => encode_utf7(name),
        }
    }

    pub(crate) fn decode(self, bytes: &[u8]) -> (String, bool) {
        let display = match self {
            Self::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
            Self::ModifiedUtf7 => decode_utf7(bytes),
        };
        // INBOX casing is canonical identity, but must not fold a malformed
        // shifted spelling of INBOX into the reserved mailbox.
        let canonical = bytes == self.encode(&display).as_bytes()
            || (display.eq_ignore_ascii_case("INBOX") && bytes == display.as_bytes());
        (display, canonical)
    }
}

/// Modified Base64 alphabet string for IMAP UTF-7 (RFC 3501 Section 5.1.3).
///
/// Standard Base64 with `/` replaced by `,`.
const IMAP_B64_ALPHABET_STR: &str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+,";

/// Modified Base64 engine for IMAP UTF-7 (RFC 3501 Section 5.1.3).
///
/// The alphabet is standard Base64 with `/` replaced by `,`, and no padding.
///
/// The decoder is configured to:
/// - Accept non-zero trailing bits (RFC 3501 Section 5.1.3 does not require
///   zero-padding of unused bits, and non-conformant encoders may set them).
/// - Tolerate padding characters (`=`) from non-conformant servers, even
///   though the spec says padding is omitted (Postel's law).
///
/// Constructed once via `LazyLock` to avoid rebuilding the engine on every
/// encode/decode call.
#[allow(clippy::expect_used)] // Invariant: hardcoded 64-char alphabet literal cannot fail.
static IMAP_B64_ENGINE: LazyLock<GeneralPurpose> = LazyLock::new(|| {
    // The alphabet literal is a compile-time constant with exactly 64 valid
    // Base64 characters  -  `Alphabet::new` cannot fail here.
    let alphabet = Alphabet::new(IMAP_B64_ALPHABET_STR)
        .expect("IMAP modified Base64 alphabet is a valid 64-char constant");
    // RFC 3501 Section 5.1.3: no padding on encode, but tolerate padding
    // and non-zero trailing bits on decode for robustness.
    let config = GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent)
        .with_decode_allow_trailing_bits(true);
    GeneralPurpose::new(&alphabet, config)
});

/// Encode a UTF-8 mailbox name into modified UTF-7 per RFC 3501 Section 5.1.3.
///
/// Printable US-ASCII (0x20..=0x7E) except `&` passes through unchanged.
/// `&` is encoded as `&-`. Non-ASCII runs are converted to UTF-16BE,
/// then encoded with the modified Base64 alphabet and wrapped in `&`...`-`.
pub(crate) fn encode_utf7(input: &str) -> String {
    let engine = &*IMAP_B64_ENGINE;

    let mut out = String::with_capacity(input.len());
    let mut utf16_buf: Vec<u8> = Vec::new();

    for ch in input.chars() {
        if ch == '&' {
            // Flush any pending non-ASCII run first.
            flush_utf16(engine, &mut utf16_buf, &mut out);
            // RFC 3501 Section 5.1.3: `&` is encoded as `&-`.
            out.push_str("&-");
        } else if ch.is_ascii() && (0x20..=0x7E).contains(&(ch as u32)) {
            // Printable US-ASCII  -  send as-is.
            flush_utf16(engine, &mut utf16_buf, &mut out);
            out.push(ch);
        } else {
            // Non-ASCII or control character  -  accumulate UTF-16BE bytes.
            let mut u16_buf = [0u16; 2];
            let encoded = ch.encode_utf16(&mut u16_buf);
            for code_unit in encoded.iter() {
                utf16_buf.extend_from_slice(&code_unit.to_be_bytes());
            }
        }
    }

    // Flush any trailing non-ASCII run.
    flush_utf16(engine, &mut utf16_buf, &mut out);

    out
}

/// Flush accumulated UTF-16BE bytes as a modified Base64 `&`...`-` segment
/// (RFC 3501 Section 5.1.3).
fn flush_utf16(engine: &GeneralPurpose, utf16_buf: &mut Vec<u8>, out: &mut String) {
    if utf16_buf.is_empty() {
        return;
    }
    out.push('&');
    out.push_str(&engine.encode(&utf16_buf));
    out.push('-');
    utf16_buf.clear();
}

/// Decode an IMAP modified UTF-7 mailbox name into a UTF-8 string
/// per RFC 3501 Section 5.1.3.
///
/// Returns the decoded string, or the input lossily converted if decoding fails.
pub(crate) fn decode_utf7(input: &[u8]) -> String {
    let engine = &*IMAP_B64_ENGINE;

    let mut out = String::with_capacity(input.len());
    let mut i = 0;

    while i < input.len() {
        if input[i] == b'&' {
            i += 1;
            if i < input.len() && input[i] == b'-' {
                // `&-` -> literal `&`
                out.push('&');
                i += 1;
            } else {
                // Find the closing `-`.
                let start = i;
                while i < input.len() && input[i] != b'-' {
                    i += 1;
                }
                // Decode the modified Base64 segment.
                let b64_slice = &input[start..i];
                // RFC 3501 Section 5.1.3 requires every shifted segment to
                // end in `-`. An unterminated segment is not a mailbox name
                // the server can validly produce, so preserve its raw bytes
                // rather than decoding a prefix and inventing a different
                // mailbox identity.
                if i >= input.len() {
                    out.push('&');
                    out.push_str(&String::from_utf8_lossy(b64_slice));
                    continue;
                }
                if i < input.len() {
                    i += 1; // skip the `-`
                }
                if let Ok(utf16_bytes) = engine.decode(b64_slice) {
                    // RFC 3501 Section 5.1.3: "Modified BASE64 MUST NOT be used to
                    // represent any printing US-ASCII character which can represent
                    // itself." This is a requirement on the *encoder*, not the
                    // decoder. Per Postel's law ("be liberal in what you accept"),
                    // we decode the segment regardless of whether it contains
                    // printable ASCII. Non-conformant servers (e.g. some Exchange
                    // versions) do encode printable ASCII in Base64 segments, and
                    // rejecting them corrupts mailbox names.
                    decode_utf16be(&utf16_bytes, &mut out);
                } else {
                    // Malformed  -  emit the raw bytes lossily.
                    out.push('&');
                    out.push_str(&String::from_utf8_lossy(b64_slice));
                    out.push('-');
                }
            }
        } else if input[i] >= 0x80 {
            // Non-ASCII byte outside a Base64 segment.
            // RFC 3501 Section5.1.3: bytes 0x80-0xFF should be Base64-encoded,
            // but non-conformant servers (e.g. Exchange) send raw UTF-8.
            // Try to decode as UTF-8 per Postel's law.
            let start = i;
            // Gather the full run of high bytes (potential multi-byte UTF-8).
            while i < input.len() && input[i] >= 0x80 {
                i += 1;
            }
            let raw = &input[start..i];
            match std::str::from_utf8(raw) {
                Ok(s) => out.push_str(s),
                Err(_) => {
                    // Not valid UTF-8  -  fall back to lossy conversion.
                    out.push_str(&String::from_utf8_lossy(raw));
                }
            }
        } else if input[i] >= 0x20 && input[i] <= 0x7E {
            // Printable ASCII (0x20-0x7E)  -  pass through (RFC 3501 Section 5.1.3).
            out.push(input[i] as char);
            i += 1;
        } else {
            // Control characters (0x00-0x1F, 0x7F) are not printable US-ASCII
            // per RFC 3501 Section 5.1.3. Replace with U+FFFD.
            out.push('\u{FFFD}');
            i += 1;
        }
    }

    out
}

/// Decode UTF-16BE byte pairs into UTF-8 characters appended to `out`
/// (RFC 3501 Section 5.1.3).
fn decode_utf16be(bytes: &[u8], out: &mut String) {
    // Track whether there's a trailing odd byte.
    let has_trailing = !bytes.len().is_multiple_of(2);

    // Each code unit is 2 bytes big-endian.
    for result in char::decode_utf16(bytes.chunks(2).filter_map(|chunk| {
        if chunk.len() == 2 {
            Some(u16::from_be_bytes([chunk[0], chunk[1]]))
        } else {
            None
        }
    })) {
        match result {
            Ok(ch) => out.push(ch),
            Err(_) => out.push('\u{FFFD}'),
        }
    }

    // RFC 3501 Section 5.1.3: UTF-16BE requires byte pairs.
    // An orphaned trailing byte indicates corruption  -  emit U+FFFD.
    if has_trailing {
        out.push('\u{FFFD}');
    }
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;

/// Controls how the encoder produces literal markers.
///
/// RFC 3501 Section 4.3 defines synchronizing literals (`{N}\r\n`) which
/// require the client to wait for a `+` continuation response. RFC 7888
/// introduces two extensions that allow non-synchronizing literals (`{N+}\r\n`):
///
/// - **LITERAL+** (RFC 7888 Section 4): non-synchronizing literals of any size.
/// - **LITERAL-** (RFC 7888 Section 5): non-synchronizing literals up to 4096
///   bytes; larger literals MUST use synchronizing form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralMode {
    /// No literal extension  -  all literals use synchronizing form `{N}\r\n`
    /// (RFC 3501 Section 4.3).
    Synchronizing,
    /// LITERAL+ (RFC 7888 Section 4)  -  non-synchronizing `{N+}\r\n` for all sizes.
    LiteralPlus,
    /// LITERAL- (RFC 7888 Section 5)  -  non-synchronizing `{N+}\r\n` only when
    /// the literal is <= 4096 bytes; larger literals use synchronizing `{N}\r\n`.
    LiteralMinus,
}
