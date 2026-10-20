//! METADATA command encoders (RFC 5464).

use super::super::validate_metadata_entry_name;
use super::CommandWriter;
use crate::codec::encode::LiteralForm;

/// Encode GETMETADATA command (RFC 5464 Section 4.2).
///
/// Single entry: `GETMETADATA [options] "<mailbox>" <entry>`
/// Multiple entries: `GETMETADATA [options] "<mailbox>" (<entry1> <entry2> ...)`
///
/// Returns an error if `entries` is empty or if `depth` is not a valid value.
pub(in crate::codec::encode) fn encode_getmetadata(
    w: &mut CommandWriter,
    tag: &str,
    mailbox: impl AsRef<[u8]>,
    entries: &[String],
    max_size: Option<u64>,
    depth: Option<&str>,
    utf8: bool,
) -> Result<(), crate::Error> {
    // RFC 5464 Section 5: `maxsize-opt = "MAXSIZE" SP number`
    // RFC 3501 Section 9: `number = 1*DIGIT`, unsigned 32-bit integer.
    if let Some(n) = max_size
        && n > u64::from(u32::MAX)
    {
        return Err(crate::Error::InvalidInput(format!(
            "GETMETADATA MAXSIZE must fit in number (u32) per RFC 5464 Section 5 / RFC 3501 Section 9, got {n}"
        )));
    }

    // RFC 5464 Section 4.2 ABNF: `entries = entry / "(" entry *(SP entry) ")"`.
    if entries.is_empty() {
        return Err(crate::Error::InvalidInput(
            "GETMETADATA requires at least one entry (RFC 5464 Section 4.2)".into(),
        ));
    }

    for entry in entries {
        validate_metadata_entry_name(entry, "GETMETADATA entry name")?;
    }

    // RFC 5464 Section 4.2.2: `scope-opt = "DEPTH" SP ("0" / "1" / "infinity")`
    if let Some(d) = depth
        && d != "0"
        && d != "1"
        && d != "infinity"
    {
        return Err(crate::Error::InvalidInput(format!(
            "GETMETADATA DEPTH must be \"0\", \"1\", or \"infinity\" \
             (RFC 5464 Section 4.2.2), got: {d:?}"
        )));
    }

    w.raw(tag.as_bytes());
    w.raw(b" GETMETADATA");

    // RFC 5464 Section 5 ABNF:
    // getmetadata = "GETMETADATA" [SP getmetadata-options] SP mailbox SP entries
    // Verified errata 2785 / 2786 correct the examples in Sections 4.2.1-4.2.2.
    if max_size.is_some() || depth.is_some() {
        w.raw(b" (");
        let first_opt = if let Some(n) = max_size {
            w.raw(b"MAXSIZE ");
            w.raw(n.to_string().as_bytes());
            false
        } else {
            true
        };
        if let Some(d) = depth {
            if !first_opt {
                w.raw(b" ");
            }
            w.raw(b"DEPTH ");
            w.raw(d.as_bytes());
        }
        w.raw(b")");
    }

    w.raw(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    w.string(mailbox.as_ref(), utf8);

    w.raw(b" ");
    if entries.len() == 1 {
        // Single entry uses no parentheses per RFC 5464 Section 4.2.
        w.string(entries[0].as_bytes(), utf8);
    } else {
        w.raw(b"(");
        for (i, entry) in entries.iter().enumerate() {
            if i > 0 {
                w.raw(b" ");
            }
            w.string(entry.as_bytes(), utf8);
        }
        w.raw(b")");
    }
    w.raw(b"\r\n");
    Ok(())
}

/// Encode SETMETADATA command (RFC 5464 Section 4.3).
///
/// Format: `SETMETADATA <mailbox> (<entry> <value> ...)`.
/// A `None` value is encoded as `NIL` to delete the entry.
/// RFC 5464 Section 5: `value = nstring / literal8`; values are raw bytes.
pub(in crate::codec::encode) fn encode_setmetadata(
    w: &mut CommandWriter,
    tag: &str,
    mailbox: impl AsRef<[u8]>,
    entries: &[(String, Option<Vec<u8>>)],
    utf8: bool,
) -> Result<(), crate::Error> {
    // RFC 5464 Section 5 ABNF: `entry-values = "(" entry *(SP entry) ")"`.
    if entries.is_empty() {
        return Err(crate::Error::InvalidInput(
            "SETMETADATA requires at least one entry (RFC 5464 Section 5)".into(),
        ));
    }

    for (name, _) in entries {
        validate_metadata_entry_name(name, "SETMETADATA entry name")?;
    }

    w.raw(tag.as_bytes());
    w.raw(b" SETMETADATA ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    w.string(mailbox.as_ref(), utf8);
    w.raw(b" (");
    for (i, (name, value)) in entries.iter().enumerate() {
        if i > 0 {
            w.raw(b" ");
        }
        w.string(name.as_bytes(), utf8);
        w.raw(b" ");
        match value {
            // RFC 5464 Section 5: value = nstring / literal8. `nstring`
            // includes classic literals via `string`, so only NUL-bearing data
            // requires literal8.
            Some(v) => encode_metadata_value(w, v),
            None => w.raw(b"NIL"),
        }
    }
    w.raw(b")\r\n");
    Ok(())
}

/// Encode a METADATA value as quoted string, classic literal, or literal8.
///
/// RFC 5464 Section 5 defines `value = nstring / literal8`. `nstring`
/// expands to `string / nil` (RFC 3501 Section 9 / RFC 9051 Section 9), and
/// `string` includes classic IMAP literals carrying `*CHAR8`, where
/// `CHAR8 = %x01-ff`. As a result:
/// - printable ASCII can use quoted form;
/// - any non-NUL non-quotable octets can use classic literal form; and
/// - only NUL (`%x00`) requires literal8 (`*OCTET`).
///
/// The quoting judgement is ASCII-only even on a UTF-8 connection: a value is
/// an opaque octet string, not a name, so high bytes travel as a literal.
/// METADATA's own grammar admits literal8 values, so a NUL value needs no
/// BINARY advertisement; whether its marker may be non-synchronizing is still
/// the writer's literal8 rule.
pub(in crate::codec::encode) fn encode_metadata_value(w: &mut CommandWriter, data: &[u8]) {
    if data.contains(&0) {
        w.literal(data, LiteralForm::Literal8);
    } else {
        w.string(data, false);
    }
}
