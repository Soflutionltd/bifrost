//! IMAP command encoder.
//!
//! Serializes [`Command`](crate::types::Command) values into a
//! [`WireCommand`] ready for the wire. Each command is prefixed with a unique
//! tag (e.g. `A001`, `A002`, ...).
//!
//! Command syntax is defined in RFC 3501 Section 6 / RFC 9051 Section 6.
//! String encoding (quoted vs literal) follows RFC 3501 Section 9 / RFC 9051
//! Section 9, and is decided in one place, [`CommandWriter`].

mod commands;
mod core;
mod dispatch;

#[cfg(test)]
#[path = "tests.rs"]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;

use base64::Engine as _;

#[cfg(test)]
use crate::types::Command;
use crate::types::QresyncParams;

pub(crate) use commands::list_status_return_option_items;
#[cfg(test)]
use commands::{
    encode_getmetadata, encode_login, encode_metadata_value, encode_select_or_examine,
    encode_simple,
};
pub(crate) use core::{
    CommandWriter, EncodeOptions, LITERAL_MINUS_MAX, LiteralForm, LiteralMode, WireCommand,
};
pub(crate) use dispatch::encode_command;
#[cfg(test)]
use dispatch::encode_command_to_buf;

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Reject raw protocol parameters that contain CR or LF bytes.
///
/// IMAP commands are line-delimited by CRLF (RFC 3501 Section 2.2).
/// Embedding CR/LF in unquoted parameters would terminate the command
/// prematurely and allow injection of arbitrary commands.
fn validate_no_crlf(s: &str, context: &str) -> Result<(), crate::Error> {
    if s.bytes().any(|b| b == b'\r' || b == b'\n') {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must not contain CR or LF  -  IMAP commands are \
             CRLF-delimited (RFC 3501 Section 2.2)"
        )));
    }
    Ok(())
}

/// Validate SEARCH-family criteria while permitting IMAP literals that match
/// the negotiated literal mode.
///
/// RFC 3501 Section 6.4.4 defines many SEARCH keys in terms of `astring`, and
/// RFC 3501 Section 4.3 defines classic synchronizing literals as `{number}`
/// followed by CRLF and exactly `number` octets of data. RFC 7888 Section 3
/// adds the non-synchronizing `{number+}` form only when `LITERAL+` or
/// `LITERAL-` has been advertised. RFC 7888 Section 5 and RFC 9051 Section 4.3
/// limit non-synchronizing literals in `LITERAL-` / `IMAP4rev2` mode to 4096
/// octets. SORT and THREAD inherit the same search-criteria grammar
/// (RFC 5256 Section 5), so their criteria arguments need the same checks.
///
/// This validator therefore rejects raw CR/LF bytes except when they are part
/// of a syntactically valid IMAP literal marker and body, and it rejects
/// non-synchronizing literal markers that are not permitted by `literal_mode`.
fn validate_search_criteria_crlf(
    criteria: &str,
    context: &str,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    let bytes = criteria.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'{' {
            use crate::connection::literals::{LiteralMarker, literal_marker_at};
            if let LiteralMarker::Counted {
                data_start,
                size,
                synchronizing,
            } = literal_marker_at(bytes, i)
            {
                let size = usize::try_from(size).map_err(|_| {
                    crate::Error::InvalidInput(format!("{context} literal count exceeds usize"))
                })?;
                let has_plus = !synchronizing;
                // A non-synchronizing literal the caller wrote is well-formed;
                // what is missing is the server's LITERAL+ (or LITERAL- for
                // this size), so both refusals are capability gaps.
                if has_plus {
                    match literal_mode {
                        LiteralMode::Synchronizing => {
                            return Err(crate::Error::MissingCapability(format!(
                                "{context} uses a non-synchronizing literal \
                                 without negotiated LITERAL+/LITERAL- support \
                                 (RFC 7888 Section 3)"
                            )));
                        }
                        LiteralMode::LiteralMinus if size > LITERAL_MINUS_MAX => {
                            return Err(crate::Error::MissingCapability(format!(
                                "{context} uses a non-synchronizing literal of {size} octets, \
                                 which exceeds the 4096-octet limit for LITERAL- / IMAP4rev2 \
                                 mode (RFC 7888 Section 5 / RFC 9051 Section 4.3)"
                            )));
                        }
                        LiteralMode::LiteralMinus | LiteralMode::LiteralPlus => {}
                    }
                }
                let data_end = data_start.checked_add(size).ok_or_else(|| {
                    crate::Error::InvalidInput(format!(
                        "{context} literal length overflows usize \
                         (RFC 3501 Section 4.3)"
                    ))
                })?;
                if data_end > bytes.len() {
                    return Err(crate::Error::InvalidInput(format!(
                        "{context} literal declares {size} octets but the provided \
                         criteria fragment ends early (RFC 3501 Section 4.3)"
                    )));
                }
                i = data_end;
                continue;
            }
        }

        if matches!(bytes[i], b'\r' | b'\n') {
            return Err(crate::Error::InvalidInput(format!(
                "{context} must not contain raw CR or LF except inside an IMAP literal \
                 (RFC 3501 Sections 2.2 and 4.3)"
            )));
        }

        i += 1;
    }

    Ok(())
}

/// Write caller-supplied SEARCH-family criteria, re-emitting every counted
/// literal the caller wrote through the writer.
///
/// Must run AFTER [`validate_search_criteria_crlf`] has accepted `criteria`,
/// which judged the caller's ORIGINAL markers (a `{n+}` the connection cannot
/// take is a capability refusal there, and is never silently rewritten here).
/// Every byte that is not a literal marker is copied verbatim, and every
/// literal payload is copied verbatim, skipped by its declared length so
/// marker-shaped bytes inside it stay payload. Only the marker itself is
/// re-emitted, by [`CommandWriter::literal`], which is what records the
/// segment boundary a synchronizing literal needs and applies the one `+`
/// rule. Two spellings can therefore differ from what the caller wrote: a
/// count with leading zeros is written canonically (`{0001}` becomes `{1}`),
/// and a literal8 `~{n+}` the connection may not send non-synchronizing loses
/// its `+`, because sending it would have the server expect octets the client
/// then writes without a grant.
///
/// Works on bytes, never on `str` slices: the walk advances by octet offsets
/// that need not be UTF-8 boundaries.
fn write_criteria(w: &mut CommandWriter, criteria: &str) {
    use crate::connection::literals::{LiteralMarker, literal_marker_at};

    let bytes = criteria.as_bytes();
    let mut i = 0usize;
    let mut text_start = 0usize;
    while i < bytes.len() {
        let LiteralMarker::Counted {
            data_start, size, ..
        } = literal_marker_at(bytes, i)
        else {
            i += 1;
            continue;
        };
        // The validator has already proven the payload is inside `criteria`.
        let Some(data_end) = usize::try_from(size)
            .ok()
            .and_then(|size| data_start.checked_add(size))
            .filter(|&end| end <= bytes.len())
        else {
            i += 1;
            continue;
        };
        // A literal8 marker owns the tilde before its brace (RFC 4466
        // Section 2.1); leaving the tilde as text would let the writer emit a
        // classic marker after it and get the literal8 sync rule wrong.
        let (marker_start, form) = if i > text_start && bytes[i - 1] == b'~' {
            (i - 1, LiteralForm::Literal8)
        } else {
            (i, LiteralForm::Classic)
        };
        w.raw(&bytes[text_start..marker_start]);
        w.literal(&bytes[data_start..data_end], form);
        i = data_end;
        text_start = data_end;
    }
    w.raw(&bytes[text_start..]);
}

/// Detect whether SEARCH criteria begin with a CHARSET specification.
///
/// RFC 3501 Section 6.4.4 places the optional `CHARSET <astring>` prefix at
/// the start of the SEARCH criteria syntax.
fn search_criteria_starts_with_charset(criteria: &str) -> bool {
    criteria
        .split_whitespace()
        .next()
        .is_some_and(|token| token.eq_ignore_ascii_case("CHARSET"))
}

/// Validate that LOGIN credentials remain ASCII-only.
///
/// RFC 6855 Section 5 does not extend `LOGIN` to permit UTF-8 usernames or
/// passwords. Clients needing internationalized credentials MUST use
/// `AUTHENTICATE` instead.
///
/// CR/LF is deliberately *not* rejected here. `login = "LOGIN" SP userid SP
/// password` with both arguments `astring`, and RFC 9051 Section 4.3 allows a
/// literal to carry any CHAR8 including CR and LF. A credential containing a
/// line break is therefore encoded as a counted literal whose octet count is
/// taken from the same bytes that are written, so the line break is payload
/// the server consumes inside the literal and can never begin a new command.
/// Rejecting it would lock out accounts whose password legitimately contains
/// one on servers that offer no SASL mechanism.
pub(crate) fn validate_login_credential_ascii(
    value: &str,
    field: &str,
) -> Result<(), crate::Error> {
    if !value.is_ascii() {
        return Err(crate::Error::InvalidInput(format!(
            "LOGIN {field} must be ASCII-only; RFC 6855 Section 5 requires \
             AUTHENTICATE for non-ASCII credentials"
        )));
    }
    Ok(())
}

/// Validate that a SASL initial-response string contains only valid base64
/// characters per RFC 4959 Section 3.
///
/// The ABNF from RFC 4959:
/// ```text
/// initial-response = base64 / "="
/// ```
/// where `base64` is defined by RFC 4648 Section 4.
///
/// An empty string is allowed  -  the caller encodes it as `"="` on the wire
/// (RFC 4959 Section 3).
fn validate_sasl_initial_response(ir: &str) -> Result<(), crate::Error> {
    // Reject CRLF first  -  this is a command injection vector (RFC 3501 Section 2.2).
    validate_no_crlf(ir, "AUTHENTICATE initial response")?;

    // RFC 4959 Section 3: the wire form is either the special `=` marker
    // for an empty initial response, or a syntactically valid RFC 4648
    // base64 string.
    if ir.is_empty() || ir == "=" {
        return Ok(());
    }

    base64::engine::general_purpose::STANDARD
        .decode(ir.as_bytes())
        .map(|_| ())
        .map_err(|_| {
            crate::Error::InvalidInput(
                "AUTHENTICATE initial response must be RFC 4648 base64 or the special \
                 \"=\" empty marker (RFC 4959 Section 3)"
                    .into(),
            )
        })
}

// Sequence-set validation has moved to `SequenceSet::new()` in
// `crate::types::validated` (RFC 3501 Section 9). The `SequenceSet` newtype
// guarantees validity at construction time, so callers no longer need
// runtime re-validation.

/// Maximum value for `mod-sequence-value` and `mod-sequence-valzer`
/// (RFC 7162 Section 7): a positive unsigned 63-bit integer.
const MOD_SEQ_MAX: u64 = i64::MAX as u64;

/// Validates a `mod-sequence-value` per RFC 7162 Section 7:
/// `mod-sequence-value = 1*DIGIT` where `1 <= n <= 9,223,372,036,854,775,807`.
///
/// Used for CHANGEDSINCE (RFC 7162 Section 3.1.4.1) and QRESYNC `mod_seq`
/// (RFC 7162 Section 3.2.5.2).
fn validate_mod_sequence_value(val: u64, context: &str) -> Result<(), crate::Error> {
    if val == 0 {
        return Err(crate::Error::InvalidInput(format!(
            "{context} mod-sequence-value must be >= 1 per RFC 7162 Section 7, got 0"
        )));
    }
    if val > MOD_SEQ_MAX {
        return Err(crate::Error::InvalidInput(format!(
            "{context} mod-sequence-value must be <= {MOD_SEQ_MAX} per RFC 7162 Section 7, got {val}"
        )));
    }
    Ok(())
}

/// Validates a `mod-sequence-valzer` per RFC 7162 Section 7:
/// `mod-sequence-valzer = "0" / mod-sequence-value` where `0 <= n <= 9,223,372,036,854,775,807`.
///
/// Used for UNCHANGEDSINCE (RFC 7162 Section 3.1.3).
fn validate_mod_sequence_valzer(val: u64, context: &str) -> Result<(), crate::Error> {
    if val > MOD_SEQ_MAX {
        return Err(crate::Error::InvalidInput(format!(
            "{context} mod-sequence-valzer must be <= {MOD_SEQ_MAX} per RFC 7162 Section 7, got {val}"
        )));
    }
    Ok(())
}

/// Encode the CHANGEDSINCE fetch modifier (RFC 7162 Section 3.1.4.1),
/// optionally followed by VANISHED (RFC 7162 Section 3.2.6).
///
/// Emits ` (CHANGEDSINCE <modseq>)` or ` (CHANGEDSINCE <modseq> VANISHED)`.
/// Validates that `modseq` satisfies the `mod-sequence-value` production
/// (>= 1, <= `i64::MAX`).
fn encode_changedsince_modifier(
    w: &mut CommandWriter,
    modseq: u64,
    vanished: bool,
) -> Result<(), crate::Error> {
    validate_mod_sequence_value(modseq, "CHANGEDSINCE")?;
    w.raw(b" (CHANGEDSINCE ");
    w.raw(modseq.to_string().as_bytes());
    // RFC 7162 Section 3.2.6: VANISHED modifier appended in the same
    // parenthesized fetch modifier list as CHANGEDSINCE.
    if vanished {
        w.raw(b" VANISHED");
    }
    w.raw(b")");
    Ok(())
}

/// Encode ENABLE command (RFC 5161 Section 3).
///
/// RFC 5161 Section 3.1 ABNF:
/// `enable = "ENABLE" 1*(SP capability)`
/// where each capability is an `atom` per RFC 3501 Section 9.
/// The capability list must be non-empty.
fn encode_enable(
    w: &mut CommandWriter,
    tag: &str,
    capabilities: &[String],
) -> Result<(), crate::Error> {
    if capabilities.is_empty() {
        return Err(crate::Error::InvalidInput(
            "ENABLE requires at least one capability (RFC 5161 Section 3.1)".into(),
        ));
    }
    for cap in capabilities {
        validate_atom(cap, "ENABLE capability")?;
    }
    w.raw(tag.as_bytes());
    w.raw(b" ENABLE");
    for cap in capabilities {
        w.raw(b" ");
        w.raw(cap.as_bytes());
    }
    w.raw(b"\r\n");
    Ok(())
}

/// Validate that a string is a valid `atom` per RFC 3501 Section 9.
///
/// Delegates to the shared validation logic in
/// [`crate::types::validated::validate_atom_bytes`].
///
/// `context` is a human-readable label for error messages (e.g., "flag keyword",
/// "SETQUOTA resource name").
fn validate_atom(s: &str, context: &str) -> Result<(), crate::Error> {
    crate::types::validated::validate_atom_bytes(s.as_bytes(), context)?;
    Ok(())
}

/// Validate a `charset` argument for SORT/THREAD.
///
/// RFC 5256 Section 5 defines `charset = atom / quoted`, so the encoder must
/// accept either an IMAP atom or an RFC 3501 Section 9 quoted-string.
fn validate_sort_thread_charset(charset: &str) -> Result<(), crate::Error> {
    if charset.starts_with('"') || charset.ends_with('"') {
        return validate_imap_quoted_string(charset, "THREAD/SORT charset");
    }
    validate_atom(charset, "THREAD/SORT charset")
}

/// Validate an RFC 3501 quoted-string token.
///
/// RFC 3501 Section 9:
/// - `quoted = DQUOTE *QUOTED-CHAR DQUOTE`
/// - `QUOTED-CHAR = <any TEXT-CHAR except quoted-specials> / "\\" quoted-specials`
/// - `quoted-specials = DQUOTE / "\\"`
fn validate_imap_quoted_string(s: &str, context: &str) -> Result<(), crate::Error> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'"' || bytes[bytes.len() - 1] != b'"' {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must be either an atom or a quoted-string \
             (RFC 5256 Section 5 / RFC 3501 Section 9): {s:?}"
        )));
    }

    let inner = &bytes[1..bytes.len() - 1];
    if inner.is_empty() {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must not be empty (RFC 5256 Section 5: charset values \
             name an IANA-registered charset)"
        )));
    }

    let mut idx = 0usize;
    while idx < inner.len() {
        match inner[idx] {
            b'\\' => {
                let Some(&escaped) = inner.get(idx + 1) else {
                    return Err(crate::Error::InvalidInput(format!(
                        "{context} quoted-string must not end with a bare backslash \
                         (RFC 3501 Section 9)"
                    )));
                };
                if escaped != b'"' && escaped != b'\\' {
                    return Err(crate::Error::InvalidInput(format!(
                        "{context} quoted-string may only escape DQUOTE or backslash \
                         (RFC 3501 Section 9): {s:?}"
                    )));
                }
                idx += 2;
            }
            b'"' => {
                return Err(crate::Error::InvalidInput(format!(
                    "{context} quoted-string contains an unescaped DQUOTE \
                     (RFC 3501 Section 9): {s:?}"
                )));
            }
            b'\r' | b'\n' | 0x00 => {
                return Err(crate::Error::InvalidInput(format!(
                    "{context} quoted-string contains CR, LF, or NUL \
                     (RFC 3501 Section 9): {s:?}"
                )));
            }
            byte if !byte.is_ascii() => {
                return Err(crate::Error::InvalidInput(format!(
                    "{context} quoted-string must be ASCII-only \
                     (RFC 3501 Section 9 CHAR): {s:?}"
                )));
            }
            _ => {
                idx += 1;
            }
        }
    }

    Ok(())
}

/// Validate that a custom flag keyword contains only ATOM-CHARs.
///
/// RFC 3501 Section 9: `flag-keyword = atom`.
/// Delegates to [`validate_atom`].
pub(crate) fn validate_flag_keyword(s: &str) -> Result<(), crate::Error> {
    validate_atom(s, "flag keyword")
}

/// Validate a METADATA entry name before sending GETMETADATA/SETMETADATA.
///
/// RFC 5464 Section 3.2 imposes additional restrictions beyond generic
/// IMAP strings: entry names are slash-separated paths and MUST NOT contain
/// `*`, `%`, non-ASCII characters, octets in the range `0x00..=0x19`,
/// consecutive `/`, or a trailing `/`. The first path component also defines
/// the annotation scope and is currently limited to `/private` or `/shared`.
fn validate_metadata_entry_name(entry: &str, context: &str) -> Result<(), crate::Error> {
    if entry.contains("//") {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must not contain consecutive '/' characters \
             (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    if entry.ends_with('/') {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must not end with '/' (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    if !entry.is_ascii() {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must not contain non-ASCII characters \
             (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    if entry
        .bytes()
        .any(|byte| matches!(byte, 0x00..=0x19) || byte == b'*' || byte == b'%')
    {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must not contain '*', '%', or control octets \
             0x00..=0x19 (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    // RFC 5464 Section 3.2: entry names are slash-separated hierarchical
    // paths whose first component defines the annotation scope. The only
    // standardized scope prefixes are `/private` and `/shared`.
    let Some(stripped) = entry.strip_prefix('/') else {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must start with '/' and use the /private or /shared \
             scope prefixes (RFC 5464 Section 3.2): {entry:?}"
        )));
    };
    let scope = stripped.split('/').next().unwrap_or_default();
    if !matches!(scope.to_ascii_lowercase().as_str(), "private" | "shared") {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must use the /private or /shared scope prefixes \
             (RFC 5464 Section 3.2): {entry:?}"
        )));
    }

    Ok(())
}

/// Filter and validate flags for client commands (STORE, APPEND).
///
/// RFC 3501 Section 9: the `flag` production excludes `\Recent`
/// (read-only, server-set) and `\*` (only valid in `flag-perm` for
/// PERMANENTFLAGS responses).  Custom flag keywords are validated
/// against ATOM-CHAR (RFC 3501 Section 9: `flag-keyword = atom`).
///
/// Returns the filtered flags with `\Recent` and `\*` removed.
fn validate_and_filter_flags<'a>(
    flags: &'a [crate::types::Flag],
    context: &str,
) -> Result<Vec<&'a crate::types::Flag>, crate::Error> {
    let valid_flags: Vec<_> = flags
        .iter()
        .filter(|f| {
            if matches!(f, crate::types::Flag::Recent | crate::types::Flag::Wildcard) {
                tracing::debug!(
                    flag = %f,
                    "Skipping flag not permitted in {context} per RFC 3501 Section 9 (flag production)"
                );
                false
            } else {
                true
            }
        })
        .collect();
    for flag in &valid_flags {
        if let crate::types::Flag::Custom(s) = flag {
            validate_flag_keyword(s)?;
        }
    }
    Ok(valid_flags)
}

/// Validate an APPEND date-time string against RFC 3501 Section 9.
///
/// The expected format (without the surrounding DQUOTEs, which the caller adds):
///
/// ```text
/// date-day-fixed "-" date-month "-" date-year SP time SP zone
/// date-day-fixed = (SP DIGIT) / 2DIGIT          ; 1-31, space-padded or zero-padded
/// date-month     = "Jan" / "Feb" / "Mar" / "Apr" / "May" / "Jun" /
///                  "Jul" / "Aug" / "Sep" / "Oct" / "Nov" / "Dec"
/// date-year      = 4DIGIT
/// time           = 2DIGIT ":" 2DIGIT ":" 2DIGIT  ; HH:MM:SS
/// zone           = ("+" / "-") 4DIGIT
/// ```
#[allow(clippy::too_many_lines)]
pub(crate) fn validate_append_datetime(date: &str) -> Result<(), crate::Error> {
    const MONTHS: [&[u8]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];

    let b = date.as_bytes();

    // Expected length: " 7-Jul-1996 02:44:25 -0700" = 26 bytes.
    // date-day-fixed (2) + "-" (1) + month (3) + "-" (1) + year (4)
    //   + SP (1) + time (8) + SP (1) + zone (5) = 26
    if b.len() != 26 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected 26 characters, got {}  -  \
             date-time must be date-day-fixed \"-\" date-month \"-\" date-year \
             SP time SP zone (RFC 3501 Section 9)",
            b.len()
        )));
    }

    // date-day-fixed = (SP DIGIT) / 2DIGIT (RFC 3501 Section 9).
    //
    // The `(SP DIGIT)` form covers space-padded single-digit days (` 1`-` 9`).
    // The `2DIGIT` form covers any two-digit day; semantically valid days are
    // `01`-`31`. Zero-padded single-digit days (`01`-`09`) are valid per the
    // `2DIGIT` alternative  -  RFC 5234 Section 3.4 defines `2DIGIT = DIGIT DIGIT`.
    let day_hi = b[0];
    let day_lo = b[1];
    let valid_day = match day_hi {
        b' ' => day_lo.is_ascii_digit() && day_lo != b'0',
        b'0' => (b'1'..=b'9').contains(&day_lo),
        b'1'..=b'2' => day_lo.is_ascii_digit(),
        b'3' => day_lo == b'0' || day_lo == b'1',
        _ => false,
    };
    if !valid_day {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid day {:?}  -  date-day-fixed must be (SP DIGIT) / 2DIGIT \
             with values 1-31 (RFC 3501 Section 9)",
            std::str::from_utf8(&b[0..2]).unwrap_or("??")
        )));
    }

    // First separator: "-" (RFC 3501 Section 9).
    if b[2] != b'-' {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected '-' at position 2, got {:?} (RFC 3501 Section 9)",
            b[2] as char
        )));
    }

    // date-month (RFC 3501 Section 9).
    // RFC 3501 Section 9 paragraph (1): "all alphabetic characters are
    // case-insensitive ... Implementations MUST accept these strings in a
    // case-insensitive fashion."  RFC 5234 Section 2.3 confirms ABNF
    // literal strings are case-insensitive.
    let month = &b[3..6];
    if !MONTHS.iter().any(|m| m.eq_ignore_ascii_case(month)) {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid month {:?}  -  date-month must be Jan/Feb/.../Dec (RFC 3501 Section 9)",
            std::str::from_utf8(month).unwrap_or("???")
        )));
    }

    let day: u8 = match day_hi {
        b' ' => day_lo - b'0',
        _ => (day_hi - b'0') * 10 + (day_lo - b'0'),
    };

    // Second separator: "-" (RFC 3501 Section 9).
    if b[6] != b'-' {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected '-' at position 6, got {:?} (RFC 3501 Section 9)",
            b[6] as char
        )));
    }

    // date-year = 4DIGIT (RFC 3501 Section 9).
    if !b[7..11].iter().all(u8::is_ascii_digit) {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid year {:?}  -  date-year must be 4DIGIT (RFC 3501 Section 9)",
            std::str::from_utf8(&b[7..11]).unwrap_or("????")
        )));
    }

    // Parse year for the day-vs-month cross-check below (needed for
    // leap-year validation of February 29).
    let year: u16 = u16::from(b[7] - b'0') * 1000
        + u16::from(b[8] - b'0') * 100
        + u16::from(b[9] - b'0') * 10
        + u16::from(b[10] - b'0');

    // Day-vs-month cross-check (RFC 3501 Section 9).
    //
    // The ABNF `date-day-fixed` allows 1-31 structurally, but not every day
    // is valid for every month.  Postel's law says "be conservative in what
    // you send", so we reject invalid dates before transmitting them.
    //   - 31-day months: Jan, Mar, May, Jul, Aug, Oct, Dec
    //   - 30-day months: Apr, Jun, Sep, Nov
    //   - Feb: 29 in leap years, 28 otherwise
    let max_day: u8 = match month[0].to_ascii_lowercase() {
        // Feb  -  28 or 29 depending on leap year.
        // Leap year rules: divisible by 400 -> leap; else by 100 -> not;
        // else by 4 -> leap; else -> not.
        b'f' => {
            let y = u32::from(year);
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        // Apr -> 30
        b'a' if month[1].eq_ignore_ascii_case(&b'p') => 30,
        // Jun -> 30
        b'j' if month[1].eq_ignore_ascii_case(&b'u') && month[2].eq_ignore_ascii_case(&b'n') => 30,
        // Sep, Nov -> 30
        b's' | b'n' => 30,
        // Jan, Mar, May, Jul, Aug, Oct, Dec -> 31
        _ => 31,
    };
    if day > max_day {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid day {} for month {:?} year {}  -  maximum is {} (RFC 3501 Section 9)",
            day,
            std::str::from_utf8(month).unwrap_or("???"),
            year,
            max_day
        )));
    }

    // SP between date and time (RFC 3501 Section 9).
    if b[11] != b' ' {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected SP at position 11, got {:?} (RFC 3501 Section 9)",
            b[11] as char
        )));
    }

    // time = 2DIGIT ":" 2DIGIT ":" 2DIGIT (RFC 3501 Section 9).
    let time = &b[12..20];
    let valid_time = time[0].is_ascii_digit()
        && time[1].is_ascii_digit()
        && time[2] == b':'
        && time[3].is_ascii_digit()
        && time[4].is_ascii_digit()
        && time[5] == b':'
        && time[6].is_ascii_digit()
        && time[7].is_ascii_digit();
    if !valid_time {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid time {:?}  -  time must be 2DIGIT \":\" 2DIGIT \":\" 2DIGIT \
             (RFC 3501 Section 9)",
            std::str::from_utf8(time).unwrap_or("????????")
        )));
    }

    // Semantic range checks for time components (RFC 3501 Section 9).
    // The ABNF `time = 2DIGIT ":" 2DIGIT ":" 2DIGIT` is purely structural,
    // but since we already enforce semantic day ranges (1-31) and month names,
    // we apply the same rigour here.  Valid ranges follow RFC 5322 Section 3.3:
    //   hour   = 00-23
    //   minute = 00-59
    //   second = 00-60  (60 allows for leap seconds)
    let hour = (time[0] - b'0') * 10 + (time[1] - b'0');
    let minute = (time[3] - b'0') * 10 + (time[4] - b'0');
    let second = (time[6] - b'0') * 10 + (time[7] - b'0');

    if hour > 23 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid hour {hour:02}  -  must be 00-23 (RFC 3501 Section 9, \
             RFC 5322 Section 3.3)"
        )));
    }
    if minute > 59 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid minute {minute:02}  -  must be 00-59 (RFC 3501 Section 9, \
             RFC 5322 Section 3.3)"
        )));
    }
    if second > 60 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid second {second:02}  -  must be 00-60 (RFC 3501 Section 9, \
             RFC 5322 Section 3.3; 60 permits leap seconds)"
        )));
    }

    // SP between time and zone (RFC 3501 Section 9).
    if b[20] != b' ' {
        return Err(crate::Error::InvalidAppendDate(format!(
            "expected SP at position 20, got {:?} (RFC 3501 Section 9)",
            b[20] as char
        )));
    }

    // zone = ("+" / "-") 4DIGIT (RFC 3501 Section 9).
    let zone = &b[21..26];
    let valid_zone =
        (zone[0] == b'+' || zone[0] == b'-') && zone[1..].iter().all(u8::is_ascii_digit);
    if !valid_zone {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid zone {:?}  -  zone must be (\"+\" / \"-\") 4DIGIT (RFC 3501 Section 9)",
            std::str::from_utf8(zone).unwrap_or("?????")
        )));
    }

    // Semantic range checks for zone components (RFC 3501 Section 9).
    // The zone is ±HHMM.  RFC 5322 Section 4.3 (obs-zone) does not
    // constrain the digits, but real UTC offsets range from -1200 to +1400,
    // and minutes are always 00-59.  We enforce those limits for consistency
    // with the semantic day/time checks above.
    let zone_hh = (zone[1] - b'0') * 10 + (zone[2] - b'0');
    let zone_mm = (zone[3] - b'0') * 10 + (zone[4] - b'0');

    if zone_hh > 14 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid zone hour {zone_hh:02}  -  must be 00-14 \
             (maximum real UTC offset is ±14:00, RFC 3501 Section 9)"
        )));
    }
    if zone_mm > 59 {
        return Err(crate::Error::InvalidAppendDate(format!(
            "invalid zone minute {zone_mm:02}  -  must be 00-59 \
             (RFC 3501 Section 9)"
        )));
    }
    // RFC 3501 Section 9: `zone = ("+" / "-") 4DIGIT`  -  the grammar
    // imposes no semantic constraint beyond valid digits.  We cap the
    // hour at 14 and minute at 59 above for basic sanity, but do NOT
    // reject non-zero minutes when the hour is 14.  Real-world offsets
    // with sub-hour components exist (e.g., +12:45 Chatham Islands
    // standard, +13:45 Chatham Islands DST), and future timezone
    // changes could introduce new offsets at any hour.

    Ok(())
}
