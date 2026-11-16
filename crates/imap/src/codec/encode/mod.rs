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
#[path = "tests/mod.rs"]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;

use base64::Engine as _;

#[cfg(test)]
use crate::types::Command;
use crate::types::{Capability, QresyncParams};

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
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!(
                    "{context} must not contain CR or LF  -  IMAP commands are \
             CRLF-delimited (RFC 3501 Section 2.2)"
                )
                .into(),
            ),
        ));
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
pub(crate) fn validate_search_criteria_crlf(
    criteria: &str,
    context: &str,
    literal_mode: LiteralMode,
) -> Result<(), crate::Error> {
    let bytes = criteria.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'{' {
            use crate::codec::literals::{LiteralMarker, literal_marker_at};
            if let LiteralMarker::Counted {
                data_start,
                size,
                synchronizing,
            } = literal_marker_at(bytes, i)
            {
                let size = usize::try_from(size).map_err(|_| {
                    crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
                        format!("{context} literal count exceeds usize").into(),
                    ))
                })?;
                let has_plus = !synchronizing;
                // A non-synchronizing literal the caller wrote is well-formed;
                // what is missing is the server's LITERAL+ (or LITERAL- for
                // this size), so both refusals are capability gaps.
                if has_plus {
                    match literal_mode {
                        LiteralMode::Synchronizing => {
                            return Err(crate::Error::LocalRefusal(
                                crate::error::LocalRefusal::MissingCapability(
                                    crate::error::CapabilityFailure::new(
                                        "LITERAL+".into(),
                                        crate::error::CapabilityFailureReason::Alternatives(vec![
                                            "LITERAL-".into(),
                                        ]),
                                        format!(
                                            "{context} uses a non-synchronizing literal \
                                 without negotiated LITERAL+/LITERAL- support \
                                 (RFC 7888 Section 3)"
                                        ),
                                    ),
                                ),
                            ));
                        }
                        LiteralMode::LiteralMinus if size > LITERAL_MINUS_MAX => {
                            return Err(crate::Error::LocalRefusal(
                                crate::error::LocalRefusal::MissingCapability(
                                    crate::error::CapabilityFailure::new(
                                        "LITERAL+".into(),
                                        crate::error::CapabilityFailureReason::LiteralTooLarge {
                                            size,
                                            limit: LITERAL_MINUS_MAX,
                                        },
                                        format!(
                                            "{context} uses a non-synchronizing literal of {size} octets, \
                                 which exceeds the 4096-octet limit for LITERAL- / IMAP4rev2 \
                                 mode (RFC 7888 Section 5 / RFC 9051 Section 4.3)"
                                        ),
                                    ),
                                ),
                            ));
                        }
                        LiteralMode::LiteralMinus | LiteralMode::LiteralPlus => {}
                    }
                }
                let data_end = data_start.checked_add(size).ok_or_else(|| {
                    crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
                        format!(
                            "{context} literal length overflows usize \
                         (RFC 3501 Section 4.3)"
                        )
                        .into(),
                    ))
                })?;
                if data_end > bytes.len() {
                    return Err(crate::Error::LocalRefusal(
                        crate::error::LocalRefusal::InvalidInput(
                            format!(
                                "{context} literal declares {size} octets but the provided \
                         criteria fragment ends early (RFC 3501 Section 4.3)"
                            )
                            .into(),
                        ),
                    ));
                }
                i = data_end;
                continue;
            }
        }

        if matches!(bytes[i], b'\r' | b'\n') {
            return Err(crate::Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidInput(
                    format!(
                        "{context} must not contain raw CR or LF except inside an IMAP literal \
                 (RFC 3501 Sections 2.2 and 4.3)"
                    )
                    .into(),
                ),
            ));
        }

        i += 1;
    }

    Ok(())
}

/// Write typed SEARCH-family criteria. Strings go through the writer's
/// quoting/literal rule; a `Raw` escape is written by [`write_criteria`].
fn write_search(
    w: &mut CommandWriter,
    criteria: &crate::types::SearchCriteria,
    utf8: crate::types::wire::WireMode,
) {
    use crate::types::search::SearchKey;
    for (index, key) in criteria.keys().iter().enumerate() {
        if index != 0 {
            w.raw(b" ");
        }
        match key {
            SearchKey::Keyword(keyword) => w.raw(keyword.as_bytes()),
            SearchKey::Date { key, value } => {
                w.raw(key.as_bytes());
                w.raw(b" ");
                w.raw(value.as_bytes());
            }
            SearchKey::Number { key, value } => {
                w.raw(key.as_bytes());
                w.raw(b" ");
                w.raw(value.to_string().as_bytes());
            }
            SearchKey::Flag { excluded, value } => {
                w.raw(if *excluded {
                    b"UNKEYWORD "
                } else {
                    b"KEYWORD "
                });
                w.raw(value.as_bytes());
            }
            SearchKey::Uid(set) => {
                w.raw(b"UID ");
                w.raw(set.as_bytes());
            }
            SearchKey::Sequence(set) => w.raw(set.as_bytes()),
            SearchKey::String { prefix, value } => {
                w.raw(prefix.as_bytes());
                w.raw(b" ");
                w.string(value.as_bytes(), utf8);
            }
            SearchKey::Not(inner) => {
                w.raw(b"NOT (");
                write_search(w, inner, utf8);
                w.raw(b")");
            }
            SearchKey::Or(a, b) => {
                w.raw(b"OR (");
                write_search(w, a, utf8);
                w.raw(b") (");
                write_search(w, b, utf8);
                w.raw(b")");
            }
            SearchKey::Group(inner) => {
                w.raw(b"(");
                write_search(w, inner, utf8);
                w.raw(b")");
            }
            SearchKey::Raw { text, .. } => write_criteria(w, text),
        }
    }
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
    use crate::codec::literals::{LiteralMarker, literal_marker_at};

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
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!(
                    "LOGIN {field} must be ASCII-only; RFC 6855 Section 5 requires \
             AUTHENTICATE for non-ASCII credentials"
                )
                .into(),
            ),
        ));
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
            crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
                "AUTHENTICATE initial response must be RFC 4648 base64 or the special \
                 \"=\" empty marker (RFC 4959 Section 3)"
                    .into(),
            ))
        })
}

// Sequence-set validation has moved to `SequenceSet::new()` in
// `crate::types::validated` (RFC 3501 Section 9). The `SequenceSet` newtype
// guarantees validity at construction time, so callers no longer need
// runtime re-validation.

/// Encode the CHANGEDSINCE fetch modifier (RFC 7162 Section 3.1.4.1),
/// optionally followed by VANISHED (RFC 7162 Section 3.2.6).
///
/// Emits ` (CHANGEDSINCE <modseq>)` or ` (CHANGEDSINCE <modseq> VANISHED)`.
/// The operand already satisfies the positive 63-bit MODSEQ invariant.
fn encode_changedsince_modifier(
    w: &mut CommandWriter,
    modseq: crate::types::ModSeq,
    vanished: bool,
) {
    w.raw(b" (CHANGEDSINCE ");
    w.raw(modseq.to_string().as_bytes());
    // RFC 7162 Section 3.2.6: VANISHED modifier appended in the same
    // parenthesized fetch modifier list as CHANGEDSINCE.
    if vanished {
        w.raw(b" VANISHED");
    }
    w.raw(b")");
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
    capabilities: &[Capability],
) -> Result<(), crate::Error> {
    if capabilities.is_empty() {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                "ENABLE requires at least one capability (RFC 5161 Section 3.1)".into(),
            ),
        ));
    }
    for cap in capabilities {
        validate_atom(&cap.to_string(), "ENABLE capability")?;
    }
    w.raw(tag.as_bytes());
    w.raw(b" ENABLE");
    for cap in capabilities {
        w.raw(b" ");
        w.raw(cap.to_string().as_bytes());
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
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!(
                    "{context} must be either an atom or a quoted-string \
             (RFC 5256 Section 5 / RFC 3501 Section 9): {s:?}"
                )
                .into(),
            ),
        ));
    }

    let inner = &bytes[1..bytes.len() - 1];
    if inner.is_empty() {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!(
                    "{context} must not be empty (RFC 5256 Section 5: charset values \
             name an IANA-registered charset)"
                )
                .into(),
            ),
        ));
    }

    let mut idx = 0usize;
    while idx < inner.len() {
        match inner[idx] {
            b'\\' => {
                let Some(&escaped) = inner.get(idx + 1) else {
                    return Err(crate::Error::LocalRefusal(
                        crate::error::LocalRefusal::InvalidInput(
                            format!(
                                "{context} quoted-string must not end with a bare backslash \
                         (RFC 3501 Section 9)"
                            )
                            .into(),
                        ),
                    ));
                };
                if escaped != b'"' && escaped != b'\\' {
                    return Err(crate::Error::LocalRefusal(
                        crate::error::LocalRefusal::InvalidInput(
                            format!(
                                "{context} quoted-string may only escape DQUOTE or backslash \
                         (RFC 3501 Section 9): {s:?}"
                            )
                            .into(),
                        ),
                    ));
                }
                idx += 2;
            }
            b'"' => {
                return Err(crate::Error::LocalRefusal(
                    crate::error::LocalRefusal::InvalidInput(
                        format!(
                            "{context} quoted-string contains an unescaped DQUOTE \
                     (RFC 3501 Section 9): {s:?}"
                        )
                        .into(),
                    ),
                ));
            }
            b'\r' | b'\n' | 0x00 => {
                return Err(crate::Error::LocalRefusal(
                    crate::error::LocalRefusal::InvalidInput(
                        format!(
                            "{context} quoted-string contains CR, LF, or NUL \
                     (RFC 3501 Section 9): {s:?}"
                        )
                        .into(),
                    ),
                ));
            }
            byte if !byte.is_ascii() => {
                return Err(crate::Error::LocalRefusal(
                    crate::error::LocalRefusal::InvalidInput(
                        format!(
                            "{context} quoted-string must be ASCII-only \
                     (RFC 3501 Section 9 CHAR): {s:?}"
                        )
                        .into(),
                    ),
                ));
            }
            _ => {
                idx += 1;
            }
        }
    }

    Ok(())
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
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!(
                    "{context} must not contain consecutive '/' characters \
             (RFC 5464 Section 3.2): {entry:?}"
                )
                .into(),
            ),
        ));
    }

    if entry.ends_with('/') {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!("{context} must not end with '/' (RFC 5464 Section 3.2): {entry:?}").into(),
            ),
        ));
    }

    if !entry.is_ascii() {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!(
                    "{context} must not contain non-ASCII characters \
             (RFC 5464 Section 3.2): {entry:?}"
                )
                .into(),
            ),
        ));
    }

    if entry
        .bytes()
        .any(|byte| matches!(byte, 0x00..=0x19) || byte == b'*' || byte == b'%')
    {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!(
                    "{context} must not contain '*', '%', or control octets \
             0x00..=0x19 (RFC 5464 Section 3.2): {entry:?}"
                )
                .into(),
            ),
        ));
    }

    // RFC 5464 Section 3.2: entry names are slash-separated hierarchical
    // paths whose first component defines the annotation scope. The only
    // standardized scope prefixes are `/private` and `/shared`.
    let Some(stripped) = entry.strip_prefix('/') else {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!(
                    "{context} must start with '/' and use the /private or /shared \
             scope prefixes (RFC 5464 Section 3.2): {entry:?}"
                )
                .into(),
            ),
        ));
    };
    let scope = stripped.split('/').next().unwrap_or_default();
    if !matches!(scope.to_ascii_lowercase().as_str(), "private" | "shared") {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                format!(
                    "{context} must use the /private or /shared scope prefixes \
             (RFC 5464 Section 3.2): {entry:?}"
                )
                .into(),
            ),
        ));
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
    _context: &str,
) -> Result<Vec<crate::types::flag::StoreFlag<'a>>, crate::Error> {
    flags
        .iter()
        .filter_map(|flag| crate::types::flag::StoreFlag::from_observed(flag).transpose())
        .collect()
}

use crate::types::datetime::validate_append_datetime;
