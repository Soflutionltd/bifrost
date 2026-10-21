//! Individual IMAP command encoders.
//!
//! Each function encodes a specific IMAP command through a
//! [`CommandWriter`]. These are called from the main dispatch in
//! [`super::encode_command`].
//!
//! Command syntax is defined in RFC 3501 Section 6 / RFC 9051 Section 6.

use super::{
    CommandWriter, QresyncParams, encode_changedsince_modifier,
    search_criteria_starts_with_charset, validate_and_filter_flags, validate_append_datetime,
    validate_atom, validate_login_credential_ascii, validate_mod_sequence_value,
    validate_mod_sequence_valzer, validate_no_crlf, validate_sasl_initial_response,
    validate_search_criteria_crlf, validate_sort_thread_charset, write_criteria,
};
use crate::types::validated::MailboxName;

mod append;
mod id;
mod list;
mod metadata;
mod notify;
mod quota_acl;
mod thread_sort;

pub(super) use self::append::encode_append;
pub(super) use self::id::encode_id;
pub(crate) use self::list::list_status_return_option_items;
pub(super) use self::list::{encode_create_special_use, encode_list_extended, encode_list_status};
#[cfg(test)]
pub(super) use self::metadata::encode_metadata_value;
pub(super) use self::metadata::{encode_getmetadata, encode_setmetadata};
pub(super) use self::notify::encode_notify_set;
pub(super) use self::quota_acl::{encode_set_acl, encode_set_quota};
pub(super) use self::thread_sort::encode_thread_or_sort_cmd;

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Encode a simple command with no arguments (e.g. `NOOP`, `LOGOUT`).
///
/// Used for commands whose ABNF is just the command name followed by CRLF
/// (RFC 3501 Section 6 / RFC 9051 Section 6).
pub(super) fn encode_simple(w: &mut CommandWriter, tag: &str, command: &str) {
    w.raw(tag.as_bytes());
    w.raw(b" ");
    w.raw(command.as_bytes());
    w.raw(b"\r\n");
}

/// Encode LOGIN command (RFC 3501 Section 6.2.3 / RFC 9051 Section 6.2.3).
///
/// RFC 6855 Section 5 does not extend `LOGIN` to permit UTF-8 credentials.
/// Clients needing non-ASCII usernames or passwords MUST use `AUTHENTICATE`.
pub(super) fn encode_login(
    w: &mut CommandWriter,
    tag: &str,
    user: &str,
    pass: &str,
    utf8: bool,
) -> Result<(), crate::Error> {
    validate_login_credential_ascii(user, "user")?;
    validate_login_credential_ascii(pass, "password")?;
    w.raw(tag.as_bytes());
    w.raw(b" LOGIN ");
    // RFC 3501 Section 6.2.3 / RFC 9051 Section 6.2.3: LOGIN arguments are strings.
    w.string(user.as_bytes(), utf8);
    w.raw(b" ");
    w.string(pass.as_bytes(), utf8);
    w.raw(b"\r\n");
    Ok(())
}

/// Encode AUTHENTICATE command (RFC 3501 Section 6.2.2 / RFC 9051 Section 6.2.2).
///
/// When `initial_response` is present (SASL-IR, RFC 4959), the value is
/// validated to contain only base64 characters (`[A-Za-z0-9+/=]`) and no
/// CRLF bytes (RFC 3501 Section 2.2) before being written to the buffer.
pub(super) fn encode_authenticate(
    w: &mut CommandWriter,
    tag: &str,
    mechanism: &str,
    initial_response: Option<&str>,
) -> Result<(), crate::Error> {
    w.raw(tag.as_bytes());
    w.raw(b" AUTHENTICATE ");
    w.raw(mechanism.as_bytes());
    if let Some(ir) = initial_response {
        // Validate before writing (RFC 4959 Section 3, RFC 3501 Section 2.2).
        validate_sasl_initial_response(ir)?;
        // SASL-IR (RFC 4959): send initial response on the same line.
        // RFC 4959 Section 3: empty initial response MUST be sent as `=`.
        if ir.is_empty() {
            w.raw(b" =");
        } else {
            w.raw(b" ");
            w.raw(ir.as_bytes());
        }
    }
    w.raw(b"\r\n");
    Ok(())
}

/// Encode STATUS command (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11).
pub(super) fn encode_status(
    w: &mut CommandWriter,
    tag: &str,
    mailbox: impl AsRef<[u8]>,
    items: &str,
    utf8: bool,
) -> Result<(), crate::Error> {
    // Reject CRLF in status items to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(items, "STATUS items")?;
    let status_items = normalize_status_items(items, "STATUS items")?;
    w.raw(tag.as_bytes());
    w.raw(b" STATUS ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    w.string(mailbox.as_ref(), utf8);
    w.raw(b" ");
    w.raw(status_items.as_bytes());
    w.raw(b"\r\n");
    Ok(())
}

/// Encode a SEARCH-family command (RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4).
///
/// Shared by all SEARCH variants. `cmd` is the command prefix
/// (e.g. `"SEARCH"`, `"UID SEARCH"`, `"SEARCH RETURN (SAVE)"`).
///
/// When `return_opts` is `Some`, emits `RETURN (<opts>)` between the command
/// and criteria (RFC 4731 Section 3.2 / Section 4 ABNF).
pub(super) fn encode_search(
    w: &mut CommandWriter,
    tag: &str,
    cmd: &str,
    criteria: &str,
    return_opts: Option<&[String]>,
    utf8: bool,
) -> Result<(), crate::Error> {
    validate_search_criteria_crlf(criteria, "SEARCH criteria", w.literal_mode())?;
    validate_non_empty_search_criteria(criteria, cmd)?;
    // RFC 6855 Section 3: after ENABLE UTF8=ACCEPT, clients MUST NOT issue a
    // SEARCH command that contains a charset specification.
    if utf8 && search_criteria_starts_with_charset(criteria) {
        return Err(crate::Error::InvalidInput(format!(
            "{cmd} must not include a CHARSET specification when UTF8=ACCEPT is active \
             (RFC 6855 Section 3)"
        )));
    }
    w.raw(tag.as_bytes());
    w.raw(b" ");
    w.raw(cmd.as_bytes());
    if let Some(opts) = return_opts {
        // RFC 4731 Section 3.2: RETURN (opt1 opt2 ...)
        for opt in opts {
            // Reject CRLF in each return option to prevent command injection
            // (RFC 3501 Section 2.2).
            validate_no_crlf(opt, "SEARCH RETURN option")?;
            // RFC 4731 Section 3.2 / RFC 4466 Section 2.6.2: each
            // search-return option is a single IMAP atom token. Outer
            // whitespace may be trimmed for interoperability, but embedded
            // whitespace would change the wire option list and must be
            // rejected rather than serialized.
            validate_atom(opt.trim(), "SEARCH RETURN option")?;
        }
        w.raw(b" RETURN (");
        for (i, opt) in opts.iter().enumerate() {
            if i > 0 {
                w.raw(b" ");
            }
            w.raw(opt.trim().as_bytes());
        }
        w.raw(b")");
    }
    w.raw(b" ");
    write_criteria(w, criteria);
    w.raw(b"\r\n");
    Ok(())
}

/// Encode FETCH command (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5).
///
/// When `changed_since` is `Some`, appends the CHANGEDSINCE modifier
/// per RFC 7162 Section 3.1.4.1:
/// `"FETCH" SP sequence-set SP fetch-att SP "(CHANGEDSINCE" SP mod-sequence-value ")"`
pub(super) fn encode_fetch(
    w: &mut CommandWriter,
    tag: &str,
    sequence_set: &str,
    items: &str,
    changed_since: Option<u64>,
) -> Result<(), crate::Error> {
    // Reject CRLF in raw parameters to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(sequence_set, "FETCH sequence set")?;
    validate_no_crlf(items, "FETCH items")?;
    validate_non_empty_fetch_items(items, "FETCH items")?;
    w.raw(tag.as_bytes());
    w.raw(b" FETCH ");
    w.raw(sequence_set.as_bytes());
    w.raw(b" ");
    w.raw(items.as_bytes());
    if let Some(modseq) = changed_since {
        encode_changedsince_modifier(w, modseq, false)?;
    }
    w.raw(b"\r\n");
    Ok(())
}

/// Encode STORE / UID STORE command (RFC 3501 Section 6.4.6 / RFC 9051 Section 6.4.6).
pub(super) fn encode_store(
    w: &mut CommandWriter,
    tag: &str,
    uid: bool,
    sequence_set: &str,
    operation: crate::types::StoreOperation,
    flags: &[crate::types::Flag],
    unchanged_since: Option<u64>,
) -> Result<(), crate::Error> {
    // Reject CRLF in sequence set to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(sequence_set, "STORE sequence set")?;
    w.raw(tag.as_bytes());
    if uid {
        w.raw(b" UID STORE ");
    } else {
        w.raw(b" STORE ");
    }
    w.raw(sequence_set.as_bytes());

    // CONDSTORE modifier (RFC 7162 Section 3.1.3): UNCHANGEDSINCE uses mod-sequence-valzer
    if let Some(modseq) = unchanged_since {
        validate_mod_sequence_valzer(modseq, "UNCHANGEDSINCE")?;
        w.raw(b" (UNCHANGEDSINCE ");
        w.raw(modseq.to_string().as_bytes());
        w.raw(b")");
    }

    encode_store_flags(w, operation, flags)
}

/// Encode a command with two arguments: one atom and one quoted-or-literal string.
///
/// Used for COPY (RFC 3501 Section 6.4.7 / RFC 9051 Section 6.4.7),
/// UID COPY (RFC 3501 Section 6.4.8 / RFC 9051 Section 6.4.9),
/// and MOVE / UID MOVE (RFC 6851 Section 3 / RFC 9051 Section 6.4.8).
#[allow(clippy::too_many_arguments)]
pub(super) fn encode_two_arg(
    w: &mut CommandWriter,
    tag: &str,
    uid: bool,
    cmd: &str,
    arg1: &str,
    arg2: impl AsRef<[u8]>,
    utf8: bool,
) -> Result<(), crate::Error> {
    // Reject CRLF in the unquoted first argument (typically a sequence set)
    // to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(arg1, &format!("{cmd} sequence set"))?;
    w.raw(tag.as_bytes());
    if uid {
        w.raw(b" UID ");
    } else {
        w.raw(b" ");
    }
    w.raw(cmd.as_bytes());
    w.raw(b" ");
    w.raw(arg1.as_bytes());
    w.raw(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    w.string(arg2.as_ref(), utf8);
    w.raw(b"\r\n");
    Ok(())
}

/// Encode a command with two quoted-or-literal arguments.
///
/// Format: `tag SP cmd SP quoted-or-literal(arg1) SP quoted-or-literal(arg2) CRLF`.
/// Used by LIST (RFC 3501 Section 6.3.8), LSUB (RFC 3501 Section 6.3.9),
/// and RENAME (RFC 3501 Section 6.3.5).
pub(super) fn encode_two_quoted_args(
    w: &mut CommandWriter,
    tag: &str,
    cmd: &str,
    arg1: impl AsRef<[u8]>,
    arg2: impl AsRef<[u8]>,
    utf8: bool,
) {
    w.raw(tag.as_bytes());
    w.raw(b" ");
    w.raw(cmd.as_bytes());
    w.raw(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    w.string(arg1.as_ref(), utf8);
    w.raw(b" ");
    w.string(arg2.as_ref(), utf8);
    w.raw(b"\r\n");
}

/// Normalize STATUS data items to the on-wire `(<items>)` form.
///
/// RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11 require the STATUS
/// command to carry a parenthesized `status-att-list`. For ergonomics, the
/// encoder accepts either a raw item list (`"MESSAGES UNSEEN"`) or an already
/// parenthesized list (`"(MESSAGES UNSEEN)"`) and canonicalizes both forms to
/// the required wire syntax.
fn normalize_status_items(items: &str, context: &str) -> Result<String, crate::Error> {
    let body = normalize_status_items_body(items, context)?;
    Ok(format!("({body})"))
}

/// Normalize the body of a STATUS data item list.
///
/// Accepts either a raw space-separated item list or a parenthesized
/// `status-att-list` and returns only the inner items. The list must contain
/// at least one atom (RFC 3501 Section 6.3.10 / RFC 5819 Section 2).
fn normalize_status_items_body(items: &str, context: &str) -> Result<String, crate::Error> {
    let trimmed = items.trim();
    if trimmed.is_empty() {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must contain at least one status data item \
             (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
        )));
    }

    let body = match (trimmed.strip_prefix('('), trimmed.strip_suffix(')')) {
        (Some(without_open), Some(_)) => {
            let inner = without_open
                .strip_suffix(')')
                .ok_or_else(|| {
                    crate::Error::InvalidInput(format!(
                        "{context} must be a single parenthesized status-att-list \
                         (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    ))
                })?
                .trim();
            if inner.is_empty() {
                return Err(crate::Error::InvalidInput(format!(
                    "{context} must contain at least one status data item \
                     (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                )));
            }
            inner
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(crate::Error::InvalidInput(format!(
                "{context} must use balanced parentheses for status-att-list \
                 syntax (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
            )));
        }
        (None, None) => trimmed,
    };

    if body.contains('(') || body.contains(')') {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must be a flat status-att-list without nested parentheses \
             (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
        )));
    }

    Ok(body.to_owned())
}

/// Validate that a SEARCH-family command has at least one search criterion.
///
/// RFC 3501 Section 6.4.4 defines SEARCH as requiring one or more search
/// keys. RFC 5256 Sections 2-3 inherit the same `search-criteria` production
/// for SORT and THREAD.
fn validate_non_empty_search_criteria(criteria: &str, context: &str) -> Result<(), crate::Error> {
    if criteria.trim().is_empty() {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must contain at least one search criterion \
             (RFC 3501 Section 6.4.4 / RFC 5256 Section 6)"
        )));
    }
    Ok(())
}

/// Validate that a FETCH-family command has at least one message data item
/// and structurally balanced `fetch-att` syntax.
///
/// RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5 require FETCH and UID
/// FETCH to include either a macro or a non-empty parenthesized `fetch-att`
/// list. Individual `fetch-att` values can also contain nested delimiters,
/// such as `BODY[HEADER.FIELDS (DATE FROM)]`, partial specifiers
/// `<start.count>`, and extension modifiers like `PREVIEW (LAZY)`, so the
/// encoder validates balanced quotes and delimiters before writing the
/// command to the wire.
/// Validate a single `fetch-att` value for NOTIFY `MessageNew`.
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// message-event = ("MessageNew" [SP "(" fetch-att *(SP fetch-att) ")"])
/// ```
///
/// Each element of the `fetch_attrs` vector must be a single `fetch-att`,
/// not a parenthesized list of multiple items. The encoder wraps the
/// individual values in `(...)` with spaces. Accepting a parenthesized
/// list like `(UID FLAGS)` would produce `MessageNew ((UID FLAGS))` which
/// is malformed per the ABNF.
fn validate_single_fetch_att(attr: &str) -> Result<(), crate::Error> {
    let trimmed = attr.trim();
    if trimmed.is_empty() {
        return Err(crate::Error::InvalidInput(
            "NOTIFY MessageNew fetch-att must not be empty \
             (RFC 5465 Section 8)"
                .into(),
        ));
    }
    // A single fetch-att never starts with '('  -  that's a fetch-att list.
    // Individual fetch-atts like BODY[HEADER.FIELDS (From)] contain
    // parentheses only after brackets, not at the top level.
    if trimmed.starts_with('(') {
        return Err(crate::Error::InvalidInput(
            "NOTIFY MessageNew fetch-att must be a single item, not a \
             parenthesized list  -  use separate vector elements for each \
             fetch-att (RFC 5465 Section 8)"
                .into(),
        ));
    }
    // Delegate balanced-delimiter validation to the general helper.
    validate_non_empty_fetch_items(trimmed, "NOTIFY fetch-att")
}

fn validate_non_empty_fetch_items(items: &str, context: &str) -> Result<(), crate::Error> {
    let trimmed = items.trim();
    if trimmed.is_empty() {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must contain at least one message data item \
             (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
        )));
    }

    if let Some(inner) = trimmed.strip_prefix('(') {
        let inner = inner.strip_suffix(')').ok_or_else(|| {
            crate::Error::InvalidInput(format!(
                "{context} must use balanced parentheses for fetch-att list \
                 syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
            ))
        })?;
        if inner.trim().is_empty() {
            return Err(crate::Error::InvalidInput(format!(
                "{context} must contain at least one message data item \
                 (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
            )));
        }
    }

    let mut in_quote = false;
    let mut escaped = false;
    let mut bracket_depth = 0u32;
    let mut angle_depth = 0u32;
    let mut paren_depth = 0u32;

    for ch in trimmed.chars() {
        if in_quote {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_quote = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_quote = true,
            '[' => bracket_depth += 1,
            ']' => {
                if bracket_depth == 0 {
                    return Err(crate::Error::InvalidInput(format!(
                        "{context} must use balanced quotes and delimiters for fetch-att \
                         syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
                    )));
                }
                bracket_depth -= 1;
            }
            '<' if bracket_depth == 0 => angle_depth += 1,
            '>' if bracket_depth == 0 => {
                if angle_depth == 0 {
                    return Err(crate::Error::InvalidInput(format!(
                        "{context} must use balanced quotes and delimiters for fetch-att \
                         syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
                    )));
                }
                angle_depth -= 1;
            }
            '(' if bracket_depth == 0 && angle_depth == 0 => paren_depth += 1,
            ')' if bracket_depth == 0 && angle_depth == 0 => {
                if paren_depth == 0 {
                    return Err(crate::Error::InvalidInput(format!(
                        "{context} must use balanced quotes and delimiters for fetch-att \
                         syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
                    )));
                }
                paren_depth -= 1;
            }
            _ => {}
        }
    }

    if in_quote || bracket_depth != 0 || angle_depth != 0 || paren_depth != 0 {
        return Err(crate::Error::InvalidInput(format!(
            "{context} must use balanced quotes and delimiters for fetch-att \
             syntax (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
        )));
    }

    Ok(())
}

/// Encode an arbitrary mailbox name string for the wire.
///
/// INBOX is case-normalized per RFC 3501 Section 5.1.
/// Non-ASCII characters are MUTF-7 encoded when `utf8` is false,
/// per RFC 3501 Section 5.1.3.
///
/// Used for LIST/LSUB reference and pattern arguments, NOTIFY filter
/// mailboxes, and any other site that carries a decoded mailbox name
/// as `&str` rather than [`MailboxName`].
pub(crate) fn encode_mailbox_str(name: &str, utf8: bool) -> String {
    // RFC 3501 Section 5.1: INBOX is case-insensitive.
    if name.eq_ignore_ascii_case("INBOX") {
        return "INBOX".to_owned();
    }
    // RFC 3501 Section 5.1 / 5.1.1: check for INBOX as hierarchical prefix.
    // The exact special name is only the case-insensitive "INBOX"; child
    // mailboxes use a single hierarchy separator after that token. Restrict
    // the heuristic to ASCII separator characters so names like "Inboxé"
    // are preserved as distinct mailboxes.
    if name.len() > 5 && name.is_char_boundary(5) {
        let prefix = &name[..5];
        let sep = name[5..].chars().next();
        if prefix.eq_ignore_ascii_case("INBOX")
            && sep.is_some_and(|ch| ch.is_ascii() && !ch.is_ascii_alphanumeric())
        {
            let child = &name[5..];
            let encoded_child = if utf8 {
                child.to_owned()
            } else {
                crate::codec::utf7::encode_utf7(child)
            };
            return format!("INBOX{encoded_child}");
        }
    }
    if utf8 {
        name.to_owned()
    } else {
        crate::codec::utf7::encode_utf7(name)
    }
}

/// Encode a [`MailboxName`] for the wire.
///
/// Replay server octets in their original mode; caller names use canonical encoding.
pub(super) fn encode_mailbox_name(name: &MailboxName, utf8: bool) -> Result<Vec<u8>, crate::Error> {
    let bytes = match name.wire_bytes(utf8) {
        Some(bytes) => bytes.to_vec(),
        None if !name.can_reencode() => {
            // Lenient display decoding is not reversible. A mode change must
            // refuse this address instead of silently selecting another mailbox.
            return Err(crate::Error::InvalidInput(
                "mailbox wire identity cannot be preserved in this encoding mode".into(),
            ));
        }
        None => encode_mailbox_str(name.as_str(), utf8).into_bytes(),
    };
    // A mailbox string cannot carry NUL. Refuse here rather than relying on
    // the general string writer's command-final validation.
    if bytes.contains(&0) {
        return Err(crate::Error::InvalidInput(
            "mailbox wire name contains NUL".into(),
        ));
    }
    Ok(bytes)
}

pub(super) fn encode_mailbox_argument(
    name: &crate::types::MailboxArgument,
    utf8: bool,
) -> Result<Vec<u8>, crate::Error> {
    encode_mailbox_name(&name.0, utf8)
}

/// Encode a command that takes a single mailbox argument as a quoted-or-literal string.
///
/// Used for SELECT, EXAMINE, CREATE, DELETE, SUBSCRIBE, UNSUBSCRIBE
/// (RFC 3501 Sections 6.3.1-6.3.7 / RFC 9051 Sections 6.3.1-6.3.7).
pub(super) fn encode_mailbox_cmd(
    w: &mut CommandWriter,
    tag: &str,
    cmd: &str,
    mailbox: impl AsRef<[u8]>,
    utf8: bool,
) {
    w.raw(tag.as_bytes());
    w.raw(b" ");
    w.raw(cmd.as_bytes());
    w.raw(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    w.string(mailbox.as_ref(), utf8);
    w.raw(b"\r\n");
}

// Known-sequence-set validation has moved to `SequenceSet::new_known()` in
// `crate::types::validated` (RFC 7162 Section 3.2.5.2).

/// Encode SELECT or EXAMINE with optional CONDSTORE/QRESYNC parameters.
///
/// Without parameters: `tag SELECT mailbox\r\n` (RFC 3501 Section 6.3.1).
/// With CONDSTORE: `tag SELECT mailbox (CONDSTORE)\r\n` (RFC 7162 Section 3.1.1).
/// With QRESYNC: `tag SELECT mailbox (QRESYNC (uidvalidity modseq [known-uids [seq-match-data]]))\r\n`
/// (RFC 7162 Section 3.2.5.2).
#[allow(clippy::too_many_arguments)]
pub(super) fn encode_select_or_examine(
    w: &mut CommandWriter,
    tag: &str,
    cmd: &str,
    mailbox: impl AsRef<[u8]>,
    condstore: bool,
    qresync: Option<&QresyncParams>,
    utf8: bool,
) -> Result<(), crate::Error> {
    w.raw(tag.as_bytes());
    w.raw(b" ");
    w.raw(cmd.as_bytes());
    w.raw(b" ");
    // RFC 6855 Section 3: UTF-8 in quoted strings when UTF8=ACCEPT is active.
    w.string(mailbox.as_ref(), utf8);

    if condstore && qresync.is_none() {
        // RFC 7162 Section 3.1.1: SELECT mailbox (CONDSTORE)
        w.raw(b" (CONDSTORE)");
    }

    if let Some(params) = qresync {
        // RFC 7162 Section 3.2.5.2:
        // SELECT mailbox (QRESYNC (uidvalidity modseq [known-uids [seq-match-data]]))
        // seq-match-data = "(" known-sequence-set SP known-uid-set ")"
        // RFC 3501 Section 9: uidvalidity = nz-number (must be non-zero).
        if params.uid_validity == 0 {
            return Err(crate::Error::InvalidInput(
                "QRESYNC uid_validity must be non-zero (nz-number per RFC 3501 Section 9)".into(),
            ));
        }
        // mod_seq is mod-sequence-value per RFC 7162 Section 7 (>= 1, <= i64::MAX)
        validate_mod_sequence_value(params.mod_seq, "QRESYNC mod_seq")?;
        w.raw(b" (QRESYNC (");
        w.raw(params.uid_validity.to_string().as_bytes());
        w.raw(b" ");
        w.raw(params.mod_seq.to_string().as_bytes());
        if let Some(known_uids) = &params.known_uids {
            // Reject CRLF to prevent command injection (RFC 3501 Section 2.2).
            validate_no_crlf(known_uids, "QRESYNC known-uids")?;
            // RFC 7162 Section 3.2.5.2: "*" and "$" are not allowed in known-uids.
            // Validate via SequenceSet::new_known() which enforces this.
            crate::types::SequenceSet::new_known(known_uids.as_str())?;
            w.raw(b" ");
            w.raw(known_uids.as_bytes());
        }
        if let Some((seq_set, uid_set)) = &params.seq_match_data {
            // RFC 7162 Section 7: The ABNF places [SP known-uids] and
            // [SP seq-match-data] at the same nesting level, but
            // seq-match-data is semantically meaningless without known-uids
            // (it maps message numbers to UIDs), so we require it.
            if params.known_uids.is_none() {
                return Err(crate::Error::InvalidInput(
                    "seq-match-data requires known-uids (RFC 7162 Section 3.2.5.2)".into(),
                ));
            }
            // Reject CRLF to prevent command injection (RFC 3501 Section 2.2).
            validate_no_crlf(seq_set, "QRESYNC seq-match-data sequence set")?;
            validate_no_crlf(uid_set, "QRESYNC seq-match-data UID set")?;
            // RFC 7162 Section 3.2.5.2: "*" and "$" are not allowed in
            // known-sequence-set or known-uid-set.
            crate::types::SequenceSet::new_known(seq_set.as_str())?;
            crate::types::SequenceSet::new_known(uid_set.as_str())?;
            w.raw(b" (");
            w.raw(seq_set.as_bytes());
            w.raw(b" ");
            w.raw(uid_set.as_bytes());
            w.raw(b")");
        }
        w.raw(b"))");
    }

    w.raw(b"\r\n");
    Ok(())
}

/// Encode UID FETCH command (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5).
///
/// When `changed_since` is `Some`, appends the CHANGEDSINCE modifier
/// per RFC 7162 Section 3.1.4.1.
///
/// When `vanished` is `true`, appends the VANISHED modifier to the
/// fetch modifier list per RFC 7162 Section 3.2.6.  VANISHED requires
/// `changed_since` to be set (the modifier list must include CHANGEDSINCE).
pub(super) fn encode_uid_fetch(
    w: &mut CommandWriter,
    tag: &str,
    sequence_set: &str,
    items: &str,
    changed_since: Option<u64>,
    vanished: bool,
) -> Result<(), crate::Error> {
    // RFC 7162 Section 3.2.6: VANISHED requires CHANGEDSINCE in the same
    // fetch modifier list.
    if vanished && changed_since.is_none() {
        return Err(crate::Error::InvalidInput(
            "VANISHED modifier requires CHANGEDSINCE per RFC 7162 Section 3.2.6".into(),
        ));
    }
    // Reject CRLF in raw parameters to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(sequence_set, "UID FETCH sequence set")?;
    validate_no_crlf(items, "UID FETCH items")?;
    validate_non_empty_fetch_items(items, "UID FETCH items")?;
    w.raw(tag.as_bytes());
    w.raw(b" UID FETCH ");
    w.raw(sequence_set.as_bytes());
    w.raw(b" ");
    w.raw(items.as_bytes());
    if let Some(modseq) = changed_since {
        encode_changedsince_modifier(w, modseq, vanished)?;
    }
    w.raw(b"\r\n");
    Ok(())
}

/// Encode the `store-att-flags` portion shared by STORE and UID STORE
/// (RFC 3501 Section 6.4.6 / RFC 9051 Section 6.4.6).
///
/// `store-att-flags = (["+" / "-"] "FLAGS" [".SILENT"]) SP (flag-list / (flag *(SP flag)))`
///
/// RFC 3501 Section 9: STORE uses the `flag` production, which excludes
/// `\Recent` (read-only, server-set) and `\*` (permanent-flag wildcard,
/// only valid in PERMANENTFLAGS responses).
fn encode_store_flags(
    w: &mut CommandWriter,
    operation: crate::types::StoreOperation,
    flags: &[crate::types::Flag],
) -> Result<(), crate::Error> {
    w.raw(b" ");
    match operation {
        crate::types::StoreOperation::Add => w.raw(b"+FLAGS"),
        crate::types::StoreOperation::Remove => w.raw(b"-FLAGS"),
        crate::types::StoreOperation::Replace => w.raw(b"FLAGS"),
        crate::types::StoreOperation::AddSilent => w.raw(b"+FLAGS.SILENT"),
        crate::types::StoreOperation::RemoveSilent => w.raw(b"-FLAGS.SILENT"),
        crate::types::StoreOperation::ReplaceSilent => w.raw(b"FLAGS.SILENT"),
    }
    let valid_flags = validate_and_filter_flags(flags, "STORE")?;
    // RFC 3501 Section 9 / RFC 9051 Section 9: `flag-list = "(" [flag *(SP flag)] ")"`.
    // An empty flag-list is valid ABNF and meaningful for Replace/ReplaceSilent:
    // `FLAGS ()` clears all flags. For Add/Remove variants, adding or removing
    // zero flags is semantically useless and likely a caller bug  -  reject early.
    if valid_flags.is_empty() {
        match operation {
            crate::types::StoreOperation::Replace | crate::types::StoreOperation::ReplaceSilent => {
                // FLAGS () / FLAGS.SILENT ()  -  clear all flags.
                // Valid per RFC 3501 Section 6.4.6 flag-list ABNF.
            }
            _ => {
                return Err(crate::Error::InvalidInput(
                    "STORE +FLAGS/-FLAGS requires at least one flag; adding or \
                     removing zero flags is a no-op (RFC 3501 Section 6.4.6)"
                        .into(),
                ));
            }
        }
    }
    w.raw(b" (");
    for (i, flag) in valid_flags.iter().enumerate() {
        if i > 0 {
            w.raw(b" ");
        }
        w.raw(flag.as_imap_str().as_bytes());
    }
    w.raw(b")\r\n");
    Ok(())
}

/// Encode UID EXPUNGE command (RFC 4315 UIDPLUS Section 2.1).
pub(super) fn encode_uid_expunge(
    w: &mut CommandWriter,
    tag: &str,
    sequence_set: &str,
) -> Result<(), crate::Error> {
    // Reject CRLF in sequence set to prevent command injection (RFC 3501 Section 2.2).
    validate_no_crlf(sequence_set, "UID EXPUNGE sequence set")?;
    w.raw(tag.as_bytes());
    w.raw(b" UID EXPUNGE ");
    w.raw(sequence_set.as_bytes());
    w.raw(b"\r\n");
    Ok(())
}
