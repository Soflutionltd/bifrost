//! APPEND / MULTIAPPEND encoder.
//!
//! APPEND is an ordinary [`Command`](crate::types::Command), so the driver
//! encodes it like every other command: at send time, against the connection
//! state it owns, so every decision the wire bytes bake in (mailbox encoding,
//! the RFC 6855 `UTF8 (` wrapper, RFC 7888 `+` markers, literal8 eligibility)
//! is made from live state and cannot go stale while the command is queued.

use super::{
    CommandWriter, encode_mailbox_argument, validate_and_filter_flags, validate_append_datetime,
};
use crate::codec::encode::{EncodeOptions, LiteralForm};
use crate::types::response::Capability;
use crate::types::{AppendMessage, Flag};

/// Which syntax carries one message body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyForm {
    /// Classic `{n}` literal (RFC 3501 Section 4.3).
    Classic,
    /// `~{n}` literal8, required for bodies containing NUL (RFC 3516
    /// Section 4.4 / RFC 9051 Section 9).
    Literal8,
    /// `UTF8 (~{n}` ... `)`, the RFC 6855 Section 4 data extension, used for
    /// every message once `UTF8=ACCEPT` is enabled.
    Utf8Literal8,
}

/// Encode APPEND (`multi == false`, exactly one message) or MULTIAPPEND
/// (`multi == true`, RFC 3502).
///
/// Everything is decided from `opts`, which the driver builds from live
/// protocol state immediately before sending:
///
/// - **Validation.** MULTIAPPEND requires the MULTIAPPEND capability; the
///   message list must be non-empty (and a single APPEND carries exactly one);
///   flags and dates are validated; outside the RFC 6855 wrapper a body
///   containing NUL requires an advertised BINARY. Failures are the crate's
///   own [`crate::Error`] variants (`MissingCapability`, `InvalidInput`,
///   `InvalidAppendDate`, ...), which the account layer maps.
/// - **Mailbox.** INBOX-normalized and modified UTF-7 unless `opts.utf8_mode`
///   (`UTF8=ACCEPT` or active `IMAP4rev2`), where the raw UTF-8 name is used
///   and quoted-string form is preferred over a literal (RFC 6855 Section 3).
/// - **Flags.** `\Recent` and `\*` are not valid in APPEND and are dropped
///   (RFC 3501 Section 9); custom keywords must be ATOM-CHARs.
/// - **Body form.** `UTF8 (~{n}` under `UTF8=ACCEPT` (the group is closed with
///   `)` after the body), `~{n}` for a NUL-bearing body, `{n}` otherwise. The
///   wrapper is chosen FIRST: RFC 6855 Section 4 defines its literal8 payload
///   independently of the BINARY advertisement, so a NUL body inside it needs
///   no BINARY.
/// - **Marker.** Chosen by the [`CommandWriter`], the one place that rule
///   lives: non-synchronizing per RFC 7888 (LITERAL+ any size, LITERAL- and
///   rev2 up to 4096 octets), and both literal8 forms additionally need BINARY
///   and a non-rev2 connection (RFC 7888 Section 6, RFC 9051 Section 9).
///
/// Bodies are never copied: each message's `Bytes` is carried by reference.
pub(crate) fn encode_append(
    w: &mut CommandWriter,
    tag: &str,
    mailbox: &crate::types::MailboxArgument,
    messages: &[AppendMessage],
    multi: bool,
    opts: &EncodeOptions,
) -> Result<(), crate::Error> {
    if multi && !opts.has_capability(&Capability::MultiAppend) {
        // RFC 3502 Section 3.
        return Err(crate::Error::MissingCapability("MULTIAPPEND".into()));
    }
    if messages.is_empty() {
        return Err(crate::Error::InvalidInput(
            "MULTIAPPEND requires at least one message".into(),
        ));
    }
    if !multi && messages.len() != 1 {
        return Err(crate::Error::InvalidInput(
            "APPEND carries exactly one message; use MULTIAPPEND for several".into(),
        ));
    }

    let utf8 = opts.utf8_mode;
    let wrapper = opts.utf8_accept_enabled();

    for (i, msg) in messages.iter().enumerate() {
        if i == 0 {
            // Tag + APPEND + mailbox (RFC 3502 Section 3). RFC 6855 Section 3:
            // under UTF-8 mode the mailbox may be a quoted string.
            w.raw(tag.as_bytes());
            w.raw(b" APPEND ");
            w.string(&encode_mailbox_argument(mailbox, utf8)?, utf8);
        } else if wrapper {
            // RFC 6855 Section 4: close the previous message's UTF8 group.
            w.raw(b")");
        }

        // Flags (RFC 3501 Section 6.3.11).
        let flags: Vec<&Flag> = validate_and_filter_flags(&msg.flags, "APPEND")?;
        if !flags.is_empty() {
            w.raw(b" (");
            for (n, flag) in flags.iter().enumerate() {
                if n > 0 {
                    w.raw(b" ");
                }
                w.raw(flag.as_imap_str().as_bytes());
            }
            w.raw(b")");
        }

        // Internal date: validated against the date-time production, then a
        // quoted string (RFC 3501 Section 9).
        if let Some(date) = msg.date.as_deref() {
            validate_append_datetime(date)?;
            w.raw(b" ");
            w.string(date.as_bytes(), false);
        }

        match body_form(msg, wrapper, opts)? {
            BodyForm::Utf8Literal8 => {
                w.raw(b" UTF8 (");
                w.literal_bytes(msg.data.clone(), LiteralForm::Literal8);
            }
            BodyForm::Literal8 => {
                w.raw(b" ");
                w.literal_bytes(msg.data.clone(), LiteralForm::Literal8);
            }
            BodyForm::Classic => {
                w.raw(b" ");
                w.literal_bytes(msg.data.clone(), LiteralForm::Classic);
            }
        }
    }

    // RFC 6855 Section 4 closes the last UTF8 group; RFC 3501 Section 2.2
    // ends the command line.
    w.raw(if wrapper {
        b")\r\n".as_slice()
    } else {
        b"\r\n"
    });
    Ok(())
}

/// Choose the syntax for one message body.
fn body_form(
    msg: &AppendMessage,
    wrapper: bool,
    opts: &EncodeOptions,
) -> Result<BodyForm, crate::Error> {
    if wrapper {
        return Ok(BodyForm::Utf8Literal8);
    }
    if !msg.data.contains(&0) {
        return Ok(BodyForm::Classic);
    }
    // RFC 3516 Section 4.4: NUL octets need literal8, which needs BINARY.
    // Active IMAP4rev2 does not stand in for it: RFC 9051 Appendix B folds
    // in only the FETCH side of BINARY, not its APPEND extension, and the
    // capability authority answers BINARY accordingly (advertised only).
    if opts.has_capability(&Capability::Binary) {
        Ok(BodyForm::Literal8)
    } else {
        // The body is expressible; the server lacks the capability that
        // would carry it, so this is `Unsupported`, not a malformed request.
        Err(crate::Error::MissingCapability(
            "APPEND data containing NUL requires BINARY literal8 support \
             (RFC 3516 Section 4.4)"
                .into(),
        ))
    }
}
