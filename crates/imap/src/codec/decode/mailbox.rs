//! Mailbox name decoding for IMAP response parsing.
//!
//! RFC 3501 Section 5.1.3 defines Modified UTF-7 encoding for mailbox names.
//! RFC 9051 Section 5.1 / RFC 6855 specify raw UTF-8 when `UTF8=ACCEPT` or
//! `IMAP4rev2` is active.
//!
//! This module provides the single entry point for decoding wire-form mailbox
//! bytes into [`MailboxName`] at parse time, so the decoder's output is always
//! in decoded user-facing form, with the original address retained separately.

use crate::codec::utf7::decode_utf7;
use crate::types::validated::MailboxName;

/// Decode a mailbox name from wire bytes into a [`MailboxName`].
///
/// When `utf8_mode` is true (UTF8=ACCEPT per RFC 6855 Section 3, or `IMAP4rev2`
/// per RFC 9051 Section 5.1), the bytes are raw UTF-8 and are converted
/// lossily. Otherwise, Modified UTF-7 decoding is applied per
/// RFC 3501 Section 5.1.3.
///
/// Returns a [`MailboxName`] via `MailboxName::from_wire`. This is the
/// codec-internal constructor that bypasses user-input validation  -  the
/// bytes have already been parsed from a protocol response.
pub(in crate::codec) fn decode_mailbox_from_wire(bytes: &[u8], utf8_mode: bool) -> MailboxName {
    let decoded = if utf8_mode {
        // RFC 9051 Section 5.1 / RFC 6855 Section 3: mailbox names are
        // Net-Unicode (UTF-8). Lossy conversion handles any non-conformant
        // bytes without panicking.
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        // RFC 3501 Section 5.1.3: Modified UTF-7 encoding.
        decode_utf7(bytes)
    };
    MailboxName::from_wire(decoded, bytes, utf8_mode)
}
