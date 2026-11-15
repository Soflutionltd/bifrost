//! Byte-preserving submission-envelope parsing and draft header finalization.

use crate::account::error::{pim_malformed, stamp_imap_protocol};
use bifrost_types::{AccountError, AccountOperation};

#[derive(Debug)]
pub(super) struct ParsedDraft {
    pub(super) envelope: bifrost_types::SubmissionEnvelope,
    pub(super) body: Vec<u8>,
    pub(super) message_id: String,
}

/// Parse a saved draft's RFC 5322 headers into a submission envelope and
/// strip the `Bcc:` header from the transmitted body. To/Cc/Bcc become
/// the recipient set; shared header resolution chooses the reverse path. The
/// draft's own `Message-ID` is the fallback id.
pub(super) fn parse_draft_for_submission(
    raw: &[u8],
    default_from: &bifrost_types::Address,
    op: AccountOperation,
) -> Result<ParsedDraft, AccountError> {
    // Operate on bytes end-to-end: the transmitted body must be the
    // verbatim draft octets. Decoding through `from_utf8_lossy` would
    // turn any 8-bit / binary octet into U+FFFD on the wire while the
    // Sent-folder APPEND keeps the original bytes - a silent divergence
    // between what was sent and what is recorded. Header parsing only
    // needs the names and the address-list / message-id values, which
    // are ASCII-structured; we decode those lossily for parsing but
    // never feed the decoded form back into the transmitted bytes.
    let (header_end, body_start) = split_header_body(raw);
    let header_block = &raw[..header_end];

    let headers = bifrost_types::mime::HeaderMap::parse(header_block);

    let (from, recipients) = headers
        .submission_envelope()
        .map_err(|detail| pim_malformed(op, detail))?;
    let from = from.unwrap_or_else(|| default_from.clone());

    let message_id = headers
        .message_ids("message-id")
        .into_iter()
        .next()
        .unwrap_or_else(|| bifrost_types::mime::generate_message_id(&from));

    let body = strip_bcc_header(header_block, &raw[body_start..]);

    Ok(ParsedDraft {
        envelope: bifrost_types::SubmissionEnvelope { from, recipients },
        body,
        message_id,
    })
}

/// Locate the header/body split of a raw RFC 5322 message, tolerating
/// mixed CRLF / bare-LF line endings and a header-only draft (no blank
/// line). Returns `(header_end, body_start)` byte offsets: the header
/// block is `raw[..header_end]` and the body is `raw[body_start..]`. The
/// separator blank line itself sits between the two and is dropped. For
/// a header-only draft both offsets are `raw.len()` (empty body).
fn split_header_body(raw: &[u8]) -> (usize, usize) {
    // Find the empty line that ends the header block. We scan for the
    // first line terminator (`\r\n` or bare `\n`) that is immediately
    // followed by another line terminator. `header_end` is the offset
    // just *after* the first terminator (so the last header line keeps
    // its own ending); `body_start` is just *after* the second
    // terminator (so the blank separator line is dropped). Mixed CRLF /
    // bare-LF endings are tolerated on both sides.
    let mut i = 0;
    while i < raw.len() {
        // Length of a terminator starting at `j`, if any.
        let term_len = |j: usize| -> Option<usize> {
            match raw.get(j) {
                Some(b'\r') if raw.get(j + 1) == Some(&b'\n') => Some(2),
                Some(b'\n') => Some(1),
                _ => None,
            }
        };
        if let Some(first) = term_len(i) {
            let header_end = i + first;
            if let Some(second) = term_len(header_end) {
                return (header_end, header_end + second);
            }
        }
        i += 1;
    }
    (raw.len(), raw.len())
}

/// Reassemble the message bytes with every `Bcc:` header line removed so
/// blind recipients are not disclosed in the transmitted message.
///
/// Byte-exact: header and body octets are copied verbatim (no UTF-8
/// round-trip), so an 8-bit / binary body survives unchanged. Only the
/// dropped `Bcc:` lines and the synthesized header/body separator touch
/// line endings; the separator matches the header block's own ending
/// (CRLF unless the draft is bare-LF) so a uniform-LF draft is not
/// emitted with a mixed CRLF separator.
fn strip_bcc_header(header_block: &[u8], body: &[u8]) -> Vec<u8> {
    rewrite_header_block(header_block, body, b"bcc", None)
}

/// Reassemble a message with every field named `drop` (case-insensitive,
/// continuation lines included) removed from the header block and, when
/// `prepend` is set, that complete field line inserted as the first
/// header. Same byte-exact and separator policy as [`strip_bcc_header`].
fn rewrite_header_block(
    header_block: &[u8],
    body: &[u8],
    drop: &[u8],
    prepend: Option<&str>,
) -> Vec<u8> {
    let uses_crlf = header_block.windows(2).any(|w| w == b"\r\n") || !header_block.contains(&b'\n');
    let sep: &[u8] = if uses_crlf { b"\r\n" } else { b"\n" };

    let mut kept = Vec::with_capacity(header_block.len() + body.len() + 4);
    if let Some(field) = prepend {
        kept.extend_from_slice(field.as_bytes());
        kept.extend_from_slice(sep);
    }
    let mut skipping = false;
    for line in split_inclusive_lf(header_block) {
        let trimmed = trim_leading_crlf(line);
        let is_continuation = line.first() == Some(&b' ') || line.first() == Some(&b'\t');
        if !is_continuation {
            skipping = header_name_is(trimmed, drop);
        }
        if !skipping {
            kept.extend_from_slice(line);
        }
    }
    // Ensure the header block ends with a line terminator, then add the
    // blank-line separator, then the verbatim body.
    if !kept.ends_with(b"\n") {
        kept.extend_from_slice(sep);
    }
    kept.extend_from_slice(sep);
    kept.extend_from_slice(body);
    kept
}

/// Split on `\n`, keeping the terminator on each line (the byte analogue
/// of `str::split_inclusive('\n')`), and skipping a trailing empty slice.
fn split_inclusive_lf(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    std::iter::from_fn(move || {
        if start >= bytes.len() {
            return None;
        }
        let rest = &bytes[start..];
        match rest.iter().position(|&b| b == b'\n') {
            Some(idx) => {
                let line = &rest[..=idx];
                start += idx + 1;
                Some(line)
            }
            None => {
                let line = rest;
                start = bytes.len();
                Some(line)
            }
        }
    })
}

fn trim_leading_crlf(line: &[u8]) -> &[u8] {
    let mut s = line;
    while let [first, rest @ ..] = s
        && (*first == b'\r' || *first == b'\n')
    {
        s = rest;
    }
    s
}

/// True when a header line's field name (before the first `:`) is `name`,
/// case-insensitive, ignoring surrounding whitespace.
fn header_name_is(line: &[u8], name: &[u8]) -> bool {
    match line.iter().position(|&b| b == b':') {
        Some(idx) => line[..idx].trim_ascii().eq_ignore_ascii_case(name),
        None => false,
    }
}

/// Replace a saved draft's `Date:` with the send time, at finalization.
///
/// A draft's `Date` is its save time (or absent, for a draft another
/// client wrote); RFC 5322 section 3.6.1 makes `Date` the moment the
/// message is ready for delivery, which is now. Every `Date` field is
/// dropped and one fresh field is inserted first, so the transmitted body
/// and the Sent copy built from the result agree.
///
/// Returns the draft unchanged when it carries a `DKIM-Signature`: the
/// signer committed to those header bytes, and replacing or inserting a
/// `Date` would invalidate the signature, so a signed draft keeps its own
/// `Date` (stale or missing). Raw sends never come through here.
///
/// # Errors
///
/// `Request(Malformed)` when the header block opens with a continuation
/// line: inserting a field in front would absorb it into the new `Date`.
pub(super) fn restamp_draft_date(
    raw: &[u8],
    now: jiff::Timestamp,
    op: AccountOperation,
) -> Result<Vec<u8>, AccountError> {
    let (header_end, body_start) = split_header_body(raw);
    let header_block = &raw[..header_end];
    if split_inclusive_lf(header_block).any(|line| {
        !matches!(line.first(), Some(b' ' | b'\t'))
            && header_name_is(trim_leading_crlf(line), b"dkim-signature")
    }) {
        return Ok(raw.to_vec());
    }
    if matches!(header_block.first(), Some(b' ' | b'\t')) {
        return Err(pim_malformed(
            op,
            "draft header block opens with a continuation line",
        ));
    }
    let date = bifrost_types::format_rfc5322_date(now).map_err(stamp_imap_protocol)?;
    Ok(rewrite_header_block(
        header_block,
        &raw[body_start..],
        b"date",
        Some(&format!("Date: {date}")),
    ))
}

#[cfg(test)]
mod tests;
