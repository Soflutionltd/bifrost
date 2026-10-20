//! Shared RFC 5322 / MIME assembler for the send and draft paths.
//!
//! This is the single message serializer the protocol crates compose
//! through. It operates entirely in [`Address`](crate::compose::Address)
//! space - no protocol-specific mailbox type is constructed here - so
//! every account crate (Google, IMAP, and any future SMTP-backed
//! provider) shares one body-building path instead of maintaining a
//! divergent hand-rolled serializer each.
//!
//! It emits:
//! - text-only, html-only, or `multipart/alternative` body entities,
//! - a `multipart/mixed` wrapper when inline attachments are present,
//! - `Content-Disposition: inline` / `attachment` per
//!   [`AttachmentInline::inline`],
//! - RFC 2047 encoded-word display names and subjects,
//! - a deterministic, controlled-domain `Message-ID` (the sender's
//!   domain, never a leaked machine hostname).
//!
//! The result is the raw RFC 5322 octets plus a [`SubmissionEnvelope`]
//! (reverse path + recipient set, still in `Address` space). The narrow
//! `Address -> protocol envelope` conversion is the consuming crate's
//! responsibility at its own transport boundary.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::compose::{Address, AttachmentInline, SendRequest};
use crate::error::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, DiagnosticText,
    RequestCause, RequestErrorKind,
};

/// Bifrost's own reverse-path / recipient pair, in `Address` space.
///
/// Distinct from any protocol `Envelope`: the consuming crate converts
/// the bare addr-specs here into its transport's envelope at the send
/// boundary. The `from` reverse path is the resolved sender; the
/// `recipients` set is `to` union `cc` union `bcc`.
#[derive(Debug, Clone)]
pub struct SubmissionEnvelope {
    /// MAIL FROM reverse path.
    pub from: Address,
    /// RCPT TO recipient set (`to` + `cc` + `bcc`, in that order).
    pub recipients: Vec<Address>,
}

/// Outcome of serializing a [`SendRequest`].
#[derive(Debug, Clone)]
pub struct RenderedMessage {
    /// Reverse path + recipient set for the transport envelope.
    pub envelope: SubmissionEnvelope,
    /// The serialized RFC 5322 octets for transmission. The `Bcc:` header
    /// is omitted so blind recipients are never disclosed on the wire.
    pub raw: Vec<u8>,
    /// The serialized RFC 5322 octets for the sender's own Sent-folder
    /// copy. Identical to [`raw`](Self::raw) except it retains the `Bcc:`
    /// header: the sender's Sent copy is their record of who was blind
    /// copied, so it keeps the blind-recipient list that the transmitted
    /// body drops. `None` when the request carries no Bcc recipients (then
    /// the two bodies would be identical and the caller reuses `raw`).
    pub sent_copy: Option<Vec<u8>>,
    /// The `Message-ID` value (without angle brackets) emitted in the
    /// message headers. Callers use this as a stable fallback id when no
    /// server-side id is available.
    pub message_id: String,
}

/// Serialize a [`SendRequest`] into RFC 5322 octets plus the submission
/// envelope.
///
/// Resolves `from` from `request.from` or `default_from`, validates that
/// at least one recipient is present, rejects uploaded-attachment handles
/// (those belong to the cloud-attachment lane), and emits a
/// controlled-domain `Message-ID`.
///
/// `request.scheduled` and `request.send_as` are **not** consumed here:
/// they are routing/timing dimensions (the API path and the delivery
/// time), not body content. Each consumer validates and applies them
/// against its own transport before calling this assembler - by the time
/// a `SendRequest` reaches here those fields have already been honored or
/// rejected (`validate_scheduled`, the `send_as` capability gate). This
/// serializer deliberately ignores them rather than silently dropping a
/// schedule/send-as the caller forgot to act on.
///
/// # Errors
///
/// - `Unsupported(AttachmentUpload)` when `attachments_uploaded` is
///   non-empty.
/// - `Request(Malformed)` when `to`, `cc`, and `bcc` are all empty.
pub fn send_request_to_rfc5322(
    request: &SendRequest,
    default_from: &Address,
) -> Result<RenderedMessage, AccountError> {
    if !request.attachments_uploaded.is_empty() {
        return Err(unsupported(AccountOperation::AttachmentUpload));
    }
    if request.to.is_empty() && request.cc.is_empty() && request.bcc.is_empty() {
        return Err(malformed("send requires at least one recipient"));
    }

    let from = request.from.clone().unwrap_or_else(|| default_from.clone());
    let message_id = generate_message_id(&from);

    let mut composed = ComposedMessage {
        from: Some(&from),
        to: &request.to,
        cc: &request.cc,
        bcc: &request.bcc,
        reply_to: &request.reply_to,
        subject: request.subject.as_deref(),
        body_text: request.body_text.as_deref(),
        body_html: request.body_html.as_deref(),
        attachments_inline: &request.attachments_inline,
        in_reply_to: request.in_reply_to.as_deref(),
        references: &request.references,
        message_id: Some(&message_id),
        include_bcc_header: false,
        // Read-receipt request targets the resolved sender (the party who
        // wants the disposition notification). `None` when unset so the
        // header is absent by default.
        disposition_notification_to: request.request_read_receipt.then_some(&from),
    };

    // The transmitted body drops `Bcc:` so blind recipients are never
    // disclosed on the wire. The sender's own Sent copy retains it: it is
    // the sender's record of who was blind copied. Only render the second
    // variant when there is actually a Bcc to preserve.
    let raw = render_rfc5322(&composed);
    let sent_copy = if request.bcc.is_empty() {
        None
    } else {
        composed.include_bcc_header = true;
        Some(render_rfc5322(&composed))
    };

    let mut recipients =
        Vec::with_capacity(request.to.len() + request.cc.len() + request.bcc.len());
    recipients.extend(request.to.iter().cloned());
    recipients.extend(request.cc.iter().cloned());
    recipients.extend(request.bcc.iter().cloned());

    Ok(RenderedMessage {
        envelope: SubmissionEnvelope { from, recipients },
        raw,
        sent_copy,
        message_id,
    })
}

/// Borrowed field set the renderer consumes. Protocol crates that hold
/// their own composition struct (e.g. Google's `MailDocument`) build one
/// of these to share the body-building path.
pub struct ComposedMessage<'a> {
    /// `From` header. `None` omits the header (a draft without a resolved
    /// sender); the send path always supplies one.
    pub from: Option<&'a Address>,
    pub to: &'a [Address],
    pub cc: &'a [Address],
    pub bcc: &'a [Address],
    pub reply_to: &'a [Address],
    pub subject: Option<&'a str>,
    pub body_text: Option<&'a str>,
    pub body_html: Option<&'a str>,
    pub attachments_inline: &'a [AttachmentInline],
    pub in_reply_to: Option<&'a str>,
    pub references: &'a [String],
    /// `Message-ID` to emit, without angle brackets. `None` omits the
    /// header (e.g. a draft serializer that does not mint one).
    pub message_id: Option<&'a str>,
    /// Emit a `Bcc:` header in the body. Submission strips Bcc, so the
    /// send path leaves this `false`; the draft path sets it `true` so a
    /// saved draft preserves the blind-recipient list.
    pub include_bcc_header: bool,
    /// `Disposition-Notification-To` target (RFC 8098). `Some(addr)`
    /// emits the header so the recipient's MUA is asked to confirm
    /// display; the send path sets this to the resolved `from` address
    /// when `SendRequest::request_read_receipt` is set. `None` omits the
    /// header (the default, and the draft path which carries no
    /// read-receipt request).
    pub disposition_notification_to: Option<&'a Address>,
}

/// Render the message headers and MIME body into raw RFC 5322 octets.
///
/// This performs no recipient validation: it serializes whatever fields
/// it is given. Callers that need the "at least one recipient" guard
/// (the send path) check before calling.
#[must_use]
pub fn render_rfc5322(msg: &ComposedMessage<'_>) -> Vec<u8> {
    let mut headers = Vec::new();
    if let Some(from) = msg.from {
        push_header(&mut headers, "From", &format_address(from));
    }
    push_address_header(&mut headers, "To", msg.to);
    push_address_header(&mut headers, "Cc", msg.cc);
    if msg.include_bcc_header {
        push_address_header(&mut headers, "Bcc", msg.bcc);
    } else if msg.to.is_empty() && msg.cc.is_empty() {
        // Bcc-only transmitted copy: the wire body drops `Bcc:`, leaving
        // no destination header at all. RFC 5322 §3.6.3 permits a group
        // with an empty member list as the address-header placeholder;
        // emitting `To: undisclosed-recipients:;` keeps strict MTAs that
        // reject a header-less destination from balking, without
        // disclosing the blind recipients.
        headers.push("To: undisclosed-recipients:;".to_string());
    }
    push_address_header(&mut headers, "Reply-To", msg.reply_to);
    if let Some(notify) = msg.disposition_notification_to {
        // RFC 8098 read-receipt request. The target is an address, so it
        // goes through the same RFC 2047 / phrase encoding as From/To.
        push_header(
            &mut headers,
            "Disposition-Notification-To",
            &format_address(notify),
        );
    }
    if let Some(subject) = msg.subject {
        // `encode_header_value` already sanitizes its input and, for a
        // non-ASCII value, emits CRLF folds between encoded-words. Those
        // folds must survive verbatim, so this goes through the
        // pre-folded push that does not re-run `sanitize_header` (which
        // would flatten the folding CRLFs back into one over-long line).
        push_header_prefolded(&mut headers, "Subject", &encode_header_value(subject));
    }
    if let Some(in_reply_to) = msg.in_reply_to {
        push_header(&mut headers, "In-Reply-To", in_reply_to);
    }
    if !msg.references.is_empty() {
        push_header(&mut headers, "References", &msg.references.join(" "));
    }
    if let Some(message_id) = msg.message_id {
        push_header(&mut headers, "Message-ID", &format!("<{message_id}>"));
    }
    headers.push("MIME-Version: 1.0".to_string());

    let entity = render_entity(msg);
    format!("{}\r\n{entity}\r\n", headers.join("\r\n")).into_bytes()
}

fn render_entity(msg: &ComposedMessage<'_>) -> String {
    if msg.attachments_inline.is_empty() {
        return render_body_entity(msg);
    }

    let boundary = boundary("mixed");
    let mut out = String::new();
    out.push_str(&format!(
        "Content-Type: multipart/mixed; boundary=\"{boundary}\"\r\n\r\n"
    ));
    push_part(&mut out, &boundary, &render_body_entity(msg));
    for attachment in msg.attachments_inline {
        push_part(&mut out, &boundary, &render_attachment_entity(attachment));
    }
    out.push_str(&format!("--{boundary}--\r\n"));
    out
}

fn render_body_entity(msg: &ComposedMessage<'_>) -> String {
    match (msg.body_text, msg.body_html) {
        (Some(text), Some(html)) => {
            let boundary = boundary("alternative");
            let mut out = String::new();
            out.push_str(&format!(
                "Content-Type: multipart/alternative; boundary=\"{boundary}\"\r\n\r\n"
            ));
            push_part(&mut out, &boundary, &render_text_entity("text/plain", text));
            push_part(&mut out, &boundary, &render_text_entity("text/html", html));
            out.push_str(&format!("--{boundary}--\r\n"));
            out
        }
        (Some(text), None) => render_text_entity("text/plain", text),
        (None, Some(html)) => render_text_entity("text/html", html),
        (None, None) => render_text_entity("text/plain", ""),
    }
}

fn render_text_entity(mime: &str, body: &str) -> String {
    format!(
        "Content-Type: {mime}; charset=UTF-8\r\nContent-Transfer-Encoding: base64\r\n\r\n{}\r\n",
        wrap_base64(body.as_bytes())
    )
}

fn render_attachment_entity(attachment: &AttachmentInline) -> String {
    let disposition = if attachment.inline {
        "inline"
    } else {
        "attachment"
    };
    let filename = quote_param(&attachment.filename);
    // Stamp `Content-ID: <cid>` (RFC 2045 §7) so a `cid:` reference in the
    // HTML body resolves to this part. The value is sanitized and wrapped
    // in angle brackets; absent when the consumer set no content_id.
    let content_id = match attachment
        .content_id
        .as_deref()
        .map(sanitize_header)
        .filter(|cid| !cid.is_empty())
    {
        Some(cid) => format!("Content-ID: <{cid}>\r\n"),
        None => String::new(),
    };
    format!(
        "Content-Type: {}; name=\"{filename}\"\r\nContent-Disposition: {disposition}; filename=\"{filename}\"\r\n{content_id}Content-Transfer-Encoding: base64\r\n\r\n{}\r\n",
        quote_param(&attachment.mime),
        wrap_base64(&attachment.data)
    )
}

fn push_part(out: &mut String, boundary: &str, part: &str) {
    out.push_str(&format!("--{boundary}\r\n"));
    out.push_str(part);
}

fn push_header(headers: &mut Vec<String>, name: &str, value: &str) {
    let value = sanitize_header(value);
    if value.trim().is_empty() {
        return;
    }
    headers.push(format!("{name}: {value}"));
}

/// Push a header whose value is already sanitized and may carry RFC 5322
/// folding (`CRLF SP`) that must not be flattened. Used for RFC 2047
/// encoded-word values, which `encode_header_value` produces folded and
/// CRLF-safe; running `sanitize_header` over them would collapse the
/// folds into one over-long line.
fn push_header_prefolded(headers: &mut Vec<String>, name: &str, value: &str) {
    if value.is_empty() {
        return;
    }
    headers.push(format!("{name}: {value}"));
}

fn push_address_header(headers: &mut Vec<String>, name: &str, addresses: &[Address]) {
    if addresses.is_empty() {
        return;
    }
    let rendered = addresses
        .iter()
        .map(format_address)
        .collect::<Vec<_>>()
        .join(", ");
    push_header(headers, name, &rendered);
}

/// Format an [`Address`] as an RFC 5322 header value: `Name <addr>` with
/// the display name RFC 2047 encoded when non-ASCII, or the bare
/// addr-spec when there is no name.
#[must_use]
pub fn format_address(address: &Address) -> String {
    let email = sanitize_header(&address.address);
    match address
        .name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        Some(name) => format!("{} <{email}>", encode_phrase(name)),
        None => email,
    }
}

/// Encode an address display name as an RFC 5322 phrase.
///
/// Matches the bifrost-smtp builder (and `reference/smtp.md`): an
/// atom-shaped ASCII name is emitted as bare phrase text (no quoting),
/// which avoids the DKIM-breaking quoted-string rewrites some relays
/// perform; an ASCII name carrying specials that an atom may not hold is
/// quoted; a non-ASCII name becomes RFC 2047 encoded-words. Producing the
/// bare form when possible keeps the two send paths (IMAP and Google)
/// emitting identical From/To phrases.
fn encode_phrase(value: &str) -> String {
    let sanitized = sanitize_header(value);
    if !sanitized.is_ascii() {
        return encode_header_value(&sanitized);
    }
    if is_atom_phrase(&sanitized) {
        sanitized
    } else {
        format!(
            "\"{}\"",
            sanitized.replace('\\', "\\\\").replace('"', "\\\"")
        )
    }
}

/// An RFC 5322 phrase that needs no quoting: at least one `atext`
/// character and nothing outside `atext` plus the inter-atom whitespace
/// (space / tab). Mirrors `bifrost-smtp`'s `is_valid_phrase`.
fn is_atom_phrase(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| is_atext(b) || b == b' ' || b == b'\t')
        && value.bytes().any(is_atext)
}

/// RFC 5322 `atext` (the unquoted-atom character set).
fn is_atext(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'/'
                | b'='
                | b'?'
                | b'^'
                | b'_'
                | b'`'
                | b'{'
                | b'|'
                | b'}'
                | b'~'
        )
}

fn encode_header_value(value: &str) -> String {
    let sanitized = sanitize_header(value);
    if sanitized.is_ascii() {
        sanitized
    } else {
        encode_words(&sanitized)
    }
}

/// Maximum length of a single RFC 2047 encoded-word, including the
/// `=?UTF-8?B?` prefix and `?=` suffix. RFC 2047 caps an encoded-word at
/// 75 octets so a folded header line of `space + encoded-word` stays
/// within the 76-octet practical line budget (well under the 998-octet
/// hard limit).
const ENCODED_WORD_MAX: usize = 75;

/// Length of the `=?UTF-8?B?` ... `?=` framing around the base64 payload.
const ENCODED_WORD_OVERHEAD: usize = "=?UTF-8?B?".len() + "?=".len();

/// Encode a non-ASCII value as one or more RFC 2047 base64 encoded-words,
/// each capped at [`ENCODED_WORD_MAX`] octets and split on UTF-8
/// character boundaries (a multibyte scalar is never split across two
/// words, per RFC 2047 §5). Multiple words are joined by CRLF + a single
/// space (folding white space), so the assembled header obeys both the
/// per-word cap and the 998-octet line limit.
fn encode_words(value: &str) -> String {
    // base64 expands 3 input octets to 4 output octets; the payload
    // budget is the per-word cap minus the framing, rounded down to a
    // multiple of 3 so each chunk encodes without padding ambiguity.
    let payload_budget = (ENCODED_WORD_MAX - ENCODED_WORD_OVERHEAD) / 4 * 3;

    let mut words: Vec<String> = Vec::new();
    let mut chunk_start = 0;
    let mut chunk_len = 0;
    let bytes = value.as_bytes();
    let mut idx = 0;
    while idx < value.len() {
        // Advance to the next char boundary so we never split a scalar.
        let char_len = utf8_char_len(bytes[idx]);
        if chunk_len + char_len > payload_budget && chunk_len > 0 {
            words.push(encode_one_word(&value[chunk_start..idx]));
            chunk_start = idx;
            chunk_len = 0;
        }
        chunk_len += char_len;
        idx += char_len;
    }
    if chunk_start < value.len() {
        words.push(encode_one_word(&value[chunk_start..]));
    }
    // Folding white space between encoded-words: CRLF + a single space.
    // Encoded-words carry no internal whitespace, so the linear-white-
    // space between them is semantically elided by the parser per §6.2.
    words.join("\r\n ")
}

fn encode_one_word(segment: &str) -> String {
    format!("=?UTF-8?B?{}?=", STANDARD.encode(segment.as_bytes()))
}

/// Length in bytes of the UTF-8 scalar that starts at this lead byte.
fn utf8_char_len(lead: u8) -> usize {
    match lead {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

fn sanitize_header(value: &str) -> String {
    value.replace(['\r', '\n'], " ").trim().to_string()
}

/// Sanitize and escape a value destined for a quoted MIME parameter
/// (`name="..."` / `filename="..."`). CRLF is stripped by
/// [`sanitize_header`]; embedded `"` and `\` are backslash-escaped so a
/// quote in the value cannot prematurely close the parameter and malform
/// the surrounding header.
fn quote_param(value: &str) -> String {
    sanitize_header(value)
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn wrap_base64(bytes: &[u8]) -> String {
    let encoded = STANDARD.encode(bytes);
    encoded
        .as_bytes()
        .chunks(76)
        .map(|chunk| std::str::from_utf8(chunk).unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\r\n")
}

/// Mint a multipart boundary token.
///
/// The token contains `-` characters, which are absent from the base64
/// alphabet (`A-Za-z0-9+/=`). Every part this assembler emits is
/// `Content-Transfer-Encoding: base64`, so a boundary delimiter line can
/// never collide with part content - the collision the boundary is
/// supposed to be checked against is structurally impossible here. If a
/// future part type ever emits non-base64 content (e.g. quoted-printable
/// or 7bit), reintroduce a content-scan-and-regenerate guard before
/// trusting this token.
fn boundary(kind: &str) -> String {
    static NEXT_BOUNDARY: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_BOUNDARY.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("bifrost-{kind}-{nanos}-{sequence}")
}

/// Mint a `Message-ID` value (without angle brackets) whose right-hand
/// side is the sender's domain. Never derives from a machine hostname,
/// so the value can safely surface to the consumer as a stable id.
///
/// The domain is taken from `from.address` and filtered to the
/// `dot-atom` character set so the minted id is a well-formed RFC 5322
/// `msg-id` regardless of what the caller put in the address: the value
/// returned to the consumer in [`RenderedMessage::message_id`] is the
/// same sanitized string emitted in the header, never raw caller input.
fn generate_message_id(from: &Address) -> String {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let domain = from
        .address
        .rsplit_once('@')
        .map(|(_, domain)| sanitize_msg_id_domain(domain))
        .filter(|domain| !domain.is_empty())
        .unwrap_or_else(|| "localhost".to_string());
    format!("bifrost.{nanos}.{sequence}@{domain}")
}

/// Reduce a candidate domain to the RFC 5322 `dot-atom` character set
/// (`atext` plus `.`), dropping anything else (spaces, `<`, `>`, `@`,
/// quotes, control characters). Keeps a hostile or sloppy `from.address`
/// from leaking odd characters into the returned `Message-ID`.
fn sanitize_msg_id_domain(domain: &str) -> String {
    domain
        .trim()
        .bytes()
        .filter(|&b| is_atext(b) || b == b'.')
        .map(char::from)
        .collect()
}

fn unsupported(operation: AccountOperation) -> AccountError {
    // Rendering has no account identity. The consuming account stamps its
    // protocol at the boundary, just as it does for malformed render inputs.
    AccountError::unsupported(operation, crate::Protocol::Unknown)
}

// Provider-neutral: this assembler is shared (IMAP today, any future
// SMTP-backed provider tomorrow), so it must not assert a protocol. The
// operation is `Send` because `send_request_to_rfc5322` is the send path;
// the consuming crate stamps its protocol via the account-boundary
// context as the error propagates.
fn malformed(detail: &str) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail),
        }),
    )
    .operation(AccountOperation::Send)
    .try_build()
    .expect("valid account error classification")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn addr(name: Option<&str>, address: &str) -> Address {
        Address {
            name: name.map(ToOwned::to_owned),
            address: address.to_owned(),
        }
    }

    #[test]
    fn send_request_builds_multipart_mixed_with_alternative() {
        let request = SendRequest {
            to: vec![addr(Some("Hei There"), "hei@example.com")],
            subject: Some("Happy new year".to_owned()),
            body_text: Some("plain body".to_owned()),
            body_html: Some("<p>html body</p>".to_owned()),
            attachments_inline: vec![AttachmentInline {
                filename: "a.txt".to_owned(),
                mime: "text/plain".to_owned(),
                data: Bytes::from_static(b"hello"),
                inline: false,
                content_id: None,
            }],
            ..SendRequest::default()
        };
        let default_from = addr(None, "me@sender.test");
        let rendered = send_request_to_rfc5322(&request, &default_from).expect("renders");
        let raw = String::from_utf8(rendered.raw).expect("utf8");

        assert!(raw.contains("Content-Type: multipart/mixed;"));
        assert!(raw.contains("Content-Type: multipart/alternative;"));
        assert!(raw.contains("Content-Disposition: attachment; filename=\"a.txt\""));
        // From defaults to default_from when request.from is None.
        assert!(raw.contains("From: me@sender.test"));
        // Atom-shaped ASCII display name is emitted bare (no quoting), to
        // match the bifrost-smtp builder and avoid DKIM-breaking relay
        // rewrites of a quoted-string phrase.
        assert!(raw.contains("To: Hei There <hei@example.com>"));
        // Message-ID domain is the sender's domain, never a hostname.
        assert!(rendered.message_id.ends_with("@sender.test"));
        assert!(raw.contains(&format!("Message-ID: <{}>", rendered.message_id)));
        // No Bcc header on the send path.
        assert!(!raw.contains("Bcc:"));
    }

    #[test]
    fn send_request_envelope_unions_recipients() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            cc: vec![addr(None, "b@example.com")],
            bcc: vec![addr(None, "c@example.com")],
            body_text: Some("hi".to_owned()),
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        let recipients: Vec<&str> = rendered
            .envelope
            .recipients
            .iter()
            .map(|a| a.address.as_str())
            .collect();
        assert_eq!(
            recipients,
            ["a@example.com", "b@example.com", "c@example.com"]
        );
        assert_eq!(rendered.envelope.from.address, "me@sender.test");

        // The transmitted body strips Bcc; the sender's Sent copy retains
        // it as their record of who was blind copied.
        let wire = String::from_utf8(rendered.raw).expect("utf8");
        assert!(!wire.to_ascii_lowercase().contains("bcc:"));
        let sent =
            String::from_utf8(rendered.sent_copy.expect("bcc present -> sent copy")).expect("utf8");
        assert!(sent.contains("Bcc: c@example.com"));
    }

    #[test]
    fn send_request_without_bcc_has_no_separate_sent_copy() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            body_text: Some("hi".to_owned()),
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        // No Bcc -> the two bodies would be identical, so no separate copy.
        assert!(rendered.sent_copy.is_none());
    }

    #[test]
    fn send_request_rejects_uploaded_attachments() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            attachments_uploaded: vec![crate::compose::AttachmentHandle("h".to_owned())],
            ..SendRequest::default()
        };
        let err = send_request_to_rfc5322(&request, &addr(None, "me@sender.test"))
            .expect_err("uploaded handles rejected");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::AttachmentUpload)
        );
    }

    #[test]
    fn send_request_requires_recipient() {
        let request = SendRequest {
            body_text: Some("hi".to_owned()),
            ..SendRequest::default()
        };
        let err = send_request_to_rfc5322(&request, &addr(None, "me@sender.test"))
            .expect_err("no recipient");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }

    #[test]
    fn encode_phrase_atom_shaped_is_bare() {
        assert_eq!(encode_phrase("Jane Doe"), "Jane Doe");
        // atext specials are allowed bare; whitespace between atoms too.
        assert_eq!(encode_phrase("a-b_c'd"), "a-b_c'd");
    }

    #[test]
    fn encode_phrase_quotes_ascii_with_specials() {
        // '.' '(' ')' ',' ':' are not atext, so the phrase is quoted.
        assert_eq!(encode_phrase("Doe, Jane"), "\"Doe, Jane\"");
        assert_eq!(encode_phrase("a (b)"), "\"a (b)\"");
        // Embedded quotes and backslashes are escaped, never left to break
        // out of the quoted string.
        assert_eq!(encode_phrase("a\"b"), "\"a\\\"b\"");
        assert_eq!(encode_phrase("a\\b"), "\"a\\\\b\"");
    }

    #[test]
    fn encode_phrase_non_ascii_uses_encoded_words() {
        let encoded = encode_phrase("Café");
        assert!(encoded.starts_with("=?UTF-8?B?"));
        assert!(encoded.ends_with("?="));
    }

    #[test]
    fn encoded_word_respects_75_octet_cap() {
        // A long non-ASCII subject must split into multiple encoded-words,
        // each <= 75 octets, never one unbounded blob.
        let subject = "é".repeat(200);
        let encoded = encode_header_value(&subject);
        let words: Vec<&str> = encoded.split("\r\n ").collect();
        assert!(words.len() > 1, "long value must split into multiple words");
        for word in &words {
            assert!(
                word.len() <= ENCODED_WORD_MAX,
                "word {word:?} is {} octets, over the 75 cap",
                word.len()
            );
            assert!(word.starts_with("=?UTF-8?B?") && word.ends_with("?="));
            // Each word's payload must base64-decode to valid UTF-8: a
            // multibyte scalar was never split across a word boundary.
            let payload = &word["=?UTF-8?B?".len()..word.len() - "?=".len()];
            let bytes = STANDARD.decode(payload).expect("valid base64");
            std::str::from_utf8(&bytes).expect("word is a whole-scalar boundary");
        }
    }

    #[test]
    fn encoded_words_round_trip_to_original() {
        let subject = "Møte på fjellet \u{2603} über alles é".repeat(5);
        let encoded = encode_header_value(&subject);
        let reassembled: String = encoded
            .split("\r\n ")
            .map(|word| {
                let payload = &word["=?UTF-8?B?".len()..word.len() - "?=".len()];
                String::from_utf8(STANDARD.decode(payload).expect("base64")).expect("utf8")
            })
            .collect();
        assert_eq!(reassembled, subject);
    }

    #[test]
    fn folded_subject_obeys_998_octet_line_limit() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            subject: Some("é".repeat(500)),
            body_text: Some("hi".to_owned()),
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        let raw = String::from_utf8(rendered.raw).expect("utf8");
        for line in raw.split("\r\n") {
            assert!(
                line.len() <= 998,
                "line over 998 octets: {} bytes",
                line.len()
            );
        }
    }

    #[test]
    fn attachment_filename_quotes_are_escaped() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            body_text: Some("hi".to_owned()),
            attachments_inline: vec![AttachmentInline {
                filename: "evil\"; x=\"y.txt".to_owned(),
                mime: "text/plain".to_owned(),
                data: Bytes::from_static(b"x"),
                inline: false,
                content_id: None,
            }],
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        let raw = String::from_utf8(rendered.raw).expect("utf8");
        // The embedded quote is escaped, so the injected `x="y` param
        // cannot break out of the filename value.
        assert!(raw.contains("filename=\"evil\\\"; x=\\\"y.txt\""));
    }

    #[test]
    fn message_id_strips_odd_domain_chars() {
        let from = addr(None, "user@ex ample<>.com");
        let rendered = send_request_to_rfc5322(
            &SendRequest {
                to: vec![addr(None, "a@example.com")],
                body_text: Some("hi".to_owned()),
                from: Some(from),
                ..SendRequest::default()
            },
            &addr(None, "me@sender.test"),
        )
        .expect("renders");
        // The returned id is the sanitized form actually in the header.
        assert!(!rendered.message_id.contains(' '));
        assert!(!rendered.message_id.contains('<'));
        assert!(!rendered.message_id.contains('>'));
        assert!(rendered.message_id.ends_with("@example.com"));
        let raw = String::from_utf8(rendered.raw).expect("utf8");
        assert!(raw.contains(&format!("Message-ID: <{}>", rendered.message_id)));
    }

    #[test]
    fn inline_attachment_content_id_emits_header() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            body_html: Some("<img src=\"cid:logo@x\">".to_owned()),
            attachments_inline: vec![AttachmentInline {
                filename: "logo.png".to_owned(),
                mime: "image/png".to_owned(),
                data: Bytes::from_static(b"img"),
                inline: true,
                content_id: Some("logo@x".to_owned()),
            }],
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        let raw = String::from_utf8(rendered.raw).expect("utf8");
        // The Content-ID is wrapped in angle brackets so the cid: URI in
        // the HTML body resolves to this part.
        assert!(raw.contains("Content-ID: <logo@x>"));
    }

    #[test]
    fn no_content_id_emits_no_header() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            body_text: Some("hi".to_owned()),
            attachments_inline: vec![AttachmentInline {
                filename: "a.txt".to_owned(),
                mime: "text/plain".to_owned(),
                data: Bytes::from_static(b"x"),
                inline: false,
                content_id: None,
            }],
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        let raw = String::from_utf8(rendered.raw).expect("utf8");
        assert!(!raw.contains("Content-ID:"));
    }

    #[test]
    fn read_receipt_request_emits_disposition_notification_header() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            from: Some(addr(Some("Me"), "me@sender.test")),
            body_text: Some("hi".to_owned()),
            request_read_receipt: true,
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "default@sender.test")).expect("renders");
        let raw = String::from_utf8(rendered.raw).expect("utf8");
        // The receipt target is the resolved From address.
        assert!(raw.contains("Disposition-Notification-To: Me <me@sender.test>"));
    }

    #[test]
    fn no_read_receipt_request_omits_header() {
        let request = SendRequest {
            to: vec![addr(None, "a@example.com")],
            body_text: Some("hi".to_owned()),
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        let raw = String::from_utf8(rendered.raw).expect("utf8");
        assert!(!raw.contains("Disposition-Notification-To:"));
    }

    #[test]
    fn bcc_only_send_emits_undisclosed_recipients() {
        let request = SendRequest {
            bcc: vec![addr(None, "secret@example.com")],
            body_text: Some("hi".to_owned()),
            ..SendRequest::default()
        };
        let rendered =
            send_request_to_rfc5322(&request, &addr(None, "me@sender.test")).expect("renders");
        let raw = String::from_utf8(rendered.raw).expect("utf8");
        assert!(raw.contains("To: undisclosed-recipients:;"));
        // The blind recipient is still not disclosed on the wire.
        assert!(!raw.to_ascii_lowercase().contains("bcc:"));
        assert!(!raw.contains("secret@example.com"));
    }
}
