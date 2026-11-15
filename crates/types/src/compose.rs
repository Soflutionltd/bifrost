//! Mail composition: send, attachments, drafts.
//!
//! `SendRequest` is the top-level shape `send_message` accepts.
//! `DraftPatch` is the partial-update shape used by the draft
//! lifecycle. `AttachmentHandle` and `DraftHandle` are opaque
//! provider-minted ids returned by their respective primitives.

use bytes::Bytes;

use crate::ids::{ObjectId, PrincipalId};

/// Opaque handle to a server-side uploaded attachment.
///
/// `attachment_upload` returns one of these; subsequent
/// `draft_update` calls reference attachments by handle rather than
/// re-uploading the bytes. The handle is protocol-owned bytes;
/// consumers treat it as opaque.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttachmentHandle(pub String);

/// Opaque handle to a server-side draft.
///
/// `draft_create` returns one; `draft_update`, `draft_discard`, and
/// `draft_send` accept one. Distinct from `ObjectId` so the type
/// system catches mix-ups between "this is a draft handle" and "this
/// is a message id".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DraftHandle(pub String);

impl From<DraftHandle> for ObjectId {
    fn from(d: DraftHandle) -> Self {
        Self(d.0)
    }
}

/// A single named recipient. `name` is `None` when only the address
/// was supplied; consumers render `name <address>` when both are
/// present and `<address>` when only the address is.
///
/// Not `#[non_exhaustive]` so both consumers (`SendRequest` /
/// `DraftPatch` builders) and protocol Account impls (returning
/// `Identity::reply_to`) can construct it freely.
#[derive(Debug, Clone)]
pub struct Address {
    pub name: Option<String>,
    pub address: String,
}

impl Address {
    /// Convenience constructor for an address-only entry.
    #[must_use]
    pub fn bare(address: impl Into<String>) -> Self {
        Self {
            name: None,
            address: address.into(),
        }
    }

    /// Convenience constructor for a named entry.
    #[must_use]
    pub fn named(name: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            address: address.into(),
        }
    }
}

/// In-band attachment for `SendRequest` / `DraftPatch`. For larger
/// payloads use `attachment_upload` and reference by
/// `AttachmentHandle` instead.
///
/// Not `#[non_exhaustive]` because consumers construct it on the
/// send / draft request side.
#[derive(Debug, Clone)]
pub struct AttachmentInline {
    /// Filename presented in the MIME `Content-Disposition`.
    pub filename: String,
    // Preserve the declared wire media type. parse_content_type is a tolerant
    // inbound parser: malformed input becomes text/plain, which would silently
    // change an outbound declaration or hide inbound evidence.
    /// Content-Type.
    pub mime: String,
    /// Raw payload bytes.
    pub data: Bytes,
    /// Whether the consumer wants `Content-Disposition: inline`
    /// (preview-in-body) rather than `attachment`.
    pub inline: bool,
    /// `Content-ID` (without angle brackets) to stamp on the rendered
    /// MIME part. `Some(cid)` makes a `cid:<cid>` reference in the HTML
    /// body resolve to this part, so an inline image survives the send;
    /// `None` emits no `Content-ID` header. The renderer wraps the value
    /// in angle brackets (`Content-ID: <cid>`) per RFC 2045 §7.
    pub content_id: Option<String>,
}

/// Send-as / send-on-behalf-of identity for a shared or delegate
/// mailbox. `None` on `SendRequest::send_as` is an ordinary personal
/// send. Distinct from `SendRequest::from` (the author/From *header*):
/// `send_as` selects the *sending mailbox / API path*, which on some
/// providers (Microsoft Graph) is a routing dimension separate from
/// the From header. Gated by `PimMethodSupport.send_as`; a request
/// carrying `Some(..)` is never silently sent from the authenticated
/// user's own mailbox. See `SendRequest::send_as` for how a rejection
/// is classified on each branch of that flag.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendAs {
    /// Send **as** the shared mailbox: the mailbox is both the author
    /// (`From`) and the sender. The authenticated user must hold
    /// Send-As rights on it; the recipient sees only the shared
    /// mailbox. `As` *forces* `from` to the mailbox - any
    /// `SendRequest::from` the consumer set is overridden, because the
    /// whole contract of `As` is "the recipient sees only the shared
    /// mailbox." A consumer that wants `from` to diverge from the
    /// sending mailbox wants `OnBehalfOf`, not `As`.
    As(PrincipalId),
    /// Send **on behalf of** the shared mailbox: when the consumer does not
    /// supply `from`, the mailbox is the author (`From`); the authenticated
    /// user is the sender (`Sender`). A provider may preserve an explicit
    /// consumer `from`. The recipient sees "user on behalf of mailbox".
    OnBehalfOf(PrincipalId),
}

impl SendAs {
    /// The shared mailbox identity, regardless of mode - the
    /// `/users/{id}` routing key.
    #[must_use]
    pub fn mailbox(&self) -> &PrincipalId {
        match self {
            Self::As(m) | Self::OnBehalfOf(m) => m,
        }
    }
}

/// Top-level shape `Account::send_message` accepts.
///
/// `identity` selects which sending identity to attach (relevant for
/// accounts with multiple `Identity` rows from `identities_list`).
/// Most fields are `Option` so a minimal send can omit reply-to,
/// cc, bcc, in-reply-to, etc. without nesting one builder inside
/// another.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SendRequest {
    /// Sending identity. `None` selects the account's default.
    pub identity: Option<IdentityId>,
    /// From line. `None` uses the identity's address.
    pub from: Option<Address>,
    /// To recipients. At least one of `to`, `cc`, or `bcc` must be
    /// non-empty for the protocol to accept the send; this is
    /// checked by `validate_recipients` at the protocol boundary.
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    /// Reply-To header.
    pub reply_to: Vec<Address>,
    /// Subject line.
    pub subject: Option<String>,
    /// Plain-text body. Mutually permissive with `body_html`;
    /// providing both produces a multipart/alternative.
    pub body_text: Option<String>,
    /// HTML body.
    pub body_html: Option<String>,
    /// In-line attachments.
    pub attachments_inline: Vec<AttachmentInline>,
    /// Pre-uploaded attachment handles (from `attachment_upload`).
    pub attachments_uploaded: Vec<AttachmentHandle>,
    /// In-Reply-To and References headers (for threading).
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    /// Whether the server should append the sent message to the
    /// Sent folder. `None` leaves the choice to the protocol's
    /// default. Honored by JMAP via `EmailSubmission/set`'s
    /// `onSuccessUpdateEmail`; Gmail and Graph append on send by
    /// default and ignore this when `Some(false)`; IMAP uses the
    /// configured `bifrost-smtp` transport plus an APPEND.
    pub save_to_sent: Option<bool>,
    /// Requested server-side send time. `None` sends immediately.
    /// `Some(t)` requests delayed delivery where the provider supports
    /// it (`capabilities().pim_methods.scheduled_send`); on providers
    /// that do not, a `Some(t)` request is rejected with
    /// `Unsupported(Send)` rather than silently sent now. `t` is an
    /// absolute wall-clock instant; the provider boundary validates it
    /// is in the future and within the provider's max-delay window.
    pub scheduled: Option<std::time::SystemTime>,
    /// Send-as / send-on-behalf-of a shared or delegate mailbox.
    /// `None` is an ordinary personal send. See `SendAs`.
    ///
    /// This is the governing statement of how a `Some(..)` request that
    /// cannot be honored is classified. `capabilities().pim_methods.send_as`
    /// decides which of two answers the consumer gets, and the two mean
    /// different things:
    ///
    /// - `send_as == false`: the feature is absent, so `Unsupported(Send)` is
    ///   the only honest answer, and remediation is to reconfigure the account
    ///   or the provider so the feature exists. Why it is absent differs, and
    ///   the flag is what unifies them: imap and google hold no mailbox routing
    ///   table at all; graph holds an EMPTY shared-client map; jmap may hold a
    ///   populated foreign routing table while no entry in it advertises
    ///   submission, so it has ids and still has no route. In every case
    ///   answering `Request(Malformed)` would tell the consumer to correct an
    ///   argument when no argument could have worked.
    /// - `send_as == true` (graph with at least one shared mailbox
    ///   configured; jmap with a seeded, submission-capable foreign
    ///   account): the feature is present, so naming a mailbox the account
    ///   does not hold is a bad argument, not a missing capability. It is
    ///   rejected `Request(Malformed)` with `RequestCause::InvalidArgument`
    ///   carrying `field: Some("send_as.mailbox")`, which a UI can use to
    ///   point at the offending input. Remediation is to correct the
    ///   request. JMAP still answers `Unsupported(Send)` for its genuine
    ///   capability gaps on this path: a known account that does not
    ///   advertise submission, and a scheduled foreign send.
    ///
    /// Both kinds are terminal (`ClientBug` and `Unsupported` respectively),
    /// so nothing retries either; the distinction exists to tell the
    /// consumer whether to fix the request or reconfigure the feature.
    /// A `Some(..)` request is never silently downgraded to a personal send.
    ///
    /// Every backend that can reach this contract asserts it against its OWN
    /// flag, in `capability_contract_tests.rs`, so an implementation that
    /// drifts from this paragraph fails rather than agreeing with a local copy
    /// of the rule. That test found graph and jmap both answering
    /// `Request(Malformed)` while advertising `send_as == false`.
    pub send_as: Option<SendAs>,
    /// Request a read receipt (message disposition notification) for this
    /// send. `true` makes each provider ask the recipient's MUA to confirm
    /// the message was displayed: the SMTP/JMAP/IMAP/Gmail assemblers emit
    /// a `Disposition-Notification-To` header (RFC 8098) targeting the
    /// resolved `from` address, and Graph sets `isReadReceiptRequested`.
    /// Defaults to `false` so existing callers are unaffected. Whether the
    /// recipient honors the request is out of the sender's control on every
    /// provider.
    pub request_read_receipt: bool,
}

impl SendRequest {
    /// Reject a send with no To, Cc or Bcc recipients before provider I/O,
    /// retaining the calling protocol on the local refusal.
    pub fn validate_recipients(
        &self,
        protocol: crate::Protocol,
    ) -> Result<(), crate::AccountError> {
        validate_send_recipients(&self.to, &self.cc, &self.bcc, protocol)
    }
}

/// Validate resolved recipient lists. Draft patches must first be applied to
/// their stored draft: an omitted patch field does not mean an empty list.
pub fn validate_send_recipients(
    to: &[Address],
    cc: &[Address],
    bcc: &[Address],
    protocol: crate::Protocol,
) -> Result<(), crate::AccountError> {
    use crate::error::{
        AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, DiagnosticText,
        RequestCause, RequestErrorKind,
    };
    // The builder is given a fixed, valid classification; try_build cannot fail.
    #[allow(clippy::unwrap_in_result)]
    if to.is_empty() && cc.is_empty() && bcc.is_empty() {
        return Err(AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::user_safe("send requires at least one recipient"),
            }),
        )
        .operation(AccountOperation::Send)
        .protocol(protocol)
        .try_build()
        .expect("valid account error classification"));
    }
    Ok(())
}

/// Validate a requested scheduled-send instant at the provider
/// boundary. Shared so the past-instant and over-window rules are
/// identical across every native provider.
///
/// - `at <= now()` (past or present instant) -> `Request(Malformed)`.
/// - `at - now() > max_window` (when `max_window` is `Some`) ->
///   `Request(Malformed)`. Providers with no documented hard cap pass
///   `None` and rely on server rejection.
///
/// Returns `Ok(())` for a valid future instant inside the window.
///
/// # Errors
///
/// Returns `Request(Malformed)` when `at` is not strictly in the
/// future, or when it exceeds `max_window` past now.
pub fn validate_scheduled(
    at: std::time::SystemTime,
    max_window: Option<std::time::Duration>,
) -> Result<(), crate::error::AccountError> {
    use crate::error::{
        AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, RequestCause,
        RequestErrorKind,
    };

    let now = std::time::SystemTime::now();
    let malformed = |detail: &'static str| {
        AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::user_safe(detail),
            }),
        )
        .operation(crate::error::AccountOperation::Send)
        .try_build()
        .expect("valid account error classification")
    };

    let delta = match at.duration_since(now) {
        // Strictly-future requirement: `Ok(0)` (exactly now) and the
        // `Err` arm (at < now) are both rejected as past instants.
        Ok(d) if !d.is_zero() => d,
        _ => return Err(malformed("Scheduled send time must be in the future.")),
    };

    if let Some(window) = max_window
        && delta > window
    {
        return Err(malformed(
            "Scheduled send time exceeds the provider's maximum delay window.",
        ));
    }

    Ok(())
}

/// Partial-update shape used by `draft_create` and `draft_update`.
///
/// Every field is `Option`-shaped so a `draft_update` can change
/// just the body and leave the recipient list alone. Empty `Vec`s
/// in the optional collection fields (e.g. `to: Some(vec![])`)
/// explicitly clear the collection; `None` leaves it unchanged.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DraftPatch {
    pub identity: Option<IdentityId>,
    pub from: Option<Option<Address>>,
    pub to: Option<Vec<Address>>,
    pub cc: Option<Vec<Address>>,
    pub bcc: Option<Vec<Address>>,
    pub reply_to: Option<Vec<Address>>,
    pub subject: Option<Option<String>>,
    pub body_text: Option<Option<String>>,
    pub body_html: Option<Option<String>>,
    pub attachments_inline: Option<Vec<AttachmentInline>>,
    pub attachments_uploaded: Option<Vec<AttachmentHandle>>,
    pub in_reply_to: Option<Option<String>>,
    pub references: Option<Vec<String>>,
}

/// Engine-facing identity id. Distinct from `ObjectId` because
/// identities are a separate object class from messages and trying
/// to round-trip an identity id through `ObjectId` would lose the
/// signal that the id namespace is different.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdentityId(pub String);

/// What `send_message`, `send_raw_message` and `draft_send` return once the
/// provider accepted the submission.
///
/// `Ok` means the message was submitted; it never means every follow-up step
/// succeeded. The follow-ups a provider performs as separate requests after the
/// submission commits (IMAP's Sent-folder APPEND and post-send draft discard)
/// are reported here rather than through `Err`, because an `Err` after a
/// committed send would read as "send failed" and invite a duplicating resend.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SendReceipt {
    /// The id the call has always returned: what `cancel_scheduled_send` and
    /// `reschedule_send` accept for a scheduled send, otherwise the
    /// provider's message id (IMAP: the APPENDUID-derived Sent id when one
    /// exists, else an `imapmsgid1:<Message-ID>` correlation token, which is
    /// not an addressable IMAP object).
    pub id: ObjectId,
    /// What became of the sender's Sent copy.
    pub sent_copy: SentCopy,
    /// `draft_send` only: the post-send discard of the draft failed. `None`
    /// means no separately reported discard failure, not that the draft's
    /// disappearance was verified.
    pub draft_discard: Option<DraftDiscardFailure>,
    /// Envelope recipients the submission did not get accepted while at least
    /// one other recipient did (IMAP's SMTP submission reports these per RCPT
    /// / final reply). Empty means no recipient was reported as not accepted,
    /// not that delivery to every recipient was verified: JMAP, Gmail and
    /// Graph report nothing per recipient, and a relay's acceptance is not
    /// delivery (later bounces arrive as DSN mail).
    pub recipients_not_accepted: Vec<RecipientNotAccepted>,
}

impl SendReceipt {
    #[must_use]
    pub fn new(id: ObjectId, sent_copy: SentCopy) -> Self {
        Self {
            id,
            sent_copy,
            draft_discard: None,
            recipients_not_accepted: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_recipients_not_accepted(mut self, recipients: Vec<RecipientNotAccepted>) -> Self {
        self.recipients_not_accepted = recipients;
        self
    }

    /// A receipt whose Sent copy is [`SentCopy::ProviderManaged`].
    #[must_use]
    pub fn provider_managed(id: ObjectId) -> Self {
        Self::new(id, SentCopy::ProviderManaged)
    }

    #[must_use]
    pub fn with_draft_discard(mut self, failure: DraftDiscardFailure) -> Self {
        self.draft_discard = Some(failure);
        self
    }
}

/// The fate of the Sent copy of a submitted message.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SentCopy {
    /// This call filed the copy and the server acknowledged it. `id` is the
    /// copy's object id when the server identified it (IMAP APPENDUID), else
    /// `None`.
    Filed { id: Option<ObjectId> },
    /// The caller (or the account default) asked for no Sent copy, and this
    /// call filed none.
    NotRequested,
    /// This call did not file a copy. It does not claim no copy exists
    /// anywhere, only that this call wrote none.
    NotFiled {
        reason: SentCopyNotFiled,
        detail: Option<crate::Warning>,
    },
    /// The filing request failed in a way that leaves its application
    /// unknown: the copy may be in Sent. Check the Sent folder before filing
    /// it again, or it may be duplicated.
    Uncertain { detail: crate::Warning },
    /// The provider files the copy as its own server-side effect of the
    /// submission (Gmail and Graph, unconditionally). This call verifies
    /// neither the filing, nor its absence, nor any draft cleanup, and the
    /// processing may be deferred. JMAP does not report this: it reads the
    /// per-item answer of the implicit `Email/set` that RFC 8621 s7.5 runs in
    /// the same request, and reports `Filed`, `NotFiled`, `Uncertain` or
    /// `NotRequested` from it.
    ProviderManaged,
}

/// Why a Sent copy was not filed by this call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SentCopyNotFiled {
    /// No folder resolved to the Sent role.
    NoSentFolder,
    /// No connection could be obtained; no APPEND was attempted.
    NoConnection,
    /// The APPEND failed and its evidence establishes it did not apply.
    AppendFailed,
    /// The server refused to move the submitted message into Sent (JMAP: the
    /// implicit `Email/set` answered `notUpdated` for it, or failed as a
    /// method that made no change). The message keeps its pre-send
    /// placement, so it may remain in Drafts.
    MoveRefused,
}

/// A draft whose post-send discard failed. The message itself was sent.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DraftDiscardFailure {
    /// The draft `draft_send` was handed.
    pub draft: DraftHandle,
    /// The discard's own error, stamped as `DraftDiscard`: its recovery and
    /// any partial-commit record (for instance a draft marked deleted whose
    /// expunge failed) apply to `draft_discard` of `draft`, never to
    /// re-driving `draft_send`, which would send the message again.
    pub error: crate::AccountError,
}

impl DraftDiscardFailure {
    #[must_use]
    pub fn new(draft: DraftHandle, error: crate::AccountError) -> Self {
        Self { draft, error }
    }
}

/// One envelope recipient of a submitted message that the submission did not
/// get accepted, although another recipient did. It names an envelope
/// occurrence: when the same address appears twice and one occurrence was
/// accepted, that address may still have received the message.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RecipientNotAccepted {
    /// The envelope recipient (RCPT TO address).
    pub address: Address,
    /// Whether the recipient is known not to have it, or may have it.
    pub outcome: RecipientOutcome,
    /// The recipient's own error, stamped with the send call's operation. Its
    /// recovery applies to a NEW submission addressed to this recipient
    /// alone, never to re-driving the call: that would deliver again to every
    /// recipient that accepted, and after `draft_send` the draft may already
    /// be discarded. Where `outcome` is `Uncertain` it is authoritative over
    /// `error.recovery()`: do not resend to this recipient blindly, whatever
    /// the recovery says (a malformed final reply classifies as a provider
    /// contract violation yet still leaves acceptance unknown).
    pub error: crate::AccountError,
}

impl RecipientNotAccepted {
    #[must_use]
    pub fn new(address: Address, outcome: RecipientOutcome, error: crate::AccountError) -> Self {
        Self {
            address,
            outcome,
            error,
        }
    }
}

/// The fate of a [`RecipientNotAccepted`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecipientOutcome {
    /// The server answered negatively for this recipient (RCPT refused, or a
    /// negative final reply), so it did not accept the message for it.
    Failed,
    /// The recipient was accepted at RCPT but its final answer never arrived
    /// or could not be read: the message may have been accepted for it.
    Uncertain,
}

#[cfg(test)]
mod scheduled_send_tests {
    use std::time::{Duration, SystemTime};

    use super::{SendRequest, validate_scheduled};
    use crate::capabilities::PimMethodSupport;
    use crate::error::{AccountErrorKind, AccountOperation, RequestErrorKind};

    #[test]
    fn scheduled_send_default_request_is_immediate() {
        assert!(SendRequest::default().scheduled.is_none());
    }

    #[test]
    fn scheduled_send_capability_defaults_false() {
        assert!(!PimMethodSupport::default().scheduled_send);
    }

    #[test]
    fn scheduled_send_idempotency_placement() {
        // Both scheduled-send mutators are non-idempotent: an in-flight
        // drop on either must reconcile, not blind-retry. (Cancel was
        // previously treated idempotent, diverging from its RescheduleSend
        // sibling and from the wider destructive-op family.)
        assert!(!AccountOperation::CancelScheduledSend.is_idempotent());
        assert!(!AccountOperation::RescheduleSend.is_idempotent());
    }

    #[test]
    fn scheduled_send_validate_rejects_past_instant() {
        let past = SystemTime::now() - Duration::from_secs(60);
        let err = validate_scheduled(past, None).expect_err("past instant must be rejected");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
        assert_eq!(err.operation(), Some(AccountOperation::Send));
    }

    #[test]
    fn scheduled_send_validate_rejects_over_window() {
        let future = SystemTime::now() + Duration::from_secs(3600);
        let err = validate_scheduled(future, Some(Duration::from_secs(60)))
            .expect_err("over-window instant must be rejected");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }

    #[test]
    fn scheduled_send_validate_accepts_valid_future() {
        let future = SystemTime::now() + Duration::from_secs(120);
        assert!(validate_scheduled(future, Some(Duration::from_secs(3600))).is_ok());
        assert!(validate_scheduled(future, None).is_ok());
    }
}

#[cfg(test)]
mod send_as_tests {
    use super::{SendAs, SendRequest};
    use crate::capabilities::PimMethodSupport;
    use crate::ids::PrincipalId;

    #[test]
    fn send_as_defaults_none() {
        assert!(SendRequest::default().send_as.is_none());
    }

    #[test]
    fn send_as_capability_defaults_false() {
        assert!(!PimMethodSupport::default().send_as);
    }

    #[test]
    fn send_as_mailbox_accessor() {
        let mailbox = PrincipalId("shared@contoso.com".to_string());
        assert_eq!(SendAs::As(mailbox.clone()).mailbox(), &mailbox);
        assert_eq!(SendAs::OnBehalfOf(mailbox.clone()).mailbox(), &mailbox);
    }
}

#[cfg(test)]
mod recipient_validation_tests {
    use super::{Address, SendRequest};

    #[test]
    fn empty_send_is_malformed_but_each_recipient_lane_is_sufficient() {
        let error = SendRequest::default()
            .validate_recipients(crate::Protocol::Jmap)
            .expect_err("empty send");
        assert_eq!(
            *error.kind(),
            crate::AccountErrorKind::Request(crate::RequestErrorKind::Malformed)
        );
        assert_eq!(error.protocol(), Some(crate::Protocol::Jmap));
        for lane in 0..3 {
            let mut request = SendRequest::default();
            let recipients = match lane {
                0 => &mut request.to,
                1 => &mut request.cc,
                _ => &mut request.bcc,
            };
            recipients.push(Address::bare("recipient@example.test"));
            request
                .validate_recipients(crate::Protocol::Jmap)
                .expect("recipient present");
        }
    }
}
