//! Consumer trait and dispatcher for the new routing architecture.
//!
//! Consumers do NOT route. `classify` routes. Consumers only receive
//! responses that `classify` has already determined are theirs (either
//! solicited or `Either`) and they interpret them.
//!
//! The `Consumer` trait is typed (associated type `Output`).
//! `ConsumerErased` is a blanket-impl wrapper for the pipeline path,
//! which needs `Box<dyn ...>`.

use std::future::Future;
use std::pin::Pin;

use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{
    Capability, ContinuationRequest, ResponseCode, TaggedResponse, UntaggedResponse, UntaggedStatus,
};
use crate::types::validated::MailboxName;

mod auth;
mod fetch;
mod list;
mod notify;
mod quota;
mod search;
mod select;
mod thread_sort;

pub(crate) use auth::{AuthenticateSaslConsumer, AuthenticateScramConsumer, LoginConsumer};
#[cfg(test)]
pub(crate) use fetch::StreamingFetchConsumer;
pub(crate) use fetch::{
    BoundedStreamingFetchConsumer, BoundedStreamingFetchVanishedConsumer, FetchConsumer,
    FetchStreamItem, FetchVanishedConsumer, StoreConsumer,
};
pub(crate) use list::{
    ListConsumer, ListExtendedConsumer, ListStatusConsumer, LsubConsumer, StatusConsumer,
};
pub(crate) use notify::NotifySetConsumer;
pub(crate) use quota::{
    AclConsumer, ListRightsConsumer, MetadataConsumer, MyRightsConsumer, QuotaConsumer,
    QuotaRootConsumer,
};
pub(crate) use search::{
    CopyConsumer, EsearchConsumer, ExpungeConsumer, ExpungeWithCodeConsumer, MoveConsumer,
    SearchConsumer, SearchSaveConsumer,
};
pub(crate) use select::SelectConsumer;
#[cfg(test)]
pub(super) use select::{build_selected_mailbox, selected_mailbox_effective_responses};
pub(crate) use thread_sort::{SortConsumer, ThreadConsumer};

#[cfg(test)]
use crate::types::FetchResponse;

/// Typed consumer trait for a single command's response stream.
///
/// Not directly object-safe because `finalize` uses `Self::Output`
/// in its return type. `ConsumerErased` provides the object-safe
/// pipeline path via a blanket impl that erases `Output` to
/// `Box<dyn Any + Send>`.
pub(crate) trait Consumer: Send {
    type Output: Send + 'static;

    /// Remaining literal octets allowed in the next response.
    fn literal_budget(&self) -> Option<usize> {
        None
    }

    /// Interpret a response already routed here by classification. Return it
    /// when it is not part of this command's answer; the driver emits it at
    /// once, using the read frame's already-emitted critical-code flag.
    /// Consumers retain only payloads whose disposition needs completion.
    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse>;

    /// Produce the command result and any responses whose disposition could
    /// only be decided at completion (SELECT's CLOSED boundary, COPYUID,
    /// APPENDUID, or mutation evidence). Infallible so a tagged NO/BAD cannot
    /// discard retained events through an early `?`.
    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<Self::Output>;
}

/// Current downstream-capacity state for a streaming consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackpressureState {
    /// Consumer can accept the next routed response without awaiting.
    Ready,
    /// Consumer needs the driver to await downstream capacity before
    /// reading more bytes from the wire.
    NeedsCapacity,
    /// Downstream receiver is closed. The driver should keep draining the
    /// command to tagged completion and the consumer will discard data.
    Drained,
}

/// Consumer that can apply async backpressure before the driver reads.
pub(crate) trait StreamingConsumer: Consumer {
    /// Whether the driver should reserve downstream capacity before
    /// reading the next response from the wire.
    fn backpressure_state(&self) -> BackpressureState;

    /// Reserve capacity for the next delivered item.
    fn reserve_capacity(&mut self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>>;
}

/// Output of [`Consumer::finalize`].
pub(crate) struct Finalized<T> {
    /// The command's outcome.
    ///
    /// The error lives HERE rather than outside the envelope so that a failing
    /// command still carries its events - but it stays a real `Result`, not
    /// something buried in the erased `Box<dyn Any>`, because the driver reads
    /// it: `is_connection_fatal` decides whether to close the command receiver
    /// and move protocol state BEFORE the result is published. A consumer
    /// failure can be connection-fatal (`crate::error::ProviderFailure::Protocol`: SCRAM completing
    /// before the server-final verification) or not (`crate::error::ProviderFailure::Missing`:
    /// CAPABILITY with no capability data, STATUS with no matching response,
    /// which the tagged completion leaves with the framing intact), and hiding
    /// either inside `Any` would take that decision away from the driver.
    pub output: Result<T, Error>,
    /// Responses the consumer decided were not actually part of its
    /// solicited result. Dispatcher re-emits these to the event sink.
    /// Usually empty: ordinary declines already reached the driver at receipt.
    pub reclassified_as_events: Vec<UntaggedResponse>,
}

impl<T> Finalized<T> {
    /// The command succeeded.
    pub(crate) fn success(value: T, reclassified_as_events: Vec<UntaggedResponse>) -> Self {
        Self {
            output: Ok(value),
            reclassified_as_events,
        }
    }

    /// The command failed, and these are the responses that survive it.
    ///
    /// The events argument is mandatory and there is deliberately no
    /// `failure(error)` shorthand defaulting to none: an empty disposition is
    /// exactly the thing that used to happen silently, so it has to be typed
    /// out as `Vec::new()` at the site that chooses it.
    pub(crate) fn failure(error: Error, reclassified_as_events: Vec<UntaggedResponse>) -> Self {
        Self {
            output: Err(error),
            reclassified_as_events,
        }
    }
}

/// Consumer that handles `+` continuations (RFC 3501 Section7.5).
///
/// Used by AUTHENTICATE, APPEND, and future multi-round SASL.
/// The dispatcher routes continuations to `on_continuation` instead
/// of erroring on unexpected `+`.
pub(crate) trait ContinuationConsumer: Consumer {
    /// Handle a `+` continuation from the server.
    ///
    /// Returns either bytes to write back to the wire, or an abort
    /// signal with an error.
    fn on_continuation(
        &mut self,
        cont: ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error>;
}

/// What to do after a `+` continuation is delivered to a
/// [`ContinuationConsumer`].
pub(crate) enum ContinuationReply {
    /// Write these bytes to the wire and continue reading.
    Write(Vec<u8>),
}

/// Read-only view of the connection state the consumer needs.
///
/// Exposes only the fields a consumer is allowed to observe; does
/// NOT expose a reference to `ProtocolState` itself (which would
/// leak the private state module's shape).
pub(crate) struct ConsumerContext<'a> {
    // All fields are pub(in crate::connection)  -  constructed by the
    // dispatcher inside the connection module, read by consumers
    // via the accessor methods below. No field is pub.
    pub(in crate::connection) session: &'a crate::types::profile::SessionCapabilities,
    pub(in crate::connection) command_target: Option<&'a MailboxName>,
    /// The tag of the in-flight command. Used by consumers that need
    /// to correlate solicited responses (e.g., ESEARCH tag correlation
    /// per RFC 4466 search-correlator).
    pub(in crate::connection) command_tag: &'a str,
}

impl ConsumerContext<'_> {
    /// The mailbox argument of the current command, if applicable.
    pub(crate) fn command_target(&self) -> Option<&MailboxName> {
        self.command_target
    }

    /// The tag of the in-flight command (RFC 3501 Section2.2.1).
    pub(crate) fn command_tag(&self) -> &str {
        self.command_tag
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  NOOP / CAPABILITY / CHECK / ENABLE / NAMESPACE / IDLE
// ---------------------------------------------------------------------------

/// Consumer for commands that expect no solicited untagged data.
///
/// Validates the tagged response is OK and reclassifies all untagged
/// responses it received as events (they were `Either`  -  ambiguous
/// between solicited and async, but this command has no use for them).
///
/// Used by NOOP (RFC 3501 Section6.1.2), DELETE (RFC 3501 Section6.3.4),
/// RENAME (RFC 3501 Section6.3.5), SUBSCRIBE (RFC 3501 Section6.3.6),
/// UNSUBSCRIBE (RFC 3501 Section6.3.7), and similar.
#[derive(Default)]
pub(crate) struct TaggedOkConsumer {}

impl Consumer for TaggedOkConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        Some(resp)
    }

    fn finalize(self: Box<Self>, tagged: TaggedResponse, _ctx: &ConsumerContext) -> Finalized<()> {
        match tagged.require_ok() {
            Ok(_) => Finalized::success((), Vec::new()),
            Err(e) => Finalized::failure(e, Vec::new()),
        }
    }
}

/// Consumer for CAPABILITY (RFC 3501 Section6.1.1).
///
/// Accumulates the untagged CAPABILITY response. If the server places
/// capabilities in the tagged OK response code instead (permitted by
/// RFC 3501 Section6.1.1), finalize extracts them from there.
#[derive(Default)]
pub(crate) struct CapabilityConsumer {
    caps: Option<Vec<Capability>>,
}

impl Consumer for CapabilityConsumer {
    type Output = Vec<Capability>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        // RFC 3501 Section6.1.1: the server MUST respond with a CAPABILITY
        // untagged response. Stash it; reclassify everything else.
        if let UntaggedResponse::Capability(ref c) = resp {
            self.caps = Some(c.clone());
        } else {
            return Some(resp);
        }
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<Capability>> {
        let tagged = match tagged.require_ok() {
            Ok(t) => t,
            Err(e) => return Finalized::failure(e, Vec::new()),
        };

        // RFC 3501 Section6.1.1: capabilities may appear as an untagged
        // response or in the tagged OK response code.
        let caps = if let Some(c) = self.caps {
            c
        } else if let Some(ResponseCode::Capability(c)) = tagged.code {
            c
        } else {
            // Stays a real `Error` in `output`, which the driver inspects with
            // `is_connection_fatal` before publishing the result. An omission
            // after the tagged OK is not fatal: the connection stays usable.
            return Finalized::failure(
                Error::Provider(crate::error::ProviderFailure::Missing(
                    "CAPABILITY OK but no capability data in response \
                     (RFC 3501 Section 6.1.1)"
                        .into(),
                )),
                Vec::new(),
            );
        };

        Finalized::success(caps, Vec::new())
    }
}

/// Consumer for LOGOUT (RFC 3501 Section6.1.3).
///
/// Tracks whether the mandatory `* BYE` response was received.
/// RFC 3501 Section6.1.3: the server MUST send `* BYE` before the tagged OK.
#[derive(Default)]
pub(crate) struct LogoutConsumer {
    saw_bye: bool,
}

impl Consumer for LogoutConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        // RFC 3501 Section6.1.3: the server MUST send `* BYE` before the
        // tagged OK response to LOGOUT.
        if matches!(
            &resp,
            UntaggedResponse::Status {
                status: UntaggedStatus::Bye,
                ..
            }
        ) {
            self.saw_bye = true;
        }
        Some(resp)
    }

    fn finalize(self: Box<Self>, tagged: TaggedResponse, _ctx: &ConsumerContext) -> Finalized<()> {
        // Two discard sites, not one: the missing-BYE protocol error below
        // used to return ahead of `require_ok` and dropped the buffer just as
        // silently. Both arms surrender it. LOGOUT buffers everything it sees
        // (including the BYE itself, which `on_response` only flags rather
        // than consuming), and all of it is `Either` data.
        //
        // The tagged status is read FIRST. RFC 3501 Section 6.1.3 owes the
        // BYE only ahead of a tagged OK: a server that refuses LOGOUT with
        // NO/BAD has not begun closing, so its refusal is the answer and a
        // missing BYE there is no omission at all.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, Vec::new());
        }
        if !self.saw_bye {
            return Finalized::failure(
                Error::Provider(crate::error::ProviderFailure::Missing(
                    "LOGOUT: server did not send mandatory BYE \
                     (RFC 3501 Section 6.1.3)"
                        .into(),
                )),
                Vec::new(),
            );
        }
        Finalized::success((), Vec::new())
    }
}

/// Consumer for CREATE (RFC 3501 Section6.3.3) and CREATE-SPECIAL-USE (RFC 6154 Section3).
///
/// Extracts the optional `MAILBOXID` response code from the tagged OK
/// (RFC 8474 Section4.1). Servers advertising `OBJECTID` MUST include it;
/// others may omit it.
#[derive(Default)]
pub(crate) struct CreateConsumer {}

impl Consumer for CreateConsumer {
    type Output = Option<String>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        // CREATE has no untagged responses of its own. Buffer
        // everything for reclassification.
        Some(resp)
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Option<String>> {
        // CREATE holds nothing but `Either` data; surrendered on both arms.
        let tagged = match tagged.require_ok() {
            Ok(t) => t,
            Err(e) => return Finalized::failure(e, Vec::new()),
        };
        // RFC 8474 Section4.1: MAILBOXID in the tagged OK response code.
        let mailbox_id = match tagged.code {
            Some(ResponseCode::MailboxId(id)) => Some(id),
            _ => None,
        };
        Finalized::success(mailbox_id, Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  APPEND / MULTIAPPEND
// ---------------------------------------------------------------------------

/// Whether `resp` is an untagged `* OK [APPENDUID ...]`.
///
/// RFC 4315 Section 3: some servers send APPENDUID in an untagged OK rather
/// than in the tagged OK.
fn is_untagged_appenduid_ok(resp: &UntaggedResponse) -> bool {
    matches!(
        resp,
        UntaggedResponse::Status {
            status: UntaggedStatus::Ok,
            code: Some(ResponseCode::AppendUid { .. }),
            ..
        }
    )
}

/// Decide which APPENDUID code answers an APPEND, on the success path.
///
/// The tagged response's code wins when it has one. Otherwise the first
/// untagged `OK [APPENDUID]` the consumer saw (`appenduid_at`, an index into
/// `buffered`) is CONSUMED as the answer: it is removed from `buffered`, so it
/// is not also published as an event. When the tagged code wins, the untagged
/// OK stays in `buffered` and is surrendered to the dispatcher like any other
/// unused response.
fn resolve_appenduid(
    tagged_code: Option<ResponseCode>,
    appenduid_at: Option<usize>,
    buffered: &mut Vec<UntaggedResponse>,
) -> Option<ResponseCode> {
    if tagged_code.is_some() {
        return tagged_code;
    }
    let index = appenduid_at.filter(|&i| i < buffered.len())?;
    match buffered.remove(index) {
        UntaggedResponse::Status { code, .. } => code,
        _ => None,
    }
}

/// The `crate::types::AppendUid { validity: uid_validity, uid: crate::types::Uid::new(uid).expect("UID range is nonzero") }` pairs an APPENDUID code assigns to the
/// `submitted` messages of a MULTIAPPEND, or `None` when it is not an
/// APPENDUID or its uid-set does not name exactly `submitted` UIDs.
///
/// The cardinality is counted BEFORE anything is expanded, so a server cannot
/// make the client allocate per UID it claims: `[APPENDUID 7 1:4294967295]`
/// for a two-message MULTIAPPEND costs one subtraction, not four billion
/// pairs. A range is either direction (RFC 3501 Section 9: `4:2` and `2:4`
/// name the same UIDs) and expands ascending, the order UIDs are assigned in.
///
/// A count that disagrees is a server contradicting its own completion. The
/// messages WERE appended (the tagged OK says so), so this is not an error -
/// an error on a non-idempotent APPEND invites a retry that duplicates them -
/// but the code cannot say which UID went to which message, so it is
/// discarded, exactly as if the server had no UIDPLUS.
fn appenduid_pairs(
    code: Option<ResponseCode>,
    submitted: usize,
) -> Option<Vec<crate::types::AppendUid>> {
    let Some(ResponseCode::AppendUid { uid_validity, uids }) = code else {
        return None;
    };
    let uid_validity = crate::types::UidValidity::new(uid_validity)?;
    let bounds = |range: &crate::types::response::UidRange| match range.end() {
        Some(end) => (range.start(), end),
        None => (range.start(), range.start()),
    };
    let named = uids.iter().fold(0u64, |total, range| {
        let (low, high) = bounds(range);
        total.saturating_add(u64::from(high - low) + 1)
    });
    if u64::try_from(submitted).ok() != Some(named) {
        tracing::warn!(
            named,
            submitted,
            "APPENDUID names a different number of UIDs than messages were appended; \
             discarding it (RFC 4315 Section 3)"
        );
        return None;
    }
    let mut pairs = Vec::with_capacity(submitted);
    for range in &uids {
        let (low, high) = bounds(range);
        pairs.extend((low..=high).map(|uid| crate::types::AppendUid {
            validity: uid_validity,
            uid: crate::types::Uid::new(uid).expect("UID range is nonzero"),
        }));
    }
    Some(pairs)
}

/// Consumer for APPEND (RFC 3501 Section6.3.11).
///
/// APPEND has no solicited untagged responses of its own  -  all
/// untagged data during APPEND is async state changes (EXISTS,
/// EXPUNGE, FETCH, etc.). The result is extracted from the tagged
/// OK response code: `[APPENDUID uidvalidity uid]` (RFC 4315 Section3).
///
/// An untagged `* OK [APPENDUID ...]` is retained WHOLE, in arrival order in
/// `buffered`, with only its index remembered. Narrowing it to its
/// `ResponseCode` at `on_response` time is what used to make it
/// unrecoverable: by the time a failure was known, the response text was gone
/// and nothing faithful could be handed back to the dispatcher. Now every arm
/// says what becomes of it - consumed as the answer, or surrendered.
#[derive(Default)]
pub(crate) struct AppendConsumer {
    buffered: Vec<UntaggedResponse>,
    /// Index in `buffered` of the first untagged `OK [APPENDUID ...]`.
    appenduid_at: Option<usize>,
}

impl Consumer for AppendConsumer {
    type Output = Option<crate::types::AppendUid>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        // APPEND produces no untagged responses of its own
        // (RFC 3501 Section6.3.11). Buffer everything; remember where the
        // first untagged APPENDUID OK sits.
        if self.appenduid_at.is_none() && is_untagged_appenduid_ok(&resp) {
            self.appenduid_at = Some(self.buffered.len());
        }
        self.buffered.push(resp);
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Option<crate::types::AppendUid>> {
        // On failure everything is surrendered, the untagged APPENDUID OK
        // included: it is whole, so it is a faithful event, and it was
        // classified `Either` so it may equally be an asynchronous notice.
        let Self {
            mut buffered,
            appenduid_at,
        } = *self;
        let tagged = match tagged.require_ok() {
            Ok(t) => t,
            Err(e) => return Finalized::failure(e, buffered),
        };
        // RFC 4315 Section3: extract APPENDUID from the tagged OK response code,
        // or failing that from an untagged OK. Servers without UIDPLUS may omit it.
        let code = resolve_appenduid(tagged.code, appenduid_at, &mut buffered);
        let append_uid = match code {
            Some(ResponseCode::AppendUid { uid_validity, uids }) => {
                // Single APPEND  -  extract the first UID from the set. Taking
                // the first UID of an over-long set is deliberate and needs no
                // cardinality check: nothing is expanded, so it costs nothing,
                // and refusing it would turn an appended draft into
                // `draft_create`'s `Unsupported`, inviting a retry that
                // appends it again.
                crate::types::UidValidity::new(uid_validity).and_then(|validity| {
                    uids.first().map(|r| crate::types::AppendUid {
                        validity,
                        uid: crate::types::Uid::new(r.start()).expect("UID range is nonzero"),
                    })
                })
            }
            _ => None,
        };
        Finalized::success(append_uid, buffered)
    }
}

/// Consumer for MULTIAPPEND (RFC 3502).
///
/// Same as [`AppendConsumer`] but extracts multiple UIDs from the
/// `[APPENDUID]` response code. Each UID range is expanded into
/// individual `crate::types::AppendUid { validity: uid_validity, uid: crate::types::Uid::new(uid).expect("UID range is nonzero") }` pairs, once the code is known to name
/// exactly as many UIDs as messages were submitted (`appenduid_pairs`).
/// Retains an untagged `* OK [APPENDUID ...]` whole, exactly as
/// [`AppendConsumer`] does.
pub(crate) struct MultiAppendConsumer {
    buffered: Vec<UntaggedResponse>,
    /// Index in `buffered` of the first untagged `OK [APPENDUID ...]`.
    appenduid_at: Option<usize>,
    /// How many messages the MULTIAPPEND carries: the number of UIDs a
    /// truthful APPENDUID names.
    submitted: usize,
}

impl MultiAppendConsumer {
    /// A consumer for a MULTIAPPEND of `submitted` messages.
    pub(crate) fn new(submitted: usize) -> Self {
        Self {
            buffered: Vec::new(),
            appenduid_at: None,
            submitted,
        }
    }
}

impl Consumer for MultiAppendConsumer {
    type Output = Vec<crate::types::AppendUid>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        // MULTIAPPEND produces no untagged responses of its own
        // (RFC 3502 Section3). Buffer everything; remember where the first
        // untagged APPENDUID OK sits.
        if self.appenduid_at.is_none() && is_untagged_appenduid_ok(&resp) {
            self.appenduid_at = Some(self.buffered.len());
        }
        self.buffered.push(resp);
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<crate::types::AppendUid>> {
        // Same disposition as `AppendConsumer`.
        let Self {
            mut buffered,
            appenduid_at,
            submitted,
        } = *self;
        let tagged = match tagged.require_ok() {
            Ok(t) => t,
            Err(e) => return Finalized::failure(e, buffered),
        };
        // RFC 4315 Section3: for MULTIAPPEND, the uid-set contains one
        // UID per appended message, possibly as ranges.
        let code = resolve_appenduid(tagged.code, appenduid_at, &mut buffered);
        let results = appenduid_pairs(code, submitted).unwrap_or_default();
        Finalized::success(results, buffered)
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  ID / COMPRESS / STARTTLS / LOGOUT
// ---------------------------------------------------------------------------

/// Consumer for ID (RFC 2971 Section3.1).
///
/// Extracts the server's identity key-value pairs from the untagged
/// ID response. RFC 2971 Section3.2: the server MUST respond with an ID response.
#[derive(Default)]
pub(crate) struct IdConsumer {
    // ID is an ordered list of arbitrary extension keys and nullable values,
    // not a fixed identity record. Preserve NIL and order through the command,
    // response, and account factory boundary; a typed replacement must update
    // that boundary together rather than adding a second representation here.
    pairs: Option<Vec<(String, Option<String>)>>,
}

impl Consumer for IdConsumer {
    type Output = Vec<(String, Option<String>)>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        // RFC 2971 Section3.2: take the first ID response.
        match resp {
            UntaggedResponse::Id(pairs) if self.pairs.is_none() => {
                self.pairs = Some(pairs);
            }
            other => return Some(other),
        }
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<(String, Option<String>)>> {
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, Vec::new());
        }
        match self.pairs {
            Some(pairs) => Finalized::success(pairs, Vec::new()),
            None => Finalized::failure(
                Error::Provider(crate::error::ProviderFailure::Missing(
                    "ID OK but no untagged ID response (RFC 2971 Section 3.2)".into(),
                )),
                Vec::new(),
            ),
        }
    }
}

/// Consumer for NAMESPACE (RFC 2342 Section5).
///
/// Extracts the personal, other-users, and shared namespace
/// descriptors from the untagged NAMESPACE response.
#[derive(Default)]
pub(crate) struct NamespaceConsumer {
    namespace: Option<crate::types::NamespaceResponse>,
}

impl Consumer for NamespaceConsumer {
    type Output = crate::types::NamespaceResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        // RFC 2342 Section5: take the first NAMESPACE response.
        match resp {
            UntaggedResponse::Namespace {
                personal,
                other,
                shared,
            } if self.namespace.is_none() => {
                self.namespace = Some(crate::types::NamespaceResponse {
                    personal,
                    other,
                    shared,
                });
            }
            other => return Some(other),
        }
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<crate::types::NamespaceResponse> {
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, Vec::new());
        }
        match self.namespace {
            Some(namespace) => Finalized::success(namespace, Vec::new()),
            None => Finalized::failure(
                Error::Provider(crate::error::ProviderFailure::Missing(
                    "NAMESPACE OK but no untagged NAMESPACE response (RFC 2342 Section 5)".into(),
                )),
                Vec::new(),
            ),
        }
    }
}

/// Consumer for the ENABLE command (RFC 5161 Section 3).
///
/// Captures the `* ENABLED` untagged response and returns the list
/// of extensions the server actually enabled for this request.
#[derive(Default)]
pub(crate) struct EnableConsumer {
    /// Wire observations retain server spelling. ProtocolState parses these
    /// into its typed negotiation owner before any mode or capability gate.
    caps: Option<Vec<String>>,
}

impl Consumer for EnableConsumer {
    type Output = Vec<String>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        match resp {
            UntaggedResponse::Enabled(exts) => {
                // Every frame updates live negotiation; return that same full
                // observation so callers do not mistake enabled modes for refusals.
                self.caps.get_or_insert_with(Vec::new).extend(exts);
            }
            other => return Some(other),
        }
        None
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<String>> {
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, Vec::new());
        }
        // RFC 5161 Section 3.2: the server MUST send an ENABLED response.
        // Tolerate omission per Postel's law  -  warn and return empty.
        let exts = self.caps.unwrap_or_else(|| {
            tracing::warn!(
                "server omitted ENABLED response (RFC 5161 Section 3.2) \
                  -  treating as empty"
            );
            Vec::new()
        });
        Finalized::success(exts, Vec::new())
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;

#[cfg(test)]
pub(in crate::connection) use search::expand_uid_ranges;

/// Filter flags not valid in STORE flag lists (RFC 3501 Section 2.3.2).
///
/// `\Recent` is server-only and `\*` (wildcard) is not valid in STORE.
pub(in crate::connection) fn filter_store_flags(
    flags: &[crate::types::Flag],
) -> Vec<crate::types::Flag> {
    flags
        .iter()
        .filter(|flag| flag.is_writable())
        .cloned()
        .collect()
}

#[cfg(test)]
pub(in crate::connection) use list::{is_notify_list_event, is_notify_selection_mismatch};
