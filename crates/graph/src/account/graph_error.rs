//! Graph account error boundary.
//!
//! Every Graph-side failure that leaves a streaming helper or
//! `Account` method passes through `into_account_error` (REST) or
//! `ews_error_to_account_error` (EWS). Transport variants delegate to
//! `bifrost_net::into_account_error` with Graph provider/protocol
//! context. Graph response variants build the `AccountError` directly
//! through `AccountErrorBuilder`, classified by the typed
//! `GraphSignal`. No path in this module looks at `error.message` text
//! for control flow.

use bifrost_net::NetErrorContext;
use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AttemptCause, AuthCause, AuthErrorKind, Cause, CursorScope, DiagnosticText,
    ErrorScope, GraphSignal, InternalCause, InternalErrorKind, MailboxId, MailboxUnavailableKind,
    Protocol, ProtocolErrorKind, Provider, RequestCause, ResourceKind, RetryHint, ServerCause,
    ServerErrorKind, StateCause, SyncStateErrorKind, ThrottleScope, TransmissionState, WireCause,
};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, RETRY_AFTER};

use crate::error::{GraphError, GraphInnerError, GraphResponseError};
use crate::ews::{EwsError, SoapFaultCode};

/// Context every Graph (or EWS-fallback) error needs before the
/// account boundary. `protocol` selects `Protocol::Graph` for REST
/// calls or `Protocol::Ews` for the streaming-notification fallback.
#[derive(Clone, Debug)]
pub(crate) struct GraphErrorContext {
    pub(crate) protocol: Protocol,
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
    /// Overrides `AccountOperation::is_idempotent` for one call site.
    /// `None` leaves the operation's own answer in force.
    pub(crate) idempotency_override: Option<bool>,
    /// The Outlook mailbox budget the request was charged to, when the call
    /// site knows the client that sent it (a `$batch` subresponse) or the
    /// REST funnel tagged the failure (`GraphError::MailboxThrottled`).
    /// Stamped on the error only when it classifies as a throttle.
    pub(crate) budget: Option<bifrost_types::ThrottleBudget>,
}

impl GraphErrorContext {
    #[must_use]
    pub(crate) fn graph(operation: AccountOperation) -> Self {
        Self {
            protocol: Protocol::Graph,
            operation,
            scope: None,
            idempotency_override: None,
            budget: None,
        }
    }

    /// EWS context. Used by the EWS streaming worker and any other
    /// helper whose wire boundary is the Exchange Web Services SOAP
    /// endpoint. Every `AccountError` constructed with this context
    /// stamps `Protocol::Ews`.
    #[must_use]
    pub(crate) fn ews(operation: AccountOperation) -> Self {
        Self {
            protocol: Protocol::Ews,
            operation,
            scope: None,
            idempotency_override: None,
            budget: None,
        }
    }

    #[must_use]
    pub(crate) fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = Some(scope);
        self
    }

    /// The mailbox budget the classified request was charged to.
    #[must_use]
    pub(crate) fn with_budget(mut self, budget: bifrost_types::ThrottleBudget) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Declare this request safely repeatable even though its
    /// `AccountOperation` is not.
    ///
    /// `recovery::derive` reads `AccountOperation::is_idempotent` to choose
    /// between `Retry(SameRequest)` and `Reconcile(TransportDropAfterSend)`
    /// for an in-flight transport drop, and that answer is per operation, not
    /// per request. A read-only preflight issued under a mutating operation -
    /// the `translateExchangeIds` POST that runs before any push subscription
    /// exists - has no target to reconcile against and is safe to repeat, so
    /// it says so here rather than borrowing an unrelated operation whose
    /// name would then lie in every telemetry export.
    #[must_use]
    pub(crate) fn idempotent(mut self) -> Self {
        self.idempotency_override = Some(true);
        self
    }

    fn to_net_ctx(&self) -> NetErrorContext {
        NetErrorContext {
            provider: Some(Provider::Microsoft),
            protocol: self.protocol,
            operation: self.operation,
            scope: self.scope.clone(),
        }
    }
}

/// Convert a structured `GraphError` into the opaque
/// `bifrost_types::AccountError`. The only entry point any account
/// layer call site should use for Graph REST failures.
#[must_use]
pub(crate) fn into_account_error(error: GraphError, ctx: GraphErrorContext) -> AccountError {
    match error {
        // Routed through the context funnel rather than
        // `invalid_account_error`, which takes only an operation: a
        // configuration rejection is raised per message, per cursor scope,
        // or per subscription resource, and the caller always knows which.
        // Dropping `ctx.scope` here erased the affected id from telemetry
        // and every support export, leaving an operator a
        // `request.malformed` with nothing to act on. `ctx.protocol` rides
        // along for the same reason (the EWS constructor sets it).
        GraphError::Configuration { message } => finish(
            base_builder(
                &ctx,
                AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(message),
                }),
            ),
            &ctx,
        ),
        GraphError::MailboxThrottled { budget, source } => {
            into_account_error(*source, ctx.with_budget(budget))
        }
        GraphError::Net(net) => net_to_account_error(net, &ctx),
        GraphError::Response(response) => response_to_account_error(response, &ctx),
        GraphError::Json { message, body } => json_parse_to_account_error(&message, body, &ctx),
        // Graph answered with a link off the configured origin. The answer
        // is provider output (so `Acknowledged`), and following it would
        // hand the account bearer to another host, so it is a contract
        // violation rather than something to retry.
        GraphError::ProviderLinkRefused { reason } => {
            let detail = DiagnosticText::support_only(format!("Graph response link {reason}"));
            let builder = base_builder(
                &ctx,
                AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                Cause::Wire(WireCause::MalformedResponse {
                    protocol: ctx.protocol,
                    detail: Some(detail.clone()),
                }),
            )
            .text(detail);
            finish(push_attempt(builder, TransmissionState::Acknowledged), &ctx)
        }
        GraphError::Internal { message } => internal_invariant(message, &ctx),
        GraphError::LimitExceeded { message } => {
            internal(InternalErrorKind::LimitExceeded, message, &ctx)
        }
        GraphError::RuntimeFailure { message } => {
            internal(InternalErrorKind::RuntimeFailure, message, &ctx)
        }
        // A complete response that breaks the protocol's semantics: the
        // provider answered, so the evidence is `Acknowledged`.
        GraphError::ContractViolation { message } => {
            let detail = DiagnosticText::support_only(message);
            let builder = base_builder(
                &ctx,
                AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                Cause::Wire(WireCause::MalformedResponse {
                    protocol: ctx.protocol,
                    detail: Some(detail.clone()),
                }),
            )
            .text(detail);
            finish(push_attempt(builder, TransmissionState::Acknowledged), &ctx)
        }
    }
}

/// A client-side invariant failure (`reference/error-model.md`): nothing was
/// sent, and neither the provider nor the network is at fault.
fn internal_invariant(message: String, ctx: &GraphErrorContext) -> AccountError {
    internal(InternalErrorKind::InvariantViolated, message, ctx)
}

/// A client-side failure of the given kind (`reference/error-model.md`).
fn internal(kind: InternalErrorKind, message: String, ctx: &GraphErrorContext) -> AccountError {
    let detail = DiagnosticText::support_only(message);
    finish(
        base_builder(
            ctx,
            AccountErrorKind::Internal(kind),
            Cause::Internal(InternalCause::new(kind, Some(detail.clone()))),
        )
        .text(detail),
        ctx,
    )
}

/// Classify a transport-layer failure, re-decoding Graph's own error
/// envelope first when the failure carries one.
///
/// `bifrost-net` never hands a 4xx or 5xx back as a response: the retry
/// loop converts it into `Status`, `RateLimited`, or
/// `RetryBudgetExhausted` with the body preserved. Delegating those
/// straight to `bifrost_net::into_account_error` classifies them on the
/// HTTP status alone and throws Graph's typed `error.code` away, so the
/// whole `GraphSignal` table below applied only to `$batch` subresponses
/// and never to an ordinary REST call: a 400 `InvalidDeltaToken` - the
/// documented delta-expiry signal - landed on `Request(Malformed)` ->
/// `ClientBug` instead of `SyncState(CursorInvalid)` -> `RestartScope`,
/// and a 403 `AdminConsentRequired` on `PermissionDenied` instead of
/// `NeedsAdminConsent`.
///
/// Refinement needs the body to be Graph's: either an envelope decoded, or
/// there was no body at all and the status table is the whole answer (a
/// bare 412 is `ConcurrencyConflict` here and a bare `Server(Error)` in
/// bifrost-net, and a bare 400 on a cursor scope is the forward-compat
/// delta-expiry restart). A NON-EMPTY body that is not an envelope is left
/// on bifrost-net's mapping on purpose: Graph did not write it, so reading
/// it as `Protocol(ContractViolation)` would turn an edge device's HTML 403
/// on the Autodiscover leg into a terminal provider fault.
///
/// `AuthLost` is deliberately not refined either: it means a 401 survived a
/// forced token refresh, which is strictly stronger evidence than the
/// envelope, and Graph's own 401 arm derives the same recovery class.
#[must_use]
fn net_to_account_error(net: bifrost_net::Error, ctx: &GraphErrorContext) -> AccountError {
    if let Some((status, headers, body)) = graph_envelope_evidence(&net) {
        let response = GraphResponseError::from_response(status, headers, body);
        if response.signal.is_some() || response.body.is_empty() {
            return response_to_account_error(response, ctx);
        }
    }
    stamp_budget(
        apply_idempotency_override(bifrost_net::into_account_error(net, ctx.to_net_ctx()), ctx),
        ctx,
    )
}

/// Stamp the context's mailbox budget on an error `bifrost-net` built, when
/// it classified as a throttle. The response path stamps it at construction;
/// this covers a 429 whose body was not Graph's, which bifrost-net
/// classifies itself.
#[must_use]
fn stamp_budget(error: AccountError, ctx: &GraphErrorContext) -> AccountError {
    let Some(budget) = ctx.budget.as_ref() else {
        return error;
    };
    if !matches!(
        error.kind(),
        AccountErrorKind::Server(ServerErrorKind::RateLimited | ServerErrorKind::QuotaExhausted)
    ) {
        return error;
    }
    error
        .into_builder()
        .throttle_budget(budget.clone())
        .try_build()
        .expect("valid account error classification")
}

/// Status, headers, and body of the response a `bifrost_net::Error`
/// preserved, for the variants whose body may carry a Graph error
/// envelope.
fn graph_envelope_evidence(
    net: &bifrost_net::Error,
) -> Option<(StatusCode, HeaderMap, bytes::Bytes)> {
    match net {
        bifrost_net::Error::Status {
            code,
            body,
            headers,
        } => Some((*code, headers.clone(), body.clone())),
        bifrost_net::Error::RateLimited { final_response, .. }
        | bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            ..
        } => Some((
            final_response.status,
            final_response.headers.clone(),
            final_response.body.clone(),
        )),
        _ => None,
    }
}

/// Re-apply the context's idempotency override onto an error `bifrost-net`
/// built.
///
/// `NetErrorContext` carries no override channel, and the override is a
/// builder-only input that `into_builder` deliberately drops, so the
/// transport arm re-derives the recovery class through the documented
/// decoration path instead of teaching every net consumer a new field. The
/// kind and cause chain are untouched; only `recovery` recomputes. A call
/// site that declared nothing pays nothing - there is no rebuild.
#[must_use]
fn apply_idempotency_override(error: AccountError, ctx: &GraphErrorContext) -> AccountError {
    let Some(idempotent) = ctx.idempotency_override else {
        return error;
    };
    error
        .into_builder()
        .idempotency_override(idempotent)
        .try_build()
        .expect("valid account error classification")
}

/// Convert a structured `EwsError` into the opaque `AccountError`.
/// Stamps `Protocol::Ews` on every produced error and delegates
/// transport variants to `bifrost_net::into_account_error` with the
/// EWS context so the recovery taxonomy is identical to REST.
#[must_use]
pub(crate) fn ews_error_to_account_error(error: EwsError, ctx: GraphErrorContext) -> AccountError {
    debug_assert!(matches!(ctx.protocol, Protocol::Ews));
    match error {
        EwsError::Transport(net) => {
            apply_idempotency_override(bifrost_net::into_account_error(net, ctx.to_net_ctx()), &ctx)
        }
        EwsError::HttpStatus { status, body } => {
            // Wrap the raw HTTP failure as a Graph-style response so
            // the standard status-fallback classification applies,
            // but stamp the protocol as EWS via the context. The body
            // is passed uncapped: `response_to_account_error` caps it
            // once via `body_diagnostic`; capping here first would
            // double-truncate (cap, then cap the capped string).
            let response = GraphResponseError::from_response(status, HeaderMap::new(), body);
            response_to_account_error(response, &ctx)
        }
        EwsError::SoapFault { code, detail } => soap_fault_to_account_error(code, detail, &ctx),
        EwsError::MalformedXml(detail) => {
            let builder = base_builder(
                &ctx,
                AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
                Cause::Wire(WireCause::MalformedResponse {
                    protocol: ctx.protocol,
                    detail: Some(detail.clone()),
                }),
            )
            .text(detail);
            let builder = push_attempt(builder, TransmissionState::Acknowledged);
            finish(builder, &ctx)
        }
        EwsError::Internal(message) => internal_invariant(message, &ctx),
        EwsError::LimitExceeded(message) => {
            internal(InternalErrorKind::LimitExceeded, message, &ctx)
        }
        // An unusable configured Outlook origin: nothing was sent, and the
        // fix is the consumer's configuration.
        EwsError::Configuration(message) => finish(
            base_builder(
                &ctx,
                AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(message),
                }),
            ),
            &ctx,
        ),
    }
}

fn soap_fault_to_account_error(
    code: SoapFaultCode,
    detail: DiagnosticText,
    ctx: &GraphErrorContext,
) -> AccountError {
    // SOAP 1.1 fault codes map onto the same recovery taxonomy as
    // HTTP statuses: `Server` ~ 5xx (unavailable), `Client` ~ 4xx
    // (refused). Microsoft EWS embeds finer-grained codes in the
    // `<detail>` payload, but that is support-only diagnostics; we
    // do not branch UX on the string.
    let resource = resource_from_scope(ctx.scope.as_ref());
    // The third element flags a throttle path (rate-limit / quota): the
    // engine needs a `throttle_scope` so it buckets the EWS backoff (at the
    // account, see `throttle_scope_for`). Without it `ErrorServerBusy` ->
    // `RateLimited` carries no scope and the EWS worker hot-loops on
    // `SameRequest`, unlike the REST 429 path.
    let (kind, cause, throttle) = match code {
        SoapFaultCode::Server => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            false,
        ),
        SoapFaultCode::Client | SoapFaultCode::MustUnderstand | SoapFaultCode::VersionMismatch => (
            AccountErrorKind::Server(ServerErrorKind::Error { status: Some(400) }),
            Cause::Server(ServerCause::Error { status: Some(400) }),
            false,
        ),
        // Microsoft EWS specific faults the convergence audit
        // identified as recoverable but previously collapsed to
        // ProviderContractViolation.
        SoapFaultCode::ErrorAccessDenied => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied { resource }),
            false,
        ),
        SoapFaultCode::ErrorImpersonateUserDenied => (
            AccountErrorKind::Authorization(AccessErrorKind::ConditionalAccessBlocked),
            Cause::Access(AccessCause::ConditionalAccessBlocked),
            false,
        ),
        SoapFaultCode::ErrorServerBusy => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
            true,
        ),
        SoapFaultCode::ErrorMailboxStoreUnavailable | SoapFaultCode::ErrorMailboxMoveInProgress => {
            (
                AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                    kind: MailboxUnavailableKind::Transient,
                }),
                Cause::Access(AccessCause::MailboxUnavailable {
                    kind: MailboxUnavailableKind::Transient,
                }),
                false,
            )
        }
        SoapFaultCode::ErrorNonExistentMailbox => (
            AccountErrorKind::NotFound(ResourceKind::Mailbox),
            Cause::Request(RequestCause::NotFound {
                what: ResourceKind::Mailbox,
                id: id_from_scope(ctx.scope.as_ref()),
            }),
            false,
        ),
        SoapFaultCode::ErrorItemNotFound => {
            // Unscoped streaming and folder walks retain the historical item
            // fallback; typed hydration identifies the actual resource.
            let resource = resource.unwrap_or(ResourceKind::Message);
            (
                AccountErrorKind::NotFound(resource),
                Cause::Request(RequestCause::NotFound {
                    what: resource,
                    id: id_from_scope(ctx.scope.as_ref()),
                }),
                false,
            )
        }
        // A streaming subscription is intentionally short lived. These
        // server codes mean the current stream must be discarded and the
        // outer worker must Subscribe again, not that the account contract
        // is terminally broken.
        SoapFaultCode::ErrorStreamingSubscriptionInvalid => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            false,
        ),
        SoapFaultCode::Unknown => (
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: Some(detail.clone()),
            }),
            false,
        ),
    };
    let mut builder = base_builder(ctx, kind, cause).text(detail);
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    if throttle && let Some(scope) = throttle_scope_for(ctx) {
        builder = builder.throttle_scope(scope);
    }
    finish(builder, ctx)
}

/// Classify an Autodiscover `GetUserSettings` in-body `ErrorCode`.
///
/// Autodiscover reports its failures inside an HTTP 200, so none of these
/// is the caller's input: the lookup's only inputs are the account's own
/// address and the settings this crate names. The code says what happened,
/// and the kind follows it (the `ErrorCode` enumeration of the Autodiscover
/// SOAP protocol):
///
/// - `ServerBusy` is throttling: `Server(RateLimited)`, bucketed at the
///   account like EWS `ErrorServerBusy`.
/// - `InternalServerError` is the server's own failure, the in-body twin of
///   an HTTP 500: `Server(Unavailable)`, retried.
/// - `InvalidUser` names a mailbox the server does not know:
///   `NotFound(Mailbox)`.
/// - `InvalidRequest` / `InvalidSetting` say the request this crate built
///   was refused as malformed, the same answer an HTTP 400 gets:
///   `Request(Malformed)`.
/// - `SettingIsNotAvailable`, `InvalidDomain`, `NotFederated`, and any code
///   this build does not know are the provider declining to answer:
///   `Server(Error)` with no status, so `ProviderRefused`. An unrecognised
///   code is still a refusal (it is not `NoError`), so it is not read as an
///   unknown protocol state.
///
/// `what` names the answer level for the support text (the lookup, or one
/// setting). The verbatim code rides as the native code.
#[must_use]
pub(crate) fn autodiscover_error_code_to_account_error(
    what: &str,
    code: &str,
    message: Option<&str>,
    ctx: &GraphErrorContext,
) -> AccountError {
    autodiscover_error(what, code, code, message, ctx)
}

/// Classify an in-body `<Error>` of a POX (`autodiscover.xml`) Autodiscover
/// answer, whose `ErrorCode` is numeric rather than the SOAP protocol's
/// named codes.
///
/// Only the two codes whose meaning is well established map onto a SOAP
/// equivalent: `500` ("the e-mail address cannot be found") classifies as
/// `InvalidUser`, and `600` ("invalid request") as `InvalidRequest`. Every
/// other code - including an `<Error>` with no code at all - is the
/// provider declining to answer (`ProviderRefused`), the table's fallback
/// for an unrecognised code; guessing a transient meaning for a numeric
/// code nobody here has pinned would retry something that may be
/// permanent. The native code stays the verbatim number.
#[must_use]
pub(crate) fn autodiscover_pox_error_to_account_error(
    what: &str,
    code: &str,
    message: Option<&str>,
    ctx: &GraphErrorContext,
) -> AccountError {
    let code = code.trim();
    let classify_as = match code {
        "500" => "InvalidUser",
        "600" => "InvalidRequest",
        other => other,
    };
    autodiscover_error(what, classify_as, code, message, ctx)
}

/// The shared Autodiscover table: `classify_as` picks the kind (a SOAP
/// `ErrorCode` name), `native_code` is what the server actually sent.
fn autodiscover_error(
    what: &str,
    classify_as: &str,
    native_code: &str,
    message: Option<&str>,
    ctx: &GraphErrorContext,
) -> AccountError {
    let code = native_code;
    let is = |name: &str| classify_as.eq_ignore_ascii_case(name);
    let (kind, cause, throttle) = if is("ServerBusy") {
        (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
            true,
        )
    } else if is("InternalServerError") {
        (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            false,
        )
    } else if is("InvalidUser") {
        (
            AccountErrorKind::NotFound(ResourceKind::Mailbox),
            Cause::Request(RequestCause::NotFound {
                what: ResourceKind::Mailbox,
                id: None,
            }),
            false,
        )
    } else if is("InvalidRequest") || is("InvalidSetting") {
        (
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!("{what} refused the request: {code}")),
            }),
            false,
        )
    } else {
        (
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Server(ServerCause::Error { status: None }),
            false,
        )
    };
    let shown = if code.is_empty() {
        "without a code"
    } else {
        code
    };
    let detail = match message.map(str::trim).filter(|m| !m.is_empty()) {
        Some(message) => format!("{what} error {shown}: {message}"),
        None => format!("{what} error {shown}"),
    };
    let mut builder = base_builder(ctx, kind, cause).text(DiagnosticText::support_only(detail));
    if !code.is_empty() {
        builder = builder.native_code(code.to_string());
    }
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    if throttle && let Some(scope) = throttle_scope_for(ctx) {
        builder = builder.throttle_scope(scope);
    }
    finish(builder, ctx)
}

/// A complete provider answer, read in full, that breaks the protocol's
/// contract without failing to parse: a field the answer must carry is
/// absent (`MissingField`), or the answer contradicts itself
/// (`ContractViolation`). The provider answered, so the evidence is
/// `Acknowledged`.
#[must_use]
pub(crate) fn provider_answer_violation(
    kind: ProtocolErrorKind,
    detail: String,
    ctx: &GraphErrorContext,
) -> AccountError {
    let detail = DiagnosticText::support_only(detail);
    let builder = base_builder(
        ctx,
        AccountErrorKind::Protocol(kind),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: ctx.protocol,
            detail: Some(detail.clone()),
        }),
    )
    .text(detail);
    finish(push_attempt(builder, TransmissionState::Acknowledged), ctx)
}

/// `GraphError::Json`: a success body that did not parse.
///
/// The variant is no longer only Graph REST JSON: the Autodiscover XML
/// parsers and the OneDrive upload-session 202 read raise it too, each with a
/// message naming its source. The label therefore claims only what every
/// producer shares - the provider's response could not be read - and leaves
/// the format and source to the producer's message, rather than calling an
/// XML document a JSON parse failure.
fn json_parse_to_account_error(
    message: &str,
    body: Option<bytes::Bytes>,
    ctx: &GraphErrorContext,
) -> AccountError {
    let detail = if message.trim().is_empty() {
        DiagnosticText::support_only("provider response did not parse".to_string())
    } else {
        DiagnosticText::support_only(format!("provider response did not parse: {message}"))
    };
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: ctx.protocol,
            detail: Some(detail.clone()),
        }),
    )
    .text(detail);
    if let Some(body) = body
        && let Some(text) = body_diagnostic(&body)
    {
        builder = builder.text(text);
    }
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    finish(builder, ctx)
}

/// Crate-internal alias so other account modules can route a
/// `GraphResponseError` through the boundary classifier with their
/// own scope/operation context (per-item batch failures, for
/// example). The single classifier path keeps `AttemptCause`,
/// `WireCause::Graph`, retry-hint, and throttle-scope wiring
/// consistent across REST, EWS HTTP-status, PIM batch, and
/// `get_stream` per-item paths.
pub(crate) fn response_to_account_error_pub(
    response: GraphResponseError,
    ctx: &GraphErrorContext,
) -> AccountError {
    response_to_account_error(response, ctx)
}

fn response_to_account_error(
    response: GraphResponseError,
    ctx: &GraphErrorContext,
) -> AccountError {
    let GraphResponseError {
        status,
        headers,
        body,
        signal,
        message,
        inner,
    } = response;

    let code = status.as_u16();
    let retry_hint = parse_retry_after_header(&headers);

    // `signal == None` means we could not decode a Graph error
    // envelope; that is provider contract violation territory unless
    // the status code itself carries enough information (5xx with no
    // body is plenty; 400 with no body is malformed-response).
    let (kind, cause, throttle) = match signal.as_ref() {
        Some(signal) => classify(ctx, code, signal, retry_hint),
        None => classify_no_envelope(ctx, code, retry_hint, &body),
    };

    let mut builder = base_builder(ctx, kind, cause.clone());
    // Attach the GraphSignal as an inner wire cause whenever the
    // primary cause is not itself a wire cause, so support tooling
    // can read the typed provider code regardless of which semantic
    // bucket the kind landed in.
    if !matches!(cause, Cause::Wire(_))
        && let Some(signal) = signal.as_ref()
    {
        builder = builder.push_cause(Cause::Wire(WireCause::Graph(signal.clone())));
    }
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder = response_diagnostics(builder, status, &headers, &body, inner.as_ref());

    if let Some(msg) = message.as_ref().filter(|m| !m.trim().is_empty()) {
        builder = builder.text(DiagnosticText::support_only(msg.clone()));
    }
    if let Some(inner) = inner.as_ref() {
        if let Some(code) = inner.code.as_ref().filter(|c| !c.trim().is_empty()) {
            builder = builder.text(DiagnosticText::support_only(format!(
                "innerError.code: {code}"
            )));
        }
        if let Some(msg) = inner.message.as_ref().filter(|m| !m.trim().is_empty()) {
            builder = builder.text(DiagnosticText::support_only(format!(
                "innerError.message: {msg}"
            )));
        }
        if let Some(date) = inner.date.as_ref().filter(|d| !d.trim().is_empty()) {
            builder = builder.text(DiagnosticText::support_only(format!(
                "innerError.date: {date}"
            )));
        }
    }

    // The retry hint already lives structurally on `ServerCause`; no
    // builder side-channel is needed. `throttle_scope` annotation
    // applies only on rate-limit / quota / 503 paths.
    if throttle && let Some(scope) = throttle_scope_for(ctx) {
        builder = builder.throttle_scope(scope);
        if let Some(budget) = ctx.budget.as_ref() {
            builder = builder.throttle_budget(budget.clone());
        }
    }
    finish(builder, ctx)
}

/// Classification rules. All decisions are made on `GraphSignal` plus
/// HTTP status; no path inspects the `error.message` string. The
/// match is exhaustive over `GraphSignal` so adding a wire-enum
/// variant fails to compile here.
fn classify(
    ctx: &GraphErrorContext,
    code: u16,
    signal: &GraphSignal,
    retry_hint: Option<RetryHint>,
) -> (AccountErrorKind, Cause, bool) {
    match signal {
        GraphSignal::InvalidAuthenticationToken => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
            false,
        ),
        GraphSignal::AccessDenied | GraphSignal::Forbidden => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
            false,
        ),
        GraphSignal::AccessRestricted | GraphSignal::ConditionalAccessBlocked => (
            AccountErrorKind::Authorization(AccessErrorKind::ConditionalAccessBlocked),
            Cause::Access(AccessCause::ConditionalAccessBlocked),
            false,
        ),
        GraphSignal::AdminConsentRequired => (
            AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired),
            Cause::Access(AccessCause::AdminConsentRequired {
                needed: "admin-consent",
            }),
            false,
        ),
        GraphSignal::MailboxNotEnabledForRestApi => (
            AccountErrorKind::Authorization(AccessErrorKind::MailboxNotLicensed),
            Cause::Access(AccessCause::MailboxNotLicensed),
            false,
        ),
        GraphSignal::MailboxStoreUnavailable => (
            AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                kind: MailboxUnavailableKind::Transient,
            }),
            Cause::Access(AccessCause::MailboxUnavailable {
                kind: MailboxUnavailableKind::Transient,
            }),
            false,
        ),
        GraphSignal::ResyncRequired
        | GraphSignal::InvalidDeltaToken
        | GraphSignal::SyncStateNotFound => cursor_invalid_or_protocol(ctx),
        GraphSignal::TooManyRequests => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint }),
            true,
        ),
        GraphSignal::GenericFileError => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint }),
            false,
        ),
        GraphSignal::PreconditionFailed => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
            false,
        ),
        GraphSignal::Gone => gone_410(ctx),
        GraphSignal::NotFound => match resource_from_scope(ctx.scope.as_ref()) {
            Some(resource) => (
                AccountErrorKind::NotFound(resource),
                Cause::Request(RequestCause::NotFound {
                    what: resource,
                    id: id_from_scope(ctx.scope.as_ref()),
                }),
                false,
            ),
            None => server_error_tuple(404),
        },
        GraphSignal::Unknown { .. } => classify_by_status(ctx, code, retry_hint),
        // GraphSignal is #[non_exhaustive] from bifrost-types; new
        // variants land here unhandled and fall through to status-based
        // classification until explicit dispatch is added.
        _ => classify_by_status(ctx, code, retry_hint),
    }
}

/// CursorInvalid producer. Requires the caller to thread an
/// `ErrorScope::Cursor(_)` through the context: without it the
/// builder rejects the error at construction time
/// (`AccountErrorBuildError::CursorInvalidWithoutScope`). Producers
/// that hit this path without a cursor scope have a bug; we
/// down-route to `Protocol(ContractViolation)` so the audit surfaces
/// the producer rather than crashing in `try_build`.
fn cursor_invalid_or_protocol(ctx: &GraphErrorContext) -> (AccountErrorKind, Cause, bool) {
    if matches!(ctx.scope, Some(ErrorScope::Cursor(_))) {
        (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            false,
        )
    } else {
        // Should be unreachable in correctly-wired call sites. Keep
        // the producer surface honest: classify as a contract
        // violation so the engine routes terminal but the audit
        // captures the missing scope through telemetry.
        debug_assert!(
            false,
            "Graph CursorInvalid signal raised without ErrorScope::Cursor (op={:?})",
            ctx.operation,
        );
        (
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: Some(DiagnosticText::support_only(
                    "Graph CursorInvalid signal raised outside cursor scope".to_string(),
                )),
            }),
            false,
        )
    }
}

/// 410 Gone classification, single-sourced so the `GraphSignal::Gone`
/// and the status-fallback `410` arms cannot drift. On a cursor scope it
/// is the canonical `SyncState(CursorInvalid)` -> `Engine(RestartScope)`
/// signal; outside cursor context it is a permanent server error.
fn gone_410(ctx: &GraphErrorContext) -> (AccountErrorKind, Cause, bool) {
    if matches!(ctx.scope, Some(ErrorScope::Cursor(_))) {
        cursor_invalid_or_protocol(ctx)
    } else {
        server_error_tuple(410)
    }
}

fn classify_by_status(
    ctx: &GraphErrorContext,
    code: u16,
    retry_hint: Option<RetryHint>,
) -> (AccountErrorKind, Cause, bool) {
    match code {
        // A novel, not-yet-typed 400-coded signal on a cursor scope is
        // almost certainly a delta-token rejection Graph added after this
        // build (the typed `InvalidDeltaToken` / `SyncStateNotFound` codes
        // already route here through `classify`). Treat it like the
        // 410-coded case: route to the cursor-invalid restart rather than
        // a terminal `Request(Malformed)`. graph.md documents delta-token
        // expiry as reactive (410 / 400 InvalidDeltaToken -> RestartScope);
        // forward-compat extends that to an unknown 400 in cursor context.
        // Outside a cursor scope, a 400 stays a malformed request.
        400 if matches!(ctx.scope, Some(ErrorScope::Cursor(_))) => cursor_invalid_or_protocol(ctx),
        400 | 422 => (
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!("HTTP {code}")),
            }),
            false,
        ),
        401 => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
            false,
        ),
        403 => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
            false,
        ),
        404 => match resource_from_scope(ctx.scope.as_ref()) {
            Some(resource) => (
                AccountErrorKind::NotFound(resource),
                Cause::Request(RequestCause::NotFound {
                    what: resource,
                    id: id_from_scope(ctx.scope.as_ref()),
                }),
                false,
            ),
            None => server_error_tuple(code),
        },
        409 | 412 => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
            false,
        ),
        410 => gone_410(ctx),
        408 | 502 | 503 | 504 => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint }),
            false,
        ),
        429 => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint }),
            true,
        ),
        507 => (
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            Cause::Server(ServerCause::QuotaExhausted { retry_hint }),
            true,
        ),
        _ => server_error_tuple(code),
    }
}

/// `signal == None`: the body did not decode as a Graph error
/// envelope. For 5xx the status is enough to classify as
/// `Server(Unavailable)`. Only a 4xx carrying a *non-empty* body that
/// failed to parse as a Graph error envelope is treated as a
/// provider-contract violation (Graph documents every error code it
/// ships, so a non-empty body that does not decode is anomalous); an
/// empty 4xx falls through to the plain status mapping.
fn classify_no_envelope(
    ctx: &GraphErrorContext,
    code: u16,
    retry_hint: Option<RetryHint>,
    body: &bytes::Bytes,
) -> (AccountErrorKind, Cause, bool) {
    // 500-class without an envelope is still a server problem.
    // 4xx/other land on the existing status fallback, which will pick
    // the appropriate Auth/Authorization/etc. mapping based on the
    // code itself. The malformed-response cause is attached only
    // when the body is non-empty (a non-empty body that did not parse
    // as JSON is an actual contract violation; an empty 503 is
    // routine).
    if code >= 500 || !body.is_empty() {
        if body.is_empty() {
            return classify_by_status(ctx, code, retry_hint);
        }
        // Non-empty body that did not parse as a Graph error
        // envelope -> protocol contract violation.
        (
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: Some(DiagnosticText::support_only(format!(
                    "HTTP {code} with non-envelope body",
                ))),
            }),
            false,
        )
    } else {
        classify_by_status(ctx, code, retry_hint)
    }
}

fn server_error_tuple(code: u16) -> (AccountErrorKind, Cause, bool) {
    (
        AccountErrorKind::Server(ServerErrorKind::Error { status: Some(code) }),
        Cause::Server(ServerCause::Error { status: Some(code) }),
        false,
    )
}

fn base_builder(
    ctx: &GraphErrorContext,
    kind: AccountErrorKind,
    cause: Cause,
) -> AccountErrorBuilder {
    AccountErrorBuilder::new(kind, cause)
        .protocol(ctx.protocol)
        .operation(ctx.operation)
        .provider(Provider::Microsoft)
}

fn finish(builder: AccountErrorBuilder, ctx: &GraphErrorContext) -> AccountError {
    let builder = match &ctx.scope {
        Some(scope) => builder.scope(scope.clone()),
        None => builder,
    };
    let builder = match ctx.idempotency_override {
        Some(idempotent) => builder.idempotency_override(idempotent),
        None => builder,
    };
    builder
        .try_build()
        .expect("valid account error classification")
}

fn push_attempt(
    builder: AccountErrorBuilder,
    transmission_state: TransmissionState,
) -> AccountErrorBuilder {
    builder.push_cause(Cause::Attempt(AttemptCause::new(transmission_state)))
}

fn response_diagnostics(
    mut builder: AccountErrorBuilder,
    status: StatusCode,
    headers: &HeaderMap,
    body: &bytes::Bytes,
    inner: Option<&GraphInnerError>,
) -> AccountErrorBuilder {
    builder = builder.status(Some(status.as_u16()));

    // Prefer header request-id; fall back to innerError.request-id
    // when the headers do not carry one.
    let request_id = first_header(headers, REQUEST_ID_HEADERS)
        .or_else(|| inner.and_then(|i| i.request_id.clone()));
    if let Some(id) = request_id {
        builder = builder.request_id(id);
    }
    let client_id = header_text(headers, "client-request-id")
        .or_else(|| inner.and_then(|i| i.client_request_id.clone()));
    if let Some(trace) = client_id {
        builder = builder.trace_id(trace);
    }

    for name in SUPPORT_ONLY_HEADER_TEXT {
        if let Some(value) = header_text(headers, name) {
            builder = builder.text(DiagnosticText::support_only(format!("{name}: {value}")));
        }
    }
    if let Some(text) = body_diagnostic(body) {
        builder = builder.text(text);
    }
    builder
}

const REQUEST_ID_HEADERS: &[&str] = &["request-id", "x-ms-request-id", "x-request-id"];

const SUPPORT_ONLY_HEADER_TEXT: &[&str] = &["x-ms-ags-diagnostic"];

use bifrost_net::STATUS_BODY_CAP;

fn first_header(headers: &HeaderMap, names: &[&str]) -> Option<String> {
    for name in names {
        if let Some(value) = header_text(headers, name) {
            return Some(value);
        }
    }
    None
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    let name = HeaderName::from_bytes(name.as_bytes()).ok()?;
    let value = headers.get(name)?;
    let text = value.to_str().ok()?.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_owned())
    }
}

/// Cap response-body diagnostics at `STATUS_BODY_CAP` bytes so a
/// multi-megabyte error page does not become a multi-megabyte
/// `DiagnosticText` retained across the lifetime of the error.
fn body_diagnostic(body: &bytes::Bytes) -> Option<DiagnosticText> {
    if body.is_empty() {
        return None;
    }
    let capped = if body.len() > STATUS_BODY_CAP {
        let mut buf = String::from_utf8_lossy(&body[..STATUS_BODY_CAP]).into_owned();
        buf.push_str(" ... (truncated)");
        buf
    } else {
        String::from_utf8_lossy(body).into_owned()
    };
    Some(DiagnosticText::support_only(format!(
        "response body: {capped}"
    )))
}

/// Preserve absolute HTTP-dates, including elapsed deadlines, for recovery.
fn parse_retry_after_header(headers: &HeaderMap) -> Option<RetryHint> {
    bifrost_net::request::parse_retry_after_hint(headers.get(RETRY_AFTER))
}

/// Note the deliberate asymmetry with IMAP, which DOES read a folder id out of
/// `ErrorScope::Cursor(Folder(_))` in its own `resource_from_scope`.
///
/// Graph builds `Cursor(Folder(_))` / `Cursor(FolderType(_))` on essentially
/// every changes, inventory, push and public-folder path, so honouring it here
/// would name the folder in more diagnostics - a real if minor gain. It is not
/// a free change, though, and that is why it has not been made: this function
/// gates the `NotFound` classification above, so returning `Some(Mailbox)` for
/// a cursor scope silently converts every folder-scoped Graph 404 from a
/// generic server error into `NotFound(Mailbox)`, with a different recovery
/// class behind it. That is a behaviour change for consumers, not a
/// diagnostics improvement, and it wants deciding on its own terms rather than
/// riding along with a tidy-up.
///
/// What is actually LOST by leaving it: a Graph 403 on a shared or public
/// folder derives `NoPermission { resource: None }` and so names no id in the
/// support export, where the identical failure on IMAP names the mailbox. The
/// classification and recovery class are unchanged either way, so the cost is
/// a less specific support export, which is why this stays a deliberate hold
/// rather than a bug.
fn resource_from_scope(scope: Option<&ErrorScope>) -> Option<ResourceKind> {
    match scope? {
        ErrorScope::Message { .. } => Some(ResourceKind::Message),
        ErrorScope::Mailbox { .. } => Some(ResourceKind::Mailbox),
        ErrorScope::Thread { .. } => Some(ResourceKind::Thread),
        ErrorScope::Event { .. } => Some(ResourceKind::Event),
        ErrorScope::Calendar { .. } => Some(ResourceKind::Calendar),
        ErrorScope::Contact { .. } => Some(ResourceKind::Contact),
        ErrorScope::Account
        | ErrorScope::Cursor(_)
        | ErrorScope::CalendarCollection
        | ErrorScope::ContactCollection => None,
        _ => None,
    }
}

fn id_from_scope(scope: Option<&ErrorScope>) -> Option<String> {
    match scope? {
        ErrorScope::Message { id } => Some(id.0.clone()),
        ErrorScope::Mailbox { id } => Some(id.0.clone()),
        ErrorScope::Thread { id } => Some(id.0.clone()),
        ErrorScope::Event { id } => Some(id.0.clone()),
        ErrorScope::Calendar { id } => Some(id.0.clone()),
        ErrorScope::Contact { id } => Some(id.0.clone()),
        ErrorScope::Account
        | ErrorScope::Cursor(_)
        | ErrorScope::CalendarCollection
        | ErrorScope::ContactCollection => None,
        _ => None,
    }
}

/// `Account`, never tenant-wide. Microsoft documents the Outlook mail, calendar
/// and contacts limits per app per MAILBOX - one mailbox's throttle does not
/// affect another - so a tenant hold would pause mailboxes the provider never
/// throttled. A throttle the REST funnel or a `$batch` subresponse could
/// attribute also carries that mailbox's budget, and the engine then records
/// the budget alone. This scope is the fallback for the rest: EWS, the
/// Autodiscover and subscription endpoints, and any Outlook request the
/// funnel could not attribute. The account is the conservative answer there
/// (it pauses this account's other mailboxes too) without inventing a
/// tenant identity this crate never sees. It is a chosen approximation, not a
/// claim about how EWS or the subscription service meter.
fn throttle_scope_for(_ctx: &GraphErrorContext) -> Option<ThrottleScope> {
    Some(ThrottleScope::Account)
}

/// Build a `SyncState(ScopeRevoked)` error scoped to one foreign
/// (shared/delegate) mailbox's folder scope. With the scope attached,
/// the central `derive` resolves it to
/// `Engine(DisableScope(scope))` - the engine quarantines just that
/// cursor scope without escalating to account-wide auth loss. The owning
/// mailbox rides as support-only diagnostic text for telemetry.
#[must_use]
pub(crate) fn graph_scope_revoked(
    scope: CursorScope,
    owner: &MailboxId,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
        Cause::State(StateCause::ScopeRevoked),
    )
    .protocol(Protocol::Graph)
    .provider(Provider::Microsoft)
    .operation(operation)
    .scope(ErrorScope::Cursor(scope))
    .text(DiagnosticText::support_only(format!(
        "shared mailbox access revoked (owner {})",
        owner.0,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Map a per-scope failure for a foreign (shared/delegate) mailbox.
/// When the scope belongs to a shared mailbox (`owner.is_some()`) and
/// the failure classifies as permission-denied (the class that would
/// otherwise derive terminal `NoPermission` - `AccessDenied`/`Forbidden`/
/// 403), quarantine just this foreign scope via `ScopeRevoked` instead
/// of escalating account-wide. A primary-mailbox scope (`owner == None`),
/// or any non-permission failure, flows through the unchanged
/// `into_account_error` path - a primary-mailbox permission loss is a
/// genuine account-level signal.
#[must_use]
pub(crate) fn graph_shared_scope_error(
    error: GraphError,
    scope: &CursorScope,
    owner: Option<&MailboxId>,
    ctx: GraphErrorContext,
) -> AccountError {
    if let Some(owner) = owner {
        let account_error = into_account_error(error, ctx.clone());
        if matches!(
            account_error.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ) {
            return graph_scope_revoked(scope.clone(), owner, ctx.operation);
        }
        return account_error;
    }
    into_account_error(error, ctx)
}

/// EWS twin of `graph_shared_scope_error`. A public folder always has
/// an owning content mailbox (`owner.is_some()`), so a per-operation
/// `ErrorAccessDenied` (-> `Authorization(PermissionDenied)`) on a
/// public-folder scope quarantines just that scope via `ScopeRevoked`
/// instead of escalating account-wide. Any non-permission EWS failure,
/// or a scope with no owner, flows through the unchanged
/// `ews_error_to_account_error` path.
#[must_use]
pub(crate) fn ews_shared_scope_error(
    error: EwsError,
    scope: &CursorScope,
    owner: Option<&MailboxId>,
    ctx: GraphErrorContext,
) -> AccountError {
    if let Some(owner) = owner {
        let account_error = ews_error_to_account_error(error, ctx.clone());
        if matches!(
            account_error.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ) {
            return graph_scope_revoked(scope.clone(), owner, ctx.operation);
        }
        return account_error;
    }
    ews_error_to_account_error(error, ctx)
}

/// Build an `AccountError` for an operation this account does not support.
#[must_use]
pub(crate) fn unsupported_account_error(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .try_build()
    .expect("valid account error classification")
}

/// Build an `AccountError` for ONE item a batch surface cannot address.
///
/// The per-item twin of [`unsupported_account_error`], carrying the item's
/// scope. A surface with a per-item failure lane must not reject the whole
/// request because a single id needs a transport it does not implement -
/// that would discard the outcomes of every other id in the batch.
#[must_use]
pub(crate) fn unsupported_item_error(
    operation: AccountOperation,
    scope: ErrorScope,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .scope(scope)
    .try_build()
    .expect("valid account error classification")
}

/// Build an `AccountError` for malformed Account-level input.
#[must_use]
pub(crate) fn invalid_account_error(
    operation: AccountOperation,
    detail: impl Into<String>,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.into()),
        }),
    )
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .try_build()
    .expect("valid account error classification")
}

/// As `invalid_account_error`, but naming the request field at fault.
///
/// Same classification (`Request(Malformed)` -> `ClientBug`); the difference
/// is that `RequestCause::InvalidArgument` carries a machine-readable
/// `field`, which a consumer UI can use to highlight the offending input
/// instead of surfacing prose. Use it wherever the bad input is one
/// identifiable field of the request.
#[must_use]
pub(crate) fn invalid_argument_account_error(
    operation: AccountOperation,
    field: &'static str,
    detail: impl Into<String>,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some(field),
            message: Some(DiagnosticText::support_only(detail.into())),
        }),
    )
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .try_build()
    .expect("valid account error classification")
}

/// Build an `AccountError` for ONE item whose input was malformed before any
/// wire traffic (e.g. a Move whose destination does not resolve to a
/// same-mailbox folder). Classifies `Request(Malformed)` -> `ClientBug`: the
/// fault is the caller's request, not the provider's, and no transmission
/// evidence is attached because nothing was sent for this item.
#[must_use]
pub(crate) fn invalid_item_error(
    operation: AccountOperation,
    scope: ErrorScope,
    detail: impl Into<String>,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.into()),
        }),
    )
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .scope(scope)
    .try_build()
    .expect("valid account error classification")
}

/// Build an `AccountError` for ONE item refused because the account's LOCAL
/// state no longer matches what minted the id - a public folder with no
/// routing entry in this process, say, because the map starts empty on every
/// reopen and discovery can skip a folder after a per-folder failure.
///
/// Refused before any request, and neither the caller's fault nor the
/// provider's: `ConcurrencyConflict` with `Unsent` evidence, so
/// `Retry(AfterStateRefresh)`, per the local-refusal rule in
/// `reference/error-model.md`.
#[must_use]
pub(crate) fn stale_local_state_error(
    operation: AccountOperation,
    scope: ErrorScope,
    detail: impl Into<String>,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::ConcurrencyConflict,
        Cause::State(StateCause::ConcurrencyConflict),
    )
    .push_cause(Cause::Attempt(bifrost_types::AttemptCause::new(
        bifrost_types::TransmissionState::Unsent,
    )))
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .scope(scope)
    .text(DiagnosticText::support_only(detail.into()))
    .try_build()
    .expect("valid account error classification")
}

/// Build an `AccountError` for ONE id `translateExchangeIds` refused inside
/// an otherwise successful 200.
///
/// Graph answers per input id: a converted id carries `targetId`, a refused
/// one carries `errorDetails` and no target. The HTTP response itself
/// succeeded, so this cannot route through `response_to_account_error` - that
/// path needs a status, and synthesizing one is worse than useless here: a
/// fabricated 400 on the cursor scope this error must carry would classify
/// `SyncState(CursorInvalid)` and send the engine to `RestartScope`, which
/// cannot fix an id the server declines to convert.
///
/// The refusal is terminal by construction (resending the same id returns the
/// same answer), and what the caller submitted is the only thing that could
/// change, so it classifies `Request(Malformed)` -> `ClientBug`. The scope
/// names the folder whose id was rejected - without it the operator sees
/// "a subscription failed" and not which container - and the provider's
/// verbatim code rides as a typed `WireCause::Graph` for the support export.
#[must_use]
pub(crate) fn id_translation_refused(
    operation: AccountOperation,
    scope: ErrorScope,
    code: Option<&str>,
    message: Option<&str>,
) -> AccountError {
    let code = code.map(str::trim).filter(|code| !code.is_empty());
    let detail = DiagnosticText::support_only(match code {
        Some(code) => format!("translateExchangeIds refused an id: {code}"),
        None => "translateExchangeIds refused an id without a code".to_string(),
    });
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: detail.clone(),
        }),
    )
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .scope(scope)
    .text(detail);
    if let Some(code) = code {
        builder = builder
            .native_code(code.to_string())
            .push_cause(Cause::Wire(WireCause::Graph(crate::error::classify_code(
                code,
            ))));
    }
    if let Some(message) = message.map(str::trim).filter(|m| !m.is_empty()) {
        builder = builder.text(DiagnosticText::support_only(message.to_string()));
    }
    // A complete response arrived and named this id; only the conversion
    // failed.
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder
        .try_build()
        .expect("valid account error classification")
}

/// Translate a `CursorError` into the account-boundary `AccountError`.
///
/// `CursorProtocolMismatch`, `CursorEnvelopeUnknown`, and
/// `SchemaIncompatible` map to `SyncState(SchemaIncompatible)` so the
/// engine can restart the scope with a cleared cursor, and so does
/// `Decode`: cursor bytes that do not decode are the caller's durable state,
/// refused before any request, never a provider fault. `Unsupported`
/// maps to `Unsupported(EstablishCursor)`. `Encode` (a local serialization
/// failure) is the client's own invariant: `Internal(InvariantViolated)`.
#[must_use]
pub(crate) fn cursor_error_to_account_error(
    error: crate::account::cursor::CursorError,
    ctx: GraphErrorContext,
) -> AccountError {
    use crate::account::cursor::CursorError;
    // Every arm terminates through `finish`, which stamps `ctx.scope`.
    // Skipping it (the old `try_build` per arm) was not merely a telemetry
    // loss: `SyncState(ScopeRevoked)` derives `DisableScope(scope)` WITH a
    // scope and `RestartAccount` without one, so a single stale
    // shared-mailbox cursor escalated into an account-wide rediscovery -
    // which cannot restore a mailbox that left the configuration, so the
    // engine would take the whole account around that loop on every pass
    // instead of quarantining one folder. The other arms want the scope for
    // the ordinary reason: these errors are raised per cursor, and the
    // support export has to name which.
    let builder = match error {
        CursorError::ProtocolMismatch
        | CursorError::EnvelopeUnknown
        | CursorError::SchemaIncompatible
        | CursorError::Decode(_) => base_builder(
            &ctx,
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .text(DiagnosticText::support_only(error.to_string())),
        CursorError::CalendarWindowExpired | CursorError::InventoryInProgress => base_builder(
            &ctx,
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        )
        .text(DiagnosticText::support_only(error.to_string())),
        CursorError::Unsupported => base_builder(
            &ctx,
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
        ),
        CursorError::Configuration(msg) => base_builder(
            &ctx,
            AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
            Cause::State(StateCause::ScopeRevoked),
        )
        .text(DiagnosticText::support_only(msg)),
        // A cursor this crate built would not serialize: the client's own
        // invariant, never the provider's fault.
        CursorError::Encode(msg) => {
            let detail = DiagnosticText::support_only(msg);
            base_builder(
                &ctx,
                AccountErrorKind::Internal(InternalErrorKind::InvariantViolated),
                Cause::Internal(InternalCause::new(
                    InternalErrorKind::InvariantViolated,
                    Some(detail.clone()),
                )),
            )
            .text(detail)
        }
    };
    finish(builder, &ctx)
}

/// Build a typed `Protocol(_)` `AccountError` for graph-side
/// data-shape violations (missing id, missing etag, etc.). Used by
/// the `pim` modules and mutate.rs for the produce-without-attempting-the-wire
/// path. `scope` carries the affected resource so support exports
/// can pinpoint the message; the caller's `operation` is preserved
/// rather than coerced into `Hydrate`.
#[must_use]
pub(crate) fn protocol_violation(
    inner: ProtocolErrorKind,
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    detail: impl Into<String>,
) -> AccountError {
    let detail = DiagnosticText::support_only(detail.into());
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(inner),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Graph,
            detail: Some(detail.clone()),
        }),
    )
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .text(detail);
    if let Some(scope) = scope {
        builder = builder.scope(scope);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// Build the `AccountError` for a `$batch` subrequest the server never
/// answered.
///
/// The outer `$batch` envelope decoded, so the request WAS acknowledged;
/// what is unknown is whether the omitted subrequest ran. A decoded outer
/// response is no evidence that the item was skipped, so this must not
/// claim the item was left alone: a `move` or `DELETE` that landed and
/// then lost its response reads identically on the wire.
///
/// `Protocol(ContractViolation)` is the wrong kind here because it derives
/// `ProviderContractViolation`, which is terminal - the engine would file
/// the mutation as `failed_terminal` with no read-back and never discover
/// that the write did land. `Protocol(PartialResponse)` is the shared
/// contract's answer: it retries idempotent work (hydration, absolute-state
/// flag writes) and routes non-idempotent work (move, destroy) to
/// `Reconcile(PartialCompletionSignal)` so the caller probes the target.
#[must_use]
pub(crate) fn batch_response_missing(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    detail: impl Into<String>,
) -> AccountError {
    let detail = DiagnosticText::support_only(detail.into());
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Graph,
            detail: Some(detail.clone()),
        }),
    )
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .text(detail);
    if let Some(scope) = scope {
        builder = builder.scope(scope);
    }
    // The `$batch` envelope came back, so transmission evidence is
    // `Acknowledged`, not the `Unsent` default.
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder
        .try_build()
        .expect("valid account error classification")
}

/// Translate a single `$batch` response item into an
/// `ItemOutcome<MutationSuccess>`. Centralizes the mutation outcome
/// rules from the plan: 2xx -> applied, destroy-404 -> skipped, 404 -> not
/// found, 409 / 412 -> concurrency conflict (never `Skipped`), 429 ->
/// rate limited with retry-after + throttle scope, 5xx -> unavailable.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub(crate) fn mutation_item_outcome(
    status: u16,
    headers: HeaderMap,
    body: bytes::Bytes,
    destroy: bool,
    item_id: bifrost_types::BatchItemId,
    operation: AccountOperation,
    scope: ErrorScope,
    // The mailbox budget the item's sub-request was charged to, stamped on
    // a throttled outcome. The outer `$batch` cannot carry it (it mixes
    // mailboxes); each item can.
    budget: Option<bifrost_types::ThrottleBudget>,
) -> bifrost_types::ItemOutcome<bifrost_types::MutationSuccess> {
    use bifrost_types::{BatchFailure, BatchSuccess, ItemOutcome, MutationSuccess};

    if (200..300).contains(&status) {
        return ItemOutcome::Succeeded(BatchSuccess::new(item_id, MutationSuccess::Applied));
    }
    if status == 404 && destroy {
        return ItemOutcome::Succeeded(BatchSuccess::new(item_id, MutationSuccess::Skipped));
    }
    let response = GraphResponseError::from_response(
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        headers,
        body,
    );
    let mut ctx = GraphErrorContext::graph(operation).with_scope(scope);
    ctx.budget = budget;
    let error = response_to_account_error(response, &ctx);
    ItemOutcome::Failed(BatchFailure::new(item_id, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{GraphError, GraphResponseError};
    use bifrost_types::{
        AccessErrorKind, AccountErrorKind, AuthErrorKind, CursorScope, EngineDirective,
        ProtocolErrorKind, ReconcileAction, ReconcileReason, RecoveryClass, ServerErrorKind,
        SyncStateErrorKind,
    };
    use bytes::Bytes;
    use reqwest::StatusCode;
    use reqwest::header::{HeaderMap, HeaderValue};

    fn body(json: &str) -> Bytes {
        Bytes::copy_from_slice(json.as_bytes())
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            let name = HeaderName::from_bytes(k.as_bytes()).expect("valid header name");
            let value = HeaderValue::from_str(v).expect("valid header value");
            h.insert(name, value);
        }
        h
    }

    fn classify(
        status: StatusCode,
        json: &str,
        ctx: GraphErrorContext,
        extra_headers: &[(&str, &str)],
    ) -> AccountError {
        let response =
            GraphResponseError::from_response(status, headers(extra_headers), body(json));
        into_account_error(GraphError::Response(response), ctx)
    }

    fn graph_ctx(op: AccountOperation) -> GraphErrorContext {
        GraphErrorContext::graph(op)
    }

    /// A locally-detected configuration rejection is raised PER id (a stale
    /// shared-mailbox message), per cursor scope, or per subscription
    /// resource, and every caller supplies the one it means. Routing it
    /// through `invalid_account_error` - which takes an operation and
    /// nothing else - discarded that scope, so telemetry and every support
    /// export carried a bare `request.malformed` an operator could not tie
    /// to a message.
    #[test]
    fn a_configuration_rejection_keeps_the_callers_error_scope() {
        let error = into_account_error(
            GraphError::Configuration {
                message: "shared mailbox not configured on this account: gone@contoso.com"
                    .to_string(),
            },
            graph_ctx(AccountOperation::BulkDestroy).with_scope(ErrorScope::Message {
                id: ("AAMkmsg".to_string()).into(),
            }),
        );
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        // Stale configuration is nothing the engine can retry its way out
        // of; the operator has to restore the mailbox or drop the id.
        assert!(error.recovery().is_terminal());
        assert_eq!(
            error.scope(),
            Some(&ErrorScope::Message {
                id: ("AAMkmsg".to_string()).into()
            })
        );
    }

    /// Cursor errors are raised PER cursor, and one of them changes
    /// classification on the scope's presence rather than merely losing
    /// telemetry: `SyncState(ScopeRevoked)` derives `DisableScope(scope)`
    /// with a scope and `RestartAccount` without. A stale shared mailbox is
    /// exactly what account-wide rediscovery cannot fix, so the scope-less
    /// form sent the engine around a rediscovery loop on every pass instead
    /// of quarantining the one folder.
    #[test]
    fn a_revoked_cursor_scope_disables_that_scope_rather_than_the_account() {
        let scope = CursorScope::FolderType {
            folder: bifrost_types::FolderId("gone@contoso.com\u{1f}AAMk".to_string()),
            ty: bifrost_types::ObjectType::Email,
        };
        let error = cursor_error_to_account_error(
            crate::account::cursor::CursorError::Configuration("mailbox gone".to_string()),
            graph_ctx(AccountOperation::SyncInventory)
                .with_scope(ErrorScope::Cursor(scope.clone())),
        );
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        ));
        match error.recovery() {
            RecoveryClass::Engine(EngineDirective::DisableScope(disabled)) => {
                assert_eq!(disabled, &scope);
            }
            other => panic!("expected DisableScope, got {other:?}"),
        }
    }

    /// The same stamping applies to every other cursor arm, which is the
    /// ordinary reason to want it: the support export has to say which
    /// cursor the envelope belonged to.
    #[test]
    fn a_cursor_schema_rejection_names_its_cursor() {
        let scope = CursorScope::FolderType {
            folder: bifrost_types::FolderId("inbox".to_string()),
            ty: bifrost_types::ObjectType::Email,
        };
        let error = cursor_error_to_account_error(
            crate::account::cursor::CursorError::SchemaIncompatible,
            graph_ctx(AccountOperation::SyncChanges).with_scope(ErrorScope::Cursor(scope.clone())),
        );
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
        assert_eq!(error.scope(), Some(&ErrorScope::Cursor(scope)));
    }

    /// A client-side invariant (an unattached client) is
    /// `Internal`, terminal: never the `Transport(Network)` retry it used to
    /// be dressed as.
    #[test]
    fn a_graph_internal_error_is_internal_not_a_network_retry() {
        let error = into_account_error(
            GraphError::Internal {
                message: "Graph client is not attached to an account".to_string(),
            },
            graph_ctx(AccountOperation::SyncChanges),
        );
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Internal(InternalErrorKind::InvariantViolated)
        );
        assert_eq!(
            error.recovery(),
            &bifrost_types::RecoveryClass::InternalFailure
        );
    }

    /// A cursor this crate built that will not serialize is the client's
    /// own invariant, not a provider contract violation.
    #[test]
    fn a_cursor_encode_failure_is_internal_not_a_provider_fault() {
        let error = cursor_error_to_account_error(
            crate::account::cursor::CursorError::Encode("serializer refused".to_string()),
            graph_ctx(AccountOperation::SyncChanges),
        );
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Internal(InternalErrorKind::InvariantViolated)
        );
        assert_eq!(
            error.recovery(),
            &bifrost_types::RecoveryClass::InternalFailure
        );
    }

    /// The scope is genuinely optional - the call sites that have no id to
    /// name (a factory-level misconfiguration) must still classify.
    #[test]
    fn a_scopeless_configuration_rejection_still_classifies() {
        let error = into_account_error(
            GraphError::Configuration {
                message: "no shared mailbox".to_string(),
            },
            graph_ctx(AccountOperation::Hydrate),
        );
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(error.scope(), None);
    }

    /// An omitted `$batch` subrequest is AMBIGUOUS, and the shared
    /// contract splits that ambiguity on idempotency: read-back the
    /// non-idempotent ones, re-issue the idempotent ones. Neither may be
    /// terminal - `Protocol(ContractViolation)`, the classification this
    /// replaced, derives `ProviderContractViolation`, so the engine filed
    /// a move or destroy that may well have LANDED as `failed_terminal`
    /// with no read-back at all.
    #[test]
    fn a_missing_batch_subresponse_is_ambiguous_never_terminal() {
        for (operation, idempotent) in [
            (AccountOperation::Hydrate, true),
            (AccountOperation::UpdateFlags, true),
            (AccountOperation::BulkMove, false),
            (AccountOperation::BulkDestroy, false),
        ] {
            let err = batch_response_missing(
                operation,
                Some(ErrorScope::Message {
                    id: ("m1".to_string()).into(),
                }),
                "Graph $batch returned no response for m1",
            );
            assert!(matches!(
                err.kind(),
                AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
            ));
            assert!(!err.recovery().is_terminal(), "{operation:?}");
            assert_eq!(err.message_key(), "protocol.partial-response");

            if idempotent {
                assert!(err.recovery().is_retryable(), "{operation:?}");
            } else {
                let RecoveryClass::Reconcile(advice) = err.recovery() else {
                    panic!("{operation:?} should reconcile, got {:?}", err.recovery());
                };
                assert_eq!(advice.reason, ReconcileReason::PartialCompletionSignal);
                assert!(
                    advice
                        .guidance
                        .actions
                        .contains(&ReconcileAction::CheckTarget)
                );
            }

            // The outer `$batch` envelope decoded, so the request was
            // acknowledged. Defaulting to `Unsent` would tell a consumer
            // nothing crossed the wire.
            assert_eq!(
                err.telemetry_fields().transmission_state,
                Some(TransmissionState::Acknowledged),
                "{operation:?}"
            );
        }
    }

    #[test]
    fn invalid_authentication_token_maps_to_reauth() {
        let err = classify(
            StatusCode::UNAUTHORIZED,
            r#"{"error":{"code":"InvalidAuthenticationToken","message":"x"}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        ));
        assert!(matches!(err.recovery(), RecoveryClass::AuthLost));
        assert_eq!(err.provider(), Some(Provider::Microsoft));
        assert_eq!(err.protocol(), Some(Protocol::Graph));
    }

    #[test]
    fn access_denied_maps_to_permission_denied() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"AccessDenied","message":"nope"}}"#,
            graph_ctx(AccountOperation::Hydrate),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ));
    }

    #[test]
    fn forbidden_maps_to_permission_denied() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"Forbidden","message":"nope"}}"#,
            graph_ctx(AccountOperation::Hydrate),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ));
    }

    #[test]
    fn access_restricted_maps_to_conditional_access() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"AccessRestricted"}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::ConditionalAccessBlocked)
        ));
    }

    #[test]
    fn admin_consent_required_maps_to_admin_consent() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"AdminConsentRequired"}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired)
        ));
        assert!(matches!(
            err.recovery(),
            RecoveryClass::NeedsAdminConsent { .. }
        ));
    }

    #[test]
    fn mailbox_not_enabled_maps_to_not_licensed() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"MailboxNotEnabledForRESTAPI"}}"#,
            graph_ctx(AccountOperation::SyncInventory),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::MailboxNotLicensed)
        ));
    }

    #[test]
    fn mailbox_store_unavailable_is_transient() {
        let err = classify(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":{"code":"MailboxStoreUnavailable"}}"#,
            graph_ctx(AccountOperation::SyncInventory),
            &[],
        );
        match err.kind() {
            AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                kind: MailboxUnavailableKind::Transient,
            }) => {}
            other => panic!("expected mailbox unavailable transient, got {other:?}"),
        }
    }

    #[test]
    fn resync_required_maps_to_cursor_invalid_with_engine_restart() {
        let cursor_scope = ErrorScope::Cursor(CursorScope::Account);
        let err = classify(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"ResyncRequired"}}"#,
            graph_ctx(AccountOperation::SyncChanges).with_scope(cursor_scope.clone()),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
        match err.recovery() {
            RecoveryClass::Engine(EngineDirective::RestartScope(_)) => {}
            other => panic!("expected Engine(RestartScope(_)), got {other:?}"),
        }
    }

    #[test]
    fn invalid_delta_token_maps_to_cursor_invalid() {
        let err = classify(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"InvalidDeltaToken"}}"#,
            graph_ctx(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(CursorScope::Account)),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
    }

    #[test]
    fn sync_state_not_found_maps_to_cursor_invalid() {
        for code in ["SyncStateNotFound", "syncStateNotFound"] {
            let err = classify(
                StatusCode::BAD_REQUEST,
                &format!(r#"{{"error":{{"code":"{code}"}}}}"#),
                graph_ctx(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(CursorScope::Account)),
                &[],
            );
            assert!(
                matches!(
                    err.kind(),
                    AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
                ),
                "code={code} did not classify as CursorInvalid"
            );
        }
    }

    #[test]
    fn novel_400_code_on_cursor_scope_restarts_not_terminal() {
        // A 400 carrying a code this build does not yet type
        // (`GraphSignal::Unknown` -> `classify_by_status`) on a cursor
        // scope is almost certainly a delta-token rejection Graph added
        // later. It must route to the cursor-invalid restart, not a
        // terminal `Request(Malformed)`, matching the typed
        // InvalidDeltaToken / 410 cursor cases (forward-compat).
        let err = classify(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"SomeFutureDeltaTokenSignal"}}"#,
            graph_ctx(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(CursorScope::Account)),
            &[],
        );
        assert!(
            matches!(
                err.kind(),
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
            ),
            "expected CursorInvalid, got {:?}",
            err.kind()
        );
        match err.recovery() {
            RecoveryClass::Engine(EngineDirective::RestartScope(_)) => {}
            other => panic!("expected Engine(RestartScope(_)), got {other:?}"),
        }
    }

    #[test]
    fn novel_400_code_off_cursor_scope_stays_malformed() {
        // The same novel 400 outside a cursor scope is a genuine bad
        // request and stays terminal `Request(Malformed)`.
        let err = classify(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"SomeFutureSignal"}}"#,
            graph_ctx(AccountOperation::BulkMove),
            &[],
        );
        assert!(
            matches!(
                err.kind(),
                AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
            ),
            "expected Request(Malformed), got {:?}",
            err.kind()
        );
    }

    #[test]
    fn too_many_requests_carries_retry_hint_and_throttle_scope() {
        let err = classify(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"TooManyRequests"}}"#,
            graph_ctx(AccountOperation::BulkMove),
            &[("retry-after", "30")],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Server(ServerErrorKind::RateLimited)
        ));
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
                assert!(advice.retry_hint.is_some());
            }
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn elapsed_retry_after_date_survives_classification() {
        let date = "Sun, 06 Nov 1994 08:49:37 GMT";
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(784111777);
        let err = classify(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"TooManyRequests"}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[("retry-after", date)],
        );
        let RecoveryClass::Retry(advice) = err.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.retry_hint, Some(RetryHint::At(at)));
    }

    #[test]
    fn retry_after_http_date_parses() {
        // RFC 7231 IMF-fixdate format. Use a date deep in the future
        // so the resulting hint is non-zero even on slow CI clocks.
        let err = classify(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"TooManyRequests"}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[("retry-after", "Wed, 21 Oct 2099 07:28:00 GMT")],
        );
        match err.recovery() {
            RecoveryClass::Retry(advice) => assert!(advice.retry_hint.is_some()),
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn generic_file_error_is_retryable_unavailable() {
        let err = classify(
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"code":"GenericFileError"}}"#,
            graph_ctx(AccountOperation::OpenBlob),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
    }

    #[test]
    fn precondition_failed_maps_to_concurrency_conflict() {
        let err = classify(
            StatusCode::PRECONDITION_FAILED,
            r#"{"error":{"code":"PreconditionFailed"}}"#,
            graph_ctx(AccountOperation::SetIsRead),
            &[],
        );
        assert!(matches!(err.kind(), AccountErrorKind::ConcurrencyConflict));
        match err.recovery() {
            RecoveryClass::Retry(_) => {}
            other => panic!("expected Retry recovery, got {other:?}"),
        }
    }

    #[test]
    fn http_412_without_signal_still_maps_to_concurrency_conflict() {
        let err = classify(
            StatusCode::PRECONDITION_FAILED,
            "",
            graph_ctx(AccountOperation::BulkMove),
            &[],
        );
        assert!(matches!(err.kind(), AccountErrorKind::ConcurrencyConflict));
    }

    #[test]
    fn http_410_on_delta_maps_to_cursor_invalid() {
        let err = classify(
            StatusCode::GONE,
            "",
            graph_ctx(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(CursorScope::Account)),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
        match err.recovery() {
            RecoveryClass::Engine(EngineDirective::RestartScope(_)) => {}
            other => panic!("expected Engine(RestartScope(_)), got {other:?}"),
        }
    }

    #[test]
    fn http_410_outside_delta_maps_to_provider_server_error() {
        let err = classify(
            StatusCode::GONE,
            "",
            graph_ctx(AccountOperation::OpenBlob),
            &[],
        );
        match err.kind() {
            AccountErrorKind::Server(ServerErrorKind::Error { status: Some(410) }) => {}
            other => panic!("expected Server(Error 410), got {other:?}"),
        }
    }

    #[test]
    fn http_404_with_message_scope_maps_to_notfound_message() {
        let err = classify(
            StatusCode::NOT_FOUND,
            "",
            graph_ctx(AccountOperation::HydrateMessage).with_scope(ErrorScope::Message {
                id: ("abc".to_string()).into(),
            }),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::NotFound(ResourceKind::Message)
        ));
    }

    #[test]
    fn graph_request_ids_are_preserved() {
        let err = classify(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"TooManyRequests","innerError":{"request-id":"inner-rid","client-request-id":"inner-crid"}}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[
                ("request-id", "header-rid"),
                ("client-request-id", "header-crid"),
            ],
        );
        let telemetry = err.telemetry_fields();
        assert_eq!(telemetry.request_id, Some("header-rid"));
        assert_eq!(telemetry.trace_id, Some("header-crid"));
    }

    #[test]
    fn unparseable_body_falls_through_to_status_with_support_text() {
        let err = classify(
            StatusCode::INTERNAL_SERVER_ERROR,
            "definitely not JSON",
            graph_ctx(AccountOperation::SyncInventory),
            &[],
        );
        // Non-empty 5xx body that does not parse as an envelope is
        // classified as a contract violation rather than a server
        // error, so the support team can flag Microsoft.
        match err.kind() {
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation) => {}
            other => panic!("expected Protocol(ContractViolation), got {other:?}"),
        }
        let support: Vec<_> = err.support_consented().support_text.into_iter().collect();
        assert!(support.iter().any(|t| t.contains("definitely not JSON")));
    }

    #[test]
    fn body_diagnostic_caps_large_bodies() {
        let large = "x".repeat(STATUS_BODY_CAP * 4);
        let err = classify(
            StatusCode::INTERNAL_SERVER_ERROR,
            &large,
            graph_ctx(AccountOperation::SyncInventory),
            &[],
        );
        let support: Vec<&str> = err.support_consented().support_text;
        let body_text = support
            .iter()
            .find(|t| t.contains("response body"))
            .expect("body diagnostic present");
        assert!(
            body_text.len() < large.len(),
            "body diagnostic was not capped: {} >= {}",
            body_text.len(),
            large.len(),
        );
        assert!(
            body_text.contains("(truncated)"),
            "missing truncation marker"
        );
    }

    /// The whole `GraphSignal` table is reachable only if a REST failure
    /// carrying an envelope gets re-decoded here. `bifrost-net` converts
    /// every 4xx/5xx into an `Error` before this crate sees it, so without
    /// the re-decode the table applied to `$batch` subresponses alone and a
    /// live delta-token expiry classified `Request(Malformed)` -> terminal
    /// `ClientBug` instead of restarting the scope.
    #[test]
    fn a_transport_status_carrying_a_graph_envelope_classifies_on_its_signal() {
        let error = into_account_error(
            GraphError::Net(bifrost_net::Error::Status {
                code: StatusCode::BAD_REQUEST,
                body: body(r#"{"error":{"code":"InvalidDeltaToken","message":"expired"}}"#),
                headers: HeaderMap::new(),
            }),
            graph_ctx(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(CursorScope::Account)),
        );
        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
            ),
            "{:?}",
            error.kind()
        );
        assert!(matches!(
            error.recovery(),
            RecoveryClass::Engine(EngineDirective::RestartScope(_))
        ));

        // Same for a status the retry loop escalated: the envelope rides
        // along in the preserved final response.
        let consent = into_account_error(
            GraphError::Net(bifrost_net::Error::RetryBudgetExhausted {
                final_response: Some(bifrost_net::FinalResponse {
                    status: StatusCode::FORBIDDEN,
                    headers: HeaderMap::new(),
                    body: body(r#"{"error":{"code":"AdminConsentRequired"}}"#),
                }),
                retry_after_history: Vec::new(),
            }),
            graph_ctx(AccountOperation::SyncInventory),
        );
        assert!(matches!(
            consent.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired)
        ));

        // A bodyless failure has no envelope to read, but the status table
        // is then the whole answer and Graph's is the richer one: a bare
        // 412 on a conditioned write is a concurrency conflict the engine
        // resolves with a state refresh, not the opaque server error
        // bifrost-net's table produces.
        let conflict = into_account_error(
            GraphError::Net(bifrost_net::Error::Status {
                code: StatusCode::PRECONDITION_FAILED,
                body: Bytes::new(),
                headers: HeaderMap::new(),
            }),
            graph_ctx(AccountOperation::UpdateFlags),
        );
        assert!(
            matches!(conflict.kind(), AccountErrorKind::ConcurrencyConflict),
            "{:?}",
            conflict.kind()
        );
    }

    /// No envelope, no refinement. The status is then all the evidence
    /// there is, and bifrost-net's mapping additionally carries the
    /// transport's own (retry history, transmission state) that a rebuilt
    /// response error would drop. It also keeps the non-JSON legs honest:
    /// an edge device answering the Autodiscover POST with an HTML 403 is
    /// a permission failure, not the `Protocol(ContractViolation)` the
    /// Graph-envelope classifier reads a non-envelope body as.
    #[test]
    fn a_transport_status_without_a_graph_envelope_keeps_the_net_classification() {
        let error = into_account_error(
            GraphError::Net(bifrost_net::Error::Status {
                code: StatusCode::FORBIDDEN,
                body: body("<html><body>Forbidden</body></html>"),
                headers: HeaderMap::new(),
            }),
            graph_ctx(AccountOperation::Discover),
        );
        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
            ),
            "{:?}",
            error.kind()
        );
    }

    #[test]
    fn net_error_delegates_to_net_conversion() {
        let net_err = bifrost_net::Error::Network {
            message: "connection reset".to_string(),
            transmission_state: TransmissionState::InFlight,
            source: None,
        };
        let err = into_account_error(
            GraphError::Net(net_err),
            graph_ctx(AccountOperation::SyncChanges),
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network)
        ));
        assert_eq!(err.protocol(), Some(Protocol::Graph));
        assert_eq!(err.provider(), Some(Provider::Microsoft));
    }

    #[test]
    fn ews_soap_fault_classifies_to_account_error_with_ews_protocol() {
        let ctx = GraphErrorContext::ews(AccountOperation::PushStream);
        let detail = DiagnosticText::support_only("ErrorAccessDenied".to_string());
        let err = ews_error_to_account_error(
            EwsError::SoapFault {
                code: SoapFaultCode::Client,
                detail,
            },
            ctx,
        );
        assert_eq!(err.protocol(), Some(Protocol::Ews));
        assert_eq!(err.provider(), Some(Provider::Microsoft));
        match err.kind() {
            AccountErrorKind::Server(ServerErrorKind::Error { status: Some(400) }) => {}
            other => panic!("expected Server(Error 400), got {other:?}"),
        }
    }

    #[test]
    fn ews_transport_classifies_via_net_conversion() {
        let net = bifrost_net::Error::Network {
            message: "connection reset".to_string(),
            transmission_state: TransmissionState::InFlight,
            source: None,
        };
        let err = ews_error_to_account_error(
            EwsError::Transport(net),
            GraphErrorContext::ews(AccountOperation::PushStream),
        );
        assert_eq!(err.protocol(), Some(Protocol::Ews));
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
        ));
    }

    #[test]
    fn ews_malformed_xml_is_protocol_parse_failed() {
        let err = ews_error_to_account_error(
            EwsError::MalformedXml(DiagnosticText::support_only("bad xml".to_string())),
            GraphErrorContext::ews(AccountOperation::PushStream),
        );
        assert_eq!(err.protocol(), Some(Protocol::Ews));
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        ));
    }

    #[test]
    fn mutation_2xx_is_applied() {
        let outcome = mutation_item_outcome(
            204,
            HeaderMap::new(),
            Bytes::new(),
            false,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::UpdateFlags,
            ErrorScope::Message {
                id: ("m1".to_string()).into(),
            },
            None,
        );
        assert!(matches!(
            outcome,
            bifrost_types::ItemOutcome::Succeeded(ref s) if matches!(s.output, bifrost_types::MutationSuccess::Applied)
        ));
    }

    #[test]
    fn mutation_destroy_404_is_skipped() {
        let outcome = mutation_item_outcome(
            404,
            HeaderMap::new(),
            Bytes::new(),
            true,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::BulkDestroy,
            ErrorScope::Message {
                id: ("m1".to_string()).into(),
            },
            None,
        );
        assert!(matches!(
            outcome,
            bifrost_types::ItemOutcome::Succeeded(ref s) if matches!(s.output, bifrost_types::MutationSuccess::Skipped)
        ));
    }

    #[test]
    fn mutation_non_destroy_404_is_failed_notfound() {
        let outcome = mutation_item_outcome(
            404,
            HeaderMap::new(),
            Bytes::new(),
            false,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::SetIsRead,
            ErrorScope::Message {
                id: ("m1".to_string()).into(),
            },
            None,
        );
        match outcome {
            bifrost_types::ItemOutcome::Failed(failure) => {
                assert!(matches!(
                    failure.error.kind(),
                    AccountErrorKind::NotFound(ResourceKind::Message)
                ));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn mutation_412_is_failed_concurrency_conflict_not_skipped() {
        let outcome = mutation_item_outcome(
            412,
            HeaderMap::new(),
            Bytes::new(),
            false,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::UpdateFlags,
            ErrorScope::Message {
                id: ("m1".to_string()).into(),
            },
            None,
        );
        match outcome {
            bifrost_types::ItemOutcome::Failed(failure) => {
                assert!(matches!(
                    failure.error.kind(),
                    AccountErrorKind::ConcurrencyConflict
                ));
            }
            other => panic!("expected Failed (412 must not be Skipped), got {other:?}"),
        }
    }

    #[test]
    fn mutation_429_carries_retry_hint_and_throttle_scope() {
        let outcome = mutation_item_outcome(
            429,
            headers(&[("retry-after", "10")]),
            Bytes::new(),
            false,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::BulkMove,
            ErrorScope::Message {
                id: ("m1".to_string()).into(),
            },
            None,
        );
        match outcome {
            bifrost_types::ItemOutcome::Failed(failure) => {
                assert!(matches!(
                    failure.error.kind(),
                    AccountErrorKind::Server(ServerErrorKind::RateLimited)
                ));
                match failure.error.recovery() {
                    RecoveryClass::Retry(advice) => {
                        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
                        assert!(advice.retry_hint.is_some());
                    }
                    other => panic!("expected Retry, got {other:?}"),
                }
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn mutation_429_routes_non_idempotent_with_retry_classification() {
        // Bulk-move per-item 429: with `AttemptCause(Acknowledged)`
        // pushed at the response boundary, recovery resolves to
        // `Retry::SameRequest` rather than `ProviderRefused`.
        let outcome = mutation_item_outcome(
            429,
            HeaderMap::new(),
            Bytes::new(),
            false,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::BulkMove,
            ErrorScope::Message {
                id: ("m1".to_string()).into(),
            },
            None,
        );
        let bifrost_types::ItemOutcome::Failed(failure) = outcome else {
            panic!("expected Failed");
        };
        match failure.error.recovery() {
            RecoveryClass::Retry(_) => {}
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn json_parse_failure_is_protocol_parse_failed() {
        let err = into_account_error(
            GraphError::Json {
                message: "expected `,` or `}`".to_string(),
                body: Some(body("{not-json")),
            },
            graph_ctx(AccountOperation::SyncInventory),
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        ));
    }

    /// `GraphError::Json` also carries the Autodiscover XML parsers' and the
    /// OneDrive 202 range read's failures. The diagnostic used to call every
    /// one of them a "Graph response JSON parse failed", so a support export
    /// for a truncated Autodiscover XML answer named the wrong format and
    /// the wrong service. The producer's message names its source; the
    /// label must not contradict it.
    #[test]
    fn a_non_json_parse_failure_is_not_labelled_a_json_parse() {
        for message in [
            "Autodiscover GetUserSettings: malformed XML: unexpected end",
            "OneDrive 202 nextExpectedRanges entry is not a byte range",
        ] {
            let err = into_account_error(
                GraphError::Json {
                    message: message.to_string(),
                    body: None,
                },
                graph_ctx(AccountOperation::Discover),
            );
            let debug = format!("{err:?}");
            assert!(!debug.contains("JSON"), "{debug}");
            assert!(debug.contains(message), "{debug}");
            assert_eq!(
                err.kind(),
                &AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
            );
        }
    }

    // `GraphAccount::remove_from_container` is a one-line tail call to
    // `unsupported_account_error(RemoveFromContainer)`, so pinning the
    // helper fully pins the method's behavior end-to-end: the returned
    // `AccountError` carries the exact `Unsupported(RemoveFromContainer)`
    // kind and operation, not a generic protocol error.
    #[test]
    fn remove_from_container_is_unsupported() {
        let err = unsupported_account_error(AccountOperation::RemoveFromContainer);
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::RemoveFromContainer)
        ));
        assert_eq!(err.operation(), Some(AccountOperation::RemoveFromContainer));
        assert!(matches!(err.recovery(), RecoveryClass::Unsupported(_)));
    }

    fn forbidden_response() -> GraphError {
        GraphError::Response(GraphResponseError::from_response(
            StatusCode::FORBIDDEN,
            HeaderMap::new(),
            body(r#"{"error":{"code":"Forbidden","message":"nope"}}"#),
        ))
    }

    fn foreign_email_scope() -> CursorScope {
        CursorScope::FolderType {
            folder: crate::account::foreign::encode_foreign("shared@contoso.com", "AAMk"),
            ty: bifrost_types::ObjectType::Email,
        }
    }

    fn primary_email_scope() -> CursorScope {
        CursorScope::FolderType {
            folder: bifrost_types::FolderId("inbox".to_string()),
            ty: bifrost_types::ObjectType::Email,
        }
    }

    #[test]
    fn shared_mailbox_permission_loss_derives_disable_scope() {
        let scope = foreign_email_scope();
        let owner = MailboxId("shared@contoso.com".to_string());
        let err = graph_shared_scope_error(
            forbidden_response(),
            &scope,
            Some(&owner),
            GraphErrorContext::graph(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(scope.clone())),
        );

        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        ));
        assert_eq!(
            *err.recovery(),
            RecoveryClass::Engine(EngineDirective::DisableScope(scope.clone()))
        );
        assert_eq!(err.scope(), Some(&ErrorScope::Cursor(scope)));
    }

    #[test]
    fn public_folder_access_denied_quarantines_scope() {
        // A public folder's content mailbox is always its owner, so an
        // EWS ErrorAccessDenied quarantines just this scope via the A5a
        // ScopeRevoked path rather than escalating account-wide.
        let scope = CursorScope::Folder(bifrost_types::FolderId("AAMkPF=".to_string()));
        let owner = MailboxId("content@contoso.com".to_string());
        let err = ews_shared_scope_error(
            EwsError::SoapFault {
                code: SoapFaultCode::ErrorAccessDenied,
                detail: DiagnosticText::support_only("ErrorAccessDenied".to_string()),
            },
            &scope,
            Some(&owner),
            GraphErrorContext::ews(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(scope.clone())),
        );

        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        ));
        assert_eq!(
            *err.recovery(),
            RecoveryClass::Engine(EngineDirective::DisableScope(scope))
        );
    }

    // End-to-end through the WIRE parse path: a 200-OK EWS body whose
    // ResponseMessage is `ResponseClass="Error"` / `ErrorAccessDenied`
    // (the canonical EWS error shape, NOT a SOAP `<Fault>`) must reach
    // `check_response_error` -> `EwsError::SoapFault` ->
    // `ews_shared_scope_error` -> `ScopeRevoked` -> `DisableScope`. The
    // direct-construction test above bypasses the parse; this one proves
    // the quarantine is reachable from the actual response body.
    #[test]
    fn wire_error_access_denied_quarantines_public_folder_scope() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Error">
          <m:MessageText>Access is denied.</m:MessageText>
          <m:ResponseCode>ErrorAccessDenied</m:ResponseCode>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        let parsed = crate::ews::check_response_error(xml)
            .expect_err("ResponseClass=Error must surface as an EwsError");

        let scope = CursorScope::Folder(bifrost_types::FolderId("AAMkPF=".to_string()));
        let owner = MailboxId("content@contoso.com".to_string());
        let err = ews_shared_scope_error(
            parsed,
            &scope,
            Some(&owner),
            GraphErrorContext::ews(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(scope.clone())),
        );

        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        ));
        assert_eq!(
            *err.recovery(),
            RecoveryClass::Engine(EngineDirective::DisableScope(scope))
        );
    }

    // End-to-end through the wire parse: `ErrorServerBusy` in a 200-OK
    // body must reach `Server(RateLimited)` with an `Account` throttle
    // scope so the EWS worker backs off instead of hot-looping.
    #[test]
    fn wire_error_server_busy_is_rate_limited_with_account_throttle() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Error">
          <m:MessageText>The server is busy.</m:MessageText>
          <m:ResponseCode>ErrorServerBusy</m:ResponseCode>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;
        let parsed = crate::ews::check_response_error(xml).expect_err("ServerBusy must surface");
        let err = ews_error_to_account_error(
            parsed,
            GraphErrorContext::ews(AccountOperation::PushStream),
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Server(ServerErrorKind::RateLimited)
        ));
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
            }
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn streaming_subscription_response_codes_reconnect() {
        for code in [
            "ErrorSubscriptionNotFound",
            "ErrorInvalidSubscription",
            "ErrorSubscriptionUnsubscribed",
            "ErrorInvalidWatermark",
            "ErrorInternalServerTransientError",
        ] {
            let xml = format!(
                r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Error">
          <m:MessageText>subscription problem</m:MessageText>
          <m:ResponseCode>{code}</m:ResponseCode>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#
            );
            let parsed = crate::ews::check_response_error(&xml)
                .expect_err("an Error-classed response must surface");
            let err = ews_error_to_account_error(
                parsed,
                GraphErrorContext::ews(AccountOperation::PushStream),
            );
            assert!(matches!(
                err.kind(),
                AccountErrorKind::Server(ServerErrorKind::Unavailable)
            ));
            assert!(
                err.recovery().is_retryable(),
                "{code} recovery {:?} is not retryable",
                err.recovery()
            );
        }
    }

    #[test]
    fn ews_item_not_found_is_a_scoped_not_found_not_a_contract_violation() {
        // The classified EWS codes must keep their typed mapping: a
        // missing public-folder item is a per-item NotFound, so a hydration
        // batch reports it against that id rather than tearing down.
        let err = ews_error_to_account_error(
            EwsError::SoapFault {
                code: SoapFaultCode::ErrorItemNotFound,
                detail: DiagnosticText::support_only("gone".to_string()),
            },
            GraphErrorContext::ews(AccountOperation::Hydrate).with_scope(ErrorScope::Message {
                id: ("AAMkItem=".to_string()).into(),
            }),
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::NotFound(ResourceKind::Message)
        ));
    }

    #[test]
    fn nc2b_review_ews_not_found_keeps_existing_mail_read_and_unscoped_defaults() {
        let message = ErrorScope::Message {
            id: "public-message".into(),
        };
        for (operation, scope) in [
            (AccountOperation::Hydrate, Some(message.clone())),
            (AccountOperation::HydrateMessage, Some(message.clone())),
            (AccountOperation::OpenBlob, Some(message.clone())),
            (
                AccountOperation::SyncInventory,
                Some(ErrorScope::Cursor(CursorScope::Folder(
                    bifrost_types::FolderId("public-folder".into()),
                ))),
            ),
            (
                AccountOperation::SyncChanges,
                Some(ErrorScope::Cursor(CursorScope::Folder(
                    bifrost_types::FolderId("public-folder".into()),
                ))),
            ),
            (AccountOperation::PushStream, None),
        ] {
            let error = ews_error_to_account_error(
                EwsError::SoapFault {
                    code: SoapFaultCode::ErrorItemNotFound,
                    detail: DiagnosticText::support_only("gone".to_string()),
                },
                GraphErrorContext {
                    scope: scope.clone(),
                    ..GraphErrorContext::ews(operation)
                },
            );
            assert_eq!(
                error.kind(),
                &AccountErrorKind::NotFound(ResourceKind::Message)
            );
            assert_eq!(error.scope(), scope.as_ref());
            assert!(error.chain().iter().any(|cause| matches!(cause, Cause::Request(RequestCause::NotFound { what: ResourceKind::Message, id }) if id.as_deref() == if scope.as_ref() == Some(&message) { Some("public-message") } else { None })));
        }
    }

    #[test]
    fn ews_mailbox_move_in_progress_is_retryable_not_terminal() {
        let err = ews_error_to_account_error(
            EwsError::SoapFault {
                code: SoapFaultCode::ErrorMailboxMoveInProgress,
                detail: DiagnosticText::support_only("moving".to_string()),
            },
            GraphErrorContext::ews(AccountOperation::SyncChanges),
        );
        assert!(!err.recovery().is_terminal());
    }

    #[test]
    fn primary_mailbox_permission_loss_stays_terminal() {
        let scope = primary_email_scope();
        // A primary folder is untagged (`owner == None`): the same 403
        // must still derive terminal `NoPermission`, not quarantine.
        let err = graph_shared_scope_error(
            forbidden_response(),
            &scope,
            None,
            GraphErrorContext::graph(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(scope.clone())),
        );

        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ));
        assert!(err.recovery().is_terminal());
        assert!(matches!(err.recovery(), RecoveryClass::NoPermission { .. }));
    }
}
