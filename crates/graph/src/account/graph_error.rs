//! Graph account error boundary.
//!
//! Every Graph-side failure that leaves a streaming helper or
//! `Account` method passes through `into_account_error` (REST) or
//! `ews_error_to_account_error` (EWS). Transport variants use
//! `bifrost_net::into_account_error_with_throttle_scope` with Graph
//! provider/protocol context and an explicit account throttle scope.
//! Graph response variants build the `AccountError` directly
//! through `AccountErrorBuilder`, classified by the typed
//! `GraphSignal`. No path in this module looks at `error.message` text
//! for control flow.

use bifrost_net::NetErrorContext;
use bifrost_types::{
    AccessCause, AccountError, AccountErrorBuilder, AccountOperation, AttemptCause, AuthCause,
    Cause, CursorScope, DiagnosticText, ErrorScope, GraphSignal, InternalCause, InternalErrorKind,
    MailboxUnavailableKind, PrimaryCause, PrincipalId, Protocol, ProtocolErrorKind, Provider,
    RequestCause, ResourceKind, RetryHint, ServerCause, StateCause, ThrottleScope,
    TransmissionState, WireCause,
};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, RETRY_AFTER};

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
    /// REST funnel tagged the failure (`GraphError::MailboxContext`).
    /// Stamped on the error only when it classifies as a throttle.
    pub(crate) budget: Option<bifrost_types::ThrottleBudget>,
}

impl GraphErrorContext {
    pub(crate) fn identity(&self) -> bifrost_types::AccountErrorContext {
        bifrost_types::AccountErrorContext::new(Some(Provider::Microsoft), self.protocol)
    }

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
    ///
    /// Call sites must also declare `ReplayPolicy::Idempotent` for wire
    /// replay. Builder overrides compose monotonically, so contrary net
    /// evidence still wins if a request was declared unreplayable.
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
                PrimaryCause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(message),
                }),
            ),
            &ctx,
        ),
        GraphError::MailboxContext { budget, source } => {
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
                PrimaryCause::Protocol {
                    kind: ProtocolErrorKind::ContractViolation,
                    wire: WireCause::MalformedResponse {
                        protocol: ctx.protocol,
                        detail: Some(detail.clone()),
                    },
                },
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
                PrimaryCause::Protocol {
                    kind: ProtocolErrorKind::ContractViolation,
                    wire: WireCause::MalformedResponse {
                        protocol: ctx.protocol,
                        detail: Some(detail.clone()),
                    },
                },
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
            PrimaryCause::Internal(InternalCause::new(kind, Some(detail.clone()))),
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
/// straight to `bifrost_net::into_account_error_with_throttle_scope` with
/// Graph's explicit account throttle scope classifies them on the HTTP
/// status alone and throws Graph's typed `error.code` away, so the
/// whole `GraphSignal` table below applied only to `$batch` subresponses
/// and never to an ordinary REST call: a 400 `InvalidDeltaToken` - the
/// documented delta-expiry signal - landed on `Request(Malformed)` ->
/// `ClientBug` instead of `SyncState(CursorInvalid)` -> `RestartScope`,
/// and a 403 `AdminConsentRequired` on `PermissionDenied` instead of
/// `NeedsAdminConsent`.
///
/// Refinement needs the body to be Graph's: either an envelope decoded, or
/// there was no body at all and the status table is the whole answer (a
/// bare 412 refreshes state through the shared status table, and a bare
/// 400 on a cursor scope is the forward-compat
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
    apply_idempotency_override(net_transport_error(net, ctx), ctx)
}

/// A typed hop-policy refusal is created before its operation and cursor scope
/// are known. Add the account boundary context without changing its recovery.
fn net_transport_error(net: bifrost_net::Error, ctx: &GraphErrorContext) -> AccountError {
    let error = bifrost_net::into_account_error_with_throttle_scope(
        net,
        ctx.to_net_ctx(),
        http_error_policy(ctx),
    );
    let builder = error
        .into_builder()
        .context(ctx.identity())
        .operation(ctx.operation);
    let builder = match &ctx.scope {
        Some(scope) => builder.scope(scope.clone()),
        None => builder,
    };
    builder
        .try_build()
        .expect("valid contextual transport error")
}

fn http_error_policy(ctx: &GraphErrorContext) -> bifrost_net::HttpErrorPolicy {
    bifrost_net::HttpErrorPolicy {
        status: bifrost_net::StatusPolicy {
            resource: None,
            // Unknown 400-coded Graph cursor signals follow delta expiry.
            overrides: &[(400, bifrost_net::StatusOverride::CursorInvalid)],
        },
        throttle_scope: throttle_scope_for(),
        throttle_budget: ctx.budget.clone(),
    }
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
/// `NetErrorContext` carries no override channel, so the context's override
/// is stamped after net translates. `into_builder` keeps every override,
/// and the builder composes them monotonically: a narrowing net stamped
/// itself cannot be widened by this context. The kind and cause chain are
/// untouched; only `recovery` recomputes. A call site that declared nothing
/// pays nothing - there is no rebuild.
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
/// transport variants to `bifrost_net::into_account_error_with_throttle_scope`
/// with the EWS context and explicit account throttle scope so the recovery
/// taxonomy is identical to REST.
#[must_use]
pub(crate) fn ews_error_to_account_error(error: EwsError, ctx: GraphErrorContext) -> AccountError {
    debug_assert!(matches!(ctx.protocol, Protocol::Ews));
    match error {
        EwsError::Transport(net) => {
            apply_idempotency_override(net_transport_error(net, &ctx), &ctx)
        }
        EwsError::HttpStatus { status, body } => {
            // Wrap the raw HTTP failure as a Graph-style response so
            // the standard status-fallback classification applies,
            // but stamp the protocol as EWS via the context. The body
            // stays available for classification; diagnostics export only its size.
            let response = GraphResponseError::from_response(status, HeaderMap::new(), body);
            response_to_account_error(response, &ctx)
        }
        EwsError::SoapFault { code, detail } => soap_fault_to_account_error(code, detail, &ctx),
        EwsError::MalformedXml(_) => {
            let detail = DiagnosticText::support_only("malformed EWS XML response");
            let builder = base_builder(
                &ctx,
                PrimaryCause::Protocol {
                    kind: ProtocolErrorKind::ParseFailed,
                    wire: WireCause::MalformedResponse {
                        protocol: ctx.protocol,
                        detail: Some(detail.clone()),
                    },
                },
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
                PrimaryCause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(message),
                }),
            ),
            &ctx,
        ),
    }
}

fn soap_fault_to_account_error(
    code: SoapFaultCode,
    _detail: DiagnosticText,
    ctx: &GraphErrorContext,
) -> AccountError {
    let detail = DiagnosticText::support_only(format!("EWS SOAP fault: {code:?}"));
    // SOAP 1.1 fault codes map onto the same recovery taxonomy as
    // HTTP statuses: `Server` ~ 5xx (unavailable), `Client` ~ 4xx
    // (refused). Microsoft EWS embeds finer-grained codes in the
    // `<detail>` payload. Keep it private: arbitrary SOAP prose may echo
    // credentials, and typed fault codes alone choose recovery.
    // Cursor scopes deliberately do not imply Mailbox here: that would turn
    // folder-scoped 404s into object-not-found errors and change recovery.
    let resource = ctx.scope.as_ref().and_then(ErrorScope::resource);
    let cause = match code {
        SoapFaultCode::Server => {
            PrimaryCause::Server(ServerCause::Unavailable { retry_hint: None })
        }
        SoapFaultCode::Client | SoapFaultCode::MustUnderstand | SoapFaultCode::VersionMismatch => {
            PrimaryCause::Server(ServerCause::Error { status: Some(400) })
        }
        // Microsoft EWS specific faults the convergence audit
        // identified as recoverable but previously collapsed to
        // ProviderContractViolation.
        SoapFaultCode::ErrorAccessDenied => {
            PrimaryCause::Access(AccessCause::PermissionDenied { resource })
        }
        SoapFaultCode::ErrorImpersonateUserDenied => {
            PrimaryCause::Access(AccessCause::ConditionalAccessBlocked)
        }
        SoapFaultCode::ErrorServerBusy => PrimaryCause::Server(ServerCause::RateLimited {
            throttle: Default::default(),
            retry_hint: None,
        }),
        SoapFaultCode::ErrorMailboxStoreUnavailable | SoapFaultCode::ErrorMailboxMoveInProgress => {
            PrimaryCause::Access(AccessCause::MailboxUnavailable {
                kind: MailboxUnavailableKind::Transient,
            })
        }
        SoapFaultCode::ErrorNonExistentMailbox => PrimaryCause::Request(RequestCause::NotFound {
            what: ResourceKind::Mailbox,
            id: ctx
                .scope
                .as_ref()
                .and_then(ErrorScope::object_id)
                .map(str::to_owned),
        }),
        SoapFaultCode::ErrorItemNotFound => {
            // Unscoped streaming and folder walks retain the historical item
            // fallback; typed hydration identifies the actual resource.
            let resource = resource.unwrap_or(ResourceKind::Message);
            PrimaryCause::Request(RequestCause::NotFound {
                what: resource,
                id: ctx
                    .scope
                    .as_ref()
                    .and_then(ErrorScope::object_id)
                    .map(str::to_owned),
            })
        }
        // A streaming subscription is intentionally short lived. These
        // server codes mean the current stream must be discarded and the
        // outer worker must Subscribe again, not that the account contract
        // is terminally broken.
        SoapFaultCode::ErrorStreamingSubscriptionInvalid => {
            PrimaryCause::Server(ServerCause::Unavailable { retry_hint: None })
        }
        SoapFaultCode::Unknown => PrimaryCause::Protocol {
            kind: ProtocolErrorKind::ContractViolation,
            wire: WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: Some(detail.clone()),
            },
        },
    };
    let mut builder = base_builder(ctx, cause.clone()).text(detail);
    builder = push_attempt(builder, TransmissionState::Acknowledged);
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
/// The lookup/setting label and server message remain private. Only
/// shape-checked codes enter diagnostics.
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
/// permanent. The native code is exported only after the shared shape check.
#[must_use]
pub(crate) fn autodiscover_pox_error_to_account_error(
    what: &str,
    code: &str,
    message: Option<&str>,
    ctx: &GraphErrorContext,
) -> AccountError {
    let classify_as = match code.trim() {
        "500" => "InvalidUser",
        "600" => "InvalidRequest",
        other => other,
    };
    autodiscover_error(what, classify_as, code, message, ctx)
}

/// The shared Autodiscover table: `classify_as` picks the kind (a SOAP
/// `ErrorCode` name), `native_code` is what the server actually sent.
fn autodiscover_error(
    _what: &str,
    classify_as: &str,
    native_code: &str,
    _message: Option<&str>,
    ctx: &GraphErrorContext,
) -> AccountError {
    let code = bifrost_net::account_error::correlation_id(native_code);
    let is = |name: &str| classify_as.eq_ignore_ascii_case(name);
    let cause = if is("ServerBusy") {
        PrimaryCause::Server(ServerCause::RateLimited {
            throttle: Default::default(),
            retry_hint: None,
        })
    } else if is("InternalServerError") {
        PrimaryCause::Server(ServerCause::Unavailable { retry_hint: None })
    } else if is("InvalidUser") {
        PrimaryCause::Request(RequestCause::NotFound {
            what: ResourceKind::Mailbox,
            id: None,
        })
    } else if is("InvalidRequest") || is("InvalidSetting") {
        PrimaryCause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("Autodiscover refused the request"),
        })
    } else {
        PrimaryCause::Server(ServerCause::Error { status: None })
    };
    // Server prose may echo credentials even under support consent.
    let mut builder = base_builder(ctx, cause.clone())
        .text(DiagnosticText::support_only("Autodiscover error response"));
    if let Some(code) = code {
        builder = builder.native_code(code);
    }
    builder = push_attempt(builder, TransmissionState::Acknowledged);
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
        PrimaryCause::Protocol {
            kind,
            wire: WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: Some(detail.clone()),
            },
        },
    )
    .text(detail);
    finish(push_attempt(builder, TransmissionState::Acknowledged), ctx)
}

/// `GraphError::Json`: a success body that did not parse. Carries
/// `AttemptCause::unreadable_success`, so partial-commit application reads
/// a write leg it fails as `Uncertain`.
///
/// The variant is no longer only Graph REST JSON: the Autodiscover XML
/// parsers and the OneDrive upload-session 202 read raise it too, each with a
/// message naming its source. The label therefore claims only what every
/// producer shares - the provider's response could not be read - and leaves
/// the format unspecified. Producer parser prose may quote credentials and
/// remains private.
fn json_parse_to_account_error(
    _message: &str,
    body: Option<bytes::Bytes>,
    ctx: &GraphErrorContext,
) -> AccountError {
    let detail = DiagnosticText::support_only("provider response did not parse");
    let mut builder = base_builder(
        ctx,
        PrimaryCause::Protocol {
            kind: ProtocolErrorKind::ParseFailed,
            wire: WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: Some(detail.clone()),
            },
        },
    )
    .text(detail);
    if let Some(body) = body
        && let Some(text) = body_diagnostic(&body)
    {
        builder = builder.text(text);
    }
    // Every producer raises this only on a success status, so the write the
    // body reports on may have landed: unreadable-success evidence.
    builder = builder.push_cause(Cause::Attempt(AttemptCause::unreadable_success()));
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
        message: _,
        inner,
    } = response;

    let code = status.as_u16();
    let retry_hint = parse_retry_after_header(&headers);

    // `signal == None` means we could not decode a Graph error
    // envelope; that is provider contract violation territory unless
    // the status code itself carries enough information (5xx is enough,
    // regardless of whether an edge server supplied an HTML body).
    let cause = match signal.as_ref() {
        Some(signal) => classify(ctx, code, signal, retry_hint),
        None => classify_no_envelope(ctx, code, retry_hint, &body),
    };

    let mut builder = base_builder(ctx, cause.clone());
    // Attach the GraphSignal as an inner wire cause whenever the
    // primary cause is not itself a wire cause, so support tooling
    // can read the typed provider code regardless of which semantic
    // bucket the kind landed in.
    if !matches!(cause, PrimaryCause::Protocol { .. })
        && let Some(signal) = signal.as_ref()
    {
        builder = builder.push_cause(Cause::Wire(WireCause::Graph(match signal {
            GraphSignal::Unknown { code } => GraphSignal::Unknown {
                code: bifrost_net::account_error::correlation_id(code).unwrap_or_default(),
            },
            signal => signal.clone(),
        })));
    }
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder = response_diagnostics(builder, status, &headers, &body, inner.as_ref());

    // Keep the provider code, but never export free-form response prose.
    if let Some(code) = inner
        .as_ref()
        .and_then(|inner| inner.code.as_deref())
        .and_then(bifrost_net::account_error::correlation_id)
    {
        builder = builder.text(DiagnosticText::support_only(format!(
            "innerError.code: {code}"
        )));
    }

    // The retry hint already lives structurally on `ServerCause`; no
    // builder side-channel is needed. `throttle_scope` annotation
    // applies only on rate-limit / quota paths. A plain 503 is unavailable.
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
) -> PrimaryCause {
    match signal {
        GraphSignal::InvalidAuthenticationToken => {
            PrimaryCause::Auth(AuthCause::ReauthorizationRequired)
        }
        GraphSignal::AccessDenied | GraphSignal::Forbidden => {
            PrimaryCause::Access(AccessCause::PermissionDenied {
                resource: ctx.scope.as_ref().and_then(ErrorScope::resource),
            })
        }
        GraphSignal::AccessRestricted | GraphSignal::ConditionalAccessBlocked => {
            PrimaryCause::Access(AccessCause::ConditionalAccessBlocked)
        }
        GraphSignal::AdminConsentRequired => {
            PrimaryCause::Access(AccessCause::AdminConsentRequired {
                needed: "admin-consent",
            })
        }
        GraphSignal::MailboxNotEnabledForRestApi => {
            PrimaryCause::Access(AccessCause::MailboxNotLicensed)
        }
        GraphSignal::MailboxStoreUnavailable => {
            PrimaryCause::Access(AccessCause::MailboxUnavailable {
                kind: MailboxUnavailableKind::Transient,
            })
        }
        GraphSignal::ResyncRequired
        | GraphSignal::InvalidDeltaToken
        | GraphSignal::SyncStateNotFound => cursor_invalid_or_protocol(ctx),
        GraphSignal::TooManyRequests => PrimaryCause::Server(ServerCause::RateLimited {
            throttle: Default::default(),
            retry_hint,
        }),
        GraphSignal::GenericFileError => {
            PrimaryCause::Server(ServerCause::Unavailable { retry_hint })
        }
        GraphSignal::PreconditionFailed => PrimaryCause::State(StateCause::ConcurrencyConflict),
        GraphSignal::Gone => classify_by_status(ctx, 410, retry_hint),
        GraphSignal::NotFound => classify_by_status(ctx, 404, retry_hint),
        GraphSignal::Unknown { .. } => classify_by_status(ctx, code, retry_hint),
        // GraphSignal is #[non_exhaustive] from bifrost-types; new
        // variants land here unhandled and fall through to status-based
        // classification until explicit dispatch is added.
        _ => classify_by_status(ctx, code, retry_hint),
    }
}

/// Cursor expiry needs a cursor recovery target. Signals raised outside that
/// context retain the existing protocol-contract classification and audit path.
fn cursor_invalid_or_protocol(ctx: &GraphErrorContext) -> PrimaryCause {
    if let Some(ErrorScope::Cursor(scope)) = &ctx.scope {
        PrimaryCause::State(StateCause::CursorInvalid {
            scope: scope.clone(),
        })
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
        PrimaryCause::Protocol {
            kind: ProtocolErrorKind::ContractViolation,
            wire: WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: Some(DiagnosticText::support_only(
                    "Graph CursorInvalid signal raised outside cursor scope".to_string(),
                )),
            },
        }
    }
}

fn classify_by_status(
    ctx: &GraphErrorContext,
    code: u16,
    retry_hint: Option<RetryHint>,
) -> PrimaryCause {
    bifrost_net::classify_status(
        code,
        ctx.scope.as_ref(),
        retry_hint,
        http_error_policy(ctx).status,
    )
}

/// `signal == None`: the body did not decode as a Graph error
/// envelope. For 5xx the status is enough to classify as
/// a server failure using the status mapping. Only a 4xx carrying a
/// *non-empty* body that failed to parse as a Graph error envelope is treated as a
/// provider-contract violation (Graph documents every error code it
/// ships, so a non-empty body that does not decode is anomalous); an
/// empty 4xx falls through to the plain status mapping.
fn classify_no_envelope(
    ctx: &GraphErrorContext,
    code: u16,
    retry_hint: Option<RetryHint>,
    body: &bytes::Bytes,
) -> PrimaryCause {
    // An Outlook user expects a temporary server failure to retry even if
    // a proxy replaces its Graph JSON with HTML. Keep status-based recovery
    // (including quota classification), with the body retained in diagnostics.
    if (500..600).contains(&code) || body.is_empty() {
        classify_by_status(ctx, code, retry_hint)
    } else {
        // Non-empty body that did not parse as a Graph error
        // envelope -> protocol contract violation.
        PrimaryCause::Protocol {
            kind: ProtocolErrorKind::ContractViolation,
            wire: WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: Some(DiagnosticText::support_only(format!(
                    "HTTP {code} with non-envelope body",
                ))),
            },
        }
    }
}

fn base_builder(ctx: &GraphErrorContext, cause: PrimaryCause) -> AccountErrorBuilder {
    AccountErrorBuilder::new(http_error_policy(ctx).apply_throttle(cause))
        .context(ctx.identity())
        .operation(ctx.operation)
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
    use bifrost_net::account_error::{correlation_header, correlation_id, request_id_from_headers};
    let request_id = request_id_from_headers(headers).or_else(|| {
        inner
            .and_then(|i| i.request_id.as_deref())
            .and_then(correlation_id)
    });
    if let Some(id) = request_id {
        builder = builder.request_id(id);
    }
    let client_id = correlation_header(headers, "client-request-id").or_else(|| {
        inner
            .and_then(|i| i.client_request_id.as_deref())
            .and_then(correlation_id)
    });
    if let Some(trace) = client_id {
        builder = builder.trace_id(trace);
    }

    builder = bifrost_net::account_error::push_header_names(builder, headers);
    if let Some(text) = body_diagnostic(body) {
        builder = builder.text(text);
    }
    builder
}

/// Captured sizes support diagnosis without exporting echoed credentials.
/// An empty body carries no evidence and adds no line.
fn body_diagnostic(body: &bytes::Bytes) -> Option<DiagnosticText> {
    (!body.is_empty()).then(|| bifrost_net::account_error::body_size_text(body.len()))
}

/// Preserve absolute HTTP-dates, including elapsed deadlines, for recovery.
fn parse_retry_after_header(headers: &HeaderMap) -> Option<RetryHint> {
    bifrost_net::request::parse_retry_after_hint(headers.get(RETRY_AFTER))
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
fn throttle_scope_for() -> Option<ThrottleScope> {
    Some(ThrottleScope::Account)
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
    owner: Option<&PrincipalId>,
    ctx: GraphErrorContext,
) -> AccountError {
    into_account_error(error, ctx).revoke_foreign_scope(scope.clone(), owner)
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
    owner: Option<&PrincipalId>,
    ctx: GraphErrorContext,
) -> AccountError {
    ews_error_to_account_error(error, ctx).revoke_foreign_scope(scope.clone(), owner)
}

/// Build an `AccountError` for an operation this account does not support.
#[must_use]
pub(crate) fn unsupported_account_error(operation: AccountOperation) -> AccountError {
    AccountError::unsupported(operation, Protocol::Graph)
        .into_builder()
        .context(GraphErrorContext::graph(operation).identity())
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
    AccountError::unsupported(operation, Protocol::Graph)
        .into_builder()
        .context(GraphErrorContext::graph(operation).identity())
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
    AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::Malformed {
        detail: DiagnosticText::support_only(detail.into()),
    }))
    .operation(operation)
    .context(GraphErrorContext::graph(operation).identity())
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
    AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::InvalidArgument {
        field: Some(field),
        message: Some(DiagnosticText::support_only(detail.into())),
    }))
    .operation(operation)
    .context(GraphErrorContext::graph(operation).identity())
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
    AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::Malformed {
        detail: DiagnosticText::support_only(detail.into()),
    }))
    .operation(operation)
    .context(GraphErrorContext::graph(operation).identity())
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
    AccountErrorBuilder::new(PrimaryCause::State(StateCause::ConcurrencyConflict))
        .push_cause(Cause::Attempt(bifrost_types::AttemptCause::new(
            bifrost_types::TransmissionState::Unsent,
        )))
        .operation(operation)
        .context(GraphErrorContext::graph(operation).identity())
        .scope(scope)
        .text(DiagnosticText::support_only(detail.into()))
        .try_build()
        .expect("valid account error classification")
}

/// A public resource whose folder a COMPLETE discovery pass did not find.
///
/// Ruling (wave 2): before discovery has completed, a missing route is local
/// state not yet restored, reported by [`stale_local_state_error`] as a
/// retryable conflict. Once a full hierarchy browse has finished without the
/// folder, the folder is no longer readable to this user (deleted, moved out
/// of reach, or permission withdrawn). An enterprise Outlook user then expects
/// the item to stop being retried and to drop out of view, as the server's
/// own `ErrorItemNotFound` would make it: typed `NotFound`, refused before
/// any request.
#[must_use]
pub(crate) fn unroutable_public_resource_error(
    operation: AccountOperation,
    scope: ErrorScope,
    kind: ResourceKind,
    id: &str,
    detail: impl Into<String>,
) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::NotFound {
        what: kind,
        id: Some(id.to_owned()),
    }))
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .operation(operation)
    .context(GraphErrorContext::graph(operation).identity())
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
/// shape-checked code rides as a typed `WireCause::Graph` for the support export.
#[must_use]
pub(crate) fn id_translation_refused(
    operation: AccountOperation,
    scope: ErrorScope,
    code: Option<&str>,
    _message: Option<&str>,
) -> AccountError {
    let code = code.and_then(bifrost_net::account_error::correlation_id);
    let detail = DiagnosticText::support_only("translateExchangeIds refused an id");
    let mut builder = AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::Malformed {
        detail: detail.clone(),
    }))
    .operation(operation)
    .context(GraphErrorContext::graph(operation).identity())
    .scope(scope)
    .text(detail);
    if let Some(code) = code.as_deref() {
        builder = builder
            .native_code(code.to_string())
            .push_cause(Cause::Wire(WireCause::Graph(crate::error::classify_code(
                code,
            ))));
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
/// engine can rebuild the account's durable cursor state, and so does
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
    // Scoped state causes own their recovery target. Finish also retains
    // operation, provider and diagnostic scope on the other decode failures.
    // Keep decode reasons distinct even when they share a recovery policy.
    let decode_detail = error.to_string();
    let decode_builder = |fault: bifrost_types::CursorDecodeFault| {
        fault
            .builder(ctx.protocol, ctx.operation)
            .context(ctx.identity())
            .text(DiagnosticText::support_only(decode_detail))
    };
    let builder = match error {
        CursorError::ProtocolMismatch => {
            decode_builder(bifrost_types::CursorDecodeFault::ProtocolMismatch)
        }
        CursorError::EnvelopeUnknown => {
            decode_builder(bifrost_types::CursorDecodeFault::FutureVersion)
        }
        CursorError::SchemaIncompatible => {
            decode_builder(bifrost_types::CursorDecodeFault::RetiredVersion)
        }
        CursorError::Decode(_) => decode_builder(bifrost_types::CursorDecodeFault::Malformed),
        CursorError::ScopeMismatch => {
            decode_builder(bifrost_types::CursorDecodeFault::ScopeMismatch {
                scope: ctx
                    .scope
                    .as_ref()
                    .and_then(|s| {
                        if let ErrorScope::Cursor(scope) = s {
                            Some(scope.clone())
                        } else {
                            None
                        }
                    })
                    .expect("cursor error context"),
            })
        }
        CursorError::CalendarWindowExpired | CursorError::InventoryInProgress => base_builder(
            &ctx,
            PrimaryCause::State(StateCause::CursorInvalid {
                scope: ctx
                    .scope
                    .as_ref()
                    .and_then(|s| {
                        if let ErrorScope::Cursor(scope) = s {
                            Some(scope.clone())
                        } else {
                            None
                        }
                    })
                    .expect("cursor error context"),
            }),
        )
        .text(DiagnosticText::support_only(error.to_string())),
        CursorError::Unsupported => base_builder(
            &ctx,
            PrimaryCause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
        ),
        CursorError::Configuration(msg) => base_builder(
            &ctx,
            PrimaryCause::State(StateCause::ScopeRevoked {
                scope: ctx
                    .scope
                    .as_ref()
                    .and_then(|s| {
                        if let ErrorScope::Cursor(scope) = s {
                            Some(scope.clone())
                        } else {
                            None
                        }
                    })
                    .expect("cursor error context"),
            }),
        )
        .text(DiagnosticText::support_only(msg)),
        // A cursor this crate built would not serialize: the client's own
        // invariant, never the provider's fault.
        CursorError::Encode(msg) => {
            let detail = DiagnosticText::support_only(msg);
            base_builder(
                &ctx,
                PrimaryCause::Internal(InternalCause::new(
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
    let mut builder = AccountErrorBuilder::new(PrimaryCause::Protocol {
        kind: inner,
        wire: WireCause::MalformedResponse {
            protocol: Protocol::Graph,
            detail: Some(detail.clone()),
        },
    })
    .operation(operation)
    .context(GraphErrorContext::graph(operation).identity())
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
///
/// `subrequest_replayable` (computed by `subrequest_replayable()` before the
/// batch is sent) narrows that default: a subrequest that may not be replayed,
/// such as a PATCH carrying `If-Match`, is stamped non-idempotent so its
/// omission reconciles instead of retrying into a 412 against its own write.
#[must_use]
pub(crate) fn batch_response_missing(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    detail: impl Into<String>,
    subrequest_replayable: bool,
) -> AccountError {
    let detail = DiagnosticText::support_only(detail.into());
    let mut builder = AccountErrorBuilder::new(PrimaryCause::Protocol {
        kind: ProtocolErrorKind::PartialResponse,
        wire: WireCause::MalformedResponse {
            protocol: Protocol::Graph,
            detail: Some(detail.clone()),
        },
    })
    .operation(operation)
    .context(GraphErrorContext::graph(operation).identity())
    .text(detail);
    if !subrequest_replayable {
        builder = builder.idempotency_override(false);
    }
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

// Replay declarations and header decoding belong to account::batch. This
// classifier receives the resolved leg verdict for missing-answer recovery.

/// Invalid numeric statuses are malformed provider answers, never synthetic 500s.
pub(crate) fn batch_status_error(
    status: u16,
    headers: HeaderMap,
    body: bytes::Bytes,
    ctx: &GraphErrorContext,
) -> AccountError {
    match StatusCode::from_u16(status) {
        Ok(status) => response_to_account_error(
            GraphResponseError::from_response(status, headers, body),
            ctx,
        ),
        Err(_) => provider_answer_violation(
            ProtocolErrorKind::ContractViolation,
            format!("Graph batch response has invalid HTTP status {status}"),
            ctx,
        ),
    }
}

/// Translate a single `$batch` response item into an
/// `ItemOutcome<MutationSuccess>`. Centralizes the mutation outcome
/// rules from the plan: 2xx -> applied, destroy-404 -> skipped, 404 -> not
/// found, 409 / 412 -> concurrency conflict (never `Skipped`), 429 ->
/// rate limited with retry-after + throttle scope, 5xx -> unavailable.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub(crate) fn mutation_item_outcome<K>(
    status: u16,
    headers: HeaderMap,
    body: bytes::Bytes,
    absent: crate::account::batch::AbsentMeans,
    item_id: K,
    operation: AccountOperation,
    scope: ErrorScope,
    // The mailbox budget the item's sub-request was charged to, stamped on
    // a throttled outcome. The outer `$batch` cannot carry it (it mixes
    // mailboxes); each item can.
    budget: Option<bifrost_types::ThrottleBudget>,
) -> bifrost_types::ItemOutcome<bifrost_types::MutationSuccess, K> {
    use bifrost_types::{BatchFailure, BatchSuccess, ItemOutcome, MutationSuccess};

    if (200..300).contains(&status) {
        return ItemOutcome::Succeeded(BatchSuccess::new(item_id, MutationSuccess::Applied));
    }
    if status == 404 && matches!(absent, crate::account::batch::AbsentMeans::Success) {
        return ItemOutcome::Succeeded(BatchSuccess::new(item_id, MutationSuccess::Skipped));
    }
    let mut ctx = GraphErrorContext::graph(operation).with_scope(scope);
    ctx.budget = budget;
    let error = batch_status_error(status, headers, body, &ctx);
    ItemOutcome::Failed(BatchFailure::new(item_id, error))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod unsupported_constructor_tests;

#[cfg(test)]
mod replay_tests;

#[cfg(test)]
mod export_policy_tests {
    use super::*;

    #[test]
    fn rest_codes_are_shape_checked_without_changing_status_classification() {
        for code in [
            "Bearer private-value",
            "secret",
            "line\nbreak",
            "non-ascii-\u{e9}",
            &"x".repeat(257),
        ] {
            let body = serde_json::to_vec(&serde_json::json!({
                "error": { "code": code, "innerError": { "code": code } }
            }))
            .expect("JSON fixture");
            let error = response_to_account_error(
                GraphResponseError::from_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    HeaderMap::new(),
                    body.into(),
                ),
                &GraphErrorContext::graph(AccountOperation::SyncChanges),
            );
            assert!(error.recovery().is_retryable());
            let exported =
                serde_json::to_string(&error.support_internal()).expect("support export");
            assert!(
                !exported.contains(code),
                "rejected code reached internal export"
            );
            assert!(error.chain().iter().any(|cause| matches!(
                cause.wire(), Some(WireCause::Graph(GraphSignal::Unknown { code })) if code.is_empty()
            )));
        }
        let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
            .with_scope(ErrorScope::Cursor(CursorScope::Account));
        for (code, restart) in [("InvalidDeltaToken", true), ("NewProviderCode", false)] {
            let body = serde_json::to_vec(&serde_json::json!({
                "error": { "code": code, "innerError": { "code": "InnerDetail123" } }
            }))
            .expect("JSON fixture");
            let error = response_to_account_error(
                GraphResponseError::from_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    HeaderMap::new(),
                    body.into(),
                ),
                &ctx,
            );
            assert_eq!(error.recovery().requires_engine_action(), restart);
            let exported =
                serde_json::to_string(&error.support_internal()).expect("support export");
            assert!(exported.contains(code));
            assert!(exported.contains("InnerDetail123"));
        }
    }

    #[test]
    fn directly_constructed_response_cannot_export_unchecked_codes() {
        let response = GraphResponseError {
            status: StatusCode::BAD_REQUEST,
            headers: HeaderMap::new(),
            body: bytes::Bytes::new(),
            signal: Some(GraphSignal::Unknown {
                code: "Bearer private-value".into(),
            }),
            message: None,
            inner: Some(GraphInnerError {
                code: Some("secret".into()),
                ..Default::default()
            }),
        };
        let error = response_to_account_error(
            response,
            &GraphErrorContext::graph(AccountOperation::SyncChanges),
        );
        let exported = serde_json::to_string(&error.support_internal()).expect("support export");
        assert!(!exported.contains("Bearer private-value"));
        assert!(!exported.contains("secret"));
    }
}
