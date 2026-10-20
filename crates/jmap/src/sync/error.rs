//! JMAP -> AccountError conversion boundary.
//!
//! Every `crate::Error` exits the JMAP crate through
//! [`into_account_error`]. Transport-level failures from the default
//! reqwest transport delegate to `bifrost_net::into_account_error`;
//! parsed JMAP method errors, problem-details responses, set-error
//! responses, and local crate errors all funnel through
//! `AccountErrorBuilder` here with operation, scope, and
//! provider/protocol context attached.
//!
//! No `RecoveryClass` or `Fatal` is constructed in this crate. The
//! central recovery mapping in `bifrost_types::error::recovery` derives
//! recovery from `(kind, scope, operation, primary_cause)` at builder
//! `build()` time.
//!
//! The crate root sets `#![allow(dead_code)]`, which is load-bearing for the
//! per-RFC method modules (full `get`/`set`/`query` surfaces that consumers
//! call and the crate itself does not). It is the wrong default HERE: an
//! unreachable arm of this boundary is a silent hole in the contract above,
//! not unused API. This module opts back in so the compiler reports one.
#![warn(dead_code)]

#[cfg(test)]
use std::time::Duration;

use bifrost_net::{NetErrorContext, into_account_error as net_into_account_error};
use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AttemptCause, AuthCause, AuthErrorKind, BatchFailure, BatchItemId,
    BatchSuccess, Cause, CursorScope, DiagnosticText, ErrorScope, InternalCause, InternalErrorKind,
    ItemOutcome, JmapMethod, MutationSuccess, Protocol, ProtocolErrorKind, Provider, RequestCause,
    RequestErrorKind, ResourceKind, RetryHint, ServerCause, ServerErrorKind, StateCause, SyncEvent,
    SyncStateErrorKind, ThrottleScope, TransmissionState, TransportCause, TransportErrorKind,
    TransportKind, WireCause,
};

use crate::core::error::{JMAPError, MethodErrorType, ProblemDetails, ProblemType};
use crate::core::set::{SetError, SetErrorType};

/// Context attached at the JMAP conversion boundary. Mirrors
/// `bifrost_net::NetErrorContext` but with `protocol` fixed to
/// `Protocol::Jmap` and operation non-optional so call sites cannot
/// silently classify a non-idempotent in-flight failure with the
/// idempotent-true default of the central recovery mapper.
#[derive(Clone, Debug)]
pub(crate) struct JmapErrorContext {
    pub(crate) provider: Option<Provider>,
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
}

impl JmapErrorContext {
    #[must_use]
    pub(crate) fn new(operation: AccountOperation) -> Self {
        Self {
            provider: None,
            operation,
            scope: None,
        }
    }

    #[must_use]
    pub(crate) fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = Some(scope);
        self
    }

    #[must_use]
    pub(crate) fn cursor(operation: AccountOperation, scope: CursorScope) -> Self {
        Self::new(operation).with_scope(ErrorScope::Cursor(scope))
    }

    #[must_use]
    pub(crate) fn message(operation: AccountOperation, id: impl Into<String>) -> Self {
        Self::new(operation).with_scope(ErrorScope::Message {
            id: (id.into()).into(),
        })
    }

    /// Unused convenience wrappers: both scopes ARE produced (see
    /// `pim.rs` / `factory.rs`), but producers reach for `with_scope`
    /// directly. Kept because they are the readable spelling, and harmless
    /// because the scope readers below (`resource_from_scope`,
    /// `id_from_scope`) accept both shapes - the failure mode to avoid is a
    /// reader that handles only what a dead constructor produces.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn mailbox(operation: AccountOperation, id: impl Into<String>) -> Self {
        Self::new(operation).with_scope(ErrorScope::Mailbox {
            id: (id.into()).into(),
        })
    }

    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn thread(operation: AccountOperation, id: impl Into<String>) -> Self {
        Self::new(operation).with_scope(ErrorScope::Thread {
            id: (id.into()).into(),
        })
    }
}

/// Convert a JMAP crate error into an opaque `AccountError`. This is
/// the only conversion path from `crate::Error` to the consumer surface.
#[must_use]
pub(crate) fn into_account_error(error: crate::Error, ctx: JmapErrorContext) -> AccountError {
    match error {
        crate::Error::Transport(transport) => convert_transport(transport, ctx),
        crate::Error::Problem { details, transport } => convert_problem(*details, transport, ctx),
        crate::Error::Method(method) => convert_method(method, ctx),
        crate::Error::Set(set) => set_error_to_account_error(set, ctx, None),
        crate::Error::RequestEncode(err) => build(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(err.to_string()),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::RequestCallLimit { max } => build(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!(
                    "JMAP request exceeds maxCallsInRequest ({max})"
                )),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::RequestSizeLimit { max, size } => build(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!(
                    "JMAP request of {size} bytes exceeds maxSizeRequest ({max})"
                )),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::ResponseDecode(err) => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(err.to_string())),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::CallNotFound(id) => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(format!(
                    "call {id} not found in response"
                ))),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::UnexpectedMethodResponse {
            call_id,
            expected,
            actual,
        } => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(format!(
                    "call {call_id} returned {actual}, expected {expected}"
                ))),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::IdNotFound(id) => convert_id_not_found(id, ctx),
        crate::Error::NotParsable(detail) => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(detail)),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::InvalidUrl(detail) => build(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::InvalidArgument {
                field: Some("url"),
                message: Some(DiagnosticText::support_only(detail)),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        // The server's own session object published a URL that cannot be
        // used. The same lane as `MalformedCapability` below: the provider
        // described itself wrongly, and a reopen re-reads the same session,
        // so it is a terminal contract violation naming the property - not
        // the caller's `Request(Malformed)` it was once filed under.
        crate::Error::MalformedSessionUrl { property, detail } => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(format!(
                    "session property {property} is not a usable URL: {detail}"
                ))),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        // The server advertised the capability and then sent an object
        // that is not one. Nothing about this session changes on a reopen,
        // so it takes the contract-violation lane rather than the
        // `CapabilityChanged -> RestartAccount` reopen loop an ABSENT
        // capability takes, and the URI rides in the diagnostic.
        crate::Error::MalformedCapability { capability } => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(format!(
                    "JMAP capability object {capability} is present but not parseable"
                ))),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        // The session names no primary account for the capability the
        // request needs, so nothing was sent: a local refusal because the
        // server offers no account for that capability, which is
        // `Unsupported(operation)` - terminal, like the `AuthLost` this
        // used to borrow, but without telling the operator to
        // re-authorize, which cannot add an account to the session. Not
        // `CapabilityChanged -> RestartAccount` either: that reopen loop
        // would re-run discovery against the same session forever.
        crate::Error::NoPrimaryAccount { capability } => build(
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
            &ctx,
        )
        .text(DiagnosticText::support_only(format!(
            "session lists no primary account for capability {capability}"
        )))
        .try_build()
        .expect("valid account error classification"),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketHandshake(err) => websocket_handshake_error(err.to_string(), ctx),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketRuntime(err) => websocket_runtime_error(err.to_string(), ctx),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketSend(err) => websocket_send_error(&err, ctx),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketClosed => {
            websocket_runtime_error("WebSocket connection closed by peer".to_string(), ctx)
        }
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketNotConnected => build(
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketSetup(setup) => match setup {
            crate::WebSocketSetupError::Tls(message) => build(
                AccountErrorKind::Transport(TransportErrorKind::Tls),
                Cause::Transport(TransportCause::new(
                    TransportKind::Tls,
                    Some(DiagnosticText::support_only(message)),
                )),
                &ctx,
            )
            .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
            .try_build()
            .expect("valid account error classification"),
            crate::WebSocketSetupError::InvalidHeader(message) => build(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::InvalidArgument {
                    field: Some("authorization"),
                    message: Some(DiagnosticText::support_only(message)),
                }),
                &ctx,
            )
            .try_build()
            .expect("valid account error classification"),
            crate::WebSocketSetupError::Subprotocol(message) => build(
                AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
                Cause::State(StateCause::CapabilityChanged { delta: None }),
                &ctx,
            )
            .text(DiagnosticText::support_only(message))
            .try_build()
            .expect("valid account error classification"),
        },
    }
}

/// True if `err` is a JMAP `stateMismatch` method-level error. Used by
/// the mutation pipeline to gate an `Email/set` retry after refreshing
/// the cached state. The predicate inspects only `MethodErrorType` -
/// it does not construct recovery advice.
#[must_use]
pub(crate) fn is_state_mismatch(err: &crate::Error) -> bool {
    matches!(
        err,
        crate::Error::Method(method)
            if matches!(method.error_type(), MethodErrorType::StateMismatch)
    )
}

/// Emit `SyncEvent::Terminated(AccountError)` for stream termination.
#[must_use]
pub(crate) fn terminated<T>(error: AccountError) -> SyncEvent<T> {
    SyncEvent::Terminated(error)
}

#[must_use]
pub(crate) fn terminated_from_jmap<T>(err: crate::Error, ctx: JmapErrorContext) -> SyncEvent<T> {
    terminated(into_account_error(err, ctx))
}

/// `Unsupported` for an operation that JMAP does not provide. Lets
/// stream call sites emit a Terminated event without constructing
/// `RecoveryClass` directly.
#[must_use]
pub(crate) fn unsupported_error(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    message: impl Into<String>,
) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .text(DiagnosticText::support_only(message));
    if let Some(scope) = scope {
        builder = builder.scope(scope);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// A `SearchFilter::In` naming a container in a foreign (shared) account
/// this session does not hold a handle for.
///
/// `Email/query` addresses exactly one `accountId`, so an unreachable owner
/// leaves no honest wire form: sending the qualified id to the primary
/// account matches nothing (the empty result the consumer used to get with
/// no error), and stripping the qualifier would run the search against the
/// PRIMARY account's same-id mailbox. Reject before the wire. This is the
/// one place foreign routing cannot use hydration's stay-literal fallback,
/// because the id is a filter operand rather than the object identity the
/// server can report a miss on.
#[must_use]
pub(crate) fn search_unknown_account(
    operation: AccountOperation,
    container_id: &str,
    account_id: &str,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("filter.in"),
            message: Some(DiagnosticText::support_only(format!(
                "container {container_id} names shared account {account_id}, \
                 which this session cannot reach"
            ))),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// A search filter whose `In` containers name two different owning
/// accounts.
///
/// One `Email/query` carries one `accountId`, so this is not expressible;
/// running it as a cross-account union is a deliberate non-goal (no other
/// provider in the workspace offers one, so the shared trait cannot promise
/// it). Silently picking one owner would answer a different question than
/// the one asked.
#[must_use]
pub(crate) fn search_cross_account_filter(
    operation: AccountOperation,
    first_owner: Option<&str>,
    second_owner: Option<&str>,
) -> AccountError {
    let first = super::foreign::owner_label(first_owner);
    let second = super::foreign::owner_label(second_owner);
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("filter.in"),
            message: Some(DiagnosticText::support_only(format!(
                "search filter names containers in accounts {first} and {second}; \
                 one Email/query cannot span two accounts"
            ))),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// A search page cursor minted against one account, replayed with a filter
/// that routes to another.
///
/// The cursor is a bare position into one account's `Email/query` result
/// order, so honouring it across accounts would page account B by account
/// A's offsets: duplicates and skips with no signal.
#[must_use]
pub(crate) fn search_cursor_account_mismatch(
    operation: AccountOperation,
    cursor_owner: Option<&str>,
    filter_owner: Option<&str>,
) -> AccountError {
    let cursor_owner = super::foreign::owner_label(cursor_owner);
    let filter_owner = super::foreign::owner_label(filter_owner);
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("page_cursor"),
            message: Some(DiagnosticText::support_only(format!(
                "search page cursor belongs to account {cursor_owner}, \
                 but this filter routes to account {filter_owner}"
            ))),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// The `Email/query` result set moved between the page that minted the
/// search cursor and the page that presented it (`queryState` changed).
///
/// The cursor is a position into a server-recomputed order, so continuing
/// against a moved order silently duplicates and skips results.
///
/// This is a `ConcurrencyConflict` -> `Retry(AfterStateRefresh)`, which is
/// precisely the shape of the event: the caller's state (the page cursor)
/// is stale, refreshing it means running the search again from the first
/// page, and the retry then succeeds. The two neighbouring lanes are both
/// wrong. `SyncState(CursorInvalid)` is reserved for an ENGINE cursor
/// scope (the builder refuses it without an `ErrorScope::Cursor`,
/// `CursorInvalidWithoutScope`), and a search page cursor never enters
/// the cursor envelope, so there is no scope to restart. `Request(Malformed)`
/// derives `ClientBug`, and a single delivered message advancing
/// `queryState` is not the caller's bug and must not be reported as
/// permanently unfixable.
#[must_use]
pub(crate) fn search_result_set_superseded(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::ConcurrencyConflict,
        Cause::State(bifrost_types::StateCause::ConcurrencyConflict),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .text(DiagnosticText::support_only(
        "search result set changed between pages \
         (Email/query queryState moved); repeat the search",
    ))
    .try_build()
    .expect("valid account error classification")
}

/// A send-as mailbox id that was not present in the successfully seeded
/// foreign-account routing table. Consumers receive these ids from foreign
/// membership ownership, so an unknown id is a malformed request.
#[must_use]
pub(crate) fn send_as_unknown_account(mailbox: &bifrost_types::MailboxId) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("send_as.mailbox"),
            message: Some(DiagnosticText::support_only(format!(
                "unknown foreign submission account {}",
                mailbox.0
            ))),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(AccountOperation::Send)
    .try_build()
    .expect("valid account error classification")
}

/// A push subscribe whose every requested scope maps to no JMAP push data
/// type.
///
/// Classified `Request(Malformed)` rather than `Unsupported(PushSubscribe)`
/// for the same reason as `cross_account_destination`: the account has
/// already advertised `PushCapability::InProcess`, so `Unsupported` claims
/// the account has no push at all and invites a consumer keying off the
/// kind to downgrade push wholesale. What is actually wrong is the
/// argument - these scopes, which the mixed case already reports
/// per-scope on the accepted lane.
#[must_use]
pub(crate) fn no_mappable_push_scopes() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("push_subscribe.scopes"),
            message: Some(DiagnosticText::support_only(
                "no requested cursor scope maps to a JMAP push data type",
            )),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(AccountOperation::PushSubscribe)
    .try_build()
    .expect("valid account error classification")
}

/// A mutation whose message and whose destination mailbox belong to
/// different JMAP accounts.
///
/// `Email/set` addresses exactly one `accountId` and JMAP ids are
/// account-scoped, so no single request can express "move this shared-account
/// message into my primary Inbox". Sending it anyway is worse than an error:
/// the destination id is interpreted in the ROUTED account's namespace, so if
/// that account holds a mailbox with the same id the move succeeds against
/// the wrong container and the caller is never told. Reject before the wire.
///
/// Classified `Request(Malformed)` (a caller-side request-shape fault routing
/// to `ClientBug`) rather than `Unsupported`, which would wrongly imply the
/// protocol has no move at all. Mirrors bifrost-graph's cross-mailbox
/// `bulk_move` rejection.
#[must_use]
pub(crate) fn cross_account_destination(
    operation: AccountOperation,
    target_id: &str,
    target_owner: Option<&str>,
    destination_id: &str,
    destination_owner: Option<&str>,
) -> AccountError {
    let target_owner = super::foreign::owner_label(target_owner);
    let destination_owner = super::foreign::owner_label(destination_owner);
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("destination"),
            message: Some(DiagnosticText::support_only(format!(
                "mailbox {destination_id} belongs to account {destination_owner}, \
                 but object {target_id} belongs to account {target_owner}; \
                 one Email/set cannot span two accounts"
            ))),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .scope(ErrorScope::Message {
        id: (target_id.to_string()).into(),
    })
    .try_build()
    .expect("valid account error classification")
}

/// Convenience for stream call sites that need to emit a
/// `SyncEvent::Terminated(Unsupported(op))`. Callers pass the
/// operation the stream is performing; the helper previously
/// hard-coded `AccountOperation::Discover` for every caller,
/// which mis-classified inventory / hydrate / blob-range streams
/// and produced consumer-visible "discovery unsupported" copy for
/// operations that have nothing to do with discovery.
#[must_use]
pub(crate) fn terminated_unsupported<T>(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    message: impl Into<String>,
) -> SyncEvent<T> {
    terminated(unsupported_error(operation, scope, message))
}

/// Stream-side variant for protocol-shape failures that are
/// `Protocol(ContractViolation)`, not `Unsupported`. Pagination
/// overflows, response-shape mismatches, and similar producer-side
/// limits that the protocol cannot represent in a request live here.
/// `Discover`-coded `Unsupported` was the wrong consumer-facing
/// classification - the server is not refusing the operation; the
/// library cannot encode the request shape.
///
/// The crate's one correct terminator for a response the library cannot
/// encode: the change walks' forward-progress guards reach for it, and the
/// next stream that meets such a response should too rather than re-deriving
/// the classification - which is exactly how `Discover`-coded `Unsupported`
/// got onto inventory in the first place.
#[must_use]
pub(crate) fn terminated_contract_violation<T>(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    message: impl Into<String>,
) -> SyncEvent<T> {
    terminated(contract_violation(operation, scope, message))
}

/// The bare `AccountError` behind `terminated_contract_violation`, for
/// the streams that carry their own terminal envelope rather than
/// `SyncEvent::Terminated` - `ScopeLifecycleEvent::Terminated` is the
/// current one. The classification must not fork per envelope.
#[must_use]
pub(crate) fn contract_violation(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    message: impl Into<String>,
) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(operation);
    if let Some(scope) = scope {
        builder = builder.scope(scope);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// Per-item classification for `Email/set` outcomes. Returns
/// `Succeeded(Skipped)` when the set error is an absorbed notFound
/// (the per-item NotFound absorption policy for bulk mutations); all
/// other set errors map to a `Failed` lane with a full `AccountError`.
#[must_use]
pub(crate) fn classify_set_item(
    set_error: SetError<String>,
    ctx: JmapErrorContext,
    item: BatchItemId,
    item_scope: Option<ErrorScope>,
) -> ItemOutcome<MutationSuccess> {
    let absorb = matches!(
        set_error.error_type(),
        &SetErrorType::NotFound | &SetErrorType::BlobNotFound
    ) && matches!(
        ctx.operation,
        AccountOperation::UpdateFlags
            | AccountOperation::BulkMove
            | AccountOperation::BulkDestroy
            | AccountOperation::SetKeyword
            | AccountOperation::SetIsRead
            | AccountOperation::AddToContainer
            | AccountOperation::RemoveFromContainer
    );
    if absorb {
        ItemOutcome::Succeeded(BatchSuccess::new(item, MutationSuccess::Skipped))
    } else {
        let error = set_error_to_account_error(set_error, ctx, item_scope);
        ItemOutcome::Failed(BatchFailure::new(item, error))
    }
}

/// Convert a JMAP `SetError` to an `AccountError`. When `item_scope`
/// is present, the resource-bearing classifications (NotFound,
/// PermissionDenied) derive their resource from it.
#[must_use]
pub(crate) fn set_error_to_account_error(
    set_error: SetError<String>,
    ctx: JmapErrorContext,
    item_scope: Option<ErrorScope>,
) -> AccountError {
    let scope_for_resource = item_scope.as_ref().or(ctx.scope.as_ref());
    let error_type = set_error.error_type().clone();
    let (kind, primary) = match error_type {
        SetErrorType::Forbidden
        | SetErrorType::ForbiddenFrom
        | SetErrorType::ForbiddenMailFrom
        | SetErrorType::ForbiddenToSend => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(scope_for_resource),
            }),
        ),
        SetErrorType::OverQuota => (
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
        ),
        SetErrorType::RateLimit => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
        ),
        SetErrorType::NotFound | SetErrorType::BlobNotFound => {
            let wire = if matches!(set_error.error_type(), &SetErrorType::BlobNotFound) {
                JmapMethod::BlobNotFound
            } else {
                JmapMethod::NotFound
            };
            if let Some(resource) = resource_from_scope(scope_for_resource) {
                let id = id_from_scope(scope_for_resource);
                (
                    AccountErrorKind::NotFound(resource),
                    Cause::Request(RequestCause::NotFound { what: resource, id }),
                )
            } else {
                // No scope tells us what was missing. Classify as a
                // typed protocol-shape error so the kind/cause pair
                // matches the strict builder invariants. Without the
                // resource scope the previous shape was
                // `Server(Error { status: None }) + Cause::Wire(_)`,
                // which doesn't pass `kind_matches_cause`.
                (
                    AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
                    Cause::Wire(WireCause::Jmap(wire)),
                )
            }
        }
        SetErrorType::AlreadyExists => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        SetErrorType::TooLarge
        | SetErrorType::TooManyKeywords
        | SetErrorType::TooManyMailboxes
        | SetErrorType::TooManyRecipients
        | SetErrorType::InvalidPatch
        | SetErrorType::InvalidProperties
        | SetErrorType::InvalidEmail
        | SetErrorType::InvalidRecipients
        | SetErrorType::NoRecipients
        | SetErrorType::InvalidScript => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(set_error.to_string()),
            }),
        ),
        // The following set-error variants are legitimate JMAP wire
        // responses describing protocol-shape state conflicts (id is
        // on the destroy list, singleton already exists, mailbox has
        // children, etc.). They classify under `Protocol(Unknown)`
        // paired with a typed `Cause::Wire(JmapMethod::*)` so the
        // strict `kind_matches_cause` rule holds. Recovery for
        // `Protocol(Unknown)` is `UnknownPermanent`, which is the
        // right consumer-facing outcome: the operation cannot retry
        // and the engine has no automatic remediation.
        SetErrorType::WillDestroy => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::WillDestroy)),
        ),
        SetErrorType::Singleton => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::Singleton)),
        ),
        SetErrorType::ScriptIsActive => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::ScriptIsActive)),
        ),
        SetErrorType::CannotUnsend => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::CannotUnsend)),
        ),
        SetErrorType::MailboxHasChild => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::MailboxHasChild)),
        ),
        SetErrorType::MailboxHasEmail => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::MailboxHasEmail)),
        ),
        SetErrorType::Other(code) => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::Unknown { code })),
        ),
    };

    let mut builder = build_with(&ctx, kind, primary)
        // Set errors are returned in a successful JMAP response, which
        // already crossed the side-effect boundary.
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::Acknowledged,
        )));
    // Push the typed wire cause as a forensic layer for all non-Other
    // variants whose primary cause is not already a WireCause. For
    // WillDestroy/Singleton/etc. the primary IS the WireCause so we
    // skip to avoid duplication; for Forbidden/OverQuota/RateLimit/etc.
    // the primary is an Access/Server cause and we add the wire signal.
    let primary_was_wire = matches!(
        set_error.error_type(),
        SetErrorType::WillDestroy
            | SetErrorType::Singleton
            | SetErrorType::ScriptIsActive
            | SetErrorType::CannotUnsend
            | SetErrorType::MailboxHasChild
            | SetErrorType::MailboxHasEmail
            | SetErrorType::Other(_)
    );
    if !primary_was_wire
        && let Some(wire) = set_error_type_to_jmap_method(set_error.error_type().clone())
    {
        builder = builder.push_cause(Cause::Wire(WireCause::Jmap(wire)));
    }
    if let Some(description) = set_error.description() {
        builder = builder.text(DiagnosticText::support_only(description));
    }
    if matches!(set_error.error_type(), &SetErrorType::RateLimit) {
        builder = builder.throttle_scope(ThrottleScope::Account);
    }
    if let Some(scope) = item_scope {
        builder = builder.scope(scope);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// Map a `SetErrorType` to the corresponding typed `JmapMethod` wire
/// cause variant. Returns `None` only for `SetErrorType::Other` (no
/// stable wire code) and the resource-classified cases handled above
/// whose primary cause is already a `WireCause` (`WillDestroy`,
/// `Singleton`, etc.). Callers that already check `primary_was_wire`
/// will not call this for those variants.
fn set_error_type_to_jmap_method(error_type: SetErrorType) -> Option<JmapMethod> {
    match error_type {
        SetErrorType::Forbidden
        | SetErrorType::ForbiddenFrom
        | SetErrorType::ForbiddenMailFrom
        | SetErrorType::ForbiddenToSend => Some(JmapMethod::Forbidden),
        SetErrorType::OverQuota => Some(JmapMethod::OverQuota),
        SetErrorType::RateLimit => Some(JmapMethod::RateLimit),
        SetErrorType::NotFound => Some(JmapMethod::NotFound),
        SetErrorType::BlobNotFound => Some(JmapMethod::BlobNotFound),
        SetErrorType::AlreadyExists => Some(JmapMethod::AlreadyExists),
        SetErrorType::TooLarge => Some(JmapMethod::TooLarge),
        SetErrorType::TooManyKeywords => Some(JmapMethod::TooManyKeywords),
        SetErrorType::TooManyMailboxes => Some(JmapMethod::TooManyMailboxes),
        SetErrorType::TooManyRecipients => Some(JmapMethod::TooManyRecipients),
        SetErrorType::InvalidPatch => Some(JmapMethod::InvalidPatch),
        SetErrorType::InvalidProperties => Some(JmapMethod::InvalidProperties),
        SetErrorType::InvalidEmail => Some(JmapMethod::InvalidEmail),
        SetErrorType::InvalidRecipients => Some(JmapMethod::InvalidRecipients),
        SetErrorType::InvalidScript => Some(JmapMethod::InvalidScript),
        SetErrorType::NoRecipients => Some(JmapMethod::NoRecipients),
        // WillDestroy/Singleton/ScriptIsActive/CannotUnsend/MailboxHasChild/
        // MailboxHasEmail primary IS the WireCause; callers skip these.
        // Other(code) carries the unknown wire code already as the
        // primary `Cause::Wire(JmapMethod::Unknown { code })`; no
        // forensic duplicate is added.
        SetErrorType::WillDestroy
        | SetErrorType::Singleton
        | SetErrorType::ScriptIsActive
        | SetErrorType::CannotUnsend
        | SetErrorType::MailboxHasChild
        | SetErrorType::MailboxHasEmail
        | SetErrorType::Other(_) => None,
    }
}

/// Build a `SyncState(ScopeRevoked)` error scoped to one foreign
/// (shared/delegate) account mailbox's `Folder` scope. With the scope
/// attached, the central `derive` resolves it to
/// `Engine(DisableScope(scope))` - the engine quarantines just that
/// cursor scope without escalating account-wide. The owning account
/// rides as support-only diagnostic text for telemetry.
#[must_use]
pub(crate) fn jmap_scope_revoked(
    scope: CursorScope,
    owner: &bifrost_types::MailboxId,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
        Cause::State(StateCause::ScopeRevoked),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .scope(ErrorScope::Cursor(scope))
    .text(DiagnosticText::support_only(format!(
        "shared account access revoked (owner {})",
        owner.0,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// A `Folder` scope that parses as a foreign (shared/delegate) mailbox
/// but whose account is no longer registered (the share/delegation
/// disappeared from the session since the cursor was seeded). The scope
/// can no longer route to a real account handle, and falling back to the
/// primary account would conflate the foreign mailbox id with a primary
/// one. Classify as `SyncState(ScopeRevoked)` scoped to the cursor so the
/// engine quarantines just this scope (`Engine(DisableScope)`) rather
/// than misrouting it to the primary mailbox or escalating account-wide.
#[must_use]
pub(crate) fn unregistered_foreign_scope(
    scope: CursorScope,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
        Cause::State(StateCause::ScopeRevoked),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .scope(ErrorScope::Cursor(scope))
    .text(DiagnosticText::support_only(
        "foreign account for this mailbox scope is no longer registered in the session",
    ))
    .try_build()
    .expect("valid account error classification")
}

/// Map a per-scope failure for a foreign (shared/delegate) account
/// mailbox. When the scope belongs to a shared account (`owner.is_some()`)
/// and the failure classifies as permission-denied (the class that would
/// otherwise derive terminal `NoPermission` - JMAP `forbidden`),
/// quarantine just this foreign scope via `ScopeRevoked` instead of
/// escalating account-wide. A primary scope (`owner == None`), or any
/// non-permission failure, flows through the unchanged `into_account_error`
/// path - a primary-account permission loss is a genuine account-level
/// signal.
#[must_use]
pub(crate) fn shared_scope_error(
    err: crate::Error,
    scope: &CursorScope,
    owner: Option<&bifrost_types::MailboxId>,
    ctx: JmapErrorContext,
) -> AccountError {
    if let Some(owner) = owner {
        let account_error = into_account_error(err, ctx.clone());
        if matches!(
            account_error.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ) {
            return jmap_scope_revoked(scope.clone(), owner, ctx.operation);
        }
        return account_error;
    }
    into_account_error(err, ctx)
}

// ---- internal helpers --------------------------------------------------

pub(crate) fn internal_invariant(
    operation: AccountOperation,
    message: impl Into<String>,
) -> AccountError {
    let kind = AccountErrorKind::Internal(InternalErrorKind::InvariantViolated);
    let detail = DiagnosticText::support_only(message.into());
    let context = JmapErrorContext::new(operation);
    build(
        kind,
        Cause::Internal(InternalCause::new(
            InternalErrorKind::InvariantViolated,
            Some(detail.clone()),
        )),
        &context,
    )
    .text(detail)
    .try_build()
    .expect("valid internal invariant error")
}

fn build(kind: AccountErrorKind, primary: Cause, ctx: &JmapErrorContext) -> AccountErrorBuilder {
    build_with(ctx, kind, primary)
}

fn build_with(
    ctx: &JmapErrorContext,
    kind: AccountErrorKind,
    primary: Cause,
) -> AccountErrorBuilder {
    let mut builder = AccountErrorBuilder::new(kind, primary)
        .protocol(Protocol::Jmap)
        .operation(ctx.operation);
    if let Some(provider) = ctx.provider {
        builder = builder.provider(provider);
    }
    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }
    builder
}

fn resource_from_scope(scope: Option<&ErrorScope>) -> Option<ResourceKind> {
    match scope? {
        ErrorScope::Message { .. } => Some(ResourceKind::Message),
        ErrorScope::Mailbox { .. } => Some(ResourceKind::Mailbox),
        ErrorScope::Thread { .. } => Some(ResourceKind::Thread),
        ErrorScope::Calendar { .. } | ErrorScope::CalendarCollection => {
            Some(ResourceKind::Calendar)
        }
        ErrorScope::Contact { .. } | ErrorScope::ContactCollection => Some(ResourceKind::Contact),
        ErrorScope::Account | ErrorScope::Cursor(_) => None,
        // `ErrorScope` is `#[non_exhaustive]` in `bifrost-types`, so
        // this catch-all is required for the crate to compile after a
        // new variant lands upstream. Any new variant must be handled
        // explicitly above with the appropriate `ResourceKind` mapping
        // (or an explicit `None`) before relying on this fall-through.
        _ => None,
    }
}

fn id_from_scope(scope: Option<&ErrorScope>) -> Option<String> {
    match scope? {
        ErrorScope::Message { id } => Some(id.0.clone()),
        ErrorScope::Mailbox { id } => Some(id.0.clone()),
        ErrorScope::Thread { id } => Some(id.0.clone()),
        ErrorScope::Calendar { id } => Some(id.0.clone()),
        ErrorScope::Contact { id } => Some(id.0.clone()),
        ErrorScope::Account
        | ErrorScope::Cursor(_)
        | ErrorScope::CalendarCollection
        | ErrorScope::ContactCollection => None,
        // `ErrorScope` is `#[non_exhaustive]`; see `resource_from_scope`
        // for the explicit-handling discipline new variants must follow.
        _ => None,
    }
}

fn convert_transport(
    transport: crate::core::transport::TransportError,
    ctx: JmapErrorContext,
) -> AccountError {
    // A local failure is authoritative: checked before the net evidence and
    // the no-evidence fallback, either of which would call it a network
    // fault and retry it forever.
    if let Some(local) = transport.local {
        let kind = match local {
            crate::core::transport::LocalFailure::InvariantViolated => {
                InternalErrorKind::InvariantViolated
            }
            crate::core::transport::LocalFailure::LimitExceeded => InternalErrorKind::LimitExceeded,
        };
        let detail = DiagnosticText::support_only(transport.message.clone());
        return build(
            AccountErrorKind::Internal(kind),
            Cause::Internal(InternalCause::new(kind, Some(detail.clone()))),
            &ctx,
        )
        .text(detail)
        .try_build()
        .expect("valid account error classification");
    }
    if let Some(net) = transport.net {
        net_into_account_error(
            net,
            NetErrorContext {
                provider: ctx.provider,
                protocol: Protocol::Jmap,
                operation: ctx.operation,
                scope: ctx.scope,
            },
        )
    } else {
        // Custom transport without bifrost-net evidence. No attempt
        // cause: we have no wire-level signal about transmission.
        build(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only(transport.message.clone())),
            )),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification")
    }
}

fn convert_problem(
    details: ProblemDetails,
    transport: Option<crate::core::transport::TransportError>,
    ctx: JmapErrorContext,
) -> AccountError {
    let net_status = transport
        .as_ref()
        .and_then(|t| status_from_net(t.net.as_ref()));
    let status_u32 = details.status().or(net_status);
    let status_u16 = status_u32.and_then(|s| u16::try_from(s).ok());
    let retry_hint = transport
        .as_ref()
        .and_then(|t| retry_after_from_net(t.net.as_ref()));

    let (kind, primary) = match (details.error(), status_u16) {
        (ProblemType::JMAP(JMAPError::Limit), _) => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint }),
        ),
        (ProblemType::JMAP(JMAPError::UnknownCapability), _) => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
        ),
        // The server claims our request was not JSON or not a valid
        // JMAP request envelope; the client side did send valid JSON
        // (any deserialization issue is caught locally before the
        // request leaves), so the server's claim is a protocol contract
        // violation, not a client bug. Per jmap-F2.
        (ProblemType::JMAP(JMAPError::NotJSON | JMAPError::NotRequest), _) => (
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(details.to_string())),
            }),
        ),
        (ProblemType::Other(_), Some(400 | 422)) => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(details.to_string()),
            }),
        ),
        (ProblemType::Other(_), Some(401)) => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
        (ProblemType::Other(_), Some(403)) => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
        ),
        (ProblemType::Other(_), Some(404)) => {
            if let Some(resource) = resource_from_scope(ctx.scope.as_ref()) {
                let id = id_from_scope(ctx.scope.as_ref());
                (
                    AccountErrorKind::NotFound(resource),
                    Cause::Request(RequestCause::NotFound { what: resource, id }),
                )
            } else {
                // Scope-less HTTP 404. The three scope-less not-found
                // shapes in this crate are deliberately distinct by wire
                // source, not unified, because each preserves a different
                // diagnostic signal while all three resolve terminal:
                //   - a bare HTTP 404 (here) is a transport-level
                //     resource refusal -> `Server(Error{404})` ->
                //     `ProviderRefused`;
                //   - a JMAP `notFound` SetError without a resource scope
                //     -> `Protocol(Unknown)` -> `UnknownPermanent`
                //     (`set_error_to_account_error`);
                //   - a local "id absent from response" shape error ->
                //     `Protocol(MissingField)` ->
                //     `ProviderContractViolation` (`convert_id_not_found`).
                // The message keys differ so support exports can tell the
                // three apart; recovery is terminal in every case, so the
                // engine behaves identically.
                (
                    AccountErrorKind::Server(ServerErrorKind::Error { status: Some(404) }),
                    Cause::Server(ServerCause::Error { status: Some(404) }),
                )
            }
        }
        (ProblemType::Other(_), Some(409)) => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        (ProblemType::Other(_), Some(410)) if matches!(ctx.scope, Some(ErrorScope::Cursor(_))) => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        (ProblemType::Other(_), Some(429)) => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint }),
        ),
        (ProblemType::Other(_), Some(status)) if (500..=599).contains(&status) => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint }),
        ),
        (ProblemType::Other(_), Some(status)) => (
            AccountErrorKind::Server(ServerErrorKind::Error {
                status: Some(status),
            }),
            Cause::Server(ServerCause::Error {
                status: Some(status),
            }),
        ),
        (ProblemType::Other(code), None) => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::Unknown { code: code.clone() })),
        ),
    };

    let mut builder = build_with(&ctx, kind, primary)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::Acknowledged,
        )))
        .push_cause(Cause::Wire(wire_for_problem(details.error())));
    if let Some(status) = status_u16 {
        builder = builder.status(Some(status));
    }
    if let Some(request_id) = details.request_id() {
        builder = builder.request_id(request_id.to_string());
    }
    if let Some(title) = details.title() {
        builder = builder.text(DiagnosticText::support_only(title.to_string()));
    }
    if let Some(detail) = details.detail() {
        builder = builder.text(DiagnosticText::support_only(detail.to_string()));
    }
    if let Some(limit) = details.limit() {
        builder = builder.text(DiagnosticText::support_only(format!("limit: {limit}")));
    }
    if matches!(details.error(), ProblemType::JMAP(JMAPError::Limit)) {
        builder = builder.throttle_scope(ThrottleScope::Account);
    }
    if matches!(details.error(), ProblemType::Other(_)) && matches!(status_u16, Some(429)) {
        builder = builder.throttle_scope(ThrottleScope::Account);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

fn wire_for_problem(error: &ProblemType) -> WireCause {
    match error {
        ProblemType::JMAP(JMAPError::UnknownCapability) => {
            WireCause::Jmap(JmapMethod::UnknownCapability)
        }
        ProblemType::JMAP(JMAPError::NotJSON) => WireCause::Jmap(JmapMethod::NotJson),
        ProblemType::JMAP(JMAPError::NotRequest) => WireCause::Jmap(JmapMethod::NotRequest),
        ProblemType::JMAP(JMAPError::Limit) => WireCause::Jmap(JmapMethod::Limit),
        ProblemType::Other(code) => WireCause::Jmap(JmapMethod::Unknown { code: code.clone() }),
    }
}

/// The HTTP status behind a problem-details response. All three shapes
/// are reachable: `Status` for a terminal 4xx, and `RateLimited` /
/// `RetryBudgetExhausted` for a 429 / 5xx whose problem body the
/// transport lifted out of the preserved final response.
fn status_from_net(net: Option<&bifrost_net::Error>) -> Option<u32> {
    match net? {
        bifrost_net::Error::Status { code, .. } => Some(u32::from(code.as_u16())),
        bifrost_net::Error::RateLimited { final_response, .. } => {
            Some(u32::from(final_response.status.as_u16()))
        }
        bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            ..
        } => Some(u32::from(final_response.status.as_u16())),
        _ => None,
    }
}

/// The server's retry hint for a problem-details response.
///
/// Read the final response first: transport durations may be capped or
/// originate from an earlier attempt and cannot preserve an absolute date.
fn retry_after_from_net(net: Option<&bifrost_net::Error>) -> Option<RetryHint> {
    match net?.terminal() {
        bifrost_net::Error::Status { headers, .. } => retry_after_header(headers),
        bifrost_net::Error::RateLimited {
            retry_after,
            final_response,
        } => retry_after_header(&final_response.headers)
            .or_else(|| retry_after.map(RetryHint::After)),
        bifrost_net::Error::RetryBudgetExhausted {
            final_response,
            retry_after_history,
        } => final_response
            .as_ref()
            .and_then(|response| retry_after_header(&response.headers))
            .or_else(|| retry_after_history.last().copied().map(RetryHint::After)),
        _ => None,
    }
}

fn retry_after_header(headers: &reqwest::header::HeaderMap) -> Option<RetryHint> {
    bifrost_net::request::parse_retry_after_hint(headers.get(reqwest::header::RETRY_AFTER))
}

fn convert_method(method: crate::core::error::MethodError, ctx: JmapErrorContext) -> AccountError {
    let method_type = method.error_type();
    // CursorInvalid requires an engine cursor scope. State-based callers
    // without one must handle expiry locally (the lifecycle poller reseeds),
    // rather than interpreting absent client context as provider misconduct.
    let has_cursor_scope = matches!(ctx.scope, Some(ErrorScope::Cursor(_)));
    let (kind, primary, wire) = match method_type {
        MethodErrorType::ServerUnavailable => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            JmapMethod::ServerUnavailable,
        ),
        MethodErrorType::ServerFail => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            JmapMethod::ServerFail,
        ),
        MethodErrorType::ServerPartialFail => (
            AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
            Cause::Wire(WireCause::Jmap(JmapMethod::ServerPartialFail)),
            JmapMethod::ServerPartialFail,
        ),
        MethodErrorType::UnknownMethod => (
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
            JmapMethod::UnknownMethod,
        ),
        MethodErrorType::InvalidArguments => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("invalidArguments".to_string()),
            }),
            JmapMethod::InvalidArguments,
        ),
        MethodErrorType::InvalidResultReference => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("invalidResultReference".to_string()),
            }),
            JmapMethod::InvalidResultReference,
        ),
        MethodErrorType::Forbidden => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
            JmapMethod::Forbidden,
        ),
        MethodErrorType::AccountNotFound => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
            JmapMethod::AccountNotFound,
        ),
        MethodErrorType::FromAccountNotFound => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
            JmapMethod::FromAccountNotFound,
        ),
        MethodErrorType::AccountNotSupportedByMethod => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
            JmapMethod::AccountNotSupportedByMethod,
        ),
        MethodErrorType::FromAccountNotSupportedByMethod => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
            JmapMethod::FromAccountNotSupportedByMethod,
        ),
        MethodErrorType::AccountReadOnly => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
            JmapMethod::AccountReadOnly,
        ),
        MethodErrorType::RequestTooLarge => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("requestTooLarge".to_string()),
            }),
            JmapMethod::RequestTooLarge,
        ),
        MethodErrorType::CannotCalculateChanges if has_cursor_scope => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            JmapMethod::CannotCalculateChanges,
        ),
        MethodErrorType::CannotCalculateChanges => (
            AccountErrorKind::Internal(InternalErrorKind::InvariantViolated),
            Cause::Internal(InternalCause::new(
                InternalErrorKind::InvariantViolated,
                Some(DiagnosticText::support_only(
                    "state expiry reached error conversion without a cursor scope or local reseed"
                        .to_string(),
                )),
            )),
            JmapMethod::CannotCalculateChanges,
        ),
        MethodErrorType::StateMismatch => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
            JmapMethod::StateMismatch,
        ),
        MethodErrorType::AlreadyExists => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
            JmapMethod::AlreadyExists,
        ),
        MethodErrorType::AnchorNotFound if has_cursor_scope => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            JmapMethod::AnchorNotFound,
        ),
        MethodErrorType::AnchorNotFound => (
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::Jmap(JmapMethod::AnchorNotFound)),
            JmapMethod::AnchorNotFound,
        ),
        MethodErrorType::UnsupportedSort => (
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
            JmapMethod::UnsupportedSort,
        ),
        MethodErrorType::UnsupportedFilter => (
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
            JmapMethod::UnsupportedFilter,
        ),
        MethodErrorType::TooManyChanges if has_cursor_scope => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            JmapMethod::TooManyChanges,
        ),
        MethodErrorType::TooManyChanges => (
            AccountErrorKind::Internal(InternalErrorKind::InvariantViolated),
            Cause::Internal(InternalCause::new(
                InternalErrorKind::InvariantViolated,
                Some(DiagnosticText::support_only(
                    "state expiry reached error conversion without a cursor scope or local reseed"
                        .to_string(),
                )),
            )),
            JmapMethod::TooManyChanges,
        ),
        MethodErrorType::Other(code) => {
            let unknown = JmapMethod::Unknown { code: code.clone() };
            (
                AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
                Cause::Wire(WireCause::Jmap(unknown.clone())),
                unknown,
            )
        }
    };

    let mut builder = build_with(&ctx, kind, primary).push_cause(Cause::Attempt(
        AttemptCause::new(TransmissionState::Acknowledged),
    ));
    // Preserve the server's own explanation and named limit as
    // support-only diagnostics - the error-model contract prizes evidence
    // preservation, and these were previously dropped at the wire.
    if let Some(description) = method.description() {
        builder = builder.text(DiagnosticText::support_only(description.to_string()));
    }
    if let Some(limit) = method.limit() {
        builder = builder.text(DiagnosticText::support_only(format!(
            "server-named limit: {limit}"
        )));
    }
    // Push the wire cause as a forensic-only layer only if it differs
    // from the primary cause already on the chain (ServerPartialFail
    // and Other already use Wire as primary).
    let primary_was_wire = matches!(
        method_type,
        MethodErrorType::ServerPartialFail | MethodErrorType::Other(_)
    );
    if !primary_was_wire {
        builder = builder.push_cause(Cause::Wire(WireCause::Jmap(wire)));
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// An id the server listed in a `/get` response's `notFound`: the object
/// is gone, or this account cannot reach it. Terminal for the id, which
/// is the honest answer - re-requesting it will produce the same
/// `notFound`.
#[must_use]
pub(crate) fn get_id_not_found(id: impl Into<String>, ctx: JmapErrorContext) -> AccountError {
    convert_id_not_found(id.into(), ctx)
}

/// An id that a `/get` answered in NEITHER `list` nor `notFound`.
///
/// RFC 8620 s5.1 requires a conforming server to account for every
/// requested id in exactly one of the two, so an unanswered id is a
/// contract breach - but it must not classify as
/// `Protocol(ContractViolation)`, which derives the terminal
/// `ProviderContractViolation` and would drop the id for good. `/get` is
/// idempotent and the omission says nothing about the object, so
/// `Protocol(PartialResponse)` is the right class: the shared recovery
/// mapping retries it and the next `/get` normally answers. Transmission
/// evidence is `Acknowledged` - the method response itself arrived, only
/// this id's answer is missing.
///
/// This is the same shape `bifrost-graph` uses for a `$batch` request
/// that comes back with fewer responses than it submitted.
#[must_use]
pub(crate) fn get_id_unanswered(id: &str, ctx: JmapErrorContext) -> AccountError {
    build(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(format!(
                "requested id {id} appeared in neither `list` nor `notFound`"
            ))),
        }),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// An id a `/query` returned that the hydrating `/get` then failed to
/// materialize - either declared `notFound` or answered in neither lane.
///
/// This is the classification for a door whose return type has NO
/// per-item lane (`filters_list` hands back a plain `Vec`), so the id
/// cannot ride a failed-ids channel and must not simply be dropped:
/// silently returning a shorter list reads to the consumer as "that
/// filter does not exist", which is exactly the deletion this cannot
/// prove. `Protocol(PartialResponse)` (retryable, acknowledged
/// transmission) is the same class `get_id_unanswered` uses and the same
/// answer `contact_update` gives when a read it depends on materializes
/// nothing. `notFound` deliberately shares it rather than taking the
/// terminal `get_id_not_found` lane: at page level the honest reading of
/// a script that the query named and the get disclaimed is a delete that
/// raced the two calls, and the retry's fresh `/query` will not name it
/// again.
#[must_use]
pub(crate) fn get_id_unresolved_after_query(
    id: &str,
    declared_not_found: bool,
    ctx: JmapErrorContext,
) -> AccountError {
    let lane = if declared_not_found {
        "declared notFound by"
    } else {
        "answered in neither `list` nor `notFound` by"
    };
    build(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(format!(
                "id {id} returned by /query was {lane} the follow-up /get"
            ))),
        }),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// An `Email/set` response that accounted for a submitted update or destroy
/// id in neither its success nor its error map. RFC 8620 requires exactly
/// one answer per submitted id, but the omission says nothing about whether
/// the write landed. The method response did arrive, so preserve
/// acknowledged transmission evidence and let non-idempotent work reconcile.
#[must_use]
pub(crate) fn set_id_unanswered(id: &str, ctx: JmapErrorContext) -> AccountError {
    build(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(format!(
                "submitted Email/set id {id} appeared in neither a success nor an error map"
            ))),
        }),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .try_build()
    .expect("valid account error classification")
}

fn convert_id_not_found(id: String, ctx: JmapErrorContext) -> AccountError {
    if let Some(resource) = resource_from_scope(ctx.scope.as_ref()) {
        build(
            AccountErrorKind::NotFound(resource),
            Cause::Request(RequestCause::NotFound {
                what: resource,
                id: Some(id),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification")
    } else {
        build(
            AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(format!(
                    "id not found in response: {id}"
                ))),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification")
    }
}

#[cfg(feature = "websockets")]
fn websocket_runtime_error(message: String, ctx: JmapErrorContext) -> AccountError {
    // Per the plan, post-handshake WebSocket errors classify as
    // `Protocol(PartialResponse)` with `Attempt(Acknowledged)`. The
    // pre-handshake `WebSocketSetup(Tls)` arm is handled in the main
    // `match` and routes through `Transport(Tls) + Unsent`. This
    // helper covers stream-level errors and post-handshake closes.
    build(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(message)),
        }),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// A frame write to an established WebSocket's sink failed.
///
/// `Transport(Network)`, because the connection is gone and nothing the
/// server said is involved: this used to share `websocket_runtime_error`,
/// which stamps `Acknowledged` on a frame nothing acknowledged and, for a
/// non-idempotent operation, derived a `PartialCompletionSignal` reconcile.
/// `AlreadyClosed` is `tokio_websockets` refusing in `start_send`, before
/// the frame is queued, so it is `Unsent`; any other error came out of the
/// flush, after some of the frame may have been written, so it is
/// `InFlight`. Whether an in-flight failure replays or reconciles is then
/// the operation's idempotency, which the caller owns.
#[cfg(feature = "websockets")]
fn websocket_send_error(err: &tokio_websockets::Error, ctx: JmapErrorContext) -> AccountError {
    let transmission = if matches!(err, tokio_websockets::Error::AlreadyClosed) {
        TransmissionState::Unsent
    } else {
        TransmissionState::InFlight
    };
    build(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(
            TransportKind::Network,
            Some(DiagnosticText::support_only(format!(
                "WebSocket frame write failed: {err}"
            ))),
        )),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(transmission)))
    .try_build()
    .expect("valid account error classification")
}

/// Pre-handshake WebSocket failure. Classification is
/// `Transport(Network) + Attempt(Unsent)` - no bytes from the JMAP
/// request itself crossed the side-effect boundary. The blanket
/// `From<tokio_websockets::Error>` impl that previously routed every
/// websocket error through `websocket_runtime_error` mis-classified
/// handshake-time TCP/TLS drops.
#[cfg(feature = "websockets")]
fn websocket_handshake_error(message: String, ctx: JmapErrorContext) -> AccountError {
    build(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(
            TransportKind::Network,
            Some(DiagnosticText::support_only(message)),
        )),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .try_build()
    .expect("valid account error classification")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::{MethodError, ProblemDetails};
    use bifrost_types::{
        AccessErrorKind, AccountErrorKind, AccountOperation, ProtocolErrorKind, RecoveryClass,
        RetryDisposition, ServerErrorKind, SyncStateErrorKind, ThrottleScope,
    };

    fn method_error(kind: &str) -> crate::Error {
        let err: MethodError = serde_json::from_str(&format!(r#"{{"type":"{kind}"}}"#)).unwrap();
        crate::Error::Method(err)
    }

    fn problem_with_status(error: ProblemType, status: Option<u32>) -> crate::Error {
        crate::Error::Problem {
            details: Box::new(ProblemDetails::new(error, status, None, None, None, None)),
            transport: None,
        }
    }

    fn retry_after_headers(seconds: &'static str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static(seconds),
        );
        headers
    }

    /// Route a wire response through the same `TransportError` ->
    /// `crate::Error` conversion the transports use, so these tests see
    /// exactly what a live response produces.
    fn from_net(error: bifrost_net::Error) -> crate::Error {
        crate::Error::from(crate::core::transport::TransportError::from_net(error))
    }

    /// A local failure the transport marks is authoritative: it classifies
    /// as the client's own invariant and stops, where the no-evidence
    /// fallback called it `Transport(Network)` and the engine retried it as
    /// a network fault forever. An unmarked error with no wire evidence (a
    /// test stub's missing reply) keeps the fallback.
    #[test]
    fn a_local_transport_failure_is_internal_not_a_network_retry() {
        use crate::core::transport::TransportError;
        for local in [
            TransportError::invariant("unsupported HTTP method: PUT"),
            TransportError::invariant_with_source(
                "Invalid forwarded-for header",
                std::io::Error::other("bad byte"),
            ),
        ] {
            let err = into_account_error(
                crate::Error::from(local),
                JmapErrorContext::new(AccountOperation::SyncChanges),
            );
            assert_eq!(
                err.kind(),
                &AccountErrorKind::Internal(InternalErrorKind::InvariantViolated)
            );
            assert_eq!(
                err.recovery(),
                &bifrost_types::RecoveryClass::InternalFailure
            );
        }

        let unmarked = into_account_error(
            crate::Error::from(TransportError::new("stub transport has no queued reply")),
            JmapErrorContext::new(AccountOperation::SyncChanges),
        );
        assert_eq!(
            unmarked.kind(),
            &AccountErrorKind::Transport(TransportErrorKind::Network)
        );
    }

    /// The EventSource block cap is this crate's bound on a stream the
    /// protocol leaves unbounded, so tripping it is the client's limit, not
    /// the provider's fault and not a network drop. Driven through the real
    /// parser: the error it yields used to carry no local mark and fell to
    /// the no-evidence `Transport(Network)` fallback, a retryable class for a
    /// block a reconnect replays unchanged.
    #[test]
    fn an_oversized_event_source_block_is_an_internal_limit_not_a_network_retry() {
        use crate::event_source::parser::{EventParser, MAX_EVENT_SIZE};
        let mut parser = EventParser::default();
        let mut frame = Vec::from(":");
        frame.extend_from_slice(&vec![b'z'; MAX_EVENT_SIZE + 1]);
        parser.push_bytes(frame);
        let parse_error = parser.next().expect("an item").expect_err("the cap trips");

        let err = into_account_error(
            parse_error,
            JmapErrorContext::new(AccountOperation::PushStream),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Internal(InternalErrorKind::LimitExceeded)
        );
        assert_eq!(err.recovery(), &RecoveryClass::InternalFailure);
        assert!(
            err.chain().iter().any(|cause| matches!(
                cause,
                Cause::Internal(internal) if internal.kind == InternalErrorKind::LimitExceeded
            )),
            "the primary cause must name the same internal kind"
        );
    }

    /// A JMAP `limit` problem is not retried by bifrost-net when the
    /// server states it as a terminal 4xx, so it arrives as
    /// `Error::Status` and its only retry hint is the `Retry-After`
    /// header. Reading the hint off `RateLimited` - a shape that cannot
    /// reach this branch, because a 429 is retried and never surfaces as
    /// a status - dropped the server's hint on every one of them.
    #[test]
    fn a_terminal_limit_problem_keeps_the_servers_retry_after_header() {
        let err = into_account_error(
            from_net(bifrost_net::Error::Status {
                code: reqwest::StatusCode::BAD_REQUEST,
                body: bytes::Bytes::from_static(
                    br#"{"type":"urn:ietf:params:jmap:error:limit","limit":"maxObjectsInGet"}"#,
                ),
                headers: retry_after_headers("120"),
            }),
            JmapErrorContext::new(AccountOperation::SyncChanges),
        );

        assert_eq!(
            err.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );
        let RecoveryClass::Retry(advice) = err.recovery() else {
            panic!("expected Retry, got {:?}", err.recovery());
        };
        assert_eq!(
            advice.retry_hint,
            Some(bifrost_types::RetryHint::After(Duration::from_secs(120))),
            "the server's Retry-After must survive the problem-details path"
        );
    }

    #[test]
    fn elapsed_retry_after_date_survives_problem_classification() {
        let date = "Sun, 06 Nov 1994 08:49:37 GMT";
        let at = std::time::UNIX_EPOCH + Duration::from_secs(784111777);
        let err = into_account_error(
            from_net(bifrost_net::Error::Status {
                code: reqwest::StatusCode::BAD_REQUEST,
                body: bytes::Bytes::from_static(
                    br#"{"type":"urn:ietf:params:jmap:error:limit","limit":"maxObjectsInGet"}"#,
                ),
                headers: retry_after_headers(date),
            }),
            JmapErrorContext::new(AccountOperation::SyncChanges),
        );
        let RecoveryClass::Retry(advice) = err.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.retry_hint, Some(RetryHint::At(at)));
    }

    /// A 429 is retried, so it reaches the JMAP boundary as
    /// `RateLimited` with the response preserved on the error rather
    /// than as a status. Its problem document must still be read.
    #[test]
    fn a_retried_429_still_reaches_the_problem_details_mapping() {
        let err = into_account_error(
            from_net(bifrost_net::Error::RateLimited {
                retry_after: Some(Duration::from_secs(30)),
                final_response: bifrost_net::FinalResponse {
                    status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                    headers: reqwest::header::HeaderMap::new(),
                    body: bytes::Bytes::from_static(
                        br#"{"type":"urn:ietf:params:jmap:error:limit"}"#,
                    ),
                },
            }),
            JmapErrorContext::new(AccountOperation::SyncChanges),
        );

        assert_eq!(
            err.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );
        let RecoveryClass::Retry(advice) = err.recovery() else {
            panic!("expected Retry, got {:?}", err.recovery());
        };
        assert_eq!(
            advice.retry_hint,
            Some(bifrost_types::RetryHint::After(Duration::from_secs(30)))
        );
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    }

    /// The 5xx counterpart: a retryable status arrives as
    /// `RetryBudgetExhausted`, and the hint the loop recorded on the
    /// final attempt is the one to surface.
    #[test]
    fn a_retry_exhausted_5xx_problem_keeps_its_final_retry_hint() {
        let err = into_account_error(
            from_net(bifrost_net::Error::RetryBudgetExhausted {
                final_response: Some(bifrost_net::FinalResponse {
                    status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                    headers: retry_after_headers("45"),
                    body: bytes::Bytes::from_static(br#"{"type":"about:blank"}"#),
                }),
                retry_after_history: vec![Duration::from_secs(45)],
            }),
            JmapErrorContext::new(AccountOperation::SyncChanges),
        );

        assert_eq!(
            err.kind(),
            &AccountErrorKind::Server(ServerErrorKind::Unavailable)
        );
        let RecoveryClass::Retry(advice) = err.recovery() else {
            panic!("expected Retry, got {:?}", err.recovery());
        };
        assert_eq!(
            advice.retry_hint,
            Some(bifrost_types::RetryHint::After(Duration::from_secs(45)))
        );
        // The generic net mapping reaches the same kind and hint, so pin
        // the one thing only the problem-details path can produce: the
        // document's own type code in the cause chain.
        assert!(
            err.chain().iter().any(|cause| matches!(
                cause,
                Cause::Wire(WireCause::Jmap(JmapMethod::Unknown { code })) if code == "about:blank"
            )),
            "the problem document itself must have been read"
        );
    }

    #[test]
    fn host_attachment_unsupported() {
        // JMAP has no cloud-drive hosting; the leg builds an
        // `Unsupported(HostAttachment)` error via the shared helper.
        let err = unsupported_error(
            AccountOperation::HostAttachment,
            None,
            "JMAP has no cloud-drive attachment hosting",
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::HostAttachment)
        );
    }

    #[test]
    fn send_as_unknown_account_is_malformed() {
        let err = send_as_unknown_account(&bifrost_types::MailboxId("foreign-id".to_string()));
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert_eq!(err.operation(), Some(AccountOperation::Send));
        assert!(err.recovery().is_terminal());
    }

    #[test]
    fn state_mismatch_maps_to_concurrency_conflict() {
        let err = into_account_error(
            method_error("stateMismatch"),
            JmapErrorContext::new(AccountOperation::UpdateFlags),
        );
        assert_eq!(err.kind(), &AccountErrorKind::ConcurrencyConflict);
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.disposition, RetryDisposition::AfterStateRefresh);
            }
            other => panic!("expected Retry, got {other:?}"),
        }
        // Last-but-one cause carries the wire JmapMethod::StateMismatch.
        let wire_seen = err
            .chain()
            .iter()
            .any(|c| matches!(c, Cause::Wire(WireCause::Jmap(JmapMethod::StateMismatch))));
        assert!(wire_seen, "wire cause missing");
    }

    #[test]
    fn state_expiry_restarts_the_supplied_scope() {
        for (code, wire) in [
            ("cannotCalculateChanges", JmapMethod::CannotCalculateChanges),
            ("tooManyChanges", JmapMethod::TooManyChanges),
        ] {
            for operation in [
                AccountOperation::SyncChanges,
                AccountOperation::ScopeLifecycle,
            ] {
                let scope = CursorScope::Type(bifrost_types::ObjectType::Mailbox);
                let err = into_account_error(
                    method_error(code),
                    JmapErrorContext::cursor(operation, scope.clone()),
                );
                assert_eq!(
                    err.kind(),
                    &AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                );
                assert_eq!(
                    err.recovery(),
                    &RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(scope)),
                );
                assert_eq!(err.operation(), Some(operation));
                assert!(err.chain().iter().any(|cause| matches!(
                    cause,
                    Cause::Wire(WireCause::Jmap(method)) if method == &wire,
                )));
            }
        }
    }

    #[test]
    fn state_expiry_without_recovery_context_is_a_client_invariant() {
        // Missing client context is not evidence of a non-conforming server.
        for code in ["cannotCalculateChanges", "tooManyChanges"] {
            let err = into_account_error(
                method_error(code),
                JmapErrorContext::new(AccountOperation::SyncChanges),
            );
            assert_eq!(
                err.kind(),
                &AccountErrorKind::Internal(InternalErrorKind::InvariantViolated),
            );
            assert!(err.recovery().is_terminal());
        }
    }

    #[test]
    fn server_partial_fail_reconciles_for_send() {
        let err = into_account_error(
            method_error("serverPartialFail"),
            JmapErrorContext::new(AccountOperation::Send),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
        );
        assert!(err.recovery().requires_reconciliation());
    }

    #[test]
    fn server_partial_fail_retries_for_idempotent_update() {
        let err = into_account_error(
            method_error("serverPartialFail"),
            JmapErrorContext::new(AccountOperation::UpdateFlags),
        );
        assert!(err.recovery().is_retryable());
    }

    #[test]
    fn forbidden_maps_to_no_permission() {
        let err = into_account_error(
            method_error("forbidden"),
            JmapErrorContext::new(AccountOperation::UpdateFlags),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        );
        assert!(err.recovery().is_terminal());
        assert!(matches!(err.recovery(), RecoveryClass::NoPermission { .. }));
    }

    #[test]
    fn shared_account_permission_loss_derives_disable_scope() {
        use bifrost_types::{EngineDirective, MailboxId};
        let scope = CursorScope::Folder(super::super::foreign::encode_foreign("acct-9", "mbx-3"));
        let owner = MailboxId("acct-9".to_string());
        let err = shared_scope_error(
            method_error("forbidden"),
            &scope,
            Some(&owner),
            JmapErrorContext::cursor(AccountOperation::SyncChanges, scope.clone()),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        );
        assert_eq!(
            *err.recovery(),
            RecoveryClass::Engine(EngineDirective::DisableScope(scope.clone()))
        );
        assert_eq!(err.scope(), Some(&ErrorScope::Cursor(scope)));
    }

    #[test]
    fn primary_account_permission_loss_stays_terminal() {
        // A primary scope passes no owner: the same `forbidden` stays
        // terminal `NoPermission`, not quarantine.
        let scope = CursorScope::Type(bifrost_types::ObjectType::Email);
        let err = shared_scope_error(
            method_error("forbidden"),
            &scope,
            None,
            JmapErrorContext::cursor(AccountOperation::SyncChanges, scope.clone()),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        );
        assert!(err.recovery().is_terminal());
        assert!(matches!(err.recovery(), RecoveryClass::NoPermission { .. }));
    }

    #[test]
    fn unknown_method_maps_to_unsupported_operation() {
        let err = into_account_error(
            method_error("unknownMethod"),
            JmapErrorContext::new(AccountOperation::Search),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::Search)
        );
    }

    #[test]
    fn jmap_limit_problem_maps_rate_limited() {
        let err = into_account_error(
            problem_with_status(ProblemType::JMAP(JMAPError::Limit), Some(429)),
            JmapErrorContext::new(AccountOperation::Send),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
            }
            // Acknowledged + non-idempotent Send is still retryable
            // because the server rejected before commit (the 429 is
            // an acknowledged terminal response, no side effect).
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    /// A session naming no primary account for the capability a request
    /// needs is refused before a byte is sent, and what is missing is the
    /// server's offer, not the credential: `Unsupported` of the caller's own
    /// operation. It used to derive `AuthLost`, whose remediation
    /// (re-authorize) cannot add an account to the session. Both call sites
    /// are covered: the open-time `primary_account::<Mail>()` lookup
    /// (`Discover`) and `Request::call` on an empty default account id.
    #[test]
    fn no_primary_account_is_unsupported_not_auth_lost() {
        for operation in [AccountOperation::Discover, AccountOperation::SyncInventory] {
            let err = into_account_error(
                crate::Error::NoPrimaryAccount {
                    capability: "urn:ietf:params:jmap:mail",
                },
                JmapErrorContext::new(operation).with_scope(ErrorScope::Account),
            );
            assert_eq!(
                err.kind(),
                &AccountErrorKind::Unsupported(operation),
                "{operation:?}"
            );
            assert_eq!(
                err.recovery(),
                &RecoveryClass::Unsupported(operation),
                "{operation:?}"
            );
            assert!(
                !matches!(
                    err.suggested_remediation(),
                    Some(bifrost_types::RemediationAction::Reauthorize)
                ),
                "{operation:?}: re-authorizing cannot add a primary account"
            );
        }
    }

    /// A URL the server published in its session is the provider's
    /// malformed response: `Protocol(ContractViolation)`, the lane
    /// `MalformedCapability` takes, with the property in the diagnostic. The
    /// caller's own unusable URL (`InvalidUrl`) stays `Request(Malformed)`;
    /// before the split, server URLs were filed there too and told the
    /// operator to fix a request the client never made.
    #[test]
    fn a_malformed_session_url_is_the_providers_fault_not_the_callers() {
        let err = into_account_error(
            crate::Error::MalformedSessionUrl {
                property: "uploadUrl",
                detail: "unterminated parameter in URL template: https://x/{accountId".to_string(),
            },
            JmapErrorContext::new(AccountOperation::Discover).with_scope(ErrorScope::Account),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        );
        assert_eq!(err.recovery(), &RecoveryClass::ProviderContractViolation);
        assert!(
            err.chain().iter().any(|cause| matches!(
                cause,
                Cause::Wire(WireCause::MalformedResponse { detail: Some(detail), .. })
                    if detail.value.contains("uploadUrl")
            )),
            "the diagnostic must name the session property"
        );

        let caller = into_account_error(
            crate::Error::InvalidUrl("a custom transport client needs a session URL".to_string()),
            JmapErrorContext::new(AccountOperation::Discover),
        );
        assert_eq!(
            caller.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }

    #[test]
    fn unregistered_foreign_scope_disables_scope() {
        use bifrost_types::EngineDirective;
        let scope =
            CursorScope::Folder(super::super::foreign::encode_foreign("acct-gone", "mbx-1"));
        let err = unregistered_foreign_scope(scope.clone(), AccountOperation::SyncChanges);
        assert_eq!(
            err.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        );
        assert_eq!(
            *err.recovery(),
            RecoveryClass::Engine(EngineDirective::DisableScope(scope.clone()))
        );
        assert_eq!(err.scope(), Some(&ErrorScope::Cursor(scope)));
    }

    #[test]
    fn problem_status_401_maps_auth_lost() {
        let err = into_account_error(
            problem_with_status(ProblemType::Other("token".into()), Some(401)),
            JmapErrorContext::new(AccountOperation::SyncInventory),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        );
        assert!(matches!(err.recovery(), RecoveryClass::AuthLost));
    }

    #[test]
    fn problem_not_json_maps_protocol_contract_violation() {
        // jmap-F2: the server's "not JSON" / "not request" complaints
        // describe a protocol contract violation, not a client bug. The
        // local serializer would have caught any client-side malformed
        // JSON before sending; if the server insists otherwise, that is
        // a server-side conformance failure.
        let err = into_account_error(
            problem_with_status(ProblemType::JMAP(JMAPError::NotJSON), Some(400)),
            JmapErrorContext::new(AccountOperation::Send),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        );
    }

    #[test]
    fn response_decode_maps_protocol_parse_failed() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err = into_account_error(
            crate::Error::ResponseDecode(json_err),
            JmapErrorContext::new(AccountOperation::SyncInventory),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        );
    }

    #[test]
    fn set_not_found_with_message_scope_maps_not_found() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"notFound"}"#).unwrap();
        let err = set_error_to_account_error(
            set,
            JmapErrorContext::new(AccountOperation::HydrateMessage),
            Some(ErrorScope::Message { id: "msg-1".into() }),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::NotFound(ResourceKind::Message)
        );
    }

    #[test]
    fn set_rate_limit_maps_rate_limited() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"rateLimit"}"#).unwrap();
        let err = set_error_to_account_error(
            set,
            JmapErrorContext::new(AccountOperation::UpdateFlags),
            None,
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );
    }

    #[cfg(feature = "websockets")]
    #[test]
    fn websocket_subprotocol_maps_capability_changed() {
        let err = into_account_error(
            crate::Error::WebSocketSetup(crate::WebSocketSetupError::Subprotocol(
                "server did not accept jmap".to_string(),
            )),
            JmapErrorContext::new(AccountOperation::PushSubscribe),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged)
        );
    }

    #[test]
    fn classify_set_item_absorbs_notfound_for_bulk_destroy() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"notFound"}"#).unwrap();
        let outcome = classify_set_item(
            set,
            JmapErrorContext::new(AccountOperation::BulkDestroy),
            BatchItemId("msg-1".into()),
            Some(ErrorScope::Message { id: "msg-1".into() }),
        );
        assert!(matches!(
            outcome,
            ItemOutcome::Succeeded(BatchSuccess {
                output: MutationSuccess::Skipped,
                ..
            })
        ));
    }

    #[test]
    fn classify_set_item_fails_for_real_error() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"rateLimit"}"#).unwrap();
        let outcome = classify_set_item(
            set,
            JmapErrorContext::new(AccountOperation::UpdateFlags),
            BatchItemId("msg-2".into()),
            None,
        );
        match outcome {
            ItemOutcome::Failed(failure) => {
                assert_eq!(failure.item.0, "msg-2");
                assert_eq!(
                    failure.error.kind(),
                    &AccountErrorKind::Server(ServerErrorKind::RateLimited)
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// jmap-N2: `SetErrorType::Other` carries the actual wire code so
    /// `JmapMethod::Unknown { code }` reflects what the server sent,
    /// instead of synthesizing a placeholder `"other"` literal.
    #[test]
    fn set_error_other_preserves_wire_code() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"someNovelCode"}"#).unwrap();
        let err = set_error_to_account_error(
            set,
            JmapErrorContext::new(AccountOperation::UpdateFlags),
            None,
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::Unknown)
        );
        let mut saw_real_code = false;
        for cause in err.chain().iter() {
            if let Cause::Wire(WireCause::Jmap(JmapMethod::Unknown { code })) = cause {
                saw_real_code = true;
                assert_eq!(code, "someNovelCode");
                assert_ne!(code, "other", "must not synthesize the 'other' placeholder");
            }
        }
        assert!(
            saw_real_code,
            "Unknown wire cause with the real code missing"
        );
    }

    /// jmap-N1: `terminated_unsupported(op, scope, msg)` carries the
    /// caller's operation. The pre-fix helper hard-coded
    /// `AccountOperation::Discover`, mis-classifying every stream that
    /// emitted an unsupported event.
    #[test]
    fn terminated_unsupported_threads_operation() {
        let event = terminated_unsupported::<()>(
            AccountOperation::SyncInventory,
            Some(ErrorScope::Cursor(CursorScope::Type(
                bifrost_types::ObjectType::Thread,
            ))),
            "JMAP thread inventory is derived from Email inventory",
        );
        match event {
            SyncEvent::Terminated(err) => {
                assert_eq!(
                    err.kind(),
                    &AccountErrorKind::Unsupported(AccountOperation::SyncInventory)
                );
                assert_eq!(err.operation(), Some(AccountOperation::SyncInventory));
            }
            other => panic!("expected Terminated, got {other:?}"),
        }
    }

    /// jmap-N1 follow-on: pagination overflows reclassify as
    /// `Protocol(ContractViolation)` because the server response shape
    /// is incompatible with the request; "unsupported" was the wrong
    /// consumer-facing classification.
    #[test]
    fn terminated_contract_violation_classifies_pagination_overflow() {
        let event = terminated_contract_violation::<()>(
            AccountOperation::SyncInventory,
            Some(ErrorScope::Cursor(CursorScope::Type(
                bifrost_types::ObjectType::Email,
            ))),
            "JMAP inventory position overflowed",
        );
        match event {
            SyncEvent::Terminated(err) => {
                assert_eq!(
                    err.kind(),
                    &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
                );
                assert!(err.recovery().is_terminal());
            }
            other => panic!("expected Terminated, got {other:?}"),
        }
    }

    /// jmap-D1: WebSocket pre-handshake failures classify as
    /// `Transport(Network)` with `Attempt(Unsent)`, not
    /// `Protocol(PartialResponse) + Acknowledged`.
    #[cfg(feature = "websockets")]
    #[test]
    fn websocket_handshake_classifies_as_transport_network_unsent() {
        // Construct a `tokio_websockets::Error` indirectly via a known
        // failure path. We can't easily build one directly, so synthesize
        // the classification helper and assert its shape.
        let ctx = JmapErrorContext::new(AccountOperation::PushSubscribe);
        let err = websocket_handshake_error("handshake aborted".to_string(), ctx);
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Transport(TransportErrorKind::Network)
        );
        let mut saw_unsent = false;
        for cause in err.chain().iter() {
            if let Cause::Attempt(attempt) = cause
                && attempt.transmission_state == TransmissionState::Unsent
            {
                saw_unsent = true;
            }
        }
        assert!(saw_unsent, "expected Attempt(Unsent) on the chain");
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.disposition, RetryDisposition::SameRequest);
            }
            other => panic!("expected Retry::SameRequest, got {other:?}"),
        }
    }

    /// jmap-D1: WebSocket post-handshake (runtime) failures classify as
    /// `Protocol(PartialResponse)` with `Attempt(Acknowledged)`.
    #[cfg(feature = "websockets")]
    #[test]
    fn websocket_runtime_classifies_as_protocol_partial_response_acknowledged() {
        let ctx = JmapErrorContext::new(AccountOperation::PushSubscribe);
        let err = websocket_runtime_error("stream dropped".to_string(), ctx);
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
        );
        let mut saw_acknowledged = false;
        for cause in err.chain().iter() {
            if let Cause::Attempt(attempt) = cause
                && attempt.transmission_state == TransmissionState::Acknowledged
            {
                saw_acknowledged = true;
            }
        }
        assert!(
            saw_acknowledged,
            "expected Attempt(Acknowledged) on the chain"
        );
    }

    fn transmission_state(err: &AccountError) -> Option<TransmissionState> {
        err.chain().iter().find_map(|cause| match cause {
            Cause::Attempt(attempt) => Some(attempt.transmission_state),
            _ => None,
        })
    }

    /// A frame the library refused to queue on a closed stream reached
    /// nobody. It used to classify with the read side as
    /// `Protocol(PartialResponse)` + `Acknowledged`, which for a
    /// non-idempotent operation derives a `PartialCompletionSignal`
    /// reconcile of a write that never left the process.
    #[cfg(feature = "websockets")]
    #[test]
    fn a_frame_refused_on_a_closed_websocket_is_an_unsent_transport_failure() {
        let err = into_account_error(
            crate::Error::WebSocketSend(tokio_websockets::Error::AlreadyClosed),
            JmapErrorContext::new(AccountOperation::PushSubscribe),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Transport(TransportErrorKind::Network)
        );
        assert_eq!(transmission_state(&err), Some(TransmissionState::Unsent));
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.disposition, RetryDisposition::SameRequest);
            }
            other => panic!("an unsent frame is replayable, got {other:?}"),
        }
    }

    /// An I/O failure while flushing may have put part of the frame on the
    /// wire, so it is `InFlight`: the operation's idempotency decides
    /// between replay and read-back, not a fabricated acknowledgement.
    #[cfg(feature = "websockets")]
    #[test]
    fn a_frame_write_that_failed_mid_flush_is_an_in_flight_transport_failure() {
        let err = into_account_error(
            crate::Error::WebSocketSend(tokio_websockets::Error::Io(std::io::Error::from(
                std::io::ErrorKind::BrokenPipe,
            ))),
            JmapErrorContext::new(AccountOperation::Send),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Transport(TransportErrorKind::Network)
        );
        assert_eq!(transmission_state(&err), Some(TransmissionState::InFlight));
        assert!(
            matches!(err.recovery(), RecoveryClass::Reconcile(_)),
            "a non-idempotent write lost mid-flight is read back, got {:?}",
            err.recovery()
        );
    }

    /// Every `type` string RFC 8620 s3.6.2 defines for a method-level
    /// error, plus an unregistered one.
    const METHOD_ERROR_CODES: &[&str] = &[
        "serverUnavailable",
        "serverFail",
        "serverPartialFail",
        "unknownMethod",
        "invalidArguments",
        "invalidResultReference",
        "forbidden",
        "accountNotFound",
        "accountNotSupportedByMethod",
        "accountReadOnly",
        "requestTooLarge",
        "cannotCalculateChanges",
        "stateMismatch",
        "alreadyExists",
        "fromAccountNotFound",
        "fromAccountNotSupportedByMethod",
        "anchorNotFound",
        "unsupportedSort",
        "unsupportedFilter",
        "tooManyChanges",
        "someCodeThisBuildHasNeverSeen",
    ];

    /// Every `SetError` `type` string the crate recognises, plus an
    /// unregistered one.
    const SET_ERROR_CODES: &[&str] = &[
        "forbidden",
        "overQuota",
        "tooLarge",
        "rateLimit",
        "notFound",
        "invalidPatch",
        "willDestroy",
        "invalidProperties",
        "singleton",
        "mailboxHasChild",
        "mailboxHasEmail",
        "blobNotFound",
        "tooManyKeywords",
        "tooManyMailboxes",
        "forbiddenFrom",
        "invalidEmail",
        "tooManyRecipients",
        "noRecipients",
        "invalidRecipients",
        "forbiddenMailFrom",
        "forbiddenToSend",
        "cannotUnsend",
        "alreadyExists",
        "invalidScript",
        "scriptIsActive",
        "someCodeThisBuildHasNeverSeen",
    ];

    fn assert_wire_shape(err: &AccountError, code: &str) {
        assert_eq!(err.protocol(), Some(Protocol::Jmap), "{code}");
        let mut saw_wire = false;
        let mut saw_acknowledged = false;
        for cause in err.chain().iter() {
            match cause {
                Cause::Wire(WireCause::Jmap(_)) => saw_wire = true,
                Cause::Attempt(attempt)
                    if attempt.transmission_state == TransmissionState::Acknowledged =>
                {
                    saw_acknowledged = true;
                }
                _ => {}
            }
        }
        assert!(saw_wire, "{code}: no typed JMAP wire cause on the chain");
        assert!(
            saw_acknowledged,
            "{code}: a server-returned error crossed the side-effect boundary \
             and must carry Attempt(Acknowledged)"
        );
    }

    /// Total conformance sweep over the method-error code space. Every
    /// arm of `convert_method` ends in
    /// `try_build().expect("valid account error classification")`, so a
    /// kind/cause pair that violates the builder's `kind_matches_cause`
    /// or scope invariants panics in production rather than degrading.
    /// This pins that no code panics, that each carries the protocol,
    /// the caller's operation, a typed wire cause, and acknowledged
    /// transmission evidence, and that a scoped call keeps its scope.
    #[test]
    fn every_method_error_code_builds_a_well_formed_account_error() {
        let scope = CursorScope::Type(bifrost_types::ObjectType::Email);
        for code in METHOD_ERROR_CODES {
            let bare = into_account_error(
                method_error(code),
                JmapErrorContext::new(AccountOperation::SyncChanges),
            );
            assert_eq!(
                bare.operation(),
                Some(AccountOperation::SyncChanges),
                "{code}"
            );
            assert_eq!(bare.scope(), None, "{code}");
            assert_wire_shape(&bare, code);

            let scoped = into_account_error(
                method_error(code),
                JmapErrorContext::cursor(AccountOperation::SyncChanges, scope.clone()),
            );
            assert_eq!(
                scoped.scope(),
                Some(&ErrorScope::Cursor(scope.clone())),
                "{code}"
            );
            assert_wire_shape(&scoped, code);
        }
    }

    /// Cursor-specific expiry methods restart THIS scope only when the
    /// caller threaded a cursor scope. Without one, expiry of a change
    /// cursor is an invariant error; `anchorNotFound` remains a protocol
    /// contract violation because there is no cursor scope to restart.
    #[test]
    fn cursor_method_errors_need_a_cursor_scope_to_restart_a_scope() {
        let scope = CursorScope::Type(bifrost_types::ObjectType::Email);
        for code in ["cannotCalculateChanges", "anchorNotFound", "tooManyChanges"] {
            let scoped = into_account_error(
                method_error(code),
                JmapErrorContext::cursor(AccountOperation::SyncChanges, scope.clone()),
            );
            assert_eq!(
                scoped.kind(),
                &AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                "{code}"
            );
            assert_eq!(
                scoped.recovery(),
                &RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(scope.clone())),
                "{code}"
            );

            let bare = into_account_error(
                method_error(code),
                JmapErrorContext::new(AccountOperation::SyncChanges),
            );
            let expected = if code == "anchorNotFound" {
                AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
            } else {
                AccountErrorKind::Internal(bifrost_types::InternalErrorKind::InvariantViolated)
            };
            assert_eq!(bare.kind(), &expected, "{code}");
        }
    }

    /// A method error naming an account the session no longer resolves
    /// is a capability shift, not a permission or protocol failure: the
    /// engine must reopen and re-run discovery.
    #[test]
    fn account_scoped_method_errors_reopen_the_account() {
        for code in [
            "accountNotFound",
            "fromAccountNotFound",
            "accountNotSupportedByMethod",
            "fromAccountNotSupportedByMethod",
        ] {
            let err = into_account_error(
                method_error(code),
                JmapErrorContext::new(AccountOperation::SyncChanges),
            );
            assert_eq!(
                err.kind(),
                &AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
                "{code}"
            );
            assert_eq!(
                err.recovery(),
                &RecoveryClass::Engine(bifrost_types::EngineDirective::RestartAccount),
                "{code}"
            );
        }
    }

    fn set_error(code: &str) -> SetError<String> {
        serde_json::from_str(&format!(r#"{{"type":"{code}"}}"#)).expect("set error parses")
    }

    /// Total conformance sweep over the set-error code space, in both
    /// scope shapes. The unscoped shape is the one that historically
    /// produced a kind/cause pair the builder rejects (see the
    /// `NotFound` comment in `set_error_to_account_error`), so both are
    /// exercised.
    #[test]
    fn every_set_error_code_builds_a_well_formed_account_error() {
        for code in SET_ERROR_CODES {
            let bare = set_error_to_account_error(
                set_error(code),
                JmapErrorContext::new(AccountOperation::UpdateFlags),
                None,
            );
            assert_eq!(
                bare.operation(),
                Some(AccountOperation::UpdateFlags),
                "{code}"
            );
            assert_wire_shape(&bare, code);

            let item_scope = ErrorScope::Message {
                id: ("msg-1".to_string()).into(),
            };
            let scoped = set_error_to_account_error(
                set_error(code),
                JmapErrorContext::new(AccountOperation::UpdateFlags),
                Some(item_scope.clone()),
            );
            assert_eq!(scoped.scope(), Some(&item_scope), "{code}");
            assert_wire_shape(&scoped, code);
        }
    }

    /// The `/set` NotFound family only resolves to a typed `NotFound`
    /// when a scope names WHAT was missing. Without one there is no
    /// resource to report, and the classification falls back to a typed
    /// protocol-shape error rather than inventing a resource.
    #[test]
    fn set_not_found_without_a_scope_stays_a_protocol_shape_error() {
        for code in ["notFound", "blobNotFound"] {
            let err = set_error_to_account_error(
                set_error(code),
                JmapErrorContext::new(AccountOperation::UpdateFlags),
                None,
            );
            assert_eq!(
                err.kind(),
                &AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
                "{code}"
            );
        }
    }

    /// Per-item NotFound absorption is scoped to the bulk/idempotent
    /// operations that can tolerate a vanished object. Every other
    /// operation must surface the miss as a failure - absorbing a
    /// `notFound` on a create or a send would report success for work
    /// the server never did.
    #[test]
    fn not_found_absorption_is_limited_to_idempotent_bulk_operations() {
        let absorbing = [
            AccountOperation::UpdateFlags,
            AccountOperation::BulkMove,
            AccountOperation::BulkDestroy,
            AccountOperation::SetKeyword,
            AccountOperation::SetIsRead,
            AccountOperation::AddToContainer,
            AccountOperation::RemoveFromContainer,
        ];
        for operation in absorbing {
            for code in ["notFound", "blobNotFound"] {
                let outcome = classify_set_item(
                    set_error(code),
                    JmapErrorContext::new(operation),
                    BatchItemId("msg-1".to_string()),
                    Some(ErrorScope::Message {
                        id: ("msg-1".to_string()).into(),
                    }),
                );
                assert!(
                    matches!(
                        outcome,
                        ItemOutcome::Succeeded(BatchSuccess {
                            output: MutationSuccess::Skipped,
                            ..
                        })
                    ),
                    "{operation:?} / {code}"
                );
            }
        }

        for operation in [
            AccountOperation::Send,
            AccountOperation::DraftCreate,
            AccountOperation::ContainerCreate,
            AccountOperation::SetImportance,
        ] {
            let outcome = classify_set_item(
                set_error("notFound"),
                JmapErrorContext::new(operation),
                BatchItemId("msg-1".to_string()),
                Some(ErrorScope::Message {
                    id: ("msg-1".to_string()).into(),
                }),
            );
            assert!(
                matches!(outcome, ItemOutcome::Failed(_)),
                "{operation:?} must not absorb a notFound"
            );
        }
    }
}
