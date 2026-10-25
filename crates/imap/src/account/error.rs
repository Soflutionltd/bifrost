//! IMAP -> `AccountError` translation boundary.
//!
//! This module is the only place in `bifrost-imap` that constructs
//! `bifrost_types::AccountError`. Every account-trait call site funnels
//! its crate-private `crate::Error` through `into_account_error` with an
//! `ImapErrorContext` carrying operation, scope, and provider.
//!
//! Why this matters: the central recovery mapping in
//! `bifrost_types::error::recovery` derives `RecoveryClass` from
//! `(kind, scope, operation, primary_cause)`, where the primary cause is
//! often a `Cause::Attempt(AttemptCause { transmission_state })`. IMAP
//! is its own transport (no `bifrost-net` for protocol traffic), so the
//! driver / pipeline owns the wire-level evidence of whether a command's
//! bytes ever crossed the side-effect boundary. That evidence has to
//! survive into the chain or the engine will silently retry
//! non-idempotent operations across `InFlight` transport drops.
//!
//! The crate root sets `#![allow(dead_code)]`, which is load-bearing for the
//! protocol surface (command builders and response types that consumers use
//! but the crate itself does not). It is actively harmful HERE: a guard in
//! this module that nothing arms is a silent hole in the contract above, and
//! `Translation::skip_attempt_cause` was exactly that - documented as
//! suppressing a `try_build` invariant violation, never assigned `true`, so
//! the guarantee was upheld by comment only. This module opts back in so the
//! compiler reports an unreachable translation rather than a human catching
//! it on the next audit.
#![warn(dead_code)]

use bifrost_types::{
    AccessCause, AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation,
    AttemptCause, AuthCause, AuthErrorKind, Cause, CursorScope, DiagnosticText, ErrorScope,
    ImapResponseCode, InternalCause, InternalErrorKind, Protocol, ProtocolErrorKind, Provider,
    RequestCause, RequestErrorKind, ResourceKind, ServerCause, ServerErrorKind, StateCause,
    StrategyDowngrade, SyncStateErrorKind, ThrottleScope, TransmissionState, TransportCause,
    TransportErrorKind, TransportKind, WireCause,
};

use crate::Error;
use crate::types::{MailboxName, ResponseCode};

/// Context carried alongside `crate::Error` across the account boundary.
///
/// Constructed at every account-trait call site with the operation under
/// way and the scope identifying which mailbox / message / thread the
/// error pertains to. `transmission_state` is a fallback only - the
/// translation prefers `Error::attempt()` when the driver populated it.
#[derive(Clone, Debug)]
pub(crate) struct ImapErrorContext {
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
    pub(crate) provider: Option<Provider>,
    pub(crate) idempotency_override: Option<bool>,
    pub(crate) transmission_state: Option<TransmissionState>,
    /// Extra support-only diagnostics the call site knows and the wire
    /// error does not - notably a confirmed partial side effect that
    /// already landed before this failure. Appended to whatever text the
    /// classification produces; it never replaces the classification, so
    /// the `RecoveryClass` derivation still sees the real failure.
    pub(crate) extra_text: Vec<DiagnosticText>,
}

impl ImapErrorContext {
    pub(crate) fn operation(operation: AccountOperation) -> Self {
        Self {
            operation,
            scope: None,
            provider: None,
            idempotency_override: None,
            transmission_state: None,
            extra_text: Vec::new(),
        }
    }

    /// Unused: every producer reaches for a scope-shaped helper below
    /// (`with_cursor_scope` / `with_folder_scope` / `with_message_id`)
    /// rather than naming an `ErrorScope` directly.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = Some(scope);
        self
    }

    #[must_use]
    pub(crate) fn with_cursor_scope(mut self, scope: CursorScope) -> Self {
        self.scope = Some(ErrorScope::Cursor(scope));
        self
    }

    /// Production-dead: every folder producer migrated to
    /// `with_folder_scope` (`ErrorScope::Cursor(Folder(_))`). Tests still
    /// use it, so the scope READERS must keep accepting `Mailbox { id }` -
    /// see `id_from_scope` / `mailbox_throttle`, where assuming otherwise
    /// made `ThrottleScope::Mailbox` unreachable in production while its
    /// test kept passing.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn with_mailbox(mut self, mailbox: &MailboxName) -> Self {
        self.scope = Some(ErrorScope::Mailbox {
            id: mailbox.account_id().into(),
        });
        self
    }

    /// Like `with_mailbox` but produces an `ErrorScope::Cursor(Folder(_))`
    /// instead of `Mailbox { id }`. Folder-scoped operations (STORE,
    /// FETCH against a folder, mutation paths) use this so that the
    /// `EXPUNGEISSUED` / `CLOSED` / `NotificationOverflow` response
    /// codes - which classify as `SyncState(CursorInvalid)` - build a
    /// structurally valid error. The previous shape paired
    /// `ErrorScope::Mailbox` with `CursorInvalid` and relied on a
    /// translation-boundary auto-promote shim to fix it up; this
    /// helper makes the cursor scoping explicit at the producer.
    #[must_use]
    pub(crate) fn with_folder_scope(mut self, mailbox: &MailboxName) -> Self {
        self.scope = Some(ErrorScope::Cursor(CursorScope::Folder(
            bifrost_types::FolderId(mailbox.account_id()),
        )));
        self
    }

    #[must_use]
    pub(crate) fn with_message_id(mut self, id: impl Into<String>) -> Self {
        self.scope = Some(ErrorScope::Message {
            id: (id.into()).into(),
        });
        self
    }

    /// Unused: the driver stamps transmission state on the `Error` itself
    /// (`error.attempt()`), which `into_account_error` prefers over the
    /// context. So `ctx.transmission_state` is always `None` today.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn with_transmission_state(mut self, state: TransmissionState) -> Self {
        self.transmission_state = Some(state);
        self
    }

    /// Unused by design: IMAP lets `derive` recompute idempotency from the
    /// operation via the central `AccountOperation::is_idempotent` table
    /// rather than overriding per error. Builder round-trips preserve any
    /// explicit override, including through submission restamping.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn with_idempotency_override(mut self, idempotent: bool) -> Self {
        self.idempotency_override = Some(idempotent);
        self
    }
}

/// Translate an internal IMAP error into a public `AccountError`.
pub(crate) fn into_account_error(error: Error, ctx: ImapErrorContext) -> AccountError {
    let attempt = error.attempt().or(ctx.transmission_state);
    let translation = classify(&error, &ctx);
    let Translation {
        kind,
        primary_cause,
        wire_code,
        attempt: explicit_attempt,
        throttle_scope,
        diagnostic_text,
        native_code,
    } = translation;
    let is_transport = matches!(kind, AccountErrorKind::Transport(_));

    // CursorInvalid requires a cursor scope or `try_build` rejects
    // (`CursorInvalidWithoutScope`). Folder-scoped producers use
    // `ImapErrorContext::with_folder_scope(&mailbox)` so the scope
    // is `ErrorScope::Cursor(Folder(id))` rather than
    // `ErrorScope::Mailbox { id }`.
    let scope_for_kind = ctx.scope.clone();

    let mut builder = match kind {
        // Classification tables return parts so they can retain wire evidence.
        AccountErrorKind::Unsupported(operation) => {
            AccountError::unsupported(operation, Protocol::Imap).into_builder()
        }
        _ => AccountErrorBuilder::new(kind, primary_cause).protocol(Protocol::Imap),
    }
    .operation(ctx.operation);

    if let Some(scope) = scope_for_kind {
        builder = builder.scope(scope);
    }
    if let Some(provider) = ctx.provider {
        builder = builder.provider(provider);
    }
    if let Some(override_) = ctx.idempotency_override {
        builder = builder.idempotency_override(override_);
    }
    if let Some(throttle) = throttle_scope {
        builder = builder.throttle_scope(throttle);
    }
    if let Some(code) = native_code {
        builder = builder.native_code(code);
    }
    if let Some(text) = diagnostic_text {
        builder = builder.text(text);
    }
    for text in ctx.extra_text {
        builder = builder.text(text);
    }

    // Wire cause (IMAP response code) is pushed before the attempt
    // cause so that support exports walk wire evidence first.
    if let Some(code) = wire_code {
        builder = builder.push_cause(Cause::Wire(WireCause::Imap(code)));
    }

    // A `Transport(_)` kind means no complete server response arrived, so
    // pairing it with `Acknowledged` is a contradiction `try_build`
    // rejects outright (`TransportAcknowledged`) - which at this boundary
    // means the `.expect` below panics. Nothing constructs that pair
    // today, but nothing prevents it either: `Error::with_attempt` accepts
    // any state on the transport variants, and the driver does apply
    // `Acknowledged` after a tagged response.
    //
    // Demote rather than drop. Dropping the cause entirely would leave
    // `derive` reading its `Unsent` default, which routes a non-idempotent
    // operation to a blind retry; `InFlight` routes it to reconciliation
    // instead, and matches what `recovery::derive` itself does with this
    // pair in release builds.
    let effective_attempt = explicit_attempt.or(attempt).map(|state| {
        if is_transport && state == TransmissionState::Acknowledged {
            TransmissionState::InFlight
        } else {
            state
        }
    });
    if let Some(state) = effective_attempt {
        builder = builder.push_cause(Cause::Attempt(AttemptCause::new(state)));
    }

    builder
        .try_build()
        .expect("valid account error classification")
}

/// Build an `AccountError` for a locally detected `Unsupported` PIM
/// operation. The PIM layer used to return `AccountError::Unsupported`;
/// in the new contract every unsupported call is structured.
pub(crate) fn unsupported(operation: AccountOperation) -> AccountError {
    AccountError::unsupported(operation, Protocol::Imap)
}

/// Build a fatal-stream error for UIDVALIDITY change on a folder cursor.
pub(crate) fn uidvalidity_changed(
    folder: &MailboxName,
    expected: u32,
    actual: u32,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::SyncChanges)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .text(DiagnosticText::support_only(format!(
        "IMAP UIDVALIDITY changed for {} from {} to {}",
        folder.as_str(),
        expected,
        actual,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Build a fatal-stream error for HIGHESTMODSEQ regression on a folder cursor.
pub(crate) fn modseq_reset(
    folder: &MailboxName,
    previous: u64,
    current: Option<u64>,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::SyncChanges)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .text(DiagnosticText::support_only(format!(
        "IMAP HIGHESTMODSEQ reset for {} from {} to {:?}",
        folder.as_str(),
        previous,
        current,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Build a recoverable cursor error when a legacy QRESYNC cursor has no exact
/// membership baseline. Current server state cannot reconstruct past consumer
/// state, so the engine must inventory the scope again.
pub(crate) fn incomplete_uid_baseline(folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::SyncChanges)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .text(DiagnosticText::support_only(format!(
        "IMAP cursor for {} has no complete UID membership baseline",
        folder.as_str(),
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Build a fatal-stream error for a terminal QRESYNC/CONDSTORE strategy
/// downgrade that cannot be served on the current connection.
///
/// Unreachable today, and structurally so: a downgrade always has somewhere
/// to land (QRESYNC -> CONDSTORE -> Basic, and Basic is plain FETCH, which
/// every server serves), so the sync lanes report one as a non-fatal
/// `WarningKind::StrategyDowngraded` and continue on the weaker strategy
/// instead of failing the stream. Kept because "no terminal downgrade" is a
/// property of the current strategy ladder rather than of the error model:
/// a future strategy with no weaker peer would need exactly this.
#[allow(dead_code)]
pub(crate) fn strategy_failure(folder: &MailboxName, downgrade: StrategyDowngrade) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure),
        Cause::State(StateCause::StrategyFailure { downgrade }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::SyncChanges)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .try_build()
    .expect("valid account error classification")
}

/// Build a `SyncState(ScopeRevoked)` error scoped to one shared folder.
/// Derives to `Engine(DisableScope(Folder(id)))` - the engine quarantines
/// just that scope without escalating to account-wide auth loss. The
/// owning mailbox rides as support-only diagnostic text for telemetry.
pub(crate) fn scope_revoked(
    folder: &MailboxName,
    owner: &bifrost_types::MailboxId,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
        Cause::State(StateCause::ScopeRevoked),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .text(DiagnosticText::support_only(format!(
        "shared folder access revoked for {} (owner {})",
        folder.as_str(),
        owner.0,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Map a per-folder failure for a shared/other-user folder. When the
/// failure is a permission/access denial (the class that would otherwise
/// derive terminal `NoPermission`) and the folder is shared
/// (`shared_owner.is_some()`), produce a scoped `ScopeRevoked` that
/// quarantines just this folder instead of escalating account-wide. A
/// personal folder (`shared_owner == None`), or any non-permission
/// failure, flows through the normal `into_account_error` mapping - a
/// personal-folder permission loss is a genuine account-level signal.
pub(crate) fn shared_folder_error(
    error: Error,
    folder: &MailboxName,
    shared_owner: Option<&bifrost_types::MailboxId>,
    ctx: ImapErrorContext,
) -> AccountError {
    if let Some(owner) = shared_owner {
        let translation = classify(&error, &ctx);
        if matches!(
            translation.kind,
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
        ) {
            return scope_revoked(folder, owner, ctx.operation);
        }
    }
    into_account_error(error, ctx)
}

// ---------------------------------------------------------------------
// Internal classification machinery.
// ---------------------------------------------------------------------

struct Translation {
    kind: AccountErrorKind,
    primary_cause: Cause,
    wire_code: Option<ImapResponseCode>,
    /// Explicit attempt-state override (used by `Bye` during command
    /// response loop, etc.). When `None`, the builder uses the
    /// `Error::attempt()` evidence.
    attempt: Option<TransmissionState>,
    throttle_scope: Option<ThrottleScope>,
    diagnostic_text: Option<DiagnosticText>,
    native_code: Option<String>,
}

impl Translation {
    fn new(kind: AccountErrorKind, primary_cause: Cause) -> Self {
        Self {
            kind,
            primary_cause,
            wire_code: None,
            attempt: None,
            throttle_scope: None,
            diagnostic_text: None,
            native_code: None,
        }
    }
}

/// A client-side failure: `Internal(kind)`, never a provider fault. The
/// attempt state is whatever the error carries (`Error::attempt()`).
fn internal(kind: InternalErrorKind, detail: String) -> Translation {
    Translation::new(
        AccountErrorKind::Internal(kind),
        Cause::Internal(InternalCause::new(
            kind,
            Some(DiagnosticText::support_only(detail)),
        )),
    )
}

fn classify(error: &Error, ctx: &ImapErrorContext) -> Translation {
    match error {
        Error::MoveFallbackIncomplete { source, .. } => classify(source, ctx),
        Error::Io { source, .. } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only(source.to_string())),
            )),
        ),
        Error::Timeout { .. } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Timeout),
            Cause::Transport(TransportCause::new(TransportKind::Timeout, None)),
        ),
        Error::Closed { .. } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only("IMAP connection closed")),
            )),
        ),
        Error::DriverGone { .. } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only("IMAP driver task gone")),
            )),
        ),
        // Client-side failures (`reference/error-model.md`): the driver
        // panicked, never the server's fault. Its attempt state is the
        // caller's phase (`observe_driver_panic`), so a non-idempotent
        // command the driver owned when it died reconciles rather than
        // terminating.
        Error::DriverPanicked { message, .. } => internal(
            InternalErrorKind::Panicked,
            format!("IMAP driver panicked: {message}"),
        ),
        Error::Auth {
            text,
            code,
            mechanism,
            attempt,
        } => {
            let mut translation = classify_auth(text, code.as_ref(), *mechanism);
            if let Some(attempt) = attempt {
                translation.attempt = Some(attempt.transmission_state);
            }
            translation
        }
        Error::AuthPolicy(failure) => {
            let mut t = Translation::new(
                AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
                Cause::Access(AccessCause::PolicyBlocked),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(failure.to_string()));
            t
        }
        Error::StartTlsUnavailable => {
            let mut t = Translation::new(
                AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
                Cause::Access(AccessCause::PolicyBlocked),
            );
            // PREAUTH can make the required upgrade illegal even when the
            // greeting advertises STARTTLS. Describe the session requirement.
            t.diagnostic_text = Some(DiagnosticText::support_only(
                "required STARTTLS upgrade is unavailable in this session",
            ));
            t
        }
        Error::No { text, code, .. } => {
            classify_status(text, code.as_ref(), StatusFallback::No, ctx)
        }
        Error::Bad { text, code, .. } => {
            classify_status(text, code.as_ref(), StatusFallback::Bad, ctx)
        }
        Error::Bye { text, code, .. } => {
            classify_status(text, code.as_ref(), StatusFallback::Bye, ctx)
        }
        Error::Sieve { code, message } => classify_sieve(code.as_ref(), message, ctx),
        Error::Protocol(msg) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(msg.clone())),
            }),
        ),
        // Same recovery class as `Protocol` (`ProviderContractViolation`);
        // the kind only records that the server omitted something mandatory.
        Error::ProtocolMissing(msg) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(msg.clone())),
            }),
        ),
        Error::Parse(msg) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(msg.clone())),
            }),
        ),
        Error::InvalidInput(msg) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(msg.clone()),
            }),
        ),
        // Local refusals, all made before the first byte. None of them is a
        // provider fault (`reference/error-model.md`, "Local refusals").
        Error::InvalidState(msg) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(msg.clone()),
            }),
        ),
        Error::StateChangedBeforeSend(msg) => {
            let mut t = Translation::new(
                AccountErrorKind::ConcurrencyConflict,
                Cause::State(bifrost_types::StateCause::ConcurrencyConflict),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(msg.clone()));
            t.attempt = Some(TransmissionState::Unsent);
            t
        }
        Error::UnsupportedOperation(msg) => {
            let operation = ctx.operation;
            let mut t = Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(msg.clone()));
            t
        }
        Error::Internal(msg) | Error::InternalMidExchange { message: msg, .. } => {
            internal(InternalErrorKind::InvariantViolated, msg.clone())
        }
        Error::LocalRuntime(msg) => internal(InternalErrorKind::RuntimeFailure, msg.clone()),
        Error::MissingCapability(cap) => {
            let operation = ctx.operation;
            let mut t = Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(cap.clone()));
            t
        }
        Error::AppendLimit { size, limit } => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!(
                    "message size {size} exceeds server APPENDLIMIT of {limit}"
                )),
            }),
        ),
        Error::FetchLimit {
            estimated, limit, ..
        } => {
            let mut t = Translation::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(format!(
                        "FETCH estimated size {estimated} exceeds caller budget {limit}"
                    )),
                }),
            );
            t.attempt = Some(TransmissionState::Acknowledged);
            t
        }
        Error::FetchLiteralLimit { estimated, limit } => {
            let mut t = Translation::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(format!(
                        "FETCH literal bytes {estimated} exceed caller budget {limit}"
                    )),
                }),
            );
            t.attempt = Some(TransmissionState::Acknowledged);
            t
        }
        Error::SearchResultTruncated { returned, omitted } => {
            let mut t = Translation::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(format!(
                        "UID SEARCH result exceeded the local expansion limit after {returned} IDs; \
                         omitted {omitted:?}"
                    )),
                }),
            );
            // The server answered the search. The account boundary must not
            // return a partial Page as a successful result.
            t.attempt = Some(TransmissionState::Acknowledged);
            t
        }
        Error::InvalidAppendDate(msg) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(msg.clone()),
            }),
        ),
    }
}

/// Map a ManageSieve rejection (RFC 5804 1.3) onto the shared taxonomy.
///
/// An unrecognized or absent code falls through to the same
/// `Server(Error { status: None })` this crate produced for every sieve
/// rejection before the codes were parsed - terminal `ProviderRefused`,
/// which is the right default for "the server refused and did not say
/// why". The point of the table is the codes where that default is
/// actively wrong.
fn classify_sieve(
    code: Option<&crate::account::sieve::SieveResponseCode>,
    message: &str,
    ctx: &ImapErrorContext,
) -> Translation {
    use crate::account::sieve::SieveResponseCode as Sc;

    let mut t = match code {
        // Explicitly transient: the server asked to be retried. Deriving
        // this terminal was the imap-S1 defect.
        Some(Sc::TryLater) => Translation::new(
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        ),
        Some(Sc::Quota) => {
            let mut t = Translation::new(
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
            );
            // Sieve quotas (script count, script size) are per-account.
            t.throttle_scope = Some(ThrottleScope::Account);
            t
        }
        Some(Sc::NonExistent) => Translation::new(
            AccountErrorKind::NotFound(ResourceKind::Filter),
            Cause::Request(RequestCause::NotFound {
                what: ResourceKind::Filter,
                id: None,
            }),
        ),
        Some(Sc::AlreadyExists) => Translation::new(
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        // Deleting or overwriting the active script. A precondition the
        // caller must satisfy (deactivate first), not a server fault.
        Some(Sc::Active) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(
                    "ManageSieve refused the operation on the ACTIVE script",
                ),
            }),
        ),
        // Mechanism / channel strength refusals during AUTHENTICATE.
        Some(Sc::AuthTooWeak | Sc::EncryptNeeded) => Translation::new(
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
            Cause::Access(AccessCause::PolicyBlocked),
        ),
        Some(Sc::TransitionNeeded | Sc::Sasl) => Translation::new(
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
        Some(Sc::Referral) => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        // `WARNINGS` and `TAG` are advisory and ride on `OK` too, so on a
        // rejection they say nothing about why. Unknown extension codes
        // likewise. Both take the status-level default.
        Some(Sc::Warnings | Sc::Tag | Sc::Other(_)) | None => Translation::new(
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Server(ServerCause::Error { status: None }),
        ),
    };

    if let Some(code) = code {
        t.native_code = Some(code.code().to_owned());
    }
    if !message.is_empty() {
        t.diagnostic_text = Some(DiagnosticText::support_only(message.to_owned()));
    }
    // A tagged ManageSieve response is server-acknowledged.
    t.attempt = Some(TransmissionState::Acknowledged);
    t
}

/// Translate a server AUTHENTICATE / LOGIN rejection.
///
/// `mechanism` names the rung the server refused. It rides into the
/// SUPPORT-ONLY diagnostic tier (`DetailVisibility::SupportOnly`), never the
/// telemetry tier: not because the token is sensitive - it crosses the wire in
/// clear on both the AUTHENTICATE line and the CAPABILITY advertisement - but
/// because the telemetry tier carries no free-form text at all
/// (`telemetry_has_no_free_form_text`), and a mechanism name is not a stable
/// discriminant the shared error model declares. It is deliberately not a new
/// typed field on `AccountError`: the failure information belongs in the
/// existing diagnostics-consent machinery, reachable through
/// `support_consented()`.
///
/// This closes the asymmetry with the LOCAL-policy path, where
/// `AuthPolicyFailure::Display` already names mechanisms. Before this, the
/// paths where a SERVER refused a mechanism were the only ones that could not
/// say which one.
fn classify_auth(
    text: &str,
    code: Option<&ResponseCode>,
    mechanism: Option<&'static str>,
) -> Translation {
    let (kind, cause) = match code {
        Some(ResponseCode::Expired) => (
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        ),
        Some(ResponseCode::AuthenticationFailed) | None => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
        Some(ResponseCode::AuthorizationFailed) => (
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied { resource: None }),
        ),
        Some(ResponseCode::PrivacyRequired) => (
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
            Cause::Access(AccessCause::PolicyBlocked),
        ),
        _ => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
    };
    let mut t = Translation::new(kind, cause);
    let detail = match (text.is_empty(), mechanism) {
        (false, Some(mechanism)) => Some(format!("{text} (mechanism {mechanism})")),
        (true, Some(mechanism)) => Some(format!("server refused mechanism {mechanism}")),
        (false, None) => Some(text.to_owned()),
        (true, None) => None,
    };
    if let Some(detail) = detail {
        t.diagnostic_text = Some(DiagnosticText::support_only(detail));
    }
    if let Some(code) = code {
        t.wire_code = Some(imap_response_code(code));
        t.native_code = Some(response_code_name(code).to_owned());
    }
    // Auth failures are server-acknowledged tagged responses.
    t.attempt = Some(TransmissionState::Acknowledged);
    t
}

enum StatusFallback {
    No,
    Bad,
    Bye,
}

fn classify_status(
    text: &str,
    code: Option<&ResponseCode>,
    fallback: StatusFallback,
    ctx: &ImapErrorContext,
) -> Translation {
    let mut t = if let Some(code) = code {
        classify_response_code(code, ctx).unwrap_or_else(|| fallback_status(text, &fallback))
    } else {
        fallback_status(text, &fallback)
    };
    if let Some(code) = code {
        t.wire_code = Some(imap_response_code(code));
        t.native_code = Some(response_code_name(code).to_owned());
        // For parameterized codes whose payload is interesting, attach
        // as support-only text if not already set.
        if t.diagnostic_text.is_none()
            && let Some(payload) = response_code_payload(code)
        {
            t.diagnostic_text = Some(payload);
        }
    }
    if !text.is_empty() && t.diagnostic_text.is_none() {
        t.diagnostic_text = Some(DiagnosticText::support_only(text.to_owned()));
    }
    t
}

fn fallback_status(_text: &str, fallback: &StatusFallback) -> Translation {
    match fallback {
        StatusFallback::No => Translation::new(
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Server(ServerCause::Error { status: None }),
        ),
        StatusFallback::Bad => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("server returned BAD with no response code"),
            }),
        ),
        StatusFallback::Bye => Translation::new(
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        ),
    }
}

fn classify_response_code(code: &ResponseCode, ctx: &ImapErrorContext) -> Option<Translation> {
    let resource_from_scope = || match ctx.scope.as_ref() {
        Some(ErrorScope::Mailbox { .. }) | Some(ErrorScope::Cursor(CursorScope::Folder(_))) => {
            Some(ResourceKind::Mailbox)
        }
        Some(ErrorScope::Message { .. }) => Some(ResourceKind::Message),
        Some(ErrorScope::Thread { .. }) => Some(ResourceKind::Thread),
        _ => None,
    };
    // All three scope readers must accept BOTH mailbox shapes. Folder
    // producers carry `ErrorScope::Cursor(Folder(id))` (see
    // `with_folder_scope`), not `ErrorScope::Mailbox { id }` - so a reader
    // matching only the latter silently degrades on every real
    // folder-scoped failure rather than on none of them.
    let id_from_scope = || match ctx.scope.as_ref() {
        Some(ErrorScope::Mailbox { id }) => Some(id.0.clone()),
        Some(ErrorScope::Cursor(CursorScope::Folder(folder))) => Some(folder.0.clone()),
        Some(ErrorScope::Message { id }) => Some(id.0.clone()),
        Some(ErrorScope::Thread { id }) => Some(id.0.clone()),
        _ => None,
    };
    let mailbox_throttle = || match ctx.scope.as_ref() {
        Some(ErrorScope::Mailbox { .. } | ErrorScope::Cursor(CursorScope::Folder(_))) => {
            ThrottleScope::Mailbox
        }
        _ => ThrottleScope::Account,
    };

    let t = match code {
        // Each arm produces an owned Translation. Some arms below mutate
        // it locally before yielding; the outer binding stays immutable.
        // Auth / policy
        ResponseCode::AuthenticationFailed => Translation::new(
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
        ResponseCode::Expired => Translation::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        ),
        ResponseCode::AuthorizationFailed | ResponseCode::NoPerm => Translation::new(
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(),
            }),
        ),
        ResponseCode::PrivacyRequired | ResponseCode::ContactAdmin => Translation::new(
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
            Cause::Access(AccessCause::PolicyBlocked),
        ),
        ResponseCode::MetadataNoPrivate => Translation::new(
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: Some(ResourceKind::Mailbox),
            }),
        ),

        // Transient server
        ResponseCode::Unavailable
        | ResponseCode::InUse
        | ResponseCode::Corruption
        | ResponseCode::TempFail(_) => Translation::new(
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        ),
        ResponseCode::ServerBug => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only("server returned [SERVERBUG]")),
            }),
        ),

        // Mailbox / cursor state
        ResponseCode::ExpungeIssued | ResponseCode::Closed => Translation::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        ResponseCode::UidNotSticky => Translation::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::ScopeCapabilityLost),
            Cause::State(StateCause::ScopeCapabilityLost),
        ),
        ResponseCode::NoModSeq => Translation::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure),
            Cause::State(StateCause::StrategyFailure {
                downgrade: StrategyDowngrade::CondstoreToBasic,
            }),
        ),
        ResponseCode::Modified(_)
        | ResponseCode::AlreadyExists
        | ResponseCode::HasChildren
        | ResponseCode::NoUpdate(_) => Translation::new(
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        ResponseCode::NonExistent => {
            let resource = resource_from_scope().unwrap_or(ResourceKind::Mailbox);
            Translation::new(
                AccountErrorKind::NotFound(resource),
                Cause::Request(RequestCause::NotFound {
                    what: resource,
                    id: id_from_scope(),
                }),
            )
        }
        ResponseCode::TryCreate => Translation::new(
            AccountErrorKind::NotFound(ResourceKind::Mailbox),
            Cause::Request(RequestCause::NotFound {
                what: ResourceKind::Mailbox,
                id: id_from_scope(),
            }),
        ),

        // Quota / limits
        ResponseCode::OverQuota => {
            let mut t = Translation::new(
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
            );
            t.throttle_scope = Some(ThrottleScope::Account);
            t
        }
        ResponseCode::Limit => {
            let mut t = Translation::new(
                AccountErrorKind::Server(ServerErrorKind::RateLimited),
                Cause::Server(ServerCause::RateLimited { retry_hint: None }),
            );
            t.throttle_scope = Some(mailbox_throttle());
            t
        }
        ResponseCode::MetadataTooMany => {
            let mut t = Translation::new(
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
            );
            t.throttle_scope = Some(ThrottleScope::Account);
            t
        }
        ResponseCode::TooBig
        | ResponseCode::MaxConvertMessages(_)
        | ResponseCode::MaxConvertParts(_)
        | ResponseCode::MetadataMaxSize(_) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(response_code_name(code).to_owned()),
            }),
        ),

        // Unsupported / unknown feature
        ResponseCode::BadCharset(_) => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::Cannot | ResponseCode::Annotate(_) | ResponseCode::Annotations(_) => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::BadComparator(_) => Translation::new(
            AccountErrorKind::Unsupported(AccountOperation::Search),
            Cause::Request(RequestCause::Unsupported {
                operation: AccountOperation::Search,
            }),
        ),
        ResponseCode::BadEvent(_) | ResponseCode::UndefinedFilter(_) => Translation::new(
            AccountErrorKind::Unsupported(AccountOperation::PushSubscribe),
            Cause::Request(RequestCause::Unsupported {
                operation: AccountOperation::PushSubscribe,
            }),
        ),
        ResponseCode::UnknownCte => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::CompressionActive => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(
                    "server returned [COMPRESSIONACTIVE]",
                )),
            }),
        ),
        ResponseCode::UseAttr => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }

        // Referral and URL
        ResponseCode::Referral(_) => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::NewName(_) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only("[NEWNAME]")),
            }),
        ),
        ResponseCode::UrlMech(_) => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::BadUrl(_) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("[BADURL]"),
            }),
        ),

        // Parse and bug
        ResponseCode::Parse => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only("[PARSE]")),
            }),
        ),
        ResponseCode::ClientBug => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("[CLIENTBUG]"),
            }),
        ),

        // Search / saved-result / notification
        ResponseCode::NotSaved => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("[NOTSAVED]"),
            }),
        ),
        ResponseCode::NotificationOverflow(_) => Translation::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),

        // Other / unknown
        ResponseCode::Other { name, value } => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Imap(ImapResponseCode::Unknown {
                code: name.clone(),
                value: value
                    .as_ref()
                    .map(|v| DiagnosticText::support_only(v.clone())),
            })),
        ),

        // Informational / state-carrying codes that don't change error semantics.
        // The caller will fall back to status classification.
        ResponseCode::Alert
        | ResponseCode::Capability(_)
        | ResponseCode::PermanentFlags(_)
        | ResponseCode::ReadOnly
        | ResponseCode::ReadWrite
        | ResponseCode::UidNext(_)
        | ResponseCode::UidValidity(_)
        | ResponseCode::Unseen(_)
        | ResponseCode::AppendUid { .. }
        | ResponseCode::CopyUid { .. }
        | ResponseCode::HighestModSeq(_)
        | ResponseCode::MailboxId(_)
        | ResponseCode::MetadataLongEntries(_) => return None,
    };

    Some(t)
}

/// Map crate `ResponseCode` -> `bifrost_types::ImapResponseCode`.
///
/// Parameterized codes lose their payload at the kind level; the payload
/// is preserved separately as `DiagnosticText`. The `Unknown` variant
/// carries the raw name and an optional value.
fn imap_response_code(code: &ResponseCode) -> ImapResponseCode {
    match code {
        ResponseCode::Alert => ImapResponseCode::Alert,
        ResponseCode::BadCharset(_) => ImapResponseCode::BadCharset,
        ResponseCode::Capability(_) => ImapResponseCode::Capability,
        ResponseCode::Parse => ImapResponseCode::Parse,
        ResponseCode::PermanentFlags(_) => ImapResponseCode::PermanentFlags,
        ResponseCode::ReadOnly => ImapResponseCode::ReadOnly,
        ResponseCode::ReadWrite => ImapResponseCode::ReadWrite,
        ResponseCode::TryCreate => ImapResponseCode::TryCreate,
        ResponseCode::UidNext(_) => ImapResponseCode::UidNext,
        ResponseCode::UidValidity(_) => ImapResponseCode::UidValidity,
        ResponseCode::Unseen(_) => ImapResponseCode::Unseen,
        ResponseCode::AppendUid { .. } => ImapResponseCode::AppendUid,
        ResponseCode::CopyUid { .. } => ImapResponseCode::CopyUid,
        ResponseCode::HighestModSeq(_) => ImapResponseCode::HighestModSeq,
        ResponseCode::Modified(_) => ImapResponseCode::Modified,
        ResponseCode::NoModSeq => ImapResponseCode::NoModSeq,
        ResponseCode::Closed => ImapResponseCode::Closed,
        ResponseCode::MailboxId(_) => ImapResponseCode::MailboxId,
        ResponseCode::Unavailable => ImapResponseCode::Unavailable,
        ResponseCode::AuthenticationFailed => ImapResponseCode::AuthenticationFailed,
        ResponseCode::AuthorizationFailed => ImapResponseCode::AuthorizationFailed,
        ResponseCode::Expired => ImapResponseCode::Expired,
        ResponseCode::PrivacyRequired => ImapResponseCode::PrivacyRequired,
        ResponseCode::ContactAdmin => ImapResponseCode::ContactAdmin,
        ResponseCode::NoPerm => ImapResponseCode::NoPerm,
        ResponseCode::InUse => ImapResponseCode::InUse,
        ResponseCode::ExpungeIssued => ImapResponseCode::ExpungeIssued,
        ResponseCode::Corruption => ImapResponseCode::Corruption,
        ResponseCode::ServerBug => ImapResponseCode::ServerBug,
        ResponseCode::ClientBug => ImapResponseCode::ClientBug,
        ResponseCode::Cannot => ImapResponseCode::Cannot,
        ResponseCode::Limit => ImapResponseCode::Limit,
        ResponseCode::OverQuota => ImapResponseCode::OverQuota,
        ResponseCode::AlreadyExists => ImapResponseCode::AlreadyExists,
        ResponseCode::NonExistent => ImapResponseCode::NonExistent,
        ResponseCode::NewName(_) => ImapResponseCode::NewName,
        ResponseCode::Referral(_) => ImapResponseCode::Referral,
        ResponseCode::UrlMech(_) => ImapResponseCode::UrlMech,
        ResponseCode::BadUrl(_) => ImapResponseCode::BadUrl,
        ResponseCode::BadComparator(_) => ImapResponseCode::BadComparator,
        ResponseCode::Annotate(_) => ImapResponseCode::Annotate,
        ResponseCode::Annotations(_) => ImapResponseCode::Annotations,
        ResponseCode::TempFail(_) => ImapResponseCode::TempFail,
        ResponseCode::MaxConvertMessages(_) => ImapResponseCode::MaxConvertMessages,
        ResponseCode::MaxConvertParts(_) => ImapResponseCode::MaxConvertParts,
        ResponseCode::NoUpdate(_) => ImapResponseCode::NoUpdate,
        ResponseCode::NotificationOverflow(_) => ImapResponseCode::NotificationOverflow,
        ResponseCode::BadEvent(_) => ImapResponseCode::BadEvent,
        ResponseCode::UndefinedFilter(_) => ImapResponseCode::UndefinedFilter,
        ResponseCode::UidNotSticky => ImapResponseCode::UidNotSticky,
        ResponseCode::NotSaved => ImapResponseCode::NotSaved,
        ResponseCode::HasChildren => ImapResponseCode::HasChildren,
        ResponseCode::UnknownCte => ImapResponseCode::UnknownCte,
        ResponseCode::TooBig => ImapResponseCode::TooBig,
        ResponseCode::CompressionActive => ImapResponseCode::CompressionActive,
        ResponseCode::UseAttr => ImapResponseCode::UseAttr,
        ResponseCode::MetadataLongEntries(_) => ImapResponseCode::MetadataLongEntries,
        ResponseCode::MetadataMaxSize(_) => ImapResponseCode::MetadataMaxSize,
        ResponseCode::MetadataTooMany => ImapResponseCode::MetadataTooMany,
        ResponseCode::MetadataNoPrivate => ImapResponseCode::MetadataNoPrivate,
        ResponseCode::Other { name, value } => ImapResponseCode::Unknown {
            code: name.clone(),
            value: value
                .as_ref()
                .map(|v| DiagnosticText::support_only(v.clone())),
        },
    }
}

fn response_code_name(code: &ResponseCode) -> &'static str {
    match code {
        ResponseCode::Alert => "ALERT",
        ResponseCode::BadCharset(_) => "BADCHARSET",
        ResponseCode::Capability(_) => "CAPABILITY",
        ResponseCode::Parse => "PARSE",
        ResponseCode::PermanentFlags(_) => "PERMANENTFLAGS",
        ResponseCode::ReadOnly => "READ-ONLY",
        ResponseCode::ReadWrite => "READ-WRITE",
        ResponseCode::TryCreate => "TRYCREATE",
        ResponseCode::UidNext(_) => "UIDNEXT",
        ResponseCode::UidValidity(_) => "UIDVALIDITY",
        ResponseCode::Unseen(_) => "UNSEEN",
        ResponseCode::AppendUid { .. } => "APPENDUID",
        ResponseCode::CopyUid { .. } => "COPYUID",
        ResponseCode::HighestModSeq(_) => "HIGHESTMODSEQ",
        ResponseCode::Modified(_) => "MODIFIED",
        ResponseCode::NoModSeq => "NOMODSEQ",
        ResponseCode::Closed => "CLOSED",
        ResponseCode::MailboxId(_) => "MAILBOXID",
        ResponseCode::Unavailable => "UNAVAILABLE",
        ResponseCode::AuthenticationFailed => "AUTHENTICATIONFAILED",
        ResponseCode::AuthorizationFailed => "AUTHORIZATIONFAILED",
        ResponseCode::Expired => "EXPIRED",
        ResponseCode::PrivacyRequired => "PRIVACYREQUIRED",
        ResponseCode::ContactAdmin => "CONTACTADMIN",
        ResponseCode::NoPerm => "NOPERM",
        ResponseCode::InUse => "INUSE",
        ResponseCode::ExpungeIssued => "EXPUNGEISSUED",
        ResponseCode::Corruption => "CORRUPTION",
        ResponseCode::ServerBug => "SERVERBUG",
        ResponseCode::ClientBug => "CLIENTBUG",
        ResponseCode::Cannot => "CANNOT",
        ResponseCode::Limit => "LIMIT",
        ResponseCode::OverQuota => "OVERQUOTA",
        ResponseCode::AlreadyExists => "ALREADYEXISTS",
        ResponseCode::NonExistent => "NONEXISTENT",
        ResponseCode::NewName(_) => "NEWNAME",
        ResponseCode::Referral(_) => "REFERRAL",
        ResponseCode::UrlMech(_) => "URLMECH",
        ResponseCode::BadUrl(_) => "BADURL",
        ResponseCode::BadComparator(_) => "BADCOMPARATOR",
        ResponseCode::Annotate(_) => "ANNOTATE",
        ResponseCode::Annotations(_) => "ANNOTATIONS",
        ResponseCode::TempFail(_) => "TEMPFAIL",
        ResponseCode::MaxConvertMessages(_) => "MAXCONVERTMESSAGES",
        ResponseCode::MaxConvertParts(_) => "MAXCONVERTPARTS",
        ResponseCode::NoUpdate(_) => "NOUPDATE",
        ResponseCode::NotificationOverflow(_) => "NOTIFICATIONOVERFLOW",
        ResponseCode::BadEvent(_) => "BADEVENT",
        ResponseCode::UndefinedFilter(_) => "UNDEFINED-FILTER",
        ResponseCode::UidNotSticky => "UIDNOTSTICKY",
        ResponseCode::NotSaved => "NOTSAVED",
        ResponseCode::HasChildren => "HASCHILDREN",
        ResponseCode::UnknownCte => "UNKNOWN-CTE",
        ResponseCode::TooBig => "TOOBIG",
        ResponseCode::CompressionActive => "COMPRESSIONACTIVE",
        ResponseCode::UseAttr => "USEATTR",
        ResponseCode::MetadataLongEntries(_) => "METADATA LONGENTRIES",
        ResponseCode::MetadataMaxSize(_) => "METADATA MAXSIZE",
        ResponseCode::MetadataTooMany => "METADATA TOOMANY",
        ResponseCode::MetadataNoPrivate => "METADATA NOPRIVATE",
        ResponseCode::Other { .. } => "OTHER",
    }
}

/// Render parameterized response-code payloads as support-only diagnostic
/// text. Returns `None` when there is no useful payload.
fn response_code_payload(code: &ResponseCode) -> Option<DiagnosticText> {
    let value = match code {
        ResponseCode::BadCharset(items) if !items.is_empty() => {
            format!("BADCHARSET {}", items.join(" "))
        }
        ResponseCode::Modified(ranges) if !ranges.is_empty() => {
            format!("MODIFIED ({} ranges)", ranges.len())
        }
        ResponseCode::TempFail(Some(text))
        | ResponseCode::NoUpdate(Some(text))
        | ResponseCode::Referral(Some(text))
        | ResponseCode::UrlMech(Some(text))
        | ResponseCode::BadUrl(Some(text))
        | ResponseCode::BadComparator(Some(text))
        | ResponseCode::Annotate(Some(text))
        | ResponseCode::Annotations(Some(text))
        | ResponseCode::MaxConvertMessages(Some(text))
        | ResponseCode::MaxConvertParts(Some(text))
        | ResponseCode::NotificationOverflow(Some(text))
        | ResponseCode::BadEvent(Some(text))
        | ResponseCode::UndefinedFilter(Some(text))
        | ResponseCode::NewName(Some(text)) => text.clone(),
        ResponseCode::MetadataMaxSize(n) => format!("METADATA MAXSIZE {n}"),
        ResponseCode::MetadataLongEntries(n) => format!("METADATA LONGENTRIES {n}"),
        _ => return None,
    };
    Some(DiagnosticText::support_only(value))
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;

#[cfg(test)]
mod unsupported_constructor_tests {
    use super::*;

    #[test]
    fn unsupported_constructor_stamps_protocol_and_kind() {
        let operation = bifrost_types::AccountOperation::Send;
        let protocol = bifrost_types::Protocol::Imap;
        let error = unsupported(operation);
        assert_eq!(error.protocol(), Some(protocol));
        assert_eq!(error.operation(), Some(operation));
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(operation)
        );
    }
}
