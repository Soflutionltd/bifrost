//! IMAP -> `AccountError` translation boundary.
//!
//! This module owns IMAP account error classification and decoration.
//! Account-trait call sites translate crate-private `crate::Error` through
//! `into_account_error` with an `ImapErrorContext`, or use the domain
//! constructors below. Submission translation belongs to its sibling boundary;
//! shared classifications remain owned by `bifrost-types`.
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

    /// Unused today: folder producers use `with_folder_scope`; kept as the
    /// cursor-scope sibling for non-folder cursors.
    #[allow(dead_code)]
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

/// Preserve the shared OAuth classification for IMAP and ManageSieve.
pub(crate) fn oauth_token_error(error: bifrost_net::Error) -> Error {
    // Protocol currently has no ManageSieve variant. Keep the account's IMAP
    // identity consistently with the other ManageSieve translations until the
    // shared protocol taxonomy can represent that transport distinctly.
    // The operation is a placeholder: into_account_error restamps the
    // caller's operation, scope and provider when it surfaces this error.
    let classified =
        bifrost_net::token_source_account_error(error, Protocol::Imap, AccountOperation::Discover);
    Error::OAuthToken(crate::error::OAuthFailure(std::sync::Arc::new(classified)))
}

/// Translate an internal IMAP error into a public `AccountError`.
pub(crate) fn into_account_error(error: Error, ctx: ImapErrorContext) -> AccountError {
    let mut error = error;
    let mut operation_not_sent = false;
    let error = loop {
        error = match error {
            Error::MoveFallbackIncomplete { source, .. } => *source,
            Error::OperationNotSent(source) => {
                operation_not_sent = true;
                *source
            }
            error => break error,
        };
    };
    // A token endpoint outage must retry without asking the user to replace
    // valid credentials. It supplies no IMAP server-acknowledgement evidence.
    if let Error::OAuthToken(classified) = &error {
        let mut builder = classified
            .as_ref()
            .clone()
            .into_builder()
            .operation(ctx.operation);
        if let Some(scope) = ctx.scope {
            builder = builder.scope(scope);
        }
        if let Some(provider) = ctx.provider {
            builder = builder.provider(provider);
        }
        if let Some(idempotent) = ctx.idempotency_override {
            builder = builder.idempotency_override(idempotent);
        }
        for text in ctx.extra_text {
            builder = builder.text(text);
        }
        return builder
            .try_build()
            .expect("valid token error classification");
    }
    let attempt = error.attempt().or(ctx.transmission_state);
    let mut translation = classify(&error, &ctx);
    if operation_not_sent {
        translation.attempt = Some(TransmissionState::Unsent);
    }
    finish_translation(translation, attempt, ctx)
}

fn finish_translation(
    translation: Translation,
    attempt: Option<TransmissionState>,
    ctx: ImapErrorContext,
) -> AccountError {
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

/// Adapt the sibling protocol without putting its status vocabulary in IMAP errors.
pub(crate) fn sieve_account_error(
    error: crate::sieve::Error,
    ctx: ImapErrorContext,
) -> AccountError {
    use crate::sieve::Error as Se;
    let common = match error {
        Se::Sieve { code, message } => {
            return finish_translation(
                classify_sieve(code.as_ref(), &message, &ctx),
                ctx.transmission_state,
                ctx,
            );
        }
        Se::Io { source } => Error::Transport {
            kind: crate::error::TransportFailure::Io(crate::error::IoFailure(source)),
            attempt: None,
        },
        Se::Timeout => Error::Transport {
            kind: crate::error::TransportFailure::Timeout,
            attempt: None,
        },
        Se::StartTlsUnavailable => {
            Error::LocalRefusal(crate::error::LocalRefusal::StartTlsUnavailable)
        }
        Se::Internal(message) => Error::Internal {
            kind: crate::error::InternalFailure::Invariant(message),
            mid_exchange: None,
        },
        Se::Parse(message) => Error::Provider(crate::error::ProviderFailure::Parse(message)),
        Se::InvalidInput(message) => {
            Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(message.into()))
        }
        Se::AuthPolicy(failure) => {
            Error::LocalRefusal(crate::error::LocalRefusal::AuthPolicy(failure))
        }
        Se::Auth { message, mechanism } => Error::auth_with_mechanism(message, None, mechanism),
        // The shared protocol taxonomy has no ManageSieve identity; this
        // retains the same IMAP account identity as other sieve failures.
        Se::OAuthToken(error) => oauth_token_error(error),
        Se::Sasl(error) => Error::from(error),
    };
    into_account_error(common, ctx)
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
    expected: crate::types::UidValidity,
    actual: crate::types::UidValidity,
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
    previous: crate::types::ModSeq,
    current: Option<crate::types::ModSeq>,
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

/// The one folder-scoped classification every mailbox command uses:
/// `ImapAccount::folder_error` looks up the folder's registry owner and calls
/// this, and hermetic tests call it directly with the owner they need. A
/// permission denial on a shared folder becomes `SyncState(ScopeRevoked)`
/// scoped to that folder (deriving `Engine(DisableScope(Folder(id)))`), so a
/// withdrawn share disables only that folder; every other failure keeps its
/// normal mapping.
pub(crate) fn folder_error(
    error: Error,
    folder: &MailboxName,
    shared_owner: Option<&bifrost_types::PrincipalId>,
    ctx: ImapErrorContext,
) -> AccountError {
    // A narrower scope the caller already chose (one message, for a raw
    // fetch or a poll hydrate) survives: replacing it with the folder would
    // widen an item-local failure to the whole folder. Only an unscoped
    // context defaults to the folder; a revocation re-scopes below.
    let ctx = if ctx.scope.is_none() {
        ctx.with_folder_scope(folder)
    } else {
        ctx
    };
    shared_folder_error(error, folder, shared_owner, ctx)
}

pub(crate) fn permission_denied(error: &Error, ctx: &ImapErrorContext) -> bool {
    // Token errors carry their own classification and must not reach
    // `classify`, which refuses them; unwrap the MOVE fallback wrapper too.
    let mut inner = error;
    while let Error::MoveFallbackIncomplete { source, .. } | Error::OperationNotSent(source) = inner
    {
        inner = source;
    }
    let kind = match inner {
        Error::OAuthToken(classified) => classified.kind().clone(),
        other => classify(other, ctx).kind,
    };
    matches!(
        kind,
        AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
    )
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
    shared_owner: Option<&bifrost_types::PrincipalId>,
    ctx: ImapErrorContext,
) -> AccountError {
    let mut ctx = ctx;
    if let Some(owner) = shared_owner {
        ctx.extra_text.push(DiagnosticText::support_only(format!(
            "shared folder {} (owner {})",
            folder.as_str(),
            owner.0,
        )));
    }
    // The target folder is separate from the original error's scope: only
    // revocation widens an item-local denial to quarantine its shared folder.
    into_account_error(error, ctx).revoke_foreign_scope(super::folder_scope(folder), shared_owner)
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
        Error::OAuthToken(_) => unreachable!("token errors bypass IMAP classification"),
        Error::MoveFallbackIncomplete { source, .. } => classify(source, ctx),
        Error::OperationNotSent(source) => {
            let mut translation = classify(source, ctx);
            translation.attempt = Some(TransmissionState::Unsent);
            translation
        }
        Error::Transport {
            kind: crate::error::TransportFailure::Io(crate::error::IoFailure(source)),
            ..
        } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only(source.to_string())),
            )),
        ),
        Error::Transport {
            kind: crate::error::TransportFailure::Timeout,
            ..
        } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Timeout),
            Cause::Transport(TransportCause::new(TransportKind::Timeout, None)),
        ),
        Error::Transport {
            kind: crate::error::TransportFailure::Closed,
            ..
        } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only("IMAP connection closed")),
            )),
        ),
        Error::Transport {
            kind: crate::error::TransportFailure::DriverGone,
            ..
        } => Translation::new(
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
        Error::Internal {
            kind: crate::error::InternalFailure::Panicked(message),
            ..
        } => internal(
            InternalErrorKind::Panicked,
            format!("IMAP driver panicked: {message}"),
        ),
        Error::ServerStatus(crate::error::ServerStatus::Auth {
            text,
            code,
            mechanism,
            scram,
        }) => {
            let mut translation = classify_auth(text, code.as_ref(), *mechanism);
            if let Some(refusal) = scram {
                use bifrost_sasl::RefusalClass;
                let (kind, cause) = match refusal.refusal_class() {
                    RefusalClass::Credential => (
                        AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
                        Cause::Auth(AuthCause::ReauthorizationRequired),
                    ),
                    RefusalClass::Policy => (
                        AccountErrorKind::Authorization(
                            bifrost_types::AccessErrorKind::PolicyBlocked,
                        ),
                        Cause::Access(AccessCause::PolicyBlocked),
                    ),
                    RefusalClass::Protocol => (
                        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                        Cause::Wire(WireCause::MalformedResponse {
                            protocol: Protocol::Imap,
                            detail: Some(DiagnosticText::support_only(text.to_owned())),
                        }),
                    ),
                    RefusalClass::Unavailable => (
                        AccountErrorKind::Server(ServerErrorKind::Unavailable),
                        Cause::Server(ServerCause::Unavailable { retry_hint: None }),
                    ),
                    _ => unreachable!("unmapped SCRAM refusal class"),
                };
                translation.kind = kind;
                translation.primary_cause = cause;
            }
            translation
        }
        Error::LocalRefusal(crate::error::LocalRefusal::AuthPolicy(failure)) => {
            let mut t = Translation::new(
                AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
                Cause::Access(AccessCause::PolicyBlocked),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(failure.to_string()));
            t
        }
        Error::LocalRefusal(crate::error::LocalRefusal::StartTlsUnavailable) => {
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
        Error::ServerStatus(crate::error::ServerStatus::No { text, code, .. }) => {
            classify_status(text, code.as_ref(), StatusFallback::No, ctx)
        }
        Error::ServerStatus(crate::error::ServerStatus::Bad { text, code, .. }) => {
            classify_status(text, code.as_ref(), StatusFallback::Bad, ctx)
        }
        Error::Bye { text, code, .. } => {
            classify_status(text, code.as_ref(), StatusFallback::Bye, ctx)
        }
        Error::Provider(crate::error::ProviderFailure::Protocol(msg)) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(msg.clone())),
            }),
        ),
        // Same recovery class as `Protocol` (`ProviderContractViolation`);
        // the kind only records that the server omitted something mandatory.
        Error::Provider(crate::error::ProviderFailure::Missing(msg)) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(msg.clone())),
            }),
        ),
        Error::Provider(crate::error::ProviderFailure::Parse(msg)) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(msg.clone())),
            }),
        ),
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(msg)) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(msg.clone()),
            }),
        ),
        // Local refusals, all made before the first byte. None of them is a
        // provider fault (`reference/error-model.md`, "Local refusals").
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidState(msg)) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(msg.clone()),
            }),
        ),
        Error::LocalRefusal(crate::error::LocalRefusal::StateChangedBeforeSend(msg)) => {
            let mut t = Translation::new(
                AccountErrorKind::ConcurrencyConflict,
                Cause::State(bifrost_types::StateCause::ConcurrencyConflict),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(msg.clone()));
            t.attempt = Some(TransmissionState::Unsent);
            t
        }
        Error::LocalRefusal(crate::error::LocalRefusal::UnsupportedOperation(msg)) => {
            let operation = ctx.operation;
            let mut t = Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(msg.clone()));
            t
        }
        Error::Internal {
            kind: crate::error::InternalFailure::Invariant(msg),
            ..
        } => internal(InternalErrorKind::InvariantViolated, msg.clone()),
        Error::Internal {
            kind: crate::error::InternalFailure::Runtime(msg),
            ..
        } => internal(InternalErrorKind::RuntimeFailure, msg.clone()),
        Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(cap)) => {
            let operation = ctx.operation;
            let mut t = Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(cap.clone()));
            t
        }
        Error::LocalRefusal(crate::error::LocalRefusal::AppendLimit { size, limit }) => {
            Translation::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(format!(
                        "message size {size} exceeds server APPENDLIMIT of {limit}"
                    )),
                }),
            )
        }
        Error::ResultLimit(crate::error::ResultLimit::Fetch {
            estimated, limit, ..
        }) => {
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
        Error::ResultLimit(crate::error::ResultLimit::FetchLiteral { estimated, limit }) => {
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
        Error::ResultLimit(crate::error::ResultLimit::SearchTruncated { returned, omitted }) => {
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
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidAppendDate(msg)) => {
            Translation::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(msg.clone()),
                }),
            )
        }
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
    code: Option<&crate::sieve::SieveResponseCode>,
    message: &str,
    ctx: &ImapErrorContext,
) -> Translation {
    use crate::sieve::SieveResponseCode as Sc;

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
    mechanism: Option<crate::types::AuthMechanism>,
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
    let mechanism = mechanism.map(crate::types::AuthMechanism::name);
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
        let wire_code = ImapResponseCode::from(code);
        t.native_code = Some(wire_code.code().to_owned());
        t.wire_code = Some(wire_code);
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
        let wire_code = ImapResponseCode::from(code);
        t.native_code = Some(wire_code.code().to_owned());
        t.wire_code = Some(wire_code);
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
                detail: DiagnosticText::support_only(
                    ImapResponseCode::from(code).code().to_owned(),
                ),
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

pub(super) fn invalid_move_destination_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("bulk-move destination is not a sendable mailbox"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::BulkMove)
    .try_build()
    .expect("valid account error classification")
}

/// Build a `ConcurrencyConflict` `AccountError` for STORE UNCHANGEDSINCE
/// conflicts. Takes the caller's `AccountOperation` so flag, move, and
/// destroy paths each surface their own operation - the central recovery
/// mapping needs the correct op to pick `Retry::AfterStateRefresh` for
/// the right kind of work.
pub(super) fn concurrency_conflict_error(
    operation: AccountOperation,
    folder: &MailboxName,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::ConcurrencyConflict,
        Cause::State(StateCause::ConcurrencyConflict),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Cursor(
        bifrost_types::CursorScope::Folder(bifrost_types::FolderId(folder.account_id())),
    ))
    .try_build()
    .expect("valid account error classification")
}

/// Build a generic `Request(Malformed)` for UIDVALIDITY mismatch before mutation.
pub(super) fn uidvalidity_changed_error(
    operation: AccountOperation,
    folder: &MailboxName,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("UIDVALIDITY changed before mutation"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Cursor(
        bifrost_types::CursorScope::Folder(bifrost_types::FolderId(folder.account_id())),
    ))
    .try_build()
    .expect("valid account error classification")
}

/// Build a generic server refusal for a STORE command failure without a
/// specific per-item response code. Takes the caller's `AccountOperation`
/// so move / destroy / flag mutations each carry their own op.
pub(super) fn store_failed_error(
    operation: AccountOperation,
    folder: &MailboxName,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
        Cause::Server(ServerCause::Error { status: None }),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Cursor(
        bifrost_types::CursorScope::Folder(bifrost_types::FolderId(folder.account_id())),
    ))
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .text(DiagnosticText::support_only(
        "STORE command failed with no per-item response code",
    ))
    .try_build()
    .expect("valid account error classification")
}

pub(super) fn finish_partial(
    error: AccountError,
    record: bifrost_types::PartialCommitBuilder,
) -> AccountError {
    error
        .into_builder()
        .partial_commit(record)
        .try_build()
        .expect("valid IMAP partial commitment")
}

pub(super) fn hydration_uidvalidity_changed_error(folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("UIDVALIDITY changed before hydration"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::Hydrate)
    // Cursor(Folder(..)), not ErrorScope::Mailbox: the folder producers'
    // documented scope shape, so scope readers matching one shape do not
    // silently degrade (mutate.rs builds the identical condition this way).
    .scope(bifrost_types::ErrorScope::Cursor(
        bifrost_types::CursorScope::Folder(bifrost_types::FolderId(folder.account_id())),
    ))
    .try_build()
    .expect("valid account error classification")
}

pub(super) fn message_not_found_error(id: &super::envelope::DecodedObjectId) -> AccountError {
    fetched_message_not_found(&id.id, AccountOperation::Hydrate)
}

/// A folder restriction (`SearchFilter::In`) nested inside an `Or` has no
/// faithful single-mailbox IMAP SEARCH encoding.
pub(super) fn or_folder_restriction_error(op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("folder"),
            message: Some(DiagnosticText::support_only(
                "a folder restriction (In) inside an Or cannot be expressed as a \
                 single-mailbox IMAP SEARCH; restructure the query so folder scoping \
                 is not OR-ed with other criteria",
            )),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

/// The cursor's folder answered with a different UIDVALIDITY, so the UID
/// the walk resumes after no longer names anything there.
///
/// This stays `ConcurrencyConflict`, not the page_cursor `Malformed` fault the
/// decode and restriction cases use: the cursor was valid when issued and the
/// server replaced the mailbox under it. `Malformed` derives ClientBug, which
/// would report a server-side epoch change as an application defect; an
/// Outlook user whose mailbox was rebuilt expects the search to simply run
/// again, which a conflict's re-read recovery gives. A consumer restarts
/// paging on either error. How UIDVALIDITY changes are classified across the
/// rest of the IMAP account is a separate, still open, question.
pub(super) fn search_cursor_voided(op: AccountOperation, folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::ConcurrencyConflict,
        Cause::State(bifrost_types::StateCause::ConcurrencyConflict),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .scope(bifrost_types::ErrorScope::Cursor(
        bifrost_types::CursorScope::Folder(bifrost_types::FolderId(folder.account_id())),
    ))
    .text(DiagnosticText::support_only(
        "UIDVALIDITY of the folder this search page cursor resumes in changed; repeat the search",
    ))
    .try_build()
    .expect("valid account error classification")
}

/// The message a completed `UID FETCH` did not answer for.
///
/// `hydrate_decoded` only reaches the FETCH once `valid_uids` has confirmed the
/// id's UIDVALIDITY matches the selected mailbox, so the UID is a live name in
/// the current epoch and the id was well-formed. A tagged OK with no FETCH for
/// that UID is how RFC 3501 6.4.8 / RFC 9051 6.4.9 report a UID that no longer
/// exists: the message was expunged between the caller learning the id and
/// this FETCH. That is `NotFound(Message)`, not the caller's malformed request.
/// The server answered, so the attempt is `Acknowledged`.
pub(super) fn expunged_message(id: bifrost_types::ObjectId) -> AccountError {
    fetched_message_not_found(&id, AccountOperation::HydrateMessage)
}

/// Build a `Request(Malformed)` `AccountError` for local PIM failures
/// where the caller provided invalid or internally inconsistent state.
///
/// The operation is threaded in from the caller. Helpers reachable from
/// more than one PIM surface (id decode, mailbox-name validation,
/// search-criteria assembly, page-cursor parsing) take the op as a
/// parameter rather than guessing, so a locally-malformed request is
/// attributable to the PIM call that produced it. The recovery class is
/// `ClientBug` for all of them; the operation is the telemetry.
pub(super) fn pim_malformed(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.into()),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

/// Re-stamp an `AccountError` from the submission boundary with the IMAP
/// operation that drove it, so recovery/telemetry attribute it correctly.
///
/// `into_builder` preserves idempotency and throttle overrides. Restamping
/// changes the operation without discarding the submission boundary's evidence.
pub(super) fn restamp(err: AccountError, op: AccountOperation) -> AccountError {
    err.into_builder()
        .operation(op)
        .try_build()
        .expect("valid account error classification")
}

/// Attribute a provider-neutral assembler error to IMAP. The shared
/// `bifrost-types` MIME assembler leaves the protocol unset (it is shared
/// across providers); the IMAP send path stamps `Protocol::Imap` so
/// telemetry and recovery attribute the failure to this account.
pub(super) fn stamp_imap_protocol(err: AccountError) -> AccountError {
    err.into_builder()
        .protocol(Protocol::Imap)
        .try_build()
        .expect("valid account error classification")
}

/// `NotFound(Message)` for a raw read whose UID the server no longer holds,
/// keyed by the id exactly as submitted.
pub(super) fn raw_message_not_found(id: &bifrost_types::ObjectId) -> AccountError {
    fetched_message_not_found(id, AccountOperation::OpenRawRfc822)
}

pub(super) fn folder_of(
    container: &bifrost_types::ContainerId,
    op: AccountOperation,
) -> Result<MailboxName, AccountError> {
    MailboxName::from_account_id(&container.0).map_err(|err| {
        AccountErrorBuilder::new(
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!("not an IMAP folder identity: {err}")),
            }),
        )
        .protocol(Protocol::Imap)
        .operation(op)
        .try_build()
        .expect("valid account error classification")
    })
}

/// `Unsupported`, scoped to the folder: a `UIDNOTSTICKY` folder's UIDs do
/// not persist across selections, so it cannot be tracked by UID.
pub(super) fn not_sticky(folder: &MailboxName, op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(RequestCause::Unsupported { operation: op }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .text(DiagnosticText::support_only(
        "folder is UIDNOTSTICKY: its UIDs do not persist across selections",
    ))
    .try_build()
    .expect("valid account error classification")
}

pub(super) fn excluded_from_operand_error(
    operation: AccountOperation,
    folder: &MailboxName,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(
                "message id could not be placed in an IMAP UID operand",
            ),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Cursor(super::folder_scope(
        folder,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Build a `Request(Malformed)` `AccountError` for IMAP object-id or
/// cursor decode failures. These are caller-side invalid inputs (the
/// object id was not produced by this crate or was corrupted in transit).
///
/// The operation is threaded in from the caller: `decode_object_id` and
/// `decode_thread_id` are always reached while servicing one specific
/// account-trait call (hydration, a mutation lane, a blob open, a PIM
/// primitive), so the op is known at every call site and the resulting
/// error carries it. The recovery class is `ClientBug` either way; the
/// operation is what makes the telemetry attributable to a lane.
pub(super) fn malformed(detail: &str, op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.to_owned()),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

/// Build a `SyncState(SchemaIncompatible)` `AccountError` for IMAP
/// cursor envelope decoding failures. The cursor bytes are structurally
/// unrecognizable, which triggers the `SchemaIncompatible` engine
/// directive to clear cursor state and re-establish from inventory.
pub(super) fn schema_incompatible(detail: &str) -> AccountError {
    bifrost_types::CursorDecodeFault::Malformed
        .builder(Protocol::Imap, AccountOperation::SyncChanges)
        .text(DiagnosticText::support_only(detail.to_owned()))
        .try_build()
        .expect("valid account error classification")
}

/// A raw read refused because the selected mailbox changed epoch.
pub(super) fn raw_uidvalidity_changed(folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("UIDVALIDITY changed before fetch"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::OpenRawRfc822)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .try_build()
    .expect("valid account error classification")
}

pub(super) fn search_folders_conflict(
    a: &MailboxName,
    b: &MailboxName,
    op: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("folder"),
            message: Some(DiagnosticText::support_only(format!(
                "search filter spans two folders ({} and {}); \
                 IMAP SEARCH cannot span multiple mailboxes in one command",
                a.as_str(),
                b.as_str()
            ))),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

pub(super) fn cursor_decode_fault(
    fault: bifrost_types::CursorDecodeFault,
    scope: &CursorScope,
) -> AccountError {
    fault
        .builder(Protocol::Imap, AccountOperation::SyncChanges)
        .scope(ErrorScope::Cursor(scope.clone()))
        .try_build()
        .expect("valid account error classification")
}

pub(super) fn cursor_scope_error(error: AccountError, scope: &CursorScope) -> AccountError {
    error
        .into_builder()
        .scope(ErrorScope::Cursor(scope.clone()))
        .try_build()
        .expect("valid account error classification")
}

/// A tagged successful FETCH with no matching UID proves absence and answers
/// the command. Enterprise users expect a vanished message reported as absent;
/// retain Acknowledged evidence consistently across hydration and raw reads.
fn fetched_message_not_found(id: &bifrost_types::ObjectId, op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::NotFound(ResourceKind::Message),
        Cause::Request(RequestCause::NotFound {
            what: ResourceKind::Message,
            id: Some(id.0.clone()),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .scope(ErrorScope::Message { id: id.clone() })
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// One rejected scope in the `push_subscribe` failed lane.
///
/// Both refusals are `Unsupported(PushSubscribe)`: neither is a whole-request
/// fault (`Err` stays reserved for that), neither is retryable as issued, and
/// a rejected scope must not fail its siblings. They are distinguished by the
/// diagnostic text, not the kind, because a capacity refusal and a
/// scope-shape refusal need different operator action even though they carry
/// the same recovery class - bifrost-sync keeps polling either way.
pub(super) fn rejected_scope_error(detail: &'static str) -> AccountError {
    AccountError::unsupported(
        AccountOperation::PushSubscribe,
        bifrost_types::Protocol::Imap,
    )
    .into_builder()
    .text(bifrost_types::DiagnosticText::user_safe(detail))
    .try_build()
    .expect("valid push scope error")
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

#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use bifrost_types::AccessErrorKind;

    pub(in crate::account) fn discovery_transport(op: AccountOperation) -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
            bifrost_types::Cause::Transport(bifrost_types::TransportCause::new(
                bifrost_types::TransportKind::Network,
                None,
            )),
        )
        .operation(op)
        .try_build()
        .expect("valid account error classification")
    }

    pub(in crate::account) fn factory_transport() -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
            bifrost_types::Cause::Transport(bifrost_types::TransportCause::new(
                bifrost_types::TransportKind::Network,
                None,
            )),
        )
        .operation(AccountOperation::Discover)
        .try_build()
        .expect("valid account error classification")
    }

    pub(in crate::account) fn factory_auth_lost() -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Authentication(
                bifrost_types::AuthErrorKind::ReauthorizationRequired,
            ),
            bifrost_types::Cause::Auth(bifrost_types::AuthCause::ReauthorizationRequired),
        )
        .operation(AccountOperation::Discover)
        .try_build()
        .expect("valid account error classification")
    }

    pub(in crate::account) fn smtp_unsupported() -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send),
            bifrost_types::Cause::Request(bifrost_types::RequestCause::Unsupported {
                operation: AccountOperation::Send,
            }),
        )
        .protocol(Protocol::Smtp)
        .operation(AccountOperation::Send)
        .try_build()
        .expect("valid account error classification")
    }

    pub(in crate::account) fn unavailable_without_context() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Server(bifrost_types::ServerErrorKind::Unavailable),
            Cause::Server(bifrost_types::ServerCause::Unavailable { retry_hint: None }),
        )
        .try_build()
        .unwrap()
    }

    pub(in crate::account) fn expired_auth() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("auth error")
    }

    pub(in crate::account) fn incompatible_schema() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("schema error")
    }

    pub(in crate::account) fn personal_permission_denied() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied { resource: None }),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("personal-folder permission error")
    }

    pub(in crate::account) fn unavailable_update() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Server(bifrost_types::ServerErrorKind::Unavailable),
            Cause::Server(bifrost_types::ServerCause::Unavailable { retry_hint: None }),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("server error")
    }

    pub(in crate::account) fn unavailable_update_at(state: TransmissionState) -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .push_cause(Cause::Attempt(AttemptCause::new(state)))
        .try_build()
        .expect("valid mutation error")
    }

    pub(in crate::account) fn retryable_move() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        )
        .operation(AccountOperation::BulkMove)
        .try_build()
        .unwrap()
    }
}

#[cfg(test)]
mod fetched_message_tests {
    use super::*;

    #[test]
    fn completed_fetch_misses_share_acknowledged_evidence() {
        let id = bifrost_types::ObjectId("imap1:5:INBOX:7:9".into());
        let decoded = super::super::decode_object_id(&id, AccountOperation::Hydrate).unwrap();
        for (error, operation) in [
            (message_not_found_error(&decoded), AccountOperation::Hydrate),
            (
                expunged_message(id.clone()),
                AccountOperation::HydrateMessage,
            ),
            (raw_message_not_found(&id), AccountOperation::OpenRawRfc822),
        ] {
            assert_eq!(
                error.kind(),
                &AccountErrorKind::NotFound(ResourceKind::Message)
            );
            assert_eq!(error.operation(), Some(operation));
            assert_eq!(error.scope(), Some(&ErrorScope::Message { id: id.clone() }));
            assert!(error.chain().iter().any(|cause| matches!(
                cause,
                Cause::Attempt(attempt) if attempt.transmission_state == TransmissionState::Acknowledged
            )));
        }
    }
}
