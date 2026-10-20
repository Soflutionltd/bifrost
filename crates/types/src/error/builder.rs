use std::fmt;

use super::account_error::{AccountError, AccountErrorParts};
use super::cause::{Cause, CauseChain};
use super::diagnostic::{DiagnosticInfo, DiagnosticText, TelemetryToken};
use super::kind::{AccountErrorKind, ServerErrorKind, SyncStateErrorKind};
use super::message_key;
use super::recovery::{self, ThrottleBudget, ThrottleScope};
use super::scope::{AccountOperation, ErrorScope, Protocol, Provider};

#[derive(Clone, Copy)]
enum TelemetryField {
    RequestId,
    TraceId,
    NativeCode,
}

impl TelemetryField {
    fn name(self) -> &'static str {
        match self {
            Self::RequestId => "request id",
            Self::TraceId => "trace id",
            Self::NativeCode => "native code",
        }
    }
}

/// Producer-bug invariants that `AccountErrorBuilder::try_build`
/// detects at construction time. These never escape into
/// `Result<T, AccountError>`; protocol-crate translation boundaries
/// call `.expect("valid account error classification")` because an
/// invalid combination is a library bug, not recoverable runtime
/// state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccountErrorBuildError {
    /// The chain was empty. Every `AccountError` must carry at least
    /// one `Cause` (the primary). Unreachable through the current
    /// builder - `new` demands a primary and `try_build` always pushes
    /// it first - and kept DELIBERATELY anyway: the invariant belongs
    /// to the error, not to the one construction path that currently
    /// guarantees it, and a future builder entry point must fail here
    /// rather than freeze a causeless error.
    EmptyChain,
    /// The outermost `Cause` does not match the declared
    /// `AccountErrorKind`. Producers must classify both halves
    /// consistently.
    KindCauseMismatch {
        kind: AccountErrorKind,
        primary_cause: Cause,
    },
    /// `Transport(_)` kind combined with an `Attempt(Acknowledged)`
    /// classification cause. Transport failures by definition mean no complete
    /// server response was received; an acknowledged transmission
    /// would belong on a `Server(_)` or `Protocol(_)` kind.
    TransportAcknowledged,
    /// `SyncState(CursorInvalid)` without an `ErrorScope::Cursor`.
    /// Producers must thread the cursor scope from the call site;
    /// the engine cannot route a scope-less cursor restart.
    CursorInvalidWithoutScope,
    /// A `throttle_scope` attached to a kind that is not
    /// `Server(RateLimited | QuotaExhausted)`. The kind and cause may
    /// match each other perfectly; the fault is the throttle scope
    /// alone, so this points the producer at it instead of
    /// misdiagnosing a kind/cause mismatch.
    ThrottleScopeNotApplicable {
        kind: AccountErrorKind,
        throttle_scope: ThrottleScope,
    },
    /// A `throttle_budget` stamped on a kind that is not
    /// `Server(RateLimited | QuotaExhausted)`. Same fault shape as
    /// `ThrottleScopeNotApplicable`: a budget names where a throttle hold
    /// is recorded, so on anything but a throttle it is a producer bug.
    ThrottleBudgetNotApplicable { kind: AccountErrorKind },
}

impl fmt::Display for AccountErrorBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyChain => write!(f, "AccountError chain is empty"),
            Self::KindCauseMismatch {
                kind,
                primary_cause,
            } => write!(
                f,
                "AccountErrorKind {kind:?} does not match outermost cause {primary_cause:?}",
            ),
            Self::TransportAcknowledged => write!(
                f,
                "transport failures cannot have acknowledged transmission state",
            ),
            Self::CursorInvalidWithoutScope => {
                write!(f, "SyncState(CursorInvalid) requires an ErrorScope::Cursor",)
            }
            Self::ThrottleScopeNotApplicable {
                kind,
                throttle_scope,
            } => write!(
                f,
                "throttle_scope {throttle_scope:?} is only meaningful on \
                 Server(RateLimited | QuotaExhausted), not {kind:?}",
            ),
            Self::ThrottleBudgetNotApplicable { kind } => write!(
                f,
                "throttle_budget is only meaningful on \
                 Server(RateLimited | QuotaExhausted), not {kind:?}",
            ),
        }
    }
}

impl std::error::Error for AccountErrorBuildError {}

pub(crate) struct RebuildParts {
    pub kind: AccountErrorKind,
    pub primary_cause: Cause,
    pub chain_extras: Vec<Cause>,
    pub classification_cause_count: usize,
    pub scope: Option<ErrorScope>,
    pub operation: Option<AccountOperation>,
    pub provider: Option<Provider>,
    pub protocol: Option<Protocol>,
    pub diagnostics: DiagnosticInfo,
    pub idempotency_override: Option<bool>,
    pub throttle_scope: Option<ThrottleScope>,
    pub throttle_budgets: Vec<ThrottleBudget>,
}

#[derive(Clone, Debug)]
pub struct AccountErrorBuilder {
    kind: AccountErrorKind,
    primary_cause: Cause,
    chain_extras: Vec<Cause>,
    classification_cause_count: Option<usize>,
    scope: Option<ErrorScope>,
    operation: Option<AccountOperation>,
    provider: Option<Provider>,
    protocol: Option<Protocol>,
    diagnostics: DiagnosticInfo,
    idempotency_override: Option<bool>,
    throttle_scope: Option<ThrottleScope>,
    throttle_budgets: Vec<ThrottleBudget>,
}

impl AccountErrorBuilder {
    #[must_use]
    pub fn new(kind: AccountErrorKind, primary_cause: Cause) -> Self {
        Self {
            kind,
            primary_cause,
            chain_extras: Vec::new(),
            classification_cause_count: None,
            scope: None,
            operation: None,
            provider: None,
            protocol: None,
            diagnostics: DiagnosticInfo::default(),
            idempotency_override: None,
            throttle_scope: None,
            throttle_budgets: Vec::new(),
        }
    }

    /// Constructor used by `AccountError::into_builder`. Pre-populates
    /// the chain (split into primary + extras), diagnostics, and the
    /// top-level fields from an existing built error so the caller can
    /// `push_cause`, then `try_build()` to obtain a new `AccountError`
    /// with derived fields recomputed.
    ///
    /// Builder overrides and the cause chain are preserved so
    /// decoration cannot alter recovery classification.
    #[must_use]
    pub(crate) fn from_rebuild(parts: RebuildParts) -> Self {
        Self {
            kind: parts.kind,
            primary_cause: parts.primary_cause,
            chain_extras: parts.chain_extras,
            classification_cause_count: Some(parts.classification_cause_count),
            scope: parts.scope,
            operation: parts.operation,
            provider: parts.provider,
            protocol: parts.protocol,
            diagnostics: parts.diagnostics,
            idempotency_override: parts.idempotency_override,
            throttle_scope: parts.throttle_scope,
            throttle_budgets: parts.throttle_budgets,
        }
    }

    #[must_use]
    pub fn operation(mut self, op: AccountOperation) -> Self {
        self.operation = Some(op);
        self
    }

    #[must_use]
    pub fn scope(mut self, scope: ErrorScope) -> Self {
        self.scope = Some(scope);
        self
    }

    #[must_use]
    pub fn provider(mut self, provider: Provider) -> Self {
        self.provider = Some(provider);
        self
    }

    #[must_use]
    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = Some(protocol);
        self
    }

    /// Add evidence. On a fresh builder it participates in classification;
    /// after `AccountError::into_builder` it is diagnostic-only. To classify
    /// a combined outcome, start a fresh builder and state its own evidence.
    #[must_use]
    pub fn push_cause(mut self, cause: Cause) -> Self {
        self.chain_extras.push(cause);
        self
    }

    #[must_use]
    pub fn request_id(mut self, id: impl Into<String>) -> Self {
        self.set_telemetry_token(id.into(), TelemetryField::RequestId);
        self
    }

    #[must_use]
    pub fn trace_id(mut self, id: impl Into<String>) -> Self {
        self.set_telemetry_token(id.into(), TelemetryField::TraceId);
        self
    }

    /// Server-returned numeric status. `None` is permitted and represents
    /// "server responded with an error this protocol does not carry a
    /// numeric status for" (IMAP `NO`/`BAD` without a response code).
    /// Builders must not synthesize sentinel values like `0`; pass `None`
    /// instead.
    #[must_use]
    pub fn status(mut self, status: Option<u16>) -> Self {
        self.diagnostics.status = status;
        self
    }

    #[must_use]
    pub fn native_code(mut self, code: impl Into<String>) -> Self {
        self.set_telemetry_token(code.into(), TelemetryField::NativeCode);
        self
    }

    /// A setter call always REPLACES the selected field, whether or not the
    /// new value validates. Keeping a previously-installed token when the
    /// replacement is rejected would attribute the error to the wrong request
    /// or trace, and telemetry has no way to tell that happened - a stale id
    /// is worse than an absent one. This is reachable in practice because
    /// `AccountError::into_builder` hands back a builder whose telemetry
    /// fields are already populated; from an empty builder the rejected case
    /// simply writes `None` over `None`.
    fn set_telemetry_token(&mut self, value: String, field: TelemetryField) {
        let token = TelemetryToken::new(value.as_str());
        let rejected = token.is_none();
        match field {
            TelemetryField::RequestId => self.diagnostics.request_id = token,
            TelemetryField::TraceId => self.diagnostics.trace_id = token,
            TelemetryField::NativeCode => self.diagnostics.native_code = token,
        }
        if rejected {
            self.diagnostics
                .text
                .push(DiagnosticText::support_only(format!(
                    "invalid {} omitted from telemetry: {value}",
                    field.name()
                )));
        }
    }

    #[must_use]
    pub fn text(mut self, text: DiagnosticText) -> Self {
        self.diagnostics.text.push(text);
        self
    }

    /// Override the default idempotency for this operation. Reserved
    /// for exceptional cases where a normally non-idempotent op is
    /// made safe by a request-side guard (e.g. an `If-Match` etag) or
    /// vice versa. `AccountOperation::is_idempotent()` is the
    /// authoritative source for the common case; callers that find
    /// themselves reaching for this method should first check whether
    /// their operation is mis-classified.
    #[must_use]
    pub fn idempotency_override(mut self, idempotent: bool) -> Self {
        self.idempotency_override = Some(idempotent);
        self
    }

    /// Provider-documented throttle scope for rate-limit / quota
    /// failures. Maps onto sharable [`ThrottleKey`](super::recovery::ThrottleKey)
    /// entries the engine consults across work items. Use
    /// `ThrottleScope::CurrentOperation` for per-call hints that
    /// should not enter a shared bucket.
    #[must_use]
    pub fn throttle_scope(mut self, scope: ThrottleScope) -> Self {
        self.throttle_scope = Some(scope);
        self
    }

    /// Name a provider rate budget the throttled request was held against.
    /// Repeatable. When an error stamps any budget the engine records the
    /// deadline on exactly the stamped budgets and does NOT lift the
    /// `ThrottleScope` into a key as well - recording both would put the
    /// hold back on the account key and pause every sibling mailbox the
    /// budget exists to keep running.
    ///
    /// A budget must name what the response says to hold, not every budget
    /// the request consumed. The producer that stamps it must also declare it
    /// from `Account::throttle_keys` for the same work, or the hold is
    /// recorded where no wait site looks. Like `throttle_scope`, it is only
    /// meaningful on `Server(RateLimited | QuotaExhausted)`.
    #[must_use]
    pub fn throttle_budget(mut self, budget: ThrottleBudget) -> Self {
        if !self.throttle_budgets.contains(&budget) {
            self.throttle_budgets.push(budget);
        }
        self
    }

    /// Construct the `AccountError`. Returns `Err` only on producer-bug
    /// invariants (see [`AccountErrorBuildError`]); protocol-crate
    /// translation boundaries call `.expect("valid account error
    /// classification")` because an invalid combination is a library
    /// bug, not recoverable runtime state.
    pub fn try_build(self) -> Result<AccountError, AccountErrorBuildError> {
        let throttled = matches!(
            &self.kind,
            AccountErrorKind::Server(
                ServerErrorKind::RateLimited | ServerErrorKind::QuotaExhausted
            )
        );
        if !throttled && !self.throttle_budgets.is_empty() {
            return Err(AccountErrorBuildError::ThrottleBudgetNotApplicable { kind: self.kind });
        }
        if let Some(throttle_scope) = self.throttle_scope
            && !throttled
        {
            // throttle_scope is meaningful only on rate-limit/quota
            // failures; producers attaching it elsewhere is a bug.
            // Caught here, and named for what it is: the kind and cause
            // may match perfectly, so a KindCauseMismatch here would
            // send the producer hunting the wrong invariant.
            return Err(AccountErrorBuildError::ThrottleScopeNotApplicable {
                kind: self.kind,
                throttle_scope,
            });
        }

        if !recovery::kind_matches_cause(&self.kind, &self.primary_cause) {
            return Err(AccountErrorBuildError::KindCauseMismatch {
                kind: self.kind,
                primary_cause: self.primary_cause,
            });
        }

        // CursorInvalid without an ErrorScope::Cursor cannot be routed
        // by the engine. The convergence rewrite removed the silent
        // fallback to `RestartAccount`; producers must thread the
        // cursor scope from the call site.
        if matches!(
            self.kind,
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ) && recovery::cursor_scope(self.scope.as_ref()).is_none()
        {
            return Err(AccountErrorBuildError::CursorInvalidWithoutScope);
        }

        let mut causes = Vec::with_capacity(1 + self.chain_extras.len());
        causes.push(self.primary_cause);
        causes.extend(self.chain_extras);
        let chain = CauseChain::try_new(causes).ok_or(AccountErrorBuildError::EmptyChain)?;

        // A fresh producer owns all supplied evidence. Decoration retains that
        // prefix across every rebuild: sibling failures and historical attempts
        // must stay visible without becoming evidence about this failure.
        let classification_cause_count = self
            .classification_cause_count
            .unwrap_or_else(|| chain.iter().count());
        let classification_chain = CauseChain::try_new(
            chain
                .iter()
                .take(classification_cause_count)
                .cloned()
                .collect(),
        )
        .ok_or(AccountErrorBuildError::EmptyChain)?;

        // Transport(_) cannot pair with an Acknowledged Attempt. The
        // wire-level evidence contradicts the kind.
        if matches!(self.kind, AccountErrorKind::Transport(_))
            && classification_chain.iter().any(|cause| {
                matches!(
                    cause,
                    Cause::Attempt(attempt)
                    if attempt.transmission_state
                        == super::cause::TransmissionState::Acknowledged
                )
            })
        {
            return Err(AccountErrorBuildError::TransportAcknowledged);
        }

        let recovery = recovery::derive(
            &self.kind,
            self.scope.as_ref(),
            self.operation,
            &classification_chain,
            self.throttle_scope,
            self.idempotency_override,
        );
        let remediation = recovery::suggest(
            &self.kind,
            &recovery,
            self.scope.as_ref(),
            &classification_chain,
        );
        let message_key = message_key::derive(&self.kind);

        Ok(AccountError::from_parts(AccountErrorParts {
            kind: self.kind,
            recovery,
            remediation,
            scope: self.scope,
            operation: self.operation,
            provider: self.provider,
            protocol: self.protocol,
            diagnostics: self.diagnostics,
            chain,
            classification_cause_count,
            message_key,
            idempotency_override: self.idempotency_override,
            throttle_scope: self.throttle_scope,
            throttle_budgets: self.throttle_budgets,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{
        AttemptCause, Cause, RequestCause, RequestErrorKind, TransmissionState, TransportCause,
        TransportErrorKind, TransportKind,
    };

    #[test]
    fn decoration_does_not_adopt_sibling_transmission_evidence() {
        let original = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .idempotency_override(false)
        .try_build()
        .expect("valid transport failure");
        for state in [TransmissionState::InFlight, TransmissionState::Acknowledged] {
            let decorated = original
                .clone()
                .into_builder()
                .push_cause(Cause::Attempt(AttemptCause::new(state)))
                .try_build()
                .expect("sibling evidence cannot contradict the primary attempt");
            assert_eq!(decorated.recovery(), original.recovery());
            assert_eq!(decorated.chain().iter().count(), 2);
            // A second rebuild must not promote the previous diagnostic suffix.
            let rebuilt = decorated
                .into_builder()
                .try_build()
                .expect("valid decoration");
            assert_eq!(rebuilt.recovery(), original.recovery());
            assert_eq!(rebuilt.chain().iter().count(), 2);
        }
    }

    #[test]
    fn decoration_does_not_adopt_sibling_retry_hints_or_smtp_identity() {
        use crate::error::{EnhancedStatusCode, RetryHint, ServerCause, WireCause};

        for (kind, primary, extra) in [
            (
                ServerErrorKind::RateLimited,
                ServerCause::RateLimited { retry_hint: None },
                Cause::Server(ServerCause::RateLimited {
                    retry_hint: Some(RetryHint::After(std::time::Duration::from_secs(37))),
                }),
            ),
            (
                ServerErrorKind::Error { status: Some(554) },
                ServerCause::Error { status: Some(554) },
                Cause::Wire(WireCause::Smtp(EnhancedStatusCode::new(554, None, None))),
            ),
        ] {
            let original =
                AccountErrorBuilder::new(AccountErrorKind::Server(kind), Cause::Server(primary))
                    .try_build()
                    .expect("valid server failure");
            let decorated = original
                .clone()
                .into_builder()
                .push_cause(extra)
                .try_build()
                .expect("valid decorated server failure");
            assert_eq!(decorated.recovery(), original.recovery());
            assert_eq!(
                decorated.suggested_remediation(),
                original.suggested_remediation()
            );
            assert_eq!(decorated.chain().iter().count(), 2);
        }
    }

    #[test]
    fn build_derives_message_key_and_recovery() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("bad"),
            }),
        )
        .try_build()
        .expect("valid account error classification");

        assert_eq!(err.message_key(), "request.malformed");
        assert!(err.recovery().is_terminal());
    }

    #[test]
    fn rejected_telemetry_replacement_clears_the_previous_token() {
        // Starting from an EMPTY builder cannot detect this: the bug is that a
        // rejected replacement used to leave the earlier valid token in place,
        // so telemetry attributed the rebuilt error to the wrong request.
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("bad"),
            }),
        )
        .request_id("req-1")
        .trace_id("trace-1")
        .native_code("code-1")
        .try_build()
        .expect("valid account error classification");
        let telemetry = err.telemetry_fields();
        assert_eq!(telemetry.request_id, Some("req-1"));
        assert_eq!(telemetry.trace_id, Some("trace-1"));
        assert_eq!(telemetry.native_code, Some("code-1"));

        let replaced = err
            .into_builder()
            .request_id("req 2 with spaces")
            .trace_id("trace\n2")
            .native_code(String::new())
            .try_build()
            .expect("valid account error classification");
        let telemetry = replaced.telemetry_fields();
        assert_eq!(
            telemetry.request_id, None,
            "a rejected request id must not leave the previous one attributed"
        );
        assert_eq!(telemetry.trace_id, None);
        assert_eq!(telemetry.native_code, None);
    }

    #[test]
    fn mismatched_kind_and_cause_rejected() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .try_build()
        .expect_err("kind and cause disagree");

        assert!(matches!(
            err,
            AccountErrorBuildError::KindCauseMismatch { .. }
        ));
    }

    #[test]
    fn throttle_scope_rejected_outside_rate_or_quota() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .throttle_scope(ThrottleScope::Account)
        .try_build()
        .expect_err("throttle_scope on transport is invalid");

        // The dedicated variant, not KindCauseMismatch: the kind and
        // cause here agree with each other, and the diagnosis must
        // point at the throttle scope alone.
        assert!(matches!(
            err,
            AccountErrorBuildError::ThrottleScopeNotApplicable {
                throttle_scope: ThrottleScope::Account,
                ..
            }
        ));
    }

    #[test]
    fn throttle_budget_rejected_outside_rate_or_quota() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .throttle_budget(ThrottleBudget::new("b"))
        .try_build()
        .expect_err("throttle_budget on transport is invalid");
        assert!(matches!(
            err,
            AccountErrorBuildError::ThrottleBudgetNotApplicable { .. }
        ));
    }

    /// A stamped budget is a builder input like `throttle_scope`, and must
    /// survive decoration through `into_builder`: a producer that rebuilds an
    /// error (to add evidence or an idempotency override) would otherwise
    /// silently move its hold back onto the account key.
    #[test]
    fn throttle_budget_survives_into_builder() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(crate::error::ServerCause::RateLimited { retry_hint: None }),
        )
        .throttle_budget(ThrottleBudget::new("b"))
        .throttle_budget(ThrottleBudget::new("b"))
        .try_build()
        .expect("valid");
        let rebuilt = err
            .into_builder()
            .idempotency_override(true)
            .try_build()
            .expect("valid");
        assert_eq!(rebuilt.throttle_budgets(), &[ThrottleBudget::new("b")]);
    }

    #[test]
    fn transport_acknowledged_rejected() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .push_cause(Cause::Attempt(AttemptCause {
            transmission_state: TransmissionState::Acknowledged,
        }))
        .try_build()
        .expect_err("transport + acknowledged is invalid");

        assert_eq!(err, AccountErrorBuildError::TransportAcknowledged);
    }

    #[test]
    fn cursor_invalid_without_scope_rejected() {
        use super::super::cause::StateCause;
        let err = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        )
        .try_build()
        .expect_err("CursorInvalid requires a cursor scope");

        assert_eq!(err, AccountErrorBuildError::CursorInvalidWithoutScope);
    }
}
