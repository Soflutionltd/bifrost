use super::partial::{PartialCommit, PartialCommitBuilder, StepId};
use std::fmt;
use std::sync::Arc;

use super::account_error::{AccountError, AccountErrorParts};
use super::cause::{Cause, CauseChain, PrimaryCause};
use super::diagnostic::{DiagnosticInfo, DiagnosticText, TelemetryToken};
use super::kind::{AccountErrorKind, UnsupportedReason};
use super::message_key;
use super::recovery;
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
    /// A record can be attached only once, including across rebuilds.
    PartialCommitAlreadyAttached,
    /// No historical effect, sibling, compensation or uncertain primary exists.
    PartialCommitEmpty,
    /// The declared application contradicts the effect or leaf evidence.
    PartialCommitApplication { step: StepId },
    /// A compensation has invalid references or contradictory evidence.
    PartialCommitCompensation { compensation: StepId },
    /// A sibling or compensation already carries an operation-level record.
    PartialCommitNested { step: StepId },
    /// The chain was empty. Every `AccountError` must carry at least
    /// one `Cause` (the primary). Unreachable through the current
    /// builder - `new` demands a primary and `try_build` always pushes
    /// it first - and kept DELIBERATELY anyway: the invariant belongs
    /// to the error, not to the one construction path that currently
    /// guarantees it, and a future builder entry point must fail here
    /// rather than freeze a causeless error.
    EmptyChain,
    /// A throttle refinement requires a rate-limit or quota primary cause.
    ThrottleNotApplicable { kind: AccountErrorKind },
    /// A local unsupported reason is meaningful only on Unsupported errors.
    UnsupportedReasonNotApplicable { kind: AccountErrorKind },
    /// `Transport(_)` kind combined with an `Attempt(Acknowledged)`
    /// classification cause. Transport failures by definition mean no complete
    /// server response was received; an acknowledged transmission
    /// would belong on a `Server(_)` or `Protocol(_)` kind.
    TransportAcknowledged,
}

impl fmt::Display for AccountErrorBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartialCommitAlreadyAttached
            | Self::PartialCommitEmpty
            | Self::PartialCommitApplication { .. }
            | Self::PartialCommitCompensation { .. }
            | Self::PartialCommitNested { .. } => write!(f, "invalid partial commitment: {self:?}"),
            Self::ThrottleNotApplicable { kind } => {
                write!(f, "throttle refinement is not applicable to {kind:?}")
            }
            Self::UnsupportedReasonNotApplicable { kind } => {
                write!(f, "unsupported reason is not applicable to {kind:?}")
            }
            Self::EmptyChain => write!(f, "AccountError chain is empty"),
            Self::TransportAcknowledged => write!(
                f,
                "transport failures cannot have acknowledged transmission state",
            ),
        }
    }
}

impl std::error::Error for AccountErrorBuildError {}

pub(crate) struct RebuildParts {
    pub original_error: Option<AccountError>,
    pub partial_commit: Option<Arc<PartialCommit>>,
    pub primary_cause: PrimaryCause,
    pub chain_extras: Vec<Cause>,
    pub classification_cause_count: usize,
    pub scope: Option<ErrorScope>,
    pub operation: Option<AccountOperation>,
    pub provider: Option<Provider>,
    pub protocol: Option<Protocol>,
    pub diagnostics: DiagnosticInfo,
    pub idempotency_override: Option<bool>,
    pub unsupported_reason: Option<UnsupportedReason>,
}

#[derive(Clone, Debug)]
pub struct AccountErrorBuilder {
    original_error: Option<AccountError>,
    partial_commit: Option<PartialCommitBuilder>,
    partial_commit_duplicate: bool,
    throttle_not_applicable: bool,
    primary_cause: PrimaryCause,
    chain_extras: Vec<Cause>,
    classification_cause_count: Option<usize>,
    scope: Option<ErrorScope>,
    operation: Option<AccountOperation>,
    provider: Option<Provider>,
    protocol: Option<Protocol>,
    diagnostics: DiagnosticInfo,
    idempotency_override: Option<bool>,
    unsupported_reason: Option<UnsupportedReason>,
}

impl AccountErrorBuilder {
    #[must_use]
    pub fn new(primary_cause: PrimaryCause) -> Self {
        let status = match &primary_cause {
            PrimaryCause::Server(super::cause::ServerCause::Error { status }) => *status,
            _ => None,
        };
        Self {
            original_error: None,
            partial_commit: None,
            partial_commit_duplicate: false,
            throttle_not_applicable: false,
            primary_cause,
            chain_extras: Vec::new(),
            classification_cause_count: None,
            scope: None,
            operation: None,
            provider: None,
            protocol: None,
            diagnostics: DiagnosticInfo {
                status,
                ..DiagnosticInfo::default()
            },
            idempotency_override: None,
            unsupported_reason: None,
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
            original_error: parts.original_error,
            partial_commit: parts.partial_commit.map(|r| r.builder()),
            partial_commit_duplicate: false,
            throttle_not_applicable: false,
            primary_cause: parts.primary_cause,
            chain_extras: parts.chain_extras,
            classification_cause_count: Some(parts.classification_cause_count),
            scope: parts.scope,
            operation: parts.operation,
            provider: parts.provider,
            protocol: parts.protocol,
            diagnostics: parts.diagnostics,
            idempotency_override: parts.idempotency_override,
            unsupported_reason: parts.unsupported_reason,
        }
    }

    #[must_use]
    pub fn partial_commit(mut self, record: PartialCommitBuilder) -> Self {
        if self.partial_commit.is_some() {
            self.partial_commit_duplicate = true;
        } else {
            self.partial_commit = Some(record);
        }
        self
    }

    #[must_use]
    pub fn operation(mut self, op: AccountOperation) -> Self {
        self.operation = Some(op);
        self
    }

    #[must_use]
    pub fn scope(mut self, scope: ErrorScope) -> Self {
        if let ErrorScope::Cursor(target) = &scope
            && let Some(primary) = self.primary_cause.cursor_scope_mut()
        {
            *primary = target.clone();
        }
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
    /// instead. This is diagnostic evidence only and never changes recovery.
    /// ServerCause::Error initializes it from the primary status; an explicit
    /// setter replaces it, including on a rebuilt error.
    #[must_use]
    pub fn status(mut self, status: Option<u16>) -> Self {
        // Support must see the observed response supplied by the boundary,
        // even when the primary lacked a status. Keep recovery independent so
        // diagnostic decoration cannot authorize replay of a user's write.
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
    ///
    /// Overrides compose monotonically: `false` wins over `true`, including
    /// across `AccountError::into_builder`. A guard can establish replay
    /// safety only before contrary evidence narrows it. Enterprise users
    /// must not get a duplicate write because a later decoration erased
    /// the transport's possible-commit evidence.
    #[must_use]
    pub fn idempotency_override(mut self, idempotent: bool) -> Self {
        self.idempotency_override = Some(self.idempotency_override.unwrap_or(true) && idempotent);
        self
    }

    /// Decorate provider throttle evidence when the primary cause is rate-limit
    /// or quota exhaustion. Other causes have no throttle payload to refine.
    /// `try_build` rejects refinements on other primary causes. This cannot
    /// change the primary classification or redirect cursor recovery.
    #[must_use]
    pub fn with_throttle(mut self, refine: impl FnOnce(&mut super::cause::Throttle)) -> Self {
        if let Some(throttle) = self.primary_cause.throttle_mut() {
            let local_hold = throttle.local_hold;
            refine(throttle);
            // An outer decoration cannot claim ownership of a deadline the
            // transport already installed. Preserve the original provenance.
            throttle.local_hold |= local_hold;
        } else {
            self.throttle_not_applicable = true;
        }
        self
    }

    /// Explain a local unsupported refusal without parsing diagnostic prose.
    /// Rejected by `try_build` on any other error kind.
    #[must_use]
    pub fn unsupported_reason(mut self, reason: UnsupportedReason) -> Self {
        self.unsupported_reason = Some(reason);
        self
    }

    /// Construct the `AccountError`. Returns `Err` only on producer-bug
    /// invariants (see [`AccountErrorBuildError`]); protocol-crate
    /// translation boundaries call `.expect("valid account error
    /// classification")` because an invalid combination is a library
    /// bug, not recoverable runtime state.
    pub fn try_build(mut self) -> Result<AccountError, AccountErrorBuildError> {
        let kind = self.primary_cause.kind();
        // Scope-directed failures own their routing identity. A cursor rebind
        // updates this typed identity; unrelated diagnostic scope cannot erase it.
        if let Some(scope) = self.primary_cause.cursor_scope() {
            self.scope = Some(ErrorScope::Cursor(scope.clone()));
        }
        let throttle_scope = self
            .primary_cause
            .throttle()
            .and_then(|throttle| throttle.scope);
        if self.throttle_not_applicable {
            return Err(AccountErrorBuildError::ThrottleNotApplicable { kind });
        }
        // Keep this relational check: a shared decoration builder can receive
        // any primary family, while a refusal reason is meaningful only here.
        if self.unsupported_reason.is_some() && !matches!(kind, AccountErrorKind::Unsupported(_)) {
            return Err(AccountErrorBuildError::UnsupportedReasonNotApplicable { kind });
        }
        // Records describe several independently supplied legs and references.
        // A primary cause cannot prove their consistency or unique attachment;
        // retaining a fallible terminator avoids silently losing committed work.
        if self.partial_commit_duplicate {
            return Err(AccountErrorBuildError::PartialCommitAlreadyAttached);
        }
        let mut causes = Vec::with_capacity(1 + self.chain_extras.len());
        causes.push(self.primary_cause.clone().into());
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
        // wire-level evidence contradicts the kind. Keep this check because
        // attempts are independent evidence, not part of the transport payload.
        if matches!(kind, AccountErrorKind::Transport(_))
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
            &kind,
            self.scope.as_ref(),
            self.operation,
            &classification_chain,
            throttle_scope,
            self.idempotency_override,
        );
        let leg_recovery = recovery.clone();
        let (partial_commit, recovery) = if let Some(record) = self.partial_commit {
            let attempt = super::partial::first_attempt(classification_chain.iter());
            let (record, projected) = record.finish(&recovery, attempt, &kind)?;
            (Some(Arc::new(record)), projected)
        } else {
            (None, recovery)
        };
        let remediation =
            recovery::suggest(&kind, &recovery, self.scope.as_ref(), &classification_chain);
        let message_key = message_key::derive(&kind);

        Ok(AccountError::from_parts(AccountErrorParts {
            original_error: self.original_error,
            primary_cause: self.primary_cause,
            partial_commit,
            leg_recovery,
            kind,
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
            unsupported_reason: self.unsupported_reason,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ThrottleBudget;
    use crate::error::{
        AttemptCause, Cause, PrimaryCause, RequestCause, TransmissionState, TransportCause,
        TransportErrorKind,
    };

    #[test]
    fn idempotency_override_is_monotone_across_decoration() {
        let failure = || {
            AccountErrorBuilder::new(PrimaryCause::Transport(TransportCause::new(
                TransportErrorKind::Network,
                None,
            )))
            .operation(AccountOperation::Send)
            .push_cause(Cause::Attempt(AttemptCause::new(
                TransmissionState::InFlight,
            )))
        };
        let narrowed = failure().idempotency_override(false).try_build().unwrap();
        let widened = failure().idempotency_override(true).try_build().unwrap();
        assert!(matches!(
            widened.recovery(),
            recovery::RecoveryClass::Retry(_)
        ));
        assert!(matches!(
            narrowed.recovery(),
            recovery::RecoveryClass::Reconcile(_)
        ));
        for overrides in [[false, true], [true, false], [false, false]] {
            let error = failure()
                .idempotency_override(overrides[0])
                .idempotency_override(overrides[1])
                .try_build()
                .unwrap();
            assert_eq!(error.recovery(), narrowed.recovery());
        }
        // Clone forces the shared-Arc rebuild path; the second pass covers
        // rebuilding an owned error. Neither can erase a previous narrowing.
        let mut error = narrowed.clone();
        for _ in 0..2 {
            error = error
                .into_builder()
                .idempotency_override(true)
                .try_build()
                .unwrap();
            assert_eq!(error.recovery(), narrowed.recovery());
        }
    }

    #[test]
    fn decoration_does_not_adopt_sibling_transmission_evidence() {
        let original = AccountErrorBuilder::new(PrimaryCause::Transport(TransportCause {
            kind: TransportErrorKind::Network,
            message: None,
        }))
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

        for (primary, extra) in [
            (
                ServerCause::RateLimited {
                    throttle: Default::default(),
                    retry_hint: None,
                },
                Cause::Server(ServerCause::RateLimited {
                    throttle: Default::default(),
                    retry_hint: Some(RetryHint::After(std::time::Duration::from_secs(37))),
                }),
            ),
            (
                ServerCause::Error { status: Some(554) },
                Cause::Wire(WireCause::Smtp(EnhancedStatusCode::new(554, None, None))),
            ),
        ] {
            let original = AccountErrorBuilder::new(PrimaryCause::Server(primary))
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
        let err = AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("bad"),
        }))
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
        let err = AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("bad"),
        }))
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

    /// A stamped budget is a builder input like `throttle_scope`, and must
    /// survive decoration through `into_builder`: a producer that rebuilds an
    /// error (to add evidence or an idempotency override) would otherwise
    /// silently move its hold back onto the account key.
    #[test]
    fn throttle_budget_survives_into_builder() {
        let err = AccountErrorBuilder::new(PrimaryCause::Server(
            crate::error::ServerCause::RateLimited {
                throttle: Default::default(),
                retry_hint: None,
            },
        ))
        .with_throttle(|throttle| throttle.add_budget(ThrottleBudget::new("b")))
        .with_throttle(|throttle| throttle.add_budget(ThrottleBudget::new("b")))
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
    fn throttle_refinement_rejects_non_throttle_primary() {
        let mut called = false;
        let result = AccountError::account_closed(AccountOperation::Send, Protocol::Jmap)
            .into_builder()
            .with_throttle(|_| called = true)
            .try_build();
        assert!(!called);
        assert!(matches!(
            result,
            Err(AccountErrorBuildError::ThrottleNotApplicable { .. })
        ));
    }

    #[test]
    fn transport_acknowledged_rejected() {
        let err = AccountErrorBuilder::new(PrimaryCause::Transport(TransportCause {
            kind: TransportErrorKind::Network,
            message: None,
        }))
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::Acknowledged,
        )))
        .try_build()
        .expect_err("transport + acknowledged is invalid");

        assert_eq!(err, AccountErrorBuildError::TransportAcknowledged);
    }
}

#[cfg(test)]
mod refusal_contract_tests {
    use super::*;
    use crate::{RecoveryClass, RemediationAction, RequestCause, RequestErrorKind};

    #[test]
    fn request_refusals_validate_pairs_and_derive_terminal_keys() {
        let cases = [
            (
                RequestErrorKind::PayloadTooLarge,
                RequestCause::PayloadTooLarge {
                    detail: DiagnosticText::support_only("too big"),
                },
                "request.payload-too-large",
            ),
            (
                RequestErrorKind::PreconditionFailed,
                RequestCause::PreconditionFailed {
                    detail: DiagnosticText::support_only("active"),
                },
                "request.precondition-failed",
            ),
        ];
        for (kind, cause, key) in cases {
            let error = AccountErrorBuilder::new(PrimaryCause::Request(cause.clone()))
                .operation(AccountOperation::Send)
                .try_build()
                .expect("matching refusal");
            assert_eq!(error.message_key(), key);
            assert_eq!(error.recovery(), &RecoveryClass::ProviderRefused);
            assert_eq!(
                error.suggested_remediation(),
                Some(&RemediationAction::FixClientRequest)
            );
            assert_eq!(error.kind(), &AccountErrorKind::Request(kind));
        }
    }

    #[test]
    fn unsupported_reason_survives_shared_and_unique_decoration() {
        let plain = AccountError::unsupported(AccountOperation::Send, Protocol::Jmap);
        assert_eq!(plain.unsupported_reason(), None);
        let error = plain
            .into_builder()
            .unsupported_reason(UnsupportedReason::ForeignScheduledSend)
            .try_build()
            .expect("unsupported reason");
        let shared = error.clone();
        let decorated = error
            .into_builder()
            .scope(ErrorScope::Account)
            .text(DiagnosticText::support_only("diagnostic can change"))
            .try_build()
            .expect("shared decoration");
        let decorated = decorated
            .into_builder()
            .protocol(Protocol::Jmap)
            .try_build()
            .expect("unique decoration");
        assert_eq!(decorated.unsupported_reason(), shared.unsupported_reason());
        assert_eq!(
            decorated.unsupported_reason(),
            Some(UnsupportedReason::ForeignScheduledSend)
        );
        assert_eq!(decorated.recovery(), shared.recovery());
        assert_eq!(decorated.message_key(), "unsupported.operation");
    }

    #[test]
    fn unsupported_reason_is_rejected_on_other_kinds() {
        let error = AccountError::account_closed(AccountOperation::Send, Protocol::Jmap)
            .into_builder()
            .unsupported_reason(UnsupportedReason::SubmissionUnavailable)
            .try_build();
        assert!(matches!(
            error,
            Err(AccountErrorBuildError::UnsupportedReasonNotApplicable { .. })
        ));
    }
}

#[cfg(test)]
mod primary_cause_tests {
    use super::*;
    use crate::{
        AccessCause, AccessErrorKind, CursorScope, EngineDirective, FolderId,
        MailboxUnavailableKind, PrimaryCause, ProtocolErrorKind, RecoveryClass, ServerCause,
        ServerErrorKind, StateCause, Throttle, ThrottleBudget, ThrottleScope, WireCause,
    };

    #[test]
    fn primary_cause_preserves_mailbox_permanence() {
        for (permanence, retryable) in [
            (MailboxUnavailableKind::Transient, true),
            (MailboxUnavailableKind::Permanent, false),
        ] {
            let error =
                AccountErrorBuilder::new(PrimaryCause::Access(AccessCause::MailboxUnavailable {
                    kind: permanence,
                }))
                .try_build()
                .unwrap();
            assert_eq!(
                error.kind(),
                &AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                    kind: permanence
                })
            );
            assert_eq!(error.recovery().is_retryable(), retryable);
        }
    }

    #[test]
    fn primary_server_status_owns_classification_but_diagnostics_can_be_refined() {
        for (status, retryable) in [(Some(503), true), (Some(403), false), (None, false)] {
            let default =
                AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::Error { status }))
                    .try_build()
                    .unwrap();
            assert_eq!(default.telemetry_fields().status, status);
            let cleared = default.into_builder().status(None).try_build().unwrap();
            assert_eq!(cleared.telemetry_fields().status, None);
            let error =
                AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::Error { status }))
                    .operation(AccountOperation::Hydrate)
                    .status(Some(418))
                    .push_cause(Cause::Server(ServerCause::Error { status: Some(503) }))
                    .try_build()
                    .unwrap();
            assert_eq!(
                error.kind(),
                &AccountErrorKind::Server(ServerErrorKind::Error { status })
            );
            assert_eq!(error.telemetry_fields().status, Some(418));
            assert_eq!(error.recovery().is_retryable(), retryable);
            let rebuilt = error
                .clone()
                .into_builder()
                .status(Some(500))
                .try_build()
                .unwrap();
            assert_eq!(rebuilt.kind(), error.kind());
            assert_eq!(rebuilt.recovery(), error.recovery());
            assert_eq!(rebuilt.telemetry_fields().status, Some(500));
        }
    }

    #[test]
    fn primary_state_scope_survives_decoration_and_explicit_rebinding() {
        let original = CursorScope::Folder(FolderId("original".into()));
        let rebound = CursorScope::Folder(FolderId("rebound".into()));
        for (state, directive) in [
            (
                StateCause::CursorInvalid {
                    scope: original.clone(),
                },
                EngineDirective::RestartScope(original.clone()),
            ),
            (
                StateCause::ScopeCapabilityLost {
                    scope: original.clone(),
                },
                EngineDirective::DowngradeCapabilityForScope(original.clone()),
            ),
            (
                StateCause::ScopeRevoked {
                    scope: original.clone(),
                },
                EngineDirective::DisableScope(original.clone()),
            ),
        ] {
            let error = AccountErrorBuilder::new(PrimaryCause::State(state))
                .scope(ErrorScope::Account)
                .push_cause(Cause::State(StateCause::CursorInvalid {
                    scope: rebound.clone(),
                }))
                .try_build()
                .unwrap();
            assert_eq!(error.scope(), Some(&ErrorScope::Cursor(original.clone())));
            assert_eq!(error.recovery(), &RecoveryClass::Engine(directive));
            // Cover both the shared Arc and uniquely owned rebuild paths.
            let shared = error.clone();
            let mut rebuilt = error;
            for _ in 0..2 {
                rebuilt = rebuilt
                    .into_builder()
                    .scope(ErrorScope::Cursor(rebound.clone()))
                    .try_build()
                    .unwrap();
                assert_eq!(rebuilt.scope(), Some(&ErrorScope::Cursor(rebound.clone())));
                assert!(matches!(rebuilt.recovery(), RecoveryClass::Engine(d)
                    if d.target_scope() == Some(rebound.clone())));
                assert!(matches!(rebuilt.chain().outermost(), Cause::State(
                    StateCause::CursorInvalid { scope } | StateCause::ScopeCapabilityLost { scope }
                    | StateCause::ScopeRevoked { scope }) if scope == &rebound));
            }
            assert_eq!(shared.scope(), Some(&ErrorScope::Cursor(original.clone())));
        }
    }

    #[test]
    fn primary_throttle_provenance_cannot_be_erased_by_decoration() {
        let error = AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
            retry_hint: Some(recovery::RetryHint::After(std::time::Duration::from_secs(
                30,
            ))),
            throttle: Throttle {
                scope: Some(ThrottleScope::Account),
                budgets: vec![ThrottleBudget::new("mailbox")],
                local_hold: true,
            },
        }))
        .try_build()
        .unwrap();
        let original = error.clone();
        let rebuilt = error
            .into_builder()
            .with_throttle(|throttle| throttle.local_hold = false)
            .try_build()
            .unwrap();
        assert!(rebuilt.is_local_hold());
        assert_eq!(rebuilt.throttle_budgets(), original.throttle_budgets());
        assert_eq!(rebuilt.recovery(), original.recovery());
        assert!(
            rebuilt
                .throttle_holds()
                .into_iter()
                .all(|(target, _)| matches!(target, crate::HoldTarget::CurrentOperation))
        );
    }

    #[test]
    fn primary_protocol_subkind_survives_rebuilding() {
        for kind in [
            ProtocolErrorKind::ParseFailed,
            ProtocolErrorKind::PartialResponse,
        ] {
            let error = AccountErrorBuilder::new(PrimaryCause::Protocol {
                kind,
                wire: WireCause::MalformedResponse {
                    protocol: Protocol::Graph,
                    detail: None,
                },
            })
            .operation(AccountOperation::Hydrate)
            .try_build()
            .unwrap();
            assert_eq!(error.kind(), &AccountErrorKind::Protocol(kind));
            assert!(
                matches!(error.chain().outermost(), Cause::Protocol { kind: stored, .. } if *stored == kind)
            );
            let rebuilt = error.clone().into_builder().try_build().unwrap();
            assert_eq!(rebuilt.kind(), error.kind());
            assert_eq!(rebuilt.recovery(), error.recovery());
        }
    }
}
