use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use super::cause::{CauseChain, PrimaryCause};
use super::diagnostic::{
    DetailVisibility, DiagnosticInfo, SupportExportConsented, SupportExportInternal,
    SupportExportMinimal, TelemetryToken, TelemetryView, account_kind_discriminant,
};
use super::kind::{AccountErrorKind, UnsupportedReason};
use super::partial::{CompensationOutcome, PartialCommit, StepOutcome};
use super::recovery::{
    ReconcileAction, ReconcileReason, RecoveryClass, RemediationAction, RetryDisposition,
    RetryReason, ThrottleBudget, ThrottleScope,
};
use super::scope::{AccountOperation, ErrorScope, Protocol, Provider};

/// The one wait for a throttle or retryable failure that carries no provider
/// timing. `AccountError::throttle_holds` stamps it on hintless holds, and the
/// engine's hintless retry sleep is the same value, so a sibling declaring a
/// held budget is paced exactly as the observer paces itself.
pub const HINTLESS_HOLD_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// A throttle target before the observing account identity is supplied.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum HoldTarget {
    Account,
    Folder(crate::FolderId),
    Owner(crate::PrincipalId),
    Budget(ThrottleBudget),
    CurrentOperation,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AccountError {
    inner: Arc<AccountErrorInner>,
}

#[derive(Clone, Debug)]
struct AccountErrorInner {
    original_error: Option<AccountError>,
    partial_commit: Option<Arc<PartialCommit>>,
    leg_recovery: RecoveryClass,
    primary_cause: PrimaryCause,
    kind: AccountErrorKind,
    recovery: RecoveryClass,
    remediation: Option<RemediationAction>,
    scope: Option<ErrorScope>,
    operation: Option<AccountOperation>,
    provider: Option<Provider>,
    protocol: Option<Protocol>,
    diagnostics: DiagnosticInfo,
    chain: CauseChain,
    classification_cause_count: usize,
    message_key: &'static str,
    idempotency_override: Option<bool>,
    unsupported_reason: Option<UnsupportedReason>,
}

impl AccountError {
    /// Refuse work on a closed account incarnation before dispatch.
    #[must_use]
    pub fn account_closed(operation: AccountOperation, protocol: Protocol) -> Self {
        // An Outlook user expects closing an account to stop its work, without
        // a retry, credential prompt, or claim that the feature is unsupported.
        super::builder::AccountErrorBuilder::new(super::cause::PrimaryCause::State(
            super::cause::StateCause::AccountClosed,
        ))
        .operation(operation)
        .protocol(protocol)
        .scope(ErrorScope::Account)
        .try_build()
        .expect("valid closed account error classification")
    }

    /// Refuse an operation locally, with its protocol and canonical classification.
    /// Add scope, provider, or diagnostics through [`Self::into_builder`].
    #[must_use]
    pub fn unsupported(operation: AccountOperation, protocol: Protocol) -> Self {
        super::builder::AccountErrorBuilder::new(super::cause::PrimaryCause::Request(
            super::cause::RequestCause::Unsupported { operation },
        ))
        .operation(operation)
        .protocol(protocol)
        .try_build()
        .expect("valid unsupported account error classification")
    }

    /// A stale caller-owned page cursor asks the consumer to restart that walk.
    #[must_use]
    pub fn invalid_page_cursor(
        operation: AccountOperation,
        protocol: Protocol,
        detail: impl Into<String>,
    ) -> Self {
        // An Outlook user expects stale results to reload. This signal targets
        // only the page walk, never the account's durable sync checkpoints.
        super::builder::AccountErrorBuilder::new(super::cause::PrimaryCause::Request(
            super::cause::RequestCause::InvalidArgument {
                field: Some("page_cursor"),
                message: Some(super::diagnostic::DiagnosticText::support_only(
                    detail.into(),
                )),
            },
        ))
        .operation(operation)
        .protocol(protocol)
        .try_build()
        .expect("valid page cursor classification")
    }

    /// Quarantine a denied foreign scope while retaining the original evidence.
    /// Non-permission errors and scopes without a foreign owner pass unchanged.
    #[must_use]
    pub fn revoke_foreign_scope(
        self,
        scope: crate::CursorScope,
        owner: Option<&crate::PrincipalId>,
    ) -> Self {
        let Some(owner) = owner else {
            return self;
        };
        if self.kind()
            != &AccountErrorKind::Authorization(super::kind::AccessErrorKind::PermissionDenied)
        {
            return self;
        }
        // Losing a delegated folder must not disable an Outlook user's own
        // mailbox. Preserve every original cause and diagnostic for support.
        // Retain the classification prefix too: historical evidence added by
        // decoration must not become evidence of a new in-flight operation.
        let builder =
            super::builder::AccountErrorBuilder::from_rebuild(super::builder::RebuildParts {
                original_error: Some(self.clone()),
                partial_commit: self.inner.partial_commit.clone(),
                primary_cause: PrimaryCause::State(super::cause::StateCause::ScopeRevoked {
                    scope: scope.clone(),
                }),
                chain_extras: self.chain().iter().cloned().collect(),
                classification_cause_count: self.inner.classification_cause_count + 1,
                scope: Some(ErrorScope::Cursor(scope)),
                operation: self.inner.operation,
                provider: self.inner.provider,
                protocol: self.inner.protocol,
                diagnostics: self.inner.diagnostics.clone(),
                idempotency_override: self.inner.idempotency_override,
                unsupported_reason: None,
            });
        builder
            .text(super::diagnostic::DiagnosticText::support_only(format!(
                "shared scope access revoked (owner {})",
                owner.0,
            )))
            .try_build()
            .expect("valid foreign scope revocation")
    }

    pub(crate) fn from_parts(parts: AccountErrorParts) -> Self {
        Self {
            inner: Arc::new(AccountErrorInner {
                original_error: parts.original_error,
                partial_commit: parts.partial_commit,
                leg_recovery: parts.leg_recovery,
                primary_cause: parts.primary_cause,
                kind: parts.kind,
                recovery: parts.recovery,
                remediation: parts.remediation,
                scope: parts.scope,
                operation: parts.operation,
                provider: parts.provider,
                protocol: parts.protocol,
                diagnostics: parts.diagnostics,
                chain: parts.chain,
                classification_cause_count: parts.classification_cause_count,
                message_key: parts.message_key,
                idempotency_override: parts.idempotency_override,
                unsupported_reason: parts.unsupported_reason,
            }),
        }
    }

    /// The provider rate budgets the failed request was held against, as
    /// the producer stamped them in the primary cause's `Throttle` payload.
    /// Empty unless the error is a throttle whose producer can name its
    /// budget; the engine then records the deadline on exactly these
    /// instead of lifting the `ThrottleScope`. A local gate refusal retains
    /// these identities as evidence but does not record another shared hold.
    #[must_use]
    pub fn throttle_budgets(&self) -> &[ThrottleBudget] {
        self.inner
            .primary_cause
            .throttle()
            .map_or(&[], |throttle| throttle.budgets.as_slice())
    }

    /// A transport gate refused this leg against an already recorded hold.
    /// Recovery and budget evidence remain intact, but observing this error
    /// must not install another shared hold. This is per-leg provenance;
    /// sibling and cleanup failures keep their own ownership.
    #[must_use]
    pub fn is_local_hold(&self) -> bool {
        self.inner
            .primary_cause
            .throttle()
            .is_some_and(|throttle| throttle.local_hold)
    }

    /// Historical effects of this operation, when it crossed a partial commit.
    pub fn partial_commit(&self) -> Option<&PartialCommit> {
        self.inner.partial_commit.as_deref()
    }
    /// Full recovery of the primary failed leg, not permission to replay the operation.
    pub fn leg_recovery(&self) -> &RecoveryClass {
        &self.inner.leg_recovery
    }
    pub(crate) fn classification_attempt(&self) -> Option<super::TransmissionState> {
        self.classification_attempt_cause()
            .map(|a| a.transmission_state)
    }
    /// The classification prefix's attempt evidence, answer evidence included.
    pub(crate) fn classification_attempt_cause(&self) -> Option<super::AttemptCause> {
        super::partial::first_attempt(
            self.inner
                .chain
                .iter()
                .take(self.inner.classification_cause_count),
        )
    }
    /// Each failure with its own recovery, scope and budgets, including cleanup.
    pub fn throttle_observations(&self) -> Vec<(&AccountError, &RecoveryClass)> {
        let mut observations = vec![(self, self.leg_recovery())];
        if let Some(record) = self.partial_commit() {
            for step in record.steps() {
                if let StepOutcome::Failed { error, .. } = step.outcome() {
                    observations.push((error, error.recovery()));
                }
            }
            for c in record.compensations() {
                if let CompensationOutcome::Failed(error)
                | CompensationOutcome::Unconfirmed(error) = c.outcome()
                {
                    observations.push((error, error.recovery()));
                }
            }
        }
        observations
    }

    /// The typed local refusal reason, when the producer supplied one.
    /// This does not change recovery or the canonical unsupported message key.
    #[must_use]
    pub fn unsupported_reason(&self) -> Option<UnsupportedReason> {
        self.inner.unsupported_reason
    }

    /// Holds across primary, sibling and cleanup failures, with budget precedence.
    /// Local gate refusals pace only the observer; their owner already holds
    /// the shared deadline.
    #[must_use]
    pub fn throttle_holds(&self) -> Vec<(HoldTarget, super::RetryHint)> {
        let mut holds = Vec::new();
        for (error, recovery) in self.throttle_observations() {
            let (scope, hint, throttled) = match recovery {
                RecoveryClass::Retry(a) => (
                    a.throttle_scope,
                    a.retry_hint,
                    matches!(a.reason, RetryReason::RateLimited),
                ),
                RecoveryClass::Reconcile(a) => (
                    a.throttle_scope,
                    a.retry_hint,
                    matches!(a.reason, ReconcileReason::ThrottledMidFlight),
                ),
                _ => continue,
            };
            let fallback = super::RetryHint::After(HINTLESS_HOLD_WAIT);
            if error.is_local_hold() {
                // An Outlook user expects the existing provider wait to be
                // honored once. Re-observing a local gate must not create or
                // extend an engine hold, even when a budget was stamped.
                holds.push((HoldTarget::CurrentOperation, hint.unwrap_or(fallback)));
                continue;
            }
            if !error.throttle_budgets().is_empty() {
                holds.extend(
                    error
                        .throttle_budgets()
                        .iter()
                        .cloned()
                        .map(|b| (HoldTarget::Budget(b), hint.unwrap_or(fallback))),
                );
                continue;
            }
            // Enterprise users expect one throttled folder to pause without
            // stalling sibling folders, including when timing is omitted.
            let target = match scope {
                Some(ThrottleScope::Account) if hint.is_some() => HoldTarget::Account,
                Some(ThrottleScope::ScopedResource) => match error.scope() {
                    Some(ErrorScope::Owner { id }) => HoldTarget::Owner(id.clone()),
                    Some(ErrorScope::Mailbox { id }) => {
                        HoldTarget::Folder(crate::FolderId::from(id))
                    }
                    Some(ErrorScope::Cursor(
                        crate::CursorScope::Folder(id)
                        | crate::CursorScope::FolderType { folder: id, .. },
                    )) => HoldTarget::Folder(id.clone()),
                    _ => HoldTarget::CurrentOperation,
                },
                _ if scope.is_some() || hint.is_some() || throttled => HoldTarget::CurrentOperation,
                _ => continue,
            };
            holds.push((target, hint.unwrap_or(fallback)));
        }
        holds
    }

    #[must_use]
    pub fn kind(&self) -> &AccountErrorKind {
        &self.inner.kind
    }

    #[must_use]
    pub fn recovery(&self) -> &RecoveryClass {
        &self.inner.recovery
    }

    #[must_use]
    pub fn suggested_remediation(&self) -> Option<&RemediationAction> {
        self.inner.remediation.as_ref()
    }

    #[must_use]
    pub fn scope(&self) -> Option<&ErrorScope> {
        self.inner.scope.as_ref()
    }

    #[must_use]
    pub fn operation(&self) -> Option<AccountOperation> {
        self.inner.operation
    }

    #[must_use]
    pub fn provider(&self) -> Option<Provider> {
        self.inner.provider
    }

    #[must_use]
    pub fn protocol(&self) -> Option<Protocol> {
        self.inner.protocol
    }

    #[must_use]
    pub fn message_key(&self) -> &'static str {
        self.inner.message_key
    }

    /// Complete diagnostic evidence, including support-only text.
    /// Apply the caller's consent policy before exporting this information.
    #[must_use]
    pub fn diagnostics(&self) -> &DiagnosticInfo {
        &self.inner.diagnostics
    }

    pub fn user_safe_text(&self) -> impl Iterator<Item = &str> {
        self.inner
            .diagnostics
            .text
            .iter()
            .filter(|text| text.visibility == DetailVisibility::UserSafe)
            .map(|text| text.value.as_str())
    }

    #[must_use]
    pub fn telemetry_fields(&self) -> TelemetryView<'_> {
        TelemetryView::from_account_error(self)
    }

    #[must_use]
    pub fn support_minimal(&self) -> SupportExportMinimal<'_> {
        self.telemetry_fields()
    }

    #[must_use]
    pub fn support_consented(&self) -> SupportExportConsented<'_> {
        let mut user_safe_text = Vec::new();
        let mut support_text = Vec::new();
        for text in &self.inner.diagnostics.text {
            match text.visibility {
                DetailVisibility::UserSafe => user_safe_text.push(text.value.as_str()),
                DetailVisibility::SupportOnly => support_text.push(text.value.as_str()),
            }
        }

        SupportExportConsented {
            telemetry: self.telemetry_fields(),
            user_safe_text,
            support_text,
            scope: self.inner.scope.as_ref(),
            partial_commit: self.partial_commit().map(PartialCommit::export),
        }
    }

    #[must_use]
    pub fn support_internal(&self) -> SupportExportInternal<'_> {
        SupportExportInternal {
            consented: self.support_consented(),
            partial_commit_chains: self
                .partial_commit()
                .map(PartialCommit::chains)
                .unwrap_or_default(),
            chain: self
                .inner
                .chain
                .iter()
                .map(super::cause::Cause::summary)
                .collect(),
        }
    }

    #[must_use]
    pub fn chain(&self) -> &CauseChain {
        &self.inner.chain
    }

    /// Consume this error and return a builder pre-populated with its
    /// fields and chain.
    ///
    /// `into_builder` is for *decoration*, not reclassification: the
    /// returned builder carries the original `PrimaryCause` and exposes no
    /// kind-changing path. `push_cause` only
    /// appends secondary evidence to the chain. If a caller needs a
    /// different primary `kind` or a different outermost `Cause`, it
    /// must construct a fresh builder via `AccountErrorBuilder::new`.
    ///
    /// Derived fields (`recovery`, `remediation`, `message_key`) are
    /// recomputed on `try_build()` using only the original classification
    /// causes. Appended evidence does not participate. Builder overrides
    /// (`idempotency_override`) and all primary throttle data and retry hints
    /// survive the round-trip.
    #[must_use]
    pub fn into_builder(self) -> super::builder::AccountErrorBuilder {
        let inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        let mut causes = inner.chain.into_vec();
        causes.remove(0);
        let primary_cause = inner.primary_cause;
        super::builder::AccountErrorBuilder::from_rebuild(super::builder::RebuildParts {
            original_error: inner.original_error,
            partial_commit: inner.partial_commit,
            primary_cause,
            chain_extras: causes,
            classification_cause_count: inner.classification_cause_count,
            scope: inner.scope,
            operation: inner.operation,
            provider: inner.provider,
            protocol: inner.protocol,
            diagnostics: inner.diagnostics,
            idempotency_override: inner.idempotency_override,
            unsupported_reason: inner.unsupported_reason,
        })
    }
}

impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({:?})", self.inner.message_key, self.inner.kind)
    }
}

impl StdError for AccountError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.inner.original_error {
            Some(original) => Some(original),
            None => Some(self.inner.chain.outermost()),
        }
    }
}

pub(crate) struct AccountErrorParts {
    pub original_error: Option<AccountError>,
    pub partial_commit: Option<Arc<PartialCommit>>,
    pub leg_recovery: RecoveryClass,
    pub primary_cause: PrimaryCause,
    pub kind: AccountErrorKind,
    pub recovery: RecoveryClass,
    pub remediation: Option<RemediationAction>,
    pub scope: Option<ErrorScope>,
    pub operation: Option<AccountOperation>,
    pub provider: Option<Provider>,
    pub protocol: Option<Protocol>,
    pub diagnostics: DiagnosticInfo,
    pub chain: CauseChain,
    pub classification_cause_count: usize,
    pub message_key: &'static str,
    pub idempotency_override: Option<bool>,
    pub unsupported_reason: Option<UnsupportedReason>,
}

impl<'a> TelemetryView<'a> {
    pub(crate) fn from_account_error(error: &'a AccountError) -> Self {
        const EMPTY_ACTIONS: &[ReconcileAction] = &[];

        let (retry_disposition, retry_reason, throttle_scope, reconcile_reason, reconcile_actions) =
            recovery_fields(error.recovery());

        Self {
            unsupported_reason: error.unsupported_reason(),
            kind_discriminant: account_kind_discriminant(error.kind()),
            message_key: error.message_key(),
            recovery_discriminant: recovery_discriminant(error.recovery()),
            provider: error.provider(),
            protocol: error.protocol(),
            status: error.inner.diagnostics.status,
            native_code: error
                .inner
                .diagnostics
                .native_code
                .as_ref()
                .map(TelemetryToken::as_str),
            request_id: error
                .inner
                .diagnostics
                .request_id
                .as_ref()
                .map(TelemetryToken::as_str),
            trace_id: error
                .inner
                .diagnostics
                .trace_id
                .as_ref()
                .map(TelemetryToken::as_str),
            retry_disposition,
            retry_reason,
            throttle_scope,
            reconcile_reason,
            reconcile_actions: reconcile_actions.unwrap_or(EMPTY_ACTIONS),
            transmission_state: error.chain().iter().find_map(|cause| {
                if let super::cause::Cause::Attempt(cause) = cause {
                    Some(cause.transmission_state)
                } else {
                    None
                }
            }),
            operation: error.operation(),
            partial_commit: error
                .partial_commit()
                .map(|r| r.telemetry(error.leg_recovery())),
        }
    }
}

type RecoveryFields<'a> = (
    Option<RetryDisposition>,
    Option<RetryReason>,
    Option<ThrottleScope>,
    Option<ReconcileReason>,
    Option<&'a [ReconcileAction]>,
);

fn recovery_fields(recovery: &RecoveryClass) -> RecoveryFields<'_> {
    match recovery {
        RecoveryClass::Retry(advice) => (
            Some(advice.disposition),
            Some(advice.reason),
            advice.throttle_scope,
            None,
            None,
        ),
        RecoveryClass::Reconcile(advice) => (
            None,
            None,
            advice.throttle_scope,
            Some(advice.reason),
            Some(advice.guidance.actions.as_slice()),
        ),
        RecoveryClass::Cancelled
        | RecoveryClass::Engine(_)
        | RecoveryClass::AuthLost
        | RecoveryClass::NeedsAdminConsent { .. }
        | RecoveryClass::NeedsPolicyChange
        | RecoveryClass::NoPermission { .. }
        | RecoveryClass::Unsupported(_)
        | RecoveryClass::ClientBug
        | RecoveryClass::ProviderContractViolation
        | RecoveryClass::ProviderRefused
        | RecoveryClass::UnknownPermanent
        | RecoveryClass::InternalFailure => (None, None, None, None, None),
    }
}

pub(crate) fn recovery_discriminant(recovery: &RecoveryClass) -> &'static str {
    match recovery {
        RecoveryClass::Cancelled => "cancelled",
        RecoveryClass::Retry(_) => "retry",
        RecoveryClass::Reconcile(_) => "reconcile",
        RecoveryClass::Engine(_) => "engine",
        RecoveryClass::AuthLost => "auth_lost",
        RecoveryClass::NeedsAdminConsent { .. } => "needs_admin_consent",
        RecoveryClass::NeedsPolicyChange => "needs_policy_change",
        RecoveryClass::NoPermission { .. } => "no_permission",
        RecoveryClass::Unsupported(_) => "unsupported",
        RecoveryClass::ClientBug => "client_bug",
        RecoveryClass::ProviderContractViolation => "provider_contract_violation",
        RecoveryClass::ProviderRefused => "provider_refused",
        RecoveryClass::UnknownPermanent => "unknown_permanent",
        RecoveryClass::InternalFailure => "internal_failure",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{
        AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, PrimaryCause, RequestCause,
    };

    #[test]
    fn closed_account_is_terminal_cancellation_without_remediation() {
        let error = AccountError::account_closed(AccountOperation::Send, Protocol::Gmail);
        assert_eq!(error.kind(), &AccountErrorKind::AccountClosed);
        assert_eq!(error.recovery(), &RecoveryClass::Cancelled);
        assert!(error.recovery().is_terminal());
        assert_eq!(error.message_key(), "account.closed");
        assert_eq!(error.scope(), Some(&ErrorScope::Account));
        assert_eq!(error.operation(), Some(AccountOperation::Send));
        assert_eq!(error.protocol(), Some(Protocol::Gmail));
        assert!(error.suggested_remediation().is_none());
        assert!(matches!(
            error.chain().outermost(),
            Cause::State(super::super::cause::StateCause::AccountClosed)
        ));
    }

    fn request_error() -> AccountError {
        AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("raw provider body"),
        }))
        .text(DiagnosticText::user_safe("Check the request."))
        .text(DiagnosticText::support_only("raw provider body"))
        .try_build()
        .expect("valid account error classification")
    }

    #[test]
    fn user_safe_text_filters_support_only_text() {
        let error = request_error();
        let text = error.user_safe_text().collect::<Vec<_>>();

        assert_eq!(text, ["Check the request."]);
    }

    #[test]
    fn telemetry_has_no_free_form_text() {
        let error = request_error()
            .into_builder()
            .request_id("request id with spaces")
            .trace_id("trace\nline")
            .native_code("native code")
            .try_build()
            .expect("valid account error classification");
        let serialized = serde_json::to_value(error.telemetry_fields()).expect("serialize");
        let object = serialized.as_object().expect("telemetry object");
        let mut fields = object.keys().map(String::as_str).collect::<Vec<_>>();
        fields.sort_unstable();

        assert_eq!(
            fields,
            [
                "kind_discriminant",
                "message_key",
                "native_code",
                "operation",
                "partial_commit",
                "protocol",
                "provider",
                "reconcile_actions",
                "reconcile_reason",
                "recovery_discriminant",
                "request_id",
                "retry_disposition",
                "retry_reason",
                "status",
                "throttle_scope",
                "trace_id",
                "transmission_state",
                "unsupported_reason",
            ]
        );
        assert_eq!(object["request_id"], serde_json::Value::Null);
        assert_eq!(object["trace_id"], serde_json::Value::Null);
        assert_eq!(object["native_code"], serde_json::Value::Null);
        let encoded = serialized.to_string();
        assert!(!encoded.contains("raw provider body"));
        assert!(!encoded.contains("request id with spaces"));
    }

    #[test]
    fn unsupported_reason_survives_all_export_tiers() {
        let error = AccountError::unsupported(crate::AccountOperation::Send, crate::Protocol::Jmap)
            .into_builder()
            .unsupported_reason(crate::UnsupportedReason::SubmissionUnavailable)
            .try_build()
            .expect("valid refusal");
        let expected = Some(crate::UnsupportedReason::SubmissionUnavailable);
        assert_eq!(error.telemetry_fields().unsupported_reason, expected);
        assert_eq!(error.support_minimal().unsupported_reason, expected);
        assert_eq!(
            error.support_consented().telemetry.unsupported_reason,
            expected
        );
        assert_eq!(
            error
                .support_internal()
                .consented
                .telemetry
                .unsupported_reason,
            expected
        );
        let exported = serde_json::to_value(error.telemetry_fields()).expect("serialize");
        assert_eq!(exported["unsupported_reason"], "SubmissionUnavailable");
    }

    #[test]
    fn support_exports_have_expected_consent_tiers() {
        let error = request_error();
        let consented = error.support_consented();
        let internal = error.support_internal();

        assert_eq!(consented.user_safe_text, ["Check the request."]);
        assert_eq!(consented.support_text, ["raw provider body"]);
        assert_eq!(internal.chain.len(), 1);
    }

    #[test]
    fn into_builder_round_trips_preserves_observable_fields() {
        let original = request_error();
        let kind_before = original.kind().clone();
        let message_key_before = original.message_key();
        let chain_len_before = original.chain().iter().count();

        let rebuilt = original
            .into_builder()
            .try_build()
            .expect("valid account error classification");

        assert_eq!(rebuilt.kind(), &kind_before);
        assert_eq!(rebuilt.message_key(), message_key_before);
        assert_eq!(rebuilt.chain().iter().count(), chain_len_before);
        let text = rebuilt.user_safe_text().collect::<Vec<_>>();
        assert_eq!(text, ["Check the request."]);
    }

    #[test]
    fn into_builder_push_cause_appends_secondary_evidence() {
        use crate::error::{AttemptCause, Cause, TransmissionState};

        let original = request_error();
        let chain_len_before = original.chain().iter().count();

        let rebuilt = original
            .into_builder()
            .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
            .try_build()
            .expect("valid account error classification");

        assert_eq!(rebuilt.chain().iter().count(), chain_len_before + 1);
        let last_cause_is_attempt = matches!(
            rebuilt.chain().iter().last(),
            Some(Cause::Attempt(AttemptCause {
                transmission_state: TransmissionState::Unsent,
                ..
            }))
        );
        assert!(last_cause_is_attempt);
    }

    #[test]
    fn into_builder_preserves_recovery_overrides() {
        use std::time::Duration;

        use crate::error::{
            AccountOperation, AttemptCause, Cause, PrimaryCause, ReconcileReason, RetryHint,
            ServerCause, ThrottleScope, TransmissionState,
        };

        let hint = RetryHint::After(Duration::from_secs(30));
        let original = AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
            throttle: Default::default(),
            retry_hint: Some(hint),
        }))
        .operation(AccountOperation::Send)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::InFlight,
        )))
        .idempotency_override(false)
        .with_throttle(|throttle| throttle.scope = Some(ThrottleScope::Account))
        .try_build()
        .expect("valid account error classification");

        let rebuilt = original
            .into_builder()
            .push_cause(Cause::Attempt(AttemptCause::new(
                TransmissionState::InFlight,
            )))
            .try_build()
            .expect("valid account error classification");

        assert!(matches!(
            rebuilt.recovery(),
            RecoveryClass::Reconcile(advice)
                if advice.reason == ReconcileReason::ThrottledMidFlight
                    && advice.retry_hint == Some(hint)
                    && advice.throttle_scope == Some(ThrottleScope::Account)
        ));
    }
}

#[cfg(test)]
mod unsupported_constructor_tests {
    use super::*;

    #[test]
    fn unsupported_constructor_classifies_and_preserves_decoration() {
        let operation = AccountOperation::Send;
        let error = AccountError::unsupported(operation, Protocol::Graph)
            .into_builder()
            .scope(ErrorScope::Account)
            .provider(Provider::Microsoft)
            .try_build()
            .expect("valid decorated refusal");
        assert_eq!(error.kind(), &AccountErrorKind::Unsupported(operation));
        assert_eq!(error.operation(), Some(operation));
        assert_eq!(error.protocol(), Some(Protocol::Graph));
        assert_eq!(error.scope(), Some(&ErrorScope::Account));
        assert_eq!(error.provider(), Some(Provider::Microsoft));
        assert_eq!(error.recovery(), &RecoveryClass::Unsupported(operation));
        assert_eq!(error.message_key(), "unsupported.operation");
        assert!(
            matches!(error.chain().outermost(), super::super::cause::Cause::Request(
            super::super::cause::RequestCause::Unsupported { operation: op }
        ) if *op == operation)
        );
    }
}

#[cfg(test)]
mod shared_constructor_tests {
    use super::*;
    use crate::{
        AccessCause, AccountErrorBuilder, AttemptCause, Cause, CursorScope, DiagnosticText,
        EngineDirective, FolderId, PrimaryCause, RequestCause, RequestErrorKind, TransmissionState,
    };

    #[test]
    fn foreign_revocation_preserves_original_source_and_support_evidence() {
        let scope = CursorScope::Folder(FolderId("shared".into()));
        let owner = crate::PrincipalId("delegate".into());
        let original =
            AccountErrorBuilder::new(PrimaryCause::Access(AccessCause::PermissionDenied {
                resource: None,
            }))
            .protocol(Protocol::Ews)
            .provider(Provider::Microsoft)
            .operation(AccountOperation::SyncChanges)
            .status(Some(403))
            .request_id("request")
            .trace_id("trace")
            .native_code("denied")
            .text(DiagnosticText::support_only("original detail"))
            .push_cause(Cause::Attempt(AttemptCause::new(
                TransmissionState::Acknowledged,
            )))
            .try_build()
            .unwrap();
        let revoked = original
            .clone()
            .revoke_foreign_scope(scope.clone(), Some(&owner))
            .into_builder()
            .text(DiagnosticText::support_only("decoration"))
            .try_build()
            .unwrap();
        assert_eq!(
            revoked.recovery(),
            &RecoveryClass::Engine(EngineDirective::DisableScope(scope))
        );
        assert_eq!(revoked.protocol(), original.protocol());
        assert_eq!(revoked.provider(), original.provider());
        assert_eq!(revoked.operation(), original.operation());
        let source = revoked
            .source()
            .unwrap()
            .downcast_ref::<AccountError>()
            .unwrap();
        assert_eq!(source.kind(), original.kind());
        assert_eq!(source.chain(), original.chain());
        assert_eq!(revoked.telemetry_fields().request_id, Some("request"));
        assert_eq!(revoked.telemetry_fields().trace_id, Some("trace"));
        assert_eq!(revoked.telemetry_fields().status, Some(403));
        assert_eq!(revoked.telemetry_fields().native_code, Some("denied"));
        assert!(
            original
                .chain()
                .iter()
                .all(|cause| revoked.chain().iter().any(|retained| retained == cause))
        );
        assert!(
            revoked
                .support_consented()
                .support_text
                .contains(&"original detail")
        );
        let historical =
            AccountErrorBuilder::new(PrimaryCause::Access(AccessCause::PermissionDenied {
                resource: None,
            }))
            .try_build()
            .unwrap()
            .into_builder()
            .push_cause(Cause::Attempt(AttemptCause::new(
                TransmissionState::InFlight,
            )))
            .try_build()
            .unwrap();
        let historical = historical.revoke_foreign_scope(CursorScope::Account, Some(&owner));
        assert_eq!(
            crate::Application::from_leaf(&historical),
            crate::Application::NotApplied
        );
        let primary = original
            .clone()
            .revoke_foreign_scope(CursorScope::Account, None);
        assert_eq!(primary.kind(), original.kind());
        assert_eq!(primary.chain(), original.chain());
        let unrelated = AccountError::unsupported(AccountOperation::Search, Protocol::Imap);
        let unchanged = unrelated
            .clone()
            .revoke_foreign_scope(CursorScope::Account, Some(&owner));
        assert_eq!(unchanged.kind(), unrelated.kind());
        assert_eq!(unchanged.chain(), unrelated.chain());
    }

    #[test]
    fn foreign_revocation_suggests_no_account_remediation() {
        // A removed share must read as "shared folder no longer available"
        // (the scope-revoked message key), never as a prompt to reauthorize
        // or to contact support about the user's own account. The original
        // denial's ContactProviderSupport must not leak through the chain.
        let owner = crate::PrincipalId("delegate".into());
        let original =
            AccountErrorBuilder::new(PrimaryCause::Access(AccessCause::PermissionDenied {
                resource: None,
            }))
            .operation(AccountOperation::SyncChanges)
            .push_cause(Cause::Attempt(AttemptCause::new(
                TransmissionState::Acknowledged,
            )))
            .try_build()
            .unwrap();
        assert!(original.suggested_remediation().is_some());
        let revoked = original
            .revoke_foreign_scope(CursorScope::Folder(FolderId("shared".into())), Some(&owner));
        assert_eq!(revoked.suggested_remediation(), None);
        assert_eq!(revoked.message_key(), "syncstate.scope-revoked");
    }

    #[test]
    fn page_cursor_constructor_targets_only_the_callers_walk() {
        let error = AccountError::invalid_page_cursor(
            AccountOperation::Search,
            Protocol::Imap,
            "stale page",
        );
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
        assert_eq!(error.operation(), Some(AccountOperation::Search));
        assert_eq!(error.protocol(), Some(Protocol::Imap));
        assert!(error.scope().is_none());
        assert!(
            matches!(error.chain().outermost(), Cause::Request(RequestCause::InvalidArgument {
            field: Some("page_cursor"), message: Some(detail),
        }) if detail.as_str() == "stale page")
        );
        assert!(!matches!(error.recovery(), RecoveryClass::Engine(_)));
    }
}
