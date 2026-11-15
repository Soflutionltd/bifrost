//! Recovery plan projection, failure dispatch and provider delay resolution.

use crate::throttle::ThrottleBucket;
use bifrost_types::{
    AccountError, AccountId, EngineDirective, Fatal, ReconcileAdvice, RecoveryClass, RetryAdvice,
};
use std::time::{Duration, SystemTime};

/// Closed four-arm plan derived from a `RecoveryClass`. Every engine
/// dispatch site routes through [`plan_recovery`]. The upstream enum is
/// non-exhaustive, so unknown nonterminal advice fails closed.
#[derive(Debug, Clone)]
pub(crate) enum RecoveryPlan {
    Retry(RetryAdvice),
    Reconcile(ReconcileAdvice),
    Engine(EngineDirective),
    Terminal(Fatal),
}

/// Collapse an `AccountError`'s recovery into a [`RecoveryPlan`]. The
/// helper consumes the error so the terminal arm owns the value handed
/// to [`Fatal::try_from`]; engine sites that need to retain a copy for
/// telemetry should clone before calling.
///
/// Match the advice directly, without relying on parallel classification helpers.
/// The upstream enum is non-exhaustive. A future nonterminal variant must not
/// panic a worker or silently authorize replay; stop for operator attention.
pub(crate) fn plan_recovery(error: AccountError) -> RecoveryPlan {
    match error.recovery().clone() {
        RecoveryClass::Retry(advice) => RecoveryPlan::Retry(advice),
        RecoveryClass::Reconcile(advice) => RecoveryPlan::Reconcile(advice),
        RecoveryClass::Engine(directive) => RecoveryPlan::Engine(directive),
        _ => match Fatal::try_from(error) {
            Ok(fatal) => RecoveryPlan::Terminal(fatal),
            Err(_) => RecoveryPlan::Engine(EngineDirective::OperatorOverrideRequired {
                reason: bifrost_types::OperatorOverrideReason::UnsupportedRecoveryClassification,
            }),
        },
    }
}

/// The engine's one wait for a retryable or reconcilable failure that carries
/// no provider timing: long enough that a failed read cannot spin, short
/// enough that a transient outage recovers promptly. Provider hints override it.
pub(crate) const HINTLESS_RETRY_WAIT: Duration = bifrost_types::HINTLESS_HOLD_WAIT;

/// Shared bound for waiting on a replacement connection. A queued repair can
/// be paused or exhaust its budget without changing the incarnation.
pub(crate) const REOPEN_WAIT: Duration = Duration::from_secs(30);

/// Dispatch a failed stream before applying its lane's execution policy.
///
/// Retrying lanes use one second without a hint to prevent a failed read from
/// spinning. Provider hints remain unchanged; lifecycle and transport reconnect
/// backoffs may impose a larger local floor. Record every failed leg, even when the original
/// operation is terminal: stopping replay does not cancel a sibling's throttle.
/// An enterprise mail user expects a transient outage to retry without flooding
/// the server, and unrelated folders to keep progressing.
///
/// Publication stays with the producer: drive/fusion already publishes its
/// termination, whereas push and lifecycle own their first publication. Moving
/// publication here would duplicate drive events. Likewise generation stamps
/// stay at the stream owner; an injected watch has no incarnation to sample.
/// Engine directives stop the failed drive and are forwarded by that owner;
/// account-wide directives use REOPEN_WAIT before trying the old connection.
/// Terminal means no automatic retry, with parking versus item accounting owned
/// by the lane. Campaign replay/read-back decisions must retain the advice.
/// A synchronous injected watch can only warn on retry: its external owner
/// reconnects it. The reconciler skips hintless/unkeyed failures until ordinary
/// polling rather than blocking all sibling scopes or immediately requeuing.
/// The reopen listener records advice but never sleeps without an operation to
/// retry. Lifecycle's separate pure policy projection decides whether a
/// generation change is expected; only its timed dispatch records effects.
pub(crate) fn dispatch_stream_failure(
    error: AccountError,
    bucket: &ThrottleBucket,
    account: &AccountId,
) -> RecoveryPlan {
    observe_failure_throttles(bucket, account, &error);
    let view = error.telemetry_fields();
    tracing::debug!(
        target: "bifrost.sync.recovery",
        account = ?account,
        scope = ?error.scope(),
        kind = view.kind_discriminant,
        message_key = view.message_key,
        recovery = view.recovery_discriminant,
        operation = ?view.operation,
        "dispatching stream failure"
    );
    // Timing fallback belongs to the lane that actually retries. Preserve None
    // so consumers can distinguish provider advice from local pacing.
    plan_recovery(error)
}

/// Observe primary, sibling and cleanup throttles through the same owner.
pub(crate) fn observe_failure_throttles(
    bucket: &ThrottleBucket,
    account: &AccountId,
    error: &AccountError,
) {
    bucket.observe(account, error);
}

/// Resolve the effective sleep an engine path should observe before
/// retrying. `now` is the caller's notion of the current wall clock;
/// `fallback` is the duration used when the advice carries no hint.
#[must_use]
pub(crate) fn retry_delay(advice: &RetryAdvice, now: SystemTime, fallback: Duration) -> Duration {
    advice
        .retry_hint
        .map_or(fallback, |hint| hint.min_delay(now))
}

/// The reconcile counterpart of [`retry_delay`]. A throttle that lands
/// mid-flight on a non-idempotent operation classifies as
/// `Reconcile(ThrottledMidFlight)` rather than `Retry`, and it carries
/// the provider's retry hint; honoring it keeps the read-back probe
/// from re-entering the same rate limit it just hit.
#[must_use]
pub(crate) fn reconcile_delay(
    advice: &ReconcileAdvice,
    now: SystemTime,
    fallback: Duration,
) -> Duration {
    advice
        .retry_hint
        .map_or(fallback, |hint| hint.min_delay(now))
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;
