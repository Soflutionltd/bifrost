use super::*;
use crate::throttle::{ThrottleBucket, account_throttle_wait};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountId, AccountOperation, AttemptCause, AuthCause, Cause,
    CursorScope, EngineDirective, PrimaryCause, RecoveryClass, RetryAdvice, RetryDisposition,
    RetryHint, RetryReason, StateCause, TransmissionState, TransportCause, TransportErrorKind,
};
use std::time::Duration;
use std::time::SystemTime;

fn build_terminal() -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::Auth(AuthCause::Expired))
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid")
}

fn build_engine() -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::State(StateCause::SchemaIncompatible))
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid")
}

fn build_reconcile() -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::Transport(TransportCause::new(
        TransportErrorKind::Network,
        None,
    )))
    .operation(AccountOperation::Send)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .try_build()
    .expect("valid")
}

fn build_retry() -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::Transport(TransportCause::new(
        TransportErrorKind::Network,
        None,
    )))
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid")
}

#[test]
fn stream_dispatch_preserves_absent_provider_timing() {
    let bucket = ThrottleBucket::new();
    let account = AccountId("a".into());
    let now = SystemTime::UNIX_EPOCH;
    match dispatch_stream_failure(build_retry(), &bucket, &account) {
        RecoveryPlan::Retry(advice) => {
            assert_eq!(advice.retry_hint, None);
            assert_eq!(
                retry_delay(&advice, now, HINTLESS_RETRY_WAIT),
                HINTLESS_RETRY_WAIT
            );
        }
        other => panic!("expected retry, got {other:?}"),
    }
    match dispatch_stream_failure(build_reconcile(), &bucket, &account) {
        RecoveryPlan::Reconcile(advice) => {
            assert_eq!(advice.retry_hint, None);
            assert_eq!(
                reconcile_delay(&advice, now, HINTLESS_RETRY_WAIT),
                HINTLESS_RETRY_WAIT
            );
        }
        other => panic!("expected reconcile, got {other:?}"),
    }
    assert!(account_throttle_wait(&bucket, &account).is_none());
}

#[test]
fn stream_dispatch_preserves_engine_and_terminal_decisions() {
    let bucket = ThrottleBucket::new();
    let account = AccountId("a".into());
    assert!(matches!(
        dispatch_stream_failure(build_engine(), &bucket, &account),
        RecoveryPlan::Engine(EngineDirective::SchemaIncompatible)
    ));
    assert!(matches!(
        dispatch_stream_failure(build_terminal(), &bucket, &account),
        RecoveryPlan::Terminal(_)
    ));
}

#[test]
fn plan_recovery_retry_arm() {
    assert!(matches!(
        plan_recovery(build_retry()),
        RecoveryPlan::Retry(_)
    ));
}

#[test]
fn plan_recovery_reconcile_arm() {
    assert!(matches!(
        plan_recovery(build_reconcile()),
        RecoveryPlan::Reconcile(_)
    ));
}

#[test]
fn plan_recovery_engine_arm() {
    assert!(matches!(
        plan_recovery(build_engine()),
        RecoveryPlan::Engine(EngineDirective::SchemaIncompatible)
    ));
}

#[test]
fn plan_recovery_terminal_arm() {
    assert!(matches!(
        plan_recovery(build_terminal()),
        RecoveryPlan::Terminal(_)
    ));
}

#[test]
fn retry_delay_uses_retry_hint_when_present() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let advice = RetryAdvice::new(
        RetryDisposition::SameRequest,
        Some(RetryHint::After(Duration::from_secs(7))),
        RetryReason::Transport,
        None,
    );
    assert_eq!(
        retry_delay(&advice, now, Duration::from_secs(1)),
        Duration::from_secs(7)
    );
}

#[test]
fn retry_delay_uses_fallback_without_hint() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let advice = RetryAdvice::new(
        RetryDisposition::SameRequest,
        None,
        RetryReason::Transport,
        None,
    );
    assert_eq!(
        retry_delay(&advice, now, Duration::from_secs(3)),
        Duration::from_secs(3)
    );
}

#[test]
fn engine_directive_target_scope_reaches_sync_with_account_scope() {
    let scope = CursorScope::Account;
    assert_eq!(
        EngineDirective::RestartScope(scope.clone()).target_scope(),
        Some(scope.clone())
    );
    assert_eq!(
        EngineDirective::DowngradeCapabilityForScope(scope.clone()).target_scope(),
        Some(scope.clone())
    );
    assert_eq!(
        EngineDirective::DisableScope(scope.clone()).target_scope(),
        Some(scope)
    );
    assert_eq!(EngineDirective::RestartAccount.target_scope(), None);
    assert_eq!(EngineDirective::SchemaIncompatible.target_scope(), None);
}

#[test]
fn reconcile_delay_falls_back_when_no_hint_is_carried() {
    // A transport drop mid-send reconciles without any provider
    // timing signal; the caller's fallback is what must apply.
    let error = build_reconcile();
    let RecoveryClass::Reconcile(advice) = error.recovery() else {
        panic!("in-flight non-idempotent transport drop must reconcile");
    };
    assert_eq!(advice.retry_hint, None);

    assert_eq!(
        reconcile_delay(advice, SystemTime::now(), Duration::from_secs(1)),
        Duration::from_secs(1)
    );
}
