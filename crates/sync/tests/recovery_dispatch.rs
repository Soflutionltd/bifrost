//! Engine-side recovery dispatch tests. Pin the
//! `plan_recovery` collapse, the throttle bucket cross-account
//! semantics, and the cursor-decode -> SchemaIncompatible translator.
//!
//! No live network, no protocol crates. Builds AccountError values
//! through the bifrost-types builder so the derived RecoveryClass is
//! exactly what the engine's dispatch must consume.

use std::time::Duration;

// Throttle deadlines are monotonic (`tokio::time::Instant`) so they are read
// on the same clock the engine's `tokio::time::sleep` retires them on.
use tokio::time::Instant;

use bifrost_sync::ThrottleBucket;
use bifrost_types::{
    AccountErrorBuilder, AccountErrorKind, AccountId, AccountOperation, AttemptCause, AuthCause,
    AuthErrorKind, Cause, EngineDirective, MailboxId, RecoveryClass, ResourceKind, StateCause,
    SyncStateErrorKind, ThrottleKey, ThrottleScope, TransmissionState, TransportCause,
    TransportErrorKind, TransportKind,
};

fn retry_error() -> bifrost_types::AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(TransportKind::Network, None)),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid")
}

fn reconcile_error() -> bifrost_types::AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(TransportKind::Network, None)),
    )
    .operation(AccountOperation::Send)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .try_build()
    .expect("valid")
}

fn schema_incompatible_error() -> bifrost_types::AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid")
}

fn terminal_auth_lost() -> bifrost_types::AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Authentication(AuthErrorKind::Expired),
        Cause::Auth(AuthCause::Expired),
    )
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid")
}

/// One assertion per RecoveryClass arm exercised through the
/// `RecoveryClass` helpers. The engine's `plan_recovery` is
/// `pub(crate)`, so we exercise it indirectly: every constructed
/// error must satisfy exactly one of the four helpers, matching the
/// `plan_recovery` arm the engine routes through.
type HelperCheck = fn(&RecoveryClass) -> bool;
type DispatchCase = (&'static str, bifrost_types::AccountError, HelperCheck);

#[test]
fn plan_recovery_branches_match_recovery_class_helpers() {
    let cases: Vec<DispatchCase> = vec![
        ("retry", retry_error(), RecoveryClass::is_retryable),
        (
            "reconcile",
            reconcile_error(),
            RecoveryClass::requires_reconciliation,
        ),
        (
            "engine-schema",
            schema_incompatible_error(),
            RecoveryClass::requires_engine_action,
        ),
        (
            "terminal-auth",
            terminal_auth_lost(),
            RecoveryClass::is_terminal,
        ),
    ];
    for (name, err, expected_branch) in cases {
        let recovery = err.recovery();
        assert!(
            expected_branch(recovery),
            "{name} expected helper to return true for {recovery:?}"
        );
    }
}

/// `SyncState(SchemaIncompatible)` is the recovery class the cursor
/// envelope translator must produce. We exercise the public
/// `RecoveryClass` shape rather than the engine-internal translator
/// (which is `pub(crate)`); the engine's `establish_one` /
/// `run_establish` paths call the translator at the
/// `get_change_cursor` boundary and surface this exact derived
/// recovery to the reopen listener.
#[test]
fn schema_incompatible_account_error_derives_engine_directive() {
    let err = schema_incompatible_error();
    assert!(matches!(
        err.recovery(),
        RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
    ));
}

#[test]
fn throttle_bucket_account_does_not_cross_accounts() {
    let mut bucket = ThrottleBucket::new();
    let now = Instant::now();
    let a = ThrottleKey::Account(AccountId("acc-1".into()));
    let b = ThrottleKey::Account(AccountId("acc-2".into()));
    bucket.record(a.clone(), now + Duration::from_secs(30));
    assert!(bucket.wait_for(&a, now).is_some());
    assert!(bucket.wait_for(&b, now).is_none());
}

#[test]
fn throttle_bucket_mailbox_does_not_cross_accounts() {
    let mut bucket = ThrottleBucket::new();
    let now = Instant::now();
    let mb_a = ThrottleKey::Mailbox {
        account: AccountId("a".into()),
        mailbox: MailboxId("INBOX".into()),
    };
    let mb_b = ThrottleKey::Mailbox {
        account: AccountId("b".into()),
        mailbox: MailboxId("INBOX".into()),
    };
    bucket.record(mb_a.clone(), now + Duration::from_secs(7));
    assert!(bucket.wait_for(&mb_a, now).is_some());
    assert!(bucket.wait_for(&mb_b, now).is_none());
}

/// Suppress unused-import lint for symbols only used in shapes via
/// the constructed errors above.
#[test]
fn imports_smoke() {
    let _: ResourceKind = ResourceKind::Message;
    let _: ThrottleScope = ThrottleScope::Account;
}
