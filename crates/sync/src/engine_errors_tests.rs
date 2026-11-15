use super::*;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, AttemptCause, AuthCause,
    AuthErrorKind, Cause, CursorScope, EngineDirective, ErrorScope, RecoveryClass, StateCause,
    SyncStateErrorKind, TransmissionState, TransportCause, TransportErrorKind, TransportKind,
};

fn build_terminal() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Authentication(AuthErrorKind::Expired),
        Cause::Auth(AuthCause::Expired),
    )
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid")
}

fn build_engine() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid")
}

fn build_reconcile() -> AccountError {
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

fn build_retry() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(TransportKind::Network, None)),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid")
}
/// The two contract violations say different things.
///
/// They share a classification and a terminal outcome, and for a while they
/// shared a message too - so a live batch carrying a BACKFILL checkpoint was
/// reported as "a PageBoundary::Partial batch carried a checkpoint", sending
/// whoever read the log after the wrong thing entirely. The boundary was fine;
/// the checkpoint belonged to a lane that stream does not publish on.
///
/// The same holds for the inventory checkpoint whose withheld state
/// disagrees with its coverage: it rides the boundary helper but is not a
/// partial-boundary fault, and must not read as one.
#[test]
fn the_contract_violations_do_not_share_a_message() {
    let scope = CursorScope::Account;
    let boundary = format!(
        "{:?}",
        batch_boundary_violation(
            bifrost_types::BatchBoundaryError::PartialCarriesCheckpoint,
            None,
            AccountOperation::SyncChanges,
            &scope
        )
        .chain()
    );
    let disagreement = format!(
        "{:?}",
        batch_boundary_violation(
            bifrost_types::BatchBoundaryError::CheckpointDisagreesWithCoverage,
            None,
            AccountOperation::SyncInventory,
            &scope
        )
        .chain()
    );
    let backfill = format!(
        "{:?}",
        backfill_checkpoint_on_changes(None, AccountOperation::SyncChanges, &scope).chain()
    );
    assert_ne!(
        boundary, backfill,
        "an operator has to be able to tell a bad boundary from a checkpoint on the \
         wrong lane"
    );
    assert_ne!(boundary, disagreement);
    assert!(
        backfill.contains("backfill checkpoint"),
        "and the message must name what actually happened: {backfill}"
    );
    assert!(
        disagreement.contains("withheld"),
        "and the message must name what actually happened: {disagreement}"
    );
}

/// Both violations are the `Account` implementation's, not the
/// provider's: they classify `Internal(AccountContract)`, stop the scope,
/// point at the implementation, and keep the protocol the offending
/// checkpoint names.
#[test]
fn a_contract_violation_blames_the_implementation_not_the_provider() {
    let scope = CursorScope::Account;
    let checkpoint = bifrost_types::Checkpoint::Change(bifrost_types::ChangeCursor {
        scope: scope.clone(),
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Imap,
            envelope_version: bifrost_types::PayloadVersion::new(1),
            bytes: Vec::new(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(
            bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
        ),
    });
    for error in [
        batch_boundary_violation(
            bifrost_types::BatchBoundaryError::PartialCarriesCheckpoint,
            Some(&checkpoint),
            AccountOperation::SyncChanges,
            &scope,
        ),
        backfill_checkpoint_on_changes(Some(&checkpoint), AccountOperation::SyncChanges, &scope),
    ] {
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Internal(bifrost_types::InternalErrorKind::AccountContract)
        );
        assert_eq!(error.recovery(), &RecoveryClass::InternalFailure);
        assert_eq!(
            error.suggested_remediation(),
            Some(&bifrost_types::RemediationAction::ReportBug)
        );
        assert_eq!(error.protocol(), Some(bifrost_types::Protocol::Imap));
    }
}

#[test]
fn restart_scope_error_derives_engine_restart_scope() {
    let scope = CursorScope::Account;
    let err = restart_scope_error(scope.clone(), AccountOperation::SyncChanges);
    assert!(matches!(
        err.recovery(),
        RecoveryClass::Engine(EngineDirective::RestartScope(got)) if *got == scope
    ));
}

#[test]
fn exhausted_establishment_requires_operator_attention() {
    for scope in [
        CursorScope::Account,
        CursorScope::Folder(bifrost_types::FolderId("inbox".into())),
    ] {
        let err = establish_failure_error(scope.clone(), AccountOperation::EstablishCursor);
        assert!(matches!(
            err.recovery(),
            RecoveryClass::Engine(EngineDirective::OperatorOverrideRequired { .. })
        ));
        assert_eq!(err.scope(), Some(&ErrorScope::Cursor(scope)));
    }
}

#[test]
fn exhausted_actionable_errors_become_operator_attention() {
    let scope = CursorScope::Folder(bifrost_types::FolderId("inbox".into()));
    for last_error in [build_retry(), build_reconcile(), build_engine()] {
        let exhausted =
            recovery_exhaustion_error(last_error, scope.clone(), AccountOperation::EstablishCursor);
        assert!(matches!(
            exhausted.recovery(),
            RecoveryClass::Engine(EngineDirective::OperatorOverrideRequired { .. })
        ));
        assert_eq!(exhausted.scope(), Some(&ErrorScope::Cursor(scope.clone())));
        assert!(exhausted.throttle_budgets().is_empty());
    }

    let terminal = build_terminal();
    let preserved = recovery_exhaustion_error(
        terminal.clone(),
        CursorScope::Account,
        AccountOperation::EstablishCursor,
    );
    assert_eq!(preserved.message_key(), terminal.message_key());
    assert!(preserved.recovery().is_terminal());
}

#[test]
fn envelope_classification_uses_provenance_before_authorizing_schema_reset() {
    let scope = CursorScope::Folder(bifrost_types::FolderId("inbox".into()));
    let mut cursor = bifrost_types::ChangeCursor {
        scope: scope.clone(),
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Imap,
            envelope_version: bifrost_types::PayloadVersion::new(1),
            bytes: Vec::new(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(
            bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION + 1,
        ),
    };
    let fresh = classify_envelope(
        &cursor,
        CursorOrigin::Fresh,
        AccountOperation::EstablishCursor,
        &scope,
    )
    .expect_err("fresh unsupported envelope");
    assert_eq!(
        fresh.kind(),
        &AccountErrorKind::Internal(bifrost_types::InternalErrorKind::AccountContract),
    );
    assert!(fresh.recovery().is_terminal());
    assert_eq!(fresh.scope(), Some(&ErrorScope::Cursor(scope.clone())));
    let stored = classify_envelope(
        &cursor,
        CursorOrigin::Stored,
        AccountOperation::EstablishCursor,
        &scope,
    )
    .expect_err("stored unsupported envelope");
    assert!(matches!(
        stored.recovery(),
        RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
    ));
    cursor.envelope_version =
        bifrost_types::TraitEnvelopeVersion::new(bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION);
    for origin in [CursorOrigin::Fresh, CursorOrigin::Stored] {
        assert!(
            classify_envelope(&cursor, origin, AccountOperation::EstablishCursor, &scope).is_ok()
        );
    }
}

#[test]
fn cursor_decode_failure_routes_to_schema_incompatible() {
    let err = cursor_decode_failure(AccountOperation::SyncChanges);
    assert!(matches!(
        err.recovery(),
        RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
    ));
}
