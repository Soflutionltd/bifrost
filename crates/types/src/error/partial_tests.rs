use super::*;
use crate::error::*;
use std::time::Duration;

fn throttle() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Server(ServerErrorKind::RateLimited),
        Cause::Server(ServerCause::RateLimited {
            retry_hint: Some(RetryHint::After(Duration::from_secs(37))),
        }),
    )
    .operation(AccountOperation::EventUpdate)
    .throttle_scope(ThrottleScope::Mailbox)
    .throttle_budget(ThrottleBudget::new("budget"))
    .try_build()
    .unwrap()
}
fn attach(leaf: AccountError, replay: StepReplay, effect: StepEffect) -> AccountError {
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Updated,
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::from_leaf(&leaf),
    ));
    record.committed(effect, StepTargets::complete(vec![]), replay);
    leaf.into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap()
}
fn actions(error: &AccountError) -> &[ReconcileAction] {
    match error.recovery() {
        RecoveryClass::Reconcile(a) => &a.guidance.actions,
        other => panic!("expected reconcile, got {other:?}"),
    }
}
#[test]
fn partial_commit_absent_keeps_leaf_recovery() {
    let error = throttle();
    assert!(error.partial_commit().is_none());
    assert_eq!(error.recovery(), error.leg_recovery());
}
#[test]
fn partial_commit_blocking_prefix_protects_a_throttled_leg() {
    let leaf = throttle();
    let expected = leaf.recovery().clone();
    let error = attach(leaf, StepReplay::Blocking, StepEffect::Moved);
    assert_eq!(error.leg_recovery(), &expected);
    assert_eq!(
        actions(&error),
        [ReconcileAction::CheckTarget, ReconcileAction::ObserveOnly]
    );
    assert_eq!(error.throttle_budgets(), [ThrottleBudget::new("budget")]);
    let RecoveryClass::Reconcile(a) = error.recovery() else {
        unreachable!()
    };
    assert_eq!(a.reason, ReconcileReason::PartialCommit);
    assert_eq!(
        a.retry_hint,
        Some(RetryHint::After(Duration::from_secs(37)))
    );
    assert_eq!(a.throttle_scope, Some(ThrottleScope::Mailbox));
}
#[test]
fn partial_commit_repeatable_prefix_keeps_leg_retry() {
    let leaf = throttle();
    let expected = leaf.recovery().clone();
    let error = attach(leaf, StepReplay::Repeatable, StepEffect::Updated);
    assert_eq!(error.recovery(), &expected);
    assert_eq!(
        error.partial_commit().unwrap().plan().replay,
        ReplaySafety::Safe
    );
}
#[test]
fn partial_commit_local_refusal_after_blocking_prefix_is_terminal() {
    let leaf = AccountError::unsupported(AccountOperation::EventUpdate, Protocol::Gmail);
    let error = attach(leaf.clone(), StepReplay::Blocking, StepEffect::Moved);
    assert_eq!(error.recovery(), leaf.recovery());
    assert_eq!(
        error.partial_commit().unwrap().plan().replay,
        ReplaySafety::Never
    );
}
#[test]
fn partial_commit_uncertain_blocking_leg_allows_send_after_observation() {
    let leaf = AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause {
            kind: TransportKind::Network,
            message: None,
        }),
    )
    .operation(AccountOperation::Send)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .try_build()
    .unwrap();
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Submitted,
        StepTargets::unknown(),
        StepReplay::Blocking,
        Application::from_leaf(&leaf),
    ));
    record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Repeatable,
    );
    let error = leaf
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(
        error.partial_commit().unwrap().plan().replay,
        ReplaySafety::AfterObservation
    );
    assert_eq!(
        actions(&error),
        [
            ReconcileAction::CheckTarget,
            ReconcileAction::DedupeByClientId
        ]
    );
}
#[test]
fn partial_commit_survives_into_builder_and_refuses_a_second_record() {
    let error = attach(throttle(), StepReplay::Blocking, StepEffect::Moved);
    let rebuilt = error
        .clone()
        .into_builder()
        .text(DiagnosticText::support_only("decoration"))
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::InFlight,
        )))
        .try_build()
        .unwrap();
    assert_eq!(error.recovery(), rebuilt.recovery());
    assert!(rebuilt.partial_commit().is_some());
    assert_eq!(Application::from_leaf(&rebuilt), Application::NotApplied);
    let second = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    assert_eq!(
        rebuilt
            .into_builder()
            .partial_commit(second)
            .try_build()
            .unwrap_err(),
        AccountErrorBuildError::PartialCommitAlreadyAttached
    );
}
#[test]
fn partial_commit_succeeded_compensation_clears_only_named_steps() {
    let leaf = throttle();
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Submitted,
        StepTargets::unknown(),
        StepReplay::Blocking,
        Application::from_leaf(&leaf),
    ));
    let id = record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Blocking,
    );
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![id],
        CompensationOutcome::Succeeded,
    );
    let error = leaf
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(error.recovery(), error.leg_recovery());
    assert!(error.partial_commit().unwrap().plan().gating.is_empty());
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    let reversed = record.committed(
        StepEffect::Created,
        StepTargets::complete(vec![]),
        StepReplay::Blocking,
    );
    let standing = record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Blocking,
    );
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![reversed],
        CompensationOutcome::Succeeded,
    );
    let error = throttle()
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(
        error.partial_commit().unwrap().plan().gating,
        vec![standing]
    );
    assert_eq!(
        error.partial_commit().unwrap().plan().replay,
        ReplaySafety::Never
    );
    assert_eq!(error.partial_commit().unwrap().steps().len(), 2);
}
#[test]
fn partial_commit_unidentified_blocking_create_gates_before_a_barrier() {
    let leaf = AccountError::unsupported(AccountOperation::Send, Protocol::Graph);
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Blocking,
    );
    let error = leaf
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(
        actions(&error),
        [
            ReconcileAction::CheckTarget,
            ReconcileAction::DedupeByClientId,
            ReconcileAction::ObserveOnly
        ]
    );
}
#[test]
fn partial_commit_dedupe_follows_the_record_not_the_table() {
    for effect in [
        StepEffect::Moved,
        StepEffect::Copied,
        StepEffect::Deleted,
        StepEffect::Created,
        StepEffect::Submitted,
    ] {
        let error = attach(throttle(), StepReplay::Blocking, effect);
        assert_eq!(
            actions(&error).contains(&ReconcileAction::DedupeByClientId),
            matches!(effect, StepEffect::Created | StepEffect::Submitted)
        );
    }
    let leaf = classified(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::Jmap(JmapMethod::ServerPartialFail)),
    );
    assert!(
        matches!(leaf.recovery(),RecoveryClass::Reconcile(a) if a.guidance.actions.contains(&ReconcileAction::DedupeByClientId))
    );
    let error = attach(leaf, StepReplay::Blocking, StepEffect::Moved);
    assert_eq!(
        actions(&error),
        [ReconcileAction::CheckTarget, ReconcileAction::ObserveOnly]
    );
}
#[test]
fn partial_commit_throttle_observations_cover_every_leg() {
    let leaf = throttle();
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    let committed = record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Repeatable,
    );
    record.failed_sibling(
        Some(StepEffect::Updated),
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::from_leaf(&leaf),
        leaf.clone(),
    );
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![committed],
        CompensationOutcome::from_error(leaf.clone()),
    );
    let error = leaf
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(error.throttle_observations().len(), 3);
    assert_eq!(error.support_internal().partial_commit_chains.len(), 2);
}
#[test]
fn partial_commit_step_ids_past_u16_stay_unique() {
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    let mut ids = Vec::new();
    for _ in 0..70_000 {
        ids.push(record.committed(
            StepEffect::Updated,
            StepTargets::unknown(),
            StepReplay::Repeatable,
        ));
    }
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![*ids.last().unwrap()],
        CompensationOutcome::Succeeded,
    );
    let error = throttle()
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    let record = error.partial_commit().unwrap();
    for id in ids {
        assert_eq!(record.step(id).unwrap().id(), id);
    }
}
#[test]
fn partial_commit_telemetry_summary_has_no_targets() {
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    for _ in 0..3 {
        record.committed(
            StepEffect::Moved,
            StepTargets::complete(vec![TargetRef::new(
                TargetRole::Subject,
                ErrorScope::CloudFile {
                    id: "private-id".into(),
                },
            )]),
            StepReplay::Blocking,
        );
    }
    let error = throttle()
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(
        error.telemetry_fields().partial_commit.unwrap().committed,
        [(StepEffect::Moved, 3)]
    );
    assert!(
        !serde_json::to_string(&error.telemetry_fields())
            .unwrap()
            .contains("private-id")
    );
    assert!(
        serde_json::to_string(&error.support_consented())
            .unwrap()
            .contains("private-id")
    );
}

fn classified(kind: AccountErrorKind, cause: Cause) -> AccountError {
    AccountErrorBuilder::new(kind, cause)
        .operation(AccountOperation::Send)
        .try_build()
        .unwrap()
}
#[test]
fn partial_commit_auth_and_permission_legs_stay_barriers() {
    for leaf in [
        classified(
            AccountErrorKind::Authentication(AuthErrorKind::Revoked),
            Cause::Auth(AuthCause::Revoked),
        ),
        classified(
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied { resource: None }),
        ),
    ] {
        let error = attach(leaf.clone(), StepReplay::Blocking, StepEffect::Copied);
        assert_eq!(error.recovery(), leaf.recovery());
    }
}
#[test]
fn partial_commit_unreadable_blocking_leg_requires_a_check() {
    let leaf = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: None,
        }),
    )
    .operation(AccountOperation::Send)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .try_build()
    .unwrap();
    // The raise is legal: an Acknowledged unreadable answer is not Unsent.
    assert_eq!(Application::from_leaf(&leaf), Application::NotApplied);
    let record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Submitted,
        StepTargets::unknown(),
        StepReplay::Blocking,
        Application::Uncertain,
    ));
    let error = leaf
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(
        actions(&error),
        [
            ReconcileAction::CheckTarget,
            ReconcileAction::DedupeByClientId,
            ReconcileAction::ObserveOnly
        ]
    );
    assert!(matches!(
        error.recovery(),
        RecoveryClass::Reconcile(a) if a.reason == ReconcileReason::PartialCompletionSignal
    ));
    assert_eq!(
        error.partial_commit().unwrap().plan().replay,
        ReplaySafety::Never
    );
}
#[test]
fn partial_commit_mixed_sibling_prerequisites_keep_both_in_plan() {
    let leaf = classified(
        AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient),
        Cause::Auth(AuthCause::RefreshTransient),
    );
    let conflict = classified(
        AccountErrorKind::ConcurrencyConflict,
        Cause::State(StateCause::ConcurrencyConflict),
    );
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Updated,
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::from_leaf(&leaf),
    ));
    record.failed_sibling(
        Some(StepEffect::Updated),
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::from_leaf(&conflict),
        conflict,
    );
    let error = leaf
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    let plan = error.partial_commit().unwrap().plan();
    assert!(plan.prerequisites.auth_refresh && plan.prerequisites.state_refresh);
    assert!(
        matches!(error.recovery(), RecoveryClass::Retry(a) if a.disposition == RetryDisposition::AfterStateRefresh)
    );
}
#[test]
fn partial_commit_distinct_sibling_directives_are_all_planned() {
    let leaf = classified(
        AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
        Cause::State(StateCause::CapabilityChanged { delta: None }),
    );
    let sibling = classified(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    );
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.failed_sibling(
        None,
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::NotApplicable,
        sibling,
    );
    let error = leaf
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(error.partial_commit().unwrap().plan().directives.len(), 2);
}
#[test]
fn partial_commit_directive_does_not_lift_never() {
    let leaf = classified(
        AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
        Cause::State(StateCause::CapabilityChanged { delta: None }),
    );
    let error = attach(leaf, StepReplay::Blocking, StepEffect::Moved);
    assert!(matches!(error.recovery(), RecoveryClass::Engine(_)));
    assert_eq!(
        error.partial_commit().unwrap().plan().replay,
        ReplaySafety::Never
    );
}
#[test]
fn partial_commit_terminal_sibling_bars_reassertion() {
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Updated,
        StepTargets::unknown(),
        StepReplay::Blocking,
        Application::Uncertain,
    ));
    record.failed_sibling(
        None,
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::NotApplicable,
        AccountError::unsupported(AccountOperation::Send, Protocol::Jmap),
    );
    let error = throttle()
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(
        actions(&error),
        [ReconcileAction::CheckTarget, ReconcileAction::ObserveOnly]
    );
}
#[test]
fn partial_commit_permission_barrier_outranks_gating() {
    let permission = classified(
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
        Cause::Access(AccessCause::PermissionDenied { resource: None }),
    );
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Submitted,
        StepTargets::unknown(),
        StepReplay::Blocking,
        Application::Uncertain,
    ));
    record.failed_sibling(
        None,
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::NotApplicable,
        permission,
    );
    let error = throttle()
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert!(matches!(
        error.recovery(),
        RecoveryClass::NoPermission { .. }
    ));
    assert!(!error.partial_commit().unwrap().plan().gating.is_empty());
}
#[test]
fn partial_commit_internal_failure_after_send_is_a_barrier() {
    let leaf = AccountErrorBuilder::new(
        AccountErrorKind::Internal(InternalErrorKind::Panicked),
        Cause::Internal(InternalCause::new(InternalErrorKind::Panicked, None)),
    )
    .operation(AccountOperation::Send)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .try_build()
    .unwrap();
    let record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Submitted,
        StepTargets::unknown(),
        StepReplay::Blocking,
        Application::from_leaf(&leaf),
    ));
    let error = leaf
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert!(actions(&error).contains(&ReconcileAction::ObserveOnly));
}
#[test]
fn partial_commit_projected_retry_keeps_quota_reason() {
    let leaf = classified(
        AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
        Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
    );
    let error = attach(leaf, StepReplay::Repeatable, StepEffect::Created);
    assert!(
        matches!(error.recovery(), RecoveryClass::Retry(a) if a.reason == RetryReason::QuotaExhausted)
    );
}
#[test]
fn partial_commit_proven_refusal_survives_every_cleanup_outcome() {
    for leaf in [
        throttle(),
        AccountError::unsupported(AccountOperation::Send, Protocol::Jmap),
    ] {
        for outcome in [
            CompensationOutcome::Succeeded,
            CompensationOutcome::Failed(throttle()),
            CompensationOutcome::Unconfirmed(throttle()),
        ] {
            let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
                StepEffect::Submitted,
                StepTargets::unknown(),
                StepReplay::Blocking,
                Application::from_leaf(&leaf),
            ));
            let id = record.committed(
                StepEffect::Created,
                StepTargets::unknown(),
                StepReplay::Repeatable,
            );
            record.compensation(
                StepEffect::Deleted,
                StepTargets::unknown(),
                vec![id],
                outcome,
            );
            let error = leaf
                .clone()
                .into_builder()
                .partial_commit(record)
                .try_build()
                .unwrap();
            assert_eq!(error.recovery(), leaf.recovery());
        }
    }
}
#[test]
fn partial_commit_effectless_read_after_blocking_create_is_protected() {
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.committed(
        StepEffect::Created,
        StepTargets::complete(vec![]),
        StepReplay::Blocking,
    );
    let error = throttle()
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    assert_eq!(
        actions(&error),
        [
            ReconcileAction::CheckTarget,
            ReconcileAction::DedupeByClientId,
            ReconcileAction::ObserveOnly
        ]
    );
}

#[test]
fn partial_commit_application_from_leaf_always_validates() {
    let cases = vec![
        (
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        ),
        (
            AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient),
            Cause::Auth(AuthCause::RefreshTransient),
        ),
        (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied { resource: None }),
        ),
        (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
        ),
        (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
        ),
        (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("bad request"),
            }),
        ),
        (
            AccountErrorKind::NotFound(ResourceKind::Message),
            Cause::Request(RequestCause::NotFound {
                what: ResourceKind::Message,
                id: None,
            }),
        ),
        (
            AccountErrorKind::Unsupported(AccountOperation::Send),
            Cause::Request(RequestCause::Unsupported {
                operation: AccountOperation::Send,
            }),
        ),
        (
            AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
            Cause::Wire(WireCause::Jmap(JmapMethod::ServerPartialFail)),
        ),
        (
            AccountErrorKind::Internal(InternalErrorKind::Panicked),
            Cause::Internal(InternalCause::new(InternalErrorKind::Panicked, None)),
        ),
    ];
    for (kind, cause) in cases {
        for attempt in [
            None,
            Some(TransmissionState::Unsent),
            Some(TransmissionState::InFlight),
            Some(TransmissionState::Acknowledged),
        ] {
            for op in [AccountOperation::Send, AccountOperation::UpdateFlags] {
                let mut builder =
                    AccountErrorBuilder::new(kind.clone(), cause.clone()).operation(op);
                if let Some(state) = attempt {
                    builder = builder.push_cause(Cause::Attempt(AttemptCause::new(state)));
                }
                let leaf = match builder.try_build() {
                    Ok(e) => e,
                    Err(AccountErrorBuildError::TransportAcknowledged) => continue,
                    Err(e) => panic!("{e}"),
                };
                let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
                    StepEffect::Submitted,
                    StepTargets::unknown(),
                    StepReplay::Blocking,
                    Application::from_leaf(&leaf),
                ));
                let id = record.committed(
                    StepEffect::Created,
                    StepTargets::unknown(),
                    StepReplay::Repeatable,
                );
                record.compensation(
                    StepEffect::Deleted,
                    StepTargets::unknown(),
                    vec![id],
                    CompensationOutcome::from_error(leaf.clone()),
                );
                leaf.into_builder()
                    .partial_commit(record)
                    .try_build()
                    .unwrap();
            }
        }
    }
}
#[test]
fn partial_commit_validation_rejects_inconsistent_records() {
    let build = |record| {
        throttle()
            .into_builder()
            .partial_commit(record)
            .try_build()
            .unwrap_err()
    };
    assert_eq!(
        build(PartialCommitBuilder::new(PrimaryLeg::effectless(
            StepTargets::unknown()
        ))),
        AccountErrorBuildError::PartialCommitEmpty
    );
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.primary.application = Application::NotApplied;
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitApplication { .. }
    ));
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Updated,
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::NotApplicable,
    ));
    record.committed(
        StepEffect::Updated,
        StepTargets::unknown(),
        StepReplay::Repeatable,
    );
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitApplication { .. }
    ));
    let leaf = AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(TransportKind::Network, None)),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .try_build()
    .unwrap();
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.failed_sibling(
        Some(StepEffect::Updated),
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::NotApplied,
        leaf.clone(),
    );
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitApplication { .. }
    ));
    let unsent = AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(TransportKind::Network, None)),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .try_build()
    .unwrap();
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.failed_sibling(
        Some(StepEffect::Updated),
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::Uncertain,
        unsent.clone(),
    );
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitApplication { .. }
    ));
    for outcome in [
        CompensationOutcome::Failed(leaf),
        CompensationOutcome::Unconfirmed(unsent),
    ] {
        let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
        let id = record.committed(
            StepEffect::Created,
            StepTargets::unknown(),
            StepReplay::Repeatable,
        );
        record.compensation(
            StepEffect::Deleted,
            StepTargets::unknown(),
            vec![id],
            outcome,
        );
        assert!(matches!(
            build(record),
            AccountErrorBuildError::PartialCommitCompensation { .. }
        ));
    }
    for refs in [
        vec![],
        vec![StepId(99)],
        vec![StepId(0)],
        vec![StepId(1), StepId(1)],
    ] {
        let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
        record.committed(
            StepEffect::Created,
            StepTargets::unknown(),
            StepReplay::Repeatable,
        );
        record.compensation(
            StepEffect::Deleted,
            StepTargets::unknown(),
            refs,
            CompensationOutcome::Succeeded,
        );
        assert!(matches!(
            build(record),
            AccountErrorBuildError::PartialCommitCompensation { .. }
        ));
    }
    let nested = attach(throttle(), StepReplay::Repeatable, StepEffect::Updated);
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.failed_sibling(
        Some(StepEffect::Updated),
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::NotApplied,
        nested,
    );
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitNested { .. }
    ));
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Repeatable,
    );
    record.steps[0].effect = None;
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitApplication { .. }
    ));
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    let not_applied = record.failed_sibling(
        Some(StepEffect::Updated),
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::NotApplied,
        throttle(),
    );
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![not_applied],
        CompensationOutcome::Succeeded,
    );
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitCompensation { .. }
    ));
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    let created = record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Repeatable,
    );
    let compensation = record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![created],
        CompensationOutcome::Succeeded,
    );
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![compensation],
        CompensationOutcome::Succeeded,
    );
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitCompensation { .. }
    ));
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    let created = record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Repeatable,
    );
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![created],
        CompensationOutcome::Succeeded,
    );
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![created],
        CompensationOutcome::Succeeded,
    );
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitCompensation { .. }
    ));
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    let created = record.committed(
        StepEffect::Created,
        StepTargets::unknown(),
        StepReplay::Repeatable,
    );
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![created],
        CompensationOutcome::Failed(attach(
            throttle(),
            StepReplay::Repeatable,
            StepEffect::Updated,
        )),
    );
    assert!(matches!(
        build(record),
        AccountErrorBuildError::PartialCommitNested { .. }
    ));
    let record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    assert_eq!(
        throttle()
            .into_builder()
            .partial_commit(record.clone())
            .partial_commit(record)
            .try_build()
            .unwrap_err(),
        AccountErrorBuildError::PartialCommitAlreadyAttached
    );
}
#[test]
fn partial_commit_transport_drop_after_blocking_prefix_reconciles() {
    let leaf = AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(TransportKind::Network, None)),
    )
    .operation(AccountOperation::EventUpdate)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .try_build()
    .unwrap();
    let error = attach(leaf, StepReplay::Blocking, StepEffect::Copied);
    assert_eq!(
        error.partial_commit().unwrap().plan().replay,
        ReplaySafety::Never
    );
    assert_eq!(
        actions(&error),
        [ReconcileAction::CheckTarget, ReconcileAction::ObserveOnly]
    );
}
#[test]
fn partial_commit_unconfirmed_cleanup_of_a_gating_leg_is_not_residual() {
    let drop = AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(TransportKind::Network, None)),
    )
    .operation(AccountOperation::Send)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .try_build()
    .unwrap();
    let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
    let sibling = record.failed_sibling(
        Some(StepEffect::Created),
        StepTargets::unknown(),
        StepReplay::Repeatable,
        Application::from_leaf(&drop),
        drop.clone(),
    );
    record.compensation(
        StepEffect::Deleted,
        StepTargets::unknown(),
        vec![sibling],
        CompensationOutcome::from_error(drop),
    );
    let error = throttle()
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
    let plan = error.partial_commit().unwrap().plan();
    assert_eq!(plan.gating, vec![sibling]);
    assert!(plan.residual.is_empty());
}
#[test]
fn partial_commit_repeatable_commit_keeps_the_leg_reason() {
    let leaf = AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(TransportKind::Network, None)),
    )
    .operation(AccountOperation::Send)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .try_build()
    .unwrap();
    let error = attach(leaf, StepReplay::Repeatable, StepEffect::Created);
    assert!(
        matches!(error.recovery(),RecoveryClass::Reconcile(a) if a.reason==ReconcileReason::TransportDropAfterSend)
    );
}
