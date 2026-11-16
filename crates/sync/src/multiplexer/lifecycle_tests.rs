use super::*;
use bifrost_types::{AccountErrorBuilder, AccountOperation, AuthCause, PrimaryCause, StateCause};

use crate::delivery::tests::{account_change_checkpoint, lag_test_control, lag_test_control_over};
use crate::multiplexer::tests::{scope_tokens, track};
#[test]
fn retiring_a_deleted_scope_revokes_its_producers_before_its_cursor() {
    let coverage = Arc::new(crate::cursor::PendingCoverage::new());
    let control = lag_test_control_over(Arc::clone(&coverage));
    let cursors = CursorRegistry::new();
    let Checkpoint::Change(cursor) = account_change_checkpoint(b"live") else {
        panic!("cursor")
    };
    cursors.put(cursor.clone());
    let old = coverage.scope_fence(&cursor.scope);
    let epoch = cursors.lifecycle_epoch(&cursor.scope);
    retire_deleted_scope(&cursors, &control, &cursor.scope);
    assert!(
        !cursors.put_if(cursor.clone(), || coverage.scope_fence(&cursor.scope)
            == old)
    );
    assert!(cursors.snapshot(&cursor.scope).is_none());
    assert!(cursors.lifecycle_epoch(&cursor.scope) > epoch);
}

#[tokio::test]
async fn a_deleted_scope_leaves_no_registry_entry_and_a_moved_fence() {
    let coverage = Arc::new(crate::cursor::PendingCoverage::new());
    let control = lag_test_control_over(Arc::clone(&coverage));
    let cursors = CursorRegistry::new();
    let scope = CursorScope::Folder(bifrost_types::FolderId("deleted".into()));
    cursors.put(crate::test_common::cursor_for(&scope, b"live"));
    let old = coverage.scope_fence(&scope);
    let epoch = cursors.lifecycle_epoch(&scope);
    let (tx, _rx) = mpsc::channel(4);
    apply_lifecycle_transition(
        &cursors,
        &control,
        &Arc::default(),
        &tx,
        ScopeLifecycle::Deleted(bifrost_types::MembershipScope::Folder(
            bifrost_types::FolderId("deleted".into()),
        )),
    )
    .await;
    assert!(cursors.snapshot(&scope).is_none());
    assert!(coverage.scope_fence(&scope) > old);
    assert!(cursors.lifecycle_epoch(&scope) > epoch);
}

/// The lifecycle reader stamps a stream termination with the generation of
/// the incarnation whose stream it read. The listener's stale decline is
/// pinned separately with hand-built requests; this pins the producer, run
/// through the real `Multiplexer::run`, so a request queued past a reattach
/// carries the old incarnation and cannot restart the replacement.
/// Ablation: stamping `None` fails the match.
#[tokio::test(start_paused = true)]
async fn a_lifecycle_termination_is_stamped_with_its_streams_incarnation() {
    const GENERATION: crate::types::AccountGeneration =
        crate::types::AccountGeneration::for_test(7);
    let account_id = AccountId("lifecycle-stamp".into());
    let stub = crate::test_common::StubAccount::new(Vec::new());
    stub.lifecycle_scripts
        .lock()
        .expect("scripts")
        .push_back(vec![bifrost_types::ScopeLifecycleEvent::Terminated(
            bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::State(
                bifrost_types::StateCause::CapabilityChanged { delta: None },
            ))
            .try_build()
            .expect("restart error"),
        )]);
    let account: Arc<dyn bifrost_types::Account> = Arc::new(stub);
    let (boundary, view) = crate::cancel::Boundary::new();
    let (priority, _priority) = watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _bandwidth) = watch::channel(None);
    let control =
        crate::control::SyncControl::new(account_id.clone(), boundary, priority, bandwidth);
    let (tx, _events) = broadcast::channel(8);
    let (reopen_tx, mut reopen_rx) = mpsc::channel(4);
    let (_generation, generation_rx) = watch::channel(GENERATION);
    let gate = crate::scheduler::BudgetGate::new(crate::scheduler::ConcurrencyBudget::default());
    gate.register(account_id.clone());
    let shutdown = CancellationToken::new();
    let mux = Multiplexer {
        account_id,
        account: Arc::new(arc_swap::ArcSwap::from_pointee(
            crate::types::Incarnation::new(account, GENERATION),
        )),
        account_generation: generation_rx,
        cursors: Arc::new(CursorRegistry::new()),
        config: MultiplexerConfig::default(),
        boundary: view,
        delivery: Arc::new(ChangeDelivery::new(tx)),
        control,
        shutdown: shutdown.clone(),
        reopen_tx,
        scope_tokens: Arc::default(),
        throttles: Arc::new(crate::throttle::ThrottleBucket::new()),
        scheduler: crate::scheduler::Scheduler::new(crate::types::SchedulerConfig::default(), gate),
    };
    let run = tokio::spawn(mux.run());
    let request = tokio::time::timeout(Duration::from_secs(60), reopen_rx.recv())
        .await
        .expect("the termination is forwarded")
        .expect("request");
    assert!(
        matches!(
            request,
            ReopenRequest::Recovery {
                scope: None,
                generation: Some(GENERATION),
                ..
            }
        ),
        "{request:?}"
    );
    shutdown.cancel();
    run.await.expect("multiplexer");
}

#[tokio::test]
async fn deleted_lifecycle_purges_cursor_and_requests_durable_deletion() {
    let cursors = CursorRegistry::new();
    let folder = bifrost_types::FolderId("Projects".into());
    let scope = CursorScope::Folder(folder.clone());
    cursors.put(bifrost_types::ChangeCursor {
        scope: scope.clone(),
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Imap,
            envelope_version: bifrost_types::PayloadVersion::new(1),
            bytes: b"state".to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
    });
    let membership = bifrost_types::MembershipScope::Folder(folder);
    cursors.link_membership(membership.clone(), scope.clone());
    let tokens = scope_tokens();
    track(&tokens, &scope);
    // The REGISTRY's own liveness token, captured before the transition.
    // The poll token below covers only the poll task; this is what reaches
    // a stalled drive belonging to the push reconciler, which never has an
    // entry in `tokens`.
    let registry_cancel = cursors
        .scope_cancel(&scope)
        .expect("registered scope token");
    let (reopen_tx, mut reopen_rx) = mpsc::channel(4);

    apply_lifecycle_transition(
        &cursors,
        &lag_test_control(),
        &tokens,
        &reopen_tx,
        ScopeLifecycle::Deleted(membership),
    )
    .await;

    assert!(cursors.snapshot(&scope).is_none());
    assert!(!tokens.lock().expect("poisoned").contains_key(&scope));
    assert!(
        registry_cancel.is_cancelled(),
        "a lifecycle delete must end any in-flight drive, including the \
             reconciler's, not just the poll task"
    );
    let request = reopen_rx.try_recv().expect("durable deletion requested");
    assert!(matches!(
        request,
        ReopenRequest::ScopeDeleted { scope: deleted } if deleted == scope
    ));
}

/// A lifecycle `Created` asks for the folder to be established if it is
/// still absent. It used to synthesize a `RestartScope`, which, queued
/// behind a reattach whose discovery had already established the folder on
/// the replacement, deleted that healthy cursor and could strand the scope.
#[tokio::test]
async fn created_lifecycle_requests_establishment_only_if_absent() {
    let cursors = CursorRegistry::new();
    let sibling = CursorScope::Folder(bifrost_types::FolderId("Inbox".into()));
    cursors.put(bifrost_types::ChangeCursor {
        scope: sibling,
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Imap,
            envelope_version: bifrost_types::PayloadVersion::new(1),
            bytes: b"state".to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
    });
    let folder = bifrost_types::FolderId("Fresh".into());
    let scope = CursorScope::Folder(folder.clone());
    let (reopen_tx, mut reopen_rx) = mpsc::channel(4);

    apply_lifecycle_transition(
        &cursors,
        &lag_test_control(),
        &scope_tokens(),
        &reopen_tx,
        ScopeLifecycle::Created(bifrost_types::MembershipScope::Folder(folder)),
    )
    .await;

    let request = reopen_rx.try_recv().expect("establishment requested");
    assert!(matches!(
        request,
        ReopenRequest::ScopeCreated { scope: requested } if requested == scope
    ));
}

fn put_folder_cursor(cursors: &CursorRegistry, scope: &CursorScope) {
    cursors.put(bifrost_types::ChangeCursor {
        scope: scope.clone(),
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Jmap,
            envelope_version: bifrost_types::PayloadVersion::new(1),
            bytes: b"state".to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
    });
}

/// A rename on one stable id changes only a display name. It must not
/// retire the healthy cursor (that purged valid progress and forced a
/// cold-start re-walk); it only asks for establish-if-absent.
#[tokio::test]
async fn same_id_rename_keeps_the_cursor_and_requests_only_establishment() {
    let cursors = CursorRegistry::new();
    let folder = bifrost_types::FolderId("Projects".into());
    let scope = CursorScope::Folder(folder.clone());
    put_folder_cursor(&cursors, &scope);
    let membership =
        bifrost_types::MembershipScope::Mailbox(bifrost_types::MailboxId(folder.0.clone()));
    cursors.link_membership(membership.clone(), scope.clone());
    let tokens = scope_tokens();
    track(&tokens, &scope);
    let epoch = cursors.lifecycle_epoch(&scope);
    let (reopen_tx, mut reopen_rx) = mpsc::channel(4);

    apply_lifecycle_transition(
        &cursors,
        &lag_test_control(),
        &tokens,
        &reopen_tx,
        ScopeLifecycle::Renamed {
            old: membership.clone(),
            new: membership,
            old_name: "Projects".into(),
            new_name: "Work".into(),
        },
    )
    .await;

    assert!(cursors.snapshot(&scope).is_some(), "cursor survives");
    assert_eq!(cursors.lifecycle_epoch(&scope), epoch, "no deletion");
    assert!(tokens.lock().expect("poisoned").contains_key(&scope));
    let request = reopen_rx.try_recv().expect("establishment requested");
    assert!(matches!(
        request,
        ReopenRequest::ScopeCreated { scope: requested } if requested == scope
    ));
    assert!(reopen_rx.try_recv().is_err(), "no ScopeDeleted");
}

/// A rename across ids whose old id held the only registered folder
/// cursor must still establish the new id: the account model outlives
/// the delete half that empties the registry.
#[tokio::test]
async fn cross_id_rename_of_the_last_folder_cursor_still_creates_the_new_id() {
    let cursors = CursorRegistry::new();
    let old_folder = bifrost_types::FolderId("Old".into());
    let new_folder = bifrost_types::FolderId("New".into());
    let old_scope = CursorScope::Folder(old_folder.clone());
    let new_scope = CursorScope::Folder(new_folder.clone());
    put_folder_cursor(&cursors, &old_scope);
    let old = bifrost_types::MembershipScope::Folder(old_folder);
    cursors.link_membership(old.clone(), old_scope.clone());
    let tokens = scope_tokens();
    track(&tokens, &old_scope);
    let (reopen_tx, mut reopen_rx) = mpsc::channel(4);

    apply_lifecycle_transition(
        &cursors,
        &lag_test_control(),
        &tokens,
        &reopen_tx,
        ScopeLifecycle::Renamed {
            old,
            new: bifrost_types::MembershipScope::Folder(new_folder),
            old_name: "Old".into(),
            new_name: "New".into(),
        },
    )
    .await;

    assert!(cursors.snapshot(&old_scope).is_none());
    assert!(matches!(
        reopen_rx.try_recv().expect("deletion requested"),
        ReopenRequest::ScopeDeleted { scope } if scope == old_scope
    ));
    assert!(matches!(
        reopen_rx.try_recv().expect("creation requested"),
        ReopenRequest::ScopeCreated { scope } if scope == new_scope
    ));
}

/// A `Deleted` arriving while the registry holds no folder cursor - the
/// last one renamed away, its replacement's `ScopeCreated` still queued
/// or parked - must still route `ScopeDeleted` for the folder, or the
/// listener never unparks the pending establishment and it creates a
/// cursor for a deleted folder. Ablation: mapping deletion through the
/// live cursors sends nothing here.
#[tokio::test]
async fn deleting_a_folder_while_no_folder_cursor_is_registered_still_routes_the_deletion() {
    let cursors = CursorRegistry::new();
    let old_folder = bifrost_types::FolderId("Old".into());
    let new_folder = bifrost_types::FolderId("New".into());
    let old_scope = CursorScope::Folder(old_folder.clone());
    let new_scope = CursorScope::Folder(new_folder.clone());
    put_folder_cursor(&cursors, &old_scope);
    let old = bifrost_types::MembershipScope::Folder(old_folder);
    cursors.link_membership(old.clone(), old_scope.clone());
    let new = bifrost_types::MembershipScope::Folder(new_folder);
    let tokens = scope_tokens();
    track(&tokens, &old_scope);
    let control = lag_test_control();
    let (reopen_tx, mut reopen_rx) = mpsc::channel(4);

    apply_lifecycle_transition(
        &cursors,
        &control,
        &tokens,
        &reopen_tx,
        ScopeLifecycle::Renamed {
            old,
            new: new.clone(),
            old_name: "Old".into(),
            new_name: "New".into(),
        },
    )
    .await;
    assert!(cursors.all_scopes().is_empty());
    let _ = reopen_rx.try_recv().expect("old id deletion");
    let _ = reopen_rx.try_recv().expect("new id creation");
    let epoch_before = cursors.lifecycle_epoch(&new_scope);

    apply_lifecycle_transition(
        &cursors,
        &control,
        &tokens,
        &reopen_tx,
        ScopeLifecycle::Deleted(new),
    )
    .await;

    assert!(matches!(
        reopen_rx.try_recv().expect("deletion routed"),
        ReopenRequest::ScopeDeleted { scope } if scope == new_scope
    ));
    assert!(reopen_rx.try_recv().is_err());
    assert_eq!(
        cursors.lifecycle_epoch(&new_scope),
        epoch_before.after_deletion()
    );
}

/// A shape a reattach's replacement topology dropped no longer
/// authorizes creation, but deletion still reaches it: an establishment
/// raised under the earlier model may still be queued or parked.
#[test]
fn a_replaced_model_stops_creation_but_not_deletion_reach() {
    let cursors = CursorRegistry::new();
    put_folder_cursor(
        &cursors,
        &CursorScope::Folder(bifrost_types::FolderId("A".into())),
    );
    let replacement = CursorRegistry::new();
    replacement.note_discovered(&[CursorScope::Type(bifrost_types::ObjectType::Email)]);
    cursors.replace_topology_preserving_cursors(&replacement);
    let membership = bifrost_types::MembershipScope::Folder(bifrost_types::FolderId("B".into()));

    assert!(membership_to_cursor_scopes(&cursors, &membership).is_empty());
    assert_eq!(
        membership_to_deleted_scopes(&cursors, &membership),
        vec![CursorScope::Folder(bifrost_types::FolderId("B".into()))]
    );
}

/// A terminal recovery verdict must park the scope, not retire it: a
/// retired token is exactly what the 1s scan reads as "please respawn",
/// which turned every terminal changes-stream error into a
/// once-per-second re-drive that re-broadcast `Terminated` forever.
#[tokio::test(start_paused = true)]
async fn lifecycle_retry_records_hold_and_honours_hint_without_shortening_backoff() {
    let account = AccountId("lifecycle".into());
    let throttles = crate::throttle::ThrottleBucket::default();
    for (hint, expected) in [(600, 600), (1, 30)] {
        let error = AccountErrorBuilder::new(PrimaryCause::Server(
            bifrost_types::ServerCause::RateLimited {
                throttle: Default::default(),
                retry_hint: Some(bifrost_types::RetryHint::After(Duration::from_secs(hint))),
            },
        ))
        .with_throttle(|throttle| throttle.scope = Some(bifrost_types::ThrottleScope::Account))
        .try_build()
        .expect("valid lifecycle throttle");
        assert_eq!(
            lifecycle_termination(&error),
            LifecycleTermination::Reconnect
        );
        assert_eq!(
            lifecycle_retry_wait(&error, LIFECYCLE_RECONNECT_MIN, &throttles, &account),
            Duration::from_secs(expected),
        );
        assert_eq!(
            crate::throttle::account_throttle_wait(&throttles, &account),
            Some(Duration::from_secs(600)),
            "a shorter subsequent hint must not erase a sibling hold",
        );
    }
}

#[test]
fn lifecycle_terminal_errors_do_not_reconnect_or_reopen() {
    let error = AccountErrorBuilder::new(PrimaryCause::Auth(AuthCause::Expired))
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid terminal authentication error");

    assert_eq!(
        lifecycle_termination(&error),
        LifecycleTermination::Terminate
    );
}

/// A `RestartAccount` directive swaps the connection this reader holds,
/// so waiting for the generation change is the right park - bounded, so a
/// reopen that never succeeds cannot silence lifecycle observation.
#[test]
fn lifecycle_account_reopen_errors_wait_for_the_generation_change() {
    let error = AccountErrorBuilder::new(PrimaryCause::State(StateCause::CapabilityChanged {
        delta: None,
    }))
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid account-reopen recovery error");

    assert_eq!(
        lifecycle_termination(&error),
        LifecycleTermination::AwaitReopen
    );
}

/// Scope-level and account-wide-but-not-reopen directives never bump the
/// account generation, so parking on it would strand this reader for the
/// process lifetime. Hand the error over once, then reconnect on backoff.
#[test]
fn lifecycle_non_reopen_directives_reconnect_instead_of_parking() {
    let schema = AccountErrorBuilder::new(PrimaryCause::State(StateCause::SchemaIncompatible))
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid schema recovery error");
    assert_eq!(
        lifecycle_termination(&schema),
        LifecycleTermination::ForwardAndReconnect
    );

    let revoked = crate::engine_errors::restart_scope_error(
        CursorScope::Folder(bifrost_types::FolderId("Shared".into())),
        AccountOperation::SyncChanges,
    );
    assert_eq!(
        lifecycle_termination(&revoked),
        LifecycleTermination::ForwardAndReconnect
    );
}
