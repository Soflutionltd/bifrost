use super::*;
use bifrost_types::{AccountErrorBuilder, AccountOperation, AuthCause, PrimaryCause};

use crate::delivery::tests::account_change_checkpoint;
fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

pub(super) fn scope_tokens() -> ScopeTokens {
    Arc::new(StdMutex::new(HashMap::new()))
}

#[test]
fn registry_replacement_and_scope_restart_retire_terminal_ownership() {
    for replace_account in [false, true] {
        let cursors = CursorRegistry::new();
        let scope = CursorScope::Account;
        let cursor = crate::test_common::cursor_for(&scope, b"initial");
        cursors.put(cursor.clone());
        let tokens = scope_tokens();
        let mut entry = ScopeToken::tombstone();
        entry.drive_generation = Some(cursors.drive_generation(&scope));
        tokens
            .lock()
            .expect("poisoned")
            .insert(scope.clone(), entry);
        retire_stale_scope_token(&tokens, &cursors, &scope);
        assert!(scope_is_parked(&tokens, &scope));
        if replace_account {
            let replacement = CursorRegistry::new();
            replacement.put(cursor);
            cursors.replace_topology_preserving_cursors(&replacement);
        } else {
            cursors.delete(&scope);
            cursors.put(cursor);
        }
        retire_stale_scope_token(&tokens, &cursors, &scope);
        assert!(!tokens.lock().expect("poisoned").contains_key(&scope));
    }
}

#[tokio::test]
async fn fresh_bad_checkpoint_terminates_only_its_cursor_scope() {
    let scope = CursorScope::Account;
    let cursor = crate::test_common::cursor_for(&scope, b"initial");
    let mut invalid = cursor.clone();
    invalid.envelope_version = bifrost_types::TraitEnvelopeVersion::new(u32::MAX);
    let mut account = crate::test_common::StubAccount::new(vec![scope.clone()]);
    account.changes_hook = Some(Arc::new(move |_| {
        vec![SyncEvent::Done(Some(Checkpoint::Change(invalid.clone())))]
    }));
    let cursors = Arc::new(CursorRegistry::new());
    cursors.put(cursor.clone());
    let (tx, mut rx) = broadcast::channel(4);
    let delivery = Arc::new(ChangeDelivery::new(tx));
    let (_boundary, view) = crate::cancel::Boundary::new();
    let result = drive_changes_stream(
        &account,
        scope.clone(),
        cursor,
        cursors,
        delivery,
        view,
        None,
        None,
    )
    .await;
    let Err(Error::Account(error)) = result else {
        panic!("expected contract failure")
    };
    assert!(matches!(
        crate::recovery::plan_recovery(error.clone()),
        crate::recovery::RecoveryPlan::Terminal(_)
    ));
    assert_eq!(
        error.scope(),
        Some(&bifrost_types::ErrorScope::Cursor(scope))
    );
    assert!(matches!(
        &*rx.try_recv().expect("termination published").event,
        SyncEvent::Terminated(_)
    ));
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_scope_does_not_wait_out_its_retry_hint() {
    let scope = CursorScope::Account;
    let (reopen_tx, _reopen_rx) = mpsc::channel(1);
    let error = bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::Server(
        bifrost_types::ServerCause::RateLimited {
            throttle: Default::default(),
            retry_hint: Some(bifrost_types::RetryHint::After(
                std::time::Duration::from_secs(600),
            )),
        },
    ))
    .try_build()
    .expect("valid rate-limited server error");
    let scope_cancel = CancellationToken::new();
    scope_cancel.cancel();

    let started = tokio::time::Instant::now();
    let recovery = handle_drive_outcome(
        &scope,
        Ok(ChangesEvent::Terminated(error)),
        false,
        crate::types::AccountGeneration::for_test(7),
        &reopen_tx,
        &AccountId("cancelled".into()),
        &crate::throttle::ThrottleBucket::default(),
        &CancellationToken::new(),
        &scope_cancel,
    )
    .await;

    assert_eq!(recovery.exit, Some(PollExit::Retire));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(600),
        "the cancelled scope must not sleep off the provider hint"
    );
}

#[tokio::test(start_paused = true)]
async fn account_error_from_driver_routes_through_recovery_plan() {
    let scope = CursorScope::Account;
    let cursors = CursorRegistry::new();
    cursors.put(match account_change_checkpoint(b"unchanged") {
        Checkpoint::Change(cursor) => cursor,
        _ => unreachable!(),
    });
    let (reopen_tx, mut reopen_rx) = mpsc::channel(1);
    let error = crate::engine_errors::cursor_decode_failure(AccountOperation::SyncChanges);
    let recovery = handle_drive_outcome(
        &scope,
        Err(Error::Account(error.clone())),
        false,
        crate::types::AccountGeneration::for_test(7),
        &reopen_tx,
        &AccountId("routing".into()),
        &crate::throttle::ThrottleBucket::default(),
        &CancellationToken::new(),
        &CancellationToken::new(),
    )
    .await;

    assert!(recovery.exit.is_none());
    let request = reopen_rx.try_recv().expect("engine directive forwarded");
    assert!(matches!(
        request,
        ReopenRequest::Recovery { error: routed, generation: Some(generation), .. }
            if routed.kind() == error.kind() && generation == crate::types::AccountGeneration::for_test(7)
    ));
}

/// A poll task is nobody's detach worker, so an engine directive parked
/// on a full reopen channel (the listener alive but busy) must be freed
/// by shutdown alone, leaving the scope retired.
#[tokio::test(start_paused = true)]
async fn shutdown_unparks_an_engine_directive_send_against_a_full_reopen_channel() {
    let (reopen_tx, reopen_rx) = mpsc::channel(1);
    reopen_tx
        .try_send(ReopenRequest::ScopeCreated {
            scope: CursorScope::Account,
        })
        .expect("prefill");
    let shutdown = CancellationToken::new();
    let task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let error = crate::engine_errors::cursor_decode_failure(AccountOperation::SyncChanges);
            handle_drive_outcome(
                &CursorScope::Account,
                Err(Error::Account(error)),
                false,
                crate::types::AccountGeneration::for_test(7),
                &reopen_tx,
                &AccountId("parked".into()),
                &crate::throttle::ThrottleBucket::default(),
                &shutdown,
                &CancellationToken::new(),
            )
            .await
            .exit
        }
    });
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(!task.is_finished(), "parked on the full channel");
    shutdown.cancel();
    let exit = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .expect("shutdown frees the send")
        .expect("task");
    assert_eq!(exit, Some(PollExit::Retire));
    drop(reopen_rx);
}

/// `ScopeLifecycle::Deleted` must retire more than the in-memory
/// cursor: it raises `ReopenRequest::ScopeDeleted` so the engine also
/// purges the durable rows (change cursor AND backfill rows, completion
/// marker included). Without the request, a folder recreated under the
/// same id finds the surviving completion marker and skips its entire
/// cold-start walk.
#[tokio::test]
async fn terminal_drive_outcome_parks_the_scope_instead_of_retiring_it() {
    let scope = CursorScope::Account;
    let (reopen_tx, mut reopen_rx) = mpsc::channel(1);
    let error = AccountErrorBuilder::new(PrimaryCause::Auth(AuthCause::Expired))
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid terminal authentication error");

    let recovery = handle_drive_outcome(
        &scope,
        Ok(ChangesEvent::Terminated(error)),
        false,
        crate::types::AccountGeneration::for_test(7),
        &reopen_tx,
        &AccountId("terminal".into()),
        &crate::throttle::ThrottleBucket::default(),
        &CancellationToken::new(),
        &CancellationToken::new(),
    )
    .await;

    assert_eq!(recovery.exit, Some(PollExit::Park));
    assert!(
        reopen_rx.try_recv().is_err(),
        "terminal errors must not be handed to the reopen listener"
    );
}

/// A lag destroys batches whose checkpoints are already registered
/// as expected. If the receiver only warned, those registrations
/// would gate `pause` / `checkpoint_now` forever: surfacing the loss
/// would have converted it into a permanent hang.
/// `try_recv` shares the lag path, so it must abandon too - a
/// polling consumer that never calls `recv` would otherwise wedge
/// the boundary exactly as the awaiting one used to.
pub(super) fn track(tokens: &ScopeTokens, scope: &CursorScope) -> ScopeTokenGeneration {
    let generation = ScopeTokenGeneration::mint();
    tokens.lock().expect("poisoned").insert(
        scope.clone(),
        ScopeToken {
            generation,
            token: CancellationToken::new(),
            parked: false,
            drive_generation: None,
        },
    );
    generation
}

/// The ordinary case: a task that is still the registered owner
/// clears its own entry, so the 1s scan can respawn the scope.
#[test]
fn an_exiting_poll_task_retires_its_own_token() {
    let tokens = scope_tokens();
    let scope = CursorScope::Account;
    let generation = track(&tokens, &scope);

    retire_scope_token(&tokens, &scope, generation);

    assert!(tokens.lock().expect("poisoned").is_empty());
}

/// The interleaving that produced duplicate poll tasks: task A
/// exits while its scope is momentarily absent from the cursor
/// registry, the scan spawns B under the same key, and A's cleanup
/// then lands. Removing by key would evict B, leaving it untracked
/// and the next scan free to spawn a third live poll for one
/// scope - two concurrent producers on a lane+scope the checkpoint
/// supersession rule assumes has exactly one.
#[test]
fn an_exiting_poll_task_does_not_retire_its_successors_token() {
    let tokens = scope_tokens();
    let scope = CursorScope::Account;
    let departing = track(&tokens, &scope);
    let successor = track(&tokens, &scope);
    assert_ne!(departing, successor);

    retire_scope_token(&tokens, &scope, departing);

    let guard = tokens.lock().expect("poisoned");
    let entry = guard.get(&scope).expect("the successor stays tracked");
    assert_eq!(entry.generation, successor);
}

/// A cancelled entry left behind by a `ScopeLifecycle::Deleted` or
/// a `restart_scope` is still the successor's to own; the departing
/// task must not remove it on the strength of the cancel flag
/// alone.
#[test]
fn retirement_ignores_the_cancellation_flag() {
    let tokens = scope_tokens();
    let scope = CursorScope::Folder(bifrost_types::FolderId("Inbox".into()));
    let departing = track(&tokens, &scope);
    let successor = track(&tokens, &scope);
    tokens
        .lock()
        .expect("poisoned")
        .get(&scope)
        .expect("successor")
        .cancel();

    retire_scope_token(&tokens, &scope, departing);

    let guard = tokens.lock().expect("poisoned");
    assert_eq!(
        guard
            .get(&scope)
            .expect("successor still tracked")
            .generation,
        successor
    );
}

#[test]
fn cadence_halves_on_change() {
    let cur = AdaptiveCadence {
        interval: ms(800),
        no_change_streak: 3,
    };
    let next = Multiplexer::updated_cadence(cur, true, ms(100), ms(60_000));
    assert_eq!(next.interval, ms(400));
    assert_eq!(next.no_change_streak, 0);
}

#[test]
fn cadence_doubles_after_five_idle() {
    let mut c = AdaptiveCadence {
        interval: ms(500),
        no_change_streak: 0,
    };
    for _ in 0..4 {
        c = Multiplexer::updated_cadence(c, false, ms(100), ms(60_000));
        assert_eq!(c.interval, ms(500));
    }
    c = Multiplexer::updated_cadence(c, false, ms(100), ms(60_000));
    assert_eq!(c.interval, ms(1000));
    assert_eq!(c.no_change_streak, 0);
}

#[test]
fn cadence_respects_floor_and_ceiling() {
    let c = AdaptiveCadence {
        interval: ms(150),
        no_change_streak: 0,
    };
    let next = Multiplexer::updated_cadence(c, true, ms(200), ms(60_000));
    assert_eq!(next.interval, ms(200));
    let c = AdaptiveCadence {
        interval: ms(30_000),
        no_change_streak: 4,
    };
    let next = Multiplexer::updated_cadence(c, false, ms(100), ms(30_000));
    assert_eq!(next.interval, ms(30_000));
}

/// A lifecycle delete cancels the scope's poll task AND retires its
/// registry entry. Cancelling without retiring leaves a cancelled
/// token behind, and `spawn_missing_scope_polls` then sees an entry
/// for the scope and never respawns it if the scope comes back.
#[test]
fn cancelling_a_scope_token_also_retires_its_entry() {
    let tokens: ScopeTokens = Arc::new(StdMutex::new(HashMap::new()));
    let scope = CursorScope::Account;
    let token = CancellationToken::new();
    tokens.lock().expect("poisoned").insert(
        scope.clone(),
        ScopeToken {
            generation: ScopeTokenGeneration(7),
            token: token.clone(),
            parked: false,
            drive_generation: None,
        },
    );

    cancel_scope_token(&tokens, &scope);

    assert!(token.is_cancelled(), "the poll task must be cancelled");
    assert!(
        !tokens.lock().expect("poisoned").contains_key(&scope),
        "a cancelled scope must leave no entry blocking a later respawn"
    );
}

/// Retirement is generation-checked, so a late retirement from a
/// previous incarnation cannot delete a freshly spawned task's entry.
#[test]
fn a_stale_retirement_leaves_the_current_entry_alone() {
    let tokens: ScopeTokens = Arc::new(StdMutex::new(HashMap::new()));
    let scope = CursorScope::Account;
    tokens.lock().expect("poisoned").insert(
        scope.clone(),
        ScopeToken {
            generation: ScopeTokenGeneration(9),
            token: CancellationToken::new(),
            parked: false,
            drive_generation: None,
        },
    );

    retire_scope_token(&tokens, &scope, ScopeTokenGeneration(8));

    assert!(
        tokens.lock().expect("poisoned").contains_key(&scope),
        "an older generation must not retire the current entry"
    );
}
