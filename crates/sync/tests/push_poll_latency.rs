//! Push-path liveness and recovery routing through the real engine.
//!
//! These drive the whole attach -> reconciler -> `drive_changes_stream`
//! pipeline against the `StubAccount` seam rather than unit-testing the
//! reconciler in isolation, because both properties under test are about
//! what the reconciler does to state OUTSIDE itself: whether a push hint can
//! reach the wire while a poll task holds a long cadence, and whether a
//! failing hinted scope still reaches engine recovery.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bifrost_sync::{EngineConfig, SyncEngine};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFactory, AccountId, Batch, Cause,
    Checkpoint, CursorScope, ErrorScope, HintPayload, InvalidationHint, ObjectType, PageBoundary,
    PushSource, StateCause, SyncEvent, SyncStateErrorKind, WatchEvent,
};

/// A long cadence: any drive observed after attach's first pass came from the
/// push path, not from the poll timer.
fn hour_cadence_config() -> EngineConfig {
    let mut config = EngineConfig::default();
    config.multiplexer.poll_initial = Duration::from_secs(60 * 60);
    config.multiplexer.poll_min = Duration::from_secs(60 * 60);
    config.multiplexer.poll_max = Duration::from_secs(60 * 60);
    config
}

fn unknown_hint() -> WatchEvent {
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source: PushSource::GraphSubscription,
            payload: HintPayload::Unknown,
        },
    }
}

/// `SyncState(CursorInvalid)` carrying a cursor scope derives
/// `Engine(RestartScope(scope))` - a SCOPE-BOUND directive, which is the half
/// of the recovery space that must not end a multi-scope hint sweep.
fn scope_bound_restart(scope: &CursorScope) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .scope(ErrorScope::Cursor(scope.clone()))
    .operation(bifrost_types::AccountOperation::SyncChanges)
    .try_build()
    .expect("valid scope-bound cursor-invalid classification")
}

async fn wait_until(label: &str, budget: Duration, mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(budget, async {
        while !ready() {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{label}"));
}

#[tokio::test]
async fn push_invalidation_drives_mid_poll_cadence_without_waiting_for_the_sleep() {
    let account_id = AccountId("push-mid-cadence".to_owned());
    let scope = CursorScope::Account;
    let drives = Arc::new(AtomicUsize::new(0));

    let mut stub = common::StubAccount::new(vec![scope]);
    let hook_drives = Arc::clone(&drives);
    stub.changes_hook = Some(Arc::new(move |_| {
        hook_drives.fetch_add(1, Ordering::SeqCst);
        Vec::new()
    }));
    let stub = Arc::new(stub);

    let engine = SyncEngine::builder()
        .config(hour_cadence_config())
        .build()
        .expect("valid long-cadence config");
    let factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![Arc::clone(&stub)]));

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");

    wait_until(
        "the initial poll drive starts",
        Duration::from_secs(5),
        || drives.load(Ordering::SeqCst) > 0,
    )
    .await;

    assert!(matches!(
        engine
            .invalidation_sink()
            .push(account_id.clone(), unknown_hint()),
        bifrost_types::SinkPush::Accepted
    ));

    wait_until(
        "push must drive during the one-hour poll cadence sleep",
        Duration::from_secs(5),
        || drives.load(Ordering::SeqCst) >= 2,
    )
    .await;

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// A drive that returns `Err(Error::Account(..))` on the push path must still
/// reach recovery. A fresh unsupported cursor envelope is an account contract
/// failure with terminal recovery: its error is published, its scope is parked,
/// and later hints cannot drive it again. The sibling scope also proves the
/// first multi-scope hint completes after the terminal verdict.
#[tokio::test]
async fn a_push_drive_account_contract_error_is_published_and_parks_its_scope() {
    let account_id = AccountId("push-drive-recovery".to_owned());
    let failed_scope = CursorScope::Type(ObjectType::Email);
    let sibling_scope = CursorScope::Type(ObjectType::CalendarEvent);
    let failed_drives = Arc::new(AtomicUsize::new(0));
    let sibling_drives = Arc::new(AtomicUsize::new(0));

    let mut stub = common::StubAccount::new(vec![failed_scope.clone(), sibling_scope.clone()]);
    let hook_failed_drives = Arc::clone(&failed_drives);
    let hook_sibling_drives = Arc::clone(&sibling_drives);
    let hook_failed_scope = failed_scope.clone();
    stub.changes_hook = Some(Arc::new(move |cursor| {
        if cursor.scope == hook_failed_scope {
            // Attach's first pass is clean. The next drive is the push path,
            // which is the only path this test asks to park the scope.
            if hook_failed_drives.fetch_add(1, Ordering::SeqCst) == 0 {
                return Vec::new();
            }
            let mut invalid = cursor.clone();
            invalid.envelope_version += 1;
            vec![SyncEvent::Batch(Batch {
                items: Vec::new(),
                page_boundary: PageBoundary::Final,
                server_latency: Duration::ZERO,
                bytes_in: 0,
                checkpoint: Some(Checkpoint::Change(invalid)),
            })]
        } else {
            hook_sibling_drives.fetch_add(1, Ordering::SeqCst);
            Vec::new()
        }
    }));
    let stub = Arc::new(stub);
    let established = Arc::clone(&stub.established);

    let engine = SyncEngine::builder()
        .config(hour_cadence_config())
        .build()
        .expect("valid long-cadence config");
    let factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![Arc::clone(&stub)]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");

    wait_until(
        "the initial poll pass drives both scopes",
        Duration::from_secs(5),
        || failed_drives.load(Ordering::SeqCst) > 0 && sibling_drives.load(Ordering::SeqCst) > 0,
    )
    .await;
    assert_eq!(established.lock().expect("established lock").len(), 2);
    let mut observer = engine
        .account_changes_observer(&account_id)
        .expect("account observer");

    assert!(matches!(
        engine
            .invalidation_sink()
            .push(account_id.clone(), unknown_hint()),
        bifrost_types::SinkPush::Accepted
    ));

    wait_until(
        "the push hint must drive the failing scope and its sibling",
        Duration::from_secs(5),
        || failed_drives.load(Ordering::SeqCst) >= 2 && sibling_drives.load(Ordering::SeqCst) >= 2,
    )
    .await;

    let observed = tokio::time::timeout(Duration::from_secs(5), observer.recv())
        .await
        .expect("push termination reaches observers")
        .expect("observer remains open");
    let SyncEvent::Terminated(error) = observed.event.as_ref() else {
        panic!("expected terminal account error, got {:?}", observed.event);
    };
    assert_eq!(
        error.kind(),
        &AccountErrorKind::Internal(bifrost_types::InternalErrorKind::AccountContract)
    );
    assert_eq!(
        error.recovery(),
        &bifrost_types::RecoveryClass::InternalFailure
    );

    let specific_hint = |scope: CursorScope| WatchEvent::Invalidated {
        hint: InvalidationHint {
            source: PushSource::GraphSubscription,
            payload: HintPayload::SpecificCursorScope(scope),
        },
    };
    assert!(matches!(
        engine
            .invalidation_sink()
            .push(account_id.clone(), specific_hint(sibling_scope.clone())),
        bifrost_types::SinkPush::Accepted
    ));
    wait_until(
        "the sibling hint acts as a barrier after the terminal sweep",
        Duration::from_secs(5),
        || sibling_drives.load(Ordering::SeqCst) >= 3,
    )
    .await;
    assert!(matches!(
        engine
            .invalidation_sink()
            .push(account_id.clone(), specific_hint(failed_scope)),
        bifrost_types::SinkPush::Accepted
    ));
    assert!(matches!(
        engine
            .invalidation_sink()
            .push(account_id.clone(), specific_hint(sibling_scope)),
        bifrost_types::SinkPush::Accepted
    ));
    wait_until(
        "the sibling hint completes after the terminal-scope hint",
        Duration::from_secs(5),
        || sibling_drives.load(Ordering::SeqCst) >= 4,
    )
    .await;
    assert_eq!(
        failed_drives.load(Ordering::SeqCst),
        2,
        "later hints must not re-drive a terminally failed scope"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// E6: one failed hinted scope must not abandon the remaining scopes of a
/// multi-scope hint - AND the failing scope must still reach recovery. The
/// predecessor of this test counted drive attempts only, so it passed against
/// a reconciler that swallowed the error entirely.
///
/// Both scopes fail identically on their second drive, so the assertion does
/// not depend on `all_scopes()` ordering: whichever the sweep visits first,
/// the second one is only reached if the first did not end the sweep.
#[tokio::test]
async fn a_failed_hinted_scope_reaches_recovery_without_abandoning_its_siblings() {
    let account_id = AccountId("push-multi-scope-error".to_owned());
    let scopes = vec![
        CursorScope::Type(ObjectType::Email),
        CursorScope::Type(ObjectType::CalendarEvent),
    ];
    let drives = Arc::new(AtomicUsize::new(0));
    let per_scope: Arc<Mutex<HashMap<CursorScope, usize>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut stub = common::StubAccount::new(scopes.clone());
    let hook_drives = Arc::clone(&drives);
    let hook_per_scope = Arc::clone(&per_scope);
    stub.changes_hook = Some(Arc::new(move |cursor| {
        hook_drives.fetch_add(1, Ordering::SeqCst);
        let calls = {
            let mut calls = hook_per_scope.lock().expect("per-scope call lock");
            let entry = calls.entry(cursor.scope.clone()).or_insert(0_usize);
            *entry += 1;
            *entry
        };
        // Drive 1 is attach's poll pass; drive 2 is the hinted one and fails
        // with a scope-bound directive. Later drives (post-recovery
        // re-establishment) are clean, so the account cannot spin.
        if calls == 2 {
            vec![SyncEvent::Terminated(scope_bound_restart(&cursor.scope))]
        } else {
            Vec::new()
        }
    }));
    let stub = Arc::new(stub);
    let established = Arc::clone(&stub.established);

    let engine = SyncEngine::builder()
        .config(hour_cadence_config())
        .build()
        .expect("valid long-cadence config");
    let factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![Arc::clone(&stub)]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");

    wait_until(
        "both initial poll drives run",
        Duration::from_secs(5),
        || per_scope.lock().expect("per-scope call lock").len() == 2,
    )
    .await;
    let established_before = established.lock().expect("established lock").len();
    assert_eq!(established_before, 2, "attach establishes both scopes once");

    assert!(matches!(
        engine
            .invalidation_sink()
            .push(account_id.clone(), unknown_hint()),
        bifrost_types::SinkPush::Accepted
    ));

    // Property one: the sweep did not stop at the first failure. Keyed on
    // per-scope drive counts, not a total, so one scope driven twice cannot
    // masquerade as two scopes driven once.
    wait_until(
        "both hinted scopes must be driven despite the first one failing",
        Duration::from_secs(10),
        || {
            let calls = per_scope.lock().expect("per-scope call lock");
            scopes
                .iter()
                .all(|scope| calls.get(scope).copied().unwrap_or(0) >= 2)
        },
    )
    .await;

    // Property two: the failures were not swallowed. A scope-bound
    // `RestartScope` re-establishes exactly that scope, so a fresh
    // `establish_initial_cursor` per failing scope is the recovery's
    // fingerprint.
    wait_until(
        "each failing scope must still reach engine recovery",
        Duration::from_secs(10),
        || established.lock().expect("established lock").len() >= established_before + 2,
    )
    .await;

    engine.detach(&account_id).await.expect("detach succeeds");
}
