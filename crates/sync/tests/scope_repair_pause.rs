//! A pause landing around a scope repair must not lose the scope.
//!
//! `RestartScope` (and `DowngradeCapabilityForScope`, which ends in the same
//! repair) deletes the scope's in-memory and durable cursor and then
//! re-establishes it. The establishment refuses a paused account, and the
//! multiplexer only polls scopes still present in the cursor registry, so a
//! pause landing between the delete and the establishment stranded the scope for
//! good: after `resume_account` nothing ever raised a directive for it again.
//!
//! Two shapes, one per half of the fix:
//!
//! - the account is already paused when the dispatch reaches the repair: nothing
//!   may be deleted, and the still-registered failing cursor re-raises the
//!   directive after resume;
//! - a pause lands while the repair is running (during the backoff between two
//!   establishment attempts): the pause waits for the bounded repair, and the
//!   retry is not refused by the boundary the pause just flipped.
//!
//! Both run under a paused clock, so the recovery backoff and the poll cadence
//! are virtual and "let everything settle" is a plain sleep.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bifrost_sync::{CheckpointStore, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountFactory, AccountId, AccountOperation, Change,
    ChangeCursor, Control, CursorScope, ErrorScope, ObjectType, PrimaryCause, StateCause,
    SyncEvent,
};

use common::{StubAccount, StubFactory};

const SCOPE: CursorScope = CursorScope::Type(ObjectType::Email);

/// Upper bound on a wait for a poll-driven event. The clock is paused, so this
/// is virtual time; it has to outlast the default 60s poll cadence.
const BOUND: Duration = Duration::from_secs(300);

/// Longer than the default poll cadence, so a poll drive is certain to have run.
const POLL_CADENCE_PASSED: Duration = Duration::from_secs(200);

/// Derives `Engine(RestartScope(SCOPE))`.
fn cursor_invalid() -> AccountError {
    scoped_state_error(StateCause::CursorInvalid { scope: SCOPE })
}

/// Derives `Engine(DowngradeCapabilityForScope(SCOPE))`.
fn scope_capability_lost() -> AccountError {
    scoped_state_error(StateCause::ScopeCapabilityLost { scope: SCOPE })
}

fn scoped_state_error(cause: StateCause) -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::State(cause))
        .operation(AccountOperation::SyncChanges)
        .scope(ErrorScope::Cursor(SCOPE))
        .try_build()
        .expect("valid account error classification")
}

/// An account whose scope terminates with `error()` once per unit stored in the
/// returned counter, and is quiet otherwise.
fn failing_on_demand(error: fn() -> AccountError) -> (StubAccount, Arc<AtomicUsize>) {
    let fire = Arc::new(AtomicUsize::new(0));
    let hook_fire = Arc::clone(&fire);
    let mut account = StubAccount::new(vec![SCOPE]);
    // `subscribe_push` refuses an account with no effective push lanes.
    account.caps.push = bifrost_types::PushCapability::Webhook;
    account.push_lane_specs = StubAccount::registration_lanes();
    account.changes_hook = Some(Arc::new(move |_cursor: &ChangeCursor| {
        let fires = hook_fire
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if fires {
            vec![SyncEvent::Terminated(error())]
        } else {
            Vec::<SyncEvent<Change>>::new()
        }
    }));
    (account, fire)
}

async fn durable_cursor_exists(store: &InMemoryCheckpointStore, id: &AccountId) -> bool {
    store
        .get_change_cursor(id, &SCOPE)
        .await
        .expect("cursor lookup")
        .is_some()
}

fn established(account: &StubAccount) -> usize {
    account.established.lock().expect("established lock").len()
}

/// The dispatch reaches the repair with the account already paused.
///
/// Staged with the reopen lock: a parked `subscribe_push` holds it, so the
/// `RestartScope` directive the scope's drive raises queues on the lock. The
/// account is then paused, and only then is the lock released. The repair must
/// decline without touching the scope, and after resume the poll must raise the
/// directive again, which repairs the scope.
///
/// Ablation: without the activity gate at the top of the repair the scope's
/// cursor is deleted and the establishment is refused, so the durable cursor is
/// gone while paused and the scope is never established again.
#[tokio::test(start_paused = true)]
async fn a_repair_reaching_a_paused_account_leaves_the_scope_for_the_poll_to_re_raise() {
    a_declined_repair_leaves_the_scope("repair-pause-declined", cursor_invalid).await;
}

/// The same staging through `DowngradeCapabilityForScope`, whose arm registers
/// its activity itself (ahead of its warning) instead of through `restart_scope`.
#[tokio::test(start_paused = true)]
async fn a_capability_downgrade_reaching_a_paused_account_leaves_the_scope_for_the_poll() {
    a_declined_repair_leaves_the_scope("downgrade-pause-declined", scope_capability_lost).await;
}

async fn a_declined_repair_leaves_the_scope(label: &str, error: fn() -> AccountError) {
    let id = AccountId(label.to_owned());
    let (account, fire) = failing_on_demand(error);
    let park = Arc::new(tokio::sync::Notify::new());
    *account
        .push_subscribe_park
        .lock()
        .expect("subscribe park lock") = Some((0, Arc::clone(&park)));
    let push_log = Arc::clone(&account.push_log);
    let account = Arc::new(account);
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = Arc::new(
        SyncEngine::builder()
            .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
            .build()
            .expect("default engine config is valid"),
    );
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");
    assert!(
        durable_cursor_exists(&store, &id).await,
        "attach must have persisted the scope's cursor, or survival proves nothing"
    );
    assert_eq!(established(&account), 1);

    // Hold the reopen lock inside a parked push subscription.
    let subscribing = {
        let engine = Arc::clone(&engine);
        let id = id.clone();
        tokio::spawn(async move { engine.subscribe_push(&id, &[SCOPE]).await })
    };
    tokio::time::timeout(BOUND, async {
        while push_log.subscribed().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the subscription never reached the account");

    // The scope's drive raises the directive; the dispatch queues on the lock.
    fire.store(1, Ordering::SeqCst);
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        fire.load(Ordering::SeqCst),
        0,
        "the failure must have fired"
    );

    control.pause().await.expect("an idle account pauses");
    park.notify_one();
    subscribing
        .await
        .expect("subscribe task")
        .expect("subscribe succeeds once released");

    // The dispatch now runs against a paused account.
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert!(
        durable_cursor_exists(&store, &id).await,
        "a repair that reached a paused account deleted the scope's durable cursor"
    );
    assert_eq!(
        established(&account),
        1,
        "nothing may be re-established while the account is paused"
    );

    // After resume the still-registered failing cursor re-raises the directive.
    fire.store(1, Ordering::SeqCst);
    control.resume();
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        established(&account),
        2,
        "after resume the poll must raise the directive again and repair the scope"
    );
    assert!(durable_cursor_exists(&store, &id).await);

    engine.detach(&id).await.expect("detach");
}

/// A pause lands while the repair is between two establishment attempts.
///
/// The first attempt fails, so the repair sleeps its backoff holding one
/// activity registration. The pause flips the boundary and must wait for the
/// repair; the second attempt must then run rather than be refused by the
/// boundary the pause flipped, because it registers its own activity.
///
/// Ablation: establishing on the plain control makes the second attempt's
/// registration fail with `Paused`, so the repair gives up with the cursor
/// already deleted, and the scope is never established again.
#[tokio::test(start_paused = true)]
async fn a_pause_landing_mid_repair_waits_for_it_instead_of_stranding_the_scope() {
    let id = AccountId("repair-pause-mid".to_owned());
    let (account, fire) = failing_on_demand(cursor_invalid);
    let account = Arc::new(account);
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");
    assert_eq!(established(&account), 1);

    // The first establishment attempt of the repair fails; the retry works.
    account.establish_failures.store(1, Ordering::SeqCst);
    fire.store(1, Ordering::SeqCst);
    tokio::time::timeout(BOUND, account.establish_failed.notified())
        .await
        .expect("the repair never attempted its establishment");

    // The repair is now in its backoff, holding its activity registration.
    let mut pause = Box::pin(control.pause());
    tokio::select! {
        biased;
        result = &mut pause => panic!("pause reported quiescence mid-repair: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    pause
        .await
        .expect("the pause completes once the repair has finished");

    assert_eq!(
        established(&account),
        3,
        "attach, the failed attempt, and the retry the pause must not have refused"
    );
    assert!(
        durable_cursor_exists(&store, &id).await,
        "the pause stranded the scope: its cursor was deleted and never re-established"
    );

    control.resume();
    engine.detach(&id).await.expect("detach");
}
