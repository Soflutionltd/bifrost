//! A pause landing around account-wide schema recovery, or a lifecycle scope's
//! first establishment, must not lose scopes.
//!
//! Schema recovery deletes EVERY scope's cursor and then re-establishes each one.
//! The establishment refuses a paused account and the multiplexer only polls
//! scopes still in the cursor registry, so a pause landing inside the recovery
//! stranded every scope it had deleted. `scope_repair_pause` holds the same
//! contract for a single scope's restart; these cover the two paths that shared
//! the hole:
//!
//! - schema recovery declines on a paused account before deleting anything, and
//!   once started holds its activity registration through the last
//!   re-establishment;
//! - a `ScopeLifecycle::Created` scope has NO cursor, so a repair declined under
//!   a pause leaves nothing for the poll to re-raise the directive from. It is
//!   parked and re-offered once the account runs.
//!
//! All run under a paused clock, so backoff and poll cadence are virtual.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bifrost_sync::{CheckpointStore, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFactory, AccountId,
    AccountOperation, Cause, Change, ChangeCursor, Control, CursorScope, FolderId, MembershipScope,
    ObjectType, ScopeLifecycle, ScopeLifecycleEvent, StateCause, SyncEvent, SyncStateErrorKind,
};

use common::{StubAccount, StubFactory};

const EMAIL: CursorScope = CursorScope::Type(ObjectType::Email);
const EVENTS: CursorScope = CursorScope::Type(ObjectType::CalendarEvent);

/// Upper bound on a wait for a poll-driven event. The clock is paused, so this
/// is virtual time; it has to outlast the default 60s poll cadence.
const BOUND: Duration = Duration::from_secs(300);

/// Longer than the default poll cadence, so a poll drive is certain to have run.
const POLL_CADENCE_PASSED: Duration = Duration::from_secs(200);

/// Derives `Engine(SchemaIncompatible)`.
fn schema_incompatible() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid account error classification")
}

/// An account with two scopes whose changes stream terminates with a schema
/// error once per unit stored in the returned counter, and is quiet otherwise.
fn failing_on_demand() -> (StubAccount, Arc<AtomicUsize>) {
    let fire = Arc::new(AtomicUsize::new(0));
    let hook_fire = Arc::clone(&fire);
    let mut account = StubAccount::new(vec![EMAIL, EVENTS]);
    account.changes_hook = Some(Arc::new(move |_cursor: &ChangeCursor| {
        let fires = hook_fire
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if fires {
            vec![SyncEvent::Terminated(schema_incompatible())]
        } else {
            Vec::<SyncEvent<Change>>::new()
        }
    }));
    (account, fire)
}

async fn durable_cursor_exists(
    store: &InMemoryCheckpointStore,
    id: &AccountId,
    scope: &CursorScope,
) -> bool {
    store
        .get_change_cursor(id, scope)
        .await
        .expect("cursor lookup")
        .is_some()
}

fn established(account: &StubAccount) -> Vec<CursorScope> {
    account
        .established
        .lock()
        .expect("established lock")
        .clone()
}

fn engine_over(store: &Arc<InMemoryCheckpointStore>) -> Arc<SyncEngine> {
    Arc::new(
        SyncEngine::builder()
            .checkpoints(Arc::clone(store) as Arc<dyn CheckpointStore>)
            .build()
            .expect("default engine config is valid"),
    )
}

/// Schema recovery reaches a paused account.
///
/// Staged with the reopen lock, as in `scope_repair_pause`: a parked
/// `subscribe_push` holds it, so the directive the failing scope raises queues
/// on the lock. The account is then paused and only then is the lock released.
/// Recovery must decline without deleting ANY cursor, and after resume the
/// parked obligation re-runs the reset, which then re-establishes both scopes.
///
/// The stub's `fire` arms ONE failure, so it is not re-armed before resume:
/// a production schema-stale cursor fails at every poll and would re-raise the
/// directive too, and which of the poll and the parked reset reaches the
/// listener first would then decide the outcome. That path is pinned
/// deterministically by `a_parked_schema_reset_after_a_completed_one_only_establishes`.
///
/// Ablations: without the activity gate at the top of
/// `handle_schema_incompatible` the reset runs on the paused account, the
/// durable cursors are cleared while paused, and the re-establishment is
/// refused as `Paused`; without the parked reset (the decline returning
/// `RecoveryFollowUp::None`), nothing re-raises the directive and the count
/// stays at 2.
#[tokio::test(start_paused = true)]
async fn schema_recovery_reaching_a_paused_account_deletes_nothing_and_runs_after_resume() {
    let id = AccountId("schema-pause-declined".to_owned());
    let (account, fire) = failing_on_demand();
    let park = Arc::new(tokio::sync::Notify::new());
    *account
        .push_subscribe_park
        .lock()
        .expect("subscribe park lock") = Some((0, Arc::clone(&park)));
    let push_log = Arc::clone(&account.push_log);
    let account = Arc::new(account);
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");
    for scope in [EMAIL, EVENTS] {
        assert!(
            durable_cursor_exists(&store, &id, &scope).await,
            "attach must have persisted {scope:?}, or survival proves nothing"
        );
    }
    assert_eq!(established(&account).len(), 2);

    // Hold the reopen lock inside a parked push subscription.
    let subscribing = {
        let engine = Arc::clone(&engine);
        let id = id.clone();
        tokio::spawn(async move { engine.subscribe_push(&id, &[EMAIL]).await })
    };
    tokio::time::timeout(BOUND, async {
        while push_log.subscribed().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the subscription never reached the account");

    // A scope's drive raises the directive; the dispatch queues on the lock.
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
    for scope in [EMAIL, EVENTS] {
        assert!(
            durable_cursor_exists(&store, &id, &scope).await,
            "schema recovery on a paused account deleted {scope:?}'s durable cursor"
        );
    }
    assert_eq!(
        established(&account).len(),
        2,
        "nothing may be re-established while the account is paused"
    );

    // After resume the parked obligation re-runs the reset.
    control.resume();
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        established(&account).len(),
        4,
        "after resume the parked reset must recover both scopes"
    );
    for scope in [EMAIL, EVENTS] {
        assert!(durable_cursor_exists(&store, &id, &scope).await);
    }

    engine.detach(&id).await.expect("detach");
}

/// A pause lands while schema recovery is between two establishment attempts.
///
/// Every cursor is already deleted and the first attempt failed, so recovery
/// sleeps its backoff. The pause must wait for the recovery (which holds one
/// registration for all of it), and the retry must not be refused by the
/// boundary the pause flipped.
///
/// Ablation, hold: without the registration the pause reports quiescence
/// mid-recovery. Ablation, admitted control: re-establishing on the plain control
/// makes the retry (and the sibling scope) fail with `Paused`, so the cursors
/// deleted up front are never re-established.
#[tokio::test(start_paused = true)]
async fn a_pause_landing_mid_schema_recovery_waits_for_it_instead_of_stranding_scopes() {
    let id = AccountId("schema-pause-mid".to_owned());
    let (account, fire) = failing_on_demand();
    let account = Arc::new(account);
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");
    assert_eq!(established(&account).len(), 2);

    // The first establishment attempt of the recovery fails; the retry works.
    account.establish_failures.store(1, Ordering::SeqCst);
    fire.store(1, Ordering::SeqCst);
    tokio::time::timeout(BOUND, account.establish_failed.notified())
        .await
        .expect("recovery never attempted its establishment");

    // Recovery is now in its backoff, holding its activity registration.
    let mut pause = Box::pin(control.pause());
    tokio::select! {
        biased;
        result = &mut pause => panic!("pause reported quiescence mid-recovery: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    pause
        .await
        .expect("the pause completes once the recovery has finished");

    assert_eq!(
        established(&account).len(),
        5,
        "attach's two, the failed attempt, its retry, and the sibling scope, none refused"
    );
    for scope in [EMAIL, EVENTS] {
        assert!(
            durable_cursor_exists(&store, &id, &scope).await,
            "the pause stranded {scope:?}: its cursor was deleted and never re-established"
        );
    }

    control.resume();
    engine.detach(&id).await.expect("detach");
}

fn new_folder() -> FolderId {
    FolderId("created".to_owned())
}

/// A lifecycle account: one folder scope, and a lifecycle stream the test drives.
fn lifecycle_account() -> (
    Arc<StubAccount>,
    tokio::sync::mpsc::Sender<ScopeLifecycleEvent>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let mut account = StubAccount::new(vec![CursorScope::Folder(FolderId("inbox".to_owned()))]);
    account.lifecycle_rx = std::sync::Mutex::new(Some(rx));
    (Arc::new(account), tx)
}

async fn create_folder(tx: &tokio::sync::mpsc::Sender<ScopeLifecycleEvent>) {
    tx.send(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
        MembershipScope::Folder(new_folder()),
    )))
    .await
    .expect("the lifecycle stream accepts the event");
}

fn created_scope() -> CursorScope {
    CursorScope::Folder(new_folder())
}

fn times_established(account: &StubAccount, scope: &CursorScope) -> usize {
    established(account)
        .iter()
        .filter(|established| *established == scope)
        .count()
}

/// A `Created` scope whose establishment is asked for while the account is
/// paused is established once the account resumes.
///
/// The repair declines under the pause, and the scope has no cursor for the poll
/// to re-raise it from, so without a deferral it is lost for good. Two `Created`
/// events land during the pause: the scope is parked once, so it is established
/// exactly once (a second parked repair would restart the cursor the first one
/// had just made).
///
/// Ablation: dropping the parked re-offer (the listener discarding the declined
/// scope) leaves the scope never established, durably or otherwise. Ablation,
/// dedupe: parking twice establishes it twice.
#[tokio::test(start_paused = true)]
async fn a_created_scope_declined_under_a_pause_is_established_after_resume() {
    let id = AccountId("created-pause".to_owned());
    let (account, lifecycle) = lifecycle_account();
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    control.pause().await.expect("an idle account pauses");
    create_folder(&lifecycle).await;
    create_folder(&lifecycle).await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        0,
        "nothing may be established while the account is paused"
    );

    control.resume();
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        1,
        "the declined scope must be re-offered after resume, exactly once"
    );
    assert!(
        durable_cursor_exists(&store, &id, &created_scope()).await,
        "the established scope's cursor must be durable"
    );

    engine.detach(&id).await.expect("detach");
}

async fn delete_folder(tx: &tokio::sync::mpsc::Sender<ScopeLifecycleEvent>) {
    tx.send(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Deleted(
        MembershipScope::Folder(new_folder()),
    )))
    .await
    .expect("the lifecycle stream accepts the event");
}

/// A parked scope does not outlive its folder. Created and then deleted while
/// the account is paused, it must not be established after resume: the folder
/// is gone, and establishing it would end in the retry-budget terminal event.
///
/// The deletion reaches nothing through the registry - a parked scope has no
/// cursor - so it has to be routed to the parked set explicitly.
///
/// Ablation: dropping the unpark (or routing only registered scopes to
/// `ScopeDeleted`) establishes the deleted folder after resume.
#[tokio::test(start_paused = true)]
async fn a_parked_scope_whose_folder_is_deleted_during_the_pause_is_not_established() {
    let id = AccountId("created-deleted-pause".to_owned());
    let (account, lifecycle) = lifecycle_account();
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    control.pause().await.expect("an idle account pauses");
    create_folder(&lifecycle).await;
    delete_folder(&lifecycle).await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    control.resume();
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        0,
        "a folder deleted during the pause must not be established after resume"
    );

    engine.detach(&id).await.expect("detach");
}

/// Deleted and created AGAIN during one pause, the folder exists after resume
/// and is established exactly once: the second `Created` parks it afresh under
/// a later lifecycle epoch, and only the task that captured that epoch may
/// claim the entry. The exact epochs depend on whether the listener handled the
/// first `Created` before or after the multiplexer's bump (each deletion bumps
/// twice, once per front end), so none is asserted.
///
/// Ablation, epoch ownership: make the park task remove its entry
/// unconditionally and send its captured epoch - the older task can then
/// consume the newer obligation, its stale request is discarded at execution,
/// and the scope is never established. Which task wakes first is
/// scheduler-dependent here, so `a_park_claim_under_a_stale_epoch_takes_nothing`
/// pins the rule deterministically.
#[tokio::test(start_paused = true)]
async fn a_folder_deleted_and_recreated_during_a_pause_is_established_once() {
    let id = AccountId("created-deleted-created-pause".to_owned());
    let (account, lifecycle) = lifecycle_account();
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    control.pause().await.expect("an idle account pauses");
    create_folder(&lifecycle).await;
    delete_folder(&lifecycle).await;
    create_folder(&lifecycle).await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    control.resume();
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        1,
        "the recreated folder is established once after resume"
    );

    engine.detach(&id).await.expect("detach");
}

/// On a RUNNING account, a folder deleted while its `Created` repair is still
/// queued on the reopen listener is established by that repair, and the
/// deletion that follows on the same channel must retire the cursor, not only
/// purge its rows. Purging alone left a live cursor for a deleted folder, its
/// durable rows deleted out from under it.
///
/// Staged with the reopen lock: a parked `subscribe_push` holds it, so the
/// `Created` repair queues behind it and the `Deleted` queues behind that.
///
/// Ablation: dropping the arm's cursor retirement leaves the deleted folder's
/// cursor polling after the deletion was handled.
#[tokio::test(start_paused = true)]
async fn a_folder_deleted_while_its_repair_is_queued_leaves_no_live_cursor() {
    let id = AccountId("created-deleted-queued".to_owned());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let mut account = StubAccount::new(vec![CursorScope::Folder(FolderId("inbox".to_owned()))]);
    account.lifecycle_rx = std::sync::Mutex::new(Some(rx));
    let polls = Arc::new(AtomicUsize::new(0));
    let hook_polls = Arc::clone(&polls);
    account.changes_hook = Some(Arc::new(move |cursor: &ChangeCursor| {
        if cursor.scope == created_scope() {
            hook_polls.fetch_add(1, Ordering::SeqCst);
        }
        Vec::<SyncEvent<Change>>::new()
    }));
    let park = Arc::new(tokio::sync::Notify::new());
    *account
        .push_subscribe_park
        .lock()
        .expect("subscribe park lock") = Some((0, Arc::clone(&park)));
    let push_log = Arc::clone(&account.push_log);
    let account = Arc::new(account);
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    let subscribing = {
        let engine = Arc::clone(&engine);
        let id = id.clone();
        let inbox = CursorScope::Folder(FolderId("inbox".to_owned()));
        tokio::spawn(async move { engine.subscribe_push(&id, &[inbox]).await })
    };
    tokio::time::timeout(BOUND, async {
        while push_log.subscribed().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the subscription never reached the account");

    create_folder(&tx).await;
    delete_folder(&tx).await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    park.notify_one();
    subscribing
        .await
        .expect("subscribe task")
        .expect("subscribe succeeds once released");

    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        1,
        "the queued repair established the folder, or the race was never staged"
    );
    let settled = polls.load(Ordering::SeqCst);
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        polls.load(Ordering::SeqCst),
        settled,
        "a deleted folder's cursor must not keep polling"
    );
    assert!(
        !durable_cursor_exists(&store, &id, &created_scope()).await,
        "the deleted folder's durable cursor is purged"
    );

    engine.detach(&id).await.expect("detach");
}

/// A parked scope does not outlive the account: detach while it waits ends the
/// wait and nothing is established.
#[tokio::test(start_paused = true)]
async fn a_parked_scope_is_dropped_by_detach() {
    let id = AccountId("created-detach".to_owned());
    let (account, lifecycle) = lifecycle_account();
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    control.pause().await.expect("an idle account pauses");
    create_folder(&lifecycle).await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    engine.detach(&id).await.expect("detach");
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        0,
        "a detached account must not establish a parked scope"
    );
}

/// Schema recovery clears the rows of a scope the account never discovers, so
/// the registry never names it: its surviving completion marker would make a
/// later establishment skip the inventory walk that re-mints its ids.
///
/// Ablation: replace the writer's account clear with a per-registered-scope
/// `ResetScope { delete_backfill: true }` loop (the contact rows survive).
#[tokio::test(start_paused = true)]
async fn schema_recovery_clears_the_completion_marker_of_an_unregistered_scope() {
    let id = AccountId("schema-unregistered".into());
    let store = Arc::new(InMemoryCheckpointStore::new());
    let absent = CursorScope::Type(ObjectType::Contact);
    store
        .put_change_cursor(&id, common::cursor_for(&absent, b"old"))
        .await
        .expect("seed");
    store
        .put_backfill(
            &id,
            bifrost_types::BackfillCheckpoint {
                scope: absent.clone(),
                partition: bifrost_sync::backfill::partitioner::completion_partition(),
                progress_marker: None,
                progress: Default::default(),
                envelope_version: 1,
            },
        )
        .await
        .expect("seed marker");
    let (account, fire) = failing_on_demand();
    let account = Arc::new(account);
    let engine = engine_over(&store);
    engine
        .attach(
            id.clone(),
            Arc::new(StubFactory::queue(vec![Arc::clone(&account)])),
        )
        .await
        .expect("attach");
    fire.store(1, Ordering::SeqCst);
    tokio::time::timeout(BOUND, async {
        while established(&account).len() < 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovery");
    assert!(
        store
            .get_change_cursor(&id, &absent)
            .await
            .expect("read")
            .is_none()
    );
    assert!(
        store
            .get_backfill(&id, &absent)
            .await
            .expect("read")
            .is_none()
    );
    engine.detach(&id).await.expect("detach");
}

struct FlakyClearStore {
    inner: InMemoryCheckpointStore,
    fail: std::sync::atomic::AtomicBool,
}
impl CheckpointStore for FlakyClearStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: bifrost_sync::cursor::DebtLedger,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        if self.fail.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(bifrost_sync::Error::CheckpointStore("clear refused".into()))
            });
        }
        self.inner.clear_account_checkpoints(account, ledger)
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: bifrost_sync::CheckpointTransition,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.apply_transition(account, transition)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: bifrost_sync::cursor::DebtLedger,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<bifrost_sync::cursor::DebtLedger, bifrost_sync::Error>,
                > + Send
                + 'a,
        >,
    > {
        self.inner.get_ledger(account)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<ChangeCursor>, bifrost_sync::Error>>
                + Send
                + 'a,
        >,
    > {
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::BackfillCheckpoint>, bifrost_sync::Error>,
                > + Send
                + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.delete_backfill(account, scope)
    }
}
/// An account clear that fails through the whole retry budget pauses the
/// account with `RetryBudgetExhausted` and parks the reset with the scopes it
/// owes; once the store recovers and the account resumes, the parked reset
/// re-runs and re-establishes them. Paused time: the writer attempts back off
/// 1s then 2s, which under real time would be wall-clock sleeps.
///
/// Ablation: return `RecoveryFollowUp::None` on exhaustion (nothing is
/// established after resume).
#[tokio::test(start_paused = true)]
async fn a_schema_reset_whose_clear_keeps_failing_pauses_and_owes_its_scopes() {
    let id = AccountId("schema-clear-failure".into());
    let store = Arc::new(FlakyClearStore {
        inner: InMemoryCheckpointStore::new(),
        fail: std::sync::atomic::AtomicBool::new(true),
    });
    let (account, fire) = failing_on_demand();
    let account = Arc::new(account);
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("engine");
    let control = engine
        .attach(
            id.clone(),
            Arc::new(StubFactory::queue(vec![Arc::clone(&account)])),
        )
        .await
        .expect("attach");
    let mut events = engine.account_control_stream(&id).expect("control stream");
    fire.store(1, Ordering::SeqCst);
    tokio::time::timeout(BOUND, async {
        loop {
            if matches!(
                events.recv().await.expect("control"),
                bifrost_types::AccountControl::Pause(
                    bifrost_types::PauseReason::RetryBudgetExhausted
                )
            ) {
                break;
            }
        }
    })
    .await
    .expect("pause");
    assert_eq!(established(&account).len(), 2);
    store.fail.store(false, Ordering::SeqCst);
    control.resume();
    tokio::time::timeout(BOUND, async {
        while established(&account).len() < 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovery");
    for scope in [EMAIL, EVENTS] {
        assert!(
            store
                .get_change_cursor(&id, &scope)
                .await
                .expect("read")
                .is_some()
        );
    }
    engine.detach(&id).await.expect("detach");
}
