//! `detach`'s teardown phases each get their OWN `detach_timeout` budget.
//!
//! There are three: the worker awaits, the writer phase (the teardown discard
//! drain plus the ack writer's final drain), and `Account::close()`. Sharing one
//! deadline across them silently zeroes whichever phase runs last, which is a
//! bug twice over - once for the close, which a straggler worker could report as
//! hung on a path where nothing was wrong, and once for the writer, which a
//! straggler worker could get aborted with unpersisted acknowledged work, the
//! exact outcome the two-phase ordering and `take_ack_writer` exist to prevent.
//!
//! A fifth test pins the other side of the worker phase: a deferred inventory
//! walk parked on its provider stream is no straggler, because it answers the
//! slot's cancel.
//!
//! Every test runs current-thread under `start_paused`, so the elapsed readings
//! below are virtual and exact rather than wall-clock guesses; there are no real
//! sleeps here.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::{CheckpointStore, EngineConfig, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, CursorScope};

async fn attach(account_id: &AccountId, stub: Arc<common::StubAccount>) -> SyncEngine {
    let engine = SyncEngine::builder()
        .config(EngineConfig::default())
        .checkpoints(Arc::new(InMemoryCheckpointStore::default()) as Arc<dyn CheckpointStore>)
        .build()
        .expect("engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(common::StubFactory::queue(vec![stub]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    engine
}

/// A `close()` that never answers costs `detach` its timeout, and no more.
///
/// The gate is never notified, so the provider's close is hung for good. Without
/// the clamp `detach` never returns and the outer timeout below is what fires -
/// which is the ablation: remove the `tokio::time::timeout` around `close()` in
/// `detach_inner` and this test fails on "the detach is clamped, not hung".
///
/// The elapsed assertion is what separates this from the refusal case in the
/// next test: a clamped hang spends the whole budget, a refusal spends none of
/// it. Both end with the same warn-and-drop treatment, so the outcome alone
/// cannot tell them apart.
#[tokio::test(start_paused = true)]
async fn a_close_that_never_returns_does_not_hang_detach() {
    let account_id = AccountId("detach-close-hangs".to_owned());
    let mut stub = common::StubAccount::new(vec![CursorScope::Account]);
    stub.close_gate = Some(Arc::new(tokio::sync::Notify::new()));
    let stub = Arc::new(stub);
    let closed = Arc::clone(&stub.closed);
    let engine = attach(&account_id, Arc::clone(&stub)).await;

    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        EngineConfig::default().detach_timeout * 10,
        engine.detach(&account_id),
    )
    .await
    .expect("the detach is clamped, not hung")
    .expect("a close that hangs is not reported to the caller, exactly as a close that fails");
    let elapsed = started.elapsed();

    assert_eq!(
        closed.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the close was reached; it simply never answered"
    );
    assert_eq!(
        stub.close_returned
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "and it never answered: the future was dropped at the deadline"
    );
    assert!(
        elapsed >= EngineConfig::default().detach_timeout,
        "a hung close must be given its whole budget before the handle is abandoned; \
         waited {elapsed:?}"
    );
    // The handle is dropped rather than retained, and nothing can reach it: the
    // slot left `engine.accounts` at the top of the teardown.
    assert!(
        engine.account_changes_stream(&account_id).is_err(),
        "the account is detached whatever its close did"
    );
}

/// A `close()` that REFUSES is absorbed with no wait at all.
///
/// The counterpart reading. The two take different paths to the same outcome -
/// warn, drop the handle, report success to the caller - and this is what pins
/// that the clamp did not turn every close into a timeout: a provider that
/// answers promptly, even with an error, spends none of the budget.
#[tokio::test(start_paused = true)]
async fn a_close_that_fails_is_absorbed_without_spending_the_budget() {
    let account_id = AccountId("detach-close-fails".to_owned());
    let mut stub = common::StubAccount::new(vec![CursorScope::Account]);
    stub.close_fails = true;
    let stub = Arc::new(stub);
    let closed = Arc::clone(&stub.closed);
    let engine = attach(&account_id, Arc::clone(&stub)).await;

    let started = tokio::time::Instant::now();
    engine
        .detach(&account_id)
        .await
        .expect("a failed close is logged, not returned");
    let elapsed = started.elapsed();

    assert_eq!(closed.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        stub.close_returned
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the refusal is an answer: the future ran to completion"
    );
    assert!(
        elapsed < EngineConfig::default().detach_timeout,
        "a close that answers immediately must not be made to wait; waited {elapsed:?}"
    );
    assert!(engine.account_changes_stream(&account_id).is_err());
}

/// The clamp is a FRESH budget, not the remainder of the worker deadline.
///
/// A straggler worker that burns the whole `detach_timeout` must not take the
/// close budget with it: a perfectly healthy close needing one round trip would
/// then be reported as hung and its connection abandoned on a path where nothing
/// was wrong. The straggler here is the deferred-inventory worker, parked in the
/// membership refresh that follows a successful inventory establishment - one of
/// the workers `detach` actually joins, and a provider poll with no shutdown arm,
/// so it is aborted only at the deadline. The close, which answers only after a
/// delay of its own, still completes.
///
/// Two stagings that do NOT work, both established by measurement. A parked
/// CHANGES stream: the multiplexer owns its per-scope poll tasks and retires
/// them itself in its shutdown tail (it CANCELS their tokens - the explicit abort
/// there is for the lifecycle task), so they are not among the workers `detach`
/// waits on, and a first version of this test staged that way detached in 11ms.
/// And a parked INVENTORY stream, which this test used until the fusion walk
/// gained its shutdown arm: detach now drops that stream as soon as it cancels
/// the slot. The elapsed assertion below is what catches either, and is why it
/// stays.
///
/// Ablation: switch the clamp to `timeout_at(deadline, ...)` on the shared
/// worker deadline and the close is cut off before it answers - `closed` still
/// reads 1, since that counter is bumped on entry, and `close_returned`, which
/// is bumped only past the gate, stays at 0.
#[tokio::test(start_paused = true)]
async fn a_slow_close_after_a_straggler_worker_still_completes() {
    let account_id = AccountId("detach-close-after-straggler".to_owned());
    let mut stub = common::StubAccount::new(vec![
        CursorScope::Account,
        CursorScope::Type(bifrost_types::ObjectType::Contact),
    ]);
    stub.establishment = |scope| match scope {
        CursorScope::Type(_) => bifrost_types::CursorEstablishment::EstablishViaInventory,
        other => {
            bifrost_types::CursorEstablishment::Ready(common::cursor_for(other, b"stub-ready"))
        }
    };
    stub.inventory_hook = Some(common::establishing_inventory());
    let gate = Arc::new(tokio::sync::Notify::new());
    stub.close_gate = Some(Arc::clone(&gate));
    let stub = Arc::new(stub);
    let closed = Arc::clone(&stub.closed);
    let engine = attach(&account_id, Arc::clone(&stub)).await;
    // Armed after attach, which walks memberships itself. Never notified: the
    // deferred-inventory worker cannot leave its membership refresh, so the
    // worker deadline is spent in full and the close begins with none of it left.
    let (probe, parked, _destroyed) = common::StallProbe::install();
    *stub.memberships_stall.lock().expect("stall lock") =
        Some((Arc::new(tokio::sync::Notify::new()), Some(probe)));
    // The deferred worker waits for a real subscriber before it walks anything,
    // so the straggler does not exist until somebody subscribes.
    let _subscriber = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    tokio::time::timeout(Duration::from_secs(120), parked)
        .await
        .expect("the deferred inventory worker reaches its parked membership refresh")
        .expect("the parked signal is not dropped");

    // Release the close a beat after it is entered, so it is genuinely slow and
    // genuinely finite. `notify_waiters` only reaches a waiter already parked,
    // which the poll below establishes.
    let releasing = tokio::spawn({
        let closed = Arc::clone(&closed);
        async move {
            while closed.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            gate.notify_waiters();
        }
    });

    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        EngineConfig::default().detach_timeout * 10,
        engine.detach(&account_id),
    )
    .await
    .expect("the detach ends")
    .expect("detach succeeds");
    let elapsed = started.elapsed();
    releasing.await.expect("the releaser does not panic");
    assert!(
        elapsed >= EngineConfig::default().detach_timeout,
        "the staging requires a worker that really is a straggler: the parked \
         inventory worker must have been aborted at the worker deadline, so the close \
         begins with none of that deadline left. waited {elapsed:?}"
    );

    assert_eq!(
        closed.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the close is reached even though a worker had to be aborted at the deadline"
    );
    assert_eq!(
        stub.close_returned
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "and it RAN TO COMPLETION: the straggler worker spent the worker deadline, \
         not the close's budget"
    );
}

/// The bytes that mark the one consumer acknowledgement [`StallingAckStore`]
/// parks on. Everything else the engine writes - establishment cursors, ledger
/// updates - goes straight through, so the stall is aimed at the writer's final
/// drain and nothing else.
const ACKED_STATE: &[u8] = b"consumer-acked";

/// An `InMemoryCheckpointStore` whose `apply_transition` PARKS on one specific
/// consumer acknowledgement until released.
///
/// This is how "the ack writer still has outstanding acknowledged work when the
/// worker phase ends" is staged. The writer picks the ack up, enters the store,
/// and is still inside it when `detach` reaches its writer phase.
struct StallingAckStore {
    inner: InMemoryCheckpointStore,
    gate: Arc<tokio::sync::Notify>,
    /// Bumped when the gated write is ENTERED, before the park, so the test can
    /// wait for the writer to really be inside the store.
    entered: Arc<std::sync::atomic::AtomicUsize>,
    /// Bumped when the gated write is about to delegate to the inner store. A
    /// writer aborted mid-park leaves this behind `entered`.
    released: Arc<std::sync::atomic::AtomicUsize>,
}

impl StallingAckStore {
    fn new(gate: Arc<tokio::sync::Notify>) -> Self {
        Self {
            inner: InMemoryCheckpointStore::default(),
            gate,
            entered: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            released: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn is_gated(transition: &bifrost_sync::CheckpointTransition) -> bool {
        matches!(
            &transition.checkpoint,
            bifrost_types::Checkpoint::Change(cursor)
                if cursor.server_state.bytes == ACKED_STATE
        )
    }
}

impl CheckpointStore for StallingAckStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: bifrost_sync::cursor::DebtLedger,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.clear_account_checkpoints(account, ledger)
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: bifrost_sync::CheckpointTransition,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        if !Self::is_gated(&transition) {
            return self.inner.apply_transition(account, transition);
        }
        let gate = Arc::clone(&self.gate);
        let entered = Arc::clone(&self.entered);
        let released = Arc::clone(&self.released);
        Box::pin(async move {
            // The park future is created BEFORE the counter is published, so a
            // releaser reacting to the counter cannot notify into the gap
            // between the two and be missed by `notify_waiters`.
            let parked = gate.notified();
            entered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            parked.await;
            released.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.apply_transition(account, transition).await
        })
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::ChangeCursor>, bifrost_sync::Error>,
                > + Send
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

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: bifrost_sync::DebtLedger,
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
            dyn std::future::Future<Output = Result<bifrost_sync::DebtLedger, bifrost_sync::Error>>
                + Send
                + 'a,
        >,
    > {
        self.inner.get_ledger(account)
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

/// A straggler worker must not cost the ack writer its final drain.
///
/// The defect this pins: `detach` handed the stream workers and the ack writer
/// ONE deadline, so a provider stream that neither yields nor ends burnt the
/// whole `detach_timeout` in the worker phase and `await_worker_until` then
/// reached the writer with `remaining == 0` and aborted it IMMEDIATELY, with no
/// drain at all. Acknowledged work the writer was carrying was thrown away by a
/// worker that had nothing to do with it - the same "writer aborted with
/// unpersisted work" outcome the writer-last ordering exists to prevent, reached
/// from the other side.
///
/// Staging, in order:
/// - the deferred-inventory worker establishes its scope through a normal
///   inventory walk, then parks in the membership refresh that follows it, on a
///   provider stream that never ends. It is one of the workers `detach` actually
///   joins and that poll has no shutdown arm, so it is retired only by the
///   worker deadline's abort. A parked CHANGES stream does NOT work: the
///   multiplexer owns its per-scope poll tasks and retires them itself in its
///   shutdown tail (cancelling their tokens; the explicit abort there is for the
///   lifecycle task), so they are not among the workers `detach` joins and a
///   test staged that way detaches in milliseconds. Nor, any longer, does a
///   parked INVENTORY stream: the fusion walk polls it under the slot's shutdown
///   token.
/// - a consumer acknowledgement is in flight and the writer is parked INSIDE
///   the checkpoint store when `detach` starts, which `entered` establishes.
/// - the store is released a beat AFTER the worker deadline would have expired,
///   so only a writer given budget of its own is still there to finish.
///
/// **The straggler's own staging is pinned, not assumed.** Elapsed time alone
/// does not pin it: the store releases on its own timer at
/// `detach_timeout + 100ms`, so an `elapsed >= detach_timeout` assertion holds
/// even if the parked worker had gained a shutdown arm and left immediately.
/// What is measured instead is the stalled provider stream's DESTRUCTION, taken
/// from inside its `Drop` (`StallProbe`) rather than when this task receives the
/// signal, since a receive is subject to scheduling delay. The memberships gate
/// is never released, so the only thing that can destroy that stream is the
/// worker deadline's abort, and the assertion is that it happened at least one
/// full worker budget after `detach` began.
///
/// That probe already earned its keep once. This test used to park the
/// INVENTORY stream, and giving the fusion walk its shutdown arm - exactly the
/// change the probe was written to detect - is what failed it and forced the
/// restaging onto the membership refresh.
///
/// Two ablations, two separate claims:
/// - pass the worker phase's `deadline` to `await_ack_writer_until` instead of
///   `writer_deadline` and the writer is aborted while parked in the store -
///   `released` stays at 0, the cursor never lands, and the acknowledging caller
///   gets "ack writer dropped before persisting".
/// - give the deferred worker's membership refresh a cancellation arm, so the
///   provider stream is dropped promptly on the slot token: the destruction-time
///   assertion fails even though the store's own timer still holds total elapsed
///   above the worker budget.
#[tokio::test(start_paused = true)]
async fn a_straggler_worker_does_not_cost_the_ack_writer_its_drain() {
    let account_id = AccountId("detach-writer-drain".to_owned());
    let mut stub = common::StubAccount::new(vec![
        CursorScope::Account,
        CursorScope::Type(bifrost_types::ObjectType::Contact),
    ]);
    stub.establishment = |scope| match scope {
        CursorScope::Type(_) => bifrost_types::CursorEstablishment::EstablishViaInventory,
        other => {
            bifrost_types::CursorEstablishment::Ready(common::cursor_for(other, b"stub-ready"))
        }
    };
    stub.inventory_hook = Some(common::establishing_inventory());
    let stub = Arc::new(stub);

    let store_gate = Arc::new(tokio::sync::Notify::new());
    let store = Arc::new(StallingAckStore::new(Arc::clone(&store_gate)));
    let entered = Arc::clone(&store.entered);
    let released = Arc::clone(&store.released);
    let engine = SyncEngine::builder()
        .config(EngineConfig::default())
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("engine config is valid");
    let factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![Arc::clone(&stub)]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    let engine = Arc::new(engine);

    // Armed after attach, which walks memberships itself. Never notified: the
    // deferred-inventory worker cannot leave its membership refresh, so the
    // worker phase is spent in full and the ONLY thing that can destroy the
    // provider stream is the worker deadline's abort.
    let (probe, parked, destroyed) = common::StallProbe::install();
    *stub.memberships_stall.lock().expect("stall lock") =
        Some((Arc::new(tokio::sync::Notify::new()), Some(probe)));

    // The deferred worker waits for a real subscriber before it walks anything,
    // so the straggler does not exist until somebody subscribes.
    let _subscriber = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    // Not "the stream was constructed" but "its tail is actually parked": the
    // straggler must exist before detach starts, or the destruction measurement
    // below is measuring nothing.
    tokio::time::timeout(Duration::from_secs(120), parked)
        .await
        .expect("the deferred inventory worker reaches its parked membership refresh")
        .expect("the parked signal is not dropped");

    // Acknowledged work, outstanding: the writer takes it and parks in the store.
    let acking = tokio::spawn({
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        async move {
            engine
                .ack_checkpoint(
                    &account_id,
                    CursorScope::Account,
                    bifrost_types::Checkpoint::Change(common::cursor_for(
                        &CursorScope::Account,
                        ACKED_STATE,
                    )),
                    None,
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(120), async {
        while entered.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the ack writer reaches the checkpoint store");
    assert!(
        !acking.is_finished(),
        "the staging requires the acknowledgement to still be in flight when the \
         detach begins; a finished one persists nothing to observe"
    );

    // Released a beat AFTER the worker deadline, so the drain can only finish on
    // a budget the worker phase did not already spend.
    let releasing = tokio::spawn(async move {
        tokio::time::sleep(EngineConfig::default().detach_timeout + Duration::from_millis(100))
            .await;
        store_gate.notify_waiters();
    });

    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        EngineConfig::default().detach_timeout * 10,
        engine.detach(&account_id),
    )
    .await
    .expect("the detach ends")
    .expect("detach succeeds");
    let elapsed = started.elapsed();
    releasing.await.expect("the releaser does not panic");

    // The staging's own premise, pinned directly rather than through elapsed
    // time. The stalled provider stream can only be destroyed by the worker
    // deadline's abort (its gate is never released), and the instant comes from
    // inside `Drop`, so this says the straggler really did hold the worker phase
    // for its whole budget. Elapsed time cannot say that here: the store's
    // release timer alone would carry it past `detach_timeout`.
    let destroyed_at = tokio::time::timeout(Duration::from_secs(1), destroyed)
        .await
        .expect("the stalled memberships stream is destroyed")
        .expect("the drop stamp is delivered");
    assert!(
        destroyed_at.duration_since(started) >= EngineConfig::default().detach_timeout,
        "the staging requires a worker that really is a straggler: the parked \
         memberships stream must survive until the worker deadline aborts it, so the \
         writer phase begins with none of that deadline left. it was destroyed \
         {:?} after detach began",
        destroyed_at.duration_since(started)
    );
    assert!(
        elapsed < EngineConfig::default().detach_timeout * 2,
        "and the writer phase must not have burnt its own whole budget either - \
         the store answered a beat after the worker deadline. waited {elapsed:?}"
    );
    assert_eq!(
        released.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the writer was still alive to finish the store write: the straggler spent \
         the worker deadline, not the writer's drain budget"
    );
    acking
        .await
        .expect("the acknowledging task does not panic")
        .expect("the acknowledgement was persisted, not abandoned with the writer");

    let persisted = store
        .get_change_cursor(&account_id, &CursorScope::Account)
        .await
        .expect("the store answers")
        .expect("the acknowledged cursor is durable");
    assert_eq!(
        persisted.server_state.bytes, ACKED_STATE,
        "the durable row is the consumer's acknowledgement, not the establishment cursor"
    );
}

/// A deferred inventory walk parked on its provider stream leaves at detach,
/// not at the worker deadline.
///
/// The fusion walk polls the provider stream under the slot's shutdown token,
/// so the cancel `detach` issues drops the stream at once. Before that arm
/// existed this worker was the canonical straggler: a stream that neither
/// yields nor ends held `detach` for its whole `detach_timeout` and was then
/// aborted.
///
/// Measured from inside the stream's `Drop`, as the straggler test above does
/// and for the same reason: total elapsed would say nothing about which worker
/// held the phase. Ablation: poll the stream without the shutdown arm in
/// `InventoryFusion::run_stream` and the stream survives until the worker
/// deadline aborts it, failing the destruction-time assertion.
#[tokio::test(start_paused = true)]
async fn a_parked_inventory_walk_is_dropped_at_detach_not_at_the_deadline() {
    let account_id = AccountId("detach-inventory-walk-leaves".to_owned());
    let mut stub = common::StubAccount::new(vec![
        CursorScope::Account,
        CursorScope::Type(bifrost_types::ObjectType::Contact),
    ]);
    stub.establishment = |scope| match scope {
        CursorScope::Type(_) => bifrost_types::CursorEstablishment::EstablishViaInventory,
        other => {
            bifrost_types::CursorEstablishment::Ready(common::cursor_for(other, b"stub-ready"))
        }
    };
    // Never notified: only the shutdown arm can end this walk early.
    stub.inventory_stall = Some(Arc::new(tokio::sync::Notify::new()));
    let (probe, parked, destroyed) = common::StallProbe::install();
    *stub
        .inventory_stall_probe
        .lock()
        .expect("inventory stall probe lock") = Some(probe);
    let stub = Arc::new(stub);
    let engine = attach(&account_id, Arc::clone(&stub)).await;
    let _subscriber = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    tokio::time::timeout(Duration::from_secs(120), parked)
        .await
        .expect("the deferred inventory worker parks in its walk")
        .expect("the parked signal is not dropped");

    let started = tokio::time::Instant::now();
    engine.detach(&account_id).await.expect("detach succeeds");
    let destroyed_at = tokio::time::timeout(Duration::from_secs(1), destroyed)
        .await
        .expect("the parked inventory stream is destroyed")
        .expect("the drop stamp is delivered");
    assert!(
        destroyed_at.duration_since(started) < EngineConfig::default().detach_timeout,
        "the walk must leave on the slot's cancel, not be aborted at the worker \
         deadline: the stream was destroyed {:?} after detach began",
        destroyed_at.duration_since(started)
    );
}
