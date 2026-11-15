//! A reattach future can be dropped at any await, and whatever it owns must not
//! outlive it.
//!
//! `SyncEngine::reattach` is a public future a consumer may time out, and the
//! reopen listener running the same code is aborted at the detach deadline. A
//! guard armed at construction owns the replacement connection, the cursor rows
//! it persisted provisionally, and (after the swap) the old connection. These
//! tests drop the future at the awaits that used to be uncovered: before any
//! replacement subscription existed, and during the final close of the old
//! account.
//!
//! Every drop is staged with the park gates, and every wait is a bounded yield
//! loop rather than a sleep.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bifrost_sync::{CheckpointStore, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{
    AccountFactory, AccountId, CursorScope, FolderId, PushCapability, SubscriptionHandle,
};

use common::{PushLog, StubAccount, StubFactory};

fn inbox() -> CursorScope {
    CursorScope::Folder(FolderId("inbox".into()))
}

/// A scope the replacement discovers and the old account did not, so a reattach
/// establishes it and persists a provisional cursor row.
fn archive() -> CursorScope {
    CursorScope::Folder(FolderId("archive".into()))
}

fn parked_forever() -> Arc<tokio::sync::Notify> {
    Arc::new(tokio::sync::Notify::new())
}

fn push_caps() -> bifrost_types::AccountCapabilities {
    let mut caps = common::caps();
    caps.push = PushCapability::Webhook;
    caps
}

async fn settle(mut done: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held");
}

/// Drive `SyncEngine::reattach` until `parked` holds, then drop the future
/// there, as a consumer timeout or a worker abort would.
async fn drop_reattach_once(engine: &SyncEngine, id: &AccountId, parked: impl FnMut() -> bool) {
    let fut = engine.reattach(id);
    tokio::pin!(fut);
    tokio::select! {
        biased;
        _ = &mut fut => panic!("reattach finished before it parked"),
        () = settle(parked) => {}
    }
}

async fn row_exists(store: &InMemoryCheckpointStore, id: &AccountId, scope: &CursorScope) -> bool {
    store
        .get_change_cursor(id, scope)
        .await
        .expect("cursor lookup")
        .is_some()
}

/// Wait, bounded, for a durable row to disappear.
async fn row_disappears(store: &InMemoryCheckpointStore, id: &AccountId, scope: &CursorScope) {
    for _ in 0..10_000 {
        if !row_exists(store, id, scope).await {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the row was never rolled back");
}

async fn engine_over(store: &Arc<InMemoryCheckpointStore>) -> SyncEngine {
    SyncEngine::builder()
        .checkpoints(Arc::clone(store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("engine")
}

/// A drop parked in the old side's teardown, with the replacement holding NO
/// subscription (it has no push capability, so nothing was recreated on it),
/// must still close the replacement. The guard used to act only when it held
/// subscription handles, so a replacement with none was simply forgotten.
///
/// Ablation: making the guard's `Drop` return early when `live` is empty leaves
/// `replacement.closed` at zero, and `settle` panics.
#[tokio::test]
async fn a_reattach_dropped_before_any_replacement_subscription_closes_the_replacement() {
    let id = AccountId("drop-before-subscription".into());
    let log = Arc::new(PushLog::default());
    let old = Arc::new(StubAccount {
        caps: push_caps(),
        push_label: "old".to_owned(),
        push_log: Arc::clone(&log),
        ..StubAccount::new(vec![inbox()])
    });
    *old.push_unsubscribe_park.lock().expect("park lock") = Some(parked_forever());
    let replacement = Arc::new(StubAccount {
        push_label: "replacement".to_owned(),
        push_log: Arc::clone(&log),
        ..StubAccount::new(vec![inbox()])
    });
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store).await;
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![
        Arc::clone(&old),
        Arc::clone(&replacement),
    ]));
    engine.attach(id.clone(), factory).await.expect("attach");
    engine
        .subscribe_push(&id, &[inbox()])
        .await
        .expect("consumer subscribes");

    drop_reattach_once(&engine, &id, || log.unsubscribed().len() == 1).await;
    settle(|| replacement.closed.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        log.unsubscribed(),
        vec![("old".to_owned(), SubscriptionHandle("old".to_owned()))],
        "nothing was created on the replacement, so nothing is deleted there"
    );
    assert_eq!(old.closed.load(Ordering::SeqCst), 0, "old stays installed");

    engine.detach(&id).await.expect("detach");
}

/// A dropped reattach owes the compensation for the cursor rows it persisted
/// provisionally. Without it they stay provisional until some later reattach
/// aborts (deleting them) or commits (promoting a topology that was never
/// installed).
///
/// Ablation: not arming `rollback_owed` (or not enqueueing it in `Drop`) leaves
/// the archive row in the store, and `row_disappears` panics.
#[tokio::test]
async fn a_dropped_reattach_rolls_back_the_cursor_rows_it_inserted() {
    let id = AccountId("drop-rolls-back".into());
    let log = Arc::new(PushLog::default());
    let old = Arc::new(StubAccount {
        caps: push_caps(),
        push_label: "old".to_owned(),
        push_log: Arc::clone(&log),
        ..StubAccount::new(vec![inbox()])
    });
    *old.push_unsubscribe_park.lock().expect("park lock") = Some(parked_forever());
    let replacement = Arc::new(StubAccount::new(vec![inbox(), archive()]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store).await;
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![
        Arc::clone(&old),
        Arc::clone(&replacement),
    ]));
    engine.attach(id.clone(), factory).await.expect("attach");
    engine
        .subscribe_push(&id, &[inbox()])
        .await
        .expect("consumer subscribes");
    assert!(!row_exists(&store, &id, &archive()).await);

    // The insert precedes the old side's teardown, so parking there means the
    // archive row has been written.
    drop_reattach_once(&engine, &id, || log.unsubscribed().len() == 1).await;
    assert_eq!(
        *replacement.established.lock().expect("established lock"),
        vec![archive()],
        "the reattach established the new scope, or the rollback proves nothing"
    );
    row_disappears(&store, &id, &archive()).await;
    assert!(
        row_exists(&store, &id, &inbox()).await,
        "a row that existed before the reattach is never touched by its rollback"
    );
    settle(|| replacement.closed.load(Ordering::SeqCst) == 1).await;

    engine.detach(&id).await.expect("detach");
}

/// A drop parked inside the final close of the OLD account, after the swap, must
/// not leak that connection, and must NOT roll back the rows the successful
/// swap installed.
///
/// The gate parks the close, so the dropped attempt's close never returns; the
/// guard re-issues it. `notify_one` rather than `notify_waiters`: the spawned
/// close counts on entry but may not have registered as a waiter yet, and a
/// stored permit reaches it either way.
///
/// Ablation: closing `previous` outside the guard leaves `old.closed` at one,
/// and `settle` panics.
#[tokio::test]
async fn a_reattach_dropped_while_closing_the_old_account_still_closes_it() {
    let id = AccountId("drop-in-final-close".into());
    let gate = parked_forever();
    let old = Arc::new(StubAccount {
        close_gate: Some(Arc::clone(&gate)),
        ..StubAccount::new(vec![inbox()])
    });
    let replacement = Arc::new(StubAccount::new(vec![inbox(), archive()]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store).await;
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![
        Arc::clone(&old),
        Arc::clone(&replacement),
    ]));
    engine.attach(id.clone(), factory).await.expect("attach");

    drop_reattach_once(&engine, &id, || old.closed.load(Ordering::SeqCst) == 1).await;
    settle(|| old.closed.load(Ordering::SeqCst) == 2).await;
    gate.notify_one();
    settle(|| old.close_returned.load(Ordering::SeqCst) == 1).await;

    assert!(
        row_exists(&store, &id, &archive()).await,
        "the swap had committed; a drop after it must not roll its rows back"
    );
    assert_eq!(
        replacement.closed.load(Ordering::SeqCst),
        0,
        "the replacement is the live account and stays open"
    );

    engine.detach(&id).await.expect("detach");
}

/// The normal commit owes nothing afterwards: the guard's drop neither rolls the
/// installed rows back nor closes either account a second time.
///
/// Ablation: leaving `rollback_owed` set across the swap makes the finished
/// future's drop delete the archive row.
#[tokio::test]
async fn a_committed_reattach_leaves_its_rows_and_owes_no_close() {
    let id = AccountId("commit-owes-nothing".into());
    let old = Arc::new(StubAccount::new(vec![inbox()]));
    let replacement = Arc::new(StubAccount::new(vec![inbox(), archive()]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store).await;
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![
        Arc::clone(&old),
        Arc::clone(&replacement),
    ]));
    engine.attach(id.clone(), factory).await.expect("attach");

    engine.reattach(&id).await.expect("commit");
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert!(row_exists(&store, &id, &archive()).await);
    assert_eq!(old.closed.load(Ordering::SeqCst), 1);
    assert_eq!(replacement.closed.load(Ordering::SeqCst), 0);

    engine.detach(&id).await.expect("detach");
}
