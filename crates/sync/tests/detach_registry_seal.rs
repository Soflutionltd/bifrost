//! Every write to the push registry is sealed against detach.
//!
//! Detach cancels the slot's shutdown token BEFORE it takes and discards the
//! account's registry entry, and it does not wait for consumer-driven work. A
//! registry write that lands after that take sits under an id a later `attach`
//! of the same `AccountId` inherits, and is handed to the provider as if live.
//! The orphan restores were sealed first (`detach_orphan_race`); these cover the
//! two writes that were not: a consumer's `subscribe_push` registering its
//! handle, and a reattach commit installing the replacement's handles.
//!
//! Sealing the write is half of each fix. The subscription the refused write
//! names is LIVE on the provider, so each case also asserts it was torn down
//! rather than dropped on the floor.
//!
//! Both stage detach with the park gates: the write is held at the point just
//! before it happens, detach runs to completion, and only then is the gate
//! released.

mod common;

use std::sync::Arc;

use bifrost_sync::{Error, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, CursorScope, FolderId, PushCapability};

use common::{PushLog, StubAccount, StubFactory};

fn inbox() -> Vec<CursorScope> {
    vec![CursorScope::Folder(FolderId("inbox".into()))]
}

fn pushing(label: &str, log: &Arc<PushLog>) -> Arc<StubAccount> {
    let mut caps = common::caps();
    caps.push = PushCapability::Webhook;
    Arc::new(StubAccount {
        caps,
        push_lane_specs: StubAccount::registration_lanes(),
        push_label: label.to_owned(),
        push_log: Arc::clone(log),
        ..StubAccount::new(inbox())
    })
}

/// Yield until `done` holds, bounded by iterations so a state that never arrives
/// fails the test instead of hanging it.
async fn settle(mut done: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held");
}

fn unsubscribed_on(log: &PushLog, label: &str) -> usize {
    log.unsubscribed()
        .iter()
        .filter(|(account, _)| account == label)
        .count()
}

/// A `subscribe_push` whose provider call is in flight when detach runs.
///
/// The call registers its handle after detach's take, so the write must be
/// refused. The subscription it made is live on the provider and unreachable
/// (the consumer is told the call failed), so it must be deleted, and the error
/// must say the account is gone.
///
/// Ablation, registry: writing through an unchecked `entry().push` leaves the
/// handle under the id, and the second incarnation's `unsubscribe_push` hands it
/// to `later`. Ablation, teardown: dropping the refused handle without deleting
/// it leaves `first` with no unsubscribe.
///
/// `subscribe_push` holds the reopen lock across its provider call, and detach
/// now waits for that lock before its registry take, so the interleaving is
/// reachable only through that wait's timeout. Paused time makes the timeout
/// immediate; the seal this pins is what holds on that fallback path.
#[tokio::test(start_paused = true)]
async fn a_subscription_registered_after_detachs_take_is_torn_down_not_inherited() {
    let id = AccountId("detach-record-race".into());
    let log = Arc::new(PushLog::default());
    let first = pushing("first", &log);
    let gate = Arc::new(tokio::sync::Notify::new());
    *first.push_subscribe_park.lock().expect("park lock") = Some((0, Arc::clone(&gate)));
    let later = pushing("later", &log);

    let engine = Arc::new(SyncEngine::builder().build().expect("engine"));
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&first)]));
    engine.attach(id.clone(), factory).await.expect("attach");

    let subscribing = {
        let engine = Arc::clone(&engine);
        let id = id.clone();
        tokio::spawn(async move { engine.subscribe_push(&id, &inbox()).await })
    };
    // The provider call is logged on entry, then parks on the gate.
    settle(|| !log.subscribed().is_empty()).await;

    engine.detach(&id).await.expect("detach");

    gate.notify_one();
    let outcome = subscribing.await.expect("subscribe task");
    assert!(
        matches!(outcome, Err(Error::AccountNotAttached(_))),
        "a subscription that could not be registered is an error, not a success the \
         consumer will never be able to tear down: {outcome:?}"
    );
    assert_eq!(
        unsubscribed_on(&log, "first"),
        1,
        "the refused subscription is live on the provider and must be deleted"
    );

    let second: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&later)]));
    engine.attach(id.clone(), second).await.expect("re-attach");
    engine
        .unsubscribe_push(&id)
        .await
        .expect("nothing to tear down");
    assert_eq!(
        unsubscribed_on(&log, "later"),
        0,
        "the new incarnation inherited a handle minted by the detached one: {:?}",
        log.unsubscribed()
    );

    engine.detach(&id).await.expect("detach");
}

/// A reattach commit whose registry install lands after detach's take.
///
/// The reattach is held inside the replacement's own `push_subscribe`, so detach
/// runs between the reattach's last shutdown check and its commit. The swap is
/// past the point an abort can occur, so the reattach reports success; the
/// replacement's handle is live on the provider and no registry entry may hold
/// it, so it must be deleted, and a later attach must inherit nothing.
///
/// Ablation, registry: installing with the unchecked insert leaves the
/// replacement's handle under the id, and the second incarnation's
/// `unsubscribe_push` hands it to `later`. Ablation, teardown: dropping the
/// refused records without deleting them leaves `replacement` with no
/// unsubscribe.
///
/// Detach now waits for the reopen lock this parked reattach holds, so the
/// interleaving is reachable only through that wait's timeout. Paused time
/// makes the timeout immediate; the seal this pins is what holds on that
/// fallback path.
#[tokio::test(start_paused = true)]
async fn a_reattach_commit_after_detachs_take_tears_down_what_it_could_not_register() {
    let id = AccountId("detach-replace-race".into());
    let log = Arc::new(PushLog::default());
    let old = pushing("old", &log);
    let replacement = pushing("replacement", &log);
    let gate = Arc::new(tokio::sync::Notify::new());
    *replacement.push_subscribe_park.lock().expect("park lock") = Some((0, Arc::clone(&gate)));
    let later = pushing("later", &log);

    let engine = Arc::new(SyncEngine::builder().build().expect("engine"));
    let first: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![
        Arc::clone(&old),
        Arc::clone(&replacement),
    ]));
    engine.attach(id.clone(), first).await.expect("attach");
    engine
        .subscribe_push(&id, &inbox())
        .await
        .expect("consumer subscribes");

    let reattach = {
        let engine = Arc::clone(&engine);
        let id = id.clone();
        tokio::spawn(async move { engine.reattach(&id).await })
    };
    // The replacement's recreation of the consumer's subscription is logged on
    // entry, then parks.
    settle(|| {
        log.subscribed()
            .iter()
            .any(|(label, _)| label == "replacement")
    })
    .await;

    engine.detach(&id).await.expect("detach");

    gate.notify_one();
    let outcome = reattach.await.expect("reattach task");
    assert!(
        outcome.is_ok(),
        "the swap committed before the registry refused the install, so it is not a failed \
         reattach: {outcome:?}"
    );
    assert_eq!(
        unsubscribed_on(&log, "replacement"),
        1,
        "the replacement's subscription is live on the provider and must be deleted"
    );

    let second: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&later)]));
    engine.attach(id.clone(), second).await.expect("re-attach");
    engine
        .unsubscribe_push(&id)
        .await
        .expect("nothing to tear down");
    assert_eq!(
        unsubscribed_on(&log, "later"),
        0,
        "the new incarnation inherited a handle minted by the detached one: {:?}",
        log.unsubscribed()
    );

    engine.detach(&id).await.expect("detach");
}
