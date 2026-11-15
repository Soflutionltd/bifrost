//! An orphan must not land in the registry after detach has taken it.
//!
//! A push handle whose server-side delete was refused is registered as an orphan
//! so a later reopen or `unsubscribe_push` can retry it. `detach` discards the
//! account's registry entry, and a later `attach` of the same `AccountId` starts
//! from whatever is registered under it, handing it to the provider as if live.
//! Detach cancels the slot's shutdown token BEFORE it takes the entry, but it
//! does not wait for consumer-driven reattaches, so an orphan written by one
//! that is still unwinding lands after the take unless the write itself observes
//! the teardown.
//!
//! The staging uses the park gates to place detach squarely between an aborting
//! reattach's cleanup delete and the write of its refusal.

mod common;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use bifrost_sync::{Error, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, CursorScope, FolderId, PushCapability};

use common::{PushLog, StubAccount, StubFactory};

fn inbox() -> Vec<CursorScope> {
    vec![CursorScope::Folder(FolderId("inbox".into()))]
}

fn pushing(label: &str, log: &Arc<PushLog>, failures: usize) -> Arc<StubAccount> {
    let mut caps = common::caps();
    caps.push = PushCapability::Webhook;
    Arc::new(StubAccount {
        caps,
        push_label: label.to_owned(),
        push_log: Arc::clone(log),
        push_unsubscribe_failures: AtomicUsize::new(failures),
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

/// The ordinary abort's unwind restores a refused replacement handle as an
/// orphan. Detach lands while that delete is parked, so the restore happens
/// after detach's registry take, and must be refused rather than inherited by
/// the next attach of the same id.
///
/// Ablation: restoring through the bare, unchecked write (as the unwind did)
/// leaves the replacement's handle registered under the id, and the second
/// incarnation's `unsubscribe_push` hands it to `later`.
///
/// Detach now waits for the reopen lock this parked reattach holds, so the
/// interleaving is reachable only through that wait's timeout, after which
/// detach proceeds without the lock. Paused time makes the timeout immediate;
/// the seal this pins is what still holds on that fallback path.
#[tokio::test(start_paused = true)]
async fn an_orphan_restored_after_detachs_take_is_not_inherited_by_the_next_attach() {
    let id = AccountId("detach-orphan-race".into());
    let log = Arc::new(PushLog::default());
    // The old side refuses every teardown, so the reattach aborts and unwinds.
    let old = pushing("old", &log, usize::MAX);
    let replacement = pushing("replacement", &log, usize::MAX);
    let cleanup_gate = Arc::new(tokio::sync::Notify::new());
    *replacement.push_unsubscribe_park.lock().expect("park lock") = Some(Arc::clone(&cleanup_gate));
    let later = pushing("later", &log, 0);

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
    // The old teardown was refused; the unwind's delete on the replacement is
    // now parked (it is logged on entry).
    settle(|| log.unsubscribed().len() == 2).await;

    engine.detach(&id).await.expect("detach");

    // The delete is refused only now, after detach took the registry.
    cleanup_gate.notify_one();
    let outcome = reattach.await.expect("reattach task");
    assert!(
        matches!(outcome, Err(Error::Account(_))),
        "the aborted reattach reports its failure: {outcome:?}"
    );

    let second: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&later)]));
    engine.attach(id.clone(), second).await.expect("re-attach");
    engine
        .unsubscribe_push(&id)
        .await
        .expect("nothing to tear down");
    assert!(
        log.unsubscribed().iter().all(|(label, _)| label != "later"),
        "the new incarnation inherited a handle minted by the detached one: {:?}",
        log.unsubscribed()
    );

    engine.detach(&id).await.expect("detach");
}
