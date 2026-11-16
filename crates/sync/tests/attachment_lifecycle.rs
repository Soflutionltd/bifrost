//! Attachment lifecycle contract.

mod common;
#[path = "common/recovery.rs"]
mod recovery;

use bifrost_sync::{CheckpointStore, Error, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, CursorScope};
use recovery::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// `detach` removes the slot up front but does its registry cleanup at
/// the very tail - after awaiting workers (up to `detach_timeout`) and
/// `Account::close()`. An `attach` landing inside that window used to
/// see no slot and no in-flight entry, so it succeeded and installed
/// fresh registrations; the still-running detach then unregistered the
/// NEW incarnation from the invalidation sink, the budget gate, the
/// backfill registry, the throttle memberships, and the bandwidth
/// meter. The consumer got an account that reported itself attached and
/// silently dropped every out-of-process push, with nothing logged.
///
/// Rejecting the racing attach is the fix: the id is genuinely still
/// attached and draining, and a caller that gets an error can retry.
#[tokio::test]
async fn attach_cannot_land_inside_an_in_flight_detach() {
    let account_id = AccountId("detach-reattach-race".to_owned());
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let engine = Arc::new(
        SyncEngine::builder()
            .build()
            .expect("default engine config is valid"),
    );

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let gated: Arc<dyn AccountFactory> = Arc::new(HealFactory::with_close_gate(
        vec![CursorScope::Account],
        Arc::clone(&established),
        Arc::clone(&closed),
        CloseGate {
            entered: entered_tx,
            release: release_rx,
        },
    ));
    engine
        .attach(account_id.clone(), Arc::clone(&gated))
        .await
        .expect("attach succeeds");

    // Park the detach inside `Account::close()`, which sits after the
    // slot removal and before every registry cleanup.
    let detaching = {
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move { engine.detach(&account_id).await })
    };
    entered_rx.await.expect("detach reaches Account::close");
    assert!(
        !engine.attached_account_ids().contains(&account_id),
        "the slot is already gone; only the lifecycle guard stands between \
         a racing attach and the teardown tail"
    );

    let racing: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));
    let raced = engine.attach(account_id.clone(), racing).await;
    assert!(
        matches!(raced, Err(Error::AccountAlreadyAttached(ref id)) if id == &account_id),
        "an attach inside the teardown window must be refused, not \
         installed and then stripped of its registrations"
    );

    release_tx.send(()).expect("release the gated close");
    detaching
        .await
        .expect("detach task")
        .expect("detach succeeds");

    // The guard releases with the teardown, so the id is reusable.
    let fresh: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        established,
        Arc::clone(&closed),
    ));
    engine
        .attach(account_id.clone(), fresh)
        .await
        .expect("re-attach after the detach completes");
    engine.detach(&account_id).await.expect("detach succeeds");
}

#[tokio::test]
async fn failed_attach_closes_the_opened_account() {
    let account_id = AccountId("attach-failure-close".to_owned());
    let store = Arc::new(PoisonedStore::with_read_failure());
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        established,
        Arc::clone(&closed),
    ));

    let result = engine.attach(account_id.clone(), factory).await;
    assert!(matches!(result, Err(Error::CheckpointStore(_))));
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "every post-open attach failure must close the live handle"
    );
    assert!(
        !engine.attached_account_ids().contains(&account_id),
        "failed attach must not leave a slot behind"
    );
}
