//! Reattach topology contract.

mod common;
#[path = "common/recovery.rs"]
mod recovery;

use bifrost_sync::{CheckpointStore, Error, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::Control as _;
use bifrost_types::{
    Account, AccountError, AccountFactory, AccountFuture, AccountId, CursorScope, PushCapability,
    SubscriptionHandle,
};
use common::{caps, unsupported};
use recovery::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[tokio::test(start_paused = true)]
async fn reattach_refreshes_topology_subscriptions_and_lifecycle_handle() {
    let account_id = AccountId("full-reattach".to_owned());
    let old_scope = CursorScope::Account;
    let new_scope = CursorScope::Type(bifrost_types::ObjectType::Email);
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let closed_generations = Arc::new(Mutex::new(Vec::new()));
    let subscribed = Arc::new(Mutex::new(Vec::new()));
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let lifecycle_calls = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![old_scope.clone()],
            vec![new_scope.clone()],
        ])),
        established: Arc::clone(&established),
        closed: Arc::clone(&closed),
        closed_generations: Arc::clone(&closed_generations),
        subscribed: Arc::clone(&subscribed),
        unsubscribed: Arc::clone(&unsubscribed),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::clone(&lifecycle_calls),
        opens: AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = Arc::clone(&factory) as Arc<dyn AccountFactory>;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("initial attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&old_scope))
        .await
        .expect("initial push subscription");
    engine.reattach(&account_id).await.expect("full reattach");

    assert_eq!(factory.opens.load(Ordering::SeqCst), 2);
    assert_eq!(
        engine
            .account_capabilities(&account_id)
            .expect("capabilities")
            .push,
        PushCapability::InProcess,
        "capability snapshot must come from the replacement handle"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &old_scope)
            .await
            .expect("old cursor lookup")
            .is_none(),
        "vanished scopes must be removed durably"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &new_scope)
            .await
            .expect("new cursor lookup")
            .is_some(),
        "newly discovered scopes must be established"
    );
    assert_eq!(
        *subscribed.lock().expect("subscribed lock"),
        vec![(0, vec![old_scope.clone()])],
        "a vanished requested scope must not widen a replacement subscription"
    );
    assert_eq!(
        *unsubscribed.lock().expect("unsubscribed lock"),
        vec![(0, SubscriptionHandle("generation-0".into()))],
        "old subscription must be removed through the old handle"
    );
    assert_eq!(
        *closed_generations.lock().expect("closed generations lock"),
        vec![0],
        "successful reopen closes exactly the old handle"
    );

    // This double's lifecycle stream is empty, so its failed-open count
    // crosses replacement. Allow the earned delay before checking the new
    // handle; healthy streams are separately pinned to reopen immediately.
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    for _ in 0..200 {
        if lifecycle_calls
            .lock()
            .expect("lifecycle calls lock")
            .contains(&1)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        lifecycle_calls
            .lock()
            .expect("lifecycle calls lock")
            .contains(&1),
        "lifecycle task must resubscribe to the replacement handle"
    );

    engine.detach(&account_id).await.expect("detach");
    assert_eq!(
        *closed_generations.lock().expect("closed generations lock"),
        vec![0, 1]
    );
}

#[tokio::test]
async fn reattach_waits_for_resume_without_opening_during_pause() {
    let account_id = AccountId("paused-reopen".to_owned());
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let control = engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach");
    control
        .pause()
        .await
        .expect("idle account pauses immediately");

    let mut reopen = Box::pin(engine.reattach(&account_id));
    tokio::select! {
        result = &mut reopen => panic!("paused reopen completed early: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    assert_eq!(
        established.lock().expect("established lock").len(),
        1,
        "reopen must not open or establish a replacement while paused"
    );

    // The queued reopen must not retain the subscription-serialization
    // lock while it waits for Run. This is the cleanup ordering consumers
    // use before detach; if the lock is held, this future cannot complete
    // until the resume below and the test deterministically takes the yield
    // arm instead.
    let mut unsubscribe = Box::pin(engine.unsubscribe_push(&account_id));
    tokio::select! {
        biased;
        result = &mut unsubscribe => result.expect("paused reopen leaves push teardown available"),
        () = tokio::task::yield_now() => panic!("push teardown blocked behind a paused reopen"),
    }

    control.resume();
    reopen.await.expect("queued reopen completes after resume");
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "resume releases the queued reattach and swaps the live handle"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Factory that parks inside `open` until the test releases it, so a test can
/// observe the window between "the engine decided to reopen" and "a
/// replacement connection exists". The first open (attach) is never gated.
struct GatingFactory {
    scopes: Vec<CursorScope>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
    opens: AtomicUsize,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl AccountFactory for GatingFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let generation = self.opens.fetch_add(1, Ordering::SeqCst);
        let scopes = self.scopes.clone();
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            if generation > 0 {
                entered.notify_one();
                release.notified().await;
            }
            let account: Arc<dyn Account> = Arc::new(
                HealAccount {
                    caps: caps(),
                    scopes,
                    established,
                    closed,
                    generation,
                    closed_generations: Arc::new(Mutex::new(Vec::new())),
                    subscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                    lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
                    lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
                    degraded_contacts: false,
                    close_gate: no_close_gate(),
                    bad_boundary_batches: None,
                    live_checkpoint_batch: false,
                }
                .into_stub(),
            );
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

/// `pause` promises the consumer a quiescent account: no engine-initiated
/// protocol work in flight. Registering the reopen's activity only after
/// `factory.open()` returned left a window where a pause could report
/// quiescence while a replacement connection was being established in the
/// background - and where the replacement, on losing that race, was dropped
/// without `close()`.
#[tokio::test]
async fn pause_cannot_report_quiescence_while_a_replacement_is_mid_open() {
    let account_id = AccountId("mid-open-pause".to_owned());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let closed = Arc::new(AtomicUsize::new(0));
    let factory: Arc<dyn AccountFactory> = Arc::new(GatingFactory {
        scopes: vec![CursorScope::Account],
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::clone(&closed),
        opens: AtomicUsize::new(0),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let engine = Arc::new(
        SyncEngine::builder()
            .build()
            .expect("default engine config is valid"),
    );
    let control = engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach");

    let reopen_engine = Arc::clone(&engine);
    let reopen_id = account_id.clone();
    let reopen = tokio::spawn(async move { reopen_engine.reattach(&reopen_id).await });
    entered.notified().await;

    let mut pause = Box::pin(control.pause());
    for _ in 0..16 {
        tokio::select! {
            biased;
            result = &mut pause => {
                panic!("pause reported quiescence mid-open: {result:?}");
            }
            () = tokio::task::yield_now() => {}
        }
    }

    release.notify_one();
    reopen
        .await
        .expect("reopen task joins")
        .expect("reopen completes");
    pause.await.expect("pause completes once the swap is done");
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "the replacement swap must close exactly the old handle"
    );

    control.resume();
    engine.detach(&account_id).await.expect("detach");
}

/// Factory whose successive opens answer a scripted
/// `OpenedAccount::skipped_scopes` lane, so the engine-side plumbing of
/// the lane (store on attach, replace on reopen, expose via
/// `open_skipped_scopes`) is pinned without any protocol crate.
struct SkippingFactory {
    skips: Mutex<VecDeque<Vec<bifrost_types::SkippedScope>>>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
}

impl AccountFactory for SkippingFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let skipped_scopes = self
            .skips
            .lock()
            .expect("skips lock")
            .pop_front()
            .expect("one skip set per expected open");
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        Box::pin(async move {
            let account: Arc<dyn Account> = Arc::new(
                HealAccount {
                    caps: caps(),
                    scopes: vec![CursorScope::Account],
                    established,
                    closed,
                    generation: 0,
                    closed_generations: Arc::new(Mutex::new(Vec::new())),
                    subscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                    lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
                    lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
                    degraded_contacts: false,
                    close_gate: no_close_gate(),
                    bad_boundary_batches: None,
                    live_checkpoint_batch: false,
                }
                .into_stub(),
            );
            Ok(bifrost_types::OpenedAccount {
                account,
                skipped_scopes,
            })
        })
    }
}

#[tokio::test]
async fn open_skips_surface_on_attach_and_reattach_replaces_them() {
    let account_id = AccountId("open-skips".to_owned());
    let skip = bifrost_types::SkippedScope {
        scope: bifrost_types::ErrorScope::Mailbox {
            id: "shared-acct".into(),
        },
        error: unsupported(bifrost_types::AccountOperation::Discover),
    };
    let factory: Arc<dyn AccountFactory> = Arc::new(SkippingFactory {
        // First open (attach) reports one degraded scope; the second
        // (reopen) reports none - the outage healed.
        skips: Mutex::new(VecDeque::from([vec![skip.clone()], Vec::new()])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");

    assert!(
        matches!(
            engine.open_skipped_scopes(&account_id),
            Err(Error::AccountNotAttached(_))
        ),
        "an unattached account has no skip lane"
    );

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds despite the skipped scope");
    let skips = engine
        .open_skipped_scopes(&account_id)
        .expect("attached account exposes its skip lane");
    assert_eq!(skips.len(), 1, "attach stores the open-time skip");
    assert!(
        matches!(&skips[0].scope, bifrost_types::ErrorScope::Mailbox { id } if id.0 == "shared-acct"),
        "the lane preserves WHICH scope was skipped: {:?}",
        skips[0].scope
    );
    assert_eq!(
        skips[0].error.message_key(),
        skip.error.message_key(),
        "the lane preserves the classified error"
    );

    engine.reattach(&account_id).await.expect("reopen succeeds");
    assert!(
        engine
            .open_skipped_scopes(&account_id)
            .expect("attached account exposes its skip lane")
            .is_empty(),
        "a reopen whose open reports no skips replaces the stale lane"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}
