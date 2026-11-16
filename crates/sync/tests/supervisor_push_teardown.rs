//! Supervisor push teardown contract.

mod common;
#[path = "common/recovery.rs"]
mod recovery;

use bifrost_sync::{CheckpointStore, Error, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, CursorScope, SubscriptionHandle};
use recovery::*;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

/// Detach must drop this incarnation's push registry records.
///
/// The registry is per-engine and keyed by `AccountId`, but the handles in
/// it belong to one connection. `detach` used to forget the sink, the
/// budget, the backfill registry, throttles, and the bandwidth meter while
/// leaving `subscriptions` untouched, so re-attaching the same id inherited
/// the previous incarnation's handles and would hand them to the provider
/// as though they were live.
///
/// Observed through `unsubscribed`: after a detach and a fresh attach, a
/// teardown must find nothing to tear down. Nothing here asserts that the
/// SERVER-side subscription was deleted - it deliberately is not, since
/// that stays the consumer's job via `unsubscribe_push` BEFORE detach.
#[tokio::test]
async fn detach_drops_push_records_so_a_reattach_cannot_reuse_dead_handles() {
    let account_id = AccountId("detach-clears-registry".to_owned());
    let scope = CursorScope::Account;
    let subscribed = Arc::new(Mutex::new(Vec::new()));
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([vec![scope.clone()], vec![scope.clone()]])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::clone(&subscribed),
        unsubscribed: Arc::clone(&unsubscribed),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let engine = SyncEngine::builder()
        .checkpoints(Arc::new(InMemoryCheckpointStore::new()) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = Arc::clone(&factory) as Arc<dyn AccountFactory>;

    engine
        .attach(account_id.clone(), Arc::clone(&factory_trait))
        .await
        .expect("initial attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&scope))
        .await
        .expect("push subscription");
    assert_eq!(
        subscribed.lock().expect("subscribed lock").len(),
        1,
        "the subscription must actually register, or the test proves nothing"
    );

    // Detach WITHOUT unsubscribing - the case the contract warns about.
    engine.detach(&account_id).await.expect("detach");

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("reattach under the same id");
    engine
        .unsubscribe_push(&account_id)
        .await
        .expect("teardown on the fresh incarnation");

    assert!(
        unsubscribed.lock().expect("unsubscribed lock").is_empty(),
        "a reattached account must not inherit the previous incarnation's \
         handles; tearing one down targets a connection that is gone"
    );

    engine.detach(&account_id).await.expect("final detach");
}

/// The opt-in the contract needed: `detach_with_teardown` tears the
/// server-side subscriptions down and then detaches, so a consumer that is
/// done with the account states that once instead of remembering to call
/// `unsubscribe_push` inside a window that closes silently. Plain `detach`
/// is unchanged, which the test above pins. Ablation: with the teardown call
/// removed, `unsubscribed` stays empty.
#[tokio::test]
async fn detach_with_teardown_tears_push_subscriptions_down_before_detaching() {
    let account_id = AccountId("detach-with-teardown".to_owned());
    let scope = CursorScope::Account;
    let subscribed = Arc::new(Mutex::new(Vec::new()));
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([vec![scope.clone()]])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::clone(&subscribed),
        unsubscribed: Arc::clone(&unsubscribed),
        unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let engine = SyncEngine::builder()
        .checkpoints(Arc::new(InMemoryCheckpointStore::new()) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = Arc::clone(&factory) as Arc<dyn AccountFactory>;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&scope))
        .await
        .expect("push subscription");
    assert_eq!(subscribed.lock().expect("subscribed lock").len(), 1);

    engine
        .detach_with_teardown(&account_id)
        .await
        .expect("teardown and detach");

    assert_eq!(
        unsubscribed.lock().expect("unsubscribed lock").len(),
        1,
        "the server-side subscription must be torn down by the detach the consumer asked for"
    );
    assert!(
        matches!(
            engine.unsubscribe_push(&account_id).await,
            Err(bifrost_sync::Error::AccountNotAttached(_))
        ),
        "and the account is detached afterwards"
    );
}

#[tokio::test]
async fn failed_push_unsubscribe_is_reported_and_the_handle_is_retryable() {
    let account_id = AccountId("retry-push-unsubscribe".to_owned());
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([vec![CursorScope::Account]])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::clone(&unsubscribed),
        unsubscribe_failures: Arc::new(AtomicUsize::new(1)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, &[CursorScope::Account])
        .await
        .expect("subscribe");

    assert!(
        matches!(
            engine.unsubscribe_push(&account_id).await,
            Err(Error::Account(_))
        ),
        "a failed account-side teardown must be surfaced to the caller"
    );
    engine
        .unsubscribe_push(&account_id)
        .await
        .expect("retained handle retries successfully");
    assert_eq!(
        *unsubscribed.lock().expect("unsubscribed lock"),
        vec![
            (0, SubscriptionHandle("generation-0".into())),
            (0, SubscriptionHandle("generation-0".into())),
        ],
        "the failed handle remains registered for the next teardown call"
    );

    engine.detach(&account_id).await.expect("detach");
}

/// Correlated teardown failure: the old handle refuses to unsubscribe AND the
/// unwind of the replacement's own subscriptions fails too. `Account::close()`
/// deliberately does not delete server-side subscriptions, so a replacement
/// handle that is merely logged and forgotten here is exactly the orphaned
/// webhook the retained-handle rule exists to prevent.
#[tokio::test]
async fn correlated_teardown_failure_retains_both_sides_for_retry() {
    let account_id = AccountId("correlated-teardown".to_owned());
    let old_scope = CursorScope::Account;
    let new_scope = CursorScope::Type(bifrost_types::ObjectType::Email);
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![old_scope.clone()],
            vec![old_scope.clone(), new_scope.clone()],
        ])),
        established: Arc::new(Mutex::new(Vec::new())),
        closed: Arc::new(AtomicUsize::new(0)),
        closed_generations: Arc::new(Mutex::new(Vec::new())),
        subscribed: Arc::new(Mutex::new(Vec::new())),
        unsubscribed: Arc::clone(&unsubscribed),
        // Both the old-handle teardown and the replacement unwind that
        // follows it fail.
        unsubscribe_failures: Arc::new(AtomicUsize::new(2)),
        lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
        opens: AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = factory;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&old_scope))
        .await
        .expect("subscribe");

    assert!(
        matches!(engine.reattach(&account_id).await, Err(Error::Account(_))),
        "an unconfirmed old-handle teardown must abort the swap"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &new_scope)
            .await
            .expect("new scope lookup")
            .is_none(),
        "a replacement cursor must not become durable when the topology swap aborts"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &old_scope)
            .await
            .expect("old scope lookup")
            .is_some(),
        "an aborted swap must retain the installed topology's durable cursor"
    );
    assert_eq!(
        *unsubscribed.lock().expect("unsubscribed lock"),
        vec![
            (0, SubscriptionHandle("generation-0".into())),
            (1, SubscriptionHandle("generation-1".into())),
        ],
        "both sides are attempted, and both fail"
    );

    // The replacement was closed, but its handle must still be registered:
    // a later teardown call is the only thing that can delete it server-side.
    engine
        .unsubscribe_push(&account_id)
        .await
        .expect("both retained handles tear down on retry");
    assert_eq!(
        *unsubscribed.lock().expect("unsubscribed lock"),
        vec![
            (0, SubscriptionHandle("generation-0".into())),
            (1, SubscriptionHandle("generation-1".into())),
            (0, SubscriptionHandle("generation-0".into())),
            (0, SubscriptionHandle("generation-1".into())),
        ],
        "neither the old nor the replacement handle may be forgotten"
    );

    engine.detach(&account_id).await.expect("detach");
}
