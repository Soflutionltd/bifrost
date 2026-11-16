//! Establish subscriber wait contract.

mod common;

use bifrost_sync::{CheckpointStore, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::Checkpoint;
use bifrost_types::{AccountId, CursorScope};
use common::cursor_for;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A vanished first reader must not turn deferred establishment into a bounded
/// recovery campaign. Nobody subscribed means no provider work, even after a
/// long gap; a later reader can still receive the establishing checkpoint.
#[tokio::test(start_paused = true)]
async fn deferred_inventory_waits_again_when_its_first_subscriber_leaves() {
    let scope = CursorScope::Type(bifrost_types::ObjectType::Email);
    let first = Arc::new(Mutex::new(None::<bifrost_sync::delivery::ChangesReceiver>));
    let departed = Arc::clone(&first);
    let mut account = common::StubAccount::new(vec![scope.clone()]);
    account.establishment = |_| bifrost_types::CursorEstablishment::EstablishViaInventory;
    account.inventory_hook = Some(Arc::new(move |scope| {
        // Drop the only receiver after admission but before publication.
        drop(departed.lock().expect("first receiver").take());
        vec![bifrost_types::InventoryEvent::Done(
            bifrost_types::InventoryCompletion::complete(
                bifrost_types::CoverageDomain::full(scope.clone()),
                Some(Checkpoint::Change(cursor_for(scope, b"late-reader"))),
                bifrost_types::PartitionEnd::Exhausted,
            ),
        )]
    }));
    let calls = Arc::clone(&account.inventory_calls);
    let id = AccountId("deferred-late-reader".into());
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("engine");
    engine
        .attach(
            id.clone(),
            Arc::new(common::StubFactory::queue(vec![Arc::new(account)])),
        )
        .await
        .expect("attach");
    tokio::time::advance(Duration::from_secs(3600)).await;
    tokio::task::yield_now().await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    *first.lock().expect("first receiver") =
        Some(engine.account_changes_stream(&id).expect("subscribe"));
    tokio::time::timeout(Duration::from_secs(5), async {
        while calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first inventory attempted");
    tokio::time::advance(Duration::from_secs(3600)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "no inventory churn without a reader"
    );
    let mut receiver = engine.account_changes_stream(&id).expect("late subscribe");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = receiver.recv().await.expect("publication");
            if event.checkpoint.is_some() {
                engine
                    .ack(&id, event.ack_token().cloned().expect("ack token"))
                    .await
                    .expect("ack");
                break;
            }
        }
    })
    .await
    .expect("late reader establishes the scope");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(
        store
            .get_change_cursor(&id, &scope)
            .await
            .expect("stored cursor"),
        Some(cursor_for(&scope, b"late-reader")),
    );
    engine.detach(&id).await.expect("detach");
}
