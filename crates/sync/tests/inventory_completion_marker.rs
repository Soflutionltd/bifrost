//! A scope established by a deferred inventory walk has been inventoried in
//! full. The walk must leave the durable backfill-completion marker behind,
//! or the next attach (where the scope is no longer fusion-owned) walks and
//! re-hydrates the whole scope a second time.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::backfill::BackfillState;
use bifrost_sync::{CheckpointStore, EngineConfig, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, CursorScope, ObjectType};

#[tokio::test]
async fn an_inventory_established_scope_is_recorded_as_backfilled() {
    let account_id = AccountId("inventory-completion-marker".to_owned());
    let scope = CursorScope::Type(ObjectType::Email);

    let mut stub = common::StubAccount::new(vec![scope.clone()]);
    stub.establishment = |_| bifrost_types::CursorEstablishment::EstablishViaInventory;
    stub.inventory_hook = Some(common::establishing_inventory());
    let stub = Arc::new(stub);

    let mut config = EngineConfig::default();
    config.multiplexer.poll_initial = Duration::from_secs(60 * 60);
    config.multiplexer.poll_min = Duration::from_secs(60 * 60);
    config.multiplexer.poll_max = Duration::from_secs(60 * 60);

    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = Arc::new(
        SyncEngine::builder()
            .config(config)
            .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
            .build()
            .expect("engine config is valid"),
    );
    let factory: Arc<dyn AccountFactory> = Arc::new(common::StubFactory::queue(vec![stub]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    assert!(
        engine.backfill_registry().has_running(&account_id),
        "a scope waiting for its inventory must not read as backfilled"
    );

    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let reader_engine = Arc::clone(&engine);
    let reader_account = account_id.clone();
    let reader = tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            let (Some(checkpoint), Some(publication)) =
                (event.checkpoint.clone(), event.publication.clone())
            else {
                continue;
            };
            let _ = reader_engine
                .ack_checkpoint(
                    &reader_account,
                    event.scope.clone(),
                    checkpoint,
                    Some(publication),
                )
                .await;
        }
    });

    let marker = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(Some(stored)) = store.get_backfill(&account_id, &scope).await
                && bifrost_sync::backfill::partitioner::is_completion_partition(&stored.partition)
            {
                return stored;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the inventory walk must leave a durable completion marker");
    assert_eq!(marker.scope, scope);
    tokio::time::timeout(Duration::from_secs(10), async {
        while engine.backfill_registry().snapshot(&account_id, &scope)
            != Some(BackfillState::Completed)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the established scope must read as backfilled");
    assert!(!engine.backfill_registry().has_running(&account_id));

    reader.abort();
    engine.detach(&account_id).await.expect("detach succeeds");
}
