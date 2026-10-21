//! Inventory tombstones share their page's consumer transaction and ack.
mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::{CheckpointStore, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{
    AccountId, Change, Checkpoint, CoverageDomain, CursorEstablishment, CursorScope, FolderId,
    InventoryBatch, InventoryCompletion, InventoryCoverageReport, InventoryEvent, MembershipScope,
    ObjectId, ObjectType, PageBoundary, PartitionEnd, ScopeChangeKind, SyncEvent,
};

use common::{StubAccount, StubFactory, cursor_for};

/// Both front ends must deliver a removal-only page with its checkpoint.
/// Removing the tombstone projection from either path leaves the seeded row
/// present, even though the checkpoint still advances.
#[tokio::test(start_paused = true)]
async fn fusion_page_removals_apply_before_the_covering_checkpoint_is_acked() {
    assert_inventory_removal(true).await;
}

#[tokio::test(start_paused = true)]
async fn backfill_page_removals_apply_before_the_covering_checkpoint_is_acked() {
    assert_inventory_removal(false).await;
}

async fn assert_inventory_removal(fusion: bool) {
    let scope = CursorScope::FolderType {
        folder: FolderId("inbox".into()),
        ty: ObjectType::Email,
    };
    let removed = ObjectId("gone".into());
    let mut stub = StubAccount::new(vec![scope.clone()]);
    let page = Arc::new(move |scope: &CursorScope| {
        let checkpoint = Checkpoint::Change(cursor_for(scope, b"page-covering-removal"));
        let mut page = InventoryBatch::try_new(
            Vec::new(),
            PageBoundary::Final,
            Duration::ZERO,
            0,
            Some(checkpoint.clone()),
            InventoryCoverageReport::complete(CoverageDomain::full(scope.clone())),
        )
        .expect("valid page");
        page.removed.push(ObjectId("gone".into()));
        vec![
            InventoryEvent::Batch(page),
            InventoryEvent::Done(InventoryCompletion::complete(
                CoverageDomain::full(scope.clone()),
                Some(checkpoint),
                PartitionEnd::Exhausted,
            )),
        ]
    });
    if fusion {
        stub.establishment = |_| CursorEstablishment::EstablishViaInventory;
        stub.inventory_hook = Some(page);
    } else {
        stub.partition_hook = Some(Arc::new(move |scope, _| page(scope)));
    }
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("engine");
    let account = AccountId(format!("inventory-removal-{fusion}"));
    engine
        .attach(
            account.clone(),
            Arc::new(StubFactory::queue(vec![Arc::new(stub)])),
        )
        .await
        .expect("attach");
    let mut rx = engine.account_changes_stream(&account).expect("receiver");
    // The consumer owns this folder's membership rows. Its transaction
    // applies the entire page and
    // saves its checkpoint together; the engine must not persist ahead.
    let mut rows = HashSet::from([removed.clone(), ObjectId("sibling".into())]);
    let event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = rx.recv().await.expect("page delivery");
            if matches!(event.event.as_ref(), SyncEvent::Batch(_)) {
                break event;
            }
        }
    })
    .await
    .expect("inventory page");
    let SyncEvent::Batch(batch) = event.event.as_ref() else {
        panic!("expected batch")
    };
    let checkpoint = event.checkpoint.clone().expect("covering checkpoint");
    assert_eq!(batch.checkpoint.as_ref(), Some(&checkpoint));
    assert_eq!(
        batch.items.len(),
        1,
        "a removal-only page still has an effect"
    );
    let mut transaction = rows.clone();
    for change in &batch.items {
        let Change::ScopeChange(change) = change else {
            panic!("folder removal must not destroy the object globally")
        };
        assert_eq!(change.id, removed);
        assert_eq!(
            change.membership,
            MembershipScope::Folder(FolderId("inbox".into()))
        );
        assert_eq!(change.kind, ScopeChangeKind::Removed);
        transaction.remove(&change.id);
    }
    if fusion {
        assert!(
            store
                .get_change_cursor(&account, &scope)
                .await
                .expect("read")
                .is_none()
        );
    } else {
        assert!(
            store
                .get_backfill(&account, &scope)
                .await
                .expect("read")
                .is_none()
        );
        let Checkpoint::Backfill(page) = &checkpoint else {
            panic!("backfill page checkpoint")
        };
        assert_eq!(
            page.progress.items_done, 1,
            "removals advance page progress"
        );
    }
    // Commit the consumer transaction, then acknowledge exactly its page.
    rows = transaction;
    let committed = checkpoint.clone();
    assert!(!rows.contains(&removed));
    assert!(rows.contains(&ObjectId("sibling".into())));
    engine
        .ack_checkpoint(
            &account,
            scope.clone(),
            committed,
            event.publication.clone(),
        )
        .await
        .expect("ack");
    match checkpoint {
        Checkpoint::Change(cursor) => {
            assert_eq!(
                store
                    .get_change_cursor(&account, &scope)
                    .await
                    .expect("read"),
                Some(cursor)
            );
        }
        Checkpoint::Backfill(page) => {
            assert_eq!(
                store.get_backfill(&account, &scope).await.expect("read"),
                Some(page)
            );
        }
        _ => panic!("unexpected checkpoint"),
    }
    engine.detach(&account).await.expect("detach");
    engine.shutdown().await.expect("shutdown");
}
