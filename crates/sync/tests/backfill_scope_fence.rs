//! The scope fence, read at the seam a WALK crosses rather than at the seam a
//! PAGE crosses.
//!
//! `run_partition` re-reads the fence around its capacity park, which catches a
//! reset that closes while a page is in the producer's hand. It cannot catch a
//! reset that closes while the producer is BETWEEN partitions, because the
//! reading it would compare against is taken after the reset - the new
//! incarnation's fence compared with itself. The walk's later partitions then
//! publish above the fence, and a consumer acknowledges them in good faith,
//! rewriting the very rows a `delete_backfill: true` reset had just removed.
//!
//! Staging that needs a park point with NO page in hand and NO pre-reset fence
//! reading, and the account pause is exactly one: `run_partition` holds the
//! control's activity guard for its whole pass, so a `pause()` that returns has
//! proved the producer is parked at the top of the next partition rather than
//! mid-page. The trigger is a provider folder DELETION, because it purges the
//! scope's durable rows in full and drops the scope from the registry - so any
//! row or page appearing afterwards can only have come from the walk that
//! should have stopped.
//!
//! Current-thread runtime with `start_paused`, like the rest of the lane tests:
//! no wall-clock sleeps, and "nothing more was published" is an observation
//! about an idle runtime rather than a wall-clock guess.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::Control;
use bifrost_sync::{
    BackfillConfig, CheckpointStore, EngineConfig, InMemoryCheckpointStore, MultiplexerConfig,
    MultiplexerEvent, SyncEngine,
};
use bifrost_types::{
    AccountFactory, AccountId, Checkpoint, CoverageDomain, CursorScope, Fingerprint, FolderId,
    InventoryBatch, InventoryCompletion, InventoryCoverageReport, InventoryEntry, InventoryEvent,
    InventoryPartition, InventoryPartitioning, MembershipScope, ObjectId, PageBoundary,
    PartitionEnd, ScopeLifecycle, ScopeLifecycleEvent, ServerVersion,
};

const PAGE: u32 = 2;

fn scope() -> CursorScope {
    CursorScope::Folder(FolderId("inbox".to_owned()))
}

fn membership() -> MembershipScope {
    MembershipScope::Folder(FolderId("inbox".to_owned()))
}

fn entry(id: &str) -> InventoryEntry {
    InventoryEntry {
        id: ObjectId(id.into()),
        memberships: Vec::new(),
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: ServerVersion::Unavailable,
            size: None,
            flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
        },
        thread_id: None,
        message_id: None,
        references: Vec::new(),
        in_reply_to: None,
    }
}

/// An open-ended walk that never runs dry, so the producer always has a next
/// partition to publish: with the fence read per page, the reset is followed by
/// another page for certain rather than by luck.
///
/// The first window is deliberately fat and the rest thin. A uniform walk makes
/// "how far did it get" a coincidence of scheduling; an asymmetric head makes it
/// a measurement, and makes a page arriving after the reset unmistakable.
fn endless_stub(
    lifecycle: tokio::sync::mpsc::Receiver<ScopeLifecycleEvent>,
) -> common::StubAccount {
    let mut stub = common::StubAccount::new(vec![scope()]);
    stub.memberships = vec![membership()];
    stub.lifecycle_rx = std::sync::Mutex::new(Some(lifecycle));
    stub.partitioning = InventoryPartitioning::PageCount {
        total: None,
        page_size: Some(PAGE),
    };
    stub.partition_hook = Some(Arc::new(move |scope, partition| {
        let from = match partition {
            InventoryPartition::Page(window) => window.from(),
            other => panic!("the PageCount plan yields page partitions only: {other:?}"),
        };
        let width = if from == 0 { 8 } else { PAGE };
        let items: Vec<InventoryEntry> =
            (0..width).map(|n| entry(&format!("f{from}-{n}"))).collect();
        vec![
            InventoryEvent::Batch(
                InventoryBatch::try_new(
                    items,
                    PageBoundary::Final,
                    Duration::ZERO,
                    0,
                    None,
                    InventoryCoverageReport::complete(CoverageDomain::full(scope.clone())),
                )
                .expect("a Final page with no checkpoint is boundary-valid"),
            ),
            InventoryEvent::Done(InventoryCompletion::complete(
                CoverageDomain::full(scope.clone()),
                None,
                PartitionEnd::MoreBeyond,
            )),
        ]
    }));
    stub
}

fn backfill_checkpoint(event: &MultiplexerEvent) -> Option<&bifrost_types::BackfillCheckpoint> {
    match &event.checkpoint {
        Some(Checkpoint::Backfill(checkpoint)) => Some(checkpoint),
        _ => None,
    }
}

/// A page of the walk that must have stopped, published after a folder deletion
/// purged the scope, is the defect. It reaches the consumer, the consumer
/// acknowledges it, and the durable rows the reset deleted come back.
#[tokio::test(start_paused = true)]
async fn a_reset_between_partitions_stops_the_walk_it_invalidated() {
    let account_id = AccountId("fence-between-partitions".to_owned());
    let (lifecycle_tx, lifecycle_rx) = tokio::sync::mpsc::channel(4);
    let stub = Arc::new(endless_stub(lifecycle_rx));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let config = EngineConfig {
        multiplexer: MultiplexerConfig {
            changes_capacity: 64,
            // Long polls: this test is about the backfill producer, and a live
            // drive on every tick only adds traffic to read past.
            poll_initial: Duration::from_secs(3600),
            poll_min: Duration::from_secs(3600),
            poll_max: Duration::from_secs(3600),
            ..MultiplexerConfig::default()
        },
        backfill: BackfillConfig {
            // Wide enough that the producer never parks on the BOUND: the park
            // this test needs is the one between partitions, and a producer
            // holding a page at the bound is the case the per-page check
            // already refuses.
            lane_capacity: 16,
            ..BackfillConfig::default()
        },
        ..EngineConfig::default()
    };

    let engine = Arc::new(
        SyncEngine::builder()
            .config(config)
            .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
            .build()
            .expect("engine config is valid"),
    );
    let factory: Arc<dyn AccountFactory> = Arc::new(common::StubFactory::queue(vec![stub]));
    let control = engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");

    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    // A consumer that reads and acknowledges everything, forwarding each page it
    // saw. Acknowledging is not incidental: `pause` waits for every outstanding
    // publication to be answered, so a consumer that only reads would leave the
    // pause hanging and stage nothing.
    let (page_tx, mut page_rx) = tokio::sync::mpsc::unbounded_channel();
    let reader_engine = Arc::clone(&engine);
    let reader_account = account_id.clone();
    let reader = tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            if backfill_checkpoint(&event).is_none() {
                continue;
            }
            let publication = event
                .ack_token()
                .cloned()
                .expect("a checkpointed page carries its publication");
            assert!(event.checkpoint.is_some(), "a checkpointed page");
            let _ = page_tx.send(event.scope.cursor().expect("cursor event").clone());
            let _ = reader_engine.ack(&reader_account, publication).await;
        }
    });

    // Let the walk get going, then pause. `run_partition` holds the activity
    // guard for a whole pass, so a pause that returns has proved the producer is
    // between partitions: no page in hand, and no fence reading taken.
    tokio::time::timeout(Duration::from_secs(10), page_rx.recv())
        .await
        .expect("the walk must publish before it is paused")
        .expect("the reader stays alive");
    tokio::time::timeout(Duration::from_secs(30), control.pause())
        .await
        .expect("the pause must reach a boundary")
        .expect("an acknowledged account pauses");

    // The durable footprint the reset is about to remove.
    assert!(
        store
            .get_backfill(&account_id, &scope())
            .await
            .expect("store read")
            .is_some(),
        "the acknowledged pages must have left a durable row, or the deletion \
         below removes nothing and the test observes nothing"
    );
    while page_rx.try_recv().is_ok() {}

    // The provider deletes the folder. The scope leaves the registry and its
    // durable rows - backfill included, completion marker included - are purged.
    lifecycle_tx
        .send(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Deleted(
            membership(),
        )))
        .await
        .expect("the lifecycle stream accepts the deletion");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if store
                .get_backfill(&account_id, &scope())
                .await
                .expect("store read")
                .is_none()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the deletion must purge the scope's backfill rows");

    // Now let the old walk run again. It has a partition left, and nothing about
    // its own state has changed.
    control.resume();

    assert!(
        tokio::time::timeout(Duration::from_secs(30), page_rx.recv())
            .await
            .is_err(),
        "a walk whose scope was reset between two of its partitions must not \
         publish another page: the scope is gone from the registry, so the page \
         is acknowledged against rows the reset deleted and re-creates them"
    );
    assert!(
        store
            .get_backfill(&account_id, &scope())
            .await
            .expect("store read")
            .is_none(),
        "and no durable row may come back for a deleted folder; a surviving \
         completion marker makes a folder recreated under the same id skip its \
         entire cold-start walk"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
    reader.abort();
}
