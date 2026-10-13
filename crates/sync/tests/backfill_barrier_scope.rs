//! End-to-end pin for A2's final shape: a barrier stops the SCOPE walk
//! through the REAL orchestrator, not just inside `ScopeWalkDriver`.
//!
//! Round 3 unit-tested the driver because no reusable `Account` double
//! existed; this file drives the whole pipeline - attach, cursor
//! establishment, the backfill orchestrator's fixed-plan loop, the partition
//! runner, the broadcast channel - against `common::StubAccount`, and pins
//! the properties that were each separately gotten wrong during the arc:
//!
//! - the partition after a barrier is never even REQUESTED from the account;
//! - the barrier page's items are delivered with its checkpoint STRIPPED;
//! - no completion sentinel is broadcast for the stopped scope;
//! - nothing becomes durable without a consumer ack.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::{CheckpointStore, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{
    AccountErrorBuilder, AccountErrorKind, AccountFactory, AccountId, Cause, Checkpoint,
    CoverageDomain, CursorScope, DiagnosticText, Fingerprint, InventoryBatch, InventoryCompletion,
    InventoryCoverageReport, InventoryEntry, InventoryEvent, InventoryObligation,
    InventoryPartition, InventoryPartitioning, ObjectId, ObligationKey, PageBoundary, PartitionEnd,
    RegionRecovery, RequestCause, RequestErrorKind, ServerVersion, SyncEvent,
};

fn entry(id: &str) -> InventoryEntry {
    InventoryEntry {
        id: ObjectId(id.into()),
        memberships: Vec::new(),
        size: None,
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

fn entries(prefix: &str, count: usize) -> Vec<InventoryEntry> {
    (0..count)
        .map(|n| entry(&format!("{prefix}-{n}")))
        .collect()
}

fn barrier_report(scope: &CursorScope) -> InventoryCoverageReport {
    let error = AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("unidentifiable value"),
        }),
    )
    .try_build()
    .expect("valid account error classification");
    InventoryCoverageReport::degraded(
        CoverageDomain::full(scope.clone()),
        vec![InventoryObligation::Region {
            key: ObligationKey(b"blocked-page".to_vec()),
            failure_label: "unidentifiable-value".into(),
            error,
            recovery: RegionRecovery::barrier(),
        }],
    )
}

fn batch(items: Vec<InventoryEntry>, coverage: InventoryCoverageReport) -> InventoryEvent {
    InventoryEvent::Batch(
        InventoryBatch::try_new(
            items,
            PageBoundary::Final,
            Duration::ZERO,
            0,
            None,
            coverage,
        )
        .expect("a Final page with no checkpoint is boundary-valid"),
    )
}

// Paused time: the waived retry below waits out the engine's real retry
// backoff, which is otherwise five seconds of wall clock per sweep.
#[tokio::test(start_paused = true)]
async fn a_barrier_stops_the_whole_scope_walk_through_the_real_orchestrator() {
    let account_id = AccountId("barrier-scope".to_owned());
    let scope = CursorScope::Account;

    let mut stub = common::StubAccount::new(vec![scope.clone()]);
    // Three fixed page partitions of ten: 0..10, 10..20, 20..30.
    stub.partitioning = InventoryPartitioning::PageCount {
        total: Some(30),
        page_size: Some(10),
    };
    let hook_scope = scope.clone();
    stub.partition_hook = Some(Arc::new(move |scope, partition| {
        assert_eq!(scope, &hook_scope, "one scope in this plan");
        match partition {
            InventoryPartition::Page { from: 0, .. } => vec![
                batch(
                    entries("clean", 10),
                    InventoryCoverageReport::complete(CoverageDomain::full(scope.clone())),
                ),
                InventoryEvent::Done(InventoryCompletion::complete(
                    CoverageDomain::full(scope.clone()),
                    None,
                    PartitionEnd::MoreBeyond,
                )),
            ],
            // The second partition delivers three real entries and then
            // reports ground it cannot represent and cannot replay. It ends in
            // a `Done` carrying the same coverage, as every walk must: a
            // stream that just stops is now read as incomplete, so the waived
            // retry below would never reach the completion sentinel.
            InventoryPartition::Page { from: 10, .. } => vec![
                batch(entries("torn", 3), barrier_report(scope)),
                InventoryEvent::Done(InventoryCompletion::new(
                    None,
                    barrier_report(scope),
                    PartitionEnd::MoreBeyond,
                )),
            ],
            // The third partition must never be requested; answering
            // emptily keeps the failure observable in `walked` below
            // rather than crashing a detached engine task.
            _ => vec![InventoryEvent::Done(InventoryCompletion::complete(
                CoverageDomain::full(scope.clone()),
                None,
                PartitionEnd::Exhausted,
            ))],
        }
    }));
    let stub = Arc::new(stub);

    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![Arc::clone(&stub)]));

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    // Drain the broadcast until the barrier warning lands, recording every
    // checkpoint-bearing batch on the way. The orchestrator parks until this
    // subscriber exists, so nothing was raced away before the subscribe.
    let mut checkpointed_batches = 0_usize;
    let mut stripped_batch_items = None;
    let warning = loop {
        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("the barrier warning must arrive")
            .expect("the broadcast stays open");
        match event.event.as_ref() {
            SyncEvent::Batch(b) => {
                assert!(
                    !matches!(&event.checkpoint, Some(Checkpoint::Backfill(bf))
                        if bf.progress.items_done > 10),
                    "no checkpoint may certify ground past the barrier page"
                );
                if event.checkpoint.is_some() {
                    checkpointed_batches += 1;
                    assert!(
                        event.publication.is_some(),
                        "every checkpoint-bearing broadcast carries its publication id"
                    );
                } else if !b.items.is_empty() {
                    stripped_batch_items = Some(b.items.len());
                }
            }
            SyncEvent::Warning(warning) => break warning.clone(),
            other => panic!("unexpected event on the cold-start stream: {other:?}"),
        }
    };
    assert!(
        warning.message.value.contains("cannot represent"),
        "the barrier warning names the blocked region count: {}",
        warning.message.value
    );

    // The barrier page's items were genuinely observed and must be delivered,
    // but its checkpoint is stripped.
    assert_eq!(
        stripped_batch_items,
        Some(3),
        "the refused page's items are delivered without a checkpoint"
    );
    assert_eq!(
        checkpointed_batches, 1,
        "only the clean page before the barrier carries a checkpoint"
    );

    // The load-bearing assertion, one layer above the driver unit tests: the
    // partition PAST the barrier was never requested from the account.
    let walked: Vec<InventoryPartition> = stub
        .walked_partitions()
        .into_iter()
        .map(|(_, partition)| partition)
        .collect();
    assert_eq!(
        walked,
        vec![
            InventoryPartition::Page { from: 0, to: 10 },
            InventoryPartition::Page { from: 10, to: 20 },
        ],
        "a stopped scope walk hands out no further partition"
    );

    // Nothing was acknowledged, so nothing is durable - and in particular no
    // completion sentinel exists for the next attach to skip the re-walk on.
    assert!(
        store
            .get_backfill(&account_id, &scope)
            .await
            .expect("store read succeeds")
            .is_none(),
        "no backfill checkpoint may become durable without a consumer ack"
    );

    assert!(
        engine
            .waive_obligation(
                &account_id,
                ObligationKey(b"blocked-page".to_vec()),
                "test operator".to_owned(),
            )
            .await
            .expect("waiver persists"),
        "the recorded barrier is addressable by its obligation key"
    );

    // The next retry re-checks the waiver at the barrier hit, converts it to
    // unresolved waived debt, crosses it, requests the later partition, and
    // emits the completion sentinel. This is the end-to-end escape hatch A8
    // was missing.
    let released = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = events.recv().await.expect("broadcast stays open");
            if matches!(
                &event.checkpoint,
                Some(Checkpoint::Backfill(checkpoint)) if checkpoint.partition.0 == b"complete"
            ) {
                break;
            }
        }
    })
    .await;
    assert!(
        released.is_ok(),
        "a waived barrier must release the scope on its next retry; walked: {:?}",
        stub.walked_partitions()
    );

    assert!(
        stub.walked_partitions()
            .iter()
            .any(|(_, partition)| { *partition == InventoryPartition::Page { from: 20, to: 30 } }),
        "crossing a waived barrier reaches the partition beyond it"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}
