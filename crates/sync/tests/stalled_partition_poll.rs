//! A wedged `inventory_partition_stream` must not park the backfill runner.
//!
//! Same shape as the changes-drive wedge, different signal: the runner selects
//! the required publisher shutdown token independently of a capacity gate, rather
//! than the boundary. What makes this one delicate is not the arm but the
//! OUTCOME. A cancelled partition must never report `complete: true` or
//! `RequestNextPartition`, because the orchestrator would then write a
//! completion sentinel and the next attach would skip the scope entirely -
//! silent data invisibility, not re-work. Returning `Error::ShuttingDown`
//! sidesteps the question by reporting no outcome at all.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::backfill::{BackfillRunner, LiveSupersedes};
use bifrost_sync::delivery::{ChangeDelivery, PagePublisher};
use bifrost_sync::engine::backfill_bound::BackfillBound;
use bifrost_sync::{Boundary, Error, SyncControl};
use bifrost_sync::{BudgetGate, ConcurrencyBudget, Scheduler, SchedulerConfig};
use bifrost_types::{Account, AccountId, CursorScope, InventoryPartition, Priority};
use tokio_util::sync::CancellationToken;

use common::StubAccount;

const PROMPT: Duration = Duration::from_secs(5);

/// Cancelling a partition walk parked on the provider fails the partition
/// rather than reporting a completed one.
///
/// Both halves matter. Without the arm the walk never returns at all, and the
/// worker burns a full `detach_timeout` before being aborted. With the arm but
/// a partial `BackfillPartitionOutcome` instead of an error, a wrong
/// partition verdict could mint a completion sentinel for a scope
/// that was never walked.
#[tokio::test]
async fn a_cancelled_partition_walk_fails_rather_than_reporting_completion() {
    cancelled_partition(true).await;
}

#[tokio::test]
async fn a_cancelled_partition_walk_without_a_lane_fails_rather_than_reporting_completion() {
    cancelled_partition(false).await;
}

async fn cancelled_partition(with_lane: bool) {
    let scope = CursorScope::Account;
    let mut stub = StubAccount::new(vec![scope.clone()]);
    let release = Arc::new(tokio::sync::Notify::new());
    stub.partition_stall = Some(Arc::clone(&release));
    let account: Arc<dyn Account> = Arc::new(stub);

    let account_id = AccountId("stalled-partition".to_owned());
    let shutdown = CancellationToken::new();
    let (boundary, view) = Boundary::new();
    let (priority, p) = tokio::sync::watch::channel(Priority::Normal);
    let (bandwidth, b) = tokio::sync::watch::channel(None);
    std::mem::forget((view, p, b));
    let control = SyncControl::new(account_id.clone(), boundary, priority, bandwidth);
    let (tx, _rx) = tokio::sync::broadcast::channel(16);
    let (writer_tx, writer_task) = bifrost_sync::engine::writer::WriterHandle::spawn(
        bifrost_types::AccountId("stalled-partition".into()),
        Arc::new(bifrost_sync::InMemoryCheckpointStore::new()),
        control.clone(),
        4,
    );
    writer_task.abort();
    let publisher = PagePublisher::new(
        Arc::new(ChangeDelivery::new(tx)),
        control.clone(),
        writer_tx,
        shutdown.clone(),
    );
    let scheduler = Scheduler::new(
        SchedulerConfig::default(),
        BudgetGate::new(ConcurrencyBudget {
            per_account: 8,
            global: 64,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    );
    scheduler.budget().register(account_id.clone());
    let gate = BackfillBound::new(
        Arc::clone(publisher.coverage()),
        4,
        shutdown.clone(),
        scheduler,
        account_id,
        control,
    );
    let live = LiveSupersedes::new();

    let walk = async {
        BackfillRunner::run_partition(
            account.as_ref(),
            scope,
            InventoryPartition::Full,
            &live,
            bifrost_types::TraitEnvelopeVersion::new(1),
            &publisher,
            walk_generation(0),
            if with_lane {
                Some(gate.admit().await.expect("partition admission"))
            } else {
                None
            },
            bifrost_sync::cursor::PendingCoverage::initial_fence(),
        )
        .await
    };
    let walk = std::pin::pin!(walk);

    // Reach the poll and park before cancelling, so this exercises the arm
    // rather than an entry check.
    let cancelled = async {
        tokio::task::yield_now().await;
        shutdown.cancel();
    };
    let (outcome, ()) = tokio::time::timeout(PROMPT, futures::future::join(walk, cancelled))
        .await
        .expect("a cancelled partition walk must not wait on the provider");

    assert!(
        matches!(outcome, Err(Error::ShuttingDown)),
        "a cancelled walk reports no outcome at all, so it cannot certify \
         completion: {outcome:?}"
    );
    release.notify_waiters();
}

// Mint test proof orders through the same owner as production walks.
fn walk_generation(value: u64) -> bifrost_sync::cursor::WalkGeneration {
    let coverage = bifrost_sync::cursor::PendingCoverage::new();
    for _ in 0..value {
        let _ = coverage.next_generation();
    }
    coverage.next_generation()
}
