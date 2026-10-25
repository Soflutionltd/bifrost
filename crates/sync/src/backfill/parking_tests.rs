use super::*;

use crate::test_common as common;

use bifrost_types::{
    CoverageDomain, InventoryCompletion, InventoryEvent, PartitionEnd, ThrottleKey,
};
use std::sync::Mutex;

struct Harness {
    ctx: SlotContext,
    registry: Arc<BackfillRegistry>,
    walked: Arc<Mutex<Vec<(CursorScope, InventoryPartition)>>>,
    task: tokio::task::JoinHandle<()>,
    receiver: Option<crate::multiplexer::ChangesReceiver>,
}

fn scope() -> CursorScope {
    CursorScope::Account
}

async fn drain() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

impl Harness {
    async fn new() -> Self {
        let account_id = AccountId("parking".into());
        let throttles = Arc::new(Mutex::new(crate::recovery::ThrottleBucket::new()));
        let held = ThrottleKey::Budget(bifrost_types::ThrottleBudget::new("parking-budget"));
        let bucket = Arc::clone(&throttles);
        let key = held.clone();
        let once = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut stub = common::StubAccount::new(vec![scope()]);
        stub.partitioning = InventoryPartitioning::PageCount {
            total: Some(3),
            page_size: Some(1),
        };
        stub.scope_keys = Some(Arc::new(move |s| {
            if *s == scope() {
                vec![key.clone()]
            } else {
                vec![]
            }
        }));
        stub.partition_hook = Some(Arc::new(move |s, p| {
            if *s == scope()
                && *p == (InventoryPartition::Page { from: 0, to: 1 })
                && !once.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                bucket.lock().unwrap().record(
                    held.clone(),
                    tokio::time::Instant::now() + Duration::from_secs(60),
                );
            }
            vec![
                InventoryEvent::Batch(
                    bifrost_types::InventoryBatch::try_new(
                        vec![],
                        PageBoundary::Final,
                        Duration::ZERO,
                        0,
                        None,
                        bifrost_types::InventoryCoverageReport::complete(CoverageDomain::full(
                            s.clone(),
                        )),
                    )
                    .unwrap(),
                ),
                InventoryEvent::Done(InventoryCompletion::complete(
                    CoverageDomain::full(s.clone()),
                    None,
                    PartitionEnd::MoreBeyond,
                )),
            ]
        }));
        let walked = Arc::<Mutex<Vec<(CursorScope, InventoryPartition)>>>::clone(&stub.walked);
        let caps = stub.caps.clone();
        let account: Arc<dyn Account> = Arc::new(stub);
        let factory = Arc::new(common::StubFactory::queue(vec![]));
        let cursors = Arc::new(CursorRegistry::new());
        cursors.put(common::cursor_for(&scope(), b"initial"));
        let coverage = Arc::new(PendingCoverage::new());
        let (boundary, _) = crate::cancel::Boundary::new();
        let (priority, _) = watch::channel(bifrost_types::Priority::Normal);
        let (bandwidth, _) = watch::channel(None);
        let control = SyncControl::new_with_publications(
            account_id.clone(),
            boundary.clone(),
            priority,
            bandwidth,
            Arc::<PendingCoverage>::clone(&coverage),
        );
        let (changes_tx, _) = broadcast::channel(64);
        let delivery = Arc::new(ChangeDelivery::new(changes_tx.clone()));
        let gate =
            crate::scheduler::BudgetGate::new(crate::scheduler::ConcurrencyBudget::default());
        gate.register(account_id.clone());
        let scheduler = Scheduler::new(crate::SchedulerConfig::default(), gate);
        let (writer_tx, mut writer_rx) = mpsc::channel(16);
        tokio::spawn(async move {
            while let Some(request) = writer_rx.recv().await {
                match request {
                    WriterRequest::ScopeBarrierBlocked { done, .. } => {
                        let _ = done.send(false);
                    }
                    WriterRequest::DiscardBackfillProgress { done, .. } => {
                        let _ = done.send(Ok(()));
                    }
                    _ => panic!("unexpected writer request"),
                }
            }
        });
        let ctx = SlotContext {
            deferred_schema_reset: Arc::new(crate::engine::DeferredSchemaReset::default()),
            bandwidth_budget: None,
            factory,
            current: Arc::new(ArcSwap::from_pointee(account)),
            cursors,
            changes_tx,
            account_id,
            control,
            account_control_tx: broadcast::channel(8).0,
            throttles,
            boundary_tx: boundary.sender(),
            capabilities: Arc::new(std::sync::RwLock::new(caps)),
            subscriptions: Arc::new(SubscriptionRegistry::new()),
            account_generation_tx: watch::channel(0).0,
            reopen_lock: Arc::new(AsyncMutex::new(())),
            open_skips: Arc::new(Mutex::new(vec![])),
            shutdown: CancellationToken::new(),
            writer_tx,
            coverage,
            subscriber_notify: Arc::new(Notify::new()),
            scheduler,
            delivery,
            backfill_capacity: 64,
            reopen_tx: mpsc::channel(8).0,
            deferred_repairs: Arc::new(DeferredScopeRepairs::new()),
            scope_tokens: Arc::new(Mutex::new(HashMap::new())),
        };
        let receiver = Some(ctx.delivery.subscribe(
            Some(ctx.control.clone()),
            Some(Arc::<PendingCoverage>::clone(&ctx.coverage)),
        ));
        let registry = Arc::new(BackfillRegistry::new());
        let task = tokio::spawn(run_backfill_orchestrator(
            ctx.clone(),
            BackfillWiring {
                live: Arc::new(LiveSupersedes::new()),
                store: Arc::new(crate::cursor::InMemoryCheckpointStore::new()),
                registry: Arc::clone(&registry),
                config: BackfillConfig::default(),
                fusion_owned_scopes: HashSet::new(),
                changes_tx: Some(ctx.changes_tx.clone()),
            },
        ));
        drain().await;
        Self {
            ctx,
            registry,
            walked,
            task,
            receiver,
        }
    }

    fn partitions(&self) -> Vec<InventoryPartition> {
        self.walked
            .lock()
            .unwrap()
            .iter()
            .filter(|(s, _)| *s == scope())
            .map(|(_, p)| p.clone())
            .collect()
    }

    fn state(&self) -> Option<BackfillState> {
        self.registry.snapshot(&self.ctx.account_id, &scope())
    }

    async fn advance(&self, seconds: u64) {
        tokio::time::advance(Duration::from_secs(seconds)).await;
        drain().await;
    }

    async fn stop(self) {
        self.ctx.shutdown.cancel();
        self.task.await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn parking_later_partition_allows_another_scope() {
    let h = Harness::new().await;
    let other = CursorScope::Type(bifrost_types::ObjectType::Email);
    h.ctx.cursors.put(common::cursor_for(&other, b"other"));
    h.advance(1).await;
    assert!(h.walked.lock().unwrap().iter().any(|(s, _)| *s == other));
    assert_eq!(h.partitions().len(), 1);
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn parking_fixed_plan_retries_the_pending_partition() {
    let h = Harness::new().await;
    assert_eq!(h.state(), Some(BackfillState::Pending));
    h.advance(60).await;
    assert_eq!(
        h.partitions(),
        vec![
            InventoryPartition::Page { from: 0, to: 1 },
            InventoryPartition::Page { from: 1, to: 2 },
            InventoryPartition::Page { from: 2, to: 3 }
        ]
    );
    assert_eq!(h.state(), Some(BackfillState::Completed));
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn parking_page_loss_restarts_without_completion() {
    let mut h = Harness::new().await;
    assert_eq!(h.ctx.coverage.undelivered_watermark(&scope()), 0);
    drop(h.receiver.take());
    assert_eq!(h.ctx.coverage.undelivered_watermark(&scope()), 1);
    h.receiver = Some(h.ctx.delivery.subscribe(
        Some(h.ctx.control.clone()),
        Some(Arc::<PendingCoverage>::clone(&h.ctx.coverage)),
    ));
    h.advance(60).await;
    assert_eq!(h.partitions().len(), 1, "the holed walk must be abandoned");
    assert_eq!(h.state(), Some(BackfillState::Pending));
    h.advance(5).await;
    assert_eq!(
        h.partitions()[1],
        InventoryPartition::Page { from: 0, to: 1 }
    );
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn parking_incarnation_change_discards_the_old_walk() {
    let h = Harness::new().await;
    h.ctx.cursors.delete(&scope());
    h.ctx
        .cursors
        .put(common::cursor_for(&scope(), b"replacement"));
    h.advance(60).await;
    assert_eq!(
        h.partitions()[1],
        InventoryPartition::Page { from: 0, to: 1 }
    );
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn parking_resume_preserves_the_original_fence() {
    let h = Harness::new().await;
    assert_eq!(
        h.state(),
        Some(BackfillState::Pending),
        "the walk must park before the reset"
    );
    h.ctx.coverage.invalidate_scope(&scope());
    h.advance(60).await;
    assert_eq!(
        h.partitions().len(),
        1,
        "reset must reject the pending partition before its provider call"
    );
    assert_eq!(h.state(), Some(BackfillState::Pending));
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn parking_resume_rechecks_a_longer_hold() {
    let h = Harness::new().await;
    h.ctx.throttles.lock().unwrap().record(
        ThrottleKey::Budget(bifrost_types::ThrottleBudget::new("parking-budget")),
        tokio::time::Instant::now() + Duration::from_secs(120),
    );
    h.advance(60).await;
    assert_eq!(h.partitions().len(), 1);
    assert_eq!(h.state(), Some(BackfillState::Pending));
    h.advance(60).await;
    assert_eq!(h.partitions().len(), 3);
    assert_eq!(h.state(), Some(BackfillState::Completed));
    h.stop().await;
}
