//! One account attachment and its owned worker and registration lifetimes.

use super::*;

/// Workers share the installed slot; they do not rebuild its handles.
pub(crate) type SlotContext = Arc<AccountSlot>;

/// Engine-side per-account state. Held inside the engine's
/// `DashMap<AccountId, Arc<AccountSlot>>`.
///
/// `current` is an `ArcSwap<crate::types::Incarnation>` so a reopen swap
/// becomes visible to spawned workers (reconciler, multiplexer,
/// backfill, mutation) at their next iteration without re-spawning.
/// Workers load with `current.load_full()`.
pub(crate) struct AccountSlot {
    pub account_id: AccountId,
    /// The account's single durable writer, taken by detach.
    ///
    /// Reattach persists and rolls back replacement cursors through this
    /// channel rather than touching the store directly, so both are ordered
    /// against consumer acknowledgements. A replacement's inventory pass
    /// broadcasts checkpoint-bearing batches before the cutover: a direct
    /// write racing the ack writer let an aborted reattach delete a cursor a
    /// consumer had already acknowledged.
    pub writer: std::sync::Mutex<Option<WriterHandle>>,
    pub subscriptions: Arc<SubscriptionRegistry>,
    pub deferred_repairs: Arc<DeferredScopeRepairs>,
    pub scope_tokens: crate::multiplexer::ScopeTokens,
    pub backfill_capacity: usize,
    pub push_drain_wait: Duration,
    pub registrations: std::sync::Mutex<Option<SlotRegistrations>>,
    pub deferred_schema_reset: Arc<crate::engine::DeferredSchemaReset>,
    /// Strong owner across replacement opens, released with the slot.
    pub bandwidth_budget: Option<bifrost_net::BandwidthBudget>,
    pub scheduler: Scheduler,
    pub factory: Arc<dyn AccountFactory>,
    pub current: Arc<ArcSwap<crate::types::Incarnation>>,
    /// Monotonic signal bumped after a successful handle swap, carrying the
    /// generation of the incarnation just installed. Long-lived push and
    /// lifecycle streams select on it so they drop the old stream even when
    /// that stream never terminates itself. A wake signal only: a stale
    /// recovery request is judged against `current`, never against this.
    pub account_generation_tx: watch::Sender<crate::types::AccountGeneration>,
    pub push_health_tx: watch::Sender<crate::push::health::PushHealth>,
    pub push_topology_tx: watch::Sender<crate::engine::PushTopologyGeneration>,
    pub push_retired_tx: watch::Sender<crate::types::AccountGeneration>,
    pub reopen_lock: Arc<tokio::sync::Mutex<()>>,
    pub capabilities: Arc<std::sync::RwLock<AccountCapabilities>>,
    /// Held for the slot's lifetime so the handle's owned resources are
    /// released with the slot; engine code reaches the multiplexer through
    /// `ChangeDelivery` and `PagePublisher`, not through this field.
    #[allow(dead_code)]
    pub multiplexer: MultiplexerHandle,
    pub cursors: Arc<CursorRegistry>,
    /// What each scope's latest enumeration proved, pending the
    /// acknowledgement that makes it durable. Shared with the account's single
    /// writer, which reads it back to build one atomic checkpoint record.
    pub coverage: Arc<crate::cursor::PendingCoverage>,
    pub boundary: Boundary,
    pub shutdown: CancellationToken,
    pub control: SyncControl,
    /// Sentinel receiver keeps `changes_tx` alive across periods with
    /// no subscribers so new subscribers don't get a closed-channel
    /// error.
    #[allow(dead_code)]
    pub _sentinel_rx: tokio::sync::broadcast::Receiver<crate::delivery::MultiplexerEvent>,
    /// Spawned workers paired with their abort handles. `detach`
    /// aborts each worker on timeout so tasks do not leak past the
    /// configured deadline.
    pub workers: std::sync::Mutex<Vec<WorkerTask>>,
    /// Per-account control stream. Engine publishes
    /// `AccountControl::Pause(reason)` when the engine has automatically
    /// paused the account (operator-override directive, retry budget
    /// exhausted, tenant throttle). Consumers subscribe via
    /// `SyncEngine::account_control_stream`.
    pub account_control_tx: broadcast::Sender<AccountControl>,
    /// Sentinel receiver keeps the account-control channel alive.
    #[allow(dead_code)]
    pub _account_control_sentinel: broadcast::Receiver<AccountControl>,
    /// Notify fired when a new real subscriber arrives on
    /// `changes_tx`. Lets deferred-inventory workers park on `notified`
    /// instead of hot-polling `receiver_count`.
    pub subscriber_notify: Arc<Notify>,
    /// Per-account reopen channel. The bulk-mutation campaign routes
    /// `EngineDirective::*` raised mid-stream through here so the
    /// reopen listener dispatches via `handle_engine_directive`.
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    /// Slot-fenced view of engine-wide deadlines (shared so tenant /
    /// provider budgets cross accounts). Retirement refuses new account and
    /// resource holds without erasing existing waits or late provider-budget
    /// evidence. Recovery paths record waits
    /// keyed by `ThrottleKey`; the per-scope poll loop and the push
    /// reconciler consult it before driving work.
    pub throttles: Arc<crate::throttle::ThrottleBucket>,
    /// The `OpenedAccount::skipped_scopes` lane from the most recent
    /// successful `factory.open` (attach or reopen swap): the parts of
    /// the account surface the protocol crate discovered but could not
    /// bring up, with their classified errors. Exposed via
    /// `SyncEngine::open_skipped_scopes` so a consumer can tell a
    /// degraded shared namespace apart from one that does not exist.
    pub open_skips: Arc<std::sync::Mutex<Vec<bifrost_types::SkippedScope>>>,
    /// The account's change broadcast plus the numbering of the receivers it has
    /// handed out.
    ///
    /// The engine numbers them because `tokio::broadcast` cannot say whether a
    /// given receiver can still take delivery of a given batch, and a subscriber
    /// COUNT answers that question wrongly the moment one consumer is replaced by
    /// an overlapping successor. One mutex orders send-and-stamp,
    /// subscribe-and-number, and unregister-and-sweep, so the answer is exact
    /// rather than approximate.
    pub delivery: Arc<crate::delivery::ChangeDelivery>,
}

pub(crate) struct WorkerTask {
    pub role: WorkerRole,
    pub join: tokio::task::JoinHandle<()>,
    pub abort: tokio::task::AbortHandle,
}

/// Which spawned slot worker a [`WorkerTask`] is.
///
/// One variant per spawn site in `engine/attach.rs`, so the `role` field on the
/// detach worker phase's warn lines (rendered with `Debug`, which prints the
/// variant name) names the worker that wedged or panicked. Only `AckWriter` has
/// behavior attached (it is torn down in its own phase, last); every other
/// variant is a stream-phase worker and exists purely for attribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerRole {
    /// Durably persists cursors as they are acked; drained in its own phase.
    AckWriter,
    /// Forwards priority and bandwidth-cap changes to the open handle.
    ControlApplier,
    /// Push reconciler: turns watch events into driven scopes.
    PushReconciler,
    /// Supervises push sessions and their bounded cleanup.
    PushSupervisor,
    /// The per-scope poll multiplexer.
    Multiplexer,
    /// Walks registered scopes and runs their backfill partitions.
    BackfillOrchestrator,
    /// Establishes cold-start inventory once the slot has a subscriber.
    DeferredInventory,
    /// Serves reopen and scope-deleted requests.
    ReopenListener,
    /// Polls the bandwidth meter into the control's observed-bps atomic.
    BandwidthFeed,
    /// Retries the teardown of orphaned push subscriptions on a timer.
    OrphanTeardownRetrier,
}

impl AccountSlot {
    /// Dispatch a classified error through recovery, and park whatever the
    /// dispatch declined because the account was paused (or, for an account
    /// reset, could not complete), when nothing else would raise it again. See
    /// [`DeferredScopeRepairs`] and [`DeferredSchemaReset`].
    ///
    /// `expected_generation` is the generation of the incarnation that raised
    /// the error, when known (`ReopenRequest::PushRecovery`, a stamped
    /// `ReopenRequest::Recovery`, a deferred inventory walk): every
    /// engine-directive arm declines a stamped request as stale, under the
    /// reopen lock and before any effect, once that incarnation has been
    /// replaced. `None` acts on whichever incarnation is current.
    pub(super) async fn recover(
        &self,
        scope: Option<CursorScope>,
        error: AccountError,
        expected_generation: Option<crate::types::AccountGeneration>,
    ) {
        let writer = self.writer();
        let mut recovery = self.recovery(&writer);
        recovery.expected_generation = expected_generation;
        let followup = handle_account_error(&recovery, scope, error).await;
        self.park_followup(followup);
    }

    /// Hand a recovery follow-up to the parked state that owns it.
    pub(super) fn park_followup(&self, followup: RecoveryFollowUp) {
        match followup {
            RecoveryFollowUp::None => {}
            RecoveryFollowUp::ParkScope { scope, epoch } => self.deferred_repairs.park(
                scope,
                epoch,
                self.control.clone(),
                self.shutdown.clone(),
                self.reopen_tx.clone(),
            ),
            RecoveryFollowUp::ParkScopes(scopes) => {
                for (scope, epoch) in scopes {
                    self.deferred_repairs.park(
                        scope,
                        epoch,
                        self.control.clone(),
                        self.shutdown.clone(),
                        self.reopen_tx.clone(),
                    );
                }
            }
            RecoveryFollowUp::ParkSchemaReset(scopes) => self.deferred_schema_reset.park(
                scopes,
                self.control.clone(),
                self.shutdown.clone(),
                self.reopen_tx.clone(),
            ),
        }
    }

    /// The producer-side door onto the backfill bound, already carrying this
    /// slot's shutdown token so a parked producer always has a way out.
    ///
    /// The gate holds no ledger of its own - capacity is a property of the
    /// publication records in `coverage`. It bundles the bound, the token and
    /// admission configuration. Each admitted partition owns its permit by
    /// value, independently of this cloneable bound.
    pub(super) fn backfill_bound(&self) -> BackfillBound {
        BackfillBound::new(
            Arc::clone(&self.coverage),
            self.backfill_capacity,
            self.shutdown.clone(),
            self.scheduler.clone(),
            self.account_id.clone(),
            self.control.clone(),
        )
    }
}

impl AccountSlot {
    /// Clone the writer while this slot still owns it. Once detach begins, old slot holders get a closed endpoint rather than a replacement
    /// attachment's writer.
    pub(super) fn writer(&self) -> WriterHandle {
        self.writer
            .lock()
            .expect("slot writer lock poisoned")
            .clone()
            .unwrap_or_else(WriterHandle::closed)
    }

    pub(super) fn take_writer(&self) -> Option<WriterHandle> {
        self.writer
            .lock()
            .expect("slot writer lock poisoned")
            .take()
    }

    pub(super) fn recovery<'a>(&'a self, writer: &'a WriterHandle) -> RecoveryContext<'a> {
        RecoveryContext {
            slot: self,
            writer,
            expected_generation: None,
        }
    }
}

/// Owns the engine-wide indexes for one attachment. Detach takes this guard
/// after close, before releasing the lifecycle reservation. Retained slot Arcs
/// must never unregister a later attachment when they eventually drop.
pub(super) struct SlotRegistrations {
    pub account_id: AccountId,
    pub sink: Arc<InvalidationSinkInner>,
    pub budget: BudgetGate,
    pub backfill_registry: Arc<BackfillRegistry>,
    pub bandwidth_meter: Option<Arc<bifrost_net::BandwidthMeter>>,
}

impl Drop for SlotRegistrations {
    fn drop(&mut self) {
        self.sink.unregister(&self.account_id);
        self.budget.forget(&self.account_id);
        self.backfill_registry.forget_account(&self.account_id);
        if let Some(meter) = &self.bandwidth_meter {
            meter.forget_account(&self.account_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::WorkKind;
    use crate::test_common::{StubAccount, StubFactory};

    #[tokio::test(start_paused = true)]
    async fn retained_slot_neither_pins_writer_drain_nor_retires_a_later_attach() {
        let engine = SyncEngine::builder().build().expect("engine");
        let account_id = AccountId("retained-slot".into());
        let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![
            Arc::new(StubAccount::new(Vec::new())),
            Arc::new(StubAccount::new(Vec::new())),
        ]));
        engine
            .attach(account_id.clone(), Arc::clone(&factory))
            .await
            .expect("attach");
        let retired = engine.slot(&account_id).expect("slot");
        let hold = bifrost_types::ThrottleKey::Account(account_id.clone());
        let known_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        retired.throttles.record(hold.clone(), known_deadline);
        tokio::time::timeout(Duration::from_millis(100), engine.detach(&account_id))
            .await
            .expect("retaining the slot must not spend the writer timeout")
            .expect("detach");
        assert!(matches!(
            retired.writer().get_ledger().await,
            Err(Error::ShuttingDown)
        ));

        engine
            .attach(account_id.clone(), factory)
            .await
            .expect("fresh attach");
        let fresh = engine.slot(&account_id).expect("fresh slot");
        // Removing attach's per-slot view makes this late write extend the
        // successor's hold. Erasing holds on detach instead loses the known
        // deadline below. Both outcomes violate reconnect pacing.
        retired.throttles.record(
            hold.clone(),
            tokio::time::Instant::now() + Duration::from_secs(120),
        );
        let now = tokio::time::Instant::now();
        assert!(
            known_deadline > now,
            "the carryover assertion needs a live hold"
        );
        assert_eq!(
            fresh.throttles.wait_for(&hold, now),
            known_deadline.checked_duration_since(now)
        );
        let handle = bifrost_types::SubscriptionHandle("fresh-handle".into());
        assert!(fresh.subscriptions.record(handle.clone(), Vec::new()));
        assert!(!retired.subscriptions.record(
            bifrost_types::SubscriptionHandle("late-old-handle".into()),
            Vec::new()
        ));
        drop(retired);
        assert_eq!(fresh.subscriptions.snapshot()[0].handle, handle);
        let permit = engine
            .scheduler
            .budget()
            .acquire(&account_id, WorkKind::Sync)
            .await
            .expect("retired slot cannot forget the replacement's budget");
        drop(permit);
        engine.detach(&account_id).await.expect("final detach");
    }
}
