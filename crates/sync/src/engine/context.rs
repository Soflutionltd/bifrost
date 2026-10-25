//! The per-slot worker context.
//!
//! Every long-running worker `attach` spawns needs some subset of the
//! same eighteen handles, and each one used to take them as positional
//! arguments - the backfill orchestrator carried sixteen, the deferred
//! inventory worker twenty, and the reopen listener rebuilt a
//! seventeen-field [`RecoveryContext`] literal by hand at each of its
//! call sites. Bundling them once at the end of `attach` means a worker
//! takes the context plus only the handles that are genuinely its own,
//! and a new slot-wide handle is added in one place rather than threaded
//! through four signatures.
//!
//! Cheap to clone by construction: every field is an `Arc`, a channel
//! sender, or a handle that is itself `Arc`-backed.

use super::*;

/// Slot-wide handles shared by the workers `attach` spawns.
#[derive(Clone)]
pub(super) struct SlotContext {
    pub bandwidth_budget: Option<bifrost_net::BandwidthBudget>,
    pub factory: Arc<dyn AccountFactory>,
    /// The currently-open protocol handle. Workers load through the
    /// `ArcSwap` on every hot-path use so a reopen is observed
    /// immediately.
    pub current: Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: Arc<CursorRegistry>,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
    pub account_id: AccountId,
    pub control: SyncControl,
    pub account_control_tx: broadcast::Sender<AccountControl>,
    pub throttles: Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>,
    pub boundary_tx: watch::Sender<crate::cancel::BoundaryRequest>,
    pub capabilities: Arc<std::sync::RwLock<AccountCapabilities>>,
    pub subscriptions: Arc<SubscriptionRegistry>,
    pub account_generation_tx: watch::Sender<u64>,
    pub push_retired: watch::Sender<u64>,
    pub push_topology: watch::Sender<u64>,
    pub push_drain_wait: Duration,
    pub reopen_lock: Arc<AsyncMutex<()>>,
    /// Slot-lifetime open-skip lane; a successful reattach replaces it
    /// with the replacement open's `OpenedAccount::skipped_scopes`.
    pub open_skips: Arc<std::sync::Mutex<Vec<SkippedScope>>>,
    /// Per-slot child of the engine's root cancellation token.
    pub shutdown: CancellationToken,
    /// Sender onto the account's single durable writer.
    pub writer_tx: mpsc::Sender<WriterRequest>,
    pub coverage: Arc<PendingCoverage>,
    /// Fired when a real consumer subscribes to `changes_tx`, so a
    /// cold-start worker can park instead of broadcasting into a channel
    /// with no reader.
    pub subscriber_notify: Arc<Notify>,
    pub scheduler: Scheduler,
    /// The account's change broadcast plus its receiver numbering. The backfill
    /// producer publishes through it so a page's send and its delivery stamp are
    /// one step; see `multiplexer::ChangeDelivery`.
    pub delivery: Arc<ChangeDelivery>,
    /// How many published-but-unanswered backfill batches the account may have
    /// in flight before its cold-start producer parks.
    pub backfill_capacity: usize,
    /// The slot's own reopen channel, for re-offering a declined scope repair.
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    /// Scope repairs declined under a pause that nothing else will raise again.
    pub deferred_repairs: Arc<DeferredScopeRepairs>,
    /// An account-wide schema reset declined under a pause or exhausted, with
    /// the scopes it owes re-establishment, plus the count of resets that have
    /// committed during this attachment. See [`DeferredSchemaReset`].
    pub deferred_schema_reset: Arc<DeferredSchemaReset>,
    /// The multiplexer's per-scope poll tokens, for retiring a scope the
    /// reopen listener learns was deleted after it established it.
    pub scope_tokens: crate::multiplexer::ScopeTokens,
}

impl SlotContext {
    /// Dispatch a classified error through recovery, and park whatever the
    /// dispatch declined because the account was paused (or, for an account
    /// reset, could not complete), when nothing else would raise it again. See
    /// [`DeferredScopeRepairs`] and [`DeferredSchemaReset`].
    pub(super) async fn recover(&self, scope: Option<CursorScope>, error: AccountError) {
        self.recover_stamped(scope, error, None).await;
    }

    /// [`Self::recover`] for a request stamped with the account generation
    /// that raised it (`ReopenRequest::PushRecovery`): every engine-directive
    /// arm declines it as stale, under the reopen lock and before any effect,
    /// once the account it described has been replaced.
    pub(super) async fn recover_stamped(
        &self,
        scope: Option<CursorScope>,
        error: AccountError,
        expected_generation: Option<u64>,
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
    /// the account's scheduler admission, and that admission is the only state
    /// it owns.
    pub(super) fn lane_gate(&self) -> LaneGate {
        LaneGate::new(
            Arc::clone(&self.coverage),
            self.backfill_capacity,
            self.shutdown.clone(),
            self.scheduler.clone(),
            self.account_id.clone(),
            self.control.clone(),
        )
    }
}

impl SlotContext {
    /// A handle onto the account's single durable writer.
    ///
    /// Minted per use rather than stored, because `WriterHandle` is a
    /// thin wrapper over the sender and the recovery paths want one
    /// borrowed for the duration of a single dispatch.
    pub(super) fn writer(&self) -> WriterHandle {
        WriterHandle::new(self.writer_tx.clone())
    }

    /// Borrow this context as the recovery dispatch's argument bundle.
    ///
    /// The writer is passed separately because `RecoveryContext` borrows
    /// it, so it has to outlive the dispatch on the caller's stack.
    pub(super) fn recovery<'a>(&'a self, writer: &'a WriterHandle) -> RecoveryContext<'a> {
        RecoveryContext {
            deferred_schema_reset: &self.deferred_schema_reset,
            bandwidth_budget: self.bandwidth_budget.as_ref(),
            factory: &self.factory,
            current: &self.current,
            cursors: &self.cursors,
            changes_tx: &self.changes_tx,
            account_id: &self.account_id,
            control: &self.control,
            account_control_tx: &self.account_control_tx,
            throttles: &self.throttles,
            boundary_tx: &self.boundary_tx,
            capabilities: &self.capabilities,
            subscriptions: &self.subscriptions,
            account_generation_tx: &self.account_generation_tx,
            push_retired: &self.push_retired,
            push_drain_wait: self.push_drain_wait,
            expected_generation: None,
            reopen_lock: &self.reopen_lock,
            open_skips: &self.open_skips,
            shutdown: &self.shutdown,
            writer,
            coverage: &self.coverage,
            delivery: &self.delivery,
        }
    }
}
