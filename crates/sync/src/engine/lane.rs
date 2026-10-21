//! The bounded backfill lane: producer-side flow control for cold start.
//!
//! # Why a bound at all
//!
//! The per-account `changes_tx` is a `tokio::broadcast` with
//! `MultiplexerConfig::changes_capacity` slots (default 256) and NO
//! producer-side backpressure. That is the right shape for live changes -
//! several subscribers, each with its own cursor into one ring - and the wrong
//! shape for cold-start bulk delivery: a backfill of a large mailbox outruns a
//! consumer trivially, the ring overwrites unread pages, and the lag-abandonment
//! machinery fires as a matter of routine rather than as an exception. Every
//! abandonment costs a re-read from the last durable checkpoint.
//!
//! A bounded `mpsc` for the backfill lane was considered and NOT taken.
//! `account_changes_stream` hands every caller its own receiver and several live
//! subscribers are a supported shape; an `mpsc` is single-consumer, so backfill
//! pages would reach one of them and silently vanish for the rest. So the flow
//! control sits on the PRODUCER: before publishing a page, backfill waits until
//! the account has fewer than `BackfillConfig::lane_capacity` published-but-
//! UNREAD backfill batches.
//!
//! # Read, not acknowledged
//!
//! The permit is the consumer's RECEIPT of the page, recorded from
//! `ChangesReceiver`'s read path for numbered receivers. What overruns a ring is
//! pages nobody has taken out of it; an acknowledgement is a promise about the
//! consumer's own durability, made on whatever schedule the consumer likes, and
//! binding the producer to it deadlocked a consumer that batches its
//! acknowledgements against a producer parked here. The producer now runs
//! exactly as far ahead as the consumer reads. An acknowledgement still retires
//! the registration - and with it the boundary waiter and the coverage claim -
//! but for a page already read it frees no further capacity.
//!
//! With several numbered receivers the permit is the SLOWEST of them. A page is
//! still occupying the ring while any live receiver numbered below its
//! `delivered_at` has not read it, and a departing receiver stops charging the
//! pages it was holding. Following the fastest reader instead reproduced the very
//! overrun the bound exists to prevent: an observer that reads at once frees the
//! producer, the acknowledger that persists as it goes falls `changes_capacity`
//! events behind, lags, and the lag abandons every outstanding page.
//!
//! # Where the capacity actually lives, and why this module is thin
//!
//! In `PendingCoverage`, on the boundary registration itself. This module owns
//! no ledger of its own.
//!
//! That is the second design, and the reason for it is worth keeping. The first
//! kept a separate map of capacity permits beside the publications ledger, keyed
//! and released separately. Two consecutive cold reviews found nine defects
//! between them, and the same four-of-nine were all one mistake: the two
//! registries disagreed about what was in flight. A reset retired a publication
//! whose permit had not been inserted yet; supersession removed a record whose
//! permit the reset then could not name; an acknowledgement released capacity
//! the ledger had refused; a rejected acknowledgement released a different
//! lane's. Every one of those is a seam between two structures that must agree,
//! and the fix for each was another rule at another call site.
//!
//! `PendingCoverage::boundaries` already answers, exactly, "which publications
//! has the engine broadcast that the consumer has not answered for". So capacity
//! is a property of that entry - a set of reader sequences on the entry and on
//! each page it subsumed - and supersession, acknowledgement, retirement,
//! abandonment and scope invalidation each free capacity because they already
//! mutate it. There is no separate release call site to forget: the only thing
//! that ever RECORDS a release sits on the receiver's read path
//! (`ChangesReceiver::note_receipt`), and every other path frees capacity as a
//! side effect of the ledger mutation it was already making.
//!
//! **One point where the literal rule needed correcting**, and only one:
//! supersession removes the older entry deliberately - it is what lets a
//! consumer persist N batches of one partition, acknowledge only the last, and
//! still reach a boundary, for any N below the capacity. Under the bare rule
//! that would release capacity, and a single
//! partition could publish unboundedly many pages while never holding more than
//! one live record, so the bound would not bind at all. The survivor therefore
//! inherits the charge of what it superseded (`BoundaryEntry::subsumed`), each
//! folded page keeping its own reader set, and an acknowledgement of a
//! superseded id releases exactly that one page's worth. What the survivor
//! RETAINS is no longer bounded by the capacity, since a fully-read page carries
//! no charge - see `BoundaryEntry::subsumed` for what that costs and why the
//! pages cannot simply be dropped.
//!
//! # What the bound asks of the consumer
//!
//! Every receiver `account_changes_stream` hands out is NUMBERED, and a numbered
//! receiver that is held but never polled charges up to `lane_capacity` pages for
//! as long as it lives - parking cold start for the rest of the attachment. So
//! every numbered receiver MUST be polled or dropped. Nothing else reports that
//! stall: the departure warning fires on drop and the lag warning on poll, and a
//! receiver that does neither produces neither. [`LaneGate::wait_for_capacity`]
//! therefore warns when it parks, naming the account, the scopes and the receiver
//! sequences holding the charge. A receiver that only watches should be an
//! observer (`SyncEngine::account_changes_observer`, `ChangeDelivery::observe`),
//! which is unnumbered, counts for neither the subscriber gate nor the bound,
//! and carries no such obligation.
//!
//! # Fairness, and what a parked producer must not be holding
//!
//! Both producers send into the same ring, so delivery order is send order; the
//! bound is consulted only before a backfill send and never by a change drive.
//! When both have a batch ready, both send and neither waits. There is no merge
//! point and therefore no arbiter.
//!
//! That is only true if a parked backfill holds nothing the live lane needs. It
//! used to hold the account's SCHEDULER ADMISSION across the park, which with a
//! minimal budget (`per_account = 2`, the mutation share leaving one sync
//! permit) meant polling and push reconciliation could not be admitted until a
//! backfill acknowledgement arrived - the live lane starving on the very
//! mechanism that exists to protect it. [`BackfillAdmission`] is what this
//! module is actually for: the fast path keeps the permit, a park gives it up
//! and re-takes it on the wake, and the completion sentinel - which does no wire
//! work at all - never takes one.

use std::sync::{Arc, Mutex};

use bifrost_types::AccountId;
use tokio_util::sync::CancellationToken;

use crate::control::SyncControl;
use crate::cursor::PendingCoverage;
use crate::error::Error;
use crate::scheduler::{BudgetPermit, Scheduler, WorkKind};

/// How many backfill pages may be outstanding (published, unread) before the
/// producer parks.
///
/// Deliberately far below `MultiplexerConfig::changes_capacity` (256): the bound
/// is what keeps a cold start from overrunning the shared ring, so it has to
/// leave room for live traffic in the same ring.
pub const DEFAULT_BACKFILL_LANE_CAPACITY: usize = 64;

/// How often one gate repeats its park warning.
///
/// The warning is emitted when a park BEGINS, and a park that begins within this
/// window of the last warning is silent. Rate limited rather than one line per
/// park because the ordinary healthy case is a producer that parks, is freed by
/// one read and parks again: that shape would log once per page of the walk and
/// bury the case worth seeing, which is a park that never ends.
const PARK_WARN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// The account's scheduler admission, owned by the gate so a parked producer is
/// not sitting on budget the live lane needs.
///
/// The engine admits every wire path, and with a minimal budget the account has
/// a single sync permit. A backfill that filled the lane, started the next
/// partition, took that permit and then parked blocked polling and push
/// reconciliation until an acknowledgement arrived, and with `global = 1` other
/// accounts and mutations too. So the permit is dropped for the duration of a
/// park and re-acquired on the wake.
#[derive(Debug)]
pub struct BackfillAdmission {
    scheduler: Scheduler,
    account_id: AccountId,
    control: SyncControl,
    permit: Mutex<Option<BudgetPermit>>,
}

/// Why a capacity wait ended without a go-ahead.
///
/// The distinction is load-bearing: `Refused` is the scheduler's admission lane
/// being momentarily full, which every repeating work path in this engine treats
/// as transient, while `ShuttingDown` means the account is going away. Collapsing
/// them into one "no" retired the whole backfill orchestrator on a burst of
/// scheduler pressure, abandoning every later scope and every later rescan for
/// the life of the attachment.
#[derive(Debug)]
#[non_exhaustive]
pub enum WaitFailed {
    /// The slot is shutting down. Stop.
    ShuttingDown,
    /// The scheduler refused re-admission. Transient; retry at the next tick.
    Refused(Error),
}

impl BackfillAdmission {
    fn new(scheduler: Scheduler, account_id: AccountId, control: SyncControl) -> Self {
        Self {
            scheduler,
            account_id,
            control,
            permit: Mutex::new(None),
        }
    }

    async fn acquire(&self, shutdown: &CancellationToken) -> Result<(), WaitFailed> {
        let admitted = tokio::select! {
            () = shutdown.cancelled() => return Err(WaitFailed::ShuttingDown),
            permit = self.scheduler.admit(
                self.account_id.clone(),
                self.control.priority_snapshot(),
                WorkKind::Backfill,
            ) => permit,
        };
        match admitted {
            Ok(permit) => {
                *self
                    .permit
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(permit);
                Ok(())
            }
            Err(error) => Err(WaitFailed::Refused(error)),
        }
    }

    fn release(&self) {
        let taken = self
            .permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(taken);
    }

    /// Whether the account's sync budget is currently held. Observability.
    #[must_use]
    pub fn is_held(&self) -> bool {
        self.permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }
}

/// The producer's door onto the bound: the account's publication ledger (which
/// IS the capacity ledger), the token a park must be able to end through, and
/// the account's scheduler admission.
#[derive(Debug, Clone)]
pub struct LaneGate {
    coverage: Arc<PendingCoverage>,
    capacity: usize,
    shutdown: CancellationToken,
    admission: Arc<BackfillAdmission>,
    /// When this gate last warned about a park. Shared across clones, because
    /// the rate limit is about the ACCOUNT's log and a gate is cloned per
    /// partition pass.
    park_warned_at: Arc<Mutex<Option<tokio::time::Instant>>>,
}

impl LaneGate {
    #[must_use]
    pub fn new(
        coverage: Arc<PendingCoverage>,
        capacity: usize,
        shutdown: CancellationToken,
        scheduler: Scheduler,
        account_id: AccountId,
        control: SyncControl,
    ) -> Self {
        Self {
            coverage,
            // A zero-capacity lane is a permanently parked cold start, and a
            // misconfigured knob must not be able to produce one.
            capacity: capacity.max(1),
            shutdown,
            admission: Arc::new(BackfillAdmission::new(scheduler, account_id, control)),
            park_warned_at: Arc::new(Mutex::new(None)),
        }
    }

    #[must_use]
    pub fn admission(&self) -> &Arc<BackfillAdmission> {
        &self.admission
    }

    #[must_use]
    pub fn coverage(&self) -> &Arc<PendingCoverage> {
        &self.coverage
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The account's shutdown token, for arming a provider-stream poll.
    ///
    /// Crate-internal on purpose. It is here so `BackfillRunner::run_partition`
    /// can select its `inventory_partition_stream` poll against cancellation
    /// without taking a token parameter that only the engine could supply;
    /// every other wait in this gate already selects the same token. That makes
    /// the cancellation guarantee a property of ENGINE-DRIVEN partition walks
    /// rather than of `run_partition` itself, since a caller passing
    /// `lane: None` has no token to be armed against.
    #[must_use]
    pub(crate) fn shutdown(&self) -> &CancellationToken {
        &self.shutdown
    }

    /// Take the account's sync admission for a partition pass.
    pub async fn admit(&self) -> Result<(), WaitFailed> {
        self.admission.acquire(&self.shutdown).await
    }

    /// Give the account's sync admission back.
    pub fn release_admission(&self) {
        self.admission.release();
    }

    /// Wait for room to publish another backfill page, WITHOUT touching the
    /// account's scheduler admission.
    ///
    /// For a publication that does no wire work: the completion sentinel is a
    /// synthetic empty batch, so it neither needs a budget permit nor may be
    /// allowed to acquire one it will not release. The sentinel is emitted from
    /// the orchestrator after the partition pass has already handed its
    /// admission back, and a version of this that re-acquired admission on the
    /// wake left that permit held for the rest of the attachment - starving
    /// polling, push reconciliation and the next scope's barrier query on a
    /// one-permit budget.
    pub async fn wait_for_capacity(&self) -> Result<(), WaitFailed> {
        let in_flight = self.coverage.backfill_in_flight();
        if in_flight >= self.capacity {
            self.warn_parked(in_flight);
        }
        tokio::select! {
            () = self.shutdown.cancelled() => Err(WaitFailed::ShuttingDown),
            () = self.coverage.await_backfill_capacity(self.capacity) => Ok(()),
        }
    }

    /// Wait for room to publish another backfill page while a partition pass is
    /// in progress, giving up the account's scheduler admission for the duration
    /// of any actual wait.
    ///
    /// The fast path is non-blocking and keeps the permit, so an unbounded
    /// account pays nothing. Only a genuine park hands the budget back, and the
    /// re-acquisition after the wake reports a refusal as [`WaitFailed::Refused`]
    /// rather than as shutdown.
    pub async fn wait_for_capacity_holding_admission(&self) -> Result<(), WaitFailed> {
        if self.coverage.backfill_in_flight() < self.capacity {
            return Ok(());
        }
        self.release_admission();
        self.wait_for_capacity().await?;
        self.admit().await
    }

    /// Name the stall, at most once per [`PARK_WARN_INTERVAL`].
    ///
    /// A numbered receiver that is held but never polled parks cold start for the
    /// life of the attachment and reports nothing on its own - it never drops, so
    /// no departure warning, and it never polls, so no lag warning. The receiver
    /// SEQUENCES are the diagnosable part: they say which of the consumer's
    /// receivers is the one that stopped reading, which "backfill is stuck" alone
    /// does not.
    fn warn_parked(&self, in_flight: usize) {
        let now = tokio::time::Instant::now();
        {
            let mut last = self
                .park_warned_at
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(previous) = *last
                && now.duration_since(previous) < PARK_WARN_INTERVAL
            {
                return;
            }
            *last = Some(now);
        }
        let (holders, scopes) = self.coverage.backfill_charge_holders();
        tracing::warn!(
            target: "bifrost.sync.backfill",
            account = %self.admission.account_id.0,
            capacity = self.capacity,
            in_flight,
            holders = ?holders,
            scopes = ?scopes,
            "backfill parked on the lane bound; every numbered change-stream receiver \
             must be polled or dropped"
        );
    }

    /// Wake anything parked on the bound: the gate-side spelling of
    /// [`crate::cursor::PendingCoverage::wake_capacity`].
    ///
    /// NO in-engine caller, and the doc used to claim one. `detach` does pulse
    /// that wake beside the shutdown cancel - so a producer parked at the bound
    /// leaves at once rather than being awaited to `detach_timeout` and aborted
    /// mid-partition - but it does so through the slot's `PendingCoverage`
    /// directly, because teardown holds the coverage and has no gate. Nothing
    /// here is dead in the sense of unreachable: it is published, so a consumer
    /// holding a `LaneGate` can reach the same pulse without reaching for the
    /// ledger. Recorded rather than removed: deleting a published item is not a
    /// call this makes on its own judgement, and an audit finding it uncalled has
    /// found the intended state. Same standing as
    /// [`crate::control::SyncControl::record_checkpoint`].
    pub fn wake(&self) {
        self.coverage.wake_capacity();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor::CoverageClaim;
    use crate::engine::backfill::MarkerOutcome;
    use crate::scheduler::{BudgetGate, ConcurrencyBudget};
    use bifrost_types::{
        BackfillCheckpoint, BackfillProgress, Checkpoint, CursorScope, ObjectType, Partition,
        Priority,
    };

    fn scheduler(per_account: usize, global: usize) -> Scheduler {
        Scheduler::new(
            crate::types::SchedulerConfig::default(),
            BudgetGate::new(ConcurrencyBudget {
                per_account,
                global,
                mutation_share_num: 1,
                mutation_share_den: 4,
            }),
        )
    }

    fn control(account: &AccountId) -> SyncControl {
        let (boundary, view) = crate::cancel::Boundary::new();
        let (priority, p) = tokio::sync::watch::channel(Priority::Normal);
        let (bandwidth, b) = tokio::sync::watch::channel(None);
        std::mem::forget((view, p, b));
        SyncControl::new(account.clone(), boundary, priority, bandwidth)
    }

    struct Harness {
        coverage: Arc<PendingCoverage>,
        gate: LaneGate,
        shutdown: CancellationToken,
        account: AccountId,
        scheduler: Scheduler,
    }

    fn harness(capacity: usize) -> Harness {
        harness_with_budget(capacity, 8, 64)
    }

    fn harness_with_budget(capacity: usize, per_account: usize, global: usize) -> Harness {
        let coverage = Arc::new(PendingCoverage::new());
        let shutdown = CancellationToken::new();
        let account = AccountId("lane-unit".to_owned());
        let scheduler = scheduler(per_account, global);
        scheduler.budget().register(account.clone());
        let gate = LaneGate::new(
            Arc::clone(&coverage),
            capacity,
            shutdown.clone(),
            scheduler.clone(),
            account.clone(),
            control(&account),
        );
        Harness {
            coverage,
            gate,
            shutdown,
            account,
            scheduler,
        }
    }

    /// Poll a future exactly once and report whether it is still pending.
    ///
    /// A single poll, not a wall-clock wait: `Duration::ZERO` makes the timer arm
    /// ready immediately, and `Timeout` polls the value first, so an `Err` means
    /// "it was pending when polled". Deterministic, and the project's testing
    /// rules forbid the sleep this replaces.
    async fn still_parked<F>(fut: std::pin::Pin<&mut F>) -> bool
    where
        F: std::future::Future,
    {
        tokio::time::timeout(std::time::Duration::ZERO, fut)
            .await
            .is_err()
    }

    fn scope_a() -> CursorScope {
        CursorScope::Account
    }

    fn scope_b() -> CursorScope {
        CursorScope::Type(ObjectType::Email)
    }

    fn page(scope: &CursorScope, partition: &str, n: u64) -> Checkpoint {
        Checkpoint::Backfill(BackfillCheckpoint {
            scope: scope.clone(),
            partition: Partition(partition.as_bytes().to_vec()),
            progress_marker: None,
            progress: BackfillProgress {
                items_done: n,
                items_estimated: None,
            },
            envelope_version: 1,
        })
    }

    /// A control whose publication ledger IS the harness's coverage, as `attach`
    /// wires it. A control with its own ledger would register the marker's
    /// publication somewhere the bound cannot see.
    fn control_over(account: &AccountId, coverage: Arc<PendingCoverage>) -> SyncControl {
        let (boundary, view) = crate::cancel::Boundary::new();
        let (priority, p) = tokio::sync::watch::channel(Priority::Normal);
        let (bandwidth, b) = tokio::sync::watch::channel(None);
        std::mem::forget((view, p, b));
        SyncControl::new_with_publications(account.clone(), boundary, priority, bandwidth, coverage)
    }

    /// The lane harness plus the pieces `emit_backfill_complete` needs: a real
    /// broadcast channel with a retained receiver, so "was the marker actually
    /// published" is observable rather than inferred.
    struct MarkerHarness {
        lane: Harness,
        control: SyncControl,
        delivery: crate::multiplexer::ChangeDelivery,
        tx: tokio::sync::broadcast::Sender<crate::multiplexer::MultiplexerEvent>,
        rx: std::sync::Mutex<
            tokio::sync::broadcast::Receiver<crate::multiplexer::MultiplexerEvent>,
        >,
    }

    fn marker_harness(capacity: usize) -> MarkerHarness {
        let coverage = Arc::new(PendingCoverage::new());
        let shutdown = CancellationToken::new();
        let account = AccountId("lane-marker".to_owned());
        let scheduler = scheduler(8, 64);
        scheduler.budget().register(account.clone());
        let control = control_over(&account, Arc::clone(&coverage));
        let gate = LaneGate::new(
            Arc::clone(&coverage),
            capacity,
            shutdown.clone(),
            scheduler.clone(),
            account.clone(),
            control.clone(),
        );
        let (tx, rx) = tokio::sync::broadcast::channel(16);
        MarkerHarness {
            lane: Harness {
                coverage,
                gate,
                shutdown,
                account,
                scheduler,
            },
            control,
            delivery: crate::multiplexer::ChangeDelivery::new(tx.clone()),
            tx,
            rx: std::sync::Mutex::new(rx),
        }
    }

    impl MarkerHarness {
        /// The production emission, exactly as the orchestrator calls it.
        fn emit_marker<'a>(
            &'a self,
            scope: &'a CursorScope,
            undelivered_before: u64,
        ) -> impl std::future::Future<Output = MarkerOutcome> + 'a {
            let fence = self.lane.coverage.scope_fence(scope);
            self.emit_marker_for_walk(scope, undelivered_before, fence)
        }

        /// The production emission, exactly as the orchestrator calls it -
        /// including the fence the WALK started under, which is not necessarily
        /// the fence at the moment of emission.
        fn emit_marker_for_walk<'a>(
            &'a self,
            scope: &'a CursorScope,
            undelivered_before: u64,
            fence_at_walk_start: u64,
        ) -> impl std::future::Future<Output = MarkerOutcome> + 'a {
            crate::engine::backfill::emit_backfill_complete(
                Some(&self.tx),
                scope,
                0,
                &self.control,
                &self.lane.shutdown,
                &self.lane.gate,
                &self.delivery,
                undelivered_before,
                fence_at_walk_start,
            )
        }

        /// How many events reached the retained receiver.
        fn received(&self) -> usize {
            let mut rx = self.rx.lock().expect("receiver lock");
            let mut seen = 0;
            while rx.try_recv().is_ok() {
                seen += 1;
            }
            seen
        }
    }

    impl Harness {
        /// Register and mark delivered, the order a producer uses.
        fn publish(&self, checkpoint: &Checkpoint, seq: u64) -> crate::cursor::PublicationId {
            let id = self.coverage.register(
                checkpoint.clone(),
                CoverageClaim {
                    reports: Vec::new(),
                    generation: 0,
                },
            );
            self.coverage.mark_delivered(&id, seq);
            id
        }
    }

    /// The bound binds ACROSS supersession. This is the one place the "capacity
    /// is the live record" rule needed a correction, so it is the first thing to
    /// pin: pages of one partition supersede each other in the ledger, and if
    /// that released capacity a single partition could publish for ever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn supersession_within_a_partition_does_not_release_capacity() {
        let h = harness(3);
        for n in 1..=3 {
            h.publish(&page(&scope_a(), "page:0:30", n), 1);
        }
        assert_eq!(
            h.coverage.backfill_in_flight(),
            3,
            "three unanswered pages, even though the ledger holds one record"
        );
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                h.gate.wait_for_capacity()
            )
            .await
            .is_err(),
            "the producer must park at the bound"
        );
    }

    /// And acknowledging the survivor settles everything it subsumed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn acknowledging_the_survivor_settles_the_pages_it_subsumed() {
        let h = harness(3);
        let mut ids = Vec::new();
        for n in 1..=3 {
            ids.push(h.publish(&page(&scope_a(), "page:0:30", n), 1));
        }
        h.coverage.acknowledge_publication(ids[2].clone());
        assert_eq!(h.coverage.backfill_in_flight(), 0);
        assert!(h.gate.wait_for_capacity().await.is_ok());
    }

    /// A consumer may legitimately acknowledge a batch a later publication has
    /// superseded - it really received it. That settles exactly one page.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn acknowledging_a_superseded_page_settles_exactly_that_page() {
        let h = harness(4);
        let mut ids = Vec::new();
        for n in 1..=3 {
            ids.push(h.publish(&page(&scope_a(), "page:0:30", n), 1));
        }
        h.coverage.acknowledge_publication(ids[0].clone());
        assert_eq!(h.coverage.backfill_in_flight(), 2);
        h.coverage.acknowledge_publication(ids[0].clone());
        assert_eq!(
            h.coverage.backfill_in_flight(),
            2,
            "a repeated acknowledgement must not manufacture capacity"
        );
    }

    /// FINDING 6. A repeated acknowledgement of an OLD publication must not
    /// release a later walk's capacity. There is no scope-wide release to abuse:
    /// settlement is by publication identity, so an id the ledger no longer
    /// holds settles nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_repeated_stale_acknowledgement_releases_nothing() {
        let h = harness(4);
        let sentinel = page(&scope_a(), "complete", 1);
        let stale = h.publish(&sentinel, 1);
        h.coverage.acknowledge_publication(stale.clone());
        assert_eq!(h.coverage.backfill_in_flight(), 0);

        // A later walk of the same scope.
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        h.publish(&page(&scope_a(), "page:10:20", 1), 1);
        assert_eq!(h.coverage.backfill_in_flight(), 2);

        for _ in 0..8 {
            h.coverage.acknowledge_publication(stale.clone());
        }
        assert_eq!(
            h.coverage.backfill_in_flight(),
            2,
            "replaying an old sentinel acknowledgement must not defeat the bound"
        );
    }

    /// Sibling partitions are in flight together and neither subsumes the other,
    /// so each holds its own charge and settling one leaves the other.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sibling_partitions_each_hold_their_own_capacity() {
        let h = harness(4);
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        let y = h.publish(&page(&scope_a(), "page:10:20", 1), 1);
        h.publish(&page(&scope_b(), "page:0:10", 1), 1);
        assert_eq!(h.coverage.backfill_in_flight(), 3);

        h.coverage.acknowledge_publication(y);
        assert_eq!(h.coverage.backfill_in_flight(), 2);
    }

    fn live_cursor(state: &[u8]) -> Checkpoint {
        Checkpoint::Change(bifrost_types::ChangeCursor {
            scope: scope_a(),
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Imap,
                envelope_version: 1,
                bytes: state.to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        })
    }

    /// A live change publication is not backfill and must never consume the
    /// bound - the fairness rule, stated where it is enforced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_live_change_publication_does_not_consume_the_bound() {
        let h = harness(1);
        h.publish(&live_cursor(b"live"), 1);
        assert_eq!(h.coverage.backfill_in_flight(), 0);
        assert!(h.gate.wait_for_capacity().await.is_ok());
    }

    /// FINDING 1. The live lane supersedes too, and it never consults the bound,
    /// so inheriting the capacity charge there would make a subscriber that
    /// drains without acknowledging grow one entry's history without limit -
    /// every retained id holding a `PublicationReceipt` and its whole coverage
    /// claim. Only backfill has a bound to keep honest, so only backfill keeps
    /// the history.
    ///
    /// Bites on the SIZE of what the ledger retains, which is the actual defect;
    /// asserting `backfill_in_flight() == 0` alone would pass against it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_live_lane_retains_no_publication_history() {
        let h = harness(4);
        for round in 0..64u32 {
            h.publish(&live_cursor(&round.to_be_bytes()), 1);
        }
        assert_eq!(h.coverage.backfill_in_flight(), 0);
        assert_eq!(
            h.coverage.pending_checkpoints(),
            1,
            "one live lane, one outstanding registration"
        );
        assert_eq!(
            h.coverage.retained_history(),
            0,
            "and it must be carrying no superseded ids at all; a live subscriber that \
             drains without acknowledging would otherwise grow this for ever"
        );

        // The backfill lane is the one that does keep a history, so the assertion
        // above is about the LANE and not about the mechanism being absent.
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        h.publish(&page(&scope_a(), "page:0:10", 2), 1);
        assert_eq!(h.coverage.retained_history(), 1);
    }

    /// FINDING 2. Retiring a page a later publication has since superseded must
    /// take its charge off the survivor. Searching only `entry.id` left it
    /// charged for ever - and a failed store write on a multi-page partition is
    /// exactly how that happens in production.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retiring_a_superseded_page_frees_its_capacity() {
        let h = harness(4);
        let first = h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        h.publish(&page(&scope_a(), "page:0:10", 2), 1);
        assert_eq!(h.coverage.backfill_in_flight(), 2);

        h.coverage.retire_publication(first);
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "the superseded page's charge lives on the survivor, so that is where \
             retiring it has to come off"
        );
    }

    /// An abandonment records the pages it drains as undelivered - but only the
    /// ones that were SENT.
    ///
    /// The consumer will never answer for a page abandoned out from under it, so
    /// the walk that published it is holed and must not settle. A page that was
    /// merely REGISTERED is a different thing entirely: the producer broadcasts
    /// it moments later, the consumer receives and acknowledges it, and counting
    /// it here would judge a walk holed over a page that arrived intact. That is
    /// the same care `release_undelivered` takes with an unsent stamp, and for
    /// the same reason.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_abandonment_records_only_the_pages_it_had_sent() {
        let h = harness(4);
        let before = h.coverage.undelivered_watermark(&scope_a());

        // One page broadcast to receiver 0, one registered and not yet sent.
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        h.coverage.register(
            page(&scope_a(), "page:10:20", 1),
            CoverageClaim {
                reports: Vec::new(),
                generation: 0,
            },
        );
        assert_eq!(h.coverage.backfill_in_flight(), 2);

        assert_eq!(h.coverage.abandon_checkpoints(), 2, "both are drained");
        assert_eq!(
            h.coverage.undelivered_watermark(&scope_a()),
            before + 1,
            "and exactly the one that had been sent is recorded as reaching nobody; \
             charging the unsent page too judges the walk holed over a page the \
             consumer is about to receive and acknowledge"
        );
    }

    /// A discard request belongs to ONE scope, and only that scope's walk may
    /// take it.
    ///
    /// The walk-end drain used to take every outstanding request. A loss recorded
    /// against scope A after A's own walk ended is already repaired - the
    /// ack-time refusal withholds A's marker and the pre-retry discard drops its
    /// rows - but nothing cleared the request, so the next lossy walk of ANY
    /// scope carried it off and acted on it arbitrarily later, deleting a
    /// completion A had since re-earned and refusing the consumer's
    /// acknowledgement of it. Every producer of a request has a scope; so does
    /// every consumer of one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_walks_drain_takes_only_its_own_scopes_discard_request() {
        let h = harness(4);
        h.coverage.note_undelivered(&scope_a(), 1);
        h.coverage.note_undelivered(&scope_b(), 1);

        assert!(
            h.coverage.take_backfill_discard(&scope_b()),
            "scope B's walk takes B's request"
        );
        assert!(
            !h.coverage.take_backfill_discard(&scope_b()),
            "and taking is draining: the same request is not answered twice"
        );
        assert!(
            h.coverage.take_backfill_discard(&scope_a()),
            "A's request is still A's to answer - B's walk end must not have carried \
             it off and discarded rows A may since have re-earned"
        );
    }

    /// And it judges each SUBSUMED page by its own stamp, not by the survivor's.
    ///
    /// A survivor can be unsent while the pages folded into it were delivered -
    /// publish page A to the consumer, then register page B on the same partition
    /// before it goes out - and gating the whole entry on the survivor's stamp
    /// then records nothing at all. The page the consumer really lost to the lag
    /// goes unrecorded, and the walk's completion marker becomes durable over it.
    /// `release_undelivered` asks the question once per page for this reason;
    /// this path now does too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_abandonment_judges_each_subsumed_page_by_its_own_stamp() {
        let h = harness(4);
        let before = h.coverage.undelivered_watermark(&scope_a());

        // One page of a partition, sent; then a second on the SAME partition,
        // which supersedes it and has not been sent yet.
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        h.coverage.register(
            page(&scope_a(), "page:0:10", 2),
            CoverageClaim {
                reports: Vec::new(),
                generation: 0,
            },
        );
        assert_eq!(
            h.coverage.retained_history(),
            1,
            "the survivor carries the sent page it superseded"
        );

        assert_eq!(h.coverage.abandon_checkpoints(), 2);
        assert_eq!(
            h.coverage.undelivered_watermark(&scope_a()),
            before + 1,
            "the SUBSUMED page was delivered and is lost with the abandonment; the \
             unsent survivor is not. Reading the survivor's stamp for both records \
             nothing and lets the marker stand over a page the consumer lost"
        );
    }

    /// Retirement, abandonment and scope invalidation each free capacity because
    /// they already remove the record. No release call site to forget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_ledger_retirement_frees_capacity() {
        let h = harness(4);
        let retired = h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        h.coverage.retire_publication(retired);
        assert_eq!(h.coverage.backfill_in_flight(), 0);

        h.publish(&page(&scope_a(), "page:10:20", 1), 1);
        assert_eq!(h.coverage.abandon_checkpoints(), 1);
        assert_eq!(h.coverage.backfill_in_flight(), 0);

        // Two pages of ONE partition, so the survivor carries a subsumed page:
        // the shape the reset used to lose track of.
        h.publish(&page(&scope_a(), "page:20:30", 1), 1);
        h.publish(&page(&scope_a(), "page:20:30", 2), 1);
        h.publish(&page(&scope_b(), "page:0:10", 1), 1);
        assert_eq!(h.coverage.backfill_in_flight(), 3);
        let _ = h.coverage.invalidate_scope(&scope_a());
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "invalidation frees the subsumed page too; only the sibling scope remains"
        );
    }

    /// FINDING 4. Receiver A holds pages, B subscribes afterwards, A drops. The
    /// count never reaches zero and B started beyond those pages, so nothing can
    /// ever acknowledge them - and subscriber COUNT cannot see it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pages_no_live_receiver_can_reach_are_released() {
        let h = harness(2);
        // Published when the account had handed out exactly one receiver (A).
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        h.publish(&page(&scope_a(), "page:10:20", 1), 1);
        assert_eq!(h.coverage.backfill_in_flight(), 2);

        // B is receiver index 1; A (index 0) is gone. The lowest live sequence
        // is 1, which is not below the sequence those pages were sent at.
        assert_eq!(h.coverage.release_undelivered(Some(1)), 2);
        assert_eq!(h.coverage.backfill_in_flight(), 0);
    }

    /// The mirror. A receiver that existed when the page went out can still
    /// acknowledge it, so the bound must stay in force - otherwise it lapses the
    /// moment any second consumer appears.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_receiver_that_predates_the_page_keeps_it_charged() {
        let h = harness(2);
        // Sent when two receivers had been handed out; receiver 0 is still live.
        h.publish(&page(&scope_a(), "page:0:10", 1), 2);
        assert_eq!(h.coverage.release_undelivered(Some(0)), 0);
        assert_eq!(h.coverage.backfill_in_flight(), 1);
    }

    /// FINDING 7. A page registered but NOT YET SENT must never be judged
    /// undeliverable: a receiver subscribing in that window really does receive
    /// it. `delivered_at` is `None` until the send, and that is what the sweep
    /// keys on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_page_not_yet_sent_is_never_swept() {
        let h = harness(2);
        let id = h.coverage.register(
            page(&scope_a(), "page:0:10", 1),
            CoverageClaim {
                reports: Vec::new(),
                generation: 0,
            },
        );
        assert_eq!(
            h.coverage.release_undelivered(None),
            0,
            "an unsent page is not evidence of anything yet"
        );
        assert_eq!(h.coverage.backfill_in_flight(), 1);

        // The replacement subscribes, THEN the send happens and records the
        // sequence that includes it.
        h.coverage.mark_delivered(&id, 2);
        assert_eq!(h.coverage.release_undelivered(Some(1)), 0);
        assert_eq!(h.coverage.backfill_in_flight(), 1);
    }

    /// FINDING 1 and 5, the sentinel's two hazards. It does no wire work, so it
    /// waits without ever taking admission - there is nothing to leak and
    /// nothing to be refused.
    ///
    /// Deterministic: the wait is polled in place rather than raced against a
    /// timer, so "it parked" is observed as a `Pending` poll and not inferred
    /// from a sleep.
    #[tokio::test(start_paused = true)]
    async fn the_admission_free_wait_never_acquires_a_budget_permit() {
        let h = harness_with_budget(2, 2, 1);
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        let id = h.publish(&page(&scope_a(), "page:10:20", 1), 1);

        let wait = h.gate.wait_for_capacity();
        tokio::pin!(wait);
        assert!(
            still_parked(wait.as_mut()).await,
            "the wait must park at the bound"
        );
        assert!(!h.gate.admission().is_held());

        h.coverage.acknowledge_publication(id);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), wait)
                .await
                .expect("the wake must arrive")
                .is_ok()
        );
        assert!(
            !h.gate.admission().is_held(),
            "an admission-free wait must not leave the account's sync permit held; \
             the sentinel is emitted after the partition pass gave it back, and holding \
             it starves polling for the rest of the attachment"
        );
    }

    /// FINDING 3 of the previous round, kept: a producer parked mid-partition
    /// must give the budget back, or the live lane starves on a minimal budget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_parked_producer_gives_up_the_accounts_sync_admission() {
        let h = harness_with_budget(1, 2, 2);
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);

        h.gate.admit().await.expect("admission");
        assert!(h.gate.admission().is_held());
        let gate = h.gate.clone();
        let parked = tokio::spawn(async move { gate.wait_for_capacity_holding_admission().await });

        let released = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while h.gate.admission().is_held() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            released.is_ok(),
            "a producer parked on the bound must not hold the account's sync budget"
        );
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                h.scheduler
                    .admit(h.account.clone(), Priority::Normal, WorkKind::Sync)
            )
            .await
            .is_ok_and(|permit| permit.is_ok()),
            "polling must be admissible while cold start is parked"
        );

        h.shutdown.cancel();
        assert!(matches!(
            parked.await.expect("task"),
            Err(WaitFailed::ShuttingDown)
        ));
    }

    /// FINDING 5, exercised through the real mid-partition wait rather than
    /// through a bare `admit()`.
    ///
    /// The producer parks at the bound holding admission, gives the budget up,
    /// the bound is then freed, and the RE-ADMISSION on the way out is refused
    /// because the scheduler's lane is full. That refusal must be reported as
    /// transient: the orchestrator retires itself on shutdown, so collapsing the
    /// two abandons every later scope and every later rescan on a burst of
    /// scheduler pressure. The runner turns `Refused` into a partition error,
    /// which leaves the scope `Pending` for the rescan ramp.
    ///
    /// Deterministic throughout - every step is observed by polling the wait in
    /// place, never by waiting out a duration.
    #[tokio::test(start_paused = true)]
    async fn a_refused_readmission_is_transient_not_shutdown() {
        // One global permit and one admission-lane slot.
        let scheduler = Scheduler::with_lane_capacity(
            crate::types::SchedulerConfig::default(),
            BudgetGate::new(ConcurrencyBudget {
                per_account: 2,
                global: 1,
                mutation_share_num: 1,
                mutation_share_den: 4,
            }),
            1,
        );
        let coverage = Arc::new(PendingCoverage::new());
        let gate_for = |name: &str, coverage: Arc<PendingCoverage>| {
            let account = AccountId(name.to_owned());
            scheduler.budget().register(account.clone());
            LaneGate::new(
                coverage,
                1,
                CancellationToken::new(),
                scheduler.clone(),
                account.clone(),
                control(&account),
            )
        };

        let producer = gate_for("producer", Arc::clone(&coverage));
        producer
            .admit()
            .await
            .expect("the first admission succeeds");
        assert!(producer.admission().is_held());

        // Fill the bound, then park the mid-partition wait.
        let id = coverage.register(
            page(&scope_a(), "page:0:10", 1),
            CoverageClaim {
                reports: Vec::new(),
                generation: 0,
            },
        );
        coverage.mark_delivered(&id, 1);
        let wait = producer.wait_for_capacity_holding_admission();
        tokio::pin!(wait);
        assert!(still_parked(wait.as_mut()).await, "parked at the bound");
        assert!(
            !producer.admission().is_held(),
            "and it gave the account's sync budget back before parking"
        );

        // While it is parked, the scheduler fills up: one holder takes the only
        // global permit and one waiter occupies the only lane slot.
        let holder = gate_for("holder", Arc::new(PendingCoverage::new()));
        holder.admit().await.expect("the permit is free again");
        let queued = gate_for("queued", Arc::new(PendingCoverage::new()));
        let queued_admit = queued.admit();
        tokio::pin!(queued_admit);
        assert!(
            still_parked(queued_admit.as_mut()).await,
            "the second admission occupies the lane"
        );

        // Now free the bound. The producer wakes and finds no room to be
        // re-admitted.
        coverage.acknowledge_publication(id);
        assert!(
            matches!(
                tokio::time::timeout(std::time::Duration::from_secs(5), wait)
                    .await
                    .expect("the capacity wake must arrive"),
                Err(WaitFailed::Refused(_))
            ),
            "a full admission lane on the way out of a park is a refusal, never a \
             shutdown"
        );

        holder.release_admission();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), queued_admit).await;
    }

    /// Detach must not have to wait out `detach_timeout` on a parked producer.
    #[tokio::test(start_paused = true)]
    async fn shutdown_ends_a_park_at_once() {
        let h = harness(1);
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        let wait = h.gate.wait_for_capacity();
        tokio::pin!(wait);
        assert!(still_parked(wait.as_mut()).await);

        h.shutdown.cancel();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(5), wait)
                .await
                .expect("shutdown must cut the park short"),
            Err(WaitFailed::ShuttingDown)
        ));
    }

    /// FINDING 5. The boundary-registration cap can evict a CHARGED backfill
    /// entry, which frees capacity - and anything parked on the bound has to be
    /// woken, exactly as every other lightening path wakes it. Reached only
    /// through an unkeyed `Checkpoint` variant, which is the sort of rarely-taken
    /// arm where a missing wake sits undiscovered.
    #[tokio::test(start_paused = true)]
    async fn a_capacity_freeing_eviction_wakes_a_parked_producer() {
        let h = harness(1);
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);

        let wait = h.gate.wait_for_capacity();
        tokio::pin!(wait);
        assert!(still_parked(wait.as_mut()).await, "parked at the bound");

        // Fill the boundary registry past its cap with publications on DISTINCT
        // change lanes - so none of them supersedes another, none of them is
        // charged, and the entry the cap evicts is the oldest: the backfill page.
        for round in 0..=crate::cursor::coverage::PENDING_BOUNDARY_CAP {
            let scope = CursorScope::Folder(bifrost_types::FolderId(format!("f{round}")));
            h.coverage.register(
                Checkpoint::Change(bifrost_types::ChangeCursor {
                    scope,
                    server_state: bifrost_types::OpaqueChangeState {
                        protocol: bifrost_types::ProtocolKind::Imap,
                        envelope_version: 1,
                        bytes: Vec::new(),
                    },
                    advanced_through: None,
                    envelope_version: 1,
                }),
                CoverageClaim {
                    reports: Vec::new(),
                    generation: 0,
                },
            );
        }

        assert_eq!(
            h.coverage.backfill_in_flight(),
            0,
            "the charged entry was evicted, so the capacity is back"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), wait)
                .await
                .expect("the eviction must wake the producer parked on the bound")
                .is_ok()
        );
    }

    /// FINDING 1. "Is somebody subscribed at the end" is not the question; "did
    /// every page of this walk reach somebody" is, and only a WATERMARK can
    /// answer it.
    ///
    /// A consumer that fills the bound and leaves, is replaced before the walk
    /// finishes, and whose replacement acknowledges the rest satisfies every
    /// point-in-time check while the pages published in the gap reached nobody.
    /// The orchestrator compares this watermark across the walk for exactly that
    /// reason.
    #[tokio::test(start_paused = true)]
    async fn the_undelivered_watermark_records_pages_that_reached_nobody() {
        let h = harness(4);
        let before = h.coverage.undelivered_watermark(&scope_a());

        // Two pages delivered to receiver 0, which then goes away: the sweep
        // retires them and records the loss.
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        h.publish(&page(&scope_a(), "page:10:20", 1), 1);
        assert_eq!(h.coverage.release_undelivered(None), 2);
        let after_sweep = h.coverage.undelivered_watermark(&scope_a());
        assert!(
            after_sweep > before,
            "a sweep that retires pages nobody can read must record it"
        );

        // A page the producer sends while nobody is listening records it too -
        // that is the runner's retire-on-sentinel-only path.
        h.coverage.note_undelivered(&scope_a(), 1);
        assert!(h.coverage.undelivered_watermark(&scope_a()) > after_sweep);

        // And it is MONOTONIC: a consumer arriving afterwards cannot erase the
        // record, which is the whole point. A subscriber count would read as
        // healthy the instant one appears.
        assert!(h.coverage.undelivered_watermark(&scope_a()) > before);
    }

    /// And the record is PER SCOPE. An account-wide counter would let a page of
    /// one scope that reached nobody make another scope's in-flight walk look
    /// holed - so a walk that delivered every one of its own pages would have its
    /// completion marker withheld and be sent round again, for ever, for as long
    /// as any sibling scope kept losing pages.
    ///
    /// Both routes into the counter are checked, because both take a scope and
    /// either could take the wrong one: the departure sweep, which attributes each
    /// retired entry through its own lane, and the direct
    /// retire-on-sentinel-only note.
    #[tokio::test(start_paused = true)]
    async fn the_undelivered_watermark_is_recorded_against_one_scope_only() {
        let h = harness(4);
        let b_before = h.coverage.undelivered_watermark(&scope_b());

        // A page of scope A, published to a receiver that then departs.
        h.publish(&page(&scope_a(), "page:0:10", 1), 1);
        assert_eq!(h.coverage.release_undelivered(None), 1);
        assert!(
            h.coverage.undelivered_watermark(&scope_a()) > 0,
            "the sweep must record against the scope whose page it retired"
        );
        assert_eq!(
            h.coverage.undelivered_watermark(&scope_b()),
            b_before,
            "and against no other: a sibling scope's walk delivered everything and \
             must not be judged holed for it"
        );

        // Same for the publish path's direct note.
        h.coverage.note_undelivered(&scope_a(), 1);
        assert_eq!(
            h.coverage.undelivered_watermark(&scope_b()),
            b_before,
            "the retire-on-sentinel-only path is per scope too"
        );
        // And scope B's own losses are still recorded, on B.
        h.coverage.note_undelivered(&scope_b(), 1);
        assert_eq!(h.coverage.undelivered_watermark(&scope_b()), b_before + 1);
    }

    /// FINDING 5. A page that parks on the bound and resumes AFTER its scope was
    /// reset must be abandoned, not published.
    ///
    /// The reset deletes the scope's rows and fences every publication minted
    /// before it - but a page still waiting has not been minted, so it comes back
    /// with a fresh id ABOVE the fence and acknowledges normally, recreating the
    /// durable state the reset dropped. A stale completion marker arriving that
    /// way is worse still: it suppresses the replacement incarnation's whole
    /// inventory walk.
    ///
    /// The fence is snapshotted before the wait and re-read after. Driven
    /// through the REAL guard - `emit_backfill_complete` - rather than through a
    /// bare `wait_for_capacity` plus an assertion that the fence moved: the
    /// earlier shape observed only that `PendingCoverage` records a reset, which
    /// is true with both of the production revalidations deleted, so it passed
    /// against the bug it was written for.
    ///
    /// The second half is the sixth review's finding, and it is why this test
    /// asserts on WHICH outcome comes back rather than merely on "not published".
    /// A withheld marker and a shutting-down account used to be one `false`, so a
    /// reset landing inside one marker's capacity wait returned the caller out of
    /// `run_backfill_orchestrator` entirely, abandoning every later scope and
    /// every later rescan until the account was reattached.
    #[tokio::test(start_paused = true)]
    async fn a_reset_inside_the_markers_wait_withholds_it_without_ending_the_orchestrator() {
        let h = marker_harness(1);
        let scope = scope_a();
        // One page holds the whole bound, so the marker's wait is a real one.
        h.lane.publish(&page(&scope, "page:0:10", 1), 1);

        let before = h.lane.coverage.undelivered_watermark(&scope);
        let emit = h.emit_marker(&scope, before);
        tokio::pin!(emit);
        assert!(
            still_parked(emit.as_mut()).await,
            "the marker must park on the bound, or this test observes nothing"
        );

        // The reset closes while the marker waits: it retires the outstanding
        // publication - which frees the bound and wakes the marker - and raises
        // the fence the marker has to re-read.
        let _ = h.lane.coverage.invalidate_scope(&scope);

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), emit)
            .await
            .expect("the reset's retirement frees the bound and wakes the marker");
        assert_eq!(
            outcome,
            MarkerOutcome::Withheld,
            "a marker whose scope was reset inside its wait must be withheld - and \
             withheld DISTINCTLY from shutdown, or one reset retires the whole \
             orchestrator"
        );
        assert_eq!(
            h.received(),
            0,
            "and nothing may have been broadcast: the marker would suppress the \
             replacement incarnation's entire inventory walk"
        );
    }

    /// The marker belongs to the walk that produced it, and a reset ANYWHERE
    /// inside that walk voids it - including one that closes after the last page
    /// and before the provider stream's terminal `Done`.
    ///
    /// That window is not exotic: the reset does not cancel the stream, so the
    /// old walk simply finishes. A fence snapshotted at the emission is by then
    /// the REPLACEMENT incarnation's, so the marker compares the new fence
    /// against itself, passes, is minted above it, acknowledges normally - and
    /// suppresses the replacement's entire inventory walk on the next attach.
    /// The walk's own starting fence is therefore carried into the emission.
    #[tokio::test(start_paused = true)]
    async fn a_marker_may_not_adopt_a_fence_from_a_later_incarnation() {
        let h = marker_harness(4);
        let scope = scope_a();
        // A page of the walk, so the reset below has something to fence and the
        // mint counter it fences at is past the walk's own starting fence.
        h.lane.publish(&page(&scope, "page:0:10", 1), 1);
        let fence_at_walk_start = h.lane.coverage.scope_fence(&scope);
        let before = h.lane.coverage.undelivered_watermark(&scope);

        // The reset closes while the walk's last page is still on the provider
        // stream, so it lands BEFORE the emission rather than during its wait.
        let _ = h.lane.coverage.invalidate_scope(&scope);
        assert_ne!(
            h.lane.coverage.scope_fence(&scope),
            fence_at_walk_start,
            "the reset must have moved the fence, or this test stages nothing"
        );

        // The bound is wide open, so this is the FAST path: no park, no wake, and
        // therefore nothing but the carried fence can catch the reset.
        let outcome = h
            .emit_marker_for_walk(&scope, before, fence_at_walk_start)
            .await;
        assert_eq!(
            outcome,
            MarkerOutcome::Withheld,
            "a marker whose walk was reset must be withheld however early the reset \
             landed; comparing against a fence read at emission time compares the \
             replacement incarnation with itself"
        );
        assert_eq!(h.received(), 0, "and nothing is broadcast");
    }

    /// The sixth review's first finding. `every_page_reached_someone` is read
    /// BEFORE the marker is emitted, and the emission can park indefinitely - so
    /// the reading has to be re-validated after the wait as well.
    ///
    /// The staging is the one that loses a page: the final page of the walk is
    /// still unacknowledged, so the marker parks behind it; the receiver holding
    /// it departs, and the sweep both records the page undelivered AND frees the
    /// capacity the marker is parked on. Publishing on that wake records a
    /// durable completion for a walk that lost a page an instant earlier, and the
    /// replacement consumer acknowledges it in good faith.
    #[tokio::test(start_paused = true)]
    async fn a_page_swept_while_the_marker_waits_withholds_the_marker() {
        let h = marker_harness(1);
        let scope = scope_a();
        let before = h.lane.coverage.undelivered_watermark(&scope);
        // The walk's final page: delivered to receiver 0, unacknowledged.
        h.lane.publish(&page(&scope, "page:0:10", 1), 1);

        let emit = h.emit_marker(&scope, before);
        tokio::pin!(emit);
        assert!(still_parked(emit.as_mut()).await, "parked behind the page");

        // Receiver 0 departs. This is the sweep: it records the page as reaching
        // nobody and frees the bound in the same transition.
        assert_eq!(h.lane.coverage.release_undelivered(None), 1);

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), emit)
            .await
            .expect("the sweep frees the bound and wakes the marker");
        assert_eq!(
            outcome,
            MarkerOutcome::Withheld,
            "a walk that lost a page while its marker waited must not record a durable \
             completion; the pre-wait reading cannot see this"
        );
        assert_eq!(h.received(), 0, "and the marker is not broadcast");
    }

    /// The park warning has to NAME the receiver that stopped reading, or a
    /// consumer holding an unpolled receiver sees only "backfill is stuck".
    ///
    /// The crate has no tracing capture harness, so what is pinned is the
    /// accessor the warning reads: the live numbered receivers eligible for an
    /// outstanding page that have not read it, and that page's scope.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_charge_holders_name_the_receiver_that_has_not_read() {
        let h = harness(1);
        h.coverage.receiver_joined(0);
        h.coverage.receiver_joined(1);
        // Sent when two receivers had been handed out, so both are eligible.
        let id = h.publish(&page(&scope_a(), "page:0:10", 1), 2);

        assert_eq!(
            h.coverage.backfill_charge_holders(),
            (vec![0, 1], vec![scope_a()]),
            "both eligible receivers are holding the page, and the warning must be \
             able to say which"
        );

        h.coverage.mark_received(&id, 1);
        assert_eq!(
            h.coverage.backfill_charge_holders(),
            (vec![0], vec![scope_a()]),
            "the fast reader drops out of the list; the slow one is the stall"
        );

        h.coverage.mark_received(&id, 0);
        assert_eq!(
            h.coverage.backfill_charge_holders(),
            (Vec::new(), Vec::new()),
            "a fully read page holds nothing and names nobody"
        );
    }

    /// A capacity of zero would be a permanently parked cold start.
    ///
    /// The timeout is the assertion. Without it, removing the `max(1)` guards
    /// leaves this test HANGING rather than failing - a park is exactly what the
    /// bug produces, and an await with no deadline cannot tell "admitted" from
    /// "never returns".
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_zero_capacity_lane_still_admits_one_publication() {
        let h = harness(0);
        let admitted = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            h.gate.wait_for_capacity(),
        )
        .await
        .expect("a zero capacity must not park cold start for ever");
        assert!(admitted.is_ok());
    }
}
