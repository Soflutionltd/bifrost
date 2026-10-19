//! `Control` handle implementation.
//!
//! `bifrost_types::Control` is the consumer-facing trait. `SyncControl`
//! is the engine's concrete implementor: holds the boundary sender,
//! the priority watch sender, the bandwidth meters, and the
//! per-generation checkpoint signal.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountId, Cause, Checkpoint, Control,
    CursorScope, DurableCheckpointSet, Partition, Priority, RequestCause, RequestErrorKind,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::cancel::{Boundary, BoundaryRequest};

/// Concrete `Control` implementation.
///
/// Cloneable so the engine can hand out additional handles (e.g. one
/// per `account_changes_stream` subscription, sharing the same lifecycle).
#[derive(Clone)]
pub struct SyncControl {
    inner: Arc<SyncControlInner>,
    /// Set only on the clone [`SyncControl::admitted_by`] returns. Such a
    /// handle registers activity unconditionally, because a live
    /// [`SyncActivityGuard`] already keeps the account non-quiescent: the
    /// boundary check exists to keep NEW work out of a paused account, and the
    /// holder's own nested work is not new. Never set on a handle that leaves
    /// the crate or outlives the guard it was derived from.
    admitted: bool,
}

/// Latest checkpoint snapshot carried by the `watch` channel. The
/// generation counter increments each time `pause` or
/// `checkpoint_now` is invoked. A waiter returns only after a
/// quiescence observation in its own generation, carrying the latest
/// durable checkpoint snapshot when one exists.
#[derive(Debug, Clone)]
struct CheckpointSnapshot {
    /// Generation at which this checkpoint was recorded.
    generation: u64,
    /// The checkpoint itself, if any has been recorded yet.
    checkpoints: DurableCheckpointSet,
    /// No engine stream operation is active and every broadcast
    /// checkpoint has been consumer-acked.
    quiescent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum DurableLane {
    Change(CursorScope),
    Backfill(CursorScope, Partition),
    Unkeyed,
}

impl DurableLane {
    fn of(checkpoint: &Checkpoint) -> Self {
        match checkpoint {
            Checkpoint::Change(cursor) => Self::Change(cursor.scope.clone()),
            Checkpoint::Backfill(backfill) => {
                Self::Backfill(backfill.scope.clone(), backfill.partition.clone())
            }
            _ => Self::Unkeyed,
        }
    }
}

struct SyncControlInner {
    account: AccountId,
    boundary: Boundary,
    /// Watch channel carrying the latest persisted checkpoint and the
    /// generation in which quiescence was observed.
    checkpoint_tx: watch::Sender<CheckpointSnapshot>,
    durable:
        std::sync::Mutex<HashMap<DurableLane, (Option<crate::cursor::PublicationId>, Checkpoint)>>,
    /// Monotonic generation counter; bumped by `pause` /
    /// `checkpoint_now` BEFORE flipping the boundary so the waiter
    /// reads the new generation before parking.
    generation: AtomicU64,
    active: AtomicU64,
    /// The per-account publication ledger: coverage claims, boundary
    /// registrations, supersession, lag abandonment and retirement, in
    /// one place under one lock. At most one boundary per LANE (a
    /// scope's changes stream, or one backfill partition of a scope).
    /// Every entry leaves through `record_publication` (acked and
    /// durable), `retire_publication` (ack processed but not durable,
    /// or never delivered), abandonment after a subscriber lag, or
    /// supersession by a newer broadcast in the same lane - so an
    /// unacked batch can never wedge boundary waiters indefinitely.
    publications: Arc<crate::cursor::Publications>,
    priority: watch::Sender<Priority>,
    bandwidth_cap: watch::Sender<Option<u64>>,
    bandwidth_observed: AtomicU64,
}

impl SyncControl {
    #[must_use]
    pub fn new(
        account: AccountId,
        boundary: Boundary,
        priority: watch::Sender<Priority>,
        bandwidth_cap: watch::Sender<Option<u64>>,
    ) -> Self {
        Self::new_with_publications(
            account,
            boundary,
            priority,
            bandwidth_cap,
            Arc::new(crate::cursor::Publications::new()),
        )
    }

    pub(crate) fn new_with_publications(
        account: AccountId,
        boundary: Boundary,
        priority: watch::Sender<Priority>,
        bandwidth_cap: watch::Sender<Option<u64>>,
        publications: Arc<crate::cursor::Publications>,
    ) -> Self {
        let (checkpoint_tx, _rx) = watch::channel(CheckpointSnapshot {
            generation: 0,
            checkpoints: DurableCheckpointSet::default(),
            quiescent: true,
        });
        Self {
            inner: Arc::new(SyncControlInner {
                account,
                boundary,
                checkpoint_tx,
                durable: std::sync::Mutex::new(HashMap::new()),
                generation: AtomicU64::new(0),
                active: AtomicU64::new(0),
                publications,
                priority,
                bandwidth_cap,
                bandwidth_observed: AtomicU64::new(0),
            }),
            admitted: false,
        }
    }

    /// A handle for work nested inside `held`, whose own activity
    /// registrations succeed whatever the boundary reads.
    ///
    /// A repair that holds one registration from before its first mutation
    /// through its last has already been admitted; the establishment it runs
    /// registers activity of its own (`run_establish`, and the inventory walk
    /// under it). Those nested registrations must not be re-judged against the
    /// boundary: a pause landing mid-repair flips it to `Pause`, the nested
    /// registration would be refused, and the repair would strand exactly the
    /// half-done state holding the registration was meant to prevent. The pause
    /// still waits for the repair, because `held` is still counted.
    #[must_use]
    pub(crate) fn admitted_by(&self, held: &SyncActivityGuard) -> Self {
        debug_assert!(
            Arc::ptr_eq(&self.inner, &held.control.inner),
            "an activity guard admits nested work only on its own control"
        );
        Self {
            inner: Arc::clone(&self.inner),
            admitted: true,
        }
    }

    /// Engine-side hook called after the checkpoint store accepts a
    /// consumer ack. Removes the matching outstanding broadcast and
    /// refreshes the latest durable snapshot.
    ///
    /// Value-identified form, for a caller that holds only a
    /// checkpoint. Engine paths call `record_publication` instead:
    /// equal checkpoint VALUES legitimately describe different
    /// publications, so a value search can release a boundary that
    /// belongs to a different, still in-flight broadcast.
    ///
    /// Documented foot-gun, kept on purpose, and unreachable from every engine
    /// path by design. An audit finding it unused has found the intended state,
    /// not dead code; it is published and so not removable. Same standing as
    /// [`crate::cursor::PendingCoverage::claim`].
    pub async fn record_checkpoint(&self, checkpoint: Checkpoint) {
        self.inner.publications.acknowledge_checkpoint(&checkpoint);
        self.announce_durable(None, checkpoint);
    }

    /// Engine-side hook called after the checkpoint store accepts a
    /// consumer ack, identifying the broadcast by PUBLICATION.
    ///
    /// Release is by publication identity, never by supersession key:
    /// acking an older checkpoint says nothing about a newer
    /// outstanding one on the same scope, and claiming otherwise would
    /// report a safe boundary the consumer has not actually reached.
    /// Supersession in `register` is what keeps the set bounded.
    pub(crate) async fn record_publication(
        &self,
        publication: Option<crate::cursor::PublicationId>,
        checkpoint: Checkpoint,
    ) {
        match publication.clone() {
            Some(id) => self.inner.publications.acknowledge_publication(id),
            None => self.inner.publications.acknowledge_checkpoint(&checkpoint),
        }
        self.announce_durable(publication, checkpoint);
    }

    fn announce_durable(
        &self,
        publication: Option<crate::cursor::PublicationId>,
        checkpoint: Checkpoint,
    ) {
        let checkpoints = {
            let mut durable = self
                .inner
                .durable
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let lane = DurableLane::of(&checkpoint);
            let replace =
                durable
                    .get(&lane)
                    .is_none_or(|(current, _)| match (&publication, current) {
                        (Some(next), Some(previous)) => next >= previous,
                        // A publication-less announcement replaces
                        // unconditionally - including a lane snapshot recorded
                        // under a newer PublicationId. No engine path reaches
                        // that today (only the published `record_checkpoint`
                        // foot-gun announces without a publication), but a new
                        // publication-less caller would silently regress the
                        // lane; guard by ordering if one ever appears.
                        _ => true,
                    });
            if replace {
                durable.insert(lane, (publication, checkpoint));
            }
            DurableCheckpointSet::new(
                durable
                    .values()
                    .map(|(_, checkpoint)| checkpoint.clone())
                    .collect(),
            )
        };
        let generation = self.inner.generation.load(Ordering::SeqCst);
        let snapshot = CheckpointSnapshot {
            generation,
            checkpoints,
            quiescent: self.is_quiescent(),
        };
        self.inner.checkpoint_tx.send_replace(snapshot);
    }

    /// Register a protocol operation that may broadcast work. The
    /// increment-then-boundary-check closes the pause race: either the
    /// pause observes this activity, or this method observes the
    /// pause and drops the guard before any work starts.
    #[must_use]
    pub(crate) fn begin_activity(&self) -> Option<SyncActivityGuard> {
        self.inner.active.fetch_add(1, Ordering::SeqCst);
        if self.admitted || matches!(self.inner.boundary.snapshot(), BoundaryRequest::Run) {
            Some(SyncActivityGuard {
                control: self.clone(),
            })
        } else {
            self.end_activity();
            None
        }
    }

    /// Test shorthand for a boundary registration with no coverage
    /// claim. Production paths use `publish_checkpoint`, so a
    /// publication cannot exist with only one of its halves.
    #[cfg(test)]
    pub(crate) fn expect_checkpoint(&self, checkpoint: Checkpoint) -> crate::cursor::PublicationId {
        self.publish_checkpoint_without_report(checkpoint, 0)
    }

    /// Atomically create both halves of a checkpoint publication:
    /// the coverage claim and the boundary registration.
    ///
    /// Called BEFORE broadcasting the batch. Quiescence cannot satisfy
    /// a boundary waiter until a matching consumer ack reaches
    /// `record_publication`, or the broadcast is retired as never-acked.
    /// A newer broadcast in the same LANE supersedes the older one and
    /// folds its claim in, so the set is bounded by the account's scope
    /// and partition count rather than by how many batches a consumer
    /// left unacked: a consumer that persists several batches and acks
    /// only the last checkpoint still reaches a safe boundary.
    pub(crate) fn publish_checkpoint(
        &self,
        checkpoint: Checkpoint,
        claim: crate::cursor::CoverageClaim,
    ) -> crate::cursor::PublicationId {
        self.inner.publications.register(checkpoint, claim)
    }

    pub(crate) fn publish_checkpoint_without_report(
        &self,
        checkpoint: Checkpoint,
        generation: u64,
    ) -> crate::cursor::PublicationId {
        self.inner.publications.register(
            checkpoint,
            crate::cursor::CoverageClaim {
                reports: Vec::new(),
                generation,
            },
        )
    }

    /// The completion-marker flavour: the same publication, with the walk's
    /// undelivered watermark stamped into its receipt so the ack writer can ask
    /// again - at acknowledgement time, and after any retirement - whether the
    /// walk this marker concludes is still whole.
    pub(crate) fn publish_walk_marker(
        &self,
        checkpoint: Checkpoint,
        walk_watermark: u64,
    ) -> crate::cursor::PublicationId {
        self.inner.publications.register_walk_marker(
            checkpoint,
            crate::cursor::CoverageClaim {
                reports: Vec::new(),
                generation: 0,
            },
            walk_watermark,
        )
    }

    /// Engine-side hook for a broadcast that will never produce a
    /// durable checkpoint: the checkpoint store rejected the ack, or
    /// the batch reached no real subscriber so no ack is coming.
    ///
    /// The batch is no longer in flight either way, so it must stop
    /// gating boundary waiters - leaving it pending would wedge every
    /// later `pause` / `checkpoint_now` on this account for the rest
    /// of the process. The durable snapshot is deliberately NOT
    /// advanced, because nothing new became durable; a consumer whose
    /// ack failed learns that from `ack_checkpoint`'s own `Result`.
    ///
    /// Identified by PUBLICATION for the same reason acknowledgement
    /// is: two publications can carry equal checkpoint values, and a
    /// value search would retire the wrong one - releasing a boundary
    /// still in flight, and taking its coverage claim with it.
    pub(crate) fn retire_publication(&self, publication: crate::cursor::PublicationId) {
        self.inner.publications.retire_publication(publication);
        let generation = self.inner.generation.load(Ordering::SeqCst);
        self.publish_quiescence(generation);
    }

    /// The consumer's view of the broadcast stream lost batches: the
    /// per-account ring overwrote entries before this subscriber read
    /// them. Retire every outstanding broadcast registration.
    ///
    /// This is not a nicety. Registration happens before the send, and
    /// an entry only leaves through the matching consumer
    /// ack. Batches destroyed by ring overflow are never delivered, so
    /// their acks can never arrive, and every later `pause` /
    /// `checkpoint_now` on this account would wait forever - trading
    /// silent in-session data loss for a permanent hang, which is the
    /// worse failure. After a lag the consumer's view is definitionally
    /// incomplete, so no outstanding registration can be trusted to
    /// produce an ack; the whole set goes.
    ///
    /// Retiring an entry whose batch is still in the ring is safe: the
    /// consumer will still receive it and still ack it, and
    /// `record_checkpoint` advances the durable snapshot for a
    /// checkpoint it no longer tracks as pending. The cost is that a
    /// `pause` between the lag and that ack reports the previous
    /// durable checkpoint instead of the newer one - which is exactly
    /// what the accompanying `OperatorAttentionNeeded` warning tells
    /// the consumer to reconcile from. The durable snapshot is
    /// deliberately not advanced here; nothing became durable.
    ///
    /// Returns how many registrations were abandoned, for the warning.
    pub(crate) fn abandon_pending_checkpoints(&self) -> usize {
        let abandoned = self.inner.publications.abandon_checkpoints();
        if abandoned > 0 {
            tracing::warn!(
                target: "bifrost.sync.control",
                account = ?self.inner.account,
                abandoned,
                "change stream lagged; abandoning outstanding checkpoint registrations \
                 that can no longer be acked"
            );
        }
        let generation = self.inner.generation.load(Ordering::SeqCst);
        self.publish_quiescence(generation);
        abandoned
    }

    /// Republish quiescence after something OTHER than this handle retired
    /// outstanding registrations.
    ///
    /// `retire_publication` and `abandon_pending_checkpoints` do this as part of
    /// their own work. The receiver-drop sweep retires through the ledger
    /// directly - it is one transition under the ledger lock, which is what keeps
    /// a concurrent scope reset from losing the debt - so it has no way to reach
    /// `publish_quiescence` on its own. Without this the sweep can remove the
    /// last outstanding boundary while a `pause` or `checkpoint_now` waiter sits
    /// on the checkpoint watch, and that waiter never wakes.
    pub(crate) fn publish_boundary_progress(&self) {
        let generation = self.inner.generation.load(Ordering::SeqCst);
        self.publish_quiescence(generation);
    }

    /// Engine-side hook: bandwidth meter feeds observed throughput.
    pub fn observe_bandwidth(&self, bps: u64) {
        self.inner.bandwidth_observed.store(bps, Ordering::Relaxed);
    }

    /// Apply the latest policy while holding the control snapshot stable.
    pub(crate) fn apply_bandwidth_budget(&self, budget: &bifrost_net::BandwidthBudget) {
        budget.set_cap(*self.inner.bandwidth_cap.borrow());
    }

    /// Keep control updates ordered with a replacement account's setter.
    pub(crate) fn apply_bandwidth_cap(&self, setter: impl FnOnce(Option<u64>)) {
        let cap = self.inner.bandwidth_cap.borrow();
        setter(*cap);
    }

    /// Read the configured cap. `None` when no cap is set.
    #[must_use]
    pub fn bandwidth_cap_snapshot(&self) -> Option<u64> {
        *self.inner.bandwidth_cap.borrow()
    }

    /// Latest requested priority.
    #[must_use]
    pub fn priority_snapshot(&self) -> Priority {
        *self.inner.priority.borrow()
    }

    /// Account id this control governs. Exposed for tracing spans.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.inner.account
    }

    /// Test-only view of the announced durable snapshot, i.e. exactly what a
    /// `pause` / `checkpoint_now` waiter would be handed right now.
    #[cfg(test)]
    pub(crate) fn durable_snapshot(&self) -> DurableCheckpointSet {
        self.inner.checkpoint_tx.borrow().checkpoints.clone()
    }

    pub(crate) async fn wait_until_running(&self, shutdown: &CancellationToken) -> bool {
        let mut boundary = self.inner.boundary.subscribe();
        loop {
            match boundary.peek() {
                BoundaryRequest::Run => return true,
                BoundaryRequest::Stop => return false,
                BoundaryRequest::Pause | BoundaryRequest::CheckpointNow => {}
            }
            tokio::select! {
                () = shutdown.cancelled() => return false,
                changed = boundary.changed() => {
                    if changed.is_none() {
                        return false;
                    }
                }
            }
        }
    }

    fn is_quiescent(&self) -> bool {
        if self.inner.active.load(Ordering::SeqCst) != 0 {
            return false;
        }
        self.inner.publications.pending_checkpoints() == 0
    }

    fn publish_quiescence(&self, generation: u64) {
        if !self.is_quiescent() {
            return;
        }
        let checkpoints = self.inner.checkpoint_tx.borrow().checkpoints.clone();
        self.inner.checkpoint_tx.send_replace(CheckpointSnapshot {
            generation,
            checkpoints,
            quiescent: true,
        });
    }

    fn end_activity(&self) {
        let previous = self.inner.active.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous > 0, "sync activity counter underflow");
        if previous == 1 {
            let generation = self.inner.generation.load(Ordering::SeqCst);
            self.publish_quiescence(generation);
        }
    }

    /// Block until the account is quiescent at or after the request
    /// generation. The latest durable checkpoint may be absent when
    /// the account has never produced traffic.
    ///
    /// The wait also ends, with an error, when the boundary reads `Stop`.
    /// `Stop` is written only by `detach`, which removes the public ack
    /// sender at its top - so from that moment no outstanding registration
    /// can ever be acknowledged, and a quiescence this waiter is holding out
    /// for cannot arrive. The checkpoint watch alone cannot deliver that
    /// verdict: the consumer's own `SyncControl` clone keeps the sender
    /// alive, so the channel never closes under it. Before this arm existed
    /// the waiter's only rescue was incidental - a receiver dropped inside
    /// the teardown window ran the departure sweep, which retired the
    /// registrations and republished quiescence - and the sweep is now
    /// inert during teardown by ruling, so the rescue is deliberate instead.
    async fn wait_for_checkpoint_at_or_after(
        &self,
        generation: u64,
    ) -> Result<DurableCheckpointSet, AccountError> {
        // Subscribe to a fresh receiver. The current value is the
        // last recorded snapshot; if it already matches the
        // generation we return immediately.
        let mut rx = self.inner.checkpoint_tx.subscribe();
        let mut boundary = self.inner.boundary.subscribe();
        loop {
            {
                let snap = rx.borrow();
                if snap.generation >= generation && snap.quiescent {
                    return Ok(snap.checkpoints.clone());
                }
            }
            if boundary.peek() == BoundaryRequest::Stop {
                return Err(control_wait_ended(
                    "control: account stopped before reaching a safe boundary; a page \
                     delivered but unacknowledged at detach is the consumer's to have \
                     persisted or to forfeit",
                ));
            }
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        return Err(control_wait_ended(
                            "control: checkpoint watch channel closed",
                        ));
                    }
                }
                changed = boundary.changed() => {
                    if changed.is_none() {
                        return Err(control_wait_ended(
                            "control: boundary channel closed",
                        ));
                    }
                    // Loop: re-read the snapshot, then the boundary.
                }
            }
        }
    }
}

/// The error a `pause` / `checkpoint_now` waiter returns when the account
/// can no longer reach the boundary it asked for.
fn control_wait_ended(detail: &'static str) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: bifrost_types::DiagnosticText::support_only(detail),
        }),
    )
    .try_build()
    .expect("valid account error classification")
}

/// Restores the boundary request `checkpoint_now` displaced, whichever
/// way the wait ends: the checkpoint landing, the watch channel
/// closing, or the caller dropping the future mid-await.
///
/// `previous` is `None` only when `request_checkpoint` refused to
/// install over a `Stop`, in which case there is nothing to restore.
/// The restore is still conditional on the boundary reading
/// `CheckpointNow`, so an interleaved pause, stop, or resume wins.
struct CheckpointLatchGuard<'a> {
    boundary: &'a Boundary,
    previous: Option<BoundaryRequest>,
}

impl Drop for CheckpointLatchGuard<'_> {
    fn drop(&mut self) {
        if let Some(previous) = self.previous {
            self.boundary
                .restore_if_current(BoundaryRequest::CheckpointNow, previous);
        }
    }
}

pub(crate) struct SyncActivityGuard {
    control: SyncControl,
}

impl Drop for SyncActivityGuard {
    fn drop(&mut self) {
        self.control.end_activity();
    }
}

impl Control for SyncControl {
    fn pause(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<DurableCheckpointSet, AccountError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            // Bump the generation BEFORE flipping the boundary so
            // `record_checkpoint` calls that race with us land in the
            // new generation, not the old one.
            let gen_id = self.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
            self.inner
                .boundary
                .set_unless_stopped(BoundaryRequest::Pause);
            self.publish_quiescence(gen_id);
            self.wait_for_checkpoint_at_or_after(gen_id).await
        })
    }

    fn checkpoint_now(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<DurableCheckpointSet, AccountError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let gen_id = self.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
            // Read-and-install is one atomic step, so a `Pause` landing
            // between the two is never displaced by this request.
            // The guard restores on EVERY exit from the wait, not just
            // the success one. A `?` here instead would leave
            // `CheckpointNow` latched whenever the watch channel closed
            // under the waiter, and dropping the future would do the
            // same - and a latched `CheckpointNow` reads as not-running
            // to `wait_until_running`, so backfill, deferred inventory,
            // and `restart_account` all park until some unrelated write
            // moves the boundary.
            let _latch = CheckpointLatchGuard {
                boundary: &self.inner.boundary,
                previous: self.inner.boundary.request_checkpoint(),
            };
            self.publish_quiescence(gen_id);
            self.wait_for_checkpoint_at_or_after(gen_id).await
        })
    }

    fn resume(&self) {
        self.inner.boundary.set_unless_stopped(BoundaryRequest::Run);
    }

    fn priority(&self, p: Priority) {
        self.inner.priority.send_replace(p);
    }

    fn bandwidth_cap(&self, bps: Option<u64>) {
        self.inner.bandwidth_cap.send_replace(bps);
    }

    fn bandwidth_observed(&self) -> u64 {
        self.inner.bandwidth_observed.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for SyncControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncControl")
            .field("account", &self.inner.account)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{ChangeCursor, CursorScope, OpaqueChangeState, ProtocolKind};

    fn control() -> SyncControl {
        let (boundary, _view) = Boundary::new();
        let (priority, _priority_view) = watch::channel(Priority::Normal);
        let (bandwidth, _bandwidth_view) = watch::channel(None);
        SyncControl::new(
            AccountId("control-unit".into()),
            boundary,
            priority,
            bandwidth,
        )
    }

    #[test]
    fn replacement_bandwidth_application_serializes_control_changes() {
        let control = control();
        control.bandwidth_cap(Some(7));
        let budget = bifrost_net::BandwidthBudget::new(Some(7));
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let changing = control.clone();
        let changed_budget = budget.clone();
        let worker = std::thread::spawn(move || {
            start_rx.recv().expect("start change");
            changing.bandwidth_cap(Some(11));
            changing.apply_bandwidth_budget(&changed_budget);
            done_tx.send(()).expect("change done");
        });
        control.apply_bandwidth_cap(|cap| {
            start_tx.send(()).expect("start worker");
            // A concurrent update must wait until the setter has finished.
            let changed = done_rx.recv_timeout(std::time::Duration::from_millis(100));
            budget.set_cap(cap);
            assert!(
                changed.is_err(),
                "control changed during replacement setter"
            );
        });
        worker.join().expect("control worker");
        assert_eq!(budget.cap(), Some(11));
    }

    fn checkpoint(state: &[u8]) -> Checkpoint {
        Checkpoint::Change(ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Imap,
                envelope_version: 1,
                bytes: state.to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        })
    }

    fn durable(checkpoint: Checkpoint) -> bifrost_types::DurableCheckpointSet {
        bifrost_types::DurableCheckpointSet::new(vec![checkpoint])
    }

    #[tokio::test]
    async fn pause_waits_for_active_work_to_reach_a_boundary() {
        let control = control();
        let activity = control.begin_activity().expect("running activity");
        let waiter_control = control.clone();
        let waiter = tokio::spawn(async move { waiter_control.pause().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(activity);
        assert_eq!(
            waiter.await.expect("waiter task").expect("pause"),
            bifrost_types::DurableCheckpointSet::default()
        );
        assert!(
            control.begin_activity().is_none(),
            "paused accounts must refuse every new engine activity registration"
        );
    }

    /// Work nested inside a held registration is admitted under a pause, and a
    /// pause still waits for the outer registration.
    #[tokio::test]
    async fn a_registration_nested_in_a_held_one_survives_a_pause_landing_between() {
        let control = control();
        let held = control.begin_activity().expect("running activity");
        let nested_control = control.admitted_by(&held);

        let waiter_control = control.clone();
        let waiter = tokio::spawn(async move { waiter_control.pause().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "the held registration is counted");
        assert!(
            control.begin_activity().is_none(),
            "a plain registration is refused once the pause has flipped the boundary"
        );

        let nested = nested_control
            .begin_activity()
            .expect("a nested registration is not new work");
        drop(held);
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the nested registration still keeps the account non-quiescent"
        );
        drop(nested);
        waiter.await.expect("waiter task").expect("pause");
    }

    /// A `Stop` ends a waiter that can no longer be satisfied.
    ///
    /// `Stop` is written only by `detach`, which has already removed the public
    /// ack sender; the registration this waiter is parked on can never be
    /// acknowledged, and the consumer's own control clone keeps the checkpoint
    /// watch open, so nothing else would ever wake it. Ablation: without the
    /// boundary arm the waiter never finishes.
    #[tokio::test]
    async fn a_stop_ends_a_pause_waiting_on_an_unacknowledgeable_registration() {
        let control = control();
        control.expect_checkpoint(checkpoint(b"never-acked"));
        let waiter_control = control.clone();
        let waiter = tokio::spawn(async move { waiter_control.pause().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "the registration is outstanding");

        control.inner.boundary.set(BoundaryRequest::Stop);
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("the waiter must end once the account stops")
            .expect("waiter task");
        assert!(
            result.is_err(),
            "a stopped account cannot reach the boundary, and must not claim to"
        );
    }

    #[tokio::test]
    async fn pause_waits_for_the_consumer_ack_of_a_broadcast_checkpoint() {
        let control = control();
        let activity = control.begin_activity().expect("running activity");
        let expected = checkpoint(b"pending");
        control.expect_checkpoint(expected.clone());
        let waiter_control = control.clone();
        let waiter = tokio::spawn(async move { waiter_control.pause().await });
        tokio::task::yield_now().await;

        drop(activity);
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "quiescent workers are not safe while a consumer ack is outstanding"
        );

        control.record_checkpoint(expected.clone()).await;
        assert_eq!(
            waiter.await.expect("waiter task").expect("pause"),
            durable(expected)
        );
    }

    fn backfill_checkpoint(scope: CursorScope, partition: &[u8]) -> Checkpoint {
        Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
            scope,
            partition: bifrost_types::Partition(partition.to_vec()),
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: 1,
        })
    }

    fn pending_len(control: &SyncControl) -> usize {
        control.inner.publications.pending_checkpoints()
    }

    /// A consumer that persists several batches and acks only the last
    /// checkpoint must still reach a safe boundary. Without
    /// supersession every skipped ack would strand an entry that no
    /// later ack can match, wedging this account's waiters forever.
    #[tokio::test]
    async fn acking_only_the_latest_checkpoint_still_reaches_a_boundary() {
        let control = control();
        for state in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            control.expect_checkpoint(checkpoint(state));
        }
        assert_eq!(
            pending_len(&control),
            1,
            "a newer broadcast on the same scope supersedes its predecessors"
        );

        control.record_checkpoint(checkpoint(b"three")).await;
        assert_eq!(
            control.pause().await.expect("pause"),
            durable(checkpoint(b"three"))
        );
    }

    /// Supersession is per lane, per scope and - for backfill - per
    /// PARTITION. Concurrent work on distinct scopes must each be
    /// tracked, or a pause would report a safe boundary while another
    /// scope's batch is still unacked. Two partitions of ONE scope are
    /// in flight together by design and are equally distinct: neither
    /// subsumes the other.
    #[test]
    fn supersession_is_keyed_by_lane_scope_and_partition() {
        let control = control();
        let email = CursorScope::Type(bifrost_types::ObjectType::Email);
        control.expect_checkpoint(checkpoint(b"account-change"));
        control.expect_checkpoint(backfill_checkpoint(CursorScope::Account, b"page:0:500"));
        control.expect_checkpoint(backfill_checkpoint(email.clone(), b"page:0:500"));
        assert_eq!(pending_len(&control), 3);

        // A sibling partition of the same scope is its own registration.
        control.expect_checkpoint(backfill_checkpoint(email.clone(), b"page:500:1000"));
        assert_eq!(
            pending_len(&control),
            4,
            "a sibling partition must not release its sibling's boundary"
        );

        // A later page of the SAME partition does replace its predecessor:
        // one sequential task produced both.
        control.expect_checkpoint(backfill_checkpoint(email, b"page:500:1000"));
        assert_eq!(pending_len(&control), 4);
    }

    #[tokio::test]
    async fn out_of_order_distinct_scope_acks_preserve_the_complete_boundary() {
        let control = control();
        let account_checkpoint = checkpoint(b"account-newer");
        let type_checkpoint = Checkpoint::Change(bifrost_types::ChangeCursor {
            scope: CursorScope::Type(bifrost_types::ObjectType::Email),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Imap,
                envelope_version: 1,
                bytes: b"type-older".to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        });
        let account_publication = control.expect_checkpoint(account_checkpoint.clone());
        let type_publication = control.expect_checkpoint(type_checkpoint.clone());

        control
            .record_publication(Some(account_publication), account_checkpoint.clone())
            .await;
        control
            .record_publication(Some(type_publication), type_checkpoint.clone())
            .await;

        let boundary = control.pause().await.expect("pause");
        assert_eq!(boundary.checkpoints().len(), 2);
        assert!(boundary.checkpoints().contains(&account_checkpoint));
        assert!(boundary.checkpoints().contains(&type_checkpoint));
    }

    #[tokio::test]
    async fn an_older_ack_cannot_move_one_lane_backwards() {
        let control = control();
        let older = checkpoint(b"older");
        let newer = checkpoint(b"newer");
        let older_publication = control.expect_checkpoint(older.clone());
        let newer_publication = control.expect_checkpoint(newer.clone());

        control
            .record_publication(Some(newer_publication), newer.clone())
            .await;
        control
            .record_publication(Some(older_publication), older)
            .await;

        assert_eq!(control.pause().await.expect("pause"), durable(newer));
    }

    /// A checkpoint-store write failure leaves nothing durable, but the
    /// batch is no longer in flight. Retiring it keeps the boundary
    /// primitive usable; the durable snapshot must NOT advance.
    #[tokio::test]
    async fn a_failed_ack_retires_its_broadcast_without_advancing_the_snapshot() {
        let control = control();
        control.record_checkpoint(checkpoint(b"durable")).await;
        let failed = control.expect_checkpoint(checkpoint(b"never-persisted"));

        control.retire_publication(failed);

        assert_eq!(pending_len(&control), 0);
        assert_eq!(
            control.pause().await.expect("pause"),
            durable(checkpoint(b"durable")),
            "a failed ack must not be reported as the durable checkpoint"
        );
    }

    /// A batch that reached only the slot's sentinel receiver gets its
    /// registration retracted, so an account with no live consumer
    /// still pauses.
    #[tokio::test]
    async fn a_retracted_registration_does_not_gate_the_boundary() {
        let control = control();
        let undelivered = control.expect_checkpoint(checkpoint(b"no-subscriber"));
        control.retire_publication(undelivered);

        assert_eq!(
            control.pause().await.expect("pause"),
            bifrost_types::DurableCheckpointSet::default()
        );
    }

    /// A `checkpoint_now` that does not run to completion must still
    /// unlatch. `wait_until_running` reads `CheckpointNow` as
    /// not-running, so a leaked latch parks backfill, deferred
    /// inventory, and `restart_account` until some unrelated write
    /// moves the boundary - the account goes quiet with nothing in the
    /// logs to say why.
    #[tokio::test(start_paused = true)]
    async fn an_abandoned_checkpoint_request_unlatches_the_boundary() {
        let control = control();
        let _activity = control.begin_activity().expect("running activity");

        let abandoned = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            Control::checkpoint_now(&control),
        )
        .await;
        assert!(
            abandoned.is_err(),
            "the wait cannot finish while activity is outstanding"
        );

        assert_eq!(control.inner.boundary.snapshot(), BoundaryRequest::Run);
        assert!(
            control.wait_until_running(&CancellationToken::new()).await,
            "a dropped checkpoint request must not park the engine's workers"
        );
    }

    /// Ring overflow destroys batches whose acks were already
    /// registered as expected. Without abandonment those registrations
    /// gate every later boundary wait on this account forever.
    #[tokio::test]
    async fn abandoning_after_a_lag_unwedges_the_boundary() {
        let control = control();
        control.expect_checkpoint(checkpoint(b"lost-to-ring-overflow"));
        control.expect_checkpoint(backfill_checkpoint(CursorScope::Account, b"page:0:500"));

        assert_eq!(control.abandon_pending_checkpoints(), 2);
        assert_eq!(pending_len(&control), 0);
        assert_eq!(
            control.pause().await.expect("pause"),
            bifrost_types::DurableCheckpointSet::default(),
            "abandonment must not invent a durable checkpoint"
        );
    }

    #[tokio::test]
    async fn checkpoint_waiter_does_not_restore_over_a_concurrent_pause() {
        let control = control();
        let activity = control.begin_activity().expect("running activity");
        let waiter_control = control.clone();
        let waiter = tokio::spawn(async move { waiter_control.checkpoint_now().await });
        tokio::task::yield_now().await;
        assert_eq!(
            control.inner.boundary.snapshot(),
            BoundaryRequest::CheckpointNow
        );

        control.inner.boundary.set(BoundaryRequest::Pause);
        drop(activity);

        assert_eq!(
            waiter.await.expect("waiter task").expect("checkpoint"),
            bifrost_types::DurableCheckpointSet::default()
        );
        assert_eq!(control.inner.boundary.snapshot(), BoundaryRequest::Pause);
    }
}
