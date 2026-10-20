//! Per-account multiplexer.
//!
//! One task per account drives:
//! - `changes_stream(cursor)` for each known scope,
//! - the adaptive NOOP/STATUS poll cadence per scope,
//! - `scope_lifecycle_stream()` for scope creation / rename / deletion,
//! - inventory fusion for `EstablishViaInventory` scopes (the
//!   inventory's terminal `Done` carries the cursor, which the
//!   multiplexer registers exactly once).
//!
//! Output is a unified `SyncEvent<Change>` stream on the account's
//! broadcast channel.

pub mod changes;
pub mod fusion;
pub mod poll;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use bifrost_types::{
    Account, AccountError, AccountId, AccountOperation, ChangeCursor, Checkpoint, CursorScope,
    MembershipScope, ScopeLifecycle, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::cancel::BoundaryView;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::Error;
use crate::types::MultiplexerConfig;

pub use changes::{
    AckRequest, ChangesEvent, OperatorDecision, WriterRequest, drive_changes_stream,
};
pub use fusion::{FusionOutcome, InventoryFusion};
pub use poll::{AdaptiveCadence, PollSchedule};

/// Handle the engine stashes in the per-account slot for the
/// multiplexer task. Cancellation is rooted in the slot's shutdown
/// token; explicit shutdown shuts the task down between two
/// `boundary.peek()` calls.
#[derive(Debug)]
pub struct MultiplexerHandle {
    pub cancel: CancellationToken,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
}

/// Unified output of the multiplexer: either a synced `Change` batch
/// (with optional checkpoint) or a per-scope warning/fatal.
///
/// Wrapped in an `Arc` so subscribers receive cheap clones from the
/// broadcast.
#[derive(Debug, Clone)]
pub struct MultiplexerEvent {
    pub scope: CursorScope,
    pub event: Arc<SyncEvent<bifrost_types::Change>>,
    pub checkpoint: Option<Checkpoint>,
    /// Engine-issued identity for THIS publication of `checkpoint`, to be
    /// passed back to `Engine::ack_checkpoint`.
    ///
    /// `Some` for every acknowledgeable publication. Checkpoint-bearing
    /// events return it through `ack_checkpoint`; repair events carry no
    /// checkpoint and return it through `ack_publication`. It exists because
    /// `Checkpoint` equality is value equality and cannot identify a
    /// publication: a backfill page whose content was entirely unrepresentable
    /// increments no item count and produces a checkpoint byte-identical to its
    /// predecessor, and inventory fusion publishes its final checkpoint twice.
    /// Acknowledging by value would then apply one publication's coverage claim
    /// to a different publication's checkpoint.
    pub publication: Option<crate::cursor::PublicationId>,
}

impl MultiplexerEvent {
    /// An event carrying no checkpoint, and so nothing to acknowledge.
    #[must_use]
    pub fn unacked(scope: CursorScope, event: Arc<SyncEvent<bifrost_types::Change>>) -> Self {
        Self {
            scope,
            event,
            checkpoint: None,
            publication: None,
        }
    }
}

/// Consumer-facing receiver that turns broadcast ring overflow into an
/// observable event. The overwritten batches cannot be replayed in-session,
/// but loss is never presented as an ordinary successful receive.
///
/// The receiver also owns the recovery half of a lag. Batches destroyed by
/// ring overflow had their checkpoints registered as expected before the
/// send, and only a consumer ack retires such a registration - so a lag
/// that merely warned would leave `pause` and `checkpoint_now` waiting for
/// acks the consumer can no longer produce. Surfacing lag without doing
/// this would convert in-session data loss into a permanent hang. The
/// receiver therefore abandons the account's outstanding registrations at
/// the moment it observes the gap, and reports the count in the warning.
///
/// # Two roles, two constructors
///
/// A NUMBERED receiver ([`ChangeDelivery::subscribe`], handed out by
/// `SyncEngine::account_changes_stream`) is one that may acknowledge. It counts
/// for the subscriber gate, its reads hold and release the backfill bound, its
/// departure is swept, and its lag abandons the account's registrations.
///
/// An OBSERVER ([`ChangeDelivery::observe`], handed out by
/// `SyncEngine::account_changes_observer`) never acknowledges and the engine
/// never waits on it: it counts for nothing, its reads free nothing, its
/// departure is unremarkable, and its lag is a warning to itself alone. It
/// carries no control handle and no coverage handle, which is what makes each
/// of those true without a per-path check on the receiver's kind.
pub struct ChangesReceiver {
    /// `Option` only so `Drop` can hand the underlying receiver to
    /// [`ChangeDelivery::depart`], which destroys it under the delivery lock
    /// before deciding what is still reachable. `Some` for the whole of a
    /// receiver's usable life.
    inner: Option<broadcast::Receiver<MultiplexerEvent>>,
    control: Option<crate::control::SyncControl>,
    /// This receiver's number and the account's delivery gate, so its DEPARTURE
    /// is observable.
    ///
    /// Dropping it is the moment the engine can conclude that batches only this
    /// receiver could have taken delivery of will never be acknowledged - which
    /// is what returns their backfill capacity. Nothing else in the system
    /// notices: the broadcast channel reports a count, and the count is blind to
    /// an overlapping replacement.
    ///
    /// WEAK, so a consumer holding a receiver cannot keep the account's
    /// broadcast sender alive past `detach` and then wait for ever on a channel
    /// only it is keeping open. A failed upgrade means the engine has already
    /// torn the account down, which is exactly the case where there is nothing
    /// left to sweep.
    subscription: Option<(std::sync::Weak<ChangeDelivery>, u64)>,
    /// The account's publication ledger, which is also its backfill capacity
    /// ledger. Needed on the lag and drop paths.
    coverage: Option<Arc<crate::cursor::PendingCoverage>>,
}

impl Drop for ChangesReceiver {
    fn drop(&mut self) {
        let Some((delivery, seq)) = self.subscription.take() else {
            return;
        };
        let Some(inner) = self.inner.take() else {
            return;
        };
        let Some(delivery) = delivery.upgrade() else {
            // The account is already torn down. Nothing to sweep, nobody to tell.
            return;
        };
        // Destroy, unregister and sweep as one step under the delivery lock.
        // Anything this receiver was the last possible reader of can never be
        // acknowledged now, so retiring it here carries its debt forward exactly
        // as a lag does and returns its capacity - a cold-start producer parked
        // behind a consumer that walked away resumes at once instead of stalling
        // for the life of the attachment.
        let retired = delivery.depart(seq, inner, self.coverage.as_deref());
        if retired == 0 {
            return;
        }
        // Tell whoever is still attached, on the stream, exactly what the lag
        // path tells them. A log line does not reach a consumer.
        delivery.announce_retirement(retired);
        // Retiring the last outstanding registration can make the account
        // quiescent, and a `pause` or `checkpoint_now` is waiting on the
        // checkpoint watch rather than on the ledger. Every other retirement and
        // abandonment path republishes quiescence; this one must too, or the
        // waiter sleeps for ever on a boundary that has actually been reached.
        if let Some(control) = &self.control {
            control.publish_boundary_progress();
        }
        tracing::warn!(
            target: "bifrost.sync.changes",
            retired,
            "change stream dropped with unacknowledged batches; no live receiver can \
             acknowledge them, so their coverage debt is carried forward and the \
             consumer should reconcile from its last durable checkpoint"
        );
    }
}

/// The account's change broadcast, plus the numbering that says which live
/// receivers can still reach a given batch.
///
/// # Why the two are one type
///
/// `tokio::broadcast` cannot answer "can this receiver still get that batch": it
/// exposes a COUNT and nothing else. But a receiver joins at the ring's TAIL, so
/// the question reduces to one the engine can answer if it NUMBERS the receivers
/// it hands out - a batch broadcast when `n` receivers had ever been issued can
/// be read only by a receiver numbered below `n`.
///
/// A count, or a "has anyone left" epoch, answers correctly only when the
/// account passes through zero subscribers, and a consumer replaced by an
/// OVERLAPPING successor never does: A holds unacknowledged pages, B subscribes,
/// A drops, the count goes 1 -> 2 -> 1 without reaching zero, and B started
/// beyond A's pages.
///
/// # Why one mutex, and what it orders
///
/// The numbering is only exact if a batch's send and its stamp, a receiver's
/// broadcast subscribe and its number, and a departing receiver's unregister and
/// its sweep are each ATOMIC with respect to the others. Three separate defects
/// came from approximating that:
///
/// - the last reader dropping between the send and the stamp swept an entry
///   whose `delivered_at` was still `None`, preserved it, and then nothing swept
///   it again - the page stayed charged against a replacement that could never
///   read it;
/// - a receiver subscribing between the send and a post-hoc `issued()` read got
///   counted as a possible reader of a batch it had joined beyond;
/// - a departing receiver that computed `min_live` and was then descheduled
///   swept with a stale answer, retiring a page a receiver which subscribed in
///   the gap was about to read.
///
/// One `Mutex` around all three makes the orders total, so the sweep is exact
/// rather than approximate. It is held across `broadcast::send` (non-blocking)
/// and across the coverage ledger's own lock; the lock order is delivery ->
/// coverage and never the reverse, because nothing in `PendingCoverage` reaches
/// back here.
///
/// Live changes do NOT go through [`ChangeDelivery::publish_backfill`]: they
/// consume no capacity and are never swept, so they are never stamped. A live
/// publication that expects an ACKNOWLEDGEMENT still goes through
/// [`Self::publish_acknowledgeable`], which takes the lock only to read the live
/// numbered set at the send; events nobody acknowledges - warnings,
/// terminations, barrier pages - send on [`Self::sender`] directly and take no
/// lock.
#[derive(Debug)]
pub struct ChangeDelivery {
    tx: broadcast::Sender<MultiplexerEvent>,
    state: StdMutex<SubscriberState>,
}

#[derive(Debug, Default)]
struct SubscriberState {
    /// How many consumer receivers this account has ever handed out.
    next: u64,
    /// The sequences still holding a receiver, in order.
    live: std::collections::BTreeSet<u64>,
}

impl SubscriberState {
    fn min_live(&self) -> Option<u64> {
        self.live.iter().next().copied()
    }
}

impl ChangeDelivery {
    #[must_use]
    pub fn new(tx: broadcast::Sender<MultiplexerEvent>) -> Self {
        Self {
            tx,
            state: StdMutex::new(SubscriberState::default()),
        }
    }

    fn guard(&self) -> std::sync::MutexGuard<'_, SubscriberState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The raw sender, for publications that consume no backfill capacity.
    #[must_use]
    pub fn sender(&self) -> &broadcast::Sender<MultiplexerEvent> {
        &self.tx
    }

    /// Broadcast a backfill batch and stamp its publication with the receiver
    /// numbering as of the send, as ONE step.
    ///
    /// Returns whether a NUMBERED receiver took delivery - see
    /// [`Self::publish_acknowledgeable`] for why that, and not the broadcast's
    /// own count, is the answer a publisher needs.
    pub fn publish_backfill(
        &self,
        event: MultiplexerEvent,
        coverage: &crate::cursor::PendingCoverage,
        publication: Option<&crate::cursor::PublicationId>,
    ) -> bool {
        let state = self.guard();
        // Every receiver that can possibly hold this batch is numbered below
        // `state.next`, and no `subscribe` and no departing receiver's sweep can
        // interleave, because both want this same lock. That is what makes the
        // stamp exact rather than an approximation - and it is a STRUCTURAL
        // guarantee, not a tested one: the interleaving it rules out cannot be
        // staged from a test, because any hook placed between the send and the
        // stamp would have to take this lock to subscribe and would simply
        // deadlock. `publish_backfill` is the only path that sends a backfill
        // batch, so there is no second ordering to get wrong.
        let issued = state.next;
        let _ = self.tx.send(event);
        if let Some(id) = publication {
            coverage.mark_delivered(id, issued);
        }
        let reached = !state.live.is_empty();
        drop(state);
        reached
    }

    /// Broadcast a publication that consumes no backfill capacity but expects
    /// an acknowledgement - a live change checkpoint, an inventory fusion page,
    /// a repair batch - and say whether a NUMBERED receiver took delivery.
    ///
    /// The broadcast's own delivery count cannot answer that. It counts the
    /// slot's sentinel and every observer alongside the receivers that may
    /// acknowledge, so "more than the sentinel" was satisfied by an observer
    /// alone: a page it took delivery of was judged delivered, stayed
    /// registered, and could never be answered for once the acknowledger had
    /// gone. The live set here holds exactly the numbered receivers, and a
    /// numbered receiver's underlying `broadcast::Receiver` is destroyed only
    /// under this lock ([`Self::depart`]), so a send made while the set is
    /// non-empty reached one of them - and a send made while it is empty
    /// reached nobody who can acknowledge, whatever else it reached.
    ///
    /// Events that expect no acknowledgement - warnings, terminations, barrier
    /// pages - go on [`Self::sender`] directly and take no lock.
    pub fn publish_acknowledgeable(&self, event: MultiplexerEvent) -> bool {
        let state = self.guard();
        let _ = self.tx.send(event);
        let reached = !state.live.is_empty();
        drop(state);
        reached
    }

    /// Whether any NUMBERED receiver is live: the subscriber gate's question.
    ///
    /// A cold-start producer parks until a consumer that can acknowledge is
    /// present. The broadcast's receiver count used to stand in for this, and
    /// an observer satisfied it - the gate opened, the walk ran its pages into
    /// the observer, and the acknowledger, joining at the ring's tail
    /// afterwards, could never see them.
    #[must_use]
    pub fn has_numbered_subscriber(&self) -> bool {
        !self.guard().live.is_empty()
    }

    /// Hand out an OBSERVER on the account's change stream: a receiver that
    /// sees every event and is never waited on.
    ///
    /// Unnumbered, so it is invisible to the subscriber gate, to the delivery
    /// decision above, and to the backfill bound; it carries no control handle,
    /// so its lag warns without abandoning anything; and it carries no coverage
    /// handle, so its read and its departure touch no ledger. Nothing here takes
    /// the lock, because there is nothing to order: the numbering is exact
    /// without it, and the only thing an observer's subscribe changes is the
    /// broadcast's receiver count, which nothing reads any more.
    ///
    /// Held WEAK-free on purpose: an observer keeps no reference back here at
    /// all, so it cannot keep the sender alive past `detach` any more than a
    /// numbered receiver can.
    #[must_use]
    pub fn observe(&self) -> ChangesReceiver {
        ChangesReceiver {
            inner: Some(self.tx.subscribe()),
            control: None,
            subscription: None,
            coverage: None,
        }
    }

    /// Hand out a numbered receiver on the account's change stream.
    ///
    /// The receiver holds a WEAK reference back here. A strong one would keep
    /// this type - and therefore the broadcast `Sender` it owns - alive for as
    /// long as any consumer held a receiver, so a consumer draining the ring
    /// after `detach` would wait in `recv()` for ever instead of seeing
    /// `Closed`: it would be keeping the last sender alive itself. The engine
    /// must be able to close the channel by dropping the slot, and a weak handle
    /// is what lets it.
    pub fn subscribe(
        self: &Arc<Self>,
        control: Option<crate::control::SyncControl>,
        coverage: Option<Arc<crate::cursor::PendingCoverage>>,
    ) -> ChangesReceiver {
        let mut state = self.guard();
        // Subscribe INSIDE the lock: a receiver created here is numbered against
        // exactly the set of batches already sent, with no window in which a
        // send could slip between the two and be mis-attributed.
        let inner = self.tx.subscribe();
        let seq = state.next;
        state.next += 1;
        state.live.insert(seq);
        // Mirror the membership into the ledger under this same lock: the bound
        // follows the slowest LIVE numbered reader, so the ledger has to know who
        // is live, and it has to learn it in the same total order that numbers the
        // receiver and stamps the sends.
        if let Some(coverage) = &coverage {
            coverage.receiver_joined(seq);
        }
        drop(state);
        ChangesReceiver {
            inner: Some(inner),
            control,
            subscription: Some((Arc::downgrade(self), seq)),
            coverage,
        }
    }

    /// A receiver is gone: destroy it, unregister it, and retire every published
    /// backfill batch no remaining receiver can reach - all under one lock.
    ///
    /// The inner `broadcast::Receiver` is dropped HERE, before the sweep decides.
    /// Unregistering the number while the underlying receiver still existed left
    /// a window in which the producer published, `broadcast` counted the doomed
    /// receiver as a live one so the page stayed charged, and the receiver then
    /// finished dropping with no further sweep to reconsider it. Registration and
    /// destruction have to be ordered by the same mutex that orders sends.
    ///
    /// Returns how many were retired, for the caller's warning.
    fn depart(
        &self,
        seq: u64,
        inner: broadcast::Receiver<MultiplexerEvent>,
        coverage: Option<&crate::cursor::PendingCoverage>,
    ) -> usize {
        let mut state = self.guard();
        drop(inner);
        state.live.remove(&seq);
        // Recompute the charge for the departing sequence first. A page this
        // receiver read is settled by its departure; a page it never read stops
        // being charged to it, so a producer parked behind a slow reader that
        // walked away resumes instead of waiting out the attachment. The sweep
        // below then judges what no remaining receiver can acknowledge at all.
        if let Some(coverage) = coverage {
            coverage.receiver_departed(seq);
        }
        let min_live = state.min_live();
        // Swept while still holding the lock, so no concurrent subscribe can
        // make this answer stale and no concurrent publish can stamp a batch
        // against a numbering this sweep has not seen.
        let retired = coverage.map_or(0, |coverage| coverage.release_undelivered(min_live));
        drop(state);
        retired
    }

    /// Announce, on the account's own stream, that batches were retired because
    /// their only possible reader left.
    ///
    /// The same reconciliation instruction the LAG path delivers, and for the
    /// same reason: a consumer still attached to this account has just had
    /// unacknowledged batches abandoned out from under it, and a log line does
    /// not reach it. An overlapping replacement receiver gets this; one that
    /// subscribes later cannot, because it joins at the ring's tail - which is
    /// itself why those batches were retired.
    fn announce_retirement(&self, retired: usize) {
        let warning = bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::ChangeStreamLagged,
            format!(
                "a change stream was dropped holding {retired} unacknowledged batches; \
                 no live receiver can acknowledge them"
            ),
        )
        .with_next_action(bifrost_types::DiagnosticText::user_safe(
            "their coverage debt is carried forward; re-read from the last durable \
             checkpoint. Boundary waits (pause / checkpoint_now) stay usable, but the \
             checkpoint they report may predate the retired batches.",
        ));
        let _ = self.tx.send(MultiplexerEvent::unacked(
            CursorScope::Account,
            Arc::new(SyncEvent::Warning(warning)),
        ));
    }

    /// The lowest sequence still holding a receiver. Observability; used by
    /// tests.
    #[must_use]
    pub fn min_live(&self) -> Option<u64> {
        self.guard().min_live()
    }

    /// Mark the attachment as tearing down, ordered against the departure
    /// sweep.
    ///
    /// The flag lives on the ledger, but it is set HERE, under this lock,
    /// because the sweep runs under this lock from start to finish
    /// ([`Self::depart`]: retire, then `note_undelivered`, which records the
    /// loss AND inserts the discard request). A bare atomic store raced a
    /// sweep already admitted: it had recorded the watermark, was preempted
    /// before inserting the request, and `detach` then drained an empty
    /// request set and finished the writer - so a loss recorded BEFORE
    /// teardown, which the guarantee still covers, left rows a later attach
    /// could resume past. Taking the lock means every sweep that read `false`
    /// has completed, request included, before this returns.
    pub fn begin_teardown(&self, coverage: &crate::cursor::PendingCoverage) {
        let state = self.guard();
        coverage.begin_teardown();
        drop(state);
    }
}

impl ChangesReceiver {
    /// An UNNUMBERED receiver with whatever handles the test wants on it.
    /// Production receivers come from [`ChangeDelivery::subscribe`] (numbered)
    /// or [`ChangeDelivery::observe`] (unnumbered, no handles); this exists so a
    /// test can exercise one half of the wiring - the lag path's abandonment,
    /// say - without the other.
    #[cfg(test)]
    pub(crate) fn new(
        inner: broadcast::Receiver<MultiplexerEvent>,
        control: Option<crate::control::SyncControl>,
        coverage: Option<Arc<crate::cursor::PendingCoverage>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            control,
            subscription: None,
            coverage,
        }
    }

    pub async fn recv(&mut self) -> Result<MultiplexerEvent, broadcast::error::RecvError> {
        let Some(inner) = self.inner.as_mut() else {
            return Err(broadcast::error::RecvError::Closed);
        };
        match inner.recv().await {
            Ok(event) => {
                self.note_receipt(&event);
                Ok(event)
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => Ok(self.on_lag(skipped)),
            Err(error) => Err(error),
        }
    }

    pub fn try_recv(&mut self) -> Result<MultiplexerEvent, broadcast::error::TryRecvError> {
        let Some(inner) = self.inner.as_mut() else {
            return Err(broadcast::error::TryRecvError::Closed);
        };
        match inner.try_recv() {
            Ok(event) => {
                self.note_receipt(&event);
                Ok(event)
            }
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => Ok(self.on_lag(skipped)),
            Err(error) => Err(error),
        }
    }

    /// Record this receiver's read of the batch, so its backfill lane capacity
    /// comes back once every reader that could still be holding it has read it.
    ///
    /// Reading is the permit, not acknowledging: the bound exists to stop a cold
    /// start overrunning the shared broadcast ring, and a page every eligible
    /// consumer has taken delivery of is no longer occupying it. The consumer's
    /// durable write happens on whatever schedule it likes, and binding the
    /// producer to THAT deadlocked a consumer batching its acknowledgements
    /// against a producer parked on the bound.
    ///
    /// The receiver's own SEQUENCE goes with the receipt. Several numbered
    /// receivers are a supported shape, and a bound that lifted on the first read
    /// followed the fastest of them - so a fast observer freed the producer, the
    /// slower acknowledger fell a whole ring behind and lagged, and `on_lag`
    /// abandoned the walk's pages. The ledger judges the slowest live reader
    /// instead.
    ///
    /// NUMBERED receivers only. A receiver with no subscription is one the
    /// delivery gate never handed out and never sees depart, so it is by
    /// construction not the acknowledger; letting its read free the producer
    /// would let the walk run a whole ring ahead of whoever does acknowledge,
    /// which is exactly the overrun this bound prevents. The engine's own
    /// unnumbered readers, and the explicit observer subscription, are covered
    /// by that one condition rather than by a second rule per reader kind.
    ///
    /// Only backfill publications carry a charge, and this runs for EVERY event
    /// including every live-lane one, so the lane question is answered from the
    /// event's own checkpoint before the ledger lock is taken at all.
    fn note_receipt(&self, event: &MultiplexerEvent) {
        let Some((_, seq)) = &self.subscription else {
            return;
        };
        if !matches!(event.checkpoint, Some(Checkpoint::Backfill(_))) {
            return;
        }
        if let (Some(coverage), Some(publication)) = (&self.coverage, &event.publication) {
            coverage.mark_received(publication, *seq);
        }
    }

    fn on_lag(&self, skipped: u64) -> MultiplexerEvent {
        // An OBSERVER carries no control handle, and that is the whole of its
        // lag handling: the registrations belong to the acknowledger, and an
        // observer falling behind says nothing about whether the acknowledger
        // can still answer for them. Abandoning on an observer's lag voided
        // every in-flight walk on the account because a UI reader was slow.
        let Some(control) = &self.control else {
            return observer_lag_warning(skipped);
        };
        let abandoned = control.abandon_pending_checkpoints();
        // Nothing further is needed for the backfill bound: abandonment removes
        // the boundary registrations, and backfill capacity IS those
        // registrations, so a parked producer is already free. That is the whole
        // point of not keeping a second ledger - there is no second thing to
        // remember here.
        lag_warning(skipped, abandoned)
    }
}

fn observer_lag_warning(skipped: u64) -> MultiplexerEvent {
    let warning = bifrost_types::Warning::user_safe(
        bifrost_types::WarningKind::ChangeStreamLagged,
        format!("observer stream lagged and lost {skipped} batches"),
    )
    .with_next_action(bifrost_types::DiagnosticText::user_safe(
        "an observer never acknowledges, so nothing was abandoned and the acknowledging \
         receiver is unaffected; what this observer missed is gone from its own view only.",
    ));
    MultiplexerEvent::unacked(CursorScope::Account, Arc::new(SyncEvent::Warning(warning)))
}

fn lag_warning(skipped: u64, abandoned: usize) -> MultiplexerEvent {
    let warning = bifrost_types::Warning::user_safe(
        bifrost_types::WarningKind::ChangeStreamLagged,
        format!(
            "change stream lagged and lost {skipped} batches; reconcile from the last acknowledged checkpoint"
        ),
    )
    .with_next_action(bifrost_types::DiagnosticText::user_safe(format!(
        "{abandoned} outstanding checkpoint registrations were abandoned because their batches \
         can no longer be delivered; re-read from the last durable checkpoint. Boundary waits \
         (pause / checkpoint_now) stay usable, but the checkpoint they report may predate the \
         batches that were lost."
    )));
    MultiplexerEvent {
        scope: CursorScope::Account,
        event: Arc::new(SyncEvent::Warning(warning)),
        checkpoint: None,
        publication: None,
    }
}

/// Reopen request raised by per-scope drivers when a stream ends with
/// an account-side failure. The engine listens on the receiver and
/// dispatches according to the carried `AccountError`'s recovery.
///
/// `scope` is `Option<CursorScope>`: scope-bound directives
/// (`Engine(RestartScope)`, `Engine(DowngradeCapabilityForScope)`,
/// `Engine(DowngradeStrategy)` with `ErrorScope::Cursor`) carry
/// `Some(scope)`. Account-wide directives (`RestartAccount`,
/// `SchemaIncompatible`, `OperatorOverrideRequired`)
/// carry `None`. Workers must not paper over an account-wide directive
/// with the worker's own scope - passing `Some(arbitrary_scope)` would
/// mask the directive's account-wide intent.
///
/// `#[non_exhaustive]` is deliberate and stays: the enum is `pub` and
/// re-exported from `lib.rs`, so new request kinds must not break
/// downstream matches. Adjudicated during the error-model close-out -
/// do not re-raise.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReopenRequest {
    /// Account error observed at the scope's stream terminus.
    Recovery {
        scope: Option<CursorScope>,
        error: AccountError,
    },
    /// A provider lifecycle event deleted this scope's folder. The engine
    /// must purge the scope's durable rows - the change cursor AND the
    /// backfill rows, completion marker included. The consumer plausibly
    /// purged the folder's data on `Deleted`, so a completion marker
    /// surviving into a folder recreated under the same id would skip the
    /// new incarnation's entire cold-start walk: data invisibility, not
    /// just a leaked row.
    ScopeDeleted { scope: CursorScope },
}

/// Upper bound on the lifecycle reader's wait for a replacement connection.
///
/// Only a successful account reattach bumps the account generation. A reopen
/// that exhausts its retry budget, or that is queued behind a pause, produces
/// no generation change at all, so an unbounded wait would park the reader for
/// the process lifetime and silently stop folder/label lifecycle observation.
const LIFECYCLE_REOPEN_WAIT: Duration = Duration::from_secs(30);

/// Lifecycle-stream termination policy.
///
/// A terminal account error cannot be repaired by reconnecting the same dead
/// stream. An `Engine(RestartAccount)` directive replaces the connection this
/// reader is holding, so it hands the error over once and waits (bounded) for
/// the generation change rather than hammering a dead handle. Every other
/// directive - `RestartScope`, `DisableScope`, `SchemaIncompatible`,
/// `OperatorOverrideRequired`, the downgrades - is handled without swapping
/// the account, so no generation change is ever coming and the reader must
/// reconnect on its own backoff after handing the error over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleTermination {
    /// Forward once, then wait (bounded) for a replacement connection.
    AwaitReopen,
    /// Forward once, then reconnect this reader on backoff.
    ForwardAndReconnect,
    /// Reconnect on backoff; the engine has nothing to decide.
    Reconnect,
    /// Report once and stop this reader.
    Terminate,
}

// Lifecycle polling can normally be minutes apart. A failed first read must
// not turn that into a 50 ms request loop. Only observed lifecycle progress or
// a replacement account resets this backoff; opening a stream proves nothing.
const LIFECYCLE_RECONNECT_MIN: Duration = Duration::from_secs(30);
const LIFECYCLE_RECONNECT_MAX: Duration = Duration::from_secs(480);

fn lifecycle_retry_wait(
    error: &AccountError,
    fallback: Duration,
    throttles: &StdMutex<crate::recovery::ThrottleBucket>,
    account_id: &AccountId,
) -> Duration {
    use crate::recovery::{RecoveryPlan, plan_recovery};
    let now = std::time::SystemTime::now();
    let hint = match plan_recovery(error.clone()) {
        RecoveryPlan::Retry(advice) => {
            crate::recovery::record_throttle(throttles, account_id, &advice, error);
            // Account requests own token refresh through their transport. There
            // is no engine token source to refresh here; reconnect retries the
            // read through that same authenticated request funnel.
            crate::recovery::retry_delay(&advice, now, fallback)
        }
        RecoveryPlan::Reconcile(advice) => {
            crate::recovery::record_reconcile_throttle(throttles, account_id, &advice, error);
            // Lifecycle reads have no mutation target to reconcile. Retry the
            // uncommitted read, preserving its provider-owned diff baseline.
            crate::recovery::reconcile_delay(&advice, now, fallback)
        }
        RecoveryPlan::Engine(_) | RecoveryPlan::Terminal(_) => fallback,
    };
    fallback.max(hint)
}

fn lifecycle_termination(error: &AccountError) -> LifecycleTermination {
    use crate::recovery::{RecoveryPlan, plan_recovery};
    use bifrost_types::EngineDirective;

    match plan_recovery(error.clone()) {
        RecoveryPlan::Engine(EngineDirective::RestartAccount) => LifecycleTermination::AwaitReopen,
        RecoveryPlan::Engine(_) => LifecycleTermination::ForwardAndReconnect,
        RecoveryPlan::Terminal(_) => LifecycleTermination::Terminate,
        RecoveryPlan::Retry(_) | RecoveryPlan::Reconcile(_) => LifecycleTermination::Reconnect,
    }
}

/// A per-scope poll task's cancellation token, tagged with the
/// identity of the task that owns it.
///
/// The generation is what makes cleanup safe. A task removing its
/// entry by key alone can evict a SUCCESSOR's token: task A exits
/// naturally while its scope is momentarily absent from the registry
/// (mid-`restart_scope`), the 1s scan in `Multiplexer::run` sees a
/// re-established cursor with no live token and spawns task B, and
/// A's cleanup then deletes B's entry. The next scan sees no token and
/// spawns C, leaving B and C polling the same scope forever - doubled
/// wire traffic, doubled broadcasts, and two concurrent producers on
/// one lane+scope, which is exactly what `SyncControl`'s checkpoint
/// supersession assumes cannot happen.
#[derive(Debug, Clone)]
pub struct ScopeToken {
    generation: u64,
    token: CancellationToken,
    /// The poll task exited on a terminal verdict and left this entry behind
    /// as a tombstone. The 1s scan reads any present entry as "owned", but a
    /// tombstone is not an owner - it is a record that the scope is DEAD - so
    /// paths that drive a scope on their own initiative (the push reconciler)
    /// must be able to tell the two apart.
    parked: bool,
    drive_generation: Option<crate::cursor::DriveGeneration>,
}

impl ScopeToken {
    /// A terminal tombstone for `scope_tokens`: no owner, uncancelled, and
    /// marked parked, which is exactly the state a poll task leaves behind
    /// when its recovery verdict was `Terminal`. Both the 1s poll scan and
    /// the push reconciler refuse to drive a scope holding one.
    ///
    /// Published because `ScopeTokens` is published while every field of an
    /// entry is private, so nothing outside this module could otherwise place
    /// an entry in the map at all.
    #[must_use]
    pub fn tombstone() -> Self {
        Self {
            generation: SCOPE_TOKEN_GENERATION.fetch_add(1, Ordering::Relaxed),
            token: CancellationToken::new(),
            parked: true,
            drive_generation: None,
        }
    }

    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    fn cancel(&self) {
        self.token.cancel();
    }
}

/// Per-scope poll tokens, keyed by the scope the task polls.
pub type ScopeTokens = Arc<StdMutex<HashMap<CursorScope, ScopeToken>>>;

/// Monotonic source of `ScopeToken::generation`. Process-wide rather
/// than per-multiplexer: the value only has to be unique, and threading
/// another `Arc` through the poll-spawn argument list buys nothing.
static SCOPE_TOKEN_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Multiplexer task state. Constructed in `engine::attach` and run on
/// a `tokio::spawn`.
pub struct Multiplexer {
    pub account_id: AccountId,
    pub account: Arc<ArcSwap<Arc<dyn Account>>>,
    pub account_generation: watch::Receiver<u64>,
    pub cursors: Arc<CursorRegistry>,
    pub config: MultiplexerConfig,
    pub boundary: BoundaryView,
    /// The account's change delivery gate: the broadcast sender plus the
    /// numbering of the receivers that may acknowledge. Every checkpoint the
    /// poll loop publishes goes through it, so "did a numbered receiver take
    /// delivery" is decided under its lock rather than read off a count.
    pub delivery: Arc<ChangeDelivery>,
    pub control: SyncControl,
    pub shutdown: CancellationToken,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    /// Per-scope cancellation tokens keyed by membership-id so
    /// `ScopeLifecycle::Deleted` can stop the matching poll task.
    pub scope_tokens: ScopeTokens,
    /// Engine-wide throttle bucket. Poll tasks consult it before each
    /// drive so a tenant- or account-wide `Retry-After` recorded by one
    /// scope (or a sibling account) pauses the others; the Retry arm
    /// records deadlines it observes.
    pub throttles: Arc<StdMutex<crate::recovery::ThrottleBucket>>,
    pub scheduler: crate::scheduler::Scheduler,
}

impl Multiplexer {
    /// Apply the adaptive cadence rule to a scope: halve on change,
    /// double after five consecutive no-change ticks, clamped to
    /// `[poll_min, poll_max]`. Pure function; tested below.
    #[must_use]
    pub fn updated_cadence(
        cur: AdaptiveCadence,
        seen_change: bool,
        min: Duration,
        max: Duration,
    ) -> AdaptiveCadence {
        if seen_change {
            AdaptiveCadence {
                interval: (cur.interval / 2).max(min),
                no_change_streak: 0,
            }
        } else {
            let new_streak = cur.no_change_streak.saturating_add(1);
            let interval = if new_streak >= 5 {
                cur.interval.saturating_mul(2).min(max)
            } else {
                cur.interval
            };
            AdaptiveCadence {
                interval,
                no_change_streak: if new_streak >= 5 { 0 } else { new_streak },
            }
        }
    }

    /// Bookkeeping helper: register a cursor in the registry. Idempotent.
    pub fn register_cursor(&self, cursor: ChangeCursor) {
        self.cursors.put(cursor);
    }

    /// Run the multiplexer loop until the shutdown token is cancelled.
    ///
    /// Spawns per-scope poll tasks (each driving
    /// `changes_stream(cursor)` with adaptive cadence) and a single
    /// `scope_lifecycle_stream` task. New scopes added at runtime get
    /// their own poll task via `ScopeLifecycle::Created`; deleted
    /// scopes have their per-scope cancellation token tripped and the
    /// cursor entry dropped from the registry.
    pub async fn run(self) {
        let Self {
            account_id,
            account,
            account_generation,
            cursors,
            config,
            boundary,
            delivery,
            control,
            shutdown,
            reopen_tx,
            scope_tokens,
            throttles,
            scheduler,
        } = self;

        // Snapshot the known scopes from the registry; the engine has
        // populated it via `establish_one` before spawning us.
        spawn_missing_scope_polls(
            account_id.clone(),
            Arc::clone(&account),
            Arc::clone(&cursors),
            config,
            boundary.clone(),
            Arc::clone(&delivery),
            control.clone(),
            shutdown.clone(),
            reopen_tx.clone(),
            Arc::clone(&scope_tokens),
            Arc::clone(&throttles),
            scheduler.clone(),
        );

        // Drive scope_lifecycle in the background. `Created` asks the
        // engine to establish a cursor; the scan loop below notices it
        // and starts polling. `Deleted` cancels and drops the matching
        // scope; `Renamed` is delete+create on a fresh CursorScope id.
        let lifecycle_account = Arc::clone(&account);
        let lifecycle_shutdown = shutdown.clone();
        let lifecycle_cursors = Arc::clone(&cursors);
        let lifecycle_tokens = Arc::clone(&scope_tokens);
        let lifecycle_reopen = reopen_tx.clone();
        let lifecycle_changes = delivery.sender().clone();
        let mut lifecycle_generation = account_generation;
        let lifecycle_throttles = Arc::clone(&throttles);
        let lifecycle_account_id = account_id.clone();
        let lifecycle_handle = tokio::spawn(async move {
            let mut reconnect_delay = LIFECYCLE_RECONNECT_MIN;
            loop {
                // Recheck shared holds after every wake; sibling work can
                // extend one while this reader is waiting.
                if let Some(wait) = crate::recovery::account_throttle_wait(
                    &lifecycle_throttles,
                    &lifecycle_account_id,
                ) {
                    tokio::select! {
                        () = lifecycle_shutdown.cancelled() => return,
                        changed = lifecycle_generation.changed() => {
                            if changed.is_err() { return; }
                            reconnect_delay = LIFECYCLE_RECONNECT_MIN;
                        }
                        () = tokio::time::sleep(wait) => {}
                    }
                    continue;
                }
                let acc = lifecycle_account.load_full();
                let mut stream = acc.scope_lifecycle_stream();
                let mut retry_wait = Duration::ZERO;
                let mut reopened = false;
                loop {
                    tokio::select! {
                            () = lifecycle_shutdown.cancelled() => return,
                            changed = lifecycle_generation.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                reopened = true;
                                break;
                            }
                            next = stream.next() => {
                            let Some(ev) = next else { break; };
                            if matches!(
                                &ev,
                                bifrost_types::ScopeLifecycleEvent::Lifecycle(_)
                            ) {
                                reconnect_delay = LIFECYCLE_RECONNECT_MIN;
                            }
                            let lifecycle = match ev {
                                bifrost_types::ScopeLifecycleEvent::Lifecycle(lc) => lc,
                                bifrost_types::ScopeLifecycleEvent::Terminated(err) => {
                                    let policy = lifecycle_termination(&err);
                                    retry_wait = lifecycle_retry_wait(
                                        &err, reconnect_delay, &lifecycle_throttles,
                                        &lifecycle_account_id,
                                    );
                                    match policy {
                                        LifecycleTermination::AwaitReopen
                                        | LifecycleTermination::ForwardAndReconnect => {
                                            // Hand the classified error to the
                                            // engine exactly once. The reopen
                                            // path owns the three-strike
                                            // budget, so one dead lifecycle
                                            // stream cannot enqueue unbounded
                                            // account reopens.
                                            if lifecycle_reopen
                                                .send(ReopenRequest::Recovery {
                                                    scope: None,
                                                    error: err,
                                                })
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                            if policy == LifecycleTermination::AwaitReopen {
                                                // A replacement connection can
                                                // revive this stream, so prefer
                                                // waiting for it - but bound the
                                                // wait, because a failed or
                                                // pause-queued reopen never
                                                // bumps the generation and this
                                                // reader must not park forever.
                                                tokio::select! {
                                                    () = lifecycle_shutdown.cancelled() => return,
                                                    changed = lifecycle_generation.changed() => {
                                                        if changed.is_err() {
                                                            return;
                                                        }
                                                        reopened = true;
                                                    }
                                                    () = tokio::time::sleep(LIFECYCLE_REOPEN_WAIT) => {}
                                                }
                                            }
                                            break;
                                        }
                                        LifecycleTermination::Terminate => {
                                            // Do not retry this failed handle. An explicit
                                            // reattach can repair credentials or policy, so
                                            // keep the reader alive to observe its generation.
                                            let _ = lifecycle_changes.send(MultiplexerEvent {
                                                scope: CursorScope::Account,
                                                event: Arc::new(SyncEvent::Terminated(err)),
                                                checkpoint: None,
                                                publication: None,
                                            });
                                            tokio::select! {
                                                () = lifecycle_shutdown.cancelled() => return,
                                                changed = lifecycle_generation.changed() => {
                                                    if changed.is_err() { return; }
                                                    reopened = true;
                                                }
                                            }
                                            break;
                                        }
                                        LifecycleTermination::Reconnect => break,
                                    }
                                }
                                _ => continue,
                            };
                            apply_lifecycle_transition(
                                &lifecycle_cursors,
                                &lifecycle_tokens,
                                &lifecycle_reopen,
                                lifecycle,
                            )
                            .await;
                        }
                    }
                }
                if reopened {
                    reconnect_delay = LIFECYCLE_RECONNECT_MIN;
                    continue;
                }
                tokio::select! {
                    () = lifecycle_shutdown.cancelled() => return,
                    changed = lifecycle_generation.changed() => {
                        if changed.is_err() { return; }
                        reconnect_delay = LIFECYCLE_RECONNECT_MIN;
                        continue;
                    }
                    () = tokio::time::sleep(reconnect_delay.max(retry_wait)) => {}
                }
                reconnect_delay = reconnect_delay
                    .saturating_mul(2)
                    .min(LIFECYCLE_RECONNECT_MAX);
            }
        });

        // Park until shutdown while periodically noticing cursors
        // established by deferred inventory or lifecycle recovery.
        let mut scan = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                _ = scan.tick() => {
                    spawn_missing_scope_polls(
                        account_id.clone(),
                        Arc::clone(&account),
                        Arc::clone(&cursors),
                        config,
                        boundary.clone(),
                        Arc::clone(&delivery),
                        control.clone(),
                        shutdown.clone(),
                        reopen_tx.clone(),
                        Arc::clone(&scope_tokens),
                        Arc::clone(&throttles),
                        scheduler.clone(),
                    );
                }
            }
        }

        // Cancel every tracked scope token so per-scope polls exit
        // cleanly. Tokens are dropped as part of the map clear.
        let drained: Vec<ScopeToken> = {
            let mut g = scope_tokens.lock().expect("poisoned");
            g.drain().map(|(_, t)| t).collect()
        };
        for token in drained {
            token.cancel();
        }
        lifecycle_handle.abort();
    }

    /// Apply an inventory-fusion `Done`: only the terminal `Done` of
    /// an `EstablishViaInventory` scope carries the cursor that should
    /// be registered. Mid-inventory `Batch` checkpoints are ignored.
    pub fn fuse_inventory_done(
        &self,
        scope: &CursorScope,
        done: Option<Checkpoint>,
    ) -> Result<(), Error> {
        match done {
            Some(Checkpoint::Change(c)) if &c.scope == scope => {
                // This cursor was just minted by Account, not decoded from disk.
                c.validate_envelope().map_err(|_| {
                    Error::Account(crate::recovery::fresh_cursor_contract_error(
                        Some(&Checkpoint::Change(c.clone())),
                        bifrost_types::AccountOperation::SyncInventory,
                        scope,
                    ))
                })?;
                self.cursors.put(c);
                Ok(())
            }
            Some(Checkpoint::Change(_)) => Err(Error::Other(
                "inventory Done carried a Change checkpoint for the wrong scope".into(),
            )),
            Some(Checkpoint::Backfill(_)) => Err(Error::Other(
                "inventory Done carried a Backfill checkpoint; expected Change".into(),
            )),
            // `Checkpoint` is `#[non_exhaustive]`; reject unknown
            // variants so the engine fails closed.
            Some(_) => Err(Error::Other(
                "inventory Done carried an unknown Checkpoint variant".into(),
            )),
            None => Err(Error::Other(
                "inventory Done carried no checkpoint for EstablishViaInventory scope".into(),
            )),
        }
    }
}

/// Spawn one polling task for a single scope and track its
/// cancellation token in `scope_tokens` so lifecycle events can stop
/// it later.
#[allow(clippy::too_many_arguments)]
fn spawn_and_track_scope_poll(
    account_id: AccountId,
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    config: MultiplexerConfig,
    boundary: BoundaryView,
    delivery: Arc<ChangeDelivery>,
    control: SyncControl,
    shutdown: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    scope_tokens: ScopeTokens,
    throttles: Arc<StdMutex<crate::recovery::ThrottleBucket>>,
    scheduler: crate::scheduler::Scheduler,
    scope: CursorScope,
) {
    let scope_cancel = shutdown.child_token();
    let generation = SCOPE_TOKEN_GENERATION.fetch_add(1, Ordering::Relaxed);
    let drive_generation = cursors.drive_generation(&scope);
    scope_tokens.lock().expect("poisoned").insert(
        scope.clone(),
        ScopeToken {
            generation,
            token: scope_cancel.clone(),
            parked: false,
            drive_generation: Some(drive_generation),
        },
    );
    let cleanup_tokens = Arc::clone(&scope_tokens);
    let cleanup_scope = scope.clone();
    tokio::spawn(async move {
        let exit = spawn_scope_poll_inner(
            account_id,
            account,
            cursors,
            config,
            boundary,
            delivery,
            control,
            shutdown,
            scope_cancel,
            reopen_tx,
            throttles,
            scheduler,
            scope,
        )
        .await;
        // A terminally failed scope leaves its (uncancelled) token in the
        // map as a tombstone: the 1s scan sees a live entry and never
        // respawns the poll, so a terminal error really is terminal instead
        // of a once-per-second re-drive. The scope-retiring paths still clear
        // the entry, so a scope that is deleted-and-recreated respawns cleanly.
        //
        // Registry restart paths need not edit this map: discovery and push
        // dispatch retire entries bound to an obsolete drive generation. A late
        // terminal result can only park its original ownership generation.
        match exit {
            PollExit::Retire => retire_scope_token(&cleanup_tokens, &cleanup_scope, generation),
            // Mark the tombstone as such, by identity, so the push reconciler
            // can refuse to re-drive a terminally parked scope. Without the
            // mark a later hint drove the scope again - the milder sibling of
            // the once-per-second respawn loop the park exists to stop.
            PollExit::Park => park_scope_token(&cleanup_tokens, &cleanup_scope, generation),
        }
    });
}

/// Drop an exiting poll task's registration, by identity.
///
/// Removing by key alone is what produces the duplicate-poll
/// interleaving described on [`ScopeToken`]: the entry under this scope
/// may already belong to a successor spawned by the 1s scan while this
/// task was on its way out, and evicting it leaves that successor
/// untracked, so the next scan spawns a second live poll for the same
/// scope.
fn retire_scope_token(tokens: &ScopeTokens, scope: &CursorScope, generation: u64) {
    let mut g = tokens.lock().expect("poisoned");
    if matches!(g.get(scope), Some(entry) if entry.generation == generation) {
        g.remove(scope);
    }
}

/// Mark an exiting task's registration as a terminal tombstone, by identity.
fn park_scope_token(tokens: &ScopeTokens, scope: &CursorScope, generation: u64) {
    let mut g = tokens.lock().expect("poisoned");
    if let Some(entry) = g.get_mut(scope)
        && entry.generation == generation
    {
        entry.parked = true;
    }
}

/// Stop the poll owner and leave a terminal tombstone for a push drive.
///
/// A push reconcile can reach a terminal recovery verdict while the poll
/// worker is asleep on cadence. Cancelling that owner alone would make the
/// one-second discovery loop respawn it, while merely marking it would leave
/// it alive to poll again. Replace the matching registration with a fresh
/// tombstone bound to the drive generation; the cancelled owner's cleanup is
/// generation-checked and cannot remove the replacement.
pub(crate) fn park_scope_for_drive(
    tokens: &ScopeTokens,
    cursors: &CursorRegistry,
    scope: &CursorScope,
    drive_generation: crate::cursor::DriveGeneration,
) {
    if cursors.drive_generation(scope) != drive_generation {
        return;
    }
    let mut entries = tokens.lock().expect("poisoned");
    if entries.get(scope).is_some_and(|entry| {
        entry
            .drive_generation
            .is_some_and(|generation| generation != drive_generation)
    }) {
        return;
    }
    if let Some(owner) = entries.remove(scope) {
        owner.cancel();
    }
    let mut tombstone = ScopeToken::tombstone();
    tombstone.drive_generation = Some(drive_generation);
    entries.insert(scope.clone(), tombstone);
}

/// Whether this scope is a terminal tombstone: a poll task exited on a
/// terminal recovery verdict and nothing has cleared the entry since.
///
/// Reading `None` as "not parked" is the safe default - an unknown scope is
/// one nothing has terminated - and so is an ACTIVE token, whose owner is
/// polling it normally.
#[must_use]
pub fn scope_is_parked(tokens: &ScopeTokens, scope: &CursorScope) -> bool {
    tokens
        .lock()
        .expect("poisoned")
        .get(scope)
        .is_some_and(|entry| entry.parked)
}

/// Ownership and terminal state belong to the registry drive incarnation.
/// Comparing the existing fence covers both scope restart and reattach without
/// requiring every recovery caller to remember a second reset operation. Merely
/// clearing on account replacement would leave scope-only restarts stranded.
/// Keeping task cleanup identity separate prevents an old task retiring its successor.
pub(crate) fn retire_stale_scope_token(
    tokens: &ScopeTokens,
    cursors: &CursorRegistry,
    scope: &CursorScope,
) {
    let current = cursors.drive_generation(scope);
    let mut entries = tokens.lock().expect("poisoned");
    if entries.get(scope).is_some_and(|entry| {
        entry
            .drive_generation
            .is_some_and(|generation| generation != current)
    }) && let Some(entry) = entries.remove(scope)
    {
        entry.cancel();
    }
}

pub(crate) fn cancel_scope_token(tokens: &ScopeTokens, scope: &CursorScope) {
    let current = tokens.lock().expect("poisoned").get(scope).cloned();
    if let Some(entry) = current {
        entry.token.cancel();
        retire_scope_token(tokens, scope, entry.generation);
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_missing_scope_polls(
    account_id: AccountId,
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    config: MultiplexerConfig,
    boundary: BoundaryView,
    delivery: Arc<ChangeDelivery>,
    control: SyncControl,
    shutdown: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    scope_tokens: ScopeTokens,
    throttles: Arc<StdMutex<crate::recovery::ThrottleBucket>>,
    scheduler: crate::scheduler::Scheduler,
) {
    for scope in cursors.all_scopes() {
        retire_stale_scope_token(&scope_tokens, &cursors, &scope);
        let should_spawn = {
            let mut g = scope_tokens.lock().expect("poisoned");
            match g.get(&scope) {
                Some(token) if !token.is_cancelled() => false,
                Some(_) => {
                    g.remove(&scope);
                    true
                }
                None => true,
            }
        };
        if should_spawn {
            spawn_and_track_scope_poll(
                account_id.clone(),
                Arc::clone(&account),
                Arc::clone(&cursors),
                config,
                boundary.clone(),
                Arc::clone(&delivery),
                control.clone(),
                shutdown.clone(),
                reopen_tx.clone(),
                Arc::clone(&scope_tokens),
                Arc::clone(&throttles),
                scheduler.clone(),
                scope,
            );
        }
    }
}

/// Per-scope poll loop. Drives `changes_stream(cursor)` and sleeps
/// for the scope's adaptive cadence between passes. Stream-end
/// recovery actions are propagated to the engine via `ReopenRequest`
/// with the originating `AccountError`.
#[allow(clippy::too_many_arguments)]
async fn spawn_scope_poll_inner(
    account_id: AccountId,
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    config: MultiplexerConfig,
    mut boundary: BoundaryView,
    delivery: Arc<ChangeDelivery>,
    control: SyncControl,
    shutdown: CancellationToken,
    scope_cancel: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    throttles: Arc<StdMutex<crate::recovery::ThrottleBucket>>,
    scheduler: crate::scheduler::Scheduler,
    scope: CursorScope,
) -> PollExit {
    let mut cadence = AdaptiveCadence {
        interval: config.poll_initial,
        no_change_streak: 0,
    };
    loop {
        if shutdown.is_cancelled() || scope_cancel.is_cancelled() {
            return PollExit::Retire;
        }
        // If the boundary is asking us to pause, park here until it
        // changes back to Run (or Stop / Shutdown trips).
        if matches!(boundary.peek(), crate::cancel::BoundaryRequest::Pause) {
            // Park until the request changes.
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return PollExit::Retire,
                    () = scope_cancel.cancelled() => return PollExit::Retire,
                    next = boundary.changed() => {
                        match next {
                            None => return PollExit::Retire,
                            Some(crate::cancel::BoundaryRequest::Pause) => continue,
                            Some(_) => break,
                        }
                    }
                }
            }
        }
        // Honor any throttle deadline this scope's drive consumes - the
        // account-wide holds plus the keys the account declares for the
        // scope (a mailbox budget a sibling scope or a sibling account
        // recorded) - before driving the wire. Re-checked after waking: a
        // longer deadline can land while this scope sleeps off the first one.
        //
        // The declaration is re-asked after every wake: a reattach can swap in
        // an account that declares a different budget, and waiting on the
        // retired one's keys could hold this scope for as long as a sibling
        // keeps extending a budget it no longer consumes.
        let declared_now = || {
            crate::recovery::declared_keys(
                account.load_full().as_ref().as_ref(),
                &account_id,
                bifrost_types::ThrottleWork::Scope(&scope),
            )
        };
        while let Some(wait) =
            crate::recovery::throttle_wait(&throttles, &account_id, &declared_now())
        {
            tracing::debug!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                wait_secs = wait.as_secs(),
                "poll deferred by shared throttle deadline"
            );
            tokio::select! {
                () = shutdown.cancelled() => return PollExit::Retire,
                () = scope_cancel.cancelled() => return PollExit::Retire,
                () = tokio::time::sleep(wait) => {}
            }
        }
        let _admission = tokio::select! {
            () = shutdown.cancelled() => return PollExit::Retire,
            () = scope_cancel.cancelled() => return PollExit::Retire,
            permit = scheduler.admit(
                account_id.clone(),
                control.priority_snapshot(),
                crate::scheduler::WorkKind::Sync,
            ) => match permit {
                Ok(permit) => permit,
                Err(error) => {
                    // A refused admission is transient (a full lane, or a
                    // scheduler shutting down). Exiting here would retire
                    // the scope's poll loop permanently for a condition
                    // that clears on its own, so back off one cadence
                    // step and re-enter instead.
                    tracing::warn!(
                        target: "bifrost.sync.scheduler",
                        scope = ?scope,
                        %error,
                        "poll admission refused; retrying at the next cadence"
                    );
                    tokio::select! {
                        () = shutdown.cancelled() => return PollExit::Retire,
                        () = scope_cancel.cancelled() => return PollExit::Retire,
                        () = tokio::time::sleep(cadence.interval) => {}
                    }
                    continue;
                }
            },
        };
        // Admission can park behind other work, and a deadline recorded while
        // it parked existed before this drive started. Re-check with the
        // permit in hand; on a hold, release it (no permit is held across a
        // throttle wait) and go back round to the wait above.
        if crate::recovery::throttle_wait(&throttles, &account_id, &declared_now()).is_some() {
            continue;
        }
        // The drive's OWN boundary arm answers `Stop` and `Pause`, which is
        // what a detach publishes. This outer select answers the two signals
        // the boundary cannot carry: scope deletion or restart, which cancels
        // `scope_cancel` while the ACCOUNT boundary stays `Run`, and root
        // cancellation, which lands before this account's own detach writes
        // `Stop`. The generation fence stops the stale publication but does not
        // release the lease, so without a cancellation arm a stalled drive
        // keeps it and the replacement incarnation waits behind it on
        // `claim_drive` indefinitely. The registry's own per-scope token inside
        // `with_drive` is the general answer to that (it covers the restart
        // paths and the reconciler, neither of which has an entry in
        // `ScopeTokens`); this arm remains because it also retires the poll
        // task itself, which the registry knows nothing about.
        //
        // Safe to cut here precisely because it cannot cut anywhere
        // interesting: `with_drive` suspends only at `claim_drive` and at the
        // drive itself, the drive suspends only at its stream poll, and a
        // select cannot destroy a branch mid-poll - so the page's registration
        // and its broadcast, which are synchronous, cannot be split. Dropping
        // the future releases the lease, the activity guard and the stream.
        let driven = tokio::select! {
            () = shutdown.cancelled() => return PollExit::Retire,
            () = scope_cancel.cancelled() => return PollExit::Retire,
            driven = cursors
            .with_drive(&scope, |cursor, registry_generation| {
                let pre_advance_state = cursor.server_state.bytes.clone();
                let acc_arc = account.load_full();
                let drive_cursors = Arc::clone(&cursors);
                let advance_cursors = Arc::clone(&cursors);
                let advance_scope = scope.clone();
                let scope = scope.clone();
                let delivery = Arc::clone(&delivery);
                let boundary = boundary.clone();
                let control = control.clone();
                let throttles = Arc::clone(&throttles);
                let account_id = account_id.clone();
                async move {
                    // The LAST check before the wire. `with_drive` can park on
                    // the drive lease after admission, and a hold recorded
                    // while it parked predates this drive. A held drive
                    // returns without driving; the loop goes back round,
                    // releasing the lease and the permit before it waits.
                    //
                    // The keys are asked of `acc_arc`, the very account this
                    // drive runs against: a reopen during the waits above can
                    // install an account whose declaration differs, and keys
                    // cached from the old one would check the wrong budget.
                    let acc: &dyn Account = acc_arc.as_ref().as_ref();
                    let final_keys = crate::recovery::declared_keys(
                        acc,
                        &account_id,
                        bifrost_types::ThrottleWork::Scope(&scope),
                    );
                    if crate::recovery::throttle_wait(&throttles, &account_id, &final_keys)
                        .is_some()
                    {
                        return None;
                    }
                    let outcome = drive_changes_stream(
                        acc,
                        scope,
                        cursor,
                        drive_cursors,
                        delivery,
                        boundary,
                        Some(control),
                        Some(registry_generation),
                    )
                    .await;
                    // Read the post-drive cursor while the lease is still
                    // held. The lease no longer covers the tail of the poll
                    // iteration, so a push reconcile can advance this scope
                    // the instant we return; comparing out there would credit
                    // this drive with the reconciler's progress and hold the
                    // cadence at `poll_min` on a scope this loop never moved.
                    let advanced = advance_cursors
                        .snapshot(&advance_scope)
                        .is_some_and(|c| c.server_state.bytes != pre_advance_state);
                    Some((advanced, outcome))
                }
            }) => driven,
        };
        drop(_admission);
        let Some(driven) = driven else {
            // The scope left the registry - either before the drive started or
            // while it ran, `with_drive` reporting both the same way. Exit
            // cleanly; if the scope is re-established the 1s scan respawns us.
            return PollExit::Retire;
        };
        let Some((advanced, outcome)) = driven else {
            // Held at the last check: wait it out at the top of the loop.
            continue;
        };
        let recovered = handle_drive_outcome(
            &scope,
            outcome,
            advanced,
            &reopen_tx,
            &account_id,
            &throttles,
            &shutdown,
            &scope_cancel,
        )
        .await;

        cadence = Multiplexer::updated_cadence(
            cadence,
            recovered.advanced,
            config.poll_min,
            config.poll_max,
        );
        if let Some(exit) = recovered.exit {
            return exit;
        }
        if matches!(
            boundary.peek(),
            crate::cancel::BoundaryRequest::CheckpointNow
        ) {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return PollExit::Retire,
                    () = scope_cancel.cancelled() => return PollExit::Retire,
                    next = boundary.changed() => {
                        match next {
                            None | Some(crate::cancel::BoundaryRequest::Stop) => {
                                return PollExit::Retire;
                            }
                            Some(crate::cancel::BoundaryRequest::CheckpointNow) => continue,
                            Some(crate::cancel::BoundaryRequest::Pause) => break,
                            Some(crate::cancel::BoundaryRequest::Run) => break,
                        }
                    }
                }
            }
            if matches!(boundary.peek(), crate::cancel::BoundaryRequest::Pause) {
                continue;
            }
        }
        tokio::select! {
            () = shutdown.cancelled() => return PollExit::Retire,
            () = scope_cancel.cancelled() => return PollExit::Retire,
            () = tokio::time::sleep(cadence.interval) => {}
        }
    }
}

/// How an exiting per-scope poll task leaves its `ScopeToken` entry.
///
/// `Retire` removes the entry so the 1s scan can respawn the scope when
/// its cursor is (still or again) present. `Park` deliberately leaves the
/// uncancelled entry behind as a tombstone: the scan reads a live token
/// as "this scope is owned" and never respawns it, which is how a
/// terminal recovery verdict stays terminal. Only an explicit
/// scope-retiring path (`cancel_scope_token`, multiplexer shutdown)
/// clears a parked entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollExit {
    Retire,
    Park,
}

struct DriveRecovery {
    advanced: bool,
    exit: Option<PollExit>,
}

/// A recovery delay cut short by detach or scope deletion. The scope is left
/// RETIRED, never parked: the token is being cleared anyway, and parking would
/// leave a tombstone behind for a scope nobody asked to keep.
fn retired() -> DriveRecovery {
    DriveRecovery {
        advanced: false,
        exit: Some(PollExit::Retire),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_drive_outcome(
    scope: &CursorScope,
    outcome: Result<ChangesEvent, Error>,
    // `advanced`: whether this drive moved the scope's cursor, measured by
    // the caller while it still held the drive lease. Deliberately not
    // re-read here - the lease is already released by the time this runs, so
    // a concurrent push reconcile could be credited to this drive.
    advanced: bool,
    reopen_tx: &mpsc::Sender<ReopenRequest>,
    account_id: &AccountId,
    throttles: &StdMutex<crate::recovery::ThrottleBucket>,
    // Detach and scope deletion must interrupt a recovery delay, not wait it
    // out. A provider `Retry-After` of minutes otherwise kept a deleted scope
    // or a detaching account parked for the full hint, and detach then burned
    // toward `detach_timeout` and aborted the task instead of draining it.
    shutdown: &CancellationToken,
    scope_cancel: &CancellationToken,
) -> DriveRecovery {
    // Account errors already carry the complete recovery verdict. Normalize
    // them onto the same path as an account-authored Terminated event instead
    // of letting the generic engine-error arm silently choose "retry".
    let outcome = match outcome {
        Err(Error::Account(error)) => Ok(ChangesEvent::Terminated(error)),
        other => other,
    };
    match outcome {
        Ok(ChangesEvent::Advanced | ChangesEvent::Done) => DriveRecovery {
            advanced,
            exit: None,
        },
        Ok(ChangesEvent::Stopped) => DriveRecovery {
            advanced: false,
            exit: Some(PollExit::Retire),
        },
        // Pause is a temporary park; the poll loop's boundary check picks it
        // up at the top of the next iteration.
        Ok(ChangesEvent::Paused) => DriveRecovery {
            advanced,
            exit: None,
        },
        Ok(ChangesEvent::Terminated(error)) => {
            use crate::recovery::{
                RecoveryPlan, directive_target_scope, plan_recovery, retry_delay,
            };
            // Dispatch via `plan_recovery` so the closed `RecoveryPlan`
            // drives the branch. Retry and reconcile keep the poll
            // loop alive; engine directives hand off to the reopen
            // listener carrying the original account error; terminal
            // errors stop the loop (the broadcast already carried the
            // terminating event). The account error needs to ride
            // along on the engine directive arm; we clone it before
            // planning.
            let original = error.clone();
            match plan_recovery(error) {
                RecoveryPlan::Retry(advice) => {
                    // Share any provider-documented throttle deadline
                    // with sibling scopes (and, via tenant/provider
                    // keys, sibling accounts) before sleeping it off
                    // locally.
                    crate::recovery::record_throttle(throttles, account_id, &advice, &original);
                    let delay = retry_delay(
                        &advice,
                        std::time::SystemTime::now(),
                        std::time::Duration::from_secs(1),
                    );
                    tokio::select! {
                        () = shutdown.cancelled() => return retired(),
                        () = scope_cancel.cancelled() => return retired(),
                        () = tokio::time::sleep(delay) => {}
                    }
                    DriveRecovery {
                        advanced: false,
                        exit: None,
                    }
                }
                RecoveryPlan::Reconcile(advice) => {
                    crate::recovery::record_reconcile_throttle(
                        throttles, account_id, &advice, &original,
                    );
                    // A read stream's reconcile collapses to "rerun
                    // this scope soon". Sleep briefly so we do not
                    // hot-spin, then re-enter the poll loop. The
                    // mutation-side `Reconcile` actions (CheckTarget /
                    // DedupeByClientId) drive through the mutation
                    // pipeline; the change-stream side does not have a
                    // per-item lane to dedupe.
                    let delay = crate::recovery::reconcile_delay(
                        &advice,
                        std::time::SystemTime::now(),
                        std::time::Duration::from_secs(1),
                    );
                    tokio::select! {
                        () = shutdown.cancelled() => return retired(),
                        () = scope_cancel.cancelled() => return retired(),
                        () = tokio::time::sleep(delay) => {}
                    }
                    DriveRecovery {
                        advanced: false,
                        exit: None,
                    }
                }
                RecoveryPlan::Engine(directive) => {
                    let directive_scope = directive_target_scope(&directive);
                    let _ = reopen_tx
                        .send(ReopenRequest::Recovery {
                            scope: directive_scope,
                            error: original,
                        })
                        .await;
                    DriveRecovery {
                        advanced: false,
                        exit: None,
                    }
                }
                RecoveryPlan::Terminal(fatal) => {
                    let err = fatal.as_ref();
                    let view = err.telemetry_fields();
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?account_id,
                        scope = ?scope,
                        kind = view.kind_discriminant,
                        message_key = view.message_key,
                        recovery = view.recovery_discriminant,
                        operation = ?view.operation,
                        "changes stream terminated with terminal recovery"
                    );
                    // Park, not retire: retiring the token would let the 1s
                    // scan respawn this poll against a cursor whose provider
                    // just said "nothing left to try", turning a terminal
                    // error into a once-per-second re-drive that
                    // re-broadcasts `Terminated` forever.
                    DriveRecovery {
                        advanced: false,
                        exit: Some(PollExit::Park),
                    }
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                error = %err,
                "drive_changes_stream errored"
            );
            DriveRecovery {
                advanced: false,
                exit: None,
            }
        }
    }
}

/// Apply one `ScopeLifecycle` transition to the live registry, the poll
/// tokens, and the engine's reopen channel.
///
/// `Created` asks the engine (via a synthesized restart-scope error) to
/// establish the new scope's cursor. `Deleted` cancels the poll task,
/// drops the in-memory cursor, and raises `ReopenRequest::ScopeDeleted`
/// so the engine purges the scope's DURABLE rows too - change cursor and
/// backfill rows, completion marker included; without that, a folder
/// recreated under the same id would find the old completion marker and
/// skip its entire cold-start walk. `Renamed` is the delete half plus
/// the create half on the new id.
async fn apply_lifecycle_transition(
    cursors: &CursorRegistry,
    tokens: &ScopeTokens,
    reopen_tx: &mpsc::Sender<ReopenRequest>,
    lifecycle: ScopeLifecycle,
) {
    async fn delete_scopes(
        cursors: &CursorRegistry,
        tokens: &ScopeTokens,
        reopen_tx: &mpsc::Sender<ReopenRequest>,
        membership: &bifrost_types::MembershipScope,
    ) {
        // The scopes a `Created` for this membership would have asked the
        // engine to establish, read BEFORE any registered scope is deleted
        // below (the mapping reads the registered topology). Every one the
        // membership index does not already name below is routed to the
        // listener too, cursor or not:
        // - with no cursor, its repair may be parked under a pause or still
        //   queued on the listener's channel ahead of this request; the
        //   listener's `ScopeDeleted` arm unparks the one and retires the
        //   cursor the other establishes, both before purging;
        // - with a cursor missing from the membership index (a membership
        //   refresh after its establishment failed), the arm retires it too,
        //   where the index loop below would never find it.
        let indexed = cursors.scopes_for_membership(membership);
        let unindexed: Vec<CursorScope> = membership_to_cursor_scopes(cursors, membership)
            .into_iter()
            .filter(|scope| !indexed.contains(scope))
            .collect();
        for scope in indexed {
            // Registry first. `delete` cancels the scope's liveness token as
            // it drops the cursor, under one write lock, so there is no
            // instant where the scope looks drivable to a producer that has
            // not yet been stopped. The reverse order left exactly that
            // window: the poll token was gone while the cursor was still
            // live, and a reconciler entering it started a drive on a scope
            // about to be deleted.
            cursors.delete(&scope);
            cancel_scope_token(tokens, &scope);
            let _ = reopen_tx.send(ReopenRequest::ScopeDeleted { scope }).await;
        }
        for scope in unindexed {
            let _ = reopen_tx.send(ReopenRequest::ScopeDeleted { scope }).await;
        }
    }

    async fn establish_scopes(
        cursors: &CursorRegistry,
        reopen_tx: &mpsc::Sender<ReopenRequest>,
        membership: &bifrost_types::MembershipScope,
    ) {
        for scope in membership_to_cursor_scopes(cursors, membership) {
            // The cursor registry has no entry yet; the poll task would
            // see snapshot=None and exit cleanly unless the engine
            // establishes a cursor first. Synthesize a cursor-invalid
            // `AccountError` for this scope so the engine's
            // `handle_account_error` dispatch derives
            // `Engine(RestartScope(scope))` through the same path it
            // uses for every other recovery.
            let error =
                crate::recovery::restart_scope_error(scope.clone(), AccountOperation::SyncChanges);
            let _ = reopen_tx
                .send(ReopenRequest::Recovery {
                    scope: Some(scope.clone()),
                    error,
                })
                .await;
        }
    }

    match lifecycle {
        ScopeLifecycle::Created(membership) => {
            establish_scopes(cursors, reopen_tx, &membership).await;
        }
        ScopeLifecycle::Deleted(membership) => {
            delete_scopes(cursors, tokens, reopen_tx, &membership).await;
        }
        ScopeLifecycle::Renamed { old, new, .. } => {
            delete_scopes(cursors, tokens, reopen_tx, &old).await;
            establish_scopes(cursors, reopen_tx, &new).await;
        }
        _ => {}
    }
}

/// Map a `MembershipScope` from `ScopeLifecycle` to newly-required
/// cursor scopes using the account's registered cursor topology.
///
/// Account-wide and type-wide cursor models already cover a newly
/// created membership, so they produce no scope. Per-folder models
/// preserve their existing shape, including every registered
/// `FolderType` object type. An empty or unrecognized topology also
/// produces no scope rather than inventing one the protocol never
/// advertised.
#[must_use]
pub fn membership_to_cursor_scopes(
    cursors: &CursorRegistry,
    membership: &MembershipScope,
) -> Vec<CursorScope> {
    use bifrost_types::FolderId;
    let registered = cursors.all_scopes();
    if registered
        .iter()
        .any(|scope| matches!(scope, CursorScope::Account | CursorScope::Type(_)))
    {
        return Vec::new();
    }
    match membership {
        MembershipScope::Folder(folder) => folder_cursor_shapes(&registered, folder.clone()),
        MembershipScope::Mailbox(mailbox) => {
            folder_cursor_shapes(&registered, FolderId(mailbox.0.clone()))
        }
        MembershipScope::Query(q)
            if registered
                .iter()
                .any(|scope| matches!(scope, CursorScope::Query(_))) =>
        {
            vec![CursorScope::Query(q.clone())]
        }
        _ => Vec::new(),
    }
}

fn folder_cursor_shapes(
    registered: &[CursorScope],
    folder: bifrost_types::FolderId,
) -> Vec<CursorScope> {
    let mut scopes = Vec::new();
    if registered
        .iter()
        .any(|scope| matches!(scope, CursorScope::Folder(_)))
    {
        scopes.push(CursorScope::Folder(folder.clone()));
    }
    for scope in registered {
        if let CursorScope::FolderType { ty, .. } = scope {
            let candidate = CursorScope::FolderType {
                folder: folder.clone(),
                ty: *ty,
            };
            if !scopes.contains(&candidate) {
                scopes.push(candidate);
            }
        }
    }
    scopes
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, AccountOperation, AuthCause, AuthErrorKind, Cause,
        StateCause, SyncStateErrorKind,
    };

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn scope_tokens() -> ScopeTokens {
        Arc::new(StdMutex::new(HashMap::new()))
    }

    #[test]
    fn registry_replacement_and_scope_restart_retire_terminal_ownership() {
        for replace_account in [false, true] {
            let cursors = CursorRegistry::new();
            let scope = CursorScope::Account;
            let cursor = crate::test_common::cursor_for(&scope, b"initial");
            cursors.put(cursor.clone());
            let tokens = scope_tokens();
            let mut entry = ScopeToken::tombstone();
            entry.drive_generation = Some(cursors.drive_generation(&scope));
            tokens
                .lock()
                .expect("poisoned")
                .insert(scope.clone(), entry);
            retire_stale_scope_token(&tokens, &cursors, &scope);
            assert!(scope_is_parked(&tokens, &scope));
            if replace_account {
                let replacement = CursorRegistry::new();
                replacement.put(cursor);
                cursors.replace_topology_preserving_cursors(&replacement);
            } else {
                cursors.delete(&scope);
                cursors.put(cursor);
            }
            retire_stale_scope_token(&tokens, &cursors, &scope);
            assert!(!tokens.lock().expect("poisoned").contains_key(&scope));
        }
    }

    #[tokio::test]
    async fn fresh_bad_checkpoint_terminates_only_its_cursor_scope() {
        let scope = CursorScope::Account;
        let cursor = crate::test_common::cursor_for(&scope, b"initial");
        let mut invalid = cursor.clone();
        invalid.envelope_version = u32::MAX;
        let mut account = crate::test_common::StubAccount::new(vec![scope.clone()]);
        account.changes_hook = Some(Arc::new(move |_| {
            vec![SyncEvent::Done(Some(Checkpoint::Change(invalid.clone())))]
        }));
        let cursors = Arc::new(CursorRegistry::new());
        cursors.put(cursor.clone());
        let (tx, mut rx) = broadcast::channel(4);
        let delivery = Arc::new(ChangeDelivery::new(tx));
        let (_boundary, view) = crate::cancel::Boundary::new();
        let result = drive_changes_stream(
            &account,
            scope.clone(),
            cursor,
            cursors,
            delivery,
            view,
            None,
            None,
        )
        .await;
        let Err(Error::Account(error)) = result else {
            panic!("expected contract failure")
        };
        assert!(matches!(
            crate::recovery::plan_recovery(error.clone()),
            crate::recovery::RecoveryPlan::Terminal(_)
        ));
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Cursor(scope))
        );
        assert!(matches!(
            &*rx.try_recv().expect("termination published").event,
            SyncEvent::Terminated(_)
        ));
    }

    fn warning_event(message: &str) -> MultiplexerEvent {
        MultiplexerEvent {
            scope: CursorScope::Account,
            event: Arc::new(SyncEvent::Warning(bifrost_types::Warning::user_safe(
                bifrost_types::WarningKind::Other,
                message,
            ))),
            checkpoint: None,
            publication: None,
        }
    }

    fn lag_test_control() -> crate::control::SyncControl {
        let (boundary, _view) = crate::cancel::Boundary::new();
        let (priority, _priority_view) =
            tokio::sync::watch::channel(bifrost_types::Priority::Normal);
        let (bandwidth, _bandwidth_view) = tokio::sync::watch::channel(None);
        crate::control::SyncControl::new(
            bifrost_types::AccountId("lag".into()),
            boundary,
            priority,
            bandwidth,
        )
    }

    /// The same control over a caller-supplied ledger, so a test can observe the
    /// backfill charge the control's publications create - as `attach` does,
    /// where the slot and the control share one `PendingCoverage`.
    fn lag_test_control_over(
        coverage: Arc<crate::cursor::PendingCoverage>,
    ) -> crate::control::SyncControl {
        let (boundary, view) = crate::cancel::Boundary::new();
        let (priority, p) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
        let (bandwidth, b) = tokio::sync::watch::channel(None);
        std::mem::forget((view, p, b));
        crate::control::SyncControl::new_with_publications(
            bifrost_types::AccountId("lag".into()),
            boundary,
            priority,
            bandwidth,
            coverage,
        )
    }

    fn account_change_checkpoint(state: &[u8]) -> Checkpoint {
        Checkpoint::Change(bifrost_types::ChangeCursor {
            scope: CursorScope::Account,
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Imap,
                envelope_version: 1,
                bytes: state.to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        })
    }

    /// A scope deletion (or a detach) must CUT SHORT a recovery delay, not wait
    /// it out. The retry sleep used to be un-selected, so a deleted scope or a
    /// detaching account sat through the provider's full hint - and detach then
    /// burned toward `detach_timeout` and aborted the task instead of draining
    /// it.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_scope_does_not_wait_out_its_retry_hint() {
        let scope = CursorScope::Account;
        let (reopen_tx, _reopen_rx) = mpsc::channel(1);
        let error = bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Server(bifrost_types::ServerErrorKind::RateLimited),
            bifrost_types::Cause::Server(bifrost_types::ServerCause::RateLimited {
                retry_hint: Some(bifrost_types::RetryHint::After(
                    std::time::Duration::from_secs(600),
                )),
            }),
        )
        .try_build()
        .expect("valid rate-limited server error");
        let scope_cancel = CancellationToken::new();
        scope_cancel.cancel();

        let started = tokio::time::Instant::now();
        let recovery = handle_drive_outcome(
            &scope,
            Ok(ChangesEvent::Terminated(error)),
            false,
            &reopen_tx,
            &AccountId("cancelled".into()),
            &StdMutex::new(crate::recovery::ThrottleBucket::default()),
            &CancellationToken::new(),
            &scope_cancel,
        )
        .await;

        assert_eq!(recovery.exit, Some(PollExit::Retire));
        assert!(
            started.elapsed() < std::time::Duration::from_secs(600),
            "the cancelled scope must not sleep off the provider hint"
        );
    }

    #[tokio::test]
    async fn account_error_from_driver_routes_through_recovery_plan() {
        let scope = CursorScope::Account;
        let cursors = CursorRegistry::new();
        cursors.put(match account_change_checkpoint(b"unchanged") {
            Checkpoint::Change(cursor) => cursor,
            _ => unreachable!(),
        });
        let (reopen_tx, mut reopen_rx) = mpsc::channel(1);
        let error = crate::recovery::cursor_decode_failure(AccountOperation::SyncChanges);
        let recovery = handle_drive_outcome(
            &scope,
            Err(Error::Account(error.clone())),
            false,
            &reopen_tx,
            &AccountId("routing".into()),
            &StdMutex::new(crate::recovery::ThrottleBucket::default()),
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        assert!(recovery.exit.is_none());
        let request = reopen_rx.try_recv().expect("engine directive forwarded");
        assert!(matches!(
            request,
            ReopenRequest::Recovery { error: routed, .. }
                if routed.kind() == error.kind()
        ));
    }

    /// `ScopeLifecycle::Deleted` must retire more than the in-memory
    /// cursor: it raises `ReopenRequest::ScopeDeleted` so the engine also
    /// purges the durable rows (change cursor AND backfill rows, completion
    /// marker included). Without the request, a folder recreated under the
    /// same id finds the surviving completion marker and skips its entire
    /// cold-start walk.
    #[tokio::test]
    async fn deleted_lifecycle_purges_cursor_and_requests_durable_deletion() {
        let cursors = CursorRegistry::new();
        let folder = bifrost_types::FolderId("Projects".into());
        let scope = CursorScope::Folder(folder.clone());
        cursors.put(bifrost_types::ChangeCursor {
            scope: scope.clone(),
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Imap,
                envelope_version: 1,
                bytes: b"state".to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        });
        let membership = bifrost_types::MembershipScope::Folder(folder);
        cursors.link_membership(membership.clone(), scope.clone());
        let tokens = scope_tokens();
        track(&tokens, &scope);
        // The REGISTRY's own liveness token, captured before the transition.
        // The poll token below covers only the poll task; this is what reaches
        // a stalled drive belonging to the push reconciler, which never has an
        // entry in `tokens`.
        let registry_cancel = cursors
            .scope_cancel(&scope)
            .expect("registered scope token");
        let (reopen_tx, mut reopen_rx) = mpsc::channel(4);

        apply_lifecycle_transition(
            &cursors,
            &tokens,
            &reopen_tx,
            ScopeLifecycle::Deleted(membership),
        )
        .await;

        assert!(cursors.snapshot(&scope).is_none());
        assert!(!tokens.lock().expect("poisoned").contains_key(&scope));
        assert!(
            registry_cancel.is_cancelled(),
            "a lifecycle delete must end any in-flight drive, including the \
             reconciler's, not just the poll task"
        );
        let request = reopen_rx.try_recv().expect("durable deletion requested");
        assert!(matches!(
            request,
            ReopenRequest::ScopeDeleted { scope: deleted } if deleted == scope
        ));
    }

    /// A terminal recovery verdict must park the scope, not retire it: a
    /// retired token is exactly what the 1s scan reads as "please respawn",
    /// which turned every terminal changes-stream error into a
    /// once-per-second re-drive that re-broadcast `Terminated` forever.
    #[tokio::test]
    async fn terminal_drive_outcome_parks_the_scope_instead_of_retiring_it() {
        let scope = CursorScope::Account;
        let (reopen_tx, mut reopen_rx) = mpsc::channel(1);
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid terminal authentication error");

        let recovery = handle_drive_outcome(
            &scope,
            Ok(ChangesEvent::Terminated(error)),
            false,
            &reopen_tx,
            &AccountId("terminal".into()),
            &StdMutex::new(crate::recovery::ThrottleBucket::default()),
            &CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(recovery.exit, Some(PollExit::Park));
        assert!(
            reopen_rx.try_recv().is_err(),
            "terminal errors must not be handed to the reopen listener"
        );
    }

    /// A lag destroys batches whose checkpoints are already registered
    /// as expected. If the receiver only warned, those registrations
    /// would gate `pause` / `checkpoint_now` forever: surfacing the loss
    /// would have converted it into a permanent hang.
    /// `try_recv` shares the lag path, so it must abandon too - a
    /// polling consumer that never calls `recv` would otherwise wedge
    /// the boundary exactly as the awaiting one used to.
    #[tokio::test]
    async fn try_recv_takes_the_same_lag_recovery_path() {
        let (tx, sentinel) = broadcast::channel(1);
        let control = lag_test_control();
        let mut receiver = ChangesReceiver::new(tx.subscribe(), Some(control.clone()), None);
        control.expect_checkpoint(account_change_checkpoint(b"lost"));
        tx.send(warning_event("overwritten"))
            .expect("receivers live");
        tx.send(warning_event("retained")).expect("receivers live");

        let lag = receiver.try_recv().expect("lag becomes an event");
        assert!(matches!(
            lag.event.as_ref(),
            SyncEvent::Warning(warning)
                if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
        ));
        assert!(
            control.abandon_pending_checkpoints() == 0,
            "try_recv's lag must already have abandoned the registration"
        );
        drop(sentinel);
    }

    /// The lag path's OTHER half, and it has to be exercised through
    /// `ChangesReceiver` rather than by calling the ledger directly - the wiring
    /// is the thing that can go missing.
    ///
    /// The reasoning it pins: abandonment retires every outstanding
    /// registration, so no acknowledgement can arrive for them - and backfill
    /// capacity IS those registrations. Leaving them charged converts the
    /// in-session loss the warning reports into a cold-start producer that never
    /// runs again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lag_frees_the_backfill_bound() {
        let (tx, sentinel) = broadcast::channel(1);
        let coverage = Arc::new(crate::cursor::PendingCoverage::new());
        let control = lag_test_control_over(Arc::clone(&coverage));
        let mut receiver = ChangesReceiver::new(
            tx.subscribe(),
            Some(control.clone()),
            Some(Arc::clone(&coverage)),
        );

        // One outstanding backfill publication, charged exactly as the runner
        // would have left it.
        let checkpoint = Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
            scope: CursorScope::Account,
            partition: bifrost_types::Partition(b"page:0:10".to_vec()),
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: 1,
        });
        control.expect_checkpoint(checkpoint);
        assert_eq!(coverage.backfill_in_flight(), 1);

        // Overflow the one-slot ring under the receiver.
        for state in [b"a".as_slice(), b"b".as_slice()] {
            let _ = tx.send(MultiplexerEvent::unacked(
                CursorScope::Account,
                Arc::new(SyncEvent::Warning(bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::Other,
                    String::from_utf8_lossy(state).into_owned(),
                ))),
            ));
        }

        let lag = receiver.recv().await.expect("lag becomes an event");
        assert!(matches!(
            lag.event.as_ref(),
            SyncEvent::Warning(warning)
                if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
        ));
        assert_eq!(
            coverage.backfill_in_flight(),
            0,
            "a lag abandons the registration, and the registration is the capacity"
        );
        drop(sentinel);
    }

    /// A backfill delivery harness that publishes exactly the way the runner
    /// does - register the publication, then `publish_backfill` to send and stamp
    /// under the delivery lock. Nothing here stamps by hand, so the tests below
    /// exercise the send/stamp/subscribe/sweep ordering rather than assuming it.
    struct Delivery {
        delivery: Arc<ChangeDelivery>,
        coverage: Arc<crate::cursor::PendingCoverage>,
        control: crate::control::SyncControl,
        _sentinel: broadcast::Receiver<MultiplexerEvent>,
    }

    fn delivery_harness(capacity: usize) -> Delivery {
        let (tx, sentinel) = broadcast::channel(capacity);
        let coverage = Arc::new(crate::cursor::PendingCoverage::new());
        let control = lag_test_control_over(Arc::clone(&coverage));
        Delivery {
            delivery: Arc::new(ChangeDelivery::new(tx)),
            coverage,
            control,
            _sentinel: sentinel,
        }
    }

    impl Delivery {
        fn backfill_page(partition: &str) -> Checkpoint {
            Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
                scope: CursorScope::Account,
                partition: bifrost_types::Partition(partition.as_bytes().to_vec()),
                progress_marker: None,
                progress: bifrost_types::BackfillProgress::default(),
                envelope_version: 1,
            })
        }

        /// Register and broadcast one backfill page, as `run_partition` does.
        fn publish(&self, partition: &str) -> crate::cursor::PublicationId {
            self.publish_reaching(partition).0
        }

        /// `publish`, also reporting what the gate reported: whether a
        /// NUMBERED receiver took delivery.
        fn publish_reaching(&self, partition: &str) -> (crate::cursor::PublicationId, bool) {
            let checkpoint = Self::backfill_page(partition);
            let id = self.control.expect_checkpoint(checkpoint.clone());
            let event = MultiplexerEvent {
                scope: CursorScope::Account,
                event: Arc::new(SyncEvent::Batch(bifrost_types::Batch {
                    items: Vec::new(),
                    page_boundary: bifrost_types::PageBoundary::Final,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: Some(checkpoint.clone()),
                })),
                checkpoint: Some(checkpoint),
                publication: Some(id.clone()),
            };
            let reached = self
                .delivery
                .publish_backfill(event, &self.coverage, Some(&id));
            (id, reached)
        }

        fn subscribe(&self) -> ChangesReceiver {
            self.delivery
                .subscribe(Some(self.control.clone()), Some(Arc::clone(&self.coverage)))
        }
    }

    /// FINDINGS 3, 4 and 5 at the seam that has to notice, and all three are one
    /// ordering question. Receiver A takes delivery of two pages, B subscribes
    /// afterwards, A drops. The subscriber COUNT never reaches zero, and B joined
    /// at the ring's tail beyond A's pages, so nothing can ever acknowledge them
    /// and the producer would park for the life of the attachment.
    ///
    /// Every step goes through the real path - `ChangeDelivery::publish_backfill`
    /// for the send AND stamp, `subscribe` for the numbering,
    /// `ChangesReceiver::drop` for the sweep. A version of this that stamped by
    /// hand could not have exercised the ordering at all, which is exactly how
    /// three separate interleavings survived the previous round.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_overlapping_receiver_replacement_frees_pages_nobody_can_reach() {
        let h = delivery_harness(8);
        let a = h.subscribe();
        h.publish("page:0:10");
        h.publish("page:10:20");
        assert_eq!(h.coverage.backfill_in_flight(), 2);

        // B arrives BEFORE A leaves, so the count never touches zero.
        let mut b = h.subscribe();
        assert!(
            h.delivery.has_numbered_subscriber(),
            "a consumer is present throughout"
        );
        assert_eq!(
            h.coverage.backfill_in_flight(),
            2,
            "B's arrival alone settles nothing"
        );
        assert!(
            b.try_recv().is_err(),
            "B really did join beyond those pages - it can never receive them"
        );

        drop(a);

        assert_eq!(
            h.coverage.backfill_in_flight(),
            0,
            "B joined at the ring's tail and can never receive A's pages, so their \
             capacity must come back when A leaves"
        );
    }

    /// The receipt bound itself. A numbered receiver's READ returns the page's
    /// lane capacity, with no acknowledgement anywhere in sight - and the
    /// boundary registration survives it, because reading and acknowledging
    /// answer different questions.
    ///
    /// The ablation is the whole point: with `note_receipt` removed, or with
    /// `backfill_in_flight` summing entries rather than unread pages, the first
    /// assertion reads 2.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_numbered_receivers_read_frees_capacity_without_an_acknowledgement() {
        let h = delivery_harness(8);
        let mut a = h.subscribe();
        h.publish("page:0:10");
        h.publish("page:10:20");
        assert_eq!(h.coverage.backfill_in_flight(), 2);

        a.try_recv().expect("the first page");
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "reading a page takes it out of the ring, so its capacity comes back"
        );
        a.try_recv().expect("the second page");
        assert_eq!(h.coverage.backfill_in_flight(), 0);

        assert_eq!(
            h.coverage.pending_checkpoints(),
            2,
            "and neither page is answered for: a read frees the producer, an \
             acknowledgement retires the registration"
        );

        // The completion guarantee is unchanged by the receipt. A receiver that
        // departs holding pages it READ but never acknowledged still strands them.
        drop(a);
        assert_eq!(
            h.coverage.undelivered_watermark(&CursorScope::Account),
            2,
            "read-but-unacknowledged pages leaving with their reader are still a loss"
        );
    }

    /// The bound follows the SLOWEST live numbered reader, not the fastest.
    ///
    /// Every receiver `account_changes_stream` hands out is numbered, and several
    /// live subscribers with exactly one acknowledging is a supported and
    /// documented shape. While the receipt was a single bit, the first read by ANY
    /// of them freed the producer: the fast observer B empties the ring, cold start
    /// runs on, the acknowledger A - slower because it persists as it goes - falls
    /// `changes_capacity` events behind, lags, and `on_lag` abandons every
    /// outstanding backfill page and withholds the walk's marker. That is the
    /// overrun the bound exists to prevent, reproduced BY the bound, and it repeats
    /// for as long as B stays faster.
    ///
    /// The two halves are one property: the fast reader must not advance the
    /// producer past the slow one, and the slow one's DEPARTURE must advance it at
    /// once - a departed reader that still parked the producer would trade this
    /// failure for the stall the departure sweep exists to prevent.
    ///
    /// Ablation: with first-read semantics the first assertion reads 0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_bound_follows_the_slowest_live_numbered_reader() {
        let h = delivery_harness(8);
        let mut slow = h.subscribe();
        let mut fast = h.subscribe();
        h.publish("page:0:10");
        h.publish("page:10:20");
        assert_eq!(h.coverage.backfill_in_flight(), 2);

        fast.try_recv().expect("the first page");
        fast.try_recv().expect("the second page");
        assert_eq!(
            h.coverage.backfill_in_flight(),
            2,
            "both pages are still in the ring as far as the slow reader is concerned, \
             so the fast one's reads must buy the producer nothing"
        );

        slow.try_recv().expect("the first page");
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "the page every eligible reader has now taken out of the ring is the only \
             one released"
        );

        // And the slow reader walking away frees the rest, rather than parking the
        // producer for the life of the attachment.
        drop(slow);
        assert_eq!(
            h.coverage.backfill_in_flight(),
            0,
            "a departed reader can no longer be holding anything in the ring; the \
             departure has to recompute its charge, not only sweep what nobody can \
             reach"
        );
    }

    /// A page that a later publication SUPERSEDED keeps its own receipt, on the
    /// survivor that inherited its charge.
    ///
    /// Supersession is the one place the "capacity is the live record" rule is
    /// corrected, so it is also the one place a per-page bit can be lost by
    /// folding. Reading both pages of one partition must free both.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reading_a_superseded_page_frees_the_charge_it_left_on_its_survivor() {
        let h = delivery_harness(8);
        let mut a = h.subscribe();
        h.publish("partition:one");
        h.publish("partition:one");
        assert_eq!(
            h.coverage.backfill_in_flight(),
            2,
            "the survivor inherited the superseded page's charge"
        );

        a.try_recv().expect("the superseded page");
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "the receipt has to reach the page inside the survivor, not only the \
             survivor's own bit"
        );
        a.try_recv().expect("the survivor");
        assert_eq!(h.coverage.backfill_in_flight(), 0);
    }

    /// An UNNUMBERED receiver's read frees nothing.
    ///
    /// The bound follows the reader that can acknowledge. A reader the delivery
    /// gate never numbered is by construction not that one, and letting its read
    /// free the producer would let a walk run a whole ring ahead of the consumer
    /// who does answer for the pages - the overrun the bound exists to prevent.
    /// This is also the condition the ruled observer subscription rests on.
    ///
    /// A NUMBERED receiver has to be present and unread for this to observe
    /// anything. With none, the page's `delivered_at` is 0, no live receiver is
    /// eligible, and `page_unread` answers from its "has anybody read it"
    /// fallback - so the count reads 1 whether or not the unnumbered read was
    /// recorded, and the test passes against the bug.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unnumbered_receivers_read_frees_no_capacity() {
        let h = delivery_harness(8);
        // The acknowledger, numbered 0, which reads nothing yet.
        let mut acker = h.subscribe();
        let mut observer = ChangesReceiver::new(
            h.delivery.sender().subscribe(),
            Some(h.control.clone()),
            Some(Arc::clone(&h.coverage)),
        );
        h.publish("page:0:10");
        assert_eq!(h.coverage.backfill_in_flight(), 1);

        observer.try_recv().expect("the observer takes delivery");
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "a reader that never acknowledges must not free the producer to run \
             further ahead of the one that does"
        );

        acker.try_recv().expect("the acknowledger takes delivery");
        assert_eq!(
            h.coverage.backfill_in_flight(),
            0,
            "and the NUMBERED receiver's read is what frees it, so the assertion \
             above is about the numbering and not about the page being unreadable"
        );
    }

    /// An OBSERVER counts for neither the subscriber gate nor the delivery
    /// decision, and its reads touch the bound not at all.
    ///
    /// The three places that used to see only a raw receiver count, driven
    /// through the real path. Before the explicit observer subscription, an
    /// observer arriving first satisfied the gate and took delivery of pages the
    /// acknowledger - joining at the ring's tail - could never see; and once the
    /// acknowledger had gone, every later page was judged delivered because the
    /// observer had it, stayed charged, and could never be answered for.
    ///
    /// Ablation: an observer minted through `subscribe` fails the first assertion;
    /// a delivery decision read off `receiver_count()` fails the second.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_observer_counts_for_neither_the_gate_nor_the_delivery() {
        let h = delivery_harness(8);
        let mut observer = h.delivery.observe();
        assert!(
            !h.delivery.has_numbered_subscriber(),
            "an observer must not open the subscriber gate"
        );

        let (unreached, reached) = h.publish_reaching("page:0:10");
        assert!(
            !reached,
            "a page that reached only an observer reached nobody who can acknowledge it"
        );
        // What the runner does with that answer: retire the publication, since
        // no acknowledgement can ever come for it.
        h.control.retire_publication(unreached);
        assert_eq!(h.coverage.backfill_in_flight(), 0);
        assert!(
            !h.delivery
                .publish_acknowledgeable(MultiplexerEvent::unacked(
                    CursorScope::Account,
                    Arc::new(SyncEvent::Warning(bifrost_types::Warning::user_safe(
                        bifrost_types::WarningKind::Other,
                        "live".to_owned(),
                    ))),
                )),
            "the same answer on the live-lane path"
        );
        observer
            .try_recv()
            .expect("the observer still takes delivery");
        observer.try_recv().expect("of everything");

        // The acknowledger arrives SECOND, which is the order that used to lose
        // pages, and is now the ordinary one.
        let mut acker = h.subscribe();
        assert!(h.delivery.has_numbered_subscriber());
        let (_, reached) = h.publish_reaching("page:10:20");
        assert!(reached, "a numbered receiver took delivery");
        assert_eq!(h.coverage.backfill_in_flight(), 1);

        observer.try_recv().expect("the observer reads the page");
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "an observer's read frees nothing"
        );
        acker.try_recv().expect("the acknowledger reads the page");
        assert_eq!(
            h.coverage.backfill_in_flight(),
            0,
            "the numbered receiver's read is what frees it"
        );

        // And the observer's departure sweeps nothing: it was never holding
        // anything on the ledger's account.
        h.publish("page:20:30");
        assert_eq!(h.coverage.backfill_in_flight(), 1);
        drop(observer);
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "the page is still the acknowledger's to read"
        );
        drop(acker);
    }

    /// An observer's lag is its own problem. The registrations belong to the
    /// acknowledger, and abandoning them because a UI reader fell behind voided
    /// every in-flight walk on the account - `on_lag` used to abandon for any
    /// receiver that carried a control handle, and the old unnumbered observer
    /// carried one.
    ///
    /// The acknowledger lagging on the same ring still abandons, which is the
    /// ablation's other half: the difference is the receiver's role, not the
    /// ring.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_observers_lag_abandons_nothing() {
        let h = delivery_harness(1);
        let mut acker = h.subscribe();
        let mut observer = h.delivery.observe();
        h.control
            .expect_checkpoint(account_change_checkpoint(b"registered"));
        assert_eq!(h.coverage.pending_checkpoints(), 1);

        // Overflow the one-slot ring under both receivers.
        for text in ["overwritten", "retained"] {
            let _ = h.delivery.sender().send(warning_event(text));
        }

        let lag = observer.try_recv().expect("lag becomes an event");
        assert!(matches!(
            lag.event.as_ref(),
            SyncEvent::Warning(warning)
                if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
        ));
        assert_eq!(
            h.coverage.pending_checkpoints(),
            1,
            "an observer falling behind says nothing about whether the acknowledger \
             can still answer for the registration"
        );

        let lag = acker.try_recv().expect("the acknowledger lags too");
        assert!(matches!(
            lag.event.as_ref(),
            SyncEvent::Warning(warning)
                if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
        ));
        assert_eq!(
            h.coverage.pending_checkpoints(),
            0,
            "the acknowledger's lag is the one that abandons"
        );
    }

    /// Once teardown has begun, a departing receiver's sweep is inert: no loss
    /// is recorded and no discard is requested for the pages it was holding.
    ///
    /// The ruling this pins (2026-09-06, at `PendingCoverage::release_undelivered`):
    /// a page delivered but unacknowledged at detach is the consumer's, exactly
    /// as at a crash, and a drop anywhere in the teardown window behaves like a
    /// drop after `detach` returns. The registration itself is untouched - the
    /// sweep is what goes inert, not the ledger.
    ///
    /// Ablation: without the teardown check the watermark reads 1 and a discard
    /// request is left for a writer that has already been drained.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_departure_during_teardown_records_no_loss() {
        let h = delivery_harness(8);
        let mut a = h.subscribe();
        h.publish("page:0:10");
        a.try_recv().expect("delivered, never acknowledged");
        h.publish("page:10:20");
        assert_eq!(h.coverage.backfill_in_flight(), 1);

        h.delivery.begin_teardown(&h.coverage);
        drop(a);

        assert_eq!(
            h.coverage.undelivered_watermark(&CursorScope::Account),
            0,
            "pages in a consumer's hands at detach are the consumer's, not a loss"
        );
        assert!(
            h.coverage.take_backfill_discards().is_empty(),
            "and nothing asks the departed writer to drop rows"
        );
        assert_eq!(
            h.coverage.pending_checkpoints(),
            2,
            "the sweep is inert; the ledger is not touched"
        );
        // The unread page stays charged under the no-live-receiver fallback
        // ("has anybody read it"), as a page published to nobody always does
        // until something retires it. Harmless here: teardown has already
        // cancelled the producer this charge would otherwise park.
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "the read page is uncharged; the unread one waits for a retirement that \
             teardown will never need"
        );
    }

    /// The mirror, and the reason the sweep is keyed on the numbering rather than
    /// on "did anyone leave": a receiver that predates the page can still
    /// acknowledge it, so the bound must stay in force when a LATER receiver
    /// departs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_departing_latecomer_does_not_free_a_page_its_predecessor_holds() {
        let h = delivery_harness(8);
        let _a = h.subscribe();
        let b = h.subscribe();
        h.publish("page:0:10");

        drop(b);
        assert_eq!(
            h.coverage.backfill_in_flight(),
            1,
            "A still holds the page and can still acknowledge it"
        );
    }

    /// FINDING 4 specifically. A receiver that subscribes AFTER the send must not
    /// be counted as a possible reader of it. The stamp is taken inside the
    /// delivery lock at send time, so a post-hoc read of the numbering - which is
    /// what the previous revision did - cannot mis-attribute the batch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_subscriber_that_arrives_after_the_send_is_not_a_possible_reader() {
        let h = delivery_harness(8);
        let a = h.subscribe();
        h.publish("page:0:10");
        let _late = h.subscribe();

        drop(a);
        assert_eq!(
            h.coverage.backfill_in_flight(),
            0,
            "the only receiver that could read the page has gone; a latecomer must not \
             keep it charged"
        );
    }

    /// FINDING 6. Retiring the last outstanding registration can make the account
    /// quiescent, and a `pause` waiter sits on the checkpoint watch rather than on
    /// the ledger. The sweep goes through the ledger directly, so it has to
    /// republish quiescence itself or the waiter never wakes.
    ///
    /// Current-thread and paused, so "the waiter has parked" is a fact rather
    /// than a hope: a spawned task runs deterministically to its first await
    /// before control returns here. A multi-threaded version of this passed
    /// against the bug, because the waiter had not started when the sweep ran and
    /// then found the boundary already gone.
    #[tokio::test(start_paused = true)]
    async fn a_receiver_drop_sweep_wakes_a_boundary_waiter() {
        use bifrost_types::Control as _;

        let h = delivery_harness(8);
        let a = h.subscribe();
        h.publish("page:0:10");

        let waiter_control = h.control.clone();
        let pause = tokio::spawn(async move { waiter_control.pause().await });
        // Let the waiter reach its park on the checkpoint watch, and confirm the
        // outstanding registration really is gating it.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(
            !pause.is_finished(),
            "the outstanding registration must gate the boundary before the sweep"
        );

        drop(a);

        let released = tokio::time::timeout(std::time::Duration::from_secs(5), pause)
            .await
            .expect("the sweep must wake a boundary waiter, not only the capacity gate");
        assert_eq!(
            released.expect("pause task").expect("pause"),
            bifrost_types::DurableCheckpointSet::default(),
            "nothing became durable, so nothing may be reported as durable"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn broadcast_lag_releases_boundary_waiters_that_can_never_be_acked() {
        use bifrost_types::Control as _;

        let (tx, sentinel) = broadcast::channel(1);
        let control = lag_test_control();
        let mut receiver = ChangesReceiver::new(tx.subscribe(), Some(control.clone()), None);
        control.expect_checkpoint(account_change_checkpoint(b"lost"));
        tx.send(warning_event("overwritten"))
            .expect("receivers live");
        tx.send(warning_event("retained")).expect("receivers live");

        let pause = tokio::spawn({
            let control = control.clone();
            async move { control.pause().await }
        });
        tokio::task::yield_now().await;
        assert!(
            !pause.is_finished(),
            "the outstanding registration must gate the boundary before the lag is observed"
        );

        let _lag = receiver.recv().await.expect("lag becomes an event");
        // Bounded so the regression reports as a failure rather than a
        // hung test: the defect this pins IS an unbounded wait.
        let released = tokio::time::timeout(std::time::Duration::from_secs(5), pause)
            .await
            .expect("a lost batch's registration must stop gating the boundary");
        assert_eq!(
            released.expect("pause task").expect("pause"),
            bifrost_types::DurableCheckpointSet::default()
        );
        drop(sentinel);
    }

    #[tokio::test]
    async fn changes_receiver_surfaces_broadcast_lag_as_lag_warning() {
        let (tx, sentinel) = broadcast::channel(1);
        let mut receiver = ChangesReceiver::new(tx.subscribe(), None, None);
        tx.send(warning_event("overwritten"))
            .expect("receivers live");
        tx.send(warning_event("retained")).expect("receivers live");

        let lag = receiver.recv().await.expect("lag becomes an event");
        assert!(matches!(
            lag.event.as_ref(),
            SyncEvent::Warning(warning)
                if warning.kind == bifrost_types::WarningKind::ChangeStreamLagged
        ));
        let retained = receiver
            .recv()
            .await
            .expect("retained event follows warning");
        assert!(matches!(
            retained.event.as_ref(),
            SyncEvent::Warning(warning)
                if warning.kind == bifrost_types::WarningKind::Other
        ));
        drop(sentinel);
    }

    fn track(tokens: &ScopeTokens, scope: &CursorScope) -> u64 {
        let generation = SCOPE_TOKEN_GENERATION.fetch_add(1, Ordering::Relaxed);
        tokens.lock().expect("poisoned").insert(
            scope.clone(),
            ScopeToken {
                generation,
                token: CancellationToken::new(),
                parked: false,
                drive_generation: None,
            },
        );
        generation
    }

    /// The ordinary case: a task that is still the registered owner
    /// clears its own entry, so the 1s scan can respawn the scope.
    #[test]
    fn an_exiting_poll_task_retires_its_own_token() {
        let tokens = scope_tokens();
        let scope = CursorScope::Account;
        let generation = track(&tokens, &scope);

        retire_scope_token(&tokens, &scope, generation);

        assert!(tokens.lock().expect("poisoned").is_empty());
    }

    /// The interleaving that produced duplicate poll tasks: task A
    /// exits while its scope is momentarily absent from the cursor
    /// registry, the scan spawns B under the same key, and A's cleanup
    /// then lands. Removing by key would evict B, leaving it untracked
    /// and the next scan free to spawn a third live poll for one
    /// scope - two concurrent producers on a lane+scope the checkpoint
    /// supersession rule assumes has exactly one.
    #[test]
    fn an_exiting_poll_task_does_not_retire_its_successors_token() {
        let tokens = scope_tokens();
        let scope = CursorScope::Account;
        let departing = track(&tokens, &scope);
        let successor = track(&tokens, &scope);
        assert_ne!(departing, successor);

        retire_scope_token(&tokens, &scope, departing);

        let guard = tokens.lock().expect("poisoned");
        let entry = guard.get(&scope).expect("the successor stays tracked");
        assert_eq!(entry.generation, successor);
    }

    /// A cancelled entry left behind by a `ScopeLifecycle::Deleted` or
    /// a `restart_scope` is still the successor's to own; the departing
    /// task must not remove it on the strength of the cancel flag
    /// alone.
    #[test]
    fn retirement_ignores_the_cancellation_flag() {
        let tokens = scope_tokens();
        let scope = CursorScope::Folder(bifrost_types::FolderId("Inbox".into()));
        let departing = track(&tokens, &scope);
        let successor = track(&tokens, &scope);
        tokens
            .lock()
            .expect("poisoned")
            .get(&scope)
            .expect("successor")
            .cancel();

        retire_scope_token(&tokens, &scope, departing);

        let guard = tokens.lock().expect("poisoned");
        assert_eq!(
            guard
                .get(&scope)
                .expect("successor still tracked")
                .generation,
            successor
        );
    }

    #[test]
    fn cadence_halves_on_change() {
        let cur = AdaptiveCadence {
            interval: ms(800),
            no_change_streak: 3,
        };
        let next = Multiplexer::updated_cadence(cur, true, ms(100), ms(60_000));
        assert_eq!(next.interval, ms(400));
        assert_eq!(next.no_change_streak, 0);
    }

    #[test]
    fn cadence_doubles_after_five_idle() {
        let mut c = AdaptiveCadence {
            interval: ms(500),
            no_change_streak: 0,
        };
        for _ in 0..4 {
            c = Multiplexer::updated_cadence(c, false, ms(100), ms(60_000));
            assert_eq!(c.interval, ms(500));
        }
        c = Multiplexer::updated_cadence(c, false, ms(100), ms(60_000));
        assert_eq!(c.interval, ms(1000));
        assert_eq!(c.no_change_streak, 0);
    }

    #[test]
    fn cadence_respects_floor_and_ceiling() {
        let c = AdaptiveCadence {
            interval: ms(150),
            no_change_streak: 0,
        };
        let next = Multiplexer::updated_cadence(c, true, ms(200), ms(60_000));
        assert_eq!(next.interval, ms(200));
        let c = AdaptiveCadence {
            interval: ms(30_000),
            no_change_streak: 4,
        };
        let next = Multiplexer::updated_cadence(c, false, ms(100), ms(30_000));
        assert_eq!(next.interval, ms(30_000));
    }

    /// A lifecycle delete cancels the scope's poll task AND retires its
    /// registry entry. Cancelling without retiring leaves a cancelled
    /// token behind, and `spawn_missing_scope_polls` then sees an entry
    /// for the scope and never respawns it if the scope comes back.
    #[test]
    fn cancelling_a_scope_token_also_retires_its_entry() {
        let tokens: ScopeTokens = Arc::new(StdMutex::new(HashMap::new()));
        let scope = CursorScope::Account;
        let token = CancellationToken::new();
        tokens.lock().expect("poisoned").insert(
            scope.clone(),
            ScopeToken {
                generation: 7,
                token: token.clone(),
                parked: false,
                drive_generation: None,
            },
        );

        cancel_scope_token(&tokens, &scope);

        assert!(token.is_cancelled(), "the poll task must be cancelled");
        assert!(
            !tokens.lock().expect("poisoned").contains_key(&scope),
            "a cancelled scope must leave no entry blocking a later respawn"
        );
    }

    /// Retirement is generation-checked, so a late retirement from a
    /// previous incarnation cannot delete a freshly spawned task's entry.
    #[test]
    fn a_stale_retirement_leaves_the_current_entry_alone() {
        let tokens: ScopeTokens = Arc::new(StdMutex::new(HashMap::new()));
        let scope = CursorScope::Account;
        tokens.lock().expect("poisoned").insert(
            scope.clone(),
            ScopeToken {
                generation: 9,
                token: CancellationToken::new(),
                parked: false,
                drive_generation: None,
            },
        );

        retire_scope_token(&tokens, &scope, 8);

        assert!(
            tokens.lock().expect("poisoned").contains_key(&scope),
            "an older generation must not retire the current entry"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_retry_records_hold_and_honours_hint_without_shortening_backoff() {
        let account = AccountId("lifecycle".into());
        let throttles = StdMutex::new(crate::recovery::ThrottleBucket::default());
        for (hint, expected) in [(600, 600), (1, 30)] {
            let error = AccountErrorBuilder::new(
                AccountErrorKind::Server(bifrost_types::ServerErrorKind::RateLimited),
                Cause::Server(bifrost_types::ServerCause::RateLimited {
                    retry_hint: Some(bifrost_types::RetryHint::After(Duration::from_secs(hint))),
                }),
            )
            .throttle_scope(bifrost_types::ThrottleScope::Account)
            .try_build()
            .expect("valid lifecycle throttle");
            assert_eq!(
                lifecycle_termination(&error),
                LifecycleTermination::Reconnect
            );
            assert_eq!(
                lifecycle_retry_wait(&error, LIFECYCLE_RECONNECT_MIN, &throttles, &account),
                Duration::from_secs(expected),
            );
            assert_eq!(
                crate::recovery::account_throttle_wait(&throttles, &account),
                Some(Duration::from_secs(600)),
                "a shorter subsequent hint must not erase a sibling hold",
            );
        }
    }

    #[test]
    fn lifecycle_terminal_errors_do_not_reconnect_or_reopen() {
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid terminal authentication error");

        assert_eq!(
            lifecycle_termination(&error),
            LifecycleTermination::Terminate
        );
    }

    /// A `RestartAccount` directive swaps the connection this reader holds,
    /// so waiting for the generation change is the right park - bounded, so a
    /// reopen that never succeeds cannot silence lifecycle observation.
    #[test]
    fn lifecycle_account_reopen_errors_wait_for_the_generation_change() {
        let error = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid account-reopen recovery error");

        assert_eq!(
            lifecycle_termination(&error),
            LifecycleTermination::AwaitReopen
        );
    }

    /// Scope-level and account-wide-but-not-reopen directives never bump the
    /// account generation, so parking on it would strand this reader for the
    /// process lifetime. Hand the error over once, then reconnect on backoff.
    #[test]
    fn lifecycle_non_reopen_directives_reconnect_instead_of_parking() {
        let schema = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid schema recovery error");
        assert_eq!(
            lifecycle_termination(&schema),
            LifecycleTermination::ForwardAndReconnect
        );

        let revoked = crate::recovery::restart_scope_error(
            CursorScope::Folder(bifrost_types::FolderId("Shared".into())),
            AccountOperation::SyncChanges,
        );
        assert_eq!(
            lifecycle_termination(&revoked),
            LifecycleTermination::ForwardAndReconnect
        );
    }
}
