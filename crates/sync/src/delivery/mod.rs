//! The account output port: publication, receiver membership and departure.

use crate::engine::writer::WriterHandle;
use bifrost_types::{Checkpoint, CursorScope, SyncEvent};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Whether an event concerns the whole attachment or one real cursor.
/// An account cursor is distinct from attachment-wide engine diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EventScope {
    /// Attachment-wide status, independent of any cursor's lifetime.
    Account,
    /// One actual cursor, including providers whose cursor is account-shaped.
    Cursor(CursorScope),
}

impl EventScope {
    /// The real cursor scope, if this event belongs to one.
    #[must_use]
    pub fn cursor(&self) -> Option<&CursorScope> {
        match self {
            Self::Account => None,
            Self::Cursor(scope) => Some(scope),
        }
    }
}

/// Unified output of the multiplexer: either a synced `Change` batch
/// (with optional checkpoint) or a per-scope warning/fatal.
///
/// Wrapped in an `Arc` so subscribers receive cheap clones from the
/// broadcast.
#[derive(Debug, Clone)]
pub struct MultiplexerEvent {
    pub scope: EventScope,
    pub event: Arc<SyncEvent<bifrost_types::Change>>,
    pub checkpoint: Option<Checkpoint>,
    /// Engine-issued identity for THIS publication of `checkpoint`, to be
    /// passed back to `Engine::ack`.
    ///
    /// `Some` for every acknowledgeable publication. Both checkpoint-bearing
    /// and repair events return it through `ack`. It exists because
    /// `Checkpoint` equality is value equality and cannot identify a
    /// publication: a backfill page whose content was entirely unrepresentable
    /// increments no item count and produces a checkpoint byte-identical to its
    /// predecessor, and inventory fusion publishes its final checkpoint twice.
    /// Acknowledging by value would then apply one publication's coverage claim
    /// to a different publication's checkpoint.
    pub(crate) publication: Option<crate::cursor::AckToken>,
}

impl MultiplexerEvent {
    /// Token to acknowledge after the corresponding consumer transaction commits.
    #[must_use]
    pub fn ack_token(&self) -> Option<&crate::cursor::AckToken> {
        self.publication.as_ref()
    }

    /// An event carrying no checkpoint, and so nothing to acknowledge.
    #[must_use]
    pub fn unacked(scope: EventScope, event: Arc<SyncEvent<bifrost_types::Change>>) -> Self {
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
    subscription: Option<(std::sync::Weak<ChangeDelivery>, ReceiverSequence)>,
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
        let retired = delivery.depart(seq, inner);
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
/// numbered set at the send; checkpoint-free inventory pages, barrier pages
/// included, use it too. Events nobody acknowledges - warnings, terminations -
/// send on [`Self::publish_unacked`] directly and take no lock.
///
/// The raw broadcast sender is deliberately not exposed (no `sender()` accessor,
/// no `changes_tx` on the handle): a raw send bypasses the numbered-receiver
/// delivery guarantee and checkpoint registration. Consumers subscribe through
/// [`ChangeDelivery::observe`]; engine code publishes through `PagePublisher`
/// or [`Self::publish_unacked`].
#[derive(Debug)]
pub struct ChangeDelivery {
    tx: broadcast::Sender<MultiplexerEvent>,
    state: StdMutex<SubscriberState>,
}

/// Required safety services for an inventory walk. The writer channel can be
/// served by an in-process test double; omission never selects weaker safety.
#[derive(Debug, Clone)]
pub struct PagePublisher {
    pub(crate) delivery: Arc<ChangeDelivery>,
    pub(crate) control: crate::control::SyncControl,
    pub(crate) coverage: Arc<crate::cursor::PendingCoverage>,
    pub(crate) writer: WriterHandle,
    pub(crate) shutdown: CancellationToken,
}

/// Inventory forwarding keeps checkpoint-free pages under the same walk fence
/// as checkpoint registration. Broadcast acceptance alone proves no progress.
pub(crate) enum InventoryPublication {
    Published,
    Undelivered,
    Refused(crate::cursor::CoverageClaim),
}

impl PagePublisher {
    /// Both inventory front ends use this projection and fence-before-forward
    /// rule. The caller supplies only its accepted or minted checkpoint.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_inventory<'a>(
        &self,
        scope: &CursorScope,
        batch: &bifrost_types::InventoryBatch,
        ids: impl IntoIterator<Item = &'a bifrost_types::ObjectId>,
        removed: impl IntoIterator<Item = &'a bifrost_types::ObjectId>,
        checkpoint: Option<Checkpoint>,
        generation: crate::cursor::WalkGeneration,
        fence: crate::cursor::ScopeFence,
    ) -> Result<InventoryPublication, bifrost_types::AccountError> {
        if checkpoint.is_none() && self.coverage.scope_fence(scope) != fence {
            return Ok(InventoryPublication::Refused(
                crate::cursor::CoverageClaim {
                    reports: Vec::new(),
                    generation,
                },
            ));
        }
        let removals = crate::inventory_walk::removals(scope, removed).inspect_err(|error| {
            self.delivery
                .publish_terminated(EventScope::Cursor(scope.clone()), error.clone());
        })?;
        let synthetic = bifrost_types::Batch {
            items: crate::inventory_walk::created(ids)
                .chain(removals)
                .collect(),
            page_boundary: batch.page_boundary,
            server_latency: batch.server_latency,
            bytes_in: batch.bytes_in,
            checkpoint: checkpoint.clone(),
        };
        let event = MultiplexerEvent {
            scope: EventScope::Cursor(scope.clone()),
            event: Arc::new(SyncEvent::Batch(synthetic)),
            checkpoint: checkpoint.clone(),
            publication: None,
        };
        let reached = if checkpoint.is_some() {
            match self.publish_page(
                event,
                crate::cursor::CoverageClaim::new(batch.coverage.clone(), generation),
                fence,
            ) {
                Ok(reached) => reached,
                Err(claim) => return Ok(InventoryPublication::Refused(claim)),
            }
        } else {
            // This page belongs to the prefix Done would certify, even though
            // it has no standalone acknowledgement. Fusion must retry if lost.
            let Some(reached) =
                self.delivery
                    .publish_inventory_if_fence(event, &self.coverage, scope, fence)
            else {
                return Ok(InventoryPublication::Refused(
                    crate::cursor::CoverageClaim {
                        reports: Vec::new(),
                        generation,
                    },
                ));
            };
            reached
        };
        Ok(if reached {
            InventoryPublication::Published
        } else {
            InventoryPublication::Undelivered
        })
    }

    #[must_use]
    pub fn new(
        delivery: Arc<ChangeDelivery>,
        control: crate::control::SyncControl,
        writer: impl Into<WriterHandle>,
        shutdown: CancellationToken,
    ) -> Self {
        let coverage = control.publication_ledger();
        Self {
            delivery,
            control,
            coverage,
            writer: writer.into(),
            shutdown,
        }
    }

    #[must_use]
    pub fn coverage(&self) -> &Arc<crate::cursor::PendingCoverage> {
        &self.coverage
    }

    /// The one refusal for a backfill bound that numbers publications
    /// against a different ledger than this publisher. Every backfill entry
    /// point checks here, logs once, and refuses rather than panicking, so a
    /// user sees a stalled walk instead of a crashed sync engine.
    #[must_use]
    pub(crate) fn shares_bound_ledger(
        &self,
        bound: &crate::engine::backfill_bound::BackfillBound,
    ) -> bool {
        let same = Arc::ptr_eq(&self.coverage, bound.coverage());
        if !same {
            tracing::error!("refusing backfill bound using a different coverage ledger");
        }
        same
    }

    pub(crate) fn publish_marker(
        &self,
        mut event: MultiplexerEvent,
        watermark: crate::cursor::UndeliveredMark,
        fence: crate::cursor::ScopeFence,
    ) -> bool {
        let checkpoint = match event.checkpoint.clone() {
            Some(checkpoint @ bifrost_types::Checkpoint::Backfill(_)) => checkpoint,
            _ => {
                tracing::error!("refusing invalid backfill marker");
                return false;
            }
        };
        let scope = match &checkpoint {
            bifrost_types::Checkpoint::Backfill(cursor) => cursor.scope.clone(),
            _ => return false,
        };
        let Ok(id) = self
            .control
            .publish_walk_marker_if_fence(checkpoint, watermark, fence)
        else {
            return false;
        };
        event.publication = Some(id.clone());
        let reached = self
            .delivery
            .publish_backfill(event, &self.coverage, Some(&id));
        if !reached {
            self.coverage.note_undelivered(&scope, 1);
            self.control.retire_publication(id);
        }
        reached
    }

    /// Register against the walk's original fence, then send and retire an
    /// unreachable publication synchronously. Cancellation cannot split these
    /// steps. A mail user must get a retry rather than a completed empty folder.
    pub fn publish_page(
        &self,
        mut event: MultiplexerEvent,
        claim: crate::cursor::CoverageClaim,
        fence: crate::cursor::ScopeFence,
    ) -> Result<bool, crate::cursor::CoverageClaim> {
        let Some(checkpoint) = event.checkpoint.clone() else {
            tracing::error!("refusing page without checkpoint");
            return Err(claim);
        };
        let backfill = matches!(checkpoint, bifrost_types::Checkpoint::Backfill(_));
        let scope = match &checkpoint {
            bifrost_types::Checkpoint::Backfill(cursor) => cursor.scope.clone(),
            bifrost_types::Checkpoint::Change(cursor) => cursor.scope.clone(),
            _ => {
                tracing::error!("refusing unsupported page checkpoint");
                return Err(claim);
            }
        };
        let id = self
            .control
            .publish_checkpoint_if_fence(checkpoint, claim, fence)?;
        event.publication = Some(id.clone());
        let reached = if backfill {
            self.delivery
                .publish_backfill(event, &self.coverage, Some(&id))
        } else {
            self.delivery.publish_acknowledgeable(event)
        };
        if !reached {
            if backfill {
                self.coverage.note_undelivered(&scope, 1);
            }
            self.control.retire_publication(id);
        }
        Ok(reached)
    }
}

/// Sequence in one delivery gate's subscriber set. Only subscription admission
/// advances it; coverage may compare snapshots but cannot mint subscribers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReceiverSequence(u64);

impl ReceiverSequence {
    fn admit(&mut self) {
        self.0 += 1;
    }

    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug)]
struct SubscriberState {
    /// How many consumer receivers this account has ever handed out.
    next: ReceiverSequence,
    /// Bound by the first numbered subscription; this ledger alone owns membership.
    coverage: Option<Arc<crate::cursor::PendingCoverage>>,
}

impl Default for SubscriberState {
    fn default() -> Self {
        Self {
            next: ReceiverSequence(0),
            coverage: None,
        }
    }
}

impl SubscriberState {
    fn min_live(&self) -> Option<ReceiverSequence> {
        self.coverage
            .as_ref()
            .and_then(|coverage| coverage.min_live_receiver())
    }
}

/// Outcome of a publication that carries no acknowledgement obligation.
/// Receiver counts are broadcast counts, not proof of numbered delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[must_use]
pub enum UnackedPublication {
    /// The broadcast accepted the event for this many receivers.
    Published(usize),
    /// No broadcast receiver was present.
    NoReceivers,
    /// The event carried checkpoint or publication evidence and was not sent.
    Refused,
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

    /// Publish data or status that carries no acknowledgement obligation.
    /// A producer cannot bypass registration by smuggling a checkpoint here.
    /// Returns refusal, absence of receivers, or broadcast acceptance. Acceptance
    /// does not certify numbered delivery and cannot justify cursor advancement.
    pub fn publish_unacked(&self, event: MultiplexerEvent) -> UnackedPublication {
        let payload_unacked = match event.event.as_ref() {
            SyncEvent::Batch(batch) => batch.checkpoint.is_none(),
            SyncEvent::Done(checkpoint) => checkpoint.is_none(),
            _ => true,
        };
        if event.checkpoint.is_some() || event.publication.is_some() || !payload_unacked {
            tracing::error!("refusing unregistered checkpoint publication");
            return UnackedPublication::Refused;
        }
        match self.tx.send(event) {
            Ok(receivers) => UnackedPublication::Published(receivers),
            Err(_) => UnackedPublication::NoReceivers,
        }
    }

    /// Announce a warning without an acknowledgement obligation.
    pub fn publish_warning(&self, scope: EventScope, mut warning: bifrost_types::Warning) {
        // The publisher supplies the affected delivery domain even when the
        // warning has no narrower identity. Keep that account attribution so
        // users can locate an advisory in their account, while preserving any
        // more specific identity the producer supplied. Account here does not
        // claim that every cursor failed or authorize account-wide recovery.
        if warning.scope.is_none() {
            warning.scope = Some(match &scope {
                EventScope::Cursor(scope) => bifrost_types::ErrorScope::Cursor(scope.clone()),
                EventScope::Account => bifrost_types::ErrorScope::Account,
            });
        }
        // This constructor cannot carry checkpoint evidence. Status delivery is
        // advisory and does not advance a cursor when no receiver is present.
        let _ = self.publish_unacked(MultiplexerEvent::unacked(
            scope,
            Arc::new(SyncEvent::Warning(warning)),
        ));
    }

    /// Announce termination without an acknowledgement obligation.
    pub fn publish_terminated(&self, scope: EventScope, error: bifrost_types::AccountError) {
        // This constructor cannot carry checkpoint evidence. Status delivery is
        // advisory and does not advance a cursor when no receiver is present.
        let _ = self.publish_unacked(MultiplexerEvent::unacked(
            scope,
            Arc::new(SyncEvent::Terminated(error)),
        ));
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
        publication: Option<&crate::cursor::AckToken>,
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
        let reached = state.min_live().is_some();
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
    /// Checkpoint-free inventory pages, barrier pages included, come here from
    /// [`PagePublisher::publish_inventory`]: Done certifies their prefix, so a
    /// page nobody numbered received must read as undelivered. Events that
    /// expect no acknowledgement - warnings, terminations - go on
    /// [`Self::publish_unacked`] directly and take no lock.
    pub fn publish_acknowledgeable(&self, event: MultiplexerEvent) -> bool {
        let state = self.guard();
        let _ = self.tx.send(event);
        let reached = state.min_live().is_some();
        drop(state);
        reached
    }

    /// Order checkpoint-free inventory sends with reset under the fence owner.
    fn publish_inventory_if_fence(
        &self,
        event: MultiplexerEvent,
        coverage: &crate::cursor::PendingCoverage,
        scope: &CursorScope,
        fence: crate::cursor::ScopeFence,
    ) -> Option<bool> {
        // Membership always precedes coverage. The callback only sends: it
        // neither re-enters coverage nor calls the writer. A reset that wins
        // first refuses this old page; otherwise the send precedes the reset.
        // Users must not see stale inventory newly published after a reset.
        let state = self.guard();
        // min_live reads coverage too, so resolve it before taking the fence
        // guard. Membership cannot change while `state` remains held.
        let reached = state.min_live().is_some();
        let result = coverage.with_scope_fence(scope, fence, || {
            let _ = self.tx.send(event);
            reached
        });
        drop(state);
        result
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
        self.guard().min_live().is_some()
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
    ///
    /// The first subscription binds the coverage ledger; all later ones must
    /// use the same Arc. A mismatch is refused with an immediately closed,
    /// unnumbered receiver, leaving the existing ledger and subscribers intact.
    /// A ledger already bound to another delivery gate is refused the same way.
    /// Numbered receivers always participate in the bound ledger.
    pub fn subscribe(
        self: &Arc<Self>,
        control: Option<crate::control::SyncControl>,
        coverage: Arc<crate::cursor::PendingCoverage>,
    ) -> ChangesReceiver {
        let mut state = self.guard();
        // One delivery gate cannot number receivers against different ledgers.
        // Binding under the send/depart lock preserves the existing lock order.
        let refusal = if let Some(bound) = &state.coverage {
            (!Arc::ptr_eq(bound, &coverage))
                .then_some("refusing change subscription with a different coverage ledger")
        } else if !coverage.bind_delivery(self) {
            Some(
                "refusing change subscription with a coverage ledger bound to another delivery gate",
            )
        } else {
            state.coverage = Some(Arc::clone(&coverage));
            None
        };
        if let Some(reason) = refusal {
            // A substitute ledger or gate would route lag and departure accounting
            // away from the publications actually delivered. Users expect
            // a refused stream, never acknowledgements against another account.
            tracing::error!("{reason}");
            let (tx, inner) = broadcast::channel(1);
            drop(tx);
            return ChangesReceiver {
                inner: Some(inner),
                control: None,
                subscription: None,
                coverage: None,
            };
        }
        // Subscribe INSIDE the lock: a receiver created here is numbered against
        // exactly the set of batches already sent, with no window in which a
        // send could slip between the two and be mis-attributed.
        let inner = self.tx.subscribe();
        let seq = state.next;
        state.next.admit();
        coverage.receiver_joined(seq);
        drop(state);
        ChangesReceiver {
            inner: Some(inner),
            control,
            subscription: Some((Arc::downgrade(self), seq)),
            coverage: Some(coverage),
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
    fn depart(&self, seq: ReceiverSequence, inner: broadcast::Receiver<MultiplexerEvent>) -> usize {
        let state = self.guard();
        let coverage = state.coverage.as_deref();
        drop(inner);
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
            EventScope::Account,
            Arc::new(SyncEvent::Warning(warning)),
        ));
    }

    /// The lowest sequence still holding a receiver. Observability; used by
    /// tests.
    #[must_use]
    pub fn min_live(&self) -> Option<ReceiverSequence> {
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
    MultiplexerEvent::unacked(EventScope::Account, Arc::new(SyncEvent::Warning(warning)))
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
        scope: EventScope::Account,
        event: Arc::new(SyncEvent::Warning(warning)),
        checkpoint: None,
        publication: None,
    }
}

#[cfg(test)]
pub(crate) mod tests;
