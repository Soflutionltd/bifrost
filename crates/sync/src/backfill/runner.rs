//! Backfill partition runner.
//!
//! Walks `inventory_partition_stream(scope)` and forwards each `Batch`
//! as a `MultiplexerEvent` carrying inventory entries and removals plus
//! a `BackfillCheckpoint` checkpoint. The runner filters
//! each page through `LiveSupersedes`, which is intentionally never
//! populated - see that type's docs for why "we broadcast it" is not
//! evidence the consumer received it, and what a sound producer would
//! have to prove first.
//!
//! ## Checkpoint-durability contract
//!
//! Durable persistence is consumer-ack-deferred, mirroring
//! `drive_changes_stream`. The runner broadcasts each page and its
//! `BackfillCheckpoint` but does NOT write to `CheckpointStore`. The
//! consumer atomically persists the page items in their own store, then
//! calls `SyncEngine::ack_checkpoint` with the `Checkpoint::Backfill`;
//! the per-account ack writer routes that to
//! `CheckpointStore::put_backfill` and fires `record_checkpoint` to wake
//! pause / checkpoint waiters on a durable boundary.
//!
//! This closes the at-least-once gap an eager engine-side write would
//! open: a crash between broadcast and the consumer's write re-walks the
//! partition on restart (the orchestrator always restarts a scope from
//! its first partition), so no inventory page is lost. An eager write
//! would let the store record progress the consumer never durably
//! persisted, permanently losing that page's objects on cold start.
//!
//! ## Flow control
//!
//! The same acknowledgement is also what frees the bounded backfill lane. The
//! runner waits, before registering a page, until the account has fewer than
//! `BackfillConfig::lane_capacity` published-but-unanswered backfill batches, so
//! it is not asked for the next page while the bound is full. That capacity is
//! not a separate ledger: it is the publication's own boundary registration in
//! `PendingCoverage` (see `crate::engine::lane`), which is why nothing here has
//! to remember to release anything.
//!
//! The page is then broadcast through `ChangeDelivery::publish_backfill`, which
//! sends it and stamps its publication with the receiver numbering as ONE step
//! under the delivery lock - that stamp is what later tells a page whose readers
//! have all gone from one a newly-arrived reader is about to acknowledge. Live
//! changes take no capacity, are never stamped, and are never delayed by a
//! parked backfill.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;

use bifrost_types::{
    Account, BackfillCheckpoint, BackfillProgress, Batch, Change, Checkpoint, CursorScope,
    InventoryEntry, InventoryPartition, ObjectChange, ObjectChangeKind, ObjectId, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::broadcast;

use crate::control::SyncControl;
use crate::error::Error;
use crate::inventory_walk::{InventoryWalk, WalkDecision, cross_waived_barriers, record_barriers};
use crate::multiplexer::MultiplexerEvent;

use super::partitioner::partition_key;

/// Default cap on `LiveSupersedes`. Beyond this many entries, the
/// oldest insertions are evicted via a FIFO ring so the set does not
/// grow unboundedly in long-running sessions.
pub const LIVE_SUPERSEDES_DEFAULT_CAP: usize = 100_000;

/// Side channel intended to let live `Created` changes pre-empt stale
/// inventory observations, so cold-start inventory does not emit an old
/// entry or removal after the live stream has already shown the object's
/// newer state.
///
/// The set is capped (default `LIVE_SUPERSEDES_DEFAULT_CAP`); on
/// overflow the oldest insertion is dropped.
///
/// # Nothing populates this set, deliberately
///
/// `BackfillRunner::run_partition` filters against it, but no producer
/// calls `add`, so it filters nothing. That is a considered decision,
/// not an oversight, and wiring the multiplexer or push reconciler into
/// it re-introduces silent data loss. Two independent reasons:
///
/// 1. **Broadcasting is not receiving.** The per-account channel is a
///    `tokio::broadcast`. The slot holds a sentinel receiver, so `send`
///    reports success even when no consumer is attached; a subscriber
///    that arrives later starts at the ring's tail and never sees
///    earlier values; and a subscriber that lags has values overwritten
///    out from under it, which this crate does not detect or replay.
///    Recording an id on `send` therefore records ids the consumer will
///    never receive - and suppressing the inventory copy of one of
///    those is permanent loss of that object for the session, because
///    the in-memory cursor has already advanced past it.
///
/// 2. **Selection and publication are separate operations.** Even with
///    a correct record trigger, backfill decides to keep an entry and
///    broadcasts it much later. A live change landing in that gap is
///    recorded too late to suppress anything, so the runner still
///    emits a `Created` for an object the live stream just destroyed.
///    Any working design has to make "decide + publish" indivisible for
///    a given id on both sides.
///
/// The only sound record trigger this engine has is the consumer ack:
/// `SyncEngine::ack_checkpoint` is what the crate already treats as
/// proof of receipt, precisely because broadcast delivery is not. A
/// correct implementation buffers ids as pending, keyed by the
/// checkpoint of the batch carrying them, and promotes them to
/// suppressing only once that checkpoint is acked - with a policy for
/// batches whose `checkpoint` is `None`, since those can never be
/// promoted, and a bound on the pending buffer.
///
/// That is worth building only if the duplicate is expensive, and it is
/// not: consumers must already tolerate a repeated `Created`, because a
/// crash mid-plan re-walks every partition and re-emits acked pages
/// (see the module docs). An unpopulated set costs duplicates the
/// consumer already absorbs. A wrongly populated one drops mail.
#[derive(Debug)]
pub struct LiveSupersedes {
    inner: Mutex<LiveSupersedesInner>,
    cap: usize,
}

#[derive(Debug, Default)]
struct LiveSupersedesInner {
    /// Insertion order for ring eviction. One slot per `add` that did
    /// not collide; on overflow the front slot is popped and its id
    /// removed from `present`.
    ///
    /// `order` is a superset of `present`: `take` removes from
    /// `present` only and leaves a tombstone slot here, so the ring can
    /// hold ids the set no longer suppresses. Eviction reclaims those
    /// slots for free as they reach the front - popping a tombstone
    /// removes nothing from `present`, which lets `present` grow back
    /// toward `cap`. The two therefore reconverge without any scan.
    order: VecDeque<ObjectId>,
    /// The authoritative membership. `take` and `add` both decide
    /// purely from this; `order` only decides eviction ORDER, never
    /// whether an id is suppressed.
    present: std::collections::HashSet<ObjectId>,
}

impl Default for LiveSupersedes {
    fn default() -> Self {
        Self::new()
    }
}

impl LiveSupersedes {
    /// Construct with the default cap.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(LIVE_SUPERSEDES_DEFAULT_CAP)
    }

    /// Construct with a specific cap. Useful for tests that want to
    /// observe ring-eviction behavior cheaply.
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Mutex::new(LiveSupersedesInner::default()),
            cap: cap.max(1),
        }
    }

    /// Mark an id as superseded by the live stream. A backfill page skips
    /// inventory entries and removal ids for it. On overflow the oldest entry
    /// is dropped from the ring.
    ///
    /// No production path calls this, and adding one is not a matter of
    /// finding a convenient call site - see the type docs for what a
    /// sound record trigger has to prove.
    pub fn add(&self, id: ObjectId) {
        let mut g = self.inner.lock().expect("poisoned");
        if g.present.contains(&id) {
            return;
        }
        if g.order.len() >= self.cap
            && let Some(old) = g.order.pop_front()
        {
            g.present.remove(&old);
        }
        g.present.insert(id.clone());
        g.order.push_back(id);
    }

    /// True if the id has been superseded. Removes the entry on hit
    /// so the set drains as backfill walks past entries it can skip.
    ///
    /// O(1). Only `present` is touched; the id's slot in `order` is
    /// left behind as a tombstone rather than scanned out. Cold-start
    /// backfill calls this once per inventory entry, so scanning a
    /// ring that holds up to `LIVE_SUPERSEDES_DEFAULT_CAP` entries
    /// would make hydrating a large mailbox quadratic.
    ///
    /// Tombstones cannot cause a WRONG suppression - the outcome that
    /// would be worse than the duplicate this set exists to prevent.
    /// Suppression requires membership in `present`, and only `add`
    /// ever inserts there; a slot lingering in `order` grants nothing.
    /// The single behavioral consequence is in the other, safe
    /// direction: if an id is taken and then re-announced by the live
    /// stream, `order` holds two slots for it, and evicting the older
    /// one drops the live entry earlier than its ring position implies.
    /// That costs at most one suppression, i.e. one duplicate emit -
    /// which the cap already permits, since this set is an
    /// optimization and never a guarantee.
    #[must_use]
    pub fn take(&self, id: &ObjectId) -> bool {
        let mut g = self.inner.lock().expect("poisoned");
        g.present.remove(id)
    }

    /// Snapshot for tests.
    #[must_use]
    pub fn len(&self) -> usize {
        let g = self.inner.lock().expect("poisoned");
        g.present.len()
    }

    /// Convenience: true iff `len() == 0`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Slots currently held in the eviction ring, live plus tombstoned.
    /// Test-only: the ring is an implementation detail, but its size is
    /// the observable footprint of `take` not scanning it.
    #[cfg(test)]
    fn ring_len(&self) -> usize {
        let g = self.inner.lock().expect("poisoned");
        g.order.len()
    }
}

/// One partition's runner.
pub struct BackfillRunner;

/// Whether the ENCLOSING scope walk may ask for another partition.
///
/// A barrier is a property of the SCOPE, not of the partition that happened to
/// hit it: the region cannot be represented and cannot be replayed, so every
/// later partition or page window would carry a checkpoint that leaps over it.
/// Reporting the stop as a plain "this partition was incomplete" flag was not
/// enough - both orchestrator loops read that as a degraded partition and went
/// on to request the next one, recreating the silent-loss bug the barrier stop
/// exists to prevent one layer up. The verdict is therefore a `#[must_use]`
/// value produced by `BackfillScopeWalk::admit`, so a loop that folds an
/// outcome in cannot quietly discard the answer.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScopeWalkStep {
    /// Nothing blocked the scope; the next partition may be requested.
    RequestNextPartition,
    /// The scope stopped on ground it may not cross. No further partition or
    /// page window may be walked for this scope in this pass, and the
    /// completion marker must be withheld.
    StopScopeWalk,
}

/// Folds partition outcomes into the enclosing scope-walk decision.
///
/// The orchestrator loops own "do I request another partition?", so the rule
/// lives here rather than being re-derived at each call site.
#[derive(Debug, Clone, Copy, Default)]
pub struct BackfillScopeWalk {
    stopped: bool,
}

impl BackfillScopeWalk {
    /// A walk that has not been stopped.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one partition outcome in and answer whether the scope walk may
    /// continue. Once stopped, a walk stays stopped.
    pub fn admit(&mut self, outcome: &BackfillPartitionOutcome) -> ScopeWalkStep {
        if outcome.scope_walk == ScopeWalkStep::StopScopeWalk {
            self.stopped = true;
        }
        if self.stopped {
            ScopeWalkStep::StopScopeWalk
        } else {
            ScopeWalkStep::RequestNextPartition
        }
    }

    /// True once any partition has stopped the scope.
    #[must_use]
    pub fn stopped(&self) -> bool {
        self.stopped
    }
}

/// Outcome for one partition pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillPartitionOutcome {
    /// Inventory entries observed before the live-supersedes filter.
    pub seen: u64,
    /// Inventory entries forwarded after the live-supersedes filter.
    pub kept: u64,
    /// Whether the walk exhausted its space with everything accounted for.
    ///
    /// `false` means the partition finished but left obligations open, so the
    /// orchestrator must NOT write a completion sentinel for the scope: the
    /// sentinel makes the next attach skip the walk entirely, which would turn
    /// a declared gap into a permanent one. Note `seen` counts materialized
    /// entries and explicit removals, so an unaccounted object is not even
    /// in that total - the count cannot be used to detect this.
    pub complete: bool,
    /// Whether the enclosing scope walk may request another partition.
    ///
    /// Distinct from `complete`: an incomplete partition leaves obligations the
    /// ledger can chase later, whereas `StopScopeWalk` means no later partition
    /// of this scope may even be requested, because its checkpoint would
    /// certify a prefix that crosses a region nothing can replay.
    pub scope_walk: ScopeWalkStep,
    /// What the partition's completion declared about results past its end,
    /// or `None` when the stream ended without a completion at all. An
    /// open-ended page walk stops on `Exhausted` and nowhere else: the entry
    /// count is never read as a declaration.
    pub end: Option<bifrost_types::PartitionEnd>,
}

impl Default for BackfillPartitionOutcome {
    /// Defaults are the SAFE answers: nothing counted, nothing complete, and
    /// the scope walk stopped. A default that read as "continue" would let a
    /// construction site that forgot the field advance past a barrier.
    fn default() -> Self {
        Self {
            seen: 0,
            kept: 0,
            complete: false,
            scope_walk: ScopeWalkStep::StopScopeWalk,
            end: None,
        }
    }
}

impl BackfillRunner {
    /// Walk one inventory pass to completion. Forwards each `Batch`
    /// onto the per-account `changes_tx` broadcast as `Created` changes and
    /// projected removals (skipping superseded ids), each page carrying a
    /// `BackfillCheckpoint`.
    /// Does NOT write to `CheckpointStore`: durable advance is
    /// consumer-ack-driven (see module docs). Checkpoint progress counts
    /// entries observed before filtering because it describes the
    /// inventory position, while the returned outcome reports both the
    /// observed and forwarded totals.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_partition(
        account: &dyn Account,
        scope: CursorScope,
        partition: InventoryPartition,
        live: &LiveSupersedes,
        changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
        envelope_version: u32,
        control: Option<&SyncControl>,
        coverage: Option<&Arc<crate::cursor::PendingCoverage>>,
        writer_tx: Option<&tokio::sync::mpsc::Sender<crate::multiplexer::WriterRequest>>,
        generation: u64,
        lane: Option<&crate::engine::lane::LaneGate>,
        delivery: &crate::multiplexer::ChangeDelivery,
    ) -> Result<BackfillPartitionOutcome, Error> {
        let _activity = match control {
            Some(control) => Some(control.begin_activity().ok_or(Error::Paused)?),
            None => None,
        };
        let partition_key = partition_key(&partition);
        let mut stream = account.inventory_partition_stream(scope.clone(), partition);
        let mut seen_total: u64 = 0;
        let mut kept_total: u64 = 0;
        let mut complete = true;
        let mut scope_walk = ScopeWalkStep::RequestNextPartition;
        let mut end = None;
        let mut walk = InventoryWalk::default();
        // The incarnation this PASS belongs to, snapshotted before its first
        // page. Read once and not per page: a reset that closes between page k's
        // publish and page k+1's read is invisible to a per-page reading, which
        // is taken after the reset and therefore compares the replacement
        // incarnation's fence with itself. Every page of this pass is judged
        // against the incarnation the pass started under, exactly as the
        // completion marker is judged against the incarnation its WALK started
        // under.
        let fence_at_pass_start = lane.map_or(0, |gate| gate.coverage().scope_fence(&scope));
        loop {
            // Cancellation is armed around the POLL and nowhere else.
            //
            // Not across the body: the page's registration and its broadcast
            // are one indivisible step, and stopping between them would mint a
            // publication no acknowledgement can ever retire. Everything the
            // previous page did - `walk.accept`, the publish pair, the
            // undelivered note and its retirement, the capacity wait and its
            // scope-fence revalidation, barrier and debt recording - has
            // therefore completed before control returns here.
            //
            // The outcome is an ERROR rather than a partial
            // `BackfillPartitionOutcome`, which is what makes it safe: an
            // outcome could only be reported as `complete: false` with
            // `StopScopeWalk`, and getting either field wrong would have the
            // orchestrator write a completion sentinel that makes the next
            // attach skip the scope entirely. That is silent data
            // invisibility, not re-work. `Error::ShuttingDown` is a shape this
            // function already produces at the capacity wait, and the
            // orchestrator already routes it to `driver.fail()`, which
            // withholds the completion marker.
            //
            // Only armed when a lane is present. Production always passes one;
            // a `lane: None` caller has no token, so the guarantee is about
            // engine-driven partition walks rather than about this function.
            let next = match lane {
                Some(gate) => {
                    tokio::select! {
                        biased;
                        () = gate.shutdown().cancelled() => return Err(Error::ShuttingDown),
                        next = stream.next() => next,
                    }
                }
                None => stream.next().await,
            };
            let Some(event) = next else { break };
            match event {
                bifrost_types::InventoryEvent::Batch(batch) => {
                    if let Err(violation) = batch.validate_boundary() {
                        let error = crate::recovery::batch_boundary_violation(
                            violation,
                            batch.checkpoint.offered(),
                            bifrost_types::AccountOperation::SyncInventory,
                            &scope,
                        );
                        if let Some(tx) = &changes_tx {
                            let _ = tx.send(MultiplexerEvent::unacked(
                                scope.clone(),
                                Arc::new(SyncEvent::Terminated(error.clone())),
                            ));
                        }
                        return Err(Error::Account(error));
                    }
                    let seen = u64::try_from(batch.items.len().saturating_add(batch.removed.len()))
                        .unwrap_or(u64::MAX);
                    seen_total = seen_total.saturating_add(seen);
                    let (kept, kept_removed) =
                        filter_inventory_supersedes(&batch.items, &batch.removed, live);
                    let kept_count = u64::try_from(kept.len()).unwrap_or(u64::MAX);
                    kept_total = kept_total.saturating_add(kept_count);
                    let projected_removals = if changes_tx.is_some() {
                        match crate::inventory_walk::removals(&scope, kept_removed.iter().copied())
                        {
                            Ok(removals) => removals,
                            Err(error) => {
                                if let Some(tx) = &changes_tx {
                                    let _ = tx.send(MultiplexerEvent::unacked(
                                        scope.clone(),
                                        Arc::new(SyncEvent::Terminated(error.clone())),
                                    ));
                                }
                                return Err(Error::Account(error));
                            }
                        }
                    } else {
                        Vec::new()
                    };

                    let crossed = if batch.coverage.has_barrier() {
                        cross_waived_barriers(writer_tx, &batch.coverage, generation).await?
                    } else {
                        false
                    };
                    if !batch.coverage.is_complete() && !crossed {
                        complete = false;
                    }
                    if !crossed
                        && let WalkDecision::StopAtBarrier { resume_from } =
                            walk.inspect(&batch.coverage)
                    {
                        // Announce nothing the writer could not durably record:
                        // a barrier that only exists in this process is one a
                        // restart forgets and no operator can act on. Failing
                        // the partition instead leaves the scope Pending and
                        // re-walks it, which is recoverable.
                        record_barriers(writer_tx, &batch.coverage, generation, resume_from)
                            .await?;
                        if let Some(tx) = &changes_tx {
                            let changes = kept
                                .into_iter()
                                .map(|entry| {
                                    Change::ObjectChange(ObjectChange {
                                        id: entry.id.clone(),
                                        kind: ObjectChangeKind::Created,
                                    })
                                })
                                .chain(projected_removals)
                                .collect();
                            let synthetic = Batch {
                                items: changes,
                                page_boundary: batch.page_boundary,
                                server_latency: batch.server_latency,
                                bytes_in: batch.bytes_in,
                                checkpoint: None,
                            };
                            let _ = tx.send(MultiplexerEvent::unacked(
                                scope.clone(),
                                Arc::new(SyncEvent::Batch(synthetic)),
                            ));
                            warn_barrier(tx, &scope, &batch.coverage);
                        }
                        return Ok(BackfillPartitionOutcome {
                            seen: seen_total,
                            kept: kept_total,
                            complete: false,
                            scope_walk: ScopeWalkStep::StopScopeWalk,
                            end: None,
                        });
                    }

                    // Forward the page as a synthetic Batch carrying a
                    // BackfillCheckpoint. Durable persistence awaits the
                    // consumer ack (see module docs); the runner never
                    // writes to CheckpointStore.
                    if changes_tx.is_some() {
                        // FLOW CONTROL, and it belongs exactly here: after the
                        // page has been read off the provider stream and BEFORE
                        // it is registered. Parking here stops the stream being
                        // polled again, so the account is not asked for the next
                        // page while `lane_capacity` are unanswered.
                        //
                        // Before the registration and not after, because the
                        // registration IS the capacity: waiting afterwards would
                        // be waiting on a bound this page has already consumed.
                        // The wait also hands the account's scheduler admission
                        // back for its duration, so a parked cold start is not
                        // sitting on the permit polling needs.
                        // The incarnation this PASS was started under - see
                        // `fence_at_pass_start`. A capacity wait is indefinite
                        // and a scope reset can close inside it, but it can just
                        // as well close between two pages, so the comparison is
                        // against the pass, not against this page.
                        let fence_before = fence_at_pass_start;
                        if let Some(gate) = lane {
                            match gate.wait_for_capacity_holding_admission().await {
                                Ok(()) => {}
                                // The account is detaching. Publishing now would
                                // broadcast into a teardown; the partition fails,
                                // the scope stays Pending, and the next attach
                                // re-walks it.
                                Err(crate::engine::lane::WaitFailed::ShuttingDown) => {
                                    return Err(Error::ShuttingDown);
                                }
                                // Transient scheduler pressure. Same treatment as
                                // any other refused admission on a repeating work
                                // path: fail the partition, leave the scope
                                // Pending, come back on the rescan ramp.
                                Err(crate::engine::lane::WaitFailed::Refused(error)) => {
                                    return Err(error);
                                }
                            }
                            // REVALIDATE the incarnation before publishing. A
                            // reset that closed anywhere in this pass - while
                            // this page was parked on the bound, or between two
                            // of the pass's pages - deleted the scope's rows and
                            // fenced every publication minted before it, but this
                            // page has not been minted yet, so it would come back
                            // with a FRESH id above the fence, acknowledge
                            // normally, and recreate exactly the durable state the
                            // reset dropped. A stale completion marker arriving
                            // that way is worse than a stale page: it suppresses
                            // the replacement incarnation's whole inventory walk.
                            if gate.coverage().scope_fence(&scope) != fence_before {
                                tracing::warn!(
                                    target: "bifrost.sync.backfill",
                                    scope = ?scope,
                                    "scope was reset while this backfill pass was in \
                                     flight; abandoning the page rather than publishing \
                                     it against rows the reset deleted"
                                );
                                return Err(Error::Other(
                                    "scope reset while the backfill pass was in flight".into(),
                                ));
                            }
                        }
                        let bf = BackfillCheckpoint {
                            scope: scope.clone(),
                            partition: partition_key.clone(),
                            progress_marker: None,
                            progress: BackfillProgress {
                                items_done: seen_total,
                                items_estimated: None,
                            },
                            envelope_version,
                        };
                        let changes: Vec<Change> = kept
                            .into_iter()
                            .map(|entry| {
                                Change::ObjectChange(ObjectChange {
                                    id: entry.id.clone(),
                                    kind: ObjectChangeKind::Created,
                                })
                            })
                            .chain(projected_removals)
                            .collect();
                        let synthetic = Batch {
                            items: changes,
                            page_boundary: batch.page_boundary,
                            server_latency: batch.server_latency,
                            bytes_in: batch.bytes_in,
                            checkpoint: Some(Checkpoint::Backfill(bf.clone())),
                        };
                        // REGISTER THE COVERAGE CLAIM. Without this the page's
                        // obligations never reach the durable record at all:
                        // the runner used to read `batch.coverage` only to
                        // clear its local `complete` flag, so an acknowledged
                        // degraded page persisted a record asserting complete
                        // coverage over objects nothing had recorded - the
                        // exact invariant this machinery exists to enforce.
                        //
                        // Note the claim rides the PUBLICATION, not the scope.
                        // Two partitions of one scope are in flight at once, so
                        // a scope-keyed claim would let partition B's report be
                        // persisted under partition A's acknowledgement.
                        let expected = Checkpoint::Backfill(bf);
                        walk.accept(Some(expected.clone()));
                        let publication = control.map(|control| {
                            control.publish_checkpoint(
                                expected.clone(),
                                crate::cursor::CoverageClaim::new(
                                    batch.coverage.clone(),
                                    generation,
                                ),
                            )
                        });
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Batch(synthetic)),
                            checkpoint: Some(expected.clone()),
                            publication: publication.clone(),
                        };
                        // Send and stamp as ONE step, under the delivery lock.
                        // The stamp says which receivers could possibly hold this
                        // batch, and that is only true if no subscribe and no
                        // departing receiver's sweep can interleave between the
                        // two - see `ChangeDelivery`.
                        let reached = match lane {
                            Some(gate) => {
                                delivery.publish_backfill(me, gate.coverage(), publication.as_ref())
                            }
                            None => delivery.publish_acknowledgeable(me),
                        };
                        if !reached {
                            // This page reached nobody. Record it against the
                            // account's undelivered watermark so the enclosing
                            // walk cannot later settle the scope on the strength
                            // of a subscriber who was not there for this page.
                            if let Some(gate) = lane {
                                gate.coverage().note_undelivered(&scope, 1);
                            }
                            match (control, coverage, publication) {
                                // Through control when there is one: it
                                // releases the boundary registration and the
                                // claim together, which are one publication.
                                (Some(control), _, Some(id)) => {
                                    control.retire_publication(id);
                                }
                                (None, Some(pending), Some(id)) => pending.retire(id),
                                _ => {}
                            }
                        }
                    }
                }
                bifrost_types::InventoryEvent::Done(completion) => {
                    // The same contract check every page gets. The runner mints
                    // its own positional checkpoint and never takes the
                    // account's, but a completion whose withheld state
                    // contradicts its coverage is an account breaking its
                    // stream contract, and its `end` declaration is no more
                    // trustworthy than its checkpoint.
                    if let Err(violation) = completion.validate() {
                        let error = crate::recovery::batch_boundary_violation(
                            violation,
                            completion.checkpoint.offered(),
                            bifrost_types::AccountOperation::SyncInventory,
                            &scope,
                        );
                        if let Some(tx) = &changes_tx {
                            let _ = tx.send(MultiplexerEvent::unacked(
                                scope.clone(),
                                Arc::new(SyncEvent::Terminated(error.clone())),
                            ));
                        }
                        return Err(Error::Account(error));
                    }
                    end = Some(completion.end);
                    let crossed = if completion.coverage.has_barrier() {
                        cross_waived_barriers(writer_tx, &completion.coverage, generation).await?
                    } else {
                        false
                    };
                    if !crossed
                        && let WalkDecision::StopAtBarrier { resume_from } =
                            walk.inspect(&completion.coverage)
                    {
                        complete = false;
                        scope_walk = ScopeWalkStep::StopScopeWalk;
                        record_barriers(writer_tx, &completion.coverage, generation, resume_from)
                            .await?;
                        if let Some(tx) = &changes_tx {
                            warn_barrier(tx, &scope, &completion.coverage);
                        }
                        break;
                    }
                    if !completion.coverage.is_complete() && !crossed {
                        complete = false;
                        // The terminal summary has no checkpoint of its own to
                        // ride, so its obligations reach the ledger directly -
                        // as DEBT ONLY, never proof. Both current providers
                        // make this report cumulative with the final page, so
                        // in practice this is usually a re-assertion of what a
                        // page claim already carried; re-raising an obligation
                        // is idempotent and preserves its history. Relying on
                        // that cumulativeness instead would be an assumption
                        // about every future provider.
                        if let Some(writer) = writer_tx {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            if writer
                                .send(crate::multiplexer::WriterRequest::RecordDebt {
                                    report: completion.coverage.clone(),
                                    generation,
                                    done: done_tx,
                                })
                                .await
                                .is_ok()
                            {
                                let _ = done_rx.await;
                            }
                        }
                    }
                    break;
                }
                bifrost_types::InventoryEvent::Terminated(err) => {
                    if let Some(tx) = &changes_tx {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Terminated(err.clone())),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = tx.send(me);
                    }
                    return Err(Error::Account(err));
                }
                // Forwarded for the same reason the fusion walk forwards it:
                // a producer-emitted warning is the only announcement of a
                // degrade the consumer would otherwise have to infer (Graph's
                // public-folder additions-only mode, IMAP's QRESYNC ->
                // CONDSTORE downgrade), and the IMAP one is a one-shot that
                // cannot be re-emitted once a lane has consumed it.
                bifrost_types::InventoryEvent::Warning(warning) => {
                    if let Some(tx) = &changes_tx {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Warning(warning)),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = tx.send(me);
                    }
                }
                bifrost_types::InventoryEvent::Progress(_) => {}
                _ => {}
            }
        }
        // Every completion sets `end` before its `break`, so `None` here means
        // the stream ended with neither `Done` nor `Terminated`. Nothing proved
        // how far the enumeration got, so the partition is incomplete whatever
        // the plan: a fixed plan used to read this as complete and write the
        // scope's completion marker over ground nothing covered, which made the
        // next attach skip the walk for good.
        if end.is_none() {
            complete = false;
            if let Some(tx) = &changes_tx {
                let _ = tx.send(MultiplexerEvent::unacked(
                    scope.clone(),
                    Arc::new(SyncEvent::Warning(
                        crate::inventory_walk::unannounced_end_warning(),
                    )),
                ));
            }
        }
        Ok(BackfillPartitionOutcome {
            seen: seen_total,
            kept: kept_total,
            complete,
            scope_walk,
            end,
        })
    }
}

fn warn_barrier(
    tx: &broadcast::Sender<MultiplexerEvent>,
    scope: &CursorScope,
    coverage: &bifrost_types::InventoryCoverageReport,
) {
    let count = coverage
        .obligations()
        .iter()
        .filter(|obligation| obligation.is_barrier())
        .count();
    let warning = bifrost_types::Warning::user_safe(
        bifrost_types::WarningKind::OperatorAttentionNeeded,
        format!(
            "inventory stopped at {count} region(s) it cannot represent and cannot replay; \
             the scope cannot advance past them until an operator waives them"
        ),
    );
    let _ = tx.send(MultiplexerEvent::unacked(
        scope.clone(),
        Arc::new(SyncEvent::Warning(warning)),
    ));
}

fn filter_inventory_supersedes<'a>(
    items: &'a [InventoryEntry],
    removed: &'a [ObjectId],
    live: &LiveSupersedes,
) -> (Vec<&'a InventoryEntry>, Vec<&'a ObjectId>) {
    if live.is_empty() {
        return (items.iter().collect(), removed.iter().collect());
    }

    // A live re-add supersedes every stale inventory observation of its id,
    // including a tombstone. Forwarding that removal after the re-add could
    // erase the object the live event just restored.
    let mut superseded = HashSet::new();
    for id in items.iter().map(|entry| &entry.id).chain(removed.iter()) {
        if !superseded.contains(id) && live.take(id) {
            superseded.insert(id.clone());
        }
    }

    (
        items
            .iter()
            .filter(|entry| !superseded.contains(&entry.id))
            .collect(),
        removed
            .iter()
            .filter(|id| !superseded.contains(*id))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{Fingerprint, InventoryEntry, ObjectId, ServerVersion};

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

    #[test]
    fn an_unpopulated_set_forwards_every_inventory_entry() {
        // The production state: nothing calls `add`, so backfill keeps
        // every entry. Pinned deliberately - a change that starts
        // populating the set flips this test, which is the moment to
        // re-read the `LiveSupersedes` docs on why "we broadcast it" is
        // not evidence the consumer received it.
        let live = LiveSupersedes::new();
        let page = vec![entry("a"), entry("b"), entry("c")];
        let (kept, removed) = filter_inventory_supersedes(&page, &[], &live);
        assert_eq!(kept.len(), 3);
        assert!(removed.is_empty());
        assert!(live.is_empty());
    }

    #[test]
    fn live_supersedes_filters_inventory_removals_and_same_page_entries() {
        let live = LiveSupersedes::new();
        let id = ObjectId("readded".into());
        live.add(id.clone());
        let removed_only_id = ObjectId("readded-through-removal".into());
        live.add(removed_only_id.clone());

        let items = vec![entry("readded"), entry("still-present")];
        let removed = vec![id, removed_only_id, ObjectId("still-removed".into())];
        let (kept, kept_removed) = filter_inventory_supersedes(&items, &removed, &live);

        assert_eq!(
            kept.iter()
                .map(|item| item.id.0.as_str())
                .collect::<Vec<_>>(),
            vec!["still-present"]
        );
        assert_eq!(
            kept_removed
                .iter()
                .map(|id| id.0.as_str())
                .collect::<Vec<_>>(),
            vec!["still-removed"]
        );
        assert!(live.is_empty());
    }

    #[test]
    fn supersedes_records_and_consumes() {
        let live = LiveSupersedes::new();
        let id = ObjectId("abc".into());
        live.add(id.clone());
        assert_eq!(live.len(), 1);
        assert!(live.take(&id));
        assert!(live.is_empty());
        assert!(!live.take(&id));
    }

    #[test]
    fn take_leaves_the_ring_slot_alone() {
        // `take` is O(1) because it never touches `order`. The
        // observable footprint of that is the ring keeping its slot
        // count across a hit; a scan-out would drop it to 1.
        let live = LiveSupersedes::with_capacity(8);
        live.add(ObjectId("a".into()));
        live.add(ObjectId("b".into()));
        assert_eq!(live.ring_len(), 2);
        assert!(live.take(&ObjectId("a".into())));
        assert_eq!(live.ring_len(), 2, "the tombstone stays in the ring");
        assert_eq!(live.len(), 1, "but it no longer suppresses anything");
    }

    #[test]
    fn a_tombstone_never_causes_a_suppression() {
        // The one outcome worse than a duplicate emit would be
        // suppressing an object that should have been forwarded.
        // Suppression reads `present` only, so a slot lingering in
        // `order` can never grant one - not before eviction, not after,
        // and not while other ids churn past it.
        let live = LiveSupersedes::with_capacity(4);
        let ghost = ObjectId("ghost".into());
        live.add(ghost.clone());
        assert!(live.take(&ghost));
        for id in ["p", "q", "r", "s", "t", "u"] {
            live.add(ObjectId(id.into()));
            assert!(
                !live.take(&ghost),
                "a tombstoned id must stay unsuppressed however the ring churns"
            );
        }
    }

    #[test]
    fn tombstones_do_not_break_the_capacity_bound() {
        // Ring slots are still bounded by `cap` even though `take`
        // leaves them behind, so a take-heavy cold start cannot grow
        // the set without limit.
        let live = LiveSupersedes::with_capacity(4);
        for n in 0..64 {
            live.add(ObjectId(format!("id-{n}")));
            if n % 3 == 0 {
                let _ = live.take(&ObjectId(format!("id-{n}")));
            }
            assert!(live.ring_len() <= 4);
            assert!(live.len() <= live.ring_len());
        }
    }

    #[test]
    fn evicting_a_tombstone_lets_membership_grow_back_to_capacity() {
        // Popping a tombstone removes nothing from `present`, so the
        // live set reconverges on `cap` instead of being permanently
        // eroded by earlier takes. This is why eviction needs no
        // skip-stale loop: the plain single-pop self-heals.
        let live = LiveSupersedes::with_capacity(3);
        live.add(ObjectId("a".into()));
        live.add(ObjectId("b".into()));
        live.add(ObjectId("c".into()));
        assert!(live.take(&ObjectId("a".into())));
        assert_eq!(live.len(), 2);
        assert_eq!(live.ring_len(), 3, "a's slot is a tombstone");

        // "a"'s tombstone is at the front, so this add reclaims it and
        // spares a live entry.
        live.add(ObjectId("d".into()));
        assert_eq!(live.len(), 3);
        assert!(live.take(&ObjectId("b".into())));
        assert!(live.take(&ObjectId("c".into())));
        assert!(live.take(&ObjectId("d".into())));
    }

    #[test]
    fn re_adding_a_taken_id_costs_at_most_a_duplicate() {
        // The single behavioral consequence of tombstoning: a taken id
        // that the live stream announces again owns two ring slots, so
        // evicting the older one drops the live entry early. That costs
        // a suppression (a duplicate emit), never a wrong suppression.
        let live = LiveSupersedes::with_capacity(2);
        let x = ObjectId("x".into());
        live.add(x.clone());
        assert!(live.take(&x));
        live.add(x.clone());
        assert!(live.len() <= 2);
        // Whatever eviction does to x's two slots, the invariant that
        // matters holds: x is suppressed at most once more.
        live.add(ObjectId("y".into()));
        live.add(ObjectId("z".into()));
        // Whichever way eviction resolved x's two slots, x is
        // suppressible at most once more - never twice off one `add`,
        // and never after it has been consumed.
        let _ = live.take(&x);
        assert!(!live.take(&x), "never suppressible twice from one add");
    }

    #[test]
    fn supersedes_caps_at_capacity() {
        let live = LiveSupersedes::with_capacity(3);
        live.add(ObjectId("a".into()));
        live.add(ObjectId("b".into()));
        live.add(ObjectId("c".into()));
        live.add(ObjectId("d".into()));
        assert_eq!(live.len(), 3);
        // "a" was evicted; "b","c","d" remain.
        assert!(!live.take(&ObjectId("a".into())));
        assert!(live.take(&ObjectId("b".into())));
        assert!(live.take(&ObjectId("c".into())));
        assert!(live.take(&ObjectId("d".into())));
    }
}
