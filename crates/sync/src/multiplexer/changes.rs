//! `changes_stream` driver.
//!
//! `drive_changes_stream` consumes an `Account::changes_stream(cursor)`
//! result and translates each `SyncEvent<Change>` into a
//! `MultiplexerEvent` tagged with the originating scope. The driver
//! broadcasts the event (with its checkpoint, if any) to the per-account
//! broadcast channel and updates the in-memory cursor registry so the
//! next poll iteration uses the advanced cursor.
//!
//! ## Cursor-durability contract
//!
//! The driver does NOT persist the cursor to `CheckpointStore`. The
//! consumer is responsible for calling
//! `SyncEngine::ack_checkpoint(account, checkpoint)` after they have
//! durably persisted the batch items in their own store. Only then does
//! the engine persist the cursor.
//!
//! This shape preserves the trait contract that `Batch` carries
//! `(items, checkpoint)` for atomic consumer persistence. If the engine
//! persists the cursor before the consumer acknowledges, a crash
//! between broadcast and the consumer's write loses items. The ack
//! channel closes the gap.
//!
//! The in-memory `CursorRegistry` IS advanced on broadcast so polling
//! makes forward progress within a single session; only the durable
//! `CheckpointStore` write awaits the ack. On restart the engine reads
//! the last-acked cursor from the store and re-runs `changes_stream`
//! from there - any items the consumer never durably persisted come
//! across again.

use std::sync::Arc;

use bifrost_types::{
    Account, AccountError, Change, ChangeCursor, Checkpoint, CursorScope, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::oneshot;

use crate::cancel::{BoundaryRequest, BoundaryView};
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::Error;

use super::{ChangeDelivery, MultiplexerEvent};

/// Per-batch driver outcome. Workers consult this between batches to
/// decide whether to keep pulling.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ChangesEvent {
    /// Driver advanced past a `Batch` with a cursor checkpoint.
    Advanced,
    /// Driver saw a `Done` and exited cleanly.
    Done,
    /// Driver was asked to stop (boundary request `Stop`).
    Stopped,
    /// Driver was asked to pause, reached a clean boundary BETWEEN stream
    /// items, and released its activity guard.
    ///
    /// Deliberately does NOT promise a flushed checkpoint, which is what this
    /// said before and was never true: `post_publish_boundary` answers `Pause`
    /// after a checkpoint-free item as well, because a checkpoint-free stream
    /// that only parked at checkpoints would never let the account reach
    /// quiescence at all. A pause that cuts a pending poll is the same
    /// guarantee one item earlier - nothing is half-published, and no
    /// registration exists for the item that was never read.
    /// `CheckpointNow` is the request that does need a durable cursor.
    Paused,
    /// Driver saw a terminating stream event and aborted. Carries the
    /// full `AccountError` so the engine dispatches via the derived
    /// `RecoveryClass` (Retry / Reconcile / Engine directive /
    /// terminal). The name reflects that non-terminal recoveries can
    /// also terminate a stream and resume after dispatch.
    Terminated(AccountError),
}

/// Consume a `changes_stream` to completion or to the next boundary
/// request, broadcasting each event onto the per-account channel.
///
/// The in-memory cursor registry is updated when a checkpoint arrives;
/// the durable `CheckpointStore` write happens through
/// `SyncEngine::ack_checkpoint`, not here. See module docs for the
/// rationale.
///
/// This driver deliberately does NOT feed `LiveSupersedes`. Having
/// broadcast a change is not evidence the consumer received it, so it
/// is not a sound basis for suppressing the object's inventory copy;
/// see the `LiveSupersedes` type docs for the full argument.
///
/// A published change checkpoint always carries its `PublicationId`. The claim
/// that it does not was raised and REFUTED with evidence, so do not re-file it:
/// the `publish` closure sets the publication whenever `control` and
/// `checkpoint` are both present, both production callers
/// (`multiplexer/mod.rs` and `push/reconciler.rs`) pass `Some(control)`, and
/// `emit_backfill_complete` publishes unconditionally. The invariant is now
/// pinned by `a_published_change_checkpoint_always_carries_its_publication_id`
/// (ablated - it bites), which is what makes it enforced rather than merely
/// observed. New evidence, not a re-reading, is what would reopen this.
///
/// Takes the account's `ChangeDelivery` rather than its raw sender because a
/// published checkpoint registers a claim that only a NUMBERED receiver can
/// answer for, and only the delivery gate knows whether one took delivery.
/// Events that expect no acknowledgement still go on the raw sender.
#[allow(clippy::too_many_arguments)]
pub async fn drive_changes_stream(
    account: &dyn Account,
    scope: CursorScope,
    cursor: ChangeCursor,
    cursors: Arc<CursorRegistry>,
    delivery: Arc<ChangeDelivery>,
    mut boundary: BoundaryView,
    control: Option<SyncControl>,
    registry_generation: Option<crate::cursor::DriveGeneration>,
) -> Result<ChangesEvent, Error> {
    let _activity = match &control {
        Some(control) => match control.begin_activity() {
            Some(activity) => Some(activity),
            None => return Ok(refused_activity_outcome(boundary.peek())),
        },
        None => None,
    };
    let mut stream = account.changes_stream(cursor);
    loop {
        let event = match poll_next_event(&mut stream, &mut boundary).await {
            PolledEvent::Event(event) => event,
            PolledEvent::StreamEnded => return Ok(ChangesEvent::Done),
            PolledEvent::Boundary(outcome) => return Ok(outcome),
        };
        // A batch the account should never have produced. Returning a bare
        // engine error here would be a livelock: the poll loop logs it,
        // re-enters from the SAME unchanged cursor, and the account
        // reproduces it forever while consumers see nothing at all. Classify
        // it, publish a `Terminated` so subscribers learn the scope stopped,
        // and hand the engine a terminal recovery to dispatch.
        //
        // The offending batch is NOT forwarded with its checkpoint stripped,
        // the way fusion forwards a barrier page. A barrier is a legitimate
        // provider signal whose items were genuinely observed; this is an
        // account contradicting its own stream contract, and there is no
        // reason to trust its items more than its boundary.
        if let SyncEvent::Batch(batch) = &event
            && let Err(violation) = batch.validate_boundary()
        {
            let error = crate::recovery::batch_boundary_violation(
                violation,
                batch.checkpoint.as_ref(),
                bifrost_types::AccountOperation::SyncChanges,
                &scope,
            );
            if !publish_contract_termination(
                &cursors,
                &delivery,
                &scope,
                registry_generation,
                &error,
            ) {
                return Ok(ChangesEvent::Done);
            }
            return Ok(ChangesEvent::Terminated(error));
        }
        let checkpoint = checkpoint_for(&event).cloned();
        // A BACKFILL checkpoint on the live changes stream is refused, exactly as
        // the fusion path refuses one, and for the same structural reason: this
        // driver registers the publication and then publishes it unstamped
        // (`publish_acknowledgeable`, not `publish_backfill`), so the entry is
        // never stamped with a delivery. An unsent stamp is
        // deliberately never swept, so a `Lane::Backfill` entry minted here is
        // charged against the account's backfill bound until an acknowledgement,
        // a lag or a reset frees it - a permanent slot of the bound spent per
        // scope. And if its partition happened to be the completion sentinel, its
        // acknowledgement would reach `Unvouchable` in this very attachment,
        // because no walk ever stamped it with a reading. Refusing is the safe
        // direction and costs nothing: no provider in this workspace emits one.
        if matches!(&checkpoint, Some(Checkpoint::Backfill(_))) {
            let error = crate::recovery::backfill_checkpoint_on_changes(
                checkpoint.as_ref(),
                bifrost_types::AccountOperation::SyncChanges,
                &scope,
            );
            if !publish_contract_termination(
                &cursors,
                &delivery,
                &scope,
                registry_generation,
                &error,
            ) {
                return Ok(ChangesEvent::Done);
            }
            return Ok(ChangesEvent::Terminated(error));
        }
        if let Some(Checkpoint::Change(cursor)) = &checkpoint
            && cursor.validate_envelope().is_err()
        {
            let error = crate::recovery::fresh_cursor_contract_error(
                checkpoint.as_ref(),
                bifrost_types::AccountOperation::SyncChanges,
                &scope,
            );
            if !publish_contract_termination(
                &cursors,
                &delivery,
                &scope,
                registry_generation,
                &error,
            ) {
                return Ok(ChangesEvent::Done);
            }
            return Err(Error::Account(error));
        }
        let is_done = matches!(&event, SyncEvent::Done(_));
        // Capture the full `AccountError` so the engine has the
        // derived recovery, scope, operation, provider, protocol, and
        // diagnostics on hand.
        let terminated_error = if let SyncEvent::Terminated(err) = &event {
            Some(err.clone())
        } else {
            None
        };
        let mut me = MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(event),
            checkpoint: checkpoint.clone(),
            publication: None,
        };
        // Broadcast (item + checkpoint) together. Subscribers persist
        // `(items, checkpoint)` atomically in their own store, then
        // call `SyncEngine::ack_checkpoint` to durably advance the
        // engine's cursor.
        // Register the outstanding checkpoint BEFORE publishing it.
        // A consumer that receives, persists and acks between the send
        // and the registration would otherwise leave behind an entry
        // no ack can ever match, permanently wedging boundary waiters.
        let mut publish = || {
            let expected = match (&control, &checkpoint) {
                (Some(control), Some(checkpoint)) => {
                    let publication =
                        control.publish_checkpoint_without_report(checkpoint.clone(), 0);
                    me.publication = Some(publication.clone());
                    Some(publication)
                }
                _ => None,
            };
            (delivery.publish_acknowledgeable(me.clone()), expected)
        };
        let change_cursor = match &checkpoint {
            Some(Checkpoint::Change(cursor)) => Some(cursor.clone()),
            _ => None,
        };
        let published = match registry_generation {
            Some(generation) => {
                cursors.publish_if_drive_generation(change_cursor, &scope, generation, publish)
            }
            None => {
                let result = publish();
                if let Some(cursor) = change_cursor {
                    cursors.put(cursor);
                }
                Some(result)
            }
        };
        let Some((reached, expected)) = published else {
            return Ok(ChangesEvent::Done);
        };
        if !reached && let (Some(control), Some(expected)) = (&control, expected) {
            // No numbered receiver saw this batch - only the slot's sentinel
            // and any observers - so no consumer ack is coming for it.
            control.retire_publication(expected);
        }
        if let Some(outcome) =
            published_outcome(terminated_error, boundary.peek(), checkpoint.is_some())
        {
            return Ok(outcome);
        }
        if is_done {
            return Ok(ChangesEvent::Done);
        }
        // No fallthrough: a stream that ends is answered by
        // `PolledEvent::StreamEnded` at the top of the loop.
    }
}

/// The result of asking the provider stream for one more event while watching
/// the boundary.
enum PolledEvent {
    Event(SyncEvent<Change>),
    StreamEnded,
    /// The boundary asked the drive to stop before another event arrived.
    Boundary(ChangesEvent),
}

/// Poll the provider stream for one event, cutting the poll short if the
/// boundary asks the drive to stop or park.
///
/// Without this, a stream that neither yields nor ends parks the drive
/// forever. That is worse here than the phrase "straggler worker" suggests: the
/// per-scope poll tasks are spawned with their `JoinHandle` discarded, so a
/// wedged drive is not in `slot.workers` and `detach` neither awaits nor aborts
/// it. It costs detach no time at all and SURVIVES it, holding an
/// `Arc<dyn Account>`, a scheduler admission and a `ChangeDelivery`, still able
/// to publish and to register publications against a `PendingCoverage` nothing
/// owns any more. And `SyncControl::pause` waits for quiescence while the
/// drive's activity guard is held, so the same wedge hangs `pause` too.
///
/// The PEEK is not redundant with the select. `changed()` reports transitions
/// the receiver has not seen, so a request already in force when the drive was
/// built is never reported by it - only read. The SELECT is not redundant with
/// the peek either: without it a request arriving mid-poll waits for an event
/// that may never come.
///
/// `CheckpointNow` deliberately does NOT cut the poll. It is the one request
/// whose meaning is "give me a durable cursor", which only a checkpoint-bearing
/// event can satisfy, so interrupting the wait for one would answer it by
/// abandoning it. `Stop` and `Pause` are lifecycle requests and neither needs a
/// checkpoint to be honoured.
///
/// Cutting the poll abandons no data. An item only crosses the `Stream`
/// boundary through `Poll::Ready(Some(_))`, and `StreamExt::next` borrows the
/// stream rather than owning an item, so a poll that returned `Pending`
/// produced nothing to lose. What it CAN abandon is provider-internal work,
/// which is why `Account::changes_stream` requires its streams to tolerate
/// being dropped while pending.
async fn poll_next_event(
    stream: &mut bifrost_types::AccountStream<SyncEvent<Change>>,
    boundary: &mut BoundaryView,
) -> PolledEvent {
    loop {
        match boundary.peek() {
            BoundaryRequest::Stop => return PolledEvent::Boundary(ChangesEvent::Stopped),
            BoundaryRequest::Pause => return PolledEvent::Boundary(ChangesEvent::Paused),
            BoundaryRequest::Run | BoundaryRequest::CheckpointNow => {}
        }
        tokio::select! {
            next = stream.next() => {
                return match next {
                    Some(event) => PolledEvent::Event(event),
                    None => PolledEvent::StreamEnded,
                };
            }
            request = boundary.changed() => match request {
                // Every sender gone means nobody is left who could manage this
                // drive, which is the same situation as a stop.
                None | Some(BoundaryRequest::Stop) => {
                    return PolledEvent::Boundary(ChangesEvent::Stopped);
                }
                Some(BoundaryRequest::Pause) => {
                    return PolledEvent::Boundary(ChangesEvent::Paused);
                }
                // Not every transition is a cancellation: a checkpoint request,
                // or a restoration to `Run`, must resume the poll rather than
                // end the drive.
                Some(BoundaryRequest::Run | BoundaryRequest::CheckpointNow) => continue,
            },
        }
    }
}

/// What a drive reports when `begin_activity` refuses to admit it.
///
/// The refusal only says "the boundary is not `Run`", so the boundary itself
/// has to name the outcome. Reporting `Paused` under a `Stop` boundary told
/// the poll loop to park and re-enter at the top of the next iteration, which
/// is exactly the loop that a stopping account is trying to wind down; the
/// task only exited once something else noticed. `Stopped` is the exit signal
/// (`DriveRecovery::exit`), so it has to come from here.
fn refused_activity_outcome(request: BoundaryRequest) -> ChangesEvent {
    match request {
        BoundaryRequest::Stop => ChangesEvent::Stopped,
        // A drive refused under Pause / CheckpointNow / Run is a park: the
        // poll loop's own boundary check owns what happens next. (Run is
        // reachable here as a race - the boundary can flip between the
        // refusal and this read.)
        BoundaryRequest::Pause | BoundaryRequest::CheckpointNow | BoundaryRequest::Run => {
            ChangesEvent::Paused
        }
    }
}

// A published termination owns recovery even if a boundary changed while the
// provider was being polled. Boundary handling resumes after recovery dispatch.
fn published_outcome(
    terminated: Option<bifrost_types::AccountError>,
    request: BoundaryRequest,
    has_checkpoint: bool,
) -> Option<ChangesEvent> {
    terminated
        .map(ChangesEvent::Terminated)
        .or_else(|| post_publish_boundary(request, has_checkpoint))
}

/// Contract failures belong to the account incarnation that produced them.
/// Fence the diagnostic and its recovery outcome together, just like batches:
/// an obsolete drive must neither terminate nor request recovery of its successor.
fn publish_contract_termination(
    cursors: &CursorRegistry,
    delivery: &ChangeDelivery,
    scope: &CursorScope,
    generation: Option<crate::cursor::DriveGeneration>,
    error: &AccountError,
) -> bool {
    let publish = || {
        let _ = delivery.sender().send(MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(SyncEvent::Terminated(error.clone())),
            checkpoint: None,
            publication: None,
        });
    };
    match generation {
        Some(generation) => cursors
            .publish_if_drive_generation(None, scope, generation, publish)
            .is_some(),
        None => {
            publish();
            true
        }
    }
}

fn post_publish_boundary(request: BoundaryRequest, has_checkpoint: bool) -> Option<ChangesEvent> {
    match request {
        // Pause is a lifecycle boundary, not a request for a durable
        // cursor. A checkpoint-free batch is still a completed stream
        // item and must release the activity guard here, otherwise a
        // valid checkpoint-free stream can keep the account non-quiescent
        // forever.
        BoundaryRequest::Pause => Some(ChangesEvent::Paused),
        BoundaryRequest::Stop => Some(ChangesEvent::Stopped),
        BoundaryRequest::CheckpointNow if has_checkpoint => Some(ChangesEvent::Done),
        BoundaryRequest::CheckpointNow | BoundaryRequest::Run => None,
    }
}

/// Ack request: scope + checkpoint the engine should durably persist.
/// Consumers send these through `SyncEngine::ack_checkpoint` after
/// their own item store commits the matching batch.
pub struct AckRequest {
    pub scope: CursorScope,
    pub checkpoint: Checkpoint,
    /// Which PUBLICATION of `checkpoint` this acknowledges.
    ///
    /// `Checkpoint` equality cannot serve: a backfill page whose content was
    /// entirely unrepresentable increments no item count and produces a
    /// byte-identical checkpoint to its predecessor, and inventory fusion
    /// publishes its final checkpoint twice. Without the identity, one
    /// publication's coverage claim gets applied to another's checkpoint.
    ///
    /// `None` only for engine-internal acks that carry no coverage claim.
    pub publication: Option<crate::cursor::PublicationId>,
    /// True for legacy engine-generated acks. Current production code
    /// sends only consumer acks (`false`), but the field remains for
    /// trace readability if old tests or callers construct requests
    /// directly inside the crate.
    pub auto: bool,
    /// Completion channel for consumer-driven acks. `ack_checkpoint`
    /// waits on this so it returns only after the checkpoint store
    /// has accepted or rejected the write.
    pub complete: Option<oneshot::Sender<Result<(), Error>>>,
}

impl std::fmt::Debug for AckRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AckRequest")
            .field("scope", &self.scope)
            .field("checkpoint", &self.checkpoint)
            .field("publication", &self.publication)
            .field("auto", &self.auto)
            .field("complete", &self.complete.is_some())
            .finish()
    }
}

/// Everything that may mutate durable checkpoint state for one account.
///
/// There is exactly ONE writer per account and every durable mutation goes
/// through this channel, because ordering between them is load-bearing.
/// Reattach used to write and delete cursors directly against the
/// `CheckpointStore` while the ack writer was concurrently persisting
/// acknowledged checkpoints for the same scopes, which is a race: a
/// replacement's inventory pass broadcasts checkpoint-bearing batches BEFORE
/// the cutover (`run_establish` hands `changes_tx` to `InventoryFusion`), a
/// consumer acknowledges one, and an aborted reattach then deleted the row
/// that acknowledgement had just committed.
///
/// Serializing is necessary but NOT sufficient on its own: a FIFO queue would
/// faithfully order the acknowledged write ahead of an unconditional delete and
/// then destroy it. The writer therefore tracks which scopes are still
/// provisional - see `ReattachInsert`.
#[non_exhaustive]
pub enum WriterRequest {
    /// Persist an acknowledged checkpoint. Also DISCHARGES any provisional
    /// mark on its scope: an acknowledgement is real committed consumer
    /// progress, and a later reattach abort must not delete it.
    Ack(AckRequest),
    /// Persist a cursor the reattach freshly created, and mark its scope
    /// provisional until the reattach commits or aborts.
    ///
    /// Provisional means "this row exists only because a replacement that has
    /// not cut over put it there". Only such rows may be rolled back, which is
    /// what lets the abort path compensate by plain deletion without ever
    /// touching preexisting durable state.
    ///
    /// `attempt` is the unique id of the reattach attempt inserting the row.
    /// The provisional mark is owned by that attempt, so a delayed abort or
    /// commit from an EARLIER attempt (a dropped reattach's guard can fall back
    /// to a spawned send with no ordering against the next reattach) cannot
    /// touch this one's rows. A later attempt inserting the same scope takes
    /// ownership of it.
    ReattachInsert {
        attempt: u64,
        cursor: ChangeCursor,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// The named reattach attempt aborted: delete the rows IT owns that are
    /// STILL provisional. A scope whose checkpoint was acknowledged in the
    /// meantime is no longer provisional and survives, and so does a row now
    /// owned by a different attempt.
    ReattachAbort {
        attempt: u64,
        done: oneshot::Sender<()>,
    },
    /// The named reattach attempt cut over: the rows it owns are now ordinary
    /// durable state. Rows owned by other attempts stay provisional.
    ReattachCommit { attempt: u64 },
    /// Read one cursor through the same account-owned boundary used for writes.
    ///
    /// `claim_for` is set only by a reattach's resume read. When the row is
    /// returned and is still provisional, ownership of it moves to that
    /// attempt, ordered with inserts, aborts and commits: the resuming attempt
    /// is about to install a topology that depends on the row, so a delayed
    /// abort from the attempt that inserted it must no longer delete it. A row
    /// that is not provisional is untouched, and a read with `None` (attach,
    /// scope restart, anything outside a reattach) never claims.
    GetChangeCursor {
        scope: CursorScope,
        claim_for: Option<u64>,
        done: oneshot::Sender<Result<Option<ChangeCursor>, Error>>,
    },
    /// Persist an established live cursor without making a coverage claim.
    PersistEstablished {
        cursor: ChangeCursor,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Persist a standalone backfill checkpoint while preserving the writer's
    /// authoritative in-memory debt ledger.
    PersistBackfill {
        checkpoint: bifrost_types::BackfillCheckpoint,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Drop a scope's durable BACKFILL progress, leaving its change cursor and
    /// the debt ledger untouched.
    ///
    /// Sent when a walk published pages that reached nobody. The rows that
    /// survive such a walk are the windows a REPLACEMENT consumer received and
    /// acknowledged, and those sit past the hole - so a positional resume
    /// (`BackfillPlan::OpenPages`) would start the retry beyond the pages that
    /// were lost, take its empty end probe, and settle the scope having never
    /// re-offered them. The in-memory "restart this incarnation from scratch"
    /// note cannot answer that on its own: it dies with the attachment, and a
    /// detach before the retry leaves the misleading rows behind.
    ///
    /// Deleting them is safe in the direction that matters. A backfill row is a
    /// resume HINT, never the objects themselves; losing one costs a re-walk,
    /// and re-emitting acknowledged pages is idempotent. There is no completion
    /// marker to lose here either: it was withheld by the same condition that
    /// sends this.
    DiscardBackfillProgress {
        scope: CursorScope,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Invalidate durable state for a scope as one writer-ordered operation.
    /// Pending publications for the affected lanes are retired before the
    /// rows are deleted, so a late acknowledgement cannot revive them.
    ResetScope {
        scope: CursorScope,
        delete_backfill: bool,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Record a walk that stopped at a region the cursor may not cross.
    ///
    /// Has no checkpoint of its own by definition - nothing advanced - so it
    /// cannot ride the acknowledgement path like every other durable mutation.
    /// It still goes through this writer, because it shares the ledger with
    /// everything that does.
    RecordBarrier {
        incident: crate::cursor::BarrierIncident,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Re-check every barrier in a report against the writer's current ledger
    /// and convert an all-waived set into accepted-loss debt before crossing.
    CrossWaivedBarriers {
        report: bifrost_types::InventoryCoverageReport,
        generation: u64,
        done: oneshot::Sender<Result<bool, Error>>,
    },
    /// Read whether operator policy currently parks this scope at a barrier.
    ScopeBarrierBlocked {
        scope: CursorScope,
        done: oneshot::Sender<bool>,
    },
    /// Record obligations from a report that has no acknowledgeable checkpoint
    /// of its own - a backfill partition's terminal summary, say.
    ///
    /// Debt only, never proof: the report may describe entries the consumer
    /// never persisted, so it may not discharge anything. Over-reporting debt
    /// costs a scope that stays degraded until something re-reads it;
    /// under-reporting it costs objects nobody ever sees again.
    RecordDebt {
        report: bifrost_types::InventoryCoverageReport,
        generation: u64,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Apply the outcomes of one repair pass.
    ///
    /// `publication` is the repair batch's acknowledgement identity, present
    /// only when ids were actually published to a live subscriber. A
    /// `Recovered` resolution discharges only once that publication is
    /// acknowledged: the account having re-read the object proves the
    /// representation healed, but until the consumer durably accepts the
    /// existence notification it still does not know the object is there.
    ApplyRepair {
        resolutions: Vec<crate::repair::RepairResolution>,
        publication: Option<crate::cursor::PublicationId>,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Confirm that the consumer durably accepted a publication with no
    /// checkpoint, such as a repair notification batch.
    AcknowledgePublication {
        publication: crate::cursor::PublicationId,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Operator action on one obligation or barrier occurrence.
    ///
    /// `Waive` is the only path to accepted loss, and it exists only here:
    /// nothing automatic may reach it, because a retry budget expiring is
    /// evidence that retrying is not working, not a decision about what loss is
    /// acceptable.
    OperatorDecision {
        key: bifrost_types::ObligationKey,
        decision: OperatorDecision,
        done: oneshot::Sender<Result<bool, Error>>,
    },
}

/// What an operator decided about an obligation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum OperatorDecision {
    /// Accept the loss. Changes POLICY only: the record goes on saying that
    /// coverage was never proved, because it never was.
    Waive { by: String, at_unix_seconds: i64 },
    /// Stop automatic retries without accepting the loss. Stays visible, stays
    /// blocking, stays manually retryable.
    Block,
}

impl From<AckRequest> for WriterRequest {
    fn from(request: AckRequest) -> Self {
        Self::Ack(request)
    }
}

impl std::fmt::Debug for WriterRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ack(request) => f.debug_tuple("Ack").field(request).finish(),
            Self::ReattachInsert {
                attempt, cursor, ..
            } => f
                .debug_struct("ReattachInsert")
                .field("attempt", attempt)
                .field("scope", &cursor.scope)
                .finish(),
            Self::ReattachAbort { attempt, .. } => f
                .debug_struct("ReattachAbort")
                .field("attempt", attempt)
                .finish(),
            Self::ReattachCommit { attempt } => f
                .debug_struct("ReattachCommit")
                .field("attempt", attempt)
                .finish(),
            Self::GetChangeCursor {
                scope, claim_for, ..
            } => f
                .debug_struct("GetChangeCursor")
                .field("scope", scope)
                .field("claim_for", claim_for)
                .finish(),
            Self::PersistEstablished { cursor, .. } => f
                .debug_struct("PersistEstablished")
                .field("scope", &cursor.scope)
                .finish(),
            Self::PersistBackfill { checkpoint, .. } => f
                .debug_struct("PersistBackfill")
                .field("scope", &checkpoint.scope)
                .finish(),
            Self::DiscardBackfillProgress { scope, .. } => f
                .debug_struct("DiscardBackfillProgress")
                .field("scope", scope)
                .finish(),
            Self::ResetScope {
                scope,
                delete_backfill,
                ..
            } => f
                .debug_struct("ResetScope")
                .field("scope", scope)
                .field("delete_backfill", delete_backfill)
                .finish(),
            Self::ApplyRepair { resolutions, .. } => f
                .debug_struct("ApplyRepair")
                .field("resolutions", &resolutions.len())
                .finish(),
            Self::AcknowledgePublication { publication, .. } => f
                .debug_struct("AcknowledgePublication")
                .field("publication", publication)
                .finish(),
            Self::RecordDebt { generation, .. } => f
                .debug_struct("RecordDebt")
                .field("generation", generation)
                .finish(),
            Self::RecordBarrier { incident, .. } => f
                .debug_struct("RecordBarrier")
                .field("key", &incident.key)
                .finish(),
            Self::CrossWaivedBarriers {
                report, generation, ..
            } => f
                .debug_struct("CrossWaivedBarriers")
                .field("domain", &report.domain)
                .field("generation", generation)
                .finish(),
            Self::ScopeBarrierBlocked { scope, .. } => f
                .debug_struct("ScopeBarrierBlocked")
                .field("scope", scope)
                .finish(),
            Self::OperatorDecision { key, decision, .. } => f
                .debug_struct("OperatorDecision")
                .field("key", key)
                .field("decision", decision)
                .finish(),
        }
    }
}

fn checkpoint_for(event: &SyncEvent<Change>) -> Option<&Checkpoint> {
    match event {
        SyncEvent::Batch(b) => b.checkpoint.as_ref(),
        SyncEvent::Done(c) => c.as_ref(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{ChangesEvent, post_publish_boundary, refused_activity_outcome};
    use crate::cancel::BoundaryRequest;

    // Exercise the real drive with an Account double. The hook cuts over
    // after generation capture but before the malformed event is consumed.
    #[tokio::test]
    async fn contract_failures_publish_only_for_the_current_drive() {
        use crate::test_common::{StubAccount, cursor_for};
        use bifrost_types::{
            BackfillCheckpoint, BackfillProgress, Batch, Checkpoint, CursorScope, PageBoundary,
            Partition, SyncEvent,
        };
        use std::sync::Arc;

        for violation in 0..3 {
            // Current, replaced account, and deleted scope fences.
            for cutover in 0..3 {
                let scope = CursorScope::Account;
                let cursors = Arc::new(crate::cursor::CursorRegistry::new());
                let initial = cursor_for(&scope, b"initial");
                cursors.put(initial.clone());
                let generation = cursors.drive_generation(&scope);
                let changed = Arc::clone(&cursors);
                let mut account = StubAccount::new(vec![scope.clone()]);
                account.changes_hook = Some(Arc::new(move |cursor| {
                    match cutover {
                        1 => {
                            let replacement = crate::cursor::CursorRegistry::new();
                            replacement.put(cursor.clone());
                            changed.replace_topology_preserving_cursors(&replacement);
                        }
                        2 => changed.delete(&cursor.scope),
                        _ => {}
                    }
                    let mut invalid = cursor.clone();
                    invalid.envelope_version = u32::MAX;
                    let checkpoint = if violation == 1 {
                        Checkpoint::Backfill(BackfillCheckpoint {
                            scope: cursor.scope.clone(),
                            partition: Partition(vec![]),
                            progress_marker: None,
                            progress: BackfillProgress {
                                items_done: 0,
                                items_estimated: None,
                            },
                            envelope_version: 1,
                        })
                    } else if violation == 2 {
                        Checkpoint::Change(invalid)
                    } else {
                        Checkpoint::Change(cursor.clone())
                    };
                    vec![SyncEvent::Batch(Batch {
                        items: vec![],
                        page_boundary: if violation == 0 {
                            PageBoundary::Partial
                        } else {
                            PageBoundary::Final
                        },
                        server_latency: std::time::Duration::ZERO,
                        bytes_in: 0,
                        checkpoint: Some(checkpoint),
                    })]
                }));
                let (tx, mut rx) = tokio::sync::broadcast::channel(4);
                let delivery = Arc::new(crate::multiplexer::ChangeDelivery::new(tx));
                let (_boundary, view) = crate::cancel::Boundary::new();
                let result = super::drive_changes_stream(
                    &account,
                    scope,
                    initial,
                    cursors,
                    delivery,
                    view,
                    None,
                    Some(generation),
                )
                .await;
                if cutover == 0 {
                    assert!(matches!(
                        rx.try_recv().expect("current termination").event.as_ref(),
                        SyncEvent::Terminated(_)
                    ));
                    assert!(matches!(
                        result,
                        Ok(ChangesEvent::Terminated(_)) | Err(crate::error::Error::Account(_))
                    ));
                } else {
                    assert!(matches!(result, Ok(ChangesEvent::Done)));
                    assert!(
                        rx.try_recv().is_err(),
                        "obsolete diagnostic escaped its fence"
                    );
                }
            }
        }
    }

    #[test]
    fn published_termination_wins_over_every_boundary() {
        let error = crate::recovery::restart_scope_error(
            bifrost_types::CursorScope::Account,
            bifrost_types::AccountOperation::SyncChanges,
        );
        for request in [
            BoundaryRequest::Run,
            BoundaryRequest::Pause,
            BoundaryRequest::Stop,
            BoundaryRequest::CheckpointNow,
        ] {
            assert!(
                matches!(super::published_outcome(Some(error.clone()), request, false),
                Some(ChangesEvent::Terminated(routed)) if routed.kind() == error.kind())
            );
        }
    }

    /// A drive refused admission under a `Stop` boundary must report
    /// `Stopped`, not `Paused`: `Paused` keeps the poll task alive and
    /// re-entering, so a stopping account never sheds its poll loops.
    #[test]
    fn a_drive_refused_under_stop_reports_stopped_not_paused() {
        assert!(matches!(
            refused_activity_outcome(BoundaryRequest::Stop),
            ChangesEvent::Stopped
        ));
        for parked in [
            BoundaryRequest::Pause,
            BoundaryRequest::CheckpointNow,
            BoundaryRequest::Run,
        ] {
            assert!(
                matches!(refused_activity_outcome(parked), ChangesEvent::Paused),
                "{parked:?} is a park, not an exit"
            );
        }
    }

    /// The full boundary truth table for a published item. Pause and
    /// Stop are lifecycle boundaries and must be honoured whether or
    /// not the item carried a checkpoint - a checkpoint-free stream
    /// that only checked at checkpoint boundaries never parks, and the
    /// account never reaches quiescence. `CheckpointNow` is the one
    /// request that genuinely needs a durable cursor to satisfy, so it
    /// alone keeps running when the item produced none.
    #[test]
    fn lifecycle_boundaries_are_honoured_with_or_without_a_checkpoint() {
        for has_checkpoint in [true, false] {
            assert!(
                matches!(
                    post_publish_boundary(BoundaryRequest::Pause, has_checkpoint),
                    Some(ChangesEvent::Paused)
                ),
                "pause must park (has_checkpoint={has_checkpoint})"
            );
            assert!(
                matches!(
                    post_publish_boundary(BoundaryRequest::Stop, has_checkpoint),
                    Some(ChangesEvent::Stopped)
                ),
                "stop must stop (has_checkpoint={has_checkpoint})"
            );
            assert!(
                post_publish_boundary(BoundaryRequest::Run, has_checkpoint).is_none(),
                "run must keep going (has_checkpoint={has_checkpoint})"
            );
        }
        assert!(matches!(
            post_publish_boundary(BoundaryRequest::CheckpointNow, true),
            Some(ChangesEvent::Done)
        ));
        assert!(
            post_publish_boundary(BoundaryRequest::CheckpointNow, false).is_none(),
            "a checkpoint request is not satisfied by a checkpoint-free item"
        );
    }
}
