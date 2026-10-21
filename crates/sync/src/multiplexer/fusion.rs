//! Inventory-fusion path.
//!
//! For `EstablishViaInventory` scopes (Graph all scopes,
//! IMAP-Basic / IMAP-CONDSTORE-only folders) the inventory walk IS the
//! cursor establishment. The multiplexer consumes the inventory stream
//! and registers the cursor exactly once, on the inventory's terminal
//! `Done` event. Mid-stream `Batch` checkpoints (if any) advance the
//! inventory progress marker only; the change cursor is established in
//! memory when the inventory's terminal `Done` arrives. Durable cursor
//! persistence still waits for the consumer ack of the terminal
//! checkpoint.
//!
//! Inventory data: every `Batch` is forwarded to the per-account
//! broadcast as an `ObjectChange::Created` signal before the cursor
//! is persisted, so cold-start inventory (Graph all scopes,
//! IMAP-Basic / CONDSTORE-only folders) does not vanish.

use std::sync::Arc;

use bifrost_types::{
    Account, AccountError, Change, Checkpoint, CursorScope, ObjectChange, ObjectChangeKind,
    SyncEvent,
};
use futures::stream::StreamExt;

use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::Error;
use crate::inventory_walk::{InventoryWalk, WalkDecision};

use super::{ChangeDelivery, MultiplexerEvent};

/// Outcome of an inventory fusion pass.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FusionOutcome {
    /// Inventory completed and the in-memory cursor was established.
    Established,
    /// Inventory completed without yielding a cursor; the engine emits
    /// a Warning.
    NoCursor,
    /// Inventory terminated with an account error. Carries the full
    /// `AccountError` so the caller dispatches through
    /// `error.recovery()`.
    Terminated(AccountError),
}

pub struct InventoryFusion {
    pub account_id: bifrost_types::AccountId,
    pub cursors: Arc<CursorRegistry>,
    pub control: Option<SyncControl>,
    /// Where this walk registers what it proved, so the durable writer can put
    /// coverage and cursor into one atomic record when the consumer
    /// acknowledges. `None` in contexts with no writer behind them.
    pub coverage: Option<Arc<crate::cursor::PendingCoverage>>,
    /// Durable writer, for barrier incidents. A barrier advances no cursor, so
    /// it has no checkpoint to ride and cannot reach the store through the
    /// acknowledgement path like everything else does.
    pub writer_tx: Option<tokio::sync::mpsc::Sender<super::WriterRequest>>,
    /// Engine-issued generation for this walk. Orders proof events even when
    /// two walks produce identical cursor bytes.
    pub generation: u64,
    /// Cancelled when the walk's owner is tearing down. The walk then stops at
    /// its next provider poll and returns [`Error::ShuttingDown`], dropping the
    /// provider stream, instead of waiting on a stream that neither yields nor
    /// ends until the owner's join deadline aborts it. `None` in contexts that
    /// have no teardown to answer.
    pub shutdown: Option<tokio_util::sync::CancellationToken>,
}

impl InventoryFusion {
    /// Drive an `inventory_stream(scope)` to completion. On the
    /// terminal `Done`, register the cursor carried in
    /// its checkpoint. Mid-stream batches are ignored for the purpose
    /// of cursor establishment, but see `run_with_broadcast` for the
    /// path that forwards inventory data to subscribers.
    pub async fn run(
        &self,
        account: &dyn Account,
        scope: CursorScope,
    ) -> Result<FusionOutcome, Error> {
        self.run_with_broadcast(account, scope, None).await
    }

    /// Same contract as `run`, but each `Batch` of inventory entries
    /// is fanned out on the account's change delivery as
    /// `Change::ObjectChange::Created` events BEFORE the terminal cursor is
    /// acknowledged. This is the only path inventory data flows to consumers
    /// for `EstablishViaInventory` scopes - dropping it would lose every
    /// Graph object and every IMAP-Basic / CONDSTORE-only folder's
    /// cold-start contents.
    ///
    /// The delivery gate rather than the raw sender, because every
    /// checkpoint-bearing page this walk publishes registers a claim that only
    /// a NUMBERED receiver can answer for, and only the gate knows whether one
    /// took delivery.
    pub async fn run_with_broadcast(
        &self,
        account: &dyn Account,
        scope: CursorScope,
        delivery: Option<Arc<ChangeDelivery>>,
    ) -> Result<FusionOutcome, Error> {
        let stream = account.inventory_stream(scope.clone());
        self.run_stream(scope, stream, delivery).await
    }

    /// Resume a provider-owned inventory page cursor. The account returns
    /// `None` for a cursor it does not own, so a stale or cross-provider row
    /// cannot be accidentally reinterpreted as a fresh inventory walk.
    pub async fn run_resume_with_broadcast(
        &self,
        account: &dyn Account,
        cursor: bifrost_types::ChangeCursor,
        delivery: Option<Arc<ChangeDelivery>>,
    ) -> Result<FusionOutcome, Error> {
        let scope = cursor.scope.clone();
        let classified_as_inventory = account.is_inventory_cursor(&cursor);
        let stream = account.inventory_resume_stream(cursor);
        self.run_resume_stream_with_broadcast(scope, classified_as_inventory, stream, delivery)
            .await
    }

    /// Resume a cursor after its classifier and stream have already been
    /// queried by an establishment caller. Keeping the check here ensures an
    /// invocation routes either disagreement through scoped recovery.
    /// Callers that gate on the classifier alone can still bypass this guard
    /// when the stream hook returns `Some` for an unclassified cursor.
    pub(crate) async fn run_resume_stream_with_broadcast(
        &self,
        scope: CursorScope,
        classified_as_inventory: bool,
        stream: Option<bifrost_types::AccountStream<bifrost_types::InventoryEvent>>,
        delivery: Option<Arc<ChangeDelivery>>,
    ) -> Result<FusionOutcome, Error> {
        let stream =
            match inventory_resume_stream_checked(scope.clone(), classified_as_inventory, stream) {
                Ok(stream) => stream,
                Err(error) => {
                    // Route a bad resume cursor through the ordinary scoped
                    // recovery path. Returning `Err` here made the deferred attach
                    // worker log and exit with no registered cursor to poll again.
                    return Ok(forward_resume_contract_failure(
                        &scope,
                        error,
                        delivery.as_ref(),
                    ));
                }
            };
        let Some(stream) = stream else {
            return Ok(FusionOutcome::NoCursor);
        };
        self.run_stream(scope, stream, delivery).await
    }

    async fn run_stream(
        &self,
        scope: CursorScope,
        mut stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent>,
        changes_tx: Option<Arc<ChangeDelivery>>,
    ) -> Result<FusionOutcome, Error> {
        let _activity = match &self.control {
            Some(control) => Some(control.begin_activity().ok_or(Error::Paused)?),
            None => None,
        };
        // The last checkpoint this walk published. If a later page turns out to
        // be a barrier, this is the position a future walk resumes from - it
        // certifies a prefix ending before the barrier region begins.
        let mut walk = InventoryWalk::default();
        loop {
            // The shutdown arm wraps the provider poll and nothing else. Every
            // other await in this loop - barrier recording, waived-barrier
            // crossing - runs inside an arm, and a select cannot destroy a
            // branch mid-poll, so a page's claim registration and its publish,
            // which are synchronous, cannot be split. What the cut drops is the
            // provider stream and the activity guard, which is the point.
            //
            // An ERROR rather than `NoCursor`, for the same reason the backfill
            // runner returns one: `NoCursor` is a completed walk that yielded no
            // cursor, and callers treat it as a finished establishment.
            let next = match &self.shutdown {
                Some(shutdown) => {
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => return Err(Error::ShuttingDown),
                        next = stream.next() => next,
                    }
                }
                None => stream.next().await,
            };
            let Some(event) = next else { break };
            match event {
                bifrost_types::InventoryEvent::Done(completion) => {
                    // Same contract check as every page: a withheld state that
                    // contradicts the coverage beside it is an account breaking
                    // its stream contract, terminated as a classified violation.
                    if let Err(violation) = completion.validate() {
                        let error = crate::recovery::batch_boundary_violation(
                            violation,
                            completion.checkpoint.offered(),
                            bifrost_types::AccountOperation::SyncInventory,
                            &scope,
                        );
                        if let Some(tx) = &changes_tx {
                            let _ = tx.sender().send(MultiplexerEvent {
                                scope: scope.clone(),
                                event: Arc::new(SyncEvent::Terminated(error.clone())),
                                checkpoint: None,
                                publication: None,
                            });
                        }
                        return Ok(FusionOutcome::Terminated(error));
                    }
                    let crossed = if completion.coverage.has_barrier() {
                        crate::inventory_walk::cross_waived_barriers(
                            self.writer_tx.as_ref(),
                            &completion.coverage,
                            self.generation,
                        )
                        .await?
                    } else {
                        false
                    };
                    if !crossed
                        && let WalkDecision::StopAtBarrier { resume_from } =
                            walk.inspect(&completion.coverage)
                    {
                        // The walk ended on ground the cursor may not cross.
                        // Establishing here would advance past a region nothing
                        // can ever replay. Same rule as the backfill runner: a
                        // barrier the store refused is announced to nobody -
                        // the walk fails instead, so establishment is retried
                        // rather than warning about an incident a restart
                        // forgets and no operator can act on.
                        self.record_barriers(&scope, &completion.coverage, resume_from)
                            .await?;
                        Self::warn_degraded(&changes_tx, &scope, &completion.coverage);
                        return Ok(FusionOutcome::NoCursor);
                    }
                    // Past the stop, a barrier can only stand here if the
                    // operator waived it, which is what `crossed` says.
                    let checkpoint = completion.checkpoint.accepted(crossed).cloned();
                    validate_checkpoint_envelope(checkpoint.as_ref())?;
                    let degraded = !completion.coverage.is_complete();
                    if degraded {
                        Self::warn_degraded(&changes_tx, &scope, &completion.coverage);
                    }
                    if let (Some(tx), Some(cp)) = (&changes_tx, checkpoint.clone()) {
                        // Register the claim BEFORE publishing: a fast consumer
                        // can acknowledge between the send and a later
                        // registration, and the writer would then find no claim
                        // for a checkpoint that carried one.
                        let publication = self.control.as_ref().map(|control| {
                            control.publish_checkpoint(
                                cp.clone(),
                                crate::cursor::CoverageClaim::new(
                                    completion.coverage.clone(),
                                    self.generation,
                                ),
                            )
                        });
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Done(Some(cp.clone()))),
                            checkpoint: Some(cp.clone()),
                            publication: publication.clone(),
                        };
                        // Register before publishing so a fast consumer
                        // ack cannot land before the entry exists and
                        // leave it outstanding forever.
                        if !tx.publish_acknowledgeable(me) {
                            // Nothing can ever acknowledge it, so the claim
                            // would sit in the registry for the life of the
                            // attachment.
                            self.retire_claim(publication);
                        }
                    }
                    return self.finalize(scope, checkpoint).await;
                }
                bifrost_types::InventoryEvent::Terminated(err) => {
                    if let Some(tx) = &changes_tx {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Terminated(err.clone())),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = tx.sender().send(me);
                    }
                    return Ok(FusionOutcome::Terminated(err));
                }
                bifrost_types::InventoryEvent::Batch(batch) => {
                    // Same reasoning as the changes driver: a bare engine
                    // error would leave the establish path retrying an
                    // unchanged position with nothing on the stream to tell
                    // a consumer the walk stopped. Terminate the walk with a
                    // classified provider-contract violation instead.
                    if let Err(violation) = batch.validate_boundary() {
                        let error = crate::recovery::batch_boundary_violation(
                            violation,
                            batch.checkpoint.offered(),
                            bifrost_types::AccountOperation::SyncInventory,
                            &scope,
                        );
                        if let Some(tx) = &changes_tx {
                            let _ = tx.sender().send(MultiplexerEvent {
                                scope: scope.clone(),
                                event: Arc::new(SyncEvent::Terminated(error.clone())),
                                checkpoint: None,
                                publication: None,
                            });
                        }
                        return Ok(FusionOutcome::Terminated(error));
                    }
                    // A barrier TAINTS THE WALK. Refusing only this one
                    // checkpoint is not enough: the next page's checkpoint, or
                    // the terminal delta link, would simply leap over the same
                    // region. So the batch's items are still delivered - they
                    // were really seen - but its withheld checkpoint is not
                    // taken and the walk stops here. Checkpoints already
                    // accepted earlier in this walk stand: each of them
                    // certifies a prefix that ends before this region begins.
                    let crossed = if batch.coverage.has_barrier() {
                        crate::inventory_walk::cross_waived_barriers(
                            self.writer_tx.as_ref(),
                            &batch.coverage,
                            self.generation,
                        )
                        .await?
                    } else {
                        false
                    };
                    if !crossed
                        && let WalkDecision::StopAtBarrier { resume_from } =
                            walk.inspect(&batch.coverage)
                    {
                        self.record_barriers(&scope, &batch.coverage, resume_from)
                            .await?;
                        if let Some(tx) = &changes_tx {
                            self.forward_inventory_batch(tx, &scope, &batch, None);
                            Self::warn_degraded(&changes_tx, &scope, &batch.coverage);
                        }
                        return Ok(FusionOutcome::NoCursor);
                    }
                    let checkpoint = batch.checkpoint.accepted(crossed).cloned();
                    validate_checkpoint_envelope(checkpoint.as_ref())?;
                    walk.accept(checkpoint.clone());
                    if let Some(tx) = &changes_tx {
                        self.forward_inventory_batch(tx, &scope, &batch, checkpoint);
                    }
                }
                // A producer-emitted warning is forwarded, not absorbed. Graph
                // announces its public-folder over-cap degrade this way (past
                // the cap the folder silently becomes additions-only, so
                // deletions stop propagating), and IMAP announces a QRESYNC ->
                // CONDSTORE strategy downgrade. Dropping them here was worse
                // than losing a message in the IMAP case: that warning sits
                // behind a one-shot `AtomicBool` shared with the changes path,
                // so whichever lane reached it first consumed it, and the
                // other could never re-emit.
                bifrost_types::InventoryEvent::Warning(warning) => {
                    if let Some(tx) = &changes_tx {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Warning(warning)),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = tx.sender().send(me);
                    }
                }
                bifrost_types::InventoryEvent::Progress(_) => {}
                _ => {}
            }
        }
        // The stream ended with neither `Done` nor `Terminated`: a producer
        // breaking its contract. No cursor is established, and the reason is
        // surfaced, the same way the backfill runner surfaces it.
        if let Some(tx) = &changes_tx {
            let _ = tx.sender().send(MultiplexerEvent {
                scope: scope.clone(),
                event: Arc::new(SyncEvent::Warning(
                    crate::inventory_walk::unannounced_end_warning(),
                )),
                checkpoint: None,
                publication: None,
            });
        }
        Ok(FusionOutcome::NoCursor)
    }

    /// Release both halves of a publication nothing can ever acknowledge.
    ///
    /// Through `control` when there is one, because that also releases the
    /// boundary registration - the two halves are one publication and must not
    /// be retired separately.
    fn retire_claim(&self, publication: Option<crate::cursor::PublicationId>) {
        let Some(id) = publication else {
            return;
        };
        if let Some(control) = &self.control {
            control.retire_publication(id);
        } else if let Some(pending) = &self.coverage {
            pending.retire(id);
        }
    }

    /// Persist the blocked-progress incidents in `coverage`.
    ///
    /// A barrier is not debt behind an advanced cursor - no checkpoint was
    /// accepted past it - so it cannot ride the acknowledgement path. Writing
    /// nothing at all would be worse than it sounds: every restart would forget
    /// the scope keeps hitting the same wall, and an operator would have no
    /// durable object to waive.
    ///
    /// An `Err` is a store write that FAILED, and it must propagate: a walk
    /// that goes on to warn and return `NoCursor` over it announces a barrier
    /// nothing durable records, which a restart forgets and no operator can
    /// block or waive. The caller fails the walk instead, so establishment is
    /// retried under its ordinary error contract. A departed writer (detach,
    /// shutdown) is not a failure - there is nothing left to persist to.
    async fn record_barriers(
        &self,
        scope: &CursorScope,
        coverage: &bifrost_types::InventoryCoverageReport,
        resume_from: Option<Checkpoint>,
    ) -> Result<(), Error> {
        let result = crate::inventory_walk::record_barriers(
            self.writer_tx.as_ref(),
            coverage,
            self.generation,
            resume_from,
        )
        .await;
        if let Err(error) = &result {
            tracing::error!(
                target: "bifrost.sync.inventory",
                account = ?self.account_id,
                scope = ?scope,
                error = %error,
                "failed to persist inventory barrier incident; failing the walk"
            );
        }
        result
    }

    /// Surface degraded coverage to the consumer.
    ///
    /// Durable debt that nothing reports is only half a fix: the scope is live
    /// and converging, but objects are known-missing and only an operator can
    /// decide whether to repair, wait, or accept the gap.
    fn warn_degraded(
        changes_tx: &Option<Arc<ChangeDelivery>>,
        scope: &CursorScope,
        coverage: &bifrost_types::InventoryCoverageReport,
    ) {
        if coverage.is_complete() {
            return;
        }
        let obligations = coverage.obligations();
        let Some(tx) = changes_tx else { return };
        let barriers = obligations
            .iter()
            .filter(|obligation| obligation.is_barrier())
            .count();
        let warning = bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::OperatorAttentionNeeded,
            if barriers > 0 {
                format!(
                    "inventory stopped at {barriers} region(s) it cannot represent and cannot \
                     replay; the scope cannot advance past them until an operator waives them"
                )
            } else {
                format!(
                    "inventory completed with {} object(s) or region(s) it could not represent; \
                     the scope is live but incomplete",
                    obligations.len()
                )
            },
        );
        let _ = tx.sender().send(MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(SyncEvent::Warning(warning)),
            checkpoint: None,
            publication: None,
        });
    }

    /// Forward a page's entries, publishing `checkpoint` with them.
    ///
    /// `checkpoint` is the position the caller ACCEPTED, passed separately from
    /// the batch so nothing here can read the batch's own `PageCheckpoint` and
    /// take a withheld position.
    fn forward_inventory_batch(
        &self,
        tx: &ChangeDelivery,
        scope: &CursorScope,
        batch: &bifrost_types::InventoryBatch,
        checkpoint: Option<Checkpoint>,
    ) {
        // Inventory entries describe object existence; surface them as
        // `Created` changes so consumers handle inventory the same way
        // they handle a freshly-discovered object on the live stream.
        let changes: Vec<Change> = batch
            .items
            .iter()
            .map(|entry| {
                Change::ObjectChange(ObjectChange {
                    id: entry.id.clone(),
                    kind: ObjectChangeKind::Created,
                })
            })
            .chain(crate::inventory_walk::removals(scope, &batch.removed))
            .collect();
        let synthetic = bifrost_types::Batch {
            items: changes,
            page_boundary: batch.page_boundary,
            server_latency: batch.server_latency,
            bytes_in: batch.bytes_in,
            checkpoint: checkpoint.clone(),
        };
        // A checkpoint-bearing batch declares the coverage it advances across,
        // not just the terminal completion: Graph checkpoints per page, so
        // waiting for `Done` would let a page checkpoint become durable across
        // a gap it never declared.
        let publication = match (&self.control, &checkpoint) {
            (Some(control), Some(checkpoint)) => Some(control.publish_checkpoint(
                checkpoint.clone(),
                crate::cursor::CoverageClaim::new(batch.coverage.clone(), self.generation),
            )),
            _ => None,
        };
        let me = MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(SyncEvent::Batch(synthetic)),
            checkpoint,
            publication: publication.clone(),
        };
        if !tx.publish_acknowledgeable(me) {
            self.retire_claim(publication);
        }
    }

    async fn finalize(
        &self,
        expected_scope: CursorScope,
        checkpoint: Option<Checkpoint>,
    ) -> Result<FusionOutcome, Error> {
        let Some(Checkpoint::Change(cursor)) = checkpoint else {
            return Ok(FusionOutcome::NoCursor);
        };
        if cursor.scope != expected_scope {
            return Err(Error::Other(format!(
                "inventory Done returned cursor for the wrong scope: expected {:?}, got {:?}",
                expected_scope, cursor.scope,
            )));
        }
        self.cursors.put(cursor);
        Ok(FusionOutcome::Established)
    }
}

/// What a fusion batch's checkpoint may be, and what it may not.
///
/// A `Change` cursor is the point of the fusion path and is validated. A
/// `Backfill` checkpoint is REFUSED, and that refusal is the whole reason this
/// arm exists: fusion registers the publication and then sends on the RAW sender,
/// so the entry is never stamped with a delivery. A `Lane::Backfill` entry whose
/// `delivered_at` stays `None` for ever is charged against the account's backfill
/// bound and can be freed only by an acknowledgement, a lag or a reset - the
/// receiver-drop sweep will not touch it, because an unsent page is deliberately
/// never swept. A provider that attached one to an inventory batch would quietly
/// spend a permanent slot of the bound per scope.
///
/// No provider in this workspace emits one, so refusing costs nothing real, and
/// refusing is the safe direction: the inventory pass fails and is retried,
/// rather than the account's cold start narrowing by one page for the life of
/// the attachment.
fn validate_checkpoint_envelope(checkpoint: Option<&Checkpoint>) -> Result<(), Error> {
    match checkpoint {
        Some(Checkpoint::Change(cursor)) => cursor.validate_envelope().map_err(|_| {
            Error::Account(crate::recovery::fresh_cursor_contract_error(
                checkpoint,
                bifrost_types::AccountOperation::SyncInventory,
                &cursor.scope,
            ))
        }),
        Some(Checkpoint::Backfill(_)) => Err(Error::Other(
            "inventory fusion batch carried a backfill checkpoint; the fusion path publishes \
             without a delivery stamp, so such a publication is charged against the backfill \
             bound and never swept"
                .into(),
        )),
        _ => Ok(()),
    }
}

fn inventory_cursor_contract_error(scope: CursorScope) -> bifrost_types::AccountError {
    crate::recovery::inventory_resume_contract_failure(scope)
}

fn forward_resume_contract_failure(
    scope: &CursorScope,
    error: bifrost_types::AccountError,
    delivery: Option<&Arc<ChangeDelivery>>,
) -> FusionOutcome {
    if let Some(tx) = delivery {
        let _ = tx.sender().send(MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(SyncEvent::Terminated(error.clone())),
            checkpoint: None,
            publication: None,
        });
    }
    FusionOutcome::Terminated(error)
}

fn inventory_resume_stream_checked(
    scope: CursorScope,
    classified_as_inventory: bool,
    stream: Option<bifrost_types::AccountStream<bifrost_types::InventoryEvent>>,
) -> Result<
    Option<bifrost_types::AccountStream<bifrost_types::InventoryEvent>>,
    bifrost_types::AccountError,
> {
    if classified_as_inventory != stream.is_some() {
        Err(inventory_cursor_contract_error(scope))
    } else {
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_inventory_stream() -> bifrost_types::AccountStream<bifrost_types::InventoryEvent> {
        Box::pin(futures::stream::empty())
    }

    /// The fusion path takes a `Change` cursor and refuses a `Backfill`
    /// checkpoint.
    ///
    #[test]
    fn fresh_invalid_fusion_checkpoint_is_a_scoped_terminal_contract_failure() {
        let scope = CursorScope::Account;
        let mut cursor = crate::test_common::cursor_for(&scope, b"fresh");
        cursor.envelope_version = u32::MAX;
        let checkpoint = Checkpoint::Change(cursor);
        let Err(Error::Account(error)) = validate_checkpoint_envelope(Some(&checkpoint)) else {
            panic!("expected classified contract error");
        };
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Cursor(scope))
        );
        assert!(matches!(
            crate::recovery::plan_recovery(error),
            crate::recovery::RecoveryPlan::Terminal(_)
        ));
    }

    /// Latent rather than live - no provider in this workspace attaches one - and
    /// refused for a structural reason: fusion registers the publication and then
    /// publishes it unstamped, so a `Lane::Backfill` entry created here is never
    /// stamped with a delivery. An unsent stamp is deliberately never swept, so
    /// that entry is charged against the account's backfill bound until an
    /// acknowledgement, a lag or a reset frees it - a permanent slot of the bound
    /// spent per scope, narrowing every later cold start.
    #[test]
    fn a_fusion_batch_may_not_carry_a_backfill_checkpoint() {
        let cursor = bifrost_types::ChangeCursor {
            scope: CursorScope::Account,
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Imap,
                envelope_version: 1,
                bytes: vec![1],
            },
            advanced_through: None,
            envelope_version: 1,
        };
        assert!(
            validate_checkpoint_envelope(Some(&Checkpoint::Change(cursor))).is_ok(),
            "a change cursor is what this path exists to carry"
        );
        assert!(
            validate_checkpoint_envelope(None).is_ok(),
            "and a batch with no checkpoint is ordinary"
        );

        let backfill = Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
            scope: CursorScope::Account,
            partition: bifrost_types::Partition(b"page:0:10".to_vec()),
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: 1,
        });
        assert!(
            validate_checkpoint_envelope(Some(&backfill)).is_err(),
            "a backfill checkpoint on a fusion batch charges the bound with a page \
             nothing will ever sweep"
        );
    }

    #[test]
    fn resume_guard_rejects_both_classifier_hook_disagreements() {
        let scope = CursorScope::Folder(bifrost_types::FolderId("resume".into()));
        let Err(classified_without_stream) =
            inventory_resume_stream_checked(scope.clone(), true, None)
        else {
            panic!("a declared resume cursor needs a stream")
        };
        let Err(stream_without_classification) =
            inventory_resume_stream_checked(scope.clone(), false, Some(empty_inventory_stream()))
        else {
            panic!("an undeclared cursor cannot have a resume stream")
        };
        for error in [classified_without_stream, stream_without_classification] {
            assert_eq!(
                error.scope(),
                Some(&bifrost_types::ErrorScope::Cursor(scope.clone()))
            );
            assert!(matches!(
                error.recovery(),
                bifrost_types::RecoveryClass::Engine(
                    bifrost_types::EngineDirective::RestartScope(found)
                ) if found == &scope
            ));
        }
        assert!(inventory_resume_stream_checked(scope.clone(), false, None).is_ok());
        assert!(
            inventory_resume_stream_checked(scope, true, Some(empty_inventory_stream())).is_ok()
        );
    }

    #[test]
    fn resume_contract_failure_is_forwarded_as_a_scoped_termination() {
        let scope = CursorScope::Folder(bifrost_types::FolderId("resume".into()));
        let error = inventory_cursor_contract_error(scope.clone());
        let (sender, mut receiver) = tokio::sync::broadcast::channel(2);
        let delivery = Arc::new(ChangeDelivery::new(sender));

        let FusionOutcome::Terminated(outcome_error) =
            forward_resume_contract_failure(&scope, error, Some(&delivery))
        else {
            panic!("a resume disagreement must enter scoped recovery")
        };
        assert_eq!(
            outcome_error.scope(),
            Some(&bifrost_types::ErrorScope::Cursor(scope.clone()))
        );
        let event = receiver.try_recv().expect("termination forwarded");
        assert_eq!(event.scope, scope.clone());
        let SyncEvent::Terminated(forwarded) = event.event.as_ref() else {
            panic!("the forwarded event must be terminal")
        };
        assert!(matches!(
            forwarded.recovery(),
            bifrost_types::RecoveryClass::Engine(
                bifrost_types::EngineDirective::RestartScope(found)
            ) if found == &scope
        ));
    }

    fn barrier_coverage() -> bifrost_types::InventoryCoverageReport {
        let error = bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
            bifrost_types::Cause::Request(bifrost_types::RequestCause::Malformed {
                detail: bifrost_types::DiagnosticText::support_only("barrier"),
            }),
        )
        .try_build()
        .expect("valid error");
        bifrost_types::InventoryCoverageReport::degraded(
            bifrost_types::CoverageDomain::full(CursorScope::Account),
            vec![bifrost_types::InventoryObligation::Region {
                key: bifrost_types::ObligationKey(b"blocked".to_vec()),
                failure_label: "unidentifiable-value".into(),
                error,
                recovery: bifrost_types::RegionRecovery::barrier(),
            }],
        )
    }

    fn fusion_with_writer(
        writer_tx: tokio::sync::mpsc::Sender<crate::multiplexer::WriterRequest>,
    ) -> InventoryFusion {
        InventoryFusion {
            account_id: bifrost_types::AccountId("fusion-unit".into()),
            cursors: Arc::new(CursorRegistry::new()),
            control: None,
            coverage: None,
            writer_tx: Some(writer_tx),
            generation: 1,
            shutdown: None,
        }
    }

    /// A walk parked on a provider stream that neither yields nor ends leaves
    /// at shutdown, and drops the stream on the way out.
    ///
    /// Without the arm the poll waits on the stream for ever, and in the engine
    /// only the owner's join deadline gets it back. `NoCursor` would be the
    /// wrong answer: callers read it as a finished walk.
    #[tokio::test(start_paused = true)]
    async fn shutdown_cuts_a_walk_parked_on_its_provider_stream() {
        let (writer_tx, _writer_rx) = tokio::sync::mpsc::channel(4);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let fusion = InventoryFusion {
            shutdown: Some(shutdown.clone()),
            ..fusion_with_writer(writer_tx)
        };
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
        struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }
        let guard = OnDrop(Some(dropped_tx));
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::pending().map(move |event| {
                let _ = &guard;
                event
            }));

        let walk = fusion.run_stream(CursorScope::Account, stream, None);
        tokio::pin!(walk);
        // Poll the walk once, so it is genuinely parked in the provider poll
        // before the cancel lands.
        tokio::select! {
            biased;
            outcome = &mut walk => panic!("a pending stream cannot finish the walk: {outcome:?}"),
            () = tokio::task::yield_now() => {}
        }
        shutdown.cancel();
        // Bounded so a walk that ignores the cancel fails here rather than
        // hanging; under paused time the bound costs nothing when it holds.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(60), walk)
            .await
            .expect("the cancel must end the walk, not leave it parked");
        assert!(
            matches!(outcome, Err(Error::ShuttingDown)),
            "a cancelled walk reports shutdown, not a finished walk: {outcome:?}"
        );
        dropped_rx
            .await
            .expect("the provider stream is dropped on the way out");
    }

    /// A producer's own inventory warning must reach the consumer.
    ///
    /// Both engine inventory front ends used to absorb `InventoryEvent::Warning`
    /// in the same arm as `Progress`, one arm below the `Terminated` case that
    /// forwards. Two live producers announce a DEGRADE that way and nothing
    /// else: Graph's public-folder over-cap switch to additions-only, where
    /// deletions silently stop propagating, and IMAP's QRESYNC -> CONDSTORE
    /// strategy downgrade. The IMAP case is why absorbing it is worse than
    /// losing an ordinary message - the warning sits behind a one-shot
    /// `AtomicBool` shared with the changes path, so whichever lane arrives
    /// first consumes it and the other can never re-emit.
    #[tokio::test]
    async fn an_inventory_warning_reaches_the_change_stream() {
        let (writer_tx, _writer_rx) = tokio::sync::mpsc::channel(4);
        let fusion = fusion_with_writer(writer_tx);
        let warning = bifrost_types::Warning::support_only(
            bifrost_types::WarningKind::StrategyDowngraded,
            "QResync->Condstore",
        );
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::iter(vec![
                bifrost_types::InventoryEvent::Warning(warning),
                bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::complete(
                    bifrost_types::CoverageDomain::full(CursorScope::Account),
                    None,
                    bifrost_types::PartitionEnd::Exhausted,
                )),
            ]));
        let (changes_tx, mut changes_rx) = tokio::sync::broadcast::channel(8);

        fusion
            .run_stream(
                CursorScope::Account,
                stream,
                Some(Arc::new(ChangeDelivery::new(changes_tx))),
            )
            .await
            .expect("a clean walk carrying a warning still completes");

        let announced = changes_rx
            .try_recv()
            .expect("the producer's warning must be forwarded, not absorbed");
        assert!(
            matches!(announced.event.as_ref(), SyncEvent::Warning(w)
                if w.kind == bifrost_types::WarningKind::StrategyDowngraded),
            "the forwarded event must be the producer's own warning, got {:?}",
            announced.event
        );
    }

    /// A stream that ends with neither `Done` nor `Terminated` establishes no
    /// cursor, and says why on the change stream - the same warning the
    /// backfill runner emits for the same producer defect.
    #[tokio::test]
    async fn a_walk_that_ends_silently_warns_and_establishes_nothing() {
        let (writer_tx, _writer_rx) = tokio::sync::mpsc::channel(4);
        let fusion = fusion_with_writer(writer_tx);
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::empty());
        let (changes_tx, mut changes_rx) = tokio::sync::broadcast::channel(8);

        let outcome = fusion
            .run_stream(
                CursorScope::Account,
                stream,
                Some(Arc::new(ChangeDelivery::new(changes_tx))),
            )
            .await
            .expect("a silent end is not an engine error");
        assert!(matches!(outcome, FusionOutcome::NoCursor), "{outcome:?}");
        let announced = changes_rx.try_recv().expect("the silent end is surfaced");
        assert!(
            matches!(announced.event.as_ref(), SyncEvent::Warning(w)
                if w.kind == bifrost_types::WarningKind::InventoryEndedUnannounced),
            "{:?}",
            announced.event
        );
    }

    /// Same rule the backfill runner enforces, on the OTHER front end of the
    /// shared walk: a barrier whose incident the store refused to persist is
    /// announced to nobody. The walk fails so establishment retries; returning
    /// `NoCursor` and warning instead would announce a barrier a restart
    /// forgets and no operator can block or waive.
    #[tokio::test]
    async fn a_store_refused_barrier_fails_the_fusion_walk_instead_of_announcing_it() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let writer = tokio::spawn(async move {
            while let Some(request) = rx.recv().await {
                if let crate::multiplexer::WriterRequest::RecordBarrier { done, .. } = request {
                    let _ = done.send(Err(Error::Other("store write failed".into())));
                }
            }
        });
        let fusion = fusion_with_writer(tx);
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::iter(vec![
                bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::new(
                    None,
                    barrier_coverage(),
                    bifrost_types::PartitionEnd::Exhausted,
                )),
            ]));
        let (changes_tx, mut changes_rx) = tokio::sync::broadcast::channel(8);

        let outcome = fusion
            .run_stream(
                CursorScope::Account,
                stream,
                Some(Arc::new(ChangeDelivery::new(changes_tx))),
            )
            .await;
        assert!(
            outcome.is_err(),
            "a barrier the store refused must fail the walk, not conclude it"
        );
        assert!(
            changes_rx.try_recv().is_err(),
            "nothing may be announced for an incident nothing durable records"
        );
        drop(fusion);
        writer.await.expect("writer task");
    }

    /// The strictness stays honest: a writer that is GONE (detach, shutdown)
    /// is not a failed write, and must not fail the walk.
    #[tokio::test]
    async fn a_departed_writer_does_not_fail_the_fusion_walk() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let fusion = fusion_with_writer(tx);
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::iter(vec![
                bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::new(
                    None,
                    barrier_coverage(),
                    bifrost_types::PartitionEnd::Exhausted,
                )),
            ]));

        let outcome = fusion
            .run_stream(CursorScope::Account, stream, None)
            .await
            .expect("a departed writer is not a store failure");
        assert!(matches!(outcome, FusionOutcome::NoCursor));
    }

    /// A page claiming `Advance` over coverage that holds a barrier is the
    /// account contradicting its own stream contract, on either half of the
    /// walk. It terminates as a classified contract violation rather than
    /// being read as an ordinary barrier stop: a producer that mislabels its
    /// checkpoint has told the engine nothing it can trust.
    #[tokio::test]
    async fn a_checkpoint_disagreeing_with_its_coverage_terminates_the_walk() {
        let cursor = Checkpoint::Change(bifrost_types::ChangeCursor {
            scope: CursorScope::Account,
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Imap,
                envelope_version: 1,
                bytes: vec![1],
            },
            advanced_through: None,
            envelope_version: 1,
        });
        let page = bifrost_types::InventoryEvent::Batch(bifrost_types::InventoryBatch {
            removed: Vec::new(),
            items: Vec::new(),
            page_boundary: bifrost_types::PageBoundary::Page,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: bifrost_types::PageCheckpoint::Advance(cursor.clone()),
            coverage: barrier_coverage(),
        });
        let completion = bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion {
            checkpoint: bifrost_types::PageCheckpoint::Advance(cursor),
            coverage: barrier_coverage(),
            end: bifrost_types::PartitionEnd::Exhausted,
        });
        for event in [page, completion] {
            // A departed writer, so a walk that skipped the check would stop at
            // the barrier and answer `NoCursor` rather than hang on the writer.
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            drop(rx);
            let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
                Box::pin(futures::stream::iter(vec![event]));
            let outcome = fusion_with_writer(tx)
                .run_stream(CursorScope::Account, stream, None)
                .await
                .expect("a contract violation is an outcome, not an engine error");
            let FusionOutcome::Terminated(error) = outcome else {
                panic!("a mislabelled checkpoint must terminate the walk: {outcome:?}");
            };
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Internal(
                    bifrost_types::InternalErrorKind::AccountContract
                )
            );
        }
    }
}
