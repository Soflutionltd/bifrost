//! The account's single durable ack writer and its handle.

use crate::control::SyncControl;
use crate::cursor::store::{CheckpointTransition, DynCheckpointStore};
use crate::cursor::{ClaimLookup, PendingCoverage};
use crate::error::Error;
use bifrost_types::{AccountId, BackfillCheckpoint, ChangeCursor, Checkpoint, CursorScope};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

mod acknowledge;
mod handle;
mod ledger_ops;
mod protocol;
mod provisional;
mod repair;
mod reset;
use crate::{AckOutcome, AckRejection, CompletionRefusal};

pub use handle::WriterHandle;
use ledger_ops::persist_ledger_only;
pub(crate) use protocol::{AckRequest, OperatorDecision, WriterRequest};
use provisional::ProvisionalRows;
use repair::RepairPairing;
use reset::discard_backfill;

/// Reject inconsistent routing before provisional ownership or durable state changes.
pub(crate) fn validate_ack_scope(
    scope: &CursorScope,
    checkpoint: &Checkpoint,
) -> Result<(), Error> {
    let checkpoint_scope = match checkpoint {
        Checkpoint::Change(cursor) => &cursor.scope,
        Checkpoint::Backfill(checkpoint) => &checkpoint.scope,
        _ => {
            return Err(Error::AckRejected(AckRejection::UnsupportedCheckpoint));
        }
    };
    if scope != checkpoint_scope {
        return Err(Error::AckRejected(AckRejection::ScopeMismatch));
    }
    Ok(())
}

/// Bootstrap the writer's proof order before any producer is installed.
/// Keep issuance in PendingCoverage so existing walk producers share one
/// atomic sequence; the durable ledger, rather than attachment age, seeds it.
pub(crate) async fn restore_proof_order(
    account_id: &AccountId,
    store: &DynCheckpointStore,
) -> Result<PendingCoverage, Error> {
    let ledger = store.get_ledger(account_id).await?;
    let mut coverage = PendingCoverage::from_durable_ledger(&ledger);
    coverage.account = Some(account_id.clone());
    Ok(coverage)
}

/// Ack writer task. One per attached account. Receives `AckRequest`
/// messages on `rx` and durably persists the carried checkpoint via
/// the `CheckpointStore`. Notifies the control's checkpoint watch so
/// `pause()` / `checkpoint_now()` waiters wake on a real persisted
/// boundary. Exits when the channel closes (slot detach).
pub(crate) async fn ack_writer(
    account_id: AccountId,
    store: Arc<DynCheckpointStore>,
    control: SyncControl,
    coverage: Arc<PendingCoverage>,
    mut rx: mpsc::Receiver<WriterRequest>,
) {
    let mut writer = Writer {
        account_id,
        store,
        control,
        coverage,
        provisional: ProvisionalRows::default(),
        repairs: RepairPairing::default(),
        ledger: None,
        pending_debt: Vec::new(),
        completed_backfills: HashSet::new(),
    };
    while let Some(request) = rx.recv().await {
        writer.dispatch(request).await;
    }
}

fn proves_backfill_completion(checkpoint: &BackfillCheckpoint) -> bool {
    crate::backfill::partitioner::BackfillKey::decode(&checkpoint.partition)
        .proves_completion(checkpoint.progress.items_done)
}

fn validate_backfill_selection(
    scope: &CursorScope,
    known_complete: bool,
    selected: Option<&BackfillCheckpoint>,
) -> Result<(), Error> {
    if selected.is_some_and(|checkpoint| checkpoint.scope != *scope)
        || known_complete && !selected.is_some_and(proves_backfill_completion)
    {
        return Err(Error::CheckpointStore(
            "backfill resume selection contradicts a durable completion; use backfill_checkpoint_is_later".into(),
        ));
    }
    Ok(())
}

/// The sole custodian of an attachment's durable ledger and row ownership.
struct Writer {
    account_id: AccountId,
    store: Arc<DynCheckpointStore>,
    control: SyncControl,
    coverage: Arc<PendingCoverage>,
    provisional: ProvisionalRows,
    repairs: RepairPairing,
    ledger: Option<crate::cursor::DebtLedger>,
    // Positive evidence only, forgotten after rows are successfully deleted. Lets a
    // resume read detect a broken store ordering without another store read.
    completed_backfills: HashSet<CursorScope>,
    // Retain debt even if loading or writing the authoritative ledger fails.
    pending_debt: Vec<(
        bifrost_types::InventoryCoverageReport,
        crate::cursor::WalkGeneration,
    )>,
}

impl Writer {
    async fn dispatch(&mut self, request: WriterRequest) {
        if self.ledger.is_none() {
            match self.store.get_ledger(&self.account_id).await {
                Ok(mut ledger) => {
                    for (report, generation) in self.pending_debt.drain(..) {
                        ledger.accrue().ingest_debt_only(
                            &report,
                            generation,
                            jiff::Timestamp::now().as_second(),
                        );
                    }
                    self.ledger = Some(ledger);
                }
                Err(error) => {
                    tracing::error!(target: "bifrost.sync.changes", account = ?self.account_id,
                        error = %error, "failed to load debt ledger; refusing ledger-dependent requests");
                    request
                        .reject_unloaded(
                            error,
                            &self.account_id,
                            &self.store,
                            &mut self.pending_debt,
                        )
                        .await;
                    return;
                }
            }
        }
        match request {
            WriterRequest::Ack(req) => self.ack(req).await,
            WriterRequest::ReattachInsert {
                attempt,
                cursor,
                done,
            } => self.reattach_insert(attempt, cursor, done).await,
            WriterRequest::RecordBarrier { incident, done } => {
                self.record_barrier(incident, done).await;
            }
            WriterRequest::CrossWaivedBarriers {
                report,
                generation,
                done,
            } => self.cross_waived_barriers(report, generation, done).await,
            WriterRequest::ScopeBarrierBlocked { scope, done } => {
                self.scope_barrier_blocked(scope, done).await;
            }
            WriterRequest::ApplyRepair {
                resolutions,
                publication,
                done,
            } => self.apply_repair(resolutions, publication, done).await,
            WriterRequest::AcknowledgePublication { publication, done } => {
                self.acknowledge_publication(publication, done).await;
            }
            WriterRequest::RecordDebt {
                report,
                generation,
                done,
            } => self.record_debt(report, generation, done).await,
            WriterRequest::OperatorDecision {
                key,
                decision,
                done,
            } => self.operator_decision(key, decision, done).await,
            WriterRequest::ReattachAbort { attempt, done } => {
                self.reattach_abort(attempt, done).await;
            }
            WriterRequest::ReattachCommit { attempt } => self.reattach_commit(attempt).await,
            WriterRequest::GetLedger { done } => self.get_ledger(done).await,
            WriterRequest::GetBackfill { scope, done } => self.get_backfill(scope, done).await,
            WriterRequest::GetChangeCursor {
                scope,
                claim_for,
                done,
            } => self.get_change_cursor(scope, claim_for, done).await,
            WriterRequest::PersistEstablished { cursor, done } => {
                self.persist_established(cursor, done).await;
            }
            WriterRequest::PersistBackfill { checkpoint, done } => {
                self.persist_backfill(checkpoint, done).await;
            }
            WriterRequest::DiscardBackfillProgress { request, done } => {
                self.discard_backfill_progress(request, done).await;
            }
            WriterRequest::ResetAccountForSchemaRecovery { done } => {
                self.reset_account_for_schema_recovery(done).await;
            }
            WriterRequest::ResetScope {
                scope,
                delete_backfill,
                done,
            } => self.reset_scope(scope, delete_backfill, done).await,
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod repair_publication_tests;
