//! The private request and reply protocol of one account writer.

use super::*;

/// Ack request: scope + checkpoint the engine should durably persist.
/// Consumers send these through `SyncEngine::ack` after
/// their own item store commits the matching batch.
pub(crate) struct AckRequest {
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
    pub publication: Option<crate::cursor::AckToken>,
    /// Diagnostic provenance flag: `true` labels an engine-generated ack,
    /// and `false` labels a consumer ack in tracing. This flag does not
    /// change acknowledgement validation or persistence.
    pub auto: bool,
    /// Completion channel for consumer-driven acks. `ack`
    /// waits on this so it returns only after the checkpoint store
    /// has accepted or rejected the write.
    pub complete: Option<oneshot::Sender<Result<crate::AckOutcome, Error>>>,
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
// A short-lived channel message, moved once from producer to writer; boxing
// the largest variant would add an allocation per acknowledgement for no gain.
#[allow(clippy::large_enum_variant)]
pub(crate) enum WriterRequest {
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
        attempt: crate::engine::ReattachAttempt,
        cursor: ChangeCursor,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// The named reattach attempt aborted: delete the rows IT owns that are
    /// STILL provisional. A scope whose checkpoint was acknowledged in the
    /// meantime is no longer provisional and survives, and so does a row now
    /// owned by a different attempt.
    ReattachAbort {
        attempt: crate::engine::ReattachAttempt,
        done: oneshot::Sender<()>,
    },
    /// The named reattach attempt cut over: the rows it owns are now ordinary
    /// durable state. Rows owned by other attempts stay provisional.
    ReattachCommit {
        attempt: crate::engine::ReattachAttempt,
    },
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
        claim_for: Option<crate::engine::ReattachAttempt>,
        done: oneshot::Sender<Result<Option<ChangeCursor>, Error>>,
    },
    /// Read the authoritative ledger, including debt retained after a failed write.
    GetLedger {
        done: oneshot::Sender<Result<crate::cursor::DebtLedger, Error>>,
    },
    /// Read resume progress in order with resets and discards.
    GetBackfill {
        scope: CursorScope,
        done: oneshot::Sender<Result<Option<bifrost_types::BackfillCheckpoint>, Error>>,
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
    /// Clear every durable row of the account, completion markers included,
    /// retiring and fencing every scoped publication around the clear. The
    /// account's debt ledger survives, with the debt of every retired
    /// publication folded in.
    ResetAccountForSchemaRecovery {
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
        generation: crate::cursor::WalkGeneration,
        done: oneshot::Sender<Result<bool, Error>>,
    },
    /// Read whether operator policy currently parks this scope at a barrier.
    ScopeBarrierBlocked {
        scope: CursorScope,
        done: oneshot::Sender<Result<bool, Error>>,
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
        generation: crate::cursor::WalkGeneration,
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
        publication: Option<crate::cursor::AckToken>,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Confirm that the consumer durably accepted a publication with no
    /// checkpoint, such as a repair notification batch.
    AcknowledgePublication {
        publication: crate::cursor::AckToken,
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
pub(crate) enum OperatorDecision {
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
            Self::GetLedger { .. } => f.debug_struct("GetLedger").finish(),
            Self::GetBackfill { scope, .. } => {
                f.debug_struct("GetBackfill").field("scope", scope).finish()
            }
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
            Self::ResetAccountForSchemaRecovery { .. } => {
                f.write_str("ResetAccountForSchemaRecovery")
            }
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

impl WriterRequest {
    /// Refuse work without inventing durable state when the initial ledger read
    /// failed. No provisional rows can exist until that read succeeds. Cursor-only
    /// reads remain available; the boolean policy query conservatively parks work.
    pub(super) async fn reject_unloaded(
        self,
        error: Error,
        account: &AccountId,
        store: &Arc<DynCheckpointStore>,
        pending_debt: &mut Vec<(
            bifrost_types::InventoryCoverageReport,
            crate::cursor::WalkGeneration,
        )>,
    ) {
        match self {
            WriterRequest::Ack(req) => {
                if let Some(done) = req.complete {
                    let _ = done.send(Err(Error::AckStore(Box::new(error))));
                }
            }
            WriterRequest::ReattachInsert { done, .. }
            | WriterRequest::PersistEstablished { done, .. }
            | WriterRequest::PersistBackfill { done, .. }
            | WriterRequest::DiscardBackfillProgress { done, .. }
            | WriterRequest::ResetScope { done, .. }
            | WriterRequest::RecordBarrier { done, .. }
            | WriterRequest::ResetAccountForSchemaRecovery { done }
            | WriterRequest::ApplyRepair { done, .. } => {
                let _ = done.send(Err(error));
            }
            WriterRequest::AcknowledgePublication { done, .. } => {
                let _ = done.send(Err(Error::AckStore(Box::new(error))));
            }
            WriterRequest::RecordDebt {
                report,
                generation,
                done,
            } => {
                pending_debt.push((report, generation));
                let _ = done.send(Err(error));
            }
            WriterRequest::CrossWaivedBarriers { done, .. }
            | WriterRequest::OperatorDecision { done, .. } => {
                let _ = done.send(Err(error));
            }
            WriterRequest::ScopeBarrierBlocked { done, .. } => {
                let _ = done.send(Err(error));
            }
            WriterRequest::GetLedger { done } => {
                let _ = done.send(Err(error));
            }
            WriterRequest::GetBackfill { scope, done } => {
                let _ = done.send(store.get_backfill(account, &scope).await);
            }
            WriterRequest::GetChangeCursor { scope, done, .. } => {
                let _ = done.send(store.get_change_cursor(account, &scope).await);
            }
            WriterRequest::ReattachAbort { done, .. } => {
                let _ = done.send(());
            }
            WriterRequest::ReattachCommit { .. } => {}
        }
    }
}
