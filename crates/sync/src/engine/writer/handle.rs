//! Typed client of the single account writer.

use super::*;

/// The only handle recovery code receives for durable state. It can request
/// reads and mutations, but cannot reach the consumer's store implementation.
#[derive(Debug, Clone)]
pub struct WriterHandle {
    tx: mpsc::Sender<WriterRequest>,
}

// Channel loss means the attachment writer is unavailable, never a store error.
// Preserve store errors from the inner reply so recovery can tell them apart.
// A second writer-error enum is unnecessary at this boundary: ShuttingDown is
// already the recovery callers' typed unavailable-writer branch. Lane helpers
// may tolerate it explicitly when no permission or durable progress is claimed.
impl WriterHandle {
    /// Accept a publication after its items are committed to the consumer store.
    /// Checkpoint and repair tokens use the same ordered writer endpoint.
    pub async fn ack(&self, token: crate::cursor::AckToken) -> Result<AckOutcome, Error> {
        let Some(checkpoint) = token.checkpoint().cloned() else {
            return self
                .request(|done| WriterRequest::AcknowledgePublication {
                    publication: token,
                    done,
                })
                .await?
                .map(|()| AckOutcome::RepairAccepted);
        };
        let scope = match &checkpoint {
            Checkpoint::Change(cursor) => cursor.scope.clone(),
            Checkpoint::Backfill(checkpoint) => checkpoint.scope.clone(),
            _ => return Err(Error::AckRejected(AckRejection::UnsupportedCheckpoint)),
        };
        self.request(|done| {
            WriterRequest::Ack(AckRequest {
                scope,
                checkpoint,
                publication: Some(token),
                auto: false,
                complete: Some(done),
            })
        })
        .await?
    }

    /// Start a standalone account writer for inventory and repair producers.
    /// Engine attachments create the same writer as a slot-owned worker.
    /// Drop every handle to drain the task, then await its join handle.
    #[must_use]
    pub fn spawn(
        account_id: AccountId,
        store: Arc<DynCheckpointStore>,
        control: SyncControl,
        capacity: usize,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let coverage = control.publication_ledger();
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let task = tokio::spawn(ack_writer(account_id, store, control, coverage, rx));
        (Self::new(tx), task)
    }

    async fn request<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, Error>>) -> WriterRequest,
    ) -> Result<Result<T, Error>, Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(make(done))
            .await
            .map_err(|_| Error::ShuttingDown)?;
        recv.await.map_err(|_| Error::ShuttingDown)
    }

    pub(crate) async fn record_barrier(
        &self,
        incident: crate::cursor::BarrierIncident,
    ) -> Result<Result<(), Error>, Error> {
        self.request(|done| WriterRequest::RecordBarrier { incident, done })
            .await
    }

    pub(crate) async fn cross_waived_barriers(
        &self,
        report: bifrost_types::InventoryCoverageReport,
        generation: crate::cursor::WalkGeneration,
    ) -> Result<Result<bool, Error>, Error> {
        self.request(|done| WriterRequest::CrossWaivedBarriers {
            report,
            generation,
            done,
        })
        .await
    }

    pub(crate) async fn record_debt(
        &self,
        report: bifrost_types::InventoryCoverageReport,
        generation: crate::cursor::WalkGeneration,
    ) -> Result<Result<(), Error>, Error> {
        self.request(|done| WriterRequest::RecordDebt {
            report,
            generation,
            done,
        })
        .await
    }

    pub(crate) async fn scope_barrier_blocked(&self, scope: CursorScope) -> Result<bool, Error> {
        self.request(|done| WriterRequest::ScopeBarrierBlocked { scope, done })
            .await?
    }

    pub(crate) async fn enqueue_discard(
        &self,
        scope: CursorScope,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::DiscardBackfillProgress { scope, done })
            .await
            .map_err(|_| Error::ShuttingDown)?;
        Ok(recv)
    }

    pub(crate) async fn operator_decision(
        &self,
        key: bifrost_types::ObligationKey,
        decision: OperatorDecision,
    ) -> Result<bool, Error> {
        self.request(|done| WriterRequest::OperatorDecision {
            key,
            decision,
            done,
        })
        .await?
    }

    pub(crate) async fn discard_backfill_progress(
        &self,
        scope: CursorScope,
    ) -> Result<Result<(), Error>, Error> {
        self.request(|done| WriterRequest::DiscardBackfillProgress { scope, done })
            .await
    }

    /// Persist standalone backfill progress without making a coverage claim.
    pub async fn persist_backfill(&self, checkpoint: BackfillCheckpoint) -> Result<(), Error> {
        self.request(|done| WriterRequest::PersistBackfill { checkpoint, done })
            .await?
    }

    pub(crate) async fn apply_repair(
        &self,
        resolutions: Vec<crate::repair::RepairResolution>,
        publication: Option<crate::cursor::AckToken>,
    ) -> Result<(), Error> {
        self.request(|done| WriterRequest::ApplyRepair {
            resolutions,
            publication,
            done,
        })
        .await?
    }

    pub(crate) fn closed() -> Self {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        Self::new(tx)
    }

    pub(crate) fn new(tx: mpsc::Sender<WriterRequest>) -> Self {
        Self { tx }
    }

    /// Read authoritative debt, including reports retained after a store refusal.
    pub async fn get_ledger(&self) -> Result<crate::cursor::DebtLedger, Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::GetLedger { done })
            .await
            .map_err(|_| Error::ShuttingDown)?;
        recv.await.map_err(|_| Error::ShuttingDown)?
    }

    pub(crate) async fn get_backfill(
        &self,
        scope: CursorScope,
    ) -> Result<Option<BackfillCheckpoint>, Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::GetBackfill { scope, done })
            .await
            .map_err(|_| Error::ShuttingDown)?;
        recv.await.map_err(|_| Error::ShuttingDown)?
    }

    pub(crate) async fn get_change_cursor(
        &self,
        scope: CursorScope,
        claim_for: Option<crate::engine::ReattachAttempt>,
    ) -> Result<Option<ChangeCursor>, Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::GetChangeCursor {
                scope,
                claim_for,
                done,
            })
            .await
            .map_err(|_| Error::ShuttingDown)?;
        recv.await.map_err(|_| Error::ShuttingDown)?
    }

    pub(crate) async fn persist_established(&self, cursor: ChangeCursor) -> Result<(), Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::PersistEstablished { cursor, done })
            .await
            .map_err(|_| Error::ShuttingDown)?;
        recv.await.map_err(|_| Error::ShuttingDown)?
    }

    /// Routine cursor invalidation: drop the change cursor so the next
    /// establish re-runs, and PRESERVE backfill state.
    ///
    /// The completion marker is what stops the next attach from re-walking and
    /// re-hydrating the scope's entire history. Deleting it on every cursor
    /// invalidation - which is what `RestartScope` is - would make an ordinary
    /// recovery cost a full historical inventory pass for no schema reason.
    /// The naming exists so that choice is stated at the call site instead of
    /// riding on a bare boolean.
    pub(crate) async fn reset_scope_for_restart(&self, scope: CursorScope) -> Result<(), Error> {
        self.reset_scope(scope, false).await
    }

    /// A scope being taken out of service. Same durable footprint as a restart;
    /// nothing re-establishes afterwards.
    pub(crate) async fn reset_scope_for_disable(&self, scope: CursorScope) -> Result<(), Error> {
        self.reset_scope(scope, false).await
    }

    /// A provider-deleted scope (folder gone): drop the backfill rows too,
    /// completion marker included. Unlike a disable, the folder can come
    /// back under the same id, and a surviving completion marker would make
    /// the recreated incarnation skip its entire cold-start walk - the
    /// consumer plausibly purged its data on `Deleted`, so that is data
    /// invisibility, not a mere leak.
    pub(crate) async fn reset_scope_for_deletion(&self, scope: CursorScope) -> Result<(), Error> {
        self.reset_scope(scope, true).await
    }

    /// Schema recovery: drop every change cursor and backfill row of the
    /// account, completion markers included, keeping the debt ledger.
    ///
    /// Account-wide rather than per registered scope because the rows a schema
    /// bump must clear include scopes the registry does not name - a scope whose
    /// restart budget was exhausted, a disabled scope, one absent from this
    /// session's discovery, one whose deferred inventory has not finalized - and
    /// any surviving completion marker makes that scope's next establishment
    /// skip the inventory walk that re-mints its ids under the new encoding.
    pub(crate) async fn reset_account_for_schema_recovery(&self) -> Result<(), Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::ResetAccountForSchemaRecovery { done })
            .await
            .map_err(|_| Error::ShuttingDown)?;
        recv.await.map_err(|_| Error::ShuttingDown)?
    }

    async fn reset_scope(&self, scope: CursorScope, delete_backfill: bool) -> Result<(), Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::ResetScope {
                scope,
                delete_backfill,
                done,
            })
            .await
            .map_err(|_| Error::ShuttingDown)?;
        recv.await.map_err(|_| Error::ShuttingDown)?
    }

    /// Compensation from a dropped reattach must enter the queue synchronously
    /// before a later attempt can enqueue inserts. A full queue retains the
    /// same fallback ownership in a spawned send.
    pub(crate) fn settle_reattach_on_drop(
        &self,
        attempt: crate::engine::ReattachAttempt,
        committed: bool,
        runtime: Option<&tokio::runtime::Handle>,
        account_id: &AccountId,
    ) {
        let request = if committed {
            WriterRequest::ReattachCommit { attempt }
        } else {
            let (done, _ignored) = oneshot::channel();
            WriterRequest::ReattachAbort { attempt, done }
        };
        match self.tx.try_send(request) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(request)) => {
                if let Some(runtime) = runtime {
                    let writer = self.clone();
                    runtime.spawn(async move {
                        let _ = writer.tx.send(request).await;
                    });
                } else {
                    tracing::error!(target: "bifrost.sync.reopen", account = ?account_id,
                        "writer queue full and no runtime on a dropped reattach; \
                         provisional cursor rows keep their state until a later abort or commit");
                }
            }
        }
    }

    pub(crate) async fn reattach_insert(
        &self,
        attempt: crate::engine::ReattachAttempt,
        cursor: ChangeCursor,
    ) -> Result<(), Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::ReattachInsert {
                attempt,
                cursor,
                done,
            })
            .await
            .map_err(|_| Error::ShuttingDown)?;
        recv.await.map_err(|_| Error::ShuttingDown)?
    }

    pub(crate) async fn reattach_abort(&self, attempt: crate::engine::ReattachAttempt) {
        let (done, recv) = oneshot::channel();
        if self
            .tx
            .send(WriterRequest::ReattachAbort { attempt, done })
            .await
            .is_ok()
        {
            let _ = recv.await;
        }
    }

    pub(crate) async fn reattach_commit(
        &self,
        attempt: crate::engine::ReattachAttempt,
    ) -> Result<(), Error> {
        self.tx
            .send(WriterRequest::ReattachCommit { attempt })
            .await
            .map_err(|_| Error::ShuttingDown)
    }
}

// The channel protocol remains crate-private. This conversion lets internal
// lifecycle wiring hand off its sender while producers keep a typed endpoint.
impl From<mpsc::Sender<WriterRequest>> for WriterHandle {
    fn from(tx: mpsc::Sender<WriterRequest>) -> Self {
        Self::new(tx)
    }
}
