//! Writer operations: ledger_ops.

use super::*;

impl Writer {
    pub(super) async fn record_barrier(
        &mut self,
        incident: crate::cursor::BarrierIncident,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");

        ledger.accrue().record_barrier(incident);
        // No checkpoint advanced, so there is nothing to write
        // alongside it. The ledger still has to reach disk, or the
        // barrier is forgotten on restart and no operator can act on
        // it. Persisted against the scope's existing durable position.
        let result = persist_ledger_only(&account_id, &store, ledger).await;
        let _ = done.send(result);
    }
    pub(super) async fn cross_waived_barriers(
        &mut self,
        report: bifrost_types::InventoryCoverageReport,
        generation: crate::cursor::WalkGeneration,
        done: oneshot::Sender<Result<bool, Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");

        let mut crossed = ledger.stage();
        if !crossed.cross_waived_barriers(&report, generation, jiff::Timestamp::now().as_second()) {
            let _ = done.send(Ok(false));
            return;
        }
        let result = persist_ledger_only(&account_id, &store, &crossed).await;
        if result.is_ok() {
            crossed.commit(ledger);
        }
        let _ = done.send(result.map(|()| true));
    }
    pub(super) async fn scope_barrier_blocked(
        &mut self,
        scope: CursorScope,
        done: oneshot::Sender<Result<bool, Error>>,
    ) {
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");

        let _ = done.send(Ok(ledger.scope_has_blocked_barrier(&scope)));
    }
    pub(super) async fn record_debt(
        &mut self,
        report: bifrost_types::InventoryCoverageReport,
        generation: crate::cursor::WalkGeneration,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");

        ledger
            .accrue()
            .ingest_debt_only(&report, generation, jiff::Timestamp::now().as_second());
        let result = persist_ledger_only(&account_id, &store, ledger).await;
        let _ = done.send(result);
    }
    pub(super) async fn operator_decision(
        &mut self,
        key: bifrost_types::ObligationKey,
        decision: OperatorDecision,
        done: oneshot::Sender<Result<bool, Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");

        // Operator policy takes effect only after persistence succeeds.
        // In particular, a failed waiver must not become accepted loss
        // through a later unrelated checkpoint write.
        let mut decided = ledger.stage();
        let changed = match decision {
            OperatorDecision::Waive {
                by,
                at_unix_seconds,
            } => decided.waive(&key, by, at_unix_seconds),
            OperatorDecision::Block => decided.block(&key),
        };
        let result = if changed {
            let result = persist_ledger_only(&account_id, &store, &decided).await;
            if result.is_ok() {
                decided.commit(ledger);
            }
            result.map(|()| true)
        } else {
            Ok(false)
        };
        let _ = done.send(result);
    }
    pub(super) async fn get_ledger(
        &mut self,
        done: oneshot::Sender<Result<crate::cursor::DebtLedger, Error>>,
    ) {
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");

        let _ = done.send(Ok(ledger.clone()));
    }
}

/// Write the ledger with no checkpoint advance.
pub(super) async fn persist_ledger_only(
    account_id: &AccountId,
    store: &Arc<DynCheckpointStore>,
    ledger: &crate::cursor::DebtLedger,
) -> Result<(), Error> {
    store.put_ledger(account_id, ledger.clone()).await
}
