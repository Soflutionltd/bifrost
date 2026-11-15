//! Writer operations: provisional.

use super::*;

/// Rows written only for a replacement that has not committed yet. An abort
/// deletes exactly these rows, never a checkpoint the consumer acknowledged.
/// Attempt ownership also fences a dropped attempt's delayed abort from a
/// newer replacement that inserted or adopted the same scope.
#[derive(Default)]
pub(super) struct ProvisionalRows {
    pub(super) rows: HashMap<CursorScope, crate::engine::ReattachAttempt>,
}

impl Writer {
    pub(super) async fn reattach_insert(
        &mut self,
        attempt: crate::engine::ReattachAttempt,
        cursor: ChangeCursor,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");
        let provisional = &mut self.provisional.rows;

        let scope = cursor.scope.clone();
        // Carries NO coverage report, which means "leave the ledger
        // unchanged" - emphatically not "coverage is complete". A
        // reattach writes a cursor it obtained from establishment; it
        // proves nothing about what any enumeration covered, and
        // manufacturing a completeness claim here would discharge debt
        // that nothing re-read.
        let result = store
            .apply_transition(
                &account_id,
                CheckpointTransition {
                    checkpoint: Checkpoint::Change(cursor),
                    ledger: ledger.clone(),
                },
            )
            .await;
        if result.is_ok() {
            provisional.insert(scope, attempt);
        }
        let _ = done.send(result);
    }
    pub(super) async fn reattach_abort(
        &mut self,
        attempt: crate::engine::ReattachAttempt,
        done: oneshot::Sender<()>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let provisional = &mut self.provisional.rows;

        let owned: Vec<CursorScope> = provisional
            .iter()
            .filter(|(_, owner)| **owner == attempt)
            .map(|(scope, _)| scope.clone())
            .collect();
        for scope in owned {
            if let Err(error) = store.delete_change_cursor(&account_id, &scope).await {
                tracing::error!(
                    target: "bifrost.sync.reopen",
                    account = ?account_id,
                    scope = ?scope,
                    error = %error,
                    "failed to roll back replacement cursor after aborted reattach"
                );
            } else {
                // A failed delete leaves the row abort-owned for a retry.
                // Later queued claims, commits and durable acks may still
                // transfer or promote it before that retry arrives.
                provisional.remove(&scope);
            }
        }
        let _ = done.send(());
    }
    pub(super) async fn reattach_commit(&mut self, attempt: crate::engine::ReattachAttempt) {
        let provisional = &mut self.provisional.rows;

        provisional.retain(|_, owner| *owner != attempt);
    }
    pub(super) async fn get_change_cursor(
        &mut self,
        scope: CursorScope,
        claim_for: Option<crate::engine::ReattachAttempt>,
        done: oneshot::Sender<Result<Option<ChangeCursor>, Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let provisional = &mut self.provisional.rows;

        let mut result = store.get_change_cursor(&account_id, &scope).await;
        // A reattach's own resume read (`claim_for`) takes over a row an
        // earlier attempt inserted and has not settled, so that
        // attempt's delayed abort cannot delete a row the resuming
        // topology depends on.
        //
        // An ordinary read comes only from the LIVE incarnation's scoped
        // recovery. It must not see, and must not adopt, a row that is
        // still provisional. Drop can defer an abort send past the
        // reopen lock, so this read can land between an aborted
        // reattach's insert and its rollback. That row was minted by a
        // replacement that never became live, and no acknowledgement
        // ever confirmed it: any acknowledged change checkpoint would
        // already have promoted it out of `provisional`. Resuming from
        // it would skip an inventory baseline the consumer never
        // durably received, and a replacement opened against different
        // credentials could even hand the live handle another mailbox's
        // state. So the live scope sees the row as absent and
        // establishes fresh. Its `PersistEstablished` then releases the
        // mark, so the late abort cannot delete the live row. An
        // enterprise user pays one re-inventory in this window rather
        // than silently missing mail.
        if matches!(&result, Ok(Some(_))) && provisional.contains_key(&scope) {
            match claim_for {
                Some(attempt) => {
                    provisional.insert(scope.clone(), attempt);
                }
                None => result = Ok(None),
            }
        }
        let _ = done.send(result);
    }
    pub(super) async fn persist_established(
        &mut self,
        cursor: ChangeCursor,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");
        let provisional = &mut self.provisional.rows;

        // Only the live incarnation persists through this request
        // (a reattach writes through `ReattachInsert`). A successful
        // write replaces any provisional row the live read was shown
        // as absent, so the row now belongs to the live scope and an
        // aborting attempt must not delete it.
        let scope = cursor.scope.clone();
        let result = store
            .apply_transition(
                &account_id,
                CheckpointTransition {
                    checkpoint: Checkpoint::Change(cursor),
                    ledger: ledger.clone(),
                },
            )
            .await;
        if result.is_ok() {
            provisional.remove(&scope);
        }
        let _ = done.send(result);
    }
}
