//! Writer operations: reset.

use super::*;

impl Writer {
    pub(super) async fn get_backfill(
        &mut self,
        scope: CursorScope,
        done: oneshot::Sender<Result<Option<BackfillCheckpoint>, Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);

        let _ = done.send(store.get_backfill(&account_id, &scope).await);
    }
    pub(super) async fn persist_backfill(
        &mut self,
        checkpoint: BackfillCheckpoint,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");

        let result = store
            .apply_transition(
                &account_id,
                CheckpointTransition {
                    checkpoint: Checkpoint::Backfill(checkpoint),
                    ledger: ledger.clone(),
                },
            )
            .await;
        let _ = done.send(result);
    }
    pub(super) async fn discard_backfill_progress(
        &mut self,
        scope: CursorScope,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let coverage = Arc::clone(&self.coverage);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");

        // The change cursor stays and nothing re-establishes: this is not
        // a reset, the scope is healthy and its walk simply cannot be
        // resumed from where it stopped.
        //
        // But deleting the rows is not enough on its own, because the
        // publications of the discarded attempt are still outstanding. A
        // replacement consumer holding a later page can let this delete
        // finish and acknowledge afterwards, and the writer would honour
        // it and RECREATE exactly the resume position past the hole that
        // the discard existed to remove. So the discarded attempt is
        // fenced by the same machinery a reset uses: `invalidate_scope`
        // retires the scope's publications, raises the acknowledgement
        // fence past every id minted before now, and hands back their
        // debt - which must be ingested here or the obligations vanish
        // with the publications carrying them.
        //
        // Two passes for the reason the reset has two: the delete below
        // is an await, and a still-running walk can register a further
        // publication inside it.
        // Unbounded: the orchestrator sends this BEFORE its retry mints
        // anything, so there is nothing newer to protect.
        let discarded =
            discard_backfill(&account_id, &store, &coverage, ledger, &scope, None).await;
        let _ = done.send(discarded);
    }
    pub(super) async fn reset_account_for_schema_recovery(
        &mut self,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let coverage = Arc::clone(&self.coverage);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");
        let provisional = &mut self.provisional.rows;

        // The account-wide form of the `ResetScope` arm below, with the
        // same two-pass shape. Pass 1 retires every scoped publication,
        // takes the whole carry-forward slot and raises the account
        // fence BEFORE the first await, so nothing registered so far
        // can be acknowledged after the clear. Its debt rides the
        // clear's own atomic ledger write rather than a separate one.
        let debt = coverage.invalidate_account();
        let now = jiff::Timestamp::now().as_second();
        for report in &debt.reports {
            ledger
                .accrue()
                .ingest_debt_only(report, debt.generation, now);
        }
        let cleared = store
            .clear_account_checkpoints(&account_id, ledger.clone())
            .await;
        // Only a committed clear removed the provisional rows. A failed
        // clear left them in place, and a delayed `ReattachAbort` must
        // still find its ownership to delete them.
        if cleared.is_ok() {
            provisional.clear();
        }
        // Pass 2 closes the window the clear's await opened: anything
        // registered meanwhile is retired, fenced and its debt carried.
        let debt = coverage.invalidate_account();
        let carried = !debt.reports.is_empty();
        let now = jiff::Timestamp::now().as_second();
        for report in &debt.reports {
            ledger
                .accrue()
                .ingest_debt_only(report, debt.generation, now);
        }
        // Written after a FAILED clear too: pass 1 already removed its
        // debt from the coverage ledger, so without this write it would
        // live only in writer memory and be lost if the reset exhausts
        // and the slot detaches. That write cannot change the answer
        // (`cleared` is already `Err`); a pass-2 failure after a
        // committed clear does, so the caller retries.
        let second = if cleared.is_err() || carried {
            persist_ledger_only(&account_id, &store, ledger).await
        } else {
            Ok(())
        };
        if let Err(error) = &second {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                error = %error,
                "account reset could not persist the debt of the publications it retired"
            );
        }
        let _ = done.send(cleared.and(second));
    }
    pub(super) async fn reset_scope(
        &mut self,
        scope: CursorScope,
        delete_backfill: bool,
        done: oneshot::Sender<Result<(), Error>>,
    ) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let coverage = Arc::clone(&self.coverage);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");
        let provisional = &mut self.provisional.rows;

        // Nothing here touches backfill capacity explicitly, and that is
        // the fix rather than an omission. `invalidate_scope` removes the
        // scope's boundary registrations, and those registrations ARE the
        // capacity, so both passes free it atomically with the retirement
        // - including a page the still-running walk published inside the
        // store awaits below, and including pages a later publication had
        // already superseded. The previous shape kept a separate permit
        // map and needed a reset counter, a per-pass id list, and a rule
        // about registrations racing retirements; each of those was a
        // seam, and each seam was a defect.
        // Retiring the scope's publications and extracting their debt
        // is ONE ledger operation, and it happens BEFORE the first
        // await. Snapshotting the debt first and invalidating after the
        // store writes left an await window in which a still-running
        // scope could register a further publication: invalidation
        // would then retire it, but its debt was never in the
        // persisted snapshot, so the obligation vanished.
        let mut debt = coverage.invalidate_scope(&scope);
        let now = jiff::Timestamp::now().as_second();
        for report in &debt.reports {
            ledger
                .accrue()
                .ingest_debt_only(report, debt.generation, now);
        }
        let result = async {
            persist_ledger_only(&account_id, &store, ledger).await?;
            if delete_backfill {
                store.delete_backfill(&account_id, &scope).await?;
            }
            store.delete_change_cursor(&account_id, &scope).await?;
            Ok(())
        }
        .await;
        // Ownership follows the change row, not the reset request. A
        // failed ledger write or delete leaves that row abort-owned;
        // release the mark only once its deletion has succeeded.
        // Retaining it cannot let the old attempt's abort delete a
        // freshly established cursor: both reset callers
        // (`establish_if_absent`, `restart_scope_admitted`) re-establish
        // through `run_establish`, which reads the row first and reaches
        // `PersistEstablished` only when the read answered ABSENT. A
        // still-provisional row is answered absent to that live read, so
        // it is overwritten rather than resumed, and the persist then
        // releases the mark. A committed row that survived a failed
        // reset is resumed.
        if result.is_ok() {
            provisional.remove(&scope);
        }
        // Second pass, closing the window the deletes just opened.
        // Anything the still-draining scope published while they were
        // in flight is retired here, its debt carried, and the fence
        // moved past it - so a late acknowledgement cannot re-create
        // the cursor row this reset deleted. Ids minted after this
        // point (the re-establish) sit above the fence.
        debt = coverage.invalidate_scope(&scope);
        if !debt.reports.is_empty() {
            let now = jiff::Timestamp::now().as_second();
            for report in &debt.reports {
                ledger
                    .accrue()
                    .ingest_debt_only(report, debt.generation, now);
            }
            if let Err(error) = persist_ledger_only(&account_id, &store, ledger).await {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?scope,
                    error = %error,
                    "scope reset could not persist debt published during its deletes"
                );
            }
        }
        let _ = done.send(result);
    }
}

/// Drop a scope's durable backfill rows AND fence the attempt that produced
/// them, as one writer-ordered operation.
///
/// Called both for the orchestrator's explicit discard and from the ack path
/// when a completion marker turns out to belong to a walk that lost pages: in
/// both cases every publication of that attempt is void, and leaving them
/// acknowledgeable is what lets a late ack rebuild the resume position the
/// discard just removed.
pub(super) async fn discard_backfill(
    account_id: &AccountId,
    store: &Arc<DynCheckpointStore>,
    coverage: &PendingCoverage,
    ledger: &mut crate::cursor::DebtLedger,
    scope: &CursorScope,
    through: Option<&crate::cursor::AckToken>,
) -> Result<(), Error> {
    // The BACKFILL half only. Fencing the whole scope refused a replacement
    // consumer's acknowledgement of live batches it had really received, with an
    // `Unknown` no retry could ever satisfy and no instruction to reconcile -
    // after which the live cursor advances past changes nothing replayed. The
    // discard is about this scope's backfill rows and about nothing else.
    // The ceiling bounds the ledger retirement; it cannot bound the DELETE.
    // `CheckpointStore::delete_backfill` takes every row of the scope and knows
    // nothing of publication ids, so a delayed acknowledgement of an old walk's
    // marker - refused, correctly, as `WalkNotWhole` - would delete the rows a
    // LATER attempt earned, up to and including a durable completion its consumer
    // had already acknowledged. When a newer attempt exists, the fence is the
    // whole of what this discard may safely do: the old attempt stops being
    // acknowledgeable, the request stays outstanding (a ceilinged discard never
    // takes it), and the newer attempt's own repair - the rescan's reopen and its
    // unbounded pre-retry discard - is what removes rows if they need removing.
    let rows_belong_to_a_newer_attempt =
        through.is_some_and(|ceiling| coverage.backfill_minted_after(scope, ceiling));
    let mut debt = coverage.invalidate_scope_backfill(scope, through);
    let now = jiff::Timestamp::now().as_second();
    for report in &debt.reports {
        ledger
            .accrue()
            .ingest_debt_only(report, debt.generation, now);
    }
    let result = async {
        persist_ledger_only(account_id, store, ledger).await?;
        if rows_belong_to_a_newer_attempt {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                "a later backfill attempt has published on this scope; fencing the refused \
                 attempt without deleting rows that are not its own"
            );
            return Ok(());
        }
        store.delete_backfill(account_id, scope).await
    }
    .await;
    // Second pass, closing the window the delete just opened.
    debt = coverage.invalidate_scope_backfill(scope, through);
    if !debt.reports.is_empty() {
        let now = jiff::Timestamp::now().as_second();
        for report in &debt.reports {
            ledger
                .accrue()
                .ingest_debt_only(report, debt.generation, now);
        }
        if let Err(error) = persist_ledger_only(account_id, store, ledger).await {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                error = %error,
                "could not persist debt published while a backfill discard was in flight"
            );
        }
    }
    // The request is settled only by a delete that SUCCEEDED, and only by an
    // UNBOUNDED discard.
    //
    // Success, because a store error leaves the rows exactly where they were: a
    // request consumed by a delete that did not happen is a repair nobody will
    // attempt again, and the teardown drain finds nothing to retry. Unbounded,
    // because a ceilinged discard retires only the attempt at or below the
    // refused marker while its delete takes every row - so a delayed W1 marker
    // acknowledgement arriving during W2 would clear W2's request while leaving
    // W2's own loss unrepaired.
    if through.is_none() && result.is_ok() {
        coverage.take_backfill_discard(scope);
    }
    result
}
