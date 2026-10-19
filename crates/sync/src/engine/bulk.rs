//! The bulk mutation pipeline and its campaign entry points.

use super::*;

impl SyncEngine {
    /// Drive a single-account bulk-flag campaign against an attached
    /// account, applying the read-back guard to the retry candidates.
    ///
    /// Returns the per-batch counters aggregated across the run.
    ///
    /// Mutation accounting consumes `ItemOutcome<MutationSuccess>` and
    /// dispatches per-item failures via [`crate::recovery::plan_recovery`]:
    ///
    /// - `Retry { SameRequest | AfterAuthRefresh }` -> retry queue.
    /// - `Retry { AfterStateRefresh }` and `Reconcile { CheckTarget }`
    ///   -> read-back queue.
    /// - `Reconcile { DedupeByClientId }` -> dedupe counter increments
    ///   plus a `Warning::OperatorAttentionNeeded` (the dedupe itself
    ///   lives at the consumer because the client-id space is theirs).
    ///   (sync-D4)
    /// - `Engine(_)` -> blocked-by-engine counter increments AND the
    ///   directive is forwarded through `ReopenRequest::Recovery` so
    ///   the engine's recovery dispatch handles the restart / downgrade
    ///   / schema clear. The campaign returns with a partial outcome
    ///   accounting for work seen so far. (sync-D5)
    /// - terminal recovery -> `failed_terminal`.
    ///
    /// `ItemOutcome::Uncertain` is always queued for read-back.
    pub async fn bulk_set_flags(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        op: bifrost_types::FlagOp,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.run_bulk_pipeline(
            account_id,
            targets,
            BulkPipelineOp::SetFlags(op),
            vendor,
            protocol,
        )
        .await
    }

    /// Drive a single-account bulk-move campaign against an attached
    /// account, routing every target into `destination`.
    ///
    /// Shares the idempotency / retry / recovery loop with
    /// [`Self::bulk_set_flags`] (see [`Self::run_bulk_pipeline`]); the
    /// only differences are the wire op (`Account::bulk_move_from`) and the
    /// read-back guard, which reconciles against container membership
    /// rather than a flag set. Like `bulk_set_flags`, this is the
    /// volume path where the read-back guard and idempotency key matter,
    /// so it is NOT a one-shot direct call.
    pub async fn bulk_move(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        destination: MembershipScope,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.bulk_move_from(account_id, targets, destination, None, vendor, protocol)
            .await
    }

    /// [`Self::bulk_move`] with the source container the targets are
    /// leaving.
    ///
    /// Identical pipeline; the source rides through to
    /// [`bifrost_types::Account::bulk_move_from`], which is what lets a
    /// Gmail campaign express the detach in the same `batchModify` as
    /// the attach instead of one `remove_from_container` per message.
    /// `None` is exactly [`Self::bulk_move`].
    ///
    /// The read-back guard is unchanged: it reconciles membership of
    /// `destination`, which is the property that says the move landed.
    /// It does not separately re-verify absence from `source`.
    pub async fn bulk_move_from(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        destination: MembershipScope,
        source: Option<MembershipScope>,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.run_bulk_pipeline(
            account_id,
            targets,
            BulkPipelineOp::Move {
                destination,
                source,
            },
            vendor,
            protocol,
        )
        .await
    }

    /// Drive a single-account bulk-destroy campaign against an attached
    /// account.
    ///
    /// Shares the idempotency / retry / recovery loop with
    /// [`Self::bulk_set_flags`] (see [`Self::run_bulk_pipeline`]); the
    /// wire op is `Account::bulk_destroy` and the read-back guard
    /// reconciles against object *absence* (a destroyed id no longer
    /// hydrates). NOT a one-shot direct call, for the same reason as
    /// `bulk_set_flags`.
    pub async fn bulk_destroy(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.run_bulk_pipeline(
            account_id,
            targets,
            BulkPipelineOp::Destroy,
            vendor,
            protocol,
        )
        .await
    }

    /// Shared bulk-mutation pipeline backing [`Self::bulk_move`] and
    /// [`Self::bulk_destroy`].
    ///
    /// Mirrors [`Self::bulk_set_flags`] exactly - idempotency-key
    /// vending, the per-id `classify_item_outcome` accumulation, the
    /// retry loop, the stream-terminated recovery dispatch
    /// (Retry / Reconcile / Engine / Terminal), and the final read-back
    /// guard - with two op-specific seams: the wire submit call and the
    /// matching read-back guard. All three entry points route through
    /// here; the read-back shape (`FlagOp` / membership / absence) is
    /// selected from `op` rather than by duplicating the loop.
    async fn run_bulk_pipeline(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        op: BulkPipelineOp,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        use crate::recovery::{RecoveryPlan, plan_recovery};
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let max_retries = self.config.mutation_max_retries;
        let retry_queue_cap = self.config.mutation.retry_queue_cap;

        let key = vendor.next(protocol);

        let mut outcomes: HashMap<bifrost_types::ObjectId, MutationBucket> = HashMap::new();
        let mut retry_ids: Vec<bifrost_types::ObjectId> = Vec::new();
        let mut dedupe_count: u64 = 0;
        let mut remaining: Vec<bifrost_types::ObjectId> = targets;
        let mut attempt: u32 = 0;
        let mut retry_advice: Option<RetryAdvice> = None;
        let mut blocked_by_engine: bool = false;
        let mut forwarded_directives: HashSet<DirectiveKey> = HashSet::new();

        loop {
            if let Some(advice) = retry_advice.take() {
                let delay = crate::recovery::retry_delay(
                    &advice,
                    std::time::SystemTime::now(),
                    Duration::from_secs(1),
                );
                tokio::select! {
                    () = slot.shutdown.cancelled() => return Err(Error::ShuttingDown),
                    () = tokio::time::sleep(delay) => {}
                }
            }
            // Honor every throttle deadline this attempt consumes - the
            // account-wide holds plus the keys the account declares for the
            // targets being submitted and the move's memberships (recorded by
            // this campaign's previous attempt, a poll, or a sibling account
            // via a shared key) - before submitting.
            //
            // A campaign is ONE throttle unit: it waits until every budget its
            // submission consumes is free, rather than splitting the targets
            // by budget. Its idempotency key, retry counter, read-back guard
            // and every termination sweep all assume one submitted set, so a
            // campaign spanning a held mailbox and a free one waits for the
            // held one. That costs latency inside one operation; it never
            // sends to a held budget and never burns a retry on a wait (this
            // loop sits outside the attempt counter).
            //
            // The check is the LAST thing before submission: it runs again
            // after the retry-advice sleep above, after a pause park, and
            // after admission, because a deadline recorded during any of
            // those waits existed before this attempt started. No permit or
            // activity guard is held across a throttle wait.
            let (_activity, _admission, account) = loop {
                if !slot.control.wait_until_running(&slot.shutdown).await {
                    return Err(Error::ShuttingDown);
                }
                let keys = campaign_keys(
                    slot.current.load_full().as_ref().as_ref(),
                    account_id,
                    &remaining,
                    &op,
                );
                if let Some(wait) =
                    crate::recovery::throttle_wait(&slot.throttles, account_id, &keys)
                {
                    tokio::select! {
                        () = slot.shutdown.cancelled() => return Err(Error::ShuttingDown),
                        () = tokio::time::sleep(wait) => {}
                    }
                    continue;
                }
                let admission = tokio::select! {
                    () = slot.shutdown.cancelled() => return Err(Error::ShuttingDown),
                    permit = slot.scheduler.admit(
                        account_id.clone(),
                        slot.control.priority_snapshot(),
                        crate::scheduler::WorkKind::Mutation,
                    ) => permit?,
                };
                // The final check asks the account snapshot this attempt
                // submits through, not the one the keys above came from: a
                // reopen during admission can install one whose declaration
                // differs.
                let account = slot.current.load_full();
                let keys = campaign_keys(account.as_ref().as_ref(), account_id, &remaining, &op);
                if crate::recovery::throttle_wait(&slot.throttles, account_id, &keys).is_some() {
                    drop(admission);
                    continue;
                }
                if let Some(activity) = slot.control.begin_activity() {
                    break (activity, admission, account);
                }
            };
            let target_stream: AccountStream<bifrost_types::ObjectId> =
                Box::pin(futures::stream::iter(remaining.clone()));
            let mut stream = match &op {
                BulkPipelineOp::SetFlags(flag_op) => {
                    account.bulk_set_flags(target_stream, flag_op.clone(), key.clone())
                }
                BulkPipelineOp::Move {
                    destination,
                    source,
                } => account.bulk_move_from(
                    target_stream,
                    destination.clone(),
                    source.clone(),
                    key.clone(),
                ),
                BulkPipelineOp::Destroy => account.bulk_destroy(target_stream, key.clone()),
            };
            // Only the retry queue is per-attempt. Read-back membership
            // is derived from `outcomes` after the campaign ends, so an
            // id parked in the read-back lane survives an attempt that
            // resubmits its siblings.
            retry_ids.clear();
            let mut stream_termination_advice: Option<RetryAdvice> = None;
            let mut item_retry_advice: Option<RetryAdvice> = None;

            loop {
                let event = tokio::select! {
                    () = slot.shutdown.cancelled() => return Err(Error::ShuttingDown),
                    event = stream.next() => event,
                };
                let Some(event) = event else {
                    break;
                };
                match event {
                    bifrost_types::SyncEvent::Batch(batch) => {
                        for item in batch.items {
                            if let Some((directive, error)) = classify_item_outcome(
                                item,
                                &mut outcomes,
                                &mut retry_ids,
                                &mut dedupe_count,
                                &slot.throttles,
                                account_id,
                                &mut item_retry_advice,
                            ) {
                                if should_forward_engine_recovery(
                                    &mut forwarded_directives,
                                    &directive,
                                ) {
                                    let scope = crate::recovery::directive_target_scope(&directive);
                                    let _ = slot
                                        .reopen_tx
                                        .send(ReopenRequest::Recovery { scope, error })
                                        .await;
                                }
                                blocked_by_engine = true;
                            }
                        }
                    }
                    bifrost_types::SyncEvent::Terminated(err) => {
                        let original = err.clone();
                        match plan_recovery(err) {
                            RecoveryPlan::Retry(advice) => {
                                queue_unresolved_for_retry(&remaining, &outcomes, &mut retry_ids);
                                // Share the throttle deadline before the
                                // next attempt sleeps it off locally.
                                crate::recovery::record_throttle(
                                    &slot.throttles,
                                    account_id,
                                    &advice,
                                    &original,
                                );
                                stream_termination_advice = Some(advice);
                                break;
                            }
                            RecoveryPlan::Reconcile(advice) => {
                                crate::recovery::record_reconcile_throttle(
                                    &slot.throttles,
                                    account_id,
                                    &advice,
                                    &original,
                                );
                                let mut wants_dedupe = false;
                                for action in &advice.guidance.actions {
                                    match action {
                                        ReconcileAction::CheckTarget => {}
                                        ReconcileAction::DedupeByClientId => wants_dedupe = true,
                                        _ => {}
                                    }
                                }
                                for id in &remaining {
                                    if !matches!(
                                        outcomes.get(id),
                                        Some(
                                            MutationBucket::Applied
                                                | MutationBucket::Skipped
                                                | MutationBucket::FailedTerminal
                                        )
                                    ) {
                                        outcomes
                                            .insert(id.clone(), MutationBucket::PendingReadback);
                                    }
                                }
                                if wants_dedupe {
                                    dedupe_count = dedupe_count.saturating_add(1);
                                    let warning = bifrost_types::Warning::user_safe(
                                        bifrost_types::WarningKind::OperatorAttentionNeeded,
                                        "reconcile requested dedupe-by-client-id; consumer must dedupe",
                                    );
                                    let me = MultiplexerEvent {
                                        scope: CursorScope::Account,
                                        event: Arc::new(SyncEvent::Warning(warning)),
                                        checkpoint: None,
                                        publication: None,
                                    };
                                    let _ = slot.multiplexer.changes_tx.send(me);
                                }
                                break;
                            }
                            RecoveryPlan::Engine(directive) => {
                                let directive_scope =
                                    crate::recovery::directive_target_scope(&directive);
                                let _ = slot
                                    .reopen_tx
                                    .send(ReopenRequest::Recovery {
                                        scope: directive_scope,
                                        error: original,
                                    })
                                    .await;
                                blocked_by_engine = true;
                                for id in &remaining {
                                    if !matches!(
                                        outcomes.get(id),
                                        Some(
                                            MutationBucket::Applied
                                                | MutationBucket::Skipped
                                                | MutationBucket::FailedTerminal
                                        )
                                    ) {
                                        outcomes
                                            .insert(id.clone(), MutationBucket::BlockedByEngine);
                                    }
                                }
                                break;
                            }
                            RecoveryPlan::Terminal(fatal) => {
                                return Err(Error::Account(fatal.into_inner()));
                            }
                        }
                    }
                    bifrost_types::SyncEvent::Done(_) => break,
                    // Forwarded on the same channel the engine uses for its own
                    // campaign warnings a few lines above. Gmail deliberately
                    // interleaves a `StrategyDowngraded` warning ahead of its
                    // batch when it cannot represent a requested flag, and
                    // absorbing it here left that announcement nowhere. The
                    // structural answer still rides on
                    // `MutationSuccess::Downgraded`, so this is a second copy
                    // rather than the only one - but a lane the producer
                    // deliberately wrote to should not dead-end in the engine.
                    bifrost_types::SyncEvent::Warning(warning) => {
                        let me = MultiplexerEvent {
                            scope: CursorScope::Account,
                            event: Arc::new(SyncEvent::Warning(warning)),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = slot.multiplexer.changes_tx.send(me);
                    }
                    bifrost_types::SyncEvent::Progress(_) => {}
                    _ => {}
                }
            }

            if blocked_by_engine {
                for id in &remaining {
                    // `PendingReadback` is deliberately excluded, exactly as it
                    // is from the retry-termination sweep. Those are the ids
                    // whose write may already have landed - `Uncertain`,
                    // downgrades, `AfterStateRefresh` - and the read-back lane
                    // exists so they are VERIFIED rather than replayed or
                    // guessed at. Claiming them here emptied
                    // `unresolved_readback_ids`, skipped the guard entirely,
                    // and reported a mutation that actually applied as
                    // `blocked_by_engine`. The campaign still stops submitting;
                    // the guard that follows only observes state.
                    if !matches!(
                        outcomes.get(id),
                        Some(
                            MutationBucket::Applied
                                | MutationBucket::Skipped
                                | MutationBucket::FailedTerminal
                                | MutationBucket::BlockedByEngine
                                | MutationBucket::PendingReadback
                        )
                    ) {
                        outcomes.insert(id.clone(), MutationBucket::BlockedByEngine);
                    }
                }
                break;
            }

            attempt = attempt.saturating_add(1);
            let retry_set: HashSet<_> = retry_ids.iter().cloned().collect();
            let mut next_remaining: Vec<bifrost_types::ObjectId> = remaining
                .iter()
                .filter(|id| retry_set.contains(*id))
                .cloned()
                .collect();
            // `MutationConfig::retry_queue_cap` bounds how wide ONE
            // resubmission may be. The retry set only ever shrinks (it is
            // filtered out of `remaining`), so this is not a guard against
            // unbounded growth - it is a ceiling on the per-attempt work a
            // single oversized campaign can demand of the account, which
            // otherwise resubmits its whole still-failing set on every one of
            // `mutation_max_retries` attempts.
            //
            // The excess is NOT dropped. It is marked `PendingRetry` HERE
            // rather than left to the post-loop sweep below, because that sweep
            // runs only on the break path: an id merely truncated out of
            // `next_remaining` before a `continue` is no longer in `remaining`,
            // so no later attempt resolves it and it would leave the campaign
            // uncounted entirely - reporting success for work that never
            // happened. Marked here, it is counted as pending and then run
            // through the read-back guard, so a mutation that actually landed
            // is still reconciled rather than reported as outstanding.
            //
            // `split_off` keeps the retained prefix in `remaining` order, so
            // what survives is the earliest-submitted ids rather than an
            // arbitrary hash-order slice.
            if next_remaining.len() > retry_queue_cap {
                let deferred = next_remaining.split_off(retry_queue_cap);
                tracing::warn!(
                    target: "bifrost.sync.mutation",
                    account_id = ?account_id,
                    attempt,
                    cap = retry_queue_cap,
                    deferred = deferred.len(),
                    "retry queue cap reached; deferring the excess as pending retry"
                );
                for id in deferred {
                    outcomes.insert(id, MutationBucket::PendingRetry);
                }
            }
            if attempt < max_retries && !next_remaining.is_empty() {
                // A stream-level Retry termination speaks for the whole
                // submission, so it wins; otherwise the per-item advice of the
                // failures actually being resubmitted supplies the delay. With
                // neither, `retry_delay`'s own fallback applies once an advice
                // exists - what must not happen is resubmitting instantly.
                retry_advice = stream_termination_advice.or(item_retry_advice);
                std::mem::swap(&mut remaining, &mut next_remaining);
                continue;
            }
            for id in &retry_set {
                outcomes.insert(id.clone(), MutationBucket::PendingRetry);
            }
            break;
        }

        let mut totals = counters_from_outcomes(&outcomes);
        for _ in 0..dedupe_count {
            totals.record_dedupe_by_client_id();
        }
        let readback_ids = unresolved_readback_ids(&outcomes);
        if !readback_ids.is_empty() {
            // The read-back guard hydrates over the wire, and the
            // campaign's own admission permit was released when the
            // attempt loop broke. Re-admit rather than running unbudgeted
            // work: an oversized campaign otherwise leaves the caps
            // behind exactly where its heaviest fan-out begins.
            //
            // The read-back is wire work against the same budgets the write
            // used, so it honours them too: the ids it hydrates are the whole
            // unresolved set (`unresolved_readback_ids`), not the last retry
            // vector, which may have shrunk past an id a held budget still
            // covers. Waiting here never turns a `PendingReadback` id into a
            // resubmission; it only delays the observation.
            let (_activity, _readback_admission, account) = loop {
                if !slot.control.wait_until_running(&slot.shutdown).await {
                    return Err(Error::ShuttingDown);
                }
                let keys = crate::recovery::declared_keys(
                    slot.current.load_full().as_ref().as_ref(),
                    account_id,
                    bifrost_types::ThrottleWork::Objects(&readback_ids),
                );
                if let Some(wait) =
                    crate::recovery::throttle_wait(&slot.throttles, account_id, &keys)
                {
                    tokio::select! {
                        () = slot.shutdown.cancelled() => return Err(Error::ShuttingDown),
                        () = tokio::time::sleep(wait) => {}
                    }
                    continue;
                }
                let admission = tokio::select! {
                    () = slot.shutdown.cancelled() => return Err(Error::ShuttingDown),
                    permit = slot.scheduler.admit(
                        account_id.clone(),
                        slot.control.priority_snapshot(),
                        crate::scheduler::WorkKind::Mutation,
                    ) => permit?,
                };
                // Asked of the snapshot the read-back hydrates through.
                let account = slot.current.load_full();
                let keys = crate::recovery::declared_keys(
                    account.as_ref().as_ref(),
                    account_id,
                    bifrost_types::ThrottleWork::Objects(&readback_ids),
                );
                if crate::recovery::throttle_wait(&slot.throttles, account_id, &keys).is_some() {
                    drop(admission);
                    continue;
                }
                if let Some(activity) = slot.control.begin_activity() {
                    break (activity, admission, account);
                }
            };
            // A hydration the guard absorbs as `uncertain` may still have
            // failed on a 429; its deadline goes into the shared bucket like
            // any other lane's, so siblings see a hold the read-back found.
            let throttles = Arc::clone(&slot.throttles);
            let record = move |error: &AccountError| {
                record_error_throttle(&throttles, account_id, error);
            };
            let sink: crate::mutation::readback::ReadbackErrorSink<'_> = Some(&record);
            let readback = async {
                match &op {
                    BulkPipelineOp::SetFlags(flag_op) => {
                        crate::mutation::readback::run_readback_guard_observed(
                            account.as_ref().as_ref(),
                            readback_ids,
                            flag_op,
                            sink,
                        )
                        .await
                    }
                    BulkPipelineOp::Move { destination, .. } => {
                        crate::mutation::readback::run_move_readback_guard_observed(
                            account.as_ref().as_ref(),
                            readback_ids,
                            destination,
                            sink,
                        )
                        .await
                    }
                    BulkPipelineOp::Destroy => {
                        crate::mutation::readback::run_destroy_readback_guard_observed(
                            account.as_ref().as_ref(),
                            readback_ids,
                            sink,
                        )
                        .await
                    }
                }
            };
            let outcome = tokio::select! {
                () = slot.shutdown.cancelled() => return Err(Error::ShuttingDown),
                outcome = readback => match outcome {
                    Ok(outcome) => outcome,
                    Err(Error::Account(error)) => {
                        // A stream-level read-back termination is returned,
                        // not absorbed; record its hold before surfacing it.
                        record_error_throttle(&slot.throttles, account_id, &error);
                        return Err(Error::Account(error));
                    }
                    Err(other) => return Err(other),
                },
            };
            totals.pending_retry = totals
                .pending_retry
                .saturating_sub(outcome.skipped)
                .saturating_sub(outcome.still_failed);
            totals.skipped = totals.skipped.saturating_add(outcome.skipped);
            totals.failed_terminal = totals.failed_terminal.saturating_add(outcome.still_failed);
        }
        Ok(totals)
    }
}

/// Which bulk mutation a [`SyncEngine::run_bulk_pipeline`] run drives.
///
/// Selects the two op-specific seams in the shared pipeline: the wire
/// submit call and the
/// matching read-back guard (membership vs absence). Everything else in
/// the idempotency / retry / recovery loop is identical to
/// operation-specific read-back guard.
enum BulkPipelineOp {
    /// Apply `op` through `Account::bulk_set_flags`.
    SetFlags(bifrost_types::FlagOp),
    /// Route every target into `destination` (and, when the consumer
    /// knows it, out of `source`) via `Account::bulk_move_from`.
    Move {
        destination: MembershipScope,
        source: Option<MembershipScope>,
    },
    /// Destroy every target via `Account::bulk_destroy`.
    Destroy,
}

/// The throttle keys one campaign attempt consumes: every target being
/// submitted, plus a move's destination and supplied source - a move
/// spends the budget of the containers it touches, which need not be the
/// targets' own.
fn campaign_keys(
    account: &dyn Account,
    account_id: &AccountId,
    targets: &[bifrost_types::ObjectId],
    op: &BulkPipelineOp,
) -> Vec<bifrost_types::ThrottleKey> {
    use bifrost_types::ThrottleWork;
    let mut keys =
        crate::recovery::declared_keys(account, account_id, ThrottleWork::Objects(targets));
    if let BulkPipelineOp::Move {
        destination,
        source,
    } = op
    {
        keys.extend(crate::recovery::declared_keys(
            account,
            account_id,
            ThrottleWork::Membership(destination),
        ));
        if let Some(source) = source {
            keys.extend(crate::recovery::declared_keys(
                account,
                account_id,
                ThrottleWork::Membership(source),
            ));
        }
    }
    keys
}

/// Record the throttle hold any retryable or reconcilable error carries,
/// whatever lane observed it.
fn record_error_throttle(
    throttles: &std::sync::Mutex<crate::recovery::ThrottleBucket>,
    account_id: &AccountId,
    error: &AccountError,
) {
    match error.recovery() {
        bifrost_types::RecoveryClass::Retry(advice) => {
            crate::recovery::record_throttle(throttles, account_id, advice, error);
        }
        bifrost_types::RecoveryClass::Reconcile(advice) => {
            crate::recovery::record_reconcile_throttle(throttles, account_id, advice, error);
        }
        // Engine directives and terminal classes carry no throttle hold.
        _ => {}
    }
}

/// Per-id mutation bookkeeping bucket. Reflects the campaign's final
/// resolution for each id; `PendingRetry` items get the read-back
/// guard before counters are reported back to the consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MutationBucket {
    Applied,
    Skipped,
    FailedTerminal,
    PendingRetry,
    PendingReadback,
    BlockedByEngine,
}

/// Add every retry-eligible target to the retry queue after a
/// retryable stream termination.
///
/// A stream may terminate before it emits an item outcome for every
/// submitted target. Those unseen ids are still part of the campaign
/// and must be retried alongside explicit per-item retry failures;
/// without this sweep they are neither resubmitted, read back, nor
/// counted, and the campaign reports success for work that never
/// happened.
///
/// Deliberately narrower than the `Reconcile` / `Engine` termination
/// sweeps, which claim every id that is not already resolved. An id
/// sitting in `PendingReadback` is owned by the read-back guard -
/// that lane exists precisely so a write whose first attempt may have
/// landed is verified rather than replayed - and a `BlockedByEngine`
/// id is waiting on an engine directive. Neither is ours to resubmit,
/// so only ids the stream never resolved (`None`) or explicitly asked
/// us to retry (`PendingRetry`) are queued.
pub(super) fn queue_unresolved_for_retry(
    remaining: &[bifrost_types::ObjectId],
    outcomes: &HashMap<bifrost_types::ObjectId, MutationBucket>,
    retry_ids: &mut Vec<bifrost_types::ObjectId>,
) {
    // Set-based dedupe: `remaining` and `retry_ids` both scale with the
    // campaign, so a linear scan per candidate is quadratic on the
    // exact path (a large bulk that trips a rate limit) where it hurts.
    let mut queued: HashSet<bifrost_types::ObjectId> = retry_ids.iter().cloned().collect();
    for id in remaining {
        let eligible = matches!(outcomes.get(id), None | Some(MutationBucket::PendingRetry));
        if eligible && queued.insert(id.clone()) {
            retry_ids.push(id.clone());
        }
    }
}

/// Every id whose campaign outcome is still unresolved once the retry
/// loop ends, in no particular order.
///
/// The read-back queue is derived from `outcomes` rather than
/// accumulated during the loop: the map is the single source of truth
/// for per-id state, it already survives across attempts, and deriving
/// it means an id parked in the read-back lane cannot be lost when a
/// later attempt resubmits its siblings.
pub(super) fn unresolved_readback_ids(
    outcomes: &HashMap<bifrost_types::ObjectId, MutationBucket>,
) -> Vec<bifrost_types::ObjectId> {
    let mut ids = Vec::new();
    for (id, bucket) in outcomes {
        if matches!(
            bucket,
            MutationBucket::PendingRetry | MutationBucket::PendingReadback
        ) {
            ids.push(id.clone());
        }
    }
    ids
}

/// Keep whichever advice asks for the LONGER wait.
///
/// A batch can carry several retryable failures with different hints; the
/// campaign resubmits them together, so honoring the shortest would resubmit
/// an id whose provider named a later deadline. Ties keep the incumbent.
fn keep_longer_advice(current: &mut Option<RetryAdvice>, candidate: RetryAdvice) {
    let now = std::time::SystemTime::now();
    let fallback = Duration::from_secs(1);
    let candidate_delay = crate::recovery::retry_delay(&candidate, now, fallback);
    match current {
        Some(existing)
            if crate::recovery::retry_delay(existing, now, fallback) >= candidate_delay => {}
        _ => *current = Some(candidate),
    }
}

/// Classify one `ItemOutcome<MutationSuccess>` and update the
/// per-id outcome map and retry queue.
///
/// Read-back membership is not a separate queue: `PendingReadback`
/// (and any `PendingRetry` still unresolved when the loop ends) IS the
/// read-back lane, collected by `unresolved_readback_ids`.
///
/// Dispatch rules:
/// - `Succeeded(Applied)` -> `Applied`.
/// - `Succeeded(Skipped)` -> `Skipped`.
/// - `Failed { error }` dispatches via `error.recovery()`:
///   - `Retry::SameRequest` / `AfterAuthRefresh` -> retry queue.
///   - `Retry::AfterStateRefresh` and `Reconcile(_)` -> read-back.
///   - `Engine(_)` -> `BlockedByEngine` and returns the original
///     structured error plus directive target for the reopen listener.
///   - terminal -> `FailedTerminal`.
/// - `Uncertain { error }` always -> read-back, even if the carried
///   recovery says retryable; the uncertainty lane exists precisely
///   to avoid blindly replaying writes whose first attempt may have
///   landed.
pub(super) fn classify_item_outcome(
    item: ItemOutcome<MutationSuccess>,
    outcomes: &mut HashMap<bifrost_types::ObjectId, MutationBucket>,
    retry_ids: &mut Vec<bifrost_types::ObjectId>,
    dedupe_count: &mut u64,
    throttles: &std::sync::Mutex<crate::recovery::ThrottleBucket>,
    account_id: &AccountId,
    // The retry advice of the per-item failures that were QUEUED for
    // resubmission, folded to the longest delay. A stream that ends `Done`
    // after emitting per-item retryable failures carries no stream-level
    // advice, and without this the campaign looped straight into its next
    // attempt with no delay at all - up to `mutation_max_retries`
    // back-to-back resubmissions against a provider that just said no.
    item_retry_advice: &mut Option<RetryAdvice>,
) -> Option<(EngineDirective, AccountError)> {
    use crate::recovery::{RecoveryPlan, plan_recovery};
    match item {
        ItemOutcome::Succeeded(success) => {
            let id = bifrost_types::ObjectId(success.item.0);
            let bucket = match success.output {
                MutationSuccess::Applied => MutationBucket::Applied,
                MutationSuccess::Skipped => MutationBucket::Skipped,
                // The provider did something weaker than asked, so its claim
                // is exactly what must not be trusted. `PendingReadback` puts
                // the id in the read-back set (`unresolved_readback_ids`)
                // WITHOUT queueing a resubmission - a downgrade is not a
                // transient failure and replaying it just earns the same
                // downgrade - so the final accounting comes from observed
                // state. For the motivating case, a Gmail message trashed
                // instead of destroyed still hydrates, so the guard files it
                // `still_failed`: honest, and it breaks the permanent
                // destroy/reappear reconcile loop that reporting `Applied`
                // created.
                MutationSuccess::Downgraded { .. } => MutationBucket::PendingReadback,
                // `MutationSuccess` is #[non_exhaustive]. A new variant must
                // be classified deliberately, not folded into `Applied` - that
                // is how a downgrade got reported as a clean success in the
                // first place.
                _ => MutationBucket::PendingReadback,
            };
            outcomes.insert(id, bucket);
            None
        }
        ItemOutcome::Failed(failure) => {
            let id = bifrost_types::ObjectId(failure.item.0);
            let original = failure.error.clone();
            match plan_recovery(failure.error) {
                RecoveryPlan::Retry(advice) => {
                    // A per-item 429 (Graph files them per `$batch`
                    // subresponse) carries the same shared deadline a
                    // stream-level one would; record it so polls and
                    // sibling accounts observe it too.
                    crate::recovery::record_throttle(throttles, account_id, &advice, &original);
                    match advice.disposition {
                        bifrost_types::RetryDisposition::AfterStateRefresh => {
                            outcomes.insert(id, MutationBucket::PendingReadback);
                            None
                        }
                        bifrost_types::RetryDisposition::SameRequest
                        | bifrost_types::RetryDisposition::AfterAuthRefresh => {
                            keep_longer_advice(item_retry_advice, advice);
                            retry_ids.push(id.clone());
                            outcomes.insert(id, MutationBucket::PendingRetry);
                            None
                        }
                        // RetryDisposition is #[non_exhaustive]; new variants
                        // default to read-back (the safe path) and require
                        // explicit handling here when added.
                        _ => {
                            outcomes.insert(id, MutationBucket::PendingReadback);
                            None
                        }
                    }
                }
                RecoveryPlan::Reconcile(advice) => {
                    crate::recovery::record_reconcile_throttle(
                        throttles, account_id, &advice, &original,
                    );
                    for action in &advice.guidance.actions {
                        if matches!(action, ReconcileAction::DedupeByClientId) {
                            *dedupe_count = dedupe_count.saturating_add(1);
                        }
                    }
                    // Both reconcile shapes land in the same lane.
                    // `CheckTarget` asks for exactly what the read-back
                    // guard performs; without it the consumer must
                    // dedupe before any retry, so the engine cannot
                    // resolve the item automatically either. Either way
                    // pending-readback is the honest state and surfaces
                    // the id through the counters.
                    //
                    // The unconditional read-back here is correct rather
                    // than an over-reach, and that rests on a PRODUCER
                    // invariant rather than on anything this crate can
                    // check: every `ReconcileAdvice` that can actually be
                    // produced carries `CheckTarget`, because `try_build`
                    // always routes through `recovery::derive` and both of
                    // derive's `Reconcile` arms include it - a
                    // `DedupeByClientId` without `CheckTarget` cannot be
                    // minted. That invariant is pinned in `bifrost-types`
                    // by `every_producible_reconcile_requests_check_target`;
                    // if that test ever goes away or starts to fail, this
                    // arm has to grow a `CheckTarget` branch.
                    //
                    // Known coverage limit, accepted: there is no
                    // engine-half test driving a real failing
                    // non-idempotent mutation through here and observing
                    // both the read-back queue and the warning. It would
                    // pin behaviour that is correct BECAUSE of the
                    // producer invariant already pinned, so it earns
                    // little on its own; worth adding only if the
                    // mutation-loop harness grows for some other reason.
                    outcomes.insert(id, MutationBucket::PendingReadback);
                    None
                }
                RecoveryPlan::Engine(directive) => {
                    outcomes.insert(id, MutationBucket::BlockedByEngine);
                    Some((directive, original))
                }
                RecoveryPlan::Terminal(_) => {
                    outcomes.insert(id, MutationBucket::FailedTerminal);
                    None
                }
            }
        }
        ItemOutcome::Uncertain(uncertain) => {
            let id = bifrost_types::ObjectId(uncertain.item.0);
            outcomes.insert(id, MutationBucket::PendingReadback);
            None
        }
    }
}

/// Campaign-scoped dedupe identity for an engine directive: the directive's
/// own variant plus the scope it targets.
///
/// Keying on the target scope alone is wrong in both directions. Every
/// account-wide directive would collapse onto `None`, so the first
/// `RestartAccount` in a mixed batch would suppress a later
/// `OperatorOverrideRequired` or `SchemaIncompatible`; and `RestartScope`,
/// `DowngradeCapabilityForScope` and `DisableScope` on one folder would
/// collapse onto each other even though they ask the engine for three
/// different things.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum DirectiveKey {
    RestartScope(CursorScope),
    RestartAccount,
    DowngradeStrategy,
    DowngradeCapabilityForScope(CursorScope),
    SchemaIncompatible,
    OperatorOverrideRequired,
    DisableScope(CursorScope),
    /// `EngineDirective` is `#[non_exhaustive]`; an unnamed future variant
    /// dedupes by its resolved target only, which is never coarser than the
    /// old behavior.
    Other(Option<CursorScope>),
}

fn directive_key(directive: &EngineDirective) -> DirectiveKey {
    match directive {
        EngineDirective::RestartScope(scope) => DirectiveKey::RestartScope(scope.clone()),
        EngineDirective::RestartAccount => DirectiveKey::RestartAccount,
        EngineDirective::DowngradeStrategy(_) => DirectiveKey::DowngradeStrategy,
        EngineDirective::DowngradeCapabilityForScope(scope) => {
            DirectiveKey::DowngradeCapabilityForScope(scope.clone())
        }
        EngineDirective::SchemaIncompatible => DirectiveKey::SchemaIncompatible,
        EngineDirective::OperatorOverrideRequired { .. } => DirectiveKey::OperatorOverrideRequired,
        EngineDirective::DisableScope(scope) => DirectiveKey::DisableScope(scope.clone()),
        other => DirectiveKey::Other(crate::recovery::directive_target_scope(other)),
    }
}

/// Return whether this campaign has not yet forwarded this exact directive.
/// A per-item engine failure can name the same directive hundreds of times;
/// the reopen listener needs one request per distinct directive, not one per
/// failed object - and not one per campaign either.
pub(super) fn should_forward_engine_recovery(
    forwarded: &mut HashSet<DirectiveKey>,
    directive: &EngineDirective,
) -> bool {
    forwarded.insert(directive_key(directive))
}

fn counters_from_outcomes(
    outcomes: &HashMap<bifrost_types::ObjectId, MutationBucket>,
) -> crate::mutation::MutationCounters {
    let mut counters = crate::mutation::MutationCounters::default();
    for bucket in outcomes.values() {
        match bucket {
            MutationBucket::Applied => counters.record_applied(),
            MutationBucket::Skipped => counters.record_skipped(),
            MutationBucket::FailedTerminal => counters.record_failed(),
            MutationBucket::BlockedByEngine => counters.record_blocked_by_engine(),
            MutationBucket::PendingRetry | MutationBucket::PendingReadback => {
                counters.record_pending();
            }
        }
    }
    counters
}
