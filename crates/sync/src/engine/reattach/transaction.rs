//! Replacement open, staged cutover and account restart.

use super::*;

/// Why no replacement connection was opened.
pub(in crate::engine) enum ReplacementOpen {
    /// The boundary left `Run` before activity could be registered. Nothing
    /// was opened, so there is nothing to close.
    Paused,
    /// The slot is being torn down: its shutdown token was cancelled either
    /// before the open started or while it was in flight. A replacement opened
    /// inside that window has no owner - `detach` has already closed the handle
    /// it knew about and removed the slot - so `open_replacement` closes the
    /// replacement itself and reports this rather than swapping it into an
    /// orphaned slot, where nothing would ever close it.
    Detached,
    Failed(AccountError),
}

/// Open a replacement connection with the account's activity registration
/// already held.
///
/// The guard must span the open itself, not just the reattach that follows
/// it. `pause` reports quiescence as soon as the active count reaches zero,
/// so registering after `factory.open()` returns would let a pause observe an
/// idle account while a replacement connection is being established in the
/// background - the opposite of what the quiescence contract promises. The
/// guard is returned so the caller can hand it to `reattach_account` and keep
/// the whole reopen inside one activity registration.
///
/// Both consumer-driven reattach and restart register activity, take the
/// reopen lock, then take scheduler budget, matching scoped recovery's lock
/// order. A hold found before or after queued admission releases all those
/// owners before waiting. Neither retains the lock while waiting for resume.
pub(in crate::engine) async fn open_replacement(
    ctx: &RecoveryContext<'_>,
) -> Result<(OwnedMutexGuard<()>, SyncActivityGuard, Replacement), ReplacementOpen> {
    let gate = crate::engine::drive_start::DriveStart {
        current: &ctx.current,
        control: &ctx.control,
        scheduler: &ctx.scheduler,
        throttles: &ctx.throttles,
        account_id: &ctx.account_id,
        shutdown: &ctx.shutdown,
    };
    loop {
        let activity = ctx
            .control
            .begin_activity()
            .ok_or(ReplacementOpen::Paused)?;
        let mut boundary = ctx.control.drive_boundary();
        if boundary.peek() != BoundaryRequest::Run {
            return Err(ReplacementOpen::Paused);
        }
        let reopen_guard = tokio::select! {
            biased;
            () = ctx.shutdown.cancelled() => return Err(ReplacementOpen::Detached),
            _ = boundary.changed() => return Err(ReplacementOpen::Paused),
            guard = Arc::clone(&ctx.reopen_lock).lock_owned() => guard,
        };
        // Lock before budget, matching scope recovery. A factory open has no
        // scope declaration yet; establishment asks the replacement itself.
        let admitted = match gate
            .admit_recovery(crate::scheduler::WorkKind::Sync, None, |_| Vec::new())
            .await
        {
            Ok(admitted) => admitted,
            Err(crate::engine::drive_start::StartRefused::Paused) => {
                return Err(ReplacementOpen::Paused);
            }
            Err(crate::engine::drive_start::StartRefused::ShuttingDown) => {
                return Err(ReplacementOpen::Detached);
            }
            Err(crate::engine::drive_start::StartRefused::Held { until, .. }) => {
                drop(reopen_guard);
                drop(activity);
                tokio::select! {
                    () = ctx.shutdown.cancelled() => return Err(ReplacementOpen::Detached),
                    () = tokio::time::sleep_until(until) => {}
                }
                continue;
            }
        };
        drop(activity);
        let result = open_replacement_admitted(ctx, reopen_guard, admitted.activity).await;
        drop(admitted.permit);
        return result;
    }
}

pub(in crate::engine) async fn open_replacement_admitted(
    ctx: &RecoveryContext<'_>,
    reopen_guard: OwnedMutexGuard<()>,
    activity: SyncActivityGuard,
) -> Result<(OwnedMutexGuard<()>, SyncActivityGuard, Replacement), ReplacementOpen> {
    // Detach does not wait for consumer-driven activity, so nothing excludes a
    // `SyncEngine::reattach` that registered its activity just before detach
    // flipped the boundary to `Stop`. The slot's shutdown token is the one
    // thing that observes the teardown from here, so it is consulted on both
    // sides of the open: before, to avoid spending a connection at all, and
    // after, because detach can remove the slot and close the old handle while
    // the open is in flight. A reattach that then completes without touching
    // the closed writer channel would swap the replacement into an orphaned
    // slot and close the already-closed previous handle, leaking the
    // replacement connection for the life of the process.
    if ctx.shutdown.is_cancelled() {
        return Err(ReplacementOpen::Detached);
    }
    if let Some(budget) = ctx.bandwidth_budget.as_ref() {
        ctx.control.apply_bandwidth_budget(budget);
    }
    match ctx.factory.open(ctx.account_id.clone()).await {
        Ok(next) => {
            // Owned by the guard from this synchronous step on, so nothing
            // below, and nothing between here and `reattach_account`'s first
            // poll, can drop the connection without closing it.
            let OpenedAccount {
                account,
                skipped_scopes,
            } = next;
            let mut guard = ReattachGuard::new(ctx, account);
            if ctx.shutdown.is_cancelled() {
                // Close what we opened. Best-effort, exactly like every other
                // abort path here: the alternative is an owner-less connection.
                // Through the guard, so a drop parked in this close owes it
                // again rather than abandoning it.
                let _ = guard.close_owed().await;
                return Err(ReplacementOpen::Detached);
            }
            Ok((
                reopen_guard,
                activity,
                Replacement {
                    guard,
                    skipped_scopes,
                },
            ))
        }
        // Dropping `activity` here is the point: the failed open registered
        // no lasting work, so quiescence must not stay blocked on it.
        Err(error) => Err(ReplacementOpen::Failed(error)),
    }
}

/// A freshly opened replacement connection and the guard that owns it.
///
/// The guard is part of the value rather than built by the consumer, so a
/// `Replacement` dropped anywhere, before `reattach_account` is polled included,
/// closes the connection it holds. See [`ReattachGuard`].
pub(in crate::engine) struct Replacement {
    guard: ReattachGuard,
    skipped_scopes: Vec<SkippedScope>,
}

/// Swap in a replacement connection. `activity` is the registration taken
/// before the connection was opened; holding it here keeps
/// the open and the swap inside one uninterrupted non-quiescent window.
pub(in crate::engine) async fn reattach_account(
    ctx: &RecoveryContext<'_>,
    activity: SyncActivityGuard,
    replacement: Replacement,
) -> Result<(), Error> {
    reattach_account_paced(ctx, activity, replacement, &mut RepairPacing::default()).await
}

pub(in crate::engine) async fn reattach_account_paced(
    ctx: &RecoveryContext<'_>,
    activity: SyncActivityGuard,
    replacement: Replacement,
    pacing: &mut RepairPacing,
) -> Result<(), Error> {
    pacing.local_deadline = None;
    pacing.failed_keys.clear();
    pacing.observed = false;
    let admitted = ctx.control.admitted_by(&activity);
    let _activity = activity;
    let Replacement {
        mut guard,
        skipped_scopes: next_skips,
    } = replacement;
    let next = Arc::clone(&guard.account);
    ctx.control.apply_to(next.as_ref(), None);

    let result = async {
        let discovered = discover_scopes_from(next.as_ref()).await?;
        let staged = Arc::new(CursorRegistry::new());
        // The replacement's discovery is the account model from the cutover
        // on, including scopes left without a cursor below.
        staged.note_discovered(&discovered);
        let mut newly_established = Vec::new();

        for scope in &discovered {
            if let Some(existing) = ctx.cursors.snapshot(scope) {
                staged.put(existing);
                continue;
            }
            // A scope absent from the live registry may still have a durable
            // cursor - a prior session persisted it, this session's account
            // never discovered it, and the replacement discovers it again.
            // `run_establish` resumes from that row without writing, and
            // reports the origin itself so a resumed row can never be
            // counted as created here - an aborted swap would otherwise
            // delete a legitimately persisted cursor. The origin comes from
            // run_establish's own single store read, not a separate
            // pre-check: a pre-check both races that read and, if it
            // swallowed a store error as "no row", would misclassify a
            // preexisting row as freshly created.
            //
            // The resume read claims a still-provisional row for this attempt
            // (see `WriterRequest::GetChangeCursor`), so the abort or commit
            // owed for it is armed BEFORE the read, synchronously: a drop
            // parked inside `run_establish` after the claim must still settle
            // the row, or it would stay owned by an attempt that never
            // settles it. An abort with nothing owned is a no-op.
            guard.rollback_owed = true;
            let walk_fence = ctx.coverage.scope_fence(scope);
            let result = run_establish(
                &ctx.account_id,
                next.as_ref(),
                scope.clone(),
                Arc::clone(&staged),
                ctx.writer,
                Arc::clone(&ctx.delivery),
                &admitted,
                Arc::clone(&ctx.coverage),
                false,
                &ctx.shutdown,
                Some(guard.attempt),
                ctx,
                pacing,
                walk_fence,
            )
            .await;
            match (reattach_scope_step(&result), result) {
                (_, Ok(EstablishOrigin::ResumedStored)) => {}
                (_, Ok(EstablishOrigin::CreatedFresh)) => {
                    if let Some(cursor) = staged.snapshot(scope) {
                        newly_established.push(cursor);
                    }
                }
                (ReattachScopeStep::Skip, Err(error)) => {
                    tracing::warn!(
                        target: "bifrost.sync.reopen",
                        account = ?&ctx.account_id,
                        scope = ?scope,
                        error = %error,
                        "scope-local establishment failure or superseded scope during reopen; \
                         skipping scope"
                    );
                }
                (_, Err(error)) => return Err(error),
            }
        }

        pacing.observed = false;
        pacing.failed_keys.clear();
        link_discovered_memberships(next.as_ref(), &staged).await?;

        let discovered_set: HashSet<_> = discovered.iter().cloned().collect();
        let vanished: Vec<_> = ctx
            .cursors
            .all_scopes()
            .into_iter()
            .filter(|scope| !discovered_set.contains(scope))
            .collect();

        let previous_subscriptions = ctx.subscriptions.snapshot();
        let next_has_push = !bifrost_types::effective_push_lanes(next.as_ref()).is_empty();
        for record in previous_subscriptions.iter().filter(|record| {
            // Recreate what the consumer still WANTS, whatever the state of
            // the old handle's teardown. An orphan (not `desired`) is carried
            // for retry only; recreating it would double-subscribe the
            // account. But a `desired` record may ALSO be
            // `teardown_unconfirmed`: an earlier attempt failed to tear its
            // old handle down and aborted, leaving the old account installed
            // and the subscription live and wanted. Filtering on the teardown
            // flag instead - as this once did - dropped exactly that record on
            // the retry: its old handle was torn down or carried as an orphan,
            // nothing replaced it, and the consumer's push coverage vanished
            // without an error.
            record.desired() && next_has_push
        }) {
            let scopes: Vec<CursorScope> = record
                .scopes
                .iter()
                .filter(|scope| discovered_set.contains(*scope))
                .cloned()
                .collect();
            if scopes.is_empty() {
                // The requested topology vanished. Do not silently widen a
                // scoped subscription to every discovered scope; its old
                // handle is explicitly torn down below, and the consumer can
                // opt in again against the replacement topology.
                continue;
            }
            match next.push_subscribe(&scopes).await {
                Ok(result) => {
                    let covered = accepted_push_scopes(&result);
                    if let Some(handle) = result.handle().cloned() {
                        // Straight into the guard: no await between the provider
                        // answering and the handle being owned by something a
                        // drop will clean up.
                        guard.push(RegisteredSubscription {
                            handle,
                            scopes: covered,
                            state: crate::push::subscription::SubscriptionState::Live,
                        });
                    }
                }
                Err(error) => {
                    pacing.observe(ctx, &error);
                    unwind_replacement_subscriptions(ctx, next.as_ref(), &mut guard, pacing).await;
                    return Err(Error::Account(error));
                }
            }
        }

        // Persist the freshly created cursors while the old account and its
        // push subscriptions are still completely intact. Only rows whose
        // scope had no stored cursor before this reattach are written here,
        // so the abort path below can compensate by plain deletion without
        // ever touching preexisting durable state. Vanished-scope rows are
        // deliberately NOT deleted yet: the old account's ack writer may
        // still be persisting checkpoints for them, and a delete now would
        // need a snapshot-and-restore on abort that races that writer.
        // Their deletion happens after the cutover, where an abort can no
        // longer occur.
        // Through the account's single writer, NOT `ctx.store` directly. A
        // direct write races the ack writer, which is concurrently persisting
        // acknowledged checkpoints - including ones this very reattach caused,
        // because `run_establish` hands `changes_tx` to `InventoryFusion` and a
        // replacement inventory pass broadcasts checkpoint-bearing batches
        // before the cutover. The writer also marks these scopes provisional so
        // an abort deletes only rows no acknowledgement has claimed.
        //
        // The compensation is owed from BEFORE the first insert, synchronously,
        // so a drop parked inside an insert (whose request may already be
        // queued) still rolls back.
        if !newly_established.is_empty() {
            guard.rollback_owed = true;
        }
        let attempt = guard.attempt;
        let durable_result = async {
            for cursor in &newly_established {
                ctx.writer.reattach_insert(attempt, cursor.clone()).await?;
            }
            Ok::<(), Error>(())
        }
        .await;
        if let Err(error) = durable_result {
            rollback_reattach_inserts(ctx, &mut guard).await;
            unwind_replacement_subscriptions(ctx, next.as_ref(), &mut guard, pacing).await;
            return Err(error);
        }

        // Accounts such as Graph retain server-side subscription ids after a
        // failed delete so the same handle can retry. Tear old handles down
        // before swapping and closing their account: otherwise a vanished
        // record would lose the only retry path and leak a server subscription.
        let previous = ctx.current.load_full();
        let mut carried_unconfirmed = Vec::new();
        // The first refusal of a handle not yet known to be unreachable. The
        // pass does NOT stop at it: every remaining record is still torn down
        // and every refusal is flagged, so the retry meets no first-time
        // failure and the whole reopen costs at most one aborted attempt no
        // matter how many subscriptions the consumer holds. Stopping at the
        // first refusal made N failing records cost N + 1 attempts, and the
        // account-wide retry budget is three.
        //
        // Accepted gap: a handle deleted here stops delivering at once, and if a
        // later refusal aborts the pass its record stays `torn_down` until the
        // retry recreates it on the replacement, so that subscription delivers
        // nothing for the span between the two attempts. Closing it by
        // resubscribing on the old account was rejected: an abort here usually
        // means the old connection is failing, so the resubscribe would
        // likely fail too, and a success would mint a handle the retry must
        // then tear down again, moving the hole instead of closing it. The cost
        // is bounded, and the old account stays installed and syncing through
        // its ordinary change streams until the retry commits. If the budget is
        // exhausted the account pauses and the records stay `torn_down` and
        // `desired`, recreated by whichever reopen next commits.
        let mut first_refusal: Option<AccountError> = None;
        for record in &previous_subscriptions {
            if record.torn_down() {
                // An earlier attempt already deleted it server-side and then
                // aborted; the record is only the consumer's desire, which the
                // recreation loop above has honoured. Deleting it again is
                // an unknown-handle error on a strict provider.
                continue;
            }
            // Accepted limit: a carried orphan is retried here against whatever
            // account is current, which may be a later connection than the one
            // that minted the handle. The handle belongs to the same account id
            // and credentials, and no provider in this workspace rejects a
            // handle it does not know, so the retry clears the orphan rather
            // than failing forever. It does not promise a deletion: IMAP and
            // JMAP hold a handle's server state in the account instance, so a
            // retry that reaches a newer connection is a no-op and the
            // provider-side subscription then lives until it expires. Graph's
            // webhook handles carry their subscription ids, so a retry there
            // does delete (and its rows are also deleted by the owning
            // account's `close()`). `unsubscribe_push` retries on the same
            // terms.
            let service = ctx.subscriptions.teardown(&ctx.account_id, &ctx.throttles);
            let teardown = service
                .delete(previous.as_ref().as_ref(), &record.handle)
                .await;
            if let Err(refusal) = &teardown {
                pacing.observed = true;
                pacing.local_deadline = pacing.local_deadline.max(refusal.local_deadline);
            }
            service.settle_registered(record, &teardown);
            match teardown {
                Ok(()) => {}
                Err(refusal) if record.teardown_unconfirmed() => {
                    // Its teardown already failed once. The handle may belong
                    // to a connection that is long gone, so a repeated failure
                    // must not block the swap; keep carrying it so the next
                    // reopen or an `unsubscribe_push` call can try again.
                    //
                    // Carried as an ORPHAN even when it was `desired`: the loop
                    // above has already recreated a desired record on the
                    // replacement, and that new record is where the desire
                    // lives now (or deliberately dropped it, when its scopes
                    // vanished or the replacement has no push, exactly as for
                    // a record with a confirmed teardown). Carrying the desire
                    // here as well would make the next reopen subscribe twice.
                    tracing::warn!(
                        target: "bifrost.sync.reopen",
                        account = ?&ctx.account_id,
                        error = %refusal.error,
                        "carrying push subscription whose teardown is still unconfirmed"
                    );
                    carried_unconfirmed.push(record.clone().into_orphan());
                }
                Err(refusal) => {
                    // Still live and wanted on the old account, which stays
                    // installed if this pass aborts: flag it so the retry
                    // carries it instead of aborting on it again.
                    first_refusal.get_or_insert(refusal.error);
                }
            }
        }
        if let Some(error) = first_refusal {
            // The replacement's own subscriptions must not be stranded by this
            // unwind: `Account::close()` deliberately does not delete
            // server-side subscriptions, so any handle whose teardown did not
            // succeed stays in the registry - on whichever side it was
            // created - and is retried later.
            unwind_replacement_subscriptions(ctx, next.as_ref(), &mut guard, pacing).await;
            rollback_reattach_inserts(ctx, &mut guard).await;
            return Err(Error::Account(error));
        }
        // Commit exit of the replacement-subscription guard. Nothing between
        // here and `subscriptions.replace` awaits, so no drop can land after the
        // guard lets go and before the registry holds the handles.
        let mut installed_subscriptions = guard.disarm();
        installed_subscriptions.extend(carried_unconfirmed);

        // Take the final old-handle snapshot immediately before cutover.
        // replace_from advances the registry generation atomically with the
        // topology swap, so an old stream that finishes later cannot write a
        // cursor minted by the retired connection into this new topology.
        // The replacement is installed together with its generation, so a
        // worker that loads it can never pair it with the retiring one.
        let installed_generation = ctx.current.load().generation().successor();
        let previous = ctx.current.swap(Arc::new(crate::types::Incarnation::new(
            Arc::clone(&next),
            installed_generation,
        )));
        // No await between the swap and this: from here a drop must close the
        // OLD account (the replacement is live) and promote the provisional
        // rows instead of rolling them back.
        guard.swapped(Arc::clone(previous.account()));
        ctx.cursors.replace_topology_preserving_cursors(&staged);
        {
            let mut capabilities = match ctx.capabilities.write() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            // A mail user expects account affordances to follow renegotiation.
            // Only committed cutover has both authoritative snapshots.
            let delta = bifrost_types::CapabilityDelta::between(&capabilities, next.capabilities());
            *capabilities = next.capabilities().clone();
            if !delta.is_empty() {
                ctx.control
                    .publish_control(AccountControl::CapabilitiesChanged(delta));
            }
        }
        // Sealed against detach like every registry write (see
        // `SubscriptionRegistry::replace`). A refusal means detach cancelled the
        // slot after this reattach's last shutdown check and has taken, or will
        // take, the registry: the replacement's handles are live on the account
        // that just became current and nothing will ever hold them. They go
        // straight back into the guard, in the same synchronous step, so a drop
        // from here on still deletes them; the unwind that settles them runs at
        // the end, after every step that publishes the cutover. The carried
        // orphans name the PREVIOUS connection's handles and are dropped as detach
        // drops stranded records, which its own take logs.
        //
        // Accepted inconsistency (ruled 2026-10-04): plain detach deliberately
        // leaves server-side subscriptions alive for queue-for-later delivery,
        // and this race deletes the consumer's. It differs only for a provider
        // whose subscription outlives `Account::close()` (Graph's close deletes
        // its webhook rows, JMAP and IMAP push die with the connection), and
        // the alternative is live subscriptions no handle anywhere names: the
        // old handles were already torn down above. The reattach still returns
        // `Ok`, since the swap committed before detach ended the account.
        let mut refused_install = 0_usize;
        if let Err(refused) = ctx.subscriptions.replace(installed_subscriptions) {
            for record in refused {
                if record.desired() {
                    guard.push(record);
                } else {
                    refused_install += 1;
                }
            }
        }
        // The replacement open's skip lane supersedes the previous
        // one: a healed namespace disappears from it, a still-degraded
        // one reappears with a fresh classification.
        log_open_skips(&ctx.account_id, &next_skips, "reopen");
        {
            let mut skips = ctx
                .open_skips
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *skips = next_skips;
        }
        let retiring_generation = previous.generation();
        ctx.account_generation_tx.send_replace(installed_generation);

        // Past the last abort point, so the rows this reattach inserted are now
        // ordinary durable state. Leaving them marked provisional would let a
        // LATER reattach's abort delete cursors belonging to this one, which
        // succeeded. Deliberately after the generation bump: this is a channel
        // send, and the lifecycle reader is waiting on that watch to resubscribe
        // to the replacement handle - no await belongs between the topology swap
        // and the bump that publishes it.
        commit_reattach_inserts(ctx, &mut guard).await;

        // Delete vanished-scope rows only now that the cutover is committed.
        // Nothing after this point can abort the swap, so no compensation is
        // needed; a failed (or raced) delete merely leaks a stale row for a
        // scope no longer in the topology, and a later rediscovery of that
        // scope resumes from it, which is valid. A retired stream's late ack
        // can likewise re-persist such a row after this delete - the same
        // benign leak, never data loss.
        for scope in &vanished {
            if let Err(error) = ctx.writer.reset_scope_for_disable(scope.clone()).await {
                tracing::warn!(
                    target: "bifrost.sync.reopen",
                    account = ?&ctx.account_id,
                    scope = ?scope,
                    error = %error,
                    "failed to delete vanished-scope cursor after reopen cutover"
                );
            }
        }

        // A refused install (detach won the race for the registry) left the
        // replacement's handles in the guard. Tear them down now that nothing
        // publishing the cutover is left to delay. The reattach still reports
        // success: the swap committed and the replacement is the live account;
        // it is the registration that was refused, and a failure here would send
        // the caller's retry budget after a swap that cannot be undone.
        if !guard.live.is_empty() || refused_install > 0 {
            tracing::warn!(
                target: "bifrost.sync.reopen",
                account = ?&ctx.account_id,
                handles = guard.live.len(),
                carried_orphans = refused_install,
                "slot detached during reattach; tearing down the replacement's push \
                 subscriptions, which no registry entry can hold"
            );
        }
        unwind_replacement_subscriptions(ctx, next.as_ref(), &mut guard, pacing).await;

        // Through the guard, which owes this close until it returns: a drop
        // parked here (or before it) closes the old connection instead of
        // leaking it.
        let mut retired = ctx.push_retired_tx.subscribe();
        let drained = async {
            loop {
                if *retired.borrow_and_update() > retiring_generation {
                    break;
                }
                if retired.changed().await.is_err() {
                    break;
                }
            }
        };
        if tokio::time::timeout(ctx.push_drain_wait, drained)
            .await
            .is_err()
        {
            tracing::warn!(
                target: "bifrost.sync.recovery",
                account = ?&ctx.account_id,
                retiring_generation = ?retiring_generation,
                "push lanes did not drain within the bound; closing the old account anyway"
            );
        }
        if let Err(error) = guard.close_owed().await {
            pacing.record_holds(ctx, &error);
            tracing::warn!(
                target: "bifrost.sync.reopen",
                account = ?&ctx.account_id,
                error = %error,
                "old account close failed during reopen"
            );
        }
        Ok(())
    }
    .await;

    if let Err(Error::Account(error) | Error::EstablishCursorTerminated(error)) = &result {
        // Establishment has already captured its deadline before rollback.
        if !pacing.observed {
            pacing.observe(ctx, error);
        }
    }
    if result.is_err()
        && let Err(error) = guard.close_owed().await
    {
        pacing.record_holds(ctx, &error);
        tracing::warn!(
            target: "bifrost.sync.reopen",
            account = ?&ctx.account_id,
            error = %error,
            "replacement account close failed while unwinding reopen"
        );
    }
    result
}

pub(in crate::engine) fn accepted_push_scopes(
    result: &bifrost_types::PushSubscription,
) -> Vec<CursorScope> {
    result
        .outcomes()
        .succeeded()
        .iter()
        .map(|success| success.output.clone())
        .collect()
}

/// Restart the whole account with exponential backoff and a retry
/// budget. After three consecutive failed attempts - a `factory.open`
/// failure or a replacement that could not be swapped in, an old-side push
/// teardown failure included - the account is paused with
/// `PauseReason::RetryBudgetExhausted`. Its final error preserves terminal
/// and existing operator-attention classifications; retry, reconcile, and
/// other engine-action classifications become operator attention because no
/// automatic action remains.
///
/// Returns whether a replacement was actually swapped in. `false` covers the
/// exhausted budget (the account is now paused), a detach, and shutdown; a
/// caller must not run follow-up repairs against an account that did not
/// restart.
pub(in crate::engine) async fn restart_account(ctx: &RecoveryContext<'_>) -> bool {
    let mut backoff = crate::backoff::ReconnectBackoff::new(REOPEN_BACKOFF);
    let mut last_error: Option<AccountError> = None;
    let mut attempt = 0;
    let mut pacing = RepairPacing::default();
    'restart: while attempt < REOPEN_RETRY_BUDGET.0 {
        // A consumer pause is a quiescence boundary. Keep this recovery
        // request queued until resume instead of opening a replacement while
        // the account has promised to be idle.
        if !ctx.control.wait_until_running(&ctx.shutdown).await {
            return false;
        }
        if attempt > 0 && pacing.local_deadline.is_none() {
            let sleep_for = backoff.record_failure(crate::backoff::jitter_seed());
            if !sleep_unless_shutdown(sleep_for, &ctx.shutdown).await {
                return false;
            }
        }
        if let Err(error) = pacing.wait(ctx, None, None).await {
            if matches!(error, Error::WaitBudgetExhausted) {
                warn_wait_budget(ctx, CursorScope::Account);
                engine_pause(ctx, PauseReason::RetryBudgetExhausted);
            }
            return false;
        }
        let Some(activity) = ctx.control.begin_activity() else {
            continue;
        };
        let mut boundary = ctx.control.drive_boundary();
        if boundary.peek() != BoundaryRequest::Run {
            continue 'restart;
        }
        let reopen_guard = tokio::select! {
            biased;
            () = ctx.shutdown.cancelled() => return false,
            _ = boundary.changed() => continue 'restart,
            guard = Arc::clone(&ctx.reopen_lock).lock_owned() => guard,
        };
        if stale_generation(ctx) {
            return false;
        }
        let gate = crate::engine::drive_start::DriveStart {
            current: &ctx.current,
            control: &ctx.control,
            scheduler: &ctx.scheduler,
            throttles: &ctx.throttles,
            account_id: &ctx.account_id,
            shutdown: &ctx.shutdown,
        };
        // The reopen lock is another internal wait. Release every owner before
        // paying a hold; the next pass charges it to RepairPacing's allowance.
        if !pacing.pending_wait(ctx, None, None).is_zero() {
            drop(reopen_guard);
            drop(activity);
            continue;
        }
        // Every recovery path orders reopen lock before scheduler budget.
        // Taking a permit first can deadlock against a scoped repair that
        // owns this lock and is queued for that same one-permit budget.
        let admitted = match gate
            .admit_recovery(crate::scheduler::WorkKind::Sync, None, |_| {
                pacing.failed_keys.clone()
            })
            .await
        {
            Ok(admitted) => admitted,
            Err(crate::engine::drive_start::StartRefused::ShuttingDown) => return false,
            Err(_) => {
                drop(reopen_guard);
                drop(activity);
                continue;
            }
        };
        drop(activity);
        let crate::engine::drive_start::RecoveryAdmission { activity, permit } = admitted;
        // The previous attempt's holds have been paid. Failures from this
        // open start a new local deadline set, including a hintless failure.
        pacing.local_deadline = None;
        pacing.observed = false;
        let opened = open_replacement_admitted(ctx, reopen_guard, activity).await;
        drop(permit);
        match opened {
            // A pause won the race between the wait above and the activity
            // registration, and nothing was opened. Loop back through the
            // boundary wait instead of spending an attempt on it.
            Err(ReplacementOpen::Paused) => continue,
            // The slot is being torn down. Nothing to restart, and the
            // replacement (if one was opened at all) has already been closed.
            Err(ReplacementOpen::Detached) => return false,
            Ok((_reopen_guard, activity, next)) => {
                match reattach_account_paced(ctx, activity, next, &mut pacing).await {
                    Ok(()) => return true,
                    Err(Error::Account(error) | Error::EstablishCursorTerminated(error)) => {
                        tracing::warn!(
                            target: "bifrost.sync.changes",
                            account = ?&ctx.account_id,
                            attempt,
                            kind = ?error.kind(),
                            message_key = error.message_key(),
                            "RestartAccount: replacement attach failed"
                        );
                        last_error = Some(error);
                        attempt = attempt.saturating_add(1);
                    }
                    Err(Error::WaitBudgetExhausted) => {
                        warn_wait_budget(ctx, CursorScope::Account);
                        engine_pause(ctx, PauseReason::RetryBudgetExhausted);
                        return false;
                    }
                    Err(error) => {
                        pacing.local_deadline = None;
                        tracing::warn!(
                            target: "bifrost.sync.changes",
                            account = ?&ctx.account_id,
                            attempt,
                            error = %error,
                            "RestartAccount: replacement attach failed"
                        );
                        last_error = Some(crate::engine_errors::establish_failure_error(
                            CursorScope::Account,
                            bifrost_types::AccountOperation::EstablishCursor,
                        ));
                        attempt = attempt.saturating_add(1);
                    }
                }
            }
            Err(ReplacementOpen::Failed(err)) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?&ctx.account_id,
                    attempt,
                    kind = ?err.kind(),
                    message_key = err.message_key(),
                    "RestartAccount: factory.open failed"
                );
                pacing.failed_keys.clear();
                pacing.observe(ctx, &err);
                last_error = Some(err);
                attempt = attempt.saturating_add(1);
            }
        }
    }
    // Account-wide budget exhausted: the retry and engine-action classes no
    // longer describe work the engine will perform. Preserve terminal and
    // existing operator-attention errors, and classify every other last error
    // as operator attention before pausing all scopes with the account.
    if let Some(err) = last_error {
        broadcast_terminated(
            &ctx.delivery,
            EventScope::Account,
            crate::engine_errors::recovery_exhaustion_error(
                err,
                CursorScope::Account,
                bifrost_types::AccountOperation::EstablishCursor,
            ),
        );
    }
    engine_pause(ctx, PauseReason::RetryBudgetExhausted);
    false
}

/// What a reattach does with one scope's establishment result.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::engine) enum ReattachScopeStep {
    /// Established; stage it.
    Keep,
    /// Leave the scope out of the staged registry and carry on.
    Skip,
    /// Fail the whole reattach attempt.
    Fail,
}

/// Classify one scope's establishment during a reattach.
///
/// `Superseded` is a skip, not a failure: a reattach holds the reopen lock, so
/// the only producer of a moved fence is a provider deletion, and its queued
/// `ScopeDeleted` owns the follow-up. Failing the attempt instead would make a
/// deletion racing a reattach cost a full reattach retry, and enough of them
/// would exhaust the reopen budget and pause the account.
pub(in crate::engine) fn reattach_scope_step(
    result: &Result<EstablishOrigin, Error>,
) -> ReattachScopeStep {
    match result {
        Ok(_) => ReattachScopeStep::Keep,
        Err(Error::Superseded) => ReattachScopeStep::Skip,
        Err(Error::Account(error)) if scope_local_establish_failure(error) => {
            ReattachScopeStep::Skip
        }
        Err(_) => ReattachScopeStep::Fail,
    }
}
