//! The backfill orchestrator: scope rescan, resume planning, and the
//! per-partition walk.

use super::*;
use crate::backfill::marker::{MarkerOutcome, emit_backfill_complete};
use crate::backfill::plan::{ScopeResume, backfill_plan_for};
use crate::backfill::scan::{BackfillScan, ParkedWalk};

/// The backfill-specific half of the orchestrator's wiring; everything
/// slot-wide rides in the [`SlotContext`]. Publication always uses the slot
/// delivery endpoint, so this wiring needs no broadcast sender or enable flag.
pub(super) struct BackfillWiring {
    pub live: Arc<LiveSupersedes>,
    pub registry: Arc<BackfillRegistry>,
    pub config: BackfillConfig,
    /// Scope incarnations whose cold-start inventory the fusion worker
    /// owns; the orchestrator must not double-publish them.
    pub fusion_owned_scopes: HashSet<CursorScope>,
}

/// Backfill orchestrator. Walks the registered cursor scopes and runs
/// one partition pass per scope via `BackfillRunner::run_partition`.
///
/// The runner uses the slot's shared `LiveSupersedes` set so live
/// `Created` events from the multiplexer skip over inventory entries
/// the user has already seen.
///
/// Resume: the in-memory `BackfillRegistry` is wiped on detach, so the
/// only durable record of backfill progress is the consumer-acked
/// `BackfillCheckpoint` in the `CheckpointStore`. Before walking a scope
/// the orchestrator reads that checkpoint back via `get_backfill` and
/// hands it to `BackfillPlan::resume`, which either skips a scope a prior
/// run finished (the steady-state delta case - it must not re-walk at
/// all) or resumes after the furthest durably-checkpointed full page
/// instead of re-paginating from page 0 on every re-attach.
pub(super) async fn run_backfill_orchestrator(ctx: SlotContext, wiring: BackfillWiring) {
    // The producer's door onto the account's backfill bound. Taken
    // before the destructure below so it carries the slot's shutdown token.
    let backfill_bound = ctx.backfill_bound();
    let account = Arc::clone(&ctx.current);
    let account_id = ctx.account_id.clone();
    let cursors = Arc::clone(&ctx.cursors);
    let shutdown = ctx.shutdown.clone();
    let subscriber_notify = Arc::clone(&ctx.subscriber_notify);
    let control = ctx.control.clone();
    let throttles = Arc::clone(&ctx.throttles);
    let coverage = Arc::clone(&ctx.coverage);
    let writer_tx = ctx.writer();
    let delivery = Arc::clone(&ctx.delivery);
    let BackfillWiring {
        live,
        registry,
        config,
        fusion_owned_scopes,
    } = wiring;
    let publisher = crate::delivery::PagePublisher::new(
        Arc::clone(&delivery),
        control.clone(),
        writer_tx.clone(),
        shutdown.clone(),
    );
    let mut scan = BackfillScan::default();
    scan.publish(
        &registry,
        account_id.clone(),
        Arc::clone(&cursors),
        Arc::clone(&coverage),
    );
    // Cold-start backfill pages broadcast onto the per-account channel
    // during `attach`, but a consumer can only call
    // `account_changes_stream` after `attach` inserts the slot into
    // `self.accounts`. A `tokio::broadcast` receiver that subscribes
    // late starts at the ring's tail and never sees values sent before
    // it joined (the slot's sentinel receiver keeps `receiver_count()`
    // at 1, so the send "succeeds" yet lands in front of no real
    // reader). For a `Ready`-cursor account whose entire cold start
    // rides backfill - Gmail `CursorScope::Account`, JMAP
    // `CursorScope::Type(Email)` - that silently drops the initial
    // inventory page, so the consumer ingests zero objects. Park until a
    // real subscriber arrives, exactly as
    // `run_deferred_inventory_establishment` does for the fusion path,
    // so the first page is observed rather than raced away. (sync-N3)
    if !wait_for_real_subscriber(&delivery, &subscriber_notify, &shutdown).await {
        return;
    }
    // Fusion-owned scopes are excluded by identity, not by whether the
    // fusion worker happened to install their cursor before this scan.
    // Keep scanning for new scope incarnations for the lifetime of the
    // attach so lifecycle creation and explicit re-establishment receive
    // their own cold-start pass.
    loop {
        let scopes = scan.select(
            cursors.all_scope_incarnations(),
            &fusion_owned_scopes,
            tokio::time::Instant::now(),
            &coverage,
        );
        'scopes: for (scope, incarnation) in scopes {
            if shutdown.is_cancelled() {
                return;
            }
            // RE-GATE on subscriber loss, not just once before the first scan.
            //
            // The receiver-drop sweep wakes a parked producer, and if the
            // consumer that left was the only one, the pages that follow reach
            // the sentinel receiver alone and are retired the moment they are
            // sent. Without this the walk would run its remaining inventory into
            // nobody, reach its completion sentinel, and `record_attempt(.., true)`
            // would settle the incarnation permanently - so a consumer that
            // reattaches gets no inventory and no retry, and no amount of waiting
            // helps. Parking here is the same answer the first gate gives, for
            // the same reason.
            if !wait_for_real_subscriber(&delivery, &subscriber_notify, &shutdown).await {
                return;
            }
            let incarnation_key = (scope.clone(), incarnation);
            // Read before validating the registry snapshot. A reset between
            // validation and this read must not lend an old scan entry its
            // replacement's fence, especially for pre-retry deletion.
            let fence_at_entry = coverage.scope_fence(&scope);
            // The subscriber wait above is indefinite, and the scan snapshot this
            // loop is iterating was taken before it. Re-check that the
            // incarnation is still the live one rather than walking a scope the
            // registry has since dropped or re-established.
            if !cursors.all_scope_incarnations().contains(&incarnation_key) {
                continue;
            }
            let parked = scan.take_parked(&incarnation_key);
            let (
                mut driver,
                mut pending_partition,
                undelivered_before,
                fence_at_walk_start,
                labels,
            ) = if let Some(parked) = parked {
                if coverage.undelivered_watermark(&scope) != parked.undelivered_before {
                    scan.note_lost_pages(incarnation_key.clone());
                    discard_backfill_progress(
                        &writer_tx,
                        &account_id,
                        coverage.backfill_discard_for_walk(&scope, parked.fence_at_walk_start),
                    )
                    .await;
                    scan.record_attempt(incarnation_key, false);
                    continue;
                }
                (
                    parked.driver,
                    Some(parked.pending),
                    parked.undelivered_before,
                    parked.fence_at_walk_start,
                    parked.labels,
                )
            } else {
                // What the walk has to beat: any page that reaches nobody bumps
                // this, wherever it happens. Taken through the scan, as the
                // walk begins, so the settle is recorded against a validated
                // reading and an earlier scope's sweep is not charged here.
                let undelivered_before = scan.begin_walk(&incarnation_key, &coverage);
                // The incarnation this walk belongs to, snapshotted at its
                // START and carried into the completion marker, so a reset
                // anywhere inside the walk is never adopted as its own.
                let fence_at_walk_start = fence_at_entry;
                // Planning and writer barrier reads do no wire work. They
                // need no scheduler budget; partition admission below owns the
                // start gate and rechecks this original walk fence after waits.
                let barrier_blocked = match scope_barrier_blocked(&writer_tx, scope.clone()).await {
                    Ok(blocked) => blocked,
                    Err(error) => {
                        // Unknown debt still stops the walk, but must not tell
                        // the operator to waive a barrier nobody has observed.
                        tracing::warn!(%error, ?scope, "backfill barrier query failed");
                        scan.record_attempt(incarnation_key, false);
                        continue;
                    }
                };
                if barrier_blocked {
                    tracing::debug!(
                        target: "bifrost.sync.backfill",
                        account = ?account_id,
                        scope = ?scope,
                        "backfill parked at operator-blocked barrier"
                    );
                    // Record the park as an attempt so the incarnation rejoins
                    // the exponential rescan delay. Skipping this leaves the
                    // scope's `failed` deadline in the past, so the 1s rescan
                    // tick re-asks the single account writer about the same
                    // blocked scope every second for as long as the operator
                    // leaves the block in place - and the writer is the task
                    // that also owns every durable mutation. `select` already
                    // withholds a scope until its delay elapses, so reusing it
                    // here parks the query on the same 5s-to-5min ramp the walk
                    // itself would have used.
                    scan.record_barrier_wait(incarnation_key);
                    continue;
                }
                let acc_arc = account.load_full();
                let acc: &dyn Account = acc_arc.as_ref().as_ref();
                // Both plan shapes drive the same `ScopeWalkDriver`, so the
                // only thing that differs between them is how the durable
                // checkpoint is turned into a starting driver: a fixed plan
                // has no positional "resume from here" (its partitions are a
                // known finite set), so its durable signal is binary, while an
                // open-ended page walk resumes at the furthest acked window.
                // `BackfillPlan::resume` is where that difference lives; the
                // walk below is shared, which is what keeps the two shapes
                // from drifting apart on barrier handling, completion-marker
                // withholding, or registry bookkeeping.
                let plan = backfill_plan_for(
                    acc.inventory_partitioning(&scope),
                    &scope,
                    config,
                    jiff::Timestamp::now(),
                );
                let labels = plan.labels();
                // A PRIOR attempt on this incarnation that lost pages disqualifies
                // the stored checkpoint as a resume position, and only for a
                // positional plan does that matter - but it matters a lot there. The
                // pages the departed consumer never received sit BEHIND the windows
                // the replacement did receive and acknowledge, so resuming after the
                // furthest acked window walks past the hole, hits its empty end
                // probe, and settles the incarnation having never re-offered the
                // lost pages to anybody. The retry has to start where the walk is
                // known whole, which is the walk's own beginning; `walk_from_scratch`
                // is exactly that answer, already defined as the no-checkpoint case.
                // Re-emitting acknowledged pages is idempotent, so the cost is
                // re-work, not correctness.
                let restart_whole = scan.restarts_from_scratch(&incarnation_key);
                if restart_whole {
                    // Drop the previous attempt's durable footprint BEFORE walking,
                    // not only when the loss was noticed. The ack-time refusal
                    // repairs memory, but it cannot always repair the store: a loss
                    // that lands AFTER the marker's acknowledgement leaves that
                    // marker durable, and a detach before this retry lands its own
                    // would let the next attach read the stale marker back and answer
                    // `Skip` - the lost pages never re-offered, for good. Idempotent,
                    // and safe to fence here precisely because the retry has minted
                    // no ids yet. It also SETTLES this scope's outstanding request -
                    // in the writer after deletion succeeds. Keep the original
                    // walk fence: a reset during planning revokes this decision.
                    discard_backfill_progress(
                        &writer_tx,
                        &account_id,
                        coverage.backfill_discard_for_walk(&scope, fence_at_walk_start),
                    )
                    .await;
                }
                // The stored checkpoint is read only when it can decide anything. A
                // restart-from-scratch has already ruled it out as a resume position,
                // so reading it costs a store round trip whose answer is discarded -
                // and its failure logged "resume read failed" for a walk that was
                // starting over either way, which reads as a degrade where nothing
                // degraded.
                let resume = if restart_whole {
                    tracing::debug!(
                        target: "bifrost.sync.backfill",
                        account = ?account_id,
                        scope = ?scope,
                        "a previous attempt on this incarnation lost pages; restarting the \
                         walk from its beginning rather than resuming past them"
                    );
                    plan.walk_from_scratch()
                } else {
                    match writer_tx.get_backfill(scope.clone()).await {
                        Ok(stored) => plan.resume(stored.as_ref()),
                        Err(err) => {
                            // A read failure is not authoritative; fall back to a
                            // full walk rather than risk skipping unpersisted
                            // pages.
                            tracing::warn!(
                                target: "bifrost.sync.backfill",
                                scope = ?scope,
                                error = %err,
                                "{}", labels.resume_read_failed
                            );
                            plan.walk_from_scratch()
                        }
                    }
                };
                // Skip a scope whose backfill already reached a durable
                // conclusion on a prior run. `get_backfill` only ever returns
                // consumer-acked checkpoints, so this never skips a window the
                // consumer has not durably persisted.
                let ScopeResume::Walk(driver) = resume else {
                    scan.settled(incarnation_key);
                    continue;
                };
                // The driver owns the sequence: a barrier stops the SCOPE, not
                // merely the partition that hit it, so a stopped walk simply
                // hands out no further partition. There is no flag here to
                // forget to check.
                //
                // For the page walk that also means terminating only where a
                // partition's completion declares `PartitionEnd::Exhausted`.
                // The entry count is not a declaration: a server capping a page
                // below the requested `chunk` returns a short page with more
                // behind it, and a window whose ids all vanished between listing
                // and hydration is empty with more behind it.
                (
                    driver,
                    None,
                    undelivered_before,
                    fence_at_walk_start,
                    labels,
                )
            };

            while let Some(partition) = pending_partition.take().or_else(|| driver.next_partition())
            {
                if shutdown.is_cancelled() {
                    return;
                }
                let mut held = None;
                let Some(result) = run_backfill_partition_at_boundary(
                    &account,
                    &account_id,
                    scope.clone(),
                    partition.clone(),
                    &live,
                    &control,
                    &shutdown,
                    &throttles,
                    &coverage,
                    &writer_tx,
                    &backfill_bound,
                    &delivery,
                    fence_at_walk_start,
                    &cursors,
                    incarnation,
                    &mut held,
                )
                .await
                else {
                    if let Some(wait) = held {
                        scan.park(
                            incarnation_key.clone(),
                            ParkedWalk {
                                driver,
                                pending: partition,
                                fence_at_walk_start,
                                undelivered_before,
                                labels,
                                until: tokio::time::Instant::now() + wait,
                            },
                        );
                        continue 'scopes;
                    }
                    return;
                };
                match result {
                    Ok(outcome) => {
                        let complete = outcome.verdict.complete();
                        let step = driver.fold(&outcome);
                        if !complete {
                            // An unresolved obligation means the
                            // enumeration space was not exhausted, so the
                            // completion marker must be withheld.
                            tracing::warn!(
                                target: "bifrost.sync.backfill",
                                account = ?account_id,
                                scope = ?scope,
                                "{}", labels.unresolved_coverage
                            );
                        }
                        if step == crate::backfill::ScopeWalkStep::StopScopeWalk {
                            scan.mark_operator_blocked(&incarnation_key);
                            tracing::warn!(
                                target: "bifrost.sync.backfill",
                                account = ?account_id,
                                scope = ?scope,
                                "{}", labels.barrier_stop
                            );
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "bifrost.sync.backfill",
                            scope = ?scope,
                            error = %err,
                            "{}", labels.partition_failed
                        );
                        driver.fail();
                    }
                }
            }
            let completed = driver.completed();
            // Did EVERY page of this walk reach a consumer? Any page that reached
            // nobody bumps this, wherever it happened - the retire-on-sentinel-only
            // path in the runner, or a receiver's departure sweep. A subscriber
            // count at the end cannot answer it: a consumer that fills the bound
            // and leaves, is replaced before the walk finishes, and whose
            // replacement acknowledges the remaining pages satisfies every
            // point-in-time check while the pages published in the gap reached
            // nobody at all.
            let every_page_reached_someone =
                coverage.undelivered_watermark(&scope) == undelivered_before;
            if completed && !every_page_reached_someone {
                tracing::warn!(
                    target: "bifrost.sync.backfill",
                    account = ?account_id,
                    scope = ?scope,
                    "backfill walk finished but some of its pages reached no consumer; \
                     withholding the completion marker so the scope is re-walked"
                );
            }
            // Persist a durable completion marker through the same
            // consumer-ack path the page batches use. It is ordered behind
            // every page, so a crash before its ack re-walks instead of
            // recording a false completion.
            //
            // The marker is withheld on a walk with a hole in it, and that is
            // load-bearing rather than tidy: declining to settle the incarnation
            // in memory buys nothing on its own, because the marker is DURABLE.
            // A replacement consumer that arrives mid-walk acknowledges it into
            // the checkpoint store, and the very next rescan reads it back,
            // answers `ScopeResume::Skip`, and settles the scope with the hole
            // still in it - for this attachment and every later one.
            //
            // The emission can PARK on the bound, indefinitely, so the condition
            // is re-validated inside it against the same watermark reading rather
            // than only here: a final page can be swept undelivered while the
            // marker waits, and a marker published on the wake would record a
            // durable completion for a walk that had just lost a page.
            let mut settle = completed && every_page_reached_someone;
            if settle {
                match emit_backfill_complete(
                    &scope,
                    driver.total_seen(),
                    &publisher,
                    &backfill_bound,
                    undelivered_before,
                    fence_at_walk_start,
                )
                .await
                {
                    MarkerOutcome::Published => {}
                    // The walk was invalidated while its marker waited - a scope
                    // reset closed inside the wait, or a page was swept
                    // undelivered. Neither is a reason to stop: an earlier
                    // revision returned the same `false` for this as for
                    // shutdown, so one reset abandoned every LATER scope and
                    // every later rescan for the rest of the attachment. Record
                    // the interrupted attempt and carry on with the next scope.
                    MarkerOutcome::Withheld => settle = false,
                    MarkerOutcome::ShuttingDown => return,
                }
            }
            // A walk only SETTLES if its pages actually reached somebody. If the
            // consumer left partway through, every page since was retired on
            // send, the completion sentinel with them - so the walk "completed"
            // having delivered nothing, and settling it would retire the
            // incarnation for the life of the attachment. Recording it as an
            // attempt instead puts it back on the rescan ramp for whoever
            // subscribes next.
            //
            // Re-read rather than reusing the reading above: the completion
            // marker is itself a publication and can reach nobody in its turn,
            // which is the no-consumer case and must not settle either.
            let walk_reached_someone = coverage.undelivered_watermark(&scope) == undelivered_before;
            if !walk_reached_someone {
                // Remember it for the RETRY, not only for this decision. A
                // positional plan would otherwise resume after the windows the
                // replacement consumer acknowledged, which sit past the hole.
                scan.note_lost_pages(incarnation_key.clone());
                // And remember it DURABLY. The note above dies with the
                // attachment, while the misleading rows do not: a detach before
                // the retry leaves a fresh attach resuming past the hole, taking
                // one empty end probe and completing without ever replaying the
                // missing windows. Dropping the rows is the durable form of the
                // same decision - they are resume hints, so the cost is re-work.
                //
                // The writer settles only a current request after deletion.
                // Carry this walk's fence even if a reset has already landed;
                // adopting the replacement fence would delete its progress.
                discard_backfill_progress(
                    &writer_tx,
                    &account_id,
                    coverage.backfill_discard_for_walk(&scope, fence_at_walk_start),
                )
                .await;
            }
            let concluded = settle && walk_reached_someone;
            // The settle baseline is NOT passed in: `BackfillScan` holds the
            // reading `begin_walk` took as this walk started, so a loss landing
            // between the checks above and this line cannot be adopted as the
            // baseline it was never checked against.
            scan.record_attempt(incarnation_key, concluded);
        }
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep_until(scan.next_wake()) => {}
        }
    }
}

/// Ask the account's single writer to drop a scope's durable backfill rows.
///
/// Best effort by design: if the writer is gone the attachment is tearing down,
/// and if the delete fails the in-memory note still forces the restart for the
/// life of this attachment. Either way the failure direction is a resume that is
/// too EARLY, never one that is too late.
/// The writer alone settles a current request after a successful delete. A stale
/// no-op reply must never remove a replacement walk's outstanding loss request.
///
/// Not "the writer accepted the send": a store error leaves the rows exactly
/// where they were, and a request consumed by a delete that did not happen is a
/// repair nobody will attempt again - the teardown drain would find nothing to
/// retry. Not anything earlier either: taking the request before the reply means
/// an abort at `detach_timeout` in between loses it outright, while a request
/// still outstanding is simply retried by whoever gets there next.  Both
/// failures leave the rows in place, so the answer to both is to keep the
/// request.
pub(super) async fn discard_backfill_progress(
    writer: &WriterHandle,
    account_id: &AccountId,
    request: crate::cursor::coverage::BackfillDiscard,
) -> bool {
    let scope = request.scope.clone();
    match writer.discard_backfill_progress(request).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                error = %error,
                "could not drop the backfill rows of a walk that lost pages; the request \
                 stays outstanding so teardown retries it"
            );
            false
        }
        // The writer went away before answering. The request stays outstanding;
        // there is nothing left in this attachment to act on it, and the next one
        // starts from rows that are still there.
        Err(_) => false,
    }
}

async fn scope_barrier_blocked(writer: &WriterHandle, scope: CursorScope) -> Result<bool, Error> {
    writer.scope_barrier_blocked(scope).await
}

#[allow(clippy::too_many_arguments)]
async fn run_backfill_partition_at_boundary(
    account: &Arc<ArcSwap<crate::types::Incarnation>>,
    account_id: &AccountId,
    scope: CursorScope,
    partition: InventoryPartition,
    live: &Arc<LiveSupersedes>,
    control: &SyncControl,
    shutdown: &CancellationToken,
    throttles: &crate::throttle::ThrottleBucket,
    coverage: &Arc<PendingCoverage>,
    writer_tx: &WriterHandle,
    bound: &BackfillBound,
    delivery: &Arc<crate::delivery::ChangeDelivery>,
    fence_at_walk_start: crate::cursor::ScopeFence,
    cursors: &CursorRegistry,
    incarnation: crate::cursor::ScopeIncarnation,
    // Report a scope-local hold without retaining admission or a provider stream.
    held: &mut Option<std::time::Duration>,
) -> Option<Result<crate::backfill::BackfillPartitionOutcome, Error>> {
    // One generation per partition pass, so a re-walk's proof is ordered after
    // the debt an earlier pass raised.
    let generation = coverage.next_generation();
    loop {
        if !control.wait_until_running(shutdown).await {
            return None;
        }
        let gate = super::drive_start::DriveStart {
            current: account,
            control,
            scheduler: bound.scheduler(),
            throttles,
            account_id,
            shutdown,
        };
        let admitted = match gate
            .admit_drive(
                bifrost_types::ThrottleWork::Scope(&scope),
                crate::scheduler::WorkKind::Backfill,
            )
            .await
        {
            Ok(admitted) => admitted,
            Err(super::drive_start::StartRefused::ShuttingDown) => return None,
            Err(super::drive_start::StartRefused::Paused) => continue,
            Err(super::drive_start::StartRefused::Held { until, .. }) => {
                *held = Some(until.saturating_duration_since(tokio::time::Instant::now()));
                return None;
            }
        };
        let super::drive_start::Admitted {
            incarnation: current,
            activity,
            permit,
        } = admitted;
        let backfill_permit = bound.hold_admission(permit);
        // RE-CHECK THE WALK'S FENCE, with admission in hand and before a single
        // page has been read. Every step above this line can park indefinitely -
        // a pause, a throttle deadline, the scheduler's admission queue - and a
        // scope reset can close inside any of them. `run_partition` is handed
        // this same `fence_at_walk_start` and registers every page only if the
        // fence still equals it, so a stale page can no longer be minted; this
        // check is the cheap early exit that spares the provider a pass whose
        // every page would be refused, and the incarnation half catches a
        // registry replacement the fence does not describe. Were a pass to
        // adopt a fence read after the reset, its pages would be minted above
        // the fence, a consumer would acknowledge them in good faith, and the
        // durable rows a `delete_backfill: true` reset deleted would come back -
        // for a folder the provider has deleted, that resurrects the completion
        // marker that makes a folder recreated under the same id skip its
        // cold-start walk.
        //
        // Failing the partition is the right shape: the driver ends the walk,
        // withholds the completion marker, and leaves the scope Pending. If the
        // scope is still in the registry it is re-walked from its replacement
        // incarnation's beginning; if the reset removed it, nothing walks it,
        // which is what a deleted folder should get.
        if coverage.scope_fence(&scope) != fence_at_walk_start
            || !cursors
                .all_scope_incarnations()
                .contains(&(scope.clone(), incarnation))
        {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                "scope was reset between two partitions of a backfill walk; ending the \
                 walk rather than publishing its later partitions against rows the \
                 reset deleted"
            );
            return Some(Err(Error::Superseded));
        }
        let publisher = crate::delivery::PagePublisher::new(
            Arc::clone(delivery),
            control.admitted_by(&activity),
            writer_tx.clone(),
            shutdown.clone(),
        );
        let result = BackfillRunner::run_partition(
            current.as_ref().as_ref(),
            scope.clone(),
            partition.clone(),
            live.as_ref(),
            bifrost_types::TraitEnvelopeVersion::new(crate::cursor::ENGINE_VERSION),
            &publisher,
            generation,
            Some(backfill_permit),
            fence_at_walk_start,
        )
        .await;
        if matches!(result, Err(Error::Paused)) {
            continue;
        }
        return Some(result);
    }
}
