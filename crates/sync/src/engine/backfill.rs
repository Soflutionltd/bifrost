//! The backfill orchestrator: scope rescan, resume planning, and the
//! per-partition walk.

use super::*;

/// The backfill-specific half of the orchestrator's wiring; everything
/// slot-wide rides in the [`SlotContext`].
pub(super) struct BackfillWiring {
    pub live: Arc<LiveSupersedes>,
    pub store: Arc<DynCheckpointStore>,
    pub registry: Arc<BackfillRegistry>,
    pub config: BackfillConfig,
    /// Scope incarnations whose cold-start inventory the fusion worker
    /// owns; the orchestrator must not double-publish them.
    pub fusion_owned_scopes: HashSet<CursorScope>,
    /// Where inventory pages are published. `None` runs the walk with no
    /// broadcast at all, which is why this is not simply the context's
    /// `changes_tx`.
    pub changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
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
    // The producer's door onto the account's bounded backfill lane. Taken
    // before the destructure below so it carries the slot's shutdown token.
    let lane_gate = ctx.lane_gate();
    let SlotContext {
        current: account,
        account_id,
        cursors,
        shutdown,
        subscriber_notify,
        control,
        throttles,
        coverage,
        writer_tx,
        scheduler,
        delivery,
        ..
    } = ctx;
    let BackfillWiring {
        live,
        store,
        registry,
        config,
        fusion_owned_scopes,
        changes_tx,
    } = wiring;
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
    if changes_tx.is_some()
        && !wait_for_real_subscriber(&delivery, &subscriber_notify, &shutdown).await
    {
        return;
    }
    // Fusion-owned scopes are excluded by identity, not by whether the
    // fusion worker happened to install their cursor before this scan.
    // Keep scanning for new scope incarnations for the lifetime of the
    // attach so lifecycle creation and explicit re-establishment receive
    // their own cold-start pass.
    let mut scan = BackfillScan::default();
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
            if changes_tx.is_some()
                && !wait_for_real_subscriber(&delivery, &subscriber_notify, &shutdown).await
            {
                return;
            }
            let incarnation_key = (scope.clone(), incarnation);
            // The subscriber wait above is indefinite, and the scan snapshot this
            // loop is iterating was taken before it. Re-check that the
            // incarnation is still the live one rather than walking a scope the
            // registry has since dropped or re-established.
            if !cursors.all_scope_incarnations().contains(&incarnation_key) {
                continue;
            }
            // A hold on THIS scope alone (a mailbox budget the account declares
            // for it) defers the scope and lets the scan move on to the next
            // one: walks run one after another, so sleeping it out here would
            // stall every later scope's cold start behind one mailbox. An
            // ACCOUNT-WIDE hold blocks every scope equally and is slept out
            // here, inline. A hold that lands
            // between two partitions of a walk already under way is also
            // waited out inline there: the walk must finish against the same
            // budget either way, and abandoning it would cost a re-walk.
            //
            // The account-wide hold is slept out FIRST, and the scope-local
            // decision is taken after it: a short account hold overlapping a
            // long mailbox hold would otherwise hand the scope to the partition
            // runner's inline wait, which would then sit out the mailbox hold
            // with every later scope behind it.
            while let Some(wait) = crate::recovery::account_throttle_wait(&throttles, &account_id) {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(wait) => {}
                }
            }
            // That sleep is unbounded in the same way the subscriber wait is,
            // so the incarnation is re-validated after it, and BEFORE the walk's
            // fence is captured below: a scope reset during the sleep must not
            // be adopted as this walk's own starting fence.
            if !cursors.all_scope_incarnations().contains(&incarnation_key) {
                continue;
            }
            let keys = crate::recovery::declared_keys(
                account.load_full().as_ref().as_ref(),
                &account_id,
                bifrost_types::ThrottleWork::Scope(&scope),
            );
            if let Some(wait) = crate::recovery::throttle_wait(&throttles, &account_id, &keys) {
                tracing::debug!(
                    target: "bifrost.sync.backfill",
                    account = ?account_id,
                    scope = ?scope,
                    wait_secs = wait.as_secs(),
                    "backfill walk deferred by a throttle on this scope"
                );
                scan.defer_until(incarnation_key, tokio::time::Instant::now() + wait);
                continue;
            }
            // What the walk has to beat: any page that reaches nobody bumps this,
            // wherever it happens - the retire-on-sentinel-only path in the
            // runner, or a receiver's departure sweep. Comparing two readings is
            // the only honest way to ask "did EVERY page of this walk reach a
            // consumer", which a subscriber count at the end cannot answer.
            //
            // Taken through the SCAN and taken HERE, as this walk begins: the
            // scan owns the reading, so the settle cannot be recorded against a
            // number nothing validated, and taking it now rather than when the
            // batch was selected keeps a sweep during an earlier scope's walk
            // from being charged to this one.
            let undelivered_before = scan.begin_walk(&incarnation_key, &coverage);
            // The incarnation this walk belongs to, snapshotted at its START.
            // Carried all the way into the completion marker: a reset that closes
            // anywhere inside the walk - including after its last page but before
            // the provider's terminal `Done`, where the old stream simply finishes
            // - must not be adopted as this walk's own. An emission-time snapshot
            // reads the REPLACEMENT incarnation's fence, so the marker sails past
            // both checks, is minted above the fence, acknowledges normally, and
            // suppresses the replacement's whole inventory walk.
            let fence_at_walk_start = coverage.scope_fence(&scope);
            let barrier_admission = tokio::select! {
                () = shutdown.cancelled() => return,
                permit = scheduler.admit(
                    account_id.clone(),
                    control.priority_snapshot(),
                    crate::scheduler::WorkKind::Sync,
                ) => match permit {
                    Ok(permit) => permit,
                    Err(error) => {
                        tracing::warn!(target: "bifrost.sync.scheduler", %error, "backfill admission failed");
                        scan.record_attempt(incarnation_key, false);
                        continue;
                    }
                },
            };
            if scope_barrier_blocked(&writer_tx, scope.clone()).await {
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
                scan.record_attempt(incarnation_key, false);
                continue;
            }
            drop(barrier_admission);
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
            let plan = backfill_plan_for(acc, &scope, config);
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
                // but only once the writer has ACCEPTED it, or an abort at
                // `detach_timeout` between the two loses the request with nothing
                // left for teardown to drain.
                if discard_backfill_progress(&writer_tx, &account_id, &scope).await {
                    coverage.take_backfill_discard(&scope);
                }
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
                match store.get_backfill(&account_id, &scope).await {
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
            let ScopeResume::Walk(mut driver) = resume else {
                registry.mark(account_id.clone(), scope.clone(), BackfillState::Completed);
                scan.settled(incarnation_key);
                continue;
            };
            // Re-check the scope-local hold now that the walk is about to
            // START: admission, the barrier query and the checkpoint read
            // above can all park, and a hold recorded meanwhile still finds
            // no partition walked, so it defers the scope rather than letting
            // the first partition sit it out with every later scope behind it.
            // The resume decision is simply recomputed next time; the
            // pre-walk discard above is idempotent.
            while let Some(wait) = crate::recovery::account_throttle_wait(&throttles, &account_id) {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(wait) => {}
                }
            }
            // Asked of the current account, not the keys computed before
            // admission, the barrier query and the checkpoint read: a reopen
            // during those waits can change the declaration.
            let walk_keys = crate::recovery::declared_keys(
                account.load_full().as_ref().as_ref(),
                &account_id,
                bifrost_types::ThrottleWork::Scope(&scope),
            );
            if let Some(wait) = crate::recovery::throttle_wait(&throttles, &account_id, &walk_keys)
            {
                scan.defer_until(incarnation_key, tokio::time::Instant::now() + wait);
                continue;
            }
            registry.mark(account_id.clone(), scope.clone(), BackfillState::Running);
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
            let mut first_partition = true;
            while let Some(partition) = driver.next_partition() {
                if shutdown.is_cancelled() {
                    return;
                }
                let mut held_before_first = None;
                let defer_first = if first_partition {
                    Some(&mut held_before_first)
                } else {
                    None
                };
                first_partition = false;
                let Some(result) = run_backfill_partition_at_boundary(
                    &account,
                    &account_id,
                    scope.clone(),
                    partition,
                    &live,
                    changes_tx.clone(),
                    &control,
                    &shutdown,
                    &throttles,
                    &coverage,
                    &writer_tx,
                    &lane_gate,
                    &delivery,
                    fence_at_walk_start,
                    defer_first,
                )
                .await
                else {
                    // A scope-local hold landed before the walk read a
                    // single partition (during lane admission, say): defer
                    // the scope and move on, exactly as the pre-walk check
                    // does, rather than sit the hold out with every later
                    // scope behind it. Nothing was walked, so the scope
                    // returns to Pending and its next pass starts afresh.
                    if let Some(wait) = held_before_first {
                        registry.mark(account_id.clone(), scope.clone(), BackfillState::Pending);
                        scan.defer_until(
                            incarnation_key.clone(),
                            tokio::time::Instant::now() + wait,
                        );
                        continue 'scopes;
                    }
                    return;
                };
                match result {
                    Ok(outcome) => {
                        let complete = outcome.complete;
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
                    changes_tx.as_ref(),
                    &scope,
                    driver.total_seen(),
                    &control,
                    &shutdown,
                    &lane_gate,
                    &delivery,
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
                // Drained rather than sent directly: recording the loss already
                // REQUESTED this, at the moment it happened, so that a detach
                // landing before this line still leaves the rows gone. Taking the
                // request here is what does the work in the ordinary case - and
                // only THIS scope's, because this walk can answer for no other. A
                // drain of every outstanding request would carry off one whose
                // repair has already run and act on it arbitrarily later, against
                // rows that scope may since have re-earned. Taken only once the
                // writer has ACCEPTED it, for the reason on
                // `discard_backfill_progress`.
                if discard_backfill_progress(&writer_tx, &account_id, &scope).await {
                    coverage.take_backfill_discard(&scope);
                }
            }
            let concluded = settle && walk_reached_someone;
            // Marked AFTER the re-read, on the same answer the scan records.
            // Marking before it let the registry report `Completed` for a walk
            // the scan had just put back on the retry ramp - one observable
            // saying finished while the other says pending, for the life of the
            // attachment. One value feeds both, so they cannot disagree.
            registry.mark(
                account_id.clone(),
                scope.clone(),
                if concluded {
                    BackfillState::Completed
                } else {
                    BackfillState::Pending
                },
            );
            // The settle baseline is NOT passed in: `BackfillScan` holds the
            // reading `begin_walk` took as this walk started, so a loss landing
            // between the checks above and this line cannot be adopted as the
            // baseline it was never checked against.
            scan.record_attempt(incarnation_key, concluded);
        }
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

/// Ask the account's single writer to drop a scope's durable backfill rows.
///
/// Best effort by design: if the writer is gone the attachment is tearing down,
/// and if the delete fails the in-memory note still forces the restart for the
/// life of this attachment. Either way the failure direction is a resume that is
/// too EARLY, never one that is too late.
/// Returns whether the delete SUCCEEDED, which is the only thing that settles the
/// request.
///
/// Not "the writer accepted the send": a store error leaves the rows exactly
/// where they were, and a request consumed by a delete that did not happen is a
/// repair nobody will attempt again - the teardown drain would find nothing to
/// retry. Not anything earlier either: taking the request before the reply means
/// an abort at `detach_timeout` in between loses it outright, while a request
/// still outstanding is simply retried by whoever gets there next.  Both
/// failures leave the rows in place, so the answer to both is to keep the
/// request.
async fn discard_backfill_progress(
    writer: &mpsc::Sender<WriterRequest>,
    account_id: &AccountId,
    scope: &CursorScope,
) -> bool {
    let (done, recv) = oneshot::channel();
    if writer
        .send(WriterRequest::DiscardBackfillProgress {
            scope: scope.clone(),
            done,
        })
        .await
        .is_err()
    {
        return false;
    }
    match recv.await {
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

async fn scope_barrier_blocked(writer: &mpsc::Sender<WriterRequest>, scope: CursorScope) -> bool {
    let (done, recv) = oneshot::channel();
    if writer
        .send(WriterRequest::ScopeBarrierBlocked { scope, done })
        .await
        .is_err()
    {
        return false;
    }
    recv.await.unwrap_or(false)
}

/// First retry delay for a scope incarnation whose backfill did not
/// complete, and the ceiling the delay doubles towards.
pub(super) const BACKFILL_RETRY_INITIAL: Duration = Duration::from_secs(5);
pub(super) const BACKFILL_RETRY_CAP: Duration = Duration::from_secs(300);

/// Which scope incarnations the orchestrator's rescan still owes a
/// cold-start pass.
///
/// The distinction that matters here is settled versus attempted. An
/// incarnation leaves the rescan permanently only when its backfill
/// reached a durable conclusion: the plan ran to completion, or the
/// checkpoint store already carries a completion marker. A partition
/// failure or a checkpoint-store read failure is transient by
/// construction - the scope is deliberately left `Pending` so it can be
/// retried - so filtering it out for the rest of the attachment would
/// mean one flaky request costs the scope its entire backfill until the
/// account is reattached. Failed incarnations stay eligible and come
/// back on an exponential delay, which is what keeps a permanently
/// failing scope from re-walking on every rescan tick.
#[derive(Default)]
pub(super) struct BackfillScan {
    /// Incarnations that reached a durable conclusion, each recorded with the
    /// scope's undelivered watermark AS OF the settle.
    ///
    /// The watermark is what makes the settle revocable, and it has to be: the
    /// completion marker is published and the incarnation settled in the same
    /// step, while the loss that invalidates them can land afterwards. A
    /// receiver holding an earlier page departs, the sweep strands it, the ack
    /// writer refuses the marker - and nothing in memory knew, so the scope was
    /// never re-walked for the rest of the attachment. Comparing this reading on
    /// every rescan is what reopens it, using the same evidence the writer acted
    /// on rather than a second notification path.
    settled: HashMap<(CursorScope, u64), u64>,
    /// Fusion-owned scopes whose cold-start incarnation the fusion
    /// worker already published. Only the first sighting is fusion's;
    /// a later incarnation of the same scope is a genuine
    /// re-establishment and gets its own pass.
    fusion_skipped: HashSet<CursorScope>,
    /// Failure count plus earliest next attempt, per incarnation.
    failed: HashMap<(CursorScope, u64), (u32, tokio::time::Instant)>,
    /// The undelivered reading each incarnation's walk BEGAN under, recorded by
    /// [`BackfillScan::begin_walk`].
    ///
    /// The settle baseline is taken from here rather than from a number the call
    /// site supplies, and that is the whole point: a caller reading the watermark
    /// afresh at settle time can adopt a loss it never checked - a receiver
    /// departing between the walk's own comparison and the settle makes the
    /// writer refuse the marker and discard the rows on exactly the event the
    /// rescan then records as the baseline, so it compares that reading against
    /// itself for ever and never reopens. Owning the reading here makes the wrong
    /// one unavailable rather than merely discouraged.
    baseline: HashMap<(CursorScope, u64), u64>,
    /// Incarnations whose last attempt published pages that reached nobody.
    ///
    /// Their durable checkpoint is no longer a safe resume position: a
    /// replacement consumer legitimately acknowledges the windows it DID
    /// receive, and those sit past the pages it did not, so a positional resume
    /// walks over the hole and settles the scope having never re-offered them.
    /// The next attempt starts the walk over instead.
    lost_pages: HashSet<(CursorScope, u64)>,
    /// Incarnations whose walk was NOT started because a scope-local throttle
    /// (a key the account declares for this scope alone) was pending, with the
    /// instant that hold expires. Kept apart from `failed`: nothing failed, so
    /// the retry ramp must not grow, and the scope is due again exactly when
    /// its hold lifts rather than on the next exponential step.
    held: HashMap<(CursorScope, u64), tokio::time::Instant>,
}

impl BackfillScan {
    /// Do not start this incarnation's walk before `until`: a scope-local
    /// throttle holds it. The other scopes of the scan carry on meanwhile.
    pub(super) fn defer_until(&mut self, key: (CursorScope, u64), until: tokio::time::Instant) {
        self.held.insert(key, until);
    }

    pub(super) fn select(
        &mut self,
        available: Vec<(CursorScope, u64)>,
        fusion_owned: &HashSet<CursorScope>,
        now: tokio::time::Instant,
        coverage: &PendingCoverage,
    ) -> Vec<(CursorScope, u64)> {
        // Forget everything about incarnations the registry no longer carries.
        // A scope that is deleted, re-established, or simply never returns leaves
        // a retry deadline, a lost-pages mark and a baseline behind it otherwise,
        // one set per incarnation, for the life of the attachment - and the
        // `continue` below, which drops an incarnation that vanished during the
        // subscriber wait, adds one every time it fires. `settled` is deliberately
        // NOT pruned: it is the memory that stops a concluded incarnation being
        // walked again, and incarnation numbers only ever move forward, so an
        // entry there can never be mistaken for a later one.
        let live: HashSet<(CursorScope, u64)> = available.iter().cloned().collect();
        self.baseline.retain(|key, _| live.contains(key));
        self.failed.retain(|key, _| live.contains(key));
        self.lost_pages.retain(|key| live.contains(key));
        self.held
            .retain(|key, until| live.contains(key) && *until > now);
        let mut pending = Vec::new();
        for (scope, incarnation) in available {
            let key = (scope, incarnation);
            if let Some(settled_at) = self.settled.get(&key).copied() {
                if coverage.undelivered_watermark(&key.0) == settled_at {
                    continue;
                }
                // A page of that walk reached nobody AFTER it settled, so the
                // conclusion it settled on is void: the ack writer refuses the
                // marker on the same evidence and drops the rows, and this is
                // the half that lets the walk actually happen again.
                tracing::warn!(
                    target: "bifrost.sync.backfill",
                    scope = ?key.0,
                    "a page of a settled backfill walk has since reached no consumer; \
                     reopening the incarnation so it is walked again"
                );
                self.settled.remove(&key);
                self.lost_pages.insert(key.clone());
            }
            // A loss recorded against an incarnation whose last attempt FAILED,
            // after that attempt's own comparison had already been made.
            //
            // The settled arm above reopens a concluded walk on the same
            // evidence; this is the other half, and without it the request the
            // loss raised has no repair to reach. `note_lost_pages` runs at a
            // walk's END, so a departure sweep landing after a transient failure
            // is seen by nobody: the next attempt RESUMES (its baseline is the
            // moved reading, so it judges itself whole), earns a durable marker,
            // and the discard request sits outstanding until `detach` acts on it
            // and deletes the marker that walk legitimately earned - a full
            // re-walk on the next attach, once per flaky partition. Marking the
            // incarnation here makes the retry start from scratch, and its
            // pre-retry discard is what settles the request.
            if let Some(baseline) = self.baseline.get(&key).copied()
                && coverage.undelivered_watermark(&key.0) != baseline
                && self.lost_pages.insert(key.clone())
            {
                tracing::warn!(
                    target: "bifrost.sync.backfill",
                    scope = ?key.0,
                    "a page reached no consumer after this incarnation's last attempt ended; \
                     its next walk starts from the beginning"
                );
            }
            if fusion_owned.contains(&key.0)
                && !self.failed.contains_key(&key)
                && self.fusion_skipped.insert(key.0.clone())
            {
                // Fusion owns this incarnation's cold-start inventory;
                // walking it here would double-publish the same scope.
                let watermark = coverage.undelivered_watermark(&key.0);
                self.settled.insert(key, watermark);
                continue;
            }
            if self.failed.get(&key).is_some_and(|(_, at)| *at > now) {
                continue;
            }
            if self.held.contains_key(&key) {
                continue;
            }
            pending.push(key);
        }
        pending
    }

    /// The incarnation reached a durable conclusion with no walk of our
    /// own (completion marker already present, or fusion owns it).
    ///
    /// The settle is recorded against the reading this incarnation's walk BEGAN
    /// under - the one every check of this pass was made against - so a loss
    /// that lands after those checks reopens it on the next rescan. An unknown
    /// baseline reads as 0, which reopens on the first loss the scope ever
    /// records: the conservative direction, costing a re-walk.
    pub(super) fn settled(&mut self, key: (CursorScope, u64)) {
        self.failed.remove(&key);
        self.lost_pages.remove(&key);
        let baseline = self.baseline.remove(&key).unwrap_or(0);
        self.settled.insert(key, baseline);
    }

    /// The last attempt on this incarnation published pages that reached
    /// nobody, so its durable checkpoint may not be used as a resume position.
    pub(super) fn note_lost_pages(&mut self, key: (CursorScope, u64)) {
        self.lost_pages.insert(key);
    }

    /// Take this incarnation's baseline as its walk BEGINS, and hand it back.
    ///
    /// At the start of the walk, not when the batch was selected: one rescan
    /// hands out several scopes and they are walked one after another, so a
    /// baseline taken for all of them up front is stale for every scope but the
    /// first. A sweep during scope X's walk moves scope Y's reading too - Y's
    /// pages are swept with X's when a receiver departs - and Y would then judge
    /// itself holed on a loss that happened before it published anything, and
    /// re-walk from scratch for nothing.
    ///
    /// The scan still OWNS the reading, which is the point: the settle is
    /// recorded against the value taken here, so no call site can settle against
    /// a number nothing validated.
    pub(super) fn begin_walk(
        &mut self,
        key: &(CursorScope, u64),
        coverage: &PendingCoverage,
    ) -> u64 {
        let watermark = coverage.undelivered_watermark(&key.0);
        self.baseline.insert(key.clone(), watermark);
        watermark
    }

    /// The reading this incarnation's walk began under, which is the ONE baseline
    /// it is judged against. Unknown reads as 0, so any loss the scope has ever
    /// recorded counts against it: the conservative direction, costing a re-walk.
    ///
    /// Production reads it through `begin_walk`'s return value; this exists so a
    /// test can observe that a departed incarnation's entry is pruned.
    #[cfg(test)]
    pub(super) fn baseline_for(&self, key: &(CursorScope, u64)) -> u64 {
        self.baseline.get(key).copied().unwrap_or(0)
    }

    /// Must the next walk of this incarnation start from the beginning rather
    /// than from its durable checkpoint?
    pub(super) fn restarts_from_scratch(&self, key: &(CursorScope, u64)) -> bool {
        self.lost_pages.contains(key)
    }

    /// Record the outcome of a walk we actually ran.
    pub(super) fn record_attempt(&mut self, key: (CursorScope, u64), completed: bool) {
        if completed {
            self.settled(key);
            return;
        }
        // The baseline STAYS on a failed attempt, and that is the point: it is
        // the reading the attempt was judged against, so a loss recorded after
        // the walk ended - a departure sweep firing between attempts - is still
        // detectable at the next rescan (`select`). Dropping it here made the
        // scope's own history unreadable and left the discard request that loss
        // raised with nothing to repair it. It is pruned with the rest when the
        // incarnation leaves the registry, and overwritten by `begin_walk` when
        // the retry starts.
        let failures = self.failed.get(&key).map_or(0, |(count, _)| *count) + 1;
        let delay = BACKFILL_RETRY_INITIAL
            .saturating_mul(1_u32 << failures.min(6).saturating_sub(1))
            .min(BACKFILL_RETRY_CAP);
        self.failed
            .insert(key, (failures, tokio::time::Instant::now() + delay));
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_backfill_partition_at_boundary(
    account: &Arc<ArcSwap<Arc<dyn Account>>>,
    account_id: &AccountId,
    scope: CursorScope,
    partition: InventoryPartition,
    live: &Arc<LiveSupersedes>,
    changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    control: &SyncControl,
    shutdown: &CancellationToken,
    throttles: &std::sync::Mutex<crate::recovery::ThrottleBucket>,
    coverage: &Arc<PendingCoverage>,
    writer_tx: &mpsc::Sender<WriterRequest>,
    lane: &LaneGate,
    delivery: &crate::multiplexer::ChangeDelivery,
    fence_at_walk_start: u64,
    // `Some` on the walk's FIRST partition: a scope-local hold found before
    // it is read is reported here (and `None` returned) instead of slept out,
    // so the orchestrator defers the scope while no inventory has been
    // walked. Later partitions pass `None` and wait inline - the walk is
    // under way and must finish against the same budget.
    mut defer_first: Option<&mut Option<std::time::Duration>>,
) -> Option<Result<crate::backfill::BackfillPartitionOutcome, Error>> {
    // One generation per partition pass, so a re-walk's proof is ordered after
    // the debt an earlier pass raised.
    let generation = coverage.next_generation();
    loop {
        if !control.wait_until_running(shutdown).await {
            return None;
        }
        // Honor any throttle deadline this scope's walk consumes (account-wide
        // holds plus the keys the account declares for the scope) before
        // walking the partition: cold-start hydration is the heaviest request
        // lane the engine drives, so barreling through a provider Retry-After
        // that paused the polls would defeat the pause. Re-checked after
        // waking, and boundary-checked again, since a pause or a longer
        // deadline can land mid-sleep.
        let keys = crate::recovery::declared_keys(
            account.load_full().as_ref().as_ref(),
            account_id,
            bifrost_types::ThrottleWork::Scope(&scope),
        );
        if let Some(wait) = crate::recovery::throttle_wait(throttles, account_id, &keys) {
            if let Some(held) = defer_first.as_deref_mut() {
                // Before the first partition only the ACCOUNT-WIDE part is
                // slept, never the combined wait; a scope-local hold left
                // over is handed back for deferral.
                match crate::recovery::account_throttle_wait(throttles, account_id) {
                    None => {
                        *held = Some(wait);
                        return None;
                    }
                    Some(account_wait) => {
                        tokio::select! {
                            () = shutdown.cancelled() => return None,
                            () = tokio::time::sleep(account_wait) => {}
                        }
                        continue;
                    }
                }
            }
            tracing::debug!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                wait_secs = wait.as_secs(),
                "backfill partition deferred by shared throttle deadline"
            );
            tokio::select! {
                () = shutdown.cancelled() => return None,
                () = tokio::time::sleep(wait) => {}
            }
            continue;
        }
        // Admission is taken THROUGH the lane gate, which owns it for the whole
        // pass. That is not indirection for its own sake: `run_partition` parks
        // on the bound in the middle of the pass, and a producer parked while
        // holding the account's sync permit starves the live lane on a minimal
        // budget (`per_account = 2` leaves one sync permit once the mutation
        // share is taken). The gate drops the permit before parking and re-takes
        // it on the wake, so a parked backfill holds nothing polling or push
        // reconciliation needs.
        match lane.admit().await {
            Ok(()) => {}
            Err(crate::engine::lane::WaitFailed::ShuttingDown) => return None,
            Err(crate::engine::lane::WaitFailed::Refused(error)) => return Some(Err(error)),
        }
        // Admission can park, and a hold recorded meanwhile predates this
        // partition. Release the admission rather than carry it across the
        // wait, and go back round. The keys are asked of `current`, the very
        // account snapshot the partition runs against, not the ones computed
        // before the waits: a reopen can change them.
        let current = account.load_full();
        let final_keys = crate::recovery::declared_keys(
            current.as_ref().as_ref(),
            account_id,
            bifrost_types::ThrottleWork::Scope(&scope),
        );
        if let Some(wait) = crate::recovery::throttle_wait(throttles, account_id, &final_keys) {
            lane.release_admission();
            if let Some(held) = defer_first.as_deref_mut()
                && crate::recovery::account_throttle_wait(throttles, account_id).is_none()
            {
                *held = Some(wait);
                return None;
            }
            continue;
        }
        // RE-CHECK THE WALK'S FENCE, with admission in hand and before a single
        // page has been read. Every step above this line can park indefinitely -
        // a pause, a throttle deadline, the scheduler's admission queue - and a
        // scope reset can close inside any of them. `run_partition`'s own
        // revalidation cannot see that: its reading is taken once a page is in
        // hand, which is AFTER the reset, so it compares the replacement
        // incarnation's fence with itself and lets the page through. The pages
        // this walk would go on to publish are minted above the fence, a
        // consumer acknowledges them in good faith, and the durable rows a
        // `delete_backfill: true` reset deleted come back - for a folder the
        // provider has deleted, that resurrects the completion marker that makes
        // a folder recreated under the same id skip its cold-start walk.
        //
        // Failing the partition is the right shape: the driver ends the walk,
        // withholds the completion marker, and leaves the scope Pending. If the
        // scope is still in the registry it is re-walked from its replacement
        // incarnation's beginning; if the reset removed it, nothing walks it,
        // which is what a deleted folder should get.
        if coverage.scope_fence(&scope) != fence_at_walk_start {
            lane.release_admission();
            tracing::warn!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                "scope was reset between two partitions of a backfill walk; ending the \
                 walk rather than publishing its later partitions against rows the \
                 reset deleted"
            );
            return Some(Err(Error::Other(
                "scope reset between two partitions of the backfill walk".into(),
            )));
        }
        let result = BackfillRunner::run_partition(
            current.as_ref().as_ref(),
            scope.clone(),
            partition.clone(),
            live.as_ref(),
            changes_tx.clone(),
            crate::cursor::ENGINE_VERSION,
            Some(control),
            Some(coverage),
            Some(writer_tx),
            generation,
            Some(lane),
            delivery,
        )
        .await;
        lane.release_admission();
        if matches!(result, Err(Error::Paused)) {
            continue;
        }
        return Some(result);
    }
}

/// Resume decision for an open-ended page scope, derived purely from the
/// durably-persisted (consumer-acked) backfill checkpoint.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum OpenPagesResume {
    /// Inventory was exhausted on a prior run; do not walk at all.
    Skip,
    /// Begin (or resume) the page walk at this position.
    ResumeFrom(u32),
}

/// True if the persisted checkpoint is the durable completion marker: a
/// prior run walked the whole scope to exhaustion and the consumer acked
/// it. The single skip-on-complete signal shared by fixed and open-ended
/// plans; `get_backfill` only ever returns consumer-acked checkpoints, so
/// a marker means every page the consumer needed was durably persisted.
pub(super) fn backfill_complete_recorded(checkpoint: Option<&BackfillCheckpoint>) -> bool {
    checkpoint
        .is_some_and(|ck| crate::backfill::partitioner::is_completion_partition(&ck.partition))
}

/// Map the persisted backfill checkpoint to a resume decision.
///
/// - The completion sentinel means a prior run reached exhaustion and the
///   consumer acked it: skip entirely.
/// - Any other `page:F:T` resumes at `T`. A SHORT page (`items_done <
///   T - F`) is deliberately NOT read as exhaustion: only a partition's
///   declared `PartitionEnd::Exhausted` ends a walk, and a partition may
///   legitimately emit fewer entries than its window width while the
///   scope still has results - ids that vanished between listing and
///   hydration, or objects dropped for arriving without an id. Only the
///   completion marker proves exhaustion; a final page without one costs
///   a single probe window on re-attach, whereas skipping on it would
///   silently drop every message past the window.
/// - No checkpoint, or an unrecognised partition kind, starts fresh at 0.
///
/// Resume never skips a window the consumer has not durably persisted,
/// because `get_backfill` only ever returns consumer-acked checkpoints.
pub(super) fn open_pages_resume(checkpoint: Option<&BackfillCheckpoint>) -> OpenPagesResume {
    if backfill_complete_recorded(checkpoint) {
        return OpenPagesResume::Skip;
    }
    let Some(checkpoint) = checkpoint else {
        return OpenPagesResume::ResumeFrom(0);
    };
    if let Some((_, to)) = crate::backfill::partitioner::parse_page_partition(&checkpoint.partition)
    {
        return OpenPagesResume::ResumeFrom(to);
    }
    OpenPagesResume::ResumeFrom(0)
}

/// Broadcast a durable backfill-completion marker for a scope. The marker
/// is a synthetic empty `Batch` carrying a `BackfillCheckpoint` on the
/// `completion_partition` sentinel key; it flows through the consumer-ack
/// path exactly like a page batch, so the store only records completion
/// after the consumer has durably persisted every page. A crash before the
/// marker is acked therefore re-walks rather than recording a false "done".
///
/// `items_done` is set one past `total_seen` so the marker strictly wins
/// `get_backfill`'s "latest by items_done" query regardless of how a store
/// breaks ties: every per-partition checkpoint records its observed count
/// (`<= total_seen`), so `total_seen + 1` is guaranteed larger and the
/// marker is the row returned on re-attach. The honest total rides in
/// `items_estimated`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn emit_backfill_complete(
    changes_tx: Option<&broadcast::Sender<MultiplexerEvent>>,
    scope: &CursorScope,
    total_seen: u64,
    control: &SyncControl,
    shutdown: &CancellationToken,
    lane: &LaneGate,
    delivery: &crate::multiplexer::ChangeDelivery,
    undelivered_before: u64,
    fence_at_walk_start: u64,
) -> MarkerOutcome {
    if changes_tx.is_none() {
        return MarkerOutcome::Published;
    }
    let _activity = loop {
        if !control.wait_until_running(shutdown).await {
            return MarkerOutcome::ShuttingDown;
        }
        if let Some(activity) = control.begin_activity() {
            break activity;
        }
    };
    let marker = BackfillCheckpoint {
        scope: scope.clone(),
        partition: crate::backfill::partitioner::completion_partition(),
        progress_marker: None,
        progress: BackfillProgress {
            items_done: total_seen.saturating_add(1),
            items_estimated: Some(total_seen),
        },
        envelope_version: crate::cursor::ENGINE_VERSION,
    };
    let batch: Batch<bifrost_types::Change> = Batch {
        items: Vec::new(),
        page_boundary: PageBoundary::Final,
        server_latency: Duration::ZERO,
        bytes_in: 0,
        checkpoint: Some(Checkpoint::Backfill(marker.clone())),
    };
    let expected = Checkpoint::Backfill(marker);
    // The sentinel is a backfill publication like any other and waits on the
    // same bound. It takes NO scheduler admission, and must not: it does no wire
    // work, and it is emitted from here - after the partition pass has already
    // handed its permit back - so a wait that re-acquired admission on the wake
    // would leave that permit held for the rest of the attachment, starving
    // polling, push reconciliation and the next scope's barrier query on a
    // one-permit budget.
    // The fence to beat is the one this WALK started under, not one read here.
    // A reset can close between the walk's last page and the provider stream's
    // terminal `Done` - the stream is not cancelled, it just ends - and a
    // snapshot taken at this point is already the replacement incarnation's, so
    // the comparison below would be a walk checking itself against a fence it
    // adopted from a scope it never enumerated.
    let fence_before = fence_at_walk_start;
    if lane.coverage().scope_fence(scope) != fence_before {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            scope = ?scope,
            "the scope was reset during this walk; withholding its completion marker"
        );
        return MarkerOutcome::Withheld;
    }
    if lane.wait_for_capacity().await.is_err() {
        return MarkerOutcome::ShuttingDown;
    }
    // Same revalidation the page path does, and the stakes are higher here: a
    // completion marker published against a reset scope suppresses the
    // replacement incarnation's entire inventory walk on the next attach.
    if lane.coverage().scope_fence(scope) != fence_before {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            scope = ?scope,
            "scope was reset while its completion marker waited on the bound; \
             withholding the marker"
        );
        return MarkerOutcome::Withheld;
    }
    // And re-validate the WALK, not only the scope's rows. The wait above is
    // indefinite, and the caller's "every page of this walk reached somebody"
    // reading was taken before it: a final page still unacknowledged when the
    // marker parked can be swept undelivered by a departing receiver, whose
    // sweep is also what frees the capacity this wait is parked on. Publishing
    // on that wake records a durable completion for a walk that lost a page one
    // instant earlier, and the replacement consumer acknowledges it in good
    // faith. The page is gone for the life of the attachment either way; what
    // this refuses is making it permanent.
    if lane.coverage().undelivered_watermark(scope) != undelivered_before {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            scope = ?scope,
            "a page of this walk reached no consumer while its completion marker waited \
             on the bound; withholding the marker so the scope is re-walked"
        );
        return MarkerOutcome::Withheld;
    }
    // Register before publishing so a fast consumer ack cannot land
    // before the entry exists and leave it outstanding forever.
    // Completion eligibility stays tied to THIS walk right up to the
    // acknowledgement, so the reading is stamped into the publication's RECEIPT
    // as it is minted. The checks above are point-in-time and the marker is
    // durable: with spare capacity it reaches every receiver at once, and a
    // receiver still holding an earlier unacknowledged page can depart
    // afterwards - the sweep retires that page while a replacement, which had
    // the marker in its ring all along, acknowledges it. The writer compares
    // this reading against the scope's watermark again before it writes.
    let publication = control.publish_walk_marker(expected.clone(), undelivered_before);
    let event = MultiplexerEvent {
        scope: scope.clone(),
        event: Arc::new(SyncEvent::Batch(batch)),
        checkpoint: Some(expected.clone()),
        publication: Some(publication.clone()),
    };
    // Send and stamp as one step, exactly as the page path does.
    if !delivery.publish_backfill(event, lane.coverage(), Some(&publication)) {
        lane.coverage().note_undelivered(scope, 1);
        control.retire_publication(publication);
    }
    MarkerOutcome::Published
}

/// What became of a scope's completion marker.
///
/// Three answers rather than two, and the third is the point: an earlier
/// revision returned one `false` for "the account is going away" and for "this
/// walk was invalidated while the marker waited", and the caller could only read
/// it as shutdown - so a single scope reset landing inside one marker's capacity
/// wait retired the whole orchestrator, abandoning every later scope and every
/// later rescan until the account was reattached.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum MarkerOutcome {
    /// Published, or there was no channel to publish on.
    Published,
    /// Deliberately not published: the scope was reset, or a page of this walk
    /// reached nobody, while the marker waited. The scope is left unsettled and
    /// the orchestrator carries on with the next one.
    Withheld,
    /// The account is tearing down.
    ShuttingDown,
}

pub(super) enum BackfillPlan {
    Fixed(Vec<InventoryPartition>),
    OpenPages { chunk: u32 },
}

/// What the orchestrator does with a scope incarnation once its durable
/// checkpoint has been read back.
///
/// The two plan shapes differ ONLY here. Both drive the same
/// `ScopeWalkDriver` afterwards, so folding their resume decisions into
/// one enum is what lets the walk itself be written once - barrier
/// handling, completion-marker withholding and registry bookkeeping
/// included.
pub(super) enum ScopeResume {
    /// Durable evidence that a prior run finished this scope. Do not walk.
    Skip,
    /// Walk, starting from wherever the plan says to start.
    Walk(crate::backfill::ScopeWalkDriver),
}

/// Log wording for a walk, so collapsing the two plan arms into one loop
/// does not collapse their operator-facing messages into one another.
struct WalkLabels {
    resume_read_failed: &'static str,
    unresolved_coverage: &'static str,
    barrier_stop: &'static str,
    partition_failed: &'static str,
}

const FIXED_LABELS: WalkLabels = WalkLabels {
    resume_read_failed: "backfill resume read failed; re-walking all partitions",
    unresolved_coverage: "backfill partition completed with unresolved coverage; withholding the \
                          completion marker",
    barrier_stop: "backfill stopped at a barrier; refusing to walk any further partition of this \
                   scope",
    partition_failed: "backfill partition failed; leaving scope Pending",
};

const OPEN_PAGES_LABELS: WalkLabels = WalkLabels {
    resume_read_failed: "backfill resume read failed; re-walking from page 0",
    unresolved_coverage: "backfill page completed with unresolved coverage; withholding the \
                          completion marker",
    barrier_stop: "backfill stopped at a barrier; refusing to walk any further page window of \
                   this scope",
    partition_failed: "backfill page partition failed; leaving scope Pending",
};

impl BackfillPlan {
    fn labels(&self) -> &'static WalkLabels {
        match self {
            BackfillPlan::Fixed(_) => &FIXED_LABELS,
            BackfillPlan::OpenPages { .. } => &OPEN_PAGES_LABELS,
        }
    }

    /// Turn the durably-persisted (consumer-acked) checkpoint into a
    /// resume decision.
    ///
    /// A fixed plan's partitions are a known finite set with no positional
    /// "resume from here", so its durable signal is binary: the completion
    /// marker is present (skip the whole plan) or it is not (walk every
    /// partition; re-emitting acked pages is idempotent, so a crash
    /// mid-plan simply re-walks). An open-ended page walk resumes after the
    /// furthest durably-checkpointed window instead of re-paginating from
    /// page 0 on every re-attach.
    pub(super) fn resume(self, stored: Option<&BackfillCheckpoint>) -> ScopeResume {
        match self {
            BackfillPlan::Fixed(partitions) => {
                if backfill_complete_recorded(stored) {
                    ScopeResume::Skip
                } else {
                    ScopeResume::Walk(crate::backfill::ScopeWalkDriver::fixed(partitions))
                }
            }
            BackfillPlan::OpenPages { chunk } => match open_pages_resume(stored) {
                OpenPagesResume::Skip => ScopeResume::Skip,
                OpenPagesResume::ResumeFrom(from) => {
                    ScopeResume::Walk(crate::backfill::ScopeWalkDriver::open_pages(from, chunk))
                }
            },
        }
    }

    /// The resume decision to use when the checkpoint READ failed, as
    /// opposed to came back empty. Deliberately the same answer as an
    /// absent checkpoint: a store error proves nothing about coverage, and
    /// skipping on it would silently drop everything a prior run had not
    /// finished.
    pub(super) fn walk_from_scratch(self) -> ScopeResume {
        self.resume(None)
    }
}

fn backfill_plan_for(
    account: &dyn Account,
    scope: &CursorScope,
    config: BackfillConfig,
) -> BackfillPlan {
    match account.inventory_partitioning(scope) {
        InventoryPartitioning::Full => BackfillPlan::Fixed(vec![InventoryPartition::Full]),
        InventoryPartitioning::TimeWindowed => {
            let policy = BackfillPolicy::default();
            let plan = crate::backfill::partitioner::plan(&policy, jiff::Timestamp::now(), 0);
            BackfillPlan::Fixed(
                plan.partitions
                    .iter()
                    .map(crate::backfill::partitioner::inventory_partition_for)
                    .collect(),
            )
        }
        InventoryPartitioning::UidRange {
            max_uid: Some(max_uid),
        } => {
            let policy = BackfillPolicy {
                strategy: BackfillStrategy::UidRange {
                    chunk_size: config.uid_range_chunk,
                },
                clock_skew: std::time::Duration::ZERO,
            };
            let plan = crate::backfill::partitioner::plan(&policy, jiff::Timestamp::now(), max_uid);
            BackfillPlan::Fixed(
                plan.partitions
                    .iter()
                    .map(crate::backfill::partitioner::inventory_partition_for)
                    .collect(),
            )
        }
        InventoryPartitioning::UidRange { max_uid: None } => {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                scope = ?scope,
                "uid-range partitioning requested without max_uid; using full inventory pass"
            );
            BackfillPlan::Fixed(vec![InventoryPartition::Full])
        }
        InventoryPartitioning::PageCount {
            total: Some(total),
            page_size,
        } => {
            let policy = BackfillPolicy {
                strategy: BackfillStrategy::PageCount {
                    items_per_partition: page_size.unwrap_or(config.page_count_chunk).max(1),
                },
                clock_skew: std::time::Duration::ZERO,
            };
            let plan = crate::backfill::partitioner::plan(&policy, jiff::Timestamp::now(), total);
            BackfillPlan::Fixed(
                plan.partitions
                    .iter()
                    .map(crate::backfill::partitioner::inventory_partition_for)
                    .collect(),
            )
        }
        InventoryPartitioning::PageCount {
            total: None,
            page_size,
        } => BackfillPlan::OpenPages {
            chunk: page_size.unwrap_or(config.page_count_chunk).max(1),
        },
        _ => BackfillPlan::Fixed(vec![InventoryPartition::Full]),
    }
}
