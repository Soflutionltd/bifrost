//! The backfill orchestrator: scope rescan, resume planning, and the
//! per-partition walk.

use super::*;

/// The backfill-specific half of the orchestrator's wiring; everything
/// slot-wide rides in the [`SlotContext`].
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
        delivery,
        ..
    } = ctx;
    let BackfillWiring {
        live,
        registry,
        config,
        fusion_owned_scopes,
    } = wiring;
    let publisher = crate::multiplexer::PagePublisher::new(
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
                    if discard_backfill_progress(&writer_tx, &account_id, &scope).await {
                        coverage.take_backfill_discard(&scope);
                    }
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
                let fence_at_walk_start = coverage.scope_fence(&scope);
                // Planning and writer barrier reads do no wire work. They
                // need no scheduler budget; partition admission below owns the
                // start gate and rechecks this original walk fence after waits.
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
                    match super::ack::WriterHandle::new(writer_tx.clone())
                        .get_backfill(scope.clone())
                        .await
                    {
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
    // An unavailable writer cannot prove the scope unblocked. An enterprise
    // mail user expects inventory to stop rather than cross unknown debt.
    let (done, recv) = oneshot::channel();
    if writer
        .send(WriterRequest::ScopeBarrierBlocked { scope, done })
        .await
        .is_err()
    {
        return true;
    }
    recv.await.unwrap_or(true)
}

/// First retry delay for a scope incarnation whose backfill did not
/// complete, and the ceiling the delay doubles towards.
pub(super) const BACKFILL_RETRY_INITIAL: Duration = Duration::from_secs(5);
pub(super) const BACKFILL_RETRY_CAP: Duration = Duration::from_secs(300);
const BACKFILL_BACKOFF: bifrost_types::BackoffPolicy =
    bifrost_types::BackoffPolicy::new(BACKFILL_RETRY_INITIAL, BACKFILL_RETRY_CAP, false);

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
// Owns sequencing and totals, but no admission, activity guard or provider stream.
struct ParkedWalk {
    driver: crate::backfill::ScopeWalkDriver,
    pending: InventoryPartition,
    fence_at_walk_start: crate::cursor::ScopeFence,
    undelivered_before: crate::cursor::UndeliveredMark,
    labels: &'static WalkLabels,
    until: tokio::time::Instant,
}

type ScanKey = (CursorScope, crate::cursor::ScopeIncarnation);

/// Scheduling phases are exclusive; retry history and loss evidence survive
/// holds and parked partitions because neither starts a new attempt.
#[derive(Default)]
enum ScanPhase {
    #[default]
    Fresh,
    Running,
    Parked(Box<ParkedWalk>),
    Failed(tokio::time::Instant),
    Settled,
}

#[derive(Default)]
struct IncarnationState {
    phase: ScanPhase,
    baseline: Option<crate::cursor::UndeliveredMark>,
    lost_pages: bool,
    retry: Option<crate::backoff::ReconnectBackoff>,
}

impl IncarnationState {
    fn baseline(&self) -> crate::cursor::UndeliveredMark {
        self.baseline
            .unwrap_or_else(PendingCoverage::initial_undelivered_mark)
    }

    fn projection(&self, watermark: crate::cursor::UndeliveredMark) -> BackfillState {
        match &self.phase {
            ScanPhase::Settled if self.baseline() == watermark => BackfillState::Completed,
            ScanPhase::Running => BackfillState::Running,
            _ => BackfillState::Pending,
        }
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        match &self.phase {
            ScanPhase::Parked(walk) => Some(walk.until),
            ScanPhase::Failed(until) => Some(*until),
            _ => None,
        }
    }
}

#[derive(Default)]
struct ScanState {
    incarnations: HashMap<ScanKey, IncarnationState>,
    // This is attachment history, not scheduling state. Only the first sighting
    // of a fusion-owned scope is already inventoried by fusion; pruning it on
    // deletion would incorrectly skip the replacement incarnation too.
    fusion_skipped: HashSet<CursorScope>,
}

#[derive(Default)]
pub(super) struct BackfillScan {
    state: Arc<std::sync::Mutex<ScanState>>,
}

impl BackfillScan {
    // The registry retains the same state until detach; it owns no second
    // completion map and cannot independently change a scan verdict.
    fn publish(
        &self,
        registry: &BackfillRegistry,
        account: AccountId,
        cursors: Arc<crate::cursor::CursorRegistry>,
        coverage: Arc<PendingCoverage>,
    ) {
        let state = Arc::clone(&self.state);
        registry.observe(
            account,
            Arc::new(move |scope, expected| {
                let key = cursors.all_scope_incarnations().into_iter().find(
                    |(candidate, incarnation)| {
                        candidate == scope
                            && expected.is_none_or(|expected| expected == *incarnation)
                    },
                )?;
                let state = state.lock().expect("backfill scan poisoned");
                // Newly established scopes are pending even before the next scan.
                let projected = state
                    .incarnations
                    .get(&key)
                    .map_or(BackfillState::Pending, |entry| {
                        entry.projection(coverage.undelivered_watermark(scope))
                    });
                // Topology may have changed while waiting for the scan lock. Never
                // publish the old incarnation's completion as its replacement's.
                cursors
                    .all_scope_incarnations()
                    .contains(&key)
                    .then_some(projected)
            }),
        );
    }

    fn next_wake(&self) -> tokio::time::Instant {
        let state = self.state.lock().expect("backfill scan poisoned");
        let now = tokio::time::Instant::now();
        if state
            .incarnations
            .values()
            .any(|entry| matches!(&entry.phase, ScanPhase::Parked(walk) if walk.until <= now))
        {
            return now;
        }
        state
            .incarnations
            .values()
            .filter_map(IncarnationState::deadline)
            .filter(|until| *until > now)
            .fold(now + Duration::from_secs(1), std::cmp::min)
    }

    fn take_parked(&mut self, key: &ScanKey) -> Option<ParkedWalk> {
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let entry = state.incarnations.get_mut(key)?;
        if !matches!(entry.phase, ScanPhase::Parked(_)) {
            return None;
        }
        match std::mem::replace(&mut entry.phase, ScanPhase::Running) {
            ScanPhase::Parked(walk) => Some(*walk),
            _ => unreachable!(),
        }
    }

    fn park(&mut self, key: ScanKey, walk: ParkedWalk) {
        self.state
            .lock()
            .expect("backfill scan poisoned")
            .incarnations
            .entry(key)
            .or_default()
            .phase = ScanPhase::Parked(Box::new(walk));
    }

    pub(super) fn select(
        &mut self,
        available: Vec<ScanKey>,
        fusion_owned: &HashSet<CursorScope>,
        now: tokio::time::Instant,
        coverage: &PendingCoverage,
    ) -> Vec<ScanKey> {
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let live: HashSet<ScanKey> = available.iter().cloned().collect();
        // Incarnation identities are never reused: departed settled entries
        // cannot prevent any future walk, and must leave with the other residue.
        state.incarnations.retain(|key, _| live.contains(key));
        let mut pending = Vec::new();
        for key in available {
            let ScanState {
                incarnations,
                fusion_skipped,
            } = &mut *state;
            let entry = incarnations.entry(key.clone()).or_default();
            if let ScanPhase::Parked(walk) = &entry.phase {
                if walk.until <= now {
                    pending.push(key);
                }
                continue;
            }
            let watermark = coverage.undelivered_watermark(&key.0);
            if matches!(entry.phase, ScanPhase::Settled) {
                if entry.baseline() == watermark {
                    continue;
                }
                entry.phase = ScanPhase::Fresh;
                entry.lost_pages = true;
                tracing::warn!(target: "bifrost.sync.backfill", scope = ?key.0,
                    "a settled backfill walk lost pages; reopening the incarnation");
            }
            // A failed attempt retains its original baseline too: a receiver
            // can depart after failure, before the next attempt begins.
            if entry.baseline.is_some_and(|baseline| baseline != watermark) && !entry.lost_pages {
                entry.lost_pages = true;
                tracing::warn!(target: "bifrost.sync.backfill", scope = ?key.0,
                    "a previous backfill attempt lost pages; restarting from the beginning");
            }
            if fusion_owned.contains(&key.0)
                && entry.retry.is_none()
                && fusion_skipped.insert(key.0.clone())
            {
                entry.baseline = Some(watermark);
                entry.phase = ScanPhase::Settled;
                entry.lost_pages = false;
                continue;
            }
            if entry.deadline().is_some_and(|until| until > now) {
                continue;
            }
            pending.push(key);
        }
        pending
    }

    /// Settle against the walk's original baseline, never a fresh loss reading.
    /// An unknown baseline is conservative and reopens on any recorded loss.
    pub(super) fn settled(&mut self, key: ScanKey) {
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let entry = state.incarnations.entry(key).or_default();
        entry.phase = ScanPhase::Settled;
        entry.retry = None;
        entry.lost_pages = false;
    }

    pub(super) fn note_lost_pages(&mut self, key: ScanKey) {
        self.state
            .lock()
            .expect("backfill scan poisoned")
            .incarnations
            .entry(key)
            .or_default()
            .lost_pages = true;
    }

    /// Snapshot at walk start, rather than selection of a batch of scopes.
    /// Loss during an earlier scope must not count against this scope's walk.
    pub(super) fn begin_walk(
        &mut self,
        key: &ScanKey,
        coverage: &PendingCoverage,
    ) -> crate::cursor::UndeliveredMark {
        let watermark = coverage.undelivered_watermark(&key.0);
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let entry = state.incarnations.entry(key.clone()).or_default();
        entry.baseline = Some(watermark);
        entry.phase = ScanPhase::Running;
        watermark
    }

    #[cfg(test)]
    pub(super) fn baseline_for(&self, key: &ScanKey) -> crate::cursor::UndeliveredMark {
        self.state
            .lock()
            .expect("backfill scan poisoned")
            .incarnations
            .get(key)
            .map_or_else(
                PendingCoverage::initial_undelivered_mark,
                IncarnationState::baseline,
            )
    }

    pub(super) fn restarts_from_scratch(&self, key: &ScanKey) -> bool {
        self.state
            .lock()
            .expect("backfill scan poisoned")
            .incarnations
            .get(key)
            .is_some_and(|entry| entry.lost_pages)
    }

    pub(super) fn record_attempt(&mut self, key: ScanKey, completed: bool) {
        if completed {
            self.settled(key);
            return;
        }
        let mut state = self.state.lock().expect("backfill scan poisoned");
        let entry = state.incarnations.entry(key).or_default();
        let backoff = entry
            .retry
            .get_or_insert_with(|| crate::backoff::ReconnectBackoff::new(BACKFILL_BACKOFF));
        let delay = backoff.record_failure(crate::backoff::jitter_seed());
        entry.phase = ScanPhase::Failed(tokio::time::Instant::now() + delay);
    }
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
    throttles: &crate::recovery::ThrottleBucket,
    coverage: &Arc<PendingCoverage>,
    writer_tx: &mpsc::Sender<WriterRequest>,
    bound: &BackfillBound,
    delivery: &Arc<crate::multiplexer::ChangeDelivery>,
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
            scheduler: &bound.admission().scheduler,
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
        let _backfill_permit = bound.hold_admission(permit);
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
            bound.release_admission();
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
        let publisher = crate::multiplexer::PagePublisher::new(
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
            Some(bound),
            fence_at_walk_start,
        )
        .await;
        bound.release_admission();
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
    checkpoint.is_some_and(|ck| {
        // Empty plans used to emit the legacy marker with zero seen + one.
        // Larger legacy markers prove a nonempty walk and remain valid. An
        // enterprise mail user expects a possibly skipped empty folder to be
        // checked once; new empty inventories earn the versioned marker.
        crate::backfill::partitioner::BackfillKey::decode(&ck.partition)
            .proves_completion(ck.progress.items_done)
    })
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
/// `backfill_checkpoint_is_later` selects completion by durable key kind.
/// `items_done` still sits one past `total_seen`: a consumer-written
/// `CheckpointStore` that predates that helper and follows the earlier
/// "greatest items_done, then page bound, then bytes" contract would
/// otherwise tie the marker with the last page row and pick the page
/// (`page:` sorts after `complete:`), re-walking a finished scope on every
/// attach. The honest total rides in `items_estimated`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn emit_backfill_complete(
    scope: &CursorScope,
    total_seen: crate::backfill::scope_walk::WalkTotal,
    publisher: &crate::multiplexer::PagePublisher,
    bound: &BackfillBound,
    undelivered_before: crate::cursor::UndeliveredMark,
    fence_at_walk_start: crate::cursor::ScopeFence,
) -> MarkerOutcome {
    let control = &publisher.control;
    let shutdown = &publisher.shutdown;
    if !publisher.shares_bound_ledger(bound) {
        return MarkerOutcome::Withheld;
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
        progress: total_seen.completion_progress(),
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(crate::cursor::ENGINE_VERSION),
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
    if bound.coverage().scope_fence(scope) != fence_before {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            scope = ?scope,
            "the scope was reset during this walk; withholding its completion marker"
        );
        return MarkerOutcome::Withheld;
    }
    if bound.wait_for_capacity().await.is_err() {
        return MarkerOutcome::ShuttingDown;
    }
    // Same revalidation the page path does, and the stakes are higher here: a
    // completion marker published against a reset scope suppresses the
    // replacement incarnation's entire inventory walk on the next attach.
    if bound.coverage().scope_fence(scope) != fence_before {
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
    if bound.coverage().undelivered_watermark(scope) != undelivered_before {
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
    let event = MultiplexerEvent {
        scope: EventScope::Cursor(scope.clone()),
        event: Arc::new(SyncEvent::Batch(batch)),
        checkpoint: Some(expected),
        publication: None,
    };
    if !publisher.publish_marker(event, undelivered_before, fence_at_walk_start) {
        // `publish_marker` answers false both for a fence refusal at the mint
        // and for a marker that reached no receiver. A fence that no longer
        // equals the walk's is the refusal (the fence only moves forward, so
        // the mint cannot have passed); otherwise the marker reached nobody.
        if bound.coverage().scope_fence(scope) != fence_before {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                scope = ?scope,
                "the scope fence moved before its completion marker could be registered; \
                 withholding the marker"
            );
        } else {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                scope = ?scope,
                "its completion marker reached no consumer; withholding it so the scope \
                 is re-walked"
            );
        }
        return MarkerOutcome::Withheld;
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
    /// Published to a numbered receiver.
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
            uid_validity,
        } => {
            let policy = BackfillPolicy {
                strategy: BackfillStrategy::UidRange {
                    chunk_size: config.uid_range_chunk,
                    uid_validity,
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
        InventoryPartitioning::UidRange { max_uid: None, .. } => {
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

#[cfg(test)]
#[path = "../backfill/parking_tests.rs"]
mod parking_tests;

#[cfg(test)]
mod scan_projection_tests {
    use super::*;

    fn cursor(scope: CursorScope) -> ChangeCursor {
        ChangeCursor {
            scope,
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Jmap,
                envelope_version: bifrost_types::PayloadVersion::new(1),
                bytes: vec![1],
            },
            advanced_through: None,
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
        }
    }

    #[test]
    fn registry_projects_live_incarnation_and_loss_without_another_scan() {
        let registry = BackfillRegistry::new();
        let account = AccountId("projection".into());
        let cursors = Arc::new(crate::cursor::CursorRegistry::new());
        let coverage = Arc::new(PendingCoverage::new());
        let scope = CursorScope::Account;
        cursors.put(cursor(scope.clone()));
        let key = cursors.all_scope_incarnations().remove(0);
        let mut scan = BackfillScan::default();
        scan.publish(
            &registry,
            account.clone(),
            Arc::clone(&cursors),
            Arc::clone(&coverage),
        );
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Pending)
        );
        scan.begin_walk(&key, &coverage);
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Running)
        );
        scan.record_attempt(key.clone(), true);
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Completed)
        );
        coverage.note_undelivered(&scope, 1);
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Pending)
        );
        // Re-establishment must not inherit even a loss-free completion.
        scan.begin_walk(&key, &coverage);
        scan.record_attempt(key.clone(), true);
        cursors.delete(&scope);
        assert_eq!(registry.snapshot(&account, &scope), None);
        cursors.put(cursor(scope.clone()));
        assert_eq!(
            registry.snapshot(&account, &scope),
            Some(BackfillState::Pending)
        );
        assert_eq!(registry.snapshot_incarnation(&account, &scope, key.1), None);
        let replacement = cursors.all_scope_incarnations()[0].1;
        assert_eq!(
            registry.snapshot_incarnation(&account, &scope, replacement),
            Some(BackfillState::Pending)
        );
        registry.forget_account(&account);
        assert_eq!(registry.snapshot(&account, &scope), None);
    }

    #[test]
    fn settled_incarnations_are_pruned_with_their_other_state() {
        let mut scan = BackfillScan::default();
        let coverage = PendingCoverage::new();
        let scope = CursorScope::Account;
        let key = (scope.clone(), crate::cursor::ScopeIncarnation::for_test(1));
        scan.begin_walk(&key, &coverage);
        scan.record_attempt(key.clone(), true);
        assert!(
            scan.select(
                vec![key],
                &HashSet::new(),
                tokio::time::Instant::now(),
                &coverage
            )
            .is_empty()
        );
        scan.select(
            Vec::new(),
            &HashSet::new(),
            tokio::time::Instant::now(),
            &coverage,
        );
        assert!(
            scan.state
                .lock()
                .expect("scan poisoned")
                .incarnations
                .is_empty()
        );
        let replacement = (scope, crate::cursor::ScopeIncarnation::for_test(2));
        assert_eq!(
            scan.select(
                vec![replacement.clone()],
                &HashSet::new(),
                tokio::time::Instant::now(),
                &coverage
            ),
            vec![replacement]
        );
    }
}
