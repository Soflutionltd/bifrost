//! The bulk mutation pipeline and its campaign entry points.

use super::*;

impl SyncEngine {
    /// Drive a single-account bulk-flag campaign against an attached
    /// account, observing uncertain writes and reasserting eligible unsatisfied
    /// targets within the shared retry budget.
    ///
    /// Returns the per-batch counters aggregated across the run. Once a
    /// campaign starts, failures are retained in `MutationCounters::error`
    /// alongside partial accounting. Preflight failures return `Err`.
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

    /// Caller-version campaign. Preconditions remain frozen across retries.
    /// A rejected caller precondition is reported without read-back or replay.
    pub async fn bulk_set_flags_if_version(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::MailMutationTarget>,
        op: bifrost_types::FlagOp,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.run_bulk_pipeline(
            account_id,
            BulkTargets::versioned(targets)?,
            BulkPipelineOp::SetFlags(op),
            vendor,
            protocol,
        )
        .await
    }

    /// Caller-version campaign. Preconditions remain frozen across retries.
    /// A rejected caller precondition is reported without read-back or replay.
    pub async fn bulk_move_if_version(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::MailMutationTarget>,
        destination: MembershipScope,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.run_bulk_pipeline(
            account_id,
            BulkTargets::versioned(targets)?,
            BulkPipelineOp::Move {
                destination,
                source: None,
            },
            vendor,
            protocol,
        )
        .await
    }

    /// Caller-version campaign. Preconditions remain frozen across retries.
    /// A rejected caller precondition is reported without read-back or replay.
    pub async fn bulk_move_from_if_version(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::MailMutationTarget>,
        destination: MembershipScope,
        source: Option<MembershipScope>,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.run_bulk_pipeline(
            account_id,
            BulkTargets::versioned(targets)?,
            BulkPipelineOp::Move {
                destination,
                source,
            },
            vendor,
            protocol,
        )
        .await
    }

    /// Caller-version campaign. Preconditions remain frozen across retries.
    /// A rejected caller precondition is reported without read-back or replay.
    pub async fn bulk_destroy_if_version(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::MailMutationTarget>,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.run_bulk_pipeline(
            account_id,
            BulkTargets::versioned(targets)?,
            BulkPipelineOp::Destroy,
            vendor,
            protocol,
        )
        .await
    }

    /// Schedule bulk mutations as independently ready throttle cohorts.
    /// Targets are deduplicated and grouped by their singleton declarations;
    /// each cohort owns one vended key and its own retry and read-back state.
    /// The per-object outcome map and retry allowance belong to the campaign.
    /// Retries and desired-state reassertions share the same attempt budget,
    /// frozen cohort key and distinct-target slots. Moves never reassert.
    async fn run_bulk_pipeline(
        &self,
        account_id: &AccountId,
        targets: impl Into<BulkTargets>,
        op: BulkPipelineOp,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        use crate::recovery::{RecoveryPlan, plan_recovery};
        let BulkTargets {
            ids: targets,
            expected_versions,
        } = targets.into();
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let max_retries = self.config.mutation_max_retries;
        let mut retry_slots = self.config.mutation.retry_queue_cap;
        let mut retry_owners = HashSet::new();
        let mut identities = HashMap::new();
        let mut cohorts = Vec::new();
        let mut seen = HashSet::new();
        let account = slot.current.load_full();
        for id in targets.into_iter().filter(|id| seen.insert(id.clone())) {
            route_target(
                &mut cohorts,
                &mut identities,
                account.as_ref().as_ref(),
                account_id,
                id,
                &op,
                vendor,
                protocol,
            );
        }
        let mut outcomes = HashMap::new();
        let mut effects = HashMap::new();
        let mut partial_commit_observations = Vec::new();
        let mut dedupe_count = 0;
        let mut forwarded_directives = HashSet::new();
        let mut first_error = None;
        let mut caller_conflict = None;
        let mut blocked_by_engine = false;
        let mut cursor = 0;

        loop {
            if slot.shutdown.is_cancelled() {
                first_error.get_or_insert(Error::ShuttingDown);
                break;
            }
            let aborting = first_error.is_some();
            let mut boundary = slot.boundary_tx.subscribe();
            let mut generation = slot.account_generation_tx.subscribe();
            if aborting && !matches!(*boundary.borrow_and_update(), BoundaryRequest::Run) {
                break;
            }
            if !aborting && !slot.control.wait_until_running(&slot.shutdown).await {
                first_error.get_or_insert(Error::ShuttingDown);
                break;
            }
            // Re-route only work never submitted. Submitted and uncertain ids
            // keep their execution identity even when a reopened account changes
            // its declaration. Interning keys gives a canonical BTreeSet without
            // imposing Ord on the published ThrottleKey type.
            let account = slot.current.load_full();
            if !aborting && !blocked_by_engine {
                let mut reroute = Vec::new();
                for cohort in &mut cohorts {
                    if !cohort.submitted && !cohort.done {
                        cohort.remaining.retain(|id| {
                            let identity = cohort_identity(
                                &mut identities,
                                campaign_keys(
                                    account.as_ref().as_ref(),
                                    account_id,
                                    std::slice::from_ref(id),
                                    &op,
                                ),
                            );
                            if identity == cohort.identity {
                                true
                            } else {
                                reroute.push(id.clone());
                                false
                            }
                        });
                        cohort.targets = cohort.remaining.clone();
                        cohort.done = cohort.remaining.is_empty();
                    }
                }
                for id in reroute {
                    route_target(
                        &mut cohorts,
                        &mut identities,
                        account.as_ref().as_ref(),
                        account_id,
                        id,
                        &op,
                        vendor,
                        protocol,
                    );
                }
            }
            // A campaign schedules ONE ready action at a time. A held cohort
            // yields to siblings; neither a hold nor an admission re-check burns
            // an attempt, and no admission or activity spans a hold wait.
            if aborting || blocked_by_engine {
                for cohort in cohorts.iter_mut().filter(|cohort| !cohort.done) {
                    if !cohort.submitted {
                        for id in &cohort.targets {
                            outcomes.insert(
                                id.clone(),
                                if aborting {
                                    MutationBucket::PendingRetry
                                } else {
                                    MutationBucket::BlockedByEngine
                                },
                            );
                        }
                        cohort.done = true;
                        continue;
                    }
                    for id in &cohort.targets {
                        if matches!(outcomes.get(id), Some(MutationBucket::ReadyReassert)) {
                            outcomes.insert(
                                id.clone(),
                                if aborting {
                                    MutationBucket::Mismatched
                                } else {
                                    MutationBucket::BlockedByEngine
                                },
                            );
                        }
                    }
                    if blocked_by_engine && !aborting {
                        for id in &cohort.targets {
                            if matches!(outcomes.get(id), Some(MutationBucket::PendingRetry)) {
                                outcomes.insert(id.clone(), MutationBucket::BlockedByEngine);
                            }
                        }
                    }
                    cohort.readback = true;
                }
            }
            let readback_set: HashSet<_> = unresolved_readback_ids(&outcomes).into_iter().collect();
            let mut selected = None;
            let mut earliest = None;
            for offset in 0..cohorts.len() {
                let index = (cursor + offset) % cohorts.len();
                let cohort = &mut cohorts[index];
                if cohort.done {
                    continue;
                }
                let ids = if cohort.readback {
                    cohort
                        .targets
                        .iter()
                        .filter(|id| readback_set.contains(*id))
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    cohort.remaining.clone()
                };
                if ids.is_empty() {
                    cohort.done = true;
                    continue;
                }
                let keys = if cohort.readback {
                    crate::recovery::declared_keys(
                        account.as_ref().as_ref(),
                        account_id,
                        bifrost_types::ThrottleWork::Objects(&ids),
                    )
                } else {
                    campaign_keys(account.as_ref().as_ref(), account_id, &ids, &op)
                };
                let now = tokio::time::Instant::now();
                let mut ready_at = if cohort.readback {
                    cohort.reconcile_at
                } else {
                    cohort.retry_at
                };
                if let Some(wait) =
                    crate::recovery::throttle_wait(&slot.throttles, account_id, &keys)
                {
                    ready_at = ready_at.max(now + wait);
                }
                if ready_at <= now {
                    selected = Some((index, ids));
                    break;
                }
                earliest =
                    Some(earliest.map_or(ready_at, |old: tokio::time::Instant| old.min(ready_at)));
            }
            let Some((index, mut remaining)) = selected else {
                let Some(deadline) = earliest else {
                    break;
                };
                if aborting && slot.control.begin_activity().is_none() {
                    break;
                }
                tokio::select! {
                    () = slot.shutdown.cancelled() => { first_error.get_or_insert(Error::ShuttingDown); break; },
                    _ = boundary.changed(), if aborting => {},
                    _ = generation.changed() => {},
                    () = tokio::time::sleep_until(deadline) => {}
                }
                continue;
            };
            cursor = (index + 1) % cohorts.len();
            let admission = tokio::select! {
                () = slot.shutdown.cancelled() => { first_error.get_or_insert(Error::ShuttingDown); break; },
                _ = boundary.changed(), if aborting => { continue; },
                result = slot.scheduler.admit(account_id.clone(), slot.control.priority_snapshot(), crate::scheduler::WorkKind::Mutation) => match result {
                    Ok(permit) => permit,
                    Err(_error) => {
                        // Admission refusal is transient. Delay this action,
                        // release all permits, and let ready siblings run.
                        // It consumes neither a submission nor a retry slot.
                        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
                        if cohorts[index].readback {
                            cohorts[index].reconcile_at = deadline;
                        } else {
                            cohorts[index].retry_at = deadline;
                        }
                        continue;
                    }
                }
            };
            let account = slot.current.load_full();
            let cohort = &mut cohorts[index];
            let keys = if cohort.readback {
                crate::recovery::declared_keys(
                    account.as_ref().as_ref(),
                    account_id,
                    bifrost_types::ThrottleWork::Objects(&remaining),
                )
            } else {
                campaign_keys(account.as_ref().as_ref(), account_id, &remaining, &op)
            };
            // Admission can wait through a reopen or a newly recorded hold.
            // Return to the scheduler rather than parking this cohort inline.
            if crate::recovery::throttle_wait(&slot.throttles, account_id, &keys).is_some() {
                drop(admission);
                continue;
            }
            if !cohort.submitted
                && remaining.iter().any(|id| {
                    cohort_identity(
                        &mut identities,
                        campaign_keys(
                            account.as_ref().as_ref(),
                            account_id,
                            std::slice::from_ref(id),
                            &op,
                        ),
                    ) != cohort.identity
                })
            {
                drop(admission);
                continue;
            }
            let Some(activity) = slot.control.begin_activity() else {
                drop(admission);
                if aborting {
                    break;
                }
                continue;
            };
            if cohort.readback {
                // Observed state cannot prove an uncertain caller-guarded
                // write passed its precondition. Leave those targets pending.
                let remaining = remaining
                    .into_iter()
                    .filter(|id| {
                        if expected_versions.contains_key(id) {
                            outcomes.insert(id.clone(), MutationBucket::Unresolved);
                            false
                        } else {
                            true
                        }
                    })
                    .collect::<Vec<_>>();
                if remaining.is_empty() {
                    cohort.done = true;
                    drop(activity);
                    drop(admission);
                    continue;
                }
                use crate::mutation::readback::{ReadbackTarget, run_object_readback};
                let target = match &op {
                    BulkPipelineOp::SetFlags(flags) => ReadbackTarget::Flags(flags),
                    BulkPipelineOp::Move { destination, .. } => ReadbackTarget::Move(destination),
                    BulkPipelineOp::Destroy => ReadbackTarget::Destroy,
                };
                let record = |error: &AccountError| {
                    record_error_throttle(&slot.throttles, account_id, error);
                };
                let result = tokio::select! {
                    () = slot.shutdown.cancelled() => { first_error.get_or_insert(Error::ShuttingDown); None },
                    _ = boundary.changed(), if aborting => None,
                    result = run_object_readback(account.as_ref().as_ref(), remaining.clone(), target, Some(&record)) => Some(result),
                };
                let mut directives = Vec::new();
                if let Some(report) = result {
                    for error in report.engine_failures {
                        blocked_by_engine = true;
                        if let bifrost_types::RecoveryClass::Engine(directive) = error.recovery()
                            && should_forward_engine_recovery(&mut forwarded_directives, directive)
                        {
                            directives
                                .push((crate::recovery::directive_target_scope(directive), error));
                        }
                    }
                    if let Some(error) = report.termination {
                        if let Error::Account(error) = &error {
                            record(error);
                        }
                        first_error.get_or_insert(error);
                    }
                    for id in &remaining {
                        let origin = outcomes.get(id).copied();
                        let result = report
                            .objects
                            .get(id)
                            .copied()
                            .unwrap_or(crate::mutation::readback::ObjectReadback::Unresolved);
                        let eligible = matches!(
                            origin,
                            Some(
                                MutationBucket::PendingReadback(ReadbackOrigin::Reassertable)
                                    | MutationBucket::PendingRetry
                            )
                        ) && !expected_versions.contains_key(id)
                            && reassertion_declared(
                                &op,
                                &account.capabilities().mutation.reassertion,
                            )
                            && first_error.is_none()
                            && !blocked_by_engine
                            && retry_available(cohort.attempt, max_retries)
                            && (retry_owners.contains(id) || retry_slots > 0);
                        use crate::mutation::readback::ObjectReadback;
                        let bucket = match result {
                            ObjectReadback::Satisfied => MutationBucket::Skipped,
                            ObjectReadback::Unresolved => MutationBucket::Unresolved,
                            ObjectReadback::Unsatisfied if eligible => {
                                if retry_owners.insert(id.clone()) {
                                    retry_slots -= 1;
                                }
                                MutationBucket::ReadyReassert
                            }
                            ObjectReadback::Unsatisfied => MutationBucket::Mismatched,
                        };
                        outcomes.insert(id.clone(), bucket);
                    }
                } else {
                    for id in &remaining {
                        outcomes.insert(id.clone(), MutationBucket::Unresolved);
                    }
                }
                cohort.remaining = remaining
                    .into_iter()
                    .filter(|id| matches!(outcomes.get(id), Some(MutationBucket::ReadyReassert)))
                    .collect();
                if cohort.remaining.is_empty() {
                    cohort.done = true;
                } else {
                    cohort.readback = false;
                    cohort.retry_at = tokio::time::Instant::now();
                }
                drop(activity);
                drop(admission);
                for (scope, error) in directives {
                    let _ = slot
                        .reopen_tx
                        .send(ReopenRequest::Recovery { scope, error })
                        .await;
                }
                continue;
            }
            // A reopen can replace the account while admission is pending.
            remaining.retain(|id| {
                if matches!(outcomes.get(id), Some(MutationBucket::ReadyReassert))
                    && (first_error.is_some()
                        || blocked_by_engine
                        || !reassertion_declared(&op, &account.capabilities().mutation.reassertion))
                {
                    outcomes.insert(
                        id.clone(),
                        if blocked_by_engine && first_error.is_none() {
                            MutationBucket::BlockedByEngine
                        } else {
                            MutationBucket::Mismatched
                        },
                    );
                    false
                } else {
                    true
                }
            });
            if remaining.is_empty() {
                cohort.done = true;
                drop(activity);
                drop(admission);
                continue;
            }
            for id in &remaining {
                outcomes.insert(id.clone(), MutationBucket::InFlight);
            }
            cohort.submitted = true;
            let key = cohort.key.clone();
            let mut retry_ids = Vec::new();
            let mut reconcile_at = cohort.reconcile_at;
            let guarded = remaining
                .iter()
                .any(|id| expected_versions.contains_key(id));
            let mut stream = if guarded {
                let targets = remaining
                    .iter()
                    .cloned()
                    .map(|id| bifrost_types::MailMutationTarget {
                        expected_version: expected_versions.get(&id).cloned(),
                        id,
                    })
                    .collect::<Vec<_>>();
                let target_stream = Box::pin(futures::stream::iter(targets));
                let account = Arc::clone(account.as_ref());
                match &op {
                    BulkPipelineOp::SetFlags(flags) => {
                        account.bulk_set_flags_if_version(target_stream, flags.clone(), key.clone())
                    }
                    BulkPipelineOp::Move {
                        destination,
                        source,
                    } => account.bulk_move_from_if_version(
                        target_stream,
                        destination.clone(),
                        source.clone(),
                        key.clone(),
                    ),
                    BulkPipelineOp::Destroy => {
                        account.bulk_destroy_if_version(target_stream, key.clone())
                    }
                }
            } else {
                let target_stream: AccountStream<bifrost_types::ObjectId> =
                    Box::pin(futures::stream::iter(remaining.clone()));
                match &op {
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
                }
            };
            // Read-back membership comes from the outcome map, so an id
            // parked there survives attempts that resubmit its siblings.
            let mut stream_termination_advice: Option<RetryAdvice> = None;
            let mut item_retry_advice: Option<RetryAdvice> = None;

            loop {
                let event = tokio::select! {
                    () = slot.shutdown.cancelled() => { first_error.get_or_insert(Error::ShuttingDown); break; },
                    event = stream.next() => event,
                };
                let Some(event) = event else {
                    break;
                };
                match event {
                    bifrost_types::SyncEvent::Batch(batch) => {
                        for item in batch.items {
                            let observed = match &item {
                                ItemOutcome::Failed(f) => Some((&f.item, &f.error)),
                                ItemOutcome::Uncertain(u) => Some((&u.item, &u.error)),
                                _ => None,
                            };
                            if let Some((id, error)) = observed
                                && error.partial_commit().is_some()
                            {
                                partial_commit_observations.push(
                                    crate::mutation::PartialCommitObservation {
                                        target: Some(bifrost_types::ObjectId(id.0.clone())),
                                        error: error.clone(),
                                    },
                                );
                            }
                            if let ItemOutcome::Failed(failure) = &item {
                                let id = bifrost_types::ObjectId(failure.item.0.clone());
                                if expected_versions.contains_key(&id)
                                    && matches!(
                                        failure.error.kind(),
                                        bifrost_types::AccountErrorKind::ConcurrencyConflict
                                    )
                                {
                                    // A read-back can observe the requested state without proving
                                    // that this caller's precondition ever held. Keep the refusal.
                                    outcomes.insert(id, MutationBucket::FailedTerminal);
                                    caller_conflict
                                        .get_or_insert(Error::Account(failure.error.clone()));
                                    continue;
                                }
                            }

                            if let ItemOutcome::Succeeded(success) = &item
                                && let MutationSuccess::Downgraded { actual } = &success.output
                            {
                                effects.insert(
                                    bifrost_types::ObjectId(success.item.0.clone()),
                                    actual.clone(),
                                );
                            }
                            let error = match &item {
                                ItemOutcome::Failed(failure) => Some(failure.error.clone()),
                                ItemOutcome::Uncertain(uncertain) => Some(uncertain.error.clone()),
                                _ => None,
                            };
                            let item_id = match &item {
                                ItemOutcome::Succeeded(s) => {
                                    bifrost_types::ObjectId(s.item.0.clone())
                                }
                                ItemOutcome::Failed(f) => bifrost_types::ObjectId(f.item.0.clone()),
                                ItemOutcome::Uncertain(u) => {
                                    bifrost_types::ObjectId(u.item.0.clone())
                                }
                            };
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
                            if let Some(error) = error {
                                // Classification also records the primary hold; max deadlines make
                                // that duplicate harmless. This call observes siblings and cleanup too.
                                record_error_throttle(&slot.throttles, account_id, &error);
                                let delay = if matches!(
                                    outcomes.get(&item_id),
                                    Some(MutationBucket::PendingReadback(_))
                                ) {
                                    match error.recovery() {
                                        bifrost_types::RecoveryClass::Reconcile(advice) => {
                                            Some(crate::recovery::reconcile_delay(
                                                advice,
                                                std::time::SystemTime::now(),
                                                Duration::from_secs(1),
                                            ))
                                        }
                                        bifrost_types::RecoveryClass::Retry(advice)
                                            if matches!(
                                                advice.disposition,
                                                bifrost_types::RetryDisposition::AfterStateRefresh
                                            ) || advice.retry_hint.is_some() =>
                                        {
                                            Some(crate::recovery::retry_delay(
                                                advice,
                                                std::time::SystemTime::now(),
                                                Duration::from_secs(1),
                                            ))
                                        }
                                        _ => None,
                                    }
                                } else {
                                    None
                                };
                                if let Some(delay) = delay {
                                    reconcile_at =
                                        reconcile_at.max(tokio::time::Instant::now() + delay);
                                }
                            }
                        }
                    }
                    bifrost_types::SyncEvent::Terminated(err) => {
                        let original = err.clone();
                        if original.partial_commit().is_some() {
                            partial_commit_observations.push(
                                crate::mutation::PartialCommitObservation {
                                    target: None,
                                    error: original.clone(),
                                },
                            );
                        }
                        record_error_throttle(&slot.throttles, account_id, &original);
                        match plan_recovery(err) {
                            RecoveryPlan::Retry(advice) => {
                                if matches!(
                                    advice.disposition,
                                    bifrost_types::RetryDisposition::SameRequest
                                        | bifrost_types::RetryDisposition::AfterAuthRefresh
                                ) {
                                    queue_unresolved_for_retry(
                                        &remaining,
                                        &outcomes,
                                        &mut retry_ids,
                                    );
                                } else {
                                    let origin = if matches!(
                                        advice.disposition,
                                        bifrost_types::RetryDisposition::AfterStateRefresh
                                    ) {
                                        ReadbackOrigin::Reassertable
                                    } else {
                                        ReadbackOrigin::Protected
                                    };
                                    for id in &remaining {
                                        if expected_versions.contains_key(id) {
                                            queue_unresolved_for_retry(
                                                std::slice::from_ref(id),
                                                &outcomes,
                                                &mut retry_ids,
                                            );
                                        } else if matches!(
                                            outcomes.get(id),
                                            Some(MutationBucket::InFlight)
                                        ) {
                                            outcomes.insert(
                                                id.clone(),
                                                MutationBucket::PendingReadback(origin),
                                            );
                                        }
                                    }
                                    reconcile_at = reconcile_at.max(
                                        tokio::time::Instant::now()
                                            + crate::recovery::retry_delay(
                                                &advice,
                                                std::time::SystemTime::now(),
                                                Duration::from_secs(1),
                                            ),
                                    );
                                }
                                stream_termination_advice = Some(advice);
                                break;
                            }
                            RecoveryPlan::Reconcile(advice) => {
                                reconcile_at = reconcile_at.max(
                                    tokio::time::Instant::now()
                                        + crate::recovery::reconcile_delay(
                                            &advice,
                                            std::time::SystemTime::now(),
                                            Duration::from_secs(1),
                                        ),
                                );
                                let mut wants_dedupe = false;
                                for action in &advice.guidance.actions {
                                    match action {
                                        ReconcileAction::CheckTarget => {}
                                        ReconcileAction::DedupeByClientId => wants_dedupe = true,
                                        _ => {}
                                    }
                                }
                                let origin = reconcile_origin(&advice);
                                for id in &remaining {
                                    if matches!(outcomes.get(id), Some(MutationBucket::InFlight))
                                        || (origin == ReadbackOrigin::Protected
                                            && matches!(
                                                outcomes.get(id),
                                                Some(MutationBucket::PendingReadback(
                                                    ReadbackOrigin::Reassertable
                                                ))
                                            ))
                                    {
                                        outcomes.insert(
                                            id.clone(),
                                            MutationBucket::PendingReadback(origin),
                                        );
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
                                if should_forward_engine_recovery(
                                    &mut forwarded_directives,
                                    &directive,
                                ) {
                                    let scope = crate::recovery::directive_target_scope(&directive);
                                    let _ = slot
                                        .reopen_tx
                                        .send(ReopenRequest::Recovery {
                                            scope,
                                            error: original,
                                        })
                                        .await;
                                }
                                blocked_by_engine = true;
                                for id in &remaining {
                                    if matches!(
                                        outcomes.get(id),
                                        Some(
                                            MutationBucket::InFlight | MutationBucket::PendingRetry
                                        )
                                    ) {
                                        outcomes
                                            .insert(id.clone(), MutationBucket::BlockedByEngine);
                                    }
                                }
                                break;
                            }
                            RecoveryPlan::Terminal(fatal) => {
                                first_error.get_or_insert(Error::Account(fatal.into_inner()));
                                break;
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

            cohort.reconcile_at = reconcile_at;
            cohort.attempt = cohort.attempt.saturating_add(1);
            // Every submitted target must occupy a lane, even when a stream
            // ends without reporting it. Such an omission is uncertain.
            for id in &remaining {
                if matches!(outcomes.get(id), Some(MutationBucket::InFlight)) {
                    outcomes.insert(
                        id.clone(),
                        if slot.shutdown.is_cancelled() {
                            MutationBucket::Unresolved
                        } else if blocked_by_engine {
                            MutationBucket::BlockedByEngine
                        } else {
                            MutationBucket::PendingReadback(ReadbackOrigin::Reassertable)
                        },
                    );
                }
            }
            let retry_set: HashSet<_> = retry_ids.into_iter().collect();
            let mut next = Vec::new();
            for id in retry_candidates(&remaining, &retry_set, &outcomes) {
                outcomes.insert(id.clone(), MutationBucket::PendingRetry);
                if retry_available(cohort.attempt, max_retries)
                    && (retry_owners.contains(&id) || retry_slots > 0)
                {
                    if retry_owners.insert(id.clone()) {
                        retry_slots -= 1;
                    }
                    next.push(id);
                }
            }
            if first_error.is_some() || blocked_by_engine || next.is_empty() {
                cohort.readback = true;
            } else {
                cohort.remaining = next;
                if let Some(advice) = stream_termination_advice.or(item_retry_advice) {
                    cohort.retry_at = tokio::time::Instant::now()
                        + crate::recovery::retry_delay(
                            &advice,
                            std::time::SystemTime::now(),
                            Duration::from_secs(1),
                        );
                }
            }
            drop(activity);
            drop(admission);
        }
        // Never-submitted targets are known unsent and never observed. A
        // cancelled, paused, detached or failed observation leaves submitted
        // unresolved targets explicit rather than implying a terminal failure.
        for cohort in &cohorts {
            for id in &cohort.targets {
                if !cohort.submitted {
                    outcomes
                        .entry(id.clone())
                        .or_insert(MutationBucket::PendingRetry);
                } else if !cohort.done {
                    let value = outcomes
                        .entry(id.clone())
                        .or_insert(MutationBucket::Unresolved);
                    if matches!(
                        value,
                        MutationBucket::PendingRetry
                            | MutationBucket::PendingReadback(_)
                            | MutationBucket::InFlight
                    ) {
                        *value = MutationBucket::Unresolved;
                    } else if matches!(value, MutationBucket::ReadyReassert) {
                        *value = MutationBucket::Mismatched;
                    }
                }
            }
        }
        let mut totals = counters_from_outcomes(&outcomes);
        totals.error = first_error.or(caller_conflict).map(Arc::new);
        totals.effects = effects.into_iter().collect();
        totals.partial_commit_observations = partial_commit_observations;
        totals.dedupe_by_client_id = dedupe_count;
        Ok(totals)
    }
}

struct BulkTargets {
    ids: Vec<bifrost_types::ObjectId>,
    expected_versions: HashMap<bifrost_types::ObjectId, bifrost_types::ServerVersion>,
}

impl From<Vec<bifrost_types::ObjectId>> for BulkTargets {
    fn from(ids: Vec<bifrost_types::ObjectId>) -> Self {
        Self {
            ids,
            expected_versions: HashMap::new(),
        }
    }
}

impl BulkTargets {
    fn versioned(targets: Vec<bifrost_types::MailMutationTarget>) -> Result<Self, Error> {
        let mut ids = Vec::new();
        let mut versions = HashMap::new();
        for target in targets {
            if let Some(previous) = versions.get(&target.id) {
                if previous != &target.expected_version {
                    return Err(Error::Other(
                        "one mutation target has conflicting caller versions".into(),
                    ));
                }
                continue;
            }
            ids.push(target.id.clone());
            versions.insert(target.id, target.expected_version);
        }
        Ok(Self {
            ids,
            expected_versions: versions
                .into_iter()
                .filter_map(|(id, version)| version.map(|version| (id, version)))
                .collect(),
        })
    }
}

/// A key is interned once per campaign; equal declarations produce identical
/// ordered sets regardless of declaration order or duplicate keys.
fn cohort_identity(
    registry: &mut HashMap<bifrost_types::ThrottleKey, usize>,
    keys: Vec<bifrost_types::ThrottleKey>,
) -> std::collections::BTreeSet<usize> {
    keys.into_iter()
        .map(|key| {
            let next = registry.len();
            *registry.entry(key).or_insert(next)
        })
        .collect()
}

struct MutationCohort {
    identity: std::collections::BTreeSet<usize>,
    key: bifrost_types::IdempotencyKey,
    targets: Vec<bifrost_types::ObjectId>,
    remaining: Vec<bifrost_types::ObjectId>,
    submitted: bool,
    readback: bool,
    done: bool,
    attempt: u64,
    retry_at: tokio::time::Instant,
    reconcile_at: tokio::time::Instant,
}

#[allow(clippy::too_many_arguments)]
fn route_target(
    cohorts: &mut Vec<MutationCohort>,
    registry: &mut HashMap<bifrost_types::ThrottleKey, usize>,
    account: &dyn Account,
    account_id: &AccountId,
    id: bifrost_types::ObjectId,
    op: &BulkPipelineOp,
    vendor: &crate::mutation::IdempotencyVendor,
    protocol: bifrost_types::ProtocolKind,
) {
    let identity = cohort_identity(
        registry,
        campaign_keys(account, account_id, std::slice::from_ref(&id), op),
    );
    if let Some(cohort) = cohorts
        .iter_mut()
        .find(|c| c.identity == identity && !c.submitted && !c.done)
    {
        cohort.targets.push(id.clone());
        cohort.remaining.push(id);
    } else {
        cohorts.push(MutationCohort {
            identity,
            key: vendor.next(protocol),
            targets: vec![id.clone()],
            remaining: vec![id],
            submitted: false,
            readback: false,
            done: false,
            attempt: 0,
            retry_at: tokio::time::Instant::now(),
            reconcile_at: tokio::time::Instant::now(),
        });
    }
}

/// Which bulk mutation a [`SyncEngine::run_bulk_pipeline`] run drives.
///
/// Selects the op-specific seams in the shared pipeline: the wire submit
/// call, the matching read-back target (flag judge, membership or absence)
/// and the reassertion declaration it reads. Everything else in the
/// idempotency / retry / recovery loop is shared.
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
    for (source, recovery) in error.throttle_observations() {
        match recovery {
            bifrost_types::RecoveryClass::Retry(advice) => {
                crate::recovery::record_throttle(throttles, account_id, advice, source);
            }
            bifrost_types::RecoveryClass::Reconcile(advice) => {
                crate::recovery::record_reconcile_throttle(throttles, account_id, advice, source);
            }
            _ => {}
        }
    }
}

/// What a valid unsatisfied observation of a read-back id may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadbackOrigin {
    /// A valid unsatisfied observation may authorize reassertion.
    Reassertable,
    /// Observed only: dedupe guidance, engine or terminal recovery on an
    /// uncertain item, or a class this engine does not know.
    Protected,
}

/// Reassertable iff every reconcile action is `CheckTarget`: any other action
/// (including `DedupeByClientId` and `ObserveOnly`) protects the origin.
fn reconcile_origin(advice: &bifrost_types::ReconcileAdvice) -> ReadbackOrigin {
    if advice
        .guidance
        .actions
        .iter()
        .all(|a| matches!(a, ReconcileAction::CheckTarget))
    {
        ReadbackOrigin::Reassertable
    } else {
        ReadbackOrigin::Protected
    }
}

/// The origin of an `Uncertain` item. Retry advice with a known disposition is
/// reassertable after observation (the uncertainty lane only replaced the
/// blind retry with an observation); reconcile advice follows
/// `reconcile_origin`; engine, terminal and unknown recovery are protected.
fn uncertain_origin(error: &AccountError) -> ReadbackOrigin {
    use bifrost_types::{RecoveryClass, RetryDisposition};
    match error.recovery() {
        RecoveryClass::Reconcile(advice) => reconcile_origin(advice),
        RecoveryClass::Retry(advice)
            if matches!(
                advice.disposition,
                RetryDisposition::SameRequest
                    | RetryDisposition::AfterAuthRefresh
                    | RetryDisposition::AfterStateRefresh
            ) =>
        {
            ReadbackOrigin::Reassertable
        }
        _ => ReadbackOrigin::Protected,
    }
}

/// The account's declaration for this campaign's door. Checked when a
/// reassertion is scheduled and again on the account loaded after admission.
fn reassertion_declared(op: &BulkPipelineOp, caps: &bifrost_types::MutationReassertion) -> bool {
    match op {
        BulkPipelineOp::SetFlags(_) => caps.set_flags,
        BulkPipelineOp::Destroy => caps.destroy,
        BulkPipelineOp::Move { .. } => false,
    }
}

/// Per-id campaign state; the outcome map is the single source of truth for
/// which lane an id is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MutationBucket {
    Applied,
    Downgraded,
    Skipped,
    FailedTerminal,
    /// A retry candidate (and, once retries end, a read-back id).
    PendingRetry,
    /// Awaiting observation; the origin decides what an unsatisfied
    /// observation may do.
    PendingReadback(ReadbackOrigin),
    /// Observed unsatisfied and scheduled for reassertion: observation
    /// consumed, attempt budget checked and retry slot reserved. Never read
    /// back again before it is resubmitted.
    ReadyReassert,
    /// Written for every id of a submission round immediately before the
    /// mutation door is called; never survives the round.
    InFlight,
    /// Observed unsatisfied and not (or no longer) eligible. Final, counted
    /// pending rather than terminal.
    Mismatched,
    /// A submitted write could not be observed conclusively.
    Unresolved,
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
/// Only fresh unanswered submissions and explicit retry candidates qualify. An id
/// sitting in `PendingReadback` is owned by the read-back guard -
/// that lane exists precisely so a write whose first attempt may have
/// landed is verified rather than replayed - and a `BlockedByEngine`
/// id is waiting on an engine directive. Neither is ours to resubmit,
/// so only ids the stream never resolved (`InFlight`) or explicitly asked
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
        let eligible = matches!(
            outcomes.get(id),
            Some(MutationBucket::InFlight | MutationBucket::PendingRetry)
        );
        if eligible && queued.insert(id.clone()) {
            retry_ids.push(id.clone());
        }
    }
}

/// The ids of this round to write back as `PendingRetry`, in round order. An
/// id an `Engine` termination already swept to `BlockedByEngine` is skipped,
/// so the round-end loop cannot rewrite it, whatever the top-of-loop sweep
/// would later repair.
fn retry_candidates(
    remaining: &[bifrost_types::ObjectId],
    retry_set: &HashSet<bifrost_types::ObjectId>,
    outcomes: &HashMap<bifrost_types::ObjectId, MutationBucket>,
) -> Vec<bifrost_types::ObjectId> {
    remaining
        .iter()
        .filter(|id| {
            retry_set.contains(*id)
                && !matches!(outcomes.get(*id), Some(MutationBucket::BlockedByEngine))
        })
        .cloned()
        .collect()
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
            MutationBucket::PendingRetry | MutationBucket::PendingReadback(_)
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
///   landed. Reconcile dedupe, engine and terminal guidance protects such
///   uncertain targets from desired-state reassertion after observation.
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
                // A weaker successful effect is authoritative in the same
                // way as Applied. Preserve it separately: comparing against
                // the original request would deterministically reject a valid
                // downgrade (trash is not permanent deletion). Replaying it
                // cannot supply the missing provider capability.
                MutationSuccess::Downgraded { .. } => MutationBucket::Downgraded,
                // `MutationSuccess` is #[non_exhaustive]. A new variant must
                // be classified deliberately, not folded into `Applied` - that
                // is how a downgrade got reported as a clean success in the
                // first place.
                _ => MutationBucket::PendingReadback(ReadbackOrigin::Protected),
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
                            outcomes.insert(
                                id,
                                MutationBucket::PendingReadback(ReadbackOrigin::Reassertable),
                            );
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
                            outcomes.insert(
                                id,
                                MutationBucket::PendingReadback(ReadbackOrigin::Protected),
                            );
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
                    // Every reconcile shape lands in read-back. That rests on a
                    // PRODUCER invariant, pinned in `bifrost-types` by
                    // `every_producible_reconcile_requests_check_target`: every
                    // producible `ReconcileAdvice` carries `CheckTarget`. Any
                    // further action protects the id from reassertion.
                    outcomes.insert(
                        id,
                        MutationBucket::PendingReadback(reconcile_origin(&advice)),
                    );
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
            outcomes.insert(
                id,
                MutationBucket::PendingReadback(uncertain_origin(&uncertain.error)),
            );
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

pub(super) fn counters_from_outcomes(
    outcomes: &HashMap<bifrost_types::ObjectId, MutationBucket>,
) -> crate::mutation::MutationCounters {
    let mut counters = crate::mutation::MutationCounters::default();
    for bucket in outcomes.values() {
        match bucket {
            MutationBucket::InFlight => {
                debug_assert!(false, "submission round left an in-flight target");
                counters.record_pending();
            }
            MutationBucket::Applied => counters.record_applied(),
            MutationBucket::Downgraded => counters.downgraded += 1,
            MutationBucket::Skipped => counters.record_skipped(),
            MutationBucket::FailedTerminal => counters.record_failed(),
            MutationBucket::BlockedByEngine => counters.record_blocked_by_engine(),
            MutationBucket::PendingRetry
            | MutationBucket::PendingReadback(_)
            | MutationBucket::ReadyReassert
            | MutationBucket::Mismatched
            | MutationBucket::Unresolved => {
                counters.record_pending();
            }
        }
    }
    counters
}

// `attempts` includes the initial write; the configured budget counts only
// resubmissions. The wider attempt counter also permits the full u32 budget
// without saturating before its final submission.
fn retry_available(attempts: u64, max_retries: u32) -> bool {
    attempts <= u64::from(max_retries)
}

#[cfg(test)]
mod campaign_tests {
    use super::*;
    use bifrost_types::{BatchItemId, BatchSuccess, MutationEffect, ObjectId};

    #[test]
    fn retry_candidates_skip_engine_blocked_ids() {
        let id = ObjectId("blocked".into());
        let outcomes = HashMap::from([(id.clone(), MutationBucket::BlockedByEngine)]);
        let candidates = retry_candidates(
            std::slice::from_ref(&id),
            &HashSet::from([id.clone()]),
            &outcomes,
        );
        assert!(candidates.is_empty());
        assert_eq!(outcomes[&id], MutationBucket::BlockedByEngine);
    }
    fn origin_error(
        kind: bifrost_types::AccountErrorKind,
        cause: bifrost_types::Cause,
    ) -> AccountError {
        bifrost_types::AccountErrorBuilder::new(kind, cause)
            .scope(bifrost_types::ErrorScope::Cursor(CursorScope::Account))
            .try_build()
            .unwrap()
    }
    #[test]
    fn uncertain_origin_protects_dedupe_engine_and_terminal() {
        use bifrost_types::*;
        let dedupe = AccountErrorBuilder::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Graph,
                detail: None,
            }),
        )
        .operation(AccountOperation::BulkDestroy)
        .try_build()
        .unwrap();
        let engine = origin_error(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        );
        let terminal = AccountError::unsupported(AccountOperation::UpdateFlags, Protocol::Imap);
        for error in [&dedupe, &engine, &terminal] {
            assert_eq!(uncertain_origin(error), ReadbackOrigin::Protected);
        }
        let transport = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .operation(AccountOperation::BulkDestroy)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::InFlight,
        )))
        .try_build()
        .unwrap();
        assert_eq!(uncertain_origin(&transport), ReadbackOrigin::Reassertable);
        let conflict = origin_error(
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        );
        assert_eq!(uncertain_origin(&conflict), ReadbackOrigin::Reassertable);
        // An idempotent transport failure derives Retry(SameRequest): the
        // uncertainty lane replaced the blind retry with an observation.
        let same_request = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .unwrap();
        assert!(matches!(
            same_request.recovery(),
            RecoveryClass::Retry(advice) if advice.disposition == RetryDisposition::SameRequest
        ));
        assert_eq!(
            uncertain_origin(&same_request),
            ReadbackOrigin::Reassertable
        );
    }
    #[test]
    fn failed_engine_and_terminal_items_never_enter_read_back() {
        use bifrost_types::*;
        let engine = origin_error(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        );
        let terminal = AccountError::unsupported(AccountOperation::UpdateFlags, Protocol::Imap);
        for (error, expected) in [
            (engine, MutationBucket::BlockedByEngine),
            (terminal, MutationBucket::FailedTerminal),
        ] {
            let mut outcomes = HashMap::new();
            let mut retry = Vec::new();
            let mut dedupe = 0;
            let mut advice = None;
            classify_item_outcome(
                ItemOutcome::Failed(BatchFailure::new(BatchItemId("a".into()), error)),
                &mut outcomes,
                &mut retry,
                &mut dedupe,
                &std::sync::Mutex::new(crate::recovery::ThrottleBucket::default()),
                &AccountId("test".into()),
                &mut advice,
            );
            assert_eq!(outcomes[&ObjectId("a".into())], expected);
            assert!(unresolved_readback_ids(&outcomes).is_empty());
        }
    }

    #[test]
    fn retry_limit_counts_resubmissions_only() {
        assert!(!retry_available(1, 0));
        assert!(retry_available(1, 1));
        assert!(!retry_available(2, 1));
        assert!(retry_available(5, 5));
        assert!(!retry_available(6, 5));
    }

    #[test]
    fn weaker_success_has_its_own_bucket_and_never_replays() {
        let id = ObjectId("message".into());
        let mut outcomes = HashMap::new();
        let mut retries = Vec::new();
        let mut dedupe = 0;
        let throttles = std::sync::Mutex::new(crate::recovery::ThrottleBucket::new());
        let item = ItemOutcome::Succeeded(BatchSuccess::new(
            BatchItemId(id.0.clone()),
            MutationSuccess::Downgraded {
                actual: MutationEffect::MovedToContainer(bifrost_types::ContainerId(
                    "trash".into(),
                )),
            },
        ));
        assert!(
            classify_item_outcome(
                item,
                &mut outcomes,
                &mut retries,
                &mut dedupe,
                &throttles,
                &AccountId("account".into()),
                &mut None,
            )
            .is_none()
        );
        assert_eq!(outcomes.get(&id), Some(&MutationBucket::Downgraded));
        assert!(retries.is_empty());
        assert!(unresolved_readback_ids(&outcomes).is_empty());
        let counters = counters_from_outcomes(&outcomes);
        assert_eq!(counters.downgraded, 1);
        assert_eq!(counters.failed_terminal, 0);
    }

    #[test]
    fn non_landing_observation_is_pending_not_terminal() {
        let outcomes = [
            MutationBucket::Mismatched,
            MutationBucket::ReadyReassert,
            MutationBucket::Unresolved,
        ]
        .into_iter()
        .enumerate()
        .map(|(i, b)| (bifrost_types::ObjectId(i.to_string()), b))
        .collect();
        let counters = counters_from_outcomes(&outcomes);
        assert_eq!(counters.pending_retry, 3);
        assert_eq!(counters.failed_terminal, 0);
    }

    #[tokio::test]
    async fn caller_conflict_is_preserved_instead_of_read_back_as_success() {
        let engine = SyncEngine::builder().build().expect("engine");
        let id = AccountId("caller-conflict".into());
        let mut stub = crate::test_common::StubAccount::new(Vec::new());
        stub.versioned_flags_error = Some(
            bifrost_types::AccountErrorBuilder::new(
                bifrost_types::AccountErrorKind::ConcurrencyConflict,
                bifrost_types::Cause::State(bifrost_types::StateCause::ConcurrencyConflict),
            )
            .protocol(bifrost_types::Protocol::Imap)
            .operation(bifrost_types::AccountOperation::UpdateFlags)
            .try_build()
            .expect("valid conflict"),
        );
        let account = Arc::new(stub);
        engine
            .attach(
                id.clone(),
                Arc::new(crate::test_common::StubFactory::queue(vec![Arc::clone(
                    &account,
                )])),
            )
            .await
            .expect("attach");
        let vendor = crate::mutation::IdempotencyVendor::with_run_id(
            bifrost_types::RunId("caller-version".into()),
            Box::new(|_| bifrost_types::ProtocolSalt::Imap),
        );
        let counters = engine
            .bulk_set_flags_if_version(
                &id,
                vec![bifrost_types::MailMutationTarget {
                    id: ObjectId("one".into()),
                    expected_version: Some(bifrost_types::ServerVersion::ModSeq(100)),
                }],
                bifrost_types::FlagOp::Add(HashSet::from(["$seen".into()])),
                &vendor,
                bifrost_types::ProtocolKind::Imap,
            )
            .await
            .expect("campaign accounting");
        assert_eq!(counters.failed_terminal, 1);
        assert_eq!(counters.pending_retry, 0);
        assert_eq!(counters.skipped, 0);
        assert!(
            matches!(counters.error.as_deref(), Some(Error::Account(error))
            if matches!(error.kind(), bifrost_types::AccountErrorKind::ConcurrencyConflict))
        );
        {
            let seen = account.versioned_flags_seen.lock().unwrap();
            assert_eq!(seen.len(), 1, "a rejected caller token is never replayed");
            assert_eq!(
                seen[0].expected_version,
                Some(bifrost_types::ServerVersion::ModSeq(100))
            );
        }
        engine.detach(&id).await.expect("detach");
    }

    #[test]
    fn duplicate_targets_cannot_silently_choose_different_preconditions() {
        assert!(
            BulkTargets::versioned(vec![
                bifrost_types::MailMutationTarget {
                    id: ObjectId("one".into()),
                    expected_version: None
                },
                bifrost_types::MailMutationTarget {
                    id: ObjectId("one".into()),
                    expected_version: Some(bifrost_types::ServerVersion::ModSeq(100))
                },
            ])
            .is_err()
        );
    }

    #[tokio::test]
    async fn cancelled_campaign_retains_every_unsent_target() {
        let engine = SyncEngine::builder().build().expect("engine");
        let id = AccountId("cancelled-campaign".into());
        let account = Arc::new(crate::test_common::StubAccount::new(vec![]));
        engine
            .attach(
                id.clone(),
                Arc::new(crate::test_common::StubFactory::queue(vec![account])),
            )
            .await
            .expect("attach");
        engine.accounts.get(&id).expect("slot").shutdown.cancel();
        let vendor = crate::mutation::IdempotencyVendor::with_run_id(
            bifrost_types::RunId("campaign-test".into()),
            Box::new(|_| bifrost_types::ProtocolSalt::Imap),
        );
        let outcome = engine
            .bulk_destroy(
                &id,
                vec![ObjectId("one".into()), ObjectId("two".into())],
                &vendor,
                bifrost_types::ProtocolKind::Imap,
            )
            .await
            .expect("campaign accounting");
        assert_eq!(outcome.pending_retry, 2);
        assert!(matches!(
            outcome.error.as_deref(),
            Some(Error::ShuttingDown)
        ));
        engine.detach(&id).await.expect("detach");
    }
}

#[cfg(test)]
mod partial_commit_throttle_tests {
    use super::*;
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Application, Cause, CompensationOutcome, ErrorScope,
        MailboxId, PartialCommitBuilder, PrimaryLeg, RetryHint, ServerCause, ServerErrorKind,
        StepEffect, StepReplay, StepTargets, ThrottleBudget, ThrottleKey, ThrottleScope,
    };
    fn throttle(mailbox: &str, seconds: u64, budget: Option<&str>) -> AccountError {
        let mut builder = AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited {
                retry_hint: Some(RetryHint::After(Duration::from_secs(seconds))),
            }),
        )
        .scope(ErrorScope::Mailbox {
            id: MailboxId(mailbox.into()),
        })
        .throttle_scope(ThrottleScope::Mailbox);
        if let Some(budget) = budget {
            builder = builder.throttle_budget(ThrottleBudget::new(budget));
        }
        builder.try_build().unwrap()
    }
    #[tokio::test(start_paused = true)]
    async fn record_error_throttle_holds_each_leg_under_its_own_scope() {
        let mut record = PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
        let id = record.committed(
            StepEffect::Created,
            StepTargets::unknown(),
            StepReplay::Repeatable,
        );
        let sibling = throttle("A", 37, None);
        record.failed_sibling(
            Some(StepEffect::Updated),
            StepTargets::unknown(),
            StepReplay::Repeatable,
            Application::from_leaf(&sibling),
            sibling,
        );
        record.compensation(
            StepEffect::Deleted,
            StepTargets::unknown(),
            vec![id],
            CompensationOutcome::Unconfirmed(throttle("B", 53, Some("cleanup"))),
        );
        let error = AccountError::unsupported(
            bifrost_types::AccountOperation::Send,
            bifrost_types::Protocol::Graph,
        )
        .into_builder()
        .partial_commit(record)
        .try_build()
        .unwrap();
        assert!(error.recovery().is_terminal());
        let bucket = std::sync::Mutex::new(crate::recovery::ThrottleBucket::default());
        let account = AccountId("account".into());
        record_error_throttle(&bucket, &account, &error);
        let bucket = bucket.lock().unwrap();
        let now = tokio::time::Instant::now();
        assert_eq!(
            bucket.wait_for(
                &ThrottleKey::Mailbox {
                    account: account.clone(),
                    mailbox: MailboxId("A".into())
                },
                now
            ),
            Some(Duration::from_secs(37))
        );
        assert_eq!(
            bucket.wait_for(&ThrottleKey::Budget(ThrottleBudget::new("cleanup")), now),
            Some(Duration::from_secs(53))
        );
        assert_eq!(
            bucket.wait_for(
                &ThrottleKey::Mailbox {
                    account: account.clone(),
                    mailbox: MailboxId("B".into())
                },
                now
            ),
            None
        );
        assert_eq!(bucket.wait_for_account(&account, now), None);
    }
}
