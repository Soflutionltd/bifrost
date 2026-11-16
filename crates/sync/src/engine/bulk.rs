//! The bulk mutation pipeline and its campaign entry points.

use super::*;
use crate::mutation::campaign::*;

impl SyncEngine {
    /// Drive a single-account bulk-flag campaign against an attached
    /// account, observing uncertain writes and reasserting eligible unsatisfied
    /// targets within the shared retry budget.
    ///
    /// Returns the per-batch counters aggregated across the run. Once a
    /// campaign starts, failures are retained in `MutationCounters::error`
    /// alongside partial accounting. Preflight failures return `Err`.
    ///
    /// `protocol` selects the consumer's idempotency salt namespace. It is
    /// explicit because custom salt factories are consumer policy and a
    /// composed account's primary protocol need not identify the selected leg.
    /// It does not override provider error identity or cursor provenance.
    ///
    /// Mutation accounting consumes `ItemOutcome<MutationSuccess>` and
    /// classifies per-item failures via [`crate::recovery::plan_recovery`] after
    /// observing shared throttle holds:
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
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let account = slot.current.load_full();
        let mut campaign = Campaign::new(
            targets.into(),
            op,
            account.as_ref().as_ref(),
            account_id,
            vendor,
            protocol,
            &self.config,
        );

        loop {
            if slot.shutdown.is_cancelled() {
                campaign.fail(Error::ShuttingDown);
                break;
            }
            let aborting = campaign.aborting();
            let mut boundary = slot.boundary.subscribe();
            let mut generation = slot.account_generation_tx.subscribe();
            if aborting && !matches!(boundary.peek(), BoundaryRequest::Run) {
                break;
            }
            if !aborting && !slot.control.wait_until_running(&slot.shutdown).await {
                campaign.fail(Error::ShuttingDown);
                break;
            }
            // Re-route only work never submitted. Submitted and uncertain ids
            // keep their execution identity even when a reopened account changes
            // its declaration. Interning keys gives a canonical BTreeSet without
            // imposing Ord on the published ThrottleKey type.
            let account = slot.current.load_full();
            if !aborting && !campaign.blocked() {
                campaign.reroute_unsent(account.as_ref().as_ref(), account_id, vendor, protocol);
            }

            // A campaign schedules ONE ready action at a time. A held cohort
            // yields to siblings; neither a hold nor an admission re-check burns
            // an attempt, and no admission or activity spans a hold wait.
            if aborting || campaign.blocked() {
                campaign.prepare_cleanup();
            }
            let (index, mut remaining) = match campaign.select_action(
                account.as_ref().as_ref(),
                account_id,
                &slot.throttles,
            ) {
                CampaignAction::Ready { cohort, targets } => (cohort, targets),
                CampaignAction::Complete => break,
                CampaignAction::WaitUntil(deadline) => {
                    if aborting && slot.control.begin_activity().is_none() {
                        break;
                    }
                    tokio::select! {
                        () = slot.shutdown.cancelled() => { campaign.fail(Error::ShuttingDown); break; },
                        _ = boundary.changed(), if aborting => {},
                        _ = generation.changed() => {},
                        () = tokio::time::sleep_until(deadline) => {}
                    }
                    continue;
                }
            };
            let gate = slot.drive_start(account_id);
            let admitted = tokio::select! {
                () = slot.shutdown.cancelled() => { campaign.fail(Error::ShuttingDown); break; },
                _ = boundary.changed(), if aborting => { continue; },
                admitted = gate.admit_keys(crate::scheduler::WorkKind::Mutation, |account| {
                    if campaign.is_readback(index) {
                        crate::throttle::declared_keys(account, account_id, bifrost_types::ThrottleWork::Objects(&remaining))
                    } else {
                        campaign_keys(account, account_id, &remaining, campaign.operation())
                    }
                }) => match admitted {
                    Ok(admitted) => admitted,
                    Err(super::drive_start::StartRefused::ShuttingDown) => {
                        campaign.fail(Error::ShuttingDown);
                        break;
                    }
                    Err(super::drive_start::StartRefused::Paused) => {
                        if aborting { break; }
                        continue;
                    }
                    Err(super::drive_start::StartRefused::Held { until, .. }) => {
                        campaign.hold_action(index, until);
                        continue;
                    }
                },
            };
            let super::drive_start::Admitted {
                incarnation: account,
                activity,
                permit: admission,
            } = admitted;
            // Every directive this action raises is stamped with the incarnation
            // it ran against, so one queued past a reattach is declined rather
            // than restarting, resetting or disabling the replacement.
            let incarnation = Some(account.generation());
            if !campaign.declaration_matches(
                index,
                &remaining,
                account.as_ref().as_ref(),
                account_id,
            ) {
                drop(admission);
                continue;
            }
            if campaign.is_readback(index) {
                let remaining = campaign.readback_targets(index, remaining);
                if remaining.is_empty() {
                    drop(activity);
                    drop(admission);
                    continue;
                }
                use crate::mutation::readback::run_object_readback;
                let target = campaign.readback_target();
                let record = |error: &AccountError| {
                    crate::recovery::observe_failure_throttles(&slot.throttles, account_id, error);
                };
                let (result, shutting_down) = tokio::select! {
                    () = slot.shutdown.cancelled() => (None, true),
                    _ = boundary.changed(), if aborting => (None, false),
                    result = run_object_readback(account.as_ref().as_ref(), remaining.clone(), target, Some(&record)) => (Some(result), false),
                };
                if shutting_down {
                    campaign.fail(Error::ShuttingDown);
                }
                let directives = campaign.observe_readback(
                    index,
                    remaining,
                    result,
                    &account.capabilities().mutation.reassertion,
                    &slot.throttles,
                    account_id,
                );
                drop(activity);
                drop(admission);
                if !forward_campaign_directives(&slot, directives, incarnation).await {
                    campaign.fail(Error::ShuttingDown);
                    break;
                }
                continue;
            }
            let Some(mut submission) = campaign.begin_submission(
                index,
                &mut remaining,
                &account.capabilities().mutation.reassertion,
            ) else {
                drop(activity);
                drop(admission);
                continue;
            };
            let guarded = remaining
                .iter()
                .any(|id| campaign.expected_version(id).is_some());
            let mut stream = if guarded {
                let targets = remaining
                    .iter()
                    .cloned()
                    .map(|id| bifrost_types::MailMutationTarget {
                        expected_version: campaign.expected_version(&id).cloned(),
                        id,
                    })
                    .collect::<Vec<_>>();
                let target_stream = Box::pin(futures::stream::iter(targets));
                let account = Arc::clone(account.as_ref());
                match campaign.operation() {
                    BulkPipelineOp::SetFlags(flags) => {
                        account.bulk_set_flags_if_version(target_stream, flags.clone())
                    }
                    BulkPipelineOp::Move {
                        destination,
                        source,
                    } => account.bulk_move_from_if_version(
                        target_stream,
                        destination.clone(),
                        source.clone(),
                    ),
                    BulkPipelineOp::Destroy => account.bulk_destroy_if_version(target_stream),
                }
            } else {
                let target_stream: AccountStream<bifrost_types::ObjectId> =
                    Box::pin(futures::stream::iter(remaining.clone()));
                match campaign.operation() {
                    BulkPipelineOp::SetFlags(flag_op) => {
                        account.bulk_set_flags(target_stream, flag_op.clone())
                    }
                    BulkPipelineOp::Move {
                        destination,
                        source,
                    } => account.bulk_move_from(target_stream, destination.clone(), source.clone()),
                    BulkPipelineOp::Destroy => account.bulk_destroy(target_stream),
                }
            };
            // Accounting stays in the campaign; the driver owns stream lifetime.
            'submission: loop {
                let event = tokio::select! {
                    () = slot.shutdown.cancelled() => { campaign.fail(Error::ShuttingDown); break; },
                    event = stream.next() => event,
                };
                let Some(event) = event else {
                    break;
                };
                match event {
                    bifrost_types::SyncEvent::Batch(batch) => {
                        for item in batch.items {
                            if !campaign.record_item(
                                item,
                                &mut submission,
                                &slot.throttles,
                                account_id,
                            ) {
                                break 'submission;
                            }
                        }
                    }
                    bifrost_types::SyncEvent::Terminated(err) => {
                        if let Some(warning) =
                            campaign.terminate(err, &mut submission, &slot.throttles, account_id)
                        {
                            slot.delivery.publish_warning(EventScope::Account, warning);
                        }
                        break;
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
                        slot.delivery.publish_warning(EventScope::Account, warning);
                    }
                    bifrost_types::SyncEvent::Progress(_) => {}
                    _ => {}
                }
            }

            let directives =
                campaign.finish_submission(index, submission, slot.shutdown.is_cancelled());
            drop(stream);
            drop(activity);
            drop(admission);
            if !forward_campaign_directives(&slot, directives, incarnation).await {
                campaign.fail(Error::ShuttingDown);
                break;
            }
        }
        Ok(campaign.finalize())
    }
}

/// Deliver one action's engine directives to the reopen listener. Returns
/// `false` if shutdown interrupted delivery.
///
/// The single place a campaign sends on `reopen_tx`, and every caller must
/// already have dropped the action's provider stream, activity guard and
/// admission permit. The channel is bounded and its listener is serial: a
/// listener handling `RestartAccount` parks in `wait_until_running` while a
/// pause or `checkpoint_now` holds the boundary, and those wait for activity
/// to drain. A send awaited under the activity guard against a full channel
/// closes that cycle, so pause never completes and the listener never
/// resumes. The cost is that an item directive reaches the listener when its
/// action ends, not mid-stream; classification and `BlockedByEngine`
/// accounting stay immediate. Every request carries the incarnation the action
/// ran against, so one delivered after a reattach is declined.
async fn forward_campaign_directives(
    slot: &AccountSlot,
    directives: Vec<(Option<CursorScope>, AccountError)>,
    generation: Option<crate::types::AccountGeneration>,
) -> bool {
    for (scope, error) in directives {
        let request = ReopenRequest::Recovery {
            scope,
            error,
            generation,
        };
        tokio::select! {
            biased;
            () = slot.shutdown.cancelled() => return false,
            _ = slot.reopen_tx.send(request) => {}
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::ObjectId;
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
            Box::new(|_| crate::mutation::idempotency::ProtocolSalt::Imap),
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
