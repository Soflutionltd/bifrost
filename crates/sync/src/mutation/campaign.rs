//! Campaign state and synchronous mutation transitions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use crate::engine_errors::mutation_outcome_contract_failure;
use crate::error::Error;
use bifrost_types::{
    Account, AccountError, AccountId, CursorScope, EngineDirective, ItemOutcome, MembershipScope,
    MutationSuccess, ReconcileAction, RetryAdvice,
};

/// Owns campaign-wide accounting, execution identities and retry allowance.
/// The driver retains provider futures and admission guards. Keeping transitions
/// here prevents a cleanup exit from silently dropping a user's selected target.
pub(crate) struct Campaign {
    op: BulkPipelineOp,
    expected_versions: HashMap<bifrost_types::ObjectId, bifrost_types::ServerVersion>,
    retry_slots: usize,
    retry_owners: HashSet<bifrost_types::ObjectId>,
    identities: HashMap<bifrost_types::ThrottleKey, usize>,
    cohorts: Vec<MutationCohort>,
    outcomes: HashMap<bifrost_types::ObjectId, MutationBucket>,
    effects: HashMap<bifrost_types::ObjectId, bifrost_types::MutationEffect>,
    partial_commit_observations: Vec<super::PartialCommitObservation>,
    dedupe_count: u64,
    forwarded_directives: HashSet<bifrost_types::EngineDirectiveIdentity>,
    first_error: Option<Error>,
    caller_conflict: Option<Error>,
    blocked_by_engine: bool,
    cursor: usize,
    max_retries: u32,
}

pub(crate) enum CampaignAction {
    Ready {
        cohort: usize,
        targets: Vec<bifrost_types::ObjectId>,
    },
    WaitUntil(tokio::time::Instant),
    Complete,
}

/// Accounting for one provider submission; answers cannot cross rounds.
pub(crate) struct Submission {
    targets: Vec<bifrost_types::ObjectId>,
    submitted: HashSet<bifrost_types::ObjectId>,
    answered: HashSet<bifrost_types::ObjectId>,
    retry_ids: Vec<bifrost_types::ObjectId>,
    reconcile_at: tokio::time::Instant,
    stream_termination_advice: Option<RetryAdvice>,
    item_retry_advice: Option<RetryAdvice>,
    directives: Vec<(Option<CursorScope>, AccountError)>,
    terminated: bool,
}

impl Submission {
    fn new(targets: Vec<bifrost_types::ObjectId>, reconcile_at: tokio::time::Instant) -> Self {
        Self {
            submitted: targets.iter().cloned().collect(),
            targets,
            answered: HashSet::new(),
            retry_ids: Vec::new(),
            reconcile_at,
            stream_termination_advice: None,
            item_retry_advice: None,
            directives: Vec::new(),
            terminated: false,
        }
    }
}

impl Campaign {
    pub(crate) fn new(
        targets: BulkTargets,
        op: BulkPipelineOp,
        account: &dyn Account,
        account_id: &AccountId,
        vendor: &super::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
        config: &crate::types::EngineConfig,
    ) -> Self {
        let BulkTargets {
            ids: targets,
            expected_versions,
        } = targets;
        let retry_owners = HashSet::new();
        let mut identities = HashMap::new();
        let mut cohorts = Vec::new();
        let mut seen = HashSet::new();
        // Ruling (wave 7): a user who selected one message twice expects it
        // changed once, not an error. Duplicates are dropped here, first
        // occurrence wins, so each distinct ObjectId is submitted and answered
        // once per round; that is what lets the per-round answer check below
        // treat a repeated answer as a provider contract violation.
        for id in targets.into_iter().filter(|id| seen.insert(id.clone())) {
            route_target(
                &mut cohorts,
                &mut identities,
                account,
                account_id,
                id,
                &op,
                vendor,
                protocol,
            );
        }
        Self {
            op,
            expected_versions,
            retry_slots: config.mutation.retry_queue_cap,
            retry_owners,
            identities,
            cohorts,
            outcomes: HashMap::new(),
            effects: HashMap::new(),
            partial_commit_observations: Vec::new(),
            dedupe_count: 0,
            forwarded_directives: HashSet::new(),
            first_error: None,
            caller_conflict: None,
            blocked_by_engine: false,
            cursor: 0,
            max_retries: config.mutation_max_retries,
        }
    }

    pub(crate) fn aborting(&self) -> bool {
        self.first_error.is_some()
    }
    pub(crate) fn blocked(&self) -> bool {
        self.blocked_by_engine
    }
    pub(crate) fn fail(&mut self, error: Error) {
        self.first_error.get_or_insert(error);
    }
    pub(crate) fn operation(&self) -> &BulkPipelineOp {
        &self.op
    }
    pub(crate) fn expected_version(
        &self,
        id: &bifrost_types::ObjectId,
    ) -> Option<&bifrost_types::ServerVersion> {
        self.expected_versions.get(id)
    }
    pub(crate) fn key(&self, index: usize) -> bifrost_types::IdempotencyKey {
        self.cohorts[index].key.clone()
    }
    pub(crate) fn is_readback(&self, index: usize) -> bool {
        self.cohorts[index].phase == CohortPhase::Readback
    }
    pub(crate) fn hold_action(&mut self, index: usize, until: tokio::time::Instant) {
        let cohort = &mut self.cohorts[index];
        if cohort.phase == CohortPhase::Readback {
            cohort.reconcile_at = cohort.reconcile_at.max(until);
        } else {
            cohort.retry_at = cohort.retry_at.max(until);
        }
    }
    pub(crate) fn declaration_matches(
        &mut self,
        index: usize,
        targets: &[bifrost_types::ObjectId],
        account: &dyn Account,
        account_id: &AccountId,
    ) -> bool {
        // Admission can span a replacement account. Every unsent target must
        // still belong to this cohort before we use its execution identity;
        // one unchanged sibling cannot authorize a target whose routing moved.
        self.cohorts[index].submitted()
            || targets.iter().all(|id| {
                cohort_identity(
                    &mut self.identities,
                    campaign_keys(account, account_id, std::slice::from_ref(id), &self.op),
                ) == self.cohorts[index].identity
            })
    }
    pub(crate) fn readback_targets(
        &mut self,
        index: usize,
        targets: Vec<bifrost_types::ObjectId>,
    ) -> Vec<bifrost_types::ObjectId> {
        // Observation cannot establish that a caller's precondition ever held.
        let targets = targets
            .into_iter()
            .filter(|id| {
                if self.expected_versions.contains_key(id) {
                    self.outcomes.insert(id.clone(), MutationBucket::Unresolved);
                    false
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();
        if targets.is_empty() {
            self.cohorts[index].finish();
        }
        targets
    }
    pub(crate) fn readback_target(&self) -> crate::mutation::readback::ReadbackTarget<'_> {
        use crate::mutation::readback::ReadbackTarget;
        match &self.op {
            BulkPipelineOp::SetFlags(flags) => ReadbackTarget::Flags(flags),
            BulkPipelineOp::Move { destination, .. } => ReadbackTarget::Move(destination),
            BulkPipelineOp::Destroy => ReadbackTarget::Destroy,
        }
    }

    pub(crate) fn observe_readback(
        &mut self,
        index: usize,
        remaining: Vec<bifrost_types::ObjectId>,
        report: Option<crate::mutation::readback::ReadbackReport>,
        reassertion: &bifrost_types::MutationReassertion,
        throttles: &crate::throttle::ThrottleBucket,
        account_id: &AccountId,
    ) -> Vec<(Option<CursorScope>, AccountError)> {
        let mut directives = Vec::new();
        if let Some(report) = report {
            for error in report.engine_failures {
                self.blocked_by_engine = true;
                if let bifrost_types::RecoveryClass::Engine(directive) = error.recovery()
                    && should_forward_engine_recovery(&mut self.forwarded_directives, directive)
                {
                    directives.push((directive.target_scope(), error));
                }
            }
            if let Some(error) = report.termination {
                if let Error::Account(error) = &error {
                    crate::recovery::observe_failure_throttles(throttles, account_id, error);
                }
                self.first_error.get_or_insert(error);
            }
            self.read_back(index, remaining, Some(&report.objects), reassertion);
        } else {
            self.read_back(index, remaining, None, reassertion);
        }
        directives
    }

    pub(crate) fn begin_submission(
        &mut self,
        index: usize,
        remaining: &mut Vec<bifrost_types::ObjectId>,
        reassertion: &bifrost_types::MutationReassertion,
    ) -> Option<Submission> {
        // A reopen can replace the account while admission is pending.
        remaining.retain(|id| {
            if matches!(self.outcomes.get(id), Some(MutationBucket::ReadyReassert))
                && (self.first_error.is_some()
                    || self.blocked_by_engine
                    || !reassertion_declared(&self.op, reassertion))
            {
                self.outcomes.insert(
                    id.clone(),
                    if self.blocked_by_engine && self.first_error.is_none() {
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
            self.cohorts[index].finish();
            return None;
        }
        for id in remaining.iter() {
            self.outcomes.insert(id.clone(), MutationBucket::InFlight);
        }
        self.cohorts[index].phase = CohortPhase::Submitted;
        Some(Submission::new(
            remaining.clone(),
            self.cohorts[index].reconcile_at,
        ))
    }

    pub(crate) fn reroute_unsent(
        &mut self,
        account: &dyn Account,
        account_id: &AccountId,
        vendor: &super::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) {
        let mut reroute = Vec::new();
        for cohort in &mut self.cohorts {
            if !cohort.submitted() && !cohort.done() {
                cohort.remaining.retain(|id| {
                    let identity = cohort_identity(
                        &mut self.identities,
                        campaign_keys(account, account_id, std::slice::from_ref(id), &self.op),
                    );
                    if identity == cohort.identity {
                        true
                    } else {
                        reroute.push(id.clone());
                        false
                    }
                });
                cohort.targets = cohort.remaining.clone();
                if cohort.remaining.is_empty() {
                    cohort.finish();
                }
            }
        }
        for id in reroute {
            route_target(
                &mut self.cohorts,
                &mut self.identities,
                account,
                account_id,
                id,
                &self.op,
                vendor,
                protocol,
            );
        }
    }

    pub(crate) fn prepare_cleanup(&mut self) {
        for cohort in self.cohorts.iter_mut().filter(|cohort| !cohort.done()) {
            if !cohort.submitted() {
                for id in &cohort.targets {
                    self.outcomes.insert(
                        id.clone(),
                        if self.first_error.is_some() {
                            MutationBucket::PendingRetry
                        } else {
                            MutationBucket::BlockedByEngine
                        },
                    );
                }
                cohort.finish();
                continue;
            }
            for id in &cohort.targets {
                if matches!(self.outcomes.get(id), Some(MutationBucket::ReadyReassert)) {
                    self.outcomes.insert(
                        id.clone(),
                        if self.first_error.is_some() {
                            MutationBucket::Mismatched
                        } else {
                            MutationBucket::BlockedByEngine
                        },
                    );
                }
            }
            if self.blocked_by_engine && self.first_error.is_none() {
                for id in &cohort.targets {
                    if matches!(self.outcomes.get(id), Some(MutationBucket::PendingRetry)) {
                        self.outcomes
                            .insert(id.clone(), MutationBucket::BlockedByEngine);
                    }
                }
            }
            cohort.phase = CohortPhase::Readback;
        }
    }

    pub(crate) fn select_action(
        &mut self,
        account: &dyn Account,
        account_id: &AccountId,
        throttles: &crate::throttle::ThrottleBucket,
    ) -> CampaignAction {
        let readback_set: HashSet<_> = unresolved_readback_ids(&self.outcomes)
            .into_iter()
            .collect();
        let mut selected = None;
        let mut earliest = None;
        for offset in 0..self.cohorts.len() {
            let index = (self.cursor + offset) % self.cohorts.len();
            let cohort = &mut self.cohorts[index];
            if cohort.done() {
                continue;
            }
            let ids = if cohort.phase == CohortPhase::Readback {
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
                cohort.finish();
                continue;
            }
            let keys = if cohort.phase == CohortPhase::Readback {
                crate::throttle::declared_keys(
                    account,
                    account_id,
                    bifrost_types::ThrottleWork::Objects(&ids),
                )
            } else {
                campaign_keys(account, account_id, &ids, &self.op)
            };
            let now = tokio::time::Instant::now();
            let mut ready_at = if cohort.phase == CohortPhase::Readback {
                cohort.reconcile_at
            } else {
                cohort.retry_at
            };
            if let Some(wait) = crate::throttle::throttle_wait(throttles, account_id, &keys) {
                ready_at = ready_at.max(now + wait);
            }
            if ready_at <= now {
                selected = Some((index, ids));
                break;
            }
            earliest =
                Some(earliest.map_or(ready_at, |old: tokio::time::Instant| old.min(ready_at)));
        }
        match selected {
            Some((cohort, targets)) => {
                self.cursor = (cohort + 1) % self.cohorts.len();
                CampaignAction::Ready { cohort, targets }
            }
            None => earliest.map_or(CampaignAction::Complete, CampaignAction::WaitUntil),
        }
    }

    fn read_back(
        &mut self,
        index: usize,
        remaining: Vec<bifrost_types::ObjectId>,
        report: Option<
            &HashMap<bifrost_types::ObjectId, crate::mutation::readback::ObjectReadback>,
        >,
        reassertion: &bifrost_types::MutationReassertion,
    ) {
        if let Some(report) = report {
            for id in &remaining {
                let origin = self.outcomes.get(id).copied();
                let result = report
                    .get(id)
                    .copied()
                    .unwrap_or(crate::mutation::readback::ObjectReadback::Unresolved);
                let eligible = matches!(
                    origin,
                    Some(
                        MutationBucket::PendingReadback(ReadbackOrigin::Reassertable)
                            | MutationBucket::PendingRetry
                    )
                ) && !self.expected_versions.contains_key(id)
                    && reassertion_declared(&self.op, reassertion)
                    && self.first_error.is_none()
                    && !self.blocked_by_engine
                    && retry_available(self.cohorts[index].attempt, self.max_retries)
                    && (self.retry_owners.contains(id) || self.retry_slots > 0);
                use crate::mutation::readback::ObjectReadback;
                let bucket = match result {
                    ObjectReadback::Satisfied => MutationBucket::Skipped,
                    ObjectReadback::Unresolved => MutationBucket::Unresolved,
                    ObjectReadback::Unsatisfied if eligible => {
                        if self.retry_owners.insert(id.clone()) {
                            self.retry_slots -= 1;
                        }
                        MutationBucket::ReadyReassert
                    }
                    ObjectReadback::Unsatisfied => MutationBucket::Mismatched,
                };
                self.outcomes.insert(id.clone(), bucket);
            }
        } else {
            for id in &remaining {
                self.outcomes.insert(id.clone(), MutationBucket::Unresolved);
            }
        }
        self.cohorts[index].remaining = remaining
            .into_iter()
            .filter(|id| matches!(self.outcomes.get(id), Some(MutationBucket::ReadyReassert)))
            .collect();
        if self.cohorts[index].remaining.is_empty() {
            self.cohorts[index].finish();
        } else {
            self.cohorts[index].phase = CohortPhase::Submitted;
            self.cohorts[index].retry_at = tokio::time::Instant::now();
        }
    }

    pub(crate) fn record_item(
        &mut self,
        item: ItemOutcome<MutationSuccess>,
        submission: &mut Submission,
        throttles: &crate::throttle::ThrottleBucket,
        account_id: &AccountId,
    ) -> bool {
        let id = match &item {
            ItemOutcome::Succeeded(value) => &value.item,
            ItemOutcome::Failed(value) => &value.item,
            ItemOutcome::Uncertain(value) => &value.item,
        };
        // Validate before side effects, conflicts, throttles or accounting.
        // A mail user must never see an unrelated message counted, or a
        // repeated answer overwrite a result already reported this round.
        let id = bifrost_types::ObjectId(id.0.clone());
        if !submission.submitted.contains(&id) || !submission.answered.insert(id) {
            self.first_error
                .get_or_insert(Error::Account(mutation_outcome_contract_failure()));
            return false;
        }
        let observed = match &item {
            ItemOutcome::Failed(f) => Some((&f.item, &f.error)),
            ItemOutcome::Uncertain(u) => Some((&u.item, &u.error)),
            _ => None,
        };
        if let Some((id, error)) = observed
            && error.partial_commit().is_some()
        {
            self.partial_commit_observations
                .push(crate::mutation::PartialCommitObservation {
                    target: Some(bifrost_types::ObjectId(id.0.clone())),
                    error: error.clone(),
                });
        }
        if let ItemOutcome::Failed(failure) = &item {
            let id = bifrost_types::ObjectId(failure.item.0.clone());
            if self.expected_versions.contains_key(&id)
                && matches!(
                    failure.error.kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                )
            {
                // A read-back can observe the requested state without proving
                // that this caller's precondition ever held. Keep the refusal.
                self.outcomes.insert(id, MutationBucket::FailedTerminal);
                self.caller_conflict
                    .get_or_insert(Error::Account(failure.error.clone()));
                return true;
            }
        }

        if let ItemOutcome::Succeeded(success) = &item
            && let MutationSuccess::Downgraded { actual } = &success.output
        {
            self.effects.insert(
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
            ItemOutcome::Succeeded(s) => bifrost_types::ObjectId(s.item.0.clone()),
            ItemOutcome::Failed(f) => bifrost_types::ObjectId(f.item.0.clone()),
            ItemOutcome::Uncertain(u) => bifrost_types::ObjectId(u.item.0.clone()),
        };
        if let Some((directive, error)) = classify_item_outcome(
            item,
            &mut self.outcomes,
            &mut submission.retry_ids,
            &mut self.dedupe_count,
            throttles,
            account_id,
            &mut submission.item_retry_advice,
        ) {
            if should_forward_engine_recovery(&mut self.forwarded_directives, &directive) {
                submission
                    .directives
                    .push((directive.target_scope(), error));
            }
            self.blocked_by_engine = true;
        }
        if let Some(error) = error {
            // Classification records every failed leg once, including
            // uncertain outcomes; timing here gates only read-back.
            let delay = if matches!(
                self.outcomes.get(&item_id),
                Some(MutationBucket::PendingReadback(_))
            ) {
                match error.recovery() {
                    bifrost_types::RecoveryClass::Reconcile(advice) => {
                        Some(crate::recovery::reconcile_delay(
                            advice,
                            std::time::SystemTime::now(),
                            crate::recovery::HINTLESS_RETRY_WAIT,
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
                            crate::recovery::HINTLESS_RETRY_WAIT,
                        ))
                    }
                    _ => None,
                }
            } else {
                None
            };
            if let Some(delay) = delay {
                submission.reconcile_at = submission
                    .reconcile_at
                    .max(tokio::time::Instant::now() + delay);
            }
        }
        true
    }

    pub(crate) fn terminate(
        &mut self,
        err: AccountError,
        submission: &mut Submission,
        throttles: &crate::throttle::ThrottleBucket,
        account_id: &AccountId,
    ) -> Option<bifrost_types::Warning> {
        use crate::recovery::RecoveryPlan;
        submission.terminated = true;
        let original = err.clone();
        if original.partial_commit().is_some() {
            self.partial_commit_observations
                .push(crate::mutation::PartialCommitObservation {
                    target: None,
                    error: original.clone(),
                });
        }
        match crate::recovery::dispatch_stream_failure(err, throttles, account_id) {
            RecoveryPlan::Retry(advice) => {
                if matches!(
                    advice.disposition,
                    bifrost_types::RetryDisposition::SameRequest
                        | bifrost_types::RetryDisposition::AfterAuthRefresh
                ) {
                    queue_unresolved_for_retry(
                        &submission.targets,
                        &self.outcomes,
                        &mut submission.retry_ids,
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
                    for id in &submission.targets {
                        if self.expected_versions.contains_key(id) {
                            queue_unresolved_for_retry(
                                std::slice::from_ref(id),
                                &self.outcomes,
                                &mut submission.retry_ids,
                            );
                        } else if matches!(self.outcomes.get(id), Some(MutationBucket::InFlight)) {
                            self.outcomes
                                .insert(id.clone(), MutationBucket::PendingReadback(origin));
                        }
                    }
                    submission.reconcile_at = submission.reconcile_at.max(
                        tokio::time::Instant::now()
                            + crate::recovery::retry_delay(
                                &advice,
                                std::time::SystemTime::now(),
                                crate::recovery::HINTLESS_RETRY_WAIT,
                            ),
                    );
                }
                submission.stream_termination_advice = Some(advice);
            }
            RecoveryPlan::Reconcile(advice) => {
                submission.reconcile_at = submission.reconcile_at.max(
                    tokio::time::Instant::now()
                        + crate::recovery::reconcile_delay(
                            &advice,
                            std::time::SystemTime::now(),
                            crate::recovery::HINTLESS_RETRY_WAIT,
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
                for id in &submission.targets {
                    if matches!(self.outcomes.get(id), Some(MutationBucket::InFlight))
                        || (origin == ReadbackOrigin::Protected
                            && matches!(
                                self.outcomes.get(id),
                                Some(MutationBucket::PendingReadback(
                                    ReadbackOrigin::Reassertable
                                ))
                            ))
                    {
                        self.outcomes
                            .insert(id.clone(), MutationBucket::PendingReadback(origin));
                    }
                }
                if wants_dedupe {
                    self.dedupe_count = self.dedupe_count.saturating_add(1);
                    let warning = bifrost_types::Warning::user_safe(
                        bifrost_types::WarningKind::Engine(
                            bifrost_types::EngineNotice::DedupeByClientId,
                        ),
                        "reconcile requested dedupe-by-client-id; consumer must dedupe",
                    )
                    .with_error(original.clone());
                    return Some(warning);
                }
            }
            RecoveryPlan::Engine(directive) => {
                if should_forward_engine_recovery(&mut self.forwarded_directives, &directive) {
                    submission
                        .directives
                        .push((directive.target_scope(), original));
                }
                self.blocked_by_engine = true;
                for id in &submission.targets {
                    if matches!(
                        self.outcomes.get(id),
                        Some(MutationBucket::InFlight | MutationBucket::PendingRetry)
                    ) {
                        self.outcomes
                            .insert(id.clone(), MutationBucket::BlockedByEngine);
                    }
                }
            }
            RecoveryPlan::Terminal(fatal) => {
                self.first_error
                    .get_or_insert(Error::Account(fatal.into_inner()));
            }
        }
        None
    }

    pub(crate) fn finish_submission(
        &mut self,
        index: usize,
        submission: Submission,
        shutting_down: bool,
    ) -> Vec<(Option<CursorScope>, AccountError)> {
        if !submission.terminated && submission.answered.len() != submission.submitted.len() {
            self.first_error
                .get_or_insert(Error::Account(mutation_outcome_contract_failure()));
        }
        self.submit(
            index,
            &submission.targets,
            submission.retry_ids,
            submission.reconcile_at,
            submission
                .stream_termination_advice
                .or(submission.item_retry_advice),
            shutting_down,
        );
        submission.directives
    }

    fn submit(
        &mut self,
        index: usize,
        remaining: &[bifrost_types::ObjectId],
        retry_ids: Vec<bifrost_types::ObjectId>,
        reconcile_at: tokio::time::Instant,
        advice: Option<RetryAdvice>,
        shutting_down: bool,
    ) {
        self.cohorts[index].reconcile_at = reconcile_at;
        self.cohorts[index].attempt = self.cohorts[index].attempt.saturating_add(1);
        // Every submitted target must occupy a lane, even when a stream
        // ends without reporting it. Such an omission is uncertain.
        for id in remaining {
            if matches!(self.outcomes.get(id), Some(MutationBucket::InFlight)) {
                self.outcomes.insert(
                    id.clone(),
                    if shutting_down {
                        MutationBucket::Unresolved
                    } else if self.blocked_by_engine {
                        MutationBucket::BlockedByEngine
                    } else {
                        MutationBucket::PendingReadback(ReadbackOrigin::Reassertable)
                    },
                );
            }
        }
        let retry_set: HashSet<_> = retry_ids.into_iter().collect();
        let mut next = Vec::new();
        for id in retry_candidates(remaining, &retry_set, &self.outcomes) {
            self.outcomes
                .insert(id.clone(), MutationBucket::PendingRetry);
            if retry_available(self.cohorts[index].attempt, self.max_retries)
                && (self.retry_owners.contains(&id) || self.retry_slots > 0)
            {
                if self.retry_owners.insert(id.clone()) {
                    self.retry_slots -= 1;
                }
                next.push(id);
            }
        }
        if self.first_error.is_some() || self.blocked_by_engine || next.is_empty() {
            self.cohorts[index].phase = CohortPhase::Readback;
        } else {
            self.cohorts[index].remaining = next;
            if let Some(advice) = advice {
                self.cohorts[index].retry_at = tokio::time::Instant::now()
                    + crate::recovery::retry_delay(
                        &advice,
                        std::time::SystemTime::now(),
                        crate::recovery::HINTLESS_RETRY_WAIT,
                    );
            }
        }
    }

    pub(crate) fn finalize(mut self) -> super::MutationCounters {
        // Never-submitted targets are known unsent and never observed. A
        // cancelled, paused, detached or failed observation leaves submitted
        // unresolved targets explicit rather than implying a terminal failure.
        for cohort in &self.cohorts {
            for id in &cohort.targets {
                if !cohort.submitted() {
                    self.outcomes
                        .entry(id.clone())
                        .or_insert(MutationBucket::PendingRetry);
                } else if !cohort.done() {
                    let value = self
                        .outcomes
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
        let mut totals = counters_from_outcomes(&self.outcomes);
        totals.error = self.first_error.or(self.caller_conflict).map(Arc::new);
        totals.effects = self.effects.into_iter().collect();
        totals.partial_commit_observations = self.partial_commit_observations;
        totals.dedupe_by_client_id = self.dedupe_count;
        totals
    }
}

pub(crate) struct BulkTargets {
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
    pub(crate) fn versioned(
        targets: Vec<bifrost_types::MailMutationTarget>,
    ) -> Result<Self, Error> {
        let mut ids = Vec::new();
        let mut versions = HashMap::new();
        for target in targets {
            if let Some(previous) = versions.get(&target.id) {
                if previous != &target.expected_version {
                    return Err(Error::ConflictingCallerVersions(target.id));
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

// Completed unsent work must remain distinguishable from a completed submission
// when the campaign constructs its final per-target outcomes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CohortPhase {
    Unsubmitted,
    Submitted,
    Readback,
    DoneUnsubmitted,
    DoneSubmitted,
}

impl MutationCohort {
    fn submitted(&self) -> bool {
        !matches!(
            self.phase,
            CohortPhase::Unsubmitted | CohortPhase::DoneUnsubmitted
        )
    }

    fn done(&self) -> bool {
        matches!(
            self.phase,
            CohortPhase::DoneUnsubmitted | CohortPhase::DoneSubmitted
        )
    }

    fn finish(&mut self) {
        self.phase = if self.submitted() {
            CohortPhase::DoneSubmitted
        } else {
            CohortPhase::DoneUnsubmitted
        };
    }
}

struct MutationCohort {
    identity: std::collections::BTreeSet<usize>,
    key: bifrost_types::IdempotencyKey,
    targets: Vec<bifrost_types::ObjectId>,
    remaining: Vec<bifrost_types::ObjectId>,
    phase: CohortPhase,
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
        .find(|c| c.identity == identity && !c.submitted() && !c.done())
    {
        cohort.targets.push(id.clone());
        cohort.remaining.push(id);
    } else {
        cohorts.push(MutationCohort {
            identity,
            key: vendor.next(protocol),
            targets: vec![id.clone()],
            remaining: vec![id],
            phase: CohortPhase::Unsubmitted,
            attempt: 0,
            retry_at: tokio::time::Instant::now(),
            reconcile_at: tokio::time::Instant::now(),
        });
    }
}

/// Which bulk mutation a campaign drives.
///
/// Selects the op-specific seams in the shared pipeline: the wire submit
/// call, the matching read-back target (flag judge, membership or absence)
/// and the reassertion declaration it reads. Everything else in the
/// idempotency / retry / recovery loop is shared.
pub(crate) enum BulkPipelineOp {
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
pub(crate) fn campaign_keys(
    account: &dyn Account,
    account_id: &AccountId,
    targets: &[bifrost_types::ObjectId],
    op: &BulkPipelineOp,
) -> Vec<bifrost_types::ThrottleKey> {
    use bifrost_types::ThrottleWork;
    let mut keys =
        crate::throttle::declared_keys(account, account_id, ThrottleWork::Objects(targets));
    if let BulkPipelineOp::Move {
        destination,
        source,
    } = op
    {
        keys.extend(crate::throttle::declared_keys(
            account,
            account_id,
            ThrottleWork::Membership(destination),
        ));
        if let Some(source) = source {
            keys.extend(crate::throttle::declared_keys(
                account,
                account_id,
                ThrottleWork::Membership(source),
            ));
        }
    }
    keys
}

/// What a valid unsatisfied observation of a read-back id may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadbackOrigin {
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
pub(crate) enum MutationBucket {
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
pub(crate) fn queue_unresolved_for_retry(
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
pub(crate) fn unresolved_readback_ids(
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
    let fallback = crate::recovery::HINTLESS_RETRY_WAIT;
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
pub(crate) fn classify_item_outcome(
    item: ItemOutcome<MutationSuccess>,
    outcomes: &mut HashMap<bifrost_types::ObjectId, MutationBucket>,
    retry_ids: &mut Vec<bifrost_types::ObjectId>,
    dedupe_count: &mut u64,
    throttles: &crate::throttle::ThrottleBucket,
    account_id: &AccountId,
    // The retry advice of the per-item failures that were QUEUED for
    // resubmission, folded to the longest delay. A stream that ends `Done`
    // after emitting per-item retryable failures carries no stream-level
    // advice, and without this the campaign looped straight into its next
    // attempt with no delay at all - up to `mutation_max_retries`
    // back-to-back resubmissions against a provider that just said no.
    item_retry_advice: &mut Option<RetryAdvice>,
) -> Option<(EngineDirective, AccountError)> {
    use crate::recovery::RecoveryPlan;
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
            // Per-item accounting is not a failed stream: retain provider advice
            // and shared holds without emitting one recovery log per item.
            crate::recovery::observe_failure_throttles(throttles, account_id, &failure.error);
            match crate::recovery::plan_recovery(failure.error) {
                RecoveryPlan::Retry(advice) => {
                    // A per-item 429 (Graph files them per `$batch`
                    // subresponse) carries the same shared deadline a
                    // stream-level one would; record it so polls and
                    // sibling accounts observe it too.
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
            crate::recovery::observe_failure_throttles(throttles, account_id, &uncertain.error);
            let id = bifrost_types::ObjectId(uncertain.item.0);
            outcomes.insert(
                id,
                MutationBucket::PendingReadback(uncertain_origin(&uncertain.error)),
            );
            None
        }
    }
}

/// Return whether this campaign has not yet forwarded this exact directive.
/// A per-item engine failure can name the same directive hundreds of times;
/// the reopen listener needs one request per distinct directive, not one per
/// failed object - and not one per campaign either.
pub(crate) fn should_forward_engine_recovery(
    forwarded: &mut HashSet<bifrost_types::EngineDirectiveIdentity>,
    directive: &EngineDirective,
) -> bool {
    forwarded.insert(directive.identity())
}

pub(crate) fn counters_from_outcomes(
    outcomes: &HashMap<bifrost_types::ObjectId, MutationBucket>,
) -> crate::mutation::MutationCounters {
    let mut counters = crate::mutation::MutationCounters::default();
    for bucket in outcomes.values() {
        match bucket {
            MutationBucket::InFlight => {
                debug_assert!(false, "submission round left an in-flight target");
                counters.pending_retry = counters.pending_retry.saturating_add(1);
            }
            MutationBucket::Applied => {
                counters.applied = counters.applied.saturating_add(1);
            }
            MutationBucket::Downgraded => {
                counters.downgraded = counters.downgraded.saturating_add(1);
            }
            MutationBucket::Skipped => {
                counters.skipped = counters.skipped.saturating_add(1);
            }
            MutationBucket::FailedTerminal => {
                counters.failed_terminal = counters.failed_terminal.saturating_add(1);
            }
            MutationBucket::BlockedByEngine => {
                counters.blocked_by_engine = counters.blocked_by_engine.saturating_add(1);
            }
            MutationBucket::PendingRetry
            | MutationBucket::PendingReadback(_)
            | MutationBucket::ReadyReassert
            | MutationBucket::Mismatched
            | MutationBucket::Unresolved => {
                counters.pending_retry = counters.pending_retry.saturating_add(1);
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
    use crate::engine::SyncEngine;
    use bifrost_types::{
        Batch, BatchItemId, BatchSuccess, MutationEffect, ObjectId, PageBoundary, SyncEvent,
    };

    #[test]
    fn declaration_matches_requires_every_unsent_target_on_the_replacement() {
        use bifrost_types::{ProtocolKind, ProtocolSalt, RunId, ThrottleBudget, ThrottleKey};

        fn identity(key: &bifrost_types::IdempotencyKey) -> (&RunId, u64, &ProtocolSalt) {
            (&key.run_id, key.sequence, &key.protocol_salt)
        }

        let account_id = AccountId("replacement-routing".into());
        let targets = vec![ObjectId("stays".into()), ObjectId("moves".into())];
        let original_key = ThrottleKey::Budget(ThrottleBudget::new("original"));
        let moved_key = ThrottleKey::Budget(ThrottleBudget::new("moved"));
        let mut original = crate::test_common::StubAccount::new(Vec::new());
        let mut replacement = crate::test_common::StubAccount::new(Vec::new());
        let initial_key = original_key.clone();
        original.object_keys = Some(Arc::new(move |_| vec![initial_key.clone()]));
        replacement.object_keys = Some(Arc::new(move |ids| {
            ids.iter()
                .map(|id| {
                    if id.0 == "moves" {
                        moved_key.clone()
                    } else {
                        original_key.clone()
                    }
                })
                .collect()
        }));
        let vendor = super::super::IdempotencyVendor::with_run_id(
            RunId("declaration".into()),
            Box::new(|_| ProtocolSalt::Imap),
        );
        let mut campaign = Campaign::new(
            targets.clone().into(),
            BulkPipelineOp::Destroy,
            &original,
            &account_id,
            &vendor,
            ProtocolKind::Imap,
            &crate::types::EngineConfig::default(),
        );
        assert_eq!(campaign.cohorts.len(), 1, "both targets start together");
        assert!(campaign.declaration_matches(0, &targets, &original, &account_id));
        let key = campaign.key(0);
        // The provider snapshot loaded AFTER admission now disagrees on only
        // the second target. `any` would send it under its sibling's identity.
        assert!(!campaign.declaration_matches(0, &targets, &replacement, &account_id));
        campaign.reroute_unsent(&replacement, &account_id, &vendor, ProtocolKind::Imap);
        assert_eq!(campaign.cohorts.len(), 2);
        for (index, target) in targets.iter().enumerate() {
            assert_eq!(campaign.cohorts[index].remaining, vec![target.clone()]);
            assert!(campaign.declaration_matches(
                index,
                std::slice::from_ref(target),
                &replacement,
                &account_id,
            ));
        }
        assert_eq!(
            identity(&campaign.key(0)),
            identity(&key),
            "unchanged targets keep their identity"
        );
        assert_ne!(
            identity(&campaign.key(1)),
            identity(&key),
            "rerouted targets get their own identity"
        );

        // Once submitted, the original identity must survive a replacement:
        // uncertain writes must never acquire a fresh deduplication identity.
        let mut submitted = vec![targets[1].clone()];
        assert!(
            campaign
                .begin_submission(1, &mut submitted, &Default::default())
                .is_some()
        );
        assert!(campaign.declaration_matches(1, &submitted, &original, &account_id));
    }

    #[tokio::test]
    async fn campaign_rejects_unknown_duplicate_and_missing_answers() {
        for (answers, applied) in [
            (vec!["unrequested"], 0),
            (vec!["requested", "requested"], 1),
            (vec![], 0),
        ] {
            let engine = SyncEngine::builder().build().expect("engine");
            let id = AccountId("outcome-contract".into());
            let stub = crate::test_common::StubAccount::new(Vec::new());
            let mut script = answers
                .into_iter()
                .map(|answer| {
                    // Separate pages ensure duplicate detection spans the stream.
                    SyncEvent::Batch(Batch {
                        items: vec![ItemOutcome::Succeeded(BatchSuccess::new(
                            BatchItemId(answer.into()),
                            MutationSuccess::Applied,
                        ))],
                        page_boundary: PageBoundary::Final,
                        server_latency: Duration::ZERO,
                        bytes_in: 0,
                        checkpoint: None,
                    })
                })
                .collect::<Vec<_>>();
            script.push(SyncEvent::Done(None));
            *stub.bulk_flags_script.lock().unwrap() = Some(script);
            engine
                .attach(
                    id.clone(),
                    Arc::new(crate::test_common::StubFactory::queue(vec![Arc::new(stub)])),
                )
                .await
                .expect("attach");
            let vendor = crate::mutation::IdempotencyVendor::with_run_id(
                bifrost_types::RunId("contract".into()),
                Box::new(|_| bifrost_types::ProtocolSalt::Imap),
            );
            let counters = engine
                .bulk_set_flags(
                    &id,
                    vec![ObjectId("requested".into())],
                    bifrost_types::FlagOp::Add(bifrost_types::FlagSet::from(["$seen".into()])),
                    &vendor,
                    bifrost_types::ProtocolKind::Imap,
                )
                .await
                .expect("partial campaign accounting");
            assert!(
                matches!(counters.error.as_deref(), Some(Error::Account(error))
                if matches!(error.kind(), bifrost_types::AccountErrorKind::Internal(
                    bifrost_types::InternalErrorKind::AccountContract
                )))
            );
            assert_eq!(counters.applied, applied);
            assert_eq!(counters.pending_retry, 1 - applied);
            engine.detach(&id).await.expect("detach");
        }
    }

    #[tokio::test]
    async fn a_target_selected_twice_is_submitted_and_answered_once() {
        let engine = SyncEngine::builder().build().expect("engine");
        let id = AccountId("duplicate-input".into());
        let stub = crate::test_common::StubAccount::new(Vec::new());
        // One answer only: if the duplicate reached the provider, the round
        // would end with a target unanswered and raise a contract error.
        *stub.bulk_flags_script.lock().unwrap() = Some(vec![
            SyncEvent::Batch(Batch {
                items: vec![ItemOutcome::Succeeded(BatchSuccess::new(
                    BatchItemId("twice".into()),
                    MutationSuccess::Applied,
                ))],
                page_boundary: PageBoundary::Final,
                server_latency: Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            }),
            SyncEvent::Done(None),
        ]);
        engine
            .attach(
                id.clone(),
                Arc::new(crate::test_common::StubFactory::queue(vec![Arc::new(stub)])),
            )
            .await
            .expect("attach");
        let vendor = crate::mutation::IdempotencyVendor::with_run_id(
            bifrost_types::RunId("duplicate".into()),
            Box::new(|_| bifrost_types::ProtocolSalt::Imap),
        );
        let counters = engine
            .bulk_set_flags(
                &id,
                vec![ObjectId("twice".into()), ObjectId("twice".into())],
                bifrost_types::FlagOp::Add(bifrost_types::FlagSet::from(["$seen".into()])),
                &vendor,
                bifrost_types::ProtocolKind::Imap,
            )
            .await
            .expect("campaign");
        assert!(counters.error.is_none(), "{:?}", counters.error);
        assert_eq!(counters.applied, 1);
        engine.detach(&id).await.expect("detach");
    }

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
    fn origin_error(cause: bifrost_types::PrimaryCause) -> AccountError {
        bifrost_types::AccountErrorBuilder::new(cause)
            .scope(bifrost_types::ErrorScope::Cursor(CursorScope::Account))
            .try_build()
            .unwrap()
    }
    #[test]
    fn uncertain_origin_protects_dedupe_engine_and_terminal() {
        use bifrost_types::*;
        let dedupe = AccountErrorBuilder::new(PrimaryCause::Protocol {
            kind: ProtocolErrorKind::PartialResponse,
            wire: WireCause::MalformedResponse {
                protocol: Protocol::Graph,
                detail: None,
            },
        })
        .operation(AccountOperation::BulkDestroy)
        .try_build()
        .unwrap();
        let engine = origin_error(PrimaryCause::State(StateCause::CursorInvalid {
            scope: CursorScope::Account,
        }));
        let terminal = AccountError::unsupported(AccountOperation::UpdateFlags, Protocol::Imap);
        for error in [&dedupe, &engine, &terminal] {
            assert_eq!(uncertain_origin(error), ReadbackOrigin::Protected);
        }
        let transport = AccountErrorBuilder::new(PrimaryCause::Transport(TransportCause::new(
            TransportErrorKind::Network,
            None,
        )))
        .operation(AccountOperation::BulkDestroy)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::InFlight,
        )))
        .try_build()
        .unwrap();
        assert_eq!(uncertain_origin(&transport), ReadbackOrigin::Reassertable);
        let conflict = origin_error(PrimaryCause::State(StateCause::ConcurrencyConflict));
        assert_eq!(uncertain_origin(&conflict), ReadbackOrigin::Reassertable);
        // An idempotent transport failure derives Retry(SameRequest): the
        // uncertainty lane replaced the blind retry with an observation.
        let same_request = AccountErrorBuilder::new(PrimaryCause::Transport(TransportCause::new(
            TransportErrorKind::Network,
            None,
        )))
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
        let engine = origin_error(PrimaryCause::State(StateCause::CursorInvalid {
            scope: CursorScope::Account,
        }));
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
                &crate::throttle::ThrottleBucket::default(),
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
        let throttles = crate::throttle::ThrottleBucket::new();
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
            bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::State(
                bifrost_types::StateCause::ConcurrencyConflict,
            ))
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
                bifrost_types::FlagOp::Add(bifrost_types::FlagSet::from(["$seen".into()])),
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
}

#[cfg(test)]
mod partial_commit_throttle_tests {
    use super::*;
    use bifrost_types::{
        AccountErrorBuilder, Application, CompensationOutcome, ErrorScope, PartialCommitBuilder,
        PrimaryCause, PrimaryLeg, PrincipalId, RetryHint, ServerCause, StepEffect, StepReplay,
        StepTargets, ThrottleBudget, ThrottleKey, ThrottleScope,
    };
    fn throttle(mailbox: &str, seconds: u64, budget: Option<&str>) -> AccountError {
        let mut builder =
            AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
                throttle: Default::default(),
                retry_hint: Some(RetryHint::After(Duration::from_secs(seconds))),
            }))
            .scope(ErrorScope::Owner {
                id: PrincipalId(mailbox.into()),
            })
            .with_throttle(|throttle| throttle.scope = Some(ThrottleScope::Mailbox));
        if let Some(budget) = budget {
            builder =
                builder.with_throttle(|throttle| throttle.add_budget(ThrottleBudget::new(budget)));
        }
        builder.try_build().unwrap()
    }
    #[tokio::test(start_paused = true)]
    async fn observe_failure_throttles_holds_each_leg_under_its_own_scope() {
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
        let bucket = crate::throttle::ThrottleBucket::default();
        let account = AccountId("account".into());
        crate::recovery::observe_failure_throttles(&bucket, &account, &error);
        let now = tokio::time::Instant::now();
        assert_eq!(
            bucket.wait_for(
                &ThrottleKey::Owner {
                    account: account.clone(),
                    owner: PrincipalId("A".into())
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
                &ThrottleKey::Owner {
                    account: account.clone(),
                    owner: PrincipalId("B".into())
                },
                now
            ),
            None
        );
        assert_eq!(bucket.wait_for_account(&account, now), None);
    }
}
