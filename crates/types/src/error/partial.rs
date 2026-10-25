//! Historical write effects and operation-level replay safety.
//!
//! The public operation attaches once; its leaf classification stays intact.
//! Successful compensation resolves only the effects it names. Plans describe
//! execution constraints after prerequisites and directives, not continuations.
use super::{
    AccountError, AccountErrorBuildError, AccountErrorKind, EngineDirective, ErrorScope,
    ReconcileAction, ReconcileAdvice, ReconcileGuidance, ReconcileReason, RecoveryClass,
    RetryAdvice, RetryDisposition, RetryReason, ServerErrorKind, TransmissionState,
};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
/// Sequential record-local identity; the primary leg is zero. Serialized as u64.
pub struct StepId(usize);
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
pub enum StepEffect {
    Created,
    Updated,
    Moved,
    Copied,
    MarkedDeleted,
    Deleted,
    Submitted,
    Cancelled,
}
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum StepReplay {
    /// Replay converges, or leaves only a by-product such as an orphan draft.
    Repeatable,
    /// Replay duplicates the intended effect, addresses a changed identity, or
    /// violates a precondition this effect made false.
    Blocking,
}
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum Application {
    NotApplied,
    Uncertain,
    NotApplicable,
}
impl Application {
    /// Read only the classification prefix: InFlight or Reconcile is uncertain.
    pub fn from_leaf(leaf: &AccountError) -> Self {
        application(leaf.leg_recovery(), leaf.classification_attempt())
    }
}
fn application(recovery: &RecoveryClass, attempt: Option<TransmissionState>) -> Application {
    if attempt == Some(TransmissionState::InFlight)
        || matches!(recovery, RecoveryClass::Reconcile(_))
    {
        Application::Uncertain
    } else {
        Application::NotApplied
    }
}
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum TargetRole {
    Subject,
    Source,
    Destination,
}
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TargetRef {
    pub role: TargetRole,
    pub scope: ErrorScope,
}
impl TargetRef {
    pub fn new(role: TargetRole, scope: ErrorScope) -> Self {
        Self { role, scope }
    }
}
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
/// Incomplete with no refs means an unknown identity; with refs, at least these.
pub struct StepTargets {
    pub refs: Vec<TargetRef>,
    pub complete: bool,
}
impl StepTargets {
    pub fn complete(refs: Vec<TargetRef>) -> Self {
        Self {
            refs,
            complete: true,
        }
    }
    pub fn at_least(refs: Vec<TargetRef>) -> Self {
        Self {
            refs,
            complete: false,
        }
    }
    pub fn unknown() -> Self {
        Self::at_least(Vec::new())
    }
}
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryLeg {
    pub effect: Option<StepEffect>,
    pub targets: StepTargets,
    pub replay: StepReplay,
    pub application: Application,
}
impl PrimaryLeg {
    pub fn new(
        effect: StepEffect,
        targets: StepTargets,
        replay: StepReplay,
        application: Application,
    ) -> Self {
        Self {
            effect: Some(effect),
            targets,
            replay,
            application,
        }
    }
    pub fn effectless(targets: StepTargets) -> Self {
        Self {
            effect: None,
            targets,
            replay: StepReplay::Repeatable,
            application: Application::NotApplicable,
        }
    }
}
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct Step {
    id: StepId,
    effect: Option<StepEffect>,
    targets: StepTargets,
    replay: StepReplay,
    outcome: StepOutcome,
}
impl Step {
    pub fn id(&self) -> StepId {
        self.id
    }
    pub fn effect(&self) -> Option<StepEffect> {
        self.effect
    }
    pub fn targets(&self) -> &StepTargets {
        &self.targets
    }
    pub fn replay(&self) -> StepReplay {
        self.replay
    }
    pub fn outcome(&self) -> &StepOutcome {
        &self.outcome
    }
}
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum StepOutcome {
    Committed,
    Failed {
        application: Application,
        error: AccountError,
    },
}
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct Compensation {
    id: StepId,
    reverses: Vec<StepId>,
    effect: StepEffect,
    targets: StepTargets,
    outcome: CompensationOutcome,
}
impl Compensation {
    pub fn id(&self) -> StepId {
        self.id
    }
    pub fn reverses(&self) -> &[StepId] {
        &self.reverses
    }
    pub fn effect(&self) -> StepEffect {
        self.effect
    }
    pub fn targets(&self) -> &StepTargets {
        &self.targets
    }
    pub fn outcome(&self) -> &CompensationOutcome {
        &self.outcome
    }
}
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum CompensationOutcome {
    Succeeded,
    Failed(AccountError),
    Unconfirmed(AccountError),
}
impl CompensationOutcome {
    pub fn from_error(error: AccountError) -> Self {
        match error.classification_attempt() {
            None | Some(TransmissionState::InFlight) => Self::Unconfirmed(error),
            _ => Self::Failed(error),
        }
    }
}
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ReplaySafety {
    /// Safe after all prerequisites and directives are fulfilled.
    Safe,
    /// Every gate must conclusively permit replay first.
    AfterObservation,
    /// Neither observation nor performing a directive authorizes replay.
    Never,
}
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct Prerequisites {
    pub auth_refresh: bool,
    pub state_refresh: bool,
}
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PlannedDirective {
    pub directive: EngineDirective,
    pub origins: Vec<StepId>,
}
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct OperationPlan {
    pub gating: Vec<StepId>,
    pub residual: Vec<StepId>,
    pub replay: ReplaySafety,
    pub prerequisites: Prerequisites,
    pub directives: Vec<PlannedDirective>,
    pub barriers: Vec<(StepId, RecoveryClass)>,
    pub leg_actions: Vec<ReconcileAction>,
    pub may_duplicate: bool,
}
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PartialCommit {
    primary: PrimaryLeg,
    steps: Vec<Step>,
    compensations: Vec<Compensation>,
    plan: OperationPlan,
}
impl PartialCommit {
    pub fn primary(&self) -> &PrimaryLeg {
        &self.primary
    }
    pub fn steps(&self) -> &[Step] {
        &self.steps
    }
    pub fn compensations(&self) -> &[Compensation] {
        &self.compensations
    }
    pub fn plan(&self) -> &OperationPlan {
        &self.plan
    }
    pub fn step(&self, id: StepId) -> Option<&Step> {
        self.steps
            .binary_search_by_key(&id.0, |s| s.id.0)
            .ok()
            .map(|index| &self.steps[index])
    }
    pub(crate) fn builder(&self) -> PartialCommitBuilder {
        PartialCommitBuilder {
            primary: self.primary.clone(),
            steps: self.steps.clone(),
            compensations: self.compensations.clone(),
            next: 1 + self.steps.len() + self.compensations.len(),
        }
    }
}
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PartialCommitBuilder {
    primary: PrimaryLeg,
    steps: Vec<Step>,
    compensations: Vec<Compensation>,
    next: usize,
}
impl PartialCommitBuilder {
    pub fn new(primary: PrimaryLeg) -> Self {
        Self {
            primary,
            steps: Vec::new(),
            compensations: Vec::new(),
            next: 1,
        }
    }
    fn allocate(&mut self) -> StepId {
        let id = StepId(self.next);
        self.next += 1;
        id
    }
    pub fn committed(
        &mut self,
        effect: StepEffect,
        targets: StepTargets,
        replay: StepReplay,
    ) -> StepId {
        let id = self.allocate();
        self.steps.push(Step {
            id,
            effect: Some(effect),
            targets,
            replay,
            outcome: StepOutcome::Committed,
        });
        id
    }
    pub fn failed_sibling(
        &mut self,
        effect: Option<StepEffect>,
        targets: StepTargets,
        replay: StepReplay,
        application: Application,
        error: AccountError,
    ) -> StepId {
        let id = self.allocate();
        self.steps.push(Step {
            id,
            effect,
            targets,
            replay,
            outcome: StepOutcome::Failed { application, error },
        });
        id
    }
    pub fn compensation(
        &mut self,
        effect: StepEffect,
        targets: StepTargets,
        reverses: Vec<StepId>,
        outcome: CompensationOutcome,
    ) -> StepId {
        let id = self.allocate();
        self.compensations.push(Compensation {
            id,
            effect,
            targets,
            reverses,
            outcome,
        });
        id
    }
    pub(crate) fn finish(
        self,
        recovery: &RecoveryClass,
        attempt: Option<TransmissionState>,
        kind: &AccountErrorKind,
    ) -> Result<(PartialCommit, RecoveryClass), AccountErrorBuildError> {
        use AccountErrorBuildError as E;
        validate_leg(
            StepId(0),
            self.primary.effect,
            self.primary.application,
            recovery,
            attempt,
        )?;
        if self.steps.is_empty()
            && self.compensations.is_empty()
            && self.primary.application != Application::Uncertain
        {
            return Err(E::PartialCommitEmpty);
        }
        for step in &self.steps {
            if let StepOutcome::Failed { application, error } = &step.outcome {
                if error.partial_commit().is_some() {
                    return Err(E::PartialCommitNested { step: step.id });
                }
                validate_leg(
                    step.id,
                    step.effect,
                    *application,
                    error.recovery(),
                    error.classification_attempt(),
                )?;
            } else if step.effect.is_none() {
                return Err(E::PartialCommitApplication { step: step.id });
            }
        }
        let mut claimed = Vec::new();
        for c in &self.compensations {
            let invalid = || E::PartialCommitCompensation { compensation: c.id };
            if c.reverses.is_empty() {
                return Err(invalid());
            }
            for id in &c.reverses {
                let valid = if *id == StepId(0) {
                    self.primary.effect.is_some()
                        && self.primary.application == Application::Uncertain
                } else {
                    self.steps.iter().any(|s| {
                        s.id == *id
                            && s.effect.is_some()
                            && !matches!(
                                s.outcome,
                                StepOutcome::Failed {
                                    application: Application::NotApplied,
                                    ..
                                }
                            )
                    })
                };
                if !valid || claimed.contains(id) {
                    return Err(invalid());
                }
                claimed.push(*id);
            }
            if let CompensationOutcome::Failed(e) | CompensationOutcome::Unconfirmed(e) = &c.outcome
            {
                if e.partial_commit().is_some() {
                    return Err(E::PartialCommitNested { step: c.id });
                }
                if matches!(&c.outcome, CompensationOutcome::Failed(_))
                    && e.classification_attempt() == Some(TransmissionState::InFlight)
                    || matches!(&c.outcome, CompensationOutcome::Unconfirmed(_))
                        && e.classification_attempt() == Some(TransmissionState::Unsent)
                {
                    return Err(invalid());
                }
            }
        }
        let mut record = PartialCommit {
            primary: self.primary,
            steps: self.steps,
            compensations: self.compensations,
            plan: OperationPlan {
                gating: Vec::new(),
                residual: Vec::new(),
                replay: ReplaySafety::Safe,
                prerequisites: Prerequisites::default(),
                directives: Vec::new(),
                barriers: Vec::new(),
                leg_actions: Vec::new(),
                may_duplicate: false,
            },
        };
        record.derive_plan(recovery);
        let projected = record.project(recovery, kind);
        Ok((record, projected))
    }
}
fn validate_leg(
    id: StepId,
    effect: Option<StepEffect>,
    declared: Application,
    recovery: &RecoveryClass,
    attempt: Option<TransmissionState>,
) -> Result<(), AccountErrorBuildError> {
    let invalid = if effect.is_none() {
        declared != Application::NotApplicable
    } else {
        declared == Application::NotApplicable
            || declared == Application::NotApplied
                && application(recovery, attempt) == Application::Uncertain
            || declared == Application::Uncertain
                && application(recovery, attempt) == Application::NotApplied
                && attempt == Some(TransmissionState::Unsent)
    };
    if invalid {
        Err(AccountErrorBuildError::PartialCommitApplication { step: id })
    } else {
        Ok(())
    }
}
fn push_unique<T: PartialEq>(items: &mut Vec<T>, value: T) {
    if !items.contains(&value) {
        items.push(value);
    }
}
impl PartialCommit {
    /// Steps and legs a `Succeeded` compensation names.
    fn resolved(&self) -> Vec<StepId> {
        self.compensations
            .iter()
            .filter(|c| matches!(c.outcome, CompensationOutcome::Succeeded))
            .flat_map(|c| c.reverses.iter().copied())
            .collect()
    }
    /// Any unresolved committed `Blocking` step: the effect happened and
    /// observation cannot undo it.
    fn outstanding_blocking(&self, resolved: &[StepId]) -> bool {
        self.steps.iter().any(|s| {
            matches!(s.outcome, StepOutcome::Committed)
                && s.replay == StepReplay::Blocking
                && !resolved.contains(&s.id)
        })
    }
    fn derive_plan(&mut self, primary_recovery: &RecoveryClass) {
        let resolved = self.resolved();
        let blocking = self.outstanding_blocking(&resolved);
        let p = &mut self.plan;
        let mut legs = vec![(
            StepId(0),
            self.primary.effect,
            self.primary.replay,
            self.primary.application,
            primary_recovery,
        )];
        for s in &self.steps {
            if let StepOutcome::Failed { application, error } = &s.outcome {
                legs.push((s.id, s.effect, s.replay, *application, error.recovery()));
            }
        }
        for (id, effect, replay, app, r) in legs {
            match r {
                RecoveryClass::Engine(d) => {
                    if let Some(entry) = p.directives.iter_mut().find(|entry| entry.directive == *d)
                    {
                        entry.origins.push(id);
                    } else {
                        p.directives.push(PlannedDirective {
                            directive: d.clone(),
                            origins: vec![id],
                        });
                    }
                }
                RecoveryClass::Retry(a) => {
                    p.prerequisites.auth_refresh |=
                        a.disposition == RetryDisposition::AfterAuthRefresh;
                    p.prerequisites.state_refresh |=
                        a.disposition == RetryDisposition::AfterStateRefresh;
                }
                RecoveryClass::Reconcile(a) => {
                    if a.reason == ReconcileReason::InternalFailureAfterSend {
                        p.barriers.push((id, RecoveryClass::InternalFailure));
                    }
                    if effect.is_some() && !resolved.contains(&id) {
                        push_unique(&mut p.gating, id);
                        for action in &a.guidance.actions {
                            // Dedupe follows recorded effects, never the leaf's operation table.
                            if !matches!(
                                action,
                                ReconcileAction::CheckTarget | ReconcileAction::DedupeByClientId
                            ) {
                                push_unique(&mut p.leg_actions, *action);
                            }
                        }
                    }
                }
                _ => p.barriers.push((id, r.clone())),
            }
            if effect.is_some() && app == Application::Uncertain && !resolved.contains(&id) {
                if !p.gating.contains(&id) {
                    match replay {
                        StepReplay::Blocking => push_unique(&mut p.gating, id),
                        StepReplay::Repeatable => push_unique(&mut p.residual, id),
                    }
                }
                p.may_duplicate |= replay == StepReplay::Blocking
                    && matches!(effect, Some(StepEffect::Created | StepEffect::Submitted));
            }
        }
        for s in &self.steps {
            if resolved.contains(&s.id) {
                continue;
            }
            if matches!(s.outcome, StepOutcome::Committed) && s.replay == StepReplay::Blocking {
                if !s.targets.complete {
                    push_unique(&mut p.gating, s.id);
                }
                p.may_duplicate |=
                    matches!(s.effect, Some(StepEffect::Created | StepEffect::Submitted));
            }
        }
        for c in &self.compensations {
            if !matches!(c.outcome, CompensationOutcome::Unconfirmed(_)) {
                continue;
            }
            for id in &c.reverses {
                if resolved.contains(id) {
                    continue;
                }
                let replay = if *id == StepId(0) {
                    self.primary.replay
                } else {
                    self.steps
                        .iter()
                        .find(|s| s.id == *id)
                        .expect("validated compensation")
                        .replay
                };
                match replay {
                    StepReplay::Blocking => push_unique(&mut p.gating, *id),
                    // A leg already gating stays gating; it is never also residual.
                    StepReplay::Repeatable if !p.gating.contains(id) => {
                        push_unique(&mut p.residual, *id);
                    }
                    StepReplay::Repeatable => {}
                }
            }
        }
        p.gating.sort_by_key(|id| id.0);
        p.residual.sort_by_key(|id| id.0);
        p.replay = if blocking || !p.barriers.is_empty() {
            ReplaySafety::Never
        } else if !p.gating.is_empty() {
            ReplaySafety::AfterObservation
        } else {
            ReplaySafety::Safe
        };
    }
    fn project(&self, leaf: &RecoveryClass, kind: &AccountErrorKind) -> RecoveryClass {
        let p = &self.plan;
        if let Some((_, r)) = p.barriers.iter().find(|(_, r)| {
            matches!(
                r,
                RecoveryClass::AuthLost
                    | RecoveryClass::NeedsAdminConsent { .. }
                    | RecoveryClass::NeedsPolicyChange
                    | RecoveryClass::NoPermission { .. }
            )
        }) {
            return r.clone();
        }
        if let Some(d) = p.directives.first() {
            return RecoveryClass::Engine(d.directive.clone());
        }
        let (retry_hint, throttle_scope) = match leaf {
            RecoveryClass::Retry(a) => (a.retry_hint, a.throttle_scope),
            RecoveryClass::Reconcile(a) => (a.retry_hint, a.throttle_scope),
            _ => (None, None),
        };
        let blocking = self.outstanding_blocking(&self.resolved());
        if !p.gating.is_empty() || p.barriers.is_empty() && p.replay == ReplaySafety::Never {
            let reason = if blocking {
                ReconcileReason::PartialCommit
            } else {
                p.gating
                    .iter()
                    .find_map(|id| {
                        let r = if *id == StepId(0) {
                            Some(leaf)
                        } else {
                            self.step(*id).and_then(|s| match &s.outcome {
                                StepOutcome::Failed { error, .. } => Some(error.recovery()),
                                _ => None,
                            })
                        };
                        match r {
                            Some(RecoveryClass::Reconcile(a)) => Some(a.reason),
                            _ => None,
                        }
                    })
                    .unwrap_or(ReconcileReason::PartialCompletionSignal)
            };
            let mut actions = vec![ReconcileAction::CheckTarget];
            for a in &p.leg_actions {
                push_unique(&mut actions, *a);
            }
            if p.may_duplicate {
                push_unique(&mut actions, ReconcileAction::DedupeByClientId);
            }
            if p.replay == ReplaySafety::Never {
                push_unique(&mut actions, ReconcileAction::ObserveOnly);
            }
            return RecoveryClass::Reconcile(ReconcileAdvice {
                reason,
                guidance: ReconcileGuidance { actions },
                retry_hint,
                throttle_scope,
            });
        }
        if let Some((_, r)) = p.barriers.first() {
            return r.clone();
        }
        let reason = match leaf {
            RecoveryClass::Retry(a) => a.reason,
            _ => match kind {
                AccountErrorKind::Server(ServerErrorKind::RateLimited) => RetryReason::RateLimited,
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted) => {
                    RetryReason::QuotaExhausted
                }
                AccountErrorKind::Server(
                    ServerErrorKind::Unavailable | ServerErrorKind::Error { .. },
                ) => RetryReason::ServerUnavailable,
                _ => RetryReason::Transport,
            },
        };
        RecoveryClass::Retry(RetryAdvice::new(
            if p.prerequisites.state_refresh {
                RetryDisposition::AfterStateRefresh
            } else if p.prerequisites.auth_refresh {
                RetryDisposition::AfterAuthRefresh
            } else {
                RetryDisposition::SameRequest
            },
            retry_hint,
            reason,
            throttle_scope,
        ))
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Serialize)]
pub struct PartialCommitTelemetry {
    pub committed: Vec<(StepEffect, u32)>,
    pub primary_application: Application,
    pub sibling_count: u32,
    pub compensations: Vec<(&'static str, u32)>,
    pub replay: ReplaySafety,
    pub leg_recovery: &'static str,
}
#[non_exhaustive]
#[derive(Clone, Debug, Serialize)]
pub struct PartialCommitExport<'a> {
    pub primary: StepExport<'a>,
    pub steps: Vec<StepExport<'a>>,
    pub compensations: Vec<CompensationExport<'a>>,
}
#[non_exhaustive]
#[derive(Clone, Debug, Serialize)]
pub struct StepExport<'a> {
    pub id: StepId,
    pub effect: Option<StepEffect>,
    pub targets: &'a StepTargets,
    pub replay: StepReplay,
    /// `None` for a committed step: it applied, so no failed-leg application
    /// exists. `NotApplicable` is reserved for effectless failed legs.
    pub application: Option<Application>,
    pub outcome: &'static str,
}
#[non_exhaustive]
#[derive(Clone, Debug, Serialize)]
pub struct CompensationExport<'a> {
    pub id: StepId,
    pub effect: StepEffect,
    pub targets: &'a StepTargets,
    pub reverses: &'a [StepId],
    pub outcome: &'static str,
}
fn count<T: PartialEq>(counts: &mut Vec<(T, u32)>, value: T) {
    if let Some((_, n)) = counts.iter_mut().find(|(key, _)| *key == value) {
        *n = n.saturating_add(1);
    } else {
        counts.push((value, 1));
    }
}
fn compensation_label(outcome: &CompensationOutcome) -> &'static str {
    match outcome {
        CompensationOutcome::Succeeded => "succeeded",
        CompensationOutcome::Failed(_) => "failed",
        CompensationOutcome::Unconfirmed(_) => "unconfirmed",
    }
}
impl PartialCommit {
    pub(crate) fn telemetry(&self, recovery: &RecoveryClass) -> PartialCommitTelemetry {
        let mut committed = Vec::new();
        let mut sibling_count = 0u32;
        for s in &self.steps {
            match s.outcome {
                StepOutcome::Committed => {
                    if let Some(effect) = s.effect {
                        count(&mut committed, effect);
                    }
                }
                StepOutcome::Failed { .. } => sibling_count = sibling_count.saturating_add(1),
            }
        }
        let mut compensations = Vec::new();
        for c in &self.compensations {
            count(&mut compensations, compensation_label(&c.outcome));
        }
        PartialCommitTelemetry {
            committed,
            primary_application: self.primary.application,
            sibling_count,
            compensations,
            replay: self.plan.replay,
            leg_recovery: super::account_error::recovery_discriminant(recovery),
        }
    }
    pub(crate) fn export(&self) -> PartialCommitExport<'_> {
        PartialCommitExport {
            primary: StepExport {
                id: StepId(0),
                effect: self.primary.effect,
                targets: &self.primary.targets,
                replay: self.primary.replay,
                application: Some(self.primary.application),
                outcome: "failed",
            },
            steps: self
                .steps
                .iter()
                .map(|s| StepExport {
                    id: s.id,
                    effect: s.effect,
                    targets: &s.targets,
                    replay: s.replay,
                    application: match s.outcome {
                        StepOutcome::Committed => None,
                        StepOutcome::Failed { application, .. } => Some(application),
                    },
                    outcome: match s.outcome {
                        StepOutcome::Committed => "committed",
                        StepOutcome::Failed { .. } => "failed",
                    },
                })
                .collect(),
            compensations: self
                .compensations
                .iter()
                .map(|c| CompensationExport {
                    id: c.id,
                    effect: c.effect,
                    targets: &c.targets,
                    reverses: &c.reverses,
                    outcome: compensation_label(&c.outcome),
                })
                .collect(),
        }
    }
    pub(crate) fn chains(&self) -> Vec<(StepId, Vec<super::cause::CauseSummary<'_>>)> {
        self.steps
            .iter()
            .filter_map(|s| match &s.outcome {
                StepOutcome::Failed { error, .. } => Some((s.id, error)),
                _ => None,
            })
            .chain(self.compensations.iter().filter_map(|c| match &c.outcome {
                CompensationOutcome::Failed(e) | CompensationOutcome::Unconfirmed(e) => {
                    Some((c.id, e))
                }
                _ => None,
            }))
            .map(|(id, e)| (id, e.chain().iter().map(super::Cause::summary).collect()))
            .collect()
    }
}

#[cfg(test)]
#[path = "partial_tests.rs"]
mod tests;
