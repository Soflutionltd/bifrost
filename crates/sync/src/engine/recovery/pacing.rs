//! Admission, throttle allowance and reconnect budgets for recovery.

use super::{RecoveryContext, broadcast_warning};
use crate::engine::*;

/// Reopen-failure budget. The engine attempts three reopens; after the
/// third consecutive failure it pauses with
/// `PauseReason::RetryBudgetExhausted`. Terminal and already
/// operator-attention errors remain intact; retry, reconcile, and other
/// engine-action errors are converted to operator attention because the
/// automatic reopen budget is spent.
pub(in crate::engine) struct AttemptBudget(pub(in crate::engine) u32);
pub(in crate::engine) const REOPEN_RETRY_BUDGET: AttemptBudget = AttemptBudget(3);

/// Wait out a recovery backoff, unless the account is being torn down.
///
/// Returns `false` when the shutdown token tripped, which every caller reads as
/// "stop, do not attempt again". An un-selected sleep here is what guaranteed
/// that a detach landing during recovery burned toward `detach_timeout` and hit
/// the abort path instead of draining the worker cleanly.
pub(in crate::engine) async fn sleep_unless_shutdown(
    delay: Duration,
    shutdown: &CancellationToken,
) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => false,
        () = tokio::time::sleep(delay) => true,
    }
}

pub(in crate::engine) const REOPEN_BACKOFF: bifrost_types::BackoffPolicy =
    bifrost_types::BackoffPolicy::new(Duration::from_secs(1), Duration::from_secs(300), true);
struct CumulativeWaitBudget(Duration);
const REPAIR_WAIT_BUDGET: CumulativeWaitBudget = CumulativeWaitBudget(Duration::from_secs(300));

/// Pacing belongs to the entire repair, including discarded replacements.
#[derive(Default)]
pub(in crate::engine) struct RepairPacing {
    pub(in crate::engine) local_deadline: Option<tokio::time::Instant>,
    // A failed replacement is closed before its retry; preserve its declaration
    // until the next open, when there is no replacement left to ask.
    pub(in crate::engine) failed_keys: Vec<bifrost_types::ThrottleKey>,
    pub(in crate::engine) waited: Duration,
    // Orthogonal evidence: cleanup holds do not mean the returned failure was
    // observed. This resets per attempt, independently of the cumulative budget.
    pub(in crate::engine) observed: bool,
}

impl RepairPacing {
    pub(in crate::engine) fn exhaust_wait_budget(&mut self) -> Error {
        self.waited = REPAIR_WAIT_BUDGET.0;
        Error::WaitBudgetExhausted
    }

    pub(in crate::engine) fn observe(&mut self, ctx: &RecoveryContext<'_>, error: &AccountError) {
        self.observed = true;
        self.record_holds(ctx, error);
    }

    // Cleanup evidence must not suppress recording the original returned error.
    pub(in crate::engine) fn record_holds(
        &mut self,
        ctx: &RecoveryContext<'_>,
        error: &AccountError,
    ) {
        let deadline =
            crate::throttle::record_repair_throttle(&ctx.throttles, &ctx.account_id, error);
        self.local_deadline = self.local_deadline.max(deadline);
    }

    pub(in crate::engine) fn pending_wait(
        &self,
        ctx: &RecoveryContext<'_>,
        account: Option<&dyn Account>,
        scope: Option<&CursorScope>,
    ) -> Duration {
        let mut keys = if account.is_none() {
            self.failed_keys.clone()
        } else {
            Vec::new()
        };
        if let (Some(account), Some(scope)) = (account, scope) {
            keys.extend(crate::throttle::declared_keys(
                account,
                &ctx.account_id,
                bifrost_types::ThrottleWork::Scope(scope),
            ));
        }
        let local = self
            .local_deadline
            .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
            .unwrap_or_default();
        let shared = crate::throttle::throttle_wait(&ctx.throttles, &ctx.account_id, &keys)
            .unwrap_or_default();
        local.max(shared)
    }

    pub(in crate::engine) async fn admit(
        &mut self,
        ctx: &RecoveryContext<'_>,
        control: &SyncControl,
        account: Option<&dyn Account>,
        scope: Option<&CursorScope>,
    ) -> Result<crate::engine::drive_start::RecoveryAdmission, Error> {
        let gate = crate::engine::drive_start::DriveStart {
            current: &ctx.current,
            control,
            scheduler: &ctx.scheduler,
            throttles: &ctx.throttles,
            account_id: &ctx.account_id,
            shutdown: &ctx.shutdown,
        };
        loop {
            self.wait(ctx, account, scope).await?;
            let result = gate
                .admit_recovery(crate::scheduler::WorkKind::Sync, account, |account| {
                    if let Some(scope) = scope {
                        crate::throttle::declared_keys(
                            account,
                            &ctx.account_id,
                            bifrost_types::ThrottleWork::Scope(scope),
                        )
                    } else {
                        self.failed_keys.clone()
                    }
                })
                .await;
            match result {
                Ok(admitted) => return Ok(admitted),
                Err(crate::engine::drive_start::StartRefused::Paused) => return Err(Error::Paused),
                Err(crate::engine::drive_start::StartRefused::ShuttingDown) => {
                    return Err(Error::ShuttingDown);
                }
                // No scheduler permit or newly registered activity survives
                // this return. The next wait charges the recovery allowance.
                Err(crate::engine::drive_start::StartRefused::Held { .. }) => {}
            }
        }
    }

    // Scope repair already deleted its cursor, or replacement attach owns
    // provisional writer rows, under an outer admitted activity and reopen
    // lock. Those transaction owners deliberately span pacing: releasing them
    // would let pause report quiescence halfway through recovery, or let a
    // second replacement consume provisional state. Only the per-attempt
    // scheduler permit and inner activity come from DriveStart, and neither
    // spans a hold. Never reload current in place of the pinned account here.
    pub(in crate::engine) async fn wait(
        &mut self,
        ctx: &RecoveryContext<'_>,
        account: Option<&dyn Account>,
        scope: Option<&CursorScope>,
    ) -> Result<(), Error> {
        loop {
            if ctx.shutdown.is_cancelled() {
                return Err(Error::ShuttingDown);
            }
            let now = tokio::time::Instant::now();
            let wait = self.pending_wait(ctx, account, scope);
            if wait.is_zero() {
                return Ok(());
            }
            let remaining = REPAIR_WAIT_BUDGET.0.saturating_sub(self.waited);
            tokio::select! {
                biased;
                () = ctx.shutdown.cancelled() => return Err(Error::ShuttingDown),
                // Prefer expiry even if late polling made both timers ready.
                // A hold ending exactly at the allowance is still admissible.
                () = tokio::time::sleep(remaining), if wait > remaining => {
                    return Err(self.exhaust_wait_budget());
                }
                () = tokio::time::sleep(wait) => {
                    if now.elapsed() > remaining {
                        return Err(self.exhaust_wait_budget());
                    }
                    self.waited = self.waited.saturating_add(now.elapsed());
                }
            }
        }
    }
}

pub(in crate::engine) fn warn_wait_budget(ctx: &RecoveryContext<'_>, scope: CursorScope) {
    broadcast_warning(
        &ctx.delivery,
        Some(scope),
        bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::Engine(
                bifrost_types::EngineNotice::RecoveryWaitBudgetExhausted,
            ),
            "recovery wait budget exhausted",
        ),
    );
}
