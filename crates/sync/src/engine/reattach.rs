//! Recovery dispatch: reattach, restart, re-establish, and the
//! engine-directive arms.

use super::*;

/// Shared context bundle used by every recovery-dispatch path. Folds
/// the per-account state most paths need into a single argument so
/// `handle_account_error`, the engine-directive arm, and the reopen
/// loop don't carry seven-positional argument lists.
pub(crate) struct RecoveryContext<'a> {
    pub bandwidth_budget: Option<&'a bifrost_net::BandwidthBudget>,
    pub factory: &'a Arc<dyn AccountFactory>,
    pub current: &'a Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: &'a Arc<CursorRegistry>,
    pub changes_tx: &'a broadcast::Sender<MultiplexerEvent>,
    pub account_id: &'a AccountId,
    pub control: &'a SyncControl,
    pub account_control_tx: &'a broadcast::Sender<AccountControl>,
    pub throttles: &'a Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>,
    pub boundary_tx: &'a watch::Sender<crate::cancel::BoundaryRequest>,
    pub capabilities: &'a Arc<std::sync::RwLock<AccountCapabilities>>,
    pub subscriptions: &'a Arc<SubscriptionRegistry>,
    pub account_generation_tx: &'a watch::Sender<u64>,
    pub reopen_lock: &'a Arc<AsyncMutex<()>>,
    /// Slot-lifetime open-skip lane; a successful reattach replaces it
    /// with the replacement open's `OpenedAccount::skipped_scopes`.
    pub open_skips: &'a Arc<std::sync::Mutex<Vec<SkippedScope>>>,
    /// Cancels a queued recovery during detach.
    pub shutdown: &'a CancellationToken,
    /// The account's single durable writer.
    ///
    /// Reattach persists and rolls back replacement cursors THROUGH this
    /// channel rather than touching `store` directly. Both are ordered against
    /// consumer acknowledgements that way, which matters because a
    /// replacement's inventory pass broadcasts checkpoint-bearing batches
    /// before the cutover: a direct write racing the ack writer let an aborted
    /// reattach delete a cursor a consumer had already acknowledged.
    pub writer: &'a WriterHandle,
    /// Where inventory walks record what they proved, read back by the writer.
    pub coverage: &'a Arc<PendingCoverage>,
    /// The account's change delivery gate. Re-establishment runs inventory
    /// fusion, whose checkpoint-bearing pages have to know whether a NUMBERED
    /// receiver took delivery; the raw `changes_tx` cannot say.
    pub delivery: &'a Arc<ChangeDelivery>,
}

/// Dispatch an `AccountError` to the engine's recovery machinery.
///
/// Routes through `plan_recovery` so the closed `RecoveryPlan` enum
/// drives the dispatch. The `scope` argument is the worker's
/// convenience-suggested scope: scope-bound directives use the
/// directive's own scope when present; account-wide directives ignore
/// it.
///
/// Returns the scope of a repair that was declined because the account is
/// paused AND that nothing will raise again: a scope with no cursor registered
/// (a lifecycle `Created` scope, which has no failing cursor for the poll to
/// re-raise the directive from). The caller owes that scope a re-offer after
/// resume, see [`DeferredScopeRepairs`]. Every other outcome is `None`.
pub(crate) async fn handle_account_error(
    ctx: &RecoveryContext<'_>,
    scope: Option<CursorScope>,
    error: AccountError,
) -> Option<CursorScope> {
    use crate::recovery::{RecoveryPlan, plan_recovery};
    // Clone so we can log structured fields after dispatch consumes
    // the value.
    let plan = plan_recovery(error.clone());
    let mut deferred = None;
    match plan {
        RecoveryPlan::Retry(advice) => {
            // The reopen listener is a serialization point for real
            // engine directives. Record any shared throttle deadline,
            // but never sleep here: no failed operation is retried
            // after such a sleep, and blocking this task would hold
            // RestartScope / RestartAccount requests behind an inert
            // provider Retry-After delay.
            apply_throttle(ctx, &advice, &error);
            tracing::debug!(
                target: "bifrost.sync.recovery",
                account = ?ctx.account_id,
                scope = ?scope,
                retry = ?advice,
                "retry recovery reached reopen listener; caller owns the retry"
            );
        }
        RecoveryPlan::Reconcile(advice) => {
            crate::recovery::record_reconcile_throttle(
                ctx.throttles,
                ctx.account_id,
                &advice,
                &error,
            );
            // The poll loop / push reconciler perform the actual probe;
            // if we reach this branch via the reopen listener their
            // next pass owns the reconcile. Surface the actions for
            // telemetry so dropped guidance is visible.
            log_reconcile_advice(ctx, scope.as_ref(), &error, &advice);
        }
        RecoveryPlan::Engine(directive) => {
            // Every directive except `RestartAccount` is a bounded,
            // scope-local repair that must not interleave with a
            // connection swap, so it takes the serialization lock here.
            // RestartScope and SchemaIncompatible deliberately retain the
            // guard across their retry backoff. Releasing it during a sleep
            // would let a reopen replace the account and registry topology
            // halfway through the delete-then-establish transaction, or let
            // a second recovery establish the same scope concurrently. The
            // queueing cost is bounded by the three-attempt budget and is the
            // price of keeping recovery transitions serial.
            //
            // `RestartAccount` must NOT take it here. It waits for the
            // boundary to read `Run` before each attempt, and that wait is
            // unbounded: a consumer can pause and hold the account idle
            // indefinitely. Holding the lock across it made
            // `unsubscribe_push` - which needs the same lock - hang for
            // the entire pause, so push teardown was unavailable exactly
            // when a consumer was trying to detach cleanly. The lock it
            // does need is taken inside `open_replacement`, spanning only
            // the open and the swap. Reintroducing an acquisition here
            // deadlocks against that one rather than silently regressing.
            //
            // `DowngradeStrategy` restarts the account too, so the same
            // holds: taking the lock here made its `restart_account` wait
            // forever on `open_replacement`'s acquisition of this very
            // (non-reentrant) mutex, hanging recovery on the first strategy
            // downgrade. Its trailing scope restart takes the lock itself.
            let _reopen_guard = match directive {
                EngineDirective::RestartAccount | EngineDirective::DowngradeStrategy(_) => None,
                _ => Some(ctx.reopen_lock.lock().await),
            };
            deferred = handle_engine_directive(ctx, scope, directive, error).await;
        }
        RecoveryPlan::Terminal(fatal) => {
            // Broadcast already carried the terminating event. Emit a
            // `TelemetryView` structured log so dashboards pivot on
            // stable fields rather than `?debug` text.
            let err = fatal.as_ref();
            let view = err.telemetry_fields();
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?ctx.account_id,
                scope = ?scope,
                kind = view.kind_discriminant,
                message_key = view.message_key,
                recovery = view.recovery_discriminant,
                provider = ?view.provider,
                protocol = ?view.protocol,
                status = ?view.status,
                operation = ?view.operation,
                "terminal recovery; engine takes no automated action"
            );
        }
    }
    deferred
}

/// Record any provider-documented throttle scope on the engine's
/// shared `ThrottleBucket`, resolving the key from the identities the
/// classified error carries (`ErrorScope::Mailbox`, provider).
/// `CurrentOperation` is a per-call hint and never enters the bucket
/// (the originating caller owns any inline delay).
fn apply_throttle(ctx: &RecoveryContext<'_>, advice: &RetryAdvice, error: &AccountError) {
    crate::recovery::record_throttle(ctx.throttles, ctx.account_id, advice, error);
}

fn log_reconcile_advice(
    ctx: &RecoveryContext<'_>,
    scope: Option<&CursorScope>,
    error: &AccountError,
    advice: &ReconcileAdvice,
) {
    let actions: Vec<&'static str> = advice
        .guidance
        .actions
        .iter()
        .map(|a| match a {
            ReconcileAction::CheckTarget => "check-target",
            ReconcileAction::DedupeByClientId => "dedupe-by-client-id",
            // `ReconcileAction` is `#[non_exhaustive]`; new variants
            // are surfaced as a stable tag so dashboards don't lose
            // signal silently.
            _ => "unknown",
        })
        .collect();
    tracing::warn!(
        target: "bifrost.sync.changes",
        account = ?ctx.account_id,
        scope = ?scope,
        kind = ?error.kind(),
        message_key = error.message_key(),
        reason = ?advice.reason,
        actions = ?actions,
        "reconcile recovery reached reopen listener"
    );
}

async fn handle_engine_directive(
    ctx: &RecoveryContext<'_>,
    fallback_scope: Option<CursorScope>,
    directive: EngineDirective,
    error: AccountError,
) -> Option<CursorScope> {
    // The scope of a declined repair that nothing will raise again; see
    // `handle_account_error`.
    let mut deferred = None;
    // Every current directive has an explicit dispatch arm. The required
    // non-exhaustive fallback makes a future variant visible in logs until a
    // human gives it an intentional engine action.
    match directive {
        EngineDirective::RestartScope(directive_scope) => {
            // Pass the directive's own scope to broadcast_warning so
            // the multiplexer event's `scope` matches the affected
            // scope - not the worker-suggested fallback. (sync-N4)
            //
            // A declined repair of a scope with no registered cursor - the
            // synthesized directive a lifecycle `Created` raises, before any
            // cursor exists - has no failing cursor to re-raise it after
            // resume, so it is handed back for the listener to re-offer. A
            // scope WITH a cursor needs nothing: the poll raises it again.
            if !restart_scope(ctx, directive_scope.clone()).await
                && ctx.cursors.snapshot(&directive_scope).is_none()
            {
                deferred = Some(directive_scope);
            }
        }
        EngineDirective::DowngradeCapabilityForScope(directive_scope) => {
            // Registered before the warning, so a deferred repair does not
            // announce a downgrade that will be announced again when the poll
            // re-raises the directive after resume.
            let activity = begin_scope_repair(ctx, &directive_scope)?;
            broadcast_warning(
                ctx.changes_tx,
                Some(directive_scope.clone()),
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::Other,
                    format!("scope capability downgraded: {directive_scope:?}"),
                )
                .with_protocol_detail(DiagnosticText::support_only(format!("{directive_scope:?}"))),
            );
            restart_scope_admitted(ctx, directive_scope, &activity).await;
        }
        EngineDirective::RestartAccount => {
            restart_account(ctx).await;
        }
        EngineDirective::DowngradeStrategy(downgrade) => {
            // Route the warning to the scope the downgrade is actually
            // about: the originating error's cursor scope when it names
            // one (the same signal the immediate re-establish below
            // keys on), else the worker's suggestion, else account.
            let warning_scope = match error.scope() {
                Some(ErrorScope::Cursor(scoped)) => Some(scoped.clone()),
                _ => fallback_scope.clone(),
            };
            broadcast_warning(
                ctx.changes_tx,
                warning_scope,
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::StrategyDowngraded,
                    format!("downgraded sync strategy: {downgrade:?}"),
                )
                .with_protocol_detail(DiagnosticText::support_only(format!("{downgrade:?}"))),
            );
            // Only an account that actually restarted gets the scope repair.
            // `restart_scope` deletes the scope's cursor before it
            // re-establishes; after an exhausted budget the account is
            // paused, the establishment is refused, and the deleted scope
            // would stay gone after resume.
            if !restart_account(ctx).await {
                return None;
            }
            // If the originating error was scoped to a cursor, also
            // re-establish that scope so the downgrade takes effect
            // immediately rather than at the next poll.
            // The dispatch took no lock for this directive (see there); the
            // scope restart needs the same serialization every other scope
            // repair runs under, so it takes it now that the account restart
            // has released it. A pause landing between the account restart and
            // this repair makes `restart_scope` decline without touching the
            // scope: the account is already downgraded, and the poll raises the
            // scope's directive again after resume.
            if let Some(ErrorScope::Cursor(scoped)) = error.scope() {
                let _reopen_guard = ctx.reopen_lock.lock().await;
                restart_scope(ctx, scoped.clone()).await;
            }
        }
        EngineDirective::SchemaIncompatible => {
            handle_schema_incompatible(ctx).await;
        }
        EngineDirective::OperatorOverrideRequired { reason } => {
            // Auto-pause the account: the engine no longer drives work
            // for it until the consumer flips `AccountControl::Resume`.
            // The reason is bounded (`PauseReason::OperatorOverrideRequired`);
            // free-form reason text rides through the warning only.
            // (sync-D9)
            // Deliberately account-scoped, not the worker's fallback:
            // the directive pauses the WHOLE account, and a consumer
            // routing the warning to one folder's scope would misfile
            // an account-wide condition.
            broadcast_warning(
                ctx.changes_tx,
                None,
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::OperatorAttentionNeeded,
                    reason.clone(),
                )
                .with_protocol_detail(DiagnosticText::support_only(reason)),
            );
            engine_pause(ctx, PauseReason::OperatorOverrideRequired);
        }
        EngineDirective::DisableScope(directive_scope) => {
            disable_scope(ctx, directive_scope).await;
        }
        // EngineDirective is #[non_exhaustive] from bifrost-types; new
        // variants land here unhandled and require explicit dispatch
        // before they ship. The fallback warns rather than silently
        // routing.
        other => {
            tracing::warn!(
                ?other,
                "unhandled engine directive; defaulting to no-op until dispatch is wired"
            );
        }
    }
    deferred
}

/// Scope repairs declined under a pause that nothing else will raise again,
/// re-offered to the reopen listener once the account runs.
///
/// A repair of a scope that HAS a cursor needs no help: it declines without
/// touching the cursor, and the poll raises the directive again after resume. A
/// scope that has none - the directive a lifecycle `Created` synthesizes, before
/// any cursor exists - is invisible to the multiplexer, which only polls
/// registered scopes, so a declined repair of it was simply lost.
///
/// One instance per slot, owned by the reopen listener. Each parked scope is one
/// small task that waits for the boundary to read `Run` and then sends the same
/// synthesized directive back through the listener's own channel, so the repair
/// runs through the ordinary dispatch (reopen lock, activity registration, the
/// decline-under-pause check again). It is a task rather than a wait in the
/// listener because the listener is the serialization point for real directives:
/// blocking it across an unbounded pause would hold `RestartAccount` behind it.
/// A scope is parked at most once, so repeated `Created` events during a long
/// pause do not stack tasks; the task leaves through the shutdown token, so a
/// detach never waits on one.
pub(crate) struct DeferredScopeRepairs {
    parked: Arc<std::sync::Mutex<HashSet<CursorScope>>>,
}

impl DeferredScopeRepairs {
    pub(crate) fn new() -> Self {
        Self {
            parked: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

    /// Park `scope` until the account runs, then re-raise its restart directive
    /// on `reopen_tx`. A no-op if it is already parked.
    pub(crate) fn park(
        &self,
        scope: CursorScope,
        control: SyncControl,
        cursors: Arc<CursorRegistry>,
        shutdown: CancellationToken,
        reopen_tx: mpsc::Sender<ReopenRequest>,
    ) {
        let inserted = self
            .parked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(scope.clone());
        if !inserted {
            return;
        }
        let parked = Arc::clone(&self.parked);
        tokio::spawn(async move {
            let runs = control.wait_until_running(&shutdown).await;
            // Unparked BEFORE the send, so a repair the listener declines again
            // (a second pause) can park the scope afresh. The removal is also
            // the check: a scope `unpark` already removed - its folder was
            // deleted during the pause - is not re-raised, or it would be
            // established after resume for a folder that no longer exists and
            // end in the retry-budget terminal event. Deleted and then created
            // again during one pause, it is parked afresh and exactly one of
            // the two tasks finds it.
            let still_parked = parked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&scope);
            // Something else established it while it waited (a second `Created`,
            // a reattach): restarting a live cursor would reset healthy state.
            if !runs || !still_parked || cursors.snapshot(&scope).is_some() {
                return;
            }
            let error = crate::recovery::restart_scope_error(
                scope.clone(),
                bifrost_types::AccountOperation::SyncChanges,
            );
            tokio::select! {
                () = shutdown.cancelled() => {}
                _ = reopen_tx.send(ReopenRequest::Recovery {
                    scope: Some(scope),
                    error,
                }) => {}
            }
        });
    }

    /// Forget a parked scope whose folder was deleted, so its task re-raises
    /// nothing when the account runs again. A no-op if it is not parked.
    pub(crate) fn unpark(&self, scope: &CursorScope) {
        self.parked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(scope);
    }
}

async fn handle_schema_incompatible(ctx: &RecoveryContext<'_>) {
    // Stop trusting durable cursor envelopes. Clear every in-memory
    // cursor, delete every durable change cursor we know about, AND
    // delete every backfill checkpoint - the completion marker
    // included. The ids the consumer holds were minted under an
    // encoding the protocol has disowned (that is what a schema bump
    // asserts), and re-minting them requires the inventory re-walk
    // the completion marker would otherwise skip at the next attach.
    // For protocols whose establishment returns a `Ready` cursor from
    // live state (JMAP), that next-attach re-walk IS the migration;
    // this session's re-established cursor only covers changes from
    // now on. Then re-establish each scope from the current account
    // handle. Failure to re-establish a single scope escalates
    // per-scope (sync-D7): the account keeps running for the scopes
    // that succeed.
    //
    // The recovery deletes EVERY scope's cursor before it re-establishes any, so
    // a pause landing inside it would strand all of them for the reason a scope
    // repair must not (see `begin_scope_repair`): the establishment is
    // refused on a paused account and nothing raises the directive again. It
    // therefore registers activity before touching anything, declines outright
    // on a paused account (the failing cursors stay registered and re-raise it
    // after resume), and holds the registration through the last
    // re-establishment, which runs on a control that registration admits.
    let Some(activity) = ctx.control.begin_activity() else {
        tracing::debug!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            "schema recovery deferred: the account is paused; every cursor stays registered \
             so the poll raises the directive again after resume"
        );
        return;
    };
    let admitted = ctx.control.admitted_by(&activity);
    let scopes: Vec<CursorScope> = ctx.cursors.all_scopes();
    for s in &scopes {
        ctx.cursors.delete(s);
        if let Err(err) = ctx.writer.reset_scope_for_schema_recovery(s.clone()).await {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?ctx.account_id,
                scope = ?s,
                error = %err,
                "SchemaIncompatible: durable scope reset failed"
            );
        }
    }
    for s in scopes {
        re_establish_scope_with_backoff(ctx, s, &admitted).await;
    }
}

/// Reopen-failure budget. The engine attempts three reopens; after the
/// third consecutive failure the account is paused with
/// `PauseReason::RetryBudgetExhausted` and the last error is emitted
/// verbatim as `SyncEvent::Terminated`. (sync-D6)
const REOPEN_RETRY_BUDGET: u32 = 3;

/// Wait out a recovery backoff, unless the account is being torn down.
///
/// Returns `false` when the shutdown token tripped, which every caller reads as
/// "stop, do not attempt again". An un-selected sleep here is what guaranteed
/// that a detach landing during recovery burned toward `detach_timeout` and hit
/// the abort path instead of draining the worker cleanly.
pub(super) async fn sleep_unless_shutdown(delay: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => false,
        () = tokio::time::sleep(delay) => true,
    }
}

const REOPEN_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const REOPEN_BACKOFF_CAP: Duration = Duration::from_secs(5 * 60);

/// Restart a single scope with exponential backoff and a retry budget.
/// After three consecutive re-establishment failures the scope is
/// abandoned, a `SyncEvent::Terminated(last_error)` is broadcast for
/// that scope, and the operator is alerted via
/// `Warning::OperatorAttentionNeeded`. The other scopes keep running.
/// (sync-D6, sync-D7)
///
/// Returns whether the repair started. `false` means the account was paused or
/// pausing, nothing was touched, and the scope's failing cursor is still in the
/// registry for the poll to raise the directive against again after resume.
async fn restart_scope(ctx: &RecoveryContext<'_>, scope: CursorScope) -> bool {
    let Some(activity) = begin_scope_repair(ctx, &scope) else {
        return false;
    };
    restart_scope_admitted(ctx, scope, &activity).await;
    true
}

/// Register the activity a scope repair runs under, or decline the repair.
///
/// A repair deletes the scope's cursor and then re-establishes it, and the
/// establishment is refused on a paused account. A pause landing between the
/// two used to strand the scope for good: the cursor was gone, the
/// establishment returned `Paused`, and the multiplexer only polls scopes still
/// in the cursor registry, so nothing ever raised the directive again.
///
/// The repair must not wait for `Run` instead. The dispatch holds the reopen
/// lock, and a consumer can hold the account paused indefinitely; a wait here
/// would hang `unsubscribe_push`, which needs that lock, for the whole pause
/// (the same reason `RestartAccount` takes no lock in the dispatch). So a
/// refused registration means: do nothing at all, and let the still-registered
/// failing cursor re-raise the directive after resume.
///
/// Once this returns a guard the pause waits for the repair rather than landing
/// inside it. The guard has to be held from before the delete, and the
/// establishment underneath runs on [`SyncControl::admitted_by`] this guard, not
/// on a second registration a mid-repair pause would refuse.
fn begin_scope_repair(ctx: &RecoveryContext<'_>, scope: &CursorScope) -> Option<SyncActivityGuard> {
    let activity = ctx.control.begin_activity();
    if activity.is_none() {
        tracing::debug!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            scope = ?scope,
            "scope repair deferred: the account is paused; the failing cursor stays registered \
             so the poll raises the directive again after resume"
        );
    }
    activity
}

async fn restart_scope_admitted(
    ctx: &RecoveryContext<'_>,
    scope: CursorScope,
    activity: &SyncActivityGuard,
) {
    ctx.cursors.delete(&scope);
    if let Err(err) = ctx.writer.reset_scope_for_restart(scope.clone()).await {
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            scope = ?scope,
            error = %err,
            "RestartScope: durable scope reset failed"
        );
    }
    let admitted = ctx.control.admitted_by(activity);
    re_establish_scope_with_backoff(ctx, scope, &admitted).await;
}

/// Quarantine a single scope: delete its in-memory and durable cursor and
/// drop it from the membership index (`CursorRegistry::delete` does both),
/// then broadcast a scoped operator warning. The per-scope poll loop
/// self-terminates on its next iteration when `cursors.snapshot(scope)`
/// returns `None` (the self-drain check in the multiplexer). Unlike
/// `restart_scope`, there is NO re-establishment, NO pause, and NO
/// account-wide escalation: a revoked shared folder must stay gone until
/// the next full account reopen re-runs discovery (where MYRIGHTS filters
/// it out if still revoked, or it re-appears if access was restored).
/// Siblings are untouched.
async fn disable_scope(ctx: &RecoveryContext<'_>, scope: CursorScope) {
    ctx.cursors.delete(&scope);
    if let Err(err) = ctx.writer.reset_scope_for_disable(scope.clone()).await {
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            scope = ?scope,
            error = %err,
            "DisableScope: durable scope reset failed"
        );
    }
    broadcast_warning(
        ctx.changes_tx,
        Some(scope.clone()),
        bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::OperatorAttentionNeeded,
            format!("shared folder access revoked; scope disabled: {scope:?}"),
        )
        .with_protocol_detail(DiagnosticText::support_only(format!("{scope:?}"))),
    );
}

/// `control` is what the establishment registers its own activity on: the
/// account's plain control for a caller holding no registration, or the handle
/// [`SyncControl::admitted_by`] a registration the caller already holds, so a
/// pause that lands mid-repair cannot refuse the nested one. It is also what
/// the inventory walk under the establishment publishes its checkpoints
/// through, which is why it is passed whole rather than dropped.
async fn re_establish_scope_with_backoff(
    ctx: &RecoveryContext<'_>,
    scope: CursorScope,
    control: &SyncControl,
) {
    let mut delay = REOPEN_BACKOFF_INITIAL;
    let mut last_account_error: Option<AccountError> = None;
    for attempt in 0..REOPEN_RETRY_BUDGET {
        if attempt > 0 {
            let sleep_for = jittered(delay);
            if !sleep_unless_shutdown(sleep_for, ctx.shutdown).await {
                return;
            }
            delay = (delay.saturating_mul(2)).min(REOPEN_BACKOFF_CAP);
        }
        let acc_arc = ctx.current.load_full();
        let acc: &dyn Account = acc_arc.as_ref().as_ref();
        match run_establish(
            ctx.account_id,
            acc,
            scope.clone(),
            Arc::clone(ctx.cursors),
            ctx.writer,
            Arc::clone(ctx.delivery),
            Some(control),
            Arc::clone(ctx.coverage),
            true,
            ctx.shutdown,
            None,
        )
        .await
        {
            Ok(_) => {
                if let Err(err) = link_discovered_memberships(acc, ctx.cursors).await {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?ctx.account_id,
                        scope = ?scope,
                        error = %err,
                        "scope re-establishment: membership refresh failed"
                    );
                }
                return;
            }
            Err(Error::Paused | Error::ShuttingDown) => return,
            Err(Error::Account(err) | Error::EstablishCursorTerminated(err)) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?ctx.account_id,
                    scope = ?scope,
                    attempt,
                    kind = ?err.kind(),
                    message_key = err.message_key(),
                    "scope re-establishment failed"
                );
                // Dispatch the classified error through `plan_recovery`
                // instead of blind-retrying every class three times: a
                // scope-quarantine directive names its own action, and
                // a terminal class cannot be repaired by another
                // attempt - breaking out lands in the exhaustion tail
                // below, which broadcasts `Terminated` plus the
                // operator warning exactly as a spent budget would.
                use crate::recovery::{RecoveryPlan, plan_recovery};
                match plan_recovery(err.clone()) {
                    RecoveryPlan::Engine(EngineDirective::DisableScope(quarantined)) => {
                        disable_scope(ctx, quarantined).await;
                        return;
                    }
                    RecoveryPlan::Terminal(_) => {
                        last_account_error = Some(err);
                        break;
                    }
                    _ => {
                        last_account_error = Some(err);
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?ctx.account_id,
                    scope = ?scope,
                    attempt,
                    error = %err,
                    "scope re-establishment failed (engine error)"
                );
                // Engine-level errors (EstablishCursorFailed,
                // CheckpointStore, ...) carry no AccountError of their
                // own. Synthesize one so a budget exhausted entirely on
                // engine-level failures still broadcasts
                // SyncEvent::Terminated, per the documented contract,
                // rather than emitting only the operator warning.
                last_account_error = Some(crate::recovery::establish_failure_error(
                    scope.clone(),
                    bifrost_types::AccountOperation::EstablishCursor,
                ));
            }
        }
    }
    // Budget exhausted for this scope. Emit the last AccountError
    // verbatim and warn that operator attention is needed. The account
    // continues running for sibling scopes; only this scope stays
    // unestablished.
    if let Some(err) = last_account_error {
        broadcast_terminated(ctx.changes_tx, scope.clone(), err);
    }
    broadcast_warning(
        ctx.changes_tx,
        Some(scope.clone()),
        bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::OperatorAttentionNeeded,
            "scope re-establishment retry budget exhausted",
        )
        .with_protocol_detail(DiagnosticText::support_only(format!(
            "scope: {scope:?}; attempts: {REOPEN_RETRY_BUDGET}"
        ))),
    );
}

/// Tear down the subscriptions already created on a replacement connection
/// that is about to be discarded.
///
/// Any handle whose delete did not succeed is registered against the account
/// as an orphan (`teardown_unconfirmed`, not `desired`), because the
/// replacement is closed immediately after and `Account::close()` is not
/// contracted to delete server-side subscriptions. It is not `desired`
/// because the consumer's desire still lives in the record it was recreated
/// from, which the abort leaves in the registry untouched.
/// Without that record the engine would forget the handle entirely and
/// recreate exactly the orphaned-webhook leak the retained-handle rule exists
/// to prevent - a provider like Graph keeps delivering to the endpoint until
/// the subscription expires on its own.
///
/// Cancellation-safe: a record leaves `replacements` only AFTER its delete has
/// answered, and a refusal is restored to the registry in the same
/// synchronous step that removes it. A drop parked inside a delete therefore
/// leaves that record (and every later one) in the guard, whose `Drop` retires
/// them; nothing is ever held only by a local that a drop would discard.
async fn unwind_replacement_subscriptions(
    ctx: &RecoveryContext<'_>,
    next: &dyn Account,
    replacements: &mut ReattachGuard,
) {
    while let Some(front) = replacements.live.first() {
        let result = next.push_unsubscribe(front.handle.clone()).await;
        let replacement = replacements.live.remove(0);
        if let Err(cleanup) = result {
            tracing::warn!(
                target: "bifrost.sync.reopen",
                account = ?ctx.account_id,
                error = %cleanup,
                "replacement push cleanup failed while unwinding reopen; retaining handle for retry"
            );
            // Through the sealed write, not a bare restore: this abort can run
            // after a detach that never waited for it, and an orphan landing
            // after detach's registry take is inherited by a later attach.
            register_orphans(
                ctx.subscriptions,
                ctx.account_id,
                ctx.shutdown,
                vec![replacement],
            );
        }
    }
}

/// Everything a reattach owes for the replacement connection it holds, until the
/// step that settles each debt has answered.
///
/// `Account::close()` does not delete server-side subscriptions, and the
/// registry does not know a replacement's handles until the cutover commits
/// them, so a reattach future dropped anywhere (a consumer timing out
/// `SyncEngine::reattach`, a worker abort at the detach deadline) would strand
/// the replacement connection, its subscriptions, and any cursor rows it had
/// persisted provisionally.
///
/// The guard is armed at CONSTRUCTION, never inside an async body a poll late:
/// `open_replacement` builds it in the same synchronous step that receives the
/// replacement from the factory, and it travels with the replacement into
/// `reattach_account`, so there is no interval, a drop before that function's
/// first poll included, in which a live replacement is owned only by a local. It
/// tracks four debts:
///
/// - `live`, the push handles the replacement created. A handle is pushed here
///   in the same poll that `push_subscribe` returned it. The commit `disarm`s
///   them into the registry with no await between the two; an ordinary abort
///   empties them through `unwind_replacement_subscriptions`, which removes a
///   record only after its delete answered.
/// - `close_owed`, the account whose `close()` is still owed: the replacement
///   until the swap, and from `swapped` on the previous account. It is closed
///   through [`Self::close_owed`], which clears the debt only once the close has
///   RETURNED, so a drop parked inside a close owes it again.
/// - `rollback_owed`, the compensation for rows `reattach_insert` may have
///   written. Armed synchronously before the first insert, cleared once
///   `reattach_abort` answered.
/// - `commit_owed`, which `swapped` converts the rollback into. Past the swap an
///   abort would delete cursors belonging to a reattach that succeeded, so a drop
///   there must promote the rows instead. Cleared once the commit is enqueued.
///
/// On drop, the writer request is enqueued SYNCHRONOUSLY with `try_send`: it
/// lands in the writer's queue ahead of anything a later reattach could insert,
/// which a spawned task would not guarantee, and the abort only ever deletes rows
/// still provisional. A full channel falls back to a spawned send. Then a task is
/// spawned to delete the remaining `live` handles on the replacement (the only
/// account that can be trusted to know them), record every refusal as an orphan,
/// and close what is owed. With no runtime to spawn on, the handles are
/// registered as orphans directly, undeleted, and the close cannot run.
///
/// Orphans are retried against whatever account is current when they are
/// retried, not against the replacement that created them. That is an accepted
/// limit, benign in this workspace because no provider rejects a handle it does
/// not know (the retry clears the orphan and cannot fail forever), and it is the
/// reason the spawned task tries the replacement itself first. It is not a
/// guarantee of deletion: Graph, IMAP and JMAP keep a handle's server state in
/// the account instance, so a retry that reaches a newer connection is a no-op
/// and the provider-side subscription then lives until it expires. See
/// `reference/sync.md`.
///
/// Known residue: a drop parked INSIDE `push_subscribe` may have created a
/// subscription whose handle never reached this guard. That is the account
/// implementation's cancellation contract, not something the engine can see.
pub(super) struct ReattachGuard {
    /// The replacement connection. Deletes of `live` run against it.
    account: Arc<dyn Account>,
    subscriptions: Arc<SubscriptionRegistry>,
    account_id: AccountId,
    shutdown: CancellationToken,
    /// The slot's reopen lock, for the spawned cleanup's registry write.
    reopen_lock: Arc<AsyncMutex<()>>,
    /// The account's writer queue, for the synchronous compensation on drop.
    writer: tokio::sync::mpsc::Sender<WriterRequest>,
    /// This attempt's unique id. It rides every provisional insert and the
    /// abort or commit that settles them, so the writer touches only rows this
    /// attempt owns, even when a delayed send from a dropped attempt arrives
    /// after a later attempt has inserted.
    attempt: u64,
    live: Vec<RegisteredSubscription>,
    close_owed: Option<Arc<dyn Account>>,
    rollback_owed: bool,
    commit_owed: bool,
}

/// Source of reattach attempt ids. Process-wide and monotonic, so an id is
/// unique across every slot and every writer, not just within one account.
static NEXT_REATTACH_ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl ReattachGuard {
    fn new(ctx: &RecoveryContext<'_>, account: Arc<dyn Account>) -> Self {
        Self {
            attempt: NEXT_REATTACH_ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            close_owed: Some(Arc::clone(&account)),
            account,
            subscriptions: Arc::clone(ctx.subscriptions),
            account_id: ctx.account_id.clone(),
            shutdown: ctx.shutdown.clone(),
            reopen_lock: Arc::clone(ctx.reopen_lock),
            writer: ctx.writer.sender(),
            live: Vec::new(),
            rollback_owed: false,
            commit_owed: false,
        }
    }

    fn push(&mut self, record: RegisteredSubscription) {
        self.live.push(record);
    }

    /// The commit exit: the caller registers what this returns, synchronously.
    fn disarm(&mut self) -> Vec<RegisteredSubscription> {
        std::mem::take(&mut self.live)
    }

    /// The cutover happened: the replacement is now the live account and the
    /// previous one is what needs closing, and the provisional rows are now the
    /// installed topology's, to be promoted rather than rolled back. Called
    /// synchronously right after the swap, with no await in between.
    fn swapped(&mut self, previous: Arc<dyn Account>) {
        self.close_owed = Some(previous);
        self.rollback_owed = false;
        self.commit_owed = true;
    }

    /// Close whatever account is owed, and stop owing it once the close returned.
    async fn close_owed(&mut self) -> Result<(), AccountError> {
        let Some(account) = self.close_owed.clone() else {
            return Ok(());
        };
        let result = account.close().await;
        self.close_owed = None;
        result
    }
}

/// Enqueue a writer request from a `Drop`. Never blocks: a closed channel means
/// the writer is gone (the slot was detached) and there is nothing to
/// compensate against; a full one falls back to a spawned send.
fn enqueue_writer_request(
    writer: &mpsc::Sender<WriterRequest>,
    request: WriterRequest,
    runtime: Option<&tokio::runtime::Handle>,
    account_id: &AccountId,
) {
    match writer.try_send(request) {
        Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
        Err(mpsc::error::TrySendError::Full(request)) => {
            if let Some(runtime) = runtime {
                let writer = writer.clone();
                runtime.spawn(async move {
                    let _ = writer.send(request).await;
                });
            } else {
                tracing::error!(
                    target: "bifrost.sync.reopen",
                    account = ?account_id,
                    "writer queue full and no runtime on a dropped reattach; \
                     provisional cursor rows keep their state until a later abort or commit"
                );
            }
        }
    }
}

impl Drop for ReattachGuard {
    fn drop(&mut self) {
        let runtime = tokio::runtime::Handle::try_current().ok();
        if self.rollback_owed {
            let (done, _ignored) = oneshot::channel();
            enqueue_writer_request(
                &self.writer,
                WriterRequest::ReattachAbort {
                    attempt: self.attempt,
                    done,
                },
                runtime.as_ref(),
                &self.account_id,
            );
        }
        if self.commit_owed {
            enqueue_writer_request(
                &self.writer,
                WriterRequest::ReattachCommit {
                    attempt: self.attempt,
                },
                runtime.as_ref(),
                &self.account_id,
            );
        }
        let live = std::mem::take(&mut self.live);
        let close = self.close_owed.take();
        if live.is_empty() && close.is_none() {
            return;
        }
        let account = Arc::clone(&self.account);
        let subscriptions = Arc::clone(&self.subscriptions);
        let account_id = self.account_id.clone();
        let shutdown = self.shutdown.clone();
        let reopen_lock = Arc::clone(&self.reopen_lock);
        match runtime {
            Some(runtime) => {
                runtime.spawn(retire_stranded(Stranded {
                    account,
                    subscriptions,
                    account_id,
                    shutdown,
                    reopen_lock,
                    live,
                    close,
                }));
            }
            None => {
                // No lock here, deliberately. Drop cannot await, and a blocking
                // acquire would stall a non-async thread on a lock the dropping
                // reattach itself usually still holds (its reopen guard lives in
                // a frame that is being torn down around this one). It is also
                // unnecessary: with no runtime nothing can be running a
                // concurrent reattach, `unsubscribe_push`, or `subscribe_push`
                // against this registry, so there is no snapshot to be
                // overwritten.
                tracing::warn!(
                    target: "bifrost.sync.reopen",
                    account = ?account_id,
                    stranded = live.len(),
                    close_owed = close.is_some(),
                    "reattach dropped with no runtime to clean up on; registering replacement \
                     handles as orphans, and an account owed a close cannot be closed"
                );
                register_orphans(&subscriptions, &account_id, &shutdown, live);
            }
        }
    }
}

/// Register `records` as orphans for retry, unless the slot is being torn down.
///
/// The teardown check is made by the registry, inside the shard lock `take`
/// runs under (`SubscriptionRegistry::restore_orphans`), not here beforehand.
/// Detach cancels the shutdown token and only later takes and discards the
/// account's registry entry, so an orphan written after that take would sit
/// under an id a later attach of the same `AccountId` inherits. A check made
/// before the write leaves a gap on a multi-thread runtime between reading the
/// token and writing the entry, wide enough for detach to cancel AND take.
fn register_orphans(
    subscriptions: &SubscriptionRegistry,
    account_id: &AccountId,
    shutdown: &CancellationToken,
    records: Vec<RegisteredSubscription>,
) {
    if records.is_empty() {
        return;
    }
    let stranded = records.len();
    let orphans = records
        .into_iter()
        .map(RegisteredSubscription::into_orphan)
        .collect();
    if !subscriptions.restore_orphans(account_id.clone(), orphans, shutdown) {
        tracing::warn!(
            target: "bifrost.sync.reopen",
            account = ?account_id,
            stranded,
            "slot detached before a refused push cleanup could be registered; leaving handles to provider expiry"
        );
    }
}

/// What a dropped reattach's cleanup task needs, gathered by the guard's `Drop`.
struct Stranded {
    /// The replacement, which the `live` deletes run against.
    account: Arc<dyn Account>,
    subscriptions: Arc<SubscriptionRegistry>,
    account_id: AccountId,
    shutdown: CancellationToken,
    reopen_lock: Arc<AsyncMutex<()>>,
    live: Vec<RegisteredSubscription>,
    /// The account still owed a `close()`: the replacement before the swap, the
    /// previous account after it.
    close: Option<Arc<dyn Account>>,
}

/// Cleanup for a reattach future that was dropped while owing something.
/// Runs detached from the dropped future.
///
/// The deletes and the close run WITHOUT the slot's reopen lock, so a concurrent
/// reattach never waits on this task's network calls. Only the registry write
/// takes it. A reattach snapshots the registry and later overwrites it with
/// `replace`, built from that snapshot alone: a refusal restored between the two
/// (this task's delete failing while a later reattach is mid-flight) would be
/// erased, and the provider subscription could never be retried. Under the lock
/// the restore lands either before that reattach snapshots (which then carries it
/// as it carries any orphan) or after it commits (`restore_orphans` merges into
/// what it installed). The lock is for THAT ordering only; the detach race is
/// closed by the registry's own check, which the lock could not close because
/// detach does not take it.
///
/// The dropped reattach's own reopen guard cannot deadlock this: the guard is
/// released by the drop, independently of this task, and this task only ever
/// waits on the lock asynchronously.
async fn retire_stranded(stranded: Stranded) {
    let Stranded {
        account,
        subscriptions,
        account_id,
        shutdown,
        reopen_lock,
        live,
        close,
    } = stranded;
    let mut refused = Vec::new();
    for record in live {
        if let Err(error) = account.push_unsubscribe(record.handle.clone()).await {
            tracing::warn!(
                target: "bifrost.sync.reopen",
                account = ?account_id,
                error = %error,
                "replacement push cleanup failed after a dropped reattach; retaining handle for retry"
            );
            refused.push(record);
        }
    }
    if !refused.is_empty() {
        let _reopen_guard = reopen_lock.lock().await;
        register_orphans(&subscriptions, &account_id, &shutdown, refused);
    }
    if let Some(owed) = close
        && let Err(error) = owed.close().await
    {
        tracing::warn!(
            target: "bifrost.sync.reopen",
            account = ?account_id,
            error = %error,
            "account close failed after a dropped reattach"
        );
    }
}

/// Delete the durable cursor rows an aborted reattach created.
///
/// This is deliberately the ONLY durable compensation the reopen path has,
/// and it only ever deletes rows whose scope had no stored cursor before
/// this reattach began. Preexisting rows are never deleted, snapshotted, or
/// restored here: the old account's ack writer keeps persisting checkpoints
/// concurrently (nothing serializes it against a reopen), so any
/// snapshot-and-restore of a preexisting row could overwrite a newer
/// consumer-acknowledged checkpoint. Deleting only rows this reattach itself
/// created cannot destroy preexisting data - the worst outcome of a failed
/// delete is a leaked cursor for a topology that was never installed.
/// Compensate an aborted reattach by deleting the cursor rows it inserted.
///
/// The set of rows is NOT passed in. The account's writer tracks which scopes
/// are still provisional - inserted by this reattach and not since claimed by a
/// consumer acknowledgement - and deletes exactly those. Passing a list
/// computed here would reintroduce the bug: this task cannot see whether an ack
/// landed for one of those scopes in the meantime, and a replacement inventory
/// pass broadcasts acknowledgeable checkpoints before the cutover, so the list
/// goes stale the moment a consumer acts on one.
///
/// Serializing the delete behind the writer is necessary but not sufficient on
/// its own; a plain FIFO queue would order the acknowledged write first and
/// then faithfully destroy it. The provisional set is what makes this a
/// conditional delete.
///
/// The guard's `rollback_owed` is cleared only once the writer answered, so a
/// drop parked inside this await still compensates (a second abort is
/// idempotent: the first left nothing provisional).
async fn rollback_reattach_inserts(ctx: &RecoveryContext<'_>, guard: &mut ReattachGuard) {
    ctx.writer.reattach_abort(guard.attempt).await;
    guard.rollback_owed = false;
}

/// Promote this reattach's provisional rows to ordinary durable state.
///
/// Called after the cutover commits, past the last point an abort can occur.
/// Without it the scopes stay marked provisional and the NEXT reattach's abort
/// would delete cursors belonging to a reattach that succeeded.
///
/// The guard's `commit_owed` is cleared once the request is enqueued (a
/// `reattach_commit` is only a channel send), so a drop parked while the
/// channel is full still promotes the rows.
async fn commit_reattach_inserts(ctx: &RecoveryContext<'_>, guard: &mut ReattachGuard) {
    if ctx.writer.reattach_commit(guard.attempt).await.is_err() {
        tracing::error!(
            target: "bifrost.sync.reopen",
            account = ?ctx.account_id,
            "writer channel closed before the reattach could be committed"
        );
    }
    guard.commit_owed = false;
}

/// Why no replacement connection was opened.
pub(super) enum ReplacementOpen {
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
/// This is also the ONLY place the reopen serialization lock is taken for
/// an open/swap, and the guard leaves only inside the returned tuple. That
/// is deliberate structure, not style: callers all wait for the boundary to
/// read `Run` before they get here, and a caller that took the lock itself
/// would hold it across that unbounded wait - which is exactly how
/// `unsubscribe_push` came to hang for the whole duration of a pause. With
/// acquisition owned here, holding the lock across a pause wait is not
/// something a caller can express.
pub(super) async fn open_replacement(
    ctx: &RecoveryContext<'_>,
) -> Result<(OwnedMutexGuard<()>, SyncActivityGuard, Replacement), ReplacementOpen> {
    let activity = ctx
        .control
        .begin_activity()
        .ok_or(ReplacementOpen::Paused)?;
    let reopen_guard = Arc::clone(ctx.reopen_lock).lock_owned().await;
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
    if let Some(budget) = ctx.bandwidth_budget {
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
pub(super) struct Replacement {
    guard: ReattachGuard,
    skipped_scopes: Vec<SkippedScope>,
}

/// Swap in a replacement connection. `activity` is the registration taken by
/// `open_replacement` before the connection was opened; holding it here keeps
/// the open and the swap inside one uninterrupted non-quiescent window.
pub(super) async fn reattach_account(
    ctx: &RecoveryContext<'_>,
    activity: SyncActivityGuard,
    replacement: Replacement,
) -> Result<(), Error> {
    let _activity = activity;
    let Replacement {
        mut guard,
        skipped_scopes: next_skips,
    } = replacement;
    let next = Arc::clone(&guard.account);
    next.set_priority(ctx.control.priority_snapshot());
    ctx.control
        .apply_bandwidth_cap(|cap| next.set_bandwidth_cap(cap));

    let result = async {
        let discovered = discover_scopes_from(next.as_ref()).await?;
        let staged = Arc::new(CursorRegistry::new());
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
            match run_establish(
                ctx.account_id,
                next.as_ref(),
                scope.clone(),
                Arc::clone(&staged),
                ctx.writer,
                Arc::clone(ctx.delivery),
                None,
                Arc::clone(ctx.coverage),
                false,
                ctx.shutdown,
                Some(guard.attempt),
            )
            .await
            {
                Ok(EstablishOrigin::ResumedStored) => {}
                Ok(EstablishOrigin::CreatedFresh) => {
                    if let Some(cursor) = staged.snapshot(scope) {
                        newly_established.push(cursor);
                    }
                }
                Err(Error::Account(error)) if scope_local_establish_failure(&error) => {
                    tracing::warn!(
                        target: "bifrost.sync.reopen",
                        account = ?ctx.account_id,
                        scope = ?scope,
                        error = %error,
                        "scope-local establishment failure during reopen; skipping scope"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        link_discovered_memberships(next.as_ref(), &staged).await?;

        let discovered_set: HashSet<_> = discovered.iter().cloned().collect();
        let vanished: Vec<_> = ctx
            .cursors
            .all_scopes()
            .into_iter()
            .filter(|scope| !discovered_set.contains(scope))
            .collect();

        let previous_subscriptions = ctx.subscriptions.snapshot(ctx.account_id);
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
            record.desired && next.capabilities().push != bifrost_types::PushCapability::None
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
                    if let Some(handle) = result.handle {
                        // Straight into the guard: no await between the provider
                        // answering and the handle being owned by something a
                        // drop will clean up.
                        guard.push(RegisteredSubscription {
                            handle,
                            scopes: covered,
                            teardown_unconfirmed: false,
                            desired: true,
                            torn_down: false,
                        });
                    }
                }
                Err(error) => {
                    unwind_replacement_subscriptions(ctx, next.as_ref(), &mut guard).await;
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
            unwind_replacement_subscriptions(ctx, next.as_ref(), &mut guard).await;
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
            if record.torn_down {
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
            let teardown = previous.push_unsubscribe(record.handle.clone()).await;
            match teardown {
                Ok(()) => {
                    // Recorded immediately, with no await between the delete
                    // returning and the registry write, so an abort later in
                    // this pass (or this future being dropped) cannot leave a
                    // deleted handle registered as live for the next attempt.
                    ctx.subscriptions
                        .mark_torn_down(ctx.account_id, &record.handle);
                }
                Err(error) if record.teardown_unconfirmed => {
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
                        account = ?ctx.account_id,
                        error = %error,
                        "carrying push subscription whose teardown is still unconfirmed"
                    );
                    carried_unconfirmed.push(record.clone().into_orphan());
                }
                Err(error) => {
                    // Still live and wanted on the old account, which stays
                    // installed if this pass aborts: flag it so the retry
                    // carries it instead of aborting on it again.
                    ctx.subscriptions
                        .mark_unconfirmed(ctx.account_id, &record.handle);
                    first_refusal.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_refusal {
            // The replacement's own subscriptions must not be stranded by this
            // unwind: `Account::close()` deliberately does not delete
            // server-side subscriptions, so any handle whose teardown did not
            // succeed stays in the registry - on whichever side it was
            // created - and is retried later.
            unwind_replacement_subscriptions(ctx, next.as_ref(), &mut guard).await;
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
        let previous = ctx.current.swap(Arc::new(Arc::clone(&next)));
        // No await between the swap and this: from here a drop must close the
        // OLD account (the replacement is live) and promote the provisional
        // rows instead of rolling them back.
        guard.swapped(Arc::clone(previous.as_ref()));
        ctx.cursors.replace_topology_preserving_cursors(&staged);
        {
            let mut capabilities = match ctx.capabilities.write() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            *capabilities = next.capabilities().clone();
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
        if let Err(refused) = ctx.subscriptions.replace(
            ctx.account_id.clone(),
            installed_subscriptions,
            ctx.shutdown,
        ) {
            for record in refused {
                if record.desired {
                    guard.push(record);
                } else {
                    refused_install += 1;
                }
            }
        }
        // The replacement open's skip lane supersedes the previous
        // one: a healed namespace disappears from it, a still-degraded
        // one reappears with a fresh classification.
        log_open_skips(ctx.account_id, &next_skips, "reopen");
        {
            let mut skips = ctx
                .open_skips
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *skips = next_skips;
        }
        ctx.account_generation_tx
            .send_modify(|generation| *generation = generation.saturating_add(1));

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
                    account = ?ctx.account_id,
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
                account = ?ctx.account_id,
                handles = guard.live.len(),
                carried_orphans = refused_install,
                "slot detached during reattach; tearing down the replacement's push \
                 subscriptions, which no registry entry can hold"
            );
        }
        unwind_replacement_subscriptions(ctx, next.as_ref(), &mut guard).await;

        // Through the guard, which owes this close until it returns: a drop
        // parked here (or before it) closes the old connection instead of
        // leaking it.
        if let Err(error) = guard.close_owed().await {
            tracing::warn!(
                target: "bifrost.sync.reopen",
                account = ?ctx.account_id,
                error = %error,
                "old account close failed during reopen"
            );
        }
        Ok(())
    }
    .await;

    if result.is_err()
        && let Err(error) = guard.close_owed().await
    {
        tracing::warn!(
            target: "bifrost.sync.reopen",
            account = ?ctx.account_id,
            error = %error,
            "replacement account close failed while unwinding reopen"
        );
    }
    result
}

pub(super) fn accepted_push_scopes(result: &bifrost_types::PushSubscription) -> Vec<CursorScope> {
    result
        .outcomes
        .succeeded()
        .iter()
        .map(|success| success.output.clone())
        .collect()
}

/// Restart the whole account with exponential backoff and a retry
/// budget. After three consecutive failed attempts - a `factory.open`
/// failure or a replacement that could not be swapped in, an old-side push
/// teardown failure included - the account is paused with
/// `PauseReason::RetryBudgetExhausted` and the last `AccountError` is
/// broadcast as `SyncEvent::Terminated`. (sync-D6, sync-D7)
///
/// Returns whether a replacement was actually swapped in. `false` covers the
/// exhausted budget (the account is now paused), a detach, and shutdown; a
/// caller must not run follow-up repairs against an account that did not
/// restart.
async fn restart_account(ctx: &RecoveryContext<'_>) -> bool {
    let mut delay = REOPEN_BACKOFF_INITIAL;
    let mut last_error: Option<AccountError> = None;
    let mut attempt = 0;
    while attempt < REOPEN_RETRY_BUDGET {
        // A consumer pause is a quiescence boundary. Keep this recovery
        // request queued until resume instead of opening a replacement while
        // the account has promised to be idle.
        if !ctx.control.wait_until_running(ctx.shutdown).await {
            return false;
        }
        if attempt > 0 {
            let sleep_for = jittered(delay);
            if !sleep_unless_shutdown(sleep_for, ctx.shutdown).await {
                return false;
            }
            delay = (delay.saturating_mul(2)).min(REOPEN_BACKOFF_CAP);
        }
        match open_replacement(ctx).await {
            // A pause won the race between the wait above and the activity
            // registration, and nothing was opened. Loop back through the
            // boundary wait instead of spending an attempt on it.
            Err(ReplacementOpen::Paused) => continue,
            // The slot is being torn down. Nothing to restart, and the
            // replacement (if one was opened at all) has already been closed.
            Err(ReplacementOpen::Detached) => return false,
            Ok((_reopen_guard, activity, next)) => {
                match reattach_account(ctx, activity, next).await {
                    Ok(()) => return true,
                    Err(Error::Account(error) | Error::EstablishCursorTerminated(error)) => {
                        tracing::warn!(
                            target: "bifrost.sync.changes",
                            account = ?ctx.account_id,
                            attempt,
                            kind = ?error.kind(),
                            message_key = error.message_key(),
                            "RestartAccount: replacement attach failed"
                        );
                        last_error = Some(error);
                        attempt = attempt.saturating_add(1);
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "bifrost.sync.changes",
                            account = ?ctx.account_id,
                            attempt,
                            error = %error,
                            "RestartAccount: replacement attach failed"
                        );
                        last_error = Some(crate::recovery::establish_failure_error(
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
                    account = ?ctx.account_id,
                    attempt,
                    kind = ?err.kind(),
                    message_key = err.message_key(),
                    "RestartAccount: factory.open failed"
                );
                last_error = Some(err);
                attempt = attempt.saturating_add(1);
            }
        }
    }
    // Account-wide budget exhausted: emit Terminated + pause the
    // account. All scopes pause as a consequence of the engine no
    // longer driving work for this account.
    if let Some(err) = last_error {
        broadcast_terminated(ctx.changes_tx, CursorScope::Account, err);
    }
    engine_pause(ctx, PauseReason::RetryBudgetExhausted);
    false
}

/// Engine-driven pause: publish `AccountControl::Pause(reason)` on the
/// account's control broadcast and trip the boundary so per-scope
/// workers park. Consumers flip `AccountControl::Resume` (engine-side
/// helper on `SyncEngine`) to unpause.
fn engine_pause(ctx: &RecoveryContext<'_>, reason: PauseReason) {
    let _ = ctx.account_control_tx.send(AccountControl::Pause(reason));
    // Flip the boundary to Pause. Per-scope polls / push reconciler
    // park on `boundary.peek() == Pause`, so this halts all engine-
    // driven work for the account until a consumer calls
    // `SyncEngine::resume_account` (or sends `Resume` through
    // `Control::resume`).
    ctx.boundary_tx
        .send_replace(crate::cancel::BoundaryRequest::Pause);
}

/// Emit one structured `warn!` per open-time skip. The durable record
/// is the slot's `open_skips` lane (read via
/// `SyncEngine::open_skipped_scopes`); the log line exists so a
/// degraded shared namespace is visible in telemetry even when the
/// consumer never queries the lane.
pub(super) fn log_open_skips(account_id: &AccountId, skips: &[SkippedScope], phase: &'static str) {
    for skip in skips {
        tracing::warn!(
            target: "bifrost.sync.attach",
            account = ?account_id,
            scope = ?skip.scope,
            kind = ?skip.error.kind(),
            message_key = skip.error.message_key(),
            phase,
            "open skipped a degraded scope; primary surface attached without it"
        );
    }
}

pub(super) fn jittered(base: Duration) -> Duration {
    // ±20% jitter to break thundering-herd reopen lockstep. Entropy
    // comes from the workspace UUID RNG (the same source bifrost-net's
    // backoff uses) rather than `SystemTime::now()`: a wall-clock jump
    // or low-resolution clock must not influence the jitter, and a
    // clock running backwards would otherwise collapse the spread.
    let entropy = i64::try_from(uuid::Uuid::new_v4().as_u128() % 401).unwrap_or(0);
    let percent_offset = entropy - 200; // [-200, +200] basis points
    let base_ms = i64::try_from(base.as_millis()).unwrap_or(i64::MAX);
    let delta_ms = (base_ms * percent_offset) / 1_000;
    let total = u64::try_from((base_ms + delta_ms).max(0)).unwrap_or(0);
    Duration::from_millis(total)
}

fn broadcast_terminated(
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    scope: CursorScope,
    error: AccountError,
) {
    let me = MultiplexerEvent {
        scope,
        event: Arc::new(SyncEvent::Terminated(error)),
        checkpoint: None,
        publication: None,
    };
    let _ = changes_tx.send(me);
}

/// Broadcast a `Warning` on the per-account changes channel. The
/// `scope` argument is the directive's target scope when available;
/// the caller passes `None` only for warnings that are genuinely
/// account-wide. The fallback is `CursorScope::Account` because the
/// `MultiplexerEvent::scope` field is `CursorScope`, not
/// `Option<CursorScope>`. (sync-N4)
fn broadcast_warning(
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    scope: Option<CursorScope>,
    warning: bifrost_types::Warning,
) {
    let me = MultiplexerEvent {
        scope: scope.unwrap_or(CursorScope::Account),
        event: Arc::new(SyncEvent::Warning(warning)),
        checkpoint: None,
        publication: None,
    };
    let _ = changes_tx.send(me);
}

/// How `run_establish` obtained a scope's cursor. Reattach uses this to
/// decide which durable rows an aborted swap may delete: only a cursor
/// this establishment created fresh corresponds to a row the reattach
/// itself will write, so only those may be compensated by deletion. The
/// distinction is reported from inside `run_establish` - off the single
/// store read it already performs - rather than probed by the caller
/// beforehand, because a separate pre-check races that read (and a
/// pre-check that swallowed a store error once misclassified a
/// preexisting row as freshly created, which an abort then deleted).
#[derive(Clone, Copy, PartialEq, Eq)]
enum EstablishOrigin {
    /// A durable row already existed and was resumed from (directly, or
    /// by finishing the inventory it recorded). The stored row is
    /// preexisting data that no reattach abort may touch.
    ResumedStored,
    /// No durable row existed; whatever cursor the registry now holds
    /// for the scope was created fresh by this establishment.
    CreatedFresh,
}

/// Re-establish a single scope. Mirrors `SyncEngine::establish_one`
/// but lives at file scope so the reopen listener task can call it
/// without owning a reference to the engine.
#[allow(clippy::too_many_arguments)]
async fn run_establish(
    account_id: &AccountId,
    account: &dyn Account,
    scope: CursorScope,
    cursors: Arc<CursorRegistry>,
    writer: &WriterHandle,
    delivery: Arc<crate::multiplexer::ChangeDelivery>,
    control: Option<&SyncControl>,
    coverage: Arc<PendingCoverage>,
    persist_ready: bool,
    shutdown: &CancellationToken,
    reattach_attempt: Option<u64>,
) -> Result<EstablishOrigin, Error> {
    let _activity = match control {
        Some(control) => Some(control.begin_activity().ok_or(Error::Paused)?),
        None => None,
    };
    // Only a reattach passes an attempt. Its resume read claims a still
    // provisional row through the writer; every other caller reads without
    // claiming.
    match writer
        .get_change_cursor(scope.clone(), reattach_attempt)
        .await
    {
        Ok(Some(existing)) if existing.validate_envelope().is_ok() => {
            // A stored cursor may be a mid-inventory page position rather
            // than a live changes cursor. Putting one into the registry
            // would hand it straight to `changes_stream`, which has no
            // delta link to walk. Finish the inventory instead - the same
            // decision `establish_initial_cursor` makes at open.
            if account.is_inventory_cursor(&existing) {
                let fusion = crate::multiplexer::InventoryFusion {
                    account_id: account_id.clone(),
                    cursors: Arc::clone(&cursors),
                    control: control.cloned(),
                    coverage: Some(Arc::clone(&coverage)),
                    writer_tx: Some(writer.sender()),
                    generation: coverage.next_generation(),
                    shutdown: Some(shutdown.clone()),
                };
                return match fusion
                    .run_resume_with_broadcast(account, existing, Some(delivery))
                    .await?
                {
                    crate::multiplexer::FusionOutcome::Established
                    | crate::multiplexer::FusionOutcome::NoCursor => {
                        Ok(EstablishOrigin::ResumedStored)
                    }
                    crate::multiplexer::FusionOutcome::Terminated(error) => {
                        Err(Error::EstablishCursorTerminated(error))
                    }
                };
            }
            cursors.put(existing);
            return Ok(EstablishOrigin::ResumedStored);
        }
        Ok(None) => {}
        // Unlike `establish_one`, this path runs with a live reopen
        // listener, so an undecodable envelope is reported as a typed
        // error whose derived `RecoveryClass` is
        // `Engine(SchemaIncompatible)` and the listener runs the
        // account-wide schema-clear loop.
        Ok(Some(_)) | Err(Error::SchemaIncompatible) => {
            return Err(Error::Account(crate::recovery::cursor_decode_failure(
                bifrost_types::AccountOperation::EstablishCursor,
            )));
        }
        Err(other) => return Err(other),
    }
    match account
        .establish_initial_cursor(scope.clone())
        .await
        .map_err(Error::Account)?
    {
        CursorEstablishment::Ready(cursor) => {
            cursor.validate_envelope().map_err(|_| {
                Error::Account(crate::recovery::cursor_decode_failure(
                    bifrost_types::AccountOperation::EstablishCursor,
                ))
            })?;
            if persist_ready {
                // No coverage report: preserve the ledger rather than
                // asserting completeness a cursor establishment never proved.
                writer.persist_established(cursor.clone()).await?;
            }
            cursors.put(cursor);
            Ok(EstablishOrigin::CreatedFresh)
        }
        CursorEstablishment::EstablishViaInventory => {
            let fusion = crate::multiplexer::InventoryFusion {
                account_id: account_id.clone(),
                cursors: Arc::clone(&cursors),
                control: control.cloned(),
                coverage: Some(Arc::clone(&coverage)),
                writer_tx: Some(writer.sender()),
                generation: coverage.next_generation(),
                shutdown: Some(shutdown.clone()),
            };
            match fusion
                .run_with_broadcast(account, scope, Some(delivery))
                .await?
            {
                crate::multiplexer::FusionOutcome::Established
                | crate::multiplexer::FusionOutcome::NoCursor => Ok(EstablishOrigin::CreatedFresh),
                crate::multiplexer::FusionOutcome::Terminated(error) => {
                    Err(Error::EstablishCursorTerminated(error))
                }
            }
        }
        _ => Err(Error::EstablishCursorFailed(
            "unknown CursorEstablishment variant".into(),
        )),
    }
}
