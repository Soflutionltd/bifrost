//! Project recovery plans into engine actions.

use super::{schema::handle_schema_incompatible, scoped::*, *};
use crate::engine::reattach::restart_account;

/// Dispatch an `AccountError` to the engine's recovery machinery.
///
/// Projects already dispatched failures through `plan_recovery` so `RecoveryPlan`
/// drives the dispatch. The `scope` argument is the worker's
/// convenience-suggested scope: scope-bound directives use the
/// directive's own scope when present; account-wide directives ignore
/// it.
///
/// Returns the obligation the caller must park because nothing else will raise
/// it again: a repair of a cursorless scope declined under a pause (a lifecycle
/// `Created` scope, or a deferred inventory scope, has no failing cursor for the
/// poll to re-raise the directive from - see [`DeferredScopeRepairs`]), or an
/// account schema reset that was declined under a pause or exhausted its retry
/// budget, with the scopes it owes re-establishment (see
/// [`DeferredSchemaReset`]). Every other outcome is [`RecoveryFollowUp::None`].
pub(crate) async fn handle_account_error(
    ctx: &RecoveryContext<'_>,
    scope: Option<CursorScope>,
    error: AccountError,
) -> RecoveryFollowUp {
    use crate::recovery::RecoveryPlan;
    // The originating drive, push lane, or deferred-inventory owner already
    // recorded throttle and telemetry effects. Repeating dispatch here moves
    // relative hold deadlines forward just because the listener was queued.
    let plan = crate::recovery::plan_recovery(error.clone());
    let mut deferred = RecoveryFollowUp::None;
    match plan {
        RecoveryPlan::Retry(advice) => {
            // The reopen listener is a serialization point for real
            // engine directives. Never sleep here: no failed operation is retried
            // after such a sleep, and blocking this task would hold
            // RestartScope / RestartAccount requests behind an inert
            // provider Retry-After delay.
            tracing::debug!(
                target: "bifrost.sync.recovery",
                account = ?ctx.account_id,
                scope = ?scope,
                retry = ?advice,
                "retry recovery reached reopen listener; caller owns the retry"
            );
        }
        RecoveryPlan::Reconcile(advice) => {
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
            // does need is taken in `restart_account`'s admitted open loop,
            // spanning only the open and the swap. Reintroducing an acquisition
            // here deadlocks against that one rather than silently regressing.
            //
            // `DowngradeStrategy` restarts the account too, so the same
            // holds: taking the lock here made its `restart_account` wait
            // forever on the restart loop's acquisition of this very
            // (non-reentrant) mutex, hanging recovery on the first strategy
            // downgrade. Its trailing scope restart takes the lock itself.
            let _reopen_guard = match directive {
                EngineDirective::RestartAccount | EngineDirective::DowngradeStrategy(_) => None,
                _ => Some(ctx.reopen_lock.lock().await),
            };
            if _reopen_guard.is_some() && stale_generation(ctx) {
                return RecoveryFollowUp::None;
            }
            deferred = handle_engine_directive(ctx, scope, directive, error).await;
        }
        RecoveryPlan::Terminal(fatal) => {
            // Deferred inventory returns FusionOutcome::Terminated only after
            // fusion (including forward_inventory_batch) publishes the error.
            // Other producers send only Engine directives here. Publishing a
            // terminal again would duplicate the consumer's notification.
            // Keep this entry effect-free for terminal advice. Emit a
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
            ReconcileAction::ObserveOnly => "observe-only",
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
) -> RecoveryFollowUp {
    // The scope of a declined repair that nothing will raise again; see
    // `handle_account_error`.
    let mut deferred = RecoveryFollowUp::None;
    // Every current directive has an explicit dispatch arm. The required
    // non-exhaustive fallback makes a future variant visible in logs until a
    // human gives it an intentional engine action.
    match directive {
        EngineDirective::RestartScope(directive_scope) => {
            // Pass the directive's own scope to broadcast_warning so
            // the multiplexer event's `scope` matches the affected
            // scope - not the worker-suggested fallback. (sync-N4)
            //
            // A declined repair of a scope with no registered cursor - a
            // deferred inventory scope whose walk terminated, before any
            // cursor exists - has no failing cursor to re-raise it after
            // resume, so it is handed back for the listener to re-offer. A
            // scope WITH a cursor needs nothing: the poll raises it again.
            if !restart_scope(ctx, directive_scope.clone()).await
                && ctx.cursors.snapshot(&directive_scope).is_none()
            {
                deferred = RecoveryFollowUp::ParkScope {
                    epoch: ctx.cursors.lifecycle_epoch(&directive_scope),
                    scope: directive_scope,
                };
            }
        }
        EngineDirective::DowngradeCapabilityForScope(directive_scope) => {
            // Registered before the warning, so a deferred repair does not
            // announce a downgrade that will be announced again when the poll
            // re-raises the directive after resume.
            let Some(activity) = begin_scope_repair(ctx, &directive_scope) else {
                return RecoveryFollowUp::None;
            };
            broadcast_warning(
                ctx.delivery,
                Some(directive_scope.clone()),
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::Engine(
                        bifrost_types::EngineNotice::ScopeCapabilityDowngraded,
                    ),
                    format!("scope capability downgraded: {directive_scope:?}"),
                )
                .with_scope(ErrorScope::Cursor(directive_scope.clone()))
                .with_error(error.clone())
                .with_protocol_detail(DiagnosticText::support_only(format!("{directive_scope:?}"))),
            );
            restart_scope_admitted(ctx, directive_scope, &activity).await;
        }
        EngineDirective::RestartAccount => {
            restart_account(ctx).await;
        }
        EngineDirective::DowngradeStrategy(downgrade) => {
            // Fence even the warning before releasing the lock for restart's
            // independently admitted loop.
            let warning_guard = ctx.reopen_lock.lock().await;
            if stale_generation(ctx) {
                return RecoveryFollowUp::None;
            }
            // Route the warning to the scope the downgrade is actually
            // about: the originating error's cursor scope when it names
            // one (the same signal the immediate re-establish below
            // keys on), else the worker's suggestion, else account.
            let warning_scope = match error.scope() {
                Some(ErrorScope::Cursor(scoped)) => Some(scoped.clone()),
                _ => fallback_scope.clone(),
            };
            broadcast_warning(
                ctx.delivery,
                warning_scope,
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::StrategyDowngraded,
                    format!("downgraded sync strategy: {downgrade:?}"),
                )
                .with_strategy_downgrade(downgrade)
                .with_error(error.clone())
                .with_protocol_detail(DiagnosticText::support_only(format!("{downgrade:?}"))),
            );
            drop(warning_guard);
            // Only an account that actually restarted gets the scope repair.
            // `restart_scope` deletes the scope's cursor before it
            // re-establishes; after an exhausted budget the account is
            // paused, the establishment is refused, and the deleted scope
            // would stay gone after resume.
            if !restart_account(ctx).await {
                return RecoveryFollowUp::None;
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
                // A stamped request's own restart bumped the generation once;
                // anything else means a later replacement owns the account now.
                if ctx
                    .expected_generation
                    .is_some_and(|g| ctx.current.load().generation() != g.successor())
                {
                    tracing::debug!(
                        target: "bifrost.sync.recovery",
                        account = ?ctx.account_id,
                        "declining stale scope repair after strategy downgrade"
                    );
                    return RecoveryFollowUp::None;
                }
                restart_scope(ctx, scoped.clone()).await;
            }
        }
        EngineDirective::SchemaIncompatible => {
            let mut extra = HashMap::new();
            if let Some(scope) = fallback_scope {
                extra.insert(scope.clone(), ctx.cursors.lifecycle_epoch(&scope));
            }
            deferred = handle_schema_incompatible(ctx, extra).await;
        }
        EngineDirective::OperatorOverrideRequired { reason } => {
            // Auto-pause the account: the engine no longer drives work
            // for it until the consumer flips `AccountControl::Resume`.
            // The reason is bounded (`PauseReason::OperatorOverrideRequired`);
            // the typed directive reason rides through the warning too.
            // (sync-D9)
            // Deliberately account-scoped, not the worker's fallback:
            // the directive pauses the WHOLE account, and a consumer
            // routing the warning to one folder's scope would misfile
            // an account-wide condition.
            broadcast_warning(
                ctx.delivery,
                None,
                bifrost_types::Warning::support_only(
                    bifrost_types::WarningKind::Engine(
                        bifrost_types::EngineNotice::OperatorOverrideRequired(reason.clone()),
                    ),
                    reason.to_string(),
                )
                .with_scope(ErrorScope::Account)
                .with_error(error.clone())
                .with_protocol_detail(DiagnosticText::support_only(reason.to_string())),
            );
            engine_pause(ctx, PauseReason::OperatorOverrideRequired);
        }
        EngineDirective::DisableScope(directive_scope) => {
            disable_scope(ctx, directive_scope, error.clone()).await;
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

/// True when this request is stamped with the generation of an incarnation
/// that has since been replaced. Called under the reopen lock, before any
/// effect: reattach swaps the incarnation under that same lock, so the read
/// cannot straddle a swap. It reads the installed incarnation, not the
/// account-generation watch, which reattach publishes only after the swap.
pub(in crate::engine) fn stale_generation(ctx: &RecoveryContext<'_>) -> bool {
    let stale = ctx
        .expected_generation
        .is_some_and(|g| g != ctx.current.load().generation());
    if stale {
        tracing::debug!(
            target: "bifrost.sync.recovery",
            account = ?ctx.account_id,
            "declining stale recovery raised against a replaced account"
        );
    }
    stale
}
