//! Scope restart, establish-if-absent and quarantine.

use super::{pacing::*, *};
use crate::engine::establish::run_establish;

/// The `EstablishIfAbsent` arm, called under the reopen lock: establish
/// `scope` unless it is registered or a provider deletion moved its lifecycle
/// epoch past `epoch`.
///
/// It restarts rather than merely establishing. The obligation may come from a
/// declined `RestartScope` of a cursorless scope - a deferred inventory whose
/// stored resume cursor was rejected, say - whose durable row is exactly what
/// must go; establishing over it would resume the row the directive condemned.
/// After an account reset the row is already gone and the restart's reset is
/// a no-op.
///
/// The epoch is checked twice. Before anything moves, so a deleted or
/// registered scope is not reset at all; and again AFTER the walk fence is
/// read, which is what makes the fence trustworthy: a deletion bumps the epoch
/// before it raises the fence (`retire_deleted_scope`), so a deletion that
/// passes the second check raised its fence after the read, and every install
/// of the establishment then refuses.
pub(crate) async fn establish_if_absent(
    ctx: &RecoveryContext<'_>,
    scope: CursorScope,
    epoch: crate::cursor::LifecycleEpoch,
) -> RecoveryFollowUp {
    if ctx.cursors.lifecycle_epoch(&scope) != epoch || ctx.cursors.snapshot(&scope).is_some() {
        return RecoveryFollowUp::None;
    }
    let Some(activity) = begin_scope_repair(ctx, &scope) else {
        return RecoveryFollowUp::ParkScope { scope, epoch };
    };
    if let Err(err) = ctx.writer.reset_scope_for_restart(scope.clone()).await {
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            scope = ?scope,
            error = %err,
            "EstablishIfAbsent: durable scope reset failed"
        );
    }
    let walk_fence = ctx.coverage.scope_fence(&scope);
    if ctx.cursors.lifecycle_epoch(&scope) != epoch {
        return RecoveryFollowUp::None;
    }
    let admitted = ctx.control.admitted_by(&activity);
    re_establish_scope_with_backoff(ctx, scope, &admitted, walk_fence).await;
    RecoveryFollowUp::None
}

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
pub(in crate::engine) async fn restart_scope(
    ctx: &RecoveryContext<'_>,
    scope: CursorScope,
) -> bool {
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
pub(in crate::engine) fn begin_scope_repair(
    ctx: &RecoveryContext<'_>,
    scope: &CursorScope,
) -> Option<SyncActivityGuard> {
    let activity = ctx.control.begin_activity();
    if activity.is_none() {
        tracing::debug!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            scope = ?scope,
            "scope repair deferred: the account is paused; a registered failing cursor \
             raises the directive again after resume, and a cursorless scope is parked"
        );
    }
    activity
}

pub(in crate::engine) async fn restart_scope_admitted(
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
    let walk_fence = ctx.coverage.scope_fence(&scope);
    let admitted = ctx.control.admitted_by(activity);
    re_establish_scope_with_backoff(ctx, scope, &admitted, walk_fence).await;
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
pub(in crate::engine) async fn disable_scope(
    ctx: &RecoveryContext<'_>,
    scope: CursorScope,
    error: AccountError,
) {
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
        ctx.delivery,
        Some(scope.clone()),
        bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::Engine(bifrost_types::EngineNotice::ScopeDisabled),
            format!("shared folder access revoked; scope disabled: {scope:?}"),
        )
        .with_scope(ErrorScope::Cursor(scope.clone()))
        .with_error(error)
        .with_protocol_detail(DiagnosticText::support_only(format!("{scope:?}"))),
    );
}

/// `control` is what the establishment registers its own activity on: the
/// account's plain control for a caller holding no registration, or the handle
/// [`SyncControl::admitted_by`] a registration the caller already holds, so a
/// pause that lands mid-repair cannot refuse the nested one. It is also what
/// the inventory walk under the establishment publishes its checkpoints
/// through, which is why it is passed whole rather than dropped.
pub(in crate::engine) async fn re_establish_scope_with_backoff(
    ctx: &RecoveryContext<'_>,
    scope: CursorScope,
    control: &SyncControl,
    walk_fence: crate::cursor::ScopeFence,
) {
    let mut backoff = crate::backoff::ReconnectBackoff::new(REOPEN_BACKOFF);
    let mut last_account_error: Option<AccountError> = None;
    let mut pacing = RepairPacing::default();
    for attempt in 0..REOPEN_RETRY_BUDGET.0 {
        if attempt > 0 && pacing.local_deadline.is_none() {
            let sleep_for = backoff.record_failure(crate::backoff::jitter_seed());
            if !sleep_unless_shutdown(sleep_for, ctx.shutdown).await {
                return;
            }
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
            control,
            Arc::clone(ctx.coverage),
            true,
            ctx.shutdown,
            None,
            ctx,
            &mut pacing,
            walk_fence,
        )
        .await
        {
            Ok(_) => {
                if let Err(err) = link_discovered_memberships(acc, ctx.cursors).await {
                    if let Error::Account(error) = &err {
                        pacing.observe(ctx, error);
                    }
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
            Err(Error::Paused | Error::ShuttingDown | Error::Superseded) => return,
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
                // Dispatch the classified error through `dispatch_stream_failure`
                // instead of blind-retrying every class three times: a
                // scope-quarantine directive names its own action, and
                // a terminal class cannot be repaired by another
                // attempt - breaking out lands in the exhaustion tail
                // below, which broadcasts `Terminated` plus the
                // operator warning exactly as a spent budget would.
                use crate::recovery::RecoveryPlan;
                match crate::recovery::dispatch_stream_failure(
                    err.clone(),
                    ctx.throttles,
                    ctx.account_id,
                ) {
                    RecoveryPlan::Engine(EngineDirective::DisableScope(quarantined)) => {
                        disable_scope(ctx, quarantined, err.clone()).await;
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
            Err(Error::WaitBudgetExhausted) => {
                warn_wait_budget(ctx, scope);
                return;
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
                last_account_error = Some(crate::engine_errors::establish_failure_error(
                    scope.clone(),
                    bifrost_types::AccountOperation::EstablishCursor,
                ));
            }
        }
    }
    // Budget exhausted for this scope. A retry or reconciliation verdict no
    // longer describes an action the engine will take, so publish an
    // operator-attention error instead. Preserve provider errors already
    // classified terminal and existing operator-attention errors. The account
    // continues running for sibling scopes; only this scope stays
    // unestablished.
    let warning_error = last_account_error.clone();
    if let Some(err) = last_account_error {
        broadcast_terminated(
            ctx.delivery,
            EventScope::Cursor(scope.clone()),
            crate::engine_errors::recovery_exhaustion_error(
                err,
                scope.clone(),
                bifrost_types::AccountOperation::EstablishCursor,
            ),
        );
    }
    broadcast_warning(
        ctx.delivery,
        Some(scope.clone()),
        bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::Engine(
                bifrost_types::EngineNotice::EstablishRetryBudgetExhausted,
            ),
            "scope re-establishment retry budget exhausted",
        )
        .with_scope(ErrorScope::Cursor(scope.clone()))
        .with_optional_error(warning_error)
        .with_protocol_detail(DiagnosticText::support_only(format!(
            "scope: {scope:?}; attempts: {}",
            REOPEN_RETRY_BUDGET.0
        ))),
    );
}
