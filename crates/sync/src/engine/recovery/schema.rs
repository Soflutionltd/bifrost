//! Account-wide schema recovery and parked reset execution.

use super::{
    pacing::*,
    scoped::{establish_if_absent_after_schema_reset, re_establish_scope_after_schema_reset},
    *,
};

/// The `SchemaReset` arm, called under the reopen lock.
///
/// If a reset committed after the newest decline, its clear was account-wide,
/// so every owed scope's rows are already gone: establish each owed scope that
/// is still absent instead of running a second reset. Otherwise run the reset.
pub(crate) async fn run_parked_schema_reset(
    ctx: &RecoveryContext<'_>,
    reestablish: HashMap<CursorScope, crate::cursor::LifecycleEpoch>,
    parked_at: ResetCount,
) -> RecoveryFollowUp {
    if ctx.deferred_schema_reset.completed() <= parked_at {
        return handle_schema_incompatible(ctx, reestablish).await;
    }
    let mut declined = HashMap::new();
    for (scope, epoch) in reestablish {
        if let RecoveryFollowUp::ParkScope { scope, epoch } =
            establish_if_absent_after_schema_reset(ctx, scope, epoch).await
        {
            declined.insert(scope, epoch);
        }
    }
    if declined.is_empty() {
        RecoveryFollowUp::None
    } else {
        RecoveryFollowUp::ParkScopes(declined)
    }
}

/// Account-wide schema recovery: clear every durable row of the account and
/// re-establish every scope it owes, revoking every pre-reset producer first.
///
/// A schema bump means the ids the consumer holds were minted under an
/// encoding the protocol has disowned, and re-minting them requires the
/// inventory re-walk a surviving completion marker would skip. For protocols
/// whose establishment returns a `Ready` cursor from live state (JMAP), the
/// next attach's re-walk IS the migration; this session's re-established
/// cursor only covers changes from now on. The clear is account-wide, not per
/// registered scope, because the registry does not name every scope with rows
/// (see `WriterHandle::reset_account_for_schema_recovery`).
///
/// Order, and why:
///
/// 1. Register activity, or decline on a paused account and hand the owed
///    scopes back for parking: the reset deletes every cursor before it
///    re-establishes any, so a pause landing inside it would strand them.
/// 2. Raise the account fence: every fence-guarded producer of every scope
///    (backfill pages and markers, inventory fusions, establishments) is
///    revoked before anything else moves.
/// 3. Snapshot and delete every registered cursor, recording its lifecycle
///    epoch as owed. The registry delete bumps drive generations, revoking
///    live drives; it must precede the clear, or an old drive could mint an
///    acceptable publication after it.
/// 4. The writer's account clear, retried with backoff (the reopen lock is
///    held throughout, as `RestartScope` does).
/// 5. On success, re-establish every owed scope still absent and not deleted
///    since, each against the fence read after the reset's own raises.
/// 6. On exhaustion, warn, pause with `RetryBudgetExhausted`, and hand the
///    owed scopes back: after resume the parked reset re-runs in full.
///
/// `extra` seeds the owed set with scopes the registry does not hold - the
/// dispatch's fallback scope, which for a deferred inventory trigger is a
/// scope that never finalized and that nobody would otherwise re-establish.
pub(in crate::engine) async fn handle_schema_incompatible(
    ctx: &RecoveryContext<'_>,
    extra: HashMap<CursorScope, crate::cursor::LifecycleEpoch>,
) -> RecoveryFollowUp {
    let Some(activity) = ctx.control.begin_activity() else {
        tracing::debug!(
            target: "bifrost.sync.changes",
            account = ?&ctx.account_id,
            "schema recovery deferred: the account is paused; the reset is parked and \
             re-runs after resume"
        );
        return RecoveryFollowUp::ParkSchemaReset(extra);
    };
    let admitted = ctx.control.admitted_by(&activity);
    ctx.coverage.raise_account_fence();
    let mut owed = extra;
    for scope in ctx.cursors.all_scopes() {
        let epoch = ctx.cursors.lifecycle_epoch(&scope);
        let entry = owed.entry(scope.clone()).or_insert(epoch);
        *entry = (*entry).max(epoch);
        ctx.cursors.delete(&scope);
    }
    let mut backoff = crate::backoff::ReconnectBackoff::new(REOPEN_BACKOFF);
    let mut last = None;
    for attempt in 0..REOPEN_RETRY_BUDGET.0 {
        if attempt > 0 {
            let sleep_for = backoff.record_failure(crate::backoff::jitter_seed());
            if !sleep_unless_shutdown(sleep_for, &ctx.shutdown).await {
                return RecoveryFollowUp::None;
            }
        }
        match ctx.writer.reset_account_for_schema_recovery().await {
            Ok(()) => {
                ctx.deferred_schema_reset.reset_completed();
                for (scope, epoch) in owed {
                    // Fence BEFORE the epoch, for the reason `establish_if_absent`
                    // gives: a deletion bumps the epoch before it raises the
                    // fence, so one passing the epoch check raised its fence
                    // after this read and the establishment's installs refuse.
                    let walk_fence = ctx.coverage.scope_fence(&scope);
                    if ctx.cursors.lifecycle_epoch(&scope) != epoch
                        || ctx.cursors.snapshot(&scope).is_some()
                    {
                        continue;
                    }
                    re_establish_scope_after_schema_reset(ctx, scope, &admitted, walk_fence).await;
                }
                return RecoveryFollowUp::None;
            }
            Err(error) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?&ctx.account_id,
                    attempt,
                    error = %error,
                    "SchemaIncompatible: account-wide checkpoint clear failed"
                );
                last = Some(error);
            }
        }
    }
    let detail = last.map_or_else(String::new, |error| error.to_string());
    broadcast_warning(
        &ctx.delivery,
        None,
        bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::Engine(bifrost_types::EngineNotice::SchemaRecoveryFailed),
            "schema recovery could not clear durable checkpoints",
        )
        .with_protocol_detail(DiagnosticText::support_only(detail)),
    );
    drop(activity);
    engine_pause(ctx, PauseReason::RetryBudgetExhausted);
    RecoveryFollowUp::ParkSchemaReset(owed)
}
