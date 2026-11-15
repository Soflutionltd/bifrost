//! Recovery context and shared dispatch effects.

use super::*;

mod dispatch;
pub(super) mod pacing;
mod parked;
mod schema;
mod scoped;

#[cfg(test)]
pub(super) use schema::handle_schema_incompatible;
#[cfg(test)]
pub(super) use scoped::re_establish_scope_with_backoff;

pub(crate) use dispatch::handle_account_error;
pub(super) use dispatch::stale_generation;
pub use parked::ResetCount;
pub(crate) use parked::{DeferredSchemaReset, DeferredScopeRepairs};
pub(crate) use schema::run_parked_schema_reset;
pub(crate) use scoped::establish_if_absent;

/// Shared context bundle used by every recovery-dispatch path. Folds
/// the per-account state most paths need into a single argument so
/// `handle_account_error`, the engine-directive arm, and the reopen
/// loop don't carry seven-positional argument lists.
pub(crate) struct RecoveryContext<'a> {
    pub scheduler: &'a Scheduler,
    pub deferred_schema_reset: &'a Arc<DeferredSchemaReset>,
    pub bandwidth_budget: Option<&'a bifrost_net::BandwidthBudget>,
    pub factory: &'a Arc<dyn AccountFactory>,
    pub current: &'a Arc<ArcSwap<crate::types::Incarnation>>,
    pub cursors: &'a Arc<CursorRegistry>,
    pub account_id: &'a AccountId,
    pub control: &'a SyncControl,
    pub account_control_tx: &'a broadcast::Sender<AccountControl>,
    pub throttles: &'a Arc<crate::throttle::ThrottleBucket>,
    pub capabilities: &'a Arc<std::sync::RwLock<AccountCapabilities>>,
    pub subscriptions: &'a Arc<SubscriptionRegistry>,
    pub account_generation_tx: &'a watch::Sender<crate::types::AccountGeneration>,
    /// The lane supervisor's retirement watch (drained generation plus
    /// one). Reattach waits on it before closing the account it replaced.
    pub push_retired: &'a watch::Sender<crate::types::AccountGeneration>,
    /// Bound on that wait (`EngineConfig::detach_timeout`).
    pub push_drain_wait: Duration,
    /// The incarnation generation a stamped request (`PushRecovery`, or a
    /// `Recovery` carrying one) was raised against; `None` for an unstamped
    /// request, which acts on whichever incarnation is current.
    pub expected_generation: Option<crate::types::AccountGeneration>,
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

/// Recovery work a dispatch could not finish and that nothing else will raise
/// again, handed back for the caller to park.
pub(crate) enum RecoveryFollowUp {
    /// Nothing is owed.
    None,
    /// A cursorless scope repair declined under a pause, with the scope's
    /// lifecycle epoch read at decline time.
    ParkScope {
        scope: CursorScope,
        epoch: crate::cursor::LifecycleEpoch,
    },
    /// An account reset declined or exhausted; scopes it owes re-establishment
    /// (scope -> lifecycle epoch).
    ParkSchemaReset(HashMap<CursorScope, crate::cursor::LifecycleEpoch>),
    /// Several cursorless establishments declined under a pause: what a parked
    /// schema reset, finding a reset already committed since its decline, owes
    /// when a second pause refuses the establishments it runs instead. Each is
    /// parked as its own [`RecoveryFollowUp::ParkScope`]; this is only the
    /// batched form, since one dispatch can decline many.
    ParkScopes(HashMap<CursorScope, crate::cursor::LifecycleEpoch>),
}

/// Engine-driven pause: publish `AccountControl::Pause(reason)` on the
/// account's control broadcast and trip the boundary so per-scope
/// workers park. Consumers flip `AccountControl::Resume` (engine-side
/// helper on `SyncEngine`) to unpause.
pub(in crate::engine) fn engine_pause(ctx: &RecoveryContext<'_>, reason: PauseReason) {
    ctx.control
        .bind_control_events(ctx.account_control_tx.clone());
    ctx.control.engine_pause(reason);
}

/// Emit one structured `warn!` per open-time skip. The durable record
/// is the slot's `open_skips` lane (read via
/// `SyncEngine::open_skipped_scopes`); the log line exists so a
/// degraded shared namespace is visible in telemetry even when the
/// consumer never queries the lane.
pub(in crate::engine) fn log_open_skips(
    account_id: &AccountId,
    skips: &[SkippedScope],
    phase: &'static str,
) {
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

pub(in crate::engine) fn broadcast_terminated(
    delivery: &ChangeDelivery,
    scope: EventScope,
    error: AccountError,
) {
    delivery.publish_terminated(scope, error);
}

/// Broadcast a `Warning` on the per-account changes channel. The
/// `scope` argument is the directive's target scope when available;
/// the caller passes `None` only for warnings that are genuinely
/// account-wide. Attachment failure must not look like termination of a real
/// account cursor: an enterprise user needs to distinguish a stopped account
/// from one cursor's failure.
pub(in crate::engine) fn broadcast_warning(
    delivery: &ChangeDelivery,
    scope: Option<CursorScope>,
    warning: bifrost_types::Warning,
) {
    delivery.publish_warning(
        scope.map_or(EventScope::Account, EventScope::Cursor),
        warning,
    );
}
