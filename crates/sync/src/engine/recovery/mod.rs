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

/// One recovery dispatch borrows the account slot and its writer endpoint.
/// Only the request's incarnation stamp varies between dispatches.
pub(crate) struct RecoveryContext<'a> {
    pub slot: &'a AccountSlot,
    pub writer: &'a WriterHandle,
    pub expected_generation: Option<crate::types::AccountGeneration>,
}

impl std::ops::Deref for RecoveryContext<'_> {
    type Target = AccountSlot;
    fn deref(&self) -> &Self::Target {
        self.slot
    }
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
