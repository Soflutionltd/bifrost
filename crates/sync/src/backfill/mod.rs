//! Backfill orchestrator.
//!
//! Backfill is the cold-start hydration pass. The partitioner plans
//! newest-first partitions (so a user sees recent mail before deep
//! history), and the runner emits a consumer-ack-deferred checkpoint
//! at every partition boundary.
//!
//! `BackfillPolicy` is configurable per account; the default is
//! `TimeWindowed` with exponentially-widening windows per
//! `bifrost-sync.md` -> Default partition policy.

pub mod checkpoint;
pub(crate) mod marker;
pub mod partitioner;
pub(crate) mod plan;
pub mod runner;
pub(crate) mod scan;
pub mod scope_walk;

use bifrost_types::{AccountId, CursorScope};

pub use checkpoint::{BackfillCheckpointTarget, BackfillCheckpointWriter};
pub use partitioner::{BackfillPolicy, BackfillStrategy, PartitionPlan, default_time_boundaries};
pub use runner::{
    BackfillPartitionOutcome, BackfillRunner, LiveSupersedes, PartitionVerdict, ScopeWalkStep,
};
pub use scope_walk::ScopeWalkDriver;

/// Read-only projection of each attached account's live backfill scan.
/// Durable resume state lives in the checkpoint store.
///
/// Consumer writes (`mark`, `forget_account`) were removed deliberately: a
/// consumer write raced the engine's own writes to the same state. Consumers
/// read this projection through `SyncEngine::backfill_registry()`.
#[derive(Default)]
pub struct BackfillRegistry {
    inner: dashmap::DashMap<AccountId, std::sync::Arc<Projection>>,
}

type Projection = dyn Fn(&CursorScope, Option<crate::cursor::ScopeIncarnation>) -> Option<BackfillState>
    + Send
    + Sync;

impl std::fmt::Debug for BackfillRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackfillRegistry")
            .field("accounts", &self.inner.len())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BackfillState {
    /// This incarnation has not started its first attempt.
    Pending,
    /// A failed attempt is waiting on its retry ramp.
    Retrying,
    /// A partition is parked until a scope-local throttle hold expires.
    Throttled,
    /// An operator decision is needed to cross a checkpoint barrier.
    OperatorBlocked,
    /// A walk lost pages and must restart from its beginning.
    RestartingAfterLoss,
    Running,
    Completed,
}

impl BackfillRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn observe(&self, account: AccountId, projection: std::sync::Arc<Projection>) {
        self.inner.insert(account, projection);
    }

    /// Status of the scope's current incarnation. A replacement starts Pending;
    /// the previous incarnation's completion is never inherited.
    #[must_use]
    pub fn snapshot(&self, account: &AccountId, scope: &CursorScope) -> Option<BackfillState> {
        let projection = std::sync::Arc::clone(self.inner.get(account)?.value());
        projection(scope, None)
    }

    /// Status only if this incarnation is still live. The identity comes from
    /// `CursorRegistry::all_scope_incarnations`.
    #[must_use]
    pub fn snapshot_incarnation(
        &self,
        account: &AccountId,
        scope: &CursorScope,
        incarnation: crate::cursor::ScopeIncarnation,
    ) -> Option<BackfillState> {
        let projection = std::sync::Arc::clone(self.inner.get(account)?.value());
        projection(scope, Some(incarnation))
    }

    pub(crate) fn forget_account(&self, account: &AccountId) {
        self.inner.remove(account);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_isolates_identical_scopes_by_account() {
        let registry = BackfillRegistry::new();
        let first = AccountId("first".into());
        let second = AccountId("second".into());
        let scope = CursorScope::Account;

        registry.observe(
            first.clone(),
            std::sync::Arc::new(|_, _| Some(BackfillState::Completed)),
        );
        registry.observe(
            second.clone(),
            std::sync::Arc::new(|_, _| Some(BackfillState::Running)),
        );

        assert_eq!(
            registry.snapshot(&first, &scope),
            Some(BackfillState::Completed)
        );
        assert_eq!(
            registry.snapshot(&second, &scope),
            Some(BackfillState::Running)
        );
        registry.forget_account(&first);
        assert_eq!(registry.snapshot(&first, &scope), None);
        assert_eq!(
            registry.snapshot(&second, &scope),
            Some(BackfillState::Running)
        );
    }
}
