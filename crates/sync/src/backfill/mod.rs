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
pub mod partitioner;
pub mod runner;
pub mod scope_walk;

use bifrost_types::{AccountId, CursorScope};

pub use checkpoint::{BackfillCheckpointTarget, BackfillCheckpointWriter};
pub use partitioner::{BackfillPolicy, BackfillStrategy, PartitionPlan, default_time_boundaries};
pub use runner::{
    BackfillPartitionOutcome, BackfillRunner, BackfillScopeWalk, LiveSupersedes, ScopeWalkStep,
};
pub use scope_walk::ScopeWalkDriver;

/// Backfill registry: tracks which account scopes are pending, running,
/// or complete. Durable resume state lives in the checkpoint store.
#[derive(Debug, Default)]
pub struct BackfillRegistry {
    inner: dashmap::DashMap<(AccountId, CursorScope), BackfillState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BackfillState {
    Pending,
    Running,
    Completed,
}

impl BackfillRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark(&self, account: AccountId, scope: CursorScope, state: BackfillState) {
        self.inner.insert((account, scope), state);
    }

    #[must_use]
    pub fn snapshot(&self, account: &AccountId, scope: &CursorScope) -> Option<BackfillState> {
        self.inner
            .get(&(account.clone(), scope.clone()))
            .map(|r| *r.value())
    }

    /// Whether any scope of `account` is still being walked, including one
    /// queued behind another and not yet started.
    #[must_use]
    pub fn has_running(&self, account: &AccountId) -> bool {
        self.inner
            .iter()
            .any(|entry| entry.key().0 == *account && *entry.value() == BackfillState::Running)
    }

    pub fn forget_account(&self, account: &AccountId) {
        self.inner.retain(|(candidate, _), _| candidate != account);
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

        registry.mark(first.clone(), scope.clone(), BackfillState::Completed);
        registry.mark(second.clone(), scope.clone(), BackfillState::Running);

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

    #[test]
    fn only_a_running_scope_of_the_account_counts_as_running() {
        let registry = BackfillRegistry::new();
        let account = AccountId("account".into());
        let other = AccountId("other".into());
        registry.mark(
            account.clone(),
            CursorScope::Account,
            BackfillState::Pending,
        );
        registry.mark(other.clone(), CursorScope::Account, BackfillState::Running);
        assert!(!registry.has_running(&account));

        registry.mark(
            account.clone(),
            CursorScope::Account,
            BackfillState::Running,
        );
        assert!(registry.has_running(&account));

        registry.mark(
            account.clone(),
            CursorScope::Account,
            BackfillState::Completed,
        );
        assert!(!registry.has_running(&account));
    }
}
