//! Shared guards, error context, and recovery policy for JMAP state walks.

use std::collections::HashSet;

use bifrost_types::{
    AccountError, AccountErrorKind, AccountOperation, CursorScope, SyncStateErrorKind,
};

use super::error::JmapErrorContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateWalkFault {
    Unmoved,
    Cycle,
    RepeatedAnchor,
}

impl StateWalkFault {
    pub(crate) fn describe(self, method: &str) -> String {
        match self {
            Self::Unmoved => {
                format!("{method} reported hasMoreChanges with an unmoved state")
            }
            Self::Cycle => {
                format!("{method} returned to a state this walk had already served")
            }
            Self::RepeatedAnchor => {
                format!("{method} re-served the anchor id despite anchorOffset: 1")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeRecoveryPolicy {
    EngineRestart,
    SnapshotReseed,
}

enum WalkProgress {
    Changes { seen_states: HashSet<String> },
    Inventory { anchor: Option<String> },
}

/// One policy object for paged JMAP walks.
///
/// Changes and lifecycle walks send a prior server state and share the same
/// forward-progress guard. Inventory walks do not send a prior state: query
/// state churn is allowed, while replaying the prior anchor is not. Each
/// variant supplies the matching error context and keeps its recovery rule
/// beside that progress guard.
pub(crate) struct StateWalk {
    scope: CursorScope,
    progress: WalkProgress,
    recovery: ScopeRecoveryPolicy,
}

impl StateWalk {
    pub(crate) fn changes(scope: CursorScope, since_state: &str) -> Self {
        Self::state_changes(scope, since_state, ScopeRecoveryPolicy::EngineRestart)
    }

    pub(crate) fn lifecycle(scope: CursorScope, since_state: &str) -> Self {
        Self::state_changes(scope, since_state, ScopeRecoveryPolicy::SnapshotReseed)
    }

    fn state_changes(scope: CursorScope, since_state: &str, recovery: ScopeRecoveryPolicy) -> Self {
        Self {
            scope,
            progress: WalkProgress::Changes {
                seen_states: std::iter::once(since_state.to_string()).collect(),
            },
            recovery,
        }
    }

    pub(crate) fn inventory(scope: CursorScope) -> Self {
        Self {
            scope,
            progress: WalkProgress::Inventory { anchor: None },
            recovery: ScopeRecoveryPolicy::EngineRestart,
        }
    }

    pub(crate) fn error_context(&self, operation: AccountOperation) -> JmapErrorContext {
        if matches!(&self.progress, WalkProgress::Changes { .. }) {
            JmapErrorContext::state_cursor(operation, self.scope.clone())
        } else {
            JmapErrorContext::cursor(operation, self.scope.clone())
        }
    }

    /// The query door knows whether this page carries an anchor. A scoped
    /// inventory walk by itself is not enough to classify `anchorNotFound`:
    /// its `/get` call has the same cursor scope but cannot send an anchor.
    pub(crate) fn query_error_context(&self, operation: AccountOperation) -> JmapErrorContext {
        let context = self.error_context(operation);
        if matches!(&self.progress, WalkProgress::Inventory { anchor: Some(_) }) {
            context.with_anchored_query()
        } else {
            context
        }
    }

    pub(crate) fn recovery_policy(&self) -> ScopeRecoveryPolicy {
        self.recovery
    }

    pub(crate) fn should_snapshot_reseed(&self, error: &AccountError) -> bool {
        self.recovery == ScopeRecoveryPolicy::SnapshotReseed
            && matches!(
                error.kind(),
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
            )
    }

    pub(crate) fn observe_changes_page(
        &mut self,
        since_state: &str,
        new_state: &str,
        has_more_changes: bool,
    ) -> Option<StateWalkFault> {
        let WalkProgress::Changes { seen_states } = &mut self.progress else {
            unreachable!("inventory walk cannot observe a changes page");
        };

        if !has_more_changes {
            return None;
        }
        if new_state == since_state {
            return Some(StateWalkFault::Unmoved);
        }
        if !seen_states.insert(new_state.to_string()) {
            return Some(StateWalkFault::Cycle);
        }
        None
    }

    pub(crate) fn reset_changes_burst(&mut self, since_state: &str) {
        let WalkProgress::Changes { seen_states } = &mut self.progress else {
            unreachable!("inventory walk has no changes burst");
        };
        seen_states.clear();
        seen_states.insert(since_state.to_string());
    }

    pub(crate) fn inventory_anchor(&self) -> Option<&str> {
        match &self.progress {
            WalkProgress::Inventory { anchor } => anchor.as_deref(),
            WalkProgress::Changes { .. } => {
                unreachable!("changes walk has no inventory anchor")
            }
        }
    }

    pub(crate) fn observe_inventory_page<T: ToString>(
        &mut self,
        ids: &[T],
    ) -> Option<StateWalkFault> {
        let WalkProgress::Inventory { anchor } = &mut self.progress else {
            unreachable!("changes walk cannot observe an inventory page");
        };

        if ids.is_empty() {
            return None;
        }
        if anchor
            .as_ref()
            .is_some_and(|previous| ids.iter().any(|id| id.to_string() == *previous))
        {
            return Some(StateWalkFault::RepeatedAnchor);
        }
        *anchor = ids.last().map(ToString::to_string);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{ScopeRecoveryPolicy, StateWalk, StateWalkFault};
    use bifrost_types::{AccountOperation, CursorScope, ObjectType};

    #[test]
    fn state_walk_policy_pairs_guards_with_recovery_and_error_context() {
        let scope = CursorScope::Type(ObjectType::Email);
        let changes = StateWalk::changes(scope.clone(), "s0");
        let lifecycle = StateWalk::lifecycle(scope.clone(), "s0");
        let inventory = StateWalk::inventory(scope);

        assert_eq!(
            changes.recovery_policy(),
            ScopeRecoveryPolicy::EngineRestart
        );
        assert_eq!(
            lifecycle.recovery_policy(),
            ScopeRecoveryPolicy::SnapshotReseed
        );
        assert_eq!(
            inventory.recovery_policy(),
            ScopeRecoveryPolicy::EngineRestart
        );
        assert!(
            changes
                .error_context(AccountOperation::SyncChanges)
                .state_based
        );
        assert!(
            lifecycle
                .error_context(AccountOperation::ScopeLifecycle)
                .state_based
        );
        assert!(
            !inventory
                .error_context(AccountOperation::SyncInventory)
                .state_based
        );
        assert!(
            !inventory
                .query_error_context(AccountOperation::SyncInventory)
                .anchored_query
        );
    }

    #[test]
    fn state_walk_owns_change_progress_and_inventory_anchor_guards() {
        let scope = CursorScope::Type(ObjectType::Email);
        let mut changes = StateWalk::changes(scope.clone(), "s0");
        assert_eq!(
            changes.observe_changes_page("s0", "s0", true),
            Some(StateWalkFault::Unmoved)
        );

        let mut changes = StateWalk::changes(scope.clone(), "s0");
        assert_eq!(changes.observe_changes_page("s0", "s1", true), None);
        assert_eq!(
            changes.observe_changes_page("s1", "s0", true),
            Some(StateWalkFault::Cycle)
        );
        assert_eq!(changes.observe_changes_page("s0", "s0", false), None);

        let mut lifecycle = StateWalk::lifecycle(scope.clone(), "s0");
        assert_eq!(lifecycle.observe_changes_page("s0", "s1", true), None);
        lifecycle.reset_changes_burst("s1");
        assert_eq!(lifecycle.observe_changes_page("s1", "s0", true), None);

        let mut inventory = StateWalk::inventory(scope);
        assert_eq!(inventory.inventory_anchor(), None);
        assert_eq!(
            inventory.observe_inventory_page(&["e1".to_string(), "e2".to_string()]),
            None
        );
        assert_eq!(inventory.inventory_anchor(), Some("e2"));
        assert!(
            inventory
                .query_error_context(AccountOperation::SyncInventory)
                .anchored_query
        );
        assert!(
            !inventory
                .error_context(AccountOperation::SyncInventory)
                .anchored_query,
            "a get call keeps the cursor scope without inheriting the query anchor"
        );
        assert_eq!(
            inventory.observe_inventory_page(&["e2".to_string()]),
            Some(StateWalkFault::RepeatedAnchor)
        );
    }
}
