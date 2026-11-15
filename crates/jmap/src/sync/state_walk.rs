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

/// State-bearing changes walk, including lifecycle polling bursts.
pub(crate) struct ChangesWalk {
    scope: CursorScope,
    seen_states: HashSet<String>,
    recovery: ScopeRecoveryPolicy,
}

/// Stateless query walk. Only query requests can carry its anchor context.
pub(crate) struct InventoryWalk {
    scope: CursorScope,
    anchor: Option<String>,
}

impl ChangesWalk {
    pub(crate) fn changes(scope: CursorScope, since_state: &str) -> Self {
        Self::state_changes(scope, since_state, ScopeRecoveryPolicy::EngineRestart)
    }

    pub(crate) fn lifecycle(scope: CursorScope, since_state: &str) -> Self {
        Self::state_changes(scope, since_state, ScopeRecoveryPolicy::SnapshotReseed)
    }

    fn state_changes(scope: CursorScope, since_state: &str, recovery: ScopeRecoveryPolicy) -> Self {
        Self {
            scope,
            seen_states: std::iter::once(since_state.to_string()).collect(),
            recovery,
        }
    }

    pub(crate) fn error_context(&self, operation: AccountOperation) -> JmapErrorContext {
        JmapErrorContext::state_cursor(operation, self.scope.clone())
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
        if !has_more_changes {
            return None;
        }
        if new_state == since_state {
            return Some(StateWalkFault::Unmoved);
        }
        if !self.seen_states.insert(new_state.to_string()) {
            return Some(StateWalkFault::Cycle);
        }
        None
    }

    pub(crate) fn reset_changes_burst(&mut self, since_state: &str) {
        self.seen_states.clear();
        self.seen_states.insert(since_state.to_string());
    }
}

impl InventoryWalk {
    pub(crate) fn inventory(scope: CursorScope) -> Self {
        Self {
            scope,
            anchor: None,
        }
    }

    pub(crate) fn error_context(&self, operation: AccountOperation) -> JmapErrorContext {
        JmapErrorContext::cursor(operation, self.scope.clone())
    }

    pub(crate) fn query_error_context(&self, operation: AccountOperation) -> JmapErrorContext {
        let context = self.error_context(operation);
        if self.anchor.is_some() {
            JmapErrorContext::anchored_cursor(operation, self.scope.clone())
        } else {
            context
        }
    }

    pub(crate) fn inventory_anchor(&self) -> Option<&str> {
        self.anchor.as_deref()
    }

    pub(crate) fn observe_inventory_page<T: ToString>(
        &mut self,
        ids: &[T],
    ) -> Option<StateWalkFault> {
        if ids.is_empty() {
            return None;
        }
        if self
            .anchor
            .as_ref()
            .is_some_and(|previous| ids.iter().any(|id| id.to_string() == *previous))
        {
            return Some(StateWalkFault::RepeatedAnchor);
        }
        self.anchor = ids.last().map(ToString::to_string);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{ChangesWalk, InventoryWalk, ScopeRecoveryPolicy, StateWalkFault};
    use bifrost_types::{AccountOperation, CursorScope, ObjectType};

    #[test]
    fn changes_and_inventory_have_distinct_mode_types() {
        // Aliasing both modes back to one policy type must not compile.
        trait WalkMode {}
        impl WalkMode for ChangesWalk {}
        impl WalkMode for InventoryWalk {}
        fn assert_mode<T: WalkMode>() {}
        assert_mode::<ChangesWalk>();
        assert_mode::<InventoryWalk>();
    }

    #[test]
    fn state_walk_policy_pairs_guards_with_recovery_and_error_context() {
        let scope = CursorScope::Type(ObjectType::Email);
        let changes = ChangesWalk::changes(scope.clone(), "s0");
        let lifecycle = ChangesWalk::lifecycle(scope.clone(), "s0");
        let inventory = InventoryWalk::inventory(scope);

        assert_eq!(
            changes.recovery_policy(),
            ScopeRecoveryPolicy::EngineRestart
        );
        assert_eq!(
            lifecycle.recovery_policy(),
            ScopeRecoveryPolicy::SnapshotReseed
        );
        assert!(
            changes
                .error_context(AccountOperation::SyncChanges)
                .is_state_read()
        );
        assert!(
            lifecycle
                .error_context(AccountOperation::ScopeLifecycle)
                .is_state_read()
        );
        assert!(
            !inventory
                .error_context(AccountOperation::SyncInventory)
                .is_state_read()
        );
        assert!(
            !inventory
                .query_error_context(AccountOperation::SyncInventory)
                .is_anchored_query()
        );
    }

    #[test]
    fn state_walk_owns_change_progress_and_inventory_anchor_guards() {
        let scope = CursorScope::Type(ObjectType::Email);
        let mut changes = ChangesWalk::changes(scope.clone(), "s0");
        assert_eq!(
            changes.observe_changes_page("s0", "s0", true),
            Some(StateWalkFault::Unmoved)
        );

        let mut changes = ChangesWalk::changes(scope.clone(), "s0");
        assert_eq!(changes.observe_changes_page("s0", "s1", true), None);
        assert_eq!(
            changes.observe_changes_page("s1", "s0", true),
            Some(StateWalkFault::Cycle)
        );
        assert_eq!(changes.observe_changes_page("s0", "s0", false), None);

        let mut lifecycle = ChangesWalk::lifecycle(scope.clone(), "s0");
        assert_eq!(lifecycle.observe_changes_page("s0", "s1", true), None);
        lifecycle.reset_changes_burst("s1");
        assert_eq!(lifecycle.observe_changes_page("s1", "s0", true), None);

        let mut inventory = InventoryWalk::inventory(scope);
        assert_eq!(inventory.inventory_anchor(), None);
        assert_eq!(
            inventory.observe_inventory_page(&["e1".to_string(), "e2".to_string()]),
            None
        );
        assert_eq!(inventory.inventory_anchor(), Some("e2"));
        assert!(
            inventory
                .query_error_context(AccountOperation::SyncInventory)
                .is_anchored_query()
        );
        assert!(
            !inventory
                .error_context(AccountOperation::SyncInventory)
                .is_anchored_query(),
            "a get call keeps the cursor scope without inheriting the query anchor"
        );
        assert_eq!(
            inventory.observe_inventory_page(&["e2".to_string()]),
            Some(StateWalkFault::RepeatedAnchor)
        );
    }
}
