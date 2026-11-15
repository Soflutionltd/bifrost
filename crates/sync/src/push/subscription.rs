//! Subscription handles owned by one account attachment.

use bifrost_types::{CursorScope, SubscriptionHandle};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub(crate) struct RegisteredSubscription {
    pub handle: SubscriptionHandle,
    pub scopes: Vec<CursorScope>,
    pub state: SubscriptionState,
}

/// Server handle lifetime and consumer desire are one state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriptionState {
    Live,
    LiveTeardownFailed,
    TornDownAwaitingRecreate,
    Orphan,
}

impl RegisteredSubscription {
    pub(crate) fn desired(&self) -> bool {
        self.state != SubscriptionState::Orphan
    }

    pub(crate) fn torn_down(&self) -> bool {
        self.state == SubscriptionState::TornDownAwaitingRecreate
    }

    pub(crate) fn teardown_unconfirmed(&self) -> bool {
        matches!(
            self.state,
            SubscriptionState::LiveTeardownFailed | SubscriptionState::Orphan
        )
    }

    #[must_use]
    pub(crate) fn into_orphan(self) -> Self {
        Self {
            state: SubscriptionState::Orphan,
            ..self
        }
    }
}

/// Subscription handles for one attachment. A later attachment always gets a
/// fresh registry, even when it uses the same account id.
#[derive(Debug, Default)]
pub struct SubscriptionRegistry {
    inner: Mutex<Vec<RegisteredSubscription>>,
    shutdown: CancellationToken,
}

impl SubscriptionRegistry {
    #[must_use]
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            inner: Mutex::new(Vec::new()),
            shutdown,
        }
    }

    /// Refuse a late transfer so its caller can tear down the server handle.
    /// Slot ownership prevents cross-attachment reuse; this check additionally
    /// prevents accepting an obligation after detach's final drain.
    #[must_use]
    pub fn record(&self, handle: SubscriptionHandle, scopes: Vec<CursorScope>) -> bool {
        let mut records = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.shutdown.is_cancelled() {
            return false;
        }
        records.push(RegisteredSubscription {
            handle,
            scopes,
            state: SubscriptionState::Live,
        });
        true
    }

    #[must_use]
    pub(crate) fn take(&self) -> Vec<RegisteredSubscription> {
        let mut records = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *records)
            .into_iter()
            .filter(|record| !record.torn_down())
            .collect()
    }

    pub(crate) fn restore_orphans(&self, restored: Vec<RegisteredSubscription>) -> bool {
        if restored.is_empty() {
            return true;
        }
        let mut records = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.shutdown.is_cancelled() {
            return false;
        }
        records.extend(restored);
        true
    }

    pub(crate) fn mark_unconfirmed(&self, handle: &SubscriptionHandle) {
        let mut records = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for record in records.iter_mut() {
            if &record.handle == handle && record.state == SubscriptionState::Live {
                record.state = SubscriptionState::LiveTeardownFailed;
            }
        }
    }

    pub(crate) fn mark_torn_down(&self, handle: &SubscriptionHandle) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain_mut(|record| {
                if &record.handle != handle {
                    return true;
                }
                if record.desired() {
                    record.state = SubscriptionState::TornDownAwaitingRecreate;
                    true
                } else {
                    false
                }
            });
    }

    #[must_use]
    pub(crate) fn orphans(&self) -> Vec<RegisteredSubscription> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|record| !record.desired() && record.teardown_unconfirmed())
            .cloned()
            .collect()
    }

    pub(crate) fn remove_orphan(&self, handle: &SubscriptionHandle) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|record| record.desired() || &record.handle != handle);
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> Vec<RegisteredSubscription> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn replace(
        &self,
        replacement: Vec<RegisteredSubscription>,
    ) -> Result<(), Vec<RegisteredSubscription>> {
        let mut records = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !replacement.is_empty() && self.shutdown.is_cancelled() {
            return Err(replacement);
        }
        *records = replacement;
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orphan(handle: &str) -> RegisteredSubscription {
        RegisteredSubscription {
            handle: SubscriptionHandle(handle.to_owned()),
            scopes: Vec::new(),
            state: SubscriptionState::Live,
        }
        .into_orphan()
    }

    #[test]
    fn a_late_teardown_failure_does_not_resurrect_a_dead_handle() {
        let registry = SubscriptionRegistry::new(CancellationToken::new());
        let handle = SubscriptionHandle("handle".into());
        assert!(registry.record(handle.clone(), vec![]));
        registry.mark_torn_down(&handle);
        registry.mark_unconfirmed(&handle);
        let records = registry.snapshot();
        assert!(records[0].desired());
        assert!(records[0].torn_down());
        assert!(!records[0].teardown_unconfirmed());
        assert!(registry.take().is_empty());
    }

    /// A live slot's orphans are merged into what is registered.
    #[test]
    fn a_live_slot_keeps_its_restored_orphans() {
        let registry = SubscriptionRegistry::new(CancellationToken::new());
        assert!(registry.record(SubscriptionHandle("wanted".to_owned()), vec![]));
        assert!(registry.restore_orphans(vec![orphan("refused")]));
        assert_eq!(registry.len(), 2);
    }

    /// Once the slot's token is cancelled, an orphan is refused: detach cancels
    /// before it takes, so a write ordered after the take must see the
    /// cancellation and leave nothing behind for a later attach to inherit.
    #[test]
    fn a_cancelled_slot_refuses_orphans_and_leaves_no_entry() {
        let shutdown = CancellationToken::new();
        let registry = SubscriptionRegistry::new(shutdown.clone());
        shutdown.cancel();
        // Detach's order: cancel, then take.
        assert!(registry.take().is_empty());
        assert!(!registry.restore_orphans(vec![orphan("late")]));
        assert_eq!(registry.len(), 0);
        assert!(
            registry.take().is_empty(),
            "a later attach of the same id inherits nothing"
        );
    }

    /// A registration made after detach's take is refused, and leaves no entry.
    #[test]
    fn a_cancelled_slot_refuses_a_new_record_and_leaves_no_entry() {
        let shutdown = CancellationToken::new();
        let registry = SubscriptionRegistry::new(shutdown.clone());
        shutdown.cancel();
        assert!(registry.take().is_empty());
        assert!(!registry.record(SubscriptionHandle("late".to_owned()), vec![]));
        assert_eq!(registry.len(), 0);
    }

    /// A reattach commit landing after detach's take is refused, hands its
    /// records back, and leaves nothing behind; an empty set is never refused.
    #[test]
    fn a_cancelled_slot_refuses_a_replacement_and_returns_the_records() {
        let shutdown = CancellationToken::new();
        let registry = SubscriptionRegistry::new(shutdown.clone());
        shutdown.cancel();
        assert!(registry.take().is_empty());
        let refused = registry
            .replace(vec![orphan("late")])
            .expect_err("a cancelled slot refuses the install");
        assert_eq!(refused.len(), 1);
        assert_eq!(registry.len(), 0);
        assert!(registry.replace(Vec::new()).is_ok());
    }

    /// A live slot's replacement swaps the whole entry.
    #[test]
    fn a_live_slot_installs_a_replacement() {
        let shutdown = CancellationToken::new();
        let registry = SubscriptionRegistry::new(shutdown.clone());
        assert!(registry.record(SubscriptionHandle("old".to_owned()), vec![]));
        assert!(registry.replace(vec![orphan("new")]).is_ok());
        let held = registry.snapshot();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].handle, SubscriptionHandle("new".to_owned()));
    }
}
