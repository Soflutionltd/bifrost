//! Bounded, lossless invalidation intent, independent of any runtime.
use bifrost_types::{CursorScope, HintPayload, MembershipScope};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Pending invalidation intent moved out of a [`PushIngress`] by `take`.
#[derive(Debug, Default, Clone)]
pub struct PendingPush {
    pub full: bool,
    pub scopes: HashSet<CursorScope>,
    pub memberships: HashSet<MembershipScope>,
    pub closed: bool,
}

/// One account's pending push invalidations: distinct hinted scopes and
/// memberships up to a capacity, past which they collapse into one full
/// reconcile. Holds only invalidation intent; every method is synchronous
/// and safe from any thread, on or off a Tokio runtime.
#[derive(Debug)]
pub struct PushIngress {
    state: Mutex<PendingPush>,
    wake: tokio::sync::Notify,
    capacity: usize,
    coarsened: Arc<AtomicU64>,
}

impl PushIngress {
    /// `capacity` 0 is floored to 1. Owns a fresh coarsening counter.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self::with_counter(capacity, Arc::new(AtomicU64::new(0)))
    }
    pub(crate) fn with_counter(capacity: usize, coarsened: Arc<AtomicU64>) -> Self {
        Self {
            state: Mutex::new(PendingPush::default()),
            wake: tokio::sync::Notify::new(),
            capacity: capacity.max(1),
            coarsened,
        }
    }
    /// Record one hint. `Unknown` and any future payload request a full
    /// reconcile; exceeding the capacity collapses to one and counts one
    /// coarsening. A no-op once closed.
    pub fn invalidate(&self, payload: &HintPayload) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return;
        }
        if !state.full {
            match payload {
                HintPayload::SpecificCursorScope(scope) => {
                    state.scopes.insert(scope.clone());
                }
                HintPayload::SpecificMembership(scope) => {
                    state.memberships.insert(scope.clone());
                }
                _ => {
                    state.full = true;
                }
            }
            if !state.full && state.scopes.len() + state.memberships.len() > self.capacity {
                state.full = true;
                self.count_coarsening();
            }
            if state.full {
                state.scopes.clear();
                state.memberships.clear();
            }
        }
        drop(state);
        self.wake.notify_one();
    }
    pub fn request_full(&self) {
        self.invalidate(&HintPayload::Unknown);
    }
    /// Mark closed and wake; later invalidations are ignored.
    pub fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        self.wake.notify_one();
    }
    /// Move the pending state out, leaving an empty state with `closed`
    /// preserved.
    #[must_use]
    pub fn take(&self) -> PendingPush {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let closed = state.closed;
        std::mem::replace(
            &mut *state,
            PendingPush {
                closed,
                ..PendingPush::default()
            },
        )
    }
    /// Wait for a wake. One permit is stored, so a push between the
    /// consumer's `take` and its next wait is never missed.
    pub async fn changed(&self) {
        self.wake.notified().await;
    }
    /// Coarsenings into a full reconcile counted on this ingress's counter
    /// (shared engine-wide when built by the sink).
    #[must_use]
    pub fn coarsened(&self) -> u64 {
        self.coarsened.load(Ordering::Relaxed)
    }
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }
    pub(crate) fn count_coarsening(&self) {
        self.coarsened.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn push_ingress_collapses_to_full_past_capacity() {
        let ingress = PushIngress::new(1);
        ingress.invalidate(&HintPayload::SpecificCursorScope(CursorScope::Account));
        ingress.invalidate(&HintPayload::SpecificMembership(MembershipScope::Folder(
            bifrost_types::FolderId("one".into()),
        )));
        let pending = ingress.take();
        assert!(pending.full);
        assert!(pending.scopes.is_empty() && pending.memberships.is_empty());
        assert_eq!(ingress.coarsened(), 1);
        for _ in 0..100 {
            ingress.request_full();
        }
        assert!(ingress.take().full);
        assert_eq!(ingress.coarsened(), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn push_ingress_wakes_after_a_push_from_a_non_runtime_thread() {
        let ingress = Arc::new(PushIngress::new(1));
        let sender = Arc::clone(&ingress);
        let waiter = ingress.changed();
        std::thread::spawn(move || sender.request_full())
            .join()
            .expect("thread");
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("thread wake");
        assert!(ingress.take().full);
    }
    #[tokio::test(start_paused = true)]
    async fn push_ingress_keeps_a_push_landing_between_take_and_wait() {
        let ingress = PushIngress::new(1);
        assert!(!ingress.take().full);
        ingress.request_full();
        tokio::time::timeout(std::time::Duration::from_secs(1), ingress.changed())
            .await
            .expect("latched wake");
        assert!(ingress.take().full);
    }
    #[tokio::test(start_paused = true)]
    async fn push_ingress_ignores_pushes_after_close() {
        let ingress = PushIngress::new(1);
        ingress.close();
        ingress.request_full();
        tokio::time::timeout(std::time::Duration::from_secs(1), ingress.changed())
            .await
            .expect("latched wake");
        let pending = ingress.take();
        assert!(pending.closed);
        assert!(!pending.full);
        assert!(ingress.take().closed);
    }
}
