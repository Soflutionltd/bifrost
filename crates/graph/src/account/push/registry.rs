//! Webhook state transitions own their locks; callers never coordinate ledgers.
use super::renewal::{self, DueRenewal};
use super::webhook::{
    self, GraphSubscriptionGroup, GraphSubscriptionState, RecreatedSubscriptionIds,
};
use bifrost_types::{CursorScope, SubscriptionHandle};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct WebhookRegistry {
    groups: Arc<RwLock<HashMap<SubscriptionHandle, GraphSubscriptionGroup>>>,
    topology: Arc<watch::Sender<u64>>,
    gaps: Arc<Mutex<VecDeque<CursorScope>>>,
    recreated: RecreatedSubscriptionIds,
}
impl Default for WebhookRegistry {
    fn default() -> Self {
        Self {
            groups: Arc::default(),
            topology: Arc::new(watch::Sender::new(0)),
            gaps: Arc::default(),
            recreated: Arc::default(),
        }
    }
}
impl WebhookRegistry {
    /// Set only during open, before any account view is published. Reopens
    /// share this factory ledger; groups, gaps and topology stay per open.
    pub(crate) fn set_recreated_ids(&mut self, ids: RecreatedSubscriptionIds) {
        self.recreated = ids;
    }
    fn recreated_ids(&self) -> std::sync::MutexGuard<'_, HashMap<SubscriptionHandle, Vec<String>>> {
        self.recreated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn orphan_ids(
        &self,
        handle: &SubscriptionHandle,
        mut ids: Vec<String>,
    ) -> Vec<String> {
        if let Some(recreated) = self.recreated_ids().get(handle) {
            ids.extend(recreated.iter().cloned());
        }
        ids
    }
    pub(super) fn forget_orphan(&self, handle: &SubscriptionHandle) {
        self.recreated_ids().remove(handle);
    }
    #[cfg(test)]
    pub(super) fn recreated_for_test_edit(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<SubscriptionHandle, Vec<String>>> {
        self.recreated_ids()
    }
    pub(super) fn topology(&self) -> watch::Receiver<u64> {
        self.topology.subscribe()
    }
    pub(super) fn changed(&self) {
        self.topology.send_modify(|n| *n = n.wrapping_add(1));
    }
    pub(super) fn pop_gap(&self) -> Option<CursorScope> {
        self.gaps.lock().expect("push gap lock").pop_front()
    }
    pub(super) async fn register(
        &self,
        shutdown: &CancellationToken,
        handle: SubscriptionHandle,
        group: GraphSubscriptionGroup,
    ) -> Result<(), GraphSubscriptionGroup> {
        let mut groups = self.groups.write().await;
        // Close cancels before its snapshot: either it sees this insert, or
        // the closed check refuses it. Never hold the lock over a request.
        if shutdown.is_cancelled() {
            return Err(group);
        }
        groups.insert(handle, group);
        drop(groups);
        self.changed();
        Ok(())
    }
    pub(super) async fn handles(&self) -> Vec<SubscriptionHandle> {
        self.groups.read().await.keys().cloned().collect()
    }
    pub(super) async fn begin_teardown(&self, handle: &SubscriptionHandle) -> Option<Vec<String>> {
        webhook::mark_group_tearing_down(&mut *self.groups.write().await, handle)
    }
    /// Forget a registered server id only after its DELETE succeeded. A failed
    /// DELETE leaves both the row and its recreated identity retryable.
    pub(super) async fn forget_deleted(&self, handle: &SubscriptionHandle, server_id: &str) {
        let mut groups = self.groups.write().await;
        webhook::remove_subscription_from_groups(&mut groups, handle, server_id);
        webhook::forget_recreated_id(&mut self.recreated_ids(), handle, server_id);
    }
    pub(super) async fn owned_ids(&self) -> HashSet<String> {
        self.groups
            .read()
            .await
            .values()
            .flat_map(|g| g.subscriptions.iter().map(|s| s.server_id.clone()))
            .collect()
    }
    pub(super) async fn has_live(&self) -> bool {
        renewal::has_live_graph_subscription_group(&*self.groups.read().await)
    }
    pub(super) async fn has_terminated(&self) -> bool {
        self.groups
            .read()
            .await
            .values()
            .any(|g| !g.tearing_down && g.subscriptions.iter().any(|s| s.terminated))
    }
    pub(super) async fn due(&self) -> Vec<DueRenewal> {
        renewal::due_renewals(&mut *self.groups.write().await)
    }
    pub(super) async fn renewed(
        &self,
        handle: &SubscriptionHandle,
        server_id: &str,
        expiry: String,
    ) {
        let mut groups = self.groups.write().await;
        if let Some(state) = groups.get_mut(handle).and_then(|g| {
            g.subscriptions
                .iter_mut()
                .find(|s| s.server_id == server_id)
        }) {
            state.expires_at = expiry;
        }
    }
    /// Stop renewal after a terminal failure, retaining the row for teardown.
    pub(super) async fn terminate(&self, handle: &SubscriptionHandle, server_id: &str) {
        let mut groups = self.groups.write().await;
        if let Some(state) = groups.get_mut(handle).and_then(|g| {
            g.subscriptions
                .iter_mut()
                .find(|s| s.server_id == server_id)
        }) {
            state.terminated = true;
        }
    }
    pub(super) async fn install_replacement(
        &self,
        shutdown: &CancellationToken,
        handle: &SubscriptionHandle,
        stale_id: &str,
        replacement: GraphSubscriptionState,
    ) -> bool {
        let mut groups = self.groups.write().await;
        let created_id = replacement.server_id.clone();
        let scopes = replacement.scopes.clone();
        // One registration lock covers the row, its gap obligation and its
        // reopened-account teardown identity. Outlook users expect a retired
        // handle to stay retired, and a recovered coverage gap to be checked.
        let installed = !shutdown.is_cancelled()
            && renewal::install_replacement(&mut groups, handle, stale_id, replacement);
        if installed {
            self.gaps.lock().expect("push gap lock").extend(scopes);
            renewal::record_recreated_id(&mut self.recreated_ids(), handle, stale_id, &created_id);
        }
        installed
    }
    #[cfg(test)]
    pub(crate) fn groups_for_tests(
        &self,
    ) -> &RwLock<HashMap<SubscriptionHandle, GraphSubscriptionGroup>> {
        &self.groups
    }
    #[cfg(test)]
    pub(crate) fn gaps_for_tests(&self) -> &Mutex<VecDeque<CursorScope>> {
        &self.gaps
    }
    #[cfg(test)]
    pub(crate) fn recreated_for_tests(&self) -> &RecreatedSubscriptionIds {
        &self.recreated
    }
}
