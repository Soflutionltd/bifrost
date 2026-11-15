//! Account-owned effects survive session cancellation.
use std::sync::{Arc, Mutex};

/// One shared task registry across all metered views of an opened account.
#[derive(Clone, Default)]
pub(crate) struct PushTasks(Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>);
impl PushTasks {
    pub(crate) fn track(&self, task: tokio::task::JoinHandle<()>) {
        let mut tasks = self.0.lock().expect("push tasks lock");
        tasks.retain(|t| !t.is_finished());
        tasks.push(task);
    }
    pub(crate) async fn join(&self) {
        let handles = std::mem::take(&mut *self.0.lock().expect("push tasks lock"));
        // One join bound. Dropping an unfinished handle detaches the effect;
        // aborting would strand the server-side cleanup it owns.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut left_running = 0_usize;
        for mut handle in handles {
            if tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
            {
                left_running += 1;
            }
        }
        if left_running > 0 {
            tracing::warn!(target: "bifrost_graph::push", left_running, "push cleanup tasks outlived the close() budget; left running detached");
        }
    }
}
pub(crate) fn track(account: &super::GraphAccount, task: tokio::task::JoinHandle<()>) {
    account.push_tasks.track(task);
}
