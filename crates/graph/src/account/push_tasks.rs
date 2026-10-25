//! Account-owned effects survive session cancellation.
use super::GraphAccount;
pub(crate) fn track(account: &GraphAccount, task: tokio::task::JoinHandle<()>) {
    let mut tasks = account.push_tasks.lock().expect("push tasks lock");
    tasks.retain(|t| !t.is_finished());
    tasks.push(task);
}
