//! Writer-last worker drains and bounded teardown waits.

use super::*;

/// Remove the account's single ack-writer worker from a drained worker list.
///
/// Detach must wait on the stream workers FIRST: every one of them holds a
/// clone of the writer's `WriterRequest` sender, and the writer only sees its
/// channel close (and only then drains and persists what it already received)
/// once the last clone is gone. Waiting on the writer first therefore hits the
/// detach timeout and aborts a writer with unpersisted work.
///
/// The writer is identified by ROLE, not by position. It happens to be
/// spawned first, and the predecessor of this function read `drained[0]` on
/// that basis - a coupling between spawn order and teardown order that
/// nothing announced and that any future reordering of the spawn block would
/// have silently broken, mistaking a stream worker for the writer and then
/// waiting on the real writer in the wrong phase.
pub(super) fn take_ack_writer(workers: &mut Vec<WorkerTask>) -> Option<WorkerTask> {
    workers
        .iter()
        .position(|worker| worker.role == crate::engine::context::WorkerRole::AckWriter)
        .map(|position| workers.remove(position))
}

/// Await one stream worker's exit on the worker phase's shared deadline.
///
/// Takes the account id purely so the timeout line can name it. Detach is
/// per-account but the log stream is not: in a multi-account process an
/// unattributed "worker exceeded detach timeout" says only that some worker
/// somewhere wedged, which is not actionable. Every other teardown line in
/// `DetachingAccount::run` carries `account = ?account_id`; this one matches them.
///
/// The ROLE rides every line for the same reason. It is captured before the
/// match because `worker.join` moves into the timeout call.
///
/// The phase's deadline is SHARED by every stream worker, so once one
/// straggler has spent it, each later worker arrives here with nothing left
/// and is aborted without being awaited at all. That branch warns too, and
/// names its role: without it only the FIRST straggler of a slow detach was
/// ever attributed, and every worker aborted after it went silently.
pub(super) async fn await_worker_until(
    deadline: tokio::time::Instant,
    worker: WorkerTask,
    account_id: &AccountId,
) {
    let role = worker.role;
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        worker.abort.abort();
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?account_id,
            role = ?role,
            "worker phase detach deadline already spent by an earlier worker; aborted \
             without waiting"
        );
        return;
    }
    match tokio::time::timeout(remaining, worker.join).await {
        Ok(Ok(())) => {}
        Ok(Err(join)) => {
            if !join.is_cancelled() {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    role = ?role,
                    error = ?join,
                    "worker panic during detach"
                );
            }
        }
        Err(_) => {
            worker.abort.abort();
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                role = ?role,
                "worker exceeded detach timeout; aborted"
            );
        }
    }
}

/// Await the ack writer's final drain on the writer phase's own deadline.
///
/// Split from [`await_worker_until`] for the log line, not the mechanics. A
/// stream worker that misses its deadline was asked to stop and would not; a
/// writer that misses its own full budget is carrying acknowledged work the
/// drain exists to persist, which is a different operator story.
///
/// Do NOT read this as "the store is at fault". The engine's own senders are
/// gone by this point, but `BackfillCheckpointWriter` is published and owns a
/// `WriterRequest` sender of its own, so a consumer that retains one keeps the
/// channel open past the engine: the writer can then exhaust this budget
/// waiting for closure with a perfectly responsive `CheckpointStore`. Either
/// way the outcome is the same and bounded - abort is the only thing available,
/// since the account is gone and nothing can reach the writer for a retry - and
/// the consequence is redelivery, not loss.
pub(super) async fn await_ack_writer_until(
    deadline: tokio::time::Instant,
    worker: WorkerTask,
    account_id: &AccountId,
) {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let timed_out = if remaining.is_zero() {
        true
    } else {
        match tokio::time::timeout(remaining, worker.join).await {
            Ok(Ok(())) => false,
            Ok(Err(join)) => {
                if !join.is_cancelled() {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?account_id,
                        error = ?join,
                        "the ack writer panicked during detach; checkpoints it had \
                         accepted but not yet persisted are forfeited and the account \
                         resumes from an older checkpoint on the next attach"
                    );
                }
                false
            }
            Err(_) => true,
        }
    };
    if timed_out {
        worker.abort.abort();
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?account_id,
            "the ack writer did not finish its final drain within its own detach \
             budget: it is either blocked in the checkpoint store or still waiting \
             on a request channel a retained BackfillCheckpointWriter is holding \
             open. Aborting it forfeits the checkpoints it had accepted but not \
             persisted: the account resumes from an older checkpoint on the next \
             attach and redelivers, it does not lose changes"
        );
    }
}

#[cfg(test)]
#[path = "teardown_tests.rs"]
mod tests;
