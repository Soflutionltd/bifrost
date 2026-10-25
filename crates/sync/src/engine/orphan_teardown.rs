//! The scheduled retrier for push subscriptions whose teardown failed.
//!
//! A handle whose `push_unsubscribe` the provider refused is kept in the
//! registry as an orphan (`teardown_unconfirmed`, not `desired`) so the
//! server-side subscription stays reachable. Before this worker existed,
//! only the next reopen or the consumer's next `unsubscribe_push` retried
//! it, so an account that never reopened kept its orphan delivering until
//! the provider expired it (about a day for Graph). This worker retries on
//! a timer instead.

use super::context::SlotContext;
use super::*;

/// Retry every pure orphan's teardown each `interval` until the slot shuts
/// down.
///
/// Only pure orphans are retried. A record that is still `desired` is the
/// consumer's live coverage - even when its teardown is unconfirmed, which is
/// an aborted reopen's mark on a handle the old, still-installed account
/// serves - and deleting it here would end push the consumer still wants.
///
/// The pass deliberately does NOT take the slot's reopen lock, unlike
/// `unsubscribe_push` and reattach. It holds that lock's reason for
/// existing - a take / restore or a snapshot / replace interleaving with
/// another writer - nowhere: nothing is taken out of the registry, and the
/// only write is removing a record after its teardown succeeded. Every
/// interleaving therefore costs at most one redundant `push_unsubscribe`:
/// a reopen or consumer teardown that re-registers a handle this pass just
/// deleted hands it to the next pass, whose DELETE the provider answers as
/// already gone. Holding the lock across provider calls, on the other hand,
/// let one stalled attempt - the `Account` contract gives `push_unsubscribe`
/// no deadline - block every reattach, `subscribe_push` and
/// `unsubscribe_push` on the account, and a per-attempt bound tight enough to
/// prevent that would truncate a provider teardown that walks several ids
/// (Graph's) before it reached the later ones, every pass.
///
/// So the bound, `ATTEMPT_TIMEOUT`, is only a backstop against a future that
/// never resolves, generous enough for a multi-id teardown made of ordinary
/// requests. A timed-out attempt is a failure like any other. A pass cut
/// short by shutdown (detach cancels the token, then awaits and aborts its
/// workers) strands nothing: the record is still registered, for detach to
/// discard with the rest of the incarnation.
///
/// The interval is floored at `MIN_INTERVAL`, so a configured zero cannot
/// turn the worker into a hot loop.
pub(super) async fn run_orphan_teardown_retrier(ctx: SlotContext, interval: Duration) {
    const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5 * 60);
    const MIN_INTERVAL: Duration = Duration::from_secs(1);
    let interval = interval.max(MIN_INTERVAL);
    loop {
        tokio::select! {
            () = ctx.shutdown.cancelled() => return,
            () = tokio::time::sleep(interval) => {}
        }
        // A paused account makes no provider calls on the engine's own
        // initiative, cleanup included: a pause may be an operator override
        // or a lost credential, and either way the next pass after resume
        // picks the orphans up.
        if matches!(
            *ctx.boundary_tx.borrow(),
            crate::cancel::BoundaryRequest::Pause | crate::cancel::BoundaryRequest::Stop
        ) {
            continue;
        }
        let account = ctx.current.load_full();
        for handle in ctx.subscriptions.orphans(&ctx.account_id) {
            let _topology = super::TopologyBump(&ctx.push_topology);
            let result = tokio::select! {
                biased;
                () = ctx.shutdown.cancelled() => return,
                result = tokio::time::timeout(
                    ATTEMPT_TIMEOUT,
                    account.push_unsubscribe(handle.clone()),
                ) => result,
            };
            // Detach cancels before it takes the registry, so a removal that
            // observes the cancellation could land on a later incarnation's
            // entry; stop instead.
            if ctx.shutdown.is_cancelled() {
                return;
            }
            match result {
                Ok(Ok(())) => ctx.subscriptions.remove_orphan(&ctx.account_id, &handle),
                Ok(Err(error)) => {
                    tracing::debug!(
                        target: "bifrost.sync.push",
                        account = ?ctx.account_id,
                        error = %error,
                        "scheduled orphan teardown failed; retrying next pass"
                    );
                }
                Err(_elapsed) => {
                    tracing::debug!(
                        target: "bifrost.sync.push",
                        account = ?ctx.account_id,
                        "scheduled orphan teardown timed out; retrying next pass"
                    );
                }
            }
        }
    }
}
