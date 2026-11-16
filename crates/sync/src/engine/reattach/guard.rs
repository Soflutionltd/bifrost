//! Replacement ownership, provisional compensation and cancellation cleanup.

use super::*;

/// Tear down the subscriptions already created on a replacement connection
/// that is about to be discarded.
///
/// Any handle whose delete did not succeed is registered against the account
/// as an orphan (`teardown_unconfirmed`, not `desired`), because the
/// replacement is closed immediately after and `Account::close()` is not
/// contracted to delete server-side subscriptions. It is not `desired`
/// because the consumer's desire still lives in the record it was recreated
/// from, which the abort leaves in the registry untouched.
/// Without that record the engine would forget the handle entirely and
/// recreate exactly the orphaned-webhook leak the retained-handle rule exists
/// to prevent - a provider like Graph keeps delivering to the endpoint until
/// the subscription expires on its own.
///
/// Cancellation-safe: a record leaves `replacements` only AFTER its delete has
/// answered, and a refusal is restored to the registry in the same
/// synchronous step that removes it. A drop parked inside a delete therefore
/// leaves that record (and every later one) in the guard, whose `Drop` retires
/// them; nothing is ever held only by a local that a drop would discard.
pub(super) async fn unwind_replacement_subscriptions(
    ctx: &RecoveryContext<'_>,
    next: &dyn Account,
    replacements: &mut ReattachGuard,
    pacing: &mut RepairPacing,
) {
    let teardown = ctx.subscriptions.teardown(&ctx.account_id, &ctx.throttles);
    while let Some(front) = replacements.live.first() {
        let result = teardown.delete_or_orphan(next, front).await;
        replacements.live.remove(0);
        if let Err(refusal) = result {
            pacing.local_deadline = pacing.local_deadline.max(refusal.local_deadline);
        }
    }
}

/// Everything a reattach owes for the replacement connection it holds, until the
/// step that settles each debt has answered.
///
/// `Account::close()` does not delete server-side subscriptions, and the
/// registry does not know a replacement's handles until the cutover commits
/// them, so a reattach future dropped anywhere (a consumer timing out
/// `SyncEngine::reattach`, a worker abort at the detach deadline) would strand
/// the replacement connection, its subscriptions, and any cursor rows it had
/// persisted provisionally.
///
/// The guard is armed at CONSTRUCTION, never inside an async body a poll late:
/// `open_replacement_admitted` builds it synchronously on receiving the
/// replacement from the factory, and it travels with the replacement into
/// `reattach_account`, so there is no interval, a drop before that function's
/// first poll included, in which a live replacement is owned only by a local. It
/// tracks four debts:
///
/// - `live`, the push handles the replacement created. A handle is pushed here
///   in the same poll that `push_subscribe` returned it. The commit `disarm`s
///   them into the registry with no await between the two; an ordinary abort
///   empties them through `unwind_replacement_subscriptions`, which removes a
///   record only after its delete answered.
/// - `close_owed`, the account whose `close()` is still owed: the replacement
///   until the swap, and from `swapped` on the previous account. It is closed
///   through [`Self::close_owed`], which clears the debt only once the close has
///   RETURNED, so a drop parked inside a close owes it again.
/// - `rollback_owed`, the compensation for rows `reattach_insert` may have
///   written. Armed synchronously before the first insert, cleared once
///   `reattach_abort` answered.
/// - `commit_owed`, which `swapped` converts the rollback into. Past the swap an
///   abort would delete cursors belonging to a reattach that succeeded, so a drop
///   there must promote the rows instead. Cleared once the commit is enqueued.
///
/// On drop, the writer request is enqueued SYNCHRONOUSLY with `try_send`: it
/// lands in the writer's queue ahead of anything a later reattach could insert,
/// which a spawned task would not guarantee, and the abort only ever deletes rows
/// still provisional. A full channel falls back to a spawned send. Then a task is
/// spawned to delete the remaining `live` handles on the replacement (the only
/// account that can be trusted to know them), record every refusal as an orphan,
/// and close what is owed. With no runtime to spawn on, the handles are
/// registered as orphans directly, undeleted, and the close cannot run.
///
/// Orphans are retried against whatever account is current when they are
/// retried, not against the replacement that created them. That is an accepted
/// limit, benign in this workspace because no provider rejects a handle it does
/// not know (the retry clears the orphan and cannot fail forever), and it is the
/// reason the spawned task tries the replacement itself first. It is not a
/// guarantee of deletion: Graph, IMAP and JMAP keep a handle's server state in
/// the account instance, so a retry that reaches a newer connection is a no-op
/// and the provider-side subscription then lives until it expires. See
/// `reference/sync.md`.
///
/// Known residue: a drop parked INSIDE `push_subscribe` may have created a
/// subscription whose handle never reached this guard. That is the account
/// implementation's cancellation contract, not something the engine can see.
pub(in crate::engine) struct ReattachGuard {
    /// The replacement connection. Deletes of `live` run against it.
    pub(super) account: Arc<dyn Account>,
    subscriptions: Arc<SubscriptionRegistry>,
    account_id: AccountId,
    throttles: Arc<crate::throttle::ThrottleBucket>,
    /// The slot's reopen lock, for the spawned cleanup's registry write.
    reopen_lock: Arc<AsyncMutex<()>>,
    /// The account's writer queue, for the synchronous compensation on drop.
    writer: WriterHandle,
    /// This attempt's unique id. It rides every provisional insert and the
    /// abort or commit that settles them, so the writer touches only rows this
    /// attempt owns, even when a delayed send from a dropped attempt arrives
    /// after a later attempt has inserted.
    pub(super) attempt: crate::engine::ReattachAttempt,
    pub(super) live: Vec<RegisteredSubscription>,
    close_owed: Option<Arc<dyn Account>>,
    pub(super) rollback_owed: bool,
    pub(super) commit_owed: bool,
}

/// Provisional row owner, minted by replacement-account attempts only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReattachAttempt(u64);

impl ReattachAttempt {
    pub(super) fn mint() -> Self {
        Self(NEXT_REATTACH_ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }

    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self(value)
    }
}

/// Source of reattach attempt ids. Process-wide and monotonic, so an id is
/// unique across every slot and every writer, not just within one account.
static NEXT_REATTACH_ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl ReattachGuard {
    pub(super) fn new(ctx: &RecoveryContext<'_>, account: Arc<dyn Account>) -> Self {
        Self {
            attempt: ReattachAttempt::mint(),
            close_owed: Some(Arc::clone(&account)),
            account,
            subscriptions: Arc::clone(&ctx.subscriptions),
            account_id: ctx.account_id.clone(),
            throttles: Arc::clone(&ctx.throttles),
            reopen_lock: Arc::clone(&ctx.reopen_lock),
            writer: ctx.writer.clone(),
            live: Vec::new(),
            rollback_owed: false,
            commit_owed: false,
        }
    }

    pub(super) fn push(&mut self, record: RegisteredSubscription) {
        self.live.push(record);
    }

    /// The commit exit: the caller registers what this returns, synchronously.
    pub(super) fn disarm(&mut self) -> Vec<RegisteredSubscription> {
        std::mem::take(&mut self.live)
    }

    /// The cutover happened: the replacement is now the live account and the
    /// previous one is what needs closing, and the provisional rows are now the
    /// installed topology's, to be promoted rather than rolled back. Called
    /// synchronously right after the swap, with no await in between.
    pub(super) fn swapped(&mut self, previous: Arc<dyn Account>) {
        self.close_owed = Some(previous);
        self.rollback_owed = false;
        self.commit_owed = true;
    }

    /// Close whatever account is owed, and stop owing it once the close returned.
    /// Unlike attach/detach best-effort cleanup, this is not a bounded close:
    /// cancellation leaves the obligation on the guard for retire_stranded.
    /// Using close_within here would abandon an unfinished close and disarm that
    /// obligation on timeout. Keep this ownership contract until reattach and
    /// its dropped-future cleanup have a shared abandonment policy.
    pub(super) async fn close_owed(&mut self) -> Result<(), AccountError> {
        let Some(account) = self.close_owed.clone() else {
            return Ok(());
        };
        let result = account.close().await;
        self.close_owed = None;
        result
    }
}

impl Drop for ReattachGuard {
    fn drop(&mut self) {
        let runtime = tokio::runtime::Handle::try_current().ok();
        if self.rollback_owed {
            self.writer.settle_reattach_on_drop(
                self.attempt,
                false,
                runtime.as_ref(),
                &self.account_id,
            );
        }
        if self.commit_owed {
            self.writer.settle_reattach_on_drop(
                self.attempt,
                true,
                runtime.as_ref(),
                &self.account_id,
            );
        }
        let live = std::mem::take(&mut self.live);
        let close = self.close_owed.take();
        if live.is_empty() && close.is_none() {
            return;
        }
        let account = Arc::clone(&self.account);
        let subscriptions = Arc::clone(&self.subscriptions);
        let account_id = self.account_id.clone();
        let throttles = Arc::clone(&self.throttles);
        let reopen_lock = Arc::clone(&self.reopen_lock);
        match runtime {
            Some(runtime) => {
                runtime.spawn(retire_stranded(Stranded {
                    account,
                    subscriptions,
                    account_id,
                    throttles,
                    reopen_lock,
                    live,
                    close,
                }));
            }
            None => {
                // No lock here, deliberately. Drop cannot await, and a blocking
                // acquire would stall a non-async thread on a lock the dropping
                // reattach itself usually still holds (its reopen guard lives in
                // a frame that is being torn down around this one). It is also
                // unnecessary: with no runtime nothing can be running a
                // concurrent reattach, `unsubscribe_push`, or `subscribe_push`
                // against this registry, so there is no snapshot to be
                // overwritten.
                tracing::warn!(
                    target: "bifrost.sync.reopen",
                    account = ?account_id,
                    stranded = live.len(),
                    close_owed = close.is_some(),
                    "reattach dropped with no runtime to clean up on; registering replacement \
                     handles as orphans, and an account owed a close cannot be closed"
                );
                subscriptions
                    .teardown(&account_id, &throttles)
                    .retain_orphans(live);
            }
        }
    }
}

/// What a dropped reattach's cleanup task needs, gathered by the guard's `Drop`.
struct Stranded {
    /// The replacement, which the `live` deletes run against.
    pub(super) account: Arc<dyn Account>,
    subscriptions: Arc<SubscriptionRegistry>,
    account_id: AccountId,
    throttles: Arc<crate::throttle::ThrottleBucket>,
    reopen_lock: Arc<AsyncMutex<()>>,
    pub(super) live: Vec<RegisteredSubscription>,
    /// The account still owed a `close()`: the replacement before the swap, the
    /// previous account after it.
    close: Option<Arc<dyn Account>>,
}

/// Cleanup for a reattach future that was dropped while owing something.
/// Runs detached from the dropped future.
///
/// The deletes and the close run WITHOUT the slot's reopen lock, so a concurrent
/// reattach never waits on this task's network calls. Only the registry write
/// takes it. A reattach snapshots the registry and later overwrites it with
/// `replace`, built from that snapshot alone: a refusal restored between the two
/// (this task's delete failing while a later reattach is mid-flight) would be
/// erased, and the provider subscription could never be retried. Under the lock
/// the restore lands either before that reattach snapshots (which then carries it
/// as it carries any orphan) or after it commits (`restore_orphans` merges into
/// what it installed). The lock is for THAT ordering only; the detach race is
/// closed by the registry's own check, which the lock could not close because
/// detach does not take it.
///
/// The dropped reattach's own reopen guard cannot deadlock this: the guard is
/// released by the drop, independently of this task, and this task only ever
/// waits on the lock asynchronously.
async fn retire_stranded(stranded: Stranded) {
    let Stranded {
        account,
        subscriptions,
        account_id,
        throttles,
        reopen_lock,
        live,
        close,
    } = stranded;
    let teardown = subscriptions.teardown(&account_id, &throttles);
    let mut refused = Vec::new();
    for record in live {
        if teardown
            .delete(account.as_ref(), &record.handle)
            .await
            .is_err()
        {
            refused.push(record);
        }
    }
    if !refused.is_empty() {
        let _reopen_guard = reopen_lock.lock().await;
        teardown.retain_orphans(refused);
    }
    if let Some(owed) = close
        && let Err(error) = owed.close().await
    {
        tracing::warn!(
            target: "bifrost.sync.reopen",
            account = ?account_id,
            error = %error,
            "account close failed after a dropped reattach"
        );
    }
}

/// Delete the durable cursor rows an aborted reattach created.
///
/// This is deliberately the ONLY durable compensation the reopen path has,
/// and it only ever deletes rows whose scope had no stored cursor before
/// this reattach began. Preexisting rows are never deleted, snapshotted, or
/// restored here: the old account's ack writer keeps persisting checkpoints
/// concurrently (nothing serializes it against a reopen), so any
/// snapshot-and-restore of a preexisting row could overwrite a newer
/// consumer-acknowledged checkpoint. Deleting only rows this reattach itself
/// created cannot destroy preexisting data - the worst outcome of a failed
/// delete is a leaked cursor for a topology that was never installed.
/// Compensate an aborted reattach by deleting the cursor rows it inserted.
///
/// The set of rows is NOT passed in. The account's writer tracks which scopes
/// are still provisional - inserted by this reattach and not since claimed by a
/// consumer acknowledgement - and deletes exactly those. Passing a list
/// computed here would reintroduce the bug: this task cannot see whether an ack
/// landed for one of those scopes in the meantime, and a replacement inventory
/// pass broadcasts acknowledgeable checkpoints before the cutover, so the list
/// goes stale the moment a consumer acts on one.
///
/// Serializing the delete behind the writer is necessary but not sufficient on
/// its own; a plain FIFO queue would order the acknowledged write first and
/// then faithfully destroy it. The provisional set is what makes this a
/// conditional delete.
///
/// The guard's `rollback_owed` is cleared only once the writer answered, so a
/// drop parked inside this await still compensates (a second abort is
/// idempotent: the first left nothing provisional).
pub(super) async fn rollback_reattach_inserts(
    ctx: &RecoveryContext<'_>,
    guard: &mut ReattachGuard,
) {
    ctx.writer.reattach_abort(guard.attempt).await;
    guard.rollback_owed = false;
}

/// Promote this reattach's provisional rows to ordinary durable state.
///
/// Called after the cutover commits, past the last point an abort can occur.
/// Without it the scopes stay marked provisional and the NEXT reattach's abort
/// would delete cursors belonging to a reattach that succeeded.
///
/// The guard's `commit_owed` is cleared once the request is enqueued (a
/// `reattach_commit` is only a channel send), so a drop parked while the
/// channel is full still promotes the rows.
pub(super) async fn commit_reattach_inserts(ctx: &RecoveryContext<'_>, guard: &mut ReattachGuard) {
    if ctx.writer.reattach_commit(guard.attempt).await.is_err() {
        tracing::error!(
            target: "bifrost.sync.reopen",
            account = ?&ctx.account_id,
            "writer channel closed before the reattach could be committed"
        );
    }
    guard.commit_owed = false;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_common::{StubAccount, StubFactory};
    use std::sync::atomic::Ordering;

    #[tokio::test(start_paused = true)]
    async fn dropped_reattach_teardown_records_shared_holds_before_orphaning_and_close() {
        let engine = SyncEngine::builder().build().expect("engine");
        let id = AccountId("dropped-cleanup".into());
        engine
            .attach(
                id.clone(),
                Arc::new(StubFactory::queue(vec![Arc::new(StubAccount::new(vec![]))])),
            )
            .await
            .expect("attach");
        let slot = Arc::clone(engine.accounts.get(&id).expect("slot").value());
        let error = bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::Server(
            bifrost_types::ServerCause::RateLimited {
                throttle: Default::default(),
                retry_hint: Some(bifrost_types::RetryHint::After(Duration::from_secs(37))),
            },
        ))
        .with_throttle(|throttle| throttle.scope = Some(bifrost_types::ThrottleScope::Account))
        .try_build()
        .expect("throttle");
        let mut replacement = StubAccount::new(vec![]);
        replacement.unsubscribe_error_hook = Some(Arc::new(move |_| Some(error.clone())));
        let replacement = Arc::new(replacement);
        let writer = slot.writer();
        let mut guard = ReattachGuard::new(
            &slot.recovery(&writer),
            Arc::<StubAccount>::clone(&replacement),
        );
        guard.push(RegisteredSubscription {
            handle: bifrost_types::SubscriptionHandle("replacement".into()),
            scopes: vec![],
            state: crate::push::subscription::SubscriptionState::Live,
        });
        drop(guard);
        for _ in 0..1000 {
            if replacement.close_returned.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(replacement.close_returned.load(Ordering::SeqCst), 1);
        assert_eq!(
            crate::throttle::account_throttle_wait(&slot.throttles, &id),
            Some(Duration::from_secs(37))
        );
        assert_eq!(slot.subscriptions.orphans().len(), 1);
        engine.detach(&id).await.expect("detach");
    }
}
