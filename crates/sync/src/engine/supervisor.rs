//! The account's serial recovery and scope-lifecycle supervisor.

use super::*;

/// Reopen request raised by per-scope drivers when a stream ends with
/// an account-side failure. The engine listens on the receiver and
/// dispatches according to the carried `AccountError`'s recovery.
///
/// `scope` is `Option<CursorScope>`: scope-bound directives
/// (`Engine(RestartScope)`, `Engine(DowngradeCapabilityForScope)`,
/// `Engine(DowngradeStrategy)` with `ErrorScope::Cursor`) carry
/// `Some(scope)`. Account-wide directives (`RestartAccount`,
/// `SchemaIncompatible`, `OperatorOverrideRequired`)
/// carry `None`. Workers must not paper over an account-wide directive
/// with the worker's own scope - passing `Some(arbitrary_scope)` would
/// mask the directive's account-wide intent.
///
/// `#[non_exhaustive]` is deliberate and stays: the enum is `pub` and
/// re-exported from `lib.rs`, so new request kinds must not break
/// downstream matches. Adjudicated during the error-model close-out -
/// do not re-raise.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReopenRequest {
    /// Push directive stamped with the account instance that produced it.
    PushRecovery {
        scope: Option<CursorScope>,
        error: AccountError,
        generation: crate::types::AccountGeneration,
    },
    /// Account error observed at the scope's stream terminus, or raised by a
    /// mutation campaign or a cold-start walk.
    ///
    /// `generation` is the generation of the incarnation
    /// ([`crate::Incarnation`]) whose operation failed. A stamped request is
    /// declined, under the reopen lock and before any effect, once a reattach
    /// has installed a different incarnation: a directive about the retired
    /// connection must not restart, reset or disable its replacement, and the
    /// replacement's own workers raise it again if the fault persists. `None`
    /// acts on whichever incarnation is current when the listener reaches it;
    /// the engine leaves only a consumer-injected `InvalidationSink::push`
    /// termination unstamped, since nothing names the instance it came from.
    Recovery {
        scope: Option<CursorScope>,
        error: AccountError,
        generation: Option<crate::types::AccountGeneration>,
    },
    /// A provider lifecycle event deleted this scope's folder. The engine
    /// must purge the scope's durable rows - the change cursor AND the
    /// backfill rows, completion marker included. The consumer plausibly
    /// purged the folder's data on `Deleted`, so a completion marker
    /// surviving into a folder recreated under the same id would skip the
    /// new incarnation's entire cold-start walk: data invisibility, not
    /// just a leaked row.
    ScopeDeleted { scope: CursorScope },
    /// A provider lifecycle event created this scope's folder. The engine
    /// establishes it unless it is already registered: a reattach whose
    /// discovery ran while this request was queued may have established it on
    /// the replacement already, and that cursor must not be reset. Not fenced
    /// by account generation, since it states a fact about the server rather
    /// than about the connection that observed it.
    ScopeCreated { scope: CursorScope },
    /// Engine plumbing: establish `scope` unless it is already registered or
    /// a provider deletion moved its lifecycle epoch past `epoch` since the
    /// obligation was recorded. Raised by parked scope repairs and by the
    /// deferred inventory worker for a walk a reset or deletion superseded.
    /// The epoch is an engine-internal counter and means nothing to a
    /// consumer.
    EstablishIfAbsent {
        scope: CursorScope,
        epoch: crate::cursor::LifecycleEpoch,
    },
    /// Engine plumbing: re-run an account-wide schema reset that was declined
    /// under a pause or exhausted its retry budget, owing re-establishment to
    /// `reestablish` (scope -> lifecycle epoch). `parked_at` is the count of
    /// completed account resets as of the newest decline; a reset that has
    /// committed since already cleared every row, so the listener then only
    /// establishes the owed scopes.
    SchemaReset {
        reestablish: HashMap<CursorScope, crate::cursor::LifecycleEpoch>,
        parked_at: crate::engine::ResetCount,
    },
}

/// Drive requests in channel order, observing the reset epoch at receipt.
pub(super) async fn run(reopen_ctx: SlotContext, mut reopen_rx: mpsc::Receiver<ReopenRequest>) {
    let mut queued = std::collections::VecDeque::new();
    loop {
        let (req, observed) = match queued.pop_front() {
            Some(stamped) => stamped,
            None => tokio::select! {
                () = reopen_ctx.shutdown.cancelled() => return,
                req = reopen_rx.recv() => match req {
                    Some(req) => (req, reopen_ctx.deferred_schema_reset.completed()),
                    None => return,
                },
            },
        };
        if reopen_ctx.shutdown.is_cancelled() {
            return;
        }
        receive_during_dispatch(
            &mut reopen_rx,
            &mut queued,
            || reopen_ctx.deferred_schema_reset.completed(),
            dispatch(&reopen_ctx, req, observed),
        )
        .await;
    }
}

/// Keep receiving while a repair awaits provider work. A schema clear can
/// commit before re-establishment finishes: a new failure after that boundary
/// must get its own reset, as users expect a refreshed folder to recover too.
/// Staging is bounded by the channel capacity and preserves FIFO order.
async fn receive_during_dispatch(
    requests: &mut mpsc::Receiver<ReopenRequest>,
    queued: &mut std::collections::VecDeque<(ReopenRequest, ResetCount)>,
    epoch: impl Fn() -> ResetCount,
    action: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(action);
    let capacity = requests.max_capacity();
    let mut closed = false;
    loop {
        tokio::select! {
            biased;
            request = requests.recv(), if !closed && queued.len() < capacity => {
                match request {
                    Some(request) => queued.push_back((request, epoch())),
                    None => closed = true,
                }
            },
            () = &mut action => return,
        }
    }
}

async fn recover(
    ctx: &AccountSlot,
    scope: Option<CursorScope>,
    error: AccountError,
    generation: Option<crate::types::AccountGeneration>,
    observed: ResetCount,
) {
    let writer = ctx.writer();
    let mut recovery = ctx.recovery(&writer);
    recovery.expected_generation = generation;
    recovery.observed_schema_reset = Some(observed);
    let followup = handle_account_error(&recovery, scope, error).await;
    ctx.park_followup(followup);
}

pub(super) async fn dispatch(reopen_ctx: &SlotContext, req: ReopenRequest, observed: ResetCount) {
    match req {
        ReopenRequest::PushRecovery {
            scope,
            error,
            generation,
        } => {
            recover(reopen_ctx, scope, error, Some(generation), observed).await;
        }
        ReopenRequest::Recovery {
            scope,
            error,
            generation,
        } => {
            recover(reopen_ctx, scope, error, generation, observed).await;
        }
        // Both run under the reopen lock, like every
        // other scope repair.
        ReopenRequest::EstablishIfAbsent { scope, epoch } => {
            let writer = reopen_ctx.writer();
            let recovery = reopen_ctx.recovery(&writer);
            let _guard = reopen_ctx.reopen_lock.lock().await;
            let followup = super::recovery::establish_if_absent(&recovery, scope, epoch).await;
            reopen_ctx.park_followup(followup);
        }
        // The epoch is read here, under the lock, not by
        // the multiplexer: a deletion queued ahead of
        // this request has bumped it by now, and one
        // queued behind it bumps it later and retires
        // what this establishes.
        ReopenRequest::ScopeCreated { scope } => {
            let writer = reopen_ctx.writer();
            let recovery = reopen_ctx.recovery(&writer);
            let _guard = reopen_ctx.reopen_lock.lock().await;
            let epoch = reopen_ctx.cursors.lifecycle_epoch(&scope);
            let followup = super::recovery::establish_if_absent(&recovery, scope, epoch).await;
            reopen_ctx.park_followup(followup);
        }
        ReopenRequest::SchemaReset {
            reestablish,
            parked_at,
        } => {
            let writer = reopen_ctx.writer();
            let recovery = reopen_ctx.recovery(&writer);
            let _guard = reopen_ctx.reopen_lock.lock().await;
            let followup =
                super::recovery::run_parked_schema_reset(&recovery, reestablish, parked_at).await;
            reopen_ctx.park_followup(followup);
        }
        ReopenRequest::ScopeDeleted { scope } => {
            // Keep retirement and its writer delete inside the same topology
            // transaction as reattach. Otherwise a replacement can establish
            // between them and lose its newly earned cursor to this delete.
            let _guard = reopen_ctx.reopen_lock.lock().await;
            // A repair or schema reset parked for this
            // scope under a pause must not establish it
            // after resume: the folder it was for is
            // gone.
            reopen_ctx.deferred_repairs.unpark(&scope);
            reopen_ctx.deferred_schema_reset.unpark_scope(&scope);
            // A cursor here was established after the
            // deletion was raised: a `Created` whose
            // repair was still queued ahead of this
            // request on this same channel. Retire it
            // exactly as the multiplexer retires a
            // registered scope - epoch, fence, then
            // registry, then the poll token - before
            // purging its rows, or the rows would be
            // deleted out from under a live cursor for
            // a folder that no longer exists. The
            // retirement runs unconditionally: this
            // second epoch bump and fence raise is what
            // revokes an establishment still in flight
            // for the scope.
            let registered = reopen_ctx.cursors.snapshot(&scope).is_some();
            crate::multiplexer::retire_deleted_scope(
                &reopen_ctx.cursors,
                &reopen_ctx.control,
                &scope,
            );
            if registered {
                crate::multiplexer::cancel_scope_token(&reopen_ctx.scope_tokens, &scope);
            }
            // A provider-deleted folder retires its
            // durable rows in full: change cursor and
            // backfill rows, completion marker
            // included. A surviving marker would make
            // a folder recreated under the same id
            // skip its cold-start walk entirely.
            let writer = reopen_ctx.writer();
            if let Err(error) = writer.reset_scope_for_deletion(scope.clone()).await {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?reopen_ctx.account_id,
                    scope = ?scope,
                    error = %error,
                    "failed to purge durable rows for a provider-deleted scope"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failures_received_after_clear_get_a_new_epoch_before_dispatch_returns() {
        let epoch = std::cell::Cell::new(ResetCount::for_test(0));
        let (tx, mut rx) = mpsc::channel(2);
        let mut queued = std::collections::VecDeque::new();
        let mut step = 0;
        let action = std::future::poll_fn(|cx| {
            match step {
                0 => {
                    tx.try_send(ReopenRequest::ScopeCreated {
                        scope: CursorScope::Account,
                    })
                    .expect("pre-reset duplicate");
                }
                1 => {
                    epoch.set(ResetCount::for_test(1));
                    tx.try_send(ReopenRequest::ScopeCreated {
                        scope: CursorScope::Account,
                    })
                    .expect("post-reset failure");
                }
                _ => return std::task::Poll::Ready(()),
            }
            step += 1;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        });
        receive_during_dispatch(&mut rx, &mut queued, || epoch.get(), action).await;
        assert_eq!(queued.len(), 2);
        assert_eq!(
            queued.pop_front().expect("duplicate").1,
            ResetCount::for_test(0)
        );
        assert_eq!(
            queued.pop_front().expect("new failure").1,
            ResetCount::for_test(1)
        );
    }
}
