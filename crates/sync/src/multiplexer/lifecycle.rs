//! Scope lifecycle observation and ordered topology transitions.

use super::*;

pub(super) struct LifecycleWorker {
    pub account: Arc<ArcSwap<crate::types::Incarnation>>,
    pub shutdown: CancellationToken,
    pub cursors: Arc<CursorRegistry>,
    pub control: SyncControl,
    pub tokens: ScopeTokens,
    pub reopen: mpsc::Sender<ReopenRequest>,
    pub changes: Arc<ChangeDelivery>,
    pub generation: watch::Receiver<crate::types::AccountGeneration>,
    pub throttles: Arc<crate::throttle::ThrottleBucket>,
    pub account_id: AccountId,
}

impl LifecycleWorker {
    pub(super) async fn run(self) {
        let Self {
            account,
            shutdown,
            cursors,
            control,
            tokens,
            reopen,
            changes,
            mut generation,
            throttles,
            account_id,
        } = self;
        let mut backoff = crate::backoff::ReconnectBackoff::new(LIFECYCLE_BACKOFF);
        let mut carry = false;
        loop {
            if carry {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    changed = generation.changed() => {
                        if changed.is_err() { return; }
                        continue;
                    }
                    () = tokio::time::sleep(backoff.carried_delay()) => {}
                }
                carry = false;
            }
            // Recheck shared holds after every wake; sibling work can
            // extend one while this reader is waiting.
            if let Some(wait) = crate::throttle::account_throttle_wait(&throttles, &account_id) {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    changed = generation.changed() => {
                        if changed.is_err() { return; }
                        carry = true;
                    }
                    () = tokio::time::sleep(wait) => {}
                }
                continue;
            }
            let acc = account.load_full();
            let mut stream = acc.scope_lifecycle_stream();
            let mut retry_wait = Duration::ZERO;
            let mut reopened = false;
            let mut failure_delay = None;
            loop {
                tokio::select! {
                        () = shutdown.cancelled() => return,
                        changed = generation.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            reopened = true;
                            break;
                        }
                        next = stream.next() => {
                        let Some(ev) = next else { break; };
                        if matches!(
                            &ev,
                            bifrost_types::ScopeLifecycleEvent::Lifecycle(_) | bifrost_types::ScopeLifecycleEvent::Polled
                        ) {
                            backoff.reset();
                        }
                        let lifecycle = match ev {
                            bifrost_types::ScopeLifecycleEvent::Polled => continue,
                            bifrost_types::ScopeLifecycleEvent::Lifecycle(lc) => lc,
                            bifrost_types::ScopeLifecycleEvent::Terminated(err) => {
                                let delay = backoff.record_failure(crate::backoff::jitter_seed());
                                failure_delay = Some(delay);
                                let policy = lifecycle_termination(&err);
                                retry_wait = lifecycle_retry_wait(
                                    &err, delay, &throttles,
                                    &account_id,
                                );
                                match policy {
                                    LifecycleTermination::AwaitReopen
                                    | LifecycleTermination::ForwardAndReconnect => {
                                        // Hand the classified error to the
                                        // engine exactly once. The reopen
                                        // path owns replacement; this reader
                                        // carries its failure count across
                                        // generations to pace persistent faults.
                                        if reopen
                                            .send(ReopenRequest::Recovery {
                                                scope: None,
                                                error: err,
                                                // The incarnation whose stream
                                                // this is: a request queued past
                                                // a reattach must not restart
                                                // the replacement.
                                                generation: Some(acc.generation()),
                                            })
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                        if policy == LifecycleTermination::AwaitReopen {
                                            // A replacement connection can
                                            // revive this stream, so prefer
                                            // waiting for it - but bound the
                                            // wait, because a failed or
                                            // pause-queued reopen never
                                            // bumps the generation and this
                                            // reader must not park forever.
                                            tokio::select! {
                                                () = shutdown.cancelled() => return,
                                                changed = generation.changed() => {
                                                    if changed.is_err() {
                                                        return;
                                                    }
                                                    reopened = true;
                                                }
                                                () = tokio::time::sleep(LIFECYCLE_REOPEN_WAIT) => {}
                                            }
                                        }
                                        break;
                                    }
                                    LifecycleTermination::Terminate => {
                                        // Do not retry this failed handle. An explicit
                                        // reattach can repair credentials or policy, so
                                        // keep the reader alive to observe its generation.
                                        changes.publish_terminated(EventScope::Account, err);
                                        tokio::select! {
                                            () = shutdown.cancelled() => return,
                                            changed = generation.changed() => {
                                                if changed.is_err() { return; }
                                                reopened = true;
                                            }
                                        }
                                        break;
                                    }
                                    LifecycleTermination::Reconnect => break,
                                }
                            }
                            _ => continue,
                        };
                        apply_lifecycle_transition(
                            &cursors,
                            &control,
                            &tokens,
                            &reopen,
                            lifecycle,
                        )
                        .await;
                    }
                }
            }
            if reopened {
                carry = true;
                continue;
            }
            let delay = failure_delay
                .unwrap_or_else(|| backoff.record_failure(crate::backoff::jitter_seed()));
            tokio::select! {
                () = shutdown.cancelled() => return,
                changed = generation.changed() => {
                    if changed.is_err() { return; }
                    carry = true;
                    continue;
                }
                () = tokio::time::sleep(delay.max(retry_wait)) => {}
            }
        }
    }
}

/// Upper bound on the lifecycle reader's wait for a replacement connection.
///
/// Only a successful account reattach bumps the account generation. A reopen
/// that exhausts its retry budget, or that is queued behind a pause, produces
/// no generation change at all, so an unbounded wait would park the reader for
/// the process lifetime and silently stop folder/label lifecycle observation.
pub(super) const LIFECYCLE_REOPEN_WAIT: Duration = crate::recovery::REOPEN_WAIT;

/// Lifecycle-stream termination policy.
///
/// A terminal account error cannot be repaired by reconnecting the same dead
/// stream. An `Engine(RestartAccount)` directive replaces the connection this
/// reader is holding, so it hands the error over once and waits (bounded) for
/// the generation change rather than hammering a dead handle. Every other
/// directive - `RestartScope`, `DisableScope`, `SchemaIncompatible`,
/// `OperatorOverrideRequired`, the downgrades - is handled without swapping
/// the account, so no generation change is ever coming and the reader must
/// reconnect on its own backoff after handing the error over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LifecycleTermination {
    /// Forward once, then wait (bounded) for a replacement connection.
    AwaitReopen,
    /// Forward once, then reconnect this reader on backoff.
    ForwardAndReconnect,
    /// Reconnect on backoff; the engine has nothing to decide.
    Reconnect,
    /// Report once and stop this reader.
    Terminate,
}

// Lifecycle polling can normally be minutes apart. A failed first read must
// not turn that into a 50 ms request loop. Only observed lifecycle progress or
// a replacement account resets this backoff; opening a stream proves nothing.
pub(super) const LIFECYCLE_RECONNECT_MIN: Duration = Duration::from_secs(30);
pub(super) const LIFECYCLE_RECONNECT_MAX: Duration = Duration::from_secs(480);

pub(super) const LIFECYCLE_BACKOFF: bifrost_types::BackoffPolicy =
    bifrost_types::BackoffPolicy::new(LIFECYCLE_RECONNECT_MIN, LIFECYCLE_RECONNECT_MAX, false);

pub(super) fn lifecycle_retry_wait(
    error: &AccountError,
    fallback: Duration,
    throttles: &crate::throttle::ThrottleBucket,
    account_id: &AccountId,
) -> Duration {
    use crate::recovery::RecoveryPlan;
    let now = std::time::SystemTime::now();
    let hint = match crate::recovery::dispatch_stream_failure(error.clone(), throttles, account_id)
    {
        RecoveryPlan::Retry(advice) => {
            // Account requests own token refresh through their transport. There
            // is no engine token source to refresh here; reconnect retries the
            // read through that same authenticated request funnel.
            crate::recovery::retry_delay(&advice, now, fallback)
        }
        RecoveryPlan::Reconcile(advice) => {
            // Lifecycle reads have no mutation target to reconcile. Retry the
            // uncommitted read, preserving its provider-owned diff baseline.
            crate::recovery::reconcile_delay(&advice, now, fallback)
        }
        RecoveryPlan::Engine(_) | RecoveryPlan::Terminal(_) => fallback,
    };
    fallback.max(hint)
}

pub(super) fn lifecycle_termination(error: &AccountError) -> LifecycleTermination {
    use crate::recovery::{RecoveryPlan, plan_recovery};
    use bifrost_types::EngineDirective;

    match plan_recovery(error.clone()) {
        RecoveryPlan::Engine(EngineDirective::RestartAccount) => LifecycleTermination::AwaitReopen,
        RecoveryPlan::Engine(_) => LifecycleTermination::ForwardAndReconnect,
        RecoveryPlan::Terminal(_) => LifecycleTermination::Terminate,
        RecoveryPlan::Retry(_) | RecoveryPlan::Reconcile(_) => LifecycleTermination::Reconnect,
    }
}

/// Apply one `ScopeLifecycle` transition to the live registry, the poll
/// tokens, and the engine's reopen channel.
///
/// `Created` asks the engine (`ReopenRequest::ScopeCreated`) to
/// establish the new scope's cursor unless it is already registered. `Deleted` cancels the poll task,
/// drops the in-memory cursor, and raises `ReopenRequest::ScopeDeleted`
/// so the engine purges the scope's DURABLE rows too - change cursor and
/// backfill rows, completion marker included; without that, a folder
/// recreated under the same id would find the old completion marker and
/// skip its entire cold-start walk. `Renamed` across ids is the delete
/// half on the old id plus the create half on the new one, whose shapes
/// are read before the delete; `Renamed` on one stable id is the create
/// half alone.
///
/// Its reopen sends select no shutdown arm: the lifecycle reader that calls
/// it is aborted by an abort-on-drop guard in `Multiplexer::run`, which is
/// what frees a send parked on a full channel.
pub(super) async fn apply_lifecycle_transition(
    cursors: &CursorRegistry,
    control: &SyncControl,
    tokens: &ScopeTokens,
    reopen_tx: &mpsc::Sender<ReopenRequest>,
    lifecycle: ScopeLifecycle,
) {
    async fn delete_scopes(
        cursors: &CursorRegistry,
        control: &SyncControl,
        tokens: &ScopeTokens,
        reopen_tx: &mpsc::Sender<ReopenRequest>,
        membership: &bifrost_types::MembershipScope,
    ) {
        // Every scope a `Created` for this membership could have asked the
        // engine to establish this attachment, under any model it ran: the
        // mapping reads remembered shapes, not live cursors, so it holds when
        // no cursor of the shape is registered (a cross-id rename of the last
        // folder cursor whose create half is still queued or parked). Every
        // one the membership index does not already name below is routed to
        // the listener too, cursor or not:
        // - with no cursor, its repair may be parked under a pause or still
        //   queued on the listener's channel ahead of this request; the
        //   listener's `ScopeDeleted` arm unparks the one and retires the
        //   cursor the other establishes, both before purging;
        // - with a cursor missing from the membership index (a membership
        //   refresh after its establishment failed), the arm retires it too,
        //   where the index loop below would never find it.
        let indexed = cursors.scopes_for_membership(membership);
        let unindexed: Vec<CursorScope> = membership_to_deleted_scopes(cursors, membership)
            .into_iter()
            .filter(|scope| !indexed.contains(scope))
            .collect();
        for scope in indexed {
            // Registry first. `delete` cancels the scope's liveness token as
            // it drops the cursor, under one write lock, so there is no
            // instant where the scope looks drivable to a producer that has
            // not yet been stopped. The reverse order left exactly that
            // window: the poll token was gone while the cursor was still
            // live, and a reconciler entering it started a drive on a scope
            // about to be deleted.
            retire_deleted_scope(cursors, control, &scope);
            cancel_scope_token(tokens, &scope);
            let _ = reopen_tx.send(ReopenRequest::ScopeDeleted { scope }).await;
        }
        for scope in unindexed {
            retire_deleted_scope(cursors, control, &scope);
            let _ = reopen_tx.send(ReopenRequest::ScopeDeleted { scope }).await;
        }
    }

    async fn establish_scopes(reopen_tx: &mpsc::Sender<ReopenRequest>, scopes: Vec<CursorScope>) {
        for scope in scopes {
            // The cursor registry has no entry yet; the poll task would
            // see snapshot=None and exit cleanly unless the engine
            // establishes a cursor first. The obligation is "establish this
            // folder if it is still absent", not "restart it": the request
            // can queue behind a reattach whose discovery already established
            // the folder on the replacement, and a restart would delete that
            // healthy cursor - stranding the scope if the re-establishment
            // then failed. Nor is it fenced by account generation: it states
            // a fact about the server (the folder exists), and the
            // replacement's discovery may have run before the folder was
            // created. Deletion is ordered by the channel itself: a later
            // `Deleted` queues its `ScopeDeleted` behind this request, and the
            // listener reads the lifecycle epoch only when it handles it. An
            // epoch read here would predate the listener's own bump for a
            // deletion queued ahead, and revoke a folder deleted and then
            // created again.
            let _ = reopen_tx.send(ReopenRequest::ScopeCreated { scope }).await;
        }
    }

    match lifecycle {
        ScopeLifecycle::Created(membership) => {
            let scopes = membership_to_cursor_scopes(cursors, &membership);
            establish_scopes(reopen_tx, scopes).await;
        }
        ScopeLifecycle::Deleted(membership) => {
            delete_scopes(cursors, control, tokens, reopen_tx, &membership).await;
        }
        ScopeLifecycle::Renamed { old, new, .. } if old == new => {
            // A display-name change on a stable id. Every cursor scope is
            // keyed by id, so no cursor, durable row, completion marker,
            // membership edge, parked repair or lifecycle epoch is invalidated
            // by it; the delete half would only purge valid progress and force
            // a cold-start re-walk of the folder. The create half still runs:
            // establish-if-absent leaves a registered scope untouched, and
            // establishes one whose earlier establishment never landed.
            let scopes = membership_to_cursor_scopes(cursors, &new);
            establish_scopes(reopen_tx, scopes).await;
        }
        ScopeLifecycle::Renamed { old, new, .. } => {
            // The new id's scopes come from the remembered account model, which
            // the delete half does not shrink: deleting the old id's cursors
            // can remove the last registered cursor of a shape and the create
            // half still finds it. Only scope identities are captured; the
            // listener still reads each epoch when it handles the request,
            // after the queued `ScopeDeleted`.
            let scopes = membership_to_cursor_scopes(cursors, &new);
            delete_scopes(cursors, control, tokens, reopen_tx, &old).await;
            establish_scopes(reopen_tx, scopes).await;
        }
        _ => {}
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
