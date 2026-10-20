//! The webhook renewal health worker: the tick loop that re-issues expiring
//! Graph subscriptions, recreates the ones Graph has already dropped, emits
//! `Disconnected` / `Reconnected` / `Terminated` on the push channel, and
//! retires its own worker slot when nothing is left to renew.

use std::collections::HashMap;
use std::time::Duration;

use bifrost_types::WatchEvent;
use bifrost_types::{AccountOperation, CursorScope, ErrorScope, SubscriptionHandle};

use crate::account::GraphAccount;
use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::webhooks::{
    ExpiryCheck, check_expiry, create_subscription, delete_subscription, renew_subscription,
    subscription_is_gone,
};

use super::common::{
    PushEndpoint, announce_push_recovered, mark_push_disconnected, mark_push_reconnected,
};
use super::webhook::{
    GraphSubscriptionGroup, GraphSubscriptionState, mark_subscription_terminated, recreated_ids,
};

pub(super) const RENEWAL_CHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);
pub(super) const RENEWAL_THRESHOLD_MINUTES: i64 = 30;

pub(super) async fn run_graph_subscription_worker(account: GraphAccount) {
    // The health latch is the ACCOUNT's, not this worker's. `subscribe_graph`
    // reads the same flag to decide whether its own `Reconnected` is owed, and
    // a worker-local copy let a subscribe during a degraded period announce a
    // recovery this worker still believed had not happened (and, before the
    // latch existed at all, announce one on every first subscribe).
    loop {
        tokio::select! {
            () = account.shutdown.cancelled() => return,
            () = tokio::time::sleep(RENEWAL_CHECK_INTERVAL) => {}
        }

        let due = {
            let mut groups = account.graph_subscriptions.write().await;
            if !has_live_graph_subscription_group(&groups) {
                // Retire the slot BEFORE releasing the guard that made this
                // decision. A concurrent `push_subscribe` cannot install its
                // live group until this guard drops, so by the time its
                // `ensure_graph_worker` runs the slot is empty and it spawns a
                // replacement. Observing the emptiness here and letting the
                // task merely run out instead left a window where the new
                // subscription saw a still-unfinished `JoinHandle`, declined
                // to spawn, and was never renewed until some later subscribe.
                retire_graph_worker_slot(&account).await;
                return;
            }
            due_renewals(&mut groups)
        };

        let mut had_error = false;
        // Whether this tick recreated at least one vanished subscription, and
        // so owes the engine a reconcile for the window its resource had no
        // subscription at all. ONE per tick, however many it recreated:
        // `Reconnected` is account-wide (a `Coalesced` / `HintPayload::Unknown`
        // invalidation over every registered scope), so the second and later
        // emissions of a tick reconcile exactly what the first already did -
        // and a tick that finds several subscriptions vanished is precisely
        // the case where the redundant reconciles cost the most.
        let mut recovered_gap = false;
        for DueRenewal {
            handle,
            server_id,
            resource,
            scopes,
        } in due
        {
            // Cooperative cancellation, checked first (`biased`) at every due
            // row: once `close()` has cancelled the token no further request
            // is issued, and a PATCH already in flight is dropped. Dropping a
            // renewal is harmless - it creates nothing, and the row it was
            // extending is being deleted. The step that DOES create server
            // state, the recreate below, runs in its own task and so is not
            // affected by this worker being cancelled, joined or aborted.
            let renewal = tokio::select! {
                biased;
                () = account.shutdown.cancelled() => return,
                result = renew_subscription(&account.client, &server_id, None) => result,
            };
            match renewal {
                Ok(new_expiry) => {
                    let mut groups = account.graph_subscriptions.write().await;
                    if let Some(group) = groups.get_mut(&handle)
                        && let Some(state) = group
                            .subscriptions
                            .iter_mut()
                            .find(|state| state.server_id == server_id)
                    {
                        state.expires_at = new_expiry;
                    }
                }
                Err(error) => {
                    // Graph retains no deleted subscription to PATCH, so a
                    // vanished (404/410) subscription is replaced by a fresh
                    // create for the same resource; otherwise every renewal
                    // tick retries a permanent 404. The stale state stays
                    // installed until the replacement is in hand: dropping it
                    // first meant a failed create left the resource with no
                    // state at all, so no later tick could ever see it as due
                    // and coverage was lost until reopen.
                    let (phase, error) = match account.push_endpoint.as_ref() {
                        Some(endpoint) if subscription_is_gone(&error) => {
                            match replace_gone_subscription(
                                &account, endpoint, &handle, &server_id, &resource, &scopes,
                            )
                            .await
                            {
                                Ok(Replacement::Installed) => {
                                    // The resource had NO live subscription
                                    // between its disappearance and this
                                    // create, and Graph does not replay
                                    // notifications for that window.
                                    // `Reconnected` is the only event the
                                    // engine turns into a full reconcile
                                    // across every registered scope, so
                                    // without it the changes missed while the
                                    // subscription was absent wait for the
                                    // ordinary poll interval.
                                    recovered_gap = true;
                                    continue;
                                }
                                Ok(
                                    Replacement::HandleUnsubscribed | Replacement::AccountClosed,
                                ) => {
                                    continue;
                                }
                                // Classify the failure that actually blocks
                                // recovery. The original 404 only says the
                                // old subscription is gone, which is already
                                // known and acted on; the create error is the
                                // one whose recovery class decides whether
                                // this is worth another tick.
                                Err(recreate_error) => ("recreate", recreate_error),
                            }
                        }
                        _ => ("renew", error),
                    };
                    // Name what lost coverage. A resource covers exactly one
                    // scope in every ordinary case (each resource string is
                    // built from one folder or calendar id), so the common
                    // path gets precise attribution; the ambiguous case -
                    // `Event` and `CalendarEvent` scopes over one calendar -
                    // stays account-scoped rather than picking a scope
                    // arbitrarily, and the covered list is logged either way.
                    let context = GraphErrorContext::graph(AccountOperation::PushSubscribe);
                    let context = match scopes.as_slice() {
                        [only] => context.with_scope(ErrorScope::Cursor(only.clone())),
                        _ => context,
                    };
                    let account_error = into_account_error(error, context);
                    // Terminal recovery classes (AuthLost,
                    // NeedsPolicyChange, NoPermission, etc.) cannot
                    // be recovered without engine intervention.
                    // Surface them on the push channel so the engine
                    // tears the subscription down. Retryable classes
                    // ride out the next renewal tick - we log
                    // structured telemetry instead of formatted text
                    // so the dashboard can group on kind / message-key.
                    {
                        let telemetry = account_error.telemetry_fields();
                        tracing::warn!(
                            target: "bifrost_graph::webhooks",
                            provider = ?telemetry.provider,
                            protocol = ?telemetry.protocol,
                            message_key = telemetry.message_key,
                            recovery = telemetry.recovery_discriminant,
                            server_id = %server_id,
                            phase = phase,
                            scopes = ?scopes,
                            "Graph webhook renewal failed"
                        );
                    }
                    if account_error.recovery().is_terminal() {
                        // A 404 from the replacement create is a permanent
                        // refusal for this resource (for example, its folder
                        // was deleted). Keep the old row reachable by
                        // teardown, but stop the worker from issuing the same
                        // doomed PATCH and POST on every tick.
                        // Flagged, not removed: renewing stops, but teardown
                        // must still be able to DELETE it.
                        mark_subscription_terminated(&account, &handle, &server_id).await;
                        let _ = account.push_tx.send(WatchEvent::Terminated(account_error));
                    }
                    had_error = true;
                }
            }
        }

        // Order matters when a tick both recreated one subscription and failed
        // another: the recovery is announced first (it names a gap that really
        // happened and clears the latch), then the health latch is raised for
        // the resource that is still failing. That is the same event sequence
        // the per-recreate emission produced, minus its duplicates.
        if recovered_gap {
            announce_push_recovered(&account);
        }
        if had_error {
            mark_push_disconnected(&account);
        } else {
            mark_push_reconnected(&account);
        }
    }
}

/// Whether any registered subscription still needs renewing. A condemned
/// group and a `terminated` row both stay registered for teardown's sake and
/// both need nothing from the worker, so neither keeps it alive.
pub(super) fn has_live_graph_subscription_group(
    groups: &HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
) -> bool {
    groups.values().any(|group| {
        !group.tearing_down && group.subscriptions.iter().any(|state| !state.terminated)
    })
}

/// Clear the worker slot on the way out of `run_graph_subscription_worker`,
/// so `ensure_graph_worker` starts a fresh worker for the next subscription.
/// See `worker_slot` for the ordering rule this call is one half of; the
/// caller must still hold the `graph_subscriptions` guard that decided to
/// exit.
async fn retire_graph_worker_slot(account: &GraphAccount) {
    crate::account::worker_slot::retire_worker_slot(&account.graph_worker).await;
}

/// One subscription inside the renewal threshold, with everything the worker
/// needs to renew it, recreate it, or report what it covered.
#[derive(Debug, Clone)]
pub(super) struct DueRenewal {
    pub(super) handle: SubscriptionHandle,
    pub(super) server_id: String,
    pub(super) resource: String,
    pub(super) scopes: Vec<CursorScope>,
}

/// The subscriptions inside the renewal threshold.
///
/// A condemned (`tearing_down`) group contributes nothing. Renewing its
/// subscriptions would extend the life of exactly what the caller asked to
/// delete, and recreating a vanished one would hand teardown a server id its
/// snapshot cannot contain. Their rows stay registered only so that a failed
/// DELETE can be retried against them.
pub(super) fn due_renewals(
    groups: &mut HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
) -> Vec<DueRenewal> {
    let mut due = Vec::new();
    for (handle, group) in groups.iter_mut() {
        if group.tearing_down {
            continue;
        }
        for state in &mut group.subscriptions {
            // Its renewal failed terminally; renewing or recreating it again
            // would repeat the failure every tick. It waits for teardown.
            if state.terminated {
                continue;
            }
            match check_expiry(&state.expires_at, RENEWAL_THRESHOLD_MINUTES) {
                ExpiryCheck::Live => continue,
                ExpiryCheck::ExpiringSoon => {}
                ExpiryCheck::Unparseable => {
                    if !state.warned_unparseable_expiry {
                        state.warned_unparseable_expiry = true;
                        tracing::warn!(
                            subscription = state.server_id,
                            expiration = state.expires_at,
                            "[Graph webhooks] Unparseable subscription expiry; treating as due for renewal"
                        );
                    }
                }
            }
            due.push(DueRenewal {
                handle: handle.clone(),
                server_id: state.server_id.clone(),
                resource: state.resource.clone(),
                scopes: state.scopes.clone(),
            });
        }
    }
    due
}

/// What happened to a subscription the renewal worker had to recreate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Replacement {
    /// The fresh subscription took the vanished one's place in the group.
    Installed,
    /// The handle stopped being registered while the create was in flight,
    /// so the fresh subscription was deleted again instead of installed.
    HandleUnsubscribed,
    /// The account was already closed (or closing), so nothing was created.
    AccountClosed,
}

/// Everything the replacement task owns.
struct ReplaceRequest {
    endpoint: PushEndpoint,
    handle: SubscriptionHandle,
    stale_server_id: String,
    resource: String,
    scopes: Vec<CursorScope>,
}

/// Create a replacement for a vanished subscription and install it under
/// `handle`, or undo the create when the handle is no longer registered.
///
/// The create happens before any local state changes so a failure leaves the
/// stale entry in place for the next renewal tick to retry - the resource
/// keeps a row that reads as due rather than silently losing coverage.
///
/// The create, the install and the undo run in a task the ACCOUNT's runtime
/// owns; this future only awaits it. The renewal worker is cancelled by
/// `close()`, aborted by `unsubscribe_graph` when the last group goes, and
/// dropped by the runtime, and an inline body was torn down wherever it stood:
/// after the create returned and before the new id was recorded, or in the
/// middle of the undo DELETE, which stranded a live server subscription no
/// group knew about. A spawned task is not torn down by its awaiter going
/// away, and it has no hand-off to acknowledge: the install is its own effect
/// (there is no caller holding a handle to it), so a lost result costs only
/// the `Reconnected` the worker would have announced, and the worker is only
/// ever cancelled when the account is closing or the handle is being retired.
///
/// What cannot be made safe: a create whose response never arrived may have
/// succeeded on Graph and its id is unknowable (see `run_subscribe` in
/// `webhook.rs`); it lives until Graph's own expiry.
pub(super) async fn replace_gone_subscription(
    account: &GraphAccount,
    endpoint: &PushEndpoint,
    handle: &SubscriptionHandle,
    stale_server_id: &str,
    resource: &str,
    scopes: &[CursorScope],
) -> Result<Replacement, crate::error::GraphError> {
    let account = account.clone();
    let request = ReplaceRequest {
        endpoint: endpoint.clone(),
        handle: handle.clone(),
        stale_server_id: stale_server_id.to_string(),
        resource: resource.to_string(),
        scopes: scopes.to_vec(),
    };
    tokio::spawn(async move { create_and_install_replacement(&account, request).await })
        .await
        .unwrap_or_else(|_| {
            // The task panicked or the runtime is shutting down under it.
            Err(crate::error::GraphError::RuntimeFailure {
                message: "the webhook replacement task ended without an answer".to_string(),
            })
        })
}

async fn create_and_install_replacement(
    account: &GraphAccount,
    request: ReplaceRequest,
) -> Result<Replacement, crate::error::GraphError> {
    let ReplaceRequest {
        endpoint,
        handle,
        stale_server_id,
        resource,
        scopes,
    } = request;
    // A closed account must not gain server-side state. The registration
    // check under the write lock below is what covers a create already in
    // flight when `close()` began; this one only spares the common case a
    // POST that could only be rolled back.
    if account.shutdown.is_cancelled() {
        return Ok(Replacement::AccountClosed);
    }
    let response = create_subscription(
        &account.client,
        &resource,
        &endpoint.webhook_url,
        &endpoint.client_state,
        None,
    )
    .await?;
    let replacement = GraphSubscriptionState {
        server_id: response.id,
        expires_at: response.expiration_date_time,
        resource,
        scopes,
        warned_unparseable_expiry: false,
        terminated: false,
    };
    let created_id = replacement.server_id.clone();

    let mut groups = account.graph_subscriptions.write().await;
    // Checked under the write lock and paired with `retire_all_graph_
    // subscriptions` cancelling the token BEFORE its walk, exactly like the
    // registration in `create_and_register`: either this install lands before
    // the cancel (and the walk, which snapshots under the same lock
    // afterwards, sees it) or it observes the cancel and is undone. The group's
    // `tearing_down` marker refuses the same install once the walk reaches
    // its handle; this check keeps the rule local to the create instead of
    // resting on the walk's ordering.
    let installed = !account.shutdown.is_cancelled()
        && install_replacement(&mut groups, &handle, &stale_server_id, replacement);
    if installed {
        // The handle string cannot name the new id, so record it where an
        // orphan teardown on a reopened instance will look; see
        // `RecreatedSubscriptionIds`. Under the same write lock as the
        // install, so a teardown snapshot never sees one without the other.
        record_recreated_id(
            &mut recreated_ids(account),
            &handle,
            &stale_server_id,
            &created_id,
        );
    }
    drop(groups);
    if installed {
        return Ok(Replacement::Installed);
    }

    // `push_unsubscribe` retired the handle, or condemned it and is walking
    // a server-id snapshot that cannot contain this create, while the create
    // was in flight. Either way it deletes every subscription it knows about
    // and reports teardown as successful, so installing this one would keep
    // notifications flowing for a subscription the caller believes is gone -
    // and re-registering the group would resurrect a handle nothing will
    // ever tear down again. Delete the subscription we just minted instead.
    // Best effort: the caller's `push_unsubscribe` may already have
    // returned, so there may be nobody left to report a cleanup failure to.
    if let Err(cleanup_error) = delete_subscription(&account.client, &created_id).await {
        tracing::warn!(
            target: "bifrost_graph::webhooks",
            server_id = %created_id,
            error = ?cleanup_error,
            "failed to delete a Graph webhook subscription recreated for an unsubscribed handle"
        );
    }
    Ok(Replacement::HandleUnsubscribed)
}

/// Note `created_id` as recreated under `handle`, in place of
/// `stale_server_id` when that was itself an earlier recreate. A stale id the
/// handle names stays named there; its DELETE answers 404, which teardown
/// counts as already gone.
pub(super) fn record_recreated_id(
    ledger: &mut HashMap<SubscriptionHandle, Vec<String>>,
    handle: &SubscriptionHandle,
    stale_server_id: &str,
    created_id: &str,
) {
    let ids = ledger.entry(handle.clone()).or_default();
    ids.retain(|id| id != stale_server_id);
    ids.push(created_id.to_string());
}

/// Swap `replacement` in for `stale_server_id` under `handle`.
///
/// Returns `false` when the replacement must not be installed, in which case
/// the caller deletes the subscription it just minted. Two refusals:
///
/// - `handle` is no longer registered. The lookup is a plain `get_mut`,
///   never an `entry().or_insert_with()`: the worker's due list is a
///   snapshot, and a concurrent `push_unsubscribe` can retire the handle
///   before the replacement lands. Re-creating the group there would
///   contradict a teardown the caller was already told succeeded.
/// - the group is registered but marked `tearing_down`. Teardown keeps the
///   group registered while it deletes each server subscription, so
///   "registered" no longer implies "live". It walks a snapshot of the
///   server ids taken when the marker went up, so a replacement installed
///   after that point is invisible to it: teardown would delete the stale
///   id, retire the group, return success - and the replacement would keep
///   delivering notifications for a handle the caller believes is gone.
pub(super) fn install_replacement(
    groups: &mut HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
    handle: &SubscriptionHandle,
    stale_server_id: &str,
    replacement: GraphSubscriptionState,
) -> bool {
    let Some(group) = groups.get_mut(handle) else {
        return false;
    };
    if group.tearing_down {
        return false;
    }
    group
        .subscriptions
        .retain(|state| state.server_id != stale_server_id);
    group.subscriptions.push(replacement);
    true
}
