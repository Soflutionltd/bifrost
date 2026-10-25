//! Webhook maintenance sessions and account-owned recreate effects.

use std::collections::HashMap;
use std::time::Duration;

use bifrost_types::{AccountOperation, CursorScope, ErrorScope, SubscriptionHandle};

use crate::account::GraphAccount;
use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::webhooks::{
    ExpiryCheck, check_expiry, create_subscription, delete_subscription, renew_subscription,
    subscription_is_gone,
};

use super::common::PushEndpoint;
use super::webhook::{
    GraphSubscriptionGroup, GraphSubscriptionState, mark_subscription_terminated, recreated_ids,
};

pub(crate) const RENEWAL_CHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);
pub(super) const RENEWAL_THRESHOLD_MINUTES: i64 = 30;

pub(crate) fn renewal_session(
    account: GraphAccount,
    stop: bifrost_types::SessionStop,
    gate: bifrost_types::PushGate,
) -> bifrost_types::AccountStream<bifrost_types::PushSessionEvent> {
    use bifrost_types::{PushSessionEnd as End, PushSessionEvent as E};
    Box::pin(async_stream::stream! {
        let Some(_lease) = account.push_lease.try_claim() else { yield E::Failed(bifrost_types::push_lane_contract_error(bifrost_types::Protocol::Graph, bifrost_types::PushLane(0), "already driven")); return; };
        let mut topology = account.graph_topology.subscribe();
        let mut established = false;
        loop {
            while let Some(scope) = pop_gap(&account) { yield E::Invalidated(gap_hint(scope)); }
            topology.borrow_and_update();
            let live = { let groups = account.graph_subscriptions.read().await; has_live_graph_subscription_group(&groups) };
            if !live {
                tokio::select! { () = stop.stopped() => break, () = account.shutdown.cancelled() => break, _ = topology.changed() => {} }
                continue;
            }
            if !established { established = true; yield E::Established; }
            let due = { let mut groups = account.graph_subscriptions.write().await; due_renewals(&mut groups) };
        // Whether any row in this pass failed non-terminally. Every row is
        // attempted once regardless (a failing row must not starve its
        // siblings); the pass then ends `Incomplete` instead of `Healthy`.
        let mut had_error = false;
        for DueRenewal {
            handle,
            server_id,
            resource,
            scopes,
        } in due
        {
            tokio::select! { () = stop.stopped() => { yield E::Ended(End::Stopped); return; }, () = account.shutdown.cancelled() => { yield E::Ended(End::Stopped); return; }, () = gate.clear() => {} }
            let renewal = tokio::select! {
                biased;
                () = account.shutdown.cancelled() => { yield E::Ended(End::Stopped); return; },
                () = stop.stopped() => { yield E::Ended(End::Stopped); return; },
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
                                    // notifications for that window. The
                                    // install queued the row's scopes on
                                    // `pending_gaps` under the same lock,
                                    // and the drain after this pass (or the
                                    // next session, if this one is dropped)
                                    // invalidates them.
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
                    // Every failure is reported to the engine as
                    // `UnitFailed`, which owns its throttle, directive or
                    // terminal broadcast. The structured telemetry is
                    // logged here too so dashboards can group on kind /
                    // message-key.
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
                        // teardown, but stop every later pass from issuing the
                        // same doomed PATCH and POST.
                        // Flagged, not removed: renewing stops, but teardown
                        // must still be able to DELETE it.
                        mark_subscription_terminated(&account, &handle, &server_id).await;

                    }
                    had_error |= !account_error.recovery().is_terminal();
                    yield E::UnitFailed(account_error);
                }
            }
        }

        while let Some(scope) = pop_gap(&account) { yield E::Invalidated(gap_hint(scope)); }
        let retired = { let groups = account.graph_subscriptions.read().await; groups.values().any(|g| !g.tearing_down && g.subscriptions.iter().any(|s| s.terminated)) };
        if had_error || retired { yield E::Ended(End::Incomplete); return; }
        yield E::Healthy;
        tokio::select! { () = stop.stopped() => break, () = account.shutdown.cancelled() => break, () = tokio::time::sleep(RENEWAL_CHECK_INTERVAL) => {} }
        }
        yield E::Ended(End::Stopped);
    })
}
fn pop_gap(account: &GraphAccount) -> Option<CursorScope> {
    account
        .pending_gaps
        .lock()
        .expect("push gap lock")
        .pop_front()
}
/// A recreated row's coverage gap: one precise scope, so it names the Graph
/// subscription source rather than the wide-wake-up `Coalesced`.
fn gap_hint(scope: CursorScope) -> bifrost_types::InvalidationHint {
    bifrost_types::InvalidationHint {
        source: bifrost_types::PushSource::GraphSubscription,
        payload: bifrost_types::HintPayload::SpecificCursorScope(scope),
    }
}

/// Whether any registered subscription still needs renewing. A condemned
/// group and a `terminated` row both stay registered for teardown's sake and
/// both need nothing from a renewal pass, so with only those left a session
/// parks instead of running one.
pub(super) fn has_live_graph_subscription_group(
    groups: &HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
) -> bool {
    groups.values().any(|group| {
        !group.tearing_down && group.subscriptions.iter().any(|state| !state.terminated)
    })
}

/// One subscription inside the renewal threshold, with everything a pass
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

/// What happened to a subscription a renewal pass had to recreate.
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
/// The account owns the create, install and undo task. Dropping its session
/// only drops the reply receiver; committed gap obligations remain queued for
/// the next session. Close awaits these effects under its bounded join and
/// detaches unfinished tasks instead of aborting them.
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
    let (reply, response) = tokio::sync::oneshot::channel();
    let owner = account.clone();
    let task = tokio::spawn(async move {
        let _ = reply.send(create_and_install_replacement(&owner, request).await);
    });
    crate::account::push_tasks::track(&account, task);
    response.await.unwrap_or_else(|_| {
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
    let gap_scopes = replacement.scopes.clone();

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
        account
            .pending_gaps
            .lock()
            .expect("push gap lock")
            .extend(gap_scopes);
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
///   never an `entry().or_insert_with()`: a pass's due list is a
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
