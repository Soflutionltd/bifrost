//! The Graph `/subscriptions` webhook arm: per-scope resource construction,
//! subscription creation with rollback, the registered `GraphSubscriptionGroup`
//! state, and teardown (`push_unsubscribe` plus `close()`'s best-effort walk).
//!
//! The renewal session that keeps these subscriptions alive lives in
//! `super::renewal`.

use std::collections::HashMap;

use bifrost_types::{
    AccountError, AccountOperation, CursorScope, ErrorScope, ObjectType, SubscriptionHandle,
};

use crate::account::GraphAccount;
use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::webhooks::{create_subscription, delete_subscription};

use super::common::{
    DecodedHandle, account_closed_error, decode_handle, graph_handle, new_handle_token,
    unsupported_push_error, unsupported_push_scope_error,
};
use super::dispatch::{ArmOutcome, finalize_push_outcomes};

/// Server ids the renewal session recreated under a handle, which the handle
/// string itself cannot name.
///
/// A handle carries the ids of its subscribe time (`common::graph_handle`),
/// and the engine stores that string unchanged. When the renewal session
/// replaces a vanished subscription, the replacement's id lives only in the
/// instance's group - so if that instance's teardown fails and the engine
/// retries the handle as an orphan on a reopened instance, the replacement
/// would be out of reach and live until Graph expired it. The engine reopens
/// through the same factory, and the factory hands every account it opens
/// this one ledger, so the orphan path on the new instance finds them here.
///
/// Process-local, like the engine's own subscription registry: nothing
/// retries a handle across a process restart, and a handle that outlives the
/// process reaches only the ids it was minted with.
///
/// An id enters when a recreate installs it and leaves only when a DELETE of
/// it succeeds (or a whole orphan teardown does) - never when a terminal
/// renewal failure drops its group row, since that deletes nothing. Every
/// write and the teardown snapshot happen under the `graph_subscriptions`
/// write lock, so the two never disagree. Never held across an await.
pub(crate) type RecreatedSubscriptionIds =
    std::sync::Arc<std::sync::Mutex<HashMap<SubscriptionHandle, Vec<String>>>>;

/// Lock the recreated-id ledger, tolerating poison: every write leaves the
/// map consistent, so a panic elsewhere does not invalidate it.
pub(super) fn recreated_ids(
    account: &GraphAccount,
) -> std::sync::MutexGuard<'_, HashMap<SubscriptionHandle, Vec<String>>> {
    account
        .recreated_subscription_ids
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Clone)]
pub(crate) struct GraphSubscriptionGroup {
    pub(crate) subscriptions: Vec<GraphSubscriptionState>,
    /// Set the moment `push_unsubscribe` starts deleting this group, and
    /// never cleared - teardown intent is monotone per handle, because a
    /// later `push_subscribe` mints a fresh handle rather than reviving
    /// this one.
    ///
    /// Teardown deletes over the network, so the group must stay registered
    /// across those awaits for a failed DELETE to remain retryable. That
    /// keeps the handle visible to the renewal session, whose recreate path
    /// would otherwise install a replacement into a group teardown has
    /// already snapshotted: the removal that follows only knows the stale
    /// server id, so the replacement would stay registered and live while
    /// `push_unsubscribe` reported success. The marker is a plain flag
    /// rather than a lock because the decision it drives is local and
    /// synchronous - serializing the two paths on a mutex would hold it
    /// across DELETE and create round trips.
    pub(crate) tearing_down: bool,
}

impl GraphSubscriptionGroup {
    /// A freshly registered group, not yet being torn down.
    pub(crate) fn live(subscriptions: Vec<GraphSubscriptionState>) -> Self {
        Self {
            subscriptions,
            tearing_down: false,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GraphSubscriptionState {
    pub(crate) server_id: String,
    pub(crate) expires_at: String,
    pub(crate) resource: String,
    /// The cursor scopes this one subscription covers.
    ///
    /// Retained so a terminal renewal failure can name what lost coverage.
    /// `subscribe_graph` groups by resource string and used to discard the
    /// scopes it grouped, leaving `Terminated` with no scope attribution at
    /// all - the engine could see that push broke without being able to tell
    /// which scopes to fall back to polling. Every other error path in this
    /// crate carries an `ErrorScope`.
    pub(crate) scopes: Vec<CursorScope>,
    /// Whether the renewal session has already warned about an unparseable
    /// `expires_at` for this subscription, so the warning fires once per
    /// subscription rather than once per renewal tick.
    pub(crate) warned_unparseable_expiry: bool,
    /// A renewal of this subscription failed terminally (auth lost,
    /// permission revoked and the like), so the renewal session no longer
    /// renews or recreates it. The row stays registered: a subscription we
    /// stopped renewing is not one that stopped existing, and `close()` and
    /// `push_unsubscribe` still DELETE it. Removing the row instead took a
    /// group's last subscription out of `close()`'s reach while Graph still
    /// held it (ruled 2026-10-04).
    pub(crate) terminated: bool,
}

/// The webhook arm of `push_subscribe`: create one server subscription per
/// resource, close the ledger, and only then register and start renewing.
///
/// The ledger is closed HERE, between the last create and the registration,
/// not by the dispatcher afterwards. `finalize` is fallible (an unfiled or
/// double-filed lane id is this crate's invariant breaking), and it used to
/// run after the group was registered and the renewal session started: a
/// failure returned `Err` - no handle - over subscriptions the sessions kept
/// renewing and no caller could ever name to unsubscribe. Closed before the
/// registration, the failure path has registered nothing and only has to
/// roll back the server-side creates, exactly as a failed create does.
///
/// The creates, rollback and registration run in a spawned task so a dropped
/// caller cannot strand them; see the comments at the spawn and on
/// `run_subscribe`.
pub(super) async fn subscribe_graph(
    account: GraphAccount,
    eligible: Vec<(bifrost_types::BatchItemId, CursorScope)>,
    mut outcomes: bifrost_types::BatchOutcomeBuilder<CursorScope>,
    expected: &[bifrost_types::BatchItemId],
) -> Result<ArmOutcome, AccountError> {
    let Some(endpoint) = account.push_endpoint.clone() else {
        return Err(unsupported_push_error());
    };
    // Graph webhooks reject any subscription whose `resource` we cannot
    // construct. A scope with no resource is refused into the failed lane,
    // where the caller sees exactly which scope was declined and keeps
    // polling coverage for it, while its subscribable siblings still get a
    // live subscription.
    let mut grouped: HashMap<String, Vec<(bifrost_types::BatchItemId, CursorScope)>> =
        HashMap::new();
    for (item, scope) in eligible {
        match resource_for_scope(&account, &scope) {
            Ok(Some(resource)) => {
                grouped.entry(resource).or_default().push((item, scope));
            }
            Ok(None) => outcomes.push_failed(item, unsupported_push_scope_error(scope)),
            Err(error) => {
                // A stale shared mailbox has no subscribable resource.
                outcomes.push_failed(
                    item,
                    into_account_error(
                        error,
                        GraphErrorContext::graph(AccountOperation::PushSubscribe)
                            .with_scope(ErrorScope::Cursor(scope)),
                    ),
                );
            }
        }
    }
    if grouped.is_empty() {
        return Ok((None, finalize_push_outcomes(outcomes, expected)?));
    }

    // Mint the handle's random token BEFORE the first create.
    // `new_handle_token` is fallible (the host entropy source can fail; that
    // is `Internal(RuntimeFailure)`, never retried), and a failure here has
    // written nothing. Minting it after the creates would return an error
    // with live server-side subscriptions behind it and no handle any
    // teardown could name them by, orphaning them until they expire. The
    // handle itself is assembled after the creates, because it embeds the
    // server ids they return (see `common::decode_handle`).
    let token = new_handle_token()?;

    // A closed account must not gain server-side state. `close()` walks the
    // registered groups once; a create that started after that walk could
    // only ever be rolled back. Refusing here, before the first POST, keeps
    // the common case (subscribing on a dead account) from touching the
    // server at all; the registration step re-checks under the write lock
    // for the create that was already in flight when `close()` began.
    if account.shutdown.is_cancelled() {
        return Err(account_closed_error());
    }

    // Everything from here on runs in a task the ACCOUNT's runtime owns, and
    // this future only waits on it. `push_subscribe` is a caller-visible
    // future and callers drop those (a timeout, a `select!`, an aborted
    // reattach); dropped mid-way, a body that ran inline would strand every
    // server-side create made so far, skip the remaining rollback DELETEs, or
    // leave a registered group whose handle nobody received. A spawned task
    // is not torn down by its awaiter going away, so the creates, the
    // rollback and the registration each run to completion.
    //
    // Nothing is spawned, and nothing has been written, until this future is
    // first polled and reaches this point, so a future dropped before its
    // first poll has nothing to leak: the guard is the spawn itself, not an
    // object constructed inside the async body.
    //
    // The hand-off is two-phase because a delivered result is not a received
    // one: a `oneshot` send succeeds while the receiver is alive even if the
    // receiver's future is then dropped without ever being polled again, and
    // the value dies with it. The waiter therefore acks, in the same
    // synchronous step that takes the result, and the task treats a missing
    // ack (or a failed send) as "the handle was never received" and tears the
    // registered group down. Exactly one side ever cleans up.
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(run_subscribe(
        account,
        SubscribeRequest {
            endpoint,
            grouped,
            token,
            outcomes,
            expected: expected.to_vec(),
        },
        result_tx,
        ack_rx,
    ));
    let Ok(result) = result_rx.await else {
        // The task ended without answering: it panicked, or the runtime is
        // shutting down under it. Nothing more can be done from here.
        return Err(into_account_error(
            crate::error::GraphError::RuntimeFailure {
                message: "the push_subscribe task ended without an answer".to_string(),
            },
            GraphErrorContext::graph(AccountOperation::PushSubscribe),
        ));
    };
    // No await between taking the result and acking it: there is no drop
    // point between "received" and "acknowledged".
    let _ = ack_tx.send(());
    result
}

/// Everything the spawned subscribe task owns.
struct SubscribeRequest {
    endpoint: super::common::PushEndpoint,
    grouped: HashMap<String, Vec<(bifrost_types::BatchItemId, CursorScope)>>,
    /// The random part of the handle, minted before any create.
    token: String,
    outcomes: bifrost_types::BatchOutcomeBuilder<CursorScope>,
    expected: Vec<bifrost_types::BatchItemId>,
}

/// The spawned half of the webhook `push_subscribe`: create, register, and
/// hand the result to the waiter, tearing the group down again if the waiter
/// never took it.
///
/// What cannot be made safe, and why:
/// - A create whose response never arrived (a timeout, a reset) may have
///   succeeded on Graph. The subscription id exists only in the response we
///   did not read, so there is nothing to DELETE by; it lives until Graph's
///   own expiry (about a day) and posts to the receiver meanwhile. Recovering
///   it would mean listing `/subscriptions` and deleting by resource and
///   notification URL, which could also delete another process's identical
///   subscription; that is a product decision, not something to do silently
///   here.
/// - A process exit or runtime shutdown kills this task like any other, with
///   the same result for whatever it had created.
async fn run_subscribe(
    account: GraphAccount,
    request: SubscribeRequest,
    result_tx: tokio::sync::oneshot::Sender<Result<ArmOutcome, AccountError>>,
    ack_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let result = create_and_register(&account, request).await;
    // Only a success registered a group, and the success names its handle.
    let Some(handle) = result.as_ref().ok().and_then(|(handle, _)| handle.clone()) else {
        // Nothing registered, so nothing to retire; a dropped waiter has
        // nobody to tell.
        let _ = result_tx.send(result);
        return;
    };
    let received = result_tx.send(result).is_ok() && ack_rx.await.is_ok();
    if received {
        return;
    }
    // The group is registered and renewable, but no caller holds
    // the handle, so no `push_unsubscribe` will ever name it. Retire it the
    // way a caller would. If a DELETE fails the group stays registered
    // (marked `tearing_down`), which is exactly what lets `close()` retry it.
    if let Err(error) = unsubscribe_graph(account, handle).await {
        let telemetry = error.telemetry_fields();
        tracing::warn!(
            target: "bifrost_graph::push",
            message_key = telemetry.message_key,
            recovery = telemetry.recovery_discriminant,
            "could not retire a Graph webhook subscription whose subscribe was abandoned"
        );
    }
}

async fn create_and_register(
    account: &GraphAccount,
    request: SubscribeRequest,
) -> Result<ArmOutcome, AccountError> {
    let SubscribeRequest {
        endpoint,
        grouped,
        token,
        mut outcomes,
        expected,
    } = request;
    let expected = expected.as_slice();
    let mut subscriptions = Vec::new();
    for (resource, covered) in grouped {
        match create_subscription(
            &account.client,
            &resource,
            &endpoint.webhook_url,
            &endpoint.client_state,
            None,
        )
        .await
        {
            Ok(response) => {
                for (item, scope) in &covered {
                    outcomes.push_succeeded(item.clone(), scope.clone());
                }
                subscriptions.push(GraphSubscriptionState {
                    server_id: response.id,
                    expires_at: response.expiration_date_time,
                    resource,
                    scopes: covered.into_iter().map(|(_, scope)| scope).collect(),
                    warned_unparseable_expiry: false,
                    terminated: false,
                });
            }
            Err(error) => {
                // The handle is never registered on the account and never
                // returned on this path, so nothing downstream could ever
                // reach these subscriptions to tear them down. Retain none
                // of them. Cleanup is best effort: preserve the create
                // error the caller needs to act on even if a DELETE fails.
                //
                // This rolls back only what we hold ids for. THIS create may
                // itself have succeeded on Graph (a timeout after the server
                // acted): its id is unknowable, see `run_subscribe`.
                roll_back_created(account, &subscriptions).await;
                return Err(into_account_error(
                    error,
                    GraphErrorContext::graph(AccountOperation::PushSubscribe),
                ));
            }
        }
    }

    // Every lane must be filed before anything becomes live. See the
    // function doc: on this path nothing is registered and no session can
    // see the rows, so rolling back the creates is the whole cleanup.
    let outcomes = match finalize_push_outcomes(outcomes, expected) {
        Ok(outcomes) => outcomes,
        Err(error) => {
            roll_back_created(account, &subscriptions).await;
            return Err(error);
        }
    };

    let mut groups = account.graph_subscriptions.write().await;
    // Checked under the write lock, and paired with `retire_all_graph_
    // subscriptions` cancelling the token BEFORE it walks the groups: either
    // this insert lands before the cancel (and the walk, which needs the read
    // lock afterwards, sees and deletes it) or it sees the cancel here and
    // rolls back. Without the pairing a `close()` that walked the groups while
    // this task was mid-create would leave a registered group nobody retires.
    if account.shutdown.is_cancelled() {
        drop(groups);
        roll_back_created(account, &subscriptions).await;
        return Err(account_closed_error());
    }
    // The handle embeds the ids just created, so a reopened instance that
    // inherits it as an orphan can still DELETE them.
    let handle = graph_handle(
        &token,
        own_binding(account).as_deref(),
        subscriptions.iter().map(|state| state.server_id.as_str()),
    );
    groups.insert(handle.clone(), GraphSubscriptionGroup::live(subscriptions));
    drop(groups);
    // Wake a session parked without registrations after the committed insert.
    account
        .graph_topology
        .send_modify(|n| *n = n.wrapping_add(1));
    Ok((Some(handle), outcomes))
}

/// Best-effort DELETE of subscriptions this request created but will never
/// register or return a handle for.
///
/// Nothing downstream can reach these rows once the request fails - they are
/// in no group, so neither `push_unsubscribe` nor `close()` walks them - so a
/// DELETE that fails here leaves the subscription to Graph's own expiry.
/// Failures are logged, not returned: the caller needs the error that failed
/// the request, not the cleanup's.
async fn roll_back_created(account: &GraphAccount, subscriptions: &[GraphSubscriptionState]) {
    for subscription in subscriptions {
        if let Err(cleanup_error) =
            delete_subscription(&account.client, &subscription.server_id).await
        {
            tracing::warn!(
                target: "bifrost_graph::webhooks",
                server_id = %subscription.server_id,
                error = ?cleanup_error,
                "failed to roll back Graph webhook subscription"
            );
        }
    }
}

/// Delete the subscriptions a handle this instance does not know names.
///
/// The engine retries a failed teardown against whatever instance is current,
/// which after a reopen is a newer connection than the one that subscribed.
/// The handle carries the Graph ids (`common::decode_handle`), so any instance
/// can finish the job; 404/410 count as already gone (`delete_subscription`).
///
/// What this must not do is delete something a live subscription owns:
/// - it deletes ONLY the ids decoded from this handle, so a newer instance's
///   own subscriptions (different ids) are untouched by an older instance's
///   orphan;
/// - an id that this instance currently has registered under a group is
///   skipped, so a forged, stale or recycled handle cannot tear down a
///   subscription this instance still renews. The registered handle is the
///   only door to those.
///
/// Besides the ids the handle names, it deletes the ones the renewal session
/// recreated under it on an earlier instance (`RecreatedSubscriptionIds`),
/// and forgets those once every DELETE has succeeded.
///
/// Every id is attempted even when an earlier one fails, and the first
/// failure is returned: the handle is the engine's only record, and a retry
/// simply re-deletes (idempotent).
async fn unsubscribe_orphan(
    account: &GraphAccount,
    handle: &SubscriptionHandle,
    mut server_ids: Vec<String>,
) -> Result<(), AccountError> {
    if let Some(recreated) = recreated_ids(account).get(handle) {
        server_ids.extend(recreated.iter().cloned());
    }
    let owned: std::collections::HashSet<String> = account
        .graph_subscriptions
        .read()
        .await
        .values()
        .flat_map(|group| {
            group
                .subscriptions
                .iter()
                .map(|state| state.server_id.clone())
        })
        .collect();
    let mut first_error = None;
    for server_id in server_ids {
        if owned.contains(&server_id) {
            continue;
        }
        if let Err(error) = delete_subscription(&account.client, &server_id).await {
            first_error.get_or_insert_with(|| {
                into_account_error(
                    error,
                    GraphErrorContext::graph(AccountOperation::PushUnsubscribe),
                )
            });
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => {
            recreated_ids(account).remove(handle);
            Ok(())
        }
    }
}

/// The account binding this instance mints into and checks against handles:
/// its engine `AccountId`, when an `AccountNet` is attached (always, in
/// production). With none attached, a bound `graph2:` orphan matches nothing
/// and is left alone - the fail-safe direction, since this instance cannot
/// prove the handle is its own.
fn own_binding(account: &GraphAccount) -> Option<String> {
    account
        .client
        .account_net()
        .map(|net| super::common::account_binding(net.account()))
}

pub(super) async fn unsubscribe_graph(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    let Some(server_ids) = begin_graph_teardown(&account, &handle).await else {
        // Unknown to this instance: an orphan from an earlier connection (or
        // a handle already torn down here). The handle itself says what to
        // delete.
        return match decode_handle(&handle) {
            DecodedHandle::Graph {
                account: Some(bound),
                ..
            } if own_binding(&account).as_deref() != Some(bound.as_str()) => {
                // Minted by another account. Its ids are reachable with this
                // account's credentials when both sit in one tenant, so
                // deleting them would tear down a subscription this account
                // never owned. Left alone, like an unrecognized handle: this
                // instance has nothing of its own to retire.
                tracing::warn!(
                    target: "bifrost_graph::push",
                    "push_unsubscribe was handed a webhook handle minted by another account; \
                     nothing deleted"
                );
                Ok(())
            }
            DecodedHandle::Graph { ids, .. } => unsubscribe_orphan(&account, &handle, ids).await,
            DecodedHandle::Ews | DecodedHandle::Unrecognized => Ok(()),
        };
    };
    for server_id in server_ids {
        delete_subscription(&account.client, &server_id)
            .await
            .map_err(|e| {
                into_account_error(
                    e,
                    GraphErrorContext::graph(AccountOperation::PushUnsubscribe),
                )
            })?;
        forget_deleted_subscription(&account, &handle, &server_id).await;
    }
    account
        .graph_topology
        .send_modify(|n| *n = n.wrapping_add(1));
    Ok(())
}

/// Best-effort DELETE of every webhook subscription still registered, for
/// `close()`.
///
/// A Graph subscription lives on the server for up to its ~24h expiry and
/// keeps POSTing to the consumer's HTTPS receiver whether or not this process
/// still exists. Reopen builds a FRESH `GraphAccount` and the engine
/// resubscribes, so a `close()` that dropped the local map stranded one live
/// subscription per resource per reopen. Nothing in the `Account` contract
/// promises `push_unsubscribe` before `close()`, so `close()` has to do it.
///
/// Failures are logged, not returned: `close()` must still settle its
/// account-owned push tasks, and the engine has no useful
/// recovery for "the server kept a subscription we asked it to drop".
pub(crate) async fn retire_all_graph_subscriptions(account: &GraphAccount) {
    // Cancel BEFORE the walk: an in-flight `push_subscribe`, and a renewal
    // recreate's install, check the token under the same write lock they
    // register under, so a group or replacement they register either
    // precedes this cancel (and is walked below) or is refused and rolled
    // back. `close()` relies on this being its only cancel.
    account.shutdown.cancel();
    let handles: Vec<SubscriptionHandle> = account
        .graph_subscriptions
        .read()
        .await
        .keys()
        .cloned()
        .collect();
    for handle in handles {
        let Some(server_ids) = begin_graph_teardown(account, &handle).await else {
            continue;
        };
        for server_id in server_ids {
            match delete_subscription(&account.client, &server_id).await {
                Ok(()) => forget_deleted_subscription(account, &handle, &server_id).await,
                Err(error) => {
                    let error = into_account_error(
                        error,
                        GraphErrorContext::graph(AccountOperation::PushUnsubscribe),
                    );
                    let telemetry = error.telemetry_fields();
                    tracing::warn!(
                        target: "bifrost_graph::push",
                        message_key = telemetry.message_key,
                        recovery = telemetry.recovery_discriminant,
                        "close() could not retire a Graph webhook subscription"
                    );
                }
            }
        }
    }
}

async fn begin_graph_teardown(
    account: &GraphAccount,
    handle: &SubscriptionHandle,
) -> Option<Vec<String>> {
    let mut groups = account.graph_subscriptions.write().await;
    mark_group_tearing_down(&mut groups, handle)
}

/// Condemn one handle's group and snapshot the server ids teardown must
/// delete, without retiring their local state.
///
/// Marking and snapshotting happen under the SAME write lock, which is what
/// makes the pair race-free against the renewal session: any recreate that
/// installs before this call is in the snapshot, and any recreate that
/// resolves after it sees the marker and refuses. The state itself stays
/// registered so a failed DELETE leaves that id and every later id
/// reachable under the handle for a retry.
///
/// Returns `None` for an unknown handle - teardown is idempotent.
pub(super) fn mark_group_tearing_down(
    groups: &mut HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
    handle: &SubscriptionHandle,
) -> Option<Vec<String>> {
    let group = groups.get_mut(handle)?;
    group.tearing_down = true;
    Some(
        group
            .subscriptions
            .iter()
            .map(|state| state.server_id.clone())
            .collect(),
    )
}

/// Stop renewing one subscription whose renewal failed terminally, keeping
/// its row so teardown can still DELETE it (see
/// `GraphSubscriptionState::terminated`).
pub(super) async fn mark_subscription_terminated(
    account: &GraphAccount,
    handle: &SubscriptionHandle,
    server_id: &str,
) {
    let mut groups = account.graph_subscriptions.write().await;
    if let Some(state) = groups.get_mut(handle).and_then(|group| {
        group
            .subscriptions
            .iter_mut()
            .find(|state| state.server_id == server_id)
    }) {
        state.terminated = true;
    }
}

/// Forget one server subscription whose DELETE has succeeded: its group
/// state and its entry in the recreated-id ledger.
///
/// Teardown's only way of forgetting a subscription, so on that path a row is
/// never dropped without the DELETE that makes forgetting it honest. The other
/// exits are not teardown: a recreate swaps a vanished row (and its ledger
/// id) for its replacement, and a fully successful orphan teardown clears the
/// handle's ledger entry, having deleted everything in it.
async fn forget_deleted_subscription(
    account: &GraphAccount,
    handle: &SubscriptionHandle,
    server_id: &str,
) {
    let mut groups = account.graph_subscriptions.write().await;
    remove_subscription_from_groups(&mut groups, handle, server_id);
    forget_recreated_id(&mut recreated_ids(account), handle, server_id);
}

/// Drop `server_id` from `handle`'s ledger entry, and the entry once empty.
pub(super) fn forget_recreated_id(
    ledger: &mut HashMap<SubscriptionHandle, Vec<String>>,
    handle: &SubscriptionHandle,
    server_id: &str,
) {
    if let Some(ids) = ledger.get_mut(handle) {
        ids.retain(|id| id != server_id);
        if ids.is_empty() {
            ledger.remove(handle);
        }
    }
}

/// Forget one server subscription only after its DELETE has succeeded.
///
/// Returns whether this was the group's final subscription. Keeping this
/// state transition pure pins the teardown invariant without requiring a
/// live Graph DELETE transport seam.
pub(super) fn remove_subscription_from_groups(
    groups: &mut HashMap<SubscriptionHandle, GraphSubscriptionGroup>,
    handle: &SubscriptionHandle,
    server_id: &str,
) -> bool {
    let remove_group = if let Some(group) = groups.get_mut(handle) {
        group
            .subscriptions
            .retain(|state| state.server_id != server_id);
        group.subscriptions.is_empty()
    } else {
        false
    };
    if remove_group {
        groups.remove(handle);
    }
    remove_group
}

pub(super) fn resource_for_scope(
    account: &GraphAccount,
    scope: &CursorScope,
) -> Result<Option<String>, crate::error::GraphError> {
    // Route the resource through the scope's owning client (primary `/me`
    // or a shared mailbox's `/users/{owner}`) and use the *native* folder
    // id in the path. A foreign scope carries the owning mailbox inside the
    // `FolderId`; percent-encoding that raw foreign id into the URL (the
    // old behavior) produced a `/me/mailFolders/{owner%1Ffolder}/...`
    // resource Graph cannot resolve.
    let prefix = account.client_for_scope(scope)?.api_path_prefix();
    match scope {
        CursorScope::FolderType { folder, ty } => match ty {
            ObjectType::Email => {
                let native = crate::account::foreign::parse_folder(folder)
                    .native_id()
                    .to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Ok(Some(format!("{prefix}/mailFolders/{encoded}/messages")))
            }
            ObjectType::Event | ObjectType::CalendarEvent => {
                // The scope's folder is the calendar id - the same id
                // `inventory.rs` builds `/calendars/{id}/calendarView/delta`
                // from. Discarding it and subscribing to `{prefix}/events`
                // subscribes to the *default* calendar for every calendar
                // scope: secondary calendars would never see a notification
                // while the engine believed them push-covered, and the
                // grouping in `subscribe_graph` (keyed on this string) would
                // collapse every calendar scope into one entry.
                let native = crate::account::foreign::parse_folder(folder)
                    .native_id()
                    .to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Ok(Some(format!("{prefix}/calendars/{encoded}/events")))
            }
            ObjectType::Contact => {
                let native = crate::account::foreign::parse_folder(folder)
                    .native_id()
                    .to_string();
                let encoded = bifrost_net::url::encode_path_component(&native);
                Ok(Some(format!("{prefix}/contactFolders/{encoded}/contacts")))
            }
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}
