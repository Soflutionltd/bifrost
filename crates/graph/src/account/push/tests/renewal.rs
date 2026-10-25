//! The renewal health worker: the due list, the recreate of a vanished
//! subscription, the worker-slot retirement, and the Disconnected /
//! Reconnected edges the tick loop publishes.

use std::collections::HashMap;

use bifrost_types::SubscriptionHandle;

use crate::account::GraphAccount;
use crate::account::push::renewal::{
    due_renewals, has_live_graph_subscription_group, install_replacement, replace_gone_subscription,
};
use crate::account::push::webhook::{
    GraphSubscriptionGroup, mark_group_tearing_down, remove_subscription_from_groups,
};
use crate::client::GraphClient;

use super::fixtures::{
    created, deleted, email_scope, expiring, groups_empty, settle, state, webhook_account,
};

/// A recreated subscription takes the vanished one's place inside the
/// SAME group rather than piling up beside it: the stale `server_id`
/// would otherwise stay in the due list forever, 404 on every renewal
/// tick, and drive an endless recreate loop. Sibling resources under
/// the same handle are untouched.
#[test]
fn a_recreated_subscription_replaces_the_vanished_one_in_place() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("gone", "/me/mailFolders/inbox/messages"),
            state("healthy", "/me/events"),
        ]),
    )]);

    assert!(install_replacement(
        &mut groups,
        &handle,
        "gone",
        state("fresh", "/me/mailFolders/inbox/messages"),
    ));

    let subscriptions = &groups[&handle].subscriptions;
    assert_eq!(subscriptions.len(), 2);
    assert!(
        !subscriptions.iter().any(|s| s.server_id == "gone"),
        "the vanished subscription must not survive its replacement"
    );
    let fresh = subscriptions
        .iter()
        .find(|s| s.server_id == "fresh")
        .expect("replacement installed");
    assert_eq!(fresh.resource, "/me/mailFolders/inbox/messages");
    assert!(subscriptions.iter().any(|s| s.server_id == "healthy"));
    assert_eq!(groups.len(), 1);
}

/// The renewal session walks a SNAPSHOT of the due subscriptions, so a
/// concurrent `push_unsubscribe` can retire the handle (and delete its
/// server subscriptions) while a replacement create is in flight.
/// Re-registering the group there would tell the caller teardown
/// succeeded while notifications kept arriving; the replacement is
/// refused instead, and `replace_gone_subscription` deletes the
/// server-side subscription it just minted.
#[test]
fn a_replacement_never_resurrects_an_unsubscribed_handle() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups: HashMap<SubscriptionHandle, GraphSubscriptionGroup> = HashMap::new();
    assert!(!install_replacement(
        &mut groups,
        &handle,
        "gone",
        state("fresh", "/me/mailFolders/inbox/messages"),
    ));
    assert!(groups.is_empty(), "an unsubscribed handle stays retired");

    // A DIFFERENT live handle is not a substitute: the replacement is
    // scoped to the handle whose subscription vanished.
    let other = SubscriptionHandle("other".to_string());
    groups.insert(
        other.clone(),
        GraphSubscriptionGroup::live(vec![state("other-sub", "/me/events")]),
    );
    assert!(!install_replacement(
        &mut groups,
        &handle,
        "gone",
        state("fresh", "/me/mailFolders/inbox/messages"),
    ));
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[&other].subscriptions.len(), 1);
}

/// A group that no longer holds the stale row still takes the replacement:
/// the handle is live, so the resource needs coverage. A terminal renewal
/// used to be one way to lose the row; it now flags the row instead, but the
/// install must not depend on the row still being there.
#[test]
fn a_replacement_installs_into_a_live_group_that_lost_the_stale_row() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![state("healthy", "/me/events")]),
    )]);
    assert!(install_replacement(
        &mut groups,
        &handle,
        "already-removed",
        state("fresh", "/me/mailFolders/inbox/messages"),
    ));
    assert_eq!(groups[&handle].subscriptions.len(), 2);
}

/// The teardown/renewal race the deletes-before-removing repair opened.
///
/// Teardown now keeps the group registered while it deletes, so
/// "registered" stopped implying "live": the renewal session's recreate
/// path would install a replacement into a group whose server-id
/// snapshot had already been taken. Teardown then deleted the stale id,
/// retired the group, and returned success while the replacement kept
/// delivering notifications. The marker raised by
/// `mark_group_tearing_down` is what refuses that install.
#[test]
fn a_replacement_never_lands_in_a_group_being_torn_down() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("first", "/me/mailFolders/inbox/messages"),
            state("second", "/me/events"),
        ]),
    )]);

    // Teardown condemns the group and snapshots its ids under one lock.
    let snapshot = mark_group_tearing_down(&mut groups, &handle).expect("group registered");
    assert_eq!(snapshot, vec!["first".to_string(), "second".to_string()]);

    // The first DELETE is confirmed; the group stays registered so the
    // ids still to delete remain reachable.
    assert!(!remove_subscription_from_groups(
        &mut groups,
        &handle,
        "first"
    ));

    // The renewal session's replacement for the vanished `second` lands
    // mid-teardown. Installing it would put a live server subscription
    // under a handle whose snapshot can never name it.
    assert!(
        !install_replacement(&mut groups, &handle, "second", state("fresh", "/me/events")),
        "a condemned group must refuse a replacement"
    );
    assert!(
        !groups[&handle]
            .subscriptions
            .iter()
            .any(|state| state.server_id == "fresh"),
        "the replacement must not become state teardown cannot see"
    );

    // Teardown finishes on the ids it snapshotted, and the handle is gone.
    assert!(remove_subscription_from_groups(
        &mut groups,
        &handle,
        "second"
    ));
    assert!(groups.is_empty(), "teardown retires the handle");
}

/// A condemned group is not renewed either. Renewal would extend the
/// life of exactly what the caller asked to delete, and the recreate
/// branch behind it is the same orphan by another route.
#[test]
fn due_renewals_skip_a_group_being_torn_down() {
    let live = SubscriptionHandle("live".to_string());
    let condemned = SubscriptionHandle("condemned".to_string());
    let mut groups = HashMap::from([
        (
            live.clone(),
            GraphSubscriptionGroup::live(vec![expiring("live-sub", "/me/events")]),
        ),
        (
            condemned.clone(),
            GraphSubscriptionGroup::live(vec![expiring("condemned-sub", "/me/contacts")]),
        ),
    ]);
    assert!(mark_group_tearing_down(&mut groups, &condemned).is_some());

    let due = due_renewals(&mut groups);
    assert_eq!(due.len(), 1, "only the live group is due");
    assert_eq!(due[0].handle, live);
    assert_eq!(due[0].server_id, "live-sub");
    assert_eq!(due[0].resource, "/me/events");
    // The scopes ride along so a terminal failure can name what lost
    // coverage rather than reporting an unattributed `Terminated`.
    assert!(!due[0].scopes.is_empty());

    // And an expiry outside the threshold is not due at all.
    groups.get_mut(&live).expect("live group").subscriptions[0].expires_at =
        "2099-01-01T00:00:00Z".to_string();
    assert!(due_renewals(&mut groups).is_empty());
}

/// A terminated row needs nothing from the renewal session: it is never due,
/// and a group holding only terminated rows does not keep the worker alive.
#[test]
fn a_terminated_row_is_never_due_and_keeps_no_worker_alive() {
    let handle = SubscriptionHandle("h".to_string());
    let mut terminated = expiring("dead", "/me/events");
    terminated.terminated = true;
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![terminated]),
    )]);
    assert!(due_renewals(&mut groups).is_empty());
    assert!(!has_live_graph_subscription_group(&groups));

    groups
        .get_mut(&handle)
        .expect("group")
        .subscriptions
        .push(expiring("alive", "/me/contacts"));
    let due = due_renewals(&mut groups);
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].server_id, "alive");
    assert!(has_live_graph_subscription_group(&groups));
}

// ---- cancellation safety of the renewal recreate ------------------------
//
// `replace_gone_subscription` is driven directly, from a spawned stand-in
// for the worker that the test aborts. The write lock on the group map is
// held across the create so "the create returned, the id is not recorded
// yet" is a state the test can sit in and act on.

const RESOURCE: &str = "/me/mailFolders/inbox/messages";

async fn account_with_stale_group(client: &GraphClient) -> (GraphAccount, SubscriptionHandle) {
    let account = webhook_account(client);
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("stale", RESOURCE)]),
    );
    (account, handle)
}

/// A stand-in renewal session: one recreate, awaited inline in a task the test
/// can abort.
fn spawn_recreate(
    account: &GraphAccount,
    handle: &SubscriptionHandle,
) -> tokio::task::JoinHandle<
    Result<crate::account::push::renewal::Replacement, crate::error::GraphError>,
> {
    let account = account.clone();
    let handle = handle.clone();
    tokio::spawn(async move {
        let endpoint = account.push_endpoint.clone().expect("webhook endpoint");
        replace_gone_subscription(
            &account,
            &endpoint,
            &handle,
            "stale",
            RESOURCE,
            &[email_scope("inbox")],
        )
        .await
    })
}

/// The worker is aborted after the create returned and before its id is
/// recorded. The create, the install and the undo belong to a task the abort
/// cannot reach, so the replacement still lands in place of the stale row.
///
/// Fails if `replace_gone_subscription` awaits `create_and_install_replacement`
/// inline instead of handing it to `tokio::spawn`: the abort then drops the
/// future at the group write lock and "fresh" is never recorded.
#[tokio::test]
async fn an_aborted_recreate_still_installs_what_it_created() {
    let client = GraphClient::new("token");
    client.script_rest([created("fresh")]);
    let (account, handle) = account_with_stale_group(&client).await;

    let blocked = account.graph_subscriptions.write().await;
    let caller = spawn_recreate(&account, &handle);
    assert!(settle(|| client.wire_attempts() == 1).await, "create done");
    caller.abort();
    let _ = caller.await;
    drop(blocked);

    let installed = settle(|| {
        account.graph_subscriptions.try_read().is_ok_and(|groups| {
            groups.get(&handle).is_some_and(|group| {
                group.subscriptions.len() == 1 && group.subscriptions[0].server_id == "fresh"
            })
        })
    })
    .await;
    assert!(installed, "the replacement takes the stale row's place");
    assert_eq!(client.wire_attempts(), 1, "nothing was deleted");
}

/// The worker is aborted after the create returned, and the handle is retired
/// before the install: the undo DELETE must still run, or the new server
/// subscription is live with no group that knows it.
///
/// Fails if the undo in `create_and_install_replacement` is removed, or if the
/// recreate runs inline (the abort then kills it at the write lock, before the
/// undo).
#[tokio::test]
async fn an_aborted_recreate_for_a_retired_handle_still_deletes_the_new_subscription() {
    let client = GraphClient::new("token");
    client.script_rest([created("fresh"), deleted()]);
    let (account, handle) = account_with_stale_group(&client).await;

    let mut blocked = account.graph_subscriptions.write().await;
    let caller = spawn_recreate(&account, &handle);
    assert!(settle(|| client.wire_attempts() == 1).await, "create done");
    caller.abort();
    let _ = caller.await;
    // `push_unsubscribe` finished while the create was in flight.
    blocked.clear();
    drop(blocked);

    assert!(
        settle(|| client.wire_attempts() == 2).await,
        "the orphaned replacement is deleted"
    );
    let requests = client.take_rest_requests();
    let last = requests.last().expect("recorded");
    assert_eq!(last.method.as_str(), "DELETE");
    assert!(last.url.ends_with("/subscriptions/fresh"));
    assert!(
        groups_empty(&account),
        "a retired handle is not resurrected"
    );
}

/// `close()` cancels the token while a recreate is between its create and its
/// install, against a group the walk has not reached yet. The install must
/// refuse on the token and undo the create.
///
/// Fails if the `shutdown.is_cancelled()` check under the write lock in
/// `create_and_install_replacement` is removed: the group is live and not
/// `tearing_down`, so `install_replacement` alone would accept it.
#[tokio::test]
async fn a_recreate_racing_close_is_undone_not_installed() {
    let client = GraphClient::new("token");
    client.script_rest([created("fresh"), deleted()]);
    let (account, handle) = account_with_stale_group(&client).await;

    let blocked = account.graph_subscriptions.write().await;
    let caller = spawn_recreate(&account, &handle);
    assert!(settle(|| client.wire_attempts() == 1).await, "create done");
    account.shutdown.cancel();
    drop(blocked);

    let result = caller.await.expect("not aborted");
    assert_eq!(
        result.expect("the undo is not an error"),
        crate::account::push::renewal::Replacement::HandleUnsubscribed
    );
    assert_eq!(client.wire_attempts(), 2, "the create was rolled back");
    let groups = account.graph_subscriptions.read().await;
    let ids: Vec<&str> = groups[&handle]
        .subscriptions
        .iter()
        .map(|state| state.server_id.as_str())
        .collect();
    assert_eq!(ids, ["stale"], "the walk still finds only what it knows");
}

/// A recreate that starts on a closed account issues no POST at all.
///
/// Fails if the early `shutdown.is_cancelled()` check in
/// `create_and_install_replacement` is removed.
#[tokio::test]
async fn a_recreate_on_a_closed_account_creates_nothing() {
    let client = GraphClient::new("token");
    client.script_rest([]);
    let (account, handle) = account_with_stale_group(&client).await;
    account.shutdown.cancel();

    let result = spawn_recreate(&account, &handle)
        .await
        .expect("not aborted");
    assert_eq!(
        result.expect("no error"),
        crate::account::push::renewal::Replacement::AccountClosed
    );
    assert_eq!(client.wire_attempts(), 0);
}

#[test]
fn condemned_only_groups_do_not_keep_the_renewal_worker_alive() {
    let handle = SubscriptionHandle("condemned".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("sub", "/me/events")]),
    )]);
    assert!(has_live_graph_subscription_group(&groups));
    assert!(mark_group_tearing_down(&mut groups, &handle).is_some());
    assert!(!has_live_graph_subscription_group(&groups));
}
