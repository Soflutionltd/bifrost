//! The renewal health worker: the due list, the recreate of a vanished
//! subscription, the worker-slot retirement, and the Disconnected /
//! Reconnected edges the tick loop publishes.

use std::collections::HashMap;
use std::time::Duration;

use bifrost_types::{
    CursorScope, ErrorScope, FolderId, ObjectType, SubscriptionHandle, WatchEvent,
};

use crate::account::push::common::PushEndpoint;
use crate::account::push::renewal::{
    RENEWAL_CHECK_INTERVAL, RENEWAL_THRESHOLD_MINUTES, due_renewals,
    has_live_graph_subscription_group, install_replacement, replace_gone_subscription,
};
use crate::account::push::webhook::{
    GraphSubscriptionGroup, ensure_graph_worker, mark_group_tearing_down,
    remove_subscription_from_groups,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};

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

/// The renewal worker walks a SNAPSHOT of the due subscriptions, so a
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
/// "registered" stopped implying "live": the renewal worker's recreate
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

    // The renewal worker's replacement for the vanished `second` lands
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

/// Letting the worker exit on condemned-only groups opened a window in
/// which a brand-new subscription got NO renewal worker: the worker had
/// decided to exit but its `JoinHandle` was not finished yet, so the
/// concurrent `push_subscribe`'s `ensure_graph_worker` saw a live handle
/// and declined to spawn; the old task then completed and nothing renewed
/// the new subscription until some later subscribe. The exit therefore
/// clears the slot itself, under the guard that made the decision.
///
/// The tick here is driven with paused time and the map holds only a
/// condemned group, so the worker reaches its exit without a single HTTP
/// call. (The renewal HTTP legs themselves are scripted through the
/// REST seam where a test needs them - see the recreate test below.)
#[tokio::test(start_paused = true)]
async fn a_retiring_worker_clears_its_slot_so_the_next_subscribe_respawns() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let handle = SubscriptionHandle("condemned".to_string());
    {
        let mut groups = account.graph_subscriptions.write().await;
        groups.insert(
            handle.clone(),
            GraphSubscriptionGroup::live(vec![state("sub", "/me/events")]),
        );
        assert!(mark_group_tearing_down(&mut groups, &handle).is_some());
    }

    ensure_graph_worker(account.clone()).await;
    assert!(
        account.graph_worker.lock().await.is_some(),
        "the worker slot is occupied while the worker runs"
    );

    // Drive renewal ticks until the worker retires. Time is paused, so
    // this advances the test clock rather than the wall clock; the extra
    // iterations only exist because the worker has to be scheduled onto
    // its first `sleep` before a tick can fire at all.
    for _ in 0..64 {
        if account.graph_worker.lock().await.is_none() {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    assert!(
        account.graph_worker.lock().await.is_none(),
        "a retiring worker must clear its own slot"
    );

    // The next subscription therefore gets a worker again.
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("fresh".to_string()),
        GraphSubscriptionGroup::live(vec![state("fresh-sub", "/me/events")]),
    );
    ensure_graph_worker(account.clone()).await;
    assert!(
        account.graph_worker.lock().await.is_some(),
        "a live subscription must always have a renewal worker"
    );
    account.shutdown.cancel();
}

/// The recreate leg, end to end through the REST seam with paused time:
/// a renewal PATCH that 404s (Graph retains no deleted subscription)
/// must mint a fresh create for the SAME resource, install it in place
/// of the vanished row under the same handle, and emit `Reconnected` -
/// the engine's full-reconcile trigger - because nothing was delivered
/// between the disappearance and the replacement. No `Disconnected`
/// precedes it: the recovery succeeded within one tick.
#[tokio::test(start_paused = true)]
async fn a_vanished_subscription_is_recreated_and_reconnected_on_the_next_tick() {
    let client = GraphClient::new("token");
    client.script_rest([
        // The renewal PATCH answers 404: the subscription vanished.
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        ),
        // The replacement create.
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            serde_json::json!({"id":"fresh","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
    ]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/hook".to_string(),
        client_state: "secret".to_string(),
    });
    let mut events = account.push_tx.subscribe();
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("stale", "/me/mailFolders/inbox/messages")]),
    );

    ensure_graph_worker(account.clone()).await;
    // Drive renewal ticks until the replacement lands. Paused time, so
    // this advances the test clock; the loop bound only covers task
    // scheduling slack.
    for _ in 0..64 {
        let installed = account
            .graph_subscriptions
            .read()
            .await
            .get(&handle)
            .is_some_and(|group| {
                group
                    .subscriptions
                    .iter()
                    .any(|state| state.server_id == "fresh")
            });
        if installed {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    {
        let groups = account.graph_subscriptions.read().await;
        let group = groups.get(&handle).expect("the handle stays registered");
        assert_eq!(group.subscriptions.len(), 1, "in place, not beside");
        assert_eq!(group.subscriptions[0].server_id, "fresh");
        assert_eq!(
            group.subscriptions[0].resource,
            "/me/mailFolders/inbox/messages"
        );
    }

    // The failed PATCH, then the create for the same resource - and the
    // create carries the caller-owned clientState like any first-time
    // subscribe would.
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "PATCH");
    assert!(requests[0].url.ends_with("/subscriptions/stale"));
    assert_eq!(requests[1].method, "POST");
    assert!(requests[1].url.ends_with("/subscriptions"));
    let created = requests[1].body.as_ref().expect("create body");
    assert_eq!(
        created["resource"].as_str(),
        Some("/me/mailFolders/inbox/messages")
    );
    assert_eq!(created["clientState"].as_str(), Some("secret"));

    match events.try_recv() {
        Ok(WatchEvent::Reconnected) => {}
        other => panic!("expected Reconnected, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "a within-tick recovery emits no Disconnected"
    );

    account.shutdown.cancel();
}

/// If the folder disappeared with the old subscription, replacement creation
/// returns 404. That error is terminal: retain the stale row for teardown,
/// mark it terminated, and do not repeat the doomed POST on later ticks.
#[tokio::test(start_paused = true)]
async fn a_missing_resource_on_recreate_terminates_the_subscription_row() {
    let client = GraphClient::new("token");
    let not_found = || {
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({
                "error": { "code": "ResourceNotFound", "message": "resource is gone" }
            }),
        )
    };
    client.script_rest([not_found(), not_found()]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/hook".to_string(),
        client_state: "secret".to_string(),
    });
    let mut events = account.push_tx.subscribe();
    let handle = SubscriptionHandle("h".to_string());
    let resource = "/me/mailFolders/deleted-folder/messages";
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("stale", resource)]),
    );

    ensure_graph_worker(account.clone()).await;
    for _ in 0..64 {
        let terminated = account
            .graph_subscriptions
            .read()
            .await
            .get(&handle)
            .is_some_and(|group| group.subscriptions[0].terminated);
        if terminated {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    {
        let groups = account.graph_subscriptions.read().await;
        let group = groups.get(&handle).expect("stale row stays for teardown");
        assert_eq!(group.subscriptions.len(), 1);
        assert_eq!(group.subscriptions[0].server_id, "stale");
        assert!(group.subscriptions[0].terminated);
    }
    let Ok(WatchEvent::Terminated(error)) = events.try_recv() else {
        panic!("a terminal recreate refusal reaches the push stream");
    };
    assert!(error.recovery().is_terminal());
    assert_eq!(client.wire_attempts(), 2, "one PATCH and one failed create");

    tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(client.wire_attempts(), 2, "terminated rows are not retried");
    account.shutdown.cancel();
}

/// One `Reconnected` per TICK, not per recreated subscription.
///
/// `Reconnected` is account-wide: the engine's push reconciler answers it
/// with a `Coalesced` / `HintPayload::Unknown` invalidation over every
/// registered cursor scope. So the second recreate of a tick reconciles
/// exactly what the first already did, and a tick that finds several
/// subscriptions vanished at once - the case where the duplicates cost the
/// most - used to publish one full account reconcile per resource.
#[tokio::test(start_paused = true)]
async fn one_tick_recreating_several_subscriptions_reconnects_once() {
    let client = GraphClient::new("token");
    let gone = || {
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        )
    };
    let created = |id: &str| {
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            serde_json::json!({"id":id,"expirationDateTime":"2099-01-01T00:00:00Z"}),
        )
    };
    // Both rows are due in the same tick, so the worker walks them back to
    // back: PATCH 404, create, PATCH 404, create.
    client.script_rest([gone(), created("fresh-a"), gone(), created("fresh-b")]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/hook".to_string(),
        client_state: "secret".to_string(),
    });
    let mut events = account.push_tx.subscribe();
    let handle = SubscriptionHandle("h".to_string());
    // One group, so the due order is the vector's and the script's wire
    // order is an assertion rather than a coin flip on map iteration.
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            expiring("stale-a", "/me/mailFolders/inbox/messages"),
            expiring("stale-b", "/me/events"),
        ]),
    );

    ensure_graph_worker(account.clone()).await;
    for _ in 0..64 {
        let both_replaced = account
            .graph_subscriptions
            .read()
            .await
            .get(&handle)
            .is_some_and(|group| {
                group
                    .subscriptions
                    .iter()
                    .filter(|state| state.server_id.starts_with("fresh-"))
                    .count()
                    == 2
            });
        if both_replaced {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    // Let the tick that recreated finish its post-loop bookkeeping before
    // the channel is drained.
    tokio::task::yield_now().await;

    assert_eq!(
        client.take_rest_requests().len(),
        4,
        "two renewals, two recreates"
    );
    match events.try_recv() {
        Ok(WatchEvent::Reconnected) => {}
        other => panic!("expected Reconnected, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "a tick owes exactly one account-wide reconcile"
    );

    account.shutdown.cancel();
}

/// The plain SUCCESS leg, driven as a loop across ticks.
///
/// A due subscription is PATCHed once; the new expiry it returns is
/// written back into the state the NEXT tick reads, so the subscription
/// stops being due and no further request is issued no matter how many
/// ticks fire. Renewing on every tick (the failure mode a state that
/// never updates produces) would be an unbounded request loop against
/// Graph, and the seam is armed with exactly one response, so a second
/// PATCH panics rather than passing.
///
/// The quiet path is also pinned: a healthy renewal emits no push event
/// at all - neither `Disconnected` nor a spurious `Reconnected`.
#[tokio::test(start_paused = true)]
async fn a_successful_renewal_updates_the_expiry_and_the_next_tick_finds_nothing_due() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({
            "id": "sub",
            "resource": "/me/mailFolders/inbox/messages",
            "expirationDateTime": "2099-01-01T00:00:00Z"
        }),
    )]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let mut events = account.push_tx.subscribe();
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("sub", "/me/mailFolders/inbox/messages")]),
    );

    ensure_graph_worker(account.clone()).await;
    // Many more ticks than renewals: the point is that the extra ticks
    // issue nothing.
    for _ in 0..64 {
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    {
        let groups = account.graph_subscriptions.read().await;
        let group = groups.get(&handle).expect("the handle stays registered");
        assert_eq!(group.subscriptions.len(), 1);
        assert_eq!(group.subscriptions[0].server_id, "sub", "renewed in place");
        // The expiry stored is the one Graph GRANTED, not the one we
        // asked for. Graph may cap a renewal below the request, and a
        // locally computed expiry then has the renewer believing it has
        // coverage the server already dropped. Asserting only that the
        // stale value was replaced passes against the computed value
        // too, so this pins the granted string exactly.
        assert_eq!(
            group.subscriptions[0].expires_at, "2099-01-01T00:00:00Z",
            "the stored expiry is the server-granted one"
        );
        assert!(!crate::webhooks::is_expiring_soon(
            &group.subscriptions[0].expires_at,
            RENEWAL_THRESHOLD_MINUTES
        ));
    }

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 1, "one renewal, not one per tick");
    assert_eq!(requests[0].method, "PATCH");
    assert!(requests[0].url.ends_with("/subscriptions/sub"));
    assert!(
        requests[0].body.as_ref().expect("renewal body")["expirationDateTime"]
            .as_str()
            .is_some_and(|expiry| expiry.ends_with('Z')),
        "the PATCH carries the new expiry it then stores"
    );
    assert!(
        events.try_recv().is_err(),
        "a healthy renewal is silent on the push channel"
    );

    account.shutdown.cancel();
}

/// A terminal renewal failure must name the scope that lost push
/// coverage. `subscribe_graph` groups by resource string and used to
/// discard the scopes it grouped, so `Terminated` arrived with no
/// attribution at all and the engine could not tell which scopes to fall
/// back to polling.
#[tokio::test(start_paused = true)]
async fn a_terminal_renewal_failure_names_the_scope_that_lost_coverage() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        serde_json::json!({"error":{"code":"ErrorAccessDenied","message":"no"}}),
    )]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let mut events = account.push_tx.subscribe();
    let scope = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    let mut state = expiring("sub", "/me/mailFolders/inbox/messages");
    state.scopes = vec![scope.clone()];
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".to_string()),
        GraphSubscriptionGroup::live(vec![state]),
    );

    ensure_graph_worker(account.clone()).await;
    // A terminal failure flags the row (it stays registered for teardown),
    // so the flag is the signal the tick has run.
    for _ in 0..64 {
        let retired = account
            .graph_subscriptions
            .read()
            .await
            .values()
            .all(|group| group.subscriptions.iter().all(|state| state.terminated));
        if retired {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;

    let mut terminated = None;
    while let Ok(event) = events.try_recv() {
        match event {
            WatchEvent::Terminated(error) => terminated = Some(error),
            WatchEvent::Disconnected => {}
            other => panic!("unexpected push event {other:?}"),
        }
    }
    let terminated = terminated.expect("a terminal failure is reported");
    assert_eq!(terminated.scope(), Some(&ErrorScope::Cursor(scope)));
    let groups = account.graph_subscriptions.read().await;
    let rows: Vec<_> = groups
        .values()
        .flat_map(|group| &group.subscriptions)
        .collect();
    assert_eq!(rows.len(), 1, "the row stays registered for teardown");
    assert!(rows[0].terminated);
    drop(groups);

    account.shutdown.cancel();
}

/// A terminated row needs nothing from the renewal worker: it is never due,
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

/// The recovery edge of the same loop: a retryable renewal failure
/// announces `Disconnected` ONCE, and the tick that finally succeeds
/// announces `Reconnected`. Without the success leg clearing the
/// latch, a recovered subscription would stay reported as
/// disconnected for the life of the account.
#[tokio::test(start_paused = true)]
async fn a_failed_tick_disconnects_and_the_next_successful_tick_reconnects() {
    let client = GraphClient::new("token");
    client.script_rest([
        // Retryable (429), so the subscription stays installed and due.
        ScriptedRestResponse::json(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({"error":{"code":"activityLimitReached","message":"slow down"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({
                "id": "sub",
                "resource": "/me/events",
                "expirationDateTime": "2099-01-01T00:00:00Z"
            }),
        ),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let mut events = account.push_tx.subscribe();
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("sub", "/me/events")]),
    );

    ensure_graph_worker(account.clone()).await;
    for _ in 0..64 {
        let renewed = account
            .graph_subscriptions
            .read()
            .await
            .get(&handle)
            .is_some_and(|group| {
                !crate::webhooks::is_expiring_soon(
                    &group.subscriptions[0].expires_at,
                    RENEWAL_THRESHOLD_MINUTES,
                )
            });
        if renewed {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }

    // Let the tick that renewed finish its post-loop bookkeeping (the
    // `Reconnected` it owes) before the channel is read.
    tokio::task::yield_now().await;

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2, "the failed renewal is retried once");
    assert!(requests.iter().all(|request| request.method == "PATCH"));

    match events.try_recv() {
        Ok(WatchEvent::Disconnected) => {}
        other => panic!("expected Disconnected, got {other:?}"),
    }
    match events.try_recv() {
        Ok(WatchEvent::Reconnected) => {}
        other => panic!("expected Reconnected, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "the latch must not re-announce on later quiet ticks"
    );

    account.shutdown.cancel();
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

/// A stand-in renewal worker: one recreate, awaited inline in a task the test
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

/// The worker stops on the token without being aborted: a renewal PATCH that
/// is parked on the wire is dropped and the task returns.
///
/// Fails if the `shutdown.cancelled()` arm of the renewal `select!` in
/// `run_graph_subscription_worker` is removed (the parked PATCH never
/// resolves, so the worker never finishes).
#[tokio::test(start_paused = true)]
async fn the_worker_stops_on_cancellation_while_a_renewal_is_in_flight() {
    let client = GraphClient::new("token");
    client.script_aux_pending(1);
    let (account, _handle) = account_with_stale_group(&client).await;

    ensure_graph_worker(account.clone()).await;
    for _ in 0..64 {
        if client.wire_attempts() == 1 {
            break;
        }
        tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(client.wire_attempts(), 1, "the renewal is on the wire");

    let worker = account
        .graph_worker
        .lock()
        .await
        .take()
        .expect("worker running");
    account.shutdown.cancel();
    assert!(settle(|| worker.is_finished()).await, "the worker returned");
    worker.await.expect("it returned, it did not panic");
}

/// A cancel that lands while the worker is waiting for its due list issues no
/// request at all: the token is read before the first PATCH, not after.
///
/// Fails if the `biased` cancelled arm is removed from the renewal `select!`:
/// the scripted wire is empty, so a PATCH panics the worker task and the
/// `await` below fails.
#[tokio::test(start_paused = true)]
async fn a_worker_cancelled_before_its_first_renewal_sends_nothing() {
    let client = GraphClient::new("token");
    client.script_rest([]);
    let (account, _handle) = account_with_stale_group(&client).await;

    let blocked = account.graph_subscriptions.write().await;
    ensure_graph_worker(account.clone()).await;
    // The worker wakes from its sleep and parks on the due list's lock.
    tokio::time::advance(RENEWAL_CHECK_INTERVAL + Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    account.shutdown.cancel();
    drop(blocked);

    let worker = account
        .graph_worker
        .lock()
        .await
        .take()
        .expect("worker running");
    assert!(settle(|| worker.is_finished()).await, "the worker returned");
    worker.await.expect("it returned, it did not panic");
    assert_eq!(client.wire_attempts(), 0);
}

/// `close()` racing a recreate that has created but not installed. The
/// create is walked or rolled back, and `close()` does not return until the
/// worker - which awaits the undo - has stopped.
///
/// The undo DELETE is parked on the wire, so a `close()` that joins the worker
/// takes the whole join budget (visible on the paused clock), while one that
/// aborts it returns at once. The lock queue is FIFO, so the recreate's
/// install is granted before the walk's read.
///
/// Fails if `close()` goes back to aborting the renewal worker (elapsed stays
/// under the budget). Also hangs, and so fails the watchdog, if the token
/// check under the install's write lock is removed: the replacement is then
/// installed and the walk's DELETE consumes the parked script entry.
#[tokio::test(start_paused = true)]
async fn close_joins_the_worker_and_so_waits_for_the_recreates_undo() {
    use bifrost_types::Account;

    let client = GraphClient::new("token");
    // POST create, then the undo DELETE parks forever, then the walk's DELETE.
    client.script_rest([created("fresh")]);
    client.script_aux_pending(1);
    client.script_rest([deleted()]);
    let (account, handle) = account_with_stale_group(&client).await;

    let blocked = account.graph_subscriptions.write().await;
    let recreate = spawn_recreate(&account, &handle);
    assert!(settle(|| client.wire_attempts() == 1).await, "create done");
    // The stand-in worker occupies the slot the way the real one does.
    *account.graph_worker.lock().await = Some(tokio::spawn(async move {
        let _ = recreate.await;
    }));

    let closing = account.clone();
    let close = tokio::spawn(async move { Account::close(&closing).await });
    assert!(
        settle(|| account.shutdown.is_cancelled()).await,
        "close reached its walk and cancelled the token"
    );
    drop(blocked);

    let started = tokio::time::Instant::now();
    close.await.expect("close ran").expect("close succeeds");
    assert!(
        started.elapsed() >= crate::account::CLOSE_WORKER_JOIN_TIMEOUT,
        "close waited on the worker instead of aborting it"
    );

    let requests = client.take_rest_requests();
    let urls: Vec<(&str, &str)> = requests
        .iter()
        .map(|request| (request.method.as_str(), request.url.as_str()))
        .collect();
    assert_eq!(urls.len(), 3, "create, undo, walk: {urls:?}");
    assert!(urls[1].1.ends_with("/subscriptions/fresh") && urls[1].0 == "DELETE");
    assert!(urls[2].1.ends_with("/subscriptions/stale") && urls[2].0 == "DELETE");
    assert!(account.graph_worker.lock().await.is_none());
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
