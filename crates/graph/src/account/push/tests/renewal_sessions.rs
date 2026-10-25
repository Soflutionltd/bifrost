//! Renewal sessions preserve every row's outcome and committed gap.
use super::fixtures::{email_scope, expiring, webhook_account};
use crate::account::push::renewal::renewal_session;
use crate::account::push::webhook::GraphSubscriptionGroup;
use crate::client::ScriptedRestResponse;
use bifrost_types::{
    PushGate, PushSessionEnd as End, PushSessionEvent as E, SessionStop, SubscriptionHandle,
};
use futures::StreamExt;

#[tokio::test(start_paused = true)]
async fn a_terminal_renewal_failure_names_the_scope_that_lost_coverage() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::FORBIDDEN,
        serde_json::json!({"error":{"code":"ErrorAccessDenied","message":"denied"}}),
    )]);
    let mut row = expiring("old", "/me/mailFolders/inbox/messages");
    row.scopes = vec![email_scope("inbox")];
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(vec![row]),
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(
        matches!(session.next().await, Some(E::UnitFailed(e)) if e.recovery().is_terminal() && e.scope() == Some(&bifrost_types::ErrorScope::Cursor(email_scope("inbox"))))
    );
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    assert!(session.next().await.is_none());
    let mut next = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    let mut event = Box::pin(next.next());
    assert!(futures::poll!(&mut event).is_pending());
    assert_eq!(account.client.take_rest_requests().len(), 1);
}

/// The recreate leg, end to end through the REST seam: a renewal PATCH that
/// 404s (Graph retains no deleted subscription) mints a fresh create for the
/// SAME resource, carrying the caller-owned clientState, installs it in
/// place of the vanished row under the same handle, and invalidates the
/// row's scopes through `pending_gaps` - nothing was delivered between the
/// disappearance and the replacement. The recovery succeeded within the
/// pass, so no `UnitFailed` precedes it and the pass ends `Healthy`.
#[tokio::test(start_paused = true)]
async fn a_vanished_subscription_is_recreated_and_reconnected_on_the_next_tick() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        ),
        super::fixtures::created("fresh"),
    ]);
    let handle = SubscriptionHandle("h".into());
    let mut row = expiring("stale", "/me/mailFolders/inbox/messages");
    row.scopes = vec![email_scope("inbox")];
    account
        .graph_subscriptions
        .write()
        .await
        .insert(handle.clone(), GraphSubscriptionGroup::live(vec![row]));
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    match session.next().await {
        Some(E::Invalidated(hint)) => {
            assert!(matches!(
                hint.source,
                bifrost_types::PushSource::GraphSubscription
            ));
            assert!(
                matches!(&hint.payload, bifrost_types::HintPayload::SpecificCursorScope(scope) if *scope == email_scope("inbox")),
                "the recreated row's scope, got {:?}",
                hint.payload
            );
        }
        other => panic!("expected the gap invalidation, got {other:?}"),
    }
    assert!(matches!(session.next().await, Some(E::Healthy)));
    assert!(account.pending_gaps.lock().unwrap().is_empty());
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
    let requests = account.client.take_rest_requests();
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
}

/// If the folder disappeared with the old subscription, the replacement
/// create answers 404 too. That refusal is terminal: the stale row stays
/// registered for teardown, is flagged `terminated`, and no later session
/// repeats the doomed PATCH and POST. The script holds exactly the two
/// responses, so a retry would exhaust it and panic rather than pass.
#[tokio::test(start_paused = true)]
async fn a_missing_resource_on_recreate_terminates_the_subscription_row() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    let not_found = || {
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({
                "error": { "code": "ResourceNotFound", "message": "resource is gone" }
            }),
        )
    };
    account.client.script_rest([not_found(), not_found()]);
    let handle = SubscriptionHandle("h".into());
    let resource = "/me/mailFolders/deleted-folder/messages";
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("stale", resource)]),
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(
        matches!(session.next().await, Some(E::UnitFailed(e)) if e.recovery().is_terminal()),
        "a terminal recreate refusal reaches the engine"
    );
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    assert!(session.next().await.is_none());
    {
        let groups = account.graph_subscriptions.read().await;
        let group = groups.get(&handle).expect("stale row stays for teardown");
        assert_eq!(group.subscriptions.len(), 1);
        assert_eq!(group.subscriptions[0].server_id, "stale");
        assert!(group.subscriptions[0].terminated);
    }
    let requests = account.client.take_rest_requests();
    assert_eq!(requests.len(), 2, "one PATCH and one failed create");
    assert_eq!(requests[0].method, "PATCH");
    assert_eq!(requests[1].method, "POST");

    // The next session finds no live row and parks, however long it waits.
    let mut next = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    let mut event = Box::pin(next.next());
    for _ in 0..4 {
        assert!(futures::poll!(&mut event).is_pending());
        tokio::time::advance(
            crate::account::push::renewal::RENEWAL_CHECK_INTERVAL
                + std::time::Duration::from_secs(1),
        )
        .await;
        tokio::task::yield_now().await;
    }
    assert!(futures::poll!(&mut event).is_pending());
    assert_eq!(
        account.client.wire_attempts(),
        2,
        "terminated rows are not retried"
    );
}

/// The plain success leg, driven across several cadence waits. A due
/// subscription is PATCHed once; the expiry Graph GRANTED is written back
/// into the state the next pass reads, so the row stops being due and no
/// further request is issued however many passes run. Renewing on every
/// pass is an unbounded request loop against Graph, and the script holds
/// exactly one response, so a second PATCH panics rather than passing.
///
/// The stored expiry is the granted string exactly, not the one requested:
/// Graph may cap a renewal below the request, and a locally computed expiry
/// would have the session believe in coverage the server already dropped.
/// A healthy renewal is also quiet: only `Healthy`, no invalidation and no
/// failure.
#[tokio::test(start_paused = true)]
async fn a_successful_renewal_updates_the_expiry_and_the_next_tick_finds_nothing_due() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({
            "id": "sub",
            "resource": "/me/mailFolders/inbox/messages",
            "expirationDateTime": "2099-01-01T00:00:00Z"
        }),
    )]);
    let handle = SubscriptionHandle("h".into());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![expiring("sub", "/me/mailFolders/inbox/messages")]),
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    // Many more passes than renewals: the point is that the extra passes,
    // each one cadence wait apart under paused time, issue nothing.
    for _ in 0..16 {
        let event = session.next().await;
        assert!(
            matches!(event, Some(E::Healthy)),
            "a healthy pass reports only Healthy, got {event:?}"
        );
    }
    {
        let groups = account.graph_subscriptions.read().await;
        let group = groups.get(&handle).expect("the handle stays registered");
        assert_eq!(group.subscriptions.len(), 1);
        assert_eq!(group.subscriptions[0].server_id, "sub", "renewed in place");
        assert_eq!(
            group.subscriptions[0].expires_at, "2099-01-01T00:00:00Z",
            "the stored expiry is the server-granted one"
        );
        assert!(!crate::webhooks::is_expiring_soon(
            &group.subscriptions[0].expires_at,
            crate::account::push::renewal::RENEWAL_THRESHOLD_MINUTES
        ));
    }
    let requests = account.client.take_rest_requests();
    assert_eq!(requests.len(), 1, "one renewal, not one per pass");
    assert_eq!(requests[0].method, "PATCH");
    assert!(requests[0].url.ends_with("/subscriptions/sub"));
    assert!(
        requests[0].body.as_ref().expect("renewal body")["expirationDateTime"]
            .as_str()
            .is_some_and(|expiry| expiry.ends_with('Z') && expiry != "2099-01-01T00:00:00Z"),
        "the PATCH requests its own expiry, which is not what gets stored"
    );
}

/// A pass whose ONLY failure is terminal still ends `Incomplete`. A
/// terminal row is flagged and never renewed again, so it is not "worth
/// another tick" - but its coverage is gone, and a sibling's successful
/// renewal in the same pass must not let the lane read healthy.
#[tokio::test(start_paused = true)]
async fn a_pass_whose_only_failure_is_terminal_ends_incomplete() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::FORBIDDEN,
            serde_json::json!({"error":{"code":"ErrorAccessDenied","message":"denied"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"id":"healthy","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
    ]);
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(vec![
            expiring("denied", "/me/messages"),
            expiring("healthy", "/me/events"),
        ]),
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::UnitFailed(e)) if e.recovery().is_terminal()));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    assert!(session.next().await.is_none());
    assert_eq!(
        account.client.take_rest_requests().len(),
        2,
        "both rows attempted"
    );
    let groups = account.graph_subscriptions.read().await;
    let rows = &groups[&SubscriptionHandle("h".into())].subscriptions;
    assert!(rows[0].terminated);
    assert_eq!(rows[1].expires_at, "2099-01-01T00:00:00Z");
}

#[tokio::test(start_paused = true)]
async fn a_failed_tick_disconnects_and_the_next_successful_tick_reconnects() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({"error":{"code":"TooManyRequests"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"id":"old","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
    ]);
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(vec![expiring("old", "/me/events")]),
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::UnitFailed(_))));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    drop(session);
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Healthy)));
    assert_eq!(account.client.take_rest_requests().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn one_tick_recreating_several_subscriptions_reconnects_once() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.client.script_rest((0..2).flat_map(|i| [
        ScriptedRestResponse::json(reqwest::StatusCode::NOT_FOUND, serde_json::json!({"error":{"code":"ResourceNotFound"}})),
        ScriptedRestResponse::json(reqwest::StatusCode::CREATED, serde_json::json!({"id":format!("new-{i}"),"expirationDateTime":"2099-01-01T00:00:00Z"})),
    ]));
    let mut rows = vec![
        expiring("old-1", "/me/events"),
        expiring("old-2", "/me/messages"),
    ];
    for (i, row) in rows.iter_mut().enumerate() {
        row.scopes = vec![email_scope(&format!("f{i}"))];
    }
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(rows),
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    for _ in 0..2 {
        assert!(matches!(session.next().await, Some(E::Invalidated(_))));
    }
    assert!(matches!(session.next().await, Some(E::Healthy)));
    assert!(account.pending_gaps.lock().unwrap().is_empty());
    assert_eq!(account.client.take_rest_requests().len(), 4);
}

#[tokio::test(start_paused = true)]
async fn a_worker_cancelled_before_its_first_renewal_sends_nothing() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(vec![expiring("old", "/me/events")]),
    );
    let (stopper, stop) = SessionStop::pair();
    stopper.stop();
    let mut session = renewal_session(account.clone(), stop, PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Ended(End::Stopped))));
    assert!(account.client.take_rest_requests().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_failing_row_does_not_starve_a_healthy_row_in_the_same_pass() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({"error":{"code":"TooManyRequests"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"id":"second","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
    ]);
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(vec![
            expiring("first", "/me/messages"),
            expiring("second", "/me/events"),
        ]),
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::UnitFailed(_))));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    assert_eq!(account.client.take_rest_requests().len(), 2);
    assert_eq!(
        account.graph_subscriptions.read().await[&SubscriptionHandle("h".into())].subscriptions[1]
            .expires_at,
        "2099-01-01T00:00:00Z"
    );
}
#[tokio::test(start_paused = true)]
async fn every_row_failure_in_a_pass_is_reported() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.client.script_rest((0..2).map(|_| {
        ScriptedRestResponse::json(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            serde_json::json!({"error":{"code":"TooManyRequests"}}),
        )
    }));
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(vec![
            expiring("first", "/me/messages"),
            expiring("second", "/me/events"),
        ]),
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    for _ in 0..2 {
        assert!(matches!(session.next().await, Some(E::UnitFailed(_))));
    }
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
}
#[tokio::test(start_paused = true)]
async fn a_pass_after_a_retired_row_stays_incomplete_until_the_handle_is_unsubscribed() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    let mut retired = expiring("dead", "/me/messages");
    retired.terminated = true;
    let live = super::fixtures::state("live", "/me/events");
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(vec![retired, live]),
    );
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Incomplete))
    ));
    drop(session);
    account
        .graph_subscriptions
        .write()
        .await
        .get_mut(&SubscriptionHandle("h".into()))
        .unwrap()
        .subscriptions
        .retain(|s| !s.terminated);
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    assert!(matches!(session.next().await, Some(E::Healthy)));
    assert!(account.client.take_rest_requests().is_empty());
}
#[tokio::test(start_paused = true)]
async fn a_renewal_pass_waits_for_the_push_gate_before_each_row() {
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account.client.script_rest((0..2).map(|_| {
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"id":"renewed","expirationDateTime":"2099-01-01T00:00:00Z"}),
        )
    }));
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(vec![
            expiring("first", "/me/messages"),
            expiring("second", "/me/events"),
        ]),
    );
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let gate = std::sync::Arc::clone(&permits);
    let gate = PushGate::new(move || {
        let gate = std::sync::Arc::clone(&gate);
        Box::pin(async move {
            gate.acquire().await.unwrap().forget();
        })
    });
    let mut session = renewal_session(account.clone(), SessionStop::never(), gate);
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    assert!(futures::poll!(&mut next).is_pending());
    assert!(account.client.take_rest_requests().is_empty());
    permits.add_permits(1);
    for _ in 0..20 {
        assert!(futures::poll!(&mut next).is_pending());
        tokio::task::yield_now().await;
    }
    assert_eq!(account.client.take_rest_requests().len(), 1);
    permits.add_permits(1);
    assert!(matches!(next.await, Some(E::Healthy)));
    assert_eq!(account.client.take_rest_requests().len(), 1);
}
#[tokio::test]
async fn a_recreate_landing_after_its_session_dropped_is_invalidated_by_the_next_session() {
    use crate::account::push::renewal::replace_gone_subscription;
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    account
        .client
        .script_rest([super::fixtures::created("new")]);
    let handle = SubscriptionHandle("h".into());
    let row = expiring("old", "/me/messages");
    let scopes = row.scopes.clone();
    account
        .graph_subscriptions
        .write()
        .await
        .insert(handle.clone(), GraphSubscriptionGroup::live(vec![row]));
    let held = account.graph_subscriptions.write().await;
    let owner = account.clone();
    let endpoint = account.push_endpoint.clone().unwrap();
    let task = tokio::spawn(async move {
        replace_gone_subscription(&owner, &endpoint, &handle, "old", "/me/messages", &scopes).await
    });
    assert!(super::fixtures::settle(|| account.client.rest_requests().len() == 1).await);
    task.abort();
    let _ = task.await;
    drop(held);
    assert!(super::fixtures::settle(|| !account.pending_gaps.lock().unwrap().is_empty()).await);
    let mut session = renewal_session(account.clone(), SessionStop::never(), PushGate::open());
    assert!(matches!(session.next().await, Some(E::Invalidated(_))));
    assert!(account.pending_gaps.lock().unwrap().is_empty());
}
#[tokio::test]
async fn close_joins_the_worker_and_so_waits_for_the_recreates_undo() {
    use bifrost_types::Account;
    let account = webhook_account(&crate::client::GraphClient::new("token"));
    let gate = std::sync::Arc::new(tokio::sync::Notify::new());
    let complete = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ready = std::sync::Arc::clone(&gate);
    let done = std::sync::Arc::clone(&complete);
    crate::account::push_tasks::track(
        &account,
        tokio::spawn(async move {
            ready.notified().await;
            done.store(true, std::sync::atomic::Ordering::SeqCst);
        }),
    );
    let mut close = Box::pin(account.close());
    assert!(futures::poll!(&mut close).is_pending());
    gate.notify_one();
    close.await.unwrap();
    assert!(complete.load(std::sync::atomic::Ordering::SeqCst));
}
#[tokio::test]
async fn push_lanes_follow_mode_and_endpoint() {
    use crate::account::{GraphAccount, PushMode};
    let client = crate::client::GraphClient::new("token");
    let no_endpoint = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    assert!(crate::account::push_stream::push_lanes(&no_endpoint).is_empty());
    assert_eq!(
        crate::account::push_stream::push_lanes(&webhook_account(&client))[0].kind,
        bifrost_types::PushLaneKind::Registration
    );
    let ews = GraphAccount::new_for_tests(client, PushMode::EwsStreaming);
    assert_eq!(
        crate::account::push_stream::push_lanes(&ews)[0].kind,
        bifrost_types::PushLaneKind::Connection
    );
}

#[tokio::test]
async fn the_worker_stops_on_cancellation_while_a_renewal_is_in_flight() {
    let client = crate::client::GraphClient::new("token");
    client.script_aux_pending(1);
    let account = webhook_account(&client);
    account.graph_subscriptions.write().await.insert(
        SubscriptionHandle("h".into()),
        GraphSubscriptionGroup::live(vec![expiring("old", "/me/events")]),
    );
    let (stopper, stop) = SessionStop::pair();
    let mut session = renewal_session(account, stop, PushGate::open());
    assert!(matches!(session.next().await, Some(E::Established)));
    let mut next = Box::pin(session.next());
    for _ in 0..20 {
        assert!(futures::poll!(&mut next).is_pending());
        tokio::task::yield_now().await;
    }
    assert_eq!(client.wire_attempts(), 1);
    stopper.stop();
    assert!(matches!(next.await, Some(E::Ended(End::Stopped))));
    assert!(session.next().await.is_none());
}
