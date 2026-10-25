//! The Graph `/subscriptions` arm: resource construction, subscribe with its
//! rollback, teardown, and the health-latch edge a subscribe publishes on.

use std::collections::HashMap;

use bifrost_types::{
    AccountErrorKind, AccountId, AccountOperation, CursorScope, FolderId, ObjectType,
    SubscriptionHandle,
};

use crate::account::push::common::PushEndpoint;
use crate::account::push::common::{
    DecodedHandle, account_binding, decode_handle, ews_handle, graph_handle,
};
use crate::account::push::dispatch::{push_subscribe, push_unsubscribe};
use crate::account::push::renewal::{Replacement, record_recreated_id, replace_gone_subscription};
use crate::account::push::webhook::{
    GraphSubscriptionGroup, mark_group_tearing_down, mark_subscription_terminated, recreated_ids,
    remove_subscription_from_groups, resource_for_scope, retire_all_graph_subscriptions,
    unsubscribe_graph,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};

use super::fixtures::{
    created, deleted, email_scope, groups_empty, settle, state, webhook_account,
};

#[test]
fn graph_subscription_resource_uses_folder_messages() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    assert_eq!(
        resource_for_scope(&account, &scope)
            .expect("primary")
            .as_deref(),
        Some("/me/mailFolders/inbox/messages")
    );
}

/// Each calendar scope must subscribe to its own calendar. `{prefix}/events`
/// is the default calendar, so a shared resource string both mis-targets
/// secondary calendars and collapses them together in `subscribe_graph`'s
/// resource-keyed grouping.
#[test]
fn graph_event_subscription_names_the_scope_calendar() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = |id: &str| CursorScope::FolderType {
        folder: FolderId(id.to_string()),
        ty: ObjectType::Event,
    };
    assert_eq!(
        resource_for_scope(&account, &scope("calendar-id"))
            .expect("primary")
            .as_deref(),
        Some("/me/calendars/calendar-id/events")
    );
    assert_ne!(
        resource_for_scope(&account, &scope("secondary")).expect("primary"),
        resource_for_scope(&account, &scope("calendar-id")).expect("primary"),
    );
    let opaque = resource_for_scope(&account, &scope("AAMk/GI2="))
        .expect("primary")
        .expect("subscribable");
    assert!(!opaque.contains("AAMk/GI2="), "{opaque}");
    assert!(opaque.ends_with("/events"), "{opaque}");
}

#[test]
fn graph_subscription_resource_routes_foreign_scope_to_owner() {
    let account = GraphAccount::new_for_tests_with_shared(
        GraphClient::new("token"),
        PushMode::GraphSubscriptions,
        &["shared@contoso.com".to_string()],
    );
    let scope = CursorScope::FolderType {
        folder: crate::account::foreign::encode_foreign("shared@contoso.com", "AAMk"),
        ty: ObjectType::Email,
    };
    // The owning mailbox rides in the `/users/{id}` segment and only
    // the native folder id is in `/mailFolders/{id}` - no raw foreign
    // id (no `%1F` separator) is percent-encoded into the URL.
    let resource = resource_for_scope(&account, &scope)
        .expect("configured mailbox")
        .expect("foreign scope resolves");
    assert_eq!(
        resource,
        "/users/shared%40contoso.com/mailFolders/AAMk/messages"
    );
    assert!(!resource.contains("%1F"));
}

#[tokio::test]
async fn webhook_creation_rolls_back_each_already_created_subscription() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            serde_json::json!({"id":"first","expirationDateTime":"2099-01-01T00:00:00Z"}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"code":"ErrorInternalServerError","message":"no"}}),
        ),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
    ]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.push_endpoint = Some(PushEndpoint {
        webhook_url: "https://example.test/hook".to_string(),
        client_state: "secret".to_string(),
    });
    let scopes = vec![
        CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        },
        CursorScope::FolderType {
            folder: FolderId("contacts".to_string()),
            ty: ObjectType::Contact,
        },
    ];
    assert!(push_subscribe(account.clone(), scopes).await.is_err());
    assert!(account.graph_subscriptions.read().await.is_empty());
    let requests = client.take_rest_requests();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        ["POST", "POST", "DELETE"]
    );
    assert!(requests[2].url.ends_with("/subscriptions/first"));
}

#[tokio::test]
async fn unsubscribe_graph_deletes_every_server_row() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("one", "/me/messages"),
            state("two", "/me/events"),
        ]),
    );
    unsubscribe_graph(account.clone(), handle)
        .await
        .expect("delete loop succeeds");
    assert!(account.graph_subscriptions.read().await.is_empty());
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].url.ends_with("/subscriptions/one"));
    assert!(requests[1].url.ends_with("/subscriptions/two"));
}

#[tokio::test]
async fn close_continues_after_one_subscription_delete_fails() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"code":"InternalServerError","message":"failed"}}),
        ),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle,
        GraphSubscriptionGroup::live(vec![
            state("fails", "/me/messages"),
            state("succeeds", "/me/events"),
        ]),
    );
    retire_all_graph_subscriptions(&account).await;
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].url.ends_with("/subscriptions/succeeds"));
}

/// A subscription Graph has already dropped must not fail teardown.
/// `subscription_is_gone` used to read only `GraphError::Response`,
/// which a REST call never produces - the transport converts a 404 into
/// `bifrost_net::Error::Status` first - so on the live path the 404
/// tolerance never applied, the DELETE loop aborted on the vanished row,
/// and the handle stayed registered with its remaining siblings
/// undeleted.
#[tokio::test]
async fn unsubscribe_tolerates_a_subscription_the_server_already_dropped() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        ),
        ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let handle = SubscriptionHandle("h".to_string());
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("vanished", "/me/messages"),
            state("live", "/me/events"),
        ]),
    );

    unsubscribe_graph(account.clone(), handle)
        .await
        .expect("a row the server already dropped is not a teardown failure");
    assert!(account.graph_subscriptions.read().await.is_empty());
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].url.ends_with("/subscriptions/live"));
}

#[test]
fn contact_scope_subscribes_to_the_contact_folder_collection() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: FolderId("contacts".to_string()),
        ty: ObjectType::Contact,
    };
    assert_eq!(
        resource_for_scope(&account, &scope)
            .expect("primary")
            .as_deref(),
        Some("/me/contactFolders/contacts/contacts")
    );
}

#[test]
fn unsubscribable_scopes_have_no_graph_resource() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    assert!(
        resource_for_scope(&account, &CursorScope::Account)
            .expect("primary")
            .is_none()
    );
    // A public folder is poll-only; it must not resolve to a resource.
    assert!(
        resource_for_scope(
            &account,
            &CursorScope::Folder(FolderId("AAMkPF=".to_string()))
        )
        .expect("primary")
        .is_none()
    );
    assert!(
        resource_for_scope(
            &account,
            &CursorScope::FolderType {
                folder: FolderId("inbox".to_string()),
                ty: ObjectType::Mailbox,
            }
        )
        .expect("primary")
        .is_none()
    );
}

#[test]
fn opaque_folder_ids_are_percent_encoded_into_the_resource() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: FolderId("AAMk/GI2=".to_string()),
        ty: ObjectType::Email,
    };
    let resource = resource_for_scope(&account, &scope)
        .expect("primary")
        .expect("email scope resolves");
    assert!(!resource.contains("AAMk/GI2="), "{resource}");
    assert!(resource.ends_with("/messages"), "{resource}");
}

/// A persisted scope whose shared mailbox was removed must fail before
/// it can be rewritten into a `/me` subscription resource.
#[test]
fn an_unconfigured_foreign_mailbox_is_rejected_locally() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: crate::account::foreign::encode_foreign("other@contoso.com", "AAMk"),
        ty: ObjectType::Email,
    };
    assert!(matches!(
        resource_for_scope(&account, &scope),
        Err(crate::error::GraphError::Configuration { .. })
    ));
}

#[tokio::test]
async fn webhook_mode_without_an_endpoint_is_unsupported() {
    // `with_push_endpoint` was never called: there is nowhere for Graph
    // to deliver, so subscribing must refuse rather than create a
    // subscription pointing at nothing.
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let scope = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    let err = push_subscribe(account, vec![scope])
        .await
        .expect_err("expected Unsupported");
    assert!(matches!(
        err.kind(),
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
    ));
}

/// Teardown removes only the subscription whose DELETE was confirmed.
/// If the next DELETE fails, the remaining server ids are still in the
/// group, so retrying the same handle can finish the cleanup rather than
/// orphaning notifications until Graph's expiry window closes.
#[test]
fn unsubscribe_retains_not_yet_deleted_subscriptions_for_retry() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("deleted", "/me/mailFolders/inbox/messages"),
            state("retry-me", "/me/events"),
            state("not-attempted", "/me/contacts"),
        ]),
    )]);

    assert!(!remove_subscription_from_groups(
        &mut groups,
        &handle,
        "deleted"
    ));
    let remaining = &groups[&handle].subscriptions;
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().any(|state| state.server_id == "retry-me"));
    assert!(
        remaining
            .iter()
            .any(|state| state.server_id == "not-attempted")
    );
}

/// Condemning is idempotent and monotone: a retry after a failed DELETE
/// re-marks the same group and returns the ids still to delete, while an
/// unknown handle stays a no-op (teardown may be retried after a reopen).
#[test]
fn marking_teardown_is_idempotent_and_unknown_handles_are_none() {
    let handle = SubscriptionHandle("h".to_string());
    let mut groups = HashMap::from([(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![state("retry-me", "/me/events")]),
    )]);
    assert_eq!(
        mark_group_tearing_down(&mut groups, &handle),
        Some(vec!["retry-me".to_string()])
    );
    assert!(groups[&handle].tearing_down);
    assert_eq!(
        mark_group_tearing_down(&mut groups, &handle),
        Some(vec!["retry-me".to_string()]),
        "a teardown retry sees the ids whose DELETE has not been confirmed"
    );
    assert!(
        mark_group_tearing_down(&mut groups, &SubscriptionHandle("never".to_string())).is_none()
    );
}

#[tokio::test]
async fn unsubscribing_an_unknown_handle_is_a_no_op() {
    // Idempotent teardown: the engine may retry `push_unsubscribe`
    // after a reopen, when the in-memory group map is already empty.
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    unsubscribe_graph(account, SubscriptionHandle("never-issued".to_string()))
        .await
        .expect("unknown handle unsubscribes cleanly");
}

// ---- cancellation safety of the webhook `push_subscribe` ----------------
//
// The scripted-wire helpers (`webhook_account`, `created`, `deleted`,
// `settle`, `groups_empty`) live in `fixtures`; the renewal suite uses them too.

fn two_resources() -> Vec<CursorScope> {
    vec![
        email_scope("inbox"),
        CursorScope::FolderType {
            folder: FolderId("contacts".to_string()),
            ty: ObjectType::Contact,
        },
    ]
}

/// A future dropped after its first poll, while the create loop has not run
/// to its end, must still complete the loop and roll back what it created
/// (the second create fails, so the first must be deleted). Inline, the drop
/// unwinds the loop wherever it is and the first subscription stays live on
/// Graph until its expiry.
///
/// A parked (`Pending`) create cannot be used here: the spawned task is
/// deliberately not cancelled by the drop, so it would wait on that create
/// too, and the scripted wire cannot release it.
///
/// Fails if `subscribe_graph` awaits `create_and_register` inline instead of
/// handing it to `tokio::spawn` (the first poll then either completes the
/// whole flow, failing the pending assertion, or is dropped mid-way).
#[tokio::test]
async fn dropping_subscribe_mid_create_loop_rolls_back_the_earlier_creates() {
    let client = GraphClient::new("token");
    client.script_rest([
        created("first"),
        ScriptedRestResponse::json(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"code":"ErrorInternalServerError","message":"no"}}),
        ),
        deleted(),
    ]);
    let account = webhook_account(&client);

    let mut fut = Box::pin(push_subscribe(account.clone(), two_resources()));
    assert!(
        futures::FutureExt::now_or_never(fut.as_mut()).is_none(),
        "the request is handed to a task and the caller waits"
    );
    drop(fut);

    assert!(
        settle(|| client.wire_attempts() == 3).await,
        "the abandoned request must still issue the rollback DELETE"
    );
    let requests = client.take_rest_requests();
    let last = requests.last().expect("requests recorded");
    assert_eq!(last.method.as_str(), "DELETE");
    assert!(last.url.ends_with("/subscriptions/first"));
    assert!(groups_empty(&account));
}

/// A future dropped after the creates but while registration is blocked must
/// not leave a registered group behind a handle nobody received.
///
/// Fails if the abandoned-result teardown in `run_subscribe` (the
/// `result_tx.send` failure arm) is removed.
#[tokio::test]
async fn dropping_subscribe_before_registration_tears_down_the_orphan() {
    let client = GraphClient::new("token");
    client.script_rest([created("first"), created("second"), deleted(), deleted()]);
    let account = webhook_account(&client);

    let registration_blocked = account.graph_subscriptions.write().await;
    let caller = tokio::spawn(push_subscribe(account.clone(), two_resources()));
    assert!(
        settle(|| client.wire_attempts() == 2).await,
        "both creates done"
    );
    caller.abort();
    let _ = caller.await;
    drop(registration_blocked);

    assert!(
        settle(|| client.wire_attempts() == 4).await,
        "the orphaned group's subscriptions are deleted"
    );
    assert!(
        settle(|| groups_empty(&account)).await,
        "no group stays registered under a handle nobody holds"
    );
}

/// The result was delivered into the channel but the future was dropped
/// before it took it. A successful `oneshot::send` is not receipt, so the
/// task waits for the waiter's ack.
///
/// Fails if `run_subscribe` stops waiting for `ack_rx` (treating a
/// successful send as receipt).
#[tokio::test]
async fn dropping_subscribe_with_the_result_in_flight_tears_down_the_group() {
    let client = GraphClient::new("token");
    client.script_rest([created("only"), deleted()]);
    let account = webhook_account(&client);

    let mut fut = Box::pin(push_subscribe(account.clone(), vec![email_scope("inbox")]));
    assert!(
        futures::FutureExt::now_or_never(fut.as_mut()).is_none(),
        "spawned, awaiting"
    );
    assert!(
        settle(|| !groups_empty(&account)).await,
        "the task registered the group and delivered its result"
    );
    drop(fut);

    assert!(
        settle(|| groups_empty(&account)).await,
        "an unreceived handle's group is retired"
    );
    let requests = client.take_rest_requests();
    assert_eq!(requests.last().expect("recorded").method.as_str(), "DELETE");
}

/// The normal path must disarm: a caller that receives the handle keeps the
/// registered group and nothing is deleted behind its back.
///
/// Fails if the waiter stops acking (`ack_tx.send`), because the task then
/// retires a group the caller legitimately holds.
#[tokio::test]
async fn a_received_subscribe_is_not_torn_down() {
    let client = GraphClient::new("token");
    client.script_rest([created("only")]);
    let account = webhook_account(&client);

    let subscription = push_subscribe(account.clone(), vec![email_scope("inbox")])
        .await
        .expect("subscribe succeeds");
    let handle = subscription.handle.clone().expect("a handle");
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert!(
        account
            .graph_subscriptions
            .read()
            .await
            .contains_key(&handle)
    );
    assert_eq!(client.wire_attempts(), 1, "no DELETE was issued");
}

/// Nothing is written until the future is first polled, so a future dropped
/// before its first poll has nothing to leak.
#[tokio::test]
async fn a_subscribe_dropped_before_its_first_poll_touches_nothing() {
    let client = GraphClient::new("token");
    client.script_rest([]);
    let account = webhook_account(&client);

    drop(push_subscribe(account.clone(), two_resources()));
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert_eq!(client.wire_attempts(), 0);
    assert!(groups_empty(&account));
}

/// A closed account refuses before creating anything server-side.
///
/// Fails if the early `shutdown.is_cancelled()` check in `subscribe_graph` is
/// removed.
#[tokio::test]
async fn subscribing_on_a_closed_account_creates_nothing() {
    let client = GraphClient::new("token");
    client.script_rest([]);
    let account = webhook_account(&client);
    account.shutdown.cancel();

    assert!(
        push_subscribe(account.clone(), two_resources())
            .await
            .is_err()
    );
    assert_eq!(client.wire_attempts(), 0);
}

/// `close()` began while a subscribe was already past its creates: the
/// registration must refuse and roll back instead of installing a group the
/// finished `close()` walk will never see.
///
/// Fails if the `shutdown.is_cancelled()` check under the write lock in
/// `create_and_register` is removed.
#[tokio::test]
async fn a_subscribe_racing_close_rolls_back_instead_of_registering() {
    let client = GraphClient::new("token");
    client.script_rest([created("only"), deleted()]);
    let account = webhook_account(&client);

    let registration_blocked = account.graph_subscriptions.write().await;
    let caller = tokio::spawn(push_subscribe(account.clone(), vec![email_scope("inbox")]));
    assert!(settle(|| client.wire_attempts() == 1).await, "create done");
    account.shutdown.cancel();
    drop(registration_blocked);

    let result = caller.await.expect("caller not aborted");
    assert!(result.is_err(), "a closing account hands out no handle");
    assert_eq!(client.wire_attempts(), 2, "the create was rolled back");
    assert!(groups_empty(&account));
}

/// `close()`'s retire walk cancels the token first: that is what pairs with
/// the registration-time check above.
///
/// Fails if `retire_all_graph_subscriptions` stops cancelling the token.
#[tokio::test]
async fn retiring_all_subscriptions_cancels_the_shutdown_token_first() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    retire_all_graph_subscriptions(&account).await;
    assert!(account.shutdown.is_cancelled());
}

// ---- orphan handles: teardown on an instance that never saw the subscribe ----

fn urls(client: &GraphClient) -> Vec<(String, String)> {
    client
        .take_rest_requests()
        .into_iter()
        .map(|request| (request.method, request.url))
        .collect()
}

/// The handle `push_subscribe` returns must name the server ids, or a reopened
/// instance has nothing to delete by.
#[tokio::test]
async fn a_subscribed_handle_embeds_its_account_and_the_created_server_ids() {
    let client = GraphClient::new("token");
    client.attach_account(AccountId("acct-a".to_string()));
    client.script_rest([created("sub-a")]);
    let account = webhook_account(&client);
    let subscription = push_subscribe(account.clone(), vec![email_scope("inbox")])
        .await
        .expect("subscribe");
    let handle = subscription.handle.expect("a handle");
    assert_eq!(
        decode_handle(&handle),
        DecodedHandle::Graph {
            account: Some(account_binding(&AccountId("acct-a".to_string()))),
            ids: vec!["sub-a".to_string()],
        }
    );
    assert!(
        account
            .graph_subscriptions
            .read()
            .await
            .contains_key(&handle),
        "the registered key is the returned handle"
    );
}

/// The filed defect: the old instance's teardown failed, the account was
/// reopened, and the engine retries on the NEW instance, whose map is empty.
#[tokio::test]
async fn an_orphan_handle_is_torn_down_by_a_fresh_instance() {
    let client = GraphClient::new("token");
    client.script_rest([deleted(), deleted()]);
    let fresh = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let orphan = graph_handle("ab12", None, ["old-one", "old-two"]);

    push_unsubscribe(fresh, orphan)
        .await
        .expect("orphan teardown succeeds");
    let requests = urls(&client);
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(requests[0].0, "DELETE");
    assert!(requests[0].1.ends_with("/subscriptions/old-one"));
    assert_eq!(requests[1].0, "DELETE");
    assert!(requests[1].1.ends_with("/subscriptions/old-two"));
}

/// A row Graph already expired is done, and its siblings are still deleted.
#[tokio::test]
async fn an_orphan_teardown_treats_404_and_410_as_done() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::NOT_FOUND,
            serde_json::json!({"error":{"code":"ResourceNotFound","message":"gone"}}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::GONE,
            serde_json::json!({"error":{"code":"Gone","message":"gone"}}),
        ),
        deleted(),
    ]);
    let fresh = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    push_unsubscribe(fresh, graph_handle("ab12", None, ["a", "b", "c"]))
        .await
        .expect("already-gone rows are not failures");
    assert_eq!(client.take_rest_requests().len(), 3);
}

/// A failed DELETE is returned (the engine keeps the orphan and retries), but
/// it does not stop the remaining ids being attempted.
#[tokio::test]
async fn an_orphan_teardown_attempts_every_id_and_reports_a_failure() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"code":"InternalServerError","message":"no"}}),
        ),
        deleted(),
    ]);
    let fresh = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    push_unsubscribe(fresh, graph_handle("ab12", None, ["a", "b"]))
        .await
        .expect_err("the failed DELETE is reported");
    assert_eq!(client.take_rest_requests().len(), 2);
}

/// Two live instances during a reopen: the old instance's orphan, retried on
/// the new one, deletes the OLD ids only. The new instance's own subscription
/// survives, and an id this instance still owns is never deleted through a
/// handle that is not the one it registered.
#[tokio::test]
async fn an_orphan_never_deletes_what_this_instance_owns() {
    let client = GraphClient::new("token");
    client.script_rest([deleted()]);
    let new_instance = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let own = graph_handle("cd34", None, ["new-sub"]);
    new_instance.graph_subscriptions.write().await.insert(
        own.clone(),
        GraphSubscriptionGroup::live(vec![state("new-sub", "/me/events")]),
    );

    // A stale or forged handle that names both the old id and the live one.
    push_unsubscribe(
        new_instance.clone(),
        graph_handle("ab12", None, ["old-sub", "new-sub"]),
    )
    .await
    .expect("orphan teardown");

    let requests = urls(&client);
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(requests[0].1.ends_with("/subscriptions/old-sub"));
    assert!(
        new_instance
            .graph_subscriptions
            .read()
            .await
            .contains_key(&own),
        "the live group is untouched"
    );
}

/// The renewal session replaced a vanished subscription under a handle, the
/// old instance's teardown never landed, and the engine retries the handle on
/// a reopened instance of the same factory. The handle string names only the
/// subscribe-time id; the replacement is reached through the factory-shared
/// ledger, and forgotten once deleted.
#[tokio::test]
async fn an_orphan_teardown_reaches_a_subscription_recreated_on_the_old_instance() {
    let old_client = GraphClient::new("token");
    old_client.script_rest([created("recreated")]);
    let old = webhook_account(&old_client);
    let handle = graph_handle("ab12", None, ["original"]);
    old.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![state("original", "/me/events")]),
    );
    let endpoint = old.push_endpoint.clone().expect("webhook endpoint");
    let outcome = replace_gone_subscription(
        &old,
        &endpoint,
        &handle,
        "original",
        "/me/events",
        &[email_scope("/me/events")],
    )
    .await
    .expect("the replacement is created");
    assert_eq!(outcome, Replacement::Installed);

    let new_client = GraphClient::new("token");
    new_client.script_rest([deleted(), deleted()]);
    let mut fresh = GraphAccount::new_for_tests(new_client.clone(), PushMode::GraphSubscriptions);
    // What `GraphAccountFactory::open` does for every account it opens.
    fresh.recreated_subscription_ids = std::sync::Arc::clone(&old.recreated_subscription_ids);

    push_unsubscribe(fresh.clone(), handle.clone())
        .await
        .expect("orphan teardown succeeds");
    let requests = urls(&new_client);
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(requests[0].1.ends_with("/subscriptions/original"));
    assert!(
        requests[1].1.ends_with("/subscriptions/recreated"),
        "the recreated subscription is deleted too: {requests:?}"
    );
    assert!(
        recreated_ids(&fresh).is_empty(),
        "a completed teardown forgets what it deleted"
    );
}

/// A recreate of a recreate replaces the ledger's entry rather than growing
/// it, and a group whose last subscription is deleted takes its entry along.
#[tokio::test]
async fn the_recreated_id_ledger_follows_the_group() {
    let mut ledger = HashMap::new();
    let handle = graph_handle("ab12", None, ["original"]);
    record_recreated_id(&mut ledger, &handle, "original", "first");
    record_recreated_id(&mut ledger, &handle, "first", "second");
    assert_eq!(ledger.get(&handle), Some(&vec!["second".to_string()]));

    let client = GraphClient::new("token");
    client.script_rest([deleted()]);
    let account = webhook_account(&client);
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![state("second", "/me/events")]),
    );
    *recreated_ids(&account) = ledger;
    unsubscribe_graph(account.clone(), handle.clone())
        .await
        .expect("teardown succeeds");
    assert!(recreated_ids(&account).is_empty());
}

/// A terminal renewal failure flags a recreated subscription instead of
/// dropping its row, so the handle's teardown still deletes it and the ledger
/// forgets it only then.
#[tokio::test]
async fn a_recreated_id_flagged_by_a_terminal_renewal_failure_is_still_torn_down() {
    let client = GraphClient::new("token");
    client.script_rest([deleted(), deleted()]);
    let account = webhook_account(&client);
    let handle = graph_handle("ab12", None, ["other"]);
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![
            state("other", "/me/events"),
            state("recreated", "/me/contacts"),
        ]),
    );
    record_recreated_id(&mut recreated_ids(&account), &handle, "gone", "recreated");
    // The renewal session's terminal-failure path.
    mark_subscription_terminated(&account, &handle, "recreated").await;
    assert_eq!(
        recreated_ids(&account).get(&handle),
        Some(&vec!["recreated".to_string()]),
        "flagging deletes nothing, so the ledger keeps the id"
    );

    unsubscribe_graph(account.clone(), handle)
        .await
        .expect("teardown succeeds");
    let requests = urls(&client);
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(requests[0].1.ends_with("/subscriptions/other"));
    assert!(requests[1].1.ends_with("/subscriptions/recreated"));
    assert!(recreated_ids(&account).is_empty());
}

/// When the terminated subscription is its group's ONLY one, the group must
/// survive for `close()` to reach it. Removing the row used to remove the
/// group, and `close()` - which walks registered groups - never sent the
/// DELETE while Graph kept delivering until expiry.
#[tokio::test]
async fn close_deletes_a_subscription_whose_renewal_failed_terminally() {
    let client = GraphClient::new("token");
    client.script_rest([deleted()]);
    let account = webhook_account(&client);
    let handle = graph_handle("ab12", None, ["only"]);
    account.graph_subscriptions.write().await.insert(
        handle.clone(),
        GraphSubscriptionGroup::live(vec![state("only", "/me/events")]),
    );
    mark_subscription_terminated(&account, &handle, "only").await;

    retire_all_graph_subscriptions(&account).await;
    let requests = urls(&client);
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0].0, "DELETE");
    assert!(requests[0].1.ends_with("/subscriptions/only"));
    assert!(account.graph_subscriptions.read().await.is_empty());
}

/// A handle that is not a well-formed webhook handle chooses nothing to
/// delete, whatever it contains.
#[tokio::test]
async fn a_forged_handle_issues_no_requests() {
    let client = GraphClient::new("token");
    let fresh = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    for raw in [
        "graph1:ab12:../me/messages",
        "graph1:ab12:a?x=1",
        "graph1:nothex:id",
        "h",
    ] {
        push_unsubscribe(fresh.clone(), SubscriptionHandle(raw.to_string()))
            .await
            .expect("unrecognized handles are a no-op");
    }
    assert!(client.take_rest_requests().is_empty());
}

/// A handle names the account that minted it. Handed to an instance of a
/// DIFFERENT account - reachable with this account's credentials when both
/// sit in one tenant - it deletes nothing; handed to an instance of the same
/// account, as a reopen does, it tears its orphan down.
#[tokio::test]
async fn an_orphan_bound_to_another_account_deletes_nothing() {
    let client = GraphClient::new("token");
    client.attach_account(AccountId("acct-a".to_string()));
    client.script_rest([deleted()]);
    let instance = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);

    let foreign = account_binding(&AccountId("acct-b".to_string()));
    push_unsubscribe(
        instance.clone(),
        graph_handle("ab12", Some(&foreign), ["their-sub"]),
    )
    .await
    .expect("another account's handle is not this instance's to retire");
    assert!(
        client.take_rest_requests().is_empty(),
        "another account's subscription must not be deleted"
    );

    let own = account_binding(&AccountId("acct-a".to_string()));
    push_unsubscribe(instance, graph_handle("ab12", Some(&own), ["my-old-sub"]))
        .await
        .expect("the same account's orphan tears down");
    let requests = urls(&client);
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(requests[0].1.ends_with("/subscriptions/my-old-sub"));
}

/// A reopen can switch the push mode. The handle decides, not the mode: a
/// webhook orphan on an EWS instance still deletes, and an EWS handle on a
/// webhook instance touches nothing (EWS state dies with the connection).
#[tokio::test]
async fn orphan_routing_follows_the_handle_not_the_instance_mode() {
    let client = GraphClient::new("token");
    client.script_rest([deleted()]);
    let ews = GraphAccount::new_for_tests(client.clone(), PushMode::EwsStreaming);
    push_unsubscribe(ews, graph_handle("ab12", None, ["old"]))
        .await
        .expect("teardown");
    assert_eq!(client.take_rest_requests().len(), 1);

    let webhook = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    push_unsubscribe(webhook, ews_handle("ab12"))
        .await
        .expect("no-op");
    assert!(client.take_rest_requests().is_empty());
}
