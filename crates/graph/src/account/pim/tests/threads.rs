use crate::account::foreign::encode_foreign;
use crate::account::pim::containers::{DELETED_ITEMS, container_is_trash, trash_container_id};
use crate::account::pim::messages::resolve_target_ids;
use crate::account::pim::threads::{delete_thread, thread_hydrate};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};
use bifrost_types::{
    AccountErrorKind, AccountOperation, ContainerId, ErrorScope, MutationTarget, ObjectId,
    ProtocolErrorKind, ThreadId,
};
use serde_json::{Value, json};
use std::collections::HashMap;

#[tokio::test]
async fn foreign_thread_members_are_queried_and_requalified_on_the_owner_client() {
    let primary = GraphClient::new("token");
    primary.script_rest(std::iter::empty::<ScriptedRestResponse>());
    let shared_root = GraphClient::new("token");
    shared_root.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "value": [{
                "id": "AAMkmsg",
                "conversationId": "conversation-1",
                "parentFolderId": "AAMkfolder",
            }] }),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "value": [{ "id": "AAMkmsg" }] }),
        ),
    ]);
    let mailbox = "shared@contoso.com";
    let shared = shared_root.for_shared_mailbox(mailbox);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), shared)]),
    );
    let thread = ThreadId(format!("{mailbox}\u{1f}conversation-1"));

    let hydrated = thread_hydrate(account.clone(), thread.clone())
        .await
        .expect("shared thread hydrates");
    assert_eq!(hydrated.messages.len(), 1);
    assert_eq!(hydrated.messages[0].id.0, format!("{mailbox}\u{1f}AAMkmsg"));
    assert_eq!(hydrated.messages[0].thread_id, Some(thread.clone()));
    // `parentFolderId` comes back bare from `/users/{owner}`; it has to
    // be re-qualified or it neither joins the `Shared`-namespace
    // containers `containers_list` emits nor stays distinguishable from
    // a primary folder that happens to share the id.
    assert_eq!(
        hydrated.messages[0].containers,
        vec![ContainerId(format!("{mailbox}\u{1f}AAMkfolder"))]
    );

    let ids = resolve_target_ids(
        &account,
        MutationTarget::Thread(thread),
        AccountOperation::SetKeyword,
    )
    .await
    .expect("shared thread members resolve");

    assert_eq!(ids, vec![ObjectId(format!("{mailbox}\u{1f}AAMkmsg"))]);
    assert!(primary.take_rest_requests().is_empty());
    let requests = shared_root.take_rest_requests();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert!(
            request
                .url
                .contains("/users/shared%40contoso.com/messages?")
        );
        assert!(
            request
                .url
                .contains("conversationId%20eq%20%27conversation-1%27")
        );
    }
}

/// The complete `delete_thread` operation for a shared thread, not just
/// its id resolution.
///
/// Every read leg - the conversation query, the Trash lookup, and the
/// etag preflight - must run on the
/// owner's client, and the Trash id must come back owner-qualified. The
/// primary is armed with EXACTLY the one response the operation
/// legitimately needs (the `/$batch` POST, which is account-wide and
/// carries its routing in the subrequest URLs), so any leg that fell
/// back to `/me` consumes that response and the next primary request
/// hits the seam's exhaustion panic.
///
/// Resolving Trash on the primary is not a silent wrong-mailbox write
/// either way: `move_messages` refuses a destination whose owner
/// differs from the message's, so the bug surfaced as a bare primary
/// folder id being rejected against foreign-owned messages.
#[tokio::test]
async fn deleting_a_shared_thread_resolves_trash_in_the_owner_mailbox() {
    let mailbox = "shared@contoso.com";
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "responses": [{ "id": "0", "status": 200 }] }),
    )]);
    let shared_root = GraphClient::new("token");
    shared_root.script_rest([
        // 1. the conversation members
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [{
                    "id": "AAMkmsg",
                    "parentFolderId": "InboxId",
                    "changeKey": "CK0"
                }]
            }),
        ),
        // 2. The shared mailbox's own Trash id. Deliberately different
        // from any primary folder id: the primary's would name nothing.
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "id": "SharedTrash" })),
        // 3. the move's etag preflight
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "id": "AAMkmsg", "changeKey": "CK1" }),
        ),
    ]);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), shared_root.for_shared_mailbox(mailbox))]),
    );

    delete_thread(
        account,
        ThreadId(format!("{mailbox}\u{1f}conversation-1")),
        None,
    )
    .await
    .expect("shared thread deletes");

    let shared_requests = shared_root.take_rest_requests();
    assert_eq!(shared_requests.len(), 3);
    for request in &shared_requests {
        assert!(
            request.url.contains("/users/shared%40contoso.com/"),
            "every read leg routes through the owner: {}",
            request.url
        );
    }
    assert!(
        shared_requests[1]
            .url
            .ends_with("/users/shared%40contoso.com/mailFolders/deletedItems?$select=id"),
        "Trash is resolved against the owner's own well-known folders: {}",
        shared_requests[1].url
    );

    // The primary carried the `/$batch` envelope and nothing else. The
    // destination is the SHARED mailbox's Trash, sent native (the
    // `destinationId` body may never carry a `\u{1f}` id).
    let primary_requests = primary.take_rest_requests();
    assert_eq!(primary_requests.len(), 1);
    assert!(primary_requests[0].url.ends_with("/$batch"));
    let body = primary_requests[0].body.as_ref().expect("batch body");
    let sub = &body["requests"][0];
    assert_eq!(
        sub["url"],
        json!("/users/shared%40contoso.com/messages/AAMkmsg/move")
    );
    assert_eq!(sub["body"]["destinationId"], json!("SharedTrash"));
}

/// The cache is keyed PER MAILBOX, and each key resolves against its
/// own client. A single shared slot would hand one mailbox's Trash id to
/// another, which is a folder id that names nothing there - so the two
/// mailboxes are armed with different ids on independent scripts, and
/// each is asserted to have been asked exactly once.
#[tokio::test]
async fn trash_lookup_is_cached_per_mailbox() {
    let mailbox = "shared@contoso.com";
    let primary = GraphClient::new("token");
    primary.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "id": "PrimaryTrash" }),
    )]);
    let shared_root = GraphClient::new("token");
    shared_root.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({ "id": "SharedTrash" }),
    )]);
    let account = GraphAccount::new_for_tests_with_shared_clients(
        primary.clone(),
        PushMode::GraphSubscriptions,
        HashMap::from([(mailbox.to_string(), shared_root.for_shared_mailbox(mailbox))]),
    );

    for round in 0..2 {
        assert_eq!(
            trash_container_id(&account, None, ErrorScope::Account)
                .await
                .expect("primary Trash resolves"),
            ContainerId("PrimaryTrash".to_string()),
            "round {round}"
        );
        assert_eq!(
            trash_container_id(&account, Some(mailbox), ErrorScope::Account)
                .await
                .expect("shared Trash resolves"),
            ContainerId(format!("{mailbox}\u{1f}SharedTrash")),
            "round {round}"
        );
    }

    assert_eq!(primary.take_rest_requests().len(), 1);
    let shared_requests = shared_root.take_rest_requests();
    assert_eq!(shared_requests.len(), 1);
    assert!(
        shared_requests[0]
            .url
            .ends_with("/users/shared%40contoso.com/mailFolders/deletedItems?$select=id")
    );
}

/// Only a RESOLVED folder id may enter the cache, and a lookup that did
/// not resolve must fail rather than answer with the well-known NAME.
///
/// `deletedItems` is a valid move destination, so a degraded answer
/// keeps `delete_thread` looking healthy - but it is not the concrete id
/// `container_is_trash` compares against, so a thread ALREADY in Trash
/// gets moved there again and reports success instead of being
/// destroyed. Caching it made one transient 503 do that for the rest of
/// the account's life.
#[tokio::test]
async fn a_failed_trash_lookup_neither_answers_nor_caches() {
    let client = GraphClient::new("token");
    client.script_rest([
        // 1. transient failure
        ScriptedRestResponse::empty(reqwest::StatusCode::SERVICE_UNAVAILABLE),
        // 2. a 200 that carries no folder id
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({})),
        // 3. the real answer, which is the only one worth keeping
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "id": "PrimaryTrash" })),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);

    let transient = trash_container_id(&account, None, ErrorScope::Account)
        .await
        .expect_err("a transient lookup failure propagates");
    assert!(transient.recovery().is_retryable());
    let no_id = trash_container_id(&account, None, ErrorScope::Account)
        .await
        .expect_err("a 200 without an id is a contract violation");
    assert!(matches!(
        no_id.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    ));
    assert_eq!(no_id.scope(), Some(&ErrorScope::Account));

    let resolved = trash_container_id(&account, None, ErrorScope::Account)
        .await
        .expect("the lookup resolves once Graph answers");
    assert_eq!(resolved, ContainerId("PrimaryTrash".to_string()));
    let cached = trash_container_id(&account, None, ErrorScope::Account)
        .await
        .expect("the resolved id is cached");
    assert_eq!(cached, resolved);
    // Three lookups, not four: only the resolved one was retained.
    assert_eq!(client.take_rest_requests().len(), 3);
}

#[test]
fn container_is_trash_compares_in_the_thread_owners_namespace() {
    let mailbox = "shared@contoso.com";
    let shared_trash = ContainerId(format!("{mailbox}\u{1f}SharedTrash"));

    assert!(container_is_trash(
        &shared_trash,
        &shared_trash,
        Some(mailbox)
    ));
    // The well-known-NAME fallback still applies, but only qualified.
    assert!(container_is_trash(
        &ContainerId(format!("{mailbox}\u{1f}deletedItems")),
        &shared_trash,
        Some(mailbox)
    ));
    // A BARE `deletedItems` names the PRIMARY mailbox's Trash. Accepting
    // it for a shared thread would take the destroy branch and delete
    // messages that were never in their own Trash - unrecoverable, where
    // a redundant move is not.
    assert!(!container_is_trash(
        &ContainerId(DELETED_ITEMS.to_string()),
        &shared_trash,
        Some(mailbox)
    ));
    // Another mailbox's Trash is not this thread's Trash either.
    assert!(!container_is_trash(
        &ContainerId("other@contoso.com\u{1f}deletedItems".to_string()),
        &shared_trash,
        Some(mailbox)
    ));
    // The primary keeps its bare form on both sides.
    assert!(container_is_trash(
        &ContainerId(DELETED_ITEMS.to_string()),
        &ContainerId("PrimaryTrash".to_string()),
        None
    ));
}

#[tokio::test]
async fn deleting_a_thread_from_trash_permanently_deletes_in_its_owner_mailbox() {
    for shared in [false, true] {
        let mailbox = "shared@contoso.com";
        let primary = GraphClient::new("token");
        let owner_root = if shared {
            GraphClient::new("token")
        } else {
            primary.clone()
        };
        owner_root.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({
                    "value": [{
                        "id": "message",
                        "parentFolderId": "TrashId",
                        "changeKey": "version"
                    }]
                }),
            ),
            ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "id": "TrashId" })),
        ]);
        primary.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "responses": [{ "id": "0", "status": 204 }] }),
        )]);
        let clients = if shared {
            HashMap::from([(mailbox.to_string(), owner_root.for_shared_mailbox(mailbox))])
        } else {
            HashMap::new()
        };
        let account = GraphAccount::new_for_tests_with_shared_clients(
            primary.clone(),
            PushMode::GraphSubscriptions,
            clients,
        );
        let (thread, current, prefix) = if shared {
            (
                ThreadId(format!("{mailbox}\u{1f}conversation")),
                ContainerId(encode_foreign(mailbox, "TrashId").0),
                "/users/shared%40contoso.com",
            )
        } else {
            (
                ThreadId("conversation".to_string()),
                ContainerId("TrashId".to_string()),
                "/me",
            )
        };
        delete_thread(account, thread, Some(current))
            .await
            .expect("thread expunged");
        let reads = owner_root.take_rest_requests();
        assert_eq!(reads.len(), if shared { 2 } else { 3 });
        for request in &reads[..2] {
            assert!(
                request.url.contains(&format!("{prefix}/")),
                "{}",
                request.url
            );
        }
        let envelopes = if shared {
            primary.take_rest_requests()
        } else {
            reads[2..].to_vec()
        };
        assert_eq!(envelopes.len(), 1);
        let sub = &envelopes[0].body.as_ref().expect("batch body")["requests"][0];
        assert_eq!(sub["method"], "POST");
        assert_eq!(
            sub["url"],
            format!("{prefix}/messages/message/permanentDelete")
        );
        assert_eq!(sub["headers"]["If-Match"], "W/\"version\"");
        assert!(sub.get("body").is_none_or(Value::is_null));
    }
}

#[tokio::test]
async fn deleting_a_thread_from_trash_only_destroys_members_in_trash() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({
                "value": [
                    {
                        "id": "sent-copy",
                        "parentFolderId": "SentId",
                        "changeKey": "sent-etag"
                    },
                    {
                        "id": "inbox-copy",
                        "parentFolderId": "InboxId",
                        "changeKey": "inbox-etag"
                    },
                    {
                        "id": "trash-copy",
                        "parentFolderId": "TrashId",
                        "changeKey": "trash-etag"
                    }
                ]
            }),
        ),
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "id": "TrashId" })),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "responses": [{ "id": "0", "status": 204 }] }),
        ),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);

    delete_thread(
        account,
        ThreadId("conversation".to_string()),
        Some(ContainerId("TrashId".to_string())),
    )
    .await
    .expect("only the Trash copy is permanently deleted");

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].url.contains("/me/messages?"));
    assert!(requests[0].url.contains("parentFolderId"));
    assert!(requests[0].url.contains("changeKey"));
    let batch = requests[2].body.as_ref().expect("batch body");
    let items = batch["requests"].as_array().expect("batch items");
    assert_eq!(items.len(), 1, "Sent and Inbox copies stay untouched");
    assert_eq!(items[0]["headers"]["If-Match"], "W/\"trash-etag\"");
    assert_eq!(
        items[0]["url"],
        json!("/me/messages/trash-copy/permanentDelete")
    );
}

#[tokio::test]
async fn partial_commit_delete_thread_keeps_earlier_chunks() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(reqwest::StatusCode::OK,json!({"value":[{"id":"a","parentFolderId":"TrashId","changeKey":"a-tag"},{"id":"b","parentFolderId":"TrashId","changeKey":"b-tag"}]})),
        ScriptedRestResponse::json(reqwest::StatusCode::OK,json!({"id":"TrashId"})),
        ScriptedRestResponse::json(reqwest::StatusCode::OK,json!({"responses":[{"id":"0","status":204}]})),
        ScriptedRestResponse::json(reqwest::StatusCode::OK,json!({"responses":[{"id":"0","status":403,"body":{"error":{"code":"ErrorAccessDenied"}}}]})),
    ]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.capabilities.batching_policy.max_items = 1;
    let error = delete_thread(
        account,
        ThreadId("conversation".into()),
        Some(ContainerId("TrashId".into())),
    )
    .await
    .unwrap_err();
    let record = error.partial_commit().unwrap();
    assert_eq!(record.steps().len(), 1);
    assert_eq!(
        record.steps()[0].effect(),
        Some(bifrost_types::StepEffect::Deleted)
    );
    assert_eq!(
        record.steps()[0].targets().refs[0].scope,
        ErrorScope::Message {
            id: ObjectId("a".into())
        }
    );
    assert_eq!(client.take_rest_requests().len(), 4);
}

/// A chunk refused locally, before its `$batch` is sent, still reports the
/// chunks already committed. The refusal comes from `message_batch_url`: the
/// server named a member id holding the shared-mailbox separator, so it
/// parses as a message of a mailbox this account does not have.
#[tokio::test]
async fn partial_commit_delete_thread_keeps_earlier_chunks_on_a_local_refusal() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({"value":[
                {"id":"a","parentFolderId":"TrashId","changeKey":"a-tag"},
                {"id":"rogue\u{1f}b","parentFolderId":"TrashId","changeKey":"b-tag"}
            ]}),
        ),
        ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({"id":"TrashId"})),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({"responses":[{"id":"0","status":204}]}),
        ),
    ]);
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.capabilities.batching_policy.max_items = 1;
    let error = delete_thread(
        account,
        ThreadId("conversation".into()),
        Some(ContainerId("TrashId".into())),
    )
    .await
    .unwrap_err();
    assert_eq!(
        client.take_rest_requests().len(),
        3,
        "the refused chunk is never sent"
    );
    let record = error
        .partial_commit()
        .expect("the first chunk's destroy committed");
    assert_eq!(
        record.primary().application,
        bifrost_types::Application::NotApplied
    );
    assert_eq!(
        record.primary().targets.refs[0].scope,
        ErrorScope::Message {
            id: ObjectId("rogue\u{1f}b".into())
        }
    );
    assert_eq!(record.steps().len(), 1);
    assert_eq!(
        record.steps()[0].effect(),
        Some(bifrost_types::StepEffect::Deleted)
    );
    assert_eq!(
        record.steps()[0].targets().refs[0].scope,
        ErrorScope::Message {
            id: ObjectId("a".into())
        }
    );
}
