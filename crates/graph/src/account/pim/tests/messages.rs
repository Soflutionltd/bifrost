use crate::account::foreign::{encode_foreign, encode_message_id, encode_public_item_id};
use crate::account::pim::common::graph_extended_property_id;
use crate::account::pim::messages::{
    BatchProgress, STARRED_CATEGORY, add_to_container, batch_progress_error, categories_from_value,
    delete_extended_property, graph_importance_body, is_starred_category, message_batch_url,
    set_extended_property, set_is_read, submit_write_batch_with_targets,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};
use crate::etag::GraphETag;
use crate::types::BatchRequestItem;
use bifrost_types::{
    AccountErrorKind, AccountOperation, ContainerId, CursorScope, ErrorScope, FolderId, Importance,
    MutationTarget, ObjectId, ObjectType, RecoveryClass, RequestErrorKind, ThreadId,
};
use serde_json::json;

fn foreign_message_id(mailbox: &str, folder: &str, native: &str) -> ObjectId {
    let scope = CursorScope::FolderType {
        folder: encode_foreign(mailbox, folder),
        ty: ObjectType::Email,
    };
    encode_message_id(&scope, native)
}

fn shared_account() -> GraphAccount {
    GraphAccount::new_for_tests_with_shared(
        GraphClient::new("token"),
        PushMode::GraphSubscriptions,
        &["shared@contoso.com".to_string()],
    )
}

/// Clearing a property can change the parent message's validator. Evict that
/// observation while preserving the message identities in partial progress.
#[tokio::test]
async fn an_extended_property_clear_evicts_the_message_etag() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({
            "responses": [
                {"id":"0", "status":204},
                {"id":"1", "status":403, "body":{"error":{"code":"ErrorAccessDenied"}}}
            ]
        }),
    )]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let cleared = ObjectId("cleared".to_string());
    let refused = ObjectId("refused".to_string());
    account
        .etag_index
        .write()
        .await
        .insert(cleared.0.clone(), GraphETag::from_odata_annotation("tag"));
    let error = delete_extended_property(
        &account,
        &[cleared.clone(), refused.clone()],
        "String {00020329-0000-0000-C000-000000000046} Name x",
        AccountOperation::SetExtendedProperty,
    )
    .await
    .unwrap_err();
    assert_eq!(account.etag_index.write().await.get(&cleared.0), None);
    let record = error.partial_commit().expect("one clear committed");
    assert_eq!(
        record.primary().targets.refs[0].scope,
        ErrorScope::Message { id: refused }
    );
    assert_eq!(
        record.steps()[0].targets().refs[0].scope,
        ErrorScope::Message { id: cleared }
    );
}

#[tokio::test]
async fn write_batch_drains_past_a_failure_to_evict_a_later_destroy_etag() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        json!({
            "responses": [
                {"id":"0", "status":400, "body":{"error":{"code":"BadRequest"}}},
                {"id":"1", "status":204}
            ]
        }),
    )]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let failed = ObjectId("failed".to_string());
    let destroyed = ObjectId("destroyed".to_string());
    account.etag_index.write().await.insert(
        destroyed.0.clone(),
        GraphETag::from_odata_annotation("stale"),
    );
    let requests = vec![
        BatchRequestItem {
            id: "0".to_string(),
            method: "PATCH".to_string(),
            url: "/me/messages/failed".to_string(),
            body: None,
            headers: None,
        },
        BatchRequestItem {
            id: "1".to_string(),
            method: "DELETE".to_string(),
            url: "/me/messages/destroyed".to_string(),
            body: None,
            headers: None,
        },
    ];
    let progress = submit_write_batch_with_targets(
        &account,
        requests,
        &[failed, destroyed.clone()],
        crate::account::batch::AbsentMeans::Success,
        AccountOperation::BulkDestroy,
    )
    .await
    .unwrap();
    assert_eq!(progress.failed.len(), 1);
    assert_eq!(progress.committed.len(), 1);
    assert_eq!(account.etag_index.write().await.get(&destroyed.0), None);
    assert_eq!(client.take_rest_requests().len(), 1);
}

/// A shared-mailbox client is derived inside `GraphAccount::new`, so a
/// test never holds it. It shares the primary's script - the way it
/// already shares the `AccountNet` a request actually
/// goes down - or every foreign-mailbox request would sit outside both
/// the script and the recorded list.
#[tokio::test]
async fn a_foreign_message_write_rides_the_primary_clients_script() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({"id": "AAMkmsg", "changeKey": "ck"}),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({"responses": [{"id": "0", "status": 200}]}),
        ),
    ]);
    let account = GraphAccount::new_for_tests_with_shared(
        client.clone(),
        PushMode::GraphSubscriptions,
        &["shared@contoso.com".to_string()],
    );
    let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");

    set_is_read(account, MutationTarget::Message(id), true)
        .await
        .expect("foreign write routes to the owning mailbox");

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0]
            .url
            .contains("/users/shared%40contoso.com/messages/AAMkmsg"),
        "{}",
        requests[0].url
    );
    assert!(requests[1].url.ends_with("/$batch"));
}

#[test]
fn message_batch_url_routes_foreign_id_to_owner_with_native_id() {
    let account = shared_account();
    let id = foreign_message_id("shared@contoso.com", "AAMkfolder", "AAMkmsg");
    // Per-message PATCH / DELETE (suffix "") and the move suffix both
    // route to `/users/{owner}/messages/{native}` with no `\u{1f}`.
    assert_eq!(
        message_batch_url(&account, &id, "", AccountOperation::Hydrate).expect("configured"),
        "/users/shared%40contoso.com/messages/AAMkmsg"
    );
    assert_eq!(
        message_batch_url(&account, &id, "/move", AccountOperation::Hydrate).expect("configured"),
        "/users/shared%40contoso.com/messages/AAMkmsg/move"
    );
    assert!(
        !message_batch_url(&account, &id, "", AccountOperation::Hydrate)
            .expect("configured")
            .contains('\u{1f}')
    );
}

#[test]
fn message_batch_url_keeps_primary_id_on_me() {
    let account = shared_account();
    let id = ObjectId("AAMkmsg".to_string());
    assert_eq!(
        message_batch_url(&account, &id, "", AccountOperation::Hydrate).expect("primary"),
        "/me/messages/AAMkmsg"
    );
}

#[test]
fn message_batch_url_rejects_an_unconfigured_foreign_owner() {
    let account =
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions);
    let id = foreign_message_id("other@contoso.com", "AAMkfolder", "AAMkmsg");
    let error = message_batch_url(&account, &id, "", AccountOperation::Hydrate)
        .expect_err("stale foreign owner must not route through /me");
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    ));
    // This helper is the per-item URL builder for four `$batch`
    // surfaces, so the id it refused is what makes the failure
    // actionable on the lane it lands in.
    assert_eq!(
        error.scope(),
        Some(&ErrorScope::Message {
            id: (id.0.clone()).into()
        })
    );
}

#[test]
fn maps_last_verb_property_alias() {
    assert_eq!(
        graph_extended_property_id("PR_LAST_VERB_EXECUTED"),
        "Integer 0x1081"
    );
    assert_eq!(graph_extended_property_id("String 0x4001"), "String 0x4001");
}

#[test]
fn set_importance_produces_one_exclusive_patch_body() {
    // Exactly one wire op: a single `{ "importance": "high" }` body,
    // no clear-then-set pair. The `If-Match` etag is attached by
    // `patch_messages` from `MessagePatch::etag`, not the body.
    let body = graph_importance_body(Importance::High);
    assert_eq!(body, json!({ "importance": "high" }));
    assert_eq!(body.as_object().expect("object").len(), 1);
    assert_eq!(
        graph_importance_body(Importance::Low),
        json!({ "importance": "low" })
    );
    assert_eq!(
        graph_importance_body(Importance::Normal),
        json!({ "importance": "normal" })
    );
}

/// The `bifrost-types` convenience layer routes `set_starred` through
/// `set_category` with the reserved `"$flagged"` sentinel (Graph
/// advertises `StarredFlagShape::Category`). Pin the sentinel Graph
/// honors, plus the historical aliases, so a rename on either side is a
/// test failure rather than a silently-created literal category named
/// `$flagged` on every starred message.
#[test]
fn the_reserved_starred_sentinel_and_its_aliases_are_recognized() {
    assert_eq!(STARRED_CATEGORY, "$flagged");
    for token in [
        "$flagged",
        "$FLAGGED",
        "\\flagged",
        "\\Flagged",
        "flagged",
        "Flagged",
        "starred",
        "STARRED",
    ] {
        assert!(is_starred_category(token), "{token} must map to the flag");
    }
}

#[test]
fn ordinary_category_names_are_not_mistaken_for_the_starred_sentinel() {
    for token in ["Work", "flag", "$flagged-ish", "un-flagged", "$starred", ""] {
        assert!(
            !is_starred_category(token),
            "{token} must stay an ordinary category"
        );
    }
}

#[test]
fn setting_an_ordinary_category_is_a_read_modify_write_over_the_existing_array() {
    // `set_category` reads `categories`, edits, sorts, and writes the
    // whole array back; a naive `["new"]` write would drop the others.
    let existing = json!({ "categories": ["Zeta", "Alpha"] });
    let mut categories = categories_from_value(&existing);
    assert_eq!(categories, vec!["Zeta".to_string(), "Alpha".to_string()]);
    categories.push("Mid".to_string());
    categories.sort();
    assert_eq!(categories, vec!["Alpha", "Mid", "Zeta"]);

    // Absent / non-array `categories` reads as empty rather than
    // panicking, so a sparse `$select` cannot poison the write.
    assert!(categories_from_value(&json!({})).is_empty());
    assert!(categories_from_value(&json!({ "categories": "Work" })).is_empty());
    // Non-string members are skipped.
    assert_eq!(
        categories_from_value(&json!({ "categories": ["Work", 7, null] })),
        vec!["Work".to_string()]
    );
}

#[tokio::test]
async fn partial_commit_batch_keeps_success_refusal_and_missing_siblings() {
    let client = GraphClient::new("token");
    client.script_rest([ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({"responses":[
        {"id":"1", "status":403, "body":{"error":{"code":"ErrorAccessDenied","message":"denied"}}},
        {"id":"0", "status":200, "body":{"id":"new-a"}}
    ]}))]);
    let account =
        GraphAccount::new_for_tests_with_shared(client, PushMode::GraphSubscriptions, &[]);
    let ids: Vec<ObjectId> = ["a", "b", "c"]
        .into_iter()
        .map(|id| ObjectId(id.into()))
        .collect();
    let requests = ids
        .iter()
        .enumerate()
        .map(|(index, id)| BatchRequestItem {
            id: index.to_string(),
            method: "POST".into(),
            url: format!("/me/messages/{}/move", id.0),
            body: Some(json!({"destinationId":"dest"})),
            headers: None,
        })
        .collect();
    let progress = submit_write_batch_with_targets(
        &account,
        requests,
        &ids,
        crate::account::batch::AbsentMeans::Failure,
        AccountOperation::AddToContainer,
    )
    .await
    .unwrap();
    let error = batch_progress_error(
        progress,
        &ids,
        bifrost_types::StepEffect::Moved,
        bifrost_types::StepReplay::Blocking,
    )
    .unwrap_err();
    assert!(matches!(
        error.recovery(),
        RecoveryClass::NoPermission { .. }
    ));
    let record = error.partial_commit().unwrap();
    assert_eq!(record.steps().len(), 2);
    assert_eq!(
        record.steps()[0].targets().refs[1].scope,
        ErrorScope::Message {
            id: ObjectId("new-a".into())
        }
    );
    assert_eq!(record.plan().gating.len(), 1);
}

#[test]
fn partial_commit_batch_move_success_without_id_gates() {
    let leaf = bifrost_types::AccountError::unsupported(
        AccountOperation::AddToContainer,
        bifrost_types::Protocol::Graph,
    );
    let progress = BatchProgress {
        committed: vec![(0, None)],
        failed: vec![(1, bifrost_types::Application::from_leaf(&leaf), leaf)],
    };
    let error = batch_progress_error(
        progress,
        &[ObjectId("a".into()), ObjectId("b".into())],
        bifrost_types::StepEffect::Moved,
        bifrost_types::StepReplay::Blocking,
    )
    .unwrap_err();
    assert!(
        !error.partial_commit().unwrap().steps()[0]
            .targets()
            .complete
    );
    assert_eq!(error.partial_commit().unwrap().plan().gating.len(), 1);
    assert!(
        matches!(error.recovery(), RecoveryClass::Reconcile(a) if a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget,bifrost_types::ReconcileAction::ObserveOnly])
    );
}

#[test]
fn partial_commit_batch_reconciling_subresponse_is_uncertain() {
    let leaf = crate::account::graph_error::batch_response_missing(
        AccountOperation::AddToContainer,
        Some(ErrorScope::Message {
            id: ObjectId("a".into()),
        }),
        "missing answer",
        false,
    );
    let progress = BatchProgress {
        committed: vec![],
        failed: vec![(0, bifrost_types::Application::from_leaf(&leaf), leaf)],
    };
    let error = batch_progress_error(
        progress,
        &[ObjectId("a".into())],
        bifrost_types::StepEffect::Moved,
        bifrost_types::StepReplay::Blocking,
    )
    .unwrap_err();
    assert_eq!(
        error.partial_commit().unwrap().primary().application,
        bifrost_types::Application::Uncertain
    );
}

/// Typed writes on a public-folder item are refused before any Graph REST
/// request: the read-modify-write paths, the tolerated-404 property clear
/// (which would otherwise report success), and the move door.
#[tokio::test]
async fn typed_writes_refuse_public_folder_items_without_a_request() {
    let client = GraphClient::new("token");
    // An installed (empty) script makes any dispatch observable.
    client.script_rest([]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let public = encode_public_item_id(&FolderId("AAMkPF=".to_string()), "AAMkItem=");
    account
        .seed_public_folder_for_tests(
            FolderId("AAMkPF=".to_string()),
            crate::account::cursor::PublicFolderRouting {
                anchor_mailbox: "content@contoso.com".to_string(),
                public_folder_mailbox: Some("pf@contoso.com".to_string()),
            },
        )
        .await;
    let expect_unsupported = |result: Result<(), bifrost_types::AccountError>, operation| {
        let error = result.expect_err("public-folder write must be refused");
        assert_eq!(error.kind(), &AccountErrorKind::Unsupported(operation));
    };
    expect_unsupported(
        set_is_read(
            account.clone(),
            MutationTarget::Message(public.clone()),
            true,
        )
        .await,
        AccountOperation::SetIsRead,
    );
    expect_unsupported(
        set_extended_property(
            account.clone(),
            MutationTarget::Message(public.clone()),
            "String {00020386-0000-0000-C000-000000000046} Name x".into(),
            None,
        )
        .await,
        AccountOperation::SetExtendedProperty,
    );
    expect_unsupported(
        add_to_container(
            account.clone(),
            MutationTarget::Message(public.clone()),
            ContainerId("Inbox".into()),
        )
        .await,
        AccountOperation::AddToContainer,
    );
    // A thread move into a discovered public folder is refused before the
    // thread expansion GET.
    expect_unsupported(
        add_to_container(
            account.clone(),
            MutationTarget::Thread(ThreadId("conv-1".into())),
            ContainerId("AAMkPF=".into()),
        )
        .await,
        AccountOperation::AddToContainer,
    );
    assert!(client.take_rest_requests().is_empty());
}

#[tokio::test]
async fn successful_pim_patches_discard_the_pre_write_validator() {
    for status in [200, 204] {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({"id": "m1", "changeKey": "before"}),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({"responses": [{"id": "0", "status": status}]}),
            ),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let id = ObjectId("m1".into());
        set_is_read(account.clone(), MutationTarget::Message(id.clone()), true)
            .await
            .expect("the PATCH succeeded");
        assert_eq!(account.etag_index.write().await.get(&id.0), None);
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1].body.as_ref().unwrap()["requests"][0]["headers"]["If-Match"],
            "W/\"before\""
        );
    }
}
