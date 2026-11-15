use crate::account::pim::drafts::{
    draft_discard, draft_send, draft_update, graph_attachment_from_inline, message_from_draft_patch,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};
use crate::etag::GraphETag;
use bifrost_types::{AccountOperation, AttachmentInline, DraftPatch};
use bytes::Bytes;
use serde_json::{Value, json};

/// `DraftPatch` is `#[non_exhaustive]`, so functional-update syntax is
/// unavailable outside `bifrost-types`; build through a closure so the
/// tests below read as one expression each.
fn draft_patch(fill: impl FnOnce(&mut DraftPatch)) -> DraftPatch {
    let mut patch = DraftPatch::default();
    fill(&mut patch);
    patch
}

#[test]
fn inline_attachment_is_base64_encoded() {
    let attachment = AttachmentInline {
        filename: "note.txt".to_string(),
        mime: "text/plain".to_string(),
        data: Bytes::from_static(b"hello"),
        inline: false,
        content_id: None,
    };
    let value = graph_attachment_from_inline(&attachment).expect("attachment builds");
    assert_eq!(value["contentBytes"], json!("aGVsbG8="));
    // No content_id -> no contentId key emitted.
    assert!(value.get("contentId").is_none());
}

#[test]
fn inline_attachment_emits_content_id_when_set() {
    let attachment = AttachmentInline {
        filename: "logo.png".to_string(),
        mime: "image/png".to_string(),
        data: Bytes::from_static(b"img"),
        inline: true,
        content_id: Some("<logo@x>".to_string()),
    };
    let value = graph_attachment_from_inline(&attachment).expect("attachment builds");
    // Angle brackets are stripped to the bare cid token.
    assert_eq!(value["contentId"], json!("logo@x"));
}

#[test]
fn a_draft_patch_of_one_field_touches_only_that_field() {
    // The partial-update seam: `draft_update` passes
    // `include_empty = false`, so a subject-only rename must not emit
    // `null` / `[]` for the untouched recipient and body buckets (Graph
    // reads those as clears).
    let patch = draft_patch(|patch| {
        patch.subject = Some(Some("Renamed".to_string()));
    });
    let message = message_from_draft_patch(&patch, false).expect("patch builds");
    let object = message.as_object().expect("object");
    assert_eq!(object.get("subject"), Some(&json!("Renamed")));
    for untouched in [
        "toRecipients",
        "ccRecipients",
        "bccRecipients",
        "replyTo",
        "from",
        "body",
        "attachments",
    ] {
        assert!(
            !object.contains_key(untouched),
            "{untouched} must not appear in a sparse draft patch: {message}"
        );
    }
    assert_eq!(object.len(), 1);
}

#[test]
fn an_explicitly_cleared_draft_field_still_emits_its_clear() {
    // The flip side: `Some(None)` is a deliberate clear and MUST reach
    // the wire, so the sparse rule cannot just drop every empty value.
    let patch = draft_patch(|patch| {
        patch.subject = Some(None);
        patch.from = Some(None);
        patch.cc = Some(Vec::new());
    });
    let message = message_from_draft_patch(&patch, false).expect("patch builds");
    assert_eq!(message.get("subject"), Some(&json!("")));
    assert_eq!(message.get("from"), Some(&Value::Null));
    assert_eq!(message.get("cc"), None);
    assert_eq!(message.get("ccRecipients"), Some(&json!([])));
}

#[tokio::test]
async fn direct_draft_writes_evict_only_after_success() {
    for operation in [
        AccountOperation::DraftUpdate,
        AccountOperation::DraftDiscard,
        AccountOperation::Send,
    ] {
        for succeeds in [true, false] {
            let client = GraphClient::new("token");
            let status = if succeeds {
                reqwest::StatusCode::NO_CONTENT
            } else {
                reqwest::StatusCode::FORBIDDEN
            };
            client.script_rest([ScriptedRestResponse::empty(status)]);
            let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
            {
                let mut cache = account.etag_index.write().await;
                cache.insert("draft".into(), GraphETag::from_change_key("before"));
                cache.insert("other".into(), GraphETag::from_change_key("other"));
            }
            let draft = bifrost_types::DraftHandle("draft".into());
            let result = match operation {
                AccountOperation::DraftUpdate => {
                    draft_update(
                        account.clone(),
                        draft,
                        draft_patch(|patch| {
                            patch.subject = Some(Some("Updated".into()));
                        }),
                    )
                    .await
                }
                AccountOperation::DraftDiscard => draft_discard(account.clone(), draft).await,
                AccountOperation::Send => draft_send(account.clone(), draft).await.map(|_| ()),
                _ => unreachable!("only direct draft writes are listed"),
            };
            assert_eq!(result.is_ok(), succeeds, "{operation:?}");
            let mut cache = account.etag_index.write().await;
            assert_eq!(
                cache.get("draft").as_deref(),
                if succeeds { None } else { Some("W/\"before\"") },
                "{operation:?}",
            );
            assert_eq!(cache.get("other").as_deref(), Some("W/\"other\""));
            let requests = client.take_rest_requests();
            assert_eq!(requests.len(), 1);
            let (method, suffix) = match operation {
                AccountOperation::DraftUpdate => ("PATCH", "/me/messages/draft"),
                AccountOperation::DraftDiscard => ("DELETE", "/me/messages/draft"),
                AccountOperation::Send => ("POST", "/me/messages/draft/send"),
                _ => unreachable!("only direct draft writes are listed"),
            };
            assert_eq!(requests[0].method, method);
            assert!(requests[0].url.ends_with(suffix));
        }
    }
}
