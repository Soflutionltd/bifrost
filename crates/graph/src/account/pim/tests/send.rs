use crate::account::pim::common::graph_iso8601_utc;
use crate::account::pim::send::{
    DEFERRED_SEND_TIME_PROPERTY_ID, apply_send_as, cancel_scheduled_send,
    decode_scheduled_send_handle, deferred_send_time_body, encode_scheduled_send_handle,
    send_as_unknown_mailbox, send_message, send_raw_message, stamp_deferred_send_time,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};
use crate::etag::GraphETag;
use base64::Engine;
use bifrost_types::{AccountOperation, ObjectId, PrincipalId, SendAs};
use bytes::Bytes;
use serde_json::{Value, json};

fn mailbox_address(value: &Value) -> Option<&str> {
    value
        .get("emailAddress")
        .and_then(|e| e.get("address"))
        .and_then(Value::as_str)
}

/// Graph's raw-MIME import is the one call whose body is not JSON, so
/// it used to build its own request straight on `AccountNet` and sat
/// outside the seam entirely. It now shares the single wire funnel:
/// the base64 octets go out verbatim under `text/plain`, and the draft
/// the import returns is what gets sent.
#[tokio::test]
async fn raw_mime_import_posts_the_base64_octets_as_text_plain() {
    let client = GraphClient::new("token");
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::CREATED,
            json!({"id": "draft-1", "changeKey": "ck"}),
        ),
        ScriptedRestResponse::empty(reqwest::StatusCode::ACCEPTED),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    let raw = bytes::Bytes::from_static(b"Subject: hi\r\n\r\nbody");

    let sent = send_raw_message(account, raw.clone(), None)
        .await
        .expect("MIME import returns the draft id");
    assert_eq!(sent.0, "draft-1");

    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].content_type, "text/plain");
    assert_eq!(requests[0].body, None);
    assert_eq!(
        requests[0].raw_body.as_deref(),
        Some(
            base64::engine::general_purpose::STANDARD
                .encode(&raw)
                .as_bytes()
        )
    );
    assert!(requests[0].url.ends_with("/me/messages"));
    assert!(requests[1].url.ends_with("/me/messages/draft-1/send"));
    assert!(requests[1].content_type.is_empty());
    assert_eq!(requests[1].body, None);
    assert_eq!(requests[1].raw_body, None);
}

#[test]
fn scheduled_send_deferred_body_shape() {
    // 1970-01-01T00:00:00Z plus one day -> a stable ISO-8601 UTC value.
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(86_400);
    let body = deferred_send_time_body(at);
    let props = body
        .get("singleValueExtendedProperties")
        .and_then(serde_json::Value::as_array)
        .expect("singleValueExtendedProperties array");
    let prop = &props[0];
    assert_eq!(
        prop.get("id").and_then(serde_json::Value::as_str),
        Some("SystemTime 0x3FEF")
    );
    assert_eq!(
        prop.get("value").and_then(serde_json::Value::as_str),
        Some("1970-01-02T00:00:00Z")
    );
}

#[test]
fn scheduled_send_capability_is_true() {
    let caps =
        crate::account::capabilities::build_capabilities(&crate::account::PushConfig::None, true);
    assert!(caps.pim_methods.scheduled_send);
}

/// `send_as` is advertised from the account's routing table, so the check
/// that matters runs through `capabilities()` on a real account rather than
/// over a hand-built snapshot.
///
/// An account with no shared mailbox can only answer a `Some(send_as)`
/// request with the `send_as_unknown_mailbox` rejection, so advertising the
/// flag there hands a consumer a path that fails every time.
#[test]
fn send_as_capability_follows_configured_shared_mailboxes() {
    use bifrost_types::Account as _;

    let client = GraphClient::new("token");
    client.script_rest([]);
    let bare = GraphAccount::new_for_tests(client, crate::account::PushMode::GraphSubscriptions);
    assert!(!bare.capabilities().pim_methods.send_as);

    let client = GraphClient::new("token");
    client.script_rest([]);
    let shared = GraphAccount::new_for_tests_with_shared(
        client,
        crate::account::PushMode::GraphSubscriptions,
        &["shared@contoso.com".to_string()],
    );
    assert!(shared.capabilities().pim_methods.send_as);
}

#[test]
fn apply_send_as_as_sets_from_and_sender() {
    let mut message = json!({});
    let send_as = SendAs::As(PrincipalId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("shared@contoso.com")
    );
    assert_eq!(
        mailbox_address(message.get("sender").expect("sender")),
        Some("shared@contoso.com")
    );
}

#[test]
fn apply_send_as_on_behalf_sets_sender_to_user() {
    let mut message = json!({});
    let send_as = SendAs::OnBehalfOf(PrincipalId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("shared@contoso.com")
    );
    assert_eq!(
        mailbox_address(message.get("sender").expect("sender")),
        Some("user@contoso.com")
    );
}

#[test]
fn apply_send_as_on_behalf_omits_sender_without_user_email() {
    let mut message = json!({});
    let send_as = SendAs::OnBehalfOf(PrincipalId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, None);
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("shared@contoso.com")
    );
    assert!(message.get("sender").is_none());
}

#[test]
fn apply_send_as_on_behalf_honors_explicit_from() {
    let mut message = json!({
        "from": { "emailAddress": { "address": "author@contoso.com" } }
    });
    let send_as = SendAs::OnBehalfOf(PrincipalId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
    // Explicit author survives; only sender is stamped.
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("author@contoso.com")
    );
    assert_eq!(
        mailbox_address(message.get("sender").expect("sender")),
        Some("user@contoso.com")
    );
}

#[test]
fn apply_send_as_as_overrides_explicit_from() {
    let mut message = json!({
        "from": { "emailAddress": { "address": "author@contoso.com" } }
    });
    let send_as = SendAs::As(PrincipalId("shared@contoso.com".to_string()));
    apply_send_as(&mut message, &send_as, Some("user@contoso.com"));
    // `As` forces author == sender == mailbox.
    assert_eq!(
        mailbox_address(message.get("from").expect("from")),
        Some("shared@contoso.com")
    );
    assert_eq!(
        mailbox_address(message.get("sender").expect("sender")),
        Some("shared@contoso.com")
    );
}

#[test]
fn send_as_unknown_mailbox_is_malformed() {
    let err = send_as_unknown_mailbox(&PrincipalId("nobody@contoso.com".to_string()));
    assert!(matches!(
        err.kind(),
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert_eq!(err.operation(), Some(AccountOperation::Send));
    assert!(err.recovery().is_terminal());
    // The field pointer is the whole reason this is `InvalidArgument` and
    // not a prose `Malformed`: a consumer UI highlights the input it names,
    // and gets the same name from the only other backend that advertises
    // the capability.
    let names_the_field = err.chain().iter().any(|cause| {
        matches!(
            cause,
            bifrost_types::Cause::Request(bifrost_types::RequestCause::InvalidArgument {
                field: Some("send_as.mailbox"),
                ..
            })
        )
    });
    assert!(names_the_field, "the offending field must be named");
}

#[test]
fn scheduled_send_handle_round_trips_owning_mailbox() {
    let handle = encode_scheduled_send_handle(Some("shared@contoso.com"), "AAMkAGI2");
    let (mailbox, draft_id) = decode_scheduled_send_handle(&handle);
    assert_eq!(mailbox, Some("shared@contoso.com"));
    assert_eq!(draft_id, "AAMkAGI2");
}

#[test]
fn scheduled_send_handle_without_mailbox_is_bare_draft_id() {
    let handle = encode_scheduled_send_handle(None, "AAMkAGI2");
    assert_eq!(handle, "AAMkAGI2");
    let (mailbox, draft_id) = decode_scheduled_send_handle(&handle);
    assert_eq!(mailbox, None);
    assert_eq!(draft_id, "AAMkAGI2");
}

#[test]
fn the_deferred_send_property_is_the_pidtag_deferred_send_time_proptag() {
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000);
    let body = deferred_send_time_body(at);
    let props = body
        .get("singleValueExtendedProperties")
        .and_then(Value::as_array)
        .expect("array");
    assert_eq!(props.len(), 1);
    assert_eq!(props[0]["id"], json!(DEFERRED_SEND_TIME_PROPERTY_ID));
    // ISO-8601 UTC with a `Z`, second precision, no fraction.
    let value = props[0]["value"].as_str().expect("string value");
    assert!(value.ends_with('Z'), "{value}");
    assert!(!value.contains('.'), "{value}");
    assert_eq!(value, graph_iso8601_utc(at));
}

#[test]
fn send_as_on_behalf_of_keeps_an_explicit_author_but_stamps_the_sender() {
    let mut message = json!({ "from": { "emailAddress": { "address": "author@contoso.com" } } });
    apply_send_as(
        &mut message,
        &SendAs::OnBehalfOf(PrincipalId("shared@contoso.com".to_string())),
        Some("me@contoso.com"),
    );
    assert_eq!(
        message["from"]["emailAddress"]["address"],
        json!("author@contoso.com")
    );
    assert_eq!(
        message["sender"]["emailAddress"]["address"],
        json!("me@contoso.com")
    );
}

#[test]
fn send_as_on_behalf_of_fills_a_missing_author_with_the_mailbox() {
    let mut message = json!({});
    apply_send_as(
        &mut message,
        &SendAs::OnBehalfOf(PrincipalId("shared@contoso.com".to_string())),
        None,
    );
    assert_eq!(
        message["from"]["emailAddress"]["address"],
        json!("shared@contoso.com")
    );
    // Without a known user email `sender` is omitted so Graph fills it
    // from the authenticated context rather than being told wrongly.
    assert!(message.get("sender").is_none());
}

#[test]
fn send_as_overrides_any_consumer_supplied_author() {
    // `As` means author == sender == mailbox; a consumer-set `from`
    // must not survive, or the message would claim an identity the
    // send-as grant does not cover.
    let mut message = json!({ "from": { "emailAddress": { "address": "author@contoso.com" } } });
    apply_send_as(
        &mut message,
        &SendAs::As(PrincipalId("shared@contoso.com".to_string())),
        Some("me@contoso.com"),
    );
    assert_eq!(
        message["from"]["emailAddress"]["address"],
        json!("shared@contoso.com")
    );
    assert_eq!(
        message["sender"]["emailAddress"]["address"],
        json!("shared@contoso.com")
    );
}

#[tokio::test]
async fn deferred_send_stamping_evicts_only_the_owning_mailbox_after_success() {
    for owner in [None, Some("shared@contoso.com")] {
        for operation in [AccountOperation::Send, AccountOperation::RescheduleSend] {
            for succeeds in [true, false] {
                let client = GraphClient::new("token");
                let status = if succeeds {
                    reqwest::StatusCode::NO_CONTENT
                } else {
                    reqwest::StatusCode::FORBIDDEN
                };
                client.script_rest([ScriptedRestResponse::empty(status)]);
                let account = GraphAccount::new_for_tests_with_shared(
                    client.clone(),
                    PushMode::GraphSubscriptions,
                    &["shared@contoso.com".into()],
                );
                let primary_key = "draft".to_string();
                let shared_key = crate::account::foreign::qualify_with_owner(
                    Some("shared@contoso.com"),
                    "draft",
                );
                let (target, sibling) = if owner.is_some() {
                    (&shared_key, &primary_key)
                } else {
                    (&primary_key, &shared_key)
                };
                {
                    let mut cache = account.etag_index.write().await;
                    for key in [&primary_key, &shared_key] {
                        cache.insert(key.clone(), GraphETag::from_change_key("before"));
                    }
                }
                // Exercise the stamping seam without consulting the wall clock.
                let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000);
                let result = stamp_deferred_send_time(
                    &account,
                    owner,
                    &bifrost_types::DraftHandle("draft".into()),
                    at,
                    operation,
                )
                .await;
                assert_eq!(result.is_ok(), succeeds);
                let mut cache = account.etag_index.write().await;
                assert_eq!(
                    cache.get(target).as_deref(),
                    if succeeds { None } else { Some("W/\"before\"") },
                );
                assert_eq!(cache.get(sibling).as_deref(), Some("W/\"before\""));
                let requests = client.take_rest_requests();
                assert_eq!(requests.len(), 1);
                assert_eq!(requests[0].method, "PATCH");
                let prefix = account
                    .mailboxes
                    .client_for_owner(owner)
                    .unwrap()
                    .api_path_prefix();
                assert!(
                    requests[0]
                        .url
                        .ends_with(&format!("{prefix}/messages/draft"))
                );
                assert_eq!(
                    requests[0].body.as_ref().unwrap(),
                    &deferred_send_time_body(at)
                );
            }
        }
    }
}

#[tokio::test]
async fn cancelling_scheduled_send_evicts_only_the_owning_mailbox_after_success() {
    for owner in [None, Some("shared@contoso.com")] {
        for succeeds in [true, false] {
            let client = GraphClient::new("token");
            let status = if succeeds {
                reqwest::StatusCode::NO_CONTENT
            } else {
                reqwest::StatusCode::FORBIDDEN
            };
            client.script_rest([ScriptedRestResponse::empty(status)]);
            let account = GraphAccount::new_for_tests_with_shared(
                client.clone(),
                PushMode::GraphSubscriptions,
                &["shared@contoso.com".into()],
            );
            let key = crate::account::foreign::qualify_with_owner(owner, "draft");
            let sibling = crate::account::foreign::qualify_with_owner(
                if owner.is_some() {
                    None
                } else {
                    Some("shared@contoso.com")
                },
                "draft",
            );
            {
                let mut cache = account.etag_index.write().await;
                cache.insert(key.clone(), GraphETag::from_change_key("before"));
                cache.insert(sibling.clone(), GraphETag::from_change_key("sibling"));
            }
            let result = cancel_scheduled_send(
                account.clone(),
                ObjectId(encode_scheduled_send_handle(owner, "draft")),
            )
            .await;
            assert_eq!(result.is_ok(), succeeds);
            let mut cache = account.etag_index.write().await;
            assert_eq!(
                cache.get(&key).as_deref(),
                if succeeds { None } else { Some("W/\"before\"") },
            );
            assert_eq!(cache.get(&sibling).as_deref(), Some("W/\"sibling\""));
            let requests = client.take_rest_requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].method, "DELETE");
        }
    }
}

#[tokio::test]
async fn sending_created_drafts_evicts_the_cached_validator() {
    for raw in [true, false] {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(reqwest::StatusCode::CREATED, json!({"id": "draft"})),
            ScriptedRestResponse::empty(reqwest::StatusCode::ACCEPTED),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        account
            .etag_index
            .write()
            .await
            .insert("draft".into(), GraphETag::from_change_key("before"));
        if raw {
            send_raw_message(
                account.clone(),
                Bytes::from_static(b"Subject: test\r\n\r\nbody"),
                None,
            )
            .await
            .expect("raw send");
        } else {
            send_message(account.clone(), bifrost_types::SendRequest::default())
                .await
                .expect("structured send");
        }
        assert_eq!(account.etag_index.write().await.get("draft"), None);
        assert_eq!(client.take_rest_requests().len(), 2);
    }
}
