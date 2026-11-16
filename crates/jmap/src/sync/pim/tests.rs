use super::body::*;
use super::containers::*;
use super::mail_mutation::*;
use super::message::*;
use super::search::*;
use super::settings::*;
use super::submission::*;
use super::*;
use crate::core::transport::HttpTransport;
use crate::email::{BodyProperty, Email, EmailBodyPart, EmailId};
use crate::mailbox::Mailbox;
use crate::sync::state_cache;
use crate::sync::state_cache::StateMap;
use bifrost_types::{
    AccountError, AccountOperation, Application, AttachmentSource, BlobCapabilities, BlobEncoding,
    BlobHandle, BlobId, CompensationOutcome, ContainerId, ContainerKind, ContainerNamespace,
    ErrorScope, Importance, MailboxId, MembershipScope, MessageAttachment, MutationTarget,
    ObjectId, PrincipalId, SearchFilter, SearchRequest, StepEffect, StepReplay, ThreadId,
    VacationConfig,
};
use bytes::Bytes;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

fn attachment_account(max: Option<usize>) -> MailAccount<SendTransport> {
    let mut session = serde_json::to_value(search_session()).expect("session serializes");
    let core = session["capabilities"]["urn:ietf:params:jmap:core"]
        .as_object_mut()
        .expect("core object");
    if let Some(max) = max {
        core.insert("maxSizeUpload".to_string(), serde_json::json!(max));
    } else {
        core.remove("maxSizeUpload");
    }
    let client = crate::client::Client::with_transport(
        SendTransport::new(false, false, false, false),
        serde_json::from_value(session).expect("session parses"),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    crate::sync::registry::Slot::new(client, "primary")
}

#[tokio::test]
async fn attachment_upload_stops_polling_at_the_first_oversize_chunk() {
    for chunks in [vec!["four"], vec!["ab", "cd"]] {
        let prefix = futures::stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok(Bytes::from_static(chunk.as_bytes()))),
        );
        let tail =
            futures::stream::poll_fn(|_| panic!("oversize upload must not poll another chunk"));
        let error = attachment_upload(
            attachment_account(Some(3)),
            Box::pin(prefix.chain(tail)),
            "text/plain".to_string(),
        )
        .await
        .expect_err("oversize stream is refused before it is drained");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(
                bifrost_types::RequestErrorKind::PayloadTooLarge,
            ),
        );
    }
}

#[tokio::test]
async fn attachment_upload_requires_the_limit_before_polling_input() {
    let input =
        futures::stream::poll_fn(|_| panic!("invalid session must not poll the caller's input"));
    let error = attachment_upload(
        attachment_account(None),
        Box::pin(input),
        "text/plain".to_string(),
    )
    .await
    .expect_err("missing upload limit is a session contract violation");
    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::Protocol(
            bifrost_types::ProtocolErrorKind::ContractViolation,
        ),
    );
}

#[tokio::test]
async fn attachment_upload_accepts_the_exact_limit_and_empty_zero_limit() {
    for (max, chunks) in [(4, vec!["ab", "cd"]), (0, vec![])] {
        let input = futures::stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok(Bytes::from_static(chunk.as_bytes()))),
        );
        attachment_upload(
            attachment_account(Some(max)),
            Box::pin(input),
            "text/plain".to_string(),
        )
        .await
        .expect("upload within the advertised limit succeeds");
    }
}

fn mailbox_json(id: &str, parent: Option<&str>, rights: serde_json::Value) -> Mailbox {
    let mut value = serde_json::json!({
        "id": id,
        "name": "Reports",
        "myRights": rights,
        "isSubscribed": true,
    });
    if let Some(parent) = parent {
        value["parentId"] = serde_json::Value::String(parent.to_string());
    }
    serde_json::from_value(value).expect("mailbox deserializes")
}

/// Answers one `Mailbox/get` with two shared mailboxes: one that
/// reports `myRights`, one that omits it.
#[derive(Clone)]
struct RightsTransport;

impl HttpTransport for RightsTransport {
    async fn api_request(
        &self,
        _url: &str,
        body: Vec<u8>,
        _replay: bifrost_net::ReplayPolicy,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        let request: serde_json::Value =
            serde_json::from_slice(&body).expect("the client emits JSON");
        let call = &request["methodCalls"][0];
        assert_eq!(call[0], "Mailbox/get");
        let call_id = call[2].clone();
        let response = serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[
                "Mailbox/get",
                {
                    "accountId": "shared",
                    "state": "s1",
                    "list": [
                        {
                            "id": "mbx-rights",
                            "name": "Reports",
                            "isSubscribed": true,
                            "myRights": {
                                "mayReadItems": true, "mayAddItems": false,
                                "mayRemoveItems": false, "maySetSeen": true,
                                "maySetKeywords": false, "mayCreateChild": false,
                                "mayRename": false, "mayDelete": false,
                                "maySubmit": false
                            }
                        },
                        {
                            "id": "mbx-bare",
                            "name": "Silent",
                            "isSubscribed": true
                        }
                    ],
                    "notFound": []
                },
                call_id
            ]]
        });
        Ok(bytes::Bytes::from(response.to_string()))
    }

    async fn upload(
        &self,
        _url: &str,
        _body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no upload"))
    }

    async fn download(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no download"))
    }

    async fn get_session(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no session"))
    }
}

/// Answers every `*/set` call with neither a success nor an error entry
/// for any id: the omission an RFC 8620 server must not produce, but one
/// that does not prove the write failed. `created`, when set, is the
/// `created` map every answer carries instead.
#[derive(Clone, Default)]
struct OmittingSetTransport {
    created: Option<serde_json::Value>,
}

impl HttpTransport for OmittingSetTransport {
    async fn api_request(
        &self,
        _url: &str,
        body: Vec<u8>,
        _replay: bifrost_net::ReplayPolicy,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        let request: serde_json::Value =
            serde_json::from_slice(&body).expect("the client emits JSON");
        let call = &request["methodCalls"][0];
        let method = call[0].as_str().expect("method name").to_string();
        assert!(method.ends_with("/set"), "unexpected {method}");
        let mut answer =
            serde_json::json!({"accountId": "primary", "oldState": "s1", "newState": "s2"});
        answer["accountId"] = call[1]["accountId"].clone();
        if call[1]["accountId"] == "shared" {
            assert_eq!(call[1]["create"]["c0"]["parentId"], "parent");
        }
        if let Some(created) = &self.created {
            answer["created"] = created.clone();
        }
        let response = serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[method, answer, call[2].clone()]]
        });
        Ok(bytes::Bytes::from(response.to_string()))
    }

    async fn upload(
        &self,
        _url: &str,
        _body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no upload"))
    }

    async fn download(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no download"))
    }

    async fn get_session(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no session"))
    }
}

#[tokio::test]
async fn settings_changes_require_an_answer_for_the_requested_id() {
    for identity in [true, false] {
        let client = crate::client::Client::with_transport(
            OmittingSetTransport::default(),
            search_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let result = if identity {
            let account = SubmissionAccount::new(client, "primary");
            let mut patch = bifrost_types::IdentityPatch::default();
            patch.name = Some("Renamed".into());
            identity_update(
                Some(account),
                bifrost_types::IdentityId("identity".into()),
                patch,
            )
            .await
        } else {
            let account = VacationAccount::new(client, "primary");
            vacation_set(
                Some(account),
                VacationConfig {
                    is_enabled: true,
                    subject: None,
                    body_text: None,
                    body_html: None,
                    starts_at: None,
                    ends_at: None,
                },
            )
            .await
        };
        let error = result.expect_err("omission cannot confirm saved settings");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::PartialResponse
            )
        ));
    }
}

#[test]
fn container_wire_ids_strip_only_the_selected_owner() {
    let shared = ContainerId(super::super::foreign::encode_foreign("shared", "mbx").0);
    let registry = crate::sync::registry::test_registry(
        "primary",
        &[bifrost_types::CursorScope::Folder(
            super::super::foreign::encode_foreign_account("shared"),
        )],
    );
    assert_eq!(
        container_wire_id(&shared, registry.shared_slot("shared").unwrap()).as_str(),
        "mbx"
    );
    assert_eq!(
        container_wire_id(&shared, &registry.primary).as_str(),
        shared.0
    );
    assert_eq!(
        container_wire_id(&ContainerId("mbx".into()), &registry.primary).as_str(),
        "mbx"
    );
}

#[tokio::test]
async fn container_changes_require_an_answer_for_the_requested_id() {
    for rename in [true, false] {
        let client = crate::client::Client::with_transport(
            OmittingSetTransport::default(),
            search_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mail = crate::sync::registry::Slot::new(client, "primary");
        let result = if rename {
            container_rename(mail, ContainerId("mbx".into()), "Renamed".to_string(), None).await
        } else {
            container_move(mail, ContainerId("mbx".into()), None).await
        };
        let error = result.expect_err("newState alone does not acknowledge this id");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::PartialResponse
            )
        );
        assert_eq!(
            error.operation(),
            Some(if rename {
                AccountOperation::ContainerRename
            } else {
                AccountOperation::ContainerMove
            })
        );
    }
}

#[tokio::test]
async fn container_move_refuses_a_parent_in_another_account() {
    let client = crate::client::Client::with_transport(
        OmittingSetTransport::default(),
        search_session(),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let error = container_move(
        crate::sync::registry::Slot::new(client, "shared"),
        ContainerId(super::super::foreign::encode_foreign("shared", "mbx").0),
        Some(ContainerId("parent".into())),
    )
    .await
    .expect_err("different owners");
    assert!(matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
}

#[tokio::test]
async fn shared_child_create_returns_a_qualified_container() {
    let client = crate::client::Client::with_transport(
        OmittingSetTransport {
            created: Some(serde_json::json!({"c0": {"id": "child"}})),
        },
        search_session(),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let child = container_create(
        crate::sync::registry::Slot::new(client, "shared"),
        ContainerKind::Folder,
        "new".to_string(),
        Some(ContainerId(
            super::super::foreign::encode_foreign("shared", "parent").0,
        )),
        None,
    )
    .await
    .expect("created");
    assert_eq!(
        child.0,
        super::super::foreign::encode_foreign("shared", "child").0
    );
}

/// The direct single-id mail writes classify an omitted set answer as a
/// reconcilable or retryable partial response, never the terminal
/// `MissingField` a missing id once translated to.
#[tokio::test]
async fn an_omitted_mail_set_answer_is_a_partial_response() {
    let partial = bifrost_types::AccountErrorKind::Protocol(
        bifrost_types::ProtocolErrorKind::PartialResponse,
    );
    let mail = || {
        let mut session = serde_json::to_value(search_session()).unwrap();
        session["primaryAccounts"]["urn:ietf:params:jmap:submission"] =
            serde_json::json!("primary");
        let client = crate::client::Client::with_transport(
            OmittingSetTransport::default(),
            serde_json::from_value(session).unwrap(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        crate::sync::registry::Slot::new(client, "primary")
    };
    let states = send_state_map;
    let mut patch = bifrost_types::DraftPatch::default();
    patch.subject = Some(Some("s".into()));

    let update = draft_update(
        crate::sync::registry::Slot::from_test(mail(), states()),
        bifrost_types::DraftHandle("d1".into()),
        patch,
    )
    .await
    .unwrap_err();
    assert_eq!(update.kind(), &partial, "{update:?}");

    let discard = draft_discard(
        crate::sync::registry::Slot::from_test(mail(), states()),
        bifrost_types::DraftHandle("d1".into()),
    )
    .await
    .unwrap_err();
    assert_eq!(discard.kind(), &partial, "{discard:?}");
    assert!(matches!(
        discard.recovery(),
        bifrost_types::RecoveryClass::Reconcile(_)
    ));

    let delete = container_delete(mail(), ContainerId("mbx".into()))
        .await
        .unwrap_err();
    assert_eq!(delete.kind(), &partial, "{delete:?}");

    let create = container_create(mail(), ContainerKind::Folder, "new".to_string(), None, None)
        .await
        .unwrap_err();
    assert_eq!(create.kind(), &partial, "{create:?}");
    assert!(create.recovery().requires_reconciliation(), "{create:?}");

    let cancel = cancel_scheduled_send(
        (mail())
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        ObjectId("sub".into()),
    )
    .await
    .unwrap_err();
    assert_eq!(cancel.kind(), &partial, "{cancel:?}");
}

/// A mailbox create answered in the success map without a usable id is
/// refused, never handed back as `ContainerId("")`, while the set's new
/// state still advances the cache.
#[tokio::test]
async fn a_created_mailbox_without_a_usable_id_is_refused() {
    let run = |created: serde_json::Value| async move {
        let client = crate::client::Client::with_transport(
            OmittingSetTransport {
                created: Some(created),
            },
            search_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        container_create(
            crate::sync::registry::Slot::new(client, "primary"),
            ContainerKind::Folder,
            "new".to_string(),
            None,
            None,
        )
        .await
    };
    for entry in [serde_json::json!({}), serde_json::json!({"id": ""})] {
        let result = run(serde_json::json!({"c0": entry})).await;
        let error = result.unwrap_err();
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::MissingField
            ),
            "{error:?}"
        );
    }
    let result = run(serde_json::json!({"c0": {"id": "mbx-1"}})).await;
    assert_eq!(
        result.expect("a usable created id"),
        ContainerId("mbx-1".into())
    );
}

/// A share whose `Mailbox/get` answers without `myRights` projected
/// as `rights: None` - byte-identical to a protocol that has no
/// rights notion at all - so the sidebar could not tell "this share
/// is read-only" from "rights were never reported". The container
/// still lists (a share with unknown rights beats a missing share),
/// but the omission is now a `SkippedScope` naming that container.
#[tokio::test]
async fn a_share_omitting_my_rights_is_reported_as_a_degradation() {
    let client = crate::client::Client::with_transport(
        RightsTransport,
        search_session(),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let mut foreign = HashMap::new();
    foreign.insert(
        "shared".into(),
        crate::sync::registry::Slot::new(client.clone(), "shared"),
    );
    let foreign = crate::sync::registry::Registry::from_test(
        crate::sync::registry::Slot::new(client, "primary"),
        Arc::new(foreign),
    );

    let (containers, skipped) =
        fetch_foreign_containers(&foreign, &HashMap::new(), AccountOperation::ContainersList).await;

    // Both shares are still listed.
    assert_eq!(containers.len(), 2);
    let bare = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-bare").0);
    assert!(
        containers
            .iter()
            .find(|c| c.id == bare)
            .expect("the bare share is listed")
            .rights
            .is_none()
    );

    // Exactly the rights-less one is classified.
    assert_eq!(skipped.len(), 1);
    assert_eq!(
        skipped[0].scope,
        bifrost_types::ErrorScope::Mailbox {
            id: bare.0.clone().into()
        }
    );
    assert_eq!(
        *skipped[0].error.kind(),
        bifrost_types::AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::MissingField)
    );
}

/// A thread search is a query for emails projected onto their
/// threads, so an email the projection cannot place is a hit the
/// caller asked for and never receives. All three ways that happens -
/// declared `notFound`, answered in neither list, and returned
/// without the mandatory `threadId` - belong in `failed_ids`, and one
/// thread hit by two messages is still one thread.
#[test]
fn search_emails_without_a_thread_ride_failed_ids() {
    let email = |value: serde_json::Value| -> Email {
        serde_json::from_value(value).expect("email deserializes")
    };
    let (items, failed_ids) = reconcile_search_threads(
        vec![
            "M0".to_string(),
            "M1".to_string(),
            "M2".to_string(),
            "M3".to_string(),
            "M4".to_string(),
        ],
        &[EmailId::new("M0")],
        vec![
            email(serde_json::json!({"id": "M1", "threadId": "T1"})),
            // Same thread as M1: one thread, one item.
            email(serde_json::json!({"id": "M2", "threadId": "T1"})),
            // Returned, but unplaceable - `threadId` is mandatory.
            email(serde_json::json!({"id": "M3"})),
        ],
    );

    assert_eq!(items, vec![ThreadId("T1".to_string())]);
    let mut failed_ids = failed_ids;
    failed_ids.sort();
    assert_eq!(
        failed_ids,
        vec!["M0".to_string(), "M3".to_string(), "M4".to_string()]
    );
}

/// A JMAP boundary that expands one thread to a growing message set:
/// the first `Thread/get` answers one message, every later one answers
/// two. A `move_thread` that resolved the thread per leg would add
/// only M1 to the target and then remove BOTH M1 and M2 from the
/// source, stranding M2 with no membership at all.
struct GrowingThreadTransport {
    thread_gets: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    sets: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

impl crate::core::transport::HttpTransport for GrowingThreadTransport {
    async fn api_request(
        &self,
        _url: &str,
        body: Vec<u8>,
        _replay: bifrost_net::ReplayPolicy,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
        let call = request["methodCalls"][0].clone();
        let name = call[0].as_str().expect("method name").to_string();
        let call_id = call[2].as_str().expect("call id").to_string();
        let arguments = match name.as_str() {
            "Thread/get" => {
                let seen = self
                    .thread_gets
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let email_ids = if seen == 0 {
                    serde_json::json!(["M1"])
                } else {
                    serde_json::json!(["M1", "M2"])
                };
                serde_json::json!({
                    "accountId": "primary",
                    "state": "t1",
                    "list": [{"id": "T1", "emailIds": email_ids}],
                    "notFound": []
                })
            }
            // The email-state probe the mutation door makes before its
            // conditional `Email/set`.
            "Email/get" => serde_json::json!({
                "accountId": "primary",
                "state": "s1",
                "list": [],
                "notFound": []
            }),
            "Email/set" => {
                self.sets.lock().expect("sets").push(call[1].clone());
                let updated: serde_json::Map<String, serde_json::Value> = call[1]["update"]
                    .as_object()
                    .expect("update map")
                    .keys()
                    .map(|id| (id.clone(), serde_json::Value::Null))
                    .collect();
                serde_json::json!({
                    "accountId": "primary",
                    "oldState": "s1",
                    "newState": "s2",
                    "updated": updated
                })
            }
            other => panic!("unexpected method {other}"),
        };
        let response = serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[name, arguments, call_id]]
        });
        Ok(bytes::Bytes::from(response.to_string()))
    }

    async fn upload(
        &self,
        _url: &str,
        _body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no upload"))
    }

    async fn download(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no download"))
    }

    async fn get_session(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no session"))
    }
}

/// The two legs of a thread move must run over ONE resolved id set.
/// Re-resolving between them let a message that arrived in the middle
/// be removed from the source without ever being added to the target.
#[tokio::test]
async fn a_thread_move_resolves_the_thread_once_for_both_legs() {
    let thread_gets = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sets = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let session: crate::core::session::Session = serde_json::from_value(serde_json::json!({
        "capabilities": {
            "urn:ietf:params:jmap:core": {
                "maxSizeUpload": 1000,
                "maxConcurrentUpload": 2,
                "maxSizeRequest": 100_000,
                "maxConcurrentRequests": 4,
                "maxCallsInRequest": 8,
                "maxObjectsInGet": 256,
                "maxObjectsInSet": 256,
                "collationAlgorithms": []
            },
            "urn:ietf:params:jmap:mail": {}
        },
        "accounts": {
            "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
        },
        "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
        "username": "user@example.test",
        "apiUrl": "https://example.test/jmap/api",
        "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
        "uploadUrl": "https://example.test/upload/{accountId}",
        "eventSourceUrl": "https://example.test/eventsource",
        "state": "session-1"
    }))
    .expect("session parses");
    let client = crate::client::Client::with_transport(
        GrowingThreadTransport {
            thread_gets: std::sync::Arc::clone(&thread_gets),
            sets: std::sync::Arc::clone(&sets),
        },
        session,
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let mail = crate::sync::registry::Slot::new(client, "primary");
    let states: StateMap = crate::sync::state_cache::seeded(std::collections::HashMap::new());

    move_thread(
        crate::sync::registry::Slot::from_test(mail.clone(), Arc::clone(&states)),
        ThreadId("T1".to_string()),
        ContainerId("archive".to_string()),
        Some(ContainerId("inbox".to_string())),
    )
    .await
    .expect("move succeeds");

    assert_eq!(
        thread_gets.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the thread must be resolved once for both legs"
    );
    let sets = sets.lock().expect("sets");
    assert_eq!(sets.len(), 2, "two legs, one Email/set each");
    let ids_of = |call: &serde_json::Value| {
        let mut ids: Vec<String> = call["update"]
            .as_object()
            .expect("update map")
            .keys()
            .cloned()
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(
        ids_of(&sets[0]),
        ids_of(&sets[1]),
        "both legs must address the same message set"
    );
}

/// The search page cursor is a bare integer position, so the query it
/// pages MUST carry an explicit comparator: RFC 8621 gives an unsorted
/// `Email/query` a server-defined order with no stability guarantee, and
/// paging an unstable order duplicates and skips results.
#[test]
fn a_search_query_pins_its_sort_order() {
    let mut request = SearchRequest::default();
    request.limit = Some(10);
    let query = build_search_query(request, true, 20, 10).expect("filter is supported");
    let value = serde_json::to_value(&query).expect("query serializes");

    assert_eq!(
        value.get("sort"),
        Some(&serde_json::json!([
            {"isAscending": false, "property": "receivedAt"}
        ]))
    );
    assert_eq!(value.get("position"), Some(&serde_json::json!(20)));
    assert_eq!(value.get("limit"), Some(&serde_json::json!(10)));
    assert_eq!(value.get("collapseThreads"), Some(&serde_json::json!(true)));
}

/// Scripted `Email/query` + `Email/get` server for the search routing
/// tests. Records every request so the account the query addressed and
/// the ids it carried can be asserted.
#[derive(Clone)]
struct SearchTransport {
    requests: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

impl SearchTransport {
    fn new() -> Self {
        Self {
            requests: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn calls(&self, method: &str) -> Vec<serde_json::Value> {
        self.requests
            .lock()
            .expect("requests")
            .iter()
            .filter(|call| call[0] == serde_json::json!(method))
            .map(|call| call[1].clone())
            .collect()
    }
}

impl crate::core::transport::HttpTransport for SearchTransport {
    async fn api_request(
        &self,
        _url: &str,
        body: Vec<u8>,
        _replay: bifrost_net::ReplayPolicy,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
        let call = request["methodCalls"][0].clone();
        self.requests.lock().expect("requests").push(call.clone());
        let name = call[0].as_str().expect("method name").to_string();
        let call_id = call[2].as_str().expect("call id").to_string();
        let account_id = call[1]["accountId"].clone();
        let arguments = match name.as_str() {
            "Email/query" => serde_json::json!({
                "accountId": account_id,
                "queryState": "q-1",
                "canCalculateChanges": false,
                "position": call[1]["position"].clone(),
                // Three hits, one served per page: a page is short of
                // `total`, so the walk continues on `total`, not on the
                // page having come back full.
                "total": 3,
                "ids": ["M1"]
            }),
            "Email/get" => serde_json::json!({
                "accountId": account_id,
                "state": "s-1",
                "list": [{"id": "M1", "threadId": "T1"}],
                "notFound": []
            }),
            other => panic!("unexpected method {other}"),
        };
        Ok(bytes::Bytes::from(
            serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[name, arguments, call_id]]
            })
            .to_string(),
        ))
    }

    async fn upload(
        &self,
        _url: &str,
        _body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no upload"))
    }

    async fn download(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no download"))
    }

    async fn get_session(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no session"))
    }
}

fn search_session() -> crate::core::session::Session {
    serde_json::from_value(serde_json::json!({
        "capabilities": {
            "urn:ietf:params:jmap:core": {
                "maxSizeUpload": 1000,
                "maxConcurrentUpload": 2,
                "maxSizeRequest": 100_000,
                "maxConcurrentRequests": 4,
                "maxCallsInRequest": 8,
                "maxObjectsInGet": 256,
                "maxObjectsInSet": 256,
                "collationAlgorithms": []
            },
            "urn:ietf:params:jmap:mail": {}
        },
        "accounts": {
            "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
            "shared": {"name": "Shared", "isPersonal": false, "isReadOnly": false,
                "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
        },
        "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
        "username": "user@example.test",
        "apiUrl": "https://example.test/jmap/api",
        "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
        "uploadUrl": "https://example.test/upload/{accountId}",
        "eventSourceUrl": "https://example.test/eventsource",
        "state": "session-1"
    }))
    .expect("session parses")
}

/// Primary handle, foreign routing table, and the scripted transport
/// backing both.
type SearchHandles = (
    MailAccount<SearchTransport>,
    Arc<HashMap<crate::core::id::AccountId, MailAccount<SearchTransport>>>,
    SearchTransport,
);

/// Primary handle plus one registered share (`shared`), over one
/// scripted transport.
fn search_handles() -> SearchHandles {
    let transport = SearchTransport::new();
    let client = crate::client::Client::with_transport(
        transport.clone(),
        search_session(),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let primary = crate::sync::registry::Slot::new(client.clone(), "primary");
    let mut foreign = HashMap::new();
    foreign.insert(
        "shared".into(),
        crate::sync::registry::Slot::new(client, "shared"),
    );
    (primary, Arc::new(foreign), transport)
}

fn in_container(id: ContainerId) -> SearchRequest {
    let mut request = SearchRequest::default();
    request.filter = Some(SearchFilter::In(id));
    request.limit = Some(1);
    request
}

/// The defect: a `SearchFilter::In` naming a shared mailbox sent the
/// owner-QUALIFIED id as `inMailbox` to the PRIMARY account, which
/// matches nothing, so the consumer got an empty page and no error.
/// The query must address the owner with the native mailbox id, and
/// the hits must come back in the foreign object namespace.
#[tokio::test]
async fn a_foreign_container_search_routes_to_its_owner_account() {
    let (primary, foreign, transport) = search_handles();
    let container = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-1").0);

    let page = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        in_container(container),
    )
    .await
    .expect("search succeeds");

    let queries = transport.calls("Email/query");
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0]["accountId"], serde_json::json!("shared"));
    assert_eq!(
        queries[0]["filter"]["inMailbox"],
        serde_json::json!("mbx-1"),
        "the owner qualifier must be stripped from the wire filter"
    );
    assert_eq!(
        page.items,
        vec![ObjectId(super::super::foreign::encode_object(
            "shared", "M1"
        ))]
    );
}

/// The thread door projects query hits through `Email/get`. Both legs
/// must run against the owner, and the thread ids must be qualified:
/// a bare foreign thread id asserts primary ownership, and every
/// thread-keyed door would then act on an unrelated primary thread.
#[tokio::test]
async fn a_foreign_thread_search_qualifies_its_thread_ids() {
    let (primary, foreign, transport) = search_handles();
    let container = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-1").0);

    let page = search(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        in_container(container),
    )
    .await
    .expect("search succeeds");

    let gets = transport.calls("Email/get");
    assert_eq!(gets.len(), 1);
    assert_eq!(gets[0]["accountId"], serde_json::json!("shared"));
    assert_eq!(
        gets[0]["ids"],
        serde_json::json!(["M1"]),
        "the follow-up get sends native ids to the owner"
    );
    assert_eq!(
        page.items,
        vec![ThreadId(super::super::foreign::encode_object(
            "shared", "T1"
        ))]
    );
}

/// An `In` naming a share this session cannot reach must fail
/// honestly, before the wire. Falling back to the primary account is
/// the silent-empty-result defect in a different costume.
#[tokio::test]
async fn an_unregistered_foreign_container_search_is_refused() {
    let (primary, foreign, transport) = search_handles();
    let container = ContainerId(super::super::foreign::encode_foreign("ghost", "mbx-1").0);

    let error = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        in_container(container),
    )
    .await
    .expect_err("an unreachable share is an error");

    assert!(matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert!(
        transport.calls("Email/query").is_empty(),
        "nothing may reach the wire"
    );
}

/// One `Email/query` carries one `accountId`. Two `In` filters naming
/// different owners is an error, not an implicit union and not a
/// silent pick of one of them.
#[tokio::test]
async fn a_mixed_owner_search_filter_is_refused() {
    let (primary, foreign, transport) = search_handles();
    let mut request = SearchRequest::default();
    request.filter = Some(SearchFilter::And(vec![
        SearchFilter::In(ContainerId(
            super::super::foreign::encode_foreign("shared", "mbx-1").0,
        )),
        SearchFilter::In(ContainerId("mbx-1".to_string())),
    ]));

    let error = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        request,
    )
    .await
    .expect_err("a cross-account filter is an error");

    assert!(matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert!(transport.calls("Email/query").is_empty());
}

/// A search page cursor is a bare position into ONE account's result
/// order. It must name that account so a page 2 cannot be paged
/// against another one - which would duplicate and skip silently.
#[tokio::test]
async fn a_foreign_search_cursor_resumes_on_the_same_account() {
    let (primary, foreign, transport) = search_handles();
    let container = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-1").0);

    let page = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(
            primary.clone(),
            Arc::clone(&foreign),
        )),
        in_container(container),
    )
    .await
    .expect("search succeeds");
    let cursor = page.next_cursor.expect("a full page has a next cursor");
    assert_eq!(
        String::from_utf8(cursor.clone().into_bytes()).expect("utf8"),
        super::super::foreign::encode_object("shared", "search-3:1:q-1"),
        "the cursor names the account it was minted against, \
             the position it resumes at, and the queryState it belongs to"
    );

    // Page 2 with the same filter resumes on the same account at the
    // decoded position.
    let container = ContainerId(super::super::foreign::encode_foreign("shared", "mbx-1").0);
    let mut resume = in_container(container);
    resume.page_cursor = Some(cursor.clone());
    search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(
            primary.clone(),
            Arc::clone(&foreign),
        )),
        resume,
    )
    .await
    .expect("resume succeeds");
    let queries = transport.calls("Email/query");
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[1]["accountId"], serde_json::json!("shared"));
    assert_eq!(queries[1]["position"], serde_json::json!(1));

    // The same cursor replayed with a primary-routing request is
    // refused rather than paging the primary account by the share's
    // offsets.
    let mut crossed = SearchRequest::default();
    crossed.limit = Some(1);
    crossed.page_cursor = Some(cursor);
    let error = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        crossed,
    )
    .await
    .expect_err("a cursor may not cross accounts");
    assert!(matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert_eq!(
        transport.calls("Email/query").len(),
        2,
        "the refused resume never reached the wire"
    );
}

/// The primary path is unchanged: no `In`, or an `In` naming a primary
/// container, addresses the primary account with a bare mailbox id,
/// bare result ids, and a bare cursor.
#[tokio::test]
async fn a_primary_container_search_stays_primary_and_bare() {
    let (primary, foreign, transport) = search_handles();

    let page = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        in_container(ContainerId("mbx-1".to_string())),
    )
    .await
    .expect("search succeeds");

    let queries = transport.calls("Email/query");
    assert_eq!(queries[0]["accountId"], serde_json::json!("primary"));
    assert_eq!(
        queries[0]["filter"]["inMailbox"],
        serde_json::json!("mbx-1")
    );
    assert_eq!(page.items, vec![ObjectId("M1".to_string())]);
    assert_eq!(
        page.next_cursor
            .map(|c| String::from_utf8(c.into_bytes()).expect("utf8")),
        Some("search-3:1:q-1".to_string())
    );
}

/// A scripted `Email/query` server whose answers are given per call:
/// `(ids, total, queryState)`. Deliberately independent of
/// `SearchTransport`, whose single canned answer cannot express a
/// short page, an absent `total`, or a moving result set.
#[derive(Clone)]
struct PagingTransport {
    pages: std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<serde_json::Value>>>,
    positions: std::sync::Arc<std::sync::Mutex<Vec<i64>>>,
    /// When set, every response echoes this `position` instead of the
    /// one the request asked for - the "server ignores `position`" shape.
    forced_position: Option<i64>,
}

impl PagingTransport {
    fn new(pages: Vec<(Vec<&'static str>, Option<usize>, &'static str)>) -> Self {
        let pages = pages
            .into_iter()
            .map(|(ids, total, query_state)| {
                serde_json::json!({
                    "ids": ids,
                    "total": total,
                    "queryState": query_state,
                })
            })
            .collect();
        Self {
            pages: std::sync::Arc::new(std::sync::Mutex::new(pages)),
            positions: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            forced_position: None,
        }
    }

    fn echoing(mut self, position: i64) -> Self {
        self.forced_position = Some(position);
        self
    }

    fn positions(&self) -> Vec<i64> {
        self.positions.lock().expect("positions").clone()
    }
}

impl crate::core::transport::HttpTransport for PagingTransport {
    async fn api_request(
        &self,
        _url: &str,
        body: Vec<u8>,
        _replay: bifrost_net::ReplayPolicy,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
        let call = request["methodCalls"][0].clone();
        let name = call[0].as_str().expect("method name").to_string();
        assert_eq!(name, "Email/query", "these tests only page the query");
        let call_id = call[2].as_str().expect("call id").to_string();
        let position = call[1]["position"].as_i64().unwrap_or(0);
        self.positions.lock().expect("positions").push(position);
        let page = self
            .pages
            .lock()
            .expect("pages")
            .pop_front()
            .expect("an unscripted page was requested");
        let mut arguments = serde_json::json!({
            "accountId": call[1]["accountId"].clone(),
            "queryState": page["queryState"].clone(),
            "canCalculateChanges": false,
            "position": self.forced_position.unwrap_or(position),
            "ids": page["ids"].clone(),
        });
        if !page["total"].is_null() {
            arguments["total"] = page["total"].clone();
        }
        Ok(bytes::Bytes::from(
            serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[name, arguments, call_id]]
            })
            .to_string(),
        ))
    }

    async fn upload(
        &self,
        _url: &str,
        _body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no upload"))
    }

    async fn download(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no download"))
    }

    async fn get_session(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no session"))
    }
}

type PagingHandles = (
    MailAccount<PagingTransport>,
    Arc<HashMap<crate::core::id::AccountId, MailAccount<PagingTransport>>>,
    PagingTransport,
);

fn paging_handles(pages: Vec<(Vec<&'static str>, Option<usize>, &'static str)>) -> PagingHandles {
    handles_for(PagingTransport::new(pages))
}

fn handles_for(transport: PagingTransport) -> PagingHandles {
    let client = crate::client::Client::with_transport(
        transport.clone(),
        search_session(),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let primary = crate::sync::registry::Slot::new(client, "primary");
    (primary, Arc::new(HashMap::new()), transport)
}

fn search_with_limit(limit: u32) -> SearchRequest {
    let mut request = SearchRequest::default();
    request.limit = Some(limit);
    request
}

/// The defect: `next_cursor` was derived from "the page came back full",
/// so a page short of its `limit` ended the walk in Done-shaped silence -
/// the consumer sees a complete search missing most of its hits. `total`
/// is the authority on whether more remains.
///
/// Note the fixture is a NON-CONFORMING server, deliberately. RFC 8620
/// s5.5 does not permit an arbitrary short non-final page (`ids` runs to
/// the end of the results or to the effective limit), which is what a
/// wrong justification in three comments and the reference used to
/// claim. The rule survives the correction: fullness is still not a
/// completion test, because it would rest on a `limit` echo a server may
/// omit, and `total` costs nothing to prefer.
#[tokio::test]
async fn a_short_non_final_page_still_yields_a_next_cursor() {
    let (primary, foreign, _transport) = paging_handles(vec![(vec!["M1", "M2"], Some(9), "q-1")]);

    let page = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        search_with_limit(5),
    )
    .await
    .expect("search succeeds");

    assert_eq!(page.items.len(), 2, "the server served a short page");
    assert_eq!(page.estimated_total, Some(9));
    assert_eq!(
        page.next_cursor
            .map(|c| String::from_utf8(c.into_bytes()).expect("utf8")),
        Some("search-3:2:q-1".to_string()),
        "seven hits remain, so the walk must continue"
    );
}

/// The mirror case: a page that came back FULL but exhausted `total`
/// is final, and must not hand back a cursor that would page past the
/// end of the result set.
#[tokio::test]
async fn a_full_final_page_ends_the_walk() {
    let (primary, foreign, _transport) = paging_handles(vec![(vec!["M1", "M2"], Some(2), "q-1")]);

    let page = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        search_with_limit(2),
    )
    .await
    .expect("search succeeds");

    assert!(page.next_cursor.is_none(), "position + served == total");
}

/// With no `total` the walk continues until an EMPTY page. Page fullness
/// used to be the fallback here and it truncated: the FIRST short page
/// ended the search in Done-shaped silence - the same defect
/// `a_short_non_final_page_still_yields_a_next_cursor` pins for the
/// total-bearing case, surviving on the other branch. The honest argument
/// for continue-until-empty is that an absent `total` is not evidence of
/// completion while a conforming empty page is conclusive; it is NOT
/// "the RFC permits an arbitrary short non-final page", which is false.
///
/// The rule transfers from the anchored PIM walks to this positional
/// cursor because the successor position is the server's own echo plus
/// what it served, and `validate_search_page` refuses an echo that
/// disagrees with the request, so it advances by at least one per
/// nonempty page.
///
/// Bite check: with the fullness fallback restored, the second assertion
/// fails - page two is short, so the walk would stop with M4 unread.
#[tokio::test]
async fn a_server_that_omits_total_pages_until_an_empty_page() {
    let (primary, foreign, transport) = paging_handles(vec![
        (vec!["M1", "M2"], None, "q-1"),
        (vec!["M3"], None, "q-1"),
        (vec!["M4"], None, "q-1"),
        (vec![], None, "q-1"),
    ]);

    let mut seen: Vec<String> = Vec::new();
    let mut cursor = None;
    for _ in 0..4 {
        let mut request = search_with_limit(2);
        request.page_cursor = cursor;
        let page = search_messages(
            std::sync::Arc::new(crate::sync::registry::Registry::from_test(
                primary.clone(),
                Arc::clone(&foreign),
            )),
            request,
        )
        .await
        .expect("search succeeds");
        seen.extend(page.items.iter().map(|id| id.0.clone()));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    assert_eq!(seen, vec!["M1", "M2", "M3", "M4"]);
    assert!(cursor.is_none(), "the empty page is what ends it");
    assert_eq!(transport.positions(), vec![0, 2, 3, 4]);
}

/// The defect: page 2 was taken from whatever order the server had at
/// the time, with no check that it was the order page 1's position
/// indexed. One delivered message reorders `receivedAt desc` and the
/// window slides, silently duplicating one hit and dropping another.
/// A moved `queryState` must be refused, as a concurrency conflict the
/// caller resolves by searching again - never as terminal.
#[tokio::test]
async fn a_moved_query_state_refuses_the_next_page() {
    let (primary, foreign, _transport) = paging_handles(vec![
        (vec!["M1", "M2"], Some(9), "q-1"),
        (vec!["M2", "M3"], Some(10), "q-2"),
    ]);

    let page = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(
            primary.clone(),
            Arc::clone(&foreign),
        )),
        search_with_limit(2),
    )
    .await
    .expect("search succeeds");
    let mut resume = search_with_limit(2);
    resume.page_cursor = Some(page.next_cursor.expect("more remains"));

    let error = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        resume,
    )
    .await
    .expect_err("a reordered result set must not be paged");

    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::ConcurrencyConflict
    );
    assert!(
        !error.recovery().is_terminal(),
        "ordinary mail delivery must not permanently kill search"
    );
}

/// An unmoved `queryState` is the normal case and must page through
/// without complaint, at the position the previous page ended on.
#[tokio::test]
async fn an_unmoved_query_state_pages_on() {
    let (primary, foreign, transport) = paging_handles(vec![
        (vec!["M1", "M2"], Some(4), "q-1"),
        (vec!["M3", "M4"], Some(4), "q-1"),
    ]);

    let page = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(
            primary.clone(),
            Arc::clone(&foreign),
        )),
        search_with_limit(2),
    )
    .await
    .expect("search succeeds");
    let mut resume = search_with_limit(2);
    resume.page_cursor = Some(page.next_cursor.expect("more remains"));
    let second = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        resume,
    )
    .await
    .expect("resume succeeds");

    assert_eq!(transport.positions(), vec![0, 2]);
    assert_eq!(
        second.items,
        vec![ObjectId("M3".to_string()), ObjectId("M4".to_string())]
    );
    assert!(second.next_cursor.is_none(), "total is exhausted");
}

/// BITES, at the envelope level. All four refusals in
/// `validate_search_page`, each stated as the smallest input that
/// separates it from the old behaviour.
///
/// Bite check per row, against the code this replaced (`served_from`
/// trusted verbatim, `next_position >= total` the only test):
///
/// - `2 + 2 > 3`: `4 >= 3` ended the walk and reported it complete.
/// - echo 0 for a page requested at 2: accepted silently, and since the
///   successor is `0 + 2 = 2` the walk re-requests position 2 forever.
///
/// The advance rule is a real backstop rather than a restatement: on the
/// "echo that ignores the request" row it catches the same envelope on
/// its own (`0 + 2 <= 2`), so removing EITHER rule leaves that row
/// refused. The echo rule is the strictly stronger of the two - the
/// "moved the window" row advances to 7 and only the echo rule sees it -
/// and it runs first, which is why the advance rule has no row of its own
/// that reaches it through `search_email_ids`. It is exercised here at
/// the function, and this doc says that rather than implying a coverage
/// it does not have.
#[test]
fn a_search_page_envelope_is_validated_before_it_can_end_the_walk() {
    let op = AccountOperation::Search;
    for (case, requested, served_from, served, total) in [
        ("served past the reported total", 0, 2, 2, Some(3)),
        ("served past a zero total", 0, 0, 1, Some(0)),
        ("an echo that ignores the request", 2, 0, 2, None),
        ("an echo that moved the window", 2, 5, 2, Some(50)),
    ] {
        let error =
            validate_search_page(requested, served_from, served, total, op).expect_err(case);
        assert!(
            matches!(
                error.kind(),
                bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ContractViolation
                )
            ),
            "{case} must be a ProviderContractViolation, got {:?}",
            error.kind()
        );
    }

    // An EMPTY page is exempt from all three content rules: it carries no
    // first id for `position` to index and it terminates the walk anyway.
    // Refusing it here would turn every ordinary end-of-list into an
    // error.
    assert_eq!(
        validate_search_page(4, 4, 0, None, op).expect("an empty page is a legitimate ending"),
        4
    );
    assert_eq!(
        validate_search_page(4, 99, 0, Some(4), op).expect("so is an empty page past the end"),
        99
    );

    // The controls: conforming envelopes still produce a successor.
    assert_eq!(
        validate_search_page(0, 0, 2, Some(9), op).expect("valid first page"),
        2
    );
    assert_eq!(
        validate_search_page(2, 2, 2, Some(4), op).expect("valid continuation"),
        4
    );
    assert_eq!(
        validate_search_page(2, 2, 1, None, op).expect("valid, total-less continuation"),
        3
    );
}

/// BITES end to end, and this is the residual `reference/jmap.md` used to
/// record as undetectable: a server that omits `total` and IGNORES
/// `position` re-serves its first page forever, and the walk never
/// terminates.
///
/// It is detectable because the `queryState` is pinned. This server
/// answers `q-1` on both pages, so by its own account the result list is
/// one stable finite list - and a stable finite list cannot answer a
/// request for position 2 with the rows that live at position 0.
///
/// Bite check: with the echo rule removed the advance rule still refuses
/// this page (`0 + 2` does not pass 2); with BOTH removed, page two
/// mints `2:2:q-1` again and the caller loops on the same two ids
/// forever, which is precisely the behaviour the reference used to
/// document as unavoidable.
#[tokio::test]
async fn a_server_that_ignores_position_cannot_page_forever() {
    let (primary, foreign, transport) = handles_for(
        PagingTransport::new(vec![
            (vec!["M1", "M2"], None, "q-1"),
            (vec!["M1", "M2"], None, "q-1"),
        ])
        .echoing(0),
    );

    let page = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(
            primary.clone(),
            Arc::clone(&foreign),
        )),
        search_with_limit(2),
    )
    .await
    .expect("first page succeeds");
    let mut resume = search_with_limit(2);
    resume.page_cursor = Some(page.next_cursor.expect("more may remain"));

    let error = search_messages(
        std::sync::Arc::new(crate::sync::registry::Registry::from_test(primary, foreign)),
        resume,
    )
    .await
    .expect_err("a re-served page must not extend the walk");
    assert!(
        matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        ),
        "got {:?}",
        error.kind()
    );
    assert_eq!(
        transport.positions(),
        vec![0, 2],
        "the walk did ask for the second page"
    );
}

/// The cursor codec: version, position and an opaque `queryState`
/// round-trip, on the primary account and on a share alike. The state
/// is the payload TAIL, so a state containing the field separator
/// survives verbatim.
#[test]
fn the_search_cursor_round_trips_position_state_and_owner() {
    for owner in [None, Some("shared")] {
        for state in ["q-1", "a:b:c", ""] {
            let encoded = encode_position(42, state, owner);
            let decoded = decode_position(Some(&encoded), owner, AccountOperation::Search)
                .expect("round-trips");
            assert_eq!(decoded.position, 42);
            assert_eq!(decoded.query_state.as_deref(), Some(state));
        }
    }
}

#[test]
fn search_cursors_have_a_distinct_tag_and_refuse_the_shared_old_tag() {
    for owner in [None, Some("shared")] {
        let encoded = encode_position(42, "q:1", owner);
        assert_eq!(
            encoded,
            qualify(owner, "search-3:42:q:1".to_string()).into_bytes()
        );
        for payload in ["2:42:q:1", "2:[\"c2\",\"q1\"]"] {
            let foreign_or_primary = qualify(owner, payload.to_string()).into_bytes();
            let error = decode_position(Some(&foreign_or_primary), owner, AccountOperation::Search)
                .expect_err("a cursor with the anchored codec's tag must be refused");
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                )
            );
        }
    }
}

/// A v1 cursor (a bare integer position, no `queryState`) is REFUSED,
/// not silently upgraded: continuing it is exactly the unpinned paging
/// the version bump exists to stop.
#[test]
fn a_v1_search_cursor_is_refused_as_invalid_argument() {
    let error = decode_position(Some(b"7"), None, AccountOperation::Search)
        .expect_err("a v1 cursor must not resume");
    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    );

    let foreign = super::super::foreign::encode_object("shared", "7").into_bytes();
    let error = decode_position(Some(&foreign), Some("shared"), AccountOperation::Search)
        .expect_err("a foreign v1 cursor must not resume either");
    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    );
}

/// The owner qualification that landed with the account-mismatch fix
/// still binds: a v2 cursor minted on a share is refused against the
/// primary account, before the wire.
#[test]
fn a_v2_cursor_still_refuses_to_cross_accounts() {
    let cursor = encode_position(3, "q-1", Some("shared"));
    let error = decode_position(Some(&cursor), None, AccountOperation::Search)
        .expect_err("a cursor may not cross accounts");
    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    );
}

// A foreign-account hydration must come back in the SAME id namespace
// the foreign inventory minted, or the consumer's follow-up blob read
// and container join both address the primary account.
#[test]
fn rebase_foreign_requalifies_message_memberships_and_blob_ids() {
    let mut id = ObjectId("M1".to_string());
    let mut thread_id = ThreadId("T1".to_string());
    let mut memberships = vec![MembershipScope::Mailbox(MailboxId("inbox".to_string()))];
    let mut attachments = vec![
        MessageAttachment {
            filename: None,
            content_type: None,
            content_id: None,
            inline: false,
            size: None,
            source: AttachmentSource::Blob(BlobHandle {
                id: BlobId("B1".to_string()),
                size: None,
                content_type: None,
                digest: None,
                capabilities: BlobCapabilities {
                    supports_range: false,
                    supports_parallel: false,
                    digest_available_pre_download: false,
                    encoding: BlobEncoding::Raw8Bit,
                },
            }),
            truncated: false,
        },
        MessageAttachment {
            filename: None,
            content_type: None,
            content_id: None,
            inline: false,
            size: Some(6),
            source: AttachmentSource::Inline(Bytes::from_static(b"inline")),
            truncated: false,
        },
    ];

    qualify_foreign_message_ids(
        &mut id,
        Some(&mut thread_id),
        &mut memberships,
        &mut attachments,
        "acct-9",
    );

    // Object ids (message, blob) ride the OBJECT namespace, which
    // `open_blob` / `get_stream` decode.
    assert_eq!(id.0, super::super::foreign::encode_object("acct-9", "M1"));
    // The thread id rides the same namespace: it is what the consumer
    // hands to `thread_hydrate` and `MutationTarget::Thread`, and both
    // expand it through an accountId-scoped `Thread/get`.
    assert_eq!(
        thread_id.0,
        super::super::foreign::encode_object("acct-9", "T1")
    );
    assert_eq!(
        match &attachments[0].source {
            AttachmentSource::Blob(blob) => blob.id.0.as_str(),
            _ => panic!("blob attachment must stay blob-backed"),
        },
        super::super::foreign::encode_object("acct-9", "B1")
    );
    assert!(matches!(
        &attachments[1].source,
        AttachmentSource::Inline(bytes) if bytes == &Bytes::from_static(b"inline")
    ));
    // Foreign placement uses inventory's qualified folders and owner tag,
    // keeping the container join and shared-mailbox attribution intact.
    assert_eq!(
        memberships,
        vec![
            MembershipScope::Folder(super::super::foreign::encode_foreign("acct-9", "inbox")),
            MembershipScope::Owner(PrincipalId("acct-9".to_string())),
        ]
    );
}

#[test]
fn attachment_from_part_fills_cid_and_disposition() {
    let part: EmailBodyPart = serde_json::from_value(serde_json::json!({
        "blobId": "blob-1",
        "size": 42,
        "name": "logo.png",
        "type": "image/png",
        "cid": "logo@example.test",
        "disposition": "inline"
    }))
    .expect("attachment body part deserializes");

    let attachment = attachment_from_part(&part).expect("blob-backed attachment");
    assert_eq!(attachment.filename.as_deref(), Some("logo.png"));
    assert_eq!(attachment.content_type.as_deref(), Some("image/png"));
    assert_eq!(attachment.content_id.as_deref(), Some("logo@example.test"));
    assert!(attachment.inline);
    assert_eq!(attachment.size, Some(42));
    assert!(!attachment.truncated);
    assert!(matches!(
        attachment.source,
        AttachmentSource::Blob(BlobHandle { id, size: Some(42), content_type: Some(content_type), .. })
            if id.0 == "blob-1" && content_type == "image/png"
    ));

    let requested = body_properties();
    assert!(requested.contains(&BodyProperty::Cid));
    assert!(requested.contains(&BodyProperty::Disposition));
}

/// The routing decision behind every thread-keyed door.
///
/// A bare id is PRIMARY by construction (only foreign ids are ever
/// encoded), a qualified id names its share, and a qualified id for a
/// share that has gone away deliberately does NOT resolve to a
/// registered account - it rides the primary route with its literal
/// form so the server reports the miss, rather than being stripped
/// into a bare native id that could collide with a real primary
/// thread.
#[test]
fn a_thread_id_declares_the_account_that_must_expand_it() {
    let scope =
        bifrost_types::CursorScope::Folder(super::super::foreign::encode_foreign_account("shared"));
    let registry = crate::sync::registry::test_registry("primary", &[scope]);

    let foreign = ThreadId(super::super::foreign::encode_object("shared", "T9"));
    assert_eq!(
        registry
            .object(&foreign.0)
            .owner()
            .shared()
            .map(str::to_owned),
        Some("shared".to_string())
    );

    assert_eq!(
        registry.object("T9").owner().shared().map(str::to_owned),
        None
    );

    let departed = ThreadId(super::super::foreign::encode_object("revoked", "T9"));
    assert_eq!(
        registry
            .object(&departed.0)
            .owner()
            .shared()
            .map(str::to_owned),
        None
    );
    // ...and the primary route keeps it literal, so the wire id can
    // name no real primary thread.
    assert_eq!(registry.primary.wire(&departed.0), departed.0);
}

/// The qualification round-trip a thread-keyed door performs: qualify
/// at the projection site, decode at the door, and the wire id is the
/// native id the owning account issued.
#[test]
fn a_qualified_thread_id_round_trips_to_its_native_wire_form() {
    let qualified = super::super::foreign::encode_object("shared", "T9");
    let registry = crate::sync::registry::test_registry(
        "primary",
        &[bifrost_types::CursorScope::Folder(
            super::super::foreign::encode_foreign_account("shared"),
        )],
    );
    assert_eq!(registry.object(&qualified).wire(&qualified), "T9");
    // Byte-stable: re-qualifying the decoded native id reproduces it.
    assert_eq!(
        super::super::foreign::encode_object(
            "shared",
            registry.object(&qualified).wire(&qualified)
        ),
        qualified
    );
    // A bare (primary) thread id is untouched on the primary route.
    assert_eq!(registry.primary.wire("T9"), "T9");
}

/// A thread target answers the cross-account question the same way a
/// message target does. Before this, threads asserted primary
/// ownership unconditionally, so a foreign thread paired with a bare
/// primary container passed the guard.
#[test]
fn a_thread_target_and_its_container_must_name_the_same_account() {
    let foreign_thread = MutationTarget::Thread(ThreadId(super::super::foreign::encode_object(
        "shared", "T9",
    )));
    let foreign_container = ContainerId(super::super::foreign::encode_foreign("shared", "inbox").0);
    let primary_container = ContainerId("inbox".to_string());

    assert!(
        cross_account_container(
            &foreign_thread,
            &foreign_container,
            AccountOperation::BulkMove
        )
        .is_ok()
    );
    let err = cross_account_container(
        &foreign_thread,
        &primary_container,
        AccountOperation::BulkMove,
    )
    .expect_err("a foreign thread cannot be filed into a primary mailbox id");
    assert_eq!(
        err.kind(),
        &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    );

    // The mirror case: a primary thread and a foreign container.
    let primary_thread = MutationTarget::Thread(ThreadId("T9".to_string()));
    assert!(
        cross_account_container(
            &primary_thread,
            &primary_container,
            AccountOperation::BulkMove
        )
        .is_ok()
    );
    assert!(
        cross_account_container(
            &primary_thread,
            &foreign_container,
            AccountOperation::BulkMove
        )
        .is_err()
    );
}

// A primary (bare) id is left alone: one logical object, one wire form.
#[test]
fn primary_hydration_ids_are_not_qualified() {
    let route = super::super::hydrate::route_for_id(
        &ObjectId("M1".to_string()),
        &crate::sync::registry::test_registry("primary", &[]),
    );
    assert_eq!(route, super::super::hydrate::HydrationRoute::Primary);
    assert_eq!(super::super::foreign::native_object("M1"), "M1");
}

fn full_rights() -> serde_json::Value {
    serde_json::json!({
        "mayReadItems": true,
        "mayAddItems": true,
        "mayRemoveItems": true,
        "maySetSeen": true,
        "maySetKeywords": true,
        "mayCreateChild": true,
        "mayRename": true,
        "mayDelete": true,
        "maySubmit": true,
    })
}

#[test]
fn foreign_mailbox_projects_as_shared_namespaced_container() {
    let container = container_from_mailbox(
        mailbox_json("mbx-12", Some("mbx-1"), full_rights()),
        Some("acct-9"),
        Some("owner@example.test"),
    )
    .expect("container");

    assert_eq!(container.namespace, ContainerNamespace::Shared);
    assert_eq!(container.owner_email.as_deref(), Some("owner@example.test"));
    assert_eq!(
        container.owner,
        Some(bifrost_types::PrincipalId("acct-9".to_string()))
    );
    // `native_id` is the per-account namespaced form (what the cursor
    // scope keys on); `owner_local_id` is the bare mailbox id.
    assert_eq!(
        container.native_id(),
        super::super::foreign::encode_foreign("acct-9", "mbx-12").0
    );
    assert_eq!(container.owner_local_id.as_deref(), Some("mbx-12"));
    // The parent is re-encoded in the same namespace, so a foreign child
    // never points at a same-id primary mailbox.
    assert_eq!(
        container.parent,
        Some(ContainerId(
            super::super::foreign::encode_foreign("acct-9", "mbx-1").0
        ))
    );
    // JMAP rights still project (they already did) and JMAP folders
    // carry no content class.
    assert_eq!(
        container.rights.as_ref().and_then(|r| r.may_submit),
        Some(true)
    );
    assert!(container.content_class.is_none());
}

/// The container's `native_id` must be byte-identical to the
/// `MembershipScope::Folder` qualification the foreign inventory and
/// hydration stamp on the same mailbox - that identity is the join
/// key between a message's membership and its container. (The
/// share's SYNC scope is coarser: one account-level `Folder` scope
/// per account, pinned in `factory.rs`.)
#[test]
fn foreign_container_native_id_matches_qualified_membership() {
    let container = container_from_mailbox(
        mailbox_json("mbx-12", None, full_rights()),
        Some("acct-9"),
        None,
    )
    .expect("container");
    let mut memberships = vec![bifrost_types::MembershipScope::Mailbox(
        bifrost_types::MailboxId("mbx-12".to_string()),
    )];
    super::super::inventory::qualify_foreign_memberships(
        &mut memberships,
        &bifrost_types::PrincipalId("acct-9".to_string()),
    );
    assert!(
        memberships.contains(&bifrost_types::MembershipScope::Folder(
            bifrost_types::FolderId(container.native_id().to_string())
        )),
        "container {:?} does not join the qualified memberships {memberships:?}",
        container.native_id()
    );
}

#[test]
fn primary_mailbox_stays_personal_and_unqualified() {
    let container = container_from_mailbox(
        mailbox_json("mbx-12", Some("mbx-1"), full_rights()),
        None,
        None,
    )
    .expect("container");
    assert_eq!(container.namespace, ContainerNamespace::Personal);
    assert!(container.owner.is_none());
    assert!(container.owner_email.is_none());
    assert!(container.owner_local_id.is_none());
    assert_eq!(container.native_id(), "mbx-12");
    assert_eq!(container.parent, Some(ContainerId("mbx-1".to_string())));
}

// -- owner-email resolution (the two-level RFC 9670 gate) ------------

#[test]
fn owner_email_plan_prefers_principal_over_name() {
    // A present principal id always routes through `Principal/get`
    // first, even when the account name looks like an address.
    assert_eq!(
        owner_email_plan(Some("p-1"), "shared@example.test"),
        OwnerEmailPlan::FromPrincipal {
            principal_id: "p-1".to_string(),
            name_fallback: Some("shared@example.test".to_string()),
        }
    );
    assert_eq!(
        owner_email_plan(None, "shared@example.test"),
        OwnerEmailPlan::FromName("shared@example.test".to_string())
    );
    assert_eq!(
        owner_email_plan(None, "Shared Mailbox"),
        OwnerEmailPlan::Skip
    );
}

#[test]
fn unresolvable_principal_degrades_to_the_name_instead_of_blanking() {
    // A principal the server answers for and does not know leaves
    // nothing authoritative, so the plan carries the parsed session
    // name to degrade to. (Only `PrincipalEmail::Absent` actually
    // consumes it; a failed lookup does not.)
    let plan = owner_email_plan(Some("p-unreadable"), "shared@example.test");
    let OwnerEmailPlan::FromPrincipal { name_fallback, .. } = plan else {
        panic!("a present principal id must plan a principal lookup");
    };
    assert_eq!(name_fallback.as_deref(), Some("shared@example.test"));
}

#[test]
fn account_name_must_parse_as_an_address_not_merely_contain_an_at() {
    // RFC 8620 Account.name is a user-facing label. Sniffing for
    // '@' stored display-name forms wholesale as owner addresses.
    assert_eq!(
        account_name_as_address("shared@example.test").as_deref(),
        Some("shared@example.test")
    );
    assert_eq!(
        account_name_as_address("  shared@example.test  ").as_deref(),
        Some("shared@example.test"),
        "surrounding whitespace is trimmed, not a rejection"
    );

    for rejected in [
        "Support <support@example.com>",
        "Shared Mailbox",
        "Ada Lovelace ada@example.test",
        "@example.test",
        "ada@",
        "ada@@example.test",
        "ada@localhost",
        "ada@.example.test",
        "ada@example.",
        "\"quoted\"@example.test",
        "a@b.test, c@d.test",
        "",
        "   ",
    ] {
        assert_eq!(
            account_name_as_address(rejected),
            None,
            "{rejected:?} must not be treated as an owner address"
        );
    }
}

#[test]
fn a_display_name_account_never_becomes_a_fallback() {
    // The plan for an account whose name is a display string must
    // carry no fallback at all, so a completed-but-empty lookup
    // resolves to nothing rather than to a label.
    assert_eq!(
        owner_email_plan(Some("p-1"), "Support <support@example.com>"),
        OwnerEmailPlan::FromPrincipal {
            principal_id: "p-1".to_string(),
            name_fallback: None,
        }
    );
    assert_eq!(
        owner_email_plan(None, "Support <support@example.com>"),
        OwnerEmailPlan::Skip
    );
}

#[test]
fn only_a_completed_lookup_permits_the_name_fallback() {
    // The rule the resolver applies, stated directly: `Absent`
    // (the server answered, no address) may degrade to the parsed
    // name; `Unavailable` (no answer) must not, or a transient
    // failure overwrites real ownership with a guess.
    let fallback = || Some("shared@example.test".to_string());
    assert_eq!(
        owner_email_from_lookup(
            PrincipalEmail::Resolved("owner@example.test".into()),
            fallback()
        )
        .as_deref(),
        Some("owner@example.test"),
        "an authoritative answer always wins over the name"
    );
    assert_eq!(
        owner_email_from_lookup(PrincipalEmail::Absent, fallback()).as_deref(),
        Some("shared@example.test"),
        "a completed lookup with no address may degrade to the name"
    );
    assert_eq!(
        owner_email_from_lookup(PrincipalEmail::Unavailable, fallback()),
        None,
        "a failed lookup must not be papered over with the account name"
    );
    assert_eq!(
        owner_email_from_lookup(PrincipalEmail::Absent, None),
        None,
        "and with no parseable name there is nothing to degrade to"
    );
}

#[test]
fn unresolvable_principal_without_an_address_name_has_nothing_to_fall_back_to() {
    // A non-address name is not an email and must never be guessed
    // into one; the fallback stays empty and the account resolves to
    // no owner email at all.
    assert_eq!(
        owner_email_plan(Some("p-unreadable"), "Shared Mailbox"),
        OwnerEmailPlan::FromPrincipal {
            principal_id: "p-unreadable".to_string(),
            name_fallback: None,
        }
    );
}

fn gate_session(with_principals_capability: bool) -> crate::core::session::Session {
    let mut capabilities = serde_json::json!({
        "urn:ietf:params:jmap:core": {
            "maxSizeUpload": 50000000,
            "maxConcurrentUpload": 4,
            "maxSizeRequest": 10000000,
            "maxConcurrentRequests": 4,
            "maxCallsInRequest": 16,
            "maxObjectsInGet": 500,
            "maxObjectsInSet": 500,
            "collationAlgorithms": []
        },
    });
    if with_principals_capability {
        capabilities["urn:ietf:params:jmap:principals"] = serde_json::json!({});
    }
    serde_json::from_value(serde_json::json!({
        "capabilities": capabilities,
        "accounts": {
            "acct-owner": {
                "name": "Shared Mailbox",
                "isPersonal": false,
                "isReadOnly": false,
                "accountCapabilities": {
                    "urn:ietf:params:jmap:principals:owner": {
                        "accountIdForPrincipal": "acct-principals",
                        "principalId": "p-owner"
                    }
                }
            },
            "acct-name": {
                "name": "shared@example.test",
                "isPersonal": false,
                "isReadOnly": false,
                "accountCapabilities": {}
            },
            "acct-plain": {
                "name": "Shared Mailbox",
                "isPersonal": false,
                "isReadOnly": false,
                "accountCapabilities": {}
            }
        },
        "primaryAccounts": {},
        "username": "user@example.test",
        "apiUrl": "https://example.test/jmap/",
        "downloadUrl": "https://example.test/jmap/download/{accountId}/{blobId}/{name}",
        "uploadUrl": "https://example.test/jmap/upload/{accountId}/",
        "eventSourceUrl": "https://example.test/jmap/es/",
        "state": "s1"
    }))
    .expect("gate session deserializes")
}

#[test]
fn session_without_principals_capability_plans_nothing() {
    // Level ONE of the gate: no session principals capability means no
    // resolution at all - INCLUDING the name fallback. Resolving from
    // the account name here would populate emails where the legacy
    // behavior left them NULL.
    let session = gate_session(false);
    let acct_owner = "acct-owner".to_string();
    let acct_name = "acct-name".to_string();
    let ids = vec![&acct_owner, &acct_name];
    assert!(owner_email_plans(&session, &ids).is_empty());
}

#[test]
fn session_with_principals_capability_plans_per_account() {
    let session = gate_session(true);
    let acct_owner = "acct-owner".to_string();
    let acct_name = "acct-name".to_string();
    let acct_plain = "acct-plain".to_string();
    let acct_unknown = "acct-unknown".to_string();
    let ids = vec![&acct_owner, &acct_name, &acct_plain, &acct_unknown];
    let plans = owner_email_plans(&session, &ids);
    assert_eq!(
        plans,
        vec![
            (
                "acct-owner".to_string(),
                // "Shared Mailbox" is not address-shaped, so this
                // account has no name to degrade to.
                OwnerEmailPlan::FromPrincipal {
                    principal_id: "p-owner".to_string(),
                    name_fallback: None,
                }
            ),
            (
                "acct-name".to_string(),
                OwnerEmailPlan::FromName("shared@example.test".to_string())
            ),
            ("acct-plain".to_string(), OwnerEmailPlan::Skip),
            // `acct-unknown` is absent from the session and yields no
            // plan at all (fail-soft: absent, never an error).
        ]
    );
}

#[test]
fn importance_high_sets_keyword_others_clear() {
    // High -> set `$important`; Normal/Low -> clear it. One keyword op,
    // never an expand-into-two.
    assert!(importance_sets_important_keyword(Importance::High));
    assert!(!importance_sets_important_keyword(Importance::Normal));
    assert!(!importance_sets_important_keyword(Importance::Low));
}

#[test]
fn resolve_foreign_headers_as_overrides_consumer_from() {
    let identity = bifrost_types::Address::bare("shared@example.test");
    let (from, sender) = resolve_foreign_headers(
        &bifrost_types::SendAs::As(bifrost_types::PrincipalId("foreign".to_string())),
        &identity,
        Some(bifrost_types::Address::bare("consumer@example.test")),
        Some(bifrost_types::Address::bare("user@example.test")),
    );
    assert_eq!(from.address, "shared@example.test");
    assert!(sender.is_none());
}

#[test]
fn resolve_foreign_headers_on_behalf_of_honors_consumer_from() {
    let identity = bifrost_types::Address::bare("shared@example.test");
    let (from, sender) = resolve_foreign_headers(
        &bifrost_types::SendAs::OnBehalfOf(bifrost_types::PrincipalId("foreign".to_string())),
        &identity,
        Some(bifrost_types::Address::bare("author@example.test")),
        Some(bifrost_types::Address::bare("user@example.test")),
    );
    assert_eq!(from.address, "author@example.test");
    assert_eq!(
        sender.expect("known self address").address,
        "user@example.test"
    );
}

#[test]
fn resolve_foreign_headers_on_behalf_of_omits_unknown_sender() {
    let identity = bifrost_types::Address::bare("shared@example.test");
    let (from, sender) = resolve_foreign_headers(
        &bifrost_types::SendAs::OnBehalfOf(bifrost_types::PrincipalId("foreign".to_string())),
        &identity,
        None,
        None,
    );
    assert_eq!(from.address, "shared@example.test");
    assert!(sender.is_none());
}

fn identity_rows(json: serde_json::Value) -> Vec<crate::identity::Identity> {
    serde_json::from_value(json).expect("identity rows deserialize")
}

#[test]
fn select_concrete_identity_takes_first_concrete_email() {
    let rows = identity_rows(serde_json::json!([
        {"id": "i0", "name": "Shared", "email": "shared@example.test"},
        {"id": "i1", "email": "other@example.test"},
    ]));
    let picked = select_concrete_identity(rows, "no foreign sending identity")
        .expect("first concrete identity");
    assert_eq!(picked.id, "i0");
    assert_eq!(picked.email, "shared@example.test");
    assert_eq!(picked.name.as_deref(), Some("Shared"));
}

#[test]
fn select_concrete_identity_skips_wildcard_email() {
    let rows = identity_rows(serde_json::json!([
        {"id": "i0", "email": "*"},
        {"id": "i1", "email": "*@example.test"},
        {"id": "i2", "email": "concrete@example.test"},
    ]));
    let picked = select_concrete_identity(rows, "no foreign sending identity")
        .expect("concrete over wildcard");
    assert_eq!(picked.id, "i2");
}

#[test]
fn select_concrete_identity_empty_list_rejects_unsupported() {
    let err = select_concrete_identity(Vec::new(), "no foreign sending identity")
        .expect_err("no identity to send as");
    assert!(matches!(
        err.kind(),
        bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
    ));
}

#[test]
fn select_concrete_identity_requested_absent_rejects() {
    // A by-id get that returns nothing usable is a distinct rejection
    // detail from the empty-account case, but still `Unsupported(Send)`.
    let rows = identity_rows(serde_json::json!([{"id": "i0", "email": "*"}]));
    let err = select_concrete_identity(rows, "requested identity has no concrete email")
        .expect_err("requested identity has no concrete email");
    assert!(matches!(
        err.kind(),
        bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
    ));
}

/// In-process JMAP server for the send commit paths. It records each
/// method call and can refuse a submission, refuse an unsend, omit the
/// linked Email, or fail the post-send state probe.
#[derive(Clone)]
struct SendTransport {
    requests: Arc<std::sync::Mutex<Vec<Vec<serde_json::Value>>>>,
    submission_seen: Arc<std::sync::atomic::AtomicBool>,
    submission_reply: Option<&'static str>,
    cleanup_reply: Option<&'static str>,
    fail_submission: bool,
    fail_cancel: bool,
    fail_probe_after_submission: bool,
    missing_email: bool,
    implicit_email_state: Option<&'static str>,
    /// How the implicit Email/set (emitted only while
    /// `implicit_email_state` is set) answers for the Email: `"applied"`,
    /// `"refused"` (`notUpdated` / `notDestroyed`), `"unanswered"` (the id
    /// omitted) or `"method"` (a method-level `forbidden`).
    implicit_answer: &'static str,
}

impl SendTransport {
    fn new(
        fail_submission: bool,
        fail_cancel: bool,
        fail_probe_after_submission: bool,
        missing_email: bool,
    ) -> Self {
        Self {
            requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            submission_seen: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            submission_reply: None,
            cleanup_reply: None,
            fail_submission,
            fail_cancel,
            fail_probe_after_submission,
            missing_email,
            implicit_email_state: None,
            implicit_answer: "applied",
        }
    }

    fn calls(&self, method: &str) -> Vec<serde_json::Value> {
        self.requests
            .lock()
            .expect("recorded requests")
            .iter()
            .flat_map(|request| request.iter())
            .filter(|call| call[0] == serde_json::json!(method))
            .map(|call| call[1].clone())
            .collect()
    }

    fn method_sequence(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("recorded requests")
            .iter()
            .flat_map(|request| request.iter())
            .map(|call| call[0].as_str().expect("method name").to_string())
            .collect()
    }

    fn keyed(key: &str, value: serde_json::Value) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        object.insert(key.to_string(), value);
        serde_json::Value::Object(object)
    }
}

impl HttpTransport for SendTransport {
    async fn api_request(
        &self,
        _url: &str,
        body: Vec<u8>,
        _replay: bifrost_net::ReplayPolicy,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        let request: serde_json::Value =
            serde_json::from_slice(&body).expect("the client emits JSON");
        let calls = request["methodCalls"]
            .as_array()
            .expect("method calls array")
            .clone();
        self.requests
            .lock()
            .expect("recorded requests")
            .push(calls.clone());

        let mut responses = Vec::with_capacity(calls.len());
        for call in calls {
            let method = call[0].as_str().expect("method name");
            let args = &call[1];
            let call_id = call[2].as_str().expect("call id").to_string();
            if method == "EmailSubmission/set" {
                self.submission_seen
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            if method == "Email/get"
                && args["ids"].as_array().is_some_and(Vec::is_empty)
                && self.fail_probe_after_submission
                && self
                    .submission_seen
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err(crate::core::transport::TransportError::new(
                    "scripted post-submission probe failure",
                ));
            }

            let account_id = args["accountId"].clone();
            let result = match method {
                "Mailbox/get" => serde_json::json!({
                    "accountId": account_id,
                    "state": "mailbox-state",
                    "list": [
                        {"id": "drafts", "name": "Drafts", "role": "drafts"},
                        {"id": "sent", "name": "Sent", "role": "sent"}
                    ],
                    "notFound": []
                }),
                "EmailSubmission/get" => serde_json::json!({
                    "accountId": account_id,
                    "state": "submission-state",
                    "list": [{
                        "id": "old-submission",
                        "emailId": "email-old",
                        "envelope": {
                            "mailFrom": {"email": "from@example.test", "parameters": null},
                            "rcptTo": [{"email": "to@example.test", "parameters": null}]
                        }
                    }],
                    "notFound": []
                }),
                "Email/get" => {
                    let ids = args["ids"].as_array().expect("get ids");
                    if ids.is_empty() {
                        serde_json::json!({
                            "accountId": account_id,
                            "state": "email-probe-state",
                            "list": [],
                            "notFound": []
                        })
                    } else if self.missing_email {
                        serde_json::json!({
                            "accountId": account_id,
                            "state": "email-state",
                            "list": [],
                            "notFound": ids
                        })
                    } else {
                        serde_json::json!({
                            "accountId": account_id,
                            "state": "email-state",
                            "list": ids.iter().map(|id| serde_json::json!({"id": id})).collect::<Vec<_>>(),
                            "notFound": []
                        })
                    }
                }
                "Email/set" => {
                    if let Some(destroyed) = args["destroy"].as_array() {
                        if self.cleanup_reply == Some("drop") {
                            return Err(crate::core::transport::TransportError::new(
                                "cleanup drop",
                            ));
                        }
                        if self.cleanup_reply == Some("missing") {
                            serde_json::json!({"accountId":account_id,"oldState":"before","newState":"after"})
                        } else if self.cleanup_reply == Some("refused") {
                            serde_json::json!({"accountId":account_id,"oldState":"before","newState":"after","notDestroyed":{"email-created":{"type":"forbidden"}}})
                        } else {
                            serde_json::json!({
                                "accountId": account_id,
                                // A real server always reports the state the
                                // set started from. An unconditional destroy
                                // follows the create or import that produced
                                // the Email, so it starts from that state.
                                "oldState": if !args["ifInState"].is_null() {
                                    args["ifInState"].clone()
                                } else if !self.calls("Email/import").is_empty() {
                                    serde_json::json!("email-imported-state")
                                } else {
                                    serde_json::json!("email-created-state")
                                },
                                "newState": "email-deleted-state",
                                "destroyed": destroyed
                            })
                        }
                    } else if let Some(updated) = args["update"].as_object() {
                        let updated = updated
                            .keys()
                            .map(|id| (id.clone(), serde_json::Value::Null))
                            .collect::<serde_json::Map<_, _>>();
                        serde_json::json!({
                            "accountId": account_id,
                            "oldState": args["ifInState"],
                            "newState": "email-updated-state",
                            "updated": updated
                        })
                    } else {
                        let create_id = args["create"]
                            .as_object()
                            .expect("email create map")
                            .keys()
                            .next()
                            .expect("email create id");
                        if self.submission_reply == Some("omitted_email") {
                            serde_json::json!({
                                "accountId": account_id,
                                "oldState": "email-state-before-create",
                                "newState": "email-created-state"
                            })
                        } else {
                            serde_json::json!({
                                "accountId": account_id,
                                "oldState": "email-state-before-create",
                                "newState": "email-created-state",
                                "created": Self::keyed(
                                    create_id,
                                    if self.submission_reply == Some("missing_email_id") {
                                        serde_json::json!({})
                                    } else {
                                        serde_json::json!({"id": "email-created"})
                                    }
                                )
                            })
                        }
                    }
                }
                "Identity/get" => {
                    let identity_id = args["ids"][0].as_str().unwrap_or("identity-primary");
                    let email = if identity_id == "identity-selected" {
                        "selected@example.test"
                    } else {
                        "identity@example.test"
                    };
                    serde_json::json!({
                        "accountId": account_id,
                        "state": "identity-state",
                        "list": [{
                            "id": identity_id,
                            "name": "Sender",
                            "email": email
                        }],
                        "notFound": []
                    })
                }
                "Email/import" if self.submission_reply == Some("omitted_email") => {
                    serde_json::json!({
                        "accountId": account_id,
                        "oldState": "email-state-before-import",
                        "newState": "email-imported-state"
                    })
                }
                "Email/import" => serde_json::json!({
                    "accountId": account_id,
                    "oldState": "email-state-before-import",
                    "newState": "email-imported-state",
                    "created": {
                        "i0": if self.submission_reply == Some("missing_email_id") {
                            serde_json::json!({})
                        } else {
                            serde_json::json!({"id": "email-created"})
                        }
                    }
                }),
                "Blob/copy" => {
                    let source = args["blobIds"].as_array().expect("blob ids");
                    let copied = source
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(|id| (id.to_string(), serde_json::json!(format!("copy-{id}"))))
                        .collect::<serde_json::Map<_, _>>();
                    serde_json::json!({
                        "fromAccountId": args["fromAccountId"],
                        "accountId": account_id,
                        "copied": copied,
                        "notCopied": {}
                    })
                }
                "EmailSubmission/set" => {
                    let mut result = serde_json::json!({
                        "accountId": account_id,
                        "oldState": "submission-state-before",
                        "newState": "submission-state-after"
                    });
                    if let Some(update) = args["update"].as_object() {
                        let id = update.keys().next().expect("submission update id");
                        if self.fail_cancel {
                            result["notUpdated"] = Self::keyed(
                                id,
                                serde_json::json!({
                                    "type": "cannotUnsend",
                                    "description": "already released"
                                }),
                            );
                        } else {
                            result["updated"] = Self::keyed(id, serde_json::Value::Null);
                        }
                    }
                    if let Some(create) = args["create"].as_object() {
                        let id = create.keys().next().expect("submission create id");
                        if self.fail_submission {
                            result["notCreated"] = Self::keyed(
                                id,
                                serde_json::json!({
                                    "type": "invalidRecipients",
                                    "description": "recipient refused"
                                }),
                            );
                        } else {
                            result["created"] = Self::keyed(
                                id,
                                if self.submission_reply == Some("missing_submission_id") {
                                    serde_json::json!({})
                                } else {
                                    serde_json::json!({"id": "new-submission"})
                                },
                            );
                        }
                    }
                    result
                }
                other => panic!("unexpected method {other}"),
            };
            if method == "EmailSubmission/set" && args["create"].is_object() {
                match self.submission_reply {
                    Some("envelope") => return Ok(Bytes::from_static(b"invalid envelope")),
                    Some("partial") => {
                        responses.push(
                            serde_json::json!(["error", {"type":"serverPartialFail"}, call_id]),
                        );
                        continue;
                    }
                    Some("transport") => {
                        return Err(crate::core::transport::TransportError::new(
                            "scripted replacement transport failure",
                        ));
                    }
                    Some("refused-connection") => {
                        return Err(crate::core::transport::TransportError::from_net(
                            bifrost_net::Error::Network {
                                message: "connection refused".into(),
                                transmission_state: bifrost_types::TransmissionState::Unsent,
                                source: None,
                            },
                        ));
                    }
                    Some("missing") => continue,
                    Some("decode") => {
                        responses.push(serde_json::json!([method, {"created": false}, call_id]));
                        continue;
                    }
                    Some("empty") => {
                        responses.push(serde_json::json!([method, {
                                "accountId": account_id,
                                "newState": "submission-state-after"
                            }, call_id]));
                        continue;
                    }
                    Some("method") => {
                        responses
                            .push(serde_json::json!(["error", {"type": "forbidden"}, call_id]));
                        continue;
                    }
                    _ => {}
                }
            }
            responses.push(serde_json::json!([method, result, call_id.clone()]));
            let email_success_update = args["onSuccessUpdateEmail"].is_object();
            let email_success_destroy = args["onSuccessDestroyEmail"].is_array();
            if method == "EmailSubmission/set"
                && self.implicit_email_state.is_some()
                && !self.fail_submission
                && (email_success_update || email_success_destroy)
            {
                // The implicit Email/set answers for the submitted
                // Email's real id: the draft's for `draft_send`, the
                // created/imported Email's otherwise.
                let email_id = args["create"]
                    .as_object()
                    .and_then(|create| create.values().next())
                    .and_then(|submission| submission["emailId"].as_str())
                    .unwrap_or("email-created")
                    .to_string();
                let mut implicit = serde_json::json!({
                    "accountId": account_id.clone(),
                    "oldState": if responses.iter().any(|r| r[0] == "Email/import") {
                        "email-imported-state"
                    } else {
                        "email-created-state"
                    },
                    "newState": self.implicit_email_state,
                });
                let refusal = serde_json::json!({"type": "forbidden"});
                match (self.implicit_answer, email_success_update) {
                    ("method", _) => {
                        responses.push(serde_json::json!([
                            "error",
                            {"type": "forbidden"},
                            call_id
                        ]));
                        continue;
                    }
                    ("refused", true) => {
                        implicit["notUpdated"] = Self::keyed(&email_id, refusal);
                    }
                    ("refused", false) => {
                        implicit["notDestroyed"] = Self::keyed(&email_id, refusal);
                    }
                    ("unanswered", _) => {}
                    (_, true) => {
                        implicit["updated"] = Self::keyed(&email_id, serde_json::Value::Null);
                    }
                    (_, false) => {
                        implicit["destroyed"] = serde_json::json!([email_id]);
                    }
                }
                responses.push(serde_json::json!(["Email/set", implicit, call_id]));
            }
        }

        Ok(bytes::Bytes::from(
            serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": responses
            })
            .to_string(),
        ))
    }

    async fn upload(
        &self,
        _url: &str,
        body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Ok(bytes::Bytes::from(
            serde_json::json!({
                "accountId": "primary",
                "blobId": "blob-1",
                "type": "message/rfc822",
                "size": body.len()
            })
            .to_string(),
        ))
    }

    async fn download(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no download"))
    }

    async fn get_session(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no session"))
    }
}

fn send_account(transport: SendTransport) -> MailAccount<SendTransport> {
    let mut session = serde_json::to_value(search_session()).expect("session serializes");
    session["capabilities"]["urn:ietf:params:jmap:submission"] = serde_json::json!({});
    session["accounts"]["primary"]["accountCapabilities"]["urn:ietf:params:jmap:submission"] =
        serde_json::json!({"maxDelayedSend": 3600});
    session["primaryAccounts"]["urn:ietf:params:jmap:submission"] = serde_json::json!("primary");
    session["accounts"]["shared"]["accountCapabilities"]["urn:ietf:params:jmap:submission"] =
        serde_json::json!({"maxDelayedSend": 0});
    let client = crate::client::Client::with_transport(
        transport,
        serde_json::from_value(session).expect("submission session parses"),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    crate::sync::registry::Slot::new(client, "primary")
}

#[tokio::test]
async fn foreign_send_copies_uploaded_attachment_into_its_account() {
    let transport = SendTransport::new(false, false, false, false);
    let primary = send_account(transport.clone());
    let shared: MailAccount<_> =
        crate::sync::registry::Slot::new(primary.client().clone(), "shared");
    let handle = bifrost_types::AttachmentHandle(encode_attachment_handle(
        "primary",
        "source-blob",
        Some("image/png"),
    ));

    let body = build_body(
        &shared,
        None,
        None,
        Vec::new(),
        vec![handle],
        AccountOperation::Send,
    )
    .await
    .expect("cross-account handle is copied before Email/set");

    let copy = transport.calls("Blob/copy");
    assert_eq!(copy.len(), 1);
    assert_eq!(copy[0]["fromAccountId"], "primary");
    assert_eq!(copy[0]["accountId"], "shared");
    assert_eq!(copy[0]["blobIds"], serde_json::json!(["source-blob"]));
    assert_eq!(
        body.attachments[0]
            .blob_id()
            .map(crate::core::id::BlobId::as_str),
        Some("copy-source-blob")
    );
    assert_eq!(body.attachments[0].content_type(), Some("image/png"));
}

#[test]
fn attachment_handles_carry_their_source_account_and_reject_legacy_values() {
    assert_eq!(
        decode_attachment_handle(&encode_attachment_handle(
            "primary",
            "B1",
            Some("image/png")
        )),
        Some((
            "primary".to_string(),
            "B1".to_string(),
            Some("image/png".to_string())
        ))
    );
    assert_eq!(decode_attachment_handle("jmap:B1\nimage/png"), None);
}

fn send_state_map() -> StateMap {
    crate::sync::state_cache::seeded(std::collections::HashMap::new())
}

fn basic_send_request() -> bifrost_types::SendRequest {
    let mut request = bifrost_types::SendRequest::default();
    request.from = Some(bifrost_types::Address::bare("from@example.test"));
    request.to = vec![bifrost_types::Address::bare("to@example.test")];
    request.body_text = Some("hello".to_string());
    request
}

#[tokio::test]
async fn keyword_patch_avoids_an_account_wide_state_probe_and_precondition() {
    let transport = SendTransport::new(false, false, false, false);
    let states = send_state_map();
    state_cache::set(&states, "primary", "cached-state".into()).await;

    set_keyword(
        crate::sync::registry::Slot::from_test(
            send_account(transport.clone()),
            Arc::clone(&states),
        ),
        MutationTarget::Message(ObjectId("email-old".to_string())),
        "$seen".into(),
        true,
    )
    .await
    .expect("commutative keyword patch succeeds");

    assert_eq!(transport.method_sequence(), vec!["Email/set"]);
    let sets = transport.calls("Email/set");
    assert_eq!(sets.len(), 1);
    assert!(sets[0].get("ifInState").is_none());
    assert_eq!(sets[0]["update"]["email-old"]["keywords/$seen"], true);
    assert_eq!(
        state_cache::guard(&states, "primary").await.as_deref(),
        Some("cached-state")
    );
}

#[tokio::test]
async fn send_does_not_bless_unobserved_concurrent_email_changes() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.implicit_email_state = Some("email-after-send");
    let states = send_state_map();
    state_cache::set(&states, "primary", "unobserved-predecessor".into()).await;
    send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport), Arc::clone(&states)))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        3600,
        None,
        basic_send_request(),
    )
    .await
    .expect("submission succeeds independently of the cached guard");
    assert_eq!(
        state_cache::guard(&states, "primary").await.as_deref(),
        Some("unobserved-predecessor")
    );
}

#[tokio::test]
async fn send_advances_email_state_from_implicit_email_set_response() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.implicit_email_state = Some("email-after-send");
    let states = send_state_map();
    state_cache::set(&states, "primary", "email-state-before-create".into()).await;

    let sent = send_message(
        (crate::sync::registry::Slot::from_test(
            send_account(transport.clone()),
            Arc::clone(&states),
        ))
        .view::<crate::core::capability::Submission>()
        .unwrap(),
        3600,
        None,
        basic_send_request(),
    )
    .await
    .expect("submission succeeds");

    assert_eq!(sent.id, ObjectId("email-created".to_string()));
    assert!(matches!(
        sent.sent_copy,
        bifrost_types::SentCopy::Filed { id: Some(ref id) } if id.0 == "email-created"
    ));
    assert_eq!(
        state_cache::guard(&states, "primary").await.as_deref(),
        Some("email-after-send")
    );
    assert_eq!(
        transport.method_sequence(),
        vec!["Mailbox/get", "Email/set", "EmailSubmission/set"]
    );
}

#[tokio::test]
async fn raw_send_advances_email_state_from_implicit_email_set_response() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.implicit_email_state = Some("email-after-raw-send");
    let states = send_state_map();
    state_cache::set(&states, "primary", "email-state-before-import".into()).await;

    send_raw_message(
        (crate::sync::registry::Slot::from_test(
            send_account(transport.clone()),
            Arc::clone(&states),
        ))
        .view::<crate::core::capability::Submission>()
        .unwrap(),
        Bytes::from_static(b"raw message"),
        None,
    )
    .await
    .expect("submission succeeds");

    assert_eq!(
        state_cache::guard(&states, "primary").await.as_deref(),
        Some("email-after-raw-send")
    );
    assert_eq!(
        transport.method_sequence(),
        vec!["Mailbox/get", "Email/import", "EmailSubmission/set"]
    );
}

#[tokio::test]
async fn draft_send_uses_implicit_email_set_state_without_a_probe() {
    let mut transport = SendTransport::new(false, false, true, false);
    transport.implicit_email_state = Some("email-after-draft-send");
    let states = send_state_map();
    state_cache::set(&states, "primary", "email-created-state".into()).await;

    let sent = draft_send(
        (crate::sync::registry::Slot::from_test(
            send_account(transport.clone()),
            Arc::clone(&states),
        ))
        .view::<crate::core::capability::Submission>()
        .unwrap(),
        bifrost_types::DraftHandle("email-old".to_string()),
    )
    .await
    .expect("submission succeeds without a post-send probe");

    assert_eq!(sent.id, ObjectId("email-old".to_string()));
    assert!(matches!(
        sent.sent_copy,
        bifrost_types::SentCopy::Filed { id: Some(ref id) } if id.0 == "email-old"
    ));
    assert_eq!(
        state_cache::guard(&states, "primary").await.as_deref(),
        Some("email-after-draft-send")
    );
    assert_eq!(
        transport.method_sequence(),
        vec!["Mailbox/get", "EmailSubmission/set"]
    );
}

async fn send_with_implicit_answer(
    answer: &'static str,
    save_to_sent: Option<bool>,
) -> bifrost_types::SendReceipt {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.implicit_email_state = Some("email-after-send");
    transport.implicit_answer = answer;
    let mut request = basic_send_request();
    request.save_to_sent = save_to_sent;
    send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        3600,
        None,
        request,
    )
    .await
    .expect("a committed submission is Ok whatever the implicit Email/set says")
}

#[tokio::test]
async fn send_reports_the_implicit_sent_move_per_item() {
    use bifrost_types::{SentCopy, SentCopyNotFiled};
    assert!(matches!(
        send_with_implicit_answer("refused", None).await.sent_copy,
        SentCopy::NotFiled {
            reason: SentCopyNotFiled::MoveRefused,
            detail: Some(_)
        }
    ));
    assert!(matches!(
        send_with_implicit_answer("method", None).await.sent_copy,
        SentCopy::NotFiled {
            reason: SentCopyNotFiled::MoveRefused,
            detail: Some(_)
        }
    ));
    assert!(matches!(
        send_with_implicit_answer("unanswered", None)
            .await
            .sent_copy,
        SentCopy::Uncertain { .. }
    ));
    // No implicit response at all, though RFC 8621 s7.5 requires one.
    let mut transport = SendTransport::new(false, false, false, false);
    transport.implicit_answer = "applied";
    let receipt = send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        3600,
        None,
        basic_send_request(),
    )
    .await
    .expect("submission succeeds");
    assert!(matches!(receipt.sent_copy, SentCopy::Uncertain { .. }));
}

#[tokio::test]
async fn a_declined_sent_copy_is_not_requested_whatever_the_destroy_answer() {
    for answer in ["applied", "refused", "unanswered"] {
        let receipt = send_with_implicit_answer(answer, Some(false)).await;
        assert!(
            matches!(receipt.sent_copy, bifrost_types::SentCopy::NotRequested),
            "{answer}: {:?}",
            receipt.sent_copy
        );
        assert!(receipt.draft_discard.is_none());
    }
}

#[tokio::test]
async fn raw_and_draft_sends_report_a_refused_sent_move() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.implicit_email_state = Some("email-after-raw-send");
    transport.implicit_answer = "refused";
    let raw = send_raw_message(
        (crate::sync::registry::Slot::from_test(send_account(transport), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        Bytes::from_static(b"raw message"),
        None,
    )
    .await
    .expect("submission succeeds");
    assert!(matches!(
        raw.sent_copy,
        bifrost_types::SentCopy::NotFiled {
            reason: bifrost_types::SentCopyNotFiled::MoveRefused,
            ..
        }
    ));

    let mut transport = SendTransport::new(false, false, false, false);
    transport.implicit_email_state = Some("email-after-draft-send");
    transport.implicit_answer = "refused";
    let draft = draft_send(
        (crate::sync::registry::Slot::from_test(send_account(transport), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        bifrost_types::DraftHandle("email-old".to_string()),
    )
    .await
    .expect("submission succeeds");
    assert!(matches!(
        draft.sent_copy,
        bifrost_types::SentCopy::NotFiled {
            reason: bifrost_types::SentCopyNotFiled::MoveRefused,
            ..
        }
    ));
}

#[tokio::test]
async fn foreign_send_files_its_sent_copy_under_the_foreign_id() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.implicit_email_state = Some("email-after-send");
    let primary = send_account(transport);
    let shared: MailAccount<_> =
        crate::sync::registry::Slot::new(primary.client().clone(), "shared");
    let receipt = send_message(
        (crate::sync::registry::Slot::from_test(shared.clone(), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        0,
        Some(ForeignSubmission {
            mode: bifrost_types::SendAs::As(bifrost_types::PrincipalId("shared".into())),
            self_address: None,
        }),
        basic_send_request(),
    )
    .await
    .expect("foreign submission succeeds");
    let expected = super::super::foreign::encode_object("shared", "email-created");
    // The receipt id and the Sent copy agree on ownership: a bare native
    // id would route an id-only reader to the primary account.
    assert_eq!(receipt.id, ObjectId(expected.clone()));
    assert!(matches!(
        receipt.sent_copy,
        bifrost_types::SentCopy::Filed { id: Some(ref id) } if id.0 == expected
    ));
}

#[tokio::test]
async fn foreign_send_partial_commit_names_the_foreign_email() {
    let transport = SendTransport::new(true, false, false, false);
    let primary = send_account(transport.clone());
    let shared: MailAccount<_> =
        crate::sync::registry::Slot::new(primary.client().clone(), "shared");
    let error = send_message(
        (crate::sync::registry::Slot::from_test(shared.clone(), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        0,
        Some(ForeignSubmission {
            mode: bifrost_types::SendAs::As(bifrost_types::PrincipalId("shared".into())),
            self_address: None,
        }),
        basic_send_request(),
    )
    .await
    .expect_err("the rejected submission stays an error");
    let expected = super::super::foreign::encode_object("shared", "email-created");
    assert_eq!(
        error.partial_commit().unwrap().steps()[0].targets(),
        &email_targets(Some(&EmailId::new(expected)))
    );
    // The cleanup destroy still names the native id on the wire.
    let sets = transport.calls("Email/set");
    assert_eq!(sets[1]["destroy"], serde_json::json!(["email-created"]));
}

#[tokio::test]
async fn thread_keyword_patch_batches_at_the_advertised_set_limit() {
    let transport = DeleteThreadTransport::new();
    let mut session = serde_json::to_value(search_session()).expect("session serializes");
    session["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInSet"] = serde_json::json!(2);
    let client = crate::client::Client::with_transport(
        transport.clone(),
        serde_json::from_value(session).expect("session parses"),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let mail = crate::sync::registry::Slot::new(client, "primary");
    let states: StateMap = crate::sync::state_cache::seeded(std::collections::HashMap::new());

    set_keyword(
        crate::sync::registry::Slot::from_test(mail.clone(), Arc::clone(&states)),
        MutationTarget::Thread(ThreadId("thread-1".to_string())),
        "$seen".into(),
        true,
    )
    .await
    .expect("thread keyword patches honor maxObjectsInSet");

    let calls = transport.calls();
    assert_eq!(
        calls
            .iter()
            .map(|call| call[0].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["Thread/get", "Email/set", "Email/set", "Email/set"]
    );
    assert_eq!(calls[1][1]["update"].as_object().unwrap().len(), 2);
    assert_eq!(calls[2][1]["update"].as_object().unwrap().len(), 2);
    assert_eq!(calls[3][1]["update"].as_object().unwrap().len(), 1);
    for set in &calls[1..] {
        assert!(set[1].get("ifInState").is_none());
    }
}

#[tokio::test]
async fn draft_send_keeps_success_when_the_post_send_state_probe_fails() {
    let transport = SendTransport::new(false, false, true, false);
    let states = send_state_map();
    state_cache::set(&states, "primary", "email-before-send".into()).await;
    let result = draft_send(
        (crate::sync::registry::Slot::from_test(
            send_account(transport.clone()),
            Arc::clone(&states),
        ))
        .view::<crate::core::capability::Submission>()
        .unwrap(),
        bifrost_types::DraftHandle("email-old".to_string()),
    )
    .await;

    assert_eq!(
        result.expect("send was committed").id,
        ObjectId("email-old".to_string())
    );
    assert_eq!(
        transport.method_sequence(),
        vec!["Mailbox/get", "EmailSubmission/set", "Email/get"]
    );
    assert_eq!(
        state_cache::guard(&states, "primary").await.as_deref(),
        Some("email-before-send")
    );
    let sets = transport.calls("EmailSubmission/set");
    assert_eq!(sets.len(), 1);
    assert!(
        sets[0]["create"][SUBMISSION_CREATE_ID]
            .get("undoStatus")
            .is_none()
    );
}

#[tokio::test]
async fn ambiguous_send_retains_email_and_requires_reconciliation() {
    for reply in ["missing", "decode", "empty", "method"] {
        for raw in [false, true] {
            let mut transport = SendTransport::new(false, false, false, false);
            transport.submission_reply = Some(reply);
            let states = send_state_map();
            let error = if raw {
                send_raw_message(
                    (crate::sync::registry::Slot::from_test(
                        send_account(transport.clone()),
                        Arc::clone(&states),
                    ))
                    .view::<crate::core::capability::Submission>()
                    .unwrap(),
                    Bytes::from_static(b"raw message"),
                    None,
                )
                .await
            } else {
                send_message(
                    (crate::sync::registry::Slot::from_test(
                        send_account(transport.clone()),
                        Arc::clone(&states),
                    ))
                    .view::<crate::core::capability::Submission>()
                    .unwrap(),
                    0,
                    None,
                    basic_send_request(),
                )
                .await
            }
            .expect_err("scripted submission result must fail");
            let destroys = transport
                .calls("Email/set")
                .into_iter()
                .filter(|args| args["destroy"].is_array())
                .count();
            if reply == "method" {
                assert_eq!(destroys, 1, "confirmed method rejection permits cleanup");
            } else {
                assert_eq!(destroys, 0, "ambiguous {reply} must retain the Email");
                assert!(matches!(
                    error.recovery(),
                    bifrost_types::RecoveryClass::Reconcile(_)
                ));
                assert_eq!(
                    error.partial_commit().unwrap().primary().application,
                    Application::Uncertain
                );
                assert_eq!(
                    error.partial_commit().unwrap().steps()[0].effect(),
                    Some(StepEffect::Created)
                );
                assert!(error.partial_commit().unwrap().compensations().is_empty());
            }
        }
    }
}

#[tokio::test]
async fn replacement_failure_preserves_confirmed_cancellation() {
    // Cancellation is verified before replacement so two schedules cannot
    // remain live. Once cancellation is confirmed it cannot be rolled back;
    // failures below therefore return an acknowledged partial outcome that
    // tells the caller the old schedule was canceled and needs reconciling.
    for reply in [
        None,
        Some("missing"),
        Some("decode"),
        Some("empty"),
        Some("method"),
        Some("transport"),
        Some("missing_submission_id"),
    ] {
        let mut transport = SendTransport::new(false, false, false, false);
        transport.fail_submission = reply != Some("missing_submission_id");
        transport.submission_reply = reply;
        let error = reschedule_send(
            (send_account(transport.clone()))
                .view::<crate::core::capability::Submission>()
                .unwrap(),
            3600,
            ObjectId("old-submission".to_string()),
            SystemTime::now() + std::time::Duration::from_secs(60),
        )
        .await
        .expect_err("replacement must fail");
        let sets = transport.calls("EmailSubmission/set");
        assert_eq!(sets.len(), 2);
        assert_eq!(
            sets[0]["update"]["old-submission"]["undoStatus"],
            "canceled"
        );
        assert!(sets[1]["create"].is_object());
        assert_eq!(error.operation(), Some(AccountOperation::RescheduleSend));
        assert_eq!(
            error.partial_commit().unwrap().steps()[0].effect(),
            Some(StepEffect::Cancelled)
        );
        if reply.is_none() || reply == Some("method") {
            assert!(error.recovery().is_terminal());
        } else {
            assert!(error.recovery().requires_reconciliation());
        }
    }
}

#[tokio::test]
async fn failed_send_message_submission_best_effort_destroys_created_email() {
    let transport = SendTransport::new(true, false, false, false);
    let states = send_state_map();
    state_cache::set(&states, "primary", "email-state-before-create".into()).await;
    let result = send_message(
        (crate::sync::registry::Slot::from_test(
            send_account(transport.clone()),
            Arc::clone(&states),
        ))
        .view::<crate::core::capability::Submission>()
        .unwrap(),
        0,
        None,
        basic_send_request(),
    )
    .await;

    assert!(
        result.is_err(),
        "the rejected submission must remain an error"
    );
    let sets = transport.calls("Email/set");
    assert_eq!(sets.len(), 2, "create, followed by best-effort destroy");
    assert!(sets[0]["create"].is_object());
    assert_eq!(sets[1]["destroy"], serde_json::json!(["email-created"]));
    assert!(sets[1].get("ifInState").is_none());
    let submissions = transport.calls("EmailSubmission/set");
    assert_eq!(submissions.len(), 1);
    assert!(
        submissions[0]["create"][SUBMISSION_CREATE_ID]
            .get("undoStatus")
            .is_none()
    );
    assert_eq!(
        state_cache::guard(&states, "primary").await.as_deref(),
        Some("email-deleted-state")
    );
}

#[tokio::test]
async fn acknowledged_send_with_an_email_create_missing_its_id_is_partial() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.submission_reply = Some("missing_email_id");
    let error = send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport.clone()), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        0,
        None,
        basic_send_request(),
    )
    .await
    .expect_err("a missing created Email id cannot become an empty ObjectId");

    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::MissingField)
    );
    assert!(matches!(
        error.recovery(),
        bifrost_types::RecoveryClass::Reconcile(_)
    ));

    assert_eq!(transport.calls("EmailSubmission/set").len(), 1);
    assert_eq!(transport.calls("Email/set").len(), 1);
    assert!(transport.calls("Email/set")[0].get("destroy").is_none());
    assert!(
        matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.reason == bifrost_types::ReconcileReason::PartialCommit && a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget, bifrost_types::ReconcileAction::DedupeByClientId, bifrost_types::ReconcileAction::ObserveOnly])
    );
}

/// An Email create or import the server answers in neither `created` nor
/// `notCreated` may still have landed, so it is a partial response that
/// reconciles, never the terminal `MissingField` of a create that came
/// back without its id.
#[tokio::test]
async fn an_omitted_email_create_answer_is_a_partial_response() {
    let partial = bifrost_types::AccountErrorKind::Protocol(
        bifrost_types::ProtocolErrorKind::PartialResponse,
    );
    let omitting = || {
        let mut transport = SendTransport::new(false, false, false, false);
        transport.submission_reply = Some("omitted_email");
        transport
    };

    let draft = draft_create(
        crate::sync::registry::Slot::from_test(send_account(omitting()), send_state_map()),
        bifrost_types::DraftPatch::default(),
    )
    .await
    .expect_err("an omitted draft create answer");
    assert_eq!(draft.kind(), &partial, "{draft:?}");
    assert!(draft.recovery().requires_reconciliation(), "{draft:?}");

    let transport = omitting();
    let send = send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport.clone()), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        0,
        None,
        basic_send_request(),
    )
    .await
    .expect_err("an omitted Email create answer after an acknowledged submission");
    assert_eq!(send.kind(), &partial, "{send:?}");
    assert!(send.recovery().requires_reconciliation(), "{send:?}");
    assert_eq!(transport.calls("Email/set").len(), 1, "no cleanup destroy");

    let raw = send_raw_message(
        (crate::sync::registry::Slot::from_test(send_account(omitting()), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        Bytes::from_static(b"raw message"),
        None,
    )
    .await
    .expect_err("an omitted Email import answer after an acknowledged submission");
    assert_eq!(raw.kind(), &partial, "{raw:?}");
    assert!(raw.recovery().requires_reconciliation(), "{raw:?}");
}

#[tokio::test]
async fn failed_raw_send_submission_best_effort_destroys_imported_email() {
    let transport = SendTransport::new(true, false, false, false);
    let states = send_state_map();
    state_cache::set(&states, "primary", "email-state-before-import".into()).await;
    let result = send_raw_message(
        (crate::sync::registry::Slot::from_test(
            send_account(transport.clone()),
            Arc::clone(&states),
        ))
        .view::<crate::core::capability::Submission>()
        .unwrap(),
        Bytes::from_static(b"raw message"),
        None,
    )
    .await;

    assert!(
        result.is_err(),
        "the rejected submission must remain an error"
    );
    let imports = transport.calls("Email/import");
    assert_eq!(imports.len(), 1);
    let destroys = transport.calls("Email/set");
    assert_eq!(destroys.len(), 1, "the imported email is destroyed");
    assert_eq!(destroys[0]["destroy"], serde_json::json!(["email-created"]));
    assert!(destroys[0].get("ifInState").is_none());
    let submissions = transport.calls("EmailSubmission/set");
    assert_eq!(submissions.len(), 1);
    assert!(
        submissions[0]["create"][SUBMISSION_CREATE_ID]
            .get("undoStatus")
            .is_none()
    );
    assert_eq!(
        state_cache::guard(&states, "primary").await.as_deref(),
        Some("email-deleted-state")
    );
}

#[tokio::test]
async fn acknowledged_raw_send_with_an_import_missing_its_id_is_partial() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.submission_reply = Some("missing_email_id");
    let error = send_raw_message(
        (crate::sync::registry::Slot::from_test(send_account(transport.clone()), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        Bytes::from_static(b"raw message"),
        None,
    )
    .await
    .expect_err("a missing imported Email id cannot become an empty ObjectId");

    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::MissingField)
    );

    assert_eq!(transport.calls("EmailSubmission/set").len(), 1);
    assert_eq!(transport.calls("Email/import").len(), 1);
    assert!(transport.calls("Email/set").is_empty());
    assert!(
        matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.reason == bifrost_types::ReconcileReason::PartialCommit && a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget, bifrost_types::ReconcileAction::DedupeByClientId, bifrost_types::ReconcileAction::ObserveOnly])
    );
}

#[tokio::test]
async fn created_draft_with_missing_email_id_is_partial() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.submission_reply = Some("missing_email_id");
    let error = draft_create(
        crate::sync::registry::Slot::from_test(send_account(transport.clone()), send_state_map()),
        bifrost_types::DraftPatch::default(),
    )
    .await
    .expect_err("a missing created Email id cannot become an empty DraftHandle");

    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::MissingField)
    );

    assert!(
        matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.reason == bifrost_types::ReconcileReason::PartialCommit && a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget, bifrost_types::ReconcileAction::DedupeByClientId, bifrost_types::ReconcileAction::ObserveOnly])
    );
}

#[tokio::test]
async fn scheduled_send_omits_server_owned_undo_status() {
    let transport = SendTransport::new(false, false, false, false);
    let mut request = basic_send_request();
    request.save_to_sent = Some(false);
    request.scheduled = Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800));
    let result = send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport.clone()), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        usize::MAX,
        None,
        request,
    )
    .await;

    assert_eq!(
        result.expect("scheduled send was committed").id,
        ObjectId("new-submission".to_string())
    );
    let submissions = transport.calls("EmailSubmission/set");
    assert_eq!(submissions.len(), 1);
    let create = &submissions[0]["create"][SUBMISSION_CREATE_ID];
    assert!(create.get("undoStatus").is_none());
    assert_eq!(
        submissions[0]["onSuccessDestroyEmail"],
        serde_json::json!([format!("#{SUBMISSION_CREATE_ID}")])
    );
    assert!(create["envelope"]["mailFrom"]["parameters"]["holduntil"].is_string());
}

#[tokio::test]
async fn acknowledged_scheduled_send_with_missing_submission_id_is_partial() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.submission_reply = Some("missing_submission_id");
    let mut request = basic_send_request();
    request.scheduled = Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800));

    let error = send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport.clone()), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        usize::MAX,
        None,
        request,
    )
    .await
    .expect_err("an empty scheduled-send id cannot be returned");

    assert_eq!(
        error.kind(),
        &bifrost_types::AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::MissingField)
    );

    assert_eq!(transport.calls("EmailSubmission/set").len(), 1);
    assert!(transport.calls("Email/set")[0].get("destroy").is_none());
    assert!(
        matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.reason == bifrost_types::ReconcileReason::PartialCommit && a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget, bifrost_types::ReconcileAction::DedupeByClientId, bifrost_types::ReconcileAction::ObserveOnly])
    );
}

#[tokio::test]
async fn scheduled_send_uses_the_selected_identity_when_from_is_absent() {
    let transport = SendTransport::new(false, false, false, false);
    let mut request = basic_send_request();
    request.from = None;
    request.identity = Some(bifrost_types::IdentityId("identity-selected".to_string()));
    request.scheduled = Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800));

    let result = send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport.clone()), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        usize::MAX,
        None,
        request,
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(
        transport.method_sequence(),
        vec![
            "Identity/get",
            "Mailbox/get",
            "Email/set",
            "EmailSubmission/set"
        ]
    );
    let identities = transport.calls("Identity/get");
    assert_eq!(
        identities[0]["ids"],
        serde_json::json!(["identity-selected"])
    );
    let emails = transport.calls("Email/set");
    let email_create = emails[0]["create"]
        .as_object()
        .and_then(|creates| creates.values().next())
        .expect("Email/create entry");
    assert_eq!(email_create["from"][0]["email"], "selected@example.test");
    let submissions = transport.calls("EmailSubmission/set");
    let create = &submissions[0]["create"][SUBMISSION_CREATE_ID];
    assert_eq!(
        create["envelope"]["mailFrom"]["email"],
        "selected@example.test"
    );
    assert!(create["envelope"]["mailFrom"]["parameters"]["holduntil"].is_string());
    assert_eq!(create["identityId"], "identity-selected");
}

#[tokio::test]
async fn scheduled_send_does_not_guess_an_identity_when_none_is_selected() {
    let transport = SendTransport::new(false, false, false, false);
    let mut request = basic_send_request();
    request.from = None;
    request.identity = None;
    request.scheduled = Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800));

    let error = send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport.clone()), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        usize::MAX,
        None,
        request,
    )
    .await
    .expect_err("JMAP does not identify a default Identity");

    assert!(matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert!(transport.method_sequence().is_empty());
}

#[tokio::test]
async fn reschedule_cancels_before_creating_and_preserves_the_email() {
    let transport = SendTransport::new(false, false, false, false);
    let result = reschedule_send(
        (send_account(transport.clone()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        usize::MAX,
        ObjectId("old-submission".to_string()),
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800),
    )
    .await;

    assert_eq!(
        result.expect("replacement submission was committed"),
        ObjectId("new-submission".to_string())
    );
    assert_eq!(
        transport.method_sequence(),
        vec![
            "EmailSubmission/get",
            "Email/get",
            "EmailSubmission/set",
            "EmailSubmission/set"
        ]
    );
    let sets = transport.calls("EmailSubmission/set");
    assert_eq!(sets.len(), 2);
    assert!(sets[0]["update"]["old-submission"].is_object());
    assert!(sets[0].get("create").is_none());
    assert!(
        sets[1]["create"][SUBMISSION_CREATE_ID]
            .get("undoStatus")
            .is_none()
    );
    assert!(sets[1].get("update").is_none());
}

#[tokio::test]
async fn reschedule_does_not_create_when_cancellation_is_refused() {
    let transport = SendTransport::new(false, true, false, false);
    let result = reschedule_send(
        (send_account(transport.clone()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        usize::MAX,
        ObjectId("old-submission".to_string()),
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800),
    )
    .await;

    assert!(result.is_err());
    let sets = transport.calls("EmailSubmission/set");
    assert_eq!(sets.len(), 1, "the replacement is never created");
    assert!(sets[0].get("create").is_none());
}

#[tokio::test]
async fn reschedule_checks_the_email_before_canceling_legacy_destroyed_schedules() {
    let transport = SendTransport::new(false, false, false, true);
    let result = reschedule_send(
        (send_account(transport.clone()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        usize::MAX,
        ObjectId("old-submission".to_string()),
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(4_102_444_800),
    )
    .await;

    assert!(result.is_err());
    assert_eq!(
        transport.method_sequence(),
        vec!["EmailSubmission/get", "Email/get"]
    );
    assert!(transport.calls("EmailSubmission/set").is_empty());
}

#[derive(Clone)]
struct DeleteThreadTransport {
    calls: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    fail_set_number: Option<usize>,
    omit_new_state: bool,
    partial_set: bool,
    refuse_set_number: Option<usize>,
}

impl DeleteThreadTransport {
    fn new() -> Self {
        Self {
            calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            fail_set_number: None,
            omit_new_state: false,
            partial_set: false,
            refuse_set_number: None,
        }
    }

    fn failing_on_set(number: usize) -> Self {
        Self {
            fail_set_number: Some(number),
            ..Self::new()
        }
    }

    fn calls(&self) -> Vec<serde_json::Value> {
        self.calls.lock().expect("calls").clone()
    }
}

impl crate::core::transport::HttpTransport for DeleteThreadTransport {
    async fn api_request(
        &self,
        _url: &str,
        body: Vec<u8>,
        _replay: bifrost_net::ReplayPolicy,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        let request: serde_json::Value = serde_json::from_slice(&body).expect("request json");
        let call = request["methodCalls"][0].clone();
        self.calls.lock().expect("calls").push(call.clone());
        let method = call[0].as_str().expect("method name").to_string();
        if method == "Email/set"
            && self.fail_set_number
                == Some(
                    self.calls
                        .lock()
                        .expect("calls")
                        .iter()
                        .filter(|call| call[0] == "Email/set")
                        .count(),
                )
        {
            return Err(crate::core::transport::TransportError::new(
                "second Email/set batch failed",
            ));
        }
        if method == "Email/set"
            && self.refuse_set_number
                == Some(
                    self.calls
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|c| c[0] == "Email/set")
                        .count(),
                )
        {
            return Ok(Bytes::from(serde_json::json!({"sessionState":"session-1","methodResponses":[["error",{"type":"serverUnavailable"},call[2]]]}).to_string()));
        }
        let call_id = call[2].clone();
        let account_id = call[1]["accountId"].clone();
        let arguments = match method.as_str() {
            "Mailbox/get" => serde_json::json!({
                "accountId": account_id,
                "state": "mailbox-state",
                "list": [{ "id": "trash-id", "name": "Trash", "role": "trash" }],
                "notFound": []
            }),
            "Thread/get" => serde_json::json!({
                "accountId": account_id,
                "state": "thread-state",
                "list": [{
                    "id": "thread-1",
                    "emailIds": ["trash-copy-1", "sent-copy", "trash-copy-2", "inbox-copy", "trash-copy-3"]
                }],
                "notFound": []
            }),
            "Email/get" => {
                let requested = call[1]["ids"]
                    .as_array()
                    .expect("Email/get ids")
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect::<HashSet<_>>();
                let list = vec![
                    serde_json::json!({
                        "id": "trash-copy-1",
                        "mailboxIds": { "trash-id": true, "sent-id": false }
                    }),
                    serde_json::json!({
                        "id": "trash-copy-2",
                        "mailboxIds": { "trash-id": true, "sent-id": false }
                    }),
                    serde_json::json!({
                        "id": "trash-copy-3",
                        "mailboxIds": { "trash-id": true, "sent-id": false }
                    }),
                    serde_json::json!({
                        "id": "sent-copy",
                        "mailboxIds": { "sent-id": true }
                    }),
                    serde_json::json!({
                        "id": "inbox-copy",
                        "mailboxIds": { "inbox-id": true }
                    }),
                ]
                .into_iter()
                .filter(|email| {
                    email["id"]
                        .as_str()
                        .is_some_and(|id| requested.contains(id))
                })
                .collect::<Vec<_>>();
                serde_json::json!({
                    "accountId": account_id,
                    "state": "email-state",
                    "list": list,
                    "notFound": []
                })
            }
            "Email/set" => {
                let set_number = self
                    .calls
                    .lock()
                    .expect("calls")
                    .iter()
                    .filter(|call| call[0] == "Email/set")
                    .count();
                let prior = call[1]["ifInState"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| match set_number {
                        1 => "email-state".to_string(),
                        2 => "email-next".to_string(),
                        _ => "email-final".to_string(),
                    });
                let next = match set_number {
                    1 => "email-next".to_string(),
                    2 => "email-final".to_string(),
                    _ => format!("email-final-{set_number}"),
                };
                let mut result = serde_json::json!({
                    "accountId": account_id,
                    "oldState": prior,
                    "newState": next,
                });
                if self.omit_new_state {
                    result.as_object_mut().unwrap().remove("newState");
                }
                if let Some(destroyed) = call[1].get("destroy") {
                    result["destroyed"] = destroyed.clone();
                    result["notDestroyed"] = serde_json::json!({});
                } else {
                    let updates = call[1]["update"]
                        .as_object()
                        .expect("Email/set update object");
                    result["updated"] = serde_json::Value::Object(
                        updates
                            .keys()
                            .map(|id| (id.clone(), serde_json::json!({})))
                            .collect(),
                    );
                    result["notUpdated"] = serde_json::json!({});
                    if self.partial_set {
                        let first = call[1]["update"]
                            .as_object()
                            .unwrap()
                            .keys()
                            .next()
                            .unwrap();
                        result["updated"].as_object_mut().unwrap().remove(first);
                        result["notUpdated"] = serde_json::json!({first:{"type":"forbidden"}});
                    }
                }
                result
            }
            other => panic!("unexpected method {other}"),
        };
        let response = serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[method, arguments, call_id]]
        });
        Ok(bytes::Bytes::from(response.to_string()))
    }

    async fn upload(
        &self,
        _url: &str,
        _body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no upload"))
    }

    async fn download(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no download"))
    }

    async fn get_session(
        &self,
        _url: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        Err(crate::core::transport::TransportError::new("no session"))
    }
}

#[tokio::test]
async fn deleting_a_thread_from_a_case_distinct_folder_moves_instead_of_destroying() {
    let transport = DeleteThreadTransport::new();
    let client = crate::client::Client::with_transport(
        transport.clone(),
        search_session(),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let mail = crate::sync::registry::Slot::new(client, "primary");
    let states: StateMap = crate::sync::state_cache::seeded(std::collections::HashMap::new());
    delete_thread(
        crate::sync::registry::Slot::from_test(mail.clone(), Arc::clone(&states)),
        ThreadId("thread-1".to_string()),
        Some(ContainerId("TRASH-ID".to_string())),
    )
    .await
    .expect("case-distinct folder moves to Trash");
    let calls = transport.calls();
    let sets = calls
        .iter()
        .filter(|call| call[0] == "Email/set")
        .collect::<Vec<_>>();
    assert!(!sets.is_empty());
    for set in sets {
        assert!(set[1].get("destroy").is_none());
        assert!(set[1]["update"].is_object());
    }
}

#[tokio::test]
async fn deleting_a_thread_from_trash_destroys_only_members_in_trash() {
    let transport = DeleteThreadTransport::new();
    let mut session = serde_json::to_value(search_session()).expect("session serializes");
    session["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInGet"] = serde_json::json!(2);
    session["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInSet"] = serde_json::json!(2);
    let client = crate::client::Client::with_transport(
        transport.clone(),
        serde_json::from_value(session).expect("session parses"),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let mail = crate::sync::registry::Slot::new(client, "primary");
    let states: StateMap = crate::sync::state_cache::seeded(std::collections::HashMap::new());

    delete_thread(
        crate::sync::registry::Slot::from_test(mail.clone(), Arc::clone(&states)),
        ThreadId("thread-1".to_string()),
        Some(ContainerId("trash-id".to_string())),
    )
    .await
    .expect("only the Trash copy is destroyed");

    let calls = transport.calls();
    assert_eq!(
        calls
            .iter()
            .map(|call| call[0].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "Mailbox/get",
            "Thread/get",
            "Email/get",
            "Email/get",
            "Email/get",
            "Email/set",
            "Email/set"
        ]
    );
    assert_eq!(calls[2][1]["ids"].as_array().unwrap().len(), 2);
    assert_eq!(calls[3][1]["ids"].as_array().unwrap().len(), 2);
    assert_eq!(calls[4][1]["ids"].as_array().unwrap().len(), 1);
    for get in [&calls[2], &calls[3], &calls[4]] {
        assert_eq!(
            get[1]["properties"],
            serde_json::json!(["id", "mailboxIds"])
        );
    }
    assert_eq!(
        calls[5][1]["destroy"],
        serde_json::json!(["trash-copy-1", "trash-copy-2"])
    );
    assert_eq!(calls[5][1]["ifInState"], "email-state");
    assert_eq!(calls[6][1]["destroy"], serde_json::json!(["trash-copy-3"]));
    assert_eq!(calls[6][1]["ifInState"], "email-next");
    assert!(calls[5][1].get("update").is_none());
    assert!(calls[6][1].get("update").is_none());
}

#[tokio::test]
async fn thread_membership_patch_batches_at_the_advertised_set_limit() {
    let transport = DeleteThreadTransport::new();
    let mut session = serde_json::to_value(search_session()).expect("session serializes");
    session["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInSet"] = serde_json::json!(2);
    let client = crate::client::Client::with_transport(
        transport.clone(),
        serde_json::from_value(session).expect("session parses"),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let mail = crate::sync::registry::Slot::new(client, "primary");
    let states: StateMap = crate::sync::state_cache::seeded(std::collections::HashMap::new());
    let ids = ["M1", "M2", "M3", "M4", "M5"]
        .into_iter()
        .map(crate::email::EmailId::new)
        .collect::<Vec<_>>();

    patch_mailbox_membership_of(
        &crate::sync::registry::Slot::from_test(mail.clone(), Arc::clone(&states)),
        &ids,
        ContainerId("target-mailbox".to_string()),
        true,
        AccountOperation::AddToContainer,
    )
    .await
    .expect("thread membership patches honor maxObjectsInSet");

    let calls = transport.calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0][1]["update"].as_object().unwrap().len(), 2);
    assert_eq!(calls[1][1]["update"].as_object().unwrap().len(), 2);
    assert_eq!(calls[2][1]["update"].as_object().unwrap().len(), 1);
    assert_eq!(
        calls[0][1]["update"]["M1"]["mailboxIds/target-mailbox"],
        true
    );
}

#[tokio::test]
async fn a_later_thread_destroy_batch_failure_reports_partial_completion() {
    let transport = DeleteThreadTransport::failing_on_set(2);
    let mut session = serde_json::to_value(search_session()).expect("session serializes");
    session["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInGet"] = serde_json::json!(2);
    session["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInSet"] = serde_json::json!(2);
    let client = crate::client::Client::with_transport(
        transport.clone(),
        serde_json::from_value(session).expect("session parses"),
        "https://example.test/.well-known/jmap",
    )
    .expect("client builds");
    let mail = crate::sync::registry::Slot::new(client, "primary");
    let states: StateMap = crate::sync::state_cache::seeded(std::collections::HashMap::new());

    let error = delete_thread(
        crate::sync::registry::Slot::from_test(mail.clone(), Arc::clone(&states)),
        ThreadId("thread-1".to_string()),
        Some(ContainerId("trash-id".to_string())),
    )
    .await
    .expect_err("a failed later batch follows an acknowledged destroy");

    assert!(matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::Transport(_)
    ));
    assert!(error.recovery().is_retryable());
    assert!(!error.partial_commit().unwrap().steps().is_empty());
    assert_eq!(
        transport
            .calls()
            .iter()
            .filter(|call| call[0] == "Email/set")
            .count(),
        2
    );
}
async fn partial_send(
    reply: &'static str,
    cleanup: Option<&'static str>,
) -> (AccountError, SendTransport) {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.submission_reply = Some(reply);
    transport.cleanup_reply = cleanup;
    let error = send_message(
        (crate::sync::registry::Slot::from_test(send_account(transport.clone()), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        0,
        None,
        basic_send_request(),
    )
    .await
    .unwrap_err();
    (error, transport)
}
#[tokio::test]
async fn partial_commit_server_partial_fail_submission_retains_the_email() {
    let (error, transport) = partial_send("partial", None).await;
    let record = error.partial_commit().unwrap();
    assert_eq!(record.primary().application, Application::Uncertain);
    assert_eq!(record.steps()[0].effect(), Some(StepEffect::Created));
    assert!(record.compensations().is_empty());
    assert_eq!(transport.calls("Email/set").len(), 1);
}
#[tokio::test]
async fn partial_commit_unreadable_send_envelope_requires_a_check() {
    let (error, _) = partial_send("envelope", None).await;
    assert!(matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ParseFailed)
    ));
    assert!(
        matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget,bifrost_types::ReconcileAction::DedupeByClientId,bifrost_types::ReconcileAction::ObserveOnly])
    );
}
#[tokio::test]
async fn partial_commit_evidence_less_send_drop_is_in_flight() {
    let (error, _) = partial_send("transport", None).await;
    assert!(error.partial_commit().is_none());
    assert_eq!(Application::from_leaf(&error), Application::Uncertain);
    assert!(error.recovery().requires_reconciliation());
}
#[tokio::test]
async fn partial_commit_missing_submission_result_records_the_created_email() {
    let (error, _) = partial_send("missing", None).await;
    assert_eq!(
        error.partial_commit().unwrap().steps()[0].targets(),
        &email_targets(Some(&EmailId::new("email-created")))
    );
}
#[tokio::test]
async fn partial_commit_refused_submission_cleanup_failure_is_surfaced() {
    let (error, _) = partial_send("method", Some("refused")).await;
    assert!(matches!(
        error.recovery(),
        bifrost_types::RecoveryClass::NoPermission { .. }
    ));
    assert!(matches!(
        error.partial_commit().unwrap().compensations()[0].outcome(),
        CompensationOutcome::Failed(_)
    ));
}
#[tokio::test]
async fn partial_commit_refused_submission_cleanup_unconfirmed_keeps_refusal() {
    let (error, _) = partial_send("method", Some("missing")).await;
    assert!(matches!(
        error.recovery(),
        bifrost_types::RecoveryClass::NoPermission { .. }
    ));
    assert!(matches!(
        error.partial_commit().unwrap().compensations()[0].outcome(),
        CompensationOutcome::Unconfirmed(_)
    ));
}
#[tokio::test]
async fn partial_commit_draft_send_missing_submission_result_requires_a_check() {
    let mut transport = SendTransport::new(false, false, false, false);
    transport.submission_reply = Some("missing");
    let error = draft_send(
        (crate::sync::registry::Slot::from_test(send_account(transport), send_state_map()))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        bifrost_types::DraftHandle("draft".into()),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.partial_commit().unwrap().primary().application,
        Application::Uncertain
    );
    assert!(error.recovery().requires_reconciliation());
    assert_eq!(error.operation(), Some(AccountOperation::DraftSend));
    assert_eq!(
        error.partial_commit().unwrap().primary().targets.refs[0].scope,
        ErrorScope::Message {
            id: ObjectId("draft".into())
        }
    );
}
#[tokio::test]
async fn partial_commit_reschedule_refused_replacement_reports_cancellation() {
    let transport = SendTransport::new(true, false, false, false);
    let error = reschedule_send(
        (send_account(transport))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        3600,
        ObjectId("old-submission".into()),
        SystemTime::now() + std::time::Duration::from_secs(60),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.partial_commit().unwrap().steps()[0].effect(),
        Some(StepEffect::Cancelled)
    );
    assert!(error.recovery().is_terminal());
}
/// A replacement create whose connection was refused carries explicit
/// `Unsent` evidence. It must build (never panic) and must not be raised
/// to an uncertain submission: no dedupe, but the committed cancellation
/// still protects the operation.
#[tokio::test]
async fn partial_commit_reschedule_unsent_replacement_is_not_uncertain() {
    let mut transport = SendTransport::new(true, false, false, false);
    transport.submission_reply = Some("refused-connection");
    let error = reschedule_send(
        (send_account(transport))
            .view::<crate::core::capability::Submission>()
            .unwrap(),
        3600,
        ObjectId("old-submission".into()),
        SystemTime::now() + std::time::Duration::from_secs(60),
    )
    .await
    .unwrap_err();
    let record = error.partial_commit().unwrap();
    assert_eq!(record.primary().application, Application::NotApplied);
    assert_eq!(record.steps()[0].effect(), Some(StepEffect::Cancelled));
    assert!(
        matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.reason == bifrost_types::ReconcileReason::PartialCommit && a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget, bifrost_types::ReconcileAction::ObserveOnly])
    );
}
#[test]
fn shared_set_progress_qualifies_primary_committed_and_failed_targets() {
    let mut progress = SetProgress::default().with_owner(Some("shared".to_string()));
    progress.item(
        &EmailId::new("applied"),
        Ok(()),
        AccountOperation::SetKeyword,
    );
    for id in ["missing-one", "missing-two"] {
        progress.item(
            &EmailId::new(id),
            Err::<(), _>(crate::Error::IdUnanswered {
                id: id.to_owned(),
                context: crate::core::answer::AnswerContext {
                    method: "Email/set",
                    guarded: false,
                    write: true,
                },
            }),
            AccountOperation::SetKeyword,
        );
    }
    let error = progress.finish(StepEffect::Updated).unwrap_err();
    let record = error.partial_commit().unwrap();
    let target = |id| ErrorScope::Message {
        id: ObjectId(super::super::foreign::encode_object("shared", id)),
    };
    assert_eq!(
        record.primary().targets.refs[0].scope,
        target("missing-one")
    );
    assert_eq!(record.steps().len(), 2);
    assert_eq!(record.steps()[0].targets().refs[0].scope, target("applied"));
    assert_eq!(
        record.steps()[1].targets().refs[0].scope,
        target("missing-two")
    );
}

#[test]
fn absent_keyword_target_is_skipped_without_committed_write_evidence() {
    let mut progress = SetProgress::default();
    progress.item(
        &EmailId::new("gone"),
        Err::<(), _>(crate::Error::Set(
            serde_json::from_str(r#"{"type":"notFound"}"#).unwrap(),
        )),
        AccountOperation::SetKeyword,
    );
    assert!(!progress.is_failed());
    assert!(progress.committed.is_empty());
    assert!(progress.finish(StepEffect::Updated).is_ok());
}

#[tokio::test]
async fn partial_commit_missing_new_state_after_destroy_is_terminal() {
    let mut transport = DeleteThreadTransport::new();
    transport.omit_new_state = true;
    let mut session = serde_json::to_value(search_session()).unwrap();
    session["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInSet"] = serde_json::json!(2);
    let client = crate::client::Client::with_transport(
        transport,
        serde_json::from_value(session).unwrap(),
        "https://example.test/.well-known/jmap",
    )
    .unwrap();
    let mail = crate::sync::registry::Slot::new(client, "primary");
    let progress = destroy_thread_emails_if_unchanged(
        &crate::sync::registry::Slot::from_test(mail.clone(), send_state_map()),
        None,
        vec![
            EmailId::new("one"),
            EmailId::new("two"),
            EmailId::new("three"),
        ],
        Some("state".into()),
        AccountOperation::BulkDestroy,
    )
    .await
    .unwrap();
    let error = progress.finish(StepEffect::Deleted).unwrap_err();
    assert!(matches!(
        error.recovery(),
        bifrost_types::RecoveryClass::ProviderContractViolation
    ));
    assert_eq!(error.partial_commit().unwrap().steps().len(), 2);
}
fn partial_chunk_account(
    transport: DeleteThreadTransport,
    batch_size: usize,
) -> MailAccount<DeleteThreadTransport> {
    let mut session = serde_json::to_value(search_session()).unwrap();
    session["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInSet"] =
        serde_json::json!(batch_size);
    let client = crate::client::Client::with_transport(
        transport,
        serde_json::from_value(session).unwrap(),
        "https://example.test/.well-known/jmap",
    )
    .unwrap();
    crate::sync::registry::Slot::new(client, "primary")
}
#[tokio::test]
async fn partial_commit_chunk_drains_successes_after_the_first_failure() {
    let mut transport = DeleteThreadTransport::new();
    transport.partial_set = true;
    let mail = partial_chunk_account(transport, 50);
    let ids: Vec<_> = ["a", "b", "c"].into_iter().map(EmailId::new).collect();
    let progress = patch_mailbox_membership_of(
        &crate::sync::registry::Slot::from_test(mail.clone(), send_state_map()),
        &ids,
        ContainerId("dest".into()),
        true,
        AccountOperation::BulkMove,
    )
    .await
    .unwrap();
    let error = progress.finish(StepEffect::Updated).unwrap_err();
    assert_eq!(error.partial_commit().unwrap().steps().len(), 2);
    assert_eq!(
        error.partial_commit().unwrap().steps()[1].targets(),
        &email_targets(Some(&EmailId::new("c")))
    );
}
#[tokio::test]
async fn partial_commit_first_chunk_call_failure_is_a_plain_leaf() {
    let mail = partial_chunk_account(DeleteThreadTransport::failing_on_set(1), 2);
    let progress = destroy_thread_emails_if_unchanged(
        &crate::sync::registry::Slot::from_test(mail.clone(), send_state_map()),
        None,
        vec![EmailId::new("a")],
        Some("state".into()),
        AccountOperation::BulkDestroy,
    )
    .await
    .unwrap();
    assert!(
        progress
            .finish(StepEffect::Deleted)
            .unwrap_err()
            .partial_commit()
            .is_none()
    );
}
#[tokio::test]
async fn partial_commit_guarded_thread_destroy_later_chunk_refusal_retries() {
    let mut transport = DeleteThreadTransport::new();
    transport.refuse_set_number = Some(2);
    let mail = partial_chunk_account(transport, 2);
    let progress = destroy_thread_emails_if_unchanged(
        &crate::sync::registry::Slot::from_test(mail.clone(), send_state_map()),
        None,
        vec![EmailId::new("a"), EmailId::new("b"), EmailId::new("c")],
        Some("state".into()),
        AccountOperation::BulkDestroy,
    )
    .await
    .unwrap();
    let error = progress.finish(StepEffect::Deleted).unwrap_err();
    assert!(error.recovery().is_retryable());
    assert_eq!(error.partial_commit().unwrap().steps().len(), 2);
    assert!(
        error
            .partial_commit()
            .unwrap()
            .steps()
            .iter()
            .all(|s| s.replay() == StepReplay::Repeatable)
    );
}
#[tokio::test]
async fn partial_commit_move_thread_remove_leg_failure_keeps_add_leg() {
    let mail = partial_chunk_account(DeleteThreadTransport::failing_on_set(2), 50);
    let error = move_thread(
        crate::sync::registry::Slot::from_test(mail.clone(), send_state_map()),
        ThreadId("thread-1".into()),
        ContainerId("dest".into()),
        Some(ContainerId("source".into())),
    )
    .await
    .unwrap_err();
    assert_eq!(error.partial_commit().unwrap().steps().len(), 5);
    assert!(
        error
            .partial_commit()
            .unwrap()
            .steps()
            .iter()
            .all(|s| s.effect() == Some(StepEffect::Updated))
    );
}

#[tokio::test]
async fn literal_primary_route_preserves_partial_commit_identity() {
    for owner in ["primary", "departed"] {
        for membership in [false, true] {
            let id = crate::sync::foreign::encode_object(owner, "M1");
            let client = crate::client::Client::with_transport(
                OmittingSetTransport::default(),
                search_session(),
                "https://example.test/session",
            )
            .unwrap();
            let registry = crate::sync::registry::Registry::new(crate::sync::registry::Slot::new(
                client, "primary",
            ));
            let slot = registry.object(&id).clone();
            let target = MutationTarget::Message(ObjectId(id.clone()));
            let result = if membership {
                add_to_container(
                    slot,
                    target,
                    ContainerId(crate::sync::foreign::encode_object(owner, "archive")),
                )
                .await
            } else {
                set_keyword(slot, target, "$seen".into(), true).await
            };
            let error = result.unwrap_err();
            let record = error
                .partial_commit()
                .expect("unanswered write retains its target");
            assert_eq!(
                record.primary().targets.refs[0].scope,
                ErrorScope::Message { id: ObjectId(id) }
            );
        }
    }
}
