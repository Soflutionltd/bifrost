//! The EWS streaming arm: which scopes it can subscribe to, the chunked
//! `restId` -> `ewsId` translation, and the per-id reconciliation of its
//! answers.

use bifrost_types::{
    AccountErrorKind, AccountOperation, Cause, CursorScope, ErrorScope, FolderId, ObjectType,
    ProtocolErrorKind, TransmissionState,
};

use crate::account::graph_error::{GraphErrorContext, into_account_error};
use crate::account::push::dispatch::push_item_ids;
use crate::account::push::ews::{
    ConvertIdError, PendingEwsScope, TRANSLATE_EXCHANGE_IDS_MAX_INPUTS,
    TranslateExchangeIdsResponse, TranslatedExchangeId, ews_subscribable_folder_id,
    reconcile_translated_ews_scopes, subscribe_ews, translate_error_context, translate_ews_scopes,
    translation_input_chunks,
};
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};

use super::fixtures::{converted, email_scope, ledger, no_chunk_failures, pending, settle};

/// The EWS Subscribe boundary and the webhook boundary must agree on
/// what "subscribable" means. A non-folder scope contributes nothing
/// to `FolderIds` (empty element -> EWS rejects the whole request),
/// and a foreign folder would be resolved against the PRIMARY
/// mailbox's namespace because the Subscribe path sends no routing
/// headers. Both must be refused locally as `Unsupported`, not
/// converted into a remote failure that reads as a provider fault.
#[test]
fn ews_subscribe_accepts_only_primary_mailbox_folder_scopes() {
    let primary = CursorScope::FolderType {
        folder: FolderId("inbox".to_string()),
        ty: ObjectType::Email,
    };
    // The accepted scope also yields the id the translation request
    // sends: bare, never the raw foreign-encoded string.
    assert_eq!(
        ews_subscribable_folder_id(&primary).as_deref(),
        Some("inbox")
    );

    let foreign = CursorScope::FolderType {
        folder: crate::account::foreign::encode_foreign("shared@contoso.com", "AAMk"),
        ty: ObjectType::Email,
    };
    assert!(ews_subscribable_folder_id(&foreign).is_none());

    // A public-folder scope, and the account-wide scope the webhook
    // path already rejects loudly.
    assert!(ews_subscribable_folder_id(&CursorScope::Folder(FolderId("pf".to_string()))).is_none());
    assert!(ews_subscribable_folder_id(&CursorScope::Account).is_none());
}

#[tokio::test]
async fn exchange_id_translation_posts_each_wire_chunk_and_accumulates_answers() {
    let client = GraphClient::new("token");
    let pending: Vec<_> = (0..=TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
        .map(|index| {
            (
                bifrost_types::BatchItemId(index.to_string()),
                CursorScope::FolderType {
                    folder: FolderId(format!("f{index}")),
                    ty: ObjectType::Email,
                },
                format!("f{index}"),
            )
        })
        .collect();
    let answer = |start: usize, end: usize| serde_json::json!({"value": (start..end).map(|index| serde_json::json!({"sourceId":format!("f{index}"),"targetId":format!("e{index}")})).collect::<Vec<_>>()});
    client.script_rest([
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            answer(0, TRANSLATE_EXCHANGE_IDS_MAX_INPUTS),
        ),
        ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            answer(
                TRANSLATE_EXCHANGE_IDS_MAX_INPUTS,
                TRANSLATE_EXCHANGE_IDS_MAX_INPUTS + 1,
            ),
        ),
    ]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::EwsStreaming);
    let translated = translate_ews_scopes(&account, pending, &mut ledger())
        .await
        .expect("wire fan-out translates all folders");
    assert_eq!(translated.len(), TRANSLATE_EXCHANGE_IDS_MAX_INPUTS + 1);
    let requests = client.take_rest_requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.url.ends_with("/me/translateExchangeIds"))
    );
    assert_eq!(
        requests[0]
            .body
            .as_ref()
            .and_then(|body| body["inputIds"].as_array())
            .map(Vec::len),
        Some(TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
    );
    assert_eq!(
        requests[1]
            .body
            .as_ref()
            .and_then(|body| body["inputIds"].as_array())
            .map(Vec::len),
        Some(1)
    );
}

#[test]
fn translated_ews_ids_stay_paired_with_their_graph_scopes_when_response_is_reordered() {
    let translated = vec![
        converted("rest-archive", "ews-archive"),
        converted("rest-inbox", "ews-inbox"),
    ];

    let resolved = reconcile_translated_ews_scopes(
        pending(&["rest-inbox", "rest-archive"]),
        translated,
        &no_chunk_failures(),
        &mut ledger(),
    );
    assert_eq!(resolved[0].ews_folder_id, "ews-inbox");
    assert_eq!(resolved[1].ews_folder_id, "ews-archive");
}

/// Graph must answer every id it was given. A dropped answer is the
/// provider breaking its own contract, so it classifies as one - and it
/// still names the folder, because "a subscription failed" without a
/// container is not a diagnosable report.
#[test]
fn a_missing_ews_translation_rejects_the_subscription() {
    let mut outcomes = ledger();
    let resolved = reconcile_translated_ews_scopes(
        pending(&["rest-inbox"]),
        Vec::new(),
        &no_chunk_failures(),
        &mut outcomes,
    );
    assert!(
        resolved.is_empty(),
        "an unanswered folder is not subscribed"
    );
    let outcomes = outcomes
        .finalize(&push_item_ids(1))
        .expect("every scope is accounted for");
    let error = &outcomes.failed()[0].error;
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    ));
    assert_eq!(
        error.scope(),
        Some(&ErrorScope::Cursor(email_scope("rest-inbox")))
    );
}

/// Graph converts ids one at a time and reports a failure per id inside
/// an otherwise successful 200. The refused id must fail its own
/// subscription with its own scope and Graph's own code - not take the
/// whole response down as an unparseable body, which is what a required
/// `targetId` produced.
#[test]
fn a_per_id_translation_failure_names_its_scope_and_the_provider_code() {
    let translated = vec![
        converted("rest-inbox", "ews-inbox"),
        TranslatedExchangeId {
            source_id: "rest-stale".to_string(),
            target_id: None,
            error_details: Some(ConvertIdError {
                code: Some("ErrorInvalidIdMalformed".to_string()),
                message: Some("Id is malformed.".to_string()),
            }),
        },
    ];

    let mut outcomes = ledger();
    let resolved = reconcile_translated_ews_scopes(
        pending(&["rest-inbox", "rest-stale"]),
        translated,
        &no_chunk_failures(),
        &mut outcomes,
    );
    assert_eq!(resolved.len(), 1, "the accepted sibling still subscribes");
    assert_eq!(resolved[0].scope, email_scope("rest-inbox"));
    let outcomes = outcomes
        .finalize(&push_item_ids(2))
        .expect("every scope is accounted for");
    assert_eq!(outcomes.succeeded().len(), 1);
    assert_eq!(outcomes.succeeded()[0].output, email_scope("rest-inbox"));
    assert_eq!(outcomes.failed().len(), 1);
    let error = &outcomes.failed()[0].error;
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));
    assert!(error.recovery().is_terminal());
    assert_eq!(
        error.scope(),
        Some(&ErrorScope::Cursor(email_scope("rest-stale")))
    );
    let carries_code = error.chain().iter().any(|cause| {
        matches!(
            cause,
            Cause::Wire(bifrost_types::WireCause::Graph(
                bifrost_types::GraphSignal::Unknown { code }
            )) if code == "ErrorInvalidIdMalformed"
        )
    });
    assert!(carries_code, "the provider code must survive to support");
}

/// The one shape that would otherwise fall through with no id at all.
#[test]
fn a_translation_answer_with_neither_target_nor_error_is_a_contract_violation() {
    let translated = vec![TranslatedExchangeId {
        source_id: "rest-inbox".to_string(),
        target_id: Some("   ".to_string()),
        error_details: None,
    }];
    let mut outcomes = ledger();
    let resolved = reconcile_translated_ews_scopes(
        pending(&["rest-inbox"]),
        translated,
        &no_chunk_failures(),
        &mut outcomes,
    );
    assert!(resolved.is_empty());
    let outcomes = outcomes
        .finalize(&push_item_ids(1))
        .expect("every scope is accounted for");
    let error = &outcomes.failed()[0].error;
    assert!(matches!(
        error.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    ));
}

/// A `restId` is per folder, not per scope, so one answer serves every
/// scope over that folder. Asking twice would waste a slot against
/// Graph's cap for no gain.
#[test]
fn scopes_sharing_a_folder_translate_once_and_both_resolve() {
    let pending = vec![
        (
            bifrost_types::BatchItemId("0".to_string()),
            email_scope("rest-shared"),
            "rest-shared".to_string(),
        ),
        (
            bifrost_types::BatchItemId("1".to_string()),
            CursorScope::FolderType {
                folder: FolderId("rest-shared".to_string()),
                ty: ObjectType::Contact,
            },
            "rest-shared".to_string(),
        ),
    ];
    let chunks = translation_input_chunks(&pending);
    assert_eq!(chunks, vec![vec!["rest-shared".to_string()]]);

    let resolved = reconcile_translated_ews_scopes(
        pending,
        vec![converted("rest-shared", "ews-shared")],
        &no_chunk_failures(),
        &mut ledger(),
    );
    assert_eq!(resolved.len(), 2);
    assert!(resolved.iter().all(|s| s.ews_folder_id == "ews-shared"));
}

/// Graph rejects an `inputIds` collection over 1,000 strings outright,
/// so a large mailbox has to fan out. Before chunking, every scope went
/// into one collection and the subscription failed before EWS setup was
/// even attempted.
#[test]
fn translation_requests_stay_inside_the_graph_input_id_cap() {
    let folders: Vec<String> = (0..2_500).map(|n| format!("rest-{n}")).collect();
    let pending: Vec<PendingEwsScope> = folders
        .iter()
        .enumerate()
        .map(|(index, folder)| {
            (
                bifrost_types::BatchItemId(index.to_string()),
                email_scope(folder),
                folder.clone(),
            )
        })
        .collect();

    let chunks = translation_input_chunks(&pending);
    assert_eq!(chunks.len(), 3);
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.len() <= TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
    );
    let flattened: Vec<String> = chunks.into_iter().flatten().collect();
    assert_eq!(flattened, folders, "order and coverage both survive");
}

/// The translation fan-out exists only because Graph caps `inputIds`, so a
/// chunk boundary is an artifact of the request size, not a property of the
/// folders. One chunk's transport failure used to fail the whole request and
/// discard every answer already collected - denying push to folders Graph had
/// successfully translated because an unrelated later POST died.
#[tokio::test]
async fn a_failed_translation_chunk_fails_only_its_own_scopes() {
    let client = GraphClient::new("token");
    let pending: Vec<PendingEwsScope> = (0..=TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
        .map(|index| {
            (
                bifrost_types::BatchItemId(index.to_string()),
                email_scope(&format!("f{index}")),
                format!("f{index}"),
            )
        })
        .collect();
    let first_chunk = serde_json::json!({
        "value": (0..TRANSLATE_EXCHANGE_IDS_MAX_INPUTS)
            .map(|index| serde_json::json!({
                "sourceId": format!("f{index}"),
                "targetId": format!("e{index}"),
            }))
            .collect::<Vec<_>>()
    });
    client.script_rest([
        ScriptedRestResponse::json(reqwest::StatusCode::OK, first_chunk),
        ScriptedRestResponse::json(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"code":"InternalServerError","message":"no"}}),
        ),
    ]);
    let account = GraphAccount::new_for_tests(client, PushMode::EwsStreaming);
    let mut outcomes = ledger();
    let translated = translate_ews_scopes(&account, pending, &mut outcomes)
        .await
        .expect("a failed chunk is a per-scope failure, not a request failure");
    assert_eq!(
        translated.len(),
        TRANSLATE_EXCHANGE_IDS_MAX_INPUTS,
        "the answered chunk's folders still subscribe"
    );
    let outcomes = outcomes
        .finalize(&push_item_ids(TRANSLATE_EXCHANGE_IDS_MAX_INPUTS + 1))
        .expect("every scope is accounted for exactly once");
    assert_eq!(
        outcomes.succeeded().len(),
        TRANSLATE_EXCHANGE_IDS_MAX_INPUTS
    );
    assert_eq!(outcomes.failed().len(), 1);
    let failure = &outcomes.failed()[0];
    assert_eq!(
        failure.item,
        bifrost_types::BatchItemId(TRANSLATE_EXCHANGE_IDS_MAX_INPUTS.to_string())
    );
    // The failed scope carries the transport error, not the
    // "translateExchangeIds omitted an answer" contract violation it would
    // fall through to if the chunk failure were not recorded per id.
    assert!(
        !matches!(
            failure.error.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        ),
        "a chunk whose POST failed never had a chance to omit an answer: {:?}",
        failure.error.kind()
    );
    assert_eq!(
        failure.error.scope(),
        Some(&ErrorScope::Cursor(email_scope(&format!(
            "f{TRANSLATE_EXCHANGE_IDS_MAX_INPUTS}"
        ))))
    );
}

/// The response Graph documents, decoded end to end: a converted id and
/// a refused one in the same 200. Deserialization must survive the
/// refused entry - the whole finding was that it did not.
#[test]
fn a_mixed_convert_id_result_deserializes() {
    let body = r#"{
        "value": [
            { "sourceId": "rest-a", "targetId": "ews-a" },
            {
                "sourceId": "rest-b",
                "errorDetails": {
                    "code": "ErrorInvalidIdMalformed",
                    "message": "Id is malformed."
                }
            }
        ]
    }"#;
    let response: TranslateExchangeIdsResponse =
        serde_json::from_str(body).expect("a per-id failure is not a parse failure");
    assert_eq!(response.value.len(), 2);
    assert_eq!(response.value[0].target_id.as_deref(), Some("ews-a"));
    assert!(response.value[1].target_id.is_none());
    assert_eq!(
        response.value[1]
            .error_details
            .as_ref()
            .and_then(|details| details.code.as_deref()),
        Some("ErrorInvalidIdMalformed")
    );
}

/// The translation POST creates nothing: it runs before the handle, the
/// local state, and the EWS subscription all exist. A drop mid-flight
/// has no target to reconcile against, so it must stay a plain retry
/// even though `PushSubscribe` is a non-idempotent operation.
#[test]
fn a_dropped_translation_request_retries_instead_of_reconciling() {
    let dropped = crate::error::GraphError::Net(bifrost_net::Error::Network {
        message: "connection reset".to_string(),
        transmission_state: TransmissionState::InFlight,
        source: None,
    });
    let error = into_account_error(dropped, translate_error_context());
    assert!(
        error.recovery().is_retryable(),
        "recovery was {:?}",
        error.recovery()
    );

    // The same drop under the plain PushSubscribe context is the
    // behavior the override corrects, not a coincidence of the kind.
    let dropped = crate::error::GraphError::Net(bifrost_net::Error::Network {
        message: "connection reset".to_string(),
        transmission_state: TransmissionState::InFlight,
        source: None,
    });
    let reconciled = into_account_error(
        dropped,
        GraphErrorContext::graph(AccountOperation::PushSubscribe),
    );
    assert!(reconciled.recovery().requires_reconciliation());
}

// ---- cancellation safety of `subscribe_ews` ------------------------------
//
// This arm creates no server state itself (the worker owns the EWS
// Subscribe), so what a dropped caller can strand is the LOCAL registration:
// installed under a handle nobody received, streamed by the worker until
// `close()`. The window is the awaits after the insert.

fn translate_ok() -> ScriptedRestResponse {
    ScriptedRestResponse::json(
        reqwest::StatusCode::OK,
        serde_json::json!({"value":[{"sourceId":"inbox","targetId":"ews-inbox"}]}),
    )
}

fn registration_count(account: &GraphAccount) -> Option<usize> {
    account
        .ews_subscriptions
        .try_read()
        .ok()
        .map(|registrations| registrations.len())
}

fn spawn_subscribe_ews(
    account: &GraphAccount,
) -> tokio::task::JoinHandle<
    Result<crate::account::push::dispatch::ArmOutcome, bifrost_types::AccountError>,
> {
    let account = account.clone();
    let (item, scope, _) = pending(&["inbox"]).remove(0);
    tokio::spawn(async move {
        let expected = vec![item.clone()];
        subscribe_ews(account, vec![(item, scope)], ledger(), &expected).await
    })
}

/// The normal path disarms: a caller that receives the handle keeps its
/// registration.
///
/// Fails if `subscribe_ews` stops calling `registration.disarm()`.
#[tokio::test]
async fn a_received_ews_subscribe_keeps_its_registration() {
    let client = GraphClient::new("token");
    client.script_rest([translate_ok()]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::EwsStreaming);
    // A worker already occupies the slot, so none is spawned against the
    // scripted wire.

    let (handle, _) = spawn_subscribe_ews(&account)
        .await
        .expect("not aborted")
        .expect("subscribe succeeds");
    assert!(handle.is_some());
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert_eq!(registration_count(&account), Some(1));
    assert!(*account.ews_topology.borrow() > 0);
}

/// A closed account is refused before the translation request, as the webhook
/// arm refuses before its first POST: nothing is registered and no worker is
/// spawned to exit at once.
///
/// Fails if the early `shutdown.is_cancelled()` check in `subscribe_ews` is
/// removed (the empty script then panics on the translation POST).
#[tokio::test]
async fn subscribing_ews_on_a_closed_account_registers_nothing() {
    let client = GraphClient::new("token");
    client.script_rest([]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::EwsStreaming);
    account.shutdown.cancel();

    let error = spawn_subscribe_ews(&account)
        .await
        .expect("not aborted")
        .expect_err("a closed account refuses");

    assert!(matches!(error.kind(), AccountErrorKind::Internal(_)));
    assert_eq!(client.wire_attempts(), 0);
    assert_eq!(registration_count(&account), Some(0));
}

/// `close()` began while the translation was in flight: the registration step
/// refuses instead of installing state no worker will stream.
///
/// Fails if the `shutdown.is_cancelled()` check under the write lock in
/// `subscribe_ews` is removed.
#[tokio::test]
async fn an_ews_subscribe_racing_close_does_not_register() {
    let client = GraphClient::new("token");
    client.script_rest([translate_ok()]);
    let account = GraphAccount::new_for_tests(client.clone(), PushMode::EwsStreaming);

    let registration_blocked = account.ews_subscriptions.write().await;
    let caller = spawn_subscribe_ews(&account);
    assert!(
        settle(|| client.wire_attempts() == 1).await,
        "translation done, parked on the registration lock"
    );
    account.shutdown.cancel();
    drop(registration_blocked);

    caller
        .await
        .expect("not aborted")
        .expect_err("a closing account hands out no handle");
    assert_eq!(registration_count(&account), Some(0));
}
