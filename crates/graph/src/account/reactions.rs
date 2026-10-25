//! Exchange-native message reaction read surface.
//!
//! Reads the two `singleValueExtendedProperties` the Outlook clients write
//! for reactions (`OwnerReactionType`, `ReactionsCount`) via `$batch` GETs,
//! chunked to Graph's batch limit of 20 and merged into ONE
//! [`BatchOutcome`] whose `finalize` accounts for every submitted id.
//!
//! The three-lane mapping is the whole point of the surface:
//!
//! - a batch item with a 2xx status goes to `succeeded`, carrying a
//!   [`MessageReactionState`] whose fields are `None` when the property is
//!   absent - that is a real "no reaction" answer the consumer may act on
//!   (its classifier deletes the cached owner row on it);
//! - a non-2xx item goes to `failed`, as does a public-folder id this
//!   Graph-only surface has no transport for (locally, per item - never as
//!   a top-level rejection that would discard the rest of the batch);
//! - a chunk that never returned, or an item the `$batch` envelope did not
//!   answer, goes to `uncertain`.
//!
//! A `failed` or `uncertain` item MUST NOT be reported as an empty state:
//! collapsing the lanes would let a transient Graph error wipe the
//! consumer's cached reactions.

use std::collections::HashSet;

use bifrost_types::{
    AccountError, AccountOperation, BatchItemId, BatchOutcome, BatchOutcomeBuilder, ErrorScope,
    MessageReactionState, ObjectId,
};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;

use super::GraphAccount;
use super::graph_error::{
    GraphErrorContext, batch_response_missing, into_account_error, response_to_account_error_pub,
    unsupported_item_error,
};
use super::pim::{ews_read_folder, message_batch_url};
use crate::error::{GraphError, GraphResponseError};
use crate::types::{BatchRequest, BatchRequestItem, BatchResponseItem};

/// The MAPI named-property GUID Outlook stores reactions under. Identical
/// to what Outlook Web writes; the ids below are the fully-qualified
/// `singleValueExtendedProperties` ids Graph filters on.
const REACTIONS_GUID: &str = "{41F28F13-83F4-4114-A584-EEDB5A6B0BFF}";

/// Graph's `$batch` request limit.
const BATCH_LIMIT: usize = 20;

#[derive(Debug, Deserialize)]
struct SingleValueExtendedProperty {
    id: String,
    value: String,
}

fn owner_reaction_property_id() -> String {
    format!("String {REACTIONS_GUID} Name OwnerReactionType")
}

fn reactions_count_property_id() -> String {
    format!("Integer {REACTIONS_GUID} Name ReactionsCount")
}

/// Read reaction state for `ids`, chunked internally to the `$batch`
/// limit and merged into one finalized [`BatchOutcome`].
pub(crate) async fn message_reactions(
    account: GraphAccount,
    ids: &[ObjectId],
) -> Result<BatchOutcome<MessageReactionState>, AccountError> {
    let operation = AccountOperation::MessageReactionsRead;
    // Dedupe while preserving order: `finalize` treats a repeated id as a
    // duplicate-lane invariant violation, and a caller asking twice about
    // one message is asking one question.
    let mut seen = HashSet::new();
    let unique: Vec<ObjectId> = ids
        .iter()
        .filter(|id| seen.insert(id.0.clone()))
        .cloned()
        .collect();
    let expected: Vec<BatchItemId> = unique.iter().map(|id| BatchItemId(id.0.clone())).collect();

    // Public-folder ids name EWS items, not Graph messages, and this
    // Graph-only extended-property surface has no EWS implementation. Fail
    // them locally rather than send a native EWS `ItemId` to
    // `/me/messages/{id}` and misreport Graph's 404 as a missing message -
    // the same public-folder discriminator both hydration doors use.
    //
    // Per item, never per request: this surface carries a per-item failure
    // lane, so one public id in a mixed batch must not cost every ordinary
    // Graph message in it its outcome.
    let (supported, unsupported) = partition_supported_ids(unique);

    let mut builder = BatchOutcomeBuilder::new();
    for id in unsupported {
        builder.push_failed(
            BatchItemId(id.0.clone()),
            unsupported_item_error(operation, ErrorScope::Message { id: id.0.into() }),
        );
    }

    let owner_id = owner_reaction_property_id();
    let count_id = reactions_count_property_id();
    let filter = format!("$filter=id eq '{owner_id}' or id eq '{count_id}'");

    let suffix = format!("/singleValueExtendedProperties?{filter}");
    for chunk in supported.chunks(BATCH_LIMIT) {
        // An id whose shared mailbox is no longer configured has no URL to
        // build. It fails in the FAILED LANE, like the public-folder ids
        // above, and for a second reason on top of the per-item one: past
        // the 20-id chunk limit, earlier chunks have already been
        // transmitted, so a top-level `Err` here would both discard their
        // accumulated outcomes and contradict the boundary contract that
        // `Err(_)` means nothing was transmitted.
        let (routable, rejected) = super::batch_routing::partition_routable(chunk, |id| {
            message_batch_url(&account, id, &suffix, operation)
        });
        for (id, error) in rejected {
            builder.push_failed(BatchItemId(id.0.clone()), error);
        }
        let (chunk, urls): (Vec<ObjectId>, Vec<String>) = routable.into_iter().unzip();
        if chunk.is_empty() {
            continue;
        }
        // Index after the split so `classify_chunk` projects responses onto
        // exactly the ids that went out.
        let requests: Vec<BatchRequestItem> = urls
            .into_iter()
            .enumerate()
            .map(|(index, url)| BatchRequestItem {
                id: index.to_string(),
                method: "GET".to_string(),
                url,
                body: None,
                headers: None,
            })
            .collect();
        let replayable: Vec<_> = requests
            .iter()
            .map(super::graph_error::subrequest_replayable)
            .collect();
        match account
            .client
            .post_batch(
                account
                    .batch_resource(&chunk)
                    .map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))?,
                &BatchRequest { requests },
                bifrost_net::ReplayPolicy::Idempotent,
            )
            .await
        {
            Ok(response) => classify_chunk(
                &chunk,
                response.responses,
                operation,
                &mut builder,
                &replayable,
            ),
            Err(e) => {
                // The whole chunk never returned: every item's fate is
                // unknown. Uncertain, never an empty succeeded state.
                let ctx = GraphErrorContext::graph(operation);
                let error = into_account_error(e, ctx);
                for id in &chunk {
                    builder.push_uncertain(BatchItemId(id.0.clone()), error.clone());
                }
            }
        }
    }

    finalize_reactions(builder, &expected, operation)
}

/// Close the ledger, or report that this crate's own accounting broke.
///
/// Nothing the server sends can make `finalize` fail here: every lane id is a
/// deduplicated caller id pushed exactly once, and `classify_chunk` projects
/// Graph's `$batch` answers onto positions it bounds-checks and deduplicates
/// before pushing. A missing, duplicate, or unknown id is therefore a bug in
/// this module - `Internal(InvariantViolated)` - never the provider's
/// contract breach it used to be filed as.
fn finalize_reactions(
    builder: BatchOutcomeBuilder<MessageReactionState>,
    expected: &[BatchItemId],
    operation: AccountOperation,
) -> Result<BatchOutcome<MessageReactionState>, AccountError> {
    builder.finalize(expected).map_err(|e| {
        into_account_error(
            GraphError::Internal {
                message: format!("Graph reaction read broke batch accounting: {e}"),
            },
            GraphErrorContext::graph(operation),
        )
    })
}

/// Split the requested ids into the ones this Graph-only surface can read
/// and the public-folder ids it cannot, preserving submission order in both.
///
/// Pure so the mixed-batch rule is pinnable: the Graph ids of a batch that
/// also names a public-folder item must still reach `$batch`. Their actual
/// round trip is pinned through the `GraphClient` REST seam
/// (`a_mixed_reaction_batch_posts_its_surviving_graph_id`).
fn partition_supported_ids(ids: Vec<ObjectId>) -> (Vec<ObjectId>, Vec<ObjectId>) {
    ids.into_iter()
        .partition(|id| ews_read_folder(id).is_none())
}

/// Project one chunk's `$batch` responses into the outcome lanes.
///
/// Split out of the transport loop so the lane rules are unit-pinnable:
/// 2xx -> succeeded (parsed state), non-2xx -> failed (classified error),
/// unanswered id -> uncertain.
fn classify_chunk(
    chunk: &[ObjectId],
    responses: Vec<BatchResponseItem>,
    operation: AccountOperation,
    builder: &mut BatchOutcomeBuilder<MessageReactionState>,
    replayable: &[bool],
) {
    let mut answered: HashSet<usize> = HashSet::new();
    for item in responses {
        let Some(index) = item.id.parse::<usize>().ok().filter(|i| *i < chunk.len()) else {
            // An id we never submitted; nothing to account it against.
            continue;
        };
        if !answered.insert(index) {
            // Graph answered the same request twice; the first answer won.
            continue;
        }
        let message_id = &chunk[index];
        if (200..300).contains(&item.status) {
            builder.push_succeeded(
                BatchItemId(message_id.0.clone()),
                reaction_state(message_id, item.body.as_ref()),
            );
        } else {
            let headers = header_map(item.headers);
            let body = item
                .body
                .as_ref()
                .map(|v| bytes::Bytes::from(serde_json::to_vec(v).unwrap_or_default()))
                .unwrap_or_default();
            let response = GraphResponseError::from_response(
                StatusCode::from_u16(item.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                headers,
                body,
            );
            let ctx = GraphErrorContext::graph(operation).with_scope(ErrorScope::Message {
                id: (message_id.0.clone()).into(),
            });
            builder.push_failed(
                BatchItemId(message_id.0.clone()),
                response_to_account_error_pub(response, &ctx),
            );
        }
    }
    // Any submitted id the envelope did not answer was acknowledged as a
    // batch but never individually resolved - uncertain by definition.
    // The error rides `batch_response_missing` like the other `$batch`
    // consumers: `Protocol(PartialResponse)` with `Acknowledged`
    // transmission evidence, which derives retryable for this idempotent
    // read. The `ContractViolation` this replaced derived terminal, so a
    // consumer inspecting `recovery()` was told a re-read is pointless for
    // an omission that says nothing about the resource.
    for (index, message_id) in chunk.iter().enumerate() {
        if !answered.contains(&index) {
            builder.push_uncertain(
                BatchItemId(message_id.0.clone()),
                batch_response_missing(
                    operation,
                    Some(ErrorScope::Message {
                        id: (message_id.0.clone()).into(),
                    }),
                    format!(
                        "Graph $batch returned no response for reaction read {index} \
                         (item fate unknown)"
                    ),
                    replayable.get(index).copied().unwrap_or(false),
                ),
            );
        }
    }
}

/// Extract the reaction state from a 2xx `singleValueExtendedProperties`
/// body. Absent properties are real `None` answers; an empty-string
/// `OwnerReactionType` means "reaction removed" and maps to `None` too,
/// mirroring the legacy consumer's trim-and-check.
fn reaction_state(message_id: &ObjectId, body: Option<&Value>) -> MessageReactionState {
    let owner_id = owner_reaction_property_id();
    let count_id = reactions_count_property_id();
    let mut owner_reaction = None;
    let mut reactions_count = None;
    if let Some(values) = body
        .and_then(|body| body.get("value"))
        .and_then(Value::as_array)
    {
        for value in values {
            let Ok(prop) = serde_json::from_value::<SingleValueExtendedProperty>(value.clone())
            else {
                continue;
            };
            if prop.id.eq_ignore_ascii_case(&owner_id) {
                let trimmed = prop.value.trim();
                if !trimmed.is_empty() {
                    owner_reaction = Some(trimmed.to_string());
                }
            } else if prop.id.eq_ignore_ascii_case(&count_id) {
                reactions_count = prop.value.trim().parse::<i64>().ok();
            }
        }
    }
    MessageReactionState {
        id: message_id.clone(),
        owner_reaction,
        reactions_count,
    }
}

fn header_map(
    headers: Option<std::collections::HashMap<String, String>>,
) -> reqwest::header::HeaderMap {
    let mut map = reqwest::header::HeaderMap::new();
    for (name, value) in headers.unwrap_or_default() {
        if let (Ok(name), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_bytes()),
            reqwest::header::HeaderValue::from_str(&value),
        ) {
            map.insert(name, value);
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{GraphClient, ScriptedRestResponse};
    use bifrost_types::BatchItemOutcome;

    fn oid(value: &str) -> ObjectId {
        ObjectId(value.to_string())
    }

    fn props_body(entries: &[(&str, &str)]) -> Value {
        serde_json::json!({
            "value": entries
                .iter()
                .map(|(id, value)| serde_json::json!({ "id": id, "value": value }))
                .collect::<Vec<_>>()
        })
    }

    #[test]
    fn reaction_state_reads_owner_and_count() {
        let body = props_body(&[
            (owner_reaction_property_id().as_str(), "heart"),
            (reactions_count_property_id().as_str(), "3"),
        ]);
        let state = reaction_state(&oid("m1"), Some(&body));
        assert_eq!(state.owner_reaction.as_deref(), Some("heart"));
        assert_eq!(state.reactions_count, Some(3));
    }

    #[test]
    fn empty_owner_value_is_a_real_none_answer() {
        let body = props_body(&[(owner_reaction_property_id().as_str(), "  ")]);
        let state = reaction_state(&oid("m1"), Some(&body));
        assert_eq!(state.owner_reaction, None);
        assert_eq!(state.reactions_count, None);
    }

    #[test]
    fn absent_properties_and_absent_body_are_none() {
        assert_eq!(reaction_state(&oid("m1"), None).owner_reaction, None);
        let empty = props_body(&[]);
        let state = reaction_state(&oid("m1"), Some(&empty));
        assert_eq!(state.owner_reaction, None);
        assert_eq!(state.reactions_count, None);
    }

    #[test]
    fn property_id_match_is_case_insensitive() {
        let body = props_body(&[(owner_reaction_property_id().to_uppercase().as_str(), "like")]);
        let state = reaction_state(&oid("m1"), Some(&body));
        assert_eq!(state.owner_reaction.as_deref(), Some("like"));
    }

    fn public_id(folder: &str, item: &str) -> ObjectId {
        super::super::foreign::encode_public_item_id(
            &bifrost_types::FolderId(folder.to_string()),
            item,
        )
    }

    /// A public-folder id fails locally rather than being sent to
    /// `/me/messages/{ItemId}`, which 404s naming a bare EWS item id - but
    /// it fails in the FAILED LANE, per item. A top-level `Unsupported`
    /// would be a per-request answer on a per-item surface.
    #[tokio::test]
    async fn public_folder_ids_fail_in_their_own_lane_not_the_whole_request() {
        let id = public_id("folder", "native");
        let account = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token"),
            super::super::PushMode::GraphSubscriptions,
        );
        // No supported ids means no `$batch` leaves the process.
        let outcome = message_reactions(account, std::slice::from_ref(&id))
            .await
            .expect("a public id is a per-item failure, not a request failure");
        assert!(outcome.succeeded().is_empty());
        assert!(outcome.uncertain().is_empty());
        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, id.0);
        assert!(matches!(
            outcome.failed()[0].error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(
                bifrost_types::AccountOperation::MessageReactionsRead
            )
        ));
    }

    /// A stale shared-mailbox id fails in the FAILED LANE too, never as a
    /// top-level `Err`. Two contracts forbid the propagation this builder
    /// used to do: the surface answers per item, and past the 20-id chunk
    /// limit earlier chunks have already been transmitted - so an `Err(_)`,
    /// which the boundary contract defines as "nothing was transmitted",
    /// would both misreport that and discard the outcomes it accumulated.
    ///
    /// Hermetic because nothing in this batch routes, so no chunk reaches
    /// `$batch`.
    #[tokio::test]
    async fn a_stale_shared_mailbox_id_fails_in_its_lane_not_the_whole_request() {
        let account = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token"),
            super::super::PushMode::GraphSubscriptions,
        );
        let scope = bifrost_types::CursorScope::FolderType {
            folder: super::super::foreign::encode_foreign("gone@contoso.com", "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let stale = super::super::foreign::encode_message_id(&scope, "AAMkmsg");
        let public = public_id("pf", "item");
        let outcome = message_reactions(account, &[stale.clone(), public.clone()])
            .await
            .expect("a stale id is a per-item failure, not a request failure");
        assert!(outcome.succeeded().is_empty());
        assert!(outcome.uncertain().is_empty());
        assert_eq!(outcome.failed().len(), 2);
        let stale_entry = outcome
            .failed()
            .iter()
            .find(|failure| failure.item.0 == stale.0)
            .expect("the stale id holds its own lane entry");
        assert!(matches!(
            stale_entry.error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        // The error names the message, not just the operation, so an
        // operator can tell which id the configuration no longer covers.
        assert_eq!(
            stale_entry.error.scope(),
            Some(&ErrorScope::Message {
                id: (stale.0.clone()).into()
            })
        );
        assert!(
            outcome
                .failed()
                .iter()
                .any(|failure| failure.item.0 == public.0)
        );
    }

    #[tokio::test]
    async fn a_mixed_reaction_batch_posts_its_surviving_graph_id() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({
                "responses": [{"id":"0", "status":200, "body":{"value":[]}}]
            }),
        )]);
        let account =
            GraphAccount::new_for_tests(client.clone(), super::super::PushMode::GraphSubscriptions);
        let graph = oid("graph-message");
        let public = public_id("pf", "item");
        let outcome = message_reactions(account, &[public.clone(), graph.clone()])
            .await
            .expect("per-item outcome");
        assert_eq!(outcome.succeeded().len(), 1);
        assert_eq!(outcome.succeeded()[0].item.0, graph.0);
        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, public.0);
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].url.ends_with("/$batch"));
        assert_eq!(
            requests[0]
                .body
                .as_ref()
                .and_then(|body| body["requests"].as_array())
                .map(Vec::len),
            Some(1)
        );
    }

    /// The mixed-batch rule the per-item split exists for: one public id
    /// must not strand the ordinary Graph messages beside it. The `$batch`
    /// round trip is pinned above through the REST seam; this pins the pure
    /// partition itself - order preserved on both sides.
    #[test]
    fn a_public_id_does_not_take_the_graph_ids_of_its_batch_with_it() {
        let (supported, unsupported) = partition_supported_ids(vec![
            oid("graph-1"),
            public_id("pf", "item-1"),
            oid("graph-2"),
            public_id("pf", "item-2"),
        ]);
        let supported: Vec<&str> = supported.iter().map(|id| id.0.as_str()).collect();
        assert_eq!(supported, ["graph-1", "graph-2"]);
        assert_eq!(unsupported.len(), 2);
        assert_eq!(unsupported[0], public_id("pf", "item-1"));
        assert_eq!(unsupported[1], public_id("pf", "item-2"));
    }

    fn response_item(id: &str, status: u16, body: Option<Value>) -> BatchResponseItem {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "status": status,
            "body": body,
        }))
        .expect("batch response item deserializes")
    }

    #[test]
    fn classify_chunk_routes_items_into_the_three_lanes() {
        let chunk = [oid("ok"), oid("gone"), oid("unanswered")];
        let mut builder = BatchOutcomeBuilder::new();
        classify_chunk(
            &chunk,
            vec![
                response_item(
                    "0",
                    200,
                    Some(props_body(&[(
                        owner_reaction_property_id().as_str(),
                        "heart",
                    )])),
                ),
                response_item("1", 404, None),
            ],
            AccountOperation::MessageReactionsRead,
            &mut builder,
            &vec![true; chunk.len()],
        );
        let outcome = builder
            .finalize(&[
                BatchItemId("ok".to_string()),
                BatchItemId("gone".to_string()),
                BatchItemId("unanswered".to_string()),
            ])
            .expect("every id lands in exactly one lane");

        assert_eq!(outcome.succeeded().len(), 1);
        assert_eq!(outcome.succeeded()[0].item.0, "ok");
        assert_eq!(
            outcome.succeeded()[0].output.owner_reaction.as_deref(),
            Some("heart")
        );
        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, "gone");
        assert_eq!(outcome.uncertain().len(), 1);
        assert_eq!(outcome.uncertain()[0].item.0, "unanswered");

        // The unanswered id's error must carry the shared `$batch`
        // ambiguity classification: `Protocol(PartialResponse)` with
        // `Acknowledged` evidence, retryable for this idempotent read.
        // A terminal `ContractViolation` here would tell the consumer a
        // re-read is pointless for an omission that says nothing about
        // the resource.
        let error = &outcome.uncertain()[0].error;
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::PartialResponse
            )
        ));
        assert!(error.recovery().is_retryable());
        assert_eq!(
            error.telemetry_fields().transmission_state,
            Some(bifrost_types::TransmissionState::Acknowledged)
        );
    }

    /// A ledger that does not account for an id is this module's own bug.
    /// It was `Protocol(ContractViolation)`, which told the operator to
    /// contact Microsoft about an accounting error only bifrost can make.
    #[test]
    fn broken_reaction_accounting_is_internal_not_a_provider_fault() {
        let mut builder = BatchOutcomeBuilder::new();
        builder.push_succeeded(
            BatchItemId("m1".to_string()),
            reaction_state(&oid("m1"), None),
        );
        let error = finalize_reactions(
            builder,
            &[BatchItemId("m1".to_string()), BatchItemId("m2".to_string())],
            AccountOperation::MessageReactionsRead,
        )
        .expect_err("an unaccounted id must fail finalize");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Internal(
                bifrost_types::InternalErrorKind::InvariantViolated
            )
        );
        assert_eq!(
            error.recovery(),
            &bifrost_types::RecoveryClass::InternalFailure
        );
    }

    #[test]
    fn failed_item_never_carries_an_empty_state() {
        // The lane split is the contract: a 404 must land in `failed`,
        // not surface as a succeeded item with `None` fields (which the
        // consumer would read as "reaction removed" and delete on).
        let chunk = [oid("m1")];
        let mut builder = BatchOutcomeBuilder::new();
        classify_chunk(
            &chunk,
            vec![response_item("0", 503, None)],
            AccountOperation::MessageReactionsRead,
            &mut builder,
            &vec![true; chunk.len()],
        );
        let outcome = builder
            .finalize(&[BatchItemId("m1".to_string())])
            .expect("finalize");
        assert!(outcome.succeeded().is_empty());
        assert_eq!(outcome.failed().len(), 1);
    }

    #[test]
    fn duplicate_and_unknown_response_ids_do_not_double_account() {
        let chunk = [oid("m1")];
        let mut builder = BatchOutcomeBuilder::new();
        classify_chunk(
            &chunk,
            vec![
                response_item("0", 200, Some(props_body(&[]))),
                // Graph answering twice, and answering an id never sent:
                // both must be ignored rather than double-pushed.
                response_item("0", 500, None),
                response_item("7", 200, None),
            ],
            AccountOperation::MessageReactionsRead,
            &mut builder,
            &vec![true; chunk.len()],
        );
        let outcome = builder
            .finalize(&[BatchItemId("m1".to_string())])
            .expect("finalize");
        assert_eq!(outcome.succeeded().len(), 1);
        assert!(outcome.failed().is_empty());
        assert!(outcome.uncertain().is_empty());
    }

    #[test]
    fn submission_order_is_preserved_across_lanes() {
        let chunk = [oid("a"), oid("b")];
        let mut builder = BatchOutcomeBuilder::new();
        classify_chunk(
            &chunk,
            vec![
                response_item("1", 200, Some(props_body(&[]))),
                response_item("0", 429, None),
            ],
            AccountOperation::MessageReactionsRead,
            &mut builder,
            &vec![true; chunk.len()],
        );
        let outcome = builder
            .finalize(&[BatchItemId("a".to_string()), BatchItemId("b".to_string())])
            .expect("finalize");
        let ids: Vec<&str> = outcome
            .iter()
            .map(|item| match item {
                BatchItemOutcome::Succeeded(s) => s.item.0.as_str(),
                BatchItemOutcome::Failed(f) => f.item.0.as_str(),
                BatchItemOutcome::Uncertain(u) => u.item.0.as_str(),
            })
            .collect();
        // Push order: responses arrive b-first, then a's failure.
        assert_eq!(ids, ["b", "a"]);
    }
}
