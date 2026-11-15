use std::collections::VecDeque;

use bifrost_types::SubscriptionHandle;

use super::super::PushMode;
use super::super::push::EwsSubscriptionState;
use super::*;

use crate::client::GraphClient;

fn build_subscribe_request(scopes: &[EwsSubscriptionScope]) -> String {
    crate::ews::streaming::build_subscribe_request(
        scopes.iter().map(|scope| scope.ews_folder_id.as_str()),
    )
}

#[test]
fn parses_streaming_notification_xml() {
    let notifications =
        parse_streaming_notifications(NOTIFICATION_XML).expect("parse should succeed");
    assert_eq!(notifications.len(), 2);
    assert_eq!(notifications[0].event_type, EwsStreamingEventType::NewMail);
    assert_eq!(notifications[0].item_id.as_deref(), Some("item-1"));
    assert_eq!(
        notifications[0].parent_folder_id.as_deref(),
        Some("folder-1")
    );
    assert_eq!(notifications[1].event_type, EwsStreamingEventType::Modified);
    assert_eq!(notifications[1].item_id.as_deref(), Some("item-2"));
}

#[test]
fn streaming_frame_decoder_emits_before_the_outer_response_closes() {
    let frame = r#"<m:GetStreamingEventsResponseMessage ResponseClass="Success"><m:Notifications><t:Notification><t:SubscriptionId>sub-1</t:SubscriptionId><t:NewMailEvent><t:ItemId Id="item-1"/><t:ParentFolderId Id="folder-1"/></t:NewMailEvent></t:Notification></m:Notifications></m:GetStreamingEventsResponseMessage>"#;
    let mut decoder = StreamingFrameDecoder::default();
    let split = frame.len() / 2;
    let bytes = frame.as_bytes();
    assert!(
        decoder
            .push(&bytes[..split])
            .expect("first chunk")
            .is_empty()
    );
    let frames = decoder.push(&bytes[split..]).expect("second chunk");
    assert_eq!(frames, vec![frame.to_string()]);
    let notifications = notifications_from_streaming_frame(&frames[0]).expect("frame parses");
    assert_eq!(notifications.len(), 1);
    assert_eq!(
        notifications[0].parent_folder_id.as_deref(),
        Some("folder-1")
    );
}

/// An XML namespace prefix is an alias the writer picks. A response
/// that binds the messages namespace to something other than `m:` -
/// or makes it the default namespace and writes the element with no
/// prefix at all - is exactly as valid, and the framing must recognize
/// it. Byte-matching `<m:` dropped both shapes on the floor: no frame,
/// no partial frame, so the stream ended reporting neither an
/// invalidation nor a failure.
#[test]
fn the_framing_recognizes_any_namespace_prefix_and_the_default_namespace() {
    for frame in [
        r#"<a:GetStreamingEventsResponseMessage ResponseClass="Success"><a:Notifications><b:Notification><b:SubscriptionId>sub-1</b:SubscriptionId><b:NewMailEvent><b:ItemId Id="item-1"/><b:ParentFolderId Id="folder-1"/></b:NewMailEvent></b:Notification></a:Notifications></a:GetStreamingEventsResponseMessage>"#,
        r#"<GetStreamingEventsResponseMessage xmlns="http://schemas.microsoft.com/exchange/services/2006/messages" ResponseClass="Success"><Notifications><Notification xmlns="http://schemas.microsoft.com/exchange/services/2006/types"><SubscriptionId>sub-1</SubscriptionId><NewMailEvent><ItemId Id="item-1"/><ParentFolderId Id="folder-1"/></NewMailEvent></Notification></Notifications></GetStreamingEventsResponseMessage>"#,
    ] {
        let mut decoder = StreamingFrameDecoder::default();
        let bytes = frame.as_bytes();
        let split = bytes.len() / 2;
        assert!(
            decoder
                .push(&bytes[..split])
                .expect("first chunk")
                .is_empty(),
            "{frame}"
        );
        assert!(decoder.has_partial_frame(), "{frame}");
        let frames = decoder.push(&bytes[split..]).expect("second chunk");
        assert_eq!(frames, vec![frame.to_string()]);
        assert!(!decoder.has_partial_frame(), "{frame}");
        let notifications = notifications_from_streaming_frame(&frames[0]).expect("frame parses");
        assert_eq!(notifications.len(), 1, "{frame}");
        assert_eq!(
            notifications[0].parent_folder_id.as_deref(),
            Some("folder-1"),
            "{frame}"
        );
    }
}

/// The local name must be a whole element name, not a substring: an
/// element whose name merely ends with it, and an attribute value that
/// happens to contain it, are not frames.
#[test]
fn the_framing_matches_whole_element_names_only() {
    assert_eq!(
        find_frame_start(br#"<m:NotAGetStreamingEventsResponseMessage>"#),
        None
    );
    assert_eq!(
        find_frame_start(br#"<m:Other why="GetStreamingEventsResponseMessage">"#),
        None
    );
    assert_eq!(
        find_frame_start(br#"<m:GetStreamingEventsResponseMessageExtra>"#),
        None
    );
    assert_eq!(
        find_frame_start(br#"<:GetStreamingEventsResponseMessage>"#),
        None
    );
    // A buffer that stops mid-name is not a hit yet; the boundary byte
    // decides, and it has not arrived.
    assert_eq!(
        find_frame_start(b"<m:GetStreamingEventsResponseMessag"),
        None
    );
    assert_eq!(
        find_frame_start(b"...<m:GetStreamingEventsResponseMessage>"),
        Some(3)
    );
    assert_eq!(
        find_frame_end(b"</zz:GetStreamingEventsResponseMessage >rest"),
        Some(40)
    );
}

#[test]
fn parses_subscribe_response_xml() {
    let subscriptions =
        parse_subscribe_response(&subscribe_response_xml("sub-1")).expect("parse succeeds");
    assert_eq!(subscriptions, vec!["sub-1".to_string()]);
}

fn email_scope(folder: &str) -> CursorScope {
    CursorScope::FolderType {
        folder: bifrost_types::FolderId(folder.to_string()),
        ty: bifrost_types::ObjectType::Email,
    }
}

fn ews_scope(rest_id: &str, ews_id: &str) -> EwsSubscriptionScope {
    EwsSubscriptionScope {
        scope: email_scope(rest_id),
        ews_folder_id: ews_id.to_string(),
    }
}

#[tokio::test]
async fn active_scopes_deduplicate_ews_folder_ids_but_routing_keeps_every_scope() {
    let account = GraphAccount::new_for_tests(
        crate::client::GraphClient::new("token"),
        super::super::PushMode::EwsStreaming,
    );
    account.ews.registrations_for_tests().write().await.insert(
        bifrost_types::SubscriptionHandle("first".to_string()),
        EwsSubscriptionState {
            scopes: vec![ews_scope("rest-a", "ews-shared")],
        },
    );
    account.ews.registrations_for_tests().write().await.insert(
        bifrost_types::SubscriptionHandle("second".to_string()),
        EwsSubscriptionState {
            scopes: vec![ews_scope("rest-b", "ews-shared")],
        },
    );

    let active = active_ews_scopes(&account).await;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].ews_folder_id, "ews-shared");

    let mut routed = scopes_for_folder(&account, "ews-shared")
        .await
        .into_iter()
        .map(|scope| match scope {
            CursorScope::FolderType { folder, .. } => folder.0,
            other => panic!("unexpected scope {other:?}"),
        })
        .collect::<Vec<_>>();
    routed.sort();
    assert_eq!(routed, ["rest-a", "rest-b"]);
}

/// Two handles registering the SAME scope is the overlap case
/// `push_subscribe` allows: without collapsing them, one notification
/// would drive the reconciler through `changes_stream` twice over one
/// cursor. Scopes that merely share a folder stay distinct - each owns
/// its own delta cursor and must be invalidated separately.
#[test]
fn routing_collapses_repeated_scopes_but_keeps_distinct_ones() {
    let mail = ews_scope("rest-a", "ews-shared");
    let duplicate = ews_scope("rest-a", "ews-shared");
    let contacts = EwsSubscriptionScope {
        scope: CursorScope::FolderType {
            folder: bifrost_types::FolderId("rest-a".to_string()),
            ty: bifrost_types::ObjectType::Contact,
        },
        ews_folder_id: "ews-shared".to_string(),
    };
    let elsewhere = ews_scope("rest-z", "ews-other");
    let registrations = [
        mail.clone(),
        duplicate,
        contacts.clone(),
        elsewhere.clone(),
        mail.clone(),
    ];

    let routed = unique_scopes_for_folder(registrations.iter(), "ews-shared");
    assert_eq!(routed, vec![mail.scope.clone(), contacts.scope]);

    // A folder nothing registered routes nowhere; the caller turns the
    // empty result into an account-wide `Unknown` hint.
    assert!(unique_scopes_for_folder(registrations.iter(), "ews-absent").is_empty());
}

/// The Subscribe body must name each EWS folder once even though the
/// registrations that produced it are per scope.
#[test]
fn the_subscribe_union_carries_one_entry_per_ews_folder() {
    let registrations = [
        ews_scope("rest-a", "ews-shared"),
        ews_scope("rest-b", "ews-shared"),
        ews_scope("rest-z", "ews-other"),
        ews_scope("rest-a", "ews-shared"),
    ];

    let deduped = dedupe_by_ews_folder(registrations.iter());
    let folders = deduped
        .iter()
        .map(|scope| scope.ews_folder_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(folders, ["ews-shared", "ews-other"]);

    let body = build_subscribe_request(&deduped);
    assert_eq!(body.matches(r#"<t:FolderId Id="ews-shared"/>"#).count(), 1);
    assert_eq!(body.matches(r#"<t:FolderId Id="ews-other"/>"#).count(), 1);
}

/// The Subscribe body's folder set is the subscription's scope, which
/// EWS answers once; the long poll and the unsubscribe each name one
/// subscription. All three stay inside the single-answer invariant
/// `build_soap_envelope` enforces.
#[test]
fn the_ews_stream_bodies_stay_within_the_single_answer_invariant() {
    let subscribe = build_subscribe_request(&[
        ews_scope("r1", "f1"),
        ews_scope("r2", "f2"),
        ews_scope("r3", "f3"),
    ]);
    assert_eq!(crate::ews::per_answer_request_ids(&subscribe), 0);
    assert_eq!(
        crate::ews::per_answer_request_ids(&build_get_streaming_events_request("sub-1", 30)),
        1
    );
    assert_eq!(
        crate::ews::per_answer_request_ids(&build_unsubscribe_request("sub-1")),
        0
    );
}

#[test]
fn subscribe_request_lists_every_folder_and_the_six_event_types() {
    let body = build_subscribe_request(&[ews_scope("r1", "f1"), ews_scope("r2", "f2")]);
    assert!(body.contains(r#"<t:FolderId Id="f1"/>"#), "{body}");
    assert!(body.contains(r#"<t:FolderId Id="f2"/>"#), "{body}");
    for event in [
        "NewMailEvent",
        "CreatedEvent",
        "DeletedEvent",
        "ModifiedEvent",
        "MovedEvent",
        "CopiedEvent",
    ] {
        assert!(body.contains(event), "{event} missing from {body}");
    }
    // `StreamingSubscriptionRequest` admits only FolderIds and
    // EventTypes; a Watermark is a schema violation EWS rejects.
    assert!(!body.contains("Watermark"));
}

#[test]
fn subscribe_request_escapes_xml_metacharacters() {
    let body = build_subscribe_request(&[ews_scope("rest-id", r#"a&b<c>"d'"#)]);
    assert!(
        body.contains(r#"<t:FolderId Id="a&amp;b&lt;c&gt;&quot;d&apos;"/>"#),
        "{body}"
    );
}

#[test]
fn subscribe_request_uses_the_translated_ews_id_not_the_graph_rest_id() {
    let body = build_subscribe_request(&[ews_scope("rest-AAMk", "ews-AAE=")]);
    assert!(body.contains(r#"<t:FolderId Id="ews-AAE="/>"#), "{body}");
    assert!(!body.contains("rest-AAMk"), "{body}");
}

#[test]
fn subscribe_request_builder_emits_an_empty_list_only_for_no_translations() {
    let body = build_subscribe_request(&[]);
    assert!(body.contains("<t:FolderIds></t:FolderIds>"), "{body}");
}

#[test]
fn get_streaming_events_clamps_the_connection_timeout_to_the_ews_maximum() {
    assert!(
        build_get_streaming_events_request("sub-1", 120)
            .contains("<m:ConnectionTimeout>30</m:ConnectionTimeout>")
    );
    assert!(
        build_get_streaming_events_request("sub-1", 5)
            .contains("<m:ConnectionTimeout>5</m:ConnectionTimeout>")
    );
    assert!(
        build_get_streaming_events_request("a&b", 30)
            .contains("<t:SubscriptionId>a&amp;b</t:SubscriptionId>")
    );
}

#[test]
fn unsubscribe_request_names_the_subscription_and_escapes_it() {
    let body = build_unsubscribe_request("a&b");
    assert!(body.contains("<m:Unsubscribe>"), "{body}");
    assert!(
        body.contains("<m:SubscriptionId>a&amp;b</m:SubscriptionId>"),
        "{body}"
    );
}

#[test]
fn the_envelope_subscription_id_is_stamped_on_every_event_in_the_notification() {
    let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:PreviousWatermark>wm-0</t:PreviousWatermark>
              <t:MoreEvents>false</t:MoreEvents>
              <t:CreatedEvent>
                <t:Watermark>wm-1</t:Watermark>
                <t:ItemId Id="item-1"/>
                <t:ParentFolderId Id="folder-1"/>
              </t:CreatedEvent>
              <t:DeletedEvent>
                <t:Watermark>wm-2</t:Watermark>
                <t:ItemId Id="item-2"/>
                <t:ParentFolderId Id="folder-1"/>
              </t:DeletedEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

    let notifications = parse_streaming_notifications(xml).expect("parse should succeed");
    assert_eq!(notifications.len(), 2);
    for notification in &notifications {
        assert_eq!(notification.subscription_id.as_deref(), Some("sub-1"));
    }
    assert_eq!(notifications[0].event_type, EwsStreamingEventType::Created);
    assert_eq!(notifications[1].event_type, EwsStreamingEventType::Deleted);
}

#[test]
fn a_status_frame_without_a_parent_folder_parses_without_fabricating_one() {
    // The session maps a missing parent folder onto an account-wide
    // `Unknown` hint; the parser must report the absence honestly
    // rather than defaulting to an empty FolderId.
    let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:NewMailEvent>
                <t:Watermark>wm-9</t:Watermark>
              </t:NewMailEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

    let notifications = parse_streaming_notifications(xml).expect("parse should succeed");
    assert_eq!(notifications.len(), 1);
    assert!(notifications[0].parent_folder_id.is_none());
    assert!(notifications[0].item_id.is_none());
}

#[test]
fn unrecognized_event_elements_produce_no_notification() {
    // Forward compatibility: an EWS event type this build does not
    // know must be ignored, not turned into an untyped invalidation.
    let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:StatusEvent>
                <t:Watermark>wm-1</t:Watermark>
              </t:StatusEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

    assert!(
        parse_streaming_notifications(xml)
            .expect("parse should succeed")
            .is_empty()
    );
}

#[test]
fn subscribe_response_without_a_subscription_id_yields_no_subscription() {
    // `subscribe` turns the empty vec into `MalformedXml`; pin that the
    // parser does not invent an empty id.
    let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:SubscribeResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:SubscribeResponseMessage ResponseClass="Success">
          <m:SubscriptionId></m:SubscriptionId>
        </m:SubscribeResponseMessage>
      </m:ResponseMessages>
    </m:SubscribeResponse>
  </s:Body>
</s:Envelope>"#;
    assert!(
        parse_subscribe_response(xml)
            .expect("parse should succeed")
            .is_empty()
    );
}

#[test]
fn local_name_strips_any_namespace_prefix() {
    assert_eq!(local_name("t:ItemId"), "ItemId");
    assert_eq!(local_name("ItemId"), "ItemId");
    assert_eq!(local_name(""), "");
}

/// Both streaming parsers had no `GeneralRef` arm, so every reference -
/// even `&amp;` - vanished from a subscription id, and an unknown entity
/// or invalid character reference parsed on as if absent. References now
/// fold back in, and an unresolvable one is a malformed response.
#[test]
fn stream_parsers_resolve_references_and_refuse_unresolvable_ones() {
    let notification = |id: &str| {
        format!(
            r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:GetStreamingEventsResponse><m:Notifications><t:Notification>
<t:SubscriptionId>{id}</t:SubscriptionId>
<t:NewMailEvent><t:ItemId Id="i1"/><t:ParentFolderId Id="f1"/></t:NewMailEvent>
</t:Notification></m:Notifications></m:GetStreamingEventsResponse></s:Body></s:Envelope>"#
        )
    };
    let subscribe = |id: &str| {
        format!(
            r#"<s:Envelope xmlns:s="s" xmlns:m="m"><s:Body><m:SubscribeResponse>
<m:ResponseMessages><m:SubscribeResponseMessage ResponseClass="Success">
<m:SubscriptionId>{id}</m:SubscriptionId>
</m:SubscribeResponseMessage></m:ResponseMessages></m:SubscribeResponse></s:Body></s:Envelope>"#
        )
    };

    let parsed = parse_streaming_notifications(&notification("a&amp;b&#x43;"))
        .expect("well-formed references parse");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].subscription_id.as_deref(), Some("a&bC"));
    assert_eq!(
        parse_subscribe_response(&subscribe("a&amp;b&#x43;")).expect("parses"),
        vec!["a&bC".to_string()]
    );

    for bad in ["a&bogus;b", "a&#xD800;b"] {
        assert!(
            parse_streaming_notifications(&notification(bad)).is_err(),
            "notification {bad:?}"
        );
        assert!(
            parse_subscribe_response(&subscribe(bad)).is_err(),
            "subscribe {bad:?}"
        );
    }
}

/// Attribute values were read raw: an escaped item or folder id kept its
/// `&amp;` (so the invalidation named an id the server never meant), and
/// an unresolvable reference passed through as text. Both the Start and
/// the Empty spelling of the id elements unescape strictly now.
#[test]
fn stream_id_attributes_unescape_strictly() {
    let notification = |item: &str, folder: &str| {
        format!(
            r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:GetStreamingEventsResponse><m:Notifications><t:Notification>
<t:SubscriptionId>sub-1</t:SubscriptionId>
<t:NewMailEvent><t:ItemId Id="{item}" ChangeKey="c&amp;k"/>
<t:ParentFolderId Id="{folder}"></t:ParentFolderId></t:NewMailEvent>
</t:Notification></m:Notifications></m:GetStreamingEventsResponse></s:Body></s:Envelope>"#
        )
    };

    let parsed = parse_streaming_notifications(&notification("i&amp;1", "f&#x41;"))
        .expect("well-formed attributes parse");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].item_id.as_deref(), Some("i&1"));
    assert_eq!(parsed[0].item_change_key.as_deref(), Some("c&k"));
    assert_eq!(parsed[0].parent_folder_id.as_deref(), Some("fA"));

    for (item, folder) in [("i&bogus;", "f1"), ("i1", "f&#xD800;")] {
        assert!(
            parse_streaming_notifications(&notification(item, folder)).is_err(),
            "{item} / {folder}"
        );
    }
}

// ----- session tests over the scripted EWS transport -----

const NOTIFICATION_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:NewMailEvent>
                <t:Watermark>wm-1</t:Watermark>
                <t:ItemId Id="item-1" ChangeKey="ck-1"/>
                <t:ParentFolderId Id="folder-1" ChangeKey="fck-1"/>
              </t:NewMailEvent>
              <t:ModifiedEvent>
                <t:Watermark>wm-2</t:Watermark>
                <t:ItemId Id="item-2"/>
                <t:ParentFolderId Id="folder-1"/>
              </t:ModifiedEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

fn subscribe_response_xml(subscription_id: &str) -> String {
    format!(
        r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:SubscribeResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:SubscribeResponseMessage ResponseClass="Success">
          <m:SubscriptionId>{subscription_id}</m:SubscriptionId>
        </m:SubscribeResponseMessage>
      </m:ResponseMessages>
    </m:SubscribeResponse>
  </s:Body>
</s:Envelope>"#
    )
}

fn unsubscribe_response_xml() -> String {
    r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:UnsubscribeResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:UnsubscribeResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
        </m:UnsubscribeResponseMessage>
      </m:ResponseMessages>
    </m:UnsubscribeResponse>
  </s:Body>
</s:Envelope>"#
        .to_string()
}

enum ScriptStep {
    Delayed(std::sync::Arc<tokio::sync::Notify>, String),
    Respond(String),
    Fail(EwsError),
    /// A long poll with nothing to say: the request is reported to the
    /// test and the future then never resolves, exactly like a hanging
    /// `GetStreamingEvents`. The session leaves it via its `select!`
    /// (stop, shutdown or topology change).
    Hang,
}

/// In-process scripted EWS transport. Each `execute` reports its body
/// on the channel (which is how tests sequence deterministically,
/// without wall-clock waits) and then plays the next scripted step.
struct ScriptedEws {
    steps: tokio::sync::Mutex<VecDeque<ScriptStep>>,
    request_tx: tokio::sync::mpsc::UnboundedSender<String>,
}

fn scripted(steps: Vec<ScriptStep>) -> (ScriptedEws, tokio::sync::mpsc::UnboundedReceiver<String>) {
    let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel();
    (
        ScriptedEws {
            steps: tokio::sync::Mutex::new(steps.into()),
            request_tx,
        },
        request_rx,
    )
}

impl EwsExecute for ScriptedEws {
    async fn execute(
        &self,
        body_xml: &str,
        _headers: &EwsHeaders,
        _replay: bifrost_net::ReplayPolicy,
    ) -> Result<String, EwsError> {
        let step = self
            .steps
            .lock()
            .await
            .pop_front()
            .unwrap_or_else(|| panic!("EWS script exhausted by request: {body_xml}"));
        let _ = self.request_tx.send(body_xml.to_string());
        match step {
            ScriptStep::Respond(xml) => Ok(xml),
            ScriptStep::Delayed(ready, xml) => {
                ready.notified().await;
                Ok(xml)
            }
            ScriptStep::Fail(error) => Err(error),
            ScriptStep::Hang => std::future::pending().await,
        }
    }
}

/// Registers a handle's scopes the way `subscribe_ews` does: map write,
/// then topology bump.
async fn register(account: &GraphAccount, handle: &str, scopes: Vec<EwsSubscriptionScope>) {
    account.ews.registrations_for_tests().write().await.insert(
        SubscriptionHandle(handle.to_string()),
        EwsSubscriptionState { scopes },
    );
    account
        .ews
        .topology_for_tests()
        .send_modify(|generation| *generation = generation.wrapping_add(1));
}

/// The envelope net builds when a `Never` request meets a 503: one
/// attempt, no replay, a possible-commit ledger verdict.
fn unreplayed_unavailable() -> EwsError {
    EwsError::Transport(bifrost_net::Error::AttemptHistory {
        source: Box::new(bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(bifrost_net::FinalResponse {
                status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                headers: reqwest::header::HeaderMap::new(),
                body: bytes::Bytes::new(),
            }),
            retry_after_history: Vec::new(),
            stop_reason: bifrost_net::RetryStopReason::ReplayForbidden,
        }),
        evidence: bifrost_net::RequestEvidence::new(
            Vec::new(),
            bifrost_net::CommitEvidence::MayHaveCommitted,
        ),
    })
}

mod session;
