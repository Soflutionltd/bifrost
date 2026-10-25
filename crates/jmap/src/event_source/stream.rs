#[cfg(feature = "calendars")]
use crate::event_source::CalendarAlert;
use crate::{
    DataType, PushObject,
    client::Client,
    core::{
        session::URLPart,
        transport::{HttpTransport, SseTransport},
    },
    event_source::{
        Changes, PushNotification,
        parser::{EventParser, EventType},
    },
};
use futures::{Stream, StreamExt};
use std::collections::HashMap;

/// Flattens one decoded state-lane payload into the notifications it
/// actually carries.
///
/// A push payload may be a `Group` whose `entries` are themselves push
/// objects, so a group's state changes are exactly as authoritative as a
/// top-level one - and dropping them is the precise loss the state-teardown
/// rule in `event_source` exists to prevent, minus the teardown that would
/// at least have replayed them. The WebSocket lane already recurses
/// through groups (`sync/push.rs::emit_push`); this is the same traversal,
/// written with an explicit worklist because the nesting depth is chosen by
/// the server. Entries are pushed in reverse so they pop in wire order.
///
/// One SSE block yields at most ONE `StateChange`, because the block has
/// exactly one resume token. Emitting a state change per nested entry gave
/// them all the same `id`, so a consumer that processed and checkpointed
/// the first one and then stopped resumed AFTER the whole block and lost
/// every entry it had not reached. Every state change in the block is
/// therefore MERGED into one notification carrying that single id, which
/// makes the token atomic by construction.
///
/// The merge is over the INNER maps: `changed` is per account and then per
/// data type, so `A -> {Email: e1}` followed by `A -> {Mailbox: m1}` must
/// yield `A -> {Email: e1, Mailbox: m1}`; replacing the account's map would
/// lose data. Later wins only for the same `(account, data type)` pair, and
/// "later" means later in depth-first traversal order - state strings are
/// opaque and are never compared to decide recency. Coalescing is expressly
/// permitted by RFC 8620 s7, and no `Group` guarantee requires a consumer to
/// observe intermediate states.
///
/// Ordering is the load-bearing half. Alerts are emitted FIRST, in their
/// wire-order relative sequence, and the merged state change LAST. An alert
/// carries no token of its own, so it cannot be lost by teardown alone - but
/// if the checkpoint-bearing state change came first and were checkpointed,
/// a disconnect before the following alert would skip that alert on resume.
/// Putting the token-bearing notification after everything else in the block
/// means interrupted processing can only replay, never skip.
///
/// An EMPTY `StateChange` present in the block still yields a notification:
/// its id can advance the consumer's checkpoint even though it names no
/// change.
///
/// A `CalendarAlert` reaching the state lane is forwarded rather than
/// dropped: the payload's own discriminator is its `@type` field, and a
/// server is free to carry an alert in a default-typed block or inside a
/// group instead of under `event: calendarAlert`.
///
/// `EmailPush` is forwarded as its own notification, for the same ordering
/// reason and one more. Dropping it was silent loss: when the block also
/// carries a state change, that state change commits the block's resume token
/// and the discarded object is never replayed. It cannot be folded into the
/// merged `StateChange` either - it carries no state string, a synthesised one
/// would be a lie about a checkpoint position, and an empty `Changes` means
/// "no changed type" rather than "reconcile everything". Like an alert it is
/// token-free, so it is emitted BEFORE the merged state change.
fn flatten_push_object(object: PushObject, id: Option<&str>) -> Vec<PushNotification> {
    let mut notifications = Vec::new();
    let mut merged: HashMap<String, HashMap<DataType, String>> = HashMap::new();
    let mut saw_state_change = false;
    let mut pending = vec![object];

    while let Some(object) = pending.pop() {
        match object {
            PushObject::StateChange { changed, .. } => {
                saw_state_change = true;
                for (account_id, types) in changed {
                    merged.entry(account_id).or_default().extend(types);
                }
            }
            #[cfg(feature = "calendars")]
            PushObject::CalendarAlert(alert) => {
                notifications.push(PushNotification::CalendarAlert(alert));
            }
            #[cfg(feature = "mail")]
            PushObject::EmailPush { account_id, email } => {
                notifications.push(PushNotification::EmailPush { account_id, email });
            }
            PushObject::Group { entries } => pending.extend(entries.into_iter().rev()),
            // Nothing is dropped here. With both `mail` and `calendars` on,
            // every `PushObject` variant already has a real arm above and this
            // one is genuinely unreachable, hence the lint allowance. It exists
            // only for feature-off builds, where the cfg'd arms above vanish and
            // the corresponding variants vanish with them, leaving the match
            // non-exhaustive without a catch-all. Deleting it breaks those
            // builds; reading it as silent loss is a misreading.
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }

    if saw_state_change {
        notifications.push(PushNotification::StateChange(Changes::new(
            id.map(str::to_owned),
            merged,
        )));
    }

    notifications
}

impl<T: HttpTransport + SseTransport> Client<T> {
    pub(crate) async fn event_source(
        &self,
        types: Option<impl IntoIterator<Item = DataType>>,
        close_after_state: bool,
        ping: Option<u32>,
        last_event_id: Option<&str>,
    ) -> crate::Result<impl Stream<Item = crate::Result<PushNotification>> + Unpin> {
        let state = self.session_state();
        let mut event_source_url = String::with_capacity(state.session().event_source_url().len());
        let types = types.map(|types| types.into_iter().collect::<Vec<_>>());
        if types.as_ref().is_some_and(Vec::is_empty) {
            return Err(crate::Error::InvalidUrl(
                "EventSource type list must contain at least one type name".to_string(),
            ));
        }

        for part in state.event_source_url()? {
            match part {
                URLPart::Value(value) => {
                    event_source_url.push_str(value);
                }
                URLPart::Parameter(param) => match param {
                    super::URLParameter::Types => {
                        if let Some(types) = types.as_ref() {
                            // Each name uses its own substitution position,
                            // while the commas separating names remain
                            // literal RFC 6570 expansion delimiters.
                            let expanded = types
                                .iter()
                                .map(|type_name| {
                                    let name = type_name.to_string();
                                    if event_source_url.contains('?')
                                        || event_source_url.contains('#')
                                    {
                                        crate::core::session::encode_template_value(&name)
                                            .to_string()
                                    } else {
                                        crate::core::session::encode_template_value_at(
                                            &name,
                                            &event_source_url,
                                        )
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(",");
                            event_source_url.push_str(&expanded);
                        } else {
                            event_source_url.push('*');
                        }
                    }
                    super::URLParameter::CloseAfter => {
                        event_source_url.push_str(if close_after_state { "state" } else { "no" });
                    }
                    super::URLParameter::Ping => {
                        if let Some(ping) = ping {
                            event_source_url.push_str(&ping.to_string());
                        } else {
                            event_source_url.push('0');
                        }
                    }
                },
            }
        }

        let mut stream = self
            .transport()
            .open_sse(&event_source_url, last_event_id)
            .await
            .map_err(crate::Error::from)?;

        let mut parser = EventParser::default();

        // SSE's design intent is skip-and-continue, but that is not one
        // uniform choice here: what a dropped event costs depends on what the
        // event carried, so two failures end the stream (`break 'events`) and
        // one does not.
        //
        // A parser error ends it, but NOT because the framing is
        // unrecoverable - it is recoverable, and the parser recovers it.
        // Every error site in `parser.rs` (oversized comment, oversized
        // field name, oversized field-plus-value, and the accumulated-data
        // cap on either field-commit path) consumes input, calls
        // `discard(..)` to run to the next blank line, and returns to
        // `Init`, so the block after the bad one parses normally.
        //
        // The stream ends anyway because of what that discard destroys.
        // `discard(..)` throws away a block that may ALREADY have
        // accumulated `data` - even an oversized comment can appear inside
        // a block carrying state - and the parser reports only "too long",
        // not whether the discarded block held a state change. Continuing
        // would hand the consumer the NEXT event with the next checkpoint,
        // letting it resume past a state change that was silently dropped:
        // the same loss the state-teardown rule below exists to prevent.
        // What cannot be established is the block's content, not the
        // framing.
        //
        // Exempting a "content-free" overflow - letting a pathological
        // comment line be reported without ending the stream - was tried
        // and rejected, so do not rebuild it. The verdict is not computable
        // where the error is raised: the cap trips mid-block, and the block
        // is not over. In
        //
        //     :<more than 1 MiB>
        //     data: {"@type":"StateChange", ...}
        //
        // no data has been seen when the comment trips the cap, so the
        // overflow looks content-free - and then `discard(..)` eats the
        // rest of the block, `data:` line included. The same holds for an
        // oversized `event:`/`id:`/unknown field name ahead of a valid
        // `data:` line, and for a block whose remainder arrives in a later
        // frame. Deferring the verdict to the terminating blank line is
        // strictly more machinery than the case is worth: `discard(..)`
        // also swallows a later `id:`, so "no data lost" is narrower than
        // "continuation preserves parser semantics" and the next event
        // would carry a stale resume token; the blank line may never
        // arrive, turning an immediately reportable error into an
        // indefinitely pending read that needs its own budget and EOF
        // policy; and the EOF branch below exits without asking the parser
        // whether an overflow is pending, so frame exhaustion would be read
        // as proof of a safely completed block. All of that buys exactly
        // one thing: surviving a comment line over 1 MiB, which no
        // interoperability requirement asks for.
        //
        // An undecodable `EventType::State` payload ends it because the
        // payload was authoritative. Skipping it silently loses the state
        // change it carried and leaves the client believing it is caught up;
        // the reconnect costs a round trip but replays that change from
        // `lastEventId`, so teardown is how the change is recovered rather
        // than a way of punishing the server.
        //
        // An undecodable `EventType::CalendarAlert` is skipped, and NOT
        // because anything later repairs it. An alert reports that a
        // notification FIRED - a transient delivery event - so re-reading
        // calendar object state does not reproduce it and the alert is simply
        // lost. Terminating cannot recover it either: the same build would run
        // the same deterministic decode over the same bytes, and this stream
        // has no retry counter, no quarantine and no fallback, so a reconnect
        // that replays the block tears down again. (RFC 8620 s7.3 describes
        // resending missed state changes, not a byte-for-byte event log, so a
        // server COULD regenerate the block differently - that is not a
        // mechanism anything can depend on.) Termination therefore recovers
        // nothing while costing the consumer every SUBSEQUENT notification
        // indefinitely, letting one non-authoritative payload livelock the
        // whole push lane, mail state changes included. So: this alert could
        // not be delivered; report that loss and continue, so it does not
        // indefinitely obstruct unrelated notifications. Later reconciliation
        // does not repair it. The error is yielded before the skip, so the
        // consumer is told the loss happened.
        //
        // `EventType::Unknown` is never decoded at all, so it cannot reach
        // either rule. See the variant's own documentation for why an
        // unrecognised event name is inert rather than authoritative.
        Ok(Box::pin(async_stream::stream! {
            'events: loop {
                for event_result in parser.by_ref() {
                    match event_result {
                        Ok(event) => match event.event {
                            EventType::State => {
                                match serde_json::from_slice::<PushObject>(&event.data) {
                                    Ok(object) => {
                                        let id = (!event.id.is_empty())
                                            .then(|| String::from_utf8_lossy(&event.id).into_owned());
                                        for notification in flatten_push_object(object, id.as_deref()) {
                                            yield Ok(notification);
                                        }
                                    }
                                    Err(err) => { yield Err(err.into()); break 'events; }
                                }
                            }
                            #[cfg(feature = "calendars")]
                            EventType::CalendarAlert => {
                                match serde_json::from_slice::<CalendarAlert>(&event.data) {
                                    Ok(alert) => { yield Ok(PushNotification::CalendarAlert(alert)); }
                                    Err(err) => { yield Err(err.into()); }
                                }
                            }
                            EventType::Ping | EventType::Unknown => {}
                        },
                        Err(err) => { yield Err(err); break 'events; }
                    }
                    continue;
                }
                if let Some(result) = stream.next().await {
                    match result {
                        Ok(bytes) => {
                            parser.push_bytes(bytes);
                            continue;
                        }
                        Err(err) => {
                            yield Err(crate::Error::from(err));
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use serde_json::json;
    use std::sync::Arc;

    use crate::core::{
        session::Session,
        transport::{HttpTransport, TransportError},
    };

    /// A transport that replays a canned list of SSE frames and refuses
    /// every other request. One frame per element, so a test can also pin
    /// how the parser behaves across frame boundaries.
    struct ScriptedTransport {
        frames: Vec<Vec<u8>>,
        opened_urls: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl ScriptedTransport {
        fn new(frames: Vec<Vec<u8>>) -> Self {
            Self {
                frames,
                opened_urls: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
    }

    impl HttpTransport for ScriptedTransport {
        async fn api_request(
            &self,
            _: &str,
            _: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test does not make API requests")
        }

        async fn upload(
            &self,
            _: &str,
            _: Vec<u8>,
            _: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test does not upload")
        }

        async fn download(&self, _: &str) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test does not download")
        }

        async fn get_session(&self, _: &str) -> Result<Bytes, TransportError> {
            unreachable!("the EventSource test receives a pre-fetched session")
        }
    }

    impl SseTransport for ScriptedTransport {
        type ByteStream = stream::Iter<std::vec::IntoIter<Result<Vec<u8>, TransportError>>>;

        async fn open_sse(
            &self,
            url: &str,
            _: Option<&str>,
        ) -> Result<Self::ByteStream, TransportError> {
            self.opened_urls
                .lock()
                .expect("opened URL lock")
                .push(url.to_string());
            Ok(stream::iter(
                self.frames
                    .iter()
                    .map(|frame| Ok(frame.clone()))
                    .collect::<Vec<_>>(),
            ))
        }
    }

    const STATE_CHANGE: &str = "data: {\"@type\": \"StateChange\", \"changed\": {}}\n\n";

    /// The client has to outlive the stream it opens - `event_source` borrows
    /// `&self` - so the two cannot come from one helper without a
    /// self-referential return. Each test binds the client, then the stream.
    fn client(script: &[&str]) -> Client<ScriptedTransport> {
        client_frames(script.iter().map(|f| f.as_bytes().to_vec()).collect())
    }

    fn client_frames(script: Vec<Vec<u8>>) -> Client<ScriptedTransport> {
        Client::with_transport(
            ScriptedTransport::new(script),
            session(),
            "https://example.org/.well-known/jmap",
        )
        .expect("client accepts the test session")
    }

    async fn events(
        client: &Client<ScriptedTransport>,
    ) -> impl Stream<Item = crate::Result<PushNotification>> + Unpin {
        client
            .event_source(None::<Vec<DataType>>, false, None, None)
            .await
            .expect("EventSource opens")
    }

    async fn expect_state_change(
        events: &mut (impl Stream<Item = crate::Result<PushNotification>> + Unpin),
    ) -> Changes {
        let notification = events
            .next()
            .await
            .expect("a state change")
            .expect("no decode error");
        // `PushNotification` has more than one variant only when `websockets`
        // is on; without it this pattern is irrefutable, which is fine here.
        #[allow(irrefutable_let_patterns)]
        let PushNotification::StateChange(changes) = notification else {
            panic!("expected a state change");
        };
        changes
    }

    fn session() -> Session {
        serde_json::from_value(json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {},
            "username": "u@example.org",
            "apiUrl": "https://example.org/jmap/",
            "downloadUrl": "https://example.org/dl/{accountId}/{blobId}/{name}?accept={type}",
            "uploadUrl": "https://example.org/ul/{accountId}/",
            "eventSourceUrl": "https://example.org/es/?types={types}&closeafter={closeafter}&ping={ping}",
            "state": "s0"
        }))
        .expect("test session decodes")
    }

    /// The session's `eventSourceUrl` is checked when EventSource is used,
    /// not when the client is built, so a server whose template this crate
    /// cannot use still opens for sync. The failure surfaces here, at use,
    /// as the server's malformed `eventSourceUrl` - before any stream opens.
    #[tokio::test]
    async fn a_malformed_event_source_template_is_refused_at_use() {
        let session: Session = serde_json::from_value(json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {},
            "username": "u@example.org",
            "apiUrl": "https://example.org/jmap/",
            "downloadUrl": "https://example.org/dl/{accountId}/{blobId}/{name}?accept={type}",
            "uploadUrl": "https://example.org/ul/{accountId}/",
            "eventSourceUrl": "es/?types={types}",
            "state": "s0"
        }))
        .expect("test session decodes");
        let client = Client::with_transport(
            ScriptedTransport::new(vec![STATE_CHANGE.as_bytes().to_vec()]),
            session,
            "https://example.org/.well-known/jmap",
        )
        .expect("an unusable eventSourceUrl does not stop the client building");

        match client
            .event_source(None::<Vec<DataType>>, false, None, None)
            .await
        {
            Err(crate::Error::MalformedSessionUrl { property, detail }) => {
                assert_eq!(property, "eventSourceUrl");
                assert!(detail.contains("es/?types={types}"), "{detail}");
            }
            Err(other) => panic!("expected MalformedSessionUrl, got {other:?}"),
            Ok(_) => panic!("a malformed eventSourceUrl must not open a stream"),
        }
    }

    #[tokio::test]
    async fn repeated_types_are_expanded_and_dot_path_values_are_encoded() {
        let transport = ScriptedTransport::new(Vec::new());
        let opened_urls = Arc::clone(&transport.opened_urls);
        let mut session = serde_json::to_value(session()).expect("session serializes");
        session["eventSourceUrl"] = json!(
            "https://example.org/es/{types}?types={types}&closeafter={closeafter}&ping={ping}"
        );
        let client = Client::with_transport(
            transport,
            serde_json::from_value(session).expect("session parses"),
            "https://example.org/.well-known/jmap",
        )
        .expect("client builds");

        let _events = client
            .event_source(
                Some(vec![
                    DataType::Other("..".to_string()),
                    DataType::Other("Email".to_string()),
                ]),
                false,
                None,
                None,
            )
            .await
            .expect("non-empty type list opens EventSource");

        assert_eq!(
            *opened_urls.lock().expect("opened URL lock"),
            vec!["https://example.org/es/%252E%252E,Email?types=..,Email&closeafter=no&ping=0"]
        );
    }

    #[tokio::test]
    async fn dot_only_type_is_encoded_in_a_path_substitution() {
        let transport = ScriptedTransport::new(Vec::new());
        let opened_urls = Arc::clone(&transport.opened_urls);
        let mut session = serde_json::to_value(session()).expect("session serializes");
        session["eventSourceUrl"] = json!("https://example.org/es/{types}");
        let client = Client::with_transport(
            transport,
            serde_json::from_value(session).expect("session parses"),
            "https://example.org/.well-known/jmap",
        )
        .expect("client builds");

        let _events = client
            .event_source(
                Some(vec![DataType::Other("..".to_string())]),
                false,
                None,
                None,
            )
            .await
            .expect("non-empty type list opens EventSource");

        let opened = opened_urls.lock().expect("opened URL lock");
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].as_str(), "https://example.org/es/%252E%252E");
        assert_eq!(
            reqwest::Url::parse(&opened[0]).unwrap().path(),
            "/es/%252E%252E"
        );
        assert_eq!(
            reqwest::Url::parse("https://example.org/es/..")
                .unwrap()
                .path(),
            "/"
        );
    }

    #[tokio::test]
    async fn an_empty_types_list_is_refused_before_opening_eventsource() {
        let transport = ScriptedTransport::new(Vec::new());
        let opened_urls = Arc::clone(&transport.opened_urls);
        let client =
            Client::with_transport(transport, session(), "https://example.org/.well-known/jmap")
                .expect("client builds");

        let error = client
            .event_source(Some(Vec::<DataType>::new()), false, None, None)
            .await
            .err()
            .expect("an empty type set has no valid RFC 8620 representation");

        assert!(matches!(error, crate::Error::InvalidUrl(_)));
        assert!(opened_urls.lock().expect("opened URL lock").is_empty());
    }

    #[tokio::test]
    async fn malformed_event_terminates_the_stream_without_reading_on() {
        let client = client(&["data: not JSON\n\n", STATE_CHANGE]);
        let mut events = events(&client).await;

        assert!(events.next().await.expect("a parser error").is_err());
        assert!(events.next().await.is_none());
    }

    // An undecodable alert is a delivery that was lost and cannot be
    // regenerated by anything downstream; terminating would not recover it
    // either, and would cost every subsequent notification. So it is reported
    // and skipped, and the state lane behind it survives.
    #[cfg(feature = "calendars")]
    #[tokio::test]
    async fn malformed_calendar_alert_is_reported_but_keeps_the_state_lane() {
        let client = client(&[
            "event: calendarAlert\ndata: not JSON\n\n",
            "id: c9\ndata: {\"@type\": \"StateChange\", \"changed\": {}}\n\n",
        ]);
        let mut events = events(&client).await;

        assert!(
            events
                .next()
                .await
                .expect("the alert decode error")
                .is_err(),
            "the consumer is still told the alert was undecodable"
        );

        assert_eq!(expect_state_change(&mut events).await.id(), Some("c9"));
        assert!(events.next().await.is_none());
    }

    // The same ruling, on the build where the alert type does not exist. With
    // `calendars` off there is nothing to decode an alert into, so the block
    // is dropped without being parsed - no error, and above all no teardown.
    // Routing the unknown name to `EventType::State` instead put exactly this
    // malformed alert through the strictest arm in the loop.
    #[cfg(not(feature = "calendars"))]
    #[tokio::test]
    async fn malformed_calendar_alert_keeps_the_state_lane_with_the_feature_off() {
        let client = client(&[
            "event: calendarAlert\ndata: not JSON\n\n",
            "id: c9\ndata: {\"@type\": \"StateChange\", \"changed\": {}}\n\n",
        ]);
        let mut events = events(&client).await;

        assert_eq!(expect_state_change(&mut events).await.id(), Some("c9"));
        assert!(events.next().await.is_none());
    }

    // An event name from a future revision of the protocol is the payload
    // this build understands least, so it must get the most forgiving
    // treatment, not the strictest: it is dropped undecoded and the state
    // lane behind it keeps running.
    #[tokio::test]
    async fn an_unknown_event_type_is_dropped_without_ending_the_stream() {
        let client = client(&[
            "event: somethingFromTheFuture\ndata: not JSON\n\n",
            "id: u1\ndata: {\"@type\": \"StateChange\", \"changed\": {}}\n\n",
        ]);
        let mut events = events(&client).await;

        assert_eq!(expect_state_change(&mut events).await.id(), Some("u1"));
        assert!(events.next().await.is_none());
    }

    // A `Group` payload's entries are state changes like any other, and
    // dropping them is the loss the state-teardown rule exists to prevent.
    // But the block has ONE resume token, so they merge into ONE
    // notification: a consumer that checkpointed a per-entry notification
    // and stopped would resume past the entries it never saw.
    //
    // The merge is over the inner per-type maps. Account `a` appears in two
    // separate nested state changes with different data types, and both
    // must survive.
    #[tokio::test]
    async fn a_group_payload_merges_every_state_change_into_one_notification() {
        let client = client(&[concat!(
            "id: g1\n",
            "data: {\"@type\": \"Group\", \"entries\": [",
            "{\"@type\": \"StateChange\", \"changed\": {\"a\": {\"Core\": \"e1\"}}},",
            "{\"@type\": \"Group\", \"entries\": [",
            "{\"@type\": \"StateChange\", \"changed\": {",
            "\"a\": {\"Principal\": \"m1\"}, \"b\": {\"Core\": \"e2\"}}}]}",
            "]}\n\n"
        )]);
        let mut events = events(&client).await;

        let changes = expect_state_change(&mut events).await;
        assert_eq!(changes.id(), Some("g1"));

        let mut accounts = changes
            .changed_accounts()
            .map(String::as_str)
            .collect::<Vec<_>>();
        accounts.sort_unstable();
        assert_eq!(accounts, vec!["a", "b"]);

        let mut a = changes
            .changes("a")
            .expect("account a is present")
            .map(|(type_, state)| (type_.to_string(), state.clone()))
            .collect::<Vec<_>>();
        a.sort_unstable();
        assert_eq!(
            a,
            vec![
                ("Core".to_string(), "e1".to_string()),
                ("Principal".to_string(), "m1".to_string())
            ],
            "replacing the account's inner map would have lost one of these"
        );

        assert!(
            events.next().await.is_none(),
            "one SSE block yields at most one state change"
        );
    }

    // Last in depth-first traversal order wins for the SAME (account, type)
    // pair. State strings are opaque, so recency is positional, never a
    // comparison of the strings themselves.
    #[tokio::test]
    async fn the_last_entry_in_traversal_order_wins_per_account_and_type() {
        let client = client(&[concat!(
            "id: g2\n",
            "data: {\"@type\": \"Group\", \"entries\": [",
            "{\"@type\": \"StateChange\", \"changed\": {\"a\": {\"Core\": \"zzz\"}}},",
            "{\"@type\": \"StateChange\", \"changed\": {\"a\": {\"Core\": \"aaa\"}}}",
            "]}\n\n"
        )]);
        let mut events = events(&client).await;

        let changes = expect_state_change(&mut events).await;
        assert_eq!(
            changes
                .changes("a")
                .expect("account a is present")
                .map(|(_, state)| state.as_str())
                .collect::<Vec<_>>(),
            vec!["aaa"]
        );
    }

    // A group carrying only an EMPTY state change still yields one: the
    // block's id can advance the consumer's checkpoint even when it names
    // no changed type.
    #[tokio::test]
    async fn an_empty_nested_state_change_is_still_yielded() {
        let client = client(&[concat!(
            "id: g3\n",
            "data: {\"@type\": \"Group\", \"entries\": [",
            "{\"@type\": \"StateChange\", \"changed\": {}}",
            "]}\n\n"
        )]);
        let mut events = events(&client).await;

        assert_eq!(expect_state_change(&mut events).await.id(), Some("g3"));
    }

    // Ordering inside one block is load-bearing. The state change is declared
    // FIRST on the wire here and the alert second, and the emitted order is
    // nonetheless alert-then-state-change - so this fixture demonstrates an
    // actual reordering, not a pass-through. Only the state change carries the
    // block's resume token, so anything emitted after it could be skipped by a
    // consumer that checkpointed and then disconnected. Everything token-free
    // goes first, so interrupted processing replays rather than skips.
    #[cfg(feature = "calendars")]
    #[tokio::test]
    async fn a_group_emits_its_alerts_before_the_merged_state_change() {
        let client = client(&[concat!(
            "id: g4\n",
            "data: {\"@type\": \"Group\", \"entries\": [",
            "{\"@type\": \"StateChange\", \"changed\": {\"a\": {\"Core\": \"e1\"}}},",
            "{\"@type\": \"CalendarAlert\", \"accountId\": \"a\",",
            " \"calendarEventId\": \"ev1\", \"uid\": \"u1\", \"recurrenceId\": null,",
            " \"alertId\": \"al1\"}",
            "]}\n\n"
        )]);
        let mut events = events(&client).await;

        let first = events
            .next()
            .await
            .expect("a notification")
            .expect("no decode error");
        assert!(
            matches!(first, PushNotification::CalendarAlert(_)),
            "the token-free alert must precede the checkpoint-bearing state change"
        );

        assert_eq!(expect_state_change(&mut events).await.id(), Some("g4"));
        assert!(events.next().await.is_none());
    }

    // An `EmailPush` sharing a block with a state change is the case where
    // dropping it was silent loss: the state change commits the block's resume
    // token, so the discarded object would never be replayed. It must be
    // yielded, and - carrying no token of its own - it must precede the
    // token-bearing state change.
    #[cfg(feature = "mail")]
    #[tokio::test]
    async fn an_email_push_is_yielded_before_the_token_bearing_state_change() {
        let client = client(&[concat!(
            "id: g5\n",
            "data: {\"@type\": \"Group\", \"entries\": [",
            "{\"@type\": \"StateChange\", \"changed\": {\"a\": {\"Core\": \"e1\"}}},",
            "{\"@type\": \"EmailPush\", \"accountId\": \"a\",",
            " \"email\": {\"id\": \"m1\"}}",
            "]}\n\n"
        )]);
        let mut events = events(&client).await;

        let first = events
            .next()
            .await
            .expect("a notification")
            .expect("no decode error");
        let PushNotification::EmailPush { account_id, email } = first else {
            panic!("the email push must not be discarded");
        };
        assert_eq!(account_id, "a");
        assert_eq!(email, json!({"id": "m1"}));

        assert_eq!(expect_state_change(&mut events).await.id(), Some("g5"));
        assert!(events.next().await.is_none());
    }

    // The parser DOES resynchronise its framing after an oversized block -
    // `an_unbounded_comment_errors_once_and_resynchronises` pins that - so
    // the stream ending here is not a framing verdict. It ends because the
    // discarded block may have carried state the parser cannot report on,
    // and delivering the next event's checkpoint would let the consumer
    // resume past it.
    #[tokio::test]
    async fn a_recoverable_parser_error_still_ends_the_stream() {
        // Derived from the parser's own bound, never restated: a hardcoded
        // literal would still compile and still pass if the cap moved, while
        // no longer describing an overflow at all.
        let mut frame = Vec::from(":");
        frame.extend_from_slice(&vec![b'z'; crate::event_source::parser::MAX_EVENT_SIZE + 1]);
        frame.extend_from_slice(
            b"\n\nid: after\ndata: {\"@type\": \"StateChange\", \"changed\": {}}\n\n",
        );

        let client = client_frames(vec![frame]);
        let mut events = events(&client).await;

        assert!(events.next().await.expect("a parser error").is_err());
        assert!(
            events.next().await.is_none(),
            "the resynchronised event behind the error must not be delivered"
        );
    }

    // Comment lines are how servers keep a long-lived EventSource warm.
    // They must not surface as events, and they must not tear the stream
    // down; the resume token from the last `id` field carries across the
    // events that follow it.
    #[tokio::test]
    async fn comment_heartbeats_do_not_disturb_the_stream() {
        let client = client(&[
            ": keepalive\n\n",
            "id: c1\ndata: {\"@type\": \"StateChange\", \"changed\": {}}\n\n",
            ": keepalive\n\n",
            STATE_CHANGE,
        ]);
        let mut events = events(&client).await;

        for expected in ["c1", "c1"] {
            assert_eq!(expect_state_change(&mut events).await.id(), Some(expected));
        }
        assert!(events.next().await.is_none());
    }
}
