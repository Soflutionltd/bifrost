use std::collections::HashSet;
use std::time::Duration;

use bifrost_types::{
    AccountOperation, CursorScope, DiagnosticText, HintPayload, InvalidationHint, PushSource,
    WatchEvent,
};
use futures::StreamExt;
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::{BytesStart, Event};
use tokio::sync::watch;

use crate::ews::{
    EwsBodyStream, EwsClient, EwsError, EwsExecute, EwsHeaders, check_response_error,
    try_push_general_ref, xml_escape,
};

use super::GraphAccount;
use super::graph_error::{GraphErrorContext, ews_error_to_account_error};
use super::push::EwsSubscriptionScope;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum EwsStreamingEventType {
    NewMail,
    Created,
    Deleted,
    Modified,
    Moved,
    Copied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EwsStreamingNotification {
    pub(crate) subscription_id: Option<String>,
    pub(crate) event_type: EwsStreamingEventType,
    pub(crate) item_id: Option<String>,
    pub(crate) item_change_key: Option<String>,
    pub(crate) parent_folder_id: Option<String>,
    pub(crate) parent_folder_change_key: Option<String>,
}

#[derive(Default)]
struct NotificationBuilder {
    subscription_id: Option<String>,
    event_type: Option<EwsStreamingEventType>,
    item_id: Option<String>,
    item_change_key: Option<String>,
    parent_folder_id: Option<String>,
    parent_folder_change_key: Option<String>,
}

#[derive(Debug)]
enum StreamLoopExit {
    /// The registered EWS scope set changed. Re-subscribe without reporting
    /// a transport disconnect: this is a local topology update, not a
    /// degraded push connection. The worker still emits `Reconnected` once
    /// the replacement subscription is live, because a change arriving in
    /// the handoff window - queued only on the abandoned subscription, or
    /// predating the new one - is delivered to neither, and `Reconnected`
    /// is the engine's full-reconcile trigger.
    Resubscribe,
    Disconnected,
    Shutdown,
    /// Terminal classification (auth lost, conditional-access, etc.)
    /// surfaced through the EWS worker. The worker exits and emits
    /// `WatchEvent::Terminated(error)` on the push channel.
    Terminated(bifrost_types::AccountError),
}

pub(crate) async fn run_streaming_worker(account: GraphAccount) {
    let Some(account_net) = account.client.account_net() else {
        // Unreachable (the factory attaches before any `GraphAccount`
        // exists), so it is the client's own invariant failing. Report it
        // on the push channel rather than only logging: a silent exit would
        // leave the push registration standing with no worker behind it and
        // no health event to say so.
        tracing::warn!("[Graph EWS] Streaming worker started with no attached transport");
        let error = crate::account::graph_error::ews_error_to_account_error(
            crate::ews::EwsError::Internal(
                "EWS streaming worker has no attached account transport".into(),
            ),
            crate::account::graph_error::GraphErrorContext::ews(
                bifrost_types::AccountOperation::PushSubscribe,
            ),
        );
        let _ = account.push_tx.send(WatchEvent::Terminated(error));
        return;
    };
    let ews = EwsClient::new(account_net, account.client.ews_url());
    run_worker(account, ews).await;
}

/// The worker loop behind `run_streaming_worker`, generic over the EWS
/// transport so the Subscribe / GetStreamingEvents / Unsubscribe cycle is
/// drivable by an in-process scripted transport in tests.
async fn run_worker<E: EwsExecute>(account: GraphAccount, ews: E) {
    let mut topology = account.ews_topology.subscribe();
    // A transport failure was reported as `Disconnected`; the next
    // successful Subscribe owes a `Reconnected`.
    let mut disconnected = false;
    let mut backoff = ReconnectBackoff::default();
    // A topology handoff abandoned a live subscription; the replacement owes
    // a `Reconnected` for the coverage gap, without any `Disconnected`
    // having been (correctly) emitted.
    let mut coverage_gap = false;

    loop {
        if account.shutdown.is_cancelled() {
            return;
        }
        // Mark the current topology generation seen BEFORE reading the scope
        // map. A bump landing after this point - even while the Subscribe
        // below is in flight - makes the next `changed()` fire, so no
        // registration can slip between the read and the wait.
        topology.borrow_and_update();
        // Read the registration map and, if it has emptied, retire the
        // worker slot WITHOUT releasing the guard in between. A worker is
        // spawned only after `subscribe_ews` has installed a scope, so an
        // empty union means the final handle was removed, not that work is
        // about to arrive - the worker exits. Doing that without clearing
        // the slot under the same guard is a race: a concurrent
        // `subscribe_ews` inserts, its `ensure_ews_worker` sees this
        // still-unfinished `JoinHandle`, declines to spawn, and then this
        // task returns - leaving the new subscription with no worker and
        // push silently dead. Holding the guard makes that insert wait
        // until the slot is empty. This mirrors the Graph webhook worker's
        // `retire_graph_worker_slot`; the shared rule lives in
        // `worker_slot`.
        let registrations = account.ews_subscriptions.read().await;
        let scopes =
            dedupe_by_ews_folder(registrations.values().flat_map(|state| state.scopes.iter()));
        if scopes.is_empty() {
            super::worker_slot::retire_worker_slot(&account.ews_worker).await;
            drop(registrations);
            return;
        }
        drop(registrations);

        match subscribe(&ews, &scopes).await {
            Ok(subscription_id) => {
                if disconnected || coverage_gap {
                    // The only event the engine turns into a full reconcile
                    // across every registered scope. It covers both the
                    // transport outage and the topology-handoff window, in
                    // which EWS delivered notifications to nobody.
                    let _ = account.push_tx.send(WatchEvent::Reconnected);
                    disconnected = false;
                    coverage_gap = false;
                }
                match run_get_events_loop(
                    &ews,
                    &account,
                    &mut topology,
                    &subscription_id,
                    &mut backoff,
                )
                .await
                {
                    // No backoff on this arm, deliberately. A server that
                    // repeatedly answers long polls with an explicit
                    // resubscribe directive will spin this loop, and that was
                    // examined and judged defensible rather than fixed: each
                    // cycle is a full server-directed round trip, not a
                    // client-side hot spin, and honouring an explicit server
                    // instruction promptly is the right response to it. The
                    // `Disconnected` arm below is where an unhealthy peer
                    // lands, and that one does back off. Flagged, not changed.
                    StreamLoopExit::Resubscribe => {
                        coverage_gap = true;
                        release_subscription(&ews, &subscription_id).await;
                    }
                    StreamLoopExit::Disconnected => {
                        disconnected = true;
                        release_subscription(&ews, &subscription_id).await;
                        let failures = backoff.record_failure();
                        wait_before_reconnect(&account, failures).await;
                    }
                    // Exchange holds streaming subscriptions against a
                    // per-mailbox quota and does not retire one because the
                    // client went away, so the terminal exits owe the same
                    // Unsubscribe the reconnect exits do. Leaking here is
                    // worse than leaking on a reconnect: nothing in this
                    // worker's lifetime will ever come back for it. `close()`
                    // joins rather than aborts precisely so the `Shutdown`
                    // release below gets to run.
                    StreamLoopExit::Terminated(error) => {
                        release_subscription(&ews, &subscription_id).await;
                        let _ = account.push_tx.send(WatchEvent::Terminated(error));
                        return;
                    }
                    StreamLoopExit::Shutdown => {
                        release_subscription(&ews, &subscription_id).await;
                        return;
                    }
                }
            }
            Err(error) => {
                let account_error = ews_error_to_account_error(
                    error,
                    GraphErrorContext::ews(AccountOperation::PushSubscribe),
                );
                // Only Retry belongs to this reconnect loop. Engine and Reconcile
                // must reach the sync reconciler through Terminated too.
                if !account_error.recovery().is_retryable() {
                    let _ = account.push_tx.send(WatchEvent::Terminated(account_error));
                    return;
                }
                let telemetry = account_error.telemetry_fields();
                tracing::warn!(
                    target: "bifrost_graph::ews",
                    message_key = telemetry.message_key,
                    recovery = telemetry.recovery_discriminant,
                    "EWS Subscribe failed"
                );
                if !disconnected {
                    let _ = account.push_tx.send(WatchEvent::Disconnected);
                    disconnected = true;
                }
                let failures = backoff.record_failure();
                wait_before_reconnect(&account, failures).await;
            }
        }
    }
}

/// Reconnect backoff state for one worker lifetime.
///
/// The counter is raised by every failure exit and cleared ONLY by
/// `record_healthy_read`, which `run_get_events_loop` calls where a
/// `GetStreamingEvents` long poll completed a full response. A successful
/// `Subscribe` is deliberately NOT the reset signal: a broken proxy or an
/// idle-timeout appliance can produce an endless subscribe-then-die loop in
/// which every Subscribe succeeds, and resetting there restores exactly the
/// hot reconnect spin the backoff exists to stop. Without any reset the
/// opposite failure appears - unrelated blips accumulate over hours of
/// healthy operation until every later outage waits the full cap.
#[derive(Default)]
struct ReconnectBackoff {
    failures: u32,
}

impl ReconnectBackoff {
    /// A long poll returned a complete response: the connection is proven
    /// healthy, so the next outage starts from the shortest delay again.
    fn record_healthy_read(&mut self) {
        self.failures = 0;
    }

    fn record_failure(&mut self) -> u32 {
        self.failures = self.failures.saturating_add(1);
        self.failures
    }
}

async fn wait_before_reconnect(account: &GraphAccount, failures: u32) {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let delay = reconnect_delay(failures, seed);
    tokio::select! {
        () = account.shutdown.cancelled() => {}
        () = tokio::time::sleep(delay) => {}
    }
}

fn reconnect_delay(failures: u32, jitter_seed: u64) -> Duration {
    let exponent = failures.saturating_sub(1).min(6);
    let base = 5_u64.saturating_mul(1_u64 << exponent);
    let jitter = jitter_seed % (base / 4 + 1);
    Duration::from_secs(base.saturating_add(jitter))
}

/// Best-effort EWS `Unsubscribe` for a streaming subscription the worker is
/// abandoning (reconnect or topology handoff). Dropping the hanging
/// `GetStreamingEvents` request does not retire the server-side
/// subscription: Exchange caps concurrent streaming subscriptions per
/// mailbox, so silently leaking one per scope change accumulates against
/// that quota until each abandoned subscription times out on its own.
/// Failure is logged, never surfaced - the replacement Subscribe is the
/// operation whose outcome matters, and after a transport failure this
/// request is expected to fail right along with it.
async fn release_subscription<E: EwsExecute>(ews: &E, subscription_id: &str) {
    let body = build_unsubscribe_request(subscription_id);
    if let Err(error) = ews.execute(&body, &EwsHeaders::default()).await {
        tracing::debug!(
            target: "bifrost_graph::ews",
            error = ?error,
            "EWS Unsubscribe for an abandoned streaming subscription failed"
        );
    }
}

pub(crate) fn parse_streaming_notifications(
    xml: &str,
) -> Result<Vec<EwsStreamingNotification>, String> {
    let mut reader = Reader::from_str(xml);
    let mut notifications = Vec::new();
    let mut current = NotificationBuilder::default();
    let mut in_event = false;
    let mut notification_subscription_id: Option<String> = None;
    let mut current_tag = String::new();
    let mut buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) => {
                let local = local_name(event.name().as_ref()).to_string();
                if local == "Notification" {
                    notification_subscription_id = None;
                }
                if let Some(event_type) = event_type_for(&local) {
                    in_event = true;
                    current = NotificationBuilder {
                        subscription_id: notification_subscription_id.clone(),
                        event_type: Some(event_type),
                        ..NotificationBuilder::default()
                    };
                }
                if in_event && local == "ItemId" {
                    current.item_id = attr(event, "Id")?;
                    current.item_change_key = attr(event, "ChangeKey")?;
                }
                if in_event && local == "ParentFolderId" {
                    current.parent_folder_id = attr(event, "Id")?;
                    current.parent_folder_change_key = attr(event, "ChangeKey")?;
                }
                current_tag = local;
                buf.clear();
            }
            Ok(Event::Empty(ref event)) => {
                let local = local_name(event.name().as_ref()).to_string();
                if in_event && local == "ItemId" {
                    current.item_id = attr(event, "Id")?;
                    current.item_change_key = attr(event, "ChangeKey")?;
                }
                if in_event && local == "ParentFolderId" {
                    current.parent_folder_id = attr(event, "Id")?;
                    current.parent_folder_change_key = attr(event, "ChangeKey")?;
                }
            }
            Ok(Event::Text(ref event)) => push_text(event, &mut buf)?,
            Ok(Event::GeneralRef(ref event)) => push_ref(event, &mut buf)?,
            Ok(Event::End(ref event)) => {
                let local = local_name(event.name().as_ref()).to_string();
                let trimmed = buf.trim();
                if in_event {
                    if current_tag == "SubscriptionId" {
                        current.subscription_id = non_empty(trimmed);
                    }
                } else if current_tag == "SubscriptionId" {
                    notification_subscription_id = non_empty(trimmed);
                }
                if event_type_for(&local).is_some() && in_event {
                    if let Some(notification) = finish_notification(&current) {
                        notifications.push(notification);
                    }
                    current = NotificationBuilder::default();
                    in_event = false;
                }
                current_tag.clear();
                buf.clear();
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("EWS streaming XML parse failed: {error}")),
            _ => {}
        }
    }

    Ok(notifications)
}

/// The subscription ids a `SubscribeResponse` names, in document order.
///
/// Streaming subscriptions carry no resume state: the `Watermark` a
/// Subscribe response may include belongs to the pull/push subscription
/// families and cannot be fed back into a `StreamingSubscriptionRequest`
/// (the schema admits only `FolderIds` and `EventTypes`), so nothing here
/// extracts it. Gap coverage is the worker's `Reconnected` emission, not a
/// resume token.
pub(crate) fn parse_subscribe_response(xml: &str) -> Result<Vec<String>, String> {
    let mut reader = Reader::from_str(xml);
    let mut current_tag = String::new();
    let mut buf = String::new();
    let mut current_subscription_id = None;
    let mut subscriptions = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) => {
                current_tag = local_name(event.name().as_ref()).to_string();
                buf.clear();
            }
            Ok(Event::Text(ref event)) => push_text(event, &mut buf)?,
            Ok(Event::GeneralRef(ref event)) => push_ref(event, &mut buf)?,
            Ok(Event::End(ref event)) => {
                let local = local_name(event.name().as_ref()).to_string();
                let trimmed = buf.trim();
                if current_tag == "SubscriptionId" {
                    current_subscription_id = non_empty(trimmed);
                }
                if (local == "StreamingSubscription" || local == "SubscribeResponseMessage")
                    && let Some(subscription_id) = current_subscription_id.take()
                {
                    subscriptions.push(subscription_id);
                }
                current_tag.clear();
                buf.clear();
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("EWS Subscribe XML parse failed: {error}")),
            _ => {}
        }
    }

    Ok(subscriptions)
}

/// The streaming Subscribe body. Deliberately watermark-free:
/// `StreamingSubscriptionRequest` admits only `FolderIds` and `EventTypes`,
/// so a `<t:Watermark>` here is a schema violation EWS rejects - once one
/// had been recorded, every resubscribe (reconnect or topology change)
/// failed instead of re-establishing push.
pub(crate) fn build_subscribe_request(scopes: &[EwsSubscriptionScope]) -> String {
    let mut folder_ids = String::new();
    for scope in scopes {
        folder_ids.push_str(&format!(
            r#"<t:FolderId Id="{}"/>"#,
            xml_escape(&scope.ews_folder_id)
        ));
    }

    format!(
        r#"<m:Subscribe>
  <m:StreamingSubscriptionRequest>
    <t:FolderIds>{folder_ids}</t:FolderIds>
    <t:EventTypes>
      <t:EventType>NewMailEvent</t:EventType>
      <t:EventType>CreatedEvent</t:EventType>
      <t:EventType>DeletedEvent</t:EventType>
      <t:EventType>ModifiedEvent</t:EventType>
      <t:EventType>MovedEvent</t:EventType>
      <t:EventType>CopiedEvent</t:EventType>
    </t:EventTypes>
  </m:StreamingSubscriptionRequest>
</m:Subscribe>"#
    )
}

pub(crate) fn build_get_streaming_events_request(
    subscription_id: &str,
    timeout_minutes: u32,
) -> String {
    format!(
        r#"<m:GetStreamingEvents>
  <m:SubscriptionIds>
    <t:SubscriptionId>{}</t:SubscriptionId>
  </m:SubscriptionIds>
  <m:ConnectionTimeout>{}</m:ConnectionTimeout>
</m:GetStreamingEvents>"#,
        xml_escape(subscription_id),
        timeout_minutes.min(30)
    )
}

/// The body that retires an abandoned streaming subscription. The single
/// `m:SubscriptionId` is not an `m:`-namespaced id COLLECTION, and EWS
/// answers Unsubscribe with exactly one `UnsubscribeResponseMessage`, so
/// this stays inside the single-answer invariant `build_soap_envelope`
/// enforces.
pub(crate) fn build_unsubscribe_request(subscription_id: &str) -> String {
    format!(
        r#"<m:Unsubscribe>
  <m:SubscriptionId>{}</m:SubscriptionId>
</m:Unsubscribe>"#,
        xml_escape(subscription_id)
    )
}

async fn subscribe<E: EwsExecute>(
    ews: &E,
    scopes: &[EwsSubscriptionScope],
) -> Result<String, EwsError> {
    let body = build_subscribe_request(scopes);
    let xml = ews.execute(&body, &EwsHeaders::default()).await?;
    let subscriptions = parse_subscribe_response(&xml)
        .map_err(|error| EwsError::MalformedXml(DiagnosticText::support_only(error)))?;
    subscriptions.into_iter().next().ok_or_else(|| {
        EwsError::MalformedXml(DiagnosticText::support_only(
            "EWS Subscribe returned no StreamingSubscription".to_string(),
        ))
    })
}

async fn run_get_events_loop<E: EwsExecute>(
    ews: &E,
    account: &GraphAccount,
    topology: &mut watch::Receiver<u64>,
    subscription_id: &str,
    backoff: &mut ReconnectBackoff,
) -> StreamLoopExit {
    // The subscription this loop polls is fixed for the lifetime of the
    // loop and lives nowhere else; the worker re-subscribes (minting a
    // fresh id) on reconnect or a local scope-topology change, retiring
    // this one with a best-effort Unsubscribe.
    loop {
        if account.shutdown.is_cancelled() {
            return StreamLoopExit::Shutdown;
        }
        let body = build_get_streaming_events_request(subscription_id, 30);
        // A streaming subscription covers the scope union that existed when
        // `subscribe` ran. Adding or removing a handle changes that union,
        // so do not leave the old subscription alive until its 30-minute
        // request expires. Dropping this in-flight request is intentional:
        // the outer worker retires the abandoned subscription and
        // immediately subscribes to the current scope set.
        let headers = EwsHeaders::default();
        let result = tokio::select! {
            () = account.shutdown.cancelled() => return StreamLoopExit::Shutdown,
            changed = topology.changed() => {
                return match changed {
                    Ok(()) => StreamLoopExit::Resubscribe,
                    // The sender lives on the account this worker holds a
                    // clone of; a closed channel means teardown.
                    Err(_) => StreamLoopExit::Shutdown,
                };
            }
            result = ews.execute_streaming(&body, &headers) => result,
        };
        match result {
            Ok(stream) => match consume_streaming_events(stream, account, topology).await {
                Ok(()) => {
                    // A long poll ran to a complete response. That, and not
                    // a bare successful Subscribe, is what proves the
                    // connection healthy, so it is the only reset point.
                    backoff.record_healthy_read();
                    continue;
                }
                Err(StreamLoopExit::Resubscribe) => return StreamLoopExit::Resubscribe,
                Err(StreamLoopExit::Shutdown) => return StreamLoopExit::Shutdown,
                Err(StreamLoopExit::Disconnected) => {
                    // A parse miss mid-long-poll is not necessarily a
                    // permanent contract violation: Microsoft interleaves
                    // keep-alive / status frames into the streaming
                    // response, and a single malformed chunk should
                    // reconnect (re-subscribe), not tear push down for
                    // good. The HTTP-error branch already reconnects;
                    // treat a transient parse failure the same way rather
                    // than terminating.
                    let account_error = ews_error_to_account_error(
                        EwsError::MalformedXml(DiagnosticText::support_only(
                            "EWS streaming response ended with an incomplete or invalid frame"
                                .to_string(),
                        )),
                        GraphErrorContext::ews(AccountOperation::PushStream),
                    );
                    let telemetry = account_error.telemetry_fields();
                    tracing::warn!(
                        target: "bifrost_graph::ews",
                        message_key = telemetry.message_key,
                        recovery = telemetry.recovery_discriminant,
                        "EWS GetStreamingEvents parse failed; reconnecting"
                    );
                    let _ = account.push_tx.send(WatchEvent::Disconnected);
                    return StreamLoopExit::Disconnected;
                }
                // A response error the frame consumer classified as
                // terminal (authorization lost, conditional access) exits
                // the worker; see `classify_frame_failure`.
                Err(StreamLoopExit::Terminated(error)) => {
                    return StreamLoopExit::Terminated(error);
                }
            },
            Err(error) => {
                let account_error = ews_error_to_account_error(
                    error,
                    GraphErrorContext::ews(AccountOperation::PushStream),
                );
                if !account_error.recovery().is_retryable() {
                    return StreamLoopExit::Terminated(account_error);
                }
                let telemetry = account_error.telemetry_fields();
                tracing::warn!(
                    target: "bifrost_graph::ews",
                    message_key = telemetry.message_key,
                    recovery = telemetry.recovery_discriminant,
                    "EWS GetStreamingEvents failed"
                );
                let _ = account.push_tx.send(WatchEvent::Disconnected);
                return StreamLoopExit::Disconnected;
            }
        }
    }
}

/// EWS holds the outer SOAP document open for the duration of a streaming
/// request. Frame each complete response-message element as bytes arrive,
/// then parse that frame under a synthetic SOAP envelope. This handles a
/// notification element split across arbitrary transport chunks without
/// waiting for Exchange to close the thirty-minute response.
struct StreamingFrameDecoder {
    buffered: Vec<u8>,
}

/// The local (namespace-free) name of the element each streaming frame is.
///
/// The framing deliberately keys on the LOCAL name. An XML namespace prefix
/// is an alias chosen by the writer, not part of the element's name: a
/// response that binds the messages namespace to `a:`, or makes it the
/// default namespace and writes the element unprefixed, is exactly as valid
/// as Microsoft's usual `m:`. Matching the serialized bytes `<m:...` instead
/// discarded every such response - `has_partial_frame` stayed false, so the
/// stream ended reporting neither notifications nor a parse failure, and push
/// went dead while `push_in_process()` still advertised it.
const FRAME_LOCAL_NAME: &[u8] = b"GetStreamingEventsResponseMessage";

/// How much of a namespace prefix the decoder is willing to reassemble
/// across a chunk boundary. Prefixes are unbounded in XML; real EWS uses one
/// or two characters.
const MAX_PREFIX_LEN: usize = 64;

impl StreamingFrameDecoder {
    /// Rescans the buffer from index 0 on every chunk. Assessed and accepted as
    /// a known bound, not a defect: every path below either drains the consumed
    /// prefix or truncates the buffer to a bounded tail, so the quadratic case
    /// needs a single frame larger than the whole transfer.
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<String>, String> {
        self.buffered.extend_from_slice(chunk);
        let mut frames = Vec::new();
        loop {
            let Some(start) = find_frame_start(&self.buffered) else {
                // Keep enough trailing bytes to recognize a tag split across
                // chunks (`<` + prefix + `:` + name + one boundary byte),
                // discarding only unneeded outer-envelope content.
                let keep = FRAME_LOCAL_NAME.len() + MAX_PREFIX_LEN + 3;
                if self.buffered.len() > keep {
                    self.buffered.drain(..self.buffered.len() - keep);
                }
                break;
            };
            if start > 0 {
                self.buffered.drain(..start);
            }
            let Some(end) = find_frame_end(&self.buffered) else {
                break;
            };
            let frame = self.buffered.drain(..end).collect::<Vec<_>>();
            let frame = String::from_utf8(frame)
                .map_err(|error| format!("streaming EWS frame was not UTF-8: {error}"))?;
            frames.push(frame);
        }
        Ok(frames)
    }

    fn has_partial_frame(&self) -> bool {
        find_frame_start(&self.buffered) == Some(0)
    }
}

/// A name character that may appear in a namespace prefix. Deliberately
/// permissive: the point is to bound the walk back to the `<`, not to
/// validate XML the parser will validate anyway.
fn is_prefix_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
}

/// Walk back from an occurrence of the local name over an optional
/// `prefix:` and the opening `<` (or `</`), returning the tag's start.
fn tag_start_before(bytes: &[u8], name_at: usize, closing: bool) -> Option<usize> {
    let mut index = name_at;
    if index > 0 && bytes[index - 1] == b':' {
        let colon = index - 1;
        index = colon;
        while index > 0 && is_prefix_char(bytes[index - 1]) {
            index -= 1;
        }
        if index == colon {
            // `<:Name` - an empty prefix is not a name.
            return None;
        }
    }
    if closing {
        (index >= 2 && bytes[index - 2] == b'<' && bytes[index - 1] == b'/').then(|| index - 2)
    } else {
        (index >= 1 && bytes[index - 1] == b'<').then(|| index - 1)
    }
}

/// Start of `<[prefix:]GetStreamingEventsResponseMessage` followed by a real
/// name boundary. A buffer that ends mid-name reports `None` so the caller
/// waits for the rest rather than mistaking a prefix of the name for a hit.
fn find_frame_start(bytes: &[u8]) -> Option<usize> {
    let mut from = 0;
    while let Some(offset) = find_bytes(&bytes[from..], FRAME_LOCAL_NAME) {
        let name_at = from + offset;
        let after = name_at + FRAME_LOCAL_NAME.len();
        let boundary = matches!(bytes.get(after), Some(byte) if byte.is_ascii_whitespace() || *byte == b'>' || *byte == b'/');
        if boundary && let Some(start) = tag_start_before(bytes, name_at, false) {
            return Some(start);
        }
        from = name_at + 1;
    }
    None
}

/// One past the `>` of the matching `</[prefix:]…>` close tag.
fn find_frame_end(bytes: &[u8]) -> Option<usize> {
    let mut from = 0;
    while let Some(offset) = find_bytes(&bytes[from..], FRAME_LOCAL_NAME) {
        let name_at = from + offset;
        let mut after = name_at + FRAME_LOCAL_NAME.len();
        if tag_start_before(bytes, name_at, true).is_some() {
            while matches!(bytes.get(after), Some(byte) if byte.is_ascii_whitespace()) {
                after += 1;
            }
            if bytes.get(after) == Some(&b'>') {
                return Some(after + 1);
            }
        }
        from = name_at + 1;
    }
    None
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Parse one framed response message, keeping the EWS classification.
///
/// The typed `EwsError` matters: `check_response_error` is what turns an
/// `ErrorAccessDenied` response into a TERMINAL classification. Flattening
/// it to a string here (and mapping every failure onto malformed XML at the
/// call site) turned a permanent authorization failure into an endless
/// disconnect-and-resubscribe loop.
fn notifications_from_streaming_frame(
    frame: &str,
) -> Result<Vec<EwsStreamingNotification>, EwsError> {
    // The synthetic envelope binds the conventional `m:`/`t:` prefixes so a
    // frame that used them still resolves; the frame may declare its own
    // prefixes, and neither this parser nor `check_response_error` is
    // namespace-resolving, so both simply read local names.
    let xml = format!(
        r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types"><soap:Body><m:GetStreamingEventsResponse><m:ResponseMessages>{frame}</m:ResponseMessages></m:GetStreamingEventsResponse></soap:Body></soap:Envelope>"#
    );
    check_response_error(&xml)?;
    parse_streaming_notifications(&xml)
        .map_err(|error| EwsError::MalformedXml(DiagnosticText::support_only(error)))
}

async fn consume_streaming_events(
    mut stream: EwsBodyStream,
    account: &GraphAccount,
    topology: &mut watch::Receiver<u64>,
) -> Result<(), StreamLoopExit> {
    let mut decoder = StreamingFrameDecoder {
        buffered: Vec::new(),
    };
    loop {
        let next = tokio::select! {
            () = account.shutdown.cancelled() => return Err(StreamLoopExit::Shutdown),
            changed = topology.changed() => {
                return Err(if changed.is_ok() {
                    StreamLoopExit::Resubscribe
                } else {
                    StreamLoopExit::Shutdown
                });
            }
            chunk = stream.next() => chunk,
        };
        let Some(chunk) = next else {
            return (!decoder.has_partial_frame())
                .then_some(())
                .ok_or(StreamLoopExit::Disconnected);
        };
        let chunk = chunk.map_err(|_| StreamLoopExit::Disconnected)?;
        let frames = decoder
            .push(&chunk)
            .map_err(|_| StreamLoopExit::Disconnected)?;
        for frame in frames {
            let notifications =
                notifications_from_streaming_frame(&frame).map_err(classify_frame_failure)?;
            emit_streaming_notifications(account, notifications).await;
        }
    }
}

/// Decide whether a failed frame ends the worker or just the connection.
///
/// A malformed or truncated frame is transient - Exchange interleaves
/// keep-alive and status frames into the streaming response, and a single
/// bad chunk should reconnect. A CLASSIFIED response error is not: an
/// `ErrorAccessDenied` inside the stream derives `NoPermission`, which
/// nothing in this worker can fix, so reconnecting forever would burn a
/// subscribe against the mailbox quota every few seconds and never report
/// the failure. All non-Retry classifications, including engine directives,
/// exit through `WatchEvent::Terminated` as the HTTP-error path does.
fn classify_frame_failure(error: EwsError) -> StreamLoopExit {
    let account_error =
        ews_error_to_account_error(error, GraphErrorContext::ews(AccountOperation::PushStream));
    classified_push_failure(account_error)
}

fn classified_push_failure(account_error: bifrost_types::AccountError) -> StreamLoopExit {
    if !account_error.recovery().is_retryable() {
        StreamLoopExit::Terminated(account_error)
    } else {
        StreamLoopExit::Disconnected
    }
}

async fn emit_streaming_notifications(
    account: &GraphAccount,
    notifications: Vec<EwsStreamingNotification>,
) {
    for notification in notifications {
        let payloads = match notification.parent_folder_id.as_deref() {
            Some(folder_id) => {
                let scopes = scopes_for_folder(account, folder_id).await;
                if scopes.is_empty() {
                    vec![HintPayload::Unknown]
                } else {
                    scopes
                        .into_iter()
                        .map(HintPayload::SpecificCursorScope)
                        .collect()
                }
            }
            None => vec![HintPayload::Unknown],
        };
        for payload in payloads {
            let _ = account.push_tx.send(WatchEvent::Invalidated {
                hint: InvalidationHint {
                    source: PushSource::EwsStreaming,
                    payload,
                },
            });
        }
    }
}

/// The union the worker subscribes to. The worker itself inlines this read
/// so it can retire its slot under the same guard (see the exit path in
/// `run_worker`); this wrapper exists for tests that only want the union.
#[cfg(test)]
async fn active_ews_scopes(account: &GraphAccount) -> Vec<EwsSubscriptionScope> {
    let states = account.ews_subscriptions.read().await;
    dedupe_by_ews_folder(states.values().flat_map(|state| state.scopes.iter()))
}

/// The union of every handle's registrations, one entry per EWS folder.
///
/// `build_subscribe_request` emits one `<t:FolderId>` per entry, and the
/// same folder can be registered more than once (two `push_subscribe`
/// calls over overlapping scopes, or two `FolderType` scopes differing
/// only in `ObjectType` - the translation request already collapses those
/// onto one `ewsId`). A repeated `FolderId` in a Subscribe body is
/// something EWS may accept, ignore, or reject the whole subscription
/// over; nothing here can find out, so the body simply never contains one.
/// Which registration survives is immaterial - only `ews_folder_id` is
/// read from it.
///
/// Pure over the scope registrations so the rule is pinnable without a
/// live client.
fn dedupe_by_ews_folder<'a>(
    scopes: impl Iterator<Item = &'a EwsSubscriptionScope>,
) -> Vec<EwsSubscriptionScope> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut deduped = Vec::new();
    for scope in scopes {
        if seen.insert(scope.ews_folder_id.as_str()) {
            deduped.push(scope.clone());
        }
    }
    deduped
}

async fn scopes_for_folder(account: &GraphAccount, folder_id: &str) -> Vec<CursorScope> {
    let states = account.ews_subscriptions.read().await;
    unique_scopes_for_folder(
        states.values().flat_map(|state| state.scopes.iter()),
        folder_id,
    )
}

/// Every DISTINCT cursor scope registered against `folder_id`, in
/// first-seen order.
///
/// Fan-out is per scope, not per registration: one EWS folder can back
/// several scopes (a `FolderType` per `ObjectType` over one container),
/// and each of those needs its own invalidation because each owns its own
/// delta cursor. The SAME scope reaching us twice is a different story -
/// two handles registering overlapping scopes, or one request naming a
/// scope twice - and every repeat costs the reconciler another full
/// `changes_stream` run over a scope it is already reconciling. Distinct
/// scopes are kept; equal ones collapse.
///
/// Pure over the scope registrations so the routing rule is pinnable
/// without a live client.
fn unique_scopes_for_folder<'a>(
    scopes: impl Iterator<Item = &'a EwsSubscriptionScope>,
    folder_id: &str,
) -> Vec<CursorScope> {
    let mut seen: HashSet<&CursorScope> = HashSet::new();
    let mut routed = Vec::new();
    for scope in scopes {
        if scope.ews_folder_id == folder_id && seen.insert(&scope.scope) {
            routed.push(scope.scope.clone());
        }
    }
    routed
}

fn finish_notification(builder: &NotificationBuilder) -> Option<EwsStreamingNotification> {
    Some(EwsStreamingNotification {
        subscription_id: builder.subscription_id.clone(),
        event_type: builder.event_type.clone()?,
        item_id: builder.item_id.clone(),
        item_change_key: builder.item_change_key.clone(),
        parent_folder_id: builder.parent_folder_id.clone(),
        parent_folder_change_key: builder.parent_folder_change_key.clone(),
    })
}

fn event_type_for(local: &str) -> Option<EwsStreamingEventType> {
    match local {
        "NewMailEvent" => Some(EwsStreamingEventType::NewMail),
        "CreatedEvent" => Some(EwsStreamingEventType::Created),
        "DeletedEvent" => Some(EwsStreamingEventType::Deleted),
        "ModifiedEvent" => Some(EwsStreamingEventType::Modified),
        "MovedEvent" => Some(EwsStreamingEventType::Moved),
        "CopiedEvent" => Some(EwsStreamingEventType::Copied),
        _ => None,
    }
}

/// A named attribute (matched on its local name), unescaped; `None` when
/// absent. Strict like the text readers: an attribute list that does not
/// parse, or a value whose reference does not resolve, is `Err`. The raw
/// value used to be returned, so an escaped id kept its `&amp;`, and a
/// malformed attribute was skipped as if absent.
fn attr(event: &BytesStart<'_>, name: &str) -> Result<Option<String>, String> {
    for attr in event.attributes() {
        let attr = attr.map_err(|error| format!("malformed EWS XML attribute: {error}"))?;
        if local_name(attr.key.as_ref()) == name {
            return unescape(&attr.value)
                .map(|value| Some(value.into_owned()))
                .map_err(|error| format!("malformed EWS XML attribute {name}: {error}"));
        }
    }
    Ok(None)
}

fn local_name(raw: &str) -> &str {
    raw.rsplit_once(':').map_or(raw, |(_, local)| local)
}

/// Append one unescaped text run; one that does not unescape is a malformed
/// response, never dropped.
fn push_text(event: &quick_xml::events::BytesText<'_>, buf: &mut String) -> Result<(), String> {
    let text =
        unescape(event.as_ref()).map_err(|error| format!("malformed EWS XML text: {error}"))?;
    buf.push_str(&text);
    Ok(())
}

/// Fold one entity or character reference back into the text run. quick-xml
/// emits every reference as its own event; these parsers once had no arm for
/// it, so even `&amp;` vanished from a value. An unknown entity or invalid
/// character reference is a malformed response.
fn push_ref(event: &quick_xml::events::BytesRef<'_>, buf: &mut String) -> Result<(), String> {
    try_push_general_ref(event, buf).map_err(|reason| format!("malformed EWS XML: {reason}"))
}

fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use bifrost_types::SubscriptionHandle;

    use super::super::PushMode;
    use super::super::push::EwsSubscriptionState;
    use super::*;

    #[test]
    fn engine_directives_leave_the_stream_worker() {
        use bifrost_types::{
            AccountErrorBuilder, AccountErrorKind, Cause, StateCause, SyncStateErrorKind,
        };
        let error = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
        )
        .operation(AccountOperation::PushStream)
        .protocol(bifrost_types::Protocol::Ews)
        .try_build()
        .expect("capability error");
        assert!(error.recovery().requires_engine_action());
        assert!(matches!(
            classified_push_failure(error),
            StreamLoopExit::Terminated(_)
        ));
    }

    #[test]
    fn frame_failures_only_reconnect_for_retry() {
        use crate::ews::SoapFaultCode;
        for (code, retry) in [
            (SoapFaultCode::ErrorStreamingSubscriptionInvalid, true),
            (SoapFaultCode::ErrorAccessDenied, false),
            (SoapFaultCode::ErrorNonExistentMailbox, false),
        ] {
            let result = classify_frame_failure(EwsError::SoapFault {
                code,
                detail: DiagnosticText::support_only("scripted fault"),
            });
            if retry {
                assert!(matches!(result, StreamLoopExit::Disconnected));
            } else {
                assert!(matches!(result, StreamLoopExit::Terminated(_)));
            }
        }
    }

    #[test]
    fn reconnect_backoff_grows_and_caps_with_bounded_jitter() {
        assert_eq!(reconnect_delay(1, 0), Duration::from_secs(5));
        assert_eq!(reconnect_delay(2, 0), Duration::from_secs(10));
        assert_eq!(reconnect_delay(8, 0), Duration::from_secs(320));
        assert_eq!(reconnect_delay(80, 0), Duration::from_secs(320));
        assert!(reconnect_delay(3, u64::MAX) <= Duration::from_secs(25));
    }

    /// The backoff must be cleared by a demonstrated healthy long-poll
    /// READ, not by a successful Subscribe. Without any reset, unrelated
    /// blips accumulate over a worker's whole lifetime until every later
    /// outage waits the full cap even after hours of healthy streaming.
    #[tokio::test]
    async fn a_healthy_long_poll_read_resets_the_reconnect_backoff() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        let good = r#"<m:GetStreamingEventsResponseMessage ResponseClass="Success"><m:Notifications><t:Notification><t:SubscriptionId>sub-1</t:SubscriptionId><t:NewMailEvent><t:ItemId Id="item-1"/><t:ParentFolderId Id="folder-1"/></t:NewMailEvent></t:Notification></m:Notifications></m:GetStreamingEventsResponseMessage>"#;
        let (scripted, _requests) = scripted(vec![
            ScriptStep::Respond(good.to_string()),
            ScriptStep::Respond(
                r#"<m:GetStreamingEventsResponseMessage ResponseClass="Success"><m:Notifications>"#
                    .to_string(),
            ),
        ]);
        let (_topology_tx, mut topology) = watch::channel(0_u64);
        let mut backoff = ReconnectBackoff { failures: 5 };
        let exit =
            run_get_events_loop(&scripted, &account, &mut topology, "sub-1", &mut backoff).await;
        assert!(
            matches!(exit, StreamLoopExit::Disconnected),
            "the truncated second poll disconnects"
        );
        assert_eq!(
            backoff.failures, 0,
            "the completed first long poll cleared the accumulated failures"
        );
    }
    use crate::client::GraphClient;

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
        let mut decoder = StreamingFrameDecoder {
            buffered: Vec::new(),
        };
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
            let mut decoder = StreamingFrameDecoder {
                buffered: Vec::new(),
            };
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
            let notifications =
                notifications_from_streaming_frame(&frames[0]).expect("frame parses");
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
        account.ews_subscriptions.write().await.insert(
            bifrost_types::SubscriptionHandle("first".to_string()),
            EwsSubscriptionState {
                scopes: vec![ews_scope("rest-a", "ews-shared")],
            },
        );
        account.ews_subscriptions.write().await.insert(
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
        // The worker maps a missing parent folder onto an account-wide
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

    // ----- worker-loop tests over the scripted EWS transport -----

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
        Respond(String),
        Fail(EwsError),
        /// A long poll with nothing to say: the request is reported to the
        /// test and the future then never resolves, exactly like a hanging
        /// `GetStreamingEvents`. The worker leaves it via its `select!`
        /// (shutdown or topology change).
        Hang,
    }

    /// In-process scripted EWS transport. Each `execute` reports its body
    /// on the channel (which is how tests sequence deterministically,
    /// without wall-clock waits) and then plays the next scripted step.
    struct ScriptedEws {
        steps: tokio::sync::Mutex<VecDeque<ScriptStep>>,
        request_tx: tokio::sync::mpsc::UnboundedSender<String>,
    }

    fn scripted(
        steps: Vec<ScriptStep>,
    ) -> (ScriptedEws, tokio::sync::mpsc::UnboundedReceiver<String>) {
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
        async fn execute(&self, body_xml: &str, _headers: &EwsHeaders) -> Result<String, EwsError> {
            let step = self
                .steps
                .lock()
                .await
                .pop_front()
                .unwrap_or_else(|| panic!("EWS script exhausted by request: {body_xml}"));
            let _ = self.request_tx.send(body_xml.to_string());
            match step {
                ScriptStep::Respond(xml) => Ok(xml),
                ScriptStep::Fail(error) => Err(error),
                ScriptStep::Hang => std::future::pending().await,
            }
        }
    }

    /// Registers a handle's scopes the way `subscribe_ews` does: map write,
    /// then topology bump.
    async fn register(account: &GraphAccount, handle: &str, scopes: Vec<EwsSubscriptionScope>) {
        account.ews_subscriptions.write().await.insert(
            SubscriptionHandle(handle.to_string()),
            EwsSubscriptionState { scopes },
        );
        account
            .ews_topology
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    #[tokio::test]
    async fn an_uncertain_subscribe_is_forwarded_for_reconciliation() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        let mut events = account.push_tx.subscribe();
        register(&account, "h1", vec![ews_scope("rest-a", "folder-1")]).await;
        let (scripted, mut requests) = scripted(vec![ScriptStep::Fail(EwsError::Transport(
            bifrost_net::Error::Network {
                message: "lost subscribe response".to_owned(),
                transmission_state: bifrost_types::TransmissionState::InFlight,
                source: None,
            },
        ))]);
        run_worker(account.clone(), scripted).await;
        assert!(requests.recv().await.is_some());
        match events.try_recv().expect("termination") {
            WatchEvent::Terminated(error) => assert!(error.recovery().requires_reconciliation()),
            other => panic!("expected reconciliation termination, got {other:?}"),
        }
    }

    /// The registration that starts the worker bumps the topology BEFORE
    /// the worker exists. That pre-recorded change must not read as a
    /// later topology update: the worker subscribes once and settles into
    /// its long poll, rather than immediately abandoning the first
    /// subscription and minting a redundant second one.
    #[tokio::test]
    async fn the_first_registration_subscribes_exactly_once() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        register(&account, "h1", vec![ews_scope("rest-a", "folder-1")]).await;
        let (scripted, mut requests) = scripted(vec![
            ScriptStep::Respond(subscribe_response_xml("sub-1")),
            ScriptStep::Hang,
            ScriptStep::Respond(unsubscribe_response_xml()),
        ]);
        let worker = tokio::spawn(run_worker(account.clone(), scripted));

        let subscribe_body = requests.recv().await.expect("subscribe request");
        assert!(subscribe_body.contains("<m:Subscribe>"), "{subscribe_body}");
        assert!(
            subscribe_body.contains(r#"<t:FolderId Id="folder-1"/>"#),
            "{subscribe_body}"
        );
        let poll_body = requests.recv().await.expect("long-poll request");
        assert!(
            poll_body.contains("<t:SubscriptionId>sub-1</t:SubscriptionId>"),
            "{poll_body}"
        );

        account.shutdown.cancel();
        worker.await.expect("worker joins");
        // Shutdown releases the subscription: Exchange holds streaming
        // subscriptions against a per-mailbox quota and does not retire one
        // because the client stopped polling.
        let unsubscribe = requests.recv().await.expect("unsubscribe on shutdown");
        assert!(
            unsubscribe.contains("<m:SubscriptionId>sub-1</m:SubscriptionId>"),
            "{unsubscribe}"
        );
        assert!(
            requests.try_recv().is_err(),
            "one Subscribe, one long poll, one release are the whole conversation"
        );
    }

    /// A `push_subscribe` while the stream is live: the worker abandons the
    /// long poll, retires the old subscription with an EWS Unsubscribe (the
    /// server holds streaming subscriptions against a per-mailbox quota),
    /// subscribes to the enlarged union without a watermark (the streaming
    /// schema has none), and emits exactly one `Reconnected` - covering the
    /// handoff window EWS delivered into nowhere - with no false
    /// `Disconnected`.
    #[tokio::test]
    async fn a_topology_change_hands_off_the_stream_without_a_false_disconnect() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        let mut events = account.push_tx.subscribe();
        register(&account, "h1", vec![ews_scope("rest-a", "folder-1")]).await;
        let (scripted, mut requests) = scripted(vec![
            ScriptStep::Respond(subscribe_response_xml("sub-1")),
            ScriptStep::Hang,
            ScriptStep::Respond(unsubscribe_response_xml()),
            ScriptStep::Respond(subscribe_response_xml("sub-2")),
            ScriptStep::Hang,
            ScriptStep::Respond(unsubscribe_response_xml()),
        ]);
        let worker = tokio::spawn(run_worker(account.clone(), scripted));

        let first_subscribe = requests.recv().await.expect("first subscribe");
        assert!(
            first_subscribe.contains(r#"<t:FolderId Id="folder-1"/>"#),
            "{first_subscribe}"
        );
        let _first_poll = requests.recv().await.expect("first long poll");

        // A second handle registers while the long poll hangs.
        register(&account, "h2", vec![ews_scope("rest-b", "folder-2")]).await;

        let unsubscribe = requests.recv().await.expect("unsubscribe of sub-1");
        assert!(unsubscribe.contains("<m:Unsubscribe>"), "{unsubscribe}");
        assert!(
            unsubscribe.contains("<m:SubscriptionId>sub-1</m:SubscriptionId>"),
            "{unsubscribe}"
        );

        let second_subscribe = requests.recv().await.expect("replacement subscribe");
        assert!(
            second_subscribe.contains(r#"<t:FolderId Id="folder-1"/>"#),
            "{second_subscribe}"
        );
        assert!(
            second_subscribe.contains(r#"<t:FolderId Id="folder-2"/>"#),
            "{second_subscribe}"
        );
        assert!(
            !second_subscribe.contains("Watermark"),
            "{second_subscribe}"
        );

        let second_poll = requests.recv().await.expect("second long poll");
        assert!(
            second_poll.contains("<t:SubscriptionId>sub-2</t:SubscriptionId>"),
            "{second_poll}"
        );

        // `Reconnected` was sent before the second poll's request went out,
        // so it is already buffered: the handoff gap gets its reconcile.
        let event = events.try_recv().expect("one push event");
        assert!(
            matches!(event, WatchEvent::Reconnected),
            "expected Reconnected, got {event:?}"
        );
        assert!(
            events.try_recv().is_err(),
            "a local topology change is not a transport disconnect"
        );

        account.shutdown.cancel();
        worker.await.expect("worker joins");
        let released = requests.recv().await.expect("unsubscribe on shutdown");
        assert!(
            released.contains("<m:SubscriptionId>sub-2</m:SubscriptionId>"),
            "{released}"
        );
    }

    /// End-to-end dispatch: a streamed notification for a subscribed folder
    /// comes back as `Invalidated` hints naming that folder's scope, and
    /// the loop keeps polling the same subscription.
    #[tokio::test]
    async fn a_streamed_notification_invalidates_the_folder_scope() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        let mut events = account.push_tx.subscribe();
        register(&account, "h1", vec![ews_scope("rest-a", "folder-1")]).await;
        let (scripted, mut requests) = scripted(vec![
            ScriptStep::Respond(subscribe_response_xml("sub-1")),
            ScriptStep::Respond(NOTIFICATION_XML.to_string()),
            ScriptStep::Hang,
            ScriptStep::Respond(unsubscribe_response_xml()),
        ]);
        let worker = tokio::spawn(run_worker(account.clone(), scripted));

        let _subscribe = requests.recv().await.expect("subscribe");
        let _first_poll = requests.recv().await.expect("first long poll");
        let _second_poll = requests
            .recv()
            .await
            .expect("re-poll after the notification");

        // The fixture carries two events on folder-1; each maps to the one
        // registered scope.
        for _ in 0..2 {
            let event = events.try_recv().expect("invalidation");
            match event {
                WatchEvent::Invalidated { hint } => {
                    assert!(matches!(hint.source, PushSource::EwsStreaming));
                    match hint.payload {
                        HintPayload::SpecificCursorScope(scope) => {
                            assert_eq!(scope, email_scope("rest-a"));
                        }
                        other => panic!("expected a specific scope hint, got {other:?}"),
                    }
                }
                other => panic!("expected Invalidated, got {other:?}"),
            }
        }
        assert!(events.try_recv().is_err());

        account.shutdown.cancel();
        worker.await.expect("worker joins");
    }

    /// The same dispatch, over a response that binds the EWS namespaces to
    /// prefixes other than `m:`/`t:`. The framing keys on local names, so
    /// this is an ordinary notification - not a silently discarded one.
    #[tokio::test]
    async fn a_notification_under_an_alien_namespace_prefix_still_invalidates() {
        const ALIEN_PREFIX_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<x:Envelope xmlns:x="http://schemas.xmlsoap.org/soap/envelope/">
  <x:Body>
    <a:GetStreamingEventsResponse xmlns:a="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:b="http://schemas.microsoft.com/exchange/services/2006/types">
      <a:ResponseMessages>
        <a:GetStreamingEventsResponseMessage ResponseClass="Success">
          <a:Notifications>
            <b:Notification>
              <b:SubscriptionId>sub-1</b:SubscriptionId>
              <b:NewMailEvent>
                <b:ItemId Id="item-1"/>
                <b:ParentFolderId Id="folder-1"/>
              </b:NewMailEvent>
            </b:Notification>
          </a:Notifications>
        </a:GetStreamingEventsResponseMessage>
      </a:ResponseMessages>
    </a:GetStreamingEventsResponse>
  </x:Body>
</x:Envelope>"#;

        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        let mut events = account.push_tx.subscribe();
        register(&account, "h1", vec![ews_scope("rest-a", "folder-1")]).await;
        let (scripted, mut requests) = scripted(vec![
            ScriptStep::Respond(subscribe_response_xml("sub-1")),
            ScriptStep::Respond(ALIEN_PREFIX_XML.to_string()),
            ScriptStep::Hang,
            ScriptStep::Respond(unsubscribe_response_xml()),
        ]);
        let worker = tokio::spawn(run_worker(account.clone(), scripted));

        let _subscribe = requests.recv().await.expect("subscribe");
        let _first_poll = requests.recv().await.expect("first long poll");
        let _second_poll = requests
            .recv()
            .await
            .expect("re-poll after the notification");

        match events.try_recv().expect("invalidation") {
            WatchEvent::Invalidated { hint } => match hint.payload {
                HintPayload::SpecificCursorScope(scope) => {
                    assert_eq!(scope, email_scope("rest-a"));
                }
                other => panic!("expected a specific scope hint, got {other:?}"),
            },
            other => panic!("expected Invalidated, got {other:?}"),
        }

        account.shutdown.cancel();
        worker.await.expect("worker joins");
    }

    /// A CLASSIFIED response error inside the stream is terminal, not a
    /// reconnect. `ErrorAccessDenied` derives `NoPermission`, which no
    /// amount of resubscribing fixes; flattening the typed `EwsError` into
    /// a string turned it into an endless disconnect-and-resubscribe loop
    /// that burned a subscription against the mailbox quota each time and
    /// never told the engine anything.
    #[tokio::test]
    async fn a_terminal_response_error_in_the_stream_terminates_instead_of_reconnecting() {
        const ACCESS_DENIED_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Error">
          <m:MessageText>Access is denied. Check credentials and try again.</m:MessageText>
          <m:ResponseCode>ErrorAccessDenied</m:ResponseCode>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        let mut events = account.push_tx.subscribe();
        register(&account, "h1", vec![ews_scope("rest-a", "folder-1")]).await;
        // Deliberately short: a reconnect loop would demand a second
        // Subscribe and blow up on the exhausted script instead of
        // quietly passing.
        let (scripted, mut requests) = scripted(vec![
            ScriptStep::Respond(subscribe_response_xml("sub-1")),
            ScriptStep::Respond(ACCESS_DENIED_XML.to_string()),
            ScriptStep::Respond(unsubscribe_response_xml()),
        ]);
        let worker = tokio::spawn(run_worker(account.clone(), scripted));

        let _subscribe = requests.recv().await.expect("subscribe");
        let _poll = requests.recv().await.expect("long poll");
        // The worker exits on its own: no shutdown, no topology change.
        worker.await.expect("worker joins");

        let release = requests.recv().await.expect("the subscription is released");
        assert!(
            release.contains("<m:SubscriptionId>sub-1</m:SubscriptionId>"),
            "{release}"
        );
        assert!(
            requests.try_recv().is_err(),
            "a terminal error must not resubscribe"
        );

        match events.try_recv().expect("a terminal push event") {
            WatchEvent::Terminated(error) => {
                assert!(error.recovery().is_terminal(), "{error:?}");
                assert_eq!(error.protocol(), Some(bifrost_types::Protocol::Ews));
            }
            other => panic!("expected Terminated, got {other:?}"),
        }
    }

    /// The worker retires its own slot while still holding the guard that
    /// told it the registration map is empty. Returning without clearing
    /// the slot let a concurrent `subscribe_ews` see a still-unfinished
    /// `JoinHandle`, decline to spawn a replacement, and end up with no EWS
    /// worker at all once this task returned.
    #[tokio::test]
    async fn the_worker_clears_its_slot_when_the_last_registration_is_gone() {
        let account =
            GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::EwsStreaming);
        // Stand in for the handle `ensure_ews_worker` installed: a task
        // that never finishes, so `is_finished` cannot paper over a slot
        // this exit failed to clear.
        *account.ews_worker.lock().await = Some(tokio::spawn(std::future::pending()));

        let (scripted, mut requests) = scripted(Vec::new());
        run_worker(account.clone(), scripted).await;

        assert!(
            account.ews_worker.lock().await.is_none(),
            "the exiting worker must leave an empty slot for the next subscribe"
        );
        assert!(
            requests.try_recv().is_err(),
            "an empty registration map subscribes to nothing"
        );

        // What the emptied slot buys: the next registration starts a fresh
        // worker rather than trusting the departed one.
        register(&account, "h1", vec![ews_scope("rest-a", "folder-1")]).await;
        super::super::push_stream::ensure_ews_worker(account.clone()).await;
        assert!(account.ews_worker.lock().await.is_some());
        account.shutdown.cancel();
    }
}
