//! EWS streaming request, notification and byte-frame codecs.

use bifrost_types::DiagnosticText;
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::{BytesStart, Event};

use super::{EwsError, check_response_error, push_text, try_push_general_ref, xml_escape};

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
/// extracts it. Gap coverage is the full reconcile the engine owes at the
/// next session's `Established` after any lost, failed or reconfigured
/// session, not a resume token.
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
pub(crate) fn build_subscribe_request(
    folder_ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> String {
    let mut encoded_ids = String::new();
    for folder_id in folder_ids {
        encoded_ids.push_str(&format!(
            r#"<t:FolderId Id="{}"/>"#,
            xml_escape(folder_id.as_ref())
        ));
    }

    format!(
        r#"<m:Subscribe>
  <m:StreamingSubscriptionRequest>
    <t:FolderIds>{encoded_ids}</t:FolderIds>
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

/// EWS holds the outer SOAP document open for the duration of a streaming
/// request. Frame each complete response-message element as bytes arrive,
/// then parse that frame under a synthetic SOAP envelope. This handles a
/// notification element split across arbitrary transport chunks without
/// waiting for Exchange to close the thirty-minute response.
#[derive(Default)]
pub(crate) struct StreamingFrameDecoder {
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

// Local safety bound: a stalled or hostile response must not exhaust the mail client.
const MAX_FRAME_BYTES: usize = 1024 * 1024;

impl StreamingFrameDecoder {
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<Vec<String>, String> {
        let mut frames = Vec::new();
        // Bound temporary accumulation even when one transport chunk is huge.
        for part in chunk.chunks(MAX_FRAME_BYTES) {
            frames.extend(self.push_part(part)?);
        }
        Ok(frames)
    }

    fn push_part(&mut self, chunk: &[u8]) -> Result<Vec<String>, String> {
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
                if self.buffered.len() > MAX_FRAME_BYTES {
                    self.buffered.clear();
                    return Err("streaming EWS frame exceeded local byte limit".to_string());
                }
                break;
            };
            if end > MAX_FRAME_BYTES {
                self.buffered.clear();
                return Err("streaming EWS frame exceeded local byte limit".to_string());
            }
            let frame = self.buffered.drain(..end).collect::<Vec<_>>();
            let frame = String::from_utf8(frame)
                .map_err(|error| format!("streaming EWS frame was not UTF-8: {error}"))?;
            frames.push(frame);
        }
        Ok(frames)
    }

    pub(crate) fn has_partial_frame(&self) -> bool {
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
pub(crate) fn find_frame_start(bytes: &[u8]) -> Option<usize> {
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

/// One past the `>` of the matching `</[prefix:]...>` close tag.
pub(crate) fn find_frame_end(bytes: &[u8]) -> Option<usize> {
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
pub(crate) fn notifications_from_streaming_frame(
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

pub(crate) fn local_name(raw: &str) -> &str {
    raw.rsplit_once(':').map_or(raw, |(_, local)| local)
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
