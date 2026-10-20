use quick_xml::Reader;
use quick_xml::escape::{resolve_xml_entity, unescape};
use quick_xml::events::{BytesRef, BytesText, Event};

// SOAP envelope.

pub(crate) fn build_soap_envelope(body_xml: &str) -> String {
    // Single-result callers use the whole-response error scan. Keep them
    // fenced from multi-id requests: those require the separate GetItem
    // funnel, which reconciles one response message per submitted id.
    debug_assert!(
        per_answer_request_ids(body_xml) <= 1,
        "EWS request names more than one per-answer id; the response-error \
         scan and the operation parsers are whole-response and would report \
         one item's outcome for all of them"
    );
    soap_envelope(body_xml)
}

/// Multi-item GetItem has its own per-response parser; other callers retain
/// the single-answer guard above.
pub(super) fn get_items_envelope(body_xml: &str) -> String {
    soap_envelope(body_xml)
}

fn soap_envelope(body_xml: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types"
               xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
  <soap:Header>
    <t:RequestServerVersion Version="Exchange2016"/>
  </soap:Header>
  <soap:Body>
    {body_xml}
  </soap:Body>
</soap:Envelope>"#
    )
}

/// How many ids a request body names on a surface EWS answers PER ID.
///
/// EWS emits one `ResponseMessage` per entry of an `m:`-namespaced id
/// collection (`m:ItemIds`, `m:FolderIds`, `m:ParentFolderIds`,
/// `m:AttachmentIds`, `m:SubscriptionIds`), so a body naming N ids gets N
/// independent verdicts back. This crate's response handling is
/// whole-response on the single-result funnel - see `build_soap_envelope` -
/// so its count must stay at most one. The typed multi-item GetItem funnel
/// has an independent response-message parser.
///
/// The subscription folder set is deliberately NOT counted. Subscribe
/// carries its folders in `t:FolderIds` inside
/// `m:StreamingSubscriptionRequest`: that is the subscription's scope, not
/// a request-item list, and EWS answers it with exactly one
/// `SubscribeResponseMessage` however many folders it names. The namespace
/// prefix is precisely what separates the two cases, which is why this
/// matches on the prefixed name rather than the local one. The builders in
/// this crate hardcode the `m:` / `t:` prefixes declared by
/// `build_soap_envelope`, so the prefix is authoritative here.
pub(crate) fn per_answer_request_ids(body_xml: &str) -> usize {
    let mut reader = Reader::from_str(body_xml);
    let mut ids = 0usize;
    let mut depth = 0usize;
    // Depth of the enclosing per-answer id collection, if we are inside one.
    let mut collection_depth: Option<usize> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().as_ref().to_owned();
                if collection_depth.is_some_and(|start| depth == start + 1) {
                    ids += 1;
                } else if collection_depth.is_none() && is_per_answer_id_collection(&name) {
                    collection_depth = Some(depth);
                }
                depth += 1;
            }
            Ok(Event::Empty(_)) => {
                // A self-closing collection element has no children, so only
                // a child of an open collection counts.
                if collection_depth.is_some_and(|start| depth == start + 1) {
                    ids += 1;
                }
            }
            Ok(Event::End(_)) => {
                depth = depth.saturating_sub(1);
                if collection_depth == Some(depth) {
                    collection_depth = None;
                }
            }
            Ok(Event::Eof) => break,
            // A body this crate cannot re-read is not a body whose id count
            // we can vouch for; report zero rather than a wrong number, and
            // let the request fail on the wire where it is classifiable.
            Err(_) => return 0,
            _ => {}
        }
    }

    ids
}

fn is_per_answer_id_collection(name: &str) -> bool {
    matches!(
        name,
        "m:ItemIds" | "m:FolderIds" | "m:ParentFolderIds" | "m:AttachmentIds" | "m:SubscriptionIds"
    )
}

/// Strip namespace prefixes from element names for easier matching.
/// e.g. "t:FolderId" -> "FolderId", "soap:Fault" -> "Fault"
pub(crate) fn strip_ns(name: &str) -> &str {
    match name.find(':') {
        Some(i) => &name[i + 1..],
        None => name,
    }
}

/// Escape the five XML metacharacters for embedding a value inside a
/// SOAP request body. Copied verbatim from the EWS request builders.
pub(crate) fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Well-known distinguished folder IDs that EWS treats specially.
/// `publicfoldersroot` is the entry point for the public-folder
/// hierarchy; any other (opaque base64) id is an ordinary `FolderId`.
pub(crate) fn is_distinguished_folder_id(id: &str) -> bool {
    matches!(
        id,
        "publicfoldersroot"
            | "inbox"
            | "drafts"
            | "sentitems"
            | "deleteditems"
            | "junkemail"
            | "outbox"
            | "calendar"
            | "contacts"
            | "tasks"
            | "notes"
            | "root"
            | "msgfolderroot"
    )
}

/// Read a named attribute off a start/empty tag, unescaped, returning the
/// empty string when absent.
///
/// Strict like the text readers: an attribute list that does not parse, or
/// a value whose entity or character reference does not resolve, is `Err`
/// with the reason. The raw value used to be returned as-is, so `&amp;` in
/// an id stayed escaped and a malformed attribute was skipped as if absent.
pub(crate) fn extract_attribute(
    e: &quick_xml::events::BytesStart<'_>,
    attr_name: &str,
) -> Result<String, String> {
    for attr in e.attributes() {
        let attr = attr.map_err(|error| format!("malformed XML attribute: {error}"))?;
        if attr.key.as_ref() == attr_name {
            return unescape(&attr.value)
                .map(std::borrow::Cow::into_owned)
                .map_err(|error| format!("malformed XML attribute {attr_name}: {error}"));
        }
    }
    Ok(String::new())
}

// SOAP fault check.

pub(crate) fn check_soap_fault(xml: &str) -> Result<(), super::EwsError> {
    use super::SoapFaultCode;
    use bifrost_types::DiagnosticText;

    let mut reader = Reader::from_str(xml);
    let mut in_fault = false;
    let mut in_faultstring = false;
    let mut in_faultcode = false;
    let mut fault_message = String::new();
    let mut fault_code_raw = String::new();
    let mut buf = String::new();
    let mut xml_error: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                if local == "Fault" {
                    in_fault = true;
                }
                if in_fault && local == "faultstring" {
                    in_faultstring = true;
                }
                if in_fault && local == "faultcode" {
                    in_faultcode = true;
                }
                buf.clear();
            }
            Ok(Event::Text(ref e)) => {
                if let Err(error) = push_text(e, &mut buf) {
                    xml_error = Some(error);
                    break;
                }
            }
            Ok(Event::GeneralRef(ref e)) => {
                if let Err(error) = try_push_general_ref(e, &mut buf) {
                    xml_error = Some(error);
                    break;
                }
            }
            Ok(Event::End(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                if in_faultstring && local == "faultstring" {
                    fault_message = buf.trim().to_string();
                    in_faultstring = false;
                }
                if in_faultcode && local == "faultcode" {
                    fault_code_raw = buf.trim().to_string();
                    in_faultcode = false;
                }
                if local == "Fault" {
                    break;
                }
                buf.clear();
            }
            Ok(Event::Eof) => break,
            Err(error) => {
                xml_error = Some(error.to_string());
                break;
            }
            _ => {}
        }
    }

    if let Some(error) = xml_error {
        return Err(super::EwsError::MalformedXml(DiagnosticText::support_only(
            format!("EWS SOAP fault parse failed: {error}"),
        )));
    }

    if in_fault || !fault_message.is_empty() || !fault_code_raw.is_empty() {
        let code = if fault_code_raw.is_empty() {
            SoapFaultCode::Unknown
        } else {
            SoapFaultCode::parse(&fault_code_raw)
        };
        let detail_text = if fault_message.is_empty() {
            "Unknown SOAP fault".to_string()
        } else {
            fault_message
        };
        return Err(super::EwsError::SoapFault {
            code,
            detail: DiagnosticText::support_only(detail_text),
        });
    }

    Ok(())
}

/// Inspect a 200-OK EWS response body for an application-level error
/// carried in the canonical `ResponseClass="Error"` / `<m:ResponseCode>`
/// shape (distinct from a SOAP `<Fault>`, which `check_soap_fault`
/// handles). EWS reports the overwhelming majority of operation failures
/// (`ErrorAccessDenied`, `ErrorServerBusy`, `ErrorItemNotFound`, and so
/// on) this way inside an HTTP 200, not as a SOAP fault. Without this the
/// failed response parses to an empty success: a revoked or throttled
/// public folder would read as "zero items" and the deletion reconcile
/// would emit a spurious mass-`Destroyed` while the cursor advanced.
///
/// The first `ResponseMessage` that is `ResponseClass="Error"` AND names a
/// fault wins; its `<m:ResponseCode>` token is mapped through
/// `SoapFaultCode::parse` (the same `ErrorXxx` vocabulary) and the optional
/// `<m:MessageText>` becomes the support-only detail. A
/// `ResponseClass="Warning"` is not an error and is ignored, as is any
/// message that is not error-classed.
///
/// An error-classed message that closes without a `ResponseCode`, or that
/// carries the self-contradictory `NoError`, is unclassifiable but still a
/// FAILURE: it reports as `MalformedXml`, never success. Dropping it would
/// hand a failed response to the operation parsers, several of which project
/// an absent result set as an empty successful one - public folders or items
/// would silently disappear. The scan state is per-message, so a later
/// warning or success cannot donate its code to such a message; a later
/// CLASSIFIABLE error outranks it, because a real code carries the real
/// classification (`ErrorAccessDenied` quarantines just that scope) where
/// `Protocol(ParseFailed)` throws it away.
pub(crate) fn check_response_error(xml: &str) -> Result<(), super::EwsError> {
    use super::SoapFaultCode;
    use bifrost_types::DiagnosticText;

    let mut reader = Reader::from_str(xml);
    let mut in_error_message = false;
    let mut error_message_element = None;
    let mut in_response_code = false;
    let mut in_message_text = false;
    let mut response_code = String::new();
    let mut message_text = String::new();
    let mut buf = String::new();
    // The first error-classed message that carried no usable code, as the
    // detail text it will be reported with if nothing classifiable follows.
    let mut unclassifiable_error: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                if extract_attribute(e, "ResponseClass").map_err(scan_failed)? == "Error" {
                    in_error_message = true;
                    error_message_element = Some(local.to_string());
                }
                if in_error_message && local == "ResponseCode" {
                    in_response_code = true;
                }
                if in_error_message && local == "MessageText" {
                    in_message_text = true;
                }
                buf.clear();
            }
            // A reference or text run that does not resolve is malformed XML
            // exactly like a reader error, and reported the same way: it
            // used to vanish, so a `ResponseCode` or `MessageText` could
            // read as a token the server never sent.
            Ok(Event::Text(ref e)) => push_text(e, &mut buf).map_err(scan_failed)?,
            Ok(Event::GeneralRef(ref e)) => {
                try_push_general_ref(e, &mut buf).map_err(scan_failed)?;
            }
            Ok(Event::End(ref e)) => {
                let name = e.name().as_ref().to_owned();
                let local = strip_ns(&name);
                if in_response_code && local == "ResponseCode" {
                    response_code = buf.trim().to_string();
                    in_response_code = false;
                }
                if in_message_text && local == "MessageText" {
                    message_text = buf.trim().to_string();
                    in_message_text = false;
                }
                // The first errored ResponseMessage that names a fault
                // decides the classification; stop once we have its code.
                if in_error_message && is_classifiable(&response_code) {
                    break;
                }
                if error_message_element.as_deref() == Some(local) {
                    // An error response we could not classify must not make a
                    // later warning or success response donate its code to
                    // this message. Remember that the body contained a failed
                    // response - it is malformed, not successful - and keep
                    // scanning for a classifiable error, which outranks the
                    // malformed report.
                    if unclassifiable_error.is_none() {
                        unclassifiable_error = Some(unclassifiable_detail(local, &response_code));
                    }
                    in_error_message = false;
                    in_response_code = false;
                    in_message_text = false;
                    error_message_element = None;
                    message_text.clear();
                    // A `NoError` code belongs to the message that closed
                    // here; leaving it set would leak into the next one.
                    response_code.clear();
                }
                buf.clear();
            }
            Ok(Event::Eof) => break,
            // A parse error here is reported as malformed XML, identical
            // to the per-operation parsers, rather than silently treated
            // as success.
            Err(error) => return Err(scan_failed(error.to_string())),
            _ => {}
        }
    }

    if in_error_message && is_classifiable(&response_code) {
        let detail = if message_text.is_empty() {
            format!("EWS ResponseCode {response_code}")
        } else {
            format!("{response_code}: {message_text}")
        };
        return Err(super::EwsError::SoapFault {
            code: SoapFaultCode::parse(&response_code),
            detail: DiagnosticText::support_only(detail),
        });
    }

    // An error message truncated before its closing tag never reached the
    // per-message branch above, so record it here.
    if in_error_message && unclassifiable_error.is_none() {
        let element = error_message_element
            .as_deref()
            .unwrap_or("ResponseMessage");
        unclassifiable_error = Some(unclassifiable_detail(element, &response_code));
    }

    if let Some(detail) = unclassifiable_error {
        return Err(super::EwsError::MalformedXml(DiagnosticText::support_only(
            detail,
        )));
    }

    Ok(())
}

fn scan_failed(reason: String) -> super::EwsError {
    super::EwsError::MalformedXml(bifrost_types::DiagnosticText::support_only(format!(
        "EWS response error scan failed: {reason}"
    )))
}

/// Whether a `ResponseCode` on an error-classed message names a fault this
/// scan can map onto the shared taxonomy. An absent code says nothing, and
/// `NoError` contradicts the `ResponseClass="Error"` it arrived with; both
/// are failures the crate refuses to read as success, but neither can be
/// classified, so a later message carrying a real code outranks them.
fn is_classifiable(response_code: &str) -> bool {
    !response_code.is_empty() && response_code != "NoError"
}

fn unclassifiable_detail(element: &str, response_code: &str) -> String {
    if response_code == "NoError" {
        format!("EWS {element} is ResponseClass=\"Error\" with ResponseCode NoError")
    } else {
        format!("EWS {element} is ResponseClass=\"Error\" with no ResponseCode")
    }
}

/// Append one unescaped text run. A run that does not unescape is `Err` with
/// the reason, never dropped.
fn push_text(e: &BytesText<'_>, buf: &mut String) -> Result<(), String> {
    let text = unescape(e.as_ref()).map_err(|error| format!("malformed XML text: {error}"))?;
    buf.push_str(&text);
    Ok(())
}

/// Fold one entity or character reference back into a text run.
///
/// quick-xml 0.36+ emits `Event::GeneralRef` separately from `Event::Text`,
/// so every parser that accumulates body text must fold these back in or
/// `&lt;` and friends vanish. An unknown entity or an invalid character
/// reference is `Err` with the reference text: it is malformed XML, and
/// dropping it would leave a value the server never sent. There is no
/// lenient twin; every parser surfaces the `Err` through its own error path.
pub(crate) fn try_push_general_ref(e: &BytesRef<'_>, buf: &mut String) -> Result<(), String> {
    let name: &str = e.as_ref();
    if let Some(rest) = name.strip_prefix('#') {
        let codepoint = if let Some(hex) = rest.strip_prefix(['x', 'X']) {
            u32::from_str_radix(hex, 16).ok()
        } else {
            rest.parse::<u32>().ok()
        };
        let c = codepoint
            .and_then(char::from_u32)
            .ok_or_else(|| format!("invalid character reference &{name};"))?;
        buf.push(c);
    } else {
        let s = resolve_xml_entity(name).ok_or_else(|| format!("unknown entity &{name};"))?;
        buf.push_str(s);
    }
    Ok(())
}
