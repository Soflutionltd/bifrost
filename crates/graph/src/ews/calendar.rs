//! Calendar-specific shapes. CalendarView expands recurring series server-side.
use super::xml_escape;

fn shape(body: bool) -> String {
    let mut fields = [
        "item:ItemClass",
        "item:Subject",
        "item:Sensitivity",
        "item:Preview",
        "calendar:UID",
        "calendar:Start",
        "calendar:End",
        "calendar:OriginalStart",
        "calendar:IsAllDayEvent",
        "calendar:CalendarItemType",
        "calendar:IsCancelled",
        "calendar:LegacyFreeBusyStatus",
        "calendar:Location",
        "calendar:Organizer",
        "calendar:RequiredAttendees",
        "calendar:OptionalAttendees",
        "calendar:Resources",
        "calendar:MyResponseType",
        "calendar:StartTimeZone",
        "calendar:EndTimeZone",
        "calendar:Recurrence",
    ]
    .iter()
    .filter(|field| body || matches!(**field, "item:ItemClass" | "calendar:CalendarItemType"))
    .map(|field| format!(r#"<t:FieldURI FieldURI="{field}"/>"#))
    .collect::<String>();
    if body {
        fields.push_str(r#"<t:FieldURI FieldURI="item:Body"/>"#);
    }
    format!(
        "<m:ItemShape><t:BaseShape>IdOnly</t:BaseShape><t:BodyType>Text</t:BodyType><t:AdditionalProperties>{fields}</t:AdditionalProperties></m:ItemShape>"
    )
}

pub(crate) fn find_body(folder: &str, start: &str, end: &str, cap: u32) -> String {
    format!(
        r#"<m:FindItem Traversal="Shallow">{}<m:CalendarView StartDate="{}" EndDate="{}" MaxEntriesReturned="{cap}"/><m:ParentFolderIds><t:FolderId Id="{}"/></m:ParentFolderIds></m:FindItem>"#,
        shape(false),
        xml_escape(start),
        xml_escape(end),
        xml_escape(folder)
    )
}

pub(crate) fn get_body(item: &str) -> String {
    format!(
        r#"<m:GetItem>{}<m:ItemIds><t:ItemId Id="{}"/></m:ItemIds></m:GetItem>"#,
        shape(true),
        xml_escape(item)
    )
}

/// Typed multi-item builder, reachable only through the per-response funnel.
pub(super) fn get_many_body(ids: &[String]) -> String {
    let items = ids
        .iter()
        .map(|id| format!(r#"<t:ItemId Id="{}"/>"#, xml_escape(id)))
        .collect::<String>();
    format!(
        "<m:GetItem>{}<m:ItemIds>{items}</m:ItemIds></m:GetItem>",
        shape(true)
    )
}

fn malformed(detail: &str) -> super::EwsError {
    super::EwsError::MalformedXml(bifrost_types::DiagnosticText::support_only(
        detail.to_string(),
    ))
}

/// Validate the full document before splitting. EWS response order follows
/// request order, including error messages without an ItemId. Successful ids
/// are additionally checked against the requested identity by the account.
pub(super) fn parse_get_items(
    xml: &str,
    count: usize,
) -> Result<Vec<Result<super::contacts::Node, super::EwsError>>, super::EwsError> {
    use quick_xml::{Reader, Writer, events::Event};
    super::contacts::contact_nodes(xml)?;
    let mut reader = Reader::from_str(xml);
    let mut stack = Vec::new();
    let mut frame = None;
    let mut frames = Vec::new();
    loop {
        let event = reader
            .read_event()
            .map_err(|_| malformed("invalid GetItem batch XML"))?;
        match &event {
            Event::Start(e) | Event::Empty(e) => {
                let name = super::strip_ns(e.name().as_ref()).to_string();
                if stack.last().map(String::as_str) == Some("ResponseMessages") {
                    if name != "GetItemResponseMessage" || frame.is_some() {
                        return Err(malformed("unexpected GetItem response message"));
                    }
                    frame = Some(Writer::new(Vec::new()));
                }
                if !matches!(event, Event::Empty(_)) {
                    stack.push(name);
                }
            }
            Event::End(_) => {
                stack.pop();
            }
            Event::Eof => break,
            _ => {}
        }
        if let Some(writer) = frame.as_mut() {
            writer
                .write_event(event)
                .map_err(|_| malformed("cannot frame GetItem response"))?;
            if stack.last().map(String::as_str) == Some("ResponseMessages") {
                let bytes = frame.take().expect("active response frame").into_inner();
                frames.push(
                    String::from_utf8(bytes).map_err(|_| malformed("invalid response encoding"))?,
                );
            }
        }
    }
    if frames.len() != count {
        // Exchange may answer a general batch-processing failure with one
        // error message instead of one message per id. It has no per-id
        // correlation, so retain its classification as a request failure.
        if frames.len() == 1 {
            super::check_response_error(&frames[0])?;
        }
        return Err(malformed("GetItem response count differs from request"));
    }
    Ok(frames
        .into_iter()
        .map(|xml| {
            super::check_response_error(&xml)?;
            let mut reader = Reader::from_str(&xml);
            match reader
                .read_event()
                .map_err(|_| malformed("missing GetItem response"))?
            {
                Event::Start(e)
                    if super::extract_attribute(&e, "ResponseClass")
                        .map_err(|_| malformed("invalid ResponseClass"))?
                        == "Success" => {}
                _ => return Err(malformed("GetItem response is not successful")),
            }
            let mut nodes = super::contacts::contact_nodes(&xml)?;
            if nodes.len() != 1 {
                return Err(malformed("GetItem response must contain one item"));
            }
            Ok(nodes.remove(0))
        })
        .collect())
}

pub(crate) fn search_body(folder: &str, offset: u32, query: &str) -> String {
    let restriction = if query.is_empty() {
        String::new()
    } else {
        let clauses = ["item:Subject", "calendar:Location", "item:Body"].iter().map(|field| format!(r#"<t:Contains ContainmentMode="Substring" ContainmentComparison="IgnoreCase"><t:FieldURI FieldURI="{field}"/><t:Constant Value="{}"/></t:Contains>"#, xml_escape(query))).collect::<String>();
        format!("<m:Restriction><t:Or>{clauses}</t:Or></m:Restriction>")
    };
    format!(
        r#"<m:FindItem Traversal="Shallow">{}<m:IndexedPageItemView MaxEntriesReturned="250" Offset="{offset}" BasePoint="Beginning"/>{restriction}<m:ParentFolderIds><t:FolderId Id="{}"/></m:ParentFolderIds></m:FindItem>"#,
        shape(false),
        xml_escape(folder)
    )
}
