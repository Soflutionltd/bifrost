//! Contact-specific SOAP shapes and fields. Mail read shapes cannot supply
//! indexed contact dictionaries, and FindItem cannot supply a complete body.
use quick_xml::{Reader, events::Event};

use super::{EwsError, extract_attribute, strip_ns, try_push_general_ref, xml_escape};

pub(crate) fn shape(body: bool) -> String {
    let mut fields = [
        "item:ItemClass",
        "contacts:DisplayName",
        "contacts:GivenName",
        "contacts:Surname",
        "contacts:EmailAddresses",
        "contacts:PhoneNumbers",
        "contacts:PhysicalAddresses",
        "contacts:CompanyName",
        "contacts:JobTitle",
    ]
    .iter()
    .map(|field| format!(r#"<t:FieldURI FieldURI="{field}"/>"#))
    .collect::<String>();
    if body {
        fields.push_str(r#"<t:FieldURI FieldURI="item:Body"/>"#);
    }
    format!(
        "<m:ItemShape><t:BaseShape>IdOnly</t:BaseShape><t:BodyType>Text</t:BodyType><t:AdditionalProperties>{fields}</t:AdditionalProperties></m:ItemShape>"
    )
}

pub(crate) fn find_body(folder: &str, offset: u32, query: Option<&str>) -> String {
    let restriction = query.filter(|query| !query.is_empty()).map(|query| {
        let fields = ["DisplayName", "GivenName", "Surname"].iter()
            .map(|field| format!(r#"<t:FieldURI FieldURI="contacts:{field}"/>"#))
            .chain((1..=3).map(|index| format!(r#"<t:IndexedFieldURI FieldURI="contacts:EmailAddress" FieldIndex="EmailAddress{index}"/>"#)));
        let clauses = fields.map(|field| format!(r#"<t:Contains ContainmentMode="Substring" ContainmentComparison="IgnoreCase">{field}<t:Constant Value="{}"/></t:Contains>"#, xml_escape(query))).collect::<String>();
        format!("<m:Restriction><t:Or>{clauses}</t:Or></m:Restriction>")
    }).unwrap_or_default();
    format!(
        r#"<m:FindItem Traversal="Shallow">{}<m:IndexedPageItemView MaxEntriesReturned="250" Offset="{offset}" BasePoint="Beginning"/>{restriction}<m:ParentFolderIds><t:FolderId Id="{}"/></m:ParentFolderIds></m:FindItem>"#,
        shape(false),
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

/// Only the top-level Items children are projected; nested mailbox identities
/// must never overwrite a contact's own ItemId or dictionary keys.
#[derive(Default)]
pub(crate) struct Node {
    pub(crate) name: String,
    pub(crate) key: Option<String>,
    pub(crate) routing_type: Option<String>,
    pub(crate) text: String,
    pub(crate) id: Option<String>,
    pub(crate) change_key: Option<String>,
    pub(crate) children: Vec<Node>,
}

impl Node {
    pub(crate) fn child(&self, name: &str) -> Option<&Self> {
        self.children.iter().find(|child| child.name == name)
    }
    pub(crate) fn value(&self, name: &str) -> Option<String> {
        self.child(name)
            .map(|node| node.text.clone())
            .filter(|text| !text.trim().is_empty())
    }
}

pub(crate) fn contact_nodes(xml: &str) -> Result<Vec<Node>, EwsError> {
    let mut reader = Reader::from_str(xml);
    let mut stack: Vec<Node> = Vec::new();
    let mut result = Vec::new();
    // Preserve dictionary nesting and validate the complete document before projection.
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => stack.push(Node {
                name: strip_ns(e.name().as_ref()).to_string(),
                key: attribute(&e, "Key")?,
                routing_type: routing_type(&e)?,
                id: attribute(&e, "Id")?,
                change_key: attribute(&e, "ChangeKey")?,
                ..Node::default()
            }),
            Ok(Event::Empty(e)) => {
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(Node {
                        name: strip_ns(e.name().as_ref()).to_string(),
                        key: attribute(&e, "Key")?,
                        routing_type: routing_type(&e)?,
                        id: attribute(&e, "Id")?,
                        change_key: attribute(&e, "ChangeKey")?,
                        ..Node::default()
                    });
                }
            }
            Ok(Event::Text(e)) => {
                if let Some(node) = stack.last_mut() {
                    node.text.push_str(
                        &quick_xml::escape::unescape(e.as_ref())
                            .map_err(|error| malformed(error.to_string()))?,
                    );
                }
            }
            Ok(Event::CData(e)) => {
                if let Some(node) = stack.last_mut() {
                    node.text.push_str(e.as_ref());
                }
            }
            Ok(Event::GeneralRef(e)) => {
                if let Some(node) = stack.last_mut() {
                    try_push_general_ref(&e, &mut node.text).map_err(malformed)?;
                }
            }
            Ok(Event::End(_)) => {
                if let Some(node) = stack.pop()
                    && let Some(parent) = stack.last_mut()
                {
                    if parent.name == "Items" {
                        result.push(node);
                    } else {
                        parent.children.push(node);
                    }
                }
            }
            Ok(Event::Eof) => {
                if !stack.is_empty() {
                    return Err(malformed("truncated public contact response"));
                }
                return Ok(result);
            }
            Err(error) => return Err(malformed(error.to_string())),
            _ => {}
        }
    }
}

fn malformed(detail: impl Into<String>) -> EwsError {
    EwsError::MalformedXml(bifrost_types::DiagnosticText::support_only(detail.into()))
}

fn attribute(
    e: &quick_xml::events::BytesStart<'_>,
    name: &str,
) -> Result<Option<String>, EwsError> {
    Ok(Some(extract_attribute(e, name).map_err(malformed)?).filter(|value| !value.is_empty()))
}

fn routing_type(e: &quick_xml::events::BytesStart<'_>) -> Result<Option<String>, EwsError> {
    if e.try_get_attribute("RoutingType")
        .map_err(|error| malformed(error.to_string()))?
        .is_some()
    {
        // A present empty type is not missing metadata and must not get the
        // untyped SMTP syntax fallback.
        extract_attribute(e, "RoutingType")
            .map(Some)
            .map_err(malformed)
    } else {
        Ok(None)
    }
}
