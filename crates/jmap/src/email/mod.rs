// The Account impl uses a strict subset of the Email/* surface
// (Set / Get / Query / blob upload); the unused builder helpers,
// SearchSnippet, and Import paths stay built for completeness.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod import;
pub(crate) mod parse;
pub(crate) mod query;
pub(crate) mod search_snippet;
pub(crate) mod set;

use jiff::Timestamp;
use serde::{
    Deserialize, Serialize,
    de::{IgnoredAny, MapAccess, Visitor},
};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

use crate::core::id::BlobId;
use crate::core::request::ResultReference;
use crate::mailbox::MailboxId;
use crate::thread::ThreadId;

mod marker {
    pub(crate) enum Email {}
}
/// Strongly-typed Email ID.
pub(crate) type EmailId = crate::core::id::Id<marker::Email>;

/// RFC 8621 s4.1.1 IMAP-derived keyword marking a message as a draft.
/// A JMAP server derives "this is a draft" from the keyword, not from
/// mailbox membership, so it has to be cleared when a draft is sent.
pub(crate) const DRAFT_KEYWORD: &str = "$draft";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct Email {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<EmailId>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) blob_id: Option<BlobId>,

    #[serde(rename = "threadId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) thread_id: Option<ThreadId>,

    #[serde(rename = "mailboxIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) mailbox_ids: Option<HashMap<MailboxId, bool>>,

    #[serde(rename = "keywords")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) keywords: Option<HashMap<String, bool>>,

    #[serde(rename = "size")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) size: Option<usize>,

    #[serde(rename = "receivedAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) received_at: Option<Timestamp>,

    #[serde(alias = "header:Message-ID:asMessageIds")]
    #[serde(rename = "messageId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) message_id: Option<Vec<String>>,

    #[serde(rename = "inReplyTo")]
    #[serde(alias = "header:In-Reply-To:asMessageIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) in_reply_to: Option<Vec<String>>,

    #[serde(rename = "references")]
    #[serde(alias = "header:References:asMessageIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) references: Option<Vec<String>>,

    #[serde(rename = "sender")]
    #[serde(alias = "header:Sender:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sender: Option<Vec<EmailAddress>>,

    #[serde(rename = "from")]
    #[serde(alias = "header:From:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) from: Option<Vec<EmailAddress>>,

    #[serde(rename = "to")]
    #[serde(alias = "header:To:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) to: Option<Vec<EmailAddress>>,

    #[serde(rename = "cc")]
    #[serde(alias = "header:Cc:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cc: Option<Vec<EmailAddress>>,

    #[serde(rename = "bcc")]
    #[serde(alias = "header:Bcc:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) bcc: Option<Vec<EmailAddress>>,

    #[serde(rename = "replyTo")]
    #[serde(alias = "header:Reply-To:asAddresses")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reply_to: Option<Vec<EmailAddress>>,

    #[serde(rename = "subject")]
    #[serde(alias = "header:Subject:asText")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) subject: Option<String>,

    #[serde(rename = "sentAt")]
    #[serde(alias = "header:Date:asDate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sent_at: Option<Timestamp>,

    #[serde(rename = "bodyStructure")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) body_structure: Option<Box<EmailBodyPart>>,

    #[serde(rename = "bodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) body_values: Option<HashMap<String, EmailBodyValue>>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_body: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_body: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "attachments")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) attachments: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "hasAttachment")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) has_attachment: Option<bool>,

    #[serde(rename = "preview")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) preview: Option<String>,

    #[serde(flatten)]
    #[serde(deserialize_with = "deserialize_headers")]
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub(super) headers: HashMap<Header, Option<HeaderValue>>,
}

/// Retain only dynamic `header:*` properties in the flattened map. Servers
/// may include extension properties alongside an Email projection; those are
/// not headers and must not make the whole object undecodable.
fn deserialize_headers<'de, D>(
    deserializer: D,
) -> Result<HashMap<Header, Option<HeaderValue>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct HeadersVisitor;

    impl<'de> Visitor<'de> for HeadersVisitor {
        type Value = HashMap<Header, Option<HeaderValue>>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map of dynamic JMAP header properties")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut headers = HashMap::new();

            while let Some(key) = map.next_key::<String>()? {
                if let Some(header) = Header::parse(&key) {
                    headers.insert(header, map.next_value()?);
                } else {
                    map.next_value::<IgnoredAny>()?;
                }
            }

            Ok(headers)
        }
    }

    deserializer.deserialize_map(HeadersVisitor)
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct EmailCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "mailboxIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) mailbox_ids: Option<HashMap<MailboxId, bool>>,

    #[serde(rename = "#mailboxIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) mailbox_ids_ref: Option<ResultReference>,

    #[serde(rename = "keywords")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) keywords: Option<HashMap<String, bool>>,

    #[serde(rename = "receivedAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) received_at: Option<Timestamp>,

    #[serde(rename = "messageId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) message_id: Option<Vec<String>>,

    #[serde(rename = "inReplyTo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) in_reply_to: Option<Vec<String>>,

    #[serde(rename = "references")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) references: Option<Vec<String>>,

    #[serde(rename = "sender")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sender: Option<Vec<EmailAddress>>,

    #[serde(rename = "from")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) from: Option<Vec<EmailAddress>>,

    #[serde(rename = "to")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) to: Option<Vec<EmailAddress>>,

    #[serde(rename = "cc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cc: Option<Vec<EmailAddress>>,

    #[serde(rename = "bcc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) bcc: Option<Vec<EmailAddress>>,

    #[serde(rename = "replyTo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reply_to: Option<Vec<EmailAddress>>,

    #[serde(rename = "subject")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) subject: Option<String>,

    #[serde(rename = "sentAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sent_at: Option<Timestamp>,

    #[serde(rename = "bodyStructure")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) body_structure: Option<Box<EmailBodyPart>>,

    #[serde(rename = "bodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) body_values: Option<HashMap<String, EmailBodyValue>>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_body: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_body: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "attachments")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) attachments: Option<Vec<EmailBodyPart>>,

    #[serde(flatten)]
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub(super) headers: HashMap<Header, Option<HeaderValue>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct EmailPatch {
    #[serde(rename = "mailboxIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) mailbox_ids: Option<HashMap<MailboxId, bool>>,

    #[serde(rename = "keywords")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) keywords: Option<HashMap<String, bool>>,

    #[serde(rename = "subject")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) subject: Option<String>,

    /// Dotted-path patch entries (`mailboxIds/<id>`, `keywords/<flag>`, etc.)
    /// flatten directly into the body.
    #[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) patch: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct EmailBodyPart {
    #[serde(rename = "partId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) part_id: Option<String>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) blob_id: Option<BlobId>,

    #[serde(rename = "size")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) size: Option<usize>,

    #[serde(rename = "headers")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) headers: Option<Vec<EmailHeader>>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) type_: Option<String>,

    #[serde(rename = "charset")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) charset: Option<String>,

    #[serde(rename = "disposition")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) disposition: Option<String>,

    #[serde(rename = "cid")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cid: Option<String>,

    #[serde(rename = "language")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) language: Option<Vec<String>>,

    #[serde(rename = "location")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) location: Option<String>,

    #[serde(rename = "subParts")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sub_parts: Option<Vec<EmailBodyPart>>,

    #[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) header: Option<HashMap<Header, HeaderValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EmailBodyValue {
    #[serde(rename = "value")]
    pub(super) value: String,

    #[serde(rename = "isEncodingProblem")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_encoding_problem: Option<bool>,

    #[serde(rename = "isTruncated")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_truncated: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EmailAddress {
    pub(super) name: Option<String>,
    pub(super) email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EmailAddressGroup {
    pub(super) name: Option<String>,
    pub(super) addresses: Vec<EmailAddress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EmailHeader {
    pub(super) name: String,
    pub(super) value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub(crate) enum Property {
    Id,
    BlobId,
    ThreadId,
    MailboxIds,
    Keywords,
    Size,
    ReceivedAt,
    MessageId,
    InReplyTo,
    References,
    Sender,
    From,
    To,
    Cc,
    Bcc,
    ReplyTo,
    Subject,
    SentAt,
    BodyStructure,
    BodyValues,
    TextBody,
    HtmlBody,
    Attachments,
    HasAttachment,
    Preview,
    Header(Header),
    Other(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum HeaderValue {
    AsGroupedAddressesAll(Vec<Vec<EmailAddressGroup>>),
    AsGroupedAddresses(Vec<EmailAddressGroup>),
    AsAddressesAll(Vec<Vec<EmailAddress>>),
    AsAddresses(Vec<EmailAddress>),
    AsTextListAll(Vec<Vec<String>>),
    AsDateAll(Vec<Timestamp>),
    AsDate(Timestamp),
    AsTextAll(Vec<String>),
    AsText(String),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Clone)]
pub(crate) struct Header {
    pub(crate) name: String,
    pub(crate) form: HeaderForm,
    pub(crate) all: bool,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, PartialOrd, Ord)]
#[non_exhaustive]
pub(crate) enum HeaderForm {
    Raw,
    Text,
    Addresses,
    GroupedAddresses,
    MessageIds,
    Date,
    URLs,
}

impl Property {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(Property::Id),
            "blobId" => Some(Property::BlobId),
            "threadId" => Some(Property::ThreadId),
            "mailboxIds" => Some(Property::MailboxIds),
            "keywords" => Some(Property::Keywords),
            "size" => Some(Property::Size),
            "receivedAt" => Some(Property::ReceivedAt),
            "messageId" => Some(Property::MessageId),
            "inReplyTo" => Some(Property::InReplyTo),
            "references" => Some(Property::References),
            "sender" => Some(Property::Sender),
            "from" => Some(Property::From),
            "to" => Some(Property::To),
            "cc" => Some(Property::Cc),
            "bcc" => Some(Property::Bcc),
            "replyTo" => Some(Property::ReplyTo),
            "subject" => Some(Property::Subject),
            "sentAt" => Some(Property::SentAt),
            "hasAttachment" => Some(Property::HasAttachment),
            "preview" => Some(Property::Preview),
            "bodyValues" => Some(Property::BodyValues),
            "textBody" => Some(Property::TextBody),
            "htmlBody" => Some(Property::HtmlBody),
            "attachments" => Some(Property::Attachments),
            "bodyStructure" => Some(Property::BodyStructure),
            _ if value.starts_with("header:") => Some(Property::Header(Header::parse(value)?)),
            _ => Some(Property::Other(value.to_string())),
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::BlobId => write!(f, "blobId"),
            Property::ThreadId => write!(f, "threadId"),
            Property::MailboxIds => write!(f, "mailboxIds"),
            Property::Keywords => write!(f, "keywords"),
            Property::Size => write!(f, "size"),
            Property::ReceivedAt => write!(f, "receivedAt"),
            Property::MessageId => write!(f, "messageId"),
            Property::InReplyTo => write!(f, "inReplyTo"),
            Property::References => write!(f, "references"),
            Property::Sender => write!(f, "sender"),
            Property::From => write!(f, "from"),
            Property::To => write!(f, "to"),
            Property::Cc => write!(f, "cc"),
            Property::Bcc => write!(f, "bcc"),
            Property::ReplyTo => write!(f, "replyTo"),
            Property::Subject => write!(f, "subject"),
            Property::SentAt => write!(f, "sentAt"),
            Property::BodyStructure => write!(f, "bodyStructure"),
            Property::BodyValues => write!(f, "bodyValues"),
            Property::TextBody => write!(f, "textBody"),
            Property::HtmlBody => write!(f, "htmlBody"),
            Property::Attachments => write!(f, "attachments"),
            Property::HasAttachment => write!(f, "hasAttachment"),
            Property::Preview => write!(f, "preview"),
            Property::Header(header) => header.fmt(f),
            Property::Other(other) => write!(f, "{other}"),
        }
    }
}

impl Serialize for Property {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct PropertyVisitor;

impl<'de> Visitor<'de> for PropertyVisitor {
    type Value = Property;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP e-mail property")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Property::parse(v)
            .ok_or_else(|| serde::de::Error::custom(format!("Failed to parse JMAP property '{v}'")))
    }
}

impl<'de> Deserialize<'de> for Property {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(PropertyVisitor)
    }
}

impl Serialize for Header {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct HeaderVisitor;

impl<'de> Visitor<'de> for HeaderVisitor {
    type Value = Header;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP header")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Header::parse(v)
            .ok_or_else(|| serde::de::Error::custom(format!("Failed to parse JMAP header '{v}'")))
    }
}

impl<'de> Deserialize<'de> for Header {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(HeaderVisitor)
    }
}

impl HeaderForm {
    pub(crate) fn parse(value: &str) -> Option<HeaderForm> {
        match value {
            "asText" => Some(HeaderForm::Text),
            "asAddresses" => Some(HeaderForm::Addresses),
            "asGroupedAddresses" => Some(HeaderForm::GroupedAddresses),
            "asMessageIds" => Some(HeaderForm::MessageIds),
            "asDate" => Some(HeaderForm::Date),
            "asURLs" => Some(HeaderForm::URLs),
            _ => None,
        }
    }
}

impl Header {
    pub(crate) fn as_raw(name: impl Into<String>, all: bool) -> Header {
        Header {
            name: name.into(),
            form: HeaderForm::Raw,
            all,
        }
    }
    pub(crate) fn as_text(name: impl Into<String>, all: bool) -> Header {
        Header {
            name: name.into(),
            form: HeaderForm::Text,
            all,
        }
    }
    pub(crate) fn as_addresses(name: impl Into<String>, all: bool) -> Header {
        Header {
            name: name.into(),
            form: HeaderForm::Addresses,
            all,
        }
    }
    pub(crate) fn as_grouped_addresses(name: impl Into<String>, all: bool) -> Header {
        Header {
            name: name.into(),
            form: HeaderForm::GroupedAddresses,
            all,
        }
    }
    pub(crate) fn as_message_ids(name: impl Into<String>, all: bool) -> Header {
        Header {
            name: name.into(),
            form: HeaderForm::MessageIds,
            all,
        }
    }
    pub(crate) fn as_date(name: impl Into<String>, all: bool) -> Header {
        Header {
            name: name.into(),
            form: HeaderForm::Date,
            all,
        }
    }
    pub(crate) fn as_urls(name: impl Into<String>, all: bool) -> Header {
        Header {
            name: name.into(),
            form: HeaderForm::URLs,
            all,
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Header> {
        let mut all = false;
        let mut form = HeaderForm::Raw;
        let mut header = None;
        for (pos, part) in value.split(':').enumerate() {
            match pos {
                0 if part == "header" => (),
                1 => header = part.into(),
                2 | 3 if part == "all" => all = true,
                2 => form = HeaderForm::parse(part)?,
                _ => return None,
            }
        }
        Header {
            name: header?.to_string(),
            form,
            all,
        }
        .into()
    }
}

impl Display for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut value = format!("header:{}{}", self.name, self.form);
        if self.all {
            value.push_str(":all");
        }
        f.pad(&value)
    }
}

impl Display for HeaderForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeaderForm::Raw => Ok(()),
            HeaderForm::Text => write!(f, ":asText"),
            HeaderForm::Addresses => write!(f, ":asAddresses"),
            HeaderForm::GroupedAddresses => write!(f, ":asGroupedAddresses"),
            HeaderForm::MessageIds => write!(f, ":asMessageIds"),
            HeaderForm::Date => write!(f, ":asDate"),
            HeaderForm::URLs => write!(f, ":asURLs"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub(crate) enum BodyProperty {
    PartId,
    BlobId,
    Size,
    Headers,
    Name,
    Type,
    Charset,
    Disposition,
    Cid,
    Language,
    Location,
    SubParts,
    Header(Header),
}

impl BodyProperty {
    fn parse(value: &str) -> Option<BodyProperty> {
        match value {
            "partId" => Some(BodyProperty::PartId),
            "blobId" => Some(BodyProperty::BlobId),
            "size" => Some(BodyProperty::Size),
            "name" => Some(BodyProperty::Name),
            "type" => Some(BodyProperty::Type),
            "charset" => Some(BodyProperty::Charset),
            "headers" => Some(BodyProperty::Headers),
            "disposition" => Some(BodyProperty::Disposition),
            "cid" => Some(BodyProperty::Cid),
            "language" => Some(BodyProperty::Language),
            "location" => Some(BodyProperty::Location),
            "subParts" => Some(BodyProperty::SubParts),
            _ if value.starts_with("header:") => Some(BodyProperty::Header(Header::parse(value)?)),
            _ => None,
        }
    }
}

impl Display for BodyProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodyProperty::PartId => write!(f, "partId"),
            BodyProperty::BlobId => write!(f, "blobId"),
            BodyProperty::Size => write!(f, "size"),
            BodyProperty::Name => write!(f, "name"),
            BodyProperty::Type => write!(f, "type"),
            BodyProperty::Charset => write!(f, "charset"),
            BodyProperty::Header(header) => header.fmt(f),
            BodyProperty::Headers => write!(f, "headers"),
            BodyProperty::Disposition => write!(f, "disposition"),
            BodyProperty::Cid => write!(f, "cid"),
            BodyProperty::Language => write!(f, "language"),
            BodyProperty::Location => write!(f, "location"),
            BodyProperty::SubParts => write!(f, "subParts"),
        }
    }
}

impl Serialize for BodyProperty {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct BodyPropertyVisitor;

impl<'de> Visitor<'de> for BodyPropertyVisitor {
    type Value = BodyProperty;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP body property")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        BodyProperty::parse(v).ok_or_else(|| {
            serde::de::Error::custom(format!("Failed to parse JMAP body property '{v}'"))
        })
    }
}

impl<'de> Deserialize<'de> for BodyProperty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(BodyPropertyVisitor)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct MailCapabilities {
    #[serde(rename = "maxMailboxesPerEmail")]
    max_mailboxes_per_email: Option<usize>,

    #[serde(rename = "maxMailboxDepth")]
    max_mailbox_depth: usize,

    #[serde(rename = "maxSizeMailboxName")]
    max_size_mailbox_name: usize,

    #[serde(rename = "maxSizeAttachmentsPerEmail")]
    max_size_attachments_per_email: usize,

    #[serde(rename = "emailQuerySortOptions")]
    email_query_sort_options: Vec<String>,

    #[serde(rename = "mayCreateTopLevelMailbox")]
    may_create_top_level_mailbox: bool,
}

/// Account capabilities for `urn:ietf:params:jmap:submission` (RFC 8621 s1.3.2).
///
/// The session object is an empty support marker. Only the selected account
/// supplies this mandatory delayed-send window; an absent field there is
/// malformed, whereas an advertised zero permits immediate submission.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SubmissionCapabilities {
    #[serde(rename = "maxDelayedSend")]
    max_delayed_send: usize,

    #[serde(rename = "submissionExtensions")]
    #[serde(default)]
    submission_extensions: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct QueryArguments {
    #[serde(rename = "collapseThreads")]
    #[serde(skip_serializing_if = "Option::is_none")]
    collapse_threads: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct GetArguments {
    #[serde(rename = "bodyProperties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    body_properties: Option<Vec<BodyProperty>>,

    #[serde(rename = "fetchTextBodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_text_body_values: Option<bool>,

    #[serde(rename = "fetchHTMLBodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_html_body_values: Option<bool>,

    #[serde(rename = "fetchAllBodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_all_body_values: Option<bool>,

    #[serde(rename = "maxBodyValueBytes")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_body_value_bytes: Option<usize>,
}

impl QueryArguments {
    pub(crate) fn collapse_threads(&mut self, collapse_threads: bool) {
        self.collapse_threads = collapse_threads.into();
    }
}

impl GetArguments {
    pub(crate) fn body_properties(
        &mut self,
        body_properties: impl IntoIterator<Item = BodyProperty>,
    ) -> &mut Self {
        self.body_properties = Some(body_properties.into_iter().collect());
        self
    }

    pub(crate) fn fetch_text_body_values(&mut self, fetch_text_body_values: bool) -> &mut Self {
        self.fetch_text_body_values = fetch_text_body_values.into();
        self
    }

    pub(crate) fn fetch_html_body_values(&mut self, fetch_html_body_values: bool) -> &mut Self {
        self.fetch_html_body_values = fetch_html_body_values.into();
        self
    }

    pub(crate) fn fetch_all_body_values(&mut self, fetch_all_body_values: bool) -> &mut Self {
        self.fetch_all_body_values = fetch_all_body_values.into();
        self
    }

    pub(crate) fn max_body_value_bytes(&mut self, max_body_value_bytes: usize) -> &mut Self {
        self.max_body_value_bytes = max_body_value_bytes.into();
        self
    }
}

impl crate::core::Object for Email {
    type Property = Property;
    type Id = EmailId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Email {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for Email {
    type GetArguments = GetArguments;
}

impl crate::core::set::SetObject for Email {
    type Create = EmailCreate;
    type Patch = EmailPatch;
    type SetArguments = ();
}

impl crate::core::SetCreate for EmailCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        EmailCreate {
            _create_id: create_id,
            ..Default::default()
        }
    }
}

crate::define_get_method!(EmailGet, Email, "Email/get", crate::core::capability::Mail);
crate::define_set_method!(EmailSet, Email, "Email/set", crate::core::capability::Mail);
crate::define_changes_method!(
    EmailChanges,
    Email,
    "Email/changes",
    crate::core::capability::Mail
);
crate::define_query_method!(
    EmailQuery,
    Email,
    "Email/query",
    crate::core::capability::Mail
);
crate::define_query_changes_method!(
    EmailQueryChanges,
    Email,
    "Email/queryChanges",
    crate::core::capability::Mail
);
crate::define_copy_method!(
    EmailCopy,
    Email,
    "Email/copy",
    crate::core::capability::Mail
);

impl EmailGet {
    #[must_use]
    pub(crate) fn body_properties(
        mut self,
        body_properties: impl IntoIterator<Item = BodyProperty>,
    ) -> Self {
        self.arguments().body_properties(body_properties);
        self
    }

    #[must_use]
    pub(crate) fn fetch_text_body_values(mut self, v: bool) -> Self {
        self.arguments().fetch_text_body_values(v);
        self
    }

    #[must_use]
    pub(crate) fn fetch_html_body_values(mut self, v: bool) -> Self {
        self.arguments().fetch_html_body_values(v);
        self
    }

    #[must_use]
    pub(crate) fn fetch_all_body_values(mut self, v: bool) -> Self {
        self.arguments().fetch_all_body_values(v);
        self
    }

    #[must_use]
    pub(crate) fn max_body_value_bytes(mut self, v: usize) -> Self {
        self.arguments().max_body_value_bytes(v);
        self
    }
}

impl EmailQuery {
    #[must_use]
    pub(crate) fn collapse_threads(mut self, v: bool) -> Self {
        self.arguments().collapse_threads(v);
        self
    }
}

impl MailCapabilities {
    pub(crate) fn max_mailboxes_per_email(&self) -> Option<usize> {
        self.max_mailboxes_per_email
    }

    pub(crate) fn max_mailbox_depth(&self) -> usize {
        self.max_mailbox_depth
    }

    pub(crate) fn max_size_mailbox_name(&self) -> usize {
        self.max_size_mailbox_name
    }

    pub(crate) fn max_size_attachments_per_email(&self) -> usize {
        self.max_size_attachments_per_email
    }

    pub(crate) fn email_query_sort_options(&self) -> &[String] {
        &self.email_query_sort_options
    }

    pub(crate) fn may_create_top_level_mailbox(&self) -> bool {
        self.may_create_top_level_mailbox
    }
}

impl SubmissionCapabilities {
    pub(crate) fn max_delayed_send(&self) -> usize {
        self.max_delayed_send
    }

    pub(crate) fn submission_extensions(&self) -> &HashMap<String, Vec<String>> {
        &self.submission_extensions
    }
}

#[cfg(feature = "debug")]
use std::collections::BTreeMap;

#[cfg(feature = "debug")]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct TestEmail {
    #[serde(rename = "mailboxIds")]
    pub(crate) mailbox_ids: Option<BTreeMap<String, bool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) keywords: Option<BTreeMap<String, bool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) size: Option<usize>,

    #[serde(rename = "receivedAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) received_at: Option<Timestamp>,

    #[serde(rename = "messageId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) message_id: Option<Vec<String>>,

    #[serde(rename = "inReplyTo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) in_reply_to: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) references: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sender: Option<Vec<EmailAddress>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<Vec<EmailAddress>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) to: Option<Vec<EmailAddress>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cc: Option<Vec<EmailAddress>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bcc: Option<Vec<EmailAddress>>,

    #[serde(rename = "replyTo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reply_to: Option<Vec<EmailAddress>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<String>,

    #[serde(rename = "sentAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sent_at: Option<Timestamp>,

    #[serde(rename = "bodyStructure")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) body_structure: Option<Box<EmailBodyPart>>,

    #[serde(rename = "bodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) body_values: Option<BTreeMap<String, EmailBodyValue>>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text_body: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) html_body: Option<Vec<EmailBodyPart>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) attachments: Option<Vec<EmailBodyPart>>,

    #[serde(rename = "hasAttachment")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) has_attachment: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) preview: Option<String>,

    #[serde(flatten)]
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) headers: BTreeMap<Header, Option<HeaderValue>>,
}

#[cfg(feature = "debug")]
impl From<Email> for TestEmail {
    fn from(email: Email) -> Self {
        TestEmail {
            mailbox_ids: email
                .mailbox_ids
                .map(|ids| ids.into_iter().map(|(k, v)| (k.to_string(), v)).collect()),
            keywords: email
                .keywords
                .map(|keywords| keywords.into_iter().collect()),
            size: email.size,
            received_at: email.received_at,
            message_id: email.message_id,
            in_reply_to: email.in_reply_to,
            references: email.references,
            sender: email.sender,
            from: email.from,
            to: email.to,
            cc: email.cc,
            bcc: email.bcc,
            reply_to: email.reply_to,
            subject: email.subject,
            sent_at: email.sent_at,
            body_structure: email.body_structure,
            body_values: email
                .body_values
                .map(|body_values| body_values.into_iter().collect()),
            text_body: email.text_body,
            html_body: email.html_body,
            attachments: email.attachments,
            has_attachment: email.has_attachment,
            preview: email.preview,
            headers: email.headers.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_form_aliases_decode_into_their_canonical_fields() {
        let email: Email = serde_json::from_value(serde_json::json!({
            "header:From:asAddresses": [{ "email": "sender@example.test" }]
        }))
        .expect("header-form email decodes");

        let from = email.from.expect("header-form alias populates from");
        assert_eq!(from[0].email, "sender@example.test");
    }
}
