use super::{
    EmailAddress, EmailAddressGroup, EmailBodyPart, EmailBodyValue, EmailCreate, EmailHeader,
    EmailPatch, Header, HeaderValue,
};
use crate::core::id::BlobId;
use crate::core::patch::{self, IdSet, IdSetPatch};
use crate::core::request::ResultReference;
use crate::mailbox::MailboxId;
use serde::Serialize;
use std::collections::HashMap;

impl EmailCreate {
    pub(crate) fn mailbox_ids<T, U>(&mut self, mailbox_ids: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<MailboxId>,
    {
        self.mailbox_ids = Some(IdSet::collect(mailbox_ids.into_iter().map(Into::into)));
        self.mailbox_ids_ref = None;
        self
    }

    pub(crate) fn mailbox_ids_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.mailbox_ids_ref = reference.into();
        self.mailbox_ids = None;
        self
    }

    pub(crate) fn keywords<T, U>(&mut self, keywords: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<super::keyword::Keyword>,
    {
        self.keywords = Some(IdSet::collect(keywords.into_iter().map(Into::into)));
        self
    }

    pub(crate) fn message_id<T, U>(&mut self, message_id: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.message_id = Some(
            message_id
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        );
        self
    }

    pub(crate) fn in_reply_to<T, U>(&mut self, in_reply_to: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.in_reply_to = Some(
            in_reply_to
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        );
        self
    }

    pub(crate) fn references<T, U>(&mut self, references: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.references = Some(
            references
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        );
        self
    }

    pub(crate) fn sender<T, U>(&mut self, sender: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.sender = Some(sender.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn from<T, U>(&mut self, from: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.from = Some(from.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn to<T, U>(&mut self, to: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.to = Some(to.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn cc<T, U>(&mut self, cc: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.cc = Some(cc.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn bcc<T, U>(&mut self, bcc: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.bcc = Some(bcc.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn reply_to<T, U>(&mut self, reply_to: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.reply_to = Some(reply_to.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub(crate) fn subject(&mut self, subject: impl Into<String>) -> &mut Self {
        self.subject = Some(subject.into());
        self
    }

    // Accept only validated dates; an invalid date must not become 1970.
    pub(crate) fn sent_at(&mut self, sent_at: jiff::Timestamp) -> &mut Self {
        self.sent_at = Some(sent_at);
        self
    }

    pub(crate) fn body_structure(&mut self, body_structure: EmailBodyPart) -> &mut Self {
        self.body_structure = Some(body_structure.into());
        self
    }

    pub(crate) fn body_value(
        &mut self,
        id: String,
        body_value: impl Into<EmailBodyValue>,
    ) -> &mut Self {
        self.body_values
            .get_or_insert_with(HashMap::new)
            .insert(id, body_value.into());
        self
    }

    pub(crate) fn text_body(&mut self, text_body: impl Into<EmailBodyPart>) -> &mut Self {
        self.text_body
            .get_or_insert_with(Vec::new)
            .push(text_body.into());
        self
    }

    pub(crate) fn html_body(&mut self, html_body: impl Into<EmailBodyPart>) -> &mut Self {
        self.html_body
            .get_or_insert_with(Vec::new)
            .push(html_body.into());
        self
    }

    pub(crate) fn attachment(&mut self, attachment: impl Into<EmailBodyPart>) -> &mut Self {
        self.attachments
            .get_or_insert_with(Vec::new)
            .push(attachment.into());
        self
    }

    pub(crate) fn header(&mut self, header: Header, value: impl Into<HeaderValue>) -> &mut Self {
        self.headers.insert(header, Some(value.into()));
        self
    }

    // A validated timestamp prevents an invalid send date becoming 1970.
    pub(crate) fn received_at(&mut self, received_at: jiff::Timestamp) -> &mut Self {
        self.received_at = Some(received_at);
        self
    }
}

impl EmailPatch {
    /// Set/clear a single mailbox membership via dotted-path patch.
    pub(crate) fn mailbox_id(&mut self, mailbox_id: &MailboxId, set: bool) -> &mut Self {
        self.mailbox_ids = None;
        self.remove_patch_property("mailboxIds");
        IdSetPatch::member_entries(
            self.patch.get_or_insert_with(HashMap::new),
            "mailboxIds",
            mailbox_id.as_str(),
            set,
        );
        self
    }

    pub(crate) fn mailbox_ids<T, U>(&mut self, mailbox_ids: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<MailboxId>,
    {
        self.mailbox_ids = Some(IdSet::collect(mailbox_ids.into_iter().map(Into::into)));
        self.remove_patch_entries("mailboxIds");
        self
    }

    /// Set/clear a single keyword via dotted-path patch.
    pub(crate) fn keyword(&mut self, keyword: &super::keyword::Keyword, set: bool) -> &mut Self {
        self.keywords = None;
        self.remove_patch_property("keywords");
        IdSetPatch::member_entries(
            self.patch.get_or_insert_with(HashMap::new),
            "keywords",
            keyword.as_str(),
            set,
        );
        self
    }

    pub(crate) fn keywords<T, U>(&mut self, keywords: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<super::keyword::Keyword>,
    {
        self.keywords = Some(IdSet::collect(keywords.into_iter().map(Into::into)));
        self.remove_patch_entries("keywords");
        self
    }

    pub(crate) fn subject(&mut self, subject: impl Into<String>) -> &mut Self {
        self.remove_patch_entries("subject");
        self.subject = Some(subject.into());
        self
    }

    pub(crate) fn raw_property<T: Serialize>(
        &mut self,
        property: impl Into<String>,
        value: &T,
    ) -> serde_json::Result<&mut Self> {
        let value = serde_json::to_value(value)?;
        let property = property.into();
        self.clear_typed_set(&property);
        patch::insert(self.patch.get_or_insert_with(HashMap::new), property, value);
        Ok(self)
    }

    pub(crate) fn null_property(&mut self, property: impl Into<String>) -> &mut Self {
        let property = property.into();
        self.clear_typed_set(&property);
        patch::insert(
            self.patch.get_or_insert_with(HashMap::new),
            property,
            serde_json::Value::Null,
        );
        self
    }

    fn clear_typed_set(&mut self, path: &str) {
        match path.split('/').next() {
            Some("mailboxIds") => self.mailbox_ids = None,
            Some("keywords") => self.keywords = None,
            Some("subject") => self.subject = None,
            _ => {}
        }
    }

    fn remove_patch_property(&mut self, property: &str) {
        if let Some(patch) = &mut self.patch {
            patch.remove(property);
        }
    }

    /// Drop the raw entry for `property` and every dotted path into it.
    ///
    /// RFC 8620 s5.3 forbids a PatchObject from naming both a property
    /// and a pointer inside it, so a wholesale setter has to clear the
    /// exact key a previous `null_property`/`raw_property` installed as
    /// well as the children a previous path setter installed. Otherwise
    /// the flattened map and the typed field both serialise `keywords`
    /// and the request carries duplicate, conflicting keys.
    fn remove_patch_entries(&mut self, property: &str) {
        if let Some(patch) = &mut self.patch {
            patch::remove(patch, property);
        }
    }

    /// The `onSuccessUpdateEmail` patch for a message that has just
    /// been submitted: relocate it to the Sent mailbox and stop it
    /// being a draft.
    ///
    /// Every edit is a dotted-path patch, matching RFC 8621 s7.5's own
    /// submission example (`mailboxIds/{draftsId}: null`,
    /// `mailboxIds/{sentId}: true`, `keywords/$draft: null`). Assigning
    /// `mailboxIds` as a whole value instead would be a *replacement*,
    /// silently dropping any unrelated mailbox the submitted message
    /// also belongs to - a user-filed label on a JMAP server that
    /// models labels as mailboxes, or a shared-folder membership.
    ///
    /// Clearing `$draft` is not optional either: RFC 8621 s4.1.1 makes
    /// the keyword the authoritative "this is a draft" signal (mailbox
    /// membership is a consequence, not the cause), so a submitted
    /// message that keeps the keyword is listed under BOTH Drafts and
    /// Sent by any client that filters on it.
    ///
    /// `drafts` is `None` when the caller could not resolve a Drafts
    /// mailbox; the message then stays wherever it was and merely gains
    /// Sent, which is the safe degradation (a stale Drafts membership
    /// is recoverable; a dropped one is not).
    pub(crate) fn submitted_to_sent(
        &mut self,
        sent: &MailboxId,
        drafts: Option<&MailboxId>,
    ) -> &mut Self {
        if let Some(drafts) = drafts
            && drafts != sent
        {
            self.mailbox_id(drafts, false);
        }
        self.mailbox_id(sent, true);
        self.keyword(
            &super::keyword::Keyword::parse(super::DRAFT_KEYWORD).expect("valid standard keyword"),
            false,
        )
    }
}

impl EmailBodyPart {
    pub(crate) fn new() -> EmailBodyPart {
        EmailBodyPart::default()
    }

    pub(crate) fn with_part_id(mut self, part_id: impl Into<String>) -> Self {
        self.part_id = Some(part_id.into());
        self
    }

    pub(crate) fn with_blob_id(mut self, blob_id: impl Into<BlobId>) -> Self {
        self.blob_id = Some(blob_id.into());
        self
    }

    pub(crate) fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.type_ = Some(content_type.into());
        self
    }

    pub(crate) fn with_content_id(mut self, content_id: impl Into<String>) -> Self {
        self.cid = Some(content_id.into());
        self
    }

    pub(crate) fn with_content_language<T, U>(mut self, content_language: T) -> Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.language = Some(
            content_language
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        );
        self
    }

    pub(crate) fn with_content_location(mut self, content_location: impl Into<String>) -> Self {
        self.location = Some(content_location.into());
        self
    }

    pub(crate) fn with_sub_part(mut self, sub_part: EmailBodyPart) -> Self {
        self.sub_parts.get_or_insert_with(Vec::new).push(sub_part);
        self
    }
}

impl From<String> for EmailBodyValue {
    fn from(value: String) -> Self {
        EmailBodyValue {
            value,
            is_encoding_problem: None,
            is_truncated: None,
        }
    }
}

impl From<&str> for EmailBodyValue {
    fn from(value: &str) -> Self {
        EmailBodyValue {
            value: value.to_string(),
            is_encoding_problem: None,
            is_truncated: None,
        }
    }
}

impl EmailAddress {
    pub(crate) fn new(email: String) -> EmailAddress {
        EmailAddress { name: None, email }
    }

    pub(crate) fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
}

impl From<String> for EmailAddress {
    fn from(email: String) -> Self {
        EmailAddress { name: None, email }
    }
}

impl From<(String, String)> for EmailAddress {
    fn from(parts: (String, String)) -> Self {
        EmailAddress {
            name: parts.0.into(),
            email: parts.1,
        }
    }
}

impl From<&str> for EmailAddress {
    fn from(email: &str) -> Self {
        EmailAddress {
            name: None,
            email: email.to_string(),
        }
    }
}

impl From<(&str, &str)> for EmailAddress {
    fn from(parts: (&str, &str)) -> Self {
        EmailAddress {
            name: parts.0.to_string().into(),
            email: parts.1.to_string(),
        }
    }
}

impl EmailAddressGroup {
    pub(crate) fn new() -> EmailAddressGroup {
        EmailAddressGroup {
            name: None,
            addresses: Vec::new(),
        }
    }

    pub(crate) fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn with_address(mut self, address: impl Into<EmailAddress>) -> Self {
        self.addresses.push(address.into());
        self
    }
}

impl Default for EmailAddressGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl EmailHeader {
    pub(crate) fn new(name: String, value: String) -> EmailHeader {
        EmailHeader { name, value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submitted_to_sent_moves_and_clears_the_draft_keyword() {
        let mut patch = EmailPatch::default();
        patch.submitted_to_sent(&MailboxId::new("sent-1"), Some(&MailboxId::new("drafts-1")));
        let json = serde_json::to_value(&patch).expect("serializable patch");

        assert_eq!(
            json.get("mailboxIds/sent-1"),
            Some(&serde_json::Value::Bool(true)),
            "the sent message must land in the Sent mailbox"
        );
        assert_eq!(
            json.get("mailboxIds/drafts-1"),
            Some(&serde_json::Value::Null),
            "the draft must leave Drafts"
        );
        assert_eq!(
            json.get("keywords/$draft"),
            Some(&serde_json::Value::Null),
            "a submitted message that keeps $draft shows under Drafts AND Sent"
        );
        // RFC 8620 s5.3: a property and a patch path into that same
        // property must not both appear. Neither `keywords` nor
        // `mailboxIds` wholesale may show up alongside their paths.
        assert!(json.get("keywords").is_none());
        assert!(json.get("mailboxIds").is_none());
    }

    /// Patch paths are JSON Pointers (RFC 8620 s5.3 / RFC 6901), and IMAP
    /// keywords legally contain `/` (atoms exclude parens, `%`, `*`, `"`,
    /// `\` - not `/`). Unescaped, `keywords/work/urgent` names a nested key
    /// inside the keyword map's `work` entry: a strict server answers
    /// `invalidPatch`, a lenient one misapplies it.
    #[test]
    fn keyword_and_mailbox_patch_paths_escape_json_pointer_tokens() {
        let mut patch = EmailPatch::default();
        patch.keyword(
            &crate::email::keyword::Keyword::parse("work/urgent").unwrap(),
            true,
        );
        let json = serde_json::to_value(&patch).expect("serializable patch");
        assert_eq!(
            json.get("keywords/work~1urgent"),
            Some(&serde_json::Value::Bool(true))
        );

        let mut patch = EmailPatch::default();
        patch.keyword(
            &crate::email::keyword::Keyword::parse("odd~flag").unwrap(),
            false,
        );
        let json = serde_json::to_value(&patch).expect("serializable patch");
        assert_eq!(
            json.get("keywords/odd~0flag"),
            Some(&serde_json::Value::Null)
        );

        let mut patch = EmailPatch::default();
        patch.mailbox_id(&MailboxId::new("box/nested"), true);
        let json = serde_json::to_value(&patch).expect("serializable patch");
        assert_eq!(
            json.get("mailboxIds/box~1nested"),
            Some(&serde_json::Value::Bool(true))
        );
    }

    #[test]
    fn submitted_to_sent_does_not_replace_unrelated_memberships() {
        // Replacing `mailboxIds` as a whole value would drop every
        // membership the caller did not name. Only the two paths the
        // submission actually owns may appear.
        let mut patch = EmailPatch::default();
        patch.submitted_to_sent(&MailboxId::new("sent-1"), Some(&MailboxId::new("drafts-1")));
        let paths = patch.patch.as_ref().expect("patch paths");
        assert_eq!(paths.len(), 3);
        assert!(patch.mailbox_ids.is_none());
    }

    #[test]
    fn submitted_to_sent_without_a_drafts_mailbox_only_adds_sent() {
        let mut patch = EmailPatch::default();
        patch.submitted_to_sent(&MailboxId::new("sent-1"), None);
        let json = serde_json::to_value(&patch).expect("serializable patch");
        assert_eq!(
            json.get("mailboxIds/sent-1"),
            Some(&serde_json::Value::Bool(true))
        );
        assert_eq!(json.as_object().map(serde_json::Map::len), Some(2));
    }

    #[test]
    fn submitted_to_sent_never_adds_and_removes_the_same_mailbox() {
        // A server whose Drafts and Sent are the same mailbox must not
        // get `true` and `null` for one path in one patch.
        let mut patch = EmailPatch::default();
        patch.submitted_to_sent(&MailboxId::new("one"), Some(&MailboxId::new("one")));
        let json = serde_json::to_value(&patch).expect("serializable patch");
        assert_eq!(
            json.get("mailboxIds/one"),
            Some(&serde_json::Value::Bool(true))
        );
    }
}
