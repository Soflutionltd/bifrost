use super::{
    Email, EmailAddress, EmailAddressGroup, EmailBodyPart, EmailBodyValue, EmailHeader, EmailId,
    Header, HeaderValue,
};
use crate::core::id::BlobId;
use crate::mailbox::MailboxId;
use crate::thread::ThreadId;

impl Email {
    pub(crate) fn id(&self) -> Option<&EmailId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> EmailId {
        self.id.take().unwrap_or_else(|| EmailId::new(""))
    }

    pub(crate) fn blob_id(&self) -> Option<&BlobId> {
        self.blob_id.as_ref()
    }

    pub(crate) fn take_blob_id(&mut self) -> BlobId {
        self.blob_id.take().unwrap_or_else(|| BlobId::new(""))
    }

    pub(crate) fn thread_id(&self) -> Option<&ThreadId> {
        self.thread_id.as_ref()
    }

    pub(crate) fn take_thread_id(&mut self) -> Option<ThreadId> {
        self.thread_id.take()
    }

    pub(crate) fn mailbox_ids(&self) -> Vec<&MailboxId> {
        self.mailbox_ids
            .as_ref()
            .map(|m| m.iter().filter(|(_, v)| **v).map(|(k, _)| k).collect())
            .unwrap_or_default()
    }

    pub(crate) fn has_keywords(&self) -> bool {
        self.keywords.is_some()
    }

    pub(crate) fn keywords(&self) -> Vec<&str> {
        self.keywords
            .as_ref()
            .map(|k| {
                k.iter()
                    .filter(|(_, v)| **v)
                    .map(|(k, _)| k.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn size(&self) -> usize {
        self.size.unwrap_or(0)
    }

    pub(crate) fn received_at(&self) -> Option<i64> {
        self.received_at.map(jiff::Timestamp::as_second)
    }

    pub(crate) fn message_id(&self) -> Option<&[String]> {
        self.message_id.as_deref()
    }

    pub(crate) fn in_reply_to(&self) -> Option<&[String]> {
        self.in_reply_to.as_deref()
    }

    pub(crate) fn references(&self) -> Option<&[String]> {
        self.references.as_deref()
    }

    pub(crate) fn sender(&self) -> Option<&[EmailAddress]> {
        self.sender.as_deref()
    }

    pub(crate) fn take_sender(&mut self) -> Option<Vec<EmailAddress>> {
        self.sender.take()
    }

    pub(crate) fn from(&self) -> Option<&[EmailAddress]> {
        self.from.as_deref()
    }

    pub(crate) fn take_from(&mut self) -> Option<Vec<EmailAddress>> {
        self.from.take()
    }

    pub(crate) fn reply_to(&self) -> Option<&[EmailAddress]> {
        self.reply_to.as_deref()
    }

    pub(crate) fn take_reply_to(&mut self) -> Option<Vec<EmailAddress>> {
        self.reply_to.take()
    }

    pub(crate) fn to(&self) -> Option<&[EmailAddress]> {
        self.to.as_deref()
    }

    pub(crate) fn take_to(&mut self) -> Option<Vec<EmailAddress>> {
        self.to.take()
    }

    pub(crate) fn cc(&self) -> Option<&[EmailAddress]> {
        self.cc.as_deref()
    }

    pub(crate) fn take_cc(&mut self) -> Option<Vec<EmailAddress>> {
        self.cc.take()
    }

    pub(crate) fn bcc(&self) -> Option<&[EmailAddress]> {
        self.bcc.as_deref()
    }

    pub(crate) fn take_bcc(&mut self) -> Option<Vec<EmailAddress>> {
        self.bcc.take()
    }

    pub(crate) fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    pub(crate) fn take_subject(&mut self) -> Option<String> {
        self.subject.take()
    }

    pub(crate) fn sent_at(&self) -> Option<i64> {
        self.sent_at.map(jiff::Timestamp::as_second)
    }

    pub(crate) fn body_structure(&self) -> Option<&EmailBodyPart> {
        self.body_structure.as_deref()
    }

    pub(crate) fn body_value(&self, id: &str) -> Option<&EmailBodyValue> {
        self.body_values.as_ref().and_then(|v| v.get(id))
    }

    pub(crate) fn text_body(&self) -> Option<&[EmailBodyPart]> {
        self.text_body.as_deref()
    }

    pub(crate) fn html_body(&self) -> Option<&[EmailBodyPart]> {
        self.html_body.as_deref()
    }

    pub(crate) fn attachments(&self) -> Option<&[EmailBodyPart]> {
        self.attachments.as_deref()
    }

    pub(crate) fn has_attachment(&self) -> bool {
        *self.has_attachment.as_ref().unwrap_or(&false)
    }

    pub(crate) fn header(&self, id: &Header) -> Option<&HeaderValue> {
        self.headers.get(id).and_then(|v| v.as_ref())
    }

    pub(crate) fn has_header(&self, id: &Header) -> bool {
        self.headers.contains_key(id)
    }

    pub(crate) fn preview(&self) -> Option<&str> {
        self.preview.as_deref()
    }

    pub(crate) fn take_preview(&mut self) -> Option<String> {
        self.preview.take()
    }

    #[cfg(feature = "debug")]
    pub(crate) fn into_test(self) -> super::TestEmail {
        self.into()
    }
}

impl EmailBodyPart {
    pub(crate) fn part_id(&self) -> Option<&str> {
        self.part_id.as_deref()
    }

    pub(crate) fn blob_id(&self) -> Option<&BlobId> {
        self.blob_id.as_ref()
    }

    pub(crate) fn size(&self) -> usize {
        *self.size.as_ref().unwrap_or(&0)
    }

    pub(crate) fn headers(&self) -> Option<&[EmailHeader]> {
        self.headers.as_deref()
    }

    pub(crate) fn header(&self, id: &Header) -> Option<&HeaderValue> {
        self.header.as_ref().and_then(|v| v.get(id))
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn charset(&self) -> Option<&str> {
        self.charset.as_deref()
    }

    pub(crate) fn content_type(&self) -> Option<&str> {
        self.type_.as_deref()
    }

    pub(crate) fn content_disposition(&self) -> Option<&str> {
        self.disposition.as_deref()
    }

    pub(crate) fn content_id(&self) -> Option<&str> {
        self.cid.as_deref()
    }

    pub(crate) fn content_language(&self) -> Option<&[String]> {
        self.language.as_deref()
    }

    pub(crate) fn content_location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    pub(crate) fn sub_parts(&self) -> Option<&[EmailBodyPart]> {
        self.sub_parts.as_deref()
    }
}

impl EmailBodyValue {
    pub(crate) fn value(&self) -> &str {
        self.value.as_str()
    }

    pub(crate) fn is_encoding_problem(&self) -> bool {
        self.is_encoding_problem.unwrap_or(false)
    }

    pub(crate) fn is_truncated(&self) -> bool {
        self.is_truncated.unwrap_or(false)
    }
}

impl EmailAddress {
    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn email(&self) -> &str {
        self.email.as_str()
    }

    pub(crate) fn unwrap(self) -> (String, Option<String>) {
        (self.email, self.name)
    }
}

impl EmailAddressGroup {
    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn addresses(&self) -> &[EmailAddress] {
        self.addresses.as_ref()
    }
}

impl EmailHeader {
    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    pub(crate) fn value(&self) -> &str {
        self.value.as_str()
    }
}
