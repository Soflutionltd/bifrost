//! Compose body construction and account-scoped attachment handles.

use super::{MailAccount, to_acct_err};
use crate::core::SetCreate;
use crate::core::transport::HttpTransport;
use crate::email::{
    DRAFT_KEYWORD, EmailAddress as JmapEmailAddress, EmailBodyPart, EmailBodyValue, EmailPatch,
};
use crate::mailbox::MailboxId;
use crate::sync::error::invalid_attachment_handle;
use bifrost_types::{AccountError, AccountOperation};
use std::collections::HashMap;

const ATTACHMENT_HANDLE_PREFIX: &str = "jmap:";

pub(super) async fn build_email_create_from_send<T: HttpTransport>(
    mail: &MailAccount<T>,
    request: bifrost_types::SendRequest,
    mailbox: MailboxId,
) -> Result<crate::email::EmailCreate, AccountError> {
    // RFC 8098 read receipt: targets the resolved sender. JMAP carries
    // arbitrary headers as structured Email properties, so this becomes a
    // `Disposition-Notification-To` address header on the create. Captured
    // before `request` is consumed field-by-field below.
    let read_receipt_to = request
        .request_read_receipt
        .then(|| request.from.clone())
        .flatten();
    let mut patch = bifrost_types::DraftPatch::default();
    patch.identity = request.identity;
    patch.from = Some(request.from);
    patch.to = Some(request.to);
    patch.cc = Some(request.cc);
    patch.bcc = Some(request.bcc);
    patch.reply_to = Some(request.reply_to);
    patch.subject = Some(request.subject);
    patch.body_text = Some(request.body_text);
    patch.body_html = Some(request.body_html);
    patch.attachments_inline = Some(request.attachments_inline);
    patch.attachments_uploaded = Some(request.attachments_uploaded);
    patch.in_reply_to = Some(request.in_reply_to);
    patch.references = Some(request.references);
    let mut create =
        build_email_create_from_draft(mail, patch, mailbox, AccountOperation::Send).await?;
    if let Some(addr) = read_receipt_to {
        create.header(
            crate::email::Header {
                name: "Disposition-Notification-To".to_string(),
                form: crate::email::HeaderForm::Addresses,
                all: false,
            },
            crate::email::HeaderValue::AsAddresses(vec![address_to_jmap(addr)]),
        );
    }
    Ok(create)
}

pub(super) async fn build_email_create_from_draft<T: HttpTransport>(
    mail: &MailAccount<T>,
    patch: bifrost_types::DraftPatch,
    mailbox: MailboxId,
    op: AccountOperation,
) -> Result<crate::email::EmailCreate, AccountError> {
    let mut create = crate::email::EmailCreate::new(None);
    let body_patch = patch.clone();
    create.mailbox_ids([mailbox]);
    create.keywords([
        crate::email::keyword::Keyword::parse(DRAFT_KEYWORD).expect("valid standard keyword")
    ]);
    if let Some(from) = patch.from.flatten() {
        create.from([address_to_jmap(from)]);
    }
    if let Some(to) = patch.to {
        create.to(to.into_iter().map(address_to_jmap));
    }
    if let Some(cc) = patch.cc {
        create.cc(cc.into_iter().map(address_to_jmap));
    }
    if let Some(bcc) = patch.bcc {
        create.bcc(bcc.into_iter().map(address_to_jmap));
    }
    if let Some(reply_to) = patch.reply_to {
        create.reply_to(reply_to.into_iter().map(address_to_jmap));
    }
    if let Some(Some(subject)) = patch.subject {
        create.subject(subject);
    }
    if let Some(Some(in_reply_to)) = patch.in_reply_to {
        create.in_reply_to([in_reply_to]);
    }
    if let Some(references) = patch.references {
        create.references(references);
    }
    apply_body_to_create(mail, &mut create, body_patch, op).await?;
    Ok(create)
}

async fn apply_body_to_create<T: HttpTransport>(
    mail: &MailAccount<T>,
    create: &mut crate::email::EmailCreate,
    patch: bifrost_types::DraftPatch,
    op: AccountOperation,
) -> Result<(), AccountError> {
    let body = build_body(
        mail,
        patch.body_text.flatten(),
        patch.body_html.flatten(),
        patch.attachments_inline.unwrap_or_default(),
        patch.attachments_uploaded.unwrap_or_default(),
        op,
    )
    .await?;
    if let Some(structure) = body.body_structure {
        create.body_structure(structure);
    }
    for (id, value) in body.body_values {
        create.body_value(id, value);
    }
    for part in body.text_body {
        create.text_body(part);
    }
    for part in body.html_body {
        create.html_body(part);
    }
    for part in body.attachments {
        create.attachment(part);
    }
    Ok(())
}

pub(super) async fn apply_draft_patch_to_email_patch<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_patch: &mut EmailPatch,
    patch: bifrost_types::DraftPatch,
    op: AccountOperation,
) -> Result<(), AccountError> {
    if let Some(from) = patch.from {
        match from {
            Some(value) => {
                email_patch
                    .raw_property("from", &vec![address_to_jmap(value)])
                    .map_err(crate::Error::RequestEncode)
                    .map_err(to_acct_err(op))?;
            }
            None => {
                email_patch.null_property("from");
            }
        }
    }
    if let Some(to) = patch.to {
        let values = to.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("to", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(cc) = patch.cc {
        let values = cc.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("cc", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(bcc) = patch.bcc {
        let values = bcc.into_iter().map(address_to_jmap).collect::<Vec<_>>();
        email_patch
            .raw_property("bcc", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(reply_to) = patch.reply_to {
        let values = reply_to
            .into_iter()
            .map(address_to_jmap)
            .collect::<Vec<_>>();
        email_patch
            .raw_property("replyTo", &values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if let Some(subject) = patch.subject {
        if let Some(subject) = subject {
            email_patch.subject(subject);
        } else {
            email_patch.null_property("subject");
        }
    }
    if let Some(in_reply_to) = patch.in_reply_to {
        if let Some(in_reply_to) = in_reply_to {
            email_patch
                .raw_property("inReplyTo", &vec![in_reply_to])
                .map_err(crate::Error::RequestEncode)
                .map_err(to_acct_err(op))?;
        } else {
            email_patch.null_property("inReplyTo");
        }
    }
    if let Some(references) = patch.references {
        email_patch
            .raw_property("references", &references)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    if patch.body_text.is_some()
        || patch.body_html.is_some()
        || patch.attachments_inline.is_some()
        || patch.attachments_uploaded.is_some()
    {
        let body = build_body(
            mail,
            patch.body_text.flatten(),
            patch.body_html.flatten(),
            patch.attachments_inline.unwrap_or_default(),
            patch.attachments_uploaded.unwrap_or_default(),
            op,
        )
        .await?;
        if let Some(structure) = body.body_structure {
            email_patch
                .raw_property("bodyStructure", &structure)
                .map_err(crate::Error::RequestEncode)
                .map_err(to_acct_err(op))?;
        }
        email_patch
            .raw_property("bodyValues", &body.body_values)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
        email_patch
            .raw_property("textBody", &body.text_body)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
        email_patch
            .raw_property("htmlBody", &body.html_body)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
        email_patch
            .raw_property("attachments", &body.attachments)
            .map_err(crate::Error::RequestEncode)
            .map_err(to_acct_err(op))?;
    }
    Ok(())
}

pub(super) struct BuiltBody {
    pub(super) body_structure: Option<EmailBodyPart>,
    pub(super) body_values: HashMap<String, EmailBodyValue>,
    pub(super) text_body: Vec<EmailBodyPart>,
    pub(super) html_body: Vec<EmailBodyPart>,
    pub(super) attachments: Vec<EmailBodyPart>,
}

pub(super) async fn build_body<T: HttpTransport>(
    mail: &MailAccount<T>,
    text: Option<String>,
    html: Option<String>,
    inline: Vec<bifrost_types::AttachmentInline>,
    uploaded: Vec<bifrost_types::AttachmentHandle>,
    op: AccountOperation,
) -> Result<BuiltBody, AccountError> {
    let mut body_values = HashMap::new();
    let mut text_body = Vec::new();
    let mut html_body = Vec::new();
    let mut body_parts = Vec::new();

    if let Some(text) = text {
        let part = EmailBodyPart::new()
            .with_part_id("text")
            .with_content_type("text/plain");
        body_values.insert("text".to_string(), EmailBodyValue::from(text));
        text_body.push(part.clone());
        body_parts.push(part);
    }
    if let Some(html) = html {
        let part = EmailBodyPart::new()
            .with_part_id("html")
            .with_content_type("text/html");
        body_values.insert("html".to_string(), EmailBodyValue::from(html));
        html_body.push(part.clone());
        body_parts.push(part);
    }
    if body_parts.is_empty() {
        let part = EmailBodyPart::new()
            .with_part_id("text")
            .with_content_type("text/plain");
        body_values.insert("text".to_string(), EmailBodyValue::from(""));
        text_body.push(part.clone());
        body_parts.push(part);
    }

    let mut attachments = Vec::new();
    for attachment in inline {
        let blob = mail
            .upload(attachment.data.to_vec(), Some(&attachment.mime))
            .await
            .map_err(to_acct_err(op))?;
        let mut part = EmailBodyPart::new()
            .with_blob_id(blob.blob_id)
            .with_name(attachment.filename)
            .with_content_type(attachment.mime);
        // Carry the Content-ID so a `cid:` reference in the HTML body
        // resolves to this part. JMAP's `cid` property is the bare token
        // (no angle brackets); strip any the caller supplied.
        if let Some(cid) = attachment.content_id.as_deref().map(|cid| {
            cid.trim()
                .trim_matches(|c| c == '<' || c == '>')
                .to_string()
        }) && !cid.is_empty()
        {
            part = part.with_content_id(cid);
        }
        attachments.push(part);
    }
    for handle in uploaded {
        let Some((source_account, blob_id, mime)) = decode_attachment_handle(&handle.0) else {
            return Err(invalid_attachment_handle(op));
        };
        let blob_id = copy_attachment_blob(mail, &source_account, blob_id, op).await?;
        let mut part = EmailBodyPart::new().with_blob_id(blob_id);
        if let Some(mime) = mime {
            part = part.with_content_type(mime);
        }
        attachments.push(part);
    }

    let content = if body_parts.len() == 1 {
        body_parts.remove(0)
    } else {
        body_parts.into_iter().fold(
            EmailBodyPart::new().with_content_type("multipart/alternative"),
            EmailBodyPart::with_sub_part,
        )
    };
    let body_structure = if attachments.is_empty() {
        Some(content)
    } else {
        let mut mixed = EmailBodyPart::new()
            .with_content_type("multipart/mixed")
            .with_sub_part(content);
        for attachment in &attachments {
            mixed = mixed.with_sub_part(attachment.clone());
        }
        Some(mixed)
    };

    Ok(BuiltBody {
        body_structure,
        body_values,
        text_body,
        html_body,
        attachments,
    })
}

pub(super) fn encode_attachment_handle(
    account_id: &str,
    blob_id: &str,
    mime: Option<&str>,
) -> String {
    format!(
        "{ATTACHMENT_HANDLE_PREFIX}v2\n{}",
        serde_json::to_string(&(account_id, blob_id, mime))
            .expect("attachment handle fields serialize")
    )
}

pub(super) fn decode_attachment_handle(handle: &str) -> Option<(String, String, Option<String>)> {
    let payload = handle
        .strip_prefix(ATTACHMENT_HANDLE_PREFIX)?
        .strip_prefix("v2\n")?;
    let (account, blob, mime): (String, String, Option<String>) =
        serde_json::from_str(payload).ok()?;
    if account.is_empty() || blob.is_empty() {
        return None;
    }
    Some((account, blob, mime))
}

async fn copy_attachment_blob<T: HttpTransport>(
    mail: &MailAccount<T>,
    source_account: &str,
    blob_id: String,
    op: AccountOperation,
) -> Result<crate::core::id::BlobId, AccountError> {
    let blob_id = crate::core::id::BlobId::new(blob_id);
    if source_account == mail.id_str() {
        return Ok(blob_id);
    }

    let mut response = mail
        .core()
        .call(crate::blob::copy::CopyBlobRequest::new(source_account).blob_id(blob_id.clone()))
        .await
        .map_err(to_acct_err(op))?;
    if response.from_account_id().as_str() != source_account
        || response.account_id().as_str() != mail.id_str()
    {
        // A readable answer for different accounts cannot establish this copy.
        // This is a broken response contract, not a blob-format parse failure.
        return Err(crate::sync::error::contract_violation(
            op,
            None,
            "Blob/copy response named accounts other than the requested source and destination",
        ));
    }
    response.copied(&blob_id).map_err(to_acct_err(op))
}

pub(super) fn address_to_jmap(address: bifrost_types::Address) -> JmapEmailAddress {
    let mut out = JmapEmailAddress::new(address.address);
    if let Some(name) = address.name {
        out = out.with_name(name);
    }
    out
}

pub(super) fn address_from_jmap(address: &JmapEmailAddress) -> bifrost_types::Address {
    bifrost_types::Address {
        name: address.name().map(str::to_string),
        address: address.email().to_string(),
    }
}
