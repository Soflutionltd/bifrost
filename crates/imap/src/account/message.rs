//! Typed message and thread hydration, including MIME body projection.

use super::error::{expunged_message, folder_err, folder_preflight_err, pim_malformed};
use super::mail_actions::IMPORTANCE_KEYWORD;
use super::message_targets::{ThreadScope, group_by_folder, pim_operand, resolve_thread_members};
use super::{
    DecodedObjectId, ImapAccount, decode_object_id, encode_object_id, selected_uidvalidity,
};
use crate::types::{FetchAttr, MailboxName};
use bifrost_types::compose::Address;
use bifrost_types::container::ContainerId;
use bifrost_types::hydration::{
    AttachmentSource, HydrationProjection, Importance, Message, MessageAttachment, ThreadHydration,
};
use bifrost_types::ids::{ObjectId, ThreadId};
use bifrost_types::mime::{Defect, MimeLimits, SelectOptions, parse_message, select_body_with};
use bifrost_types::{AccountError, AccountFuture, AccountOperation};

pub(crate) fn thread_hydrate(
    account: ImapAccount,
    thread: ThreadId,
) -> AccountFuture<Result<ThreadHydration, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.thread_hydrate {
            return Err(super::error::unsupported(AccountOperation::HydrateThread));
        }
        let members = resolve_thread_members(
            &account,
            &thread,
            ThreadScope::Account,
            AccountOperation::HydrateThread,
        )
        .await?;
        let messages = hydrate_decoded(
            &account,
            members,
            HydrationProjection::Full,
            AccountOperation::HydrateThread,
        )
        .await?;
        Ok(ThreadHydration {
            id: thread,
            messages,
        })
    })
}

pub(crate) fn message_hydrate(
    account: ImapAccount,
    message: ObjectId,
    projection: HydrationProjection,
) -> AccountFuture<Result<Message, AccountError>> {
    Box::pin(async move {
        if !account.capabilities.pim_methods.message_hydrate {
            return Err(super::error::unsupported(AccountOperation::HydrateMessage));
        }
        let decoded = decode_object_id(&message, AccountOperation::HydrateMessage)?;
        // The canonical spelling of the requested id, compared against the
        // ids `fetch_to_message` mints. `hydrate_decoded` already drops
        // FETCH responses for UIDs nobody asked about; picking by id rather
        // than by position keeps the answer right independently of that.
        // The answer then carries the id as submitted (a legacy `imap1` or
        // non-canonical spelling included), so the caller gets back the key
        // it asked under, matching the batch hydration lane.
        let wanted = encode_object_id(
            &decoded.folder,
            decoded.uidvalidity,
            crate::types::Uid::new(decoded.uid.get()).expect("wire UID is nonzero"),
        );
        let messages = hydrate_decoded(
            &account,
            vec![decoded],
            projection,
            AccountOperation::HydrateMessage,
        )
        .await?;
        let mut found = messages
            .into_iter()
            .find(|hydrated| hydrated.id == wanted)
            .ok_or_else(|| expunged_message(message.clone()))?;
        found.id = message;
        Ok(found)
    })
}

async fn hydrate_decoded(
    account: &ImapAccount,
    ids: Vec<DecodedObjectId>,
    projection: HydrationProjection,
    op: AccountOperation,
) -> Result<Vec<Message>, AccountError> {
    let mut messages = Vec::new();
    for (folder, ids) in group_by_folder(ids) {
        let err = folder_err(account, &folder, op);
        let preflight = folder_preflight_err(account, &folder, op);
        let mut conn = account
            .checkout_for_folder(&folder)
            .await
            .map_err(preflight)?;
        let selected = account
            .select_folder(&mut conn, &folder, super::SelectPurpose::Read)
            .await
            .map_err(preflight)?;
        let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(preflight)?;
        let operand = pim_operand(valid_uids(ids, uidvalidity, op)?, op)?;
        let Some(uid_set) = operand.uid_set() else {
            continue;
        };
        let mut attrs = attrs_for_hydration(projection);
        super::hydration::push_native_thread_attrs(&mut attrs, &conn.connection().server_profile());
        let fetches = conn
            .connection()
            .uid_fetch(uid_set.as_sequence_set(), &attrs, account.command_timeout())
            .await
            .map_err(err)?;
        let merged = super::get::merge_requested_fetches(fetches, operand.uids(), |_, _| {
            Ok::<(), crate::Error>(())
        })
        .map_err(err)?;
        messages.extend(
            merged
                .into_values()
                .filter_map(|fetch| fetch_to_message(&folder, uidvalidity, fetch, projection)),
        );
    }
    messages.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    Ok(messages)
}

fn valid_uids(
    ids: Vec<DecodedObjectId>,
    uidvalidity: crate::types::UidValidity,
    op: AccountOperation,
) -> Result<Vec<u32>, AccountError> {
    let mut uids = Vec::new();
    for id in ids {
        if id.uidvalidity != uidvalidity {
            // This is the hydration read guard. Writes use the shared
            // mutation engine's per-target stale-epoch classification.
            return Err(pim_malformed(
                op,
                "UIDVALIDITY changed before IMAP operation",
            ));
        }
        uids.push(id.uid.get());
    }
    Ok(uids)
}

fn body_for_hydration(projection: HydrationProjection) -> super::hydration::BodySelection {
    use super::hydration::BodySelection;
    match projection {
        HydrationProjection::Headers => BodySelection::None,
        HydrationProjection::Preview(limit) => BodySelection::Preview(limit),
        HydrationProjection::Full | HydrationProjection::FullWithBlobs => BodySelection::Whole,
        // Not dead code: `HydrationProjection` is `#[non_exhaustive]` and that
        // attribute is scoped per crate, so a match in bifrost-imap on an enum
        // defined in bifrost-types needs a wildcard even when the named arms
        // are exhaustive in practice.
        _ => BodySelection::None,
    }
}

fn attrs_for_hydration(projection: HydrationProjection) -> Vec<FetchAttr> {
    super::hydration::FetchSelection {
        flags: true,
        envelope: true,
        size: true,
        modseq: false,
        body: body_for_hydration(projection),
    }
    .attributes()
}

fn fetch_to_message(
    folder: &MailboxName,
    uidvalidity: crate::types::UidValidity,
    mut fetch: crate::types::FetchResponse,
    projection: HydrationProjection,
) -> Option<Message> {
    let uid = fetch.uid?;
    let envelope = fetch.envelope.clone();
    let raw_body =
        super::hydration::decode_selected_body(&mut fetch, body_for_hydration(projection));
    let parsed = raw_body.as_deref().map(parse_message);
    let decoded = parsed.as_ref().map(|message| {
        select_body_with(
            message,
            SelectOptions {
                include_attachment_bytes: matches!(projection, HydrationProjection::FullWithBlobs),
                max_body_bytes: match projection {
                    HydrationProjection::Preview(limit) => Some(limit),
                    _ => None,
                },
                limits: MimeLimits::default(),
            },
        )
    });
    let incomplete = parsed
        .as_ref()
        .is_some_and(|message| message.defects.iter().any(is_incomplete_defect))
        || decoded
            .as_ref()
            .is_some_and(|body| body.defects.iter().any(is_incomplete_defect));
    // Unlike inventory and hydration, a PIM read is a one-off display answer
    // that writes no cached flag state, so a reply missing FLAGS still shows
    // the message (with no flags) rather than hiding it: an enterprise user
    // asking for a message expects to see it. The sync lanes, whose output
    // would overwrite cached read/flagged state, are the strict ones.
    let flags = super::inventory::flags_set(fetch.flags.as_deref().unwrap_or(&[]));
    let importance = if flags.contains(IMPORTANCE_KEYWORD) {
        Importance::High
    } else {
        Importance::Normal
    };
    Some(Message {
        id: encode_object_id(folder, uidvalidity, crate::types::Uid::new(uid)?),
        thread_id: super::envelope::encode_server_thread_id(
            fetch.thread_id.as_deref(),
            fetch.gmail_thread_id,
        ),
        from: envelope
            .as_ref()
            .map(|env| addresses(&env.from))
            .unwrap_or_default(),
        to: envelope
            .as_ref()
            .map(|env| addresses(&env.to))
            .unwrap_or_default(),
        cc: envelope
            .as_ref()
            .map(|env| addresses(&env.cc))
            .unwrap_or_default(),
        bcc: envelope
            .as_ref()
            .map(|env| addresses(&env.bcc))
            .unwrap_or_default(),
        reply_to: envelope
            .as_ref()
            .map(|env| addresses(&env.reply_to))
            .unwrap_or_default(),
        subject: envelope.as_ref().and_then(|env| env.subject.clone()),
        date: parsed
            .as_ref()
            .and_then(|message| message.headers.date("Date")),
        containers: vec![ContainerId(folder.account_id())],
        importance,
        flags,
        body_text: decoded.as_ref().and_then(|body| body.text.clone()),
        body_html: decoded.as_ref().and_then(|body| body.html.clone()),
        attachments: match projection {
            HydrationProjection::Full | HydrationProjection::FullWithBlobs => decoded
                .as_ref()
                .map(|body| {
                    body.attachments
                        .iter()
                        .map(|attachment| MessageAttachment {
                            filename: attachment.filename.clone(),
                            content_type: Some(attachment.content_type.clone()),
                            content_id: attachment.content_id.clone(),
                            inline: attachment.inline,
                            size: Some(attachment.size),
                            source: attachment
                                .data
                                .clone()
                                .map(AttachmentSource::Inline)
                                .unwrap_or(AttachmentSource::None),
                            truncated: attachment.truncated,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            // `Headers` and `Preview` carry no attachment list, and the
            // wildcard is required because the enum is `#[non_exhaustive]`
            // outside its defining crate.
            _ => Vec::new(),
        },
        size_bytes: fetch.rfc822_size,
        in_reply_to: envelope
            .as_ref()
            .and_then(|env| env.first_in_reply_to().map(str::to_owned)),
        references: parsed
            .as_ref()
            .map(|message| message.headers.message_ids("References"))
            .unwrap_or_default(),
        incomplete,
    })
}

fn is_incomplete_defect(defect: &Defect) -> bool {
    matches!(
        defect,
        Defect::Truncated
            | Defect::TextTruncated
            | Defect::DepthExceeded
            | Defect::PartCountExceeded
            | Defect::HeaderBlockTooLarge
    )
}

fn addresses(addresses: &[crate::types::EnvelopeAddress]) -> Vec<Address> {
    addresses
        .iter()
        .filter_map(crate::types::EnvelopeAddress::to_message_address)
        .map(|addr| Address {
            name: addr.name,
            address: addr.email,
        })
        .collect()
}

#[cfg(test)]
mod tests;
