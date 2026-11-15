//! Message and conversation target resolution for account actions and hydration.

use super::error::{folder_preflight_err, pim_malformed};
use super::targets::UidOperand;
use super::{
    DecodedObjectId, ImapAccount, decode_object_id, decode_thread_id, selected_uidvalidity,
};
use crate::types::{MailboxAttribute, MailboxName};
use bifrost_types::container::MutationTarget;
use bifrost_types::ids::ThreadId;
use bifrost_types::{AccountError, AccountOperation};
use std::collections::HashMap;

/// Which folders an account-scoped native thread is resolved in for one
/// action. Snapshot handles ignore it: their members are fixed.
#[derive(Debug, Clone, Copy)]
pub(super) enum ThreadScope<'a> {
    /// Every folder that can hold a member (hydration, flag changes).
    Account,
    /// One folder only (removal from that folder).
    Only(&'a MailboxName),
    /// Every member folder except this one (copy into it).
    Except(&'a MailboxName),
}

pub(super) fn is_native_thread(target: &MutationTarget) -> bool {
    matches!(target, MutationTarget::Thread(thread) if thread.0.starts_with("imapnative2:"))
}

/// The folders a native thread's members are searched in. Gmail labels are
/// views of one message store, so X-GM-THRID is searched once in the `\All`
/// folder when the server lists one (searching every label would return the
/// same message once per label); OBJECTID servers hold distinct messages per
/// folder, so THREADID is searched in every selectable folder. The order is
/// the deterministic account order the search pager uses.
fn native_thread_folders(
    entries: Vec<std::sync::Arc<super::folder_registry::FolderEntry>>,
    kind: super::envelope::NativeThreadKind,
    scope: ThreadScope<'_>,
) -> Vec<MailboxName> {
    if let ThreadScope::Only(folder) = scope {
        return vec![folder.clone()];
    }
    let selectable: Vec<_> = entries
        .into_iter()
        .filter(|entry| entry.selectable())
        .collect();
    let all_mail = (kind == super::envelope::NativeThreadKind::Gmail)
        .then(|| {
            selectable.iter().find(|entry| {
                entry
                    .attributes()
                    .iter()
                    .any(|attr| matches!(attr, MailboxAttribute::All))
            })
        })
        .flatten();
    let mut folders: Vec<MailboxName> = match all_mail {
        Some(entry) => vec![entry.name.clone()],
        None => selectable.iter().map(|entry| entry.name.clone()).collect(),
    };
    if let ThreadScope::Except(excluded) = scope {
        folders.retain(|folder| folder != excluded);
    }
    folders.sort_by(MailboxName::cmp_account_order);
    folders
}

/// Resolve a thread handle to its current member ids.
///
/// A snapshot handle decodes to its recorded UIDs. An account-scoped native
/// handle runs one UID SEARCH per folder chosen by `native_thread_folders`
/// under the epoch SELECT reports now; the handle records no UIDVALIDITY, so
/// there is no stale epoch to refuse here. An epoch change between this
/// resolution and the write receives the shared mutation engine's stale-epoch
/// answer. Hydration separately validates its read epoch through `valid_uids`.
pub(super) async fn resolve_thread_members(
    account: &ImapAccount,
    thread: &ThreadId,
    scope: ThreadScope<'_>,
    op: AccountOperation,
) -> Result<Vec<DecodedObjectId>, AccountError> {
    let Some(native) = super::envelope::decode_server_thread_id(thread, op)? else {
        let decoded = decode_thread_id(thread, op)?;
        return Ok(decoded
            .uids
            .identifiers()
            .map(|uid| DecodedObjectId::canonical(decoded.folder.clone(), decoded.uidvalidity, uid))
            .collect());
    };
    let mut members = Vec::new();
    for folder in native_thread_folders(account.folders.entries(), native.kind, scope) {
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
        let uids = conn
            .connection()
            .uid_search(&native.criteria, account.command_timeout())
            .await
            .map_err(preflight)?
            .ids;
        members.extend(uids.into_iter().map(|uid| {
            DecodedObjectId::canonical(
                folder.clone(),
                uidvalidity,
                crate::types::Uid::new(uid).expect("wire UID is nonzero"),
            )
        }));
    }
    Ok(members)
}

pub(super) async fn resolved_targets(
    account: &ImapAccount,
    target: &MutationTarget,
    scope: ThreadScope<'_>,
    op: AccountOperation,
) -> Result<Vec<DecodedObjectId>, AccountError> {
    if let MutationTarget::Thread(thread) = target {
        return resolve_thread_members(account, thread, scope, op).await;
    }
    decoded_targets(target, op)
}

pub(super) fn decoded_targets(
    target: &MutationTarget,
    op: AccountOperation,
) -> Result<Vec<DecodedObjectId>, AccountError> {
    match target {
        MutationTarget::Message(id) => Ok(vec![decode_object_id(id, op)?]),
        MutationTarget::Thread(thread) => {
            let decoded = decode_thread_id(thread, op)?;
            Ok(decoded
                .uids
                .identifiers()
                .map(|uid| {
                    DecodedObjectId::canonical(decoded.folder.clone(), decoded.uidvalidity, uid)
                })
                .collect())
        }
        _ => Err(pim_malformed(
            op,
            "unsupported MutationTarget variant for IMAP per-message operation",
        )),
    }
}

pub(super) fn group_by_folder(
    ids: Vec<DecodedObjectId>,
) -> Vec<(MailboxName, Vec<DecodedObjectId>)> {
    let mut grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)> = HashMap::new();
    for id in ids {
        grouped
            .entry(id.folder.account_id())
            .or_insert_with(|| (id.folder.clone(), Vec::new()))
            .1
            .push(id);
    }
    grouped.into_values().collect()
}

/// Build the wire operand for a PIM primitive, from the same source the
/// batch lanes use.
///
/// These lanes answer `Result<(), AccountError>` for the whole request:
/// there is no per-id outcome lane, so a UID the operand cannot carry has
/// nowhere to be reported. Refuse the command instead of sending an operand
/// narrower than the caller asked for, which would report success for a
/// message the server never saw.
pub(super) fn pim_operand<I: IntoIterator<Item = u32>>(
    uids: I,
    op: AccountOperation,
) -> Result<UidOperand, AccountError> {
    let operand = UidOperand::build(uids);
    if !operand.excluded().is_empty() {
        return Err(pim_malformed(
            op,
            "message id could not be placed in an IMAP UID operand",
        ));
    }
    Ok(operand)
}

#[cfg(test)]
mod tests;
