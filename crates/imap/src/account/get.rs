use std::collections::{HashMap, HashSet};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, AccountStream,
    BatchFailure, BatchItemId, BatchUncertain, Cause, DiagnosticText, HydratedObject,
    HydratedObjectKind, ItemOutcome, PageBoundary, Projection, Protocol, RequestCause,
    RequestErrorKind, ResourceKind, SyncEvent,
};
use futures::StreamExt;

use crate::types::{FetchAttr, FetchResponse, MailboxName};

use super::hydration::{BodySelection, FetchSelection, decode_body};
use super::inventory::{fetch_to_inventory, flags_set};
#[cfg(test)]
use super::pim::PREVIEW_FETCH_BYTES;
use super::targets::{TargetBatch, Verdict};
use super::{
    BATCH_ITEMS, DecodedObjectId, ImapAccount, batch, decode_object_id, guarded_receiver_stream,
};

/// Memory ceiling for one body-bearing hydration FETCH.
///
/// The engine sizes hydration batches, so this is not a per-message limit
/// but a guard on the batch as a whole: it exists so a corrupt or
/// adversarial server cannot make us buffer an unbounded literal, not to
/// second-guess a legitimate batch of large mail. Crossing it surfaces as
/// `Error::FetchLimit` after the tagged completion, with the IMAP stream
/// still synchronized.
const HYDRATION_FETCH_BUDGET: usize = 256 * 1024 * 1024;

pub(crate) fn get_stream(
    account: ImapAccount,
    mut ids: AccountStream<bifrost_types::ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    let task = tokio::spawn(async move {
        let mut grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)> = HashMap::new();
        let mut buffered = 0usize;
        while let Some(id) = ids.next().await {
            match decode_object_id(&id, AccountOperation::Hydrate) {
                Ok(decoded) => {
                    grouped
                        .entry(decoded.folder.account_id())
                        .or_insert_with(|| (decoded.folder.clone(), Vec::new()))
                        .1
                        .push(decoded);
                    buffered += 1;
                }
                Err(err) => {
                    // Locally-invalid id: surface as per-item Failed
                    // rather than a free-form Warning. Drops the id (a
                    // bifrost-shaped value that the caller will fail to
                    // hydrate elsewhere anyway) into the structured
                    // failure lane.
                    let item_id = BatchItemId(id.0.clone());
                    let outcome = ItemOutcome::Failed(BatchFailure::new(item_id, err));
                    if tx
                        .send(batch(vec![outcome], PageBoundary::Page, None, 0))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            if buffered >= super::TARGET_BUFFER_ITEMS {
                if flush_get_groups(&account, &mut grouped, projection, &tx)
                    .await
                    .is_err()
                {
                    return;
                }
                buffered = 0;
            }
        }
        if flush_get_groups(&account, &mut grouped, projection, &tx)
            .await
            .is_err()
        {
            return;
        }
        let _ = tx.send(SyncEvent::Done(None)).await;
    });
    guarded_receiver_stream(rx, task)
}

async fn flush_get_groups(
    account: &ImapAccount,
    grouped: &mut HashMap<String, (MailboxName, Vec<DecodedObjectId>)>,
    projection: Projection,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<ItemOutcome<HydratedObject>>>,
) -> Result<(), ()> {
    // Folder order is deterministic inside each bounded input window.
    let mut groups: Vec<(MailboxName, Vec<DecodedObjectId>)> =
        std::mem::take(grouped).into_values().collect();
    groups.sort_by(|a, b| a.0.cmp_account_order(&b.0));
    for (folder, ids) in groups {
        let mut pending = ids;
        match run_folder_get(account, &folder, &mut pending, projection, tx).await {
            Ok(()) => {}
            Err(GetError::ChannelDropped) => return Err(()),
            Err(other) => {
                // Per-folder failure does not collapse the stream:
                // still-unresolved items in this folder surface as
                // `Uncertain`, the next folder still runs. Only ids
                // without a published outcome fall into that lane:
                // stale-UIDVALIDITY ids were already emitted as
                // `Failed` before the fallible FETCH, and re-emitting
                // them here would put one id in two lanes. A
                // shared-folder SELECT denial is pre-classified to
                // `ScopeRevoked` so it quarantines that scope rather
                // than masquerading as a generic hydration failure.
                let (account_err, bytes_in) = match other {
                    GetError::Account(err, bytes_in) => (err, bytes_in),
                    GetError::Imap(err, bytes_in) => {
                        let account_err = super::account_error_with(
                            err,
                            super::error::ImapErrorContext::operation(
                                bifrost_types::AccountOperation::Hydrate,
                            )
                            .with_folder_scope(&folder),
                        );
                        (account_err, bytes_in)
                    }
                    GetError::ChannelDropped => unreachable!("handled above"),
                };
                let uncertain: Vec<ItemOutcome<HydratedObject>> = pending
                    .into_iter()
                    .map(|id| {
                        let item = BatchItemId(
                            super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0,
                        );
                        ItemOutcome::Uncertain(BatchUncertain::new(item, account_err.clone()))
                    })
                    .collect();
                if tx
                    .send(batch(uncertain, PageBoundary::Page, None, bytes_in))
                    .await
                    .is_err()
                {
                    return Err(());
                }
            }
        }
    }
    Ok(())
}

/// Per-folder hydration failure: either an IMAP wire error to classify
/// per-item, or a dropped output channel (silent return).
enum GetError {
    Imap(crate::Error, u64),
    /// A pre-classified `AccountError` (shared-folder SELECT denial built
    /// as `ScopeRevoked` rather than letting the raw permission denial
    /// derive account-terminal `NoPermission`).
    Account(bifrost_types::AccountError, u64),
    ChannelDropped,
}

impl From<crate::Error> for GetError {
    fn from(value: crate::Error) -> Self {
        Self::Imap(value, 0)
    }
}

fn uidvalidity_changed_error(folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("UIDVALIDITY changed before hydration"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::Hydrate)
    // Cursor(Folder(..)), not ErrorScope::Mailbox: the folder producers'
    // documented scope shape, so scope readers matching one shape do not
    // silently degrade (mutate.rs builds the identical condition this way).
    .scope(bifrost_types::ErrorScope::Cursor(
        bifrost_types::CursorScope::Folder(bifrost_types::FolderId(folder.account_id())),
    ))
    .try_build()
    .expect("valid account error classification")
}

fn message_not_found_error(id: &DecodedObjectId) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::NotFound(ResourceKind::Message),
        Cause::Request(RequestCause::NotFound {
            what: ResourceKind::Message,
            id: Some(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::Hydrate)
    .scope(bifrost_types::ErrorScope::Message {
        id: super::encode_object_id(&id.folder, id.uidvalidity, id.uid),
    })
    .try_build()
    .expect("valid account error classification")
}

fn failed_hydration(
    ids: impl IntoIterator<Item = DecodedObjectId>,
    error: AccountError,
) -> Vec<ItemOutcome<HydratedObject>> {
    ids.into_iter()
        .map(|id| {
            let item = BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
            ItemOutcome::Failed(BatchFailure::new(item, error.clone()))
        })
        .collect()
}

/// Publish the stale-UIDVALIDITY failures for a folder. Called before the
/// hydration FETCH: these outcomes are already known, and holding them until
/// after a fallible command would lose them whenever that command fails and
/// the folder collapses into the uncertain lane.
async fn emit_stale_failures(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<ItemOutcome<HydratedObject>>>,
    stale: Vec<DecodedObjectId>,
    folder: &MailboxName,
    connection: &crate::ImapConnection,
    previous_bytes_in: &mut u64,
) -> Result<(), GetError> {
    if stale.is_empty() {
        return Ok(());
    }
    let outcomes = failed_hydration(stale, uidvalidity_changed_error(folder));
    let bytes_in = super::measured_bytes_in(connection, previous_bytes_in);
    tx.send(batch(outcomes, PageBoundary::Page, None, bytes_in))
        .await
        .map_err(|_| GetError::ChannelDropped)
}

/// Hydrate one folder's ids. `pending` is the caller's unresolved set: ids
/// whose outcome this function has already published are removed from it,
/// so a folder-level error afterwards downgrades only the ids that still
/// lack a lane. Without that pruning, a FETCH failure after the stale
/// publication would re-emit the stale ids as `Uncertain` on top of their
/// `Failed`, breaking the one-outcome-per-id contract the engine holds.
async fn run_folder_get(
    account: &ImapAccount,
    folder: &MailboxName,
    pending: &mut Vec<DecodedObjectId>,
    projection: Projection,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<ItemOutcome<HydratedObject>>>,
) -> Result<(), GetError> {
    let mut conn = account.checkout_for_folder(folder).await?;
    let mut previous_bytes_in = conn.connection().bytes_in();
    let folder_entry = account.folders.get(folder);
    let cursor = folder_entry.as_ref().and_then(|entry| entry.cursor());
    let shared_owner = folder_entry
        .as_ref()
        .and_then(|entry| entry.shared_owner.clone());
    let selected = match account
        .select_folder(&mut conn, folder, cursor.as_ref(), true)
        .await
    {
        Ok(selected) => selected,
        // A permission denial on a shared folder quarantines just that
        // scope (`ScopeRevoked`) instead of surfacing as a generic
        // hydration failure that could escalate account-wide; a personal
        // folder, or any non-permission failure, flows through the normal
        // mapping.
        Err(err) if shared_owner.is_some() => {
            let bytes_in = super::measured_bytes_in(conn.connection(), &mut previous_bytes_in);
            return Err(GetError::Account(
                super::error::shared_folder_error(
                    err,
                    folder,
                    shared_owner.as_ref(),
                    super::error::ImapErrorContext::operation(
                        bifrost_types::AccountOperation::Hydrate,
                    )
                    .with_folder_scope(folder),
                ),
                bytes_in,
            ));
        }
        Err(err) => {
            let bytes_in = super::measured_bytes_in(conn.connection(), &mut previous_bytes_in);
            return Err(GetError::Imap(err, bytes_in));
        }
    };
    let uidvalidity = super::selected_uidvalidity(&selected.mailbox).map_err(|err| {
        let bytes_in = super::measured_bytes_in(conn.connection(), &mut previous_bytes_in);
        GetError::Imap(err, bytes_in)
    })?;
    // `pending` is drained only now: a checkout or SELECT failure above
    // must leave every id in the caller's unresolved set.
    let (valid, stale): (Vec<_>, Vec<_>) = pending
        .drain(..)
        .partition(|id| id.uidvalidity == uidvalidity);
    // One owner for "requested ids -> wire operand -> outcome attribution".
    // The excluded lane is published with the stale failures: like them it
    // is known truth before any wire work, and unlike them it must never be
    // able to reach the FETCH path and be reported as hydrated.
    let target_batch = TargetBatch::new(valid);
    let uid_set = target_batch.uid_set().cloned();
    let requested_uids: HashSet<u32> = target_batch.uids().iter().copied().collect();
    let (valid, excluded_outcomes) =
        target_batch.settle_streaming(bifrost_types::AccountOperation::Hydrate, folder);
    // The permits are the only way to answer for these ids; `pending` keeps
    // a plain copy so an early return leaves them in the unresolved set.
    let pending_ids: Vec<DecodedObjectId> =
        valid.iter().map(|target| target.id().clone()).collect();
    // Stale-UIDVALIDITY failures are known before any wire work, so they are
    // emitted first. Buffering them behind the FETCH would lose established
    // per-item truth if that FETCH fails and the folder falls back to the
    // uncertain lane.
    emit_stale_failures(tx, stale, folder, conn.connection(), &mut previous_bytes_in).await?;
    if !excluded_outcomes.is_empty() {
        let bytes_in = super::measured_bytes_in(conn.connection(), &mut previous_bytes_in);
        tx.send(batch(excluded_outcomes, PageBoundary::Page, None, bytes_in))
            .await
            .map_err(|_| GetError::ChannelDropped)?;
    }
    // The stale and excluded failures are on the channel: from here on only
    // the valid ids remain unresolved should the FETCH (or MODSEQ
    // recording) fail.
    *pending = pending_ids;
    let mut out: Vec<ItemOutcome<HydratedObject>> = Vec::with_capacity(BATCH_ITEMS);
    let Some(uid_set) = uid_set else {
        return Ok(());
    };
    let include_modseq = selected.mailbox.highest_mod_seq.is_some() && !selected.mailbox.no_mod_seq;
    let selection = selection_for_projection(projection, include_modseq);
    let attrs = selection.attributes();
    // A body-bearing projection buffers whole messages, so it runs under an
    // explicit byte budget: `uid_fetch` has none, and a hydration batch of
    // large messages (or a server answering with more than it was asked
    // for) would otherwise be materialised in full. Metadata and flag
    // projections are bounded by their own response shape.
    let fetches = if selection.needs_body_budget() {
        conn.connection()
            .uid_fetch_limited(
                &uid_set,
                &attrs,
                HYDRATION_FETCH_BUDGET,
                account.command_timeout(),
            )
            .await
    } else {
        conn.connection()
            .uid_fetch(uid_set.as_sequence_set(), &attrs, account.command_timeout())
            .await
    }
    .map_err(|err| {
        let bytes_in = super::measured_bytes_in(conn.connection(), &mut previous_bytes_in);
        GetError::Imap(err, bytes_in)
    })?;
    let mut page_bytes_in = super::measured_bytes_in(conn.connection(), &mut previous_bytes_in);
    // A server may emit several FETCH responses for one UID: the solicited
    // one plus unsolicited FLAGS updates that carry none of the requested
    // data items. Merge them per UID before conversion so a trailing partial
    // response cannot replace the complete one.
    let mut fetched_by_uid: HashMap<u32, FetchResponse> = HashMap::new();
    for fetch in fetches {
        let Some(uid) = fetch.uid else {
            continue;
        };
        if !requested_uids.contains(&uid) {
            continue;
        }
        if let Some(modseq) = fetch.mod_seq {
            account
                .folders
                .record_modseq(folder, uidvalidity, uid, modseq)
                .map_err(|err| GetError::Imap(err, page_bytes_in))?;
        }
        match fetched_by_uid.entry(uid) {
            std::collections::hash_map::Entry::Occupied(mut existing) => {
                merge_fetch_response(existing.get_mut(), fetch);
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(fetch);
            }
        }
    }
    let hydrated_by_uid: HashMap<u32, HydratedObject> = fetched_by_uid
        .into_iter()
        .filter_map(|(uid, fetch)| {
            let object = fetch_to_hydrated(
                folder,
                uidvalidity,
                fetch,
                projection,
                shared_owner.as_ref(),
            )?;
            Some((uid, object))
        })
        .collect();
    for target in valid {
        // The permit stamps the id: a hydrated object can only ever be
        // published under the id that was requested for it.
        let verdict = match hydrated_by_uid.get(&target.uid()).cloned() {
            Some(object) => Verdict::Succeeded(object),
            None => Verdict::Failed(message_not_found_error(target.id())),
        };
        out.push(target.seal(verdict).into_outcome());
        if out.len() >= BATCH_ITEMS {
            tx.send(batch(
                std::mem::take(&mut out),
                PageBoundary::Page,
                None,
                std::mem::take(&mut page_bytes_in),
            ))
            .await
            .map_err(|_| GetError::ChannelDropped)?;
        }
    }
    if !out.is_empty() {
        tx.send(batch(out, PageBoundary::Page, None, page_bytes_in))
            .await
            .map_err(|_| GetError::ChannelDropped)?;
    }
    Ok(())
}

fn attrs_for_projection(projection: Projection, include_modseq: bool) -> Vec<FetchAttr> {
    selection_for_projection(projection, include_modseq).attributes()
}

fn selection_for_projection(projection: Projection, include_modseq: bool) -> FetchSelection {
    match projection {
        Projection::FlagsOnly => FetchSelection {
            flags: true,
            envelope: false,
            size: false,
            modseq: include_modseq,
            body: BodySelection::None,
        },
        Projection::Metadata => FetchSelection {
            flags: true,
            envelope: true,
            size: true,
            modseq: include_modseq,
            body: BodySelection::None,
        },
        Projection::Headers => FetchSelection {
            flags: false,
            envelope: false,
            size: false,
            modseq: false,
            body: BodySelection::Headers,
        },
        Projection::Preview(count) => FetchSelection {
            flags: false,
            envelope: false,
            size: false,
            modseq: false,
            body: BodySelection::Preview(count),
        },
        Projection::TextOnly => FetchSelection {
            flags: false,
            envelope: false,
            size: false,
            modseq: false,
            body: BodySelection::Whole,
        },
        Projection::Full | Projection::FullWithBlobs => FetchSelection {
            flags: false,
            envelope: false,
            size: true,
            modseq: false,
            body: BodySelection::Whole,
        },
        _ => FetchSelection {
            flags: true,
            envelope: false,
            size: false,
            modseq: false,
            body: BodySelection::None,
        },
    }
}

fn fetch_to_hydrated(
    folder: &MailboxName,
    uidvalidity: u32,
    mut fetch: FetchResponse,
    projection: Projection,
    shared_owner: Option<&bifrost_types::MailboxId>,
) -> Option<HydratedObject> {
    let uid = fetch.uid?;
    let id = super::encode_object_id(folder, uidvalidity, uid);
    let kind = match projection {
        Projection::FlagsOnly => {
            HydratedObjectKind::FlagsOnly(flags_set(fetch.flags.as_deref().unwrap_or(&[])))
        }
        Projection::Metadata => HydratedObjectKind::Metadata(fetch_to_inventory(
            folder,
            uidvalidity,
            fetch,
            shared_owner,
        )),
        _ => HydratedObjectKind::RawMime(bytes::Bytes::from(
            decode_body(&mut fetch).unwrap_or_default(),
        )),
    };
    Some(HydratedObject {
        id,
        kind,
        blobs: Vec::new(),
    })
}

/// Fold a later FETCH response for the same UID into the one already held.
/// Data items only ever fill gaps: a second response may add `FLAGS` or a
/// section the first lacked, but it must never blank out data the first
/// response carried. Shared with `pim::hydrate_decoded`, which faces the
/// same split responses.
pub(super) fn merge_fetch_response(existing: &mut FetchResponse, later: FetchResponse) {
    if later.flags.is_some() {
        existing.flags = later.flags;
    }
    if later.mod_seq.is_some() {
        existing.mod_seq = later.mod_seq;
    }
    if existing.envelope.is_none() {
        existing.envelope = later.envelope;
    }
    if existing.body_structure.is_none() {
        existing.body_structure = later.body_structure;
    }
    if existing.rfc822_size.is_none() {
        existing.rfc822_size = later.rfc822_size;
    }
    if existing.internal_date.is_none() {
        existing.internal_date = later.internal_date;
    }
    if existing.save_date.is_none() {
        existing.save_date = later.save_date;
    }
    if existing.preview.is_none() {
        existing.preview = later.preview;
    }
    if existing.email_id.is_none() {
        existing.email_id = later.email_id;
    }
    if existing.thread_id.is_none() {
        existing.thread_id = later.thread_id;
    }
    if existing.gmail_msg_id.is_none() {
        existing.gmail_msg_id = later.gmail_msg_id;
    }
    if existing.gmail_thread_id.is_none() {
        existing.gmail_thread_id = later.gmail_thread_id;
    }
    // Labels are mutable state like FLAGS: the later report is the fresher.
    if later.gmail_labels.is_some() {
        existing.gmail_labels = later.gmail_labels;
    }
    for section in later.body_sections {
        if existing
            .body_sections
            .iter()
            .any(|held| held.section.eq_ignore_ascii_case(&section.section) && held.data.is_some())
        {
            continue;
        }
        existing
            .body_sections
            .retain(|held| !held.section.eq_ignore_ascii_case(&section.section));
        existing.body_sections.push(section);
    }
    for (section, size) in later.binary_sizes {
        if existing
            .binary_sizes
            .iter()
            .any(|(held, _)| *held == section)
        {
            continue;
        }
        existing.binary_sizes.push((section, size));
    }
    for section in later.binary_sections {
        if existing
            .binary_sections
            .iter()
            .any(|held| held.section == section.section)
        {
            continue;
        }
        existing.binary_sections.push(section);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountErrorKind, ObjectId, RequestErrorKind};

    #[test]
    fn locally_invalid_object_id_classifies_as_request_malformed() {
        // imap-D4: get_stream emits ItemOutcome::Failed carrying a
        // structured `AccountError` for locally-invalid ids (rather
        // than dropping the failure as a free-form Warning). The
        // streaming wrapper plumbs the structured failure straight
        // through; this test pins the classification of the failure
        // that `decode_object_id` produces - if that classification
        // changes, the lane's `AccountError` changes with it.
        let bad = ObjectId("not-a-valid-imap-id".into());
        let err = super::super::decode_object_id(&bad, AccountOperation::Hydrate)
            .expect_err("invalid id should not decode");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        // The hydration lane's own operation rides the error, so a
        // malformed id is attributable to the lane that saw it rather
        // than landing as an operation-less ClientBug.
        assert_eq!(err.operation(), Some(AccountOperation::Hydrate));
    }

    #[test]
    fn metadata_projection_requests_modseq_only_when_available() {
        assert!(
            attrs_for_projection(Projection::Metadata, true)
                .iter()
                .any(|attr| matches!(attr, FetchAttr::ModSeq))
        );
        assert!(
            attrs_for_projection(Projection::Metadata, false)
                .iter()
                .all(|attr| !matches!(attr, FetchAttr::ModSeq))
        );
    }

    fn folder() -> MailboxName {
        MailboxName::new("INBOX").expect("valid mailbox")
    }

    // Every projection must ask for UID: without it `fetch_to_hydrated`
    // cannot mint an ObjectId and silently drops the item.
    #[test]
    fn every_projection_requests_the_uid() {
        for projection in [
            Projection::FlagsOnly,
            Projection::Metadata,
            Projection::Headers,
            Projection::Preview(512),
            Projection::TextOnly,
            Projection::Full,
            Projection::FullWithBlobs,
        ] {
            let attrs = attrs_for_projection(projection, false);
            assert!(
                attrs.iter().any(|attr| matches!(attr, FetchAttr::Uid)),
                "{projection:?} must request UID",
            );
        }
    }

    #[test]
    fn body_bearing_projections_peek_rather_than_setting_seen() {
        // A hydration read must never flip `\Seen` as a side effect, so
        // every BODY section the read paths request is a PEEK.
        for projection in [
            Projection::Preview(64),
            Projection::TextOnly,
            Projection::Full,
            Projection::FullWithBlobs,
        ] {
            let attrs = attrs_for_projection(projection, false);
            let peeks: Vec<bool> = attrs
                .iter()
                .filter_map(|attr| match attr {
                    FetchAttr::BodySection { peek, .. } => Some(*peek),
                    _ => None,
                })
                .collect();
            assert!(!peeks.is_empty(), "{projection:?} must fetch a body");
            assert!(peeks.iter().all(|peek| *peek), "{projection:?} must PEEK");
        }
    }

    #[test]
    fn flags_only_and_metadata_never_fetch_a_body_section() {
        for projection in [Projection::FlagsOnly, Projection::Metadata] {
            assert!(
                attrs_for_projection(projection, true)
                    .iter()
                    .all(|attr| !matches!(attr, FetchAttr::BodySection { .. })),
                "{projection:?} must stay a metadata-only fetch",
            );
        }
    }

    #[test]
    fn hydrated_object_requires_a_uid() {
        let no_uid = FetchResponse {
            flags: Some(vec![crate::types::Flag::Seen]),
            ..Default::default()
        };
        assert!(
            fetch_to_hydrated(&folder(), 9, no_uid, Projection::FlagsOnly, None).is_none(),
            "a FETCH without UID cannot be addressed and must be dropped",
        );
    }

    #[test]
    fn flags_only_hydration_lowercases_the_flag_set() {
        let fetch = FetchResponse {
            uid: Some(3),
            flags: Some(vec![
                crate::types::Flag::Seen,
                crate::types::Flag::Custom("$Important".to_owned()),
            ]),
            ..Default::default()
        };
        let object = fetch_to_hydrated(&folder(), 9, fetch, Projection::FlagsOnly, None)
            .expect("uid present");
        assert_eq!(object.id, super::super::encode_object_id(&folder(), 9, 3));
        assert!(object.blobs.is_empty());
        match object.kind {
            HydratedObjectKind::FlagsOnly(flags) => {
                assert!(flags.contains("\\seen"));
                assert!(flags.contains("$important"));
            }
            other => panic!("expected FlagsOnly, got {other:?}"),
        }
    }

    #[test]
    fn metadata_hydration_reuses_the_inventory_projection_and_owner_tag() {
        let fetch = FetchResponse {
            uid: Some(3),
            rfc822_size: Some(77),
            ..Default::default()
        };
        let owner = bifrost_types::MailboxId("alice".to_owned());
        let object = fetch_to_hydrated(&folder(), 9, fetch, Projection::Metadata, Some(&owner))
            .expect("uid present");
        match object.kind {
            HydratedObjectKind::Metadata(entry) => {
                assert_eq!(entry.size, Some(77));
                assert!(
                    entry
                        .memberships
                        .contains(&bifrost_types::MembershipScope::Mailbox(owner)),
                    "the shared-mailbox membership must survive hydration",
                );
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    // A raw-MIME projection with no returned section yields empty bytes
    // rather than dropping the item: the id still reaches the consumer.
    #[test]
    fn raw_mime_hydration_tolerates_a_missing_section() {
        let fetch = FetchResponse {
            uid: Some(3),
            ..Default::default()
        };
        let object =
            fetch_to_hydrated(&folder(), 9, fetch, Projection::Full, None).expect("uid present");
        match object.kind {
            HydratedObjectKind::RawMime(bytes) => assert!(bytes.is_empty()),
            other => panic!("expected RawMime, got {other:?}"),
        }
    }

    // A preview is a prefix of the whole message, so what comes back is a
    // single unnamed section that already carries its own headers.
    #[test]
    fn preview_hydration_returns_the_whole_message_prefix() {
        let fetch = FetchResponse {
            uid: Some(3),
            body_sections: vec![crate::types::fetch::BodySection {
                section: String::new(),
                origin: Some(0),
                data: Some(b"Subject: hi\r\n\r\npreview body".to_vec()),
            }],
            ..Default::default()
        };
        let object = fetch_to_hydrated(&folder(), 9, fetch, Projection::Preview(16), None)
            .expect("uid present");
        match object.kind {
            HydratedObjectKind::RawMime(bytes) => {
                assert_eq!(bytes.as_ref(), &b"Subject: hi\r\n\r\npreview body"[..]);
            }
            other => panic!("expected RawMime, got {other:?}"),
        }
    }

    // `BODY[TEXT]` of a multipart message is boundaries and base64 with no
    // headers to decode it by, which is exactly what a consumer must not be
    // handed as "preview text". Both body-prefix projections ask for the
    // whole message; only the byte budget differs.
    #[test]
    fn preview_and_text_only_fetch_a_whole_message_not_body_text() {
        for projection in [Projection::Preview(16), Projection::TextOnly] {
            let sections: Vec<_> = attrs_for_projection(projection, false)
                .into_iter()
                .filter_map(|attr| match attr {
                    FetchAttr::BodySection {
                        section, partial, ..
                    } => Some((section, partial)),
                    _ => None,
                })
                .collect();
            assert_eq!(sections.len(), 1, "{projection:?} fetches one section");
            assert!(
                sections[0].0.is_none(),
                "{projection:?} must fetch the whole message, not a named section",
            );
        }

        // The floor is what makes the prefix reach past MIME framing: a
        // caller asking for 16 bytes still gets a usable message.
        let partial = attrs_for_projection(Projection::Preview(16), false)
            .into_iter()
            .find_map(|attr| match attr {
                FetchAttr::BodySection { partial, .. } => partial,
                _ => None,
            })
            .expect("preview fetches a partial section");
        assert_eq!(partial, (0, PREVIEW_FETCH_BYTES));

        let large = usize::try_from(PREVIEW_FETCH_BYTES).expect("fits") * 4;
        let partial = attrs_for_projection(Projection::Preview(large), false)
            .into_iter()
            .find_map(|attr| match attr {
                FetchAttr::BodySection { partial, .. } => partial,
                _ => None,
            })
            .expect("preview fetches a partial section");
        assert_eq!(
            partial,
            (0, PREVIEW_FETCH_BYTES * 4),
            "a request larger than the floor is honoured in full",
        );
    }

    #[test]
    fn unreturned_and_stale_hydration_ids_are_failed() {
        let stale = DecodedObjectId {
            folder: folder(),
            uidvalidity: 8,
            uid: 2,
        };
        let stale = failed_hydration(vec![stale], uidvalidity_changed_error(&folder()));
        assert!(
            matches!(stale.as_slice(), [ItemOutcome::Failed(failure)] if matches!(
                failure.error.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            ))
        );

        let missing = DecodedObjectId {
            folder: folder(),
            uidvalidity: 9,
            uid: 3,
        };
        let error = message_not_found_error(&missing);
        assert!(matches!(
            error.kind(),
            AccountErrorKind::NotFound(bifrost_types::ResourceKind::Message)
        ));
    }

    fn body_section(section: &str, data: &[u8]) -> crate::types::fetch::BodySection {
        crate::types::fetch::BodySection {
            section: section.to_owned(),
            origin: None,
            data: Some(data.to_vec()),
        }
    }

    // A server is free to append an unsolicited FLAGS-only FETCH for a UID
    // it has already answered fully. Folding it in must not blank the body
    // that the solicited response carried.
    #[test]
    fn trailing_flags_update_does_not_erase_a_complete_hydration() {
        let mut complete = FetchResponse {
            uid: Some(7),
            rfc822_size: Some(120),
            body_sections: vec![body_section("", b"From: a\r\n\r\nfull body")],
            ..Default::default()
        };
        let trailing = FetchResponse {
            uid: Some(7),
            flags: Some(vec![crate::types::Flag::Seen]),
            mod_seq: Some(42),
            ..Default::default()
        };
        merge_fetch_response(&mut complete, trailing);
        assert_eq!(complete.mod_seq, Some(42));
        assert_eq!(complete.rfc822_size, Some(120));
        assert!(
            complete
                .flags
                .as_ref()
                .is_some_and(|flags| flags.contains(&crate::types::Flag::Seen)),
            "the later FLAGS update must be adopted",
        );
        let object =
            fetch_to_hydrated(&folder(), 9, complete, Projection::Full, None).expect("uid present");
        match object.kind {
            HydratedObjectKind::RawMime(bytes) => assert_eq!(
                bytes.as_ref(),
                &b"From: a\r\n\r\nfull body"[..],
                "a partial trailing FETCH must not replace the fetched body",
            ),
            other => panic!("expected RawMime, got {other:?}"),
        }
    }

    // Every data item fills a gap the earlier response left, not only the
    // body-shaped ones: `pim::fetch_to_message` reads THREADID and the Gmail
    // thread id, and a split response used to lose them. BINARY.SIZE entries
    // add per section without replacing one already held; Gmail labels are
    // mutable state like FLAGS, so the later report wins.
    #[test]
    fn merging_fetches_fills_every_optional_item_and_binary_size() {
        let mut first = FetchResponse {
            uid: Some(7),
            binary_sizes: vec![(vec![1], 10)],
            gmail_labels: Some(vec!["old".to_owned()]),
            ..Default::default()
        };
        merge_fetch_response(
            &mut first,
            FetchResponse {
                uid: Some(7),
                thread_id: Some("T1".to_owned()),
                email_id: Some("E1".to_owned()),
                gmail_thread_id: Some(42),
                preview: Some("hi".to_owned()),
                binary_sizes: vec![(vec![1], 99), (vec![2], 20)],
                gmail_labels: Some(vec!["new".to_owned()]),
                ..Default::default()
            },
        );
        assert_eq!(first.thread_id.as_deref(), Some("T1"));
        assert_eq!(first.email_id.as_deref(), Some("E1"));
        assert_eq!(first.gmail_thread_id, Some(42));
        assert_eq!(first.preview.as_deref(), Some("hi"));
        assert_eq!(first.binary_sizes, vec![(vec![1], 10), (vec![2], 20)]);
        assert_eq!(first.gmail_labels, Some(vec!["new".to_owned()]));
    }

    // A trailing partial response may add a section the first lacked, but
    // must never overwrite one that already carries data.
    #[test]
    fn merging_fetches_fills_gaps_without_overwriting_returned_sections() {
        let mut first = FetchResponse {
            uid: Some(7),
            body_sections: vec![body_section("", b"Subject: hi\r\n\r\npreview body")],
            ..Default::default()
        };
        merge_fetch_response(
            &mut first,
            FetchResponse {
                uid: Some(7),
                body_sections: vec![
                    body_section("", b"Subject: WRONG\r\n\r\n"),
                    body_section("HEADER", b"Subject: hi\r\n\r\n"),
                ],
                ..Default::default()
            },
        );
        assert_eq!(
            first.body_sections.len(),
            2,
            "the section the first response lacked is adopted",
        );
        let object = fetch_to_hydrated(&folder(), 9, first, Projection::Preview(16), None)
            .expect("uid present");
        match object.kind {
            HydratedObjectKind::RawMime(bytes) => {
                assert_eq!(bytes.as_ref(), &b"Subject: hi\r\n\r\npreview body"[..]);
            }
            other => panic!("expected RawMime, got {other:?}"),
        }
    }

    // Stale-UIDVALIDITY failures are known truth before the FETCH runs. They
    // must already be on the channel when a later step fails, otherwise the
    // folder-level error path relabels them uncertain.
    #[tokio::test]
    async fn stale_failures_reach_the_consumer_before_the_fetch_can_fail() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let stale = vec![DecodedObjectId {
            folder: folder(),
            uidvalidity: 8,
            uid: 2,
        }];
        let (connection, _server) =
            crate::connection::test_support::driver_pair(b"* PREAUTH ready\r\n").await;
        let mut previous_bytes_in = connection.bytes_in();
        assert!(
            emit_stale_failures(&tx, stale, &folder(), &connection, &mut previous_bytes_in)
                .await
                .is_ok()
        );
        // Simulate the hydration FETCH blowing up after this point.
        drop(tx);
        let event = rx.recv().await.expect("stale failures already published");
        match event {
            SyncEvent::Batch(batch) => assert!(matches!(
                batch.items.as_slice(),
                [ItemOutcome::Failed(failure)] if matches!(
                    failure.error.kind(),
                    AccountErrorKind::Request(RequestErrorKind::Malformed)
                )
            )),
            other => panic!("expected a batch, got {other:?}"),
        }
    }
}
