use std::collections::{HashMap, HashSet};

use bifrost_types::{
    AccountError, AccountOperation, AccountStream, BatchFailure, BatchItemId, BatchUncertain,
    HydratedObject, HydratedObjectKind, ItemOutcome, PageBoundary, Projection, SyncEvent,
};

use super::error::{
    hydration_uidvalidity_changed_error as uidvalidity_changed_error, message_not_found_error,
};
use futures::StreamExt;

#[cfg(test)]
use crate::types::FetchAttr;
use crate::types::{FetchResponse, MailboxName};

#[cfg(test)]
use super::hydration::PREVIEW_FETCH_BYTES;
use super::hydration::{BodySelection, FetchSelection, decode_selected_body};
use super::inventory::{fetch_to_inventory, flags_set};
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
/// `crate::error::ResultLimit::Fetch` after the tagged completion, with the IMAP stream
/// still synchronized.
const HYDRATION_FETCH_BUDGET: usize = 256 * 1024 * 1024;

pub(crate) fn get_stream(
    account: ImapAccount,
    mut ids: AccountStream<bifrost_types::ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    if projection_plan(projection, false).is_none() {
        // A projection this client cannot represent is a local per-item
        // refusal, established before any folder checkout or FETCH.
        let error = super::error::unsupported(AccountOperation::Hydrate);
        return Box::pin(
            ids.map(move |id| {
                batch(
                    vec![ItemOutcome::Failed(BatchFailure::new(
                        BatchItemId(id.0),
                        error.clone(),
                    ))],
                    PageBoundary::Page,
                    None,
                    0,
                )
            })
            .chain(futures::stream::once(async { SyncEvent::Done(None) })),
        );
    }
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
                        let account_err = account.folder_error(
                            err,
                            &folder,
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
                        let item = BatchItemId(id.id.0);
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

fn failed_hydration(
    ids: impl IntoIterator<Item = DecodedObjectId>,
    error: AccountError,
) -> Vec<ItemOutcome<HydratedObject>> {
    ids.into_iter()
        .map(|id| {
            let item = BatchItemId(id.id.0);
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
    let shared_owner = folder_entry
        .as_ref()
        .and_then(|entry| entry.shared_owner.clone());
    let selected = match account
        .select_folder(&mut conn, folder, super::SelectPurpose::Read)
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
    let include_modseq = selected.mailbox.modseq().is_some();
    let plan = projection_plan(projection, include_modseq).ok_or_else(|| {
        GetError::Account(super::error::unsupported(AccountOperation::Hydrate), 0)
    })?;
    let selection = plan.0;
    // `pending` is drained only now: a checkout or SELECT failure above
    // must leave every id in the caller's unresolved set.
    let (valid, stale): (Vec<_>, Vec<_>) = pending
        .drain(..)
        .partition(|id| id.uidvalidity == uidvalidity);
    // One owner for "requested ids -> wire operand -> outcome attribution".
    // Typed identities all reach the operand; every target gets a permit.
    let target_batch = TargetBatch::new(valid);
    let uid_set = target_batch.uid_set().cloned();
    let requested_uids = target_batch.uids().to_vec();
    let valid = target_batch.settle_streaming();
    // The permits are the only way to answer for these ids; `pending` keeps
    // a plain copy so an early return leaves them in the unresolved set.
    let pending_ids: Vec<DecodedObjectId> =
        valid.iter().map(|target| target.id().clone()).collect();
    // Stale-UIDVALIDITY failures are known before any wire work, so they are
    // emitted first. Buffering them behind the FETCH would lose established
    // per-item truth if that FETCH fails and the folder falls back to the
    // uncertain lane.
    emit_stale_failures(tx, stale, folder, conn.connection(), &mut previous_bytes_in).await?;
    // Only the valid ids remain unresolved if FETCH or MODSEQ recording fails.
    *pending = pending_ids;
    let mut out: Vec<ItemOutcome<HydratedObject>> = Vec::with_capacity(BATCH_ITEMS);
    let Some(uid_set) = uid_set else {
        return Ok(());
    };
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
    let fetched_by_uid = merge_requested_fetches(fetches, &requested_uids, |uid, fetch| {
        // Observe each accepted fragment in wire order, before later mutable
        // fields replace it. In particular a later ignored MODSEQ 0 must not
        // erase an earlier valid cache observation.
        if let Some(modseq) = fetch.mod_seq {
            account
                .folders
                .record_modseq(folder, uidvalidity, uid, modseq)?;
        }
        Ok::<(), crate::Error>(())
    })
    .map_err(|err| GetError::Imap(err, page_bytes_in))?;
    let hydrated_by_uid: HashMap<u32, Result<HydratedObject, AccountError>> = fetched_by_uid
        .into_iter()
        .map(|(uid, fetch)| {
            let object = hydrate_planned(folder, uidvalidity, fetch, plan.1, shared_owner.as_ref());
            (
                uid,
                object.map_err(|err| {
                    super::account_error_with(
                        err,
                        super::error::ImapErrorContext::operation(AccountOperation::Hydrate)
                            .with_folder_scope(folder),
                    )
                }),
            )
        })
        .collect();
    for target in valid {
        // The permit stamps the id: a hydrated object can only ever be
        // published under the id that was requested for it.
        let verdict = match hydrated_by_uid.get(&target.uid()).cloned() {
            Some(Ok(object)) => Verdict::Succeeded(answer_as(object, &target.id().id)),
            Some(Err(error)) => Verdict::Failed(error),
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

#[cfg(test)]
fn attrs_for_projection(projection: Projection, include_modseq: bool) -> Vec<FetchAttr> {
    selection_for_projection(projection, include_modseq).attributes()
}

#[derive(Clone, Copy)]
enum HydrationKind {
    Flags,
    Metadata,
    // The section the projection asked for. A header or preview projection
    // gets back a differently keyed section than a whole-message one, so the
    // decoder must be told which to take or it finds nothing.
    Mime(BodySelection),
}

fn projection_plan(
    projection: Projection,
    include_modseq: bool,
) -> Option<(FetchSelection, HydrationKind)> {
    Some(match projection {
        Projection::FlagsOnly => (
            FetchSelection {
                flags: true,
                envelope: false,
                size: false,
                modseq: include_modseq,
                body: BodySelection::None,
            },
            HydrationKind::Flags,
        ),
        Projection::Metadata => (
            FetchSelection {
                flags: true,
                envelope: true,
                size: true,
                modseq: include_modseq,
                body: BodySelection::None,
            },
            HydrationKind::Metadata,
        ),
        Projection::Headers => (
            FetchSelection {
                flags: false,
                envelope: false,
                size: false,
                modseq: false,
                body: BodySelection::Headers,
            },
            HydrationKind::Mime(BodySelection::Headers),
        ),
        Projection::Preview(count) => (
            FetchSelection {
                flags: false,
                envelope: false,
                size: false,
                modseq: false,
                body: BodySelection::Preview(count),
            },
            HydrationKind::Mime(BodySelection::Preview(count)),
        ),
        Projection::TextOnly => (
            FetchSelection {
                flags: false,
                envelope: false,
                size: false,
                modseq: false,
                body: BodySelection::Whole,
            },
            HydrationKind::Mime(BodySelection::Whole),
        ),
        Projection::Full | Projection::FullWithBlobs => (
            FetchSelection {
                flags: false,
                envelope: false,
                size: true,
                modseq: false,
                body: BodySelection::Whole,
            },
            HydrationKind::Mime(BodySelection::Whole),
        ),
        // Future projections require an explicit wire selection and converter;
        // asking only for FLAGS must never fabricate a successful empty body.
        _ => return None,
    })
}

#[cfg(test)]
fn selection_for_projection(projection: Projection, include_modseq: bool) -> FetchSelection {
    projection_plan(projection, include_modseq)
        .expect("supported projection")
        .0
}

/// Rename a hydrated object to the id its target was submitted under. The
/// UID-keyed result is minted with the canonical id and may serve several
/// targets naming the same message in different spellings, so each target
/// stamps its own copy; the metadata entry carries a second id that must
/// agree with the wrapper's.
fn answer_as(mut object: HydratedObject, id: &bifrost_types::ObjectId) -> HydratedObject {
    object.id = id.clone();
    if let HydratedObjectKind::Metadata(entry) = &mut object.kind {
        entry.id = id.clone();
    }
    object
}

fn hydrate_planned(
    folder: &MailboxName,
    uidvalidity: crate::types::UidValidity,
    mut fetch: FetchResponse,
    kind: HydrationKind,
    shared_owner: Option<&bifrost_types::PrincipalId>,
) -> Result<HydratedObject, crate::Error> {
    let uid = fetch.uid.filter(|uid| *uid != 0).ok_or_else(|| {
        crate::Error::Provider(crate::error::ProviderFailure::Missing(
            "FETCH response lacked a nonzero UID".into(),
        ))
    })?;
    let id = super::encode_object_id(
        folder,
        uidvalidity,
        crate::types::Uid::new(uid).expect("wire UID is nonzero"),
    );
    let kind = match kind {
        HydrationKind::Flags => {
            let flags = fetch.flags.as_deref().ok_or_else(|| {
                crate::Error::Provider(crate::error::ProviderFailure::Missing(
                    "FETCH response lacked FLAGS".into(),
                ))
            })?;
            HydratedObjectKind::FlagsOnly(flags_set(flags))
        }
        HydrationKind::Metadata => HydratedObjectKind::Metadata(fetch_to_inventory(
            folder,
            uidvalidity,
            fetch,
            shared_owner,
        )?),
        HydrationKind::Mime(selection) => {
            let body = decode_selected_body(&mut fetch, selection).ok_or_else(|| {
                crate::Error::Provider(crate::error::ProviderFailure::Missing(
                    "FETCH response lacked the requested body".into(),
                ))
            })?;
            HydratedObjectKind::RawMime(body)
        }
    };
    Ok(HydratedObject {
        id,
        kind,
        blobs: Vec::new(),
    })
}

#[cfg(test)]
fn fetch_to_hydrated(
    folder: &MailboxName,
    uidvalidity: crate::types::UidValidity,
    fetch: FetchResponse,
    projection: Projection,
    shared_owner: Option<&bifrost_types::PrincipalId>,
) -> Option<HydratedObject> {
    let (_, kind) = projection_plan(projection, false)?;
    hydrate_planned(folder, uidvalidity, fetch, kind, shared_owner).ok()
}

/// Fold a UID FETCH's collected responses into one response per REQUESTED
/// UID.
///
/// The connection hands back every FETCH seen during the command, and two
/// kinds are not the answer: an unsolicited FETCH for a UID nobody asked
/// about (another client's flag change), which would otherwise hydrate as a
/// message the caller never requested, and a second response for a requested
/// UID - the server may split data items across responses, or append a
/// FLAGS-only update - which would otherwise hydrate as a duplicate, possibly
/// bodiless, copy. Responses without a UID cannot be attributed and are
/// dropped. The fold is `get`'s `merge_fetch_response`: later data fills gaps
/// and never blanks what an earlier response carried.
pub(super) fn merge_requested_fetches<E>(
    fetches: Vec<crate::types::FetchResponse>,
    requested: &[u32],
    mut observe: impl FnMut(u32, &FetchResponse) -> Result<(), E>,
) -> Result<HashMap<u32, FetchResponse>, E> {
    let requested: HashSet<u32> = requested.iter().copied().collect();
    let mut by_uid: HashMap<u32, crate::types::FetchResponse> = HashMap::new();
    for fetch in fetches {
        let Some(uid) = fetch.uid else {
            continue;
        };
        if !requested.contains(&uid) {
            continue;
        }
        observe(uid, &fetch)?;
        match by_uid.entry(uid) {
            std::collections::hash_map::Entry::Occupied(mut held) => {
                merge_fetch_response(held.get_mut(), fetch);
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(fetch);
            }
        }
    }
    Ok(by_uid)
}

/// Fold the protocol observations collected by account hydration.
pub(super) fn merge_fetch_response(existing: &mut FetchResponse, later: FetchResponse) {
    existing.merge(later);
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
            fetch_to_hydrated(
                &folder(),
                crate::types::UidValidity::new(9).expect("valid protocol test value"),
                no_uid,
                Projection::FlagsOnly,
                None
            )
            .is_none(),
            "a FETCH without UID cannot be addressed and must be dropped",
        );
    }

    #[test]
    fn requested_fetch_fold_filters_and_observes_fragments_before_merging() {
        let response = |uid, mod_seq| FetchResponse {
            uid,
            mod_seq,
            ..Default::default()
        };
        let mut observed = Vec::new();
        let merged = merge_requested_fetches(
            vec![
                response(
                    None,
                    Some(crate::types::ModSeqValzer::new(1).expect("valid protocol test value")),
                ),
                response(
                    Some(99),
                    Some(crate::types::ModSeqValzer::new(2).expect("valid protocol test value")),
                ),
                response(
                    Some(3),
                    Some(crate::types::ModSeqValzer::new(7).expect("valid protocol test value")),
                ),
                response(Some(3), Some(crate::types::ModSeqValzer::new(0).unwrap())),
            ],
            &[3],
            |uid, fetch| {
                observed.push((uid, fetch.mod_seq.map(crate::types::ModSeqValzer::get)));
                Ok::<(), ()>(())
            },
        )
        .unwrap();
        assert_eq!(observed, vec![(3, Some(7)), (3, Some(0))]);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[&3].mod_seq.map(crate::types::ModSeqValzer::get),
            Some(0)
        );
    }

    #[test]
    fn flag_bearing_hydration_requires_an_observation() {
        for projection in [Projection::FlagsOnly, Projection::Metadata] {
            let (_, kind) = projection_plan(projection, false).unwrap();
            let missing = FetchResponse {
                uid: Some(3),
                ..Default::default()
            };
            assert!(matches!(
                hydrate_planned(
                    &folder(),
                    crate::types::UidValidity::new(9).expect("valid protocol test value"),
                    missing,
                    kind,
                    None
                ),
                Err(crate::Error::Provider(
                    crate::error::ProviderFailure::Missing(_)
                ))
            ));
            let empty = FetchResponse {
                uid: Some(3),
                flags: Some(Vec::new()),
                ..Default::default()
            };
            assert!(
                hydrate_planned(
                    &folder(),
                    crate::types::UidValidity::new(9).expect("valid protocol test value"),
                    empty,
                    kind,
                    None
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn mime_hydration_requires_a_body_observation() {
        let fetch = FetchResponse {
            uid: Some(3),
            ..Default::default()
        };
        assert!(matches!(
            hydrate_planned(
                &folder(),
                crate::types::UidValidity::new(9).expect("valid protocol test value"),
                fetch,
                HydrationKind::Mime(BodySelection::Whole),
                None
            ),
            Err(crate::Error::Provider(
                crate::error::ProviderFailure::Missing(_)
            ))
        ));
    }

    #[test]
    fn flags_only_hydration_lowercases_the_flag_set() {
        let fetch = FetchResponse {
            uid: Some(3),
            flags: Some(vec![
                crate::types::Flag::Seen,
                crate::types::Flag::from_imap_str("$Important"),
            ]),
            ..Default::default()
        };
        let object = fetch_to_hydrated(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            fetch,
            Projection::FlagsOnly,
            None,
        )
        .expect("uid present");
        assert_eq!(
            object.id,
            super::super::encode_object_id(
                &folder(),
                crate::types::UidValidity::new(9).expect("nonzero epoch"),
                crate::types::Uid::new(3).expect("valid protocol test value")
            )
        );
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
    fn answering_as_a_submitted_id_renames_the_metadata_entry_too() {
        let fetch = FetchResponse {
            uid: Some(3),
            flags: Some(Vec::new()),
            rfc822_size: Some(77),
            ..Default::default()
        };
        let object = fetch_to_hydrated(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            fetch,
            Projection::Metadata,
            None,
        )
        .expect("uid present");
        let legacy = bifrost_types::ObjectId("imap1:5:INBOX:9:3".to_owned());
        let object = answer_as(object, &legacy);
        assert_eq!(object.id, legacy);
        match object.kind {
            HydratedObjectKind::Metadata(entry) => assert_eq!(entry.id, legacy),
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    #[test]
    fn metadata_hydration_reuses_the_inventory_projection_and_owner_tag() {
        let fetch = FetchResponse {
            uid: Some(3),
            flags: Some(Vec::new()),
            rfc822_size: Some(77),
            ..Default::default()
        };
        let owner = bifrost_types::PrincipalId("alice".to_owned());
        let object = fetch_to_hydrated(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            fetch,
            Projection::Metadata,
            Some(&owner),
        )
        .expect("uid present");
        match object.kind {
            HydratedObjectKind::Metadata(entry) => {
                assert_eq!(entry.fingerprint.size, Some(77));
                assert!(
                    entry
                        .memberships
                        .contains(&bifrost_types::MembershipScope::Owner(owner)),
                    "the shared-mailbox membership must survive hydration",
                );
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    // A raw-MIME projection with no returned section is not an empty
    // message: it must not hydrate as a successful empty body. The caller's
    // per-UID lane reports it as a failure instead.
    #[test]
    fn raw_mime_hydration_refuses_a_missing_section() {
        let fetch = FetchResponse {
            uid: Some(3),
            ..Default::default()
        };
        assert!(
            fetch_to_hydrated(
                &folder(),
                crate::types::UidValidity::new(9).expect("valid protocol test value"),
                fetch,
                Projection::Full,
                None
            )
            .is_none()
        );
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
                data: Some(b"Subject: hi\r\n\r\npreview body".to_vec().into()),
            }],
            ..Default::default()
        };
        let object = fetch_to_hydrated(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            fetch,
            Projection::Preview(16),
            None,
        )
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
        let stale = DecodedObjectId::canonical(
            folder(),
            crate::types::UidValidity::new(8).expect("nonzero epoch"),
            crate::types::Uid::new(2).expect("valid protocol test value"),
        );
        let stale = failed_hydration(vec![stale], uidvalidity_changed_error(&folder()));
        assert!(
            matches!(stale.as_slice(), [ItemOutcome::Failed(failure)] if matches!(
                failure.error.kind(),
                AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CursorInvalid)
            ))
        );

        let missing = DecodedObjectId::canonical(
            folder(),
            crate::types::UidValidity::new(9).expect("nonzero epoch"),
            crate::types::Uid::new(3).expect("valid protocol test value"),
        );
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
            data: Some(data.to_vec().into()),
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
            mod_seq: Some(crate::types::ModSeqValzer::new(42).expect("valid protocol test value")),
            ..Default::default()
        };
        merge_fetch_response(&mut complete, trailing);
        assert_eq!(
            complete.mod_seq.map(crate::types::ModSeqValzer::get),
            Some(42)
        );
        assert_eq!(complete.rfc822_size, Some(120));
        assert!(
            complete
                .flags
                .as_ref()
                .is_some_and(|flags| flags.contains(&crate::types::Flag::Seen)),
            "the later FLAGS update must be adopted",
        );
        let object = fetch_to_hydrated(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            complete,
            Projection::Full,
            None,
        )
        .expect("uid present");
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
    // body-shaped ones: `message::fetch_to_message` reads THREADID and the Gmail
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
                thread_id: Some(
                    crate::types::validated::ObjectId::new("T1".to_owned())
                        .expect("valid objectid"),
                ),
                email_id: Some(
                    crate::types::validated::ObjectId::new("E1".to_owned())
                        .expect("valid objectid"),
                ),
                gmail_thread_id: Some(crate::types::GmailThreadId::new(42)),
                preview: Some("hi".to_owned()),
                binary_sizes: vec![(vec![1], 99), (vec![2], 20)],
                gmail_labels: Some(vec!["new".to_owned()]),
                ..Default::default()
            },
        );
        assert_eq!(first.thread_id.as_deref(), Some("T1"));
        assert_eq!(first.email_id.as_deref(), Some("E1"));
        assert_eq!(
            first.gmail_thread_id,
            Some(crate::types::GmailThreadId::new(42))
        );
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
        // These sections carry no partial origin, so they answer a whole-message
        // request; a preview projection now takes only the origin-0 section.
        let object = fetch_to_hydrated(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            first,
            Projection::Full,
            None,
        )
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
        let stale = vec![DecodedObjectId::canonical(
            folder(),
            crate::types::UidValidity::new(8).expect("nonzero epoch"),
            crate::types::Uid::new(2).expect("valid protocol test value"),
        )];
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
                    AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CursorInvalid)
                )
            )),
            other => panic!("expected a batch, got {other:?}"),
        }
    }
}
