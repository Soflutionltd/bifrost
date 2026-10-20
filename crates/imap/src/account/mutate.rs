use std::collections::{BTreeMap, HashMap, HashSet};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, AccountStream,
    AttemptCause, BatchFailure, BatchItemId, BatchUncertain, Cause, DiagnosticText, FlagOp,
    IdempotencyKey, ItemOutcome, MembershipScope, MutationSuccess, PageBoundary, Protocol,
    RequestCause, RequestErrorKind, ServerCause, ServerErrorKind, StateCause, SyncEvent,
    TransmissionState,
};
use futures::StreamExt;

use crate::types::{Flag, MailboxName, ResponseCode, StoreOperation};

use super::targets::{TargetBatch, Verdict};
use super::{
    CompactUidSet, DecodedObjectId, ImapAccount, batch, boxed_receiver_stream, decode_object_id,
};

// The Account surface supplies an idempotency key to each bulk operation, but
// IMAP has no request token that a server can use to deduplicate these
// commands. Keep the key intentionally unused; uncertain outcomes are
// reconciled through the per-UID result lanes and read-back.
pub(crate) fn bulk_set_flags(
    account: ImapAccount,
    targets: AccountStream<bifrost_types::ObjectId>,
    op: FlagOp,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    if let Err(error) = op.validate_for_account(Protocol::Imap) {
        return Box::pin(futures::stream::once(async move {
            SyncEvent::Terminated(error)
        }));
    }
    mutation_stream(account, targets, MutationKind::Flags(op))
}

pub(crate) fn bulk_move(
    account: ImapAccount,
    targets: AccountStream<bifrost_types::ObjectId>,
    destination: MembershipScope,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(account, targets, MutationKind::Move(destination))
}

pub(crate) fn bulk_destroy(
    account: ImapAccount,
    targets: AccountStream<bifrost_types::ObjectId>,
    _key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(account, targets, MutationKind::Destroy)
}

pub(super) enum MutationKind {
    Flags(FlagOp),
    Move(MembershipScope),
    Destroy,
}

/// True when a folder-level mutation failure must collapse the entire
/// stream rather than surfacing per-item outcomes for the failing folder.
/// Personal-folder authorization loss and schema / capability breaks are
/// account-wide; a shared-folder permission denial is converted to scoped
/// `ScopeRevoked` before reaching this predicate. Per-folder failures emit
/// per-item outcomes and continue.
fn stream_terminating(err: &AccountError) -> bool {
    // Authorization stays terminal for a personal-folder NoPermission; the
    // shared-folder mapper turns that same response into ScopeRevoked first.
    matches!(
        err.kind(),
        AccountErrorKind::Authentication(_)
            | AccountErrorKind::Authorization(_)
            | AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::SchemaIncompatible
                    | bifrost_types::SyncStateErrorKind::CapabilityChanged
            )
    )
}

fn mutation_operation(kind: &MutationKind) -> AccountOperation {
    match kind {
        MutationKind::Flags(_) => AccountOperation::UpdateFlags,
        MutationKind::Move(_) => AccountOperation::BulkMove,
        MutationKind::Destroy => AccountOperation::BulkDestroy,
    }
}

pub(super) fn mutation_stream(
    account: ImapAccount,
    mut targets: AccountStream<bifrost_types::ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    tokio::spawn(async move {
        let move_destination = match validated_move_destination(&kind) {
            Ok(destination) => destination,
            Err(error) => {
                // Request validation failed for every target, so the outcomes
                // are known without touching the wire. Accumulate them to the
                // same window the grouped path flushes on rather than sending
                // one batch per id: a bad destination against a large target
                // set otherwise costs one channel send per target.
                let mut rejected = Vec::new();
                while let Some(id) = targets.next().await {
                    let item = BatchItemId(id.0);
                    rejected.push(ItemOutcome::Failed(BatchFailure::new(item, error.clone())));
                    if rejected.len() >= super::TARGET_BUFFER_ITEMS
                        && flush_items(&mut rejected, &tx).await.is_err()
                    {
                        return;
                    }
                }
                if flush_items(&mut rejected, &tx).await.is_err() {
                    return;
                }
                let _ = tx.send(SyncEvent::Done(None)).await;
                return;
            }
        };
        let mut grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)> = HashMap::new();
        // Ids that fail to decode never reach a folder group, but they are
        // buffered against the same window as the ones that do: they ride one
        // batch per flush instead of one batch each, and they count toward
        // `buffered` so the undecodable half cannot grow without bound while
        // no folder group ever reaches the flush threshold.
        let mut undecodable = Vec::new();
        let mut buffered = 0usize;
        while let Some(id) = targets.next().await {
            match decode_object_id(&id, mutation_operation(&kind)) {
                Ok(decoded) => {
                    grouped
                        .entry(decoded.folder.as_str().to_owned())
                        .or_insert_with(|| (decoded.folder.clone(), Vec::new()))
                        .1
                        .push(decoded);
                }
                Err(err) => {
                    let item_id = BatchItemId(id.0.clone());
                    undecodable.push(ItemOutcome::Failed(BatchFailure::new(item_id, err)));
                }
            }
            buffered += 1;
            if buffered >= super::TARGET_BUFFER_ITEMS {
                if flush_items(&mut undecodable, &tx).await.is_err() {
                    return;
                }
                if flush_mutation_groups(
                    &account,
                    &mut grouped,
                    &kind,
                    move_destination.as_ref(),
                    &tx,
                )
                .await
                .is_err()
                {
                    return;
                }
                buffered = 0;
            }
        }
        if flush_items(&mut undecodable, &tx).await.is_err() {
            return;
        }
        if flush_mutation_groups(
            &account,
            &mut grouped,
            &kind,
            move_destination.as_ref(),
            &tx,
        )
        .await
        .is_err()
        {
            return;
        }
        let _ = tx.send(SyncEvent::Done(None)).await;
    });
    boxed_receiver_stream(rx)
}

/// Send `items` as one `Batch` and clear the accumulator, doing nothing when
/// it is empty so a flush point costs no send unless it has something to say.
/// `Err(())` means the receiver is gone, which every streaming task treats as
/// silent termination.
async fn flush_items(
    items: &mut Vec<ItemOutcome<MutationSuccess>>,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<ItemOutcome<MutationSuccess>>>,
) -> Result<(), ()> {
    if items.is_empty() {
        return Ok(());
    }
    if tx
        .send(batch(std::mem::take(items), PageBoundary::Page, None))
        .await
        .is_err()
    {
        return Err(());
    }
    Ok(())
}

async fn flush_mutation_groups(
    account: &ImapAccount,
    grouped: &mut HashMap<String, (MailboxName, Vec<DecodedObjectId>)>,
    kind: &MutationKind,
    move_destination: Option<&MailboxName>,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<ItemOutcome<MutationSuccess>>>,
) -> Result<(), ()> {
    // Per-folder failure semantics:
    // - A folder error after per-item emissions in any prior folder
    //   must NOT collapse those items by emitting a global
    //   `SyncEvent::Terminated`. The failing folder's items use the
    //   mutation transmission evidence: preparation errors are stamped
    //   Unsent relative to the mutation, while an in-flight mutation can
    //   be uncertain.
    // - Stream-level `Terminated` is reserved for failures that
    //   prevent any further folder attempts at all (auth lost,
    //   schema break, capability shift). Those classify as
    //   terminal or as an engine directive; we surface them with
    //   `Terminated` and stop.
    // - Anything else (transient transport, rate limit, per-folder
    //   server error) emits per-item `Uncertain` for the failing
    //   folder and continues to the next folder.
    for (folder, ids) in sorted_mutation_groups(std::mem::take(grouped)) {
        match run_folder_mutation(account, &folder, ids, kind, move_destination).await {
            Ok(results) => {
                if tx
                    .send(batch(results, PageBoundary::Page, None))
                    .await
                    .is_err()
                {
                    return Err(());
                }
            }
            // The folder path hands the still-unaccounted ids back with its
            // error: nothing before the first outcome is minted consumes
            // them, so the happy path never has to clone them just to keep
            // this lane fed.
            Err(FolderMutationFailure { error, unaccounted }) => {
                // SELECT and its response describe preparation, not the
                // STORE / MOVE / EXPUNGE represented by these ids. Preserve
                // the failure classification but reset its attempt evidence
                // to the operation that was never sent. Shared ACL denials
                // become folder-scoped revocations here, before the
                // account-wide terminal check below.
                let entry = account.folders.get(&folder);
                let shared_owner = entry.as_ref().and_then(|entry| entry.shared_owner.as_ref());
                let account_err = super::error::shared_folder_error(
                    error.with_attempt(TransmissionState::Unsent),
                    &folder,
                    shared_owner,
                    super::error::ImapErrorContext::operation(mutation_operation(kind))
                        .with_folder_scope(&folder),
                );
                if stream_terminating(&account_err) {
                    let _ = tx.send(SyncEvent::Terminated(account_err)).await;
                    return Err(());
                }
                let verdict = mutation_error_verdict(&account_err);
                let outcomes = unaccounted
                    .into_iter()
                    .map(|id| {
                        let item = BatchItemId(
                            super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0,
                        );
                        match &verdict {
                            Verdict::Uncertain(error) => {
                                ItemOutcome::Uncertain(BatchUncertain::new(item, error.clone()))
                            }
                            Verdict::Failed(error) => {
                                ItemOutcome::Failed(BatchFailure::new(item, error.clone()))
                            }
                            Verdict::Succeeded(_) => {
                                unreachable!("a folder error cannot produce a successful verdict")
                            }
                        }
                    })
                    .collect();
                if tx
                    .send(batch(outcomes, PageBoundary::Page, None))
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

fn sorted_mutation_groups(
    grouped: HashMap<String, (MailboxName, Vec<DecodedObjectId>)>,
) -> Vec<(MailboxName, Vec<DecodedObjectId>)> {
    let mut grouped: Vec<_> = grouped.into_values().collect();
    grouped.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
    grouped
}

/// Validate a bulk-move destination before opening any source folder. This
/// is our own request validation, so every submitted target must fail rather
/// than being labelled uncertain by the per-folder error path.
fn validated_move_destination(kind: &MutationKind) -> Result<Option<MailboxName>, AccountError> {
    let MutationKind::Move(destination) = kind else {
        return Ok(None);
    };
    let MembershipScope::Folder(destination) = destination else {
        return Err(invalid_move_destination_error());
    };
    MailboxName::new(destination.0.clone())
        .map(Some)
        .map_err(|_| invalid_move_destination_error())
}

/// A folder-level mutation failure that happened before any of the folder's
/// ids were accounted for, carrying those ids back to the caller so it can
/// mint their outcomes. Every failure this type can express is raised while
/// the id vector is still whole, which is what lets the caller move the ids
/// in rather than clone them for a lane that usually never runs.
struct FolderMutationFailure {
    error: crate::Error,
    unaccounted: Vec<DecodedObjectId>,
}

/// Open the folder without replaying the synchronization baseline.
/// Split out from `run_folder_mutation` so every fallible step that precedes
/// the first minted outcome sits in one place, and the id vector can be
/// handed back untouched when any of them fails.
async fn open_folder_for_mutation(
    account: &ImapAccount,
    folder: &MailboxName,
) -> Result<(super::PooledConn, u32), crate::Error> {
    let mut conn = account.checkout_for_folder(folder).await?;
    // Mutation admission needs the live UIDVALIDITY, not a replay of all
    // changes since the consumer's checkpoint. Keep its existing MODSEQ cache
    // as the opportunistic STORE guard rather than refreshing it from a
    // mutation's own QRESYNC SELECT before testing for a conflict.
    let selected = account
        .select_folder(&mut conn, folder, None, false)
        .await?;
    let uidvalidity = super::selected_uidvalidity(&selected.mailbox)?;
    Ok((conn, uidvalidity))
}

async fn run_folder_mutation(
    account: &ImapAccount,
    folder: &MailboxName,
    ids: Vec<DecodedObjectId>,
    kind: &MutationKind,
    move_destination: Option<&MailboxName>,
) -> Result<Vec<ItemOutcome<MutationSuccess>>, FolderMutationFailure> {
    let (conn, uidvalidity) = match open_folder_for_mutation(account, folder).await {
        Ok(opened) => opened,
        Err(error) => {
            return Err(FolderMutationFailure {
                error,
                unaccounted: ids,
            });
        }
    };
    let (valid, stale) = split_by_uidvalidity(ids, uidvalidity);
    let stale_results = failed_all(
        stale,
        uidvalidity_changed_error(mutation_operation(kind), folder),
    );
    if let MutationKind::Flags(op) = kind {
        let mut results = stale_results;
        results
            .extend(run_flag_mutation_groups(account, &conn, folder, uidvalidity, valid, op).await);
        return Ok(results);
    }
    if matches!(kind, MutationKind::Destroy) {
        let mut results = stale_results;
        results
            .extend(run_destroy_mutation_groups(account, &conn, folder, uidvalidity, valid).await);
        return Ok(results);
    }
    let operation = mutation_operation(kind);
    let target_batch = TargetBatch::new(valid);
    let uids: Vec<u32> = target_batch.uids().to_vec();
    let wire_set = target_batch.uid_set().cloned();

    // `None` means no id reached the operand, so no command is sent - but
    // the batch still owes an outcome for every id it holds, which `settle`
    // mints from the excluded lane.
    let outcome = match wire_set {
        None => None,
        Some(uid_set) => Some(match kind {
            MutationKind::Flags(_) => unreachable!("flags handled above"),
            MutationKind::Move(_) => {
                let destination =
                    move_destination.expect("bulk-move destination validated before folder loop");
                conn.connection()
                    .uid_move_messages(
                        uid_set.as_sequence_set(),
                        destination.as_str(),
                        account.command_timeout(),
                    )
                    .await
                    .map(|_| StoreWireOutcome::Applied)
            }
            MutationKind::Destroy => unreachable!("destroy handled above"),
        }),
    };

    // The wire answer and its folder-level side effect are resolved before
    // the settle: the classifier runs once per target and must decide only
    // that target's lane.
    let classified = match outcome {
        None => None,
        Some(Ok(outcome)) => {
            account.folders.clear_modseqs(folder, uidvalidity, &uids);
            Some(Ok(outcome))
        }
        Some(Err(err)) => Some(Err(super::account_error_with(
            err,
            super::error::ImapErrorContext::operation(operation).with_folder_scope(folder),
        ))),
    };

    let mut results = stale_results;
    match classified {
        None => results.extend(target_batch.settle_unsent(operation, folder)),
        Some(Ok(outcome)) => {
            results.extend(target_batch.settle(operation, folder, |target| {
                let verdict = mutation_verdict(target.uid(), &outcome, operation, folder);
                target.seal(verdict)
            }));
        }
        Some(Err(error)) => {
            results.extend(target_batch.settle(operation, folder, |target| {
                let verdict = mutation_error_verdict(&error);
                target.seal(verdict)
            }));
        }
    }
    Ok(results)
}

async fn run_destroy_mutation_groups(
    account: &ImapAccount,
    conn: &super::PooledConn,
    folder: &MailboxName,
    uidvalidity: u32,
    ids: Vec<DecodedObjectId>,
) -> Vec<ItemOutcome<MutationSuccess>> {
    let mut results = Vec::new();
    for (unchanged_since, ids) in partition_by_modseq(account, folder, uidvalidity, ids) {
        let target_batch = TargetBatch::new(ids);
        let uids: Vec<u32> = target_batch.uids().to_vec();
        let Some(uid_set) = target_batch.uid_set().cloned() else {
            results.extend(target_batch.settle_unsent(AccountOperation::BulkDestroy, folder));
            continue;
        };
        let store = conn
            .connection()
            .uid_store(
                uid_set.as_sequence_set(),
                StoreOperation::AddSilent,
                &[Flag::Deleted],
                unchanged_since,
                account.command_timeout(),
            )
            .await;
        let outcome = match store {
            Ok(result) => StoreWireOutcome::from_store_result(&result),
            Err(err) => {
                let error = super::account_error_with(
                    err,
                    super::error::ImapErrorContext::operation(AccountOperation::BulkDestroy)
                        .with_folder_scope(folder),
                );
                results.extend(target_batch.settle(
                    AccountOperation::BulkDestroy,
                    folder,
                    |target| {
                        let verdict = mutation_error_verdict(&error);
                        target.seal(verdict)
                    },
                ));
                continue;
            }
        };
        let expunge_uids = applied_uids_after_store(&uids, &outcome);
        account
            .folders
            .clear_modseqs(folder, uidvalidity, &expunge_uids);
        if let Some(expunge_set) = target_batch.subset_uid_set(&expunge_uids)
            && let Err(err) = expunge_uids_or_fall_back(
                account,
                conn,
                expunge_set.as_sequence_set(),
                &expunge_uids,
            )
            .await
        {
            // The expunge failure applies only to the UIDs this round
            // actually marked `\Deleted`; the rest keep the STORE's own
            // verdict.
            let expunge_error = expunge_failed_after_delete_mark(folder, err);
            results.extend(
                target_batch.settle(AccountOperation::BulkDestroy, folder, |target| {
                    let verdict = if expunge_uids.contains(&target.uid()) {
                        mutation_error_verdict(&expunge_error)
                    } else {
                        mutation_verdict(
                            target.uid(),
                            &outcome,
                            AccountOperation::BulkDestroy,
                            folder,
                        )
                    };
                    target.seal(verdict)
                }),
            );
            continue;
        }
        results.extend(
            target_batch.settle(AccountOperation::BulkDestroy, folder, |target| {
                let verdict = mutation_verdict(
                    target.uid(),
                    &outcome,
                    AccountOperation::BulkDestroy,
                    folder,
                );
                target.seal(verdict)
            }),
        );
    }
    results
}

async fn run_flag_mutation_groups(
    account: &ImapAccount,
    conn: &super::PooledConn,
    folder: &MailboxName,
    uidvalidity: u32,
    ids: Vec<DecodedObjectId>,
    op: &FlagOp,
) -> Vec<ItemOutcome<MutationSuccess>> {
    let mut results = Vec::new();
    for (unchanged_since, ids) in partition_by_modseq(account, folder, uidvalidity, ids) {
        let target_batch = TargetBatch::new(ids);
        let uids: Vec<u32> = target_batch.uids().to_vec();
        let Some(uid_set) = target_batch.uid_set().cloned() else {
            results.extend(target_batch.settle_unsent(AccountOperation::UpdateFlags, folder));
            continue;
        };
        if let FlagOp::Patch { add, remove } = op
            && !add.is_empty()
            && !remove.is_empty()
        {
            results.extend(
                run_patch_mutation_group(
                    account,
                    conn,
                    folder,
                    uidvalidity,
                    PatchMutationWork {
                        targets: target_batch,
                        add,
                        remove,
                        unchanged_since,
                    },
                )
                .await,
            );
            continue;
        }
        let outcome = apply_flag_op(
            conn,
            uid_set.as_sequence_set(),
            op,
            unchanged_since,
            account.command_timeout(),
        )
        .await;
        // Resolve the wire answer and the MODSEQ-cache side effect before the
        // settle, so the classifier is a pure per-target decision.
        let classified = match outcome {
            Ok(outcome) => {
                let changed_uids = applied_uids_after_store(&uids, &outcome);
                account
                    .folders
                    .clear_modseqs(folder, uidvalidity, &changed_uids);
                Ok(outcome)
            }
            Err(err) => {
                if matches!(op, FlagOp::Patch { .. }) {
                    account.folders.clear_modseqs(folder, uidvalidity, &uids);
                }
                Err(super::account_error_with(
                    err,
                    super::error::ImapErrorContext::operation(AccountOperation::UpdateFlags)
                        .with_folder_scope(folder),
                ))
            }
        };
        match classified {
            Ok(outcome) => results.extend(target_batch.settle(
                AccountOperation::UpdateFlags,
                folder,
                |target| {
                    let verdict = mutation_verdict(
                        target.uid(),
                        &outcome,
                        AccountOperation::UpdateFlags,
                        folder,
                    );
                    target.seal(verdict)
                },
            )),
            Err(error) => results.extend(target_batch.settle(
                AccountOperation::UpdateFlags,
                folder,
                |target| {
                    let verdict = mutation_error_verdict(&error);
                    target.seal(verdict)
                },
            )),
        }
    }
    results
}

/// Apply the two halves of a flag patch with exact per-item accounting.
/// A guarded add can apply to the non-conflicting subset, so its successful
/// UIDs must receive the unguarded remove before they can be reported as
/// `Succeeded(Applied)`.
struct PatchMutationWork<'a> {
    targets: TargetBatch,
    add: &'a HashSet<String>,
    remove: &'a HashSet<String>,
    unchanged_since: Option<u64>,
}

async fn run_patch_mutation_group(
    account: &ImapAccount,
    conn: &super::PooledConn,
    folder: &MailboxName,
    uidvalidity: u32,
    work: PatchMutationWork<'_>,
) -> Vec<ItemOutcome<MutationSuccess>> {
    let target_batch = work.targets;
    let uids: Vec<u32> = target_batch.uids().to_vec();
    let set = target_batch
        .uid_set()
        .cloned()
        .expect("a two-sided patch group always has at least one target");
    let first = match store_flags(
        conn,
        set.as_sequence_set(),
        StoreOperation::AddSilent,
        work.add,
        work.unchanged_since,
        account.command_timeout(),
    )
    .await
    {
        Ok(first) => first,
        // A failure of the first STORE decides only this MODSEQ group. It
        // must not escape the group loop: outcomes already established for
        // earlier groups are known truth and may never be downgraded to the
        // folder-wide uncertain lane.
        Err(err) => {
            account.folders.clear_modseqs(folder, uidvalidity, &uids);
            let error = patch_first_store_failure(err, folder);
            return target_batch.settle(AccountOperation::UpdateFlags, folder, |target| {
                let verdict = mutation_error_verdict(&error);
                target.seal(verdict)
            });
        }
    };
    let applied = applied_uids_after_store(&uids, &first);
    account.folders.clear_modseqs(folder, uidvalidity, &applied);

    let second = match target_batch.subset_uid_set(&applied) {
        Some(set) => store_flags(
            conn,
            set.as_sequence_set(),
            StoreOperation::RemoveSilent,
            work.remove,
            None,
            account.command_timeout(),
        )
        .await
        .map_err(|err| {
            super::account_error_with(
                err,
                super::error::ImapErrorContext::operation(AccountOperation::UpdateFlags)
                    .with_folder_scope(folder),
            )
        }),
        None => Ok(StoreWireOutcome::Applied),
    };
    target_batch.settle(AccountOperation::UpdateFlags, folder, |target| {
        let verdict = patch_mutation_verdict(target.uid(), &first, &second, folder);
        target.seal(verdict)
    })
}

/// The error for a MODSEQ group whose guarded add STORE errored on the wire.
/// Scoped to the group's own ids: the surrounding loop keeps the outcomes it
/// has already established for other groups, matching the
/// single-outcome-per-id contract the sync engine relies on.
fn patch_first_store_failure(err: crate::Error, folder: &MailboxName) -> AccountError {
    super::account_error_with(
        err,
        super::error::ImapErrorContext::operation(AccountOperation::UpdateFlags)
            .with_folder_scope(folder),
    )
}

/// The verdict for one UID of a two-sided patch group.
///
/// A UID the guarded add did not apply to is decided entirely by that first
/// STORE (a `MODIFIED` conflict, or the whole-command rejection that
/// `PendingRetry`/`Failed` carry). Only a UID the add did apply to reaches
/// the unguarded remove, and only there can the second STORE's answer make
/// it `Succeeded(Applied)`.
fn patch_mutation_verdict(
    uid: u32,
    first: &StoreWireOutcome,
    second: &Result<StoreWireOutcome, AccountError>,
    folder: &MailboxName,
) -> Verdict<MutationSuccess> {
    if !store_applied_uid(uid, first) {
        return mutation_verdict(uid, first, AccountOperation::UpdateFlags, folder);
    }
    match second {
        Ok(StoreWireOutcome::Applied) => Verdict::Succeeded(MutationSuccess::Applied),
        Ok(_) => Verdict::Failed(store_failed_error(AccountOperation::UpdateFlags, folder)),
        Err(error) => mutation_error_verdict(error),
    }
}

/// The single predicate for "the STORE applied to this UID".
fn store_applied_uid(uid: u32, outcome: &StoreWireOutcome) -> bool {
    match outcome {
        StoreWireOutcome::Applied => true,
        StoreWireOutcome::Modified(modified) => !modified.contains(uid),
        StoreWireOutcome::PendingRetry(_) | StoreWireOutcome::Failed => false,
    }
}

fn applied_uids_after_store(requested_uids: &[u32], outcome: &StoreWireOutcome) -> Vec<u32> {
    requested_uids
        .iter()
        .copied()
        .filter(|uid| store_applied_uid(*uid, outcome))
        .collect()
}

fn partition_by_modseq(
    account: &ImapAccount,
    folder: &MailboxName,
    uidvalidity: u32,
    ids: Vec<DecodedObjectId>,
) -> Vec<(Option<u64>, Vec<DecodedObjectId>)> {
    let mut protected: BTreeMap<u64, Vec<DecodedObjectId>> = BTreeMap::new();
    let mut unprotected = Vec::new();
    for id in ids {
        if let Some(modseq) = account.folders.modseq(folder, uidvalidity, id.uid) {
            protected.entry(modseq).or_default().push(id);
        } else {
            unprotected.push(id);
        }
    }
    let mut groups = protected
        .into_iter()
        .map(|(modseq, ids)| (Some(modseq), ids))
        .collect::<Vec<_>>();
    if !unprotected.is_empty() {
        groups.push((None, unprotected));
    }
    groups
}

async fn apply_flag_op(
    conn: &super::PooledConn,
    set: &crate::types::SequenceSet,
    op: &FlagOp,
    unchanged_since: Option<u64>,
    timeout: std::time::Duration,
) -> Result<StoreWireOutcome, crate::Error> {
    match op {
        FlagOp::Add(flags) => {
            store_flags(
                conn,
                set,
                StoreOperation::AddSilent,
                flags,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Remove(flags) => {
            store_flags(
                conn,
                set,
                StoreOperation::RemoveSilent,
                flags,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Set(flags) => {
            store_flags(
                conn,
                set,
                StoreOperation::ReplaceSilent,
                flags,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Patch { add, remove } if add.is_empty() && remove.is_empty() => {
            Ok(StoreWireOutcome::Applied)
        }
        FlagOp::Patch { add, remove } if add.is_empty() => {
            store_flags(
                conn,
                set,
                StoreOperation::RemoveSilent,
                remove,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Patch { add, remove } if remove.is_empty() => {
            store_flags(
                conn,
                set,
                StoreOperation::AddSilent,
                add,
                unchanged_since,
                timeout,
            )
            .await
        }
        FlagOp::Patch { .. } => {
            unreachable!("two-sided patches are handled with per-UID accounting before this call")
        }
        // `FlagOp` is published and `#[non_exhaustive]`: this arm is a variant
        // added upstream that this crate has not learned, not a server gap.
        _ => Err(crate::Error::UnsupportedOperation(
            "flag operation not implemented by bifrost-imap".into(),
        )),
    }
}

async fn store_flags(
    conn: &super::PooledConn,
    set: &crate::types::SequenceSet,
    operation: StoreOperation,
    flags: &HashSet<String>,
    unchanged_since: Option<u64>,
    timeout: std::time::Duration,
) -> Result<StoreWireOutcome, crate::Error> {
    let flags = flags
        .iter()
        .map(|flag| Flag::from(flag.as_str()))
        .collect::<Vec<_>>();
    let result = conn
        .connection()
        .uid_store(set, operation, &flags, unchanged_since, timeout)
        .await?;
    Ok(StoreWireOutcome::from_store_result(&result))
}

// protocol-specific: IMAP STORE can return MODIFIED before it maps to shared MutationOutcome.
#[derive(Debug, Clone, PartialEq, Eq)]
enum StoreWireOutcome {
    Applied,
    Modified(CompactUidSet),
    PendingRetry(CompactUidSet),
    Failed,
}

impl StoreWireOutcome {
    /// Classify a completed STORE.
    ///
    /// Takes the whole [`StoreResult`] rather than a `(code, tagged_ok)` pair
    /// on purpose: the tagged status is the half a caller silently drops, and
    /// dropping it turns a refusal into `Applied`. There is no constructor
    /// that lets a caller supply the code without the status.
    fn from_store_result(result: &crate::types::StoreResult) -> Self {
        let tagged_ok = matches!(result.status, crate::types::response::StatusKind::Ok);
        match (tagged_ok, modified_uids(result.code.as_ref())) {
            (true, None) => Self::Applied,
            (true, Some(modified)) => Self::Modified(modified),
            (false, Some(modified)) => Self::PendingRetry(modified),
            (false, None) => Self::Failed,
        }
    }
}

fn modified_uids(code: Option<&ResponseCode>) -> Option<CompactUidSet> {
    match code {
        // Keep the server's set compact: classification asks membership only
        // for submitted targets, never expands unsolicited numeric spans.
        Some(ResponseCode::Modified(ranges)) => Some(CompactUidSet::from_ranges(
            ranges
                .iter()
                .map(|range| {
                    let end = range.end.unwrap_or(range.start);
                    crate::types::UidRange::range(range.start.min(end), range.start.max(end))
                })
                .collect(),
        )),
        _ => None,
    }
}

/// The verdict for one UID under one STORE answer.
fn mutation_verdict(
    uid: u32,
    outcome: &StoreWireOutcome,
    operation: AccountOperation,
    folder: &MailboxName,
) -> Verdict<MutationSuccess> {
    match outcome {
        StoreWireOutcome::Applied => Verdict::Succeeded(MutationSuccess::Applied),
        StoreWireOutcome::Modified(modified) => {
            if modified.contains(uid) {
                Verdict::Failed(concurrency_conflict_error(operation, folder))
            } else {
                Verdict::Succeeded(MutationSuccess::Applied)
            }
        }
        // Tagged-NO STORE carrying `[MODIFIED ...]`: the server
        // explicitly rejected the command and named the conflicting
        // UIDs. This is a *server-acknowledged* conflict - a complete
        // response crossed the wire, so there is no transmission
        // ambiguity. Per the error model the conflicting UIDs are
        // therefore `Failed(ConcurrencyConflict)` (deriving
        // `Retry::AfterStateRefresh`), NOT `Uncertain` (which is the
        // analogue of an in-flight transport drop and queues for
        // read-back). The remaining UIDs were not committed because the
        // whole STORE was rejected.
        StoreWireOutcome::PendingRetry(modified) => {
            if modified.contains(uid) {
                Verdict::Failed(concurrency_conflict_error(operation, folder))
            } else {
                Verdict::Failed(store_failed_error(operation, folder))
            }
        }
        StoreWireOutcome::Failed => Verdict::Failed(store_failed_error(operation, folder)),
    }
}

fn failed_all(ids: Vec<DecodedObjectId>, error: AccountError) -> Vec<ItemOutcome<MutationSuccess>> {
    ids.into_iter()
        .map(|id| {
            let item = BatchItemId(super::encode_object_id(&id.folder, id.uidvalidity, id.uid).0);
            ItemOutcome::Failed(BatchFailure::new(item, error.clone()))
        })
        .collect()
}

/// Which lane a wire error puts one target in. Transmission evidence
/// decides: an `InFlight` attempt is `Uncertain` (the engine reads it back),
/// an `Unsent` or server-acknowledged failure is `Failed`.
fn mutation_error_verdict(error: &AccountError) -> Verdict<MutationSuccess> {
    let in_flight = error.chain().iter().any(|cause| {
        matches!(
            cause,
            Cause::Attempt(attempt)
                if attempt.transmission_state == TransmissionState::InFlight
        )
    });
    if in_flight {
        Verdict::Uncertain(error.clone())
    } else {
        Verdict::Failed(error.clone())
    }
}

fn invalid_move_destination_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("bulk-move destination is not a sendable mailbox"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::BulkMove)
    .try_build()
    .expect("valid account error classification")
}

/// Build a `ConcurrencyConflict` `AccountError` for STORE UNCHANGEDSINCE
/// conflicts. Takes the caller's `AccountOperation` so flag, move, and
/// destroy paths each surface their own operation - the central recovery
/// mapping needs the correct op to pick `Retry::AfterStateRefresh` for
/// the right kind of work.
fn concurrency_conflict_error(operation: AccountOperation, folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::ConcurrencyConflict,
        Cause::State(StateCause::ConcurrencyConflict),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Cursor(
        bifrost_types::CursorScope::Folder(bifrost_types::FolderId(folder.as_str().to_owned())),
    ))
    .try_build()
    .expect("valid account error classification")
}

/// Build a generic `Request(Malformed)` for UIDVALIDITY mismatch before mutation.
fn uidvalidity_changed_error(operation: AccountOperation, folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("UIDVALIDITY changed before mutation"),
        }),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Cursor(
        bifrost_types::CursorScope::Folder(bifrost_types::FolderId(folder.as_str().to_owned())),
    ))
    .try_build()
    .expect("valid account error classification")
}

/// Build a generic server refusal for a STORE command failure without a
/// specific per-item response code. Takes the caller's `AccountOperation`
/// so move / destroy / flag mutations each carry their own op.
fn store_failed_error(operation: AccountOperation, folder: &MailboxName) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
        Cause::Server(ServerCause::Error { status: None }),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(bifrost_types::ErrorScope::Cursor(
        bifrost_types::CursorScope::Folder(bifrost_types::FolderId(folder.as_str().to_owned())),
    ))
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .text(DiagnosticText::support_only(
        "STORE command failed with no per-item response code",
    ))
    .try_build()
    .expect("valid account error classification")
}

/// Remove the just-marked messages, with a bounded fallback for servers
/// that have neither UIDPLUS nor IMAP4rev2.
///
/// `UID EXPUNGE` (RFC 4315 Section 2) is the only command that names the
/// messages to remove. Plain `EXPUNGE` (RFC 3501 Section 6.4.3) removes
/// *every* `\Deleted` message in the mailbox, including ones another
/// client marked and has not committed to removing - so it is only run
/// when `UID SEARCH DELETED` shows the mailbox's `\Deleted` set is
/// a subset of the set this batch marked. This is a check at SEARCH time,
/// not an atomic guarantee: another client can mark a message `\Deleted`
/// before the following plain `EXPUNGE`, and IMAP has no command to close
/// that race without UIDPLUS. When a foreign `\Deleted` message is already
/// present at SEARCH time, return the original `MissingCapability` and leave
/// this batch's messages flagged rather than knowingly expunging foreign mail.
pub(super) async fn expunge_uids_or_fall_back(
    account: &ImapAccount,
    conn: &super::PooledConn,
    expunge_set: &crate::types::SequenceSet,
    expunge_uids: &[u32],
) -> Result<(), crate::Error> {
    let timeout = account.command_timeout();
    match conn.connection().uid_expunge(expunge_set, timeout).await {
        Ok(_) => return Ok(()),
        Err(crate::Error::MissingCapability(_)) => {}
        Err(err) => return Err(err),
    }
    let deleted = conn.connection().uid_search("DELETED", timeout).await?;
    let marked: std::collections::BTreeSet<u32> = expunge_uids.iter().copied().collect();
    let found: std::collections::BTreeSet<u32> = deleted.ids.into_iter().collect();
    if !found.is_subset(&marked) {
        return Err(crate::Error::MissingCapability("UIDPLUS".into()));
    }
    conn.connection().expunge(timeout).await?;
    Ok(())
}

/// The STORE half of IMAP deletion is already acknowledged before this
/// helper runs, so a failed UID EXPUNGE leaves a confirmed server-side
/// partial effect: those messages are still flagged `\Deleted`.
///
/// The failure keeps its own classification. Collapsing every EXPUNGE
/// error into `Protocol(PartialResponse)` would erase exactly the
/// distinctions `reference/error-model.md` asks the producer to preserve -
/// an auth loss, an ACL denial, a quota or rate-limit throttle, and a
/// capability loss each derive a different `RecoveryClass`, and none of
/// them is served by a generic reconcile. What this helper adds is the
/// side-effect evidence, as support-only diagnostic text riding on the
/// real error, so the engine can see both "why it failed" and "what
/// already landed".
fn expunge_failed_after_delete_mark(folder: &MailboxName, error: crate::Error) -> AccountError {
    super::account_error_with(
        error,
        super::error::ImapErrorContext::operation(AccountOperation::BulkDestroy)
            .with_folder_scope(folder)
            .with_extra_text(DiagnosticText::support_only(format!(
                "UID EXPUNGE failed for {} after UID STORE +FLAGS.SILENT \\Deleted was \
                 acknowledged; those messages remain flagged \\Deleted server-side",
                folder.as_str(),
            ))),
    )
}

fn split_by_uidvalidity(
    ids: Vec<DecodedObjectId>,
    uidvalidity: u32,
) -> (Vec<DecodedObjectId>, Vec<DecodedObjectId>) {
    let mut valid = Vec::new();
    let mut stale = Vec::new();
    for id in ids {
        if id.uidvalidity == uidvalidity {
            valid.push(id);
        } else {
            stale.push(id);
        }
    }
    (valid, stale)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::UidRange;

    #[test]
    fn flag_op_maps_to_wire_atoms() {
        let flags = HashSet::from(["\\Seen".to_owned(), "$Important".to_owned()]);
        let atoms = flags
            .iter()
            .map(|flag| Flag::from(flag.as_str()).as_imap_str().to_owned())
            .collect::<HashSet<_>>();
        assert!(atoms.contains("\\Seen"));
        assert!(atoms.contains("$Important"));
    }

    #[test]
    fn mutation_groups_are_processed_in_mailbox_order() {
        let mut grouped = HashMap::new();
        for name in ["Zeta", "Archive", "INBOX"] {
            let folder = MailboxName::new(name).expect("valid mailbox");
            grouped.insert(name.to_owned(), (folder, Vec::new()));
        }
        let names: Vec<_> = sorted_mutation_groups(grouped)
            .into_iter()
            .map(|(folder, _)| folder.as_str().to_owned())
            .collect();
        assert_eq!(names, ["Archive", "INBOX", "Zeta"]);
    }

    #[test]
    fn modified_code_maps_per_uid_outcomes() {
        let code = ResponseCode::Modified(vec![UidRange::range(2, 3)]);
        let result = |status, code: Option<ResponseCode>| crate::types::StoreResult {
            fetches: Vec::new(),
            status,
            code,
        };
        assert_eq!(
            StoreWireOutcome::from_store_result(&result(
                crate::types::response::StatusKind::Ok,
                Some(code.clone())
            )),
            StoreWireOutcome::Modified(CompactUidSet::from_uids([2, 3]))
        );
        assert_eq!(
            StoreWireOutcome::from_store_result(&result(
                crate::types::response::StatusKind::No,
                Some(code)
            )),
            StoreWireOutcome::PendingRetry(CompactUidSet::from_uids([2, 3]))
        );
        assert_eq!(
            StoreWireOutcome::from_store_result(&result(
                crate::types::response::StatusKind::No,
                None
            )),
            StoreWireOutcome::Failed
        );
    }

    #[test]
    fn pending_retry_conflict_is_failed_not_uncertain() {
        // A tagged-NO STORE carrying `[MODIFIED ...]` is a
        // server-acknowledged conflict with no transmission ambiguity:
        // the conflicting UIDs must surface as `Failed(ConcurrencyConflict)`
        // (driving Retry::AfterStateRefresh), never `Uncertain` (the
        // in-flight-drop analogue that queues for read-back).
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let ids = vec![
            DecodedObjectId {
                folder: folder.clone(),
                uidvalidity: 7,
                uid: 2,
            },
            DecodedObjectId {
                folder: folder.clone(),
                uidvalidity: 7,
                uid: 5,
            },
        ];
        let outcome = StoreWireOutcome::PendingRetry(CompactUidSet::from_uids([2]));
        let outcomes =
            TargetBatch::new(ids).settle(AccountOperation::UpdateFlags, &folder, |target| {
                let verdict = mutation_verdict(
                    target.uid(),
                    &outcome,
                    AccountOperation::UpdateFlags,
                    &folder,
                );
                target.seal(verdict)
            });
        // Every item is Failed - none Uncertain, none Succeeded.
        assert!(
            outcomes.iter().all(|o| matches!(o, ItemOutcome::Failed(_))),
            "PendingRetry conflict must surface as Failed, never Uncertain"
        );
        // The conflicting UID (2) carries ConcurrencyConflict.
        let has_conflict = outcomes.iter().any(|o| {
            matches!(o, ItemOutcome::Failed(f) if matches!(
                f.error.kind(),
                AccountErrorKind::ConcurrencyConflict
            ))
        });
        assert!(
            has_conflict,
            "conflicting UID must surface ConcurrencyConflict"
        );
    }

    #[test]
    fn applied_uids_after_store_excludes_modified_conflicts() {
        assert_eq!(
            applied_uids_after_store(&[1, 2, 3, 4], &StoreWireOutcome::Applied),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            applied_uids_after_store(
                &[1, 2, 3, 4],
                &StoreWireOutcome::Modified(CompactUidSet::from_uids([2, 4])),
            ),
            vec![1, 3]
        );
        assert!(
            applied_uids_after_store(
                &[1, 2, 3, 4],
                &StoreWireOutcome::PendingRetry(CompactUidSet::from_uids([2, 4])),
            )
            .is_empty()
        );
        assert!(applied_uids_after_store(&[1], &StoreWireOutcome::Failed).is_empty());
    }

    #[test]
    fn concurrency_conflict_error_reports_callers_operation() {
        // concurrency_conflict_error / store_failed_error /
        // uidvalidity_changed_error must surface the caller's op so
        // recovery routes correctly for non-idempotent paths.
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        for op in [
            AccountOperation::UpdateFlags,
            AccountOperation::BulkMove,
            AccountOperation::BulkDestroy,
        ] {
            let err = concurrency_conflict_error(op, &folder);
            assert_eq!(err.operation(), Some(op));
            // Folder producers use the cursor-shaped folder scope.
            assert!(matches!(
                err.scope(),
                Some(bifrost_types::ErrorScope::Cursor(
                    bifrost_types::CursorScope::Folder(_)
                ))
            ));
        }
    }

    #[test]
    fn store_failed_error_reports_callers_operation() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let err = store_failed_error(AccountOperation::BulkDestroy, &folder);
        assert_eq!(err.operation(), Some(AccountOperation::BulkDestroy));
        assert!(matches!(
            err.scope(),
            Some(bifrost_types::ErrorScope::Cursor(
                bifrost_types::CursorScope::Folder(_)
            ))
        ));
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Server(ServerErrorKind::Error { status: None })
        ));
    }

    /// The partial-effect note must ride on the real error, not replace it.
    /// An ACL denial and a quota exhaustion derive different recovery, and
    /// an engine that saw only `Protocol(PartialResponse)` would reconcile
    /// (or retry) both identically.
    #[test]
    fn expunge_failure_keeps_its_classification_and_adds_the_side_effect_note() {
        let denied = expunge_failed_after_delete_mark(
            &folder(),
            crate::Error::No {
                text: "permission denied".to_owned(),
                code: Some(crate::types::ResponseCode::NoPerm),
                attempt: None,
            },
        );
        assert!(
            matches!(
                denied.kind(),
                AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
            ),
            "ACL denial must not be laundered into a generic partial response: {:?}",
            denied.kind()
        );

        let over_quota = expunge_failed_after_delete_mark(
            &folder(),
            crate::Error::No {
                text: "over quota".to_owned(),
                code: Some(crate::types::ResponseCode::OverQuota),
                attempt: None,
            },
        );
        assert!(matches!(
            over_quota.kind(),
            AccountErrorKind::Server(bifrost_types::ServerErrorKind::QuotaExhausted)
        ));
        assert_ne!(
            denied.recovery(),
            over_quota.recovery(),
            "distinct failures must keep deriving distinct recovery classes"
        );

        for err in [&denied, &over_quota] {
            assert!(
                err.support_consented().support_text.iter().any(|text| {
                    text.contains("UID EXPUNGE failed")
                        && text.contains("remain flagged \\Deleted server-side")
                }),
                "the confirmed partial side effect must be attached"
            );
        }
    }

    #[test]
    fn uidvalidity_changed_error_reports_callers_operation() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let err = uidvalidity_changed_error(AccountOperation::BulkMove, &folder);
        assert_eq!(err.operation(), Some(AccountOperation::BulkMove));
    }

    #[test]
    fn stream_terminating_keeps_account_authz_and_schema_terminal() {
        use bifrost_types::{
            AccessCause, AccessErrorKind, AccountErrorBuilder, AuthCause, AuthErrorKind, MailboxId,
            StateCause, SyncStateErrorKind,
        };
        let auth = AccountErrorBuilder::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("auth error");
        assert!(stream_terminating(&auth));

        let schema = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("schema error");
        assert!(stream_terminating(&schema));

        let personal_permission_denied = AccountErrorBuilder::new(
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied { resource: None }),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("personal-folder permission error");
        assert!(stream_terminating(&personal_permission_denied));

        let shared_revoked = super::super::error::scope_revoked(
            &folder(),
            &MailboxId("alice".to_owned()),
            AccountOperation::UpdateFlags,
        );
        assert!(!stream_terminating(&shared_revoked));

        let transient = AccountErrorBuilder::new(
            AccountErrorKind::Server(bifrost_types::ServerErrorKind::Unavailable),
            Cause::Server(bifrost_types::ServerCause::Unavailable { retry_hint: None }),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::UpdateFlags)
        .try_build()
        .expect("server error");
        assert!(!stream_terminating(&transient));
    }

    fn folder() -> MailboxName {
        MailboxName::new("INBOX").expect("valid mailbox")
    }

    fn ids(uids: &[u32]) -> Vec<DecodedObjectId> {
        uids.iter()
            .map(|uid| DecodedObjectId {
                folder: folder(),
                uidvalidity: 7,
                uid: *uid,
            })
            .collect()
    }

    /// Settle a real `TargetBatch` over `uids`, classifying each target with
    /// `classify`. The per-item accounting tests drive the production
    /// `settle` rather than calling a verdict function directly, so what
    /// they assert is the accounting the mutation paths actually use.
    fn settled<F>(
        uids: &[u32],
        operation: AccountOperation,
        mut classify: F,
    ) -> Vec<ItemOutcome<MutationSuccess>>
    where
        F: FnMut(u32) -> Verdict<MutationSuccess>,
    {
        TargetBatch::new(ids(uids)).settle(operation, &folder(), |target| {
            let verdict = classify(target.uid());
            target.seal(verdict)
        })
    }

    #[test]
    fn huge_modified_span_stays_compact_and_classifies_only_submitted_targets() {
        let result = crate::types::StoreResult {
            fetches: Vec::new(),
            status: crate::types::response::StatusKind::Ok,
            code: Some(ResponseCode::Modified(vec![UidRange::range(4000000000, 1)])),
        };
        let outcome = StoreWireOutcome::from_store_result(&result);
        let StoreWireOutcome::Modified(ref modified) = outcome else {
            panic!("expected conflicts");
        };
        assert_eq!(modified.ranges(), &[UidRange::range(1, 4000000000)]);
        assert_eq!(
            applied_uids_after_store(&[2, 7, u32::MAX], &outcome),
            vec![u32::MAX]
        );
    }

    #[test]
    fn only_the_modified_response_code_yields_conflicting_uids() {
        assert_eq!(modified_uids(None), None);
        assert_eq!(modified_uids(Some(&ResponseCode::Alert)), None);
        assert_eq!(
            modified_uids(Some(&ResponseCode::Modified(vec![
                UidRange::single(2),
                UidRange::range(5, 7),
            ]))),
            Some(CompactUidSet::from_uids([2, 5, 6, 7]))
        );
        // An empty MODIFIED list is still a MODIFIED response: the STORE
        // outcome must not silently degrade to "no conflict reported".
        assert_eq!(
            modified_uids(Some(&ResponseCode::Modified(Vec::new()))),
            Some(CompactUidSet::default())
        );
    }

    // A tagged-OK STORE carrying `[MODIFIED ...]` partially applied: the
    // named UIDs conflicted, everything else in the same command landed.
    #[test]
    fn modified_outcome_splits_conflicts_from_successes_per_uid() {
        let outcome = StoreWireOutcome::Modified(CompactUidSet::from_uids([2]));
        let outcomes = settled(&[1, 2, 3], AccountOperation::UpdateFlags, |uid| {
            mutation_verdict(uid, &outcome, AccountOperation::UpdateFlags, &folder())
        });
        assert_eq!(outcomes.len(), 3);
        let failed: Vec<&BatchFailure> = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                ItemOutcome::Failed(failure) => Some(failure),
                _ => None,
            })
            .collect();
        assert_eq!(failed.len(), 1, "only the MODIFIED uid conflicts");
        assert!(
            failed[0].item.0.ends_with(":7:2"),
            "item: {}",
            failed[0].item.0
        );
        assert!(matches!(
            failed[0].error.kind(),
            AccountErrorKind::ConcurrencyConflict
        ));
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
                .count(),
            2,
        );
    }

    #[test]
    fn partially_conflicting_patch_only_succeeds_after_the_remove_lands() {
        let first = StoreWireOutcome::Modified(CompactUidSet::from_uids([2]));
        let second = Ok(StoreWireOutcome::Applied);
        let outcomes = settled(&[1, 2, 3], AccountOperation::UpdateFlags, |uid| {
            patch_mutation_verdict(uid, &first, &second, &folder())
        });
        assert_eq!(outcomes.len(), 3);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
                .count(),
            2,
            "only the add-applied subset may succeed after the remove does too",
        );
        assert!(outcomes.iter().any(|outcome| {
            matches!(outcome, ItemOutcome::Failed(failure) if matches!(
                failure.error.kind(),
                AccountErrorKind::ConcurrencyConflict
            ))
        }));
    }

    #[test]
    fn acknowledged_second_half_rejection_is_failed() {
        let first = StoreWireOutcome::Modified(CompactUidSet::from_uids([2]));
        let second = Err(store_failed_error(AccountOperation::UpdateFlags, &folder()));
        let outcomes = settled(&[1, 2], AccountOperation::UpdateFlags, |uid| {
            patch_mutation_verdict(uid, &first, &second, &folder())
        });
        assert!(
            outcomes
                .iter()
                .all(|outcome| matches!(outcome, ItemOutcome::Failed(_)))
        );
        assert!(outcomes.iter().any(|outcome| {
            matches!(outcome, ItemOutcome::Failed(failure) if matches!(
                failure.error.kind(),
                AccountErrorKind::ConcurrencyConflict
            ))
        }));
        assert!(
            outcomes
                .iter()
                .all(|outcome| !matches!(outcome, ItemOutcome::Succeeded(_))),
            "a failed remove after a successful add must not claim a complete patch",
        );
    }

    // A wire error on the first STORE of a two-sided patch decides only the
    // MODSEQ group it belongs to. Earlier groups already carry known
    // outcomes, and known truth must never be downgraded by a later failure.
    #[test]
    fn first_store_error_in_a_patch_group_keeps_earlier_group_outcomes() {
        let first = StoreWireOutcome::Applied;
        let second = Ok(StoreWireOutcome::Applied);
        let mut results = settled(&[1, 2], AccountOperation::UpdateFlags, |uid| {
            patch_mutation_verdict(uid, &first, &second, &folder())
        });
        assert_eq!(
            results
                .iter()
                .filter(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
                .count(),
            2,
        );
        let error = patch_first_store_failure(
            crate::Error::Protocol("STORE never answered".into()),
            &folder(),
        );
        results.extend(settled(&[5], AccountOperation::UpdateFlags, |_| {
            mutation_error_verdict(&error)
        }));
        assert_eq!(results.len(), 3);
        assert_eq!(
            results
                .iter()
                .filter(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
                .count(),
            2,
            "the failing group must not erase outcomes proven for other groups",
        );
        let failed: Vec<&BatchFailure> = results
            .iter()
            .filter_map(|outcome| match outcome {
                ItemOutcome::Failed(failure) => Some(failure),
                _ => None,
            })
            .collect();
        assert_eq!(failed.len(), 1);
        assert_eq!(
            failed[0].item,
            BatchItemId(super::super::encode_object_id(&folder(), 7, 5).0),
            "only the failing group's id may carry the failure",
        );
    }

    #[test]
    fn invalid_bulk_move_destination_is_rejected_before_folder_work() {
        let kind = MutationKind::Move(MembershipScope::Folder(bifrost_types::FolderId(
            "IN\r\nBOX".to_owned(),
        )));
        let err = validated_move_destination(&kind).expect_err("CRLF cannot be sent as a mailbox");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        assert_eq!(err.operation(), Some(AccountOperation::BulkMove));
    }

    #[test]
    fn applied_outcome_succeeds_every_target() {
        let outcome = StoreWireOutcome::Applied;
        let outcomes = settled(&[4, 5], AccountOperation::BulkMove, |uid| {
            mutation_verdict(uid, &outcome, AccountOperation::BulkMove, &folder())
        });
        assert!(
            outcomes
                .iter()
                .all(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
        );
    }

    // A tagged-NO STORE with no `[MODIFIED ...]` names no conflicting
    // UIDs, so every target fails - none may be reported Succeeded.
    #[test]
    fn failed_outcome_fails_every_target() {
        let outcome = StoreWireOutcome::Failed;
        let outcomes = settled(&[4, 5], AccountOperation::BulkDestroy, |uid| {
            mutation_verdict(uid, &outcome, AccountOperation::BulkDestroy, &folder())
        });
        assert_eq!(outcomes.len(), 2);
        for outcome in &outcomes {
            match outcome {
                ItemOutcome::Failed(failure) => assert_eq!(
                    failure.error.operation(),
                    Some(AccountOperation::BulkDestroy)
                ),
                other => panic!("expected Failed, got {other:?}"),
            }
        }
    }

    #[test]
    fn in_flight_mutation_error_is_uncertain_but_acknowledged_is_failed() {
        let build = |state| {
            AccountErrorBuilder::new(
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            )
            .protocol(Protocol::Imap)
            .operation(AccountOperation::UpdateFlags)
            .push_cause(Cause::Attempt(AttemptCause::new(state)))
            .try_build()
            .expect("valid mutation error")
        };

        let in_flight = build(TransmissionState::InFlight);
        let uncertain = settled(&[4], AccountOperation::UpdateFlags, |_| {
            mutation_error_verdict(&in_flight)
        });
        assert!(matches!(uncertain.as_slice(), [ItemOutcome::Uncertain(_)]));

        let acknowledged = build(TransmissionState::Acknowledged);
        let failed = settled(&[4], AccountOperation::UpdateFlags, |_| {
            mutation_error_verdict(&acknowledged)
        });
        assert!(matches!(failed.as_slice(), [ItemOutcome::Failed(_)]));
    }

    // The destroy path's expunge-failure split is now a per-UID decision
    // inside one settle: the UIDs this round marked `\Deleted` carry the
    // expunge failure, everything else keeps the STORE's own verdict.
    #[test]
    fn an_expunge_failure_applies_only_to_the_uids_it_marked() {
        let expunge_uids = [2u32];
        let outcome = StoreWireOutcome::Modified(CompactUidSet::from_uids([3]));
        let expunge_error = expunge_failed_after_delete_mark(
            &folder(),
            crate::Error::Protocol("EXPUNGE refused".into()),
        );
        let outcomes = settled(&[2, 3], AccountOperation::BulkDestroy, |uid| {
            if expunge_uids.contains(&uid) {
                mutation_error_verdict(&expunge_error)
            } else {
                mutation_verdict(uid, &outcome, AccountOperation::BulkDestroy, &folder())
            }
        });
        assert_eq!(outcomes.len(), 2);
        assert!(
            outcomes
                .iter()
                .all(|outcome| !matches!(outcome, ItemOutcome::Succeeded(_))),
            "neither the expunged nor the conflicting UID may claim success",
        );
        let conflicts = outcomes
            .iter()
            .filter(|outcome| {
                matches!(outcome, ItemOutcome::Failed(failure) if matches!(
                    failure.error.kind(),
                    AccountErrorKind::ConcurrencyConflict
                ))
            })
            .count();
        assert_eq!(conflicts, 1, "only the MODIFIED uid is a conflict");
    }

    // Every target handed to `failed_all` must come back as its own
    // `Failed` item carrying the encoded object id: a stale-UIDVALIDITY
    // batch must not collapse into one aggregate failure.
    #[test]
    fn failed_all_emits_one_outcome_per_target() {
        let error = uidvalidity_changed_error(AccountOperation::BulkMove, &folder());
        let outcomes = failed_all(ids(&[1, 2, 3]), error);
        assert_eq!(outcomes.len(), 3);
        let items: Vec<String> = outcomes
            .iter()
            .map(|outcome| match outcome {
                ItemOutcome::Failed(failure) => failure.item.0.clone(),
                other => panic!("expected Failed, got {other:?}"),
            })
            .collect();
        assert!(items[0].ends_with(":7:1"), "item: {}", items[0]);
        assert!(items[2].ends_with(":7:3"), "item: {}", items[2]);
    }

    #[test]
    fn stale_uidvalidity_targets_are_kept_for_failed_results() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let ids = vec![
            DecodedObjectId {
                folder: folder.clone(),
                uidvalidity: 9,
                uid: 1,
            },
            DecodedObjectId {
                folder,
                uidvalidity: 10,
                uid: 2,
            },
        ];

        let (valid, stale) = split_by_uidvalidity(ids, 10);
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].uid, 2);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].uid, 1);
    }
}
