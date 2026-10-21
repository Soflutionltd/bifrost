use std::collections::BTreeSet;

use bifrost_types::{
    AccountError, Change, ChangeCursor, Checkpoint, CostClass, CursorDescriptor, ObjectChange,
    ObjectChangeKind, PageBoundary, ScopeChange, ScopeChangeKind, SyncEvent, SyncStrategy, Warning,
    WarningKind,
};

use crate::types::{FetchAttr, MailboxName, SelectedMailbox, UidSet};

use super::{
    BATCH_ITEMS, CompactUidSet, FolderCursor, ImapAccount, ScopeHandler, batch, decode_cursor,
    encode_cursor, encode_object_id, folder_from_scope, folder_scope, membership_scope,
    route_scope, terminated_event,
};

pub(crate) fn describe_cursor(account: &ImapAccount, cursor: &ChangeCursor) -> CursorDescriptor {
    // A typed scope owned by a sub-account is described by that
    // sub-account; only IMAP folder cursors fall through to the
    // strategy/cost classification below.
    if let Ok(ScopeHandler::Delegate(sub)) = route_scope(
        account,
        &cursor.scope,
        bifrost_types::AccountOperation::SyncChanges,
    ) {
        return sub.describe_cursor(cursor);
    }
    let decoded = decode_cursor(cursor);
    let folder =
        folder_from_scope(&cursor.scope, bifrost_types::AccountOperation::SyncChanges).ok();
    let freshness = folder
        .as_ref()
        .and_then(|folder| account.folders.get(folder))
        .and_then(|entry| entry.last_seen());

    match decoded {
        Ok(FolderCursor::QResync { .. }) if account.qresync_enabled() => CursorDescriptor {
            cost_class: CostClass::Cheap,
            strategy: SyncStrategy::QResync,
            freshness,
        },
        Ok(FolderCursor::QResync { .. }) => CursorDescriptor {
            cost_class: CostClass::Medium,
            strategy: SyncStrategy::Condstore,
            freshness,
        },
        Ok(FolderCursor::Condstore { .. }) => CursorDescriptor {
            cost_class: CostClass::Medium,
            strategy: SyncStrategy::Condstore,
            freshness,
        },
        Ok(FolderCursor::Basic { .. }) => CursorDescriptor {
            cost_class: CostClass::Expensive,
            strategy: SyncStrategy::Basic,
            freshness,
        },
        Err(_) => CursorDescriptor {
            cost_class: CostClass::Expensive,
            strategy: SyncStrategy::None,
            freshness: None,
        },
    }
}

pub(crate) fn changes_stream(
    account: ImapAccount,
    cursor: ChangeCursor,
) -> bifrost_types::AccountStream<SyncEvent<Change>> {
    // Route first: a typed scope owned by a sub-account delegates the
    // whole changes stream.
    match route_scope(
        &account,
        &cursor.scope,
        bifrost_types::AccountOperation::SyncChanges,
    ) {
        Ok(ScopeHandler::Delegate(sub)) => return sub.changes_stream(cursor),
        Ok(ScopeHandler::Folder(_)) => {}
        Err(error) => {
            return Box::pin(futures::stream::iter([SyncEvent::Terminated(error)]));
        }
    }
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    let scope_for_ctx = cursor.scope.clone();
    let task = tokio::spawn(async move {
        match run_changes(account, cursor, tx.clone()).await {
            Ok(()) | Err(ChangeError::ChannelDropped) => {}
            Err(ChangeError::Account(err)) => {
                let _ = tx.send(terminated_event(err)).await;
            }
            Err(ChangeError::Imap(err)) => {
                let _ = tx
                    .send(terminated_event((
                        err,
                        super::error::ImapErrorContext::operation(
                            bifrost_types::AccountOperation::SyncChanges,
                        )
                        .with_cursor_scope(scope_for_ctx),
                    )))
                    .await;
            }
            Err(ChangeError::UidValidityChanged {
                folder,
                expected,
                actual,
            }) => {
                let _ = tx
                    .send(terminated_event(super::error::uidvalidity_changed(
                        &folder, expected, actual,
                    )))
                    .await;
            }
            Err(ChangeError::ModSeqReset {
                folder,
                previous,
                current,
            }) => {
                let _ = tx
                    .send(terminated_event(super::error::modseq_reset(
                        &folder, previous, current,
                    )))
                    .await;
            }
        }
    });
    // Abort-on-drop, not fire-and-forget: the engine cuts a pending
    // changes poll on every PAUSE and then calls back in on the same
    // account, so a task that outlived its receiver would keep issuing
    // IMAP commands beside its successor while sitting on a pool checkout
    // it only releases at its next failed send. See
    // `guarded_receiver_stream` for why aborting mid-command is safe.
    super::guarded_receiver_stream(rx, task)
}

#[derive(Debug)]
enum ChangeError {
    Account(AccountError),
    Imap(crate::Error),
    UidValidityChanged {
        folder: MailboxName,
        expected: u32,
        actual: u32,
    },
    ModSeqReset {
        folder: MailboxName,
        previous: u64,
        current: Option<u64>,
    },
    /// The consumer dropped the output receiver. This is not an error
    /// to escalate: per IMAP plan, dropped output channels stop the
    /// streaming task silently and return without emitting a fatal.
    ChannelDropped,
}

impl From<crate::Error> for ChangeError {
    fn from(value: crate::Error) -> Self {
        Self::Imap(value)
    }
}

impl From<AccountError> for ChangeError {
    fn from(value: AccountError) -> Self {
        Self::Account(value)
    }
}

/// Map a SELECT error for a shared/other-user folder to a scoped
/// `ScopeRevoked` (quarantine just this folder) when it is a permission
/// denial; otherwise return the raw error for the normal mapping. A
/// personal folder (`shared_owner == None`) always returns the raw error.
fn select_error(account: &ImapAccount, folder: &MailboxName, err: crate::Error) -> ChangeError {
    let shared_owner = account
        .folders
        .get(folder)
        .and_then(|entry| entry.shared_owner.clone());
    if shared_owner.is_some() {
        return ChangeError::Account(super::error::shared_folder_error(
            err,
            folder,
            shared_owner.as_ref(),
            super::error::ImapErrorContext::operation(bifrost_types::AccountOperation::SyncChanges)
                .with_folder_scope(folder),
        ));
    }
    ChangeError::Imap(err)
}

async fn run_changes(
    account: ImapAccount,
    change_cursor: ChangeCursor,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let folder = folder_from_scope(
        &change_cursor.scope,
        bifrost_types::AccountOperation::SyncChanges,
    )?;
    let cursor = decode_cursor(&change_cursor)?;
    if matches!(
        cursor,
        FolderCursor::QResync {
            known_uids_complete: false,
            ..
        }
    ) {
        return Err(super::error::incomplete_uid_baseline(&folder).into());
    }
    match cursor {
        FolderCursor::QResync { .. } if account.qresync_enabled() => {
            run_qresync(account, folder, cursor, tx).await
        }
        FolderCursor::QResync {
            uidvalidity,
            modseq,
            known_uids,
            known_uids_complete,
        } => {
            // The NON-consuming read, deliberately. This used to take the
            // account-level one-shot first and fall back to this, which could
            // not change a single byte of what this arm emits - both return the
            // same `Option<String>` - while spending the one-shot so
            // `run_inventory`, the only place it does real work, found it
            // already gone. Since the stream is channel-backed by a spawned
            // task that outlives a dropped receiver, and the engine now drops a
            // pending changes stream on every PAUSE rather than only at
            // teardown, that take could burn the account-level warning without
            // anyone ever receiving it.
            let reason = account.qresync_negotiation_reason().unwrap_or_else(|| {
                "QRESYNC is disabled for this account session; continuing with CONDSTORE"
                    .to_string()
            });
            send_strategy_downgrade(&tx, SyncStrategy::QResync, SyncStrategy::Condstore, &reason)
                .await?;
            run_condstore_with_baseline(
                account,
                folder,
                FolderCursor::Condstore {
                    uidvalidity,
                    modseq,
                    known_uids,
                },
                known_uids_complete,
                tx,
            )
            .await
        }
        FolderCursor::Condstore { .. } => run_condstore(account, folder, cursor, tx).await,
        FolderCursor::Basic { .. } => run_basic(account, folder, cursor, tx).await,
    }
}

async fn run_qresync(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let FolderCursor::QResync {
        uidvalidity: expected_uidvalidity,
        modseq,
        known_uids,
        known_uids_complete,
    } = cursor.clone()
    else {
        return Ok(());
    };
    let mut conn = account.checkout_for_folder(&folder).await?;
    let mut previous_bytes_in = conn.connection().bytes_in();
    let selected = match account
        .select_folder(&mut conn, &folder, Some(&cursor), true)
        .await
    {
        Ok(selected) => selected,
        Err(err) if should_disable_qresync(&err) => {
            if should_discard_conn_after_qresync_error(&err) {
                conn.discard();
            }
            account.disable_qresync_for_session();
            send_strategy_downgrade(
                &tx,
                SyncStrategy::QResync,
                SyncStrategy::Condstore,
                "QRESYNC SELECT failed during response parsing; continuing with CONDSTORE",
            )
            .await?;
            // Release the checkout (and its pool permit) before the retry:
            // run_condstore_with_baseline re-enters checkout_for_folder, and
            // holding the permit across that call deadlocks at data_cap == 1
            // and needlessly dials a second connection above it.
            drop(conn);
            return run_condstore_with_baseline(
                account,
                folder,
                FolderCursor::Condstore {
                    uidvalidity: expected_uidvalidity,
                    modseq,
                    known_uids,
                },
                known_uids_complete,
                tx,
            )
            .await;
        }
        Err(err) => return Err(select_error(&account, &folder, err)),
    };
    let uidvalidity = selected_uidvalidity(&selected.mailbox)?;
    validate_uidvalidity(&folder, expected_uidvalidity, uidvalidity)?;
    debug_assert!(known_uids_complete);
    if selected.mailbox.no_mod_seq || selected.mailbox.highest_mod_seq.is_none() {
        send_strategy_downgrade(
            &tx,
            SyncStrategy::QResync,
            SyncStrategy::Basic,
            "selected mailbox has no persistent mod-sequences",
        )
        .await?;
        return run_basic_from_selected(
            account,
            folder,
            known_uids,
            selected.mailbox,
            conn,
            tx,
            &mut previous_bytes_in,
        )
        .await;
    }
    validate_modseq_not_reset(&folder, modseq, selected.mailbox.highest_mod_seq)?;
    // The SELECT did not use QRESYNC: the account policy was withdrawn
    // between the gate above and building the SELECT, or this member does
    // not have QRESYNC enabled (its dial-time ENABLE was refused or not
    // confirmed). The checkout is validated and selected either way, so the
    // run continues as CONDSTORE on it. The policy is NOT withdrawn for a
    // member's shortfall: the server can do QRESYNC on other members.
    let reason = match selected.qresync {
        crate::types::QresyncOutcome::Used => None,
        crate::types::QresyncOutcome::NotRequested => {
            Some("QRESYNC is disabled for this account session; continuing with CONDSTORE")
        }
        _ => Some("this connection does not have QRESYNC enabled; continuing with CONDSTORE"),
    };
    if let Some(reason) = reason {
        send_strategy_downgrade(&tx, SyncStrategy::QResync, SyncStrategy::Condstore, reason)
            .await?;
        return run_condstore_from_selected(
            account,
            folder,
            SelectedBaseline {
                uidvalidity,
                modseq,
                known_uids,
                selected: selected.mailbox,
            },
            conn,
            tx,
            &mut previous_bytes_in,
        )
        .await;
    }
    let mut live_uids = known_uids.clone();
    let mut fetch_change_seen = BTreeSet::new();
    let mut removed_seen = BTreeSet::new();
    let mut changes = Vec::with_capacity(BATCH_ITEMS);
    for range in selected.mailbox.vanished.clone() {
        account
            .folders
            .clear_modseq_ranges(&folder, uidvalidity, &[range]);
        let removed = live_uids.remove_range(range);
        for uid in removed.iter() {
            if removed_seen.insert(uid) {
                changes.push(removed_change(&folder, uidvalidity, uid));
            }
            if changes.len() >= BATCH_ITEMS {
                flush_page(&tx, &mut changes, conn.connection(), &mut previous_bytes_in).await?;
            }
        }
    }
    for fetch in selected.mailbox.changed_messages.clone() {
        if let (Some(uid), Some(modseq)) = (fetch.uid, fetch.mod_seq) {
            account
                .folders
                .record_modseq(&folder, uidvalidity, uid, modseq)?;
        }
        record_fetch_change(
            &folder,
            uidvalidity,
            fetch.uid,
            &mut live_uids,
            &mut fetch_change_seen,
            &mut removed_seen,
            &mut changes,
        );
        if changes.len() >= BATCH_ITEMS {
            flush_page(&tx, &mut changes, conn.connection(), &mut previous_bytes_in).await?;
        }
    }
    // With known UIDs supplied, SELECT need only report changes to that
    // set (RFC 7162 Section 3.2.5.1). Arrivals can appear only in EXISTS.
    // Reconcile membership when SELECT has not accounted for that count;
    // do not repeat the all-UID CHANGEDSINCE/VANISHED flags fetch.
    let live_set = if usize::try_from(selected.mailbox.exists).ok() != Some(live_uids.uid_count()) {
        let current = CompactUidSet::from_uids(search_all(&account, conn.connection()).await?);
        let diff = live_uids.diff(&current);
        for uid in diff.added {
            changes.push(added_change(&folder, uidvalidity, uid));
            flush_page(&tx, &mut changes, conn.connection(), &mut previous_bytes_in).await?;
        }
        for uid in diff.removed {
            account.folders.clear_modseqs(&folder, uidvalidity, &[uid]);
            changes.push(removed_change(&folder, uidvalidity, uid));
            flush_page(&tx, &mut changes, conn.connection(), &mut previous_bytes_in).await?;
        }
        current
    } else {
        live_uids
    };
    // Keep SELECT's MODSEQ: changes concurrent with SEARCH must still be
    // eligible for the next poll. A failed SEARCH never advances the cursor.
    warn_if_uid_count_mismatch(&tx, &folder, selected.mailbox.exists, live_set.uid_count()).await?;
    let next = account.cursor_from_select(&selected.mailbox, Some(live_set))?;
    let bytes_in = super::measured_bytes_in(conn.connection(), &mut previous_bytes_in);
    finish_changes(account, folder, next, changes, tx, bytes_in).await
}

async fn run_condstore(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    run_condstore_with_baseline(account, folder, cursor, true, tx).await
}

async fn run_condstore_with_baseline(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    known_uids_complete: bool,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let FolderCursor::Condstore {
        modseq, known_uids, ..
    } = cursor.clone()
    else {
        return Ok(());
    };
    let mut conn = account.checkout_for_folder(&folder).await?;
    let mut previous_bytes_in = conn.connection().bytes_in();
    let selected = account
        .select_folder(&mut conn, &folder, Some(&cursor), true)
        .await
        .map_err(|err| select_error(&account, &folder, err))?;
    let uidvalidity = selected_uidvalidity(&selected.mailbox)?;
    validate_uidvalidity(&folder, cursor.uidvalidity(), uidvalidity)?;
    if !known_uids_complete {
        return Err(super::error::incomplete_uid_baseline(&folder).into());
    }
    if selected.mailbox.no_mod_seq || selected.mailbox.highest_mod_seq.is_none() {
        send_strategy_downgrade(
            &tx,
            SyncStrategy::Condstore,
            SyncStrategy::Basic,
            "selected mailbox has no persistent mod-sequences",
        )
        .await?;
        return run_basic_from_selected(
            account,
            folder,
            known_uids,
            selected.mailbox,
            conn,
            tx,
            &mut previous_bytes_in,
        )
        .await;
    }
    validate_modseq_not_reset(&folder, modseq, selected.mailbox.highest_mod_seq)?;
    run_condstore_from_selected(
        account,
        folder,
        SelectedBaseline {
            uidvalidity,
            modseq,
            known_uids,
            selected: selected.mailbox,
        },
        conn,
        tx,
        &mut previous_bytes_in,
    )
    .await
}

/// A SELECT already made and validated for an incremental run: UIDVALIDITY
/// matched the cursor, the mailbox has persistent mod-sequences, and its
/// HIGHESTMODSEQ did not go backwards.
struct SelectedBaseline {
    uidvalidity: u32,
    modseq: u64,
    known_uids: CompactUidSet,
    selected: crate::types::SelectedMailbox,
}

/// The CONDSTORE diff on a checkout that has already selected and validated
/// the folder: `CHANGEDSINCE` for updates, a UID SEARCH ALL snapshot diffed
/// against the complete baseline for arrivals and expunges.
///
/// Takes the checkout instead of acquiring one, so a QRESYNC run whose own
/// SELECT could not use QRESYNC continues on the member it already holds
/// rather than re-entering the pool (which deadlocks at `data_cap == 1` and
/// may land on a different member).
///
/// The next cursor is minted by `cursor_from_select`, the same policy-based
/// rule inventory uses: QResync while the account's policy allows it, else
/// Condstore. A CONDSTORE run therefore never demotes a folder for good; a
/// member that could not do QRESYNC this time does not decide the strategy of
/// the next run. The MODSEQ is the SELECT's HIGHESTMODSEQ, never a later
/// value, so no flag change is skipped.
async fn run_condstore_from_selected(
    account: ImapAccount,
    folder: MailboxName,
    baseline: SelectedBaseline,
    conn: super::PooledConn,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
    previous_bytes_in: &mut u64,
) -> Result<(), ChangeError> {
    let SelectedBaseline {
        uidvalidity,
        modseq,
        known_uids,
        selected,
    } = baseline;
    let all_uids = UidSet::all();
    let (mut fetch_rx, fetch_fut) = conn.connection().uid_fetch_changed_since_stream(
        all_uids.as_sequence_set(),
        &[FetchAttr::Uid, FetchAttr::Flags, FetchAttr::ModSeq],
        modseq,
        account.command_timeout(),
    )?;
    tokio::pin!(fetch_fut);
    let mut changes = Vec::with_capacity(BATCH_ITEMS);
    let mut fetch_result = None;
    loop {
        tokio::select! {
            result = &mut fetch_fut, if fetch_result.is_none() => {
                fetch_result = Some(result);
            }
            item = fetch_rx.recv() => {
                let Some(item) = item else {
                    match fetch_result.take() {
                        Some(result) => result?,
                        None => (&mut fetch_fut).await?,
                    }
                    break;
                };
                let fetch = item?;
                if let Some(uid) = fetch.uid {
                    if let Some(modseq) = fetch.mod_seq {
                        account
                            .folders
                            .record_modseq(&folder, uidvalidity, uid, modseq)?;
                    }
                    // CHANGEDSINCE also returns arrivals. Only a UID in the
                    // complete prior baseline is an update; the live snapshot
                    // diff below owns additions.
                    if known_uids.contains(uid) {
                        changes.push(updated_change(&folder, uidvalidity, uid));
                        flush_page(&tx, &mut changes, conn.connection(), previous_bytes_in).await?;
                    }
                }
            }
        }
    }
    let live_set = CompactUidSet::from_uids(search_all(&account, conn.connection()).await?);
    warn_if_uid_count_mismatch(&tx, &folder, selected.exists, live_set.uid_count()).await?;
    let diff = known_uids.diff(&live_set);
    for uid in diff.added {
        changes.push(added_change(&folder, uidvalidity, uid));
        flush_page(&tx, &mut changes, conn.connection(), previous_bytes_in).await?;
    }
    for uid in diff.removed {
        account.folders.clear_modseqs(&folder, uidvalidity, &[uid]);
        changes.push(removed_change(&folder, uidvalidity, uid));
        flush_page(&tx, &mut changes, conn.connection(), previous_bytes_in).await?;
    }
    let next = account.cursor_from_select(&selected, Some(live_set))?;
    let bytes_in = super::measured_bytes_in(conn.connection(), previous_bytes_in);
    finish_changes(account, folder, next, changes, tx, bytes_in).await
}

async fn run_basic(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
) -> Result<(), ChangeError> {
    let known_uids = cursor.known_uids().clone();
    let cursor_uidnext = match cursor {
        FolderCursor::Basic { uidnext, .. } => uidnext,
        _ => 0,
    };
    let mut conn = account.checkout_for_folder(&folder).await?;
    let mut previous_bytes_in = conn.connection().bytes_in();
    let selected = account
        .select_folder(&mut conn, &folder, Some(&cursor), true)
        .await
        .map_err(|err| select_error(&account, &folder, err))?;
    let uidvalidity = selected_uidvalidity(&selected.mailbox)?;
    validate_uidvalidity(&folder, cursor.uidvalidity(), uidvalidity)?;
    let uidnext_unchanged = cursor_uidnext != 0
        && selected.mailbox.uid_next == Some(cursor_uidnext)
        && usize::try_from(selected.mailbox.exists).ok() == Some(known_uids.uid_count());
    let live_set = if uidnext_unchanged {
        known_uids.clone()
    } else {
        CompactUidSet::from_uids(search_all(&account, conn.connection()).await?)
    };
    warn_if_uid_count_mismatch(&tx, &folder, selected.mailbox.exists, live_set.uid_count()).await?;
    let diff = known_uids.diff(&live_set);
    let mut changes = Vec::with_capacity(BATCH_ITEMS);
    for fetch in &selected.mailbox.changed_messages {
        if let Some(uid) = fetch.uid {
            if let Some(modseq) = fetch.mod_seq {
                account
                    .folders
                    .record_modseq(&folder, uidvalidity, uid, modseq)?;
            }
            if let Some(change) =
                basic_updated_change(&folder, uidvalidity, uid, &known_uids, &live_set)?
            {
                changes.push(change);
                flush_page(&tx, &mut changes, conn.connection(), &mut previous_bytes_in).await?;
            }
        }
    }
    for uid in diff.added {
        changes.push(added_change(&folder, uidvalidity, uid));
        flush_page(&tx, &mut changes, conn.connection(), &mut previous_bytes_in).await?;
    }
    for uid in diff.removed {
        account.folders.clear_modseqs(&folder, uidvalidity, &[uid]);
        changes.push(removed_change(&folder, uidvalidity, uid));
        flush_page(&tx, &mut changes, conn.connection(), &mut previous_bytes_in).await?;
    }
    let next = FolderCursor::Basic {
        uidvalidity,
        uidnext: selected.mailbox.uid_next.unwrap_or_default(),
        known_uids: live_set,
    };
    let bytes_in = super::measured_bytes_in(conn.connection(), &mut previous_bytes_in);
    finish_changes(account, folder, next, changes, tx, bytes_in).await
}

async fn run_basic_from_selected(
    account: ImapAccount,
    folder: MailboxName,
    known_uids: CompactUidSet,
    selected: crate::types::SelectedMailbox,
    conn: super::PooledConn,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
    previous_bytes_in: &mut u64,
) -> Result<(), ChangeError> {
    let uidvalidity = selected_uidvalidity(&selected)?;
    let live = search_all(&account, conn.connection()).await?;
    let live_set = CompactUidSet::from_uids(live);
    warn_if_uid_count_mismatch(&tx, &folder, selected.exists, live_set.uid_count()).await?;
    let diff = known_uids.diff(&live_set);
    let mut changes = Vec::with_capacity(BATCH_ITEMS);
    for fetch in &selected.changed_messages {
        if let Some(uid) = fetch.uid {
            if let Some(modseq) = fetch.mod_seq {
                account
                    .folders
                    .record_modseq(&folder, uidvalidity, uid, modseq)?;
            }
            if let Some(change) =
                basic_updated_change(&folder, uidvalidity, uid, &known_uids, &live_set)?
            {
                changes.push(change);
                flush_page(&tx, &mut changes, conn.connection(), previous_bytes_in).await?;
            }
        }
    }
    for uid in diff.added {
        changes.push(added_change(&folder, uidvalidity, uid));
        flush_page(&tx, &mut changes, conn.connection(), previous_bytes_in).await?;
    }
    for uid in diff.removed {
        account.folders.clear_modseqs(&folder, uidvalidity, &[uid]);
        changes.push(removed_change(&folder, uidvalidity, uid));
        flush_page(&tx, &mut changes, conn.connection(), previous_bytes_in).await?;
    }
    let next = FolderCursor::Basic {
        uidvalidity,
        uidnext: selected.uid_next.unwrap_or_default(),
        known_uids: live_set,
    };
    let bytes_in = super::measured_bytes_in(conn.connection(), previous_bytes_in);
    finish_changes(account, folder, next, changes, tx, bytes_in).await
}

async fn search_all(
    account: &ImapAccount,
    conn: &crate::ImapConnection,
) -> Result<Vec<u32>, crate::Error> {
    Ok(conn.uid_search("ALL", account.command_timeout()).await?.ids)
}

/// A run whose residual is empty ends on `[Page, Page]` with no `Final` batch
/// at all - the `changes.is_empty()` guard below skips it. That is accepted,
/// not a boundary bug: `SyncEvent::Done(checkpoint)` is the terminator of this
/// stream, and nothing downstream may key on seeing a `Final`. Emitting an
/// empty final batch purely to decorate the boundary would publish a page that
/// carries nothing.
async fn finish_changes(
    account: ImapAccount,
    folder: MailboxName,
    cursor: FolderCursor,
    changes: Vec<Change>,
    tx: tokio::sync::mpsc::Sender<SyncEvent<Change>>,
    bytes_in: u64,
) -> Result<(), ChangeError> {
    let checkpoint = Some(Checkpoint::Change(encode_cursor(
        folder_scope(&folder),
        &cursor,
    )));
    if !changes.is_empty() {
        tx.send(batch(
            changes,
            PageBoundary::Final,
            checkpoint.clone(),
            bytes_in,
        ))
        .await
        .map_err(|_| ChangeError::ChannelDropped)?;
    }
    tx.send(SyncEvent::Done(checkpoint))
        .await
        .map_err(|_| ChangeError::ChannelDropped)?;
    // The in-memory cursor is a hint for later SELECTs. Do not advance it if
    // this changes stream could not enqueue its terminal checkpoint; the
    // engine's durable cursor still belongs to the consumer.
    account.folders.set_cursor(&folder, cursor);
    Ok(())
}

/// The at-most-`BATCH_ITEMS`-per-page guarantee holds by convention, not by
/// construction. QRESYNC, CONDSTORE, and both Basic paths call this helper
/// after adding changes; a new emission loop must keep doing so because the
/// event types do not enforce a page-size limit.
///
/// Consolidating the three behind a `Strategy` trait was proposed and rejected:
/// QRESYNC handles its SELECT-parse downgrade and discards the suspect
/// connection, CONDSTORE has a fallible bounded FETCH stream but no VANISHED
/// lane, and Basic has no change-source stream at all. A shared runner would
/// end up owning strategy-specific wire policy. The consolidation was taken
/// at this narrower seam instead, which is the part that is genuinely common.
async fn flush_page(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<Change>>,
    changes: &mut Vec<Change>,
    connection: &crate::ImapConnection,
    previous_bytes_in: &mut u64,
) -> Result<(), ChangeError> {
    if changes.len() < BATCH_ITEMS {
        return Ok(());
    }
    let bytes_in = super::measured_bytes_in(connection, previous_bytes_in);
    tx.send(batch(
        std::mem::take(changes),
        PageBoundary::Page,
        None,
        bytes_in,
    ))
    .await
    .map_err(|_| ChangeError::ChannelDropped)
}

/// A non-conformant server may name the same UID in both VANISHED and FETCH.
/// While both are still buffered this retracts the removal into a single
/// update, but once the `Removed` has ESCAPED in a flushed page it cannot be
/// retracted, and the UID re-`Add`s instead.
///
/// That remove-then-re-add pair is deliberate and is not suppressed. It is a
/// coherent sequence for the consumer - the object left and came back - and the
/// alternative would be rewriting history a page after it was published, which
/// the stream has no mechanism for and no right to do. Accepted, not open.
fn record_fetch_change(
    folder: &MailboxName,
    uidvalidity: u32,
    uid: Option<u32>,
    live_uids: &mut CompactUidSet,
    fetch_change_seen: &mut BTreeSet<u32>,
    removed_seen: &mut BTreeSet<u32>,
    changes: &mut Vec<Change>,
) {
    let Some(uid) = uid else {
        return;
    };
    if !fetch_change_seen.insert(uid) {
        return;
    }
    if removed_seen.remove(&uid) {
        let removed_was_buffered =
            remove_buffered_removed_change(folder, uidvalidity, uid, changes);
        live_uids.insert(uid);
        if removed_was_buffered {
            changes.push(updated_change(folder, uidvalidity, uid));
        } else {
            changes.push(added_change(folder, uidvalidity, uid));
        }
        return;
    }
    if live_uids.insert(uid) {
        changes.push(added_change(folder, uidvalidity, uid));
    } else {
        changes.push(updated_change(folder, uidvalidity, uid));
    }
}

fn remove_buffered_removed_change(
    folder: &MailboxName,
    uidvalidity: u32,
    uid: u32,
    changes: &mut Vec<Change>,
) -> bool {
    let id = encode_object_id(folder, uidvalidity, uid);
    let Some(index) = changes.iter().rposition(|change| {
        matches!(
            change,
            Change::ScopeChange(ScopeChange {
                id: change_id,
                kind: ScopeChangeKind::Removed,
                ..
            }) if change_id == &id
        )
    }) else {
        return false;
    };
    changes.remove(index);
    true
}

fn added_change(folder: &MailboxName, uidvalidity: u32, uid: u32) -> Change {
    Change::ScopeChange(ScopeChange {
        id: encode_object_id(folder, uidvalidity, uid),
        membership: membership_scope(folder),
        kind: ScopeChangeKind::Added,
    })
}

fn removed_change(folder: &MailboxName, uidvalidity: u32, uid: u32) -> Change {
    Change::ScopeChange(ScopeChange {
        id: encode_object_id(folder, uidvalidity, uid),
        membership: membership_scope(folder),
        kind: ScopeChangeKind::Removed,
    })
}

fn updated_change(folder: &MailboxName, uidvalidity: u32, uid: u32) -> Change {
    Change::ObjectChange(ObjectChange {
        id: encode_object_id(folder, uidvalidity, uid),
        kind: ObjectChangeKind::Updated,
    })
}

/// Classify a server-authored `changed_messages` UID on a Basic run.
///
/// A UID in the prior baseline is a genuine `Updated`. A UID that is absent
/// from the baseline but present in the live snapshot is an arrival: RFC 7162
/// Section 3.2.5 has the server return changed-message FETCH data for every
/// message above the client's MODSEQ, new ones included, so this is the common
/// case on a mailbox that just downgraded out of QRESYNC/CONDSTORE. The
/// baseline diff already reports it as `Added`, exactly once, and announcing
/// `Updated` for an object the consumer has no membership record of is the
/// hazard the CONDSTORE path guards. Emitting nothing here is therefore
/// lossless. Only a UID in neither set is contradictory - the server described
/// a message it does not list - and that invalidates the cursor.
///
/// On Basic servers, SELECT's changed-message FETCH responses are only
/// opportunistic hints. Without CONDSTORE there is no reliable way to detect
/// flag changes for every existing UID without fetching the full mailbox.
fn basic_updated_change(
    folder: &MailboxName,
    uidvalidity: u32,
    uid: u32,
    known_uids: &CompactUidSet,
    live_uids: &CompactUidSet,
) -> Result<Option<Change>, ChangeError> {
    if known_uids.contains(uid) {
        return Ok(Some(updated_change(folder, uidvalidity, uid)));
    }
    if live_uids.contains(uid) {
        return Ok(None);
    }
    Err(super::error::incomplete_uid_baseline(folder).into())
}

fn validate_uidvalidity(
    folder: &MailboxName,
    expected: u32,
    actual: u32,
) -> Result<(), ChangeError> {
    if expected == actual {
        return Ok(());
    }
    Err(ChangeError::UidValidityChanged {
        folder: folder.clone(),
        expected,
        actual,
    })
}

fn selected_uidvalidity(selected: &SelectedMailbox) -> Result<u32, ChangeError> {
    Ok(super::selected_uidvalidity(selected)?)
}

fn validate_modseq_not_reset(
    folder: &MailboxName,
    previous: u64,
    current: Option<u64>,
) -> Result<(), ChangeError> {
    if current.is_some_and(|current| current >= previous) {
        return Ok(());
    }
    Err(ChangeError::ModSeqReset {
        folder: folder.clone(),
        previous,
        current,
    })
}

async fn warn_if_uid_count_mismatch<T>(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<T>>,
    folder: &MailboxName,
    expected: u32,
    actual: usize,
) -> Result<(), ChangeError> {
    if usize::try_from(expected).ok() == Some(actual) {
        return Ok(());
    }
    send_warning(
        tx,
        WarningKind::Other,
        &format!(
            "IMAP UID count differs from SELECT EXISTS for {}: SELECT EXISTS {}, UID set {}",
            folder.as_str(),
            expected,
            actual,
        ),
    )
    .await
}

async fn send_warning<T>(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<T>>,
    kind: WarningKind,
    message: &str,
) -> Result<(), ChangeError> {
    tx.send(SyncEvent::Warning(
        Warning::support_only(kind, message)
            .with_protocol_detail(bifrost_types::DiagnosticText::support_only("imap")),
    ))
    .await
    .map_err(|_| ChangeError::ChannelDropped)?;
    Ok(())
}

async fn send_strategy_downgrade<T>(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<T>>,
    from: SyncStrategy,
    to: SyncStrategy,
    reason: &str,
) -> Result<(), ChangeError> {
    tx.send(SyncEvent::Warning(
        Warning::support_only(WarningKind::StrategyDowngraded, reason).with_protocol_detail(
            bifrost_types::DiagnosticText::support_only(format!("{from:?}->{to:?}")),
        ),
    ))
    .await
    .map_err(|_| ChangeError::ChannelDropped)?;
    Ok(())
}

fn should_disable_qresync(err: &crate::Error) -> bool {
    match err {
        crate::Error::Parse(_) => true,
        // "ENABLE" covers select_for_sync's enable() leg on a server that
        // advertises QRESYNC without ENABLE (possible pre-rev2): in this
        // QRESYNC-only context a missing ENABLE means QRESYNC cannot be
        // negotiated, which is a downgrade, not a terminal error.
        crate::Error::MissingCapability(cap) => {
            mentions_qresync_capability(cap) || cap.eq_ignore_ascii_case("ENABLE")
        }
        _ => false,
    }
}

fn should_discard_conn_after_qresync_error(err: &crate::Error) -> bool {
    matches!(err, crate::Error::Parse(_))
}

fn mentions_qresync_capability(value: &str) -> bool {
    value
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '=')))
        .any(|token| token.eq_ignore_ascii_case("QRESYNC"))
}

#[cfg(test)]
mod tests {
    use bifrost_types::{CursorScope, RecoveryClass};

    use super::*;

    #[tokio::test]
    async fn a_failed_done_send_does_not_advance_the_cached_cursor() {
        use bifrost_types::Account;

        let (conn, server) = crate::connection::test_support::driver_pair(
            &crate::connection::test_support::preauth_greeting("IMAP4rev1"),
        )
        .await;
        drop(server);
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let registry = super::super::folder_registry::FolderRegistry::from_list(vec![
            crate::types::MailboxInfo {
                name: folder.clone(),
                ..Default::default()
            },
        ]);
        let mut parts = super::super::test_support::scripted_account_parts(conn, 1);
        parts.folders = std::sync::Arc::new(registry);
        let account = super::super::ImapAccount::new(parts);
        let cursor = FolderCursor::Basic {
            uidvalidity: 7,
            uidnext: 2,
            known_uids: CompactUidSet::from_uids([1]),
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(finish_changes(
            account.clone(),
            folder.clone(),
            cursor,
            vec![added_change(&folder, 7, 1)],
            tx,
            0,
        ));

        let final_page = rx.recv().await.expect("final page queued");
        assert!(matches!(
            final_page,
            SyncEvent::Batch(ref batch) if batch.page_boundary == PageBoundary::Final
        ));
        rx.close();
        assert!(matches!(
            task.await.expect("finish task"),
            Err(ChangeError::ChannelDropped)
        ));
        assert!(
            account
                .folders
                .get(&folder)
                .expect("registered folder")
                .cursor()
                .is_none(),
            "a failed terminal checkpoint send must leave the SELECT hint unchanged"
        );
        account.close().await.expect("account cleanup");
    }

    // Construct removal state for the FETCH ordering tests through the same
    // range subtraction used by the stream.
    fn record_removed_change(
        folder: &MailboxName,
        uidvalidity: u32,
        uid: u32,
        live_uids: &mut CompactUidSet,
        removed_seen: &mut BTreeSet<u32>,
        changes: &mut Vec<Change>,
    ) {
        let removed = live_uids.remove_range(crate::types::UidRange::single(uid));
        if !removed.is_empty() && removed_seen.insert(uid) {
            changes.push(removed_change(folder, uidvalidity, uid));
        }
    }

    #[test]
    fn uidvalidity_mismatch_requests_scope_restart() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let err = validate_uidvalidity(&folder, 7, 8).expect_err("mismatch should fail");
        let ChangeError::UidValidityChanged {
            folder,
            expected,
            actual,
        } = err
        else {
            panic!("expected uidvalidity change");
        };
        let account_err = crate::account::error::uidvalidity_changed(&folder, expected, actual);
        assert!(matches!(
            account_err.recovery(),
            RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(
                CursorScope::Folder(_)
            ))
        ));
    }

    #[test]
    fn modseq_reset_requests_scope_restart() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let err = validate_modseq_not_reset(&folder, 10, Some(9)).expect_err("reset should fail");
        let ChangeError::ModSeqReset {
            folder,
            previous,
            current,
        } = err
        else {
            panic!("expected modseq reset");
        };
        let account_err = crate::account::error::modseq_reset(&folder, previous, current);
        assert!(matches!(
            account_err.recovery(),
            RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(
                CursorScope::Folder(_)
            ))
        ));
    }

    #[tokio::test]
    async fn uid_count_mismatch_emits_warning() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<SyncEvent<Change>>(1);

        warn_if_uid_count_mismatch(&tx, &folder, 2, 1)
            .await
            .expect("warning should send");
        let event = rx.recv().await.expect("warning event");
        assert!(matches!(
            event,
            SyncEvent::Warning(Warning {
                kind: WarningKind::Other,
                ..
            })
        ));
    }

    #[test]
    fn qresync_record_helpers_deduplicate_select_and_fetch_data() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let mut live_uids = CompactUidSet::from_uids([1, 2]);
        let mut fetch_seen = BTreeSet::new();
        let mut removed_seen = BTreeSet::new();
        let mut changes = Vec::new();

        record_fetch_change(
            &folder,
            99,
            Some(3),
            &mut live_uids,
            &mut fetch_seen,
            &mut removed_seen,
            &mut changes,
        );
        record_fetch_change(
            &folder,
            99,
            Some(3),
            &mut live_uids,
            &mut fetch_seen,
            &mut removed_seen,
            &mut changes,
        );
        record_removed_change(
            &folder,
            99,
            2,
            &mut live_uids,
            &mut removed_seen,
            &mut changes,
        );
        record_removed_change(
            &folder,
            99,
            2,
            &mut live_uids,
            &mut removed_seen,
            &mut changes,
        );
        record_fetch_change(
            &folder,
            99,
            Some(2),
            &mut live_uids,
            &mut fetch_seen,
            &mut removed_seen,
            &mut changes,
        );

        assert_eq!(changes.len(), 2);
        assert!(matches!(
            changes[0],
            Change::ScopeChange(ScopeChange {
                kind: ScopeChangeKind::Added,
                ..
            })
        ));
        assert!(matches!(
            changes[1],
            Change::ObjectChange(ObjectChange {
                kind: ObjectChangeKind::Updated,
                ..
            })
        ));
        assert_eq!(live_uids, CompactUidSet::from_uids([1, 2, 3]));
    }

    // A HIGHESTMODSEQ that merely stayed put is not a reset; only a
    // backwards move (or a mailbox that reports none at all after we had
    // one) invalidates the cursor.
    #[test]
    fn modseq_reset_detection_accepts_equal_and_rejects_absent() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        assert!(validate_modseq_not_reset(&folder, 10, Some(10)).is_ok());
        assert!(validate_modseq_not_reset(&folder, 10, Some(11)).is_ok());
        assert!(validate_modseq_not_reset(&folder, 0, Some(0)).is_ok());
        assert!(validate_modseq_not_reset(&folder, 10, None).is_err());
    }

    #[tokio::test]
    async fn uid_count_match_emits_no_warning() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<SyncEvent<Change>>(1);
        warn_if_uid_count_mismatch(&tx, &folder, 3, 3)
            .await
            .expect("agreement is not a failure");
        drop(tx);
        assert!(
            rx.recv().await.is_none(),
            "EXISTS agreeing with the UID set must stay silent"
        );
    }

    #[tokio::test]
    async fn strategy_downgrade_warning_names_both_strategies() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<SyncEvent<Change>>(1);
        send_strategy_downgrade(
            &tx,
            SyncStrategy::QResync,
            SyncStrategy::Condstore,
            "server said no",
        )
        .await
        .expect("warning should send");
        let event = rx.recv().await.expect("warning event");
        assert!(
            matches!(
                event,
                SyncEvent::Warning(Warning {
                    kind: WarningKind::StrategyDowngraded,
                    ..
                })
            ),
            "a downgrade must be reported as StrategyDowngraded, not a generic warning",
        );
    }

    // The dedup only rewrites a Removed that is still buffered in THIS
    // batch. Once the Removed has been flushed to the consumer, a later
    // FETCH for the same UID is a genuine re-add, not an update.
    #[test]
    fn a_fetch_after_an_already_flushed_removal_reports_added() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let mut live_uids = CompactUidSet::from_uids([1, 2]);
        let mut fetch_seen = BTreeSet::new();
        let mut removed_seen = BTreeSet::new();
        let mut changes = Vec::new();

        record_removed_change(
            &folder,
            99,
            2,
            &mut live_uids,
            &mut removed_seen,
            &mut changes,
        );
        // Simulate the batch flush: the Removed already left the process.
        changes.clear();

        record_fetch_change(
            &folder,
            99,
            Some(2),
            &mut live_uids,
            &mut fetch_seen,
            &mut removed_seen,
            &mut changes,
        );

        assert_eq!(changes.len(), 1);
        assert!(
            matches!(
                changes[0],
                Change::ScopeChange(ScopeChange {
                    kind: ScopeChangeKind::Added,
                    ..
                })
            ),
            "a flushed removal cannot be retracted, so the UID re-Adds",
        );
        assert!(live_uids.contains(2), "the UID is live again");
    }

    // VANISHED and FETCH may name the same UID on a non-conformant
    // server. When both are still buffered, the message must surface
    // exactly once, as an update, never as both expunge and update.
    #[test]
    fn a_buffered_removal_is_retracted_into_an_update() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let mut live_uids = CompactUidSet::from_uids([1, 2]);
        let mut fetch_seen = BTreeSet::new();
        let mut removed_seen = BTreeSet::new();
        let mut changes = Vec::new();

        record_removed_change(
            &folder,
            99,
            2,
            &mut live_uids,
            &mut removed_seen,
            &mut changes,
        );
        record_fetch_change(
            &folder,
            99,
            Some(2),
            &mut live_uids,
            &mut fetch_seen,
            &mut removed_seen,
            &mut changes,
        );

        assert_eq!(changes.len(), 1, "exactly one change for the UID");
        assert!(matches!(
            changes[0],
            Change::ObjectChange(ObjectChange {
                kind: ObjectChangeKind::Updated,
                ..
            })
        ));
    }

    #[test]
    fn a_fetch_without_a_uid_is_ignored() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let mut live_uids = CompactUidSet::default();
        let mut fetch_seen = BTreeSet::new();
        let mut removed_seen = BTreeSet::new();
        let mut changes = Vec::new();
        record_fetch_change(
            &folder,
            99,
            None,
            &mut live_uids,
            &mut fetch_seen,
            &mut removed_seen,
            &mut changes,
        );
        assert!(changes.is_empty());
        assert!(live_uids.is_empty());
    }

    #[test]
    fn basic_server_update_is_classified_against_baseline_and_live_set() {
        let folder = MailboxName::new("INBOX").expect("valid mailbox");
        let known = CompactUidSet::from_uids([1, 2]);
        let live = CompactUidSet::from_uids([1, 2, 3]);

        assert!(
            basic_updated_change(&folder, 7, 2, &known, &live)
                .expect("a baseline UID is a plain update")
                .is_some(),
            "a UID the consumer already knows must surface as Updated"
        );
        assert!(
            basic_updated_change(&folder, 7, 3, &known, &live)
                .expect("an arrival is not a cursor fault")
                .is_none(),
            "an arrival is owned by the baseline diff's Added lane, not Updated"
        );

        let error = basic_updated_change(&folder, 7, 9, &known, &live)
            .expect_err("a UID in neither set cannot be described at all");
        let ChangeError::Account(error) = error else {
            panic!("unknown Basic UID must be a cursor error");
        };
        assert!(matches!(
            error.recovery(),
            RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(_))
        ));
    }

    #[test]
    fn qresync_disable_fallback_uses_parse_errors_and_enable_misses() {
        assert!(should_disable_qresync(&crate::Error::Parse(
            "malformed FETCH".to_string(),
        )));
        assert!(should_disable_qresync(&crate::Error::MissingCapability(
            "QRESYNC (not ENABLEd)".to_string(),
        )));
        // The enable() leg on a server advertising QRESYNC without ENABLE.
        assert!(should_disable_qresync(&crate::Error::MissingCapability(
            "ENABLE".to_string(),
        )));
        assert!(!should_disable_qresync(&crate::Error::Protocol(
            "missing FLAGS".to_string(),
        )));
        assert!(!should_disable_qresync(&crate::Error::MissingCapability(
            "CONDSTORE".to_string(),
        )));
        assert!(!should_disable_qresync(&crate::Error::MissingCapability(
            "QRESYNC_V2".to_string(),
        )));
        assert!(mentions_qresync_capability(
            "SELECT (QRESYNC) requires QRESYNC"
        ));
    }
}
