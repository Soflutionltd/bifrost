use std::collections::HashSet;

use bifrost_types::{
    AccountError, AccountFuture, AccountStream, Checkpoint, CursorEstablishment, CursorScope,
    DiagnosticText, Fingerprint, InventoryEntry, PageBoundary, ServerVersion, SyncEvent,
    SyncStrategy, ThreadId, Warning, WarningKind,
};
use futures::StreamExt as _;

use crate::types::{FetchAttr, FetchResponse, MailboxName, UidSet};

use super::{
    BATCH_ITEMS, CompactUidSet, ImapAccount, ScopeHandler, batch, encode_cursor, encode_object_id,
    folder_from_scope, folder_scope, membership_scope, route_scope, terminated_event,
};

pub(crate) fn establish_initial_cursor(
    account: ImapAccount,
    scope: CursorScope,
) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
    Box::pin(async move {
        match route_scope(
            &account,
            &scope,
            bifrost_types::AccountOperation::EstablishCursor,
        )? {
            ScopeHandler::Folder(_) => Ok(CursorEstablishment::EstablishViaInventory),
            // A composed sub-account owns this typed scope: defer the
            // whole cursor-establishment decision to it.
            ScopeHandler::Delegate(sub) => sub.establish_initial_cursor(scope).await,
        }
    })
}

pub(crate) fn inventory_stream(
    account: ImapAccount,
    scope: CursorScope,
) -> AccountStream<bifrost_types::InventoryEvent> {
    // Route first: a typed scope owned by a sub-account delegates the
    // whole inventory stream rather than checking out an IMAP folder.
    match route_scope(
        &account,
        &scope,
        bifrost_types::AccountOperation::SyncInventory,
    ) {
        // The sub-account already speaks the inventory envelope, so its stream
        // passes straight through. Converting here instead would relabel
        // whatever coverage the delegate reported as COMPLETE.
        Ok(ScopeHandler::Delegate(sub)) => return sub.inventory_stream(scope),
        Ok(ScopeHandler::Folder(_)) => {}
        Err(error) => {
            return Box::pin(futures::stream::iter([
                bifrost_types::InventoryEvent::Terminated(error),
            ]));
        }
    }
    let domain = bifrost_types::CoverageDomain::full(scope.clone());
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    let ticket = QresyncWarningTicket::new(account.clone(), tx.clone());
    let task = tokio::spawn(async move {
        match run_inventory(account, scope.clone(), tx.clone(), &ticket).await {
            Ok(()) | Err(InventoryError::ChannelDropped) => {}
            Err(InventoryError::Imap(err)) => {
                let _ = tx
                    .send(terminated_event((
                        err,
                        super::error::ImapErrorContext::operation(
                            bifrost_types::AccountOperation::SyncInventory,
                        )
                        .with_cursor_scope(scope),
                    )))
                    .await;
            }
            Err(InventoryError::Account(err)) => {
                let _ = tx
                    .send(super::terminated_event::<InventoryEntry, _>(err))
                    .await;
            }
        }
    });
    // The channel carries `SyncEvent<InventoryEntry>` internally; this walk
    // terminates wholesale on failure, so COMPLETE coverage is accurate.
    // Abort-on-drop for the same reason as `changes_stream`: a dropped
    // stream must stop the work and release the pooled connection rather
    // than leave a second driver running against the same account.
    Box::pin(
        super::guarded_receiver_stream(rx, task).map(bifrost_types::lift_complete_walk(domain)),
    )
}

/// Ownership ticket for the account-level QRESYNC negotiation one-shot.
///
/// `run_inventory` is the only place that spends that one-shot for real,
/// and it does so inside a spawned task whose receiver the engine may drop
/// at any time. Taking it and then losing the stream would burn the
/// warning with nobody informed, and there is no second chance: the flag
/// is one-way. The ticket therefore un-spends the one-shot when the task
/// ends with its output channel already closed - the only case in which
/// the warning provably reached no one. If the receiver is still alive the
/// warning is either delivered or sitting buffered in the channel, where
/// it survives the sender being dropped, so nothing is restored and the
/// no-per-folder-spam property of the one-shot is preserved.
struct QresyncWarningTicket {
    account: ImapAccount,
    tx: tokio::sync::mpsc::Sender<SyncEvent<InventoryEntry>>,
    taken: std::sync::atomic::AtomicBool,
}

impl QresyncWarningTicket {
    fn new(account: ImapAccount, tx: tokio::sync::mpsc::Sender<SyncEvent<InventoryEntry>>) -> Self {
        Self {
            account,
            tx,
            taken: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn take(&self) -> Option<String> {
        let reason = self.account.take_qresync_negotiation_warning()?;
        self.taken.store(true, std::sync::atomic::Ordering::Release);
        Some(reason)
    }
}

impl Drop for QresyncWarningTicket {
    fn drop(&mut self) {
        if self.taken.load(std::sync::atomic::Ordering::Acquire) && self.tx.is_closed() {
            self.account.restore_qresync_negotiation_warning();
        }
    }
}

/// Marker for an output-channel-dropped send failure. Per the IMAP plan,
/// a dropped consumer is not an error to escalate: the streaming task
/// just returns. We model it as a separate variant so the spawn handler
/// can distinguish "consumer left" (silent return) from "wire failed"
/// (`terminated_event`).
struct ChannelDropped;

enum InventoryError {
    Imap(crate::Error),
    /// A pre-classified `AccountError` (the shared-folder revocation path
    /// builds `ScopeRevoked` rather than letting the raw permission denial
    /// derive terminal `NoPermission`).
    Account(bifrost_types::AccountError),
    ChannelDropped,
}

impl From<crate::Error> for InventoryError {
    fn from(value: crate::Error) -> Self {
        Self::Imap(value)
    }
}

impl From<ChannelDropped> for InventoryError {
    fn from(_: ChannelDropped) -> Self {
        Self::ChannelDropped
    }
}

async fn run_inventory(
    account: ImapAccount,
    scope: CursorScope,
    tx: tokio::sync::mpsc::Sender<SyncEvent<InventoryEntry>>,
    qresync_warning: &QresyncWarningTicket,
) -> Result<(), InventoryError> {
    if let Some(reason) = qresync_warning.take() {
        tx.send(SyncEvent::Warning(
            Warning::support_only(WarningKind::StrategyDowngraded, reason).with_protocol_detail(
                DiagnosticText::support_only(format!(
                    "{:?}->{:?}",
                    SyncStrategy::QResync,
                    SyncStrategy::Condstore
                )),
            ),
        ))
        .await
        .map_err(|_| ChannelDropped)?;
    }
    // `folder_from_scope` already returns a classified `AccountError`
    // (`Unsupported` for a non-folder scope, `Request(Malformed)` for an
    // unsendable name). Stringifying it into `Error::Protocol` would
    // re-derive it as a provider contract violation at the boundary, which
    // is exactly what the producer-preserves-classification rule forbids.
    let folder = folder_from_scope(&scope, bifrost_types::AccountOperation::SyncInventory)
        .map_err(InventoryError::Account)?;
    let shared_owner = account
        .folders
        .get(&folder)
        .and_then(|entry| entry.shared_owner.clone());
    let mut conn = account.checkout_for_folder(&folder).await?;
    let mut previous_bytes_in = conn.connection().bytes_in();
    let selected = match account.select_folder(&mut conn, &folder, None, true).await {
        Ok(selected) => selected,
        // A permission denial on a shared folder quarantines just that
        // scope (`ScopeRevoked` -> `DisableScope`) instead of escalating
        // account-wide. A personal folder, or any non-permission failure,
        // flows through the normal mapping.
        Err(err) if shared_owner.is_some() => {
            return Err(InventoryError::Account(super::error::shared_folder_error(
                err,
                &folder,
                shared_owner.as_ref(),
                super::error::ImapErrorContext::operation(
                    bifrost_types::AccountOperation::SyncInventory,
                )
                .with_folder_scope(&folder),
            )));
        }
        Err(err) => return Err(err.into()),
    };
    let uidvalidity = super::selected_uidvalidity(&selected.mailbox)?;
    let include_modseq = selected.mailbox.highest_mod_seq.is_some() && !selected.mailbox.no_mod_seq;
    let attrs = inventory_attrs(include_modseq);
    let all_uids = UidSet::all();
    let (mut fetch_rx, fetch_fut) = conn.connection().uid_fetch_stream(
        all_uids.as_sequence_set(),
        &attrs,
        account.command_timeout(),
    )?;
    tokio::pin!(fetch_fut);

    let mut batch_items = Vec::with_capacity(BATCH_ITEMS);
    let mut known = Vec::new();
    let mut fetch_result = None;

    loop {
        tokio::select! {
            result = &mut fetch_fut, if fetch_result.is_none() => {
                fetch_result = Some(result);
            }
            item = fetch_rx.recv() => {
                match item {
                    Some(Ok(fetch)) => {
                        if let Some(uid) = fetch.uid {
                            if let Some(modseq) = fetch.mod_seq {
                                account
                                    .folders
                                    .record_modseq(&folder, uidvalidity, uid, modseq)?;
                            }
                            known.push(uid);
                            batch_items.push(fetch_to_inventory(
                                &folder,
                                uidvalidity,
                                fetch,
                                shared_owner.as_ref(),
                            ));
                            if batch_items.len() >= BATCH_ITEMS {
                                let out = std::mem::take(&mut batch_items);
                                let bytes_in = super::measured_bytes_in(
                                    conn.connection(),
                                    &mut previous_bytes_in,
                                );
                                tx.send(batch(out, PageBoundary::Page, None, bytes_in))
                                    .await
                                    .map_err(|_| ChannelDropped)?;
                            }
                        }
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        // The driver drops the item sender before it answers
                        // the oneshot, so a failed FETCH closes `fetch_rx`
                        // first and this arm can win the race against
                        // `fetch_fut`. Awaiting the future here (rather than
                        // reading whatever `select!` happened to store) is
                        // what makes the failure unconditional: without it a
                        // tagged NO, a read error, or a timeout would break on
                        // the success path and checkpoint a truncated mailbox.
                        match fetch_result.take() {
                            Some(result) => result?,
                            None => (&mut fetch_fut).await?,
                        }
                        break;
                    }
                }
            }
        }
    }

    let known_uids = CompactUidSet::from_uids(known);
    let cursor = account.cursor_from_select(&selected.mailbox, Some(known_uids))?;
    let bytes_in = super::measured_bytes_in(conn.connection(), &mut previous_bytes_in);
    finish_inventory(
        &account.folders,
        &folder,
        cursor,
        batch_items,
        &tx,
        bytes_in,
    )
    .await?;
    Ok(())
}

async fn finish_inventory(
    folders: &super::folder_registry::FolderRegistry,
    folder: &MailboxName,
    cursor: super::FolderCursor,
    batch_items: Vec<InventoryEntry>,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<InventoryEntry>>,
    bytes_in: u64,
) -> Result<(), ChannelDropped> {
    let checkpoint = Some(Checkpoint::Change(encode_cursor(
        folder_scope(folder),
        &cursor,
    )));
    if batch_items.is_empty() {
        tx.send(SyncEvent::Done(checkpoint))
            .await
            .map_err(|_| ChannelDropped)?;
    } else {
        tx.send(batch(
            batch_items,
            PageBoundary::Final,
            checkpoint.clone(),
            bytes_in,
        ))
        .await
        .map_err(|_| ChannelDropped)?;
        tx.send(SyncEvent::Done(checkpoint))
            .await
            .map_err(|_| ChannelDropped)?;
    }
    // Commit only after the consumer channel accepted the checkpoint.
    folders.set_cursor(folder, cursor);
    Ok(())
}

pub(crate) fn inventory_attrs(include_modseq: bool) -> Vec<FetchAttr> {
    let mut attrs = vec![
        FetchAttr::Uid,
        FetchAttr::Flags,
        FetchAttr::Envelope,
        FetchAttr::Rfc822Size,
    ];
    if include_modseq {
        attrs.push(FetchAttr::ModSeq);
    }
    attrs
}

pub(crate) fn fetch_to_inventory(
    folder: &MailboxName,
    uidvalidity: u32,
    fetch: FetchResponse,
    shared_owner: Option<&bifrost_types::MailboxId>,
) -> InventoryEntry {
    let flags_hash = flags_hash(fetch.flags.as_deref().unwrap_or(&[]));
    let envelope = fetch.envelope;
    // A shared/other-user folder's inventory item carries both its
    // `Folder` membership and its owning `Mailbox(owner)` membership so
    // the consumer can map the item to a shared mailbox identity (A5c).
    let mut memberships = vec![membership_scope(folder)];
    if let Some(owner) = shared_owner {
        memberships.push(bifrost_types::MembershipScope::Mailbox(owner.clone()));
    }
    InventoryEntry {
        id: encode_object_id(folder, uidvalidity, fetch.uid.unwrap_or_default()),
        memberships,
        size: fetch.rfc822_size,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: fetch
                .mod_seq
                .map(ServerVersion::ModSeq)
                .unwrap_or(ServerVersion::Unavailable),
            size: fetch.rfc822_size,
            flags_hash,
        },
        thread_id: fetch
            .thread_id
            .or_else(|| fetch.gmail_thread_id.map(|id| id.to_string()))
            .map(ThreadId),
        message_id: envelope
            .as_ref()
            .and_then(|env| env.bare_message_id().map(str::to_owned)),
        references: Vec::new(),
        in_reply_to: envelope
            .as_ref()
            .and_then(|env| env.first_in_reply_to().map(str::to_owned)),
    }
}

pub(crate) fn flags_hash(flags: &[crate::types::Flag]) -> u64 {
    bifrost_types::canonical_flags_hash(flags.iter().map(crate::types::Flag::as_imap_str))
}

pub(crate) fn flags_set(flags: &[crate::types::Flag]) -> HashSet<String> {
    flags
        .iter()
        .map(|flag| flag.as_imap_str().to_ascii_lowercase())
        .collect()
}

/// The IMAP flag judge behind `ImapAccount::flag_target`. `\Recent` is
/// server-owned: no STORE sets or clears it, so an operation naming it is
/// undecidable, and a `Set` compares the observation with `\Recent` removed.
/// Everything else is the generic case-insensitive judge, which is IMAP flag
/// identity.
pub(crate) fn imap_flag_target(
    observed: &HashSet<String>,
    op: &bifrost_types::FlagOp,
) -> bifrost_types::FlagTargetState {
    use bifrost_types::{FlagOp, FlagTargetState};
    let recent = |flags: &HashSet<String>| flags.iter().any(|f| f.eq_ignore_ascii_case("\\recent"));
    let names_recent = match op {
        FlagOp::Add(s) | FlagOp::Remove(s) | FlagOp::Set(s) => recent(s),
        FlagOp::Patch { add, remove } => recent(add) || recent(remove),
        _ => return FlagTargetState::Indeterminate,
    };
    if names_recent {
        return FlagTargetState::Indeterminate;
    }
    if matches!(op, FlagOp::Set(_)) {
        let flags = observed
            .iter()
            .filter(|f| !f.eq_ignore_ascii_case("\\recent"))
            .cloned()
            .collect();
        bifrost_types::flag_target_state(&flags, op)
    } else {
        bifrost_types::flag_target_state(observed, op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imap_flag_target_ignores_recent_for_set() {
        let observed = ["\\seen", "\\recent"]
            .map(String::from)
            .into_iter()
            .collect();
        let op = bifrost_types::FlagOp::Set(["\\Seen"].map(String::from).into_iter().collect());
        assert_eq!(
            super::imap_flag_target(&observed, &op),
            bifrost_types::FlagTargetState::Satisfied
        );
    }

    #[test]
    fn imap_flag_target_refuses_ops_naming_recent() {
        use bifrost_types::{FlagOp, FlagTargetState};
        let recent = ["\\Recent"].map(String::from).into_iter().collect();
        for op in [
            FlagOp::Add(recent),
            FlagOp::Remove(["\\recent".into()].into_iter().collect()),
            FlagOp::Set(["\\RECENT".into()].into_iter().collect()),
            FlagOp::Patch {
                add: ["x".into()].into_iter().collect(),
                remove: ["\\recent".into()].into_iter().collect(),
            },
        ] {
            assert_eq!(
                super::imap_flag_target(&Default::default(), &op),
                FlagTargetState::Indeterminate
            );
        }
    }

    #[test]
    fn inventory_attrs_request_modseq_only_when_available() {
        assert!(
            inventory_attrs(true)
                .iter()
                .any(|attr| matches!(attr, FetchAttr::ModSeq))
        );
        assert!(
            inventory_attrs(false)
                .iter()
                .all(|attr| !matches!(attr, FetchAttr::ModSeq))
        );
    }

    fn folder() -> MailboxName {
        MailboxName::new("INBOX").expect("valid mailbox")
    }

    mod qresync_ticket {
        use super::super::super::test_support::scripted_account_parts;
        use super::super::super::{ImapAccount, ImapAccountParts};
        use super::super::QresyncWarningTicket;
        use crate::connection::test_support::{driver_pair, preauth_greeting};

        async fn account() -> (ImapAccount, tokio::io::DuplexStream) {
            let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
            let account = ImapAccount::new(ImapAccountParts {
                qresync_negotiation_warning: Some("QRESYNC refused by the server".to_string()),
                ..scripted_account_parts(conn, 1)
            });
            (account, server)
        }

        /// A taker whose output channel is already gone un-spends the
        /// one-shot: the warning reached nobody, and the flag is one-way.
        #[tokio::test]
        async fn a_ticket_whose_receiver_is_gone_restores_the_one_shot() {
            let (account, _server) = account().await;
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let ticket = QresyncWarningTicket::new(account.clone(), tx);

            assert!(ticket.take().is_some(), "the one-shot starts unspent");
            drop(rx);
            drop(ticket);

            assert!(
                account.take_qresync_negotiation_warning().is_some(),
                "a warning delivered to nobody must remain available",
            );
        }

        /// With the receiver still alive the warning is delivered, or
        /// buffered in a channel that outlives its sender, so the one-shot
        /// stays spent and no later folder re-reports it.
        #[tokio::test]
        async fn a_ticket_with_a_live_receiver_keeps_the_one_shot_spent() {
            let (account, _server) = account().await;
            let (tx, _rx) = tokio::sync::mpsc::channel(1);
            let ticket = QresyncWarningTicket::new(account.clone(), tx);

            assert!(ticket.take().is_some());
            drop(ticket);

            assert!(
                account.take_qresync_negotiation_warning().is_none(),
                "a delivered warning must not be re-reported per folder",
            );
        }
    }

    #[tokio::test]
    async fn inventory_cursor_waits_for_terminal_checkpoint_admission() {
        use super::super::folder_registry::{CompactUidSet, FolderRegistry};
        for has_items in [false, true] {
            let folder = folder();
            let folders = FolderRegistry::from_list(vec![crate::types::MailboxInfo {
                name: folder.clone(),
                ..Default::default()
            }]);
            let cursor = super::super::FolderCursor::Basic {
                uidvalidity: 7,
                uidnext: 2,
                known_uids: CompactUidSet::from_uids([1]),
            };
            let items = if has_items {
                vec![fetch_to_inventory(
                    &folder,
                    7,
                    FetchResponse {
                        uid: Some(1),
                        ..Default::default()
                    },
                    None,
                )]
            } else {
                Vec::new()
            };
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            // Empty inventory blocks on Done; nonempty inventory admits the
            // final batch and then blocks on Done. Neither may commit yet.
            if !has_items {
                tx.send(SyncEvent::Done(None)).await.unwrap();
            }
            let mut finish = Box::pin(super::finish_inventory(
                &folders, &folder, cursor, items, &tx, 0,
            ));
            assert!(futures::poll!(finish.as_mut()).is_pending());
            assert!(folders.get(&folder).unwrap().cursor().is_none());
            drop(rx);
            assert!(finish.await.is_err());
            assert!(folders.get(&folder).unwrap().cursor().is_none());
            let cursor = super::super::FolderCursor::Basic {
                uidvalidity: 7,
                uidnext: 2,
                known_uids: CompactUidSet::from_uids([1]),
            };
            let (tx, _rx) = tokio::sync::mpsc::channel(2);
            assert!(
                super::finish_inventory(&folders, &folder, cursor.clone(), Vec::new(), &tx, 0)
                    .await
                    .is_ok()
            );
            assert_eq!(folders.get(&folder).unwrap().cursor(), Some(cursor));
        }
    }

    fn keyword(name: &str) -> crate::types::Flag {
        crate::types::Flag::Custom(name.to_owned())
    }

    // The fingerprint's flags_hash is what the engine diffs to decide
    // "refetch or not", so it must be stable under wire-order and
    // wire-case differences that carry no semantic change, and it must
    // collapse a duplicated flag.
    #[test]
    fn flags_hash_ignores_order_case_and_duplicates() {
        use crate::types::Flag;
        let a = flags_hash(&[Flag::Seen, Flag::Flagged]);
        let b = flags_hash(&[Flag::Flagged, Flag::Seen]);
        assert_eq!(a, b, "flag order must not change the fingerprint");

        let canonical = flags_hash(&[keyword("$Important")]);
        let shouted = flags_hash(&[keyword("$IMPORTANT")]);
        assert_eq!(canonical, shouted, "IMAP flags are case-insensitive");

        let once = flags_hash(&[Flag::Seen]);
        let twice = flags_hash(&[Flag::Seen, Flag::Seen]);
        assert_eq!(once, twice, "a repeated flag must not change the hash");
    }

    #[test]
    fn flags_hash_separates_distinct_flag_sets() {
        use crate::types::Flag;
        assert_ne!(flags_hash(&[]), flags_hash(&[Flag::Seen]));
        assert_ne!(flags_hash(&[Flag::Seen]), flags_hash(&[Flag::Flagged]));
        assert_ne!(
            flags_hash(&[Flag::Seen]),
            flags_hash(&[Flag::Seen, Flag::Flagged])
        );
        // The canonical join uses `\n`, so two keywords must not collide
        // with the single keyword formed by concatenating them.
        assert_ne!(
            flags_hash(&[keyword("$a"), keyword("$b")]),
            flags_hash(&[keyword("$a$b")])
        );
    }

    #[test]
    fn flags_set_lowercases_the_wire_form() {
        use crate::types::Flag;
        let set = flags_set(&[Flag::Seen, keyword("$Important")]);
        assert!(set.contains("\\seen"));
        assert!(set.contains("$important"));
        assert!(!set.contains("\\Seen"));
    }

    #[test]
    fn inventory_entry_carries_owner_membership_only_for_a_shared_folder() {
        let fetch = FetchResponse {
            uid: Some(42),
            mod_seq: Some(7),
            rfc822_size: Some(1024),
            ..Default::default()
        };
        let personal = fetch_to_inventory(&folder(), 9, fetch.clone(), None);
        assert_eq!(
            personal.memberships,
            vec![bifrost_types::MembershipScope::Folder(
                bifrost_types::FolderId("INBOX".to_owned())
            )]
        );
        assert_eq!(personal.id, encode_object_id(&folder(), 9, 42));
        assert_eq!(personal.size, Some(1024));
        assert_eq!(
            personal.fingerprint.server_version,
            ServerVersion::ModSeq(7)
        );

        let owner = bifrost_types::MailboxId("alice".to_owned());
        let shared = fetch_to_inventory(&folder(), 9, fetch, Some(&owner));
        assert!(
            shared
                .memberships
                .contains(&bifrost_types::MembershipScope::Mailbox(owner)),
            "a shared folder's item must also carry its owning mailbox",
        );
        assert_eq!(shared.memberships.len(), 2);
    }

    // Without CONDSTORE there is no per-message version stamp, so the
    // fingerprint has to fall back to `Unavailable` and let the
    // size + flags_hash pair carry the diff.
    #[test]
    fn inventory_entry_without_modseq_reports_an_unavailable_server_version() {
        let fetch = FetchResponse {
            uid: Some(1),
            ..Default::default()
        };
        let entry = fetch_to_inventory(&folder(), 9, fetch, None);
        assert_eq!(entry.fingerprint.server_version, ServerVersion::Unavailable);
        assert_eq!(entry.size, None);
        assert_eq!(entry.fingerprint.size, None);
    }

    #[test]
    fn inventory_entry_prefers_objectid_threadid_over_the_gmail_one() {
        let both = FetchResponse {
            uid: Some(1),
            thread_id: Some("T-objectid".to_owned()),
            gmail_thread_id: Some(1234),
            ..Default::default()
        };
        let entry = fetch_to_inventory(&folder(), 9, both, None);
        assert_eq!(entry.thread_id, Some(ThreadId("T-objectid".to_owned())));

        let gmail_only = FetchResponse {
            uid: Some(1),
            gmail_thread_id: Some(1234),
            ..Default::default()
        };
        let entry = fetch_to_inventory(&folder(), 9, gmail_only, None);
        assert_eq!(entry.thread_id, Some(ThreadId("1234".to_owned())));
    }

    #[test]
    fn inventory_entry_strips_angle_brackets_from_the_threading_headers() {
        let envelope = crate::types::Envelope {
            message_id: Some("<abc@example.test>".to_owned()),
            in_reply_to: Some("<parent@example.test>".to_owned()),
            ..Default::default()
        };
        let fetch = FetchResponse {
            uid: Some(1),
            envelope: Some(envelope),
            ..Default::default()
        };
        let entry = fetch_to_inventory(&folder(), 9, fetch, None);
        assert_eq!(entry.message_id.as_deref(), Some("abc@example.test"));
        assert_eq!(entry.in_reply_to.as_deref(), Some("parent@example.test"));
        // IMAP ENVELOPE has no References field, so the list stays empty.
        assert!(entry.references.is_empty());
    }
}
