use bifrost_types::FlagSet;
use bifrost_types::{
    AccountError, AccountFuture, AccountStream, Checkpoint, CursorEstablishment, CursorScope,
    DiagnosticText, Fingerprint, InventoryEntry, PageBoundary, ServerVersion, SyncEvent,
    SyncStrategy, Warning, WarningKind,
};
use futures::StreamExt as _;

use crate::types::{FetchAttr, FetchResponse, MailboxName, UidSetExpr};

use super::{
    BATCH_ITEMS, ImapAccount, ScopeHandler, UidSet, batch, encode_cursor, encode_object_id,
    folder_scope, membership_scope, route_scope, terminated_event,
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
    let folder = match route_scope(
        &account,
        &scope,
        bifrost_types::AccountOperation::SyncInventory,
    ) {
        // The sub-account already speaks the inventory envelope, so its stream
        // passes straight through. Converting here instead would relabel
        // whatever coverage the delegate reported as COMPLETE.
        Ok(ScopeHandler::Delegate(sub)) => return sub.inventory_stream(scope),
        Ok(ScopeHandler::Folder(folder)) => folder,
        Err(error) => {
            return Box::pin(futures::stream::iter([
                bifrost_types::InventoryEvent::Terminated(error),
            ]));
        }
    };
    let domain = bifrost_types::CoverageDomain::full(scope.clone());
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    let ticket = QresyncWarningTicket::new(account.clone(), tx.clone());
    let ledger = ObligationLedger::default();
    let task_ledger = ledger.clone();
    let task = tokio::spawn(async move {
        match run_inventory(
            account.clone(),
            scope.clone(),
            folder.clone(),
            tx.clone(),
            &ticket,
            &task_ledger,
        )
        .await
        {
            Ok(()) | Err(InventoryError::ChannelDropped) => {}
            Err(InventoryError::Imap(err)) => {
                let error = account.folder_error(
                    err,
                    &folder,
                    super::error::ImapErrorContext::operation(
                        bifrost_types::AccountOperation::SyncInventory,
                    ),
                );
                let _ = tx.send(terminated_event(error)).await;
            }
            Err(InventoryError::Account(err)) => {
                let _ = tx
                    .send(super::terminated_event::<InventoryEntry, _>(err))
                    .await;
            }
        }
    });
    // The channel carries `SyncEvent<InventoryEntry>` internally; this walk
    // terminates wholesale on a wire failure, so coverage is COMPLETE except
    // for the per-object obligations `run_inventory` records in the ledger
    // (a FETCH response missing a data item the entry needs). Those are
    // attached to the next page and, cumulatively, to `Done`.
    // Abort-on-drop for the same reason as `changes_stream`: a dropped
    // stream must stop the work and release the pooled connection rather
    // than leave a second driver running against the same account.
    let mut lift = bifrost_types::lift_complete_walk(domain.clone());
    let mut walk: Vec<bifrost_types::InventoryObligation> = Vec::new();
    Box::pin(super::guarded_receiver_stream(rx, task).map(move |event| {
        let mut lifted = lift(event);
        // Object obligations are never barriers, so replacing the
        // coverage leaves the derived `PageCheckpoint` variant valid.
        match &mut lifted {
            bifrost_types::InventoryEvent::Batch(page) => {
                let fresh = ledger.drain();
                walk.extend(fresh.iter().cloned());
                page.coverage = bifrost_types::InventoryCoverageReport::from_obligations(
                    domain.clone(),
                    &fresh,
                );
            }
            bifrost_types::InventoryEvent::Done(completion) => {
                walk.extend(ledger.drain());
                completion.coverage =
                    bifrost_types::InventoryCoverageReport::from_obligations(domain.clone(), &walk);
            }
            _ => {}
        }
        lifted
    }))
}

/// Per-object obligations a walk recorded, handed from the FETCH task to the
/// stream adapter that stamps page coverage.
#[derive(Clone, Default)]
struct ObligationLedger(std::sync::Arc<std::sync::Mutex<Vec<bifrost_types::InventoryObligation>>>);

impl ObligationLedger {
    fn record(&self, obligation: bifrost_types::InventoryObligation) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(obligation);
    }

    fn drain(&self) -> Vec<bifrost_types::InventoryObligation> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// A FETCH response for a known UID that lacked a data item the entry needs
/// (FLAGS). The message exists, so it is neither emitted with invented state
/// nor silently dropped: an Object obligation tells the engine the walk did
/// not represent it, so its prior state is kept and repaired by id later.
fn unrepresented_object(
    folder: &MailboxName,
    uidvalidity: crate::types::UidValidity,
    uid: u32,
    error: crate::Error,
) -> bifrost_types::InventoryObligation {
    let id = encode_object_id(
        folder,
        uidvalidity,
        crate::types::Uid::new(uid).expect("wire UID is nonzero"),
    );
    bifrost_types::InventoryObligation::Object {
        key: bifrost_types::ObligationKey(id.0.as_bytes().to_vec()),
        id,
        error: super::account_error_with(
            error,
            super::error::ImapErrorContext::operation(
                bifrost_types::AccountOperation::SyncInventory,
            )
            .with_folder_scope(folder),
        ),
        repair: Vec::new(),
    }
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
    folder: super::ImapFolder,
    tx: tokio::sync::mpsc::Sender<SyncEvent<InventoryEntry>>,
    qresync_warning: &QresyncWarningTicket,
    obligations: &ObligationLedger,
) -> Result<(), InventoryError> {
    if let Some(reason) = qresync_warning.take() {
        tx.send(SyncEvent::Warning(
            Warning::support_only(WarningKind::StrategyDowngraded, reason)
                .with_strategy_downgrade(bifrost_types::StrategyDowngrade::QResyncToCondstore)
                .with_scope(bifrost_types::ErrorScope::Cursor(scope.clone()))
                .with_protocol_detail(DiagnosticText::support_only(format!(
                    "{:?}->{:?}",
                    SyncStrategy::QResync,
                    SyncStrategy::Condstore
                ))),
        ))
        .await
        .map_err(|_| ChannelDropped)?;
    }
    let folder = folder.into_mailbox();
    let shared_owner = account
        .folders
        .get(&folder)
        .and_then(|entry| entry.shared_owner.clone());
    let mut conn = account.checkout_for_folder(&folder).await?;
    let mut previous_bytes_in = conn.connection().bytes_in();
    let selected = match account
        .select_folder(&mut conn, &folder, super::SelectPurpose::Read)
        .await
    {
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
    // UIDNOTSTICKY must keep exposing mail: enterprise users expect the
    // folder to remain visible. A cursor-free walk cannot be enabled just
    // here: CursorEstablishment has only Ready and EstablishViaInventory,
    // and the engine's FusionOutcome::NoCursor does not schedule another
    // inventory poll. Done(None) would therefore stop ongoing sync rather
    // than fix it. The shared contract needs a recurring inventory mode,
    // with authoritative replacement on each poll and generation-scoped
    // object handles, before this site can stop minting UID cursors safely.
    let uidvalidity = super::selected_uidvalidity(&selected.mailbox)?;
    let include_modseq = selected.mailbox.modseq().is_some();
    let mut attrs = inventory_attrs(include_modseq);
    super::hydration::push_native_thread_attrs(&mut attrs, &conn.connection().server_profile());
    let all_uids = UidSetExpr::all();
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
                        // A response without a nonzero UID cannot be
                        // attributed to any message and is ignored.
                        if let Some(uid) = fetch.uid.filter(|uid| *uid != 0) {
                            if let Some(modseq) = fetch.mod_seq {
                                account
                                    .folders
                                    .record_modseq(&folder, uidvalidity, uid, modseq)?;
                            }
                            known.push(uid);
                            // One response missing FLAGS fails only that
                            // message: an enterprise user expects the rest
                            // of the folder to sync, and that message's
                            // cached read/flagged state to stay as it was
                            // rather than be cleared. The obligation tells
                            // the engine this walk did not represent it.
                            match fetch_to_inventory(
                                &folder,
                                uidvalidity,
                                fetch,
                                shared_owner.as_ref(),
                            ) {
                                Ok(entry) => batch_items.push(entry),
                                Err(error) => obligations.record(unrepresented_object(
                                    &folder,
                                    uidvalidity,
                                    uid,
                                    error,
                                )),
                            }
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

    let known_uids = UidSet::from_numbers(known);
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
    uidvalidity: crate::types::UidValidity,
    fetch: FetchResponse,
    shared_owner: Option<&bifrost_types::PrincipalId>,
) -> Result<InventoryEntry, crate::Error> {
    // Missing observation is not an empty flag set: inventing it can make
    // an enterprise client's cached read/flagged state silently regress.
    let uid = fetch.uid.filter(|uid| *uid != 0).ok_or_else(|| {
        crate::Error::Provider(crate::error::ProviderFailure::Missing(
            "FETCH response lacked a nonzero UID".into(),
        ))
    })?;
    let flags = fetch.flags.as_deref().ok_or_else(|| {
        crate::Error::Provider(crate::error::ProviderFailure::Missing(format!(
            "FETCH response lacked FLAGS for UID {uid}"
        )))
    })?;
    let flags_hash = flags_hash(flags);
    let envelope = fetch.envelope;
    // A shared/other-user folder's inventory item carries both its
    // `Folder` membership and its owning `Owner(owner)` membership so
    // the consumer can map the item to a shared mailbox identity (A5c).
    let mut memberships = vec![membership_scope(folder)];
    if let Some(owner) = shared_owner {
        memberships.push(bifrost_types::MembershipScope::Owner(owner.clone()));
    }
    Ok(InventoryEntry {
        id: encode_object_id(
            folder,
            uidvalidity,
            crate::types::Uid::new(uid).expect("wire UID is nonzero"),
        ),
        memberships,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: fetch
                .mod_seq
                .map(|value| ServerVersion::ModSeq(value.get()))
                .unwrap_or(ServerVersion::Unavailable),
            size: fetch.rfc822_size,
            flags_hash,
        },
        thread_id: super::envelope::encode_server_thread_id(
            fetch.thread_id.as_deref(),
            fetch.gmail_thread_id,
        ),
        message_id: envelope
            .as_ref()
            .and_then(|env| env.bare_message_id().map(str::to_owned)),
        references: Vec::new(),
        in_reply_to: envelope
            .as_ref()
            .and_then(|env| env.first_in_reply_to().map(str::to_owned)),
    })
}

pub(crate) fn flags_hash(flags: &[crate::types::Flag]) -> u64 {
    bifrost_types::canonical_flags_hash(flags.iter().map(crate::types::Flag::as_imap_str))
}

pub(crate) fn flags_set(flags: &[crate::types::Flag]) -> FlagSet {
    flags
        .iter()
        .map(|flag| flag.as_imap_str().to_ascii_lowercase())
        .collect()
}

/// The IMAP flag judge behind `ImapAccount::flag_target`. `\Recent` is
/// server-owned, and `\*` is only a permanent-keyword grant. Neither can be
/// requested by STORE, so naming either is undecidable. Set comparison removes
/// both from the observation.
/// Requests use the same neutral-keyword conversion as STORE, followed by
/// the generic case-insensitive judge. Observations retain wire identity.
pub(crate) fn imap_flag_target(
    observed: &FlagSet,
    op: &bifrost_types::FlagOp,
) -> bifrost_types::FlagTargetState {
    use bifrost_types::{FlagOp, FlagTargetState};
    let unwritable = |flags: &FlagSet| {
        flags
            .iter()
            .any(|f| !crate::types::Flag::from_neutral_keyword(f).is_writable())
    };
    let names_unwritable = match op {
        FlagOp::Add(s) | FlagOp::Remove(s) | FlagOp::Set(s) => unwritable(s),
        FlagOp::Patch { add, remove } => unwritable(add) || unwritable(remove),
        _ => return FlagTargetState::Indeterminate,
    };
    if names_unwritable {
        return FlagTargetState::Indeterminate;
    }
    // Translate only the request: a literal server keyword `$seen` is not
    // evidence that the system flag `\Seen` was stored.
    let normalize = |flags: &FlagSet| {
        flags
            .iter()
            .map(|flag| {
                crate::types::Flag::from_neutral_keyword(flag)
                    .as_imap_str()
                    .to_owned()
            })
            .collect()
    };
    let op = match op {
        FlagOp::Add(flags) => FlagOp::Add(normalize(flags)),
        FlagOp::Remove(flags) => FlagOp::Remove(normalize(flags)),
        FlagOp::Set(flags) => FlagOp::Set(normalize(flags)),
        FlagOp::Patch { add, remove } => FlagOp::Patch {
            add: normalize(add),
            remove: normalize(remove),
        },
        _ => return FlagTargetState::Indeterminate,
    };
    if matches!(op, FlagOp::Set(_)) {
        let flags = observed
            .iter()
            .filter(|f| crate::types::Flag::from_imap_str(f).is_writable())
            .cloned()
            .collect();
        bifrost_types::flag_target_state(&flags, &op)
    } else {
        bifrost_types::flag_target_state(observed, &op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imap_flag_target_uses_store_aliases_for_every_operation() {
        use bifrost_types::{FlagOp, FlagTargetState};
        for (neutral, wire) in [
            ("$SEEN", "\\Seen"),
            ("$answered", "\\Answered"),
            ("$flagged", "\\Flagged"),
        ] {
            let requested = FlagSet::from([neutral.to_owned()]);
            let observed = FlagSet::from([wire.to_owned()]);
            for op in [
                FlagOp::Add(requested.clone()),
                FlagOp::Set(requested.clone()),
            ] {
                assert_eq!(imap_flag_target(&observed, &op), FlagTargetState::Satisfied);
                assert_eq!(
                    imap_flag_target(&requested, &op),
                    FlagTargetState::Unsatisfied
                );
            }
            for op in [
                FlagOp::Remove(requested.clone()),
                FlagOp::Patch {
                    add: FlagSet::from(["custom".to_owned()]),
                    remove: requested,
                },
            ] {
                assert_eq!(
                    imap_flag_target(&observed, &op),
                    FlagTargetState::Unsatisfied
                );
                let cleared = FlagSet::from(["custom".to_owned()]);
                assert_eq!(imap_flag_target(&cleared, &op), FlagTargetState::Satisfied);
            }
        }
        let overlap = FlagOp::Patch {
            add: FlagSet::from(["$seen".to_owned()]),
            remove: FlagSet::from(["\\Seen".to_owned()]),
        };
        assert_eq!(
            imap_flag_target(&FlagSet::new(), &overlap),
            FlagTargetState::Indeterminate
        );
    }

    #[test]
    fn imap_flag_target_ignores_server_only_flags_for_set() {
        let observed = ["\\seen", "\\recent", "\\*"]
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
            FlagOp::Remove(["\\recent".to_owned()].into_iter().collect()),
            FlagOp::Set(["\\RECENT".to_owned()].into_iter().collect()),
            FlagOp::Patch {
                add: ["x".to_owned()].into_iter().collect(),
                remove: ["\\recent".to_owned()].into_iter().collect(),
            },
        ] {
            assert_eq!(
                super::imap_flag_target(&Default::default(), &op),
                FlagTargetState::Indeterminate
            );
        }
    }

    #[test]
    fn imap_flag_target_refuses_wildcard_requests() {
        use bifrost_types::{FlagOp, FlagTargetState};
        let flags = || ["\\*".to_owned()].into_iter().collect();
        for op in [
            FlagOp::Add(flags()),
            FlagOp::Remove(flags()),
            FlagOp::Set(flags()),
            FlagOp::Patch {
                add: flags(),
                remove: Default::default(),
            },
            FlagOp::Patch {
                add: Default::default(),
                remove: flags(),
            },
        ] {
            assert_eq!(
                imap_flag_target(&Default::default(), &op),
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
                qresync: super::super::super::QresyncNegotiation::Refused(
                    super::super::super::QresyncRefusal::EnableNotConfirmed,
                ),
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
        use super::super::folder_registry::FolderRegistry;
        for has_items in [false, true] {
            let folder = folder();
            let folders = FolderRegistry::from_list(vec![crate::types::MailboxInfo {
                name: folder.clone(),
                ..Default::default()
            }]);
            let cursor = super::super::FolderCursor::Basic {
                uidvalidity: crate::types::UidValidity::new(7).expect("nonzero epoch"),
                uidnext: crate::types::Uid::new(2),
                known_uids: UidSet::from_numbers([1]),
            };
            let items = if has_items {
                vec![
                    fetch_to_inventory(
                        &folder,
                        crate::types::UidValidity::new(7).expect("valid protocol test value"),
                        FetchResponse {
                            flags: Some(Vec::new()),
                            uid: Some(1),
                            ..Default::default()
                        },
                        None,
                    )
                    .unwrap(),
                ]
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
                uidvalidity: crate::types::UidValidity::new(7).expect("nonzero epoch"),
                uidnext: crate::types::Uid::new(2),
                known_uids: UidSet::from_numbers([1]),
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
        crate::types::Flag::from_imap_str(name)
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
        // Membership is case-insensitive under `Flag` identity, so the
        // lowercased wire spelling is pinned through the retained spelling.
        let mut spellings: Vec<&str> = set.iter().map(bifrost_types::Flag::as_str).collect();
        spellings.sort_unstable();
        assert_eq!(spellings, ["$important", "\\seen"]);
    }

    #[test]
    fn inventory_requires_uid_and_flags_observations() {
        for uid in [None, Some(0), Some(1)] {
            for flags in [None, Some(Vec::new())] {
                let valid = uid == Some(1) && flags.is_some();
                let fetch = FetchResponse {
                    uid,
                    flags,
                    ..Default::default()
                };
                let result = fetch_to_inventory(
                    &folder(),
                    crate::types::UidValidity::new(9).expect("valid protocol test value"),
                    fetch,
                    None,
                );
                if valid {
                    assert!(result.is_ok());
                } else {
                    assert!(matches!(
                        result,
                        Err(crate::Error::Provider(
                            crate::error::ProviderFailure::Missing(_)
                        ))
                    ));
                }
            }
        }
    }

    #[test]
    fn inventory_entry_carries_owner_membership_only_for_a_shared_folder() {
        let fetch = FetchResponse {
            flags: Some(Vec::new()),
            uid: Some(42),
            mod_seq: Some(crate::types::ModSeqValzer::new(7).expect("valid protocol test value")),
            rfc822_size: Some(1024),
            ..Default::default()
        };
        let personal = fetch_to_inventory(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            fetch.clone(),
            None,
        )
        .unwrap();
        assert_eq!(
            personal.memberships,
            vec![bifrost_types::MembershipScope::Folder(
                bifrost_types::FolderId("INBOX".to_owned())
            )]
        );
        assert_eq!(
            personal.id,
            encode_object_id(
                &folder(),
                crate::types::UidValidity::new(9).expect("nonzero epoch"),
                crate::types::Uid::new(42).expect("valid protocol test value")
            )
        );
        assert_eq!(personal.fingerprint.size, Some(1024));
        assert_eq!(
            personal.fingerprint.server_version,
            ServerVersion::ModSeq(7)
        );

        let owner = bifrost_types::PrincipalId("alice".to_owned());
        let shared = fetch_to_inventory(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            fetch,
            Some(&owner),
        )
        .unwrap();
        assert!(
            shared
                .memberships
                .contains(&bifrost_types::MembershipScope::Owner(owner)),
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
            flags: Some(Vec::new()),
            uid: Some(1),
            ..Default::default()
        };
        let entry = fetch_to_inventory(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            fetch,
            None,
        )
        .unwrap();
        assert_eq!(entry.fingerprint.server_version, ServerVersion::Unavailable);
        assert_eq!(entry.fingerprint.size, None);
    }

    #[test]
    fn inventory_entry_prefers_objectid_threadid_over_the_gmail_one() {
        let both = FetchResponse {
            flags: Some(Vec::new()),
            uid: Some(1),
            thread_id: Some(
                crate::types::validated::ObjectId::new("T-objectid".to_owned())
                    .expect("valid objectid"),
            ),
            gmail_thread_id: Some(crate::types::GmailThreadId::new(1234)),
            ..Default::default()
        };
        let entry = fetch_to_inventory(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            both,
            None,
        )
        .unwrap();
        assert_eq!(
            entry.thread_id,
            crate::account::envelope::encode_server_thread_id(Some("T-objectid"), None)
        );

        let gmail_only = FetchResponse {
            flags: Some(Vec::new()),
            uid: Some(1),
            gmail_thread_id: Some(crate::types::GmailThreadId::new(1234)),
            ..Default::default()
        };
        let entry = fetch_to_inventory(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            gmail_only,
            None,
        )
        .unwrap();
        assert_eq!(
            entry.thread_id,
            crate::account::envelope::encode_server_thread_id(
                None,
                Some(crate::types::GmailThreadId::new(1234))
            )
        );
    }

    #[test]
    fn inventory_entry_strips_angle_brackets_from_the_threading_headers() {
        let envelope = crate::types::Envelope {
            message_id: Some("<abc@example.test>".to_owned()),
            in_reply_to: Some("<parent@example.test>".to_owned()),
            ..Default::default()
        };
        let fetch = FetchResponse {
            flags: Some(Vec::new()),
            uid: Some(1),
            envelope: Some(envelope),
            ..Default::default()
        };
        let entry = fetch_to_inventory(
            &folder(),
            crate::types::UidValidity::new(9).expect("valid protocol test value"),
            fetch,
            None,
        )
        .unwrap();
        assert_eq!(entry.message_id.as_deref(), Some("abc@example.test"));
        assert_eq!(entry.in_reply_to.as_deref(), Some("parent@example.test"));
        // IMAP ENVELOPE has no References field, so the list stays empty.
        assert!(entry.references.is_empty());
    }
}
