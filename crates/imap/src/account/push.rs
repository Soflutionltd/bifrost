use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AccountStream, BatchItemId, BatchOutcomeBuilder,
    CursorScope, HintPayload, InvalidationHint, PushSource, SubscriptionHandle,
};
use tokio_util::sync::CancellationToken;

use crate::account::error::ImapErrorContext;
use crate::connection::IdleEvent;

use super::{ImapAccount, account_error_with, folder_scope};

struct PushScopes {
    by_handle: HashMap<String, HashSet<CursorScope>>,
    version: u64,
    exclusions: HashMap<String, u64>,
}

pub(crate) struct PushState {
    scopes: Mutex<PushScopes>,
    supports_notify: bool,
    push_capacity: usize,
    push_slots: std::sync::Arc<tokio::sync::Semaphore>,
    assignment: tokio::sync::watch::Sender<u64>,
    leases: Vec<bifrost_types::LaneLease>,
    next_id: AtomicU64,
}
impl PushState {
    pub(crate) fn new(supports_notify: bool, push_capacity: usize) -> Self {
        let lanes = if push_capacity == 0 {
            0
        } else if supports_notify {
            1
        } else {
            push_capacity
        };
        Self {
            scopes: Mutex::new(PushScopes {
                by_handle: HashMap::new(),
                version: 0,
                exclusions: HashMap::new(),
            }),
            supports_notify,
            push_capacity,
            push_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(push_capacity)),
            assignment: tokio::sync::watch::Sender::new(0),
            leases: (0..lanes)
                .map(|_| bifrost_types::LaneLease::new())
                .collect(),
            next_id: AtomicU64::new(1),
        }
    }
    #[cfg(test)]
    pub(crate) fn available_push_slots(&self) -> usize {
        self.push_slots.available_permits()
    }
    fn bump_generation(&self) {
        self.assignment.send_modify(|n| *n = n.wrapping_add(1));
    }
}

pub(crate) fn push_lanes(account: &ImapAccount) -> Vec<bifrost_types::PushLaneSpec> {
    account
        .push
        .leases
        .iter()
        .enumerate()
        .map(|(n, _)| bifrost_types::PushLaneSpec {
            lane: bifrost_types::PushLane(u16::try_from(n).expect("push lane index")),
            kind: bifrost_types::PushLaneKind::Connection,
            backoff: bifrost_types::BackoffPolicy::new(
                std::time::Duration::from_secs(5),
                std::time::Duration::from_secs(300),
                false,
            ),
        })
        .collect()
}

pub(crate) fn push_subscribe(
    account: ImapAccount,
    scopes: Vec<CursorScope>,
) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
    Box::pin(async move {
        let id = account.push.next_id.fetch_add(1, Ordering::AcqRel);
        let handle = SubscriptionHandle(format!("imap-idle-{id}"));
        // Batch item ids are submission positions, per the cross-crate
        // contract in `reference/error-model.md`.
        let expected: Vec<_> = (0..scopes.len())
            .map(|index| BatchItemId(index.to_string()))
            .collect();
        let mut outcomes = BatchOutcomeBuilder::new();
        let accepted = {
            let mut subscriptions = account
                .push
                .scopes
                .lock()
                .expect("push scopes lock poisoned");
            // Admission runs against the folders this account already
            // pushes, under the same lock the insert takes, so two
            // concurrent subscribes cannot both admit the last free slot.
            let mut covered: HashSet<String> = subscribed_idle_folders(&subscriptions.by_handle)
                .into_iter()
                .map(|folder| folder.account_id())
                .collect();
            let mut accepted = HashSet::new();
            for (item, scope) in expected.iter().cloned().zip(scopes.iter().cloned()) {
                let CursorScope::Folder(folder) = &scope else {
                    outcomes.push_failed(item, unsupported_push_scope_error());
                    continue;
                };
                // A composed DAV collection scope is a syntactically valid
                // mailbox NAME but names no mailbox. Admitting it would
                // report the collection as pushed (a misreport bifrost-sync
                // trusts) and burn a budget slot on a folder no IDLE worker
                // can ever SELECT. The DAV sub-accounts have no push lane,
                // so the honest per-item answer is the same refusal a typed
                // DAV scope has always received; the collection keeps
                // polling.
                if account.dav_scopes.owner(folder).is_some() {
                    outcomes.push_failed(item, unsupported_push_scope_error());
                    continue;
                }
                // Admission must accept exactly what the worker assignment
                // can watch: `subscribed_idle_folders` parses each stored
                // scope through `MailboxName::new`, which rejects NUL/CR/LF.
                // Admitting such a name would report it succeeded and burn a
                // budget slot on a folder no worker can ever SELECT.
                if crate::types::MailboxName::from_account_id(&folder.0).is_err() {
                    outcomes.push_failed(item, invalid_folder_name_error());
                    continue;
                }
                // Zero push capacity (`pool_cap = 1`) refuses everything.
                // Otherwise NOTIFY folds every folder onto one session, and
                // a folder already covered costs no new session either; only
                // a new distinct folder consumes a slot. A refusal is per
                // scope and never fails a sibling.
                let admitted = account.push.push_capacity > 0
                    && (account.push.supports_notify
                        || covered.contains(&folder.0)
                        || covered.len() < account.push.push_capacity);
                // Accepted residual, not an open defect: on a NOTIFY server
                // this `Succeeded` is settled here, before the first dial,
                // and the whole scope list rides on a `NOTIFY SET` that the
                // server can still reject at runtime. When it does, coverage
                // collapses to the one SELECTed mailbox while these outcomes
                // already said "pushed", and nothing can take them back -
                // `BatchOutcome` is returned by value from this call and the
                // `Account` push surface has no per-scope demotion event.
                // Retracting it would need a new event on that shared
                // `bifrost-types` surface (a decision no consumer has asked
                // for), so the collapse is instead degraded rather than
                // silenced: see `signal_notify_coverage_loss` below, which
                // emits an account-wide coarse invalidation on every round
                // whose registration did not take. Re-raise only with a
                // consumer that needs the demotion typed.
                if admitted {
                    covered.insert(folder.0.clone());
                    accepted.insert(scope.clone());
                    outcomes.push_succeeded(item, scope);
                } else {
                    outcomes.push_failed(item, idle_budget_error());
                }
            }
            if !accepted.is_empty() {
                subscriptions
                    .by_handle
                    .insert(handle.0.clone(), accepted.clone());
                subscriptions.version = subscriptions.version.wrapping_add(1);
            }
            accepted
        };
        account.push.bump_generation();
        let outcomes = outcomes.finalize(&expected).map_err(|error| {
            account_error_with(
                crate::Error::Internal(error.to_string()),
                ImapErrorContext::operation(AccountOperation::PushSubscribe),
            )
        })?;
        // No handle when nothing was accepted: a handle bifrost-sync records
        // would claim coverage this account is not providing, and there is
        // nothing for the matching unsubscribe to tear down.
        bifrost_types::PushSubscription::new((!accepted.is_empty()).then_some(handle), outcomes)
            .map_err(|failure| {
                account_error_with(
                    crate::Error::Internal(format!(
                        "IMAP push subscription invariant failed: {failure}"
                    )),
                    ImapErrorContext::operation(AccountOperation::PushSubscribe),
                )
            })
    })
}

/// One rejected scope in the `push_subscribe` failed lane.
///
/// Both refusals are `Unsupported(PushSubscribe)`: neither is a whole-request
/// fault (`Err` stays reserved for that), neither is retryable as issued, and
/// a rejected scope must not fail its siblings. They are distinguished by the
/// diagnostic text, not the kind, because a capacity refusal and a
/// scope-shape refusal need different operator action even though they carry
/// the same recovery class - bifrost-sync keeps polling either way.
fn rejected_scope_error(detail: &'static str) -> AccountError {
    AccountError::unsupported(
        AccountOperation::PushSubscribe,
        bifrost_types::Protocol::Imap,
    )
    .into_builder()
    .text(bifrost_types::DiagnosticText::user_safe(detail))
    .try_build()
    .expect("valid push scope error")
}

/// IMAP push is folder-granular: IDLE and `NOTIFY SET` both name mailboxes,
/// so an account-wide or other non-folder scope has nothing to register.
fn unsupported_push_scope_error() -> AccountError {
    rejected_scope_error("IMAP push covers folder scopes only")
}

/// The account has filled its push capacity with distinct folders, and the
/// server has no NOTIFY to fold another one onto an existing session. The
/// scope is refused rather than silently dropped, so bifrost-sync keeps it in
/// the polling lane instead of believing it is pushed.
fn idle_budget_error() -> AccountError {
    rejected_scope_error(
        "IMAP push session budget exhausted within pool_cap; folder remains poll-only",
    )
}

/// The scope names a folder that is not a sendable IMAP mailbox name
/// (`MailboxName::new` rejects NUL, CR, and LF). No IDLE worker could ever
/// SELECT it, so admitting it would misreport it as pushed while burning a
/// budget slot on nothing.
fn invalid_folder_name_error() -> AccountError {
    rejected_scope_error("IMAP push scope names an invalid mailbox name")
}

pub(crate) fn push_unsubscribe(
    account: ImapAccount,
    handle: SubscriptionHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let removed = {
            let mut scopes = account
                .push
                .scopes
                .lock()
                .expect("push scopes lock poisoned");
            let removed = scopes.by_handle.remove(&handle.0).is_some();
            if removed {
                scopes.version = scopes.version.wrapping_add(1);
            }
            removed
        };
        if removed {
            // Wake every live session to re-choose its assignment. Sessions
            // are engine-driven and lane-leased, so an unsubscribe followed
            // by a subscribe only re-chooses; it can never start a second
            // session on a lane or leave none.
            account.push.bump_generation();
        }
        Ok(())
    })
}

/// The capacity phase of a worker's session: a push slot, then a fresh pool
/// session. The slot is handed to the pool as an extra, so it lives in the
/// new driver's exit slot and comes back only when that transport is gone -
/// a retired session's LOGOUT tail keeps holding it. `waited()` covers a
/// wait in either step.
async fn reserve_push_session(
    account: &ImapAccount,
) -> Result<super::pool::FreshReservation, crate::Error> {
    let slots = std::sync::Arc::clone(&account.push.push_slots);
    let (slot, waited) = match std::sync::Arc::clone(&slots).try_acquire_owned() {
        Ok(slot) => (slot, false),
        Err(tokio::sync::TryAcquireError::Closed) => return Err(crate::Error::closed()),
        Err(tokio::sync::TryAcquireError::NoPermits) => (
            slots
                .acquire_owned()
                .await
                .map_err(|_| crate::Error::closed())?,
            true,
        ),
    };
    Ok(account
        .pool
        .reserve_fresh(vec![Box::new(slot)])
        .await?
        .with_prior_wait(waited))
}

fn classify(error: crate::Error, folder: &crate::types::MailboxName) -> AccountError {
    account_error_with(
        error,
        ImapErrorContext::operation(AccountOperation::PushStream).with_folder_scope(folder),
    )
}

fn exclude(account: &ImapAccount, folder: &crate::types::MailboxName, version: u64) {
    let mut scopes = account
        .push
        .scopes
        .lock()
        .expect("push scopes lock poisoned");
    if scopes.version == version {
        scopes.exclusions.insert(folder.account_id(), version);
    }
    drop(scopes);
    account.push.bump_generation();
}

fn select_excludes(error: &AccountError, folder: &crate::types::MailboxName) -> bool {
    error.recovery().is_terminal()
        || matches!(error.recovery(), bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::DisableScope(scope)) if *scope == folder_scope(folder))
}

fn notify_failure(error: crate::Error, folder: &crate::types::MailboxName) -> (AccountError, bool) {
    let refused = matches!(error, crate::Error::No { .. } | crate::Error::Bad { .. });
    let error = classify(error, folder);
    let degrade = refused
        && error.recovery().is_terminal()
        && matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(_)
                | bifrost_types::AccountErrorKind::Unsupported(_)
                | bifrost_types::AccountErrorKind::Server(
                    bifrost_types::ServerErrorKind::Error { .. }
                )
        );
    (error, degrade)
}

pub(crate) fn push_stream(
    account: ImapAccount,
    lane: bifrost_types::PushLane,
    stop: bifrost_types::SessionStop,
) -> AccountStream<bifrost_types::PushSessionEvent> {
    use bifrost_types::{PushSessionEnd as End, PushSessionEvent as E};
    Box::pin(async_stream::stream! {
        let Some(lease) = account.push.leases.get(usize::from(lane.0)) else {
            yield E::Failed(bifrost_types::push_lane_contract_error(bifrost_types::Protocol::Imap, lane, "not a lane of this account")); return;
        };
        let Some(_lease) = lease.try_claim() else {
            yield E::Failed(bifrost_types::push_lane_contract_error(bifrost_types::Protocol::Imap, lane, "already driven")); return;
        };
        let slot = usize::from(lane.0);
        let mut assignment = account.push.assignment.subscribe();
        'assignment: loop {
            assignment.borrow_and_update();
            let Some((mut folder, _)) = choose_assignment(&account, slot) else {
                tokio::select! { () = stop.stopped() => break, () = account.shutdown.cancelled() => break, _ = assignment.changed() => {} }
                continue;
            };
            let reservation = reserve_push_session(&account);
            tokio::pin!(reservation);
            let reserved = loop {
                tokio::select! {
                    biased;
                    () = stop.stopped() => break 'assignment,
                    () = account.shutdown.cancelled() => break 'assignment,
                    _ = assignment.changed() => {
                        match choose_assignment(&account, slot) { Some((current, _)) => folder = current, None => continue 'assignment }
                    }
                    result = &mut reservation => break result,
                }
            };
            let waited = reserved.as_ref().is_ok_and(super::pool::FreshReservation::waited);
            let dialed = match reserved {
                Ok(reserved) => tokio::select! { () = stop.stopped() => break, () = account.shutdown.cancelled() => break, result = reserved.dial() => result },
                Err(e) => Err(e),
            };
            let conn = match dialed { Ok(conn) => conn, Err(e) => { yield E::Failed(classify(e, &folder)); return; } };
            assignment.borrow_and_update();
            let Some((folder, version)) = choose_assignment(&account, slot) else { drop(conn); continue; };
            let selected = tokio::select! {
                () = stop.stopped() => break,
                () = account.shutdown.cancelled() => break,
                result = conn.select(&folder, account.command_timeout()) => result,
            };
            let selected = match selected {
                Ok(selected) => selected,
                Err(e) => {
                    drop(conn);
                    let e = classify(e, &folder);
                    if select_excludes(&e, &folder) { exclude(&account, &folder, version); yield E::UnitFailed(e); yield E::Ended(End::Reconfigure); }
                    else { yield E::Failed(e); }
                    return;
                }
            };
            let uidvalidity = selected.uid_validity;
            let mut first = true;
            loop {
                let watched = { let scopes = account.push.scopes.lock().expect("push scopes lock poisoned"); subscribed_idle_folders(&scopes.by_handle) };
                let notify = tokio::select! { () = stop.stopped() => break 'assignment, () = account.shutdown.cancelled() => break 'assignment, result = register_notify(&conn, &folder, &watched, account.command_timeout()) => result };
                let registered = match notify {
                    Ok(registered) => registered,
                    Err(e) => {
                        let (e, degrade) = notify_failure(e, &folder);
                        if e.recovery().is_retryable() || e.recovery().requires_reconciliation() { yield E::UnitFailed(e); }
                        else if degrade { yield E::Warning(bifrost_types::Warning::support_only(bifrost_types::WarningKind::Other, format!("NOTIFY SET refused: {}", e.message_key()))); }
                        else { drop(conn); yield E::Failed(e); return; }
                        false
                    }
                };
                if let Some(event) = signal_notify_coverage_loss(&account.push, registered) { yield event; }
                if first {
                    if waited {
                        for event in signal_coverage_established(&folder, &watched, registered) { yield event; }
                    }
                    yield E::Established;
                    first = false;
                }
                loop {
                    let (result, changed, stopped) = {
                    let round_cancel = CancellationToken::new();
                    let round = conn.idle(account.config.idle_timeout, account.command_timeout(), round_cancel.clone());
                    tokio::pin!(round);
                    tokio::select! {
                        result = &mut round => (result, false, false),
                        () = stop.stopped() => { round_cancel.cancel(); (round.await, false, true) },
                        () = account.shutdown.cancelled() => { round_cancel.cancel(); (round.await, false, true) },
                        _ = assignment.changed() => { round_cancel.cancel(); (round.await, true, false) },
                    }
                    };
                    if stopped {
                        let _ = tokio::time::timeout(account.command_timeout(), conn.logout()).await;
                        drop(conn);
                        yield E::Ended(End::Stopped); return;
                    }
                    let event = match result { Ok(event) => event, Err(e) => { drop(conn); yield E::Failed(classify(e, &folder)); return; } };
                    if absorb_idle_event(&account, &folder, uidvalidity, &event).is_err() { yield invalidated(HintPayload::Unknown); }
                    if let Some(end) = idle_event_ending(&event, &folder) { drop(conn); yield end; return; }
                    if changed {
                        assignment.borrow_and_update();
                        let chosen = choose_assignment(&account, slot);
                        if !matches!(resubscribe_action(&folder, chosen.as_ref().map(|(f, _)| f)), ResubscribeAction::Reidle) {
                            drop(conn); yield signal_idle_interrupt_loss(); yield E::Ended(End::Reconfigure); return;
                        }
                        if let Some(event) = map_idle_event(event, &folder) { yield event; }
                        break;
                    }
                    yield E::Healthy;
                    if let Some(event) = map_idle_event(event, &folder) { yield event; }
                }
            }
        }
        yield E::Ended(End::Stopped);
    })
}

/// What an IDLE round cancelled by a subscription change does next.
#[derive(Debug, PartialEq, Eq)]
enum ResubscribeAction {
    /// The new scope set assigns this slot the same mailbox: keep the
    /// connection and issue another IDLE on it.
    Reidle,
    /// A different mailbox, or none at all: release the connection and let
    /// the outer loop re-dial for the new assignment.
    Redial,
}

/// A subscribe/unsubscribe cancels the in-flight IDLE round so the folder
/// choice can be re-evaluated - but re-evaluating it very often yields the
/// same mailbox (subscribing a SECOND folder does not move slot 0). Dialing
/// a brand-new session for the assignment this connection already serves
/// costs a full connect+auth+SELECT on every subscription change, which is
/// exactly what providers enforcing per-user connection rate limits punish,
/// and it forces a coarse `Unknown` invalidation for a window a same-folder
/// re-IDLE on the same connection never loses.
///
/// The pure decision is pinned. The pool connector seam also makes the
/// re-IDLE and NOTIFY-collapse wiring hermetically drivable; those additional
/// wiring tests remain a separate work item.
fn resubscribe_action(
    current: &crate::types::MailboxName,
    chosen: Option<&crate::types::MailboxName>,
) -> ResubscribeAction {
    match chosen {
        Some(chosen) if chosen == current => ResubscribeAction::Reidle,
        _ => ResubscribeAction::Redial,
    }
}

fn choose_assignment(
    account: &ImapAccount,
    slot: usize,
) -> Option<(crate::types::MailboxName, u64)> {
    let mut scopes = account
        .push
        .scopes
        .lock()
        .expect("push scopes lock poisoned");
    let version = scopes.version;
    scopes.exclusions.retain(|_, v| *v == version);
    if scopes.by_handle.is_empty() {
        return None;
    }
    let mut folders = subscribed_idle_folders(&scopes.by_handle);
    if folders.is_empty()
        && let Some(inbox) = first_inbox_folder(account.folders.entries())
    {
        folders.push(inbox);
    }
    folders
        .into_iter()
        .filter(|f| !scopes.exclusions.contains_key(&f.account_id()))
        .nth(slot)
        .map(|f| (f, version))
}

fn first_inbox_folder(
    entries: Vec<std::sync::Arc<super::folder_registry::FolderEntry>>,
) -> Option<crate::types::MailboxName> {
    entries
        .into_iter()
        .filter(|entry| entry.name.as_str().eq_ignore_ascii_case("INBOX"))
        // Registry order is unspecified. Prefer a stable mailbox identity
        // when multiple entries match the case-insensitive INBOX role.
        .min_by(|left, right| left.name.cmp_account_order(&right.name))
        .map(|entry| entry.name.clone())
}

/// Every subscribed folder scope, sorted and deduplicated.
///
/// This is both the admission ledger and the worker assignment. A
/// HashMap/HashSet backs subscriptions, so the sort is what keeps iteration
/// order from deciding which mailbox a slot watches. Worker `n` takes the
/// nth entry; on a NOTIFY server only slot 0 runs and the rest of the list
/// rides on `NOTIFY SET` from that one session. Admission caps the list at
/// carved-out push capacity on a non-NOTIFY server, so every accepted folder
/// always has a slot - the assignment of folder to slot index shifts when
/// the set changes, the coverage of the set does not.
fn subscribed_idle_folders(
    scopes: &HashMap<String, HashSet<CursorScope>>,
) -> Vec<crate::types::MailboxName> {
    let mut folders = scopes
        .values()
        .flat_map(|scopes| scopes.iter())
        .filter_map(|scope| match scope {
            CursorScope::Folder(folder) => {
                crate::types::MailboxName::from_account_id(&folder.0).ok()
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    folders.sort_by_key(crate::types::MailboxName::account_id);
    folders.dedup_by(|left, right| left == right);
    folders
}

/// The `NOTIFY SET` registration for one IDLE round: message events on the
/// selected mailbox, the same events on the other watched mailboxes.
///
/// RFC 5465 Section 5.1: `FlagChange` is only legal alongside `MessageNew`
/// and `MessageExpunge`. Fetch attributes are legal only on the `selected`
/// filter (Section 5.2) and we ask for none - the account layer refetches
/// through the changes stream, so a payload here would be wasted bandwidth
/// on every flag flip.
fn notify_params(others: Vec<crate::types::MailboxArgument>) -> crate::types::NotifySetParams {
    let events = || {
        vec![
            crate::types::NotifyEvent::MessageNew {
                fetch_attrs: Vec::new(),
            },
            crate::types::NotifyEvent::MessageExpunge,
            crate::types::NotifyEvent::FlagChange,
        ]
    };
    crate::types::NotifySetParams::new(
        vec![
            crate::types::NotifyEventGroup::new(crate::types::MailboxFilter::Selected, events()),
            crate::types::NotifyEventGroup::new(
                crate::types::MailboxFilter::Mailboxes(others),
                events(),
            ),
        ],
        false,
    )
}

/// Extend this IDLE session's coverage from the one SELECTed mailbox to
/// every subscribed folder, via `NOTIFY SET` (RFC 5465 Section 3).
///
/// IDLE alone reports only the selected mailbox, so an account subscribing
/// ten folder scopes would get push for one of them and silence for the
/// other nine. NOTIFY fixes that on one connection: message events on the
/// selected mailbox keep arriving as EXISTS/EXPUNGE/FETCH, and the same
/// events on the other watched mailboxes arrive as STATUS responses
/// (RFC 5465 Section 4), which `map_idle_event` already turns into a
/// per-folder invalidation.
///
/// `status` is left off deliberately: the initial STATUS snapshot would
/// invalidate every watched scope immediately after subscribe, right when
/// discovery and inventory have just run. A push session that waited for
/// capacity publishes per-folder invalidations itself
/// (`signal_coverage_established`), which this decision does not forbid
/// because it fires only after a wait.
///
/// Returns whether every watched folder is covered. A server without
/// NOTIFY leaves the loop watching only the selected mailbox. A rejected
/// registration returns the error so the caller can forward non-Retry
/// recovery; a retryable failure retains selected-only coverage and feeds
/// that answer to `signal_notify_coverage_loss`, which turns a collapse on
/// a NOTIFY-advertising account into a coarse invalidation rather than
/// silence, because the subscribe outcome already reported those folders
/// pushed and cannot be retracted.
async fn register_notify(
    conn: &crate::connection::ImapConnection,
    selected: &crate::types::MailboxName,
    watched: &[crate::types::MailboxName],
    timeout: std::time::Duration,
) -> Result<bool, crate::Error> {
    let others: Vec<crate::types::MailboxArgument> = watched
        .iter()
        .filter(|folder| *folder != selected)
        .map(crate::types::MailboxArgument::from)
        .collect();
    if others.is_empty() {
        return Ok(true);
    }
    if !conn.server_profile().supports_notify() {
        tracing::warn!(
            watched = others.len(),
            selected = selected.as_str(),
            "server does not advertise NOTIFY (RFC 5465); IDLE push covers only the \
             selected mailbox and the other subscribed folders will not be pushed"
        );
        return Ok(false);
    }
    match conn.notify_set(notify_params(others), timeout).await {
        Ok(()) => Ok(true),
        Err(err) => {
            tracing::warn!(
                error = %err,
                selected = selected.as_str(),
                "NOTIFY SET rejected; IDLE push covers only the selected mailbox"
            );
            Err(err)
        }
    }
}

fn absorb_idle_event(
    account: &ImapAccount,
    selected: &crate::types::MailboxName,
    uidvalidity: Option<u32>,
    event: &IdleEvent,
) -> Result<(), crate::Error> {
    match event {
        IdleEvent::Fetch(fetch) => {
            if let (Some(uidvalidity), Some(uid), Some(modseq)) =
                (uidvalidity, fetch.uid, fetch.mod_seq)
            {
                account
                    .folders
                    .record_modseq(selected, uidvalidity, uid, modseq)?;
            }
        }
        IdleEvent::Vanished { uids, .. } => {
            if let Some(uidvalidity) = uidvalidity {
                account
                    .folders
                    .clear_modseq_ranges(selected, uidvalidity, uids);
            }
        }
        IdleEvent::MailboxEvent(info) => account.folders.apply_mailbox_event(info.clone()),
        _ => {}
    }
    Ok(())
}

/// The terminal item for an IDLE round whose event leaves the connection
/// unusable, or `None` when the session carries on.
///
/// A server-initiated tagged completion carries no error and ends `Lost`.
///
/// On the live path a BYE never arrives here. Every driver read loop that
/// serves IDLE (the `+` grant wait, the loop, the post-DONE drain) runs the
/// shared untagged prologue, which turns an untagged BYE into `Error::Bye`
/// before any event is emitted, so the session sees it through `idle()`'s
/// error arm and ends `Failed` there. The `Bye` arm is defensive: `idle()`
/// still maps a BYE typed event to `IdleEvent::Bye`, and should the driver's
/// short-circuit ever move, this arm keeps the session contract for it -
/// classified like the error path (`PushStream`, folder scope), ended
/// `Failed` whatever its recovery, and never an exclusion, because a BYE is
/// connection-level and does not say which folder failed.
fn idle_event_ending(
    event: &IdleEvent,
    folder: &crate::types::MailboxName,
) -> Option<bifrost_types::PushSessionEvent> {
    match event {
        IdleEvent::Bye { code, text } => Some(bifrost_types::PushSessionEvent::Failed(classify(
            crate::Error::Bye {
                code: code.clone(),
                text: text.clone(),
                attempt: None,
            },
            folder,
        ))),
        IdleEvent::ServerTerminated => Some(bifrost_types::PushSessionEvent::Ended(
            bifrost_types::PushSessionEnd::Lost,
        )),
        _ => None,
    }
}

pub(crate) fn map_idle_event(
    event: IdleEvent,
    selected: &crate::types::MailboxName,
) -> Option<bifrost_types::PushSessionEvent> {
    match event {
        IdleEvent::Exists(_)
        | IdleEvent::Expunge(_)
        | IdleEvent::Vanished { .. }
        | IdleEvent::Fetch(_)
        | IdleEvent::Recent(_) => Some(invalidated(HintPayload::SpecificCursorScope(
            folder_scope(selected),
        ))),
        IdleEvent::MailboxStatus { mailbox, .. } => Some(invalidated(
            HintPayload::SpecificCursorScope(folder_scope(&mailbox)),
        )),
        IdleEvent::MailboxEvent(info) => Some(invalidated(HintPayload::SpecificCursorScope(
            folder_scope(&info.name),
        ))),
        IdleEvent::MetadataChange { .. }
        | IdleEvent::SearchUpdate(_)
        | IdleEvent::StatusUpdate { .. }
        | IdleEvent::NotificationOverflow { .. }
        | IdleEvent::Alert(_)
        // Never mapped: the driver delivers a BYE during IDLE as `idle()`'s
        // error, and a `Bye` event, were one ever emitted, ends the session in
        // `idle_event_ending` before mapping. The arm exists because the
        // match must be total, and a coarse invalidation is the safe answer.
        | IdleEvent::Bye { .. }
        | IdleEvent::ExtensionEvent(_) => Some(invalidated(HintPayload::Unknown)),
        IdleEvent::Timeout | IdleEvent::Cancelled | IdleEvent::ServerTerminated => None,
    }
}

/// A round cancelled by a subscription change that moved this slot's
/// assignment: the window between the cancel and the next session's
/// coverage is unknown, so the whole account is invalidated.
fn signal_idle_interrupt_loss() -> bifrost_types::PushSessionEvent {
    invalidated(HintPayload::Unknown)
}

/// A NOTIFY-advertising account whose registration did not take this round
/// covers only the selected mailbox while its subscribe outcome reported
/// every folder pushed, so the collapse is degraded into a coarse
/// invalidation rather than silence.
fn signal_notify_coverage_loss(
    push: &PushState,
    registered: bool,
) -> Option<bifrost_types::PushSessionEvent> {
    (push.supports_notify && !registered).then(|| invalidated(HintPayload::Unknown))
}

/// A session that waited for capacity starts covering late: invalidate the
/// selected folder and, when NOTIFY registered, every other watched folder.
fn signal_coverage_established(
    selected: &crate::types::MailboxName,
    watched: &[crate::types::MailboxName],
    registered: bool,
) -> Vec<bifrost_types::PushSessionEvent> {
    std::iter::once(selected)
        .chain(watched.iter().filter(|f| registered && *f != selected))
        .map(|f| invalidated(HintPayload::SpecificCursorScope(folder_scope(f))))
        .collect()
}

fn invalidated(payload: HintPayload) -> bifrost_types::PushSessionEvent {
    bifrost_types::PushSessionEvent::Invalidated(InvalidationHint {
        source: PushSource::ImapNotify,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbox_fallback_choice_is_stable_across_registry_order() {
        let upper = crate::types::MailboxName::new("INBOX").expect("valid mailbox");
        let lower = crate::types::MailboxName::new("inbox").expect("valid mailbox");
        let entry = |name| {
            std::sync::Arc::new(super::super::folder_registry::FolderEntry::from_mailbox(
                crate::types::MailboxInfo {
                    name,
                    ..Default::default()
                },
            ))
        };

        for names in [
            [lower.clone(), upper.clone()],
            [upper.clone(), lower.clone()],
        ] {
            let selected = first_inbox_folder(names.into_iter().map(entry).collect())
                .expect("an INBOX folder");
            assert_eq!(selected, upper);
        }
    }

    /// The unchanged-assignment case must NOT redial. Every subscribe and
    /// unsubscribe cancels the in-flight round, and the common shape is a
    /// scope set that grows or shrinks somewhere other than this slot, so
    /// treating every interrupt as a redial burns a connect+auth+SELECT per
    /// subscription change on providers that rate-limit connections - and
    /// throws away a window that a same-folder re-IDLE keeps.
    #[test]
    fn a_resubscribe_keeps_the_connection_when_the_folder_is_unchanged() {
        let inbox = crate::types::MailboxName::new("INBOX").expect("valid mailbox");
        let same = crate::types::MailboxName::new("INBOX").expect("valid mailbox");
        let other = crate::types::MailboxName::new("Archive").expect("valid mailbox");

        assert_eq!(
            resubscribe_action(&inbox, Some(&same)),
            ResubscribeAction::Reidle,
            "the same mailbox must be re-IDLEd on the connection already holding it",
        );
        assert_eq!(
            resubscribe_action(&inbox, Some(&other)),
            ResubscribeAction::Redial,
            "a different assignment needs a session selected on that mailbox",
        );
        assert_eq!(
            resubscribe_action(&inbox, None),
            ResubscribeAction::Redial,
            "no assignment at all releases the connection",
        );
    }

    #[test]
    fn subscribed_folder_choice_is_deterministic() {
        let mut scopes = HashMap::new();
        scopes.insert(
            "later".to_owned(),
            HashSet::from([CursorScope::Folder(bifrost_types::FolderId(
                "Zebra".to_owned(),
            ))]),
        );
        scopes.insert(
            "first".to_owned(),
            HashSet::from([CursorScope::Folder(bifrost_types::FolderId(
                "Archive".to_owned(),
            ))]),
        );

        assert_eq!(
            subscribed_idle_folders(&scopes)
                .into_iter()
                .next()
                .expect("a subscribed folder")
                .as_str(),
            "Archive"
        );
    }

    // The SELECTed mailbox is one of these; the rest ride on NOTIFY. Two
    // handles subscribing the same folder must not register it twice.
    #[test]
    fn every_subscribed_folder_is_watched_once_in_a_stable_order() {
        let mut scopes = HashMap::new();
        scopes.insert(
            "later".to_owned(),
            HashSet::from([
                CursorScope::Folder(bifrost_types::FolderId("Zebra".to_owned())),
                CursorScope::Folder(bifrost_types::FolderId("Archive".to_owned())),
            ]),
        );
        scopes.insert(
            "first".to_owned(),
            HashSet::from([
                CursorScope::Folder(bifrost_types::FolderId("Archive".to_owned())),
                CursorScope::Account,
            ]),
        );

        let watched: Vec<String> = subscribed_idle_folders(&scopes)
            .iter()
            .map(crate::types::MailboxName::account_id)
            .collect();
        assert_eq!(
            watched,
            vec![String::from("Archive"), String::from("Zebra")]
        );
    }

    // RFC 5465 Section 5.1 makes FlagChange illegal without MessageNew and
    // MessageExpunge, and Section 5.2 makes fetch attributes illegal off
    // the `selected` filter. A registration that breaks either is rejected
    // with [BADEVENT] and the account silently loses push for every folder
    // but one.
    #[test]
    fn notify_registration_is_legal_for_both_filters() {
        let params = notify_params(vec![
            crate::types::MailboxArgument::from("Archive"),
            crate::types::MailboxArgument::from("Zebra"),
        ]);
        assert!(
            !params.status,
            "the initial STATUS snapshot would invalidate every scope right after subscribe",
        );
        assert_eq!(params.event_groups.len(), 2);
        assert_eq!(
            params.event_groups[0].filter,
            crate::types::MailboxFilter::Selected
        );
        assert_eq!(
            params.event_groups[1].filter,
            crate::types::MailboxFilter::Mailboxes(vec![
                crate::types::MailboxArgument::from("Archive"),
                crate::types::MailboxArgument::from("Zebra"),
            ])
        );
        for group in &params.event_groups {
            assert!(
                group
                    .events
                    .iter()
                    .any(|event| matches!(event, crate::types::NotifyEvent::MessageNew { .. }))
            );
            assert!(
                group
                    .events
                    .iter()
                    .any(|event| matches!(event, crate::types::NotifyEvent::MessageExpunge)),
                "FlagChange requires MessageExpunge alongside it",
            );
            assert!(
                group.events.iter().all(|event| !matches!(
                    event,
                    crate::types::NotifyEvent::MessageNew { fetch_attrs } if !fetch_attrs.is_empty()
                )),
                "fetch attributes are only legal on the selected filter, and we want none",
            );
        }
    }
}

#[cfg(test)]
mod session_classification_tests {
    use super::*;
    #[test]
    fn idle_failures_split_into_folder_local_and_session_failures() {
        let folder = crate::types::MailboxName::new("gone").unwrap();
        let nonexistent = classify(
            crate::Error::No {
                code: Some(crate::types::ResponseCode::NonExistent),
                text: "gone".into(),
                attempt: None,
            },
            &folder,
        );
        assert!(select_excludes(&nonexistent, &folder));
        let transport = classify(
            crate::Error::Io {
                source: std::sync::Arc::new(std::io::Error::from(
                    std::io::ErrorKind::ConnectionReset,
                )),
                attempt: None,
            },
            &folder,
        );
        assert!(!select_excludes(&transport, &folder));
    }
    #[test]
    fn a_refused_notify_registration_warns_and_keeps_selected_coverage() {
        let folder = crate::types::MailboxName::new("INBOX").unwrap();
        let (_, degrade) = notify_failure(
            crate::Error::No {
                code: None,
                text: "unsupported filter".into(),
                attempt: None,
            },
            &folder,
        );
        assert!(degrade);
        let (_, degrade) = notify_failure(
            crate::Error::No {
                code: Some(crate::types::ResponseCode::NoPerm),
                text: "denied".into(),
                attempt: None,
            },
            &folder,
        );
        assert!(!degrade);
    }
    /// The defensive `Bye` arm, which the live driver never reaches (it
    /// delivers a BYE as `idle()`'s error; `a_bye_never_excludes_the_selected_folder`
    /// pins that path). Were a `Bye` event ever emitted, it must end the
    /// session `Failed` with the error path's classification, even when that
    /// classification is terminal and would exclude the folder on SELECT.
    #[test]
    fn a_bye_event_ends_the_session_failed_and_never_excludes() {
        use bifrost_types::{PushSessionEnd as End, PushSessionEvent as E};
        let folder = crate::types::MailboxName::new("INBOX").unwrap();
        let denied = idle_event_ending(
            &IdleEvent::Bye {
                code: Some(crate::types::ResponseCode::NoPerm),
                text: "denied".into(),
            },
            &folder,
        );
        let Some(E::Failed(e)) = denied else {
            panic!("a BYE event ends the session Failed, got {denied:?}");
        };
        assert!(e.recovery().is_terminal());
        assert!(
            select_excludes(&e, &folder),
            "the classification alone would exclude; the arm must not"
        );
        let plain = idle_event_ending(
            &IdleEvent::Bye {
                code: None,
                text: "logging out".into(),
            },
            &folder,
        );
        assert!(
            matches!(&plain, Some(E::Failed(e)) if matches!(e.kind(), bifrost_types::AccountErrorKind::Server(bifrost_types::ServerErrorKind::Unavailable))),
            "a plain BYE classifies as on the error path, got {plain:?}"
        );
        assert!(matches!(
            idle_event_ending(&IdleEvent::ServerTerminated, &folder),
            Some(E::Ended(End::Lost))
        ));
        for event in [
            IdleEvent::Exists(2),
            IdleEvent::Timeout,
            IdleEvent::Cancelled,
        ] {
            assert!(idle_event_ending(&event, &folder).is_none(), "{event:?}");
        }
        assert!(matches!(
            map_idle_event(
                IdleEvent::Bye {
                    code: None,
                    text: "bye".into()
                },
                &folder
            ),
            Some(E::Invalidated(InvalidationHint {
                payload: HintPayload::Unknown,
                ..
            }))
        ));
    }
}

#[cfg(test)]
mod assignment_tests {
    use super::*;
    use bifrost_types::Account;
    async fn account() -> (ImapAccount, tokio::io::DuplexStream) {
        let (account, peer, _) = super::super::scripted_tests::session_account(2, 0, false).await;
        account
            .push_subscribe(&[CursorScope::Folder(bifrost_types::FolderId("INBOX".into()))])
            .await
            .unwrap();
        (account, peer)
    }
    #[tokio::test]
    async fn an_exclusion_from_a_stale_subscription_version_is_discarded() {
        let (account, _peer) = account().await;
        let (folder, version) = choose_assignment(&account, 0).unwrap();
        account
            .push_subscribe(&[folder_scope(&folder)])
            .await
            .unwrap();
        exclude(&account, &folder, version);
        assert!(
            !account
                .push
                .scopes
                .lock()
                .unwrap()
                .exclusions
                .contains_key(&folder.account_id())
        );
        assert!(choose_assignment(&account, 0).is_some());
    }
    #[tokio::test]
    async fn an_all_excluded_lane_parks_until_the_subscription_set_changes() {
        use futures::StreamExt;
        let (account, _peer) = account().await;
        let (folder, version) = choose_assignment(&account, 0).unwrap();
        exclude(&account, &folder, version);
        assert!(choose_assignment(&account, 0).is_none());
        let (stopper, stop) = bifrost_types::SessionStop::pair();
        let mut session = push_stream(account.clone(), bifrost_types::PushLane(0), stop);
        let mut next = Box::pin(session.next());
        assert!(futures::poll!(&mut next).is_pending());
        stopper.stop();
        assert!(matches!(
            next.await,
            Some(bifrost_types::PushSessionEvent::Ended(
                bifrost_types::PushSessionEnd::Stopped
            ))
        ));
        account
            .push_subscribe(&[folder_scope(&folder)])
            .await
            .unwrap();
        assert!(choose_assignment(&account, 0).is_some());
    }
    #[test]
    fn an_account_wide_directive_on_select_fails_the_session_without_excluding() {
        let folder = crate::types::MailboxName::new("INBOX").unwrap();
        let error = classify(
            crate::Error::No {
                code: Some(crate::types::ResponseCode::NoModSeq),
                text: "changed".into(),
                attempt: None,
            },
            &folder,
        );
        assert!(error.recovery().requires_engine_action());
        assert!(!select_excludes(&error, &folder));
    }
    #[test]
    fn a_scope_repair_on_select_does_not_exclude_the_folder() {
        let folder = crate::types::MailboxName::new("INBOX").unwrap();
        for code in [
            crate::types::ResponseCode::Closed,
            crate::types::ResponseCode::UidNotSticky,
        ] {
            let error = classify(
                crate::Error::No {
                    code: Some(code),
                    text: "changed".into(),
                    attempt: None,
                },
                &folder,
            );
            assert!(error.recovery().requires_engine_action());
            assert!(!select_excludes(&error, &folder));
        }
    }
    #[tokio::test]
    async fn lane_count_follows_notify_and_capacity() {
        for capacity in [0, 1, 3] {
            for notify in [false, true] {
                let push = PushState::new(notify, capacity);
                assert_eq!(
                    push.leases.len(),
                    if notify {
                        usize::from(capacity > 0)
                    } else {
                        capacity
                    }
                );
            }
        }
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use bifrost_types::PushSessionEvent as E;
    #[test]
    fn a_resubscribe_interrupted_idle_round_still_invalidates() {
        assert!(matches!(
            signal_idle_interrupt_loss(),
            E::Invalidated(InvalidationHint {
                payload: HintPayload::Unknown,
                ..
            })
        ));
    }
    #[test]
    fn a_rejected_notify_registration_degrades_instead_of_going_silent() {
        let push = PushState::new(true, 1);
        assert!(signal_notify_coverage_loss(&push, true).is_none());
        assert!(matches!(
            signal_notify_coverage_loss(&push, false),
            Some(E::Invalidated(InvalidationHint {
                payload: HintPayload::Unknown,
                ..
            }))
        ));
    }
    #[test]
    fn a_non_notify_account_does_not_invalidate_on_a_missing_registration() {
        assert!(signal_notify_coverage_loss(&PushState::new(false, 2), false).is_none());
    }
    #[test]
    fn coverage_establishment_names_each_covered_folder() {
        let selected = crate::types::MailboxName::new("INBOX").unwrap();
        let other = crate::types::MailboxName::new("Archive").unwrap();
        let watched = vec![selected.clone(), other.clone()];
        for registered in [false, true] {
            let events = signal_coverage_established(&selected, &watched, registered);
            assert_eq!(events.len(), if registered { 2 } else { 1 });
            for (event, folder) in events.into_iter().zip([&selected, &other]) {
                assert!(
                    matches!(event, E::Invalidated(InvalidationHint { payload: HintPayload::SpecificCursorScope(scope), .. }) if scope == folder_scope(folder))
                );
            }
        }
    }
}
