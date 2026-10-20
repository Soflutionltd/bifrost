use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFuture, AccountOperation,
    AccountStream, BatchItemId, BatchOutcomeBuilder, Cause, CursorScope, HintPayload,
    InvalidationHint, PushSource, RequestCause, SubscriptionHandle, WatchEvent,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::account::error::ImapErrorContext;
use crate::connection::IdleEvent;

use super::{ImapAccount, account_error_with, boxed_receiver_stream, folder_scope};

pub(crate) struct PushState {
    tx: broadcast::Sender<WatchEvent>,
    scopes: Mutex<HashMap<String, HashSet<CursorScope>>>,
    pub(super) task_cancel: Mutex<Vec<CancellationToken>>,
    supports_notify: bool,
    idle_budget: usize,
    /// Monotonic generation of the subscribed scope set. Every worker holds
    /// a receiver and re-evaluates `choose_idle_folder` when it moves, so a
    /// scope added after the workers parked is picked up without waiting for
    /// a connection to break.
    ///
    /// A `Notify` cannot serve this with more than one worker. `notify_one`
    /// wakes exactly one of them, which leaves the other slots parked on a
    /// stale assignment; `notify_waiters` wakes all of them but stores
    /// nothing, so a worker in the window between deciding it has no folder
    /// and awaiting the notification misses the wake and parks forever. A
    /// `watch` generation latches, so both readings are safe.
    resubscribe: tokio::sync::watch::Sender<u64>,
    next_id: AtomicU64,
}

impl PushState {
    pub(crate) fn new(supports_notify: bool, idle_budget: usize) -> Self {
        let (tx, _rx) = broadcast::channel(128);
        Self {
            tx,
            scopes: Mutex::new(HashMap::new()),
            task_cancel: Mutex::new(Vec::new()),
            supports_notify,
            idle_budget: idle_budget.max(1),
            resubscribe: tokio::sync::watch::Sender::new(0),
            next_id: AtomicU64::new(1),
        }
    }

    /// Publish that the subscribed scope set changed.
    fn bump_generation(&self) {
        self.resubscribe
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    pub(crate) fn stop(&self) {
        for cancel in self
            .task_cancel
            .lock()
            .expect("push task lock poisoned")
            .drain(..)
        {
            cancel.cancel();
        }
    }
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
            let mut covered: HashSet<String> = subscribed_idle_folders(&subscriptions)
                .into_iter()
                .map(|folder| folder.as_str().to_owned())
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
                if crate::types::MailboxName::new(folder.0.clone()).is_err() {
                    outcomes.push_failed(item, invalid_folder_name_error());
                    continue;
                }
                // NOTIFY folds every folder onto one session, so the budget
                // does not apply there. A folder already covered costs no
                // new session either; only a new distinct folder consumes a
                // slot. A refusal is per scope and never fails a sibling.
                let admitted = account.push.supports_notify
                    || covered.contains(&folder.0)
                    || covered.len() < account.push.idle_budget;
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
                subscriptions.insert(handle.0.clone(), accepted.clone());
            }
            accepted
        };
        if !accepted.is_empty()
            && let Err(error) = ensure_idle_tasks(account.clone())
        {
            account
                .push
                .scopes
                .lock()
                .expect("push scopes lock poisoned")
                .remove(&handle.0);
            return Err(account_error_with(
                error,
                ImapErrorContext::operation(AccountOperation::PushSubscribe),
            ));
        }
        // Nudge an already-running IDLE loop so a scope added after it
        // parked on another folder is reconsidered without waiting for the
        // current IDLE connection to break.
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
        Ok(bifrost_types::PushSubscription::new(
            (!accepted.is_empty()).then_some(handle),
            outcomes,
        ))
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
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe),
        Cause::Request(RequestCause::Unsupported {
            operation: AccountOperation::PushSubscribe,
        }),
    )
    .operation(AccountOperation::PushSubscribe)
    .protocol(bifrost_types::Protocol::Imap)
    .text(bifrost_types::DiagnosticText::user_safe(detail))
    .try_build()
    .expect("valid push scope error")
}

/// IMAP push is folder-granular: IDLE and `NOTIFY SET` both name mailboxes,
/// so an account-wide or other non-folder scope has nothing to register.
fn unsupported_push_scope_error() -> AccountError {
    rejected_scope_error("IMAP push covers folder scopes only")
}

/// The account is at `idle_connection_budget` distinct pushed folders and the
/// server has no NOTIFY to fold another one onto an existing session. The
/// scope is refused rather than silently dropped, so bifrost-sync keeps it in
/// the polling lane instead of believing it is pushed.
fn idle_budget_error() -> AccountError {
    rejected_scope_error("IMAP IDLE connection budget exhausted; folder remains poll-only")
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
            scopes.remove(&handle.0).is_some()
        };
        if removed {
            // The task is account-owned, not subscription-owned. It stays
            // alive while this account is open and waits when there are no
            // scopes, so an unsubscribe followed by a subscribe cannot race
            // a cancelling task into two IDLE loops (or no loop at all).
            account.push.bump_generation();
        }
        Ok(())
    })
}

pub(crate) fn push_stream(account: ImapAccount) -> AccountStream<WatchEvent> {
    let mut rx = account.push.tx.subscribe();
    let shutdown = account.shutdown.clone();
    let (tx, out) = tokio::sync::mpsc::channel(128);
    // Detached rather than `guarded_receiver_stream`, deliberately: this
    // forwarder holds no pool checkout and issues no IMAP command, only a
    // broadcast receiver, and it ends on account shutdown or on its next
    // failed send. The hazard the guard exists for - two drivers sharing one
    // account's connection - cannot arise from it.
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                result = rx.recv() => {
                    match result {
                        Ok(event) => {
                            if tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            let event = WatchEvent::Invalidated {
                                hint: InvalidationHint {
                                    source: PushSource::Coalesced,
                                    payload: HintPayload::Unknown,
                                },
                            };
                            if tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    });
    boxed_receiver_stream(out)
}

fn ensure_idle_tasks(account: ImapAccount) -> Result<(), crate::Error> {
    let mut guard = account
        .push
        .task_cancel
        .lock()
        .map_err(|_| crate::Error::Internal("push task lock poisoned".into()))?;
    if !guard.is_empty() {
        return Ok(());
    }
    let workers = if account.push.supports_notify {
        1
    } else {
        account.push.idle_budget
    };
    for slot in 0..workers {
        let cancel = CancellationToken::new();
        guard.push(cancel.clone());
        let worker_account = account.clone();
        tokio::spawn(async move {
            idle_loop(worker_account, cancel, slot).await;
        });
    }
    Ok(())
}

/// First wait after a failed dial or SELECT in the push loop.
const INITIAL_REDIAL_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);
/// Ceiling for the doubling ramp. A server that is down for an hour must not
/// be dialed 720 times by every account watching it.
const MAX_REDIAL_BACKOFF: std::time::Duration = std::time::Duration::from_secs(300);

/// Wait out the current backoff, then double it. Returns `false` if the loop
/// was cancelled or the account shut down while waiting - a flat sleep here
/// would hold shutdown hostage for the whole (now up to five minute) delay.
async fn sleep_backoff(
    account: &ImapAccount,
    cancel: &CancellationToken,
    backoff: &mut std::time::Duration,
) -> bool {
    let wait = *backoff;
    *backoff = (*backoff * 2).min(MAX_REDIAL_BACKOFF);
    tokio::select! {
        () = cancel.cancelled() => false,
        () = account.shutdown.cancelled() => false,
        () = tokio::time::sleep(wait) => true,
    }
}

async fn idle_loop(account: ImapAccount, cancel: CancellationToken, slot: usize) {
    // Track whether the consumer has seen a `Disconnected` since the last
    // `Reconnected`. The very first successful connect must NOT emit
    // `Reconnected` (there was no prior disconnect): the reconciler treats
    // `Reconnected` as a full account-wide `Unknown` reconcile, which is
    // spurious right after subscribe when discovery/inventory just ran.
    let mut was_disconnected = false;
    let mut backoff = INITIAL_REDIAL_BACKOFF;
    let mut resubscribe = account.push.resubscribe.subscribe();
    loop {
        if cancel.is_cancelled() || account.shutdown.is_cancelled() {
            break;
        }
        // Mark the current generation seen BEFORE choosing, so a scope set
        // that changes between the choice and the park below latches and
        // wakes this worker instead of being lost.
        resubscribe.mark_unchanged();
        let folder = choose_idle_folder(&account, slot);
        let Some(folder) = folder else {
            // Do not tear down the task merely because the last subscription
            // left. `ensure_idle_task` has one account-lifetime owner; a
            // subsequent subscription wakes this parked loop directly.
            tokio::select! {
                () = cancel.cancelled() => break,
                () = account.shutdown.cancelled() => break,
                changed = resubscribe.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
            continue;
        };
        let conn = match account.pool.dial_idle().await {
            Ok(conn) => conn,
            Err(_) => {
                if !was_disconnected {
                    let _ = account.push.tx.send(WatchEvent::Disconnected);
                }
                was_disconnected = true;
                if !sleep_backoff(&account, &cancel, &mut backoff).await {
                    break;
                }
                continue;
            }
        };
        let selected = match conn
            .select(folder.as_str(), account.command_timeout())
            .await
        {
            Ok(selected) => selected,
            Err(_) => {
                if !was_disconnected {
                    let _ = account.push.tx.send(WatchEvent::Disconnected);
                }
                was_disconnected = true;
                if !sleep_backoff(&account, &cancel, &mut backoff).await {
                    break;
                }
                continue;
            }
        };
        let uidvalidity = selected.uid_validity;
        let watched = {
            let scopes = account
                .push
                .scopes
                .lock()
                .expect("push scopes lock poisoned");
            subscribed_idle_folders(&scopes)
        };
        let registered = register_notify(&conn, &folder, &watched, account.command_timeout()).await;
        signal_notify_coverage_loss(&account.push, registered);
        if was_disconnected {
            let _ = account.push.tx.send(WatchEvent::Reconnected);
            was_disconnected = false;
        }
        // Set when the session dies in-IDLE (server BYE / termination / idle
        // error). The redial then waits out the backoff: a server that
        // accepts dial+auth+SELECT but kills IDLE at once must not produce a
        // zero-delay hot loop of full reconnect cycles. The backoff ramp
        // resets only on a completed IDLE round below, not on the SELECT
        // round trip, for the same reason.
        let mut session_died = false;
        loop {
            if cancel.is_cancelled() || account.shutdown.is_cancelled() {
                let _ = conn.logout().await;
                return;
            }
            // Each IDLE round gets a child cancel token so a resubscribe
            // notification (a new scope was added) can break this IDLE
            // cleanly; the outer loop then re-runs `choose_idle_folder`,
            // letting the newly-subscribed folder be picked up instead of
            // waiting for this connection to break.
            let round_cancel = cancel.child_token();
            let notify_cancel = round_cancel.clone();
            // The receiver was marked seen once, before `choose_idle_folder`
            // read the scope set this round is built on. It must NOT be
            // re-marked here: the dial, SELECT, and `NOTIFY SET` between the
            // choice and this point all await the network, and a generation
            // bump landing in that window is exactly what the latch exists
            // to preserve. Clearing it would leave this worker on a stale
            // assignment for a full `idle_timeout`. The clone keeps the
            // original's seen version, so a latched bump cancels the very
            // first round immediately and the outer loop re-chooses.
            let mut resubscribe_for_round = resubscribe.clone();
            let nudge = tokio::spawn(async move {
                tokio::select! {
                    _ = resubscribe_for_round.changed() => notify_cancel.cancel(),
                    () = notify_cancel.cancelled() => {}
                }
            });
            let idle_result = conn
                .idle(
                    account.config.idle_timeout,
                    account.command_timeout(),
                    round_cancel.clone(),
                )
                .await;
            let interrupted_by_resubscribe = round_cancel.is_cancelled() && !cancel.is_cancelled();
            nudge.abort();
            match idle_result {
                // A BYE that lands in the same round a resubscribe cancels is
                // still a BYE: the connection is dead, so it takes the plain
                // arm below (Disconnected, redial) rather than a re-IDLE on a
                // closed socket.
                Ok(event) if interrupted_by_resubscribe && !event_closes_connection(&event) => {
                    // The scope set changed and this IDLE was cancelled so
                    // the folder choice can be re-evaluated. Absorb what did
                    // come back either way (cache coherence is independent
                    // of what happens next).
                    if !matches!(event, IdleEvent::Cancelled)
                        && absorb_idle_event(&account, &folder, uidvalidity, &event).is_err()
                    {
                        let _ = account.push.tx.send(invalidated(HintPayload::Unknown));
                    }
                    // Re-choose under the new scope set. Marking the
                    // generation seen first keeps the same discipline as the
                    // outer loop: a bump landing after this read still
                    // latches and cancels the next round.
                    resubscribe.mark_unchanged();
                    let chosen = choose_idle_folder(&account, slot);
                    if !matches!(
                        resubscribe_action(&folder, chosen.as_ref()),
                        ResubscribeAction::Reidle
                    ) {
                        // A different folder (or none): this connection is
                        // wrong for the new assignment, so the outer loop
                        // redials. Cancellation outranks queued events
                        // inside `idle()` and the events the DONE drain
                        // emitted die with this session, so the window
                        // degrades to a coarse invalidation rather than
                        // being lost silently.
                        signal_idle_interrupt_loss(&account.push);
                        break;
                    }
                    // Same mailbox: re-IDLE on the SAME connection. No
                    // redial (providers with strict connection-rate limits
                    // charge for one on every subscription change), and no
                    // coarse `Unknown` invalidation - nothing was lost,
                    // because `drain_idle_responses` emitted the queued
                    // events to the sink and this session survives into the
                    // next round. The event this round did return is
                    // published rather than folded into the coarse hint.
                    if let Some(event) = map_idle_event(event, &folder) {
                        let _ = account.push.tx.send(event);
                    }
                    // The watched set may still have changed around this
                    // folder, so the NOTIFY registration is re-issued; a
                    // server without NOTIFY logs and keeps selected-only
                    // coverage exactly as it did on the first round.
                    let watched = {
                        let scopes = account
                            .push
                            .scopes
                            .lock()
                            .expect("push scopes lock poisoned");
                        subscribed_idle_folders(&scopes)
                    };
                    let registered =
                        register_notify(&conn, &folder, &watched, account.command_timeout()).await;
                    signal_notify_coverage_loss(&account.push, registered);
                }
                Ok(event) => {
                    if absorb_idle_event(&account, &folder, uidvalidity, &event).is_err() {
                        let _ = account.push.tx.send(invalidated(HintPayload::Unknown));
                    }
                    // A server BYE (or any server-initiated termination)
                    // closes the connection: surface it as a disconnect
                    // and tear down this IDLE so the outer loop redials,
                    // rather than reporting an `Unknown` invalidation and
                    // spinning `idle()` on a dead socket until it errors.
                    if event_closes_connection(&event) {
                        let _ = account.push.tx.send(WatchEvent::Disconnected);
                        was_disconnected = true;
                        session_died = true;
                        break;
                    }
                    // A full IDLE round completed and the session survived:
                    // the next failure starts a fresh backoff ramp.
                    backoff = INITIAL_REDIAL_BACKOFF;
                    if let Some(event) = map_idle_event(event, &folder) {
                        let _ = account.push.tx.send(event);
                    }
                }
                Err(_) => {
                    let _ = account.push.tx.send(WatchEvent::Disconnected);
                    was_disconnected = true;
                    session_died = true;
                    break;
                }
            }
        }
        if session_died && !sleep_backoff(&account, &cancel, &mut backoff).await {
            break;
        }
    }
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
/// Pure so the decision is pinnable without a live server. Only the
/// decision is pinned: the loop around it takes its connection from
/// `pool.dial_idle()`, a real dial, so the re-IDLE wiring above is not
/// hermetically testable and rests on review.
///
/// The same blind spot covers the NOTIFY-collapse degradation. Both
/// `signal_notify_coverage_loss` and the `register_notify` that feeds it are
/// individually tested, but that the two are WIRED - `register_notify`'s
/// answer reaching `signal_notify_coverage_loss` on both call sites, the
/// outer dial-and-SELECT path and the re-IDLE path in the interrupted-round
/// arm - is verified by reading the loop, not by a test. Accepted limit:
/// pinning either would need a dial seam in the pool so the IDLE worker
/// could be driven against a scripted connection, which was ruled heavier
/// machinery than the risk it retires (see also the hermetic-STARTTLS
/// ruling at `ImapStream::into_tcp`). If that seam is ever built for another
/// reason, both of these become cheap and should be pinned together.
fn resubscribe_action(
    current: &crate::types::MailboxName,
    chosen: Option<&crate::types::MailboxName>,
) -> ResubscribeAction {
    match chosen {
        Some(chosen) if chosen.as_str() == current.as_str() => ResubscribeAction::Reidle,
        _ => ResubscribeAction::Redial,
    }
}

fn choose_idle_folder(account: &ImapAccount, slot: usize) -> Option<crate::types::MailboxName> {
    let scopes = account
        .push
        .scopes
        .lock()
        .expect("push scopes lock poisoned");
    if scopes.is_empty() {
        return None;
    }
    if let Some(folder) = subscribed_idle_folders(&scopes).into_iter().nth(slot) {
        return Some(folder);
    }
    drop(scopes);

    // The INBOX fallback belongs to one worker only. Every slot taking it
    // would point the whole budget at the same mailbox and burn
    // `idle_connection_budget` sessions to watch it once.
    //
    // Reachability note: with today's admission (`push_subscribe` inserts
    // only non-empty sets of `CursorScope::Folder` whose names pass
    // `MailboxName::new`), a non-empty scopes map always yields at least
    // one entry above and slot 0 never falls through to here. The fallback
    // is kept as a guard for any future scope kind that subscribes without
    // naming a watchable folder; it is NOT a default-INBOX-push with no
    // subscriptions (the `scopes.is_empty()` early return above forecloses
    // that deliberately).
    if slot != 0 {
        return None;
    }
    account
        .folders
        .entries()
        .into_iter()
        .find(|entry| entry.name.as_str().eq_ignore_ascii_case("INBOX"))
        .map(|entry| entry.name.clone())
}

/// Every subscribed folder scope, sorted and deduplicated.
///
/// This is both the admission ledger and the worker assignment. A
/// HashMap/HashSet backs subscriptions, so the sort is what keeps iteration
/// order from deciding which mailbox a slot watches. Worker `n` takes the
/// nth entry; on a NOTIFY server only slot 0 runs and the rest of the list
/// rides on `NOTIFY SET` from that one session. Admission caps the list at
/// `idle_connection_budget` on a non-NOTIFY server, so every accepted folder
/// always has a slot - the assignment of folder to slot index shifts when
/// the set changes, the coverage of the set does not.
fn subscribed_idle_folders(
    scopes: &HashMap<String, HashSet<CursorScope>>,
) -> Vec<crate::types::MailboxName> {
    let mut folders = scopes
        .values()
        .flat_map(|scopes| scopes.iter())
        .filter_map(|scope| match scope {
            CursorScope::Folder(folder) => crate::types::MailboxName::new(folder.0.clone()).ok(),
            _ => None,
        })
        .collect::<Vec<_>>();
    folders.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    folders.dedup_by(|left, right| left.as_str() == right.as_str());
    folders
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
/// discovery and inventory have just run.
///
/// Returns whether every watched folder is covered. A server without
/// NOTIFY, or one that rejects the registration, leaves the loop watching
/// only the selected mailbox - the pre-NOTIFY behaviour. The caller feeds
/// that answer to `signal_notify_coverage_loss`, which turns a collapse on
/// a NOTIFY-advertising account into a coarse invalidation rather than
/// silence, because the subscribe outcome already reported those folders
/// pushed and cannot be retracted.
/// The `NOTIFY SET` registration for one IDLE round: message events on the
/// selected mailbox, the same events on the other watched mailboxes.
///
/// RFC 5465 Section 5.1: `FlagChange` is only legal alongside `MessageNew`
/// and `MessageExpunge`. Fetch attributes are legal only on the `selected`
/// filter (Section 5.2) and we ask for none - the account layer refetches
/// through the changes stream, so a payload here would be wasted bandwidth
/// on every flag flip.
fn notify_params(others: Vec<String>) -> crate::types::NotifySetParams {
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

async fn register_notify(
    conn: &crate::connection::ImapConnection,
    selected: &crate::types::MailboxName,
    watched: &[crate::types::MailboxName],
    timeout: std::time::Duration,
) -> bool {
    let others: Vec<String> = watched
        .iter()
        .filter(|folder| folder.as_str() != selected.as_str())
        .map(|folder| folder.as_str().to_owned())
        .collect();
    if others.is_empty() {
        return true;
    }
    if !conn.server_profile().supports_notify() {
        tracing::warn!(
            watched = others.len(),
            selected = selected.as_str(),
            "server does not advertise NOTIFY (RFC 5465); IDLE push covers only the \
             selected mailbox and the other subscribed folders will not be pushed"
        );
        return false;
    }
    match conn.notify_set(notify_params(others), timeout).await {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(
                error = %err,
                selected = selected.as_str(),
                "NOTIFY SET rejected; IDLE push covers only the selected mailbox"
            );
            false
        }
    }
}

/// Cover the events a resubscribe-cancelled IDLE round may have swallowed.
///
/// `ImapConnection::idle` gives cancellation strict priority over queued
/// server events, and the DONE handshake that follows drains and discards
/// whatever else was in flight. Reconfiguring the subscription set must not
/// therefore turn into silent push loss for the scopes that are staying:
/// `reference/sync.md` requires a missed push to degrade to a coarser
/// invalidation, never to nothing. `Unknown` is the honest hint - the folder
/// this round was watching is known, but the content of the lost window is
/// not, and unsolicited events can name other mailboxes.
fn signal_idle_interrupt_loss(push: &PushState) {
    let _ = push.tx.send(invalidated(HintPayload::Unknown));
}

/// Cover the folders a runtime `NOTIFY SET` rejection leaves unwatched.
///
/// Admission in `push_subscribe` accepts every folder scope unconditionally
/// on a NOTIFY server, because NOTIFY folds them all onto one session; the
/// account then runs a single IDLE worker. That bet is only settled later,
/// when `register_notify` actually issues `NOTIFY SET` on the dialed
/// connection. If the server refuses it - `[BADEVENT]`, an unsupported
/// mailbox filter, or a connection whose post-redial CAPABILITY no longer
/// advertises NOTIFY - coverage collapses to the one SELECTed mailbox while
/// every other subscribed folder was already reported `Succeeded`.
///
/// The subscribe outcome cannot be retracted: `BatchOutcome` is settled and
/// returned before the first dial, and there is no per-scope demotion event
/// in the watch stream. What is available is the contract `reference/sync.md`
/// states for a missed push - degrade to a coarser invalidation, never to
/// silence. So an unregistered round emits one account-wide `Unknown`, and
/// because registration is re-attempted on every redial and every
/// resubscribe-interrupted round, the uncovered folders keep getting a
/// coarse invalidation at the IDLE cadence instead of nothing at all.
///
/// Gated on the account-level NOTIFY capability on purpose. Without it the
/// account runs `idle_connection_budget` workers and admission caps the
/// subscribed set at that many folders, so every folder has its own SELECTed
/// session and a `false` here means only "this worker has no others to fold
/// in" - not a coverage gap. Firing there would invalidate the whole account
/// once per round on every non-NOTIFY server.
fn signal_notify_coverage_loss(push: &PushState, registered: bool) {
    if push.supports_notify && !registered {
        let _ = push.tx.send(invalidated(HintPayload::Unknown));
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

/// Whether an IDLE event signals that the server is closing this
/// connection. A `BYE` or server-initiated termination leaves the socket
/// unusable; the push loop must redial rather than keep issuing `idle()`.
pub(crate) fn event_closes_connection(event: &IdleEvent) -> bool {
    matches!(event, IdleEvent::Bye { .. } | IdleEvent::ServerTerminated)
}

pub(crate) fn map_idle_event(
    event: IdleEvent,
    selected: &crate::types::MailboxName,
) -> Option<WatchEvent> {
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
        // Unreachable from the push loop - `event_closes_connection` matches
        // `Bye` first. Kept deliberately: the mapping is total over a
        // `#[non_exhaustive]` enum, and the arm costs nothing.
        | IdleEvent::Bye { .. }
        | IdleEvent::ExtensionEvent(_) => Some(invalidated(HintPayload::Unknown)),
        IdleEvent::Timeout | IdleEvent::Cancelled | IdleEvent::ServerTerminated => None,
    }
}

fn invalidated(payload: HintPayload) -> WatchEvent {
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source: PushSource::ImapNotify,
            payload,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bye_and_server_termination_close_the_connection() {
        assert!(event_closes_connection(&IdleEvent::Bye {
            code: None,
            text: "logging out".to_string(),
        }));
        assert!(event_closes_connection(&IdleEvent::ServerTerminated));
        assert!(!event_closes_connection(&IdleEvent::Exists(3)));
        assert!(!event_closes_connection(&IdleEvent::Timeout));
    }

    /// An unsubscribe (or subscribe) cancels the in-flight IDLE round, and
    /// cancellation outranks any server event still queued on that
    /// connection. The retained subscriptions must still learn that
    /// something may have happened.
    #[tokio::test]
    async fn a_resubscribe_interrupted_idle_round_still_invalidates() {
        let push = PushState::new(false, 4);
        let mut rx = push.tx.subscribe();

        signal_idle_interrupt_loss(&push);

        let event = rx.try_recv().expect("an invalidation must be emitted");
        assert!(matches!(
            event,
            WatchEvent::Invalidated {
                hint: InvalidationHint {
                    payload: HintPayload::Unknown,
                    ..
                }
            }
        ));
    }

    /// `push_subscribe` admits every folder scope on a NOTIFY-advertising
    /// account and reports them `Succeeded` before any connection exists,
    /// betting that one session's `NOTIFY SET` will cover them all. When the
    /// server refuses that registration at runtime the account is left
    /// watching one mailbox while bifrost-sync believes all of them are
    /// pushed. The outcome cannot be retracted, so the round must at least
    /// degrade to a coarse invalidation instead of going silent.
    #[tokio::test]
    async fn a_rejected_notify_registration_degrades_instead_of_going_silent() {
        let push = PushState::new(true, 1);
        let mut rx = push.tx.subscribe();

        signal_notify_coverage_loss(&push, true);
        assert!(
            rx.try_recv().is_err(),
            "a registration that took must not invalidate anything",
        );

        signal_notify_coverage_loss(&push, false);
        let event = rx
            .try_recv()
            .expect("a collapsed NOTIFY registration must invalidate");
        assert!(matches!(
            event,
            WatchEvent::Invalidated {
                hint: InvalidationHint {
                    payload: HintPayload::Unknown,
                    ..
                }
            }
        ));
    }

    /// Without NOTIFY the account runs one worker per admitted folder, so
    /// `register_notify` answering `false` means only "nothing to fold in",
    /// not a coverage gap. Invalidating there would fire an account-wide
    /// reconcile once per IDLE round on every non-NOTIFY server.
    #[tokio::test]
    async fn a_non_notify_account_does_not_invalidate_on_a_missing_registration() {
        let push = PushState::new(false, 4);
        let mut rx = push.tx.subscribe();

        signal_notify_coverage_loss(&push, false);

        assert!(
            rx.try_recv().is_err(),
            "per-folder IDLE workers already cover every admitted folder",
        );
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
            .map(|folder| folder.as_str().to_owned())
            .collect();
        assert_eq!(watched, vec!["Archive".to_owned(), "Zebra".to_owned()]);
    }

    // RFC 5465 Section 5.1 makes FlagChange illegal without MessageNew and
    // MessageExpunge, and Section 5.2 makes fetch attributes illegal off
    // the `selected` filter. A registration that breaks either is rejected
    // with [BADEVENT] and the account silently loses push for every folder
    // but one.
    #[test]
    fn notify_registration_is_legal_for_both_filters() {
        let params = notify_params(vec!["Archive".to_owned(), "Zebra".to_owned()]);
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
            crate::types::MailboxFilter::Mailboxes(vec!["Archive".to_owned(), "Zebra".to_owned()])
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
