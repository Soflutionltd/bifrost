use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, PoisonError, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AccountStream, CursorScope, SubscriptionHandle,
    WatchEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;

use super::error;

const DEFAULT_RENEW_AFTER: Duration = Duration::from_secs(6 * 24 * 60 * 60);
const RENEW_BEFORE_EXPIRY: Duration = Duration::from_secs(24 * 60 * 60);
const RENEW_RETRY_AFTER: Duration = Duration::from_secs(5 * 60);
/// Floor for any *computed* renewal delay.
///
/// `start_renewer` clears `retry_after` on a successful re-watch, so the
/// failure-path damper never engages on the success path. Without a floor,
/// any expiration inside the one-day renewal window - a clock skewed
/// forward, a watch whose real TTL is under a day, an expiration Gmail
/// returns unchanged - collapses the delay to zero and turns the renewer
/// into an unthrottled `users.watch` storm. Deliberately not applied to
/// the `None` fallback, which is already six days.
const MIN_RENEW_DELAY: Duration = RENEW_RETRY_AFTER;

/// Process-wide watch coordination for one Gmail mailbox.
///
/// A Gmail watch belongs to the MAILBOX, not to the account instance that
/// created it: `users.watch` on a mailbox that already has one replaces it,
/// and `users.stop` takes no watch identity and stops whatever watch the
/// mailbox has. Two live `GoogleAccount` instances for one mailbox therefore
/// share one remote watch - and the sync engine's reopen creates exactly that
/// pair, subscribing on the replacement BEFORE it tears down the old
/// account's handle. Without coordination the old account's teardown
/// `users.stop` kills the replacement's fresh watch, and push stays silently
/// dead until the next renewal, up to six days later (the abort path of the
/// same reopen does the mirror image to the old account).
///
/// `holders` counts the actors in this process that currently own a live
/// watch on the mailbox; a teardown sends `users.stop` only when no OTHER
/// holder exists. `exchange` serializes every `users.watch` and `users.stop`
/// for the mailbox across instances, so a stop decided on a zero count
/// cannot reach Gmail after another instance's watch that the count did not
/// yet show.
///
/// The bias is deliberate: a watch nobody stops is cheap (it expires within
/// seven days, and a notification with no listener is dropped), a watch
/// stopped under a live listener is expensive (push is silently dead). Every
/// ambiguity - a holder released late, a second consumer in the process
/// using another topic - errs toward skipping the stop. Instances in OTHER
/// processes are invisible here; `users.stop` gives no way to scope to a
/// watch, so that residual cannot be closed on this side.
///
/// The key is the account's configured address, ASCII-lowercased. Two
/// instances that name the same mailbox by different addresses (an alias, a
/// googlemail.com spelling) get separate entries and no coordination; closing
/// that would need the canonical address from Gmail's profile, and the bias
/// above keeps the cost of a miss to a stopped watch only when both instances
/// are live, as before this coordination existed.
#[derive(Default)]
struct MailboxWatch {
    exchange: tokio::sync::Mutex<()>,
    holders: Mutex<usize>,
}

static MAILBOX_WATCHES: LazyLock<Mutex<HashMap<String, Weak<MailboxWatch>>>> =
    LazyLock::new(Mutex::default);

fn mailbox_watch(mailbox: &str) -> Arc<MailboxWatch> {
    let key = mailbox.to_ascii_lowercase();
    let mut watches = MAILBOX_WATCHES
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    watches.retain(|_, watch| watch.strong_count() > 0);
    if let Some(existing) = watches.get(&key).and_then(Weak::upgrade) {
        return existing;
    }
    let fresh = Arc::new(MailboxWatch::default());
    watches.insert(key, Arc::downgrade(&fresh));
    fresh
}

/// One actor's stake in its mailbox's shared watch. Dropping it (the actor
/// ending, for any reason) releases the claim, so a dead instance never pins
/// another instance's stop decision.
struct WatchOwnership {
    mailbox: Arc<MailboxWatch>,
    claimed: bool,
}

impl WatchOwnership {
    fn new(mailbox: Arc<MailboxWatch>) -> Self {
        Self {
            mailbox,
            claimed: false,
        }
    }

    fn holders(&self) -> std::sync::MutexGuard<'_, usize> {
        self.mailbox
            .holders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn claim(&mut self) {
        if !self.claimed {
            *self.holders() += 1;
            self.claimed = true;
        }
    }

    fn release(&mut self) {
        if self.claimed {
            let mut holders = self.holders();
            *holders = holders.saturating_sub(1);
            drop(holders);
            self.claimed = false;
        }
    }

    /// Live watch holders in this process other than this actor.
    fn others(&self) -> usize {
        self.holders().saturating_sub(usize::from(self.claimed))
    }
}

impl Drop for WatchOwnership {
    fn drop(&mut self) {
        self.release();
    }
}

/// Whether a teardown reached Gmail or deferred to another live holder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopOutcome {
    Stopped,
    SharedWithAnotherInstance,
}

/// `users.stop`, unless another instance in this process holds a live watch
/// on the same mailbox - stopping would kill that instance's watch too. The
/// caller must hold the mailbox's `exchange` guard. Releases this actor's own
/// claim on every non-error outcome: either the watch is gone, or it now
/// belongs to the other holder.
async fn stop_unless_shared(
    client: &GmailClient,
    ownership: &mut WatchOwnership,
) -> crate::Result<StopOutcome> {
    if ownership.others() > 0 {
        ownership.release();
        return Ok(StopOutcome::SharedWithAnotherInstance);
    }
    stop_watch(client).await?;
    ownership.release();
    Ok(StopOutcome::Stopped)
}

/// `stop_unless_shared` under the mailbox's `exchange` guard. Every teardown
/// path goes through here, so no stop decision is taken outside the
/// serialization that `watch_claimed` also runs under.
async fn stop_exclusive(
    client: &GmailClient,
    ownership: &mut WatchOwnership,
) -> crate::Result<StopOutcome> {
    let mailbox = Arc::clone(&ownership.mailbox);
    let _exchange = mailbox.exchange.lock().await;
    stop_unless_shared(client, ownership).await
}

/// `users.watch` under the mailbox's `exchange` guard, taking this actor's
/// claim before the guard is released. A stop decided by another instance
/// either ran wholly before this watch (and this watch replaced whatever that
/// stop left) or sees the claim and defers. Claiming is idempotent, so a
/// renewal re-enters here harmlessly.
async fn watch_claimed(
    client: &GmailClient,
    config: &PubSubConfig,
    ownership: &mut WatchOwnership,
) -> crate::Result<GmailWatchResponse> {
    let mailbox = Arc::clone(&ownership.mailbox);
    let _exchange = mailbox.exchange.lock().await;
    let response = watch_once(client, config).await?;
    ownership.claim();
    Ok(response)
}

/// Gmail Cloud Pub/Sub watch configuration for `GoogleAccountFactory`.
#[derive(Debug, Clone)]
pub struct PubSubConfig {
    /// Full Pub/Sub topic name passed to Gmail `users.watch`.
    pub topic: String,
    /// Optional Gmail label filter for watch subscriptions.
    pub label_ids: Vec<String>,
}

impl PubSubConfig {
    /// Create an account-wide watch configuration for `topic`.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            label_ids: Vec::new(),
        }
    }

    /// Restrict watch notifications to selected Gmail label ids.
    pub fn with_label_ids(
        mut self,
        label_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.label_ids = label_ids.into_iter().map(Into::into).collect();
        self
    }
}

pub(crate) struct PubSubControl {
    commands: mpsc::Sender<WatchCommand>,
    health_tx: broadcast::Sender<WatchEvent>,
    shutdown: CancellationToken,
}

enum WatchCommand {
    Subscribe {
        reply: oneshot::Sender<Result<SubscriptionHandle, AccountError>>,
    },
    Unsubscribe {
        handle: SubscriptionHandle,
        reply: oneshot::Sender<Result<(), AccountError>>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
    #[cfg(test)]
    SeedHandle {
        handle: SubscriptionHandle,
        reply: oneshot::Sender<()>,
    },
    #[cfg(test)]
    HasHandles {
        reply: oneshot::Sender<bool>,
    },
}

#[derive(Debug)]
enum WatchLifecycle {
    Unwatched,
    Watched {
        history_id: String,
        expiration: Option<SystemTime>,
    },
    Renewing,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandleRemoval {
    Last,
    Remaining,
    NotPresent,
}

impl PubSubControl {
    /// `mailbox` is the account's Gmail address: the identity Gmail scopes a
    /// watch to, and so the key under which instances coordinate teardown.
    pub(crate) fn new(
        client: Arc<GmailClient>,
        config: Option<PubSubConfig>,
        shutdown: CancellationToken,
        mailbox: &str,
    ) -> Self {
        let (health_tx, _) = broadcast::channel(32);
        let (commands, receiver) = mpsc::channel(16);
        tokio::spawn(watch_actor(
            client,
            config,
            shutdown.clone(),
            health_tx.clone(),
            receiver,
            WatchOwnership::new(mailbox_watch(mailbox)),
        ));
        Self {
            commands,
            health_tx,
            shutdown,
        }
    }

    #[cfg(test)]
    pub(crate) async fn insert_handle(&self, handle: &SubscriptionHandle) {
        let (reply, response) = oneshot::channel();
        let _ = self
            .commands
            .send(WatchCommand::SeedHandle {
                handle: handle.clone(),
                reply,
            })
            .await;
        let _ = response.await;
    }

    #[cfg(test)]
    pub(crate) async fn has_handles(&self) -> bool {
        let (reply, response) = oneshot::channel();
        let _ = self.commands.send(WatchCommand::HasHandles { reply }).await;
        response.await.unwrap_or(false)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GmailWatchResponse {
    history_id: String,
    expiration: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GmailSubscriptionHandle {
    topic: String,
    history_id: String,
    expiration: Option<String>,
}

pub(crate) fn push_subscribe(
    pubsub: Arc<PubSubControl>,
    scopes: Vec<CursorScope>,
) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
    Box::pin(async move {
        if scopes.is_empty()
            || scopes
                .iter()
                .any(|scope| !matches!(scope, CursorScope::Account))
        {
            return Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::PushSubscribe),
                error::GmailErrorContext::push_subscribe(),
            ));
        }
        if pubsub.shutdown.is_cancelled() {
            return Err(closed_error(AccountOperation::PushSubscribe));
        }
        let (reply, response) = oneshot::channel();
        pubsub
            .commands
            .send(WatchCommand::Subscribe { reply })
            .await
            .map_err(|_| closed_error(AccountOperation::PushSubscribe))?;
        response
            .await
            .unwrap_or_else(|_| Err(closed_error(AccountOperation::PushSubscribe)))
    })
}

pub(crate) fn push_unsubscribe(
    pubsub: Arc<PubSubControl>,
    handle: SubscriptionHandle,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let (reply, response) = oneshot::channel();
        pubsub
            .commands
            .send(WatchCommand::Unsubscribe { handle, reply })
            .await
            .map_err(|_| closed_error(AccountOperation::PushUnsubscribe))?;
        response
            .await
            .unwrap_or_else(|_| Err(closed_error(AccountOperation::PushUnsubscribe)))
    })
}

pub(crate) async fn close_watch(pubsub: &PubSubControl) {
    let (reply, response) = oneshot::channel();
    if pubsub
        .commands
        .send(WatchCommand::Close { reply })
        .await
        .is_ok()
    {
        let _ = response.await;
    }
}

pub(crate) fn push_stream(
    pubsub: Arc<PubSubControl>,
    shutdown: CancellationToken,
) -> AccountStream<WatchEvent> {
    let receiver = pubsub.health_tx.subscribe();
    Box::pin(futures::stream::unfold(
        (receiver, shutdown),
        |(mut receiver, shutdown)| async move {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return None,
                    result = receiver.recv() => {
                        match result {
                            Ok(event) => return Some((event, (receiver, shutdown))),
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => return None,
                        }
                    }
                }
            }
        },
    ))
}

async fn watch_actor(
    client: Arc<GmailClient>,
    config: Option<PubSubConfig>,
    shutdown: CancellationToken,
    health_tx: broadcast::Sender<WatchEvent>,
    mut commands: mpsc::Receiver<WatchCommand>,
    mut ownership: WatchOwnership,
) {
    let mut lifecycle = WatchLifecycle::Unwatched;
    let mut handles = HashSet::new();
    let mut disconnected = false;
    // Consecutive failed renewal attempts, so the transient-failure
    // `Warning` reports how deep the failure run is instead of a
    // constant 1. Cleared by any completed request (subscribe or
    // renewal), which is the same edge the `disconnected` latch uses.
    let mut consecutive_renewal_failures: u32 = 0;
    let mut retry_after = None;
    loop {
        // A claim is held only while this actor owns a live watch. Claims are
        // taken eagerly (inside the serialized exchange that created the
        // watch) and dropped here lazily; a late release only makes another
        // instance skip a stop, which is the cheap direction.
        if !matches!(
            lifecycle,
            WatchLifecycle::Watched { .. } | WatchLifecycle::Renewing
        ) {
            ownership.release();
        }
        // Computed WITHOUT consuming `retry_after`. This runs on every trip
        // around the loop, including trips that end in a command rather than
        // in the renewal timer, and a `take()` here let any command arriving
        // during the five-minute failure backoff erase the damper - for a
        // watch with no expiration the next computation then fell back to
        // the six-day default, stretching a transient-failure retry into six
        // days of dead push. The damper is cleared where the timer actually
        // fires, and on the paths that install a fresh lifecycle state.
        let delay = match &lifecycle {
            WatchLifecycle::Watched { expiration, .. } => Some(
                retry_after.unwrap_or_else(|| renewal_delay_at(*expiration, client.wall_now())),
            ),
            _ => None,
        };
        tokio::select! {
            biased;
            command = commands.recv() => {
                let Some(command) = command else { return; };
                match command {
                    WatchCommand::Subscribe { reply } => {
                        let result = actor_subscribe(
                            &client, config.as_ref(), &shutdown,
                            &mut lifecycle, &mut handles, &mut ownership,
                        ).await;
                        if result.is_ok() {
                            // A completed subscribe is a completed request,
                            // so the Disconnected latch is cleared here to
                            // keep Reconnected edge-triggered. It does NOT
                            // clear `retry_after`: a transient-failure
                            // damper reset by a *subscribe* is how an
                            // endless subscribe-then-die loop escapes its
                            // backoff.
                            if disconnected {
                                let _ = health_tx.send(WatchEvent::Reconnected);
                            }
                            disconnected = false;
                            consecutive_renewal_failures = 0;
                        }
                        let _ = reply.send(result);
                    }
                    // Accepted: because commands are biased ahead of the
                    // shutdown arm, an unsubscribe already queued when
                    // `close()` cancels the token can still issue a wire
                    // `users.stop` after close intent. The transport is still
                    // attached at that point and stopping the watch is what
                    // close wants anyway, so this small hole in "no wire
                    // traffic after shutdown" is deliberate.
                    WatchCommand::Unsubscribe { handle, reply } => {
                        let result = actor_unsubscribe(
                            &client, handle, &mut lifecycle, &mut handles, &mut ownership,
                        ).await;
                        let _ = reply.send(result);
                    }
                    WatchCommand::Close { reply } => {
                        actor_close(&client, &mut lifecycle, &mut handles, &mut ownership).await;
                        let _ = reply.send(());
                    }
                    #[cfg(test)]
                    WatchCommand::SeedHandle { handle, reply } => {
                        handles.insert(handle.0);
                        let _ = reply.send(());
                    }
                    #[cfg(test)]
                    WatchCommand::HasHandles { reply } => {
                        let _ = reply.send(!handles.is_empty());
                    }
                }
            }
            () = shutdown.cancelled(), if !matches!(lifecycle, WatchLifecycle::Retired) => {
                lifecycle = WatchLifecycle::Retired;
                retry_after = None;
            }
            () = async { tokio::time::sleep(delay.expect("guarded renewal delay")).await }, if delay.is_some() => {
                // The wait this damper asked for has now been served.
                retry_after = None;
                let WatchLifecycle::Watched { history_id, expiration } = &lifecycle else {
                    continue;
                };
                let previous_history_id = history_id.clone();
                let previous_expiration = *expiration;
                lifecycle = WatchLifecycle::Renewing;
                let Some(config) = config.as_ref() else {
                    lifecycle = WatchLifecycle::Unwatched;
                    continue;
                };
                // Deliberately NOT raced against `shutdown.cancelled()`.
                // An unbiased select between the token and the request can
                // pick the completed request, and every commit after that
                // point would install a watch nobody will ever renew or
                // stop. The request runs to completion and the decision to
                // keep its result is made by `commit_watched`, which cannot
                // lose that race because it reads the token after the
                // response is already in hand.
                let result = watch_claimed(&client, config, &mut ownership).await;
                match result {
                    Ok(response) => {
                        if !commit_watched(
                            &client, &shutdown, &mut lifecycle, &response, &mut ownership,
                        ).await {
                            retry_after = None;
                            continue;
                        }
                        retry_after = None;
                        consecutive_renewal_failures = 0;
                        if disconnected {
                            let _ = health_tx.send(WatchEvent::Reconnected);
                            disconnected = false;
                        }
                    }
                    Err(err) => {
                        let account_error = error::into_account_error(
                            err, error::GmailErrorContext::push_subscribe(),
                        );
                        if account_error.recovery().is_terminal() {
                            let _ = health_tx.send(WatchEvent::Terminated(account_error));
                            lifecycle = WatchLifecycle::Unwatched;
                            retry_after = None;
                            disconnected = false;
                            consecutive_renewal_failures = 0;
                            continue;
                        }
                        consecutive_renewal_failures =
                            consecutive_renewal_failures.saturating_add(1);
                        let warning = bifrost_types::Warning::support_only(
                            bifrost_types::WarningKind::OperatorAttentionNeeded,
                            format!("gmail Pub/Sub renewal transient failure: {}", account_error.message_key()),
                        ).with_retry_count(consecutive_renewal_failures);
                        let _ = health_tx.send(WatchEvent::Warning(warning));
                        if !disconnected {
                            let _ = health_tx.send(WatchEvent::Disconnected);
                            disconnected = true;
                        }
                        if shutdown.is_cancelled() {
                            lifecycle = WatchLifecycle::Retired;
                            retry_after = None;
                            continue;
                        }
                        lifecycle = WatchLifecycle::Watched {
                            history_id: previous_history_id,
                            expiration: previous_expiration,
                        };
                        retry_after = Some(RENEW_RETRY_AFTER);
                    }
                }
            }
        }
    }
}

/// Install a Gmail watch and return its handle.
///
/// Emits no health event of its own. `WatchEvent::Reconnected` is
/// edge-triggered off the actor's `disconnected` latch by the caller: a
/// subscribe is the consumer's FIRST act, so announcing a reconnect from
/// here published an event before any consumer could hold a
/// `push_stream()` receiver, and `broadcast` drops messages with no
/// receivers - making the stream's first observable state depend on
/// scheduling rather than on the watch.
async fn actor_subscribe(
    client: &GmailClient,
    config: Option<&PubSubConfig>,
    shutdown: &CancellationToken,
    lifecycle: &mut WatchLifecycle,
    handles: &mut HashSet<String>,
    ownership: &mut WatchOwnership,
) -> Result<SubscriptionHandle, AccountError> {
    if shutdown.is_cancelled() || matches!(lifecycle, WatchLifecycle::Retired) {
        return Err(closed_error(AccountOperation::PushSubscribe));
    }
    let Some(config) = config else {
        return Err(error::into_account_error(
            crate::error::Error::unsupported_with(
                AccountOperation::PushSubscribe,
                "no Pub/Sub topic configured",
            ),
            error::GmailErrorContext::push_subscribe(),
        ));
    };
    // Deliberately NOT raced against `shutdown.cancelled()` - see the
    // matching note on the renewal path. Racing the two lets a completed
    // `users.watch` be committed by the arm that won, leaving a Gmail-side
    // watch that no renewer refreshes and no `close()` retires. The request
    // runs to completion and `commit_watched` decides afterwards.
    let response = watch_claimed(client, config, ownership)
        .await
        .map_err(|error| {
            error::into_account_error(error, error::GmailErrorContext::push_subscribe())
        })?;
    // Encode before committing. An encode failure after the commit would
    // leave `Watched` installed with no handle in the set, so `close()`
    // would skip `users.stop` and the renewer would keep the orphan alive.
    let handle = GmailSubscriptionHandle {
        topic: config.topic.clone(),
        history_id: response.history_id.clone(),
        expiration: response.expiration.clone(),
    };
    let handle = match serde_json::to_string(&handle).map(SubscriptionHandle) {
        Ok(handle) => handle,
        Err(error) => {
            // Only tear down the Gmail-side watch if no other subscriber is
            // sharing it. A later subscriber joining an existing watch must
            // not stop the watch the earlier handles depend on; the watch
            // stays live and the renewer keeps refreshing it.
            if handles.is_empty() {
                let _ = stop_exclusive(client, ownership).await;
            }
            return Err(error::into_account_error(
                crate::error::Error::invalid_request(
                    AccountOperation::PushSubscribe,
                    format!("subscription handle encode failed: {error}"),
                ),
                error::GmailErrorContext::push_subscribe(),
            ));
        }
    };
    if !commit_watched(client, shutdown, lifecycle, &response, ownership).await {
        return Err(closed_error(AccountOperation::PushSubscribe));
    }
    handles.insert(handle.0.clone());
    Ok(handle)
}

async fn actor_unsubscribe(
    client: &GmailClient,
    handle: SubscriptionHandle,
    lifecycle: &mut WatchLifecycle,
    handles: &mut HashSet<String>,
    ownership: &mut WatchOwnership,
) -> Result<(), AccountError> {
    let _decoded: GmailSubscriptionHandle = serde_json::from_str(&handle.0).map_err(|error| {
        error::into_account_error(
            crate::error::Error::invalid_request(
                AccountOperation::PushUnsubscribe,
                format!("invalid gmail subscription handle: {error}"),
            ),
            error::GmailErrorContext::push_unsubscribe(),
        )
    })?;
    // `Retired` is absorbing. `close()` (or the shutdown token) has already
    // stopped the watch and cleared the handle set, so there is nothing left
    // for this handle to release and no wire call to make - and the
    // fall-through below would both issue a post-close `users.stop` and
    // overwrite `Retired` with `Unwatched`, reviving a lifecycle the guard
    // in `commit_watched` promises can never come back.
    if matches!(lifecycle, WatchLifecycle::Retired) {
        return Ok(());
    }
    let removal = remove_handle(handles, &handle);
    match removal {
        HandleRemoval::Last => {}
        HandleRemoval::Remaining => return Ok(()),
        HandleRemoval::NotPresent if !handles.is_empty() => return Ok(()),
        HandleRemoval::NotPresent => {}
    }
    if let Err(error) = stop_exclusive(client, ownership).await {
        if removal == HandleRemoval::Last {
            handles.insert(handle.0);
        }
        return Err(error::into_account_error(
            error,
            error::GmailErrorContext::push_unsubscribe(),
        ));
    }
    *lifecycle = WatchLifecycle::Unwatched;
    Ok(())
}

async fn actor_close(
    client: &GmailClient,
    lifecycle: &mut WatchLifecycle,
    handles: &mut HashSet<String>,
    ownership: &mut WatchOwnership,
) {
    if !handles.is_empty()
        && let Err(error) = stop_exclusive(client, ownership).await
    {
        let account_error =
            error::into_account_error(error, error::GmailErrorContext::push_unsubscribe());
        tracing::warn!(
            target: "bifrost_google::push",
            message_key = account_error.message_key(),
            recovery = ?account_error.recovery(),
            "close() could not retire the Gmail watch",
        );
    }
    handles.clear();
    *lifecycle = WatchLifecycle::Retired;
}

fn remove_handle(handles: &mut HashSet<String>, handle: &SubscriptionHandle) -> HandleRemoval {
    if !handles.remove(&handle.0) {
        return HandleRemoval::NotPresent;
    }
    if handles.is_empty() {
        HandleRemoval::Last
    } else {
        HandleRemoval::Remaining
    }
}

/// The only place a `users.watch` response is turned into `Watched`.
///
/// Both request paths run `watch_once` to completion rather than racing it
/// against the shutdown token, so by the time control reaches here a remote
/// watch definitely exists and the only open question is whether we are
/// still allowed to own it. Retirement is decided *after* the response is in
/// hand, which is what makes this unlosable: a check inside a `select!` arm
/// can be beaten by the request arm, a check on the committed value cannot.
///
/// Returns `false` when the account retired underneath the request. In that
/// case the freshly created remote watch is retired best-effort - leaving it
/// is the orphan-watch failure this guard exists to prevent - and the
/// lifecycle is pinned to `Retired` so no later transition can revive it.
///
/// The history id and the expiration move together, as one transition on one
/// owned value: there is no window in which a new history id is visible
/// against a stale expiration.
async fn commit_watched(
    client: &GmailClient,
    shutdown: &CancellationToken,
    lifecycle: &mut WatchLifecycle,
    response: &GmailWatchResponse,
    ownership: &mut WatchOwnership,
) -> bool {
    if shutdown.is_cancelled() || matches!(lifecycle, WatchLifecycle::Retired) {
        *lifecycle = WatchLifecycle::Retired;
        if let Err(error) = stop_exclusive(client, ownership).await {
            let account_error =
                error::into_account_error(error, error::GmailErrorContext::push_unsubscribe());
            tracing::warn!(
                target: "bifrost_google::push",
                message_key = account_error.message_key(),
                recovery = ?account_error.recovery(),
                "could not retire a Gmail watch that completed after close()",
            );
        }
        return false;
    }
    *lifecycle = WatchLifecycle::Watched {
        history_id: response.history_id.clone(),
        expiration: response.expiration.as_deref().and_then(parse_expiration),
    };
    true
}

fn closed_error(operation: AccountOperation) -> AccountError {
    error::into_account_error(
        crate::error::Error::unsupported_with(operation, "account is closed"),
        error::GmailErrorContext::push_subscribe(),
    )
}

async fn watch_once(
    client: &GmailClient,
    config: &PubSubConfig,
) -> crate::Result<GmailWatchResponse> {
    let mut body = json!({ "topicName": config.topic });
    if !config.label_ids.is_empty() {
        body["labelIds"] = json!(config.label_ids);
    }
    client.post("/watch", &body).await
}

async fn stop_watch(client: &GmailClient) -> crate::Result<()> {
    client.post_no_content("/stop", &json!({})).await
}

fn parse_expiration(value: &str) -> Option<SystemTime> {
    let millis = value.parse::<u64>().ok()?;
    UNIX_EPOCH.checked_add(Duration::from_millis(millis))
}

#[cfg(test)]
fn renewal_delay(expiration: Option<SystemTime>) -> Duration {
    renewal_delay_at(expiration, SystemTime::now())
}

fn renewal_delay_at(expiration: Option<SystemTime>, now: SystemTime) -> Duration {
    let Some(expiration) = expiration else {
        return DEFAULT_RENEW_AFTER;
    };
    let Ok(until_expiration) = expiration.duration_since(now) else {
        return MIN_RENEW_DELAY;
    };
    until_expiration
        .checked_sub(RENEW_BEFORE_EXPIRY)
        .unwrap_or(Duration::ZERO)
        .max(MIN_RENEW_DELAY)
}

#[cfg(test)]
mod tests {
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use bytes::Bytes;
    use reqwest::StatusCode;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn project_hold_watch_renewer_makes_no_wire_calls() {
        let (client, script) = scripted_client([Canned::Response {
            status: StatusCode::OK,
            headers: Default::default(),
            body: Bytes::from_static(br#"{"historyId":"1","expiration":"0"}"#),
        }]);
        let client = Arc::new(
            client
                .with_fixed_clock(UNIX_EPOCH + Duration::from_secs(1_000_000))
                .with_quota_project("123".into()),
        );
        let shutdown = CancellationToken::new();
        let control = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            Some(PubSubConfig::new("projects/p/topics/t")),
            shutdown.clone(),
            "quota-hold@push.test",
        ));
        let mut health = control.health_tx.subscribe();
        push_subscribe(Arc::clone(&control), vec![CursorScope::Account])
            .await
            .unwrap();
        let wall = SystemTime::UNIX_EPOCH;
        client
            .account_net()
            .shared_hold("gmail-project|123")
            .record(
                bifrost_net::HoldDeadline::new(
                    wall + Duration::from_secs(10_000),
                    wall,
                    tokio::time::Instant::now(),
                ),
                bifrost_net::HoldKind::QuotaExhausted,
            );
        tokio::time::advance(MIN_RENEW_DELAY + Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(script.requests().len(), 1);
        assert!(
            std::iter::from_fn(|| health.try_recv().ok())
                .any(|event| matches!(event, WatchEvent::Warning(_)))
        );
        shutdown.cancel();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(script.requests().len(), 1);
    }

    fn encoded_handle() -> SubscriptionHandle {
        SubscriptionHandle(
            serde_json::to_string(&GmailSubscriptionHandle {
                topic: "projects/p/topics/t".to_owned(),
                history_id: "12345".to_owned(),
                expiration: Some("1700000000000".to_owned()),
            })
            .expect("handle encodes"),
        )
    }

    fn scripted_client(
        steps: impl IntoIterator<Item = Canned>,
    ) -> (Arc<GmailClient>, Arc<ScriptedDispatch>) {
        let script = ScriptedDispatch::new(steps);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        (
            Arc::new(GmailClient::with_account_net("https://gmail.test", net)),
            script,
        )
    }

    fn canned(status: StatusCode) -> Canned {
        Canned::Response {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    fn in_future(after: Duration) -> SystemTime {
        SystemTime::now() + after
    }

    // ---- parse_expiration ---------------------------------------------

    /// Gmail returns the watch expiration as epoch milliseconds in a
    /// JSON string.
    #[test]
    fn expiration_parses_epoch_millis() {
        let parsed = parse_expiration("1700000000000").expect("epoch millis parse");
        assert_eq!(
            parsed
                .duration_since(UNIX_EPOCH)
                .expect("after the epoch")
                .as_millis(),
            1_700_000_000_000
        );
    }

    #[test]
    fn expiration_rejects_non_numeric_and_negative_values() {
        assert!(parse_expiration("").is_none());
        assert!(parse_expiration("not-a-number").is_none());
        assert!(parse_expiration("-1").is_none());
        assert!(
            parse_expiration("1.5e12").is_none(),
            "a float rendering is not the documented shape"
        );
        assert!(
            parse_expiration(" 1700000000000 ").is_none(),
            "the parser does not trim; a padded value is rejected rather than guessed at"
        );
    }

    // ---- renewal_delay --------------------------------------------------

    /// No expiration in the watch response means Gmail told us nothing;
    /// fall back to the six-day default rather than renewing eagerly.
    #[test]
    fn missing_expiration_falls_back_to_the_six_day_default() {
        assert_eq!(renewal_delay(None), DEFAULT_RENEW_AFTER);
        assert_eq!(DEFAULT_RENEW_AFTER, Duration::from_secs(6 * 24 * 60 * 60));
    }

    /// The normal case: Gmail's watch lasts seven days, so the renewer
    /// wakes one day before expiry.
    #[test]
    fn a_seven_day_expiration_renews_one_day_early() {
        let seven_days = Duration::from_secs(7 * 24 * 60 * 60);
        let delay = renewal_delay(Some(in_future(seven_days)));
        let expected = seven_days - RENEW_BEFORE_EXPIRY;
        assert!(delay <= expected, "got {delay:?}");
        assert!(
            delay + Duration::from_secs(5) >= expected,
            "got {delay:?}, expected roughly {expected:?}"
        );
    }

    #[test]
    fn an_expiration_inside_the_renewal_window_uses_the_minimum_delay() {
        assert_eq!(
            renewal_delay(Some(in_future(Duration::from_secs(60)))),
            MIN_RENEW_DELAY,
            "one minute from expiry is inside the one-day window"
        );
        assert_eq!(
            renewal_delay(Some(in_future(RENEW_BEFORE_EXPIRY))),
            MIN_RENEW_DELAY,
            "exactly at the window boundary"
        );
        assert_eq!(
            renewal_delay(Some(SystemTime::now() - Duration::from_secs(3600))),
            MIN_RENEW_DELAY,
            "an already-expired watch still observes the floor"
        );
        assert_eq!(
            renewal_delay(Some(UNIX_EPOCH)),
            MIN_RENEW_DELAY,
            "a nonsense epoch-zero expiration behaves the same way"
        );
    }

    /// Just past the window the delay becomes positive again, so the
    /// zero above is a boundary behaviour rather than a constant.
    #[test]
    fn an_expiration_past_the_renewal_window_yields_a_positive_delay() {
        let delay = renewal_delay(Some(in_future(
            RENEW_BEFORE_EXPIRY + Duration::from_secs(600),
        )));
        assert!(delay > Duration::ZERO, "got {delay:?}");
        assert!(delay <= Duration::from_secs(600));
    }

    #[test]
    fn retry_cadence_constants_are_the_documented_ones() {
        assert_eq!(RENEW_RETRY_AFTER, Duration::from_secs(5 * 60));
        assert_eq!(RENEW_BEFORE_EXPIRY, Duration::from_secs(24 * 60 * 60));
    }

    // ---- subscription handle envelope ----------------------------------

    /// The handle a consumer holds is a JSON envelope; `push_unsubscribe`
    /// round-trips it before touching the active-handle set, so a
    /// malformed handle is rejected as a local request error rather than
    /// stopping someone else's watch.
    #[test]
    fn subscription_handle_round_trips_through_json() {
        let handle = GmailSubscriptionHandle {
            topic: "projects/p/topics/t".to_owned(),
            history_id: "12345".to_owned(),
            expiration: Some("1700000000000".to_owned()),
        };
        let encoded = serde_json::to_string(&handle).expect("encode");
        let decoded: GmailSubscriptionHandle = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded.topic, handle.topic);
        assert_eq!(decoded.history_id, handle.history_id);
        assert_eq!(decoded.expiration, handle.expiration);

        assert!(
            serde_json::from_str::<GmailSubscriptionHandle>("not json").is_err(),
            "a malformed handle must not decode"
        );
        assert!(
            serde_json::from_str::<GmailSubscriptionHandle>(r#"{"topic":"t"}"#).is_err(),
            "history_id is required in the envelope"
        );
    }

    #[tokio::test]
    async fn handle_removal_distinguishes_unknown_remaining_and_last() {
        let mut handles = HashSet::new();
        let first = SubscriptionHandle("first".to_string());
        let second = SubscriptionHandle("second".to_string());
        let unknown = SubscriptionHandle("unknown".to_string());

        assert_eq!(
            remove_handle(&mut handles, &unknown),
            HandleRemoval::NotPresent
        );
        handles.insert(first.0.clone());
        handles.insert(second.0.clone());
        assert_eq!(
            remove_handle(&mut handles, &unknown),
            HandleRemoval::NotPresent
        );
        assert_eq!(
            remove_handle(&mut handles, &first),
            HandleRemoval::Remaining
        );
        assert_eq!(remove_handle(&mut handles, &second), HandleRemoval::Last);
        assert_eq!(
            remove_handle(&mut handles, &second),
            HandleRemoval::NotPresent
        );
    }

    #[tokio::test]
    async fn persisted_handle_after_restart_still_stops_the_watch() {
        let (client, script) = scripted_client([canned(StatusCode::NO_CONTENT)]);
        let shutdown = CancellationToken::new();
        let control = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            None,
            shutdown,
            "persisted-handle@push.test",
        ));

        push_unsubscribe(control, encoded_handle())
            .await
            .expect("persisted handle stops watch");

        let requests = script.requests();
        assert_eq!(requests.len(), 1, "the test must observe the stop request");
        assert_eq!(requests[0].method, reqwest::Method::POST);
        assert_eq!(requests[0].url.path(), "/stop");
    }

    #[tokio::test]
    async fn failed_last_stop_restores_the_handle_for_retry() {
        let (client, script) = scripted_client([
            canned(StatusCode::FORBIDDEN),
            canned(StatusCode::NO_CONTENT),
        ]);
        let shutdown = CancellationToken::new();
        let control = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            None,
            shutdown,
            "failed-last-stop@push.test",
        ));
        let handle = encoded_handle();
        control.insert_handle(&handle).await;

        assert!(
            push_unsubscribe(Arc::clone(&control), handle.clone())
                .await
                .is_err()
        );
        assert!(
            control.has_handles().await,
            "failed stop must remain retryable"
        );

        push_unsubscribe(Arc::clone(&control), handle)
            .await
            .expect("retry stops watch");
        assert!(!control.has_handles().await);
        assert_eq!(
            script.requests().len(),
            2,
            "both stop attempts reached the wire"
        );
    }

    #[tokio::test]
    async fn close_stops_a_locally_active_watch_before_clearing_state() {
        let (client, script) = scripted_client([canned(StatusCode::NO_CONTENT)]);
        let shutdown = CancellationToken::new();
        let control = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            None,
            shutdown,
            "close-stops@push.test",
        ));
        control.insert_handle(&encoded_handle()).await;

        close_watch(&control).await;

        assert!(!control.has_handles().await);
        let requests = script.requests();
        assert_eq!(requests.len(), 1, "close must reach users.stop");
        assert_eq!(requests[0].url.path(), "/stop");
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_renewal_then_resubscribe_has_a_live_renewer() {
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis();
        let watch_body = |history_id| {
            format!("{{\"historyId\":\"{history_id}\",\"expiration\":\"{now_millis}\"}}")
        };
        let stable_expiration = now_millis + Duration::from_secs(7 * 24 * 60 * 60).as_millis();
        let (client, script) = scripted_client([
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from(watch_body("1").into_bytes()),
            },
            canned(StatusCode::FORBIDDEN),
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from(watch_body("2").into_bytes()),
            },
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from(
                    format!("{{\"historyId\":\"3\",\"expiration\":\"{stable_expiration}\"}}")
                        .into_bytes(),
                ),
            },
        ]);
        let shutdown = CancellationToken::new();
        let pubsub = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            Some(PubSubConfig::new("projects/p/topics/t")),
            shutdown,
            "terminal-renewal@push.test",
        ));
        let mut health = pubsub.health_tx.subscribe();

        push_subscribe(Arc::clone(&pubsub), vec![CursorScope::Account])
            .await
            .expect("subscribe issues the watch");
        tokio::time::advance(MIN_RENEW_DELAY + Duration::from_secs(1)).await;
        loop {
            if matches!(
                health.recv().await.expect("health event"),
                WatchEvent::Terminated(_)
            ) {
                break;
            }
        }

        push_subscribe(Arc::clone(&pubsub), vec![CursorScope::Account])
            .await
            .expect("subscribe after terminal renewal restarts the lifecycle");
        tokio::time::advance(MIN_RENEW_DELAY + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        pubsub.shutdown.cancel();
        assert_eq!(
            script.requests().len(),
            4,
            "the replacement watch must itself renew",
        );
    }

    /// The failure damper must survive unrelated actor traffic. The delay is
    /// recomputed on every loop trip, and consuming `retry_after` during that
    /// computation meant any command landing inside the five-minute backoff
    /// erased it - for a watch with no expiration the next computation fell
    /// back to `DEFAULT_RENEW_AFTER`, turning a transient renewal failure
    /// into six days of dead push. The renewal must still fire on the
    /// five-minute cadence after a command interrupts the wait.
    #[tokio::test(start_paused = true)]
    async fn a_command_during_the_renewal_backoff_does_not_erase_the_damper() {
        let (client, script) = scripted_client([
            // Subscribe: a watch with NO expiration, so the fallback delay
            // is the six-day default - the value the erased damper falls
            // back to.
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from_static(br#"{"historyId":"1"}"#),
            },
            // First renewal: transient failure, arming the damper.
            canned(StatusCode::SERVICE_UNAVAILABLE),
            // The damped retry, five minutes later.
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from_static(br#"{"historyId":"2"}"#),
            },
        ]);
        let shutdown = CancellationToken::new();
        let pubsub = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            Some(PubSubConfig::new("projects/p/topics/t")),
            shutdown.clone(),
            "damper@push.test",
        ));

        push_subscribe(Arc::clone(&pubsub), vec![CursorScope::Account])
            .await
            .expect("subscribe issues the watch");
        tokio::time::advance(DEFAULT_RENEW_AFTER + Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(script.requests().len(), 2, "the failing renewal ran");

        // Unrelated actor traffic inside the backoff window.
        let _ = pubsub.has_handles().await;

        tokio::time::advance(RENEW_RETRY_AFTER + Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        shutdown.cancel();
        assert_eq!(
            script.requests().len(),
            3,
            "the damped retry must fire five minutes after the failure, \
             not six days after a command recomputed the delay",
        );
    }

    /// Two health-lane properties that used to be wrong in opposite
    /// directions.
    ///
    /// `Reconnected` is edge-triggered off the `disconnected` latch, so the
    /// FIRST subscribe announces nothing. It used to fire unconditionally
    /// from inside `actor_subscribe`, before any consumer could hold a
    /// `push_stream()` receiver - and `broadcast` drops messages with no
    /// receivers, so whether the stream opened on `Reconnected` or on
    /// nothing depended on scheduling.
    ///
    /// The transient-failure `Warning` counts consecutive failures instead
    /// of reporting a constant 1, so an operator reading the lane can tell
    /// one stumble from a sustained outage.
    #[tokio::test(start_paused = true)]
    async fn renewal_failures_escalate_the_warning_count_and_first_subscribe_is_silent() {
        let (client, _script) = scripted_client([
            // Subscribe: no expiration, so renewal falls back to the default.
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from_static(br#"{"historyId":"1"}"#),
            },
            canned(StatusCode::SERVICE_UNAVAILABLE),
            canned(StatusCode::SERVICE_UNAVAILABLE),
        ]);
        let shutdown = CancellationToken::new();
        let pubsub = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            Some(PubSubConfig::new("projects/p/topics/t")),
            shutdown.clone(),
            "warning-count@push.test",
        ));
        let mut health = pubsub.health_tx.subscribe();

        push_subscribe(Arc::clone(&pubsub), vec![CursorScope::Account])
            .await
            .expect("subscribe issues the watch");
        assert!(
            health.try_recv().is_err(),
            "a first subscribe has nothing to reconnect FROM, so it must be silent",
        );

        let mut counts = Vec::new();
        tokio::time::advance(DEFAULT_RENEW_AFTER + Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(RENEW_RETRY_AFTER + Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        shutdown.cancel();
        while let Ok(event) = health.try_recv() {
            if let WatchEvent::Warning(warning) = event {
                counts.push(warning.retry_count);
            }
        }

        assert_eq!(
            counts,
            vec![1, 2],
            "consecutive renewal failures must escalate the reported count",
        );
    }

    /// `Retired` is absorbing: an unsubscribe arriving after `close()` has
    /// nothing left to release, so it succeeds locally with no wire call.
    /// The fall-through used to issue a post-close `users.stop` AND
    /// overwrite `Retired` with `Unwatched`.
    #[tokio::test]
    async fn unsubscribe_after_close_is_a_local_no_op() {
        let (client, script) = scripted_client([canned(StatusCode::NO_CONTENT)]);
        let shutdown = CancellationToken::new();
        let control = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            None,
            shutdown,
            "unsubscribe-after-close@push.test",
        ));

        close_watch(&control).await;
        assert!(
            script.requests().is_empty(),
            "no handle, so close sends nothing"
        );

        push_unsubscribe(Arc::clone(&control), encoded_handle())
            .await
            .expect("unsubscribing a retired watch is idempotent teardown");

        assert!(
            script.requests().is_empty(),
            "a retired actor must not issue users.stop for a late unsubscribe",
        );
    }

    #[tokio::test]
    async fn subscribe_after_shutdown_is_rejected_without_a_wire_call() {
        let (client, script) = scripted_client([]);
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let pubsub = Arc::new(PubSubControl::new(
            client,
            Some(PubSubConfig::new("projects/p/topics/t")),
            shutdown,
            "subscribe-after-shutdown@push.test",
        ));

        let error = push_subscribe(pubsub, vec![CursorScope::Account])
            .await
            .expect_err("closed accounts reject push subscribe");

        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(_)
        ));
        assert!(script.requests().is_empty());
    }

    #[test]
    fn watch_response_decodes_gmails_string_shaped_fields() {
        let response: GmailWatchResponse =
            serde_json::from_str(r#"{"historyId":"987","expiration":"1700000000000"}"#)
                .expect("watch response decodes");
        assert_eq!(response.history_id, "987");
        assert_eq!(response.expiration.as_deref(), Some("1700000000000"));

        let no_expiry: GmailWatchResponse =
            serde_json::from_str(r#"{"historyId":"987"}"#).expect("expiration is optional");
        assert!(no_expiry.expiration.is_none());
    }

    /// gmail-G14: the history id and the expiration are one transition on
    /// one owned value. The assertion pins the EXACT expected pair rather
    /// than "not the stale one" - a test that only excludes the stale
    /// expiration is also satisfied by any other wrong value.
    #[tokio::test]
    async fn commit_watched_moves_history_id_and_expiration_together() {
        let (client, script) = scripted_client([]);
        let shutdown = CancellationToken::new();
        let mut lifecycle = WatchLifecycle::Watched {
            history_id: "stale-history".to_owned(),
            expiration: Some(UNIX_EPOCH + Duration::from_millis(1_600_000_000_000)),
        };
        let response = GmailWatchResponse {
            history_id: "new-history".to_owned(),
            expiration: Some("1700000000000".to_owned()),
        };

        let mut ownership = WatchOwnership::new(mailbox_watch("commit-moves@push.test"));

        assert!(
            commit_watched(
                &client,
                &shutdown,
                &mut lifecycle,
                &response,
                &mut ownership
            )
            .await
        );

        let WatchLifecycle::Watched {
            history_id,
            expiration,
        } = &lifecycle
        else {
            panic!("watch response must enter Watched");
        };
        assert_eq!(history_id, "new-history");
        assert_eq!(
            *expiration,
            Some(UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)),
            "the exact server-granted expiration, not a locally computed one",
        );
        assert!(
            script.requests().is_empty(),
            "committing a live watch issues no wire traffic"
        );
    }

    /// gmail-G3, the half a `select!` cannot hold. Both request paths run
    /// `users.watch` to completion, so a close that lands while the request
    /// is in flight finds a *created* remote watch. Committing it would leave
    /// a Gmail-side watch nobody renews and no `close()` retires - G3's exact
    /// failure mode surviving G3's fix. The guard must refuse the commit AND
    /// retire the watch it refused.
    #[tokio::test]
    async fn a_watch_that_completes_after_close_is_retired_not_installed() {
        let (client, script) = scripted_client([canned(StatusCode::NO_CONTENT)]);
        let shutdown = CancellationToken::new();
        let mut lifecycle = WatchLifecycle::Renewing;
        let response = GmailWatchResponse {
            history_id: "9".to_owned(),
            expiration: Some("1700000000000".to_owned()),
        };

        let mut ownership = WatchOwnership::new(mailbox_watch("completes-after-close@push.test"));

        shutdown.cancel();
        let committed = commit_watched(
            &client,
            &shutdown,
            &mut lifecycle,
            &response,
            &mut ownership,
        )
        .await;

        assert!(!committed, "a cancelled account must not install Watched");
        assert!(
            matches!(lifecycle, WatchLifecycle::Retired),
            "the refused commit pins Retired so nothing can revive it, got {lifecycle:?}",
        );
        let requests = script.requests();
        assert_eq!(
            requests.len(),
            1,
            "the orphan watch must be retired on the wire"
        );
        assert_eq!(requests[0].url.path(), "/stop");
    }

    /// A lifecycle already `Retired` refuses the commit even with a live
    /// token, so the actor's own state - not only the token - is what makes
    /// the guard unlosable.
    #[tokio::test]
    async fn a_retired_lifecycle_refuses_a_commit_even_with_a_live_token() {
        let (client, script) = scripted_client([canned(StatusCode::NO_CONTENT)]);
        let shutdown = CancellationToken::new();
        let mut lifecycle = WatchLifecycle::Retired;
        let response = GmailWatchResponse {
            history_id: "9".to_owned(),
            expiration: None,
        };

        let mut ownership = WatchOwnership::new(mailbox_watch("retired-lifecycle@push.test"));

        assert!(
            !commit_watched(
                &client,
                &shutdown,
                &mut lifecycle,
                &response,
                &mut ownership
            )
            .await
        );
        assert!(matches!(lifecycle, WatchLifecycle::Retired));
        assert_eq!(script.requests()[0].url.path(), "/stop");
    }

    /// Restores the coverage the actor rewrite dropped: the pre-actor suite
    /// pinned "a renewal in flight at close must not re-issue `users.watch`"
    /// by parking the renewer on the lifecycle mutex. There is no mutex to
    /// park on any more, so the same behaviour is pinned through the actor's
    /// own scheduling: the shutdown arm is biased ahead of the renewal timer,
    /// so a cancelled account retires instead of renewing even when both are
    /// ready in the same poll.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_account_retires_instead_of_renewing() {
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis();
        let (client, script) = scripted_client([
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from(
                    format!("{{\"historyId\":\"1\",\"expiration\":\"{now_millis}\"}}").into_bytes(),
                ),
            },
            // Consumed only by the bug: a renewal that fires anyway.
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from(
                    format!("{{\"historyId\":\"2\",\"expiration\":\"{now_millis}\"}}").into_bytes(),
                ),
            },
        ]);
        let shutdown = CancellationToken::new();
        let pubsub = Arc::new(PubSubControl::new(
            Arc::clone(&client),
            Some(PubSubConfig::new("projects/p/topics/t")),
            shutdown.clone(),
            "cancelled-retires@push.test",
        ));

        push_subscribe(Arc::clone(&pubsub), vec![CursorScope::Account])
            .await
            .expect("subscribe issues the watch");
        assert_eq!(script.requests().len(), 1);

        shutdown.cancel();
        tokio::time::advance(MIN_RENEW_DELAY + Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            script.requests().len(),
            1,
            "a renewal due at close must not re-issue users.watch",
        );
    }

    // ---- shared mailbox watch -------------------------------------------

    fn watch_ok(history_id: &str) -> Canned {
        Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(format!("{{\"historyId\":\"{history_id}\"}}").into_bytes()),
        }
    }

    fn watching_control(client: &Arc<GmailClient>, mailbox: &str) -> Arc<PubSubControl> {
        Arc::new(PubSubControl::new(
            Arc::clone(client),
            Some(PubSubConfig::new("projects/p/topics/t")),
            CancellationToken::new(),
            mailbox,
        ))
    }

    fn stop_paths(script: &ScriptedDispatch) -> usize {
        script
            .requests()
            .iter()
            .filter(|request| request.url.path() == "/stop")
            .count()
    }

    /// The sync engine's reopen subscribes on the replacement BEFORE tearing
    /// down the old account's handle. `users.stop` stops the mailbox's watch,
    /// so the old account's unsubscribe must not send it while the replacement
    /// holds a live watch; the last holder's unsubscribe must.
    ///
    /// Bites on the `others() > 0` early return in `stop_unless_shared`:
    /// without it the first unsubscribe posts `/stop` (and the exhausted
    /// script turns that into an error).
    #[tokio::test]
    async fn unsubscribe_defers_the_stop_while_another_instance_holds_a_watch() {
        let mailbox = "shared-unsubscribe@push.test";
        let (old_client, old_script) = scripted_client([watch_ok("1")]);
        let (new_client, new_script) =
            scripted_client([watch_ok("2"), canned(StatusCode::NO_CONTENT)]);
        let old = watching_control(&old_client, mailbox);
        let replacement = watching_control(&new_client, mailbox);

        let old_handle = push_subscribe(Arc::clone(&old), vec![CursorScope::Account])
            .await
            .expect("old account subscribes");
        let new_handle = push_subscribe(Arc::clone(&replacement), vec![CursorScope::Account])
            .await
            .expect("replacement subscribes");

        push_unsubscribe(Arc::clone(&old), old_handle)
            .await
            .expect("old account's teardown succeeds without a wire stop");
        assert_eq!(
            stop_paths(&old_script),
            0,
            "the old account must not stop the replacement's watch"
        );
        assert_eq!(stop_paths(&new_script), 0);

        push_unsubscribe(Arc::clone(&replacement), new_handle)
            .await
            .expect("last holder stops the watch");
        assert_eq!(
            stop_paths(&new_script),
            1,
            "the last holder's teardown must reach users.stop"
        );
    }

    /// Same contract on the `close()` path.
    ///
    /// Bites on the same `others() > 0` early return: `actor_close` routes
    /// through `stop_exclusive`, and reverting it makes the first close post
    /// `/stop`.
    #[tokio::test]
    async fn close_defers_the_stop_while_another_instance_holds_a_watch() {
        let mailbox = "shared-close@push.test";
        let (old_client, old_script) = scripted_client([watch_ok("1")]);
        let (new_client, new_script) =
            scripted_client([watch_ok("2"), canned(StatusCode::NO_CONTENT)]);
        let old = watching_control(&old_client, mailbox);
        let replacement = watching_control(&new_client, mailbox);
        push_subscribe(Arc::clone(&old), vec![CursorScope::Account])
            .await
            .expect("old account subscribes");
        push_subscribe(Arc::clone(&replacement), vec![CursorScope::Account])
            .await
            .expect("replacement subscribes");

        close_watch(&old).await;
        assert_eq!(stop_paths(&old_script), 0, "shared watch must survive");
        assert!(!old.has_handles().await, "close still clears local handles");

        close_watch(&replacement).await;
        assert_eq!(stop_paths(&new_script), 1, "last holder's close stops");
    }

    /// An instance that holds no watch of its own (a handle persisted across
    /// a restart) still must not stop a live watch another instance in this
    /// process created on the mailbox.
    ///
    /// Bites on the same early return: `others()` counts the live holder
    /// even though this actor never claimed.
    #[tokio::test]
    async fn an_unclaimed_unsubscribe_defers_to_a_live_holder() {
        let mailbox = "shared-unclaimed@push.test";
        let (holder_client, _holder_script) = scripted_client([watch_ok("1")]);
        let (stale_client, stale_script) = scripted_client([canned(StatusCode::NO_CONTENT)]);
        let holder = watching_control(&holder_client, mailbox);
        let stale = watching_control(&stale_client, mailbox);
        push_subscribe(Arc::clone(&holder), vec![CursorScope::Account])
            .await
            .expect("holder subscribes");

        push_unsubscribe(Arc::clone(&stale), encoded_handle())
            .await
            .expect("stale handle teardown succeeds locally");

        assert_eq!(stop_paths(&stale_script), 0);
    }

    /// A stop is decided and sent under the mailbox `exchange` guard, so it
    /// cannot interleave with another instance's `users.watch`.
    ///
    /// Bites on the `exchange.lock()` in `stop_exclusive`: without it the
    /// stop reaches the wire while the test still holds the guard.
    #[tokio::test]
    async fn a_stop_waits_for_the_mailbox_exchange() {
        let mailbox_name = "shared-exchange@push.test";
        let (client, script) = scripted_client([canned(StatusCode::NO_CONTENT)]);
        let control = watching_control(&client, mailbox_name);
        let mailbox = mailbox_watch(mailbox_name);
        let exchange = mailbox.exchange.lock().await;

        let mut unsubscribe = push_unsubscribe(Arc::clone(&control), encoded_handle());
        for _ in 0..8 {
            assert!(
                futures::future::poll_immediate(&mut unsubscribe)
                    .await
                    .is_none(),
                "the stop must wait for the exchange guard"
            );
            tokio::task::yield_now().await;
        }
        assert!(script.requests().is_empty(), "nothing on the wire yet");

        drop(exchange);
        unsubscribe
            .await
            .expect("stop proceeds once the guard drops");
        assert_eq!(stop_paths(&script), 1);
    }

    // ---- config builder -------------------------------------------------

    #[test]
    fn pubsub_config_defaults_to_an_account_wide_watch() {
        let config = PubSubConfig::new("projects/p/topics/t");
        assert_eq!(config.topic, "projects/p/topics/t");
        assert!(
            config.label_ids.is_empty(),
            "an empty label filter means watch the whole account"
        );

        let filtered = PubSubConfig::new("t").with_label_ids(["INBOX", "Label_1"]);
        assert_eq!(filtered.label_ids, vec!["INBOX", "Label_1"]);
    }
}
