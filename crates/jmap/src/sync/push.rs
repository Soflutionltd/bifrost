use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AccountStream, CursorScope, HintPayload,
    InvalidationHint, ObjectType, PushCapability, PushSource, SubscriptionHandle, WatchEvent,
};

use tokio::sync::{Mutex, broadcast, watch};
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::{DataType, PushObject};

pub(crate) type DataTypeSet = HashSet<DataType>;

/// Routing snapshot for push notifications: which JMAP `accountId` is the
/// primary, and which seeded account-level `Folder` cursor scope each
/// foreign (shared/delegate) account's state changes drive.
///
/// RFC 8620 s7.1 keys `StateChange.changed` by `accountId`, so a
/// notification names the account whose state moved. Each foreign account
/// seeds exactly ONE cursor scope (JMAP state is per `(accountId, type)`),
/// so one foreign push is one hint - the map value is a single scope by
/// construction, never a fanout list. The engine's cursor topology is
/// derived from the same seed snapshot at attach, so the two views cannot
/// drift within a session; a share granted after open is invisible to
/// both until reopen (the documented foreign-lifecycle limit).
pub(crate) struct PushRouting {
    primary_account_id: String,
    /// Foreign JMAP `accountId` -> its one seeded `Folder` scope.
    foreign_scopes: HashMap<String, CursorScope>,
}

impl PushRouting {
    pub(crate) fn new<'a>(
        primary_account_id: String,
        seed_scopes: impl Iterator<Item = &'a CursorScope>,
    ) -> Self {
        let mut foreign_scopes: HashMap<String, CursorScope> = HashMap::new();
        for scope in seed_scopes {
            if let CursorScope::Folder(folder) = scope
                && let Some(parsed) = super::foreign::parse_foreign(folder)
            {
                foreign_scopes.insert(parsed.account_id, scope.clone());
            }
        }
        Self {
            primary_account_id,
            foreign_scopes,
        }
    }
}

/// Push state is three values under three locks, and collapsing them into one
/// mutex-protected object was proposed and rejected. Keep the shape.
///
/// The argument for collapsing rested on there being no transaction boundary,
/// and that premise is stale. Subscribe and unsubscribe hold the subscriptions
/// registry, the enabled set and the push position across their sole apply
/// await in ONE lock order (`subscriptions -> enabled -> push_state`) and
/// commit all three only after it succeeds, so cancellation and apply failure
/// mutate nothing; the reader can update the position only after taking that
/// same final guard, so it cannot interleave a new position into an in-flight
/// reconfigure.
///
/// The three also have deliberately different meanings and lifetimes, which a
/// single object would blur rather than encode: the registry is the REQUESTED
/// handle set, `enabled` is the last wire-APPLIED union, and `push_state` is
/// the RFC 8887 position, retained across a non-empty reconfigure and cleared
/// when the union goes empty. Merging their storage would reduce the mutex
/// count without strengthening the existing boundary or capturing those rules
/// in a type, at the price of rewriting test-pinned cancellation and replay
/// machinery with no invariant gap left to close.
pub(crate) struct WsState {
    pub(crate) tx: PushEventSender,
    /// The last union actually applied to the wire. Starts EMPTY at `open()`,
    /// which is why the first reader pass sends a disable frame; see
    /// `apply_push_set`, where that behaviour is explained and ruled correct.
    pub(crate) enabled: Arc<Mutex<DataTypeSet>>,
    pub(crate) push_state: Arc<Mutex<Option<String>>>,
    /// The spawned reader, kept so `close()` can prove it stopped.
    ///
    /// Dropping this handle detaches the task, which is what made
    /// `close()` returning no evidence at all that the reader had ended:
    /// it kept holding client resources for as long as it happened to take
    /// to notice the cancellation. `None` when push is unavailable and no
    /// reader was spawned, and taken by the first `join`.
    reader: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// `ReconnectPolicy::connect_timeout`, carried so `close()` bounds its
    /// teardown awaits by the same configured number the reader bounds its
    /// setup awaits by, without threading the whole policy through the
    /// account constructor.
    teardown_timeout: Duration,
}

/// Push events use broadcast for hints and a watch value for terminal health.
/// Broadcast is still the right fit for lossy hints, but terminal failure must
/// survive both late subscription and a lagged receiver.
#[derive(Clone)]
pub(crate) struct PushEventSender {
    events: broadcast::Sender<WatchEvent>,
    terminal: watch::Sender<Option<AccountError>>,
}

pub(crate) struct PushEventReceiver {
    events: broadcast::Receiver<WatchEvent>,
    terminal: watch::Receiver<Option<AccountError>>,
}

impl PushEventSender {
    fn new(capacity: usize) -> Self {
        let (events, _) = broadcast::channel(capacity);
        let (terminal, _) = watch::channel(None);
        Self { events, terminal }
    }

    pub(crate) fn subscribe(&self) -> PushEventReceiver {
        PushEventReceiver {
            events: self.events.subscribe(),
            terminal: self.terminal.subscribe(),
        }
    }

    fn send(&self, event: WatchEvent) -> Result<usize, broadcast::error::SendError<WatchEvent>> {
        if let WatchEvent::Terminated(error) = &event {
            let _ = self.terminal.send_replace(Some(error.clone()));
        }
        self.events.send(event)
    }
}

// pub: re-exported through crate::sync for AccountFactory reconnect tuning.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    pub initial: Duration,
    pub max: Duration,
    /// Upper bound on each pre-read setup await in the push reader: the
    /// WebSocket handshake and the push re-enable frame that follows it.
    ///
    /// Neither has a protocol-level deadline, and both can park on a
    /// half-open socket indefinitely. Without a bound a wedged handshake
    /// stalls push for the life of the session without ever reaching the
    /// reconnect backoff, and a wedged re-enable holds the client's
    /// WebSocket sink lock, blocking every `push_subscribe` caller too.
    /// Exceeding the bound is treated as a transient disconnect.
    pub connect_timeout: Duration,
    /// How long the read loop tolerates total silence before probing the
    /// link with a WebSocket ping.
    ///
    /// The read loop is where the reader spends essentially all of its
    /// life, and JMAP defines no application-level keepalive: on a
    /// half-open TCP connection (NAT or firewall silently dropping state,
    /// the common fate of a long-lived idle WebSocket) the stream never
    /// errors and never closes, so the reader parks forever and push is
    /// dead for the life of the account with the engine unable to tell it
    /// apart from a quiet mailbox. A ping after this much silence forces
    /// the question; the pong must then arrive within `connect_timeout` or
    /// the connection is treated as a transient disconnect and rebuilt.
    pub keepalive: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(30),
            keepalive: Duration::from_secs(120),
        }
    }
}

/// The client operations the push reader awaits outside its read loop.
///
/// WebSocket push remains the reqwest production specialization, so this
/// narrow trait makes the reader's shutdown and timeout behavior testable
/// without a socket. It carries nothing the reader does not already call.
pub(crate) trait PushTransport: Send + Sync + 'static {
    type Stream: futures::Stream<Item = crate::Result<crate::client_ws::WebSocketMessage>>
        + Send
        + Unpin;

    /// Perform the WebSocket handshake and yield the message stream.
    fn connect_push(&self) -> impl Future<Output = crate::Result<Self::Stream>> + Send;

    /// Apply `data_types` as the connection's push subscription.
    ///
    /// Returns the crate error unconverted, because the callers do not all
    /// treat it alike: `unsubscribe` reads a failed sink write as a
    /// completed removal (see there), `subscribe` reports it through
    /// `push_set_error`, and the reader only needs to know it failed.
    fn set_push_data_types(
        &self,
        data_types: &DataTypeSet,
        push_state: Option<String>,
    ) -> impl Future<Output = crate::Result<()>> + Send;

    /// Send a WebSocket ping on the current connection.
    ///
    /// The reader's only liveness probe: the answering pong is the sole
    /// evidence a silent connection is still carrying bytes.
    fn ping_push(&self) -> impl Future<Output = crate::Result<()>> + Send;
}

type BoxedWsStream =
    Pin<Box<dyn futures::Stream<Item = crate::Result<crate::client_ws::WebSocketMessage>> + Send>>;

impl PushTransport for Client {
    type Stream = BoxedWsStream;

    async fn connect_push(&self) -> crate::Result<Self::Stream> {
        let stream: Self::Stream = self.connect_ws().await?;
        Ok(stream)
    }

    fn set_push_data_types(
        &self,
        data_types: &DataTypeSet,
        push_state: Option<String>,
    ) -> impl Future<Output = crate::Result<()>> + Send {
        apply_push_set(self, data_types, push_state)
    }

    fn ping_push(&self) -> impl Future<Output = crate::Result<()>> + Send {
        self.ws_ping()
    }
}

impl WsState {
    pub(crate) fn spawn(
        client: Client,
        push_available: bool,
        shutdown: CancellationToken,
        policy: ReconnectPolicy,
        routing: Arc<PushRouting>,
    ) -> Self {
        let tx = PushEventSender::new(256);
        let enabled = Arc::new(Mutex::new(HashSet::new()));
        let push_state = Arc::new(Mutex::new(None));
        let reader = push_available.then(|| {
            tokio::spawn(reader_loop(
                client,
                tx.clone(),
                Arc::clone(&enabled),
                Arc::clone(&push_state),
                shutdown,
                policy,
                routing,
            ))
        });

        Self {
            tx,
            enabled,
            push_state,
            reader: Arc::new(Mutex::new(reader)),
            teardown_timeout: policy.connect_timeout,
        }
    }

    /// A handle onto the reader task, for `close()` to await.
    pub(crate) fn reader_handle(&self) -> Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> {
        Arc::clone(&self.reader)
    }

    pub(crate) fn teardown_timeout(&self) -> Duration {
        self.teardown_timeout
    }
}

/// Wait for the spawned reader to finish, up to `timeout`.
///
/// Called after the shutdown token is cancelled. Every await the reader
/// performs is cancellation-covered, so this normally returns immediately;
/// the bound is there so a wedged reader cannot hold `close()` - and with
/// it the engine's teardown/reopen - open indefinitely. A reader that
/// outlives the bound is abandoned rather than aborted: aborting it at an
/// arbitrary await point is no safer than leaving it to notice the token,
/// and `close()` has already given up on it either way.
pub(crate) async fn join_reader(
    reader: &Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    timeout: Duration,
) {
    let handle = reader.lock().await.take();
    if let Some(handle) = handle {
        let _ = tokio::time::timeout(timeout, handle).await;
    }
}

pub(crate) fn stream(
    mut rx: PushEventReceiver,
    shutdown: CancellationToken,
) -> AccountStream<WatchEvent> {
    Box::pin(async_stream::stream! {
        let terminal = { rx.terminal.borrow_and_update().clone() };
        if let Some(error) = terminal {
            yield WatchEvent::Terminated(error);
            return;
        }

        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                result = rx.events.recv() => {
                    match result {
                        Ok(event) => {
                            let terminal = matches!(event, WatchEvent::Terminated(_));
                            yield event;
                            if terminal {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            let terminal = { rx.terminal.borrow_and_update().clone() };
                            if let Some(error) = terminal {
                                yield WatchEvent::Terminated(error);
                                break;
                            } else {
                                yield WatchEvent::Invalidated {
                                    hint: InvalidationHint {
                                        source: PushSource::Coalesced,
                                        payload: HintPayload::Unknown,
                                    },
                                };
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                changed = rx.terminal.changed() => {
                    match changed {
                        Ok(()) => {
                            let terminal = { rx.terminal.borrow_and_update().clone() };
                            if let Some(error) = terminal {
                                yield WatchEvent::Terminated(error);
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    })
}

pub(crate) fn subscribe<T: PushTransport>(
    client: T,
    push: PushCapability,
    handle: SubscriptionHandle,
    scopes: Vec<CursorScope>,
    subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, DataTypeSet>>>,
    enabled: Arc<Mutex<DataTypeSet>>,
    push_state: Arc<Mutex<Option<String>>>,
) -> AccountFuture<Result<(SubscriptionHandle, Vec<bool>), AccountError>> {
    Box::pin(async move {
        if push != PushCapability::InProcess {
            return Err(super::error::unsupported_error(
                AccountOperation::PushSubscribe,
                None,
                "JMAP push requires WebSocket subprotocol support",
            ));
        }

        let accepted = scopes
            .iter()
            .map(|scope| data_type_for_scope(scope).is_some())
            .collect::<Vec<_>>();
        let data_types = scopes
            .iter()
            .filter_map(data_type_for_scope)
            .collect::<DataTypeSet>();

        if data_types.is_empty() {
            return Err(super::error::no_mappable_push_scopes());
        }

        // Lock order is subscriptions -> enabled -> push_state everywhere that
        // takes more than one, and every commit happens after the frame send
        // succeeds. Holding all three across the send is what makes a
        // cancelled subscribe future commit nothing at all.
        let mut subscriptions_guard = subscriptions.lock().await;
        let mut enabled_guard = enabled.lock().await;
        let mut push_state_guard = push_state.lock().await;
        let mut prospective = subscriptions_guard.clone();
        prospective.insert(handle.clone(), data_types.clone());
        let union = union_data_types(&prospective);
        match client
            .set_push_data_types(&union, push_state_guard.clone())
            .await
        {
            Ok(()) => {}
            // No connection yet: the sink is unset only before the reader's
            // first handshake and is never cleared, and the capability check
            // above already refused a server without WebSocket support. So
            // this is the reader not having connected YET, not a refusal.
            // Commit the desired set as a deferred success: the reader
            // applies the committed `enabled` union and `push_state` on every
            // connect, and it reads them only after installing its sink -
            // behind the guards this call holds until the commit - so it
            // cannot apply the set from before this subscribe.
            //
            // Reporting it as a terminal `Unsupported` cost whole accounts:
            // the engine's reopen aborts on any error from the replacement's
            // subscribe, so a WebSocket endpoint slow to answer during reopen
            // could exhaust `restart_account`'s attempts and pause the
            // account, HTTP sync included.
            //
            // A dead sink (`WebSocketSend`) is the same deferred success. In
            // tokio-websockets every send error leaves the stream closed for
            // good: `start_send` fails only with `AlreadyClosed`, and a
            // failed flush sets `CloseAcknowledged`, which nothing resets to
            // `Active`. The writer's failure does not wake the reader, but
            // its keepalive ping then fails and it reconnects, re-applying
            // the committed `enabled` set under the same guard ordering as
            // above. The cost is that push for this subscription can start up
            // to one keepalive interval late. Re-verify this if
            // tokio-websockets is bumped.
            Err(crate::Error::WebSocketNotConnected | crate::Error::WebSocketSend(_)) => {}
            Err(err) => return Err(push_set_error(err)),
        }
        subscriptions_guard.insert(handle.clone(), data_types);
        commit_push_set(&mut enabled_guard, &mut push_state_guard, union);
        Ok((handle, accepted))
    })
}

/// Tear down a subscription handle.
///
/// This crate's push is RFC 8887 WebSocket enable/disable only: a handle is a
/// key into the in-memory `subscriptions` registry, and the server-side state
/// it stands for (the enabled data-type set) belongs to the socket and dies
/// with it. No RFC 8620 section 7.2 `PushSubscription` object is ever created
/// (the `push_subscription` module is an unwired wire-type surface), so there
/// is nothing that outlives a connection for a handle to name.
///
/// That is why the engine's retry of an orphaned handle against a NEWER
/// account instance is correct and deletes nothing: the handle is unknown to
/// the new instance's registry, `prospective` equals the current registry, and
/// the frame re-sends the union the new connection already holds. It is
/// idempotent, and no other subscription is narrowed. If a
/// `PushSubscription`-backed mode is ever added, the handle must carry the
/// server id so any instance can destroy it.
pub(crate) fn unsubscribe<T: PushTransport>(
    client: T,
    handle: SubscriptionHandle,
    subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, DataTypeSet>>>,
    enabled: Arc<Mutex<DataTypeSet>>,
    push_state: Arc<Mutex<Option<String>>>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mut subscriptions_guard = subscriptions.lock().await;
        let mut enabled_guard = enabled.lock().await;
        let mut push_state_guard = push_state.lock().await;
        let mut prospective = subscriptions_guard.clone();
        prospective.remove(&handle);
        let union = union_data_types(&prospective);
        match client
            .set_push_data_types(&union, push_state_guard.clone())
            .await
        {
            Ok(()) => {}
            // The frame could not be written because the connection is
            // finished: every failed sink write leaves the stream closed
            // for good. RFC 8887 push is enabled per connection, so the
            // subscription this frame would have narrowed exists nowhere
            // any more, and the removal is complete rather than pending.
            // Commit it: the reader re-applies the committed `enabled`
            // union on its next connection. It cannot apply a stale one:
            // the send found the dead sink, so the reader had not yet
            // installed its next connection, and its re-enable reads
            // `enabled` only after installing it - behind the guard this
            // call holds until the commit.
            //
            // Reporting this as a failure lost subscriptions. The engine's
            // reopen tears down each old handle on the old account before
            // cutover; after a WebSocket drop that account's sink is dead,
            // the teardown failed, the reopen aborted, and the record was
            // marked `teardown_unconfirmed` - which the next attempt
            // carries but never recreates, so the consumer's subscription
            // silently stopped existing on the replacement.
            //
            // No connection yet is the same answer, for a simpler reason:
            // before the first handshake no subscription exists anywhere, so
            // there is nothing to narrow. See `subscribe`.
            Err(crate::Error::WebSocketSend(_) | crate::Error::WebSocketNotConnected) => {}
            Err(err) => return Err(push_set_error(err)),
        }
        subscriptions_guard.remove(&handle);
        commit_push_set(&mut enabled_guard, &mut push_state_guard, union);
        Ok(())
    })
}

/// Commit a newly applied data-type union.
///
/// The RFC 8887 `pushState` we hold is the last position the server
/// acknowledged for a LIVE subscription. Reconfiguring the data-type set keeps
/// that position - it is what stops changes around the reconfigure from being
/// missed - but disabling push retires the subscription the position belongs
/// to, so the cached value must be dropped. Keeping it would let a later
/// re-enable replay a position minted under an abandoned configuration, which
/// is a wrong replay rather than merely a cold start.
fn commit_push_set(enabled: &mut DataTypeSet, push_state: &mut Option<String>, union: DataTypeSet) {
    if union.is_empty() {
        *push_state = None;
    }
    *enabled = union;
}

fn data_type_for_scope(scope: &CursorScope) -> Option<DataType> {
    match scope {
        CursorScope::Type(ObjectType::Email) => Some(DataType::Email),
        // Push subscriptions are per data type, not per mailbox. An Email
        // subscription covers every account visible to this session,
        // including delegate/shared accounts represented by Folder scopes.
        CursorScope::Folder(_) => Some(DataType::Email),
        CursorScope::Type(ObjectType::Mailbox) => Some(DataType::Mailbox),
        _ => None,
    }
}

fn scope_for_data_type(data_type: &DataType) -> Option<CursorScope> {
    match data_type {
        DataType::Email => Some(CursorScope::Type(ObjectType::Email)),
        // RFC 8621's EmailDelivery state changes only when a new Email is
        // added, which also changes Email state. Email is the tracked
        // cursor; emitting a second Unknown hint would broaden that signal.
        DataType::EmailDelivery => Some(CursorScope::Type(ObjectType::Email)),
        DataType::Mailbox => Some(CursorScope::Type(ObjectType::Mailbox)),
        _ => None,
    }
}

fn union_data_types(subscriptions: &HashMap<SubscriptionHandle, DataTypeSet>) -> DataTypeSet {
    let mut union = HashSet::new();
    for values in subscriptions.values() {
        for value in values {
            union.insert(value.clone());
        }
    }
    union
}

/// An empty set maps to a disable frame, including on the FIRST reader pass,
/// where `enabled` is still empty from `open()` and nobody had enabled push.
/// So a freshly opened connection sends a disable nobody asked for and then
/// announces `Reconnected`. That is intended and was rejected as a defect;
/// leave it.
///
/// `WatchEvent::Reconnected` is defined in `bifrost-types` as a connection
/// HEALTH transition, not as proof that any scope is subscribed, and the empty
/// set is the applied desired state - the frame makes that state explicit on
/// every newly opened connection rather than leaving it implied. The sync
/// reconciler deliberately turns every reconnect, this first one included,
/// into a full `Unknown` reconcile, so suppressing the event or skipping the
/// frame would give the same transition a second meaning and make the reader's
/// applied-state guarantee conditional on which pass it is. What remains is
/// one frame of cosmetic wire traffic with no incorrect state behind it.
async fn apply_push_set(
    client: &Client,
    data_types: &DataTypeSet,
    push_state: Option<String>,
) -> crate::Result<()> {
    if data_types.is_empty() {
        client.disable_push_ws().await
    } else {
        let values = data_types.iter().cloned().collect::<Vec<_>>();
        client.enable_push_ws(Some(values), push_state).await
    }
}

/// Classify a failed push-set frame for the consumer.
///
/// The frame is replay-safe, whatever the central table says about
/// `PushSubscribe`. That operation is non-idempotent because on webhook
/// providers a subscribe CREATES a server resource, and replaying it after
/// an in-flight drop can create a second one. A JMAP `WebSocketPushEnable`
/// or `WebSocketPushDisable` creates nothing: it carries the complete
/// desired data-type set for the connection, so applying it twice leaves
/// the same state as applying it once. Without the override an in-flight
/// sink failure derives `Reconcile(TransportDropAfterSend, [CheckTarget])`,
/// which asks the consumer to read back a target that does not exist.
///
/// `WebSocketNotConnected` never reaches here: both callers absorb it as a
/// deferred success.
fn push_set_error(err: crate::Error) -> AccountError {
    super::error::into_account_error(
        err,
        super::error::JmapErrorContext::new(AccountOperation::PushSubscribe),
    )
    .into_builder()
    .idempotency_override(true)
    .try_build()
    .expect("valid account error classification")
}

/// How one connect-read pass of the reader ended.
enum ReaderStep {
    /// Shutdown was requested. Stop without emitting anything further.
    Stop,
    /// Non-Retry classification: emit `Terminated` and stop the reader so
    /// the engine can apply the classified recovery.
    Terminal(AccountError),
    /// Transient: emit `Disconnected` and back off. `reset_backoff` is
    /// set only when the pass actually read a message off the connection,
    /// which is the evidence that the endpoint is healthy and the backoff
    /// should start over. Merely reaching the read loop is not: the
    /// push-enable rejection arrives there as a `RequestError`.
    Retry { reset_backoff: bool },
}

fn classified_reader_failure(error: crate::Error, reset_backoff: bool) -> ReaderStep {
    let error = super::error::into_account_error(
        error,
        super::error::JmapErrorContext::new(AccountOperation::PushStream),
    );
    if error.recovery().is_retryable() {
        ReaderStep::Retry { reset_backoff }
    } else {
        ReaderStep::Terminal(error)
    }
}

/// Await `future` under the reader's shutdown token and a wall-clock
/// bound, yielding `None` if either fires first.
///
/// Every await the reader performs before it reaches its (already
/// cancellation-covered) read loop goes through here. Dropping the
/// in-flight future is what makes `close()` prompt: otherwise a handshake
/// against a black-holed peer, or a re-enable frame on a half-open
/// socket, keeps the detached reader task and the client resources it
/// borrows alive long after the account is gone.
async fn bounded<F: Future>(
    shutdown: &CancellationToken,
    timeout: Duration,
    future: F,
) -> Option<F::Output> {
    tokio::select! {
        () = shutdown.cancelled() => None,
        () = tokio::time::sleep(timeout) => None,
        output = future => Some(output),
    }
}

/// Disambiguate a `bounded` miss: cancellation stops the reader, a
/// timeout is just another transient failure to reconnect through.
fn interrupted(shutdown: &CancellationToken) -> ReaderStep {
    if shutdown.is_cancelled() {
        ReaderStep::Stop
    } else {
        ReaderStep::Retry {
            reset_backoff: false,
        }
    }
}

async fn reader_loop<T: PushTransport>(
    transport: T,
    tx: PushEventSender,
    enabled: Arc<Mutex<DataTypeSet>>,
    push_state: Arc<Mutex<Option<String>>>,
    shutdown: CancellationToken,
    policy: ReconnectPolicy,
    routing: Arc<PushRouting>,
) {
    let mut backoff = policy.initial;
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        match reader_pass(
            &transport,
            &tx.events,
            &enabled,
            &push_state,
            &shutdown,
            policy,
            &routing,
        )
        .await
        {
            ReaderStep::Stop => break,
            ReaderStep::Terminal(err) => {
                let _ = tx.send(WatchEvent::Terminated(err));
                break;
            }
            ReaderStep::Retry { reset_backoff } => {
                if reset_backoff {
                    backoff = policy.initial;
                }
                let _ = tx.send(WatchEvent::Disconnected);
            }
        }

        tokio::select! {
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = std::cmp::min(backoff.saturating_mul(2), policy.max);
    }
}

/// Connect, re-apply the push subscription, and read until the link ends.
async fn reader_pass<T: PushTransport>(
    transport: &T,
    tx: &broadcast::Sender<WatchEvent>,
    enabled: &Arc<Mutex<DataTypeSet>>,
    push_state: &Arc<Mutex<Option<String>>>,
    shutdown: &CancellationToken,
    policy: ReconnectPolicy,
    routing: &PushRouting,
) -> ReaderStep {
    let mut ws = match bounded(shutdown, policy.connect_timeout, transport.connect_push()).await {
        None => return interrupted(shutdown),
        // Only Retry is local; the reconciler must receive engine directives.
        Some(Err(err)) => return classified_reader_failure(err, false),
        Some(Ok(ws)) => ws,
    };

    // A re-enable that never completes means the sink is wedged even
    // though the handshake answered; one that fails FAST means the link
    // died between handshake and subscribe. A server-side rejection of
    // the frame never fails it - that arrives as a `RequestError` on the
    // read stream instead. Either way this connection carries no applied
    // subscription, so reading from it would announce `Reconnected` and
    // then fall silent: a dead-push state the engine cannot tell apart
    // from a quiet mailbox. Drop the connection and reconnect.
    //
    // Classify even setup writes: only Retry is safe to keep local. The
    // transport seam can also report capability or session failures here.
    match bounded(
        shutdown,
        policy.connect_timeout,
        reenable_current_push_set(transport, enabled, push_state),
    )
    .await
    {
        None => return interrupted(shutdown),
        Some(Err(err)) => return classified_reader_failure(err, false),
        Some(Ok(())) => {}
    }

    let _ = tx.send(WatchEvent::Reconnected);

    // Whether this pass ever read a message SUCCESSFULLY. It gates the
    // backoff reset below: reaching the read loop is not evidence the
    // endpoint is healthy, because the push-enable frame's only failure
    // signal is a `RequestError` arriving asynchronously on this stream.
    // A server that will never accept the subscription (unknown dataTypes
    // value, capability withdrawn, quota) therefore completes handshake and
    // sink write, gets `Reconnected` announced, answers `RequestError`, and
    // - if the mere fact of having reached the loop reset the backoff -
    // does it all again one second later, forever: an unbounded 1 Hz
    // handshake storm plus a Disconnected/Reconnected pair per second on
    // the broadcast channel. Only traffic the peer actually served counts.
    let mut saw_traffic = false;
    // Set once a ping is outstanding, so the next silence window is the
    // pong deadline rather than another keepalive interval.
    let mut awaiting_pong = false;

    loop {
        let silence = if awaiting_pong {
            policy.connect_timeout
        } else {
            policy.keepalive
        };
        let message = tokio::select! {
            () = shutdown.cancelled() => return ReaderStep::Stop,
            () = tokio::time::sleep(silence) => {
                if awaiting_pong {
                    // Pinged and got nothing back inside the deadline: the
                    // socket is half-open. Drop it and reconnect rather
                    // than park on a link that will never speak again.
                    break;
                }
                match bounded(shutdown, policy.connect_timeout, transport.ping_push()).await {
                    None => return interrupted(shutdown),
                    // A ping that cannot even be written means the sink is
                    // already gone; classify before deciding to retry.
                    Some(Err(err)) => return classified_reader_failure(err, saw_traffic),
                    Some(Ok(())) => {}
                }
                awaiting_pong = true;
                continue;
            }
            message = futures::StreamExt::next(&mut ws) => message,
        };
        let Some(message) = message else {
            break;
        };
        match message {
            // Any successful frame - a pong included - proves the link is
            // carrying bytes, so it clears the outstanding probe.
            Ok(crate::client_ws::WebSocketMessage::PushNotification(push)) => {
                saw_traffic = true;
                awaiting_pong = false;
                remember_push_state(&push, push_state).await;
                emit_push(push, tx, routing);
            }
            Ok(
                crate::client_ws::WebSocketMessage::Response { .. }
                | crate::client_ws::WebSocketMessage::IgnoredFrame
                | crate::client_ws::WebSocketMessage::Pong,
            ) => {
                saw_traffic = true;
                awaiting_pong = false;
            }
            Ok(crate::client_ws::WebSocketMessage::UnknownFrame) => {
                // A malformed known push may have hidden a state change, so
                // keep the link alive while asking the engine for a broad
                // reconcile.
                tracing::warn!("malformed JMAP push frame; emitting unknown invalidation");
                saw_traffic = true;
                awaiting_pong = false;
                let _ = tx.send(invalidated(HintPayload::Unknown));
            }
            Err(err) => {
                // Classify every exit error so consumers learn whether
                // the loop ended for an auth-lost / schema-mismatch
                // reason or for a transient drop. The previous shape
                // (`Err(_) => break`) erased the signal.
                let acct = super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(AccountOperation::PushStream),
                );
                if !acct.recovery().is_retryable() {
                    return ReaderStep::Terminal(acct);
                }
                break;
            }
        }
    }

    if shutdown.is_cancelled() {
        return ReaderStep::Stop;
    }
    ReaderStep::Retry {
        reset_backoff: saw_traffic,
    }
}

/// The `enabled` and `push_state` guards are taken in SEPARATE critical
/// sections here, and that is a known, examined residual rather than an
/// oversight. Do not "fix" it by holding the first guard across the
/// `set_push_data_types` await.
///
/// The mutators (subscribe, unsubscribe) hold the registry, enabled-set and
/// push-position guards across their sole apply await and commit all three
/// only on success, in one lock order, so a cancelled mutator mutates nothing.
/// The reader is the only non-atomic pair, and its worst case is bounded: if a
/// mutator commits between the two reads, the replay can re-apply a union the
/// mutators have already superseded - after a racing final unsubscribe, the
/// wire briefly carries a subscription the state says is empty, with a `None`
/// position. That costs extra push frames and spurious invalidation hints and
/// nothing else. It can never MISS a change, because hints are
/// over-approximate by contract and the sync reconciler treats every reconnect
/// as a full `Unknown` reconcile regardless; and it self-heals on the next
/// reconnect or reconfigure, whose frames go out under all three guards.
///
/// Closing it means a lock held across an await in teardown-adjacent code,
/// which is the exact shape that has opened a new hole every time it has been
/// attempted here. The cost of the fix exceeds the cost of the residual.
async fn reenable_current_push_set<T: PushTransport>(
    transport: &T,
    enabled: &Arc<Mutex<DataTypeSet>>,
    push_state: &Arc<Mutex<Option<String>>>,
) -> crate::Result<()> {
    let current = {
        let guard = enabled.lock().await;
        guard.clone()
    };
    let push_state = push_state.lock().await.clone();
    transport.set_push_data_types(&current, push_state).await
}

async fn remember_push_state(push: &PushObject, state: &Arc<Mutex<Option<String>>>) {
    if let Some(value) = last_push_state(push) {
        *state.lock().await = Some(value.to_string());
    }
}

fn last_push_state(push: &PushObject) -> Option<&str> {
    match push {
        PushObject::StateChange { push_state, .. } => push_state.as_deref(),
        PushObject::Group { entries } => entries.iter().filter_map(last_push_state).next_back(),
        _ => None,
    }
}

fn emit_push(push: PushObject, tx: &broadcast::Sender<WatchEvent>, routing: &PushRouting) {
    match push {
        PushObject::StateChange { changed, .. } => {
            // RFC 8620 s7.1: `changed` is keyed by `accountId`. Route each
            // entry to the cursor scopes that account actually drives
            // instead of collapsing every account onto the primary type
            // scopes.
            for (account_id, by_type) in &changed {
                let has_email_state = by_type.contains_key(&DataType::Email);
                for data_type in by_type.keys() {
                    // RFC 8621 advances EmailDelivery when a new Email is
                    // added, which also advances Email. If a server includes
                    // both keys, the Email hint already covers the same
                    // tracked cursor; do not emit it twice.
                    if has_email_state && matches!(data_type, DataType::EmailDelivery) {
                        continue;
                    }
                    emit_state_change(account_id, data_type, tx, routing);
                }
            }
        }
        PushObject::Group { entries } => {
            for entry in entries {
                emit_push(entry, tx, routing);
            }
        }
        _ => {
            let _ = tx.send(invalidated(HintPayload::Unknown));
        }
    }
}

fn emit_state_change(
    account_id: &str,
    data_type: &DataType,
    tx: &broadcast::Sender<WatchEvent>,
    routing: &PushRouting,
) {
    if account_id == routing.primary_account_id {
        if let Some(scope) = scope_for_data_type(data_type) {
            let _ = tx.send(invalidated(HintPayload::SpecificCursorScope(scope)));
        } else if !matches!(data_type, DataType::Thread) {
            // Preserve broad invalidation for unmapped types: this client
            // cannot know which tracked scope their state may affect. RFC
            // 8887 filters conforming notifications to requested types, so
            // an extra untracked state is not part of the normal Email push.
            // Thread is known but has no inventory or changes stream.
            let _ = tx.send(invalidated(HintPayload::Unknown));
        }
        return;
    }
    let Some(scope) = routing.foreign_scopes.get(account_id) else {
        // An accountId this session never seeded: a probe-skipped share
        // or one granted after open. JMAP state is per-(accountId, type),
        // so no registered cursor's state can have moved - there is
        // nothing to invalidate, and a broad repoll would gain nothing.
        return;
    };
    if !matches!(data_type, DataType::Email | DataType::EmailDelivery) {
        // Foreign Mailbox / Thread state is tracked by no cursor: foreign
        // mailbox lifecycle is reopen-only by design and no foreign
        // Thread scope exists. A count-only Mailbox bump arrives alongside
        // the Email entry that caused it, which is routed below; degrading
        // to `Unknown` here would full-repoll the whole account on every
        // foreign delivery and defeat the narrow routing.
        return;
    }
    // A foreign account's Email state is account-wide and the account
    // syncs as exactly one account-level `Folder` cursor scope, so one
    // push is one hint. The engine's reconciler skips a scope without a
    // registered cursor, so a hint for a quarantined scope is a no-op.
    let _ = tx.send(invalidated(HintPayload::SpecificCursorScope(scope.clone())));
}

fn invalidated(payload: HintPayload) -> WatchEvent {
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source: PushSource::JmapStateChange,
            payload,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Mutex as StdMutex;

    fn drain(rx: &mut broadcast::Receiver<WatchEvent>) -> Vec<WatchEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    fn state_change(account_id: &str, data_type: DataType, state: &str) -> PushObject {
        let mut changed = HashMap::new();
        let mut by_type = HashMap::new();
        by_type.insert(data_type, state.to_string());
        changed.insert(account_id.to_string(), by_type);
        PushObject::StateChange {
            changed,
            push_state: None,
        }
    }

    fn foreign_scope(account_id: &str) -> CursorScope {
        CursorScope::Folder(super::super::foreign::encode_foreign_account(account_id))
    }

    /// A primary account plus one seeded foreign account - the same
    /// snapshot `factory.rs::open` builds from `seed_states`: exactly one
    /// account-level `Folder` scope per share, however many mailboxes it
    /// holds.
    fn routing() -> PushRouting {
        let seeds = [foreign_scope("acct-foreign")];
        PushRouting::new("acct-primary".to_string(), seeds.iter())
    }

    fn hinted_scope(event: &WatchEvent) -> Option<CursorScope> {
        match event {
            WatchEvent::Invalidated { hint } => match &hint.payload {
                HintPayload::SpecificCursorScope(scope) => Some(scope.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    #[test]
    fn a_primary_email_state_change_invalidates_the_email_cursor_scope() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-primary", DataType::Email, "s2"),
            &tx,
            &routing(),
        );

        let events = drain(&mut rx);
        assert_eq!(events.len(), 1);
        match &events[0] {
            WatchEvent::Invalidated { hint } => {
                assert_eq!(hint.source, PushSource::JmapStateChange);
            }
            other => panic!("expected Invalidated, got {other:?}"),
        }
        assert_eq!(
            hinted_scope(&events[0]),
            Some(CursorScope::Type(ObjectType::Email))
        );
    }

    #[test]
    fn an_untracked_primary_data_type_degrades_to_an_unknown_hint() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-primary", DataType::Principal, "s2"),
            &tx,
            &routing(),
        );

        let events = drain(&mut rx);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            WatchEvent::Invalidated { hint }
                if matches!(&hint.payload, HintPayload::Unknown)
        ));
    }

    #[test]
    fn email_delivery_invalidates_the_email_scope_without_a_broad_hint() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-primary", DataType::EmailDelivery, "d2"),
            &tx,
            &routing(),
        );

        let events = drain(&mut rx);
        assert_eq!(events.len(), 1);
        assert_eq!(
            hinted_scope(&events[0]),
            Some(CursorScope::Type(ObjectType::Email))
        );
    }

    #[test]
    fn email_and_email_delivery_in_one_change_emit_one_email_hint() {
        let (tx, mut rx) = broadcast::channel(8);
        let mut by_type = HashMap::new();
        by_type.insert(DataType::Email, "e2".to_string());
        by_type.insert(DataType::EmailDelivery, "d2".to_string());
        let mut changed = HashMap::new();
        changed.insert("acct-primary".to_string(), by_type);

        emit_push(
            PushObject::StateChange {
                changed,
                push_state: None,
            },
            &tx,
            &routing(),
        );

        let events = drain(&mut rx);
        assert_eq!(events.len(), 1);
        assert_eq!(
            hinted_scope(&events[0]),
            Some(CursorScope::Type(ObjectType::Email))
        );
    }

    #[test]
    fn thread_state_changes_are_dropped_without_a_thread_cursor() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-primary", DataType::Thread, "t2"),
            &tx,
            &routing(),
        );

        assert!(drain(&mut rx).is_empty());
        assert_eq!(
            data_type_for_scope(&CursorScope::Type(ObjectType::Thread)),
            None
        );
    }

    #[test]
    fn a_grouped_push_fans_out_to_one_event_per_entry() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            PushObject::Group {
                entries: vec![
                    state_change("acct-primary", DataType::Email, "s2"),
                    state_change("acct-primary", DataType::Mailbox, "m2"),
                ],
            },
            &tx,
            &routing(),
        );

        let events = drain(&mut rx);
        assert_eq!(events.len(), 2);
        let scopes = events.iter().filter_map(hinted_scope).collect::<Vec<_>>();
        assert!(scopes.contains(&CursorScope::Type(ObjectType::Email)));
        assert!(scopes.contains(&CursorScope::Type(ObjectType::Mailbox)));
    }

    // RFC 8620 s7.1 keys `StateChange.changed` by `accountId`. A foreign
    // account's Email state is account-wide and the account seeds exactly
    // one account-level `Folder` cursor scope, so a foreign Email change
    // is exactly ONE hint - not a per-mailbox fanout - and must NOT touch
    // the primary `Type(Email)` scope, which did not change.
    #[test]
    fn a_foreign_account_email_change_is_one_hint_for_its_account_scope() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-foreign", DataType::Email, "s2"),
            &tx,
            &routing(),
        );

        let events = drain(&mut rx);
        let scopes = events.iter().filter_map(hinted_scope).collect::<Vec<_>>();
        assert_eq!(scopes, vec![foreign_scope("acct-foreign")]);
        assert!(
            !scopes.contains(&CursorScope::Type(ObjectType::Email)),
            "the primary scope did not change and must not be repolled"
        );
    }

    #[test]
    fn a_foreign_email_delivery_change_uses_its_account_scope() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-foreign", DataType::EmailDelivery, "d2"),
            &tx,
            &routing(),
        );

        let events = drain(&mut rx);
        let scopes = events.iter().filter_map(hinted_scope).collect::<Vec<_>>();
        assert_eq!(scopes, vec![foreign_scope("acct-foreign")]);
    }

    // Foreign Mailbox state is tracked by no cursor (foreign mailbox
    // lifecycle is reopen-only by design). Count-only Mailbox bumps ride
    // alongside the Email entry that caused them, so emitting anything
    // here - especially `Unknown`, a full-account repoll - would be
    // noise on every foreign delivery.
    #[test]
    fn a_foreign_mailbox_change_emits_no_hint() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-foreign", DataType::Mailbox, "m2"),
            &tx,
            &routing(),
        );

        assert!(drain(&mut rx).is_empty());
    }

    // An accountId this session never seeded (probe-skipped share, or
    // one granted after open) drives no registered cursor: JMAP state is
    // per-(accountId, type), so nothing this session tracks can have
    // moved.
    #[test]
    fn an_unseeded_account_change_emits_no_hint() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            state_change("acct-stranger", DataType::Email, "s2"),
            &tx,
            &routing(),
        );

        assert!(drain(&mut rx).is_empty());
    }

    #[test]
    fn a_mixed_group_routes_each_entry_to_its_own_account() {
        let (tx, mut rx) = broadcast::channel(8);
        emit_push(
            PushObject::Group {
                entries: vec![
                    state_change("acct-primary", DataType::Email, "s2"),
                    state_change("acct-foreign", DataType::Email, "f2"),
                ],
            },
            &tx,
            &routing(),
        );

        let scopes = drain(&mut rx)
            .iter()
            .filter_map(hinted_scope)
            .collect::<Vec<_>>();
        assert_eq!(scopes.len(), 2);
        assert!(scopes.contains(&CursorScope::Type(ObjectType::Email)));
        assert!(scopes.contains(&foreign_scope("acct-foreign")));
    }

    // `PushRouting::new` buckets only foreign-encoded `Folder` scopes;
    // primary `Type(_)` seeds and a codec-less `Folder` id contribute
    // nothing.
    #[test]
    fn push_routing_buckets_only_foreign_folder_scopes() {
        let seeds = [
            CursorScope::Type(ObjectType::Email),
            CursorScope::Folder(bifrost_types::FolderId("no-separator".to_string())),
            foreign_scope("acct-a"),
            foreign_scope("acct-b"),
        ];
        let routing = PushRouting::new("acct-primary".to_string(), seeds.iter());
        assert_eq!(routing.foreign_scopes.len(), 2);
        assert_eq!(routing.foreign_scopes["acct-a"], foreign_scope("acct-a"));
        assert_eq!(routing.foreign_scopes["acct-b"], foreign_scope("acct-b"));
    }

    #[test]
    fn a_foreign_folder_scope_maps_to_email_push_data_type() {
        let folder = foreign_scope("acct-9");
        assert_eq!(data_type_for_scope(&folder), Some(DataType::Email));

        // Primary type scopes do map.
        assert_eq!(
            data_type_for_scope(&CursorScope::Type(ObjectType::Email)),
            Some(DataType::Email)
        );
        assert_eq!(
            data_type_for_scope(&CursorScope::Type(ObjectType::Mailbox)),
            Some(DataType::Mailbox)
        );
    }

    #[test]
    fn data_type_and_scope_mappings_are_inverse_for_the_supported_types() {
        for (scope, data_type) in [
            (CursorScope::Type(ObjectType::Email), DataType::Email),
            (CursorScope::Type(ObjectType::Mailbox), DataType::Mailbox),
        ] {
            assert_eq!(data_type_for_scope(&scope), Some(data_type.clone()));
            assert_eq!(scope_for_data_type(&data_type), Some(scope));
        }
    }

    type AppliedPushSets = Arc<StdMutex<Vec<(DataTypeSet, Option<String>)>>>;

    struct RecordingPushTransport {
        fail: bool,
        applied: AppliedPushSets,
    }

    impl PushTransport for RecordingPushTransport {
        type Stream = BoxedWsStream;

        async fn connect_push(&self) -> crate::Result<Self::Stream> {
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn set_push_data_types(
            &self,
            data_types: &DataTypeSet,
            push_state: Option<String>,
        ) -> crate::Result<()> {
            self.applied
                .lock()
                .expect("apply log")
                .push((data_types.clone(), push_state));
            // A genuine failure. `WebSocketNotConnected` is not one: subscribe
            // and unsubscribe absorb it as a deferred success.
            if self.fail {
                Err(unrelated_failure())
            } else {
                Ok(())
            }
        }

        async fn ping_push(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    struct FramePushTransport {
        frames: StdMutex<Option<Vec<crate::Result<crate::client_ws::WebSocketMessage>>>>,
    }

    impl PushTransport for FramePushTransport {
        type Stream = BoxedWsStream;

        async fn connect_push(&self) -> crate::Result<Self::Stream> {
            let frames = self
                .frames
                .lock()
                .expect("frame script")
                .take()
                .unwrap_or_default();
            Ok(Box::pin(futures::stream::iter(frames)))
        }

        async fn set_push_data_types(
            &self,
            _data_types: &DataTypeSet,
            _push_state: Option<String>,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn ping_push(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    struct TerminalHandshakeTransport;

    impl PushTransport for TerminalHandshakeTransport {
        type Stream = BoxedWsStream;

        async fn connect_push(&self) -> crate::Result<Self::Stream> {
            Err(crate::Error::NotParsable("handshake response".to_string()))
        }

        async fn set_push_data_types(
            &self,
            _data_types: &DataTypeSet,
            _push_state: Option<String>,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn ping_push(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn capability_failure_in_a_push_frame_reaches_engine_recovery() {
        let (tx, _) = broadcast::channel(8);
        let transport = FramePushTransport {
            frames: StdMutex::new(Some(vec![Err(crate::Error::from_subprotocol(
                "push capability changed",
            ))])),
        };
        let step = reader_pass(
            &transport,
            &tx,
            &Arc::new(Mutex::new(HashSet::new())),
            &Arc::new(Mutex::new(None)),
            &CancellationToken::new(),
            ReconnectPolicy::default(),
            &routing(),
        )
        .await;
        match step {
            ReaderStep::Terminal(error) => assert!(error.recovery().requires_engine_action()),
            _ => panic!("a capability failure must leave the reconnect loop"),
        }
    }

    #[tokio::test]
    async fn capability_failure_in_reenable_is_not_retried() {
        fn capability_changed() -> crate::Error {
            crate::Error::from_subprotocol("push capability changed")
        }
        let transport = FailingPushSetTransport {
            error: capability_changed,
        };
        let (tx, _) = broadcast::channel(8);
        let step = reader_pass(
            &transport,
            &tx,
            &Arc::new(Mutex::new(HashSet::new())),
            &Arc::new(Mutex::new(None)),
            &CancellationToken::new(),
            ReconnectPolicy::default(),
            &routing(),
        )
        .await;
        match step {
            ReaderStep::Terminal(error) => assert!(error.recovery().requires_engine_action()),
            _ => panic!("re-enable must forward the engine directive"),
        }
    }

    #[test]
    fn capability_failure_at_setup_is_not_a_local_retry() {
        match classified_reader_failure(crate::Error::from_subprotocol("changed"), false) {
            ReaderStep::Terminal(error) => assert!(error.recovery().requires_engine_action()),
            _ => panic!("setup must forward the engine directive"),
        }
        assert!(matches!(
            classified_reader_failure(crate::Error::WebSocketClosed, false),
            ReaderStep::Retry { .. }
        ));
    }

    #[tokio::test]
    async fn an_unreadable_push_frame_invalidates_broadly_then_keeps_reading() {
        let (tx, mut rx) = broadcast::channel(8);
        let transport = FramePushTransport {
            frames: StdMutex::new(Some(vec![
                Ok(crate::client_ws::WebSocketMessage::UnknownFrame),
                Ok(crate::client_ws::WebSocketMessage::PushNotification(
                    state_change("acct-primary", DataType::Email, "s2"),
                )),
            ])),
        };

        let step = reader_pass(
            &transport,
            &tx,
            &Arc::new(Mutex::new(HashSet::new())),
            &Arc::new(Mutex::new(None)),
            &CancellationToken::new(),
            ReconnectPolicy::default(),
            &routing(),
        )
        .await;

        assert!(matches!(
            step,
            ReaderStep::Retry {
                reset_backoff: true
            }
        ));
        assert!(matches!(rx.try_recv(), Ok(WatchEvent::Reconnected)));
        match rx.try_recv() {
            Ok(WatchEvent::Invalidated { hint }) => {
                assert_eq!(hint.source, PushSource::JmapStateChange);
                assert!(matches!(hint.payload, HintPayload::Unknown));
            }
            other => panic!("expected broad invalidation, got {other:?}"),
        }
        assert!(matches!(
            rx.try_recv(),
            Ok(WatchEvent::Invalidated { hint })
                if matches!(hint.payload, HintPayload::SpecificCursorScope(_))
        ));
    }

    #[tokio::test]
    async fn a_bad_ws_frame_close_reconnects_without_terminal_health() {
        let tx = PushEventSender::new(8);
        let mut events = tx.events.subscribe();
        let shutdown = CancellationToken::new();
        let reader = tokio::spawn(reader_loop(
            FramePushTransport {
                frames: StdMutex::new(Some(vec![Err(crate::Error::WebSocketClosed)])),
            },
            tx.clone(),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(None)),
            shutdown.clone(),
            ReconnectPolicy {
                initial: Duration::from_secs(3600),
                max: Duration::from_secs(3600),
                connect_timeout: Duration::from_secs(30),
                keepalive: Duration::from_secs(120),
            },
            Arc::new(routing()),
        ));

        assert!(matches!(events.recv().await, Ok(WatchEvent::Reconnected)));
        assert!(matches!(events.recv().await, Ok(WatchEvent::Disconnected)));
        let terminal = tx.terminal.subscribe();
        assert!(
            terminal.borrow().is_none(),
            "a malformed-frame close must use transient health"
        );

        shutdown.cancel();
        reader.await.expect("reader task panicked");
    }

    /// Push transport whose every push-set frame fails with `error()`.
    struct FailingPushSetTransport {
        error: fn() -> crate::Error,
    }

    impl PushTransport for FailingPushSetTransport {
        type Stream = BoxedWsStream;

        async fn connect_push(&self) -> crate::Result<Self::Stream> {
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn set_push_data_types(
            &self,
            _data_types: &DataTypeSet,
            _push_state: Option<String>,
        ) -> crate::Result<()> {
            Err((self.error)())
        }

        async fn ping_push(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    fn sink_refused() -> crate::Error {
        crate::Error::WebSocketSend(tokio_websockets::Error::AlreadyClosed)
    }

    fn sink_failed_mid_flush() -> crate::Error {
        crate::Error::WebSocketSend(tokio_websockets::Error::Io(std::io::Error::from(
            std::io::ErrorKind::BrokenPipe,
        )))
    }

    fn unrelated_failure() -> crate::Error {
        crate::Error::NotParsable("scripted".to_string())
    }

    /// A subscribe whose frame hit a dead sink is a deferred success, like
    /// one before the first connect. Failing it let a WebSocket that died
    /// during the engine's reopen abort the reopen and pause the account.
    /// The dead stream never revives, so the reader's keepalive fails, it
    /// reconnects, and the re-enable applies the committed set.
    #[tokio::test]
    async fn a_subscribe_on_a_dead_sink_commits_and_the_next_connection_applies_it() {
        for error in [sink_refused as fn() -> crate::Error, sink_failed_mid_flush] {
            let subscriptions = Arc::new(Mutex::new(HashMap::new()));
            let enabled = Arc::new(Mutex::new(HashSet::new()));
            let push_state = Arc::new(Mutex::new(None));
            let handle = SubscriptionHandle("dead".to_string());
            subscribe(
                FailingPushSetTransport { error },
                PushCapability::InProcess,
                handle.clone(),
                vec![CursorScope::Type(ObjectType::Email)],
                Arc::clone(&subscriptions),
                Arc::clone(&enabled),
                Arc::clone(&push_state),
            )
            .await
            .expect("a dead sink defers the subscription to the next connection");
            assert!(subscriptions.lock().await.contains_key(&handle));

            let applied = Arc::new(StdMutex::new(Vec::new()));
            reenable_current_push_set(
                &RecordingPushTransport {
                    fail: false,
                    applied: Arc::clone(&applied),
                },
                &enabled,
                &push_state,
            )
            .await
            .expect("re-enable succeeds");
            assert_eq!(
                applied.lock().expect("apply log").clone(),
                vec![([DataType::Email].into_iter().collect::<DataTypeSet>(), None)],
                "the next connection applies the committed subscription"
            );
        }
    }

    /// The knock-on the dead-sink failure caused. After a WebSocket drop
    /// the engine's reopen tears each old handle down on the OLD account,
    /// whose sink is dead; the teardown failed, the record was marked
    /// `teardown_unconfirmed`, and unconfirmed records are never recreated
    /// on the replacement, so the consumer's subscription vanished.
    ///
    /// Push is per connection, so the dead connection's subscription is
    /// already gone: the removal succeeds and commits, and the next
    /// connection's re-enable applies the narrowed union.
    #[tokio::test]
    async fn an_unsubscribe_on_a_dead_sink_completes_and_the_next_connection_applies_it() {
        for error in [sink_refused as fn() -> crate::Error, sink_failed_mid_flush] {
            let kept = SubscriptionHandle("kept".to_string());
            let dropped = SubscriptionHandle("dropped".to_string());
            let subscriptions = Arc::new(Mutex::new(HashMap::from([
                (
                    kept.clone(),
                    [DataType::Email].into_iter().collect::<DataTypeSet>(),
                ),
                (
                    dropped.clone(),
                    [DataType::Mailbox].into_iter().collect::<DataTypeSet>(),
                ),
            ])));
            let enabled = Arc::new(Mutex::new(
                [DataType::Email, DataType::Mailbox].into_iter().collect(),
            ));
            let push_state = Arc::new(Mutex::new(Some("push-7".to_string())));

            unsubscribe(
                FailingPushSetTransport { error },
                dropped.clone(),
                Arc::clone(&subscriptions),
                Arc::clone(&enabled),
                Arc::clone(&push_state),
            )
            .await
            .expect("the dead connection's subscription is already gone");

            assert!(!subscriptions.lock().await.contains_key(&dropped));
            assert!(subscriptions.lock().await.contains_key(&kept));

            let applied = Arc::new(StdMutex::new(Vec::new()));
            reenable_current_push_set(
                &RecordingPushTransport {
                    fail: false,
                    applied: Arc::clone(&applied),
                },
                &enabled,
                &push_state,
            )
            .await
            .expect("re-enable succeeds");
            let applied = applied.lock().expect("apply log").clone();
            assert_eq!(
                applied,
                vec![(
                    [DataType::Email].into_iter().collect::<DataTypeSet>(),
                    Some("push-7".to_string())
                )],
                "the next connection carries the narrowed union and the live position"
            );
        }
    }

    /// The absorption is for a finished or not-yet-made connection only. Any
    /// other failure is still reported, and still commits nothing.
    #[tokio::test]
    async fn an_unsubscribe_that_fails_otherwise_still_fails_and_commits_nothing() {
        let handle = SubscriptionHandle("only".to_string());
        let subscriptions = Arc::new(Mutex::new(HashMap::from([(
            handle.clone(),
            [DataType::Email].into_iter().collect::<DataTypeSet>(),
        )])));
        let enabled = Arc::new(Mutex::new([DataType::Email].into_iter().collect()));

        unsubscribe(
            FailingPushSetTransport {
                error: unrelated_failure,
            },
            handle.clone(),
            Arc::clone(&subscriptions),
            Arc::clone(&enabled),
            Arc::new(Mutex::new(None)),
        )
        .await
        .expect_err("only a failed sink write proves the connection finished");

        assert!(subscriptions.lock().await.contains_key(&handle));
        assert_eq!(
            *enabled.lock().await,
            [DataType::Email].into_iter().collect::<DataTypeSet>()
        );
    }

    #[tokio::test]
    async fn a_failed_subscribe_commits_neither_handle_nor_enabled_union() {
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let enabled = Arc::new(Mutex::new(HashSet::new()));
        let result = subscribe(
            RecordingPushTransport {
                fail: true,
                applied: Arc::new(StdMutex::new(Vec::new())),
            },
            PushCapability::InProcess,
            SubscriptionHandle("hidden".to_string()),
            vec![CursorScope::Type(ObjectType::Email)],
            Arc::clone(&subscriptions),
            Arc::clone(&enabled),
            Arc::new(Mutex::new(None)),
        )
        .await;

        assert!(result.is_err());
        assert!(subscriptions.lock().await.is_empty());
        assert!(enabled.lock().await.is_empty());
    }

    /// Subscribing before the reader's first handshake is not a refusal: the
    /// sink is simply not installed yet. It used to be a terminal
    /// `Unsupported`, and the engine's reopen aborts on any error from the
    /// replacement's subscribe, so a slow WebSocket endpoint during reopen
    /// could pause the whole account. It now commits the desired set, and the
    /// first connection's re-enable applies it.
    #[tokio::test]
    async fn a_subscribe_before_the_first_connect_is_a_deferred_success() {
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let enabled = Arc::new(Mutex::new(HashSet::new()));
        let push_state = Arc::new(Mutex::new(None));
        let handle = SubscriptionHandle("early".to_string());
        let (returned, accepted) = subscribe(
            FailingPushSetTransport {
                error: || crate::Error::WebSocketNotConnected,
            },
            PushCapability::InProcess,
            handle.clone(),
            vec![CursorScope::Type(ObjectType::Email)],
            Arc::clone(&subscriptions),
            Arc::clone(&enabled),
            Arc::clone(&push_state),
        )
        .await
        .expect("no connection yet is a deferred success, not a refusal");
        assert_eq!(returned, handle);
        assert_eq!(accepted, vec![true]);
        assert!(subscriptions.lock().await.contains_key(&handle));

        let applied = Arc::new(StdMutex::new(Vec::new()));
        reenable_current_push_set(
            &RecordingPushTransport {
                fail: false,
                applied: Arc::clone(&applied),
            },
            &enabled,
            &push_state,
        )
        .await
        .expect("re-enable succeeds");
        assert_eq!(
            applied.lock().expect("apply log").clone(),
            vec![([DataType::Email].into_iter().collect::<DataTypeSet>(), None)],
            "the first connection applies the set committed before it existed"
        );

        // And the matching teardown before any connection completes too:
        // nothing exists server-side to narrow.
        unsubscribe(
            FailingPushSetTransport {
                error: || crate::Error::WebSocketNotConnected,
            },
            handle.clone(),
            Arc::clone(&subscriptions),
            Arc::clone(&enabled),
            Arc::clone(&push_state),
        )
        .await
        .expect("no connection yet has nothing to tear down");
        assert!(subscriptions.lock().await.is_empty());
        assert!(enabled.lock().await.is_empty());
    }

    #[tokio::test]
    async fn subscribe_reports_each_supported_and_unsupported_position() {
        let result = subscribe(
            RecordingPushTransport {
                fail: false,
                applied: Arc::new(StdMutex::new(Vec::new())),
            },
            PushCapability::InProcess,
            SubscriptionHandle("mixed".to_string()),
            vec![
                CursorScope::Type(ObjectType::Email),
                CursorScope::Query(bifrost_types::QueryId("q1".to_string())),
                CursorScope::Type(ObjectType::Email),
            ],
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(None)),
        )
        .await
        .expect("supported subset applies");

        assert_eq!(result.1, vec![true, false, true]);
    }

    /// An all-rejected subscribe is a fact about the SCOPES, not about the
    /// account: push was advertised as `InProcess`, and the mixed case
    /// already reports the same rejection per-scope on the accepted lane.
    /// `Unsupported(PushSubscribe)` would tell a consumer keying off the
    /// kind that this account has no push at all, inviting a wholesale
    /// downgrade.
    #[tokio::test]
    async fn zero_mappable_scopes_is_a_malformed_request_not_absent_push() {
        let error = subscribe(
            RecordingPushTransport {
                fail: false,
                applied: Arc::new(StdMutex::new(Vec::new())),
            },
            PushCapability::InProcess,
            SubscriptionHandle("none".to_string()),
            vec![CursorScope::Query(bifrost_types::QueryId("q1".to_string()))],
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(None)),
        )
        .await
        .expect_err("no scope maps");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
    }

    /// Reconfiguring the live data-type set must carry the last acknowledged
    /// RFC 8887 position onto the new `WebSocketPushEnable`. Sending `None`
    /// there abandons the position, so changes around the reconfigure are
    /// never replayed - the same outage window the reconnect fix closes.
    #[tokio::test]
    async fn reconfiguring_the_data_type_set_preserves_the_push_position() {
        let applied = Arc::new(StdMutex::new(Vec::new()));
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let enabled = Arc::new(Mutex::new([DataType::Email].into_iter().collect()));
        let push_state = Arc::new(Mutex::new(Some("push-7".to_string())));

        subscribe(
            RecordingPushTransport {
                fail: false,
                applied: Arc::clone(&applied),
            },
            PushCapability::InProcess,
            SubscriptionHandle("second".to_string()),
            vec![CursorScope::Type(ObjectType::Mailbox)],
            Arc::clone(&subscriptions),
            Arc::clone(&enabled),
            Arc::clone(&push_state),
        )
        .await
        .expect("reconfigure applies");

        assert_eq!(
            applied.lock().expect("apply log")[0].1.as_deref(),
            Some("push-7"),
            "the reconfigure frame must carry the live position"
        );
        assert_eq!(push_state.lock().await.as_deref(), Some("push-7"));
    }

    /// The other half: once the last subscription goes away the frame is a
    /// disable, the position it belonged to is retired, and a reconnect after
    /// that must NOT replay it. Retaining the value here would make the
    /// reconnect fix produce a wrong replay the original bug could not.
    #[tokio::test]
    async fn a_reconnect_after_the_last_unsubscribe_replays_no_stale_position() {
        let applied = Arc::new(StdMutex::new(Vec::new()));
        let handle = SubscriptionHandle("only".to_string());
        let subscriptions = Arc::new(Mutex::new(HashMap::from([(
            handle.clone(),
            [DataType::Email].into_iter().collect::<DataTypeSet>(),
        )])));
        let enabled = Arc::new(Mutex::new([DataType::Email].into_iter().collect()));
        let push_state = Arc::new(Mutex::new(Some("push-7".to_string())));

        unsubscribe(
            RecordingPushTransport {
                fail: false,
                applied: Arc::clone(&applied),
            },
            handle,
            Arc::clone(&subscriptions),
            Arc::clone(&enabled),
            Arc::clone(&push_state),
        )
        .await
        .expect("unsubscribe applies");

        assert!(push_state.lock().await.is_none());

        let transport = RecordingPushTransport {
            fail: false,
            applied: Arc::clone(&applied),
        };
        reenable_current_push_set(&transport, &enabled, &push_state)
            .await
            .expect("re-enable succeeds");
        assert_eq!(
            applied.lock().expect("apply log")[1].1,
            None,
            "a reconnect after the subscription was retired must start cold"
        );
    }

    /// An orphaned handle retried against a newer instance is unknown there.
    /// Push state is per-connection, so that must delete nothing: the live
    /// subscription survives and the frame re-sends the union it already held.
    #[tokio::test]
    async fn unsubscribing_an_unknown_handle_leaves_live_subscriptions_intact() {
        let applied = Arc::new(StdMutex::new(Vec::new()));
        let live = SubscriptionHandle("live".to_string());
        let email: DataTypeSet = [DataType::Email].into_iter().collect();
        let subscriptions = Arc::new(Mutex::new(HashMap::from([(live.clone(), email.clone())])));
        let enabled = Arc::new(Mutex::new(email.clone()));
        let push_state = Arc::new(Mutex::new(Some("push-3".to_string())));

        unsubscribe(
            RecordingPushTransport {
                fail: false,
                applied: Arc::clone(&applied),
            },
            SubscriptionHandle("orphan-from-old-connection".to_string()),
            Arc::clone(&subscriptions),
            Arc::clone(&enabled),
            Arc::clone(&push_state),
        )
        .await
        .expect("unknown handle unsubscribes cleanly");

        assert!(subscriptions.lock().await.contains_key(&live));
        assert_eq!(*enabled.lock().await, email);
        assert_eq!(push_state.lock().await.as_deref(), Some("push-3"));
        let frames = applied.lock().expect("apply log");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, email, "the frame must not narrow the union");
    }

    /// The replay is only as good as the capture. A frame carrying a
    /// `pushState` must move the cached position, a frame without one must
    /// leave it alone (RFC 8887 makes the property optional, and treating an
    /// absent one as "reset to cold" would throw the position away on the
    /// first server that omits it), and a `Group` reports its LAST member's
    /// position rather than its first.
    #[tokio::test]
    async fn the_reader_captures_the_latest_push_state_and_never_unsets_it() {
        let state = Arc::new(Mutex::new(None));

        remember_push_state(&state_change("u1", DataType::Email, "s1"), &state).await;
        assert!(state.lock().await.is_none(), "no pushState, nothing cached");

        let mut with_state = state_change("u1", DataType::Email, "s1");
        if let PushObject::StateChange { push_state, .. } = &mut with_state {
            *push_state = Some("ps-1".to_string());
        }
        remember_push_state(&with_state, &state).await;
        assert_eq!(state.lock().await.as_deref(), Some("ps-1"));

        remember_push_state(&state_change("u1", DataType::Email, "s2"), &state).await;
        assert_eq!(
            state.lock().await.as_deref(),
            Some("ps-1"),
            "a frame without a pushState must not clear the position"
        );

        let mut later = state_change("u1", DataType::Mailbox, "s3");
        if let PushObject::StateChange { push_state, .. } = &mut later {
            *push_state = Some("ps-2".to_string());
        }
        let group = PushObject::Group {
            entries: vec![with_state, later],
        };
        remember_push_state(&group, &state).await;
        assert_eq!(state.lock().await.as_deref(), Some("ps-2"));
    }

    #[tokio::test]
    async fn reconnect_replays_the_last_push_state() {
        let applied = Arc::new(StdMutex::new(Vec::new()));
        let transport = RecordingPushTransport {
            fail: false,
            applied: Arc::clone(&applied),
        };
        let enabled = Arc::new(Mutex::new([DataType::Email].into_iter().collect()));
        let state = Arc::new(Mutex::new(Some("push-7".to_string())));

        reenable_current_push_set(&transport, &enabled, &state)
            .await
            .expect("re-enable succeeds");

        assert_eq!(
            applied.lock().expect("apply log")[0].1.as_deref(),
            Some("push-7")
        );
    }

    #[test]
    fn the_enabled_data_type_union_spans_every_live_subscription() {
        let mut subscriptions: HashMap<SubscriptionHandle, DataTypeSet> = HashMap::new();
        subscriptions.insert(
            SubscriptionHandle("a".to_string()),
            [DataType::Email].into_iter().collect(),
        );
        subscriptions.insert(
            SubscriptionHandle("b".to_string()),
            [DataType::Email, DataType::Mailbox].into_iter().collect(),
        );

        let union = union_data_types(&subscriptions);
        assert_eq!(union.len(), 2);
        assert!(union.contains(&DataType::Email));
        assert!(union.contains(&DataType::Mailbox));

        // Dropping one handle leaves only the other's types, which is what
        // drives `apply_push_set` back down to a narrower subscription.
        subscriptions.remove(&SubscriptionHandle("b".to_string()));
        let union = union_data_types(&subscriptions);
        assert_eq!(union.len(), 1);
        assert!(union.contains(&DataType::Email));
    }

    #[tokio::test]
    async fn the_push_stream_ends_when_the_shutdown_token_is_cancelled() {
        let tx = PushEventSender::new(4);
        let shutdown = CancellationToken::new();
        let mut events = super::stream(tx.subscribe(), shutdown.clone());

        let _ = tx.send(WatchEvent::Reconnected);
        assert!(matches!(events.next().await, Some(WatchEvent::Reconnected)));

        shutdown.cancel();
        assert!(
            events.next().await.is_none(),
            "cancellation must terminate the subscriber, not just stop new sends"
        );
    }

    fn terminal_push_error() -> AccountError {
        let error = super::super::error::into_account_error(
            crate::Error::NotParsable("push frame".to_string()),
            super::super::error::JmapErrorContext::new(AccountOperation::PushStream),
        );
        assert!(error.recovery().is_terminal());
        error
    }

    #[tokio::test]
    async fn a_terminal_push_event_is_replayed_to_a_late_subscriber() {
        let tx = PushEventSender::new(4);
        let error = terminal_push_error();
        let reader = tokio::spawn(reader_loop(
            TerminalHandshakeTransport,
            tx.clone(),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(None)),
            CancellationToken::new(),
            ReconnectPolicy::default(),
            Arc::new(routing()),
        ));

        // No broadcast receiver exists yet. The hint send is dropped, but
        // terminal health must remain observable for every later subscriber.
        tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("terminal handshake failure stops the reader")
            .expect("reader task panicked");

        let mut events = super::stream(tx.subscribe(), CancellationToken::new());
        assert!(matches!(
            events.next().await,
            Some(WatchEvent::Terminated(terminal))
                if terminal.message_key() == error.message_key()
        ));
        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn a_lagged_subscriber_reads_the_latched_terminal_event() {
        let tx = PushEventSender::new(1);
        let mut events = super::stream(tx.subscribe(), CancellationToken::new());
        let _ = tx.send(WatchEvent::Reconnected);
        let _ = tx.send(WatchEvent::Disconnected);
        let error = terminal_push_error();
        let _ = tx.send(WatchEvent::Terminated(error.clone()));

        assert!(matches!(
            events.next().await,
            Some(WatchEvent::Terminated(terminal))
                if terminal.message_key() == error.message_key()
        ));
        assert!(events.next().await.is_none());
    }

    /// Which setup await the stub transport parks on forever.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Hang {
        Handshake,
        Reenable,
    }

    /// Push transport whose chosen setup await never resolves.
    ///
    /// `entered` gains a permit the instant the reader parks inside the
    /// await under test, so a test cancels from *inside* that await
    /// rather than racing the spawn (a cancel that lands before the task
    /// runs would exit at the loop's top-of-iteration check and prove
    /// nothing).
    struct HangingTransport {
        hang: Hang,
        entered: Arc<tokio::sync::Semaphore>,
    }

    impl PushTransport for HangingTransport {
        type Stream = BoxedWsStream;

        fn connect_push(&self) -> impl Future<Output = crate::Result<Self::Stream>> + Send {
            let entered = Arc::clone(&self.entered);
            let hang = self.hang == Hang::Handshake;
            async move {
                if hang {
                    entered.add_permits(1);
                    std::future::pending::<()>().await;
                }
                // A connection that is up but silent: the reader must
                // reach its next await, not fall out of the read loop.
                let stream: Self::Stream = Box::pin(futures::stream::pending());
                Ok(stream)
            }
        }

        fn set_push_data_types(
            &self,
            _data_types: &DataTypeSet,
            _push_state: Option<String>,
        ) -> impl Future<Output = crate::Result<()>> + Send {
            let entered = Arc::clone(&self.entered);
            let hang = self.hang == Hang::Reenable;
            async move {
                if hang {
                    entered.add_permits(1);
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
        }

        async fn ping_push(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    /// Spawn a reader on a stub that hangs at `hang`, wait until it is
    /// parked in that await, then cancel and require the task to finish.
    ///
    /// The `connect_timeout` is set far beyond the assertion window so
    /// cancellation is the only thing that can end the reader; the outer
    /// bound only fires when the fix is absent.
    async fn assert_cancel_unblocks(hang: Hang) {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let shutdown = CancellationToken::new();
        let tx = PushEventSender::new(8);
        let reader = tokio::spawn(reader_loop(
            HangingTransport {
                hang,
                entered: Arc::clone(&entered),
            },
            tx,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(None)),
            shutdown.clone(),
            ReconnectPolicy {
                initial: Duration::from_secs(3600),
                max: Duration::from_secs(3600),
                connect_timeout: Duration::from_secs(3600),
                keepalive: Duration::from_secs(3600),
            },
            Arc::new(routing()),
        ));

        let _permit = entered.acquire().await.expect("reader reached the await");
        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("shutdown must drop the in-flight setup future, not wait on the network")
            .expect("reader task panicked");
    }

    // `close()` cancels the shutdown token while the reader may be parked
    // anywhere in its lifecycle. The handshake has no protocol deadline,
    // so a cancel that only takes effect between connections leaves the
    // detached task and its client resources alive indefinitely.
    #[tokio::test]
    async fn a_shutdown_during_the_websocket_handshake_ends_the_reader() {
        assert_cancel_unblocks(Hang::Handshake).await;
    }

    // Same for the push re-enable that follows a successful handshake:
    // it awaits a WebSocket send holding the client's sink lock, so a
    // half-open socket parks the reader there just as indefinitely.
    #[tokio::test]
    async fn a_shutdown_during_the_push_re_enable_ends_the_reader() {
        assert_cancel_unblocks(Hang::Reenable).await;
    }

    /// Push transport whose handshake succeeds (silent but live stream)
    /// and whose re-enable fails immediately, counting attempts.
    struct FailingReenableTransport {
        attempts: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl PushTransport for FailingReenableTransport {
        type Stream = BoxedWsStream;

        async fn connect_push(&self) -> crate::Result<Self::Stream> {
            let stream: Self::Stream = Box::pin(futures::stream::pending());
            Ok(stream)
        }

        fn set_push_data_types(
            &self,
            _data_types: &DataTypeSet,
            _push_state: Option<String>,
        ) -> impl Future<Output = crate::Result<()>> + Send {
            let attempts = Arc::clone(&self.attempts);
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // The shape a fast re-enable failure really takes: the
                // link died between handshake and subscribe, and the
                // library refuses the frame on the closed stream.
                Err(crate::Error::WebSocketSend(
                    tokio_websockets::Error::AlreadyClosed,
                ))
            }
        }

        async fn ping_push(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    // A re-enable that fails FAST (dead sink detected immediately) must
    // end the pass as a transient disconnect. Proceeding to the read
    // loop instead would announce `Reconnected` over a connection with
    // no applied subscription - a silent dead-push state the
    // `connect_timeout` bound cannot see.
    #[tokio::test(start_paused = true)]
    async fn a_failed_push_re_enable_disconnects_instead_of_announcing_reconnected() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let tx = PushEventSender::new(8);
        let mut rx = tx.events.subscribe();
        let reader = tokio::spawn(reader_loop(
            FailingReenableTransport {
                attempts: Arc::clone(&attempts),
            },
            tx,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(None)),
            shutdown.clone(),
            ReconnectPolicy::default(),
            Arc::new(routing()),
        ));

        // Two consecutive passes: each must surface as `Disconnected`
        // with no `Reconnected` in between, proving the reader both
        // refuses the unsubscribed link and keeps reconnecting rather
        // than parking on it.
        for _ in 0..2 {
            match rx.recv().await {
                Ok(WatchEvent::Disconnected) => {}
                other => panic!("expected Disconnected, got {other:?}"),
            }
        }
        assert!(
            attempts.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "the reader must retry the connect + re-enable cycle"
        );

        shutdown.cancel();
        reader.await.expect("reader task panicked");
    }

    // Independently of shutdown: a handshake that never answers must
    // fall to the reconnect backoff rather than stall push for the life
    // of the session. Time is paused, so the bound elapses without any
    // wall-clock wait.
    #[tokio::test(start_paused = true)]
    async fn a_handshake_that_outlives_its_bound_falls_to_the_reconnect_backoff() {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let shutdown = CancellationToken::new();
        let tx = PushEventSender::new(8);
        let mut rx = tx.events.subscribe();
        let reader = tokio::spawn(reader_loop(
            HangingTransport {
                hang: Hang::Handshake,
                entered,
            },
            tx,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(None)),
            shutdown.clone(),
            ReconnectPolicy {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(60),
                connect_timeout: Duration::from_secs(30),
                keepalive: Duration::from_secs(120),
            },
            Arc::new(routing()),
        ));

        assert!(
            matches!(rx.recv().await, Ok(WatchEvent::Disconnected)),
            "an unbounded handshake must surface as a transient disconnect"
        );

        shutdown.cancel();
        reader.await.expect("reader task panicked");
    }

    /// Push transport that connects cleanly onto a permanently silent
    /// stream and counts pings, never answering one.
    struct SilentTransport {
        pings: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl PushTransport for SilentTransport {
        type Stream = BoxedWsStream;

        async fn connect_push(&self) -> crate::Result<Self::Stream> {
            let stream: Self::Stream = Box::pin(futures::stream::pending());
            Ok(stream)
        }

        async fn set_push_data_types(
            &self,
            _data_types: &DataTypeSet,
            _push_state: Option<String>,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn ping_push(&self) -> crate::Result<()> {
            self.pings.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    // The read loop is where the reader spends its life, and a half-open
    // socket never errors and never closes there. Silence past the
    // keepalive must therefore provoke a ping, and an unanswered ping must
    // end the connection - otherwise push is dead for the life of the
    // account and the engine cannot tell it apart from a quiet mailbox.
    #[tokio::test(start_paused = true)]
    async fn a_silent_connection_is_pinged_and_dropped_when_the_pong_never_comes() {
        let pings = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let tx = PushEventSender::new(8);
        let mut rx = tx.events.subscribe();
        let reader = tokio::spawn(reader_loop(
            SilentTransport {
                pings: Arc::clone(&pings),
            },
            tx,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(None)),
            shutdown.clone(),
            ReconnectPolicy {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(60),
                connect_timeout: Duration::from_secs(5),
                keepalive: Duration::from_secs(10),
            },
            Arc::new(routing()),
        ));

        assert!(matches!(rx.recv().await, Ok(WatchEvent::Reconnected)));
        assert!(
            matches!(rx.recv().await, Ok(WatchEvent::Disconnected)),
            "an unanswered keepalive must surface as a transient disconnect"
        );
        assert!(
            pings.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "the reader must probe the silent link rather than park on it"
        );

        shutdown.cancel();
        reader.await.expect("reader task panicked");
    }

    /// Push transport whose stream yields one non-terminal error right
    /// away - the shape of a server that rejects the push-enable frame,
    /// whose only failure signal is an asynchronous `RequestError` on the
    /// read stream. Records when each connect happened.
    struct RejectingTransport {
        connects: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
    }

    impl PushTransport for RejectingTransport {
        type Stream = BoxedWsStream;

        async fn connect_push(&self) -> crate::Result<Self::Stream> {
            self.connects
                .lock()
                .expect("connect log")
                .push(tokio::time::Instant::now());
            let stream: Self::Stream = Box::pin(futures::stream::once(async {
                Err(crate::Error::WebSocketClosed)
            }));
            Ok(stream)
        }

        async fn set_push_data_types(
            &self,
            _data_types: &DataTypeSet,
            _push_state: Option<String>,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn ping_push(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    // A server that will never accept the subscription completes the
    // handshake and the sink write, then rejects on the read stream. If
    // merely REACHING the read loop reset the backoff, that is an
    // unbounded 1 Hz handshake storm plus a Disconnected/Reconnected pair
    // per second forever. The backoff must keep growing.
    #[tokio::test(start_paused = true)]
    async fn a_pass_that_reads_no_traffic_does_not_reset_the_backoff() {
        let connects = Arc::new(std::sync::Mutex::new(Vec::new()));
        let shutdown = CancellationToken::new();
        let tx = PushEventSender::new(64);
        let mut rx = tx.events.subscribe();
        let reader = tokio::spawn(reader_loop(
            RejectingTransport {
                connects: Arc::clone(&connects),
            },
            tx,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(None)),
            shutdown.clone(),
            ReconnectPolicy {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(60),
                connect_timeout: Duration::from_secs(30),
                keepalive: Duration::from_secs(120),
            },
            Arc::new(routing()),
        ));

        // Four passes: gaps must be 1s, 2s, 4s rather than 1s, 1s, 1s.
        let mut disconnects = 0;
        while disconnects < 4 {
            if let Ok(WatchEvent::Disconnected) = rx.recv().await {
                disconnects += 1;
            }
        }
        shutdown.cancel();
        reader.await.expect("reader task panicked");

        let connects = connects.lock().expect("connect log").clone();
        assert!(
            connects.len() >= 4,
            "expected four passes, got {connects:?}"
        );
        let gaps = connects
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect::<Vec<_>>();
        assert_eq!(
            &gaps[..3],
            &[
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4)
            ],
            "the backoff must keep doubling against a peer that only ever rejects"
        );
    }

    // `close()` cancels the shutdown token and then joins the reader. The
    // join is bounded so a wedged reader cannot hold teardown open, but a
    // healthy one must be genuinely awaited - the old shape dropped the
    // handle at spawn, so `close()` returning was no evidence at all.
    #[tokio::test(start_paused = true)]
    async fn joining_the_reader_waits_for_it_and_gives_up_on_a_wedged_one() {
        let handle = Arc::new(Mutex::new(Some(tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }))));
        let started = tokio::time::Instant::now();
        join_reader(&handle, Duration::from_secs(30)).await;
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(1),
            "a healthy reader is awaited to completion"
        );
        assert!(
            handle.lock().await.is_none(),
            "the handle is consumed, so a second close is a no-op"
        );

        let wedged = Arc::new(Mutex::new(Some(tokio::spawn(std::future::pending::<()>()))));
        let started = tokio::time::Instant::now();
        join_reader(&wedged, Duration::from_secs(30)).await;
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(30),
            "a wedged reader must not hold teardown open past the bound"
        );
    }

    #[tokio::test]
    async fn a_lagged_broadcast_slot_coalesces_into_an_unknown_invalidation() {
        // A slow consumer must never lose a wake-up: an overflowed slot
        // becomes one coalesced invalidation so the engine full-repolls.
        let tx = PushEventSender::new(1);
        let shutdown = CancellationToken::new();
        let mut events = super::stream(tx.subscribe(), shutdown);

        let _ = tx.send(WatchEvent::Reconnected);
        let _ = tx.send(WatchEvent::Disconnected);

        match events.next().await {
            Some(WatchEvent::Invalidated { hint }) => {
                assert_eq!(hint.source, PushSource::Coalesced);
                assert!(matches!(hint.payload, HintPayload::Unknown));
            }
            other => panic!("expected a coalesced invalidation, got {other:?}"),
        }
    }
}
