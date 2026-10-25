# Engine-owned push sessions and bounded push ingress: implementation spec (SHARED-014, SYNC-009)

Status: design sparred to consensus with a cold codex session (read-only
profile) on 2026-10-25, six design exchanges plus consistency reads of this
document whose findings are folded in; the positions conceded on each side are
recorded in section 11. Cold-reviewed twice (r1, r2) on 2026-10-25; every
finding was validated against the tree, the valid ones are folded in and the
rejected ones are recorded with reasons in section 12, together with the
questions left for the owner. Implementation not landed.

## 0. Standing references (read them, do not just note that they exist)

Implementers and reviewers MUST read these before touching code. They are the
contract this spec is written against and the ground it is judged on.

- `reference/technical-implementation-spec.md` - the contract this document is
  written against.
- `reference/error-model.md` - the cross-cutting `AccountError` contract. Read
  all of it; this spec decides how the engine acts on every recovery class a
  push session or lifecycle stream reports, and adds one paragraph to it
  (section 8).
- `notes/bugs-deferred.md`, entry "SHARED-014 - Provider-owned long-running
  loops each own their recovery triage and backoff", and `notes/bugs-sync.md`,
  entry "SYNC-009 - Push ingress can still lose or unboundedly queue control
  events" - the originating items. The owner ruled (2026-10-21) that they are
  specified together and that the direction is the structural option: provider
  push and lifecycle loops stop owning recovery and the engine owns the outer
  loop, backoff, throttle holds and directive routing; SHARED-014 also absorbs
  the JMAP "push health as state" proposal.
- `reference/types.md` - the shared type crate the new surface lives in.
- `reference/sync.md` - "Push", the lifecycle paragraphs of "Multiplexer",
  "Error / Terminated / Warning", "Construction" and "File map": the engine
  being rebuilt.
- `reference/imap.md` ("Push lifecycle"), `reference/jmap.md` ("Push and
  reconnect", "Push failure routing"), `reference/graph.md` ("Push: webhooks
  plus EWS streaming fallback" and its subsections, "Push failure routing"),
  `reference/google.md` ("Push: Cloud Pub/Sub") - the four push producers.
- `reference/caldav.md`, `reference/carddav.md` - the two crates with no push
  lane; they only change signature.

## 1. The items, and whether they should be built

SHARED-014: four push reconnect loops (IMAP `idle_loop` with `sleep_backoff`,
JMAP `reader_loop`, Graph EWS `run_worker` with `ReconnectBackoff`, the Google
watch actor's renewal damper) plus the Graph webhook renewal worker each decide
locally which failures are retryable and how long to wait, and each re-learned
the same lesson about resetting backoff only on a demonstrated healthy read.
The ruled direction moves the outer loop, backoff, throttle holds and directive
routing into the engine.

SYNC-009: `InvalidationSinkInner` keeps an unbounded per-registration ingress,
its forwarder can drop a coalesced invalidation after a 100 ms timeout, and the
in-process forwarder has the same 100 ms loss. A sticky full-reconcile bit
cannot carry warnings or terminations, so a bounded lossless ingress needs a
control-event policy or an API change.

The items were checked against the code and the contract before design. They
should be built, with one reinterpretation, sparred and recorded:

- The defects are live. Every provider's `push_stream` replays its latched
  terminal error to each new subscriber (IMAP `push_event_stream`, JMAP
  `stream`, Graph `push_stream`, Google `push_stream`), and the engine forwarder
  in `engine/attach.rs` re-calls `push_stream` whenever the stream ends, with a
  50 ms doubling to 30 s backoff that a `Terminated` never resets. The
  reconciler then re-plans the same error each time: a `Terminal` class
  re-broadcasts `SyncEvent::Terminated`, an engine directive re-enters the
  reopen channel. That repeats for as long as the latch survives - for JMAP and
  Graph the account instance's life, for Google even after a resubscribe made
  the watch healthy again (`reference/google.md`: "The latch lasts for the
  account instance, including if an explicit subscribe restarts the watch
  actor's lifecycle"). The 100 ms losses and the unbounded ingress in
  `push/mod.rs::forward_events` and the attach forwarder are as SYNC-009
  describes; a paused account with a busy webhook receiver grows the ingress
  without bound.
- The lifecycle half of SHARED-014 is already mostly delivered: every lifecycle
  stream yields `ScopeLifecycleEvent::Terminated` on failure and the engine's
  lifecycle task owns reconnect backoff (30 s to 480 s), throttle holds and
  directive routing (`multiplexer/mod.rs`, `lifecycle_termination` and
  `lifecycle_retry_wait`). The "two lifecycle backoffs" the entry counts (Google
  doubling to 480 s, JMAP fixed 300 s on failure) no longer exist. The one
  remaining lifecycle defect is that a successful poll with no lifecycle change
  is invisible to the engine, so its backoff never resets on one.
- Reinterpretation: the item names "one-shot (one poll, one connection)" as the
  mechanism. The property it exists to deliver is that no provider owns failure
  triage or failure backoff. This spec delivers that property with a uniform
  rule instead of literal one-shot streams: a provider stream runs until its
  first failure and never retries after one; healthy-progress loops whose
  cadence is protocol knowledge (IMAP IDLE rounds on one connection, the JMAP
  read loop, Graph renewal ticks, the Google renewal due time, lifecycle poll
  intervals including JMAP's session-change wake) stay inside the stream and
  report progress through a heartbeat. For push in-process lanes that is
  exactly one connection per stream. For lifecycle it means streams keep their
  shape and gain a `Polled` heartbeat. This spec's opening position was
  literal one-shot lifecycle streams; the partner showed that a bare end of
  stream is no evidence of a healthy read and that moving the cadence into the
  engine would bury JMAP's session-change wake, the heartbeat was proposed in
  answer, and the partner accepted it as delivering the item's property
  (section 11).

## 2. Survey

### 2.1 The engine push path today

- `crates/types/src/account.rs`: `Account::push_subscribe`,
  `push_unsubscribe`, `push_stream(&self) -> AccountStream<WatchEvent>`.
  `crates/types/src/events.rs`: `WatchEvent { Invalidated, Disconnected,
  Reconnected, Terminated(AccountError), Warning(Warning) }`,
  `InvalidationHint`, `PushSource`, `HintPayload`, `InvalidationSink::push(&self,
  AccountId, WatchEvent)` returning `()`.
- `crates/sync/src/engine/attach.rs`: creates the per-account bounded
  `mpsc::channel::<WatchEvent>(watch_capacity)`, registers its sender with the
  engine sink, spawns `push::Reconciler::run(watch_rx)` and the "push
  forwarder" task, which loops `push_stream()` with its own 50 ms to 30 s
  backoff, forwards events with `try_send`, and on `Full` either awaits a
  lossless send (`Terminated`, `Warning`) or coalesces to `Invalidated {
  Coalesced, Unknown }` with a 100 ms timeout that can lose the request.
- `crates/sync/src/push/mod.rs`: `InvalidationSinkInner { senders:
  DashMap<AccountId, SinkSender>, drop_counter }`, `register(account,
  mpsc::Sender<WatchEvent>)` (spawns `forward_events` over an unbounded ingress
  on a runtime, a direct non-waiting sender off one), `unregister`, `dropped`,
  `requires_lossless_delivery`, `coalesced_event`, `shared_sink`, and the unit
  tests pinning the current queue behaviour, including
  `full_reconcile_cannot_represent_control_events`.
- `crates/sync/src/push/reconciler.rs`: `Reconciler::run(rx)` parks while the
  boundary reads `Pause`, handles `Invalidated` by `drive_scopes`,
  `Disconnected` by a warning, `Reconnected` by a full `Unknown` reconcile,
  `Terminated` by `plan_recovery` (Engine to the reopen channel, Retry and
  Reconcile record a throttle and warn, Terminal broadcasts). `drive_scopes`
  defers scope-local throttled scopes, honours parked tombstones, normalizes an
  account error from a drive onto the `Terminated` path, and returns early on an
  account-wide directive, dropping the hint's remaining scopes.
- `crates/sync/src/engine/mod.rs`: `invalidation_sink()`, `subscribe_push`,
  `unsubscribe_push`, `detach_inner` (stop workers, drain writer, take reopen
  lock, `close()`, drop registry, `sink.unregister`).
  `crates/sync/src/engine/reattach.rs`: recreates desired subscriptions on the
  replacement, tears old handles down, swaps, bumps `account_generation`,
  closes the old account last. `crates/sync/src/engine/orphan_teardown.rs`
  retries orphan `push_unsubscribe` on a timer.
- `crates/sync/src/types.rs`: `MultiplexerConfig::watch_capacity` (256),
  `PushConfig`, `AccountSlot`, `WorkerRole::{PushReconciler, PushForwarder}`.

### 2.2 Provider push loops today

- IMAP `crates/imap/src/account/push.rs`: `PushState` holds a broadcast,
  a bounded pre-subscription `pending_events` queue, the subscribed scope map,
  `task_cancel`, `push_capacity`, the `push_slots` semaphore (permits live in the
  driver exit slot, so a LOGOUT tail keeps its slot), the `resubscribe`
  generation watch, a `terminal` latch and `next_id`. `push_subscribe` admits
  per scope and calls `ensure_idle_tasks`, which spawns one `idle_loop` per
  slot (1 with NOTIFY, else `push_capacity`). `idle_loop` chooses a folder,
  reserves a session, dials, SELECTs, registers NOTIFY, runs IDLE rounds, and
  on failure either latches a terminal (`report_idle_failure`, then parks until
  a resubscribe) or emits `Disconnected` and sleeps `sleep_backoff` (5 s to
  300 s, reset on a completed round). Event helpers:
  `signal_coverage_established`, `signal_idle_interrupt_loss`,
  `signal_notify_coverage_loss`, `absorb_idle_event`, `map_idle_event`.
  `close.rs` calls `push.stop()`. Hermetic tests live in `push.rs` and in
  `scripted_tests.rs` (the `session_account` push tests).
- JMAP `crates/jmap/src/sync/push.rs`: `WsState::spawn` (called from
  `factory.rs` during `open`) spawns `reader_loop` when push is available.
  `reader_pass` connects (bounded), re-enables the committed data-type union
  (bounded), announces `Reconnected`, reads with a keepalive ping, and returns
  `ReaderStep::{Stop, Terminal, Retry { reset_backoff }}`; `reader_loop` owns
  the 1 s to 60 s backoff (`ReconnectPolicy`, public, re-exported, factory
  tunable). `PushEventSender` is a broadcast plus a terminal watch; `stream`
  replays the latch. `subscribe` / `unsubscribe` / `disable_for_close` hold
  three locks across one frame send. `account.rs::close` disables push and
  joins the reader. `PushTransport` is the hermetic seam.
- Graph EWS `crates/graph/src/account/ews_stream.rs`: `run_streaming_worker`
  / `run_worker` (spawned by `push_stream.rs::ensure_ews_worker` through
  `worker_slot.rs`) subscribes, loops `GetStreamingEvents` long polls, and on
  exit releases the subscription inline (`release_subscription`); backoff is
  `ReconnectBackoff` (5 s doubling to 320 s, quarter jitter, reset on a complete
  long poll); local triage in three `is_retryable` sites plus the
  `subscribe_status_stopped_by_replay_policy` exception; `StreamLoopExit::
  Resubscribe` re-subscribes without backoff and owes `Reconnected`.
  `GraphAccount` (`mod.rs`) holds `push_tx`, `push_terminal`, `graph_worker`,
  `push_disconnected`, `ews_worker`, `ews_topology`; `close()` retires webhook
  rows, joins the EWS worker so its release runs, and joins the renewal worker.
- Graph webhook `crates/graph/src/account/push/renewal.rs`:
  `run_graph_subscription_worker` (spawned by `webhook.rs::ensure_graph_worker`
  after each subscribe) sleeps `RENEWAL_CHECK_INTERVAL` (10 min), renews due
  rows, recreates vanished rows through the spawned
  `replace_gone_subscription` task, marks terminal rows `terminated` and
  publishes the terminal latch, raises or clears the account's
  `push_disconnected` latch (`common.rs::mark_push_disconnected`,
  `mark_push_reconnected`, `announce_push_recovered`), and retires its worker
  slot when nothing is live.
- Google `crates/google/src/account/push.rs`: one `watch_actor` task owns the
  lifecycle (`Unwatched`, `Watched`, `Renewing`, `Retired`), handles, mailbox
  watch ownership (`MailboxWatch`, `WatchOwnership`, `stop_exclusive`,
  `watch_claimed`, `commit_watched`), subscribe/unsubscribe/close commands and
  a renewal timer arm with a five-minute `retry_after` damper, a `disconnected`
  latch, a consecutive-failure warning count, local triage with the
  `renewal_status_stopped_by_replay_policy` exception, a health broadcast and a
  terminal latch.

### 2.3 Lifecycle today

`multiplexer/mod.rs` runs one lifecycle task per account: account-wide hold
check, open `scope_lifecycle_stream`, apply `Lifecycle` events (each resets the
backoff), route `Terminated` through `lifecycle_termination` and
`lifecycle_retry_wait`, reconnect after `max(backoff, retry hint)`, backoff
`LIFECYCLE_RECONNECT_MIN` 30 s doubling to `LIFECYCLE_RECONNECT_MAX` 480 s,
reset also on a generation change. Google (`scopes.rs`,
`scope_lifecycle_stream_with_baseline`) polls every 30 s with an account-owned
baseline and ends with `Terminated` on failure. JMAP (`discover.rs`,
`scope_lifecycle`) polls every 300 s, wakes early on `session_changes`, and ends
with `Terminated` on failure. IMAP pends until shutdown. Graph, CalDAV and
CardDAV return a finite empty stream, which the engine re-opens on its growing
backoff.

### 2.4 Dependents

- Every `Account` implementor changes signature: `crates/imap` (and
  `account/test_support.rs`), `crates/jmap`, `crates/google`, `crates/graph`,
  `crates/caldav`, `crates/carddav`, `crates/sync/src/mutation/readback_tests.rs`,
  and the sync test stubs in `crates/sync/tests/common/mod.rs`,
  `filter_passthrough.rs`, `settings_passthrough.rs`, `convenience_dispatch.rs`,
  `attach_schema_recovery.rs`, `readback_guard.rs` (three impls).
- Callers of `InvalidationSink::push`: `crates/sync/tests/push_poll_latency.rs`
  and the push module tests; out of workspace, ratatoskr's webhook and Pub/Sub
  receivers.
- Constructors of `push::Reconciler`: `engine/attach.rs` and
  `crates/sync/tests/push_reconcile_throttle.rs`.
- Out of workspace, any consumer driving `Account::push_stream` directly (no
  engine). After this spec such a consumer must drive every lane itself
  (3.3 S12); this is documented, not hidden.
- Consumers matching `ReopenRequest` (published, `#[non_exhaustive]`) see one
  new variant, `PushRecovery` (4.6); no existing variant changes.

### 2.5 Stale contract text found while surveying

- `reference/sync.md` "Multiplexer": "Google still retries locally: its
  stream-local diff baseline must become account-owned..." is false; the
  baseline is account-owned and Google forwards `Terminated`.
- `crates/types/src/cursor.rs` `ScopeLifecycleEvent` doc: "retry classes
  typically don't reach this envelope because the protocol sleeps and
  continues" is false for every provider.
- `crates/types/src/account.rs` `scope_lifecycle_stream` doc describes the old
  sleep-and-retry history rather than the contract.
- `reference/types.md`: "`Account::close()` ... does not delete durable
  server-side push subscriptions" contradicts Graph, whose `close()` deletes its
  webhook rows (`reference/graph.md`).

All four are fixed by the documentation bricks (section 8).

## 3. Target contract (`bifrost-types`)

### 3.1 New module `crates/types/src/push.rs`

Re-exported from `crates/types/src/lib.rs` beside the existing push types.

```rust
use std::sync::Arc;
use std::time::Duration;
use futures::channel::oneshot;
use futures::future::{BoxFuture, FutureExt, Shared};

/// One independently reconnecting push session slot of an account instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PushLane(pub u16);

/// How a lane delivers coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushLaneKind {
    /// The session itself carries the notifications (IMAP IDLE, JMAP
    /// WebSocket, EWS streaming): coverage exists while a session is live.
    Connection,
    /// The session maintains a server registration whose notifications reach
    /// the consumer's receiver and then the engine's `InvalidationSink`
    /// (Graph webhooks, Gmail Pub/Sub): coverage outlives any one session.
    Registration,
}

/// Failure pacing for one lane, owned and applied by the engine. The fields
/// are private so the normalization in `new` cannot be bypassed (a
/// `BackoffPolicy { initial: ZERO, max: ZERO, .. }` literal would otherwise
/// pace a failing lane at zero delay forever).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    initial: Duration,
    max: Duration,
    /// Add a uniform delay in [0, base/4] to each computed delay.
    jitter: bool,
}

impl BackoffPolicy {
    /// `initial` below one millisecond is raised to one millisecond; `max`
    /// below `initial` is raised to `initial`. The only constructor.
    #[must_use]
    pub fn new(initial: Duration, max: Duration, jitter: bool) -> Self;

    #[must_use]
    pub fn initial(&self) -> Duration;
    #[must_use]
    pub fn max(&self) -> Duration;
    #[must_use]
    pub fn jitter(&self) -> bool;

    /// The delay after the `failures`-th consecutive failure (1-based),
    /// jitter excluded: `min(initial * 2^(failures - 1), max)`, saturating.
    /// `failures == 0` is `Duration::ZERO`.
    #[must_use]
    pub fn base_delay(&self, failures: u32) -> Duration;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushLaneSpec {
    pub lane: PushLane,
    pub kind: PushLaneKind,
    pub backoff: BackoffPolicy,
}

/// One item of a push session (`Account::push_stream`).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PushSessionEvent {
    /// Drive the hinted scopes. Over-approximation is always allowed.
    Invalidated(InvalidationHint),
    /// Connection lanes: the session reached live coverage (IDLE running,
    /// WebSocket subscription applied, EWS subscription created).
    /// Registration lanes: the registration is believed live.
    Established,
    /// Demonstrated healthy progress: a completed IDLE round, a frame the
    /// peer served, a complete long poll, a fully successful renewal pass.
    Healthy,
    /// Advisory; does not end the session.
    Warning(Warning),
    /// One unit of the session failed (a Graph row, an IMAP folder); the
    /// session continues or ends per the contract (3.3).
    UnitFailed(AccountError),
    /// The session ended without a classified failure. Always the last item.
    Ended(PushSessionEnd),
    /// The session ended on a classified failure. Always the last item.
    Failed(AccountError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushSessionEnd {
    /// The stop signal or the account's own shutdown ended it.
    Stopped,
    /// The subscribed topology changed and the session must be rebuilt now.
    Reconfigure,
    /// Unclassified transport loss (EOF, pong timeout, server termination,
    /// a setup step past its bound).
    Lost,
    /// Registration lanes: a maintenance pass finished with at least one
    /// non-terminal `UnitFailed`, or with units still retired by an earlier
    /// terminal failure that the consumer has not yet unsubscribed (coverage
    /// the lane knows is lost).
    Incomplete,
}

/// Engine-to-provider request that a push session end. Cloneable. Resolves
/// on `SessionStopper::stop` and also when the stopper is dropped.
#[derive(Clone)]
pub struct SessionStop(StopInner);

#[derive(Clone)]
enum StopInner {
    Signal(Shared<oneshot::Receiver<()>>),
    Never,
}

pub struct SessionStopper(Option<oneshot::Sender<()>>);

impl SessionStop {
    #[must_use]
    pub fn pair() -> (SessionStopper, SessionStop);
    /// A stop that never fires, for callers that end sessions by dropping.
    #[must_use]
    pub fn never() -> Self;
    /// Resolves once stop was requested (or the stopper dropped).
    pub async fn stopped(&self);
    /// Non-blocking check (`now_or_never` on a clone of the shared receiver).
    #[must_use]
    pub fn is_stopped(&self) -> bool;
}

impl SessionStopper {
    pub fn stop(self);
}

/// Engine-to-provider throttle gate. A Registration session awaits
/// `clear()` (selecting on its `SessionStop` and the account shutdown)
/// before every maintenance request, so an engine throttle hold recorded
/// while the session is live holds that session's next request.
#[derive(Clone)]
pub struct PushGate(Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>>);

impl PushGate {
    /// A gate that never holds, for callers without an engine.
    #[must_use]
    pub fn open() -> Self;
    #[must_use]
    pub fn new(clear: impl Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static) -> Self;
    /// Resolves once no engine hold covers this lane's work (at once for
    /// `open()`).
    pub fn clear(&self) -> BoxFuture<'static, ()>;
}
```

`SessionStop`, `SessionStopper` and `PushGate` get hand-written `Debug`
impls; the first two print only whether stop was requested, the gate only
whether it is open. `bifrost-types` has no Tokio dependency, which is why the
gate hands back a future the engine builds rather than sleeping itself. `bifrost-types` gains no dependency: the
workspace `futures` dependency (root `Cargo.toml`: `default-features = false`,
features `async-await` and `std`) already provides `futures::channel::oneshot`,
`futures::future::Shared` and `FutureExt::now_or_never` under `std`.

Addendum, the single-driver helpers every provider uses (S11):

```rust
/// One lane's single-driver lease. Cheap to clone; the account keeps one
/// per lane and each session stream takes a guard into itself.
#[derive(Debug, Clone, Default)]
pub struct LaneLease(Arc<AtomicBool>);

impl LaneLease {
    #[must_use]
    pub fn new() -> Self;
    /// `compare_exchange(false, true)`; `None` while a live session holds it.
    #[must_use]
    pub fn try_claim(&self) -> Option<LaneLeaseGuard>;
}

/// Releases the lease (`store(false)`) on drop, so a dropped stream frees it.
#[derive(Debug)]
pub struct LaneLeaseGuard(Arc<AtomicBool>);

/// `Internal(AccountContract)` for `AccountOperation::PushStream`, protocol
/// `protocol`, support-only detail naming `lane` and `detail` ("already
/// driven" or "not a lane of this account"). Derives `InternalFailure`
/// (terminal), built exactly like `crates/sync/src/recovery.rs`
/// `fresh_cursor_contract_error`.
#[must_use]
pub fn push_lane_contract_error(protocol: Protocol, lane: PushLane, detail: &'static str)
    -> AccountError;
```

### 3.2 `Account` trait changes (`crates/types/src/account.rs`)

```rust
/// The push lanes this account instance runs, constant for its life. Empty
/// when push is unavailable. The engine reads it once per account
/// generation and only when `capabilities().push != PushCapability::None`.
fn push_lanes(&self) -> Vec<PushLaneSpec> {
    Vec::new()
}

/// One push session on `lane`. See the session contract.
fn push_stream(&self, lane: PushLane, stop: SessionStop, gate: PushGate)
    -> AccountStream<PushSessionEvent>;
```

`push_stream` keeps its name and is reshaped (signature and item type), which
is a shape change under the 2026-09-30 ruling; nothing is removed or renamed.
The method's doc comment is replaced by the contract below. The doc on
`push_subscribe` keeps its per-scope text. The doc on `scope_lifecycle_stream`
is rewritten (3.5).

### 3.3 The session contract (normative; copied into the `push_stream` doc)

- S1. A session is a re-runnable unit. Every effect a session may have
  committed before it failed or was dropped is idempotent under re-run,
  self-expiring, or recorded in account state for the next session or
  `close()` to settle. Under this contract the engine treats `Reconcile` from a
  push session exactly like `Retry` (re-run after backoff, recording the
  reconcile throttle), as the lifecycle task already does.
- S2. A session never retries after a failure. Connection sessions end at
  their first failure (`Failed` or `Ended(Lost)`), with two exceptions, both
  reporting the failure as `UnitFailed` so its recovery (throttle, hint,
  directive or terminal broadcast) still reaches the engine: a failure local
  to one unit of the session's assignment (IMAP: a folder whose SELECT is
  refused with a recovery plan that targets that folder alone and removes it,
  5.1 step 6), after which the provider
  removes that unit from its assignment and the session ends
  `Ended(Reconfigure)` so the next session is built on the remaining
  assignment; and a retryable or reconcilable failure of an optional coverage
  extension (IMAP: `NOTIFY SET`), after which the session continues with its
  base coverage (the selected folder plus coarse invalidations).
- S3. Registration sessions run maintenance passes. Every session start
  performs an immediate due scan, so no cadence delay is added after a backoff
  the engine already served. A pass attempts every due unit exactly once,
  reports every unit failure as `UnitFailed`, retires terminal units in
  provider state so they never recur, and ends `Ended(Incomplete)` after the
  pass if any unit failed non-terminally OR any unit is still retired by a
  terminal failure the consumer has not yet unsubscribed; only a pass with
  neither yields `Healthy`. So a lane that has permanently lost a
  registration reads `Down` until the consumer acts, instead of `Up`. Every
  maintenance request is preceded by `gate.clear()` (S13). A whole-session
  fault ends `Failed`.
- S4. `Healthy` is yielded only for demonstrated healthy progress (see the
  variant docs). Reaching a read loop, opening a stream or a successful
  Subscribe without a completed read is not progress.
- S5. Provider-known coverage gaps are invalidated by the provider. A gap the
  provider learns of inside a healthy session is committed to account state
  atomically with the effect that revealed it, and sessions drain that state by
  removing ONE obligation immediately before each `yield` of its
  `Invalidated`, holding no lock across the yield, so a dropped session leaves
  the remainder in account state for the next session. This commit-and-drain
  rule binds Registration lanes, whose coverage outlives a session. A
  Connection session may yield an in-session gap invalidation directly (the
  IMAP `signal_*` helpers, JMAP and EWS hints): every way such a session can
  end before the yield either sets the lane's `owe_reconcile` (a lost,
  failed or reconfigured session, a new generation) or is a detach, so the
  next `Established` requests a full reconcile that covers the gap.
- S6. The terminal item (`Ended` or `Failed`) is yielded only after the
  session finished its own cleanup; the stream then returns `None`. The engine
  may drop the stream once it has the terminal item.
- S7. On stop the session performs bounded cleanup and ends `Ended(Stopped)`.
  The engine keeps polling after requesting stop (still forwarding
  `Invalidated`), so cleanup placed after a `yield` runs.
- S8. Dropping a session must never corrupt account state. Effects that must
  outlive a drop run in account-owned tasks or actors (Graph recreate and EWS
  release tasks, the Google actor), and `close()` settles or awaits what is
  still owed, bounded.
- S9. A session yields `Established` before reporting coverage-dependent
  progress. Registration sessions yield `Established` at start when the
  registration is believed live.
- S10. `UnitFailed` errors carry their own scope attribution; an error whose
  unit covers several scopes stays account-scoped.
- S11. One live session per lane. The provider enforces it with a per-lane
  lease owned by the stream (an `Arc<AtomicBool>` claimed by
  `compare_exchange` and released by a guard the stream holds, so dropping
  the stream releases it); a second concurrent `push_stream` for a driven
  lane, and a `push_stream` for a lane not in `push_lanes()`, yields one
  `Failed(push_lane_contract_error(..))` and ends. Every provider uses the
  shared `bifrost_types::LaneLease` and `push_lane_contract_error` (3.1
  addendum).
- S12. Without the engine nothing runs push. A direct consumer must drive
  every lane in `push_lanes()` (re-open on end, apply its own pacing) and hand
  `Invalidated` to its own reconciliation. `push_subscribe` only registers.
- S13. Registration sessions await `gate.clear()` before every maintenance
  request (a Graph pass's renewal of each due row, a Google `Renew`),
  selecting on stop and shutdown. Connection sessions do not consult the gate
  after setup: their wire activity past setup is the live connection itself
  (an EWS long poll included, as today), and their setup is held by the lane
  before the session opens (4.6 step 1).

### 3.4 `InvalidationSink` (`crates/types/src/events.rs`)

```rust
pub trait InvalidationSink: Send + Sync + 'static {
    fn push(&self, account: AccountId, event: WatchEvent) -> SinkPush;
}

/// What the engine did with a consumer-injected push event.
#[must_use = "a refused event is handed back for the caller to retry"]
#[derive(Debug)]
#[non_exhaustive]
pub enum SinkPush {
    /// Recorded or acted on. Invalidations may have been coalesced into a
    /// wider reconcile; that is never a loss of the reconcile itself.
    Accepted,
    /// An engine directive was not queued, and the event is returned
    /// untouched for the caller to resubmit, for one of two reasons. The
    /// account boundary does not read `Run` (paused or checkpointing): a
    /// paused account takes no recovery work, and a scope repair handed to
    /// the listener under a pause would be declined and lost, so the sink
    /// refuses instead of answering `Accepted`; resubmit after resume. Or the
    /// account's recovery queue is full: it is drained by the engine's
    /// recovery listener, which keeps draining while the account runs.
    Refused(WatchEvent),
    /// No such account is attached (never attached, or detached).
    NotAttached,
}
```

The return type is a reshape of a published method, not a removal. `SinkPush`
is re-exported from `crates/types/src/lib.rs` and from `crates/sync/src/lib.rs`
beside `InvalidationSink`.

### 3.5 Lifecycle heartbeat (`crates/types/src/cursor.rs`)

`ScopeLifecycleEvent` gains `Polled`: "a complete successful poll finished,
including any pagination burst, follow-up reads and the state commit". A
provider yields it once per such poll, never per page and never after a
partially successful poll that then failed. The enum doc and
`Account::scope_lifecycle_stream` doc are rewritten to the actual contract:
the stream runs until its first failure, which it reports as `Terminated` and
then ends; it never sleeps and retries after a failure; poll cadence for
successful polls is the provider's; the engine owns reconnect, backoff,
throttle holds and directive routing, and resets its backoff only on `Polled`
or a `Lifecycle` event. An account replacement reopens the stream but does not
reset the failure count (4.1).

### 3.6 `WatchEvent`'s role

`WatchEvent` keeps every variant. It becomes the consumer-sink vocabulary
only: what an out-of-process receiver hands to `InvalidationSink::push`. Its
doc is rewritten accordingly; `PushSource::Coalesced`'s doc changes from "the
sink overflowed" to "engine- or provider-synthesized wide wake-up".

## 4. Engine (`bifrost-sync`)

### 4.1 `ReconnectBackoff` (new `crates/sync/src/backoff.rs`, crate-private)

```rust
pub(crate) struct ReconnectBackoff {
    policy: BackoffPolicy,
    failures: u32,
}

impl ReconnectBackoff {
    /// Re-runs `BackoffPolicy::new` on the policy's values as a second
    /// guard, so no path reaches a zero delay.
    pub(crate) fn new(policy: BackoffPolicy) -> Self;
    /// A backoff carrying a count across an account generation (below).
    pub(crate) fn with_failures(policy: BackoffPolicy, failures: u32) -> Self;
    /// `policy.base_delay(failures)`: the wait a carried count earns before
    /// the first open on a new generation, zero when the count is zero.
    pub(crate) fn carried_delay(&self) -> Duration;
    pub(crate) fn reset(&mut self);
    pub(crate) fn failures(&self) -> u32;
    /// Count one failed attempt and return the delay before the next:
    /// `policy.base_delay(failures)` plus, when `policy.jitter`, the
    /// jitter `jitter_seed % (base_ms / 4 + 1)` milliseconds.
    pub(crate) fn record_failure(&mut self, jitter_seed: u64) -> Duration;
}

/// Sub-second wall-clock nanoseconds, the seed `ews_stream.rs` uses today.
pub(crate) fn jitter_seed() -> u64;
```

Delay sequence, exactly: with `n` consecutive failures since the last reset,
the wait is `min(initial * 2^(n-1), max)` plus jitter, and every caller waits
`max(that, retry hint)` where a hint exists. Resets: push `Healthy`,
lifecycle `Polled` or `Lifecycle`, and nothing else. The maximum bounds a
delay, not the attempt rate after a reset.

A new account generation does NOT reset the count. It ends the current stream
or lane set at once, and the replacement opens after `carried_delay()`: at
once when the count is zero (the old stream or lane last demonstrated health),
otherwise after the delay its last failure earned. This is what paces a
failure that persists across reopen: a session or lifecycle stream that fails
`Engine(RestartAccount)` gets its reattach, the reattach succeeds and bumps
the generation, and with a fresh count the replacement would fail at once and
request the next reattach, so the account would be reattached back to back for
as long as the condition lasts (JMAP's `CapabilityChanged -> RestartAccount`
is the documented case; `restart_account`'s `REOPEN_RETRY_BUDGET` cannot
stop it, because it counts failures within one call and every one of these
calls succeeds). With the count carried, those reattaches are spaced by the
lane's or the lifecycle reader's growing backoff. A healthy account reattached
for any other reason still reopens at once.

### 4.2 Lifecycle loop (`crates/sync/src/multiplexer/mod.rs`)

The lifecycle task replaces `reconnect_delay` with
`ReconnectBackoff::new(BackoffPolicy::new(LIFECYCLE_RECONNECT_MIN,
LIFECYCLE_RECONNECT_MAX, false))`. `Polled` and `Lifecycle(_)` call `reset()`;
`Polled` is otherwise ignored (`continue`).

Failure accounting, pinned so the fallback does not lag a step: a
`Terminated(err)` item calls `delay = record_failure(seed)` at once, then
`retry_wait = lifecycle_retry_wait(&err, delay, ..)`, so the fallback handed
to `lifecycle_retry_wait` is the delay this failure just earned (today's code
passes the pre-doubling value and doubles after the sleep, which yields the
same sequence). This is a wording pin, not a behaviour fix: the wait is
`max(delay, retry_wait)` and `lifecycle_retry_wait` returns
`max(fallback, hint)`, so a fallback lagging one step would still produce
`max(delay, hint)`; no test can bite on it and none is listed. A
stream that ends without a `Terminated` item (the Graph, CalDAV and CardDAV
empty streams) calls `record_failure` at its end and waits that. Both then
wait `max(delay, retry_wait)` as today.

A generation change no longer resets (4.1): wherever today's loop sets
`reconnect_delay = LIFECYCLE_RECONNECT_MIN` on a generation (the account-wide
hold wait, the read loop, the `AwaitReopen` and `Terminate` waits, the
post-stream sleep), the new loop keeps the count and, before opening the
replacement's stream, waits `carried_delay()` (selecting shutdown and a
further generation change). Today the `AwaitReopen` path reopens immediately
on the generation its own forward caused, which is the same unpaced reattach
loop 4.1 describes; this closes it. `lifecycle_termination` and
`lifecycle_retry_wait` are unchanged. Empty streams keep growing to 480 s
exactly as today.

### 4.3 `PushIngress` (new `crates/sync/src/push/ingress.rs`, public)

Holds only invalidation intent. Every method is synchronous and safe from any
thread, on or off a Tokio runtime.

```rust
#[derive(Debug)]
pub struct PushIngress {
    state: std::sync::Mutex<PendingPush>,
    wake: tokio::sync::Notify,
    capacity: usize,
    coarsened: Arc<AtomicU64>,
}

#[derive(Debug, Default, Clone)]
pub struct PendingPush {
    pub full: bool,
    pub scopes: HashSet<CursorScope>,
    pub memberships: HashSet<MembershipScope>,
    pub closed: bool,
}

impl PushIngress {
    /// `capacity` 0 is floored to 1. Owns a fresh coarsening counter.
    #[must_use]
    pub fn new(capacity: usize) -> Self;
    pub(crate) fn with_counter(capacity: usize, coarsened: Arc<AtomicU64>) -> Self;
    /// `SpecificCursorScope` inserts the scope, `SpecificMembership` the
    /// membership, `Unknown` and any future variant set `full`. When not
    /// `full` and `scopes.len() + memberships.len() > capacity`, set `full`,
    /// clear both sets and count one coarsening. No-op once closed. Always
    /// `notify_one` after releasing the lock.
    pub fn invalidate(&self, payload: &HintPayload);
    pub fn request_full(&self);
    /// Mark closed and wake; later invalidations are ignored.
    pub fn close(&self);
    /// Move the pending state out, leaving an empty state with `closed`
    /// preserved.
    #[must_use]
    pub fn take(&self) -> PendingPush;
    /// Wait for a wake. `Notify` stores one permit, so a push between the
    /// consumer's `take` and its next wait is never missed.
    pub async fn changed(&self);
    #[must_use]
    pub fn coarsened(&self) -> u64;
}
```

`MultiplexerConfig::watch_capacity` keeps its name and default (256) and is
redefined in its doc as "distinct hinted scopes and memberships the pending
push state holds before collapsing to one full reconcile".

### 4.4 Sink targets and synchronous control handling (`crates/sync/src/push/mod.rs`)

```rust
#[derive(Debug, Clone)]
pub struct SinkTarget {
    pub ingress: Arc<PushIngress>,
    pub delivery: Arc<ChangeDelivery>,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    pub throttles: Arc<std::sync::Mutex<ThrottleBucket>>,
    /// The account's boundary, read synchronously (`peek`) by the directive
    /// arm below.
    pub boundary: BoundaryView,
}

pub struct InvalidationSinkInner {
    targets: DashMap<AccountId, SinkTarget>,
    coarsened: Arc<AtomicU64>,
}
```

- `register(&self, account: AccountId, target: SinkTarget)` replaces the
  mpsc-taking signature (still `pub`). `unregister(&self, account)` removes the
  target and calls `target.ingress.close()`. `dropped()` and the
  `bifrost_sync_push_dropped_total` metric keep their names; their docs are
  redefined as the number of coarsenings of distinct hints into a full
  reconcile (ingress and work set). A coarsening loses no reconcile. `new_ingress(&self, capacity) -> Arc<PushIngress>` builds an
  ingress sharing the engine counter. `shared_sink` stays.
- `forward_events`, `SinkSender`, `requires_lossless_delivery` and
  `coalesced_event` are deleted with every test that pinned them
  (`full_sink_preserves_warning_from_a_non_runtime_thread`,
  `full_sink_preserves_terminated_classification`,
  `full_sink_counts_a_coalesced_invalidation_as_dropped`,
  `queued_forwarder_cannot_deliver_a_later_event_first`,
  `no_runtime_discard_of_a_lossless_event_is_counted`,
  `full_reconcile_cannot_represent_control_events`,
  `only_control_information_requires_lossless_delivery`), and the code comments
  in `register` and `forward_events` describing the open policy go with them.
- `pub(crate) fn route_watch_event(target: &SinkTarget, account: &AccountId,
  event: WatchEvent) -> SinkPush`, synchronous, is the single handler.
  `InvalidationSink::push` looks up the target (`NotAttached` if absent) and
  calls it:
  - `Invalidated { hint }`: `ingress.invalidate(&hint.payload)`; `Accepted`.
  - `Reconnected`: `ingress.request_full()`; `Accepted`.
  - `Disconnected`: broadcast the existing account-scoped
    `WarningKind::Other` "push transport disconnected" on
    `delivery.sender()`; `Accepted`.
  - `Warning(w)`: broadcast `SyncEvent::Warning(w)` account-scoped; `Accepted`.
  - `Terminated(error)` through `plan_recovery`: `Retry` records the throttle
    and broadcasts the existing "push stream terminated with retryable
    recovery" warning; `Reconcile` records the reconcile throttle and the same
    warning; `Terminal` broadcasts `SyncEvent::Terminated(error)` scoped
    `CursorScope::Account`, exactly as the reconciler does today (see
    "Terminated scope" below); `Engine(directive)`: when `boundary.peek()` is
    not `Run`, hand the event back as `Refused(WatchEvent::Terminated(error))`
    without queueing it; otherwise `try_send` `ReopenRequest::Recovery {
    scope: directive_target_scope(&directive), error }` - `Ok` is
    `Accepted`, `Full` hands the error back as
    `Refused(WatchEvent::Terminated(error))`, `Closed` is `NotAttached`.
  - any future `WatchEvent` variant: `request_full()`; `Accepted`.
  Broadcast sends are synchronous and lossy only through the broadcast ring's
  own lag, exactly as the reconciler's sends were. Nothing control-shaped is
  stored, so nothing needs an overflow policy.

Directives under a pause, a deliberate behaviour change: today a consumer
`Terminated(Engine(_))` waits in the watch channel while the reconciler parks
on `Pause` and is dispatched after resume. Handling it at push time without
the boundary check would hand a `RestartScope` or
`DowngradeCapabilityForScope` to the listener during the pause, where
`begin_scope_repair` declines it; the scope still has a cursor, so no
`RecoveryFollowUp::ParkScope` is produced, and the consumer would have been
told `Accepted` for a directive that is gone unless polling happens to hit the
same failure. A queued `RestartAccount` would also hold the serial listener for
the whole pause. Refusing while not `Run` returns the obligation to the caller.
Residual, unchanged from today's poll path: a directive accepted while `Run`
whose dispatch is overtaken by a pause landing before the listener reaches it
is declined like any other declined repair.

Ordering, deliberately not preserved: SYNC-009 names termination
classifications "or their ordering". Control events are acted on at push time
and so overtake invalidations still pending in the ingress. That is harmless:
an invalidation is an idempotent wake-up whose effect does not depend on what
was decided about control events before or after it, a `Terminal` or
directive outcome never suppresses a pending reconcile (the work set keeps it,
4.5), and `Reconnected` only adds a full reconcile. Nothing downstream reads
the relative order.

Terminated scope: every push-originated `SyncEvent::Terminated` (sink, lane
`UnitFailed`, lane `Failed`) is scoped `CursorScope::Account`, as the
reconciler and the lifecycle reader scope theirs today. On the poll path a
cursor-scoped `Terminated` means that scope's poll is parked; a push failure
parks no poll, so scoping it to the folder would let a consumer misread a
push-only failure as "this scope is dead". The error itself still carries its
scope attribution for the consumer to read. Whether a push unit failure should
be a `Terminated` at all is an owner question (section 12, Q3); this spec
keeps today's event.

### 4.5 Reconciler work set (`crates/sync/src/push/reconciler.rs`)

- `Reconciler` gains `pub account_generation: watch::Receiver<u64>`;
  `run(self, ingress: Arc<PushIngress>)` replaces `run(self, rx)`. `handle`
  and the `Disconnected` / `Reconnected` / `Terminated` arms are deleted (4.4
  owns them).
- A private `WorkSet { full: bool, scopes: HashSet<CursorScope>, hold:
  Option<(u64, tokio::time::Instant)> }` lives for the task's life, across
  pauses.
- Loop: park while the boundary reads `Pause` OR `CheckpointNow` (the work
  set and the ingress keep accumulating, both bounded); `Run` resumes, and
  `Stop` falls through to the drives, which answer `Stopped`. Parking on
  `CheckpointNow` too is required, not cosmetic: `drive_changes_stream`
  begins with `control.begin_activity()`, which refuses whenever the boundary
  is not `Run`, and `refused_activity_outcome` maps `CheckpointNow` to
  `Paused`; `checkpoint_now` holds `CheckpointNow` (`CheckpointLatchGuard`)
  until the account reaches quiescence, which waits on consumer
  acknowledgements. With a `Pause`-only park, a `Paused` drive's re-inserted
  scope would make the work set ready again at once and the task would loop
  sweep, admission, `with_drive`, refusal, re-insert for the whole
  checkpoint. This matches `SyncControl::wait_until_running`, which treats
  `Pause` and `CheckpointNow` alike. Then select on shutdown, on
  `ingress.changed()`, on the existing deferred-scope timer, and - when `hold`
  is set - on `account_generation.changed()` and the hold instant. On every
  wake, move the ingress into the work set: `take()`, OR `full` in, add
  `scopes`, resolve every membership through
  `cursors.scopes_for_membership`. If the taken state is `closed`, finish the
  current sweep and return (detach discards attachment-local work).
- Ready internal work is a reason to sweep BEFORE waiting: at the top of
  every iteration, after the pause park, if the work set is non-empty and no
  hold is active the task merges the ingress (non-blocking `take`) and sweeps
  without selecting on anything; it waits only when the work set is empty or
  held. So a scope re-inserted by a `Paused` drive is swept as soon as the
  boundary reads `Run` again, with no ingress notification needed, and never
  before (the park above runs first).
- The work set is bounded like the ingress: when a MERGE of ingress hints
  leaves `scopes.len()` above `watch_capacity`, set `full`, clear `scopes` and
  count one coarsening (the same counter). A union of bounded snapshots
  accumulated during a hold therefore never grows past the cap. The check runs
  on merges only, never on the scopes a sweep itself resolved from `full`
  (a registry larger than the cap is legal; a coarsened remainder simply
  resolves to every scope again at the next sweep, which is wider, never
  narrower).
- A sweep runs when the work set is non-empty and no hold is active (a hold
  clears on a generation change or at its instant). It first resolves `full`
  into `cursors.all_scopes()` at that moment and clears the flag, then
  dispatches scopes. `drive_scopes` takes `&mut WorkSet` and removes a scope
  from it at every decision it reaches about that scope, with exactly three
  exceptions: a scope-local hold moves it to the existing `deferred` set; a
  `Paused` drive puts it back; an account-wide directive stops the sweep
  before the remaining scopes are reached (they stay). So a terminally parked
  scope, a scope whose `with_drive` answers `None` (deleted or left
  mid-drive), a refused admission and every completed or terminated drive all
  discharge the scope; an undispatchable scope can never stay ready and spin
  the sweep. Everything else it does today stays (parking, account-wide and
  scope-local holds, admission, the last-check hold, `with_drive`,
  account-error normalization, deferral).
- Outcome changes: `ChangesEvent::Paused` re-inserts the scope into the work
  set; `Stopped` drops it (detach). An `Engine` directive with no target scope
  stops dispatch, keeps every undispatched scope, and sets `hold =
  (current generation, now + 30 s)` (the same bound as the lifecycle
  `LIFECYCLE_REOPEN_WAIT`), preserving the existing intent of not driving
  cursors the engine is about to discard without losing the obligation. The
  stored generation is read with `account_generation.borrow_and_update()`
  when the hold is set, and a wake from `account_generation.changed()` clears
  the hold only when the value it then reads differs from the stored one. A
  bare `changed()` is not enough: a receiver that never marked an earlier
  bump seen returns from `changed()` at once and would clear the hold
  spuriously.

### 4.6 Lane supervisor and lane loop (new `crates/sync/src/push/lanes.rs`)

```rust
pub(crate) struct PushLanes {
    pub account_id: AccountId,
    pub current: Arc<ArcSwap<Arc<dyn Account>>>,
    pub generation: watch::Receiver<u64>,
    pub topology: watch::Receiver<u64>,
    pub ingress: Arc<PushIngress>,
    pub delivery: Arc<ChangeDelivery>,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    pub throttles: Arc<std::sync::Mutex<ThrottleBucket>>,
    pub boundary: BoundaryView,
    pub shutdown: CancellationToken,
    pub health: watch::Sender<PushHealth>,
    /// The highest account generation whose lane set has fully drained.
    /// Reattach waits on it before closing the account it replaced (4.10).
    pub retired: watch::Sender<u64>,
    /// `EngineConfig::detach_timeout / 2`, so a drain that uses its whole
    /// bound still finishes well inside detach's worker deadline (4.9).
    pub drain_bound: Duration,
}
```

Supervisor `run(self)`: loop until shutdown. Read the generation
(`borrow_and_update`), load the account, compute `lanes` (empty when
`capabilities().push == None`), publish a fresh `PushHealth { generation,
lanes: all Starting }`, create a generation stop token (child of shutdown),
and run every lane future concurrently in this one task (`join_all`, pinned).
Select on the joined future, on `generation.changed()` and on shutdown; on
either signal cancel the generation token and keep awaiting the SAME joined
future, which completes once every lane drained (each bounded by
`drain_bound`). Once the set has drained, `retired.send_replace(generation)`.
On shutdown return; on a generation change start the next set.
If the joined future completes FIRST (always at once for an empty lane set; a
lane returns early only after its account shut down under it), the
supervisor publishes `retired` at once (that set has drained), waits for
`generation.changed()` or shutdown and never rebuilds the same set - so an
account without push costs one parked task.

Each lane future returns its final consecutive-failure count. The supervisor
keeps them by `PushLane` and seeds the next set's lane with the same id from
it (zero for a lane id the old set did not have), which is how a count
crosses a generation (4.1).

This sequencing is the generation fence for lane STATE: a retiring set
finishes before its replacement exists, so no old outcome can touch
replacement lane state or health. It does not, by itself, fence recovery
requests a retiring lane already submitted to the reopen channel; those
survive the drain, and the listener processes `Recovery` without generation
attribution, so two old-session directives could otherwise queue, the first
replace the account and the second reset or disable replacement state. Lanes
therefore submit `ReopenRequest::PushRecovery { scope, error, generation }`
(a new variant of the published `#[non_exhaustive]` enum), stamped with the
lane's generation, and the listener validates the stamp at execution:
`recover` gains `expected_generation: Option<u64>` (`None` for every other
request kind, which keeps today's behaviour), and every engine-directive arm
compares it with the current account generation AFTER acquiring the reopen
lock and before any effect - for the scope repairs, `SchemaIncompatible`
and `OperatorOverrideRequired` right after `handle_account_error` takes the
lock; for `RestartAccount` and `DowngradeStrategy` inside `restart_account`'s
admitted loop right after `lock_owned`, so a consumer reattach that swapped
and bumped while this request waited is observed. A mismatch declines the
request as stale with a debug log: the condition belonged to a replaced
account, and if it persists the replacement's own sessions raise it again.
Checking under the lock matters because reattach swaps the account before it
bumps the generation, and both happen under that lock. Retry, Reconcile and
Terminal plans never reach the listener from a lane.

Health writes: each lane owns the entry at its own index in
`PushHealth::lanes` (lanes are numbered in `push_lanes()` order) and updates
it only through `health.send_modify(|h| h.lanes[index].state = ..)`, which is
atomic per update, so concurrent lanes never overwrite each other's entries.
The down-edge check reads the other entries inside the same `send_modify`
closure.

Lane loop `run_lane(ctx, account, spec, generation, stop, carried: u32) ->
u32` keeps lane-local state: `backoff: ReconnectBackoff::with_failures(
spec.backoff, carried)`, `owe_reconcile = generation > 0 && kind ==
Connection`, `pending: VecDeque<ReopenRequest>` (every entry a stamped
`PushRecovery`), `last_error: Option<AccountError>` (the latest error this
lane reported through `UnitFailed` or `Failed` since its last `Healthy`) and
`blind_reconfigures: u32` (consecutive `Reconfigure` outcomes from sessions
that never yielded `Established`). It returns `backoff.failures()`.
For `kind == Registration && generation > 0` it calls
`ingress.request_full()` once at lane start (cutover debt; old-account gap
obligations die with that account). Before its first session it waits
`backoff.carried_delay()`, selecting on `stop` (zero unless the previous
generation's lane with this id ended on unrecovered failures, 4.1). Then:

1. Flush `pending` completely, submitting only while the boundary reads
   `Run`: each entry waits for `boundary.peek() == Run` and then
   `reopen_tx.reserve()`, both selecting on `stop`. A new session never opens
   while `pending` is non-empty, so `pending` is bounded by one session's unit
   failures plus one, and a session ending at EOF cannot leave queued
   directives behind it. The boundary gate is what keeps a pause from
   stacking directives: the listener does not drain `RestartAccount` during a
   pause (`restart_account` begins with `wait_until_running` and the
   listener is serial), and `RestartScope` / `DowngradeCapabilityForScope`
   handed to it under a pause are declined by `begin_scope_repair`. Without
   the gate a lane whose sessions keep failing `Engine(RestartAccount)`
   through a long pause would submit one more request per failed session,
   fill the shared 16-slot reopen channel (blocking poll tasks, the
   deferred-inventory worker and the reconciler on their sends, and making
   the consumer sink refuse unrelated directives), and after resume the
   listener would reattach once per queued duplicate; an
   `OperatorOverrideRequired` would re-pause the paused account and
   re-broadcast its warning every backoff period. With the gate a failing
   lane holds at most one undelivered directive through any pause, submits
   it after resume, and the generation stamp (above) makes any duplicate
   that still reaches the listener stale once the first reattach lands. The
   cost: a lane holding an undelivered directive opens no session until
   resume, which loses push latency for a paused account only; polling is
   unaffected. Then wait out account-wide holds (`account_throttle_wait`), selecting on
   `stop`.
2. Snapshot `topo_seen` (`topology.borrow_and_update()`), create
   `SessionStop::pair()`, build the session's `PushGate` (a closure returning
   a future that loops `while let Some(wait) = account_throttle_wait(..)`
   sleeping `wait`, so a hold recorded while the session is live, by this
   lane's own `UnitFailed` or by any other engine work, holds the session's
   next maintenance request, S13), open `account.push_stream(spec.lane,
   session_stop, gate)`, and clear the session's `max_unit_hint` and
   `established` flag.
3. Poll loop - `select!` over: `stop.cancelled()` (enter drain, step 5); a
   `reopen_tx.reserve()` arm enabled only while `pending` is non-empty and
   the boundary reads `Run` (send the front), with a `boundary.changed()` arm
   while `pending` is non-empty so a resume enables it; `session.next()`:
   - `Invalidated(h)`: `ingress.invalidate(&h.payload)`.
   - `Established`: set `established`; `blind_reconfigures = 0`. Connection -
     health `Up`; if `owe_reconcile`, call `ingress.request_full()` and clear
     it. Registration - health `Up` only if the lane is `Starting` (a
     Registration session yields `Established` at its start, before its pass
     proves anything, so it must not clear `Down`; recovery to `Up` is
     `Healthy`).
   - `Healthy`: `backoff.reset()`; `last_error = None`; health `Up`.
   - `Warning(w)`: broadcast account-scoped.
   - `UnitFailed(e)` by `plan_recovery`, after `last_error = Some(e.clone())`:
     `Retry` / `Reconcile` record the (reconcile) throttle and raise
     `max_unit_hint` to the advice's hint; `Engine(d)` pushes `PushRecovery {
     scope: directive_target_scope(&d), error, generation }` onto `pending`;
     `Terminal` broadcasts `SyncEvent::Terminated` scoped
     `CursorScope::Account` (4.4).
   - terminal item or `None` (a `None` without a terminal item is logged as a
     contract violation and treated as `Ended(Lost)`): leave the poll loop with
     that outcome; drop the stream.
4. Outcome:
   - `Ended(Reconfigure)`: Connection sets `owe_reconcile` and sets its health
     entry `Starting` (the session that covered something is gone and the
     next one has not established; see 4.7 for what `Starting` means).
     If the session never yielded `Established`, `blind_reconfigures += 1`.
     Restart at once (step 1), `n` unchanged - except that from the fourth
     consecutive blind `Reconfigure` on, the restart first waits
     `backoff.record_failure(seed)` (no down edge: nothing failed) and a
     warn-level log names the lane as a likely provider contract violation.
     A burst of legitimate exclusions (each costs one blind session) is thus
     unpaced for three sessions and paced after, and a topology watch that
     never settles cannot hot-loop dials.
   - `Ended(Lost)`: down edge (below); Connection sets `owe_reconcile`; wait
     `backoff.record_failure(seed)`.
   - `Ended(Incomplete)`: down edge; wait `max(record_failure, max_unit_hint)`.
   - `Ended(Stopped)` outside drain: the account shut down under the lane; wait
     for `stop` and return.
   - `Failed(e)` by `plan_recovery`, after `last_error = Some(e.clone())`:
     `Retry(a)` - record the throttle, down edge, Connection owes, wait
     `max(record_failure, retry_delay(a))`; `Reconcile(a)` - the same with
     the reconcile throttle and `reconcile_delay`; `Engine(d)` - push the
     stamped recovery request onto `pending`, down edge, Connection owes;
     flush `pending` (stop-selectable, boundary-gated as in step 1); only once
     that request has been SUBMITTED, if `d` is `RestartAccount`, wait up to
     30 s (cut short by `stop`, which a generation change cancels); then wait
     the delay the down edge's `record_failure` returned; `Terminal(f)` -
     broadcast `SyncEvent::Terminated` once, scoped `CursorScope::Account`
     (4.4); Connection: sets `owe_reconcile` (the lane covers nothing for the
     whole park, so its next `Established` owes a full reconcile like every
     other coverage loss), health `Parked`, and if `topology` changed since
     `topo_seen` restart at once, else wait for `topology.changed()` or
     `stop`; Registration: down edge and wait `record_failure` (the provider
     retired the failing unit or state, so the next session parks inside
     itself).
   - Every wait selects on `stop` and keeps flushing `pending` through the
     boundary-gated reserve arm.
5. Drain (stop observed): set this lane's entry `Draining` (the published
   `PushHealth` is still this retiring generation's until the supervisor
   publishes the next set), `stopper.stop()`, then keep polling the session up
   to `drain_bound`: forward `Invalidated` to the ingress and ignore every
   other item (no reopen send, no broadcast, no park, no further health or
   backoff write); discard `pending`; drop the stream at its terminal item, at
   `None` or at the bound; return.

Down edge: every outcome that waits on the backoff calls `record_failure`
FIRST and only then publishes the lane `Down { consecutive_failures:
backoff.failures(), last_error: last_error.clone() }`, so the published count
includes this failure (an `Incomplete` caused only by units an earlier
terminal failure retired carries that terminal error, or `None` once a
`Healthy` cleared it and a later generation never saw it); if no lane of this generation was `Down` or `Parked` before, it
broadcasts the existing "push transport disconnected" warning once. `Up`
after `Down` broadcasts nothing (the reconcile is the signal, as today).

### 4.7 Push health (new `crates/sync/src/push/health.rs`, public)

```rust
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PushHealth {
    pub generation: u64,
    pub lanes: Vec<PushLaneHealth>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PushLaneHealth {
    pub lane: PushLane,
    pub kind: PushLaneKind,
    pub state: PushLaneState,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PushLaneState {
    Starting,
    Up,
    Down { consecutive_failures: u32, last_error: Option<AccountError> },
    Parked { error: AccountError },
    Draining,
}

impl PushHealth {
    /// True iff some lane is `Down` or `Parked`; `Starting`, `Up` and
    /// `Draining` are not down.
    #[must_use]
    pub fn any_down(&self) -> bool;
}
```

What each state means, documented on the variants:

- `Starting`: the lane holds no established coverage and has no unrecovered
  failure. That covers a lane connecting, a Connection lane rebuilding after
  `Reconfigure`, and a lane parked with nothing to cover - an IMAP lane with
  no assignment (every non-NOTIFY lane past the subscribed folder count,
  every lane before the first subscribe), an EWS or Graph session with no
  registration, a Google session with no watch on a lane that never failed.
  `Starting` is never down: an idle lane is not a failure.
- `Up`: the lane's current session established coverage (Connection), or the
  registration is believed live with no failure or retired unit outstanding
  (Registration: set by `Established` on a lane still `Starting`, and by
  `Healthy` - a fully successful pass, a renewal, or a fresh Google watch).
  A Connection lane leaves `Up` the moment its session ends: `Reconfigure`
  sets `Starting`, every failure sets `Down` or `Parked`.
- `Down`: the lane's last outcome was a failure, or (Registration) a unit is
  still retired by a terminal failure, and no `Healthy` has followed.
- `Parked`: a Connection lane stopped on a terminal session failure and waits
  for a topology bump or a new generation.
- `Draining`: the lane's generation is retiring.

`SyncEngine::push_health(&self, account_id: &AccountId) ->
Result<watch::Receiver<PushHealth>, Error>` (`AccountNotAttached` when absent)
returns a receiver on the slot's `push_health_tx`. A late subscriber reads the
current state, which is the structural form of the absorbed "push health as
state" proposal. `crates/sync/src/lib.rs` re-exports `PushHealth`,
`PushLaneHealth`, `PushLaneState`.

### 4.8 Push topology version

`AccountSlot` gains `push_topology_tx: watch::Sender<u64>`. A private guard
`TopologyBump<'a>(&'a watch::Sender<u64>)` bumps on drop. Every engine call
of `Account::push_subscribe` or `push_unsubscribe` outside reattach holds one
across the provider call: `subscribe_push` (including its detach-race
cleanup), `unsubscribe_push` (so `detach_with_teardown` too), and the orphan
retrier. The bump therefore happens after the provider commit on completion
and on cancellation alike (a cancel before the commit bumps spuriously, which
only restarts a parked lane once). IMAP, JMAP and EWS commit inside the call.
Reattach needs no bump: its generation bump restarts every lane.

### 4.9 Wiring

Landing 2 part (ingress and sink; needs nothing from 4.6-4.8):

- `engine/attach.rs`: the `watch_tx` / `watch_rx` channel is deleted. After
  the reopen channel is created, build `ingress =
  self.sink.new_ingress(config.multiplexer.watch_capacity)` and
  `register(account_id, SinkTarget { ingress, delivery, reopen_tx, throttles,
  boundary: boundary_view.clone() })`. The reconciler is spawned with
  `run(ingress)` and a clone of the `account_generation` receiver. The
  forwarder stays as the interim forwarder described in section 7, landing 2.
- `crates/sync/src/types.rs`: the `watch_capacity` doc changes (4.3).
- `engine/mod.rs`: `sink.unregister` (which now closes the ingress) stays
  where it is in `detach_inner`.

Landing 3 part (lanes, health, topology):

- `engine/attach.rs`: the interim forwarder is deleted; `push_health_tx`
  (initial value: generation 0, every lane of the opened account `Starting`),
  `push_topology_tx` and `push_retired_tx` (initial 0, meaning nothing
  retired yet; generation 0's set publishes 0 when it drains, and reattach
  only ever waits for a generation it is replacing) are created and stored on
  the slot; `PushLanes::run` is spawned under `WorkerRole::PushLanes` with
  `drain_bound = detach_timeout / 2`.
- `crates/sync/src/types.rs`: `WorkerRole::PushForwarder` becomes
  `PushLanes` (crate-private); `AccountSlot` gains `push_health_tx`,
  `push_topology_tx` and `push_retired_tx`.
- `engine/mod.rs`: `push_health`; `TopologyBump` at the sites in 4.8;
  `detach_inner` keeps its order (workers including `PushLanes` are stopped
  and drained in the worker phase, then the writer, then `close()`).
- Drain bound versus the detach deadline: `detach_inner` cancels the slot
  shutdown and awaits every worker against ONE deadline of `detach_timeout`,
  then aborts. Were `drain_bound` equal to `detach_timeout`, a drain that used
  its bound would be aborted at the same instant rather than complete, since
  the supervisor's drain starts a hair after the cancel. Half the timeout
  leaves the other half for the lane loop to drop its stream and return and
  for the supervisor to publish `retired`. When a session's own cleanup is
  longer than the bound - IMAP's stop path is `logout()` bounded by
  `command_timeout`, 60 s by default - the lane drops the session at the
  bound: the in-flight LOGOUT is abandoned and the socket closes with the
  dropped connection, the server ends its session on the close, and the
  `push_slots` permit, which lives in the driver exit slot, is released when
  the driver exits. Nothing in account state depends on the LOGOUT
  completing (S8).
- `engine/reattach.rs` and `RecoveryContext` (4.10).

### 4.10 Reattach waits for the retiring lane set, and the listener checks stamps

`reattach_account_paced` bumps the generation and then closes the account it
replaced. Without a wait, the retiring lane set's S7 cleanup (an IMAP LOGOUT
on its session connection, a JMAP session's teardown) would run concurrently
with the old account's `close()` (IMAP pool teardown, JMAP
`disable_for_close`). `RecoveryContext` therefore gains `push_retired:
&watch::Receiver<u64>` and `push_drain_wait: Duration` (`detach_timeout`),
and after the generation bump, immediately before `guard.close_owed()`,
reattach waits until `push_retired` reads at least the generation it is
replacing, bounded by `push_drain_wait` (the lane drain itself is bounded by
`drain_bound`, half of that). On expiry it logs and closes anyway, which is
the S8 case: every provider's close must already tolerate a session that was
dropped rather than drained. The wait runs under the reopen lock reattach
already holds there, so a detach queued on that lock waits at most this bound
longer. It cannot deadlock against the listener it may be running inside
(`restart_account`): the retiring lanes need neither the reopen lock nor the
listener to drain, a lane blocked on `reopen_tx.reserve()` is released by its
stop token, which the generation bump before the wait has already cancelled,
and the drain discards `pending` instead of sending it. Graph EWS needed no wait (the release ledger serializes it); the wait
is uniform anyway so no provider's teardown order is left to analysis.

The listener-side stamp check is specified in 4.6 ("generation fence").

## 5. Providers

### 5.1 IMAP (`crates/imap/src/account/push.rs`, `mod.rs`, `close.rs`, `Cargo.toml`)

`Cargo.toml` gains `async-stream = { workspace = true }`.

`PushState` keeps `supports_notify`, `push_capacity`, `push_slots` and
`next_id`, and replaces everything else: `tx`, `pending_events`,
`task_cancel`, `resubscribe`, `terminal`, `publish_event`, `subscribe_events`,
`stop`, `retain_pending_event` and `coalesced_invalidation` are deleted. New:

- `scopes: Mutex<PushScopes>` replaces `scopes: Mutex<HashMap<String,
  HashSet<CursorScope>>>`, with `struct PushScopes { by_handle:
  HashMap<String, HashSet<CursorScope>>, version: u64, exclusions:
  HashMap<String, u64> }`. `version` is the subscription version: incremented
  INSIDE the same guard as every insert (`push_subscribe`) and removal
  (`push_unsubscribe`), so no reader can see a new set with an old version.
  `exclusions` maps a folder account id to the version it was excluded under;
  an entry whose version differs from `version` is void and pruned on read.
  Exclusions are read, validated and written only under this guard.
- `assignment: tokio::sync::watch::Sender<u64>`, bumped (after the guard is
  released) on every subscription change and every exclusion insert;
  sessions watch it instead of `resubscribe`, with the same
  mark-unchanged-before-choosing discipline;
- `leases: Vec<LaneLease>`, one per lane, built at open.

`push_subscribe` keeps admission unchanged (it already runs under the scopes
guard), drops `ensure_idle_tasks` and the terminal clear, increments the
version in the guard and bumps `assignment`. `push_unsubscribe` does the same
on a removal.
`ImapAccount::push_lanes()`: `push_capacity == 0` gives none; NOTIFY gives
`[PushLane(0)]`; otherwise `0..push_capacity`; kind `Connection`, backoff
`BackoffPolicy::new(5 s, 300 s, false)` (the old `INITIAL_REDIAL_BACKOFF` /
`MAX_REDIAL_BACKOFF`, which move here). `close.rs` drops `push.stop()`;
sessions observe `account.shutdown`.

`choose_assignment(account, slot) -> Option<(MailboxName, u64)>` replaces
`choose_idle_folder`. Under one scopes guard: compute the unfiltered
`subscribed_idle_folders(&by_handle)`; if `by_handle` is empty return `None`;
if the unfiltered list is empty (the guard case the old fallback exists for)
apply the existing slot-0 INBOX fallback; otherwise drop every folder with a
live exclusion and take the `slot`-th remaining entry, returning `None` when
there is none - the INBOX fallback never bypasses exclusions. The returned
version is `PushScopes::version` from the same guard. `exclude(account,
folder, version)` inserts only if `PushScopes::version == version` (otherwise
the attempt was made under a stale subscription set and the decision is
discarded), and reports whether it inserted; the caller bumps `assignment`
either way.

`idle_session(account, lane, stop)` (async-stream) replaces `idle_loop`:

1. A lane index outside `leases` yields the S11 failure; otherwise take that
   lane's lease or yield the S11 failure. The guard lives in the stream state
   and releases on stream end or drop.
2. `assignment.mark_unchanged()`; `choose_assignment`. None: wait for an
   assignment change, `stop` or shutdown (the last two end `Stopped`); an
   unassigned or all-excluded lane parks here on the latched version and never
   cycles `Reconfigure`.
3. Capacity phase (`reserve_push_session`, unchanged): an assignment change
   re-chooses and keeps the reservation, or drops it and returns to step 2.
4. Dial, interrupted only by `stop` or shutdown. Error: `Failed(classified)`.
   Dial failures never exclude a folder.
5. Re-choose after the dial (unchanged rule); None drops the connection and
   returns to step 2.
6. SELECT. Error classified as `PushStream` with the folder scope
   (unchanged), then routed by its recovery PLAN, never by its kind: every
   push error is built `with_folder_scope(folder)`, so "scoped to this
   folder" is always true and says nothing, and kind `SyncState` spans
   account-wide classes (`CapabilityChanged -> RestartAccount`,
   `SchemaIncompatible`, `OperatorOverrideNeeded`, `StrategyFailure ->
   DowngradeStrategy`). Folder-local, and so excluded, are exactly: a
   `Terminal` plan (for example `NotFound`, which derives `ProviderRefused`,
   or a terminal `Authorization` class), and `Engine(DisableScope(s))` where
   `s` is this folder's cursor scope - the two outcomes after which the
   folder will not be served by this account until something changes. For
   those: `exclude(folder, snapshot_version)`, bump `assignment`, yield
   `UnitFailed(e)`, drop the connection and end `Reconfigure` (S2
   exception). Every other plan - `Retry`, `Reconcile`, an account-wide
   directive, and the folder-scoped repairs that may succeed
   (`RestartScope`, `DowngradeCapabilityForScope`) - ends `Failed(e)`, so
   the engine routes the directive and applies the `Failed(Engine)` arm's
   pacing (an account-wide directive reported as a unit failure would
   restart at once with `n` unchanged and skip the `RestartAccount` wait),
   and a repaired folder is selected again by the next session instead of
   staying excluded for the life of the attachment.
7. `register_notify` (unchanged). `Ok(registered)` feeds
   `signal_notify_coverage_loss`. An error is first classified exactly as
   today (`PushStream`, folder scope), then routed by its plan: `Retry` or
   `Reconcile` (for example `NO [LIMIT]`, rate limited) - yield
   `UnitFailed(e)` so the engine records its throttle, and continue with
   `registered = false` (S2 exception); `Engine` - end `Failed(e)`; terminal -
   degrade only when BOTH the raw error is `crate::Error::No` or
   `crate::Error::Bad` answered to `NOTIFY SET` AND the classified kind is a
   command refusal: `AccountErrorKind::Request(_)`,
   `AccountErrorKind::Unsupported(_)` or
   `AccountErrorKind::Server(ServerErrorKind::Error { .. })` (`[BADEVENT]`, an
   unsupported filter, a withdrawn capability, a bare refusal). Then yield
   `Warning(support_only "NOTIFY SET refused: <message key>")` and continue
   with `registered = false`, so the account degrades to selected-folder
   coverage plus coarse invalidations, the documented contract for a NOTIFY
   collapse. Every other terminal error - including a tagged `NO` whose code
   classifies as `Authentication(_)` or `Authorization(_)`
   (`[AUTHENTICATIONFAILED]`, `[EXPIRED]`, `[NOPERM]`, policy codes), a `Bye`,
   a broken connection - ends `Failed(e)`. The classification decides at
   every step; the raw variant only further narrows the terminal arm.
8. If the reservation waited, the `signal_coverage_established`
   invalidations. Yield `Established`.
9. IDLE rounds on this connection, each with a round cancel token cancelled by
   an assignment change, `stop` or shutdown:
   - interrupted by an assignment change, same folder: re-IDLE, re-register
     NOTIFY (step 7 handling), publish the round's event (unchanged);
   - interrupted, different folder or none: absorb, `signal_idle_interrupt_loss`,
     drop the connection, end `Reconfigure`;
   - interrupted by `stop` or shutdown: `logout()` bounded by
     `command_timeout`, end `Stopped`;
   - a completed round: absorb (absorb failure invalidates `Unknown` as today),
     yield `Healthy`, then the mapped `Invalidated`;
   - `ServerTerminated`: end `Lost`;
   - `Bye` and IDLE errors: classify as today (`PushStream`, folder scope)
     and end `Failed(e)` whatever the plan. They are connection-level: a
     `Bye` whose code classifies `Authorization` says the connection was
     refused, not which folder failed, so excluding the selected folder would
     be a guess. Never an exclusion.
10. `signal_*` helpers return the events (or an `Option`) instead of
    publishing; `map_idle_event` returns `Option<PushSessionEvent>`.

The per-worker health comment at the top of `idle_loop`, `report_idle_failure`,
`wait_for_push_reconfigure`, `sleep_backoff` and the was-disconnected
bookkeeping are deleted (the engine owns health edges).

Exclusion lifetime, stated in `reference/imap.md`: until the subscribed set
changes or the account is replaced. IMAP has no lifecycle source, so nothing
else repairs a folder. Only terminal and `DisableScope` outcomes exclude
(step 6), so no repair that can succeed is outlived by an exclusion.

Exclusion shifts slot indices, stated in the same place: non-NOTIFY lanes take
the `slot`-th entry of the filtered list, so excluding one folder moves every
higher lane's assignment, and each of those lanes ends `Reconfigure`, redials
and owes a full reconcile. A subscription change already does exactly this
today; exclusion is one more trigger of the same cost, accepted rather than
paying for a stable assignment scheme. Each higher lane reconfigures once
per exclusion, and an exclusion needs a new subscription version to recur.

`idle_session` ignores its `PushGate` (S13: a Connection session).

### 5.2 JMAP (`crates/jmap/src/sync/push.rs`, `account.rs`, `factory.rs`)

- `WsState` loses the fields `tx` and `reader`, and with `reader` its
  accessor method `reader_handle()`, whose one caller is the reader join in
  `account.rs::close` (dropped below; the close-path edit must not be
  missed); keeps `enabled`,
  `push_state`, `teardown_timeout`; gains `policy: ReconnectPolicy`,
  `routing: Arc<PushRouting>`, `lease: LaneLease`, `available: bool`.
  `WsState::spawn` becomes `WsState::new` and spawns nothing; `factory.rs`
  calls it with the same arguments. `push_stream(lane, stop, _gate)` (a
  Connection session ignores the gate, S13) clones the
  client, the `Arc`s, the policy, the routing, the account shutdown token and
  the `LaneLease` into the owned session future (no borrow of `&self`
  survives); `lane != PushLane(0)` or `!available` yields the S11 failure.
- `PushEventSender`, `PushEventReceiver`, `stream`, `join_reader`,
  `reader_loop`, `ReaderStep`, `classified_reader_failure` and `interrupted`
  are deleted. `ReconnectPolicy` keeps every field; `initial` and `max` now
  feed `push_lanes()`.
- `JmapAccount::push_lanes()`: one `Connection` lane when `caps.push ==
  InProcess`, backoff `BackoffPolicy::new(policy.initial, policy.max, false)`.
- `session<T: PushTransport>(transport, enabled, push_state, shutdown, policy,
  routing, lease: LaneLease, stop)` (async-stream) is `reader_pass` as a
  stream: claim the lease first (S11), keeping the guard in the stream state;
  bounded connect - interrupted by stop or shutdown ends `Stopped`, past the
  bound ends `Lost`, an error ends `Failed(classified PushStream)` whatever its
  class; bounded re-enable with the same three arms; yield `Established`;
  read loop with the keepalive ping exactly as today, except: a
  `PushNotification` remembers the push state, yields its hints (`emit_push`
  and `emit_state_change` return `Vec<InvalidationHint>`) and one `Healthy`;
  `Response`, `IgnoredFrame` and `Pong` yield `Healthy`; `UnknownFrame` yields
  `Invalidated(Unknown)` and `Healthy`; a failed ping or a frame error ends
  `Failed(classified)`; pong timeout or end of stream ends `Lost`; stop or
  shutdown ends `Stopped`.
- `account.rs`: `push_stream(lane, stop, _gate)` builds the session from the client
  and `WsState`; `close()` keeps the bounded `disable_for_close` and drops the
  reader join. `subscribe` / `unsubscribe` / `disable_for_close` and the
  deferred-success arms are unchanged: the session re-applies the committed
  union on every connect.
- Behaviour change, accepted: the WebSocket connects when the engine starts
  the lane after attach, not inside `open()`, and the first connection of
  generation 0 no longer triggers a full reconcile (it never covered a gap).

### 5.3 Graph EWS (`crates/graph/src/account/ews_stream.rs`, `mod.rs`)

`GraphAccount` loses `push_tx`, `push_terminal`, `push_disconnected`,
`graph_worker` and `ews_worker`, and gains `push_lease: LaneLease`,
`push_tasks: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>`
(finished handles pruned on insert), `graph_topology: Arc<watch::Sender<u64>>`,
`pending_gaps: Arc<std::sync::Mutex<VecDeque<CursorScope>>>` and
`ews_ledger: Arc<std::sync::Mutex<EwsLedger>>`, all built in
`GraphAccount::new`. `worker_slot.rs` and `push_stream.rs` (with
`publish_terminal`, `ensure_ews_worker`) are deleted.

```rust
/// Sends one EWS Unsubscribe for an id over the transport that created it.
pub(crate) type EwsReleaser =
    Arc<dyn Fn(String) -> futures::future::BoxFuture<'static, Result<(), EwsError>> + Send + Sync>;

pub(crate) struct EwsLedger {
    entries: HashMap<String, (EwsSubState, EwsReleaser)>,
    retired: bool,
}

enum EwsSubState {
    Live,
    Releasing(futures::future::Shared<futures::future::BoxFuture<'static, ()>>),
    Orphaned,
}
```

Each entry carries the releaser of the session that created it (built from
that session's `Arc<E: EwsExecute>` around today's
`build_unsubscribe_request` and `ReplayPolicy::Idempotent`), so `close()` and
later sessions release through the same transport and tests stay hermetic.

Release is an atomic claim, `release(account, id) -> Shared completion`: under
the lock, `Live | Orphaned` becomes `Releasing(shared)` around exactly one
spawned account-owned task running the entry's releaser (its handle also
joins `push_tasks`); `Releasing(shared)` returns a clone of that completion;
an absent id is already released and returns a ready completion. Only the
claiming task performs the final transition: Unsubscribe `Ok` removes the
entry; `Err` removes it and logs if `retired`, else sets `Orphaned`. No await
happens under the lock.

`ews_session<E: EwsExecute + Send + Sync + 'static>(account, ews: Arc<E>,
stop)` (async-stream; `push_stream` ignores its `PushGate`, S13, and passes
the real `EwsClient` built as
`run_streaming_worker` builds it today, tests a scripted `EwsExecute`; a
missing account transport ends `Failed` with today's internal error)
replaces `run_worker`:

1. `lane != PushLane(0)` yields the S11 failure; otherwise claim `push_lease`
   or yield it. Claim a release for every `Orphaned` entry (not awaited).
2. Mark the topology seen; read the deduplicated scope union. Empty: park on
   the topology watch, `stop` or shutdown (`Stopped`).
3. `subscribe` (declared `ReplayPolicy::Never`, unchanged), deliberately NOT
   raced against `stop` or shutdown: an in-flight Subscribe completes so its
   id becomes known and can be released (racing it is what would create an
   unknowable id). Error: `Failed(classified PushSubscribe)` whatever its
   class; the `subscribe_status_stopped_by_replay_policy` exception is
   deleted, because S1 makes re-running the session the reconciliation.
   Ok(id): lock the ledger and insert `(Live, this session's releaser)`; if
   `retired`, or if `stop` or shutdown fired meanwhile, claim its release at
   once (after unlocking), await it and end `Stopped`; otherwise yield
   `Established`.
4. Long polls (`run_get_events_loop` logic): each complete response yields
   `Healthy`; notifications yield `Invalidated`; a topology change awaits
   `release(id)` and ends `Reconfigure`; a parse failure or truncated frame
   awaits `release(id)` and ends `Lost`; an HTTP or classified frame error
   awaits `release(id)` and ends `Failed(e)` whatever its class; stop or
   shutdown awaits `release(id)` and ends `Stopped`.

`ReconnectBackoff`, `wait_before_reconnect`, `reconnect_delay`,
`classified_push_failure`'s retry split and the `coverage_gap` /
`disconnected` bookkeeping are deleted (the engine owes the Reconfigure
reconcile). `push_lanes()` in EWS mode: one `Connection` lane, backoff
`BackoffPolicy::new(5 s, 320 s, true)`: the same base sequence as today's
`reconnect_delay` (5 s doubling, capped after six doublings at 320 s), with
the quarter jitter now computed in milliseconds rather than whole seconds -
an intended change, not a preservation.

`close()`: `retire_all_graph_subscriptions` (unchanged, cancels the token
first), then lock the ledger, set `retired`, claim a release for every `Live`
and `Orphaned` entry, unlock, then take every handle out of `push_tasks` and
await them together under one `CLOSE_WORKER_JOIN_TIMEOUT`. Handles still
running at the bound are DROPPED, never aborted (dropping a `JoinHandle`
detaches its task without cancelling it), and the close logs how many were
left running: those tasks own EWS releases and Graph recreate install-or-undo
work that must not be cut short. A release spawned after that join also runs
to completion.

`crates/graph/src/account/push/ews.rs::subscribe_ews`: the
`ensure_ews_worker(account).await` call is deleted, so the registration
insert, the topology bump and the return have no await between them. Its
`RegistrationGuard` (armed to retire a registration whose caller was dropped
at that await) therefore guards an empty window and is deleted with its
`Drop` impl; the comment at the insert is rewritten to say the registration
is the call's last step with no await after it. The module doc's reference to
`push_stream::ensure_ews_worker` becomes a reference to the session.

Residual kept and documented: a Subscribe whose response never arrives (the
session dropped mid-request) may have created a subscription whose id is
unknowable; it lapses unpolled.

### 5.4 Graph webhook (`crates/graph/src/account/push/{renewal,webhook,common,dispatch}.rs`)

- `renewal_session(account, stop, gate)` replaces `run_graph_subscription_worker`.
  `lane != PushLane(0)` yields the S11 failure; otherwise claim `push_lease`
  or yield it. Loop: drain `pending_gaps` (pop one scope, yield
  `Invalidated(SpecificCursorScope)`, repeat - S5); mark `graph_topology`
  seen (`borrow_and_update` on the session's receiver) BEFORE reading the
  groups, so a registration landing between the check and the wait still
  wakes it; if no live group (`has_live_graph_subscription_group`, unchanged),
  wait for `graph_topology.changed()`, `stop` or shutdown and loop; otherwise
  yield `Established` once per session, run a pass, drain `pending_gaps`
  again (a recreate installed during the pass is invalidated now, not after
  the cadence wait or a backoff), then end `Incomplete` if the pass reports
  lost coverage (below), else yield `Healthy`, wait `RENEWAL_CHECK_INTERVAL`
  (selecting `stop`, shutdown) and loop. The first pass runs at session start
  (S3). Stop and shutdown end `Stopped`.
- A pass awaits `gate.clear()` (selecting `stop` and shutdown, which end
  `Stopped`) before each due row's renewal, so an engine hold recorded while
  the session is live - by a sibling row's 429 in this same pass, or by any
  other engine work - holds the remaining requests (S13).
- A pass renews every due row once (`due_renewals`, unchanged). Ok updates the
  expiry. A gone row goes through `replace_gone_subscription` (still a spawned
  account-owned task, its handle joining `push_tasks`): `Installed` happens in
  `create_and_install_replacement` under the `graph_subscriptions` write lock,
  which now also pushes the row's scopes onto `pending_gaps`; the session does
  not yield the gap itself, it drains `pending_gaps` (so a recreate landing
  after its session dropped is still invalidated by the next one).
  `HandleUnsubscribed` and `AccountClosed` continue. Every other failure is
  classified as today (`renew` or `recreate` phase, single-scope attribution)
  and yielded as `UnitFailed`; a terminal one also calls
  `mark_subscription_terminated`. The pass reports lost coverage when any
  failure in it was non-terminal OR, after the pass, any registered group not
  `tearing_down` still holds a `terminated` row (a terminated row stays in
  its group until the consumer unsubscribes that handle). The session loop
  above turns that into `Incomplete` or `Healthy`. Without the second clause a
  pass whose only failures were terminal yielded `Healthy`, the lane read
  `Up`, and even losing every registration could leave `any_down()` false
  indefinitely; with it the lane reads `Down` (carrying the terminal error as
  `last_error`) for as long as the loss stands, and the extra sessions cost
  nothing new, because the lane's backoff equals the renewal cadence and each
  session's pass still renews every due live row. When every row is
  terminated there is no live group, so the session parks without a pass and
  the lane keeps the `Down` the previous session left. Residual, accepted: a
  consumer that then unsubscribes every handle leaves the lane reading `Down`
  until its next subscribe, because an idle session reports nothing.
- `push_lanes()` in webhook mode: one `Registration` lane when
  `push_endpoint` is configured, none otherwise; backoff
  `BackoffPolicy::new(RENEWAL_CHECK_INTERVAL, RENEWAL_CHECK_INTERVAL, false)`.
- `webhook.rs`: `create_and_register` drops `mark_push_reconnected` and
  `ensure_graph_worker` and bumps `graph_topology` after the group registers;
  `unsubscribe_graph` drops the worker-abort block and bumps after a removal;
  `ensure_graph_worker`, `retire_graph_worker_slot` and the slot-race comments
  are deleted. `common.rs`: `mark_push_disconnected`, `mark_push_reconnected`
  and `announce_push_recovered` are deleted.
- `close()` (5.3) joins `push_tasks`, which covers the recreate install-or-undo
  the old renewal-worker join waited for.

### 5.5 Google (`crates/google/src/account/push.rs`, `mod.rs`)

- The actor keeps every command, the lifecycle, handles, ownership and
  `commit_watched`. Deleted: the renewal timer arm, `retry_after`,
  `disconnected`, `consecutive_renewal_failures`, the health broadcast,
  `terminal`, `push_stream`'s broadcast adapter and
  `renewal_status_stopped_by_replay_policy`.
- New shared state on `PubSubControl`: `schedule:
  tokio::sync::watch::Receiver<Option<tokio::time::Instant>>` (the actor holds
  the sender and publishes `Some(now + renewal_delay_at(expiration, wall_now))`
  on every entry to `Watched` and `None` on every exit), `gap: Arc<AtomicBool>`,
  `fresh: Arc<AtomicBool>`, `lease: LaneLease`.
- New actor state `lapsed: Option<Option<SystemTime>>`: set when a terminal
  renewal failure moves the actor to `Unwatched`, to the expiration of the
  watch it stopped renewing (`Some(None)` when that expiration was unknown);
  cleared by the next commit from `Unwatched`. Not set by an unsubscribe-driven
  exit: that loss of coverage was the consumer's choice.
- Subscribe-driven recovery. When a subscribe command's `commit_watched`
  moves the actor from `Unwatched` to `Watched`, then in that same transition:
  if `lapsed` is `Some(previous)` and `previous` is absent or no later than the
  commit's completion wall time (`client.wall_now()`), set `gap` (the old
  watch expired before the new one existed, so notifications in between were
  never sent); clear `lapsed`; and if `lapsed` was `Some(_)` set `fresh`. The
  first subscribe on an instance has `lapsed == None`, so it records neither
  and stays silent. Without the gap rule, a watch that lapsed after a terminal
  renewal failure and was restored by an explicit subscribe left a hole no
  invalidation covered: generation-zero Registration lanes owe no automatic
  reconcile, and the renewal gap rule below only fires on a `Renew`.
- New command `WatchCommand::Renew { reply: oneshot::Sender<Result<RenewOutcome,
  AccountError>> }` with `RenewOutcome::{Renewed, NotDue, NotWatched}`. The
  actor rechecks on receipt: not `Watched`, no handles, or `Retired` replies
  `NotWatched`; a published due instant still in the future replies `NotDue`;
  otherwise it performs the renewal exactly as the old timer arm did
  (`watch_claimed` not raced against shutdown, `commit_watched`). On success it
  compares the PREVIOUS expiration with the completion wall time
  (`client.wall_now()`): previous absent or no later than completion sets `gap`
  in the same transition as the commit. On failure, by `RecoveryClass` of the
  classified error: a terminal class goes to `Unwatched` (schedule `None`), so a
  later subscribe starts a fresh watch as today; `Retry`, `Reconcile` and
  `Engine` classes all restore the previous `Watched` state with the schedule
  due now, so the session the engine starts after its backoff renews at once
  (a `/watch` 503 that net declined to replay classifies `Reconcile` and must
  not park the watch; an engine directive is forwarded by the lane, and if no
  replacement account arrives the old watch keeps being renewed). It replies
  with the classified error. A session dropped mid-renewal never cancels it:
  the actor owns the request.
- `renewal_session(control, stop, gate)`: `lane != PushLane(0)` yields the
  S11 failure; otherwise claim the lease or yield it. Loop: if
  `gap.swap(false)`, yield `Invalidated(Account)` (single obligation, S5).
  Read the schedule. `None`: wait for a change, `stop` or shutdown.
  `Some(due)`: yield `Established` once per session; if `fresh.swap(false)`,
  yield `Healthy`; if `due` is in the future wait for it (selecting a change,
  `stop`, shutdown); await `gate.clear()` (selecting `stop`, shutdown; S13);
  send `Renew`; `Renewed` drains the gap and yields `Healthy`; `NotDue` and
  `NotWatched` loop; `Err(e)` ends `Failed(e)`. Stop while awaiting the reply
  ends `Stopped` without cancelling the actor.
- Why `fresh` yields `Healthy`: after a terminal renewal failure the lane is
  `Down` and the next session parks on `None`. A later successful subscribe
  publishes a due time days away and the session yields `Established`, which
  for a Registration lane may not clear `Down` (4.6), so without a recovery
  signal the lane would read `Down` until the next renewal. A subscribe that
  commits a fresh watch is a successful `/watch` round trip, the same wire
  call a renewal makes, so it is demonstrated healthy progress under S4 and
  resetting the backoff on it is correct; it carries no gap meaning of its
  own (the gap rule above is separate).
- `push_lanes()`: one `Registration` lane when a `PubSubConfig` is configured,
  none otherwise; backoff `BackoffPolicy::new(RENEW_RETRY_AFTER,
  RENEW_RETRY_AFTER, false)` (five minutes, the old damper). `MIN_RENEW_DELAY`
  stays (success-path cadence floor).
- Behaviour change, accepted: the per-failure `Warning` carrying a consecutive
  failure count is replaced by the engine's down-edge warning plus
  `PushLaneState::Down { consecutive_failures, .. }`.

### 5.6 No-lane implementors

CalDAV, CardDAV, the IMAP `StubAccount` and every sync test stub inherit the
empty `push_lanes()`, so every lane is invalid for them and `push_stream(lane,
_, _)` yields the S11 failure (`Failed(push_lane_contract_error(protocol, lane,
"not a lane of this account"))`) and ends; the engine never calls it because
the lane list is empty. The sync test stub in
`crates/sync/tests/common/mod.rs` additionally gains:

- `push_lane_specs: Vec<PushLaneSpec>` (returned by `push_lanes`; a lane not
  in it gets the S11 failure);
- `push_sessions: Mutex<VecDeque<Vec<SessionStep>>>` with `enum SessionStep {
  Emit(PushSessionEvent), Park(Arc<Notify>), AwaitStop, AwaitStopFor(Duration),
  AwaitGate }`, each `push_stream` call for a declared lane popping the next
  script (an empty queue parks until stop and ends `Stopped`;
  `AwaitStopFor(d)` waits for stop and then spends `d` on its cleanup before
  ending `Stopped`; `AwaitGate` awaits `gate.clear()` and records the instant
  it cleared), and `push_session_log: Arc<Mutex<Vec<SessionRecord>>>`
  recording lane, open instant, whether stop was requested, gate-clear
  instants and whether the session ran to its terminal item;
- `lifecycle_scripts: Mutex<VecDeque<Vec<ScopeLifecycleEvent>>>` and
  `lifecycle_opens: Arc<Mutex<Vec<tokio::time::Instant>>>`; when scripts are
  present each `scope_lifecycle_stream` call pops one (the existing single
  `lifecycle_rx` path stays for the tests that use it). These two fields land
  in landing 1 (the heartbeat tests need them); the push fields land in
  landing 3.

### 5.7 Lifecycle producers (landing 1)

- Google `crates/google/src/account/scopes.rs`,
  `scope_lifecycle_stream_with_baseline`: in the `Ok(new)` arm, after
  `note_poll_succeeded` and the baseline commit of `pending`, return
  `Some((ScopeLifecycleEvent::Polled, state))`; the queued diff events follow
  on the next iterations exactly as today (they live in the account-owned
  baseline, so a drop after `Polled` loses none of them). No `Polled` on the
  error arm or before a fetch.
- JMAP `crates/jmap/src/sync/discover.rs`, `scope_lifecycle`: yield
  `Polled` at exactly two points - after `state_cache::set` when
  `!response.has_more_changes()` (immediately before that arm's
  `poll_pause`), and after a successful reseed's commit (immediately before
  its `poll_pause`). No `Polled` for the unseeded-state pause (no read
  happened), between pages of a burst, or after a failure.
- Existing producer tests keep their assertions: the test-only constructors
  wrap the stream in a `skip_polled` filter - Google's `#[cfg(test)]
  scope_lifecycle_stream` helper, JMAP's `lifecycle_stream`,
  `lifecycle_stream_with_names` and `lifecycle_stream_with_limit`, and the
  second constructor call in JMAP's
  `dropping_a_partial_reseed_does_not_hide_undelivered_events`. This covers
  the two tests that read the stream across a successful poll
  (`expired_lifecycle_history_reconciles_and_keeps_polling`,
  `a_shared_cache_refresh_between_polls_does_not_swallow_creation`) and every
  other item-by-item reader. The new `Polled` tests use the raw streams
  (Google's `scope_lifecycle_stream_with_baseline`, JMAP's `scope_lifecycle`).

## 6. Verification bricks

Every test below is hermetic (stub transports, canned transcripts, scripted
dispatch, `StubAccount`). Every regression test marked (A) is ablated: revert
the production change it pins, confirm it fails, restore; record the result in
the landing commit message. Check the name filter matched before believing a
PASS.

### 6.1 Types

- `brokkr test -p bifrost-types session_stop_` -
  `session_stop_resolves_on_stop`, `session_stop_resolves_when_the_stopper_drops`,
  `session_stop_never_stays_pending`, `session_stop_is_stopped_is_non_blocking`.
- `brokkr test -p bifrost-types backoff_policy_` -
  `backoff_policy_doubles_to_the_ceiling`, `backoff_policy_normalizes_inverted_bounds`,
  `backoff_policy_zero_failures_is_no_delay`, `backoff_policy_saturates_on_large_counts`.
- `brokkr test -p bifrost-types push_gate_` -
  `push_gate_open_clears_at_once`, `push_gate_runs_the_engine_future_on_each_clear`.

### 6.2 Sync

Unit tests (in-module):

- `brokkr test -p bifrost-sync reconnect_backoff_` -
  `reconnect_backoff_records_failures_and_resets`,
  `reconnect_backoff_jitter_stays_within_a_quarter`,
  `reconnect_backoff_carried_count_earns_its_delay_and_zero_earns_none`.
- `brokkr test -p bifrost-sync push_ingress_` -
  `push_ingress_collapses_to_full_past_capacity` (A: counts one coarsening,
  keeps the reconcile), `push_ingress_wakes_after_a_push_from_a_non_runtime_thread`,
  `push_ingress_keeps_a_push_landing_between_take_and_wait` (A),
  `push_ingress_ignores_pushes_after_close`.
- `brokkr test -p bifrost-sync sink_` (`push/mod.rs`) -
  `sink_reconnected_requests_a_full_reconcile`,
  `sink_terminal_is_broadcast_as_terminated_at_push_time`,
  `sink_retry_termination_records_its_throttle_at_push_time`,
  `sink_engine_directive_is_refused_with_its_event_when_the_reopen_queue_is_full`
  (A), `sink_push_for_an_unknown_account_is_not_attached`,
  `sink_warning_and_disconnect_are_broadcast_without_queueing`,
  `sink_never_blocks_on_a_full_queue_from_a_non_runtime_thread`,
  `sink_engine_directive_is_refused_while_the_boundary_is_not_run` (A: under
  `Pause` and under `CheckpointNow` the event comes back `Refused` and the
  reopen channel stays empty), `sink_terminal_is_broadcast_account_scoped`
  (A: a folder-scoped terminal error is broadcast on `CursorScope::Account`).

Integration tests:

- `crates/sync/tests/push_reconcile_throttle.rs`, rewritten onto
  `run(ingress)` with a closed ingress ending the run:
  `brokkr test -p bifrost-sync a_throttled_scope_does_not_freeze_the_account_wide_sweep`,
  `brokkr test -p bifrost-sync a_terminally_parked_scope_is_not_re_driven_by_a_later_hint`;
  new, each `brokkr test -p bifrost-sync <name>`:
  - `a_paused_drive_keeps_its_scope_in_the_work_set` (A);
  - `an_account_wide_directive_holds_the_remainder_until_the_generation_moves`
    (A);
  - `a_held_checkpoint_now_allows_at_most_one_drive_attempt` (A: a pending
    push scope under a held `CheckpointNow` is driven at most once until the
    boundary returns to `Run`, then once more; ablate by reverting the park to
    `Pause` only and watch the attempt count climb). Count attempts at the
    scheduler admission or the drive lease, NOT at the stub's
    `changes_stream`: `begin_activity` refuses before the account is called,
    so a stub-side counter would read zero against the bug too and the test
    would not bite;
  - `the_work_set_coarsens_on_merge_but_not_on_its_own_full_resolution` (A:
    merging more distinct hinted scopes than `watch_capacity` counts one
    coarsening and sweeps everything; a registry larger than the cap swept
    from `full` is driven scope by scope without a coarsening);
  - `undispatchable_scopes_are_discharged_without_spinning` (A: a parked
    scope, a scope whose `with_drive` answers `None` and a refused admission
    are each attempted once, and the task then waits);
  - `an_account_wide_hold_clears_at_its_instant_without_a_generation_change`;
  - `a_hold_is_not_cleared_by_an_already_published_generation` (A: a
    receiver that never marked an earlier bump seen does not clear a newly
    set hold);
  - `a_membership_hint_resolves_to_its_scopes_at_merge_time`.
- `crates/sync/tests/push_poll_latency.rs`: the three tests assert
  `SinkPush::Accepted` on every push and otherwise stay:
  `brokkr test -p bifrost-sync push_invalidation_drives_mid_poll_cadence_without_waiting_for_the_sleep`,
  `brokkr test -p bifrost-sync a_push_drive_account_contract_error_is_published_and_parks_its_scope`,
  `brokkr test -p bifrost-sync a_failed_hinted_scope_reaches_recovery_without_abandoning_its_siblings`.
- New `crates/sync/tests/lifecycle_heartbeat.rs`:
  `brokkr test -p bifrost-sync a_polled_heartbeat_resets_the_lifecycle_backoff`
  (A: three `Terminated` streams, then a stream that yields `Polled` and
  ends; the open after that stream waits 30 s, not the 240 s an unreset
  backoff would give),
  `brokkr test -p bifrost-sync an_empty_lifecycle_stream_keeps_growing_its_backoff`,
  `brokkr test -p bifrost-sync a_lifecycle_restart_account_loop_is_paced_across_generations`
  (A: streams that always end `Terminated(Engine(RestartAccount))`, each
  forward answered by a generation bump; the reopens are spaced 30 s, 60 s,
  120 s, not back to back; ablate by resetting on the generation).
- Landing 2 only (deleted with the interim forwarder in landing 3), in the
  engine's in-module tests:
  `brokkr test -p bifrost-sync interim_forwarder_resubmits_a_refused_directive_after_resume`
  (A: a directive refused under a pause reaches the reopen channel once after
  resume, and one refused for a full queue is sent once the queue drains).
- New `crates/sync/tests/push_lanes.rs` (landing 3), all with paused time:
  - `a_terminal_session_failure_is_broadcast_once_and_parks_the_lane` (A: the
    replay-loop regression; one `SyncEvent::Terminated` across a minute, one
    session opened);
  - `a_parked_lane_restarts_on_a_push_topology_bump` (driven through
    `SyncEngine::subscribe_push`, so it tests the `TopologyBump` wiring and not
    only the lane);
  - `unsubscribe_push_and_the_orphan_retrier_bump_the_push_topology` (A: each
    site, including `subscribe_push`'s detach-race cleanup, restarts a parked
    lane);
  - `a_lane_failing_restart_account_through_a_long_pause_submits_one_request`
    (A: sessions failing `Engine(RestartAccount)` across an hour-long pause
    leave exactly one request in the reopen channel at resume; ablate by
    removing the boundary gate);
  - `a_scope_repair_failed_during_a_pause_is_submitted_after_resume`;
  - `restart_account_failures_space_reattaches_by_the_carried_backoff` (A:
    sessions that always fail `RestartAccount` produce reattaches spaced by the
    growing lane backoff, not back to back; ablate by resetting the count on a
    generation);
  - `a_healthy_lane_reopens_at_once_on_a_new_generation`;
  - `a_stale_push_recovery_is_declined_after_its_account_was_replaced` (A: two
    stamped requests queued; the first reattaches, the second, stamped with
    the replaced generation, changes nothing on the replacement);
  - `a_hold_recorded_after_establishment_holds_the_next_maintenance_request`
    (A: a Registration session's `AwaitGate` step clears only after a hold
    recorded mid-session expires);
  - `a_terminal_parked_connection_lane_owes_a_reconcile_on_restart` (A);
  - `blind_reconfigures_are_paced_from_the_fourth` (A);
  - `reconfigure_returns_a_connection_lane_to_starting`;
  - `a_registration_lane_with_a_retired_unit_reads_down`;
  - `a_topology_bump_during_the_failing_session_prevents_parking` (A);
  - `a_parked_lane_restarts_on_a_new_generation`;
  - `backoff_grows_across_failed_sessions_and_resets_on_healthy`;
  - `a_retry_hint_longer_than_the_backoff_is_honored_and_recorded`;
  - `an_engine_directive_is_forwarded_once_per_failed_session`;
  - `a_full_reopen_queue_does_not_stop_session_polling_or_stop` (A);
  - `restart_account_waits_for_the_generation_before_backing_off`;
  - `a_connection_lane_owes_a_full_reconcile_after_a_lost_session`;
  - `a_first_session_on_generation_zero_owes_nothing`;
  - `a_connection_lane_on_a_new_generation_reconciles_at_establishment`;
  - `a_registration_lane_on_a_new_generation_reconciles_at_lane_start`;
  - `reconfigure_restarts_at_once_and_owes_a_reconcile`;
  - `unit_failures_record_every_throttle_and_incomplete_waits_the_longest_hint`
    (A);
  - `the_disconnect_warning_fires_once_across_lanes`;
  - `detach_stops_sessions_and_drains_their_cleanup_before_close` (A: the
    stub's `AwaitStop` step completes and the session records its terminal item
    before `close()` is called);
  - `a_drain_longer_than_its_bound_is_dropped_before_the_worker_deadline` (A:
    an `AwaitStopFor` longer than `drain_bound` is dropped at the bound, the
    `PushLanes` worker returns on its own and is not aborted; ablate by setting
    `drain_bound` to `detach_timeout`);
  - `reattach_closes_the_old_account_only_after_its_lanes_drained` (A: the
    old session's `AwaitStopFor` cleanup finishes before the old account's
    `close()` is called);
  - `a_retiring_generation_cannot_mutate_the_replacement_lane` (A: an old
    session yielding `Failed(Engine(RestartAccount))` during drain sends no
    reopen request and leaves the new generation's health untouched);
  - `push_health_reports_lane_states_to_a_late_subscriber`;
  - `lanes_are_not_run_for_an_account_without_push`.
  Command for each: `brokkr test -p bifrost-sync <name>`.

### 6.3 Providers (landing 3, plus the two lifecycle tests in landing 1)

Existing tests that pinned removed machinery are deleted or rewritten as named;
everything else stays green unchanged.

- IMAP (`brokkr test -p bifrost-imap <name>`):
  - deleted: `an_event_published_before_stream_creation_reaches_the_late_reader`,
    `an_unobserved_event_backlog_coalesces_at_the_ring_capacity`,
    `permanent_idle_failure_terminates_late_and_lagged_readers`;
  - rewritten in `push.rs`: `idle_failures_classify_before_deciding_to_redial`
    becomes `idle_failures_split_into_folder_local_and_session_failures`
    (folder-locality decided by the recovery plan: terminal and
    `DisableScope` exclude, every other plan and every `Bye` or IDLE error
    ends `Failed`);
    `a_resubscribe_interrupted_idle_round_still_invalidates`,
    `a_rejected_notify_registration_degrades_instead_of_going_silent`,
    `a_non_notify_account_does_not_invalidate_on_a_missing_registration`,
    `coverage_establishment_names_each_covered_folder` read the returned
    events;
  - rewritten in `scripted_tests.rs` to drive `push_stream(lane, stop,
    PushGate::open())`:
    `push_is_refused_entirely_when_pool_cap_leaves_no_room` (no lanes, no
    dials), `a_notify_account_admits_every_folder_on_one_worker` renamed
    `a_notify_account_admits_every_folder_on_one_lane`,
    `an_idle_worker_session_counts_against_pool_cap`,
    `a_subscription_change_mid_dial_keeps_the_pending_session`,
    `a_push_session_that_waited_for_capacity_invalidates_the_folder_it_starts_covering`,
    `a_push_session_dialed_at_once_publishes_no_coverage_invalidation` (now
    `Established`, then the peer drop ends the session `Failed` with a
    retryable class), `a_refused_push_dial_is_retried_not_latched_as_terminal`
    renamed `a_refused_push_dial_fails_the_session_without_redialing` (A: one
    dial, `Failed` retryable, no second dial within a minute);
  - new: `a_nonexistent_selected_folder_is_excluded_and_the_session_reconfigures`
    (A), `an_exclusion_from_a_stale_subscription_version_is_discarded` (A),
    `an_account_wide_directive_on_select_fails_the_session_without_excluding`
    (A: a `SyncState` capability-change refusal of SELECT ends `Failed` with
    `RestartAccount` and leaves the folder assignable),
    `a_scope_repair_on_select_does_not_exclude_the_folder` (A),
    `a_bye_never_excludes_the_selected_folder` (A),
    `an_all_excluded_lane_parks_until_the_subscription_set_changes`,
    `a_completed_idle_round_reports_healthy`,
    `a_second_session_on_a_driven_lane_fails_the_lease`,
    `a_stopped_session_logs_out_before_ending`,
    `lane_count_follows_notify_and_capacity`,
    `a_refused_notify_registration_warns_and_keeps_selected_coverage`.
- JMAP (`brokkr test -p bifrost-jmap <name>`; these live under
  `crates/jmap/src/sync/`, behind the non-default `sync` feature. A focused
  `-p bifrost-jmap` run overrides the workspace selection, so no sibling
  donates `sync` there; the `jmap-sync` sweep is the leg that compiles and
  runs them. SKIP on every other sweep is expected and the `jmap-sync` PASS is
  required - `--sweep jmap-sync` runs only that leg):
  - deleted: `a_terminal_push_event_is_replayed_to_a_late_subscriber`,
    `a_lagged_subscriber_reads_the_latched_terminal_event`,
    `a_lagged_broadcast_slot_coalesces_into_an_unknown_invalidation`,
    `joining_the_reader_waits_for_it_and_gives_up_on_a_wedged_one`;
  - rewritten onto the session: `capability_failure_in_a_push_frame_reaches_engine_recovery`
    (ends `Failed` with an engine class), `capability_failure_in_reenable_is_not_retried`,
    `capability_failure_at_setup_is_not_a_local_retry`,
    `an_unreadable_push_frame_invalidates_broadly_then_keeps_reading`,
    `a_bad_ws_frame_close_reconnects_without_terminal_health` renamed
    `a_bad_ws_frame_close_fails_the_session_retryably` (its injected
    `Err(crate::Error::WebSocketClosed)` is a classified frame error, so the
    session ends `Failed` with a retryable class; `Lost` is only for an
    unclassified end of stream),
    `the_push_stream_ends_when_the_shutdown_token_is_cancelled` (and on stop),
    `a_shutdown_during_the_websocket_handshake_ends_the_reader`,
    `a_shutdown_during_the_push_re_enable_ends_the_reader` (both also for
    stop), `a_failed_push_re_enable_disconnects_instead_of_announcing_reconnected`
    (`Failed` before any `Established`),
    `a_handshake_that_outlives_its_bound_falls_to_the_reconnect_backoff`
    renamed `a_handshake_past_its_bound_ends_the_session_lost`,
    `a_silent_connection_is_pinged_and_dropped_when_the_pong_never_comes`,
    `a_pass_that_reads_no_traffic_does_not_reset_the_backoff` renamed
    `a_session_that_reads_no_traffic_never_reports_healthy` (A),
    `the_reader_captures_the_latest_push_state_and_never_unsets_it`,
    `reconnect_replays_the_last_push_state`;
  - new: `a_second_session_on_the_lane_fails_the_lease`,
    `push_lanes_follow_websocket_capability_and_reconnect_policy`.
- Graph (`brokkr test -p bifrost-graph <name>`):
  - deleted: `push_stream.rs` tests
    `a_subscriber_after_terminal_health_reads_the_latched_error`,
    `a_lagged_subscriber_reads_terminal_health_instead_of_losing_it`;
    `push/tests/common.rs` `a_late_subscriber_sees_a_coherent_latch_after_a_lost_disconnect`,
    `the_latch_records_the_outage_not_its_delivery`; `push/tests/webhook.rs`
    `a_first_webhook_subscribe_publishes_no_reconnect`;
    `push/tests/renewal.rs` `a_retiring_worker_clears_its_slot_so_the_next_subscribe_respawns`;
    `ews_stream.rs` `reconnect_backoff_grows_and_caps_with_bounded_jitter`;
    `push/tests/ews.rs` `a_dropped_ews_subscribe_retires_the_registration_nobody_received`
    (its await and `RegistrationGuard` are gone, 5.3);
  - rewritten: `ews_stream.rs` `a_healthy_long_poll_read_resets_the_reconnect_backoff`
    renamed `a_complete_long_poll_reports_healthy`,
    `engine_directives_leave_the_stream_worker` and
    `frame_failures_only_reconnect_for_retry` merged into
    `every_classified_stream_failure_ends_the_session_failed`,
    `an_uncertain_subscribe_is_forwarded_for_reconciliation` (ends `Failed`
    with `Reconcile`), `an_unreplayed_subscribe_status_backs_off_and_resubscribes`
    renamed `an_unreplayed_subscribe_status_fails_the_session_without_resubscribing`,
    `the_first_registration_subscribes_exactly_once`,
    `a_topology_change_hands_off_the_stream_without_a_false_disconnect`
    (releases, then `Reconfigure`), `a_streamed_notification_invalidates_the_folder_scope`,
    `a_notification_under_an_alien_namespace_prefix_still_invalidates`,
    `a_terminal_response_error_in_the_stream_terminates_instead_of_reconnecting`
    (releases, then `Failed` terminal),
    `the_worker_clears_its_slot_when_the_last_registration_is_gone` renamed
    `a_session_with_no_registrations_parks`; `push/tests/renewal.rs`
    `a_vanished_subscription_is_recreated_and_reconnected_on_the_next_tick`
    (the row's scopes are invalidated through `pending_gaps`),
    `one_tick_recreating_several_subscriptions_reconnects_once` (each recreated
    row's scopes once), `a_failed_tick_disconnects_and_the_next_successful_tick_reconnects`
    (ends `Incomplete`; the next session renews at its start),
    `a_terminal_renewal_failure_names_the_scope_that_lost_coverage`
    (`UnitFailed`, then the pass ends `Incomplete` because the row stays
    retired), `the_worker_stops_on_cancellation_while_a_renewal_is_in_flight`,
    `a_worker_cancelled_before_its_first_renewal_sends_nothing` (stop variants),
    `close_joins_the_worker_and_so_waits_for_the_recreates_undo` (close joins
    `push_tasks`), `condemned_only_groups_do_not_keep_the_renewal_worker_alive`
    and `a_terminated_row_is_never_due_and_keeps_no_worker_alive` (the session
    parks); `push/tests/ews.rs` `a_received_ews_subscribe_keeps_its_registration`
    loses its `ews_worker` slot setup and cleanup, keeps
    `registration_count(&account) == Some(1)` and additionally asserts the
    `ews_topology` version moved; `subscribing_ews_on_a_closed_account_registers_nothing`
    and `an_ews_subscribe_racing_close_does_not_register` lose their
    `ews_worker` assertions and keep asserting that nothing was registered;
    `push/tests/dispatch.rs`
    `a_ledger_that_cannot_close_leaves_nothing_registered_in_either_mode` drops
    its `graph_worker` / `ews_worker` assertions; the remaining renewal tests
    that called `ensure_graph_worker` drive `renewal_session` directly;
  - new: `a_failing_row_does_not_starve_a_healthy_row_in_the_same_pass` (A),
    `every_row_failure_in_a_pass_is_reported` (A),
    `a_recreate_landing_after_its_session_dropped_is_invalidated_by_the_next_session`
    (A), `ews_release_is_claimed_once_and_joined_by_every_caller` (A),
    `a_failed_ews_release_keeps_its_obligation_without_hiding_a_newer_subscription`
    (A), `a_subscribe_answered_after_close_is_released_not_recorded` (A),
    `a_forced_session_drop_leaves_the_subscription_for_close_to_release`,
    `a_session_start_releases_orphaned_subscriptions`,
    `a_pass_whose_only_failure_is_terminal_ends_incomplete` (A: ablate by
    returning to "any non-terminal failure"),
    `a_pass_after_a_retired_row_stays_incomplete_until_the_handle_is_unsubscribed`
    (A), `a_renewal_pass_waits_for_the_push_gate_before_each_row` (A),
    `push_lanes_follow_mode_and_endpoint`.
- Google (`brokkr test -p bifrost-google <name>`):
  - deleted: `terminal_health_survives_late_and_lagged_subscribers`,
    `lagged_health_stream_emits_a_warning_with_the_loss_count`,
    `a_command_during_the_renewal_backoff_does_not_erase_the_damper`,
    `only_a_non_replayed_retry_candidate_status_is_damped`,
    `renewal_failures_escalate_the_warning_count_and_first_subscribe_is_silent`;
  - rewritten: `project_hold_watch_renewer_makes_no_wire_calls` (the session
    ends `Failed` with a retry hint and no wire call),
    `terminal_renewal_then_resubscribe_has_a_live_renewer` (the next session
    parks on `None`, a resubscribe publishes a due time, the session renews),
    `engine_directive_from_renewal_stops_local_retry_and_can_resubscribe`,
    `a_cancelled_account_retires_instead_of_renewing`,
    `retry_cadence_constants_are_the_documented_ones` (now pins the lane
    backoff);
  - new: `a_renewal_completing_after_expiry_records_a_gap` (A),
    `a_renewal_with_an_unknown_previous_expiration_records_a_gap`,
    `a_renewal_reply_lost_with_its_session_still_reaches_the_next_session` (A),
    `a_due_renewal_runs_at_session_start_after_a_failure`,
    `renew_not_due_after_a_concurrent_subscribe_makes_no_wire_call` (A),
    `a_stopped_session_does_not_cancel_an_in_flight_renewal`,
    `a_resubscribe_after_a_terminal_renewal_failure_reports_healthy` (A: the
    session yields `Healthy` right after the subscribe, not at the next
    renewal),
    `a_watch_lapsed_after_a_terminal_failure_and_restored_by_subscribe_records_a_gap`
    (A), `a_resubscribe_before_the_old_watch_expired_records_no_gap`,
    `a_first_subscribe_records_no_gap_and_no_fresh_watch`,
    `a_renewal_waits_for_the_push_gate` (A),
    `push_lanes_follow_pubsub_configuration`.
- Lifecycle producers (landing 1):
  `brokkr test -p bifrost-google a_successful_label_poll_yields_polled_after_its_commit`,
  `brokkr test -p bifrost-google a_failed_label_poll_yields_no_polled`,
  `brokkr test -p bifrost-jmap a_completed_mailbox_changes_burst_yields_polled`,
  `brokkr test -p bifrost-jmap a_failed_name_hydration_yields_no_polled`.

### 6.4 Green-tree gate

`brokkr check` after every landing. Never `-p`.

## 7. Landings (each coherent, each `brokkr check` green)

1. **Lifecycle heartbeat and the backoff primitive.** 3.5 (`Polled` and the
   doc rewrites), 4.1, 4.2 (including the count carried across a generation),
   5.7, the `BackoffPolicy` type from 3.1 (needed by 4.1; nothing else from
   3.1), the two lifecycle fields of the sync test stub (5.6), 6.1
   `backoff_policy_`, 6.2 `reconnect_backoff_` and `lifecycle_heartbeat.rs`,
   the 6.3 lifecycle producer tests. Docs: the lifecycle parts of section 8.
2. **Bounded ingress and synchronous sink (SYNC-009).** 3.4, 3.6, 4.3, 4.4,
   4.5, the landing-2 part of 4.9, and an interim in-process forwarder: the
   existing forwarder loop keeps calling the old `push_stream()` and hands
   each `WatchEvent` to `route_watch_event` against the slot's own target; on
   `Refused` it waits until the boundary reads `Run` (`wait_until_running`,
   which also returns on shutdown) and then awaits `reopen_tx.send`
   selecting on shutdown, since a refusal may now mean a pause as well as a
   full queue. Its 100 ms timeouts and `try_send` queue logic are deleted, so
   the SYNC-009 losses are gone in this landing; the replay-loop defect
   remains until landing 3. Nothing from 4.6-4.8 (no health, topology or
   lanes). Tests: 6.2 `push_ingress_`, `sink_`, the reconciler and
   `push_poll_latency` tests, and the landing-2-only interim forwarder test.
   Docs: the Push section's ingress and sink parts. Delete the SYNC-009 entry
   from `notes/bugs-sync.md`.
3. **Engine-owned push sessions (SHARED-014).** The rest of 3.1-3.3
   (including `PushGate`), 4.6-4.8, the landing-3 part of 4.9, 4.10
   (`ReopenRequest::PushRecovery`, the listener's stamp check, the reattach
   wait), 5.1-5.6, the interim forwarder deleted with its test, 6.1
   `session_stop_` and `push_gate_`, `push_lanes.rs` and every remaining 6.3
   provider test.
   This landing is one change because the `push_stream` reshape touches every
   implementor at once. Docs: the rest of section 8. Delete the SHARED-014
   entry from `notes/bugs-deferred.md`.

Each landing carries its reference edits; the keep/revert decision for each is
its gate results.

## 8. Documentation bricks

- `reference/types.md`: the push lane row of the trait table becomes four
  methods (`push_lanes` added); the push bullet describes lanes and sessions;
  the `close()` sentence becomes "does not delete durable server-side push
  subscriptions unless the provider documents that it does (Graph deletes its
  webhook rows)"; `ScopeLifecycleEvent::Polled`; `InvalidationSink` returns
  `SinkPush`; the new `push.rs` module.
- `reference/sync.md`:
  - "Push": rewritten - ingress holds only invalidation intent and collapses
    past `watch_capacity`; the sink handles control events synchronously
    (4.4) and `SinkPush`; the reconciler work set, pause retention and the
    account-wide hold (4.5); the lane supervisor, session handling, backoff
    sequence, the failure count carried across a generation, recovery
    routing, Connection parking and topology version, the generation fence
    and drain, the boundary gate on directive submission (lanes and sink),
    `ReopenRequest::PushRecovery` and the listener's stamp check, the push
    gate, coverage debt rules, push health with the meaning of each state and
    `SyncEngine::push_health` (4.6-4.8, 4.10); that control events are not
    ordered against pending invalidations, and why; that every push-originated
    `SyncEvent::Terminated` is account-scoped and parks no poll; that reattach
    waits for the retiring lane set before closing the old account. Every
    paragraph describing the 100 ms bound, the unbounded ingress, the lossless
    queue path and the forwarder is removed.
  - "Multiplexer": lifecycle backoff resets on `Polled` and `Lifecycle` only;
    a replacement reopens after the carried delay; the false "Google still
    retries locally" sentence goes.
  - "Construction" / config: `watch_capacity` meaning; "File map":
    `backoff.rs`, `push/ingress.rs`, `push/lanes.rs`, `push/health.rs`.
- `reference/error-model.md`: one paragraph - push sessions and lifecycle
  streams report every classified failure; the engine paces Retry and
  Reconcile identically for them because the session contract makes re-running
  the session the reconciliation (3.3 S1); engine directives route with the
  original error; terminal push failures are broadcast once.
- `reference/imap.md` "Push lifecycle": lanes and sessions replace workers;
  the latch and the pre-subscription queue are gone; exclusions (decided by
  recovery plan, never on a `Bye` or IDLE error), their lifetime and the slot
  shift they cause; NOTIFY refusal degrades with a warning; backoff values are the lane
  policy applied by the engine; the "resets on the first successful SELECT"
  sentence (already wrong: the code resets on a completed IDLE round) is
  replaced by the `Healthy` rule.
- `reference/jmap.md` "Push and reconnect" and "Push failure routing": the
  session replaces the reader; `ReconnectPolicy` initial and max feed the lane
  policy; connect timing after attach; no latch.
- `reference/graph.md`: "Renewal worker" becomes the renewal session (passes,
  `UnitFailed`, `Incomplete` including retired rows, the push gate before each
  row, `pending_gaps`); "EWS streaming mode" becomes the
  session with the release ledger; `close()` awaits `push_tasks` within its
  bound and leaves any still-running task to finish on its own (dropped, never
  aborted); the cancellation-safety subsections drop the worker-join wording;
  "Push failure routing" loses the replay-policy exception.
- `reference/google.md` "Push: Cloud Pub/Sub": the actor without a renewal arm,
  the `Renew` command and its rechecks, the schedule watch, gap bookkeeping
  (renewal and subscribe-after-lapse), the `fresh` recovery signal, the push
  gate, the session, the lane policy; the damper, health ring, warning count and latch
  paragraphs go.
- `reference/caldav.md`, `reference/carddav.md`: the "empty streams" sentence
  names the new signature and the empty lane list.
- Code docs: `crates/types/src/account.rs` (`push_stream`, `push_lanes`,
  `scope_lifecycle_stream`), `crates/types/src/cursor.rs`
  (`ScopeLifecycleEvent`), `crates/types/src/events.rs` (`WatchEvent`,
  `PushSource::Coalesced`, `InvalidationSink`), `crates/types/src/capabilities.rs`
  (`PushCapability` variants and `push_in_process` no longer describe
  `push_stream` traffic), `crates/sync/src/types.rs` (`watch_capacity`).

## 9. Stopping rule, exclusions, residuals and filed findings

Stops at: the push and lifecycle contract in `bifrost-types`, the engine's
push ingress, reconciler input, lanes, health and lifecycle backoff, and the
four push producers' session code, plus three narrow edits outside that
set, each required by a review finding: the reopen listener gains the
`PushRecovery` variant and its stamp check (4.6), reattach gains one bounded
wait for the retiring lane set before it closes the old account (4.10), and
`RecoveryContext` carries what those two need. Not touched: `push_subscribe` /
`push_unsubscribe` admission and registry semantics, the subscription
registry and reattach recreation logic, cursor drives, the listener's
handling of every other request kind, JMAP's three-lock subscribe
transaction, Google's mailbox-watch ownership.

Explicit residuals (documented, not open defects):

- A Subscribe (EWS) or create (Graph recreate) whose response never arrives may
  have created a server subscription with an unknowable id; it lapses or
  expires on the server's schedule.
- Bounded cleanup can expire (`drain_bound`, `CLOSE_WORKER_JOIN_TIMEOUT`);
  dropped work is then whatever account-owned tasks have not finished, which
  still run to completion.
- Push is a latency optimization over polling, which is never suppressed;
  every coverage-debt rule above trades latency, never data.
- A consumer directive accepted while the boundary read `Run` and overtaken
  by a pause before the listener dispatches it is declined like any declined
  repair (4.4), as on today's poll path.
- A Graph lane that went `Down` on retired rows and whose consumer then
  unsubscribed every handle reads `Down` until the next subscribe (5.4).
- Connection sessions do not consult the push gate after setup, so an EWS
  long poll continues through an engine hold recorded mid-session, as today
  (S13).

Separate items, excluded (not deferral): Graph lifecycle polling (the
`scopes.rs` comment anticipates it); IMAP lifecycle detection.

Filed for `notes/todo.md` (lateral, not built here):

- The engine's lifecycle task does not consult the account boundary, so
  lifecycle polls continue while an account is paused, against "a paused
  account is quiescent by contract".
- `EngineDirective::DisableScope` does not narrow push subscriptions; an IMAP
  folder excluded after a refused SELECT stays subscribed until the consumer
  resubscribes or the account is replaced.
- Push sessions keep running while an account is paused (today's behaviour:
  provider loops never consulted the boundary); only directive submission is
  gated (4.6 step 1). Whether lanes should park while paused - renewal lanes
  arguably must keep registrations alive, Connection lanes hold live
  connections against "a paused account is quiescent" - is the owner question
  Q1 in section 12, the same question as the lifecycle item above.
- `ReopenRequest::Recovery` from poll tasks, the reconciler, the lifecycle
  reader and the deferred-inventory worker carries no generation stamp, so a
  directive queued against a replaced account can still act on its
  replacement (for example several poll tasks each queuing `RestartAccount`,
  reattaching once per request). Pre-existing; this spec stamps only the
  lanes' requests (`PushRecovery`), which is where it claimed a fence.

## 10. Item requirements to bricks

| Requirement | Where |
|---|---|
| provider push loops stop owning the outer loop | 3.3 S2-S3, 5.1-5.5 (no provider spawns a push loop) |
| engine owns backoff | 4.1, 4.6 step 4, 3.2 `push_lanes` policies |
| engine owns throttle holds | 4.6 steps 1-4 (including the session's `PushGate`); 3.3 S13; 4.4 |
| engine owns directive routing | 4.6 steps 1, 3-4 (`pending`, boundary-gated reserve arm, `PushRecovery` stamps), 4.4, 4.10 |
| no provider decides retry versus give-up or how long to wait (every local `is_retryable` / `is_terminal` retry decision removed; what remains in providers only decides WHICH unit failed or whether an optional extension degrades) | 5.1 steps 4, 6, 7, 9; 5.2; 5.3 steps 3-4; 5.4; 5.5 |
| reset only on a demonstrated healthy read, one place | 3.3 S4, 4.1 (a generation change carries the count) |
| lifecycle loops | 3.5, 4.2 (reinterpretation in section 1) |
| push health as state | 4.7 |
| SYNC-009: no lost invalidation, bounded ingress | 4.3, 4.5 |
| SYNC-009: control events neither lost nor queued unboundedly | 4.4 (synchronous handling, `SinkPush::Refused` for a full queue or a non-`Run` boundary) |
| SYNC-009: ordering | 4.4 (deliberately not preserved, with the reason) |
| hermetic tests, ablation, full `brokkr check` | 6 |

## 11. Spar record

Codex (read profile), session `01a17305-7bcf-7582-8bee-422eac51ab9c`, six
design exchanges followed by three consistency reads of this document.
Opening position: literal one-shot sessions for push and
lifecycle, an engine lane loop with "once per failure run" directive dedupe,
engine-owned coverage debt for every lane, a mixed bounded ingress holding
warnings and terminations with severity eviction, and a stop signal the
engine raised and then simply awaited. Codex verified the three opening facts
(lifecycle mostly done, latched-terminal replay loop, SYNC-009 losses).
Conceded after verification against the code:

- `Healthy` does not mean continuous coverage; renewal lanes repair gaps
  inside healthy sessions -> provider-reported gaps committed to account state
  with the effect, drained one obligation per yield;
- one Graph renewal lane cannot fail on its first row without starving
  siblings, and keeping only the first error loses sibling throttle hints ->
  Registration sessions finish the pass, report every unit through
  `UnitFailed`, end `Incomplete`, and the engine waits for the longest hint;
- centralizing triage must not silently turn `Reconcile` into replay -> the
  re-runnable-unit contract (S1) with a per-provider statement;
- a stopped stream must be polled for its cleanup to run, and dropping must not
  cancel server-side effects -> S6-S8, account-owned tasks and the Google actor;
- Google watch ownership must outlive a session; the expiry check must use
  completion time, persisted in actor state;
- IMAP lane restarts must not exceed transports -> driver-exit-bound
  `push_slots` kept; exclusions need the attempted assignment's subscription
  version;
- coverage debt for Registration lanes cannot wait for a renewal days away ->
  lane kinds; cutover debt at lane start for Registration;
- severity eviction loses independent recovery obligations -> control events
  are never stored: the sink handles them synchronously and directives use the
  bounded reopen channel with an explicit `Refused` handing the event back;
- "once per failure run" has no defined end -> no lane dedupe; one forward per
  failed session, paced by the exact backoff sequence;
- retiring-generation outcomes must not touch the replacement -> the
  supervisor's sequential lane sets and drain mode;
- a parked lane must not miss a topology bump that landed while its session
  failed -> topology snapshot before each session;
- a reopen channel that is full must not block session polling or stop ->
  lane-local pending queue flushed through a reserve arm;
- taking the ingress must transfer, not discharge, the work -> the
  reconciler's persistent work set with pause retention and the account-wide
  hold;
- a single EWS live-id slot loses ids across release handoffs, and a
  double-spawned release can resurrect a deleted id -> the per-id ledger with
  release as an atomic claim and a shared completion.

Accepted by codex after the author proposed it in answer to its lifecycle
findings: lifecycle `Polled` delivers the item's property without one-shot
lifecycle streams and keeps JMAP's session-change wake. Conceded by codex: no
extra
cursor fence is needed for invalidations racing a scope restart (`with_drive`
and the registry generation already fence it). At the end of the design
exchanges codex found no further correctness hole and restated the mechanism
in its own words; the restatement matched sections 3-5.

Consistency read of the written document (16 findings, all verified and
folded): `close()` must drop, not abort, timed-out push tasks; Google
`Reconcile` and `Engine` renewal failures keep the watch due now instead of
parking it; an empty lane set must not spin the supervisor; ready work set
entries sweep before waiting and the work set is bounded on merge; the
lane-local directive queue is bounded by never opening a session while it is
non-empty, and the `RestartAccount` wait starts only after submission; the
IMAP INBOX fallback cannot bypass exclusions, NOTIFY degradation is limited to
tagged refusals of the NOTIFY command, and the subscription version lives
under the scopes guard; Graph drains gaps after every pass and marks the
topology seen before reading the groups; `subscribe_ews` loses its worker
await and `RegistrationGuard`; 4.9 is split by landing and the lifecycle
harness moves to landing 1; existing lifecycle tests get a `skip_polled`
adapter; the lane lease and invalid-lane handling are owned and shared
(`LaneLease`); S2 states the unit-failure exception; health writes are
per-entry, `any_down` is defined, and failure counts are recorded before the
down edge publishes; the futures dependency statement, the JMAP sweep note and
the EWS jitter granularity are corrected. A second read found five more, also
folded: retryable NOTIFY failures report `UnitFailed` (a second S2 exception)
and the definite-refusal fallback is decided after classification; no-lane
implementors yield the S11 failure; the work set discharges undispatchable
scopes; the received-EWS-subscribe test keeps its registration assertion; the
malformed-close JMAP test expects a retryable `Failed`. The author's own pass
added: EWS Subscribe is not raced against stop, the drain's `Draining` write
is the only health write of a retiring lane, and `SinkPush::Refused` states
that refusals can persist through a consumer `Pause`. A third read found one
more, folded: the NOTIFY degradation requires a command-refusal
classification, so a tagged `NO` carrying an authentication or authorization
code still ends the session `Failed`; it then reported no remaining problem.

## 12. Cold review record (r1, r2)

Two cold reviews of this document (r1: 2 P1, 4 P2, 12 P3, 3 questions; r2:
1 P1, 4 P2) were consolidated and each finding was checked against the tree
at 31d7d3ed before folding. Overlaps: r1 P1-2 and r2 1 both concern
directives a lane submits (stacking under a pause, and surviving the
generation fence); they are fixed together (boundary gate plus generation
stamp). r1 P3-7 and r2 3/4 all concern what `PushHealth` reports; they are
fixed together in 4.7, 5.4 and 5.5.

### Folded (validated)

| Finding | Verified against | Where folded |
|---|---|---|
| r1 P1-1 work set spins under `CheckpointNow` | `begin_activity` refuses unless `Run`; `refused_activity_outcome` maps `CheckpointNow` to `Paused`; `CheckpointLatchGuard` holds it until quiescence | 4.5 park, 6.2 test |
| r1 P1-2 lanes stack directives through a pause | `restart_account` starts with `wait_until_running`; serial listener; `mpsc::channel::<ReopenRequest>(16)`; `begin_scope_repair` declines under pause | 4.6 step 1 and reserve arm, 6.2 tests |
| r2 1 generation fence does not fence queued requests | `ReopenRequest::Recovery` has no generation; reattach swaps then bumps under the reopen lock | 4.6 fence paragraph, `PushRecovery`, 6.2 test, filed residual for other producers |
| r1 P2-1 unpaced reattach loop on persistent `RestartAccount` | `restart_account` budget counts failures within one call; lifecycle loop resets on every generation, including on its own `AwaitReopen` | 4.1, 4.2, 4.6 supervisor carry, 6.2 tests |
| r1 P2-2 IMAP exclusion keyed on kind | `report_idle_failure` always builds `with_folder_scope`; `derive_sync_state` maps account-wide classes | 5.1 steps 6 and 9, S2, 6.3 tests |
| r1 P2-3 consumer directives during a pause | today the watch channel holds them through the reconciler's pause park | 3.4 `Refused`, 4.4, landing 2 forwarder, 6.2 test |
| r1 P2-4 drain bound equals detach deadline; reattach closes during drain | `detach_inner` one `detach_timeout` worker deadline; IMAP `command_timeout` 60 s; `reattach_account_paced` closes after the bump with no wait | 4.6 struct, 4.9, 4.10, 9, 6.2 tests |
| r2 2 long-lived sessions bypass new holds | lanes check holds only before opening; `ThrottleBucket` has no notification | 3.1 `PushGate`, 3.2, S3, S13, 4.6 step 2, 5.4, 5.5, tests |
| r2 3 Graph `Healthy` after terminal loss | pass returned only "any non-terminal failure"; `has_live_graph_subscription_group` ignores terminated rows | `Incomplete` doc, S3, 5.4, 4.7, tests |
| r2 4 Google health stuck `Down` after resubscribe | Registration `Established` only lifts `Starting` | 5.5 `fresh`, test |
| r2 5 Google gap through subscribe | renewal gap rule fires only on `Renew` | 5.5 `lapsed`, tests |
| r1 P3-1 terminal-parked lane owes nothing | 4.6 step 4 | 4.6 `Terminal` arm, test |
| r1 P3-2 `Reconfigure` unpaced | 4.6 step 4 | blind-`Reconfigure` pacing, test |
| r1 P3-3 S5 contradicted by Connection providers | 5.1 step 10, 5.2, 5.3 | S5 scoped |
| r1 P3-4 `BackoffPolicy` normalization bypassable | public fields, no `#[non_exhaustive]` | 3.1 private fields, 4.1 `new` |
| r1 P3-5 hold cleared by a bare `changed()` | `watch::Receiver` semantics | 4.5 |
| r1 P3-6 ordering dropped silently | 4.4 | 4.4 paragraph, section 10 row |
| r1 P3-7 health of idle and reconfiguring lanes undefined | 5.1 step 2, 4.6 | 4.7 state meanings, `Reconfigure` sets `Starting`, test |
| r1 P3-8 exclusion shifts slots | 5.1 `choose_assignment` | 5.1 paragraph, section 8 |
| r1 P3-9 push `Terminated` scope changes | today's reconciler broadcasts on `CursorScope::Account` | 4.4 "Terminated scope", 4.6; Q3 open |
| r1 P3-10 lifecycle fallback ordering | `multiplexer/mod.rs` lifecycle loop | 4.2 wording pin (see partial rejection below) |
| r1 P3-11 missing gates | section 6 | 6.2 and 6.3 additions |
| r1 P3-12 `reader_handle` is a method | `crates/jmap/src/sync/push.rs`, `account.rs` close | 5.2 |

### Rejected, in whole or in part

- r1 P3-10, the defect half. The ambiguity is real and the wording is now
  pinned, but the claimed consequence ("the fallback lags one step") cannot
  occur in behaviour: the wait is `max(delay, retry_wait)` and
  `lifecycle_retry_wait` returns `max(fallback, hint)`, so a lagged fallback
  still yields `max(delay, hint)`. No test is added, because no ablation
  could make one fail.
- r2 4, its caveat that the recovery signal must not reset failure backoff.
  A subscribe that commits a fresh watch is a successful `/watch` round trip,
  the very wire call a renewal makes and on which a renewal yields `Healthy`,
  so it is demonstrated healthy progress under S4 and resetting is correct.
  The finding itself is folded.
- r1 Q2 as a separate question. Folding P2-1 answers it: a reattach is a
  clean slate only for a lane or reader that last demonstrated health; a
  count earned by unrecovered failures crosses the generation. No
  per-account reattach budget across calls is added.

No finding was rejected outright: every other finding described the code
accurately and named a real gap in this document.

### Questions for the owner

- Q1 (r1). Should push lanes run while the account is paused? This spec
  keeps today's behaviour (provider loops never consulted the boundary) and
  gates only directive submission, which fixes r1 P1-2 whichever way Q1 is
  answered. Renewal lanes arguably must keep server registrations alive
  through a pause; Connection lanes hold live connections against "a paused
  account is quiescent by contract". Filed in section 9 beside the
  lifecycle-while-paused item, which is the same question.
- Q3 (r1). Should a push unit failure with a `Terminal` plan be surfaced as
  `SyncEvent::Terminated` at all, given that the scope keeps polling? This
  spec keeps today's event and today's account scope (4.4) rather than
  introduce a folder-scoped `Terminated` that would read as "this scope's
  poll is parked"; a distinct warning would need a way to carry the
  structured error.
