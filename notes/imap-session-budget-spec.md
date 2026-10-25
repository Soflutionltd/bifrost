# Spec: one physical session budget for an IMAP account, IDLE included

Technical implementation specification for the `bifrost-imap` finding
"IDLE sessions are still uncounted against `pool_cap`" (IMAP-018 in
`notes/bugs-imap.md`). Transient: it lives in `notes/` and dies when the work
lands.

## Standing references (read them, do not just note them)

Implementers and reviewers must READ these before touching code. They are the
ground this is built on and judged against.

- `reference/technical-implementation-spec.md` - the contract this document is
  written against.
- `reference/error-model.md` - the cross-cutting `AccountError` contract. This
  work moves no error classification, but it changes which `push_subscribe`
  scopes land in the failed lane ("Batch and stream outcomes", the
  `push_subscribe` paragraph: refused scopes carry their own error, the handle
  covers exactly the succeeded lane, `Err` still means nothing subscribed) and
  it keeps `Error::Closed` as the answer a closed pool gives every acquisition.
- `reference/imap.md` - "Configuration and connect", "Account layer" (the
  `pool.rs` bullet), and all of "Push lifecycle" through the backoff paragraph,
  in particular the registry, close, dial-admission and parking linearization
  paragraphs, and the NOTIFY paragraph (no initial STATUS snapshot).
- `notes/bugs-imap.md`, entry IMAP-018 - the source item. Its original filing
  (commit `bd8a21bf`) quoted the reference's former promise that the push
  budget kept "the account's total connection footprint inside the low
  per-user allowance many providers enforce"; the October wave corrected the
  documents to the real total `pool_cap - 1 + idle_connection_budget` and left
  the product question. The owner ruled on 2026-10-21 that it needs a
  specification.

## The source item, and how this spec reads it

The entry asks whether IDLE workers should draw from a shared session budget
so the account's footprint stays inside `pool_cap` on providers with a low
per-user connection limit. This spec answers yes and builds it: `pool_cap`
becomes the cap on PHYSICAL IMAP sessions of one open account - data sessions,
push sessions, replacement dials, and sessions still tearing down - and push
capacity is carved out of it, never added to it.

Reading the code against that goal turned up five things the item does not
say, all verified, all load-bearing for the design:

1. **The current permit is a checkout permit, not a session permit.** Parked
   data members hold no permit; a member is dialed only under a checkout
   permit, so the data member count happens to stay under the data cap, but
   nothing counts a session whose handle is gone while its driver still runs
   its best-effort LOGOUT (`LOGOUT_DRAIN_TIMEOUT`), a discarded member,
   dead parked members pruned by `idle.retain(is_alive)` (`is_alive` is the
   command channel, which the driver closes before its future finishes), a
   dial whose result was sent into an abandoned oneshot, or the old member
   during `replace_selected_member`. A strict cap needs every transport to own
   its capacity from connector start until its driver has exited. That is the
   structural change here: **a lease belongs to a session, not to a
   checkout.**
2. **Push sessions must never be adopted from the data pool.** A parked data
   connection can hold unsolicited FETCH / VANISHED / EXISTS events from its
   previous mailbox, which carry no mailbox identity. An IDLE worker that
   adopted it and SELECTed its own folder would feed those events to
   `absorb_idle_event`, which records MODSEQs and clears cache entries in the
   WRONG folder. Push therefore always dials fresh, as it does today, and makes
   room by retiring a parked data member when the cap is full.
3. **Worker count does not bound push sessions.** A worker that retires its
   session on reassignment leaves a tailing transport; with only a shared
   session semaphore, reconfiguration churn could fill every permit with push
   sessions and tails. Push needs its own sub-cap, held through driver exit.
4. **Waiting for a session widens the uncovered window.** A push worker that
   today dials at once may now wait behind data checkouts. Changes landing
   between subscribe and the first IDLE are not reported by IDLE. bifrost-sync
   never suppresses polling on push coverage, so nothing is lost: this is a
   latency window that polling already covers, and it exists today for the
   length of one dial. Waiting can stretch it arbitrarily, so a push session
   that had to WAIT for capacity publishes a per-folder invalidation for what
   it starts covering. One that got capacity at once does not, which keeps the
   standing decision behind `register_notify` leaving STATUS off (no
   invalidation burst right after subscribe, when discovery and inventory have
   just run).
5. **Today's checkouts are FIFO at the cap, and that must survive.** A
   checkout's permit drops in `PooledConn` after `Drop::drop` has parked the
   member, and tokio's semaphore assigns a released permit straight to the
   oldest queued waiter. A task that drops a checkout and immediately asks for
   another goes to the back of the queue. Real loops do exactly that:
   `flush_mutation_groups` in `mutate.rs` takes a fresh checkout per folder
   (through `run_folder_mutation`), and
   the only await between iterations is a `tx.send` that usually completes
   without yielding. Once parking releases no permit (point 1), a naive
   "park, then wake a waiter" lets that loop re-adopt the member it just
   parked ahead of every waiter for as long as it runs. Returned members are
   therefore HANDED to the oldest waiter, and parked only when nobody waits.

## Survey of the ground

### Configuration (`crates/imap/src/account/factory.rs`)

- `ImapAccountConfig::pool_cap: usize` (pub, default 4): today "data-session
  sizing input", data cap `data_connection_budget() = max(pool_cap - 1, 1)`.
- `ImapAccountConfig::idle_connection_budget: usize` (pub, default 4): today
  extra non-NOTIFY push sessions, clamped to at least one.
- `data_connection_budget` (private) feeds `Pool::new(.., data_cap, ..)` at
  open. Test `data_and_idle_connection_budgets_are_separate` pins today's
  arithmetic.
- `open_using` connects and authenticates the primed connection BEFORE the
  pool exists (discovery runs on it), then hands it to `Pool::new`. It builds
  the account capabilities through `capabilities::build_capabilities`, whose
  `push` is `InProcess` whenever the profile advertises IDLE.

**Adjudication: option 1.** Both the meaning change and the default change
proceed under the existing rules. AGENTS.md requires an owner decision for
removal or renaming of a published item; both fields keep their names and
types. The 2026-09-30 shape ruling does not itself authorize semantic or
default changes, but its silence does not create another approval requirement.
These changes belong to the specified IMAP-018 shared-budget work, rather
than an unrelated product decision. No owner decision remains open.

### Pool (`crates/imap/src/account/pool.rs`)

- `PoolInner { permits: Arc<Semaphore>(data_cap.max(1)), idle:
  Mutex<Vec<PoolMember>>, sessions: Mutex<Vec<SessionRegistration>>, dials:
  RwLock<()>, cancel_dials, closed, qresync_policy, config, meter,
  bandwidth_budget, close_completion }`.
- `checkout_for_folder` / `checkout_any`: acquire a permit
  (`PoolInner::permit`, closed re-checked on both sides of the await), prune
  dead parked members, adopt (affinity first), else `dial(Some(permit))`.
- `dial_idle`: `dial(None)` - no permit at all. The only push entry point.
- `dial` -> `dial_with_permit(connect, permit)`: owned task, read admission on
  `dials`, select on `cancel_dials` / `tx.closed()` / `dial_admitted`;
  `dial_admitted` connects (keepalive disabled at the connector), registers,
  sets keepalive, authenticates, enables QRESYNC per policy. Failure
  terminates the connected driver before the permit clone drops; a successful
  send into a receiver dropped after the send is NOT covered.
- `PooledConn { member, pool: Arc<PoolInner>, _permit: Arc<OwnedSemaphorePermit> }`;
  Drop parks a reusable member under the idle lock (closed re-checked under
  it), then the permit field drops. `discard` drops the member while the
  permit stays with the checkout. `replace_selected_member` awaits an
  unbounded `logout()` on the old member, then dials under the SAME permit,
  so today the replacement never waits for capacity.
- `close`: flag, `permits.close()`, `cancel_dials`, owned drain task
  (exclusive `dials`, drain idle and registry, concurrent LOGOUT under one
  `command_timeout`, join every driver).
- `cfg(test)` seams: `dial_with(connect)` and `dial_with_permit(connect,
  permit)`; tests `close_cancels_a_dial_before_registration`,
  `close_cancels_authentication_already_on_the_wire`,
  `close_cancels_stalled_token_lookup_and_joins_its_driver`,
  `cancelled_checkouts_keep_capacity_until_the_dial_is_cancelled`,
  `close_gates_every_way_into_the_pool` (calls `dial_idle`),
  `dropping_a_checkout_releases_its_permit`,
  `a_zero_data_cap_is_clamped_to_one_permit` and the affinity / parking /
  close tests, all built on `pool_of(primed, data_cap)` with an unroutable
  `test.invalid` config standing in for "must not dial". Several reach into
  `pool.inner.idle` directly (`park`, `set_primed_affinity`, `idle_len`,
  `idle_selection`, `close_joins_a_detached_healthy_drivers_graceful_logout`,
  `a_drop_that_finishes_after_the_close_drain_parks_nothing`,
  `bandwidth_two_pool_members_share_one_inbound_rate`).

### Connection lifecycle (`crates/imap/src/connection/`)

- `DriverTask { handle: tokio::Mutex<Option<JoinHandle>>, abort_handle,
  inbound_bytes }`, constructors `new(Option<JoinHandle>)` and
  `with_inbound_counter(..)`; `is_finished`, `finish_or_abort`,
  `abort_and_join`, `abort`.
- Driver spawn sites: `lifecycle.rs` `connect_with_tls_connector_metered`
  (`tokio::spawn(driver::driver_task(..))`, no await between spawn and
  return), `test_support.rs` `driver_pair_with_capacity_and_budget` (the
  private body behind `driver_pair`, `driver_pair_metered` and
  `driver_pair_with_capacity`), and `connection/tests.rs`
  (`DriverTask::new(Some(handle))`). `DriverTask::new(None)` fakes:
  `test_support.rs` (stub connection) and `pipeline/tests.rs`.
- `ImapConnection::drop` aborts the driver of an abandoned connection; a
  healthy one runs the driver's bounded best-effort LOGOUT.
- Public `ImapConnection::connect` / `connect_with_tls_connector` and
  crate-private `connect_config_metered` / `ImapConfig::connect_authenticated_metered`
  all route through `connect_with_tls_connector_metered`.
- STARTTLS runs before the spawn and the driver spawns no child task owning
  the socket, so "driver future destroyed" means "transport destroyed".

### Push (`crates/imap/src/account/push.rs`)

- `PushState::new(supports_notify, idle_budget)` stores `idle_budget.max(1)`.
- Admission in `push_subscribe`: `supports_notify || covered.contains ||
  covered.len() < idle_budget`; refusal is `idle_budget_error()`
  (`Unsupported(PushSubscribe)`, user-safe text containing "budget").
  `push_subscribe` bumps the resubscribe generation unconditionally, even when
  it accepted nothing; `push_unsubscribe` bumps on every removal.
- `ensure_idle_tasks`: 1 worker with NOTIFY, else `idle_budget`.
- `idle_loop`: marks the generation seen, chooses a folder, `dial_idle`
  (not raced against anything), SELECT, `register_notify`,
  `signal_notify_coverage_loss`, IDLE rounds; a bump during the dial is
  picked up by the latched first IDLE round. The Reidle branch keeps the
  session; every other exit drops the connection (the driver's graceful
  LOGOUT) or awaits an unbounded `logout()` on cancel/shutdown.
- `register_notify` leaves `status` off the `NOTIFY SET` deliberately (its
  doc, and `reference/imap.md`: "no initial STATUS snapshot"), and the
  `idle_loop` comment on not emitting `Reconnected` on a first connect rests
  on the same reasoning.
- The comment on `resubscribe_action` says pinning the re-IDLE wiring and the
  NOTIFY-collapse wiring "would need a dial seam in the pool" and that they
  should be pinned together if that seam is ever built.

### Capabilities (`crates/imap/src/account/capabilities.rs`)

- `build_capabilities(profile, folders, has_sieve, contacts, calendars,
  submission_configured, foreign_namespaces_advertised)` sets
  `push: InProcess` iff the profile advertises IDLE. bifrost-sync's push
  forwarder (`engine/attach.rs`) and reattach (`engine/reattach.rs`) gate on
  `capabilities().push != None`. The crate treats capability honesty as a
  rule (the `blob_range` comment in the same function).

### Account wiring (`crates/imap/src/account/mod.rs`, `test_support.rs`)

- `ImapAccount::new` builds `PushState::new(parts.supports_notify,
  parts.config.idle_connection_budget)`.
- `scripted_account_parts(conn, pool_cap)` sets `config.pool_cap` and calls
  `Pool::new(config, conn, pool_cap, ..)` (the test cap is used directly as the
  data cap); its doc and `scripted_tests.rs` `scripted_account`'s doc both
  claim the pool "holds exactly `pool_cap` permits".
  `guarded_stream_tests::pool_with` does the same.

### Existing tests that pin today's semantics and must change

- `factory.rs` `data_and_idle_connection_budgets_are_separate`.
- `scripted_tests.rs` `non_notify_push_reports_the_folder_beyond_its_budget_as_poll_only`
  (asserts default budget 4 at `pool_cap = 1`),
  `a_folder_scope_with_an_unsendable_name_is_refused_not_misreported` and
  `a_composed_dav_collection_scope_is_refused_by_push_admission` (both at
  `pool_cap = 1`, which leaves no push capacity under the new rule).
- `scripted_tests.rs` `deselect_fallback_releases_the_old_connection_before_redialing`:
  vacuous for the ordering it claims. It closes the pool before running the
  fallback, and `Pool::close` sends LOGOUT to every registered session,
  including the checked-out one, so the LOGOUT the script reads is the close
  drain's. It passes whatever order `replace_selected_member` uses.
- `pool.rs` `close_gates_every_way_into_the_pool` (`dial_idle`),
  `cancelled_checkouts_keep_capacity_until_the_dial_is_cancelled`
  (`PoolInner::permit`), `dropping_a_checkout_releases_its_permit` and
  `a_zero_data_cap_is_clamped_to_one_permit` (names and docs describe a
  per-checkout permit and a `Pool::new` clamp that no longer exist), every
  caller of `dial_with`, and every test that touches `pool.inner.idle`.

Each change is justified by the semantics change itself; no assertion about
behaviour this spec does not change is weakened.

### Nested checkouts

None: the QRESYNC parse fallback in `changes.rs` drops its checkout before
re-entering CONDSTORE (commented), folder CRUD passes its checkout into the
re-LIST (`container_create_completes_under_a_single_permit_pool`), and
per-folder loops in `pim.rs` / `mutate.rs` drop each checkout per iteration.
Data concurrency can already fall to one today (`pool_cap <= 2`), so the new
minimum introduces no new deadlock class. Streams holding a checkout while
awaiting output capacity are an existing backpressure dependency, more
frequent at low data capacity; unchanged here.

### Other IMAP session minters

Only the factory's primed connection (before the pool exists) and the pool.
SMTP submission, CardDAV/CalDAV and ManageSieve are separate protocols with
their own limits and are not IMAP sessions.

## Target design

### Semantics

- `pool_cap` = maximum physical IMAP sessions one open account holds at once,
  counting every session from the moment its dial starts connecting until its
  driver task has exited. Clamped to at least 1. Default stays 4.
- Push capacity (`ImapAccountConfig::push_session_capacity(supports_notify)`):
  `headroom = pool_cap.max(1) - 1`; with NOTIFY `min(1, headroom)`; without
  NOTIFY `min(idle_connection_budget.max(1), headroom)`. Push therefore never
  holds the last session; at `pool_cap = 1` push is refused entirely, every
  folder stays poll-only, and the account advertises `PushCapability::None`.
  No IDLE/data multiplexing on one connection is built.
- `idle_connection_budget` = the ceiling on non-NOTIFY push sessions, now
  inside `pool_cap`. Default drops from 4 to 2, so a fully pushed default
  account keeps two data sessions (an interactive mutation is not serialized
  behind a long inventory page). Keeping the default at 4 would instead allow
  `min(4, 4 - 1) = 3` push sessions, leaving only one data slot. The headroom
  clamp protects that last slot; choosing 2 preserves room for two data
  sessions at the default cap. Clamp to at least one stays.
- Data sessions have no sub-cap of their own. With no push subscribed, data
  checkouts can use all `pool_cap` sessions (today `pool_cap - 1`); push
  establishment then depends on retiring a parked data member or waiting for
  a checked-out one to come back.
- Push capacity and worker topology are fixed at open from the account's
  admission policy (`supports_notify` from the factory profile). A redialed
  session advertising different capabilities never resizes either.
- Checkouts at the cap are served in arrival order (see "Returning a member"),
  as they are today.

Costs this introduces, all consequences of "held through driver exit", to be
stated in the reference:

- At the cap, a discarded or non-reusable member that is not abandoned runs
  the driver's graceful LOGOUT, bounded by `LOGOUT_DRAIN_TIMEOUT`, before its
  permit returns, and the next dial waits for it. Today the permit returns
  immediately and the dial overshoots the cap.
- Every push redial (reassignment, BYE, error) waits for its push slot until
  its previous session's driver exits. With NOTIFY (`push_capacity = 1`),
  every reassignment serializes behind the old session's LOGOUT.
- A push worker can wait behind data checkouts that are held for a long time
  (a long stream). Polling covers the scopes meanwhile.
- The DELETE / RENAME replacement on a server without UNSELECT can wait at
  the cap: the room its own retired member frees goes to an acquisition that
  was already queued before it (see "Fresh acquisition"). Today it reuses
  the checkout's permit and never waits.

### Driver exit retention (`crates/imap/src/connection/mod.rs`)

```rust
/// Values whose lifetime is tied to a driver task's transport: released only
/// once the driver future, and with it the socket, has been destroyed.
pub(crate) struct DriverExit {
    state: std::sync::Mutex<ExitState>,
}

enum ExitState {
    Running(Vec<Box<dyn Send>>),
    Exited,
}

impl DriverExit {
    fn running(retained: Vec<Box<dyn Send>>) -> Arc<Self>;
    fn exited() -> Arc<Self>;
    /// Store `value` until exit, or drop it at once (outside the lock) if the
    /// driver has already exited. Linearized with `mark_exited` by `state`.
    fn retain(&self, value: Box<dyn Send>);
    /// Idempotent. Swaps in `Exited` under the lock, drops the taken values
    /// after unlocking.
    fn mark_exited(&self);
    fn has_exited(&self) -> bool;
}

/// The future actually spawned for a driver. `inner` is destroyed BEFORE the
/// exit is published, on completion (inside `poll`) and in `Drop` (abort,
/// panic unwinding through tokio, runtime teardown, abort before first poll).
struct DriverScope<F> {
    inner: Option<Pin<Box<F>>>,
    exit: Arc<DriverExit>,
}
impl<F: Future<Output = ()>> Future for DriverScope<F> { /* poll inner; on Ready: inner = None, exit.mark_exited() */ }
impl<F> Drop for DriverScope<F> { /* inner = None; exit.mark_exited() */ }

/// Hand-off cell for values that must be owned by a driver from the instant
/// it is spawned. The pool's dial task keeps one clone; the connector takes
/// the values at spawn time with no await in between.
#[derive(Clone)]
pub(crate) struct LeaseHandoff(Arc<std::sync::Mutex<Vec<Box<dyn Send>>>>);
impl LeaseHandoff {
    pub(crate) fn new(values: Vec<Box<dyn Send>>) -> Self;
    pub(crate) fn none() -> Self;
    pub(crate) fn take(&self) -> Vec<Box<dyn Send>>;
}
```

`DriverTask` gains `exit: Arc<DriverExit>` and replaces `new` /
`with_inbound_counter` with:

```rust
impl DriverTask {
    pub(crate) fn spawn<F>(driver: F, inbound_bytes: Arc<AtomicU64>, retained: Vec<Box<dyn Send>>) -> Self
    where F: Future<Output = ()> + Send + 'static;
    /// Fakes with no driver: starts `Exited`.
    pub(crate) fn absent() -> Self;
    pub(crate) fn retain_until_exit(&self, value: Box<dyn Send>);
    #[cfg(test)] pub(crate) fn has_exited(&self) -> bool;
}
```

`spawn` builds the `DriverScope` (exit constructed OUTSIDE any async block and
moved in as a field) and `tokio::spawn`s it; the join handle and abort handle
are kept exactly as today, because close still joins the task - the exit slot
proves transport destruction, it does not replace joining.

`connect_with_tls_connector_metered` and `connect_config_metered` take
`leases: &LeaseHandoff` and spawn with `DriverTask::spawn(driver::driver_task(..),
inbound_bytes, leases.take())`, synchronously after the setup awaits. Public
`connect` / `connect_with_tls_connector` and
`ImapConfig::connect_authenticated_metered` pass `&LeaseHandoff::none()`.
In `test_support.rs`, the private `driver_pair_with_capacity_and_budget` gains
a `leases: &LeaseHandoff` parameter and spawns through `DriverTask::spawn`;
the existing wrappers pass `&LeaseHandoff::none()`, and a new
`driver_pair_leased(greeting, &LeaseHandoff)` wrapper passes the given one at
the default capacity and budget. `connection/tests.rs` switches to
`DriverTask::spawn`; the two `new(None)` fakes become `DriverTask::absent()`.

### Pool connector seam (`pool.rs`)

```rust
pub(crate) struct ConnectRequest {
    pub(crate) config: crate::ImapConfig,              // keepalive already None
    pub(crate) meter: Option<bifrost_net::MeterSinkHandle>,
    pub(crate) bandwidth_budget: bifrost_net::BandwidthBudget,
    pub(crate) leases: crate::connection::LeaseHandoff,
}
/// Contract: the connector spawns the connection's driver as its last step,
/// with `req.leases.take()` handed to the spawn and no await between the take
/// and the spawn, and returns immediately after the spawn. `dial_admitted`
/// registers the connection only once the connector has returned, so a
/// connector that awaited after spawning would, on cancellation, drop an
/// unregistered live connection: close would never join it, and it would run
/// a graceful LOGOUT after `Account::close()` returned.
pub(crate) type Connector = Arc<
    dyn Fn(ConnectRequest) -> futures::future::BoxFuture<'static, Result<ImapConnection, Error>>
        + Send + Sync,
>;
pub(crate) fn production_connector() -> Connector; // connect_config_metered(&req.config, req.meter, Some(req.bandwidth_budget), &req.leases)
```

`PoolInner` gains `connector: Connector`. The production keepalive decision is
preserved: the connector dials with keepalive disabled and `dial_admitted`
sets it after registration.

### Pool session accounting (`pool.rs`, full rewrite of admission)

```rust
struct PoolInner {
    sessions: Arc<Semaphore>,          // pool_cap.max(1); one permit per physical session
    idle: Mutex<IdleState>,
    connector: Connector,
    // unchanged: config, meter, bandwidth_budget, closed, qresync_policy,
    // sessions registry (rename the registry field to `registry` to free the name),
    // dials, cancel_dials, close_completion
}

struct IdleState {
    parked: Vec<PoolMember>,           // each still owns its permit via its driver exit
    waiters: VecDeque<Waiter>,         // acquisitions waiting for a returned member, oldest first
    next_waiter: u64,
}

struct Waiter {
    id: u64,
    tx: tokio::sync::oneshot::Sender<PoolMember>,
}

/// Built under the idle lock in the same synchronous section that pushes the
/// `Waiter`, never inside a later await, so a future dropped before its next
/// poll still runs this guard. Drop: lock, remove `id` from `waiters` if still
/// there, then `rx.try_recv()`; a member already handed here goes back
/// through `return_member` (next waiter, or parked) instead of being lost.
struct WaiterGuard {
    pool: Arc<PoolInner>,
    id: u64,
    rx: tokio::sync::oneshot::Receiver<PoolMember>,
}

pub(crate) struct PooledConn {
    member: Option<PoolMember>,
    pool: Arc<PoolInner>,
}

/// Capacity granted for one fresh session, not yet dialed.
pub(crate) struct FreshReservation {
    pool: Arc<PoolInner>,
    permit: OwnedSemaphorePermit,
    extra: Vec<Box<dyn Send>>,
    waited: bool,
}

impl FreshReservation {
    /// Whether capacity was not available on the first attempt.
    pub(crate) fn waited(&self) -> bool;
    /// Fold in a wait that happened before the pool was asked (the push slot).
    pub(crate) fn with_prior_wait(self, waited: bool) -> Self;
    pub(crate) async fn dial(self) -> Result<Arc<ImapConnection>, Error>;
}

impl PoolInner {
    async fn acquire_member(self: &Arc<Self>, affinity: Option<&MailboxName>) -> Result<PoolMember, Error>;
    /// Capacity for a fresh session that never comes from the parked list.
    /// `retiring` is the caller's own member, retired after this acquisition
    /// has queued on the semaphore (member replacement).
    async fn reserve_fresh(self: &Arc<Self>, extra: Vec<Box<dyn Send>>, retiring: Option<PoolMember>)
        -> Result<FreshReservation, Error>;
    /// Hand a reusable member to the oldest waiter, or park it.
    fn return_member(&self, member: PoolMember);
}

impl Pool {
    pub(crate) fn new(config: Arc<ImapAccountConfig>, primed: ImapConnection, connector: Connector,
                      meter: Option<MeterSinkHandle>, bandwidth_budget: BandwidthBudget) -> Self;
    pub(crate) async fn checkout_for_folder(&self, folder: &MailboxName) -> Result<PooledConn, Error>;
    pub(crate) async fn checkout_any(&self) -> Result<PooledConn, Error>;
    /// Thin wrapper over `PoolInner::reserve_fresh(extra, None)`; used by push.
    pub(crate) async fn reserve_fresh(&self, extra: Vec<Box<dyn Send>>) -> Result<FreshReservation, Error>;
    /// `reserve_fresh(extra)` then `dial()`.
    pub(crate) async fn acquire_fresh(&self, extra: Vec<Box<dyn Send>>) -> Result<Arc<ImapConnection>, Error>;
    pub(crate) async fn close(&self);
    #[cfg(test)] pub(crate) fn available_sessions(&self) -> usize;
}
```

`reserve_fresh` lives on `PoolInner` because `replace_selected_member` runs on
`PooledConn`, which holds `Arc<PoolInner>`, not `Pool`.

The session cap comes from `config.session_cap()` (`pool_cap.max(1)`), so
`Pool::new` loses its `data_cap` parameter. `Pool::new` acquires one permit
with `try_acquire_owned` (cap >= 1, fresh semaphore) and
`primed.driver_task().retain_until_exit(Box::new(permit))`.

**Invariant.** Every connecting, live or tailing session owns exactly one
session permit, from before its connector starts until its driver has exited;
every push session additionally owns one push slot over the same interval.
Neither resource is released before its transport is destroyed. The physical
cap is enforced by the semaphore alone; parked adoption only avoids dials.
Under the idle lock, `parked` and `waiters` are never both non-empty: a
member is parked only when nobody waits, and an acquisition registers as a
waiter only when nothing is parked or someone already waits.

**Returning a member** (`return_member`, called from `PooledConn::drop` after
today's reusability decision - Logout state or not reusable drops the member
and its permit follows its driver): under the idle lock, if `closed` drop the
member; else pop waiters oldest first and `send` the member to the first
whose receiver is still there; park it only when the queue is empty. No
await. `discard()` drops the member at once; its permit is already owned by
its driver, so nothing transfers.

A member handed to a waiter ignores that waiter's affinity: arrival order
outranks affinity at the cap, and a waiter handed a member selected elsewhere
re-SELECTs. Affinity still decides among parked members when nobody waits.

**Data acquisition** (`acquire_member(affinity)`, behind both checkouts):

```
let mut acquire = pin!(sessions.clone().acquire_owned());   // one queue position for the whole call
loop {
    lock idle; if closed -> Err(Closed)
    prune dead parked members (their permits follow their own drivers)
    if waiters.is_empty(): adopt affinity match, else any -> return member
    register a Waiter and build its WaiterGuard (same lock section)
    unlock
    select! { biased;
        member = &mut guard.rx => match member {
            Ok(m) if m.conn.is_alive() => return m,
            Ok(_dead) => continue,                  // re-register; the same window exists today between prune and adopt
            Err(_) => return Err(Closed),           // close dropped the sender
        },
        permit = &mut acquire => {
            permit closed -> Err(Closed)
            if closed -> Err(Closed)                // a grant just before close is not authorization
            lock idle; remove own entry; if rx.try_recv() is Ok(m) { unlock; drop(permit); return m }
            unlock; return dial(permit, Vec::new())
        }
    }
}
```

**Fresh acquisition** (`reserve_fresh(extra, retiring)`):

```
if closed -> Err(Closed)
let mut acquire = pin!(sessions.clone().acquire_owned());
// First poll: either a grant, or this call is now queued on the semaphore.
if let Ready(permit) = poll!(acquire) { closed checks; return reservation(permit, extra, waited = false) }
if let Some(own) = retiring { retire(own).await }
loop {
    if let Ready(permit) = poll!(acquire) { closed checks; return reservation(permit, extra, waited = true) }
    lock idle; if closed -> Err(Closed); prune dead
    if waiters.is_empty() and a member is parked: take it
    else: register a Waiter and build its WaiterGuard (same lock section)
    unlock
    if taken { retire(member).await; continue }
    select! { biased;
        permit = &mut acquire => {
            closed checks
            lock idle; remove own entry; if rx.try_recv() is Ok(m) { return_member(m) }   // a fresh session never adopts
            unlock; return reservation(permit, extra, waited = true)
        }
        member = &mut guard.rx => match member {
            Ok(m) => { retire(m).await; continue }
            Err(_) => return Err(Closed),
        },
    }
}

async fn retire(member: PoolMember) {
    let _ = tokio::time::timeout(command_timeout, member.conn.logout()).await;
    member.conn.terminate().await;   // abort + join: its driver has exited, its permit is back
}
```

The semaphore acquisition is created once and polled BEFORE any retire, so
this call is already queued when the retired driver exits. Tokio's semaphore
is fair: a released permit is assigned to the oldest queued acquisition even
if that future is not being polled at the moment, so the room a retire makes
goes to the acquisition that did the retiring, ahead of anyone who queued
during the retire. A replacement (`retiring = Some(old)`) therefore keeps the
room its own member frees against every acquisition that arrives after it.
It does NOT keep it against an acquisition already queued on the semaphore
when the replacement starts: that one is older, takes the freed permit and
dials, and the replacement waits for the next session to come back (handed
to it as the oldest waiter, retired, its permit then its own). That is
arrival order, but it is a change from today, where the replacement reuses
the checkout's shared permit and never waits. It costs latency on the
DELETE / RENAME fallback of a pre-UNSELECT server at the cap, never
progress: the replacement holds no session while it waits, and only
finitely many acquisitions can be queued ahead of it.

A retired member is out of `idle` and still registered, so a concurrent close
terminates it too; `retire` tolerates that (`terminate` takes the
`DriverTask` handle mutex, so a second concurrent terminate waits for the
first join). Cancellation mid-retire leaks nothing: the member owns its
permit through its own exit slot, and dropping it runs the driver's bounded
LOGOUT. `extra` is held by the `reserve_fresh` future and then by the
`FreshReservation` (no session exists yet; cancelling either releases it at
once) and moves into the dial task synchronously in `dial`, before any await.

**Dial** (`dial(self: &Arc<Self>, permit: OwnedSemaphorePermit, extra:
Vec<Box<dyn Send>>)`, behind `FreshReservation::dial` and data acquisition):
builds `LeaseHandoff::new([Box::new(permit)] ++ extra)` and a `ConnectRequest`
from it, then the owned dial task as today (read admission on `dials`, select
on `cancel_dials` / `tx.closed()` / `dial_admitted`), with two changes:

- The handoff clone is declared before the select and dropped explicitly
  after the select statement has dropped its futures and before the result
  is sent on `tx`, so before the driver spawns, the connector future and its
  transport are destroyed before the leases are released, and a caller that
  has received the dial's `Err` knows both have happened. After spawn, the leases are owned by the driver exit
  slot, so an abandoned receiver (before or after a successful send), a
  cancelled auth, or a close all release capacity only at driver exit.
- `dial_admitted` keeps today's order: closed check, connect, register
  (locked closed re-check), keepalive, authenticate, QRESYNC.

The `cfg(test)` seam becomes `dial_with(permit: OwnedSemaphorePermit, extra:
Vec<Box<dyn Send>>, connect: impl FnOnce(LeaseHandoff) -> F)`, routed through
the same owned dial task as production (callers that need no extra pass
`Vec::new()`); `dial_with_permit` is deleted (crate-private test seam,
not published surface). The existing dial-cancellation tests migrate as
follows, because the primed session now owns a permit:

- `close_cancels_a_dial_before_registration`,
  `close_cancels_authentication_already_on_the_wire` and
  `close_cancels_stalled_token_lookup_and_joins_its_driver` build their pool
  at `pool_cap = 2` and take the second permit with
  `pool.inner.sessions.clone().try_acquire_owned().unwrap()` before calling
  `dial_with`. The first one's connect closure ignores its handoff and keeps
  today's signal-then-pend future. The other two build their `late`
  connections INSIDE the connect closure, because the handoff only exists
  there: the closure's future awaits
  `driver_pair_leased(greeting, &handoff)`, sends the server end out on a
  oneshot the test holds, and returns `Ok(late)`, so the permit lands in the
  late driver's exit slot. Assertions unchanged, plus
  `available_sessions() == 2` once close has completed (close joined the
  primed driver and the late one, or the unspawned dial returned its permit;
  closing a semaphore refuses acquisition but keeps returned permits
  countable).
- `cancelled_checkouts_keep_capacity_until_the_dial_is_cancelled` builds its
  pool at `pool_cap = 2` (primed peer kept alive, so exactly one permit is
  spare). Each of the 16 iterations acquires that permit with
  `sessions.clone().acquire_owned()`, calls `dial_with(permit, ..)` with the
  pending connector future, polls once, waits for `entered`, drops the
  checkout future, asserts `available_sessions() == 0` with no executor
  yield, then acquires the permit again (which can only succeed once the
  dial task has dropped the cancelled connector future and its handoff),
  asserts `active == 0`, and drops it.

**Replacement.** `replace_selected_member`: take the old member, then
`self.member = Some(PoolMember { conn: self.pool.reserve_fresh(Vec::new(),
Some(old)).await?.dial().await?, selected: None })`. The old member is retired
(bounded LOGOUT, terminate) after the acquisition has queued and before the
dial, so the cap+1 and RFC 2683 2.2.2 reasons in the existing doc comment
still hold, and the room it frees comes back to this replacement unless an
older acquisition was already queued (see "Fresh acquisition"). The doc
comment's closing claim that teardown-first "costs one round trip" is
reworded to say the replacement can also wait at the cap. A fresh
dial is unselected by construction, so DELETE / RENAME safety holds; no
parked member is ever substituted. `deselect_target` is unchanged.

**Close.** Flag, `sessions.close()`, then under the idle lock clear
`waiters` (dropping every sender, so each waiter sees `Err(Closed)`);
`cancel_dials.cancel()`; owned drain task unchanged (registry drain, LOGOUT
under one `command_timeout`, join every driver). Registration re-checks
`closed` under the same lock, so a waiter is either cleared or never
registers.

**Fairness, stated at the code.** Both queues are FIFO: the waiter queue for
returned members, tokio's semaphore for released sessions. A new acquisition
never adopts while anyone waits, so a re-checkout loop cannot overtake a
queued waiter. Each acquisition holds one semaphore position for its whole
call. The remaining non-FIFO edge is between the two queues: a waiter granted
a permit and a member at once keeps the member and releases the permit to the
next semaphore waiter. No physical-cap property depends on fairness;
latency at the cap does.

### Capabilities (`capabilities.rs`, `factory.rs`)

`build_capabilities` gains `push_capacity: usize` and sets `push: InProcess`
only when the profile advertises IDLE AND `push_capacity > 0`. `open_using`
passes `cfg.push_session_capacity(profile.supports_notify())`; existing test
callers pass 1. The comment says why: advertising push the account will
refuse for every scope would keep bifrost-sync's push forwarder open and
every subscribe refused.

### Push (`push.rs`, `mod.rs`)

- `PushState { .., push_capacity: usize, push_slots: Arc<Semaphore> }`;
  `PushState::new(supports_notify, push_capacity)` with no `.max(1)`.
  `ImapAccount::new` passes `parts.config.push_session_capacity(parts.supports_notify)`.
  `#[cfg(test)] PushState::available_push_slots(&self) -> usize`.
- Admission: `admitted = push_capacity > 0 && (supports_notify ||
  covered.contains(..) || covered.len() < push_capacity)`. Refusal stays
  `idle_budget_error()`; its text becomes "IMAP push session budget
  exhausted within pool_cap; folder remains poll-only" (still contains
  "budget"). The NOTIFY accepted-residual comment stays.
- `ensure_idle_tasks`: workers = 1 with NOTIFY, else `push_capacity`.
  Reached only when something was accepted, hence capacity >= 1.
- Worker acquisition replaces `dial_idle`, split into a capacity phase and a
  dial phase:

  ```rust
  async fn reserve_push_session(account: &ImapAccount) -> Result<FreshReservation, crate::Error> {
      let slots = Arc::clone(&account.push.push_slots);
      let (slot, slot_waited) = match slots.clone().try_acquire_owned() {
          Ok(slot) => (slot, false),
          Err(TryAcquireError::Closed) => return Err(crate::Error::closed()),
          Err(TryAcquireError::NoPermits) => (slots.acquire_owned().await.map_err(|_| crate::Error::closed())?, true),
      };
      let reservation = account.pool.reserve_fresh(vec![Box::new(slot)]).await?;
      Ok(reservation.with_prior_wait(slot_waited))   // waited = slot_waited || pool waited
  }
  ```

  In `idle_loop`, after `choose_idle_folder`:
  - The pinned `reserve_push_session` future sits in a loop over a select
    with `cancel.cancelled()` (break), `account.shutdown.cancelled()` (break)
    and `resubscribe.changed()`. On a change the worker marks the generation
    seen and re-chooses: `Some(folder)` updates the folder and keeps polling
    the SAME pinned reservation (no session exists yet, so nothing is
    folder-specific); `None` drops the reservation (slot and any permit come
    back at once) and `continue 'worker`, which parks.
  - `reservation.dial()` is raced only with `cancel` and `shutdown`, never
    with `resubscribe`. A generation bump during connect / authenticate does
    not cancel the dial: today `dial_idle` is not interruptible either, and a
    cancelled dial would cost the provider a connect plus authentication
    while the worker immediately dials again, outside the failure backoff.
  - After the dial returns `Ok`, the worker marks the generation seen and
    re-chooses once more: `None` drops the session and `continue 'worker`;
    `Some(folder)` is the folder it SELECTs. Bumps after this mark latch for
    the first IDLE round exactly as today.
  - An `Err` from either phase takes today's `dial_idle` error arm unchanged.
    The connection is a plain `Arc<ImapConnection>`; every existing drop site
    retires it, and its session permit and push slot are released at its
    driver's exit. Push sessions are never parked.
- Coverage establishment: new `signal_coverage_established(push, selected,
  watched, registered)`, called on the outer (fresh-session) path after SELECT
  and registration handling whenever the worker is about to enter IDLE (that
  is, after `register_notify` returned `Ok(_)` or a retryable error mapped to
  `false`; a terminal registration failure retires the session and does not
  get here), and ONLY when the session's reservation `waited()`. It publishes
  `invalidated(HintPayload::SpecificCursorScope(folder_scope(selected)))`
  always, and one per other watched folder only when `registered` is true and
  the registration covered others. Per folder, never account-wide, so the
  existing reason for not emitting `Reconnected` on a first connect (no
  spurious account-wide reconcile) still holds. Gating on `waited` keeps the
  `register_notify` STATUS decision intact for the common case (capacity
  available at once, the window as short as today's dial); it fires where this
  work widened the window. Redials after a lost session already publish
  `Reconnected` or `signal_idle_interrupt_loss`. The Reidle branch does not
  call it. Two places where a wait, and so this invalidation, is routine,
  both accepted: a reassignment redial usually finds its push slot still
  held by its own previous session's LOGOUT tail, so it waits and publishes
  per-folder invalidations right after `signal_idle_interrupt_loss` already
  published an account-wide `Unknown` (redundant, harmless); and a first
  push session opened while discovery and inventory hold every data session
  waits behind them and publishes right after subscribe, which is the
  burst the STATUS decision avoids, but here the uncovered window really
  did stretch for the length of the wait.
- `register_notify`'s doc paragraph on leaving `status` off gains one
  sentence: a push session that waited for capacity publishes per-folder
  invalidations itself (`signal_coverage_established`), which the snapshot
  decision does not forbid because it fires only after a wait.
- `pool.rs` `dial_idle` is deleted.
- The `resubscribe_action` comment is rewritten: the pool connector seam now
  exists, so the re-IDLE wiring and the NOTIFY-collapse wiring are drivable
  hermetically but remain unpinned (filed, see the stopping rule).

### Configuration (`factory.rs`)

```rust
/// Maximum physical IMAP sessions this account holds at once, clamped to at
/// least one: pooled data sessions, push (IDLE) sessions, replacement dials,
/// and sessions still finishing their LOGOUT. Push capacity is carved out of
/// it and never takes the last session, so `pool_cap = 1` disables push.
/// SMTP submission, CardDAV/CalDAV and ManageSieve are separate protocols and
/// are not counted.
pub pool_cap: usize,
/// Ceiling on push sessions on a server without NOTIFY, clamped to at least
/// one, and further capped at `pool_cap - 1`. Drawn from `pool_cap`, not
/// added to it. A NOTIFY server uses one push session.
pub idle_connection_budget: usize,

pub(crate) fn session_cap(&self) -> usize;                                  // pool_cap.max(1)
pub(crate) fn push_session_capacity(&self, supports_notify: bool) -> usize; // as in Semantics
```

`data_connection_budget` is deleted. Default `idle_connection_budget: 2`.
`open_using` passes `production_connector()` to `Pool::new` and the push
capacity to `build_capabilities`.

## Landings

Two landings, each one coherent change, `brokkr check` green at both
boundaries. Landing 1 lays the instruments the budget's gates need; it changes
no admission behaviour.

### Landing 1: driver exit retention and the pool connector seam

1. `DriverExit`, `DriverScope`, `LeaseHandoff`, `DriverTask::spawn` /
   `absent` / `retain_until_exit` / `has_exited`; migrate the three spawn
   sites and two fakes; thread `&LeaseHandoff` through
   `connect_with_tls_connector_metered` / `connect_config_metered`,
   `driver_pair_with_capacity_and_budget` and their callers
   (`LeaseHandoff::none()` everywhere in this landing); add
   `driver_pair_leased`.
2. `ConnectRequest`, `Connector` (with its contract doc),
   `production_connector`; `PoolInner.connector`; `Pool::new` gains the
   `connector` parameter (factory passes production; every test constructor
   passes a test connector); `dial` builds its request through the connector.
3. `account/test_support.rs`: `ScriptedConnector` -
   `scripted_connector(greetings: Vec<Vec<u8>>) -> (Connector, ScriptedDials)`
   where each call pops the next greeting, builds `driver_pair_leased(greeting,
   &req.leases)`, sends the server end on an unbounded channel, and counts the
   call; with no greeting left it counts the call and returns
   `Err(Error::Internal("scripted connector has no session to dial"))`.
   `ScriptedDials { calls: Arc<AtomicUsize>, servers: UnboundedReceiver<DuplexStream> }`.
   `refusing_connector()` = `scripted_connector(Vec::new())`.
   `scripted_account_parts_with(conn, pool_cap, connector)` is the body of
   `scripted_account_parts`, which passes `refusing_connector()`. Test configs
   set `imap.keepalive = None` and `auth_policy =
   AuthPolicy::default().allow_cleartext_without_tls()`. The scripted dial
   transcript, used verbatim by every test below that completes a dial:
   - greeting `* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN SASL-IR] ready\r\n`;
   - the script reads the `AUTHENTICATE PLAIN` line and answers
     `<tag> OK [CAPABILITY IMAP4rev1 IDLE UNSELECT BINARY] done\r\n`. The
     inline tagged CAPABILITY is load-bearing: without it `complete_auth`
     issues a CAPABILITY command first. `IDLE` is required for the push tests
     (the encoder refuses IDLE without it), `UNSELECT` is dropped where a
     test names a server without it, and `BINARY` is the marker that
     identifies a dialed session (the primed session uses `ACL`, as the
     existing pool tests do);
   - a `SELECT` is answered `* FLAGS (\Seen)\r\n* 1 EXISTS\r\n* 0 RECENT\r\n*
     OK [UIDVALIDITY 1] valid\r\n<tag> OK [READ-WRITE] selected\r\n`;
   - an `IDLE` is answered `+ idling\r\n`;
   - a `LOGOUT` is answered `* BYE closing\r\n<tag> OK done\r\n`.
   Test-only accessors: `#[cfg(test)] Pool::available_sessions(&self) ->
   usize` and `#[cfg(test)] PushState::available_push_slots(&self) -> usize`
   (landing 2), so `scripted_tests.rs` can observe both semaphores without
   reaching into private fields; every test, including `pool.rs` ones, uses
   `available_sessions()` rather than reading the semaphore. Directly
   injected members (`pool.rs` `park`, and the direct idle insertion in
   `bandwidth_two_pool_members_share_one_inbound_rate`) must own a session
   permit like every other session: in landing 2 they take
   `pool.inner.sessions.clone().try_acquire_owned()` and hand it to
   `conn.driver_task().retain_until_exit(..)` before parking, and every
   fixture that parks an extra member runs at `pool_cap >= 2` (the existing
   ones already pass 2). The bandwidth test keeps `driver_pair_metered` and
   that `retain_until_exit` route.
4. Tests below. `pool_of` and `scripted_account_parts` use
   `refusing_connector()`, which also removes the stray real-DNS dials that
   today's push admission tests trigger through `test.invalid`.

Tests, landing 1 (`connection/tests.rs` unless noted; all hermetic):

- `a_value_retained_by_a_running_driver_is_released_only_at_driver_exit` -
  `driver_pair`, retain a `Box` whose `Drop` sets a flag; flag clear while the
  driver runs; drop the server end, `finish_or_abort(LOGOUT_DRAIN_TIMEOUT)`;
  flag set and `has_exited()`.
- `a_value_retained_after_driver_exit_is_released_at_once` -
  `DriverTask::absent()`.
- `aborting_a_driver_before_its_first_poll_releases_its_retained_values` -
  `DriverTask::spawn(std::future::pending(), .., vec![flag])`, `abort()` with
  no yield, then `abort_and_join`; flag set AND `has_exited()`.
- `the_driver_future_is_destroyed_before_its_retained_values` - a driver
  future owning a value that pushes "driver" into a shared `Vec` on drop, a
  retained value pushing "lease"; run once to completion and once aborted;
  order is `["driver", "lease"]` both times.
- `pool.rs` `a_checkout_dials_through_the_injected_connector` - pool built
  with a data cap of 2 (`Pool::new(.., connector, 2, ..)` in landing 1;
  landing 2 restates it as `pool_cap = 2`), primed checked out, second
  checkout dials: one connector call, the scripted AUTHENTICATE answered, the
  checkout's connection carries the scripted greeting's capability marker.

Gates, landing 1:

```
brokkr test -p bifrost-imap a_value_retained_by_a_running_driver_is_released_only_at_driver_exit
brokkr test -p bifrost-imap a_value_retained_after_driver_exit_is_released_at_once
brokkr test -p bifrost-imap aborting_a_driver_before_its_first_poll_releases_its_retained_values
brokkr test -p bifrost-imap the_driver_future_is_destroyed_before_its_retained_values
brokkr test -p bifrost-imap a_checkout_dials_through_the_injected_connector
brokkr check
```

Ablations (restore after each): (a) mark exit in `DriverScope::drop` BEFORE
dropping `inner`: the ordering test must fail; (b) keep the
`Arc<DriverExit>` holding the values on `DriverTask`, but call `mark_exited`
only from a guard constructed inside the spawned `async` body instead of from
`DriverScope`: an abort before the first poll never constructs the guard, so
the abort-before-first-poll test must fail on both the flag and
`has_exited()`; (c) have `retain` push even after exit: the absent test must
fail.

### Landing 2: the session budget

1. `PoolInner` rewrite per "Pool session accounting": `sessions` semaphore,
   `IdleState` with the waiter queue, `WaiterGuard`, registry field renamed
   `registry`, `acquire_member`, `reserve_fresh` / `FreshReservation`,
   `return_member`, `retire`, dial owning `permit + extra` through
   `LeaseHandoff`, `PooledConn` without a permit, primed permit, replacement
   through `reserve_fresh(.., Some(old))`, close clearing waiters, fairness
   comment, `Pool::reserve_fresh` / `acquire_fresh` / `available_sessions`.
   Delete `dial_idle`, `PoolInner::permit`, `dial_with_permit`.
2. `factory.rs`: field docs, `session_cap`, `push_session_capacity`, default
   `idle_connection_budget = 2`, delete `data_connection_budget`, pass the
   push capacity to `build_capabilities`.
3. `capabilities.rs`: the `push_capacity` parameter and its comment; test
   callers pass 1.
4. `push.rs` / `mod.rs`: `PushState` capacity and slots, admission,
   worker count, `reserve_push_session` and the two-phase acquisition in
   `idle_loop`, coverage establishment gated on `waited`, refusal text,
   `register_notify` doc sentence, comments that name
   `idle_connection_budget` as an additive budget (on `idle_budget_error`,
   `choose_idle_folder`, `subscribed_idle_folders`,
   `signal_notify_coverage_loss`) reworded to the carved-out capacity,
   `resubscribe_action` comment rewritten.
5. Test migrations: `pool_of(primed, cap)` and `scripted_account_parts(conn,
   cap)` and `guarded_stream_tests::pool_with` set `config.pool_cap = cap` and
   stop passing a cap to `Pool::new`; the docs of `scripted_account_parts`
   and `scripted_tests.rs` `scripted_account` say the pool holds `pool_cap`
   physical sessions, the primed one included. Tests that touch
   `pool.inner.idle` go through `.parked`. Every test that reads
   `pool.inner.sessions` as the session REGISTRY moves to
   `pool.inner.registry`, since `sessions` becomes the semaphore: among them
   the `park` helper, the `close_cancels_*` tests (registry lookups of the
   late connection and the empty-registry assertions) and the close tests
   that assert the registry is empty or count its entries after the drain.
   `dropping_a_checkout_releases_its_permit`
   is renamed `a_returned_checkout_is_available_to_the_next_checkout` with a
   doc about the member coming back (a lost member at `pool_cap == 1` is the
   permanent deadlock now); `a_zero_data_cap_is_clamped_to_one_permit` is
   renamed `a_zero_pool_cap_is_clamped_to_one_session` with its doc pointing
   at `session_cap()`. `close_gates_every_way_into_the_pool` replaces its
   `dial_idle` assertion with `pool.acquire_fresh(Vec::new())` returning
   `Err(Closed)`. The three push admission tests, the factory test and the
   deselect test change as listed below.
6. Documentation and notes, same landing (below).

Tests, landing 2 (`pool.rs` unless noted; `#[tokio::test(start_paused =
true)]` where a timeout or bounded LOGOUT is involved; every "stays pending"
assertion is a `futures::poll!` on a pinned future taken BEFORE any time
advance, never an await that would let paused time run out):

- `the_primed_session_owns_a_permit` - `pool_cap = 1`:
  `available_sessions() == 0` right after `Pool::new`.
- `a_discarded_session_keeps_its_permit_until_its_driver_exits` - `pool_cap =
  2`, primed plus one scripted dial checked out; permits 0; `discard()` the
  dialed one; permits still 0; answer its LOGOUT, join its driver; permits 1.
  Bites against "permit lives on the checkout".
- `a_checkout_at_the_cap_waits_for_a_member_parked_later` - `pool_cap = 1`:
  hold the primed checkout, pin a second `checkout_any`, poll: pending; drop
  the first; `tokio::time::timeout(Duration::from_secs(5), second)` completes
  with the primed marker; connector calls 0. Bites against waiting on the
  semaphore alone (no permit is ever released, so the timeout fires).
- `a_returned_member_goes_to_the_oldest_waiter_not_a_barging_checkout` -
  `pool_cap = 1`, one task: hold checkout L (primed); pin waiter W =
  `checkout_any()`, poll: pending; drop L and, with no yield, pin L2 =
  `checkout_any()` and poll it: pending; poll W: ready with the primed
  marker. Bites against "park, then wake a waiter": L2 adopts the member it
  just parked and is ready, W stays pending. The waiter is registered before
  the re-checkout, so single-task scheduling cannot reproduce the answer.
- `a_checkout_at_the_cap_dials_once_a_session_exits` - `pool_cap = 1`: hold
  the primed checkout, pin a second, poll: pending, calls 0; discard the
  first and drop its server end; the second dials (calls 1).
- `fresh_acquisition_retires_a_parked_member_to_make_room` - `pool_cap = 1`,
  primed parked: `acquire_fresh(vec![])` sends LOGOUT on the primed
  connection (script answers), terminates it, then dials (calls 1); the
  result carries the scripted marker and the primed connection is not alive.
- `fresh_acquisition_takes_a_member_returned_while_it_waits` - `pool_cap =
  1`: hold the primed checkout; pin F = `acquire_fresh(vec![])`, poll:
  pending; drop the checkout and pin D = `checkout_any()`, poll: pending;
  the script answers the LOGOUT F sends on the primed connection; F completes
  with the scripted marker (calls 1); D is still pending. Bites against a
  fresh acquisition that waits on the semaphore without registering as a
  waiter (the member parks and D adopts it).
- `the_room_a_retire_makes_returns_to_the_retiring_acquisition` - `pool_cap =
  1`, primed parked: pin F = `acquire_fresh(vec![])`, poll: F takes the
  primed member and its LOGOUT is on the wire; pin D = `checkout_any()`,
  poll: pending; answer the LOGOUT; F completes (calls 1), D still pending.
  Bites against creating the semaphore acquisition only after the retire (D
  is first in the semaphore queue, takes the freed permit and dials).
- `fresh_acquisition_never_adopts_a_parked_member` - `pool_cap = 2`, primed
  parked, one permit free: `acquire_fresh` dials (calls 1) and the primed
  member is still parked.
- `extra_values_are_released_only_at_the_fresh_session_driver_exit` -
  `pool_cap = 2`, primed parked: `acquire_fresh(vec![flag])`; drop the
  returned connection while the server end stays silent; flag still clear and
  `available_sessions() == 0`; drop the server end and let the driver finish;
  flag set and `available_sessions() == 1`. The connection reaches no
  checkout, which is the lease-equivalent of a receiver dropped after a
  successful send: past the spawn, ownership does not depend on who drops
  the handle. Bites against a dial task that keeps the permit in its own
  scope (today's `_permit`), which releases it when the task ends right after
  the send.
- `a_failed_fresh_dial_returns_its_permit_and_extras_at_once` - `pool_cap =
  2`, primed parked, `refusing_connector()`: `acquire_fresh(vec![flag])`
  returns `Err`; `available_sessions() == 1` and the flag is set.
- `a_cancelled_connector_is_destroyed_before_its_leases` - `pool_cap = 2`,
  spare permit taken explicitly; `dial_with(permit, vec![lease], ..)` where
  `lease` pushes "lease" into a shared `Vec` on drop, and the connect closure
  returns a hand-written pending future struct with two fields in this
  order: the `LeaseHandoff` it received, then a guard that pushes
  "transport" into the same `Vec` on drop. The pending future signals an
  `entered` oneshot on its first poll; wait for it, then `close()`, then
  await the dial future to its `Err(Closed)` before reading the `Vec` (close
  completing does not by itself prove the dial task has run, because a dial
  task still waiting for its read admission queues behind the drain's
  write guard; the dial's `Err` is sent only after both drops). Order is
  `["transport", "lease"]`. The struct's field order
  makes it bite: without the dial task's own handoff clone, the struct drops
  its `LeaseHandoff` first, which is then the last clone, and "lease" comes
  first.
- `replacement_dials_fresh_instead_of_adopting_a_parked_member` - `pool_cap =
  2`, server greeting without `UNSELECT`: checkout selected on `Archive`,
  second member parked with affinity `Archive`; `deselect_target(Archive)`
  retires the checkout's member (LOGOUT answered) and dials (calls 1); the
  parked member is still parked; the checkout's affinity is `None`.
- `scripted_tests.rs` `deselect_fallback_releases_the_old_connection_before_redialing`
  - rewritten on the connector seam, without closing the pool:
  `scripted_account_parts_with(conn, 1, scripted_connector([greeting]))`,
  primed without `UNSELECT`; SELECT `Archive` on the checkout; pin
  `deselect_target(Archive)` and poll it until the primed connection's
  LOGOUT has reached the script (the replacement's semaphore acquisition is
  queued by then); the script asserts connector calls `== 0` at that point;
  only now pin a second `checkout_any()` and poll it: pending; the script
  answers the LOGOUT, then serves the dial; the deselect future completes
  `Ok` within a 5 s `tokio::time::timeout` (so ablation (b4) fails instead
  of hanging), calls `== 1`, the checkout carries the dialed marker with affinity
  `None`, and the second checkout is still pending. Pins both the
  teardown-first order and that the replacement keeps the room its own
  member freed against an acquisition that arrived during the retire. The
  second checkout must NOT be queued before the deselect starts: an
  acquisition already queued is older than the replacement and takes the
  freed permit by design (see "Fresh acquisition"), so a test arranged that
  way hangs inside `deselect_target`.
- `close_releases_waiters_with_closed` - `pool_cap = 1`: hold the primed
  checkout, pin a second checkout and an `acquire_fresh`, poll both pending,
  `close()`; both return `Err(Closed)`; calls 0.
- `factory.rs` `push_capacity_is_carved_out_of_pool_cap` - table for
  `(pool_cap, idle_connection_budget) -> (non-NOTIFY, NOTIFY)`: defaults
  `(4, 2) -> (2, 1)`; `(4, 4) -> (3, 1)`; `(8, 4) -> (4, 1)`; `(2, 0) -> (1,
  1)`; `(1, 4) -> (0, 0)`; `(0, 4) -> (0, 0)`; plus `session_cap` for 0, 1, 4
  and the defaults `pool_cap == 4`, `idle_connection_budget == 2`.
- `capabilities.rs` `push_is_not_advertised_without_push_capacity` - an IDLE
  profile: `push_capacity = 0` gives `PushCapability::None`, 1 gives
  `InProcess`; a profile without IDLE gives `None` at 1.
- `scripted_tests.rs` `non_notify_push_reports_the_folder_beyond_its_budget_as_poll_only`
  - rewritten at `pool_cap = 4` with the default budget asserted as 2: items
  "0" and "1" succeed, "2" fails with text containing "budget", workers == 2.
- `scripted_tests.rs` `push_is_refused_entirely_when_pool_cap_leaves_no_room`
  - `pool_cap = 1`, once with `supports_notify = false` and once `true`: every
  scope in the failed lane, no handle, no worker started.
- `scripted_tests.rs` `a_notify_account_admits_every_folder_on_one_worker`
  - `pool_cap = 2`, `supports_notify = true`, five folders: all succeed, one
  worker.
- `a_folder_scope_with_an_unsendable_name_is_refused_not_misreported` and
  `a_composed_dav_collection_scope_is_refused_by_push_admission` move to
  `pool_cap = 2`; assertions unchanged.
- `scripted_tests.rs` `an_idle_worker_session_counts_against_pool_cap` -
  account with a scripted connector (one greeting), `pool_cap = 2`,
  non-NOTIFY, `idle_connection_budget = 1`; subscribe `INBOX`; the script
  answers the worker's AUTHENTICATE and `SELECT INBOX` per the transcript and
  reads `IDLE`, answering `+ idling`; then `account.pool.available_sessions()
  == 0` and `account.push.available_push_slots() == 0`; hold the primed
  checkout; a pinned second checkout polls pending and connector calls stay
  1. Bites against a worker that dials without a session permit, and (via
  the push-slot assertion) against a `reserve_push_session` that drops its
  slot instead of handing it to `reserve_fresh` - the latter is the
  regression that would let reassignment tails exceed push capacity. The
  test then drops the server end and closes the account.
- `scripted_tests.rs` `a_subscription_change_mid_dial_keeps_the_pending_session`
  - same account (one greeting, `pool_cap = 2`, budget 1). Leg 1: subscribe
  `INBOX`; the script reads the worker's AUTHENTICATE and holds it; subscribe
  `INBOX` again under a second handle (accepted, since an already covered
  folder costs no slot, so the generation bumps and the assignment does not
  move; a refused subscribe would also bump today, but the filed "bumps the
  generation when it accepted nothing" fix would remove that bump and leave
  this leg toothless); answer AUTHENTICATE; the script reads `SELECT INBOX` and
  `IDLE`; connector calls `== 1`. Leg 2 (fresh account): subscribe `INBOX`,
  hold AUTHENTICATE, unsubscribe `INBOX` and subscribe `Archive`, answer;
  the script reads `SELECT Archive`; calls `== 1`. Bites against racing the
  dial phase with `resubscribe.changed()` (the dial is cancelled and a
  second connector call is made, which the one-greeting connector refuses).
- `scripted_tests.rs` `a_push_session_that_waited_for_capacity_invalidates_the_folder_it_starts_covering`
  - scripted connector with two greetings, `pool_cap = 2`, budget 1,
  `push_stream` attached first. Check out the primed session and a second
  one (dial 1, answered), so both sessions are taken; subscribe `INBOX`
  (the worker waits); drop the dialed checkout (handed to the worker, which
  retires it: LOGOUT answered); the worker dials (dial 2), the script
  answers AUTHENTICATE and `SELECT INBOX` and reads `IDLE`; the stream's
  first event is `Invalidated { SpecificCursorScope(Folder("INBOX")) }`.
- `scripted_tests.rs` `a_push_session_dialed_at_once_publishes_no_coverage_invalidation`
  - one greeting, `pool_cap = 2`, budget 1, primed parked, `push_stream`
  attached first; subscribe `INBOX`; the script answers through `IDLE`, then
  drops the server end; the stream's first event is `Disconnected`, not an
  `Invalidated`.
- `push.rs` `coverage_establishment_names_each_covered_folder` - pure:
  `signal_coverage_established` with `registered = false` publishes only the
  selected folder; with `registered = true` and two other watched folders
  publishes three per-folder invalidations and never `Unknown`.

Gates, landing 2:

```
brokkr test -p bifrost-imap the_primed_session_owns_a_permit
brokkr test -p bifrost-imap a_discarded_session_keeps_its_permit_until_its_driver_exits
brokkr test -p bifrost-imap a_checkout_at_the_cap_waits_for_a_member_parked_later
brokkr test -p bifrost-imap a_returned_member_goes_to_the_oldest_waiter_not_a_barging_checkout
brokkr test -p bifrost-imap a_checkout_at_the_cap_dials_once_a_session_exits
brokkr test -p bifrost-imap fresh_acquisition_retires_a_parked_member_to_make_room
brokkr test -p bifrost-imap fresh_acquisition_takes_a_member_returned_while_it_waits
brokkr test -p bifrost-imap the_room_a_retire_makes_returns_to_the_retiring_acquisition
brokkr test -p bifrost-imap fresh_acquisition_never_adopts_a_parked_member
brokkr test -p bifrost-imap extra_values_are_released_only_at_the_fresh_session_driver_exit
brokkr test -p bifrost-imap a_failed_fresh_dial_returns_its_permit_and_extras_at_once
brokkr test -p bifrost-imap a_cancelled_connector_is_destroyed_before_its_leases
brokkr test -p bifrost-imap replacement_dials_fresh_instead_of_adopting_a_parked_member
brokkr test -p bifrost-imap deselect_fallback_releases_the_old_connection_before_redialing
brokkr test -p bifrost-imap close_releases_waiters_with_closed
brokkr test -p bifrost-imap close_gates_every_way_into_the_pool
brokkr test -p bifrost-imap a_returned_checkout_is_available_to_the_next_checkout
brokkr test -p bifrost-imap a_zero_pool_cap_is_clamped_to_one_session
brokkr test -p bifrost-imap push_capacity_is_carved_out_of_pool_cap
brokkr test -p bifrost-imap push_is_not_advertised_without_push_capacity
brokkr test -p bifrost-imap non_notify_push_reports_the_folder_beyond_its_budget_as_poll_only
brokkr test -p bifrost-imap push_is_refused_entirely_when_pool_cap_leaves_no_room
brokkr test -p bifrost-imap a_notify_account_admits_every_folder_on_one_worker
brokkr test -p bifrost-imap a_folder_scope_with_an_unsendable_name_is_refused_not_misreported
brokkr test -p bifrost-imap a_composed_dav_collection_scope_is_refused_by_push_admission
brokkr test -p bifrost-imap an_idle_worker_session_counts_against_pool_cap
brokkr test -p bifrost-imap a_subscription_change_mid_dial_keeps_the_pending_session
brokkr test -p bifrost-imap a_push_session_that_waited_for_capacity_invalidates_the_folder_it_starts_covering
brokkr test -p bifrost-imap a_push_session_dialed_at_once_publishes_no_coverage_invalidation
brokkr test -p bifrost-imap coverage_establishment_names_each_covered_folder
brokkr test -p bifrost-imap container_create_completes_under_a_single_permit_pool
brokkr test -p bifrost-imap dropping_a_guarded_stream_aborts_its_task_and_returns_the_connection
brokkr test -p bifrost-imap close_cancels_a_dial_before_registration
brokkr test -p bifrost-imap close_cancels_authentication_already_on_the_wire
brokkr test -p bifrost-imap close_cancels_stalled_token_lookup_and_joins_its_driver
brokkr test -p bifrost-imap cancelled_checkouts_keep_capacity_until_the_dial_is_cancelled
brokkr test -p bifrost-imap bandwidth_two_pool_members_share_one_inbound_rate
brokkr check
```

Check each name filter matched the intended test before believing a PASS.
Ablations (restore after each): (a) keep the permit on `PooledConn` and
release it at checkout drop: `a_discarded_session_keeps_its_permit_until_its_driver_exits`
must fail; (b) make `return_member` always park, never hand off:
`a_checkout_at_the_cap_waits_for_a_member_parked_later` must fail on its
5 s timeout; (b2) replace the hand-off with "park, then wake the oldest
waiter": `a_returned_member_goes_to_the_oldest_waiter_not_a_barging_checkout`
must fail; (b3) make `reserve_fresh` wait on the semaphore without
registering as a waiter: `fresh_acquisition_takes_a_member_returned_while_it_waits`
must fail; (b4) create `reserve_fresh`'s semaphore acquisition only after a
retire: `the_room_a_retire_makes_returns_to_the_retiring_acquisition` and the
rewritten deselect test must fail; (b5) keep the permit in the dial task's
own scope instead of the handoff: `extra_values_are_released_only_at_the_fresh_session_driver_exit`
must fail on `available_sessions`; (b6) drop the dial task's handoff clone
before the select: `a_cancelled_connector_is_destroyed_before_its_leases`
must fail; (c) let the worker dial without a session permit:
`an_idle_worker_session_counts_against_pool_cap` must fail on
`available_sessions`; (c2) pass `Vec::new()` instead of the push slot to
`reserve_fresh`: the same test must fail on `available_push_slots`; (c3)
race `reservation.dial()` with `resubscribe.changed()`:
`a_subscription_change_mid_dial_keeps_the_pending_session` must fail; (d)
make `reserve_fresh` adopt a parked member:
`fresh_acquisition_never_adopts_a_parked_member` must fail; (e) drop the
`push_capacity > 0` term from admission:
`push_is_refused_entirely_when_pool_cap_leaves_no_room` must fail on its
NOTIFY leg; (e2) ignore `push_capacity` in `build_capabilities`:
`push_is_not_advertised_without_push_capacity` must fail; (f) remove the
`signal_coverage_established` call:
`a_push_session_that_waited_for_capacity_invalidates_the_folder_it_starts_covering`
must fail; (f2) drop the `waited` gate:
`a_push_session_dialed_at_once_publishes_no_coverage_invalidation` must fail.

### Documentation and notes, in landing 2

- `reference/imap.md` "Configuration and connect": `pool_cap` is the
  physical-session cap, `idle_connection_budget` is carved out of it, push
  capacity formula, `pool_cap = 1` disables push. The sentence
  "`ImapAccountConfig::idle_connection_budget` is the visible non-NOTIFY
  push-session budget and defaults to 4" is rewritten without a hard-coded
  default count, pointing at the field doc.
- `reference/imap.md` `pool.rs` bullet: one session permit per physical
  session from connector start to driver exit; data checkouts adopt parked
  members or dial; returned members are handed to the oldest waiter and
  parked only when nobody waits; fresh acquisition for push and replacement
  never adopts, queues on the semaphore before retiring, and retires a parked
  member to make room; the connector seam and its contract; the latency costs
  listed under "Semantics".
- `reference/imap.md` "Push lifecycle": replace the "data and push budgets are
  separate ... seven total" text with the carved-out capacity (defaults: two
  push sessions without NOTIFY, one with, inside four), worker count, push
  slots held through driver exit, fresh-only push sessions and why (mailbox-
  less unsolicited events from a previous selection would be absorbed into
  the wrong folder's cache), the two-phase worker acquisition (a subscription
  change re-chooses during the capacity wait and never cancels a dial), the
  coverage-establishment invalidation after a wait, the advertised push
  capability following push capacity, the push latency costs, and the
  fairness sentence. Rewrite the dial-admission paragraph ("Data dials share
  ownership of the checkout permit ... Dedicated IDLE dials keep their
  separate push budget") to the lease-in-driver-exit rule, and the
  linearization paragraph's "`checkout_*` re-checks the flag after acquiring
  its permit" to cover both acquisitions and the waiter queue. The NOTIFY
  paragraph's "no initial STATUS snapshot" stays and gains the after-a-wait
  exception. Name the driver exit slot in the connection section where
  `DriverTask` joining is described.
- `notes/bugs-imap.md`: delete the IMAP-018 entry.
- `notes/todo.md` "IMAP unbounded LOGOUT outside pool drain": drop the
  `replace_selected_member` half (now bounded by `retire`), keep the push
  cancellation half.
- `notes/todo.md` "IMAP abandoned checkout cascade": append that under the
  physical cap, "obtaining another checkout" must discard the abandoned one
  first (a second checkout taken while holding it is a nested checkout that
  waits forever at `pool_cap = 1` or once data capacity is exhausted), and
  the discarded session's permit returns only at its driver's exit.
- `notes/todo.md`, new items (lateral findings, filed not built):
  - "IMAP push: same-session NOTIFY extension is uncovered until
    registration" - in the Reidle branch a newly added folder is watched only
    after the re-issued `NOTIFY SET`; changes in it before that are not
    invalidated. Pre-existing, unchanged in size by this work.
  - "IMAP push: pin the re-IDLE and NOTIFY-collapse wiring through the pool
    connector seam" - the seam the `resubscribe_action` comment asked for now
    exists.
  - "Session cap does not span account incarnations" - a failed or cancelled
    factory open, or a closing incarnation during reopen, can leave a tail the
    next incarnation's cap does not count.
  - "IMAP push: false NOTIFY warning on non-NOTIFY accounts" -
    `register_notify` logs "server does not advertise NOTIFY ... the other
    subscribed folders will not be pushed" on every fresh session and every
    Reidle round of a non-NOTIFY multi-folder account, where every other
    folder has its own worker; `signal_notify_coverage_loss` already reasons
    why `false` is not a gap there.
  - "IMAP push: `push_subscribe` bumps the generation when it accepted
    nothing" - wakes every worker for no change; gate the bump on a non-empty
    accepted set.
  - "IMAP DELETE / RENAME while a parked member has the target selected" -
    `deselect_target` handles only the checkout's own member; another parked
    member can stay SELECTed on the target while the command goes out
    (RFC 2683 2.2.2). Pre-existing.

## Stopping rule

In scope: everything above. Out of scope, named:

- IDLE/data multiplexing on one connection (borrowing an IDLE session for a
  data command). Not needed for the cap; `pool_cap = 1` simply disables push.
- Adopting parked data sessions into push, or parking push sessions. Rejected
  for correctness (event contamination), not deferred.
- Cross-incarnation accounting (filed).
- The push shutdown/cancel path's unbounded `logout()` (existing todo item;
  pool close terminates it, and under the push slot it delays only that slot).
- The same-session NOTIFY extension gap and the re-IDLE / NOTIFY-collapse
  wiring pins (filed).
- The false NOTIFY warning, the unconditional generation bump, and parked
  members selected on a DELETE / RENAME target (filed).
- Session limits of SMTP, DAV and ManageSieve.
- Any change to `bifrost-types`, `bifrost-sync` or another crate. The
  `push_subscribe` contract is unchanged; only which scopes are refused moves.

Published surface: `ImapAccountConfig::pool_cap` and
`ImapAccountConfig::idle_connection_budget` keep their names and types; their
meaning changes and the `idle_connection_budget` default drops from 4 to 2
(see the adjudication in "Configuration"). Downstream consumers that sized
`pool_cap` as a data-only budget now get a smaller total footprint, which is
the point of the item. At `pool_cap = 1`, every push scope lands in the failed
lane (bifrost-sync logs those at warn), and the account advertises
`PushCapability::None`; this is an accepted consequence of reserving the last
session for data. Nothing published is removed or renamed.

## Review record

Sparred with a fresh codex session (read-only profile) before writing. Points
conceded and folded in: per-checkout permits leave pruned, discarded,
abandoned-dial and replacement tails uncounted (now per-session leases owned
by the driver exit slot); waiting on the semaphore alone deadlocks once
permits sit in parked members (now returned members reach waiters directly);
worker count does not bound push sessions with tails (now push slots held
through driver exit); adopting a data session into push misattributes
old-mailbox events into the push folder's MODSEQ cache (now fresh-only push);
replacement through ordinary acquisition could adopt a member selected on the
DELETE / RENAME target (now fresh-only replacement); coverage invalidation
must also fire when a retryable registration error leaves the session in
selected-only IDLE; `TryAcquireError::Closed` is distinct from `NoPermits`; a
permit granted just before close is re-checked.

A second, cold codex session reviewed the written document. Folded in: the
scripted AUTHENTICATE reply must carry an inline tagged CAPABILITY (else
`complete_auth` issues CAPABILITY first) and must advertise IDLE (the encoder
refuses IDLE otherwise); the SELECT reply bytes are pinned; the existing
dial-cancellation tests need `pool_cap = 2` and an explicit permit because the
primed session now owns one; directly injected members must own a permit;
the parking test's permit-before-park claim was void under per-session
leases and was removed; the push-slot handoff got its own assertion and
ablation; test-only semaphore accessors replace private-field access from
`scripted_tests.rs`.

Two further cold reviews (r1, r2) were validated against the tree at
`5d24c7a8` and consolidated. Every finding was valid at least in part; all
were folded in:

- Parking without a permit lost today's FIFO order at the cap, letting a
  per-folder mutation loop re-adopt its own member ahead of every waiter (r1).
  Now: hand-off to the oldest waiter, one semaphore position per acquisition,
  and two tests that pin overtakes with non-uniform arrival. Validating it
  turned up the same hole in the replacement path (the old member's freed
  permit would go to the oldest semaphore waiter, stalling the DELETE /
  RENAME fallback that today keeps its permit), narrowed by queueing on the
  semaphore before retiring and pinned in the rewritten deselect test. It is
  closed against acquisitions that arrive during the retire, not against
  ones already queued before the replacement started; those are older and
  take the permit by design, which is stated as a cost.
- Racing the worker's acquisition against `resubscribe.changed()` cancelled
  dials in flight on every generation bump, including bumps from subscribes
  that accepted nothing (r1, r2). Now: a capacity phase that re-chooses on a
  change and a dial phase that is never raced with it, with a test that
  bumps mid-authentication.
- `avoid_retiring` preferred the wrong member and nothing tested it (r1).
  Resolved by deleting the parameter rather than flipping it: once the
  replacement queues before retiring its own member, the room comes back to
  it and a parked member is retired only in a narrow race with an earlier
  fresh acquisition, which leaves a preference nothing could pin. The
  underlying hazard (a parked member selected on the target) is pre-existing
  and filed.
- The coverage invalidation contradicted the `register_notify` STATUS
  decision and overstated its motivation, since polling covers the window
  (r1). Now gated on the reservation having waited, argued as latency, with
  a negative test; the `register_notify` doc and reference sentence are in the
  documentation list.
- `pool_cap = 1` kept advertising push (r1). `build_capabilities` now takes
  the push capacity.
- Landing 1 ablation (b) would not have bitten (r1); rewritten, and the test
  asserts `has_exited()`.
- Missing tests: fresh acquisition waiting for a returned member, release on a
  failed dial (r1); the pre-spawn transport-before-lease order and capacity
  retained past a successful send (r2). Added, each with an ablation.
- The deselect ordering test was vacuous because close's drain sent the LOGOUT
  (r1); rewritten on the connector seam.
- Stale test names, docs and the `close_gates_every_way_into_the_pool`
  replacement (r1); listed in the migrations and gates.
- API placement and naming slips: `reserve_fresh` on `PoolInner`,
  `driver_pair_with_capacity_and_budget`, landing-1 data-cap phrasing, one
  accessor name, how the `close_cancels_*` tests now build `late` (r1).
- Unstated latency costs and the data-uses-all-sessions change (r1); in
  "Semantics" and the reference list.
- The connector contract (r1); on the `Connector` doc.
- Reliance on tokio `Notified` drop-forwarding (r1). Superseded: the
  `Notify` is gone, and the equivalent load-bearing step - a cancelled
  waiter's guard re-routing a member already handed to it - is stated on
  `WaiterGuard`.
- The survey claimed a ruling for meaning and default changes that the
  recorded shape ruling does not cover (r1). Resolved by the option-1
  adjudication in "Configuration": neither change triggers the removal or
  renaming rule, and the default of 2 preserves room for two data sessions.
- r1's lateral findings are filed in `notes/todo.md`, not built.

Not taken as proposed, with reasons:

- r1's ticketing alternative for fairness: a "waiters ahead of me" count does
  not deliver the member to the waiter, so a woken waiter still races the
  next arrival; the hand-off queue does.
- r1's "publish only for the selected folder" alternative for the coverage
  invalidation: after a real wait, a NOTIFY session's other folders were
  uncovered just as long, so they are invalidated too; gating on the wait
  answers the STATUS concern instead.
- r2's literal "drop the receiver after the send without consuming it": past
  the spawn the leases live in the driver exit slot whoever drops the
  connection, and the seam offers no deterministic point between the send
  and the receive. The test drops the received connection unconsumed by any
  checkout instead, which exercises the same ownership and bites against the
  same regression.
- r1's last stale-docs bullet (`dial_idle` and field docs saying `pool_cap`
  is not a shared cap): no separate action; both are deleted or rewritten by
  the spec already.
- r1's "assert `available_push_slots()` after a failed dial": the failed-dial
  test runs at pool level, where the push slot is just an `extra` value, so
  a drop-flagged extra stands in for it; that push hands its slot to the
  pool as an extra at all is pinned separately by
  `an_idle_worker_session_counts_against_pool_cap` and ablation (c2).

A re-validation pass of the r1 / r2 fold against the tree at `5d24c7a8`
corrected the fold itself:

- The rewritten deselect test queued the second checkout BEFORE the
  replacement started. Under the design that checkout is older, takes the
  permit the retired member frees, and the single-task test would hang in
  `deselect_target`. The test now queues it during the retire, and
  "Fresh acquisition", "Replacement" and the costs list say plainly that
  an already queued acquisition wins (the fold had claimed the replacement
  keeps its room "as it does today").
- `a_cancelled_connector_is_destroyed_before_its_leases` read the order
  right after `close()`, which does not prove the dial task ran; it now
  waits for the connector's first poll and reads the order after the dial's
  `Err`, and the dial design says the handoff drops before the result is
  sent.
- Leg 1 of the mid-dial resubscribe test relied on a refused subscribe
  bumping the generation, which a filed lateral fix removes; it now bumps
  with an accepted same-folder subscribe.
- The registry rename breaks every test that reads `pool.inner.sessions` as
  the registry; added to the migrations.
- `close_cancels_a_dial_before_registration` has no `late` connection; the
  migration now says so.
- The FIFO motivation cited `run_mutation_groups`, which does not exist; the
  loop is `flush_mutation_groups` (inherited from r1).
- The coverage invalidation now states the two places a wait is routine
  (reassignment redials behind their own tail, and a first session opened
  while inventory holds every data session).
