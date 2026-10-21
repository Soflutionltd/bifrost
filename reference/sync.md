# bifrost-sync reference

Current architecture of the sync engine.

Scope: scheduler, multiplexer, backfill orchestrator, push
reconciler, mutation pipeline, checkpoint envelope versioning,
observability. Depends only on `bifrost-types` (plus
`bifrost-net` for the bandwidth meter); opaque to every protocol
crate. Consumers wire one `Arc<dyn AccountFactory>` per account.

## Architecture

```
SyncEngine
  AccountSlot per attached account:
    current: ArcSwap<Arc<dyn Account>>
    control: SyncControl
    workers: Vec<WorkerTask {join, abort}>
    Multiplexer task        -> per-scope poll + lifecycle + reopen
    Backfill orchestrator   -> partition runner per scope
                               (parks on the account's backfill bound)
    Push reconciler task    -> reads per-account mpsc<WatchEvent>
    Push forwarder task     -> Account::push_stream -> per-account mpsc
    Ack writer task         -> AckRequest -> CheckpointStore
    Control applier task    -> priority / bandwidth-cap watchers
    Bandwidth feed (optional) -> BandwidthMeter -> control atomic
    Reopen listener         -> ReopenRequest -> handle_recovery
    Mutation campaigns      -> bulk_set_flags retry loop
  Shared:
    Scheduler (four-lane: Foreground / Normal / Background / Bulk)
    BudgetGate (global + per-account semaphores)
    CursorRegistry (scope -> cursor; membership index)
    LaneGate (per account; the backfill bound's wait plus its admission)
    CheckpointStore (consumer-provided; InMemoryCheckpointStore for tests)
    InvalidationSinkInner (per-account ingress senders)
    SubscriptionRegistry (per-engine push subscription handles)
```

`slot.current` is `ArcSwap<Arc<dyn Account>>` so reopens are
visible to spawned tasks immediately: every iteration calls
`current.load_full()`. No `RwLock` on the hot path.

## Construction

`SyncEngineBuilder` is the entry point. Methods:

- `budget(ConcurrencyBudget)` - sets `EngineConfig::budget`.
- `config(EngineConfig)` - replaces the whole config.
- `checkpoints(Arc<DynCheckpointStore>)` - consumer-provided
  store; defaults to `InMemoryCheckpointStore` when omitted.
- `with_bandwidth_registry(Arc<bifrost_net::BandwidthRegistry>)` on the
  builder or engine configures slot-owned payload budgets. Configure it before
  attaching accounts, using ONE registry across every Net and the IMAP factory.
  Each account has independent inbound and outbound budgets shared across its
  protocol legs. Without a registry, cap forwarding behaves as before. The
  payload smoothing bound is defined in `reference/net.md`.
- `with_bandwidth_meter(Arc<bifrost_net::BandwidthMeter>)` -
  wires the meter; when unset, `Control::bandwidth_observed`
  returns 0 for every account.
- `build()` - validates the budget, constructs the scheduler
  with the configured lane capacity, returns `SyncEngine`.

`SyncEngine::builder()` is the canonical constructor; no public
`SyncEngine::new()`.

## Lifecycle

```rust
let control = engine.attach(account_id, factory).await?;
// ... engine drives the account ...
engine.detach(account_id).await?;
engine.shutdown().await?; // explicit cleanup; preferred over Drop
```

`attach`:
1. Take the per-account lifecycle guard
   (`AsyncMutex<HashSet<AccountId>>` on `engine.lifecycle_inflight`)
   and reject duplicate / racing attaches with
   `Error::AccountAlreadyAttached`. The guard is released on both
   success and failure paths so a failed `attach_inner` does not
   strand the slot. `detach` holds the same guard for its whole
   teardown (see below), so an attach racing a detach of the same id is
   refused rather than admitted into that window.
2. When a bandwidth registry is configured, acquire a strong account budget
   BEFORE opening, and set its cap to `None`, matching the initial control
   channel. A failed open releases this owner by RAII. The budget moves into
   `AccountSlot` on success and stays alive across replacement opens until
   detach releases the slot. The ControlApplier writes cap changes directly to
   this budget as well as forwarding to the current Account, so changes during
   a slow replacement open take effect immediately. Replacement opens reuse
   the same budget and its current cap and schedules. Applying the cap to a
   replacement account holds the control snapshot through its setter, so a
   concurrent control change cannot be overwritten by a saved cap.
   `factory.open(account_id).await` -> `OpenedAccount { account,
   skipped_scopes }`. The engine threads its own `AccountId` through
   so the protocol crate can register against `bifrost-net` /
   `MeterSink` / trace correlation under the same key the engine knows
   the account by. The skip lane - parts of the surface the protocol
   crate discovered but could not bring up (an unreachable foreign
   JMAP account, a failed composed-DAV open), each with its classified
   error - is warn-logged and stored on the slot;
   `SyncEngine::open_skipped_scopes(account)` exposes the current
   lane. `Err(_)` still fails the attach as `OpenFailed`: the contract
   reserves it for the primary surface being unavailable, precisely
   because this path has no retry budget (only the reopen path does).

   This open-time lane is the only skip lane the engine STORES. For the
   page-level lanes see "Page loss lanes" below.
3. Read `capabilities()` (snapshotted on the slot).
4. `discover_cursor_scopes()` -> for each, `establish_initial_cursor(scope)`:
   - `Ready(cursor)`: persist, start `changes_stream` immediately.
   - `EstablishViaInventory`: defer to backfill; the inventory
     pass's terminal `Done` carries the cursor.
   - Error whose derived `RecoveryClass` is
     `Engine(DisableScope(_))` (`scope_local_establish_failure`,
     pure): SKIP the scope, log, keep attaching. The protocol crate
     has already classified the failure as independently
     quarantinable (a revoked shared mailbox, an unreadable public
     folder), and the running path honors that via `disable_scope`;
     this is the same rule at attach. Previously every establish
     error propagated, so one dead share failed the whole account
     and the consumer got no sync at all, primary mail included.
     Any other error still fails the attach.
5. `discover_memberships()` -> populate `CursorRegistry`
   membership index for push-hint routing. `scope_covers_membership`
   in `engine/attach.rs` is the engine-policy mapping (account-wide
   cursors cover every membership; folder-typed cursors cover the
   matching folder; query cursors cover the matching query).
6. Spawn workers: ack writer, control applier, push reconciler,
   push forwarder, multiplexer,
   backfill orchestrator, deferred-inventory worker (if any),
   reopen listener, bandwidth feed (if a meter is wired). Store
   all `WorkerTask`s. The push forwarder parks while
   `PushCapability::None` and wakes on the reopen-generation signal,
   so a capability change does not require spawning a new task.
7. Return `SyncControl`.

Any error after `factory.open` and before slot installation closes the
opened account best-effort before returning, so failed discovery or
establishment cannot strand protocol workers or connections.

`detach` sends `Stop` on the boundary channel, cancels the
shutdown token, calls `Account::close()`, awaits all workers
with `EngineConfig::detach_timeout` (default 5s) and aborts
stragglers, removes the slot, unregisters the sink and ack
sender.

Worker awaits are two-phase and ORDERED: every stream worker first, the ack
writer last. Each stream worker holds a clone of the writer's request sender,
so the writer's channel only closes - and the writer only drains and persists
what it already received - once they are all gone; waiting on the writer first
burns the whole `detach_timeout` and then aborts it with unpersisted work. The
writer is identified by `WorkerRole::AckWriter` through `take_ack_writer`, not
by its spawn position. It IS spawned first, and the predecessor read
`drained[0]` on that basis - an unannounced coupling between spawn order and
teardown order that any reordering of the spawn block would have broken
silently.

Each teardown PHASE carries its own fresh `detach_timeout`; they do not share one
deadline. There are four: the worker awaits, then the writer phase (the teardown
discard drain and the ack writer's final drain, which run while the writer is the
last worker alive), then the wait for the slot's reopen lock, then
`Account::close()`. So the worst-case `detach` is `4 * detach_timeout` - 20s at
the default - and only when a wedged worker, a checkpoint store that stops
answering, a reattach wedged inside a provider call, and a provider close that
never returns all coincide. A consumer calling `detach` on the way out of a
process is bounding its own shutdown by that number.

The reopen-lock wait is what makes detach and a consumer-driven `reattach`
exclusive. A reattach is not a worker, so nothing else orders the two: the
writer phase waits for it only incidentally (an in-flight reattach holds a clone
of the writer's sender, so the writer cannot drain until it returns) and only
for that one budget. A reattach outlasting it used to race the close: detach
closed the old account while the reattach swapped in a replacement nobody would
ever close, and the reattach then closed the old account a second time. Reattach
holds the lock from its open through the swap and the close of what it replaced,
so detach takes it around its load and close: an in-flight reattach finishes
first and detach then closes its replacement, and one that has not opened yet
waits, sees the cancelled shutdown token, and opens nothing. On a lock-wait
timeout the close proceeds without the lock, with a warning, so the race reopens
only for a reattach that has overrun both the writer phase and the lock wait.

The per-phase budget is not a nicety. A shared deadline silently zeroes whichever
phase runs last, so a provider stream that neither yields nor ends - on any
worker poll without a shutdown arm, such as the deferred-inventory worker's
membership refresh - burnt the whole budget in the worker phase and left the writer to be aborted IMMEDIATELY, with `remaining == 0` and no
drain at all. That is exactly the "writer aborted with unpersisted work" outcome
the two-phase ordering and `take_ack_writer` exist to prevent, arrived at by a
different route. Pinned by
`tests/detach_close_clamp.rs::a_straggler_worker_does_not_cost_the_ack_writer_its_drain`.

A writer that exhausts its OWN budget is answered explicitly rather than falling
through the generic straggler path (`await_ack_writer_until`, not
`await_worker_until`). It is either blocked in the `CheckpointStore` or still
waiting on its request channel: every ENGINE-side sender is gone by that point,
but `BackfillCheckpointWriter` is published and owns a `WriterRequest` sender,
so a consumer that retains one across the detach keeps the channel open and the
writer can exhaust the budget with a perfectly responsive store. The diagnostic
must not name the store as the culprit. Abort is still the only
available outcome (the account is gone; no caller can reach the writer for a
retry; waiting longer only lengthens the bound above), but it gets its own log
line, because the acknowledged work it was carrying is precisely what the drain
exists to persist. The consequence is redelivery, not loss: an acknowledgement is
durable only once the writer says so, so the account resumes from an older
checkpoint on the next attach.

`Account::close()` is clamped too, and every await in the teardown now is. Its
budget is a FRESH `detach_timeout` rather than the remainder of the worker
deadline: detach has always been "worker awaits
PLUS the trailing phases", and sharing the deadline would take the close budget away
whenever a straggler had already spent it, reporting a healthy close that needs
one round trip as hung. A TIMED-OUT close is treated exactly as a FAILED one -
warn, drop the handle, carry on to the registry cleanup, report success to the
caller - because no other outcome is available to it: the slot left
`engine.accounts` at the top, so nothing can reach the handle for a retry, and
retaining it would keep a dead incarnation's connection reachable under an id a
fresh `attach` may already have claimed. The two get distinct log lines, since
"the provider refused" and "the provider never answered" want different
follow-up. Pinned by `tests/detach_close_clamp.rs`, whose four tests separate a
clamped hang from a refusal by elapsed budget and by whether the close future
ever completed, and pin both trailing phases against a straggler worker.

The whole teardown runs under the same `lifecycle_inflight` guard
`attach` takes, claimed together with the slot removal under one lock
acquisition. The slot leaves `engine.accounts` at the top while the
registry cleanup (invalidation sink, budget gate, backfill registry,
throttle memberships, bandwidth meter) happens at the very bottom,
after up to four `detach_timeout` budgets of worker awaits, writer drain, the
reopen-lock wait and `Account::close()`.
Without the guard an attach landing in that window saw no slot, no
in-flight entry, succeeded, and then had its brand-new registrations
unregistered by the detach's tail - leaving an account that reported
itself attached while silently dropping every out-of-process push
(`push` returns early on a missing sender), with nothing logged. A
racing attach now gets `AccountAlreadyAttached`, which is literally
true: the incarnation is attached and draining. A racing detach, and an
attach still in flight, both yield `AccountNotAttached`. Pinned by
`tests/attach_schema_recovery.rs::attach_cannot_land_inside_an_in_flight_detach`.

`shutdown(self)` cancels the engine-root token FIRST, then enumerates attached
accounts and calls `detach` for each (logging but not failing on per-account
errors). Strongly preferred over relying on `Drop`, which can only fire a
best-effort sync cancel.

Cancel-first is load-bearing. Every slot's shutdown token is a child of the root
(`engine/attach.rs`), so the cancel trips the same token `detach_inner` cancels
per account, only earlier and for all accounts at once. Cancelling AFTER the
sequential loop - which is what it did - left the last of `N` accounts fully
operational (polling, publishing, writing) for up to
`(N - 1)` worst-case detaches after the consumer called `shutdown`: a caller
that believed it had stopped the engine had only started a queue whose tail was
still working.

What cancel-first claims is narrow: **cancellation is REQUESTED for every
account before cleanup begins.** It does not establish quiescence and does not
mean publication has stopped.

**Respond promptly**: the control applier, the bandwidth feed, the push
forwarder, the multiplexer main loop, the push reconciler, and backfill and
deferred inventory while in their subscriber / pause / throttle / capacity /
admission waits.

**Stop at the next poll boundary, but on different signals.**
`BackfillRunner::run_partition` selects the account shutdown token at its
`inventory_partition_stream` poll (through `LaneGate::shutdown`) and returns
`Error::ShuttingDown`, which withholds both the partition outcome and the scope
completion marker. `InventoryFusion::run_stream` does the same with its own
`shutdown` field at its provider poll - the slot's shutdown token for the
deferred-inventory worker, the recovery context's for re-establishment - and
also returns `Error::ShuttingDown` rather than `NoCursor`, which callers would
read as a finished walk. `drive_changes_stream` selects the account BOUNDARY at its
`changes_stream` poll - `Stop` and `Pause`, never `CheckpointNow`, which is the
one request that genuinely needs a checkpoint-bearing item to satisfy - so it
stops when that account's own `detach` publishes `Stop`, which for a later
account in the shutdown loop is some time after root cancellation. The per-scope
poll task and the push reconciler additionally select the shutdown token around
the whole drive, and the poll task selects its per-scope token too, which is
what releases the drive LEASE when a scope is deleted or restarted while the
account boundary is still `Run`.

In every case cancellation stops the loop at a poll boundary. It never
interrupts the processing or publication of an event the stream has already
RETURNED TO THE LOOP: the registration and the broadcast are synchronous and a
`select!` cannot split them. It can, however, discard an item that became ready
in the same poll as the cancellation - the changes drive's select is unbiased,
so either branch may win, and the runner's is `biased` toward cancellation on
purpose. So the honest guarantee is not "declines to read another event" but
this: no publication is ever partial, and no cursor advances past an item the
consumer did not receive, so a discarded item is re-read by the next drive.

**Do not**: the deferred-inventory worker's membership refresh after a
successful establishment (`link_discovered_memberships`) does not select on
cancellation while reading `discover_memberships`, so a wedged provider stream
there keeps the worker alive until teardown aborts it at the deadline. The
detach teardown tests use exactly that as their straggler. The push reconciler holds no per-scope token, so a
scope deleted while one of its drives is stalled waits for the boundary rather
than for the deletion.

Because the engine now drops pending provider streams as a matter of routine -
on every pause, not only at teardown - `Account::changes_stream` requires its
streams to tolerate that and leave the account usable, and states so.

**The reopen listener is both**, and which one depends on what it is doing when
the token fires. Idle, its select covers the wait for the next request and it
exits at once. Inside `handle_account_error` it does not: the recovery backoffs
and the replacement-open boundaries consult the token, but the provider calls,
writer requests, scope re-establishment and membership discovery it awaits do
not, so a wedged provider operation keeps that worker alive until its own
account reaches its worker deadline.

One asymmetry a CONSUMER can observe, and the reason this enumeration is a
contract rather than a note: a bulk mutation campaign selects on the same token
and records `Error::ShuttingDown` in its returned counters when it fires - even for an account
whose detach is several budgets away - while in that same window the account is
still discoverable, still hands out change receivers, and still accepts
acknowledgements through its writer. Shutdown therefore closes the MUTATION
surface first and the read surfaces last, per account, for as long as that
account waits its turn.

A strict publication cutoff is a separate, deliberate change, and would have to
be named as one.

Detach stays SEQUENTIAL. Cancel-first already lets the cooperative workers
across all accounts wind down concurrently; sequential cleanup then limits the
overlap of discard processing and provider closes without changing failure
attribution, and a dying process is the wrong place to introduce fan-out. The
conservative total is therefore still `4 * N * detach_timeout` plus overhead -
typical joins get faster, but an unresponsive provider operation or a slow store
still justifies the bound.

The reattach path (driven internally by `EngineDirective::RestartAccount`
through the reopen lane, and public as `SyncEngine::reattach`) is a
staged reattach. It opens a replacement,
reapplies priority and bandwidth, rediscovers cursor scopes and
memberships into a temporary registry, establishes newly-appeared
scopes, removes vanished cursors, recreates registered push
subscriptions whose requested scopes still exist, refreshes the
capability snapshot, and then swaps the
handle and registry topology. On a successful swap the replacement
open's `skipped_scopes` replace the slot's stored lane, so a healed
namespace disappears from `open_skipped_scopes` and a still-degraded
one reappears with a fresh classification. The public entry is what a consumer pairs
with `capabilities().discovers_foreign_namespaces_on_rediscovery`: when that
flag is true, a share granted after the last open surfaces only through
this rediscovery, and the scheduling cadence (how often the reattach's
wire cost is worth paying) is consumer policy - the engine does not
schedule speculative reattaches on its own.

That flag is advisory TO THE CONSUMER, not a promise the engine keeps.
Nothing in `bifrost-sync` reads it: it is not an input to any engine
decision, and no internal caller of `SyncEngine::reattach` exists at all.
It reports rediscovery POTENTIAL (IMAP derives it from NAMESPACE, JMAP is
constitutively true), and a consumer that wants shares granted after open
to appear must read it and drive `SyncEngine::reattach` on its own clock.
The engine will not grow a rediscovery timer: the right interval depends
on things it cannot see - whether the app is foregrounded, whether the
connection is metered, whether shares are common in the deployment - and
a reattach is a full staged operation with real wire cost, so an interval on
the consumer's side is both cheaper and better informed. An
`EngineConfig` interval was rejected (default-off would go unused,
default-on would be wrong for most deployments); an
`accounts_awaiting_rediscovery()` accessor that exposes the candidate set
without owning the clock is the alternative to revisit first if consumer
footwork turns out to be the problem. A generation watch wakes
the push and lifecycle readers even when their old streams never end.

Cursor and membership topology live under one registry state lock, so
reattach replaces both atomically. The same replacement increments a
registry generation while holding that lock. Every live change drive
captures the generation with its scope lease and conditionally publishes
the batch and installs its cursor together under the state lock. The swap
also overlays the final live cursor snapshot onto every retained scope.
A batch from an old handle that finishes after cutover is therefore fenced
out before broadcast or durable ack, instead of rolling either cursor copy
backward. This fencing avoids waiting for an unbounded old stream during
a reattach.

## Page loss lanes

`Page<T>` carries two lanes beside `items`: `failed_ids` (resources the
provider fetched but could not materialize - an unparseable vCard, a
resource refused inside a 207) and `skipped_scopes` (scopes a multi-scope
walk quarantined rather than visited, each with its classified error).
Absence of results from a skipped scope is missing data, not evidence of
absence.

`Page` appears only on on-demand QUERY surfaces (`search`,
`search_messages`, `contacts_list`, `contacts_search`, `directory_*`, the
calendar list/range walks), never in the background sync pipeline, which
runs on streams and `SyncEvent`. The engine forwards four of them
(`contacts_list`, `directory_search`, `directory_groups_list`,
`directory_group_expand`) and does not expose the rest at all, so in every
case the consumer physically receives both lanes in the returned `Page`.
That copy is the actionable one: it names the scopes and carries their
errors.

What the engine adds is an announcement, not a record.
`announce_page_loss` emits a `SyncEvent::Warning`
(`OperatorAttentionNeeded`, with a `next_action` pointing at the lanes) on
the account's normal change stream when a forwarded page comes back with
either lane non-empty. The message carries COUNTS only - `failed_ids`
holds native provider identifiers, which are not user-safe text. A page
with both lanes empty emits nothing.

This is deliberately an event rather than engine state, and the reasoning
is worth keeping: a page lane is true of one walk at one moment. Unlike
the open-time lane there is no later point at which it can be said to have
healed, so accumulating it would need an invented expiry, an invented
dedupe key, and a cap. Worse, an accessor would be systematically
incomplete - the query surfaces the engine does not expose would never
contribute - so it would report "no skips" while a direct `Account` call
had just quarantined three scopes. A warning stream makes no completeness
claim; a queryable lane named after the data would.
If every requested scope of a registered push subscription vanished,
the record is dropped rather than widened to all discovered scopes; its
old handle is explicitly unsubscribed before the swap. All old-handle
push teardowns must succeed before the replacement is installed, so an
account that retains failed server-side DELETE state (Graph) remains
live and retryable instead of being closed with an orphaned subscription.
The abort is kept on purpose: it buys one retry against the still-installed
old account, which is the only place a provider's retained delete state
lives. It is made cheap instead. The teardown pass never stops at a
refusal; it tries every old handle and flags every refusing one
(`teardown_unconfirmed`), so the retry meets no first-time failure and a
reopen costs at most one aborted attempt however many subscriptions the
consumer holds (the account-wide budget is three attempts).
A handle whose teardown SUCCEEDED before the pass aborted is recorded at
once as `torn_down` (a wanted record) or removed (a pure orphan). A
`torn_down` record is still `desired`, so the retry recreates it, but it is
never handed to `push_unsubscribe` again, and `SubscriptionRegistry::take`
(consumer `unsubscribe_push`, detach) drops it rather than tearing it down.
`Account::close()` is called best-effort after the swap. Any failure
before the swap closes the replacement and leaves the running handle
installed - and any subscription already created on that replacement is
torn down first, with every handle whose delete did not succeed kept in
the registry as an orphan (`teardown_unconfirmed`, not `desired`).
`close()` does not delete server-side subscriptions, so a handle the
engine forgets is an orphan that keeps delivering until the provider
expires it; an orphan is therefore never recreated against a replacement,
is carried across swaps, and is retried by the next reopen,
`unsubscribe_push`, or the account's orphan-teardown timer
(`PushConfig::orphan_teardown_retry_interval`).
Two independent flags sit on each registry record, and they answer
different questions. `teardown_unconfirmed` says a server-side teardown
of the handle was attempted and failed, so it may still be live and its
teardown is retried. `desired` says the consumer still wants the
coverage, so a reopen recreates it on the replacement. A record is always
`desired`, `teardown_unconfirmed`, or both, and reopen recreation is
driven by `desired` alone. The two were once one flag, and a reopen
aborted by a failed old-side teardown then marked the consumer's live
subscription as an orphan: the retry carried the old handle but never
recreated it, and the subscription silently vanished. Such an aborted
reopen (`mark_unconfirmed`) now leaves `desired` alone, since the old
account stays installed and the coverage is still wanted. Only
`into_orphan` clears `desired`, and it is applied to a consumer
unsubscribe whose teardown failed, to a replacement-side handle unwound
by an aborted reopen (the consumer's desire still lives in the record it
was recreated from), and to an old handle carried past a committed reopen
(its desire now lives in the recreated record; carrying it as desired too
would subscribe twice on the next reopen).
A repeat failure on an already-unconfirmed record is logged rather than
aborting the swap, because a handle belonging to a long-dead connection
must not wedge every future reopen.

A reattach is cancellation-safe end to end. `reattach_account` is reachable
through a public future (`SyncEngine::reattach`, which a consumer may time out)
and through the reopen listener (aborted at the detach deadline), so it can be
dropped at any await, and what it owns has to be cleaned up by something a drop
still runs. That something is a `ReattachGuard`, armed at CONSTRUCTION: it is
built inside `open_replacement` in the same synchronous step that receives the
replacement from the factory, and travels with it (`Replacement`) into
`reattach_account`, so there is no interval, including one before the
function's first poll, in which a live replacement is owned only by a local. It
tracks four debts, each cleared by the step that settles it and only after that
step's await has answered:

- Push handles the replacement created (`live`). Each handle
  `push_subscribe` returns goes straight into the guard. The cutover `disarm`s
  them into the registry with no await between the two; an ordinary abort
  empties them through `unwind_replacement_subscriptions` (which removes a
  record only after its delete answered, restoring a refusal as an orphan in
  the same synchronous step).
- The close owed to an account. Until the swap that is the replacement; the
  swap (`swapped`, with no await after it) hands the guard the previous account
  instead, so a drop between the swap and `previous.close()` closes the old
  connection rather than leaking it. The close is awaited through the guard and
  cleared only once it returns, so a drop parked inside a close owes it again.
- The rollback of provisional cursor rows (`rollback_owed`), armed
  synchronously before the first fresh-scope establishment (whose resume read
  may claim a row) and cleared once
  `reattach_abort` answered.
- The commit of those rows (`commit_owed`), which the swap converts the
  rollback into: past the swap an abort would delete cursors belonging to a
  reattach that succeeded, so a drop there must promote them, not roll them back.
  Cleared once the commit is enqueued. That conversion is not observable from a
  test: once a commit has landed the writer's abort finds nothing provisional
  and is a no-op, so only a drop between the swap and the commit reaching the
  queue depends on it, and the only await in that span is the commit's own
  channel send.

Provisional state is attempt-scoped. Each guard mints a unique attempt id from a
process-wide monotonic counter; `ReattachInsert`, `ReattachAbort` and
`ReattachCommit` all carry it, and the writer keeps a scope-to-attempt map. An
abort deletes, and a commit promotes, only rows the named attempt owns; a later
attempt inserting the same scope takes ownership. An acknowledged checkpoint
still discharges a scope's provisional state whichever attempt owns it.

A reattach that RESUMES a stored row (`run_establish` reading it through the
writer's `GetChangeCursor`) passes its attempt as `claim_for`; if the row is
still provisional the writer moves ownership to the resuming attempt, in queue
order with inserts, aborts and commits. Without that, a dropped attempt's late
abort deleted a row the installed topology depends on. A committed row has no
provisional entry and is untouched, and reads outside a reattach (scope
restart, and attach, which reads the store directly) never claim. The
inventory fusion does not read stored cursors through the writer. Because the
claim can land inside `run_establish`, the guard arms `rollback_owed` before
each fresh-scope establishment rather than only before the first insert; an
abort with nothing owned is a no-op.

On a drop the guard enqueues the writer's `ReattachAbort` or `ReattachCommit`
synchronously with `try_send`. That is deliberate: the request lands in the
writer's queue ahead of anything a later reattach could insert, and the writer's
abort handles only rows still provisional. A full channel falls back to a
spawned send, which has no ordering against the next reattach; that is safe
because the attempt id stops the late request touching a newer attempt's rows.
Then it spawns one
task that deletes any remaining handles on the replacement, registers refusals
as orphans, and closes whatever account is owed (a second close after a
cancelled first one, since the cancelled call proved nothing). The deletes
and the close run without the slot's reopen lock, so a concurrent reattach does
not wait on network calls; only the registry write takes it, because a reattach
commits with `replace` built from its own snapshot, so an unlocked restore
landing between that snapshot and the commit would be erased and the provider
subscription never retried. Under the lock the restore lands before the
snapshot (carried like any orphan) or after the commit (`restore_orphans`
merges).
With no runtime to spawn on the handles are registered as undeleted orphans
without the lock (Drop cannot await, the dropping reattach usually still holds
it, and with no runtime nothing can be running a concurrent reattach) and the
close cannot run.

Every registry write made off the detach path is sealed the same way: orphan
restores (`restore_orphans`), a consumer's `subscribe_push` registration
(`record`) and the reattach commit's install (`replace`) each take the slot's
shutdown token and read it INSIDE the registry's own shard lock, the same lock
`take` runs under. Detach
cancels the token before it takes the registry, so a restore is either ordered
before the take (and discarded by it) or after it (and sees the cancelled token
and refuses). A check made before the write, as the cleanup task once did,
leaves a window on a multi-thread runtime: the token is read, detach cancels
and takes, and the orphan then lands under an id a later attach of the same
`AccountId` inherits. Detach does not take the reopen lock to close that window,
deliberately: consumer-driven reattaches hold it across `factory.open()` and
network calls, detach does not wait for them, and a lock acquisition there would
make the one call a consumer has for getting rid of an account wait on a wedged
open. The same sealed write is used by the ordinary abort's unwind and by
`unsubscribe_push`.

A refused `record` or `replace` names subscriptions that are LIVE on the
provider, so refusal is followed by teardown, not a drop. Detach's "leave
subscriptions alive" policy is a consumer's deliberate keep of what it knowingly
subscribed; neither of these is that. `subscribe_push` deletes the handle it just
created (on its own spawned task, awaited, so a caller dropping the future while
the delete is parked cannot strand it) and returns
`Error::AccountNotAttached`, since the consumer is told the call failed and will
never hold the handle; it also checks the token after taking the reopen lock, so
a detach that won the queue creates nothing. A refused reattach install hands its
records back: the replacement's `desired` handles go into the `ReattachGuard`
synchronously, and are deleted after every step that publishes the cutover, so a
drop anywhere still deletes them. The carried orphans, which name the previous
connection's handles, are dropped as detach drops stranded records. The
reattach still returns `Ok`: the swap committed, and reporting failure would send
the caller's retry budget after a change that cannot be undone. Pinned by
`a_subscription_registered_after_detachs_take_is_torn_down_not_inherited` and
`a_reattach_commit_after_detachs_take_tears_down_what_it_could_not_register`.

Orphans are retried against whichever account is current at that time, not the
replacement that created them, which is why the cleanup task tries the
replacement itself first. That is an accepted limit and it is benign in this
workspace: no provider rejects a handle it does not know (IMAP and JMAP answer
`Ok` without touching the wire; Graph deletes the ids the handle reaches,
counting already-gone ones as done; Google re-derives the mailbox's watch
state), so a retry on a later connection clears the orphan rather than failing
forever on an unknown handle. It does not always delete anything: IMAP and JMAP keep the handle's
server state in the account instance, so a retry that reaches a newer
connection is a no-op and the provider-side subscription then lives until it
expires. Graph's webhook handles carry their subscription ids, and its factory
shares the ids its renewal worker recreated, so a Graph retry on a newer
connection does delete (and the owning account's `close()`, which the guard
runs, deletes its webhook rows too).
Known residue: a drop parked inside `push_subscribe` itself may leave a
provider-side subscription whose handle the engine never received (the account
implementation's cancellation contract).

Pinned by `reopen_drop_points.rs` (a drop before any replacement subscription
existed closes the replacement, a drop rolls back the rows it inserted, a drop
inside the final close of the old account re-issues that close and leaves the
committed rows alone, and a committed reattach owes nothing) and
`detach_orphan_race.rs` (an orphan restored after detach's take is not
inherited by the next attach), with the registry-level check itself pinned in
`push/subscription.rs`. No drop point exists between `open_replacement`
returning and `reattach_account`'s first poll today (the two are one
expression), so that interval is covered structurally, by building the guard
inside the former, rather than by a test that could not reach it.

Accepted gap: a handle whose old-side teardown succeeded before a later
refusal aborted the pass delivers nothing until the retry recreates it on the
replacement (its record stays `torn_down` and `desired`). Resubscribing on
the old account to cover the span was rejected: the abort usually means the
old connection is failing, and a success would mint a handle the retry must
tear down again.

Cursor establishment for newly discovered scopes is staged in memory. After
replacement subscription creation, reattach durably persists only the cursors
it freshly created - a scope whose row already existed in the store (persisted
by a prior session and rediscovered by the replacement) is resumed from,
never rewritten. An aborted swap (store failure or old-handle teardown
failure) compensates by deleting exactly those freshly created rows, so a
replacement that is later closed cannot leave durable cursor state for a
topology the engine never installed, and the abort path can never destroy or
overwrite a preexisting row. Vanished-scope rows are deleted only after the
cutover is committed: the old account's ack writer persists checkpoints
concurrently with a reopen (nothing serializes it against one), so a
pre-cutover delete would need a snapshot-and-restore that races that writer
and could overwrite a newer acknowledged checkpoint. A failed post-cutover
delete, or a retired stream's late ack re-persisting one, leaks a stale row
that a later rediscovery validly resumes from - a leak, never data loss.
Compensation failure is error-logged because the checkpoint-store trait has
no transaction primitive.

The rule that makes the above safe: **never write what an abort would have to
restore**, and derive the "preexisting versus freshly created" classification
from the SAME store read the action uses. A separate pre-check `get` whose
errors were swallowed (`.is_ok_and`) demoted a preexisting prior-session row to
"created" on a transient read failure, and the abort path then deleted it - the
exact data loss the staged design existed to eliminate, resurfacing through the
error path of a duplicated read, plus a TOCTOU between the two reads.
`run_establish` now reports the origin off its own single read
(`EstablishOrigin`), so a store error aborts the reattach before any durable
write. Pinned by
`transient_get_failure_cannot_demote_preexisting_cursor_to_created`. Generally:
a classification that feeds a destructive compensation must come from the read
the action uses, and must fail closed on error.

**One writer per account owns every durable mutation.** Reattach does NOT
touch `CheckpointStore` directly: it persists freshly created cursors and rolls
them back through the same `WriterRequest` channel the ack writer serves.
Direct writes raced acknowledged ones, and not hypothetically - `run_establish`
hands `changes_tx` to `InventoryFusion`, so a replacement's inventory pass
broadcasts checkpoint-bearing batches BEFORE the cutover, a consumer can
acknowledge one, and the abort path then deleted the row that acknowledgement
had just committed.

Serializing is necessary but not sufficient on its own: a FIFO queue would
order the acknowledged write ahead of an unconditional delete and faithfully
destroy it. The writer therefore tracks which scopes are still PROVISIONAL -
inserted by an uncommitted reattach and not since claimed by an
acknowledgement - and an abort deletes exactly those. That makes the rollback a
conditional delete implemented in the engine, where there is one
implementation, rather than a compare-and-swap every `CheckpointStore` backend
would have to get right. `commit_reattach_inserts` clears the marks past the
last abort point, so a later reattach's abort cannot delete rows belonging to
one that succeeded; it runs after the generation bump because the lifecycle
reader is waiting on that watch and no await belongs between the topology swap
and the bump that publishes it.

A compare-and-swap primitive on `CheckpointStore` would remove the class
outright. It was considered and deliberately not taken - it is a published trait
change, so it is an owner-level proposal rather than hardening work.

The same boundary covers recovery. `RecoveryContext` holds a `WriterHandle`,
not `Arc<DynCheckpointStore>`. Scope restart, disable, schema reset, established
cursor persistence, vanished-scope cleanup, and reattach compensation are all
writer requests. A reset first retires the scope's live publications, folds
their degraded debt into the writer-owned ledger, persists that ledger, and
then deletes the rows.

Retiring the publications and extracting their debt is ONE operation under the
ledger lock, taken BEFORE the first store await. Snapshotting the debt and
retiring afterwards leaves an await window in which a still-draining scope
registers a further publication: the later retirement drops it, its debt was
never in the snapshot the writer persisted, and the coverage obligation is lost
silently. The reset therefore runs the same operation a SECOND time once its
deletes have landed, folding in whatever was published during them.

Each pass also raises a per-scope acknowledgement fence to the current
publication mint counter. Any publication at or below the fence names a batch
published against rows the reset has since deleted, so acknowledging it would
recreate the very cursor the reset dropped to force re-establishment; the claim
lookup refuses it before it can reach a live claim. The fence is an exclusive
bound against a monotonic counter, so ids minted after the reset closed - which
is to say by the re-establish that follows it - are above it and acknowledge
normally. There is no unfencing step to forget.

`RestartScope` and `DisableScope` preserve backfill state; only schema recovery
deletes it. The `WriterHandle` names that choice per caller
(`reset_scope_for_restart`, `reset_scope_for_disable`,
`reset_scope_for_schema_recovery`) rather than passing a bare boolean, because
getting it wrong is silent: deleting the completion marker on ordinary cursor
invalidation turns every recovery into a full historical inventory and
hydration walk.

Schema recovery currently enumerates only registered cursors. A scope still
establishing, or absent after exhausting its establishment budget, can retain
durable rows and completion markers. The store contract has no account-wide
row reset or durable scope enumeration; resetting registered scopes therefore
does not guarantee an account-wide schema migration. Closing this gap requires
an account-wide store operation routed through the checkpoint writer, including
its publication retirement and acknowledgement fences.

`CheckpointStore` remains a published consumer-implemented trait; ownership is
enforced by which internal engine type can reach its `Arc`, not by hiding the
trait. `BackfillCheckpointWriter` is also published, but its `store` field is an
account-owned `BackfillCheckpointTarget`, not a store `Arc`.
`SyncEngine::backfill_checkpoint_writer` constructs it from the attached
account's writer channel, and `persist` sends `WriterRequest::PersistBackfill`.
The writer applies that checkpoint beside its authoritative in-memory ledger,
so the helper cannot perform a read-modify-write outside writer ordering.

The target is a shape change, not a removal: `BackfillCheckpointTarget::direct`
takes exactly the `Arc<DynCheckpointStore>` the field used to be, so code that
built the writer by hand still can, and `BackfillCheckpointWriter::new` gives it
a constructor. That route performs the read-modify-write it always did and is
documented as carrying the race; the attached route is the one without it.

A `SyncEngine::reattach` call or an engine-initiated reopen queues behind `Pause` and runs after
resume: pause is a quiescence boundary, not permission to open a
replacement connection in the background. The activity registration that
makes the account non-quiescent is taken BEFORE `factory.open()`, not
after it, so `pause` cannot report quiescence while a replacement
connection is being established; a pause that wins the race against the
registration means nothing was opened at all, and the caller loops back
to the boundary wait without spending a retry attempt.

## Stream contract: broadcast + consumer ack

### Object changes and inventory removals

`ObjectChangeKind::Updated` is an upsert. An unknown id is a create;
`Created` is an optional precision hint, and consumers must tolerate either
kind for an existing or unknown id. The engine forwards live object kinds
unchanged and keeps no object-existence index. Consumers hydrate and insert
or refresh the object before acknowledging the batch.

`InventoryBatch::removed` carries removals observed while walking inventory.
Both `InventoryFusion` and `BackfillRunner` put them in the SAME synthetic
change batch as the page entries and accepted checkpoint. Folder and
folder-type scopes emit `ScopeChange::Removed` for that folder; query scopes
remove query membership; account and type scopes emit `ObjectChange::Destroyed`.
A scope removal does not assert global destruction. Consumers apply the whole
batch, including removals, and its checkpoint in one transaction before acking.
A barrier page still delivers removals, but carries no accepted checkpoint.
Graph fills this lane from delta tombstones; other providers leave it empty.
If a nonempty removal list arrives for a cursor scope without a defined
projection, the walk emits a scoped `Internal(AccountContract)` termination
before forwarding the batch or its checkpoint. Neither global destruction nor
dropping the removal is a safe guess.

### Batch boundary validation

Before anything else, `drive_changes_stream` and `InventoryFusion::run_stream`
call `validate_boundary` on every batch the account yields. `Batch`'s fields
are public, so `PageBoundary::Partial` carrying a checkpoint is constructible
and no type can stop a protocol crate from emitting it.

A violation is NOT a bare `Error::Other`.
`recovery::batch_boundary_violation` classifies it as
`Internal(AccountContract)`, which derives to terminal `InternalFailure`. The
boundary is authored by the `Account` implementation, so the failure is
attributed to client code rather than the provider. The driver publishes
`SyncEvent::Terminated` to subscribers and returns `ChangesEvent::Terminated` /
`FusionOutcome::Terminated`, so `plan_recovery` stops the scope. An
implementation emitting a nonsense boundary does not heal by being asked
again.

More generally, `handle_drive_outcome` normalizes every `Error::Account` onto
the same `plan_recovery` path as `ChangesEvent::Terminated`; the generic
log-and-repoll arm is reserved for engine-internal errors with no account
recovery classification. Cursor-envelope rejection publishes `Terminated`
before returning its classified account error, so an engine directive is
dispatched and subscribers are not left with a silently stalled scope.

The protocol tag on that error is read off the offending checkpoint's change
cursor. There is no protocol accessor on `dyn Account`, so a checkpoint that
carries no tag yields `Protocol::Unknown` rather than a guess.

**Both inventory front ends and the live driver refuse a BACKFILL checkpoint
outright**, and the reason is about the bound rather than about envelopes. Both
register the publication and then send on the RAW sender, so an entry created
there is never stamped with a delivery, and an unsent stamp is deliberately
never swept. A `Lane::Backfill` entry minted that way would sit charged against
the account's backfill bound until an acknowledgement, a lag or a reset freed
it: a permanent slot of the bound spent per scope. No provider in this workspace
attaches one, so refusing costs nothing real. Fusion fails the inventory pass,
which is retried; the live driver terminates the scope as a contract violation
with its own message, so the consumer is told rather than the driver re-polling
the same cursor for ever. Pinned by
`a_fusion_batch_may_not_carry_a_backfill_checkpoint` and
`the_live_driver_refuses_a_backfill_checkpoint`.

### Broadcast

`drive_changes_stream` broadcasts each `Batch` (item plus
optional `Checkpoint`) onto the per-account broadcast channel and
advances the in-memory `CursorRegistry` immediately so the next
poll iteration starts from the freshly-yielded cursor. It does
NOT write to `CheckpointStore`, and it holds no handle on the writer
channel - the driver has no way to persist even if a future edit wanted
it to, which is what makes the consumer-ack rule structural rather than
conventional. Durable persistence is consumer-ack-driven:

Polling and push reconciliation enter the same `CursorRegistry::with_drive`
scope for each `CursorScope`. The combinator snapshots the cursor and registry
generation under the lease, holds it through exactly one stream drive, and
releases it before recovery handoff, retry delay, channel backpressure, or poll
cadence sleep. This makes lane + scope a single-producer channel without
letting an idle poll delay a push invalidation:
the next producer starts from the cursor installed by the previous one,
and checkpoint supersession cannot retire another producer's unacked
batch.

The narrowed extent is a contract, not an implementation detail: **only the
drive is serialized.** Anything that reads a drive's effect must read it inside
the combinator, because the other producer may start on the same scope the
instant the callback returns - the poll loop measures whether its own drive
advanced the cursor there, for exactly that reason, since measuring it after
the fact would credit a push reconcile's progress to the poll and pin the
cadence at `poll_min`. What protects durable state is not the lease but the
generation fence: `publish_if_drive_generation` refuses a publication whose
generation the registry has moved past, and a deleted scope makes the next
`with_drive` return `None` instead of driving an unowned cursor.

The fence is a PAIR, `DriveGeneration { registry, scope }`. `registry` moves
only when reattach replaces topology wholesale; `scope` is a per-scope counter
that `delete` bumps. Splitting them is load-bearing in both directions: a
single account-wide counter bumped on delete fenced every SIBLING scope's
in-flight drive, which then discarded valid results and repeated its walk from
its old cursor, while a registry-only fence could not see a delete at all.
`publish_if_generation` (account-wide only) stays published for callers that
hold just that value.

Deleting a scope does NOT remove its drive lease. The lease entry outlives the
scope so a re-established incarnation waits for the previous incarnation's
drive to finish: dropping the entry lets the new drive mint a different mutex
and run its protocol stream concurrently with the old one, and the generation
fence stops the stale publication but not the concurrent wire work. Lease
entries no drive holds are pruned on the same call, which is safe precisely
because a held lease keeps a second `Arc` alive and `claim_drive` clones it
under the same write lock.

1. Consumer subscribes via `account_changes_stream`, receives a
   `MultiplexerEvent { scope, event, checkpoint }`.
2. Consumer atomically persists `(items, checkpoint)` in their
   own store.
3. Consumer calls `SyncEngine::ack_checkpoint(account, scope,
   checkpoint)`.
4. The per-account writer task (one per slot, fed by an
   mpsc kept in `engine.ack_senders`) receives an `AckRequest`,
   writes to `CheckpointStore`, then fires
   `SyncControl::record_checkpoint` to wake `pause` /
   `checkpoint_now` waiters. The returned `DurableCheckpointSet`
   contains the latest durable checkpoint for every change scope and
   backfill partition; it is empty when the account is safely idle
   without any durable checkpoint yet.

On restart the engine reads the last-acked cursor from the store
and re-runs `changes_stream` from there; items the consumer
never durably persisted come across again.

`InventoryFusion::run_with_broadcast` follows the same shape for
`EstablishViaInventory` scopes: inventory batches stream as the
cursor-establishment pass progresses. A provider may attach a page checkpoint
before its terminal cursor exists; that checkpoint persists only after the
consumer ack, and BOTH cursor-establishment paths - `establish_initial_cursor`
at attach and `run_establish` on reopen - ask whether the saved state is a
resumable inventory position before putting it into the registry. The default
`is_inventory_cursor` implementation delegates directly to
`inventory_resume_stream(cursor.clone()).is_some()`, and the resume seam guards
both directions of that equivalence once reached. A mismatch is a scoped cursor
invalidation (`RestartScope`), not an envelope schema change. Deferred attach
forwards a stored resume candidate to the ordinary recovery path so only that
scope is reset and re-established. Both initial and replacement attach check the
classifier and stream before deciding whether a stored cursor can enter the
changes registry. Either positive answer defers the cursor to the resume guard,
which catches disagreements before a page position can reach `changes_stream`.
The terminal `Done` still installs the ordinary live change cursor in the
registry.

`ChangesEvent` is the per-batch outcome returned by the driver:
`Advanced` / `Done` / `Stopped` / `Paused` / `Terminated(AccountError)`.

`account_changes_stream` returns `ChangesReceiver`, which preserves the
ordinary broadcast `recv` / `try_recv` shape but converts ring overflow
into a synthetic account-scoped `ChangeStreamLagged` warning. The
warning carries the overwritten batch count and directs the consumer to
reconcile from its last acknowledged checkpoint; retained ring entries
remain readable afterward. The engine cannot replay the overwritten
batches in-session, but it never presents that loss as success.
`account_changes_observer` returns the same type in its observer role: same
events, same lag warning shape, but the engine never waits on it and its lag
abandons nothing (see the consumer contract under "Bounded backfill lane").

Observing a lag on the ACKNOWLEDGING receiver also abandons the account's
outstanding checkpoint publications through the per-account `Publications`
ledger, and the warning's `next_action` reports how many. This is load-bearing, not
tidy-up: registration runs before the send and an entry leaves
only through a matching consumer ack, so a registration whose batch the
ring destroyed can never be retired and would gate every later `pause` /
`checkpoint_now` on the account forever. The ledger folds what every abandoned
coverage claim OWED into the next publication rather than discarding it, so a
degraded page lost to the ring still raises its durable debt when the consumer
next acknowledges progress. What an abandoned claim proved CLEAN does not
travel: a `Complete` report discharges obligations on the strength of an
enumeration the consumer took delivery of, and folding it forward would
discharge debt against a batch the ring destroyed. Under-reporting coverage
costs a re-walk; over-reporting it loses objects nobody sees again.
Surfacing lag without this
would turn silent in-session loss into a permanent hang. Abandonment is also
what frees the account's backfill bound, because that bound IS the set of
outstanding boundary registrations (see "The bounded backfill lane"); no second
step is needed here, and an earlier revision that kept a separate permit map did
need one. The cost is
bounded and disclosed: a registration whose batch is still in the ring
is also abandoned, so a boundary wait taken between the lag and that
batch's ack reports the previous durable checkpoint rather than the
newer one. Nothing durable is invented - the snapshot is not advanced -
and the accompanying warning is what tells the consumer to reconcile.

## Multiplexer

`Multiplexer::run` drives:

- **Adaptive polling** per scope:
  `AdaptiveCadence` starts at `MultiplexerConfig::poll_initial`
  (default 60s), halves on observed change down to `poll_min`
  (default 30s), doubles after **five** consecutive no-change
  ticks up to `poll_max` (default 30 minutes). The pure helper is
  `Multiplexer::updated_cadence(cur, seen_change, min, max)`.
  Every recovery delay in the poll loop - the `Retry` and `Reconcile` arms of
  `handle_drive_outcome` - selects on the scope token and the account shutdown
  token, so a deleted scope or a detaching account leaves immediately instead
  of waiting out a provider hint (which made detach burn toward
  `detach_timeout` and abort the task rather than draining it). The two
  recovery backoffs on the engine side, `re_establish_scope_with_backoff` and
  `restart_account`, wait through the shared `sleep_unless_shutdown` for the
  same reason. Pinned by `a_cancelled_scope_does_not_wait_out_its_retry_hint`
  and `a_recovery_backoff_is_cut_short_by_shutdown`.
- **Scope lifecycle** events from `Account::scope_lifecycle_stream`.
  `Created` and `Renamed` consult the registered cursor shapes.
  Account-wide and type-wide models already cover the new membership
  and create no cursor. Per-folder and query models translate the
  membership into matching `RestartScope` requests so the engine
  establishes only shapes the protocol already advertises. `Deleted`
  (and the delete half of `Renamed`) cancels the per-scope token, drops
  the in-memory cursor, and raises `ReopenRequest::ScopeDeleted`, on
  which the engine's reopen listener purges the scope's durable rows in
  full - change cursor and backfill rows, completion marker included
  (`reset_scope_for_deletion`). The marker must go: the consumer
  plausibly purged the folder's data on `Deleted`, so a marker surviving
  into a folder recreated under the same id would skip the new
  incarnation's entire cold-start walk.
  Not every protocol feeds this stream: IMAP deliberately emits
  no lifecycle events (the stream stays open and yields nothing
  until shutdown). IMAP discovers folders only at open/reopen, and
  mid-session folder mutations surface as
  `WatchEvent::Invalidated` via push IDLE rather than as
  `ScopeLifecycle` events, so a folder that appears after attach is
  invisible to the engine until the next account reopen. True
  NOTIFY-MAILBOXES / LIST-diff lifecycle detection for IMAP is a
  deferred feature, not a bug. The lifecycle reader reloads the
  replacement account whenever the reopen-generation watch changes;
  a naturally-ended or retryable stream reconnects with exponential
  backoff from 30 seconds to 480 seconds. Opening a stream alone does
  not reset the delay; a lifecycle event or account replacement does.
  Retry and reconcile errors record shared throttle holds and wait at
  least the larger of the backoff and provider retry hint. The reader
  rechecks account-wide holds before opening another stream, and both
  shutdown and account replacement interrupt the wait. Authenticated
  requests retain transport ownership of token refresh.
  JMAP forwards read failures, including failures of its expired-history
  snapshot repair; successful expired-history repair stays local because
  its Mailbox position is private to lifecycle. Google still retries
  locally: its stream-local diff baseline must become account-owned
  before reconnect can preserve changes across a failed label poll.
  A terminal lifecycle error is broadcast once and parks the reader
  until account replacement. Engine directives are handed to the reopen
  channel exactly once per stream failure - only `RestartAccount`, the one
  directive that actually replaces this reader's connection, then waits
  for the generation change. That wait is bounded (30s): a reopen that
  exhausts its budget or queues behind a pause never bumps the
  generation. Every other directive (scope restarts, scope disable,
  schema reset, capability downgrade, operator override) is handled
  without swapping the account, so the reader reconnects on its own
  backoff instead of parking for a signal that is never coming.
- **Reopen requests** on a `mpsc::Sender<ReopenRequest>` channel:
  `RestartScope` deletes the in-memory and durable cursor before
  re-establishing; `RestartAccount` routes up to the engine's reopen
  path. Capability shifts arrive as `RestartAccount` since the
  `EngineDirective::CapabilityChanged` variant was removed in Phase
  5A.

Per-scope cancellation tokens live in
`Multiplexer::scope_tokens` so a `ScopeLifecycle::Deleted` stops
the matching poll task without disturbing siblings. Each entry is a
`ScopeToken { generation, token }` and an exiting task retires its
registration by generation match (`retire_scope_token`), never by key.
Removing by key lets a departing task evict a successor's entry: task A
exits while its scope is momentarily absent from the registry
(mid-`restart_scope`), the 1s scan spawns B under the same key, A's
cleanup deletes B's entry, and the next scan - seeing no token - spawns
C. B and C then poll one scope forever, which is both doubled wire
traffic and two concurrent producers on a lane+scope that the
checkpoint supersession rule in `control.rs` assumes has exactly one.

An exiting poll task also reports how to leave its token (`PollExit`):
every ordinary exit retires the entry so the 1s scan can respawn the
scope, but a `RecoveryPlan::Terminal` verdict parks it - the uncancelled
token stays in the map as a tombstone the scan reads as "owned", so a
terminal error stays terminal instead of becoming a once-per-second
respawn/re-drive loop (each drive of which would re-broadcast
`Terminated`). Poll ownership and parked state are bound to the registry's
`DriveGeneration`. Before poll discovery or push dispatch, an entry whose
fence differs from the registry is cancelled and retired. Scope deletion and
re-establishment change the scope fence; a successful account replacement
changes the account fence. Both therefore revive terminally parked scopes
without requiring recovery paths to reset a second state store. Task cleanup
still uses its separate ownership generation so an old task cannot retire a
successor. A terminal lifecycle reader broadcasts once and waits for shutdown
or an account generation change, then opens the replacement's lifecycle stream.

Fresh unsupported change cursor envelopes from changes or inventory fusion
are scoped `Internal(AccountContract)` failures with terminal recovery. They
do not request an account-wide schema reset; schema recovery is reserved for
incompatible persisted envelopes. A published `Terminated` outcome reaches
recovery before a concurrently changed pause or stop boundary is honored.

The tombstone is MARKED (`ScopeToken::parked`), not merely present, because
the push reconciler shares the same map and needs to tell a tombstone from a
live poll task's registration. The reconciler is the other producer on the
lane, and it now skips a parked scope (`scope_is_parked`) instead of driving
it: without that, a later push hint re-drove a terminally failed scope,
re-broadcasting `Terminated` and spending a wire call that cannot succeed -
the milder sibling of the once-per-second respawn loop the park exists to
stop. `ScopeToken::tombstone()` is published because `ScopeTokens` is
published while every field of an entry is private, so nothing outside the
module could otherwise place an entry in the map. Pinned by
`a_terminally_parked_scope_is_not_re_driven_by_a_later_hint`.

Output is a `broadcast::Sender<MultiplexerEvent>`. The event
carries `{ scope, event: Arc<SyncEvent<Change>>, checkpoint }`.

After publishing any complete stream item, `Pause` returns `Paused` and
drops the activity guard even when that item carried no checkpoint.
`CheckpointNow` still waits for a checkpoint-bearing item because its
request is specifically for a durable cursor boundary.

## Backfill

A scope-local throttle encountered between partitions parks the walk in
`BackfillScan`, keyed by scope and incarnation, while other scopes proceed.
The parked state retains the driver, including its totals and coverage
verdict, the pending partition already taken from the driver, and the
original scope fence and undelivered watermark baseline. It holds no
scheduler admission, activity guard or provider stream.

When the deadline passes, the orchestrator retries the pending partition
before requesting another. It bypasses fresh planning, `begin_walk` and
durable checkpoint reconstruction. Current declared throttle keys are
checked again, including after admission; another scope-local hold parks
the same walk again. Scope removal or an incarnation change discards it.
The original fence remains authoritative after admission and at completion.
A changed undelivered watermark abandons the parked walk without completion
and sends it through the existing scratch-restart path.

The rescan wait is shortened to the earliest parked, held or retry deadline,
within the normal one-second discovery cadence, and selects shutdown.
Account-wide holds continue to sleep inline.

`BackfillRunner::run_partition` walks
`inventory_partition_stream(scope, partition)` and broadcasts each
page as a synthetic `Batch` carrying a `BackfillCheckpoint`. It does
NOT write to `CheckpointStore`: durable persistence is
consumer-ack-deferred, exactly like `drive_changes_stream`. The
consumer persists the page items, then calls
`SyncEngine::ack_checkpoint` with the `Checkpoint::Backfill`; the
ack writer routes that to `CheckpointStore::put_backfill`. This keeps
at-least-once intact for cold-start hydration - a crash between
broadcast and the consumer's write re-walks the partition on restart
(the orchestrator always restarts a scope from its first partition),
so no inventory page is lost. An eager engine-side write would let the
store record progress the consumer never durably persisted, losing
that page's objects permanently. The orchestrator continuously rescans
the cursor registry by scope incarnation and plans partitions from
`Account::inventory_partitioning(scope)`. Before walking any scope the
orchestrator parks on `wait_for_real_subscriber` (the same guard the
deferred-inventory fusion path uses): cold-start pages broadcast onto
the per-account channel during `attach`, but the consumer can only
`account_changes_stream` after `attach` inserts the slot, and a
`tokio::broadcast` receiver that joins late starts at the ring tail and
never sees values sent before it subscribed (the slot keeps a sentinel
receiver so the channel never closes, and the send "succeeds" in front of
no real reader). The gate asks `ChangeDelivery::has_numbered_subscriber`,
never the broadcast's receiver count, so neither the sentinel nor an
observer opens it. For a `Ready`-cursor account whose entire cold start
rides backfill (Gmail `CursorScope::Account`, JMAP
`CursorScope::Type(Email)`) skipping the wait silently drops the initial
inventory page and the consumer ingests zero objects. That gate and the
bounded lane below compose cleanly and answer different questions: the gate is
a one-time wait for a FIRST subscriber before the walk starts, while the lane
bounds how far ahead of that subscriber the walk may run. A subscriber that
arrives, satisfies the gate, and then leaves is handled by the lane's release
paths, not by re-entering the gate - the walk keeps running and its pages are
retired as undelivered, which is the pre-lane behaviour. The partitioner
(`backfill/partitioner.rs::plan`) handles three strategies:
`TimeWindowed` (boundaries plus a final open-ended partition),
`UidRange` (newest-first chunking), and `PageCount` (page-size
chunking). `Full` falls through as a single partition. No protocol crate
in this workspace currently advertises `PageCount`: JMAP Email used to,
and now reports `Full`, because its inventory pages `Email/query` by
`anchor` rather than by integer position and an engine partition
boundary would discard that anchor (see `reference/jmap.md`). The
`PageCount` strategy and the `OpenPages` plan it drives stay published
for a protocol whose paging really is positionally stable.

Every range partition in this workspace is HALF-OPEN, `[from, to)`, with no
exceptions. `PartitionBounds::Time`, `PartitionBounds::Page`,
`InventoryPartition::Time`, `InventoryPartition::Page` and
`CoverageCoordinate::PageRange` always were; `PartitionBounds::Uid`,
`InventoryPartition::Uid` and `CoverageCoordinate::UidRange` were inclusive
until this was unified, and are now half-open too. Their endpoints are `u64`
rather than `u32` for exactly that reason: a walk covering the whole UID space
has an exclusive end of `u32::MAX + 1`, which a `u32` cannot hold. The UID
values themselves are still `u32` - `InventoryPartitioning::UidRange`'s
`max_uid` and the partitioner's `total` stay `u32`, and the widening is only
in the range endpoints. No protocol crate currently serves
`InventoryPartition::Uid`, so the only producers and consumers are the
partitioner and `CoverageDomain::for_partition`; `CoverageDomain::covers`
tests containment (`self_from <= other_from && self_to >= other_to`) and reads
identically under either convention.

One upgrade consequence: `partition_key` renders a UID partition as
`uid:{from}:{to}`, so durable backfill checkpoints written under the old
inclusive convention no longer match the newly planned keys. Those partitions
re-walk once. That is re-work, not loss - a missing partition checkpoint means
"start this chunk over", and the chunk boundaries themselves are unchanged.

The deferred-inventory set captured at attach is a structural exclusion:
the first registry incarnation of each such scope belongs to
`InventoryFusion` and is never walked again by backfill, regardless of
which worker wins the subscriber wake. Newly-created scopes and later
delete + re-establish incarnations are discovered by subsequent scans
and receive their own pass.

The rescan retires an incarnation only on a durable conclusion: the plan
ran to completion, or the checkpoint store already carries its
completion marker. A partition failure or a checkpoint-store read
failure deliberately leaves the scope `Pending`, so those incarnations
stay eligible and return on an exponential delay (5s doubling to a 5min
ceiling) rather than being filtered out for the rest of the attachment -
otherwise one transient request would cost that scope its whole backfill
until the next attach. `CursorRegistry::all_scope_incarnations`
enumerates live cursors and annotates each with its incarnation, so the
rescan can never plan a walk for a scope the registry has dropped.

Open-ended `PageCount` (`total: None`) drives `BackfillPlan::OpenPages`:
the orchestrator walks `Page { from, to }` windows of width `chunk`,
advancing `from = to` each pass, and terminates only when a partition's
completion DECLARES `PartitionEnd::Exhausted` (carried on
`BackfillPartitionOutcome::end`). The entry count is never read as that
declaration: a server whose query cap sits below `chunk` returns a short
page with more behind it, and a window whose ids all vanished before
hydration is empty with more behind it. A partition stream that ends
without any completion declared nothing, so the walk ends there without
completing the scope - neither asking for windows forever nor writing a
sentinel over ground nothing proved. The same holds for every plan, not only
`OpenPages`: `run_partition` reports a silently ended stream as
`complete: false`, so a fixed plan (Full, time windows, UID ranges) no longer
writes its completion sentinel over it, and the scope stays Pending. Both
inventory front ends surface the silent end as a typed `SyncEvent::Warning`
of kind `WarningKind::InventoryEndedUnannounced`
(`inventory_walk::unannounced_end_warning`); the fusion walk establishes no
cursor. A silent end is a producer breaking the `InventoryEvent` contract,
and the user sees a scope that never finishes, so the reason travels on the
change stream rather than in a log (ruled 2026-10-04). Producers that emit `SyncEvent`
streams get the declaration from `lift_complete_walk`, the one place that
still infers it (see `reference/types.md`).

Resume: the `BackfillRegistry` (`BackfillState::Pending/Running/Completed`)
is an engine-wide observability index keyed by `(AccountId, CursorScope)`.
Account rows are removed on detach, so the registry cannot merge
identically-shaped scopes from different accounts or carry completion
across an attach -> detach -> re-attach cycle. The sole durable record is
the consumer-acked `BackfillCheckpoint` in the `CheckpointStore`. On
completion the orchestrator broadcasts a durable completion marker - a
synthetic empty `Batch` whose `BackfillCheckpoint` sits on the
`completion_partition` sentinel key (`partitioner::completion_partition`,
distinct from every `page:F:T` / `uid:F:T` / `time:F:T` key). It rides
the same consumer-ack path as the page batches (`emit_backfill_complete`
-> broadcast -> consumer persist+ack -> ack writer `put_backfill`), and
because it is ordered behind every page it only lands durably after the
consumer has persisted all of them - a crash before completion re-walks
rather than recording a false "done". Its `items_done` is stamped at
`total_walked + 1` (the honest total rides in `items_estimated`) so it
strictly wins `get_backfill`'s "latest by items_done" query regardless of
how a store breaks ties, and on re-attach the marker is the checkpoint
returned.

This skip-on-complete signal is uniform across plan kinds; the difference
is whether positional resume is also possible:

- **`OpenPages`** (no current producer; see `PageCount` above): the
  orchestrator runs the persisted
  checkpoint through the pure `open_pages_resume` decision - completion
  sentinel -> **skip entirely**; any other `page:F:T` -> resume the walk
  at `T` rather than page 0; no checkpoint or an unrecognised partition
  kind -> start fresh at 0. A SHORT page (`items_done < T - F`) is
  deliberately not read as exhaustion: a partition may emit fewer entries
  than its window width while the scope still has results (ids deleted
  between the listing call and the hydration call, id-less objects
  dropped), so
  only the completion marker proves exhaustion. Resuming at `T` after a
  final page whose marker had not yet landed costs one probe window,
  whose declared `Exhausted` then lands the marker.
- **`Fixed`** (`Full` / `TimeWindowed` / `UidRange`): the partition list is
  a known finite set with no positional "resume from here", so the signal
  is binary - the completion marker is present (`backfill_complete_recorded`
  -> **skip the whole plan**, no inventory call) or it is not (walk every
  partition). A crash mid-plan leaves no marker and re-walks all partitions
  on re-attach; re-emitting consumer-acked pages is idempotent, so nothing
  is lost. Positional mid-plan resume is a possible future refinement.

Because `get_backfill` returns a consumer-acked checkpoint, resume never
skips a window the consumer has not durably persisted; a read error falls
back to a full walk.

Both decisions land in `BackfillPlan::resume`, which returns a
`ScopeResume` of either `Skip` or `Walk(ScopeWalkDriver)`. That is the
only place the two plan shapes differ: the orchestrator's walk loop
below it is written once, so barrier handling, completion-marker
withholding and `BackfillRegistry` bookkeeping cannot drift apart
between them. A read failure routes through `walk_from_scratch`, which
is defined as the no-checkpoint answer rather than a second code path.

Pause and checkpoint waiters observe backfill boundaries through the
ack writer: it calls `SyncControl::record_checkpoint` after the
consumer-acked `put_backfill` lands, so waiters wake on a durable,
consumer-acknowledged boundary - identical to the change-cursor path.

### The bounded backfill lane

Backfill publications are flow controlled; live changes are not. Before
publishing a page the runner waits until the account has fewer than
`BackfillConfig::lane_capacity` (default 64) published-but-UNREAD backfill
batches. The wait sits after the page is read off the provider stream
and before its publication is registered, so a parked producer is not asked for
the next page either. Keeping the bound well under `changes_capacity` (default
256) is what stops a cold start overrunning the shared ring on its own. A
live-lane burst still can, and that case keeps the lag-abandonment machinery
above exactly as it was. Both producers send into the same ring and the bound
is consulted only before a backfill send, so there is no fairness arbiter: when
both have a batch ready, both send.

**The consumer contract this rests on.** There are two subscriptions, and they
are two roles. `account_changes_stream` hands out the ACKNOWLEDGING receiver,
NUMBERED by the delivery gate; `account_changes_observer` hands out an
OBSERVER, unnumbered, that sees the same events and is never waited on. Exactly
one acknowledger is supported: the boundary ledger keeps one entry per lane and
releases it on the first acknowledgement, so a second acknowledger's own
registration is settled out from under it. Several numbered receivers are
still admitted and the RING is followed by the slowest of them, not the
fastest: a page charges the lane while any live numbered receiver that could
hold it has not read it. Following the fastest reader instead reproduced the
overrun the bound exists to prevent - the fast reader frees the producer, the
acknowledger falls `changes_capacity` events behind and lags, `on_lag`
abandons the walk's pages and withholds its marker, and the cycle repeats for
the life of the attachment.

An observer counts for nothing, and each of the three places that used to read
a raw receiver count now asks the gate instead. The subscriber gate
(`wait_for_real_subscriber`) asks `ChangeDelivery::has_numbered_subscriber`,
so an observer that arrives first no longer opens it and takes delivery of
pages the acknowledger - joining at the ring's tail - can never see; that was
the failure shape before the bound existed, when a walk ran into the observer
and settled, and it is why the acknowledger once had to subscribe first, which
it no longer must. A publication that expects an acknowledgement is sent
through `ChangeDelivery::publish_backfill` or
`ChangeDelivery::publish_acknowledgeable`, both of which answer "did a NUMBERED
receiver take delivery" from the live set under the gate's own lock - a page
that reached only observers reached nobody, and is retired at once rather than
staying registered for an acknowledgement that can never come. And an observer
carries no control handle, so its lag is reported to it alone
(`ChangeStreamLagged`, "observer stream lagged") without abandoning any
registration; abandoning on an observer's lag voided every in-flight walk on
the account because a UI reader was slow. Its reads free no capacity
(`note_receipt` is keyed on the numbering, pinned by
`an_unnumbered_receivers_read_frees_no_capacity`), and it may be held unpolled
for ever at no cost. Acknowledging from an observer is a contract violation:
the engine judged the page delivered to nobody. The two are separate methods
rather than a flag on one, because the roles differ and a flag would let a call
site flip one into the other. Pinned by
`an_observer_counts_for_neither_the_gate_nor_the_delivery` and
`an_observers_lag_abandons_nothing`.

**Every numbered receiver MUST be polled or dropped.** Every receiver
`account_changes_stream` hands out is numbered, so a consumer that opens a
second such receiver for a UI and stops polling it holds up to `lane_capacity`
pages unread for the life of the attachment and parks cold start permanently.
That stall reports nothing on its own - the departure warning fires on drop and
the lag warning on poll, and this receiver does neither - so `LaneGate` warns on
`bifrost.sync.backfill` when a park BEGINS, at most once per 30s per account,
naming the account, the charged scopes and the receiver sequences holding the
charge (`PendingCoverage::backfill_charge_holders`). A receiver that only
watches should be an observer, which carries no such obligation.

**The precise guarantee, which is narrower than "the slowest reader".** The
bound follows the slowest live numbered reader among pages NOT YET
ACKNOWLEDGED; an ACKNOWLEDGEMENT frees a page for every receiver. The reader
set lives on the boundary entry, so every path that resolves the publication -
`acknowledge_publication`, `acknowledge_checkpoint`, `retire_publication`, and
the supersession fold in `register_in` - removes the record together with the
evidence that a slower receiver had not read it. What that leaves: with two
numbered receivers where the ACKNOWLEDGER is the faster, its acknowledgements
free the bound at its own pace, the producer runs on, and the slower numbered
observer is overwritten in the ring and lags - after which `on_lag` abandons
every outstanding registration of the account, including pages the acknowledger
read but had not answered for, the marker is withheld and the walk restarts.
The structural close is a residual `(id, delivered_at, readers)` record kept
when an entry leaves the ledger still unread by an eligible live receiver; it
is filed as a residual and deliberately not built here. The observer
subscription removes the case where the slow receiver is an observer - an
observer is unnumbered and never in the reader set - and what remains is two
ACKNOWLEDGING receivers, which the contract above already declares unsupported.

**The permit is the RECEIPT, not the acknowledgement.** A page charges the lane
from its publication until every eligible NUMBERED receiver has read it off the
stream (`ChangesReceiver::recv` / `try_recv` call
`PendingCoverage::mark_received` with the receiver's own sequence, which pulses
the capacity wake); the entry and each page it subsumed carry their own set of
reader sequences, and `backfill_in_flight` counts a page while any live receiver
numbered below its `delivered_at` is absent from that set. Eligibility is the
numbering, because a broadcast receiver joins at the ring's TAIL: a later
subscriber never held the page and can never re-charge it, and a departing
receiver stops charging what it was holding (`PendingCoverage::receiver_departed`,
called from the departure sweep under the delivery lock) so a slow reader that
walks away does not park the producer for the life of the attachment. The
producer therefore runs exactly as far ahead as the slowest consumer READS, and the
consumer may acknowledge on any schedule it likes - including never, at the
usual cost of never advancing its durable cursor. Read and acknowledged free
different things: a read returns lane capacity, an acknowledgement retires the
registration, its boundary waiter and its coverage claim. Acknowledging a page
already read frees no further capacity, because the read already did.

The bound is protection for the shared broadcast ring, and what overruns a ring
is pages nobody has taken out of it. Binding it to the acknowledgement - a
promise about the CONSUMER's durability - deadlocked a consumer that batches
acknowledgements against the parked producer, since nothing broke the wait but
an acknowledgement the consumer was holding. What the receipt bound gives up is
that the lane no longer limits the consumer's unpersisted backlog, which is the
ordinary trade on any channel, and with it the bound on how many superseded
pages one partition's surviving entry retains (see `BoundaryEntry::subsumed`).
That retained history is unbounded PER PARTITION, and for the `Full` plan -
which is what JMAP Email uses - there is one partition per scope, so it is
unbounded over the whole scope's backfill: one `PublicationId`, its
`PublicationReceipt` and its coverage claim per page the consumer read and did
not acknowledge.
Marking is keyed on the numbering: a reader the delivery gate never handed out
is not the acknowledger, and letting its read free the producer would let a walk
run a whole ring ahead of whoever answers for the pages.

The completion guarantee is unchanged, and deliberately does not consult the
receipt: a receiver that departs holding pages it read but never acknowledged
still strands them, and the departure sweep still records the loss.

A bounded `mpsc` for backfill was rejected on the same
contract: it is single-consumer, so pages would reach one receiver and vanish
for the rest, while a bound on the producer leaves the stream byte-for-byte
unchanged.

**The capacity is the publication ledger itself.** An account's backfill
in-flight count is the sum over its live backfill `BoundaryEntry`s in
`PendingCoverage`, each charging one for itself and one per page it superseded,
counting only the pages some live numbered receiver could still be holding in
the ring. `engine/lane.rs` holds no
ledger: `LaneGate` is the wait plus the
account's scheduler admission. A first design kept a permit map beside the
ledger, and two reviews found the same defect four times over, the two
structures disagreeing about what was in flight. There is no separate release
call site to forget: the only path that RECORDS a release is the receiver's read
(`ChangesReceiver::note_receipt`), and every other ledger mutation that removes
or lightens an entry frees capacity as a side effect of what it was already
doing, pulsing one `Notify`:

- a numbered receiver's READ adds its sequence to the page's reader set, on the
  entry or on the `SubsumedPage` that holds it, and is the ordinary way capacity
  comes back - the charge lifts once the set covers every eligible live receiver;
- an acknowledgement removes the entry, or strips the acknowledged id from its
  survivor's `subsumed` when the page has since been superseded. It frees
  nothing further for a page that was already read - but removing the ENTRY
  frees every unread page folded into it as well, and it does so for every
  receiver, which is the narrowing described in the contract above;
- with no eligible live receiver at all, `page_unread` falls back to "has
  anybody read it": a page broadcast to nobody stays charged until some other
  path retires it - which is what keeps a send that reached only the slot's
  sentinel receiver charged until the runner retires it - while a page whose
  only reader has since departed has left the ring for good and frees the
  producer rather than parking it for the life of the attachment;
- a store failure after a valid acknowledgement retires the entry the same way.
  The entry tracks delivery, not durability, and holding it would park cold
  start over a transient store error;
- a REFUSED acknowledgement retires nothing. `AckFailure::Rejected` (a token
  from another lane, an undecodable envelope) is evidence about the caller and
  `AckFailure::Store` is evidence about the write; only the second retires;
- a send that reached only the slot's sentinel receiver is retired by the
  runner, as it always was;
- a subscriber lag (`abandon_checkpoints`) drains every registration;
- a durable scope reset (`invalidate_scope`, on both of the writer's passes)
  removes the scope's entries, subsumed pages included, and also extracts that
  scope's share of the carry-forward slot, so debt rescued by a sweep reaches
  the reset instead of dying with the attachment;
- a `PENDING_BOUNDARY_CAP` eviction of a charged entry;
- a receiver departure, which both recomputes the departing sequence's charge
  and sweeps what no remaining receiver can reach - below;
- `detach` cancels the slot token and pulses the wake, so a parked producer
  leaves at once rather than being awaited to `detach_timeout` and aborted.

A stale acknowledgement settles nothing: settlement is by publication identity
against the live entries, and there is no scope-wide branch to replay against.

**Supersession is the one place the literal rule is corrected.** Supersession
removes the older entry on purpose, so that a consumer can persist N batches
of one partition and acknowledge only the last, for any N below the bound.
Under the bare rule one partition could publish
without limit while holding one record. The survivor therefore inherits the
charge of what it displaced (`BoundaryEntry::subsumed`, each page with its own
delivery stamp), and acknowledging a superseded id settles exactly that page.
Only the backfill lane inherits: `Lane::Change` never consults the bound, and
inheriting there would let a subscriber that drains without acknowledging grow
one entry's history without limit.

**A parked producer holds no scheduler admission.** With a minimal budget the
account has one sync permit, and a backfill parked while holding it starved
polling and push reconciliation. `wait_for_capacity_holding_admission` keeps
the permit on the non-blocking fast path, releases it for a real park and
re-takes it on the wake. A refused re-admission is `WaitFailed::Refused`, which
the runner turns into a failed partition on the rescan ramp; it is never read
as `WaitFailed::ShuttingDown`, which would retire the whole orchestrator. The
completion marker takes no admission at all (`wait_for_capacity`): it does no
wire work and is emitted after the partition pass has handed its permit back.

**Which receivers can still acknowledge a page is tracked per receiver.** A
`tokio::broadcast` receiver joins at the ring's tail, so it can never take
delivery of a batch broadcast before it subscribed. A receiver holding
unacknowledged pages that departs while a replacement is already subscribed
leaves pages nobody can answer for, and a subscriber count never sees it. So
`ChangeDelivery` numbers every receiver it hands out, stamps each backfill
publication with the numbering at send time (`delivered_at`), and on a
receiver's drop retires every entry, and every subsumed page, whose stamp is at
or beyond the lowest live number (`release_undelivered`). The retirement
carries the pages' debt forward exactly as a lag does, in the same ledger
transition that removes the entries, and announces the same
`ChangeStreamLagged` warning on the stream. A page registered but not yet sent
is never swept, since a receiver subscribing in that window really does receive
it. One mutex orders a send with its stamp, a subscribe with its number, and a
receiver's destruction with its unregister and sweep; the lock order is
delivery then coverage, never the reverse. Live-lane publications are never
stamped or swept, but one that expects an acknowledgement still goes through
the gate (`publish_acknowledgeable`), which takes the same lock only to answer
whether a numbered receiver was live at the send; only events nobody
acknowledges - warnings, terminations, barrier pages - send on the raw sender.
The receiver holds a weak handle to the gate,
so a receiver retained across `detach` sees `Closed` rather than keeping the
sender alive itself. The acknowledgeability question cannot be answered by the
ledger alone, because `claim_checkpoint` falls back to the publication receipt
by design so acknowledgements can replay after a writer restart.

**A producer revalidates its scope before publishing, at every seam it can park
on.** A reset fences every id minted before it, but a page still waiting has no
id yet, so it would come back above the fence and rebuild the durable state the
reset deleted. `run_partition`, `run_backfill_partition_at_boundary` and
`emit_backfill_complete` all snapshot `PendingCoverage::scope_fence` and refuse
to publish if it moved, and each snapshot is taken at the start of the span it
protects rather than at the moment of publishing - a reading taken after the
reset compares the replacement incarnation's fence with itself:

- the PAGE check compares against the fence the PASS started under, since a
  reset can close between page k's publish and page k+1's read as easily as
  inside a capacity park;
- the PARTITION check runs once admission is in hand and before a single page is
  read, comparing against the fence the WALK started under, because everything
  between two partitions can park indefinitely (a pause, a throttle deadline,
  the scheduler's admission queue) and the pages that follow would otherwise be
  published against rows a `delete_backfill: true` reset had just removed. It
  fails the partition, which ends the walk and withholds the marker;
- the MARKER check compares against the walk's starting fence too, because a
  reset can close between the last page and the provider stream's terminal
  `Done`.

Only a full `invalidate_scope` (the writer's `ResetScope`) moves this fence; the
backfill-lane discard fences `fenced_backfill`, which `scope_fence` does not
read, so a walk that lost pages is still re-walked rather than aborted.
`MarkerOutcome` names
`Published`, `Withheld` and `ShuttingDown`, so a withheld marker records an
attempt and the orchestrator carries on with the next scope. The orchestrator
also re-checks the subscriber gate before every scope, not once at start, and
re-checks that the incarnation it is about to walk is still live.

Pinned by `tests/backfill_lane_flow_control.rs` (the stall at the bound on pages
held UNREAD, the release a single read buys and the further release an
acknowledgement of that same page does NOT buy, live
changes on a one-permit budget, no loss or duplication through an 8-slot ring, a
completed walk not stranding admission, the eager-acker bound-of-one walk,
detach while parked, the receiver replaced while parked, the retained receiver
seeing `Closed`, and the live driver's refusal of a backfill checkpoint), the
gate's unit tests in `engine/lane.rs`, `tests/backfill_scope_fence.rs` (the
between-partitions reset, staged through an account pause - `run_partition`
holds the control's activity guard for a whole pass, so a `pause()` that returns
has proved the producer is parked at the top of the NEXT partition with no page
in hand - and triggered by a provider folder deletion, whose purge of the
scope's durable rows makes any later row or page proof that a walk kept running),
the ledger tests in
`cursor/coverage.rs`, the receiver-ordering and receipt tests in
`multiplexer::tests` (multi-threaded: a numbered receiver's read frees capacity
with no acknowledgement while its registration survives, a superseded page's
receipt reaches it inside its survivor, an unnumbered reader's read frees
nothing, an observer counts for neither the gate nor the delivery decision and
its lag abandons nothing), and the reset and ack-failure tests in
`engine/tests.rs`.
Everything in the integration file runs current-thread under `start_paused`
with no wall-clock sleeps; none of it is a race test. Each ablation named in
those tests' doc comments was applied, observed to fail the test, and restored,
with one deliberate exception: the single-lock ordering in `ChangeDelivery` is
structural and cannot be staged, since any hook between a send and its stamp
would deadlock on the same lock rather than race.

### Completion integrity across consumer replacement

The receiver sweep above made a pre-existing defect visible, and the engine
now guarantees against it: a consumer replaced mid-walk must not leave a
durable completion marker, or a durable resume position, over pages nobody
received. A receiver that departs holding unacknowledged pages strands them -
read or unread alike, since the sweep does not consult the receipt bit: freeing
lane capacity and answering for a page are different questions -
because its replacement joined at the ring's tail. Left alone, the walk
completes, the replacement acknowledges the completion marker in good faith,
and every later attach answers `Skip` with the stranded pages never offered
again. The guarantee holds for one acknowledging receiver under the contract
above, and it costs re-walks in the direction that is safe: under-reporting
coverage re-walks, over-reporting it loses objects nobody sees again.

**The guarantee ends where the attachment does.** A page delivered but
unacknowledged at `detach` is the consumer's to have persisted or to forfeit,
exactly as at a crash. The departure sweep goes inert the moment `detach`
begins (`ChangeDelivery::begin_teardown`, which sets the ledger's flag under
the delivery lock so a sweep already admitted finishes recording its loss and
its discard request first), so a receiver dropped anywhere in
the teardown window - during the discard drain, after it, or after `detach`
returns - records no loss and requests no discard; before the flag, the two
halves of that window already had one outcome (a request the departed writer
could not act on, or no sweep at all because the receiver's weak handle no
longer upgrades), and the flag makes the whole window behave like its end.
The ORDERING that puts the flag there - `begin_teardown` above the boundary
`Stop`, the shutdown cancel and the worker awaits, with no await between the
slot removal and it - is pinned separately from the flag's effect, by
`tests/backfill_lane_flow_control.rs::a_departure_inside_a_live_detach_records_no_loss`.
The unit test beside the sweep calls `begin_teardown` directly, so deleting the
production call or moving it below the worker shutdown leaves it green; the
integration test drives the real `detach`, holds it inside its worker await with
a parked deferred-inventory walk, and drops a receiver holding an
unacknowledged page into that window. Both ablations were run and both fail it.

One mechanical note that cost a round: parking a CHANGES stream does not hold
`detach` at all. The multiplexer spawns its per-scope poll tasks itself and
aborts them; the workers `detach` joins are the ones `attach` stored, so the
straggler has to be one of those - the deferred-inventory worker, the backfill
orchestrator, the multiplexer task itself. A test that stages a window must
assert it is really in one (`JoinHandle::is_finished`), because a detach that
has already returned passes every such test. Its positive control is
`a_loss_with_no_walk_running_is_settled_by_detach`, the identical setup with the
drop one step earlier, which does destroy the completion marker.

Ruled 2026-09-06, close by contract: the alternative was a durable "scope lost
pages" row, a new `CheckpointStore` method every downstream store implements,
bought for a consumer already outside the persist-then-acknowledge contract.
The drain of discard requests recorded BEFORE teardown still runs, with both
its writer awaits clamped to the writer phase's own fresh `detach_timeout`
deadline, so a store that hangs in `delete_backfill` or `put_ledger`
cannot hang `detach`. A `pause` or `checkpoint_now` waiter parked on a
registration that detach has made unacknowledgeable (the public ack sender
goes at the top of `detach`) ends with an error when the boundary reads
`Stop`: the consumer's own control clone keeps the checkpoint watch open, so
the boundary is the only channel that can carry the verdict, and the
departure sweep that used to wake such a waiter by accident is inert.

**Every page that reaches nobody is recorded, per scope.** `PendingCoverage`
keeps a monotonic undelivered counter keyed by scope
(`undelivered_watermark`, bumped by `note_undelivered`). Three paths feed it:
the runner's send that reached only the sentinel receiver, the receiver-drop
sweep (`release_undelivered`, attributing each retired entry through its own
lane), and a lag abandonment (`abandon_checkpoints`). The last two judge one
page at a time and count only pages that were actually sent, since a survivor
can be unsent while the pages folded into it were delivered, and an unsent
page is about to reach whoever subscribes next. Keyed by scope rather than
counted per account because the reading is a verdict on one walk; a shared
counter would re-walk scope Y for scope X's losses for as long as X kept
losing pages. It has its own mutex because the sweep records after dropping
the ledger lock.

**A walk is judged whole by comparing two readings.** `BackfillScan::begin_walk`
takes the scope's reading as each walk starts, not when the batch was
selected, since one rescan hands out several scopes walked in turn, and the
scan owns it. The orchestrator compares after the walk; `emit_backfill_complete`
compares again after its capacity wait, because a departing receiver's sweep
both records the walk's last page undelivered and frees the capacity the
marker is parked on; and the orchestrator re-reads once more after the
emission, because the marker is itself a publication that can reach nobody. A
walk that is not whole records an attempt instead of settling, the registry is
marked from the same answer, and `MarkerOutcome::Withheld` keeps the
orchestrator running on the next scope.

**The marker carries its walk's reading to acknowledgement time.** With spare
capacity the marker reaches every receiver at once, so the loss can land after
it was published: A holds a page, B subscribes behind it, both receive the
marker, A departs, B acknowledges. The debt check cannot see this, because a
clean page that was never persisted owes nothing. So `register_walk_marker`
stamps the walk's reading into the publication RECEIPT, not the boundary entry,
which a store failure or a supersession removes while the acknowledgement is
still valid, and the writer's `completion_refusal` compares it against the
scope's current reading. `WalkNotWhole` is evaluated before `OpenDebt`, since
only the first is about the walk and its repair must not be masked. An
acknowledgement carrying NO publication is `Unvouchable` too, and refused for
the same reason it is refused everywhere else here: with no id there is no
reading, no reset fence and no claim lookup, so every check between an
acknowledgement and a durable completion is skipped and the marker would become
durable on the caller's say-so. No engine path produces one - the live driver
refuses a backfill checkpoint outright and the orchestrator always publishes an
id - so this is a consumer that dropped the id, and a completion is the one
outcome that cannot be walked back. A page acknowledged the same way still
persists: a page row is a resume position, so an unvouchable one costs a re-read
rather than a scope nobody walks again.

A publication minted by a PRIOR attachment is recognised by the id's instance
segment (`minted_here`). The arm that actually runs for one is the backfill
withhold BEFORE the claim lookup, which matches any backfill checkpoint whose
receipt names the same scope and partition - the completion partition included -
so a marker replayed across a detach is withheld there, with nothing of the
prior attachment's evidence ingested on the way past. `CompletionRefusal::
Unvouchable` covers what is left: a marker whose receipt names some other
partition, and the no-publication case above. Either way nothing is touched,
since writing it would land a completion no ledger can vouch for and discarding
on it would delete one an earlier attachment earned. All of these ride
`AckPersistOutcome::Withheld`: the consumer's acknowledgement succeeds, and only
the durable row is refused.

The same instance segment bounds the ledger's own watermarks. `claim_checkpoint`
answers from `persisted` and `folded` only for ids THIS ledger minted: both hold
this instance's ids and both compare by raw id, so a foreign id from an earlier
instance sits under them and a replay was reported `AlreadyPersisted` the moment
this attachment had persisted anything on that lane - a durable boundary
announced for a write nobody made. A foreign id goes to the receipt fallback,
which is what the replay is for.

**A walk that lost pages forfeits its resume position, durably.** Withholding
the marker is not enough for `BackfillPlan::OpenPages`, whose resume starts
after the furthest acknowledged window, which sits past the hole. So
`BackfillScan` marks the incarnation `lost_pages` and its next walk takes
`walk_from_scratch`; and because that note dies with the attachment while the
rows do not, the scope's backfill rows are also dropped
(`WriterRequest::DiscardBackfillProgress`, rows only: no cursor, no ledger).
The discard FENCES the attempt that wrote the rows, the way a reset does and
twice around its store awaits, or a late acknowledgement from a consumer still
holding one of that walk's pages rebuilds the row. It fences the backfill lane
only (`invalidate_scope_backfill`, a second per-scope fence map), because
fencing the whole scope refused live-lane acknowledgements the replacement
had genuinely received. The ack-time discard is ceilinged at the refused
marker's id, the newest id its attempt minted, so a delayed acknowledgement of
an old walk's marker cannot retire a retry that is already publishing.

**The ceiling bounds the retirement; it cannot bound the delete.**
`CheckpointStore::delete_backfill` takes every row of the scope and knows
nothing of publication ids, so the same delayed acknowledgement that must not
retire a later attempt's publications must not delete its ROWS either - the
resume position it has already earned, and with it everything that attempt has
offered so far. A ceilinged discard therefore performs the fence and skips the
delete once the scope has minted a backfill publication above the ceiling
(`PendingCoverage::backfill_minted_after`, answered from a per-scope
highest-minted record rather than from the live entries, because a later
attempt's publications are gone the moment they are acknowledged). The request
it cannot answer stays outstanding, exactly as before, and the later attempt's
own repair - the rescan's reopen and its unbounded pre-retry discard - is what
removes rows if any need removing.

**The discard is requested when the loss is recorded, and settled only by a
delete that succeeded.** `note_undelivered` records the scope in a request
set, because every trigger that ran at walk end lived in the attachment that
saw the loss and the orchestrator returns on shutdown from inside its
partition loop. A walk takes its OWN scope's request at its end, the pre-retry
discard takes it because it is the whole repair asked for, and `detach` drains
what is left while the writer is still alive. A failed delete leaves the
request outstanding, and so does the writer's `WalkNotWhole` discard, which is
ceilinged at the refused marker and therefore not the whole repair; the
rescan's reopen and its pre-retry discard are what settle that one.

**The rescan reopens a settled incarnation whose reading moved.** The marker
is published and the incarnation settled in one step, and the loss that voids
both can land afterwards, so `BackfillScan::settled` records the walk's
baseline and `select` compares it on every rescan: a moved reading unsettles
the incarnation as `lost_pages`. A retry that restarts from scratch discards
BEFORE it walks, since a loss landing after the marker's acknowledgement
leaves a durable marker that nothing else removes. `select` prunes the
baseline, retry deadline and lost-pages mark of any incarnation the registry
no longer carries.

The comparison covers a FAILED attempt too, and the baseline survives the
failure for that purpose. `note_lost_pages` runs at a walk's end, so a departure
sweep landing after a transient partition failure is seen by nobody: the next
attempt resumes, takes the moved reading as its own baseline, judges itself
whole and earns a durable marker, while the discard request that loss raised
sits outstanding until `detach` acts on it and deletes the marker that second
walk legitimately earned - a full re-walk on the next attach, once per flaky
partition. Reopening it at the rescan makes the retry restart from scratch, and
that retry's pre-retry discard is what settles the request.

A restart-from-scratch does not read `get_backfill` at all: the stored
checkpoint has already been ruled out as a resume position, so reading it costs
a round trip whose answer is discarded and whose failure logged a "resume read
failed" degrade for a walk that was starting over regardless.

**One recorded gap.** A receiver dropped between `detach`'s drain of the
request set and the slot being dropped records a request nobody can act on,
because the writer is gone. The cost is the ordinary one for an unrepaired
loss on an `OpenPages` scope: the next attach resumes past the hole. The
ruling is to close it by contract (a page delivered but unacknowledged at
detach is the consumer's to have persisted or to forfeit, as at a crash) and
it is not yet built.

Two consumer-visible answers describe one durable outcome: an acknowledgement
that lands after the pre-retry discard is refused as an unknown publication,
one a moment earlier is answered `Ok` with the row withheld. In both cases
nothing became durable and the consumer's correct response is the same
re-read from its last durable checkpoint.

Pinned end to end in `tests/backfill_lane_flow_control.rs` by the mid-walk
replacement, the open-ended retry, the loss after the marker was published
and after it was durable, the incomplete positional walk across a detach, the
withheld marker not abandoning the account's other scopes, the runner's own
sentinel-only recording, the ack-time refusal reopening the incarnation and
the next attach, the loss with no walk running settled by detach, the detach
between a loss and the walk's end, the page acknowledgement replayed across a
detach, the walk-end drain deleting only its own scope's rows, and pages lost
to a lag withholding the marker. Pinned at the writer in `engine/tests.rs` by
the discard fence, its confinement to the backfill lane, the marker retried
after a store failure and after a loss, the marker replayed after reattach in
both directions, the failed and the ceilinged discard leaving the request
outstanding, the stale marker leaving the retry's pages alone and a later
attempt's rows alone, a completion marker acknowledged with no publication
being withheld where a page is not, and open debt not masking the discard; at
the scan by the departed incarnation's residue, the loss after the checks and
the loss after a failed attempt; and at the ledger and gate by the watermark
tests, a prior attachment's replay not being answered from this ledger's
watermarks, and the abandonment's per-page recording. Each ablation named in those
tests' doc comments was applied, observed to fail the test, and restored.

`LiveSupersedes` is the ring-evicting `(VecDeque + HashSet)` set
`BackfillRunner::run_partition` filters each inventory page through,
via `filter_inventory_supersedes`. It filters removal ids as well as entries:
a newer live re-add supersedes a stale inventory tombstone, which otherwise
could erase the object after that re-add. When one id appears in both lanes of
the same page, both observations are suppressed. Default cap
`LIVE_SUPERSEDES_DEFAULT_CAP = 100_000`; overflow drops the oldest
insertion.

**Nothing populates it, and that is deliberate.** No production
path calls `add`, so the filter is a no-op and every inventory
entry and removal is forwarded. Cold-start can therefore re-emit a `Created`
for an object the live stream already announced. That duplicate is
accepted: consumers must already tolerate a repeated `Created`,
because a crash mid-plan re-walks every partition and re-emits
consumer-acked pages.

Populating it from `drive_changes_stream` looks obvious and is
wrong, for two independent reasons:

- **Broadcasting is not receiving.** The per-account channel is a
  `tokio::broadcast`. The slot's sentinel receiver makes `send`
  report success with no consumer attached, a subscriber that
  attaches later starts at the ring's tail, and a lagging
  subscriber has values overwritten out from under it. `ChangesReceiver`
  surfaces the skipped count but the crate still cannot replay those
  entries. Recording on `send`
  records ids the consumer will never see, and suppressing the
  inventory copy of one of those loses that object for the session,
  since the in-memory cursor has already advanced.
- **Selection and publication are separate operations.** The runner
  decides to keep an entry and broadcasts it later; the live feed
  records and broadcasts separately. These are distinct spawned
  tasks on a multi-threaded runtime, so they run genuinely in
  parallel - the absence of an await between the runner's `take` and
  its `send` does not close the window. A mutex makes each `add` and
  `take` atomic but does nothing for the compound take-then-publish,
  which is what the guarantee needs.

The only sound record trigger the engine has is the consumer ack:
`ack_checkpoint` is what this crate already treats as proof of
receipt, precisely because broadcast delivery is not. A correct
design buffers ids as pending keyed by their batch's checkpoint and
promotes them only on ack, with a policy for batches carrying no
checkpoint and a bound on the pending buffer. It would also need
take-and-publish to be one indivisible decision on both sides, and
a test-only seam to prove it - the window has no scheduling point,
so no mock server can stage the interleaving black-box.

`take` is O(1): it removes from the membership set only and leaves
a tombstone slot in the eviction ring, so a future sound producer
does not make cold-start quadratic. A tombstone cannot cause a
wrong suppression - suppression reads the membership set, which
only `add` writes - and eviction reclaims tombstone slots as they
reach the front, so membership reconverges on `cap` with no scan.

`BackfillCheckpointWriter` (in `backfill/checkpoint.rs`) is a
typed handle onto the attached account's single writer. The runner no longer
persists (the ack writer does), so the wrapper is unused on the hot path today;
consumers obtain one through `SyncEngine::backfill_checkpoint_writer` when they
need the published helper, or build one over a bare store with
`BackfillCheckpointTarget::direct`.

Partition SEQUENCING is not the orchestrator's; `ScopeWalkDriver` in
`backfill/scope_walk.rs` owns it, so a barrier stops the enclosing scope walk
rather than only the partition that hit it. See "A barrier taints the walk".

Contract-violation terminations from live changes drives use the same account
and scope generation fence as ordinary publications. A malformed batch or
checkpoint returned by an obsolete handle produces neither a termination
broadcast nor a recovery outcome for the replacement.

## Push

`InvalidationSinkInner` maps each account to an ordered ingress sender
(or a direct bounded sender off-runtime), plus a drop counter. `attach` calls
`register(account, watch_tx)` with the per-account bounded mpsc
(capacity `MultiplexerConfig::watch_capacity`, default 256) that
the reconciler reads; `detach` calls `unregister`. The drop
counter is exposed as `bifrost_sync_push_dropped_total`.

Implements `bifrost_types::InvalidationSink::push`:
`try_send` first; on `Full`, increment the drop counter only for
coalescible invalidations and connection-health transitions. They are
coalesced into one `HintPayload::Unknown` send with a 100ms bound. If the
reconciler stays busy past that deadline, the reconcile request itself is
lost until a later hint or poll; the counter therefore measures delivery
loss as well as payload coarsening. Connection-health transitions can also
lose their warning when replaced by an invalidation.
`Terminated` and `Warning` are lossless control information on the runtime
forwarder path. Each
registration made on a Tokio runtime creates one ordered forwarder on that
runtime and stores its unbounded ingress sender, so calls from receiver
threads outside Tokio do not depend on whichever account registered first
and later events cannot bypass an event waiting for bounded queue space. A
registration made outside Tokio is explicitly a direct, non-waiting fallback;
a full queue drops and counts every event. `Closed` (account detached
mid-push) is silently ignored.

The ingress queue remains unbounded. A sticky full-reconcile bit would bound
invalidation storage and avoid the timeout loss, but cannot retain arbitrary
warning payloads, termination classifications, or their ordering. Bounding
this synchronous sink's entire mixed event ingress requires an explicit
control-event overflow policy or a backpressure-capable API. The bounded
`watch_capacity` applies to the reconciler channel, not total sink storage.

The in-process push forwarder spawned in `attach` runs the same
overflow policy against its per-account `tx`: redundant invalidations
collapse to an `Unknown` reconcile request, while warnings and
terminations retain their original classification.

`push::reconciler` receives `WatchEvent::Invalidated { hint }`,
calls `scopes_for_hint(&cursors, &hint.payload)` (which consults
the membership index populated at attach), then drives
`changes_stream` to completion for each affected scope.

A drive that returns an ENGINE-INTERNAL error is logged for that scope and does
not abandon the remaining scopes of the hint. A drive that returns
`Error::Account` is NOT: an account error already carries a derived recovery
verdict, so it is normalized onto the `Terminated(err)` path below, exactly as
the poll loop's `handle_drive_outcome` does. Logging it instead would leave the
offending cursor installed and stall the scope until an unrelated poll happened
to reproduce the failure. The two properties hold together: the sweep survives
one scope's failure, and the failing scope still reaches recovery.

On
`Terminated(err)`, the reconciler routes through
`crate::recovery::plan_recovery` and forwards
`RecoveryPlan::Engine(directive)` to the slot's reopen channel
carrying the original account error. `Retry(advice)` and
`Reconcile(advice)` record their deadline in the shared throttle bucket and
then SKIP that scope, continuing the sweep. They deliberately do NOT sleep the
delay out: this is one task for the whole account, so a provider `Retry-After`
of minutes naming one scope froze every other scope's push reconciliation for
that long while the watch channel filled and coalesced behind it. The sleep was
redundant with the bucket, which every drive path consults - including the top
of each scope in this sweep, under a shutdown select. Pinned by
`a_throttled_scope_does_not_freeze_the_account_wide_sweep`.

`directive_target_scope` IS the blast radius, and the sweep obeys it:
`Some(scope)` names one scope to reset, so the hint's remaining scopes are
still swept after the handoff, while `None` (`RestartAccount`,
`SchemaIncompatible`, `OperatorOverrideRequired`) is account-wide and ends the
sweep, because driving further scopes into cursors the engine is about to
discard only burns wire calls.

`Disconnected` emits an account-scoped
`WarningKind::Other` with message "push transport disconnected".
`Reconnected` emits no warning and triggers a full
`HintPayload::Unknown` reconcile whose synthetic source is
`PushSource::Coalesced`.

`WatchEvent::Terminated(AccountError)` (push streams that classify
their own exit) flows through the same `plan_recovery` dispatch:
engine directives route through the reopen channel, terminal
classes broadcast `SyncEvent::Terminated(err)` so consumers observe
the structured error, and retryable / reconcilable verdicts emit a
warning while the in-process forwarder reconnects on its next
iteration.

`SubscriptionRegistry` (per-engine `DashMap<AccountId,
Vec<RegisteredSubscription>>`) stashes each returned handle together
with only the scopes in `PushSubscription.outcomes`' succeeded lane. Failed
scopes remain polling-only and are never recorded as covered. The validated
three-lane outcome accounts for every requested scope, and an all-rejected
request returns no handle. That accepted-scope snapshot lets account
reopen recreate subscriptions against the replacement topology.
The consumer can later call
`SyncEngine::unsubscribe_push(account)` to walk the handles back
through `Account::push_unsubscribe`. A failed teardown is returned to
the caller and its registry record is retained, while successful records
are retired; a later call retries only the failed handles. This composes
with Graph's retained server ids after a failed DELETE.
`SyncEngine::subscribe_push` is the engine-side entry that returns the
per-scope result and records its handle only when at least one scope succeeded.
Reopen likewise recreates and records only accepted scopes. Both entries
serialize on the slot's reopen lock:
reattach snapshots the registry, tears old handles down, and installs
the replacement set wholesale, so a registration or teardown landing
inside that window would either be silently erased - orphaning a
server-side subscription whose handle is connection-local state on
Graph and IMAP - or resurrect records the consumer just tore down.
The lock means both calls queue behind an active open/swap attempt rather
than racing it. A reopen queued behind `Pause` does not hold the lock
while it waits for `Run`, so `unsubscribe_push` remains available for
shutdown cleanup during an indefinitely paused account.

`open_replacement` also consults the slot's SHUTDOWN TOKEN on both sides of
`factory.open()`, and answers `ReplacementOpen::Detached` when it is cancelled -
closing the replacement it just opened before returning. `detach` deliberately
does not wait for consumer-driven activity, so nothing else excludes a public
`SyncEngine::reattach` whose `begin_activity()` landed just before detach flipped
the boundary to `Stop`: the open proceeds while detach removes the slot, awaits
the workers, and closes the old handle. With unchanged topology the reattach
establishes no cursor and recreates no subscription - the two paths that would
touch the dead writer channel and abort - so it used to run to completion, swap
the replacement into the orphaned slot, and close the already-closed previous
handle, leaving the replacement connection with no owner and never closed. The
public entry reports `AccountNotAttached`; `restart_account` simply stops.
Pinned by
`a_reattach_racing_detach_closes_its_replacement_instead_of_leaking_it`.

That property is structural rather than remembered. `open_replacement`
is the single acquisition site for an open/swap, taking the guard after
its activity registration and returning it in its `Ok` tuple so it lives
exactly as long as the open plus the `reattach_account` that consumes
it. Callers wait for the boundary to read `Run` before calling in and
hold nothing while they wait, so "hold the reopen lock across a pause"
is not a shape a caller can write. `handle_engine_directive`'s other
directives are bounded scope-local repairs that must not interleave with
a swap, so `handle_account_error` still takes the lock around them - and
skips it for `RestartAccount` precisely because that path reaches
`open_replacement`; reintroducing it there deadlocks loudly instead of
regressing quietly.

The scope repair (`RestartScope`, `DowngradeCapabilityForScope`, and the
trailing repair of `DowngradeStrategy`) deletes the scope's in-memory and
durable cursor and then re-establishes it, and holds the reopen lock throughout,
so it may not wait for `Run` either. It registers activity first
(`begin_scope_repair`) and, if the account is paused or pausing, DECLINES: it
deletes nothing and returns. The failing cursor stays in the registry, the scope
stays polled, and the poll raises the directive again after resume. Before this
the repair deleted first and the establishment's own registration was refused
under the pause, which stranded the scope for good: the multiplexer only polls
scopes still in the cursor registry, so nothing raised the directive again. Once
the repair has started it holds that one registration from before the delete
through the establishment, so a pause waits for a bounded repair rather than
landing inside it. The establishment underneath registers activity of its own
(`run_establish`, and the inventory walk it may run), and a second registration
would be judged against the boundary the pause just flipped; the repair therefore
runs them on `SyncControl::admitted_by(&activity)`, a clone whose registrations
succeed because the guard it was derived from is still counted. The handle is
passed whole, not dropped, because the inventory walk also publishes its
checkpoints through it. Pinned by
`a_repair_reaching_a_paused_account_leaves_the_scope_for_the_poll_to_re_raise`
and `a_pause_landing_mid_repair_waits_for_it_instead_of_stranding_the_scope`.

Schema recovery follows the same discipline for the whole account: it registers
activity before deleting any cursor, declines outright on a paused account (every
cursor stays registered, so the poll raises the directive again after resume),
and holds the registration through the last re-establishment, run on
`admitted_by`. It deletes every scope before re-establishing any, so a pause
landing inside it used to strand all of them. Pinned by
`schema_recovery_reaching_a_paused_account_deletes_nothing_and_runs_after_resume`
and `a_pause_landing_mid_schema_recovery_waits_for_it_instead_of_stranding_scopes`.

A `Created` lifecycle scope has no cursor, so a repair declined under a pause has
nothing for the poll to re-raise the directive from. `handle_account_error`
returns the scope of a declined `RestartScope` that has no registered cursor, and
`SlotContext::recover` (the reopen listener and the deferred-inventory worker
both dispatch through it) parks it in the slot's `DeferredScopeRepairs`. Each
parked scope is a small task that waits for the boundary to read `Run`, then
sends the same synthesized restart directive back through the slot's reopen
channel, so the repair re-runs through the ordinary dispatch, including the
decline check should the account be paused again. It is a task, not a wait in
the listener, because the listener serializes real directives and an unbounded
pause would hold `RestartAccount` behind it. A scope parks at most once, the task
skips a scope some other path established meanwhile (restarting a live cursor
would reset healthy state), and it leaves through the shutdown token, so detach
never waits on one. Pinned by
`a_created_scope_declined_under_a_pause_is_established_after_resume` and
`a_parked_scope_is_dropped_by_detach`.

Two entry points sit one layer apart and their names read as near
anagrams of each other, so it is worth naming the difference:
`Account::push_unsubscribe(handle)` is the protocol-crate trait method
and destroys ONE subscription; `SyncEngine::unsubscribe_push(account)`
is the engine method that takes the account's registry records and
calls the trait method once per record. An application holds an engine,
not an `Account`, so an application calls the second.

`detach` drops the detaching incarnation's registry records. The handles
in the registry belong to that incarnation (on IMAP and JMAP they are
connection-local outright), so carrying them across a detach would let a
later attach of the same `AccountId` inherit handles minted by a dead
connection and present them to the provider as live. Dropping loses nothing retryable: after detach, `unsubscribe_push`
rejects with `AccountNotAttached` and reopen only runs on an attached
slot, so nothing could have reached those records anyway.

Detach is therefore the last point at which server-side teardown is
possible, and plain `detach` deliberately does NOT perform it - the contract
leaves that with the consumer, because push delivers to a consumer-owned
endpoint and an application that shuts down wanting events to queue for
its next start is a legitimate pattern that unconditional teardown would
break silently. A detach with records still registered means
`unsubscribe_push` was never called, so the provider holds live
subscriptions until it expires them itself (24h for Graph); that case is
logged on `bifrost.sync.push` rather than absorbed.

The other intent has its own door: `SyncEngine::detach_with_teardown` runs
`unsubscribe_push` and then `detach`, so a consumer that is done with the
account states it once rather than remembering a call inside a window that
closes silently (xc-2, ruled 2026-09-07: an explicit opt-in, leaving plain
`detach` unchanged; unconditional teardown was rejected for breaking the
queue-for-later pattern silently, and a warning alone still left the consumer
with nothing to act on). The detach runs whether or not the teardown succeeded
- a subscription the provider refused to delete is retained
as an orphan (`teardown_unconfirmed`, not `desired`) and then dropped as stranded, with the same log line -
and the teardown error is returned unless the detach itself failed. Pinned by
`detach_with_teardown_tears_push_subscriptions_down_before_detaching`, beside
`detach_drops_push_records_so_a_reattach_cannot_reuse_dead_handles`, which
pins that plain `detach` still does not tear down.

Two races with detach DO tear down, because what they would otherwise leave is
a live subscription no handle anywhere names (ruled 2026-10-04):

- A `subscribe_push` whose registration lands after detach took the registry
  deletes the subscription it just created and returns `AccountNotAttached`.
  The caller is told the call failed and never holds the handle, so this is
  not the queue-for-later pattern, which is a consumer's deliberate keep of a
  subscription it knowingly made.
- A reattach whose registry install lands after detach's take tears the
  replacement's subscriptions down and still returns `Ok`: the swap committed
  before detach ended the account. Accepted inconsistency: for a provider whose
  subscription outlives `Account::close()`, a clean detach would have kept the
  consumer's subscriptions alive for queue-for-later delivery, and this race
  deletes them. Graph's `close()` deletes its webhook rows anyway and JMAP and
  IMAP push die with the connection, so the two paths differ only for such a
  provider, and only when the race hits. The old handles were already torn
  down earlier in the same reattach, so keeping the replacements would have
  meant live subscriptions with no handle anywhere.

## Mutation pipeline

All three bulk operations share `run_bulk_pipeline`. `BulkPipelineOp`
selects `SetFlags`, `Move`, or `Destroy`, which determines the wire
method and final read-back guard. Campaign flow:

1. Deduplicate targets and group them by the canonical set of throttle keys
   declared for each singleton target. Moves include the source and
   destination membership declarations. Every target belongs to exactly one
   cohort.

2. Allocate each cohort's idempotency key once through `IdempotencyVendor`.
   Retain that key across all retries. Re-declare the actual submitted set
   before every submission. A never-submitted target whose singleton
   declaration changes is re-routed; submitted and uncertain targets retain
   their cohort and execution identity.

3. Schedule one ready action at a time: a submission, retry, or read-back
   for one cohort. Save its state after the action and return to the
   scheduler. Readiness combines the current declared-key holds, retry
   deadline, and local reconcile deadline. When every action is held,
   sleep until the earliest readiness, selecting on shutdown. A replacement
   account wakes the scheduler to re-evaluate declarations.

   Admission remains mandatory. Refused admission delays the cohort by 100 ms
   without spending a submission or retry slot; ready siblings can proceed.
   Re-check declarations and holds against
   the account snapshot used after admission; release admission and return
   to the scheduler if readiness changed. Hold waits consume no attempts.

   `mutation_max_retries` counts resubmissions independently for each cohort,
   excluding its initial submission (zero allows only the initial write).
   `MutationConfig::retry_queue_cap` is one campaign-wide retry-target
   allowance. A target reserves a slot on its first scheduled retry and
   retains it for later retries. Excess targets remain `PendingRetry`
   and enter read-back rather than disappearing from accounting. Slots
   are reserved when retries are scheduled, before readiness waits, so a
   held cohort can consume slots before another cohort retries.

4. Read back unresolved submitted objects per cohort. Observations return
   per-object results and merge into the campaign's outcome map.
   Matching objects become `Skipped`; observed mismatches remain
   `PendingRetry` for a later consumer campaign, even after the retry budget
   or retry-target allowance is exhausted. An observation cannot manufacture
   a terminal provider failure. Failed or missing observations remain explicitly
   unresolved. Partial observations survive a stream termination.
   Final object counters derive from the merged map.

5. Preserve the first error and stop further mutation submissions.
   Submitted cohorts with unresolved work receive at most one read-back
   attempt while the account remains running and attached. Shutdown,
   detach, pause, or observation failure leaves submitted work explicitly
   unresolved without an observation retry loop. Never-submitted targets
   become known-unsent `PendingRetry` and are excluded from read-back.
   Return counters after cleanup, with the first error in
   `MutationCounters::error`. Only preflight failures, such as an unattached
   account, return `Err`; once a campaign starts, partial accounting survives
   every exit. The error is shared through `Arc<Error>`, so the counters remain
   cloneable.

An engine directive also stops further submissions. Remaining retry work
becomes `BlockedByEngine`, while `PendingReadback` remains protected:
uncertain writes are observed rather than replayed or guessed at.
Forwarded directives are deduplicated across the campaign by directive
variant and target scope.

`ItemOutcome::Succeeded` buckets by its `MutationSuccess` payload:
`Applied` -> `applied`, `Skipped` -> `skipped`, and `Downgraded { actual }`
-> `downgraded`. Each downgraded object's identity and reported effect are
retained in `MutationCounters::effects`. Like an applied success, the weaker
successful effect is taken from the provider report. It does not assert that
the original request was satisfied. Downgrades are neither replayed nor
hydrated against the original request: trash membership cannot satisfy object
absence, and unsupported flags cannot satisfy the original flag operation.
Unknown future success variants remain protected in the read-back lane.

`MutationCounters` buckets `ItemOutcome::Failed` outcomes via
`crate::recovery::plan_recovery`:
- `Retry { SameRequest | AfterAuthRefresh }` -> `pending_retry`.
- `Retry { AfterStateRefresh }` and
  `Reconcile { CheckTarget }` -> `pending_retry` then the
  read-back guard reclassifies into `skipped` or leaves the item pending.
  A mismatch does not trigger automatic replay: reconcile guidance can also
  require consumer-side dedupe.
- `Reconcile { DedupeByClientId }` increments
  `dedupe_by_client_id` and broadcasts a
  `Warning::OperatorAttentionNeeded` (the dedupe lives at the
  consumer because the client-id space is theirs).
- `Engine(_)` -> `blocked_by_engine` AND the directive is
  forwarded through `ReopenRequest::Recovery` so the engine's
  recovery dispatch performs the restart / downgrade / schema
  clear. `blocked_by_engine` is distinct from `failed_terminal`:
  the campaign was halted by the engine, not by per-item
  terminal classification. Forwarding is deduped per campaign by
  directive identity - the variant plus its target scope - not by the
  target alone: a 500-item batch whose items all name one directive
  costs one reopen request, while a mixed batch still delivers each
  distinct directive. Keying on the target alone would collapse every
  account-wide directive onto `None` (the first `RestartAccount`
  swallowing a later `OperatorOverrideRequired` or schema reset) and
  every same-folder directive onto that folder (a `RestartScope`
  swallowing a later `DisableScope`).
- terminal recovery -> `failed_terminal`.

`ItemOutcome::Uncertain` always queues for read-back so a
non-idempotent transport drop never replays blindly.

`SyncEngine::bulk_move` and `SyncEngine::bulk_move_from` use the move
arm, which submits through
`Account::bulk_move_from(targets, destination, source, key)` and
`bulk_move` is exactly `source: None`. `source` exists for request
count, not semantics: the folder-model providers vacate the source as
part of the move itself, so the trait's default impl forwards to
`bulk_move` and drops it, while Gmail folds it into the same
`batchModify` and thereby removes the O(n) `remove_from_container`
workaround. The read-back guard is unchanged either way - it
reconciles membership of `destination`, the property that says the
move landed, and does not separately re-verify absence from `source`.

`IdempotencyKey` is `{ run_id, sequence, protocol_salt }`. `run_id`
is consumer-minted and consumer-persisted across process restarts
so retries from a previous process correlate. `IdempotencyVendor` does not
persist it; the consumer supplies the durable value.

Before every wire action, including read-back, the scheduler waits through
`SyncControl::wait_until_running` and registers an activity guard.
A pause that wins registration parks normal campaign work without
changing its attempt count or frozen key. In-flight mutation accounting
finishes under its activity guard. During error cleanup, a pause leaves
remaining submitted work unresolved instead of parking cleanup.
Detach cancellation interrupts admission, readiness waits, mutation
streams, and read-back and records `Error::ShuttingDown` in
`MutationCounters::error` unless an earlier error is already being preserved.

Consumers must still bound campaign input when retaining every target and
outcome is too costly. `retry_queue_cap` bounds the campaign-wide retry
target allowance, not initial input or unresolved outcome retention.

`mutation::fanout::partition_by_account` is a cross-account
fanout helper: given an input stream of `(AccountId, T)` and a
prebuilt `HashMap<AccountId, mpsc::Sender<T>>`, it spawns a task
that routes each item to the matching sender, dropping items
addressed to unattached accounts.

`MutationConfig` (`fanout_buffer = 256`, `retry_queue_cap =
4096`) lives on `EngineConfig::mutation`. `PushConfig` holds
push-only knobs (the per-account `WatchEvent` mpsc capacity lives
on `MultiplexerConfig::watch_capacity`): today
`orphan_teardown_retry_interval`, default 15 minutes, `None` to
disable. Each attached account runs an `OrphanTeardownRetrier`
worker that, on that interval, retries `push_unsubscribe` for
every pure orphan in the subscription registry
(`teardown_unconfirmed` and not `desired`) and drops the record on
success. It does not take the slot's reopen lock: it never takes
records out of the registry, so racing a reopen or
`unsubscribe_push` costs at most one redundant, idempotent retry,
whereas holding the lock let a stalled provider call block every
reattach and consumer push call on the account. A paused account
skips its passes until resumed. A `desired` record is never
touched, even when its teardown is unconfirmed: that is an
aborted reopen's live handle. Without the timer, an orphan on an
account that never reopens waited for the consumer's next
`unsubscribe_push` or the provider's expiry.

## Hydration passthrough

The engine broadcast is id-only. A live `Change` already carries
`{ id, kind }`; both `InventoryFusion` and `BackfillRunner` deliberately
down-convert each `InventoryEntry` to
`Change::ObjectChange { id, Created }`. Fingerprints, threading headers,
and inventory memberships do not reach consumers through
`MultiplexerEvent`. A consumer that turns those signals into real rows
must call `get_stream`, normally with `Projection::Metadata` first and
then a fuller projection as needed. This repeats metadata the protocol
may already have fetched during cold start, but keeps one event
vocabulary and is the explicit v1 design.
The `Account` handle that can do so lives behind `slot.current`
(`ArcSwap<Arc<dyn Account>>`) and is otherwise private, so `SyncEngine`
exposes a read-only passthrough cluster as the consumer's single door
to hydration:

- `get_stream(account, ids, projection)` -> `Account::get_stream`. The
  primary entry: streams `ItemOutcome<HydratedObject>` for a stream of
  ids at a chosen `Projection`.
- `message_hydrate(account, id, projection)` -> `Account::message_hydrate`.
  One parsed `Message` at a `HydrationProjection`.
- `thread_hydrate(account, thread)` -> `Account::thread_hydrate`.
- `open_raw_rfc822(account, id)` -> `Account::open_raw_rfc822`. Verbatim
  server-assembled MIME octets.
- `open_blob(account, handle)` / `open_blob_range(account, handle, range)`
  -> the matching `Account` blob openers.

Two deliberate properties:

- **Always-live handle.** Every method resolves through the private
  `live_account` helper, i.e. `ArcSwap::load_full`, so a hydrate issued
  after a `RestartAccount` reopen runs against the freshly-installed
  connection, never a stale snapshot the consumer cached. This is the
  same discipline the spawned workers follow on their hot paths. The
  forwarded methods return `'static` streams / futures that capture
  their own internal `Arc` clones, so they outlive the short-lived
  handle resolved per call; an unattached account yields
  `Error::AccountNotAttached` up front.
- **Read surface only.** Mutations stay funnelled through
  `bulk_set_flags` so the idempotency / read-back / recovery pipeline
  remains the one chokepoint for writes; cursor and push driving stay
  engine-owned. Handing out a raw `Arc<dyn Account>` would leak both the
  write surface and the reopen-snapshot discipline, so the engine does
  not - it forwards the read methods explicitly instead.

These consumer-driven calls do not pass through the `Scheduler` /
`BudgetGate` - unlike the engine's own wire paths, which are all
admitted (see "Scheduler + budget" below). That asymmetry is
deliberate: the budget bounds what the ENGINE spends on an account's
behalf, while a consumer-initiated hydrate is the consumer spending
its own latency budget. Consumer-driven hydration and engine-driven
backfill still share the same underlying client, where `bifrost-net`
is the rate-limit chokepoint.

Alongside the read-only hydration cluster, the engine forwards sibling
**direct passthrough** clusters that resolve through the same
`live_account` discipline and forward 1:1 to the matching `Account`
method, inventing no new semantics: object-level mutation conveniences,
compose / draft, container read + CRUD, the **contact** cluster
(`address_books_list`, `contacts_list`, `contact_get`, `contact_create`,
`contact_update`, `contact_delete`, `directory_search`,
`directory_groups_list`, `directory_group_expand`), the
**server-filter** cluster (`filters_list`, `filter_create`,
`filter_update`, `filter_delete`, `filter_validate`) - whose supported
model a consumer reads off `capabilities().filter_rule_shape` and
`capabilities().pim_methods` before dispatching - and the **settings**
cluster (`identities_list`, `identity_update`, `vacation_get`,
`vacation_set`, `quota_get`), whose per-method support a consumer reads
off `capabilities().pim_methods`. Each mirrors the
`Account` trait's argument shapes but takes `account_id: &AccountId` and
returns the engine `Error` (the trait's `AccountError` folds in through
`?`); an unattached account yields `Error::AccountNotAttached` up front.
These are single-op conveniences and reads, so - like the container /
compose clusters - they deliberately bypass the idempotency / read-back /
recovery pipeline that guards the volume mutations.

## Scheduler + budget

**`Scheduler`, `ConcurrencyBudget`, `SchedulerConfig`, `MutationConfig`,
`LiveSupersedes`, `BackfillCheckpointWriter` and `mutation::fanout` stay. Do not
delete them.** All seven were removed in one commit on the reasoning that nothing
in this workspace wired them, and restored at the repository owner's instruction.
This is a library crate: its consumers are outside the workspace by definition,
so a workspace-wide grep establishes nothing about who uses a published item, and
"documented as deliberately unwired, with no dated plan" describes somebody's
plan rather than evidence of abandonment. Where one of these reads as unfinished,
the fix is to finish it rather than drop the published surface. Removing or
renaming any published item here is the
owner's call; the standing lessons in `AGENTS.md` carry the full account.

`Scheduler` is a strict-priority gate (not an executor) with four
lanes: `Foreground` / `Normal` / `Background` / `Bulk`. Starvation
guard forces a lower-lane pull after a configurable threshold of
consecutive higher-lane pulls (`SchedulerConfig::starvation_floor`,
default 64). A configured value of zero is floored to one, so lower lanes do
not leapfrog higher-priority work on every pull.

Lane selection combines account priority and work kind for both admission
and submitted work items. Live polls, push reconciliation and mutations retain
the account lane. Backfill partitions, backfill barrier walks and deferred
inventory use `WorkKind::Backfill`: Foreground and Normal accounts run this
work in Background; Background and Bulk accounts run it in Bulk. Live sync
and backfill share the sync budget class, so an older backfill waiter cannot
reserve its own acquisition ahead of a later live poll. The starvation guard
still applies, and running work is not interrupted.

`LaneQueue` is bounded (default 1024 items, configurable via
`EngineConfig::lane_capacity`). Standalone queues can select either
`LaneShedPolicy`: `DropOldest` evicts the head and `DropNewest` rejects the
incoming item. Both increment the queue-local counter and emit a `warn!`.
Production scheduler work-item queues use `DropOldest`; `EngineConfig` does not
expose a selector. The scheduler keeps those queues private and exposes their
depths, not their shed counters. The same capacity bounds the separate
admission queues used by engine work; admission overflow refuses the incoming
request and does not increment a shed counter.

`BudgetGate` exposes two semaphores per account (sync + mutation)
plus global caps. Acquisition takes the per-account permit first and
the global permit second, so a waiter parked on a busy account cannot
consume global capacity needed by another account. The account pools,
registration state and forgotten-account tombstones share one mutex. This
keeps lazy creation, registration and forgetting ordered: registering an
active account preserves its pools, forgetting retires them and cancels pending
acquisitions, and a later attach explicitly creates fresh pools. A tombstone
prevents a stray acquire after detach from lazily recreating the account's
semaphores. `ConcurrencyBudget::validate` requires `per_account >= 2`
and a mutation share that leaves at least one real sync permit; the
gate no longer masks a zero split by granting an extra permit. Its public
constructor still floors the global semaphore at one permit, because direct
construction bypasses builder validation and must not create a permanently
blocked gate from `global: 0`.

`Scheduler::pull` stays synchronous and non-blocking, returning
`Option<WorkItem>`, so a consumer can ask whether work is available without
committing to a wait; `pull_next().await` is the waiting form and wakes
directly from `submit`, and `try_pull` is a name-symmetric alias of `pull`.

**Admission is a separate path from `submit`/`pull`.** `Scheduler::admit`
queues the request in its own four priority lanes and a single dispatcher task
grants it a `BudgetPermit`, held for the complete protocol operation. The
ordering constraint is the whole point: a request is dequeued only when its
budget can actually be granted, so nothing ever parks on the semaphore itself.
Two properties follow, and neither survives an implementation that hands the
waiter to `BudgetGate::acquire` directly:

- A blocked request counts against `lane_capacity` for its entire wait.
  Requests parked in the semaphore's queue are invisible to the lane bound,
  which makes the bound vacuous.
- A `Foreground` request that arrives after a queued `Background` one still
  runs first. The semaphore's own order is arrival order, so preemption is
  lost the moment a request reaches it.

The dispatcher races one acquisition per distinct `(account, kind)` class
present in the lanes rather than only the head class. A single-head dispatcher
blocks the whole engine behind one account whose per-account sub-pool is
exhausted, which is exactly the cross-account starvation the per-account layer
exists to prevent. It re-plans when a strictly more preferred lane gains work
or when a class appears that it is not already racing; an arrival that changes
neither does not restart the in-flight acquisitions. `admission_snapshot`
reports the waiting depths.

Every wire path is admitted: poll and push change drives, backfill partitions,
the backfill operator-barrier writer query (admitted separately, so a large
scope never holds one sync permit across its whole multi-partition walk),
mutation attempts, the mutation READ-BACK guard (which runs after the attempt
loop released the campaign's permit and would otherwise hydrate unbudgeted),
and deferred inventory fusion. Admission for a change drive happens before
`CursorRegistry::with_drive`, so a queued task never owns a scope lease, and
the permit is released before recovery handoff and cadence sleeps.

`admit` returns `Error::Other` when its lane is at capacity - the incoming
request is refused rather than displacing an older waiter. Repeating work paths
treat that as transient: the poll loop backs off one cadence step and re-enters
rather than retiring the scope, and the push sweep skips that scope only, which
is the same blast radius rule the sweep uses for a failed drive.

`Scheduler`, `BudgetGate`, and their configuration are publicly re-exported now
that the engine uses them, and `SyncEngine::scheduler()` hands out the live
handle.

## Control

`SyncControl` carries the priority hint, bandwidth-observed
counter, pause/resume token, and the boundary channel.
`pause().await` and `checkpoint_now().await` return
`Result<DurableCheckpointSet, AccountError>`. The set contains one latest
durable checkpoint per change scope and per backfill partition, and is empty
when an idle account has never produced one. Older acknowledgements cannot
replace a newer snapshot in the same lane. `DurableCheckpointSet::new`
normalizes to one entry per lane, keeping the last, so a repeated lane is not
representable; equality is order-insensitive and symmetric, which a one-way
containment check over a duplicate-bearing vector is not. `SyncControl` tracks active stream
operations plus every broadcast
checkpoint awaiting a consumer ack. A boundary waiter resolves only
when activity reaches zero and that pending set is empty. This gives an
idle account a completion source without claiming safety while a batch
is still in flight.

Every producer registers its checkpoint and its coverage claim as ONE
publication (`SyncControl::publish_checkpoint`) BEFORE broadcasting the
batch, and retracts it with `retire_publication` when the send reached
only the slot's sentinel receiver. Registering after the send would let
a consumer that acks in that window strand an entry no ack can match,
and registering the two halves separately would let a publication exist
with only one of them.

An entry leaves the pending set three ways, and every broadcast hits
one of them:

- `record_publication`, fired by the ack writer after
  `put_change_cursor` / `put_backfill` succeeds for an `AckRequest`.
  Removes the entry by PUBLICATION identity and refreshes the durable
  snapshot. (`record_checkpoint` is the value-identified form, for a
  caller that holds only a checkpoint.) Both change-cursor and backfill paths are
  consumer-ack-deferred.
- `retire_publication`, fired when the ack was processed but produced
  nothing durable (store write failed) or when no real subscriber
  received the batch. The entry stops gating waiters - it is no
  longer in flight - but the durable snapshot is NOT advanced, and
  the consumer learns of a failed write from `ack_checkpoint`'s own
  `Result`. Leaving it pending instead would wedge every later
  `pause` / `checkpoint_now` on the account for the process lifetime.
- Supersession: a newer broadcast on the same LANE replaces the older
  one, folding its coverage claim in. A lane is one scope's changes
  stream, or one backfill partition of one scope. The shared per-scope
  drive lease makes polling and push reconciliation one sequential
  producer, while `ScopeToken` generation matching prevents duplicate
  poll tasks, so acking the newest checkpoint proves the earlier ones
  from that producer are durable too. Sibling backfill partitions are
  deliberately NOT one lane: they run concurrently and neither subsumes
  the other. This bounds the set by the account's scope and partition
  count instead of by how many batches a consumer left unacked, and lets
  a consumer that acks coarsely (persist N batches, ack the last
  checkpoint) still reach a boundary. `record_publication` removes by
  publication identity rather than by lane or by checkpoint value, so
  acking an OLD checkpoint never retires a newer outstanding one.

The mutation pipeline records counters, not checkpoints; it does
not call `record_checkpoint`.

`bandwidth_observed` is fed by the optional bandwidth-feed task
spawned in `attach` when `SyncEngineBuilder::with_bandwidth_meter`
was set; the task polls `BandwidthMeter::account(id).observed_bps()`
once per second and calls `control.observe_bandwidth(bps)`.

`Control` itself (the trait re-exported from `bifrost-types`) is
dyn-safe; `SyncControl` is the engine's concrete implementation.
Boundary, priority, bandwidth-cap, and checkpoint watch senders use
`send_replace`, so their canonical snapshots update even when no
receiver is alive.

Boundary writes from `SyncControl` are compare-and-set, not blind
writes:

- `checkpoint_now` installs `CheckpointNow` and reads the request it
  displaced in one atomic step (`Boundary::request_checkpoint`), then
  restores that value only if the boundary still reads `CheckpointNow`
  (`Boundary::restore_if_current`). An interleaved pause, stop, or
  resume wins in both halves - neither the install nor the restore can
  clobber it. The restore runs from a drop guard, so it fires on every
  exit from the wait: the checkpoint landing, the watch channel closing
  under the waiter, and the caller dropping the future mid-await. A
  latch left installed is not cosmetic - `wait_until_running` reads
  `CheckpointNow` as not-running, so backfill, deferred inventory, and
  `restart_account` would park until some unrelated write moved the
  boundary.
- `Stop` is terminal for consumer-driven writes
  (`Boundary::set_unless_stopped`, used by `pause` / `resume`, and
  refused outright by `request_checkpoint`). A pause written over a
  detach's `Stop` parks the workers instead of letting them drain,
  and `detach` then waits out its whole timeout. `Boundary::set` is
  still the unconditional primitive the engine's own teardown uses.

## Inventory coverage

Inventory answers a question no other stream does: what did the enumeration
PROVE about the objects it walked past? The rule is

> Any accepted inventory progress checkpoint must certify that every provider
> result before that checkpoint was either materialized as an `InventoryEntry`
> or proved irrelevant to the inventory snapshot.

A checkpoint advances a cursor past everything behind it, and the changes
stream only reports SUBSEQUENT changes, so an object a walk skipped without
recording becomes permanently invisible to that account. Making the failure
visible to a consumer does not fix that; the checkpoint has to carry the
unresolved obligations with it, atomically, or not advance.

Inventory therefore has its own envelope (`InventoryEvent`, not `SyncEvent<T>`)
in which every checkpoint-bearing `InventoryBatch` and the terminal
`InventoryCompletion` carry an `InventoryCoverageReport`. Coverage rides
checkpoint-bearing BATCHES, not only the terminal completion, because Graph and
`BackfillRunner` both advance per page - waiting for `Done` would let a page
checkpoint become durable across a gap it never declared.

### Report and ledger are different things

`InventoryCoverageReport` (in `bifrost-types`) is one account's observation about
one enumeration of one region. `DebtLedger` (in `bifrost-sync`) is what the
engine has concluded after folding every ACCEPTED report, every operator
decision, and eventually every repair result together. Calling both "coverage"
hid the seam, and the seam is where the mistakes live: the account reports
evidence, the engine owns retry and acceptable-loss policy, and no protocol crate
constructs ledger state. `DebtLedger::ingest` is the single exhaustive path in.

### A report names its extent

A completeness claim is only meaningful about a stated region, so every report
carries a `CoverageDomain`: scope, a semantic `CoverageCoordinate`, and a
`SnapshotIdentity`. The coordinate is deliberately NOT the durable `Partition`
key - that key names an execution unit and indexes resume state, and says nothing
about what range of objects the unit covered. Repartitioning makes the difference
concrete: debt raised under a `30d..90d` window is covered by the union of
`7d..60d` and `60d..180d` and by neither alone, and an opaque key comparison sees
three unrelated strings.

`CoverageDomain::covers` answers conservatively, because a wrong `true`
discharges debt nothing re-read (silent loss with a proof record attached) while
a wrong `false` only leaves a scope degraded until something wider covers it. Two
rules with teeth: a UID range does not cross a UIDVALIDITY change, and PAGE
RANGES ARE INCOMPARABLE ACROSS SNAPSHOTS - page 500..1000 of a later walk may
hold entirely different objects, so repeating the coordinate proves nothing
unless both reports observed the same snapshot. Two walks that cannot name their
snapshot are never the same snapshot, which is why `SnapshotIdentity` comparison
is `same_snapshot_as` rather than `==`.

Discharge needs BOTH a generation not older than the debt's AND a covering
domain or union of domains. Generation alone stops a stale report overwriting
newer state; it proves nothing on its own, because a newer partial walk is still
partial. The ledger retains only proofs that can still contribute to an open
obligation or barrier. Proofs older than newly-raised debt cannot discharge it,
and proofs whose load-bearing debt has closed are discarded, bounding the
durable hot-path history without forgetting a partial union still in progress.

### Proof and policy are separate axes

`ProofStatus` is what the system knows (`Unresolved` / `Discharged`).
`PolicyStatus` is what it will do (`Retrying` / `OperatorBlocked` / `Waived`).

Collapsing them is the mistake the split exists to prevent. A waiver is accepted
loss, NOT proof that anything was materialized or proved irrelevant, so a waived
obligation stays `Unresolved` forever - what changes is that it stops blocking
automatic completion. Otherwise no audit, and no later walk, can tell proved
coverage from loss somebody agreed to live with.

**No local counter may produce `Waived` or `Discharged`.** A retry budget
expiring is evidence that automatic work is not helping, which is
`OperatorBlocked`: still visible, still blocking, still manually retryable. Only
an operator waives, through `SyncEngine::waive_obligation`, because only an
operator can decide what loss is acceptable. Waiver targets ONE occurrence by
`ObligationKey`; waiving a failure CLASS would authorize every future instance
sight unseen.

### `Object`, `Region`, and the barrier

`InventoryObligation` splits `Object` from `Region`. An id-less provider result
must not become an `Object` with a synthetic id: an absent id may mean one
malformed object, a schema mismatch affecting many, a truncated page, or a
response that cannot be correlated with pagination at all, so naming it a
single-object loss claims knowledge the walk does not have.

A `Region` declares its `RegionRecovery`: `DurableReplay { token }` or
`CheckpointBarrier`. Two variants, not three. A finite-horizon token - "this page
link works for another thirty minutes" - is not durable coverage, because the
engine cannot guarantee repair before a deadline across crashes, offline periods
or operator blocking; at the moment the checkpoint is considered it has exactly
the same safe disposition as no token at all. Converting a perishable
continuation into a durable artifact is the ACCOUNT's job, done before it hands
the obligation over. `CheckpointBarrier::transient_replay` is diagnostics only,
deliberately a field rather than a variant so no scheduler can read "not expired
yet" as permission to advance.

**A barrier taints the walk.** Refusing one checkpoint is not enough - the next
page's checkpoint or the terminal delta link would leap the same region. So the
batch's items are still delivered, its `PageCheckpoint::Withheld` position is
not taken, and the walk stops. A front end takes a position only through
`PageCheckpoint::accepted(crossed)`, where `crossed` is the writer's answer to
whether an operator waived every barrier, and it terminates a page or
completion whose variant disagrees with its coverage as an `Account` contract
violation. Checkpoints accepted EARLIER in that walk stand: each certifies a prefix
ending before the barrier region begins. Barriers do not become ledger debt (no
cursor advanced past them, so there is nothing durable to hang debt off); they
are recorded as `BarrierIncident`, which is what gives a restart its memory and
an operator something to waive or block. `InventoryFusion` and
`BackfillRunner` both make this decision through the shared `InventoryWalk`
state, including a barrier carried by terminal `Done`. The incident's
`resume_from` is always the last checkpoint accepted before the barrier. The
backfill front end therefore never mints or publishes a positional checkpoint
for a refused page.

**A barrier stops the SCOPE walk, not only the partition that hit it.** Every
later partition or page window would carry a checkpoint certifying a prefix that
crosses the blocked region, so stopping the partition alone reproduces the loss
one layer up. The backfill orchestrator therefore does not sequence partitions
itself: `ScopeWalkDriver` (`backfill/scope_walk.rs`) hands out the next partition
and folds each `BackfillPartitionOutcome` back in, and a walk whose outcome
carried `ScopeWalkStep::StopScopeWalk` produces no further partition. Both plan
shapes - the fixed partition set and the open-ended page walk - go through it,
and the completion sentinel is withheld for the scope.

**An incident the store refused is not an incident.** `record_barriers` returns
the writer's inner result, and BOTH front ends refuse to announce a barrier they
could not persist: the backfill partition fails (the scope stays `Pending` and
the walk is retried), and the fusion walk returns the error instead of
`NoCursor`, so establishment retries under its ordinary error contract.
Accepting the failure would announce a barrier that a restart forgets, leaving
an operator with no durable object to block or waive. A writer that is GONE
(detach, shutdown) is a different case and is not a failure - there is nothing to
persist to and nothing to retry against.

At a barrier hit, both inventory front ends ask the account writer whether
EVERY barrier in that report is waived. The writer decides against its current
ledger, not a walk-start snapshot, so a waiver or block racing the walk is
ordered by the single writer. An all-waived report is atomically converted from
barrier incidents into unresolved waived ledger entries and persisted before
the writer authorizes the crossing. One unknown or unwaived key stops the whole
report. A crossed barrier is accepted loss, never proof, and the scope may then
reach its completion sentinel.

An `OperatorBlocked` barrier parks the scope's backfill BEFORE any wire work:
the orchestrator asks the writer (`WriterRequest::ScopeBarrierBlocked`) once
per scan candidate and, when blocked, records the park through
`BackfillScan::record_attempt` exactly as a failed walk would. That is
deliberate. The check is cheap but it runs on the single account writer that
also owns every durable mutation, and the rescan tick is one second: without
recording the park, a blocked scope's retry deadline stays in the past and it
re-asks the writer every second for as long as the block stands. Recording it
puts the query on the same 5s-doubling-to-5min ramp, and a later waiver
releases the scope on the next elapsed tick.

`bifrost-graph` reports its id-less-value case as `CheckpointBarrier`. The only
token at page granularity is a continuation of that delta session, dead as soon
as the walk takes its `deltaLink`, so recording it as repairable would create
debt nothing could ever discharge - silent permanent loss wearing a declared-debt
costume.

**Elsewhere, the scope converges DEGRADED rather than never converging.** A walk
that hits an unreadable but re-readable object records it and keeps going, so the
cursor establishes and the account enters its change stream. The alternative
considered and rejected was refusing every checkpoint after the first failure,
which is safe but never converges: one permanently-broken object would cost the
mailbox its entire backfill, forever. Declared loss beats permanent
non-convergence; SILENT loss beats neither. For a barrier there is no third
option that preserves both progress and coverage, which is exactly why the
operator waiver is load-bearing rather than a nicety.

### Publication identity

Coverage reaches the durable record through the per-account `Publications`
ledger, keyed by an
engine-issued `PublicationId`. It cannot ride `Checkpoint`, which is a published
type crossing the broadcast channel and back through `ack_checkpoint`.

Keying by scope is unsound: two backfill partitions of one scope are in flight
together, and the second's report overwrites the first's. Keying by `Checkpoint`
VALUE is also unsound, because `Checkpoint: Eq` is value equality and never
promised to identify a publication:

- `BackfillRunner` counts materialized entries and explicit removals, so a page whose
  content was entirely unrepresentable increments nothing, and with no progress
  marker the next `BackfillCheckpoint` is byte-identical to its predecessor while
  describing a different boundary and a different report;
- inventory fusion publishes its final checkpoint twice, on the final batch and
  again on `Done`;
- a later walk can produce the same cursor bytes as an earlier one while proving
  different coverage.

`MultiplexerEvent::publication` therefore carries a restart-safe token to the consumer.
The token contains the immutable checkpoint lane and coverage receipt as well as
its compact numeric identity. A consumer persists the whole token beside the
batch. If the writer restarts before the acknowledgement, the new writer can
apply the exact original claim; accepting the number with an empty claim would
advance the cursor while silently discarding degraded coverage.
`ack_checkpoint` takes it back for checkpoint-bearing events;
`ack_publication` acknowledges repair events, which intentionally carry no
checkpoint because they advance no cursor. A repeated acknowledgement of the same
publication is idempotent. A token whose receipt names the wrong lane is refused
without consuming any live claim. It is never defaulted to complete coverage.

Idempotency is carried by a per-lane watermark of the highest PERSISTED
publication, not by a record per acknowledgement. Within a lane publications are
sequential and supersession folds older claims into newer ones, so a persisted id
at or above the one being retried means everything it proved is durable. That is
one entry per lane - bounded by the account's scope count - where a tombstone per
acknowledgement grew for the life of the attachment, and unlike a fixed-size ring
it never forgets: a delayed consumer retry answers correctly however many
acknowledgements have landed since.

The watermark moves AFTER the store write lands, never before. A retried
acknowledgement of a write that failed replays from its receipt; it is never
reported already persisted for a checkpoint no store accepted.

Checkpoint registration, its coverage claim, boundary gating, supersession,
lag abandonment, retirement, and claim consumption are owned by that one
ledger, under one lock. The whole registration transition - carry-forward,
supersession lookup, removal, folding, and insertion of the survivor and its
boundary - is atomic, because two backfill partitions of one scope are in flight
together by design and any gap lets both leave a live entry for one key.

A publication's LANE is its supersession key and its acknowledgement key. Lanes
are one changes stream per scope, one backfill PARTITION per scope, and one
repair lane. The partition belongs in the backfill key: sibling partitions of a
scope neither subsume nor release each other. The lane is checked on
acknowledgement, so `ack_publication` handed a checkpoint publication's id
refuses instead of consuming it - consuming it would make the later, real
`ack_checkpoint` short-circuit as already persisted and return before writing,
while the engine announced a durable boundary the store never accepted.

Boundary release and retirement identify the publication, never the checkpoint
VALUE, for the same reason the claim does: equal values are routinely different
publications, and a value search reaches into a sibling that is still in flight.

Supersession FOLDS. When a newer publication
supersedes an older outstanding one for acknowledgement purposes, the survivor
absorbs the superseded claim; dropping it would stop the control path waiting for
the older checkpoint while quietly discarding its obligations. A consumer may
still acknowledge the superseded batch - it received it, and producers run ahead
of consumers - and that acknowledgement persists its checkpoint while ingesting
nothing, because its coverage now rides the survivor. Cumulative reports
within one walk make this harmless duplication; across partitions it is the only
thing keeping partition A's debt alive when B supersedes it.

### One atomic transition

`CheckpointStore::apply_transition` is the only mutating operation, taking the
checkpoint and the resulting ledger together. They cannot be two calls: checkpoint
first loses the debt, ledger first records debt for progress that never
committed. `put_change_cursor` / `put_backfill` remain as conveniences for writes
that carry NO coverage report, and they read the ledger and write it back
unchanged - "this write says nothing about coverage" means the debt survives, and
emphatically not that coverage is complete. An earlier revision had a convenience
of exactly that shape stamping `Complete` on every write, which is how a durable
record ends up certifying coverage nothing proved.

Two mutations have no cursor to ride: a barrier incident (nothing advanced, by
definition) and an operator decision. Both go through the account writer and
persist the writer-owned ledger without a checkpoint transition.

The writer loads its authoritative ledger before dispatching its first request.
A failed load refuses ledger-dependent requests with a store error and retries
on the next request; it never substitutes an empty ledger. Cursor-only reads
remain available, and an unavailable barrier-policy query conservatively parks
work. No publication claim is consumed while the ledger is unavailable.
Operator waive and block decisions mutate a candidate ledger and adopt it only
after persistence succeeds, so a failed decision cannot leak into a later write.

Claims are applied when the CONSUMER acknowledges, never when the account emits.
An unacknowledged report may describe entries the consumer never persisted, so it
cannot prove anything. The one exception is `ingest_debt_only`, used for a
backfill partition's terminal summary, which has no checkpoint of its own: it
records obligations but never proof, because over-reporting debt costs a degraded
scope while under-reporting it costs objects nobody sees again.

### Sentinel eligibility is decided by the writer

`BackfillRunner` reports `complete` per partition, but that flag is NOT
authoritative for the durable sentinel. Debt from a sibling partition can be
acknowledged after the runner decides and before the sentinel reaches the writer,
and the sentinel makes the next attach skip the walk entirely - so writing it
over an open obligation converts a declared gap into a permanent one. The writer
therefore evaluates `DebtLedger::completion_permitted` against the ledger AS IT
STANDS at the moment it processes the sentinel acknowledgement, and withholds the
marker if the scope owes anything unwaived. The consumer's acknowledgement still
succeeds; the marker simply does not become durable, so the next attach re-walks.

A withheld sentinel is NOT reported as a durable checkpoint. `persist_ack_request`
answers `Durable` or `SentinelWithheld`, and the withheld arm skips both
`PendingCoverage::settle_checkpoint` and `SyncControl::record_publication`,
RETIRING the publication instead - the same treatment a failed store write gets.
Settling would let a retried acknowledgement answer `AlreadyPersisted` for a row
the store never accepted, and announcing would put the backfill-complete boundary
into the `DurableCheckpointSet` every later `pause` / `checkpoint_now` reports,
while the next attach re-walks the scope. Retiring still releases boundary
waiters, so nothing wedges. Nothing durable is invented; the snapshot is not
advanced. Pinned by
`a_withheld_completion_sentinel_is_not_announced_durable`.

Note `seen` counts materialized entries and explicit removals, so an object that never
became an entry is not in that total - the count cannot be used to detect any of
this.

Degraded coverage is surfaced to the consumer as a
`Warning::OperatorAttentionNeeded`, and open debt is enumerable through
`SyncEngine::debt`. Durable debt that nothing reports, or that nothing can list,
is only half a fix.

### Repair

`SyncEngine::repair_debt` runs one pass: it reads the ledger back, asks the
account to re-read what it could not represent, publishes what came back, and
discharges only what the consumer acknowledged. Caller-driven rather than
scheduled, because repair is remote work against an account that may be
throttled or degraded and the consumer knows better than the engine when to
spend that budget.

`Account::repair_inventory` takes a stream of `InventoryRepairRequest` and
yields correlated outcomes. Not `get_stream`, which hydrates known ids at a
projection and cannot express region replay, completeness proof, or
authoritative absence. The default implementation refuses PER REQUEST rather
than terminating the stream, so every attempt gets its answer and unsupported
repair classifies straight to `OperatorBlocked` instead of burning a transient
budget on a capability that will never exist.

Correlation is by engine-issued `RepairAttemptId`, not by `ObligationKey`. The
key identifies durable debt, not one execution: an attempt may be retried after
a stream terminates, a stale event may arrive from an abandoned stream, and the
same key may be reopened at a newer generation - so key-only correlation risks
applying an old result to a newly reopened instance. Exactly one terminal
outcome per accepted request; a duplicate, an unknown attempt, or an outcome
whose kind does not match its request is dropped. `Terminated` explains why a
stream stopped and does NOT answer for outstanding requests: those become LOCAL
deferrals, because recording a conclusion nobody reached is the same invented
certainty the coverage model exists to prevent.

Ledger entries retain the account-minted `InventoryRepairTarget`. Without it the
executor cannot reconstruct a request at all - key, domain and error are engine
or diagnostic facts, and the error in particular must never serve as a repair
descriptor because classifications change between revisions.

**Repair publishes ids, never object state, and that is what removes the race it
looks like it should have.** No inventory path delivers an `InventoryEntry` to a
consumer: `InventoryFusion` and `BackfillRunner` both keep the id, discard the
entry, and publish `ObjectChange::Created`. Repair mirrors that exactly. A
repair publication therefore carries no payload that could overwrite a newer
representation and nothing to insert after a tombstone; a stale repair `Created`
is resolved the way a stale backfill `Created` already is, by hydration
returning not-found. So there is no conditional application, no tombstone
requirement, no version-relation hook, and no lease held against polling. This
matters because `ServerVersion` could not have arbitrated it anyway - `ETag` and
`StateAt` are equality tokens, `ModSeq` orders only within a UIDVALIDITY epoch,
and the type derives `Eq` and not `Ord` for exactly that reason.

The entry still crosses the ACCOUNT boundary on `ObjectRecovered`, because
building it is the proof that the representation failure which raised the
obligation has healed; an id alone would only prove the object still exists. The
engine validates it (one entry, matching id, right request kind), keeps the id,
and drops the rest.

**One publication, and one writer request, PER SCOPE.** A pass plans across
every repairable obligation in the ledger, which can span scopes, so the
recovered ids are grouped by the scope they were recovered from and each group
is broadcast as its own `MultiplexerEvent`. `MultiplexerEvent::scope` is the
routing key a consumer files or filters on, so one batch stamped with whichever
scope happened to be recovered first delivers scope B's ids as scope A's, or
drops them. The RESOLUTIONS are grouped the same way, which is what keeps
discharge honest after the split: an obligation discharges only on an
acknowledgement of the batch that actually carried its id, never on a sibling
scope's batch. Within a scope the pass is still all-or-nothing. Pinned by
`recovered_ids_are_published_under_their_own_scope`.

Discharge happens on the consumer's explicit `ack_publication` acknowledgement
of the repair publication,
and both halves are recorded as `DischargeEvidence::RepairedAndPublished`.
Neither alone suffices: an account recovery nobody was told about leaves the
consumer unaware, and a published id with no successful account result merely
repeats an id. An unacknowledged recovery costs an attempt and stays owed. The
bar is deliberately no higher than the successful path's: a durably acknowledged
`Created` is the most any walk ever achieves for any object, so demanding proof
of successful hydration would fuse two failure domains - coverage asks whether
enumeration announced the object, hydration asks whether a projection can be
fetched now, and a later hydration failure belongs to the hydration lane and its
`ItemOutcome`.

`DefinitivelyIrrelevant` discharges and publishes NOTHING: absence from an old
inventory snapshot is not a deletion to apply against current state. It is a
typed account conclusion, never a status code - a 404 may mean deleted, moved
out of scope, permission lost, wrongly routed, or not yet replicated. Google's
variant is `AbsentUnderCursorBridge`, and it rests on the Gmail cursor being
anchored at the historyId sampled BEFORE the walk that raised the obligation, so
any deletion since is carried by the change stream. Under a different cursor
model the same `NotFound` would not be dischargeable.

Region discharge needs proof, never just entries. `RegionRepairProof` is
`ExactReplay` (identity-checked: the account consumed the exact region its own
token named), `CoveredBy` (checked with `CoverageDomain::covers`), or
`Partitioned` (the account asserts the split where the extent is opaque and the
lattice cannot do set algebra over it).

Retry budgets accrue per LINEAGE, counting completed outcomes only. A crash
after provider work but before acknowledgement must cost a repeated attempt, not
a consumed budget. Per-key budgets would be evaded by re-minting an equivalent
obligation each pass, so `replace_obligation` is an atomic parent-to-children
swap - appending would double-count the extent and replay the parent forever -
and children inherit the lineage. A child already open under a different
lineage, or whose key is another live lineage's id, is refused outright
(`ChildCollidesWithForeignLineage`) rather than silently given two lineages; a
child reusing its parent's or its lineage's own key is refused
(`ChildReusesLineageKey`); and a split arriving for a parent that is no longer
`Retrying` - an operator blocked or waived it, or its budget ran out, while the
result was in flight - is refused (`PolicyNoLongerRetrying`) and charges
nothing, because accepting it would discharge the only entry carrying that
decision and hand the debt to fresh `Retrying` children.

**Where the budget lives.** A lineage's identity is its root key: an entry with
`parent: None` is its own lineage, and a child's `parent` names the root
directly - lineages are flat by construction, and the codec refuses a row that
is not. The count lives in the ledger's own `lineages` table (a
`LineageBudget { attempts, exhausted }` per lineage id), NOT on any entry. It
used to live on the root entry's `Retrying { attempts }`, and because a
replacement always discharges its parent that put the one cap on a key-rotating
loop onto a discharged entry: compaction had to pin discharged roots, and
expiry wrote `OperatorBlocked` onto the discharged root while every open child
kept its own `Retrying` policy, so `repairable` went on planning the children and
the cap never bound after the first split.

One private `settle_lineages` pass runs at the end of every public mutator. It
keeps a row exactly while the ledger RETAINS a member of its lineage, open or
discharged, whatever that member's policy. A discharged member counts because
`upsert` reopens a rediscovered entry with its history intact, so dropping the
row at discharge would let a provider alternating a covering walk with a
rediscovery reset the budget every cycle; the row goes when compaction folds
the last member, which keeps the table bounded by the entry map. It rewrites
every open `Retrying` member from its row: blocked when
the row is exhausted, otherwise mirroring the row's count. `Retrying { attempts }`
on an entry is therefore a view, never read back to decide anything; waived and
`OperatorBlocked` members are never written, and discharged entries are never
touched. Exhaustion is sticky - a later charge with a larger budget does not
reopen a lineage - so a member that joins a spent lineage later (a closed
sibling rediscovered, say) comes back blocked.

Charging: `record_attempt(key)` charges `key`'s lineage only while `key` has an
OPEN entry. A result for an obligation discharged while its attempt was in
flight charges nothing, folded or not; that cannot let a loop escape the cap,
because a key with no open entry is never planned again. A result for a key
re-raised as a fresh occurrence meanwhile charges the new occurrence - an
over-charge bounded by the results in flight, which is the conservative
direction. A `Stalled` replacement is charged through `charge_lineage` with a
lineage `ack.rs` resolved via `open_lineage` BEFORE the swap, since the swap
discharges the key it came from.
Progress is judged across extent, granularity, proof gained and repair
authority, not extent equality: turning one opaque region into three addressable
objects is progress at identical extent, while repartitioning into equally
opaque children is not, and only the multi-dimensional test gets both right.

**Still not built:** region repair has no provider implementing it (Graph's
region is a barrier by construction), so `ExactReplay`, `Partitioned` and
lineage splitting are exercised by tests rather than by a live provider.

### Ledger compaction

`entries` used to grow monotonically - discharged entries were retained forever
for audit - while its two neighbours were already bounded: `proved` drops any
proof that can no longer contribute to an open obligation or barrier, and
`record_proof` retains barriers on the same test. Repair churns entries faster
than that retention rule was written for, so the entry map is now bounded too.

**Only `Discharged` entries compact, into a per-scope count plus an audit root.
Every `Unresolved` entry stays a live entry, waived ones included.** The line
falls out of `ProofStatus`. `Discharged` is TERMINAL: something proved the
coverage and nothing transitions it again, so a count plus a root loses nothing
anyone could act on. `Unresolved` is not terminal even when waived - a waiver is
a policy decision that stops the entry blocking completion and leaves the proof
`Unresolved` forever by design, and a later walk with a covering domain can
still discharge it. Compacting a waived entry would destroy an `ObligationKey`
an operator may still need and a proof may still land on. That is also what
preserves the proved-versus-waived distinction BY CONSTRUCTION rather than by
bookkeeping: only the proved side folds, so the waived entries are exactly the
ones left in the table in full.

`DischargeAudit` is `{ count, root: [u8; 32], latest_generation }` per
`CursorScope`. The root is a DETECTION root: the 256-bit wrapping SUM of
per-entry SHA-256 fingerprints over (key, generation, evidence discriminant).
Folding a candidate history and comparing detects a divergence - a discharge
that quietly vanished, a corrupted durable row - and that is the whole claim.
It is NOT proof of exact history: a sum of digests is weaker than its summands,
two histories can in principle fold alike, and nothing here defends against an
adversary who picks obligation keys. Addition rather than XOR because a key
discharged, folded, rediscovered and folded again must count twice, and XOR
would cancel it. `DischargeAudit::fold` and `discharge_fingerprint` are public
so an audit can rebuild a root without reimplementing the framing.

The fingerprint is SHA-256 (`sha2`, already a pinned workspace dependency for
bifrost-sasl) and not the 128-bit FNV-1a it started as. The additive fold made
the summands' collision resistance load-bearing, and FNV was not up to it: the
histories `{"00", "05"}` and `{"01", "04"}` at generation 3, all
`ProvedIrrelevant`, folded to identical count, generation and root out of
ordinary two-byte keys. `the_short_key_collision_that_sank_the_additive_fnv_root_is_gone`
keeps that pair as a regression.

**The audited history is narrower than the word suggests.** Only
`(key, generation, evidence KIND)` enters the fold. Repair attempt ids, covering
domains, `ReplacedByChildren` child lists and `ProvedIrrelevant` reason text are
INVISIBLE to the root at any hash strength - they are diagnostic payloads that
legitimately change between revisions, and hashing them would make the root
disagree with itself across a refactor. An audit comparing roots is comparing
which obligations closed at which generation by which kind of proof, and nothing
else.

It deliberately cannot answer "was key K discharged" on its own. Open debt is
the actionable state and is retained in full; an exact index means retaining the
keys, which is the growth this removes; and a probabilistic index errs by
reporting DISCHARGED for something that never was, which is the silent-loss
shape the whole coverage model exists to prevent.

Compaction runs at the END of `ingest` / `ingest_debt_only`, in the single
writer, once the entry map crosses `COMPACTION_THRESHOLD` (512);
`compact_discharged` is public for a consumer that knows a repair burst just
closed. Running only at a fold boundary is what makes it safe against a
concurrent transition rather than merely unlucky to race one, and the removal
itself is inert by construction: `discharge_repaired` and `replace_obligation`
already bail on `!is_open()`, and the foreign-lineage check only fires on an
open entry, so a removed terminal entry and a present one give the same answer.

That includes the lineage, which used to be the exception. While the retry
budget lived on the discharged root entry, compaction had to retain every
discharged ancestor a live child charged against, and keep process-local parent
links for folded keys so a late repair result could still find its root. The
budget now lives in the ledger's `lineages` table and a child's `parent` is an
identity rather than a link anything dereferences, so every discharged entry
folds and nothing is pinned. `DischargeEvidence::ReplacedByChildren` also names
entries, and no reader dereferences that list for a decision either - it is
evidence, not a link.

Accepted cost: a compacted key rediscovered later is raised as a NEW entry, with
`first_seen_unix_seconds` at now and - unless its key is still a live lineage
id - the budget at zero, where an uncompacted discharged entry would have kept
both. The exception is a compacted lineage ROOT re-raised while members of its
lineage are still retained: its lineage id is its own key, it finds that row,
and it rejoins the lineage's budget. That is the conservative direction, and it
is what happened before the root could fold at all. The entry was proved covered before it
was folded, so a re-raise after proof is a fresh gap rather than a continuing
one. `DebtLedger::is_empty` counts the audit table for the same reason: a caller
that skips writing an "empty" ledger must not erase the only surviving record
that those obligations existed.

## Cursor envelope

`OpaqueChangeState` carries `protocol`, `envelope_version`, and
`bytes`. `decode_envelope` returns:

- `Error::SchemaIncompatible` for a header version outside
  `[MIN_MIGRATABLE, ENGINE_VERSION]`, in either direction.
- Otherwise decoded `Checkpoint::Change` / `Checkpoint::Backfill`.

Both out-of-range directions are the same classification on purpose:
both describe a durable row this revision cannot read, and
`SchemaIncompatible` is the only signal the two healing paths key on.
An over-version row classified as `Error::Other` instead would
propagate as an ordinary establish failure - three burnt reopen
attempts, a broadcast `Terminated`, and the undecodable row still on
disk for the next attach to trip over identically.

Encoding an `ObjectType` or `ProtocolKind` variant the codec does not
know panics, the same rule `encode_scope` already applied to unknown
`CursorScope` variants. Writing a placeholder tag instead would
persist a durable row nothing can decode, and the account would then
re-establish, re-encode the same unreadable value, and re-poison the
row on every attach. Decoding the reserved tag `0xFF` (written by an
engine revision that did exactly that) yields
`Error::SchemaIncompatible` so those rows still heal.

Every encoded length is checked against `u32::MAX`. An impossible
larger in-memory field panics at the encoding boundary instead of
writing a false truncated prefix and silently corrupting the envelope.

An unreadable row is classified where it is found. `establish_one` -
the attach path - deletes the row and re-establishes that one scope
inline, because the reopen listener is spawned later in `attach_inner`
and does not exist yet. `run_establish` also translates an unreadable
row into a typed `AccountError` whose derived `RecoveryClass` is
`Engine(SchemaIncompatible)`, but its callers do not feed that result
back through `RecoveryContext::recover` and `handle_account_error`, which
dispatches to the schema-clear arm. Per-scope recovery retries and then
terminates that scope. Replacement attach rejects the candidate
connection, and automatic account restart retries the reopen. It leaves
the durable row untouched because a replacement
can abort while the installed account remains live. Clearing a
preexisting cursor and its backfill completion would mutate that account's
state, and reattach rollback has no safe restore path because consumer
acknowledgements may persist newer rows. Rollback only deletes rows
created by its own attempt. A consumer-driven reattach returns the
failure. The account-wide schema clear loop is reached when a running
worker sends a classified error through `RecoveryContext::recover`, not
from the `run_establish` result alone.

(These are engine `Error` enum variants, not `RecoveryClass`
variants.)

`ChangeCursor` carries its own `envelope_version` separately from
`OpaqueChangeState.envelope_version` (outer vs inner versioning). The outer
version is owned by `bifrost-types` through
`CHANGE_CURSOR_ENVELOPE_VERSION`; every producer stamps that shared constant,
the persisted change envelope carries it, and the engine calls
`ChangeCursor::validate_envelope` before protocol dispatch. The inner version
remains protocol-owned and rides inside the payload, where a bump invalidates
only that protocol's bytes.

`validate_envelope` is a STRICT EQUALITY gate, and the decoder is the single
migration boundary that makes that safe. `decode_envelope` accepts the whole
`[MIN_MIGRATABLE, ENGINE_VERSION]` window, but a `Checkpoint::Change` it
returns has already been run through `migrate_change_cursor` and stamped
current - so every cursor in the engine's hands is at the current layout, and
the codec is the only code in the workspace that knows a historical one.

The alternative - widening `validate_envelope` to accept the migration window -
was considered and rejected. It spreads knowledge of every past layout across
every consumer, and it hands a protocol crate a cursor in a shape that crate
has no code to interpret. Leaving the window and the gate disagreeing was the
live defect: a version-1 row would decode successfully and then fail
validation, so the engine would classify a migratable durable checkpoint as
`SchemaIncompatible` and discard it - a full resync per account, arriving the
moment `ENGINE_VERSION` is first bumped.

Pinned by
`tests/envelope_roundtrip.rs::every_accepted_envelope_version_decodes_into_a_cursor_that_validates`,
which loops the window constants rather than a literal so it widens on its own
at the next bump, and by
`an_unknown_change_cursor_outer_version_is_detectable`. Bumping
`ENGINE_VERSION` means adding the per-version fixup in `migrate_change_cursor`
AND a real byte fixture for the outgoing layout; the window loop patches the
header over current-layout bytes and so cannot prove a fixup reads an old
payload correctly.

Validation is applied at every seam where an account-authored cursor enters the
engine - the attach path, `run_establish`, `persist_ack_request`,
`drive_changes_stream`, the inventory fusion checkpoints, and
`fuse_inventory_done` - and a mismatch maps to classified schema recovery
(`Error::SchemaIncompatible` or `recovery::cursor_decode_failure`), never to a
silent drop. `CursorRegistry::put` carries only a `debug_assert!`: by then the
condition is unreachable, and aborting on a value a third-party `Account` impl
authored would trade a recoverable resync for an outage. `encode_envelope` does
panic on an unsupported outer version, consistent with the rest of that codec,
which refuses to write any row its own decoder would reject.

## Ledger envelope

The checkpoint half of `apply_transition` has always been serializable; the
ledger half was not, so a persistent backend could store only one of the two
things the atomicity contract is about. `encode_ledger` / `decode_ledger` in
`cursor/ledger_envelope.rs` close that, and are maintained together with the
cursor envelope: same header shape, same little-endian length-prefixed
primitives, same `Error::SchemaIncompatible` for a version outside
`[MIN_MIGRATABLE_LEDGER, LEDGER_ENVELOPE_VERSION]` in either direction, same
rule that a panic belongs to encode and never to decode. `decode_ledger` is
likewise the single migration boundary: a version inside the window is migrated
through `migrate_ledger`, not merely accepted, so what comes out is
current-shaped ledger state.

The discharge audit table and the lineage budget table are appended to the
PAYLOAD rather than widening the header, so the header keeps one shape. The
lineage table's version moved the retry budget off the discharged root entry;
the repository owner ruled that no consumer had persisted a ledger when it
landed, so it raised
`MIN_MIGRATABLE_LEDGER` to itself instead of synthesizing budgets from the old
per-entry counters, which for a split lineage disagreed with each other. An
older row therefore decodes to `Error::SchemaIncompatible`, the classification
that authorizes clearing the row and re-establishing rather than reading as a
store failure.

A new tag in a vocabulary the error digest encodes is a version bump too,
because an older decoder would refuse it: the version that added the
`Internal` account error kind changed nothing else, so the version before it
stays in the window and is read as is, while a row of that older version
carrying the new tag is corrupt (`Error::Other`), since no writer of it could
have produced one. Version 5 adds the Event error resource and scope for typed
public-calendar reads. Versions 3 and 4 still decode, but reject the new Event
tags as corruption; existing tags and layouts are unchanged. Version 6 adds
`DirectoryGateway { url }` scope (tag 10), preserving versions 3 through 5
without that tag and rejecting the tag in older-version rows.

Decode VALIDATES, never heals: a duplicated entry, barrier or lineage key,
trailing bytes, a lineage that is not flat (a parent naming itself or an entry
that has a parent, which covers every chain and cycle), an entry whose lineage
has no budget row, a row with no retained member, or an open `Retrying` entry
disagreeing with its row is `Error::Other`. Repairing any of these would mean choosing which half
of a contradiction to believe, and a guessed budget is the silent reset budgets
exist to stop. A parent naming an ABSENT key is legal - its root was compacted.

The encoded form covers everything a restart must not forget: the entry map, the
barrier map, the retained proof set, the per-scope discharge audit (scope,
`count`, a fixed 32-byte `root`, `latest_generation`) - dropping which would
silently reset an account's discharged count to zero and make its root
uncheckable forever after - and the lineage budgets, without which a restart
would reset every retry budget. Proof retention is easy to mistake for
a cache - it is not. Coverage discharges by UNION, so a proof dropped on restart
turns debt that two later windows jointly cover into debt neither covers alone.
A `BarrierIncident::resume_from` nests a whole cursor envelope through
`encode_envelope` rather than re-deriving the layout, so the two codecs compose
instead of drifting.

`AccountError` is the one field that does not round-trip as a typed value, and
the reason is structural: the `Cause` chain holds `&'static str` payloads
(`AccessCause::InsufficientScope { needed }`,
`RequestCause::InvalidArgument { field }`) that no decoder can produce from
bytes without leaking. So `last_error` and `BarrierIncident::evidence` persist
as a digest - the exact classification (`AccountErrorKind`, `ErrorScope`,
operation, provider, protocol) plus the whole `DiagnosticInfo` - rebuilt through
`AccountErrorBuilder` with the canonical `Cause` its kind demands. Secondary
cause payloads, `idempotency_override` and `throttle_scope` do not survive.

That is affordable because no ledger predicate reads these fields.
`blocks_completion`, `completion_permitted`, `repairable`, `lineage_root`,
`record_attempt` and every discharge path read proof, policy, domain, generation,
target, parent and the lineage budgets; `LedgerEntry::target` exists precisely so a repair descriptor is
never taken from an error whose classifications change between revisions. The
error is operator evidence, and a digest preserves operator evidence. Anything
that starts DECIDING on a restored error has to promote the field first.

`DebtLedger::from_parts` is the only door into the private maps that is not
`ingest`, and it is crate-private for the reason the fields are: no protocol
crate may construct ledger state. A consumer restores a ledger by decoding bytes
this engine wrote, never by asserting one.

## Checkpoint store

```rust
pub trait CheckpointStore: Send + Sync {
    fn put_change_cursor<'a>(&'a self, account: &'a AccountId, cursor: ChangeCursor)
        -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
    fn get_change_cursor<'a>(&'a self, account: &'a AccountId, scope: &'a CursorScope)
        -> Pin<Box<dyn Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>;
    fn put_backfill<'a>(&'a self, account: &'a AccountId, checkpoint: BackfillCheckpoint)
        -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
    fn get_backfill<'a>(&'a self, account: &'a AccountId, scope: &'a CursorScope)
        -> Pin<Box<dyn Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a>>;
    fn delete_change_cursor<'a>(&'a self, account: &'a AccountId, scope: &'a CursorScope)
        -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
    fn delete_backfill<'a>(&'a self, account: &'a AccountId, scope: &'a CursorScope)
        -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
}
```

`put_*` take owned `ChangeCursor` / `BackfillCheckpoint`; `get_*`
take a borrowed `&CursorScope`. `delete_change_cursor` is required
because `RecoveryClass::Engine(EngineDirective::RestartScope)` must
drop the durable cursor so the next establish re-runs via inventory;
a no-op delete would silently preserve the stale cursor. It is also
what attach calls to clear an undecodable envelope, so a store that
no-ops here strands the account on a row it cannot read.
`delete_backfill` drops every backfill row for the scope, completion
marker included; only `SchemaIncompatible` recovery calls it (the
marker is what would make the next attach skip the inventory re-walk
that re-mints ids under the new encoding), and routine `RestartScope`
deliberately does not - re-walking a completed backfill after every
cursor invalidation would re-hydrate the scope for no schema reason.

A store that decodes durable bytes must surface a schema it cannot
read as `Error::SchemaIncompatible` from `get_change_cursor`; that is
the signal both establish paths key on. Any other error is treated as
a store failure and propagates.

The same applies to the ledger. `get_ledger` returns a `DebtLedger` by value and
`apply_transition` / `put_ledger` take one, so a durable backend needs a
serialized form for it, and `encode_ledger` / `decode_ledger` are it - a backend
is not expected to invent one, and must not, because ledger state is
engine-owned. A backend that keeps ledgers only in memory is running a
DEGRADED store, not a simpler one: a restart forgets accepted debt, and a
degraded scope comes back looking clean until something re-enumerates it. That
is a legitimate choice for a test double, which is what
`InMemoryCheckpointStore` is, and not one for a persistent backend.

`InMemoryCheckpointStore` is the test backend (HashMap-backed). No
sled / sqlite default; storage is consumer-owned.

`get_backfill` selects the greatest `items_done`. Equal-progress rows
use the greatest parsed page upper bound, then lexicographically
greatest opaque partition bytes, so equal-sized page windows resume
deterministically from the furthest one. Page checkpoint
`items_done` counts entries and explicit removals observed before
`LiveSupersedes` filtering;
filtering therefore cannot make a full inventory window look short and
falsely signal exhaustion.

`Partition` is `Hash + Eq` (additive change to `bifrost-types`)
because the in-memory store keys on it.

## Read-back guard

Applies to all four protocols today (every protocol declares
`MutationReplaySafety::None`; the engine reads back after retry to
disambiguate ambiguous transport failures). The guard re-fetches
affected ids via `Account::get_stream(ids, Projection::FlagsOnly)`
and reconciles against the intended mutation. Destroy read-back treats
only an explicit `AccountErrorKind::NotFound` as proof that the object
is absent; any other per-item fetch failure remains uncertain.

## Error / Terminated / Warning

Engine-side `Error` (distinct from `bifrost-types::AccountError`):

- `AccountNotAttached(AccountId)`
- `AccountAlreadyAttached(AccountId)`
- `OpenFailed(#[source] bifrost_types::AccountError)`
- `EstablishCursorFailed(String)`
- `EstablishCursorTerminated(#[source] bifrost_types::AccountError)`
- `CheckpointStore(String)`
- `SchemaIncompatible`
- `ShuttingDown`
- `Paused`
- `Account(#[from] bifrost_types::AccountError)`
- `Other(String)`

Stream termination is the `SyncEvent::Terminated(AccountError)`
variant; the engine dispatches every termination through
`crate::recovery::plan_recovery(error)`, which collapses
`AccountError::recovery()` into the closed
`RecoveryPlan { Retry, Reconcile, Engine, Terminal(Fatal) }` enum.
Adding a future `RecoveryClass` variant fails to compile in
`plan_recovery` (the four mutually-exclusive helpers cover the
case, but the closed `RecoveryPlan` enum enforces the dispatch
surface). `Engine(EngineDirective::*)` directives drive
`restart-scope` / `restart-account` / `downgrade-strategy` /
`downgrade-capability` / `schema-clear` / `operator-override` /
`disable-scope` flows. `DisableScope` quarantines a single cursor scope
(an admin revoked one shared/other-user IMAP folder mid-sync): the engine
deletes the scope's in-memory and durable cursor, drops it from the
membership index, and broadcasts a scoped
`Warning::OperatorAttentionNeeded`; the poll loop self-drains on the next
iteration, siblings keep syncing, and the account is neither paused nor
treated as auth-lost. `Terminal(Fatal)` is the type-system collapse point
that enforces "the engine has nothing left to try"; both the terminal arms
(multiplexer and engine) emit a structured `TelemetryView` `warn!` rather
than writing to any built-in queue or dashboard. Any operator-notification
queue is the consumer's to build off the broadcast
`SyncEvent::Terminated`.

### Producer-emitted inventory warnings are forwarded, not absorbed

Both inventory front ends - `InventoryFusion::run_stream` and
`BackfillRunner::run_partition` - forward `InventoryEvent::Warning` onto
the account change stream as a `SyncEvent::Warning`, the same way they
forward `Terminated`. Only `Progress` is absorbed.

This matters because for two live producers the warning is the ONLY
announcement of a degrade the consumer would otherwise have to infer.
Graph's public-folder walk emits one when a folder passes
`PUBLIC_FOLDER_LIVE_IDS_CAP` and drops to additions-only, at which point
deletions stop propagating for that folder; `reference/graph.md` describes
that degrade as announced. IMAP emits one for a QRESYNC -> CONDSTORE
strategy downgrade.

The IMAP case is why absorbing these was worse than dropping an ordinary
message: `take_qresync_negotiation_warning` is a ONE-SHOT guarded by an
`AtomicBool` shared with the changes path. Backfill and deferred inventory
both reach it at attach, racing the multiplexer, so whichever lane arrived
first consumed the flag - and if that lane discarded the warning, the
changes path could never re-emit it. The consumer never learned the
account was running on a downgraded sync strategy.

Note this is distinct from the warnings the engine SYNTHESIZES
(`warn_degraded` for incomplete coverage, `announce_page_loss` for page
lanes). Those describe what the engine concluded; these carry what the
protocol crate observed.

The reopen listener never sleeps for a bare `RecoveryPlan::Retry`.
The originating poll, push, or mutation path owns an actionable retry;
the listener only records shared throttle deadlines and stays
available for engine directives. Sleeping there would serialize
restart and schema recovery behind a delay after which no operation was
actually retried.

Reopens use exponential backoff with ±20% jitter (1s initial, 5min
cap) and a three-attempt budget. Per-scope re-establishment
dispatches each failure through `plan_recovery` rather than
blind-retrying every class: a `DisableScope` establish failure
quarantines the scope immediately (same rule attach applies), and a
terminal class skips the remaining budget straight into the
exhaustion tail - another attempt cannot repair it. After the budget
is spent (or short-circuited terminal) the engine
broadcasts `SyncEvent::Terminated(last_error)` for the affected
scope (per-scope re-establishment) or for the account
(`RestartAccount`). When the last failure is engine-internal and has no
account error, the synthetic error derives
`Engine(OperatorOverrideRequired)` and retains the affected cursor scope;
it does not request another automatic scope restart. A provider retry,
reconciliation, or other engine-action error is also replaced with this scoped
operator-attention error at per-scope exhaustion; an already terminal error or
an existing operator-attention error keeps its classification. The
account-wide `RestartAccount` exhaustion tail applies the same rule to its last
error: terminal and existing operator-attention errors stay intact, while retry,
reconciliation, and other engine-action errors become account-scope operator
attention because no automatic action remains. The tail then pauses with
`PauseReason::RetryBudgetExhausted`.
Account exhaustion then publishes
`AccountControl::Pause(PauseReason::RetryBudgetExhausted)` and
flips the boundary to `Pause` so workers park. Consumers subscribe
to the per-account `AccountControl` broadcast via
`SyncEngine::account_control_stream` and flip back via
`SyncEngine::resume_account`. The account-wide shape is pinned
end-to-end by
`tests/attach_schema_recovery.rs::three_failed_reopens_terminate_and_pause_the_account`,
which runs under paused time so the recorded per-attempt open instants
expose the 1s / 2s backoff schedule exactly.

`EngineDirective::OperatorOverrideRequired { reason }` auto-pauses
the account with `PauseReason::OperatorOverrideRequired` and emits
a `Warning::OperatorAttentionNeeded` carrying the protocol-supplied
reason. The reason rides on the warning's free-form fields; the
`PauseReason` enum is bounded.

`crate::recovery::ThrottleBucket` is engine-wide (one per
`SyncEngine`, shared by every slot): deadlines keyed by
`ThrottleKey::{Mailbox, Account, Budget}`. `Budget` is the only key that
crosses accounts. `ThrottleScope::CurrentOperation` is a per-call hint and
never enters the bucket.

Keys reach the work they hold two ways. A key LIFTED from a
`ThrottleScope` (see below) is the account's own `Account` key or one of its
`Mailbox` keys. A key DECLARED by `Account::throttle_keys` for
the work about to be driven is looked up literally by
`recovery::throttle_wait`, so an account that has
never failed still sees the first deadline a sibling recorded under it.
Every wait site asks the account what its work consumes, through
`recovery::declared_keys`: the poll loop and the reconciler for the scope
(`ThrottleWork::Scope`), the backfill orchestrator and partition runner and
the deferred-inventory worker for the scope, the mutation campaign for its
unresolved targets plus a move's destination and supplied source
(`ThrottleWork::Objects`, `ThrottleWork::Membership`), and the read-back for
every id in `unresolved_readback_ids`. Recovery-driven establishment
(`run_establish` in `engine/reattach.rs`, for scope recovery and replacement
accounts) honours account-wide holds and the current account's declarations
for `ThrottleWork::Scope`. Before every attempt, including the first, it
waits for both those holds and the previous failure's local deadline.
Failures record shareable Retry or Reconcile holds before rollback or close,
and throttled failures pace on their holds instead of the jittered backoff.
Hold waits do not consume attempts, but share a cumulative
`REOPEN_BACKOFF_CAP` allowance per scope repair or account restart; expiry
never authorizes a provider call and ends recovery with the distinct warning
`recovery wait budget exhausted`. The promise is bounded additional throttle
delay, not bounded transaction occupancy. Restart retries keep the failed
scope's declared keys and local deadline and wait before opening another
replacement; replacement establishment asks the replacement account for its
declarations. Recovery repairs resolve an absolute hint once
(`record_repair_throttle`), keep a local monotonic deadline, and pass an
equivalent duration hint to the shared recorder. Recovery establishment
rechecks holds after the checkpoint writer responds, before driving the
provider. Restart rechecks after acquiring the reopen lock and releases both
lock and activity before waiting on a newly discovered hold. Every teardown
refusal records its holds immediately; the first refusal remains the returned
error, while local pacing keeps the longest deadline. Recovery also records
holds from replacement-subscription unwind failures, both account-close
failure paths, and best-effort membership refresh failures; cleanup keeps the
original result and the longest local deadline, and membership refresh stays
best-effort. The wait is the longest pending
deadline across the account-wide keys and the declared ones. `Mailbox` and
`Budget` keys are never part of the account-wide wait: they hold one
mailbox's work, and reach it only by declaration. IMAP declares its
`ThrottleKey::Mailbox` for a folder scope, a folder's objects and a folder
membership; Graph declares the Outlook mailbox `ThrottleKey::Budget` each
scope, object and destination folder is routed to; every other crate
declares nothing and waits on account-wide holds only.

The guarantee is drive-start granularity. Each site checks immediately
before driving, and checks again after every wait that precedes the drive
(a retry-advice sleep, a pause park, scheduler or lane admission, and - for
the poll loop and the reconciler - the drive lease, checked inside the
lease-protected closure before `changes_stream` is called), asking the
declaration of the very account snapshot the work then runs against (a reopen
during the waits can install an account that declares different keys), because a
deadline recorded during those waits existed before the drive started; on a
hold the permit is released rather than carried across the throttle wait. A
deadline recorded mid-drive is honoured at the next drive or attempt; pacing
inside one drive is the protocol's own retry hint.

A wait that holds one scope must not stall the others behind it, and three
callers walk scopes one after another. The push reconciler keeps scopes held
by a SCOPE-LOCAL key in a deferred set owned by its run loop, re-driven by a
timer once the earliest hold lifts; only the scope is kept (no cursor, lease,
permit or activity guard), parking and deletion are re-checked at dispatch,
and a hint that arrives during a drive is read after it and re-marks the
scope, so no invalidation is lost. A throttled hinted drive is deferred only
when a hold actually covers it, else it is skipped as before and its poll loop
catches up. The backfill orchestrator defers a scope-locally held scope on the
scan (`BackfillScan::defer_until`, kept apart from the failure ramp) and walks
the next one. The deferred-inventory worker requeues a held scope behind the
others and sleeps only once every queued scope is held. An ACCOUNT-WIDE hold
blocks every scope equally and is slept out inline at all three - BEFORE the
scope-local decision, so a short account hold overlapping a long mailbox hold
cannot leave the held scope in an inline wait with the others behind it.
Scope-local holds between backfill partitions park the walk (see Backfill);
account-wide holds still sleep inline. A mutation campaign schedules throttle
cohorts independently. A held mailbox cohort does not delay a free cohort's
ready submission, retry, or read-back. Each action honours the account-wide
holds and its current declared keys, with a second check after admission. No
admission permit or activity guard spans a hold wait, and waits never consume
mutation attempts. Account-wide holds still cover every cohort.

Both sides are wired. Recording: the reopen listener, the poll loop's
and push reconciler's Retry and Reconcile arms (both the per-drive
terminations and the reconciler's top-level `WatchEvent::Terminated`),
and the mutation campaigns (per-item outcomes and stream-level
terminations) and their read-back (every per-item hydration failure the
guard absorbs as uncertain, through the crate-internal
run_object_readback path, plus a returned stream-level termination) funnel through
`recovery::record_throttle` or
`recovery::record_reconcile_throttle`, and the local sleep those arms
observe comes from `recovery::retry_delay` /
`recovery::reconcile_delay`, which honor the carried `RetryHint` and
otherwise fall back to one second. An error that stamps throttle budgets
(`AccountError::throttle_budgets`) is recorded on exactly those
`ThrottleKey::Budget` keys and nothing is lifted from its `ThrottleScope`:
recording the account key as well would pause every sibling mailbox the
budget exists to keep running. A stamped budget with no hint is held for the
same one-second fallback the observer sleeps, so a sibling declaring it is
paced alike. An unstamped hintless error records nothing, except on a
lifted `Mailbox` key, which is as narrow as a budget and gets the same
fallback: IMAP's folder-scoped `[LIMIT]` carries no hint, and without it the
per-folder hold was never recorded at all. Otherwise
both recorders resolve the key from the
identities the classified error actually carries
(the error's mailbox scope). The mailbox identity is
read in BOTH shapes it is produced in - `ErrorScope::Mailbox { id }` and
`ErrorScope::Cursor(CursorScope::Folder(id))` - because every production folder
producer builds the latter, via `with_folder_scope`, and IMAP's
`mailbox_throttle` (the only production emitter of `ThrottleScope::Mailbox`)
fires for either. Reading only the former made `ThrottleKey::Mailbox`
unreachable in production while its test passed against the synthetic shape.
A mailbox throttle carrying no mailbox identity at all remains local to the
current operation: degrading it to the account key would widen a per-mailbox 429
into an account-wide stall. There is no tenant- or provider-wide scope: a
provider whose throttle genuinely spans accounts stamps and declares a
budget, which needs no identity on the error. Reading happens at
every wait site listed above, re-checked after waking since a longer
deadline can land mid-sleep; cold-start hydration (the backfill runner and
the deferred-inventory worker) is the heaviest request lane the engine
drives, so it consults the same keys at the same boundary as its pause
checks rather than barrelling through a Retry-After that paused the
polls. An account's own `Account` deadline is deliberately retained until expiry: it describes the
stable account identity, not the connection incarnation, and prevents detach
plus immediate reattach from bypassing a provider wait. Expired waits are
pruned opportunistically, so retained entries are time-bounded.

Bucket deadlines are `tokio::time::Instant`, not `SystemTime`. Every wait
site parks the returned duration as a `tokio::time::sleep`, so a deadline
read on the wall clock is retired by a timer on a different clock; the two
agree only where tokio's clock IS the system clock. Under paused time they do
not, and the re-check loop then re-derives the full wait after every sleep and
spins without progress - which is why the deferral had no hermetic test until
the bucket moved. `tokio::time::Instant::now()` falls back to the system clock
when no tokio clock is in force, so production behaviour is unchanged. Nothing
persists a throttle deadline (the bucket is one in-memory `Arc<Mutex<_>>` per
engine, never serialized), so a monotonic instant never has to survive a
process. The wall-clock-to-monotonic conversion happens at exactly one
boundary, `record_throttle_parts`: a provider `RetryHint` - a duration
(`After`) or an absolute HTTP date (`At`) - is resolved through `min_delay`
against `SystemTime::now()` and immediately anchored to `Instant::now()`.
`account_throttle_wait` takes no `now` for the same reason: there is one
correct clock for a wait site, and a caller-supplied instant is how the wrong
one got read. This is the same move `bifrost-smtp` made for `AsyncDeadline`
and `ByteBucket`.

`tests/throttle_defers_changes.rs` pins the engine-level fact the unit
tests cannot reach: that a recorded deadline actually defers `changes_stream`
(a scope that never failed - so it holds no retry advice and takes no local
`retry_delay` sleep - stops polling on its fixed cadence and resumes when the
`Retry-After` elapses). It asserts a poll COUNT flat
across a window many cadences wide and then RESUMING, not a total elapsed
time another timer could satisfy. `tests/throttle_budget_isolation.rs` pins
the declared-key half the same way: a stamped budget holds the scopes that
declare it, on this account and on a second attached account that never
failed (first-deadline reach), while a sibling scope on
another budget keeps polling (recording precedence). In
`tests/readback_guard.rs`, `a_campaign_waits_for_a_budget_its_targets_declare`
pins the campaign side across two accounts.

Cursor envelope schema mismatches at `get_change_cursor` are
translated through `crate::recovery::cursor_decode_failure` into an
`AccountError` whose derived recovery is
`Engine(SchemaIncompatible)`; the reopen listener then clears every
in-memory and durable cursor and re-establishes each scope under
the same backoff/budget contract above. Per-scope failures
escalate per-scope (account continues for sibling scopes);
`RestartAccount` failure escalates per-account (every scope pauses).

`Warning` is a re-export of `bifrost_types::Warning` and is
outside the error model (advisory only, never aborts streams).

## File map

```
crates/sync/src/
  lib.rs                  // public re-exports; AccountId/Priority/etc.
                          // from bifrost-types
  engine/
    mod.rs                // SyncEngine, SyncEngineBuilder, Drop;
                          // detach / reattach / shutdown;
                          // ack_checkpoint, ack_publication, debt,
                          // repair_debt, waive/block_obligation;
                          // account_changes_stream,
                          // account_control_stream, invalidation_sink,
                          // subscribe/unsubscribe_push,
                          // open_skipped_scopes (open-time skip lane),
                          // account_capabilities. Every path that
                          // resolved through `crate::engine` before the
                          // split still resolves here.
    context.rs            // SlotContext: the slot-wide handle bundle
                          // every spawned worker shares, plus
                          // `recovery()` which borrows it as a
                          // RecoveryContext and `lane_gate()` which
                          // borrows it as the backfill producer's door
                          // onto the bounded lane
    lane.rs               // LaneGate + BackfillAdmission + WaitFailed.
                          // The backfill bound itself lives on the
                          // publication records in PendingCoverage, so
                          // this module keeps no ledger: it is the wait
                          // (`wait_for_capacity`, and the variant that
                          // gives up scheduler admission while parked)
                          // plus the account's sync admission, which is
                          // the only state it owns
    attach.rs             // attach / attach_inner / attach_opened,
                          // discover_scopes(_from), establish_one,
                          // persist_cursor, scope_covers_membership,
                          // link_discovered_memberships,
                          // wait_for_real_subscriber,
                          // run_deferred_inventory_establishment
    backfill.rs           // run_backfill_orchestrator + BackfillWiring;
                          // BackfillScan (rescan eligibility + retry
                          // ramp); BackfillPlan -> ScopeResume is the
                          // ONE seam where the fixed and open-page
                          // shapes differ - the walk itself is shared;
                          // open_pages_resume,
                          // backfill_complete_recorded,
                          // emit_backfill_complete
    ack.rs                // ack_writer (the account's single durable
                          // writer) + WriterHandle; persist_ack_request,
                          // apply_repair_resolutions, apply_replacement;
                          // take_ack_writer / await_worker_until
                          // (detach's writer-last ordering)
    reattach.rs           // RecoveryContext + handle_account_error;
                          // handle_engine_directive, restart/disable/
                          // re-establish scope, reattach_account,
                          // restart_account, run_establish,
                          // accepted_push_scopes, log_open_skips
    bulk.rs               // bulk_set_flags / bulk_move / bulk_move_from
                          // / bulk_destroy + run_bulk_pipeline;
                          // classify_item_outcome, MutationBucket,
                          // retry + read-back queueing, directive
                          // de-duplication
    passthrough.rs        // live_account + the read-only hydration and
                          // PIM forwarders (get_stream,
                          // message_hydrate, open_blob, contacts_*,
                          // filters_*, identities_*, ...);
                          // announce_page_loss
    tests.rs              // the module's unit tests
  control.rs              // SyncControl + record_checkpoint hook
  error.rs                // engine Error wrapping AccountError + Warning
  inventory_walk.rs       // shared inventory barrier and resume state
  types.rs                // EngineConfig, MultiplexerConfig,
                          // BackfillConfig, MutationConfig, PushConfig,
                          // SchedulerConfig, AccountSlot, WorkerTask
  recovery.rs             // plan_recovery + RecoveryPlan dispatch;
                          // ThrottleBucket (engine-wide, monotonic
                          // tokio::time::Instant deadlines) +
                          // resolve_throttle_key / record_throttle /
                          // account_throttle_wait;
                          // retry_delay; restart_scope_error;
                          // cursor_decode_failure translator
  multiplexer/
    mod.rs                // Multiplexer::run; ReopenRequest;
                          // MultiplexerEvent { scope, event, checkpoint };
                          // lifecycle_reopen wiring; ChangeDelivery -
                          // the change broadcast plus the numbering of
                          // the receivers handed out on it, under one
                          // mutex so send+stamp, subscribe+number and
                          // unregister+sweep are each atomic
    changes.rs            // drive_changes_stream + ChangesEvent +
                          // AckRequest (broadcast-then-consumer-ack)
    fusion.rs             // InventoryFusion::run_with_broadcast
    poll.rs               // adaptive cadence helper
  backfill/
    mod.rs                // account-keyed BackfillRegistry,
                          // BackfillPolicy / Strategy
    runner.rs             // BackfillRunner::run_partition,
                          // LiveSupersedes (ring-evicting, default
                          // cap LIVE_SUPERSEDES_DEFAULT_CAP)
    partitioner.rs        // plan() for TimeWindowed / UidRange / PageCount
    checkpoint.rs         // BackfillCheckpointWriter wrapper
    scope_walk.rs         // ScopeWalkDriver: partition sequencing;
                          // a barrier stops the whole scope walk
  push/
    mod.rs                // InvalidationSinkInner
                          // (per-account ingress) + coalesced_event
    reconciler.rs         // hint -> scope -> changes_stream;
                          // warning_event on Disconnected/Reconnected
    subscription.rs       // SubscriptionRegistry (per-engine DashMap)
  mutation/
    mod.rs                // MutationCounters,
                          // fanout::partition_by_account
    idempotency.rs        // IdempotencyKey vending
    readback.rs           // Projection::FlagsOnly read-back guard
  cursor/
    mod.rs                // CursorRegistry + membership index
    coverage.rs           // PendingCoverage: the publication ledger -
                          // claims, boundary registrations, supersession,
                          // fences, watermarks - and, on those same
                          // boundary entries, the backfill lane's
                          // capacity (`backfill_in_flight`,
                          // `await_backfill_capacity`,
                          // `release_undelivered`)
    envelope.rs           // MIN_MIGRATABLE / ENGINE_VERSION + migrations
    ledger.rs             // DebtLedger: entries, barriers, retained proofs,
                          // compacted discharge audit, folded parent links
    ledger_envelope.rs    // encode_ledger / decode_ledger + error digest
    store.rs              // CheckpointStore trait (6 methods) +
                          // InMemoryCheckpointStore
  scheduler/
    mod.rs                // four-lane Scheduler + admission dispatcher
    lanes.rs              // bounded LaneQueue + LaneShedPolicy
                          // (DropOldest default / DropNewest)
    budget.rs             // BudgetGate (account-pool lifecycle and cancellation)
  cancel/
    mod.rs boundary.rs    // safe-boundary mechanism

crates/sync/tests/
  cross_crate_conformance.rs  // cross-crate trait/contract checks
  envelope_roundtrip.rs       // cursor envelope encode/decode tests
  ledger_envelope_roundtrip.rs // durable DebtLedger encode/decode tests
  partition_planner.rs        // backfill partitioner unit tests
  readback_guard.rs           // mutation readback reconciliation
  scheduler_priority.rs       // lane ordering, starvation guard, pull shape
  scheduler_admission.rs      // admission queueing, preemption, cross-account
                              // liveness, single-permit engine end-to-end
  backfill_lane_flow_control.rs // the bounded backfill lane end to end: stall
                              // at the bound, live lane unaffected on a
                              // one-sync-permit budget, no loss or duplication
                              // through an undersized ring, a completed walk
                              // not stranding admission, and the teardown
                              // cases (detach while parked, receiver replaced
                              // while parked)
```

### Published surface the bounded lane added

Listed because the standing rule - a change that removes or renames a published
item stops and asks the repository owner - needs a list to protect, and these are
not obvious from the module names above. `engine::lane` is a PUBLIC module
(`LaneGate`, `BackfillAdmission`, `WaitFailed`, `DEFAULT_BACKFILL_LANE_CAPACITY`),
public because `BackfillRunner::run_partition` is public and gained two
parameters, a `LaneGate` and a `ChangeDelivery`. `LaneGate::wake` within it has
no in-engine caller - teardown pulses the slot's
`PendingCoverage::wake_capacity` directly, having the coverage and no gate - and
is kept as published surface rather than removed, the same standing as
`SyncControl::record_checkpoint`. `ChangeDelivery` is public from `multiplexer`
(`new`, `sender`, `publish_backfill`, `subscribe`, `min_live`). `PendingCoverage`
gained sixteen public methods: `mark_delivered`, `mark_received`
(the receipt bound's permit), `receiver_joined` and `receiver_departed` (the
live numbered set the slowest-reader rule is evaluated against),
`backfill_in_flight`,
`await_backfill_capacity`, `release_undelivered`, `scope_fence` and
`retained_history` with the bound, then `register_walk_marker`,
`walk_watermark`, `minted_here`, `note_undelivered`, `take_backfill_discard`,
`take_backfill_discards` and `undelivered_watermark` with the completion
guarantee. `PublicationReceipt` gained a public field,
`walk_watermark: Option<u64>`, which is a struct-literal break for any
downstream constructing one. `BackfillConfig` gained `lane_capacity`, and
`WriterRequest` a `DiscardBackfillProgress` variant (crate-internal - that enum
is not re-exported). Everything else the lane work touched is `pub(crate)` or
narrower.

## Caller versions on PIM updates

Contact and event update patches forward optional `expected_version` intact to
the account. `MutationConcurrency::ExpectedVersion` identifies the individual
update doors that enforce it. `StateBased` retains its legacy meaning of
provider-observed guards; it is not a caller-version promise. Bulk mutation
inputs and delete methods do not yet carry caller versions.
