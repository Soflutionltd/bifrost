# bifrost-sync defect findings

1. An entry is removed entirely when completely resolved. No historical record
   stays here.
2. Stable IDs never change and are never reused; removal leaves a gap.
3. An entry adjudicated against, verified incorrect, or whose outcome is that no
   action is taken owes comments at the code sites it names - and, where the
   claim touches a documented contract, the relevant `reference/` or `docs/`
   page - before the entry is removed, so the finding is not hunted again.
4. Once all findings are resolved, the file gets deleted.
5. Finding IDs are never written into the code or other documents. They are
   stable only until this document is drained; the next hunt writes new ones,
   and they are never deduplicated through git history. Carry the context
   inline instead.

Filed from the October 2026 defect hunt: the sync engine scope, plus the
engine-side read-back finding from the sync adoption scope. Provider-side
recovery triage that bypasses the engine is in SHARED-012 and SHARED-013.

Coverage note from the engine hunter: `cursor/coverage.rs`, `cursor/ledger.rs`,
`cursor/ledger_envelope.rs`, `backfill/*`, `engine/lane.rs` and `repair.rs`
were NOT covered in depth. They are the largest files and need their own pass.

## SYNC-002 - Terminal-parked scopes and a terminated lifecycle reader are never revived, even by a successful reattach

Scope: sync engine.

`multiplexer/mod.rs`: a `RecoveryPlan::Terminal` drive outcome parks the
scope's `ScopeToken` as an uncancelled tombstone. `spawn_missing_scope_polls`
treats it as owned and never respawns, and the push reconciler checks
`scope_is_parked` and refuses to drive it. The only production path clearing a
tombstone is `cancel_scope_token`, reached from lifecycle `Deleted` and the
reopen listener's `ScopeDeleted` arm. `reattach.rs`, `restart_scope`,
`restart_account` and `handle_schema_incompatible` never touch
`scope_tokens`.

So an auth-expired (terminal) `Account` scope parks; the consumer fixes
credentials and calls `SyncEngine::reattach`; the reattach succeeds and
reinstalls the cursor; the scope never polls or reconciles again for the rest
of the attachment. A later `RestartScope` (lifecycle `Created`, a
mutation-raised directive) re-establishes the cursor into the registry and it
stays dead too.

Likewise the lifecycle reader: on `LifecycleTermination::Terminate` it
broadcasts and returns from the task; nothing respawns it on a generation
bump, so after a successful reattach the account has no lifecycle detection
until detach.

Claims broken: `reference/sync.md` (a parked scope "is only revived by an
explicit scope-retiring path: `cancel_scope_token` (lifecycle deletion, engine
restart of the scope)"; engine restart does not clear it); the reattach
contract ("reapplies... recreates...") implies a working account afterwards;
the reconciler's own `drive_scopes` comment lists engine restart as a clearing
path.

Structural fix: key the tombstone to the registry/drive (or account)
generation so a reattach swap or successful re-establishment clears it, and run
the lifecycle reader in a loop that restarts on generation change. The hunter
notes this is a product of overlap: the tombstone map (`ScopeTokens`), the
registry's per-scope liveness token, the drive lease and `DriveGeneration` are
four mechanisms for "who may drive this scope", and revival is wired to one of
them. Folding parked state into the registry entry (terminal is a property of a
scope incarnation) clears it by construction.

## SYNC-003 - A `Terminated` event in a drive is broadcast and then dropped when the boundary is not `Run`

Scope: sync engine.

`multiplexer/changes.rs`, `drive_changes_stream`: after publishing an event it
checks `post_publish_boundary(...)` BEFORE `is_done` and `terminated_error`.
Under `Pause` (or `Stop`), a `SyncEvent::Terminated(err)` just yielded is
broadcast, but the driver returns `ChangesEvent::Paused` / `Stopped` instead of
`ChangesEvent::Terminated(err)`, so `handle_drive_outcome` never runs
`plan_recovery`:
- `DisableScope`, `SchemaIncompatible`, `RestartAccount` or
  `OperatorOverrideRequired` directives never reach the reopen listener;
- a terminal verdict never parks the scope, so after resume it re-drives and
  re-broadcasts `Terminated`;
- a Retry/Reconcile throttle deadline is never recorded.

Consumers saw `Terminated` while the engine acted as if nothing happened, in
exactly the window of a consumer pause. Claims broken: the
`ChangesEvent::Terminated` doc ("Carries the full `AccountError` so the engine
dispatches via the derived `RecoveryClass`") and `reference/sync.md`
("`handle_drive_outcome` normalizes every `Error::Account` onto the same
`plan_recovery` path"). Fix: the terminated arm wins over the boundary peek, or
the boundary check returns the terminated outcome when one is present.

## SYNC-004 - One provider-minted bad change checkpoint triggers an account-wide schema reset

Scope: sync engine. Graph's own cursor decoder has a sibling instance,
GRAPH-009.

`changes.rs`: when a batch's `Checkpoint::Change` fails `validate_envelope`,
the driver builds `recovery::cursor_decode_failure(SyncChanges)`, which carries
NO scope and derives `Engine(SchemaIncompatible)`.
`handle_schema_incompatible` then deletes every registered scope's cursor AND
backfill rows, completion markers included, and re-establishes all of them; on
the next attach every scope re-walks its full inventory. The driver had
`&scope` in hand. `Multiplexer::fuse_inventory_done` has the same scope-less
construction.

Claims broken: the per-scope blast-radius principle applied everywhere else
(`DisableScope` at attach, "one dead share must not fail the whole account");
the `cursor_decode_failure` doc, meant for "the on-disk schema below
`MIN_MIGRATABLE`", i.e. stored rows. A wrong outer version on a FRESH
checkpoint is an `Account` contract violation, the case `contract_violation` /
`Internal(AccountContract)` exists for. Fix: classify a fresh bad envelope as
`contract_violation` scoped to the cursor; reserve `SchemaIncompatible` for
stored rows the engine decodes.

## SYNC-005 - Bulk mutation campaigns abort on a transient admission refusal, and any abort discards all per-id accounting

Scope: sync engine.

`engine/bulk.rs`, `run_bulk_pipeline`: `scheduler.admit` returning `Err` (lane
full) does `first_error.get_or_insert(error)`; the campaign winds down in
aborting mode and returns `Err(error)`. Same on a stream-level `Terminal`
termination and on shutdown. On every `Err` return the `MutationCounters`
(applied / skipped / failed / pending per id) are discarded; aborting mode even
keeps running read-back passes (real wire work) whose results are thrown away.

Claims broken: `scheduler/mod.rs` module doc and `reference/sync.md` ("A refused
admission is TRANSIENT, never terminal... A caller must back off"), which the
poll loop and push sweep obey; `bulk_set_flags` doc ("terminal recovery ->
`failed_terminal`", "The campaign returns with a partial outcome accounting for
work seen so far"): a stream-level terminal returns no accounting, so the
consumer cannot tell which ids landed. Minor: the inner loop sets
`first_error = Some(Error::ShuttingDown)` (and `Some(Error::Account(..))` on
terminal) by plain assignment, overwriting an earlier first error, where
elsewhere it uses `get_or_insert`.

Direction: return counters with the error (`CampaignOutcome { counters, error:
Option<Error> }`) and retry admission refusals on backoff. The hunter notes
SYNC-005 to SYNC-007 all live in transitions between flags in one 550-line
function (`submitted`, `readback`, `done` on a struct, `aborting`,
`blocked_by_engine` as locals); an explicit per-cohort state machine
(`Queued -> Submitted(attempt) -> Readback -> Done(outcome)`) returning the
campaign outcome on every exit path makes them unrepresentable.

## SYNC-006 - `mutation_max_retries` permits one fewer resubmission than it says

Scope: sync engine.

`types.rs` documents `mutation_max_retries` as "attempts up to this many
resubmissions". `bulk.rs` increments `cohort.attempt` after every submission,
the first included, and resubmits only while `cohort.attempt < max_retries`.
Default 5 gives 4 resubmissions; `mutation_max_retries = 1` gives zero.

## SYNC-007 - The read-back guard turns retryable and never-landed writes into `failed_terminal`

Scope: sync engine.

`bulk.rs`: ids ending in `PendingRetry` (budget spent or `retry_queue_cap`
exhausted), `PendingReadback` (from `Uncertain`, `Reconcile`, or `Retry {
AfterStateRefresh }`) and `Downgraded` all go to read-back, and a `Mismatched`
observation maps to `MutationBucket::FailedTerminal`.

- `MutationCounters` doc: `failed_terminal` "means the item itself terminally
  failed". An item the provider called `Retry { SameRequest }` that lost the
  `retry_queue_cap` race is reported terminal. `MutationConfig::retry_queue_cap`
  says excess targets "remain `PendingRetry` and pass through the read-back
  guard"; unless their write happened to land they leave as `failed_terminal`.
- `RetryDisposition::AfterStateRefresh` is by name "retry after refreshing
  state", but the pipeline never retries it: one read-back, and a mismatch is
  final. Once read-back proves an `Uncertain` write did not land, replay is the
  safe case the uncertainty lane protected, and the pipeline still never
  resubmits.

At minimum a mismatch for a retry-classified id reports pending or
retry-exhausted, not terminal; better, a proven non-landing write returns to
the retry lane.

## SYNC-008 - Read-back of `Downgraded` outcomes compares against the request, not the reported effect

Scope: sync adoption (engine-side seam).

`reference/sync.md` ("Mutation pipeline") routes `Downgraded` to
`PendingReadback` so "the final accounting come[s] from observed state". But
`crates/sync/src/mutation/readback.rs` compares the observation against the
ORIGINAL request, not the `actual` effect the provider reported:
- Gmail trash-instead-of-destroy (`apply_destroy` in
  `crates/google/src/account/mutation.rs`): the destroy read-back treats a
  successful fetch as `ReadbackTarget::Destroy => false`, i.e. `Mismatched`, so
  every trashed message is counted `failed_terminal` (`still_failed`). The
  consumer is told the delete failed when the message moved to Trash, and the
  `MutationEffect` saying so is discarded.
- Gmail partial flags (`label_patch_successes` with unsupported flags):
  `matches_target` checks the requested `FlagOp`, including the flag Gmail
  cannot store, so the read-back is deterministically `Mismatched`.

Each costs a full hydrate per id to learn an answer knowable without one, and
misreports what happened. Claims broken: the "observed state" justification in
sync.md and the reason `Downgraded { actual }` exists. Fix at the seam:
read-back verifies against `actual` (trashed means in Trash, partial means the
supported subset), and the counters get a `downgraded` bucket instead of
folding into `failed_terminal`. Interacts with SYNC-007.

## SYNC-009 - Push sink: queued invalidations can be lost outright, and the ingress queue is unbounded

Scope: sync engine.

`push/mod.rs`, `forward_events`: on a full per-account channel a non-lossless
event is replaced by a coalesced `Invalidated { Unknown }` and sent with a
100 ms `timeout`. If the reconciler is busy past that (an account-wide throttle
sleep inside `drive_scopes`, a slow drive), the coalesced event is DROPPED and
the original hint's scopes are lost until their next poll (up to `poll_max`,
30 min default). Separately the per-registration ingress is
`mpsc::unbounded_channel`, and the forwarder blocks indefinitely on a lossless
event (`sender.send(..).await`), so a webhook storm behind one blocked
`Warning` / `Terminated` grows memory without bound.

Claims broken: `MultiplexerConfig::watch_capacity` presents the buffer as
bounded; the drop counter is documented as counting "PAYLOAD loss, not delivery
loss", yet delivery loss happens and is counted the same way.
`WatchEvent::Disconnected` is also coalesced into an invalidation, so the "push
transport disconnected" warning is lost. A sticky "needs full reconcile" flag
drained by the reconciler would replace both the timeout and the unbounded
queue: lossless, bounded, no worse than the coalesced event.

## SYNC-011 - `Terminated` events carry non-terminal recovery classes

Scope: sync engine.

`recovery::establish_failure_error` is byte-for-byte `restart_scope_error`:
`SyncState(CursorInvalid)` -> `Engine(RestartScope(scope))`. It is broadcast as
`SyncEvent::Terminated` when a scope's re-establishment budget or the account
restart budget is exhausted (`re_establish_scope_with_backoff`,
`restart_account`). A consumer dispatching on the `RecoveryClass` is told
"restart this scope" about a scope the engine abandoned, and for
`restart_account` `RestartScope(Account)` about an account now paused with
`RetryBudgetExhausted`. The doc says the synthetic error honours "after three
failures broadcast `Terminated(last_error)`"; it honours the event, not the
meaning. It needs a classification deriving terminal or operator-attention.

## SYNC-012 - `SchedulerConfig::starvation_floor = 0` inverts priority

Scope: sync engine.

Accepted unvalidated. Both `try_pull` and the admission dispatcher's
`visit_order` test `higher_consecutive >= floor`, always true at 0, so lower
lanes are always visited first. `ConcurrencyBudget::validate` exists; the
scheduler config has no equivalent.

## SYNC-013 - `BudgetGate::forget` at detach races in-flight acquisitions

Scope: sync engine.

`forget` removes the account's semaphores, but a dispatcher acquisition in
flight for that account (or a later stray `acquire`) re-creates them through
`entry().or_insert_with`; the entries leak for the engine's lifetime. A
re-attach then `register`s with `insert`, replacing semaphores an in-flight
acquisition may still hold permits from, briefly doubling the account's
concurrency.

## SYNC-014 - Contract-violation `Terminated` sends bypass the drive-generation fence

Scope: sync engine.

`drive_changes_stream`'s three contract-violation paths send `Terminated` on
the raw sender with no `publish_if_drive_generation` fence, so a drive fenced
out by a reattach can still broadcast a scope `Terminated` for the old handle's
misbehaviour after the cutover.

## SYNC-015 - `handle_schema_incompatible` resets only registered scopes

Scope: sync engine.

It resets only `cursors.all_scopes()`. Durable rows for scopes not currently in
the registry (deferred-inventory scopes still establishing, scopes whose
re-establishment budget ran out) keep their completion markers, exempt from the
re-walk the reset claims to force ("delete every backfill checkpoint - the
completion marker included").

## SYNC-016 - Documentation disagreements in the engine

Scope: sync engine.

- `reference/sync.md` "Batch boundary validation" says a boundary violation
  "classifies as `Protocol(ContractViolation)`, which derives to the terminal
  `ProviderContractViolation`". Code (`recovery::contract_violation`) and its
  test say `Internal(AccountContract)` -> `InternalFailure`, deliberately
  ("blames the implementation not the provider"). The reference is stale.
- `EngineConfig::lane_capacity` doc: "On overflow the oldest item is shed and a
  counter incremented." True for `submit` lanes; the same capacity bounds the
  admission lanes, which refuse the NEWEST request and count nothing, and all
  engine work goes through admission.
- `LaneQueue::push` doc: "the engine surfaces shed counters through
  observability". `Scheduler` keeps its `LaneQueue`s private and exposes no
  `shed_count`, and admission refusals have no counter, so sheds cannot be
  observed. `LaneShedPolicy::DropNewest` is a published variant nothing can
  select (the field is only ever set to the default). The reference admits
  this; it is still a published type advertising a capability.
- Push reconciler pause-loop comment: "Pause / CheckpointNow / Stop all keep the
  loop parked". The `continue` re-evaluates `while peek == Pause`, so
  `CheckpointNow` and `Stop` UNPARK it; `Stop` racing an uncancelled shutdown
  token then reaches a drive, which the drive's boundary arm stops. Harmless;
  the comment is wrong.
- `Scheduler::with_lane_capacity` doc refers to "`H4` in the engine notes", and
  `EngineConfig::detach_timeout` / `lane_capacity` cite `H1` / `H4`: pointers
  into notes that no longer exist, which AGENTS.md forbids in comments.

## SYNC-017 - `apply_repair_resolutions` mutates the authoritative ledger before persisting

Scope: sync engine. Lateral from the ack writer fix.

Same shape as the operator waive/block defect that was just closed: the repair
resolution path in `engine/ack.rs` applies discharges to the in-memory ledger
and then persists, so a failed write leaves the discharges available to later
ledger-bearing writes. Apply to a candidate ledger and adopt it only on success.
