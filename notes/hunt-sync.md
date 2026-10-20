# Defect hunt: bifrost-sync

Scope: `crates/sync/`, read against `reference/sync.md` and `reference/error-model.md`.
The crate is about 39k lines. This pass went deep on the scheduler and budget, the
changes driver and poll loop, the push reconciler and sink, the bulk mutation
pipeline and read-back guards, the ack writer, and the reopen/recovery dispatch.
It did NOT cover `cursor/coverage.rs`, `cursor/ledger.rs`,
`cursor/ledger_envelope.rs`, `backfill/*`, `engine/lane.rs` or `repair.rs` in
depth. Those are the largest files and need their own pass.

Findings are ordered roughly by severity. Each one names the claim it breaks.

---

## 1. A transient `get_ledger` failure at writer start durably erases the account's debt ledger

`engine/ack.rs`, `ack_writer`: if `store.get_ledger` errors, the writer logs
"starting empty, which can only UNDER-report debt" and carries on with
`DebtLedger::default()`. Every later durable mutation then writes that
in-memory ledger whole: `apply_transition { ledger: ledger.clone() }` for acks,
`ReattachInsert`, `PersistEstablished` and `PersistBackfill`, and
`put_ledger(ledger.clone())` for `persist_ledger_only`. The first acknowledged
checkpoint after a flaky read therefore overwrites the persisted ledger with an
empty one. Every open obligation, barrier, waiver and block recorded by earlier
sessions is destroyed for good.

Claims broken:
- The log line itself. It does not merely under-report for this session: it
  writes the under-report over the durable truth.
- The crate's own stated asymmetry (`WriterRequest::RecordDebt` docs, the lag
  section of `reference/sync.md`): "under-reporting [debt] costs objects nobody
  ever sees again." Here it happens silently after one transient read error.
- The "never write what an abort would have to restore" and "fail closed on
  error" rule from the reattach section. This is the same shape (a swallowed
  read error feeding a destructive write) on the ledger instead of the cursor
  row.

Direction for a fix: a ledger load failure has to fail closed. Either refuse
every ledger-bearing write until a load succeeds (retry the load and fail acks
with a store error meanwhile), or fail the attach. A writer that has not read
the ledger must never write one.

## 2. Terminal-parked scopes and a terminated lifecycle reader are never revived, even by a successful reattach

`multiplexer/mod.rs`: a `RecoveryPlan::Terminal` drive outcome parks the
scope's `ScopeToken` as an uncancelled tombstone. `spawn_missing_scope_polls`
treats it as owned and never respawns the scope, and the push reconciler checks
`scope_is_parked` and refuses to drive it. The only production path that clears
a tombstone is `cancel_scope_token`, reached from lifecycle `Deleted` and the
reopen listener's `ScopeDeleted` arm. The comment at the spawn site says so.
`reattach.rs` never touches `scope_tokens`. Nor do `restart_scope`,
`restart_account` or `handle_schema_incompatible`.

The result: an auth-expired (terminal) `Account` scope parks. The consumer fixes
credentials and calls `SyncEngine::reattach`. The reattach succeeds, reinstalls
the cursor, and the scope still never polls or reconciles again for the rest of
the attachment. A later `RestartScope` for that scope (lifecycle `Created`, a
mutation-raised directive) re-establishes the cursor into the registry, and it
also stays dead.

Likewise the lifecycle reader. On `LifecycleTermination::Terminate` it
broadcasts and `return`s from the spawned task. Nothing respawns it on a
generation bump, so after a successful reattach the account has no lifecycle
detection until detach.

Claims broken:
- `reference/sync.md` says a parked scope "is only revived by an explicit
  scope-retiring path: `cancel_scope_token` (lifecycle deletion, engine restart
  of the scope)". Engine restart of the scope does not clear it.
- The reattach contract (staged swap of handle and topology, "reapplies...
  recreates...") implies a working account afterwards.
- The reconciler's own comment (`drive_scopes`) lists "an engine restart of the
  scope" as a clearing path. It is not one.

Structural fix: key the tombstone to the registry/drive generation (or the
account generation). A reattach swap or a successful re-establishment of the
scope then implicitly clears it, and the lifecycle reader lives in a loop that
restarts on generation change instead of returning.

## 3. A `Terminated` event in a drive is broadcast and then silently dropped when the boundary is not `Run`

`multiplexer/changes.rs`, `drive_changes_stream`: after publishing an event it
checks `post_publish_boundary(...)` BEFORE `is_done` and `terminated_error`.
Under `Pause` (or `Stop`), a `SyncEvent::Terminated(err)` the provider just
yielded is broadcast to subscribers, but the driver returns
`ChangesEvent::Paused`/`Stopped` instead of `ChangesEvent::Terminated(err)`. So
`handle_drive_outcome` never runs `plan_recovery`:
- A `DisableScope`, `SchemaIncompatible`, `RestartAccount` or
  `OperatorOverrideRequired` directive is never forwarded to the reopen
  listener.
- A terminal verdict never parks the scope, so after resume the scope re-drives
  and re-broadcasts `Terminated`.
- A Retry/Reconcile throttle deadline is never recorded.

Consumers have seen `Terminated` on the stream while the engine acted as if
nothing happened. The window is exactly a consumer pause, which is common.

Claim broken: the `ChangesEvent::Terminated` doc ("Carries the full
`AccountError` so the engine dispatches via the derived `RecoveryClass`") and
`reference/sync.md`'s "`handle_drive_outcome` normalizes every `Error::Account`
onto the same `plan_recovery` path".

Fix: the terminated arm must win over the boundary peek, or the boundary check
must return the terminated outcome when one is present.

## 4. One provider-minted bad change checkpoint triggers an account-wide schema reset

`changes.rs`: when a batch's `Checkpoint::Change` fails `validate_envelope`, the
driver builds `recovery::cursor_decode_failure(SyncChanges)`. That error carries
NO scope (`cursor_decode_failure` sets none) and derives
`Engine(SchemaIncompatible)`. `handle_schema_incompatible` then deletes every
registered scope's cursor AND its backfill rows, completion markers included, and
re-establishes all of them. On the next attach every scope re-walks its full
inventory. The driver knew the scope (`&scope` is in hand).
`Multiplexer::fuse_inventory_done` has the same scope-less construction.

Claims broken:
- The per-scope blast-radius principle the crate applies everywhere else
  (`DisableScope` at attach, "one dead share must not fail the whole account",
  sync-D7 "account keeps running for the scopes that succeed").
- The `cursor_decode_failure` doc: it is meant for "the on-disk schema below
  `MIN_MIGRATABLE`", meaning stored rows. A wrong outer version on a FRESH
  checkpoint from the account is an `Account` contract violation, the very
  case `contract_violation` / `Internal(AccountContract)` exists for, not an
  engine schema migration.

Fix: classify a fresh bad envelope as `contract_violation` scoped to the
cursor. Reserve `SchemaIncompatible` for stored rows the engine itself decodes.

## 5. Bulk mutation campaigns abort entirely on a transient admission refusal, and any abort discards all per-id accounting

`engine/bulk.rs`, `run_bulk_pipeline`: `scheduler.admit` returning `Err` (lane
full) does `first_error.get_or_insert(error)`. The campaign then winds down in
"aborting" mode and returns `Err(error)`. The same happens on a stream-level
`Terminal` termination and on shutdown. On every `Err` return the
`MutationCounters` (applied / skipped / failed / pending per id) are thrown away.
The "aborting" mode even keeps running read-back passes (real wire work) whose
results are then discarded with the counters.

Claims broken:
- `scheduler/mod.rs` module doc and `reference/sync.md`: "A refused admission is
  TRANSIENT, never terminal... A caller must back off." The poll loop and push
  sweep obey this. The mutation pipeline treats it as fatal for a whole
  campaign that may already have applied thousands of writes.
- `bulk_set_flags` doc: "terminal recovery -> `failed_terminal`" and "The
  campaign returns with a partial outcome accounting for work seen so far." A
  stream-level terminal returns no accounting at all, so the consumer cannot
  tell which ids landed.
- Minor: the inner stream loop sets `first_error = Some(Error::ShuttingDown)`
  (and `Some(Error::Account(..))` on terminal) by plain assignment, overwriting
  an earlier first error. Elsewhere it uses `get_or_insert`.

Structural direction: return the counters alongside the error (a
`CampaignOutcome { counters, error: Option<Error> }`), and retry admission
refusals on a backoff like every other admitted path.

## 6. `mutation_max_retries` permits one fewer resubmission than it says

`types.rs` documents `mutation_max_retries` as "attempts up to this many
resubmissions". `bulk.rs` increments `cohort.attempt` after every submission,
the first one included, and resubmits only while `cohort.attempt < max_retries`.
With the default 5 that is 4 resubmissions, and `mutation_max_retries = 1` means
zero retries.

## 7. The read-back guard turns retryable and never-landed writes into `failed_terminal`

`bulk.rs`: ids that end in `PendingRetry` (retry budget spent or
`retry_queue_cap` exhausted), `PendingReadback` (from `Uncertain`, `Reconcile`,
or `Retry { AfterStateRefresh }`) and `Downgraded` all go to the read-back. A
`Mismatched` observation maps to `MutationBucket::FailedTerminal`.

- `MutationCounters` doc: "`failed_terminal`... means the item itself terminally
  failed". An item whose provider said `Retry { SameRequest }` but which lost the
  `retry_queue_cap` race is reported terminal. `MutationConfig::retry_queue_cap`
  says excess targets "remain `PendingRetry` and pass through the read-back
  guard". In fact, unless their write happened to land, they leave it as
  `failed_terminal`.
- `RetryDisposition::AfterStateRefresh` is, by its name, "retry after
  refreshing state". The pipeline never retries it. It reads back once, and a
  mismatch is final. Once the read-back has proven an `Uncertain` write did not
  land, replay is exactly the safe case the uncertainty lane was protecting.
  The pipeline still never resubmits.

At minimum a mismatch for a retry-classified id should report as pending or
retry-exhausted, not terminal. Better, a proven non-landing write should go back
into the retry lane.

## 8. Push sink: queued invalidations can be lost outright, and the ingress queue is unbounded

`push/mod.rs`, `forward_events`: on a full per-account channel, a non-lossless
event is replaced by a coalesced `Invalidated { Unknown }` and sent with a 100ms
`timeout`. If the reconciler is busy past that (an account-wide throttle sleep
inside `drive_scopes`, a slow drive), the coalesced event is DROPPED. The
specific scopes of the original hint are then lost until their next poll (up to
`poll_max`, 30 min by default). Separately, the per-registration ingress is
`mpsc::unbounded_channel`, and the forwarder blocks indefinitely on a lossless
event (`sender.send(..).await`). A webhook storm behind one blocked
`Warning`/`Terminated` grows memory without bound.

Claims broken: `MultiplexerConfig::watch_capacity` presents the push buffer as
bounded. The drop counter is documented as counting "PAYLOAD loss, not delivery
loss", yet delivery loss also happens and is counted the same way.
`WatchEvent::Disconnected` is also coalesced into an invalidation, so the
"push transport disconnected" warning the reconciler would emit is lost.

A sticky "needs full reconcile" flag that the reconciler drains would replace
both the timeout and the unbounded queue: lossless, bounded, and no worse than
the coalesced event.

## 9. Operator waive/block report failure while taking effect anyway

`ack.rs`, `WriterRequest::OperatorDecision`: `ledger.waive` / `ledger.block`
mutate the authoritative in-memory ledger in place, then `persist_ledger_only`
runs. If persistence fails the operator gets `Err`, but the waiver stays in
memory and is written to disk by the next ledger-bearing write. The sibling
`CrossWaivedBarriers` arm does this correctly: it clones, persists, and adopts
only on success. Waive is described as "the only path to accepted loss", so
reporting it as failed while applying it is the worst direction to get wrong.

## 10. Terminated events carry non-terminal recovery classes

`recovery::establish_failure_error` is byte-for-byte `restart_scope_error`:
`SyncState(CursorInvalid)`, which derives `Engine(RestartScope(scope))`. It is
broadcast as `SyncEvent::Terminated` when a scope's re-establishment budget or
the account restart budget is exhausted on engine-level errors
(`re_establish_scope_with_backoff`, `restart_account`). A consumer that
dispatches on the `Terminated` error's `RecoveryClass` (the error model's whole
point) is told "restart this scope" about a scope the engine just abandoned,
and for `restart_account` is told `RestartScope(Account)` about an account that
is now paused with `RetryBudgetExhausted`. The doc comment claims the synthetic
error exists to honour "after three failures broadcast `Terminated(last_error)`".
It honours the event but not the meaning. It needs a classification that
derives terminal or operator-attention.

## 11. Documentation disagreements

- `reference/sync.md` "Batch boundary validation" says a boundary violation
  "classifies as `Protocol(ContractViolation)`, which derives to the terminal
  `ProviderContractViolation`". The code (`recovery::contract_violation`) and its
  test say `Internal(AccountContract)` -> `InternalFailure`, deliberately so
  ("blames the implementation not the provider"). The reference is stale.
- `EngineConfig::lane_capacity` doc: "On overflow the oldest item is shed and a
  counter incremented." That holds for the `submit` lanes. The same capacity
  bounds the admission lanes, which refuse the NEWEST request and count
  nothing. All engine work goes through admission.
- `LaneQueue::push` doc: "the engine surfaces shed counters through
  observability". `Scheduler` keeps its `LaneQueue`s private and exposes no
  `shed_count`, and admission refusals have no counter at all, so the scheduler's
  sheds cannot be observed. `LaneShedPolicy::DropNewest` is a published variant
  nothing can select (the field is set only to the default). The reference
  admits this. It is still a published type advertising a capability.
- Push reconciler pause loop comment: "Pause / CheckpointNow / Stop all keep the
  loop parked". The code's `continue` re-evaluates
  `while peek == Pause`, so `CheckpointNow` and `Stop` both UNPARK it.
  `Stop` racing a still-uncancelled shutdown token then reaches a drive, which
  the drive's own boundary arm stops. Harmless, but the comment is wrong.
- `scheduler/mod.rs` `Scheduler::with_lane_capacity` doc refers to "`H4` in the
  engine notes", and `EngineConfig::detach_timeout` / `lane_capacity` cite `H1`
  / `H4`. Those are pointers into notes that no longer exist (AGENTS.md:
  comments must not point at notes).

## 12. Smaller defects

- `SchedulerConfig::starvation_floor = 0` is accepted unvalidated. Both
  `try_pull` and the admission dispatcher's `visit_order` test
  `higher_consecutive >= floor`, which is always true, so lower lanes are
  ALWAYS visited first and priority is fully inverted.
  `ConcurrencyBudget::validate` exists. The scheduler config has no equivalent.
- `BudgetGate::forget` at detach removes the account's semaphores, but a
  dispatcher acquisition still in flight for that account (or a later stray
  `acquire`) re-creates them through `entry().or_insert_with`. The entries leak
  for the engine's lifetime. A re-attach then `register`s with `insert`,
  replacing semaphores that an in-flight acquisition may still hold permits
  from, which briefly doubles the account's concurrency.
- `drive_changes_stream`'s three contract-violation paths send `Terminated` on
  the raw sender with no `publish_if_drive_generation` fence. A drive fenced out
  by a reattach can still broadcast a scope `Terminated` for the old handle's
  misbehaviour after the cutover.
- `handle_schema_incompatible` resets only `cursors.all_scopes()`. Durable rows
  for scopes not currently in the registry (deferred-inventory scopes still
  establishing, scopes whose re-establishment budget ran out) keep their
  completion markers, so they are exempt from the re-walk the schema reset
  claims to force ("delete every backfill checkpoint - the completion marker
  included").

## 13. Structural observations (not defects)

- Every admission (poll, push, backfill partitions, deferred inventory,
  mutation) passes `control.priority_snapshot()`, the ACCOUNT's priority. The
  four-lane scheduler therefore ranks accounts against each other and never
  ranks work kinds: within one account, a historical backfill partition
  competes on equal terms with a live poll and with foreground-visible
  mutations. Only the sync/mutation sub-pool split separates them. If the lanes
  are meant to keep cold-start backfill from delaying live mail, nothing routes
  backfill to `Background`/`Bulk` today. If that is not the intent, four lanes
  per account is more machinery than one priority per account needs.
- The tombstone map (`ScopeTokens`), the registry's per-scope liveness token,
  the drive lease, and `DriveGeneration` are four overlapping mechanisms for
  "who may drive this scope". Finding 2 is a direct product of the overlap:
  revival is wired to one mechanism and not the others. Folding the parked
  state into the registry entry (terminal is a property of a scope incarnation)
  would make a re-establishment or a reattach clear it by construction.
- `bulk.rs::run_bulk_pipeline` is one 550-line function holding cohort state,
  per-id state, abort mode and read-back mode as booleans on a struct
  (`submitted`, `readback`, `done`) and on locals (`aborting`,
  `blocked_by_engine`). Findings 5 to 7 all live in transitions between those
  flags. An explicit per-cohort state machine
  (`Queued -> Submitted(attempt) -> Readback -> Done(outcome)`) with the
  campaign outcome returned on every exit path would make these gaps
  unrepresentable.
