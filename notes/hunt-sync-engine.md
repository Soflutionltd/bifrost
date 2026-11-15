# Hunt: sync-engine (shape and placement)

Scope read: `crates/sync/src/engine/*`, `scheduler/`, `multiplexer/` (mod, changes, poll; fusion as far as needed), `cancel/`, `control.rs`, `recovery.rs`, `repair.rs`, `backoff.rs`, `error.rs`, `types.rs`, `lib.rs`, the integration tests under `crates/sync/tests/`, plus the parts of `push/`, `backfill/`, `cursor/coverage.rs` and `bifrost-types` the questions led into.

The report is in four parts: live and latent defects found on the way (they come first because they are the cheapest to act on), then the three questions in the brief. Each finding names the sites, whether they already disagree, and where the single owner should live. Severity tags on the defects are my own estimate.

---

## Part 0 - Defects and surprises found along the way

These are bugs or contract gaps, kept apart from the shape proposals that follow.

### D1 (P2, latent hang). Engine auto-pause overwrites a detach's `Stop`

`engine/reattach.rs::engine_pause` writes `ctx.boundary_tx.send_replace(BoundaryRequest::Pause)`. That is the raw watch sender, not `Boundary::set_unless_stopped`. It runs from four places: `restart_account` exhaustion (twice), `handle_schema_incompatible` exhaustion, and the `OperatorOverrideRequired` arm. All of them execute inside the reopen listener's `handle_account_error`, which by the crate's own documentation does not stop for shutdown once it has started. `detach_inner` writes `Stop` and then waits for that listener.

So a detach that lands during the last restart attempt, or during an operator-override directive forwarded by a campaign, can have its `Stop` replaced by `Pause`. Two consequences:

- `SyncControl::wait_for_checkpoint_at_or_after` gets out of a detach only through its `boundary.peek() == Stop` arm, because the consumer's own `SyncControl` clone keeps the checkpoint watch open. After the overwrite, a consumer `pause()` or `checkpoint_now()` waiting on an unacknowledged registration never returns.
- `Boundary` documents `Stop` as terminal, and that rule is enforced only on the consumer-facing writers (`set_unless_stopped`, `request_checkpoint`). The engine's own writers bypass it.

Root cause is a shape problem, covered in C1 and C9: the raw `watch::Sender<BoundaryRequest>` is threaded through `AccountSlot`, `SlotContext` and `RecoveryContext`, so any holder can make any write.

### D2 (P2, latent data loss). `ack_checkpoint`'s `scope` parameter is never checked against the checkpoint

`SyncEngine::ack_checkpoint(account, scope, checkpoint, publication)` sends `AckRequest { scope, checkpoint, .. }`. In `engine/ack.rs::ack_writer`, the `Ack` arm discharges the provisional reattach mark with `provisional.remove(&req.scope)`, using the consumer-supplied scope. It then persists `req.checkpoint`, which may belong to a different scope. Nothing compares the two.

Suppose a consumer passes scope X with scope Y's checkpoint:

1. Y's row is written.
2. X's provisional mark is cleared and Y's is not.
3. A later `ReattachAbort` deletes Y's row, which the consumer has just acknowledged.

That is exactly the loss the provisional machinery exists to prevent. The same redundancy also exists a third time: `PublicationId.1.checkpoint` carries the lane too. See A1 for the shape fix (derive the scope, do not accept it).

### D3 (P2/P3, disagreement). A freshly minted cursor with a bad envelope gets three different classifications

The question is what to do when an `Account` hands the engine a brand-new `ChangeCursor` whose envelope fails `validate_envelope()`.

| Site | Classification |
|---|---|
| `multiplexer/changes.rs::drive_changes_stream` | `fresh_cursor_contract_error`, which is `Internal(AccountContract)`, terminal, scoped |
| `multiplexer/mod.rs::fuse_inventory_done` | same as the row above |
| `engine/reattach.rs::run_establish`, `Ready` arm | `cursor_decode_failure`, which is `SyncState(SchemaIncompatible)` and derives `Engine(SchemaIncompatible)`, the persisted-schema directive |
| `engine/attach.rs::persist_cursor` | bare `Error::SchemaIncompatible`, which fails the whole attach as "checkpoint envelope schema is incompatible" |

`reference/sync.md` says fresh account-authored envelopes are contract failures and that schema recovery is reserved for persisted envelopes. Two of the four sites contradict that.

The single owner should be one function that takes the cursor's origin, something like `classify_envelope(cursor, Origin::Fresh | Origin::Stored)`, with every site calling it.

### D4 (P3). Campaigns trust the account's item ids, with no exactly-once check at the boundary

`run_bulk_pipeline` inserts every `ItemOutcome` it receives into `outcomes` keyed by `ObjectId(item.0)`, whether or not that id was submitted in this round or exists in the campaign at all. So:

- An outcome for an unrequested id becomes a new counted entry.
- A duplicate outcome is last-write-wins.

`BatchOutcomeBuilder::finalize` enforces exactly-once only for providers that use it. The streaming bulk path is a `SyncEvent<ItemOutcome>` stream, and the engine validates nothing on it. This is the same boundary gap that inventory batches have, and those the engine does validate (`validate_boundary`, which terminates as `AccountContract`). Mutation streams deserve the same guard. The structural fix is in A3.

### D5 (P3). `debt()` and `repair_debt()` read the ledger from the store, not from the writer that owns it

`SyncEngine::debt` and `repair_debt` call `self.checkpoints.get_ledger` directly. But `ack_writer` keeps an authoritative in-memory ledger that can legitimately differ from the store: several arms mutate it in place before persisting, and keep the mutation when the write fails (`RecordDebt`, `RecordBarrier`, `ResetScope`, and both reset passes).

After a transient `put_ledger` failure, `debt()` under-reports, and `repair_debt` plans against a ledger the writer no longer agrees with. Generation-aware discharge limits the damage but does not make the enumeration honest.

The engine has the same habit elsewhere: `engine/backfill.rs` reads `store.get_backfill` directly, while `DiscardBackfillProgress` for the same scope is ordered through the writer. Reads should go through the writer as well. A `WriterRequest::GetLedger` and `GetBackfill`, in the style of `GetChangeCursor`, would do it.

### D6 (P3). The orphan-teardown retrier skips most of the "may the engine do wire work now" gate

`engine/orphan_teardown.rs` peeks the raw boundary and skips on `Pause` or `Stop`. It skips nothing on `CheckpointNow`. It also:

- takes no `begin_activity`, so a pause can report quiescence while a `push_unsubscribe` is in flight;
- takes no scheduler admission;
- does not consult the throttle bucket;
- records no hold when a teardown fails.

Its comment says "a paused account makes no provider calls on the engine's own initiative", and the code does not keep that promise. `repair_debt` likewise does wire work with no activity guard, admission or throttle wait. See C1 for the shape.

### D7 (P3). "The writer is gone" maps to different errors depending on the method

`WriterHandle::reset_account_for_schema_recovery` maps a closed channel to `Error::ShuttingDown`. Every other `WriterHandle` method, plus `ack_checkpoint`, `ack_publication`, `operator_decision` and `repair.rs`, maps it to `Error::Other("writer channel closed: ..")`.

Callers do branch on `Error::ShuttingDown`. For example, `run_deferred_inventory_establishment` returns quietly on it, but logs `Other` as "deferred inventory failed". A consumer calling `ack_checkpoint` during detach gets `Other(String)` instead of `AccountNotAttached`.

### D8 (P3). A pause cut short by detach is classified as `Request(Malformed)`

`control.rs::control_wait_ended` builds `AccountErrorKind::Request(RequestErrorKind::Malformed)` for three situations:

- "account stopped before reaching a safe boundary";
- "checkpoint watch closed";
- "boundary channel closed".

A consumer that branches on kind sees a malformed request it never made. A typed control-wait error is proposed in A6.

### D9 (P3). Jitter is decided twice, and the two rules contradict each other

`engine/reattach.rs::jittered` gives +/-20%. Its comment explicitly rejects `SystemTime::now()` as an entropy source ("a wall-clock jump or low-resolution clock must not influence the jitter") and uses a UUID instead.

`backoff.rs::jitter_seed` uses `SystemTime::now().subsec_nanos()` and applies +0..25%. It is used by the push lanes and the lifecycle reader. So the engine has two jitter shapes, and one of them uses the clock the other was written to avoid. More on backoff in B4.

### D10 (P3, divergence). Two resume paths with different effects

- `SyncEngine::resume_account` writes `Run` with a raw `send_replace` and publishes `AccountControl::Resume`.
- `Control::resume` (`SyncControl`) uses `set_unless_stopped` and publishes nothing.

A consumer following `account_control_stream` sees the engine's `Pause(reason)` but never sees a `Resume` if the resume came through `Control`. Engine pause reason and state are only broadcast, never retained, so a late subscriber cannot learn that the account is engine-paused, or why. Compare `push_health`, which is a watch.

### D11 (doc vs code). `WriterRequest` is published

`reference/sync.md` ("Published surface the bounded lane added") says `WriterRequest` is "crate-internal - that enum is not re-exported". In fact `multiplexer/mod.rs` has `pub use changes::{AckRequest, ChangesEvent, OperatorDecision, WriterRequest, drive_changes_stream}` under a `pub mod`, with public variants, which makes it `bifrost_sync::multiplexer::WriterRequest`.

Under the standing "never delete published API" rule, the writer's internal protocol is now frozen surface by accident. The same applies more broadly. Most of the engine internals are `pub` with public fields: `Multiplexer`, `InventoryFusion`, `ScopeTokens` and `ScopeToken::tombstone`, `drive_changes_stream`, `AckRequest`, `PublicationId(pub u64, pub Arc<..>)`. Narrowing them is removal, so it is the owner's call. It is recorded here because every one of these widens what later refactors must preserve.

### D12. Vestigial surface

These are flagged, not proposed for deletion. They are published, so removing them is the owner's call.

- `AckRequest::auto` is always `false`. Nothing produces `true`, and the doc says the field "remains for trace readability".
- `ThrottleBucket::record_for` ignores its account argument.
- `ThrottleBucket::forget_account` is a no-op that detach still calls.
- `BackfillWiring::changes_tx: Option<broadcast::Sender>` is only ever used as `is_some()`, i.e. as a bool meaning "broadcast at all". The actual publication goes through `delivery`.

### D13. The passthrough rationale is false as written

`reference/sync.md` ("Hydration passthrough") justifies forwarding explicitly instead of exposing `Arc<dyn Account>` as "Read surface only. Mutations stay funnelled through `bulk_set_flags`".

`engine/passthrough.rs` forwards about 60 methods, including `send_message`, `container_delete`, `contact_delete`, `filter_create`, `set_starred`, `move_thread` and `delete_thread`. None of them:

- honours pause;
- consults or records throttle holds (a direct `send_message` 429 records no hold that the polls would honour);
- takes admission.

The real boundary is "whatever someone wrote a forwarder for", which is the decision covered in C3.

---

## Part A - Axes that should be types

### A1. The acknowledgement is assembled from three loose parts the consumer must keep in step

The facts live in four places:

- `ack_checkpoint(account_id, scope, checkpoint, publication: Option<PublicationId>)`;
- the event itself, `MultiplexerEvent { scope, event, checkpoint, publication }`;
- the receipt, `PublicationId(u64, Arc<PublicationReceipt { checkpoint, claim, walk_watermark }>)`.

So scope appears three times and checkpoint twice, and the engine re-derives agreement in several places:

- `claim_checkpoint` rejects a lane mismatch;
- `persist_ack_request` pattern-matches `id.1.checkpoint` against `acked.scope`;
- `completion_refusal` treats `publication: None` as `Unvouchable`;
- the `Ack` arm trusts `req.scope` (D2).

Because `PublicationId`'s fields are `pub`, a consumer can mint one with any receipt. `minted_here` is the run-time patch for that.

Proposal: the event carries a non-forgeable `AckToken` (private fields, minted only by `PendingCoverage`), and the public entry becomes `ack(account, token)`. Scope, checkpoint and lane all come from the token. The repair lane's `ack_publication` folds into the same entry, since a repair token is the same thing with no checkpoint. This is the `Target` permit move from `869ea04f`, applied to acknowledgements:

- a token cannot name the wrong lane;
- a token cannot be minted by the consumer;
- `publication: None` becomes unrepresentable for anything acknowledgeable.

This reshapes a published signature and type, which is allowed. It does not remove the capability.

### A2. `MultiplexerEvent.scope: CursorScope` doubles as "account-wide / unscoped"

Engine-synthesised account-wide events use `CursorScope::Account` as a sentinel:

- `reattach.rs::broadcast_warning` (the fallback is documented as sync-N4);
- `broadcast_terminated` at `RestartAccount` exhaustion;
- the lifecycle reader's `Terminate` arm;
- `push/mod.rs::broadcast`;
- `passthrough.rs::announce_page_loss`;
- both campaign warning sites in `bulk.rs`.

But `CursorScope::Account` is also Gmail's real cursor scope. A Gmail consumer cannot tell "the Gmail account cursor terminated" from "the engine gave up restarting the whole account and paused it".

Proposal: `EventScope::{Cursor(CursorScope), Account}`. It is a shape change on a published struct, and the right one: every producer then has to say which it means.

### A3. Mutation outcome identity is a re-wrapped `String`

`bulk.rs` and `mutation/readback.rs` recover the target with `ObjectId(item.0)`, where `item: BatchItemId(pub String)`. Every protocol crate mints `BatchItemId(id.0.clone())` by hand, with about 50 sites across imap, jmap, graph, google, types and the tests. The engine's per-object accounting (`outcomes: HashMap<ObjectId, MutationBucket>`) is correct only if every implementor mints the item id byte-identically to the submitted `ObjectId`. Nothing enforces that, and qualification layers such as JMAP foreign ids are exactly where a mismatch would appear.

Proposal, cross-crate:

- object mutations yield `ItemOutcome` keyed by the submitted `ObjectId`, or by a `BatchItemId` constructible only from `&ObjectId`;
- the campaign checks each outcome against the submitted set (closing D4).

This is the IMAP `Target` permit lifted into `bifrost-types` for every mail mutation door.

### A4. About ten distinct `u64` counters travel bare, often side by side

The counters:

- scope fence (`coverage.scope_fence`), with `0` meaning "attach time" (`DeferredInventory { fence: 0 }`);
- lifecycle epoch (`cursors.lifecycle_epoch`; `EstablishIfAbsent { epoch }`; `ParkScope { epoch }`; the owed maps `HashMap<CursorScope, u64>`);
- account generation (`Incarnation::generation`; `ReopenRequest::Recovery { generation: Option<u64> }`; `PushRecovery { generation: u64 }`);
- registry generation, already typed inside `DriveGeneration`;
- scope-token generation;
- reattach attempt id (`run_establish(.., reattach_attempt: Option<u64>, .., walk_fence: u64)`);
- completed-reset count (`SchemaReset { parked_at }`);
- walk generation (`coverage.next_generation()`);
- walk watermark;
- publication number.

`run_establish` takes an attempt and a fence as adjacent `u64`s. `establish_if_absent(ctx, scope, epoch)` would accept a fence without complaint. `plan_requests` mints `RepairAttemptId(coverage.next_generation())`, so repair attempt ids share a counter with walk generations by coincidence.

`DriveGeneration { registry, scope }` already shows the right move. Apply it to every counter: `ScopeFence`, `LifecycleEpoch`, `AccountGeneration`, `ReattachAttempt`, `ResetCount`, `WalkGeneration`, each minted only by its owner.

### A5. Three hand-rolled bool sets that encode one state machine each

- **`RegisteredSubscription { teardown_unconfirmed, desired, torn_down }`.** `reference/sync.md` states the invariant "always desired, teardown_unconfirmed, or both" in prose. Records are built literally in `reattach.rs` and converted with `into_orphan`, `mark_unconfirmed` and `mark_torn_down`. The states are really these: Wanted, Wanted+TeardownUnconfirmed, Wanted+TornDown, Orphan(unconfirmed). Eight bool combinations are representable, and two of them are documented as impossible.
- **`MutationCohort { submitted, readback, done }`** plus `retry_at` and `reconcile_at`. The phase is Unsubmitted, Submitted+retry pending, Readback, or Done. `done && readback` and `!submitted && readback` are representable. `run_bulk_pipeline` flips the flags in roughly twenty places.
- **`RepairPacing { exhausted, observed, .. }`.** `exhausted` is a side channel so callers can recognise `Error::EstablishCursorFailed("recovery wait budget exhausted")`. Every caller matches `Err(_) if pacing.exhausted`. The result type should carry `WaitBudgetExhausted`.

### A6. Failures reach callers only as prose

- `Error::Other(String)` is constructed about 98 times in `crates/sync/src`. Branchable cases hidden inside it:
  - scheduler admission refusal: "lane is full", "admission was abandoned", "account budget was forgotten" (detached), "poisoned". Every caller re-sleeps on a guess: the poll loop sleeps a cadence, deferred inventory 1s, campaigns 100ms, and none of them can tell that the account is gone;
  - writer channel closed (D7);
  - "one mutation target has conflicting caller versions";
  - wrong-scope `Done` in `fuse_inventory_done`.
- `Error::EstablishCursorFailed(String)` carries "unknown CursorEstablishment variant" and "recovery wait budget exhausted", which are two unrelated outcomes.
- `Error::CheckpointStore(String)` carries "acknowledgement names an unknown publication", which is a caller-side rejection, not a store failure. `AckFailure::{Rejected, Store}` is precisely the typed distinction, and it collapses at the public boundary.
- `ack_checkpoint` returns `Ok(())` for both durable and withheld outcomes (`AckPersistOutcome::Withheld` for `OpenDebt`, `WalkNotWhole`, `Unvouchable`, and prior-attachment backfill rows). A consumer cannot tell "durable" from "honoured but nothing written". Return `AckOutcome::{Durable, Withheld(CompletionRefusal)}`.
- Engine-synthesised warnings are told apart only by text. All of these use `WarningKind::OperatorAttentionNeeded` or `Other`:
  - "recovery wait budget exhausted";
  - "scope re-establishment retry budget exhausted";
  - "schema recovery could not clear durable checkpoints";
  - "shared folder access revoked; scope disabled";
  - "scope capability downgraded";
  - "push transport disconnected";
  - "push stream terminated with retryable recovery", which also drops the `AccountError` it was about;
  - "reconcile requested dedupe-by-client-id".

  `PauseReason` is already a bounded enum, and the warnings deserve the same treatment: an engine-notice kind per case, carrying the scope and the originating error.

### A7. `ReopenRequest::PushRecovery` duplicates `Recovery`

The listener handles `PushRecovery { generation }` as `recover(scope, error, Some(generation))` and `Recovery { generation }` as `recover(scope, error, generation)`. One variant with a required stamp-or-unstamped type, `Stamp::{Incarnation(AccountGeneration), Injected}`, says the policy in the type: only the sink is unstamped.

---

## Part B - Decisions made in more than one place

### B1. "Given a stream-terminating `AccountError`, what does this lane do?" (about nine sites)

Every long-lived lane matches `plan_recovery` itself and decides four things: what to record in the bucket, how long to sleep (and the fallback), whether and how to forward (stamped or not), and whether to broadcast `Terminated`.

The sites:

- `multiplexer/mod.rs::handle_drive_outcome`;
- `push/reconciler.rs`;
- `push/lanes.rs`;
- `push/mod.rs::route_watch_event`;
- the lifecycle reader (`lifecycle_termination` plus `lifecycle_retry_wait`);
- `engine/reattach.rs::handle_account_error`;
- `re_establish_scope_with_backoff`;
- `bulk.rs` (item and stream);
- deferred inventory (through `ctx.recover`).

They already disagree:

- **Retry fallback when no hint is given:** 1s in the poll loop and campaigns, `Duration::ZERO` in push lanes, the 30s floor in lifecycle.
- **Terminal:** the poll loop parks and logs, the sink broadcasts `Terminated`, `handle_account_error` only logs ("broadcast already carried the terminating event", which is true only for some producers).
- **Throttle recording:** the push lanes record holds. A retryable sink termination records a hold and then reports only a prose warning.

Proposal: one `StreamFailure` dispatcher in `recovery`. It takes the error, the scope, the incarnation stamp and the lane kind, performs the shared side effects once (record the holds, telemetry), and returns a typed action: `Retry { after }`, `Forward(ReopenRequest)`, `Terminal`. Lanes keep only their genuinely lane-specific policy, such as the lifecycle reader's await-reopen rule.

### B2. "Is this error a throttle, and for how long?" (four-plus recorders)

The recorders:

- `recovery::record_throttle_parts`: budgets win, mailbox gets a 1s fallback, otherwise a hint is required;
- `recovery::record_repair_throttle`: also fires on `RetryReason::RateLimited` or `ReconcileReason::ThrottledMidFlight` with no scope or hint, and returns a local deadline with a 1s fallback;
- `bulk.rs::record_error_throttle`: walks `error.throttle_observations()` per leg, while `classify_item_outcome` separately records the primary leg ("max deadlines make that duplicate harmless");
- the push lanes and the lifecycle reader, through `record_throttle`.

So whether a partial commit's sibling-leg throttle is honoured depends on which lane saw the error. Campaigns record every leg; the poll loop, reconciler, repair and push lanes record only the primary.

The owner belongs in `bifrost-types`: `AccountError::throttle_holds() -> Vec<(HoldTarget, RetryHint)>` covering every leg, budgets and scope lifting, so the engine's job reduces to `ThrottleBucket::observe(account, &error) -> Option<Instant>`. Also make `ThrottleBucket` own its lock, instead of free functions taking `&std::sync::Mutex<ThrottleBucket>` (state split from behaviour).

**Cross-crate question to settle.** `bifrost-net` has its own hold store: `SharedHold` and `hold_gate`, string-keyed with `HoldKind::{RateLimited, QuotaExhausted}`, used by Google. The engine has `ThrottleBucket`, keyed by `ThrottleKey`, used through stamped budgets by Graph and IMAP. Both answer "is this provider budget held right now", with different keys, clocks and lifetimes, and an error from a net local hold is recorded into the engine bucket a second time. Decide whether the engine bucket should be fed from net's registry, or the reverse, so that one store owns provider holds.

### B3. "May the engine start wire work for this account now?" (the drive-start gate)

The full protocol:

1. boundary is `Run`;
2. activity is registered;
3. account-wide holds are slept out first;
4. the current account's declared keys are checked;
5. admission is taken;
6. the keys are re-checked against the freshly loaded incarnation after admission;
7. the fence is re-checked;
8. on a hold, the permit is released.

It is written out by hand in:

- the poll loop, and again inside the `with_drive` closure;
- the push reconciler;
- the backfill orchestrator;
- the partition runner through `LaneGate`;
- the deferred inventory worker;
- the campaign scheduler;
- `RepairPacing::wait` together with `restart_account`.

Each copy carries its own admission-refusal backoff: cadence, 1s, 100ms, or a `record_attempt` ramp. `orphan_teardown.rs`, `repair_debt` and the passthrough cluster skip some or all of the gate (D6, D13). The doc's "drive-start granularity" guarantee is held together by about seven parallel copies of one sequence.

Proposal: one `DriveStart` primitive on the slot:

```
admit_drive(work: ThrottleWork, kind: WorkKind) -> Result<Admitted { permit, activity, incarnation }, Held { scope_local: bool, until }>
```

It owns steps 1 to 8 and re-checks after every wait internally. Lanes keep only their reaction to `Held { scope_local: true }` (requeue, defer or park), which is the genuinely different part. Orphan teardown and repair would go through it too.

### B4. "How long before attempt N?" (backoff policy, about six implementations)

- `reattach.rs`: `REOPEN_BACKOFF_INITIAL/CAP` with `jittered`, hand-looped three times (`restart_account`, `handle_schema_incompatible`, `re_establish_scope_with_backoff`), each with `delay = delay.saturating_mul(2).min(CAP)`.
- `backoff.rs::ReconnectBackoff` with +0..25% jitter (push lanes; lifecycle with jitter off, 30s to 480s).
- `engine/backfill.rs`: `BACKFILL_RETRY_INITIAL/CAP` (5s to 5min) inside `BackfillScan`.
- `RepairPacing`: a cumulative wait budget measured against `REOPEN_BACKOFF_CAP`, the same constant reused as a different concept.

`bifrost_types::BackoffPolicy` already exists and `ReconnectBackoff` wraps it. Every engine retry loop should be a `ReconnectBackoff` built from a named `BackoffPolicy` const, with one jitter rule (D9), and the "attempt budget" and "cumulative wait budget" should be separate named types rather than a reused cap.

### B5. "Which cursor scopes does a membership map to?" (inverse relation, two sites, plus a third bridging identities)

- `engine/attach.rs::scope_covers_membership` is the predicate from a `CursorScope` to the memberships it covers. It feeds the membership index for push-hint routing.
- `multiplexer/mod.rs::membership_scopes` and `folder_cursor_shapes` generate cursor scopes from a membership, for lifecycle create and delete.
- `recovery.rs::resolve_throttle_key` turns `ErrorScope::Cursor(Folder(id))` into `ThrottleKey::Mailbox(MailboxId(folder.0))`.

All three treat `FolderId` and `MailboxId` as the same string via `.0` ("Graph maps folders 1:1 to mailboxes; we treat the FolderId / MailboxId String the same"). They agree today. Nothing ties them: adding `MembershipScope::Label` handling, or a new `CursorScope` shape, means editing two inverse functions in two modules and hoping. The `Type(_)` arm also says "covers every membership", including memberships of an unrelated object type.

The owner belongs in `bifrost-types` next to both enums: `CursorScope::covers(&MembershipScope)` plus a shape-driven `scopes_for(membership)` that is tested as its inverse. The Folder versus Mailbox identity question should become an explicit conversion, not a `.0` re-wrap.

### B6. "Is this stored cursor an inventory position?" and "Is this stored row readable?" (two establishment paths)

`attach.rs::establish_one` and `reattach.rs::run_establish` (whose doc says it "Mirrors `SyncEngine::establish_one`") each:

- read the stored cursor;
- decide readable versus unreadable with the same `Ok(Some(c)) if validate_envelope().is_ok() / Ok(Some(_)) | Err(SchemaIncompatible)` pattern, then react differently by design;
- ask `is_inventory_cursor(&c) || inventory_resume_stream(c).is_some()`;
- handle `Ready` versus `EstablishViaInventory`;
- fail an unknown variant with `EstablishCursorFailed("unknown CursorEstablishment variant")`.

They already diverge on fresh-envelope classification (D3). The attach path also writes the store directly (`persist_cursor` as `get_ledger` then `apply_transition`, and `clear_account_checkpoints`), while recovery goes through the writer. That is two implementations of "persist an established cursor while preserving the ledger" and of "account schema clear". See C2 for the merge.

### B7. "Which Priority maps to which lane, and when does starvation fire?" (scheduler, two copies)

`scheduler/mod.rs::lane_index`, `AdmissionState::visit_order` and `record_grant` re-implement the work-item path's `Scheduler::lane` and `try_pull` starvation rule ("exactly as `Scheduler::try_pull` does", "matching `Scheduler::lane`"). Two queue types back them: `LaneQueue` with a shed policy, and `VecDeque<AdmissionRequest>` with a refuse policy. They agree by inspection today.

One `PriorityLanes<T>` with a pluggable overflow policy would own the floor rule once. The work-item path is published and engine-unused, so it must stay, but it can be a thin user of the same type.

### B8. "Who publishes, and through which door?" (raw sender versus delivery gate)

`changes_tx: broadcast::Sender<MultiplexerEvent>` is threaded alongside `delivery: Arc<ChangeDelivery>`, which wraps the same sender, into `AccountSlot`, `MultiplexerHandle`, `SlotContext`, `RecoveryContext` and `BackfillWiring`. Each call site picks a door:

- `publish_backfill`;
- `publish_acknowledgeable`;
- `delivery.sender().send`;
- `changes_tx.send` (reattach, bulk, push).

The "numbered receiver took delivery" invariant is enforced only for callers that pick the right one, and hand-built `MultiplexerEvent { scope: CursorScope::Account, checkpoint: None, publication: None, .. }` literals appear in at least six places.

Proposal: the raw sender is private to `ChangeDelivery`, which gains `publish_warning(EventScope, Warning)` and `publish_terminated(EventScope, AccountError)`. No context carries `changes_tx`.

### B9. "What does a push teardown refusal mean?" (five sites)

The sites:

- `SyncEngine::unsubscribe_push`;
- `subscribe_push`'s refused-registration cleanup;
- `reattach.rs::unwind_replacement_subscriptions`;
- `retire_stranded`;
- `orphan_teardown.rs`.

Each calls `push_unsubscribe`, decides whether a refusal becomes an orphan, and logs. Only the unwind path records throttle holds (through `RepairPacing`). Only the retrier applies a timeout. A `SubscriptionTeardown` service on the registry, with "delete, settle or orphan, record holds" as one operation, would own it.

### B10. "Apply the control snapshot to an incarnation" (three sites)

Priority and bandwidth cap reach the account in three places:

- the control-applier closure in `attach.rs` (initial apply, then on change, writing both `budget.set_cap` and `account.set_bandwidth_cap`);
- `open_replacement_admitted` (`apply_bandwidth_budget` before the open);
- `reattach_account_paced` (`set_priority` plus `apply_bandwidth_cap` after the open).

The serialisation trick (holding the watch borrow through the setter) is repeated rather than owned. A `ControlApplier::apply_to(&Incarnation)` used by both attach and swap would own it.

### B11. "Ledger mutation: apply then persist, or persist then commit?" (writer arms)

Inside `ack_writer`, the in-memory ledger is mutated in two ways:

- **Mutated in place, kept even if `put_ledger` fails:** `RecordDebt`, `RecordBarrier`, `ResetScope`, `ResetAccountForSchemaRecovery`, the discard passes.
- **Cloned, persisted, and replaced only on success:** `CrossWaivedBarriers`, `OperatorDecision`, `ApplyRepair`.

The split is defensible (debt is monotone and must not be lost, policy must not take effect unpersisted), but each arm decides it independently, and the ack path's `persist_ack_request` mutates in place before an `apply_transition` that can fail. The rule should be a type:

- `ledger.accrue(..)` for monotone debt, which is sticky by design;
- `ledger.stage(..) -> Staged` for policy and proof, committed by the successful write.

The writer then cannot get an arm wrong, and D5's read-side divergence becomes explainable.

---

## Part C - Structure

### C1. The real seam is the per-account actor, and the code splits it three ways

`AccountSlot` (`types.rs`, about 25 fields), `SlotContext` (`engine/context.rs`, about 28 fields) and `RecoveryContext` (`engine/reattach.rs`, about 24 borrowed fields) are three bundles of the same per-account handles:

- `SlotContext::recovery` rebuilds a `RecoveryContext` literal;
- `SyncEngine::reattach` hand-builds a second one from `AccountSlot` fields;
- `AccountSlot` and `SlotContext` are populated field by field side by side at the end of `attach_opened`.

On top of that, `SyncEngine` keeps a parallel `ack_senders: DashMap<AccountId, Sender>` beside `accounts: DashMap<AccountId, Arc<AccountSlot>>`. Many methods look up both, and detach removes them separately. Several more engine-wide registries are keyed by `AccountId` and cleaned at the bottom of detach: sink, budget gate, backfill registry, throttles, meter, subscriptions. Each is its own registration with its own forget call.

Proposal, a big but clean rewrite:

- **One `Slot` type** (`Arc`) that owns every per-account handle, the writer sender, the boundary (as `Boundary`, never the raw sender, which fixes D1 structurally) and the workers.
- **Recovery, reattach, establishment, campaigns and passthrough become methods on `Slot`.**
- **`SyncEngine` becomes a registry:** `engine.slot(id)?` followed by the operation.
- **An RAII `SlotRegistrations` guard** (sink, budget, meter, backfill registry), released by drop at the end of detach, replaces the hand-ordered tail.

This deletes the three-bundle problem and the hand-built `RecoveryContext`, and makes "is this account attached" one lookup instead of two.

### C2. Establishment is one operation written twice, and attach bypasses the writer

Spawn the ack writer first in attach (it has no dependencies on the other workers). Then attach's establishment can run through the same `Establishment` routine as recovery and reattach (`run_establish`), with an origin mode (`Attach`, `Recover`, `Reattach { attempt }`) deciding the differences that are real:

- persist-ready or stage;
- claim provisional rows;
- defer to fusion or run fusion inline.

This collapses B6, fixes D3 at the root, and makes "one writer per account owns every durable mutation" literally true. Today the attach-time account clear and cursor persist are the exceptions.

### C3. `engine/reattach.rs` (2667 lines) does several unrelated jobs

It contains, in one file:

- the recovery dispatcher;
- the directive arms;
- `DeferredScopeRepairs` and `DeferredSchemaReset` (parking actors);
- schema reset;
- `RepairPacing`;
- scope restart and disable;
- the reattach transaction;
- `ReattachGuard` and its drop-time cleanup;
- replacement open;
- establishment;
- warning and termination broadcast helpers;
- jitter.

Separately, the reopen listener (the account's serial supervisor, whose loop is the real entry to all of the above) is an inline closure inside `attach_opened` in `attach.rs`.

Suggested layout under `engine/`:

| File | Contents |
|---|---|
| `supervisor.rs` | listener loop and `ReopenRequest`, moved out of `multiplexer/` |
| `recovery/dispatch.rs` | the B1 dispatcher and the directive arms |
| `recovery/parked.rs` | the two deferred-obligation actors |
| `recovery/pacing.rs` | `RepairPacing` plus the B4 backoff |
| `reattach/{transaction,guard}.rs` | the reattach transaction and its guard |
| `establish.rs` | the C2 establishment routine |

`ReopenRequest` belongs to the supervisor, not to `multiplexer`. Most of its variants (`EstablishIfAbsent`, `SchemaReset`) are documented as "engine plumbing" and are raised by non-multiplexer code.

### C4. `multiplexer/mod.rs` (3776 lines) holds four subsystems

1. **`ChangeDelivery` and `ChangesReceiver`:** the consumer-facing delivery subsystem, with receiver numbering, departure sweeps and lag handling. This is the engine's output port, and the backfill lane, repair and push all depend on it. It should be its own module (`delivery/`), with B8 applied.
2. **The lifecycle reader:** a roughly 170-line closure inline in `Multiplexer::run`. It should be its own worker type with its own file and tests.
3. **The poll loop, scope tokens and drive outcome.** This is what "multiplexer" should mean.
4. **Membership to scope mapping** (B5), which belongs in `bifrost-types` or `cursor/`.

Also, `multiplexer/changes.rs` defines `AckRequest`, `WriterRequest` and `OperatorDecision`, which are the writer's protocol, consumed only by `engine/ack.rs` and `backfill/checkpoint.rs`. They should move next to the writer (`engine/writer/`). The current placement is also how `WriterRequest` leaked into the public surface (D11).

### C5. `engine/ack.rs`: the writer is a 600-line match over five concerns

The concerns:

- ack persistence and completion-sentinel judgement;
- reattach provisional ownership;
- repair parking and acknowledgement pairing;
- scope and account resets;
- operator decisions and barriers.

Alongside it, `reject_unloaded_request` is a second exhaustive match deciding per variant what "ledger unavailable" means, away from the variant.

Proposal: a `Writer` struct with a method per concern (`ProvisionalRows`, `RepairPairing`, ledger ops per B11), a `WriterRequest` impl giving each variant its `requires_ledger()` answer, and the detach-phase helpers (`take_ack_writer`, `await_*_until`) moved out to the detach/teardown code they serve.

### C6. `engine/bulk.rs`: `run_bulk_pipeline` is one 850-line function

The function carries about fifteen loose locals:

- `outcomes`, `effects`, `retry_slots`, `retry_owners`, `identities`, `cohorts`, `forwarded_directives`;
- `first_error`, `caller_conflict`, `blocked_by_engine`, `dedupe_count`, `cursor`;
- and more.

These are the campaign's state, but the state has no type and no methods, so every phase rule (abort, engine block, reassert finalisation) is a loop over cohorts inside the main loop.

Proposal:

- a `Campaign` struct owning that state, with `select_action()`, `submit()`, `read_back()`, `finalize()` methods;
- `MutationCohort` phases as an enum (A5);
- the B3 drive-start primitive for admission and holds.

Also `DirectiveKey` (here) and `directive_target_scope` (`recovery.rs`) are two engine-side shadows of `EngineDirective`'s shape, kept in step by hand. Both are protocol-neutral facts about a `bifrost-types` enum and belong there (`EngineDirective::target_scope()`, `EngineDirective::identity()`).

### C7. `recovery.rs` mixes three concerns

1. Plan dispatch (`plan_recovery`, delays).
2. The throttle bucket and its free-function API.
3. A catalogue of engine-authored `AccountError` constructors:
   - `restart_scope_error`;
   - `establish_failure_error`;
   - `inventory_resume_contract_failure`;
   - `inventory_removal_scope_contract_failure`;
   - `recovery_exhaustion_error`;
   - `cursor_decode_failure`;
   - `batch_boundary_violation`;
   - `backfill_checkpoint_on_changes`;
   - `fresh_cursor_contract_error`.

   `control.rs::control_wait_ended` is a stray member of the same catalogue.

Split them into `recovery/plan.rs`, `throttle.rs` (with B2) and `engine_errors.rs`. With all the engine-minted errors in one file, D3 and D8 are visible side by side.

### C8. Test layout mirrors history, not the design

- **`engine/lane.rs` is about 110 lines of gate and about 1100 lines of tests,** most of which exercise `PendingCoverage` capacity, supersession and watermark behaviour. The module doc explains why: capacity moved into `cursor/coverage.rs` and the tests did not follow. They belong with `PendingCoverage`.
- **`tests/attach_schema_recovery.rs` (3146 lines) is a grab-bag:** page-loss warnings, detach push teardown, reattach topology, lifecycle reader parking, account restart exhaustion, partial-batch boundary, publication-id invariant, deferred schema reset. It also redefines `unsupported`, `caps` and `cursor_for` already in `tests/common/mod.rs`, and has its own `HealAccount` double.
- **There are nine hand-written `impl Account` test doubles in `crates/sync`:** `common::StubAccount`, `HealAccount`, and in `readback_guard.rs` alone `FlagsAccount`, `JudgingAccount` and `ReassertAccount`, plus the passthrough ones. Their default-method behaviour is copied rather than shared.
- **`tests/common/mod.rs`'s `StubAccount` has about 50 hook fields.** It is included into the crate as `crate::test_common` via `#[path]` and is also the integration-test double.

A test-support layout that follows the design would have one configurable double per lane (changes, inventory, push, mutation, lifecycle) composed into a stub, and test files named after the contract they pin (`supervisor_*`, `delivery_*`, `campaign_*`, `establish_*`), not after the first bug they were written for.

### C9. Ownership oddities

- `types.rs` mixes published configuration (`EngineConfig` and friends, `Incarnation`) with the engine-private `AccountSlot`, `WorkerTask` and `WorkerRole`. The slot belongs in `engine/` (C1).
- `BackfillAdmission` holds a `BudgetPermit` in `Mutex<Option<_>>` and exposes `admit()` and `release_admission()` as separate calls. That turns an RAII permit into a flag the caller must remember to clear. The permit should travel by value through the wait (`wait_for_capacity(permit) -> Result<permit, WaitFailed>`).
- Engine pause state (D10) lives only on a broadcast. Fold it into the C1 `Slot` as a watch: `EngineState::{Running, Paused(PauseReason), Stopping}`. Then `resume_account` and `Control::resume` become one transition with one notification.

---

## Ranking, if only a few things get done

1. **D1 and D2.** Small, real, and each is a symptom of a larger shape: C1 and C9 (boundary ownership) and A1 (ack token).
2. **A1 (ack token), A2 (`EventScope`), A3 (outcome identity keyed by submitted object, closing D4).** Published-shape changes that make wrong values unconstructible.
3. **B1 (one stream-failure dispatcher), B3 (one drive-start gate), B2 (throttle observation owned by types).** These are the decisions with the most copies, and B1 and B2 already disagree.
4. **C1 plus C2 (one `Slot`, one establishment path, writer spawned first).** The big rewrite that removes most of the bundle and parameter churn, and makes "one writer owns every durable mutation" true at attach.
5. **The C3, C4, C5, C6 file splits.** Mechanical once 3 and 4 have landed.
