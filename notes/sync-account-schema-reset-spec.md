# Spec: an account-wide schema reset that clears every durable row and revokes every pre-reset producer

Technical implementation specification for the `bifrost-sync` finding
"`handle_schema_incompatible` still resets only registered scopes" (SYNC-015 in
`notes/bugs-sync.md`). Transient: it lives in `notes/` and dies when the work
lands.

## Standing references (read them, do not just note them)

Implementers and reviewers must READ these before touching code. They are the
ground this is built on and judged against.

- `reference/technical-implementation-spec.md` - the contract this document is
  written against.
- `reference/error-model.md` - the cross-cutting `AccountError` contract. This
  work moves no classification: `SyncState(SchemaIncompatible)` still derives
  `Engine(SchemaIncompatible)` ("Central derivation", the SyncState row), and
  `restart_scope_error` still derives `Engine(RestartScope(scope))`. It adds one
  engine-side `Error` variant (`Superseded`), which is not an `AccountError` and
  never crosses into recovery dispatch.
- `reference/sync.md` - in particular: "One writer per account owns every
  durable mutation" through the paragraph ending "and its acknowledgement
  fences" (the reset discipline, the two-pass invalidation, the acknowledgement
  fence, the schema-recovery gap paragraph this spec closes); the pause
  paragraphs around "Schema recovery follows the same discipline for the whole
  account" and the `DeferredScopeRepairs` paragraph after it; "Cursor envelope"
  (the attach heal paragraph beginning "An unreadable row is classified where
  it is found"); "Checkpoint store"; and the recovery summary paragraph
  beginning "Cursor envelope schema mismatches at `get_change_cursor`".
- `reference/jmap.md` (the `OpaqueChangeState` paragraph, v2 object-id change)
  and `reference/graph.md` (the cursor envelope paragraph, v2 object-id change)
  - why schema recovery deletes completion markers at all: the inventory
  re-walk the marker would suppress IS the id migration.
- `notes/bugs-sync.md`, entry SYNC-015 - the source item. The owner ruled on
  2026-10-21 that it needs a specification because it changes the
  consumer-implemented checkpoint store contract.

## The source item, and how this spec reads it

The entry says durable completion markers of scopes that are not in the
registry survive the reset, and that a complete fix needs writer or store
support for an account-wide reset or durable enumeration.

The defect is real. `handle_schema_incompatible`
(`crates/sync/src/engine/reattach.rs`) iterates `ctx.cursors.all_scopes()` and
resets each through `WriterHandle::reset_scope_for_schema_recovery`. Rows the
registry does not name survive, and these are ordinary, not exotic:

- a scope whose re-establishment budget was exhausted: `restart_scope_admitted`
  reset it with `reset_scope_for_restart`, which keeps backfill rows, then
  terminated it, so its completion marker survives and it is not registered;
- a `DisableScope`-quarantined scope (`reset_scope_for_disable` keeps backfill);
- a scope absent from discovery this session (a hidden or temporarily
  inaccessible folder) whose rows remain from earlier sessions;
- a scope whose attach-time deferred inventory is queued or running
  (`run_deferred_inventory_establishment`), which is not registered until
  `InventoryFusion::finalize`.

When such a scope is next established the backfill orchestrator reads the
surviving marker (`backfill_complete_recorded`, `BackfillPlan::resume`) and
skips the inventory walk, and for JMAP or Graph that walk is the object-id
migration: the consumer keeps ids minted under a disowned encoding for that
scope permanently. A surviving stale change cursor of such a scope also
re-triggers a second account-wide clear when the scope is later registered.

Reading the item as "clear more scopes" is not enough, and the spar established
why (see "Review record"). Clearing rows only helps if no producer that
predates the reset can recreate them afterwards. Today the engine's per-scope
resets rely on a numeric acknowledgement fence plus point-in-time producer
checks; a whole-account reset widens every gap in that discipline from one
scope to all of them. This spec therefore delivers two properties together:

1. **Coverage.** When the account reset commits, the store holds no change
   cursor and no backfill row for the account, for any scope, completion
   markers included. The ledger survives, with the debt of every retired
   publication folded in.
2. **Revocation.** After the reset commits, no ENGINE producer that existed
   before it can land an accepted change cursor, backfill row, or completeness
   proof, and no debt it observed is dropped. Consumer assertion paths that
   carry no publication are outside this guarantee and named below.

Durable enumeration inside the engine (for example a scope index carried in the
engine-owned `DebtLedger`) was considered and rejected: the rows a schema bump
must clear were by definition written by an OLDER engine revision, whose ledger
would carry no such index, so it fails exactly the case it exists for. The
store can delete everything for an account in one transaction. The new store
method is therefore the account-wide reset the item asks for.

One more reader of the same identity is in scope: attach. `establish_one`
heals an unreadable row by deleting only that scope's change cursor. A release
that moves the engine envelope out of the migration window makes EVERY row
unreadable at the outer level; attach then heals each one per scope, no
protocol door ever fails afterwards, and nothing ever triggers the account
reset, so the markers survive. An unreadable row has unknowable provenance and
its scope's ids cannot be proved current; attach therefore runs the account
clear too.

Excluded, and filed (see "Stopping rule"): the consumer assertion paths
`BackfillCheckpointWriter::persist` and `SyncEngine::ack_checkpoint` with
`publication: None` (for a change cursor or a backfill page; a completion
marker with no publication is already refused by `completion_refusal`),
ledger-only writer requests from pre-reset producers, and in-session paths
that discard an unvalidated row before classifying it. The `None` acknowledgement
matters to this item: `persist_ack_request` skips the claim lookup and every
fence for it, so a backfill PAGE a consumer acknowledges without its id after
the reset lands, and for an `OpenPages` plan that row is a resume position past
windows the post-reset walk then never re-mints. Nothing in the engine sends
one (the only production `WriterRequest::Ack` is `ack_checkpoint` itself), so
it is the same class as `persist`: a consumer that dropped the id.

## Survey of the ground

### Store (`crates/sync/src/cursor/store.rs`)

`CheckpointStore` is the published consumer-implemented trait. Required
methods: `apply_transition`, `get_change_cursor`, `get_backfill`, `put_ledger`,
`get_ledger`, `delete_change_cursor`, `delete_backfill`; provided
(do-not-override) `put_change_cursor`, `put_backfill`. No enumeration, no
account-wide operation. `delete_backfill`'s doc claims schema recovery is its
only caller, which is already false: `ResetScope { delete_backfill: true }` for
scope deletion and `discard_backfill` also call it. `InMemoryCheckpointStore`
keys rows by `(AccountId, CursorScope)` and `(AccountId, CursorScope,
Partition)` under one mutex.

Implementations in the workspace (all in `bifrost-sync`; every one gains the
new method): `InMemoryCheckpointStore`; `FlakyLedgerStore`
(`src/engine/ack.rs` tests); `FailingTransitionStore`, `FailingDeleteStore`,
`FailingOnceStore`, `GatedDeleteStore` (`src/engine/tests.rs`);
`StallingAckStore` (`tests/detach_close_clamp.rs`); `PoisonedStore`,
`FlakyGetStore`, `AckRacingStore` (`tests/attach_schema_recovery.rs`);
`RecordingDeleteStore`, `FailingDeleteStore`
(`tests/backfill_lane_flow_control.rs`). Delegating doubles delegate; doubles
whose `delete_backfill` returns a fixed answer return the same fixed answer.

### Writer (`crates/sync/src/engine/ack.rs`)

`ack_writer` owns the account's in-memory `DebtLedger`, the provisional map
(`HashMap<CursorScope, u64>`, reattach attempt ownership), parked repairs. The
`ResetScope` arm: `provisional.remove(&scope)`; pass 1
`coverage.invalidate_scope` (retire, take debt, fence) and ingest;
`persist_ledger_only`, optional `delete_backfill`, `delete_change_cursor`;
pass 2 `invalidate_scope`, ingest and `put_ledger` if it carried debt, logging
(not returning) a pass-2 failure. `reject_unloaded_request` answers every
request when the initial ledger read failed. `WriterHandle` names reset intent
per caller: `reset_scope_for_restart`, `reset_scope_for_disable`,
`reset_scope_for_deletion`, `reset_scope_for_schema_recovery` (the last has one
production caller, `handle_schema_incompatible`, and one test,
`a_schema_recovery_reset_deletes_backfill`).

`WriterRequest` (`crates/sync/src/multiplexer/changes.rs`) is `pub`,
`#[non_exhaustive]`.

### Coverage (`crates/sync/src/cursor/coverage.rs`)

`PendingCoverage` (alias `Publications`) mints ids as
`next.fetch_add(1, Relaxed)` in `mint`, CALLED BEFORE the ledger lock in both
`publish` and `register_in`. Ids carry a per-instance segment in the high bits
(`minted_here`). Ledger state includes `boundaries` (registrations and backfill
capacity), `claims`, `carried` (carry-forward debt), `fenced` (per-scope
exclusive bound), `fenced_backfill`, `last_backfill_mint`, `persisted`,
`folded`. `invalidate_in` reads the watermark as `next.load` under the lock;
`claim_in` checks `fenced` / `fenced_backfill` before live claims, and for a
foreign id falls through to the receipt fallback (`Apply(id.1.claim)` when the
receipt's lane matches). `scope_fence(scope)` returns the per-scope fence or 0.

Hole (pre-existing, also in the per-scope reset): an id minted before an
invalidation reads the counter but registered after it releases the lock is
below the fence and was retired by nobody; its claim's debt sits in a
registration whose acknowledgement is refused.

Hole (pre-existing): an invalidation with nothing minted since the previous
one leaves the fence unchanged, so a walk that snapshotted the old value does
not see the reset.

### Producers of scoped publications

- Live drives (`multiplexer/changes.rs`, `drive_changes_stream`): publish
  inside `CursorRegistry::publish_if_drive_generation`, under the registry lock;
  `CursorRegistry::delete` bumps the generation. This is a real ownership
  revocation.
- Backfill pages (`backfill/runner.rs`, `run_partition`): snapshot
  `scope_fence` at pass start (`fence_at_pass_start`), compare after the
  capacity wait, then `control.publish_checkpoint` - a point-in-time check, not
  atomic with the mint. The orchestrator (`engine/backfill.rs`,
  `run_backfill_partition_at_boundary`) checks its own `fence_at_walk_start`
  before calling `run_partition`, which then re-snapshots; a reset between the
  two is adopted by the runner as its own.
- Completion marker (`engine/backfill.rs`, `emit_backfill_complete`): compares
  `fence_at_walk_start` before and after the capacity wait, then
  `control.publish_walk_marker` - point-in-time.
- Inventory fusion (`multiplexer/fusion.rs`): pages through
  `forward_inventory_batch` and the terminal `Done` through
  `control.publish_checkpoint`, with no fence at all; `finalize` puts the cursor
  into the registry unconditionally. Constructed in
  `engine/attach.rs::run_deferred_inventory_establishment` (attach-time
  deferred items, which run outside the reopen lock) and twice in
  `engine/reattach.rs::run_establish` (under the reopen lock). Fusion forwards
  checkpoint-less batches (and the barrier-stop batch, sent with `None`) as
  `Created` changes with no publication at all.
- `run_establish` itself: before either fusion it awaits the writer's
  `get_change_cursor` and `pacing.wait`, and its stored-cursor path
  (`cursors.put(existing)`) and `Ready` path (`persist_established`, then
  `cursors.put(cursor)`) install unconditionally. The multiplexer deletes
  scopes without the reopen lock, so a deletion can land inside any of those
  awaits; today the queued `ScopeDeleted` request retires what was installed.
  Its callers: `re_establish_scope_with_backoff` (retries up to the budget,
  each attempt a fresh `run_establish`) and `reattach_account_paced`, which
  skips a scope only on `scope_local_establish_failure` and fails the whole
  attempt on any other error.
- Repair publications (`Lane::Repair`) carry no scope and are not fenced by
  any reset; obligations survive resets, so they stay acknowledgeable.

Deferred items (`engine/mod.rs`, `DeferredInventory::{Start, Resume}`) are
created in `attach_inner` BEFORE `pending_coverage` exists, so every fence they
could have observed is 0. A `Resume` holds a cursor read from the store at
attach.

### Recovery dispatch (`crates/sync/src/engine/reattach.rs`, `context.rs`)

`handle_account_error` returns `Option<CursorScope>`: a declined repair of a
cursorless scope that `SlotContext::recover` parks in `DeferredScopeRepairs`.
The park task re-raises a synthesized `RestartScope` after checking
`cursors.snapshot(scope).is_none()` OUTSIDE the reopen lock, so a scope
established between that check and the dispatch is restarted (pre-existing).
`handle_schema_incompatible` declines on a paused account relying on
registered failing cursors to re-raise; a `SchemaIncompatible` raised by the
deferred inventory worker has no registered cursor. That worker dispatches a
`FusionOutcome::Terminated` through `ctx.recover(Some(scope), error)`, inline
on its own task (the dispatch completes before it pops the next queued item),
and the `EngineDirective::SchemaIncompatible` arm ignores `fallback_scope`: on
the success path too, the triggering scope is in nobody's re-establishment
set. A queued `Resume` item still holds a cursor of the disowned encoding, so
its first provider call is the likeliest thing to raise the same error and run
a further reset. The `RestartScope` and
`SchemaIncompatible` arms run under the reopen lock;
`re_establish_scope_with_backoff` returns on `Paused | ShuttingDown`.

### Deletion (`multiplexer/mod.rs::apply_lifecycle_transition`, listener `ScopeDeleted` arm in `engine/attach.rs`)

The multiplexer deletes registered scopes from the registry, cancels poll
tokens, and sends `ReopenRequest::ScopeDeleted`; the listener unparks the
scope, retires any cursor established after the deletion was raised, and sends
`reset_scope_for_deletion`. Between the registry delete and the writer's first
invalidation the scope fence has not moved, so a fusion that reaches `Done` in
that window publishes and finalizes, resurrecting the deleted scope in the
registry. Pre-existing; the fusion guard below must close it, or the guard is
incomplete.

### Attach (`crates/sync/src/engine/attach.rs`)

`attach_inner` discovers scopes, then calls `establish_one` per scope;
`establish_one` reads the store directly and, on `Ok(Some(invalid))` or
`Err(SchemaIncompatible)`, deletes that change cursor (logging a failure) and
re-establishes. No writer, no coverage, no publications exist yet.

### Tests that pin today's semantics and change

- `src/engine/tests.rs::a_schema_recovery_reset_deletes_backfill` - calls the
  deleted `reset_scope_for_schema_recovery`; replaced (landing 2).
- `tests/attach_schema_recovery.rs::attach_heals_an_undecodable_cursor_instead_of_failing`
  - asserts `store.deleted() == [scope]`; the clear replaces the per-scope
  delete (landing 2).
- `tests/attach_schema_recovery.rs::one_undecodable_scope_does_not_disturb_its_siblings`
  - asserts the opposite of the new attach rule; replaced (landing 2).
- `tests/schema_recovery_pause.rs` - both schema tests keep their assertions;
  their ablation notes change (landing 2).
- `tests/checkpoint_store.rs::delete_backfill_drops_every_partition_for_the_scope_only`
  - doc comment calls it the schema-clear half; reworded (landing 2).
- Every `BackfillRunner::run_partition` caller in tests
  (`tests/backfill_partition_contract.rs`, `tests/stalled_partition_poll.rs`)
  gains the new argument (landing 1).
- `multiplexer/mod.rs` tests calling `apply_lifecycle_transition` gain the new
  argument (landing 1).
- `multiplexer/fusion.rs` test helpers constructing `InventoryFusion` gain the
  new field (landing 1).
- `src/engine/reattach_tests.rs` callers of `run_establish` and
  `re_establish_scope_with_backoff` gain the `walk_fence` argument (landing 1).
- `src/engine/attach.rs` tests,
  `attach_defers_a_resume_stream_rejected_by_its_classifier`, calls
  `establish_one` with the store-reading signature; it seeds no store row for
  the call and passes the cursor as `stored` instead (landing 2).

## Target design

### Coverage (`cursor/coverage.rs`)

Ledger gains:

```rust
/// Account-wide acknowledgement fence, raised by an account reset. Exclusive
/// bound over this ledger's ids, like `fenced`; once nonzero it also refuses
/// every scoped id this ledger did not mint. Segments come from a
/// process-global counter, so a foreign id from an earlier attachment in THIS
/// process carries a lower segment and is already below the bound; the
/// `minted_here` clause is for a foreign id with a HIGHER segment - a receipt
/// persisted by a previous process (segments restart at 0) - which the bound
/// alone would admit. Any foreign id was minted by an attachment that ended
/// before this reset, so none may land after it.
fenced_account: u64,
```

Everything that touches `fenced_account` (the field, its clause in
`scope_fence` and `claim_in`, `raise_account_fence`, `invalidate_account`) lands
in landing 2 with its only production callers; landing 1 would otherwise add
items that are dead in a non-test build, and any rustc warning fails the
`brokkr check` clippy phase.

`mint` is called only with the ledger lock held:
`fn mint(&self, _held: &mut Ledger, receipt: PublicationReceipt) -> PublicationId`.
`publish` and `register_in` take the lock once, at the top, and hold that one
guard through the mint AND the registration (the receipt is built before
taking the lock, the id after). The borrow in the signature makes minting
without the lock a compile error; it does not by itself stop a future edit
from minting under one guard and registering under a second, so `mint`'s doc
comment states the one-critical-section rule and its reason (an id minted
before an invalidation and registered after it escapes both passes with its
debt stranded). The interleaving this closes lies between two statements
under a mutex and cannot be staged by a deterministic unit test; the
structure is the instrument and `brokkr check` (compile, clippy, and the
existing coverage suite) stands in as its gate. A private
`burn_fence(&self, _held: &mut Ledger) -> u64`
returns `self.next.fetch_add(1, Ordering::Relaxed) + 1`, called only under the
lock: it consumes one id so the returned exclusive bound is strictly greater
than every id minted before and every previous fence.

```rust
#[must_use]
pub fn scope_fence(&self, scope: &CursorScope) -> u64 {
    let ledger = self.guard();
    ledger.fenced.get(scope).copied().unwrap_or(0).max(ledger.fenced_account)
}

/// Revoke every fence-guarded producer of every scope, now. Retires nothing.
pub(crate) fn raise_account_fence(&self);

/// Revoke every fence-guarded producer of one scope, now. Retires nothing.
pub(crate) fn raise_scope_fence(&self, scope: &CursorScope);

/// Retire every boundary entry whose lane has a scope (every lane, every
/// scope; the scope-less Repair lane is untouched), take the debt of their
/// claims (`debt_only`) and the WHOLE carry-forward slot, and raise
/// `fenced_account` with a burnt id. One critical section.
pub(crate) fn invalidate_account(&self) -> CoverageClaim;

/// Mint and register only if `scope_fence(scope) == expected_fence`, compared
/// under the same lock. A refusal registers nothing, mints nothing, and hands
/// the claim back so its debt can be preserved.
pub(crate) fn register_if_fence(
    &self,
    checkpoint: Checkpoint,
    claim: CoverageClaim,
    expected_fence: u64,
    walk_watermark: Option<u64>,
) -> Result<PublicationId, CoverageClaim>;
```

`register_if_fence` takes the scope from `Lane::of(&checkpoint).scope()`; a
lane with no scope is a caller bug and panics (`expect`), since every caller
passes a change or backfill checkpoint. `register_in` becomes the body shared
by `register`, `register_walk_marker` and `register_if_fence`, with the fence
comparison as its first step under the lock when an expected fence is given.

`raise_account_fence`, `raise_scope_fence` and `invalidate_account` each call
`wake_capacity` after dropping the lock. `invalidate_scope` and
`invalidate_scope_backfill` use `burn_fence` instead of `next.load` when they
are unbounded (`through: None`), so the per-scope fence also strictly
increases. A bounded invalidation keeps `ceiling + 1`.

`claim_in`, `fenced_out` becomes:

```rust
let fenced_out = lane.scope().is_some_and(|scope| {
    let account_wide = ledger.fenced_account > id.0
        || (ledger.fenced_account != 0 && !self.minted_here(&id));
    // ... existing scope_wide and lane_wide ...
    account_wide || scope_wide || lane_wide
});
```

### Control (`control.rs`)

```rust
pub(crate) fn publish_checkpoint_if_fence(
    &self, checkpoint: Checkpoint, claim: CoverageClaim, expected_fence: u64,
) -> Result<PublicationId, CoverageClaim>;
pub(crate) fn publish_walk_marker_if_fence(
    &self, checkpoint: Checkpoint, walk_watermark: u64, expected_fence: u64,
) -> Result<PublicationId, CoverageClaim>;
pub(crate) fn raise_scope_fence(&self, scope: &CursorScope);
```

Each forwards to `self.inner.publications`.

**Refused debt.** A refused page is never broadcast, and the producer owes the
writer the debt of the claim it got back. One shared helper in
`crate::inventory_walk` (beside `record_barriers`, which the runner and the
fusion already use):

```rust
/// Persist the debt of a claim whose publication was refused by a fence.
/// Debt only: never proof, never a checkpoint.
pub(crate) async fn record_refused_debt(
    writer_tx: Option<&mpsc::Sender<WriterRequest>>,
    claim: CoverageClaim,
) -> Result<(), Error>;
```

It sends one `WriterRequest::RecordDebt { report, generation: claim.generation, done }`
per report of `claim.debt_only()`, awaiting each `done` before the next. The
outcomes, and why each preserves the debt:

- `Ok(Ok(()))` - persisted.
- `Ok(Err(_))` - the writer RETAINED the report, in one of two ways, and the
  helper logs and continues with the next report:
  - a store error on persist: the `RecordDebt` arm ingests into its
    authoritative in-memory ledger BEFORE it persists, so the debt lands with
    the next successful ledger write (every durable writer mutation writes the
    whole ledger) - the contract every other ledger mutation has;
  - the writer's ledger is not loaded yet (its initial `get_ledger` failed):
    today `reject_unloaded_request` answers `RecordDebt` with the error and
    drops the report. It changes: `ack_writer` keeps
    `pending_debt: Vec<(InventoryCoverageReport, u64)>`, the unloaded arm for
    `RecordDebt` pushes `(report, generation)` onto it before answering `Err`,
    and the moment a later request's load succeeds the writer ingests every
    pending entry with `ingest_debt_only` (timestamp now) before dispatching
    that request, so its write carries them. This also closes the same loss
    for the existing terminal-summary `RecordDebt` callers. `pending_debt` is
    unbounded while the ledger never loads. That is deliberate: dropping an
    entry would be the loss this closes, and every entry is one report a
    producer already held in memory. It is bounded in practice by the walks
    that can run against an account whose store cannot even be read.
- channel closed or `done` dropped - the writer is gone, which means the slot
  is detaching; the helper returns `Err` and the debt is dropped exactly as the
  debt of every outstanding publication is at detach (the next attach
  re-walks). No retry exists to be had.

It is awaited by the producer itself, inside the walk, exactly as
`record_barriers` and `cross_waived_barriers` already are: the writer takes no
scheduler admission, no activity registration and no reopen lock, so a
producer awaiting it while holding its admission cannot deadlock against it.
(The one request that must NOT be sent while holding resources is
`EstablishIfAbsent`, below, whose handler establishes a scope.) The completion
marker's claim is empty, so it sends nothing.

### Registry (`cursor/mod.rs`, `CursorRegistry`)

```rust
/// Insert `cursor` only if `admit()` holds, evaluated under the registry write
/// lock. Lock order registry -> coverage, the order `publish_if_drive_generation`
/// already uses.
pub(crate) fn put_if(&self, cursor: ChangeCursor, admit: impl FnOnce() -> bool) -> bool;

/// Per-scope lifecycle epoch: bumped when a provider deletes the scope.
pub(crate) fn lifecycle_epoch(&self, scope: &CursorScope) -> u64;
pub(crate) fn bump_lifecycle_epoch(&self, scope: &CursorScope);
```

The epochs live in their own `Mutex<HashMap<CursorScope, u64>>` beside the
registry's map: one entry per scope deleted during the attachment, so bounded
by the deletions in one session, not by the account's current scope count
(a deleted folder usually comes back under a new id). Negligible in practice.

The registry's existing per-scope `scope_generations` counter cannot serve as
the lifecycle epoch, and must not be "unified" with it later: `delete` bumps it
on EVERY registry delete - a `RestartScope`, a disable, an account reset - so an
obligation keyed to it would be voided by the very repair that owes it. The
lifecycle epoch moves only on provider deletion.

### Producers

**Backfill runner.** `BackfillRunner::run_partition` gains a parameter
`walk_fence: u64` after `lane`; it is the fence the WALK started under and the
runner never snapshots `scope_fence` itself (`fence_at_pass_start` is
deleted). With a lane, the post-capacity point-in-time comparison is replaced
by `control.publish_checkpoint_if_fence(expected, claim, walk_fence)`; `Err(claim)`
awaits `record_refused_debt(writer_tx, claim)` (its result only logged) and
returns `Err(Error::Superseded)`, which the orchestrator handles as any failed
partition (the driver fails, the marker is withheld). Without a lane (`lane: None`, test callers) the
argument is ignored and publication is unconditional, as today. The
orchestrator passes its `fence_at_walk_start`; test callers pass `0`.

**Completion marker.** `emit_backfill_complete` keeps its two early
comparisons (they avoid parking on the bound for a dead walk) and replaces
`control.publish_walk_marker` with
`control.publish_walk_marker_if_fence(expected, undelivered_before, fence_at_walk_start)`;
`Err(_)` returns `MarkerOutcome::Withheld` with the existing warning (the
marker's claim is empty; there is no debt to send).

**Inventory fusion.** `InventoryFusion` gains `pub expected_fence: u64`.
`forward_inventory_batch` and the `Done` publication in `run_stream` use
`publish_checkpoint_if_fence(.., self.expected_fence)` (with `control: None`,
no publication, as today). `forward_inventory_batch` stays synchronous and
returns `Result<Forwarded, AccountError>` with
`enum Forwarded { Published, Refused(CoverageClaim) }`; on `Refused` nothing is
sent on the delivery. `run_stream` (async) then awaits
`record_refused_debt(self.writer_tx.as_ref(), claim)` (result logged) and
returns `Ok(FusionOutcome::Invalidated)`. The `Done` arm does the same on
refusal, without broadcasting the `Done` event and without calling `finalize`.

A batch with NO checkpoint (including the barrier-stop batch) registers no
claim, so the conditional registration never sees it; without a further check
the fusion would keep streaming `Created` ids - for a deleted folder, ids of a
folder that no longer exists - until its first checkpoint-bearing page. So when
`coverage` is present, `forward_inventory_batch` first compares
`coverage.scope_fence(scope) != self.expected_fence` and, if the fence moved,
returns `Refused` with an empty claim before sending anything (a
checkpoint-less batch carries no claim today and still does not, so there is
no debt to route). The comparison is point-in-time for a checkpoint-less batch,
which is enough: such a batch makes nothing durable.

Every `Invalidated` exit broadcasts one scoped
`Warning::user_safe(WarningKind::Other, "inventory walk superseded by a reset or deletion of its scope")`
before returning, so a consumer does not see an inventory that silently stops
with no `Done`, `Terminated` or `Warning`.

`FusionOutcome::Invalidated` is a new unit variant, doc: "a reset or deletion
of the scope closed while the walk ran; no checkpoint was published, no batch
was forwarded and no cursor was installed after the walk observed it". (Not
"nothing was published after it": a checkpoint-less batch forwarded between the
fence raise and the fusion's next comparison did go out.) `finalize` becomes
`self.cursors.put_if(cursor, || coverage.scope_fence(&scope) == self.expected_fence)`
(with `coverage: None`, unconditional `put`); `false` returns
`FusionOutcome::Invalidated` (the `Done` publication it raced is fenced by the
same raise, so its acknowledgement is refused).

Construction sites: `run_deferred_inventory_establishment` passes the item's
recorded fence (below); both `run_establish` sites pass the `walk_fence`
`run_establish` was given (next paragraph). Reading the fence immediately
before constructing the fusion would repeat the runner defect this spec fixes:
`run_establish` awaits the writer's store read and `pacing.wait` before either
site, a deletion landing in those awaits raises the fence, and a fence read
afterwards adopts the deletion as the walk's own, so a stale resume stream
publishes and installs past every conditional check.

**Establishment fence.** `run_establish` gains `walk_fence: u64`, the fence its
caller's ownership of the establishment began under; it never reads
`scope_fence` itself. Both fusions take it as `expected_fence`. The stored-cursor
install becomes `cursors.put_if(existing, || coverage.scope_fence(&scope) == walk_fence)`
and the `Ready` install the same over `cursor`; `false` returns
`Err(Error::Superseded)`. On the `Ready` path with `persist_ready`, a refusal
can follow a landed `persist_established`; that row belongs to the deletion
whose fence moved, whose `ScopeDeleted` request is queued behind this
establishment on the listener and whose `reset_scope_for_deletion` reaches the
FIFO writer after the persist, so it deletes the row. `run_establish` maps
`Invalidated` to `Err(Error::Superseded)`. Where the fence is read:

- `re_establish_scope_with_backoff(ctx, scope, control, walk_fence)` takes it
  once and passes the same value to every attempt, so a fence moved between
  attempts ends the loop with `Superseded` instead of being adopted by the next
  attempt.
- `restart_scope_admitted` reads `ctx.coverage.scope_fence(&scope)` right after
  `reset_scope_for_restart` returns (its own reset raised the fence; nothing is
  awaited between the reply and the read).
- `handle_schema_incompatible` step 5 reads it per scope after the writer's
  account reset returned.
- `establish_if_absent` reads it BEFORE its epoch and registry checks.
  `retire_deleted_scope` bumps the epoch before it raises the fence, so a fence
  read first and an epoch read second either both predate the deletion or the
  epoch shows it: a deletion can never pass the epoch check with its fence
  already adopted.
- `reattach_account_paced` reads it per scope before calling `run_establish`
  (reattach performs no reset of its own).

The remaining window - a deletion between a caller's reset reply and its fence
read, two synchronous statements apart - is the one the queued `ScopeDeleted`
request already retires, as today.

**Engine error.** `Error::Superseded` (new, doc: "the work was invalidated by
a reset or deletion that closed while it ran; whoever closed it owns the
follow-up"). `re_establish_scope_with_backoff` returns on
`Err(Error::Paused | Error::ShuttingDown | Error::Superseded)`.
`reattach_account_paced` treats `Err(Error::Superseded)` as a skipped scope,
beside its `scope_local_establish_failure` arm (logged, the scope left out of
the staged registry), NOT as a failed attempt: during a reattach, which holds
the reopen lock, the only producer of a moved fence is a provider deletion, and
its queued `ScopeDeleted` owns the follow-up. Routed through the generic arm it
would abort the attempt, so a deletion racing a reattach would cost a full
reattach retry and enough of them would exhaust the reopen budget and pause the
account - a regression this spec would otherwise introduce, since today the
fusion installs unconditionally. `restart_account` sees no `Superseded`
directly; it only sees reattach's result.

**Deferred items.** `DeferredInventory` becomes
`struct DeferredInventory { kind: DeferredKind, fence: u64, epoch: u64 }` with
`enum DeferredKind { Start(CursorScope), Resume(ChangeCursor) }`; `scope()`
stays. `attach_inner` records `fence: 0` (the coverage ledger does not exist
yet, so 0 is the fence of every scope at that instant) and
`epoch: cursors.lifecycle_epoch(&scope)`. On `FusionOutcome::Invalidated` the
worker sends `ReopenRequest::EstablishIfAbsent { scope, epoch }` (awaited, no
admission or activity held), and continues with the next item.

Before it takes admission and opens the provider stream for an item, the worker
compares `coverage.scope_fence(item.scope()) != item.fence`; on a mismatch it
skips the walk, sends `EstablishIfAbsent { scope, epoch: item.epoch }` exactly
as for `Invalidated`, and continues. Without it every queued item crosses an
account reset and runs anyway: a queued `Resume` holds a cursor of the
disowned encoding, its first provider call is the likeliest thing to fail with
the same schema error, and that `Terminated` goes back through `ctx.recover`
and runs another complete account reset - one per queued stale `Resume` - while
the `Invalidated` path designed for it is never reached. The in-fusion guard
stays for a reset that lands mid-walk. Because the worker's own
`ctx.recover` runs inline, a reset one item triggers has committed before the
next item is popped, so the comparison sees it. This check lands in landing 2
with the test that pins it.

### Deletion boundary (`multiplexer/mod.rs`, listener)

One helper owns the ordering, in `multiplexer/mod.rs`:

```rust
/// Take a provider-deleted scope out of service in memory: revoke its
/// obligations (lifecycle epoch), revoke its fence-guarded producers (scope
/// fence), and only THEN drop its registry entry, if any. The order is the
/// point: a fusion finalizing before the fence raise put a cursor the delete
/// below removes; one finalizing after it fails `put_if`.
pub(crate) fn retire_deleted_scope(cursors: &CursorRegistry, control: &SyncControl, scope: &CursorScope) {
    cursors.bump_lifecycle_epoch(scope);
    control.raise_scope_fence(scope);
    cursors.delete(scope);
}
```

`apply_lifecycle_transition` gains `control: &SyncControl`. In `delete_scopes`,
the indexed loop's `cursors.delete(&scope)` becomes
`retire_deleted_scope(cursors, control, &scope)` (still followed by
`cancel_scope_token` and the send), and the unindexed loop calls
`retire_deleted_scope` before its send too (its registry delete is a no-op for
an absent scope). The listener's `ScopeDeleted` arm calls
`retire_deleted_scope` unconditionally in place of its
`if snapshot.is_some() { cursors.delete }`, keeping its `cancel_scope_token`
inside the `is_some` branch (read before the call).

Every deletion therefore bumps the lifecycle epoch, and raises the scope fence,
TWICE: once in the multiplexer, once in the listener. That is intended. The
second call is what retires a cursor a queued repair established after the
multiplexer's call, and the extra bump cannot strand a legitimate obligation:
every `ParkScope` epoch is read at listener decline time, which is after the
multiplexer's bump for any deletion that preceded the request, and the deferred
items' and schema-reset epochs are re-read at execution. The consequence for
readers of epoch values: a park that follows a deletion does not see "the
deletion's epoch plus one" but some later value, so no test may assert exact
epoch numbers across a deletion.

### Establish-if-absent

`ReopenRequest::EstablishIfAbsent { scope: CursorScope, epoch: u64 }` (new
variant). Listener arm, under the reopen lock:

```rust
if cursors.lifecycle_epoch(&scope) != epoch || cursors.snapshot(&scope).is_some() {
    return; // deleted since, or already established
}
if !restart_scope(&recovery, scope.clone()).await {
    // declined under a pause: park (scope, epoch)
}
```

`DeferredScopeRepairs` stores `HashMap<CursorScope, u64>` (scope -> epoch) and
its task re-raises `EstablishIfAbsent { scope, epoch }` instead of a
synthesized `RestartScope`; its outside-the-lock snapshot check is deleted
(the arm checks under the lock). Ownership of a parked entry belongs to the
task that carries its epoch:

- `park(scope, epoch, ..)`: if the map already holds `scope -> epoch`, no-op.
  Otherwise insert (overwriting an older epoch) and spawn a task capturing
  `(scope, epoch)`.
- the task, once `wait_until_running` returns `true`, calls
  `fn claim(&self, scope: &CursorScope, epoch: u64) -> bool`, which removes the
  entry only if it still maps to the captured epoch and reports whether it
  did; the task sends only on `true`. A task whose entry was unparked or
  overwritten by a newer epoch exits silently; the newer epoch's own task owns
  the obligation.
- `unpark(&scope)` removes whatever entry exists.

`handle_account_error` returns the follow-up described next, and the
`RestartScope` arm's deferred scope carries `cursors.lifecycle_epoch(&scope)`
read at decline time.

What that epoch does and does not protect, stated narrowly. Read at decline
time, it is already past the multiplexer's bump for any deletion that preceded
the `Created` request's processing, so it cannot tell "deleted before this
repair was raised" from "recreated": what protects a deleted scope in that
order is FIFO on the reopen channel plus `unpark` in the `ScopeDeleted` arm.
The epoch adds the case `unpark` cannot reach: a deletion that lands after the
park task has claimed its entry and sent `EstablishIfAbsent`, while that
request is still queued; the multiplexer bumps the epoch before it sends
`ScopeDeleted`, so execution sees the mismatch and establishes nothing. The
epoch is load-bearing in full for deferred items (captured at attach) and for
the schema reset's owed map (captured in step 3). Capturing it where the
lifecycle `Created` is raised, so that `ParkScope` gains the stronger
property too, would change the `Created` routing from a synthesized
`RestartScope` to a direct `EstablishIfAbsent`; that is filed, not built here.

### Recovery follow-up

`handle_account_error` returns `RecoveryFollowUp`:

```rust
pub(crate) enum RecoveryFollowUp {
    None,
    /// A cursorless scope repair declined under a pause.
    ParkScope { scope: CursorScope, epoch: u64 },
    /// An account reset declined or exhausted; scopes it owes re-establishment.
    ParkSchemaReset(HashMap<CursorScope, u64>),
}
```

`SlotContext::recover` dispatches `ParkScope` to `deferred_repairs.park` and
`ParkSchemaReset` to `deferred_schema_reset.park` (below).

### Parked schema reset (`engine/reattach.rs`)

```rust
pub(crate) struct DeferredSchemaReset {
    state: Arc<std::sync::Mutex<ParkedReset>>,
}
struct ParkedReset {
    owed: Option<HashMap<CursorScope, u64>>,
    task: bool,
    /// Account resets whose clear committed during this attachment.
    completed: u64,
    /// `completed` as of the most recent `park`.
    parked_at: u64,
}
```

`park(scopes, control, shutdown, reopen_tx)`: merge `scopes` into `owed`
(creating it if `None`; per scope keep the greater epoch); set
`parked_at = completed` (every park, so the newest decline's evidence decides);
if `task` is false, set it and spawn the task. The task:
`control.wait_until_running(&shutdown)`; under the mutex take `owed` and
`parked_at` and clear `task`; if it took `Some(map)`, send
`ReopenRequest::SchemaReset { reestablish: map, parked_at }` (select against
shutdown). `unpark_scope(&scope)` removes one scope from `owed` (called by the
`ScopeDeleted` arm; an optimisation, since execution re-checks epochs).
`reset_completed(&self)` increments `completed`; `handle_schema_incompatible`
calls it when the writer's clear committed (step 5). `completed(&self) -> u64`
reads it. `SlotContext` gains `deferred_schema_reset: Arc<DeferredSchemaReset>`
and `RecoveryContext` borrows it.

`ReopenRequest::SchemaReset { reestablish: HashMap<CursorScope, u64>, parked_at: u64 }`
(new variant). Listener arm, under the reopen lock, factored as
`async fn run_parked_schema_reset(ctx, reestablish, parked_at) -> RecoveryFollowUp`:

- if `ctx.deferred_schema_reset.completed() > parked_at`, a reset committed
  after the newest decline. Its clear was account-wide, so every owed scope's
  rows are already gone; run `establish_if_absent(ctx, scope, epoch)` for each
  owed entry instead of a second reset, merging any `ParkScope` follow-ups;
- otherwise run `handle_schema_incompatible(&recovery, reestablish)`.

The arm parks whatever follow-up it returns.

Why the generation: on a paused decline the registered failing cursors stay
registered and re-raise the directive after resume (today's mechanism), and the
parked obligation re-runs the reset as well. Without the check, a decline costs
two complete resets on every resume when the poll's directive is processed
first, the second wiping the cursors and in-flight backfill the first just
established. The check is made at execution, not at take time in the task, so a
`SchemaReset` already queued on the channel behind a poll's directive still
sees the completed reset. When the parked request is processed first, step 3
deletes the stale cursors from the registry, bumping their drive generations,
so no poll of them raises a later directive; a directive one of them already
queued before that is the filed duplicate-directive cost.

### Account reset (`engine/reattach.rs::handle_schema_incompatible`)

Signature
`async fn handle_schema_incompatible(ctx, extra: HashMap<CursorScope, u64>) -> RecoveryFollowUp`.
The `EngineDirective::SchemaIncompatible` arm passes
`{fallback_scope -> ctx.cursors.lifecycle_epoch(&fallback_scope)}` when the
dispatch carries a fallback scope, and an empty map otherwise. That entry is
what re-establishes a cursorless trigger: the deferred inventory worker
dispatches its terminated fusion with `Some(scope)`, the scope never finalized
so it is not registered, and without the seed the reset clears its rows and
nobody establishes it again this session, on the success path and (through
`ParkSchemaReset(extra)`) the decline path alike. Seeding a registered scope is
harmless: step 3 overwrites its entry and step 5 re-establishes it once. The
epoch is read at dispatch, so a deletion raised before the dispatch is already
counted in it; that scope may then be established by step 5 and is retired by
its queued `ScopeDeleted`, the existing late-cursor path. Order:

1. `ctx.control.begin_activity()`; on refusal return
   `RecoveryFollowUp::ParkSchemaReset(extra)` (the still-registered scopes are
   picked up by the snapshot when it runs; the incoming extras are preserved).
2. `ctx.coverage.raise_account_fence()` - every fence-guarded producer of every
   scope is revoked before anything else moves.
3. `let mut owed = extra;` then for each `s` in `ctx.cursors.all_scopes()`:
   `owed.insert(s.clone(), ctx.cursors.lifecycle_epoch(&s))` (keeping the
   greater epoch) and `ctx.cursors.delete(&s)` - bumps drive generations,
   revoking live drives.
4. Up to `REOPEN_RETRY_BUDGET` attempts of
   `ctx.writer.reset_account_for_schema_recovery()`, with the
   `REOPEN_BACKOFF_INITIAL` doubling `jittered` backoff between attempts through
   `sleep_unless_shutdown` (shutdown returns `RecoveryFollowUp::None`).
5. On success: `ctx.deferred_schema_reset.reset_completed()`; drop every
   `owed` entry whose epoch differs from `ctx.cursors.lifecycle_epoch(scope)`
   or whose scope is registered (an establishment raced in under its own
   ownership), then for each remaining scope read
   `walk_fence = ctx.coverage.scope_fence(&s)` (after the reset's own raises)
   and `re_establish_scope_with_backoff(ctx, s, &admitted, walk_fence)`, and
   return `None`.
6. On exhaustion: broadcast an account-scoped (`None`)
   `Warning::user_safe(OperatorAttentionNeeded, "schema recovery could not clear durable checkpoints")`
   with the last store error as `support_only` protocol detail; drop the
   activity guard; `engine_pause(ctx, PauseReason::RetryBudgetExhausted)`; return
   `ParkSchemaReset(owed)`. After resume the parked task re-runs the whole reset
   with those scopes. `RetryBudgetExhausted`, not `OperatorOverrideRequired`
   (which the spar draft named): the bounded `PauseReason` vocabulary already
   uses `RetryBudgetExhausted` for "the engine's automatic retry budget is
   spent" (the reopen budget in `restart_account`), which is exactly this
   condition, while `OperatorOverrideRequired` is the pause a protocol-raised
   `OperatorOverrideRequired` directive asks for. The operator-attention
   warning carries the specific reason either way.

The reopen lock is held throughout, including the backoff, as `RestartScope`
already does (bounded by the budget).

### Writer (`engine/ack.rs`, `multiplexer/changes.rs`)

**Ledger preservation, adjudicated 2026-10-25.** The reset preserves the
existing `DebtLedger` and adds the retired publications' debt. It neither
discharges nor re-keys obligations, nor resets barriers, operator decisions,
retry history or discharge audits. This follows `reference/sync.md`, "Proof
and policy are separate axes", "One atomic transition" and "Checkpoint
store": invalidating a cursor proves no coverage, only an operator accepts
loss, and a persistent backend must not forget accepted debt. A schema failure
is no stronger evidence than a cursor invalidation. `DebtLedger::ingest`,
`record_proof` and `discharge_repaired` in `cursor/ledger.rs` require accepted
coverage or repair evidence; the reset supplies neither. Deleting an obligation
because its target id has become unreadable would silently erase a known gap.

The ledger has its own envelope version, and can contain nested checkpoints
in barrier incidents. This reset requires a successfully loaded ledger; an
unreadable ledger, including an unreadable nested checkpoint, fails the reset
or attach rather than being replaced with an empty one. Existing id-bound
repair targets remain as recorded. Later accepted coverage discharges them
only when generation and domain
comparison permit it; a re-walk is not guaranteed to do so (page ranges from
different snapshots, for example, are incomparable). Otherwise debt stays
visible and can remain completion-blocking; repair and operator policy retain
their existing behavior. Re-keying targets would require account-owned identity
migration evidence the engine does not have, and is separate work, not an
alternative silently selected by this reset.

`WriterRequest::ResetAccountForSchemaRecovery { done: oneshot::Sender<Result<(), Error>> }`
(new variant, doc: "clear every durable row of the account, completion markers
included, retiring and fencing every scoped publication around the clear").
`Debug` prints the variant name. `reject_unloaded_request` answers it with the
load error. Arm:

```rust
let mut debt = coverage.invalidate_account();
ingest(ledger, &debt);
let cleared = store.clear_account_checkpoints(&account_id, ledger.clone()).await;
if cleared.is_ok() {
    provisional.clear();
}
debt = coverage.invalidate_account();
let carried = !debt.reports.is_empty();
ingest(ledger, &debt);
let second = if cleared.is_err() || carried {
    persist_ledger_only(&account_id, &store, ledger).await
} else {
    Ok(())
};
let _ = done.send(cleared.and(second));
```

(`ingest` is the existing `for report in &debt.reports {
ledger.ingest_debt_only(report, debt.generation, now) }`.) A retry after a
pass-2 failure is idempotent and re-persists the in-memory ledger that holds
the pass-2 debt. `provisional` is cleared only by a committed clear: a failed
clear leaves the rows, and their delayed `ReattachAbort` must still find its
ownership.

The ledger write after a FAILED clear keeps the per-scope arm's guarantee. The
`ResetScope` arm persists its pass-1 debt BEFORE its deletes, so a failed delete
still leaves that debt durable; the account arm couples pass-1 debt to the
clear's atomicity, so without this write a failed clear with no pass-2 debt
would leave the pass-1 debt - already removed from the coverage ledger by
`invalidate_account` - only in writer memory, lost if the reset then exhausts
and the slot detaches. Its result does not change the answer (`cleared` is
already `Err`), and a failure of it is only logged.

`WriterHandle::reset_account_for_schema_recovery(&self) -> Result<(), Error>`
replaces `reset_scope_for_schema_recovery`, which is deleted (crate-private).
Its doc carries the reason the reset is account-wide.

### Store (`cursor/store.rs`)

**Required method, adjudicated 2026-10-25.** Add
`CheckpointStore::clear_account_checkpoints` with no default body. This adds a
published item; it removes or renames none, so `AGENTS.md`'s mandatory owner
stop for removals and renames does not apply. The 2026-09-30 shape ruling's
fields-and-variants examples need not be stretched to cover a trait method:
the owner's 2026-10-21 instruction in `notes/bugs-sync.md` explicitly calls for
this specification of a changed consumer-implemented store contract, and
`reference/sync.md`'s schema-recovery gap requires an account-wide operation
through the writer. This is that requested contract change, with the downstream
implementation obligations stated below. Every downstream implementation must
add the method; workspace success does not prove downstream compatibility.

The 2026-09-06 ruling has been read at
`PendingCoverage::release_undelivered` in `cursor/coverage.rs` and in
`reference/sync.md`, "Completion integrity across consumer replacement". It
declined a durable scope-loss row and a new store method for pages the consumer
had already forfeited under the detach contract. Its stated reopening condition
concerns paying for THAT row. It is a proportionality ruling for that issue,
not a ban on new store methods. Here the engine's own reset leaves completion
markers suppressing the migration it owes; no consumer contract violation is
involved. The separate compare-and-swap proposal in `reference/sync.md` likewise
does not reserve every trait addition for another ruling after the owner has
requested a specification of this contract change.

An engine-maintained scope index cannot enumerate rows written before that
index existed. A store enumeration method would itself require a new method
and still need an atomic delete-plus-ledger operation. The existing trait has
neither enumeration nor an account-wide primitive, so no provided default can
perform this reset. A no-op default would silently retain the defect; a default
returning an unsupported error would leave existing stores unable to complete
schema recovery and turn a compile-time implementation obligation into a
runtime failure. Neither satisfies the requested complete fix.

Signature and implementation contract:

```rust
/// Drop every change cursor and every backfill checkpoint stored for
/// `account` - every scope, completion markers included - and persist `ledger`
/// as the account's debt ledger, as ONE atomic operation.
///
/// Used by the engine's `SchemaIncompatible` recovery and by attach when it
/// meets a row it cannot read. A schema bump means the ids the consumer holds
/// were minted under an encoding the protocol has disowned, and any surviving
/// completion marker - including one of a scope the engine does not currently
/// track - makes the next establishment of that scope skip the inventory walk
/// that re-mints them. There is deliberately no default body: the trait has no
/// enumeration, so any default would leave exactly those rows behind.
///
/// Atomic, like `apply_transition`: either every row is gone and the ledger is
/// written, or nothing changed. Rows of other accounts are untouched.
///
/// What an implementation needs: one transaction spanning the account's change
/// cursors, its backfill rows and its ledger, plus a way to address every row of
/// one account (a key prefix or an account column) without the engine naming
/// the scopes.
fn clear_account_checkpoints<'a>(
    &'a self,
    account: &'a AccountId,
    ledger: DebtLedger,
) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
```

`InMemoryCheckpointStore`: one lock; `change.retain`, `backfill.retain` on the
account, `ledgers.insert`. `delete_backfill`'s doc is rewritten to name its real
callers (scope deletion reset, backfill discard) and drop the schema claim.

### Attach (`engine/attach.rs`)

`establish_one(account_id, account, scope, stored: Option<ChangeCursor>, cursors)`
no longer reads the store; it starts from `stored` exactly where today's
`Ok(Some(existing)) if valid` / `Ok(None)` arms do. `attach_inner`, after
discovery and before the loop:

```rust
let mut stored = HashMap::new();
let mut unreadable = false;
for scope in &scopes {
    match self.checkpoints.get_change_cursor(&account_id, scope).await {
        Ok(Some(cursor)) if cursor.validate_envelope().is_ok() => { stored.insert(scope.clone(), cursor); }
        Ok(None) => {}
        Ok(Some(_)) | Err(Error::SchemaIncompatible) => unreadable = true,
        Err(other) => return Err(other), // after closing `opened`, as every attach failure does
    }
}
if unreadable {
    warn!("an unreadable durable cursor; clearing every durable row of the account and establishing every scope fresh");
    let ledger = self.checkpoints.get_ledger(&account_id).await?;
    self.checkpoints.clear_account_checkpoints(&account_id, ledger).await?;
    stored.clear();
}
```

A failed clear fails the attach (the consumer retries it); establishing fresh
over surviving markers would recreate the defect. The failure exit closes the
opened account exactly as the existing `Err(other)` exits do.

**Account-wide attach healing, adjudicated 2026-10-25.** One unreadable stored
change cursor is sufficient to clear every change cursor and backfill row of
that account, including undiscovered scopes; the ledger survives as specified
above. `reference/sync.md`, "Cursor envelope", deliberately classifies both
older and newer unsupported envelope versions as `SchemaIncompatible`;
`establish_one` also treats a failed `validate_envelope` that way. Those signals
cannot distinguish local corruption from an account-wide encoding change. The
per-scope heal removes the only evidence before a protocol worker can request
the schema reset, preserving the very completion markers this item must clear.
The owner-requested complete migration fix therefore includes this reader of
the same checkpoint identity. Re-walking healthy siblings on corruption or
downgrade is the conservative cost of that classification, rather than grounds
to preserve a path that can silently skip migration. Ordinary store failures
still propagate as errors; `Ok(None)` alone does not trigger a reset.

The current
`tests/attach_schema_recovery.rs::one_undecodable_scope_does_not_disturb_its_siblings`
pins the old per-scope deletion, but seeds no readable sibling cursor or
completion marker. It establishes both scopes and only asserts which scope
was deleted; it cannot establish that retained sibling rows are safe under a
schema migration. Its replacement below seeds those rows and pins the new
contract. This changes a test's behavior, not a published item's identity, and
no standing rule protects the defective behavior from the requested fix.

## Landings

Two landings, each one coherent change kept or reverted on its gates, each
leaving `brokkr check` green. Landing 1 closes the revocation holes and the
deletion boundary and is valuable on its own; landing 2 builds the account
reset on top.

### Landing 1: fence-guarded registration, walk fences, deletion revocation, establish-if-absent

Bricks: coverage changes (mint under the lock, `burn_fence`,
`raise_scope_fence`, `register_if_fence`; no `fenced_account` yet, see
"Coverage" above); control wrappers; registry `put_if` and lifecycle epochs;
runner `walk_fence`; marker conditional registration; fusion
`expected_fence`, conditional pages and `Done`, the pre-forward fence check for
checkpoint-less batches, the superseded warning, `put_if` finalize,
`FusionOutcome::Invalidated`; `run_establish`'s `walk_fence` and its
conditional installs, with every caller reading the fence where "Establishment
fence" says; refused-claim debt routing; `Error::Superseded` and its mapping,
including the reattach skip; `DeferredInventory` struct; deletion boundary;
`ReopenRequest::EstablishIfAbsent` and its arm; `DeferredScopeRepairs`
re-raising it; `RecoveryFollowUp` with `None` and `ParkScope` (the
`ParkSchemaReset` variant lands in landing 2).

Instruments (each a new test; each ablation is reverted after confirming it
fails):

1. `cursor/coverage.rs` tests,
   `a_scope_fence_moves_even_when_nothing_was_minted`: read `scope_fence` of a
   scope with no entry, `raise_scope_fence` twice with no registration between;
   each reading strictly exceeds the previous.
   `brokkr test -p bifrost-sync a_scope_fence_moves_even_when_nothing_was_minted`
   Ablation: compute the fence with `next.load` instead of `burn_fence`.
2. `register_if_fence_refuses_a_stale_fence_and_registers_nothing`: after
   `raise_scope_fence(A)`, `register_if_fence` with the old fence returns
   `Err(claim)` whose `reports` and `generation` equal those of the claim
   passed in (`CoverageClaim` derives only `Debug` and `Clone`, so the test
   compares the two fields, the reports through their encoded form or an
   equality the report type already has), and `pending_checkpoints()` is
   unchanged; with the current fence it returns `Ok`.
   `brokkr test -p bifrost-sync register_if_fence_refuses_a_stale_fence_and_registers_nothing`
   Ablation: skip the comparison.
3. `multiplexer/fusion.rs` tests,
   `a_fusion_page_under_a_moved_fence_is_refused_and_its_debt_recorded`: a
   fusion whose `expected_fence` was read before `raise_scope_fence`, with a
   writer sender whose receiver the test drives (answering each `done` with
   `Ok`), run over a scripted stream of one degraded page and `Done`; outcome
   `Invalidated`, `cursors.snapshot(scope)` is `None`,
   `pending_checkpoints() == 0`, nothing reached a receiver subscribed to the
   delivery's sender, and the writer receiver got one `RecordDebt` carrying the
   page's obligation.
   `brokkr test -p bifrost-sync a_fusion_page_under_a_moved_fence_is_refused_and_its_debt_recorded`
   Ablation: drop the `record_refused_debt` call.
4. `a_fusion_done_under_a_moved_fence_is_neither_published_nor_installed`: a
   scripted stream built with `futures::stream::unfold` that yields one clean
   page, and on its NEXT poll calls `coverage.raise_scope_fence(&scope)` before
   yielding a checkpoint-bearing `Done`; outcome `Invalidated`, no event
   carrying `SyncEvent::Done` reached a receiver subscribed to the delivery's
   sender, and `cursors.snapshot(scope)` is `None`.
   `brokkr test -p bifrost-sync a_fusion_done_under_a_moved_fence_is_neither_published_nor_installed`
   Ablation: publish `Done` through the unconditional `publish_checkpoint`
   (the `Done` event is then broadcast).
5. `a_fusion_finalize_under_a_moved_fence_installs_nothing`: call the private
   `finalize` directly from the fusion test module with a valid cursor for
   `scope` and an `expected_fence` read before `raise_scope_fence(&scope)`;
   it returns `Ok(FusionOutcome::Invalidated)` and `cursors.snapshot(scope)` is
   `None`.
   `brokkr test -p bifrost-sync a_fusion_finalize_under_a_moved_fence_installs_nothing`
   Ablation: restore the unconditional `cursors.put` in `finalize`.
6. `a_fusion_forwards_no_batch_once_its_fence_has_moved`: a fusion whose
   `expected_fence` was read before `raise_scope_fence`, run over a scripted
   stream of one checkpoint-LESS page carrying an item and a checkpoint-bearing
   `Done`; outcome `Invalidated`, no `SyncEvent::Batch` reached a receiver
   subscribed to the delivery's sender, and exactly one scoped
   `SyncEvent::Warning` did (the superseded warning).
   `brokkr test -p bifrost-sync a_fusion_forwards_no_batch_once_its_fence_has_moved`
   Ablations: drop the pre-forward fence check (the batch is broadcast; the
   refused `Done` still yields `Invalidated`, so only the batch assertion
   bites); drop the warning broadcast.
7. `engine/lane.rs` tests (the `MarkerHarness` module),
   `a_partition_pass_judges_pages_against_the_walk_fence_it_was_given`: run
   `run_partition` with a lane and `walk_fence` read before
   `raise_scope_fence(scope)`; the call returns `Err` and nothing reached the
   harness receiver.
   `brokkr test -p bifrost-sync a_partition_pass_judges_pages_against_the_walk_fence_it_was_given`
   Ablation: restore the runner's own `scope_fence` snapshot.
8. `src/engine/reattach_tests.rs`,
   `run_establish_judges_its_installs_against_the_fence_it_was_given`: two
   calls of `run_establish` with a `walk_fence` read before
   `raise_scope_fence(&scope)`. This is the gated "establishment parked before
   fusion construction, scope retired, establishment released" staging, made
   deterministic by the fence being a parameter: a deletion landing inside the
   establishment's awaits is exactly a fence older than the current one. (a) A
   `common::StubAccount` answering `Ready`, `persist_ready = false`: the call
   returns `Err(Error::Superseded)` and `cursors.snapshot(scope)` is `None`.
   (b) A stub answering `EstablishViaInventory` whose `inventory_hook` yields
   one checkpoint-bearing page and a checkpoint-bearing `Done`: the call
   returns `Err(Error::Superseded)`, the registry does not hold the scope, and
   no `SyncEvent::Batch` or `SyncEvent::Done` reached a receiver subscribed to
   the delivery.
   `brokkr test -p bifrost-sync run_establish_judges_its_installs_against_the_fence_it_was_given`
   Ablations: restore the unconditional `cursors.put` on the `Ready` path ((a)
   fails); construct the fusion with `coverage.scope_fence(&scope)` read at
   construction ((b) fails: the fusion adopts the moved fence and publishes).
   Where each caller reads the fence (before the epoch check in
   `establish_if_absent`, once per `re_establish_scope_with_backoff` call) is
   ordering between a caller's statements and a deletion on another task, not
   stageable deterministically; it is written once per caller with the reason
   in a comment, and `brokkr check` stands in as its gate, as for the mint
   rule.
9. `src/engine/reattach.rs` tests,
   `a_superseded_scope_is_skipped_by_a_reattach_not_failed`: factor
   `reattach_account_paced`'s per-scope match into
   `fn reattach_scope_step(result: &Result<EstablishOrigin, Error>) -> ReattachScopeStep`
   (`Keep`, `Skip`, `Fail`); `Err(Error::Superseded)` maps to `Skip`, a
   scope-local `Error::Account` to `Skip`, any other error to `Fail`.
   `brokkr test -p bifrost-sync a_superseded_scope_is_skipped_by_a_reattach_not_failed`
   Ablation: drop the `Superseded` arm (it maps to `Fail`). This pins the
   mapping, not a live reattach racing a deletion; the wiring is the one match
   in `reattach_account_paced` that calls it.
10. `multiplexer/mod.rs` tests,
    `retiring_a_deleted_scope_revokes_its_producers_before_its_cursor`: with a
    registered scope and a `SyncControl` over a fresh `PendingCoverage`, read
    the scope fence (`old`) and call `retire_deleted_scope`. Then a
    fusion-style `cursors.put_if(cursor, || coverage.scope_fence(&scope) == old)`
    returns `false`, the registry does not hold the scope, and the lifecycle
    epoch increased. Synchronous and writer-free, so no later invalidation can
    mask a missing raise.
    `brokkr test -p bifrost-sync retiring_a_deleted_scope_revokes_its_producers_before_its_cursor`
    Ablation: drop the `raise_scope_fence` call from the helper (the probe's
    `put_if` then succeeds and resurrects the scope).
11. Both deletion paths call `retire_deleted_scope`; that wiring is pinned by
    `a_deleted_scope_leaves_no_registry_entry_and_a_moved_fence` in
    `multiplexer/mod.rs` tests (apply `ScopeLifecycle::Deleted` through
    `apply_lifecycle_transition` with no writer behind it; fence and epoch
    increased, registry entry gone). The listener arm is not separately
    pinned: a deterministic test of it would observe the fence only after the
    writer's own reset has also raised it, which masks a missing call (the
    spar's finding); it relies on calling the same helper, and the existing
    `tests/schema_recovery_pause.rs::a_folder_deleted_while_its_repair_is_queued_leaves_no_live_cursor`
    keeps pinning that it retires the late cursor. Instrument 10 pins the
    raise's PRESENCE, not its order: with the two statements swapped its probe
    still runs after both and still refuses. The order (fence before registry
    delete) matters only against a fusion finalizing on another task between
    the two statements, an interleaving no deterministic test can stage; it is
    enforced structurally by the helper being the one place the sequence is
    written, with the reason in its doc comment, and `brokkr check` stands in
    as its gate, as for the mint rule.
    `brokkr test -p bifrost-sync a_deleted_scope_leaves_no_registry_entry_and_a_moved_fence`
    Ablation: call `cursors.delete` instead of the helper in `delete_scopes`.
12. `src/engine/reattach_tests.rs` (its `Fixture` builds a `RecoveryContext`
    over a real slot), `establish_if_absent_skips_a_registered_or_deleted_scope`:
    call the `EstablishIfAbsent` arm's handler directly (factor it as
    `async fn establish_if_absent(ctx: &RecoveryContext<'_>, scope, epoch) -> RecoveryFollowUp`,
    called under the reopen lock as the arm does) three times against a
    `common::StubAccount` whose `established` list counts establishments: with the
    scope registered (no establishment), with a stale epoch after
    `bump_lifecycle_epoch` (no establishment), and absent with the current
    epoch (one establishment).
    `brokkr test -p bifrost-sync establish_if_absent_skips_a_registered_or_deleted_scope`
    Ablation: delete the registered-scope check.
13. Existing `tests/schema_recovery_pause.rs::a_folder_deleted_and_recreated_during_a_pause_is_established_once`
    already stages the epoch-ownership case: a `Created` parked at some epoch
    E1, a `Deleted` whose multiplexer and listener calls each bump the epoch
    (see "Deletion boundary"), a second `Created` parked at a later epoch E2,
    resume. The exact values depend on whether the listener handled the first
    `Created` before or after the multiplexer's bump, so the test asserts none
    of them; it asserts one establishment. It keeps its assertions; its doc
    gains the epoch-ownership ablation, which must be run:
    `brokkr test -p bifrost-sync a_folder_deleted_and_recreated_during_a_pause_is_established_once`
    Ablation: make the park task remove its entry unconditionally and send its
    captured epoch (the older task consumes the E2 obligation, its E1 request
    is discarded at execution, and the scope is never established). Which task
    wakes first is scheduler-dependent there, so the ownership rule also gets
    a deterministic unit test in `engine/reattach.rs` tests,
    `a_park_claim_under_a_stale_epoch_takes_nothing`: construct the parked
    state `(scope, E)` and then `(scope, E + 1)` through a crate-private
    `#[cfg(test)]` `insert_for_test(scope, epoch)` (`park` would spawn a task;
    this runs none), then call `claim` directly: `claim(&scope, E)` is `false`
    and leaves the entry; `claim(&scope, E + 1)` is `true`.
    `brokkr test -p bifrost-sync a_park_claim_under_a_stale_epoch_takes_nothing`
    Ablation: make `claim` remove unconditionally.
14. A PIN of existing behaviour, not a bite test of this work: the `RecordDebt`
    arm already ingests before it persists, so there is no production change
    here for it to bite against, and its ablation below is a hypothetical
    alternative implementation. Kept because `record_refused_debt`'s
    "retained on a store error" outcome rests on it.
    `src/engine/tests.rs`,
    `refused_debt_survives_a_failed_ledger_write`: over `writer_harness_over` a
    store whose first `put_ledger` fails (the `FailingOnceStore` shape),
    `record_refused_debt` of a degraded claim returns `Ok` after logging the
    store error; then any successful ledger write (a second `RecordDebt` of an
    unrelated clean report) lands, and the persisted ledger holds the first
    claim's obligation.
    `brokkr test -p bifrost-sync refused_debt_survives_a_failed_ledger_write`
    Ablation: make the `RecordDebt` arm persist a clone with the report
    ingested and only adopt it on success (the debt is then lost with the
    failed write).
15. `engine/ack.rs` tests (beside `FlakyLedgerStore`, whose `get_ledger` fails
    on demand), `debt_offered_before_the_ledger_loads_is_kept`: with the first
    `get_ledger` failing, send `RecordDebt` of a degraded report (answered
    `Err`); let the load succeed and send any ledger-writing request (a clean
    `RecordDebt`); the persisted ledger holds the first report's obligation.
    `brokkr test -p bifrost-sync debt_offered_before_the_ledger_loads_is_kept`
    Ablation: drop the push onto `pending_debt` in the unloaded arm.

Existing tests that must keep passing unchanged:
`a_marker_may_not_adopt_a_fence_from_a_later_incarnation`,
`debt_published_during_a_scope_reset_survives_it`, every
`tests/schema_recovery_pause.rs` test, every `tests/backfill_lane_flow_control.rs`
test.

Gate: `brokkr check`

### Landing 2: the account reset, its store contract, and attach

Bricks: `CheckpointStore::clear_account_checkpoints` with the in-memory and
every test-double implementation; `WriterRequest::ResetAccountForSchemaRecovery`,
its arm and `reject_unloaded_request` arm; `WriterHandle::reset_account_for_schema_recovery`
and deletion of `reset_scope_for_schema_recovery`; the ledger write after a
failed clear; the `fenced_account` plumbing (field, `scope_fence` and
`claim_in` clauses, `raise_account_fence`, `invalidate_account`);
`RecoveryFollowUp::ParkSchemaReset`; `DeferredSchemaReset` with its reset
generation, and `ReopenRequest::SchemaReset` with `run_parked_schema_reset`;
the rewritten `handle_schema_incompatible`, including the fallback-scope seed;
the deferred worker's pre-walk fence check; attach pre-read and clear;
documentation.

Instruments:

1. `tests/checkpoint_store.rs`,
   `clear_account_checkpoints_drops_every_row_of_the_account_and_only_that_account`:
   change cursors and backfill rows (completion marker included) on two scopes
   of account `a`, one row on account `b`; clear `a` with a ledger carrying a
   recorded barrier; every `a` row reads `None`, `b`'s row survives, and
   `encode_ledger(get_ledger(a))` equals `encode_ledger` of the passed ledger.
   `brokkr test -p bifrost-sync clear_account_checkpoints_drops_every_row_of_the_account_and_only_that_account`
   Ablation: leave `backfill` untouched in the in-memory impl.
2. `src/engine/tests.rs`,
   `an_account_schema_reset_deletes_every_scope_and_its_completion_markers`
   (replaces `a_schema_recovery_reset_deletes_backfill`): seed a completion
   marker and a change cursor on the email scope and on a contact scope
   nothing in the harness ever registers. BEFORE starting `writer_harness_over`,
   seed its `AccountId("acct".into())` with a ledger using `degraded_claim`'s
   object-obligation shape: three distinct keys with old-encoding object ids,
   one left retrying, one `waive`d and one `block`ed; call `record_attempt` on
   the retrying key with budget 3. Save its `encode_ledger` bytes. With no
   pending publications, `reset_account_for_schema_recovery`;
   all four checkpoint rows read `None`, and the persisted ledger's encoded
   bytes equal the saved bytes. The old target ids, unresolved proof, operator
   decisions and attempt count all survive. Seed before the harness can load
   its authoritative ledger; a later direct store write would bypass the
   writer and invalidate the setup.
   `brokkr test -p bifrost-sync an_account_schema_reset_deletes_every_scope_and_its_completion_markers`
   Ablations, separately: replace the arm's clear with `delete_backfill` of the
   email scope only; pass `DebtLedger::default()` to the account clear.
3. `debt_published_during_an_account_reset_survives_it`: the shape of
   `debt_published_during_a_scope_reset_survives_it`, over a `GatedClearStore`
   (wraps the in-memory store, parks inside `clear_account_checkpoints` on a
   semaphore); a degraded publication on a second scope registered while the
   writer is parked; after release, the persisted ledger holds that
   publication's obligation.
   `brokkr test -p bifrost-sync debt_published_during_an_account_reset_survives_it`
   Ablation: delete pass 2.
4. `a_failed_second_pass_ledger_write_fails_the_account_reset`: a store whose
   `put_ledger` fails and whose clear succeeds, with a degraded publication
   registered inside the gated clear; the reset returns `Err`.
   `brokkr test -p bifrost-sync a_failed_second_pass_ledger_write_fails_the_account_reset`
   Ablation: send `cleared` instead of `cleared.and(second)`.
5. `a_failed_account_clear_keeps_reattach_ownership`: `reattach_insert`
   attempt 7 for the email scope; a reset over a store whose clear fails;
   `reattach_abort(7)`; the email change cursor reads `None`.
   `brokkr test -p bifrost-sync a_failed_account_clear_keeps_reattach_ownership`
   Ablation: clear `provisional` before the transaction.
6. `tests/schema_recovery_pause.rs`,
   `schema_recovery_clears_the_completion_marker_of_an_unregistered_scope`:
   the `failing_on_demand` account (discovers EMAIL and EVENTS) over an
   in-memory store seeded with a change cursor and a completion marker
   (`bifrost_sync::backfill::partitioner::completion_partition()`) for
   `CursorScope::Type(ObjectType::Contact)`, which the account never
   discovers; fire one schema error; after recovery the contact scope's
   cursor and backfill both read `None`, and EMAIL and EVENTS are established
   again (`established(&account).len() == 4`).
   `brokkr test -p bifrost-sync schema_recovery_clears_the_completion_marker_of_an_unregistered_scope`
   Ablation: replace the writer request with a per-registered-scope
   `ResetScope { delete_backfill: true }` loop.
7. `a_schema_reset_whose_clear_keeps_failing_pauses_and_owes_its_scopes`:
   the same account over a store whose `clear_account_checkpoints` fails while
   an `AtomicBool` is set; fire; the account control stream yields
   `Pause(RetryBudgetExhausted)` and nothing is established while paused;
   clear the flag and `resume`; EMAIL and EVENTS are established again and
   their durable cursors exist. `#[tokio::test(start_paused = true)]` like the
   rest of the file: the three writer attempts back off 1s then 2s and the
   re-establishment follows, which under real time would be wall-clock sleeps
   and would run into the 20s watchdog.
   `brokkr test -p bifrost-sync a_schema_reset_whose_clear_keeps_failing_pauses_and_owes_its_scopes`
   Ablation: return `RecoveryFollowUp::None` on exhaustion.
8. `src/engine/reattach_tests.rs`,
   `a_declined_schema_reset_keeps_the_scopes_it_owes`: on the `Fixture` slot,
   `slot.control.pause().await`, then call
   `handle_schema_incompatible(&ctx, extra)` directly (under the reopen lock,
   as the dispatch does) with `extra = {EMAIL -> 0, EVENTS -> 0}`; it returns
   `RecoveryFollowUp::ParkSchemaReset(map)` with `map == extra`, and the
   writer received no reset (the store rows seeded before the call still
   exist). Deterministic: the refusal branch is entered by construction, with
   no task scheduling involved. The parked task's take-and-send and its merge
   are pinned by instrument 7 (end to end) and by the unit test
   `parked_schema_resets_merge_by_greater_epoch` in `engine/reattach.rs` tests:
   `park` `{A -> 2}` then `{A -> 1, B -> 0}` through a crate-private
   `#[cfg(test)]` `merge_for_test` that skips the spawn; the owed map is
   `{A -> 2, B -> 0}`.
   `brokkr test -p bifrost-sync a_declined_schema_reset_keeps_the_scopes_it_owes`
   `brokkr test -p bifrost-sync parked_schema_resets_merge_by_greater_epoch`
   Ablations: return `ParkSchemaReset(HashMap::new())` on the refusal path;
   make the merge overwrite instead of keeping the greater epoch.
9. `tests/attach_schema_recovery.rs`,
   `one_undecodable_scope_clears_every_row_of_the_account_at_attach`
   (replaces `one_undecodable_scope_does_not_disturb_its_siblings`): poisoned
   EMAIL; MAILBOX with a stored readable cursor and a completion marker;
   after attach MAILBOX was established fresh (it is in `established`) and its
   completion marker reads `None`. `PoisonedStore::clear_account_checkpoints`
   records a call count, clears every poisoned scope and delegates. Also seed
   a contact scope's cursor and completion marker absent from discovery; both
   must read `None`. Seed an unresolved object obligation in the ledger before
   attach, save its `encode_ledger` bytes, and assert the stored ledger has
   identical bytes after attach. Use `HealFactory`'s idle changes stream so no
   accepted coverage can discharge that obligation during the assertion.
   `brokkr test -p bifrost-sync one_undecodable_scope_clears_every_row_of_the_account_at_attach`
   Ablations, separately: restore the per-scope `delete_change_cursor` heal;
   pass `DebtLedger::default()` from attach to the account clear.
10. `attach_heals_an_undecodable_cursor_instead_of_failing`: assertion on
    `store.deleted()` becomes "one account clear was recorded"; the rest is
    unchanged.
    `brokkr test -p bifrost-sync attach_heals_an_undecodable_cursor_instead_of_failing`
11. `cursor/coverage.rs` tests, `an_account_fence_refuses_every_scope_minted_before_it`:
    register a change publication on scope A and one on scope B (B never reset
    per scope), `raise_account_fence`, both `claim_checkpoint` answer `Unknown`;
    a publication registered on A afterwards answers `Apply`.
    `brokkr test -p bifrost-sync an_account_fence_refuses_every_scope_minted_before_it`
    Ablation: drop the `fenced_account > id.0` clause.
12. `an_account_fence_moves_every_scope_fence_even_when_nothing_was_minted`:
    read `scope_fence` of a scope with no entry, `raise_account_fence` twice
    with no registration between; each reading strictly exceeds the previous.
    `brokkr test -p bifrost-sync an_account_fence_moves_every_scope_fence_even_when_nothing_was_minted`
    Ablation: compute the account fence with `next.load` instead of
    `burn_fence`.
13. `an_account_fence_refuses_a_publication_from_another_process`: construct
    the CURRENT `PendingCoverage` FIRST and the foreign one AFTER it, so the
    foreign ids carry a HIGHER segment - the shape of a receipt persisted by a
    previous process, whose segments restarted at 0. (Constructed the other
    way round, as the existing replay test stages `previous` then `current`,
    the foreign ids sit below `fenced_account` and the `fenced_account > id.0`
    clause alone refuses them, so the ablation below would pass.) A change
    publication minted by the foreign ledger; before the raise
    `claim_checkpoint` on the current one answers `Apply` (receipt fallback),
    after it `Unknown`. The test's doc states the construction-order reason.
    `brokkr test -p bifrost-sync an_account_fence_refuses_a_publication_from_another_process`
    Ablation: drop the `minted_here` clause.
14. `invalidate_account_carries_every_scoped_debt_and_spares_repair_claims`:
    the module's `degraded(key)` helper is pinned to one scope, so the test
    adds a `degraded_on(scope, key)` variant (or `degraded` gains the scope
    argument) to build reports for A, B and C. FIRST register a degraded
    publication on scope C and `abandon_checkpoints()` (its debt moves to the
    carry-forward slot; done first because `abandon_checkpoints` abandons every
    outstanding registration); THEN register degraded publications on scopes A
    and B and a repair publication; the claim `invalidate_account` returns
    holds the reports of A, B and C, `pending_checkpoints() == 0`, and
    `claim_repair` of the repair id still answers `Apply`.
    `brokkr test -p bifrost-sync invalidate_account_carries_every_scoped_debt_and_spares_repair_claims`
    Ablation: retire only the entries of the first scope found.
15. `src/engine/tests.rs`,
    `a_failed_account_clear_still_persists_its_first_pass_debt`: a degraded
    publication registered on a scope, then a reset over a store whose
    `clear_account_checkpoints` fails and whose `put_ledger` succeeds; the
    reset returns `Err`, and `get_ledger` holds the publication's obligation.
    `brokkr test -p bifrost-sync a_failed_account_clear_still_persists_its_first_pass_debt`
    Ablation: write the ledger only when pass 2 carried debt (the shape before
    this fold).
16. `tests/attach_schema_recovery.rs`,
    `a_deferred_inventory_that_raises_schema_incompatible_is_established_after_the_reset`:
    a `common::StubAccount` discovering two scopes A and B, both answering
    `EstablishViaInventory`, so both become attach-time deferred items, A
    queued first. Its `inventory_hook` counts walks per scope; it terminates
    A's FIRST walk with a `SyncState(SchemaIncompatible)` error and completes
    every other walk with a checkpoint-bearing `Done`. The test subscribes a
    numbered receiver (the worker waits for one) and acknowledges what it
    receives. After recovery both A and B are registered, A was walked twice
    (the failing walk and the reset's step 5) and B exactly once.
    `start_paused = true`.
    `brokkr test -p bifrost-sync a_deferred_inventory_that_raises_schema_incompatible_is_established_after_the_reset`
    Ablations: pass an empty `extra` from the directive arm (A is never
    registered); drop the worker's pre-walk fence check (B walks twice: its
    queued walk runs after the reset, is refused at its first publication and
    is re-run through `EstablishIfAbsent`).
17. `src/engine/reattach_tests.rs`,
    `a_parked_schema_reset_after_a_completed_one_only_establishes`: on the
    `Fixture` slot, with change cursors seeded in the store for EMAIL and a
    scope nothing registers, call `run_parked_schema_reset(&ctx, {EMAIL -> e}, parked_at)`
    directly (under the reopen lock) after `reset_completed()` has moved
    `completed` past `parked_at`, with `e` the current epoch; the seeded rows
    still exist (no second clear ran) and EMAIL was established once (the
    stub's `established` list).
    `brokkr test -p bifrost-sync a_parked_schema_reset_after_a_completed_one_only_establishes`
    Ablation: ignore `parked_at` and always run `handle_schema_incompatible`
    (the seeded rows are cleared).

`tests/schema_recovery_pause.rs::schema_recovery_reaching_a_paused_account_deletes_nothing_and_runs_after_resume`
changes in landing 2 (it stays unchanged through landing 1). The second
`fire.store(1, Ordering::SeqCst)` before `control.resume()` is removed: the
declined directive is now parked and re-runs on resume by itself, and arming a
second failure would legitimately run a second reset. Removing it does NOT
mean the double reset is gone: the stub's `fire` arms ONE failure, whereas a
production schema-stale cursor fails at every poll, so the stub cannot model
"the stale cursor re-raises after resume". The edit hides that path from this
test, which is why the generation check has its own deterministic instrument
(17); with `fire` re-armed the outcome would depend on which of the poll and
the parked task reaches the listener first. The assertions stay
(nothing deleted while paused; `established(&account).len() == 4` after
resume, from the parked obligation alone; both cursors exist). Its doc's
"after resume the still-registered failing cursor raises the directive again"
becomes "the parked obligation re-runs the reset", and its ablation note gains
"without the parked reset, nothing re-raises the directive and the count stays
at 2".

Gate: `brokkr check`

### Documentation and notes, in the landing that changes them

Landing 1:

- `reference/sync.md`, the acknowledgement-fence paragraphs: fence-guarded
  registration (`register_if_fence`), mint under the lock, burnt fences that
  strictly increase, the walk fence passed into the runner, fusion
  `expected_fence` and `put_if` finalize, the checkpoint-less batch check and
  the superseded warning, `run_establish`'s `walk_fence` and where each caller
  reads it, `Error::Superseded` (including reattach skipping a superseded
  scope), the deletion boundary raising the fence and epoch before the
  registry delete (twice per deletion, deliberately), the lifecycle epoch and
  why it is not `scope_generations`, and establish-if-absent replacing the
  park's synthesized restart, with the epoch's protection stated as narrowly
  as "Establish-if-absent" does. The `DeferredScopeRepairs` paragraph changes
  accordingly.

Landing 2:

- `reference/sync.md`: the paragraph beginning "`RestartScope` and
  `DisableScope` preserve backfill state; only schema recovery deletes it",
  which names `reset_scope_for_schema_recovery` and is already stale (scope
  deletion deletes backfill too): it lists `reset_scope_for_restart`,
  `reset_scope_for_disable`, `reset_scope_for_deletion` and the account-wide
  `reset_account_for_schema_recovery`, and says which of them delete backfill;
  replace the gap paragraph ("Schema recovery currently
  enumerates only registered cursors ...") with the account reset (order 1-6,
  the fallback-scope seed, the store method, revocation before the clear, the
  ledger write after a failed clear, the parked obligation and its reset
  generation, the deferred worker's pre-walk check); the
  pause paragraph "Schema recovery follows the same discipline" (the decline
  parks, exhaustion pauses with `RetryBudgetExhausted`); "Cursor envelope",
  the attach heal paragraph (attach clears the whole account on any
  unreadable change-cursor row, and why, including corruption and downgrade
  conservatively taking the same path); "Checkpoint store": replace the stale
  six-method snippet with the current trait including `clear_account_checkpoints`, and
  rewrite the `delete_backfill` sentence; state that the required account clear
  preserves the supplied ledger, its old target ids, operator decisions and
  history, and that a reset itself proves no coverage (an incomparable re-walk
  may leave debt open); the recovery summary paragraph
  ("the reopen listener then clears every in-memory and durable cursor" becomes
  every durable row of the account); the file map entry for `store.rs` loses its
  method count.
- `reference/jmap.md` and `reference/graph.md`: the schema-clear sentences say
  every durable row of the account, scopes the engine does not track included.
- `notes/bugs-sync.md`: delete SYNC-015.
- `notes/todo.md`, new items (filed, not built):
  - "`BackfillCheckpointWriter::persist` bypasses completion refusal and every
    reset fence" - the attached route accepts any consumer-supplied
    `BackfillCheckpoint`, completion partition included, with no publication;
    the direct route has no engine at all. The account reset excludes this
    consumer assertion path.
  - "Ledger-only writer requests from pre-reset producers" - `RecordBarrier`
    and `CrossWaivedBarriers` sent by a walk that predates a scope or account
    reset are accepted; a stale barrier fails closed (it blocks the
    replacement walk and asks an operator) and needs its own policy.
    `RecordDebt` acceptance is the safe direction and stays.
  - "Schema-incompatibility evidence discarded before classification" - a
    lifecycle `Created` restart deletes a preexisting row unread;
    `run_establish` types an unreadable row as `Engine(SchemaIncompatible)`
    without routing it to the account reset, and the per-scope retry loop
    treats it as retryable. Reachable for a preexisting row of a scope absent
    from discovery at attach that returns mid-session before any sibling door
    failure.
  - "Per-scope fences do not refuse foreign publications" - the account fence
    refuses ids minted by earlier attachments; per-scope resets still accept
    a prior attachment's change receipt through the fallback.
  - "Duplicate SchemaIncompatible directives run duplicate account resets" -
    every scope whose door fails sends its own directive, and each runs a full
    clear and re-establishment. (The parked-reset double and the queued
    deferred `Resume` items are closed by this spec; the remaining cost is
    directives already queued by distinct live drives.)
  - "`ack_checkpoint` with no publication bypasses every fence" - a change
    cursor or backfill page acknowledged with `publication: None` skips the
    claim lookup and every reset fence in `persist_ack_request`; after an
    account reset such a backfill page becomes a resume position past windows
    the replacement walk never re-mints. Nothing in the engine sends one. Two
    answers to rule on: name it as a consumer assertion path (as this spec
    does), or refuse a `None`-publication backfill acknowledgement (always, by
    the reasoning `completion_refusal` already records, or once an account
    fence is raised).
  - "Conditional registration ignores `fenced_backfill`" - `register_if_fence`
    compares `scope_fence`, which reads `fenced` (and `fenced_account`), not
    `fenced_backfill`, so a backfill discard still revokes an in-flight
    backfill producer only at acknowledgement, not at registration.
    Pre-existing and probably harmless (the discard runs before the retry
    mints), but the same point-in-time question this spec closes for `fenced`.
  - "Backfill discard requests survive an account reset" - detach drains
    `take_backfill_discards()` and sends `DiscardBackfillProgress` per scope; a
    discard recorded before an account reset deletes the post-reset walk's rows
    at detach. Safe direction (costs a re-walk) and pre-existing for per-scope
    resets.
  - "Capture the lifecycle epoch where `Created` is raised" - routing the
    lifecycle `Created` as `EstablishIfAbsent { scope, epoch }` read at the
    multiplexer, instead of a synthesized `RestartScope` whose park reads the
    epoch at decline time, would let the epoch alone distinguish "deleted
    before the repair was raised" from "recreated". Changes the routing an
    existing multiplexer test pins.
  - "Engine-internal requests on the published `ReopenRequest`" -
    `ScopeDeleted`, and now `EstablishIfAbsent` and `SchemaReset`, are
    engine plumbing whose epochs and generations mean nothing to a consumer,
    on a `pub` enum re-exported from `lib.rs`. Moving them to a crate-private
    request enum removes published variants, so it needs the owner's ruling.

## Stopping rule

In scope: everything above. Out of scope, named:

- The items filed above, and the remaining duplicate-directive cost.
- Any change to protocol crates or `bifrost-types`: no producer of
  `SchemaIncompatible` changes, and no published type in `bifrost-types`
  changes shape.
- The reattach and `restart_account` handling of an unreadable row beyond the
  `Superseded` mapping.
- Durability of the account fence across attachments: a later attachment
  starts unfenced. A replayed prior-attachment backfill receipt is already
  withheld; a replayed change receipt is either schema-stale (it fails at the
  protocol door and re-raises the reset) or current (plain redelivery), so no
  migration is lost.

Published surface: `WriterRequest`, `ReopenRequest`, `FusionOutcome` and
`Error` gain variants (all `#[non_exhaustive]`); `InventoryFusion` gains a
public field; `BackfillRunner::run_partition` gains a parameter. These are
shape changes to published types, which the 2026-09-30 ruling covers.
`ReopenRequest`'s new variants are engine-internal, like `ScopeDeleted`
before them (see the filed item). Nothing published is removed or renamed.
`WriterHandle::reset_scope_for_schema_recovery`, `DeferredInventory`'s enum
shape, `fence_at_pass_start`, `run_establish` and
`re_establish_scope_with_backoff` are crate-private.

`CheckpointStore` gains a required method with no default. Its authorization,
the issue-specific scope of the 2026-09-06 ruling, and the rejected alternatives
are settled inline under "Store". Account-wide attach healing and preservation
of the ledger are settled under "Attach" and "Writer" respectively. These are
implementation decisions within the requested fix; none removes or renames a
published item. No owner decision remains open.

## Review record

Sparred with one codex session (read-only profile) over five exchanges before
writing. Conceded and folded in:

- Registry retirement must precede the final fence: live drives are revoked
  only by `publish_if_drive_generation`, so a reset that committed before the
  registry delete let an old drive mint an acceptable publication.
- Minting before taking the ledger lock let a publication escape both
  invalidation passes with its debt stranded; mint moved under the lock.
- Queued `DeferredInventory::Resume` items and running fusions crossed the
  reset unguarded, pages, `Done` and `finalize` alike; now fence-guarded, with
  establish-if-absent on invalidation.
- The decline and failure paths cannot rely on a registered failing cursor (a
  deferred-inventory trigger has none); now an explicit parked obligation that
  merges scope sets and survives a second pause.
- Attach healed an unreadable row per scope, erasing the only trigger.
- A pass-2 ledger failure was reported as success; now it fails the reset,
  and provisional ownership follows the clear's outcome.
- The backfill runner re-snapshotted the fence after the orchestrator's check;
  the walk fence is now passed in.
- A refused conditional registration dropped the observed debt; it is now
  routed as `RecordDebt`.
- The park task's outside-the-lock check could restart a healthy replacement;
  establish-if-absent checks under the lock.
- Absence includes deliberately deleted scopes; obligations carry a lifecycle
  epoch checked at execution. (Narrowed by the cold review below: for
  `ParkScope` the epoch is read at decline time and protects only against a
  deletion after the park; FIFO plus `unpark` cover the earlier order.)
- A fusion finalizing between a deletion's registry delete and its writer
  reset resurrected the scope; the deletion boundary now raises the scope fence
  first.

Not taken as proposed, with reasons:

- Fencing `BackfillCheckpointWriter::persist`: it writes caller-asserted rows
  by design and already bypasses every completion check; filed. The partner
  accepted this provided the guarantee names the exclusion.
- A durable cross-attachment fence: the partner withdrew the objection under
  the receipt semantics above.
- Writer-side ownership for `RecordBarrier` / `CrossWaivedBarriers`: fails
  closed and is pre-existing for per-scope resets; filed.

The partner's closing position: no remaining concrete interleaving for the
account reset or the deletion boundary under the folds, stated as the limit of
the review rather than a proof.

The same session then checked this document against the converged mechanism.
Folded in: the refused claim is returned by value (`Result<PublicationId,
CoverageClaim>`) and persisted by a named helper with every failure exit
specified (and the admission rule corrected: only `EstablishIfAbsent` must be
sent without held resources); the park task removes its entry only under its
own epoch; the fusion `Done` and `finalize` instruments now each exercise their
own path (the earlier page-first staging never reached `Done`, and the
`put_if` unit test passed with an unconditional `finalize`); the carry-forward
staging in the `invalidate_account` instrument abandons first; the deletion
listener arm has its own instrument, and the ordering limit is stated; the
declined-reset instrument now forces the refusal through the reopen lock; the
existing paused-schema test changes in landing 2; the pause reason choice is
explained. A further pass folded in: debt offered while the writer's ledger is
unloaded is retained (`pending_debt`) rather than dropped; the declined-reset
instrument became a direct deterministic call plus a merge unit test; the
deletion sequence moved into one helper; epoch ownership got a deterministic
`claim` test; and two verification claims were narrowed to what they prove
(the `&mut Ledger` borrow proves minting under a lock, not one critical
section; the deletion helper test proves the raise's presence, not its order),
with both orders stated as structural and `brokkr check` standing in. The
partner's last pass found no further production hole, undesigned retention
exit, or landing dependency.

### Cold reviews (r1, r2), consolidated

Two independent cold reviews of this document against `ba06c045`
(`notes/sync-account-schema-reset-spec-r1.md`,
`notes/sync-account-schema-reset-spec-r2.md`). Each finding was checked against
the source before folding. Both reviews found the same top defect; it is listed
once.

Folded in:

- **The cursorless schema trigger was stranded on the success path** (r1 #1,
  r2 #1). The deferred worker dispatches its terminated fusion with
  `Some(scope)` and the directive arm ignored it, so the reset cleared the
  scope and nobody re-established it. Now the arm seeds `extra` with the
  fallback scope; instrument L2-16.
- **`run_establish` adopted a deletion's fence** (r2 #2). Both fusion sites
  read the fence after awaited store and pacing calls, and the stored-cursor
  and `Ready` installs were unconditional. Now `walk_fence` is a parameter read
  where each caller's ownership begins (before the epoch check in
  `establish_if_absent`), and every install is `put_if`; instrument L1-8.
- **`Superseded` would have failed reattach** (r1 lateral 1, taken in scope
  because this spec introduces it). A deletion racing a reattach would have
  aborted the attempt and could exhaust the reopen budget. Reattach now skips a
  superseded scope; instrument L1-9.
- **Queued deferred items re-triggered the reset** (r1 #2). A pre-walk fence
  check skips a superseded item straight to `EstablishIfAbsent`; instrument
  L2-16's second ablation.
- **`ack_checkpoint` with `publication: None`** (r1 #3). Named as a consumer
  assertion path in the Revocation property and the exclusions, and filed with
  the refusal alternative for a ruling.
- **The cross-attachment instrument would not bite** (r1 #4). Now staged with
  the foreign ledger built after the current one (L2-13), and the
  `fenced_account` doc gives the cross-process reason.
- **"Ruled fine" overclaimed** (r1 #5). The consolidation originally put the
  required trait method to the owner. The 2026-10-25 adjudication resolves it
  inline under "Store", with the actual authorization, the prior ruling's
  limited scope, the rejected alternatives and what a store needs.
- **Landing 1 would not be green** (r1 #6). `brokkr`'s clippy phase fails on
  any rustc warning, and `raise_account_fence` / `invalidate_account` would be
  dead in a non-test build until landing 2. All `fenced_account` plumbing and
  its four instruments moved to landing 2 (L2-11 to L2-14); landing 1 gained a
  `raise_scope_fence` version of the burnt-fence instrument (L1-1).
- **A failed clear left pass-1 debt only in writer memory** (r1 #7). The arm
  now writes the ledger after a failed clear; instrument L2-15.
- **The parked reset made the double reset systematic** (r1 #8). A reset
  generation checked at execution turns a stale parked request into
  `EstablishIfAbsent` per owed scope; instrument L2-17. The paused-test edit is
  explained as hiding the path rather than closing it.
- **The listener double bump** (r1 #9): stated as intended, with why it strands
  nothing, and L1-13 no longer asserts exact epochs.
- **The `ParkScope` epoch claim** (r1 #10): narrowed to what it protects (see
  "Establish-if-absent" and the bullet above).
- **"Nothing was published after it"** (r1 #11). Checkpoint-less batches are
  now checked before forwarding, the `Invalidated` doc says what is true, and
  a refused fusion broadcasts a warning; instrument L1-6.
- **Unlisted test and reference paragraph** (r1 #12, #13): both added.
- **Instrument details** (r1 #14): the claim comparison is spelled out (L1-2),
  the per-scope `degraded` helper is named (L2-14), L1-14 is labelled a pin,
  L2-7 states `start_paused`, and L1-13's unit test names `insert_for_test`
  up front.
- **Smaller design notes** (r1 #15): why `scope_generations` is not the
  lifecycle epoch; the corrected bound of the epoch map; `pending_debt`'s
  unboundedness stated and justified; `ReopenRequest`'s internal variants
  noted and filed.
- **Owner questions** (r1 questions 1 to 3): resolved by adjudication on
  2026-10-25 inline under "Store", "Attach" and "Writer". The open-decision
  section is removed. The design is retained; landing 2 instruments 2 and 9
  additionally pin preservation of existing ledger state through the writer
  and attach, including old object ids. Proceed to the spec commit, then the
  specified landings and their gates.
- **Laterals** (r1 laterals 2 and 3): filed in `notes/todo.md` as written.

Rejected, with reasons:

- **r1 #10, "the `ParkScope` epoch adds nothing beyond `unpark`"**, in that
  strong form. The epoch also refuses a deletion that lands after the park
  task has claimed its entry and sent `EstablishIfAbsent` while the request is
  still queued: `unpark` cannot reach a request already on the channel, and the
  multiplexer bumps the epoch before it sends `ScopeDeleted`. The narrowing the
  finding asked for is folded; the "adds nothing" claim and the alternative
  (route `Created` as a direct `EstablishIfAbsent`) are not built here. The
  alternative changes routing an existing test pins and is filed.
- **r1 #15, make `ReopenRequest`'s new variants crate-private or
  `#[doc(hidden)]`.** Not done here. `ScopeDeleted` is the standing precedent
  for engine-internal plumbing on that enum, and moving them to a crate-private
  enum would remove published variants, which needs the owner's ruling. Filed.
- **r1 #3's alternative, refuse a `None`-publication backfill acknowledgement
  inside this spec.** Not built: it changes the behaviour of a published entry
  point on a consumer path the engine never takes, which makes it a lateral
  finding to rule on, not part of the ruled scope. Filed with both options.
