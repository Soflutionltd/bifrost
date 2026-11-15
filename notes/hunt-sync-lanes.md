# Hunt: sync-lanes

Scope: `crates/sync/src/backfill/`, `crates/sync/src/cursor/`,
`crates/sync/src/mutation/` (idempotency, read-back), `crates/sync/src/push/`,
`crates/sync/src/inventory_walk.rs`. Read into `engine/backfill.rs`,
`engine/ack.rs`, `engine/attach.rs`, `multiplexer/{mod,fusion,changes}.rs`,
`control.rs`, `repair.rs`, and the matching `bifrost-types` items where a
question crossed over.

This is a reading. Nothing was edited and nothing was run, so every "live"
label below is a reading of the code, not a reproduction.

The findings are split by kind first, as the standing lessons ask: defects
(live or latent) are listed on their own at the top. The three question
sections after that hold proposals. A proposal that would remove or rename a
published item is marked **(published: owner ruling)**.

---

## 0. Defects found along the way

These are bugs or near-bugs, not shape opinions. Each one is also cited later
from the structural finding that explains why it could happen.

### 0.1 Proof generations restart at zero every attachment, while ledger generations are durable (live)

`PendingCoverage::next_generation` is an `AtomicU64` that starts at `0` in
`PendingCoverage::default`. A fresh `PendingCoverage` is built per attachment
(`engine/attach.rs`, `Arc::new(PendingCoverage::new())`). But every
`LedgerEntry::generation`, `BarrierIncident::generation` and every retained
`proved` pair is written to the durable ledger (`cursor/ledger_envelope.rs`
encodes all three) and reloaded on the next attach.

Every discharge rule in `cursor/ledger.rs` compares a new generation against
the stored one:

- `record_proof` discharges only through `proof_union_covers(&self.proved,
  entry.generation, ..)`, which needs `proof_generation >= entry.generation`;
- the same call drops a barrier only when the proof generation is at least
  `barrier.generation`;
- the `proved.retain` pass then drops the new proof straight away, because it
  is older than every entry it could have helped.

So once a process restarts, a clean covering walk in the new session cannot
discharge debt (or clear a barrier) raised in an earlier session at a higher
generation. That holds until the new session's counter has climbed past the
old value, and the counter moves once per walk or repair attempt. In the
meantime `completion_permitted` stays false and the completion marker is
withheld, which costs a full re-walk on every attach for as long as the gap
lasts. A degraded scope whose debt came from a long first session can stay
stuck for many sessions. `upsert` overwrites `existing.generation =
generation` on rediscovery. That can lower an entry's generation, so the
ordering is not even monotonic within the durable record.

Root cause: the counter that orders durable evidence is minted by an in-memory
object with attachment lifetime. See A1: the generation should be minted by
the owner of the durable ledger (the writer), seeded from the loaded ledger's
maximum.

A second, smaller conflation sits on the same counter: `repair.rs`
`plan_requests` mints `RepairAttemptId(coverage.next_generation())`, so repair
attempt ids and walk proof generations share one sequence.

### 0.2 The completion marker wins resume selection by a magic number, and it can lose (latent, partly live-shaped)

The durable "backfill is done" fact is encoded as a `BackfillCheckpoint` on the
partition key `b"complete"` with `items_done = total_seen + 1`
(`engine/backfill.rs::emit_backfill_complete`). Its doc claims "every
per-partition checkpoint records its observed count (`<= total_seen`), so
`total_seen + 1` is guaranteed larger". The store selects the resume row by
greatest `items_done` (`CheckpointStore::get_backfill` contract,
`InMemoryCheckpointStore` via `backfill_checkpoint_is_later`).

That guarantee only covers rows written by the same walk. Rows survive across
walks:

- `items_done` is per partition and per walk (`run_partition` resets
  `seen_total`), while `total_seen` counts only the current walk.
- In a multi-partition `Fixed` plan, if the scope shrank between walks and a
  window now yields no page, that window's old row keeps its old count. If
  that count exceeds the new walk's total, the stale page row beats the marker.
  `backfill_complete_recorded` then reads false and the scope is fully
  re-walked on every attach, indefinitely.
- In an `OpenPages` walk that resumed at `T`, `total_seen` covers only the
  resumed part. A final empty probe that declares `Exhausted` stamps the
  marker with `items_done = 1`, and the earlier `page:F:T` row with up to
  `chunk` items beats it. The scope then re-probes on every attach.
- `TimeWindowed` keys embed absolute instants computed from
  `Timestamp::now()` at plan time (`partition_key` renders
  `time:{from}:{to}`; `backfill_plan_for` calls
  `plan(&policy, Timestamp::now(), 0)`). Every attach therefore writes a fresh
  set of keys that never overwrite the previous set, so rows accumulate in the
  consumer store without bound, and every one of them competes with the marker
  in the `items_done` race. The partitioner comment "keeping partition identity
  stable across runs" only holds for the `Timestamp::MIN` sentinel.

Today every in-tree provider reports `InventoryPartitioning::Full` (the
`bifrost-types` default, plus jmap, caldav and carddav overrides), and `Full`
has one key that each walk overwrites. So all three paths are latent. They
become live the moment any provider adopts the partitioning the engine
advertises. See A2 and D5.

### 0.3 Backfill never tells the consumer about degraded coverage; fusion does (live divergence)

`InventoryFusion::warn_degraded` broadcasts `OperatorAttentionNeeded` ("completed
with N object(s) or region(s) it could not represent") at a degraded `Done`.
`BackfillRunner::run_partition` warns only for barriers (`warn_barrier`). A
degraded page or degraded completion clears `complete`, registers the claim or
sends `RecordDebt`, and announces nothing. `reference/sync.md` states
"Degraded coverage is surfaced to the consumer as a
`Warning::OperatorAttentionNeeded`". That holds for one front end only, and the
backfill front end is the one every `Ready`-cursor cold start rides (Gmail,
JMAP Email). See D1.

### 0.4 The runner discards the terminal-summary debt write's result (live, minor)

In `run_partition`'s `Done` arm, the `WriterRequest::RecordDebt` reply is
dropped with `let _ = done_rx.await;`. A store error and a departed writer are
both silently absorbed. `inventory_walk::record_refused_debt`, which sends the
same request, logs the store error. The writer retains the report on failure,
so the loss is only of the diagnostic. It is still a fourth answer to the
writer-reply question in D7.

### 0.5 The ledger codec panics on an `AccountOperation` it has not learned (latent)

`cursor/ledger_envelope.rs::encode_operation` looks the operation up in the
positional table `OPERATIONS` and panics if it is missing. The table matches
`bifrost-types` today. But `AccountOperation` is `#[non_exhaustive]` and lives
in another crate, so nothing at compile time ties the two together. The first
new operation that reaches a persisted `last_error` or barrier `evidence`
panics the account writer. `cursor/envelope.rs` has the same shape for
`ObjectType` and `ProtocolKind` ("encoding a variant the codec does not know
panics"), and that codec runs inside consumer stores. See S7.

### 0.6 A future `Checkpoint` variant from a provider panics fence-guarded registration (latent)

`PendingCoverage::register_in` does
`lane.scope().expect("conditional registration of a checkpoint with no scope")`
whenever `expected_fence` is `Some`. Fusion's `validate_checkpoint_envelope`
admits any `Checkpoint` variant it does not recognise (`_ => Ok(())`) and then
publishes it through `publish_checkpoint_if_fence`, which maps to
`Lane::UnkeyedCheckpoint` with no scope and hits the panic. `Checkpoint` is
`#[non_exhaustive]` in `bifrost-types`. The account boundary should refuse an
unknown variant as a contract violation, as it already refuses
`Checkpoint::Backfill`.

### 0.7 `PendingCoverage::retire` drops debt and strands the boundary entry (latent foot-gun)

`retire(id)` removes only `claims[id]`. It leaves the `BoundaryEntry` charged
against the backfill bound and drops the claim's debt without `debt_only` or
carry-forward. `retire_publication` exists because exactly that
("an earlier revision deleted the claim outright") destroyed rescued
obligations. The runner's `(None, Some(pending), Some(id)) => pending.retire(id)`
arm and fusion's `retire_claim` fallback both call the unsafe one. Both are
unreachable today, because a publication id only exists when `control` is
`Some`. `retire` is published and documented as the right call for a
publication that "reached no real subscriber", which is exactly what it gets
wrong. See D9.

### 0.8 Destroy read-back treats any `NotFound` as proof of absence (live policy inconsistency)

`mutation/readback.rs::destroy_readback_saw_absence` accepts
`AccountErrorKind::NotFound(_)` of any `ResourceKind`, so a `NotFound(Mailbox)`
or `NotFound(Account)` per-item failure counts as "the destroy landed"
(`Skipped`). The repair lane takes the opposite position on the same question:
`reference/sync.md` ("a 404 may mean deleted, moved out of scope, permission
lost, wrongly routed, or not yet replicated") and `DefinitivelyIrrelevant` is a
typed account conclusion, "never a status code". See D8.

### 0.9 The "restart-safe" publication receipt has no codec and is forgeable (latent, design gap)

`PublicationId(pub u64, pub Arc<PublicationReceipt>)` and every field of
`PublicationReceipt` and `CoverageClaim` are public. The docs say a consumer
"persists the whole token beside the batch" so that an acknowledgement can
replay after a writer restart, but the engine ships no encoding for it. It does
ship one for cursors (`encode_envelope`) and ledgers (`encode_ledger`). So a
consumer has to hand-serialize an `InventoryCoverageReport`, and on replay
`claim_in`'s receipt fallback ingests whatever claim the consumer rebuilt.
`Complete` reports discharge debt. For change-cursor lanes nothing checks the
receipt against anything the engine minted (`minted_here` gates only the
backfill withhold). That reopens "no protocol crate (or consumer) may construct
ledger state", which `DebtLedger::from_parts` being crate-private exists to
enforce. See A1 and S1.

### 0.10 `emit_backfill_complete` reports `Published` when nothing was published (latent)

`if changes_tx.is_none() { return MarkerOutcome::Published; }`. The
orchestrator then settles the incarnation as concluded although no marker was
broadcast or made durable. Production always wires `changes_tx`, so this only
matters as an instance of the Option-mode problem in A6.

### 0.11 `CursorRegistry::replace_topology_preserving_cursors` resets per-scope fence counters (latent fragility)

The swap installs the staging registry's `RegistryState` wholesale
(`*current = replacement`), including its `scope_generations`. Those are
usually empty, so every scope's delete counter drops back to zero. It is safe
today only because the same call bumps `registry_generation`, which fences
every pre-swap `DriveGeneration` anyway. A counter documented as a fence should
be monotonic, not resettable by a merge. Incarnations and cancel tokens are
carried across explicitly, so this field was simply missed. See S10.

### 0.12 Stale documentation

- `mutation/idempotency.rs`: "the consumer-facing `MutationCampaignId` is the
  key into the `CheckpointStore` slot that holds it". `CheckpointStore` has no
  such slot, and nothing in the crate persists a `RunId`.
- `cursor/coverage.rs`: `pub type Publications = PendingCoverage` is labelled
  "compatibility alias while callers migrate". Both names are live in the
  crate, so the migration never ran.
- `AckRequest::auto` is documented as a field for "legacy engine-generated
  acks", which production never produces.

---

## 1. Axes that should be types

### A1. The `u64` zoo: about fifteen independent identities share one primitive

These are every bare `u64` (or `u64`-in-a-tuple) in scope that names a
different domain fact:

| Fact | Where it lives | Minted by |
|---|---|---|
| registry (topology) generation | `CursorRegistry::registry_generation`, `DriveGeneration::registry` | reattach swap |
| scope delete generation | `RegistryState::scope_generations`, `DriveGeneration::scope` | `CursorRegistry::delete` |
| scope incarnation | `RegistryState::incarnations`, `all_scope_incarnations`, every `BackfillScan` key `(CursorScope, u64)` | `put_locked` |
| lifecycle epoch | `CursorRegistry::lifecycle_epochs` | provider deletion |
| scope fence | `Ledger::fenced`, `scope_fence`, `walk_fence`, `fence_at_walk_start`, `InventoryFusion::expected_fence` | `burn_fence` (publication-id space) |
| backfill fence | `Ledger::fenced_backfill` | discard |
| account fence | `Ledger::fenced_account` | schema reset |
| publication id | `PublicationId.0` (32-bit instance segment plus counter, packed) | `mint` |
| receiver sequence | `SubscriberState::next/live`, `BoundaryEntry::readers`, `delivered_at`, `Ledger::live` | `ChangeDelivery::subscribe` |
| undelivered watermark | `undelivered_watermark`, `PublicationReceipt::walk_watermark`, `undelivered_before`, `BackfillScan::settled/baseline` values | `note_undelivered` |
| walk / proof generation | `CoverageClaim::generation`, `LedgerEntry::generation`, `BarrierIncident::generation`, `proved` | `next_generation` |
| repair attempt id | `RepairAttemptId` | **the same `next_generation` counter** |
| account (incarnation) generation | `Incarnation`, `PushHealth::generation`, `ReopenRequest::*::generation`, reconciler `hold: Option<(u64, Instant)>` | reattach |
| reattach attempt id | `WriterRequest::Reattach*::attempt` | reattach guard |
| walk totals | `total_seen`, `items_done` | runner |

Swap hazards that compile today:

- `emit_backfill_complete(.., undelivered_before: u64, fence_at_walk_start: u64)`:
  two adjacent `u64`s of different axes, beside `total_seen: u64`.
- `SyncControl::publish_walk_marker_if_fence(checkpoint, walk_watermark: u64,
  expected_fence: u64)`: adjacent again.
- `BackfillRunner::run_partition(.., generation: u64, lane, walk_fence: u64, ..)`.
- `ParkedWalk { fence_at_walk_start: u64, undelivered_before: u64 }`.
- A fence is compared against `PublicationId.0` (`claim_in`: `*watermark >
  id.0`) and handed out by `scope_fence` as a plain `u64` that callers compare
  for equality, so a fence and a publication id are interchangeable by
  construction.

Proposal: one newtype per row, each with exactly one minting site:
`TopologyGen`, `ScopeDeleteGen`, `Incarnation`, `LifecycleEpoch`, `ScopeFence`
(opaque and comparable only to `PublicationId`), `PublicationSeq` (segment plus
counter as fields, not bit-packed), `ReceiverSeq`, `UndeliveredMark`,
`ProofGeneration`, `RepairAttemptId` (its own counter), `AccountGeneration`,
`ReattachAttempt`. `BackfillScan`'s key becomes `ScopeIncarnation { scope,
incarnation }`.

The load-bearing one is `ProofGeneration`. It must be minted by the account's
writer, which owns the durable ledger, from `max(generation)` in the loaded
ledger. Minting it there fixes 0.1 rather than merely typing it.
`PendingCoverage` should not mint anything that is persisted.

The same applies to `PublicationReceipt`. Make it opaque, give it a codec next
to `encode_envelope` / `encode_ledger` (and an integrity tag if consumer
storage is not trusted), and make construction crate-private
**(published: owner ruling, shape change of pub fields)**. That closes 0.9.

### A2. "Backfill is complete" is a magic partition string plus a magic count

Three encodings of one fact:

1. The partition key bytes `b"complete"` (`completion_partition`,
   `is_completion_partition`). Read by the orchestrator
   (`backfill_complete_recorded`), by the ack writer (`persist_ack_request`)
   and, by literal byte comparison, throughout `tests/` (more than twenty
   copies of `checkpoint.partition.0 == b"complete"`). A consumer has no typed
   way to ask the question either.
2. `items_done = total_seen + 1`, so the store's selection picks it (0.2).
3. `PublicationReceipt::walk_watermark.is_some()`, which is set only by
   `register_walk_marker` and read in `completion_refusal`.

Partition keys themselves are stringly: `partition_key` formats
`full | time:F:T | uid:F:T | page:F:T` with a catch-all `"unknown"` arm, so
every future `InventoryPartition` variant collides on one key, and
`parse_page_partition` parses it back.

Proposal: a typed durable key, `enum BackfillKey { Partition(PartitionKey),
Complete }`, with `PartitionKey` an enum carrying the bounds and its own codec
in `cursor/envelope.rs`. The completion row becomes its own row kind rather
than a winner of a numeric race. `CheckpointStore` gains
`get_backfill_resume -> BackfillResume { None, Complete, At(PartitionKey) }`,
or the engine selects among rows it reads itself (D5)
**(published: owner ruling, trait change)**. `BackfillProgress::items_done`
goes back to meaning a count.

### A3. `BackfillPartitionOutcome` is a product of three flags with illegal combinations

`{ complete: bool, scope_walk: ScopeWalkStep, end: Option<PartitionEnd> }`
(plus counts). `StopScopeWalk` with `end: Some(Exhausted)` is meaningless,
`complete: true` with `end: None` is incoherent, and the `Default` exists only
to pick "safe" values for fields a construction site might forget.
`ScopeWalkDriver::fold` re-derives the real state from the three.

Proposal: `enum PartitionVerdict { Exhausted { degraded }, MoreBeyond {
degraded }, EndedUnannounced, StoppedAtBarrier }` beside the counts.
`ScopeWalkDriver::fold` becomes a match. `BackfillScopeWalk` (one `bool`)
disappears into the driver (D11).

### A4. `RegisteredSubscription` is a state machine encoded as three bools

`teardown_unconfirmed`, `desired`, `torn_down` give eight combinations. The
field docs list the four legal ones ("always `desired`, `teardown_unconfirmed`,
or both"; "`torn_down` implies `desired` and not `teardown_unconfirmed`"), and
every mutator (`into_orphan`, `mark_unconfirmed`, `mark_torn_down`,
`remove_orphan`, `orphans`, `take`) re-establishes them by hand.
`mark_unconfirmed` does not clear `torn_down`, so it can produce the forbidden
`torn_down && teardown_unconfirmed` if it is ever called on such a record.

Proposal: `enum SubscriptionState { Live, LiveTeardownFailed,
TornDownAwaitingRecreate, Orphan }`, with transitions as methods.

### A5. Push lane verdicts and intent travel as tuples and paired flags

- `PushLanes::failure() -> (Duration, bool, bool)` is (delay, terminal,
  restart). A named `LaneDisposition` (see D3) fits.
- `PendingPush { full, scopes, memberships, closed }` and
  `WorkSet { full, scopes, hold }` both encode "full reconcile" as a bool
  beside a set that must be empty when it is true. Every site calls
  `clear()` to keep that so. `enum Intent { Full, Some { scopes, memberships } }`
  makes the illegal state unconstructable.
- `WorkSet::hold: Option<(u64, Instant)>` is a bare tuple of account generation
  and deadline.

### A6. `Option` parameters as hidden mode switches that change safety semantics

`BackfillRunner::run_partition` takes `changes_tx: Option<broadcast::Sender>`,
`control: Option<&SyncControl>`, `coverage: Option<&Arc<PendingCoverage>>`,
`writer_tx: Option<&mpsc::Sender<WriterRequest>>`, `lane: Option<&LaneGate>`
and a non-optional `delivery`. Thirteen parameters in all. Which ones are
`Some` silently selects behaviour:

- `lane.is_some()` picks fence-conditional registration
  (`publish_checkpoint_if_fence`) over unconditional (`publish_checkpoint`).
  So the reset-fence guarantee depends on whether a flow-control gate was
  passed.
- `lane.is_some()` also arms the shutdown select. A `lane: None` caller "has
  no token, so the guarantee is about engine-driven partition walks".
- `(control, coverage, publication)` chooses which retirement function runs
  (0.7).
- `writer_tx: None` makes `record_barriers` succeed and
  `cross_waived_barriers` answer "not waived".
- `changes_tx.is_none()` publishes nothing and skips the removal projection.

`InventoryFusion` repeats the pattern with `control`, `coverage`, `writer_tx`
and `shutdown`, including a `fence_moved` point-in-time branch that exists
only for the `control: None` case.

Production passes `Some` for all of them. The `None` combinations are a test
convenience that the safety logic must stay correct under. Proposal: one
`WalkContext` / `PagePublisher` value (S2) built once per walk in production,
plus a test double implementing the same trait, so the safety decisions have
one form.

### A7. A departed writer has no typed answer

See D7 for the decision. The axis fix is `WriterHandle` everywhere, returning
`Result<T, WriterError>` with `WriterError::Gone` distinct from
`WriterError::Store(Error)`, so each call site states which of the two it
tolerates instead of choosing between `Ok(())`, `Ok(false)`, `Err(ShuttingDown)`
and `Err(Other("writer channel closed"))`.

### A8. Persisted errors are fabricated `AccountError`s

`LedgerEntry::last_error` and `BarrierIncident::evidence` are `AccountError`,
but the codec can only persist a digest. On decode, `canonical_cause`
synthesizes a cause the original never had: every `StrategyFailure` comes back
as `QResyncToCondstore`, every `RateLimited` with `retry_hint: None`, every
`InsufficientScope` with `needed: "unknown"`, and `CursorInvalid` with no
cursor scope gets `CursorScope::Account` invented for it. The restored value
then answers `recovery()`, `message_key()`, `retry_hint()` from causes nobody
observed. The reference says no ledger predicate reads these, but
`SyncEngine::debt` hands the entries to consumers, and nothing stops a
consumer branching on `last_error.recovery()`.

Proposal: `ErrorDigest { kind, scope, operation, provider, protocol,
diagnostics }` as the field type, with no `recovery()`. It is what the record
actually holds, and it removes `canonical_cause` (D10)
**(published: owner ruling, field type change)**.

### A9. `PolicyStatus::Retrying { attempts }` is a mirrored view, and barriers reuse a policy that means nothing for them

`attempts` on an entry is documented as "a VIEW", rewritten from the lineage
table by `settle_lineages` after every mutation and cross-checked on decode by
`validate_lineages` ("an open `Retrying` entry disagreeing with its row is
`Error::Other`"). That is two copies held in step by a pairwise-agreement
check, the exact shape the hunt brief names. `BarrierIncident::policy` is
minted as `Retrying { attempts: 0 }` (`inventory_walk::barrier_incidents`) and
"is inert".

Proposal: `EntryPolicy { Automatic, OperatorBlocked, Waived { .. } }`, with the
attempt count read through `DebtLedger::lineage_budget(entry)`. Add a separate
`BarrierPolicy { Undecided, OperatorBlocked, Waived { .. } }`. `Waived { by:
String, at_unix_seconds: i64 }` should carry an `OperatorId` and a timestamp
type, not a free string and a raw `i64`.

### A10. Operator-facing failures reach the consumer only as prose

Every attention signal in scope is `WarningKind::OperatorAttentionNeeded` (or
`WarningKind::Other`) plus a `format!` string. That covers barrier stop with
a count (runner and fusion, two copies of the text), degraded inventory with a
count, "inventory walk superseded by a reset or deletion" (`Other`), "push
transport disconnected" (`Other`, two sites), "push stream terminated with
retryable recovery" (`Other`), and, in the sibling scope, scope disabled,
retry budget exhausted and schema clear failed. A consumer that wants to
render "operator must waive N regions in scope S" as distinct from "push is
down" has to match on English.

`BackfillState { Pending, Running, Completed }` likewise folds "never
started", "failed and retrying on the ramp", "parked on a throttle", "blocked
by an operator-blocked barrier" and "walk lost pages, restarting" into
`Pending`.

Proposal: a typed attention payload (`AttentionReason::BarrierStop { regions }`,
`DegradedCoverage { obligations }`, `WalkSuperseded`, `PushDown { lane }`, and
so on) carried on the warning, and a richer `BackfillState`.

### A11. The protocol axis has two enums and three sources of truth

- The consumer passes `protocol: ProtocolKind` into every `bulk_*` campaign
  entry, and it is used only to pick a `ProtocolSalt` that "never travels on
  the wire" (`default_salt_factory`).
- `Account::protocol()` exists on the trait and returns `crate::Protocol`,
  defaulting to `Unknown`.
- `recovery::batch_boundary_violation` reads the protocol off the offending
  checkpoint's `OpaqueChangeState::protocol` tag.

`ProtocolKind { Jmap, Imap, Gmail, Graph, CardDav, CalDav }` and
`Protocol { .., Smtp, Lmtp, Ews, Unknown }` describe overlapping axes, and
each has its own codec (`envelope.rs::encode_protocol` and
`ledger_envelope.rs::encode_protocol`). Nothing checks that the consumer's
`ProtocolKind` matches the account.

Proposal: derive the salt from the account, which already knows its protocol,
and drop the parameter. Decide in `bifrost-types` whether `ProtocolKind` is a
projection of `Protocol`, and give the mapping one owner
**(published: owner ruling)**.

### A12. Smaller instances

- `PartitionBounds::Time { from: Timestamp::MIN, .. }` uses a sentinel value
  for "open" where `InventoryPartition::Time` uses `Option`.
  `PartitionBounds` is a near-copy of `InventoryPartition` whose only job is
  to be converted into it (`inventory_partition_for`). See S8.
- `mutation/readback.rs` turns item identity into object identity by
  `ObjectId(success.item.0)` / `ObjectId(f.item.0)`, which means
  "`BatchItemId` is an `ObjectId`" is asserted by string coercion at four
  sites. A typed accessor on the hydration outcome (or a
  `From<BatchItemId>` owned by types, if that equivalence is the contract)
  would state it once.
- `IdempotencyVendor::salt_for: Box<dyn Fn(ProtocolKind) -> ProtocolSalt>`
  configures something with no reader (A11).

---

## 2. Decisions made in more than one place

### D1. "What does an inventory front end do with one page?" has three to five implementations

The question covers: project entries to `Created` ids, project removals, decide
whether the page crosses a waived barrier, decide whether it stops the walk,
record barriers, warn, judge the fence, and publish. It is answered by:

- `BackfillRunner::run_partition`, twice internally: the barrier-page branch
  and the normal branch each build the `kept -> Change::ObjectChange { Created }`
  chain and the synthetic `Batch` separately;
- `InventoryFusion::run_stream` plus `forward_inventory_batch`;
- `repair.rs::publish_recovered`, which down-converts recovered entries to
  `Created` ids and publishes per scope;
- the crossed-or-stop pattern (`let crossed = if coverage.has_barrier() {
  cross_waived_barriers(..).await? } else { false }; if !crossed && let
  StopAtBarrier = walk.inspect(..)`), written four times: runner batch, runner
  `Done`, fusion batch, fusion `Done`.

They already disagree:

- degraded coverage is announced by fusion and never by backfill (0.3);
- fusion judges the fence before forwarding a checkpoint-less barrier page
  (`forward_inventory_batch` checks `fence_moved` when nothing registers),
  while the runner's barrier page goes out on the raw sender unfenced;
- the boundary-violation outcome is `Err(Error::Account)` in the runner but
  `Ok(FusionOutcome::Terminated)` in fusion;
- the barrier warning text is duplicated verbatim between `warn_barrier` and
  `warn_degraded`;
- a failed barrier write is logged in fusion (`InventoryFusion::record_barriers`
  wraps with `tracing::error!`) and silent in the runner.

`inventory_walk.rs` records an explicit trigger for unifying the minting sites:
"a THIRD inventory front end appears". Repair is that third front end in
everything but checkpoint minting. The trigger has fired.

Proposal: `InventoryWalk` grows `async fn judge(&mut self, coverage, writer) ->
PageVerdict { Advance, CrossedWaived, Stop { resume_from } }`, which owns the
crossing, the stop, the barrier recording and the degraded flag. Add one
`PagePublisher` (S2) that owns entry projection, removal projection, the
fence-before-forward rule and the warnings. The only thing left per front end
is where its checkpoint comes from (minted positional vs account-supplied),
which is the genuine difference the module doc names.

### D2. "Which numbered receivers are live?" is held twice, kept in step by call discipline

`ChangeDelivery::SubscriberState::live` and `cursor::coverage::Ledger::live`
are the same set. The ledger copy is "a mirror of `ChangeDelivery`'s own
`live` set", maintained by `receiver_joined` / `receiver_departed` calls made
under the delivery lock. In one departure, `ChangeDelivery::depart` computes
`min_live` from its own copy and passes it to
`PendingCoverage::release_undelivered`, while `BoundaryEntry::page_unread` and
`backfill_charge_holders` read the ledger's copy. `ChangeDelivery::subscribe`
takes `coverage: Option<Arc<PendingCoverage>>`, so a numbered receiver created
with `None` is live in one set and absent from the other. Production always
passes `Some`, so they agree today only because every call site does.

Proposal: one owner. Either `ChangeDelivery` owns the ledger (it already holds
the lock that orders sends, stamps and departures), or `PendingCoverage` owns
receiver numbering and `ChangeDelivery` asks it. In both cases the
`Option<coverage>` on `subscribe` goes away.

### D3. "What does a drive do with a `RecoveryPlan`?" is decided per driver

These sites each map `plan_recovery(error)` to throttle recording, delay,
directive forwarding and parking independently:

- `multiplexer::handle_drive_outcome` (poll);
- `push::reconciler::Reconciler::drive_scopes` (push), whose comment says it
  does this "exactly as the poll loop's `handle_drive_outcome` does";
- `push::lanes::PushLanes::failure`;
- `push::route_watch_event` (sink);
- the backfill orchestrator and the mutation campaign in the sibling scope.

Some differences are intended, such as poll sleeping inline while the
reconciler defers. Others are not visibly intended. The account-wide
`Engine(_)` verdict makes the reconciler hold the remainder for 30 s, while
the poll loop returns with no exit and carries on at cadence. The
wait-for-the-restart bound itself is written three times:
`LIFECYCLE_REOPEN_WAIT` (a named constant in the multiplexer), a literal
`Duration::from_secs(30)` in the reconciler hold, and a literal in
`drive_lane`'s restart wait. `retry_delay` / `reconcile_delay` floors are also
literal per site (`Duration::from_secs(1)` in poll and reconciler,
`Duration::ZERO` in lanes).

Proposal: `recovery.rs` returns a `Disposition { record: ThrottleRecord,
wait: Wait, directive: Option<ReopenRequest>, park: bool, announce:
Option<Warning> }` from one function that takes a `DriverKind` where the
answer legitimately differs. Each driver applies it. The 30 s bound becomes one
named constant.

### D4. The cursor-scope to membership relation is decided in five places, with two answers on folder-vs-mailbox identity

- `engine/attach.rs::scope_covers_membership`: `Folder(f)` covers
  `Mailbox(m)` when the strings match ("we treat the FolderId / MailboxId
  String the same");
- `multiplexer::membership_scopes`: `Mailbox(m)` maps to `Folder(FolderId(m.0))`
  shapes, the same string identity in the other direction;
- `inventory_walk::removals`: a folder scope's removal projects only to
  `MembershipScope::Folder`, never to `Mailbox`, and an unknown `CursorScope`
  is an `Internal(AccountContract)` error;
- `mutation/readback.rs` move check: `entry.memberships.contains(destination)`
  is strict enum equality, so a `Folder("x")` destination never matches an
  observed `Mailbox("x")`;
- `cursor::ScopeShapes::record` and `folder_cursor_shapes`: which shapes exist.

All of them carry a `_ =>` arm because `CursorScope` and `MembershipScope` are
`#[non_exhaustive]` across the crate boundary. A new variant becomes
`false` / ignored / an error depending on the site.

Proposal: one `cursor/topology.rs` owning `covers(scope, membership)`,
`removal_projection(scope)`, `scopes_for(shapes, membership)` and
`membership_eq(a, b)` (the folder/mailbox identity rule, stated once). If the
folder/mailbox equivalence is a protocol fact rather than engine policy, it
belongs in `bifrost-types` next to the enums, where the matches can be
exhaustive.

### D5. "Which backfill row is the resume position?" is split between the engine and every consumer store

- `CheckpointStore::get_backfill`'s doc contract: greatest `items_done`, then
  greatest parsed page upper bound, then greatest raw bytes. Every downstream
  backend has to reimplement this, including parsing the engine's
  `page:F:T` key format. `InMemoryCheckpointStore` borrows the engine's
  `partitioner::parse_page_partition`; an external backend has to copy it.
- `emit_backfill_complete` stamps `items_done = total + 1` so that rule picks
  the marker (0.2).
- `engine/backfill.rs::open_pages_resume` re-parses the selected row.

The selection is engine policy implemented in consumer code, so a backend that
breaks ties differently changes resume behaviour. Proposal: the store returns
rows, or a typed resume record, and the engine selects (A2). Then no consumer
ever parses a partition key.

### D6. "Is this checkpoint the completion marker?" has three answers

The partition sentinel, the receipt's `walk_watermark`, and the `items_done`
magic (A2). `completion_refusal` treats the second as required for the first
(`None => Unvouchable`), which is right but is a cross-check between two
encodings of one fact. Folded by A2.

### D7. "The writer is gone" means four different things

| Site | Writer gone means |
|---|---|
| `inventory_walk::record_barriers` | `Ok(())`: "not a persistence failure" |
| `inventory_walk::cross_waived_barriers` | `Ok(false)`: not waived, so the walk stops |
| `inventory_walk::record_refused_debt` | `Err(Error::ShuttingDown)` |
| runner `Done` `RecordDebt` | ignored (0.4) |
| `engine/backfill.rs::scope_barrier_blocked` | `false`: not blocked, so the walk proceeds |
| `engine/backfill.rs::discard_backfill_progress` | `false`: request stays outstanding |
| `WriterHandle::get_change_cursor`, `persist_established`, `reset_scope`, `reattach_insert` | `Err(Error::Other("writer channel closed"))` |
| `WriterHandle::reset_account_for_schema_recovery` | `Err(Error::ShuttingDown)` |
| `BackfillCheckpointWriter::persist` (Writer route) | `Err(Error::Other(..))` |
| `repair.rs` `ApplyRepair` | `Err(Error::Other(..))` |

Some divergence is principled (a barrier with nowhere to persist is not a
failure). But `scope_barrier_blocked` answering "not blocked" on a gone writer
while `cross_waived_barriers` answers "not waived" are opposite fail-open and
fail-closed choices on the same ledger, and `Error::Other` versus
`Error::ShuttingDown` changes which recovery arm the caller takes. The lanes
hold raw `mpsc::Sender<WriterRequest>`s and hand-roll the oneshot dance at
each site; only recovery code gets `WriterHandle`. Proposal: A7 plus S6.

### D8. "Is this object proven gone?" has opposite answers in mutation and repair

Covered in 0.8. One owner would be a `bifrost-types` predicate, such as
`AccountError::proves_absence_of(kind)` or a typed `ItemOutcome` absence
variant, that both read-back and repair consult. Moving the decision to the
producer, which knows whether its 404 means deleted, matches how repair
already works.

### D9. "Retire a publication" has five public doors with different semantics

| Method | boundary entry | claim | debt carried | wake |
|---|---|---|---|---|
| `retire_publication` | removed (incl. subsumed) | removed | yes | yes |
| `abandon([id])` | removed (incl. subsumed) | removed | yes | yes |
| `retire` | **kept** | removed | **no** | no |
| `acknowledge_checkpoint(&cp)` | removed by VALUE | **kept** | n/a | yes |
| `acknowledge_publication` | removed | kept (consumed later by `claim_*`) | n/a | yes |
| `supersede(a, b)` | **untouched** | folded | n/a | no |

`retire_publication` and `abandon` are the same operation twice. `retire`,
`acknowledge_checkpoint` and `supersede` each update half a publication, and
the module's own comments call two of them foot-guns. `SyncControl`
additionally wraps `retire_publication` with `publish_quiescence`, which the
`PendingCoverage`-level call does not do, so a caller holding only the ledger
retires without waking boundary waiters.

Proposal: one `retire(id, why: RetireReason)` transition on the ledger that
always handles both halves, debt and wake, with quiescence publication owned
by the same object (S1). The half-operations go crate-private
**(published: owner ruling)**.

### D10. Error-cause legality is restated by the ledger codec

`ledger_envelope.rs::canonical_cause` restates `bifrost-types`'
`kind_matches_cause` (the `try_build` invariant) as its own table, and the
`CursorInvalid` scope patch restates another builder rule. The only thing
keeping them in step is `try_build` failing at decode time with
"restored error failed classification". That surfaces as a ledger decode
error, which the ack writer treats as an unreadable ledger. Removed by A8.

### D11. Backfill completion state is tracked four times in memory

- `BackfillScopeWalk::stopped` (one bool);
- `ScopeWalkDriver::{exhausted, completed}`, which holds a
  `BackfillScopeWalk` and also re-derives stop;
- `BackfillScan::settled` (per incarnation, with a watermark);
- `BackfillRegistry` (engine-wide, keyed by `(AccountId, CursorScope)` without
  incarnation, marked from the same answer, published through
  `SyncEngine::backfill_registry`).

The registry and the scan agree because the orchestrator writes both. The
registry has no incarnation, so a re-established scope reads the previous
incarnation's state until the orchestrator overwrites it. Proposal: the
registry becomes a read-only projection of `BackfillScan` (S4), keyed by
incarnation, rather than a second record.

### D12. Hint resolution is written twice

`push::reconciler::scopes_for_hint` is published and has no production caller.
`WorkSet::merge` does its own resolution (`scopes_for_membership`, then
`all_scopes` at sweep). The coarsening rule ("more than `capacity` distinct
items becomes full, count one coarsening") is implemented in both
`PushIngress::invalidate` and `WorkSet::merge`. Proposal: `WorkSet::merge`
calls one resolver, and the coarsening rule lives on the `Intent` type (A5).

---

## 3. Structure

### S1. `PendingCoverage` is five subsystems behind one lock and one public surface

It holds, all in `Ledger` or beside it:

1. the publication ledger: claims, lanes, persisted and folded watermarks,
   carry-forward;
2. backfill flow control: boundary entries as ring charge, `readers`,
   `delivered_at`, `subsumed`, the `live` mirror, the capacity `Notify`;
3. delivery-loss accounting: the per-scope `undelivered` watermark,
   `discard_requests`, `tearing_down`, `last_backfill_mint`;
4. fencing: `fenced`, `fenced_backfill`, `fenced_account`, `burn_fence`;
5. minting: publication ids with instance segments, and walk generations
   (0.1).

The single lock is justified, because the transitions really do span 1, 2 and
4 (`register_in`, `invalidate_in`, `release_undelivered`). The single type is
not. It has about 45 `pub` methods, most of which must only be called under an
ordering the delivery gate or the writer enforces (`mark_delivered` "under the
delivery lock", `begin_teardown` "through `ChangeDelivery::begin_teardown`",
`receiver_joined`), and some of which are half-operations (D9). A consumer
holding `Arc<PendingCoverage>` can break every invariant the engine relies on.

Proposal: keep one mutex, but give the guarded state sub-structs (`Claims`,
`RingCharge`, `Fences`, `LossLedger`) whose methods are the named transitions,
and expose a small facade: `register_page`, `register_marker`, `acknowledge`,
`retire`, `invalidate_scope`, `on_receiver_read`, `on_receiver_departed`.
Minting of persisted generations moves to the writer (A1). Demote the rest to
`pub(crate)` **(published: owner ruling)**. The type alias `Publications`
should pick one name.

### S2. Publishing one batch involves four handles, and the producer picks among them

A backfill page is published by threading `changes_tx` (raw broadcast sender),
`delivery: &ChangeDelivery`, `control: &SyncControl` (which wraps the
publication ledger) and `coverage: &Arc<PendingCoverage>` (the same ledger
again) into `run_partition`. Fusion does the same with
`Option<Arc<ChangeDelivery>>`, `control` and `coverage`. Each call site then
decides:

- raw sender versus gate (`publish_backfill` / `publish_acknowledgeable` /
  `sender().send`): "events nobody acknowledges send on the raw sender" is a
  convention, and the runner's barrier page and both front ends' terminations
  follow it by hand;
- through control or through coverage ("Through control when there is one");
- fenced registration or not (A6).

Proposal: one per-account `Publisher`, owning the broadcast sender privately
together with the ledger and the delivery gate, offering
`publish_page(page, claim, fence) -> PageOutcome`,
`publish_marker(..)`, `publish_unacked(event)` and `announce(attention)`.
Producers get a `Publisher` and nothing else. The raw `sender()` stops being
reachable from lanes, so "did this reach a numbered receiver, and if not,
retire and record" is decided inside `publish_page` once instead of at three
call sites.

### S3. The backfill lane is split across `backfill/` and `engine/backfill.rs` along the wrong seam

`backfill/` holds the partitioner, the runner, `ScopeWalkDriver`, the
checkpoint writer and the registry. `engine/backfill.rs` (about 1500 lines)
holds the orchestrator loop and also several pure decisions about the backfill
domain:

- `backfill_plan_for` (`InventoryPartitioning` to plan);
- `BackfillPlan::resume`, `open_pages_resume`, `backfill_complete_recorded`;
- `emit_backfill_complete` (marker construction);
- `BackfillScan` (the per-incarnation state machine);
- `MarkerOutcome`, `ScopeResume`, `WalkLabels`.

The partition key format lives in `backfill/partitioner.rs`, but its two
readers (`open_pages_resume` and the ack writer) live in `engine/`. The test
layout mirrors the accident: `backfill/parking_tests.rs` tests the engine
orchestrator's `SlotContext` and is mounted from `engine/backfill.rs` with
`#[path = "../backfill/parking_tests.rs"]`.

Proposal: `backfill/plan.rs` (partitioning to plan plus resume, pure),
`backfill/marker.rs` (marker construction plus the typed key from A2),
`backfill/scan.rs` (`BackfillScan`), with `engine/backfill.rs` keeping only
the async loop, admission and wiring. The tests move with what they test.

### S4. `BackfillScan` is one state machine spread across seven parallel maps

`settled`, `failed`, `baseline`, `lost_pages`, `held`, `parked` (all keyed
`(CursorScope, u64)`) and `fusion_skipped` (keyed by scope). `select` spends
its first block pruning six of them in step ("forget everything about
incarnations the registry no longer carries"), and `settled` is deliberately
never pruned, so it grows by one entry per incarnation for the life of the
attachment.

Proposal: `HashMap<ScopeIncarnation, IncarnationState>` with `enum
IncarnationState { Fresh, Running { baseline }, Parked(ParkedWalk), Held {
until }, Failed { count, until, baseline }, Settled { watermark } }` and
`lost_pages` as a flag on the non-settled states. Pruning becomes one
`retain`, exclusivity is structural, and `BackfillRegistry` becomes a
projection of it (D11).

### S5. Engine-wide registries keyed by `AccountId` that are really per-slot state

`SubscriptionRegistry` is a per-engine `DashMap<AccountId, Vec<..>>`, and every
method takes the account id plus that slot's `shutdown` token. The whole
"seal" mechanism (`record`, `restore_orphans`, `replace` reading the token
inside the shard lock), together with its detach-ordering argument and its
two integration tests, exists because a record written after detach would land
under an id that "a later attach of the same `AccountId` inherits".
`BackfillRegistry` has the same shape (`forget_account` on detach).

If the slot owned its subscriptions, a late write could only reach the dead
slot's object, and the next attach gets a fresh one by construction. The
token-in-shard-lock reasoning, and the class of bug it guards, would disappear
rather than being defended. `InvalidationSinkInner`'s map is legitimately
engine-wide (the consumer addresses it by id) and is not part of this.

### S6. The writer protocol lives in the multiplexer, and its typed handle is reserved for recovery

`WriterRequest` (fifteen variants, with the provisional-row and
publication-identity semantics documented on them) is defined in
`multiplexer/changes.rs`. The writer that serves it is `engine/ack.rs`.
`WriterHandle`, the typed wrapper, is `pub(crate)` in `engine/ack.rs` with
`pub(super)` methods, so the lanes in this scope (`inventory_walk`, runner,
fusion, `engine/backfill.rs`, `repair.rs`, `BackfillCheckpointWriter`) all
build oneshots by hand against the raw sender (D7). Proposal: a `writer`
module, plausibly `cursor/writer.rs` since the writer is the custodian of
the cursor store and the ledger, owning `WriterRequest`, `WriterHandle` and the
reply typing. Every lane takes `WriterHandle`.

### S7. Codecs for `bifrost-types` vocabulary live in `bifrost-sync`, where their matches cannot be exhaustive

`ledger_envelope.rs` encodes `AccountErrorKind`, `ErrorScope`, `ResourceKind`,
`AccountOperation` (by positional table), `Provider`, `Protocol`, `Cause`
reconstruction and diagnostics. `envelope.rs` encodes `CursorScope`,
`ObjectType` and `ProtocolKind`. Every one of these enums is
`#[non_exhaustive]` from sync's side, so each codec carries a wildcard arm that
panics on encode or errors on decode (0.5). The ledger version history in
`reference/sync.md` shows the coupling: versions 5, 6 and 7 were bumped
because `bifrost-types` grew an `ErrorScope` or `ResourceKind` variant.

The dependency edge points the wrong way for change safety. The crate that
adds a variant cannot see the codec that must learn it. Proposal: move the tag
codecs for types-owned vocabulary into `bifrost-types` (a `wire` module with
exhaustive in-crate matches), versioned there. The ledger codec then composes
them, as it already composes `encode_envelope` for `resume_from`. Adding a
variant becomes a compile error in the right crate.

### S8. A partitioning framework with no producer imposes its costs on every consumer

`BackfillPolicy`, `BackfillStrategy`, `PartitionPlan`, `PartitionBounds`,
`plan_time` / `plan_uid` / `plan_page`, `OpenPages`, `parse_page_partition`,
the page tie-break in `get_backfill`, and `open_pages_resume` all serve
`TimeWindowed`, `UidRange` and `PageCount`. No in-tree provider returns any of
them; every scope is `Full`. `BackfillPolicy` is documented as "configurable
per account" but is never configured: `backfill_plan_for` builds
`BackfillPolicy::default()` or a literal with `clock_skew: Duration::ZERO`.
`BackfillStrategy` duplicates `InventoryPartitioning` from `bifrost-types`, and
`PartitionBounds` duplicates `InventoryPartition`.

This is not a deletion recommendation. These items are published, and
"nothing calls it" is not evidence for a library. The structural point is
narrower:

- the translation chain `InventoryPartitioning -> BackfillPolicy -> plan ->
  PartitionBounds -> InventoryPartition` has two redundant middle layers.
  Collapsing it to `InventoryPartitioning -> Vec<InventoryPartition>` (or
  `OpenPages`) in `backfill/plan.rs` keeps every behaviour
  **(published: owner ruling for the middle types)**;
- the durable cost of this unexercised machinery, the selection rule every
  consumer store must implement (D5) and the drifting time keys (0.2), is paid
  now by every backend, for producers that do not exist yet. A2 and D5 move
  that cost back into the engine.

### S9. "Lane" means five different things

- `cursor::coverage::Lane` (publication supersession and acknowledgement
  lane);
- `engine::lane::LaneGate` (backfill bound plus admission);
- `scheduler::lanes` (Foreground, Normal, Background, Bulk);
- `push::lanes::PushLanes` (push session supervision);
- `bifrost_types::PushLane` / `PushLaneKind`.

The module docs repeatedly disambiguate ("the backfill lane", "the lane +
scope"). Renaming the internal ones, for example `PublicationStream` for
coverage's `Lane` and `BackfillBound` for `LaneGate`, costs nothing
published.

### S10. Per-scope drive state is split between the registry and the multiplexer

`CursorRegistry` owns cursor, incarnation, scope delete generation, liveness
token and drive lease. The multiplexer's `ScopeTokens` owns poll-task
ownership and the terminal-park tombstone (`ScopeToken::parked`), which the
reconciler reaches through free functions (`retire_stale_scope_token`,
`scope_is_parked`, `park_scope_for_drive`). "Is this scope parked?" is a
property of a scope incarnation under a `DriveGeneration`, and the
reconciler, a second producer, needs it as much as the poll loop does.

Proposal: the parked verdict lives in `RegistryState`, bound to the scope's
`DriveGeneration` (it already is, logically: a fence change revives it), and
`ScopeTokens` shrinks to poll-task bookkeeping. `RegistryState` replacement
(0.11) should then merge per-scope state explicitly field by field, rather
than adopting the staging copy wholesale.

### S11. The mutation directory is a satellite of the pipeline it names

`mutation/` holds `MutationCounters`, `fanout`, `PartialCommitObservation`,
the vendor and read-back. The campaign state machine (cohorts, scheduler,
reassertion, read-back routing) is `engine/bulk.rs` at about 2000 lines.
Read-back's `ReadbackReport` / `ObjectReadback` are `pub(crate)` precisely so
`engine/bulk.rs` can consume them. Proposal: the campaign state machine moves
into `mutation/campaign.rs` (pure state plus transitions), with
`engine/bulk.rs` keeping the async driving. `MutationCounters`' public fields
next to `record_*` methods allow both direct writes and method writes; pick
one.

---

## Lateral notes

- `BoundaryEntry::subsumed` is unbounded per partition under the receipt
  bound, and for `Full` plans that means the whole scope. It is documented and
  filed, but it is the main memory hazard in the ledger, and it interacts with
  0.9: each retained id holds a full `PublicationReceipt` and claim.
- `BackfillScan::settled` grows one entry per incarnation per attachment, by
  design (S4 bounds it).
- `PublicationId` has no account identity, so `minted_here` cannot tell "a
  prior attachment of this account" from "another account in this process".
  The ack writer documents this as accepted imprecision. A typed
  `PublicationSeq { account, segment, counter }` (A1) makes it answerable at
  no cost.
- `LiveSupersedes` is filtered on every page through a `Mutex` even though
  nothing populates it. `filter_inventory_supersedes` short-circuits on
  `is_empty()`, which takes the lock once per page. This is harmless and noted
  only because it is the hot path.
- `ScopeWalkDriver::with_source` starts `completed: true`, so a `Fixed` plan
  with zero partitions (UID or page `total == 0`) settles and writes a
  completion marker without any inventory call. That is right only if the
  provider's `max_uid` / `total` is authoritative. A stale or zero hint
  becomes a durable "done".
