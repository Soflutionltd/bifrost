# Bugs - design hunt, November 2026

Defects the design-hunt hunters turned up on the way. They were not what the
hunt asked for; they are the findings a resolution loop can land first. Each
entry is the hunters' account, unverified; the fix pass verifies and may find
an entry wrong.

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

Each entry names the hunt scope(s) that reported it. The raw reports are in the
commit that precedes this file's.

## BUG-001 - JMAP and CalDAV disagree on whether an event overlaps a range window

**Reported by:** types (LIVE), jmap-account (live, P1 by its estimate).

The jmap-account hunter confirms the consequence: JMAP's `CalendarEvent/query`
does not request `expandRecurrences`, so the server returns the master and the
local filter then discards it - `events_in_range` silently omits recurring events
whose first occurrence precedes the window. It also notes shared
`EventRecurrence` / attendee validation is decided per provider
(`validate_shared_recurrence` in JMAP, `validate_recurrence` in Graph), and RRULE
parsing/printing lives in JMAP's `calendar_ops.rs`.

Question: does a `CalendarEvent` fall inside an `EventRange`? Both
`caldav/src/account.rs::event_in_range` and
`jmap/src/sync/calendar_ops.rs::event_in_range` apply a local guard to results
the server already filtered, with near-copy helpers (`time_interval`,
`comparable_time`). They disagree:

| Case | CalDAV | JMAP |
|---|---|---|
| all-day event | always kept (floating DATE has no instant; the server had the tz) | compared as UTC midnight |
| zero-length event on the window start | kept (RFC 4791 s9.9, the earlier dav ruling) | dropped (`event_end > range_start` is false) |
| recurring master starting before the window | kept unless UNTIL ends before it | dropped |
| zoneless local date-time | unparsed, so kept | parsed as UTC |

JMAP plausibly drops events a JMAP server's `after`/`before` filter correctly
returned (JMAP Calendars matches on any occurrence). Google and Graph answer the
same question a third and fourth way, by delegating to the server and
normalizing bounds (`google_range_bound`, Graph `events_in_range`).

Root cause is the untyped `EventTime` (see STR-003). Proposed owner:
`bifrost-types`, `EventRange::admits(&CalendarEvent) -> bool` carrying the RFC
4791 rules, used by both DAV and JMAP. Both crates carry same-named tests
(`range_filter_includes_all_day_on_window_start`) that would move with it.

## BUG-002 - Graph `open_blob_range` trusts caller-forgeable handle capability flags

**Reported by:** types.

`AccountCapabilities::blob_range` and `blob_digest_pre_download` restate at
account level what `BlobHandle::capabilities` says per handle. The handle is a
public, caller-constructible struct, and Graph's `open_blob_range` trusts
`handle.capabilities.supports_range` to decide whether to send a ranged
request, so a forged or stale handle steers the provider. Gmail and JMAP refuse
regardless. Proposal: providers re-derive handle capability from the `BlobId`
they minted rather than reading it off the caller's handle.

## BUG-003 - Google `Message` hydration drops recipients without marking the message incomplete

**Reported by:** types.

`Address::parse_list` (lossy) and `parse_list_checked` are two doors to one
parser, and each call site decides whether silent recipient loss is acceptable.
Google's `Message` hydration in `google/src/account/pim.rs` reads ONE `To`
header (`header_from`) lossily, while its draft path reads every repeated
header checked. A hydrated `Message` that dropped recipients does not set
`incomplete`. Proposal: a single `AddressList { accepted, rejected }` return so
every caller sees the loss; at minimum read all repeated headers and set
`incomplete` on rejection.

## BUG-006 - A freshly minted cursor with a bad envelope gets three classifications

**Reported by:** sync-engine (P2/P3, sites already disagree).

When an `Account` hands the engine a brand-new `ChangeCursor` whose envelope
fails `validate_envelope()`:

| Site | Classification |
|---|---|
| `multiplexer/changes.rs::drive_changes_stream` | `fresh_cursor_contract_error`: `Internal(AccountContract)`, terminal, scoped |
| `multiplexer/mod.rs::fuse_inventory_done` | same |
| `engine/reattach.rs::run_establish`, `Ready` arm | `cursor_decode_failure`: `SyncState(SchemaIncompatible)`, deriving the persisted-schema directive |
| `engine/attach.rs::persist_cursor` | bare `Error::SchemaIncompatible`, failing the whole attach |

`reference/sync.md` says fresh account-authored envelopes are contract failures
and schema recovery is reserved for persisted envelopes; two sites contradict
it. Owner: one function taking the cursor's origin,
`classify_envelope(cursor, Origin::Fresh | Origin::Stored)`, called by every
site. The two establishment paths that host two of these sites are CON-017.

## BUG-007 - Mutation campaigns trust the account's item ids with no exactly-once check

**Reported by:** sync-engine (P3).

`run_bulk_pipeline` inserts every `ItemOutcome` it receives into `outcomes`
keyed by `ObjectId(item.0)`, whether or not that id was submitted this round or
exists in the campaign at all. An outcome for an unrequested id becomes a new
counted entry; a duplicate is last-write-wins. `BatchOutcomeBuilder::finalize`
enforces exactly-once only for providers that use it; the streaming bulk path is
a `SyncEvent<ItemOutcome>` stream the engine does not validate, unlike inventory
batches (`validate_boundary`, terminating as `AccountContract`). Mutation
streams deserve the same guard. Structural fix: STR-005.

## BUG-008 - `debt()` and `repair_debt()` read the ledger from the store, not from the writer that owns it

**Reported by:** sync-engine (P3).

`SyncEngine::debt` and `repair_debt` call `self.checkpoints.get_ledger`
directly, but `ack_writer` keeps an authoritative in-memory ledger that can
differ from the store: `RecordDebt`, `RecordBarrier`, `ResetScope` and both reset
passes mutate it in place and keep the mutation when the write fails. After a
transient `put_ledger` failure `debt()` under-reports and `repair_debt` plans
against a ledger the writer no longer agrees with. Generation-aware discharge
limits the damage but does not make the enumeration honest. Same habit in
`engine/backfill.rs`, which reads `store.get_backfill` directly while
`DiscardBackfillProgress` for the same scope is ordered through the writer.
Proposal: `WriterRequest::GetLedger` and `GetBackfill`, like `GetChangeCursor`.
The ledger-mutation discipline behind the divergence is CON-022.

## BUG-009 - The orphan-teardown retrier and `repair_debt` skip most of the wire-work gate

**Reported by:** sync-engine (P3).

`engine/orphan_teardown.rs` peeks the raw boundary and skips on `Pause` or
`Stop`, but not `CheckpointNow`; it takes no `begin_activity` (so a pause can
report quiescence while a `push_unsubscribe` is in flight), no scheduler
admission, does not consult the throttle bucket, and records no hold when a
teardown fails. Its comment says "a paused account makes no provider calls on
the engine's own initiative", which the code does not keep. `repair_debt`
likewise does wire work with no activity guard, admission or throttle wait. The
gate itself is CON-014.

## BUG-010 - "The writer is gone" means something different at every call site

**Reported by:** sync-engine (P3), sync-lanes (live divergence).

`WriterHandle::reset_account_for_schema_recovery` maps a closed channel to
`Error::ShuttingDown`; every other `WriterHandle` method, plus `ack_checkpoint`,
`ack_publication`, `operator_decision` and `repair.rs`, maps it to
`Error::Other("writer channel closed: ..")`. Callers branch on `ShuttingDown`:
`run_deferred_inventory_establishment` returns quietly on it but logs `Other` as
"deferred inventory failed". A consumer calling `ack_checkpoint` during detach
gets `Other(String)` instead of `AccountNotAttached`.

The sync-lanes hunter's table of the lanes' answers:

| Site | Writer gone means |
|---|---|
| `inventory_walk::record_barriers` | `Ok(())`: "not a persistence failure" |
| `inventory_walk::cross_waived_barriers` | `Ok(false)`: not waived, so the walk stops |
| `inventory_walk::record_refused_debt` | `Err(Error::ShuttingDown)` |
| runner `Done` `RecordDebt` | ignored: `let _ = done_rx.await;` drops a store error and a departed writer alike (the writer retains the report, so only the diagnostic is lost; `record_refused_debt`, sending the same request, logs it) |
| `engine/backfill.rs::scope_barrier_blocked` | `false`: not blocked, so the walk proceeds |
| `engine/backfill.rs::discard_backfill_progress` | `false`: request stays outstanding |
| `WriterHandle::get_change_cursor`, `persist_established`, `reset_scope`, `reattach_insert` | `Err(Error::Other("writer channel closed"))` |
| `WriterHandle::reset_account_for_schema_recovery` | `Err(Error::ShuttingDown)` |
| `BackfillCheckpointWriter::persist` (Writer route) | `Err(Error::Other(..))` |
| `repair.rs` `ApplyRepair` | `Err(Error::Other(..))` |

Some divergence is principled (a barrier with nowhere to persist is not a
failure), but `scope_barrier_blocked` answering "not blocked" while
`cross_waived_barriers` answers "not waived" are opposite fail-open and
fail-closed choices on the same ledger, and `Error::Other` vs
`Error::ShuttingDown` changes which recovery arm the caller takes. The lanes hold
raw `mpsc::Sender<WriterRequest>`s and hand-roll the oneshot dance at each site;
only recovery code gets `WriterHandle`. Proposal: `WriterHandle` everywhere,
returning `Result<T, WriterError>` with `WriterError::Gone` distinct from
`WriterError::Store(Error)`, so each call site states which it tolerates (see
STR-031 for where the writer protocol should live).

## BUG-011 - A pause cut short by detach is classified `Request(Malformed)`

**Reported by:** sync-engine (P3).

`control.rs::control_wait_ended` builds
`AccountErrorKind::Request(RequestErrorKind::Malformed)` for "account stopped
before reaching a safe boundary", "checkpoint watch closed" and "boundary
channel closed". A consumer branching on kind sees a malformed request it never
made. A typed control-wait error is part of STR-027.

## BUG-012 - Two resume paths with different effects, and engine pause state is never retained

**Reported by:** sync-engine (P3, divergence).

`SyncEngine::resume_account` writes `Run` with a raw `send_replace` and
publishes `AccountControl::Resume`; `Control::resume` (`SyncControl`) uses
`set_unless_stopped` and publishes nothing. A consumer following
`account_control_stream` sees the engine's `Pause(reason)` but never a `Resume`
if the resume came through `Control`. Engine pause reason and state are only
broadcast, never retained, so a late subscriber cannot learn the account is
engine-paused or why (compare `push_health`, a watch). Proposal (sync-engine
C9): fold it into the per-account slot as a watch,
`EngineState::{Running, Paused(PauseReason), Stopping}`, so both resumes are one
transition with one notification.

## BUG-013 - `WriterRequest` is published, contrary to the reference

**Reported by:** sync-engine (doc vs code).

`reference/sync.md` ("Published surface the bounded lane added") says
`WriterRequest` is "crate-internal - that enum is not re-exported". In fact
`multiplexer/mod.rs` has `pub use changes::{AckRequest, ChangesEvent,
OperatorDecision, WriterRequest, drive_changes_stream}` under a `pub mod`, with
public variants, making it `bifrost_sync::multiplexer::WriterRequest`. Under the
never-delete-published-API rule, the writer's internal protocol is frozen
surface by accident. More broadly most engine internals are `pub` with public
fields: `Multiplexer`, `InventoryFusion`, `ScopeTokens` and
`ScopeToken::tombstone`, `drive_changes_stream`, `AckRequest`,
`PublicationId(pub u64, pub Arc<..>)`. Narrowing them is removal and needs the
owner; recorded because each widens what later refactors must preserve. Either
the reference is corrected or the owner rules on narrowing.

## BUG-014 - Engine passthroughs bypass pause, throttle holds and admission, and the reference's rationale for them is false

**Reported by:** sync-engine.

`reference/sync.md` ("Hydration passthrough") justifies forwarding explicitly
instead of exposing `Arc<dyn Account>` as "Read surface only. Mutations stay
funnelled through `bulk_set_flags`". `engine/passthrough.rs` forwards about 60
methods, including `send_message`, `container_delete`, `contact_delete`,
`filter_create`, `set_starred`, `move_thread` and `delete_thread`. None honours
pause, consults or records throttle holds (a direct `send_message` 429 records
no hold the polls would honour), or takes admission. The real boundary is
"whatever someone wrote a forwarder for". Either the doc's rationale is
corrected or the forwarders go through the gate (CON-014).

## BUG-015 - Proof generations restart at zero every attachment while ledger generations are durable

**Reported by:** sync-lanes (live, by reading).

`PendingCoverage::next_generation` is an `AtomicU64` starting at `0` in
`PendingCoverage::default`, and a fresh `PendingCoverage` is built per
attachment (`engine/attach.rs`). But every `LedgerEntry::generation`,
`BarrierIncident::generation` and retained `proved` pair is written to the
durable ledger (`cursor/ledger_envelope.rs` encodes all three) and reloaded on
the next attach. Every discharge rule in `cursor/ledger.rs` compares a new
generation against the stored one: `record_proof` discharges only through
`proof_union_covers(&self.proved, entry.generation, ..)`, which needs
`proof_generation >= entry.generation`; the same call drops a barrier only when
the proof generation is at least `barrier.generation`; the `proved.retain` pass
then drops the new proof at once, older than every entry it could help.

After a restart, a clean covering walk cannot discharge debt (or clear a
barrier) raised in an earlier session at a higher generation until the new
counter climbs past it, moving once per walk or repair attempt. Meanwhile
`completion_permitted` stays false and the marker is withheld: a full re-walk on
every attach for as long as the gap lasts, possibly many sessions. `upsert`
overwrites `existing.generation = generation` on rediscovery, which can LOWER an
entry's generation, so the durable ordering is not even monotonic.

Root cause: the counter ordering durable evidence is minted by an in-memory
object with attachment lifetime. Fix: a `ProofGeneration` minted by the
account's writer (which owns the durable ledger) from `max(generation)` in the
loaded ledger; `PendingCoverage` mints nothing persisted. Also, `repair.rs`
`plan_requests` mints `RepairAttemptId(coverage.next_generation())`, so repair
attempt ids share the sequence (also STR-025).

## BUG-016 - The backfill completion marker wins resume selection by a magic number, and can lose

**Reported by:** sync-lanes (latent; live once any provider adopts partitioning).

The durable "backfill is done" fact is a `BackfillCheckpoint` on partition key
`b"complete"` with `items_done = total_seen + 1`
(`engine/backfill.rs::emit_backfill_complete`), whose doc claims every
per-partition row records `<= total_seen`. The store selects the resume row by
greatest `items_done` (`CheckpointStore::get_backfill` contract,
`InMemoryCheckpointStore` via `backfill_checkpoint_is_later`). The guarantee
only covers rows from the same walk, and rows survive across walks:

- `items_done` is per partition and per walk (`run_partition` resets
  `seen_total`); `total_seen` counts only the current walk. In a multi-partition
  `Fixed` plan, if the scope shrank and a window now yields no page, its old row
  keeps its old count; if that exceeds the new total, the stale row beats the
  marker, `backfill_complete_recorded` reads false, and the scope is re-walked
  on every attach indefinitely.
- In an `OpenPages` walk resumed at `T`, `total_seen` covers only the resumed
  part; a final empty `Exhausted` probe stamps `items_done = 1`, and the earlier
  `page:F:T` row with up to `chunk` items beats it. The scope re-probes on every
  attach.
- `TimeWindowed` keys embed absolute instants from `Timestamp::now()` at plan
  time (`partition_key` renders `time:{from}:{to}`; `backfill_plan_for` calls
  `plan(&policy, Timestamp::now(), 0)`), so every attach writes fresh keys that
  never overwrite the last set: rows accumulate without bound in the consumer
  store, each competing with the marker. The partitioner's "keeping partition
  identity stable across runs" holds only for the `Timestamp::MIN` sentinel.

Every in-tree provider reports `InventoryPartitioning::Full` today (one key each
walk overwrites), so all three are latent.

The same fact has three encodings: the `b"complete"` key (read by
`backfill_complete_recorded`, by `persist_ack_request`, and by more than twenty
literal `checkpoint.partition.0 == b"complete"` comparisons in `tests/`; a
consumer has no typed way to ask), the `items_done` magic, and
`PublicationReceipt::walk_watermark.is_some()` (set only by
`register_walk_marker`, read in `completion_refusal`, which cross-checks the
second against the first). Partition keys are stringly too: `partition_key`
formats `full | time:F:T | uid:F:T | page:F:T` with a catch-all `"unknown"` arm,
so every future `InventoryPartition` variant collides on one key, and
`parse_page_partition` parses it back. And "which row is the resume position?"
is engine policy implemented in every consumer store: the `get_backfill`
contract (greatest `items_done`, then greatest parsed page upper bound, then
greatest raw bytes) forces each backend to parse the engine's `page:F:T` format
(`InMemoryCheckpointStore` borrows `partitioner::parse_page_partition`; an
external backend must copy it), and `open_pages_resume` re-parses the result.

Proposal: a typed durable key, `enum BackfillKey { Partition(PartitionKey),
Complete }`, `PartitionKey` an enum carrying the bounds with its own codec in
`cursor/envelope.rs`, the completion row its own row kind; `CheckpointStore`
gains `get_backfill_resume -> BackfillResume { None, Complete,
At(PartitionKey) }`, or the store returns rows and the engine selects. Trait
change, needs the owner. `BackfillProgress::items_done` goes back to meaning a
count.

## BUG-017 - Backfill never tells the consumer about degraded coverage; fusion does

**Reported by:** sync-lanes (live divergence).

`InventoryFusion::warn_degraded` broadcasts `OperatorAttentionNeeded`
("completed with N object(s) or region(s) it could not represent") at a degraded
`Done`. `BackfillRunner::run_partition` warns only for barriers
(`warn_barrier`); a degraded page or completion clears `complete`, registers the
claim or sends `RecordDebt`, and announces nothing. `reference/sync.md` says
"Degraded coverage is surfaced to the consumer as a
`Warning::OperatorAttentionNeeded`" - true for one front end only, and backfill
is the one every `Ready`-cursor cold start rides (Gmail, JMAP Email). The
underlying duplication of per-page front-end logic is CON-024.

## BUG-018 - The ledger and cursor codecs panic on a `bifrost-types` variant they have not learned

**Reported by:** sync-lanes (latent).

`cursor/ledger_envelope.rs::encode_operation` looks the operation up in the
positional table `OPERATIONS` and panics if it is missing. The table matches
`bifrost-types` today, but `AccountOperation` is `#[non_exhaustive]` in another
crate, so nothing at compile time ties them: the first new operation reaching a
persisted `last_error` or barrier `evidence` panics the account writer.
`cursor/envelope.rs` has the same shape for `ObjectType` and `ProtocolKind`
("encoding a variant the codec does not know panics"), and that codec runs
inside consumer stores. The structural cause is STR-045.

## BUG-019 - An unknown `Checkpoint` variant from a provider panics fence-guarded registration

**Reported by:** sync-lanes (latent).

`PendingCoverage::register_in` does `lane.scope().expect("conditional
registration of a checkpoint with no scope")` whenever `expected_fence` is
`Some`. Fusion's `validate_checkpoint_envelope` admits any `Checkpoint` variant
it does not recognise (`_ => Ok(())`) and publishes it through
`publish_checkpoint_if_fence`, which maps to `Lane::UnkeyedCheckpoint` with no
scope and hits the panic. `Checkpoint` is `#[non_exhaustive]`. The account
boundary should refuse an unknown variant as a contract violation, as it
already refuses `Checkpoint::Backfill`.

## BUG-020 - `PendingCoverage::retire` drops debt and strands the boundary entry; "retire a publication" has five doors

**Reported by:** sync-lanes (latent foot-gun).

`retire(id)` removes only `claims[id]`: it leaves the `BoundaryEntry` charged
against the backfill bound and drops the claim's debt without `debt_only` or
carry-forward. `retire_publication` exists because exactly that ("an earlier
revision deleted the claim outright") destroyed rescued obligations. The
runner's `(None, Some(pending), Some(id)) => pending.retire(id)` arm and
fusion's `retire_claim` fallback both call the unsafe one; both are unreachable
today (a publication id exists only when `control` is `Some`). `retire` is
published and documented as the right call for a publication that "reached no
real subscriber", which is exactly what it gets wrong.

The doors:

| Method | boundary entry | claim | debt carried | wake |
|---|---|---|---|---|
| `retire_publication` | removed (incl. subsumed) | removed | yes | yes |
| `abandon([id])` | removed (incl. subsumed) | removed | yes | yes |
| `retire` | kept | removed | no | no |
| `acknowledge_checkpoint(&cp)` | removed by VALUE | kept | n/a | yes |
| `acknowledge_publication` | removed | kept (consumed later by `claim_*`) | n/a | yes |
| `supersede(a, b)` | untouched | folded | n/a | no |

`retire_publication` and `abandon` are the same operation twice; `retire`,
`acknowledge_checkpoint` and `supersede` each update half a publication, and the
module's comments call two of them foot-guns. `SyncControl` also wraps
`retire_publication` with `publish_quiescence`, which the ledger-level call does
not, so a caller holding only the ledger retires without waking boundary
waiters. Proposal: one `retire(id, why: RetireReason)` transition that always
handles both halves, debt and wake, with quiescence owned by the same object
(STR-042); the half-operations go crate-private (owner ruling: published).

## BUG-021 - Destroy read-back treats any `NotFound` as proof of absence; repair refuses to

**Reported by:** sync-lanes (live policy inconsistency).

`mutation/readback.rs::destroy_readback_saw_absence` accepts
`AccountErrorKind::NotFound(_)` of any `ResourceKind`, so a `NotFound(Mailbox)` or
`NotFound(Account)` per-item failure counts as "the destroy landed" (`Skipped`).
The repair lane takes the opposite position on the same question:
`reference/sync.md` says "a 404 may mean deleted, moved out of scope, permission
lost, wrongly routed, or not yet replicated", and `DefinitivelyIrrelevant` is a
typed account conclusion, "never a status code". Owner: a `bifrost-types`
predicate both consult (`AccountError::proves_absence_of(kind)` or a typed
`ItemOutcome` absence variant), with the producer, which knows whether its 404
means deleted, deciding - as repair already works.

## BUG-022 - Topology replacement resets the per-scope fence counters

**Reported by:** sync-lanes (latent fragility).

`CursorRegistry::replace_topology_preserving_cursors` installs the staging
registry's `RegistryState` wholesale (`*current = replacement`), including its
`scope_generations`, which are usually empty, so every scope's delete counter
drops to zero. Safe today only because the same call bumps `registry_generation`,
fencing every pre-swap `DriveGeneration`. A counter documented as a fence should
be monotonic, not resettable by a merge; incarnations and cancel tokens are
carried across explicitly, so this field was missed. Proposal (with STR-048):
merge per-scope state field by field rather than adopting the staging copy.

## BUG-023 - A `Fixed` backfill plan with zero partitions writes a durable completion without any inventory call

**Reported by:** sync-lanes (lateral).

`ScopeWalkDriver::with_source` starts `completed: true`, so a `Fixed` plan with
zero partitions (UID or page `total == 0`) settles and writes a completion
marker without asking the provider anything. That is right only if the
provider's `max_uid` / `total` is authoritative; a stale or zero hint becomes a
durable "done".

## BUG-024 - A server reporting UIDVALIDITY 0 makes every inventoried IMAP object unhydratable

**Reported by:** imap-protocol (latent; "is a zero UIDVALIDITY / MODSEQ / UIDNEXT usable" has no owner).

The decoder accepts 0 for UIDNEXT, UIDVALIDITY, UNSEEN, HIGHESTMODSEQ, MODSEQ,
APPENDUID and COPYUID uidvalidity ("Postel, Dovecot sends 0 for empty
mailboxes"). Then: connection SELECT folding stores `UidValidity(0)` as
`Some(0)` although the `SelectedMailbox::uid_validity` doc says `None` exists to
avoid the invalid sentinel 0 (HIGHESTMODSEQ 0 there is treated as NOMODSEQ);
`account::selected_uidvalidity` ("the one place the classification lives")
checks presence only and returns `Ok(0)`; `poll::observation` additionally
rejects 0; `folder_registry::record_modseq` silently skips uidvalidity 0 and
modseq 0; `select_options` drops QRESYNC when `UidValidity::new(cursor value)`
is `None`; the encoder rejects uidvalidity 0 and modseq 0 in QRESYNC as
`InvalidInput` (a caller-blaming class); `decode_object_id` rejects 0 via
`parse_nonzero_u32`.

Consequence: inventory, changes and hydration mint object ids
`imap2:..:0:<uid>` through `encode_object_id`, and `decode_object_id` rejects
every one, so nothing inventoried can be hydrated or mutated; the folder cursor
is written with uidvalidity 0, so every reconnect silently drops QRESYNC and
full-resyncs. Owner: the decoder (or the SELECT consumer) turns the wire number
into `Option<UidValidity>` / `Option<ModSeq>` once, stating the "0 means absent"
tolerance there; every downstream field is the typed value.

## BUG-025 - IMAP SEARCH and APPEND date validators disagree

**Reported by:** imap-protocol (live, minor).

`search_helpers::validate_imap_date` (SEARCH SINCE/BEFORE/ON) compares the month
case-SENSITIVELY against `"Jan".."Dec"` with no day-of-month check;
`encode::validate_append_datetime` (APPEND) compares case-insensitively (RFC
3501 section 9) with day-vs-month, leap-year and hour/minute/second/zone checks.
`account/pim.rs::imap_date` formats a `SystemTime` into the string the first
validator re-parses; `FetchResponse::internal_date` / `save_date` stay raw
`String` and nothing parses INTERNALDATE. So `"13-feb-2025"` is rejected for
SEARCH and accepted for APPEND, and `"30-Feb-2025"` is accepted for SEARCH and
rejected for APPEND. Proposal: `ImapDate` / `ImapDateTime` types (`jiff` is
already a dependency) with `Display` for the encoder and a decoder-side parser.

## BUG-026 - IMAP folds a neutral `$seen` keyword on one write path and not the other

**Reported by:** imap-protocol (live).

`pim::imap_flag_for_keyword` folds `$seen` / `$flagged` / `$answered` (JMAP
spellings) onto `\Seen` / `\Flagged` / `\Answered`; `account/mutate.rs` uses
plain `Flag::from` (no folding); the flag judge compares raw strings. So
`set_keyword("$seen")` stores `\Seen` while `bulk_set_flags({"$seen"})` stores a
literal keyword `$seen`, and a read-back of the latter judges against `\seen`.
Owner: `Flag::from_neutral_keyword` in `types/flag.rs`, used by every write path
and by the judge. The writable-flag type split behind it is STR-054.

## BUG-027 - "Is this mailbox INBOX / the same mailbox?" has six answers, and a solicited STATUS is misrouted

**Reported by:** imap-protocol (live).

- `MailboxName::eq` / `hash`: wire identity, NO INBOX folding (`"inbox"` !=
  `"INBOX"`).
- `MailboxName::matches_mailbox`: folds exact `INBOX` only.
- `classification::mailbox_names_eq`: folds exact `INBOX`; production-dead,
  kept alive only by `connection/helpers_tests.rs` through the `inbox_eq` alias
  - a copy that exists to be tested.
- `encode::commands::encode_mailbox_str`: folds `INBOX` AND rewrites an
  `INBOX<ascii-separator>child` prefix to upper case.
- `account/push.rs::first_inbox_folder`: case-insensitive, with a tie-break
  because the registry can hold two INBOX-equivalent entries (a consequence of
  the first bullet).
- `account/pim.rs::folder_role` name fallback: leaf-name `"inbox"` (BUG-028).

The disagreement that bites: a caller names `inbox/Sub`; the encoder sends
`INBOX/Sub`; the server answers `* STATUS "INBOX/Sub"`; `matches_mailbox`
compares `inbox/Sub` with `INBOX/Sub` and says no; `classify` routes the
solicited STATUS as unsolicited; the STATUS consumer's finalize reports the
missing STATUS as `ProtocolMissing`. Same shape for QUOTAROOT, ACL, MYRIGHTS,
METADATA and LIST-correlated consumers. Owner: `MailboxName` construction
canonicalizes the INBOX token (and the INBOX-prefix, if the encoder's rule is
kept) once, for caller and server names alike; `eq`, `hash`, `matches_mailbox`
and the encoder then agree by construction, and `mailbox_names_eq` is deleted
with its tests. The display/wire conversion it rides on is CON-031.

## BUG-028 - IMAP role folders ignore namespace, so a shared folder can win the role

**Reported by:** imap-protocol (live, needs confirmation), imap-account (live).

The imap-account hunter reached the same defect independently: a shared
`#shared/team/Sent` (sorting before `Sent`), or a shared folder carrying a
`\Sent` attribute when the personal one has none, becomes the account's Sent, so
`send_message` files the sender's copy there and `delete_thread` moves mail into
another principal's Trash. It counts four deciders for "which folder plays role
R": `pim::folder_role` / `role_folder_from_entries` (below);
`capabilities::build_capabilities`'s `has_drafts` (SPECIAL-USE `\Drafts` or leaf
`"drafts"` only - not `"draft"` - and only over the personal LIST, since it runs
before shared folders are merged; its comment claims it is "the same leaf rule
`folder_role` uses", but only the leaf split is shared, so a server with a
`Draft` folder resolves `role_folder(Drafts)` while advertising `draft_create =
false`); `push::first_inbox_folder` (full name equals `INBOX`
case-insensitively, not the role rule); and `pim::quota_probe_folder`
(`role_folder(Inbox)` then first selectable). Its owner: the registry computes
roles once at ingest (personal namespace only, with an explicit policy for
shared), and capabilities, send, draft, trash and push all read that one
answer.

`pim::role_folder_from_entries` filters every registry entry, personal and
shared (`FolderEntry::shared_owner` is not consulted), by `folder_role`, ranks
attribute matches above name matches, and breaks ties with
`MailboxName::cmp_account_order` (lexicographic display order). On a server
without SPECIAL-USE that exposes shared folders, personal `Trash` and shared
`Shared/alice/Trash` both match by name with equal rank, and
`"Shared/alice/Trash" < "Trash"`, so `delete` / trash fallback and
`role_folder(Sent|Drafts)` can resolve to another user's folder. With the
leaf-name rule for INBOX, `Archive/Inbox` (`'A' < 'I'`) beats `INBOX` for
`quota_probe_folder`, and `containers_list` labels every shared user's `INBOX`
as `FolderRole::Inbox`. Fix: restrict role resolution to the personal namespace,
and match INBOX only as the top-level name. The duplicated role tables are
CON-028.

## BUG-029 - IMAP command admission has three front doors and two clocks, and they disagree

**Reported by:** imap-protocol (live), imap-connection (live; its top-ranked finding).

"Which capability (and ENABLE state) does this command need" is answered in the
encoder's per-arm `missing(..)` gates (`encode/dispatch.rs`) and again,
separately written, in the connection handle (`extensions.rs`
ACL/METADATA/QUOTA/NAMESPACE/ID/ENABLE/COMPRESS, `mailbox.rs`,
`helpers::require_condstore` / `require_searchres`, `uid_ops`, `sort_thread`,
`auth`) - not one answer re-checked at two chokepoints, but the predicate
restated, and it differs:

- SELECT/EXAMINE with QRESYNC parameters: the handle requires QRESYNC to be
  ENABLEd (`validate_qresync_params`, RFC 7162 section 3.2.3 MUST); the encoder,
  documented as the live authority at the head of the queue, only checks
  `has_capability(QResync)`, i.e. advertised.
- UID FETCH ... VANISHED: handle requires ENABLEd (`uid_ops`), encoder checks
  advertised. UID FETCH is pipelinable, so a pipelined CHANGEDSINCE+VANISHED
  fetch is gated only by the weaker encoder check.
- AUTHENTICATE PLAIN: `connection/auth.rs` tests
  `capabilities.contains(&Capability::Auth("PLAIN"))` directly, not through
  `supports`.

Owner: `Command::requirements() -> Requirements { capabilities, enabled,
states, refused_after_selection }` beside `CommandKind::legal_states`, read by
both handle and encoder against one `ConnectionCaps` (STR-055); search criteria
contribute through `SearchKey::required_capabilities()` (STR-053).

The imap-connection hunter mapped all four doors. (1) Handle methods, against
the watch snapshot: `require_state`, `require_condstore`, `require_searchres`,
`check_utf8_only_enforced`, `require_notify`, `require_auth_mechanism`, five
hand-coded `state_refusal(.., &[SessionState::NotAuthenticated])` calls in
`auth.rs` (a second copy of the `CommandKind::legal_states` row for
LOGIN/AUTHENTICATE), 17 `sequence_set.as_str().contains('$')` scans
(`uid_ops.rs`, `seq_ops.rs`, `ergonomics.rs`), three QRESYNC-ENABLEd scans,
`validate_requested_fetch_items`, `validate_requested_status_items`,
`validate_list_extended_request`, CHECK-on-rev2 in `helpers::check`, STARTTLS,
COMPRESS, UNAUTHENTICATE, ENABLE, ESEARCH. (2) The driver's
`live_state_refusal` (state table plus `refused_after_selection`) against live
state - a deliberate re-check, fine. (3) The encoder's gates, against live
state. (4) The `Pipeline` builder: `execute_raw` calls `require_state` only.
Further disagreements it found:

- `$` in a sequence set: only the handle checks it (the encoder checks `$` only
  inside search criteria), so the pipeline's FETCH/STORE/COPY/MOVE with `$`
  reach the wire ungated.
- FETCH item gates (MODSEQ, PREVIEW, SAVEDATE, OBJECTID, X-GM-EXT-1, BINARY):
  handle-only, against a snapshot the encoder's own comment says "may predate a
  CAPABILITY/ENABLE update"; the pipeline's `items: String` bypasses them.
  STATUS item gates, LIST-EXTENDED option gates, CHECK on rev2 and UTF8=ONLY are
  handle-only and bypassed the same way.
- STARTTLS availability: three rules, three outcomes - setup's
  `ensure_starttls_available` (`StartTlsUnavailable`), the handle's raw
  `Capability::StartTls` scan plus `tls_active` (`StartTlsUnavailable` /
  `InvalidInput` - `starttls_with_connector` refuses an encrypted connection
  with `InvalidInput`, a session-state refusal), the encoder
  (`MissingCapability`).
- Capability spelling: `snapshot_supports`'s doc says "EVERY handle gate ...
  goes through here", but `require_notify`, `login`'s LOGINDISABLED scan, the
  AUTH gates, the STARTTLS gate and the APPENDLIMIT gate use raw
  `capabilities.contains` / `iter().any` - equivalent today only because none of
  those is in the rev2 baseline.
- "Advertised but not ENABLEd" has two error answers:
  `check_utf8_only_enforced` returns `InvalidState` ("the same request succeeds
  after the ENABLE"); the QRESYNC-not-ENABLEd gates return `MissingCapability`,
  which `account/error.rs` maps to `Unsupported(operation)`. Same situation, two
  recovery classes.
- Eager vs deferred: `uid_fetch_changed_since_stream` checks `require_state`
  eagerly; `uid_fetch_stream_budgeted` checks it inside the returned future.

Its owner: one `fn admit(command: &Command, session: &SessionView) -> Result<(),
Refusal>` (in `types/command.rs` beside `legal_states`, or
`connection/admission.rs`), `Refusal` carrying a typed reason (STR-057). The
handle calls it against the snapshot for an early refusal; the driver calls it
in `prepare_command` against live state as the authority, mapping a refusal it
alone finds to `StateChangedBeforeSend`; the encoder becomes a pure encoder; the
pipeline builder gets every gate for free. About 60 inline gate blocks go - "the
IMAP analogue of the status-line consolidation".

## BUG-030 - SASL vocabulary, ladders and policy are split across IMAP, ManageSieve and SMTP

**Reported by:** imap-protocol, imap-connection, imap-account, smtp-sasl.

The payload defects are fixed: bifrost-sasl `plain_payload` (NUL-refusing,
`Secret`) now serves IMAP and ManageSieve PLAIN, and ManageSieve XOAUTH2 goes
through `xoauth2_payload`. What remains is the consolidation:

- SMTP still builds its own PLAIN payload in
  `Mechanism::response_with_token_at_index` rather than `plain_payload`.
- Mechanism vocabulary is three types: `bifrost-imap::AuthMechanism` (`Login`
  meaning the IMAP LOGIN command, SCRAM names hard-coded), public
  `bifrost-smtp::Mechanism` (`Login` meaning SASL LOGIN, no CRAM-MD5), and the
  SCRAM token in bifrost-sasl. IMAP's edge is stringly (`Capability::Auth(String)`,
  `Command::Authenticate { mechanism: String }`,
  `ServerProfile::auth_mechanisms: Vec<String>`), and "is AUTH=X advertised" is
  answered at several sites with different case rules.
- Ladders: IMAP `password_mechanism_ladder` (fixed order with CRAM-MD5 and LOGIN,
  `AuthPolicy`, downgrade skip, fall-through) vs SMTP `password_mechanism`
  (caller allow-list, single choice); the SCRAM hash mapping is written inline
  three times in IMAP. SMTP has `PASSWORD_MECHANISMS`, `MECHANISM_PREFERENCE`
  and `DEFAULT_MECHANISMS`.
- Cleartext gate has three answers: IMAP per mechanism (SCRAM allowed over
  plaintext), SMTP refuses all AUTH unencrypted unless
  `dangerous_allow_insecure_auth`, ManageSieve gates everything and
  re-implements the `AuthPolicy` gate. ManageSieve supports only PLAIN and
  XOAUTH2 (no SCRAM, no OAUTHBEARER).
- Channel-binding resolution is two comment-held copies
  (`imap::connection::auth::scram_binding_from_der`,
  `smtp::authentication::resolve_scram_binding`).
- `SaslError` mapping differs: IMAP maps an unknown variant to an auth failure,
  SMTP to `invalid_input`.
- `types/secret.rs::SecretString` is a near-copy of `bifrost_sasl::Secret` with a
  hand-rolled constant-time `eq`; crate-private, so deletable without a ruling.

Owner: bifrost-sasl gains the mechanism enum, the OAuth preference order, one
`negotiate(advertised, policy, encrypted, binding)` ladder and one binding
resolver; IMAP, Sieve and SMTP select from it (owner ruling on what SMTP's public
`Mechanism` becomes). The SASL client layer this lives in is STR-135; where
ManageSieve should live is STR-060.

## BUG-031 - A hydrated IMAP `Message` extracts Message-IDs with two different parsers

**Reported by:** imap-protocol (live, minor).

`types/envelope.rs::bare_message_id` / `first_in_reply_to` use a structural
scanner that skips `<..>` inside quoted strings and comments and falls back to a
bare token; `bifrost_types::mime::HeaderMap::message_ids` takes the first `<`
and next `>` naively. `account/pim.rs` builds `Message::in_reply_to` with the
first and `Message::references` with the second, so `"\"<fake>\" <real>"` yields
`real` in one field and `fake` in the other; the other providers use the naive
one. Owner: `bifrost-types` mime, promoting the structural algorithm; the IMAP
copy becomes a call.

## BUG-032 - IMAP FETCH merge collapses partial fetches of one section at different origins

**Reported by:** imap-protocol (latent).

`BodySection { section: String, origin }` and `FetchAttr::BodySection {
section: Option<String> }`. `account/get.rs::merge_fetch_response` matches body
sections case-insensitively by name and IGNORES `origin`, so two partial fetches
of the same section at different origins in one response collapse (the second
is dropped); binary sections compare `Vec<u32>` exactly. Proposal: a typed
`SectionSpec { parts: Vec<u32>, text: Option<Header | HeaderFields(..) | Text |
Mime> }`, with origin part of the key that owns "same section".

## BUG-033 - IMAP compile-fail doctests never run and import non-existent paths

**Reported by:** imap-protocol (latent).

`crates/imap/Cargo.toml` sets `doctest = false`. `MailboxName`'s doc carries two
`compile_fail` blocks asserting there is no `From<String>` / `From<&str>`,
importing `bifrost_imap::MailboxName`, which is not exported; they never run, and
if run would pass for the wrong reason (unresolved import). The
`types/search.rs` module example imports `bifrost_imap::types::SearchCriteria`
(private) and links `crate::ImapConnection::search` (crate-private). The
guarantees these blocks advertise are unpinned. Either move them to a `tests/`
trybuild-style check against a crate-internal path or drop the claim.

## BUG-035 - Single-command and pipeline paths disagree on NOTIFY after an interleaved NOTIFICATIONOVERFLOW

**Reported by:** imap-connection (live).

"Which session state does this command move?" is answered by
`driver::run_prepared_command` (a `matches!` chain arming `set_in_logout`,
`set_in_auth`, `set_in_select`, `set_in_close`, `set_in_unauthenticate`,
`set_in_notify_set` before the send); `pipeline::apply_pipeline_pre_effects`
(NOTIFY SET/NONE again, armed at tagged-response arrival rather than before the
send - its doc says a second copy "is the drift shape this crate has paid for
before"; it is the second copy); `CommandKind::pipelinable` (an independent list
of state-moving commands; NOTIFY moves notify flags but is pipelinable, which is
why the pipeline needs its own hook); `ProtocolState::apply_tagged` (the
completion half, with per-flag clearing rules, BUG-034); and
`wire_send::wait_for_continuation` vs the pipeline router's `early_own_ok` (two
implementations of "an early OK before the `+` must not complete the armed
effect").

A `* OK [NOTIFICATIONOVERFLOW]` arriving between a NOTIFY SET's send and its
tagged OK clears the pending registration on the single-command path (armed
before send, cleared by `apply_untagged`) but not on the pipeline path (armed
only when the tagged OK arrives, after the overflow was processed), so one server
sequence leaves NOTIFY registered in one path and unregistered in the other.
Which is right depends on whether the overflow preceded the server's processing;
nothing reconciles the copies. Owner: `Command::pending_effect(&self) ->
Option<PendingEffect>` in `types/command.rs` beside `legal_states`, with
`pipelinable` derived from it, the armed effect keyed by tag (STR-065), and both
paths arming through the one function before the first byte.

## BUG-037 - IMAP literal budgets are not enforced on the pipeline, IDLE or continuation-wait read loops

**Reported by:** imap-connection (latent).

Only `dispatch_response_loop` calls `read_one_limited(utf8,
consumer.literal_budget())`; every other loop calls `read_one(utf8)`. No
pipelined consumer has a budget today (`FetchConsumer::new()`), so nothing
breaks, but `ConsumerErased::literal_budget` is part of the pipeline consumer
contract and is silently ignored there. Structural fix: one read-loop engine
(CON-034).

## BUG-038 - IMAP APPENDLIMIT has two answers for one advertisement

**Reported by:** imap-connection (live disagreement).

`ServerProfile::new` (public, `types/profile.rs`), on a contradictory
advertisement (bare and numeric forms together), reports
`AppendLimitPolicy::PerMailbox` and drops the numeric value ("preserve the safer
bare interpretation"). `append.rs::AdvertisedAppendLimits` plus
`append_limit_from_advertisement` (enforcement) applies `min(global, mailbox)`. A
consumer reading `server_profile().append_limit` is told something different from
what `append` enforces. `validate_requested_status_items`'s APPENDLIMIT gate is a
third reader (`matches!(cap, AppendLimit(_))`). Owner: `AppendLimitPolicy` grows
the shape enforcement needs (`PerMailbox { global_ceiling: Option<u64> }`), and
`append.rs` reads it instead of re-folding capabilities. Lateral: `append_message`
computes the APPEND deadline, then runs the APPENDLIMIT STATUS preflight with a
fresh `timeout`, then submits APPEND against the partly spent deadline, so it can
spend up to twice the caller's timeout - a real bound, not the documented one.

## BUG-039 - IMAP SELECT swallows unrecognized coded statuses instead of publishing them

**Reported by:** imap-connection.

"Which SELECT/EXAMINE responses belong to the SELECT?" is answered by
`selected_mailbox_effective_responses` (`mod.rs`, the `[CLOSED]` cut),
`reclassify_select_responses` (`dispatch/select.rs`, a second `rposition` of
`[CLOSED]`, then `is_select_solicited_response` decides consumed vs event),
`build_selected_mailbox` / `extract_selected_code` (which responses and codes
feed `SelectedMailbox`), `validate_select_responses` (the mandatory set: FLAGS,
EXISTS, RECENT or LIST), `codec::classification` for SELECT/EXAMINE, and
`account::selected_uidvalidity` (UIDVALIDITY's mandatoriness judged in the
account layer). They disagree at the edges: `is_select_solicited_response`
consumes every `Status { code: Some(_) }`, so a `* NO [EXPUNGEISSUED]` or any
unrecognized coded status during SELECT is swallowed and never reaches the event
stream, while `build_selected_mailbox` uses only the select codes. Owner: one
`partition_select(responses, ctx) -> (SelectInputs, Vec<UntaggedResponse>)` that
cuts on `[CLOSED]` once, assigns each response to exactly one side, and whose
`SelectInputs` constructor judges mandatoriness (UIDVALIDITY included); the
helpers belong in `dispatch/select.rs`, not `mod.rs` (STR-068).

## BUG-041 - `Pipeline::execute_raw` waits unbounded, contradicting "no infinite waits"

**Reported by:** imap-connection.

`auth::submit_driver_command` is "the one submission body behind the four
helpers", with strict admission and the withdraw/commit race. Three more
submission paths have their own rules: `Pipeline::execute_raw`
(`cmd_tx.reserve().await` with no deadline - the only command-carrying call with
no bound, contradicting the `ImapConnection` doc's "no infinite waits");
`ImapConnection::idle` (its own `select!` over cancel, deadline and `reserve` -
with reason, though its admission half could be `submit_driver_command`'s); and
`set_keepalive` / `peer_certificate_der` (`cmd_tx.send(..).await` with no reply
cell and no deadline, queued behind any command; the callers wrap them in
`in_flight()` or `timeout_at`). `execute_raw` and the two non-wire requests have
no reason to differ.

## BUG-042 - IMAP `native_code` is `"OTHER"` for every unknown response code

**Reported by:** imap-account (live), imap-protocol.

The wire name of each response code is written in three tables: the codec
decoder (`codec/decode/flags_caps.rs`, string -> variant),
`account/error.rs::response_code_name` (variant -> string), and
`bifrost_types::ImapResponseCode::code` (private), reached through a 60-arm
`imap_response_code` mirror of `crate::types::ResponseCode`. `classify_status`
stamps `native_code` from `response_code_name`, which returns the literal
`"OTHER"` for every unknown code, while `ImapResponseCode::Unknown { code }`
(built by `imap_response_code` for the same error) returns the server's real
name. So `native_code` loses exactly the information an operator needs for an
unrecognized code. Owner: make `ImapResponseCode::code` public and derive
`native_code` from the already-built `imap_response_code(code)`, deleting the
middle table; the IMAP crate should own the conversion as an `impl From` beside
`ResponseCode` in the codec, with the name table in exactly one place (see
STR-013 on provider vocabularies in the neutral crate).

## BUG-043 - An IMAP server without IDLE exposes push lanes and accepts direct `push_subscribe` while advertising no push

**Reported by:** imap-account.

`build_capabilities`: `PushCapability::InProcess` iff `Capability::Idle` and
`push_capacity > 0`. `PushState::new`: lane count from `push_capacity` and
`supports_notify`, no IDLE check. `push_subscribe` admission: `push_capacity > 0`
and NOTIFY-or-budget, no IDLE check. `ImapAccountConfig::push_session_capacity`:
the capacity rule itself. On a server without IDLE and `pool_cap >= 2`,
capabilities say "no push" while `push_lanes()` is non-empty and a direct
`push_subscribe` reports scopes in the succeeded lane. The engine reads lanes only
when the capability is not `None`, so it is protected; a direct consumer is not.
Owner: one `PushPlan { lanes, topology: Notify | PerFolder, capacity }` computed
at open, `None` when the server cannot IDLE, from which capabilities, lanes and
admission all read. The workspace-level form of the question is CON-004.
`supports_notify` is also stored twice, on `ImapAccountInner` and `PushState`.

## BUG-044 - Standalone CardDAV and CalDAV lifecycle streams end immediately, and the engine cycles them on backoff

**Reported by:** imap-account (found by comparison).

IMAP's `scope_lifecycle_stream` stays pending until shutdown, on the stated
ground that the engine treats an ended stream as a fault. CardDAV and CalDAV
return `stream::empty()`. The multiplexer's lifecycle reader confirms IMAP's
reading: on `None` it records a backoff failure, sleeps and re-subscribes, so a
standalone DAV account cycles its lifecycle reader on backoff for its whole life.
Inside an IMAP composition it is invisible, because IMAP does not merge the
sub-accounts' lifecycle streams. Owner: a shared
`bifrost_types::idle_lifecycle_stream(shutdown)` helper both use.

## BUG-045 - IMAP drops shared-namespace discovery failures and reports a degraded open twice

**Reported by:** imap-account, net-dav (adds that IMAP's `factory.rs` comment says both DAV crates sync exactly one collection and report the rest on `skipped_scopes`, while both DAV factories return an empty skip lane and `caldav::lib` says every collection is a cursor scope; `DavAttach::Degraded.transient` is computed and carried under `allow(dead_code)`).

`types.md` defines `OpenedAccount::skipped_scopes`, `ContainerList` and
`Page::skipped_scopes` for exactly this. IMAP: `discover_shared_folders` logs a
failed `LIST` under a namespace prefix, and a failed `NAMESPACE`, with
`tracing::debug!` only - no `SkippedScope` - and `containers_list` then returns
`ContainerList::complete(..)` with a comment saying the skips "were handled" at
open; they were dropped. `DavScopeIndex::build` collisions (a collection that
will never sync) go only to the prose warning lane, not to `skipped_scopes`. A
failed DAV open is reported twice: a prose `Warning` via a bespoke `dav_degraded:
Mutex<Vec<Warning>>` with peek / acknowledge-on-`Done` logic in
`discover_cursor_scopes`, and a `SkippedScope`; the bespoke lane predates the skip
lane and duplicates it, and `DavAttach::Degraded { transient }` is a dead field.
`classify_dav_open` encodes "transient vs needs config" as the choice between
`WarningKind::Other` and `OperatorAttentionNeeded` while discarding the
classified `AccountError` that decided it. The search half (one folder's failure
fails the whole page) is already an owner-ruling item in `notes/todo.md`.

## BUG-046 - Revoked shares on IMAP PIM, search and hydration paths escalate to account-wide loss; every quarantine helper drops the original evidence

**Reported by:** imap-account (live), graph.

The graph hunter reports the same four copies and adds two facts. All four
translate the error, test `kind == Authorization(PermissionDenied)`, and replace
the result with a freshly built `SyncState(ScopeRevoked)` - discarding the
original's status, request id, trace id, attempt cause and wire signal, so a
quarantined share exports no request id to support. And a fifth, different answer
sits in Graph's `scopes::discover_cursor_scopes_inner`: a foreign mailbox's
`PermissionDenied` during discovery becomes a skipped scope plus a
`Warning(OperatorAttentionNeeded)`, while every other error fails the whole
discovery, a primary-mailbox outage included. Its owner:
`AccountError::revoke_foreign_scope(self, scope, owner) -> AccountError` in
`bifrost-types`, keeping the original chain as the revoked error's cause.

Within IMAP, `error::shared_folder_error` is applied by each caller after it
looks up `folders.get(&folder).shared_owner` itself: `inventory::run_inventory`,
`get::run_folder_get`, `blob::run_fetch`, `mutate::flush_mutation_groups`,
`changes::select_error`, `poll::examine`. It is NOT applied by
`pim::copy_messages`, `delete_messages`, `set_flag`, `hydrate_decoded` (so
`set_is_read`, `set_keyword`, `set_importance`, `add/remove_from_container`,
`draft_discard`, `draft_send`'s discard, `message_hydrate`, `thread_hydrate`,
`move_thread`, `delete_thread`), `search`, `search_messages`,
`fetch_full_message`, or the push session's SELECT. On those paths a revoked
share's `NO [NOPERM]` derives terminal account-wide `NoPermission` - the
escalation the helper exists to prevent.

Across the workspace the same decision has four copies:
`imap::error::shared_folder_error`, `jmap::sync::error::shared_scope_error`,
`graph::graph_error::graph_shared_scope_error` and `ews_shared_scope_error`, each
with a sibling `*_scope_revoked` builder differing only in protocol stamp and
text. Owners at two levels: in IMAP, the SELECT is the chokepoint every lane
passes, so `ImapAccount::select_folder` (or a `SelectedFolder` checkout type it
returns) should carry the registry's ownership fact and return the
already-classified error; across crates, "PermissionDenied on a foreign-owned
scope derives DisableScope(scope)" is a recovery decision, and if `ErrorScope`
(or the builder) could carry "owned by another principal", `derive` could apply
it once and the four helpers collapse into a builder call.

## BUG-047 - An IMAP `Message::thread_id` cannot be redeemed by the same account's thread methods

**Reported by:** imap-account (live).

`fetch_to_inventory` (`inventory.rs`) and `fetch_to_message` (`pim.rs`) set
`thread_id` to the server's raw `THREADID` (RFC 8474) or the decimal X-GM-THRID;
`search` returns `Page<ThreadId>` minted by `encode_thread_id`
(`imapthread2:<len>:<folder>:<uidvalidity>:<uid,uid,..>`); `thread_hydrate`,
`move_thread`, `delete_thread` and every `MutationTarget::Thread` path
(`decoded_targets`) accept only the `imapthread` form. So `Message::thread_id` and
`InventoryEntry::thread_id` from this account fail `Request(Malformed)` when
handed back to its own thread methods. JMAP has one thread namespace. Either the
two need different types (a grouping key vs a redeemable handle) or IMAP mints one
redeemable form for both lanes; `bifrost-types` cannot currently express the
difference, which is why nothing caught it.

## BUG-048 - IMAP sync lanes ignore `UIDNOTSTICKY`

**Reported by:** imap-account.

Only the poller and its raw read check `uid_not_sticky`. Inventory and changes on
such a folder mint cursors the next SELECT voids, so the scope cycles
CursorInvalid -> re-inventory indefinitely. The poller's refusal (`Unsupported`,
folder-scoped) is the answer the sync lanes should share.

## BUG-049 - `describe_cursor` reports an incomplete QRESYNC cursor as cheap QRESYNC, which `run_changes` refuses

**Reported by:** imap-account (live).

`changes::describe_cursor` maps cursor tag plus `qresync_enabled()` to a strategy
and cost; `run_changes` makes the same mapping again to dispatch; and
`cursor_from_select` decides it a third time when minting. They agree because
`describe_cursor_reports_the_strategy_the_run_will_actually_use` holds two in step
- and that test misses the case where they already disagree: a `QResync {
known_uids_complete: false }` cursor is described as `Cheap / QResync`, while
`run_changes` refuses it with `CursorInvalid` before any wire work. Owner: a pure
`ChangePlan::for_cursor(&FolderCursor, policy)` returning the strategy (and the
`Refuse(incomplete baseline)` arm) both consume. The bool behind it is STR-076.

## BUG-050 - IMAP hydration and raw reads issue QRESYNC SELECTs and discard the payload

**Reported by:** imap-account.

Every caller of `select_folder` decides what to pass as the cursor: inventory,
mutation, poller, PIM primitives and search pass `None` (mutation with an explicit
comment that admission must not replay changes since the consumer's checkpoint).
`get::run_folder_get` and `blob::run_fetch` pass the registry's hint cursor, so a
hydration or raw read on a QRESYNC session issues a QRESYNC SELECT whose VANISHED
and changed-message payload is discarded, and the hint cursor is not advanced from
it: wasted wire on every hydration batch, and a policy the mutation path
explicitly rejected. Owner: a `SelectPurpose::{Sync(cursor), Read, Mutate}`
argument decided once per lane, replacing `(cursor, read_only)` (STR-076).

## BUG-051 - IMAP `open_raw_rfc822` keeps fetching after its receiver is dropped

**Reported by:** imap-account.

`guarded_receiver_stream` (abort on drop) is used by `get_stream`,
`inventory_stream` and `changes_stream`; `boxed_receiver_stream` (detached) by
`mutation_stream`, `open_raw_rfc822` and `scope_lifecycle_stream`. The rationale on
`guarded_receiver_stream` ("two drivers issue IMAP commands concurrently over one
account while the older one sat on a pool checkout") applies verbatim to
`open_raw_rfc822`, which can keep fetching a 256 MiB message for a receiver that is
gone. Mutation may legitimately want to run to completion, but nothing says so at
the call site. Owner: one spawn helper taking an explicit `OnDrop::{Abort,
RunToCompletion}`.

## BUG-052 - IMAP `bulk_move` to a DAV-owned folder goes to the IMAP server while `throttle_keys` says IMAP does not own it

**Reported by:** imap-account (live).

`Account::throttle_keys` documents that "the answer must follow the same routing
decision the operation itself makes". IMAP's `ThrottleWork::Membership(Folder)`
arm consults `dav_scopes` and declares no key for a DAV-owned folder; `bulk_move`
with that destination never consults `dav_scopes` (`validated_move_destination`
accepts any sendable name) and sends a `UID MOVE` to IMAP. The `Scope` arm also
normalizes through `MailboxName::from_account_id(..).account_id()` while the
`Membership` arm keys on raw `folder.0`. Root cause: `DavScopeIndex`'s doc says
"EVERY `CursorScope::Folder` consumer must consult this index" and records that a
fifth consumer once forgot; `route_scope` answers it, but `folder_from_scope` and
`MailboxName::from_account_id` turn any `FolderId` into a mailbox without
routing. Owner: a `RoutedScope` / `ImapFolder` type obtainable only from the
router, for `CursorScope` and `MembershipScope` alike, making the rule a compile
error. Smaller: `route_scope` produces a validated `MailboxName` that
`inventory_stream` and `changes_stream` discard (`Ok(ScopeHandler::Folder(_)) =>
{}`) and recompute with `folder_from_scope` inside the task.

## BUG-053 - Missing FLAGS hashes as "no flags" on IMAP Metadata and inventory, and an unknown projection hydrates as an empty body

**Reported by:** imap-account.

`get::run_folder_get` enforces the `bifrost-types` rule for
`Projection::FlagsOnly` (missing FLAGS -> `Failed(Protocol(MissingField))`).
`Projection::Metadata` and `inventory::fetch_to_inventory` carry the same flag
state as `Fingerprint::flags_hash`, and there a missing FLAGS silently becomes
`flags_hash(&[])`, a fingerprint claiming "no flags" - the rule enforced on one
sibling arm and not the other. `get::selection_for_projection` and
`get::fetch_to_hydrated` each match `Projection` independently and their wildcard
arms disagree: the selection asks only for FLAGS, the conversion emits
`RawMime(decode_body(..).unwrap_or_default())`, so a future projection variant
hydrates as a successful empty MIME body. One `match` producing both the
attributes and the converter closes it. Also `fetch_to_inventory` mints
`fetch.uid.unwrap_or_default()`, a UID 0 object id, because nothing in the type
says the caller already checked.

## BUG-055 - IMAP `SearchFilter::Has` (attachment filename or type) compiles to a full-text `BODY` search

**Reported by:** imap-account.

It returns messages that merely mention the string.

## BUG-056 - "Was this JMAP id answered?" is decided per response and per caller, and an omitted `Blob/copy` answer is terminal

**Reported by:** jmap-protocol (live; its second-ranked move).

RFC 8620 promises one answer per submitted id; servers omit. What an omission
means is decided by each response type in `core/` and again by each caller in
`sync/`:

| Site | Omitted id becomes |
|---|---|
| `SetResponse::created` / `updated` / `destroyed` | `Error::SetIdUnanswered(id)` |
| `CopyResponse::created` (`*/copy`) | `Error::SetIdUnanswered(id)` |
| `EmailImportResponse::created` | `Error::SetIdUnanswered(id)` |
| `BlobUploadResponse::created` | `SetIdUnanswered`; a `notCreated` entry becomes `Error::NotParsable(format!("Blob {id} not created: {error:?}"))` |
| `CopyBlobResponse::copied` (`Blob/copy`) | `Error::IdNotFound(id)` |
| `ParseResponse::parsed`, `EmailParseResponse::parsed` | `IdNotFound`, or `NotParsable(id)` |
| `GetResponse` | nothing: `notFound` defaults to empty, and the comment tells every caller to reconcile `list + notFound` against what it submitted |

`sync/error.rs` re-decides: `into_account_error` maps a bare `SetIdUnanswered` to
`set_id_unanswered("*/set", id, ctx, guarded = true)`, guessing both method and
guard because the error does not carry them; `set_answer_error(op, method,
guarded)` exists so callers can re-supply what the error lost, and a caller that
forgets gets the guess; `get_id_unanswered`, `get_id_unresolved_after_query`,
`require_created_id` / `created_id_unusable` and `convert_id_not_found` are the
per-lane classifiers, referenced at roughly forty call sites across `pim.rs`,
`contacts.rs`, `calendar_ops.rs`, `filters.rs`, `hydrate.rs`, `page_cursor.rs`,
`discover.rs`.

They already disagree. The `SetIdUnanswered` doc says the create maps of
"`*/copy` and `Blob/upload`" take the partial-response lane; `Blob/copy` does not:
an omitted blob becomes `IdNotFound`, which `convert_id_not_found` (no scope on
the context) turns into terminal `Protocol(MissingField)`, live in production
through `copy_attachment_blob` in `sync/pim.rs`, while sibling write lookups give
retryable `Protocol(PartialResponse)`. The same function reports a response
naming the wrong accounts as `NotParsable` prose (`ParseFailed`) rather than a
contract violation. A `Blob/upload` refusal (`overQuota`, `tooLarge`,
`rateLimit`) loses its `SetError` type and classifies as `Protocol(ParseFailed)`,
a provider-fault class for a quota refusal (latent: `BlobUploadRequest` has no
production caller).

Owner: one generic in `core/` every response's id lanes decode into
(`IdAnswers<K, T, E>` with `take(&K) -> Answer<T, E>`, `Answer = Ok(T) |
Refused(E) | NotFound | Unanswered`, method name and guard stamped on the request
side). Better, the `Target` permit move: the request builder hands out a
non-`Clone` permit per submitted id (`SetRequest::update` / `destroy` / `create`,
`GetRequest::ids`) and the response's only accessor is `settle(permit) ->
Answer`, making exactly-once accounting a compile-time property; the "reconcile
yourself" instruction on `GetResponse` disappears, and `SetIdUnanswered` carries
`{ method: &'static str, id, guarded }`, so `set_answer_error` and the `"*/set"`
guess go.

## BUG-057 - What an absent or zero JMAP core limit means is decided in eight places, with live disagreements

**Reported by:** jmap-protocol (live disagreement).

RFC 8620 section 2 core limits are read raw (`CoreCapabilities` fields are
`Option<usize>`) and interpreted per reader:

| Reader | absent | zero |
|---|---|---|
| `core/request.rs::CallLimit` | `Unadvertised`: no limit enforced | `Invalid`: not enforced |
| `sync/factory.rs::batched_open_probes` | "not a licence to batch": one call per request | (`max_calls < 2`) one call per request |
| `client_ws.rs::encode_ws_request` (`maxSizeRequest`) | not enforced | not enforced (`max > 0`) |
| `blob/upload.rs::upload_to` (`maxSizeUpload`) | not enforced | enforced (every non-empty upload refused) |
| `Session::api_request_concurrency` / `upload_concurrency` | 1 | 1 |
| `sync/capabilities.rs::validate_session` | refuses open (calls, get, set, size, concurrency, collations) | refuses calls/get/set/size; accepts upload and concurrency zero |
| `validate_session` again, after refusing | `.unwrap_or(1).max(1)` on get/set (dead defaults restating a third reading) | |
| `sync/capabilities.rs::upload_size_limit` | refuses | accepts |

With `maxCallsInRequest` omitted, `CallLimit` lets a request grow without bound
while `batched_open_probes` refuses to put two calls in one; `validate_session`
then refuses the account, so the first two answers are reachable only before
validation (the `CallLimit` doc says this is deliberate, because probes run
before validation). Open probes running against an unvalidated session is itself
why three of these readers exist. Owner: validate once, at
`SessionState::derive` (which already runs on every installed session),
producing a typed `CoreLimits` with `NonZeroUsize` where the RFC or the crate
requires positive and an explicit `Unbounded`/`Serial` reading where it does
not; every reader takes it. The session interpretation itself should move out of
the engine-facing layer: `validate_session`, `upload_size_limit`, the
"which capabilities are malformed and which matter" policy, and the `CoreLimits`
type live in `sync/capabilities.rs`, interleaved with the `AccountCapabilities`
mapping, though they are a reading of RFC 8620 section 2 and run after `Client`
already acted on the session unvalidated; they belong in `core/session.rs` as
`SessionState::derive -> ValidatedSession`, with `sync/capabilities.rs` keeping
only the mapping.

## BUG-058 - JMAP blob capabilities are read from the session level, which RFC 9404 leaves empty

**Reported by:** jmap-protocol (latent; no production reader yet).

Which capability URIs are read from `session.capabilities` and which from the
primary account's `accountCapabilities` is decided in four places:
`deserialize_session_capabilities_map` (core, websocket, quota, blob at session
level; mail, submission, sieve, contacts, calendars, principals, principals:owner
as account-level markers); `Session::capability_config` (its own copy of the
four-URI list); the `session_cap_accessor!` invocations
(`blob_capabilities` / `quota_capabilities` / `websocket_capabilities` /
`core_capabilities` read the session map) against six hand-written
`*_capability_state` methods reading the primary account, each repeating the same
"session-level Malformed check, then primary lookup" body; and
`core/capability.rs`'s trait doc ("`Config` describes the session object for Core
and WebSocket and the account object for account-scoped extensions"), which
disagrees with the code for quota and blob. RFC 9404 section 3 makes the
session-level `urn:ietf:params:jmap:blob` value an empty object and puts
`maxSizeBlobSet`, `maxDataSources`, `supportedTypeNames`,
`supportedDigestAlgorithms` in `accountCapabilities`, so
`Session::blob_capabilities` always returns all-defaults; any future
`Blob/upload` size gate would read `None`. The comment in
`deserialize_session_capabilities_map` states the rule the blob arm breaks. Owner:
the placement on the `Capability` trait (`const LEVEL: CapabilityLevel`) with the
`Capabilities` variant it decodes into, driving the deserializer,
`capability_config` and the accessors; one macro over the markers replaces the
six account accessors, and `capability_config`'s serialize-then-deserialize round
trip goes.

## BUG-061 - A JMAP create emits `null` inside an `Id[Boolean]` set, and a test pins it

**Reported by:** jmap-protocol (live wire defect).

The JSON-object macro (`json_object_struct!`) gives Create and Patch identical
setter vocabulary, so `CalendarEventCreate::calendar_id(id, false)` and
`ContactCardCreate::address_book_id(id, false)` put `{"cal-1": null}` into a
CREATE. An `Id[Boolean]` set in a create must contain only `true`; `null` is
patch-only syntax. `tests.rs::patch_object_null_semantics` pins this wire-invalid
create. Owner: a membership-set type that serializes differently in create
(`IdSet<T>`, values always `true`) and patch (path ops) - see CON-041 and
STR-084.

## BUG-064 - `Client::with_transport` fabricates a `Basic ` WebSocket credential; the JMAP credential is held twice

**Reported by:** jmap-protocol.

What credential a request presents: HTTP bearer is injected by `AccountNet` from
the token source; HTTP Basic is formatted by `ReqwestTransport::build_request`;
SSE (`ReqwestTransport::open_sse`) repeats the default-header loop and the Basic
branch of `build_request` line for line; WebSocket (`Client::connect_ws`) reads the
bearer source itself, formats the header, and runs its own one-shot 401
`refresh_if_unchanged` retry, with Basic through `Authorization::header_value`.
The `Authorization` value is held twice, inside `ReqwestTransport` (HTTP) and on
`ClientInner` (WS); `ClientBuilder::connect` clones one into both, but
`Client::with_transport` fabricates `Authorization::Basic(String::new())` for the
WS copy, so a client built that way presents `Basic ` on the WebSocket whatever
the transport uses for HTTP (reachable only by combining `with_transport` with
`connect_ws`). Owner: the transport owns the credential and exposes one
`authorize(builder)` for all three HTTP doors and a `ws_authorization()` for the
handshake; `ClientInner::authorization` is deleted.

## BUG-066 - JMAP container rename, move, delete and child-create on a shared mailbox are unroutable

**Reported by:** jmap-account (live, P2).

`containers_list` returns shared mailboxes with `ContainerRights` (`may_rename`,
`may_delete`, `may_create_child` from `myRights`), and
`pim_methods.container_rename / move / delete / create` are unconditionally
`true`, but the container doors run on `self.mail` with
`MailboxId::new(container.0)` - no routing, no stripping. Renaming, moving or
deleting a shared folder sends the qualified id to the primary account, which
answers `notFound`, and because the door attaches no scope that becomes
`Protocol(Unknown)` (`UnknownPermanent`). Creating a child under a shared parent
fails the same way. The flag-vs-gate duplication behind it is CON-006; the routing
type that would prevent it is STR-097.

## BUG-068 - The JMAP state cache advances from its cached state instead of `oldState`, laundering concurrent changes into the next guard

**Reported by:** jmap-account (latent, P2).

`state_cache::StateMap = Arc<Mutex<HashMap<String, Option<String>>>>`. The module
documents a tri-state (absent / probed-empty / known) and then `get` flattens it,
so no caller can observe the distinction it was built for. About fifteen writer
sites across `account.rs`, `changes.rs`, `discover.rs`, `mutation.rs` and `pim.rs`
each choose `set` vs `advance` and what to pass as `expected`. Unguarded writes
(`pim::send_email_set`, the unguarded branch of
`mutation::apply_batch_with_version`, `destroy_thread_emails_if_unchanged`) read
the cache, send the `/set` without `ifInState`, then `advance(expected = cached,
new = response.newState)`. If another writer changed the account between the
cached state and this set, the server's `newState` covers that change, and the
cache jumps over it; the cache is then used as the `ifInState` guard for the next
`FlagOp::Set` or move (`uses_if_in_state`), whose purpose is to refuse when an
unobserved concurrent change happened. `SetResponse::old_state()` exists and is
read nowhere in `sync/`. The correct rule: advance from `oldState` to `newState`,
and only if the cache currently equals `oldState`. Also `mailbox_states` is
write-only: container create/rename/move/delete, the Mailbox changes walk,
establishment and the factory maintain it, nothing reads it to decide anything
(establishment reads it only to CAS against itself; the lifecycle poller has its
own `lifecycle_mailbox_states`), and the shared accounts' entries are seeded for a
scope type (shared Mailbox) that does not exist. Owner: a `StateCell` per `(Slot,
ObjectType)` owning `observe_set(old, new)`, `observe_changes(since, new)`,
`observe_probe(s)`, `guard() -> Option<IfInState>`; delete `mailbox_states`.

## BUG-069 - JMAP page-cursor faults derive the account-wide schema-clear directive

**Reported by:** jmap-account (live, P2).

A malformed, retired or overflowing search / contact / calendar page cursor is
`SyncState(SchemaIncompatible)` (`pim::schema_incompatible_search_cursor`,
`page_cursor::cursor_error`), which derives
`Engine(EngineDirective::SchemaIncompatible)` unconditionally - the account-wide
durable-state clear - for a cursor the engine never holds.
`error::search_result_set_superseded`'s own comment explains why `CursorInvalid` is
wrong for page cursors (reserved for engine cursor scopes); the same reasoning
rules out `SchemaIncompatible`. A page-cursor fault is a caller-state fault and
wants its own kind or `Request(InvalidArgument)`. Also `result_set_superseded`
exists twice (`error.rs` for mail search, `PageKind` for contacts/calendars) with
the same classification, and `anchorNotFound` is decided twice: `convert_method`
maps it by the `anchored_query` flag plus a cursor scope (inventory), and
`PageKind::page_call_err` pre-intercepts it before `into_account_error` to map it
to `ConcurrencyConflict` (page walks), because the context cannot express
"anchored page walk" (STR-100). The two page-cursor codecs are STR-101.

## BUG-070 - The same JMAP `notFound` is success through `bulk_set_flags` and terminal through `set_keyword`; providers disagree on vanished targets

**Reported by:** jmap-account (live, P2).

`error::classify_set_item` turns a JMAP `notFound` / `blobNotFound` into
`Succeeded(Skipped)` for `UpdateFlags`, `BulkMove`, `BulkDestroy`, `SetKeyword`,
`SetIsRead`, `AddToContainer`, `RemoveFromContainer`. Graph absorbs a 404 as
`Skipped` only for destroy. So a flag update or a move of a vanished message is a
success on JMAP and `Failed(NotFound)` on Graph; a move that moved nothing
reported as success is questionable on its face. Inside JMAP the list is half
dead: only `mutation.rs` calls `classify_set_item`, with the three bulk
operations; `pim.rs`'s single doors route the same `notFound` through
`SetProgress::item -> set_answer_error -> into_account_error` with no item scope,
landing on `Protocol(Unknown)` (`UnknownPermanent`). Same wire answer, same
keyword patch: success via `bulk_set_flags`, terminal unknown error via
`set_keyword`. Owner: a per-`AccountOperation` "absent target" policy in
`bifrost-types`, applied by every provider. The read-back side of "is it gone" is
BUG-021.

## BUG-071 - JMAP partial-commit records name shared-account targets by native wire id; two `Email/set` pipelines

**Reported by:** jmap-account (live, P3).

`mutation.rs` (bulk doors) and `pim.rs` (single-target doors, threads, drafts,
send) both build and send `Email/set` for the same patches (keywords,
`mailboxIds`, destroy) and answer three questions differently. Guard:
`mutation::uses_if_in_state` guards `FlagOp::Set` and moves with the cached state
and retries once on `stateMismatch`; `pim::send_email_set` never guards (its
comment argues patches need no guard); `destroy_thread_emails_if_unchanged`
guards with a read state. Absent target: BUG-070. Outcome vocabulary: per-item
`ItemOutcome` lanes vs a `PartialCommit` record built by `SetProgress`.
`SetProgress::finish` names its targets with the WIRE id (`ErrorScope::Message {
id: EmailId::as_str() }`), which for a shared account is the native, unqualified
id, so a partial-commit record from `set_keyword` / `move_thread` /
`delete_thread` on a shared message names ids the consumer never saw and cannot
correlate - and the record is the only place the consumer learns which targets
committed. Proposal: one `mail_set.rs` executor shared by both (STR-104), and the
same question asked for IMAP in STR-080.

## BUG-072 - HTTP status -> error kind is decided by five ladders that already disagree

**Reported by:** jmap-account (live, P3), net-dav (live).

The net-dav hunter widened this to every HTTP crate. Sites: bifrost-net
`account_error.rs::status_kind_cause` (and, inside the same file,
`server_cause_from_status` answering a sub-question a second time: 408/502/503/504,
429, 507, 5xx); dav-core `error.rs::status_error` (two parallel if-chains, one for
the kind and one for the cause, held in step inside one function); Graph
`graph_error.rs::classify_by_status` (a near-copy of net's, down to the `(kind,
cause, bool)` tuple); Google `account/error.rs::classify_by_status`; JMAP
`sync/error.rs::convert_problem` (below).

| status | net | DAV | Graph | Google |
|---|---|---|---|---|
| 409 | ConcurrencyConflict | ConcurrencyConflict | ConcurrencyConflict | Server(Error 409) |
| 412 | Server(Error 412) | ConcurrencyConflict | ConcurrencyConflict | ConcurrencyConflict |
| 423 | Server(Error) | ConcurrencyConflict | Server(Error) | Server(Error) |
| 500 | Server(Error 500) | Server(Error 500) | Server(Error 500) | Server(Unavailable) |
| 507 | QuotaExhausted | QuotaExhausted | QuotaExhausted | Server(Unavailable) |
| 413 | Server(Error) | Server(Error) | Server(Error) | Request(Malformed) |
| 404, no scope | Server(Error 404) | NotFound(dialect resource) | Server(Error 404) | resource-dependent |
| 400/422 cause | `RequestCause::Malformed` | `RequestCause::InvalidArgument` | `Malformed` | `Malformed` |

Graph's own comment states the 412 split ("a bare 412 is `ConcurrencyConflict` here
and a bare `Server(Error)` in bifrost-net"). Every consumer falling through to net's
funnel - DAV's unreplayable histories and Graph's non-envelope bodies included -
gets net's table, so the same 412 is a conflict on one path and an unknown server
error on another inside one account. Owner: one classification function exported
by bifrost-net (or bifrost-types if IMAP-side callers want it), roughly
`classify_status(status, &StatusContext) -> StatusClass { kind, cause, throttles:
bool }`, with provider refinements passed in as a declared override set
(cursor-context 400/410, Gmail's history endpoint, DAV's 412/423, Graph's envelope
codes, JMAP's problem types) rather than copied tables. The scope projection beside
it is CON-044; throttle stamping is CON-061.

The jmap-account hunter's original account follows.

`error::convert_problem` classifies the HTTP status of a problem-details response
itself; `bifrost-net`'s `status_kind_cause` classifies the same statuses for every
other HTTP failure. 5xx: JMAP maps every 500-599 to `Server(Unavailable)`
(retryable), net maps only 502/503/504 to `Unavailable` and the rest to
`Server(Error{status})`. 507: net maps to `QuotaExhausted` with a throttle scope,
JMAP to `Unavailable` with none. 408: net maps to `Unavailable`, JMAP to
`Server(Error{408})`. An identical 500 from the same server is retried or not
depending on whether it carried an `application/problem+json` body. The
status-line precedent applies: net exports the status classification, and JMAP
overrides only the JMAP-typed problem types (`limit`, `unknownCapability`,
`notJSON`, `notRequest`).

## BUG-073 - Account crates classify the same cursor-decode failures differently; JMAP and Gmail are exactly inverted

**Reported by:** types (LATENT), jmap-account (live disagreement), graph, google.

The google hunter confirms the Google arm (`GmailCursorFailure::MalformedPayload`
-> `SyncState(CursorInvalid)` -> `RestartScope(Account)`) against Graph's and
JMAP's `SchemaIncompatible`, and proposes `OpaqueChangeState::decode::<T>(protocol,
version)` returning one typed `CursorDecodeError` with one mapping in the error
model. It also notes `changes.rs` re-implements
`decode_gmail_state_for_profile`'s identity check inline instead of calling it.

The graph hunter adds that Graph (`cursor::decode_cursor`) splits newer
(`EnvelopeUnknown`) from older (`SchemaIncompatible`) and then maps both to
`SchemaIncompatible` anyway, while IMAP, CalDAV and CardDAV use strict equality;
its owner is `OpaqueChangeState::open(expected_protocol, expected_version) ->
Result<&[u8], InnerEnvelopeMismatch>` with the one mapping onto `AccountError`.

The types hunter's sites: `imap/src/account/envelope.rs::decode_cursor`,
`carddav/src/account.rs::decode_cursor_snapshot`, `caldav/src/account.rs` (same
shape), `graph/src/account/cursor.rs::decode_cursor`,
`jmap/src/sync/state.rs::decode`, `google/src/account/cursor.rs::decode_gmail_state`.
It read them as converging on `SyncState(SchemaIncompatible)` by different routes:
Graph and JMAP split newer (`EnvelopeUnknown`) from older (`SchemaIncompatible`);
IMAP, DAV and Gmail do not distinguish direction; DAV merges protocol and version
into one message; IMAP stamps `AccountOperation::EstablishCursor` even when
decoding for `changes_stream`; Gmail mints its own message keys.

The jmap-account hunter found they do not converge:

| Failure | JMAP (`changes::stream`) | Gmail (`InvalidCursor`) | Graph (`cursor_error_to_account_error`) | IMAP (`envelope.rs`) |
|---|---|---|---|---|
| wrong `protocol` tag | `CursorInvalid` (restart scope) | `SchemaIncompatible` (account clear) | `SchemaIncompatible` | `SchemaIncompatible` |
| malformed payload bytes | `SchemaIncompatible` | `CursorInvalid` | `SchemaIncompatible` | `SchemaIncompatible` |

JMAP and Gmail are exactly inverted, each with a comment arguing its own choice.
The protocol-tag check is protocol-neutral (it compares
`OpaqueChangeState::protocol` with the account's own protocol), so four crates
should not answer it at all. Within JMAP the mapping is also built inline in
`changes::stream` rather than in `error.rs`, outside the module carrying
`#![warn(dead_code)]`.

Owner: `bifrost-types` (or the engine, which knows `account.protocol()` before
dispatch). The types hunter's shape: `OpaqueChangeState::payload_for(protocol,
version) -> Result<&[u8], PayloadMismatch { Protocol, Older, Newer }>` plus one
`AccountError` constructor, each crate supplying only operation and scope. The
jmap-account hunter's: a shared `CursorDecodeFault { ProtocolMismatch,
FutureVersion, RetiredVersion, Malformed, ScopeMismatch }` -> `AccountError`
mapping every crate calls. The interchangeable version integers beneath it are
STR-007.

## BUG-075 - Graph presents its Graph-audience token to EWS and Autodiscover

**Reported by:** graph (probable live; not observable hermetically).

`bifrost_net::TokenSource::current()` takes no resource or host.
`public_folder::ews_client` builds the EWS client over the Graph client's own
`AccountNet`, and Autodiscover posts through `execute_aux` with
`AuxTarget::Bearer` on that same net. Both present the Graph access token, whose
audience is `https://graph.microsoft.com`, to `outlook.office365.com`, which needs
an Outlook/EWS-audience token; Azure AD issues one audience per access token, and
the trait gives a consumer no way to answer differently per host. Unless
something outside the crate compensates, EWS streaming push, public-folder sync,
public contacts and calendars, and delegate Autodiscover cannot authenticate in
production; the scripted wire accepts any bearer. The axis belongs in net: either
`AccountSpec` carries a token source per trusted origin, or
`TokenSource::current(&Audience)`, with Graph constructing its Outlook-origin
requests with the Outlook audience. Related: STR-113 (cloud-aware origins).

## BUG-076 - A Graph PIM write leaves the pre-write etag cached, so the next bulk write conflicts with itself; four `$batch` reconcilers disagree

**Reported by:** graph (live).

"Given the submitted legs and the subresponses, what is each leg's outcome, and
what does the etag cache learn?" is answered by (1) `mutate::resolve_batch_responses`,
`reconcile_mutation_responses` and `update_etag_cache_from_responses` (bulk
mutation); (2) `get::reconcile_hydration_responses` (hydration), repeating the id
resolution verbatim, both warn strings included; (3)
`pim::messages::submit_write_batch_resource` (PIM writes); (4)
`reactions::classify_chunk`. On invalid input, 1 and 2 discard a subresponse whose
id is unparseable or out of range, while 3 dedupes by raw string, builds an
outcome for an unparseable id scoped `ErrorScope::Account`, and lets an
out-of-range numeric index into `BatchProgress::failed`. On etag maintenance, 1
evicts on 412 and 404, evicts on move or destroy success, and installs the new
etag on any other 2xx; 3 evicts only on destroy success.

Consequence: `set_is_read`, `set_category`, `set_importance` and
`set_extended_property` call `fetch_message_value`, which caches the pre-write
etag, then PATCH under `If-Match`; the PATCH succeeds and moves the change key,
and nothing refreshes or evicts the cached entry. The next `bulk_set_flags` on
that message finds the stale etag in `EtagIndex`, sends it, and gets a 412
`ConcurrencyConflict` against its own earlier write - a guaranteed wasted round
trip and a spurious conflict reported to the engine.

`EtagIndex` is written at five sites, each restating what an observation means:
`inventory_stream_from` (mail only; evicted on `@removed`), `changes_stream` (the
same rules, re-written), `get::fetch_batch` (every successful hydration body),
`mutate::update_etag_cache_from_responses` (the 412/404/move/destroy rules), and
`pim::messages::cache_etag_for` plus the destroy eviction in
`submit_write_batch_resource`. Owners: one `BatchAnswers::resolve(&legs,
responses)` (id resolution, header decoding, the absent-leg rule, etag
observation in one place; the four front ends only project outcomes - STR-108),
and `EtagIndex::observe(id, Observation)` with `Observation::{Live(etag), Gone,
Superseded}` called by each lane.

## BUG-077 - Graph shared mailboxes that differ only in case become two clients and two scope sets

**Reported by:** graph (live; latent in the retag half).

"Which mailbox is this" and "are these two the same mailbox" are answered by
`GraphAccount::shared_clients: HashMap<String, GraphClient>` and
`client_for_owner` (exact byte equality), `autodiscover::merge_shared_mailboxes`
(exact equality), `GraphClient::concurrency_key` (`eq_ignore_ascii_case` against
the primary address, then Unicode `to_lowercase`), and
`GraphClient::mailbox_budget` (Unicode `to_lowercase`). If
`with_shared_mailbox("Shared@contoso.com")` is configured and delegate
Autodiscover returns `shared@contoso.com`, the account gets two shared clients,
two complete sets of discovered folder scopes for one mailbox (duplicate sync,
duplicate ids differing only in owner-tag case), and one throttle budget for both.
Also `ClientInner::mailbox_id: Option<String>` (`None` meaning `/me`) is `enum
MailboxRoute { Primary, Shared(MailboxKey) }`; and `primary_account` is
first-write-wins (`fix_primary_account`) while `mailbox_budget`'s fallback reads
`account_net().account()`, which `retag` changes, so after a retag on the
`with_account_net` path the concurrency key and the throttle budget name different
accounts. Owner: a `MailboxKey` newtype with case-insensitive `Eq`/`Hash`, minted
once at configuration and discovery, and one `MailboxIdentity` per client yielding
both the concurrency key and the throttle budget.

## BUG-078 - A default-constructed Graph account advertises webhook push with no lane and refuses every subscribe

**Reported by:** graph (live).

`GraphAccountFactory::new` defaults to `PushMode::GraphSubscriptions` with
`push_endpoint: None`. `capabilities::build_capabilities` reads `push_mode` only
and advertises `PushCapability::WebhookOrEwsStream`; `push_stream::push_lanes`
reads mode and endpoint and declares no lane; `webhook::subscribe_graph` reads the
endpoint and returns `Unsupported(PushSubscribe)`. Owner: one value, `enum
PushConfig { None, Webhook(PushEndpoint), EwsStreaming }`, with capability, lanes
and subscribe all functions of it and `None` mapping to `PushCapability::None`;
the public builders (`with_push_endpoint_client_state`, `with_ews_streaming`) keep
their names. Related naming smell: `PushCapability::WebhookOrEwsStream` is
documented as Graph webhooks only, and Graph's EWS streaming maps to `InProcess`,
so "OrEwsStream" in the published name is false; renaming needs the owner.
The workspace-level form is CON-004.

## BUG-079 - Graph reads back an unrouted public-folder item as deleted

**Reported by:** graph (live).

An `ObjectId` of the public-item shape whose folder is not in `routing_map` (which
starts empty on each reopen, and discovery may skip a folder) gets three answers:
`get::get_stream`, through `partition_ews_ids`, sends it down the REST arm, where
Graph answers 404 and the item is reported `NotFound(Message)` (pinned as
deliberate by `an_unrouted_public_folder_id_hydrates_through_the_rest_arm_url`);
`pim::hydrate::public_message_hydrate` and `get::fetch_ews_outcomes` answer
`stale_local_state_error` (`ConcurrencyConflict`, `Retry(AfterStateRefresh)`) - the
branch in `fetch_ews_outcomes` unreachable because `partition_ews_ids` already
diverted every unrouted id to REST; `mutate::submit_batch_with_version` answers
`Unsupported`, its comment saying that sending such an id to REST produces "false
claims that the item is gone", which is exactly what `get_stream` does on purpose.
`get_stream` is the engine's read-back path, so a read-back of an unrouted public
item reports it deleted (compare BUG-021). "Is this a public folder?" is itself
answered four ways: presence in `routing_map` (`establish_initial_cursor`,
`inventory_stream`, `partition_ews_ids`); the decoded cursor kind
(`changes_stream`); the bare `CursorScope::Folder` variant (`budget_for_scope`,
`push_subscribe`); `routing_map` or `public_folder_meta` (`is_known_public_folder`).
Owner: a typed internal scope whose `PublicFolder` variant is produced once from
discovery state (STR-106), plus one `PublicItemRoute::resolve(id) -> Routed |
Unrouted` every lane matches on, with one classification for `Unrouted`.

## BUG-080 - A Graph delta value without an id gets two verdicts, one of them `Unsupported`

**Reported by:** graph (minor misclassification plus a lane disagreement).

On the same server input, `inventory_stream_from` records an
`InventoryObligation::Region` checkpoint barrier and continues the walk, with the
obligation's error `unsupported_account_error(SyncInventory)` claiming the
operation is unsupported - it should be the contract violation it is - while
`changes::changes_stream` terminates the scope with a terminal
`Protocol(ContractViolation)`. The `changes_stream` comment explains the changes
lane has no obligation vocabulary, a fact about `SyncEvent<Change>`, not a reason
for different verdicts on the provider. Owner: the shared delta-walk driver
(STR-116), deciding once and letting each lane express the verdict.

## BUG-083 - Graph's funnel tags a 503 with a mailbox budget that the classifier then never stamps

**Reported by:** graph (live disagreement; comment contradicts code).

"Is this status a throttle?": `GraphClient::tag_throttle` treats 429, 503 and 507
as throttle-shaped and wraps the failure in `MailboxThrottled { budget }`;
`graph_error::classify_by_status` returns throttle `true` for 429 and 507 and
`false` for 503 (`Server(Unavailable)`); `stamp_budget`, on the net-classified
path, decides from the kind (`RateLimited | QuotaExhausted`). A 503 is tagged at
the funnel and never stamped. The comment in `response_to_account_error` claims
the annotation "applies only on rate-limit / quota / 503 paths". Graph's
classifiers also return `(AccountErrorKind, Cause, bool)` where the bool means
"this is a throttle" - a function of the kind that `stamp_budget` re-derives
independently. Owner: the classifier; the funnel carries the budget
unconditionally as context, and the throttle decision is made once from the kind.

## BUG-086 - Google stamps Calendar, People and Drive errors as Gmail, and names the wrong OAuth scope for them

**Reported by:** google (live; its headline), net-dav (independently flagged `execute_builder`'s string service with its `_ => ApiSurface::Drive` default, and asked that it take `ApiSurface`).

The crate is four Google API clients (Gmail, People, Calendar, Drive) sharing one
handle named after one of them, `GmailClient`, and nothing in the type system says
which API a request belongs to. The crate rebuilds that fact three times from three
inputs: `client.rs::surface_for_url` (longest-prefix match over three bases, with
`Drive` the catch-all for anything matching none); `client.rs::execute_builder`,
mapping a `&str` service name (`"Gmail API" | "People API" | "Calendar API" | _`)
and sending anything unrecognized to `Drive`, so a typo in `"Gmail API"` silently
drops the concurrency limit and the project hold; and
`account/error.rs::GmailErrorContext.surface`, defaulting to `Gmail` in `base()`,
overridden in the `calendar_*` and `contact*` constructors, set by hand in
`cloud.rs::ctx()`. `quota.rs::ApiSurface` is the right enum, but nothing carries it
on a request. The error model then forgets it: `parse_json_response` and
`check_response_status` take a `_service: &str` and ignore it, and `GmailService`
has one variant, `GmailApi`, so Calendar, People and Drive errors go out as
`Provider::Gmail` / `Protocol::Gmail` / `WireCause::Gmail(..)`. `classify_response`
treats every envelope as Gmail's (`FailedPrecondition` outside the history endpoint
maps to `ConcurrencyConflict`; `PubSubSubscriptionDeleted` is matched on
`operation == PushSubscribe`), though People reports `google.rpc` statuses and
Calendar uses its own reason set. Graph distinguishes `Protocol::Graph` from
`Protocol::Ews`; Google stamps one protocol for four. And `gmail_scope_for` is keyed
on `AccountOperation` with a `_ => "gmail.modify"` catch-all, so an
`insufficientPermissions` from Calendar or People tells the consumer to request
`gmail.modify`.

Whether a response may stamp the Gmail project budget is decided twice - at
observation time keyed on the URL's surface (`send_recorded`), and at translation
time keyed on the context's surface (`translate_response`, and
`translate_net_error`'s `LocalHold` arm) - held in step by
`quota::tests::check_surface`, which forges headers to prove the context-side copy
also refuses: a pairwise-agreement test over two copies. Owner: a typed handle per
API (STR-121), carrying the surface on every request and in its errors' type; each
API gets its own classifier feeding one builder funnel.

## BUG-087 - Google Calendar and Drive traffic debits the Gmail per-user quota bucket

**Reported by:** google (live in production), net-dav (confirms, noting the comment says Calendar bills per request while each call debits one Gmail unit from the per-user bucket).

`default_account_net` registers one rate-limit bucket per distinct host. In
production Gmail, Calendar and Drive all live on `www.googleapis.com`, so Calendar
"rides the Gmail registration", and Drive does too without mention. The
registration comment says Calendar "takes the general Google bucket", but in
production that bucket is the Gmail quota-unit bucket (100 units/s,
`cost_default` 1), so Calendar and Drive traffic debit Gmail's per-user budget and a
large Gmail hydration delays calendar reads. bifrost-net already supports two quota
scopes on one host (`RequestBuilder::quota_scope`), and `reference/net.md` names the
Gmail per-project vs per-user case as the reason it exists. Owner: the typed surface
handle (STR-121) stamps `quota_scope(surface)` on every request, and registration
declares one bucket per surface rather than per host. Two request funnels also
decide cost and concurrency independently: `execute_if_match` applies the cost from
`gmail_quota_cost` and the concurrency limit unless the path is `/watch` or
`/stop`; `execute_builder` applies the concurrency limit only for the `"Gmail API"`
string and applies NO cost (its comment tells callers to add it themselves);
`mutation::post_empty_json` builds through the raw funnel only to add the cost back
by hand, though `client.post_no_content(path, body, replay)` already does exactly
that. The hunter proposes deleting the second funnel for every Gmail caller; only
Drive's resumable upload (custom headers, bearer-less, redirects off) needs the raw
builder.

## BUG-089 - A Google filter `MoveTo(archive)` sends the synthetic archive id in `addLabelIds`

**Reported by:** google (live, untested).

`filters::action_from_filter_actions` lowers `MoveTo(c)` to "add c, remove INBOX"
and `Delete` to "add TRASH, remove INBOX", never consulting
`flags::move_placement_patch`. `flags.rs` documents that Gmail rejects a modify
adding the synthetic `archive` id, and `MoveTo("INBOX")` emits INBOX in both add and
remove; neither case is tested. Meanwhile `move_placement_patch` is shared, its
comment saying the bulk and single-object paths "agree on the wire shape", but the
source-folding rule (drop a redundant source if it is archive, equal to the
destination, or already among the removals) is copied verbatim in
`mutation::move_patch` and `pim::move_container_patch`. Owner: move the source fold
into `flags` (`move_placement_patch(destination, source)`) and route filter moves
through it.

## BUG-091 - Google label vocabulary disagrees on case, archive and label type

**Reported by:** google (live).

System label ids are declared three times as `const`s in `flags.rs`, `pim.rs` and
`filters.rs`, each a different subset, with `ARCHIVE_ID` in both `flags.rs` and
`filters.rs`. The facts about a system label are spread across files: exclusive
display container (`flags::EXCLUSIVE_CONTAINERS`), read-only projection
(`flags::is_read_only_label`), maps-to-a-flag (`canonical_flags_indexed`,
`flag_to_add_label`, `flag_to_remove_label`, `gmail_flag_target`, and the
hard-coded `pim::set_is_read`), hidden from flags (`canonical_flags_indexed`'s
inline skip list, which alone includes `"CHAT"`), folder role
(`pim::container_from_label`), search operator (`pim::container_query` and
`filters::container_query`), and system or user (`GmailLabel.label_type:
Option<String>` compared against `"user"` / `"system"`). Disagreements: CASE -
`flags.rs` and `pim::delete_thread` compare label ids ASCII-case-insensitively,
while `pim::container_from_label` and both `container_query` copies match
`LABEL_INBOX` etc. exactly. ARCHIVE - `pim::container_query` tests archive with
`is_archive_id` (case-insensitive), `filters::container_query` tests `id ==
ARCHIVE_ID`, so `SearchFilter::In("Archive")` becomes the archive expression while
`FilterCondition::InContainer("Archive")` becomes `label:Archive`. LABEL TYPE -
`flags::patch_for_set` and `user_label_id_from_flag` treat only `Some("user")` as a
user label; `scopes::diff_snapshots` emits renames for anything not `Some("system")`
(counting `None` as user); `pim::label_is_system` requires `Some("system")`. Owner:
`GmailLabelId` (STR-125). The query encoders behind the archive split are CON-053.

## BUG-094 - A Gmail Pub/Sub registration loss is classified as a cursor loss

**Reported by:** google (needs a ruling: a classification chosen for lack of vocabulary).

`pubsubSubscriptionDeleted` / `Expired` under `PushSubscribe` map to
`SyncState(CursorInvalid)`, deriving `RestartScope(Account)`; the context comment
says this is deliberate. But a lost watch costs only a gap, which push already
signals (`gap_hint`); the history cursor is still valid. Restarting the scope throws
away a good cursor because the error model has no push-registration kind.

## BUG-095 - Non-UTF-8 bodies vanish in Google `message_hydrate`, reported complete

**Reported by:** google (live).

`collect_body_parts` decodes Gmail `body.data` with `String::from_utf8` and drops
anything that fails, so a Latin-1 or ISO-2022-JP text part yields `body_text: None`
with `incomplete: false`. This is a Gmail-only re-implementation of "which part is
the body" (first `text/plain`, first `text/html`, `String::from_utf8` or drop)
beside `bifrost_types::mime::select_body`, which handles charsets; it also always
sets `Message::incomplete: false`. Owner: hydrate over the shared MIME selector
(STR-130).

## BUG-096 - A transient Gmail `labels.list` failure terminates discovery despite a usable cache

**Reported by:** google.

`scopes::labels_for_flags` and `scopes::scope_snapshot_or_refresh` are the same
single-flight staleness check with different failure policies: the first returns the
stale vocabulary when a refresh fails, the second propagates the error.
`discover_memberships` uses the second. Separately, the lifecycle poller
(`scope_lifecycle_stream_with_baseline`) fetches `labels.list` every 30s into its own
baseline and never feeds `ScopeCache`, so two label views run side by side: the
lifecycle stream can announce `Created(Label(x))` while flag translation still lacks
`x` and treats it as unsupported until the next refresh, costing a second
`labels.list` round trip the poll already paid for. Owner: one vocabulary actor that
the poller refreshes, with lifecycle diffs a view over its snapshots.

## BUG-098 - Google resolves retry hints against the injected clock on one path and the real clock on others

**Reported by:** google.

The wall clock is injectable (`GmailClient::wall_clock`) and the observer uses it,
but `GmailResponseHeaders::from_headers` calls `SystemTime::now()` and is used by
`parse_json_response`, `check_response_status`, `post_empty_json`,
`cloud::response_error` and the refine path in `account/error.rs`, so `retry_at` is
resolved against the injected clock on one path and the real clock on the others.
Thread the client's clock, or resolve retry hints only in the observer.

## BUG-099 - IMAP and ManageSieve turn every OAuth token-source failure into server-acknowledged `AuthLost`

**Reported by:** smtp-sasl (live; its top-ranked finding).

SMTP `oauth_token_error` projects the `bifrost_net::Error` through
`into_account_error_with_throttle_scope` and tunnels the typed result with
`with_preclassified_account_error`, explicitly so a transient refresh failure is not
flattened into a terminal one. IMAP `authenticate_best` maps it to
`Error::auth_with_mechanism(format!("failed to read OAuth access token: {e}"), None,
..)`; `classify_auth` with no response code yields
`Authentication(ReauthorizationRequired)` and stamps `TransmissionState::Acknowledged`
("auth failures are server-acknowledged tagged responses"). `account/sieve.rs` does
the same via `Error::auth_with_code(.., None)`. So on IMAP a token endpoint outage, a
`RefreshFailed { Network }`, or the `Cancelled` lost-driver case `reference/net.md`
says must NOT be `AuthLost` all become terminal `AuthLost` with false "the server
answered" evidence, sending the user to re-authorize an account whose token is
fine - net.md records fixing exactly this in bifrost-net; the IMAP consumer
reintroduces it one layer up. Owner: IMAP gets an `Error` variant carrying a
preclassified `AccountError` (SMTP's tunnel) or a typed token failure, and the
decision is made once - e.g. a bifrost-net helper `token_failure_account_error(error,
protocol, operation)` for IMAP, ManageSieve and SMTP. Related smell: SMTP's
`oauth2_token_blocking` turns `TokenSnapshot::Contended` into a fabricated
`bifrost_net::Error::RefreshFailed { source: Network { "OAuth token cache is busy" }
}` purely to obtain a transient classification; the classification of a snapshot
state belongs on `TokenSnapshot` (or in net's projection).

## BUG-100 - A SCRAM server error other than `invalid-proof` is `AuthLost` on IMAP and `PolicyBlocked` on SMTP

**Reported by:** smtp-sasl (drift).

SMTP `From<SaslError>`: `invalid-proof` -> `ErrorKind::Authentication` (AuthLost);
any other value -> `InvalidInput` + Auth phase -> `Authorization(PolicyBlocked)`,
pinned by `a_scram_policy_error_carries_its_rung_through_the_policy_lane`. IMAP
`From<SaslError>` and `AuthenticateScramConsumer::named_sasl_error`: every
`AuthFailed` -> `Error::Auth` -> `ReauthorizationRequired` -> AuthLost. So
`e=channel-binding-not-supported` is a policy block on SMTP and "re-enter your
password" on IMAP, for the same server and credential. IMAP's comment says it
"mirrors `bifrost_smtp`'s `named_sasl_error`; the two crates must not differ" - they
differ in the class and agree only on the message suffix. Root cause: the server
error is prose with a magic prefix (STR-133). Owner: bifrost-sasl, via a typed
`ScramServerError` and one `is_credential_rejection()` / `refusal_class()`.

## BUG-101 - The OAuth error-continuation answer is wrong for one mechanism in each crate

**Reported by:** smtp-sasl (drift).

After a failed XOAUTH2 / OAUTHBEARER the server sends a continuation carrying a JSON
error. IMAP `AuthenticateXoauth2Consumer` (shared by both mechanisms) answers with
an empty line; SMTP `Mechanism::response_with_token_at_index` answers `\x01`
(base64 `AQ==`) for both. RFC 7628 section 3.2.3 has the OAUTHBEARER client answer
with `%x01`; Google's XOAUTH2 documentation has the client send an empty response.
Each crate is right for one mechanism and wrong for the other. The OAuth-payload
consolidation explicitly left "the RFC 7628 `\x01` error-continuation handling" in
SMTP as "protocol command flow"; it is a per-mechanism SASL rule and belongs in
bifrost-sasl (`oauth_error_ack(mechanism) -> &'static [u8]`).

## BUG-104 - The reverse path depends on which door sends the message

**Reported by:** smtp-sasl (drift).

Given RFC 5322 headers, which address is the reverse path: bifrost-smtp
`Envelope::try_from(&Headers)` lets `Sender` win over `From` (pinned by
`envelope_from_headers_sender`); IMAP `pim.rs::parse_draft_for_submission`
(draft_send, send_raw_message) takes first `From`, else first `Sender`, else the
configured default; bifrost-types `mime::render` (send_message) uses the resolved
`from` (request or default) and does not model `Sender`. The same raw message sent
through bifrost-smtp's `Transport::send` and through the IMAP account's
`send_raw_message` gets a different MAIL FROM; RFC 5322 makes `Sender` the agent
responsible for transmission, the conventional bounce target, and IMAP chose the
opposite without saying why. Owner: one `HeaderMap::submission_envelope()` in
bifrost-types used by IMAP and by `Envelope::try_from`. Adjacent: when a draft has
no Message-ID, `parse_draft_for_submission` falls back to the constant
`bifrost.draft@{domain}`, so every such draft from one domain gets the same id,
which then becomes the "stable fallback id" on the receipt
(`append_to_sent_or_fallback`) - not unique by construction.

## BUG-105 - SMTP `SendProgress` phase flags are written and never read; "positive" accepts a 3yz final reply

**Reported by:** smtp-sasl (defect).

`SendProgress { body_started: bool, body_finished: bool, data_response:
Option<Response> }`: `core.rs` calls `set_body_started` / `set_body_finished`, and
nothing in production reads either. `resolve` decides every `Accepted` recipient's
lane from `data_response` alone - `Some(positive)` succeeded, `Some(negative)`
failed, `None` uncertain with an `InFlight` partial-completion error - and that last
arm is correct only when the body may have left, so a pre-body exit depends on the
machine having already called `mark_unresolved_unsent`. The flags look meant to
gate exactly that default and never got wired; the batch tests set them, so the
suite reads as though they matter. Also `Response::is_positive` (and
`Code::is_positive`) is true for 2yz and 3yz and is used for "DATA accepted" (wants
354), "message accepted" (wants 250), "RSET acknowledged"
(`Epilogue::ResetReading`) and, in `resolve`, "the recipient's message was
delivered" - so a 3yz final reply after end-of-data resolves as delivered. Owner: a
transaction-phase enum the machine advances and `resolve` matches (`Envelope ->
DataIssued -> BodySent -> FinalReply(Response)`, `Envelope` -> `Unsent`, `BodySent`
-> `Uncertain`), and `Response::is_completion()` / `is_intermediate()` (or an
`expect(ReplyClass)` at each read). The hunter asks that the phase enum be audited
for bite: pre-body exits should stop depending on a separate `mark_*` call.

## BUG-106 - bifrost-smtp's `account-error` feature cannot be turned off

**Reported by:** smtp-sasl (defect: a promise to consumers the build does not keep).

`Cargo.toml` makes `bifrost-types` optional behind `account-error` (a default
feature), but nothing in `src/` is gated on it, and `account_error.rs`, `batch.rs`,
`error.rs`, `authentication.rs`, `client/core.rs`, `client/envelope.rs`,
`client/connection.rs`, `client/async_connection.rs`, `transport.rs` and
`async_transport.rs` all name `bifrost_types::` unconditionally, so
`--no-default-features` cannot compile. No `[[check]]` sweep builds bifrost-smtp
without default features (`smtp-dkim` adds a feature; `install-shape` uses
defaults). `SmtpTransmissionState` (+ `to_types_state`) duplicates
`bifrost_types::TransmissionState` and only makes sense if the dependency were truly
optional. Either make the dependency unconditional (dropping a published feature
name needs the owner, and `SmtpTransmissionState` is replaced by the types enum) or
actually gate the account surface and add a no-default sweep. Smaller:
`Credentials::oauth2_token` carries `#[cfg_attr(not(feature = "tokio"),
allow(dead_code))]` to keep an async-only helper compiled in blocking builds, and
`test-support` exposes `ScriptedConnector`/`Transcript`, usable only by the async
executor path, so a downstream crate cannot script the published blocking
transport.

## BUG-107 - SMTP default ports disagree between code, docs and IMAP

**Reported by:** smtp-sasl (drift).

`SmtpTransport::relay` sets 465, `starttls_relay` 587, `builder_dangerous` 25 (SMTP)
/ 24 (LMTP). `connection_url::from_connection_url` gives `smtp` with no `tls`
`SMTP_PORT` (25), while the doc table on `SmtpTransport::from_url` (and its async
twin) says that row defaults to 587. IMAP `SubmissionTls::default_port` gives
Implicit 465, StartTls 587, Plaintext 587, and then always calls `.port(port)` over
whatever `relay` / `starttls_relay` / `builder_dangerous` already chose. Owner: one
`default_port(dialect, tls_mode)` in bifrost-smtp consumed by the builders, the URL
parser and IMAP.

## BUG-108 - SMTP configuration errors are classified as retryable transport failures

**Reported by:** smtp-sasl (drift with bifrost-net).

`from_connection_url` reports a malformed URL, a missing host, an unknown scheme and
a bad percent-encoding as `error::connection(..)`, which `into_account_error` maps
to `Transport(Network)` - retryable, so a config typo is retried forever as a
network outage. `TlsParametersBuilder::build`, `Certificate::from_pem/from_der` and
`Identity::from_pem` map native-tls parse failures to `error::tls`, i.e.
`Transport(Tls)`, while bifrost-net classifies the identical failure (corrupt root
cert DER at client construction) as `InvalidRequest` -> `Request(Malformed)`, "not
as a retryable transport setup failure" (`reference/net.md`). And IMAP
`SubmissionTransport::build_with_connector` maps any bifrost-smtp builder error to
`crate::Error::InvalidInput(e.to_string())`, discarding the kind entirely.

## BUG-110 - "May this logical request have committed?" is decided four ways in net and DAV, and they disagree

**Reported by:** net-dav.

Given the attempts so far, what is the effective transmission state, and is replay
forbidden? The `request.rs` send loop sets `may_have_committed |= !replayable &&
status != 401`, sets `unreplayable_terminal` via `left_the_process`, and decides
"proven refusal" in `status_error_is_replayable` (401 and 429 only, with a comment
that 502/503/504 can follow a commit). `account_error.rs`'s `AttemptHistory` arm
re-derives everything from the translated cause chains: it excludes 401s
(`rejected_unauthorized`), computes `acknowledged_may_have_committed`, and
reclassifies to `PartialResponse` when `inflight || (acknowledged && terminal ==
Some(Unsent))`. Its `LocalHold` arm folds the same states differently, with no 401
exclusion. dav-core's `RedirectHistory::record_response` applies its own
`unreplayable && status != 401` rule, and `attach` reclassifies to
`PartialResponse`/`InFlight` when `unreplayable && may_have_committed` and the
terminal is `None | Some(Unsent)`. dav-core `wire_outcome`'s `refused_only` treats
earlier attempts of 401, 429, 503 and 501 as proven refusals. Disagreements: net
reclassifies on "acknowledged earlier attempt + `Unsent` terminal" even for a
replayable request, DAV only for an unreplayable one; on a terminal with no attempt
evidence (`None`, e.g. a credential error) DAV reclassifies while net pushes an
Acknowledged attempt and keeps the terminal's kind; DAV's refusal set includes 503,
which net's own comment says does not prove refusal (dead today for unreplayable
requests, because net never retries past a 503 on them - a second rule waiting to
drift). Owner: a public `AttemptLedger` in bifrost-net recording typed outcomes,
owning one `is_proven_refusal(StatusCode)` predicate and exposing a verdict
(`CommitEvidence::{Unsent, Acknowledged, MayHaveCommitted}`); net's loop and any
self-walking caller (DAV) feed it, and `account_error` reads the verdict rather than
re-mining `Cause::Attempt` entries out of translated errors. The evidence fields it
replaces are STR-144; the second walker is CON-060.

## BUG-111 - DAV bearer token sources are never coordinated

**Reported by:** net-dav (likely live).

`Net::attach_account` wraps any non-coordinating source in an `OAuthRefresher`, and
JMAP's `Authorization::from_credentials` does the same on its own. DAV does neither:
`dav_account_spec()` attaches with `AccountSpec::new(None)`, and
`DavCredentials::Bearer { token_source }` is the consumer's raw source; `mint_auth`
calls `token_source.current()` on every request, and the 401 path calls
`refresh_if_unchanged`, whose trait default is a plain `refresh()`. A raw provider
`TokenSource` passed to `CalDavCredentials::bearer_source` or
`CardDavCredentials::bearer_source` gets none of the single-flight,
proactive-refresh, max-age or failure-backoff guarantees the reference promises
every other HTTP crate. The hunter did not check whether IMAP XOAUTH2 or SMTP have
the same gap. Type fix: a `CoordinatedTokenSource` newtype mintable only by
`coordinate(Arc<dyn TokenSource>)`, which wraps when needed, held by `AccountSpec`,
JMAP and DAV, making `is_coordinating()` unnecessary; DAV can keep minting headers
itself through `AccountNet::bearer()` if it attaches the source to its spec.

## BUG-112 - Two DAV hrefs naming one resource both land on a page

**Reported by:** net-dav (live).

Are two hrefs the same resource, and in what order do they sort? `dav_href_key` /
`same_dav_url` (decoded path segments, host case, default port) serve the snapshot
diff, `DirectoryGuard` and relocation checks; `sorted_candidate_hrefs` sorts and
dedups on raw strings; `slice_after_watermark` compares raw strings with `>`
against the watermark; `diff_snapshots` uses raw `uri ==` for tie-breaking inside
one key bucket; `url_origin` is a string origin for the credential gate; net
`redirect::same_origin` works on `Url`; `DirectoryGuard::contains_url` is a
hand-rolled ancestor walk; and `DavDispatch::resolve_url` vs `xml::resolve_href`
join relative hrefs differently (with and without a trailing slash on the base).
Consequence: two spellings of one resource a query returns survive
`sorted_candidate_hrefs` and both land on a page; watermark ordering is on raw bytes
while identity is on the key. Type fix: a parsed `DavHref` with its key and order
derived once, and an `Origin` newtype in bifrost-net both redirect walkers and the
DAV gate compare on (STR-146).

## BUG-113 - One unparseable multistatus status line gets two verdicts in two DAV lanes

**Reported by:** net-dav.

`ResponseParts` exposes `entry_href`, `failed_href`, `failed_member`, `fetched`,
`failed_resource`, `missing_data_href` and `member_status_code`, each recomputing
the lane from the same flags. For a response whose only propstat failed with an
unparseable status line: the listing lane's `failed_member` returns a member with
`status: None`, which `classify_207` reads as "not a missing resource", so the
input can drive a `CompleteFailure`; the multiget lane's `failed_resource` returns
`None`, so the same response falls into `missing_data_href`, the benign lane (the
"accepted edge" comment on `failed_resource` documents this for multiget only).
Owner: `ResponseParts::lane() -> ResponseLane { Collection, Entry, Failed { status
}, MissingData }`, computed once, every accessor reading it.

## BUG-116 - The HTTP bandwidth meter cannot be handed to the engine or IMAP

**Reported by:** net-dav (likely live wiring defect; verify before acting).

The bandwidth registry can be injected (`Net::with_bandwidth_registry`), but the
meter cannot: `Net::new` always builds a private one, `Net::meter()` returns
`&BandwidthMeter`, and `BandwidthMeter` is not `Clone`, while
`SyncEngineBuilder::with_bandwidth_meter`, `ImapAccountFactory::with_bandwidth_meter`
and `MeterSinkHandle::from_meter` all require an `Arc<BandwidthMeter>`. As far as the
hunter can see, no consumer can hand the engine (or IMAP) the meter HTTP traffic is
recorded into, so `Control::bandwidth_observed` reads zero for JMAP, Graph, Google
and DAV accounts. More broadly, per-account state on `Net` is split across separately
keyed registries (the meter map refcounted per id, the bandwidth registry with weak
entries per id, `account_hosts` per id and per registration token, the concurrency
and hold registries by string key), and `attach_account`, `detach_registration` and
`retag` must each touch all of them correctly. Owner: one injected `BandwidthPlane {
registry, meter }` (or `NetResources`) shared by Net, the engine and the IMAP
factory, and behind it an `AccountRegistration` value owning every per-account
entry, so detach and retag are one operation each. The string-keyed registries are
STR-145.

## BUG-117 - CardDAV writes the file extension into the vCard UID

**Reported by:** net-dav (cross-dialect divergence).

`contact_create` builds `id = "{uuid}.vcf"` and passes that as the UID; CalDAV uses a
bare UID and puts the extension only on the path. One decision, "new resource UID
and file name", answered two ways.

## BUG-119 - JMAP Principal query filter still spells the property `timezone`

**Reported by:** resolution wave (lateral, while fixing the Principal object's
`timeZone` decoding).

`crates/jmap/src/principal/query.rs` emits `timezone` in its filter. RFC 9670 spells
the Principal property `timeZone`; whether the filter condition follows the
property name depends on the filter extension's contract, which was not checked.

## BUG-120 - JMAP contact-move pre-read conflates explicit absence with an unanswered id

**Reported by:** resolution wave (lateral, beside the single-object read helper
`crates/jmap/src/sync/single_get.rs`).

Single-object contact and event reads now tell a `notFound` answer (scoped
`NotFound`) from an id the server never answered (`PartialResponse`). The pre-read
in the contact move path still treats both the same way; it could use the same
helper.

## BUG-121 - A matching-scope acknowledgement clears provisional ownership before persistence succeeds

**Reported by:** resolution wave (lateral, beside the `ack_checkpoint` scope check).

In `crates/sync/src/engine/ack.rs` the `Ack` arm now refuses a scope that does not
match the checkpoint, but for a matching scope it still removes the provisional
reattach mark before publication validation and the store write have succeeded. A
rejected or failed persist leaves the row without its provisional mark, so a later
`ReattachAbort` no longer deletes it. Ordering the removal after a successful write
would close it; check the reattach-abort path's expectations first.

## BUG-122 - IMAP pending LOGOUT is cleared by any tagged response, not LOGOUT's own

**Reported by:** resolution wave review (lateral).

`ProtocolState::apply_tagged` (`crates/imap/src/connection/state.rs`) now clears
`in_logout` on NO/BAD, but the check is not tag-aware: a pipelined earlier
command's tagged NO/BAD arriving after LOGOUT was sent clears the flag, and
LOGOUT's own OK then does not move the session to `Logout` (BYE still does). The
tagged pending-effect shape in STR-065 closes it.

## BUG-123 - JMAP filter deactivation can now fail on an unrelated script change

**Reported by:** resolution wave review (lateral; needs a ruling, not a defect on
its face).

`filters::update` (`crates/jmap/src/sync/filters.rs`) deactivates an active target
with a `SieveScript/set` guarded by `ifInState` from the read just before, so any
unrelated script change in between yields `ConcurrencyConflict`. Correct against
the deactivate-the-wrong-filter hazard, but a new user-visible failure mode; decide
whether one automatic re-read-and-retry belongs here.

## BUG-124 - The IMAP BYE-ALERT event test does not drive the driver

**Reported by:** resolution wave review (lateral).

`logout_bye_alert_emits_both_code_and_session_end_once`
(`crates/imap/src/connection/driver/events.rs`) re-implements the reclassification
rule inline instead of driving the dispatch loop, so it passes regardless of the
driver's behaviour. Drive a scripted LOGOUT with `* BYE [ALERT]` through the real
loop instead.

