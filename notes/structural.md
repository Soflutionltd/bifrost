# Structural - design hunt, November 2026

Shape and placement: the axes that should be types, and the moves, splits and
rewrites, each with the payoff it buys. Each entry is the hunters' account,
unverified. Renaming, moving and reshaping published items is ruled fine; an
entry that would remove a capability entirely (something a consumer could do
that would no longer be possible) needs the repository owner's ruling before
anyone builds it, whatever the entry argues.

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

Each entry names the hunt scope(s) that reported it. Where hunters disagreed,
both readings are recorded. The raw reports are in the commit that precedes
this file's.

## STR-002 - Owner-qualified ids travel as in-band separator strings

**Reported by:** types (LATENT), jmap-account (latent disagreement among the stripping functions).

JMAP (`jmap/src/sync/foreign.rs`) and Graph (`graph/src/account/foreign.rs`)
each encode "this id belongs to principal P" as `"P\u{1f}native"` inside
`FolderId`, `ObjectId`, `ThreadId`, `BlobId`, `ContainerId`; Graph adds `\u{1e}`
for public folders plus parsers for public address books, contacts, calendars
and events. Each crate also re-decides "a message and the container it is filed
into must have the same owner" (JMAP `owner_of`; Graph `folder_owner` /
`parse_message_id`). Meanwhile `Container` stores the same fact structurally
(`namespace`, `owner`, `owner_local_id`, `owner_email`, and `native_id`, the
encoded form) - two representations of one fact, kept consistent per producer.

**Partly resolved.** JMAP now has one codec and one stripping rule
(`foreign::Owner`, `JmapRef`, `registry::Route`, `Slot::wire`), and a
primary-qualified id stays literal at every door. What remains is the
cross-crate half: Graph keeps its own `\u{1f}` / `\u{1e}` codecs and owner
checks, and no shared `Qualified<T>` exists in `bifrost-types`. The JMAP
mint-site gap (a native id containing the separator) is tracked under STR-097.

The JMAP module doc argues a shared type "would be a false generalization"
because JMAP's owner is an `accountId` and Graph's a `/users/{id}` key. The
hunter answers that this is about the owner string's MEANING, which stays
provider-owned; the shared part is the STRUCTURE (optional owner plus native
id, bare means primary, two operands must agree), and `bifrost-types` already
generalized the owner as `MailboxId`.

Proposal: `Qualified<T> { owner: Option<PrincipalId>, native: T }` (or an
`owner()` accessor plus one codec) for the id newtypes that can be foreign, with
one encoding and one "same owner?" check. If the in-band string must survive for
persisted keys, the codec still belongs in one place.

## STR-005 - `BatchItemId(String)` has three meanings

**Reported by:** types (LATENT), sync-engine.

The sync-engine hunter counted about 50 hand-minted `BatchItemId(id.0.clone())`
sites across imap, jmap, graph, google, types and the tests, and noted the
engine's per-object accounting (`outcomes: HashMap<ObjectId, MutationBucket>`)
is correct only if every implementor mints the item id byte-identically to the
submitted `ObjectId` - qualification layers such as JMAP foreign ids are where a
mismatch would appear. Its proposal: object mutations yield `ItemOutcome` keyed
by the submitted `ObjectId` (or a `BatchItemId` constructible only from
`&ObjectId`), and the campaign checks each outcome against the submitted set,
closing BUG-007 - the IMAP `Target` permit lifted into `bifrost-types` for every
mail mutation door. The sync-lanes hunter adds that `mutation/readback.rs`
asserts "`BatchItemId` is an `ObjectId`" by string coercion
(`ObjectId(success.item.0)`, `ObjectId(f.item.0)`) at four sites. The smtp-sasl
hunter adds the positional meaning's own round trip: IMAP `send_rfc5322` mints
`BatchItemId(index.to_string())` and `not_accepted_recipients` parses it back with
`id.0.parse::<usize>()`, skipping ids that do not parse; `SmtpBatchRecipient` could
carry the caller's own payload (a generic `BatchItem<T>` already exists in
bifrost-types), handing IMAP its `bifrost_types::Address` back from the lane.

Streaming mutation and hydration lanes mint `BatchItemId(object_id.0)`
(`mutation.rs::refused_mail_target`, every provider) and the engine turns it
back with `ObjectId(item.0)` in `sync/src/mutation/readback.rs` and many places
in `sync/src/engine/bulk.rs`. `PushSubscription::all_succeeded` uses
`BatchItemId(index.to_string())`, a position. Vec-batch surfaces use
caller-supplied ids checked by `validate_batch_input`. Nothing makes "the
streaming lane id is the requested ObjectId" true; the IMAP `Target` permit work
found a site stamping the hydrated id instead. Proposal: make the key a type
parameter (`ItemOutcome<K, T>` / `BatchSuccess<K, T>`), `K = ObjectId` on the
streaming lanes, a `SubmissionIndex` for positional ledgers, `BatchItemId` only
where the caller mints it.

## STR-008 - Versions are read as `etag: Option<String>` and written as `ServerVersion`

**Reported by:** types, jmap-account.

`ContactCard::etag` and `CalendarEvent::etag` are strings; the write side takes
`ServerVersion` in three placements: a field on `EventPatch`/`ContactPatch`, a
parameter on `*_delete_if_version`/`event_rsvp_if_version`, and
`MailMutationTarget::expected_version`. `ServerVersion::expected_etag` must then
accept both the DAV-normalized and wire forms because the consumer wrapped the
read string itself. Proposal: `version: Option<ServerVersion>` on the read
shapes and one placement for the write precondition.

The jmap-account hunter adds provenance: JMAP accepts a caller's `StateAt` as an
`ifInState` for Email (flags, move, destroy), ContactCard and CalendarEvent writes
(`mutation::caller_state`); the token is per `(accountId, type)` (a comment says
so), but the type carries neither. Inventory mints `StateAt(Email/get state)` for
messages and `StateAt(Mailbox/get state)` for mailboxes indistinguishably, and
nothing stops a consumer passing a Mailbox-inventory token, or a primary token for
a shared message, into `bulk_set_flags_if_version`, giving an endless
`ConcurrencyConflict -> Retry(AfterStateRefresh)` for a token that can never match.
For contacts and events there is no read-side producer at all (the caller-version
item in `notes/todo.md` already records JMAP PIM reads exposing no caller
type-state field). Cross-crate, every provider decides at write time which
`ServerVersion` variant it accepts (JMAP `caller_state`, Google's contact/calendar
ETag checks, Graph's ETag paths) with no read-side mint naming the door it is valid
for. Owner: `bifrost-types`, a version minted by the read path with its object type
(and, for JMAP, owner) inside it.

## STR-012 - The engine's consumer API lives in the contract crate

**Reported by:** types (needs owner: moves published items).

`Control`, `DurableCheckpointSet`, `InvalidationSink`, `SinkPush`, `WatchEvent`,
`AccountControl` and `PauseReason` are produced and consumed only by
`bifrost-sync` and its consumer; no protocol crate touches them (JMAP uses
`WatchEvent` in a test). `SyncControl` is the only `Control` implementation.
They make `bifrost-types` the place an engine API change lands, and make
`events.rs` mix stream plumbing, inventory, push ingress and control. Proposal:
move them to `bifrost-sync`.

## STR-013 - Provider wire vocabularies in the neutral crate

**Reported by:** types.

`GraphSignal`, `JmapMethod`, `ImapResponseCode`, `GmailSignal`,
`EnhancedStatusCode` enumerate each provider's native error codes inside
`bifrost-types`, so adding a Graph error code changes the contract crate. Only
one central decision reads them (`smtp_reply`, CON-003). Options: a
`WireCause::Native { protocol, code }` with a provider-supplied typed code
behind a trait, or keep the enums and concede they are shared vocabulary; if
they stay, `derive_server` should key on `protocol`, not on the `WireCause`
variant.

The graph hunter adds that the tables are split across crates: Graph's
`error::classify_code` (string -> `GraphSignal`) lives in graph and
`GraphSignal::code` (variant -> string) in types, the two directions of one
bijection maintained by hand, with the `syncStateNotFound` alias in only one
direction; JMAP and Gmail have the same split (`JmapMethod`, `GmailSignal`), and
IMAP's three-table version is BUG-042. If the enums stay in types, their full
tables move with them (`GraphSignal::from_code` beside `code()`, plus a round-trip
test over every variant). It also lists `PushCapability::WebhookOrEwsStream`
(whose "OrEwsStream" is false, BUG-078) and `PushSource::GraphSubscription` as
Graph names in the neutral crate.

The google hunter shows the cost from the other side: `GmailSignal::Unknown { code
}` is used deliberately for reasons the crate RECOGNIZES - `invalidArgument`,
`insufficientPermissions`, `domainPolicy`, `conditionNotMet` and
`notAuthorizedToAccessThisResource` are routed by string match inside the `Unknown`
arm of `classify_response` - so a consumer branching on `WireCause::Gmail` sees
"unknown" for reasons Google documents; promoting them is a types change. It weighs
an opaque `(Provider, code)` plus a crate-owned typed decode, but calls the current
placement defensible if consumers branch on typed Gmail reasons, and the one place
it would not move without asking who reads it.

## STR-014 - MIME lives in `bifrost-types`, and there are two RFC 5322 renderers

**Reported by:** types.

`src/mime/` (parser, header decoder, transfer decoding, body selection,
renderer, about 4,000 lines with tests) is why the contract crate depends on
`base64`, `encoding_rs` and `jiff` for MIME. Its consumers are essentially IMAP
and Google. `bifrost-smtp` carries a second, lettre-derived builder
(`smtp/src/message/`), so header encoding, address formatting and boundary
generation are decided by two published renderers. Proposal: a `bifrost-mime`
crate (depended on by types for `Address`, or the reverse) and an explicit
ruling on how `smtp::message` relates to it.

The smtp-sasl hunter (needs owner on direction; no deletion implied) shows the two
composers already hold opposite Message-ID policies: bifrost-smtp's
`MessageBuilder::message_id(None)` generates `<36 alphanumerics@HOSTNAME>` from
`hostname::get()`, while bifrost-types' `mime::render` documents "a deterministic,
controlled-domain Message-ID (the sender's domain, never a leaked machine
hostname)". The workspace also has two address-list parsers (bifrost-smtp
`Mailbox`/`Mailboxes` FromStr with `message/mailbox/parsers/rfc2822.rs` vs
`bifrost_types::Address::parse_list`), and two Date formatters; `account_error.rs`
notes nothing in the workspace builds a bifrost-smtp `Message`. bifrost-types'
renderer is what every account crate's send path uses (IMAP, Google drafts);
bifrost-smtp's `message/` is a richer published builder (DKIM, attachments, typed
headers) used by nobody in the workspace. A plausible direction: bifrost-smtp's
builder becomes a typed facade over bifrost-types' header and body writers (it
already depends on bifrost-types), with the DKIM signer staying in bifrost-smtp, so
header encoding, folding, Message-ID, Date and Bcc stripping are decided once.

## STR-015 - `account.rs` mixes the contract with product policy

**Reported by:** types.

The trait file is about 1,750 lines and its default-impl tier encodes policy:
`dispatch_label` hard-codes a `ProtocolKind` switch (Gmail -> label membership,
Graph -> category, others -> keyword) while the next convenience,
`set_starred`, argues at length that capability dispatch is "the chosen design"
over a protocol switch. `set_starred` hard-codes Gmail's `"STARRED"` label id
and reuses `"$flagged"` as a Graph category; `mark_replied`/`mark_forwarded`
hard-code MAPI `PR_LAST_VERB_EXECUTED` codes. Proposal: conveniences in their
own module (an extension trait with a blanket impl, or free functions over `&dyn
Account`), dispatched uniformly through `ConvenienceShape` (add a `LabelShape`),
providers owning their sentinel strings.

## STR-017 - Write-only capability fields

**Reported by:** types (removal needs owner; wire-or-retire).

No reader outside producers and their tests: `cursor_freshness`,
`blob_digest_pre_download`, `rate_limit_class`, `quota_signal`,
`requires_uidvalidity_recheck`, `historyid_expires_after`,
`delta_token_expires_after` (the last two `None` in every provider).
`RateLimitClass`'s doc says "Used by `bifrost-net` and the engine"; neither
reads it. `BatchingPolicy` is read only by Graph, of its own value, and its
doc's per-protocol defaults are stale (IMAP declares 1024 items, not 5000;
Gmail's wait is 75 ms, not 200 ms). They may be consumed by ratatoskr, so the
question is wire-or-retire. The protocol-named fields are the wrong shape for a
protocol-neutral snapshot either way. The imap-account hunter confirms IMAP sets
`requires_uidvalidity_recheck = true` and nothing reads it.

## STR-018 - `IdempotencyKey` is threaded through every bulk method and read by no account

**Reported by:** types (wire-or-retire, needs owner).

No account reads `run_id`, `sequence` or `protocol_salt`; the salt's string
payloads are always empty. Its only function is engine bookkeeping, which need
not cross the account boundary.

## STR-020 - Hydration has two projection enums, and two placement vocabularies

**Reported by:** types.

`mutation::Projection` (for `get_stream`) and `hydration::HydrationProjection`
(for `message_hydrate`) overlap (`Preview(usize)`, `Full`, `FullWithBlobs`), and
`HydratedObjectKind` is only loosely tied to the projection requested
("consumers down-cast"); the read-back guard's need for `FlagsOnly` to yield
`FlagsOnly` is validated in the engine, not by the type. A hydrated `Message`
places the message by `containers: Vec<ContainerId>` while inventory uses
`memberships: Vec<MembershipScope>`; nothing maps one to the other without
provider knowledge (Gmail fills the former with label ids, the latter with
`MembershipScope::Label`).

## STR-028 - The per-account actor is split across three bundles

**Reported by:** sync-engine (the hunter calls it a big but clean rewrite).

`AccountSlot` (`types.rs`, about 25 fields), `SlotContext` (`engine/context.rs`,
about 28) and `RecoveryContext` (`engine/reattach.rs`, about 24 borrowed fields)
bundle the same per-account handles: `SlotContext::recovery` rebuilds a
`RecoveryContext` literal; `SyncEngine::reattach` hand-builds a second from
`AccountSlot` fields; `AccountSlot` and `SlotContext` are populated field by
field side by side at the end of `attach_opened`. `SyncEngine` also keeps a
parallel `ack_senders: DashMap<AccountId, Sender>` beside `accounts`; many
methods look up both and detach removes them separately. More engine-wide
registries keyed by `AccountId` are cleaned at the bottom of detach (sink,
budget gate, backfill registry, throttles, meter, subscriptions), each with its
own forget call.

Proposal: one `Slot` (`Arc`) owning every per-account handle, the writer
sender, the boundary as `Boundary` (never the raw sender, fixing BUG-004
structurally) and the workers; recovery, reattach, establishment, campaigns and
passthrough become methods on it; `SyncEngine` becomes a registry
(`engine.slot(id)?` then the operation); an RAII `SlotRegistrations` guard
(sink, budget, meter, backfill registry) released by drop at the end of detach
replaces the hand-ordered tail. "Is this account attached" becomes one lookup.

The sync-lanes hunter makes the same argument for `SubscriptionRegistry`, a
per-engine `DashMap<AccountId, Vec<..>>` whose every method takes the account
id plus that slot's `shutdown` token. The whole "seal" mechanism (`record`,
`restore_orphans`, `replace` reading the token inside the shard lock), with its
detach-ordering argument and two integration tests, exists because a record
written after detach would land under an id "a later attach of the same
`AccountId` inherits". If the slot owned its subscriptions, a late write could
only reach the dead slot's object and the next attach gets a fresh one by
construction, so the bug class disappears rather than being defended.
`BackfillRegistry` has the same shape (`forget_account` on detach).
`InvalidationSinkInner`'s map is legitimately engine-wide (the consumer
addresses it by id) and is not part of this.

## STR-032 - `run_bulk_pipeline` is one 850-line function over untyped campaign state

**Reported by:** sync-engine.

About fifteen loose locals (`outcomes`, `effects`, `retry_slots`,
`retry_owners`, `identities`, `cohorts`, `forwarded_directives`, `first_error`,
`caller_conflict`, `blocked_by_engine`, `dedupe_count`, `cursor`, ...) are the
campaign's state with no type or methods, so every phase rule (abort, engine
block, reassert finalization) is a loop over cohorts inside the main loop.
Proposal: a `Campaign` struct owning that state with `select_action()`,
`submit()`, `read_back()`, `finalize()`; `MutationCohort` phases as an enum
(STR-026); the CON-014 drive-start primitive for admission and holds.

The sync-lanes hunter places it: `mutation/` holds `MutationCounters`,
`fanout`, `PartialCommitObservation`, the vendor and read-back, while the
campaign state machine (cohorts, scheduler, reassertion, read-back routing) is
`engine/bulk.rs` at about 2,000 lines, and read-back's `ReadbackReport` /
`ObjectReadback` are `pub(crate)` precisely so `engine/bulk.rs` can consume
them. Proposal: the state machine moves into `mutation/campaign.rs` (pure state
plus transitions), with `engine/bulk.rs` keeping the async driving.
`MutationCounters` has public fields next to `record_*` methods, allowing both
direct and method writes; pick one.

## STR-034 - The sync test layout mirrors history, not the design

**Reported by:** sync-engine.

- `engine/lane.rs` is about 110 lines of gate and 1,100 lines of tests, mostly
  exercising `PendingCoverage` capacity, supersession and watermarks; the
  capacity moved into `cursor/coverage.rs` and the tests did not follow.
- `tests/attach_schema_recovery.rs` (about 3,150 lines) is a grab-bag:
  page-loss warnings, detach push teardown, reattach topology, lifecycle reader
  parking, account restart exhaustion, partial-batch boundary, publication-id
  invariant, deferred schema reset. It redefines `unsupported`, `caps` and
  `cursor_for` from `tests/common/mod.rs` and has its own `HealAccount`.
- Nine hand-written `impl Account` doubles in `crates/sync`
  (`common::StubAccount`, `HealAccount`, `FlagsAccount`, `JudgingAccount` and
  `ReassertAccount` in `readback_guard.rs` alone, plus the passthrough ones)
  copy default-method behaviour rather than share it.
- `tests/common/mod.rs`'s `StubAccount` has about 50 hook fields, and is
  included into the crate as `crate::test_common` via `#[path]` as well as being
  the integration double.

Proposal: one configurable double per lane (changes, inventory, push, mutation,
lifecycle) composed into a stub, and test files named after the contract they
pin (`supervisor_*`, `delivery_*`, `campaign_*`, `establish_*`).

## STR-035 - Engine ownership oddities

**Reported by:** sync-engine.

- `types.rs` mixes published configuration (`EngineConfig` and friends,
  `Incarnation`) with the engine-private `AccountSlot`, `WorkerTask` and
  `WorkerRole`; the slot belongs in `engine/` (STR-028).
- `BackfillAdmission` holds a `BudgetPermit` in `Mutex<Option<_>>` and exposes
  `admit()` and `release_admission()` separately, turning an RAII permit into a
  flag the caller must clear. The permit should travel by value through the
  wait (`wait_for_capacity(permit) -> Result<permit, WaitFailed>`).

## STR-036 - Vestigial engine surface

**Reported by:** sync-engine (published; removal needs the owner).

- `AckRequest::auto` is always `false`; nothing produces `true`, and the doc
  says it "remains for trace readability".
- `ThrottleBucket::record_for` ignores its account argument.
- `ThrottleBucket::forget_account` is a no-op that detach still calls.
- `BackfillWiring::changes_tx: Option<broadcast::Sender>` is only used as
  `is_some()`, a bool meaning "broadcast at all"; publication goes through
  `delivery`.

## STR-037 - `BackfillPartitionOutcome` is a product of flags with illegal combinations

**Reported by:** sync-lanes.

`{ complete: bool, scope_walk: ScopeWalkStep, end: Option<PartitionEnd> }` plus
counts. `StopScopeWalk` with `end: Some(Exhausted)` is meaningless, `complete:
true` with `end: None` is incoherent, and the `Default` exists only to pick safe
values for fields a construction site might forget; `ScopeWalkDriver::fold`
re-derives the real state from the three. Proposal: `enum PartitionVerdict {
Exhausted { degraded }, MoreBeyond { degraded }, EndedUnannounced,
StoppedAtBarrier }` beside the counts; `fold` becomes a match and
`BackfillScopeWalk` (one bool) disappears into the driver.

## STR-038 - Push lane verdicts and invalidation intent travel as tuples and paired flags

**Reported by:** sync-lanes.

- `PushLanes::failure() -> (Duration, bool, bool)` is (delay, terminal,
  restart); a named `LaneDisposition` fits (CON-012).
- `PendingPush { full, scopes, memberships, closed }` and `WorkSet { full,
  scopes, hold }` encode "full reconcile" as a bool beside a set that must be
  empty when it is true, and every site calls `clear()` to keep it so. `enum
  Intent { Full, Some { scopes, memberships } }` makes the illegal state
  unconstructable and is the natural home of the coarsening rule (CON-027).
- `WorkSet::hold: Option<(u64, Instant)>` is a bare tuple of account generation
  and deadline.

## STR-042 - `PendingCoverage` is five subsystems behind one lock and one public surface

**Reported by:** sync-lanes (owner ruling for demotions).

It holds (1) the publication ledger (claims, lanes, persisted and folded
watermarks, carry-forward); (2) backfill flow control (boundary entries as ring
charge, `readers`, `delivered_at`, `subsumed`, the `live` mirror, the capacity
`Notify`); (3) delivery-loss accounting (per-scope `undelivered` watermark,
`discard_requests`, `tearing_down`, `last_backfill_mint`); (4) fencing
(`fenced`, `fenced_backfill`, `fenced_account`, `burn_fence`); (5) minting
(publication ids with instance segments, and walk generations, BUG-015). The
single lock is justified, since transitions span 1, 2 and 4 (`register_in`,
`invalidate_in`, `release_undelivered`); the single type is not. It has about 45
`pub` methods, most callable only under an ordering the delivery gate or writer
enforces (`mark_delivered` "under the delivery lock", `begin_teardown` "through
`ChangeDelivery::begin_teardown`", `receiver_joined`), some half-operations
(BUG-020). A consumer holding `Arc<PendingCoverage>` can break every invariant
the engine relies on.

Proposal: keep one mutex, give the guarded state sub-structs (`Claims`,
`RingCharge`, `Fences`, `LossLedger`) whose methods are the named transitions,
and expose a small facade (`register_page`, `register_marker`, `acknowledge`,
`retire`, `invalidate_scope`, `on_receiver_read`, `on_receiver_departed`).
Minting of persisted generations moves to the writer. Demote the rest to
`pub(crate)`. The `Publications` alias should pick one name. Lateral:
`LiveSupersedes` is filtered on every page through a `Mutex` although nothing
populates it (`filter_inventory_supersedes` short-circuits on `is_empty()`,
taking the lock once per page) - harmless, noted because it is the hot path.

## STR-043 - The backfill lane is split across `backfill/` and `engine/backfill.rs` along the wrong seam

**Reported by:** sync-lanes.

`backfill/` holds the partitioner, runner, `ScopeWalkDriver`, checkpoint writer
and registry. `engine/backfill.rs` (about 1,500 lines) holds the orchestrator
loop and also pure backfill-domain decisions: `backfill_plan_for`,
`BackfillPlan::resume`, `open_pages_resume`, `backfill_complete_recorded`,
`emit_backfill_complete` (marker construction), `BackfillScan`, `MarkerOutcome`,
`ScopeResume`, `WalkLabels`. The partition key format lives in
`backfill/partitioner.rs` while its two readers (`open_pages_resume` and the ack
writer) live in `engine/`. `backfill/parking_tests.rs` tests the engine
orchestrator's `SlotContext` and is mounted from `engine/backfill.rs` with
`#[path = "../backfill/parking_tests.rs"]`. Proposal: `backfill/plan.rs`
(partitioning to plan plus resume, pure), `backfill/marker.rs` (marker plus the
typed key of BUG-016), `backfill/scan.rs` (`BackfillScan`), with
`engine/backfill.rs` keeping only the async loop, admission and wiring; tests
move with what they test.

## STR-046 - A partitioning framework with no producer imposes its costs on every consumer

**Reported by:** sync-lanes (published; not a deletion recommendation).

`BackfillPolicy`, `BackfillStrategy`, `PartitionPlan`, `PartitionBounds`,
`plan_time` / `plan_uid` / `plan_page`, `OpenPages`, `parse_page_partition`,
the page tie-break in `get_backfill`, and `open_pages_resume` all serve
`TimeWindowed`, `UidRange` and `PageCount`; no in-tree provider returns any of
them. `BackfillPolicy` is documented as "configurable per account" but never
configured (`backfill_plan_for` builds `BackfillPolicy::default()` or a literal
with `clock_skew: Duration::ZERO`). `BackfillStrategy` duplicates
`InventoryPartitioning` and `PartitionBounds` duplicates `InventoryPartition`
(using a `Timestamp::MIN` sentinel for "open" where `InventoryPartition::Time`
uses `Option`). The hunter's narrower points: the chain
`InventoryPartitioning -> BackfillPolicy -> plan -> PartitionBounds ->
InventoryPartition` has two redundant middle layers, and collapsing it to
`InventoryPartitioning -> Vec<InventoryPartition>` (or `OpenPages`) in
`backfill/plan.rs` keeps every behaviour (owner ruling for the middle types);
and the durable cost of the unexercised machinery (the selection rule every
store implements, the drifting time keys, BUG-016) is paid now by every backend
for producers that do not exist yet.

## STR-048 - Per-scope drive state is split between the cursor registry and the multiplexer

**Reported by:** sync-lanes.

`CursorRegistry` owns cursor, incarnation, scope delete generation, liveness
token and drive lease. The multiplexer's `ScopeTokens` owns poll-task ownership
and the terminal-park tombstone (`ScopeToken::parked`), which the reconciler
reaches through free functions (`retire_stale_scope_token`, `scope_is_parked`,
`park_scope_for_drive`). "Is this scope parked?" is a property of a scope
incarnation under a `DriveGeneration`, and the reconciler needs it as much as
the poll loop. Proposal: the parked verdict lives in `RegistryState`, bound to
the scope's `DriveGeneration` (a fence change already revives it), `ScopeTokens`
shrinks to poll-task bookkeeping, and `RegistryState` replacement merges
per-scope state field by field (BUG-022).

## STR-050 - UID versus sequence number is not in the IMAP response types

**Reported by:** imap-protocol.

`UntaggedResponse::Search { uids: Vec<u32> }` ("sequence numbers or UIDs"),
`Sort { nums }`, `ThreadNode::id: Option<u32>`, `EsearchResponse { uid: bool,
min/max: Option<u32>, all: Vec<UidRange> }`, `ResponseCode::Modified(Vec<UidRange>)`
("STORE seqs, UID STORE UIDs"), `Expunge(u32)`, `Removal::Seq(u32)`,
`FetchResponse { seq: u32, uid: Option<u32> }`. The typed `Uid` / `Seq` exist and
are unused; whether a number is a UID is decided by each consumer from which
command it remembers issuing. Proposal: make addressing a type parameter of the
result (`SearchResult<Uid>` vs `SearchResult<Seq>`), parse the ESEARCH `UID`
indicator into the type, and hand `MODIFIED` to the consumer as a raw set it
types by its command. Relatedly, ten UID/sequence `Command` twins
(`Search`/`UidSearch`, `Fetch`/`UidFetch`, ...) double every match in `kind`,
`mailbox_target`, the encoder and the classifier while `CommandKind` collapses
them, so classification cannot tell UID FETCH from FETCH (`(CK::Fetch, VANISHED
(EARLIER)) => OnlySolicited` also admits a non-UID FETCH, which cannot solicit
VANISHED). One variant with `target: Addressed { Uid(UidSet), Seq(SeqNumSet) }`
halves the matches and lets `CommandKind` carry the addressing.

The imap-connection hunter reports the same axis from the connection layer,
which uses the typed ids only at the ergonomics edge (`SyncFetchRequest::uids:
UidSet`): `Command::Fetch` and `Command::UidFetch` both take `SequenceSet`, so
`uid_fetch(&SequenceSet, ..)` accepts a `SeqSet`'s inner set; `SearchResult {
ids: Vec<u32>, mod_seq: Option<u64> }` holds sequence numbers for SEARCH and UIDs
for UID SEARCH/UID SORT, told apart only by which method was called
(`SortConsumer` returns the same type); `IdleEvent::SearchUpdate`'s doc admits
"the `uid` field may be inaccurate for legacy SEARCH"; `IdleEvent::{Exists,
Expunge, Recent}(u32)`; `AppendConsumer::Output = Option<(u32, u32)>` and
`MultiAppendConsumer::Output = Vec<(u32, u32)>` - a bare `(uid_validity, uid)`
tuple of two silently swappable `u32`s; `uid_fetch_changed_since(.., mod_seq:
u64)`, `uid_store(.., unchanged_since: Option<u64>)`; and raw `uidvalidity`,
`fetch.uid`, `fetch.mod_seq` at the point `account/push.rs::absorb_idle_event`
writes the modseq cache. Its target adds `Command::UidFetch { uids: UidSet, .. }`
/ `Command::Fetch { seqs: SeqSet, .. }` for STORE/COPY/MOVE/EXPUNGE/SEARCH too,
`AppendUid { validity: UidValidity, uid: Uid }`, and `ModSeq` in every modseq
position.

## STR-056 - Smaller IMAP protocol axes

**Reported by:** imap-protocol.

The escape-payload newtypes and `MailboxAttribute::Inbox` landed. One residue:

- `MoveResult::copy_uid` / `CopyResult::copy_uid: Option<ResponseCode>` are
  documented "Always a `ResponseCode::CopyUid` when present" and every consumer
  re-matches. Use `Option<CopyUid { validity: UidValidity, source: UidSet, dest:
  UidSet }>` (and the same struct inside `ResponseCode::CopyUid`); its readers
  are `account/mutate.rs` and three matches in `account/error.rs`.

## STR-059 - IMAP dependency edges point upward

**Reported by:** imap-protocol.

`codec/classification.rs` imports `crate::connection::NotifyFlags`;
`codec/encode/mod.rs` imports `crate::connection::literals::literal_marker_at`
(wire grammar in the connection layer); `codec/encode/dispatch.rs` calls
`crate::connection::ImapConnection::search_criteria_contains_atom`;
`types/command.rs` imports `crate::connection::SessionState` for
`legal_states`; `types/events.rs` implements on `crate::TypedEvent`; the
crate-root `error.rs` embeds `crate::account::sieve::SieveResponseCode`. The
protocol core should be a leaf, `types <- codec <- connection <- account`: move
`SessionState` and the `NotifyFlags` derivation into `types`, the literal-marker
scanner into `codec`, the search grammar into a `SearchKey` AST (STR-053), and
give ManageSieve its own error mapped at the account boundary. The
imap-connection hunter reported the same four `codec`/`types` -> `connection`
edges independently; `literals.rs` is a pure literal-marker scanner also used by
`connection/wire.rs`.

## STR-061 - The IMAP crate-root `allow(dead_code)` hides dead copies of live decisions

**Reported by:** imap-protocol.

Dead today: `ImapAtom`, `validated::ObjectId`, `GmailMessageId`,
`GmailThreadId`, `types::Address`, the `EnvelopeAddress -> Address`
conversions, `MailboxInfo::special_use`, `MailboxRights::can_insert` /
`can_delete`, `classification::mailbox_names_eq`, `EventImpact` production use,
`TypedEvent::MetadataChange` / `ServerMetadataChange` construction. Three are
second copies of decisions with a live owner elsewhere (special_use, can_delete,
mailbox_names_eq - CON-028, CON-029, BUG-027), each kept green by its own tests
while the live owner differs. All are crate-private. The hunter recommends
`#![warn(dead_code)]` on `types/` and `codec/` specifically (a small, closed
vocabulary, unlike the measured crate-wide case), then wiring each item in where
STR-051 says it belongs, deleting only what has no consumer.

## STR-062 - Protocol-value behaviour lives in IMAP `account/`, and account identity lives in `types/`

**Reported by:** imap-protocol.

`CompactUidSet` (folder_registry) is IMAP set algebra (STR-049).
`get.rs::merge_fetch_response` encodes FetchResponse merge semantics (which
items are mutable state, which fill gaps), is shared with pim, and belongs on
`FetchResponse`. Conversely `MailboxName::account_id` / `from_account_id` /
`cmp_account_order` (the `~imap-wire:` FolderId encoding) are account identity
policy embedded in the protocol type; they belong with `encode_object_id` in the
account layer (or a dedicated identity module), which also hosts the single
`MessageRef` (STR-051).

The imap-account hunter reports the registry side: `CompactUidSet`, `UidSetDiff`
and the range arithmetic live in `folder_registry.rs` while `UidSet`, `UidRange`
and `SequenceSet` live in `types/`, with conversions between them
(`UidSet::from_ranges(known_uids.ranges())`, `CompactUidSet::from_ranges`); the
compact set belongs in `types/` as the set of record. `FolderEntry` bundles LIST
facts, ownership, MYRIGHTS, a cursor hint, a MODSEQ cache and a last-seen instant
behind four locks, and is the implicit (unexercised) owner of folder roles;
making roles, ownership and the shared-error mapping explicit registry
responsibilities (BUG-028, BUG-046) gives those decisions a home.

## STR-064 - IMAP test layout mirrors the pre-split monolith

**Reported by:** imap-protocol, imap-connection.

The imap-connection hunter adds: `connection/tests.rs` is a 3,300-line catch-all
beside per-file `*_tests.rs`; two inline modules are named after a review
(`lifecycle.rs::bandwidth_review_tests`, `driver/upgrade.rs::bandwidth_review_tests`)
rather than a behaviour; and the behaviours the layer must pin (admission,
pending effects, framing/retirement, event classification) have no home of their
own because no module owns them - once they have owners, their tests move with
them.

The imap-account hunter adds: `account/scripted_tests.rs` (about 5,800 lines, about
140 functions) is organized by "needs a scripted peer" - send and Sent-append,
draft send, mutation batching, push admission and sessions, all three change
strategies, the poller, raw reads, search paging and PIM hydration share one file.
A `scripted/` directory with one file per lane (the harness staying in
`test_support.rs`) makes a module's coverage visible from the module; `pim.rs`
carrying about 1,400 inline test lines is the same accident in the other
direction.

The decoder was split into nine grammar modules but `codec/decode/tests.rs` is
one file of about 14,500 lines and `codec/encode/tests.rs` about 8,400. Tests
should sit with the grammar they pin (or be golden transcript tables per
response family), which also makes the CON-032 tables testable per row. The
`capability_matrix` oracle is a test copy of production logic and should go once
production is exhaustive.

## STR-069 - Twelve `impl ImapConnection` files each restate their gates; a request layer collapses them

**Reported by:** imap-connection.

`auth.rs`, `helpers.rs`, `mailbox.rs`, `seq_ops.rs`, `uid_ops.rs`,
`extensions.rs`, `sort_thread.rs`, `append.rs`, `idle.rs`, `lifecycle.rs`,
`ergonomics.rs`, `search_validation.rs` each `use super::*` and open `impl
ImapConnection`. Split by command family is fine, but every method is the same
five steps: gate (BUG-029), build `Command`, pick a consumer, compute a deadline,
`submit_*`. Command-consumer pairing is decided twice, here and in
`pipeline/mod.rs` (pipelined `uid_fetch` always uses `FetchVanishedConsumer`; the
handle picks among four). Proposal: `trait Request { type Consumer: Consumer; fn
command(&self) -> Command; fn consumer(&self) -> Self::Consumer; }` with
admission derived from `command()`; handle methods become one-liners over
`submit(request, timeout)`, the pipeline builder pushes the same `Request`
values, and the deadline discipline is enforced once. `helpers.rs` mixes snapshot
accessors, gates, STATUS/LIST option parsing, NOOP, CHECK and CAPABILITY - the
file of things with no other home.

## STR-070 - IMAP connection setup runs outside the driver task

**Reported by:** imap-connection.

`lifecycle::connect_with_tls_connector_metered` runs the greeting, initial
CAPABILITY and STARTTLS on stack-held `WireReader`, `ProtocolState`,
`TagGenerator` and `DriverEventSink`, then spawns the driver with them, producing
a second execution context with its own timeout model (`setup_timeout` maps
expiry to `Error::Timeout { attempt: None }`, reading as `Unsent` even after the
STARTTLS line was written - already an open item in `notes/todo.md`); its own
ALERT delivery (`events_tx.try_send(TypedEvent::Alert(..))` on the raw channel,
because `DriverEventSink::emit` is `pub(in crate::connection::driver)` - a
visibility accident shaping the design); an ungated flavour of every driver
routine (`run_one_command`, `run_starttls_upgrade` with `tls_active: None`, `gate:
None`, which is why `starttls_upgrade` takes `Option<Arc<AtomicBool>>` and
`Option<&CommitGate>`); and its own STARTTLS availability rule (BUG-029).
Proposal: spawn the driver first; it runs a `Setup` phase, and the handle awaits
a ready oneshot carrying the first snapshot or the setup error. One execution
context, one evidence model, no Option parameters, and `tls_active` written in
one place (today the driver writes it at the stream swap and
`starttls_with_connector` writes it again).

## STR-071 - The IMAP pipeline has no production caller yet shapes the driver's hardest invariants

**Reported by:** imap-connection (crate-private, but removal is an owner decision; flagged, not recommended).

`ImapConnection::pipeline()` (about 1,700 lines) has no caller in `account/` nor
anywhere outside its own tests. It shapes the driver: `PipelineRouting` with
`sending: Option<usize>` as an exclusive ownership bound,
`route_pipeline_response` called from inside the sender's continuation wait,
`correlated_esearch_owner`, `group_into_sub_batches` driven by
`CommandKind::untagged_routing_key` (CON-032), the second NOTIFY pre-effect copy
(BUG-035), and `SendOutcome::Rejected` / `ContinuationOutcome::Rejected` arms
unreachable outside it. `pipeline/mod.rs` carries an "audit boundary: not
line-audited" note. It bypasses every handle gate (BUG-029) and waits unbounded
(BUG-041). Options: (a) keep it and route it through the request/admission layer
(STR-069, BUG-029) so it stops being a gate bypass; (b) remove it and simplify
`wait_for_continuation` to one mode, dropping `PipelineRouting`,
`untagged_routing_key` and the router.

## STR-072 - A library-grade IMAP connection surface nobody outside the crate can reach

**Reported by:** imap-connection (owner decision).

The connection layer is written as a direct API: `pub fn` methods, a typed
`Pipeline` with tuple unfolding, `ergonomics.rs` whose every method says "Direct
API counterpart to Account ...", `IdleEvent` with exhaustive RFC documentation,
`drain_event_impacts`. None is reachable from outside `bifrost-imap`
(`ImapConnection` is `pub(crate)`), and `lib.rs` carries a crate-wide
`#![allow(dead_code)]` "because the dead surface is spread across most of the
connection module tree". Either promote a deliberate direct-IMAP public API (then
gates, typed ids and bounded waits matter for real callers), or shape the layer
as the account's transport and let the compiler find the dead surface by removing
the allow from `connection/` alone (the crate-wide count was measured at 105;
a connection-only removal gives the exact list). The current middle state pays
the complexity of a public API without its users. Related: STR-061.

## STR-073 - About 30 IMAP consumers each own an untagged buffer they almost always surrender

**Reported by:** imap-connection.

Roughly 30 consumers carry `buffered: Vec<UntaggedResponse>` and surrender it on
every `finalize` arm; `Finalized::failure` takes the buffer as a mandatory
argument so the choice is visible. The design is right about the failure mode
(responses lost on `?`), but "this response is not mine" is decided at
`on_response` time in nearly every consumer and only deferred to `finalize` by
the shape. Proposal: `on_response` returns `Option<UntaggedResponse>` (declined)
and the driver holds declined responses, removing most buffers and the chance of
forgetting one; the few consumers that must decide at finalize (SELECT's
`[CLOSED]` cut, APPENDUID's index) keep their own. This also gives BUG-036's
"already emitted" flag a home in the driver.

## STR-074 - Distinct failures collapse into `Request(Malformed)` because the shared vocabulary has no other slot

**Reported by:** imap-account (cross-crate gap in `bifrost-types`).

All derive `ClientBug`, and a caller cannot tell them apart: `container_delete`
on a non-empty mailbox (`pim_malformed`, a precondition); `Error::AppendLimit`,
`TOOBIG`, `MAXCONVERT*`, `METADATA MAXSIZE` (payload too large);
`Error::FetchLimit` / `FetchLiteralLimit` / `SearchResultTruncated` (local budget
exceeded on a server-acknowledged answer; the last is already an open item in
`notes/todo.md`); sieve `ACTIVE` ("deactivate first" precondition); UIDVALIDITY
changed (not the caller's fault at all; also open in `notes/todo.md`). Only
`valid_uids` and `merge_folder` / `or_folder_restriction_error` are genuine
caller errors. `RequestErrorKind` has only `Malformed` and `BatchInputInvalid`, so
protocol crates have nowhere else to put these. Proposal: `PayloadTooLarge`,
`PreconditionFailed` (or reuse `ConcurrencyConflict` for the not-empty/active
cases), so a UI can say something true.

## STR-078 - Credentials and TLS mode, three copies each

**Reported by:** imap-account, smtp-sasl, net-dav.

The net-dav hunter adds the HTTP side: `CalDavCredentials` and `CardDavCredentials`
are identical published enums, both converting into `DavCredentials`, identical
again; JMAP's Basic credential stores a pre-encoded value while DAV stores username
and password and encodes per request. A shared `HttpCredential` in bifrost-net would
carry the token-coordination rule (BUG-111). Removing or renaming the published
enums needs the owner; reshaping them into wrappers does not.

The smtp-sasl hunter details the three credential types: `bifrost_smtp::Credentials`
(public enum, `password: Zeroizing<String>`, re-exporting `zeroize::Zeroizing`),
`bifrost_imap::Credentials` (struct over a private `CredentialsKind`, using
`SecretString`), and `bifrost_imap::SubmissionCredentials` (public, `password:
String` - plain, not zeroizing); `resolve_credentials` copies between them with
`password.as_str().to_owned()`. All model the same two shapes (password; OAuth
identity plus token source). A shared `SaslCredentials` in bifrost-sasl (which
would need `TokenSource`, so either sasl depends on bifrost-net or the type lives in
bifrost-net's auth module) is the single owner; the public SMTP and IMAP types need
the owner, but at minimum `SubmissionCredentials::Password.password` should be a
secret type.

Credentials: `crate::types::Credentials` (with `CredentialsKind`),
`SubmissionCredentials` (password a plain `String`), and
`bifrost_smtp::...::Credentials`, translated structurally in
`resolve_credentials`. The token-source type already lives in `bifrost-net`; one
credentials type there or in `bifrost-sasl` would give IMAP, SMTP, ManageSieve and
DAV one secret-handling story. TLS mode: `SubmissionTls { Implicit, StartTls,
Plaintext }`, `ManageSieveTlsMode { Implicit, StartTls, None }`, and
`ImapConfig`'s own, each with its own default-port table.

## STR-079 - IMAP `pim.rs` is not PIM

**Reported by:** imap-account.

At about 4,600 lines it holds the direct mail-mutation primitives and their
partial-commit records; SMTP send, raw send, draft create / send / discard;
byte-level RFC 5322 header surgery (`split_header_body`, `rewrite_header_block`,
`restamp_draft_date`, Bcc stripping); a search engine (criteria compiler,
provider-query validator, position-based pager with its own cursor codec);
container CRUD; folder-role derivation; MYRIGHTS projection; quota; typed message
and thread hydration including MIME selection; and about 1,400 lines of inline
tests. Contacts and calendars - the actual PIM - are not in it (they are
forwarding in `mod.rs`). Its header says the long tail was never line-audited.
Suggested split: `compose/` (send, draft, header rewriting - the header surgery
arguably beside `bifrost_types::mime`), `search.rs`, `containers.rs`, roles into
the registry (BUG-028), and the primitives folded into the mutation engine
(STR-080).

## STR-082 - IMAP does not partition inventory, so large folders get no bounded backfill

**Reported by:** imap-account (not a bug; a gap between vocabulary and its intended producer).

`inventory_partitioning` is not overridden, so IMAP folders inventory as one `UID
FETCH 1:*` that also accumulates every UID in a `Vec<u32>` before minting the
cursor. The half-open `u64`-ended `InventoryPartition::Uid` vocabulary was
designed with IMAP in mind (`types.md` says as much), and the bounded backfill
lane depends on partitions, but no production crate mints a `Uid` partition.
Related: STR-046.

## STR-091 - Delete JMAP's `HttpTransport` generic; it is a test seam above `AccountNet`

**Reported by:** jmap-protocol (its top-ranked move; "the largest single simplification available in the crate").

`core/transport.rs::HttpTransport` has one production implementor
(`ReqwestTransport`) and about thirty-five test implementors across `client.rs`,
`core/tests.rs`, `blob/`, `event_source/`, and twenty more in `sync/`. Each double
stubs `upload`, `download`, `get_session` and `api_request` and returns
`TransportError::new("...")` with no `net` evidence - exactly the shape
`reference/net.md` says each crate grew and should not have. Consequences:

- A classification path only tests reach: `sync/error.rs::convert_transport`'s
  "custom transport without bifrost-net evidence" branch classifies those errors
  as `Transport(Network)`; production never produces a `TransportError` without
  `net` or `local`. The doubles exercise a copy of the status contract instead of
  bifrost-net's - the situation `bifrost_net::test_support` was published to end.
- The generic is why `JmapAccount` is "undrivable": the comment above
  `MailAccount` in `sync/account.rs` says no scripted transport can be installed
  at that level and closing it "means threading the transport generic through
  `JmapAccount`". It does not: `ReqwestTransport::scripted_with_retry`
  (cfg(test), in `transport_reqwest.rs`) already builds the production transport
  over `ScriptedDispatch`, and `capability_contract_tests.rs` already constructs a
  real `ReqwestTransport` for a whole `JmapAccount`. A scripted net there makes
  every `Account` door drivable hermetically through the real retry budget, replay
  policy, concurrency gate and bandwidth meter.
- Three traits for one transport: `SseTransport` and
  `blob::download::BlobTransport` exist so the generic can be partially
  implemented; both have only the reqwest implementor.
- Every type carries `T`: `Client<T>`, `Request<'x, T>`, `Account<Tr>`,
  `MethodTuple::add_to_request<T>`, and every generic helper in `sync/`.

Proposal: make `Client` concrete over `ReqwestTransport` (or `AccountNet`
directly), delete the three traits, `convert_transport`'s no-net branch and the
doubles, and port tests to `scripted_net`. The WebSocket push lane keeps its own
seam (`sync/push.rs::PushTransport`), the genuinely non-HTTP part.

**The jmap-account hunter reached the opposite remedy for the same symptom.** It
reports that `JmapAccount` hardwires `MailAccount = Account<ReqwestTransport>`
because WebSocket push is reqwest-specific, and that as a consequence
`capability_contract_tests.rs` can only check the false direction (CON-006);
`route_object_id`, `route_mutation_target`, `draft_route_account` are generic over
a handle type `H` "purely so they can be exercised"; `foreign_account_id_for_object`
exists only under `#[cfg(test)]` as a test pin for a rule production implements
elsewhere (`route_object_id`) - a copy kept alive by its own test; and the account
methods are one-line delegations "to keep the pin meaningful", which is why logic
lives in free functions taking six to eight parameters. Its proposal: make
`JmapAccount<T: HttpTransport>` generic (with `Client<T>: PushTransport` as a
bound, or push behind its own injected `Arc<dyn>` seam), as the code comment
estimates, so whole-`Account` tests replace the scaffolding. The jmap-protocol
hunter's reading is that the generic is unnecessary because a scripted
`ReqwestTransport` already exists; both readings agree the current pin is what
makes `JmapAccount` hard to drive.

## STR-095 - JMAP test layout mirrors review history

**Reported by:** jmap-protocol.

`src/tests.rs` (about 3,800 lines) is a numbered list of sections, one per past
review pass, each `#[cfg(...)]`-gated on feature combinations and testing modules
it does not sit beside (mailbox wire, email submission wire, session capability
fallbacks, URL template parsing, blob management, calendar, contacts, principal,
quota ...). `core/tests.rs` (about 1,100 lines) is the same for core. Several
sections exist only to hold two copies in step (CON-042, CON-043), and at least one
pins a defect (BUG-061). Wire tests should live in the module whose wire shape
they pin; the agreement tables go when their duplication goes.

## STR-097 - The JMAP account needs a per-account registry; its per-account facts are parallel maps keyed by bare strings

**Reported by:** jmap-account (its dominant missing owner; the hunter recommends a rewrite of the account layer's internal plumbing rather than local fixes).

The core landed: `sync/registry.rs` holds `Registry { primary, accounts }` of
`Slot`s, each owning its handle, limits, state cell, scopes and submission bit,
with `Registry::route` / `Route` as the one owner lookup. What remains:

- `LifecycleTopology` (`discover.rs`, consumed in `account.rs` and `factory.rs`)
  is still keyed by bare strings rather than by registry slot.
- The optional families (submission, vacation, quota, sieve, contacts,
  calendars) live once on `Registry::families`, not per slot, so a shared
  account's family handles are not ordinary slot facts.
- About ten `foreign::owner_of` declared-owner reads remain, mostly in
  `pim.rs` plus `mutation.rs`; each should become `JmapRef::parse(..).owner`
  against `registry.route`.
- No mint site checks that a server-minted native id does not itself contain
  the `\u{1f}` separator; a non-conformant id containing it would silently
  route as foreign.

## STR-102 - JMAP refusals reach callers as prose

**Reported by:** jmap-account (lower priority than the rest).

`error::unsupported_error(op, scope, message)` is the universal refusal, and the
reason a caller would branch on lives only in the `DiagnosticText`: submission
absent, scheduled send window absent, foreign scheduled send, non-folder container
kind, label kind, `FlagOp` variant, range transport hook, raw-MIME projection,
legacy scope. `route_send_as` encodes a four-way precedence whose outcome a caller
can tell apart only by reading text. The kind is mostly right; a typed refusal
reason (or at least distinct message keys) would let a consumer tell "this account
never sends" from "not scheduled, not for shares". Related: STR-010, STR-074.

Blocked on bifrost-types: `RequestCause::Unsupported` carries only the
operation and message keys are derived from the kind, so a typed refusal reason
(with builder and accessor support) must land in bifrost-types first; the JMAP
sites are commented to say so.

## STR-104 - JMAP `pim.rs` does ten unrelated jobs, and `calendar_ops.rs` mixes doors with a conversion library

**Reported by:** jmap-account.

`pim.rs` is about 4,600 production lines (8,800 with tests): single-target mail
mutations, the submission pipeline with partial-commit records, drafts and the
attachment-handle codec, scheduled send, mail search with its own cursor codec and
envelope validation, `containers_list` plus RFC 9670 owner-email resolution,
identities, vacation, quota, thread and message hydration, JMAP <-> shared
`Message` mapping, and role-mailbox lookup. Natural seams: `mail_set.rs` (the one
`Email/set` executor shared with `mutation.rs`, BUG-071); `submission.rs` (send,
draft send, schedule, cancel, reschedule, partial-commit construction);
`drafts.rs`; `search.rs` (moved onto `page_cursor.rs`, STR-101); `containers.rs`;
`message.rs` beside `hydrate.rs`; `settings.rs` (identities, vacation, quota).
`calendar_ops.rs` (about 2,200 production lines) mixes the JMAP door logic with
RRULE <-> JSCalendar recurrence conversion, iCal `UNTIL` handling, ISO 8601
duration parsing (BUG-060) and time classification; the conversion half is a pure
mapping module, and the parts restating shared-vocabulary rules (window overlap,
recurrence validation) belong in `bifrost-types` (BUG-001). Same question for IMAP:
STR-079.

## STR-118 - Graph test layout mirrors the order the code was written

**Reported by:** graph.

About 450 lines of `#[cfg(test)]` scripting infrastructure (`ScriptedRest`,
`ScriptedRestResponse`, `ScriptedDownload`, the record and script methods) live
inside the production `client.rs` and belong in `client/test_support.rs`, like
IMAP's `account/test_support.rs`. `pim/tests.rs` is one 2,559-line file covering
nine modules, while `push/` splits its tests per arm. `account/ews_session_tests.rs`
sits at the account root though it tests `ews_stream`. `graph_error.rs` carries
about 1,900 lines of tests inline in three `mod` blocks.

## STR-119 - Graph event and contact sync lanes have no scope producer

**Reported by:** graph (needs a ruling, not a deletion: the published-API rule applies to the behaviour these lanes promise).

`scopes::discover_cursor_scopes_inner` emits only `ObjectType::Email` folder scopes
plus public folders, and the engine drives only discovered scopes. Reachable only
through a scope nothing in production discovers: `GraphCursorKind::Events` and
`Contacts`; the calendarView and contacts delta paths;
`CursorError::CalendarWindowExpired` and the reseed logic; `EVENT_DELTA_PREFER`;
`resource_for_scope`'s events and contacts arms; `sync_object_id`'s event and
contact arms; and the inventory tests for all of these. Either calendar and contact
folder discovery is missing, or the lanes are built ahead of a producer; either
way a large surface compiles and is tested with no path to it.

## STR-129 - Google's mailbox watch registry is process-global

**Reported by:** google.

`push::MAILBOX_WATCHES` is a `static LazyLock<Mutex<HashMap<String, Weak<..>>>>`,
process-wide rather than scoped to the `Net` or the factory; the tests mint unique
mailboxes because of it (`NEXT_MAILBOX` in `mod.rs` tests). bifrost-net already
offers Net-scoped string-keyed registries (`shared_hold`,
`shared_concurrency_limit`) for this kind of cross-account coordination; the
coordinator belongs in one, keyed by the typed mailbox identity (STR-124), so two
independent `Net`s really are independent.

Blocked on bifrost-net: its hold and concurrency registries cannot store
arbitrary provider state, and `AccountNet` exposes no Net-owned slot for it. The
move needs a Net-scoped provider-state store (a typed, keyed slot owned by the
`Net` and reachable through `AccountNet`) first; the registry site records this.

## STR-138 - Smaller SMTP axes and placement notes

**Reported by:** smtp-sasl.

The reply-mode enum, `MechanismChoice`, the credentials and challenge-framing
split, and the shared message-error strings landed. One residue needs a
bifrost-net API first: `client/metering.rs`'s blocking admission loop belongs
beside `BandwidthBudget` in bifrost-net as a blocking budget API, which does not
exist yet. (The rest of `authentication.rs` moving to bifrost-sasl is STR-135.)

## STR-139 - The SMTP sans-IO core stops one layer too low

**Reported by:** smtp-sasl.

The session machine, the shared `ReplyAssembler` (`client/reply.rs`, EOF now a
transport loss), and the transcript-driven `client/conformance.rs` suite have
landed. What remains: the send-path transcript suites are still twinned across
`client/connection.rs` and `client/async_connection.rs` (for example
`a_421_data_final_reply_aborts_instead_of_parking_a_dying_connection` and
`a_421_data_final_reply_aborts_the_batch_connection_too` exist in both), so a
send rule is still pinned twice and can drift once. Move them into the
conformance suite, run through both adapters.

## STR-141 - The SMTP 2x2 product at the public surface, and a Lettre-vestige executor that owns dialing

**Reported by:** smtp-sasl (both need owner rulings).

`SmtpTransport`, `LmtpTransport`, `AsyncSmtpTransport<E>`, `AsyncLmtpTransport<E>`
and four builders each repeat `hello_name`, `credentials`, `password`, `oauth2`,
`oauth2_source`, `authentication`, `dangerous_allow_insecure_auth`, `timeout`,
`bandwidth_metering`, `port`, `tls`, `pool_config`, `build`; each transport repeats
`test_connection`, `verify`, `expand`, `send_raw_with_options`, `send_raw_bdat*`,
`send_raw_batch_with_options` with the same body (validate, map, checkout, send,
resolve). `connection_url.rs` already introduced a private `TransportBuilder` trait to
paper over two of the four builders, and the setters drift in small ways (the async
LMTP builder's `tls` clears `unix_socket` and is redundantly `#[cfg(feature =
"tokio")]` inside an always-tokio file). Proposal: one `Builder<D: Dialect, R:
Runtime>` over `SmtpInfo` and one `Transport<D, R>` whose methods are written once
over a runtime-abstract connection (feasible once STR-139 lands), with the current
names surviving as type aliases (structs becoming aliases needs the owner).
Separately, `Executor` is sealed with exactly one implementor (`TokioExecutor`),
every async transport is generic over `E` with `#[allow(private_bounds)]` on most
methods, and `SmtpExecutor::connect` holds the TLS-mode dial sequence (CON-058);
dialing policy is not a property of the runtime. Moving `connect` out is internal;
dropping the `E` parameter needs the owner. `ConnectionOptions` exists "keeping the
dial signature stable across features" - a symptom of the parameter list living in
the wrong place.

## STR-152 - bifrost-dav-core is half a consolidation; the account layer is still mirrored in caldav and carddav

**Reported by:** net-dav (its other "substantial rewrite" verdict).

Steps (1) to (3) landed: a `Dialect` trait (`caldav/src/dialect.rs`,
`carddav/src/dialect.rs`), the generic collection kernel
(`dav-core/src/collection.rs`, `collection_account.rs`) and the shared
`Unsupported` lanes (`dav-core/src/unsupported.rs`). What remains:

- The two `client.rs` test modules still re-test dav-core's dispatcher under the
  same names in both crates (credential origin, cross-origin redirect, hop count,
  plaintext home, the discovery fallbacks, the multiget concurrency bound). They
  should move into dav-core, parameterized over both dialects.
- Small twins survive: `one_outcome_per_id` in both `account.rs` files, although
  `dav-core::collection_account::one_outcome_per_id` now exists, and
  `multiget_failure` / `listing_failure` in both `client.rs` files.
- bifrost-net now has a pass-through exchange mode (`RequestBuilder::
  statuses_as_data`), but DAV does not use it: `wire_outcome` still rebuilds the
  response from `AttemptHistory`, special-cases 501 for the MOVE fallback, and
  keeps its private refusal list. Moving DAV onto `statuses_as_data` (and onto
  net's redirect walk, 401 recovery, deadline and attempt evidence) is the
  residue of the net exchange-core work.

