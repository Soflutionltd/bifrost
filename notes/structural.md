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

## STR-036 - Vestigial engine surface

**Reported by:** sync-engine (published; removal needs the owner).

- `AckRequest::auto` is always `false`; nothing produces `true`, and the doc
  says it "remains for trace readability".
- `ThrottleBucket::record_for` ignores its account argument.
- `ThrottleBucket::forget_account` is a no-op that detach still calls.
- `BackfillWiring::changes_tx: Option<broadcast::Sender>` is only used as
  `is_some()`, a bool meaning "broadcast at all"; publication goes through
  `delivery`.

## STR-042 - `PendingCoverage` is five subsystems behind one lock and one public surface

**Reported by:** sync-lanes.

Residue. The guarded state is split into sub-structs (`Claims`, `RingCharge`,
`Fences`, `LossLedger`, `PublicationMint`) under one mutex, and the ordering-
sensitive methods are `pub(crate)`. Not done: the named transition facade
(`register_page`, `register_marker`, `acknowledge`, `retire`,
`invalidate_scope`, `on_receiver_read`, `on_receiver_departed`) was not built,
so the crate-internal surface is still about forty methods; and walk-generation
minting (`PendingCoverage::next_generation`, still `pub`) did not move to the
writer. Worth doing only if a new ordering defect shows the wide internal
surface is the cause.

## STR-043 - The backfill lane is split across `backfill/` and `engine/backfill.rs` along the wrong seam

**Reported by:** sync-lanes.

Plan, marker and scan moved into `backfill/`, and the parking tests moved to
`engine/`. What remains: the ack writer (`engine/writer/acknowledge.rs`)
still reads the partition key format directly through
`backfill::partitioner::is_completion_partition` rather than through a typed
key owned by `backfill/marker.rs`.

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
store implements, the time-key compaction contract) is paid now by every backend
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

**Reported by:** imap-protocol, imap-connection.

Search, sort and thread results are typed by addressing, ESEARCH's UID
indicator is checked against the command, the command sets are typed
(`SequenceSet<Seq>` versus `SequenceSet` of UIDs), `AppendUid` and `ModSeq`
landed, and `CommandKind` tells UID FETCH from FETCH. What remains: the
UID/sequence `Command` twins (`Search`/`UidSearch`, `Fetch`/`UidFetch`, ...)
are still separate variants doubling every match in `kind`, `mailbox_target`
and the encoder; one variant with an addressed target would halve them. The
wire-level `FetchResponse { seq: u32, uid: Option<u32> }` and
`Removal::Seq(u32)` are still bare numbers.

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

**Reported by:** imap-protocol, imap-account.

The set algebra, the FETCH merge and the `~imap-wire:` mailbox identity moved
to their owners. What remains is the registry side: `FolderEntry` bundles LIST
facts, ownership, MYRIGHTS, a cursor hint, a MODSEQ cache and a last-seen
instant behind four locks, and is the implicit owner of folder roles; making
roles, ownership and the shared-error mapping explicit registry
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
(`connection/request.rs`, BUG-029) so it stops being a gate bypass; (b) remove it and simplify
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
`capability_contract_tests.rs` can only check the false direction;
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
over a runtime-abstract connection (feasible now that the sans-IO session machine has landed), with the current
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

The dialect trait, the generic collection kernel, the shared `Unsupported`
lanes, the shared failure helpers and the move onto net's `statuses_as_data`
exchange landed. What remains: the discovery-level credential tests
(`discovered_cross_origin_https_home_receives_credentials`,
`cross_origin_redirect_is_redispatched_with_credentials`,
`discovered_plaintext_home_never_receives_credentials`) are still twinned in
both `client.rs` test modules and could move into dav-core, parameterized over
both dialects.

