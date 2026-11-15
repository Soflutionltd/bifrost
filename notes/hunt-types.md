# Hunt: bifrost-types - wrong shape, wrong place

Scope: `crates/types/` (the `Account` / `AccountFactory` contract, its
vocabulary, `src/error/`, `src/mime/`), checked against how the protocol
crates produce it and how `bifrost-sync` and `bifrost-net` consume it.

Source citations name files and functions, not line numbers.

Tags used below:

- **LIVE** - the sites already disagree, or a defect is observable now.
- **LATENT** - the sites agree today and nothing would catch them drifting.
- **SHAPE** - a structural proposal; no defect claimed.
- **RULING** - the fix moves, renames or removes a published item, so it needs
  the owner's decision before anyone builds it.

---

## 0. Headline findings, ranked

1. **LIVE - "does this event overlap the window" is decided twice, and the two
   answers already differ** (CalDAV `event_in_range` vs JMAP
   `event_in_range`). The JMAP copy lacks the dav-F9 zero-length rule, the
   all-day timezone exemption and the recurring-master retention, so it can
   drop events the server correctly returned. Root cause: `EventTime` is a
   `String` plus a sibling `is_all_day: bool`, so every crate re-parses it.
   Section 2.1.
2. **LATENT, edging LIVE - `MembershipScope::Mailbox(MailboxId)` means three
   different things**: a JMAP container, a shared-owner principal tag (JMAP and
   Graph), and, in the engine, a folder id under another name. `MailboxId`'s
   own doc comment says it is two things. Section 1.1.
3. **LATENT - kind and cause are two parallel taxonomies held in step by a
   runtime validator** (`kind_matches_cause`) and roughly 350
   `try_build().expect(..)` sites. Two of the enums are literally identical
   (`TransportKind` / `TransportErrorKind`). The kind is derivable from the
   cause almost everywhere. Section 2.3.
4. **LATENT - the stringly `BatchItemId` is silently reinterpreted as an
   `ObjectId`** at about a dozen engine sites, while other producers use it as
   a submission position. Three meanings, one `String`. Section 1.5.
5. **LATENT - six crates each decide how a foreign / older / newer cursor
   payload is classified**, with different internal branching and different
   operation stamps, converging on the same kind today by coincidence of
   maintenance. Section 2.2.
6. **LATENT - protocol identity has six vocabularies** (`ProtocolKind`,
   `Protocol`, `Provider`, `ProtocolSalt`, `PushSource`, dav-core's
   `DavProtocol`) and the mappings between them live outside `bifrost-types`,
   where `#[non_exhaustive]` forces wildcard arms. One of those wildcards,
   `default_salt_factory`'s `_ => ProtocolSalt::Imap`, is exactly the catch-all
   the `ProtocolSalt::CalDav` doc comment warns against. Section 2.4.
7. **SHAPE / RULING - `bifrost-types` carries the engine's consumer API**
   (`Control`, `DurableCheckpointSet`, `InvalidationSink`, `SinkPush`,
   `WatchEvent`, `AccountControl`, `PauseReason`). No account crate produces
   or consumes any of it. Section 3.1.
8. **SHAPE - classified failures are flattened to prose** at two degradation
   seams that have a typed sibling (`SkippedScope`): composed sub-account
   discovery failure in `merge_scope_streams`, and Graph's skipped shared
   mailbox during discovery. Section 1.12.

---

## 1. Axes that should be types

### 1.1 `MembershipScope::Mailbox` / `MailboxId` is overloaded (LATENT)

Producers:

- JMAP primary inventory: `MembershipScope::Mailbox(native mailbox id)`
  (`jmap/src/sync/inventory.rs`, the primary projection), and JMAP scope
  lifecycle emits `Created / Renamed / Deleted(MembershipScope::Mailbox(id))`
  for primary mailboxes (`jmap/src/sync/discover.rs`).
- JMAP foreign inventory: `qualify_foreign_memberships` rewrites each
  `Mailbox(native)` into `Folder(encode_foreign(owner, native))` and then
  APPENDS `Mailbox(owner)` as an owner tag. Same crate, same variant, opposite
  meaning.
- Graph: `Mailbox(owner)` is only ever the owner tag
  (`graph/src/account/foreign.rs::owner_tag`, `graph/src/account/inventory.rs`);
  folders are `Folder`.
- `ids.rs` documents `MailboxId` as "Either a JMAP mailbox id, or ... the
  shared/delegate-mailbox routing key".

Consumers:

- `sync/src/engine/attach.rs::scope_covers_membership` treats
  `Folder(id)` cursors as covering `Mailbox(id)` by string equality, justified
  by "Graph maps folders 1:1 to mailboxes" - which is not what Graph does any
  more; Graph's `Mailbox` is an owner.
- `sync/src/multiplexer/mod.rs::membership_scopes` maps a lifecycle
  `Mailbox(x)` onto `Folder(x)` / `FolderType { folder: x, .. }` cursor scopes.
  A provider that ever emits lifecycle for an owner tag would mint a cursor
  scope named after a principal.
- `ErrorScope::Mailbox { id: MailboxId }`, `ThrottleKey::Mailbox`,
  `SendAs::{As, OnBehalfOf}(MailboxId)`, `Container::owner: Option<MailboxId>`
  all use the owner meaning.

A JMAP account whose primary mailbox id equals a foreign `accountId` string
(both are short opaque server strings; Stalwart-style one-letter ids make this
plausible) produces two indistinguishable memberships.

**Proposal.** Split the axis: `MembershipScope::Owner(PrincipalId)` for owner
tags, keep `Mailbox(MailboxId)` for containers only, and give the routing key
its own newtype (`PrincipalId` / `OwnerId`) used by `Container::owner`,
`SendAs`, `ThrottleKey::Mailbox` and `ErrorScope::Mailbox`. Then
`scope_covers_membership` and `membership_scopes` have to state what an owner
tag covers instead of string-comparing it with a folder. Renaming
`ThrottleKey::Mailbox` / `ErrorScope::Mailbox` is RULING; adding `Owner` and
the new newtype is a shape change.

### 1.2 Owner-qualified ids travel as in-band separator strings (LATENT)

JMAP (`jmap/src/sync/foreign.rs`) and Graph (`graph/src/account/foreign.rs`)
each encode "this id belongs to principal P" as `"P\u{1f}native"` inside
`FolderId`, `ObjectId`, `ThreadId`, `BlobId`, `ContainerId`, and Graph adds a
second separator `\u{1e}` for public folders plus parsers for public address
books, contacts, calendars and events. Each crate also re-decides "a message
and the container it is filed into must have the same owner" (JMAP `owner_of`
doc comment; Graph `folder_owner` / `parse_message_id`).

Meanwhile `Container` stores the same fact structurally: `namespace`,
`owner`, `owner_local_id`, `owner_email`, and `native_id` (the encoded form).
The structural fields and the encoded string are two representations of one
fact, kept consistent by each producer.

The JMAP module doc argues a shared type "would be a false generalization"
because JMAP's owner is an `accountId` and Graph's is a `/users/{id}` key. That
argument is about the *meaning* of the owner string, which stays
provider-owned either way. What is shared is the *structure* - an optional
owner plus a native id, bare means primary, two operands must agree - and
`bifrost-types` has already generalized the owner itself as `MailboxId` in
`Container::owner`, `SendAs` and the owner tag.

**Proposal.** A `bifrost-types` `Qualified<T> { owner: Option<PrincipalId>,
native: T }` (or an `owner()` accessor plus a single codec) for the id
newtypes that can be foreign, with one encoding and one "same owner?" check.
Consumers would stop needing provider knowledge to tell a shared object from a
primary one. If the in-band string must survive for the engine's persisted
keys, the codec still belongs in one place.

### 1.3 `EventTime` is a string with a sibling bool (LIVE via 2.1)

`EventTime { value: String, timezone: Option<String> }` holds either RFC 3339
or `YYYY-MM-DD`, and whether it is a date lives on the enclosing
`CalendarEvent::is_all_day` / `EventCreate::is_all_day` /
`EventPatch::is_all_day`. Every crate re-derives "date or date-time" from
`value.len() == 10` or the bool, and re-parses: CalDAV `comparable_time`,
`caldav_query_time`, `ical_time_from_event_time`; JMAP `comparable_time`,
`local_datetime`, `end_from_start_duration`; Google `event_time`,
`google_range_bound`, `reject_unexpressible_all_day_patch`; Graph
`event_time`, `is_all_day_value`, `all_day_date`. The all-day-exclusive-end
contract on the doc comment is enforced by each producer by hand.
`ReminderTrigger::Relative { offset: String }` and `Absolute(String)` repeat
the pattern. `jiff` is already a dependency of this crate.

**Proposal.** `enum EventTime { Date(civil::Date), Floating(civil::DateTime),
Zoned(Zoned or Timestamp + tz) }`, with `is_all_day` derived rather than
stored, and an `EventRange` that owns overlap (2.1).

### 1.4 Flag identity is a rule restated, not a type (LATENT)

"Flags are ASCII-case-insensitive" is implemented three times in
`mutation.rs` (`canonical_flags_hash` lowercases, `FlagOp::validate` uses
`eq_ignore_ascii_case`, `flag_target_state` folds) and carried in
`HashSet<String>` (`FlagOp`, `HydratedObjectKind::FlagsOnly`, `Message::flags`,
`Account::flag_target`'s `observed`), which happily holds `\Seen` and `\SEEN`
as two members. `set_keyword(keyword: String)` and `FilterAction::SetKeyword`
are the same axis untyped.

**Proposal.** `Flag` newtype with case-folded `Eq`/`Hash`, and `FlagSet`.
The three functions then cannot disagree because the comparison is the
type's.

### 1.5 `BatchItemId(String)` has three meanings (LATENT)

- Streaming mutation and hydration lanes: producers mint
  `BatchItemId(object_id.0)` (`mutation.rs::refused_mail_target`, every
  provider), and the engine turns it back with `ObjectId(item.0)` in
  `sync/src/mutation/readback.rs` and many places in
  `sync/src/engine/bulk.rs`.
- `PushSubscription::all_succeeded`: `BatchItemId(index.to_string())`,
  a submission position.
- Vec-batch surfaces: caller-supplied ids validated by `validate_batch_input`.

Nothing makes "the streaming lane id is the requested ObjectId" true; the
IMAP `Target` permit work (`869ea04f`) found a site stamping the hydrated id
rather than the requested one.

**Proposal.** Make the key a type parameter: `ItemOutcome<K, T>` /
`BatchSuccess<K, T>`, with `K = ObjectId` on the streaming lanes, a
`SubmissionIndex` for positional ledgers, and `BatchItemId` only where the
caller mints it. The engine's rewrapping disappears.

### 1.6 Page cursors are bare `Option<Vec<u8>>` (SHAPE)

`Page::next_cursor` and the `page_cursor` inputs of `contacts_list`,
`directory_search`, `directory_groups_list`, `directory_group_expand`,
`SearchRequest`, `ContactSearchRequest`, `EventRange`, `EventSearchRequest`
are all `Option<Vec<u8>>`. A contacts cursor can be handed to the directory
search; each provider decodes and rejects a foreign one by its own rules.
A `PageCursor` newtype (optionally branded per surface) costs nothing.

### 1.7 Version numbers are interchangeable `u32`s (SHAPE)

`ChangeCursor::envelope_version` and `OpaqueChangeState::envelope_version`
are both `u32` and mean different things (the struct doc spends a paragraph
saying so); `BackfillCheckpoint::envelope_version` is a third. Distinct
newtypes (`TraitEnvelopeVersion`, `PayloadVersion`) make the swap a compile
error. See 2.2 for the decision that sits on top.

### 1.8 Send and scheduled-send handles are not object ids (SHAPE)

`SendReceipt::id: ObjectId` is documented as possibly an
`imapmsgid1:<Message-ID>` correlation token "which is not an addressable IMAP
object", and is also what `cancel_scheduled_send` / `reschedule_send` accept,
both typed `ObjectId`. `DraftHandle` exists "so the type system catches
mix-ups" and then `impl From<DraftHandle> for ObjectId` undoes it.

**Proposal.** `SendReceipt::id: SubmissionRef` enum (`Message(ObjectId)`,
`Scheduled(ScheduledSendHandle)`, `Correlation(String)`), with the scheduled
primitives taking `ScheduledSendHandle`.

### 1.9 Versions: read as `etag: Option<String>`, written as `ServerVersion` (SHAPE)

`ContactCard::etag` and `CalendarEvent::etag` are strings; the write side
takes `ServerVersion` through three different placements: a field on
`EventPatch` / `ContactPatch`, a parameter on `*_delete_if_version` /
`event_rsvp_if_version`, and `MailMutationTarget::expected_version`.
`ServerVersion::expected_etag` then has to accept both the DAV-normalized and
the wire form because the consumer had to wrap the read string itself.
Expose `version: Option<ServerVersion>` on the read shapes and pick one
placement for the write precondition.

### 1.10 Bool clusters that are enums (SHAPE)

- `ConvenienceShape::{replied_via_keyword, replied_via_extended_property}`
  and the forwarded pair: both may be `true`; `mark_replied` resolves it by
  `if` order. `starred` is already an enum (`StarredFlagShape`); the other
  two should be too.
- `MutationConcurrency::ExpectedVersion { 8 bools }` plus `None` plus
  `StateBased`: "no door" has two representations (`None` and all-false;
  IMAP's `build_capabilities` normalizes with an `if` chain, others need not),
  and `StateBased` is constructed nowhere. A `VersionedDoors` bitset with
  `is_empty()` is the whole axis.

### 1.11 HTTP/SMTP status lives in three places (LATENT)

`ServerErrorKind::Error { status }`, `ServerCause::Error { status }`, and
`DiagnosticInfo::status` (builder `.status()`). `recovery::derive_server`
reads the kind's status, falling back to the cause chain; the diagnostic
copy is never consulted; `kind_matches_cause` accepts a `None` on either
side. One status, owned by the cause.

### 1.12 Classified failures flattened to prose (SHAPE, one LIVE asymmetry)

- `account_compose.rs::drain_sub_discovery` folds a composed sub-account's
  `Terminated(AccountError)` into `Warning::support_only(WarningKind::Other,
  format!(... error.message_key() ...))`. The recovery class, scope and
  remediation are discarded; a consumer cannot tell a revoked CalDAV grant
  from a 503.
- Graph discovery (`graph/src/account/scopes.rs`) reports a skipped shared
  mailbox as prose (`"shared mailbox {mailbox} skipped: access denied during
  discovery"`), while `containers_list`, `search` and `AccountFactory::open`
  report the same event as a `SkippedScope` with a classified error. Same
  degradation, two vocabularies, chosen per surface.
- `Warning` has no slot for an error or a scope; `WarningKind::Other` has
  roughly twenty producers across imap, sync and types.
- `StateCause::OperatorOverrideNeeded { reason: String }` flows into
  `EngineDirective::OperatorOverrideRequired { reason: String }`: a machine
  caller branching on why an operator is needed has only prose.

**Proposal.** Give `Warning` an optional `SkippedScope` (or
`Option<AccountError>` + `Option<ErrorScope>`), and give discovery streams a
skip lane, so the degradation question has one answer shape everywhere.
Type the operator-override reason.

### 1.13 Smaller axes

- `CloudUploadMeta::size` "MUST equal `bytes.len()`" - an input that can only
  be wrong; derive it.
- `ByteRange` is documented "Inclusive" with `start` + `length`, while every
  other range in the crate is half-open `[from, to)`.
- `CoverageDomain::for_partition(scope, partition, uid_validity: u32)` takes
  the epoch even for non-UID partitions; JMAP's caller passes `0` as a
  sentinel. Put the epoch in `InventoryPartition::Uid` or return a domain that
  demands it.
- `SnapshotIdentity` derives `PartialEq` while `same_snapshot_as` documents
  that `==` is the wrong question; `CoverageDomain` derives `Eq` through it.
  The derived impl is a trap waiting for a caller.
- `InventoryEntry::differs_from` hand-lists every field; a new field is
  silently excluded. Destructure exhaustively (`let Self { id, .. } = self`
  without `..`). It also compares `memberships: Vec<_>` order-sensitively, so
  a provider reordering labels reports a change. `InventoryEntry::size`
  duplicates `Fingerprint::size`.
- `set_bandwidth_cap(Option<u64>)` documents `Some(0)` as a 1 B/s trickle; a
  `BandwidthCap` type would not admit 0.
- `CapabilityKey(String)` / `CapabilityValue(String)` / `CapabilityDelta`:
  never constructed anywhere in the workspace; every `CapabilityChanged`
  carries `delta: None`. Either compute it (the engine's reattach path holds
  both snapshots) or ask whether it should stay (RULING).
- `AttachmentInline::mime`, `attachment_upload(mime: String)`,
  `CloudUploadMeta::mime`, `MessageAttachment::content_type`: unparsed media
  types beside a crate that has `mime::parse_content_type`.
- `ObjectType::Event` and `ObjectType::CalendarEvent` name one domain. Graph
  treats them as aliases (`graph/src/account/inventory.rs::event_aliases`,
  `cursor.rs`, `push/webhook.rs`); `account_compose::route_typed_scope` routes
  only `CalendarEvent` to the calendar sub-account; the engine keys them as
  distinct `CursorScope`s. LATENT disagreement on whether they are the same.

---

## 2. Decisions made in more than one place

### 2.1 Event-window overlap (LIVE)

Question: does a `CalendarEvent` fall inside an `EventRange`?

Sites: `caldav/src/account.rs::event_in_range` and
`jmap/src/sync/calendar_ops.rs::event_in_range`, both applied as a local guard
to results the server already filtered. Helper bodies (`time_interval`,
`comparable_time`) are near copies.

They disagree:

| Case | CalDAV | JMAP |
|---|---|---|
| all-day event | always kept (floating DATE has no instant; the server had the tz) | compared as UTC midnight |
| zero-length event on the window start | kept (RFC 4791 s9.9, ruled dav-F9) | dropped (`event_end > range_start` is false) |
| recurring master starting before the window | kept unless UNTIL ends before it | dropped |
| zoneless local date-time | unparsed, so kept | parsed as UTC |

The JMAP behaviour plausibly drops events a JMAP server's `after`/`before`
filter returned (JMAP Calendars matches on any occurrence). Google and Graph
answer the same question by delegating to the server and normalizing bounds
(`google_range_bound`, Graph `events_in_range`), a third and fourth reading.

Owner: `bifrost-types`, once `EventTime` is typed (1.3):
`EventRange::admits(&CalendarEvent) -> bool` with the RFC 4791 rules, used by
both DAV and JMAP. Both crates have same-named tests
(`range_filter_includes_all_day_on_window_start`) that would move with it.

### 2.2 Cursor payload ownership and version gate (LATENT)

Question: is this stored `OpaqueChangeState` mine, and is its payload version
readable?

Sites: `imap/src/account/envelope.rs::decode_cursor`,
`carddav/src/account.rs::decode_cursor_snapshot`,
`caldav/src/account.rs` (same shape), `graph/src/account/cursor.rs::decode_cursor`,
`jmap/src/sync/state.rs::decode`, `google/src/account/cursor.rs::decode_gmail_state`.

Today they converge on `SyncState(SchemaIncompatible)`, but by different
routes: Graph and JMAP split newer (`EnvelopeUnknown`) from older
(`SchemaIncompatible`); IMAP, DAV and Gmail do not distinguish direction; DAV
merges protocol and version into one message; IMAP stamps
`AccountOperation::EstablishCursor` even when decoding for `changes_stream`;
Gmail mints its own message keys. The outer envelope check
(`ChangeCursor::validate_envelope`) is already centralized; the inner one is
not.

Owner: `OpaqueChangeState::payload_for(protocol, version) ->
Result<&[u8], PayloadMismatch { Protocol, Older, Newer }>` plus one
`AccountError` constructor for it, so the classification is stated once and
each crate supplies only operation and scope.

### 2.3 Kind vs cause (LATENT)

Question: what class of failure is this?

Every producer answers it twice, `AccountErrorBuilder::new(kind, cause)`,
and `recovery::kind_matches_cause` rejects disagreement at runtime. The
pairs are nearly 1:1: `AuthErrorKind` / `AuthCause`, `AccessErrorKind` /
`AccessCause`, `SyncStateErrorKind` / `StateCause`, `ServerErrorKind` /
`ServerCause`, `InternalErrorKind` / `InternalCause::kind`, and
`TransportErrorKind` / `TransportKind`, which are the same three variants
under two names. The exceptions are small: `Request(Malformed)` accepts
`Malformed | InvalidArgument`; `Protocol(_)` accepts any `Wire(_)` (the
protocol kind is not in the cause at all); `ServerErrorKind::Error`'s status
may be absent on either side.

Builder-time invariants are further runtime checks of shapes a type could
make unconstructable: `throttle_scope` / `throttle_budget` only on
`RateLimited | QuotaExhausted` (they belong inside those `ServerCause`
variants), `CursorInvalid` requires an `ErrorScope::Cursor` (it belongs in
`StateCause::CursorInvalid { scope }`). And the sibling scope-requiring kinds
disagree on the rule: `CursorInvalid` without a cursor scope is rejected, while
`ScopeRevoked` and `ScopeCapabilityLost` without one silently become
`RestartAccount` (`derive_sync_state`).

Owner: make the cause primary. `AccountErrorBuilder::new(cause)` with
`Cause::kind()` derived; move the `ProtocolErrorKind` into `WireCause` (or a
`Cause::Protocol { kind, wire }`); carry throttle and scope data in the causes
that need them. This deletes `kind_matches_cause`, `KindCauseMismatch`,
`ThrottleScopeNotApplicable`, `ThrottleBudgetNotApplicable`,
`CursorInvalidWithoutScope` and most of the `.expect(..)` sites. Changing the
builder's signature is a shape change; removing `TransportKind` or the
build-error variants is RULING. This is a big rewrite of every error
translation boundary and I think it is the right one: the current shape asks
~350 call sites to restate a fact the type system could own.

### 2.4 Protocol identity and its mappings (LATENT, one LIVE wildcard)

Vocabularies: `cursor::ProtocolKind` (6), `error::Protocol` (9 + `Unknown`,
adds Smtp, Lmtp, Ews), `error::Provider` (vendors), `mutation::ProtocolSalt`
(6, string payloads always empty), `events::PushSource`, and dav-core's
`DavProtocol`. Plus `FilterCondition::ProviderExpression { provider: Provider }`
using the vendor enum where every other "which protocol minted this" field
uses `ProtocolKind`.

Mappings, all outside the defining crate and therefore forced into wildcards:

- `sync/src/recovery.rs::checkpoint_protocol`: `ProtocolKind -> Protocol`,
  `_ => Protocol::Unknown`, derived from a checkpoint even though the engine
  holds the account and `Account::protocol()`.
- `sync/src/mutation/idempotency.rs::default_salt_factory`: `ProtocolKind ->
  ProtocolSalt`, `_ => ProtocolSalt::Imap` - the catch-all the
  `ProtocolSalt::CalDav` doc comment says stops being harmless "the moment the
  salt acquires meaning".
- dav-core `error.rs`: `DavProtocol -> Protocol`.
- `recovery::derive_server` decides "is this an SMTP reply" from
  `Cause::Wire(WireCause::Smtp(_))` in the chain (`smtp_reply`), not from the
  error's own `protocol` field, so "which protocol" is answered by two fields
  of one error.

`Account::protocol()` defaults to `Protocol::Unknown` "for compatibility",
yet every implementation in the workspace overrides it. Providers also stamp
`.provider(..)` and `.protocol(..)` at each builder call (Gmail ~15 sites,
Graph ~14) although the account knows both once.

Owner: `bifrost-types`. `From<ProtocolKind> for Protocol` (exhaustive inside
the crate), make `Account::protocol()` required, and stamp protocol/provider
at the account boundary (an error context the account owns) rather than at
each builder. Collapsing the enums is RULING.

### 2.5 Which push lanes run (LATENT)

Question: does this account run push, and on which lanes?

Answered by `AccountCapabilities::push: PushCapability` and by
`Account::push_lanes()` / `PushLaneKind`. The engine re-combines them at three
sites (`sync/src/engine/attach.rs` when seeding push health,
`sync/src/push/lanes.rs::PushLanes::run`, `sync/src/engine/reattach.rs` when
carrying subscriptions), each spelling `if caps.push == None { [] } else {
push_lanes() }`. The three non-`None` variants carry nothing the engine reads;
`InProcess` vs out-of-process is already per lane in `PushLaneKind`, and
`OutOfProcessPubsub` vs `WebhookOrEwsStream` encode provider identity.

Owner: derive the capability from the lanes (`None` iff no lanes; in-process
iff any `Connection` lane), or put one `effective_push_lanes(&dyn Account)` in
`bifrost-types`.

### 2.6 Partitions of `RecoveryClass` outside the crate (LATENT)

- `google/src/account/error.rs::requires_stream_termination`: which classes
  leave per-item lanes and end the stream; `_ => true`.
- `dav-core/src/error.rs::recovery_rank`: which class dominates a multi-leg
  failure; `_ => u8::MAX`.

Both are questions about the type itself, answered where `#[non_exhaustive]`
makes them non-exhaustive. Owner: methods on `RecoveryClass`
(`is_item_local()`, `severity()`), next to `is_terminal()`, where a new
variant is a compile error. Other crates likely make the per-item vs
stream-terminal call implicitly; giving it a name is how to find them.

### 2.7 Capability flag vs method refusal (LATENT; struct ruled to stay)

`PimMethodSupport` was ruled (2026-10-03) to stay, and nothing here proposes
removing it. The duplicated decision remains: each provider sets a flag in
`capabilities.rs` and independently refuses in the method body. Related sites
the ruling does not cover:

- `MutationConcurrency::ExpectedVersion`'s doors are read by no consumer in
  the workspace. The engine sends `MailMutationTarget` versions without
  consulting them (`sync/src/engine/bulk.rs`), and the trait's default
  `bulk_*_if_version` refuses without consulting them either.
- The `send_as` rule (which of `Unsupported(Send)` and `Request(Malformed)` a
  rejected `Some(send_as)` gets, keyed on the flag) is restated by each of
  Graph and JMAP, held by `capability_contract_tests.rs`, which caught both
  answering wrong once.
- IMAP's `build_capabilities` hand-merges composed DAV sub-accounts' doors and
  hard-codes `event_rsvp: false` rather than reading CalDAV's.

A non-removing option: a `bifrost-types` helper that turns a flag into the
canonical refusal (`caps.pim_methods.gate(AccountOperation::X)?`,
`caps.mutation.concurrency.gate_version(door, version)?`,
`classify_send_as(&caps, ..)`), so the error answer is read from the flag at
the one place it is decided.

### 2.8 Smaller duplicated decisions

- **"At least one recipient"**: `mime::render::send_request_to_rfc5322`, Google
  `pim.rs` (its own `require_recipient` branch), IMAP `pim.rs` draft send, JMAP
  scheduled send. Owner: a `SendRequest` / `DraftPatch` validator in
  `compose.rs`.
- **Inventory cursor classification**: `Account::inventory_resume_stream` and
  `Account::is_inventory_cursor` "must accept EXACTLY" the same cursors; only
  Graph overrides, and only one of them. One method returning a classification
  (`Option<InventoryResume>`) removes the second answer.
- **Throttle budget identity**: an implementation must declare in
  `throttle_keys` every budget its errors stamp. Google mints the same budget
  at `client.rs` (quota project) and again in `error.rs::translate_net_error`
  from the `LocalHold` key string; Graph mints in `client.rs`. A per-crate
  budget constructor used by both the declaration and the stamp would make
  the pairing structural.
- **Lossy vs checked address parsing**: `Address::parse_list` and
  `parse_list_checked` are two doors to one parser, and each call site decides
  whether silent recipient loss is acceptable. Google's `Message` hydration
  in `google/src/account/pim.rs` reads one `To` header (`header_from`) lossily, while its draft path reads every repeated
  header checked; a hydrated `Message` that dropped recipients does not set
  `incomplete`. A single `AddressList { accepted, rejected }` return would
  make the loss visible to every caller.
- **Composite capabilities**: only IMAP composes sub-accounts and only IMAP
  knows how to merge their `AccountCapabilities`. `account_compose.rs` claims
  to be the reusable composition layer ("tomorrow JMAP / Graph") but holds no
  capability merge.

---

## 3. Structure

### 3.1 The engine's consumer API lives in the contract crate (SHAPE, RULING)

`Control`, `DurableCheckpointSet`, `InvalidationSink`, `SinkPush`,
`WatchEvent`, `AccountControl` and `PauseReason` are produced and consumed
only by `bifrost-sync` and its consumer; no protocol crate touches them
(JMAP uses `WatchEvent` in a test). `SyncControl` is the only `Control`
implementation. They make `bifrost-types` the place an engine API change
lands, and `events.rs` the file that mixes stream plumbing, inventory,
push ingress and control.

Move them to `bifrost-sync` (re-exported there already in practice). This
moves published items: RULING.

### 3.2 Provider wire vocabularies in the neutral crate (SHAPE)

`GraphSignal`, `JmapMethod`, `ImapResponseCode`, `GmailSignal`,
`EnhancedStatusCode` enumerate each provider's native error codes inside
`bifrost-types`, so adding a Graph error code is a change to the contract
crate. Only one central decision reads them (`smtp_reply`, 2.4). Options: a
`WireCause::Native { protocol, code }` with a provider-supplied typed code
behind a trait, or leaving the enums and conceding they are shared
vocabulary. If they stay, `derive_server` should key on `protocol`, not on the
`WireCause` variant.

### 3.3 MIME in `bifrost-types` (SHAPE)

`src/mime/` (parser, header decoder, transfer decoding, body selection,
renderer, ~4,000 lines with tests) is why the contract crate depends on
`base64`, `encoding_rs` and `jiff` for MIME. Its consumers are essentially IMAP
and Google. Meanwhile `bifrost-smtp` carries a second,
lettre-derived RFC 5322 builder (`smtp/src/message/`), so header encoding,
address formatting and boundary generation are decided by two published
renderers. A `bifrost-mime` crate depended on by types (for `Address`) or the
reverse, and an explicit ruling on how `smtp::message` relates to it, would
put each decision in one crate.

### 3.4 `account_compose.rs` (SHAPE)

Name collides with `compose.rs` (mail composition) though it is account
composition; it is used only by IMAP; its tests live in IMAP by design; it
holds routing and discovery fan-in but not the capability merge (2.8) or the
`ObjectType::Event` alias decision (1.13). Either grow it into the real
composition layer (`composition.rs`: routing, discovery merge with a typed
skip lane, capability merge) or move it into `bifrost-imap` until a second
composer exists.

### 3.5 `account.rs` mixes the contract with policy (SHAPE)

The trait file is ~1,750 lines and its default-impl tier encodes product
policy: `dispatch_label` hard-codes a `ProtocolKind` switch (Gmail -> label
membership, Graph -> category, others -> keyword) while the very next
convenience, `set_starred`, argues at length that capability dispatch is
"the chosen design" over a protocol switch. `set_starred` also hard-codes
Gmail's `"STARRED"` label id and reuses `"$flagged"` as a Graph category
name; `mark_replied` / `mark_forwarded` hard-code MAPI
`PR_LAST_VERB_EXECUTED` codes. Put the conveniences in their own module (an
extension trait with a blanket impl, or free functions over `&dyn Account`),
dispatch them uniformly through `ConvenienceShape` (add a `LabelShape`), and
let the providers own their sentinel strings.

### 3.6 Write-only capability fields (SHAPE, RULING to remove)

No reader anywhere in the workspace outside producers and their tests:
`cursor_freshness`, `blob_digest_pre_download`, `rate_limit_class`,
`quota_signal`, `requires_uidvalidity_recheck`, `historyid_expires_after`,
`delta_token_expires_after` (the last two are `None` in every provider).
`RateLimitClass`'s doc says "Used by `bifrost-net` and the engine"; neither
reads it. `BatchingPolicy` is read only by Graph, of its own value, and its
doc's per-protocol defaults are stale (IMAP declares 1024 items, not 5000;
Gmail's wait is 75 ms, not 200 ms). These may be consumed by ratatoskr, so
the question for the owner is wire-or-retire, not delete. The protocol-named
fields (`requires_uidvalidity_recheck`, `historyid_*`, `delta_token_*`) are
the wrong shape for a protocol-neutral snapshot either way.

### 3.7 `IdempotencyKey` is threaded and unread (SHAPE)

Every bulk method takes an `IdempotencyKey`; no account reads `run_id`,
`sequence` or `protocol_salt`. The salt's string payloads are always empty.
Its only function is engine bookkeeping, which does not need to cross the
account boundary. Wire-or-retire question for the owner (RULING).

### 3.8 Three hand-kept enumerations of the trait surface (SHAPE)

`Account` methods, `PimMethodSupport` fields and `AccountOperation` variants
mirror each other by hand (the `PimMethodSupport` doc concedes it). Within the
2026-10-03 ruling, a declarative macro listing each optional method once and
generating the flag field, the operation variant and the default refusal
would keep the struct and remove the drift surface.

### 3.9 Parallel range vocabularies (SHAPE)

`InventoryPartition { Time, Uid, Page }`, `CoverageCoordinate { TimeRange,
UidRange, PageRange }` and `InventoryPartitioning { TimeWindowed, UidRange,
PageCount }` restate the same three windows with different names and integer
widths (`Uid` u64 endpoints vs `max_uid: Option<u32>`), joined by
`CoverageDomain::for_partition`. Shared `TimeWindow`, `UidWindow`,
`PageWindow` half-open types (with `from <= to` enforced, which nothing checks
today) would carry the convention the reference doc had to fix by hand once.

### 3.10 Hydration has two projection enums (SHAPE)

`mutation::Projection` (for `get_stream`) and `hydration::HydrationProjection`
(for `message_hydrate`) overlap (`Preview(usize)`, `Full`, `FullWithBlobs`),
and `HydratedObjectKind`'s variant is only loosely tied to the projection
requested ("consumers down-cast"). The read-back guard depends on a
`FlagsOnly` request yielding `FlagsOnly`; that is validated in the engine, not
by the type. Relatedly, a hydrated `Message` places the message by
`containers: Vec<ContainerId>` while inventory uses
`memberships: Vec<MembershipScope>`; nothing maps one to the other without
provider knowledge (Gmail fills the former with label ids, the latter with
`MembershipScope::Label`).

### 3.11 Blob capability at two levels, trusted from caller data (SHAPE)

`AccountCapabilities::blob_range` and `blob_digest_pre_download` restate, at
account level, what `BlobHandle::capabilities` says per handle. The handle is
a public, caller-constructible struct, and Graph's `open_blob_range` trusts
`handle.capabilities.supports_range` to decide whether to send a ranged
request; a forged or stale handle steers the provider. Gmail and JMAP refuse
regardless. Providers should re-derive handle capability from the `BlobId`
they minted.

---

## 4. Lateral findings

- **Probable bug**: JMAP `events_in_range` local guard drops zero-length
  events on the window start, recurring masters whose first instance precedes
  the window, and may misplace all-day events across offsets (2.1).
- **Smell with a stated hazard**: `default_salt_factory`'s `_ =>
  ProtocolSalt::Imap` (2.4).
- **Stale engine comment and policy**: `scope_covers_membership`'s "Graph maps
  folders 1:1 to mailboxes" no longer describes Graph, and the string-equality
  rule it justifies is the third meaning of `Mailbox` (1.1).
- **Graph trusts caller-forgeable handle flags** (3.11).
- **Stale docs**: `BatchingPolicy` defaults, `RateLimitClass` readers
  (3.6); `account_compose` module doc's "tomorrow JMAP / Graph"; types.md says
  `DecodedAttachment` is "mapped onto `MessageAttachment` by each protocol
  crate" but only IMAP calls `select_body`.
- **Dead vocabulary**: `CapabilityDelta` and friends (1.13),
  `MutationConcurrency::StateBased` (1.10).
- **`unsupported_inventory_stream`** keeps a `_scope` parameter "for
  compatibility" and defaults to `Protocol::Unknown`; the `_with_protocol`
  sibling is what every caller should reach for. Fold once `protocol()` is
  required (2.4).
- **`PushSubscription`** validates handle/lane agreement in constructors but
  keeps public fields, so the invariant is advisory. Same pattern as `Batch`
  (accepted there with an engine-side check); here no consumer re-checks.
- **`Container`** carries `kind` and `provenance.kind`, `native_id` and
  `provenance.native`: duplicated public fields with no check that they agree.
