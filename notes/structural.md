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

## STR-001 - `MembershipScope::Mailbox` / `MailboxId` means three things

**Reported by:** types (LATENT, edging LIVE), jmap-account (latent collision).

Producers: JMAP primary inventory emits `MembershipScope::Mailbox(native
mailbox id)` and JMAP lifecycle emits `Created/Renamed/Deleted(Mailbox(id))`
for primary mailboxes; JMAP foreign inventory (`qualify_foreign_memberships`)
rewrites each `Mailbox(native)` into `Folder(encode_foreign(owner, native))`
and then APPENDS `Mailbox(owner)` as an owner tag - same crate, same variant,
opposite meaning. Graph's `Mailbox(owner)` is only ever the owner tag
(`graph/src/account/foreign.rs::owner_tag`). `ids.rs` documents `MailboxId` as
"Either a JMAP mailbox id, or ... the shared/delegate-mailbox routing key".

Consumers: `sync/src/engine/attach.rs::scope_covers_membership` treats
`Folder(id)` cursors as covering `Mailbox(id)` by string equality, justified by
"Graph maps folders 1:1 to mailboxes" - which no longer describes Graph.
`sync/src/multiplexer/mod.rs::membership_scopes` maps a lifecycle `Mailbox(x)`
onto `Folder(x)` / `FolderType { folder: x, .. }`; a provider emitting
lifecycle for an owner tag would mint a cursor scope named after a principal.
`ErrorScope::Mailbox { id }`, `ThrottleKey::Mailbox`, `SendAs::{As,
OnBehalfOf}(MailboxId)`, `Container::owner` all use the owner meaning. A JMAP
account whose primary mailbox id equals a foreign `accountId` (Stalwart-style
one-letter ids make this plausible) produces two indistinguishable memberships.

Proposal: `MembershipScope::Owner(PrincipalId)` for owner tags, `Mailbox` for
containers only, and a `PrincipalId`/`OwnerId` newtype for `Container::owner`,
`SendAs`, `ThrottleKey::Mailbox` and `ErrorScope::Mailbox`. The engine then has
to state what an owner tag covers instead of string-comparing it with a folder.
Renaming `ThrottleKey::Mailbox` / `ErrorScope::Mailbox` needs the owner; adding
`Owner` and the newtype is a shape change.

The jmap-account hunter reached the same collision hazard from inside JMAP:
`discover::memberships` emits the primary account's mailbox ids as
`MembershipScope::Mailbox(id)` in the same list as the owner tags
`MembershipScope::Mailbox(accountId)` (from `foreign::owner_tag`), and servers
minting short sequential ids for both (Stalwart uses single-letter base32 ids for
both) can produce a primary mailbox and a shared account with the same id. The
shared account's identity is typed as a mailbox id almost everywhere it crosses a
function boundary: `changes::stream`, `inventory::stream`, `email_inventory_loop`
take `owner: Option<TypesMailboxId>` (and `changes::stream` additionally takes
`account_id: String` and the routed `mail` handle - three parameters for one fact,
assembled from `mail_for_scope`, `account_id_for_scope`, `owner_of_scope`);
`container_from_mailbox` sets `Container::with_owner(MailboxId(accountId))`;
open-time skips use `ErrorScope::Mailbox { id: accountId }`. It too wants a
first-class `MembershipScope::Owner(..)` (or a field on the entry) in
`bifrost-types`.

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

The jmap-account hunter calls this the crate's dominant missing type and counts
the JMAP decode sites: `account.rs` `route_object_id`, `route_mutation_target`
(unregistered owner -> primary, literal), `resolve_foreign_account_id` and
`owner_of_scope` (`None`), `is_unregistered_foreign` (terminal `ScopeRevoked`),
`hydrate.rs` `route_for_id` / `HydrationRoute`, `mutation.rs`
`account_id_for_target`, `blob.rs` `foreign_split`, `pim.rs` `thread_owner` (all
primary, literal), `pim.rs` `search_owner` / `route_search` (`Request(Malformed)`),
the `owner_of` callers (ignore registration), `push.rs` `PushRouting::new`
(dropped). The wire form is derived four times, and they already disagree
(latent): `hydrate::wire_object_id(id, owner)` strips only when the route is
foreign and the owner matches; `mutation::wire_email_id(id, account_id,
foreign_account: bool)` only when the caller passed `true` and the owner matches;
`mutation::wire_mailbox_id` likewise; `pim::wire_id_for_mail(id, account_id)`
strips whenever the owner equals `mail.id_str()`, INCLUDING on the primary route,
so a primary-qualified id is stripped by `pim` and kept literal by `hydrate` and
`mutation` (no producer mints that shape today; `hydrate::wire_object_id`'s doc
says it "must match what the mutation pipeline does"). Nothing validates at the
mint sites that a server-minted native id does not itself contain `\u{1f}`; the
codec's soundness rests on RFC 8620 id syntax, so a non-conformant primary id
containing the separator silently routes as foreign. Its shape: `enum Owner {
Primary, Shared(JmapAccountId) }`, `struct JmapRef<K> { owner, native: Id<K> }`,
`enum Route<'a> { Primary(&'a Slot), Shared(&'a Slot), Departed(JmapAccountId) }`;
`JmapRef::parse` refuses a native part containing the separator,
`JmapRef::to_consumer()` is the one encoder, `Route::wire(&ref)` the one stripping
rule, and each door states its `Departed` policy in one `match`. The prose
distinction between "declared owner" (`owner_of`) and "reachable owner" (the
registered predicates) becomes `ref.owner` vs `registry.route(&ref)` (STR-097).

## STR-003 - `EventTime` is a string with a sibling bool

**Reported by:** types (LIVE through BUG-001).

`EventTime { value: String, timezone: Option<String> }` holds either RFC 3339 or
`YYYY-MM-DD`; whether it is a date lives on the enclosing
`CalendarEvent::is_all_day` / `EventCreate::is_all_day` /
`EventPatch::is_all_day`. Every crate re-derives "date or date-time" from
`value.len() == 10` or the bool and re-parses: CalDAV `comparable_time`,
`caldav_query_time`, `ical_time_from_event_time`; JMAP `comparable_time`,
`local_datetime`, `end_from_start_duration`; Google `event_time`,
`google_range_bound`, `reject_unexpressible_all_day_patch`; Graph `event_time`,
`is_all_day_value`, `all_day_date`. The all-day-exclusive-end contract is
enforced by each producer by hand. `ReminderTrigger::Relative { offset: String
}` and `Absolute(String)` repeat the pattern. `jiff` is already a dependency.

Proposal: `enum EventTime { Date(civil::Date), Floating(civil::DateTime),
Zoned(..) }`, `is_all_day` derived, and an `EventRange` that owns overlap.

## STR-004 - Flag identity is a rule restated, not a type

**Reported by:** types (LATENT).

"Flags are ASCII-case-insensitive" is implemented three times in `mutation.rs`
(`canonical_flags_hash` lowercases, `FlagOp::validate` uses
`eq_ignore_ascii_case`, `flag_target_state` folds) and carried in
`HashSet<String>` (`FlagOp`, `HydratedObjectKind::FlagsOnly`, `Message::flags`,
`Account::flag_target`'s `observed`), which holds `\Seen` and `\SEEN` as two
members. `set_keyword(keyword: String)` and `FilterAction::SetKeyword` are the
same axis untyped. Proposal: a `Flag` newtype with case-folded `Eq`/`Hash` and
a `FlagSet`, so the three functions cannot disagree.

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

## STR-006 - Page cursors are bare `Option<Vec<u8>>`

**Reported by:** types.

`Page::next_cursor` and the `page_cursor` inputs of `contacts_list`,
`directory_search`, `directory_groups_list`, `directory_group_expand`,
`SearchRequest`, `ContactSearchRequest`, `EventRange`, `EventSearchRequest`. A
contacts cursor can be handed to directory search; each provider rejects a
foreign one by its own rules. A `PageCursor` newtype (optionally branded per
surface) costs nothing.

The graph hunter found five untagged cursor formats inside Graph alone: bare link
bytes (`contacts::link_cursor`, `paging::decode_link_cursor`); `PagedCursor { url,
skip, mode }` JSON (`paging::encode_paged_cursor` and `encode_directory_cursor`);
`b"\x1fsearchapi:" + offset` (calendar search); the `Position` structs in
`contacts/public.rs` and `calendar_public.rs`; and `SearchPosition` in
`calendar_public.rs`. `decode_paged_cursor` falls back to reading a non-JSON input
as a link cursor, so a cursor minted by one surface decodes on another.
`DirectoryMode` lives in `paging.rs` though only directory search uses it, and
`encode_paged_cursor` always stamps `DirectoryMode::Filter`. Its owner: one
`GraphPageCursor { surface, .. }` enum with a single versioned codec and a surface
tag checked on decode, turning a cross-surface cursor into `Request(Malformed)`.
JMAP's two codecs sharing tag `2` are STR-101.

The google hunter reports Google has no cursor type at all, and a malformed (not
UTF-8) page cursor is classified three ways: `pim::page_token` uses
`other_error(Search)` (so `search_messages` reports operation `Search`); `calendar`
uses `caller_input_error`; `contacts` uses `caller_input_error` with the raw
`FromUtf8Error` text. JMAP classifies a stale or malformed page cursor as
`SyncState(SchemaIncompatible)` (BUG-069). Google should have a `PageCursor` whose
refusal matches the workspace answer.

## STR-007 - Envelope version numbers are interchangeable `u32`s

**Reported by:** types.

`ChangeCursor::envelope_version` and `OpaqueChangeState::envelope_version` are
both `u32` and mean different things (the struct doc spends a paragraph saying
so); `BackfillCheckpoint::envelope_version` is a third. Distinct newtypes
(`TraitEnvelopeVersion`, `PayloadVersion`) make the swap a compile error. The
decision on top is BUG-073.

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

## STR-009 - Bool clusters that are enums

**Reported by:** types.

- `ConvenienceShape::{replied_via_keyword, replied_via_extended_property}` and
  the forwarded pair may both be `true`; `mark_replied` resolves it by `if`
  order. `starred` is already an enum (`StarredFlagShape`); the other two
  should be.
- `MutationConcurrency::ExpectedVersion { 8 bools }` plus `None`: "no door" has
  two representations (`None` and all-false; IMAP's `build_capabilities`
  normalizes with an `if` chain, others need not). A `VersionedDoors` bitset
  with `is_empty()` is the whole axis. (`StateBased` is already an owner-ruling
  item in `notes/todo.md`.)

## STR-010 - Classified failures flattened to prose at degradation seams

**Reported by:** types.

- `account_compose.rs::drain_sub_discovery` folds a composed sub-account's
  `Terminated(AccountError)` into `Warning::support_only(WarningKind::Other,
  format!(... message_key ...))`, discarding recovery class, scope and
  remediation; a consumer cannot tell a revoked CalDAV grant from a 503.
- Graph discovery (`graph/src/account/scopes.rs`) reports a skipped shared
  mailbox as prose, while `containers_list`, `search` and `AccountFactory::open`
  report the same event as a classified `SkippedScope`.
- `Warning` has no slot for an error or a scope; `WarningKind::Other` has
  roughly twenty producers across imap, sync and types.
- `StateCause::OperatorOverrideNeeded { reason: String }` flows into
  `EngineDirective::OperatorOverrideRequired { reason: String }`; a machine
  caller branching on why an operator is needed has only prose.

Proposal: give `Warning` an optional `SkippedScope` (or `Option<AccountError>` +
`Option<ErrorScope>`), give discovery streams a skip lane, and type the
operator-override reason.

The imap-account hunter adds the strategy downgrade: `bifrost_types::StrategyDowngrade`
exists (used only by the dead `error::strategy_failure`), but every live
downgrade is `Warning::support_only(WarningKind::StrategyDowngraded, reason)`
with `protocol_detail = format!("{from:?}->{to:?}")` (`run_inventory`,
`send_strategy_downgrade`), and the scripted tests assert on that Debug string
(`downgrade_detail`); a consumer wanting "this folder is now on Basic" must parse
Rust Debug output. Make the kind carry the typed value
(`WarningKind::StrategyDowngraded(StrategyDowngrade)` or a structured field).
Also prose-only: `warn_if_uid_count_mismatch` (`WarningKind::Other`, counts in
prose), `DavScopeIndex::build` collisions (`OperatorAttentionNeeded`, href only
in text), and `push_stream`'s "NOTIFY SET refused: ..." (`Other`).

## STR-011 - Smaller axes in `bifrost-types`

**Reported by:** types.

- `CloudUploadMeta::size` "MUST equal `bytes.len()`": an input that can only be
  wrong; derive it.
- `ByteRange` is documented "Inclusive" with `start` + `length`, while every
  other range in the crate is half-open.
- `CoverageDomain::for_partition(scope, partition, uid_validity: u32)` takes the
  epoch even for non-UID partitions; JMAP passes `0` as a sentinel. Put the
  epoch in `InventoryPartition::Uid`.
- `SnapshotIdentity` derives `PartialEq` while `same_snapshot_as` documents that
  `==` is the wrong question; `CoverageDomain` derives `Eq` through it.
- `InventoryEntry::differs_from` hand-lists every field, so a new field is
  silently excluded (destructure exhaustively instead); it compares
  `memberships: Vec<_>` order-sensitively, so a provider reordering labels
  reports a change. `InventoryEntry::size` duplicates `Fingerprint::size`.
- `set_bandwidth_cap(Option<u64>)` documents `Some(0)` as a 1 B/s trickle; a
  `BandwidthCap` type would not admit 0.
- `CapabilityKey(String)` / `CapabilityValue(String)` / `CapabilityDelta` are
  never constructed; every `CapabilityChanged` carries `delta: None`. Compute it
  (reattach holds both snapshots) or ask the owner whether it stays.
- `AttachmentInline::mime`, `attachment_upload(mime: String)`,
  `CloudUploadMeta::mime`, `MessageAttachment::content_type`: unparsed media
  types beside a crate that has `mime::parse_content_type`.

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

## STR-016 - `account_compose.rs` is neither the composition layer nor IMAP's

**Reported by:** types.

Its name collides with `compose.rs` (mail composition) though it is account
composition; it is used only by IMAP; its tests live in IMAP by design; it holds
routing and discovery fan-in but not the capability merge (CON-010) or the
`ObjectType::Event` alias decision (CON-011); its module doc still says
"tomorrow JMAP / Graph". Either grow it into the real composition layer
(`composition.rs`: routing, discovery merge with a typed skip lane, capability
merge) or move it into `bifrost-imap` until a second composer exists.

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

## STR-019 - Three parallel range vocabularies

**Reported by:** types.

`InventoryPartition { Time, Uid, Page }`, `CoverageCoordinate { TimeRange,
UidRange, PageRange }` and `InventoryPartitioning { TimeWindowed, UidRange,
PageCount }` restate the same three windows with different names and integer
widths (`Uid` u64 endpoints vs `max_uid: Option<u32>`), joined by
`CoverageDomain::for_partition`. Shared half-open `TimeWindow`, `UidWindow`,
`PageWindow` types (with `from <= to` enforced, which nothing checks today)
would carry the convention the reference doc had to fix by hand once.

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

## STR-021 - Public-field invariants that nothing re-checks

**Reported by:** types.

`PushSubscription` validates handle/lane agreement in constructors but keeps
public fields, so the invariant is advisory; unlike `Batch` (accepted with an
engine-side check), no consumer re-checks. `Container` carries `kind` and
`provenance.kind`, `native_id` and `provenance.native`: duplicated public fields
with no check that they agree.

## STR-023 - The acknowledgement is assembled from loose parts the consumer must keep in step

**Reported by:** sync-engine.

The facts live in `ack_checkpoint(account_id, scope, checkpoint, publication:
Option<PublicationId>)`, the event `MultiplexerEvent { scope, event, checkpoint,
publication }`, and the receipt `PublicationId(u64, Arc<PublicationReceipt {
checkpoint, claim, walk_watermark }>)`: scope three times, checkpoint twice. The
engine re-derives agreement in several places (`claim_checkpoint` rejects a lane
mismatch; `persist_ack_request` matches `id.1.checkpoint` against
`acked.scope`; `completion_refusal` treats `publication: None` as `Unvouchable`;
the `Ack` arm trusts `req.scope`, BUG-005). Because `PublicationId`'s fields are
`pub`, a consumer can mint one with any receipt; `minted_here` is the run-time
patch.

Proposal: the event carries a non-forgeable `AckToken` (private fields, minted
only by `PendingCoverage`), and the public entry becomes `ack(account, token)`;
scope, checkpoint and lane come from the token. `ack_publication` folds in (a
repair token is the same thing with no checkpoint). The `Target` permit move
applied to acknowledgements: no wrong lane, no consumer minting, and
`publication: None` unrepresentable for anything acknowledgeable. Reshapes a
published signature and type without removing the capability.

The sync-lanes hunter reports the restart side of the same gap (latent): the
docs say a consumer "persists the whole token beside the batch" so an
acknowledgement can replay after a writer restart, but every field of
`PublicationReceipt` and `CoverageClaim` is public and the engine ships no
encoding for it, though it does for cursors (`encode_envelope`) and ledgers
(`encode_ledger`). A consumer must hand-serialize an `InventoryCoverageReport`,
and on replay `claim_in`'s receipt fallback ingests whatever claim the consumer
rebuilt; `Complete` reports discharge debt. For change-cursor lanes nothing
checks the receipt against anything the engine minted (`minted_here` gates only
the backfill withhold). That reopens "no protocol crate (or consumer) may
construct ledger state", which `DebtLedger::from_parts` being crate-private
exists to enforce. Its proposal: make the receipt opaque, give it a codec next
to `encode_envelope` / `encode_ledger` (with an integrity tag if consumer
storage is not trusted), and make construction crate-private (owner ruling:
shape change of public fields). Lateral: `BoundaryEntry::subsumed` is unbounded
per partition under the receipt bound (the whole scope for `Full` plans) and
each retained id holds a full receipt and claim, the ledger's main memory
hazard.

## STR-025 - About ten distinct engine counters travel as bare `u64`s

**Reported by:** sync-engine.

Scope fence (`coverage.scope_fence`, with `0` meaning "attach time" in
`DeferredInventory { fence: 0 }`); lifecycle epoch (`cursors.lifecycle_epoch`,
`EstablishIfAbsent { epoch }`, `ParkScope { epoch }`, the owed maps); account
generation (`Incarnation::generation`, `ReopenRequest::Recovery { generation:
Option<u64> }`, `PushRecovery { generation: u64 }`); registry generation
(already typed in `DriveGeneration`); scope-token generation; reattach attempt
id; completed-reset count (`SchemaReset { parked_at }`); walk generation
(`coverage.next_generation()`); walk watermark; publication number.
`run_establish(.., reattach_attempt: Option<u64>, .., walk_fence: u64)` takes
two adjacent; `establish_if_absent(ctx, scope, epoch)` would accept a fence;
`plan_requests` mints `RepairAttemptId(coverage.next_generation())`, sharing a
counter with walk generations by coincidence. Proposal: the `DriveGeneration`
move for every counter - `ScopeFence`, `LifecycleEpoch`, `AccountGeneration`,
`ReattachAttempt`, `ResetCount`, `WalkGeneration` - each minted only by its
owner.

The sync-lanes hunter independently counted about fifteen identities on one
primitive in the lanes: registry (topology) generation, scope delete generation,
scope incarnation (every `BackfillScan` key is `(CursorScope, u64)`), lifecycle
epoch, scope fence (minted by `burn_fence` in publication-id space), backfill
fence, account fence, publication id (a 32-bit instance segment plus counter,
bit-packed), receiver sequence, undelivered watermark, walk/proof generation,
repair attempt id (the same counter), account generation (the reconciler's
`hold: Option<(u64, Instant)>`), reattach attempt id, walk totals. Swap hazards
that compile today: `emit_backfill_complete(.., undelivered_before: u64,
fence_at_walk_start: u64)` beside `total_seen: u64`;
`SyncControl::publish_walk_marker_if_fence(checkpoint, walk_watermark: u64,
expected_fence: u64)`; `BackfillRunner::run_partition(.., generation: u64, lane,
walk_fence: u64, ..)`; `ParkedWalk { fence_at_walk_start: u64,
undelivered_before: u64 }`; and a fence compared against `PublicationId.0`
(`claim_in`: `*watermark > id.0`) and handed out by `scope_fence` as a plain
`u64`, so a fence and a publication id are interchangeable by construction. Its
list adds `TopologyGen`, `ScopeDeleteGen`, `ScopeFence` (opaque and comparable
only to `PublicationId`), `PublicationSeq` (segment and counter as fields, and
with account identity, which would also let `minted_here` tell "a prior
attachment of this account" from "another account in this process"),
`ReceiverSeq`, `UndeliveredMark`, `ProofGeneration` (BUG-015), its own
`RepairAttemptId` counter, and a `ScopeIncarnation { scope, incarnation }` key.

## STR-027 - Engine failures reach callers only as prose

**Reported by:** sync-engine.

- `Error::Other(String)` is constructed about 98 times in `crates/sync/src`.
  Branchable cases inside it: scheduler admission refusal ("lane is full",
  "admission was abandoned", "account budget was forgotten" - detached,
  "poisoned"), where every caller re-sleeps on a guess (poll loop a cadence,
  deferred inventory 1s, campaigns 100ms) and none can tell the account is
  gone; writer channel closed (BUG-010); "one mutation target has conflicting
  caller versions"; wrong-scope `Done` in `fuse_inventory_done`.
- `Error::EstablishCursorFailed(String)` carries "unknown CursorEstablishment
  variant" and "recovery wait budget exhausted", two unrelated outcomes.
- `Error::CheckpointStore(String)` carries "acknowledgement names an unknown
  publication", a caller-side rejection, not a store failure;
  `AckFailure::{Rejected, Store}` is the typed distinction and collapses at the
  public boundary.
- `ack_checkpoint` returns `Ok(())` for both durable and withheld outcomes
  (`AckPersistOutcome::Withheld` for `OpenDebt`, `WalkNotWhole`, `Unvouchable`,
  prior-attachment backfill rows). Return `AckOutcome::{Durable,
  Withheld(CompletionRefusal)}`.
- Control waits cut short by detach (BUG-011).
- Engine-synthesized warnings are told apart only by text, all
  `WarningKind::OperatorAttentionNeeded` or `Other`: "recovery wait budget
  exhausted", "scope re-establishment retry budget exhausted", "schema recovery
  could not clear durable checkpoints", "shared folder access revoked; scope
  disabled", "scope capability downgraded", "push transport disconnected",
  "push stream terminated with retryable recovery" (which also drops the
  `AccountError` it was about), "reconcile requested dedupe-by-client-id".
  `PauseReason` is already bounded; the warnings deserve an engine-notice kind
  per case carrying scope and originating error. Related: STR-010.

The sync-lanes hunter reports the lane-side warnings the same way: barrier stop
with a count (runner and fusion, two copies of the text), degraded inventory
with a count, "inventory walk superseded by a reset or deletion" (`Other`),
"push transport disconnected" (`Other`, two sites), "push stream terminated with
retryable recovery" (`Other`). A consumer wanting to render "operator must
waive N regions in scope S" distinctly from "push is down" must match on
English. Its proposal: a typed attention payload carried on the warning
(`AttentionReason::BarrierStop { regions }`, `DegradedCoverage { obligations }`,
`WalkSuperseded`, `PushDown { lane }`, ...). Also `BackfillState { Pending,
Running, Completed }` folds "never started", "failed and retrying on the ramp",
"parked on a throttle", "blocked by an operator-blocked barrier" and "walk lost
pages, restarting" into `Pending`; it wants a richer state.

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

## STR-029 - `engine/reattach.rs` does several unrelated jobs

**Reported by:** sync-engine.

In about 2,700 lines: the recovery dispatcher, the directive arms,
`DeferredScopeRepairs` and `DeferredSchemaReset` (parking actors), schema reset,
`RepairPacing`, scope restart and disable, the reattach transaction,
`ReattachGuard` and its drop-time cleanup, replacement open, establishment,
warning and termination broadcast helpers, jitter. The reopen listener (the
account's serial supervisor, the real entry to all of it) is an inline closure
inside `attach_opened` in `attach.rs`.

Suggested layout under `engine/`: `supervisor.rs` (listener loop and
`ReopenRequest`, moved out of `multiplexer/`), `recovery/dispatch.rs` (the
CON-012 dispatcher and directive arms), `recovery/parked.rs` (the two
deferred-obligation actors), `recovery/pacing.rs` (`RepairPacing` plus CON-015
backoff), `reattach/{transaction,guard}.rs`, `establish.rs` (CON-017).
`ReopenRequest` belongs to the supervisor: most variants are documented as
"engine plumbing" and raised by non-multiplexer code.

## STR-030 - `multiplexer/mod.rs` holds four subsystems

**Reported by:** sync-engine.

In about 3,800 lines: (1) `ChangeDelivery` and `ChangesReceiver`, the engine's
consumer-facing output port (receiver numbering, departure sweeps, lag), on
which the backfill lane, repair and push depend - its own `delivery/` module
with CON-019 applied; (2) the lifecycle reader, a roughly 170-line closure
inline in `Multiplexer::run` - its own worker type, file and tests; (3) the poll
loop, scope tokens and drive outcome - what "multiplexer" should mean; (4)
membership to scope mapping (CON-016), which belongs in `bifrost-types` or
`cursor/`. Also `multiplexer/changes.rs` defines `AckRequest`, `WriterRequest`
and `OperatorDecision`, the writer's protocol, consumed only by `engine/ack.rs`
and `backfill/checkpoint.rs`; they belong next to the writer (`engine/writer/`).
That placement is how `WriterRequest` leaked into the public surface (BUG-013).

## STR-031 - The checkpoint writer is a 600-line match over five concerns

**Reported by:** sync-engine.

`engine/ack.rs` mixes ack persistence and completion-sentinel judgement,
reattach provisional ownership, repair parking and acknowledgement pairing,
scope and account resets, and operator decisions and barriers. Beside it,
`reject_unloaded_request` is a second exhaustive match deciding per variant what
"ledger unavailable" means, away from the variant. Proposal: a `Writer` struct
with a method per concern (`ProvisionalRows`, `RepairPairing`, ledger ops per
CON-022), a `WriterRequest` impl giving each variant its `requires_ledger()`
answer, and the detach-phase helpers (`take_ack_writer`, `await_*_until`) moved
to the teardown code they serve.

The sync-lanes hunter adds the placement half: `WriterRequest` (fifteen
variants, carrying the provisional-row and publication-identity semantics) is
defined in `multiplexer/changes.rs` while its server is `engine/ack.rs`, and
`WriterHandle`, the typed wrapper, is `pub(crate)` in `engine/ack.rs` with
`pub(super)` methods, so the lanes (`inventory_walk`, runner, fusion,
`engine/backfill.rs`, `repair.rs`, `BackfillCheckpointWriter`) all build
oneshots by hand against the raw sender (BUG-010). Proposal: a `writer` module,
plausibly `cursor/writer.rs` since the writer is the custodian of the cursor
store and ledger, owning `WriterRequest`, `WriterHandle` and the reply typing;
every lane takes `WriterHandle`.

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

## STR-033 - `recovery.rs` mixes plan dispatch, the throttle bucket and an error catalogue

**Reported by:** sync-engine.

(1) Plan dispatch (`plan_recovery`, delays); (2) the throttle bucket and its
free-function API; (3) a catalogue of engine-authored `AccountError`
constructors (`restart_scope_error`, `establish_failure_error`,
`inventory_resume_contract_failure`, `inventory_removal_scope_contract_failure`,
`recovery_exhaustion_error`, `cursor_decode_failure`,
`batch_boundary_violation`, `backfill_checkpoint_on_changes`,
`fresh_cursor_contract_error`), with `control.rs::control_wait_ended` a stray
member. Proposal: `recovery/plan.rs`, `throttle.rs` (with CON-013) and
`engine_errors.rs`; with every engine-minted error in one file, BUG-006 and
BUG-011 are visible side by side.

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

## STR-039 - `Option` parameters as hidden mode switches that change safety semantics

**Reported by:** sync-lanes.

`BackfillRunner::run_partition` takes thirteen parameters, including
`changes_tx: Option<broadcast::Sender>`, `control: Option<&SyncControl>`,
`coverage: Option<&Arc<PendingCoverage>>`, `writer_tx:
Option<&mpsc::Sender<WriterRequest>>`, `lane: Option<&LaneGate>` and a
non-optional `delivery`. Which are `Some` silently selects behaviour:
`lane.is_some()` picks fence-conditional registration over unconditional (so
the reset-fence guarantee depends on whether a flow-control gate was passed)
and also arms the shutdown select; `(control, coverage, publication)` chooses
which retirement function runs (BUG-020); `writer_tx: None` makes
`record_barriers` succeed and `cross_waived_barriers` answer "not waived";
`changes_tx.is_none()` publishes nothing and skips the removal projection - and
`emit_backfill_complete` answers `if changes_tx.is_none() { return
MarkerOutcome::Published; }`, so the orchestrator settles the incarnation
although nothing was broadcast or made durable. `InventoryFusion` repeats the
pattern with `control`, `coverage`, `writer_tx` and `shutdown`, including a
`fence_moved` branch that exists only for `control: None`. Production passes
`Some` for all; the `None` combinations are a test convenience the safety logic
must stay correct under. Proposal: one `WalkContext` / `PagePublisher` value
(CON-019) built once per walk in production, plus a test double implementing
the same trait.

## STR-040 - Persisted ledger errors are fabricated `AccountError`s

**Reported by:** sync-lanes (owner ruling: field type change).

`LedgerEntry::last_error` and `BarrierIncident::evidence` are `AccountError`, but
the codec persists only a digest. On decode, `canonical_cause` synthesizes a
cause the original never had: every `StrategyFailure` comes back as
`QResyncToCondstore`, every `RateLimited` with `retry_hint: None`, every
`InsufficientScope` with `needed: "unknown"`, and `CursorInvalid` without a
cursor scope gets `CursorScope::Account` invented. The restored value then
answers `recovery()`, `message_key()`, `retry_hint()` from causes nobody
observed. The reference says no ledger predicate reads these, but
`SyncEngine::debt` hands the entries to consumers, and nothing stops one
branching on `last_error.recovery()`. Also, `canonical_cause` restates
`bifrost-types`' `kind_matches_cause` (CON-002) as its own table, and the
`CursorInvalid` scope patch restates another builder rule; they stay in step
only because `try_build` fails at decode time with "restored error failed
classification", surfacing as a ledger decode error the writer treats as an
unreadable ledger. Proposal: `ErrorDigest { kind, scope, operation, provider,
protocol, diagnostics }` as the field type, with no `recovery()`; it removes
`canonical_cause`.

## STR-041 - `PolicyStatus::Retrying { attempts }` is a mirrored view; barriers reuse a policy that means nothing for them

**Reported by:** sync-lanes.

`attempts` on an entry is documented as "a VIEW", rewritten from the lineage
table by `settle_lineages` after every mutation and cross-checked on decode by
`validate_lineages` ("an open `Retrying` entry disagreeing with its row is
`Error::Other`") - two copies held in step by a pairwise-agreement check, the
shape the hunt brief names. `BarrierIncident::policy` is minted as `Retrying {
attempts: 0 }` (`inventory_walk::barrier_incidents`) and "is inert". Proposal:
`EntryPolicy { Automatic, OperatorBlocked, Waived { .. } }` with the attempt
count read through `DebtLedger::lineage_budget(entry)`, and a separate
`BarrierPolicy { Undecided, OperatorBlocked, Waived { .. } }`. `Waived { by:
String, at_unix_seconds: i64 }` should carry an `OperatorId` and a timestamp
type.

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

## STR-044 - `BackfillScan` is one state machine spread across seven parallel maps

**Reported by:** sync-lanes.

`settled`, `failed`, `baseline`, `lost_pages`, `held`, `parked` (all keyed
`(CursorScope, u64)`) and `fusion_skipped` (keyed by scope). `select` spends its
first block pruning six of them in step, and `settled` is deliberately never
pruned, so it grows by one entry per incarnation for the life of the
attachment. Proposal: `HashMap<ScopeIncarnation, IncarnationState>` with `enum
IncarnationState { Fresh, Running { baseline }, Parked(ParkedWalk), Held { until
}, Failed { count, until, baseline }, Settled { watermark } }` and `lost_pages`
a flag on the non-settled states; pruning becomes one `retain`, exclusivity is
structural, and `BackfillRegistry` becomes a projection (CON-026).

## STR-045 - Codecs for `bifrost-types` vocabulary live in `bifrost-sync`, where their matches cannot be exhaustive

**Reported by:** sync-lanes.

`ledger_envelope.rs` encodes `AccountErrorKind`, `ErrorScope`, `ResourceKind`,
`AccountOperation` (by positional table), `Provider`, `Protocol`, `Cause`
reconstruction and diagnostics; `envelope.rs` encodes `CursorScope`,
`ObjectType` and `ProtocolKind`. Every enum is `#[non_exhaustive]` from sync's
side, so each codec carries a wildcard arm that panics on encode or errors on
decode (BUG-018). The ledger version history shows the coupling: versions 5, 6
and 7 were bumped because `bifrost-types` grew an `ErrorScope` or `ResourceKind`
variant. The dependency edge points the wrong way for change safety: the crate
that adds a variant cannot see the codec that must learn it. Proposal: move the
tag codecs for types-owned vocabulary into `bifrost-types` (a `wire` module with
exhaustive in-crate matches), versioned there; the ledger codec composes them,
as it already composes `encode_envelope` for `resume_from`.

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

## STR-047 - "Lane" means five different things in the engine

**Reported by:** sync-lanes.

`cursor::coverage::Lane` (publication supersession and acknowledgement lane),
`engine::lane::LaneGate` (backfill bound plus admission), `scheduler::lanes`
(Foreground, Normal, Background, Bulk), `push::lanes::PushLanes` (push session
supervision), and `bifrost_types::PushLane` / `PushLaneKind`. The module docs
repeatedly disambiguate. Renaming the internal ones (for example
`PublicationStream` for coverage's `Lane`, `BackfillBound` for `LaneGate`) costs
nothing published.

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

## STR-049 - IMAP has six representations of "a set of UIDs / sequence numbers", and they disagree on reversed ranges

**Reported by:** imap-protocol (the hunter's top-ranked move; latent disagreement).

The hunter's thesis for the whole protocol layer: it owns a good typed
vocabulary (`Uid`, `Seq`, `UidValidity`, `ModSeq`, `UidSet`, `SeqSet`,
`SearchCriteria`, `FetchAttr`, `StatusItem`, `MailboxRights`, `AuthMechanism`,
`ObjectId`, `ImapAtom`, `GmailMessageId`) and almost none survives to the place
that acts on it: values are rendered to `String` / `u32` / `u64` one hop before
the encoder, which re-parses and re-validates, and the decoder produces raw
numbers and strings that every consumer re-judges. "Render early, re-parse
late." The biggest payoff is a rewrite carrying typed values from builder to
wire and wire to consumer, the encoder a pure renderer and the decoder the one
place wire tolerance is decided. This entry and STR-050 to STR-058 are its
parts.

Representations: `SequenceSet(String)` (`types/validated.rs`; `new_known`
rejects `*`/`$` by `contains`); `UidSet(SequenceSet)` / `SeqSet(SequenceSet)`
(`types/ids.rs`; constructors format strings and `expect` the validator;
`coalesce_numbers` and `UidSet::from_ranges` are two coalescers); `UidRange`
(`types/uid_range.rs`; public `start: u32`, `end: Option<u32>` plus two hidden
`*_is_star` bools; `single(n)` and `range(n, n)` are distinct values for one set;
`Default` is the invalid zero UID; `range` only debug-asserts non-zero;
`try_range` permits reversed ranges; it carries sequence numbers too);
`ParsedUidSet(Vec<(u32, u32)>)` (re-parses a `SequenceSet`, maps `*` to
`u32::MAX`); `CompactUidSet { ranges: Vec<UidRange> }`
(`account/folder_registry.rs`, the real set algebra, plus `normalized_range`,
which exists only because `UidRange` has two spellings of one UID);
`expand_uid_ranges` (`connection/mod.rs`, its own sort/merge/expand). The cursor
codec (`account/envelope.rs`) encodes a single UID as an `end = 0` sentinel, and
`InventoryPartition::Uid` is half-open `u64`.

"What does a reversed range mean": decoder `uid_range` / `seq_range` swap
(Postel); `ParsedUidSet::new` and `intersect_uid_ranges` swap;
`CompactUidSet::from_ranges` DROPS (`end >= start` filter);
`CompactUidSet::remove_range` swaps and clamps to 1; `expand_uid_ranges` DROPS
("names no messages"); the cursor decoder REJECTS as schema-incompatible;
`account/mutate.rs` and `connection/dispatch.rs` swap inline. The decoder
normalizes first, but any `UidRange` built through `try_range` or the public
fields can reach a consumer reversed.

Proposal: one value type, `types::set::UidSet` = sorted, disjoint, maximally
merged `RangeInclusive<Uid>` (no dual spellings, no zero, no reversed), with
`CompactUidSet`'s algebra moved onto it; a `SeqNumSet` twin over `Seq`; a
separate command operand `SetExpr { Set(UidSet), All, SavedResult, ..WithStar
}` for the wire-only `*` / `$` forms. The decoder produces `UidSet`, the encoder
renders it, the cursor serializes it, and the one conversion to the engine's
half-open `u64` partition lives beside it.

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

## STR-051 - IMAP typed identifiers never reach their consumers

**Reported by:** imap-protocol.

- `ImapAtom`, `validated::ObjectId`, `GmailMessageId`, `GmailThreadId`,
  `types::Address` and the `EnvelopeAddress -> Address` conversions are used
  only by their own tests (hidden by the crate-root `allow(dead_code)`).
- `Uid` / `UidValidity` / `ModSeq` reach two places (`account/targets.rs`,
  `account/mod.rs::select_options`); everywhere else is raw:
  `ResponseCode::UidValidity(u32)`, `AppendUid { uid_validity: u32 }`,
  `SelectedMailbox { uid_validity: Option<u32>, uid_next: Option<u32>,
  highest_mod_seq: Option<u64>, mailbox_id: Option<String> }`, `FetchResponse {
  mod_seq, email_id, thread_id, gmail_msg_id }`, `FolderCursor { uidvalidity:
  u32, modseq: u64 }`, `DecodedObjectId { uidvalidity: u32, uid: u32 }`,
  `ModSeqCache { uidvalidity: Option<u32>, by_uid: HashMap<u32, u64> }`,
  `Error::FetchLimit { seq: Option<u32>, uid: Option<u32> }`.
- The PUBLISHED surface uses bare `NonZeroU32` for two swappable siblings:
  `ImapMessageRef::new(container, uid_validity: NonZeroU32, uid: NonZeroU32)`,
  `UidScan { uid_validity, from, through }`, `UidPage { completed_through,
  next_from }`, `FolderObservation { uid_validity, uid_next }`,
  `RawMessage::GenerationChanged { observed }`. Nothing stops a UID going where
  a UIDVALIDITY goes; publishing `Uid` / `UidValidity` and retyping the fields
  is a shape change.
- Two typed views of one `(folder, uidvalidity, uid)` triple: `DecodedObjectId`
  (raw `u32`) and public `ImapMessageRef` (`NonZeroU32`), the latter rebuilt
  from the former with `expect("the id decoder rejects zero")`. One `MessageRef`
  with typed fields would serve both.

The imap-account hunter ranks this its top finding: the triple travels in at
least seven shapes in `account/` - `DecodedObjectId`, `DecodedThreadId {
uidvalidity: u32, uids: Vec<u32> }`, `DecodedPartHandle` (`parts.rs`),
`SearchPosition { folder, uidvalidity: u32, key: u32 }`, `FolderEffect { uids:
Vec<u32>, uidvalidity: u32 }`, `ImapMessageRef` (std `NonZeroU32`, a third
numeric representation), `FolderCursor::{QResync, Condstore, Basic} {
uidvalidity: u32, modseq: u64, uidnext: u32 }`, `RawFetchOutcome::GenerationChanged
{ observed: u32 }`, `ModSeqCache`, `MutationKind::ConditionalFlags(FlagOp, u64)`,
and `append_message`'s `Option<(u32, u32)>` destructured positionally in
`draft_create` and `append_to_sent_or_fallback`. `encode_object_id(folder,
uidvalidity: u32, uid: u32)` and `encode_thread_id` take two adjacent `u32`s every
call site and test (`encode_object_id(&f, 7, 42)`) can swap silently. Its
proposal: one `ImapRef { folder: MailboxName, epoch: UidValidity, uid: Uid }`
(plus `ImapThreadRef`) owned by one codec module through which every durable IMAP
string goes - `imap1`/`imap2` object ids, `imapthread1`/`2`, `imapsearch1`
cursors, `imappart2`/`3` part handles, the `imapmsgid1` correlation token,
`DraftHandle` ("an object id in a costume"), `imap-idle-N` subscription handles;
`ImapMessageRef` a thin public wrapper over it; `FolderCursor` fields typed.

## STR-052 - IMAP QRESYNC parameters are typed, stringified, then re-validated

**Reported by:** imap-protocol.

`SyncSelectOptions { uid_validity: Option<UidValidity>, mod_seq:
Option<ModSeq>, known_uids: Option<UidSet>, seq_match_data: Option<(SeqSet,
UidSet)> }` is converted by `qresync_params()` into `QresyncParams {
uid_validity: u32, mod_seq: u64, known_uids: Option<String>, seq_match_data:
Option<(String, String)> }`; the handle checks "seq-match requires known-uids",
and the encoder checks it again, plus non-zero uidvalidity, the modseq range,
CRLF in the set strings, and re-runs `SequenceSet::new_known`. Proposal:
`Command::Select` carries `Qresync { validity: UidValidity, modseq: ModSeq,
known: Option<KnownUids { uids: UidSet, seq_match: Option<(SeqNumSet, UidSet)>
}> }`, making seq-match without known-uids unrepresentable and collapsing the
stringly `QresyncParams` and the `SelectOptions`/`SyncSelectOptions` pair. The
mod-sequence range is a constant in two places and a type in none (`MOD_SEQ_MAX
= i64::MAX as u64` in the encoder's `validate_mod_sequence_value` / `_valzer`, a
literal in the decoder's `response_code_has_overflow`, `ModSeq(u64)` with no
constraint, `changed_since` / `unchanged_since: Option<u64>`): a `ModSeq`
constructor enforcing 1..=2^63-1 plus a `ModSeqValzer` admitting 0 for
UNCHANGEDSINCE owns it. Command parameters also live in the wrong file:
`QresyncParams` and `SelectOptions` sit in `types/response.rs`, and
`types/sync.rs` defines `SyncSelectOptions` / `SyncFetchRequest` as
"higher-level" duplicates; one typed request per command, in a `request`
module.

## STR-053 - IMAP `Command` is stringly typed, so the encoder re-parses the builder's output

**Reported by:** imap-protocol.

`Fetch { items: String }`, `Status { items: String }`, `ListStatus {
status_items: String }`, `Search { criteria: String }`, `SearchReturn {
return_opts: Vec<String> }` (SAVE detected by `trim().eq_ignore_ascii_case`),
`Enable { capabilities: Vec<String> }`, `Authenticate { mechanism: String }`,
`ListExtended { selection_options, return_options: Vec<String> }`, `GetMetadata
{ depth: Option<String> }`, `SetAcl { rights: String }`, `Sort { sort_criteria,
charset }`, `Thread { algorithm, charset }`, `NotifyEvent::MessageNew {
fetch_attrs: Vec<String> }`, `AppendMessage { date: Option<String> }` - though
typed sources exist for most. The sharpest cost: `SearchCriteria` is a `String`
builder, so deciding which capability a SEARCH needs requires
`ImapConnection::search_criteria_contains_atom`, a hand-written SEARCH grammar
walker (`connection/search_validation.rs`) the ENCODER calls through
`crate::connection` to rediscover whether the builder wrote `MODSEQ`, `OLDER`,
`SAVEDBEFORE`, `EMAILID` or `$`. Quoting is decided twice:
`search_helpers::quote_imap_string` always quotes and rejects CR/LF at build
time (before the UTF-8 mode is known), while `CommandWriter::string` chooses
quoted-vs-literal from the live mode. Proposal: `SearchKey` as an enum AST (with
an explicit `Raw { text, requires: Vec<Capability> }` escape hatch), rendered
through `CommandWriter::string`, with `SearchKey::required_capabilities()`; the
grammar walker, `validate_search_criteria_crlf` and `write_criteria`'s literal
re-emission go for typed criteria. NOTIFY event-group legality is the same
shape: RFC 5465's dependency rules are checked by
`encode::commands::notify::validate_notify_event_group` and asserted again in
`account/push.rs` tests; `MessageEvents { new_fetch: Vec<FetchAttr>,
flag_change: bool, annotation_change: bool }` makes the illegal group
unrepresentable.

The imap-connection hunter adds: handle methods take `&[FetchAttr]` and validate
them (`validate_requested_fetch_items`), then `format_fetch_attrs` flattens them
to `items: String`, and the `Pipeline` builder takes the `String` directly,
bypassing validation (BUG-029) - keep `Vec<FetchAttr>` in the command. STATUS
items are `status_items: String` / `items: &str`, tokenized by
`status_item_tokens` and matched as strings in `validate_requested_status_items`.
LIST-EXTENDED `selection_options` / `return_options: Vec<String>`: "is SUBSCRIBED
selected" is a string scan in `mailbox::list_extended_names`,
`pipeline::list_extended` and `is_notify_selection_mismatch` (which also scans
RECURSIVEMATCH, REMOTE, SPECIAL-USE), and `validate_list_extended_request` scans
SPECIAL-USE, RECURSIVEMATCH and REMOTE again; typed `ListSelect` / `ListReturn`
enums remove five spellings. SEARCH RETURN options (`search_return_requests_save`
scans for `SAVE`) likewise. The `search_validation.rs` lexer is hung off
`ImapConnection` as associated functions used only by the codec encoder (and a
`#[cfg(test)]` adapter); it belongs in `codec/` or disappears with `SearchKey`.

## STR-054 - IMAP has one `Flag` type for read-only and writable flags

**Reported by:** imap-protocol.

`Flag` includes `Recent` and `Wildcard` (`\*`, PERMANENTFLAGS-only) beside
writable flags, and `Custom(String)` is unvalidated. "Which flags may a STORE /
APPEND carry" is decided at `encode::validate_and_filter_flags` (silently drops
`\Recent` / `\*` with a debug log, validates keyword atoms) and again at
`connection::filter_store_flags`; `account/inventory.rs::imap_flag_target`
separately special-cases `\recent` by string. Proposal: `StoreFlag` (system
flags + validated `Keyword(ImapAtom)`), `Flag` (observed, includes `Recent`) and
`PermanentFlag` (includes `Wildcard`). The live keyword-folding defect on top is
BUG-026; the workspace-level flag identity question is STR-004.

## STR-055 - IMAP ENABLEd extensions are `Vec<String>` and the UTF-8 mode is a bool

**Reported by:** imap-protocol.

`enabled: Vec<String>` lives in `ProtocolState`, the connection snapshot,
`EncodeOptions` and `ServerProfile`, fed by `UntaggedResponse::Enabled(Vec<String>)`.
"Is UTF8=ACCEPT enabled" is a string scan at `driver::utf8_mode`,
`EncodeOptions::utf8_accept_enabled`, `helpers::check_utf8_only_enforced`,
`helpers::utf8_enabled` and the ENABLE path in `extensions.rs`; "is QRESYNC
enabled" at `mailbox::validate_qresync_params` (whose comment says it matches
"exactly as `ServerProfile::enabled` does" - agreement held by a comment) and
`uid_ops`; `imap4rev2_active` scans for `"IMAP4rev2"`. `profile.rs`'s own doc says
the `(capabilities, enabled)` pair is owned by four views and asks callers to go
"through a typed wrapper" that does not exist. Proposal: `ConnectionCaps {
advertised: CapabilitySet, enabled: EnabledSet }` (enabled entries typed as
`Capability`), one value held by all four views, owning `supports`,
`imap4rev2_active`, `binary_fetch_usable`, `utf8_mode`, `literal_mode`,
`qresync_enabled`; replace the about 60 `utf8_mode: bool` / `utf8: bool`
parameters across decode and encode with `WireMode { Ascii, Utf8 }`.
`ServerProfile::auth_mechanisms: Vec<String>` and `thread_algorithms:
Vec<String>` duplicate `Capability::Auth` / `Thread` and should be views. Also
`build_encode_options` clones `capabilities` and `enabled` for every encoded
command; behind an `Arc` that becomes a refcount bump. The live gate
disagreement is BUG-029.

The imap-connection hunter counted five views (`ProtocolState::enabled`,
`ConnectionStateSnapshot::enabled`, `EncodeOptions::enabled`,
`ConsumerContext::enabled`, `ServerProfile::enabled`, all `Vec<String>`) and the
"is X ENABLEd" scans at `driver::utf8_mode`, `helpers::check_utf8_only_enforced`
and `helpers::utf8_enabled`, `extensions::enable_utf8`, `codec/encode/core.rs`,
`uid_ops::uid_fetch_vanished` and `uid_fetch_vanished_stream` (verbatim copies
whose comment says "exactly as `ServerProfile::enabled` does" - a self-declared
copy of a rule with an owner), `mailbox::validate_qresync_params`,
`account/changes.rs`, `types::profile::imap4rev2_active`, and
`ProtocolState::apply_untagged` (dedup on insert). The pair is threaded as two
slices into `types::profile::supports(capabilities, enabled, cap)`, whose doc
warns that nothing stops a caller pairing one view's capabilities with another's
enabled list and that "the wrappers exist so that ordinary call sites cannot" -
but the only wrappers are `auth::snapshot_supports` and
`auth::is_rev2_from_snapshot`, and the driver and encoder call the slice
functions directly. Its target is the same `SessionCapabilities { advertised,
enabled: EnabledSet }`, owned by `ProtocolState`, published in the snapshot as an
`Arc` (both vectors are cloned into the watch after every command today, and
every handle gate re-borrows the watch, `validate_list_extended_request` four
times, so an `Arc<SessionView>` also gives a method one coherent view for all its
gates), embedded in `EncodeOptions` and `ConsumerContext`, with
`ServerProfile` a projection and `Command::Enable` taking typed capabilities.

## STR-056 - Smaller IMAP protocol axes

**Reported by:** imap-protocol.

Allocation-free capability hashing, the unit `SortDisplay`, one canonical
suffix spelling per capability variant, and direct case-insensitive flag and
attribute equality landed. What remains crosses into `crates/imap/src/account/`,
so it wants a fixer owning both sides:

- Escape payloads are still public, so equality has to tolerate
  `Custom("\\Seen")` meaning `Seen`. Make the payload a private newtype minted
  only by `from_imap_str`; the account constructs `Flag::Custom(s)` and
  `Capability::Thread(s)` directly and must switch to `from_imap_str`.
- `MailboxAttribute` has no `Inbox` variant, so the account's
  `folder_registry.rs` role match reads `Custom("\\Inbox")`.
- `MoveResult::copy_uid` / `CopyResult::copy_uid: Option<ResponseCode>` are
  documented "Always a `ResponseCode::CopyUid` when present" and every consumer
  re-matches. Use `Option<CopyUid { validity: UidValidity, source: UidSet, dest:
  UidSet }>` (and the same struct inside `ResponseCode::CopyUid`); its readers
  are `account/mutate.rs` and three matches in `account/error.rs`.

## STR-058 - `MailboxName`'s wire encoding is a bool, and identity is recomputed per comparison

**Reported by:** imap-protocol.

`MailboxName(String, Option<(Vec<u8>, bool)>)`, `identity() -> (bool, Vec<u8>)`,
`wire_bytes(utf8: bool)`. `identity()` runs `encode_utf7` and allocates on every
`eq` and `hash`, so each `HashMap<MailboxName, _>` probe UTF-7-encodes the name;
`can_reencode()` (called by `child`, `parent`, `account_id`, `matches_mailbox`,
the encoder) computes two identities. The canonicality bit is recomputed by
round-trip because `decode_utf7` returns a lossy `String` with no "was this
canonical" signal; it also maps control characters to U+FFFD without signalling,
so a name with a control byte decodes to a display name that re-encodes
differently, caught only because `MailboxName` keeps raw wire bytes - any path
dropping them (`from_decoded`) silently changes identity. And
`MailboxArgument::from(String)` goes through `MailboxName::from_decoded` (no
validation, "Text is validated by the encoder") while the rest of the crate
validates at `MailboxName::new`: two validation boundaries for one input.
Proposal: the codec returns the canonical flag; store `enum Wire { Canonical,
Raw { bytes, mode: WireMode } }` and a precomputed identity key. The conversion
decision is CON-031.

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

## STR-060 - ManageSieve is a second protocol hidden in IMAP's `account/`

**Reported by:** imap-protocol, imap-account.

The imap-account hunter adds that it also does TCP connect, implicit TLS and
STARTTLS, line and literal framing, read timeouts and bandwidth metering,
sharing nothing with the IMAP driver, and that its failures are
`crate::Error::Sieve` (an IMAP error enum carrying another protocol's codes)
classified in `account/error.rs::classify_sieve`; it should expose a client the
account composes the way it composes SMTP.

`account/sieve.rs` (about 1,300 lines) is a full RFC 5804 client: its own
response reader, quoting, response-code enum, SASL negotiation and payloads. It
is not account logic, and its error variant leaks into the crate's root
`Error`. It belongs beside `codec`/`connection` as a sibling protocol module (or
its own crate, as SMTP is), consuming `bifrost-sasl` for every payload
(BUG-030), with `account/` only adapting it to the filter lane.

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

## STR-063 - The IMAP crate error is a flat bag with per-variant side tables

**Reported by:** imap-protocol.

`Error` is about 30 variants with a hand-written `PartialEq` (because of
`Arc<io::Error>`), plus `is_connection_fatal`, `attempt`, `with_attempt`,
`response_code` as four more per-variant tables. `attempt: Option<ImapAttempt>`
(a one-field newtype over `TransmissionState`) is optional on `No` / `Bad`
although the docs say a tagged NO/BAD is by definition acknowledged. Proposal:
`ServerStatus { status: No | Bad | Bye | AuthRejected, text, code }`
(acknowledged by construction), `Transport { kind, attempt: TransmissionState
}`, `LocalRefusal { kind }` (InvalidInput, InvalidState,
MissingCapability(Capability), StateChangedBeforeSend, ...) and `Internal {
mid_exchange: Option<TransmissionState> }`, making fatality and attempt state
functions of the category.

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

## STR-066 - IMAP modseq support is `Option<u64>` plus a contradicting `bool`

**Reported by:** imap-connection.

`SelectedMailbox` carries `highest_mod_seq: Option<u64>` and `no_mod_seq: bool`.
They can contradict: `extract_selected_code` sets `no_mod_seq = true` on
`HIGHESTMODSEQ 0` or `[NOMODSEQ]` without clearing a `highest_mod_seq` set by an
earlier code in the same response set. "Does this mailbox carry modseqs" is then
answered as `highest_mod_seq.is_some() && !no_mod_seq` at seven account sites
(`mutate.rs`, `inventory.rs`, `get.rs`, `changes.rs` twice in inverted form,
`mod.rs::cursor_from_select` twice). Target: `enum ModSeqSupport { Unknown,
NoModSeq, Highest(ModSeq) }`, built once in `build_selected_mailbox`, read through
`SelectedMailbox::modseq() -> Option<ModSeq>`; `HIGHESTMODSEQ 0` normalization
becomes part of `ModSeq` construction (BUG-024).

## STR-068 - `connection/mod.rs` does seven jobs

**Reported by:** imap-connection.

(a) Protocol vocabulary (`TlsMode`, `TcpKeepalive`, `SessionState`, `IdleEvent`,
`SearchResult`, `NotifyFlags`); (b) the `ImapConnection` handle, its `Drop`, and
event draining; (c) driver-task lifetime machinery (`DriverExit`, `ExitState`,
`DriverScope`, `LeaseHandoff`, `DriverTask`, `InFlightGuard`); (d) SELECT
response assembly (`build_selected_mailbox`, `extract_selected_code` with
`#[allow(clippy::too_many_arguments)]` and nine `&mut` out-params,
`selected_mailbox_effective_responses`); (e) NOTIFY LIST heuristics
(`is_notify_selection_mismatch`, `is_notify_list_event`); (f) ESEARCH expansion
(`expand_uid_ranges`); (g) STORE flag filtering. (d) to (g) are consumer logic
for `dispatch/` beside their only callers (`select.rs`, `list.rs`, `search.rs`);
(c) belongs in `driver/`; `SessionState` and `NotifyFlags` in `types/`.
`extract_selected_code` should be a builder (`SelectedMailboxBuilder::apply(&ResponseCode)`).

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

## STR-076 - IMAP boolean parameters that name modes

**Reported by:** imap-account.

- `select_folder(conn, folder, cursor, read_only: bool)`: every call reads `...,
  None, true)`; with the cursor argument it is really a `SelectPurpose`
  (BUG-050).
- `FolderCursor::QResync { known_uids_complete: bool }`: "baseline complete" vs
  "legacy, must re-inventory" is a state, special-cased in `run_changes` before
  dispatch; `Baseline::{Complete(CompactUidSet), Legacy}` makes the incomplete
  cursor impossible to hand to `run_qresync`, which `debug_assert!`s it today
  (BUG-049).
- `blob::run_fetch(.., refuse_not_sticky: bool, ..)`, whose `NotSticky` outcome is
  `unreachable!` on one caller.

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

## STR-080 - IMAP has two mutation engines

**Reported by:** imap-account.

`mutate.rs` is the careful one: per-item three-lane outcomes through
`TargetBatch` permits, opportunistic `UNCHANGEDSINCE` from the MODSEQ cache with
`MODIFIED` attribution, stale-epoch partitioning, shared-folder `ScopeRevoked`
mapping, the guarded EXPUNGE fallback, partial-commit records per group. `pim.rs`
re-implements STORE, `\Deleted` + EXPUNGE and COPY for the `Result<(),
AccountError>` lanes (`set_flag`, `delete_messages`, `copy_messages`) with its own
`FolderProgress` record builder - no MODSEQ guard, no shared mapping (BUG-046), a
different stale-epoch answer. The primitives could run `mutation_stream` and
collapse per-item outcomes into one result plus partial-commit record, deleting a
parallel engine and its divergences. The hunter notes the workspace-wide pattern
of a `pim` module per provider beside the bulk lane makes the same question worth
asking in JMAP, Google and Graph.

## STR-081 - IMAP and JMAP error construction escapes the armed `error.rs` boundary

**Reported by:** imap-account, jmap-account.

The jmap-account hunter reports the same in JMAP: `error.rs` carries
`#![warn(dead_code)]` so an unreachable classification arm is a warning, but
`AccountError`s are also built inline in `capabilities.rs` (four builders),
`changes.rs` (the cursor decode mapping, BUG-073), `page_cursor.rs` (three),
`pim.rs` (`schema_incompatible_search_cursor`, `missing_shared_rights`,
`reschedule_*`, partial commits), `blob.rs` (`NotFound`, range `InvalidArgument`)
and `contacts.rs` / `calendar_ops.rs` (`unsupported` wrappers); the duplicates in
BUG-065 and BUG-069 are a product of that scatter. Move every classification into
`error.rs` (split by family if size is the concern).

`error.rs` opens by claiming to be "the only place in bifrost-imap that constructs
`bifrost_types::AccountError`" and opts back into `dead_code` so an unreachable
translation is caught. In fact about 36 `AccountErrorBuilder::new` sites live in
`mutate.rs` (10), `pim.rs` (7), `blob.rs` (3), `get.rs`, `envelope.rs`,
`factory.rs`, `poll.rs` (2 each), `targets.rs`, `scopes.rs`. Two builders for
`NotFound(Message)` (`get::message_not_found_error`, `pim::expunged_message`,
plus `blob::raw_message_not_found`) differ in whether they stamp `Acknowledged`.
Gathering the domain builders into `error.rs` makes the claim true and puts them
under the armed `dead_code` lint.

## STR-082 - IMAP does not partition inventory, so large folders get no bounded backfill

**Reported by:** imap-account (not a bug; a gap between vocabulary and its intended producer).

`inventory_partitioning` is not overridden, so IMAP folders inventory as one `UID
FETCH 1:*` that also accumulates every UID in a `Vec<u32>` before minting the
cursor. The half-open `u64`-ended `InventoryPartition::Uid` vocabulary was
designed with IMAP in mind (`types.md` says as much), and the bounded backfill
lane depends on partitions, but no production crate mints a `Uid` partition.
Related: STR-046.

## STR-086 - JMAP states are all `String`; the typed `State` alias is dead

**Reported by:** jmap-protocol.

`core/id.rs` defines `pub(crate) type State = Id<marker::State>`, used nowhere.
Object state (`ChangesRequest::since_state`, `SetRequest::if_in_state`,
`GetResponse::state`), query state (`since_query_state`,
`QueryResponse::query_state`), session state (`Response::session_state`) and push
state (`PushObject::StateChange::push_state`, `WebSocketPushEnable::push_state`)
are bare `String`, so passing a `queryState` as `sinceState`, or an `Email` state as
`ifInState` on `Mailbox/set`, compiles. The sync layer stores them as
`HashMap<String, Option<String>>` (`email_states`, `mailbox_states`,
`lifecycle_mailbox_states`). Proposal: `ObjectState<O>` (phantom on the object),
`QueryState<O>`, `SessionState`, `PushState` in `core/id.rs`, with
`ChangesMethod::since` and `define_set_method!`'s `if_in_state` taking the
object-typed state.

## STR-087 - A capability-scoped JMAP account forgets its capability

**Reported by:** jmap-protocol.

`Client::primary_account::<C>()` selects the account by capability and returns
`Account<Tr>`, discarding `C`. `JmapAccount` holds `mail`, `submission`,
`vacation`, `quota`, `sieve`, `contacts`, `calendars` and every foreign account
as the same `MailAccount` type, so handing the mail account to a
`CalendarEventGet` compiles and silently addresses the wrong `accountId` on a
server whose capabilities use different primary accounts. `Request::call` knows
`M::Cap` but uses it only to fill `using`. `Request::new` defaults to
`Session::default_account_id`, the lowest-URI primary - documented as an open
product decision about generic requests. Proposal: `Account<C, Tr>` (or
`CapAccount<C>`), with `call<M: JmapMethod<Cap = C>>` or a `ServedBy<C>` bound for
methods any account serves (`Blob/copy`, core); with every request built from a
capability-scoped account, the generic default-account fallback has no caller and
the open question dissolves.

A partial migration was refused (it would break sync or leave the untyped door
open): completing this needs capability-typed sync fields, factory and helper
signatures, and checked cross-capability views for foreign submission and core
methods. A wave of one owning both halves of `crates/jmap`.

## STR-089 - Smaller JMAP axes travelling as primitives

**Reported by:** jmap-protocol.

The HTTP method enum, typed `Blob/lookup` names with automatic capabilities,
and `Timestamp` Email date setters landed. What remains needs coordinated
edits in `crates/jmap/src/sync/` (account maps and push routing, mutation
writers, `page_cursor.rs` and `pim.rs` search cursors, `calendar_ops.rs`), so
it wants a fixer owning both halves of the crate:

- Keywords as `String`, membership as `HashMap<MailboxId, bool>`: keywords have a
  grammar (IMAP atom, length bound) and the workspace treats flag identity as
  ASCII-case-insensitive (STR-004); a `Keyword` newtype in `email/` would own
  both. `mailboxIds` / `keywords` / `calendarIds` / `addressBookIds` values are
  always `true`; they are sets.
- Account ids as `String` past the request boundary: `Session::accounts`,
  `primary_accounts`, `PushObject::StateChange::changed`, `Changes`,
  `EmailPush::account_id`, `CalendarAlert::account_id`, and on the sync side
  `foreign_mail: HashMap<String, MailAccount>`. `AccountId` exists and is used
  only inside method structs.
- Query position is `i32` on the request and the response (RFC: response
  `position` is `UnsignedInt`), with `i32::try_from` at the sync call sites; a
  `QueryPosition` owning the conversion ends the ceremony.
- JSCalendar values as `String` (`CalendarEventCreate::start`, `duration`,
  `GetArguments::recurrence_overrides_before`, filter bounds), with the
  LocalDateTime / UTCDateTime / Duration grammar as `strftime`/`strptime` format
  strings and a hand parser in `sync/calendar_ops.rs` (BUG-060); they belong as
  types in `calendar_event/`.
- `from_timestamp(i64) -> Timestamp` (`core/set.rs`) still returns `UNIX_EPOCH`
  for an out-of-range input; the Email setters no longer use it, but filters,
  vacation dates and push-subscription expiry still do and can fabricate a
  1970 date.

## STR-090 - JMAP failures that reach the caller as prose, or with the wrong provenance

**Reported by:** jmap-protocol.

A `Blob/upload` refusal now keeps its typed `SetError`, and full create and
update failure iterators exist. The rest needs the protocol layer and
`sync/error.rs` changed together:

- `Error::NotParsable(String)` is still overloaded: an unparsable blob id
  (`ParseResponse`), the `Blob/copy` response naming the wrong accounts (a
  provider contract violation, which should classify as one), and, in
  `sync/pim.rs`, a missing Email state; all classify as `Protocol(ParseFailed)`.
- `From<serde_json::Error> for Error` maps every stray `?` to `ResponseDecode`;
  the `RequestContract::Send` contract compensates. Decode provenance is a fact at the source and should be a variant:
  `EnvelopeDecode` (from `Client::send_request` / the WS frame), `ResultDecode {
  call_id }` (from `Response::get`), and no blanket `From`.
- `SetResponse::unwrap_update_errors` / `unwrap_create_errors` still return the
  first error from a `HashMap` iteration; their sync callers should move to the
  new full-failure iterators.
- `TransportError { message: String, .. }` with `TransportError::new(message)`
  and no `net` evidence classifies as a generic network fault (STR-091); the
  message is the only carrier.

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

`JmapAccount` has twenty-five fields and a twenty-one-argument positional
constructor (`#[allow(clippy::too_many_arguments)]`) that tests call with
`HashMap::new()` five times in a row. The per-JMAP-account facts are spread across
six maps keyed by bare `String` ids (`foreign_mail`, `foreign_submission`,
`seed_states`, `email_states`, `mailbox_states`, `PushRouting::foreign_scopes`),
built at open in `factory.rs` (`SeededOpenState`, `apply_foreign_seed`) and
re-joined by every door. Trait methods are mostly one-line forwards passing
`(handle.clone(), Arc::clone(&self.email_states), handle.id_str().to_string())`
into a free function - about twenty-five such call sites in `account.rs`. Nothing
stops a door pairing one handle with another account's state key. Proposal:
`Registry { primary: Slot, shared: BTreeMap<JmapAccountId, Slot> }` plus
`registry.route(&JmapRef) -> Route` (STR-002). `Slot` owns `handle`, `limits`
(CON-045), `email_state: StateCell` (BUG-068), `families` (submission, vacation,
... per slot, making shared-account submission an ordinary slot bit instead of
`foreign_submission: HashSet<String>`), and its `JmapScope` (STR-098).
`PushRouting` becomes a view of the registry rather than a second copy built from
`seed_states`. Doors take `&Slot` or `Route` and nothing else.

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

## STR-108 - One Graph `$batch` engine with typed legs and answers

**Reported by:** graph.

`types::BatchRequestItem { method: String, headers: Option<HashMap<String, String>>
}`. A leg's replay decision is made after construction by
`graph_error::subrequest_replayable`, which re-parses the method string and every
header and re-derives "a PATCH is `IdempotentUnlessConditional`". The outer
`$batch` POST's policy is written by hand at each submission site
(`get::fetch_batch` uses `Idempotent`; `mutate::submit_batch_with_version`,
`pim::messages::submit_write_batch_resource` and `reactions` each choose their own),
with nothing tying it to the legs. On the response side, `BatchResponseItem {
status: u16, headers, body: Option<Value> }` is re-converted at every consumer:
headers rebuilt into a `HeaderMap` in four places (`get::reconstruct_headers`,
inline in `mutate::reconcile_mutation_responses`, inline in
`submit_write_batch_resource`, `reactions::header_map`), the body re-serialized in
three, the status through `from_u16(..).unwrap_or(500)` in two (BUG-081). And
`destroy: bool` on `mutation_item_outcome` / `submit_write_batch_resource` carries
two meanings ("a 404 on this leg is success" and "evict the cached etag on
success"); `pim::messages::delete_extended_property` passes `true` purely for the
404 absorption and passes no targets so the second meaning does not fire.
Proposal: one module owning `struct BatchLeg { method: Method, url: AdmittedPath,
precondition: Option<GraphETag>, replay: ReplayPolicy, absent: AbsentMeans }` and
`struct BatchAnswer { status: StatusCode, headers: HeaderMap, body: Bytes }`,
chunking at 20, the outer replay as the meet of the legs' policies (computed,
never written by hand), response resolution, the absent-leg policy (Uncertain for
writes, `PartialResponse` for reads), and etag observation (BUG-076). Hydration,
bulk mutation, PIM writes and reactions become thin front ends; about 600 lines
across four files answer these questions slightly differently today.

## STR-112 - The recurrence rule is a `String` that three crates each parse

**Reported by:** graph (cross-crate).

`bifrost_types::EventRecurrence { rrule: Option<String>, rdate: Vec<String>,
exdate: Vec<String>, .. }`. Graph (`calendar::graph_recurrence_rule`, `ParsedRRule`,
`RRULE_MAPPED_PARTS` / `RRULE_UNMAPPED_PARTS`), JMAP
(`calendar_ops::jmap_recurrence_rule_from_rrule`, `validate_shared_recurrence`)
and CalDAV (`rrule_until`) each parse it and decide which RRULE parts are valid and
supported. Validity is protocol-neutral (RFC 5545 grammar); expressibility is per
provider. Proposal: `RecurrenceRule` parsed at the `bifrost-types` boundary with
`UNTIL`/`COUNT` and `BYDAY` typed, each provider answering only "can I express this
parsed rule". Today a rule JMAP accepts and Graph refuses is refused for different
stated reasons, and nothing compares the parsers. Related: STR-003.

## STR-115 - `GraphAccount` is a bag of about twenty-five shared fields with no owners

**Reported by:** graph (the hunter says this is where concentrating invariants pays most, given the standing lesson that teardown and lifetime fixes move holes).

`account/mod.rs` holds the primary and shared clients; capabilities and the push
mode and endpoint; the push lease, push tasks, two topology watches,
`pending_gaps`, the EWS ledger, two subscription maps and the recreated-id ledger;
the cursor index, folder tree, etag index and trash cache; the public-folder state
(routing map, metadata, item classes, scope); the user email, folder seed and
shutdown token. The behaviour is free functions in other files reaching into the
fields: `webhook.rs` locks `graph_subscriptions`, `recreated_subscription_ids` and
`graph_topology` by hand, and documents lock-ordering invariants in prose ("Every
write and the teardown snapshot happen under the `graph_subscriptions` write
lock"). The real components: `MailboxRouter` (primary and shared clients,
`client_for_*`, `budget_for_*`, `owner_of_scope`, the identity of BUG-077);
`PublicFolders` (`routing_map`, `public_folder_meta`, `public_item_classes`, the
public-folder scope, the resolver of BUG-079); `WebhookRegistry` (groups, the
recreated-id ledger, the topology watch, pending gaps, teardown and renewal as
methods owning their locks); `EwsRegistry` (subscriptions, topology watch, release
ledger); `EtagIndex` (BUG-076's API) and the trash cache. `GraphAccount` composes
them, and invariants stated in comments become private to the type holding the
lock. The JMAP twin is STR-097.

## STR-116 - Fold the two Graph delta walks into one driver

**Reported by:** graph.

`inventory::inventory_stream_from` and `changes::changes_stream` are the same loop:
`PageWalk::enter`, `fetch_delta_page`, admit next and delta links; project each
value and update the etag cache; branch on next link, delta link, or neither
(contract violation). Each rebuilds `GraphErrorContext::graph(op).with_scope(ErrorScope::Cursor(scope.clone()))`
about ten times. They have diverged on the id-less value verdict (BUG-080), and
their termination sequencing is inconsistent: `changes_stream` yields `Terminated`
without `Done` on its first refusal and `Terminated` plus `Done(None)` on every
other path. Proposal: one `DeltaWalk { scope: GraphScope, client, ctx }` driver
with a `DeltaProjector` per lane (inventory entries, removals and obligations; or
changes) - the move the JMAP inventory-walk fold made. The public-folder EWS poll
is a different protocol and stays separate.

## STR-117 - Graph wire codecs live in the account layer

**Reported by:** graph.

- `account/ews_stream.rs` mixes the push session driver with a byte-level frame
  decoder (`StreamingFrameDecoder`, `find_frame_start` and friends) and the
  Subscribe, GetStreamingEvents and Unsubscribe XML build and parse; the codec
  half belongs in `ews/` beside `ops.rs` and `parse.rs`.
- `account/autodiscover.rs` (about 2,400 lines) is an XML protocol codec (SOAP
  `GetUserSettings`, POX, redirect walk) plus `impl GraphAccount` methods; the
  codec and redirect walk are a sibling of `ews/`.
- The XML text unescape helper `push_text` exists four times
  (`ews/xml_helpers.rs`, `ews/parse.rs`, `account/ews_stream.rs`,
  `account/autodiscover.rs`) - identical and harmless, but showing the missing
  shared XML layer.
- `account/calendar.rs` (about 4,300 lines) holds the typed calendar operations,
  an RRULE codec of about 800 lines, a Windows time-zone table, and its own wire
  DTOs; `GraphRecipient` and `GraphEmailAddress` are defined both here and in
  `types.rs` with different field shapes. The RRULE codec moves to types under
  STR-112, or to its own module if not.
- `types.rs` and `contacts.rs` each define a `CONTACT_SELECT` constant with
  different values (the sync select and the typed-read select).

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

## STR-121 - Split the Google client into one typed handle per API

**Reported by:** google (its headline structural fix).

Use one typed handle per API (`GmailApi`, `PeopleApi`, `CalendarApi`, `DriveApi`),
each a thin view over the shared transport, owning its base URL, `ApiSurface`,
quota scope and cost table, concurrency rule, error service and wire types; URL
sniffing, the service string and the hand-set context field all go (BUG-086,
BUG-087). Proposed layout: `transport.rs` (shared `AccountNet` attachment,
`ByteTally`, `send_recorded`), `gmail/{api,types,quota,endpoint}.rs`,
`people/{api,types}.rs`, `calendar/{api,types}.rs`, `drive/{api,types}.rs`
(resumable session, permissions, base URL configurable), `error.rs` (`Error`
carries the `ApiSurface`; `GmailService` grows to all four or is replaced by
`ApiSurface`), `account/...`. Today the Calendar and People wire types
(`GoogleEvent`, `Person`, ...) and URL builders live inside `account/calendar.rs`
and `account/contacts.rs`, mixed with the `Account` mapping; only Gmail has an
`api.rs` and `types.rs`; Drive hard-codes `SESSION_URL` and
`https://www.googleapis.com/drive/v3/...` with no base override, unlike the other
three. Two pairs of duplicate Gmail wire types exist: `inventory.rs` defines its own
`ListMessagesResponse` and `GmailMessageStub` beside the ones in `types.rs`, and its
own `list_messages_page` beside `api::list_messages`.

## STR-122 - Google HTTP method and endpoint identity travel as strings

**Reported by:** google.

`execute` and `execute_if_match` take `method: &str` and `unreachable!()` on
anything not one of five strings; `gmail_quota_cost(path, method)` is keyed on the
string, and `post_empty_json` passes the literal `"POST"` back in to look up its own
cost. "Which Gmail endpoint is this" is answered from three representations: the
quota table re-parses the URL into segments, the concurrency bypass in
`execute_if_match` string-matches `"/watch" | "/stop"`, and error classification
keys on `GmailErrorContext.history_endpoint: bool` (the five `if
ctx.history_endpoint` arms in `classify_response` and `classify_by_status`).
Proposal: a `GmailEndpoint { Profile, History, Watch, Stop, MessagesGet,
BatchModify, .. }` minted by the `api.rs` method building the request, owning the
unit cost, the concurrency class and the history-endpoint classification.

## STR-125 - Gmail label ids and flag tokens need types

**Reported by:** google.

The label vocabulary disagreements are BUG-091. Proposal: a `GmailLabelId` parsing
into `System(SystemLabel) | Category(..) | User(String)`, with `SystemLabel { Inbox,
Sent, Draft, Trash, Spam, Unread, Starred, Important, Chat }` and methods
`is_exclusive`, `is_writable`, `role`, `search_operator`, `flag`;
`GmailLabel.label_type` an enum with an `Unknown` arm, decided once. And the
`$gmail-label:<id>:<name>` flag encoding is produced in `canonical_flags_indexed` as
a literal `format!` (not with `USER_LABEL_FLAG_PREFIX`) and in `patch_for_set` with
the constant, and parsed in `flags::user_label_id_from_flag`,
`flags::gmail_flag_target`, and `mutation::has_unresolved_label_id` (its own literal
`"$gmail-label:"` plus an extra empty-id filter the others lack). A `GmailFlag` enum
with `parse` and `Display` (`Seen`, `Flagged`, `Draft`, `Important`, `Label { id,
advisory_name }`, `Unknown(String)`) replaces all four. Related: STR-004, STR-111.

## STR-128 - Google stream state machines as boolean pairs

**Reported by:** google.

`ChangeState`, `HydrateState` and `MutationState` each encode their phase as
`finished: bool` plus `emitted_done: bool`, plus `checked_profile`,
`pending_termination`, `pending_event` and `Option` fields whose combinations
include impossible states; `changes.rs` has an arm minting
`Internal(InvariantViolated)` for "checked the profile but has no start history
id", existing only because the state allows it. The doc comment on
`get_stream_cancellable` argues soundly against a shared driver, but not against a
per-stream phase enum, e.g. `enum Phase { Probe { cursor }, Walking { start:
HistoryId, .. }, Terminating(AccountError), Done }`, which makes the
invariant-violation arm unconstructable.

## STR-129 - Google's mailbox watch registry is process-global

**Reported by:** google.

`push::MAILBOX_WATCHES` is a `static LazyLock<Mutex<HashMap<String, Weak<..>>>>`,
process-wide rather than scoped to the `Net` or the factory; the tests mint unique
mailboxes because of it (`NEXT_MAILBOX` in `mod.rs` tests). bifrost-net already
offers Net-scoped string-keyed registries (`shared_hold`,
`shared_concurrency_limit`) for this kind of cross-account coordination; the
coordinator belongs in one, keyed by the typed mailbox identity (STR-124), so two
independent `Net`s really are independent.

## STR-130 - Google `pim.rs` does several unrelated jobs

**Reported by:** google.

Single-object label mutations; send; the draft read-modify-write; MIME body
selection (`body_parts`, `collect_body_parts`, a re-implementation of
`mime::select_body`, BUG-095); search translation; container CRUD; identities;
vacation; thread and message hydration; `MailDocument`. Suggested split:
`labels_single.rs` (or fold into `mutation.rs` with the shared move rule, BUG-089),
`compose.rs` (send, drafts, `MailDocument`), `hydrate.rs` (Message and Thread over
the shared MIME selector), `search.rs` (with one query encoder, CON-053),
`settings.rs`. The same shape in IMAP and JMAP is STR-079 and STR-104.

## STR-133 - SASL errors are prose; SCRAM server errors carry a magic prefix

**Reported by:** smtp-sasl.

`SaslError::AuthFailed(String)` is built in `verify_server_final` as
`format!("SCRAM server error: {error}")`, and `SaslError::scram_server_error()`
recovers the token with `strip_prefix("SCRAM server error: ")`; SMTP's
`From<SaslError>` branches on `scram_server_error() == Some("invalid-proof")`, and
tests on both sides assert on the exact prose. RFC 5802 section 7 enumerates the
`server-error-value` vocabulary (`invalid-encoding`, `extensions-not-supported`,
`invalid-proof`, `channel-bindings-dont-match`,
`server-does-support-channel-binding`, `channel-binding-not-supported`,
`unsupported-channel-binding-type`, `unknown-user`, `invalid-username-encoding`,
`no-resources`, `other-error`, plus extensions). Proposal: `enum SaslError {
Protocol(String), InvalidCredential(String), ServerRejected(ScramServerError) }`
(typed, with `Other(String)`), plus a local-runtime case (BUG-102), with
`ScramServerError::is_credential_rejection()` owned by bifrost-sasl - what makes
BUG-100 fixable in one place. Mechanism names on error stamps are the same smell:
`SmtpCommandPhase::Auth { mechanism: Option<&'static str> }` and IMAP `Error::Auth {
mechanism: Option<&'static str> }` carry a string though each crate has a mechanism
enum ("the name that went out on the wire" is the enum's `name()`, not a reason to
erase it); `ScramExchange` stores both `hash` and a separately computed
`mechanism_name: &'static str`; IMAP's `AuthenticateXoauth2Consumer` carries
`mechanism: &'static str` because one consumer serves two mechanisms. Typed, these
become `Option<Mechanism>`. Lateral: `cram_md5_response` maps an HMAC
key-construction failure to `SaslError::Protocol` ("invalid CRAM-MD5 key"), blaming
the peer for the caller's password (HMAC accepts any key length, so unreachable,
but wrongly classified if it fires); same for `hmac_digest` in `scram.rs`. Several
SMTP and IMAP tests assert on error prose (`to_string().contains("unsolicited
reply")`, exact `"SCRAM server error: ..."`); typed errors would let them assert on
variants.

## STR-134 - `select_scram_rung` answers three questions through one signature

**Reported by:** smtp-sasl.

`select_scram_rung(hash, plus: bool, advertised: impl Fn(&str) -> bool,
binding_data: Option<&[u8]>) -> Option<(ScramHash, ScramChannelBinding)>` is called
four ways, each decoded differently: SMTP `password_mechanism` passes
`binding_available.then_some(&[][..])` - a fake empty binding - only to learn
whether a rung is eligible; IMAP `password_mechanism_ladder` passes `None` and
`plus = false` to ask a different question ("is the unbound rung
downgrade-forbidden"); IMAP `authenticate_best` destructures its own resolved
binding with `_ => unreachable!("certificate resolver returns bound data")` to feed
the bytes back in; SMTP `SmtpConnection::auth` (and the async twin) calls it again
after selection with `.expect("selected SCRAM rung remains advertised")`. The echoed
`ScramHash` never differs from the input; `plus: bool` duplicates `ChannelBinding`;
the binding bytes are a bare `Vec<u8>` / `&[u8]` anything can mint. Proposal: a
`ChannelBindingData` newtype only `tls_server_end_point` constructs,
`binding_from_peer_certificate(Option<&[u8]>) -> Result<Option<ChannelBindingData>>`
(replacing the two comment-held copies in BUG-030), and `scram_rungs(advertised:
&dyn Fn(&str) -> bool, binding: Option<&ChannelBindingData>) -> Vec<Result<ScramRung,
ScramSkip>>` with `ScramSkip::{NotAdvertised, PlusWithoutBinding,
DowngradeForbidden}`; IMAP's `AuthMechanismRejectionReason::ChannelBindingUnavailable`
and SMTP's `skipped_plus_for_binding` both fall out of `ScramSkip`.

## STR-135 - bifrost-sasl should own the SASL client, not only the arithmetic

**Reported by:** smtp-sasl.

The crate's stated scope - "computation-only: no I/O, no async, no protocol command
flow" - is right, but drawn too close to the math: a SASL client exchange (initial
response, step on a challenge, verify, done) is itself pure, protocol-neutral
computation. Because sasl stops at `scram_client_final`, every consumer
reimplements the client around it (BUG-030, BUG-100 to BUG-102), and the crate's
docs carry rules for its consumers ("a PLUS consumer MUST build client-first from
this, or the desync this type prevents re-enters at the consumer boundary"). The
SCRAM exchange is written twice: SMTP `ScramExchange` ("Mirrors IMAP's
`AuthenticateScramConsumer`") and IMAP `AuthenticateScramConsumer` each hold
`client_nonce`, `client_first_bare`, `binding`, the prepared password,
`expected_server_signature` and a three-or-four-state enum, each formats
`n={user},r={nonce}`, prefixes the GS2 header, calls `scram_client_final`, stores the
signature and calls `verify_server_final`. LOGIN and OAuth are stateless encoders
pretending: `Mechanism::response_with_token_at_index(credentials, challenge,
challenge_index: usize, oauth_token: Option<&str>)` drives LOGIN's username-then-password
by an index the connection loop must count correctly (the comment warns it must not
derive from the remaining-challenge budget), and the OAuth `None` token is a "driver
bug" error at runtime (`oauth_identity_and_token`). Target surface: `Mechanism`
(shared vocabulary, names, `from_advertised`, credential exposure);
`negotiate(advertised, policy, encrypted, binding) -> Ladder` with typed skips
(absorbing `select_scram_rung`, both ladders, the cleartext gate); a `SaslClient`
per mechanism (`Plain`, `Login`, `CramMd5`, `XOAuth2`, `OAuthBearer`, `Scram { hash,
binding }`) with `initial_response()` and `step(challenge) -> Step { Reply(Secret),
Done }`, owning nonce, GS2 header, client-first-bare, expected signature, OAuth
error-continuation answer and LOGIN position, so "OAuth mechanism without a token"
cannot be constructed; `ChannelBindingData` + `binding_from_peer_certificate`
(STR-134); typed `SaslError` (STR-133) with one `refusal_class()`. What stays in IMAP
and SMTP: framing (`AUTHENTICATE` + `+` vs `AUTH` + `334`, SASL-IR, base64 line
framing), the `235 v=...` field quirk, capability snapshots, deadlines; ManageSieve
becomes a third thin framing layer and gets OAUTHBEARER and SCRAM for free. Lateral:
bifrost-sasl is described as private and "not public", but it is in the
`install-shape` published set and its `Secret` appears in IMAP's and SMTP's
internals, so the "private" framing will mislead if it ever becomes a public type of
a published crate; and `strip_frame_delim` in `oauth.rs` silently rewrites a
credential containing `\x01` rather than refusing it (to keep the builders
infallible) - behind a fallible `SaslClient`, refusing is free and honest.

## STR-136 - SMTP enhanced status codes are demoted to consent-tier prose at the boundary

**Reported by:** smtp-sasl (a bifrost-types change; SMTP is its only producer).

bifrost-smtp parses a typed `EnhancedStatusCode { class, subject, detail }`. At the
account boundary `smtp_wire_cause` converts it to `bifrost_types::error::EnhancedStatusCode
{ reply: u16, enhanced: Option<DiagnosticText>, text }`, and
`response_to_account_error` sets `native_code` to the same dotted string. The
structured, non-secret fact `5.1.1` reaches consumers only as support-only text
behind the diagnostics consent tier, so a machine caller cannot branch on X.1.1 vs
X.2.2 without parsing prose it may not be allowed to read. The type in bifrost-types
should carry the three numbers. Lateral: `classify_status` / `classify_enhanced`
sniff reply text for "rate", "limit", "throttle", "mailbox", "storage", "quota",
"policy", "security" on 552, 554 and the 4.7.x fallback - reasonable heuristics, but
the only classification in the crate driven by free-form server prose, deserving a
named table rather than inline `contains` chains.

## STR-138 - Smaller SMTP axes and placement notes

**Reported by:** smtp-sasl.

The reply-mode enum, `MechanismChoice`, the credentials and challenge-framing
split, and the shared message-error strings landed. One residue needs a
bifrost-net API first: `client/metering.rs`'s blocking admission loop belongs
beside `BandwidthBudget` in bifrost-net as a blocking budget API, which does not
exist yet. (The rest of `authentication.rs` moving to bifrost-sasl is STR-135.)

## STR-139 - The SMTP sans-IO core stops one layer too low

**Reported by:** smtp-sasl.

`client/core.rs` is a deliberate sans-IO design for the transaction
(MAIL/RCPT/DATA/BDAT/RSET/LMTP drain). Its header lists what it leaves to the
adapters: "connecting, the greeting and EHLO/LHLO exchange, the AUTH ladder, the
STARTTLS upgrade, reply-line reading and parsing ...". Everything on that list
except connecting and deadlines is protocol, and each is written twice: AUTH
(`SmtpConnection::auth` / `auth_scram` / `auth_legacy` / `verify_scram_server_final` /
`write_auth_continuation` and the async versions, including the `334`-vs-`235`
server-final branch, the ten-challenge loop, and `decode_scram_payload`'s
enhanced-code skip); reply framing (line cap, total cap, UTF-8 check,
unsolicited-reply check, `nom` incomplete handling, "incomplete response" on EOF, in
`read_response_until` and its async twin); STARTTLS (the "unsolicited reply before
the STARTTLS upgrade" refusal); and the `try_smtp!` macro ("mirrors the sync
sibling"). The blocking and async connection files carry parallel suites with the
same test names (`starttls_downgrade_is_refused_without_a_wire_command`,
`starttls_refuses_plaintext_bytes_buffered_after_the_reply`, ...), so a rule is
pinned twice and can drift once. Proposal: extend `Op` / `OpOutcome` /
`ProtocolMachine` to a session machine (`Greeting -> Hello -> (StartTls -> Hello)? ->
Auth(ladder) -> Ready`, with `Op::UpgradeTls` and `Op::ResolveToken` - the one place
blocking and async genuinely differ: async awaits `current()`, blocking takes
`snapshot()`), plus a sans-IO `ReplyAssembler` fed lines; both adapters shrink to
read-a-line / write-bytes / upgrade / deadline, and one transcript-driven
conformance suite runs every scenario through both. Side note: EOF mid-reply is
reported by both readers as `error::parse("incomplete response")` - a provider
contract violation - for what is a connection closing under us; a transport drop
with InFlight evidence is the truthful class.

## STR-140 - Four SMTP protocol machines for one transaction

**Reported by:** smtp-sasl.

`DirectSmtp`, `DirectLmtp`, `BatchSmtp`, `BatchLmtp` share a skeleton (`Writing`,
`MailReply`, `RcptReply`, `WindowWritten`, `WindowOpened`, `MailWindowReply`,
`RcptWindowReply`, `WindowClosing`, `DataReply`, `BodyWritten`, `FinalReply` /
`FinalStatus`) and each re-decides what a negative MAIL FROM, a negative RCPT, a
421 and a write failure mean. It has already drifted: `DirectLmtp`'s MAIL FROM arm
carries the comment "Aborting here cost one reconnect per rejected envelope ... and
made the direct LMTP path differ from both `DirectSmtp` and `BatchLmtp`", and
`closing_channel` exists because "the end-of-data arms had it and the envelope arms
did not". Capability asymmetries fall out of the split rather than design:
`DirectLmtp` and `BatchLmtp` never pipeline; the batch API has no BDAT
(`build_batch` passes `allow_binary_mime = false`, `BatchSmtp` has no `BodyKind`);
`PhasedError`'s "cannot ship undecorated" guarantee applies to `DirectSmtp` only
while `DirectLmtp` uses `or_phase`. Proposal: one machine, generic over a dialect
(SMTP: one final reply for all accepted; LMTP: one per accepted; STR-132) and a
recipient policy (`FailFast` - RSET on the first rejection - vs `PerRecipient`),
always recording into `SendProgress` (BUG-105); the four public result shapes become
projections, and CON-057's disagreement becomes an explicit policy parameter.

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

## STR-143 - Submission composition lives in IMAP but is not IMAP's

**Reported by:** smtp-sasl.

`imap/src/account/submission.rs` holds a TLS-mode enum (`SubmissionTls`) mirroring
both IMAP's TLS modes and bifrost-smtp's `Tls`; default ports (BUG-107); a credential
type (STR-078); the envelope-address conversion (BUG-103); the HOLDUNTIL formatting
(BUG-054); and the BatchOutcome-to-send-result projection (CON-057). None is about
IMAP. Proposal: bifrost-smtp exposes a `Submission` facade (`from(SubmissionConfig)
-> Submission`, `submit(&SubmissionEnvelope, raw, hold: Option<SystemTime>) ->
Result<SubmissionReceipt, AccountError>`) in `bifrost_types` vocabulary, reused by
any future account crate composing SMTP (a CalDAV-only or JMAP-without-submission
deployment) rather than copying IMAP's file; IMAP keeps only the decision to own
one.

## STR-144 - bifrost-net's error evidence fields need a typed ledger

**Reported by:** net-dav.

The typed `AttemptLedger` (`crates/net/src/attempt.rs`) now owns the internal
evidence and the proven-refusal rule, and net and DAV feed it. Residue: the
public error-envelope fields (`may_have_committed: bool`, `attempts: Vec<Error>`
with nested `AttemptHistory`) were kept so consumers compile unchanged; move the
public surface onto the ledger's verdict and migrate the consumers.
`RetryBudgetExhausted` means two things - "the attempt budget is spent" and "the
replay policy forbade another attempt" (its doc says so) - so a caller cannot branch
on which happened, though the second is the commit-ambiguity case; split it or add a
`StopReason`. The server's retry hint has four representations:
`RateLimited.retry_after: Option<Duration>`,
`RetryBudgetExhausted.retry_after_history: Vec<Duration>` (capped and resolved, so
lossy), `RefreshFailed.retry_after: Option<SystemTime>`, and
`bifrost_types::RetryHint`; `account_error` re-parses the `Retry-After` header because
the stored durations lost the HTTP-date form. Store `RetryHint` at observation time
and derive the capped wait from it. `HoldKind { RateLimited, QuotaExhausted }` also
restates two `ServerErrorKind` variants that the `LocalHold` arm re-maps.

## STR-145 - bifrost-net registry keys are unnamespaced strings

**Reported by:** net-dav.

`Net` holds three registries keyed by bare strings, and the rate governor adds a
fourth key space on top of the host: quota scope
(`RequestBuilder::quota_scope(String)`, `RateLimit::with_quota_scope(String)`, where
an empty string means "the host-only compatibility bucket"); holds
(`hold_gate(String)`, `shared_hold(String)`); concurrency limits
(`shared_concurrency_limit(String, usize)`). Google builds one key as
`format!("gmail:{:?}", (account, origin))`, so the `Debug` output of a tuple became a
registry key; Graph uses the raw account id as its quota scope. Type fix:
`QuotaScope` (with an explicit `HostOnly` variant), `HoldKey` and `LimitKey` newtypes
- or better, one provider-minted `BudgetKey` naming the quota, the hold and the
concurrency scope together, a fact Google spells as three strings today. Related:
CON-009, CON-013, STR-129.

## STR-146 - URLs travel as `String` through every net and DAV layer

**Reported by:** net-dav.

`RequestBuilderInner.url` (re-parsed for the host on every hop and again for
redirect classification), `redirect_guard: Fn(&str, &str)`,
`RedirectStep.next_url: String`, `DavRequest.url`, `DavResponse.url` and
`DavBody.url`, `DavDispatch::trusted_origins: Vec<String>` and `base_url: String`,
and every DAV native id. `reqwest::Url` is already a dependency; with an `Origin` and
a `DavHref` on top (BUG-112), several of the CON-060 and BUG-112 disagreements become
unrepresentable. Related: CON-047.

## STR-147 - Smaller bifrost-net axes

**Reported by:** net-dav.

The timeout enum, typed `ByteRange` validation and typed priority landed. The
two public reshapes below were deferred because they need consumer-crate
migration; do them as one change that also edits the consumers.

- `AccountSpec::connect_timeout` is a legacy alias for `response_headers_timeout`,
  so two fields carry one meaning and a precedence rule lives in `attach_account`
  (reshaping is ruled fine; removing the field needs the owner).
- `RedirectPolicy`: `same_origin_only: bool` beside `trusted_hosts: HashSet<String>`;
  the bool silently overrides the set, and in that mode `allows_host` returns false
  for every host including the departing one, as the docs warn. Type fix:
  `CrossOrigin { Any, AllowList(set), Forbidden }`.

## STR-150 - A DAV collection is a `CursorScope::Folder`, so IMAP resolves ownership by string collision

**Reported by:** net-dav.

When DAV is composed into IMAP, `DavScopeIndex` decides which sub-account owns a
scope by comparing `FolderId` strings with IMAP mailbox names and the other
dialect's collections, and on a collision emits a prose `Warning` ("... this
collection will not sync", also BUG-045). A typed provenance makes the collision
impossible to express: a dedicated scope variant for a DAV collection, or a
`FolderId` carrying its namespace. Related: STR-001, BUG-052.

## STR-151 - bifrost-net needs a public exchange core; `send_streaming_inner` does five jobs

**Reported by:** net-dav (one of its two "substantial rewrite" verdicts).

`send_streaming_inner` runs about 580 lines and builds `InternalStreaming` in four
places. The hop state is a dozen `let mut` locals rewritten by hand in the redirect
arm: method, url, body, headers, host, quota scope, cost, cost units,
`auth_for_next_hop`, `carrying_auth` and `auth_self_stripped` - exactly the state DAV
rebuilds as `DavRequest` plus its own flags. Today DAV cannot use net's redirect
walk, 401 recovery, deadline or attempt evidence, so it re-implements all four, and
the copies already disagree (BUG-110, CON-060). Proposed split: a `Hop` with
`follow(step)`, `rate_key()` and `credential()`; the `AttemptLedger` (BUG-110); a
public `RequestDeadline`; a pass-through outcome constructor; plus the per-hop policy
trait of CON-060 (credential minting, method-rewrite mode, header rewrite, target
guard) - one hop state, one attempt ledger, one deadline. A related missing mode
(STR-154) completes it.

## STR-152 - bifrost-dav-core is half a consolidation; the account layer is still mirrored in caldav and carddav

**Reported by:** net-dav (its other "substantial rewrite" verdict).

dav-core owns the dispatcher, the 207 machine and the snapshot codec. Still
duplicated line for line, with "twin of" or "must stay in step" comments - in
`client.rs`: `MULTIGET_BATCH_SIZE` and `MULTIGET_LEG_CONCURRENCY`; `fetch_events` /
`fetch_vcards` (chunking, body building, buffered fan-out, the `worse_recovery`
fold); `accumulate_leg`, `run_leg`, `MultigetLeg` and `MultigetFetch::settle`;
`href_query_leg` and `HrefLeg`; the per-property text-search fan-out with the
`FilterUnsupported` degrade; `multiget_failure`, `listing_failure` and
`complete_failure_error`; `QUERY_HREF_PROPS`; six error-constructor shims. In
`account.rs`: `RelocateFailure` and `relocation_error` (byte-identical apart from the
scope constructor); `relocate_*` (MOVE, with copy-then-delete fallback);
`put_condition` and `resource_file_name`; `worse_recovery_option`; the cursor
wrappers (`cursor_from_snapshot`, `encode_cursor_snapshot`, `decode_cursor_snapshot`,
`cursor_error`, the diff and preserve wrappers); `validate_*_scope`,
`collection_url_for_scope`, `scope_collection_url`, `collection_coverage_domain`,
`default_collection_url`, `discovered_collection_urls`; `skipped_*_scope`,
`one_outcome_per_id`, `decode_*_page_cursor`; `*_PAGE_SIZE = 250`; about sixty mail,
compose, filter and settings `Unsupported` stubs per crate; `unsupported_future` and
`unsupported_stream`. The two `client.rs` test modules re-test dav-core's dispatcher
under the same names in both crates (credential origin, cross-origin redirect, hop
count, plaintext home, the discovery fallbacks, the multiget concurrency bound).
Proposal: (1) turn `DavProtocol` into a `Dialect` trait carrying what genuinely
differs - multiget element and namespace, data property, file extension, item
`ErrorScope` and `ResourceKind` (BUG-115), cursor magic, token kind (sync-token or
ctag), listing and multiget parsers, the typed item; (2) move a generic
`DavCollectionAccount<D: Dialect>` kernel into dav-core - snapshot polling, inventory,
changes (with a dialect hook so CalDAV can use `sync-collection`), paging,
relocation, conditional CRUD, the unsupported lanes; (3) CalDAV keeps iCalendar,
scheduling, recurrence ids and `sync-collection`, CardDAV keeps vCard, the directory
guard and the ctag lane; (4) the mirrored tests move into dav-core, parameterized over
both dialects - the dispatcher move applied one layer up.

## STR-154 - bifrost-net forces consumers to rebuild responses it already turned into errors

**Reported by:** net-dav.

Net turns every 4xx and 5xx into `Err`. DAV, Graph (`graph_envelope_evidence`) and
Google (308 handling, `UnexpectedResponse`) all need some statuses as data. DAV's
`wire_outcome` therefore unwraps `AttemptHistory.terminal()` to rebuild a response,
special-cases 501 inside `RetryBudgetExhausted` for the MOVE fallback, and carries
its private refusal list (BUG-110). Proposal: a per-request `statuses_as_data(set)`
mode returning `Ok(Response)` for the listed statuses with the ledger attached;
classifying the response is then the caller's call against the shared ladder
(BUG-072).

