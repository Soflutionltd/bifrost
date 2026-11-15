# Hunt: jmap-account (`crates/jmap/src/sync/`)

Scope: the `Account` / `AccountFactory` implementation of `bifrost-jmap`.
Question: what is in the wrong shape, or in the wrong place. Findings are
grouped by the three questions, then a list of defects found along the way.
Each defect is tagged live (reachable today) or latent (needs a producer or a
drift that does not exist yet). Nothing here was edited.

## Headline

The crate has one dominant missing type and one dominant missing owner, and
most findings below are consequences of them.

- **The missing type is an owner-qualified JMAP reference.** Whether an id,
  a scope, a container or a cursor belongs to the primary account or to a
  shared account travels as a `String` with an embedded `\u{1f}`, and a bare
  string means "primary". Every door re-parses it, re-decides whether the
  owner is registered, re-decides the fallback, and re-derives the wire form.
  There are at least ten routing sites and four wire-stripping functions, and
  the stripping functions already disagree.
- **The missing owner is a per-account registry.** `JmapAccount` holds the
  per-JMAP-account facts as parallel maps keyed by bare `String` account ids
  (`foreign_mail`, `foreign_submission`, `seed_states`, `email_states`,
  `mailbox_states`, `PushRouting::foreign_scopes`), and every door is handed
  a hand-assembled triple `(handle, email_states, handle.id_str())` that
  could be mismatched. A `Registry { primary: Slot, shared: BTreeMap<JmapAccountId, Slot> }`
  where a `Slot` owns the handle, its limits, its state cells and its
  capability bits, plus `registry.route(&JmapRef) -> Route`, would delete the
  bulk of the routing code and make the per-door fallback policy explicit.

The right move is a rewrite of the account layer's internal plumbing around
those two types, not a sequence of local fixes. The RFC client underneath does
not need to change.

---

## 1. Axes that should be types

### A1. Owner-qualified ids are strings with a magic separator

`foreign.rs` encodes `"{accountId}\u{1f}{native}"` for object ids
(`encode_object`), container/membership ids (`encode_foreign`) and the
account-level cursor scope (`encode_foreign_account`, empty native part). A
bare string is primary. The decoding is repeated at every door:

| Site | Question it answers | Unregistered owner |
|---|---|---|
| `account.rs` `route_object_id` | handle for a message/thread id | primary, literal |
| `account.rs` `route_mutation_target` | handle for a `MutationTarget` | primary, literal |
| `account.rs` `resolve_foreign_account_id` | owner of a `CursorScope` | `None` (primary) |
| `account.rs` `is_unregistered_foreign` | is the scope a departed share | terminal `ScopeRevoked` |
| `account.rs` `owner_of_scope` | owner tag for error isolation | `None` |
| `hydrate.rs` `route_for_id` / `HydrationRoute` | hydration route | primary, literal |
| `mutation.rs` `account_id_for_target` | bulk mutation route | primary, literal |
| `blob.rs` `foreign_split` | blob download account | primary, literal |
| `pim.rs` `thread_owner` | thread hydrate route | primary, literal |
| `pim.rs` `search_owner` / `route_search` | search route | `Request(Malformed)` |
| `pim.rs` / `mutation.rs` `owner_of` callers | "declared owner" for cross-account checks | ignores registration |
| `push.rs` `PushRouting::new` | push accountId -> scope | dropped |

And the wire form (native id for the routed account, literal otherwise) is
derived four times:

- `hydrate::wire_object_id(id, owner: Option<&str>)` strips only when the
  route is foreign and the owner matches.
- `mutation::wire_email_id(id, account_id, foreign_account: bool)` strips only
  when the caller passed `foreign_account = true` and the owner matches.
- `mutation::wire_mailbox_id` is the same rule for a mailbox.
- `pim::wire_id_for_mail(id, account_id)` strips whenever the owner equals
  `mail.id_str()`, **including on the primary route**.

The last one already disagrees with the other three: an id qualified with the
primary account's own id (`"{primary}\u{1f}x"`) is stripped and resolved by
`pim` but kept literal by `hydrate` and `mutation`. No producer mints that
shape today (foreign ids exclude the primary), so this is latent, but it is
the "copies held in step by care" shape, and `hydrate::wire_object_id`'s doc
comment says in words that it "must match what the mutation pipeline does".

There is also no validation at the mint sites that a native id from the
server does not itself contain `\u{1f}`. The codec's soundness rests on
RFC 8620 id syntax, which a non-conformant server can violate; a primary id
containing the separator would silently route as foreign.

**Shape that fixes it.** A parsed reference type, produced once at the
`Account` boundary and never re-parsed:

```rust
enum Owner { Primary, Shared(JmapAccountId) }
struct JmapRef<K> { owner: Owner, native: Id<K> }   // K = Email, Thread, Mailbox, Blob
enum Route<'a> { Primary(&'a Slot), Shared(&'a Slot), Departed(JmapAccountId) }
```

`JmapRef::parse` refuses a native part that contains the separator;
`JmapRef::to_consumer()` is the one encoder; `Route::wire(&ref)` is the one
stripping rule. Each door then states its `Departed` policy in one `match`
(stay literal on the primary route, refuse as malformed, or terminate the
scope as revoked) instead of re-deriving it from `Option`s. The distinction
the code currently carries in prose between "declared owner" (`owner_of`,
registration-agnostic) and "reachable owner" (the registered predicates)
becomes `ref.owner` versus `registry.route(&ref)`.

### A2. A shared JMAP account id travels as `bifrost_types::MailboxId`

The shared account's identity is typed as a *mailbox* id almost everywhere it
crosses a function boundary:

- `changes::stream`, `inventory::stream`, `email_inventory_loop` take
  `owner: Option<TypesMailboxId>`; `changes::stream` additionally takes
  `account_id: String` and the routed `mail` handle - three parameters for
  one fact, which the caller assembles from three different
  `JmapAccount` methods (`mail_for_scope`, `account_id_for_scope`,
  `owner_of_scope`).
- `foreign::owner_tag` emits `MembershipScope::Mailbox(MailboxId(accountId))`
  into discovery and into every shared inventory entry and Metadata
  hydration.
- `container_from_mailbox` sets `Container::with_owner(MailboxId(accountId))`.
- Open-time skips use `ErrorScope::Mailbox { id: accountId }`.

The owner tag is a real collision hazard, not only a naming smell:
`discover::memberships` emits the primary account's mailbox ids as
`MembershipScope::Mailbox(id)` in the same list as the owner tags
`MembershipScope::Mailbox(accountId)`. JMAP ids are server-chosen opaque
strings; servers that mint short sequential ids for both accounts and
mailboxes (Stalwart uses single-letter base32 ids for both) can produce a
primary mailbox and a shared account with the same id, and a consumer then
cannot tell "message is in primary mailbox `b`" from "message belongs to
shared account `b`". (Latent; depends on the server.)

This is a cross-crate vocabulary question: `MembershipScope` has no "owner"
variant, so every provider with shared mailboxes has to smuggle ownership
through `Mailbox`. Graph does the same thing with `/users/{id}` routing keys.
The owner should be a first-class `MembershipScope::Owner(..)` (or a field on
the entry) in `bifrost-types`, not a convention each crate re-implements.

### A3. "Which scopes does this account drive" is a `CursorScope` match in eight places

JMAP drives exactly three scope shapes: primary `Type(Email)`, primary
`Type(Mailbox)`, and one account-level `Folder` per shared account. Each of
the following re-derives that from a raw `CursorScope`:

- `state::JmapScopeRepr::from_cursor_scope` (codec; also accepts `Thread`,
  `Query`, and per-mailbox `Folder`, documented as "legal but dead").
- `account::describe_cursor_support` (decides `Cheap` / `ServerCursor`).
- `account::cursor_scopes_from_seeds` (discovery order, Email then Mailbox
  then sorted Folders - the sort exists only because `seed_states` is a
  `HashMap`).
- `account::fresh_cursor_establishment` (which probe, which state map).
- `changes::stream` (which `*/changes` walk, which state map; `Thread` and
  `Query` terminate `Unsupported`).
- `inventory::stream` (which walk; per-mailbox `Folder` takes an
  `inMailbox`-filtered walk nothing seeds).
- `push::data_type_for_scope` (accepts any `Folder(_)` without parsing it) and
  `push::scope_for_data_type`.
- `account::Account::inventory_stream` / `changes_stream` gates
  (`is_unregistered_foreign_scope`).

`seed_states: HashMap<CursorScope, OpaqueChangeState>` is the only record of
which scopes are live, and **its values are never read in production**:
`establish_initial_cursor` uses it as a membership gate only and always probes
fresh (`fresh_cursor_establishment`, by design). The factory encodes three
cursor payloads at open that nothing decodes.

**Shape.** `enum JmapScope { Email, Mailbox, Shared(JmapAccountId) }` with one
`TryFrom<&CursorScope>` and one `Into<CursorScope>`, held in a
`BTreeSet<JmapScope>` (deterministic order for free). Each of the eight sites
becomes an exhaustive `match` on three variants; adding a scope kind becomes a
compile error at every one of them instead of a review item. The codec keeps
decoding the dead tags so old rows still classify, but maps them to a
`LegacyScope` refusal at decode time rather than letting them flow to three
separate `Unsupported` arms. Whether the per-mailbox `Folder` branches in
`changes.rs` and `inventory.rs` are reachable at all depends on whether any
`PAYLOAD_ENVELOPE_VERSION = 2` build ever seeded per-mailbox scopes; if none
did, every such row is already refused as `SchemaIncompatible` by the version
gate and those branches are dead.

### A4. `StateWalk` is two types in one struct

`state_walk::StateWalk` holds `WalkProgress::{Changes, Inventory}` and every
mode-specific method opens with `let ... else { unreachable!() }`
(`observe_changes_page`, `reset_changes_burst`, `inventory_anchor`,
`observe_inventory_page`). The recovery policy enum
(`EngineRestart` / `SnapshotReseed`) is also only meaningful for the changes
mode. Two types (`ChangesWalk { recovery }`, `InventoryWalk`) sharing an
`error_context` trait remove four panics and make a mode mix-up a compile
error.

### A5. `JmapErrorContext` routes classification through booleans

`state_based`, `anchored_query` and `send_boundary` are independent `bool`s
that steer `convert_method`, `convert_transport` and the `ResponseDecode` arm.
Their legal combinations are a small enum of request contracts
(`Plain`, `StateRead`, `AnchoredQuery`, `Send`), and `state_expiry_error` /
the `AnchorNotFound` arms already have to handle the illegal combinations by
minting `Internal(InvariantViolated)` at runtime ("anchored query reached
error conversion without a cursor scope"). Making the contract an enum whose
state-bearing variants carry their `CursorScope` deletes those runtime
invariant arms. `provider: Option<Provider>` is never set to `Some` in
production and can go.

### A6. The state cache is an untyped map with fifteen hand-written update rules

`state_cache::StateMap = Arc<Mutex<HashMap<String, Option<String>>>>`. The
module documents a tri-state (absent / probed-empty / known) and then `get`
flattens it, so no caller can observe the distinction it was built for. There
are about fifteen writer sites across `account.rs`, `changes.rs`,
`discover.rs`, `mutation.rs` and `pim.rs`, each choosing `set` vs `advance`
and each choosing what to pass as `expected`.

Two consequences:

- **`mailbox_states` is write-only.** Container create/rename/move/delete,
  the Mailbox changes walk, establishment and the factory all maintain it,
  and nothing reads its value to make a decision (establishment reads it only
  to CAS against itself; the lifecycle poller has its own
  `lifecycle_mailbox_states`). The shared accounts' entries are seeded for a
  scope type (shared Mailbox) that does not exist.
- **The CAS uses the wrong "before" value (latent defect, P2).** Unguarded
  writes (`pim::send_email_set`, the unguarded branch of
  `mutation::apply_batch_with_version`, `destroy_thread_emails_if_unchanged`)
  read the cache, send the `/set` without `ifInState`, then
  `advance(expected = cached, new = response.newState)`. If another writer
  changed the account between the cached state and this set, the server's
  `newState` covers that change, and the cache jumps over it. The cache is
  then used as the `ifInState` guard for the next `FlagOp::Set` or move
  (`uses_if_in_state`), whose whole purpose is to refuse when an unobserved
  concurrent change happened. `SetResponse::old_state()` exists and is not
  read anywhere in `sync/`. The correct rule is "advance from `oldState` to
  `newState`, and only if the cache currently equals `oldState`".

**Shape.** A `StateCell` per `(Slot, ObjectType)` owning the rule:
`observe_set(old, new)`, `observe_changes(since, new)`, `observe_probe(s)`,
`guard() -> Option<IfInState>`. Fifteen call sites become calls that cannot
choose the wrong `expected`. Delete `mailbox_states`.

### A7. `ServerVersion::StateAt(String)` has no provenance

JMAP accepts a caller's `StateAt` as an `ifInState` for Email (flags, move,
destroy), ContactCard and CalendarEvent writes (`mutation::caller_state`).
The token is per `(accountId, type)` (the code says so in a comment), but the
type carries neither: inventory mints `StateAt(Email/get state)` for messages
and `StateAt(Mailbox/get state)` for mailboxes indistinguishably, and nothing
stops a consumer passing a Mailbox-inventory token, or a primary token for a
shared message, into `bulk_set_flags_if_version`. The result is an endless
`ConcurrencyConflict -> Retry(AfterStateRefresh)` for a token that can never
match. For contacts and events there is no read-side producer at all:
`ContactCard` and `CalendarEvent` in `bifrost-types` carry no version, so the
`contact_update` / `event_update` doors advertised under
`MutationConcurrency::ExpectedVersion` accept a token the consumer has no
door to obtain.

Cross-crate: every provider decides at write time which `ServerVersion`
variant it accepts (JMAP `caller_state`, Google's contact/calendar ETag
checks, Graph's ETag paths), with no read-side mint that names the door it is
valid for. Owner: `bifrost-types`, as a version minted by the read path with
its object type (and, for JMAP, owner) inside it.

### A8. Two page-cursor codecs, both tagged `2`

`page_cursor.rs` is the shared anchored, state-pinned cursor for contact and
calendar walks: `2:` + JSON `[anchor, queryState]`. Mail search has its own
in `pim.rs` (`encode_position` / `decode_position`): `2:<position>:<queryState>`,
owner-qualified with the object-id codec, position-based, with its own
envelope validation (`validate_search_page`) restating the same three
refusals `PageKind::next_cursor` enforces. `page_cursor.rs` argues at length
that position paging under a server-defined order skips and duplicates, and
names mail search as carrying "the same limitation". Both payloads start with
the byte `2`, so a cursor from one door handed to the other is distinguished
only by parse failure. Fold mail search into `PageKind` (with an owner
field), or give the two payloads distinct tags.

### A9. Refusals reach callers as prose

`error::unsupported_error(op, scope, message)` is the crate's universal
refusal, and the reason a caller would branch on lives only in the
`DiagnosticText`: submission absent, scheduled send window absent, foreign
scheduled send, non-folder container kind, label kind, `FlagOp` variant,
range transport hook, raw-MIME projection, legacy scope. `route_send_as`
encodes a four-way precedence whose outcome a caller can only tell apart by
reading text. These are mostly true `Unsupported`s, so the kind is right, but
a typed refusal reason (or at least distinct message keys) would let a
consumer tell "this account never sends" from "not scheduled, not for shares".
Lower priority than everything above.

---

## 2. Decisions made in more than one place

### D1. Cursor decode failure -> recovery class (cross-crate, already disagree)

Every account crate decodes its own `OpaqueChangeState` and maps the failure
to an `AccountError`. They disagree on the same two questions:

| Failure | JMAP (`changes::stream`) | Gmail (`InvalidCursor`) | Graph (`cursor_error_to_account_error`) | IMAP (`envelope.rs`) |
|---|---|---|---|---|
| wrong `protocol` tag | `CursorInvalid` (restart scope) | `SchemaIncompatible` (account clear) | `SchemaIncompatible` | `SchemaIncompatible` |
| malformed payload bytes | `SchemaIncompatible` | `CursorInvalid` | `SchemaIncompatible` | `SchemaIncompatible` |

JMAP and Gmail are exactly inverted, and each has a comment arguing its own
choice. The protocol-tag check is protocol-neutral (it compares
`OpaqueChangeState::protocol` with the account's own protocol), so it should
not be answered by four crates at all: `bifrost-types` (or the engine, which
knows `account.protocol()` before dispatch) should own it, together with a
shared `CursorDecodeFault { ProtocolMismatch, FutureVersion, RetiredVersion,
Malformed, ScopeMismatch }` -> `AccountError` mapping that every crate calls.
Within JMAP the mapping also lives in the wrong file: it is built inline in
`changes::stream` rather than in `error.rs`, outside the module that carries
`#![warn(dead_code)]`.

### D2. HTTP status -> error kind (JMAP vs bifrost-net, already disagree)

`error::convert_problem` classifies the HTTP status of a problem-details
response itself. `bifrost-net`'s `status_kind_cause` classifies the same
statuses for every other HTTP failure. They disagree:

- 5xx: JMAP maps every 500-599 to `Server(Unavailable)` (retryable); net maps
  only 502/503/504 to `Unavailable` and the rest to `Server(Error{status})`.
- 507: net maps to `QuotaExhausted` with a throttle scope; JMAP to
  `Unavailable` with none.
- 408: net maps to `Unavailable`; JMAP to `Server(Error{408})`.

So an identical 500 from the same server is retried or not depending on
whether it carried a `application/problem+json` body. The 77df77df precedent
applies directly: net should export the status classification, and JMAP
should override only the JMAP-typed problem types (`limit`,
`unknownCapability`, `notJSON`, `notRequest`).

### D3. `ErrorScope` -> `ResourceKind` (three copies, already disagree)

`resource_from_scope` / `id_from_scope` exist in `net/src/account_error.rs`,
`graph/src/account/graph_error.rs` and `jmap/src/sync/error.rs`. Net and
Graph map `ErrorScope::Event` to `ResourceKind::Event` and the two
`*Collection` scopes to `None`. JMAP has no `Event` arm (it falls through to
`_ => None`, the exact shape of the 2026-07-31 sweep findings) and maps
`CalendarCollection` / `ContactCollection` to `Calendar` / `Contact`. JMAP
never produces any calendar or contact scope today (see D5), so it is latent.
This is a property of `ErrorScope` and belongs on it in `bifrost-types`
(`ErrorScope::resource()`, `ErrorScope::object_id()`).

### D4. "The target of a bulk mutation is gone" (cross-crate, already disagree)

`error::classify_set_item` turns a JMAP `notFound` / `blobNotFound` into
`Succeeded(Skipped)` for `UpdateFlags`, `BulkMove`, `BulkDestroy`,
`SetKeyword`, `SetIsRead`, `AddToContainer`, `RemoveFromContainer`. Graph
absorbs a 404 as `Skipped` only for destroy. So a flag update or a move of a
vanished message is a success on JMAP and a `Failed(NotFound)` on Graph; a
move that moved nothing reported as success is questionable on its face.

Inside JMAP the list is also half dead: only `mutation.rs` calls
`classify_set_item`, with the three bulk operations. The four single-target
operations in the list never reach it; `pim.rs`'s single doors route the
same `notFound` through `SetProgress::item -> set_answer_error ->
into_account_error` with no item scope, which lands on `Protocol(Unknown)`
(`UnknownPermanent`). Same wire answer, same keyword patch: success via
`bulk_set_flags`, terminal unknown error via `set_keyword`. Owner: a per-
`AccountOperation` "absent target" policy in `bifrost-types`, applied by every
provider.

### D5. "A single-object read came back empty"

- `thread_hydrate`: `IdNotFound` with `ErrorScope::Thread` -> `NotFound(Thread)`.
- `message_hydrate`: `IdNotFound` with `ErrorScope::Message` -> `NotFound(Message)`.
- `open_raw_rfc822`: hand-built `NotFound(Message)`.
- `contacts::get`: `Unsupported(ContactGet)`, "JMAP contact was not found".
- `calendar_ops::get_raw_event` (used by `event_get`, `event_update`,
  `event_rsvp`): `Unsupported(op)`, "CalendarEvent/get returned no event".

The last two are **live defects**: a deleted contact or event reports the
operation as unsupported by the protocol, which is terminal capability
absence and tells a consumer keying on the kind to stop offering the door.
`get_raw_event` also takes the head of the list positionally rather than
correlating on the requested id, the pattern `blob.rs` and
`reconcile_hydration` were written to eliminate. One helper
(`require_one(response, requested_id, ErrorScope)`) should answer this for
every JMAP `/get` of one id.

### D6. "Does this event overlap this window" (JMAP vs CalDAV, already disagree)

`calendar_ops::event_in_range` and `caldav/src/account.rs`'s `event_in_range`
are the same local post-filter over server results. CalDAV's copy was fixed
(ruled dav-F9) for three cases JMAP's copy still gets wrong:

- a zero-length instant exactly at the window start (JMAP's strict
  `start < end && end > start` drops it);
- all-day floating dates (JMAP compares them as UTC midnight);
- recurring masters whose own interval precedes the window while an
  occurrence overlaps it (JMAP drops them; its `CalendarEvent/query` does not
  request `expandRecurrences`, so the server returns the master and the
  local filter then discards it).

**Live defect**: `events_in_range` silently omits recurring events whose
first occurrence is before the window. The rule is protocol-neutral and
belongs in `bifrost-types` beside `EventRange`. Related, softer: shared
`EventRecurrence` / attendee validation is decided per provider
(`validate_shared_recurrence` here, `validate_recurrence` in Graph), and
RRULE parsing/printing lives in this crate's `calendar_ops.rs`.

### D7. Capability flags vs door gates

`capabilities::build` derives `PimMethodSupport`'s sixty `bool`s from a
`PimSupport` struct of eight `bool`s; the doors independently gate on
`Option<MailAccount>` handles and on `max_delayed_send != 0`
(`cancel_scheduled_send`, `reschedule_send` restate
`scheduled_send: submission && max_delayed_send > 0`). The factory keeps the
two derived from one fact today (`resolve_optional_families`), and
`capability_contract_tests.rs` checks the agreement - but only in the false
direction, and its fixture builds `PimSupport` and the handles by hand, so a
factory change that let them drift would not be caught. That file is a
pairwise-agreement test over two copies, which is the finding shape the brief
names.

The copies already disagree in one place: `containers_list` returns shared
mailboxes with `ContainerRights` (`may_rename`, `may_delete`,
`may_create_child` from `myRights`), and `pim_methods.container_rename /
move / delete / create` are unconditionally `true`, but the container doors
run on `self.mail` with `MailboxId::new(container.0)` - no routing, no
stripping. **Live defect**: renaming, moving or deleting a shared folder
sends the qualified id to the primary account, which answers `notFound`, and
because the door attaches no scope that becomes `Protocol(Unknown)`
(`UnknownPermanent`). Creating a child under a shared parent fails the same
way.

Shape: a slot-owned capability set from which both the `PimMethodSupport`
flags and the door gates are read (`slot.require(Family::Submission)?`), so
there is one copy. With the transport generic (S2) the contract test can then
check both directions.

### D8. Object limits and batching bounds

The answer to "how many ids per call" is computed in five places:

- `capabilities::validate_session` (`unwrap_or(1).max(1)` after proving `> 0`);
- `capabilities::build` (`batching_policy.max_items = max_objects_in_set.clamp(1, 500)`,
  `max_wait = 100ms`);
- `mutation::mutation_stream` and `mutation::versioned_stream` (each
  `clamp(1, 500)` again);
- `factory::max_objects_in_get` / `factory::max_objects_in_set`, which re-read
  the session (with the same `unwrap_or(1)` fallback) for "doors that hold a
  bare Account" - used by `pim.rs` single doors and `filters::list`;
- `hydrate::ROUTE_MAX_WAIT`, a local constant whose comment says it duplicates
  the advertised `max_wait` because threading it was inconvenient.

The factory module is the wrong home for a per-request limit, and the reason
these doors "hold a bare Account" is that the account handle does not carry
its `CoreLimits`. In the registry shape the limits live on the `Slot` and
there is one reading.

### D9. Page cursor faults and lost anchors

- A malformed, retired or overflowing search / contact / calendar page cursor
  is `SyncState(SchemaIncompatible)` (`pim::schema_incompatible_search_cursor`,
  `page_cursor::cursor_error`). That kind derives
  `Engine(EngineDirective::SchemaIncompatible)` unconditionally - the
  account-wide durable-state clear - for a cursor the engine never holds.
  `error::search_result_set_superseded`'s own comment explains why
  `CursorInvalid` is wrong for page cursors (it is reserved for engine
  cursor scopes); the same reasoning rules out `SchemaIncompatible`. A
  page-cursor fault is a caller-state fault; it wants its own kind or
  `Request(InvalidArgument)`.
- `result_set_superseded` exists twice (`error.rs` for mail search,
  `PageKind` for contacts/calendars) with the same classification.
- `anchorNotFound` is decided twice: `convert_method` maps it by the
  `anchored_query` flag plus a cursor scope (inventory), and
  `PageKind::page_call_err` pre-intercepts it before `into_account_error` to
  map it to `ConcurrencyConflict` (page walks). The pre-intercept exists
  because the context type cannot express "anchored page walk" (A5).

### D10. Two `Email/set` pipelines with different policies

`mutation.rs` (bulk doors) and `pim.rs` (single-target doors, threads,
drafts, send) both build and send `Email/set` for the same patches
(keywords, `mailboxIds`, destroy). They answer three questions differently:

- **Guard**: `mutation::uses_if_in_state` guards `FlagOp::Set` and moves with
  the cached state and retries once on `stateMismatch`; `pim::send_email_set`
  never guards (its comment argues patches need no guard), and
  `destroy_thread_emails_if_unchanged` guards with a read state.
- **Absent target**: see D4.
- **Outcome vocabulary**: per-item `ItemOutcome` lanes versus a
  `PartialCommit` record built by `SetProgress`.

`SetProgress::finish` also names its targets with the **wire** id
(`ErrorScope::Message { id: EmailId::as_str() }`), which for a shared account
is the native, unqualified id - so a partial-commit record from
`set_keyword` / `move_thread` / `delete_thread` on a shared message names ids
the consumer never saw and cannot correlate with its own (live, P3: the
record is advisory, but it is the only place the consumer learns which
targets committed).

### D11. Scope support vs scope driving

Covered by A3; listed here because it is also a decision duplicate:
`describe_cursor_support` and `changes::stream` independently decide which
decoded scopes are drivable, and the codec decides a third answer. The
comment on `JmapScopeRepr::from_cursor_scope` accepts this as deliberate. The
`JmapScope` enum removes the need to accept it.

---

## 3. Structure

### S1. `JmapAccount` is state split from the behaviour acting on it

The struct has twenty-five fields and a twenty-one-argument positional
constructor (`#[allow(clippy::too_many_arguments)]`) that tests call with
`HashMap::new()` five times in a row. The per-JMAP-account facts are spread
across six maps keyed by bare `String` ids, built at open in `factory.rs`
(`SeededOpenState`, `apply_foreign_seed`) and then re-joined by every door.
Trait methods are mostly one-line forwards that pass
`(handle.clone(), Arc::clone(&self.email_states), handle.id_str().to_string())`
into a free function - about twenty-five such call sites in `account.rs`.
Nothing stops a door from pairing one handle with another account's state
key.

Recommendation: the `Registry` / `Slot` / `Route` design above. `Slot` owns
`handle`, `limits`, `email_state: StateCell`, `families` (submission,
vacation, ... per slot, which also makes shared-account submission an
ordinary slot bit instead of `foreign_submission: HashSet<String>`), and its
`JmapScope`. `PushRouting` becomes a view of the registry rather than a
second copy built from `seed_states`. Doors take `&Slot` or `Route` and
nothing else. The free-function decomposition then follows the slot, not the
transport pin (S2).

### S2. The production transport pin shapes the whole test layout

`JmapAccount` hardwires `MailAccount = Account<ReqwestTransport>` because
WebSocket push is reqwest-specific. As a direct consequence:

- `capability_contract_tests.rs` can only check the false direction (D7).
- `route_object_id`, `route_mutation_target`, `draft_route_account` are made
  generic over a handle type `H` "purely so they can be exercised".
- `foreign_account_id_for_object` exists only under `#[cfg(test)]` as a test
  pin for a rule production implements elsewhere (`route_object_id`) - a
  copy kept alive by its own test.
- The account methods are deliberately one-line delegations "to keep the pin
  meaningful", which is why logic lives in free functions taking six to
  eight parameters.

`push.rs` already introduced the seam the comment says is missing
(`PushTransport`, implemented for `Client`). Making `JmapAccount<T: HttpTransport>`
generic (with `Client<T>: PushTransport` as a bound, or push behind its own
injected `Arc<dyn>` seam) closes the residual and lets the whole-`Account`
tests replace the scaffolding. The comment estimates this as the right fix;
nothing here argues otherwise.

### S3. `pim.rs` does ten unrelated jobs

4,600 production lines (8,800 with tests) holding: single-target mail
mutations, the submission pipeline with partial-commit records, drafts and
attachment-handle codec, scheduled send, mail search with its own cursor
codec and envelope validation, `containers_list` plus RFC 9670 owner-email
resolution, identities, vacation, quota, thread and message hydration,
JMAP <-> shared `Message` mapping, and role-mailbox lookup. Natural seams:

- `mail_set.rs`: the one `Email/set` executor shared with `mutation.rs` (D10).
- `submission.rs`: send, draft send, schedule, cancel, reschedule, the
  partial-commit construction.
- `drafts.rs`: draft create/update/discard, body building, attachment handle.
- `search.rs`: search doors, moved onto `page_cursor.rs` (A8).
- `containers.rs`: list/create/rename/move/delete, owner emails, rights.
- `message.rs`: `thread_hydrate`, `message_hydrate`, `email_to_message`,
  alongside `hydrate.rs`.
- `settings.rs`: identities, vacation, quota.

`calendar_ops.rs` (2,200 production lines) mixes the JMAP door logic with
RRULE <-> JSCalendar recurrence conversion, iCal `UNTIL` handling, ISO 8601
duration parsing and time classification. The conversion half is a pure
mapping module and should be one; the parts that restate shared-vocabulary
rules (window overlap, recurrence validation) belong in `bifrost-types` (D6).

### S4. Error construction escapes the armed boundary

`error.rs` carries `#![warn(dead_code)]` precisely so that an unreachable
classification arm is a compiler warning. But `AccountError`s are also built
inline in `capabilities.rs` (four builders), `changes.rs` (the cursor decode
mapping, D1), `page_cursor.rs` (three), `pim.rs`
(`schema_incompatible_search_cursor`, `missing_shared_rights`,
`reschedule_*`, partial commits), `blob.rs` (`NotFound`, range
`InvalidArgument`), and `contacts.rs` / `calendar_ops.rs` (`unsupported`
wrappers). The duplicates in D5 and D9 are a direct product of that scatter.
Move every classification into `error.rs` (split it by family if size is the
concern) so the one armed module is the one place a class is chosen.

### S5. Lifecycle topology is two locks for one fact

`discover::scope_lifecycle` keeps the lifecycle position in
`lifecycle_mailbox_states` and the announced names in `mailbox_names`, two
separate `Arc<Mutex<..>>`s that must change together; the reseed path takes
both locks to publish atomically and the comment explains the drop hazard the
split creates. The normal path updates names per mailbox before the state
commit. Both belong in one `LifecycleTopology { position, names }` behind
one lock, owned by the lifecycle stream (nothing else reads either). The
factory also fetches mailbox names at open and `discover::memberships` then
issues a fresh `Mailbox/get` for ids only.

A related seam mismatch: discovery emits primary per-mailbox memberships and
the shared owner tags, but never the shared per-mailbox memberships
(`Folder("{acct}\u{1f}{mbx}")`) that inventory and hydration stamp on every
shared message, so the engine's membership index learns those only from
items. Lifecycle is primary-only by design (documented); the membership gap
is a consequence worth stating in the reference.

### S6. Cross-crate owners this scope points at

Collected from above, the items whose correct home is outside this crate:

- `bifrost-types`: protocol-tag check and cursor-fault mapping (D1),
  `ErrorScope::resource()` (D3), absent-target policy (D4), event window
  overlap and shared recurrence validation (D6), an owner membership variant
  (A2), provenance-carrying versions (A7).
- `bifrost-net`: HTTP status classification exported for protocol overrides
  (D2).

---

## 4. Defects found along the way

Live unless marked latent. Priorities are a reader's estimate.

1. **P1** `contacts::get` and `calendar_ops::get_raw_event` report a missing
   contact or event as `Unsupported(op)` (D5). Affects `contact_get`,
   `event_get`, `event_update` (context read), `event_rsvp`.
2. **P1** `events_in_range` drops recurring events whose master interval
   precedes the window, zero-length events at the window start, and
   misplaces all-day events (D6).
3. **P2** Container rename/move/delete/create on a shared mailbox are
   unroutable and fail as `Protocol(Unknown)`, while `containers_list`
   advertises the rights to do them (D7).
4. **P2** `filters::update` with `is_active: Some(false)` sends
   `onSuccessDeactivateScript: true`, which deactivates whichever script is
   active - including a different filter than the one being updated. A
   caller marking an already-inactive filter B as inactive switches off the
   active filter A.
5. **P2 latent** State-cache CAS advances from the cached state instead of
   `response.oldState`, so unguarded writes can launder a concurrent change
   into the value later used as an `ifInState` guard (A6).
6. **P2** Page-cursor faults derive the account-wide schema-clear engine
   directive (D9).
7. **P2** Same `notFound` on a keyword patch is `Skipped` success through
   `bulk_set_flags` and `UnknownPermanent` through `set_keyword` (D4).
8. **P3** Partial-commit records name shared-account targets by native wire
   id (D10).
9. **P3** JMAP problem-details 5xx/507/408 classification diverges from
   bifrost-net (D2).
10. **latent** Owner tag `MembershipScope::Mailbox(accountId)` shares a
    namespace with primary mailbox ids (A2).
11. **latent** `pim::wire_id_for_mail` strips a primary-qualified id where the
    other three wire functions keep it literal (A1); no separator validation
    on server-minted native ids.
12. **latent** JMAP's `resource_from_scope` drops `ErrorScope::Event` to
    `None` (D3).
13. **minor** `get_raw_event` trusts the head of the `/get` list without id
    correlation (D5).
14. **minor** `blob::open_range` returns `Request(Malformed)` for an
    out-of-range start before it returns the `Unsupported` that
    `BlobRangeSupport::No` promises; ordering the capability refusal first
    makes the answer independent of the argument.
15. **minor** `seed_states` values, `mailbox_states`, and
    `JmapErrorContext::provider` are dead state (A3, A6, A5).
16. **minor** `push::data_type_for_scope` accepts any `CursorScope::Folder`,
    including garbage and departed shares, as an Email subscription (A3).
