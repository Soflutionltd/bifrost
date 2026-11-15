# Hunt: bifrost-graph - wrong shape, wrong place

Scope: `crates/graph/` (client, origin, paging, error, webhooks, `ews/`,
`account/`). There is no `crates/graph/tests/` directory; every test is an
in-crate module. Cross-crate comparisons against imap, jmap, google, dav-core,
net and types are made where the same question is answered there.

Findings are split into the three questions. Each names the question, every
site answering it, whether they already disagree, and where the owner should
live. Live and latent defects found on the way are marked **[defect]** and
collected again at the end, separated from the proposals.

---

## 1. Axes that should be types

### 1.1 `GraphCursorPayload` is a product type encoding a sum **(highest value)**

`account/cursor.rs`. The payload carries `kind`, `delta_link: String`,
`advanced_through: Option<GraphPageMarker>`, `inventory_in_progress: bool` and
`calendar_window_end: Option<i64>` side by side. The real states are three:

- a live delta cursor (delta link, optional mid-walk marker);
- an inventory page position (marker, no delta link);
- a public-folder poll state (none of the delta fields).

Everything else is constructible and has to be refused somewhere else:

- `delta_link` is `String::new()` for two of the three states, and
  `resume_url()` returns `""` for a public folder. Tests pin these illegal
  states as intended (`an_empty_delta_link_survives_the_round_trip`,
  `public_folder_kind_ignores_the_envelope_delta_fields`).
- `CursorError::InventoryInProgress` exists only to name two states a sum
  could not express: `inventory_in_progress == true` with no marker
  (`inventory_stream_from`), and an inventory position handed to
  `changes_stream`.
- `calendar_window_end` is meaningful only for `GraphCursorKind::Events`;
  `calendar_window_needs_reseed` has to re-check the kind.
- The folder id is stored twice, once in `ChangeCursor::scope` and once in
  `GraphCursorKind`. The agreement check lives one layer up in
  `inventory::scope_matches_payload` (and its `event_aliases` escape hatch),
  and `encode_does_not_cross_check_the_scope_against_the_payload_kind` pins
  that the codec does not check it.

Proposed shape:

```rust
enum GraphCursorState {
    Delta { link: AdmittedLink, marker: Option<GraphPageMarker>, window: Option<CalendarWindow> },
    InventoryPage { marker: GraphPageMarker, window: Option<CalendarWindow> },
    PublicFolder(PublicFolderCursor),
}
```

The collection would be derived from `ChangeCursor::scope` at decode time
instead of being persisted beside it, and `CalendarWindow` would exist only on
event scopes. `InventoryInProgress`, `scope_matches_payload` and the
empty-string tests would all go. This is an inner-envelope bump (v6), which the
crate already treats as a reseed boundary.

### 1.2 Push configuration is `PushMode` plus `Option<PushEndpoint>` **[defect]**

`GraphAccountFactory::new` defaults to `PushMode::GraphSubscriptions` with
`push_endpoint: None`. The three decisions that read this pair each read a
different part of it:

- `capabilities::build_capabilities` reads `push_mode` only, and advertises
  `PushCapability::WebhookOrEwsStream`;
- `push_stream::push_lanes` reads mode and endpoint, and declares no lane;
- `webhook::subscribe_graph` reads the endpoint, and returns
  `Unsupported(PushSubscribe)`.

A default-constructed Graph account therefore tells the consumer that webhook
CRUD lives on the account, has no lane, and refuses every subscribe. The owner
is one value, `enum PushConfig { None, Webhook(PushEndpoint), EwsStreaming }`.
The capability, the lanes and the subscribe arm should all be functions of it,
and `None` should map to `PushCapability::None`. The public builders
(`with_push_endpoint_client_state`, `with_ews_streaming`) keep their names.

Related naming smell in types: `PushCapability::WebhookOrEwsStream` is
documented as Graph webhooks only, and Graph's EWS streaming maps to
`InProcess`. The "OrEwsStream" in the published name is false. Renaming it is a
published-item rename, so it needs the owner's ruling.

### 1.3 Mailbox identity travels as a raw `String` with four equality rules **[defect, latent]**

The questions "which mailbox is this" and "are these two the same mailbox" are
answered by:

- `GraphAccount::shared_clients: HashMap<String, GraphClient>` and
  `client_for_owner`: exact byte equality;
- `autodiscover::merge_shared_mailboxes`: exact equality;
- `GraphClient::concurrency_key`: `eq_ignore_ascii_case` against the primary
  address, then Unicode `to_lowercase`;
- `GraphClient::mailbox_budget`: Unicode `to_lowercase`.

They already disagree. If `with_shared_mailbox("Shared@contoso.com")` is
configured and delegate Autodiscover returns `shared@contoso.com`, the account
gets two shared clients, two complete sets of discovered folder scopes for one
mailbox (duplicate sync, duplicate ids that differ only in owner-tag case), and
one throttle budget for both.

There are two more axes in the same state:

- `ClientInner::mailbox_id: Option<String>`, where `None` means `/me`. This is
  `enum MailboxRoute { Primary, Shared(MailboxKey) }`.
- `primary_account` is first-write-wins (`fix_primary_account`), while
  `mailbox_budget`'s fallback reads `account_net().account()`, which `retag`
  changes. After a retag on the `with_account_net` path, the concurrency key
  and the throttle budget name different accounts.

The owner is a `MailboxKey` newtype with case-insensitive `Eq`/`Hash`, minted
once at configuration and discovery. A single `MailboxIdentity` computed per
client should yield both the concurrency key and the throttle budget, replacing
the two independent string builders in `client.rs`.

### 1.4 ETags and change keys are both `String`

`inventory::graph_etag` encodes a rule that is enforced only by comment: a bare
`changeKey` must never go into `If-Match`, it must be wrapped as `W/"..."`
(`graph_change_key_etag`). ETags travel as `String` through `EtagIndex`,
`MessagePatch::etag`, `patch_if_match(etag: &str)` and
`BatchRequestItem::headers["If-Match"]`, so a change key can be passed where an
etag belongs and the type system will not notice. A `GraphETag` newtype whose
only constructors are `from_odata_annotation` and `from_change_key` would make
the bare form unsendable. `bifrost_types::ServerVersion::ETag` already exists
on the boundary.

### 1.5 `$batch` legs are stringly typed, so their replay policy is re-derived by parsing

`types::BatchRequestItem { method: String, headers: Option<HashMap<String,
String>> }`. The replay decision for a leg is made after construction by
`graph_error::subrequest_replayable`, which re-parses the method string,
re-parses every header, and re-derives "a PATCH is
`IdempotentUnlessConditional`". The policy of the outer `$batch` POST is
written by hand at each submission site: `get::fetch_batch` uses `Idempotent`,
while `mutate::submit_batch_with_version`, `pim::messages::submit_write_batch_resource`
and `reactions` each choose their own. Nothing ties the outer policy to the
legs it carries.

On the response side, `BatchResponseItem { status: u16, headers:
Option<HashMap<String,String>>, body: Option<Value> }` is re-converted at every
consumer:

- headers are rebuilt into a `HeaderMap` in four places (`get::reconstruct_headers`,
  inline in `mutate::reconcile_mutation_responses`, inline in
  `submit_write_batch_resource`, and `reactions::header_map`);
- the body is re-serialized to bytes in three of them;
- the status goes through `StatusCode::from_u16(status).unwrap_or(INTERNAL_SERVER_ERROR)`
  in two (`mutation_item_outcome`, `reconcile_hydration_responses`). **[defect, latent]**
  A subresponse status outside 100..=999 is therefore silently classified as a
  retryable 500 `Server(Unavailable)` rather than as the contract violation it is.

The owner is typed legs and typed responses, covered in 3.3:

```rust
struct BatchLeg { method: Method, url: AdmittedPath, precondition: Option<GraphETag>, replay: ReplayPolicy, absent: AbsentMeans }
struct BatchAnswer { status: StatusCode, headers: HeaderMap, body: Bytes }
```

Each leg's `replay` is declared once, where the leg is built. The outer policy
is the meet of the legs' policies, computed and never written by hand.

### 1.6 `destroy: bool` carries two meanings

`mutation_item_outcome(.., destroy: bool, ..)` and
`submit_write_batch_resource(.., destroy: bool, ..)` use the bool for two
things:

- "a 404 on this leg is success (`Skipped`)";
- "evict the target's cached etag on success".

`pim::messages::delete_extended_property` passes `true` purely for the 404
absorption, and its comment explains that it passes no targets so that the
second meaning does not fire. That is one bool forced to carry two independent
flags. The owner is `AbsentMeans { Done, NotFound }` on the leg (see 1.5),
with etag eviction derived from the leg's effect on the message resource.

### 1.7 Funnel parameters that are not types

- `GraphClient::execute_wire(method: &str, .., options: (Option<&str>, bool), ..)`
  ends in `unreachable!` for an unknown verb. The bare tuple means
  `(prefer, eventual_consistency)`. Use `reqwest::Method` and a small
  `RequestOptions { prefer, consistency }`.
- `execute_aux` takes a `Resource` only to `debug_assert!` that it is
  `Unmetered`. The parameter carries no information. Drop it or make the aux
  funnel's resource implicit.
- `throttle_scope_for(_ctx)` ignores its argument and returns a constant.

### 1.8 Subscription expiry is a `String` parsed on every tick

`GraphSubscriptionState::expires_at: String` is re-parsed by
`webhooks::check_expiry` on every 10-minute pass. `ExpiryCheck::Unparseable`
and the `warned_unparseable_expiry` latch on the state both exist only because
the parse is deferred. The owner is to parse at receipt (`create_subscription`
and `renew_subscription` return a `jiff::Timestamp`), with an unparseable
answer classified once, as the provider contract violation it is.

### 1.9 The Graph flag vocabulary is string literals with two alias tables

The tokens `"\\seen"`, `"\\flagged"` and the case-sensitive `"category:"`
prefix are spelled out independently in:

- `mutate` (`graph_flag_field`, `categories_from_flags`, `graph_flag_target`);
- `get::flags_from_value` and `get::ews_flags`;
- `inventory::tracked_flags_hash`, which uses a different pseudo-flag spelling
  (`isread=`, `flag=`);
- `pim::messages`.

`graph_flag_field` is documented as "the single alias table", but
`pim::messages::is_starred_category` is a second alias table
(`$flagged`, `\flagged`, `flagged`, `starred`) that differs from the first: it
adds `$flagged` and does not include `\seen`/`read`. The owner is a
`GraphFlag { Read, Flagged, Category(String) }` with `parse(&str)` and
`token()`, used by both the write projection and the read projection. The
starred-category sentinel would then become one variant of the same parse.

### 1.10 Cross-crate: the recurrence rule is a `String` that three crates each parse

`bifrost_types::EventRecurrence { rrule: Option<String>, rdate: Vec<String>,
exdate: Vec<String>, .. }`. Graph (`calendar::graph_recurrence_rule`,
`ParsedRRule`, `RRULE_MAPPED_PARTS`/`RRULE_UNMAPPED_PARTS`), JMAP
(`calendar_ops::jmap_recurrence_rule_from_rrule`, `validate_shared_recurrence`)
and CalDAV (`rrule_until`) each parse the string and each decide which RRULE
parts are valid and supported. Validity is a protocol-neutral question (RFC
5545 grammar). Expressibility is per provider. The owner of the first is
bifrost-types (`RecurrenceRule` parsed at the boundary, with `UNTIL`/`COUNT`
and `BYDAY` typed); each provider would then answer only "can I express this
parsed rule". Today a rule JMAP accepts and Graph refuses is refused for
different stated reasons, and nothing compares the two parsers.

### 1.11 Cross-crate: `ObjectType::Event` and `ObjectType::CalendarEvent` name one thing

Graph mints `Event` (`cursor::scope_for_kind`). CalDAV and IMAP-composed
calendars mint `CalendarEvent`. `types::account_compose` routes only
`CursorScope::Type(ObjectType::CalendarEvent)` to the composed calendar
delegate. Google matches both. Graph carries local alias machinery for the
duplication: `inventory::event_aliases`, the
`scope_for_kind_collapses_calendar_event_onto_event` test, and the renewal
session's "ambiguous case - `Event` and `CalendarEvent` scopes over one
calendar" branch. Nothing in types says how the two variants differ. Either
document a real difference or collapse them. Collapsing is a published
removal, so it needs the owner's ruling.

### 1.12 Cross-crate: error kind and cause are two copies checked pairwise at runtime

`AccountErrorBuilder::new(kind, cause)` takes both, and
`recovery::kind_matches_cause` verifies at `try_build` that they agree. Every
translation boundary ends in `.expect("valid account error classification")`.
Graph's `classify`, `classify_by_status`, `soap_fault_to_account_error` and
`autodiscover_error` write about forty literal pairs such as
`(AccountErrorKind::NotFound(r), Cause::Request(RequestCause::NotFound { what: r, .. }))`.

The pairwise check also has holes. `access_kind_matches_cause` matches
`MailboxUnavailable { .. }` against `MailboxUnavailable { .. }`, so a
`Transient` kind paired with a `Permanent` cause builds cleanly, and
`server_kind_matches_cause` accepts any status when one side is `None`. The
kind is a function of the cause in every arm except `Protocol(_)` with
`Wire(_)`. The owner is in types: `AccountErrorBuilder::from_cause(cause)`
derives the kind, and a separate constructor takes the protocol sub-kind for
the `Wire` case.

Graph adds a third parallel copy. Its classifiers return
`(AccountErrorKind, Cause, bool)`, where the bool means "this is a throttle".
That bool is also a function of the kind, and `stamp_budget` re-derives it from
the kind independently (see 2.1).

### 1.13 Caller page cursors: five formats, no tag

The cursor formats are:

- bare link bytes: `contacts::link_cursor`, `paging::decode_link_cursor`;
- `PagedCursor { url, skip, mode }` JSON: `paging::encode_paged_cursor` and
  `encode_directory_cursor`;
- `b"\x1fsearchapi:" + offset`: calendar search;
- the `Position` structs in `contacts/public.rs` and `calendar_public.rs`;
- `SearchPosition` in `calendar_public.rs`.

`decode_paged_cursor` falls back to reading a non-JSON input as a link cursor,
so a cursor minted by one surface decodes on another. `DirectoryMode` lives in
`paging.rs` although only directory search uses it, and `encode_paged_cursor`
always stamps `DirectoryMode::Filter`. The owner is one
`GraphPageCursor { surface, .. }` enum with a single versioned codec and a
surface tag that is checked on decode, which turns a cross-surface cursor into
`Request(Malformed)`.

### 1.14 `TokenSource` has no audience **[defect, probable live]**

`bifrost_net::TokenSource::current()` takes no resource or host.
`public_folder::ews_client` builds the EWS client over the Graph client's own
`AccountNet`, and Autodiscover also posts through `execute_aux` with
`AuxTarget::Bearer` on that same net. Both therefore present the Graph access
token, whose audience is `https://graph.microsoft.com`, to
`outlook.office365.com`, which needs an Outlook/EWS-audience token. Azure AD
issues one audience per access token, and the trait gives a consumer no way to
answer differently per host.

Unless something outside this crate compensates, EWS streaming push,
public-folder sync, public contacts and calendars, and delegate Autodiscover
cannot authenticate in production. Hermetic tests cannot see this: the scripted
wire accepts any bearer.

The axis belongs in net. Either `AccountSpec` carries a token source per
trusted origin, or `TokenSource::current(&Audience)`. Graph would then
construct its Outlook-origin requests with the Outlook audience.

### 1.15 Outlook origin derivation is a host comparison, not a cloud

`client::derive_outlook_base` maps `graph.microsoft.com` to
`outlook.office365.com`, and maps every other host to its own origin.
Sovereign Graph hosts (`graph.microsoft.us`, `dod-graph.microsoft.us`,
`microsoftgraph.chinacloudapi.cn`) therefore derive an EWS and Autodiscover
origin on the Graph host, which is functionally wrong. An explicit
`with_outlook_base` is the only escape. The axis is
`enum MicrosoftCloud { Global, UsGov, UsGovDoD, China, Custom { graph, outlook } }`,
which owns both bases and (with 1.14) both audiences.

### 1.16 Delta items and hydration bodies are untyped `serde_json::Value`

`inventory_entry_from_value`, `membership_from_value`, `flags_hash`,
`get::hydrated_from_value`, `validate_flags_value` and the changes loop all
walk raw `Value` with `.get("...")`. `validate_flags_value` exists to recover,
by hand, the completeness a typed `struct DeltaMessage { id: Option<String>,
is_read: bool, flag: Flag, categories: Vec<String>, .. }` deserialization would
enforce. The same applies to the `@removed` tombstone shape.

---

## 2. Decisions made in more than one place

### 2.1 "Is this status a throttle?" - three answers, already disagreeing on 503

The sites are:

- `GraphClient::tag_throttle` treats 429, 503 and 507 as throttle-shaped and
  wraps the failure in `MailboxThrottled { budget }`;
- `graph_error::classify_by_status` returns throttle `true` for 429 and 507,
  and `false` for 503 (`Server(Unavailable)`);
- `stamp_budget`, on the net-classified path, decides from the kind
  (`RateLimited | QuotaExhausted`).

A 503 is therefore tagged with a mailbox budget at the funnel and then never
stamped, because the classifier says it is not a throttle. The comment in
`response_to_account_error` claims the annotation "applies only on rate-limit /
quota / 503 paths", so the comment and the code disagree as well. The owner is
the classifier: the funnel should carry the budget unconditionally (as context,
not as a verdict), and the throttle decision should be made once, from the kind.

### 2.2 Cross-crate: "a permission denial on a foreign scope becomes ScopeRevoked" - four copies

The copies are:

- `graph_error::graph_shared_scope_error` (REST);
- `graph_error::ews_shared_scope_error` (EWS);
- `jmap::sync::error::shared_scope_error`;
- `imap::account::error::shared_folder_error`.

All four translate the error, test `kind == Authorization(PermissionDenied)`,
and replace the result with a freshly built `SyncState(ScopeRevoked)`. All four
discard the original error's evidence when they do so: status, request id,
trace id, attempt cause and wire signal. A quarantined share therefore exports
no request id to support.

A fifth, different answer to the same question sits in
`scopes::discover_cursor_scopes_inner`. There, a foreign mailbox's
`PermissionDenied` during discovery becomes a skipped scope plus a
`Warning(OperatorAttentionNeeded)`, and every other error fails the whole
discovery, a primary-mailbox outage included.

The rule is protocol-neutral, so its owner is bifrost-types:
`AccountError::revoke_foreign_scope(self, scope, owner) -> AccountError`. It
should keep the original chain as the revoked error's cause, so the evidence
survives the reclassification.

### 2.3 `$batch` response reconciliation - four reconcilers that disagree **[defect]**

The question "given the submitted legs and the subresponses, what is each
leg's outcome, and what does the etag cache learn?" is answered by:

1. `mutate::resolve_batch_responses`, `reconcile_mutation_responses` and
   `update_etag_cache_from_responses` (bulk mutation);
2. `get::reconcile_hydration_responses` (hydration). This repeats the id
   resolution verbatim, including both warn strings;
3. `pim::messages::submit_write_batch_resource` (PIM writes);
4. `reactions::classify_chunk`.

They disagree on invalid input. Reconcilers 1 and 2 discard a subresponse
whose id is unparseable or out of range. Reconciler 3 dedupes by raw string,
builds an outcome for an unparseable id scoped `ErrorScope::Account`, and lets
an out-of-range numeric index into `BatchProgress::failed`.

They also disagree on etag maintenance. Reconciler 1 evicts on 412 and 404,
evicts on move or destroy success, and installs the new etag on any other 2xx.
Reconciler 3 evicts only on destroy success.

The consequence: `set_is_read`, `set_category`, `set_importance` and
`set_extended_property` call `fetch_message_value`, which caches the
pre-write etag. They then PATCH under `If-Match`, the PATCH succeeds and moves
the change key, and nothing refreshes or evicts the cached entry. The next
`bulk_set_flags` on that message finds the stale etag in `EtagIndex`, sends it,
and gets a 412 `ConcurrencyConflict` against its own earlier write. That is a
guaranteed wasted round trip, and a spurious conflict reported to the engine.

The owner is one `BatchAnswers::resolve(&legs, responses)`: id resolution,
header decoding, the absent-leg rule, and the etag observation applied in one
place. The four front ends would only project outcomes.

### 2.4 Two mail write pipelines, each re-deciding the same rules

`mutate.rs` (the engine's bulk doors) and `pim/messages.rs` (the typed doors)
each build If-Match PATCH, move and delete legs, submit `$batch`, and
reconcile. The rules decided twice:

- **Cross-mailbox move refusal.** `request_for_mutation` returns `None` for it
  and the caller files `invalid_item_error`; `move_messages` checks it
  explicitly. The comment in `move_messages` says "exactly as the bulk-move path
  in `mutate.rs` refuses the same condition", which is a pairwise agreement held
  by hand.
- **Public-folder write refusal.** `mutate` partitions with `ews_read_folder`
  plus `is_known_public_folder` on the destination. `pim` uses
  `refuse_public_write`, called per id in `resolve_target_ids`, again in
  `move_messages`, and again in `message_batch_url`. On the `add_to_container`
  path, the move-into-public-folder check runs twice (`add_to_container`, then
  `move_messages`).
- **"Move carries the observed etag as a legacy guard."** Both pipelines
  decide this.
- **Etag preflight.** `refresh_missing_etags` in mutate, and
  `fetch_message_value` and `message_values_for_ids` in pim.

The owner is one write planner, `plan_message_write(target, effect) ->
Result<BatchLeg, ItemRefusal>`, used by both doors. The pipelines would then
differ only in how they report (streaming `ItemOutcome` vs `PartialCommit`).

### 2.5 "What is an unrouted public-folder item?" - three answers **[defect]**

An `ObjectId` of the public-item shape whose folder is not in `routing_map`
(the map starts empty on each reopen, and discovery may skip a folder) gets a
different answer from each lane:

- `get::get_stream`, through `partition_ews_ids`, sends it down the REST arm,
  where Graph answers 404 and the item is reported `NotFound(Message)`. This is
  pinned as deliberate by `an_unrouted_public_folder_id_hydrates_through_the_rest_arm_url`.
- `pim::hydrate::public_message_hydrate` and `get::fetch_ews_outcomes` answer
  `stale_local_state_error`, which is `ConcurrencyConflict` and
  `Retry(AfterStateRefresh)`. The branch in `fetch_ews_outcomes` is
  unreachable, because `partition_ews_ids` already diverted every unrouted id to
  REST.
- `mutate::submit_batch_with_version` answers `Unsupported`. Its comment says
  that sending such an id to REST produces "false claims that the item is
  gone", which is exactly what `get_stream` does on purpose.

`get_stream` is the engine's read-back path, so a read-back of an unrouted
public item reports it deleted.

The underlying question, "is this a public folder?", is itself answered four
ways:

- presence in `routing_map`: `establish_initial_cursor`, `inventory_stream`,
  `partition_ews_ids`;
- the decoded cursor kind: `changes_stream`;
- the bare `CursorScope::Folder` variant: `budget_for_scope`, `push_subscribe`;
- `routing_map` or `public_folder_meta`: `is_known_public_folder`.

The owner is a typed internal scope (see 2.6) whose `PublicFolder` variant is
produced once from discovery state, plus one `PublicItemRoute::resolve(id) ->
Routed | Unrouted` that every lane matches on, with a single classification
for `Unrouted`.

### 2.6 Scope to Graph collection - eight matches on `CursorScope`

These functions each match `CursorScope::FolderType { folder, ty }` to decide
what the scope means:

- `inventory::initial_delta_request` (delta path);
- `webhook::resource_for_scope` (subscription resource);
- `push::ews::ews_subscribable_folder_id` (EWS subscribe id);
- `inventory::sync_object_id` (object id minting);
- `inventory::sync_scope_readable` (shared non-mail refusal);
- `cursor::kind_for_scope`;
- `inventory::delta_prefer`;
- `inventory::is_mail_scope` (etag caching).

They have drifted before: `resource_for_scope`'s comment records the events arm
subscribing to the default calendar for every calendar scope. They also differ
on foreign scopes today: webhook accepts them, EWS rejects them, and
`sync_scope_readable` rejects shared non-mail.

The owner is one parse, run at each `Account` entry:

```rust
enum GraphScope {
    Mail { mailbox: MailboxRoute, folder: NativeFolderId },
    Events { calendar: NativeCalendarId },
    Contacts { folder: NativeFolderId },
    PublicFolder(FolderId),
}
```

Its methods would be `delta_path()`, `subscription_resource()`,
`ews_folder_id()`, `mint_object_id()`, `client()` and `etag_cached()`. A scope
that cannot parse would be refused once, with one classification.

### 2.7 "What does a delta value without an id mean?" - two answers

On the same server input:

- `inventory_stream_from` records an `InventoryObligation::Region` checkpoint
  barrier and continues the walk. The obligation's error is
  `unsupported_account_error(SyncInventory)`, which claims the operation is
  unsupported. **[defect, minor]** That is a misclassification; it should be
  the contract violation it is.
- `changes::changes_stream` terminates the scope with a terminal
  `Protocol(ContractViolation)`.

The comment in `changes_stream` explains that the changes lane has no
obligation vocabulary, which is a fact about `SyncEvent<Change>`, not a reason
for the two lanes to reach different verdicts on the provider. The owner is
the shared delta-walk driver (3.2), which decides once and lets each lane
express the verdict in its own vocabulary.

### 2.8 Cross-crate: pagination guard - five copies, opposite orders

The copies are Graph's `paging::PageWalk`, and in Google
`changes::walk_refusal`, `inventory::inventory_walk_refusal`,
`contacts::address_book_walk_refusal` and the inline loop in
`calendar::calendars_list`. All use a 10,000-page budget, a repeat as
contract violation, and the budget as `LimitExceeded`.

They already disagree, and both sides call their choice deliberate:

- Graph checks the budget FIRST (`the_budget_is_checked_before_the_repeat`:
  "the classification does not depend on which link the server happened to hand
  out last");
- Google checks the repeat FIRST (`walk_refusal`: "a repeated token is the
  sharper diagnosis").

The `PageWalk` module doc cites the Google fix as the reason for its own
design. The decision is protocol-neutral and pure, so it can live in
bifrost-types (or bifrost-net) as one `PaginationGuard<K: Hash + Eq>`, with one
ruled order.

### 2.9 Cross-crate: the inner cursor envelope gate is per crate

Every account crate re-implements "protocol tag matches, inner version
acceptable, map a mismatch to `SchemaIncompatible`":

- Graph (`cursor::decode_cursor`) splits newer (`EnvelopeUnknown`) from older
  (`SchemaIncompatible`), and then maps both to `SchemaIncompatible` anyway;
- IMAP (`envelope.rs`) and CalDAV and CardDAV (`account.rs`) use strict
  equality.

Types already owns the outer gate, `validate_envelope`. The owner of the inner
gate is `OpaqueChangeState::open(expected_protocol, expected_version) ->
Result<&[u8], InnerEnvelopeMismatch>`, together with the one mapping onto
`AccountError`.

### 2.10 Cross-crate: "may this URL carry the account credential?"

net attaches the bearer to the first request whatever its host (stated in
`origin.rs`'s module doc), and leaves the admission decision to each crate:

- Graph (`origin.rs`) uses an exact-origin `AdmittedUrl` type and refuses
  everything off the base;
- dav-core (`DavDispatch::admit_discovered_urls` / `is_trusted_url`) uses a
  string allowlist consulted per request. It admits a cross-origin HTTPS
  discovered home, and `resolve_url` decides "absolute" by the case-sensitive
  literal prefixes `http://` and `https://`;
- JMAP relies on the redirect policy's `same_origin()`.

The transport is the layer that attaches the credential, so it should own the
admission. `AccountSpec` would carry trusted origins, bearer-carrying requests
would take an `AdmittedUrl` minted by net, and each crate would contribute only
its policy for widening the set (DAV's discovered homes, Graph's upload
sessions).

### 2.11 Cross-crate: Failed vs Uncertain after a whole-submission failure

The question is whether an item whose submission failed may have landed.

- Graph's `mutate::append_batch_failure` scans the cause chain for an
  `InFlight` attempt, or a `GraphError::Json` (the latter is redundant with
  `AttemptCause::unreadable_success`).
- Google (`mutation.rs`, the `acknowledged_bodies`/`in_flight` match) and IMAP
  (`mutate.rs`, `Verdict::Uncertain`) each have their own verdict.
- `RecoveryClass::requires_reconciliation` already answers the question
  centrally.

The owner is in types: `ItemOutcome::from_submission_error(item, error)`,
which picks the lane from the error's own transmission evidence.

### 2.12 Idempotency is declared twice per request, held together by one test

A Graph request's replay safety is declared by the `ReplayPolicy` at the call
site. Its recovery idempotency is declared separately by
`GraphErrorContext::idempotent()`, and `apply_idempotency_override` re-applies
that after net translates. The doc on `idempotent()` admits the coupling: a
widening context would overwrite a narrowing net stamped, and the coupling is
guarded only by `translate_exchange_ids_is_declared_idempotent`. That is two
copies of one fact, held in step by a pairwise test.

The owner is the declaration. net already carries the declared policy into the
error (`may_have_committed`), so a `ReplayPolicy::Idempotent` request could
widen recovery itself, and no classification-site override would be needed.

### 2.13 Capability mirror held in step by a pairwise test (workspace-wide)

`capabilities::build_capabilities` hand-writes about sixty
`PimMethodSupport` bools that mirror which `Account` methods refuse.
`capability_contract_tests.rs` exists to catch the two drifting, and only in
the false direction, as its module doc says. It caught a live violation on the
day it was written. This is exactly the "copies plus agreement test" shape. The
types reference records why a supertrait split was rejected. That reasoning
covers the trait shape, not the mirror: a per-method
`fn supports(&self, op: AccountOperation) -> bool` derived from the same match
that the refusing default dispatches through would make the flag and the
refusal one decision. It is worth re-opening at the types level, since every
account crate carries the same mirror.

### 2.14 Cross-crate: wire-code tables split across two crates

`error::classify_code` (string to `GraphSignal`) lives in graph, and
`GraphSignal::code` (variant to string) lives in types. The two directions of
one bijection are maintained by hand in two crates, and the
`syncStateNotFound` alias exists in only one direction. JMAP and Gmail have the
same split (`JmapMethod`, `GmailSignal`). The owner is types:
`GraphSignal::from_code` beside `code()`, plus a round-trip test over every
variant.

### 2.15 Mailbox-qualified id spelling - one codec, three spellings

`foreign.rs` says `qualify_with_owner` keeps every id namespace on one
separator, but there are others:

- `pim::send::SCHEDULED_SEND_HANDLE_SEP` is a second `'\u{1f}'` constant, with
  its own encode and decode;
- `pim::containers` recognizes the literal `"{owner}\u{1f}deletedItems"`;
- `foreign::parse_public_address_book` and `parse_public_calendar` hard-code
  the strings `"public-contacts\u{1e}"` and `"public-calendar\u{1e}"`, while the
  encoders build the same prefixes from `PUBLIC_SEP`;
- `calendar.rs` adds a fourth separator, `"::"`, for event ids, and
  `sync_object_id` relies on `ObjectId` and `EventId` sharing that string form
  by convention.

`send::client_for_scheduled_send_handle` also re-implements
`GraphAccount::client_for_owner` with a different error class for the same
missing mailbox (`send_as_unknown_mailbox` vs `GraphError::Configuration`).

**[defect, latent]** `parse_public_contact` and `parse_public_event` fall back
to `(tail, "")` when the item part is missing. A truncated id therefore parses
as a request for an empty item id, instead of being refused as caller input.

The owner is one typed id codec,
`GraphId::{Message, Thread, Folder, PublicItem, PublicContact, PublicEvent, Event, ScheduledSend}`,
with one `parse` that refuses partial forms.

### 2.16 Etag cache writes are decided at five sites

`EtagIndex` is written by:

- `inventory_stream_from` (mail only; evicted on `@removed`);
- `changes_stream` (the same rules, re-written);
- `get::fetch_batch` (every successful hydration body);
- `mutate::update_etag_cache_from_responses` (the 412/404/move/destroy rules);
- `pim::messages::cache_etag_for` and the destroy eviction in
  `submit_write_batch_resource`.

Each restates what an observation means for the cache, and 2.3 is the case
where two of them disagree. The owner is `EtagIndex::observe(id, Observation)`,
with `Observation::{Live(etag), Gone, Superseded}`, called by each lane.

---

## 3. Structure

### 3.1 `GraphAccount` is a bag of about twenty-five shared fields with no owners

`account/mod.rs`. The struct holds:

- the primary client and shared clients;
- capabilities and the push mode and endpoint;
- the push lease, push tasks, two topology watches, `pending_gaps`, the EWS
  ledger, two subscription maps and the recreated-id ledger;
- the cursor index, folder tree, etag index and trash cache;
- the public-folder state: routing map, metadata, item classes and scope;
- the user email, the folder seed, and the shutdown token.

The behaviour acting on this state is free functions in other files that take
`&GraphAccount` and reach into the fields. `webhook.rs`, for example, locks
`graph_subscriptions`, `recreated_subscription_ids` and `graph_topology` by
hand, and documents lock-ordering invariants in prose ("Every write and the
teardown snapshot happen under the `graph_subscriptions` write lock").

The real components are:

- `MailboxRouter`: the primary and shared clients, `client_for_*`,
  `budget_for_*`, `owner_of_scope`, and the identity from 1.3;
- `PublicFolders`: `routing_map`, `public_folder_meta`, `public_item_classes`,
  the public-folder scope, and 2.5's resolver;
- `WebhookRegistry`: groups, the recreated-id ledger, the topology watch,
  pending gaps, and teardown and renewal state transitions as methods that own
  their locks;
- `EwsRegistry`: subscriptions, the topology watch, the release ledger;
- `EtagIndex` (with 2.16's API) and the trash cache.

`GraphAccount` would compose them. Invariants now stated in comments
("checked under the write lock, and paired with
`retire_all_graph_subscriptions` cancelling the token BEFORE it walks") would
become private to the type that holds the lock. Given the standing lesson that
teardown and lifetime fixes move holes, this is where concentrating the
invariants pays most.

### 3.2 Fold the delta walks (the `cdfcaa4f` move)

`inventory::inventory_stream_from` and `changes::changes_stream` are the same
loop:

- `PageWalk::enter`, `fetch_delta_page`, admit next and delta links;
- project each value, update the etag cache;
- branch on next link, delta link, or neither (contract violation).

Each also rebuilds
`GraphErrorContext::graph(op).with_scope(ErrorScope::Cursor(scope.clone()))`
about ten times. They have diverged on the id-less value verdict (2.7). Their
termination sequencing is also inconsistent: `changes_stream` yields
`Terminated` without `Done` on its first refusal and `Terminated` plus
`Done(None)` on every other path.

The owner is one `DeltaWalk { scope: GraphScope, client, ctx }` driver with a
`DeltaProjector` per lane (inventory entries, removals and obligations; or
changes). The public-folder EWS poll is a different protocol and stays separate.

### 3.3 One `$batch` engine

From 1.5, 1.6, 2.3, 2.4 and 2.16: one module owning `BatchLeg`,
`BatchAnswer`, chunking at 20, outer replay derived from the legs, response
resolution, absent-leg policy (Uncertain for writes, `PartialResponse` for
reads), and etag observation. Hydration, bulk mutation, PIM writes and
reactions become thin front ends. Today about 600 lines across four files
answer the same questions slightly differently.

### 3.4 Wire codecs live in the account layer

- `account/ews_stream.rs` mixes the push session driver with a byte-level
  frame decoder (`StreamingFrameDecoder`, `find_frame_start` and friends) and
  the Subscribe, GetStreamingEvents and Unsubscribe XML build and parse. The
  codec half belongs in `ews/` beside `ops.rs` and `parse.rs`.
- `account/autodiscover.rs` (about 2,400 lines) is an XML protocol codec (SOAP
  `GetUserSettings`, POX, redirect walk) plus `impl GraphAccount` methods. The
  codec and redirect walk are a sibling of `ews/`, not an account concern.
- The XML text unescape helper `push_text` exists four times
  (`ews/xml_helpers.rs`, `ews/parse.rs`, `account/ews_stream.rs`,
  `account/autodiscover.rs`). These are identical helpers, harmless on their own,
  but they show the missing shared XML layer.
- `account/calendar.rs` (about 4,300 lines) holds the typed calendar
  operations, an RRULE codec of about 800 lines, a Windows time-zone table, and
  its own wire DTOs. `GraphRecipient` and `GraphEmailAddress` are defined both
  here and in `types.rs`, with different field shapes. The RRULE codec should
  move to types if 1.10 is taken, or to its own module if not.
- `types.rs` and `contacts.rs` each define a `CONTACT_SELECT` constant with
  different values: the sync select and the typed-read select.

### 3.5 Test layout

- About 450 lines of `#[cfg(test)]` scripting infrastructure (`ScriptedRest`,
  `ScriptedRestResponse`, `ScriptedDownload`, the record and script methods)
  live inside the production `client.rs`. They belong in
  `client/test_support.rs`, like imap's `account/test_support.rs`.
- `pim/tests.rs` is one 2,559-line file covering nine modules, while `push/`
  splits its tests per arm.
- `account/ews_session_tests.rs` sits at the account root, though it tests
  `ews_stream`.
- `graph_error.rs` carries about 1,900 lines of tests inline in three `mod`
  blocks.

Each layout mirrors the order in which the code was written, not the design.

### 3.6 Event and contact sync lanes have no scope producer

`scopes::discover_cursor_scopes_inner` emits only `ObjectType::Email` folder
scopes plus public folders. The engine drives only discovered scopes. All of
the following are reachable only through a scope nothing in production
discovers:

- `GraphCursorKind::Events` and `Contacts`;
- the calendarView and contacts delta paths;
- `CursorError::CalendarWindowExpired` and the reseed logic;
- `EVENT_DELTA_PREFER`;
- `resource_for_scope`'s events and contacts arms;
- `sync_object_id`'s event and contact arms;
- the inventory tests for all of these.

Either calendar and contact folder discovery is missing, or the lanes are
built ahead of a producer. Whichever it is, the current shape keeps a large
surface compiling and tested with no path to it. This needs a ruling, not a
deletion: the published-API rule applies to the behaviour these lanes promise.

### 3.7 Provider vocabulary in the neutral crate

`GraphSignal`, `PushCapability::WebhookOrEwsStream` and
`PushSource::GraphSubscription` are Graph names in bifrost-types. The
`WireCause` enums have to live where `WireCause` lives. But under the current
split, each provider crate owns half of the table for its own enum (2.14), and
one name is false (1.2). If the enums stay in types, their full tables should
move with them.

---

## 4. Defects and other lateral findings

Separated from the proposals above, per the standing rule. These are candidate
bugs, not yet verified with a failing test.

1. **Bearer audience.** EWS and Autodiscover send the Graph-audience token to
   `outlook.office365.com` (1.14). If confirmed, EWS push, public folders and
   delegate discovery cannot authenticate in production. Not observable
   hermetically.
2. **Stale etag after a PIM write.** `set_is_read`, `set_category`,
   `set_importance` and `set_extended_property` leave the pre-write etag
   cached, so the next `bulk_set_flags` on that message hits a 412 against its
   own write (2.3).
3. **Duplicate mailbox by case.** Configured and discovered shared mailboxes
   that differ only in case become two clients and two scope sets for one
   mailbox (1.3).
4. **Default factory push advertisement.** Advertises `WebhookOrEwsStream`
   with no lanes and an `Unsupported` subscribe (1.2).
5. **Unrouted public item read back as deleted.** `get_stream` reports it
   `NotFound(Message)`, while every other lane calls it stale local state or
   unsupported (2.5).
6. **Id-less inventory value classified as Unsupported.** Its obligation
   carries `unsupported_account_error(SyncInventory)` (2.7).
7. **Out-of-range subresponse status read as 500.** `$batch` subresponse status
   outside the `StatusCode` range becomes a retryable 500 (1.5).
8. **Partial public ids accepted.** `parse_public_contact` and
   `parse_public_event` accept a missing item part as an empty id (2.15).
9. **Comment contradicts code on 503.** The throttle annotation comment claims
   503; the code excludes it, and the funnel tags it (2.1).
10. **Subscription lifetime understated.** `webhooks.rs` requests 10,080
    minutes (7 days) for every subscription. `run_subscribe`'s doc says an
    unrecoverable create "lives until Graph's own expiry (about a day)", and
    `reference/sync.md` says providers hold live subscriptions "until it expires
    them itself (24h for Graph)". The orphan and queue-for-later analyses
    therefore understate the exposure by about 7x.
11. **Unencoded subscription id.** `webhooks::renew_subscription` and
    `delete_subscription` interpolate the subscription id into the path
    unencoded, while every other id in the crate goes through
    `encode_path_component`.
12. **Quarantine drops evidence.** The foreign-scope quarantine helpers drop
    the original error's status, request id and cause chain (2.2).
13. **Inconsistent stream ending.** `changes_stream` ends some terminations
    with `Done(None)` and one without (3.2).
14. **Missing tests directory.** `crates/graph/tests/` does not exist. The
    brief lists it in scope, so either it is planned or the brief is stale.
