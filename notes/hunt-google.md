# Hunt: bifrost-google - wrong shape, wrong place

Scope: all of `crates/google/`. Read against `reference/types.md`,
`reference/sync.md` and `reference/net.md`, with Graph, JMAP, IMAP and
dav-core consulted where the same contract question is answered elsewhere.

## Headline

The crate is four Google API clients (Gmail, People, Calendar, Drive) that
share one handle named after one of them, `GmailClient`. Nothing in the type
system says which API a request belongs to. So the crate rebuilds that fact
three times, from three different inputs: from the URL
(`GmailClient::surface_for_url`), from a free-text service string
(`execute_builder(.., "Gmail API" | "People API" | "Calendar API" | _)`), and
from a field the caller sets by hand (`GmailErrorContext::surface`). The error
model then forgets it completely: `parse_json_response` and
`check_response_status` take a `_service: &str` and ignore it, and
`GmailService` has one variant, `GmailApi`. Calendar, People and Drive errors
therefore go out as `Provider::Gmail` / `Protocol::Gmail` /
`WireCause::Gmail(..)`. Quota cost, the concurrency queue, the project hold
gate, error attribution, the `insufficientScope` hint and rate-limit buckets
all hang off this missing axis, and several of them already disagree (below).

The structural fix is to make the surface a type. Use one typed handle per API
(`GmailApi`, `PeopleApi`, `CalendarApi`, `DriveApi`), each a thin view over the
shared transport. Each one owns its base URL, its `ApiSurface`, its quota
scope and cost table, its concurrency rule, its error service and its wire
types. With that in place, URL sniffing, the service string and the hand-set
context field all go away. Most of the other findings in this report are
symptoms of that one missing type, or would be fixed along the way.

## 1. Axes that should be types

### 1.1 API surface (see headline)

Sites that answer "which Google API is this":
- `client.rs` `surface_for_url` (longest-prefix match over three bases, with
  `Drive` as the catch-all for anything that matches none of them);
- `client.rs` `execute_builder`, which maps a `&str` service name and sends
  anything unrecognized to `Drive`. A typo in `"Gmail API"` silently drops the
  concurrency limit and the project hold;
- `account/error.rs` `GmailErrorContext.surface`. It defaults to `Gmail` in
  `base()`, is overridden in the `calendar_*` and `contact*` constructors, and
  is set by hand in `cloud.rs::ctx()`;
- `quota.rs` `ApiSurface` itself, which is the right enum, but nothing carries
  it on a request.

Whether a response may stamp the Gmail project budget is decided twice. It is
decided at observation time, keyed on the URL's surface (`send_recorded`), and
again at translation time, keyed on the context's surface
(`translate_response`, and `translate_net_error`'s `LocalHold` arm). The two
copies are held in step by `quota::tests::check_surface`, which forges headers
to prove that the context-side copy also refuses. That is a pairwise-agreement
test over two copies of one decision. Owner: the typed surface handle. A
request built through `GmailApi` is the only thing that can carry a project
observation, and its errors carry the surface in their own type.

### 1.2 HTTP method as `&str`

`execute` and `execute_if_match` take `method: &str` and `unreachable!()` on
anything that is not one of five strings. `gmail_quota_cost(path, method)` is
also keyed on the string, and `post_empty_json` passes the literal `"POST"`
back in to look up its own cost. Use `http::Method`, or better, a per-method
typed call, so that the cost lookup and the watch/stop concurrency bypass are
keyed on the endpoint rather than re-derived from a string.

### 1.3 Endpoint identity as path strings

The quota table (`gmail_quota_cost`) re-parses the URL into segments. The
concurrency bypass (`execute_if_match`) string-matches `"/watch" | "/stop"`
separately. Error classification is keyed on a third flag,
`GmailErrorContext.history_endpoint: bool`. All three answer "which Gmail
endpoint is this", from three representations. A
`GmailEndpoint { Profile, History, Watch, Stop, MessagesGet, BatchModify, .. }`
value, minted by the `api.rs` method that builds the request, could own the
unit cost, the concurrency class, and the history-endpoint classification
(the five `if ctx.history_endpoint` arms in `classify_response` and
`classify_by_status`).

### 1.4 History id is a string parsed in five places

`GmailProfile.history_id`, `GmailMessage.history_id`,
`GmailHistoryResponse.history_id` and `GmailWatchResponse.history_id` are all
`String`. They are parsed to `u64` separately, each with its own error wording:
- in `GoogleAccount::open`;
- in `inventory::inventory_checkpoint`;
- in the history-page arm of `changes`;
- in `inventory_entry_from_message_indexed`, where a parse failure silently
  becomes `ServerVersion::Unavailable`.

`push::WatchLifecycle::Watched` keeps it as an unparsed `String`.
`ChangeState.start_history_id` is a `u64` that was formatted back into a
`String`. A `HistoryId(u64)` newtype with a string-number serde adapter would
parse once, at deserialization. The classification would then be uniform: a
complete response whose field did not parse becomes `JsonDecode`, which maps
to `ParseFailed`.

### 1.5 Mailbox identity is a bare email compared ad hoc

"Is this the same Google account" is answered by:
- `cursor::decode_gmail_state_for_profile`, with `eq_ignore_ascii_case`;
- `changes.rs`, twice: it decodes the cursor and re-implements that same
  comparison inline instead of calling `decode_gmail_state_for_profile`, then
  compares again against a live `getProfile`;
- `push::mailbox_watch`, which keys on `to_ascii_lowercase()`. Its own comment
  notes that aliases and googlemail.com spellings escape coordination;
- `cloud::account_domain`, which splits the same string to derive a sharing
  domain.

A `MailboxAddress` newtype, minted only from a `getProfile` response and
carrying its own equality, would give every site one answer. It would also
make "came from the provider, not the caller" true by construction, which is
the distinction `cloud.rs` has to argue in prose
(`ProviderValueUnusable` versus `InvalidRequest`).

### 1.6 Gmail label vocabulary is strings plus scattered constants

The system label ids are declared three times, as `const`s in `flags.rs`,
`pim.rs` and `filters.rs`, each with its own subset. `ARCHIVE_ID` is declared
in both `flags.rs` and `filters.rs`. The facts about a system label are spread
across files:

| Fact | Where it lives |
|---|---|
| Exclusive display container | `flags::EXCLUSIVE_CONTAINERS` |
| Read-only projection | `flags::is_read_only_label` |
| Maps to a flag | `canonical_flags_indexed`, `flag_to_add_label`, `flag_to_remove_label`, `gmail_flag_target`, and the hard-coded `pim::set_is_read` |
| Hidden from flags | `canonical_flags_indexed`'s inline skip list, which includes `"CHAT"` and nowhere else does |
| Folder role | `pim::container_from_label` |
| Search operator | `pim::container_query` and `filters::container_query` |
| System or user | `GmailLabel.label_type: Option<String>`, compared against `"user"` / `"system"` |

These already disagree:
- **Case.** `flags.rs` and `pim::delete_thread` compare label ids
  ASCII-case-insensitively. `pim::container_from_label` and both
  `container_query` copies match `LABEL_INBOX` etc. exactly.
- **Archive.** `pim::container_query` tests archive with `is_archive_id`
  (case-insensitive). `filters::container_query` tests `id == ARCHIVE_ID`.
  So `SearchFilter::In("Archive")` becomes the archive expression, while
  `FilterCondition::InContainer("Archive")` becomes `label:Archive`.
- **Label type.** `flags::patch_for_set` and `user_label_id_from_flag` treat
  only `Some("user")` as a user label. `scopes::diff_snapshots` emits renames
  for anything that is not `Some("system")`, so it counts `None` as a user
  label. `pim::label_is_system` requires `Some("system")`.

Owner: a `GmailLabelId` that parses into
`System(SystemLabel) | Category(..) | User(String)`, with
`SystemLabel { Inbox, Sent, Draft, Trash, Spam, Unread, Starred, Important, Chat }`.
It would carry methods for `is_exclusive`, `is_writable`, `role`,
`search_operator` and `flag`. `GmailLabel.label_type` should be an enum with an
`Unknown` arm, decided once.

### 1.7 The `$gmail-label:<id>:<name>` flag encoding is parsed four times

It is produced in `canonical_flags_indexed` as a literal `format!`, not with
`USER_LABEL_FLAG_PREFIX`, and in `patch_for_set` with the constant. It is
parsed in:
- `flags::user_label_id_from_flag`;
- `flags::gmail_flag_target`;
- `mutation::has_unresolved_label_id`, with its own literal `"$gmail-label:"`
  and an extra empty-id filter that the others lack.

A `GmailFlag` enum with `parse` and `Display` (`Seen`, `Flagged`, `Draft`,
`Important`, `Label { id, advisory_name }`, `Unknown(String)`) would replace
all four.

### 1.8 `LabelPatch::unsupported_flags` carries two unrelated things

It holds flag tokens the translator could not express. It also holds prose
diagnostics: `"unknown future FlagOp variant"`,
`"gmail move destination must be a label"`, and
`"gmail move source must be a label"`. `apply_label_patch_tracked` then
reinterprets the field by operation (`operation == BulkMove && !unsupported.is_empty()`
becomes `InvalidRequest`). Meanwhile, for `UpdateFlags`, the same field feeds
the `FlagsPartiallyApplied { unsupported }` effect that a consumer reads as a
list of flags. A move whose destination is not a label is a caller error and
should never be a `LabelPatch`. `move_patch` should return
`Result<LabelPatch, MoveRefusal>`, and `unsupported_flags` should hold only
flags.

### 1.9 Opaque ids that are really composites

- `EventId` is `calendar_id + "::" + event_id` (`join_event_id` /
  `split_event_id`).
- The cross-calendar search cursor is `calendar_id + '\n' + page_token`.
- `BlobId` is a JSON `GmailBlobKey`.
- `SubscriptionHandle` is a JSON blob whose `topic`, `history_id` and
  `expiration` fields are never read back (`actor_unsubscribe` decodes it into
  `_decoded` only to validate it). Only `handle_id` matters.
- Obligation keys are `format!("gmail:message:{id}")`.

Each composite has its own ad hoc codec and its own failure classification.
Typed keys with one codec each would also fix live defect D1.

### 1.10 Page cursors: one shape, three refusal classifications

A malformed page cursor (not UTF-8) is classified differently at each site:
- `pim::page_token` uses `other_error(Search)`, so `search_messages` reports
  operation `Search`;
- `calendar` uses `caller_input_error`;
- `contacts` uses `caller_input_error` with the raw `FromUtf8Error` text.

Across crates, JMAP's `page_cursor.rs` classifies a stale or malformed page
cursor as `SyncState(SchemaIncompatible)`, and Graph has an `AdmittedUrl`
cursor type in `crate::paging`. Google has no cursor type at all. It should
have one (`PageCursor`), and its refusal should match the workspace answer.

### 1.11 Failures that reach callers only as prose

- `GmailSignal::Unknown { code }` is used deliberately for reasons the crate
  RECOGNIZES: `invalidArgument`, `insufficientPermissions`, `domainPolicy`,
  `conditionNotMet` and `notAuthorizedToAccessThisResource` are routed by
  string match inside the `Unknown` arm of `classify_response`. A consumer
  branching on `WireCause::Gmail` sees "unknown" for reasons Google documents.
  Promote them to variants. Note that `GmailSignal` lives in bifrost-types, so
  this is a types change (see 3.6).
- `gmail_scope_for` returns `&'static str` scope names
  (`"gmail.modify"`, `"drive.file"`). It is keyed on `AccountOperation` with a
  `_ => "gmail.modify"` catch-all, so every People and Calendar operation
  claims to need `gmail.modify`. Live defect D2.

### 1.12 Stream state machines as boolean pairs

`ChangeState`, `HydrateState` and `MutationState` each encode their phase as
`finished: bool` plus `emitted_done: bool`, plus `checked_profile`,
`pending_termination`, `pending_event` and `Option` fields whose combinations
include impossible states. `changes.rs` has an arm that mints an
`Internal(InvariantViolated)` error for "checked the profile but has no start
history id". That error exists only because the state allows it.

The doc comment on `get_stream_cancellable` argues against a shared driver,
and that argument is sound. It does not, however, argue against a per-stream
phase enum, for example
`enum Phase { Probe { cursor }, Walking { start: HistoryId, .. }, Terminating(AccountError), Done }`.
With that shape, the invariant-violation arm cannot be constructed.

## 2. Decisions made in more than one place

### 2.1 "Which Google API, and what does it cost" (see 1.1 to 1.3)

Two request funnels decide quota cost and concurrency independently:
- `execute_if_match` applies the cost from `gmail_quota_cost`, and the
  concurrency limit unless the path is `/watch` or `/stop`;
- `execute_builder` applies the concurrency limit only if the service string
  is `"Gmail API"`, and applies NO cost. Its comment tells callers to apply the
  cost themselves.

`mutation::post_empty_json` builds through the raw funnel only to add the cost
back by hand, and gains nothing from it. `client.post_no_content(path, body, replay)`
already does exactly this: a buffered POST, a status check, and the same
error shape. Delete the second funnel for every Gmail caller. The raw builder
is genuinely needed only by Drive's resumable upload (custom headers,
bearer-less, redirects off).

### 2.2 Rate-limit bucket per host, but Gmail's quota is per API

`default_account_net` registers one bucket per distinct host. In production,
Gmail, Calendar and Drive all live on `www.googleapis.com`, so Calendar
"rides the Gmail registration". Drive does too, and nothing mentions it. The
registration comment says Calendar "takes the general Google bucket", but in
production that bucket is the Gmail quota-unit bucket (100 units/s,
`cost_default` 1). Calendar and Drive traffic therefore debits Gmail's
per-user budget, and a large Gmail hydration delays calendar reads.

bifrost-net already supports two quota scopes on one host
(`RequestBuilder::quota_scope`). `reference/net.md` names the Gmail
per-project versus per-user case as the reason it exists. Owner: the typed
surface handle stamps `quota_scope(surface)` on every request, and
registration declares one bucket per surface rather than per host.

### 2.3 "Is a NotFound here dischargeable": the cursor-bridge argument made twice

The reason absence is not owed is the same in both places: the cursor is
anchored at the historyId sampled before the walk. It is written out as prose
at two sites, which then act differently on the same fact:
- the inventory walk drops the id silently (`continue`);
- `repair_inventory` mints `DefinitivelyIrrelevant::AbsentUnderCursorBridge`.

Both test `account_error.kind() == NotFound(Message)` after translation. The
bisection and per-item lanes in `mutation.rs` answer "is this a 404" a third
way, from the raw status (`is_not_found` on `response_parts`).
`MutationProgress::finish_failures` takes that raw-status answer as a
`not_found: bool`.

Owner: a single function, `fn absence_proven_by_cursor_bridge(&AccountError) -> Option<Evidence>`,
next to `cursor.rs`, because the proof rests on the cursor model. Both
inventory sites would call it. The walk should arguably report the same
evidence rather than silently `continue`, so the engine can see it.

### 2.4 Page-walk guards copied four times, held in step by comments

`inventory_walk_refusal`, `changes::walk_refusal`,
`contacts::address_book_walk_refusal` and the inline loop in
`calendar::calendars_list` each implement the same rule: repeated token first,
then budget. Each doc comment says it "matches" the other three. The calendar
copy differs in shape: its budget is the `for` bound, so it refuses only after
fetching the 10,000th page rather than before following its token.
`PageRefusal` is the shared result type, but there is no shared guard.

Owner: a `PageWalk { seen: HashSet<String>, pages: usize, budget: usize }`
with `admit(next: Option<&str>) -> Result<Option<Token>, PageRefusal>`, in
`crate::error`, or better, in bifrost-net beside `parse_retry_after_hint`.

Cross-crate: Graph centralizes its equivalent in `crate::paging` with an
`AdmittedUrl` type, so the workspace has two independent page-walk
admissions. That is the shape of `77df77df` waiting to happen.

### 2.5 "Move into X, optionally out of source S" is decided twice

`flags::move_placement_patch` is shared, and its comment says the bulk and
single-object paths "agree on the wire shape". But the source-folding rule is
copied verbatim, in `mutation::move_patch` and `pim::move_container_patch`.
The rule drops a redundant source if it is archive, equal to the destination,
or already among the removals. Move the source fold into `flags` too:
`move_placement_patch(destination, source)`.

A third "move" exists with different semantics. `filters::action_from_filter_actions`
lowers `MoveTo(c)` to "add c, remove INBOX" and `Delete` to "add TRASH, remove
INBOX". It never consults `move_placement_patch`, and that produces live
defect D3.

### 2.6 Content-Disposition and Content-ID: two copies that disagree

`blobs::content_disposition_inline` compares the disposition TYPE (the first
`;` token) to `inline`. `pim::content_disposition_inline` uses
`value.to_ascii_lowercase().contains("inline")`, so it matches
`attachment; filename="inline.png"`. Hydration (`blobs`) and draft re-render
(`pim::attachment_inlines`) therefore classify the same part differently.
`content_id_value` is also duplicated. One copy should exist, and arguably it
belongs in bifrost-types' MIME module beside `DecodedAttachment`, since every
protocol crate answers this question.

### 2.7 `References` header parsing: two copies that disagree

`inventory_entry_from_message_indexed` splits and trims angle brackets but
keeps empty items. `pim::references_from_header` filters them out. The same
header therefore yields different `InventoryEntry::references` and
`Message::references`. `InventoryEntry::differs_from` compares that field.

### 2.8 Search query and filter query translators

`pim::filter_query` (`SearchFilter` to Gmail `q`) and `filters::condition_query`
and `apply_condition` (`FilterCondition` to Gmail `q`) duplicate `query_term`,
`sanitize_term`/`sanitize_header`, `container_query` and `gmail_date`, and
translate nearly the same algebra (From, To, Subject, Body, Has, In,
DateRange, And, Or, Not). The `container_query` copies already disagree on
archive (1.6). One Gmail query encoder should exist.

Lateral, for bifrost-types: `SearchFilter` and `FilterCondition` are two
message-predicate algebras with overlapping variants. Every crate with both
search and filters re-translates the overlap twice.

### 2.9 Label vocabulary: two loaders, two staleness policies

`scopes::labels_for_flags` and `scopes::scope_snapshot_or_refresh` are the
same single-flight staleness check with different failure policies. The first
returns the stale vocabulary when a refresh fails; the second propagates the
error. `discover_memberships` uses the second, so a transient `labels.list`
failure terminates discovery even when a usable cached vocabulary exists.

Separately, the lifecycle poller (`scope_lifecycle_stream_with_baseline`)
fetches `labels.list` every 30 s into its own baseline and never feeds
`ScopeCache`. Two label views run side by side. The lifecycle stream can
announce `Created(Label(x))` while flag translation still lacks `x` and treats
it as unsupported until the next refresh. That costs a second `labels.list`
round trip that the poll already paid for.

Owner: one vocabulary actor that the poller refreshes. Lifecycle diffs become
a view over its snapshots.

### 2.10 Cursor identity check (see 1.5)

`changes.rs` re-implements `decode_gmail_state_for_profile` inline instead of
calling it. The checks agree today only because both use
`eq_ignore_ascii_case`.

### 2.11 Which operations are multi-leg: decided far from the legs

`mod.rs::shutdown_account_future_error` hard-codes
`EventUpdate | ContactUpdate` as the operations whose first write can commit
before a second, and narrows their idempotency to false. The legs themselves
live in `calendar::update` (move, then PATCH) and `contacts::update` (fields,
then photo), and each of those already builds a `PartialCommit` record. A new
multi-leg operation, or a change to these two, will not update the list in
`mod.rs`.

Owner: the operation's own future should report its leg state, or at least
declare its multi-leg-ness beside its `PartialCommit` builder. The adapter
should not keep a separate list.

### 2.12 Idempotency overrides that restate the shared table

`GmailErrorContext::send()` and `draft(DraftSend)` set
`idempotency_override(Some(false))`. `AccountOperation::is_idempotent` already
returns `false` for `Send` and `DraftSend`. The override is a copy of the
table. It is harmless today, but it is exactly a local restatement that will
not follow the table if the table changes. Delete it.

### 2.13 Caller-version extraction: two shapes

`calendar` uses `ServerVersion::expected_etag(Protocol::Gmail, op, false)`.
`contacts::update` hand-matches `ServerVersion::ETag(etag) if !etag.is_empty()`
and refuses everything else as `Unsupported`. That includes an empty ETag,
which the shared helper may classify differently. Use one helper.

### 2.14 Cross-crate: cursor-decode failure classification disagrees

For a stored cursor whose bytes do not decode:

| Crate | Classification | Derived recovery |
|---|---|---|
| Google (`GmailCursorFailure::MalformedPayload`) | `SyncState(CursorInvalid)` | `RestartScope(Account)` |
| Graph (`CursorError::Decode`) | `SchemaIncompatible` | |
| JMAP (`state.rs`, every decode failure) | `SchemaIncompatible` | |

The protocol-tag check and the inner envelope-version check are also
re-implemented per crate (`decode_gmail_state`, Graph `cursor.rs`, JMAP
`state.rs`, IMAP `envelope.rs`). Owner: an `OpaqueChangeState::decode::<T>(protocol, version)`
in bifrost-types, returning one typed `CursorDecodeError`, with one mapping in
the error model.

### 2.15 Cross-crate: "call on a closed account" has no shared answer

Google refuses calls on a closed account as `Unsupported(op)` (`account_closed_error`,
`push::closed_error`). That is the same class a consumer reads as "this
account cannot do this", for a method that `capabilities()` advertises.
Graph's `push::common::account_closed_error` refuses as
`Internal(RuntimeFailure)`. bifrost-types has no lifecycle kind for this, so
each crate picks one.

### 2.16 Cross-crate: per-item versus stream-level recovery partition

`error::requires_stream_termination` partitions `RecoveryClass` into "fan out
per item" and "terminate the stream". It is used by mutation, inventory
hydration and get_stream. dav-core has a sibling partition
(`recovery_rank`). Neither lives in bifrost-types, although the question is
contract-level: which recovery classes describe one object, and which describe
the stream. Its owner is `RecoveryClass`.

### 2.17 Capability mirror held in step by a test

`capabilities.rs::PimMethodSupport` is a hand-written table of bools. The
methods in `mod.rs` and `pim.rs` refuse independently.
`capability_contract_tests.rs` says so in its own header: it is "a
hand-maintained mirror", checked in one direction only. Every account crate
has the same structure.

Owner, in bifrost-types: let the `Account` default for each optional method
consult `capabilities().pim_methods.x` and refuse before calling an
implementation hook, so a `false` flag is the refusal rather than a promise
of one. Alternatively, derive the table from which hooks a crate implements.

## 3. Structure

### 3.1 Split the client by API (the headline)

Proposed layout:

```
src/
  transport.rs        shared AccountNet attachment, ByteTally, send_recorded
  gmail/{api,types,quota,endpoint}.rs
  people/{api,types}.rs
  calendar/{api,types}.rs
  drive/{api,types}.rs   resumable session, permissions; base URL configurable
  error.rs            Error carries the ApiSurface (GmailService grows to all four,
                      or is replaced by ApiSurface)
  account/...
```

Today the Calendar and People wire types (`GoogleEvent`, `Person`, ...) and
their URL builders live inside `account/calendar.rs` and
`account/contacts.rs`, mixed with the `Account` mapping. Only Gmail has an
`api.rs` and a `types.rs`. Drive hard-codes `SESSION_URL` and
`https://www.googleapis.com/drive/v3/...`, with no base override, unlike the
other three APIs.

Two pairs of duplicate Gmail wire types exist: `inventory.rs` defines its own
`ListMessagesResponse` and `GmailMessageStub` beside the ones in `types.rs`,
and its own `list_messages_page` beside `api::list_messages`.

### 3.2 `account/error.rs` is one translator for four APIs

`classify_response` treats every envelope as Gmail's. `FailedPrecondition`
outside the history endpoint maps to `ConcurrencyConflict`, and
`PubSubSubscriptionDeleted` is matched on `operation == PushSubscribe`. People
reports `google.rpc` statuses and Calendar uses its own reason set, yet both
run through the Gmail reason table and are stamped `WireCause::Gmail`. With
typed surfaces, each API gets its own classifier feeding one builder funnel,
and `Provider` / `Protocol` stop claiming Gmail for Calendar.

For contrast: Graph distinguishes `Protocol::Graph` from `Protocol::Ews` for
its two surfaces. Google stamps one protocol for four.

### 3.3 Process-global watch registry

`push::MAILBOX_WATCHES` is a `static LazyLock<Mutex<HashMap<String, Weak<..>>>>`.
It is process-wide, not scoped to the `Net` or to the factory. The tests have
to mint unique mailboxes because of it (`NEXT_MAILBOX` in `mod.rs` tests).
bifrost-net already offers Net-scoped string-keyed registries
(`shared_hold`, `shared_concurrency_limit`) for exactly this kind of
cross-account coordination. The mailbox watch coordinator belongs in one of
those, keyed by the typed mailbox identity (1.5), so that two independent
`Net`s really are independent.

### 3.4 `pim.rs` does several unrelated jobs

It contains:
- single-object label mutations;
- send;
- the draft read-modify-write;
- the MIME body selection (`body_parts`, `collect_body_parts`);
- search translation;
- container CRUD;
- identities;
- vacation;
- thread and message hydration;
- `MailDocument`.

The body selection is a Gmail-only re-implementation of "which part is the
body": first `text/plain`, first `text/html`, `String::from_utf8` or drop.
bifrost-types' `mime::select_body` already exists for that question. It also
always sets `Message::incomplete: false`.

Suggested split: `labels_single.rs` (or fold into `mutation.rs` with the
shared move rule), `compose.rs` (send, drafts, `MailDocument`),
`hydrate.rs` (Message and Thread, ideally over the shared MIME selector),
`search.rs` (with the shared query encoder from 2.8), and `settings.rs`.

### 3.5 Single-object mutations bypass the bulk driver's classification

`pim::modify_target` uses `GmailErrorContext::base(operation)`, with no
resource and no scope. A 404 on a thread target therefore classifies as
`NotFound(Message)` with no id. `move_thread` and `delete_thread` stamp
`AccountOperation::BulkMove` and `BulkDestroy` instead of `MoveThread` and
`DeleteThread`. Recovery is the same, because all four are non-idempotent,
but telemetry, the insufficient-scope hint and consumer routing are wrong.
Where `GmailErrorContext` is built from a `MutationTarget`, these cannot
diverge.

### 3.6 Provider vocabulary owned by bifrost-types

`GmailSignal` (and its Graph sibling) live in `bifrost-types/src/error/cause.rs`.
Every reason-code change in this crate is therefore a change to the
protocol-neutral contract crate. 1.11 is an example. Weigh whether
`WireCause` should carry an opaque `(Provider, code)` plus a crate-owned
typed decode, so that the vocabulary moves to its owner. If consumers need
to branch on typed Gmail reasons, the current placement is defensible; it is
the one place I would not move without asking who reads it.

### 3.7 Clocks

The wall clock is injectable (`GmailClient::wall_clock`) and the observer
uses it. But `GmailResponseHeaders::from_headers` calls `SystemTime::now()`,
and it is used by `parse_json_response`, `check_response_status`,
`post_empty_json`, `cloud::response_error` and the refine path in
`account/error.rs`. So `retry_at` is resolved against the injected clock on
one path and the real clock on the others. Thread the client's clock, or
resolve retry hints only in the observer.

## Live defects found along the way

- **D1. Blob errors scope a JSON blob id as a message id.**
  `GmailErrorContext::open_blob(id)` puts its argument into
  `ErrorScope::Message { id }`, and `blobs::translate` passes `blob_id.0`,
  which is the JSON `{"message_id":..,"attachment_id":..}`. A missing
  attachment therefore reports `NotFound(Message)` with the JSON string as the
  message id. The comment beside it says the intent is to anchor on the parent
  message. The `impl Into<String>` parameter is what let the wrong id through
  (1.9).
- **D2. `gmail_scope_for` names the wrong OAuth scope for People and
  Calendar.** Every People and Calendar operation falls through to
  `_ => "gmail.modify"`, so an `insufficientPermissions` from Calendar tells
  the consumer to request `gmail.modify`.
- **D3. Filter `MoveTo(archive)` sends the synthetic `archive` id in
  `addLabelIds`.** `flags.rs` documents that Gmail rejects that modify.
  `MoveTo("INBOX")` emits INBOX in both add and remove. Neither case is
  tested. Fixed by routing filter moves through `move_placement_patch` (2.5).
- **D4. Draft re-render misclassifies attachments as inline** when the
  disposition parameters contain the substring "inline" (2.6).
- **D5. The archive search operator depends on which API you call** (1.6):
  `"Archive"` becomes the archive expression in search and `label:Archive` in
  filters.
- **D6. Calendar and Drive traffic debits the Gmail per-user quota bucket in
  production** (2.2).
- **D7. Stale quota claims in comments.** `inventory::hydrate_one` says
  `messages.get` costs "5 units each", and `flags::patch_for_set` says
  "5 quota units each". The table in `client.rs` (dated May 2026) says 20. The
  argument in `patch_for_set` against read-then-diff is four times stronger
  than it states. Reword the comments so the figure is not hard-coded.
- **D8. A Pub/Sub registration loss is classified as a cursor loss.**
  `pubsubSubscriptionDeleted` / `Expired` under `PushSubscribe` map to
  `SyncState(CursorInvalid)`, which derives `RestartScope(Account)`. The
  context comment says this is deliberate. But a lost watch costs only a gap,
  which push already signals (`gap_hint`); the history cursor is still valid.
  Restarting the scope throws away a good cursor because the error model has
  no push-registration kind. This is a classification choice made for lack of
  vocabulary, so it needs a ruling rather than a silent fix.
- **D9. Non-UTF-8 bodies vanish in `message_hydrate`.** `collect_body_parts`
  decodes Gmail `body.data` with `String::from_utf8` and drops anything that
  fails, so a Latin-1 or ISO-2022-JP text part yields `body_text: None` with
  `incomplete: false`. The shared MIME selector handles charsets; this copy
  does not (3.4).

## Smaller notes

- `discover_memberships` terminates on a label refresh failure even when a
  cached snapshot exists (2.9).
- `api::list_messages` returns a three-tuple
  `(Vec<Stub>, Option<String>, Option<i64>)`; return the typed response.
- `GoogleAccountFactory::from_token_source_with_net` repeats the Gmail base
  literal instead of using `GMAIL_API_BASE`.
- `push::closed_error` always uses the `push_subscribe()` context, even when
  called for `PushUnsubscribe`, so its scope is `Cursor(Account)` for an
  unsubscribe.
- `MutationCapabilities::reassertion.destroy = true`, while a destroy can
  downgrade to a TRASH move on a scope failure. A reasserted destroy repeats
  the same failing `batchDelete` and its fallback. That is consistent with
  the contract (`Downgraded` is not replayed), but each reassertion costs a
  50-unit refusal. Worth confirming that this is intended.
- `MailDocument::ensure_from` uses `IdentityId.0` as an address. The identity
  id doubles as the email (`identities_list` mints it from `send_as_email`),
  which is another composite-in-a-string (1.9).
