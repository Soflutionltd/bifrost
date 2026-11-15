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

## BUG-002 - Graph `open_blob_range` trusts caller-forgeable handle capability flags

**Reported by:** types.

`AccountCapabilities::blob_range` and `blob_digest_pre_download` restate at
account level what `BlobHandle::capabilities` says per handle. The handle is a
public, caller-constructible struct, and Graph's `open_blob_range` trusts
`handle.capabilities.supports_range` to decide whether to send a ranged
request, so a forged or stale handle steers the provider. Gmail and JMAP refuse
regardless. Proposal: providers re-derive handle capability from the `BlobId`
they minted rather than reading it off the caller's handle.

## BUG-016 - The backfill completion marker wins resume selection by a magic number, and can lose

**Reported by:** sync-lanes (latent; live once any provider adopts partitioning).

Mostly fixed: a typed `BackfillKey` with codecs for every existing encoding, one
public resume-comparison helper consumer stores can call, proven completion
beating counts, honest completion counts, and atomic completion compaction in
the in-memory store. Residue: incomplete `TimeWindowed` walks still mint fresh
absolute-instant keys each attach and accumulate rows; an unknown future
`InventoryPartition` variant still falls back to the historical shared key
(a safe opaque identity needs bifrost-types support); and external
`CheckpointStore` implementations must adopt the comparison helper and the
compaction contract, which nothing enforces. The original account:

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

## BUG-018 - The ledger and cursor codecs panic on a `bifrost-types` variant they have not learned

**Reported by:** sync-lanes (latent).

Partly fixed in wave 14: the operation, object-type and protocol tag encoders
moved into bifrost-types as exhaustive matches. Residue: the other
non-exhaustive ledger vocabulary encoders in `cursor/ledger_envelope.rs` still
fall back to a panic on an unknown variant. The original account:

`cursor/ledger_envelope.rs::encode_operation` looks the operation up in the
positional table `OPERATIONS` and panics if it is missing. The table matches
`bifrost-types` today, but `AccountOperation` is `#[non_exhaustive]` in another
crate, so nothing at compile time ties them: the first new operation reaching a
persisted `last_error` or barrier `evidence` panics the account writer.
`cursor/envelope.rs` has the same shape for `ObjectType` and `ProtocolKind`
("encoding a variant the codec does not know panics"), and that codec runs
inside consumer stores. The structural cause is STR-045.

## BUG-032 - IMAP FETCH merge collapses partial fetches of one section at different origins

**Reported by:** imap-protocol (latent).

`BodySection { section: String, origin }` and `FetchAttr::BodySection {
section: Option<String> }`. `account/get.rs::merge_fetch_response` matches body
sections case-insensitively by name and IGNORES `origin`, so two partial fetches
of the same section at different origins in one response collapse (the second
is dropped); binary sections compare `Vec<u32>` exactly. Proposal: a typed
`SectionSpec { parts: Vec<u32>, text: Option<Header | HeaderFields(..) | Text |
Mime> }`, with origin part of the key that owns "same section".

## BUG-052 - IMAP `bulk_move` to a DAV-owned folder goes to the IMAP server while `throttle_keys` says IMAP does not own it

**Reported by:** imap-account (live).

The live defect is fixed: `bulk_move` and the membership throttle arm both go
through `DavScopeIndex::imap_mailbox`. Residue: nothing makes the routing rule a
compile error. `folder_from_scope` and `MailboxName::from_account_id` still turn
any `FolderId` into a mailbox without routing, and `route_scope` produces a
validated `MailboxName` that `inventory_stream` and `changes_stream` discard
(`Ok(ScopeHandler::Folder(_)) => {}`) and recompute inside the task. Owner: a
`RoutedScope` / `ImapFolder` type obtainable only from the router, for
`CursorScope` and `MembershipScope` alike, carried into the tasks.

## BUG-055 - IMAP `SearchFilter::Has` (attachment filename or type) compiles to a full-text `BODY` search

**Reported by:** imap-account.

It returns messages that merely mention the string. Refusing the filter as
`Unsupported` would take away a search path that works today; a correct fix
needs attachment-aware evaluation that composes with boolean filters and
position paging (for example BODYSTRUCTURE-based post-filtering of the
candidate set). The defect is now documented at the `pim.rs` arm and in
`reference/imap.md`.

## BUG-057 - What an absent or zero JMAP core limit means is decided in eight places, with live disagreements

**Reported by:** jmap-protocol (live disagreement).

Partly fixed in wave 11: `CoreLimits`, required-field validation and the upload
limit now live in `crates/jmap/src/core/session.rs`, the bare-handle batching
callers decode through it, and factory validation already precedes the open
probes. Upload-size zero and concurrency zero keep deliberately distinct
policies (commented there). Residue: `SessionState::derive` does not yet
validate once per installed session and cache the result, and the
`CallLimit` / request-size / concurrency readers still interpret raw fields.
The original account:

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

## BUG-070 - The same JMAP `notFound` is success through `bulk_set_flags` and terminal through `set_keyword`; providers disagree on vanished targets

**Reported by:** jmap-account (live, P2).

The JMAP-internal split is gone: `sync/mail_set.rs` is one executor for the bulk
and single-target doors, and a vanished target is `Skipped` on both. Residue,
cross-provider: JMAP answers a flag update or a move of a vanished message as
`Skipped`, while Graph absorbs a 404 as `Skipped` only for destroy and reports
`Failed(NotFound)` otherwise, so the same user action reads differently by
provider. Owner: a per-`AccountOperation` "absent target" policy in
`bifrost-types`, applied by every provider. The read-back side of "is it gone" is
BUG-021.

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

## BUG-099 - SMTP fabricates a network failure to classify a contended token cache

**Reported by:** smtp-sasl; residue after wave 11 fixed the IMAP and ManageSieve token paths.

SMTP's `oauth2_token_blocking`
(`crates/smtp/src/transport/smtp/authentication/credentials.rs`) turns
`TokenSnapshot::Contended` into a fabricated `bifrost_net::Error::RefreshFailed {
source: Network { "OAuth token cache is busy" } }` purely to obtain a transient
classification. The classification of a snapshot state belongs on `TokenSnapshot`
(or in net's projection). Token-lookup failures themselves are now classified
once, by `bifrost_net::token_source_account_error`, for IMAP, ManageSieve and
SMTP. IMAP's `Error::OAuthToken` no longer carries the SASL mechanism name.

## BUG-170 - Small wave 10 lateral findings across Google, Graph, JMAP and IMAP

**Reported by:** wave 10 fixers (lateral).

- Graph etag updates apply every insert before every removal, whatever the wire
  order of a repeated id, so a delete then re-add of one id within a page can
  leave the wrong final state.
- JMAP: a method response omitted entirely from a JMAP reply maps through
  `CallNotFound` to a terminal `MissingField` outside the send path, where the
  per-id lanes now report an omission as unanswered (retryable).
- IMAP: NOTIFY `MessageNew.fetch_attrs` still bypasses FETCH extension admission
  (`validate_fetch_wire`); its raw items get syntax validation only.

## BUG-171 - Small wave 11 lateral findings across IMAP, net uploads and sync

**Reported by:** wave 11 fixers (lateral).

- IMAP `account/mutate.rs`: the COPYUID diagnostic target construction can
  still encode a zero UIDVALIDITY epoch, although SELECT assembly now
  normalizes zero to absent.
- Resumable uploads (`crates/net/src/resumable.rs`, used by Google Drive and
  OneDrive): both paths accept a 200 or 201 after a non-final chunk without
  verifying that every payload byte was sent.
- Sync `recovery.rs`: documentation claims `plan_recovery` is exhaustive at
  compile time, but its helper-based classification can reach a runtime panic
  instead.
- Sync `recovery.rs::dispatch_stream_failure` stamps a 1 s
  `RetryHint::After` on every Retry and Reconcile plan, including per-item
  mutation failures (`bulk::classify_item_outcome`), so `keep_longer_advice`
  always sees a hint, and it logs once per failed item. The reconciler comment
  "skips hintless/unkeyed failures" now only describes throttle keys.
- Sync `reattach::handle_account_error` dispatches again for an error the drive
  or reconciler already dispatched before forwarding it, so holds are recorded
  twice (harmless under max-deadline merging).
- Sync proof generations: if the bounded `compacted` audit history evicts an
  entry while `proved` is empty, `DebtLedger::latest_generation` can fall back
  to an older baseline. No open debt sits above it today; nothing pins it.
- Graph: a missing Outlook token source (Outlook origin differing from Graph's)
  is refused by bifrost-net as `invalid_argument`, a caller-input Request error
  rather than a configuration lane. Unverified whether EWS streaming then ends
  push for the whole account or only drops the EWS fallback.
- `event_in_range` in `crates/caldav/src/account.rs` and
  `crates/jmap/src/sync/calendar_ops.rs` is the same test-only shim over
  `EventRange::admits`; a public window constructor would remove both.
- Graph: no account-open or factory configuration path supplies
  `GraphClient::with_outlook_source` yet, so on a cloud whose Outlook origin
  differs from Graph's, EWS streaming fallback, delegate Autodiscover and public
  folders are refused until a consumer can configure one. Wire the source
  through the factory.
- Sync `dispatch_stream_failure` rewrites every Retry/Reconcile advice to
  carry a hint, so a reader of the dispatched advice cannot tell a provider
  hint from the local 1 s floor (no current reader is affected).
- IMAP `oauth_token_error` stamps `Protocol::Imap` for ManageSieve too.
- IMAP SELECT assembly: a `UIDVALIDITY 0` (or `UIDNEXT 0`) after a valid one
  in the same response erases it; order-dependent, unlike the modseq rule.
- IMAP `native_code` for an unknown response code is now the server's own
  spelling, which gives that telemetry field unbounded cardinality.
- Google `flags::eq_flag` now compares label flags (id and advisory name)
  case-sensitively, where the whole string used to compare ASCII
  case-insensitively. Probably intended; no test pins it.

## BUG-172 - Small wave 12 lateral findings across sync, net, Google, Graph, SASL and providers

**Reported by:** wave 12 fixers (lateral).

- Sync `engine/attach.rs`: cancelling `attach` can leave `lifecycle_inflight`
  occupied, because removal happens only after its awaited setup returns.
- bifrost-net `AccountNet::session_request` returns a general
  `RequestBuilder`, which still lets a caller explicitly re-enable redirects;
  the session policy is only documented, not enforced.
- Closed-account guards are not universal across provider methods: the shared
  `AccountClosed` kind replaced existing refusals, but many methods have no
  lifecycle gate at all.
- Graph `account/public_folder.rs` emits `Done` after some `Terminated`
  inventory events.
- bifrost-sasl SCRAM server-final parsing still accepts an empty `e=` and a
  message carrying both `e=` and `v=`.
- Sync `engine/attach.rs` `AttachingWriter` aborts the checkpoint writer task
  on any attach error or cancellation before slot installation; an abort landing
  inside a store await could interrupt a multi-statement durable write
  (`reset_account_for_schema_recovery`, `persist_established`). Closing the
  channel and awaiting the writer may be the correct teardown.
- dav-core `etag.rs` keeps `normalize_http_etag` and `prepare_if_match`, which
  restate strong/weak classification beside `bifrost_types::EntityTag`.
- `EntityTag::valid_http(false)` rejects a lowercase `w/` prefix while `parse`
  accepts it case-insensitively.
- Gmail: a malformed cursor payload now maps to `SchemaIncompatible` (account
  clear) through `CursorDecodeFault::Malformed`, where it used to be
  `CursorInvalid` with a scope restart. Matches the engine's stance on
  unreadable rows; confirm it is intended.
- `bifrost_types::compose::validate_send_recipients` builds its error without
  a protocol stamp, so empty-send refusals lost the `Protocol` JMAP's scheduled
  path used to stamp; it could take a `Protocol` argument.
- Graph `cursor_error_to_account_error` maps protocol mismatch, unknown envelope
  and schema incompatibility all to `CursorDecodeFault::Malformed`, losing which
  failure it was; JMAP `changes::stream` maps every `JmapCursorError::Other` to
  `ScopeMismatch`, so a future `Other` would silently restart a scope.
- `EntityTag::parse` classifies after `trim()` while `as_str` and `valid_http`
  use the untrimmed value (unreachable from current producers).
- Sync `reattach::run_establish` runs `pacing.wait` twice on a stored resume
  path, and once before an unreadable row is classified where it used to fail
  first.
- SMTP `TooManyFrom` is now unused (multiple From with one Sender is accepted)
  but is still a public error variant.

## BUG-173 - Small wave 13 lateral findings across sync, IMAP, Graph and JMAP

**Reported by:** wave 13 fixers (lateral).

- Sync `backfill/scope_walk.rs`: `ScopeWalkDriver::completed()` returns true
  before enumeration begins, so a caller can mistake eligibility for finished
  enumeration.
- IMAP `codec/decode/envelope_fetch.rs::gmail_id` documents unsigned 64-bit
  ids but parses with `number64`, which rejects values above the signed
  63-bit ceiling.
- IMAP `account/envelope.rs` cursor decoding still accepts a zero or oversized
  persisted MODSEQ, although request construction now refuses those values
  through the bounded `ModSeq`.
- JMAP `sync/mutation.rs` labels an unsupported `FlagOp` as `NotParsable`.
- JMAP state cache: an empty probe state can become an `ifInState` guard while
  set continuity rejects empty endpoints; the existing policy was preserved.
- Net and engine hold stores: the engine records a second hold for a net
  `LocalHold` error because nothing in `AccountError` marks local-hold
  provenance; removing it needs a provenance marker set by net and every
  provider error path. Harmless today (same deadline, same budget).
- bifrost-net `HoldKey`, `LimitKey` and `QuotaScope` lack `Ord`, so Graph turns
  its concurrency `LimitKey` back into a `String`; `QuotaScope` has both
  `Deref<Target = str>` and `as_str`, inviting cross-domain string comparisons.
- Sync `engine/ack.rs` `persist_ack_request`: on a `WalkNotWhole` completion
  refusal the claim's proof now lands only in the uncommitted staged candidate
  and is dropped by `discard_backfill`; debt is kept. Probably intended for a
  walk that was not whole, but not recorded as a decision at the code.
- Graph `account/batch.rs` computes the outer `$batch` replay policy from its
  legs, so unconditional PATCH and DELETE envelopes (bulk flags, extended
  property clears, thread deletes) moved from `ReplayPolicy::Never` to
  `Idempotent` and may now be resent by net when uncertain.
- IMAP `account/mod.rs::select_options` has an `.expect` on
  `QresyncParams::with_known` that would panic in production if a cached set
  were ever refused; returning the error is safer.
- IMAP `codec/classification.rs::untagged_routing_key` allocates and runs a
  fixed-point loop per pipelined command although the answer depends only on
  the `CommandKind`; a const table or `OnceLock` would remove it.
- JMAP `decode_error` now stamps `unreadable_success`
  (Acknowledged) on every decode failure at every operation, where only the
  Send boundary did before; this changes retry evidence for reads.

## BUG-174 - Small wave 14 lateral findings across sync, Graph, Google, JMAP, IMAP and SMTP

**Reported by:** wave 14 fixers (lateral).

- Sync `multiplexer/fusion.rs` can retire an undelivered change publication
  and still install its terminal cursor in memory, a delivery-loss recovery
  gap.
- Sync `engine/attach.rs::scope_covers_membership` compares folder strings
  without object type, so a type-qualified DAV `FolderType` scope can match a
  same-string mail membership.
- Graph EWS streaming frame accumulation (`ews/streaming.rs`) has no size cap.
- Graph `merge_shared_mailboxes` documents exact-string deduplication but
  compares case-insensitively.
- Graph `pim/messages.rs::delete_extended_property` builds a `$batch`
  envelope with no 20-leg bound.
- Google single-label mutations accept an empty patch before validating the
  target kind.
- Google `account/calendar.rs::recurrence_lines` writes RRULE text without the
  shared `bifrost_types` recurrence validation.
- JMAP `sync/pim.rs` turns an unrepresentable scheduled-send timestamp into an
  empty string; JMAP's event end calculation substitutes the start when date
  addition overflows.
- IMAP NOTIFY FETCH operands lack FETCH-equivalent capability admission, and
  `SearchCriteria::mod_seq(u64)` accepts values above the protocol ceiling.
- SMTP: in both drivers an OAuth mechanism incompatibility falls through to
  password mechanism selection.
- JMAP `sync/calendar_ops.rs::event_from_jmap` now fails on a malformed server
  duration; if list and range reads propagate it, one bad event fails the whole
  read instead of being skipped. An enterprise user expects the rest of the
  calendar to show. Check the callers.
- Sync `cursor/ledger.rs` `ErrorDigest` exposes the complete `DiagnosticInfo`,
  support-only text included, as public fields reachable through
  `SyncEngine::debt`; nothing enforces the consent tier before export.
- IMAP `types/capability.rs` `Capability::Auth` now carries
  `bifrost_sasl::MechanismName`, a type from the private sasl crate in a public
  IMAP enum, not re-exported from bifrost-imap.
- Google `error.rs` `RetryAfter { Delta, At }` is a private copy of
  `bifrost_types::RetryHint`, converted right after parsing.
- Sync `PagePublisher::publish_page`, `publish_marker`, `publish_unacked` and
  the `Arc::ptr_eq` asserts in `BackfillRunner::run_partition` and
  `emit_backfill_complete` guard wiring mistakes with `assert!`, `expect` and
  `unreachable!` on production paths; any would take down a sync task, and the
  lane-coverage identity they assume is not enforced by type.
- Net `RequestEvidence::from_ledger` overwrites the ledger built from nested
  histories with the caller's, so a nested MayHaveCommitted verdict missing
  from the supplied ledger would weaken (latent). `Error::RateLimited` and
  `RetryBudgetExhausted` take `retry_after_history.last()`, so a final 429
  without `Retry-After` reports an earlier attempt's hint.
- IMAP auth ladder rejection list names `LOGIN` twice (SASL LOGIN and the
  LOGIN command).
- Graph `calendar/recurrence.rs` keeps three weekday-name maps and
  re-validates WKST the shared parser already checked;
  `account/calendar_public.rs` hard-codes an unnamed `chunks(20)` for EWS SOAP
  batches.

## BUG-048 - IMAP sync lanes cycle on a `UIDNOTSTICKY` folder

**Reported by:** imap-account; reopened by the wave 1 review.

Inventory and changes on a `UIDNOTSTICKY` folder mint cursors the next SELECT
voids, so the scope cycles CursorInvalid -> re-inventory. Refusing the folder
(as the poller does) was tried and reverted: it would make the folder's mail
vanish from the user's view, which removes a capability. The remaining fix is a
cursor-free sync mode for such folders (re-inventory on each poll, no durable
UID cursor, no CursorInvalid loop); the inventory site and `reference/imap.md`
say so. It cannot be built inside IMAP alone: it needs a shared
`CursorEstablishment` variant in bifrost-types and engine scheduling of
repeated authoritative inventory (today `NoCursor` schedules no further poll),
plus safe hydration and mutation semantics for UID handles that do not survive
a re-SELECT. A wave of one owning types, sync and imap.

