# bifrost-google reference

Generic net translation receives the context throttle scope, falling back to
account scope, just like the provider status ladder. Response excerpts use
net's shared byte cap and truncation marker.

Every request declares its `ReplayPolicy`; the table lives at the call sites.
Label modify and absolute replacements use Idempotent. People updateContact
uses Never for its body etag; Calendar updates and RSVPs use Never for scheduling
side effects. Unabsorbed DELETEs are Never. Non-replayable requests stop at the
first ambiguous retryable status or in-flight drop; 401/429 refusals keep their
existing retries.

Current architecture of the Google Account-layer code under
`crates/google/src/account/`. Public surface is
`bifrost_google::account::{GoogleAccountFactory, PubSubConfig}`;
everything else is crate-private behind `Account` / `AccountFactory`.
The mail path uses a `GmailClient` for Gmail REST history-id sync,
Cloud Pub/Sub push, and flag canonicalization; People contacts and
Google Calendar ride alongside it.

Gmail has no UID model and no mailbox-scoped server state. A single
`historyId` walks the account-wide change log, and labels play the
role of folders: one `CursorScope::Account` cursor per account, a
`changes_stream` paging `users.history.list`, and mutations routing
through `messages.batchModify` / `batchDelete`.

## Gmail project quota holds

`GoogleAccountFactory::with_quota_project(id)` configures the canonical
quota-consuming Cloud project identity and rejects empty or whitespace-only
input. Use the project NUMBER everywhere, consistently across factories.
This is a grouping identity only: no billing or quota-project header is sent.
The budget identity and net gate key are both `gmail-project|ID`.

Error contexts explicitly distinguish Gmail, Calendar, People and Drive.
Only the Gmail surface can select the project budget. On Gmail,
`dailyLimitExceeded` means the project's daily API limit and remains
`Server(QuotaExhausted)`. `rateLimitExceeded` remains `Server(RateLimited)`;
Google's documentation disagrees on user versus project scope, so the client
conservatively treats that reason as project-wide. `userRateLimitExceeded`,
a concurrent-request 429 (even with a `rateLimitExceeded` reason), and
undocumented `quotaExceeded` remain account-scoped and never stamp the
project budget. Without a configured
project, throttling retains `ThrottleScope::Account` and has no project gate.
Calendar and People daily or rate errors never stamp or record it. Requests
select their API surface using the most specific configured URL path root,
including when redirected bases share an origin or nest under the Gmail base;
Gmail quota costs and concurrency use that same selection. Google rejects
cross-surface redirects with a terminal net `Error::RedirectRejected` before dispatching the
target; same-surface redirects continue through net's normal policy. A
request-level net redirect guard keeps the initial gate, observer, quota cost
and concurrency attribution valid for the whole redirect walk. This applies
to typed and raw builders, including the raw Drive session-creation POST;
pre-authenticated Drive chunk uploads and cancellation use the same guarded
builder funnel and disable redirects. Session creation accepts a `Location`
only on the HTTPS `www.googleapis.com/upload/drive/v3/files` surface (the exact
path or descendants, port 443, without userinfo). Chunk PUT and cancellation
validate that surface again before dispatch. A successful session-creation
response cannot authorize sending to Gmail, Calendar, People or another host. Thus Calendar/People cannot redirect into a held Gmail API, and
Gmail cannot record Calendar/People errors into its project hold.

The client observes each error response before net's retry decision and
resolves its deadline once. `Retry-After` retains both delta-seconds and
HTTP-date syntax; a delta is converted to an absolute time at observation.
A provider hint wins. Without one, a daily limit waits until the next civil
date's midnight in `America/Los_Angeles`, using jiff zoned arithmetic and a
bundled timezone database. This is neither a fixed UTC offset nor an addition
of 24 hours: transition days can be 23 or 25 hours. A project rate limit without
a hint uses a one-second stamped fallback. The response carries the resolved
deadline as `ServerCause`'s `RetryHint::At`, and net records the paired
monotonic deadline using its shared hold's maximum semantics.

Every Gmail request funnel attaches that gate: typed requests, raw builders,
`users.watch`, and `users.stop`. Label lifecycle polling and the watch renewer
therefore make no wire calls while held, including when a request was already
waiting for admission when another account recorded the hold. A local refusal
uses the hold's winning kind, stamps the same budget, retains prior attempt
evidence, and reports `RetryHint::After` from the remaining monotonic duration.
If a prior non-idempotent attempt was acknowledged, including a mutation that
returned a redirect, the refusal carries effective `InFlight` evidence and
reconciles through `ThrottledMidFlight`; it does not replay the mutation.
It never renews the deadline. Dropping all account handles and reopening on the
same Net does not erase the hold. Different project identities are independent.

`GoogleAccount::throttle_keys` declares the project budget for
`CursorScope::Account`, nonempty generic `ObjectId` work, and label memberships.
Those generic object hydration and mutation paths route only to Gmail;
Calendar and People have separate typed `EventId` and `ContactId` APIs.
Other cursor scopes and membership kinds declare no Gmail project budget.
The declaration and the stamped error therefore name the same engine bucket,
including across accounts on one project.

Holds are in memory only. Raising the provider quota early does not clear an
already recorded deadline; it expires naturally. Recovery under a daily hold
can end with "recovery wait budget exhausted" before midnight. That is intended:
the recovery lane is bounded, and exhaustion does not authorize another wire
attempt through an active project gate.

## Module layout

Public modules:

- `account` - public `GoogleAccountFactory` and `PubSubConfig`; the
  opened account itself is returned inside `OpenedAccount`.

Internal modules:

`crates/google/src/account/`:

- `mod.rs` - crate-private `GoogleAccount`, public
  `GoogleAccountFactory`, `impl Account`.
- `capabilities.rs` - `AccountCapabilities` builder.
- `cursor.rs` - `GmailChangeState`, envelope encode/decode,
  `cursor_from_state`.
- `scopes.rs` - `discover_cursor_scopes`, `discover_memberships`,
  `scope_lifecycle_stream`, `ScopeCache` / `ScopeSnapshot`.
- `inventory.rs` - inventory pass and `get_stream` hydration.
- `changes.rs` - history-id driven change stream.
- `push.rs` - Cloud Pub/Sub `watch`/`stop`, `PubSubConfig`, the
  `PubSubControl` watch actor, and the process-wide `MailboxWatch`
  registry that coordinates `users.stop` across instances of one mailbox.
- `mutation.rs` - `bulk_set_flags`, `bulk_move`, `bulk_destroy`.
- `pim.rs` - Phase 3.6 PIM primitives: message/thread label
  mutations, MIME send and drafts, search translation, container
  CRUD, identities, vacation responder, message/thread hydration.
- `filters.rs` - Gmail settings filter list/create/delete and
  typed-rule mapping.
- `flags.rs` - Gmail-label-to-IMAP-flag canonicalization and the
  reverse `LabelPatch` translation used by mutations.
- `blobs.rs` - `open_blob` / `open_blob_range` over Gmail
  attachments, plus `open_raw_rfc822` (whole message via `format=raw`,
  decoded by `inventory::raw_bytes`).
- `cloud.rs` - `host_attachment`: Drive resumable upload + sharing
  permission, one call. Session POST uses the raw `account_net()`
  builder (sets `X-Upload-Content-*`, which typed `post` cannot); the
  pre-authed chunk PUT skips auth (`.without_bearer_auth()`) and
  resumes on 308 via `Range: bytes=0-N`. A 308 never advances by the bytes
  just sent, so it can never silently skip a gap. An ABSENT `Range` on a 308
  is Google's documented "no bytes received yet" and reads as offset 0; the
  progress check below then refuses it (no progress from offset 0, or a
  regression after earlier progress), so it is never resumed past. The
  loop also bounds total chunk attempts to the expected chunk count plus
  eight, so malicious tiny progress cannot hold the caller forever. Then a
  `permissions` POST (`type: anyone` / `type: domain` + account domain)
  and a `webViewLink` GET. The chunk PUTs and the session cancel run with
  per-request `follow_redirects(FollowRedirects::Disabled)`, so every 3xx
  reaches this module as a plain response and nothing is ever sent to a
  `Location`. Drive's 308 carries a `Range` and no `Location` and arrives as
  progress; a 3xx WITH a `Location` is a real redirect the resumable protocol
  never uses and is `Protocol(ContractViolation)`, without echoing the
  server-chosen `Location`. The permission body, and so the account domain,
  is derived before the session POST: the account email is Gmail's own
  profile value, so one with no domain is `Protocol(ParseFailed)` with
  `AttemptCause(Unsent)`, refused with nothing sent rather than after the
  upload, which would leave an uploaded file nothing shares.
  The upload's local refusals are classified by what happened, and only
  those raised before any request blame the caller: a mismatched declared
  size or an empty payload is `Request(Malformed)`, refused before the
  session POST, so no session exists to cancel. The refusals that follow a
  complete response carry `AttemptCause(Acknowledged)`: a 2xx session response with no `Location`
  is `Protocol(MissingField)`; a `Location` that is present but not
  visible-ASCII text, or not an absolute http or https URL with a host, is
  `Protocol(ParseFailed)`, since accepting it would send every chunk PUT and
  the cancel to a bogus URL. An absolute URL outside the HTTPS Drive upload
  surface is `Protocol(ContractViolation)` and is refused before any chunk or
  cancellation is sent; a present 308 `Range` that does not parse
  is `Protocol(ParseFailed)`; a 308 claiming bytes never sent, fewer bytes
  than it already reported, or every byte without completing the upload is
  `Protocol(ContractViolation)`. A chunk that made no progress is legal
  (Drive documents resending from its reported offset), so refusing it,
  like the exhausted chunk-attempt budget, is this crate's own limit:
  `Internal(LimitExceeded)`, terminal `InternalFailure`. A misaligned chunk
  size or an empty payload reaching the chunk loop, and the loop ending
  with neither a file response nor a refusal, are unreachable by
  construction and map to `Internal(InvariantViolated)` with no attempt
  cause.
  A resumable session is server-side state that Drive holds for about a
  week, so any failure of the upload leg cancels its own session on the
  way out. Account shutdown interrupts the chunk loop and awaits that cleanup;
  session creation is allowed to finish so its response can supply the cleanup
  URI, and completed uploads finish their sharing leg. Cleanup uses one
  un-retried `DELETE` against the session URI, bounded by its
  own deadline (`GDRIVE_CANCEL_TIMEOUT`). Its success, failure or unconfirmed
  outcome is a typed compensation of the session step; it never replaces the
  primary error. 499, 404 and 410 confirm cancellation. An unfollowed redirect
  is a Failed compensation with its actual response error; an unadmitted URL
  sends no cancel and records no compensation. Session creation, accepted chunks,
  the completed file and the sharing permission are separate historical effects.
  Non-final failed chunks update UploadSession; a failed final chunk creates an
  incompletely identified CloudFile. A successful permission POST followed by a
  failed webViewLink GET has an effectless primary and a committed permission.
  These effects are Repeatable relative to hosting: replay may leave expiring
  sessions or unlinked files, so recovery follows the failed leg. No session URL
  enters scopes, and a failed cancel passes through
  `bifrost_net::Error::redact_request_text` before it becomes the compensation,
  so the transport's error text cannot carry the URL into an export. A successful session status without a usable
  Location raises application to Uncertain, because the session may exist.
  Dropping the future cannot await cancellation and spawns no detached cleanup.
- `error.rs` - translation boundary from `crate::Error` to
  `AccountError` via the central `AccountErrorBuilder`.

`crates/google/src/api.rs` is the Gmail wire wrapper used by these
modules. Every dynamic label, thread, message, attachment, draft,
filter, send-as, and page-token component is passed through
the matching `bifrost_net::url` path or query encoder before URL
assembly. The Drive hosting path applies the path rule to returned
file ids.

## GoogleAccount / GoogleAccountFactory

Consumers construct `GoogleAccountFactory` with
`from_access_token(token)` (raw) or `from_token_source(Arc<dyn
TokenSource>)` (shared source, read live per request), then optionally
attach a `PubSubConfig` via `with_pubsub_config`/`with_pubsub_topic`.
`from_token_source_with_net(source, net)` uses an application-owned `Net`,
including its transport, shared governor and registries; attachment still waits
for `open(account_id)`.
The factory is the only public entry point; the raw `GmailClient`,
Gmail wire DTOs, and crate-local `Error` are `pub(crate)`.

### Three independent API bases

The crate talks to three Google surfaces, and each base is configured
separately because none can be derived from another:

| Surface | Production base | Override |
|---|---|---|
| Gmail mail | `www.googleapis.com/gmail/v1/users/me` | `from_access_token_with_api_base` / `from_token_source_with_api_base` |
| People (contacts, directory) | `people.googleapis.com/v1` | `with_people_api_base` |
| Calendar | `www.googleapis.com/calendar/v3` | `with_calendar_api_base` |

Calendar shares a HOST with Gmail but not a path root, so it cannot ride the
Gmail base; People differs in both. All three compose, so a harness can redirect
any subset.

`RATATOSKR_TEST_GCAL_ENDPOINT` still sets the Calendar base when
`with_calendar_api_base` is not called. It is **legacy, kept working
deliberately**: downstream harnesses set it, and removing it would not fail
their builds - it would silently stop redirecting and send their test traffic to
the real Google Calendar API. It is now read ONCE per client construction
(`default_calendar_base`) rather than on every request. It was previously a
`std::env::var` read per call, which meant a `getenv` on a hot path,
process-global state read from inside a library, no way to point two accounts at
two Calendar endpoints in one process, and a bifrost crate naming its downstream
consumer in an identifier. An explicit `with_calendar_api_base` always wins; a
variable set after a client is built does not affect that client.

Rate limits are registered against the hosts derived from these configured
bases, not against literal production hostnames - a redirected base was
otherwise completely unmetered. Each declaration carries the engine
`AccountId` as its `quota_scope`, so Gmail's per-user quota is per account
rather than pooled across every account in the process that happens to share
`www.googleapis.com`. The same `(host, quota_scope)` key is registered once:
Gmail and Calendar share `www.googleapis.com` in production, and registering it
twice would install a second bucket for one key so the effective limit became
whichever registration won. Because this account declares exactly one scope per
host, its requests need no `RequestBuilder::quota_scope` override - the
account-level default resolves unambiguously. A base that fails to parse falls
back to the production host, so a malformed override meters the real host rather
than nothing.

### Gmail data concurrency

`GMAIL_DATA_CONCURRENCY` is an explicit local policy of 32, equal to the
hydration fan-out width (`HYDRATE_BATCH_SIZE`), not a discovered Gmail ceiling.
Gmail enforces an unpublished per-user concurrent request limit shared across
all clients of that user; no local bound can guarantee staying under it.
This bound prevents concurrent walks and scopes from multiplying the per-walk
width. `FullWithBlobs` issues two requests per hydration future, so even one
walk previously peaked near 64 data requests.

Each Gmail data request adds a weight-1 `RequestBuilder::concurrency_limit`
claim. `AccountNet::shared_concurrency_limit` supplies the handle from its
per-Net registry, keyed by `gmail:` plus the debug-formatted tuple of engine
account id and Gmail API base origin (scheme, host and effective port).
The client retains the handle, so overlapping clients for that key share it.
Typed requests and raw-builder Gmail mutations are covered. `users.watch`
renewal and `users.stop` are independent of this concurrency queue; rate
admission, token refresh and retries still apply. Calendar, People and Drive
remain ungated: they share the account transport, have narrow fan-outs, and
advertise no concurrency limit. There is no account-wide concurrency claim.

### Per-batch byte accounting

`GmailClient` carries an optional `ByteTally`. `metered()` hands back a handle
that is one `Arc` bump over the same client and reports every request's
request-local `bytes_in` into a fresh accumulator; each engine
stream takes one at construction and each emitted batch `take`s it, so
consecutive batches partition the traffic. The record sits at `send_recorded`,
the single point every buffered request leaves through, which is what keeps
`delete`, `post_no_content` and caller-built `execute_builder` requests counted
rather than free. It records from the request-local counter, not from the
`Response`, so a FAILED request contributes too: the mutation lane turns a
refused `batchModify` / `batchDelete` into per-item failures and still emits a
batch, and the `batchDelete` permission fallback's refused primary call is pure
error-path traffic.

This matters because one batch is routinely many requests: an inventory batch
covers a `users.messages.list` page plus up to 32 concurrent
hydration futures, whose requests share the Gmail data bound, and a mutation batch covers `batchModify` plus any
label refresh and per-id TRASH fallback. The two remaining zero-valued sites are
genuinely synthetic - `discover_cursor_scopes` returns a constant, and a
`discover_memberships` cache hit performs no request - and say so at the call
site. Blob batches keep their own exact transferred size and do not use the
accumulator. `open_raw_rfc822` takes its size from the request-local response
tally before decoding the base64url MIME field, so it reports the JSON
envelope bytes rather than the smaller decoded message. The Drive
resumable-upload session in `cloud.rs` builds its own
requests off `account_net()` and so sits outside the accumulator; it feeds no
`bytes_in` field, so no lane reports a number it did not measure.

`GoogleAccountFactory` carries an `Arc<GmailClient>` and an optional
`PubSubConfig`. `open(account_id)` asks the client for an
account-scoped clone attached to `bifrost-net` under the engine
`AccountId`, does one `users.getProfile` round-trip, parses
`profile.historyId` into a `u64` (a value that is present but not a number is
`Protocol(ParseFailed)`, not a missing field), and stores the
`GmailChangeState` as `seed_state`. The opened `GoogleAccount` retains:

- `client: Arc<GmailClient>` (crate-private REST wrapper).
- `capabilities: AccountCapabilities` snapshotted at open.
- `profile: GmailProfile` for `email_address` and history-id
  identity checks downstream.
- `seed_state: OpaqueChangeState` used by
  `establish_initial_cursor` to mint a cursor without a second
  network call.
- `pubsub: Arc<PubSubControl>` holding the watch actor command sender,
  shutdown token, and a `broadcast::Sender<WatchEvent>`. The actor task owns the
  optional config, lifecycle state, active-handle set, and its claim on the
  mailbox's shared watch exclusively. It is built from the profile's email
  address, the key under which instances coordinate `users.stop`.
- `scope_cache: Arc<ScopeCacheState>` for the shared label vocabulary used by
  canonicalization: an `RwLock<ScopeSnapshot>` for the snapshot plus a
  `tokio::sync::Mutex` that serializes refreshes and invalidations. Membership
  discovery returns a fresh cached snapshot without a request; stale reads
  re-check after taking the lock. Container listing still fetches the latest
  server view, but its write uses the same lock. Lifecycle diff state is
  held separately in account-owned `lifecycle_baseline` and never reads this cache.
- `lifecycle_baseline` retains the last observed label snapshot and undelivered
  lifecycle events across replacement streams.
- `shutdown: CancellationToken` for the watch actor, cancellable account
  streams, adapter-wrapped PIM, People, and Calendar futures, and Drive cleanup.
- `set_priority` / `set_bandwidth_cap` delegate to the underlying `AccountNet`.

Clients (`from_access_token` or `from_token_source`) retain their parent
`Net` without attaching a placeholder account. `open(account_id)` is
the first point that mints an `AccountNet`, under the engine id. A
failed open detaches that registration before returning the error.
`GoogleAccountFactory::from_token_source_with_net` accepts a consumer-owned
`Net`; callers still interact with opened accounts through the shared `Account`
trait.

`AccountFactory::open(account_id)` returns `OpenedAccount` with an
always-empty skip lane (single-namespace account; open probes only the
principal's own profile). `reopen`
flows from the engine: it drops the previous `Arc` and calls the factory
again with the same `AccountId`. The factory holds the credentials and
client, so the new `GoogleAccount` carries a fresh
`shutdown`/`pubsub`/`scope_cache` and reads the current profile at open. The
engine's reopen subscribes on the replacement before it tears down the old
handle, so the two instances briefly share one mailbox and one remote watch;
see the push section for how that is kept from killing the fresh watch.

`close()` is idempotent and cancellation-safe. It marks `closed` and cancels
`shutdown` synchronously, before the returned future exists, so the watch actor
and the push and scope-lifecycle streams are retired whatever the caller does
with that future. The future then makes a best-effort `users.stop` call for a
locally active Gmail watch, unless another instance in this process holds a
live watch on the same mailbox (see the push section) - it needs the
transport, so it cannot precede the detach - and clears the watch state; the `bifrost-net` detach runs from a drop
guard constructed before the future is returned, and therefore happens on
completion, on mid-poll cancellation, and when the future is dropped
without ever being polled. A stop
failure is classified and logged but cannot keep a closing account alive. The
one thing a dropped close future cannot guarantee is the remote half: Gmail
may keep delivering until the 7-day watch expires.
`Drop` also cancels and detaches as a fallback when a consumer omits
`close()`. The watch actor and `push_stream` both select on
`shutdown.cancelled()` and retire cleanly. The scope lifecycle stream selects
on it during both its polling wait and an in-flight `labels.list` request, and
stops before emitting queued lifecycle events. The token is also threaded
through inventory, changes, hydration and mutations. `repair_inventory`,
membership discovery and blob reads use the adapter's stream wrapper. PIM,
contacts and Calendar futures use its future wrapper. A cancelled active future
carries conservative in-flight evidence; event and contact updates explicitly
require reconciliation because a first leg may already have committed. The
generic wrapper deliberately adds no target scope: after an event move the
original scope may be wrong, and it has no operation-owned continuation state.
Drive hosting handles the token internally so shutdown of an active upload
awaits session cleanup rather than dropping the multi-request future.

## Capabilities

`gmail_capabilities()` in `capabilities.rs`:

- `cursor_freshness: CursorFreshness::ServerIssued`. The `historyId` is
  server-issued and monotone per account; trusted without a local clock.
- `blob_range: BlobRangeSupport::No`. Gmail attachments arrive base64url inside
  a JSON envelope with no HTTP Range surface; `open_blob_range` enforces it.
- `blob_digest_pre_download: false`. Attachment metadata carries
  no digest separate from the body.
- `push: PushCapability::OutOfProcessPubsub`. Push lives on Cloud Pub/Sub, not a
  connection bifrost owns; `push_in_process()` is false and consumers wire their
  own subscriber feeding `InvalidationSink`.
- `mutation.concurrency: ExpectedVersion` enables event update, delete and RSVP
  only. Gmail `batchModify` has no conditional history-ID primitive; versioned
  bulk mail refuses supplied tokens before I/O.
- `mutation.replay_safety: MutationReplaySafety::None`. No client-mintable
  dedup token; the shared `IdempotencyKey` is held engine-side.
- `mutation.reassertion: { set_flags: true, destroy: true }`. Gmail ids are
  immutable and `batchModify` / `batchDelete` converge when reapplied; the flag
  judge is described under "Flag observation and reassertion capability".
- `batching_policy: BatchingPolicy { max_items: 1000, max_wait:
  75ms, flush_on_input_close: true }`. 1000 = Gmail's `batchModify`
  cap.
- `rate_limit_class: RateLimitClass::Tiered`. Per-user quota units,
  not a uniform rps cap. Gmail publishes a per-user rate of 250 quota units
  per second; the Gmail host bucket is deliberately registered below it, at
  100 units/s with a 100-unit burst, so a backfill leaves headroom for the
  interactive traffic sharing the same per-user budget.
- `quota_signal: QuotaUnits`. Every Gmail request debits the published
  per-method cost, applied in `GmailClient::execute` and, for the raw batch
  mutation builder, by calling `GmailClient::gmail_quota_cost` directly.
  Method costs are transcribed from Google's published quota table (May 2026
  revision, rechecked August 2026) in `client.rs::gmail_quota_cost`, which
  carries the provenance note. A Gmail method absent from that table is billed
  at 20 units rather than 1, so an unchecked method cannot under-charge. The
  host `cost_default` of 1 unit applies only to the non-Gmail Google surfaces
  sharing the transport (Calendar, Drive, People).
- `requires_uidvalidity_recheck: false`. Gmail has no
  UIDVALIDITY model.
- `historyid_expires_after: None`. No documented `historyId` retention window;
  expiry is detected reactively via `classify_history_error` (404/410 ->
  `RestartScope`), not on a timer. `describe_cursor` reports
  `CostClass::Expensive` only when the cursor envelope no longer decodes against
  the open account's email-address; that flips `SyncStrategy::ServerCursor` and
  `freshness` to `None`, prompting re-establish.
- `delta_token_expires_after: None`.
- `pim_methods`:
  - Supported: `add_to_container`, `remove_from_container`,
    `set_label_membership`, `set_is_read`, `send_message`,
    `draft_create`, `draft_update`, `draft_discard`, `draft_send`,
    `search`, `search_messages`, `containers_list`,
    `container_create`, `container_rename`, `container_delete`,
    `identities_list`, `identity_update`, `vacation_get`,
    `vacation_set`, `thread_hydrate`, `message_hydrate`,
    `address_books_list`, `contacts_list`, `contact_get`,
    `contact_create`, `contact_update`, `contact_delete`,
    `contact_search`, `contact_autocomplete`, `directory_search`,
    `calendars_list`,
    `events_in_range`, `event_get`, `event_create`,
    `event_update`, `event_delete`, `event_rsvp`, `event_search`,
    `event_autocomplete`, and `host_attachment` (Drive hosting).
  - Unsupported: `set_keyword`, `set_category`,
    `set_extended_property`, `set_importance` (Gmail has no
    message-importance field), `attachment_upload`, `container_move`,
    `scheduled_send`, and `quota_get`. The Gmail REST API has no
    scheduled-send lever (web-UI only), so a scheduled `SendRequest` is
    rejected `Unsupported(Send)` before any wire call, and
    `cancel_scheduled_send` / `reschedule_send` are `Unsupported`.
    `send_as` is likewise false, so a `Some(send_as)` request is the
    `Unsupported(Send)` branch of the rule stated on `SendRequest::send_as`.
- `filter_rule_shape: Rules`. Gmail filters are wired for
  list/create/delete plus local validation; `filter_update` is
  unsupported (no update/replace endpoint).
- `conveniences.starred: LabelMembership`. The default
  `set_starred` convenience dispatches to Gmail's `STARRED` label.
  Replied and forwarded convenience flags are false because Gmail
  derives that state from messages rather than exposing a writeable
  flag. `mdn_sent_via_keyword` is false: Gmail's read-receipt model is
  read-only, so `mark_mdn_sent` returns `Unsupported(MarkMdnSent)`.
  Hydrated `Message.importance` is always `Normal`.

## PIM primitives and conveniences

`pim.rs` implements the S1-W2 Gmail shape for the unified Account
trait.

Mail mutation primitives use Gmail label modification:

- `add_to_container` and `remove_from_container` dispatch to
  `users.messages.modify` or `users.threads.modify` depending on
  `MutationTarget`.
- `set_label_membership` is the same add/remove label operation.
- `set_is_read` flips Gmail's `UNREAD` label with inverted polarity.
- `set_keyword`, `set_category`, `set_extended_property`, and
  `set_importance` are `Unsupported` (see the capability list above).
- The Archive container is synthetic: adding to Archive is lowered as
  a relocation (see below), never as a label to apply; removing from
  Archive is a no-op (archive is the absence of a display container,
  not a native label).
- `move_thread` is ONE `threads.modify`: the add and every removal
  (the exclusive display containers plus the caller's `source` label)
  ride the same request, so there is no window where the thread sits
  in both containers.

One relocation rule, shared by `flags::move_placement_patch` between
the bulk `batchModify` driver and the single-object builders in
`pim.rs`, so a consumer cannot get different wire semantics depending
on which entry point it reached. `INBOX` / `SPAM` / `TRASH` are Gmail's
mutually exclusive display containers: Gmail renders a message under
whichever of them it carries, whatever else is also attached. "Move
into `destination`" therefore lowers to *add `destination`, remove
every exclusive container that is not `destination`*:

- `INBOX` -> add INBOX, drop SPAM + TRASH (un-spam / un-trash).
- a user label -> add it, drop INBOX + SPAM + TRASH, so filing a
  spammed or trashed message actually takes it out of Spam / Trash.
- `archive` -> add nothing (it is not a Gmail label id, and Gmail
  rejects a modify that tries to apply it), drop all three.
- `SPAM` / `TRASH` -> add it, drop the other two.

Case-insensitive matches for the three system destinations are normalized to
their canonical Gmail wire spelling before entering `addLabelIds`; user label
ids retain the caller's exact casing.

An optional `source` is the one part the destination cannot imply - a
user label being filed out of - and it joins the same
`removeLabelIds`. It is skipped when it is the synthetic `archive`,
equal to the destination, or already implied.

Composition renders MIME through the shared `bifrost-types::mime`
serializer (`render_rfc5322`, shared with IMAP send) and sends the
base64url message through Gmail. `MailDocument` keeps Gmail
orchestration (draft-patch apply, hydration) and emits `Bcc:` so Gmail
learns blind recipients:

- `send_message` calls `users.messages.send`. Inline attachments are
  encoded into the MIME tree. Pre-uploaded attachment handles are
  unsupported (Gmail has no separate message-attachment upload).
- `draft_create`, `draft_update`, `draft_discard`, `draft_send` call
  Gmail drafts endpoints. `draft_update` fetches the draft in `full`,
  projects editable fields into `MailDocument`, applies the patch, then
  replaces the draft with the re-rendered raw MIME.
- `attachment_upload` returns `Unsupported`; large over-limit
  attachments use `host_attachment` (`cloud.rs`) -> Google Drive.

Search translates the shared `SearchRequest` AST into Gmail query
strings and uses `users.threads.list` for thread-shaped search and
`users.messages.list` for message-shaped search. `provider_query` is
appended verbatim so consumers can use Gmail-specific operators such
as `larger:5M`. Both list endpoints explicitly send
`includeSpamTrash=true`; inventory and search therefore see the same
first-class Spam and Trash containers that history changes can name.
The inventory page walk calls the same `mail_list_query` builder as
search, including its page-token query encoding.

Container CRUD treats Gmail labels as `ContainerKind::Label` and returns
native label ids. System labels `INBOX`, `SENT`, `DRAFT`, `TRASH`, `SPAM`
map to the matching `FolderRole`; Archive is a synthetic label-shaped
container with native id `archive` and `FolderRole::Archive`. Create,
rename, and delete call Gmail label endpoints. Each dispatched label mutation
marks the shared vocabulary stale afterward, including an attempt whose
response is ambiguous, so later flag translation refreshes before using the
old label list. Moving containers is unsupported because Gmail labels are
flat.

Settings primitives map to Gmail settings endpoints:

- `identities_list` and `identity_update` use
  `users.settings.sendAs`. Gmail stores the signature as HTML; when only
  `IdentityPatch.signature_text` is supplied, the client escapes its text and
  converts line breaks to `<br>` before writing. An explicit
  `signature_html` patch is sent as HTML unchanged and takes precedence.
- `vacation_get` and `vacation_set` use
  `users.settings.vacation`.
- `quota_get` returns `Unsupported`; the Gmail API profile exposes
  message counts but not storage quota bytes.

Hydration primitives use Gmail's `full` and `metadata` formats.
`thread_hydrate` calls `users.threads.get` and projects each message.
`message_hydrate` picks the cheapest format for the requested
`HydrationProjection`, parses common address and threading headers, maps
label ids to containers and canonical flags, and surfaces attachment blob
handles for `FullWithBlobs`. Address-list headers use the shared
`Address::parse_list` for ordinary hydration, including quoted-pair handling
and RFC 2047 display-name decoding; rejected items are omitted from this read
projection. `draft_update` reparses From, To, Cc, Bcc, and Reply-To with
`Address::parse_list_checked` before fetching inline attachments or writing.
Any rejected item returns `Request(Malformed)` and leaves the stored draft
untouched, so an update never writes a shortened address list. Accepted
addresses preserve quoted commas and escaped quotes without introducing
phantom recipients or re-encoding encoded-words.

Google Calendar maps shared lifecycle status, availability, visibility,
attendees, and recurrence on create/update. Organizer is server-derived
for created events; create payloads carrying a shared organizer are
rejected as unsupported rather than silently dropped.

The Gmail overrides for multi-call conveniences are:

- `move_thread` adds the target container first, then removes the
  source container when supplied. The implementation lives in the
  crate so it can own a cloned client inside the boxed future.
- `delete_thread` moves to `TRASH` unless the caller says the thread
  is already in Trash, in which case it calls `users.threads.delete`
  for permanent deletion.

Other conveniences inherit the trait default. `set_starred` routes
through `set_label_membership` because capabilities advertise
`LabelMembership`. `apply_label` and `remove_label` use the default
provenance dispatch for Gmail label ids. The synthetic `archive`
container is label-shaped for round trips, but applying it is the
documented relocation operation: it removes INBOX, SPAM, and TRASH
rather than adding a native Gmail label.

People contacts use `people/me/connections`, `people:get`,
`people:createContact`, `people:updateContact`, and `people:deleteContact`.
Auto-collected addresses (People `otherContacts.list`) are surfaced as a
distinct corpus: a synthetic read-only address book `google:other-contacts`
(no create / update / delete) appears in `address_books_list` carrying
`ContactCorpus::OtherAutoCollected`, and a `contacts_list` scoped to that
book id routes to `/v1/otherContacts` (read-mask limited to
names / emails / phones / metadata), stamping each card
`OtherAutoCollected`. Every other book, group, and card is
`ContactCorpus::Main`; `contact_get` derives the corpus from the resource
name (`otherContacts/*` -> auto-collected). The discriminator lets the
consumer route the two corpora to distinct local stores without matching
on the provider.
`contact_create` requires the successful response's `resourceName`; an absent
field is `Protocol(MissingField)` and an empty value is `Protocol(ParseFailed)`,
both with acknowledged response evidence.
Update fetches the raw `Person`, preserves its CONTACT source identity and
requires that source's ETag, and replaces only
fields named by the shared `ContactPatch`. Display-name updates rewrite the
first modeled name's given/family split while preserving unmodeled People name
fields and additional name entries. Custom email and phone labels use People
`type=custom` plus `formattedType`; postal addresses map through the shared
`ContactAddress` model. Contact search sends the People empty-query warmup
before `searchContacts` so the cache is populated after mutations; a transient
warmup failure is ignored because it only primes the cache, while terminal
failures propagate. If modeled fields are acknowledged but a requested photo
update or deletion then fails, the error records the committed Repeatable
Updated contact effect with the photo request as its own Updated primary.
Application comes from the photo leaf; the original kind, scope, transmission,
recovery and timing are preserved. Replay refetches source metadata and converges
on the requested values; versioned photo updates are refused before I/O.
The account
exposes the synthetic `google:contacts` address book plus People
`contactGroups.list` groups. List/search accept no id, `google:contacts`, or
`contactGroups/*` ids; group-scoped queries filter returned contacts by
`contactGroupMembership`, and group-scoped creates include the matching People
membership. Contact photos are read-only through the shared `photo_url`; URL
update attempts return unsupported because People `updateContactPhoto` requires
image bytes, not a URL. Raw `ContactPatch.photo` updates call
`updateContactPhoto` (its field mask travels in the body, not the URL), and
clearing that field calls `deleteContactPhoto`.

Both `pim_methods.directory_search` (non-empty lookup) and
`pim_methods.directory_enumerate` (empty-query enumeration) are true.
Directory pages default to `truncated: false`; continuation remains in
`next_cursor`.

`directory_search` (organization directory / GAL, distinct from the personal
`contact_search` corpus) returns `Page<DirectoryCard>`. An empty query
enumerates via `people:listDirectoryPeople` (no warmup); a non-empty query runs
`people:searchDirectoryPeople`, preceded by an empty-query warmup like the
`searchContacts` family. Both use `sources=DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE`
and a `names,emailAddresses,phoneNumbers,organizations` readMask; email-less
rows are dropped, the first email is the key and the rest fill
`additional_emails`. Accounts with no directory (personal Gmail, or missing the
`directory.readonly` scope) answer 403; the impl swallows the directory-absence
classifications (`PermissionDenied`, `InsufficientScope`) to an empty page,
while `PolicyBlocked` (a real admin refusal) propagates.

Google Calendar RSVP fetches the raw event, updates only the matching
account attendee's `responseStatus`, and sends the attendee array back
with unmodeled attendee fields preserved. It selects the provider attendee
marked `self: true`, falling back to a case-insensitive profile-email match
when Google omits that marker; if no account attendee is present, RSVP is
`Unsupported(EventRsvp)` because the event state offers no target. `Delegated`,
`Unknown`, and future shared RSVP statuses are refused as
`Unsupported(EventRsvp)` before the event is fetched because Google has no
safe `responseStatus` mapping for them. Event status maps through Google
Calendar `status` on read, create, and update; an absent `status` reads as
`Confirmed` (Google's documented default), so `Unknown` means only an
unrecognized value. Event search sends `showDeleted=true` only when
`EventSearchRequest::include_cancelled` is set (default `false`, where Google
hides cancelled events), on both the single-calendar and cross-calendar paths;
the cancelled stubs that returns cross the tombstone-tolerant projection
described below. Event search uses the
requested calendar when supplied; otherwise it walks the calendar list and
searches each, using an internal cursor that records the calendar id plus
the provider page token. `calendars_list` walks every `calendarList` page at
the provider's 250-item maximum, rejects repeated page tokens, and has a
finite 10,000-page request budget. It returns the whole `Vec<Calendar>`
rather than a `Page`, deliberately: the walk is bounded, a real account is
one page, and the trait reserves `Page` for corpora that are large by nature
(`contacts_list`). Each page is one ordinary request
against `www.googleapis.com`, so it debits the host `cost_default` of one
unit through the same governor as before; the paginated walk costs units
in proportion to the pages the provider actually hands back, and no
Calendar traffic rides for free. Calendar ids occupy path components on
event resource URLs, while the `events.move` destination occupies a query
value and uses the corresponding encoder. The two encoders differ only on
the complete `.` and `..` components, which the path encoder
double-escapes so the URL parser cannot resolve a provider id as path
navigation; in a query value dots carry no structural meaning. Cross-calendar
search respects the requested `limit`, never over-fetching at a calendar
boundary and clipping any loose provider page. `events_in_range` requests
`showDeleted=true` alongside `singleEvents=true`, so cancelled recurring
instances remain visible as `EventStatus::Cancelled` instead of disappearing
from a range reread. Google may return such tombstones with only
`originalStartTime`; the projection uses that value for both time fields so
the stable instance id and cancelled status can cross the shared
`CalendarEvent` surface. A cancelled STANDALONE event is a different shape
again - a bare stub carrying an id and `status: "cancelled"` and no `start`,
`end` or `originalStartTime` at all - and it projects with an empty-valued
`start` and an `end` of `None` rather than being refused (a cancelled event
with a start and no end likewise has no end, not a copy of its start): a deletion notice is
not a malformed event, and it is the notice `showDeleted=true` was asked
for. The tolerance is gated on the cancelled status. A LIVE event the
provider sends without times is still a projection error
(`Protocol(MissingField)`), and that includes a live event with a start and
no end: Google documents that an end is provided even when the event's end
is unspecified (`endTimeUnspecified: true`), so projecting a missing one as
zero-length would invent a duration the provider promised to state. The one
exception is `endTimeUnspecified: true`: there the provided `end` is a
placeholder, not a real end, so the projection discards it and
`CalendarEvent.end` is `None`, and a missing `end` on such an event is not
refused. A read-modify-write that echoes `None` into `EventPatch.end` sends no
end at all. `endTimeUnspecified` is not a writable Events property, so "no
end" cannot be written, and an empty (or whitespace-only) `end` a caller passes
is never sent as `dateTime: ""`. A patch carrying one omits `end`, leaving the
stored end untouched, so a write neither fails nor fabricates an end. That holds when the patch also
carries `start`: an echoed unchanged start cannot be told from a move without
fetching the event, and a real move past Google's stored placeholder end is
refused by Google itself. A create with an empty `end` is refused as
`Unsupported` before any request: an insert needs some end, and inventing one
is what the projection refused to do, so the caller must supply it. An
empty `start` has no "no start" reading and is refused as `Request(Malformed)`
naming `start`. All of this runs in `reject_unusable_write_times`, before any
request and, on update, before the calendar move, alongside the all-day check.
The same guard checks the SHAPE of every non-empty start and end, as
`Request(Malformed)`: a bare `YYYY-MM-DD` date for an all-day value (the write's
`is_all_day`, or each value's own shape when a patch leaves it unset), an RFC
3339 timestamp with an offset for a timed one, or a local date-time beside a
`timezone`. Google would answer a bad value with a 400, which already
classifies as the caller's malformed input with nothing sent in error, and that
is honest on a create or a plain field patch. The local check exists for the
update that also moves the calendar: `events.move` has already succeeded when
the field patch is rejected, so the common malformed time shapes are refused
before the first leg. Accepted limit: the `timezone` NAME is not validated (no
time-zone database is carried, and a false refusal is worse than Google's
400), so an unknown zone combined with a calendar move can still leave the
event moved with a failed destination patch. That PATCH error retains its leaf classification and records the Blocking
Moved effect plus the destination Updated primary.
RSVP patches only `attendees` and re-sends no time.
Projection failures are per item, not per page. `page_from_events` routes a
refused event onto `Page::failed_ids` under its composite native id and
serves the rest of the page, matching how bifrost-jmap and bifrost-caldav
treat per-item conversion failures; collecting through `Result` previously
let one item destroy every neighbour in the page and left the walk no way
past it. Cross-calendar search concatenates the per-calendar `failed_ids`
into its aggregate `Page` rather than zeroing the lane. `is_all_day` is read off that EFFECTIVE start rather
than off `event.start`, because a cancelled instance of an all-day recurrence
carries only `originalStartTime.date` - reading `event.start` alone paired
date-valued times with `is_all_day: false`, which the shared surface treats as
a timed event. Calendar update
uses `events.move` when `EventPatch.calendar_id` targets a different
calendar, then applies any remaining field patch against the destination.
Patch-local validation runs before the move, so a patch that could never
be applied cannot strand the event in the destination calendar.
That provider operation is non-atomic. If the move succeeds and the field
PATCH fails, the returned error preserves the PATCH's leaf classification,
chain, hint and destination scope. A committed Blocking Moved step names source
and destination Event ids; the Repeatable Updated primary names the destination.
The operation recovery protects replay with PartialCommit, CheckTarget and
ObserveOnly for retryable leaves, while leg_recovery preserves the PATCH advice.
Auth/permission and terminal leaves retain their class with the record.
No compensating move is issued.
A present `EventPatch.recurrence` always writes the `recurrence` key; an
empty `EventRecurrence` sends `[]` (the Google idiom for clearing
RRULE/RDATE/EXDATE), while an absent recurrence patch omits the key.
Google carries the timed/all-day distinction in the `start`/`end` shape
(`date` vs `dateTime`), not a flag, so an `is_all_day` flip is rejected
as `Unsupported(EventUpdate)` unless the patch also carries both `start` and
`end` (`reject_unexpressible_all_day_patch`). `Unsupported`, not
`Request(Malformed)`: the patch is valid shared input that other providers
apply as it stands (CalDAV re-emits the stored DTSTART/DTEND under the new
value type, JMAP sets `showWithoutTime`); it is this implementation that
cannot express it without reading the current bounds, so a `ClientBug`
classification would be false.
Create and update attendee payloads accept only Google's `accepted`,
`declined`, `tentative`, and `needsAction` response statuses. Shared
`Delegated`, `Unknown`, and future statuses are refused as
`Unsupported(EventCreate | EventUpdate)` before any request; an update checks
them before moving the event so it cannot strand the event after the move.

Calendar event updates and `event_delete_if_version` accept caller ETags via
`If-Match`. A stale token maps to `ConcurrencyConflict`; unsupported token
kinds and versioned relocations are refused before I/O. No-version event
writes keep their previous behavior. `event_rsvp_if_version` reads the attendee array but preserves the caller's
ETag on the resulting PATCH, even if the GET returns a newer tag. A stale
caller therefore conflicts. Ordinary `event_rsvp` keeps its existing
unconditional behavior and lost-update window.

## Cursor envelope

`GmailChangeState` carries:

- `history_id: u64`.
- `profile_email: String`.
- `schema_version: u8` pinned at `1`.

`encode_gmail_state` serializes to JSON and wraps as
`OpaqueChangeState { protocol: ProtocolKind::Gmail,
envelope_version: 1, bytes }`. Serialization is an invariant boundary:
the data-only state must serialize, and an impossible encoder failure
panics instead of minting an empty poisoned cursor. `decode_gmail_state` rejects
wrong protocol, wrong envelope version, and wrong schema
version, each translating to `SyncState(SchemaIncompatible)`.
A payload that fails JSON deserialization translates to
`SyncState(CursorInvalid)`.

`decode_gmail_state_for_profile` layers an identity check on top: the decoded
`profile_email` must equal the open account's `email_address` under an
ASCII-case-insensitive comparison. A mismatch
returns `GmailLocalError::AccountIdentityMismatch`, translated to
`SyncState(SchemaIncompatible)` with support-only text naming both sides,
preventing a checkpoint
from one Google account being replayed into another (for example after the
consumer rotates accounts under the same persistence key).

`cursor_from_state` projects to
`ChangeCursor { scope: CursorScope::Account, server_state,
advanced_through: None, envelope_version: 1 }`. There is no
`advanced_through`; Gmail does not page over time intervals.

## Per-scope inventory / changes / hydration

`CursorScope::Account` is the only scope. `inventory_stream`,
`establish_initial_cursor`, `push_subscribe`, and the change
stream each reject any other scope with a
`SyncEvent::Terminated(AccountError)` whose kind is
`Unsupported(_)` and recovery is `Unsupported(_)` - the central
recovery mapping resolves an `Unsupported` kind to the terminal
`Unsupported` `RecoveryClass`.

`inventory_stream` samples `users.getProfile` before the first list
page, then walks `users.messages.list` in pages of 500 ids and
hydrates each page through `users.messages.get` with the `metadata`
format under `buffer_unordered` concurrency of 32. Page boundaries are
`PageBoundary::Page` for intermediate batches and `PageBoundary::Final`
for the last batch. The final batch is emitted even when it has no items,
so an empty mailbox or a terminal page whose listed messages all vanished
still exposes the boundary and checkpoint. A message-level
`NotFound(Message)` during hydration is discharged as the ordinary list/get
deletion race - the id came from this walk's own listing and the cursor is
anchored before the walk, so absence is the correct inventory state. Every
remaining object-attributable permanent hydration failure becomes an
`InventoryObligation::Object` (key `gmail:message:<id>`, stable across
walks so the same unreadable message re-raises the same key; no
provider-native repair token, since a Gmail message is re-readable from
its id alone) and the walk continues. A failure whose `RecoveryClass` requires
stream-level action, including retry, reconciliation, engine recovery, auth
loss, or account authorization repair, terminates the walk and discards the
unfinished page. Inventory hydration uses the same recovery predicate as the
mutation lane. Terminating on an object-attributable permanent failure would
discard every page already emitted for one unreadable object, then hit the same
object on the retry, forever. Each emitted batch and the terminal
completion carry an `InventoryCoverageReport` over
`CoverageDomain::full(scope)`: complete while no obligation exists,
degraded and naming every unresolved obligation from the first one onward
- a checkpoint certifies the results before it, so a page checkpoint
claiming `Complete` would let the cursor advance past an object nothing
recorded. A LIST-page failure (as opposed to a per-object hydration
failure) still terminates the walk.

The checkpoint that final batch and the terminal `Done` carry is
derived from the pre-walk profile sample, and **the engine does not
read it**. Gmail answers `establish_initial_cursor` with
`CursorEstablishment::Ready`, not `EstablishViaInventory`, so the
change cursor is anchored at `open()` from `seed_state` and
`changes_stream` starts from it immediately, in parallel with the
inventory walk - a message that races the walk is caught there, not by
anything inventory emits. Gmail's inventory therefore only ever feeds
the backfill path (the default `inventory_partition_stream` resolves
`InventoryPartition::Full` to `inventory_stream`), and
`BackfillRunner::run_partition` substitutes its own
`Checkpoint::Backfill` on every page and discards the terminal `Done`
checkpoint outright.

Sampling the profile before rather than after the walk is kept because
it is the honest ordering for a value that claims to describe the start
of the pass, but it is presently inert: nothing downstream consumes it.
Emitting a `Checkpoint::Change` at all is a mild deviation from the
`inventory_stream` contract, which reserves the terminal checkpoint for
scopes that returned `EstablishViaInventory`. Revisit both if Gmail
ever moves to inventory-established cursors.

Inventory and hydration resolve the label vocabulary through
`labels_for_flags` before canonicalizing flags, never through
`snapshot` directly. `ScopeSnapshot.fetched_at` is an
`Option<Instant>`, so a cache that has never been populated is stale
regardless of age while a successful fetch that returned no labels is
fresh for the usual five minutes. If the initial refresh fails with
nothing cached, the operation fails rather than canonicalizing against
an empty vocabulary; a populated stale cache stays usable when a later
refresh fails.

`scope_lifecycle_stream` shares an account-owned baseline and pending event
queue, independent of the label vocabulary cache. The first successful label
fetch seeds the baseline without emitting Created events. Later polls diff
against that baseline, so cache refreshes by other operations cannot consume
changes. Both the baseline and queued events survive replacement streams;
dropping a stream halfway through a diff does not discard the remaining events.
Rename events carry the stable label id and both display names.

The first poll runs immediately and successful polls use a thirty-second cadence.
Every classified poll failure emits `ScopeLifecycleEvent::Terminated` once and
ends the stream. The engine owns reconnect backoff, retry hints and throttle
holds. Waiting and in-flight reads select against the account shutdown token.

`get_stream` consumes a stream of `ObjectId`s in batches of 32
and emits `AccountStream<SyncEvent<ItemOutcome<HydratedObject>>>`.
Per-item hydration outcomes flow as `ItemOutcome::Succeeded` for a
hydrated message and `ItemOutcome::Failed(BatchFailure)` carrying
classified object-attributable permanent `AccountError`. A recovery class
that requires stream-level action terminates the stream instead of blaming
one object; resolved sibling outcomes are emitted before that terminator.
A single object-attributable failure no longer poisons the stream. The
boundary a hydration batch carries is derived only from what draining
the id stream already revealed: a batch cut short because the id stream
closed is `PageBoundary::Final`, every other batch is `PageBoundary::Page`,
and an empty input emits only `Done(None)`. A last batch that happens to
fill exactly to the 32-id hydration width therefore stays `Page`, with the
terminal `Done(None)` as its only end marker. This is deliberate and must
not be "fixed" with a one-item lookahead: the ids arrive from a
backpressured producer that may be waiting on hydration output before it
yields again, so polling for one more id before hydrating the batch
already in hand deadlocks the two sides against each other. The
dispatch per `Projection` is:

- `FlagsOnly` - `format=minimal` then `flag_set` over the label list.
- `Metadata` - `format=metadata` via `inventory_entry_from_message`.
- `FullWithBlobs` - `format=raw` for bytes plus `format=full` to
  enumerate attachment blob handles, the two issued CONCURRENTLY
  (`future::try_join`) since neither depends on the other's answer.
  Both calls are irreducible: `raw` is the only format returning the
  original MIME bytes and returns no `payload` part tree, while `full`
  is the only format returning the part tree carrying
  `body.attachmentId` - an id Gmail mints that appears nowhere in the
  message's own bytes, so it cannot be recovered by parsing the raw
  MIME, and `format=metadata` returns headers without parts and cannot
  supply it either. The pairing is also cheaper than it looks: `full`
  inlines `body.data` only for parts WITHOUT an `attachmentId`, so a
  20 MB attachment is not transferred twice; the duplication is the
  inline text bodies. The real cost is the second call's 5 quota units.
- `Headers` / `Preview` / `TextOnly` / `Full` - `format=raw`, with
  `HydratedObjectKind::RawMime`.
- Any other variant falls back to `format=metadata`.

A hydration batch runs up to 32 hydration futures (`buffer_unordered`),
with their data requests sharing the account's Gmail bound of 32,
matching the fan-out `inventory_stream` already used, and each future carries
its own id alongside its result so a per-item failure still names the object it
could not represent. The label vocabulary is resolved once per batch and
projected into a `LabelNameIndex` (id -> name) once, rather than rebuilt inside
`canonical_flags` for every message; `flags::canonical_flags` and
`flags::flag_set` remain as the by-slice entry points over the same index.
The label refresh runs after the id drain and before any
`users.messages.get`, so a refresh failure means not one id of the batch was
transmitted. The stream emits `SyncEvent::Terminated` alone - no per-id lanes,
because nothing was attempted for them and `Terminated` means unreported - and
the error is scoped `ErrorScope::Account` with resource `Label`, not to an
arbitrary first id. Scoping it to `ids[0]` named a message the failing
`labels.list` never mentioned and said nothing about the other 31; there is no
message-collection `ErrorScope` to name the batch as a whole, so the
account-wide resource that actually failed is the honest scope. The operation
stays `HydrateMessage` so the failure is still attributable to the hydration
lane. `labels_for_flags` is single-flight: a stale cache is refreshed under a
`tokio::sync::Mutex` in `ScopeCacheState`, with the staleness re-checked after
the lock, so 32 concurrent hydrations issue one `labels.list` rather than 32.

### Shutdown observation

`inventory_stream`, `get_stream`, `changes_stream` and the mutation driver all
take the account's `CancellationToken` and select on it around every provider
call and every wait on an input stream. Without it, a stream that outlived
`close()` kept calling Gmail through a deregistered `AccountNet` - unmetered,
and reporting the result as a transport fault rather than as a closed account.
The adapter also wraps `repair_inventory`, `discover_memberships`, blob reads,
and the network-backed PIM, People and Calendar futures with that token. A
wrapped stream ends silently on close. A wrapped future called after close is
refused before its inner operation is polled; cancellation after it has been
polled returns `Transport(Network)` with `AttemptCause(InFlight)`, because the
adapter cannot inspect the request's progress. The operation identifies the
recovery lane, but this adapter error does not retain a resource-specific scope.
For `EventUpdate` and `ContactUpdate`, shutdown overrides idempotency to false:
these composite writes may already have committed a move or contact fields
before their second request is interrupted. Their generic shutdown error requires
reconciliation (`TransportDropAfterSend`, `CheckTarget`), never a plain retry of
the original input. Unlike a provider error handled inside the update path, this
adapter error has no destination continuation; callers must inspect provider
state, including the requested destination calendar, before continuing.

The rule for the READ streams is that cancellation ends them SILENTLY: they
return without a `Done`, a `PageBoundary::Final`, a checkpoint or a coverage
report. A dropped in-flight read loses nothing, and the one thing that would be
unsafe - a partial walk published as complete - is exactly what is excluded.
Hydrated items already collected for an unfinished page are discarded with it.

The mutation driver cannot follow that rule, and does not; see the mutation
pipeline below.

`changes_stream` decodes the cursor, re-verifies
`profile_email`, then `users.getProfile`-checks that the open
account still matches the cursor's recorded identity. A drift
yields a `SyncEvent::Terminated(AccountError)` with kind
`SyncState(SchemaIncompatible)`, which the central mapping
resolves to `Engine(SchemaIncompatible)`. Email identity comparisons
are ASCII-case-insensitive in both cursor decoding and the live profile
check - `getProfile` returning different casing must not wedge the account into
a permanent `SchemaIncompatible`-clear-re-establish cycle.

This per-poll round trip is **kept deliberately**, and the cost objection to it
has already been raised and answered. It genuinely doubles the request count and
the failure surface on the 30-second poll path. It is also the only thing today
that catches a rotated token now pointing at a DIFFERENT Google account before
that account's history is mixed into the existing slot, and there is no
token-source identity binding upstream to lean on. It comes out when that
binding exists, not before; do not re-file it as free savings.

With the identity confirmed, the stream pages `users.history.list` from
`startHistoryId`. Intermediate pages emit `checkpoint: None`, because Gmail's
opaque page token is not represented in the cursor. Each response `historyId`
is the mailbox's current position when that page is requested, not a marker for
how far the response snapshot has been consumed. The walk therefore retains the
first page's `historyId` and uses that value for the final
`Checkpoint::Change`. Records arriving while later pages are fetched may replay
on the next walk, but cannot fall below a checkpoint the walk did not earn. The
stream then terminates with `SyncEvent::Done(None)`.

History entries map to `Change` variants in
`changes_from_history`:

- `messagesAdded` -> `ObjectChange::Created` plus one
  `ScopeChange::Added` per label on the new message.
- `messagesDeleted` -> `ObjectChange::Destroyed`.
- `labelsAdded` / `labelsRemoved` -> `ScopeChange` rows scoped
  per label.

## Paging inventory

Google has no universal paging helper, but every internally traversed walk has
its own repeated-token detection and 10,000-page refusal budget. Where a page
trips both guards, the repeated-token check is evaluated FIRST and owns the
diagnostic: it names a provider contract breach, where the budget only reports
"too many pages", which is also what an honestly huge corpus looks like. Both
refusals end the walk with a terminal recovery class, so the order is purely a
diagnostic choice - but it is a fixed one in the two Gmail
sync walks and in `contactGroups.list`, pinned per lane by a test that feeds a
page tripping both, and `calendars_list` reaches the same order structurally
(its budget is the `for` loop's own bound, so it can only fire after the token
check).

Every walk hands its refusal to the error boundary as one crate-level
`PageRefusal` (`RepeatedToken` or `BudgetExceeded`), converted to the
matching `GmailLocalError` variant, so every walk classifies the two guards
the same way. They are classified by who is at fault, not as a missing field:

- A repeated token (`GmailLocalError::PageTokenRepeated`) is the provider
  cycling, a contract breach no further paging fixes:
  `Protocol(ContractViolation)` with `AttemptCause(Acknowledged)`, because a
  complete response carried the token. It derives the terminal
  `ProviderContractViolation`.
- An exhausted page budget (`GmailLocalError::PageBudgetExceeded`) is
  neither a provider fault nor a bug - it is what an honestly huge collection
  looks like, and the implementation declined to go further:
  `Internal(LimitExceeded)`, terminal `InternalFailure`, with no attempt
  cause.

The production paging sites are:

- Gmail `search` and `search_messages` each request one provider page and return
  its `nextPageToken`. They are bounded by one request per call and resume at a
  page boundary, not inside an over-delivered page.
- Gmail `inventory_stream` traverses `users.messages.list` with repeated-token
  detection and a 10,000-page budget. It has no durable mid-walk resume token;
  cancellation or refusal terminates without `Done`, a final boundary, or a
  checkpoint, so partial enumeration can never claim full or degraded coverage.
- Gmail `changes_stream` traverses `users.history.list` with repeated-token
  detection and a 10,000-page budget (`MAX_HISTORY_PAGES`). It cannot resume
  mid-walk, and only its final batch checkpoints, using the first page's history
  boundary as described above - so a refusal must terminate rather than truncate,
  or the walk would emit a checkpoint for ground it never read. Both guards
  therefore produce `SyncEvent::Terminated`, carrying the classification
  above, and discard the page in hand; the next walk restarts from the same
  unchanged `startHistoryId`.
- People `address_books_list` traverses `contactGroups.list` with repeated-token
  detection and a 10,000-page budget. It returns `Err` on refusal, never the
  accumulated prefix as a complete address-book list.
- People personal-contact list, other-contact list, contact search,
  other-contact search, autocomplete, and directory search each request one
  provider page and return its token. They are bounded by one request per call
  and resume at page boundaries. None clips an over-delivered page, so none needs
  an intra-page offset to preserve returned items.
- Calendar `calendars_list` traverses the calendar list with repeated-token
  detection and a 10,000-page budget. It returns only after the traversal, so it
  cannot resume mid-walk.
- Calendar `events_in_range` and a search constrained to one calendar each
  request one provider page and return its token. They are bounded by one request
  per call and resume at page boundaries.
- Cross-calendar event search first runs the bounded `calendars_list`, then
  walks the finite calendar vector until the requested result limit or a provider
  page boundary. Its cursor records calendar id plus provider page token, so it
  resumes at either boundary. It does not encode an intra-page offset; the code
  relies on Google's `maxResults` contract and defensively clips a loose page.

## Repairing inventory coverage debt

Gmail is the first `Account::repair_inventory` implementor, and it exercises the
object lane only - it has no region obligations to repair.

`repair_inventory` re-reads each requested message with
`users.messages.get(format=metadata)` and rebuilds its `InventoryEntry` from
that fresh read. Rebuilding is the point, not a side effect: the obligation was
raised because the object could not be REPRESENTED, so confirming it still
exists proves only half of what is owed. The rebuilt entry crosses the Account
boundary as proof and is then discarded by the engine, which publishes only the
id - the same thing a successful walk publishes.

A `NotFound` becomes `DefinitiveIrrelevance::AbsentUnderCursorBridge`, and the
reason is the cursor model rather than the status code. The scope cursor is
anchored at the `historyId` sampled BEFORE the walk that raised the obligation,
so any deletion since then is carried by the change stream the consumer is
already reading; absence is therefore the correct inventory state and nothing is
owed. Under a different pagination or cursor model the identical `NotFound`
would NOT be dischargeable, which is why the conclusion is typed and carries its
authority rather than being inferred from the error kind. This is the same
argument the inventory walk already uses to discharge a message deleted between
`users.messages.list` and `users.messages.get`.

Everything else defers, costing one attempt against the lineage budget. A region
request reaching this account is an engine bug; it is answered with `Deferred`
rather than dropped, so the one-terminal-outcome-per-request contract holds even
then.

The label map is fetched once per pass. If that fetch fails no per-request
conclusion is possible, so the stream terminates and the engine converts every
outstanding attempt to a local deferral - it must not record a conclusion the
account never reached. That terminator is scoped by
`GmailErrorContext::repair_labels()`, the sibling of the hydration lane's
`hydrate_labels()`: `ErrorScope::Account` with resource `Label`, keeping
`AccountOperation::SyncInventory`. It deliberately does NOT reuse the inventory
walk's context, whose `resource: Message` would classify a `labels.list` 404 as
a missing MESSAGE and whose `Cursor(CursorScope::Account)` scope is the
coordinate bifrost-sync routes cursor directives by - a transient label-vocabulary
fault has nothing to say about the cursor.

## Push: Cloud Pub/Sub

Push is out-of-process. `PubSubConfig` holds a `topic` and an
optional `label_ids` filter. Wiring is opt-in: an account opened
without `with_pubsub_config` rejects `push_subscribe` with
`AccountError::Unsupported`.

`push_subscribe` sends a command to the watch actor, which issues `users.watch`
with the configured topic and atomically enters
`Watched { history_id, expiration }`. The returned `SubscriptionHandle` is a
JSON envelope `{ topic, history_id, expiration, handle_id }`. Gmail may return
the same watch state for several subscribers on a quiet mailbox, so `handle_id`
is a process-local monotonic identity that keeps each subscription removable
independently. Older persisted envelopes without `handle_id` remain readable
for unsubscribe.

Gmail only accepts the account-wide `CursorScope::Account` push scope. A
`push_subscribe` request with at least one account scope creates one watch and
reports every account entry succeeded; each other scope is reported as an
`Unsupported(PushSubscribe)` failure correlated to its `CursorScope`. An empty
request or one with no account scope returns `Err` because no watch was
created. A failed `users.watch` also returns `Err`, so no successful scope is
claimed when the request itself failed.

A subscribe emits `WatchEvent::Reconnected` only when the actor's
`disconnected` latch is set - that is, only when it genuinely reconnects.
A FIRST subscribe is silent: it is the consumer's opening act, so
announcing a reconnect from there published an event before any consumer
could hold a `push_stream()` receiver, and `broadcast` drops messages with
no receivers, making the stream's first observable state a function of
task scheduling rather than of the watch.

One actor task owns the four-state lifecycle (`Unwatched`, `Watched`, `Renewing`,
`Retired`), the active handles, subscription commands, renewal timing, and close.
There is no renewer handle for another task to clear or restart. Its renewal arm:

- Sleeps until `renewal_delay(expiration)` - one day before
  the expiration timestamp from Gmail, or `DEFAULT_RENEW_AFTER`
  (six days) if no expiration was returned. Computed renewal delays
  have a five-minute floor, including already-expired timestamps, so
  a short or unchanged watch expiration cannot create a successful
  hot renewal loop.
- Re-issues `users.watch` and replaces history id and expiration together in one
  state transition.
- On failure, the actor classifies through
  `error::into_account_error(_, GmailErrorContext::push_subscribe())`. The
  context scopes the error to `CursorScope::Account` because the watch covers
  that cursor. The actor retries locally only for `RecoveryClass::Retry`, plus
  one exception: a retry-candidate status (5xx or a policy-listed status) that
  net declined to replay because `/watch` is declared `ReplayPolicy::Never`.
  That error classifies as `Reconcile`, but the damped re-watch is its
  reconciliation (`users.watch` replaces whatever watch the mailbox has, and
  the old watch stays live until expiry), so it takes the retry path
  (`renewal_status_stopped_by_replay_policy`). Every other class,
  including engine directives and reconciliation, emits
  `WatchEvent::Terminated(AccountError)` and returns to `Unwatched`, so a later
  subscribe command starts a fresh watch and renewal schedule. Retry classes
  emit a structured
  `WatchEvent::Warning` per failure (support-only text carrying the
  message key) plus `WatchEvent::Disconnected` (once) and retry after
  `RENEW_RETRY_AFTER` (five minutes); the next success emits
  `WatchEvent::Reconnected`. Every failure goes
  through the classifier first. The warning's `retry_count` is the number
  of CONSECUTIVE renewal failures, cleared by any completed request
  (subscribe or renewal) and by a terminal classification, so an operator
  reading the lane can tell one stumble from a sustained outage.
- Selects on `shutdown.cancelled()` against the renewal sleep, biased so that a
  cancelled account retires rather than renewing when both are ready in the same
  poll. Cancellation moves the actor to `Retired`.
- Deliberately does **not** race `shutdown.cancelled()` against the `users.watch`
  request itself, on either the renewal or the subscribe path. An unbiased
  `select!` between the token and the request can pick the completed request, and
  any commit after that point installs a watch nobody renews and no `close()`
  retires - the orphan-watch failure the shutdown check exists to prevent. The
  request runs to completion and `commit_watched` decides afterwards whether we
  are still allowed to own it. It is the single place a response becomes
  `Watched`; if the account retired underneath the request it pins `Retired`,
  issues a best-effort `users.stop` for the watch it just refused, and reports
  failure. A check on the committed value cannot lose that race, where a check in
  a `select!` arm can.

  Subscribe additionally checks shutdown before queueing the command and again on
  entry to the actor, so the common post-close case costs no wire traffic at all.
  The handle envelope is encoded before the commit: an encode failure after it
  would leave `Watched` installed with no handle in the set, so `close()` would
  skip `users.stop` and the renewer would keep the orphan alive.

A successful subscribe clears the `Disconnected` latch so `Reconnected` stays
edge-triggered, but deliberately does not clear the transient-failure
`retry_after` damper: a backoff reset by a *subscribe* rather than by a completed
request is how a subscribe-then-die loop escapes its backoff. The damper also
survives unrelated actor traffic: the per-loop delay computation reads it
without consuming it, and it is cleared only where the renewal timer actually
fires or a fresh lifecycle state is installed. Consuming it during the
computation let any command landing inside the five-minute backoff erase it,
and for a watch with no expiration the recomputed delay then fell back to the
six-day default.

`push_subscribe`, renewals, `push_unsubscribe`, and close are actor messages or
actor-owned transitions, so no lifecycle state is assembled from independently
locked fields. A renewal cannot recreate a watch after a successful stop, and a
concurrent subscribe cannot be stopped by the preceding teardown.
`push_unsubscribe` decodes the handle envelope, and on a `Retired` lifecycle
returns `Ok(())` with no wire call - `Retired` is absorbing, `close()` has
already stopped the watch and cleared the handle set, so a late unsubscribe is
idempotent teardown rather than a reason to issue a post-close `users.stop` or
to overwrite `Retired` with `Unwatched`. Otherwise it removes the handle from
the active-handle set. A non-last known handle, or an unknown handle while other
known handles remain, is a no-op. When the set is empty, including on a fresh
process receiving a persisted handle, it calls `users.stop`. A failed stop for
the last known handle re-inserts that handle and leaves the `Watched` state
intact so the engine can retry. A successful stop
enters `Unwatched`. The active-handle set lets multiple
subscribers share one Gmail watch; correctness after restart does not depend on
that in-memory set surviving.

A Gmail watch belongs to the MAILBOX, not to the account instance that made it:
`users.watch` on a mailbox that already has one replaces it, and `users.stop`
takes no watch identity and stops whatever watch the mailbox has. Two live
`GoogleAccount` instances for one mailbox therefore share one remote watch, and
the engine's reopen creates exactly that pair. Uncoordinated, the old account's
teardown `users.stop` killed the replacement's fresh watch and push stayed
silently dead until the next renewal, up to six days. A process-wide
`MailboxWatch`, found through a registry keyed by the lowercased address and
holding weak references so an idle mailbox's entry is dropped, coordinates
instances:

- `holders` counts the actors in this process that currently own a live watch
  on the mailbox. Each actor's `WatchOwnership` takes its claim right after a
  successful `users.watch` (idempotent, so a renewal re-enters harmlessly) and
  releases it when the actor is no longer `Watched` or `Renewing`, when a stop
  completes or is deferred, and on drop, so a dead instance never pins another
  instance's stop decision.
- `exchange`, an async mutex, serializes every `users.watch` and `users.stop`
  for the mailbox across instances, and the claim is taken inside that lock. A
  stop decided on a zero count therefore cannot reach Gmail after another
  instance's watch the count did not yet show.
- Every teardown path (unsubscribe, close, the refused-commit stop in
  `commit_watched`, and the handle-encode-failure rollback) goes through
  `stop_exclusive`, which sends `users.stop` only when no OTHER holder exists;
  otherwise it skips the wire call and the actor's claim moves to the other
  holder. An instance that holds no watch of its own, such as one receiving a
  handle persisted across a restart, also defers to a live holder.

The bias is deliberate: a watch nobody stops is cheap (it expires, and a
notification with no listener is dropped), while a watch stopped under a live
listener silently kills push. Every ambiguity, such as a claim released late or
a second consumer in the process using another topic, errs toward skipping the
stop. Residual: instances in OTHER processes are invisible to the registry and
`users.stop` cannot be scoped to a watch, so cross-process teardown of a shared
mailbox can still stop a live watch until the next renewal.

`push_stream` adapts the 32-event broadcast health ring with shutdown wiring.
The actor also latches terminal failures in account-owned state before
broadcasting them. Each stream checks that latch before receiving and after
lag, emits `Terminated` once, then ends. Late subscribers therefore see the
terminal failure too. The latch lasts for the account instance, including if
an explicit subscribe restarts the watch actor's lifecycle. Lag without a
terminal latch emits a support-only warning with the number of events lost.

The Pub/Sub message payload itself is not parsed by this crate;
the consumer subscribes to the Pub/Sub topic out of process,
decodes the JSON envelope, and feeds the change hint into the
engine's `InvalidationSink`. The Account layer only owns the
watch lifecycle.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, `bulk_move_from`, and `bulk_destroy`
share a single `mutation_stream` driver:

- Drain up to 1000 ids per round (the Gmail `batchModify` /
  `batchDelete` cap).
- Translate the operation once per stream: `SetFlags` runs
  through `translate_flag_op` against the cached label list;
  `Move` builds an add/remove `LabelPatch` via `move_patch` from the
  destination and the optional source (see the relocation rule above);
  `Destroy` carries no translation.
- Gmail is the reason `Account::bulk_move_from` exists: `batchModify`
  carries `addLabelIds` and `removeLabelIds` in one request, so the
  source detach is free here, where the destination-only `bulk_move`
  forces a consumer into one `remove_from_container` PER ID. `bulk_move`
  is `bulk_move_from` with `source: None`.
- Post the batch:
  - `SetFlags` / `Move` -> `users.messages.batchModify`.
    A 404 from a multi-id request is ambiguous because Gmail rejects the whole
    request when any one message is absent. For each label body, the driver
    bisects that body's id list, keeping successful sub-batches live for the
    remaining label bodies and reducing 404 sub-batches to singletons. Only a
    singleton that still answers 404 receives `NotFound(Message)`, and it is
    excluded from later bodies. Bisection is used instead of per-id replay
    because the common sparse-failure case takes logarithmic rounds rather than
    one extra request per message.

    The driver tracks acknowledgements per id across the label bodies. An id is
    `Applied` (or `Downgraded` for unsupported flags) only after every body has
    succeeded. A permanent non-404 failure on a later body after an earlier
    body was acknowledged becomes `Uncertain` with the failure attached, since
    part of the requested patch already landed. A retry-class error terminates
    the stream; fully resolved ids and ids with an acknowledged partial patch keep
    their lanes in a non-final page ahead of `Terminated`. A sub-batch with no
    acknowledged effect remains unreported unless its request was ambiguous in
    flight, so the engine can re-issue it.

Shutdown of a mutation stream follows the TRANSMISSION EVIDENCE, not the loop
that caught it. Before dispatch, cancellation ends the stream silently: no byte
has crossed the side-effect boundary for the batch in hand, so nothing is owed.
Once `batchModify` / `batchDelete` has been dispatched, dropping the future
would lose writes Gmail may already have applied, and reporting them `Failed`
would assert they did not land. For label patches, the driver preserves already
resolved outcomes, reports ids with an acknowledged partial patch or an
unanswered request as `Uncertain`, and leaves untouched ids unreported. For
`batchDelete`, if that request itself is still unanswered, every dispatched id
is `Uncertain`. The accounted items are emitted on a `PageBoundary::Page` batch
(never `Final` - the operation did not complete), followed by
`SyncEvent::Terminated` carrying
`Transport(Network)` with a secondary
`AttemptCause(TransmissionState::InFlight)`. That is the same rule
`bifrost-imap` applies to an `InFlight` drop, and it is what routes a
non-idempotent mutation to reconciliation rather than to a blind retry. The
select is `biased` toward the request so an answer that has already arrived is
classified normally even when the token fires in the same poll, and the
driver's pending-event slot is drained BEFORE the shutdown check so the
terminator parked behind accounted lanes cannot be swallowed by the token
that produced it.
  - `Destroy` -> `users.messages.batchDelete`. On a parsed 403 Gmail reason of
    `forbidden` or `insufficientPermissions` it falls
    back to a label patch that moves the messages into `TRASH`,
    matching the scopes Gmail OAuth tokens with the
    `gmail.modify` scope can perform but `gmail.metadata` cannot.
    An unparseable 403 body follows ordinary classified failure handling and
    never triggers the downgrade.

    A successful fallback reports `MutationSuccess::Downgraded { actual:
    MovedToContainer(TRASH) }`, never
    `Applied`: those messages moved to Trash and still exist. Reported as
    `Applied` they came back on the next inventory or history pass, were
    destroyed again, and the account sat in a permanent reconcile loop - on
    the ordinary `gmail.modify` scope, which triggers this fallback by design
    rather than by misconfiguration. `bifrost-sync` treats the reported
    `Downgraded` effect as final and never reads it back against the original
    destroy target. An uncertain fallback observed as present may reassert,
    repeat the fallback, and end `Downgraded`; a reasserted `batchDelete` that
    now succeeds instead fulfils the original request.

    Before dispatching the fallback, the driver sets its actual effect in the
    shared progress ledger. Each successful trash sub-batch is recorded there as
    `Downgraded` immediately, so normal completion, terminal failure and shutdown
    all observe the same outcome. Failed and uncertain ids keep their lanes.
    `the_destroy_trash_fallback_downgrades_only_what_it_trashed` pins that lane
    preservation. `cancelled_trash_fallback_preserves_downgraded_sub_batch`
    cancels with one trash sub-batch acknowledged and another pending, asserting
    the downgrade, the uncertain pending id and the terminator. The scripted
    transport test `destroy_scope_failure_sends_trash_fallback_and_reports_downgraded`
    drives the production 403 classification, asserts both request bodies, and
    pins the downgraded outcome without a socket.
- The driver reads one item ahead at the 1000-item boundary. Every
  final mutation batch is marked `PageBoundary::Final`, including a
  stream whose item count is exactly divisible by 1000, followed by
  `SyncEvent::Done(None)`.

Result classification:

- All ids in a successful batch -> `ItemOutcome::Succeeded` with
  `MutationSuccess::Applied`.
- A legitimate empty patch (label op with no changes) ->
  `ItemOutcome::Succeeded` with `MutationSuccess::Skipped` per id.
- A `Destroy` that fell back to a TRASH patch for scope reasons ->
  `ItemOutcome::Succeeded` with `MutationSuccess::Downgraded { actual:
  MovedToContainer(TRASH) }` per
  successfully-trashed id (see the `Destroy` bullet above).
- A non-label move scope is malformed caller input and produces
  `ItemOutcome::Failed` per id with a `Request(Malformed)` account error.
- Unsupported flags do not prevent representable flags in the same operation
  from reaching Gmail. Successfully modified ids produce a
  `MutationSuccess::Downgraded { actual: FlagsPartiallyApplied { unsupported } }`
  outcome, preceded by a `WarningKind::StrategyDowngraded` warning. This replaces
  the earlier deliberate all-or-nothing policy.
- An operation with NO representable half sends no request and changes nothing,
  so it takes no success lane at all: every id gets `ItemOutcome::Failed` with
  `Unsupported(UpdateFlags)` -> `RecoveryClass::Unsupported`, alongside the same
  warning. `Downgraded` is not available here - its published contract in
  `crates/types/src/error/stream.rs` requires that the target actually changed,
  and `bifrost-sync` treats every downgrade as a final reported effect,
  which would falsely claim a mutation ran. `Skipped` is equally wrong:
  it claims the target was already in the requested state.
- A retry-class or auth-class error -> stream-level
  `SyncEvent::Terminated(AccountError)` so the engine can re-issue
  via `error.recovery()`. Label-patch ids with an earlier acknowledged body
  are emitted as `Uncertain`; ids with no acknowledged effect remain
  unreported unless their current request was transmission-ambiguous. A
  `batchDelete` request with `InFlight` transmission evidence also contributes
  an `Uncertain` lane.
- Any other non-404 error -> per-id `ItemOutcome::Failed(BatchFailure {
  error, .. })`, carrying the projected `AccountError`, unless an earlier
  label body was acknowledged for that id, in which case the partial patch
  is `Uncertain` and carries the same error for read-back.

Flag canonicalization in `flags.rs`:

- Gmail labels project to IMAP-style flags: `UNREAD` toggles
  `\Seen` (presence of `UNREAD` removes `\Seen`), `STARRED` is
  `\Flagged`, `DRAFT` is `\Draft`, `IMPORTANT` is
  `$Important`. Provider system-label comparisons are
  ASCII-case-insensitive. Folder-like labels (`INBOX`, `SENT`,
  `TRASH`, `SPAM`, `CHAT`) drop out. Every other label id - user labels and
  the `CATEGORY_*` system labels alike - projects to
  `$gmail-label:<id>:<name>`.
- The reverse direction in `translate_flag_op` handles `Add`,
  `Remove`, `Patch`, and `Set`. A `$gmail-label:` flag resolves on
  its label id alone; the embedded display name is advisory, so a
  server-side rename does not invalidate a queued mutation. Generic
  label flags resolve user labels and `CATEGORY_*` system labels, the
  two families `canonical_flags` emits with this spelling. Reserved system
  ids such as `INBOX`, `TRASH`, and `UNREAD` are refused in this lane, so a
  caller-forged flag cannot bypass the relocation patch or contradict
  `\\Seen` translation. If a well-formed id is absent from the current
  vocabulary, mutation translation forces one serialized `labels.list`
  refresh and retries before reporting it unsupported; if it remains absent,
  the original unsupported result is retained.
- `Set` re-derives an add/remove patch over the writable canonical flags
  plus the *user* label vocabulary. Since the patch is built without
  knowing what the target messages currently carry, an exact set
  names every known user label it omits, so `remove_label_ids`
  scales with the account's user label count rather than with the
  incoming flag set. `CATEGORY_*` labels are exempt from that
  re-derivation in both directions - Gmail's classifier owns them,
  so they resolve without poisoning the patch but are never added or
  removed by a `Set`.

  That vocabulary-sized removal list is load-bearing, not an oversight
  awaiting a read-back-then-diff replacement. One `LabelPatch` is computed
  once per mutation and applied to up to 1000 ids, which is sound only
  because the patch is
  state-independent: it asserts the same absolute set for every target
  whatever each currently carries, making it idempotent and immune to a
  concurrent label change landing between decision and write. A diffed
  patch is per-message by construction, so it would need a `messages.get`
  per target (5 quota units each) to learn current state, shatter one
  `batchModify` into one call per distinct diff, and open a lost-update
  window: a label added by another client after the read is absent from
  the diff's `removeLabelIds` and survives an exact-set meant to clear it.
  The engine's read-back guard does not close that window, since it
  verifies after the write rather than supplying pre-state. A few KB of
  request body is the cheaper side of that trade.

  Two different caps bound one `batchModify`, and only one of them is on
  the ids. `GMAIL_BATCH_MODIFY_LIMIT` (1000) bounds `ids[]`;
  `GMAIL_MODIFY_LABEL_LIMIT` (100) bounds `addLabelIds` and
  `removeLabelIds`, which the `users.messages.modify` reference documents
  for both lists and the `batchModify` reference leaves unstated. Since
  the two methods share one label-mutation path and Gmail only
  *recommends* keeping an account under 500 labels, the crate holds every
  request under the documented figure rather than assuming the silent
  method is unbounded. `mutation::batch_modify_bodies` therefore lowers a
  patch onto as many capped request bodies as its longer label list
  needs. They initially carry the same ids; if one body returns 404, the
  driver bisects its id list and carries only surviving ids into later
  bodies. This is the one bound the 404 bisector cannot reach: halving
  the id list leaves the label list exactly as long, so a label-heavy
  account without the split would 400 on every exact-set with no recovery
  path. Splitting is sound for the same reason the vocabulary-sized list
  is: an idempotent, state-independent patch says nothing about labels a
  given request does not name, so applying every body to each surviving
  id converges on the same final label set. An account under the cap -
  nearly all of them - still emits exactly one request.
  Once a multi-id 404 starts recovery, that request and all later
  `batchModify` calls in the mutation batch share a 128-request budget (6,400
  quota units). This leaves room for a 100-body patch and a single missing id
  in a full-size batch while stopping mass-delete fanout. Reaching the budget
  emits `Internal(LimitExceeded)` with acknowledged evidence, preserves
  outcomes already resolved, and leaves unvisited ids unreported. The cap is
  finite because the input batch is already bounded at 1000 ids: a one-body
  all-404 split would otherwise issue at most 1,999 calls (99,950 units). The
  detector sees HTTP status only, and batchModify supplies no per-id result,
  so it cannot distinguish a missing message from a missing label if Gmail
  uses 404 for either resource.
- `DRAFT` and `SENT` are read-only projections: Gmail answers 400 for either id
  in `addLabelIds` or in `removeLabelIds`, so neither ever reaches the wire from
  any reverse translation path, including a crafted `$gmail-label:SENT:...`
  spelling. Excluded from the wire is NOT the same as absent from the report,
  though - a flag the driver cannot send is a flag it did not apply - so
  `\Draft` and a read-only `$gmail-label:` flag land in `unsupported_flags` and
  are reported through the partial-application path above rather than dropped.
  Their OMISSION from a `FlagOp::Set` is deliberately not reported: draft-ness
  and sent-ness are structural in Gmail rather than toggles, so every ordinary
  exact set omits them, and reporting each one as partially applied would make
  `Applied` unreachable for `Set` while telling a consumer nothing actionable.
  The residual gap - a `Set` omitting `\Draft` against a message that really is
  a draft - needs a read-back the translation layer does not have.
- Any other unrecognized flag falls into `unsupported_flags`, while its
  representable siblings are still applied as a downgraded mutation.
- A canonical flag set is hashed (FNV-1a) into the
  `Fingerprint.flags_hash` field of an `InventoryEntry`.

Gmail accepts no client-mintable idempotency header on these endpoints, so the
mutation impls take the shared `IdempotencyKey` and hold it engine-side rather
than wiring it onto the request.

## Blobs

`open_blob` decodes the `BlobHandle.id` (a JSON
`{ message_id, attachment_id }` payload), fetches the
attachment, base64url-decodes the body, and emits a single
`Batch { items: vec![bytes], page_boundary: Final }` followed
by `Done`. The batch's `bytes_in` is the exact buffered JSON response-body
length transferred, not the smaller decoded attachment size.

`open_blob_range` always short-circuits with a Fatal carrying
`Unsupported(OpenBlobRange)`. Gmail attachment bodies are base64url inside
JSON and expose no byte-range transport, and a forged handle claiming range
support cannot bypass that provider capability.

`attachments_for_message` walks the MIME tree, surfacing any part
whose `body.attachment_id` is set as a `MessageAttachment` with
`source: AttachmentSource::Blob(handle)`. Inline bodies (no attachment
id) are not surfaced. `message_from_gmail` calls it under `Full` and
`FullWithBlobs`, matching the cross-provider rule that `Full` reports
attachment metadata without bytes and only `FullWithBlobs` implies
fetchable content. The `inline` marker reads only the leading
`Content-Disposition` token, case-insensitively; parameter text such as a
filename containing `inline` does not make an attachment inline.
`blob_handles_for_message` remains as the
`Vec<BlobHandle>`-only helper `open_blob`'s tests build against.

## Error translation

`account/error.rs::into_account_error(error, ctx)` is the single
boundary converting `crate::Error` (Net / ObservedNet / Response / JsonDecode /
Base64 / Local) into an `AccountError`. `GmailErrorContext` carries
the calling operation so `bifrost-types::recovery::derive` produces a
precise `RecoveryClass`; the crate keeps no local classification
tables.

Mapping highlights:

- Stable Gmail reason codes from `GmailErrorEnvelope.primary_reason()`
  route to typed `WireCause::Gmail(GmailSignal::*)` variants and
  the appropriate `AccountErrorKind` (e.g.
  `dailyLimitExceeded`/`quotaExceeded` -> `Server(QuotaExhausted)`;
  `rateLimitExceeded`/`userRateLimitExceeded` -> `Server(RateLimited)`).
  Project attribution follows the surface and configured identity above.
- Authentication failures (HTTP 401 or `authError`) ->
  `Authentication(ReauthorizationRequired)` -> `AuthLost`.
- Authorization failures (HTTP 403 outside the quota path) ->
  `Authorization(PermissionDenied)` -> `NoPermission`. The
  `InsufficientScope::needed` carrier is selected per `AccountOperation` via
  `gmail_scope_for` (`gmail.send`/`compose`/`labels`/`modify`/`readonly`/
  `metadata`/`settings.basic`, and `directory.readonly` for `DirectorySearch`).
- Transport network failures (DNS, TLS, timeout) ->
  `Transport(_)` with an `AttemptCause` whose
  `transmission_state` carries the wire-level evidence; the
  central mapping picks `Retry::SameRequest` for idempotent ops
  and `Reconcile` for non-idempotent ops caught mid-flight.
- 5xx and `internalError` -> `Server(Unavailable)` ->
  `Retry::SameRequest, reason: ServerUnavailable`.
- Every `Error::Response` and every post-200 decode failure
  (`JsonDecode`, `Base64`) pushes
  `AttemptCause(Acknowledged)` onto the chain so the central
  mapping never falls back to "absence treated as Unsent" for an
  actually-acknowledged request.
- `failedPrecondition` from the history endpoint ->
  `SyncState(CursorInvalid)`. `failedPrecondition` elsewhere ->
  `ConcurrencyConflict` (etag / version mismatch semantics),
  routed through `Retry::AfterStateRefresh`.
- 404 / 410 on the history endpoint -> `SyncState(CursorInvalid)`
  -> `Engine(RestartScope(scope))`; the scope is always
  `CursorScope::Account`, so this reseeds from `getProfile`.
- 404 on non-message resources routes to the appropriate
  `ResourceKind`: `Draft`, `Identity`, `Vacation`,
  `PushSubscription` (for Pub/Sub watch). Blob 404 maps to the
  parent message's `NotFound(Message)`.
- Gmail-envelope `Retry-After` is resolved at response observation to
  `RetryHint::At` on the originating
  `ServerCause::{Unavailable, RateLimited, QuotaExhausted}` and forwarded to
  `RetryAdvice::retry_hint`. Unrecognized bodies retain net's generic hint
  handling. There is no separate `retry_not_before` side-channel.
- Local failures (`GmailLocalError::*`) map by who is at fault, per the
  three disjoint blame families in `reference/error-model.md`:
  - `InvalidRequest` (caller input this crate cannot use, refused before
    the request it would shape is sent) -> `Request(Malformed)` ->
    `ClientBug`. Google Calendar and People route an event id without a
    calendar id, a non-UTF-8 page cursor, a range bound with no RFC 3339
    offset, and an unsupported address-book id here (`caller_input_error`). A
    cross-calendar search cursor naming a calendar no longer listed is
    state that moved rather than caller input: `ConcurrencyConflict` with
    `AttemptCause(Unsent)`, retried after a state refresh.
  - `Unsupported` and `BlobRangeUnsupported` -> `Unsupported(op)`. An RSVP
    whose event has no attendee for the account is also `Unsupported(EventRsvp)`;
    the provider event state, not caller syntax, has no target for the action.
  - `MissingField` (a response lacking something it must carry, such as a
    created event with no id or a live event missing start or end) ->
    `Protocol(MissingField)`, with `AttemptCause(Acknowledged)`. Every
    producer raises it after a complete response arrived, so it must never be
    raised for a missing LOCAL value (that is `Internal`) or missing caller
    input (`Request(Malformed)`). A field that is present but unusable is not
    missing: a `historyId` that does not parse, in the profile or a history
    page, is `Protocol(ParseFailed)`.
  - `ProviderResponse` (a complete, successful-status response that broke
    its exchange's contract, such as the Drive resumable upload's) ->
    `Protocol(MissingField | ParseFailed | ContractViolation)` by
    `ProviderFault`, with `AttemptCause(Acknowledged)`.
  - `ProviderValueUnusable` (a value an EARLIER provider response supplied,
    such as the profile's email address, is unusable for the request now being
    built, which is refused before any byte is sent) -> the same `Protocol`
    kinds by `ProviderFault`, but `AttemptCause(Unsent)`: the provider's
    fault, yet this exchange never reached the wire. The Drive hosting
    path's account-domain derivation is its producer.
  - `PageTokenRepeated` -> `Protocol(ContractViolation)`, acknowledged;
    `PageBudgetExceeded` -> `Internal(LimitExceeded)` (see Paging
    inventory above).
  - `Internal` (a state this crate believes impossible, such as a change
    stream with no cursor, or one past its profile check with no start
    history id) -> `Internal(InvariantViolated)`;
    `LimitExceededAfterResponse` (this crate's own limit stopping a
    multi-request exchange after every request sent was answered) ->
    `Internal(LimitExceeded)` with `AttemptCause(Acknowledged)`. Both
    derive the terminal `InternalFailure`.
  - The cursor-envelope protocol, envelope-version and schema-version
    mismatches and the account-identity mismatch produce
    `SyncState(SchemaIncompatible)` -> `Engine(SchemaIncompatible)` so the
    engine clears the cursor; a cursor payload that does not deserialize is
    `SyncState(CursorInvalid)`.

`mutation_error(ids, error, ctx)` is the ordinary per-id fan-out for batched
mutations: it translates the crate-level error once via `into_account_error`
and produces one `ItemOutcome::Failed(BatchFailure { error:
account_error.clone(), .. })` per id - `AccountError` is `Arc<Inner>`-backed,
so the clones share storage. The label-patch driver intercepts ambiguous
multi-id `batchModify` 404s before this fan-out and isolates them by bisection.
When a later label body fails after an earlier one was acknowledged, the
per-id failed lane is promoted to `Uncertain` because the requested patch has
already changed the message.

## Known limitations

- Only `CursorScope::Account`; non-Account scopes return `Unsupported`.
- No blob range (`BlobRangeSupport::No`); push requires a consumer-supplied
  Pub/Sub topic (`with_pubsub_config`), else `push_subscribe` is `Unsupported`.
- `historyId` expiry is reactive (no advertised TTL). Gmail mail concurrency
  and replay safety remain unavailable. Calendar update/delete/RSVP support caller
  ETags; other no-version writes retain their existing behavior.
- No independent message attachment-upload primitive (callers send inline bytes;
  over-limit attachments go through `host_attachment` -> Drive); flat labels, so
  `container_move` is unsupported; no storage-quota bytes, so `quota_get` is
  unsupported; replied/forwarded are not writeable Gmail flags.
- Server-side Gmail filters map to typed rules. Direct criteria
  (`from`/`to`/`subject`/attachment/size) plus native query strings surface as
  `FilterCondition::ProviderExpression`. Writes reject names, disabled rules,
  stop-processing, and unstorable actions. `filter_update` is unsupported.
  Read/write asymmetry: `To`, `Cc`, and `Recipient` conditions all collapse
  into Gmail's `criteria.to` on write (Gmail's `to` matches all recipient
  fields), and `criteria.to` projects back as `FilterCondition::Recipient` on
  read - so a created `To` filter reads back as `Recipient`. A consumer doing
  create-then-list equality checks must expect this.

### Accepted residuals

Assessed and deliberately left as they are. These are decisions, not gaps,
and each is also stated at the code it describes.

- **Cross-calendar event update is non-atomic.** Google exposes the move and the
  field PATCH as separate requests. If the move succeeds and the destination
  PATCH fails, the returned error keeps the PATCH's original kind, recovery
  class, retry hint, scope, telemetry, and cause chain. It is scoped to the
  event in its DESTINATION calendar because the caller's `source::` composite
  id no longer addresses it, and carries support-only text recording that the
  move succeeded. A user-safe diagnostic exposes partial commitment and the
  continuation: when recovery allows, pass the destination-scoped event id
  and original patch to `event_update`; never replay the source id. This
  skips the already completed move. Retry advice applies to the destination
  PATCH, preserving terminal/auth classes and a 429's deadline. The model
  has no typed continuation carrier. No compensating move is attempted,
  since that adds another blind write and another partial-failure window.
- **A `close()` future dropped mid-`users.stop` leaves the Gmail-side watch
  running until it expires.** The local half is cancellation-safe at every await
  - `closed` and `shutdown.cancel()` happen synchronously before the future
  exists, and the bifrost-net detach lives in a drop guard constructed BEFORE
  `Box::pin` and moved in. (A guard built inside the async block is never built
  at all if the future is dropped before its first poll, and with `closed`
  already set `Drop` would skip the detach too, leaking the rate-limiter
  registration unreclaimably - the same shape as the bug the guard was added to
  fix.) Only `users.stop` remains inside the future, because it needs the
  transport the detach sheds, so retrying it after a cancelled close is not
  possible.
- **Watch teardown is coordinated within a process only.** Instances of one
  mailbox in this process share a `MailboxWatch` and skip `users.stop` while
  another holds a live watch; instances in other processes are not visible and
  `users.stop` cannot be scoped to a watch, so a stop from another process can
  still kill a live watch until its next renewal. Errs toward skipping the
  stop on every in-process ambiguity (see the push section).
- **`open_blob_range` always returns a classified `Unsupported(OpenBlobRange)`**,
  including for a forged handle claiming `supports_range`. That is a decision
  that Gmail attachments have no byte-range transport, not a stub awaiting
  implementation. The earlier "defensive" branch returned `stream::empty()`,
  which is a silently-ended stream for any consumer awaiting bytes.
- **`get_stream` marks `PageBoundary::Final` only from information the drain
  already has.** A batch cut short by the id stream closing is `Final`; a last
  batch that fills exactly to `HYDRATE_BATCH_SIZE` stays `Page` with the
  following `Done` as terminator. **Do not "finish" this with a one-item
  lookahead.** The ids come from a backpressured producer that may be waiting on
  hydration output before it yields again, so polling for id 33 before hydrating
  ids 1-32 parks both sides forever - and short of a deadlock it holds every
  partial batch until one more id arrives. `bifrost-sync` reads `Final` in no
  hydration path, so the boundary is advisory; losing it on one alignment is far
  cheaper than a stall.
- **Inventory has no per-item failed lane; unreadable objects travel as
  coverage debt instead.** `InventoryEvent` carries no `ItemOutcome` wrapper.
  The list/get deletion race (`NotFound(Message)`) is discharged outright, and
  object-attributable permanent hydration failures are recorded as an
  `InventoryObligation` on the batch and completion coverage reports (see the
  inventory section above) and repaid through `repair_inventory`. A recovery
  class requiring stream-level action terminates the walk and does not claim
  coverage for the unfinished page.

## Unsupported error construction

Local refusal helpers use `AccountError::unsupported` through the shared error
funnel, retaining their provider, scope and diagnostic decoration.
`Account::protocol` returns `Protocol::Gmail`, including for inherited
default-method refusals.

## Caller versions on contact and event updates

`EventPatch.expected_version`, `event_delete_if_version` and
`event_rsvp_if_version` enforce caller ETags through Calendar's native `If-Match` support. Compound relocations and
empty versioned event patches are refused. There is no refresh-and-retry of
a rejected caller token. Native contract:
https://developers.google.com/workspace/calendar/api/guides/version-resources

`ContactCard.etag` exposes the CONTACT source's `metadata.sources.etag`,
never the merged `Person.etag` cache token. People `updateContact` requires
contact source metadata and rejects a stale source ETag with 400
`failedPrecondition`. Field updates preserve the fetched source identity and
send the caller's `ContactPatch.expected_version` ETag in that source when
supplied, without refresh-and-retry. Caller-versioned updates require exactly
one CONTACT source carrying an ETag and fail locally otherwise. Unversioned
updates use that source ETag when it is unique and present, and then omit the
merged `Person.etag` from the body; when CONTACT sources exist but their token
is missing or ambiguous, they keep the merged-`Person.etag` request and leave
source metadata unchanged, because the API does not say which of several
sources a write targets. `ContactCard.etag` is absent for ambiguous or missing
source tokens and for OTHER_CONTACT. Contact update concurrency is advertised
for these field writes.
Versioned empty patches and any versioned patch containing a photo action are
refused before I/O, since the separate photo actions cannot enforce that token.
The resource definitions distinguish the two ETags:
https://developers.google.com/people/api/rest/v1/people#Person
https://developers.google.com/people/api/rest/v1/people#Source
The update precondition is explicit:
https://developers.google.com/people/api/rest/v1/people/updateContact

`contact_delete_if_version` continues to refuse supplied versions before I/O,
and conditional contact deletion is not advertised. `deleteContact` documents
only the resource name and an empty body, with no ETag precondition. A local
GET-and-compare cannot enforce a precondition across the deletion race:
https://developers.google.com/people/api/rest/v1/people/deleteContact

Gmail history IDs cannot condition `batchModify` or `batchDelete`; inherited
versioned bulk methods refuse supplied mail versions per target and send only
unversioned targets through the ordinary mail implementation.

## Flag observation and reassertion capability

The account declares flag and destroy desired-state reassertion safe on stable
Gmail ids. Its flag judge uses the label snapshot and write translator,
compares labels by id rather than display name, and interprets Seen as the
inverse of UNREAD. Named Draft, unsupported tokens, invalid operations, and
an unfetched vocabulary are Indeterminate. Set leaves classifier categories,
omitted Draft, and observed labels outside the snapshot alone, as writes do.
A stale snapshot can cause bounded futile reassertion. The pure judge clones
the vocabulary and translates once per observed id, costing O(ids x labels).

Gmail's classifier can re-add IMPORTANT after a write, making a target
unsatisfiable. After an uncertain write, such a target can spend its remaining
attempt budget on writes that cannot stick. This bounded futility is accepted,
like label-snapshot staleness.

## Multi-step producer audit

Calendar move/PATCH, People fields/photo and Drive hosting attach partial-commit
records. Gmail chunked batchModify already reports per-item success, failure and
uncertainty, as do bulk mutation streams. Reaction reads have no write effects.
Single-request event/contact/filter creates are outside this multi-step contract,
including acknowledged creates without a usable id. Other event/contact writes
are single requests and need no multi-step record.
