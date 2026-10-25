# Request-level replay policy: implementation spec

Status: adjudicated on 2026-10-25 after cold reviews r1 and r2. Implementation
not yet landed. Ruling: C at the shared net boundary, with the existing 401
authentication and 429 refusal retries preserved. No open design decisions.

## 0. Standing references (read them, do not just note that they exist)

Implementers and reviewers MUST read these before touching code. They are the
contract this spec is written against and the ground it is judged on.

- `reference/technical-implementation-spec.md` - the contract this document is
  written against.
- `reference/error-model.md` - the cross-cutting `AccountError` contract. Read
  "Central derivation", "Scope, operation, provider, protocol" and "Caller
  mutation preconditions" in particular; this spec changes what feeds
  `derive`'s `idempotent` input for HTTP errors.
- `notes/bugs-shared.md`, the entry "Replay safety needs request-level operation
  context" - the originating item, including the owner's agreed direction
  (2026-10-21).
- `reference/net.md` - "Replay safety", "Shared provider holds and error
  observation", the redirect loop, and the error model section.
- `reference/caldav.md` and `reference/carddav.md` - "Which requests are
  declared unreplayable", "DELETE is replayable" (the 2026-09-07 ruling), and
  the classification-override paragraphs.
- `reference/graph.md`, `reference/google.md`, `reference/jmap.md` - the funnel
  paragraphs at the top of each, which record why the funnels currently carry
  no request safety.
- `reference/sync.md` - the mutation read-back section: how the engine consumes
  `Reconcile { CheckTarget }` and `Retry { AfterStateRefresh }`. This is the
  consumer of the classification change.

## 1. The item, restated

Today there are two idempotency authorities. `AccountOperation::is_idempotent`
(through `derive`) decides logical recovery; the HTTP method rule
(`reqwest::Method::is_idempotent`, overridable with `RequestBuilder::idempotent`)
decides whether `bifrost-net` replays a request on the wire, and dav-core
re-derives that same method rule in three places. The owner's direction:

1. Every request declares its replay policy on net's request builder:
   `Idempotent`, `Never`, `IdempotentUnlessConditional`. Each provider funnel
   sets it where the operation and the leg are known.
2. The HTTP-method rule becomes only the default.
3. Conditional requests (`If-Match`, `ifInState`) are not auto-replayed.
4. An ambiguous loss of such a request surfaces as `Reconcile` for read-back.

### 1.1 The defects this closes

- A DAV `If-Match` update PUT whose response is lost is replayed by net (PUT is
  method-idempotent). If the first attempt committed, the replay answers 412,
  the DAV ladder reads `ConcurrencyConflict`, and a write the caller itself
  committed is reported as somebody else's conflicting change. Since caller
  versions landed (`EventPatch::expected_version` and friends) this is a false
  lost-update report to the consumer, not just a refresh-and-retry.
- A caller-versioned DAV DELETE (`If-Match`) replayed after a lost response sees
  412 (RFC 9110: `If-Match` on a missing resource is false) where the
  unversioned delete reports success.
- Google Calendar `DELETE` with `If-Match` and Graph `delete_if_match` have the
  same replay shape (DELETE is method-idempotent).
- Graph `PATCH` with `If-Match`, Graph `$batch` POSTs carrying `If-Match` on
  subrequests, Google People `updateContact` (etag in the JSON body) and JMAP
  `*/set` with `ifInState` are never replayed by net (POST/PATCH default), but a
  lost response under an idempotent operation (`EventUpdate`, `ContactUpdate`,
  `UpdateFlags`, ...) derives `Retry(SameRequest)`: the consumer or engine
  replays, and the replay answers 412 / `stateMismatch` / `failedPrecondition`.
  Same misreport, one layer up.
- The same ambiguity created by a provider rather than by the transport: a
  Graph `$batch` whose envelope succeeded but omitted a conditional
  subrequest's response (`batch_response_missing`, `Acknowledged`), and a JMAP
  `*/set` carrying `ifInState` whose response names an id in neither collection
  (`set_id_unanswered`), both derive `Retry(SameRequest)` under an idempotent
  operation. `pim/messages.rs` returns that advice straight to a direct PIM
  caller.
- Unconditional Gmail, People, Calendar and Graph DELETEs are replayed by net
  today, and none of their call sites absorbs a 404/410: a replay after a
  committed-then-lost DELETE reports a successful delete as
  `NotFound` -> `ProviderRefused`. (DAV absorbs it by ruling; the others do not.)
- A non-replayable request is still status-retried after a 5xx. If its first
  attempt committed, a guarded JMAP set can replay into HTTP 200 with
  `stateMismatch`, and a Graph mutation batch into HTTP 200 with a subrequest
  412. Net returns success and the provider reports the caller's own write as
  a conflict. A create or send can succeed twice and duplicate its effect.
  Error translation cannot repair either successful response path.
- A non-replayable request whose only attempt answers a retryable 5xx under a
  single-attempt policy (`RetryPolicy::disabled()`, or any consumer-configured
  `max_attempts: 1`) has `may_have_committed` set in net, but the envelope is
  dropped because the attempt list is empty. The bare `RetryBudgetExhausted`
  derives `Server` with `Acknowledged`, which retries the same request whatever
  the operation says. The same 503 under a two-attempt policy (`[503, 503]`)
  already reconciles through the history. Same ambiguity, opposite answer,
  decided by the retry budget.
- Lost resilience in the other direction: JMAP read-only API POSTs (every
  `*/get`, `*/query`, `*/changes` batch), Google `messages/modify` /
  `batchModify`, Graph read `$batch` and unconditional absolute PATCHes are
  method-default non-replayable, so a transient drop on a pure read surfaces to
  the caller instead of being replayed by net.

### 1.2 Contracts this spec corrects

The following documented positions are wrong against the owner's direction and
are rewritten as part of the landing (section 6):

- `reference/caldav.md` / `reference/carddav.md`: "`If-Match` and unconditional
  PUTs are deliberately NOT declared ... a replay arriving after the first
  attempt committed answers 412 -> `ConcurrencyConflict` ->
  `Retry(AfterStateRefresh)`, which is already the right handling." That
  reasoning still holds for a validator the account fetched itself (CalDAV
  `event_update` and CardDAV send `If-Match: <current etag>` whenever the
  caller supplied no version, and Graph calendar `update` falls back to the
  fetched `changeKey`): there the replay's 412 leads to a refresh and reapply
  that converges. It stopped holding for caller-supplied versions, where the
  412 is reported to the caller as a conflict with its own write, and it is
  contrary to the owner's direction, which makes conditional requests
  non-replayable without distinguishing who minted the validator. The tests
  `an_if_match_update_put_still_replays_after_a_mid_flight_drop` (both crates)
  pin the replay and are replaced.

  **The cost, stated so the rewritten references carry it.** Because nearly
  every DAV and Graph update carries a self-fetched validator, nearly every
  update drop now surfaces as `Reconcile(TransportDropAfterSend,
  [CheckTarget])` instead of being replayed. In the drop-before-commit case,
  where today's replay simply succeeds, the direct caller now receives an error
  and pays a read-back. This is the cost the current `reference/caldav.md`
  paragraph says "the fix must not impose"; this spec imposes it on purpose,
  per the direction. If the owner later wants that resilience back for
  self-fetched validators, the design already has the tool: the call site
  declares `ReplayPolicy::Idempotent` when the validator is its own, since an
  explicit `Idempotent` wins over a precondition (3.1). That is NOT done here.
- `reference/net.md` top paragraph and "Replay safety": "Graph, Google and JMAP
  transport funnels do not receive an account operation; propagating request
  safety would require typed caller context". They do not need the operation:
  they need the request's replay policy, which each funnel's caller knows.
- `reference/error-model.md` "Scope, operation, provider, protocol": "Net
  independently uses request-level replay safety and otherwise its HTTP-method
  default. Those defaults are not equivalent." After this change the request
  policy also narrows classification (section 3.4).
- `reference/error-model.md` "Central derivation": `PartialResponse` on a
  non-idempotent input always adds `DedupeByClientId`. That advice is about a
  possible duplicate create, which only the operation can imply; attaching it
  to a request-level narrowing of an absolute write sends that write to
  `bifrost-sync`'s protected (never reasserted) read-back lane (3.4.1).

The dav-D1 ruling (unconditional DELETE is replayable and a gone target is a
success) is NOT reopened: an unconditional DAV DELETE stays replayable under
`IdempotentUnlessConditional`.

## 2. Survey of the ground

### 2.1 bifrost-net

- `RequestBuilderInner::idempotent: Option<bool>`, set by
  `RequestBuilder::idempotent(bool)`.
- `send_streaming_inner` resolves `replayable` per attempt as
  `idempotent.unwrap_or_else(|| method.is_idempotent())`. An explicit override
  survives a method-rewriting redirect (301/302/303 to GET).
- `replayable` gates exactly two things: `transport_error_is_replayable` (the
  transport-failure retry branch) and `may_have_committed |= !replayable &&
  status != 401` on every received response.
- Status-driven retries (5xx, 429, `policy.statuses`) ignore `replayable`.
  When the budget is spent the loop returns `RetryBudgetExhausted` (or
  `RateLimited` for a 429) without pushing the final response into the
  history, so a single-attempt policy yields a bare exhausted error even when
  `may_have_committed` was set by that one response.
- A 2xx return builds the success value with the accumulated evidence, but
  nothing outside net reads it: `may_have_committed` on a success is invisible
  to every caller.
- `AttemptEvidence { attempts, may_have_committed }::finish` wraps the terminal
  error in `Error::AttemptHistory` only when `attempts` is non-empty. A
  non-replayable request whose single attempt drops `InFlight` returns a bare
  `Error::Network { InFlight }` (or `Timeout` / `Tls`), carrying no record that
  the request was unreplayable.
- `into_account_error_with_throttle_scope` (`account_error.rs`): the
  `AttemptHistory` arm reclassifies to `Protocol(PartialResponse)` when
  `may_have_committed` or any `InFlight` state is present; the `LocalHold` arm
  raises the state to `InFlight` on `may_have_committed`. Neither stamps an
  idempotency override, so `derive` still consults the operation table: a
  committed-maybe POST under an idempotent operation derives
  `Retry(SameRequest)`.
- Success responses: `pending_response` stays `Some` and is pushed into the
  evidence after the loop, so every body failure after a received response is
  already wrapped with its `may_have_committed`.
- Consumers that read net errors as data read statuses only (`Status`,
  `RateLimited`, `RetryBudgetExhausted`, `UnexpectedResponse`, `LocalHold`), in
  Graph (`graph_envelope_evidence`, `response_status`), Google
  (`translate_net_error`, `response_parts`), JMAP (`preserved_response_body`,
  status readers in `sync/error.rs`, `client.rs`) and DAV (`wire_outcome`). No
  production consumer matches a bare transport variant of a request error
  before translation. (Production code constructs synthetic `Network` /
  `Timeout` values in Graph cloud and Google cloud; those do not pass through
  `send` and are unaffected.)

### 2.2 bifrost-dav-core, bifrost-caldav, bifrost-carddav

- `DavRequest::idempotent: Option<bool>` and `DavRequest::idempotent(bool)`
  (published).
- The method rule is restated three times in `DavDispatch`: `wire_outcome`
  (`request.idempotent.unwrap_or_else(|| request.method.is_idempotent())`),
  `send_raw_request` and `send_raw_until`.
- `dispatch_once` stamps `idempotency_override(false)` on every failure when
  `request.idempotent == Some(false)` (explicit declarations only; MOVE,
  create-PUT). `send_raw_request` stamps it when the method-derived rule says
  unreplayable. `RedirectHistory` records commit evidence with the same flag.
- `wire_outcome`'s `refused_only` is `attempts.iter().all(..)`, vacuously true
  for an `AttemptHistory` with no attempts.
- Declarations today: `propfind_raw`, `report_raw_response` -> `idempotent(true)`;
  `move_resource` -> `idempotent(false)`; caldav `put_event` and carddav
  `put_vcard` -> `idempotent(false)` on `PutCondition::IfNoneMatch` only.
  Undeclared: GETs, `delete_resource`, caldav `delete_event_if_match`, carddav
  `delete_vcard_if_match`, carddav `delete_vcard`, caldav `post_schedule_reply`.

### 2.3 bifrost-graph

- One REST funnel, `GraphClient::execute_wire`, plus `execute_aux` (OneDrive
  upload session chunk PUT and cancel DELETE, Autodiscover POST), and the EWS
  funnel `EwsClient::execute_envelope` / `execute_streaming` (trait
  `EwsExecute`). None declares replay safety; the comment in `execute_wire`
  records why.
- `GraphErrorContext::idempotent()` widens classification for one call site:
  the `translateExchangeIds` POST in `push/ews.rs`.
- Mutating helpers and their production call sites (complete list; GET-family
  helpers are reads):

| Call site | Helper / method | Operation | Request shape |
|---|---|---|---|
| `calendar.rs` create | `post` | EventCreate | create |
| `calendar.rs` update | `patch_if_match` / `patch` | EventUpdate | absolute PATCH; If-Match from the caller version, else the fetched `changeKey`; unconditional only when neither exists |
| `calendar.rs` delete_if_version | `delete_if_match` / `delete` | EventDelete | DELETE, If-Match when known; 404 not absorbed |
| `calendar.rs` rsvp | `post_no_response` | EventRsvp | action POST |
| `calendar.rs` search | `post` `/search/query` | EventSearch | read |
| `contacts.rs` create | `post` | ContactCreate | create |
| `contacts.rs` update | `patch_if_match` / `patch` | ContactUpdate | absolute PATCH, If-Match when known |
| `contacts.rs` delete | `delete_if_match` / `delete` | ContactDelete | DELETE; 404 not absorbed |
| `pim/containers.rs` create / move | `post` | ContainerCreate / ContainerMove | create / action |
| `pim/containers.rs` rename | `patch` | ContainerRename | absolute PATCH by id |
| `pim/containers.rs` delete | `delete` | ContainerDelete | DELETE; 404 not absorbed |
| `pim/identities.rs` | `patch` | IdentityUpdate | absolute PATCH |
| `pim/send.rs` MIME draft create | `post_mime` | DraftCreate (send path) | create |
| `pim/send.rs` `stamp_deferred_send_time` | `patch` | Send (scheduled-send path) / RescheduleSend | absolute PATCH; one call site serving both operations |
| `pim/send.rs` cancel scheduled | `delete` | CancelScheduledSend | DELETE; 404 not absorbed |
| `pim/drafts.rs` update | `patch` | DraftUpdate | absolute PATCH |
| `pim/drafts.rs` discard | `delete` | DraftDiscard | DELETE; 404 not absorbed |
| `pim/drafts.rs` create / send | `post` / `post_empty` | DraftCreate / DraftSend | create / action |
| `api.rs` message rules | `post` / `patch_json` / `delete` | FilterCreate / FilterUpdate / FilterDelete | create / absolute PATCH / DELETE, 404 not absorbed |
| `webhooks.rs` create | `post` | PushSubscribe | create |
| `webhooks.rs` renew | `patch_json` | PushSubscribe | absolute PATCH (expiry computed before send) |
| `webhooks.rs` delete | `delete` | PushUnsubscribe | DELETE; 404/410 absorbed by `subscription_is_gone` |
| `cloud.rs` upload session / link | `post` | AttachmentUpload / HostAttachment | create |
| `cloud.rs` chunk PUT | `execute_aux` PUT | AttachmentUpload | resumable chunk at a known offset |
| `cloud.rs` session cancel | `execute_aux` DELETE | AttachmentUpload | best-effort cleanup |
| `autodiscover.rs` | `execute_aux` POST | Discover | read |
| `push/ews.rs` translateExchangeIds | `post` | PushSubscribe (read preflight) | read |
| `get.rs` hydration batch | `post_batch` | Hydrate* | read `$batch` (all GET) |
| `reactions.rs` | `post_batch` | MessageReactionsRead | read `$batch` (all GET) |
| `mutate.rs` | `post_batch` | UpdateFlags / BulkMove / BulkDestroy / ... | mutation `$batch`; PATCH and move POST subrequests always carry If-Match, permanentDelete POST when an etag is cached |
| `pim/messages.rs` | `post_batch` | SetImportance / SetCategory / ... | mutation `$batch`; `patch_messages` PATCH and `move_messages` POST subrequests always carry If-Match; `delete_extended_property` DELETE subrequests carry none and the batch absorbs 404 (`destroy: true`) |

- EWS operations sent: `FindFolder`, `GetFolder`, `FindItem`, `GetItem` (reads,
  `ews/ops.rs`, `get_calendar_items`, `calendar_public.rs`,
  `contacts/public.rs`, `public_folder.rs`), `Subscribe` (`ews_stream.rs`
  `subscribe`), `Unsubscribe` (`release_subscription`, best effort, errors
  swallowed), `GetStreamingEvents` (`execute_streaming`).

### 2.4 bifrost-google

- One funnel, `GmailClient::execute_if_match` (reached through `execute`,
  `get`, `post`, `put`, `patch`, `delete`, `delete_absolute`,
  `post_no_content`), and `execute_builder` for caller-built builders. None
  declares replay safety; the comment in `execute_if_match` records why.
- Production call sites:

| Call site | Method | Operation | Request shape |
|---|---|---|---|
| `api.rs` create_label / create_filter | POST | ContainerCreate / FilterCreate | create |
| `api.rs` update_label | PATCH | ContainerRename / ContainerStyle | absolute PATCH by id |
| `api.rs` delete_label / delete_filter / delete_thread / delete_draft | DELETE | ContainerDelete / FilterDelete / BulkDestroy / DraftDiscard | DELETE; 404 not absorbed |
| `api.rs` modify_thread / modify_message | POST | flag and label writes | absolute add/remove label sets |
| `api.rs` send_message / create_draft / send_draft | POST | Send / DraftCreate / DraftSend | create / send |
| `api.rs` update_draft / update_vacation | PUT | DraftUpdate / VacationSet | absolute replace |
| `api.rs` patch_send_as | PATCH | IdentityUpdate | absolute PATCH |
| `mutation.rs` `post_empty_json` batchModify | POST via `execute_builder` | flag / move writes | absolute add/remove label sets |
| `mutation.rs` `post_empty_json` batchDelete | POST via `execute_builder` | BulkDestroy | permanent delete |
| `contacts.rs` create | POST | ContactCreate | create |
| `contacts.rs` update (`updateContact`) | PATCH | ContactUpdate | body carries the source or person etag: conditional |
| `contacts.rs` `updateContactPhoto` | PATCH | ContactUpdate | absolute photo replace |
| `contacts.rs` `deleteContactPhoto` / `deleteContact` | DELETE | ContactUpdate / ContactDelete | DELETE; 404 not absorbed |
| `calendar.rs` create / move | POST | EventCreate / EventUpdate | create / action |
| `calendar.rs` update / rsvp | PATCH via `execute_if_match` | EventUpdate / EventRsvp | absolute PATCH, If-Match when a version is supplied |
| `calendar.rs` delete_if_version | DELETE via `execute_if_match` | EventDelete | DELETE, If-Match when supplied; 404/410 not absorbed |
| `cloud.rs` create_upload_session | POST via `execute_builder` | AttachmentUpload / HostAttachment | create |
| `cloud.rs` chunk PUT | PUT via `execute_builder` | AttachmentUpload | resumable chunk; Drive documents resending from its offset |
| `cloud.rs` cancel_upload_session | DELETE via `execute_builder`, retry disabled | cleanup | 404/410/499 absorbed |
| `cloud.rs` permission create | POST | HostAttachment | create |
| `push.rs` / `client.rs` `/watch`, `/stop` | POST | PushSubscribe / PushUnsubscribe | action |
| every `get` / GET `execute` | GET | reads | read |

### 2.5 bifrost-jmap

- `HttpTransport::api_request(url, body)` / `api_request_measured(url, body)`
  receive opaque bytes. `ReqwestTransport::build_request` sends every API call
  as an undeclared POST (comment records why). `upload` POST, `download` /
  `download_stream` / `get_session` / `open_sse` GET.
- `Client::send_request(&Request)` holds the typed request: `Request::method_calls:
  Vec<RawMethodCall>` built only through `Request::call::<M: JmapMethod>`.
- `JmapMethod` (`core/method.rs`) carries `NAME`, `Cap`, `Response`. Its
  implementations: the `define_get_method`, `define_set_method`,
  `define_changes_method`, `define_query_method`,
  `define_query_changes_method`, `define_copy_method`, `define_parse_method`
  macros, and explicit impls for `EmailParseRequest`,
  `SieveScriptValidateRequest`, `CopyBlobRequest`, `BlobUploadRequest`,
  `BlobGetRequest`, `BlobLookupRequest`, `PrincipalGetAvailabilityRequest`,
  `SearchSnippetGetRequest`, `EmailImportRequest`.
- `ifInState` is only ever carried by `*/set` (and `*/copy` arguments).
- About thirty in-crate test doubles implement `HttpTransport::api_request`.
- `set_id_unanswered` (`sync/error.rs`, one caller in `sync/mutation.rs`)
  builds `Protocol(PartialResponse)` with `Attempt(Acknowledged)` for an id the
  `*/set` response names in neither collection.
- The WebSocket transport (`client_ws.rs`) does not go through net's retry loop
  and is out of scope.

### 2.6 Consumers of the classification

- `bifrost-sync` mutation campaigns route `Retry { AfterStateRefresh }` and
  `Reconcile { CheckTarget }` identically into reassertable read-back
  (`reference/sync.md`). Turning an ambiguous flag-write loss from
  `Retry(SameRequest)` into `Reconcile(TransportDropAfterSend, [CheckTarget])`
  moves it from `pending_retry` into reassertable read-back, which is the
  intended "read-back" of the direction. Any reconcile action other than
  `CheckTarget` makes the origin protected (`reconcile_origin` in
  `engine/bulk.rs`); that is why 3.4.1 keeps `DedupeByClientId` off narrowed
  absolute writes.
- Direct `Account` callers (PIM event/contact/draft/settings methods) receive
  the `AccountError`; `Reconcile(CheckTarget)` is the documented "read the
  target back" instruction.

## 3. Target design

### 3.1 `bifrost_net::ReplayPolicy`

New, in `crates/net/src/request.rs`, re-exported from the crate root:

```rust
/// Whether the transport may send a request again after bytes of an earlier
/// attempt may have reached the server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReplayPolicy {
    /// A replay can neither change server state nor change the answer the
    /// caller reads. Holds whatever headers the request carries.
    Idempotent,
    /// Never replay once an attempt may have left the process.
    Never,
    /// `Idempotent`, unless the request carries a precondition on a
    /// non-safe method, in which case `Never`.
    IdempotentUnlessConditional,
}

impl ReplayPolicy {
    /// The default for a request that declares nothing: safe methods
    /// (GET, HEAD, OPTIONS, TRACE) are `Idempotent`, PUT and DELETE are
    /// `IdempotentUnlessConditional`, everything else is `Never`.
    #[must_use]
    pub fn for_method(method: &reqwest::Method) -> Self;

    /// Resolve against the request as it will be sent.
    #[must_use]
    pub fn permits_replay(self, method: &reqwest::Method, headers: &HeaderMap) -> bool;
}

/// The one replay rule, shared by net's retry loop and dav-core's own walk:
/// the declared policy, else `ReplayPolicy::for_method`, resolved against
/// `method` and `headers`.
#[must_use]
pub fn request_is_replayable(
    declared: Option<ReplayPolicy>,
    method: &reqwest::Method,
    headers: &HeaderMap,
) -> bool;
```

`permits_replay`:

- `Idempotent` -> `true`.
- `Never` -> `false`.
- `IdempotentUnlessConditional` -> `method.is_safe() || !carries_precondition(headers)`.

`carries_precondition(headers: &HeaderMap) -> bool` (crate-private) is true
when any of `If-Match`, `If-None-Match`, `If-Unmodified-Since`, WebDAV `If`
(RFC 4918 section 10.4), or CalDAV `If-Schedule-Tag-Match` (RFC 6638 section
8.3) is present. No in-tree caller sends `If-Schedule-Tag-Match` today; it is
in the set because the `IdempotentUnlessConditional` default exists for
undeclared downstream callers, and a PUT or DELETE carrying it is conditional
in exactly this sense. Excluded, each on purpose so nobody adds it by analogy:
`If-Modified-Since` and `If-Range` (RFC 9110 defines them only for GET/HEAD
and range requests, which are safe), and WebDAV `Overwrite: F` (RFC 4918
section 10.6; it yields 412, but it applies only to MOVE and COPY, which
default to `Never` anyway). Header names compare case-insensitively
(`HeaderMap` semantics).

Explicit `Idempotent` wins over a precondition. That is deliberate and pinned:
it is the caller asserting it has established safety for this payload.

`PUT` and `DELETE` defaulting to `IdempotentUnlessConditional` (rather than
today's unconditional replay) closes the defect for any caller that has not
declared anything, including downstream consumers of `bifrost-net`.

### 3.2 `RequestBuilder`

- `RequestBuilderInner::idempotent: Option<bool>` becomes
  `replay: Option<ReplayPolicy>`.
- New `pub fn replay(mut self, policy: ReplayPolicy) -> Self`. Its doc states
  both effects of the resolved policy: it gates transport-failure and
  ambiguous-status replay (3.3.1), AND
  a non-replayable request that received any non-401 answer, or dropped past
  `Unsent`, carries `may_have_committed`, which narrows the classification
  (3.4). The current `idempotent` doc ("Only the transport-failure path
  consults this") is already false today, because the flag also feeds
  `may_have_committed`; it is rewritten, not carried over.
- `pub fn idempotent(self, idempotent: bool) -> Self` is KEPT (published API; a
  removal would need the owner) and becomes exactly
  `self.replay(if idempotent { ReplayPolicy::Idempotent } else { ReplayPolicy::Never })`.
  Its doc points at `replay`.
- In `send_streaming_inner`, `replayable` per attempt becomes
  `request_is_replayable(replay, &method, &headers)`.
- Method rewrite resets the declaration. In the `RedirectAction::Follow` arm,
  before `method = step.next_method`, `if step.next_method != method { replay =
  None; }`. A declared policy describes the request as written; a 301/302/303
  that turns it into a bodiless GET is a different request, and the GET's
  method default applies. `may_have_committed` stays sticky across the rewrite
  exactly as today, so a committed POST cannot be replayed through the GET.
  Without this reset, every funnel that now declares `Never` on a POST would
  lose replay of the follow-up GET that it has today.

### 3.3 Carrying single-attempt commit evidence out of net

`AttemptEvidence` gains `unreplayable_terminal: bool`. It is set at exactly two
points, both immediately before an `Err` return, and only for a request whose
resolved policy is not replayable:

1. The transport-failure arm (the `Err(e)` arm after `dispatched`), after the
   existing retry check and before `return Err(e)`:

   ```rust
   if !replayable && left_the_process(&e) {
       unreplayable_terminal = true;
   }
   ```

   `left_the_process(&Error) -> bool` is `true` for `Network`, `Tls` and
   `Timeout` whose `transmission_state` is not `Unsent`, `false` for everything
   else. It is the complement of the `Unsent` half of
   `transport_error_is_replayable` and sits next to it.

2. The retryable-status terminal branch, when the budget is exhausted OR
   replay is forbidden by 3.3.1, on the `RetryBudgetExhausted` return only,
   when `!replayable && status != StatusCode::UNAUTHORIZED`. Set
   `unreplayable_terminal = true` before that return. A retryable status that
   may follow a commit is ambiguous whether it is the first attempt or the
   third; neither the retry budget nor the replay gate may discard that fact.
   The `RateLimited` return (429) does NOT set it: a 429 is treated as a
   pre-processing refusal, and wrapping a lone 429 would hide the top-level
   `RateLimited`, with its `Retry-After` and throttle scope, from provider
   readers. A configured bare 401 without bearer auth likewise does not set
   it: 401 is an explicit refusal. An earlier 429 inside a multi-attempt
   history still counts conservatively through the existing sticky
   `may_have_committed`; this ruling does not change that evidence rule.

`AttemptEvidence::finish`:

```rust
pub(crate) fn finish(self, source: Error) -> Error {
    if self.attempts.is_empty() && !self.unreplayable_terminal {
        return source;
    }
    Error::AttemptHistory {
        source: Box::new(source),
        attempts: self.attempts,
        may_have_committed: self.may_have_committed || self.unreplayable_terminal,
    }
}
```

So the newly wrapped shapes are exactly two, both on a non-replayable request
with no earlier attempt: a terminal transport failure past `Unsent`, and a
terminal `RetryBudgetExhausted` when the retryable status may follow a commit.
Terminal `Status` and `RateLimited` errors are never newly wrapped.
The second shape now occurs under the normal multi-attempt account policy:
net stops on the first ambiguous status instead of using the remaining budget.
All provider status readers must preserve this envelope and route it through
net's translator; they must not unwrap `terminal()` to refine a 503 or a
provider JSON error into ordinary retry advice. The readers inventoried in
2.1 already accept `AttemptHistory` for multi-attempt exhaustion.
`unreplayable_terminal` is never set on the success path.

`Error::AttemptHistory`'s doc gains: "`may_have_committed` is also set when a
request that was not replayable ended in a transport failure past `Unsent` or
a retryable status that may follow a commit; `attempts` may then be empty."

#### 3.3.1 Status replay obeys the same policy (adjudication, 2026-10-25)

**Ruling: C, enforced in net for every resolved non-replayable request.** This
includes explicit `Never`, conditional `IdempotentUnlessConditional`, and an
undeclared request whose method default resolves to non-replayable. It covers
all 5xx and every additional status in `RetryPolicy::statuses`, with two
explicit refusal exceptions: 401 and 429. A configured retry status is a
retry candidate, not evidence that replay is safe. No body parsing or provider
retry-budget override is introduced.

Add next to `transport_error_is_replayable`, crate-private:

```rust
fn status_error_is_replayable(status: StatusCode, replayable: bool) -> bool {
    replayable || matches!(status, StatusCode::UNAUTHORIZED | StatusCode::TOO_MANY_REQUESTS)
}
```

In `send_streaming_inner`, keep the existing status-candidate check
`policy.statuses.contains(&status) || status.is_server_error()`. Inside it:

1. Drain and observe the response exactly as today, before deciding whether to
   retry. Preserve the final status, headers, capped body, request/trace ids
   and `Retry-After`; parse and append the final hint before the terminal
   check. Provider holds still observe the first failure.
2. Replace the terminal condition `attempt >= policy.max_attempts` with
   `attempt >= policy.max_attempts || !status_error_is_replayable(status, replayable)`.
   Keep `pending_response = None` and the existing terminal error constructors:
   429 returns `RateLimited`, everything else `RetryBudgetExhausted`.
   Set the 3.3 terminal flag at the specified return. Thus the latter error
   also means the replay policy stopped retrying before the numerical budget
   was spent; its variant and fields remain unchanged. Document that meaning
   on `Error::RetryBudgetExhausted` and `RetryPolicy`.
3. A policy-stopped attempt returns immediately: no retry wait, no refund for
   a retry that never happens, no second dispatch, and no success result.
   The terminal response remains diagnostics inside the envelope. Translation
   in 3.4 yields `Protocol(PartialResponse)` and
   `Reconcile(PartialCompletionSignal, [CheckTarget])` under an idempotent
   operation; creates and sends also retain `DedupeByClientId` per 3.4.1.
4. Keep the existing 401 force-refresh budget separate and unchanged. A 429
   remains a retry candidate only when selected by `policy.statuses`; it can
   retry a `Never` request within the ordinary budget, honors `Retry-After`,
   observes holds, and returns `RateLimited` on exhaustion. These exceptions
   establish refusal of the attempted work; a generic 5xx does not.
5. Replayable requests retain all existing status retries, backoff, hint,
   hold and budget behavior. An explicit `Idempotent` still wins over a
   precondition. Redirect policy and transport `Unsent` retries are unchanged.

Required rustdoc lines (adapt links to the owning module; keep the meaning):

- `RequestBuilder::replay`: "The resolved policy gates transport failures and
  retryable HTTP statuses. A 401 or 429 refusal may be retried even when the
  policy forbids replay. An ambiguous terminal outcome retains commit
  evidence for read-back instead of authorizing another write."
- `Error::RetryBudgetExhausted`: "Retrying stopped because the attempt budget
  was spent or the request's replay policy forbids another attempt. The final
  response is preserved when one was received." Change its display text to
  `"request retries stopped by attempt budget or replay policy"` so a policy
  stop with unused budget does not claim numerical exhaustion.
- `RetryPolicy::statuses`: "Additional retry candidates on top of the 5xx
  family. The resolved request replay policy still gates retries, except for
  explicit 401 and 429 refusals. Listing a status does not make replay safe."
  Replace the existing claim of unconditional 5xx retry; document that
  `max_attempts` is an upper bound and replay policy may stop earlier.
- `Error::Status`: describe a terminal status outside the retry-candidate set;
  a selected status stopped by budget or replay policy uses
  `RetryBudgetExhausted` (429 uses `RateLimited`).

**Why this is within SHARED-007.** The owner directed that conditional requests
are not auto-replayed. A status-driven second attempt is an auto-replay. The
same request policy already promises `Never` for creates and sends; its
successful replay can duplicate work before any classifier sees an error.
Both holes concern this item's own replay decision, so filing either would
leave the item incomplete. The previous section 8 recommendation A is
rejected. B could recover some false conflicts after replay, but cannot undo
a duplicate create or send; it also adds success-evidence plumbing across
provider response paths. Preventing the unauthorized second attempt closes
both holes at the policy boundary already being built.

**Accepted cost.** A transient 503 on a `Never` POST, conditional write or
side-effecting update now reaches the caller as `Reconcile` after one attempt,
even with budget left. Direct callers perform read-back; the sync engine's
absolute writes enter reassertable read-back, and creates, sends, moves and
destroys retain their protected lane. A read-only POST declared `Idempotent`
still retries transparently. This cost is accepted for all providers and all
non-replayable policies, not just body-level preconditions.

### 3.4 Narrowing the classification

In `into_account_error_with_throttle_scope`:

- `Error::AttemptHistory { source, attempts, may_have_committed }`:
  - if `attempts.is_empty()` AND the terminal is a transport failure
    (`Network`, `Tls`, `Timeout`): the result is the terminal's own
    translation (`into_account_error_with_throttle_scope(*source, ..)`), with
    no `PartialResponse` reclassification. There is no earlier attempt whose
    outcome a later failure could obscure, so the terminal's own kind and
    transmission state are the whole truth. A lone `InFlight` drop keeps
    `Transport(Network)` and derives `TransportDropAfterSend`.
  - otherwise, including the empty-attempt `RetryBudgetExhausted` envelope of
    3.3: the existing logic, unchanged. For that envelope it reclassifies to
    `Protocol(PartialResponse)` with `InFlight` (because `may_have_committed`).
    This is required, not incidental: the terminal's own translation is
    `Server` with `Acknowledged`, and `transient_retry_or_reconcile` retries
    `Acknowledged` whatever the idempotency input says, so an override alone
    would leave it at `Retry(SameRequest)`.
  - in every case, when `may_have_committed` is true, the result carries
    `idempotency_override(false)`.
- `Error::LocalHold`: when `may_have_committed` is true, the builder carries
  `idempotency_override(false)`.

The rule, stated once for the reference: `may_have_committed` means a request
that was not replayable may have reached the server; a blind same-request retry
of the logical operation would replay it; so `derive` must see it as
non-idempotent whatever the operation table says. The override only ever
narrows. Widening stays an operation-level decision of the producer (Graph's
`GraphErrorContext::idempotent()` for the read preflight, JMAP's WebSocket push
frames), made on errors whose requests are declared replayable.

The converse is deliberate and stated so nobody reads it as a gap: a request
declared `Idempotent` but classified under an operation the table calls
non-idempotent (a JMAP read batch issued inside a send flow, a Graph
`Idempotent` rename under `ContainerRename`) still reconciles, with
`DedupeByClientId`, after its budget is spent. The override only narrows; the
operation table stays the default everywhere net does not narrow. That is
pre-existing for GETs and harmless.

Effects, by case (operation idempotent in the table unless stated):

| Request | Event | Today | After |
|---|---|---|---|
| DAV PUT If-Match | drop InFlight, first attempt committed | replayed, 412, `ConcurrencyConflict` | not replayed, `Reconcile(TransportDropAfterSend, [CheckTarget])` |
| DAV DELETE If-Match (EventDelete) | drop InFlight | replayed, 412 or 404 misreport | not replayed, `Reconcile` |
| Graph PATCH If-Match | drop InFlight | not replayed, `Retry(SameRequest)` | `Reconcile` |
| JMAP `Email/set` (ifInState) under UpdateFlags | drop InFlight | not replayed, `Retry(SameRequest)` | `Reconcile` |
| POST declared Never, script [503, 503], budget two | status retry | two attempts, `PartialResponse` InFlight, `Retry` under idempotent op | one attempt, empty-history envelope, `PartialResponse` InFlight, `Reconcile` |
| Guarded JMAP set, script [503, 200 with stateMismatch] | status replay into body-level conflict | false `ConcurrencyConflict` | one attempt, 200 unconsumed, `Reconcile` |
| Graph mutation batch, script [503, 200 with subrequest 412] | status replay into body-level conflict | false `ConcurrencyConflict` | one attempt, 200 unconsumed, `Reconcile` |
| Never create/send, script [503 after commit, 200 success] | status replay duplicates effect | two writes possible | one attempt, read-back before another write |
| POST declared Never, single-attempt policy, 503 | exhausted at once | bare `RetryBudgetExhausted`, `Retry(SameRequest)` | envelope, `PartialResponse` InFlight, `Reconcile` with `[CheckTarget]` |
| POST declared Never, single-attempt policy, 429 | exhausted at once | bare `RateLimited`, `Retry` with hint | unchanged (not wrapped, 3.3) |
| JMAP read-only POST (declared Idempotent) | drop InFlight | not replayed, `Retry` | replayed by net; if the budget is spent, `AttemptHistory` reclassified to `PartialResponse` by the existing logic, `Retry(SameRequest)` because nothing narrows a replayable request |
| Unconditional DAV DELETE | drop InFlight | replayed, gone target absorbed | unchanged |
| Gmail send POST (Send) | drop InFlight | `Reconcile(TransportDropAfterSend)` | unchanged recovery; error is now an `AttemptHistory` envelope in net |

**A 5xx is acknowledgement, not proof of refusal.** A gateway 502, 503 or 504
can follow a commit. Both the wire gate (3.3.1) and classification therefore
honor the resolved replay policy. A non-replayable request stops on that
response and reconciles; an `Idempotent` request may retry. `Acknowledged` in
the generic server recovery mapper still permits retry; net must retain the
ambiguity envelope and reclassify it as `PartialResponse` with `InFlight`
before invoking that mapper. Stamping the override on a bare acknowledged
server error is insufficient.

The status gate also applies when a precondition is in a body rather than an
HTTP header. The JMAP typed batch is already `Never` for every write, and the
Graph mutation batch is already `Never`; no `ifInState` flag or scan of the
JSON is needed in net. The misleading HTTP 200 replies never arrive at the
provider. A genuine first-attempt HTTP 200 with `stateMismatch` or a Graph
subrequest 412 remains a real conflict and retains today's classification.

#### 3.4.1 `DedupeByClientId` follows the operation, not the override

The narrowing feeds `derive_protocol` for non-empty histories (they
reclassify to `PartialResponse`), and today `PartialResponse` on a
non-idempotent input always derives `Reconcile(PartialCompletionSignal,
[CheckTarget, DedupeByClientId])`. `bifrost-sync` treats any action other than
`CheckTarget` as protected (observe, never reassert). A conditional flag batch
that took a 401 refresh and then dropped would therefore land in protected
read-back, while the same drop without the 401 (empty history, kind
`Transport`) lands in reassertable read-back: the recovery would depend on the
history's shape, not on what the request was.

Whether `DedupeByClientId` belongs on the advice is a property of the logical
operation, not of the request: the table's non-idempotent set (sends, creates,
moves, destroys) is exactly the set `bifrost-sync` must keep in its protected
lane. For creates and sends the action means what it says (a possible
duplicate to dedupe). For destroys and moves it is kept because its presence
is what routes the item to protected read-back (`reconcile_origin` in
`engine/bulk.rs`); a reasserted destroy or move would be wrong. A
request-level narrowing changes neither fact. So `derive_protocol` gains the
operation input:

```rust
fn derive_protocol(kind: ProtocolErrorKind, idempotent: bool, may_duplicate: bool) -> RecoveryClass
```

with `may_duplicate = operation.is_some_and(|op| !op.is_idempotent())` (the
table only, never the override). `PartialResponse` on a non-idempotent input
derives `actions: [CheckTarget]` plus `DedupeByClientId` when `may_duplicate`.
Effects: request-level narrowing on an absolute-write operation yields
CheckTarget-only (reassertable); sends, creates, destroys and moves keep both
actions (the Graph missing-`permanentDelete` and JMAP unanswered-destroy rows
of `reference/sync.md` are unchanged); producers that stamp
`idempotency_override(false)` on a `PartialResponse` under `EventUpdate` /
`ContactUpdate` (the DAV partial-move and partial-sequence wrappers) lose
`DedupeByClientId`. That last one is correct: those sequences leave an
observable state at known URLs, which `CheckTarget` reads; there is no
client-minted id to dedupe by.

Two more consequences, both pinned in brick 0:

- `operation: None` flips. Today `None` with `idempotency_override(false)` on
  a `PartialResponse` derives `[CheckTarget, DedupeByClientId]`; after, it
  derives `[CheckTarget]` (`may_duplicate` is false without an operation). No
  producer reaches that shape today; the intended answer is `[CheckTarget]`,
  because nothing says the request could duplicate anything.
- The change is protocol-wide, not HTTP-only: `derive_protocol` serves every
  producer. It moves no live IMAP or SMTP behavior, verified: IMAP's
  `with_idempotency_override` is uncalled (`#[allow(dead_code)]`), and SMTP's
  only error context carries `operation: Some(Send)`, which keeps both actions.

### 3.5 dav-core: one rule, not three

- `DavRequest::idempotent: Option<bool>` becomes `replay: Option<ReplayPolicy>`.
- New `pub fn replay(mut self, policy: ReplayPolicy) -> Self`.
- `pub fn idempotent(self, bool) -> Self` is KEPT (published) and maps exactly as
  net's does.
- New `pub fn replayable(&self) -> bool { bifrost_net::request_is_replayable(self.replay, &self.method, &self.headers) }`.
  The headers DAV hands net are `self.headers` plus the minted `Authorization`;
  `Authorization` is not a precondition, so both sides resolve identically by
  construction.
- `dispatch_once`: forwards `builder.replay(policy)` when `request.replay` is
  `Some`; its `idempotency_override(false)` stamp keys on `!request.replayable()`
  instead of `request.idempotent == Some(false)`. It keeps stamping every
  failing arm, because `response_read_error` (oversized body) is minted here,
  not by net.
- `wire_outcome`: the replayable test becomes `request.replayable()`, and
  `refused_only` is redefined. Today it reads the terminal status of an
  unreplayable request as data whenever every earlier attempt was a 401, 429,
  503 or 501. But status retries replay an unreplayable request after a 503 or
  429, and a gateway 503 can follow a commit, so the replay's answer can be the
  consequence of the request's OWN committed first attempt: 412 for a
  conditional PUT or DELETE (a conflict against the caller's own write; for the
  `If-None-Match: *` create-PUT, an invitation to re-create under a new UUID),
  404 for a versioned DELETE or a MOVE whose source the first attempt already
  moved (a successful mutation reported as `NotFound`). The new rule: the
  terminal status is data when every earlier attempt is a 401/429/503/501 AND
  the terminal is 405 or 501. A 405/501 answer denies the method itself, which
  the request's own earlier commit cannot produce, and it is the only status
  DAV needs as data after a refusal (the MOVE fallback). Every other history
  of an unreplayable request goes to `into_account_error`, whose
  `may_have_committed` path reconciles. Replayable requests are unaffected
  (they already read the terminal as data).

  A clause "or every earlier attempt is a 401" (a 401 rejects before
  authentication, so nothing committed) is kept as defensive code, and the
  comment says so: it cannot fire in DAV today. DAV builds every net request
  `without_bearer_auth()` with no token source, so net's 401 refresh arm
  (which requires `auth_for_next_hop`) never runs and a 401 is always a
  terminal `Status`; DAV refreshes in `send_raw_until` across separate
  `dispatch_once` calls, and net redirects are disabled for DAV. The clause
  exists so the rule stays right if DAV ever routes auth through net.

  Accepted conservatism: after `[429, 412]` the 412 is almost certainly a
  genuine conflict, because a 429 is decided before processing, yet the new
  rule reconciles it. That matches net's own `may_have_committed`, which
  counts any non-401 answer, and the cost is one read-back.

  The empty-attempt envelopes of 3.3 satisfy the "every earlier attempt"
  half vacuously. That is correct for both shapes: a transport terminal is
  neither a `Status`, a 501 exhaustion nor `ResponseTooLarge`, so the arm
  falls through to `into_account_error`; a `RetryBudgetExhausted` terminal is
  read as data only when its status is 501 (the existing extraction), which
  is the MOVE fallback, and any other status falls through. In DAV the second
  shape now occurs on the first ambiguous status under DAV's default
  multi-attempt policy too. A lone 501 is still exposed for MOVE fallback;
  a lone 503 reconciles immediately, so `[503, 405]` can no longer reach the
  405 and must not promise a fallback.
- `send_raw_request` and `send_raw_until`: `let unreplayable = !request.replayable();`.
- `send_raw_until`'s per-hop `DavRequest { .. }` literal (after the
  destination rebase) carries `replay: request.replay` in place of
  `idempotent: request.idempotent`. DAV keeps the method on every hop (it
  surfaces a 303 to the caller rather than rewriting the method), which is why
  net's method-rewrite reset (3.2) has no DAV counterpart.
- The struct field doc and the `dispatch_once` comment are rewritten to state the
  shared rule rather than "net derives replay safety from the method".

### 3.6 Provider declarations

Every production request declares its policy at the call site. The funnel
helpers make that structural: each mutating helper takes a required
`replay: ReplayPolicy` argument, so a new call site cannot compile without a
decision. GET-family helpers pass `ReplayPolicy::Idempotent` internally.

The declaration rule (goes into `reference/net.md` "Replay safety"):

- `Idempotent`: reads, including reads carried over POST; absolute-state writes
  addressed by a stable id whose replay produces the same end state and the
  same answer; protocol-defined resumable uploads.
- `IdempotentUnlessConditional`: absolute-state writes that carry a header
  precondition only some of the time; DELETEs whose call site absorbs 404/410 as
  the delete's end state.
- `Never`: creates, sends, moves, action POSTs; DELETEs whose call site does not
  absorb not-found (a replay after a committed delete would report success as
  `NotFound`); requests carrying a body-level precondition (JMAP `ifInState`,
  People etag in JSON, Graph `$batch` with subrequest `If-Match`); calendar
  event writes that can emit scheduling messages (event updates and RSVPs, on
  every provider, conditional or not); anything whose replay safety is
  undocumented.

The scheduling row is the same hazard the JMAP paragraph below cites against
per-call `*/set` safety ("`CalendarEvent/set` can send scheduling messages on
update"), applied uniformly: a Graph event PATCH by the organizer, a Google
Calendar attendee PATCH, and a CalDAV PUT under RFC 6638 implicit scheduling
can each emit an iTIP message, and nothing documents that an identical replay
cannot emit a second one. Refusing it for one provider and accepting it for
two others would be incoherent. For Graph and Google this preserves today's
wire behavior (PATCH was never replayed); for CalDAV it removes the replay of
the rare unconditional event PUT (no etag, or a weak one). Its classification
cost is the 1.2 cost, under `EventUpdate`: a read-back instead of a replay.
`EventRsvp` is non-idempotent in the operation table already, so an RSVP drop
reconciled before and still does.

Deliberate departures from the operation table, each one stated: Graph
`ContainerRename` (rename by id), Graph `FilterUpdate` (a `messageRule` PATCH
by id, an absolute replace; the table lists `FilterUpdate` for Gmail's
delete-plus-create and ManageSieve's name keying, neither of which applies),
Graph `RescheduleSend` (`stamp_deferred_send_time` PATCHes an absolute
`singleValueExtendedProperties` deferred-send time on a known draft id; the
same call site also serves the scheduled-send path under `Send`, and the
declaration is right for both), and Google `update_label` under
`ContainerRename` (label PATCH by id) are non-idempotent in the table but
wire-`Idempotent` here. The `scope.rs` comment invites exactly this for
by-id renames. The table still governs their classification after a spent
budget (3.4).

#### Graph (`crates/graph/src/client.rs`)

Signatures (`replay` appended as the last parameter):
`post`, `post_empty`, `post_no_response`, `post_mime`, `patch`,
`patch_if_match`, `patch_json`, `delete`, `delete_if_match`, `post_batch`,
`execute`, `execute_if_match`, `execute_request`, `request`, `request_prefer`,
`execute_wire`, `execute_aux`. `execute_wire` calls
`builder.replay(replay)`. `get`, `get_json`, `get_json_prefer`, `get_eventual`,
`get_prefer` pass `ReplayPolicy::Idempotent`. The `execute_wire` comment is
rewritten to state that the caller supplies the policy.

| Call site | Policy |
|---|---|
| calendar create, contacts create, containers create, containers move, send MIME draft, drafts create, drafts send (`post_empty`), rsvp (`post_no_response`), message rule create, webhook create, cloud upload session, cloud create link | `Never` |
| calendar update (both `patch_if_match` and `patch`) | `Never` (scheduling write) |
| contacts update (both `patch_if_match` and `patch`) | `IdempotentUnlessConditional` |
| calendar delete, contacts delete (both `delete_if_match` and `delete`), containers delete, drafts discard, cancel scheduled send, message rule delete | `Never` |
| containers rename, identities, `stamp_deferred_send_time` (send and reschedule), drafts update, message rule update (`patch_json`), webhook renew (`patch_json`) | `Idempotent` |
| webhook delete (absorbs 404/410) | `IdempotentUnlessConditional` |
| calendar search POST, translateExchangeIds POST, read `$batch` in `get.rs` and `reactions.rs`, Autodiscover `execute_aux` POST | `Idempotent` |
| mutation `$batch` in `mutate.rs` and `pim/messages.rs` | `Never` |
| cloud chunk PUT (`execute_aux`) | `Idempotent` (unchanged wire behavior; see 4.2) |
| cloud session cancel (`execute_aux` DELETE) | `IdempotentUnlessConditional` (unchanged wire behavior) |

`batch_response_missing(operation, scope, detail)` gains a fourth parameter,
`subrequest_replayable: bool`, and stamps `idempotency_override(false)` when it
is false. One crate-private helper in `graph_error.rs`,
`subrequest_replayable(&BatchRequestItem) -> bool`, computes it, so the four
callers cannot drift:

- the declared policy follows the 3.6 declaration rule, not the bare method
  default: `Some(ReplayPolicy::IdempotentUnlessConditional)` for a PATCH
  subrequest (an absolute PATCH by id, replayable unless conditional), `None`
  (the method default) for every other method. Every production PATCH
  subrequest carries `If-Match` today (`patch_messages`, `mutate.rs`
  `SetFlags`), so this changes no live result; it keeps the helper coherent
  with the top-level declarations;
- it calls `bifrost_net::request_is_replayable(declared, &method, &headers)`;
- it fails closed: an unparseable method string, or any header name or value
  in `BatchRequestItem::headers` (`Option<HashMap<String, String>>`) that does
  not convert to a `HeaderName` / `HeaderValue`, returns `false`. Silently
  dropping an unconvertible entry, as the response-side conversion in
  `submit_write_batch_resource` does, would let a dropped `If-Match` read as
  replayable.

Every caller moves its `requests` vector into `BatchRequest { requests }`
before the missing-id sweep (`submit_write_batch_resource`, `mutate.rs`,
`get.rs`, `reactions.rs`), so each computes the per-id verdicts (or keeps the
items it needs) before `post_batch`, alongside the existing `expected_ids`.

Results: `get.rs` and `reactions.rs` GET subrequests are true (unchanged);
`mutate.rs` and `pim/messages.rs` PATCH and POST subrequests with `If-Match`
are false, POST subrequests are false by default, and `pim/messages.rs`'s
header-less extended-property DELETE (404 absorbed) is true. For `mutate.rs`
the item already rides `ItemOutcome::Uncertain` (always read back); the stamp
makes its advice CheckTarget-only rather than a blind retry. For
`pim/messages.rs` it is the whole fix: that error is returned to a direct
caller.

`GraphErrorContext::idempotent()` doc gains: valid only where every request
classified under the context is declared `ReplayPolicy::Idempotent`; such a
request never carries `may_have_committed`, so the widening can never undo a
net narrowing. The coupling is real, not hypothetical: an override survives
`into_builder` (`types/src/error/account_error.rs`), and both Graph's
`apply_idempotency_override` and Google's `LocalHold` arm re-apply the
context's override after net translates, so a `true` in any such context
WOULD overwrite net's `false`. Today only the `translateExchangeIds` context
carries `true`; brick 5's `translate_exchange_ids_is_declared_idempotent`
guards it.

EWS (`crates/graph/src/ews/`): `EwsClient::execute(body_xml, headers, replay)`,
`execute_envelope(envelope, headers, replay)`, and the `EwsExecute` trait
methods mirror it (`execute(body, headers, replay)`; `execute_streaming` keeps
its signature and declares `Idempotent` internally, since `GetStreamingEvents`
is its only use).

`execute_streaming` also preserves today's wire behavior explicitly: it sets
`.retry(policy)` where `policy` is a clone of `self.net.default_retry()` with
`network_errors = false` (`RetryPolicy` is `#[non_exhaustive]`, so the field is
assigned on the clone rather than through struct-update syntax). Without that,
the `Idempotent` declaration would be a real widening: today the POST is never
replayed past `Unsent`, and a pre-header drop of the stream would start being
replayed inside net, below `ews_stream.rs`'s own reconnect, backoff and health
accounting, which could double the hang on a dead path. `Idempotent` (rather
than `Never`) is still the declaration because `PushStream` is idempotent in
the table and a `Never` drop would newly narrow to `Reconcile`. Status retries
are unchanged. The other EWS reads are short request-response calls and gain
net-level replay deliberately, like the JMAP read batches. `ews/ops.rs` (`find_folder`, `get_folder`, `find_items`,
`get_item`, `get_attachment`), `get_calendar_items`, `calendar_public.rs`,
`contacts/public.rs`, `public_folder.rs` pass `Idempotent`;
`ews_stream.rs` `subscribe` passes `Never`; `release_subscription` passes
`Idempotent` (best effort, errors already swallowed). Every `EwsExecute` test
double gains the parameter.

#### Google (`crates/google/src/client.rs`)

Signatures: `post`, `put`, `patch`, `delete`, `delete_absolute`,
`post_no_content`, `execute`, `execute_if_match` take `replay` last;
`execute_builder(builder, service, replay)` applies `builder.replay(replay)`;
`get` passes `Idempotent`. `mutation.rs` `post_empty_json` gains `replay`. The
`execute_if_match` comment is rewritten.

| Call site | Policy |
|---|---|
| create_label, create_filter, send_message, create_draft, send_draft, People createContact, Calendar create, Calendar move, Drive upload session, Drive permission create, `/watch`, `/stop` | `Never` |
| delete_label, delete_filter, delete_thread, delete_draft, People deleteContactPhoto, People deleteContact, Calendar delete_if_version | `Never` |
| People updateContact | `Never` (body-level precondition) |
| update_label, patch_send_as, update_draft, update_vacation, modify_thread, modify_message, batchModify, People updateContactPhoto | `Idempotent` |
| batchDelete | `Never` |
| Calendar update, Calendar rsvp (`execute_if_match` PATCH) | `Never` (scheduling write; preserves today's PATCH wire behavior. `EventRsvp` is non-idempotent in the table and Graph's RSVP is `Never`) |
| Drive chunk PUT | `Idempotent` (Drive documents resending from the reported offset) |
| Drive cancel DELETE | `IdempotentUnlessConditional` (retry is disabled there anyway) |
| GET `execute` (attachment with size) and every `get` | `Idempotent` |

#### JMAP

- `JmapMethod` gains a required associated constant `const REPLAY_SAFE: bool;`
  (no default, so every implementation states it): `true` exactly for reads.
  `true` for the get, changes, query, queryChanges and parse macros,
  `EmailParseRequest`, `SieveScriptValidateRequest`, `BlobGetRequest`,
  `BlobLookupRequest`, `PrincipalGetAvailabilityRequest`,
  `SearchSnippetGetRequest`. `false` for the set and copy macros,
  `CopyBlobRequest`, `BlobUploadRequest`, `EmailImportRequest`.
  Every write is `false`, including an update-only `*/set` without `ifInState`,
  on purpose. A per-call rule ("update-only, unguarded, absolute PatchObjects")
  was considered and refused in the spar because it is not sound: updates can
  carry side effects (`EmailSubmission/set` success actions fire on successful
  updates, RFC 8621 section 7.5; `undoStatus` is a one-way transition;
  `CalendarEvent/set` can send scheduling messages on update), and
  individually safe writes do not compose within one batch (an update of
  `alerts/a/acknowledged` followed by a second set removing `alerts/a`
  succeeds once and answers `invalidPatch` on replay). A `*/set` drop
  therefore reconciles; `bifrost-sync` reads it back and may reassert it.
- `RawMethodCall` gains `replay_safe: bool`, set from `M::REPLAY_SAFE` in
  `Request::call`.
- `Request::replay_policy(&self) -> ReplayPolicy`: `Idempotent` when every call
  is replay-safe, else `Never`. An empty request has no effect and is
  `Idempotent`.
- `set_id_unanswered(id, ctx)` gains `guarded: bool` and stamps
  `idempotency_override(false)` when it is true; its one caller in
  `sync/mutation.rs` passes whether the `Email/set` it sent carried
  `ifInState` (`uses_if_in_state` / `expected` decide that there today). This
  is a per-id question, unlike the batch-level transport verdict: the id's own
  patch is an absolute `Email/set` update whose only replay hazard is the
  precondition. An unanswered id from a guarded set reconciles instead of
  retrying into `stateMismatch`; one from an unguarded set keeps
  `Retry(SameRequest)`, as `reference/jmap.md` documents today.

  `guarded` (the set carried `ifInState`), not `expected.is_some()` (the
  caller supplied the state), is the chosen input, and it is the more
  conservative of the two. The misreport hazard is strictly the caller-state
  case: with an internally probed `ifInState`, an engine retry re-probes the
  state cache and converges. `guarded` is chosen anyway because the owner's
  direction makes `ifInState` requests non-replayable without distinguishing
  who minted the state, and because the cost is small: a flag write that
  reconciles lands in reassertable read-back (one probe). This mirrors the
  1.2 decision for self-fetched HTTP validators.
- `HttpTransport::api_request(url, body, replay)` and
  `api_request_measured(url, body, replay)`. `Client::send_request` passes
  `request.replay_policy()`. `ReqwestTransport::build_request` gains `replay`
  and calls `request.replay(replay)`; `send_measured` / `send_once` / `send`
  thread it. `upload` passes `Never`; `download`, `download_stream`,
  `get_session`, `open_sse` pass `Idempotent`. The `build_request` comment is
  rewritten. Every in-crate `HttpTransport` test double gains the parameter.

## 4. Stopping rule and exclusions

### 4.1 In scope

types (`error/recovery.rs` `derive` / `derive_protocol`, and the stale
comment in its `idempotent_inflight_transport_retries` test),
net (`request.rs`, `retry.rs` docs, `error.rs` docs, `account_error.rs`,
`lib.rs` re-export; status replay gating included),
dav-core (`transport.rs`, `dispatch.rs`), caldav and carddav (`client.rs`
declarations and tests), graph (`client.rs`, `ews/`, `account/ews_stream.rs`
call sites, the call sites in 3.6, `graph_error.rs` doc,
`batch_response_missing` and `subrequest_replayable`), google (`client.rs`,
`api.rs`, `account/*` call sites), imap (the `pim::restamp` doc and the
`with_idempotency_override` comment in `account/error.rs`, doc only),
jmap (`core/method.rs`, the explicit `JmapMethod` impls, `core/request.rs`,
`core/transport.rs`, `client.rs`, `transport_reqwest.rs`,
`sync/error.rs` `set_id_unanswered` and its caller in `sync/mutation.rs`, test
doubles), the
references in section 6, and the removal of the originating entry from
`notes/bugs-shared.md` together with the code comments that cite its old
reasoning.

### 4.2 Out of scope (separate questions, named, not deferred)

- Resumable-upload chunk semantics (Graph OneDrive, Google Drive). Their
  declarations preserve today's wire behavior. Whether a replayed committed
  OneDrive chunk is answered with a resumable offset or an error is an
  upload-session protocol question, not a replay-policy one.
- New not-found absorption on Gmail, People, Calendar or Graph deletes. The DAV
  absorption is an owner ruling; extending it to other providers is a product
  decision. Without it, those DELETEs are correctly `Never`.
- IMAP, SMTP, ManageSieve, JMAP WebSocket: no net request. Brick 0 does touch
  their classification path (`derive_protocol` is protocol-wide), with no live
  change, verified in 3.4.1.
- JMAP direct PIM set readers (`calendar_ops::update`, the contact update,
  calendar RSVP) that read their id through `SetResponse::updated`: an id the
  response omits becomes `Error::IdNotFound` -> `convert_id_not_found` ->
  `Protocol(MissingField)`, terminal, with no read-back. That is a
  classification defect of a provider omission, and it involves no replay:
  nothing retries a terminal error. It is filed separately (with the evidence
  above) rather than built here; the bulk path's `set_id_unanswered` is the
  precedent for its fix.
- `AccountOperation::is_idempotent` stays the default authority for `derive`
  everywhere net does not narrow (and for every non-HTTP protocol).

## 5. Bricks, in landing order

This is ONE landing: the net default change, the narrowing, and the provider
declarations cannot be separated without an intermediate tree in which provider
tests flip to `Reconcile` and back. Bricks are ordered so each one's named tests
can run (scoped to its crate) as soon as it is laid; the landing's keep/revert
gate is the full `brokkr check` at the end.

Each new test below is audited for bite before the landing is kept: revert the
production half it names, confirm the test fails, restore (AGENTS.md "Audit new
tests for bite"). Check the name filter matched before trusting a PASS.

Fixture rule for every replay test in bricks 1 to 7: the request must run under
a retry policy that permits a second attempt (`max_attempts: 2`, zero
backoff; net's own tests use `two_attempts()`), or a "replays" assertion fails
whatever the declaration says. Tests that pin single-attempt behavior say
`RetryPolicy::disabled()` explicitly. Several
existing harnesses disable retries: Graph's `script_rest` installs
`RetryPolicy::disabled()`, so Graph tests use `GraphClient::script_wire(canned,
policy)` with a two-attempt policy and count wire attempts with
`rest_requests()` (the transport snapshots), never `take_rest_requests()`
(one record per logical call). Google tests build the account with
`bifrost_net::test_support::scripted_account(&script, NetConfig::default(),
Vec::new(), policy)` and count with `script.requests()`. DAV tests use the
existing `dav_script` / `scripted_dav_net` helpers and confirm their policy
permits a retry before relying on them (the existing
`an_if_match_update_put_still_replays_after_a_mid_flight_drop` proves the DAV
helper does).

### Brick 0: `DedupeByClientId` follows the operation (types)

Lay 3.4.1 in `crates/types/src/error/recovery.rs`: `derive_protocol` takes
`may_duplicate`, computed in `derive` from `operation` alone.

Instrument (new, `recovery.rs` tests):
`partial_response_dedupe_follows_the_operation_table` - `derive` for
`Protocol(PartialResponse)` with `Attempt(InFlight)`: `UpdateFlags` with
`idempotency_override(false)` -> `Reconcile(PartialCompletionSignal)` with
actions exactly `[CheckTarget]`; `Send` (no override) -> `[CheckTarget,
DedupeByClientId]`; `BulkDestroy` -> `[CheckTarget, DedupeByClientId]`;
`UpdateFlags` without override -> `Retry(SameRequest)`; `operation: None` with
`idempotency_override(false)` -> actions exactly `[CheckTarget]` (3.4.1).
Bite: make `may_duplicate` always true.

Also in this brick: the comment in `idempotent_inflight_transport_retries`
("flag writes are now correctly in the non-idempotent set") is stale,
`UpdateFlags` is idempotent in `scope.rs`; correct it.

```
brokkr test -p bifrost-types partial_response_dedupe_follows_the_operation_table
```

### Brick 1: `ReplayPolicy`, the resolver and the envelope (net)

Add the enum, `for_method`, `permits_replay`, `carries_precondition`,
`request_is_replayable`, the crate-root re-export, `RequestBuilder::replay`, and
re-express `idempotent(bool)`. Switch `send_streaming_inner` to the resolver
with the method-rewrite reset. Lay 3.3 and 3.3.1 (`unreplayable_terminal`,
both setting points, `finish`, the shared status gate and the error/retry docs)
here too: the brick's own tests
assert the empty-attempt envelope, so the envelope cannot wait for brick 2.

Instrument (new, `request.rs` tests):
`replay_policy_resolves_methods_preconditions_and_explicit_declarations` - a
table over (declared policy or none, method, headers) asserting
`request_is_replayable`: GET/HEAD/OPTIONS default true, GET with `If-None-Match`
default true; PUT/DELETE default true bare and false with each of `If-Match`,
`If-None-Match`, `If-Unmodified-Since`, `If`; POST/PATCH/MOVE default false;
`Idempotent` with `If-Match` true; `Never` on GET false;
`IdempotentUnlessConditional` on PATCH bare true and with `If-Match` false;
PUT with `If-Schedule-Tag-Match` default false; MOVE with `Overwrite: F`
declared `IdempotentUnlessConditional` true (not a precondition).
Bite: change the PUT default back to `Idempotent`.

Instrument (new): `a_conditional_put_is_not_replayed_after_an_in_flight_drop` -
scripted `[in_flight_reset(), 200]`, `two_attempts()`, undeclared PUT with
`If-Match`: one request; error is `AttemptHistory { attempts: [], may_have_committed: true, .. }`
with `terminal()` `Network { InFlight }`. Control in the same test: the same
request without `If-Match` makes two requests and succeeds. Bite: remove the
`If-Match` arm of `carries_precondition`.

Instrument (new): `a_method_rewriting_redirect_restores_the_method_default` -
POST declared `Never`, scripted `[303 with Location, in_flight_reset(), 200]`,
under `two_attempts()` with zero backoff (the per-hop `AttemptBudget` reset
makes two attempts sufficient for the GET hop): three requests (POST, GET,
GET) and success. Bite: delete the `replay = None` reset (the GET then refuses
replay and the request fails after two).

Instrument (new): `a_single_exhausted_status_on_an_unreplayable_request_is_wrapped` -
`RetryPolicy::disabled()`, undeclared POST scripted `[503]`: error is
`AttemptHistory { attempts: [], may_have_committed: true, .. }` with
`terminal()` `RetryBudgetExhausted`. Controls in the same test: the same POST
declared `.replay(ReplayPolicy::Idempotent)` returns a bare
`RetryBudgetExhausted`; the undeclared POST scripted `[429]` returns a bare
`RateLimited`. Bite: drop the status-exhaustion setting point of
`unreplayable_terminal`.

Instrument (new): `status_replay_obeys_policy_except_proven_refusals` - table
of `status_error_is_replayable`: both replayability inputs for 401, 429, 409,
500, 502, 503, 504 and 599. Only 401/429 return true when non-replayable.
Bite: return true for every status.

Instrument (new): `ambiguous_status_retries_obey_the_resolved_request_policy` -
script `[status, 200]` under `two_attempts()` for each of 500, 502, 503, 504,
599, and a custom 409 explicitly added to `policy.statuses`. Cross with POST
`Never`, undeclared POST, PUT/DELETE with `If-Match` (both method-default and
explicit `IdempotentUnlessConditional`), and an explicit `Never` GET. Each
makes exactly one wire attempt, leaves one scripted reply unconsumed, and
returns `AttemptHistory { attempts: [], may_have_committed: true, .. }` with
terminal `RetryBudgetExhausted` preserving that status, headers and body.
Include a `Retry-After: 60` response with a request deadline of one second:
using paused Tokio time, assert it returns before advancing time (no retry
wait) and preserves the final hint. In each row the `Idempotent` control uses
zero hint/backoff, makes two attempts and succeeds, including a request with
`If-Match` (explicit wins). Bite: remove only the new status-gate operand
from the terminal condition, retaining the resolver and envelope machinery.

Instrument (new): `a_never_request_still_retries_a_429_refusal` - `Never` POST,
`[429 with Retry-After: 0, 200]`, `two_attempts()`: two attempts, success.
`[429]` under `RetryPolicy::disabled()` stays bare `RateLimited` with hint and
response evidence. Existing 401 force-refresh tests stay green unchanged.
Bite: remove the 429 exception from `status_error_is_replayable`.

Updated: `scripted_retry_refunds_quota_and_preserves_final_response` declares
its synthetic POST `Idempotent`; its subject is the mechanics of an allowed
retry, so it must opt into one. Do not widen any production POST to keep an
old test green. The Graph GET test
`a_transient_5xx_is_retried_below_the_graph_funnel` stays green unchanged.

Updated: `an_in_flight_failure_on_a_post_is_not_replayed` and
`an_explicitly_non_idempotent_get_is_not_replayed` assert on `error.terminal()`
and on the envelope's `may_have_committed`.

```
brokkr test -p bifrost-net replay_policy_resolves_methods_preconditions_and_explicit_declarations
brokkr test -p bifrost-net a_conditional_put_is_not_replayed_after_an_in_flight_drop
brokkr test -p bifrost-net a_method_rewriting_redirect_restores_the_method_default
brokkr test -p bifrost-net a_single_exhausted_status_on_an_unreplayable_request_is_wrapped
brokkr test -p bifrost-net status_replay_obeys_policy_except_proven_refusals
brokkr test -p bifrost-net ambiguous_status_retries_obey_the_resolved_request_policy
brokkr test -p bifrost-net a_never_request_still_retries_a_429_refusal
brokkr test -p bifrost-net scripted_retry_refunds_quota_and_preserves_final_response
brokkr test -p bifrost-net an_in_flight_failure_on_a_post_is_not_replayed
brokkr test -p bifrost-net an_explicitly_non_idempotent_get_is_not_replayed
```

### Brick 2: the narrowing (net)

Lay 3.4.

Instrument (new, `account_error.rs` tests or `tests/account_error.rs`):
`an_unreplayable_drop_reconciles_under_an_idempotent_operation` - drive a PUT
with `If-Match` through the scripted dispatcher with `[in_flight_reset()]`,
translate with `operation: EventUpdate`: kind `Transport(Network)`, recovery
`Reconcile` with reason `TransportDropAfterSend` and action `CheckTarget`.
Control: the same request declared `.replay(ReplayPolicy::Idempotent)` with
`[in_flight_reset(), in_flight_reset()]` and `two_attempts()` (replayed, budget
spent, `may_have_committed: false`) derives `Retry(SameRequest)`. Bite: remove
the override stamp (the first half derives `Retry`).

Instrument (new): `a_post_that_may_have_committed_reconciles_under_an_idempotent_operation`
- POST `Never`, scripted `[401, 503]` with a working refresh token source and
`two_attempts()`: exactly two wire attempts; the 401 refresh does not consume
the network budget. The non-empty history contains the refused 401, the
terminal preserves the 503, and `may_have_committed` is true. Translated with
`operation: SetIsRead`: `Reconcile(PartialCompletionSignal)` with actions
exactly `[CheckTarget]` (brick 0). Bite: remove the stamp from the non-empty
branch (derives `Retry(SameRequest)`). `[503, 503]` cannot exercise that branch
anymore because the first 503 now stops the request.

Instrument (new): `a_single_exhausted_status_reconciles_under_an_idempotent_operation`
- the brick 1 single-attempt `[503]` envelope translated with
`operation: SetIsRead`: kind `Protocol(PartialResponse)`, recovery
`Reconcile(PartialCompletionSignal)` with actions exactly `[CheckTarget]`.
Bite: route empty-attempt envelopes of every terminal through the
terminal's own translation (the transport carve-out applied too widely),
which derives `Retry(SameRequest)` from `Server` with `Acknowledged` even with
the override stamped.

Instrument (new): `a_policy_stopped_status_reconciles_before_a_duplicate_effect`
- drive `Never` POSTs with `[503, 200]` under `two_attempts()`, translated
separately with `EventCreate` and `Send`. One wire attempt, one unconsumed
success, `Reconcile(PartialCompletionSignal)` with actions exactly
`[CheckTarget, DedupeByClientId]`. The final 503's body and request id survive
translation. Repeat with `SetIsRead`: actions exactly `[CheckTarget]`.
Bite: remove the status gate (success returned), then restore and separately
remove the empty-history status envelope (acknowledged server error retries).
Each ablation must fail independently. This covers both duplicate-prone
operations and the default multi-attempt empty-history narrowing seam.

Updated (`hold.rs` tests): separate the custom-403 case of
`review_r1_committed_attempt` into
`review_r1_nonreplayable_post_stops_before_a_retry_hold`: the terminal is now
the enveloped 403, not `LocalHold`; the hold is still recorded, one wire
attempt occurs, and recovery is `PartialCompletionSignal` with
`[CheckTarget, DedupeByClientId]` under `Send`. Preserve the existing 303/307
cases and their `ThrottledMidFlight` expectations. Bite: remove only the
status gate (the error becomes `LocalHold`).

Instrument (new, `hold.rs` tests):
`a_hold_after_a_possible_commit_reconciles_under_an_idempotent_operation` - the
existing POST-303-then-held-GET shape translated with `SetIsRead`: recovery
`Reconcile(ThrottledMidFlight)`. Bite: remove the `LocalHold` stamp.

```
brokkr test -p bifrost-net an_unreplayable_drop_reconciles_under_an_idempotent_operation
brokkr test -p bifrost-net a_post_that_may_have_committed_reconciles_under_an_idempotent_operation
brokkr test -p bifrost-net a_single_exhausted_status_reconciles_under_an_idempotent_operation
brokkr test -p bifrost-net a_policy_stopped_status_reconciles_before_a_duplicate_effect
brokkr test -p bifrost-net review_r1_nonreplayable_post_stops_before_a_retry_hold
brokkr test -p bifrost-net a_hold_after_a_possible_commit_reconciles_under_an_idempotent_operation
```

### Brick 3: dav-core shares the rule (dav-core)

Lay 3.5. Re-declare the dav-core call sites: `propfind_raw`,
`report_raw_response` -> `.replay(Idempotent)`; `move_resource` ->
`.replay(Never)`; `delete_resource` -> `.replay(IdempotentUnlessConditional)`
(the dav-D1 ruling, now explicit).

Instrument (new, `transport.rs` tests):
`dav_requests_resolve_replay_through_the_net_rule` - `DavRequest::replayable` for
PROPFIND declared `Idempotent` (true), MOVE declared `Never` (false), PUT with
`If-None-Match: *` undeclared (false), PUT with `If-Match` declared
`IdempotentUnlessConditional` (false), bare PUT (true), bare DELETE (true),
DELETE with `If-Match` (false). Bite: restore the
`unwrap_or_else(|| method.is_idempotent())` body.

Instrument (new, `dispatch.rs` tests):
`an_undeclared_conditional_put_drop_reconciles` - an undeclared PUT with
`If-Match` sent through `send_raw_request` under `EventUpdate`, scripted
`[dav_dropped_after_send(), 204]`: one request; `Err` whose recovery is
`Reconcile` with `CheckTarget`. Bite: revert the PUT default in brick 1 (two
requests, `Ok`).

Instrument (new, `dispatch.rs` tests):
`a_status_after_a_possible_commit_is_not_read_as_data` - directly feed
`wire_outcome` synthetic `AttemptHistory` values with `may_have_committed: true`,
an earlier `Status(503)`, and terminal `Status(412)` for conditional PUT
(`EventUpdate`), `Status(404)` for conditional DELETE (`EventDelete`), or
`Status(404)` for MOVE (`EventUpdate`). Each reconciles, never returns status
as data. This pins defensive history consumption; 3.3.1 now makes these
same-leg status-replay histories unreachable through the live dispatcher.
Controls: synthetic `[503, 405]` history still returns the 405 as data;
an empty-attempt envelope with terminal `RetryBudgetExhausted(501)` still
returns 501 for MOVE fallback. Bite: restore the old `refused_only` set (the
first three cases return status as data). Directly constructing the history
is necessary: a live `[503, 412]` now stops at 503 under either reader rule,
so it would not bite this change.

Instrument (new, `dispatch.rs` tests):
`an_unreplayable_dav_status_stops_before_the_next_reply` - two-attempt policy,
conditional PUT `[503, 412]`, conditional DELETE `[503, 404]`, and MOVE
`[503, 405]`: one wire attempt each, one reply unconsumed, and `Reconcile`
from the first 503. MOVE does not fall back after that ambiguous 503.
Controls: a first-attempt MOVE 405 or 501 still yields `Ok(false)` from
`move_resource`; a PUT with `If-Match` `[412]` still yields
`ConcurrencyConflict`. A conditional PUT `[401, 412]` with bearer credentials
and working refresh still yields `ConcurrencyConflict`; that is DAV's own
refresh walk (`send_raw_until` across two `dispatch_once` calls), not net's
401 branch. Bite: remove only net's status gate (the queued terminal status
is consumed, even if classification still reconciles).

Existing, must stay green: `a_delete_replays_and_reads_a_gone_target_as_success`,
`an_oversized_response_on_an_unreplayable_request_reconciles`.

```
brokkr test -p bifrost-dav-core dav_requests_resolve_replay_through_the_net_rule
brokkr test -p bifrost-dav-core an_undeclared_conditional_put_drop_reconciles
brokkr test -p bifrost-dav-core a_status_after_a_possible_commit_is_not_read_as_data
brokkr test -p bifrost-dav-core an_unreplayable_dav_status_stops_before_the_next_reply
brokkr test -p bifrost-dav-core a_delete_replays_and_reads_a_gone_target_as_success
brokkr test -p bifrost-dav-core an_oversized_response_on_an_unreplayable_request_reconciles
```

### Brick 4: CalDAV and CardDAV declarations

`put_event`: `.replay(Never)` for every `PutCondition` (a scheduling write,
3.6; the `IfNoneMatch` header is kept). `put_vcard`: `IfNoneMatch` ->
`.replay(Never)` (header kept); `IfMatch` and `None` ->
`.replay(IdempotentUnlessConditional)`;
`delete_event_if_match` / `delete_vcard_if_match` -> `.replay(Never)`;
`delete_vcard` -> `.replay(IdempotentUnlessConditional)`; `get_event` /
`get_vcard` -> `.replay(Idempotent)`; `post_schedule_reply` -> `.replay(Never)`.
Rewrite the `put_event` comment (and its carddav twin) to the new reasoning.

Replaced in both crates: `an_if_match_update_put_still_replays_after_a_mid_flight_drop`
becomes `an_if_match_update_put_reconciles_after_a_mid_flight_drop` - script
`[dav_dropped_after_send(), 204]`: one transcript; error recovery `Reconcile`
with `CheckTarget`. Bite: declare the PUT `.replay(Idempotent)` (two
transcripts, success).

New in caldav: `an_unconditional_event_put_is_not_replayed_after_a_mid_flight_drop`
- `put_event` with `PutCondition::None` scripted `[dav_dropped_after_send(),
204]` under `EventUpdate`: one transcript; `Reconcile` with `CheckTarget`.
Control in carddav (`an_unconditional_vcard_put_replays_after_a_mid_flight_drop`):
`put_vcard` with `PutCondition::None`, same script: two transcripts, `Ok`.
Bite: declare the caldav `None` arm `IdempotentUnlessConditional`.

New in both crates: `a_versioned_delete_reconciles_after_a_mid_flight_drop` -
caldav `delete_event_if_match` / carddav `delete_vcard_if_match` with
`[dav_dropped_after_send(), 204]`: one transcript; `Reconcile` with
`CheckTarget`. Bite: declare the DELETE `IdempotentUnlessConditional` and drop
`If-Match` from `carries_precondition`.

Existing, must stay green in both crates:
`a_dropped_delete_replays_and_a_gone_target_is_a_success`,
`a_create_put_is_not_replayed_after_a_mid_flight_drop`,
`a_dropped_relocate_move_reconciles_rather_than_replaying`,
`a_move_without_server_move_support_copies_then_deletes`.

```
brokkr test -p bifrost-caldav an_if_match_update_put_reconciles_after_a_mid_flight_drop
brokkr test -p bifrost-carddav an_if_match_update_put_reconciles_after_a_mid_flight_drop
brokkr test -p bifrost-caldav an_unconditional_event_put_is_not_replayed_after_a_mid_flight_drop
brokkr test -p bifrost-carddav an_unconditional_vcard_put_replays_after_a_mid_flight_drop
brokkr test -p bifrost-caldav a_versioned_delete_reconciles_after_a_mid_flight_drop
brokkr test -p bifrost-carddav a_versioned_delete_reconciles_after_a_mid_flight_drop
brokkr test -p bifrost-caldav a_dropped_delete_replays_and_a_gone_target_is_a_success
brokkr test -p bifrost-carddav a_dropped_delete_replays_and_a_gone_target_is_a_success
```

### Brick 5: Graph declarations

Lay the Graph part of 3.6.

New (`client.rs` tests, using the scripted transport and
`Canned::Error(bifrost_net::Error::Network { InFlight })` as the existing
`mutate.rs` tests do):

- `graph_if_match_patch_reconciles_after_a_drop` - calendar `update` with an
  `expected_version`: script the event GET, then a dropped PATCH; one PATCH
  request; error `Reconcile` with `CheckTarget`. Bite: declare
  `Idempotent`.
- `graph_unconditional_event_patch_is_not_replayed` - calendar `update` with
  no `expected_version`, the scripted event GET carrying no `changeKey` (so
  the PATCH goes out unconditional), then `[drop, 200]`: one PATCH request;
  error `Reconcile` with `CheckTarget`. Bite: declare
  `IdempotentUnlessConditional` (two PATCHes, `Ok`).
- `graph_unconditional_absolute_patch_replays_after_a_drop` - containers
  `container_rename`: `[drop, 200]`: two PATCH requests, `Ok`. Bite: declare
  `Never`.
- `graph_unabsorbed_delete_is_not_replayed` - containers `container_delete`:
  `[drop, 204]`: one DELETE; error requires reconciliation. Bite: declare
  `IdempotentUnlessConditional`.
- `graph_read_batch_replays_and_mutation_batch_does_not` - `post_batch` with
  `Idempotent` and with `Never`, each scripted `[drop, 200]`: two requests and
  one request respectively. Bite: swap the declarations.
- `a_503_before_a_batch_412_reconciles_without_replay` (`account/pim/messages.rs`
  tests) - build a test `GraphAccount` around a client using
  `script_wire(canned, policy)` with `max_attempts: 2` and zero backoff. Call
  the real `patch_messages` with one `MessagePatch` (id `m1`, etag `v1`, an
  absolute category patch), `AccountOperation::SetCategory`. Queue outer
  HTTP 503, then outer HTTP 200 with
  `{"responses":[{"id":"0","status":412,"body":{"error":{"code":"ErrorIrresolvableConflict","message":"stale etag"}}}]}`.
  Assert one wire POST to `$batch`, its subrequest carries exactly
  `If-Match: v1`, and the returned `AccountError` is
  `Protocol(PartialResponse)` with `Reconcile(PartialCompletionSignal)` and
  actions exactly `[CheckTarget]`. Count with `rest_requests()` or
  `wire_attempts()`, not `take_rest_requests()`. The queued 200 is not used.
  Control in a fresh client: queue only that HTTP 200/412 envelope; one wire
  attempt and `ConcurrencyConflict`. Do not change the per-item 412 mapper.
  Bite: remove only net's new status-gate operand (retain the declarations,
  envelope and narrowing); the two-attempt run consumes the 200 and reports
  `ConcurrencyConflict`. Separately declare the mutation envelope
  `Idempotent` at its provider forwarding point: the test must fail in the
  same way. Both ablations are required.
- `translate_exchange_ids_is_declared_idempotent` - the `push/ews.rs`
  preflight with `[drop, 200]`: two requests. This pins the coupling that keeps
  `GraphErrorContext::idempotent()` from widening a narrowed error. Bite:
  declare `Never`.
- `a_missing_conditional_subresponse_reconciles` (`graph_error.rs` tests) -
  `batch_response_missing(SetCategory, scope, detail, false)` derives
  `Reconcile` with actions exactly `[CheckTarget]`; with `true` it derives
  `Retry(SameRequest)`; `batch_response_missing(BulkDestroy, .., false)` keeps
  `[CheckTarget, DedupeByClientId]`. Plus, in `pim/messages.rs` tests, a
  `SetCategory` write batch whose 200 envelope omits the PATCH subresponse
  returns an error that requires reconciliation. Bite: drop the stamp.
- `subrequest_replayable_follows_the_declaration_rule_and_fails_closed`
  (`graph_error.rs` tests) - GET true; PATCH bare true; PATCH with `If-Match`
  false; POST bare false; DELETE bare true; a method string that does not
  parse false; a PATCH whose header map holds an `If-Match` entry with an
  invalid header value false. Bite: skip unconvertible header entries instead
  of failing closed (the last case turns true).
- `ews_subscribe_is_not_replayed_and_reads_are` (EWS client tests, scripted
  net under a two-attempt policy) - `subscribe` with `[drop, 200]`: one
  request; `find_folder` with `[drop, 200]`: two requests. Bite: declare
  `Subscribe` `Idempotent`.
- `ews_streaming_request_is_not_replayed_by_net` - `execute_streaming` with
  `[drop, 200]` under a two-attempt account default: one request, so
  reconnection stays with `ews_stream.rs`. Bite: drop the
  `network_errors = false` policy override.

The existing `ews_stream.rs` reconnect tests must stay green unchanged: with
the override, net's transport behavior for the stream is identical to today's
POST default. If one moves, the override is wrong, not the test.

```
brokkr test -p bifrost-graph graph_if_match_patch_reconciles_after_a_drop
brokkr test -p bifrost-graph graph_unconditional_event_patch_is_not_replayed
brokkr test -p bifrost-graph subrequest_replayable_follows_the_declaration_rule_and_fails_closed
brokkr test -p bifrost-graph ews_subscribe_is_not_replayed_and_reads_are
brokkr test -p bifrost-graph ews_streaming_request_is_not_replayed_by_net
brokkr test -p bifrost-graph graph_unconditional_absolute_patch_replays_after_a_drop
brokkr test -p bifrost-graph graph_unabsorbed_delete_is_not_replayed
brokkr test -p bifrost-graph graph_read_batch_replays_and_mutation_batch_does_not
brokkr test -p bifrost-graph a_503_before_a_batch_412_reconciles_without_replay
brokkr test -p bifrost-graph translate_exchange_ids_is_declared_idempotent
brokkr test -p bifrost-graph a_missing_conditional_subresponse_reconciles
```

### Brick 6: Google declarations

Lay the Google part of 3.6.

New (scripted net, `Canned::Error(Network { InFlight })` as `cloud.rs` tests do):

- `gmail_label_modify_replays_after_a_drop` - `modify_message` `[drop, 200]`:
  two POSTs, `Ok`. Bite: declare `Never`.
- `people_update_reconciles_after_a_drop` - contacts `update` (GET person, then
  dropped PATCH): one PATCH; `Reconcile` under `ContactUpdate`. Bite: declare
  `Idempotent`.
- `calendar_delete_is_not_replayed_after_a_drop` - unversioned
  `delete_if_version`: `[drop, 204]`: one DELETE; error requires reconciliation.
  Bite: declare `IdempotentUnlessConditional`.
- `calendar_event_writes_are_not_replayed_after_a_drop` - unversioned
  `update` (no move, field patch only) and unversioned `rsvp` (event GET, then
  the PATCH), each with the PATCH scripted `[drop, 200]`: one PATCH each;
  `update` reconciles under `EventUpdate`, `rsvp` under `EventRsvp`. Bite:
  declare the `execute_if_match` PATCH `IdempotentUnlessConditional` (two
  PATCHes each, `Ok`).

```
brokkr test -p bifrost-google gmail_label_modify_replays_after_a_drop
brokkr test -p bifrost-google people_update_reconciles_after_a_drop
brokkr test -p bifrost-google calendar_delete_is_not_replayed_after_a_drop
brokkr test -p bifrost-google calendar_event_writes_are_not_replayed_after_a_drop
```

### Brick 7: JMAP declarations

Lay the JMAP part of 3.6.

New test instrument, before the tests that use it: move the existing
`transport_reqwest.rs` private `scripted_transport` construction into a
`#[cfg(test)] pub(crate) fn scripted_with_retry` associated constructor on
`ReqwestTransport`, taking
`(&Arc<bifrost_net::test_support::ScriptedDispatch>, bifrost_net::RetryPolicy)`.
It constructs the same Basic-auth transport, sets the attached
`AccountSpec::default_retry` to the supplied policy, and uses `scripted_net`; it does
not fetch a session. Keep `scripted_transport` as a local wrapper passing the
normal default so existing tests keep their behavior. The sync mutation test
uses the new constructor with its existing `mutation_accounts(transport)`
helper, which installs a fixed session through `Client::with_transport` and
creates the `MailAccount`. This connects the actual typed `Request`, client,
`ReqwestTransport`, and net loop in-process; a fake `HttpTransport` that returns
bytes directly would skip the defect. The new status test below gates this
instrument too.

New:

- `request_replay_policy_is_idempotent_only_when_every_call_is_replay_safe`
  (`core/request.rs` tests) - `Email/get` + `Email/query` -> `Idempotent`;
  adding an update-only `Email/set` without `ifInState` -> `Never`;
  `Email/import` alone -> `Never`; an empty request -> `Idempotent`. Bite: make
  the set macro's `REPLAY_SAFE` `true`.
- `send_request_hands_the_typed_batch_policy_to_the_transport`
  (`client.rs` tests) - a recording `HttpTransport` double captures the
  `replay` argument; `Client::send_request` with a read batch records
  `Idempotent`, with a guarded `Email/set` records `Never`. This is the seam
  between the typed request and the transport; the two tests around it cannot
  see a `send_request` that hard-codes either value. Bite: pass
  `ReplayPolicy::Never` unconditionally in `send_request`.
- `an_unanswered_id_from_a_guarded_set_reconciles` (`sync/error.rs` tests) -
  `set_id_unanswered(id, UpdateFlags ctx, true)` (guarded) -> `Reconcile` with
  actions exactly `[CheckTarget]`; with `false` -> `Retry(SameRequest)`. The existing
  `an_unanswered_set_id_is_a_reconcilable_partial_response` in
  `sync/mutation.rs` stays green. Bite: drop the stamp.
- `a_guarded_flag_write_with_an_omitted_id_reconciles` (`sync/mutation.rs`
  tests) - the propagation seam the unit test above cannot see (the existing
  omitted-id test uses `BulkDestroy`, which reconciles from its operation
  whatever the flag says). Drive `apply_batch_with_version` with an
  `UpdateFlags` target and a caller state token; assert the emitted
  `Email/set` carries exactly that `ifInState`; answer with a set response
  that names the target in neither result map; assert the item's error
  reconciles with actions exactly `[CheckTarget]`. Bite: hard-code
  `guarded: false` at the forwarding point in `classify_set_response`.
- `a_read_only_api_post_replays_after_a_drop` (`transport_reqwest.rs` tests,
  scripted net) - `api_request(.., ReplayPolicy::Idempotent)` with
  `[drop, 200]`: two requests. Control: `ReplayPolicy::Never`: one request.
  Bite: drop the `request.replay(replay)` line.
- `a_503_before_state_mismatch_reconciles_without_replay` (`sync/mutation.rs`
  tests) - use that real transport fixture with `max_attempts: 2` and zero
  backoff. Call `apply_batch_with_version` for one flag-write id `m1`, an
  `UpdateFlags` mutation, and `expected: Some("caller-old")`; the supplied
  caller state avoids a preliminary probe. Queue HTTP 503 followed by HTTP
  200 with
  `{"sessionState":"session-1","methodResponses":[["error",{"type":"stateMismatch"},"s0"]]}`.
  Assert the captured single POST has `Email/set` and
  `ifInState: "caller-old"`; the fallback response call id must equal the
  emitted call id (the fresh request starts at `"s0"`; derive it from the
  request encoder if that convention changes). The function returns an error;
  translate it through `sync::error::into_account_error` with
  `JmapErrorContext::new(AccountOperation::UpdateFlags)`. Assert kind
  `Protocol(PartialResponse)`, recovery `Reconcile(PartialCompletionSignal)`
  with actions exactly `[CheckTarget]`, exactly one wire attempt, and
  `ScriptedDispatch::remaining() == 1`. No state-refresh retry occurs.
  Control with a fresh fixture and only the HTTP 200 `stateMismatch`: the
  result is a failed item with `ConcurrencyConflict` through the existing
  caller-state arm; exactly one wire attempt. Preserve that genuine conflict.
  Bite: remove only net's new status-gate operand; the queued 200 is consumed
  and the first run now returns the false conflict. Separately pass
  `Idempotent` instead of the typed request policy in `send_request`; it
  must fail too. Restore each ablation before running the other.
- `a_dropped_set_reconciles_under_an_idempotent_operation`
  (`sync/error.rs` tests) - a `crate::Error` built from the net error a `Never`
  API POST returns on `[drop]`, classified with
  `JmapErrorContext::new(AccountOperation::UpdateFlags)`: `Reconcile` with
  `CheckTarget`. Bite: revert brick 2's stamp.

```
brokkr test -p bifrost-jmap request_replay_policy_is_idempotent_only_when_every_call_is_replay_safe
brokkr test -p bifrost-jmap send_request_hands_the_typed_batch_policy_to_the_transport
brokkr test -p bifrost-jmap a_read_only_api_post_replays_after_a_drop
brokkr test -p bifrost-jmap a_503_before_state_mismatch_reconciles_without_replay
brokkr test -p bifrost-jmap a_dropped_set_reconciles_under_an_idempotent_operation
brokkr test -p bifrost-jmap an_unanswered_id_from_a_guarded_set_reconciles
brokkr test -p bifrost-jmap an_unanswered_set_id_is_a_reconcilable_partial_response
brokkr test -p bifrost-jmap a_guarded_flag_write_with_an_omitted_id_reconciles
```

### Brick 8: references and the originating entry

Section 6. No test; the gate is the landing's `brokkr check` (gremlins and
textlint phases cover the prose).

### Landing gate

```
brokkr check
```

Expected fallout the gate will surface, and how each is resolved:

- Tests elsewhere that assert a bare `Network`/`Timeout`/`Tls` from a
  non-replayable request's single in-flight drop: assert on `terminal()` and on
  the envelope. Known: the two in brick 1.
- Tests that assert `Retry(SameRequest)` for an in-flight drop of a request
  this spec declares non-replayable under an idempotent operation: flip to
  `Reconcile` ONLY after confirming the request is declared `Never` or is
  conditional per the 3.6 tables. If the request is a read or an absolute write
  declared `Idempotent`, the flip is a bug in the declaration, not in the test.
- Tests that assert a replay of a conditional PUT/DELETE: rewrite to the new
  expectation as in brick 4.
- Tests that assert `DedupeByClientId` on a `PartialResponse` under an
  idempotent operation with `idempotency_override(false)`: the DAV wrappers
  (caldav `partial_sequence_error`, `partial_move_error` in both crates) and
  JMAP `sync/pim.rs` `partial_email_set_error`. Drop that action from the
  expectation per 3.4.1. A test that expects it under a non-idempotent
  operation must stay green unchanged; if it fails, brick 0 is wrong. NOT
  fallout, despite stamping `override(false)`: Graph `push/common.rs` and
  Google `account/mod.rs` `shutdown_account_future_error` stamp it on a
  `Transport` kind, which `derive_protocol` never sees; if one of their tests
  moves, brick 0 is wrong.
- Tests that use `RetryPolicy::disabled()` and assert a bare
  `RetryBudgetExhausted` from a single 5xx on a non-replayable request: assert
  on `terminal()` and on the envelope (3.3). This also occurs under the normal
  multi-attempt policy after 3.3.1.
- Tests expecting a `Never` request to consume a reply queued after 5xx or a
  custom retryable status: update to one attempt and reconciliation, unless
  the test's request is a pure read or absolute write that should have been
  explicitly `Idempotent`. Never broaden production policy just to retain a
  retry fixture. The custom-403 hold case is rewritten in brick 2; its
  303/307 siblings retain their original expectations.
- Tests that assert a replay of an unconditional CalDAV event PUT: rewrite to
  one transcript and `Reconcile` (3.6 scheduling row).

Keep the landing when `brokkr check` is green and every new test has passed its
bite audit; otherwise revert it whole.

## 6. Reference and note edits (part of the landing)

- `reference/net.md`: rewrite the opening paragraph and "Replay safety" around
  `ReplayPolicy` (the three policies, the method defaults, the precondition
  set and its stated exclusions, explicit-wins, the method-rewrite reset, the
  declaration rule from 3.6 including the scheduling row, the single-attempt
  envelope (both shapes), the narrowing, and the acknowledgement-versus-refusal
  rule in 3.4). Replace the "status retries deliberately NOT gated" paragraph
  and update "Retry decision": all 5xx and `policy.statuses` entries are only
  candidates; the resolved policy gates them except for 401/429 refusals,
  and a policy stop preserves the final response and hint in an empty-history
  envelope even with budget left. State the 3.3.1 cost and that a body-level
  conflict cannot follow an automatic replay of a `Never` write. Update the builder
  setters paragraph (`replay`, `idempotent(bool)` as its spelling) and the
  `AttemptHistory` and `RetryBudgetExhausted` bullets in "Error model" (the
  latter also means replay policy stopped retrying before numerical budget
  exhaustion). Correct the retry-quota paragraph to say an allowed retry
  refunds a rate slot; a policy-stopped attempt does not. Remove "The JMAP
  transport does not set it today".
- `reference/error-model.md` "Scope, operation, provider, protocol": replace the
  "Net independently uses request-level replay safety ... not equivalent"
  sentences with: the request's `ReplayPolicy` decides wire replay; when a
  non-replayable request may have committed, net's translation stamps
  `idempotency_override(false)`; the operation table is the default otherwise
  and for every non-HTTP protocol; widening is an operation-level producer
  decision on replayable requests only. In "Central derivation", the Protocol
  row becomes: `PartialResponse` -> `Retry(SameRequest)` if idempotent, else
  `Reconcile(PartialCompletionSignal, [CheckTarget])` plus `DedupeByClientId`
  when the operation table (not the override) calls the operation
  non-idempotent, with the 3.4.1 reason (protected-lane routing for destroys
  and moves, dedupe for creates and sends). Wherever the `Acknowledged` rule
  is stated, explain that an HTTP response alone does not prove refusal:
  net's 401/429 exceptions preserve the existing refusal retries, whereas a
  5xx or another configured ambiguous status on a non-replayable request
  stops wire retry and translates to `PartialResponse` with `InFlight` and
  `idempotency_override(false)`. The generic server mapper's acknowledged
  retry rule remains; this ambiguity must be represented before invoking it.
- `reference/sync.md`: the destroy read-back table is unchanged; add one line
  that a narrowed absolute write (request-level unreplayable, operation
  idempotent) arrives CheckTarget-only and is reassertable.
- `reference/caldav.md` and `reference/carddav.md`: rewrite "Which requests are
  declared unreplayable" (create-PUT and MOVE `Never`; every CalDAV event PUT
  `Never` as a scheduling write; CardDAV conditional PUT and versioned DELETE
  non-replayable through `IdempotentUnlessConditional` / `Never`, with the
  caller-version reason AND the 1.2 cost for self-fetched validators, stated
  as accepted rather than the old reasoning called wrong); keep the dav-D1
  DELETE paragraph and state that only the unconditional DELETE is
  replayable; record that the `refused_only` 401 clause is defensive and
  unreachable in DAV; update the "declaration does two
  things" paragraph to say net now narrows unreplayable drops itself and
  `dispatch_once` still stamps for DAV-minted failures; replace the test name
  list; carddav's opening paragraph likewise.
- `reference/graph.md`, `reference/google.md`, `reference/jmap.md`: replace
  each opening funnel paragraph with "every request declares its
  `ReplayPolicy`; the table lives at the call sites" plus the provider's own
  notable choices (Graph: unabsorbed DELETEs `Never`, read `$batch`
  `Idempotent`, calendar update `Never`, EWS streaming `Idempotent` with
  network retries off, the operation-table departures; Google: label modify
  `Idempotent`, People update `Never`, Calendar update and RSVP `Never`;
  JMAP: `Idempotent` iff every call is replay-safe, and the `guarded` choice
  for `set_id_unanswered`). Each reference also states that a non-replayable
  request stops at its first ambiguous retryable status, not only an in-flight
  transport drop; 429/401 refusal handling remains. JMAP and Graph explicitly
  state that `[503, 200 with stateMismatch/subrequest 412]` stops at 503 and
  reconciles, while a first-response HTTP 200 conflict stays a conflict.
  Graph's paragraph on `.idempotent()` gains the coupling from 3.6.
- Stale override-survival claims, corrected together: `into_builder` keeps
  the override (`types/src/error/account_error.rs`), as
  `reference/error-model.md` says. Three source comments say otherwise:
  `crates/graph/src/account/graph_error.rs` `apply_idempotency_override` doc,
  `crates/imap/src/account/pim.rs` `restamp` doc ("`into_builder` does NOT
  preserve builder-only" inputs), and the `with_idempotency_override` comment
  in `crates/imap/src/account/error.rs`, which defers to `restamp`'s
  reasoning. Correct all three in this landing, not only Graph's.
- `notes/bugs-shared.md`: remove the originating entry.
- `notes/todo.md`: file only the JMAP direct PIM `SetResponse::updated`
  omission (4.2). Status-replayed false conflicts and duplicate creates/sends
  are closed by 3.3.1, not filed.

## 7. Review checklist (for the cold reviewer)

- Does any production consumer match a bare transport variant of a request
  error before translation (it would now see `AttemptHistory`)?
- Can any path set `unreplayable_terminal` and then return a success? Is it
  set on a `RateLimited` return or on a replayable request?
- Can any producer apply `idempotency_override(true)` after net narrowed? (Only
  Graph's `.idempotent()` and JMAP's WebSocket frames widen; check both.)
- Does `request_is_replayable` see the same headers in DAV as in net?
- Does the method-rewrite reset weaken any explicit `Never` on a GET (it must
  not: the reset fires only when the method changes)?
- Does any production path run a non-replayable request under a
  single-attempt policy and read a top-level `RetryBudgetExhausted` as data
  (it would now see the envelope)?
- Does `subrequest_replayable` fail closed on every conversion failure?
- Does every retryable-status candidate pass the resolved-policy gate before
  any retry wait, refund or second dispatch, including custom `statuses`?
- Are 401 refresh and configured 429 retries preserved without granting 5xx
  replay to a `Never` request?
- Do the JMAP 503/200-stateMismatch and Graph 503/200-subrequest-412 tests drive
  the actual provider and net paths with budget for two attempts, assert one
  wire attempt, and fail when only the status gate is ablated?
- Does a first-response body-level precondition failure remain a real conflict?
- Are DAV MOVE's first-response 405/501 fallbacks preserved without treating
  `[503, 405]` as an obtainable fallback after this change?

## 9. Review dispositions (r1, r2)

Every finding of `notes/net-request-replay-policy-spec-r1.md` and
`notes/net-request-replay-policy-spec-r2.md` was checked against the tree at
`49cd19e7`. The 2026-10-25 adjudication accepts r1 P2-1 and r2 P1 as findings
inside SHARED-007, including P1 severity: the wire gate, successful-envelope
false conflicts, and duplicate creates/sends are resolved together in 3.3.1,
3.4, bricks 1, 2, 3, 5 and 7, section 6 and the review checklist. The requested
deterministic status scripts and independent gate/declaration ablations are
required. Their former deferral and recommendation A are superseded; section
8 is removed. Other folded findings: r2 P2 (1.1, 2.1,
3.3, 3.4, 3.5, bricks 1 and 2), r2 P3 (envelope moved into brick 1), r1 P2-2
(scheduling row in 3.6, Graph and Google tables, CalDAV `put_event`, bricks
4 to 6), r1 P2-3 (1.2 cost, 2.3 table, `set_id_unanswered` rationale), r1
P3-1 (`subrequest_replayable`), r1 P3-2 (3.5, brick 3), r1 P3-3 (3.4.1, 4.2,
brick 0), r1 P3-4 (landing gate), r1 P3-5 (EWS streaming override, brick 5),
r1 P3-6 (3.4 table and converse paragraph), r1 P3-7 (precondition set), r1
P3-8 (redirect test policy, fixture rule), r1 P3-9 (3.5 literal), r1 P3-10
(3.6 departures paragraph and Graph table), and r1's three lateral findings
(3.2 doc, section 6, brick 0).

Rejected, in whole or in part:

- **r1 P3-1, the claimed live flip.** r1 says an omitted unconditional
  `SetImportance` / `SetCategory` PATCH subresponse in `pim/messages.rs`
  would flip from `Retry(SameRequest)` to `Reconcile`. No production PATCH
  subrequest is unconditional: `patch_messages` and `mutate.rs` `SetFlags`
  always attach `If-Match`, so the method default and the declaration rule
  agree on every live request. The coherence point (the helper should follow
  the declaration rule) is accepted and folded; the regression claim is not.
- **r1 P3-5, accepting the streaming widening and listing fallout.** r1
  offered "list the streaming reconnect tests as expected fallout" as one
  resolution. Rejected in favor of preserving today's wire behavior (network
  retries off for the stream), so no reconnect test should move; declaring
  `Never` instead was also rejected because `PushStream` is idempotent and a
  `Never` drop would newly narrow to `Reconcile`.
- **r1 P2-3, the suggested "declare `Idempotent` for self-fetched
  validators".** Not adopted: it contradicts the owner's categorical
  direction. The finding's substance (state the cost, stop calling the old
  reasoning wrong) is folded, and the option is recorded in 1.2 for the
  owner.
- **r1 P2-3, `expected.is_some()` for `set_id_unanswered`.** r1 left the
  choice open; `guarded` is kept, with the reason now stated.
- **r1 P2-2, "accept the scheduling risk uniformly".** The alternative
  resolution was declined in favor of narrowing uniformly, which for Graph
  and Google preserves today's wire behavior and costs only the rare
  unconditional CalDAV event PUT replay.
- **r2 P2, the implied scope "a conditional PATCH ... receives a post-commit
  503".** The fix applies to every resolved non-replayable request's ambiguous
  retryable status, including an immediate policy stop with budget left,
  not only conditional requests or exhausted budgets. It deliberately does
  NOT newly wrap a single 429 (3.3): a 429
  is a pre-processing refusal, and wrapping it would hide the throttle hint
  and scope.
