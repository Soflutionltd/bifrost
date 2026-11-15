# Hunt: net-dav (bifrost-net, bifrost-dav-core, bifrost-caldav, bifrost-carddav)

A reading. No code was changed. Findings are ranked within each section by
how much they would buy. Cross-crate findings (Graph, Google, JMAP, IMAP as
consumers of net and DAV) are included where the question led there.

Verdict up front: the right move is a substantial rewrite in two places, not
a series of patches.

1. **bifrost-net needs a public "exchange" core** - one hop state, one attempt
   ledger, one deadline, and per-hop policy hooks (credential minting,
   method-rewrite mode, statuses-as-data). Today DAV cannot use net's redirect
   walk, 401 recovery, deadline or attempt evidence, so it re-implements all
   four, and the copies already disagree (D2, D3). Several other decisions
   are made more than once for the same reason (D1, D5, D6).
2. **bifrost-dav-core is half a consolidation.** It owns the dispatcher, the
   207 machine and the snapshot codec. But the multiget fan-out, the
   filtered-query lanes, relocation, conditional CRUD, the cursor wrappers,
   the content-line codec and about sixty stub methods are still mirrored
   line for line in caldav and carddav, with comments saying "twin of ...
   must stay in step". `DavProtocol` should become a `Dialect` trait, and a
   generic collection-account kernel should move into dav-core (S2, S3).

---

## 1. Decisions made in more than one place

### D1. HTTP status -> `AccountErrorKind` / `Cause`: five ladders, already disagreeing

The question: what does HTTP status N mean for recovery?

Sites that answer it:

- `bifrost-net` `account_error.rs`: `status_kind_cause`. Inside the same file,
  `server_cause_from_status` answers a sub-question a second time
  (408/502/503/504, 429, 507, 5xx).
- `bifrost-dav-core` `error.rs`: `status_error`. This one is two parallel
  if-chains, one for the kind and one for the cause, held in step inside a
  single function.
- `bifrost-graph` `graph_error.rs`: `classify_by_status`. A near-copy of
  net's, down to the `(kind, cause, bool)` tuple and its own
  `resource_from_scope` / `id_from_scope`.
- `bifrost-google` `account/error.rs`: `classify_by_status`.
- `bifrost-jmap` `sync/error.rs`: `convert_problem`, a status arm combined
  with the problem type, plus its own `resource_from_scope` / `id_from_scope`.

They already disagree:

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

Graph's own comment states the 412 split outright ("a bare 412 is
`ConcurrencyConflict` here and a bare `Server(Error)` in bifrost-net").

This matters operationally. Every consumer that falls through to net's
funnel - including DAV's unreplayable histories and Graph's non-envelope
bodies - gets net's table. The same 412 is therefore a conflict on one path
and an unknown server error on another, inside one account.

The scope-to-resource projections disagree as well. JMAP maps
`CalendarCollection` / `ContactCollection` to Calendar / Contact and has no
`Event` arm. Net and Graph map `Event` and drop both collection variants.

Single owner: one classification function, exported by bifrost-net (or
bifrost-types if the IMAP-side callers want it). Its shape would be roughly
`classify_status(status, &StatusContext) -> StatusClass { kind, cause, throttles: bool }`.
Provider refinements become a declared override set passed in, not a copied
table: cursor-context 400/410, Gmail's history endpoint, DAV's 412/423, and
Graph's envelope codes. `ErrorScope::resource_kind()` and `ErrorScope::id()`
belong on the type in bifrost-types, replacing three private copies.

### D2. "May this logical request have committed?" decided four ways

The question: given the attempts so far, what is the effective transmission
state, and is replay forbidden?

Sites:

- **`request.rs` send loop.** It sets
  `may_have_committed |= !replayable && status != 401`, sets
  `unreplayable_terminal` via `left_the_process`, and decides "proven refusal"
  in `status_error_is_replayable` (401 and 429 only, with a comment that
  502/503/504 can follow a commit).
- **`account_error.rs`, `AttemptHistory` arm.** It re-derives everything from
  the translated cause chains: it excludes 401s (`rejected_unauthorized`),
  computes `acknowledged_may_have_committed`, and reclassifies to
  `PartialResponse` when `inflight || (acknowledged && terminal == Some(Unsent))`.
- **`account_error.rs`, `LocalHold` arm.** A different fold of the same
  states, with no 401 exclusion.
- **dav-core `RedirectHistory`.** `record_response` applies its own
  `unreplayable && status != 401` rule. `attach` reclassifies to
  `PartialResponse`/`InFlight` when `unreplayable && may_have_committed` and
  the terminal state is `None | Some(Unsent)`.
- **dav-core `wire_outcome`.** Its `refused_only` treats earlier attempts of
  401, 429, **503** and 501 as proven refusals.

Where they disagree:

- Net reclassifies on "acknowledged earlier attempt + `Unsent` terminal" even
  for a replayable request. DAV reclassifies only for an unreplayable one.
- On a terminal with no attempt evidence (`None`, for example a credential
  error), DAV reclassifies. Net pushes an Acknowledged attempt instead and
  keeps the terminal's kind.
- DAV's refusal set includes 503, which net's own comment says does not prove
  refusal. Today this is dead for unreplayable requests, because net never
  retries past a 503 on them. It is a second rule waiting to drift.

Single owner: a public `AttemptLedger` in bifrost-net. It records typed
outcomes, owns the one `is_proven_refusal(StatusCode)` predicate and exposes
a verdict (`CommitEvidence::{Unsent, Acknowledged, MayHaveCommitted}`). Net's
loop and any self-walking caller (DAV) both feed it, and `account_error`
reads the verdict rather than re-mining `Cause::Attempt` entries out of
already-translated errors.

### D3. Two redirect walkers, two 401-refresh-once paths, two deadlines

The question: how is a 3xx followed, and what does the next hop carry?

Sites: `request.rs::send_streaming_inner` (net), and dav-core
`DavDispatch::send_raw_until`, with `RedirectHistory`, `rebase_destination`
and `mint_auth_until`.

DAV's stated reason for the second walker is that net strips `Authorization`
on an origin change and cannot restore it. That is a missing hook in net's
API, not a reason for two walkers. The copies differ in policy:

- **Method rewriting.** Net rewrites 301/302 on unsafe methods and every 303
  to GET. DAV preserves the method on 301/302/307/308 and refuses to follow
  303.
- **Origin comparison.** Net uses `same_origin(&Url, &Url)`. DAV uses
  `url_origin(&str) -> String`, and a third rule lives in `dav_href_key`.
- **Hop cap.** The two are kept in step only by both naming
  `DEFAULT_MAX_HOPS`. That is a constant shared by convention.
- **Deadline.** DAV keeps its own `Instant + DAV_CLIENT_TIMEOUT`, hand-maps
  its expiry to `NetError::Timeout { Unsent }`, and also sets
  `request_timeout` on the spec. `RequestDeadline` would own all of that, but
  it is `pub(crate)`.

Single owner: net's walk, parameterized by a per-hop policy trait with
roughly these parts:

- `admit(prior, next) -> HopDecision`, carrying the credential to present
  (re-minted per origin) or a refusal;
- the method-rewrite mode (RFC 7231 rewrite, or preserve-and-refuse-303);
- a header rewrite hook (the DAV `Destination` rebase);
- the dialect target guard.

DAV's `send_raw_until`, `RedirectHistory`, its 401 handling and its deadline
would then disappear.

### D4. "Wrap a raw token source in the single-flight refresher": two crates do it, DAV does not

Sites:

- `Net::attach_account` wraps any non-coordinating source in an
  `OAuthRefresher`.
- JMAP's `Authorization::from_credentials` does the same thing on its own.
- DAV does neither. `dav_account_spec()` attaches with
  `AccountSpec::new(None)`, and `DavCredentials::Bearer { token_source }` is
  the consumer's raw source. `mint_auth` calls `token_source.current()` on
  every request, and the 401 path calls `refresh_if_unchanged`, whose trait
  default is a plain `refresh()`.

The effect: a raw provider `TokenSource` passed to
`CalDavCredentials::bearer_source` or `CardDavCredentials::bearer_source`
gets none of the single-flight, proactive-refresh, max-age or
failure-backoff guarantees the reference promises every other HTTP crate.
This is likely a live defect for any consumer that supplies a
non-coordinating source. I did not check whether IMAP XOAUTH2 or SMTP have
the same gap.

Type fix: a `CoordinatedTokenSource` newtype that can only be minted by
`coordinate(Arc<dyn TokenSource>)`, which wraps when needed. `AccountSpec`,
JMAP and DAV hold that type, and the `is_coordinating()` bool becomes
unnecessary. DAV can keep minting headers itself through `AccountNet::bearer()`
if it attaches the source to its spec.

### D5. Throttle-scope stamping: three sites for DAV alone

Sites:

- net `status_kind_cause` returns a throttle `bool` for 429/507;
- DAV `status_error` stamps `ThrottleScope::Account` on 429 and 507 itself;
- DAV `net_account_error` passes `Some(Account)` into net's funnel;
- DAV `dispatch_once` re-stamps `Account` afterwards on any `RateLimited` or
  `QuotaExhausted` kind;
- Graph's `stamp_budget` re-stamps after net;
- JMAP passes `Account`.

The test `generic_transport_throttle_matches_dav_status_policy` exists to
hold two DAV copies in step. That is the pairwise-agreement shape the brief
names.

Single owner: the classification from D1 returns `throttles: bool`. The
provider context supplies the scope (and Graph's budget) exactly once.

### D6. The final idempotency override is decided by three crates, and one can widen

Sites:

- **Net narrows** with `idempotency_override(false)`.
- **DAV narrows three more times** for the same request: in `dispatch_once`,
  in `send_raw_request`, and in `RedirectHistory::attach`.
- **Graph re-stamps after net and can undo net's narrowing.**
  `apply_idempotency_override` replaces the override. Its comment admits that
  a widening context is valid only if every request was declared
  `Idempotent`, and nothing enforces that.

Type fix: carry the caller's override on `NetErrorContext`, so net composes
it, and make the builder override monotone. A `narrow_idempotency()` that
cannot widen, or an explicit `widen_idempotency(proof)` that requires
evidence, would both work.

### D7. "Is this entity tag weak?": five sites

Sites:

- dav-core `etag::normalize_http_etag`;
- dav-core `xml::normalize_etag` - the same rule implemented a second time
  inside the same crate, and both are exported;
- dav-core `etag::prepare_if_match`, with its own weak check;
- `caldav::account::put_condition`;
- `carddav::account::put_condition` - byte-identical to the CalDAV one, and a
  third weak check.

The validator travels as `String`, both in DAV and in
`ServerVersion::ETag(String)` in bifrost-types, with the weak marker kept as
text precisely so later code can re-detect it.

Type fix: an `EntityTag { Strong(String), Weak(String) }` parsed once at the
wire, with `PutCondition::IfMatch(&StrongTag)`. A conditional write on a weak
tag then cannot be expressed. Graph's `If-Match`, Google People and Google
Calendar etags are candidates for the same type.

### D8. Href identity: several rules, and the paging lanes use the weak one

The question: are these two hrefs the same resource, and in what order do
they sort?

Sites:

- `dav_href_key` / `same_dav_url`: decoded path segments, host case, default
  port. Used by the snapshot diff, `DirectoryGuard`, and relocation checks.
- `sorted_candidate_hrefs`: raw string sort and dedup.
- `slice_after_watermark`: raw string `>` comparison against the watermark.
- `diff_snapshots`: raw `uri ==` for tie-breaking inside one key bucket.
- `url_origin`: a string origin for the credential gate.
- net `redirect::same_origin` on `Url`.
- `DirectoryGuard::contains_url`: a hand-rolled ancestor walk.
- `DavDispatch::resolve_url` versus `xml::resolve_href`: two href resolution
  rules that join relative hrefs differently (with and without a trailing
  slash on the base).

Consequence: two spellings of one resource that a query returns survive
`sorted_candidate_hrefs` and both land on a page. Watermark ordering is on
raw bytes, while identity is on the key.

Type fix: a parsed `DavHref`, with its key and order derived once, and an
`Origin` newtype in bifrost-net that both redirect walkers and the DAV gate
compare on.

### D9. Multistatus lane assignment, re-derived per accessor, with two verdicts on one input

`ResponseParts` exposes `entry_href`, `failed_href`, `failed_member`,
`fetched`, `failed_resource`, `missing_data_href` and `member_status_code`.
Each recomputes which lane the response is in from the same flags.

Take one input: a response whose only propstat failed with an unparseable
status line. The two lanes read it differently:

- **Listing lane.** `failed_member` returns a member with `status: None`.
  `classify_207` reads `None` as "not a missing resource", so the input can
  drive a `CompleteFailure`.
- **Multiget lane.** `failed_resource` returns `None`, so the same response
  falls into `missing_data_href`, the benign lane. The "accepted edge" comment
  on `failed_resource` documents this for multiget only.

Single owner: `ResponseParts::lane() -> ResponseLane { Collection, Entry, Failed { status }, MissingData }`,
computed once, with every accessor reading it.

### D10. The "empty listing is no observation" rule, written three times

`diff_snapshots`, `preserve_unobserved_entries`, and `Observation::complete`
each encode "empty current against populated previous means nothing was
observed". They agree today.

Single owner: an `Observed` value built once from
`(previous, current, failed)`, which the diff and the token-advance decision
both consume.

### D11. Small duplicated predicates in dav-core

- **404|410 means "the target is gone".** `FailedResource::is_missing_resource`,
  `worst_failed_status` and `delete_target_already_gone` each spell it.
- **Throttle statuses.** The 429|507 set appears in `status_error` and again
  in net.

Each should be one predicate on a status type.

### D12. CalDAV RSVP availability: two gates that disagree on blank values

`scheduling_available` treats a whitespace-only address or outbox as absent.
`event_rsvp` gates on `let Some(..)` only. A blank configured address
therefore advertises `event_rsvp = false`, yet the method proceeds.

Type fix: compute `Option<SchedulingEndpoint { outbox, address }>` once at
open. Derive the capability from `is_some()` and pattern-match the same value
in the method.

### D13. Capability flags versus method bodies (both DAV crates)

`PimMethodSupport` and `MutationConcurrency::ExpectedVersion` are hand-set
bools. The `Account` impl separately returns `Unsupported` from each method
body. `capability_contract_tests.rs`, about 585 lines in each crate and
mirrored between them, exists to pair the two.

This is a bifrost-types-level shape question. The DAV kernel in S2 would at
least let one dialect-generic impl and one test serve both crates.

### D14. CalDAV picks two different error scopes for "one event resource"

`client::event_scope` deliberately overloads `ErrorScope::Calendar` with an
event URL. Its comment justifies that by saying bifrost-types has no
`ErrorScope::Event`. That variant exists: `relocation_error` and `rsvp_error`
in the same crate use it, and so do net and Graph.

Related: `DavProtocol::resource()` returns `Calendar` for CalDAV but
`Contact` for CardDAV. The CalDAV answer names the collection and the CardDAV
answer names the item, so a 404 on an event GET is `NotFound(Calendar)` while
a 404 on a contact GET is `NotFound(Contact)`.

One crate now holds two answers, and the stated reason for the first has
gone.

### D15. Resumable chunked upload: Google Drive and OneDrive drive the same protocol separately

Google's `upload_file_chunked_progress` and Graph's `upload_chunks` /
`admitted_resume_offset` repeat the same pieces: window arithmetic, the
`bytes {offset}-{end-1}/{total}` header, resume-offset admission,
final-chunk tracking for the partial-commit record, and redirect refusal.
They disagree in two places:

- **A resume that made no progress.** Google classifies it as
  `Internal(LimitExceeded)` ("Drive documents resending"). Graph classifies it
  as `ContractViolation`.
- **Attempt budget.** Google has a total attempt budget. Graph has none and
  relies on strictly increasing offsets.

Single owner: a resumable-upload driver in bifrost-net that takes a provider
closure to parse the server's resume offset. The provider-specific
no-progress rule should be an explicit parameter, not a fork.

### D16. "Send to a pre-authenticated session URL" is three bools that must agree

`without_bearer_auth().credential_url().follow_redirects(Disabled)` is the
same triple at three sites: Graph `execute_aux` for `AuxTarget::Anonymous`,
and Google `cloud.rs` twice. Forgetting one of the three leaks a token,
leaks the URL into diagnostics, or replays a chunk to a redirect target.

Single owner: `AccountNet::session_request(method, &SessionUrl)`, with a
`SessionUrl` newtype whose `Debug` and `Display` redact.

---

## 2. Axes that should be types

### A1. The net `Error` evidence fields

- **`may_have_committed: bool` and `attempts: Vec<Error>`** sit on two
  variants, and the recursion allows nested `AttemptHistory`. That is why
  `terminal()` recurses. `AttemptEvidence` carries two bools
  (`may_have_committed`, `unreplayable_terminal`) that `finish` ORs back into
  one. A typed ledger (D2) replaces all of it.
- **`RetryBudgetExhausted` means two things.** It covers both "the attempt
  budget is spent" and "the replay policy forbade another attempt"; its doc
  says so. An operator or machine caller cannot branch on which happened,
  even though the second is the commit-ambiguity case. Split it, or add a
  `StopReason` field.
- **The server's retry hint has four representations:**
  - `RateLimited.retry_after: Option<Duration>`;
  - `RetryBudgetExhausted.retry_after_history: Vec<Duration>`, capped and
    resolved, so lossy;
  - `RefreshFailed.retry_after: Option<SystemTime>`;
  - `bifrost_types::RetryHint`.

  `account_error` re-parses the `Retry-After` header because the stored
  durations have already lost the HTTP-date form. Store `RetryHint` at
  observation time and derive the capped wait from it.

### A2. Registry keys are unnamespaced strings

`Net` holds three registries keyed by bare strings, and the rate governor
adds a fourth key space on top of the host:

- **Quota scope.** `RequestBuilder::quota_scope(String)` and
  `RateLimit::with_quota_scope(String)`, where an empty string means "the
  host-only compatibility bucket".
- **Holds.** `hold_gate(String)` and `shared_hold(String)`.
- **Concurrency limits.** `shared_concurrency_limit(String, usize)`.

Google builds one key as `format!("gmail:{:?}", (account, origin))`, so the
`Debug` output of a tuple has become a registry key. Graph uses the raw
account id as its quota scope.

Type fix: `QuotaScope` (with an explicit `HostOnly` variant), `HoldKey` and
`LimitKey` newtypes. Better, one provider-minted `BudgetKey` that names the
quota, the hold and the concurrency scope together. Google currently spells
that one fact as three strings.

### A3. URLs travel as `String` through every layer

Every layer re-parses URLs held as `String`:

- `RequestBuilderInner.url`, re-parsed for the host on every hop and again
  for redirect classification;
- `redirect_guard: Fn(&str, &str)`;
- `RedirectStep.next_url: String`;
- `DavRequest.url`, `DavResponse.url` and `DavBody.url`;
- `DavDispatch::trusted_origins: Vec<String>` and `base_url: String`;
- every DAV native id.

`reqwest::Url` is already a dependency. With an `Origin` and a `DavHref` on
top (D8), several of the D3 and D8 disagreements become unrepresentable.

### A4. Timeout configuration has hidden tri-states and aliases

- `RequestBuilderInner.timeout: Option<Option<Duration>>` encodes "unset,
  explicitly none, or a value". An enum would say so.
- `AccountSpec::connect_timeout` is a legacy alias for
  `response_headers_timeout`, so two fields carry one meaning and a
  precedence rule lives in `attach_account`. (Changing the published shape is
  ruled fine. Removing the field needs an owner ruling.)

### A5. `RedirectPolicy`: a bool overrides a set

`same_origin_only: bool` beside `trusted_hosts: HashSet<String>`. The bool
silently overrides the set, and in that mode `allows_host` returns false for
every host, including the departing one; the docs warn about exactly that.

Type fix: `CrossOrigin { Any, AllowList(set), Forbidden }`.

### A6. Ranges are encoded to a string and parsed back

`encode_range(ByteRange) -> String`, and then `content_range_matches(&str, &str)`
parses that string back to recover the numbers. The typed `ByteRange` should
reach the validator.

### A7. DAV status and failure carriers

- **Statuses.** `FailedResource.status: Option<u16>`,
  `ResponseParts.response_status` and `staged_status` as `Option<String>`, and
  `staged_success: Option<bool>`. `complete_failure_error` turns a missing
  status into a synthetic 500.
- **Failures as prose.** `parse_multistatus`, `extract_*` and
  `decode_snapshot` return `Result<_, String>`, and the prose is wrapped into
  an `AccountError` later.
- **Stringly request parts.** Depth is passed as `"0"` / `"1"`. Methods are
  rebuilt with `Method::from_bytes(b"PROPFIND")` at each call site.
  `DirectoryGuard` checks `method.as_str() == "REPORT"`.

A `MemberStatus` type, a `Depth` enum, method constants and a typed
`MultistatusError` would cover these.

### A8. The DAV cursor carries two version axes and a duplicated collection URL

- **Two version axes.** The magic bytes carry a version (`CALDAVET1`,
  `CDAVCTAG1`). `OpaqueChangeState.envelope_version` also carries one, set
  from a `CURSOR_ENVELOPE_VERSION = 2` constant repeated in each crate.
- **Duplicated collection URL.** The URL is stored both in the
  `CursorScope::Folder` and in the cursor bytes, and the two are
  cross-checked with `same_dav_url` on every decode.
- **One field, two meanings.** `token: Option<String>` is a sync-token in one
  dialect and a getctag in the other.

### A9. A DAV collection is a `CursorScope::Folder(FolderId)`, so IMAP resolves ownership by string collision

When DAV is composed into IMAP, `DavScopeIndex` decides which sub-account
owns a scope by comparing `FolderId` strings with IMAP mailbox names and the
other dialect's collections. When two collide, it emits a prose `Warning`:
"... this collection will not sync".

A typed provenance would make the collision impossible to express: either a
dedicated scope variant for a DAV collection, or a `FolderId` that carries
its namespace.

### A10. Credential carriers repeat each other

- **Three identical credential enums.** `CalDavCredentials` and
  `CardDavCredentials` are identical published enums. Both convert into
  `DavCredentials`, which is identical again.
- **Two Basic encodings.** JMAP's Basic credential stores a pre-encoded
  value; DAV stores username and password and encodes per request.

A shared `HttpCredential` in bifrost-net would carry the D4 coordination
rule. Removing or renaming the published enums needs an owner ruling;
reshaping them into wrappers does not.

### A11. Smaller items

- `HoldKind { RateLimited, QuotaExhausted }` restates two `ServerErrorKind`
  variants, and the `LocalHold` arm re-maps them.
- `AccountNetInner.priority` is an `AtomicU8` decoded with an `_ => Normal`
  fallback.

---

## 3. Structure

### S1. `send_streaming_inner` is one function doing five jobs

It runs to about 580 lines and builds `InternalStreaming` in four places.
The hop state is a dozen `let mut` locals rewritten by hand in the redirect
arm: method, url, body, headers, host, quota scope, cost, cost units,
`auth_for_next_hop`, `carrying_auth` and `auth_self_stripped`. Those locals
are exactly the state DAV rebuilds as `DavRequest` plus its own flags.

Proposed split:

- `Hop`, with `follow(step)`, `rate_key()` and `credential()`;
- `AttemptLedger` (D2);
- the public `RequestDeadline`;
- a pass-through outcome constructor.

Combined with the per-hop policy trait from D3, this is the exchange core
the verdict describes.

### S2. dav-core stops at the dispatcher; the account layer is still mirrored

Still duplicated line for line, with "twin of" or "must stay in step"
comments, in caldav and carddav.

**`client.rs`:**

- `MULTIGET_BATCH_SIZE` and `MULTIGET_LEG_CONCURRENCY`;
- `fetch_events` / `fetch_vcards`: chunking, body building, buffered
  fan-out and the `worse_recovery` fold;
- `accumulate_leg`, `run_leg`, `MultigetLeg` and `MultigetFetch::settle`;
- `href_query_leg` and `HrefLeg`;
- the per-property text-search fan-out with the `FilterUnsupported` degrade;
- `multiget_failure`, `listing_failure` and `complete_failure_error`;
- `QUERY_HREF_PROPS`;
- the six error-constructor shims.

**`account.rs`:**

- `RelocateFailure` and `relocation_error`, byte-identical apart from the
  scope constructor;
- `relocate_*` (MOVE, with copy-then-delete fallback);
- `put_condition` and `resource_file_name`;
- `worse_recovery_option`;
- the cursor wrappers: `cursor_from_snapshot`, `encode_cursor_snapshot`,
  `decode_cursor_snapshot`, `cursor_error`, and the diff and preserve
  wrappers;
- `validate_*_scope`, `collection_url_for_scope`, `scope_collection_url`,
  `collection_coverage_domain`, `default_collection_url` and
  `discovered_collection_urls`;
- `skipped_*_scope`, `one_outcome_per_id` and `decode_*_page_cursor`;
- `*_PAGE_SIZE = 250`;
- about sixty mail, compose, filter and settings `Unsupported` stubs per
  crate;
- `unsupported_future` and `unsupported_stream`.

Tests are mirrored as well. The two `client.rs` test modules re-test
dav-core's dispatcher under the same names in both crates: credential
origin, cross-origin redirect, hop count, plaintext home, the discovery
fallbacks, and the multiget concurrency bound.

Proposal:

1. Turn `DavProtocol` into a `Dialect` trait carrying everything that
   genuinely differs: multiget element and namespace, data property, file
   extension, item `ErrorScope` and `ResourceKind`, cursor magic, token kind
   (sync-token or ctag), listing and multiget parsers, and the typed item.
2. Move a generic `DavCollectionAccount<D: Dialect>` kernel into dav-core:
   snapshot polling, inventory, changes (with a dialect hook so CalDAV can
   use `sync-collection`), paging, relocation, conditional CRUD, and the
   unsupported lanes.
3. CalDAV keeps iCalendar, scheduling, recurrence ids and `sync-collection`.
   CardDAV keeps vCard, the directory guard and the ctag lane.
4. Move the mirrored tests into dav-core, parameterized over both dialects.

This is the same move `cdfcaa4f` and `f218f809` made, applied one layer up.

### S3. The iCalendar and vCard content-line codec is duplicated

`ical.rs` and `vcard.rs` each carry fold-at-75-octets, `escape_text`,
`unescape_text`, RFC 6868 `escape_param` / `unescape_param`, `text_value`
and `param_value` validators, and a write-error struct (`WriteError`,
`VCardWriteError`). The vCard fold even says "(matches fold_ical_lines)".
They agree today; `escape_text` replace order differs but is equivalent.

Single home: a `content_line` module in dav-core, or a small crate. It
covers RFC 5545 section 3.1 and RFC 6350 section 3.2 framing, TEXT escaping,
caret parameters, and per-value-type validators behind one error type.

### S4. Per-account state on `Net` is split across registries, and the meter cannot be shared

`Net` keeps, each keyed separately:

- the meter map, refcounted per id;
- the bandwidth registry, weak entries per id;
- `account_hosts`, per id and per registration token;
- the concurrency registry, by string key;
- the hold registry, by string key.

`attach_account`, `detach_registration` and `retag` must touch each of them
correctly.

The bandwidth registry can be injected (`Net::with_bandwidth_registry`), but
the meter cannot: `Net::new` always builds a private one. `Net::meter()`
returns `&BandwidthMeter`, and `BandwidthMeter` is not `Clone`. Meanwhile
`SyncEngineBuilder::with_bandwidth_meter`, `ImapAccountFactory::with_bandwidth_meter`
and `MeterSinkHandle::from_meter` all require an `Arc<BandwidthMeter>`.

As far as I can see, no consumer can hand the engine (or IMAP) the meter
that HTTP traffic is actually recorded into. If so, `Control::bandwidth_observed`
reads zero for JMAP, Graph, Google and DAV accounts. This is likely a live
wiring defect; verify before acting.

Single owner: one injected `BandwidthPlane { registry, meter }` (or a
`NetResources`) shared by Net, the engine and the IMAP factory. Behind it, an
`AccountRegistration` value that owns every per-account entry, so that
detach and retag are one operation each.

### S5. Net forces DAV to rebuild responses it has already turned into errors

Net turns every 4xx and 5xx into `Err`. DAV, Graph (`graph_envelope_evidence`)
and Google (308 handling, `UnexpectedResponse`) all need some statuses as
data.

DAV's `wire_outcome` therefore unwraps `AttemptHistory.terminal()` to rebuild
a response, special-cases 501 inside `RetryBudgetExhausted` for the MOVE
fallback, and carries its private refusal list from D2.

Proposal: a per-request `statuses_as_data(set)` mode that returns
`Ok(Response)` for the listed statuses with the ledger attached. Classifying
the response then becomes the caller's call against the shared ladder from
D1.

### S6. Net carries DAV-specific URL helpers

`url::well_known_url` (RFC 6764) and `url::parent_collection_url` are used
only by DAV. `status_line` is HTTP grammar and belongs in net. The other two
belong next to the DAV `Url`, `Origin` and `Href` types if those are
introduced. Low priority.

### S7. bifrost-net's module docs are out of date

The `lib.rs` crate doc still talks about "Phase 2 ... frozen for Phase 2
consumers" and lists only JMAP, Gmail and Graph as users; DAV now rides it
too. The docs need rewording so they stop naming a stale consumer set.

---

## 4. Lateral findings (outside the three questions, or likely bugs)

- **L1, likely defect: DAV bearer sources are never coordinated** (D4). A raw
  `TokenSource` behind `CalDavCredentials` or `CardDavCredentials` gets no
  single-flight, no proactive refresh and no failure backoff.
- **L2, likely defect: the HTTP bandwidth meter is unreachable** (S4). The
  engine and IMAP cannot be given the meter HTTP traffic is recorded into.
- **L3, cross-dialect divergence: CardDAV writes the file extension into the
  vCard UID.** `contact_create` builds `id = "{uuid}.vcf"` and passes that as
  the UID. CalDAV uses a bare UID and puts the extension only on the path.
  This is one decision, "new resource UID and file name", answered two ways.
- **L4, stale comment and dead state in IMAP's factory.** `factory.rs` says
  both DAV crates sync exactly one collection and report the rest on
  `skipped_scopes`. Both DAV factories return an empty skip lane, and
  `caldav::lib` says every collection is a cursor scope.
  `DavAttach::Degraded.transient` is computed and carried under
  `allow(dead_code)`.
- **L5, stale rationale: `caldav::client::event_scope`** argues that
  `ErrorScope::Event` does not exist (D14).
- **L6, Google rate buckets.** Calendar rides Gmail's quota-unit bucket on
  `www.googleapis.com`, while the comment says Calendar bills per request.
  That is the "two quotas on one host must use `quota_scope`" case
  `reference/net.md` warns about. Calendar requests debit one Gmail unit per
  call from the per-user Gmail bucket.
- **L7, Google `execute_builder` takes the service as a string.**
  `"Gmail API"` is matched by string, with `_ => ApiSurface::Drive` as the
  default. `surface_for_url` derives the same surface from the URL, so a typo
  silently changes concurrency, gating and hold attribution. It should take
  `ApiSurface`.
- **L8, Graph can widen an idempotency narrowing net applied** (D6).
- **L9, `classify_redirect` and replay disagree on "safe method".** The
  redirect rewrite treats only GET and HEAD as safe; replay uses
  `Method::is_safe()`, which adds OPTIONS and TRACE. A 301 on OPTIONS is
  rewritten to GET. Minor.
- **L10, duplicate hrefs on one page.** `sorted_candidate_hrefs`
  deduplicates on raw strings, so equivalent href spellings appear twice on
  one page (D8).
- **L11, credential gate decided twice in DAV.** `DavDispatch::auth_headers`
  and `send_raw_until` both gate on `is_trusted_url` with different error
  texts. That is a deliberate re-check at a chokepoint, not a finding; noted
  so it is not re-filed.
