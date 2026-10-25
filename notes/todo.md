# TODO

## What belongs here, and what does not

This file holds OPEN work: defects to fix, decisions the repository owner
has not made, and ruled work that has not landed. Nothing else.

- **A completed item is DELETED**, never annotated DONE or CLOSED. Its
  "what" is in the code and `reference/`; its "why" is in git history.
- **A known limit, an accepted non-defect, a deliberate trade, a coverage
  boundary, or a "do not re-file this" marker does NOT go here.** It goes
  to an inline comment at the code it describes - the function, constant,
  match arm or test whose behaviour it explains - written so the next
  reader of that code understands the limit, why it is accepted, and what
  would change it. Where a consumer relies on it, it is also stated in the
  crate's `reference/` doc. A bullet in this file is invisible from the
  code, and every past close-out has had to move such bullets there by
  hand; put them there directly.
- **A rejected proposal gets the same treatment**: a comment at the code
  the proposal would have changed, stating the ruling and its condition
  for re-raising. Code comments never cite `notes/` paths.
- Verify every item against the code before working it. This file has no
  truth guarantee; `reference/` and passing tests are authoritative and a
  finding that contradicts them loses. Fix only what is named and
  confirmed; a change that removes or renames a published item stops and
  asks the owner, whatever the argument. A shared-crate change (`types`,
  `net`, `sasl`, `sync`) is verified with the full-workspace `brokkr
  check`, never `-p`.
- An existing test you must modify is a red flag; justify it explicitly.

The order below is a priority triage of the findings taken at face value,
not verified against the code. Verification happens when an item is worked.

## 1. Priority

Items that leak something or report success for something that did not
happen, plus one surface gap that matters if the consumer relies on it.

- **Upload session URLs can reach support exports.** bifrost-net's
  `send_error_to_error` builds `Error::Network { message }` from reqwest's
  Display, which names the request URL, and `support_cause_from_source` keeps
  that message as support-only text. For the Graph and Google chunk PUTs the
  URL is the pre-authenticated session credential, and the chunk-PUT error is
  the failure leaf. The session cancel DELETE already passes through
  `bifrost_net::Error::redact_request_text` at both cancel sites; the chunk PUT
  still needs URL scrubbing at the net boundary or the upload call sites.
- **Graph mixed flag operations silently discard unmodeled tokens while
  reporting Applied** (`patch_for_flags` and `flag_op_is_unexpressible`). The
  consumer believes a keyword is set that is not, and nothing corrects it. The
  read-back flag judge already refuses to decide these operations; the write
  outcome needs its own answer, likely refusing the unexpressible part.
- **IMAP sends report success when the Sent copy failed.** `send_message`,
  `send_raw_message` and `draft_send` log a failed Sent APPEND and return
  success; `draft_send` likewise logs a post-send discard failure. The caller
  cannot learn that the Sent copy is missing or the draft lingers. The mail did
  go out, so this is not an error; it needs a "sent, but" signal (a warning on
  the success value, or the partial-commit record). These paths return Ok after
  SMTP commits, so they are outside the adopted failure-path contract in
  `account/pim.rs`.
- **Legacy `imap1` object ids fail correlation.** They decode, but outcomes
  re-encode as `imap2`, so legacy ids fail correlation in mutation and
  hydration. Matters exactly when a consumer still holds stored `imap1` ids.
- **`SyncEngine` exposes almost no calendar primitives.** It forwards contact
  primitives, but of the calendar `Account` methods only `event_delete` and
  `event_delete_if_version` (`crates/sync/src/engine/passthrough.rs`). List,
  range, get, create, update, RSVP and search have no passthrough, and
  `event_rsvp_if_version` needs one too:
  `pub async fn event_rsvp_if_version(&self, account_id: &AccountId, event: EventId, status: RsvpStatus, expected_version: Option<ServerVersion>) -> Result<(), Error>`.
  An omission only if the engine is meant to be the consumer's single door
  for calendar, as it is for contacts.

## 2. Blocked on a live probe

Nothing hermetic can settle these.

- **Google `events_in_range` combines `orderBy=startTime` with
  `showDeleted=true`.** `crates/google/src/account/calendar.rs`. Google's docs
  bless `showDeleted=true` with `singleEvents=true`, and `orderBy=startTime`
  requires `singleEvents=true`, but there are long-standing reports of the live
  API answering 400 "The requested ordering is not available for the particular
  query" for some `showDeleted` + `orderBy` combinations, and cancelled
  instances have no `start` to order by (only `originalStartTime`, which the
  projection substitutes). If Google rejects the combination, every production
  range read fails: top severity if real.
- **Graph directory search: the alias `$select` may cost directory reach.**
  `otherMails` and `proxyAddresses` sit outside the property set
  `User.ReadBasic.All` grants, as do `businessPhones`, `companyName`,
  `jobTitle` and `department`. A tenant holding only basic directory consent
  may now 403 the WHOLE `/users` query where it previously succeeded. It maps
  to `NoPermission` correctly, but a tenant with working directory search can
  lose it. Confirming needs a live ReadBasic-only tenant. If it fires, the fix
  is a select-narrowing retry after the first 403 (narrow all properties
  outside the basic set and re-issue), NOT dropping them from the query, which
  would give every tenant the degraded projection.
- **Graph event delta without `$select` may not return `changeKey`.** Event
  inventory and changes pages no longer send `$select` (Graph documents it as
  unsupported on event delta) and receive Graph's default delta property set.
  If that set omits `changeKey` or `recurrence`, the event fingerprint degrades
  to `Unavailable`.

## 3. Blocked on downstream

- **Retire `RATATOSKR_TEST_GCAL_ENDPOINT`.**
  `GoogleAccountFactory::with_calendar_api_base` exists and takes precedence.
  The environment variable was KEPT on purpose: `ratatoskr` and `sæhrimnir`
  both set it, and deleting it would not fail their builds - it would silently
  point their test traffic at the real Google Calendar API. Once both have
  migrated, delete `default_calendar_base` and have the constructors take
  `CALENDAR_API_BASE` directly. That removes the last `std::env::var` read in
  the workspace and the last place a bifrost crate names a downstream
  consumer.

## 4. Owner rulings

Product, API-shape and error-model decisions. None is a live defect on its
face.

### Error model and published surface

- **A proxy 407 maps to `AuthLost`, implying the wrong remedy.** JMAP
  classifies net's typed `TunnelError` (407 ->
  `Authentication(ReauthorizationRequired)` / `AuthLost`, proxy TLS ->
  `Transport(Tls)`, other refusals by status). The error model has no
  proxy-credential kind, so a 407 asks the consumer to reauthorize the mail
  account when the proxy's credentials are wrong; only support detail says so.
  Decide whether the taxonomy gains a proxy-credential kind or a different
  existing kind fits better.
- **`MutationConcurrency::StateBased` has no producer left.** Keep or remove
  is the owner's decision, to be made later. JMAP, CalDAV, CardDAV and Graph
  moved off it, so no provider advertises it. The `IdempotencyKey` doc in
  `crates/types/src/mutation.rs` still contrasts itself with `StateBased` as
  "optimistic concurrency on the wire".
- **Engine-internal requests on the published `ReopenRequest`.**
  `ScopeDeleted`, `EstablishIfAbsent` and `SchemaReset` are engine plumbing
  whose epochs and generations mean nothing to a consumer, on a `pub` enum
  re-exported from `lib.rs`. Moving them to a crate-private request enum
  removes published variants.
- **Caller-version residue.** Ruled (2026-10-21) and largely landed. Still
  open: JMAP PIM reads expose no caller type-state field, so a consumer has
  nothing to hand back, although JMAP capabilities already advertise
  `ExpectedVersion` on those doors (advertising an unusable guarantee is a
  product question); IMAP versioned flag writes are still submitted per
  target; other mutation families have no caller-version surface.
- **BulkDestroy PartialResponse derives DedupeByClientId** even though destroy
  has no client-id deduplication. This protects Graph/JMAP's usual ambiguous
  destroy answers from reassertion. An operation-aware derivation requires an
  error-model ruling.
- **A dedupe-bearing stream termination leaves same-round per-item
  SameRequest/AfterAuthRefresh blind retry candidates eligible.** Whether
  whole-submission dedupe should suppress these retries needs a ruling.

### Write outcomes and compensation

- **Acknowledged single-request creates without a usable identity can invite
  a duplicating manual retry:** IMAP draft_create without APPENDUID, Google
  People contact_create without resourceName, standalone Graph draft_create,
  Google and Graph filter_create, and Google calendar event_create. The
  committed-unidentified partial-commit shape can carry them without inventing
  uncertainty.
- **IMAP container operations return a refresh_folders failure after the
  write commits.** The partial-commit record makes this honest; whether to
  return success with a stale registry is a separate API decision.
- **Graph send_message leaves an orphan draft after a refused send;** each
  automatic retry can create another. Accepted Repeatable clutter, bounded by
  retry budgets. Orphan cleanup would be a new compensation; none exists in
  `pim/send.rs`.
- **Graph cancel_upload_session records a 404 or 410 as a Failed
  compensation,** though both mean the session is already gone. Google treats
  404, 410 and 499 on its cancel as Succeeded. Decide whether Graph should
  match.
- **Mutation outcomes carry no per-id detail.**
  `MutationCounters::pending_retry` is a single count that lumps unsent work,
  exhausted retries, read-back mismatches and inconclusive read-backs, with no
  object ids. A consumer that wants to replay or report precisely can only
  re-run the whole campaign.

### Engine behaviour

- **Push terminal event semantics.** A terminal push unit failure broadcasts
  account-scoped `SyncEvent::Terminated` while polling continues; its error
  carries the failing unit's attribution. Decide whether this needs a distinct
  structured notification; a folder-scoped termination could incorrectly imply
  that folder polling died. Matters if a consumer UI reads it as "account
  dead".
- **Push and lifecycle pause policy.** Lifecycle streams continue polling
  while the account boundary is paused; the multiplexer owns their pacing but
  does not consult the boundary. Push sessions gate only directive submission;
  registration maintenance may need to keep server watches alive, while
  Connection lanes retain live transports. Decide their pause semantics
  together.
- **Scope disable and push subscriptions.** DisableScope quarantines a cursor
  but does not narrow desired push subscriptions. An IMAP folder excluded after
  SELECT refusal remains in the subscription registry until consumer
  unsubscribe or account replacement. Decide whether engine scope quarantine
  should also change desired coverage.
- **Uncertain mutation items carrying Engine recovery are observed but do not
  forward the directive** (`classify_item_outcome` in sync engine bulk). The
  reassertion path protects these ids; forwarding is separate work.

### New capability

- **Graph contact moves.** `crates/graph/src/account/contacts.rs`. A folder
  change is refused as `Unsupported(ContactUpdate)` before I/O. Supporting it
  means folder-aware move routing (Graph lists `parentFolderId` as an update
  field).

## 5. Low priority

Narrow edge cases, audits with no known defect, and design work.

- **JMAP set omissions outside the calendar and contact doors.** Direct
  single-id readers of `SetResponse::updated` / `destroyed` other than those
  doors still map an omission through the generic translation (`IdNotFound`,
  terminal `MissingField`): the filter doors in `sync/filters.rs`, and in
  `sync/pim.rs` the email update, mailbox destroy and submission update reads.
  Not checked case by case; some sit inside multi-step operations that already
  carry their own partial-commit record.
- **Generation stamps on non-push recovery producers.** Poll tasks, the
  reconciler, lifecycle reader and deferred inventory still enqueue unstamped
  `ReopenRequest::Recovery`. A request queued before reattach can act on the
  replacement. Push session requests carry a generation checked under the
  reopen lock; extending that fence to other producers is separate.
- **Existing submission-item and stream directive sends await bounded
  `reopen_tx` while holding campaign activity and admission.** A handler
  waiting for activity drain could wait on the sender. Read-back forwarding
  releases both guards first; the existing paths need a lifetime review.
- **IMAP connection transition partial completion.** Inspect `complete_auth`
  and `unauthenticate` (including disposition after failed required
  CAPABILITY), and `run_starttls_upgrade` for earlier committed transitions
  followed by a later failure. Include later steps withdrawn while queued,
  dropped unclaimed by a dying driver, and already expired at admission.
- **IMAP bandwidth admission after claim.** `send_wire_command` waits for
  `BandwidthBudget` after driver claim but before the socket write. Caller
  expiry there conservatively reads `InFlight`. Withdrawal through that window
  needs retirement rules for bytes buffered by TLS even when the socket
  counter has not advanced.
- **Session cap does not span account incarnations.** A failed or cancelled
  factory open, or a closing incarnation during reopen, can leave a transport
  tail that the next incarnation's cap does not count.
- **IMAP push: same-session NOTIFY extension is uncovered until
  registration.** In the Reidle branch, a newly added folder is watched only
  after the re-issued NOTIFY SET. Changes before registration are not
  invalidated.
- **IMAP literals are not emitted incrementally.** FETCH body and binary
  literals are `Bytes` slices of the framed response, but a FETCH response is
  fully framed and parsed before delivery. Standalone parsers without an owned
  backing still copy, generic string fields still parse owned, and non-literal
  syntax and parser overhead are outside the literal ceiling. Incremental
  delivery needs a chunk-aware parser and dispatch contract: design work,
  suggested for the technical-specification track.
- **Account schema reset follow-ups.**
  - `BackfillCheckpointWriter::persist` bypasses completion refusal and every
    reset fence: the attached route accepts any consumer-supplied
    `BackfillCheckpoint`, completion partition included, with no publication;
    the direct route has no engine at all. The account reset excludes this
    consumer assertion path.
  - Ledger-only writer requests from pre-reset producers: `RecordBarrier` and
    `CrossWaivedBarriers` sent by a walk that predates a scope or account
    reset are accepted; a stale barrier fails closed (it blocks the
    replacement walk and asks an operator) and needs its own policy.
    `RecordDebt` acceptance is the safe direction and stays.
  - Schema-incompatibility evidence discarded before classification: a
    lifecycle `Created` restart deletes a preexisting row unread;
    `run_establish` types an unreadable row as `Engine(SchemaIncompatible)`
    without routing it to the account reset, and the per-scope retry loop
    treats it as retryable. Reachable for a preexisting row of a scope absent
    from discovery at attach that returns mid-session before any sibling door
    failure.
  - Per-scope fences do not refuse foreign publications: the account fence
    refuses ids minted by earlier attachments; per-scope resets still accept a
    prior attachment's change receipt through the fallback.
  - Duplicate SchemaIncompatible directives run duplicate account resets:
    every scope whose door fails sends its own directive, and each runs a full
    clear and re-establishment, for directives already queued by distinct live
    drives.
  - `ack_checkpoint` with no publication bypasses every fence: a change cursor
    or backfill page acknowledged with `publication: None` skips the claim
    lookup and every reset fence in `persist_ack_request`; after an account
    reset such a backfill page becomes a resume position past windows the
    replacement walk never re-mints. Nothing in the engine sends one. Two
    answers to rule on: name it as a consumer assertion path, or refuse a
    `None`-publication backfill acknowledgement (always, by the reasoning
    `completion_refusal` already records, or once an account fence is raised).
  - Conditional registration ignores `fenced_backfill`: `register_if_fence`
    compares `scope_fence`, which reads `fenced` (and `fenced_account`), not
    `fenced_backfill`, so a backfill discard revokes an in-flight backfill
    producer only at acknowledgement, not at registration. Probably harmless
    (the discard runs before the retry mints).
  - Backfill discard requests survive an account reset: detach drains
    `take_backfill_discards()` and sends `DiscardBackfillProgress` per scope; a
    discard recorded before an account reset deletes the post-reset walk's rows
    at detach. Safe direction (costs a re-walk).
  - Capture the lifecycle epoch where `Created` is raised: routing it as
    `EstablishIfAbsent { scope, epoch }` read at the multiplexer, instead of a
    synthesized `RestartScope` whose park reads the epoch at decline time,
    would let the epoch alone distinguish "deleted before the repair was
    raised" from "recreated". Changes the routing an existing multiplexer test
    pins.

## Coverage gaps from the October 2026 hunt

Not findings; areas the hunt did not reach in depth, for the next pass.

- Graph: public-folder EWS, autodiscover, cloud upload and search.
- Sync: `cursor/coverage.rs`, `cursor/ledger.rs`, `cursor/ledger_envelope.rs`,
  `backfill/*`, `engine/lane.rs` and `repair.rs`, the largest files.
