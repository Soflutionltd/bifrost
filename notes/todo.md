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
  confirmed; breaking, renaming or moving published API is fine, but a
  change that removes a capability entirely stops and asks the owner,
  whatever the argument. A shared-crate change (`types`,
  `net`, `sasl`, `sync`) is verified with the full-workspace `brokkr
  check`, never `-p`.
- An existing test you must modify is a red flag; justify it explicitly.

The order below is a priority triage of the findings taken at face value,
not verified against the code. Verification happens when an item is worked.

## 1. Priority

Items that leak something or report success for something that did not
happen, plus one surface gap that matters if the consumer relies on it.

- **`SyncEngine` exposes almost no calendar primitives.** It forwards contact
  primitives, but of the calendar `Account` methods only `event_delete` and
  `event_delete_if_version` (`crates/sync/src/engine/passthrough.rs`). List,
  range, get, create, update, RSVP and search have no passthrough, and
  `event_rsvp_if_version` needs one too:
  `pub async fn event_rsvp_if_version(&self, account_id: &AccountId, event: EventId, status: RsvpStatus, expected_version: Option<ServerVersion>) -> Result<(), Error>`.
  An omission only if the engine is meant to be the consumer's single door
  for calendar, as it is for contacts.
- **An expired CalDAV sync token fails every poll.** `cursor_invalid_error` in
  `crates/caldav/src/client.rs` scopes the error to `CursorScope::Folder(url)`,
  but live CalDAV cursors are `CursorScope::FolderType`, so the engine's
  exact-match `RestartScope` misses the cursor and the stale token is replayed
  on every poll. `reference/caldav.md` documents the wrong scope too. Scope it
  to the live cursor's shape and pin it with a restart test.

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

- **An IMAP send with no acknowledged recipient but an uncertain one is `Err`.**
  `send_rfc5322` (`crates/imap/src/account/submission.rs`) returns the uncertain
  lane's error (InFlight evidence, `Application::Uncertain`) when nothing landed
  in the succeeded lane, and the call skips the Sent APPEND and, for
  `draft_send`, the draft discard, although the message may have been accepted.
  A consumer dispatching on `error.recovery()` alone can also get terminal
  advice: a malformed final reply in that lane is `ProviderContractViolation`.
  The other recipients' failures are dropped from that `Err`. Decide whether
  "acceptance uncertain, none acknowledged" becomes a receipt state, and if so
  what Sent filing and draft cleanup do for a maybe-sent message.

- **Gmail and Graph ignore `save_to_sent = false`.** Both file the Sent copy
  server-side unconditionally (`_save_to_sent` is unused in
  `crates/google/src/account/pim.rs` and `crates/graph/src/account/pim/send.rs`),
  and their `SendReceipt` says `ProviderManaged`. Decide whether a declined Sent
  copy should be refused as `Unsupported`, honoured by a post-send delete, or
  documented as a provider limit on `SendRequest::save_to_sent`.
- **`SendReceipt::id` multiplexes three identities.** It is a message id, a
  scheduled-send handle (JMAP returns the EmailSubmission id when scheduled), or
  an IMAP `imapmsgid1:` correlation token that names no addressable object.
  Separate fields (message id, correlation id, scheduled handle) would let
  `cancel_scheduled_send` / `reschedule_send` take the handle by type. Also
  `draft_send` has no per-call `save_to_sent`; IMAP uses the account default.
  The November 2026 design hunt (types scope) proposed the same split as
  `SendReceipt::id: SubmissionRef` (`Message(ObjectId)`,
  `Scheduled(ScheduledSendHandle)`, `Correlation(String)`), with the scheduled
  primitives taking `ScheduledSendHandle`, and noted that `DraftHandle` exists
  "so the type system catches mix-ups" while `impl From<DraftHandle> for
  ObjectId` undoes it. The imap-account scope of the same hunt reached the same
  shape from `append_to_sent_or_fallback`, which returns either an
  APPENDUID-derived id or `ObjectId("imapmsgid1:<Message-ID>")` that
  `decode_object_id` rejects, and noted `SentCopy::Filed { id: Option<..> }`
  already carries the addressable case separately.
- **A JMAP send's leftover staging Email has no routable handle on the
  receipt.** When the implicit `Email/set` refuses or does not confirm the
  Sent move, the created/imported Email (or, for `draft_send`, the submitted
  draft, which now looks sendable) may remain in Drafts with `$draft`; when it
  refuses or does not confirm the `save_to_sent = false` destroy, the staging
  Email may survive. The receipt says `NotFiled` / `Uncertain` / `NotRequested`
  and the destroy case is only logged (`implicit_sent_copy` in
  `crates/jmap/src/sync/pim.rs`). Reusing `DraftDiscardFailure` was rejected:
  no discard was attempted on the move path, and `draft_discard` routes only
  through the primary draft account, so a foreign send-as staging Email cannot
  be addressed by a `DraftHandle`. Decide whether `SendReceipt` grows a field
  naming the retained or possibly retained Email with an owner-preserving
  handle, distinguishing refused, uncertain and failed-destroy.
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
  "optimistic concurrency on the wire". The November 2026 design hunt (types
  scope) confirmed it is constructed nowhere and folded the question into a
  proposal to replace `ExpectedVersion`'s eight bools plus `None` with one
  doors bitset (filed in the hunt's `structural.md`).
- **Engine-internal requests on the published `ReopenRequest`.**
  `ScopeDeleted`, `ScopeCreated`, `EstablishIfAbsent` and `SchemaReset` are
  engine plumbing whose epochs and generations mean nothing to a consumer, on
  a `pub` enum re-exported from `lib.rs`. Moving them to a crate-private
  request enum removes published variants. Same ruling: `PushRecovery` is now
  identical in meaning to `Recovery { generation: Some(_) }` (both are
  stamped with the producing incarnation's generation and declined the same
  way), so merging it into `Recovery` also removes a published variant.
  The November 2026 design hunt (sync-engine scope) proposed one variant
  carrying a stamp type, `Stamp::{Incarnation(AccountGeneration), Injected}`,
  so the type says only the sink is unstamped, and argued `ReopenRequest`
  belongs to the per-account supervisor rather than to `multiplexer`.
- **Caller-version residue.** Ruled (2026-10-21) and largely landed. Still
  open: JMAP PIM reads expose no caller type-state field, so a consumer has
  nothing to hand back, although JMAP capabilities already advertise
  `ExpectedVersion` on those doors (advertising an unusable guarantee is a
  product question); IMAP versioned flag writes are still submitted per
  target; other mutation families have no caller-version surface.
- **BulkDestroy PartialResponse derives DedupeByClientId** even though destroy
  has no client-id deduplication. This protects Graph/JMAP's usual ambiguous
  destroy answers from reassertion. An operation-aware derivation requires an
  error-model ruling. The same derivation now reaches the JMAP single-id
  doors that read an unanswered set id as `PartialResponse`: an unanswered
  `CancelScheduledSend`, the `RescheduleSend` cancel leg (no replacement
  create was attempted), `ContainerDelete`, `FilterDelete` and `DraftDiscard`
  all carry `DedupeByClientId`. A reschedule-only partial-commit record with a
  `Cancelled` Uncertain primary would drop it there, but was not built ahead
  of the general ruling.
- **A dedupe-bearing stream termination leaves same-round per-item
  SameRequest/AfterAuthRefresh blind retry candidates eligible.** Whether
  whole-submission dedupe should suppress these retries needs a ruling.

### Write outcomes and compensation

- **Acknowledged single-request creates without a usable identity can invite
  a duplicating manual retry:** IMAP draft_create without APPENDUID, Google
  People contact_create without resourceName, standalone Graph draft_create,
  Google and Graph filter_create, Google calendar event_create, and JMAP's
  filter, mailbox, event and contact creates (`error::require_created_id`
  returns a bare terminal `Protocol(MissingField)` with acknowledged evidence
  and no record; `Application::from_leaf` would read that leaf NotApplied, but
  nothing calls it there). The committed-unidentified partial-commit shape can
  carry them without inventing uncertainty. The November 2026 design hunt
  (imap-account scope) reported the IMAP case independently: with no APPENDUID
  (RFC 4315 forbids it for a `UIDNOTSTICKY` mailbox, and servers may omit it)
  the draft exists and the caller gets `Err(Unsupported(DraftCreate))`, which
  reads as "nothing happened", so a retry duplicates the draft; the UIDPLUS
  capability gate does not cover it.
- **Graph and JMAP judge the same identity-less draft create differently.**
  Graph `send_created_draft` (crates/graph/src/account/pim/send.rs) records
  the committed unidentified `Created` as `Repeatable`; JMAP `draft_create`
  records it `Blocking`. Unreviewed whether the difference is intended.
- **IMAP container operations return a refresh_folders failure after the
  write commits.** The partial-commit record makes this honest; whether to
  return success with a stale registry is a separate API decision.
- **Graph send_message leaves an orphan draft after a refused send;** each
  automatic retry can create another. Accepted Repeatable clutter, bounded by
  retry budgets. Orphan cleanup would be a new compensation; none exists in
  `pim/send.rs`.
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

- **Caller-supplied composition `Date` on `SendRequest`.** The shared
  renderer stamps `Date:` with the submission instant. An offline outbox that
  submits later would want the moment the user pressed Send instead; a
  `SendRequest` field (defaulting to now) would carry it. Surfaced by the
  codex spar on bz#1; not built because it is a new consumer-facing knob.
- **Gmail `draft_send` and `Date`.** Google drafts are rendered without a
  `Date` and handed to drafts.send as stored. If a live probe shows Gmail
  keeps a missing `Date` missing in the sent copy, `draft_send` could fetch
  the draft and pass replacement `message.raw` stamped with the send time
  in the same drafts.send call.

- **Graph contact moves.** `crates/graph/src/account/contacts.rs`. A folder
  change is refused as `Unsupported(ContactUpdate)` before I/O. Supporting it
  means folder-aware move routing (Graph lists `parentFolderId` as an update
  field).

- **Graph public-folder mail writes over EWS.** `crates/graph/src/account/mutate.rs`,
  `pim/messages.rs`. Flag writes, moves, destroys and the typed writers refuse
  public-folder items as `Unsupported` before I/O, because Graph REST cannot
  address them and the EWS client has no write operations. Building it means an
  EWS arm (`UpdateItem` for `IsRead` / `Flag` / `Categories`, `MoveItem`,
  `DeleteItem`) routed by the folder's `routing_map` headers, with change-key
  preconditions, replacement ids after a move, etag-cache and ambiguous-
  completion handling that meet the mutation contracts, and a capability
  story for a namespace-specific write gap. Not built: nobody has ruled
  public folders writable.
- **Undiscovered public-folder move destinations.** A public folder id is a
  bare EWS `FolderId`; only discovery (`is_known_public_folder`) tells it
  apart from a primary folder id, so a move into an undiscovered public
  folder still goes to Graph REST and fails there.

## 5. Low priority

Narrow edge cases, audits with no known defect, and design work.

- **Lateral findings from the review of the design-hunt batch, unverified.**
  Each wants its own check and, where marked, a ruling.
  - DAV: `reference/caldav.md` and `reference/carddav.md` call a cursor scope
    mismatch schema-incompatible; the code treats it as `CursorInvalid`.
    `dav-core` `http_error_policy_for_protocol` has an `unreachable!` panic
    path. The pre-dispatch raw-URL trust check has no test.
  - Google: the 404 `Message` fallback now also labels a scopeless 403 as
    `NoPermission` on `Message` for filter and directory calls.
  - net: `server_cause_from_status` returns a `Server` cause for unlisted 4xx
    statuses.
  - IMAP: `ImapMessageRef` equality depends on the container spelling,
    including INBOX case, contradicting its doc. The stricter OAuth and
    CRAM-MD5 handling and ManageSieve's OAUTHBEARER preference are not in
    `reference/imap.md`.
  - SASL: `AGENTS.md` still calls `bifrost-sasl` private although it is
    published. `strip_frame_delim` in `crates/sasl/src/oauth.rs` still
    rewrites for callers that bypass `SaslClient`.
  - JMAP: a capability dropped mid-session yields terminal `Unsupported` where
    `CapabilityChanged` may fit (ruling). The foreign-discovery paragraph of
    `reference/jmap.md` names a constructor that is now test-only. An all-day
    event with no start loses its all-day flag.
  - SMTP: a rejected batch DATA reports per-recipient RCPT rejections. Batch
    sends have no BDAT entry point (ruling). An LMTP final 421 marks the
    remaining recipients uncertain without reading their finals. `Session` and
    `Transaction` use `unreachable!` on impossible outcomes.
  - sync: the operator-override warning moved from `user_safe` to
    `support_only` (ruling). `plan_recovery` lost its unhandled-classification
    detail. `ThrottleScope::Mailbox` no longer names what it means. Several
    engine sites still hold raw `WriterRequest` senders instead of
    `WriterHandle`. `publish_warning` stamps `Account` scope on unscoped
    warnings. `AckOutcome` is not `#[must_use]`: adding it is clean in
    production but about fifty test call sites discard the value.
  - Graph: `public_folder` change events carry `Owner` memberships that only
    `Account` cursors cover.
  - Calendars: CalDAV and JMAP normalize floating times to UTC.

- **The backfill bound docs say an acknowledgement frees capacity; the code
  frees it on receipt.** `engine/backfill_bound.rs` documents the permit as the
  consumer's receipt of the page (`ChangesReceiver::note_receipt`), but the
  `BackfillConfig::lane_capacity` doc in `crates/sync/src/types.rs`, the
  `readers` field doc in `crates/sync/src/cursor/coverage.rs` and
  `reference/sync.md` still say "an acknowledgement frees a page for every
  receiver". Reconcile the prose with the receipt bound (doc-only).
- **`reference/jmap.md` misstates the `CallNotFound` classification.** It lists
  `CallNotFound -> Protocol(MissingField)`; `crates/jmap/src/sync/error.rs`
  maps it to `Protocol(PartialResponse)` with `Acknowledged` transmission
  evidence (an omitted call is unanswered). Fix the reference.
- **JMAP raw-message reads collapse two failures into `NotFound(Message)`.**
  `open_raw_rfc822` in `crates/jmap/src/sync/blob.rs` takes the same
  `raw_message_not_found` lane when the correlated Email has no `blobId` and
  when the `Email/get` list does not contain the requested id at all. The
  second is a server answering a different object than asked, which reads as a
  contract violation rather than a vanished message. Needs a call on whether to
  split them.
- **Cursor envelope decoding accepts trailing bytes.** `parse_envelope` in
  `crates/types/src/wire/cursor.rs` checks only `bytes.len() < payload_end`,
  so bytes after the payload are silently ignored rather than refused as
  `Malformed`. Check the ledger codec for the same shape when fixing.
- **Reattach and first subscribe disagree on accounts with no push lanes.**
  Reattach now carries a desired subscription only when
  `effective_push_lanes` of the replacement is non-empty
  (`crates/sync/src/engine/reattach.rs`), dropping it otherwise; verify the
  initial `push_subscribe` path applies the same decision, or a consumer can
  hold a subscription on one incarnation that the next silently drops.

- **IMAP `draft_send` and `draft_discard` do not check that the handle is a
  draft.** Both decode any `imap2` object id (`crates/imap/src/account/pim.rs`)
  and neither verifies Drafts-folder membership or the `\Draft` flag, so a
  handle naming an ordinary message submits it and then marks it `\Deleted`
  and expunges it. Only that one UID can be removed (the expunge fallback's
  allowed set is the singleton), so this is an input-contract question, not
  collateral expunging. Needs a ruling on whether a non-draft handle is
  `Request(Malformed)`.

- **IMAP search: one failing folder fails the whole page.** `search` and
  `search_messages` (`crates/imap/src/account/pim.rs`) propagate any
  per-folder SELECT or SEARCH error with `?` across every selectable folder,
  shared ones included, and never use `Page::skipped_scopes`; Graph
  quarantines a revoked mailbox instead. Needs a ruling on which per-folder
  failures quarantine rather than fail. The November 2026 design hunt
  (imap-account scope) reported the same and filed IMAP's wider skip-lane gap
  (shared-namespace LIST / NAMESPACE failures dropped at open) in the hunt's
  `bugs.md`.
- **IMAP search: the ESEARCH expansion cap is reported as `Request(Malformed)`.**
  `SearchResultTruncated` (`crates/imap/src/account/error.rs`) maps to a
  `ClientBug` class although the request was well formed and the server
  answered correctly; a caller cannot tell "criteria malformed" from "result
  too large" except by support-only text. Position paging does not help, since
  every page call still expands the whole folder result. The November 2026
  design hunt (imap-account scope) traced this to a gap in the shared
  vocabulary - `RequestErrorKind` has only `Malformed` and `BatchInputInvalid` -
  and filed the wider set of collapsed failures in the hunt's `structural.md`.
- **IMAP search cursor is not bound to its query.** The `imapsearch1:` cursor
  carries only a position, so it is accepted for a different `SearchRequest`.
  A position is meaningful under any query, so this resumes rather than
  corrupts, but JMAP and Graph bind their search cursors to owner and state.
- **IMAP `provider_query` is documented as ignored but is sent as raw SEARCH
  criteria.** The module doc of `crates/types/src/search.rs` says IMAP ignores
  `provider_query` unless an adapter interprets it; `search_plan`
  (`crates/imap/src/account/pim.rs`) validates it structurally and substitutes
  or appends it to the UID SEARCH criteria, and reference/imap.md has no
  paragraph on it. One of the two is wrong; the doc is the likelier one.
- **reference/imap.md still says search keeps "offset pagination stable"** in
  the object-id section, while search pages by position since the bifrost#2
  fix; the test `search_folder_offsets_survive_registry_rebuild_with_display_ties`
  keeps the old word too.
- **UIDVALIDITY change has four classifications across the IMAP account.**
  Changes stream `SyncState(CursorInvalid)`, search cursor
  `ConcurrencyConflict`, hydration, mutation and `open_raw_rfc822`
  `Request(Malformed)` (a `ClientBug` class for a server-side generation
  change); reference/error-model.md never names UIDVALIDITY. The poller
  (`crates/imap/src/account/poll.rs`) answers a typed `GenerationChanged`
  instead. Needs a ruling on the shared class. The November 2026 design hunt
  (imap-account scope) counted seven sites: `get.rs` and `mutate.rs`
  `uidvalidity_changed_error`, `blob.rs::run_raw`, `pim.rs::valid_uids` (no
  scope), `parts.rs::DecodedPartHandle::verify_uidvalidity` (all
  `Request(Malformed)`), `pim.rs::SearchPager::check_epoch`
  (`ConcurrencyConflict`), `changes.rs::validate_uidvalidity`
  (`SyncState(CursorInvalid)`). It proposed the epoch check live on one IMAP
  identity type (`ImapRef::check_epoch(live) -> Result<(), EpochVoided>`) with
  one translation of `EpochVoided` in `error.rs`, most likely
  `NotFound(Message)` with a folder-scoped cause so the engine also learns the
  scope needs re-inventory.
- **No persistence contract for a `ChangeCursor` held outside the engine.**
  bifrost#3 offered it as the alternative to the UID polling surface; the
  surface landed instead. The cursor codec lives in bifrost-sync and nothing
  documents how a direct `Account` consumer persists and migrates a cursor.
  Design work, no consumer asking today.

- **IMAP PIM partial-commit scopes name the canonical id, not the submitted
  one.** `folder_effect_targets` (`crates/imap/src/account/pim.rs`) rebuilds
  `ErrorScope::Message` from a `FolderEffect`'s folder, UIDVALIDITY and UID,
  and `copy_messages`, `delete_messages` and `set_flag` reduce their decoded
  targets to numeric UIDs via `valid_uids`, so a legacy `imap1` or
  non-canonical source id is reported in its `imap2` spelling. Batch lanes and
  hydration already echo the submitted id; no in-workspace consumer matches
  these diagnostic scopes against submitted ids (sync attributes partial
  commits by the enclosing `BatchItemId`), so this is attribution polish.
  Fixing it means carrying `DecodedObjectId::id` through `FolderEffect`; new
  destination ids stay canonical.

- **A refused JMAP send drops an unanswered Email create or import.** In
  `send_message` / `send_raw_message` the failure branch reads the Email
  result with `.ok()`, so an Email create answered in neither map, beside a
  proven submission refusal, becomes `email: None` and `failed_submission`
  returns the bare refusal: nothing records that a staging Email may exist.
  The same `.ok()` swallows an Email created with no usable id (the
  `require_created_email_id` MissingField leaf), where the Email certainly
  exists.
  Recording it needs an uncertain, unidentified staging-Email step, which sits
  next to the owner-ruling entry on leftover staging Emails having no
  routable handle.
- **Consumer-injected terminations cannot be attributed to an incarnation.**
  `InvalidationSink::push` (`route_watch_event` in
  `crates/sync/src/push/mod.rs`) sends `ReopenRequest::Recovery` with
  `generation: None` by policy, so a delayed termination from an obsolete
  external watch (a webhook or Pub/Sub subscription the consumer runs) still
  restarts, resets or disables the replacement after a reattach. Closing it
  needs optional incarnation attribution on the consumer-facing push API
  (`WatchEvent` or `SinkPush` side) while keeping the unattributed door;
  sampling the current generation at push time was rejected, since it labels
  an old event with the new identity.
- **IMAP connect-time STARTTLS expiry carries no evidence.** `setup_timeout`
  in `crates/imap/src/connection/lifecycle.rs` maps every connect-time expiry
  to `Error::Timeout { attempt: None }`, including one after the STARTTLS
  tagged OK or the stream swap. Harmless today (the half-built session is
  dropped before a driver exists and the only recovery is a fresh dial), but
  it is the one transition path whose error states no phase. The November 2026
  design hunt (imap-connection scope) found the same for the CAPABILITY line,
  and traced it to setup running outside the driver as a second execution
  context with its own evidence model; its proposal to move setup into the
  driver is filed in the hunt's `structural.md`.
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

## Coverage gaps from the October 2026 hunt

Not findings; areas the hunt did not reach in depth, for the next pass.

- Graph: public-folder EWS, autodiscover, cloud upload and search.
- Sync: `cursor/coverage.rs`, `cursor/ledger.rs`, `cursor/ledger_envelope.rs`,
  `backfill/*`, `engine/lane.rs` and `repair.rs`, the largest files.
