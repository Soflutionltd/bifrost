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

## A9 (directory search) follow-ups

- **a9-2 residual: the alias `$select` may cost directory reach.** The
  projection itself landed 2026-09-07. What it left open is a consent question
  nobody can settle hermetically: `otherMails` and `proxyAddresses` sit outside
  the property set `User.ReadBasic.All` grants. `businessPhones`, `companyName`,
  `jobTitle` and `department` are also outside that documented basic set,
  so the consent uncertainty is broader than the alias fields. A tenant holding
  only basic directory consent may now 403 the WHOLE `/users` query where it previously
  succeeded. It maps to `NoPermission` correctly, so nothing is misreported -
  but a tenant with working directory search can lose it, which is a reach
  regression rather than a graceful degradation. Confirming it needs a live
  ReadBasic-only tenant, because whether Graph actually refuses a `$select`
  overreach (as opposed to omitting the fields) is not something the hermetic
  suite can observe. If it does fire, the fix is a select-narrowing retry after
  the first 403 - narrow all properties outside the basic set and re-issue -
  NOT dropping them from the query outright, which would give every tenant the degraded
  projection to spare the minority.

## Open items folded in from the bug-hunt ledgers (2026-08-23)

Categories, as the ledgers used them: **C1** live defect, **C2** latent defect,
**C3** refactor opinion, **C4** product decision. **PUBLISHED SURFACE** means the
remedy removes, renames, or reshapes a published item - those are the repository
owner's call and must not be actioned without one, no matter how confident the
argument reads. Ledger findings were never verified against a running server;
confirm against the code before working any of them.

### Fenced for the repository owner (published surface)

- **google-B12. Retire `RATATOSKR_TEST_GCAL_ENDPOINT`.** [C3] Tail of google-B3,
  which landed 2026-08-23: `GoogleAccountFactory::with_calendar_api_base` now
  exists and takes precedence, the base is stored on `ClientInner`, the read
  happens once at construction instead of per request, and rate limits are
  registered from the configured bases rather than literal hostnames.

  The environment variable was KEPT on purpose. `ratatoskr` and `sæhrimnir` both
  set it, and deleting it would not fail their builds - it would silently stop
  redirecting and point their test traffic at the real Google Calendar API. Once
  both have migrated to `with_calendar_api_base`, delete `default_calendar_base`
  and have the constructors take `CALENDAR_API_BASE` directly. That removes the
  last `std::env::var` read in the workspace and the last place a bifrost crate
  names a downstream consumer. Coordinate with those two repos; there is nothing
  to do here until they are ready.

## Open items folded in from the third bug-hunt wave (2026-09-04)

The deferred tail of the 2026-09-04 hunt. The same category labels and the
PUBLISHED SURFACE fence apply.

- **google-C3. `events_in_range` combines `orderBy=startTime` with
  `showDeleted=true`.** [C2, needs a live probe] `crates/google/src/account/
  calendar.rs`. Google's docs bless `showDeleted=true` with
  `singleEvents=true`, and `orderBy=startTime` requires `singleEvents=true`,
  but there are long-standing reports of the live API answering 400 "The
  requested ordering is not available for the particular query" for some
  `showDeleted` + `orderBy` combinations, and cancelled instances have no
  `start` to order by (only `originalStartTime`, which the projection
  substitutes). The hermetic suite cannot catch a live refusal. One live
  probe settles it; if Google rejects the combination, every production
  range read fails, a top-severity defect hiding behind a green suite. The
  only ledger item of the wave left open as a possible defect.

## Later, from the October 2026 resolution waves

- **Graph contact moves.** `crates/graph/src/account/contacts.rs`. Graph
  contact updates never sent `address_book_id`, so a folder change was
  silently ignored; it is now refused as `Unsupported(ContactUpdate)` before
  I/O. Supporting it means folder-aware move routing (Graph lists
  `parentFolderId` as an update field). A new capability, not a fix.

## Notes

- The rules for working an item are at the top of this file. One more that
  earned its keep: an existing test you must modify is a red flag; justify
  it explicitly.
- Item keys: F-items came from the phase 5B/5C/5D error-model re-audits,
  N-items from the original post-phase-4 audit, B-items from the 2026-08
  bug-hunt ledgers, C-items from the 2026-09-04 hunt. All are tracked at
  the same level; none blocks ratatoskr.

## IMAP connection transition partial completion

- Inspect `complete_auth` and `unauthenticate` (including disposition after
  failed required CAPABILITY), and `run_starttls_upgrade` for earlier committed
  transitions followed by a later failure. Include later steps withdrawn while
  queued, dropped unclaimed by a dying driver, and already expired at admission.
  Write-side composite partial completion is covered by the shared
  partial-commit record (`reference/error-model.md`); these
  authentication/session-transition cases remain separate work.

## IMAP bandwidth admission after claim

- `send_wire_command` waits for `BandwidthBudget` after driver claim but before
  the socket write. Caller expiry there conservatively reads `InFlight`.
  Withdrawal through that window needs retirement rules for bytes buffered by
  TLS even when the socket counter has not advanced.

## IMAP abandoned checkout cascade

- `run_flag_mutation_groups` and `run_destroy_mutation_groups` continue submitting
  on the same checkout after an earlier command abandoned it. Later queued
  commands withdraw truthfully as `Unsent`, but each still waits a full timeout.
  Decide between failing fast on abandonment and obtaining another checkout.
  Under the physical session cap, obtaining another checkout must discard the
  abandoned one first: a nested checkout waits forever at pool_cap = 1 or
  when data capacity is exhausted. Its permit returns only at driver exit.

## IMAP unbounded LOGOUT outside pool drain

- The push cancellation path awaits unbounded LOGOUT. The pool drain
  provides an external timeout; until driver exit the push slot remains held.

## IMAP peer certificate lookup outside authentication deadline

- `resolve_scram_binding` awaits `peer_certificate_der` before SCRAM-PLUS.
  That non-wire request uses unbounded channel admission and reply waits, so
  it can wait behind a stalled head command beyond the authentication deadline.

## IMAP push: same-session NOTIFY extension is uncovered until registration

- In the Reidle branch, a newly added folder is watched only after the
  re-issued NOTIFY SET. Changes before registration are not invalidated.
  Pre-existing; the shared session budget does not widen this window.

## IMAP push: pin the re-IDLE and NOTIFY-collapse wiring through the pool connector seam

- The connector seam now supports hermetic worker transcripts. Pin both the
  same-session re-IDLE path and register_notify's result reaching
  signal_notify_coverage_loss on fresh and interrupted rounds.

## Session cap does not span account incarnations

- A failed or cancelled factory open, or a closing incarnation during reopen,
  can leave a transport tail that the next incarnation's cap does not count.

## IMAP push: false NOTIFY warning on non-NOTIFY accounts

- register_notify warns that other subscribed folders will not be pushed on
  every fresh session and Reidle round of a multi-folder non-NOTIFY account,
  although each other folder has its own worker. signal_notify_coverage_loss
  already treats false as no coverage gap in that topology.

## IMAP push: push_subscribe bumps the generation when it accepted nothing

- The unconditional bump wakes every worker without a scope-set change.
  Gate it on a non-empty accepted set.

## IMAP DELETE / RENAME while a parked member has the target selected

- deselect_target handles only its own checkout. Another parked member may
  remain SELECTed on the target while DELETE or RENAME goes out, contrary to
  RFC 2683 Section 2.2.2. Pre-existing.

## SMTP AsyncNetworkStream::poll_write slices with a stale retry length

- poll_write in crates/smtp/src/transport/smtp/client/async_net.rs keeps the
  offer length from an earlier Pending write and slices the next buffer with
  it. A caller re-polling with a shorter buffer would panic. Looks unreachable
  today: a dropped write leaves the connection Broken and the pool discards
  it.

## Account schema reset follow-ups

- "`BackfillCheckpointWriter::persist` bypasses completion refusal and every
  reset fence" - the attached route accepts any consumer-supplied
  `BackfillCheckpoint`, completion partition included, with no publication;
  the direct route has no engine at all. The account reset excludes this
  consumer assertion path.
- "Ledger-only writer requests from pre-reset producers" - `RecordBarrier`
  and `CrossWaivedBarriers` sent by a walk that predates a scope or account
  reset are accepted; a stale barrier fails closed (it blocks the
  replacement walk and asks an operator) and needs its own policy.
  `RecordDebt` acceptance is the safe direction and stays.
- "Schema-incompatibility evidence discarded before classification" - a
  lifecycle `Created` restart deletes a preexisting row unread;
  `run_establish` types an unreadable row as `Engine(SchemaIncompatible)`
  without routing it to the account reset, and the per-scope retry loop
  treats it as retryable. Reachable for a preexisting row of a scope absent
  from discovery at attach that returns mid-session before any sibling door
  failure.
- "Per-scope fences do not refuse foreign publications" - the account fence
  refuses ids minted by earlier attachments; per-scope resets still accept
  a prior attachment's change receipt through the fallback.
- "Duplicate SchemaIncompatible directives run duplicate account resets" -
  every scope whose door fails sends its own directive, and each runs a full
  clear and re-establishment. (The parked-reset double and the queued
  deferred `Resume` items are closed by the account reset; the remaining cost is
  directives already queued by distinct live drives.)
- "`ack_checkpoint` with no publication bypasses every fence" - a change
  cursor or backfill page acknowledged with `publication: None` skips the
  claim lookup and every reset fence in `persist_ack_request`; after an
  account reset such a backfill page becomes a resume position past windows
  the replacement walk never re-mints. Nothing in the engine sends one. Two
  answers to rule on: name it as a consumer assertion path (as the account
  reset does), or refuse a `None`-publication backfill acknowledgement (always, by
  the reasoning `completion_refusal` already records, or once an account
  fence is raised).
- "Conditional registration ignores `fenced_backfill`" - `register_if_fence`
  compares `scope_fence`, which reads `fenced` (and `fenced_account`), not
  `fenced_backfill`, so a backfill discard still revokes an in-flight
  backfill producer only at acknowledgement, not at registration.
  Pre-existing and probably harmless (the discard runs before the retry
  mints), but the same point-in-time question the walk and establishment fences close for `fenced`.
- "Backfill discard requests survive an account reset" - detach drains
  `take_backfill_discards()` and sends `DiscardBackfillProgress` per scope; a
  discard recorded before an account reset deletes the post-reset walk's rows
  at detach. Safe direction (costs a re-walk) and pre-existing for per-scope
  resets.
- "Capture the lifecycle epoch where `Created` is raised" - routing the
  lifecycle `Created` as `EstablishIfAbsent { scope, epoch }` read at the
  multiplexer, instead of a synthesized `RestartScope` whose park reads the
  epoch at decline time, would let the epoch alone distinguish "deleted
  before the repair was raised" from "recreated". Changes the routing an
  existing multiplexer test pins.
- "Engine-internal requests on the published `ReopenRequest`" -
  `ScopeDeleted`, and now `EstablishIfAbsent` and `SchemaReset`, are
  engine plumbing whose epochs and generations mean nothing to a consumer,
  on a `pub` enum re-exported from `lib.rs`. Moving them to a crate-private
  request enum removes published variants, so it needs the owner's ruling.

## Mutation outcomes carry no per-id detail

- `MutationCounters::pending_retry` is a single count that lumps unsent work,
  exhausted retries, read-back mismatches and inconclusive read-backs, with no
  object ids. A consumer that wants to replay or report precisely can only
  re-run the whole campaign.

## Read-back reassertion laterals

- Uncertain mutation items carrying Engine recovery are observed but do not
  forward the directive (`classify_item_outcome` in sync engine bulk). The
  reassertion path protects these ids; forwarding remains separate work.
- Graph mixed flag operations silently discard unmodeled tokens while
  reporting Applied (`patch_for_flags` and `flag_op_is_unexpressible`). The
  new flag judge refuses to decide these operations; the write outcome still
  needs its own ruling.
- BulkDestroy PartialResponse derives DedupeByClientId even though destroy
  has no client-id deduplication. This protects Graph/JMAP's usual ambiguous
  destroy answers from reassertion. An operation-aware derivation requires
  an error-model ruling.
- Existing submission-item and stream directive sends await bounded reopen_tx
  while holding campaign activity and admission. A handler waiting for
  activity drain could wait on the sender. New read-back forwarding releases
  both guards first; the existing paths need a separate lifetime review.
- Legacy imap1 object ids decode but outcomes re-encode imap2, so legacy ids
  fail correlation in mutation and hydration. The new hydrated-id check does
  not introduce this mismatch.
- A dedupe-bearing stream termination leaves same-round per-item
  SameRequest/AfterAuthRefresh blind retry candidates eligible, as before.
  Whether whole-submission dedupe should suppress these retries needs a ruling.

## JMAP direct PIM set omissions need partial-response classification

Direct calendar/contact update and RSVP paths read `SetResponse::updated`.
An omitted id becomes `Error::IdNotFound` and then Protocol(MissingField), a
terminal result without read-back. The bulk path's `set_id_unanswered` is the
precedent: provider omission does not prove the write failed. This concerns
first-response omission classification, independently of wire replay policy.

## Partial-commit adoption laterals

- IMAP send_message, send_raw_message and draft_send log a failed Sent APPEND
  and return success; draft_send likewise logs a post-send discard failure.
  The caller cannot learn that the Sent copy is missing or the draft lingers.
  These paths return Ok after SMTP commits, so they are outside the adopted
  failure-path contract in account/pim.rs.
- IMAP container operations return a refresh_folders failure after the write
  commits. The partial-commit record makes this honest; whether to return
  success with a stale registry is a separate API decision.
- Acknowledged single-request creates without a usable identity can invite a
  duplicating manual retry: IMAP draft_create without APPENDUID, Google People
  contact_create without resourceName, standalone Graph draft_create, Google
  and Graph filter_create, and Google calendar event_create. These need their
  own ruling; they are outside multi-step adoption. The committed-unidentified
  partial-commit shape can carry them without inventing uncertainty.
- Upload session URLs can reach support exports through transport messages.
  bifrost-net's `send_error_to_error` builds `Error::Network { message }` from
  reqwest's Display, which names the request URL, and `support_cause_from_source`
  keeps that message as support-only text. For the Graph and Google chunk PUTs
  the URL is the pre-authenticated session credential, and the chunk-PUT error
  is the failure leaf. This predates partial commitment. The session cancel
  DELETE, whose error is kept as a compensation, already passes through
  `bifrost_net::Error::redact_request_text` at both cancel sites; the chunk
  PUT still needs a URL-scrubbing decision at the net boundary or the upload
  call sites.
- Graph send_message leaves an orphan draft after a refused send; each automatic
  retry can create another. This is accepted Repeatable clutter for the send
  operation, bounded by retry budgets. Adding orphan cleanup is a separate
  compensation decision; none was added in pim/send.rs.
- Graph cancel_upload_session records a 404 or 410 as a Failed compensation,
  though both mean the upload session is already gone. Google treats 404, 410
  and 499 on its cancel as Succeeded. Decide whether Graph should match.
