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

## IMAP composite partial completion

- Multi-command operations still report the later leaf's failure rather than
  aggregate progress. In `uid_move_messages` after an acknowledged COPY, STORE
  `NO [UNAVAILABLE]` is acknowledged but retryable; a COPY response dropping
  UIDPLUS makes UID EXPUNGE refuse locally; an interleaved LOGOUT can cause
  `StateChangedBeforeSend`. These paths can authorize replaying the COPY.
  The current MOVE evidence floor covers evidence-bearing `Unsent` errors,
  including first-write failures and `Closed`, but cannot represent all partial
  outcomes. An aggregate-progress design must absorb or replace that floor.
  Include later steps withdrawn while queued, dropped unclaimed by a dying
  driver, and already expired at admission. Inspect `complete_auth` and
  `unauthenticate` (including disposition after failed required CAPABILITY),
  `run_starttls_upgrade`, `run_patch_mutation_group`, and
  `run_destroy_mutation_groups` as members of the same class.

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
