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

## Throttle budgets: follow-ups

Filed 2026-10-19 while landing declared throttle keys and stamped budgets
(the sync-F6 close-out). Each was outside that change's scope.

- **Unproduced published variants, owner's call.** `ThrottleScope::Tenant`,
  `ThrottleScope::Provider` and `ThrottleKey::Tenant` now have no in-tree
  producer: Graph, the only `Tenant` producer, meters per mailbox and stamps
  budgets instead, and nothing ever emitted `Provider`. They stay published
  for out-of-tree implementations; whether to retire them is a removal, so it
  waits for the owner.
- **Gmail per-project quota is a cross-account budget nobody declares.**
  `crates/google/src/account/error.rs` maps `rateLimitExceeded`,
  `userRateLimitExceeded` and `quotaExceeded` all to `ThrottleScope::Account`.
  If Google's project-level reasons mean the quota every account on one OAuth
  client shares, they are a `ThrottleBudget` every Google account should
  declare. Needs Google's reason semantics confirmed before it is a defect.
- **A mutation campaign is one throttle unit.** A campaign spanning a held
  mailbox and a free one waits for the held one (`reference/sync.md`).
  Splitting a campaign into per-budget cohorts with per-cohort retry
  accounting is a feature, not a fix.
- **A hold landing mid-way through a backfill walk is slept out inline**, so
  later scopes' cold start waits behind it. Abandoning the walk would cost a
  re-walk; a resumable partition-level deferral would avoid both.
- **Recovery-driven establishment does not honour throttle holds.**
  `run_establish` in `crates/sync/src/engine/reattach.rs` (scope recovery and
  replacement-account establishment) drives `InventoryFusion` without
  consulting the bucket, and its retries use their own exponential delay. It
  never waited on the account-wide hold either, so this predates declared
  keys; it is now the one engine lane outside the wait-site set in
  `reference/sync.md`. Found by the close-out review.
## A9 (directory search) follow-ups

- **a9-1 (carddav)** RFC 6352 directory-gateway leg. `directory_search`
  is `Unsupported(DirectorySearch)` on CardDAV; a server may advertise an
  optional read-only directory-gateway address book, but gateway discovery
  is a substantial provider-specific unknown with no ratatoskr precedent.
  Scoped out of A9 to keep the blast radius bounded.
- **a9-2 residual: the alias `$select` may cost directory reach.** The
  projection itself landed 2026-09-07. What it left open is a consent question
  nobody can settle hermetically: `otherMails` and `proxyAddresses` sit outside
  the property set `User.ReadBasic.All` grants, so a tenant holding only basic
  directory consent may now 403 the WHOLE `/users` query where it previously
  succeeded. It maps to `NoPermission` correctly, so nothing is misreported -
  but a tenant with working directory search can lose it, which is a reach
  regression rather than a graceful degradation. Confirming it needs a live
  ReadBasic-only tenant, because whether Graph actually refuses a `$select`
  overreach (as opposed to omitting the fields) is not something the hermetic
  suite can observe. If it does fire, the fix is a select-narrowing retry after
  the first 403 - drop the two alias fields and re-issue - NOT dropping them
  from the query outright, which would give every tenant the degraded
  projection to spare the minority.
- **a9-3 (graph)** `$search` (with `ConsistencyLevel: eventual`) as a
  richer substring directory match than the current `startswith` prefix
  `$filter`, if needed.

## Namespaced-container follow-ups

Surfaced while landing the namespaced-container surface (shared-mailbox and
public-folder containers, EWS public-folder hydration, allowlisted public-folder
scopes). Each was deliberately out of that brick's scope; none blocks the
container projection itself.

- **nc-2a (graph, filed while landing nc-2, not fixed)** A non-mail
  public-folder item now HYDRATES (the `GetItem` request is class-safe), but
  both projections are still mail-shaped: `hydrated_from_ews_item` reuses
  `item_to_inventory_entry` and `message_from_ews_item` mints a `Message`. A
  contact's `<t:DisplayName>` is not a subject and does not land anywhere, so
  a hydrated `IPM.Contact` is a near-empty `Message` carrying only its id,
  change key and memberships. Evidence: `ews::parse` fills `EwsItem` from a
  `<t:Contact>` with `item_id` / `change_key` / `item_class` and nothing else
  (`parse_get_item_response_contact`). Whether non-mail public-folder items
  should project onto a contact/event type at all is a product decision, not
  a defect - hence filed rather than built.

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

- **jmap-C4. `filters_list` blob traffic is invisible to any accounting
  surface.** Filed while closing jmap-C1 (2026-09-06). `sync::filters::list`
  downloads one sieve script body per filter (fanned out at
  `api_request_concurrency`), and those bodies can be large, but the door
  returns `Vec<ServerFilter>` rather than a `Batch`, so there is no
  `bytes_in` field to report into - the metering contract has nothing to
  violate here, and the traffic simply never appears. `Client::download`
  now records into the tally, so a metered handle would count it; what is
  missing is a place to put the number. This is a shape question about the
  `filters_list` return type, i.e. a published-surface decision, so it is
  filed rather than fixed. Same shape applies to `pim`'s upload paths, but
  those are outbound and the tally is inbound-only.

## Bandwidth cap: what it means across the workspace

Surfaced 2026-10-19 by a survey made while redesigning SMTP outbound
admission. Not verified line by line; confirm against the code before
working any of them.

- **The `Account::set_bandwidth_cap` contract is undefined.** The doc in
  `crates/types/src/account.rs` says only "apply the engine's current
  bandwidth cap"; it does not say per direction, per connection, or
  combined across protocols. Every implementation happens to keep separate
  in and out budgets. This is a ruling for the owner, and every item below
  depends on it.
- **Three separate token-bucket copies.** `bifrost-net` (`net.rs`,
  sleeps itself), `bifrost-imap` (`connection/wire.rs`, sleeps itself) and
  `bifrost-smtp` (`client/metering.rs`, debt-returning, with `priced_at`
  repricing and the tokio clock). The SMTP one is the most complete.
  Proposed once the contract is ruled: one debt-returning bucket in
  `bifrost-net`, owned per account per direction by `AccountNet`, shared by
  HTTP streams, IMAP connections and SMTP connections.
- **The cap multiplies per HTTP stream.** `wrap_metered` and the
  error / redirect drains build a fresh, full bucket per response, so N
  concurrent downloads get N times the cap plus a free first second each.
- **HTTP request bodies are never throttled.** Outbound bytes are recorded
  in `request.rs` but never capped, so JMAP blob uploads and Graph sends
  ignore the cap.
- **The cap multiplies per IMAP connection.** Every pool dial and the
  bootstrap connection build their own `WireMetering` from the account's
  atomic, so a pool of N connections runs at about N times the cap, and a
  redial starts with a full bucket.
- **IMAP and SMTP share the cap value, not the budget.** An IMAP-shaped
  account can fetch at the cap and send at the cap at once. The comment in
  `crates/imap/src/account/submission.rs` claiming a send cannot burst past
  a cap the fetch path is honouring claims more than the code does.
- **The cap never reaches an IMAP account's CardDAV and CalDAV
  sub-accounts.** `ImapAccount::set_bandwidth_cap` stores only the IMAP
  atomic; `set_priority` does not forward either, and the DAV sub-accounts
  keep their own `AccountNet`. A stale "until IMAP byte-metering is wired"
  comment sits next to it.
- **IMAP's bucket refills on `std::time::Instant` while sleeping on
  tokio**, the paused-clock mismatch SMTP's bucket already fixed.
- **`bifrost-net` clamps an oversized chunk's sleep at 60 seconds**, so a
  very low cap is exceeded: the overrun the IMAP and SMTP buckets warn
  against.
- **A re-tagged `AccountNet` copies the cap instead of sharing it.** The
  constructor that re-tags the account id builds a new `AtomicU64`, so a
  later `set_bandwidth_cap` on one handle does not reach the other.
- **SMTP's INBOUND cap is still per connection.** Outbound now goes
  through shared admission (`OutboundSchedule` in
  `crates/smtp/src/transport/smtp/client/metering.rs`); inbound keeps the
  charge-after-read `ByteBucket` with a per-stream `throttle_in`, so a fresh
  connection's first read is ungated whatever the shared balance owes.
  Left out of the outbound change on purpose: SMTP replies are small, and
  a read cannot be admitted before the peer decides how much arrives.
  Whether it is worth closing is a ruling, not a defect fix.
- **`AsyncLmtpTransportBuilder` has no `bandwidth_metering`.** Its
  connections can carry metering, but the public builder offers no way to
  configure it.

## Notes

- The rules for working an item are at the top of this file. One more that
  earned its keep: an existing test you must modify is a red flag; justify
  it explicitly.
- Item keys: F-items came from the phase 5B/5C/5D error-model re-audits,
  N-items from the original post-phase-4 audit, B-items from the 2026-08
  bug-hunt ledgers, C-items from the 2026-09-04 hunt. All are tracked at
  the same level; none blocks ratatoskr.
