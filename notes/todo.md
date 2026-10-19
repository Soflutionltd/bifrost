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

## bifrost-sync

- **sync-F6.** (residuals of the closed F4+F5 throttle wiring) What
  bounds the now-wired `ThrottleBucket`:
  (a) This is the SINGLE home for the tenant-identity item; a near-identical
  duplicate filed under "Sync residuals" was deleted 2026-09-15, so do not
  re-file it there. `ThrottleScope::Tenant` degrades to the `Account` key because the
  error contract carries no tenant identity string - `ThrottleKey::
  Tenant(String)` exists but nothing can mint one, so a Graph tenant
  429 pauses only the observing account, not tenant siblings. Fixing it
  is a `bifrost-types` change (a tenant identity on the error or the
  advice) plus producer support in graph/net.
  (b) `Mailbox` keys are recorded (from `ErrorScope::Mailbox`) but
  excluded from the account-wide wait: the engine has no
  scope-to-mailbox mapping, so it cannot pause anything narrower than
  the account without widening a per-mailbox throttle to every scope.
  Needs a scope-to-mailbox channel (or a ruling that mailbox throttles
  stay advisory).
  (c) Cross-account enrollment is lazy (an account joins a shared
  `Provider` key only when its own error stream names the identity),
  so the FIRST provider-wide deadline is invisible to a sibling that
  has never failed. Attach-time enrollment needs the account's
  provider identity at attach - the same identity-channel shape as (a).

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

### Cross-crate shaping questions

- **Concurrency limit: only JMAP is wired.** `bifrost_net::ConcurrencyLimit`
  landed 2026-10-03 and JMAP gates its API requests by the session's
  `maxConcurrentRequests`. The other overlapping-request sites still bound
  only their own fan-out width: the two google inventory
  `buffer_unordered(HYDRATE_BATCH_SIZE)` fanouts and the caldav/carddav client
  fanouts, and no in-tree code sets `AccountSpec::concurrency_limit`. Those
  providers advertise no limit of their own, so wiring them means choosing a
  bound, which is a product decision per provider.

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

## The outbound cap is evadable by reconnecting

Found by the cold review of the 2026-09-07 throttle work; PRE-EXISTING, and
explicitly not introduced by it. `throttle_out` is per-stream while `ByteBucket`
is shared, and `poll_write` consults the bucket only AFTER the socket has
accepted bytes. So a fresh connection's first write is ungated whatever the
bucket owes: write one cap-sized chunk, drop the connection, dial again, and the
cap never binds. N concurrent connections can likewise each pass a chunk against
the same zero balance, because none of them reserved anything - ordinary async
interleaving is enough, no threads needed.

What that wave DID get wrong was the claim, since corrected in
`reference/smtp.md` and at `poll_flush`, that the next connection pays the
dropped connection's debt. It does not.

The fix is a mechanism this crate does not have: shared ADMISSION, meaning
permission held against the balance that a second connection cannot
simultaneously obtain. Querying the shared deficit is NOT sufficient - other
connections consume capacity between the query and the write - and the naive
placement is actively wrong: a shared gate consulted inside `poll_write` sits
inside `with_timeout` and re-creates smtp-CR11 one layer down, so admission
waiting has to stay outside the per-operation timer (and inside the absolute
deadline under a setup budget, with the write taking the recomputed slack).

Two shapes, each with a cost that wants a ruling rather than a pick:

- An exclusive lease held from the balance check through one socket write and
  its charge. Closes the race, but one connection stalled on a write - forever,
  under `timeout(None)` - then blocks every other connection. That is a new
  dependency between connections and must be judged deliberately.
- A byte reservation. Avoids exclusive ownership of the socket wait, but permits
  acquired over time can accumulate on stalled sockets and all be exercised at
  once, so a design promising bounded bursts has to bound outstanding admission
  or revalidate it when writes actually progress.

Either way: accepted-byte debt stays in the shared bucket and a dropped future
must neither erase it nor pay it twice; settlement of a short write has to
happen in the same poll that observes `Ready(Ok(n))`, before any await, since
refunding an accepted write on drop re-opens the reconnect escape; and the
connection-local `Sleep` can survive only as a cached wakeup, never as the
authoritative gate, because a stale one waits inside `with_timeout` and is
CR11 again.

Scope notes for whoever builds it. The async application-write funnel is
currently complete - every production async SMTP and LMTP write reaches
`write_stream_with_budget`, which drains before each write - but three things
sit outside it: the BLOCKING `NetworkStream::write` uses the same bucket
implementation and writes before charging, so a universal contract needs both
adapters; direct `AsyncNetworkStream` writes in tests bypass the connection
drain, and would need the admission mechanism wherever they claim to exercise
throttling; and TLS handshake, record overhead and shutdown traffic bypass
plaintext accounting entirely. The honest guarantee is therefore about
admission of METERED PLAINTEXT writes, not about every byte on the wire.

## Notes

- The rules for working an item are at the top of this file. One more that
  earned its keep: an existing test you must modify is a red flag; justify
  it explicitly.
- Item keys: F-items came from the phase 5B/5C/5D error-model re-audits,
  N-items from the original post-phase-4 audit, B-items from the 2026-08
  bug-hunt ledgers, C-items from the 2026-09-04 hunt. All are tracked at
  the same level; none blocks ratatoskr.
