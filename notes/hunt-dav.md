# Defect hunt: DAV crates (caldav, carddav, dav-core)

Scope: `crates/caldav/`, `crates/carddav/`, `crates/dav-core/`. Reviewed against
`reference/caldav.md`, `reference/carddav.md`, `reference/error-model.md`,
`reference/sync.md` and `reference/types.md`. No code was edited.

The findings are in rough order of severity. Each one names the claim it breaks.
Proposals are kept apart from defects at the end.

---

## D1. A member the server refuses or omits is lost for good once the collection token moves past it (both crates)

**Claim broken.** `reference/caldav.md` says a transiently failed resource "is
not an absent one" and is "preserved rather than destroyed". `snapshot.rs`
says "A transiently-failed resource is not an absent one, so it is never
destroyed", and `preserve_unobserved_entries` exists "so the next diff compares
like with like". The engine contract is that a cursor checkpoint marks
observed ground.

**What the code does.** Each lane keeps the stale entry, but it also checkpoints
a fresh collection token in the same snapshot. Every later poll then
short-circuits or syncs incrementally from that token, so the change the
refused member was carrying is never asked about again:

1. **CalDAV `sync-collection` lane** (`account.rs::apply_sync_report` plus
   `changes_from_cursor`). A member with a non-2xx, non-404/410 status (403,
   503, 507) is skipped (`continue`), and `current.sync_token` still moves to
   the new token. RFC 6578 tokens are incremental, so the next REPORT reports
   only changes after the new token. The member's real change (create, update,
   or even delete) is never reported again until something else touches that
   resource. "Preserve untouched" with a token advance is a silent drop.
2. **CalDAV snapshot-poll fallback.** `event_snapshot(.., None, ..)` calls
   `collection_sync_token` and then the listing. If the listing comes back as
   an empty 207 (the guard keeps `previous` verbatim) or with `failed`
   members, the result still carries the freshly read `sync_token`. From the
   next poll on, the cursor takes the `sync-collection` branch from that token,
   so what the suppressed or failed listing missed is never reconsidered. The
   reference even states the shape it treats as safe: "The checkpoint preserves
   the prior entries and etags for both no-observation shapes while retaining a
   refreshed collection token". Keeping that refreshed token is the bug.
3. **CalDAV `establish_initial_cursor` / `inventory_stream`.** The initial
   snapshot leaves `failed_hrefs` out of `entries` (and `cursor_from_snapshot`
   does not persist `failed_hrefs` at all), but it records the collection's
   `sync-token`. The first `sync-collection` from that token never mentions a
   resource that existed before it, so a member refused during establishment
   never reaches the consumer until it is next modified.
4. **CardDAV ctag short-circuit** (`carddav/src/account.rs::changes_stream`).
   This is the same shape with `getctag`. A poll whose listing was an empty
   207, or carried failed members, keeps the previous entries but stores
   `current_ctag`. On the next poll `prev_ctag == current_ctag` and the code
   returns with no listing at all. A member that failed transiently keeps its
   stale etag indefinitely, and changes hidden behind a transient empty 207 are
   not reported until the ctag moves again.

**Fix direction.** When an observation was incomplete (empty-207 guard fired,
`failed_hrefs` non-empty, a sync member refused), do not advance the
collection token: checkpoint the previous token, or `None` to force a snapshot
diff next time. A stronger version persists the pending failed hrefs in the
cursor payload, so the next poll re-checks them explicitly. That needs a cursor
envelope bump, which this crate already knows how to do.

## D2. Inventory claims COMPLETE coverage while leaving out members the server refused (both crates)

**Claim broken.** Both `inventory_stream`s say: "COMPLETE coverage is an
accurate claim here: this walk terminates wholesale on any failure, so it never
advances a checkpoint across a gap. A version that starts absorbing per-item
failures must build `InventoryBatch` directly rather than converting."
`reference/types.md`: a completeness claim discharges the coverage debt for
the domain.

**What the code does.** The walk does absorb per-item failures. `event_snapshot`
/ `contact_snapshot` go through `list_event_hrefs_filtered` /
`list_contacts_listing`. When at least one member committed, those return
`Usable` even if other members' propstats failed (`classify_207`), and they
put the failures in `failed_hrefs`. `inventory_stream` builds `items` from
`snapshot.entries` only, drops `failed_hrefs`, and then lifts the stream with
`lift_complete_walk`. A consumer or engine that reads a complete inventory as
authoritative membership treats a transiently refused event or contact as
gone. This is exactly the case the comment says must not be converted. It
should emit `CoverageOutcome::Degraded` with the failed ids as obligations, or
fail the walk.

## D3. Bearer accounts never refresh on 401: an expired token is reported as `AuthLost`

**Claim broken.** `bifrost-net`'s `TokenSource` contract: `refresh()` is
"Called by `OAuthRefresher` ... or by the 401-retry path", and
`refresh_if_unchanged` exists to collapse concurrent 401s. For opaque tokens
with no TTL, `AccessToken` says "the refresher falls back to the 401 path".
`reference/caldav.md`: the bearer token "is read via `current().await` per DAV
request ... so a token rotated mid-sync is honored".

**What the code does.** `dispatch.rs` opts every request out of bearer
injection (`without_bearer_auth`) and mints the header itself from
`token_source.current()`. That also opts out of net's 401 refresh-and-retry,
and nothing in dav-core replaces it: no `refresh` / `refresh_if_unchanged`
call exists anywhere in the three crates. A 401 goes straight to
`status_error` -> `Auth(ReauthorizationRequired)` -> `AuthLost`, the terminal
"ask the user to sign in again" class. Every other HTTP provider gets one
silent refresh first. For a TokenSource without an expiry hint (or with clock
skew), the DAV account reports a session loss that one refresh would have
fixed. The fix belongs in `send_raw_request`: on a 401 under `Bearer`, call
`refresh_if_unchanged(used)` once, re-mint, and replay replayable requests.

## D4. `dispatch_once`'s documentation contradicts `wire_outcome` on retry hints

**Claim broken.** The `dispatch_once` doc says: "Everything else - an
exhausted retry budget, a rate limit past its budget, a transport failure - is
left to `bifrost_net::into_account_error`, which classifies with the retry
hints and transmission evidence the DAV status ladder does not carry."

**What the code does.** `wire_outcome` explicitly reconstitutes
`RetryBudgetExhausted { final_response: Some(_) }` and `RateLimited` (top-level
and inside `AttemptHistory` for replayable requests) as `Ok(DavResponse)`.
They then hit `status_error`, which builds `ServerCause::RateLimited {
retry_hint: None }` / `Unavailable { retry_hint: None }` and discards the
`Retry-After` that net had already parsed. A 429 past its budget therefore
reaches the engine with no hint, and backoff falls back to defaults. Either
the comment is stale or the code is wrong. Given `reference/error-model.md`'s
retry-hint plumbing, the code is the side to fix: pass the response headers
into the ladder for 429/503, or let these two arms go to `into_account_error`.

## D5. A terminal status after a net retry on a non-idempotent DAV request is no longer readable as data

`wire_outcome`'s `AttemptHistory` arm reconstitutes the terminal response only
when the request is replayable. When net does retry a request declared
`idempotent(false)` (a 429/503 with `Retry-After` on the first attempt, where
the server did not process it - whether net does this depends on its policy),
the terminal status goes through `into_account_error` instead. For `MOVE`, a
terminal 405/501 after such a retry then fails the update instead of returning
`Ok(false)` to start the documented copy-then-delete fallback. For a create
PUT, a terminal 412 loses the DAV ladder's `ConcurrencyConflict` mapping.
`move_resource` documents "Only 405 and 501 mean no MOVE support", and that
claim holds only on the single-attempt path. Worth a test against net's actual
retry policy. If net never retries non-idempotent requests, the arm is dead
and the comment should say so.

## D6. The CalDAV `sync-collection` lane matches hrefs by bytes, unlike the rest of the crate

`apply_sync_report` finds the existing entry with `known.uri == uri` (and
`binary_search_by` on byte order). The snapshot entries come from the
`calendar-query` / PROPFIND listing, and the sync entries from the
`sync-collection` REPORT. Each is resolved through `resolve_href`, which does
not canonicalise percent-encoding case or redundant escapes. The same crate
already found that two spellings of one URL occur in practice (the
`same_dav_url` story behind the spurious MOVE). It applies `same_dav_url` to
the collection's own response in `take_collection_response`, but not to
members. A server that spells a member `%40` in one REPORT and `@` in another
yields a phantom `Created` plus a stale entry that is never destroyed, and a
404 for the other spelling is silently ignored ("unknown href"). This is the
"fixing a reader is not finished until the other readers of the same identity
are checked" lesson: the identity rule was fixed for collections and relocation
but not for snapshot membership. The structural fix is to canonicalise every
href once at `resolve_href`, so string equality is DAV identity everywhere.

## D7. iTIP REPLY omits SEQUENCE (and RECURRENCE-ID context)

`ical.rs::rsvp_reply_ical` builds a `METHOD:REPLY` with UID, DTSTAMP,
DTSTART/DTEND, ORGANIZER and the one ATTENDEE. RFC 5546 s3.2.3 requires the
REPLY to carry the organizer's `SEQUENCE` when it is non-zero. Organizers use
it to discard replies to superseded versions, so after any reschedule
(SEQUENCE >= 1) a conforming organizer CUA may ignore or misapply this reply.
The reference claims RSVP "posts an iTIP `METHOD:REPLY`", which implies a
conforming one. The stored resource is patched locally regardless, so the user
sees "accepted" while the organizer may not. (Per-occurrence RSVP is refused
by design, so RECURRENCE-ID is not needed today.)

## D8. CardDAV `contact_search` doc claims CalDAV treats an omitted limit as unbounded

`carddav/src/account.rs` on `contact_search`: "Note that the CalDAV twin's
`events_in_range` and `event_search` instead treat an omitted `limit` as
UNBOUNDED; the two crates differ on the default". That is false. CalDAV maps
`None` to `EVENT_PAGE_SIZE` (250), and `reference/caldav.md` says so
explicitly, "matching the CardDAV twin's `CONTACT_PAGE_SIZE`". The test
comments near line 2441 ("the CalDAV twin's unbounded one") carry the same
stale claim. Doc-only, but it misstates a published paging contract.

## D9. The cursor payload's collection URL is not checked against the cursor scope

`decode_cursor_snapshot` (both crates) validates the scope's kind, then trusts
`decoded.collection_url` from the opaque bytes. `changes_stream` polls that
URL, not the `FolderId` in `cursor.scope`. A cursor whose scope and payload
disagree (a consumer or migration bug, or a scope rename after a calendar URL
correction) quietly syncs collection B and reports the changes under scope A.
The credential gate bounds the damage to admitted origins, but on one origin
it is a cross-collection mixup with no error. A one-line equality check
(`same_dav_url(scope folder, payload url)`) turning a mismatch into
`SchemaIncompatible` / `CursorInvalid` would close it.

## D10. `Batch::bytes_in` is always 0 on every DAV stream

`discover_cursor_scopes`, `inventory_stream` and `changes_stream` in both
crates hard-code `bytes_in: 0`, while every one of them performs wire reads.
If `bytes_in` is documented as the bytes consumed for the batch (as on the
other providers), this misreports to any consumer or engine that budgets on
it. The response sizes are available at `DavResponse::body.len()`. This is
low severity, and a defect only if `reference/types.md` / `sync.md` give
`bytes_in` a meaning beyond "best effort". Flagged for the owner to decide.

---

## Lateral observations and smells (not defects)

- **XML matching ignores namespaces.** `xml::local_name` drops the namespace,
  so every parser matches `href`, `status`, `calendar`, `collection`,
  `sync-token` and the rest in any namespace. A vendor property such as
  `<X:status>` nested directly in a propstat, or a vendor `<Y:calendar/>`
  inside `resourcetype`, would be read as the DAV or CalDAV element. Nothing
  observed in the wild breaks it today. A namespace-aware reader (quick-xml
  `NsReader`) removes the class.
- **Doubtful drain rationale.** The drain-loop rationale in `changes_from_cursor` says that without draining
  "every change past the truncation point was lost until etag drift". Per RFC
  6578 s3.6, the truncated response's token is a valid continuation, and the
  next poll would pick up the remainder. The drain is a latency improvement,
  not a correctness fix. The comment overstates it, and that may matter if
  someone later weighs removing it. It also means that hitting
  `SYNC_TRUNCATION_ROUNDS` while still truncated is safe, which the code relies
  on silently.
- **`CalDavAccount::event_snapshot` with `home` lists the WHOLE home** (depth-1
  PROPFIND of every calendar) just to read one collection's sync-token on
  establish and inventory, so establishment is O(calendars) per scope and
  O(calendars^2) across an account. CardDAV already moved its poll path off
  that shape. The depth-0 `collection_sync_token` would serve establishment
  too.
- **`event_in_range` all-day bounds.** It compares all-day dates as UTC
  midnight against a caller window that may be zone-offset instants, so an
  all-day event can be dropped or kept a few hours off at the window edges.
  The server prefilter generally agrees, so this rarely shows.
- **`directory_search` `worst.expect("at least one admitted gateway")`.** It
  depends on `capabilities.pim_methods.directory_search` being derived from a
  non-empty `gateways`, which `open_with_client` guarantees. A test-built
  account with the capability forced on and no gateways panics instead of
  erroring. Cheap to make it an `Internal` error.
- **Structural.** The three snapshot lanes (sync-token incremental, ctag
  short-circuit, full diff) each decide independently whether an observation
  was complete, and D1 is the result. A single `Observation { complete: bool,
  pending: Vec<href> }` type in `dav-core::snapshot`, consumed by one
  "advance-token-only-if-complete" rule, would let both crates share the
  decision the way they now share the diff.
