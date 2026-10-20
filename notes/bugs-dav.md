# DAV crates defect findings (caldav, carddav, dav-core)

1. An entry is removed entirely when completely resolved. No historical record
   stays here.
2. Stable IDs never change and are never reused; removal leaves a gap.
3. An entry adjudicated against, verified incorrect, or whose outcome is that no
   action is taken owes comments at the code sites it names - and, where the
   claim touches a documented contract, the relevant `reference/` or `docs/`
   page - before the entry is removed, so the finding is not hunted again.
4. Once all findings are resolved, the file gets deleted.
5. Finding IDs are never written into the code or other documents. They are
   stable only until this document is drained; the next hunt writes new ones,
   and they are never deduplicated through git history. Carry the context
   inline instead.

Filed from the October 2026 defect hunt: the DAV scope, plus DAV findings
from the net, types and sync adoption scopes. Several were reported
independently by more than one scope; each entry says which.

## DAV-001 - A member the server refuses or omits is lost for good once the collection token moves past it

Scope: dav. Both crates.

Claim broken: `reference/caldav.md` says a transiently failed resource "is not
an absent one" and is "preserved rather than destroyed"; `snapshot.rs` says
the same, and `preserve_unobserved_entries` exists "so the next diff compares
like with like"; the engine contract is that a cursor checkpoint marks observed
ground.

Each lane keeps the stale entry but also checkpoints a fresh collection token
in the same snapshot, so every later poll short-circuits or syncs incrementally
from that token and the refused member's change is never asked about again:

1. CalDAV `sync-collection` lane (`account.rs::apply_sync_report` plus
   `changes_from_cursor`): a member with non-2xx, non-404/410 status (403, 503,
   507) is skipped (`continue`) while `current.sync_token` moves to the new
   token. RFC 6578 tokens are incremental, so the member's real change (create,
   update, even delete) is not reported again until something else touches it.
2. CalDAV snapshot-poll fallback: `event_snapshot(.., None, ..)` calls
   `collection_sync_token` then the listing. If the listing is an empty 207
   (the guard keeps `previous` verbatim) or has `failed` members, the result
   still carries the freshly read token, and from the next poll the cursor
   takes the `sync-collection` branch from it. The reference states the shape
   it treats as safe: "The checkpoint preserves the prior entries and etags for
   both no-observation shapes while retaining a refreshed collection token".
   Keeping that refreshed token is the bug.
3. CalDAV `establish_initial_cursor` / `inventory_stream`: the initial snapshot
   leaves `failed_hrefs` out of `entries` (and `cursor_from_snapshot` does not
   persist `failed_hrefs`) but records the collection's `sync-token`; the first
   `sync-collection` never mentions a resource that existed before it, so a
   member refused during establishment never reaches the consumer until next
   modified.
4. CardDAV ctag short-circuit (`carddav/src/account.rs::changes_stream`): a poll
   whose listing was an empty 207 or had failed members keeps previous entries
   but stores `current_ctag`; next poll `prev_ctag == current_ctag` returns with
   no listing. A transiently failed member keeps its stale etag indefinitely,
   and changes hidden behind an empty 207 wait until the ctag moves again.

Fix direction: when an observation was incomplete (empty-207 guard fired,
`failed_hrefs` non-empty, a sync member refused), do not advance the token:
checkpoint the previous token, or `None` to force a snapshot diff. Stronger:
persist pending failed hrefs in the cursor payload so the next poll re-checks
them (a cursor envelope bump the crate already knows how to do). The hunter's
structural note: the three snapshot lanes (sync-token incremental, ctag
short-circuit, full diff) each decide independently whether an observation was
complete; one `Observation { complete: bool, pending: Vec<href> }` type in
`dav-core::snapshot`, consumed by one advance-token-only-if-complete rule,
would let both crates share the decision as they share the diff.

## DAV-002 - Inventory claims COMPLETE coverage while leaving out members the server refused

Scope: dav. Both crates.

Both `inventory_stream`s say: "COMPLETE coverage is an accurate claim here:
this walk terminates wholesale on any failure, so it never advances a checkpoint
across a gap. A version that starts absorbing per-item failures must build
`InventoryBatch` directly rather than converting." `reference/types.md`: a
completeness claim discharges the coverage debt for the domain.

The walk does absorb per-item failures. `event_snapshot` / `contact_snapshot`
go through `list_event_hrefs_filtered` / `list_contacts_listing`, which return
`Usable` when at least one member committed even if others' propstats failed
(`classify_207`), putting the failures in `failed_hrefs`. `inventory_stream`
builds `items` from `snapshot.entries` only, drops `failed_hrefs`, and lifts
with `lift_complete_walk`. A consumer or engine reading a complete inventory as
authoritative treats a transiently refused event or contact as gone: the exact
case the comment says must not be converted. Emit `CoverageOutcome::Degraded`
with the failed ids as obligations, or fail the walk.

## DAV-003 - Bearer accounts never refresh on 401; an expired token is reported as `AuthLost`

Scopes: dav and net adoption (both reported it independently). Related:
SHARED-002.

Claims broken: `bifrost-net`'s `TokenSource` contract (`refresh()` is "Called
by `OAuthRefresher` ... or by the 401-retry path"; `refresh_if_unchanged`
collapses concurrent 401s; for opaque tokens with no TTL, `AccessToken` says
"the refresher falls back to the 401 path"); `reference/net.md` ("401 -> force
refresh once; second 401 -> `Error::AuthLost`"), the auth-recovery contract for
every HTTP protocol crate; `reference/caldav.md` ("read via `current().await`
per DAV request ... so a token rotated mid-sync is honored").

`dispatch.rs` opts every request out of bearer injection
(`without_bearer_auth`, documented, because the origin gate mints the header)
and mints it from `token_source.current()`; DAV attaches with
`AccountSpec::new(None)`, so net holds no refresher for it. That also opts out
of net's 401 refresh-and-retry, and nothing replaces it: no `refresh` /
`refresh_if_unchanged` call exists in the three crates. `status_error`
(`crates/dav-core/src/error.rs`) maps 401 straight to
`Auth(ReauthorizationRequired)` -> terminal `AuthLost`. Every other HTTP
provider gets one silent refresh first. An OAuth CalDAV/CardDAV account whose
token expired between proactive refreshes (or an opaque token with no expiry
hint, exactly the case forced refresh exists for) is sent to a re-auth prompt.
If the consumer hands a plain `TokenSource`, DAV never sees a refreshed token
at all.

Fix options: in `send_raw_request`, on a 401 under `Bearer` call
`refresh_if_unchanged(used)` once, re-mint, replay replayable requests. Or,
keeping the credential gate in DAV and the recovery in net: let DAV pass a
per-request credential minter to net (a `RequestBuilder` authorization callback
re-invoked after a 401). See DAV-005 for the same callback removing DAV's
redirect walk.

## DAV-004 - `status_error` drops `Retry-After` and throttle scope, contradicting `dispatch_once`'s doc, and is a diverged copy of net's ladder

Scopes: dav, net adoption and types adoption (all three reported it).

The doc on `DavDispatch::dispatch_once` (`crates/dav-core/src/dispatch.rs`):
"Everything else - an exhausted retry budget, a rate limit past its budget, a
transport failure - is left to `bifrost_net::into_account_error`, which
classifies with the retry hints and transmission evidence the DAV status ladder
does not carry."

`wire_outcome` does the opposite: `RetryBudgetExhausted { final_response:
Some(_) }` and `RateLimited`, bare and as the terminal of an `AttemptHistory`
for replayable requests, are reconstituted as `Ok(DavResponse)`. They reach
`status_error(operation, status, body, protocol)`, which takes no headers, so
every `ServerCause::{RateLimited, Unavailable, QuotaExhausted}` it mints has
`retry_hint: None` and no `throttle_scope`. No DAV crate reads `Retry-After`.
A 429/503 with `Retry-After: 300` reaches the engine with no hint where net's
`into_account_error` would have produced `RetryHint::After(300s)`, and
`retry_after_history` is discarded. The error model says `ServerCause`
"carries the optional `RetryHint` ... (the only structural channel for retry
timing)". The headers are in hand: `DavResponse` carries them, and
`send_status_request` / `settle_body` / the transport `status_error` call site
all have the response.

The ladder is also a second copy of net's `status_kind_cause`
(`crates/net/src/account_error.rs`), and they disagree (from the types hunter):

| status | net | dav-core |
|---|---|---|
| 400 / 422 | `Request(Malformed)` -> `ClientBug` | `Server(Error{400})` -> `ProviderRefused` |
| 408 / 502 / 504 | `Server(Unavailable)` (carries hint) | `Server(Error{5xx})` (no hint) |
| 412 / 423 | `Server(Error)` -> `ProviderRefused` | `ConcurrencyConflict` -> `Retry(AfterStateRefresh)` |
| 410 on a cursor scope | `SyncState(CursorInvalid)` | `Server(Error{410})` |

Some divergences are deliberate (412 as conflict is right for `If-Match`), but
nothing records which; the hint loss plainly is not. Fix: pass headers into the
ladder for 429/503 and parse with `bifrost_net::parse_retry_after`, or stop
reconstituting these two variants for callers that do not read 429/503 as data
(none found). Structural: one HTTP status table with per-protocol overrides.

## DAV-005 - DAV's hand-walked redirects reset the total request deadline on every hop

Scope: net adoption.

Claim broken: `reference/net.md` ("The total deadline is computed once before
the redirect walk ... Redirects reset their per-hop retry and 401 budgets but
never the deadline") and the `RequestDeadline` rationale.
`dav_account_spec` sets `request_timeout = Some(DAV_CLIENT_TIMEOUT)` and
`FollowRedirects::Disabled`; `DavDispatch::send_raw_request` calls
`dispatch_once` per hop, each a fresh `send()` with a fresh deadline, so a
chain up to `DEFAULT_MAX_HOPS` (10) can take up to 11 x the timeout. Fix:
compute one deadline at the top of `send_raw_request` and pass the remainder
with `RequestBuilder::timeout` per hop.

Structural proposal from the same hunter: DAV's walk exists because net strips
`Authorization` on origin change with no way to re-mint. A per-hop credential
callback on net (`RequestBuilder::authorize_hop(|url| ...)`) would let DAV's
gate become that callback and use net's walk, removing DAV-003, this entry,
and the semantic drift: DAV ignores 303 and never rewrites 301/302 methods
(net's doc says rewriting "happens exactly once and the same way for every
HTTP protocol crate"), and DAV loses `may_have_committed` across hops.

## DAV-006 - A terminal status after a net retry on a non-idempotent request is no longer readable as data

Scope: dav. Needs a check against net's actual retry policy.

`wire_outcome`'s `AttemptHistory` arm reconstitutes the terminal response only
for replayable requests. If net does retry a request declared
`idempotent(false)` (a 429/503 with `Retry-After` on the first attempt, where
the server did not process it), the terminal status goes through
`into_account_error` instead. For `MOVE`, a terminal 405/501 after such a retry
then fails the update instead of returning `Ok(false)` to start the documented
copy-then-delete fallback; for a create PUT a terminal 412 loses the DAV
ladder's `ConcurrencyConflict` mapping. `move_resource` documents "Only 405 and
501 mean no MOVE support", true only on the single-attempt path. If net never
retries non-idempotent requests the arm is dead and the comment should say so.
See SHARED-007 for the two idempotency authorities underneath this.

## DAV-007 - The CalDAV `sync-collection` lane matches hrefs by bytes, unlike the rest of the crate

Scope: dav.

`apply_sync_report` finds the entry with `known.uri == uri` (and
`binary_search_by` on byte order). Snapshot entries come from the
`calendar-query` / PROPFIND listing and sync entries from the REPORT, each
resolved through `resolve_href`, which does not canonicalise percent-encoding
case or redundant escapes. The crate already found that two spellings of one
URL occur in practice (the `same_dav_url` story behind the spurious MOVE) and
applies `same_dav_url` to the collection's own response in
`take_collection_response`, but not to members. A server spelling a member
`%40` in one REPORT and `@` in another yields a phantom `Created` plus a stale
entry never destroyed, and a 404 for the other spelling is ignored as "unknown
href". The other-readers-of-the-same-identity lesson. Structural fix:
canonicalise every href once at `resolve_href` so string equality is DAV
identity everywhere.

## DAV-008 - iTIP REPLY omits SEQUENCE

Scope: dav.

`ical.rs::rsvp_reply_ical` builds `METHOD:REPLY` with UID, DTSTAMP,
DTSTART/DTEND, ORGANIZER and the one ATTENDEE. RFC 5546 s3.2.3 requires the
organizer's `SEQUENCE` when non-zero; organizers use it to discard replies to
superseded versions, so after any reschedule a conforming organizer CUA may
ignore or misapply the reply. The reference says RSVP "posts an iTIP
`METHOD:REPLY`", implying a conforming one. The stored resource is patched
locally regardless, so the user sees accepted while the organizer may not.
(Per-occurrence RSVP is refused by design, so RECURRENCE-ID is not needed
today.)

## DAV-009 - CardDAV `contact_search` doc falsely says CalDAV treats an omitted limit as unbounded

Scope: dav. Doc only.

`carddav/src/account.rs` on `contact_search`: "the CalDAV twin's
`events_in_range` and `event_search` instead treat an omitted `limit` as
UNBOUNDED". False: CalDAV maps `None` to `EVENT_PAGE_SIZE` (250), and
`reference/caldav.md` says so, "matching the CardDAV twin's
`CONTACT_PAGE_SIZE`". Test comments in the same file ("the CalDAV twin's
unbounded one") carry the same stale claim.

## DAV-010 - The cursor payload's collection URL is not checked against the cursor scope

Scope: dav. Both crates.

`decode_cursor_snapshot` validates the scope kind, then trusts
`decoded.collection_url` from the opaque bytes, and `changes_stream` polls that
URL, not the `FolderId` in `cursor.scope`. A cursor whose scope and payload
disagree (consumer or migration bug, or a scope rename after a calendar URL
correction) silently syncs collection B and reports under scope A. The
credential gate bounds damage to admitted origins, but on one origin it is a
cross-collection mixup with no error. An equality check
(`same_dav_url(scope folder, payload url)`) turning a mismatch into
`SchemaIncompatible` / `CursorInvalid` closes it.

## DAV-012 - CardDAV hand-parses a status line and gets the protocol-less form wrong

Scope: net adoption.

Claim broken: `reference/net.md` status-line helpers ("the code is the first
token in the protocol-less form (`200 OK`), or the token immediately after an
`HTTP/` version"); the helpers exist because per-crate copies drifted.
`crates/carddav/src/parse.rs` `finish_response` (directory-search truncation,
RFC 6352 s6.3.2) tests `status.split_whitespace().nth(1) == Some("507")`.
Protocol-less `507 Insufficient Storage` (accepted elsewhere through
`status_line_code`) is not detected as truncation and falls through to ordinary
per-resource quota classification, while any garbage first token followed by
`507` is accepted. Fix: `bifrost_net::status_line_code(status) == Some(507)`.
This arrived with the recent directory-search commit: the drift the
consolidation was meant to stop has restarted.

## DAV-013 - dav-core ranks recovery classes locally, below engine directives

Scopes: types adoption and sync adoption. The sync hunter marks it "verify":
callers of `worse_recovery` were not traced exhaustively, so confirm a
mixed-leg path exists before fixing.

`dav-core/src/error.rs` `recovery_rank` / `worse_recovery` is a private
severity order over `RecoveryClass`, the only one in the workspace, used to pick
the surviving error of a multi-leg REPORT. `Engine(_)` ranks 1 and the terminal
classes (`ProviderRefused`, `ProviderContractViolation`, ...) rank 2, so a
multi-leg failure where one leg says `Engine(RestartScope)` (expired sync
token) or `Engine(DisableScope)` and another `ProviderRefused` keeps the
terminal one, and the engine terminates instead of resyncing or quarantining
the scope. A new terminal class lands at `u8::MAX` (beats `AuthLost`). If a
severity order is wanted it is a property of `RecoveryClass` and belongs next to
`is_terminal` or `plan_recovery`.
