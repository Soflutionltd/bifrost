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

## DAV-007 - Full snapshot diff and failed-href preservation still compare member identity by bytes

Residue: sync reports now match equivalent URL spellings. The snapshot diff and
the failed-href preservation still compare by bytes, so a server changing a
member's spelling between listings can misclassify it. Canonicalising once at
`resolve_href` was declined because it also changes listing, hydration and
persisted identities; a migration-aware decision is needed.
