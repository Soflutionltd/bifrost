# Deferred findings: rulings needed, and low value

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

Entries from the October 2026 defect hunt moved out of the per-crate
`notes/bugs-*.md` documents, keeping their IDs. The first group waits on an
owner ruling. The second group looked, on its face, low value or not a defect:
unreachable today, already documented, design proposals, or optimizations.
Nothing here has been verified against the code.

# Rulings needed

## JMAP-033 - JMAP's well-known rule contradicts net's `well_known_url` rule

Scope: net adoption. Needs an owner ruling.

`reference/net.md` says appending the well-known suffix "to a path-bearing
configured base is wrong: it addresses a resource that is not a discovery
endpoint", and DAV uses `bifrost_net::url::well_known_url`.
`crates/jmap/src/client.rs` `well_known_session_url` appends
`/.well-known/jmap` to whatever path the caller gave, with a "Decision note"
defending it. Two binding statements disagree about the same RFC 6764/8620
rule. If JMAP's reasoning holds, it should be origin-rooted first with the
path-appended form as fallback, the shape DAV uses with its configured base.

## SMTP-006 - A SCRAM `e=` error is classified differently from a 535 for the same condition, and differently from IMAP

Hunter severity: P2. Needs a ruling: document-versus-document disagreement.

`e=invalid-proof` (wrong password) becomes `SaslError::AuthFailed` ->
`InvalidInput` plus Auth -> `Authorization(PolicyBlocked)` (`NeedsPolicyChange`).
The same wrong password answered by plain `535 5.7.8` becomes
`Authentication(ReauthorizationRequired)` (`AuthLost`). IMAP maps `AuthFailed`
to `Error::auth_with_code`, the authentication lane. `reference/sasl.md` says
"SMTP's split is the same", and it is not: one credential failure gets two
remediations depending on the server's framing. `reference/smtp.md` records the
`PolicyBlocked` routing as intent.

## GOOG-003 - Inventory turns throttling, quota holds, auth loss and transport faults into per-object obligations and keeps walking

Hunter severity: P1.

`inventory.rs::inventory_stream_cancellable`, hydration loop: every hydration
error other than `NotFound(Message)` becomes an `InventoryObligation::Object`
and the walk continues regardless of `RecoveryClass`. Under a project quota
hold (`LocalHold`, no wire call), a 429 / `userRateLimitExceeded`, an expired
token (`AuthLost`), or a network drop, every remaining message (500 per page,
every page) is recorded as an object the walk could not represent, and the walk
completes as `Degraded` coverage instead of terminating so the engine can back
off.

Claims broken: the obligation's justification in this file ("the obligation was
raised because this object could not be represented"); the error-model rule,
applied by the mutation lane through `terminates_mutation_stream`, that
`Retry`, `AuthLost`, `Engine` and `Reconcile` end the stream rather than being
laundered into per-item failures (the `mod.rs` bisection comment calls that "a
worse lie"). It also floods the ledger with thousands of obligations, each with
its own retry budget. Fix: the same `RecoveryClass` gate the mutation driver
uses; per-object obligations only for object-attributable permanent classes.

The same question applies to `get_stream`, which deliberately sends every
per-id error to `Failed` (documented in `reference/google.md`: "a hydration
item named its own id and affects no other"). A 429 or `AuthLost` across 32 ids
becomes 32 `Failed` lanes instead of one terminator; for account-wide classes
the premise is false. Worth a ruling.

## DAV-011 - `Batch::bytes_in` is always 0 on every DAV stream

Scope: dav. Low severity; the hunter flags it for an owner ruling on what
`bytes_in` means.

`discover_cursor_scopes`, `inventory_stream` and `changes_stream` in both crates
hard-code `bytes_in: 0` while all perform wire reads. If `bytes_in` means bytes
consumed for the batch (as on other providers), this misreports to anything
budgeting on it. Sizes are at `DavResponse::body.len()`. A defect only if
`reference/types.md` / `sync.md` give `bytes_in` a meaning beyond best effort.

# Low value or not defects

## JMAP-025 - `maxConcurrentUpload` / `maxSizeUpload` are not consulted, and the stated rationale is stale

Hunter severity: P3 (documentation inconsistency today). Scope: jmap protocol.

`reference/jmap.md` leaves `maxConcurrentUpload` unread because "every upload
door awaits one upload at a time". True per call site, but the engine can run
several `attachment_upload` / send operations at once on one account, and
uploads are not gated by the API `ConcurrencyLimit`. Concurrent uploads happen
from different callers, the same overlap argument the doc uses to justify the
client-wide API gate. The gate belongs on the transport for the same reason.

## JMAP-026 - EventSource `{types}` expansion edge cases

Hunter severity: P3 (unreachable from `sync/` today). Scope: jmap protocol.

`Client::event_source` uses `types.take()`, so a template naming `{types}`
twice expands the second as `*` (subscribe to everything). `Some(empty_iter)`
expands to an empty string, which servers read variously as nothing or an
error.

## JMAP-032 - Smaller jmap protocol observations

Laterals, jmap protocol and net adoption. The first bullet suggests removing
published surface, which needs the owner's decision, not a fix pass.

- `send_ws_awaiting`, `PendingRequests` and the WebSocket request/response
  correlation machinery have no caller in `sync/`; only push frames ride the
  WebSocket. The generation/teardown logic is real complexity kept alive only
  by its own tests.
- `Session::capability_config` round-trips the untagged `Capabilities` enum
  through `serde_json::Value`; for a `Malformed` entry it re-parses into
  `C::Config` and can SUCCEED where `try_cap!` failed if `C::Config` is laxer,
  making the bridge disagree with the three-state accessors. No live caller
  found.
- `Authorization::account_token_source` hands net
  `StaticTokenSource::new("", None)` for Basic "because AccountNet requires a
  token source". Stale: `AccountSpec::token_source` is `Option` and net.md
  says `None` is the non-bearer shape; it also builds a pointless refresher
  over an empty token.
- `redirect_policy_from_trusted_hosts` emulates "no cross-host redirects" by
  trusting the sentinel host `__bifrost_jmap_no_cross_host_redirects__`,
  because an empty allowlist means every host: a missing same-origin
  `RedirectPolicy` mode expressed as a magic string.
- `open_sse` fabricates `bifrost_net::Error::Status` for a passed-through 3xx;
  net defines `Status` as a terminal 4xx/5xx-style status, so classification
  sees a 3xx in a variant that never otherwise carries one. Its passthrough
  drain (`PASSTHROUGH_BODY_CAP`) also has no truncation marker and silently
  stops on a read error (see SHARED-011).

## IMAP-011 - Commands whose caller has gone are still executed

Opportunity rather than contract break. Scope: imap protocol.

`driver_task`'s `Run` arm runs `prepare_command` and sends whatever it dequeues
without checking `result_tx.is_closed()`. A command queued behind a slow one
whose caller timed out (stamped `timeout_inflight`) is still written later,
including non-idempotent APPEND / MOVE / STORE. Consistent with the documented
"cancellation-safe, keeps executing", but skipping a command that has not
reached the wire would turn an `InFlight` reconcile into a true `Unsent` and
close a duplicate-append window.

## IMAP-013 - Smaller imap protocol observations

Not defects, imap protocol.

- `CompressedStream::write_all` allocates a fresh 16 KiB `deflate_buf` on every
  write; the wire sender calls it once per admitted chunk. Keep one scratch
  buffer.
- `expand_uid_ranges` uses `u32::MAX` as the `*` sentinel, so a legal UID
  4294967295 is refused as truncated. Documented, but it conflates a value with
  a token; a typed `SeqBound::Star` in `UidRange` removes it.
- `CommandWriter::string` silently strips NUL bytes from any string, mailbox
  names included, and only warns: a NUL-bearing name is rewritten into a
  different name rather than refused. `MailboxName` validation probably rejects
  NUL first; not every `string()` caller was verified.
- The authenticate ladder's `Err(other) => return Err(other)` aborts on the
  first non-`MissingCapability` failure, so a server NO on SCRAM-SHA-256-PLUS
  (e.g. broken channel-binding config) never falls through. Looks deliberate
  (downgrade safety) but the rationale is not written next to the arm.

## SYNC-017 - Lanes rank accounts, never work kinds

Structural observation, sync engine. Not a defect.

Every admission (poll, push, backfill partitions, deferred inventory, mutation)
passes `control.priority_snapshot()`, the ACCOUNT's priority. The four-lane
scheduler ranks accounts against each other and never work kinds: within one
account a historical backfill partition competes equally with a live poll and
foreground mutations; only the sync/mutation sub-pool split separates them. If
lanes are meant to keep cold-start backfill from delaying live mail, nothing
routes backfill to `Background` / `Bulk`. If not, four lanes per account is more
machinery than one priority per account needs.

## SHARED-014 - Provider-owned long-running loops each own their recovery triage and backoff

Scope: sync adoption. Proposal, not a defect; the shape behind SHARED-012 and
SHARED-013.

Four push reconnect backoffs (IMAP `sleep_backoff`, JMAP `reader_loop`, Graph
`ReconnectBackoff`, Google renewer damper) plus two lifecycle backoffs each
re-learned the same lesson (reset only on a demonstrated healthy read; the
Google comment cites Graph's bug). Options: a single reconnect-backoff
primitive in `bifrost-net` or `bifrost-types` with the reset rule and
retry-hint honouring built in; or, more structurally, make provider loops
one-shot (one poll, one connection) and let the engine own the outer loop,
backoff, throttle holds and directive routing, as it already owns the
change-stream poll loop. That removes every local `is_terminal()` triage site
at once.

## SHARED-016 - `FlagOp::validate_for_account` fixes the operation for its caller

Scope: types adoption (lateral in bifrost-types). Minor.

It always stamps `AccountOperation::UpdateFlags` and no provider, but providers
call it from `bulk_set_flags` paths whose engine-side operation may differ in
telemetry: a shared constructor choosing the operation for its caller, the
shape the `validate_batch_input` lesson in the error model warns about.

## GRAPH-007 - Webhook expiry policy is stale and leaves a thin renewal margin

Low severity.

`webhooks.rs`: `MAX_EXPIRATION_MINUTES = 4230`,
`DEFAULT_EXPIRATION_MINUTES = 1440`. Graph's documented maximum for Outlook
message, event and contact subscriptions without resource data is 10080
minutes; 4230 was the old limit. The real issue is coupling: a 10-minute
renewal tick and 30-minute threshold mean a worker missing three ticks (process
suspend, laptop sleep) lets a subscription expire into the 404-then-recreate
path, which has a notification gap by construction. A longer expiry widens the
margin for free.

## GRAPH-010 - Delta inventory selects full bodies

Not a contract defect; the largest optimisation visible in scope.

`MESSAGE_SELECT` is shared by inventory, delta and hydration and includes
`body`, `uniqueBody` and `internetMessageHeaders`. The inventory and changes
lanes use only id, parent, flags, categories, changeKey, conversationId and the
Message-ID, References and In-Reply-To headers, so every 50-message delta page
downloads 50 full HTML bodies twice over that are discarded. On a first sync of
a large mailbox this is most of the bytes. Give the sync lanes a narrow select.

## DAV-014 - Smaller DAV observations

Scope: dav. Not defects. The drain-rationale bullet is a cheap comment fix
worth doing even if the rest is skipped.

- XML matching ignores namespaces: `xml::local_name` drops the namespace, so
  every parser matches `href`, `status`, `calendar`, `collection`,
  `sync-token` and the rest in any namespace. A vendor `<X:status>` nested in a
  propstat, or a vendor `<Y:calendar/>` in `resourcetype`, would be read as the
  DAV or CalDAV element. Nothing observed breaks today; a namespace-aware reader
  (quick-xml `NsReader`) removes the class.
- The drain-loop rationale in `changes_from_cursor` says that without draining
  "every change past the truncation point was lost until etag drift". Per RFC
  6578 s3.6 the truncated response's token is a valid continuation, so the
  drain is a latency improvement, not correctness. The comment overstates it,
  which matters if someone weighs removing it, and hitting
  `SYNC_TRUNCATION_ROUNDS` while still truncated is safe, which the code relies
  on silently.
- `CalDavAccount::event_snapshot` with `home` lists the WHOLE home (depth-1
  PROPFIND of every calendar) to read one collection's sync-token on establish
  and inventory: O(calendars) per scope, O(calendars^2) per account. CardDAV
  already moved its poll path off that shape; the depth-0
  `collection_sync_token` would serve establishment too.
- `event_in_range` compares all-day dates as UTC midnight against a window that
  may be zone-offset instants, so an all-day event can be dropped or kept a few
  hours off at the edges. The server prefilter generally agrees.
- `directory_search` `worst.expect("at least one admitted gateway")` depends on
  `capabilities.pim_methods.directory_search` being derived from non-empty
  `gateways` (which `open_with_client` guarantees). A test-built account with
  the capability forced on and no gateways panics; cheap to make it an
  `Internal` error.
