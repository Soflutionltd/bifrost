# bifrost-graph defect findings

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

Filed from the October 2026 defect hunt: the graph scope, plus a Graph lateral
from net adoption. Ordered by the hunter's confidence times impact. Where a
finding depends on Graph wire behaviour not verifiable in-repo, it says so.
Coverage note: public-folder EWS, autodiscover, cloud upload and search were
not audited in depth. The push-merge chain-reclassification finding is
SHARED-005; the Retry-After doc mismatch is SHARED-006; EWS push triage is
SHARED-013.

## GRAPH-002 - The etag cache mixes two ETag forms, one not a valid `If-Match`

`inventory.rs graph_etag` prefers `changeKey` over `@odata.etag`, and
`MESSAGE_SELECT` (`types.rs`) selects `changeKey`, so every inventory and delta
page caches the RAW change key (`CQAAABYAAA...`). Two other paths cache
differently: `refresh_missing_etags` GETs `?$select=id` (no `changeKey`) and
falls back to `@odata.etag` (`W/"CQAAABYAAA..."`); the post-write update in
`update_etag_cache_from_responses` reads the `ETag` response header first, the
quoted weak form. `request_for_mutation` sends whatever is cached as
`If-Match`. A bare change key is not an RFC 9110 entity-tag, and the same
message gets a different `If-Match` depending on which path last refreshed it;
on the common path (seen by inventory or delta, then flagged) it is the
unquoted form. Depending on Exchange's parser the result is a 412 on every
first write (a preflight-and-retry loop, possibly a permanent
`ConcurrencyConflict`) or a silently ignored precondition.

Claims broken: the reference says the etag index powers `If-Match` and that
`MESSAGE_SELECT` "explicitly requests `changeKey`" for that purpose. It also
undermines `Fingerprint.server_version` (`ETag(changeKey)` from inventory vs
`W/"..."` elsewhere). Fix: normalise at one point (always `@odata.etag`, or
always wrap as `W/"<changeKey>"`), pinned by a test feeding one item through
delta and refresh and asserting identical header bytes. Exchange's handling of
the unquoted form is unverified here.

## GRAPH-003 - Inventory and delta event ids cannot be used at the typed calendar doors

`inventory_entry_from_value` and `changes_stream` mint every object id through
`foreign::encode_message_id(scope, id)`, including for `ObjectType::Event` and
`ObjectType::Contact` scopes, so an event scope yields the bare Graph event id.
The typed calendar surface accepts only composite `{calendar}::{event}` ids;
`split_event_id` refuses anything without the separator as `Request(Malformed)`
(reference: "An `EventId` without the calendar separator was never minted
here"). `get_stream` is mail-only (`get.rs hydrate_url_for_id`). An event the
sync engine reports has no door accepting its id; a consumer must know to
rebuild `{scope.folder}::{id}`, which nothing documents.

Claims broken: composite `EventId`s are THE identity scheme (reference);
`reference/sync.md`: an id emitted by inventory or changes names an object the
account can read. The mint is also mail-shaped for foreign scopes: a foreign
contact or event scope would get a `{mailbox}\u{1f}{id}` tag that `contacts.rs`
(always `account.client`, the primary) never decodes. Side effect: event and
contact change keys go into the mail `etag_index` LRU, where nothing reads
them, evicting live mail etags and forcing extra preflight GETs. Fix: mint
per-type ids at the sync lanes (composite `EventId` for events), or document
bare ids and give the typed doors an accepting form; keep non-mail etags out of
`etag_index`.

## GRAPH-004 - The calendar delta request likely sends parameters Graph rejects or ignores

Needs one live check downstream.

`initial_delta_request` builds
`/calendars/{id}/calendarView/delta?startDateTime=..&endDateTime=..&$select={EVENT_SELECT}`.
Graph's event-delta documentation lists only `startDateTime`, `endDateTime`,
`$deltatoken` and `$skiptoken`, says `$select` is not supported, and controls
page size with `Prefer: odata.maxpagesize`. Either a 400 on every event scope's
first inventory (a capability silently dead; hermetic tests only assert the URL
string), or `$select` is ignored and delta returns its reduced property set, so
`changeKey` (GRAPH-002, GRAPH-003) and fields like `recurrence` are absent and
the fingerprint degrades to `Unavailable`. The reference also says calendar
reads send `Prefer: outlook.timezone="UTC"`; the delta lane uses plain
`client.get` with no Prefer, so start and end arrive in the mailbox zone. Claim
at risk: event scopes are advertised as delta-synced ("event deltas per
`calendars/{id}`").

## GRAPH-006 - Contact and calendar `If-Match` protects nothing the caller can see

`contacts.rs update` / `delete` and `calendar.rs` update / delete GET the
object, then immediately send `If-Match` with the fetched etag, covering only
the milliseconds between GET and PATCH. A caller doing read-modify-write from
an older read (the normal UI flow, and the echo pattern the reference describes
for events) overwrites a concurrent server edit with no conflict; when the
fetch returns no etag the write goes unconditional silently. The reference
calls this "fetch-then-`If-Match`", but the module summary sells "`If-Match`
etag mutations", and the mail lane gets real optimistic concurrency from the
cached etag. Either thread the caller's version (`ContactCard` /
`CalendarEvent` etag) through, or drop the concurrency-protection claim.

## GRAPH-008 - Renewal recreate treats a 404 for a missing resource as "subscription gone"

`subscription_is_gone` treats any 404 or 410 on `PATCH /subscriptions/{id}` as
recreate. A recreate POST for a resource whose folder was deleted then 404s
too, surfacing as `phase = "recreate"`, `Server(Error 404)` or
`NotFound(Mailbox)`, terminal or retry-forever depending on mapping. If
retryable, the worker issues a doomed PATCH plus POST every tick until reopen,
because the stale row is kept deliberately. Pin the folder-deleted recreate as
terminal (with `mark_subscription_terminated`) with a test.

## GRAPH-009 - A mid-inventory cursor with its outer marker cleared reseeds the whole account

`cursor.rs decode_cursor` makes the outer `ChangeCursor::advanced_through`
authoritative, including clearing it. `inventory_stream_from` then sees
`inventory_in_progress = true` with no marker and terminates with
`CursorError::SchemaIncompatible`, which per the v2 comment "derives
`Engine(SchemaIncompatible)`: the engine drops every durable cursor". One
scope's ambiguous position wipes every scope on the account. `changes_stream`
uses `InventoryInProgress` (scoped restart) for the sibling case, and
`CursorError`'s own doc for that variant says scope-local restart is the point;
use it (or another scope-local arm) here. The sibling-arm shape from AGENTS.md;
compare SYNC-004 for the engine's own scope-less schema reset.

## GRAPH-011 - Smaller graph observations

- `changes_stream` emits `ObjectChange::Updated` plus `ScopeChange::Added` for
  every non-removed delta value, including items new to the scope, and never
  `Created`. If the engine treats `Updated` on an unknown id differently from
  creation, new mail reads as an update; check against `reference/sync.md`'s
  `ObjectChangeKind` semantics.
- `inventory_stream_from` ignores `@removed` values except for etag eviction.
  On a RESUMED inventory (v4 page cursor) a removal between pages never reaches
  the engine during inventory and relies on the following delta walk to repeat
  it, which Graph does not guarantee for an item the enumeration already
  reported as removed.
- `changes.rs` stores the BARE id as `last_seen_id` in the page marker while
  inventory stores the encoded id. Documented as deliberate and unread for
  resume, but one field name with two encodings in one persisted type will bite
  its first reader.
- `client.rs` `execute_wire` sets `Content-Type` on every request, including
  bodyless GET and DELETE. Google's `execute` has an explicit comment that this
  "announced a JSON body on bodyless GET and DELETE requests" and was removed
  there; Graph still does it. (From net adoption.)
- `delete_thread` applies the caller's container-based decision to every
  conversation member, so a thread destroyed from Trash can permanently delete
  members that live elsewhere (compare the JMAP thread-destroy entry).
- A destroy subresponse with status 404 reports `Skipped` but leaves the cached
  etags for that id in `etag_index`.
- The shared-thread test's failure diagnostic indexes `shared_requests[4]` after
  asserting only three requests.
