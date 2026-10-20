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

Needs an API ruling. Contact and calendar writes still fetch the object and
condition the write on that fresh read, which covers only the milliseconds
between GET and PATCH; the public methods accept no caller version and delete
takes only an id. Real optimistic concurrency means threading the caller's etag
(`ContactCard` / `CalendarEvent`) through the shared patch types and `Account`
methods. The limit is documented in code and `reference/graph.md`.

## GRAPH-011 - Two delta observations that need an engine contract

- `changes_stream` reports every non-removed delta value as `Updated` with
  `ScopeChange::Added`, never `Created`: Graph delta cannot tell create from
  update and the account holds no per-scope id set to compare. Documented, with
  a test pinning the shape. `reference/sync.md` does not say how a consumer
  treats `Updated` for an unknown id; settle that.
- Inventory drops `@removed` values apart from mail etag eviction, so a removal
  between pages of a resumed inventory reaches the engine only if the following
  delta repeats it. Forwarding removals needs a shared inventory event and an
  engine contract that can represent them.

