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
from net adoption. Coverage note: public-folder EWS, autodiscover, cloud upload
and search were not audited in depth.

## GRAPH-012 - Event delta without `$select` may not return `changeKey`

Needs one live check downstream. Event inventory and changes pages no longer
send `$select` (Graph documents it as unsupported on event delta) and now
receive Graph's default delta property set. If that set omits `changeKey` or
`recurrence`, the event fingerprint degrades to `Unavailable`. Hermetic tests
cannot show which.

## GRAPH-006 - Contact and calendar `If-Match` protects nothing the caller can see

**Ruled (owner, 2026-10-21), together with IMAP-022:** build caller-version
optimistic concurrency across providers - an optional
`expected_version: Option<ServerVersion>` on the shared mutation inputs, used
when present (`If-Match` on Graph and DAV, `ifInState` on JMAP,
`UNCHANGEDSINCE` on IMAP), behaviour unchanged when absent, advertised through
`MutationConcurrency`. Drop the fetch-then-condition dance.

Contact and calendar writes still fetch the object and
condition the write on that fresh read, which covers only the milliseconds
between GET and PATCH; the public methods accept no caller version and delete
takes only an id. Real optimistic concurrency means threading the caller's etag
(`ContactCard` / `CalendarEvent`) through the shared patch types and `Account`
methods. The limit is documented in code and `reference/graph.md`.

