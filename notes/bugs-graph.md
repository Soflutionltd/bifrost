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

## GRAPH-006 - Caller-version optimistic concurrency covers only contact and event updates

**Ruled (owner, 2026-10-21), together with IMAP-022:** caller-version
optimistic concurrency across providers - an optional expected version on the
shared mutation inputs, enforced natively where present, behaviour unchanged
when absent, advertised honestly through capabilities.

Residue: `ContactPatch` and `EventPatch` carry `expected_version`; Graph and
both DAV crates enforce it (`If-Match`; DAV refuses weak validators), JMAP and
Google refuse a supplied version before I/O and advertise no guarantee. Still
open: deletes (they take only an id), the other mutation inputs, bulk mail
mutations, IMAP enforcement of a caller MODSEQ (IMAP-022), versioned DAV
relocations, and whether the absent-version fetch-then-condition path that
guards only the GET-to-write gap should go.

## GRAPH-013 - Graph calendar update preflight errors are labelled `EventGet`

Lateral from the caller-version work. In `crates/graph/src/account/calendar.rs`
an update's preflight read failure is attributed to the `EventGet` operation
rather than the update the caller made, so telemetry and recovery context name
the wrong operation.

## GRAPH-014 - A versioned Graph contact update that changes folder is not checked

Lateral from the caller-version work. DAV refuses a caller-versioned update that
relocates the object. A Graph contact update whose `address_book_id` names a
different folder was not traced: if it moves the contact, the caller's ETag
guards the PATCH but not the move, and it should be refused the same way.
Separately, Graph's `expected_etag` accepts `W/"x"` but not lowercase `w/"x"`
because the weak check is case-insensitive while the strip is not.

