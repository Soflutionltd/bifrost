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

Residue: contact and event updates and deletes carry an expected version;
Graph and both DAV crates enforce it (`If-Match`; DAV refuses weak validators),
JMAP and Google refuse a supplied version before I/O and advertise no
guarantee. Versioned DAV relocations stay refused by design: copy-then-delete
cannot enforce one atomic caller precondition. Still open: the other mutation
inputs, per-target versions on bulk mail mutations, IMAP enforcement of a
caller MODSEQ (IMAP-022), and whether the absent-version fetch-then-condition
path that guards only the GET-to-write gap should go.


