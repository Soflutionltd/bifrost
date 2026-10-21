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

## GRAPH-006 - Caller-version optimistic concurrency has gaps in JMAP PIM, People and other mutation families

**Ruled (owner, 2026-10-21):** caller-version optimistic concurrency across
providers - an optional expected version on the shared mutation inputs,
enforced natively where present, behaviour unchanged when absent, advertised
honestly through capabilities.

Residue: contact and event updates and deletes, bulk mail mutations (per-target
`MailMutationTarget`) and event RSVP carry caller versions. IMAP takes
`UNCHANGEDSINCE` from the caller ModSeq for flag STOREs; JMAP enforces caller
Email state for flags, moves and destroys; Graph enforces caller ETags on PIM
writes and flag PATCHes; DAV enforces strong ETags on PIM writes; Google
enforces caller ETags on calendar update, delete and RSVP. Versioned DAV
relocations and versioned IMAP moves, destroys and two-sided patches stay
refused. The absent-version fetch-or-cache conditions were kept, because they
still guard the narrower observation-to-write window. Still open: JMAP contact
and event caller state, the Google People source-ETag design (GOOG-028),
Graph move and permanent-delete guarantees, caller-version surfaces for other
mutation families, and versioned bulk streams submitting targets independently
rather than as one guarded batch.


