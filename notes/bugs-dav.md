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

## DAV-007 - Full snapshot diff and failed-href preservation still compare member identity by bytes

Residue: sync reports now match equivalent URL spellings. The snapshot diff and
the failed-href preservation still compare by bytes, so a server changing a
member's spelling between listings can misclassify it. Canonicalising once at
`resolve_href` was declined because it also changes listing, hydration and
persisted identities; a migration-aware decision is needed.

## DAV-015 - Smaller DAV observations after the dispatch fix

- Transmission history is not aggregated across redirect hops, so a retry
  chain split by a redirect loses `may_have_committed` evidence.
- The CardDAV parser comment cites RFC 6352 section 6.3.2 for the truncation
  rule; it lives in section 8.6.2.
- The outer request timeout sets its `sent` flag before net admission, so expiry
  while waiting for admission can report `InFlight` transmission evidence for a
  request that was never dispatched.
