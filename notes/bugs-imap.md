# bifrost-imap defect findings

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

Filed from the October 2026 defect hunt: the imap protocol and imap account
scopes.

## IMAP-004 - Literals are not emitted incrementally

Residue: FETCH body and binary literals are now `Bytes` slices of the framed
response, not copies, and the code and reference no longer overstate streaming
or the memory budget. Incremental delivery needs a chunk-aware parser and
dispatch contract, which is design work rather than a fix; the orchestrator has
suggested moving it to the technical-specification track, pending the owner.
Still open:
- A FETCH response is fully framed and parsed before delivery, so literals are
  not emitted incrementally. Standalone parsers without an owned backing still
  copy, and generic string fields still parse owned.
- Non-literal syntax and parser overhead are outside the literal ceiling.

