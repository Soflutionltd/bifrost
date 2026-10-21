# bifrost-jmap defect findings

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

Filed from the October 2026 defect hunt: the jmap protocol scope, the jmap
account scope, and the net and sync adoption scopes where they reached JMAP.
Severity labels are the hunters' own.

## JMAP-034 - The PIM send path as a state machine with compensations

**Needs a technical implementation specification** (owner, 2026-10-21),
together with GOOG-027. The push-health-as-state proposal once filed here is
folded into SHARED-014.

The PIM send path as a small state machine with compensations (create email ->
submit -> on failure destroy; cancel -> confirm -> create) instead of single
batched requests whose partial outcomes read as total failure.

