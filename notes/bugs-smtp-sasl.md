# bifrost-smtp and bifrost-sasl defect findings

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

Filed from the October 2026 defect hunt: the smtp and sasl scope, plus an SMTP
lateral from net adoption. Severity labels are the hunter's. The IMAP-side
SCRAM continuation defect is IMAP-002.

## SMTP-003 - The blocking transport cannot tell a busy refresher from one needing refresh

A stale or empty `OAuthRefresher` can still panic in `tokio::spawn` on a
runtime-less blocking thread, and a momentarily held state lock rejects a fresh
token as "not immediately available". SMTP cannot tell the cases apart through
the opaque `TokenSource` future; `is_coordinating()` is metadata and does not
resolve readiness. A safe fix needs a non-refreshing synchronous net snapshot
that distinguishes three states (usable cached token, refresh required, state
lock contended) while preserving typed source errors. The one-poll rejection is
pinned by a test.

