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

## JMAP-027 - WebSocket proxy inheritance and trace context need net facilities

Residue: the WebSocket handshake has bounded setup, a User-Agent, refused
redirects, one-shot 401 recovery and metering. It still does not inherit the
account proxy and sends no trace context, because `bifrost-net` exposes neither a
proxy or tunnel connector nor a public trace-context builder; the limits are
documented at the code sites.

## JMAP-034 - The PIM send path as a state machine with compensations

**Needs a technical implementation specification** (owner, 2026-10-21),
together with GOOG-027. The push-health-as-state proposal once filed here is
folded into SHARED-014.

The PIM send path as a small state machine with compensations (create email ->
submit -> on failure destroy; cancel -> confirm -> create) instead of single
batched requests whose partial outcomes read as total failure.

## JMAP-036 - Session discovery falls back to the path form only on a 404

Lateral from the origin-first well-known discovery. The path-appended
`/.well-known/jmap` is tried only when the origin answers 404. An origin that
answers 401, 403, a redirect to a login page, or a 200 with non-JMAP JSON makes
discovery fail or misparse without trying the configured path, which is
exactly the deployment the fallback exists for.

## JMAP-035 - `anchorNotFound` is classified from cursor scope alone

Lateral from the state-walk consolidation. `anchorNotFound` maps to
`SyncState(CursorInvalid)` -> `RestartScope` whenever the error context carries
a cursor scope. Inventory supplies that scope to both its query and get calls,
and `sync/account.rs` supplies it to the empty `Email/get` and `Mailbox/get`
state probes. RFC 8620 defines `anchorNotFound` only for a query given an
anchor; a get has no anchor, so an unrelated method error there would restart
the scope. Mark anchored query contexts explicitly, as the state-based flag now
does for `cannotCalculateChanges` / `tooManyChanges`.
