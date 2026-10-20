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

## JMAP-004 - The EventSource template still lets `.` and `..` through

Scope: net adoption. Residue: upload and download templates now use
context-aware encoding (`encode_template_value_at`) that protects dot-only path
values; `crates/jmap/src/event_source/stream.rs` still substitutes without it,
so a `{types}`-style or path variable of `.` / `..` navigates. Use the same
helper there.

## JMAP-020 - Implicit `Email/set` responses exist, contradicting the "no multiple responses" claim, and leave the state cache one behind

Hunter severity: P3. Scope: jmap protocol.

`reference/jmap.md` (internal request/response flow) and the doc on
`Response::get` justify not modelling multiplicity with "no currently wired
method produces multiple responses". `sync/pim.rs` send and draft-send set
`onSuccessUpdateEmail` / `onSuccessDestroyEmail` on `EmailSubmission/set`, so
RFC 8621 s7.5 has the server emit an implicit `Email/set` response under the
SAME call id after the `EmailSubmission/set` response. It works because `get`
takes the first match, but the implicit `Email/set` carries the post-move
`newState`, which `pim.rs` never feeds into `state_cache`; only the earlier
explicit state is advanced, so the cache is one state behind after every send.

## JMAP-003 - The sync blob facade still buffers

Residue: the client and transport have streaming doors with caller-chosen
ceilings, and the buffered APIs collect through them under a separate 512 MiB
ceiling. `sync/blob.rs` still buffers the whole blob before it emits batches, so
incremental delivery through the engine-facing facade is not there yet.

## JMAP-027 - WebSocket proxy inheritance and trace context

Residue: the WebSocket handshake now has bounded setup, a User-Agent, refuses
redirects and recovers once from a 401 knowing which token was rejected. It
still does not inherit the account proxy and sends no trace context.

## JMAP-038 - Smaller observations from the sync fixes

- `close()` sets `closed` before its returned future is polled; dropping the
  future before its first poll skips shutdown and makes a later close a no-op.
- `changes_walk` advances the state cache before yielding the batch, so it can
  run ahead of the durable checkpoint (harmless while the cache only feeds
  `ifInState`).
- Hydration sends repeated submitted ids as repeated wire ids and reports an
  outcome per occurrence; tests pin that, so it is a decision rather than a bug.

## JMAP-034 - Design proposals from the jmap account hunter

Not defects; recorded with the defects they would remove.

- Push health as state, not events: `watch<PushHealth>` plus a broadcast for
  hints, fixing JMAP-009 structurally and making `push_stream` a projection of
  current state.
- One state-walk policy: `changes_walk`, `scope_lifecycle` and the inventory
  loop each decide independently what a moved state or `cannotCalculateChanges`
  means. One "state walk" type owning the guard, the reseed-on-expiry policy
  and the error context (always carrying a scope or an explicit state-based
  flag into `convert_method`) removes JMAP-008 and JMAP-011 as a class.
- The PIM send path as a small state machine with compensations (create email
  -> submit -> on failure destroy; cancel -> confirm -> create) instead of
  single batched requests whose partial outcomes read as total failure
  (JMAP-005 to JMAP-007 share this shape).
