# Deferred findings: specifications needed, and low value

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

Entries from the October 2026 defect hunt moved out of the per-crate
`notes/bugs-*.md` documents, keeping their IDs. The first group needs a
technical implementation specification before any fix. The second group looked,
on its face, low value or not a defect.

# Specifications needed

## IMAP-011 - Commands whose caller has gone are still executed

**Needs a technical implementation specification** (owner, 2026-10-21): too
consequential to resolve by ruling; it changes the documented cancellation
contract.

Scope: imap protocol.

`driver_task`'s `Run` arm runs `prepare_command` and sends whatever it dequeues
without checking `result_tx.is_closed()`. A command queued behind a slow one
whose caller timed out (stamped `timeout_inflight`) is still written later,
including non-idempotent APPEND / MOVE / STORE. Consistent with the documented
"cancellation-safe, keeps executing", but skipping a command that has not
reached the wire would turn an `InFlight` reconcile into a true `Unsent` and
close a duplicate-append window.

## SHARED-014 - Provider-owned long-running loops each own their recovery triage and backoff

**Needs a technical implementation specification** (owner, 2026-10-21).
Direction agreed: the structural option - provider push and lifecycle loops
become one-shot and the engine owns the outer loop, backoff, throttle holds and
directive routing. Specify together with SYNC-009. It also absorbs the JMAP
"push health as state" proposal (JMAP-034).

Scope: sync adoption.

Four push reconnect backoffs (IMAP `sleep_backoff`, JMAP `reader_loop`, Graph
`ReconnectBackoff`, Google renewer damper) plus two lifecycle backoffs each
re-learned the same lesson (reset only on a demonstrated healthy read; the
Google comment cites Graph's bug). Making provider loops one-shot (one poll, one
connection) and letting the engine own the outer loop, as it already owns the
change-stream poll loop, removes every local `is_terminal()` triage site at
once.

# Low value or not defects

## JMAP-032 - JMAP observations that need net facilities

Residue; both need a `bifrost-net` addition first, and the current behaviour is
recorded at the code sites and in `reference/jmap.md`.

- `redirect_policy_from_trusted_hosts` emulates "no cross-host redirects" by
  trusting the sentinel host `__bifrost_jmap_no_cross_host_redirects__`,
  because an empty allowlist means every host: a missing same-origin
  `RedirectPolicy` mode in net expressed as a magic string.
- `open_sse` fabricates `bifrost_net::Error::Status` for a passed-through 3xx;
  net defines `Status` as a terminal 4xx/5xx-style status, so classification
  sees a 3xx in a variant that never otherwise carries one. A fix needs a
  shared response shape and account mapping for passed-through redirects.

## SHARED-016 - `FlagOp::validate_for_account` fixes the operation for its caller

Scope: types adoption (lateral in bifrost-types). Minor.

It always stamps `AccountOperation::UpdateFlags` and no provider, but providers
call it from `bulk_set_flags` paths whose engine-side operation may differ in
telemetry: a shared constructor choosing the operation for its caller, the
shape the `validate_batch_input` lesson in the error model warns about.
