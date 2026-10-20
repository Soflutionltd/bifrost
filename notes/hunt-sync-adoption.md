# Hunt: provider adoption of bifrost-sync facilities

Scope: places where a provider crate hand-rolls, bypasses or contradicts a
mechanism `bifrost-sync` owns (recovery dispatch, lifecycle/push termination
handling, read-back accounting, throttle holds). Engine-internal defects are
lateral only.

Severity is the hunter's estimate. Items are split into defects and
proposals; only the first section is bugs.

## Defects

### D1 (P2). JMAP lifecycle poll dies permanently on a legitimate `cannotCalculateChanges`

`crates/jmap/src/sync/discover.rs` `scope_lifecycle` classifies a failed
`Mailbox/changes` with `JmapErrorContext::new(AccountOperation::ScopeLifecycle)`,
which carries no cursor scope. `convert_method` in
`crates/jmap/src/sync/error.rs` maps `cannotCalculateChanges` without a
cursor scope to `Protocol(ContractViolation)` (pinned by
`cannot_calculate_changes_without_scope_is_contract_violation`), which is
terminal. The lifecycle stream then yields `Terminated` and breaks; the
engine's `lifecycle_termination` maps `Terminal` to
`LifecycleTermination::Terminate`, which broadcasts a terminal
`MultiplexerEvent` and ends the lifecycle reader for good.

`cannotCalculateChanges` on `Mailbox/changes` is ordinary RFC 8620 behavior
after a long offline period (server pruned the state), not non-conformance.
The result is an account that never again hears of a folder create, rename
or delete, plus a terminal error the consumer will read as a broken
provider.

Claim broken: the in-code comment at the `ChangeWalkGuard` site says the
`ContractViolation` termination is reserved for "a non-conformant provider";
`reference/error-model.md`'s rule that a stale state is `CursorInvalid` /
restart, not terminal. Right fix: on this error the poller should reseed its
`mailbox_states` entry from a fresh `Mailbox/get` (diffing against
`mailbox_names` to emit the missed lifecycle events), or at minimum classify
it with a scope so the engine's restart lane applies. The current shape
reimplements the "what does a stale state mean" decision locally and gets it
opposite to the engine's.

### D2 (P2). Google and JMAP lifecycle streams reimplement the engine's lifecycle termination policy, and disagree with it

`crates/google/src/account/scopes.rs` (`scope_lifecycle_stream`, the
`Err(error)` arm) and `crates/jmap/src/sync/discover.rs` (both `Err` arms)
apply their own triage: `is_terminal() || requires_engine_action()` yields
`Terminated`, everything else is swallowed into a local sleep (Google
doubling to 480 s, JMAP fixed 300 s).

The engine already owns this decision: `lifecycle_termination` in
`crates/sync/src/multiplexer/mod.rs` routes through `plan_recovery` and maps
`Retry` / `Reconcile` to `LifecycleTermination::Reconnect`. The
`ScopeLifecycleEvent::Terminated` doc in `crates/types/src/account.rs` says
the carrier exists precisely so the protocol no longer has to
"sleep-and-retry silently". Because both providers pre-filter, the engine's
`Reconnect` arm is unreachable for them, and:

- `Retry { AfterAuthRefresh }` is swallowed. Nothing in the lifecycle loop
  refreshes auth, so an expired token silently stalls lifecycle until some
  other path happens to refresh it.
- `Reconcile(_)` classes are swallowed as if transient.
- A rate limit's retry hint is ignored: the provider sleeps on its own
  cadence and the engine's throttle registry never sees the error, so no
  hold is recorded for sibling work, and the poll can fire inside a hold the
  engine is enforcing elsewhere.
- Three different backoff policies (Google exponential, JMAP flat, engine
  reconnect delay) govern the same class of failure.

Claim broken: the `scope_lifecycle_stream` trait doc; the engine as the one
place recovery is decided (`reference/sync.md`, "Error / Terminated /
Warning": "the engine dispatches every termination through
`plan_recovery`"). Fix: providers emit `Terminated` for every
classified failure and let the engine reconnect; if the engine's reconnect
delay (starts at 50 ms in the lifecycle task) is too aggressive for a
long-period poll, that is an engine knob to fix, not a reason for each
provider to keep its own copy. Check the engine reconnect delay grows and
honors retry hints before flipping (lateral risk: the hole moves).

### D3 (P2). Push readers swallow engine directives, and IMAP never reports a push failure at all

The push reconciler (`crates/sync/src/push/reconciler.rs`,
`WatchEvent::Terminated` arm) routes `RecoveryPlan::Engine(directive)` to
the reopen channel. Providers mostly never let a directive reach it:

- JMAP `reader_pass` (`crates/jmap/src/sync/push.rs`): both the handshake
  error and the read-loop error check only `is_terminal()`. An `Engine(_)`
  class (capability changed, session changed, restart-account) is treated as
  a transient reconnect and retried on the backoff forever.
- Graph EWS (`crates/graph/src/account/ews_stream.rs`) and the Google
  renewer (`crates/google/src/account/push.rs`, renewal failure arm) use the
  same `is_terminal()`-only test.
- IMAP (`crates/imap/src/account/push.rs`, the IDLE worker): `dial_idle` and
  `select` errors are discarded with `Err(_)` and redialed on backoff. IMAP
  emits no `WatchEvent::Terminated` anywhere. Auth lost, a revoked or
  deleted IDLE folder (`NO` on SELECT), or a capability break produce an
  endless `Disconnected` plus redial loop that the engine can neither see
  nor act on. A deleted folder chosen by `choose_idle_folder` will be
  re-selected forever.

Claim broken: the reconciler's documented directive routing for push
termination; `reference/error-model.md`'s rule that recovery classes are
acted on centrally. Each provider re-decided "which push errors matter"
and all chose a narrower set than the engine handles. Fix: providers should
forward any non-`Retry` classification as `Terminated` (and IMAP should
classify at all); transient retry can stay local, but only for `Retry`.

### D4 (P2/P3). `MutationSuccess::Downgraded` is routed to a read-back that can only ever report `failed_terminal`

`reference/sync.md` ("Mutation pipeline") routes `Downgraded` to
`PendingReadback` so "the final accounting come[s] from observed state".
But `crates/sync/src/mutation/readback.rs` compares the observation against
the ORIGINAL request, not against the `actual` effect the provider reported:

- Gmail trash-instead-of-destroy (`apply_destroy` in
  `crates/google/src/account/mutation.rs`): the destroy read-back treats a
  successful fetch as `ReadbackTarget::Destroy => false`, i.e. `Mismatched`,
  so every trashed message is counted `failed_terminal` (`still_failed`).
  The consumer is told the delete failed, when the message was in fact moved
  to Trash, and the `MutationEffect` describing that is thrown away.
- Gmail partial flags (`label_patch_successes` with unsupported flags):
  `matches_target` checks the requested `FlagOp`, which includes the flag
  Gmail cannot store, so the read-back is deterministically `Mismatched`.

Each costs a full hydrate per id to learn an answer that was knowable
without one, and the answer misreports what happened. Claim broken: the
"observed state" justification in sync.md, and the `Downgraded { actual }`
payload's own reason to exist. Fix lives at the seam: read-back should
verify against `actual` (trashed means in-Trash, partial means the
supported subset), and the counters need a `downgraded` bucket rather than
folding into `failed_terminal`.

### D5 (P3, verify). dav-core ranks recovery classes locally, ranking engine directives below terminal failures

`worse_recovery` / `recovery_rank` in `crates/dav-core/src/error.rs` pick the
surviving error of a multi-leg REPORT. `Engine(_)` ranks 1, the terminal
classes (`ProviderRefused`, `ProviderContractViolation`, ...) rank 2. A
multi-leg failure where one leg says `Engine(RestartScope)` (expired sync
token) or `Engine(DisableScope)` and another says `ProviderRefused` keeps the
terminal one, so the engine terminates instead of resyncing or quarantining
the one scope. The engine has no shared "which class dominates" rule; this
is a provider-owned copy of a policy that belongs next to `plan_recovery`.
Callers of `worse_recovery` were not traced exhaustively; confirm a
mixed-leg path exists before fixing.

## Lateral findings and smells

- `crates/imap/src/account/mod.rs` comment on `scope_lifecycle_stream` says
  it "streams folder lifecycle events from push state". The implementation in
  `crates/imap/src/account/scopes.rs` is a stream that never yields and ends
  at shutdown, and `reference/imap.md` says IMAP emits no scope-lifecycle
  events. The comment is false.
- Four push reconnect backoff implementations (IMAP `sleep_backoff`, JMAP
  `reader_loop`, Graph `ReconnectBackoff`, Google renewer damper) plus two
  lifecycle backoffs each re-learned the same lesson (reset only on a
  demonstrated healthy read; the Google comment explicitly cites Graph's
  bug). A single reconnect-backoff primitive in `bifrost-net` or
  `bifrost-types`, with the reset rule and retry-hint honoring built in,
  would make that lesson one place. Proposal, not a defect.
- Structural proposal: the shape behind D2 and D3 is that providers own long
  -running loops (lifecycle polls, push readers) and therefore own their
  recovery triage. A cleaner contract is that provider loops are one-shot
  (one poll, one connection) and the engine owns the outer loop, backoff,
  throttle holds and directive routing - the same way it already owns the
  change-stream poll loop. That removes every local `is_terminal()` triage
  site at once.
