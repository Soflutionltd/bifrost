# bifrost-sync defect findings

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

Filed from the October 2026 defect hunt: the sync engine scope, plus the
engine-side read-back finding from the sync adoption scope. Provider-side
recovery triage that bypasses the engine is in SHARED-012 and SHARED-013.

Coverage note from the engine hunter: `cursor/coverage.rs`, `cursor/ledger.rs`,
`cursor/ledger_envelope.rs`, `backfill/*`, `engine/lane.rs` and `repair.rs`
were NOT covered in depth. They are the largest files and need their own pass.

## SYNC-009 - Push sink: queued invalidations can be lost outright, and the ingress queue is unbounded

Scope: sync engine.

`push/mod.rs`, `forward_events`: on a full per-account channel a non-lossless
event is replaced by a coalesced `Invalidated { Unknown }` and sent with a
100 ms `timeout`. If the reconciler is busy past that (an account-wide throttle
sleep inside `drive_scopes`, a slow drive), the coalesced event is DROPPED and
the original hint's scopes are lost until their next poll (up to `poll_max`,
30 min default). Separately the per-registration ingress is
`mpsc::unbounded_channel`, and the forwarder blocks indefinitely on a lossless
event (`sender.send(..).await`), so a webhook storm behind one blocked
`Warning` / `Terminated` grows memory without bound.

Claims broken: `MultiplexerConfig::watch_capacity` presents the buffer as
bounded; the drop counter is documented as counting "PAYLOAD loss, not delivery
loss", yet delivery loss happens and is counted the same way.
`WatchEvent::Disconnected` is also coalesced into an invalidation, so the "push
transport disconnected" warning is lost. A sticky "needs full reconcile" flag
drained by the reconciler would replace both the timeout and the unbounded
queue: lossless, bounded, no worse than the coalesced event.

## SYNC-011 - `Terminated` events carry non-terminal recovery classes

Scope: sync engine.

`recovery::establish_failure_error` is byte-for-byte `restart_scope_error`:
`SyncState(CursorInvalid)` -> `Engine(RestartScope(scope))`. It is broadcast as
`SyncEvent::Terminated` when a scope's re-establishment budget or the account
restart budget is exhausted (`re_establish_scope_with_backoff`,
`restart_account`). A consumer dispatching on the `RecoveryClass` is told
"restart this scope" about a scope the engine abandoned, and for
`restart_account` `RestartScope(Account)` about an account now paused with
`RetryBudgetExhausted`. The doc says the synthetic error honours "after three
failures broadcast `Terminated(last_error)`"; it honours the event, not the
meaning. It needs a classification deriving terminal or operator-attention.

## SYNC-014 - Contract-violation `Terminated` sends bypass the drive-generation fence

Scope: sync engine.

`drive_changes_stream`'s three contract-violation paths send `Terminated` on
the raw sender with no `publish_if_drive_generation` fence, so a drive fenced
out by a reattach can still broadcast a scope `Terminated` for the old handle's
misbehaviour after the cutover.

## SYNC-015 - `handle_schema_incompatible` resets only registered scopes

Scope: sync engine.

It resets only `cursors.all_scopes()`. Durable rows for scopes not currently in
the registry (deferred-inventory scopes still establishing, scopes whose
re-establishment budget ran out) keep their completion markers, exempt from the
re-walk the reset claims to force ("delete every backfill checkpoint - the
completion marker included").

## SYNC-017 - `apply_repair_resolutions` mutates the authoritative ledger before persisting

Scope: sync engine. Lateral from the ack writer fix.

Same shape as the operator waive/block defect that was just closed: the repair
resolution path in `engine/ack.rs` applies discharges to the in-memory ledger
and then persists, so a failed write leaves the discharges available to later
ledger-bearing writes. Apply to a candidate ledger and adopt it only on success.

## SYNC-007 - A proven non-landing write is never resubmitted

Scope: sync engine. Residue: a read-back mismatch now stays pending instead of
becoming `failed_terminal`. Once read-back proves an `Uncertain` write did not
land, replay is the safe case the uncertainty lane protected, yet the pipeline
still never resubmits it. Replay was deferred because reconciliation can need
consumer-side dedupe; a ruling is needed on whether the engine replays.

## SYNC-018 - Inventory resume capability and stream disagreement uses account-wide schema recovery

Lateral from the multiplexer fixes. When a resumed inventory's declared
capability and its stream disagree, the engine recovers through the
account-wide schema reset, the blast radius the bad-checkpoint fix removed for
fresh checkpoints. Scope it to the cursor.

