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
engine-side read-back finding from the sync adoption scope.

Coverage note from the engine hunter: `cursor/coverage.rs`, `cursor/ledger.rs`,
`cursor/ledger_envelope.rs`, `backfill/*`, `engine/lane.rs` and `repair.rs`
were NOT covered in depth. They are the largest files and need their own pass.

## SYNC-007 - A proven non-landing write is never resubmitted

**Needs a technical implementation specification** (owner, 2026-10-21): too
consequential to resolve by ruling.

Scope: sync engine. Residue: a read-back mismatch now stays pending instead of
becoming `failed_terminal`. Once read-back proves an `Uncertain` write did not
land, replay is the safe case the uncertainty lane protected, yet the pipeline
still never resubmits it. Replay was deferred because reconciliation can need
consumer-side dedupe; a ruling is needed on whether the engine replays.

## SYNC-009 - Push ingress can still lose or unboundedly queue control events

**Needs a technical implementation specification** (owner, 2026-10-21): too
consequential to resolve by ruling. Specify together with SHARED-014, which
reshapes how push reaches the engine.

Needs a ruling. The 100 ms coalesced-send timeout can drop an invalidation and
the per-registration ingress is still unbounded. A sticky needs-full-reconcile
flag keeps the invalidation intent but cannot carry warning payloads,
termination classifications or their ordering, so a bounded lossless ingress
needs a control-event overflow policy or an API change. Reasoning is at both
code sites and a test shows the semantic loss.

## SYNC-015 - `handle_schema_incompatible` still resets only registered scopes

**Needs a technical implementation specification** (owner, 2026-10-21): too
consequential to resolve by ruling; it changes the consumer-implemented
checkpoint store contract.

Durable completion markers for scopes not in the registry survive the reset. A
complete fix needs writer or store support for an account-wide reset or durable
enumeration; the limitation is documented in the reference.

## SYNC-019 - Docs miscount and under-list inventory removals

Laterals from the removal-filter work. The `BackfillPartitionOutcome` docs in
`crates/sync/src/backfill/runner.rs` describe `seen` as entries, but the runner
counts entries plus removals while `kept` counts entries only. The
`InventoryBatch` docs in `crates/types/src/events.rs` omit `FolderType` from
the scopes whose removals drop membership, although the projection and
`reference/sync.md` include it.

