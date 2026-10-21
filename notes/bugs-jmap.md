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

## JMAP-038 - An omitted `collationAlgorithms` is indistinguishable from an empty one

Residue: `validate_session` now refuses an absent `maxSizeUpload`,
`maxConcurrentUpload` or `maxConcurrentRequests` (zero stays valid). RFC 8620
section 2 also requires `collationAlgorithms`, but `CoreCapabilities` in
`crates/jmap/src/core/session.rs` parses it as a defaulted `Vec<String>`, so an
omitted field and a valid empty array look the same. The model must preserve
presence before the validator can refuse omission. A fixer needs
`core/session.rs` and `sync/capabilities.rs` together. When it lands,
`reference/jmap.md` should state that account open requires the RFC 8620 core
members (`maxSizeUpload`, `maxConcurrentUpload`, `maxSizeRequest`,
`maxConcurrentRequests`, `maxCallsInRequest`, `maxObjectsInGet`,
`maxObjectsInSet`, `collationAlgorithms`), refuses a missing one as
`Protocol(ContractViolation)`, and accepts zero concurrency and an empty
`collationAlgorithms`.
