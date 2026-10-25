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

## SYNC-021 - `SyncEngine` exposes almost no calendar primitives

Lateral from the passthrough work. `SyncEngine` forwards contact primitives,
but of the calendar `Account` methods only `event_delete` and
`event_delete_if_version` (`crates/sync/src/engine/passthrough.rs`). List,
range, get, create, update, RSVP and search have no passthrough, and the new
`event_rsvp_if_version` needs one too:
`pub async fn event_rsvp_if_version(&self, account_id: &AccountId, event: EventId, status: RsvpStatus, expected_version: Option<ServerVersion>) -> Result<(), Error>`.
Whether the engine is meant to be the consumer's single door for calendar, as
it is for contacts, decides whether this is an omission.
