# Bugs - design hunt, November 2026

Defects the design-hunt hunters turned up on the way. They were not what the
hunt asked for; they are the findings a resolution loop can land first. Each
entry is the hunters' account, unverified; the fix pass verifies and may find
an entry wrong.

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

Each entry names the hunt scope(s) that reported it. The raw reports are in the
commit that precedes this file's.

## BUG-055 - IMAP `SearchFilter::Has` (attachment filename or type) compiles to a full-text `BODY` search

**Reported by:** imap-account.

It returns messages that merely mention the string. Refusing the filter as
`Unsupported` would take away a search path that works today; a correct fix
needs attachment-aware evaluation that composes with boolean filters and
position paging (for example BODYSTRUCTURE-based post-filtering of the
candidate set). The defect is now documented at the `pim.rs` arm and in
`reference/imap.md`.

## BUG-176 - Open rulings and design residue from the wave 10 to 16 lateral findings

**Reported by:** wave 10 to 16 fixers (lateral); everything fixable in those
bundles landed by wave 16. What remains needs a ruling or a design.

Rulings:

- ManageSieve errors are stamped `Protocol::Imap` because the shared
  `Protocol` enum has no ManageSieve variant (documented at
  `imap/src/account/error.rs::oauth_token_error`). Decide whether ManageSieve
  gets its own protocol identity.
- IMAP `native_code` for an unknown response code is the server's own
  spelling, giving that telemetry field unbounded cardinality. Decide whether
  to bound it (for example a fixed `OTHER` with the spelling in diagnostics).
- Closed-account guards are not universal: the shared `AccountClosed` kind
  replaced existing refusals, but many provider methods have no lifecycle gate.
  Decide whether every method gains one.
- Gmail: a malformed cursor payload maps to `SchemaIncompatible` (account
  clear) through `CursorDecodeFault::Malformed`, where it used to be
  `CursorInvalid` with a scope restart. Confirm.
- Sync acknowledgement tokens: a wrong or rotated key and a corrupted token
  decode to the same error; a key-identifier byte in the header would separate
  them.

Design:

- Sync `PendingCoverage::supersede` (standalone public form) cannot update a
  token a consumer already saved, and `AckToken` receipts hold the full
  cumulative claim; bounding either needs a new durable evidence owner or an
  acknowledgement contract (documented in `cursor/coverage.rs`).
- CalDAV and CardDAV unconditional deletes: the client absorbs 404 and 410
  under its replay contract before account code sees them, so the shared
  absent-target policy cannot be applied there without moving that decision
  (documented in both `account.rs` files).
- CalDAV and CardDAV updates sent without a strong validator can recreate a
  resource deleted after it was fetched.
- IMAP `account/inventory.rs::run_inventory` takes a `scope` parameter it does
  not use (renamed `_scope` to pass the gate); wire it or drop it once its
  intended role is known.
- Sync `engine/reattach.rs` closes the replaced account (`close_owed`) without
  a time bound, and `retire_stranded` awaits provider cleanup without one;
  bounding either means deciding when to abandon an obligation cancellation
  hands over (attach and detach use `engine::close_within`).

## BUG-048 - IMAP sync lanes cycle on a `UIDNOTSTICKY` folder

**Reported by:** imap-account; reopened by the wave 1 review.

Inventory and changes on a `UIDNOTSTICKY` folder mint cursors the next SELECT
voids, so the scope cycles CursorInvalid -> re-inventory. Refusing the folder
(as the poller does) was tried and reverted: it would make the folder's mail
vanish from the user's view, which removes a capability. The remaining fix is a
cursor-free sync mode for such folders (re-inventory on each poll, no durable
UID cursor, no CursorInvalid loop); the inventory site and `reference/imap.md`
say so. It cannot be built inside IMAP alone: it needs a shared
`CursorEstablishment` variant in bifrost-types and engine scheduling of
repeated authoritative inventory (today `NoCursor` schedules no further poll),
plus safe hydration and mutation semantics for UID handles that do not survive
a re-SELECT. A wave of one owning types, sync and imap.

