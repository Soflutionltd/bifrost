# bifrost-imap defect findings

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

Filed from the October 2026 defect hunt: the imap protocol and imap account
scopes.

## IMAP-022 - The MODSEQ cache records server state the consumer has not seen, so `UNCHANGEDSINCE` guards less than documented

Scope: imap account.

**Ruled (owner, 2026-10-21):** resolved by the caller-version optimistic
concurrency work filed under GRAPH-006: the guard value comes from the caller's
`expected_version` (`ServerVersion::ModSeq`), not the cache.

`push.rs` `absorb_idle_event` (records from IDLE FETCH), `mutate.rs`
`open_folder_for_mutation` (from the mutation's own SELECT QRESYNC
`changed_messages`), and inventory and changes recording as they stream.
`UNCHANGEDSINCE` exists to reject a write based on a stale view, but the cache
advances from any server read, including the SELECT immediately before the
STORE and push events not yet reconciled. A concurrent change by another
client after the consumer's last hydration is normally folded into the guard
value just before the STORE, which then succeeds over it. Capabilities
honestly advertise `MutationConcurrency::None`, so no promise is broken, but
`reference/imap.md` "Concurrency conflicts and UNCHANGEDSINCE" describes a
guard that, as wired, protects only the sub-second SELECT-to-STORE window.
Take the guard value from the consumer (`ServerVersion::ModSeq` already travels
in inventory entries), or say plainly how narrow the window is.

## IMAP-018 - IDLE sessions are still uncounted against `pool_cap`

**Needs a technical implementation specification** (owner, 2026-10-21): too
consequential to resolve by ruling.

Scope: imap account. Residue: the config docs and reference now state the real
total (`pool_cap - 1 + idle_connection_budget`). Whether IDLE workers should
instead draw from a shared session budget, so the footprint stays inside
`pool_cap` on providers with a low per-user connection limit, is a product
decision.

## IMAP-004 - Copy-free literals

Residue of the per-command literal ceiling (the reader checks cumulative
literal declarations against the budget before reading payload, a breach is a
fatal `Parse`, and raw-message streaming passes its budget to the budgeted
stream). Still open:
- FETCH response fields in `crates/types` (`fetch.rs`) own `Vec<u8>`, so the
  literal is still copied and nothing is emitted incrementally; splitting the
  `BytesMut` alone cannot change that.
- Non-literal syntax and parser overhead are outside the literal ceiling.

