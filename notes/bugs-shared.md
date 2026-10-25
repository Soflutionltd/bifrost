# Cross-crate adoption findings (net, types, sync)

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

Filed from the October 2026 hunt's three adoption scopes: where a provider
crate hand-rolls, bypasses or reimplements something `bifrost-net`,
`bifrost-types` or `bifrost-sync` owns, and the copy disagrees with the shared
one or escapes its guarantee. Adoption findings confined to one provider were
filed in that provider's document. What remains here spans crates or roots in a
shared crate.

## SHARED-019 - A proxy 407 maps to `AuthLost`, implying the wrong remedy

Residue: JMAP now classifies net's typed `TunnelError` (407 ->
`Authentication(ReauthorizationRequired)` / `AuthLost`, proxy TLS ->
`Transport(Tls)`, other refusals by status). The error model has no
proxy-credential kind, so a 407 asks the consumer to reauthorize the mail
account when it is the proxy's credentials that are wrong; only support detail
says so. Decide whether the taxonomy gains a proxy-credential kind or a
different existing kind fits better.

## SHARED-017 - `MutationConcurrency::StateBased` has no producer left

**Awaiting the owner** (2026-10-21): keep or remove is the owner's decision,
to be made later. Not for a fixer.

Lateral from the caller-version work. JMAP, CalDAV, CardDAV and Graph moved off
`MutationConcurrency::StateBased`, so no provider in the workspace advertises it.
Removing a published variant needs the owner. The `IdempotencyKey` doc in
`crates/types/src/mutation.rs` still contrasts itself with `StateBased` as
"optimistic concurrency on the wire".


