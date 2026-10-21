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
shared crate. Entries needing a specification or set aside as low value are in
`notes/bugs-deferred.md`.

## SHARED-007 - Replay safety needs request-level operation context

**Needs a technical implementation specification** (owner, 2026-10-21).
Direction agreed: every request declares its replay policy on net's request
builder (`Idempotent`, `Never`, `IdempotentUnlessConditional`), set by each
provider funnel where the operation and leg are known; the HTTP-method rule
becomes only the default; conditional requests (`If-Match`, `ifInState`) are not
auto-replayed, and an ambiguous loss surfaces as `Reconcile` for read-back.

The two idempotency authorities (the
`AccountOperation` rule that `derive` consults and the HTTP-method rule that net
replay and dav-core `wire_outcome` use) were checked and the universal
replacement was refused: the DAV overrides are needed because `EventUpdate` and
`ContactUpdate` also cover MOVE, the create-PUT fallback and partly completed
sequences; Graph and Google wire funnels carry no operation context; and a JMAP
POST can be a mixed batch. Reasoning is at the code sites and in the references.
Removing the second authority needs request-level context handed to net per
request (operation, plus whether this request is a replayable leg). Related
lateral: replaying a DAV conditional `If-Match` PUT whose first response was lost
can turn a committed write into a 412 conflict.

## SHARED-009 - The IMAP and SMTP wire meters stay private

**Needs a technical implementation specification** (owner, 2026-10-21): too
consequential to resolve by ruling.

Residue: JMAP WebSocket and ManageSieve now record into the account meter
through `MeteredStream`. IMAP's `WireMetering` defers accounting across COMPRESS
and carries peer-timeout state, and SMTP's also serves blocking I/O with
runtime-independent admission, so they were kept with comments; collapsing them
onto the shared stream needs those two behaviours to move into it.

## SHARED-019 - Proxy TLS failures and CONNECT refusals are unclassified

Lateral from the upgrade-routing work. Net now tunnels upgrade connections
through the account proxy with HTTP CONNECT. A TLS failure to the proxy, a 407
proxy-auth refusal or another non-2xx CONNECT answer all surface as a generic
handshake I/O error, so the account cannot tell a proxy credential problem from
a network drop. Map them to distinct TLS, proxy-auth and refused
classifications.

## SHARED-020 - The new `ProxyConfig` has no constructor or example

Lateral from the upgrade-routing work. `bifrost_net::ProxyConfig` is
`#[non_exhaustive]`, so downstream code must start from `default()` and mutate
fields, with no constructor or doc example showing how. `connect_tunnel` is also
newly public and takes `&str`, accepting http and https destinations as well as
WebSocket ones; its doc should say what it is for. System proxy selection can
also return a SOCKS proxy (ALL_PROXY), which reqwest cannot use without its
`socks` feature; upgrades fail closed on it, and the HTTP gap predates this
change.

## SHARED-017 - `MutationConcurrency::StateBased` has no producer left

**Awaiting the owner** (2026-10-21): keep or remove is the owner's decision,
to be made later. Not for a fixer.

Lateral from the caller-version work. JMAP, CalDAV, CardDAV and Graph moved off
`MutationConcurrency::StateBased`, so no provider in the workspace advertises it.
Removing a published variant needs the owner. The `IdempotencyKey` doc in
`crates/types/src/mutation.rs` still contrasts itself with `StateBased` as
"optimistic concurrency on the wire".


