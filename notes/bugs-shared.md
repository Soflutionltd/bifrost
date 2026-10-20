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
filed in that provider's document (JMAP-002, JMAP-004, JMAP-027,
DAV-003 to DAV-005, DAV-012, DAV-013, GOOG-005, GRAPH-011, JMAP-011, SYNC-008).
What remains here spans crates or roots in a shared crate. Entries set aside as
low value or awaiting a ruling are in `notes/bugs-deferred.md`.

## SHARED-010 - Unsupported-error constructors duplicated in about a dozen places, stamping inconsistently

Scope: types adoption. Weaker finding.

`unsupported_error` / `unsupported` exist in jmap (four), imap, dav-core (plus
caldav/carddav wrappers), graph (four), google (via its local error), smtp, and
privately in `types/src/account.rs`. They agree on kind and cause (the funnel
forces it) but not on stamping: Graph sets `provider`, others do not; some set
scope, some never can. The types crate's own default-method errors
(`unsupported_error`, `unsupported_inventory_stream`) carry no protocol, so a
provider leaning on a default emits a protocol-less error while its overrides
stamp one. A public `AccountError::unsupported(op, protocol)` unifies them.

## SHARED-007 - Replay safety needs request-level operation context

Needs a design ruling. The two idempotency authorities (the
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

Residue: JMAP WebSocket and ManageSieve now record into the account meter
through `MeteredStream`. IMAP's `WireMetering` defers accounting across COMPRESS
and carries peer-timeout state, and SMTP's also serves blocking I/O with
runtime-independent admission, so they were kept with comments; collapsing them
onto the shared stream needs those two behaviours to move into it.

## SHARED-008 - Net keeps its provider throttle inference, and SMTP still depends on it

Residue: the Graph, Google, JMAP and DAV funnels now pass explicit throttle
policies through `into_account_error_with_throttle_scope`. Net's hard-coded
Graph, Microsoft, Gmail and Fastmail-JMAP inference stays because SMTP still
calls the compatibility function and downstream consumers may rely on the
published behaviour. Migrate SMTP, then removing the inference (and retiring the
duration-only `parse_retry_after`, which has no in-tree production caller left)
is a removal that needs the owner.

