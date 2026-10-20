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

## SHARED-007 - Two idempotency authorities

Scope: types adoption. Weaker finding.

`AccountOperation::is_idempotent()` is documented as "the authoritative
idempotency source `derive` consults". net's replay layer decides on
`reqwest::Method::is_idempotent()` (`request.rs`, `replayable =
idempotent.unwrap_or_else(|| method.is_idempotent())`), and dav-core's
`wire_outcome` uses the same HTTP-method rule to decide whether a terminal
status may be used as data. They routinely disagree: a DAV `PUT` for
`EventCreate` / `ContactCreate` is replayed by net while `derive` calls the
operation non-idempotent; a Graph `PATCH` for an idempotent `*Update` is never
replayed while `derive` says `Retry(SameRequest)`. dav-core and carddav patch
around it with `idempotency_override(false)` (see `partial_move_error`'s
comment about the override being lost on rebuild). Feeding the operation's
idempotency into the net request removes the second authority. DAV-006 sits on
top of this.

## SHARED-008 - Provider-specific throttle policy lives in bifrost-net

Scope: types adoption. Weaker finding.

`net::account_error::throttle_scope` hard-codes "Graph / Microsoft / Gmail /
Fastmail-JMAP get `ThrottleScope::Account`, everyone else none". Any other JMAP
server's 429 carries no throttle scope, so the engine never lifts it to a
`ThrottleKey::Account` and sibling scopes keep hammering. It is a provider rule
in the transport crate keyed on `Provider` values; the protocol crate should
say it.

## SHARED-009 - JMAP WebSocket and ManageSieve traffic is capped but never metered; IMAP and SMTP each built their own meter

Scope: net adoption.

- Claim broken: `reference/net.md` "IMAP/SMTP report bytes-in/out through
  `MeterSink` for unified bandwidth accounting", and the meter's
  `observed_bps()` being the account's figure.
- `MeteredStream` admits bytes against the budget, but "No observation sink is
  attached by this adapter". Its two users, `crates/jmap/src/client_ws.rs`
  (RFC 8887 WebSocket, push and any API calls over WS) and
  `crates/imap/src/account/sieve.rs` (ManageSieve), never record into the
  `BandwidthMeter`. IMAP and SMTP each built a private `WireMetering`
  (`crates/imap/src/connection/wire.rs`,
  `crates/smtp/src/transport/smtp/client/metering.rs`) to get both halves,
  re-implementing budget admission plus sink recording around
  `bifrost_net::Reservation`.
- Fix in net: let `MeteredStream` take an optional `MeterSinkHandle` (or an
  `AccountMeter`) and record actual bytes per finished operation; the two
  `WireMetering` copies can then collapse onto it.

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

## SHARED-011 - Body-excerpt capping is copied three times, two without net's truncation marker

Scope: net adoption. Proposal.

Graph `body_diagnostic` (`graph_error.rs`) re-implements `cap_status_body`
inline; Google's `body_excerpt` (`crates/google/src/error.rs`,
`MAX_BODY_EXCERPT_BYTES`) truncates without the marker net's doc promises
("Callers ... can detect truncation"); JMAP's `open_sse` passthrough drain
(`PASSTHROUGH_BODY_CAP`) has no marker and silently stops on a read error where
net emits a read-failure marker. All three should call `cap_status_body` or a
shared capped drain.

## SHARED-006 - Net OAuth refresh and the DAV status ladder still use the duration projection of Retry-After

Residue: `bifrost_net::request::parse_retry_after_hint` returns a `RetryHint`
that keeps the HTTP-date syntax, and graph, jmap and google use it. Net OAuth
refresh failure still goes through the duration-only `parse_retry_after`, which
loses an elapsed date, and so does dav-core's status ladder. `refresh_failed` in
`crates/net/src/account_error.rs` also drops its own `retry_after` argument when
the nested cause carries no hint. Once no caller is left, the duration form can
be retired (a removal, so it needs the owner).

## SHARED-013 - Push health after the directive fix

Residue of the push-reader fix (every reader now forwards non-Retry
classifications as `Terminated`, and IMAP classifies and parks):
- IMAP health edges are still per worker, so `Reconnected` from worker B does
  not mean the account is healthy while worker A is down.
- Graph and Google push health streams are broadcast-only and can lose a
  terminal event for a late or lagged subscriber, the shape JMAP closed with a
  latch.

## SHARED-003 - JMAP and Google factories still take no injected `Net`

Residue: both shared Nets now share one bandwidth registry, meter, governor and
hold set per process, and Google has an internal constructor taking a Net. The
public factory parameter is still missing for both providers: Google needs
`account/mod.rs` and JMAP needs `transport_reqwest.rs`, so a consumer cannot
wire its own registry into those accounts.

## SHARED-012 - Google lifecycle still pre-filters failures locally

Residue: JMAP now emits `Terminated` for read and snapshot-repair failures and
the engine reconnects with a 30 to 480 s backoff that honours retry hints and
records throttle holds. Google's `scope_lifecycle_stream` still filters locally,
because an engine reconnect would drop its stream-local diff baseline and
silently lose label changes. Enabling it needs the baseline to live in
account-owned state (`google/account/mod.rs`). JMAP's successful expired-history
snapshot repair also stays local, since engine scope recovery cannot repair its
private lifecycle position.

