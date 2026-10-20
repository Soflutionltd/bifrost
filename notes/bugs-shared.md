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

## SHARED-001 - `attach_account` wraps every token source in an `OAuthRefresher`, while providers tell consumers to pass one: refreshers nest by design

Scope: net adoption.

- Claim broken: `reference/net.md` "Do not nest `OAuthRefresher`s. Wrapping one
  refresher around another causes the outer's `force_refresh` to bypass the
  inner's single-flight", repeated in `crates/net/src/auth.rs` above
  `impl TokenSource for OAuthRefresher`.
- `Net::attach_account` (`crates/net/src/net.rs`, the `token_source` arm)
  unconditionally does
  `Arc::new(OAuthRefresher::new(source).with_max_age(..))`.
- Provider entry points document the consumer's source as "typically an
  `OAuthRefresher` over its own refresh-token store":
  `crates/jmap/src/sync/factory.rs` (`JmapCredentials::Bearer`),
  `crates/graph/src/client.rs` (`with_source`),
  `crates/google/src/account/mod.rs`; SMTP's docs say the same (`transport.rs`,
  "an already-fresh `OAuthRefresher`").
- Consequence: the outer refresher's proactive window (60 s before expiry, or
  `token_max_age` = 55 min for opaque tokens) calls `inner.refresh()`, i.e.
  `force_refresh()`, an unconditional issuer round trip ignoring the inner's
  fresh cache. Each HTTP attachment (JMAP, Graph and Google each attach their
  own `AccountNet`, and reopen attaches again) is a separate outer refresher, so
  one consumer refresher is force-refreshed once per attachment per window, and
  the outer's error backoff stacks on the inner's. The 401 path is fine
  (`refresh_if_unchanged` is coordinated); the proactive path is not.
- Fix in net: do not wrap a source that already coordinates (a marker method on
  `TokenSource`, e.g. `fn is_coordinating(&self) -> bool`), or have the outer
  proactive path call `current()` on the source first and `refresh()` only when
  it returns the same stale token.

## SHARED-002 - The token lifecycle is split: HTTP requests use net's refresher, other wire paths read the raw source

Scope: net adoption.

- Claim broken: `reference/net.md`'s OAuth single-flight section (proactive
  refresh, max-age fallback, 401 one-shot forced refresh, error backoff) is
  presented as THE token lifecycle; JMAP's `Authorization::header_value` doc
  calls the bearer read "live".
- The refreshed token lives in the attach-time `OAuthRefresher`. Paths calling
  `token_source.current()` on the consumer's raw source instead:
  - JMAP WebSocket handshake: `crates/jmap/src/client.rs`
    `Authorization::header_value`, used by `client_ws.rs` `connect_ws`
    (JMAP-027). SSE and Basic use the helper only for non-bearer, so they are
    fine.
  - DAV: `crates/dav-core/src/dispatch.rs` `auth_headers`; DAV attaches with
    `AccountSpec::new(None)`, so net holds no refresher for it (DAV-003).
- With a plain `TokenSource` (the trait says `current()` returns the cached
  token and `refresh()` is driven "by `OAuthRefresher`"), these paths NEVER see
  a refreshed token: JMAP HTTP requests succeed with the refresher's new token
  while the WS upgrade presents the expired one. With a refresher, SHARED-001
  applies. Neither shape gives one coherent lifecycle.
- Structural fix: net owns one coordinating token handle per account and
  exposes it (`AccountNet::bearer()` or similar, returning the refresher's
  `current()`), and WS / DAV / IMAP / SMTP read through it.

## SHARED-003 - Bandwidth registries cannot be unified for JMAP accounts that accept invalid certs, and JMAP and Google take no injected `Net`

Scope: net adoption.

- Claim broken: `reference/net.md` bandwidth section: "The consumer must wire
  ONE registry into every Net, the engine and the IMAP factory. Matching ids in
  different registries do not coordinate."
- `Net::shared_default()` and `Net::shared_accepting_invalid_certs()`
  (`crates/net/src/net.rs`) each call `Net::new`, minting a fresh private
  `BandwidthRegistry`. JMAP attaches through `Net::shared_for_tls` with no way
  to pass a Net (`ReqwestTransport::new`); Google always uses
  `Net::shared_default` (`crates/google/src/client.rs`). The only way to satisfy
  the rule is to pass `Net::shared_default().bandwidth_registry()` to the
  engine (undocumented), and even that leaves every JMAP account with
  `accept_invalid_certs = true` on a second registry the engine's per-account
  budget (`crates/sync/src/engine/attach.rs`) never reaches. IMAP, CalDAV and
  CardDAV accept `with_net`; JMAP and Google do not.
- Fix: the two shared Nets share one process registry (and one hold registry
  and governor, for the same reason), and JMAP / Google factories accept a
  `Net` like the others.

## SHARED-004 - Three copies of the address-list parser, the IMAP one wrong on escaped quotes

Scope: types adoption.

The types crate renders display names through `format_address` /
`encode_phrase` (`crates/types/src/mime/render.rs`), quoting an ASCII name with
specials and backslash-escaping embedded `"` and `\`: a name `a", b` renders as
`"a\", b" <x@y>`.

- IMAP (`crates/imap/src/account/pim.rs`, `parse_draft_for_submission` ->
  `addresses_for` / `first_address` -> `parse_address_list` /
  `split_address_list`) toggles `in_quotes` on every `"` with no quoted-pair
  handling, so `\"` closes the quoted string and the following comma splits the
  item into a bare `"a\"` (no angle brackets, taken verbatim as an address) and
  ` b" <x@y>`. The bare one becomes an SMTP `RCPT TO` in the submission
  envelope. The name half is not unescaped (`\"` stays in `Address.name`) and
  RFC 2047 encoded-words are not decoded. Claim broken: the doc on
  `parse_address_list` itself, which says the splitter exists to stop a draft
  this crate emitted (quoting names like `"Last, First" <a@b>`) from
  round-tripping into garbage recipients; it prevents only the unescaped case.
- Google (`crates/google/src/account/pim.rs`, `parse_address_list` /
  `parse_address` / `split_address_list`, on the `draft_update` write path)
  says it is "mirroring the imap crate's", but it grew quoted-pair handling and
  name unescaping IMAP lacks, so Google is right on the input above and IMAP is
  not. Neither decodes encoded-words.
- The types crate already has the correct parser: `mime::header`'s
  `parse_addresses` / `split_address_items` handles quoted-pairs, groups,
  nested angles and encoded-words, reachable through
  `parse_message(raw).headers.addresses(name)` (IMAP already imports
  `parse_message` in the same file). It is private as a free function, which is
  probably why the copies exist.

Structural fix: make `parse_addresses` (or an `Address::parse_list`) public in
bifrost-types and delete both provider copies, along with IMAP's
`unfold_headers` / `header_value` / message-id trimming that duplicate
`HeaderMap`.

## SHARED-005 - Appending causes reclassifies an `AccountError`, and Graph's push merge loses or changes evidence because of it

Scope: types adoption (provider finding plus its root in the types crate).

Root, in bifrost-types: `reference/error-model.md` calls `into_builder` "the
decoration path, not reclassification" and says `push_cause` "only appends
secondary evidence". But `recovery::derive` and its helpers read the WHOLE
chain with `find_map` / `any` (`transmission_state`, `server_retry_hint`,
`server_status`, `access_needed`, `access_resource`, `strategy_downgrade`,
`operator_reason`, `smtp_reply`). An appended `Attempt(InFlight)` turns a
primary-without-attempt `Transport` retry into a reconcile; a sibling `Server`
hint becomes the retry hint; a `Wire(Smtp)` anywhere flips a 5xx to
`ProviderRefused`. `TransportAcknowledged` scans the whole chain, so appending
acknowledged evidence to a transport error fails the build. Every chain-merging
producer inherits this: Graph push merge, net `AttemptHistory` / `LocalHold`,
dav-core `response_read_error_with_history`, carddav/caldav
`partial_move_error` and `directory_sync_error`. Either derive reads only the
primary plus causes the producer marks as its own, or the doc says appended
causes are classification inputs.

Provider instance: `crates/graph/src/account/push/common.rs`,
`no_subscribable_push_scopes`, takes the first failed scope's error,
`into_builder()`, `push_cause`s every cause of every other failed scope, and
ends with `try_build().unwrap_or_else(|_| first.error.clone())`.
- If the first failure is `Transport(_)` (the doc names "a transport failure on
  the translation POST" as one population) and any sibling failed with an
  acknowledged response (the `Protocol(ContractViolation)` from
  `translateExchangeIds`, or any status-mapped error, all carrying
  `Attempt(Acknowledged)`), `try_build` rejects with `TransportAcknowledged` and
  the fallback returns the first error alone, dropping the per-scope diagnosis
  the doc says "is not lost with the ledger". It is the one production
  `try_build` in the workspace without `.expect(...)`, hiding exactly the
  producer bug the funnel exists to surface.
- When it builds, the merged chain changes derived recovery: a primary with no
  `Attempt` adopts a sibling's `InFlight`; a primary
  `RateLimited { retry_hint: None }` adopts a sibling's hint. The doc's "the
  first failure is promoted verbatim - it is already classified" is false
  whenever the primary lacks one of those causes.

## SHARED-006 - Retry-After: four parsers, three answers for an HTTP-date

Scopes: types adoption and net adoption.

- `bifrost_net::parse_retry_after` (`crates/net/src/request.rs`) resolves an
  HTTP-date against `SystemTime::now()` at parse time and returns
  `Option<Duration>`; an elapsed date returns `None` (`duration_since` errs), so
  the hint vanishes.
- Graph's `parse_retry_after_header` (`graph_error.rs`) delegates and wraps in
  `RetryHint::After`. Its doc says elapsed dates "collapse to `Duration::ZERO`,
  which is the engine's signal to retry immediately": false, they become
  `None`.
- JMAP (`sync/error.rs`, `retry_after_header`) delegates likewise.
- Google (`crates/google/src/error.rs`) keeps its own `parse_retry_after_value`
  that retains the syntax (`Delta` / `At`) and emits `RetryHint::At`, so an
  elapsed date stays a past deadline (min delay zero).

`RetryHint::At(SystemTime)` exists in the types crate for the HTTP-date form,
and only Google uses it. The collapse to `After` also re-anchors the hint to
when the consumer reads it rather than the instant the server named. The shared
parser should return a `RetryHint` (or a structured form `parse_retry_after`
projects from), and Google's copy should go. DAV does not read Retry-After at
all (DAV-004).

The net doc on `parse_retry_after` is also stale against the types crate: it
names "the `retry_after` field on `ServerCause::{...}` and the `not_before`
field on `RetryAdvice`". Neither exists; both are `retry_hint:
Option<RetryHint>` (the error model calls the split `not_before` field the
side-channel bug the convergence rewrite removed).

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

## SHARED-012 - Google and JMAP lifecycle streams reimplement the engine's lifecycle termination policy, and disagree with it

Scope: sync adoption. Hunter severity P2.

`crates/google/src/account/scopes.rs` (`scope_lifecycle_stream`, the `Err`
arm) and `crates/jmap/src/sync/discover.rs` (both `Err` arms) triage locally:
`is_terminal() || requires_engine_action()` yields `Terminated`, everything else
is swallowed into a local sleep (Google doubling to 480 s, JMAP fixed 300 s).
The engine owns this decision: `lifecycle_termination` in
`crates/sync/src/multiplexer/mod.rs` routes through `plan_recovery` and maps
`Retry` / `Reconcile` to `LifecycleTermination::Reconnect`. The
`ScopeLifecycleEvent::Terminated` doc in `crates/types/src/account.rs` says the
carrier exists so the protocol no longer has to "sleep-and-retry silently".
Because both providers pre-filter, the engine's `Reconnect` arm is unreachable
for them, and:
- `Retry { AfterAuthRefresh }` is swallowed; nothing in the lifecycle loop
  refreshes auth, so an expired token stalls lifecycle until another path
  happens to refresh it;
- `Reconcile(_)` classes are swallowed as transient;
- a rate limit's retry hint is ignored and the engine's throttle registry never
  sees the error, so no hold is recorded for sibling work and the poll can fire
  inside a hold the engine enforces elsewhere;
- three backoff policies (Google exponential, JMAP flat, engine reconnect
  delay) govern the same failure class.

Claims broken: the `scope_lifecycle_stream` trait doc; `reference/sync.md`
("the engine dispatches every termination through `plan_recovery`"). Fix:
providers emit `Terminated` for every classified failure and let the engine
reconnect. The hunter warns the hole may move: the engine's lifecycle reconnect
delay starts at 50 ms, so check that it grows and honours retry hints before
flipping, and fix it there if it is too aggressive for a long-period poll.
SYNC-002 (the lifecycle reader never revived after termination) and JMAP-011
(JMAP's terminal `cannotCalculateChanges`) sit on the same path.

## SHARED-013 - Push readers swallow engine directives, and IMAP never reports a push failure at all

Scopes: sync adoption and imap account (both reported the IMAP part).

The push reconciler (`crates/sync/src/push/reconciler.rs`,
`WatchEvent::Terminated` arm) routes `RecoveryPlan::Engine(directive)` to the
reopen channel, but providers mostly never let a directive reach it:
- JMAP `reader_pass` (`crates/jmap/src/sync/push.rs`): handshake and read-loop
  errors check only `is_terminal()`, so an `Engine(_)` class (capability
  changed, session changed, restart-account) is a transient reconnect retried
  forever.
- Graph EWS (`crates/graph/src/account/ews_stream.rs`) and the Google renewer
  (`crates/google/src/account/push.rs`, renewal failure arm) use the same
  `is_terminal()`-only test.
- IMAP (`crates/imap/src/account/push.rs`, `idle_loop`): `dial_idle` and
  `select` errors are discarded with `Err(_)`, unclassified, so `NONEXISTENT` or
  `NOPERM` cannot be told from a network failure, and the worker redials on
  backoff. IMAP emits no `WatchEvent::Terminated` anywhere. A subscribed folder
  that was deleted, renamed or ACL-revoked makes its worker emit
  `WatchEvent::Disconnected` (which bifrost-sync turns into the account-scoped
  "push transport disconnected" warning), then dial, authenticate and SELECT
  again every 5 to 300 s for the account's life; each retry is a full connect,
  which providers with connection-rate limits punish. A later successful round
  emits `Reconnected`, which the engine turns into an account-wide
  `HintPayload::Unknown` reconcile. Because `Disconnected` and `Reconnected`
  are account-level signals multiplexed from per-worker state, worker B can
  emit `Reconnected` while worker A is still down. A deleted folder chosen by
  `choose_idle_folder` is re-selected forever. Auth loss and capability breaks
  take the same endless loop.

Claims broken: the reconciler's documented directive routing;
`reference/error-model.md`'s rule that recovery classes are acted on
centrally; for IMAP, the push section's premise that `Disconnected` means the
transport went away. Fix: providers forward any non-`Retry` classification as
`Terminated` (IMAP must classify at all); transient retry can stay local, only
for `Retry`.

## SHARED-015 - `PushSubscription::new` does not enforce its own invariant

Scope: types adoption (lateral in bifrost-types).

The type doc says "the handle is absent when no requested scope was accepted",
and the error model says the handle covers exactly the succeeded lane.
`new(handle, outcomes)` accepts `Some(handle)` with an empty succeeded lane and
`None` with a non-empty one. Graph had to add `no_subscribable_push_scopes`
because an arm produced the handle-less all-failed shape; the constructor could
reject it.

## SHARED-017 - IMAP's `scope_lifecycle_stream` comment is false

Scope: sync adoption (lateral).

`crates/imap/src/account/mod.rs` says `scope_lifecycle_stream` "streams folder
lifecycle events from push state". The implementation in
`crates/imap/src/account/scopes.rs` never yields and ends at shutdown, and
`reference/imap.md` says IMAP emits no scope-lifecycle events.
