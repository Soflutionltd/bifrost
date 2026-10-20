# Hunt: bifrost-types adoption

Scope: places where a provider (or the shared `bifrost-net` / `dav-core`
layers the providers sit on) hand-rolls a rule the types crate owns, or
bypasses a guarantee the types crate makes. Findings are ordered by
consequence. Each names the claim it breaks.

Not re-verified by running anything; every finding below is from reading
the code at the current tree.

---

## Defects

### 1. IMAP draft submission parses recipients with a private address parser that splits inside escaped quotes

`crates/imap/src/account/pim.rs`, `parse_draft_for_submission` ->
`addresses_for` / `first_address` -> `parse_address_list` /
`split_address_list`.

The types crate renders display names through `format_address` /
`encode_phrase` (`crates/types/src/mime/render.rs`), which quotes an ASCII
name carrying specials and backslash-escapes embedded `"` and `\`. A name
`a", b` therefore renders as `"a\", b" <x@y>`.

IMAP's private `split_address_list` toggles `in_quotes` on every `"` with no
quoted-pair handling, so the `\"` closes the quoted string and the following
comma splits the item. The result is two "recipients": a bare `"a\"` (no
angle brackets, taken verbatim as an address) and ` b" <x@y>`. The bare one
becomes an SMTP `RCPT TO` in the submission envelope. The name half also is
not unescaped (`\"` stays in `Address.name`) and RFC 2047 encoded-words are
not decoded.

The types crate already has the correct parser: `mime::header`'s
`parse_addresses` / `split_address_items` handles quoted-pairs, groups,
nested angles and encoded-words, reachable through
`parse_message(raw).headers.addresses(name)` (and IMAP already imports
`parse_message` in the same file). It is private as a free function, which
is probably why the copy exists.

Claim broken: the doc comment on `parse_address_list` itself ("a draft this
crate itself emitted (the shared assembler quotes names like
`"Last, First" <a@b>`) would round-trip into garbage recipients" is what the
splitter exists to prevent) - it prevents the unescaped case only.

### 2. Google carries a third, diverged copy of the same parser

`crates/google/src/account/pim.rs`, `parse_address_list` / `parse_address`
/ `split_address_list`, on the `draft_update` write path. Its doc says
"mirroring the imap crate's"; it no longer does - it grew quoted-pair
handling and name unescaping that IMAP lacks (so Google is correct on the
finding-1 input and IMAP is not). Neither decodes encoded-words; the types
copy does. Three parsers for one grammar, each with a different set of
cases right, and the one that is most complete is not callable.

Structural fix: make `parse_addresses` (or an `Address::parse_list`) public
in bifrost-types and delete both provider copies, as well as IMAP's
`unfold_headers` / `header_value` / message-id trimming that duplicate
`HeaderMap`.

### 3. DAV status classification drops `Retry-After` and throttle scope, and disagrees with net's ladder

`crates/dav-core/src/error.rs`, `status_error(operation, status, body,
protocol)`.

It takes no headers, so a 429 / 503 / 507 becomes
`ServerCause::{RateLimited, Unavailable, QuotaExhausted} { retry_hint: None }`
unconditionally, and never stamps a `throttle_scope`. The error model says
`ServerCause` "carries the optional `RetryHint` ... (the only structural
channel for retry timing)". The headers are in hand: `DavResponse` carries
them, and `send_status_request` / `settle_body` / the transport
`status_error` call site all have the response.

This bites hardest on exactly the throttled path: `wire_outcome` converts a
net `AttemptHistory` / `RateLimited` / `RetryBudgetExhausted` for a
replayable request BACK into an `Ok(DavResponse)` so the status can be used
as data. net's own `into_account_error` would have carried the parsed
`Retry-After` (including the retry loop's recorded hint); after the
round-trip through `DavResponse` it is gone, so the engine falls back to its
own backoff on a server that told it how long to wait.

The ladder itself is a second copy of net's `status_kind_cause`
(`crates/net/src/account_error.rs`) and the two disagree:

| status | net | dav-core |
|---|---|---|
| 400 / 422 | `Request(Malformed)` -> `ClientBug` | `Server(Error{400})` -> `ProviderRefused` |
| 408 / 502 / 504 | `Server(Unavailable)` (carries hint) | `Server(Error{5xx})` (no hint) |
| 412 / 423 | `Server(Error)` -> `ProviderRefused` | `ConcurrencyConflict` -> `Retry(AfterStateRefresh)` |
| 410 on a cursor scope | `SyncState(CursorInvalid)` | `Server(Error{410})` |

Some of the DAV choices are deliberate (412 as a conflict is right for
`If-Match`), but nothing records that the divergence is intentional, and the
retry-hint loss plainly is not. One HTTP status table with per-protocol
overrides would put the rule in one place.

### 4. Graph's push merge can silently discard all sibling evidence, and changes the primary's recovery when it does not

`crates/graph/src/account/push/common.rs`, `no_subscribable_push_scopes`.

It takes the first failed scope's error, `into_builder()`, then
`push_cause`s every cause of every other failed scope, and ends with
`try_build().unwrap_or_else(|_| first.error.clone())`.

- If the first failure is `Transport(_)` (the doc names "a transport
  failure on the translation POST" as one of the populations) and any
  sibling failed with an acknowledged response (the `Protocol(ContractViolation)`
  from `translateExchangeIds`, or any status-mapped error, all of which carry
  `Attempt(Acknowledged)`), `try_build` rejects with `TransportAcknowledged`
  and the fallback returns the first error alone - dropping the per-scope
  diagnosis the doc says "is not lost with the ledger". This is the one
  production `try_build` in the workspace that does not `.expect(...)`, and
  it hides exactly the producer bug the funnel exists to surface.
- When it does build, the merged chain changes derived recovery, because
  `derive` reads the whole chain, not just the primary:
  `transmission_state` (first `Attempt` anywhere), `server_retry_hint`
  (first `Server` cause with a hint anywhere), `server_status`,
  `access_needed`, `access_resource`, `smtp_reply`. A primary with no
  `Attempt` adopts a sibling scope's `InFlight`; a primary
  `RateLimited { retry_hint: None }` adopts a sibling's hint. The doc's
  promise "the first failure is promoted verbatim - it is already
  classified" is false whenever the primary lacks one of those causes.

See lateral L1: the root is in the types crate.

### 5. Gmail `push_subscribe` refuses a mixed scope list whole, not per scope

`crates/google/src/account/mod.rs` `push_subscribe` ->
`push::push_subscribe`: any non-`Account` scope in the list returns `Err`
(uncorrelated `Unsupported(PushSubscribe)`), and success always reports
`PushSubscription::all_succeeded`.

`reference/error-model.md`: "refused scopes carry their own
scope-correlated `AccountError` in the failed lane ... an `Err(_)` from
`push_subscribe` still means no scope was subscribed at all". For
`[Account, Folder(x)]` the Gmail watch could cover `Account`, and should
report `Folder(x)` on the failed lane; instead nothing is subscribed and the
caller cannot tell which scope was the problem. IMAP, JMAP and Graph all
produce per-scope lanes.

### 6. Retry-After: four parsers, three answers for an HTTP-date

- `bifrost_net::parse_retry_after` (`crates/net/src/request.rs`) resolves an
  HTTP-date against `SystemTime::now()` at parse time and returns
  `Option<Duration>`; an already-elapsed date returns `None` (because
  `duration_since` errs), i.e. the hint vanishes.
- Graph's `parse_retry_after_header` (`graph_error.rs`) delegates to it and
  wraps in `RetryHint::After`. Its doc says elapsed dates "collapse to
  `Duration::ZERO`, which is the engine's signal to retry immediately" -
  false; they become `None`.
- JMAP (`sync/error.rs`, `retry_after_header`) delegates likewise.
- Google (`crates/google/src/error.rs`) has its own `parse_retry_after_value`
  that keeps the syntax (`Delta` / `At`) and emits `RetryHint::At`, so an
  elapsed date stays a past deadline (min_delay zero).

`RetryHint::At(SystemTime)` exists in the types crate precisely for the
HTTP-date form ("`At(SystemTime)` (HTTP-date `Retry-After`)"), and only
Google uses it. net's and Graph's collapse to `After` also means the hint is
re-anchored to whenever the consumer reads it rather than the instant the
server named. The shared parser should return a `RetryHint` and Google's
copy should go.

The net doc comment on `parse_retry_after` is also stale against the types
crate: it names "the `retry_after` field on `ServerCause::{...}` and the
`not_before` field on `RetryAdvice`". Neither field exists; both are
`retry_hint: Option<RetryHint>` (the error model calls the split
`not_before` field the side-channel bug the convergence rewrite removed).

---

## Weaker findings / smells

### 7. Two idempotency authorities

`AccountOperation::is_idempotent()` is documented as "the authoritative
idempotency source `derive` consults". net's replay layer decides on
`reqwest::Method::is_idempotent()` (`request.rs`, `replayable =
idempotent.unwrap_or_else(|| method.is_idempotent())`), and dav-core's
`wire_outcome` uses the same HTTP-method rule to decide whether a terminal
status may be used as data. The two disagree routinely: a DAV `PUT` for
`EventCreate` / `ContactCreate` is replayed by net while `derive` calls the
operation non-idempotent; a Graph `PATCH` for an idempotent `*Update` is
never replayed by net while `derive` says `Retry(SameRequest)`. dav-core and
carddav already have to patch around it with `idempotency_override(false)`
(see `partial_move_error`'s long comment about the override being lost on
rebuild). Feeding the operation's idempotency into the net request would
remove the second authority.

### 8. Provider-specific throttle policy lives in bifrost-net

`net::account_error::throttle_scope` hard-codes "Graph / Microsoft / Gmail /
Fastmail-JMAP get `ThrottleScope::Account`, everyone else none". Any other
JMAP server's 429 carries no throttle scope, so the engine never lifts it to
a `ThrottleKey::Account` and sibling scopes keep hammering. This is a
provider rule sitting in the transport crate keyed on `Provider` enum
values; the protocol crate should say it.

### 9. Unsupported-error constructors duplicated in roughly a dozen places

`unsupported_error` / `unsupported` in jmap (four), imap, dav-core (plus
caldav/carddav wrappers), graph (four), google (via its local error), smtp,
and the private one in `types/src/account.rs`. They agree on kind/cause
(the funnel forces that) but not on stamping: Graph sets `provider`, the
others do not; some set scope, some never can. The types crate's own
default-method errors (`unsupported_error`, `unsupported_inventory_stream`)
carry no protocol at all, so a provider that leans on a default emits a
protocol-less error while its own overrides stamp one. A public
`AccountError::unsupported(op, protocol)` would unify them.

### 10. dav-core ranks recovery classes by hand

`dav-core/src/error.rs` `recovery_rank` / `worse_recovery` is a private
severity order over `RecoveryClass`. It is the only such ranking in the
workspace, so a new terminal class lands at `u8::MAX` (beats `AuthLost`).
If a severity order is wanted, it is a property of `RecoveryClass` and
belongs next to `is_terminal`.

---

## Lateral findings in bifrost-types itself

### L1. `push_cause` is not "secondary evidence only"; it can reclassify

`reference/error-model.md` describes `into_builder` as "the decoration path,
not reclassification" and `push_cause` as "only appends secondary evidence".
But `recovery::derive` and its helpers read the WHOLE chain with
`find_map` / `any` (`transmission_state`, `server_retry_hint`,
`server_status`, `access_needed`, `access_resource`, `strategy_downgrade`,
`operator_reason`, `smtp_reply`). Appending a cause that the primary did not
supply changes recovery: an `Attempt(InFlight)` turns a primary-without-attempt
`Transport` retry into a reconcile; a sibling `Server` hint becomes the retry
hint; a `Wire(Smtp)` anywhere flips a 5xx to `ProviderRefused`. And
`TransportAcknowledged` scans the whole chain, so appending acknowledged
evidence to a transport error fails the build. Every chain-merging producer
(Graph push merge, net `AttemptHistory` / `LocalHold`, dav-core
`response_read_error_with_history`, carddav/caldav `partial_move_error` and
`directory_sync_error`) inherits this. Either derive should read only the
primary plus causes the producer marks as its own, or the doc should say
appended causes are inputs to classification.

### L2. `PushSubscription::new` does not enforce its own invariant

The type doc says "the handle is absent when no requested scope was
accepted" and the error model says the handle covers exactly the succeeded
lane. `new(handle, outcomes)` accepts `Some(handle)` with an empty succeeded
lane and `None` with a non-empty one. Graph had to add
`no_subscribable_push_scopes` because an arm produced the
handle-less all-failed shape; the constructor could reject it.

### L3. `FlagOp::validate_for_account` stamps no provider and fixes the operation

It always stamps `AccountOperation::UpdateFlags`, but providers call it from
`bulk_set_flags` paths whose engine-side operation may be something else in
telemetry; minor, but it is a shared constructor choosing the operation for
its caller, the same shape the `validate_batch_input` lesson in the error
model warns about.
