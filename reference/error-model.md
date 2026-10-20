# bifrost error model reference

The shared `AccountError` contract every protocol crate produces and
every consumer (`bifrost-sync`, downstream apps) reads. Lives in
`crates/types/src/error/` and is re-exported from
`bifrost_types::error`. Per-provider wire-to-`AccountError` mapping is
documented in each `reference/<crate>.md`; this doc is the shared
target they map onto. The code is authoritative for anything derivable
(the kind/cause/recovery enums and the `derive` table); this file
captures the load-bearing shape and the invariants the table enforces.

## AccountError

`AccountError` (`account_error.rs`) is an opaque
`Arc<AccountErrorInner>` newtype: `Clone` is one Arc bump, so it rides
through batch lanes, broadcast channels, and `Fatal` wrappers without
deep copies. `#[non_exhaustive]`, no public fields, no public
constructor - the only way in is `AccountErrorBuilder`. The inner
struct carries `kind`, derived `recovery` / `remediation` /
`message_key`, optional `scope` / `operation` / `provider` /
`protocol`, the `DiagnosticInfo`, and the `CauseChain`.

Accessors (all `&self`, cheap): `kind`, `recovery`,
`suggested_remediation`, `scope`, `operation`, `provider`, `protocol`,
`message_key`, `chain`, plus the diagnostic projections below.
`Display` prints `"{message_key} ({kind:?})"` - never free-form
provider text. `StdError::source()` bridges to `chain.outermost()` so
`?`-chaining and `anyhow`-style walkers reach the primary cause. Rust's
`StdError` source interface is linear and cannot represent the remaining
sibling evidence; the full ordered chain is available through `chain()` and
the internal support export. Each `Cause` variant is itself `StdError`.

`into_builder(self)` is the **decoration** path, not reclassification.
It unwraps the Arc (clone-on-share), splits the chain into primary +
extras, and returns a builder pre-loaded with the original `kind`,
primary `Cause`, top-level fields, and diagnostics. The builder exposes
no kind-changing method; `push_cause` only appends secondary evidence.
Derived fields recompute on `try_build`. Builder overrides
(`idempotency_override`, `throttle_scope`) are preserved across the
round-trip, as is a `ServerCause` retry hint because it lives in the
chain. Reclassifying to a
different primary kind/cause requires a fresh `AccountErrorBuilder::new`.

## AccountErrorBuilder funnel

`builder.rs`. `AccountErrorBuilder::new(kind, primary_cause)` is the
sole entry; setters are `#[must_use]` consuming-`self` (`operation`,
`scope`, `provider`, `protocol`, `push_cause`, `request_id`,
`trace_id`, `status`, `native_code`, `text`, `idempotency_override`,
`throttle_scope`).

The terminator is **`try_build` -> `Result<AccountError,
AccountErrorBuildError>`**, not an infallible `build`. The `Err` arm is
a producer-bug surface, not recoverable runtime state: protocol-crate
translation boundaries call
`.expect("valid account error classification")`. `AccountErrorBuildError`
(`#[non_exhaustive]`) enumerates the enforced invariants:

- `EmptyChain` - every error carries at least one primary `Cause`.
  `CauseChain::try_new` enforces this at the chain boundary. The current
  public `new` requires a primary, so this remains a defensive invariant
  for future construction paths.
- `KindCauseMismatch { kind, primary_cause }` - the declared `kind`
  and the outermost `Cause` disagree (`recovery::kind_matches_cause`).
- `ThrottleScopeNotApplicable { kind, throttle_scope }` - a
  `throttle_scope` attached to anything but
  `Server(RateLimited | QuotaExhausted)`. Its own variant because the
  kind and cause may match each other perfectly; the diagnosis must
  point the producer at the throttle scope, not a mismatch that is
  not there.
- `ThrottleBudgetNotApplicable { kind }` - a `throttle_budget` stamped
  on anything but `Server(RateLimited | QuotaExhausted)`, for the same
  reason.
- `TransportAcknowledged` - `Transport(_)` kind paired with any
  `Attempt(Acknowledged)` cause. Transport failure means no complete
  server response arrived; an acknowledged attempt belongs on a
  `Server`/`Protocol` kind.
- `CursorInvalidWithoutScope` - `SyncState(CursorInvalid)` without an
  `ErrorScope::Cursor`. The engine cannot route a scope-less cursor
  restart; the convergence rewrite removed the silent
  `RestartAccount` fallback, so producers must thread the scope.

On success `try_build` derives `recovery` (`recovery::derive`),
`remediation` (`recovery::suggest`), and `message_key`
(`message_key::derive`) from the validated parts, then freezes the
Arc. Derivation is pure: same parts in, same classification out.

## AccountErrorKind and message keys

`kind.rs`. `AccountErrorKind` (`#[non_exhaustive]`) is the stable
top-level taxonomy with per-family subkind enums:

- `Transport(TransportErrorKind)` - `Network` / `Timeout` / `Tls`.
- `Authentication(AuthErrorKind)` - `Expired` / `RefreshTransient` /
  `Revoked` / `ReauthorizationRequired`.
- `Authorization(AccessErrorKind)` - admin consent, conditional
  access, policy, scope, permission, account-disabled, mailbox
  unavailable (`MailboxUnavailableKind::{Transient,Permanent}`),
  not-licensed.
- `Server(ServerErrorKind)` - `Unavailable` / `RateLimited` /
  `QuotaExhausted` / `Error { status: Option<u16> }`.
- `SyncState(SyncStateErrorKind)` - cursor invalid, strategy failure,
  scope-capability lost, schema incompatible, capability changed,
  operator-override needed, scope revoked.
- `ConcurrencyConflict` (flat).
- `Request(RequestErrorKind)` - `Malformed` / `BatchInputInvalid`.
- `NotFound(ResourceKind)` - message, mailbox, thread, calendar, event,
  contact, draft, identity, vacation, push-subscription, account,
  filter (a server-side mail rule: ManageSieve script, Gmail filter,
  JMAP SieveScript - the operation enum already treats filters as first
  class, so a missing one does not borrow an unrelated resource).
- `Unsupported(AccountOperation)`.
- `Protocol(ProtocolErrorKind)` - parse-failed, missing-field,
  contract-violation, partial-response, unknown.
- `Internal(InternalErrorKind)` - a failure on the CLIENT side of the
  wire: this library, or any `Account` implementation (bifrost's own or a
  consumer's), never the provider and never the caller's request.
  `InvariantViolated` (a state the implementation believes impossible: a
  poisoned lock, a type-erased result of the wrong type, a cursor this code
  built that will not serialize), `RuntimeFailure` (a local facility failed:
  the entropy source for a SCRAM nonce), `Panicked` (a contained panic; the
  resource it ran on is dead), `AccountContract` (the sync engine caught an
  `Account` implementation breaking its batch contract), `LimitExceeded`
  (an implementation-defined safety or resource limit, such as a pagination
  walk's page budget: the operation is supported, the implementation
  declined to go further). Its cause is `Cause::Internal(InternalCause {
  kind, detail })`; keys `internal.invariant-violated`,
  `internal.runtime-failure`, `internal.panicked`,
  `internal.account-contract`, `internal.limit-exceeded`.

The three blame families are deliberately disjoint: `Request(_)` is the
caller asking for something inexpressible, `Protocol(_)` is the provider
sending something wrong, `Internal(_)` is the code in between. Before
`Internal` existed, client-side failures borrowed
`Protocol(ContractViolation)` and told operators to contact the provider
about a bifrost bug.

`message_key::derive(&kind) -> &'static str` is the stable telemetry
namespace: dotted, family-prefixed, hyphenated leaf
(`transport.network`, `auth.refresh-transient`,
`authz.admin-consent-required`, `server.rate-limited`,
`syncstate.cursor-invalid`, `concurrency.conflict`,
`request.batch-input-invalid`, `notfound.message`,
`protocol.partial-response`, `unsupported.operation`, ...). The keys are
exhaustively pinned by
`documented_message_keys_are_derived`. Two collapse subkind detail:
`Server(Error { status })` is always `server.error` (status rides in
`DiagnosticInfo`, not the key), and `Unsupported(_)` is
`unsupported.operation`. The dotted convention is the contract; read
`message_key.rs` for the live list rather than copying it here.

## RecoveryClass

`recovery.rs`. `RecoveryClass` (`#[non_exhaustive]`) is the consumer's
dispatch surface. Four mutually-exclusive, exhaustive helpers partition
it (proven by `recovery_helpers_are_mutually_exclusive_and_exhaustive`):

- `is_retryable()` -> `Retry(RetryAdvice)`.
- `requires_reconciliation()` -> `Reconcile(ReconcileAdvice)`.
- `requires_engine_action()` -> `Engine(EngineDirective)`.
- `is_terminal()` - an explicit match over the terminal variants (`AuthLost`,
  `NeedsAdminConsent`, `NeedsPolicyChange`, `NoPermission`,
  `Unsupported`, `ClientBug`, `ProviderContractViolation`,
  `ProviderRefused`, `UnknownPermanent`, `InternalFailure`).

`EngineDirective`: `RestartScope(CursorScope)`, `RestartAccount`,
`DowngradeStrategy(StrategyDowngrade)`,
`DowngradeCapabilityForScope(CursorScope)`, `SchemaIncompatible`,
`OperatorOverrideRequired { reason }`, `DisableScope(CursorScope)`
(quarantine one cursor scope without escalating account-wide: a revoked
shared/other-user IMAP folder). There is no longer a
`CapabilityChanged` directive - capability shifts route to
`RestartAccount` so the engine re-runs discovery (the
`StateCause::CapabilityChanged` delta is forensic-only).

`RetryAdvice { disposition, retry_hint, reason, throttle_scope }`.
Retry timing is a single **`RetryHint` enum**, not split fields:
`After(Duration)` (delta-seconds / `min_delay` semantics) or
`At(SystemTime)` (HTTP-date `Retry-After`). `not_before(now)` and
`min_delay(now)` resolve it either way (the split `not_before` +
`min_delay` fields the spec once carried produced the side-channel bug
the convergence rewrite eliminated). `disposition` is
`RetryDisposition::{SameRequest, AfterStateRefresh, AfterAuthRefresh}`;
`reason` is `RetryReason::{Transport, ServerUnavailable, RateLimited,
QuotaExhausted, ConcurrencyConflict, RefreshTransient}`.

`ReconcileAdvice { reason, guidance, retry_hint, throttle_scope }`:
`ReconcileReason::{TransportDropAfterSend, PartialCompletionSignal,
ThrottledMidFlight, InternalFailureAfterSend}`
plus `guidance.actions: Vec<ReconcileAction>` where `ReconcileAction`
is `CheckTarget` / `DedupeByClientId`.
Both advice structs are explicitly `#[non_exhaustive]`.
`ReconcileGuidance` is an ordinary constructible public struct, so the nested
shape is not accidentally sealed.

`ThrottleScope::{CurrentOperation, Mailbox, Account, Tenant, Provider}`
is the producer hint; the engine lifts the sharable scopes into a
`ThrottleKey::{Mailbox, Account, Tenant, Provider}` bucket.
`CurrentOperation` is a per-call inline delay and never enters a key.

A producer that knows which provider rate budget a throttled request was
charged to names it instead: `AccountErrorBuilder::throttle_budget(ThrottleBudget)`
(repeatable, read back through `AccountError::throttle_budgets()`), valid only on
`Server(RateLimited | QuotaExhausted)` like `throttle_scope`
(`ThrottleBudgetNotApplicable` otherwise). A `ThrottleBudget` is an opaque,
process-wide identity minted by the protocol crate; it does not embed an
`AccountId`. When an error stamps budgets, the engine records the deadline on
exactly `ThrottleKey::Budget(_)` for each and does NOT also lift the
`ThrottleScope` (recording the account key too would pause every sibling
mailbox the budget exists to keep running); a stamped budget with no retry
hint is held for the one-second local retry fallback. The other half of the
contract is `Account::throttle_keys(account, ThrottleWork)`: the keys a unit of
work (`ThrottleWork::{Scope, Objects, Membership}`) consumes, defaulting to
none. A crate that stamps a budget must declare it for the same work, through
the same function of the same routing decision, or the hold is recorded where
no wait site looks. Budgets survive `into_builder`.

No in-tree producer emits `ThrottleScope::Tenant` or `ThrottleScope::Provider`.
Graph tagged every throttle `Tenant`, but Microsoft documents the Outlook mail,
calendar and contacts limits per app per mailbox, so a tenant hold paused
mailboxes the provider never throttled; Graph now emits `Account` and stamps
the mailbox budget where it can attribute the request. The variants stay
published for implementations whose provider genuinely throttles that wide.

`Fatal(AccountError)` is the terminal-only newtype. `TryFrom` accepts
iff `recovery().is_terminal()`, returning the error back on the `Err`
arm otherwise. `Fatal` is the type-system collapse point that enforces
"the engine has nothing left to try": every terminal `RecoveryClass`
variant funnels into one carrier. The engine does not ship a built-in
operator-notification queue or permanent-failure dashboard - both
terminal arms emit a structured `TelemetryView` `warn!`; any operator
queue is the consumer's to build off the broadcast
`SyncEvent::Terminated`.

`RemediationAction` (operator-facing suggestion, derived by `suggest`):
`RefreshToken`, `Reauthorize`, `RequestAdminConsent { needed }`,
`UpdateTenantPolicy`, `CheckMailboxLicense`,
`RetryLater { retry_hint }`, `FixClientRequest`,
`ContactProviderSupport`, `ReportBug` (report to whoever ships the
implementation that failed: this library, or the consumer's own `Account`
implementation; derived for every `Internal(_)` kind, on either of its
recovery arms).

### Central derivation

`recovery::derive(kind, scope, operation, chain, throttle_scope,
idempotency_override)` is the single mapping. Two inputs come from
outside the kind: `tx_state` = the first `Attempt` cause's
`TransmissionState` (default `Unsent`), and `idempotent` =
`idempotency_override` else `operation.is_idempotent()` (a `None`
operation is treated idempotent). The rules at altitude (read
`derive_*` for exact rows):

- **Transport** -> always transient. `transient_retry_or_reconcile`:
  `Unsent`/`Acknowledged`, or `InFlight`+idempotent -> `Retry(SameRequest,
  Transport)`; `InFlight`+non-idempotent ->
  `Reconcile(TransportDropAfterSend, [CheckTarget])`. `Acknowledged` is
  a producer bug here: `debug_assert!` in debug, defensive demotion to
  `InFlight` in release (and `try_build` rejects it upstream anyway).
- **Authentication** -> `RefreshTransient` is `Retry(AfterAuthRefresh,
  RefreshTransient)` (carries the chain's server retry hint); `Expired` /
  `Revoked` / `ReauthorizationRequired` collapse to `AuthLost`.
- **Authorization** -> `AdminConsentRequired` ->
  `NeedsAdminConsent { needed }`; conditional-access / policy / scope /
  not-licensed -> `NeedsPolicyChange`; `PermissionDenied` ->
  `NoPermission { resource }`; `AccountDisabled` and
  `MailboxUnavailable(Permanent)` -> `ProviderRefused`;
  `MailboxUnavailable(Transient)` -> `Retry(SameRequest)`.
- **Server** -> `Unavailable`/`RateLimited`/`QuotaExhausted` route
  through `transient_retry_or_reconcile` (rate/quota propagate
  `throttle_scope` and the retry hint). A rate/quota response on an
  in-flight non-idempotent operation reconciles as `ThrottledMidFlight`
  while retaining both fields, so engine buckets and the reconciliation
  delay still honor the provider signal. `Error { status }`: 5xx (or no
  numeric status while `InFlight`) is transient; everything else (4xx,
  IMAP `NO`/`BAD` not in-flight) -> `ProviderRefused`. The 5xx rule is
  HTTP's, and it does not apply to an SMTP reply: RFC 5321 4.2.1 defines
  the 5yz class as PERMANENT negative completion, so `derive_server` reads
  the protocol from the cause chain (`Cause::Wire(WireCause::Smtp(_))`,
  which only the SMTP translator mints) and derives an SMTP 5xx as
  `ProviderRefused`. Before that row a 554 at the end-of-data terminator
  was `Retry(SameRequest)`: advice to resend a whole message body to a
  server that had just permanently refused it. Pinned by
  `an_smtp_5xx_is_a_permanent_refusal_not_a_retry`.
- **SyncState** -> all `Engine(_)`: `CursorInvalid` -> `RestartScope`
  (scope guaranteed by build-time check); `StrategyFailure` ->
  `DowngradeStrategy`; `ScopeCapabilityLost` ->
  `DowngradeCapabilityForScope` (or `RestartAccount` if no scope);
  `SchemaIncompatible` -> `SchemaIncompatible`; `CapabilityChanged` ->
  `RestartAccount`; `OperatorOverrideNeeded` ->
  `OperatorOverrideRequired { reason }`; `ScopeRevoked` ->
  `DisableScope` (or `RestartAccount` if no scope). CardDAV uses this for a
  personal contact collection reclassified as a directory: no sync strategy is
  eligible, so it retires the original cursor scope rather than retrying or
  downgrading. The directory refusal's redirect evidence is preserved.
- **ConcurrencyConflict** -> `Retry(AfterStateRefresh,
  ConcurrencyConflict)`.
- **Request** (both subkinds) -> `ClientBug`.
- **NotFound** -> `ProviderRefused` (unconditional; the per-operation
  "this NotFound is benign, absorb it" policy is **not** in this module
  - it lives at the call site in each protocol crate's operation, which
  swallows the absent-resource case before building an error).
- **Unsupported** -> `Unsupported(op)`.
- **Protocol** -> parse / missing-field / contract-violation ->
  `ProviderContractViolation`; `PartialResponse` -> `Retry(SameRequest)`
  if idempotent else `Reconcile(PartialCompletionSignal, [CheckTarget,
  DedupeByClientId])`; `Unknown` -> `UnknownPermanent`.
- **Internal** -> never retried: an implementation failure is deterministic
  or has left its resource dead, and retrying one in a loop is worse than
  stopping visibly. `InternalFailure` (terminal), except `InFlight` on a
  non-idempotent operation -> `Reconcile(InternalFailureAfterSend,
  [CheckTarget])`: the failure struck after the request may have reached the
  server, so whether it landed is read back rather than declared. `InFlight`
  on an idempotent operation stays terminal - replay being safe does not
  justify re-running broken code. The reconcile arm is only as good as the
  producer's attempt evidence, since a missing `Attempt` reads as `Unsent`:
  every producer that fails mid-exchange must stamp its transmission state
  (IMAP's driver death stamps the caller's phase - `Unsent` when the driver
  never received the command, `InFlight` when it owned it).

### Local refusals

**A request a protocol crate refuses LOCALLY, before any byte of it is sent,
is never a provider fault.** `Protocol(_)` means the provider sent something
malformed, out of sequence, or incomplete; it derives
`ProviderContractViolation` - terminal, remediation "contact provider
support" - and filing a local refusal there tells the operator to blame the
provider for the client's own decision. Every producer maps a local refusal by
what the refusal is about:

- **The caller's input cannot be expressed** (malformed or out-of-range
  arguments, an empty list the grammar forbids, a command the caller issued in
  a state that does not permit it) -> `Request(Malformed)`, so `ClientBug`. A
  state refresh does not make such a request valid, so it must not be
  retryable.
- **The server lacks a capability the request needs** (an extension it does
  not advertise, a command its protocol revision removed), or the client does
  not implement a variant of a published `#[non_exhaustive]` request type ->
  `Unsupported(operation)`.
- **Local state moved under a queued command** (the session left the states
  the command needs while it waited behind another) -> transient:
  `ConcurrencyConflict`, so `Retry(AfterStateRefresh)`, with an `Unsent`
  `Attempt` cause. The one exception is state that moved because the
  connection is gone (a completed LOGOUT or BYE): that is a transport condition,
  `Transport(Network)` with `Unsent`.

A missing `Attempt` cause reads as `Unsent` in `derive`, so a refusal with no
transmission evidence is already classified as unsent; stamping `Unsent`
explicitly is still right wherever the producer knows it.

The rule is about the refusal, not about where it is raised: a validation made
AFTER bytes of the same exchange are on the wire (a SASL step mid-exchange, a
post-OK stream upgrade) is not a local refusal of an unsent request, and its
classification and connection handling are the producer's to argue
separately. A local INVARIANT failing there is still the client's fault
(`Internal(InvariantViolated)`, with the exchange's transmission state);
whether the connection survives is the producer's call, and IMAP retires it.

## Cause chain

`cause.rs`. `CauseChain` is a non-empty ordered `Vec<Cause>`:
`outermost()` (the primary, index 0) and `root()` (last). `Cause`
(`#[non_exhaustive]`) variants:

- `Transport(TransportCause { kind, message })`,
- `Attempt(AttemptCause { transmission_state })`,
- `Auth(AuthCause)`, `Access(AccessCause)`, `Server(ServerCause)`,
  `State(StateCause)`, `Request(RequestCause)`,
- `Wire(WireCause)`,
- `Internal(InternalCause { kind, detail })` - the cause an
  `Internal(kind)` error must carry, `kind` matching.

`AttemptCause` / `TransmissionState::{Unsent, InFlight, Acknowledged}`
is the **transmission-evidence** carrier: it is what `derive` reads to
choose retry-vs-reconcile, and what `try_build` cross-checks against
`Transport`. Producers thread it as a secondary cause via `push_cause`.

`ServerCause` carries the optional `RetryHint` for the
unavailable/rate/quota arms (the only structural channel for retry
timing). `StateCause` carries engine-directive payloads
(`StrategyDowngrade`, `CapabilityChanged { delta }`,
`OperatorOverrideNeeded { reason }`).

`WireCause` is **diagnostic-only** - it never feeds `derive` (the
recovery mapping keys off `kind`, not the raw wire code). It preserves
the provider's native vocabulary for support exports:
`Graph(GraphSignal)`, `Jmap(JmapMethod)`, `Imap(ImapResponseCode)`,
`Smtp(EnhancedStatusCode)`, `Gmail(GmailSignal)`, and
`MalformedResponse { protocol, detail }`. Each provider enum exposes
`code()` (the on-wire string) for telemetry. `Cause::summary()` projects any
cause into a flat `CauseSummary` for the internal export tier. Alongside kind,
detail, transmission state, status, and native code, the projection preserves
structured transport kind, access resource and needed scope, strategy
downgrade, capability delta, invalid batch items, unsupported operation, and
invalid argument field evidence.

### An `AccountError` is not durably serializable, on purpose

Several `Cause` payloads are `&'static str` (`AccessCause::InsufficientScope
{ needed }`, `AccessCause::AdminConsentRequired { needed }`,
`RequestCause::InvalidArgument { field }`). A decoder holding bytes cannot
produce a `&'static str` without leaking, so a byte-exact `AccountError` round
trip is not merely expensive - it is unrepresentable, and no amount of codec
work changes that. Nothing in this workspace offers one.

`bifrost-sync`'s ledger envelope is the one place that persists an error, and it
persists a DIGEST: the exact classification (`AccountErrorKind`, `ErrorScope`,
operation, provider, protocol) plus the whole `DiagnosticInfo`, rebuilt through
`AccountErrorBuilder` with the canonical `Cause` its kind demands. Secondary
cause payloads, `idempotency_override` and `throttle_scope` are lost. That is
sound there only because the field is evidence an operator reads and never an
input to a decision; see `reference/sync.md` under "Ledger envelope". Any future
caller wanting to persist an error owes the same argument, because a restored
error's `recovery()` was derived from a synthesized cause chain and is not the
classification the producer made.

## Batch and stream outcomes

`batch.rs`. `BatchOutcome<T>` (`#[non_exhaustive]`, immutable) is the
**three-lane** result of a `Vec<BatchItem<_>>` operation:
`succeeded: [BatchSuccess<T>]`, `failed: [BatchFailure]`,
`uncertain: [BatchUncertain]`, plus an `order` index so `iter()`
replays submission order. The lanes are closed by design - a fourth
lane is a deliberate breaking change, never a wildcard-absorbed
extension. `BatchFailure` and `BatchUncertain` each carry an
`AccountError`; the uncertain lane is the batch analogue of an
`InFlight` non-idempotent drop and always queues for read-back.

`BatchItemId(String)` correlates each lane entry back to its submitted
`BatchItem`. `BatchOutcomeBuilder` accumulates via `push_succeeded` /
`push_failed` / `push_uncertain`, then `finalize(&expected)` enforces
the **accounting invariant**: every submitted id appears in exactly one
lane, with no missing / duplicate / unknown ids, else
`BatchInvariantError { missing, duplicates, unknown }`. This is a
producer-bug surface caught by per-crate tests.

Boundary contract for batch-shaped `Account` methods: **`Err(_)` means
nothing was transmitted** (whole-request failure); **`Ok(BatchOutcome)`
means every item is accounted for exactly once** across the three
lanes. `validate_batch_input` is the pre-flight guard - empty input or
empty/duplicate `BatchItemId`s surface as
`Request(BatchInputInvalid)` before any byte crosses the side-effect
boundary. The guard returns the classified `AccountError` directly. Empty
input uses `RequestCause::BatchInputEmpty`, with no fabricated item id;
item-specific failures use `BatchInputInvalid { items }`.

`validate_batch_input(items, protocol, operation)` takes the operation as a
required parameter, not as something the caller decorates afterwards.
Centralizing the construction is exactly what made dropping it possible -
the first centralization did drop it, and SMTP's rejections stopped
reporting `AccountOperation::Send`, leaving telemetry and support exports
unable to say which operation refused the input and the derived idempotency
wrong on top of that.

`Protocol::Unknown` exists for errors the ENGINE mints about an account's
behaviour rather than errors a protocol crate maps from a wire response.
There is no protocol accessor on `dyn Account`, so where the offending
payload carries no protocol tag of its own (a backfill checkpoint, say) the
error declines to name one rather than guessing. Protocol crates always know
their own protocol and must never use it.

The same three lanes carry `push_subscribe`, whose input is a scope list
rather than a `Vec<BatchItem<_>>`. `PushSubscription { handle, outcomes }`
puts a `BatchOutcome<CursorScope>` beside an optional handle: the handle
covers exactly the succeeded lane, refused scopes carry their own
scope-correlated `AccountError` in the failed lane, and the handle is absent
when no scope was accepted. `BatchItemId`s are the scopes' submission
positions, because a `CursorScope` is not an id and one request may legitimately
name the same folder twice. No fourth lane and no parallel contract: an
`Err(_)` from `push_subscribe` still means no scope was subscribed at all.

`stream.rs`. `ItemOutcome<T>` is the streaming counterpart -
`Succeeded(BatchSuccess<T>)` / `Failed(BatchFailure)` /
`Uncertain(BatchUncertain)` - the same closed three-lane model, emitted
per item instead of collected. Deliberately not `#[non_exhaustive]`: a
wildcard arm would let stale consumer policy silently apply to a new
lane. `MutationSuccess::{Applied, Skipped, Downgraded { actual }}` is the
mutation success payload, and the three are distinct answers to
distinct questions: `Applied` means the target is in the requested
state, `Skipped` means it already was and nothing was done, and
`Downgraded` means the provider accepted a WEAKER operation - the target
changed but is not in the requested state, and `actual: MutationEffect` says
what weaker operation landed.

`Downgraded` exists because reporting either neighbour in its place is
a wrong answer a consumer cannot detect. Gmail `bulk_destroy` under the
`gmail.modify` OAuth scope cannot permanently delete, so it falls back
to moving the messages to Trash: reported `Applied`, the engine believed
a state it re-observed as false on every subsequent pass and re-issued
the destroy forever; reported `Skipped`, a real mutation would be
hidden. `bifrost-sync` files it `PendingReadback` rather than trusting
it, so the final accounting comes from observed state - a downgrade is
the one success report whose own claim is known to be incomplete.
Gmail reports `MovedToContainer(TRASH)`. A provider that applied only the
representable subset of a flag operation reports
`FlagsPartiallyApplied { unsupported }`, preserving the flags it could not
apply for read-back and diagnostics. It is deliberately NOT queued for
resubmission: a downgrade is not
transient, and replaying it earns the same downgrade.

"The target changed" is a precondition of `Downgraded`, not a description
of it. An operation the provider could not perform at all - every
requested flag unrepresentable, so no request is even sent - is not a
downgrade and must not borrow the lane. It produces `ItemOutcome::Failed`
per id with `Unsupported(operation)` -> `RecoveryClass::Unsupported`,
which is permanent and not retried. This matters beyond tidiness:
`bifrost-sync` files every `Downgraded` as `PendingReadback`, so a no-op
wearing the downgrade lane schedules a read-back for a mutation that
never happened, and spends the read-back budget of a real one. Protocol
crates: if there is nothing you could do, say so in the failure lane.

## Diagnostics and consent tiers

`diagnostic.rs`. `DiagnosticInfo` holds `request_id`, `trace_id`,
`status`, `native_code`, and `text: Vec<DiagnosticText>`.
`DiagnosticText { value, visibility }` tags each string
`DetailVisibility::{UserSafe, SupportOnly}` (constructors
`user_safe` / `support_only`). This tagging drives a graduated
consent-tier export off `AccountError`:

- `telemetry_fields()` / `support_minimal()` -> `TelemetryView`:
  discriminants + ids + structured recovery fields + transmission state.
  Producer-supplied ids and native codes pass through the bounded, single-line
  `TelemetryToken`; rejected values remain support-only text and are omitted
  from telemetry. **No free-form text** (`telemetry_has_no_free_form_text`); safe
  to ship to metrics unconditionally.
- `user_safe_text()` -> iterator over only the `UserSafe` strings.
- `support_consented()` -> `SupportExportConsented`: telemetry +
  user-safe text + support-only text + `scope`. Requires user consent.
- `support_internal()` -> `SupportExportInternal`: the consented export
  plus the full `chain` as `CauseSummary`s. Internal tier only.

All export structs are `Serialize`; `TelemetryView` projects the
recovery fields (disposition/reason/throttle for retry,
reason/actions/throttle for reconcile, none for terminal) via the
`recovery_fields` / `recovery_discriminant` matches in
`account_error.rs`.

## Scope, operation, provider, protocol

`scope.rs`. `ErrorScope` (`#[non_exhaustive]`, custom `Serialize`)
locates the failure: `Account`, `Cursor(CursorScope)`, and id-bearing
`Mailbox` / `Message` / `Thread` / `Calendar` / `Event` / `Contact` plus the
collection variants, plus `DirectoryGateway { url }` for a read-only directory
endpoint that a multi-gateway lookup skipped. The id-bearing variants use the same typed ids as
the account surface (`MailboxId`, `ObjectId`, `ThreadId`, `CalendarId`,
`EventId`, `ContactId`) and serialize their inner strings for support exports.
`scope_fields` is the single source for both the declared struct field
count and the fields written, so the count cannot drift from the body
across the variable-width cursor shapes - a drift that self-describing
JSON hides but length-prefixed formats turn into corrupt output.
`AccountOperation` is the large
`#[non_exhaustive]` operation enum; `is_idempotent()` is the
**authoritative idempotency source** `derive` consults. Absolute-state writes
against a known id, including the `*Update` family (except `FilterUpdate`,
which is delete+create on Gmail and name-keyed on ManageSieve, so it sits with
the non-idempotent writes) and the singleton settings
writers, are idempotent; sends, creates, deletes, moves, renames, uploads, and
other potentially double-applied side effects are not. Rename is with the moves
deliberately: IMAP `RENAME` keys on the old name rather than an id, so a replay
of a rename that landed names a mailbox that no longer exists. `Provider` and
`Protocol` are the small stable provenance enums.

## Warning is outside the error model

`warning.rs`. `Warning { kind: WarningKind, message, next_action,
protocol_detail, retry_count }` is **deliberately not** part of the
error model: it is advisory, never aborts a stream, carries no
`RecoveryClass`, and never converts to `AccountError`.
`WarningKind::{StrategyDowngraded, OperatorAttentionNeeded, Throttled,
ClockSkew, BlobNotByteStream, ReadbackSkipped, ChangeStreamLagged,
InventoryEndedUnannounced, Other}`. The sync engine emits warnings
alongside (not instead of) errors; see `reference/sync.md`.
`ChangeStreamLagged` is structural on purpose - the multiplexer's
`ChangesReceiver` mints it (and only it) when broadcast ring overflow
destroys batches, and a consumer's detach/reconcile recovery keys on the
kind rather than on message text. `InventoryEndedUnannounced` is structural
for the same reason: both engine inventory front ends mint it when a
provider's inventory stream ends with neither `Done` nor `Terminated`, and
the scope it names is not treated as complete.

## File map

```
crates/types/src/error/
  mod.rs           // module + public re-exports
  account_error.rs // AccountError, accessors, into_builder,
                   // StdError bridge, TelemetryView projection
  builder.rs       // AccountErrorBuilder + try_build invariants
                   // (AccountErrorBuildError)
  kind.rs          // AccountErrorKind + subkind enums
  message_key.rs   // derive() stable dotted namespace
  recovery.rs      // RecoveryClass, derive/suggest, RetryHint,
                   // EngineDirective, Fatal, ThrottleKey/Scope
  cause.rs         // Cause/CauseChain, Attempt/TransmissionState,
                   // WireCause provider vocabularies, CauseSummary
  batch.rs         // BatchOutcome three-lane + builder/finalize,
                   // BatchItemId, validate_batch_input
  stream.rs        // ItemOutcome, MutationSuccess
  diagnostic.rs    // DiagnosticInfo/Text, DetailVisibility,
                   // TelemetryView, support-export tiers
  scope.rs         // ErrorScope, AccountOperation (is_idempotent),
                   // Provider, Protocol
  warning.rs       // Warning + WarningKind (outside the error model)
```

## Page completeness

`Page::truncated` defaults to false at every constructor. It signals a result
limit that left a terminal page non-exhaustive, independent of `failed_ids`
(resources that could not materialize) and `skipped_scopes` (scopes that failed).
A missing `next_cursor` means terminal, not necessarily complete. CardDAV
directory gateways use this signal for collection-level 507 truncation and
local overflow beyond the global caller limit. A skipped gateway uses
`ErrorScope::DirectoryGateway { url }`, so its omission is identifiable.
