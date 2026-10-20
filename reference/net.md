# bifrost-net reference

Current architecture of the shared HTTP transport crate.

Scope: HTTP/2 connection pool, OAuth bearer-token refresh, retry
budget, per-host rate limiting, per-account bandwidth metering,
native-tls, W3C `traceparent` injection, URL component-encoding
helpers, resource-parent extraction, and origin-rooted well-known URL
construction shared by the HTTP protocol crates. Used by `bifrost-jmap`,
`bifrost-google`, `bifrost-graph`, and - through `bifrost-dav-core` -
`bifrost-caldav` and `bifrost-carddav`. Not used by `bifrost-imap` or
`bifrost-smtp` (those carry their own TCP/TLS stacks); IMAP/SMTP
report bytes-in/out through `MeterSink` for unified bandwidth
accounting.

## Two-tier handle

```rust
let net = Net::new(NetConfig::default())?;             // process-wide
let account = net.attach_account(account_id, spec);     // per-account
```

- `Net` holds the shared `reqwest::Client`, the per-host
  `RateLimitGovernor`, and the `BandwidthMeter`. Cheap-cloneable
  (one `Arc` bump); shared across every account so multi-account
  quota coordination on Gmail / Graph hosts works out of the box.
- `AccountNet` is account-scoped: token source, timeouts, User-Agent,
  buffered-body ceiling, redirect policy, default `RetryPolicy`,
  `AtomicU8` priority, and a shared `BandwidthBudget` from its Net registry.
  `Clone` is one `Arc` bump; protocol crates clone per spawned task.
- `Net::shared_default()` returns a process-wide `OnceLock<Net>`
  built from `NetConfig::default()`. The default registration target
  for protocol crates whose ergonomic constructors need a `Net`
  without forcing the caller to thread one through; multiple clients
  in the same process share the connection pool, governor, and
  bandwidth meter. Applications that need a custom `NetConfig` still
  call `Net::new` and hand the result to the protocol clients
  explicitly.

`NetConfig` contains only client-instance settings: pool sizing, keepalive,
connection setup timeout, and TLS trust. `AccountSpec` contains request behavior:
response-header and body timeouts, optional total timeout, User-Agent,
buffered-body ceiling, redirect policy, token max-age, retry policy, token source,
and rate declarations.
The caller-authored configuration records `NetConfig`, `AccountSpec`,
`RateLimit`, `RetryPolicy`, and `RedirectPolicy` are `#[non_exhaustive]`; callers
start from their constructors or defaults and then assign supported fields.
JMAP attaches every independently opened account to the shared transport
selected by `Net::shared_for_tls` (see the TLS section), so its accounts share
the client, governor, and meter within each trust class as advertised. The
CalDAV and CardDAV clients ride `AccountNet` too, through
`bifrost-dav-core`'s `DavDispatch`: `AccountNet::request(Method, ..)` is what
carries their WebDAV extension verbs, and they attach with no token source
because their credential-origin gate mints the `Authorization` header itself and
every request opts out with `without_bearer_auth`.

Every `attach_account` receives a monotone registration token carried
by its `AccountNet`. `AccountNet::detach()` removes that exact
registration, decrements the meter's per-id attach count, and
decrements the governor for only the hosts that attachment
successfully registered. Release is also automatic: `AccountNetInner`
has a `Drop` that runs the same registration-keyed teardown when the
last clone of a handle goes. Google and Graph call `detach()`
explicitly, the JMAP reqwest transport never did, and
`NetInner::account_hosts` plus the meter map therefore grew by one
entry per JMAP account open for the process lifetime; making the
release automatic closes that for every caller rather than relying on
each to remember, and leaves `detach()` as the way to release early.

Keying on the exact token is what makes this safe when two live handles
share an `AccountId`: a stale Drop cannot detach the replacement.
`detach_registration` returns before touching the meter or the governor
once its token is absent, so a Drop after an explicit `detach()`, or
after a cross-id `retag()` moved the token, is a complete no-op. A
SAME-id `retag` returns the receiver itself rather than minting a
second `AccountNetInner` over one token - two owners of one
registration is ambiguous, and whichever dropped first would release
what the other still depends on.
`Net::detach_account(id)` remains the ID-only compatibility entry point
and removes one registration. Host buckets and meter counters are
reclaimed only after their final matching detach.

`AccountNet::retag(new_id)` re-mints the handle under a different
engine `AccountId`. Used by protocol-crate factories whose
`open(account_id)` needs to honor an engine-minted id when the
consumer built the `AccountNet` ahead of time via
`*Client::with_account_net` and the parent `Net` is no longer
reachable through the client. The exact token's per-account host entry
moves under `new_id`, and the meter transfers one attachment count from
the old id to the new one (it does not separately register the new id,
so a retag never inflates the total attach count). The new handle
inherits token
source, retry policy and priority from the old. Future admissions use the
destination id budget from the registry; outstanding source grants retain
the source budget, even after the old handle is dropped.
Governor refcounts are per-host and not touched. The old handle keeps
working for in-flight clones; its old-id teardown is a no-op because
the token moved.

## Request flow

```rust
let response = account.post(url)
    .header("X-Custom", "value")
    .json(&body)
    .cost(5)                       // rate-limit quota debit
    .retry(RetryPolicy::default())
    .timeout(Duration::from_secs(30))
    .send()
    .await?;
```

`RequestBuilder` consumes `self` on every setter and drives the
retry loop in `send` / `send_streaming`. Each retry attempt is a
fresh wire request: governor debit + outbound metering + body send
+ inbound metering. Buffered (`send`) and streaming
(`send_streaming`) share `send_streaming_inner`; the buffered path
drains the body through the same metering reader.

### Shared provider holds and error observation

`Net::shared_hold(key)` and `AccountNet::shared_hold(key)` open a `SharedHold`
against the same string-keyed registry on that Net. The registry owns its
entries until their monotonic expiry; dropping every account and hold handle
cannot erase an unexpired hold. Expired entries are pruned opportunistically.
Handles look up the key again on every access, so an old handle still shares
updates with one opened after pruning. Independent Nets have independent hold
registries; Google factories use the shared default Net.

`HoldDeadline` pairs a `tokio::time::Instant` for enforcement with a
`SystemTime` for reporting, resolved together at response observation.
`SharedHold::record(deadline, kind)` keeps the later deadline WITH its
`HoldKind::{RateLimited, QuotaExhausted}`. Quota wins a monotonic-deadline tie;
a shorter quota deadline cannot relabel a longer rate deadline, and a shorter
rate deadline cannot weaken a longer quota hold.

`RequestBuilder::hold_gate(key)` checks that registry immediately before every
wire attempt, after concurrency admission, rate admission and token lookup or
refresh. This includes net-internal retries and redirect hops. A held gate
returns `Error::LocalHold { key, held, attempts, may_have_committed }` without dispatching or
recording anything back into the hold. Already dispatched attempts finish.
Rate admission is refunded by the existing debit guard on a local refusal.

`attempts` retains the earlier outcomes in order, including transport state and
response status, headers and capped diagnostic body. An initial refusal has no
earlier attempts and translates with `Attempt(Unsent)`. A refusal between
attempts preserves those causes and outcome diagnostics; the effective state
is `InFlight` if any attempt remains uncertain OR an acknowledged
non-idempotent attempt may have committed, otherwise `Acknowledged` if a
response arrived, otherwise `Unsent`. The `may_have_committed` flag survives
redirect method rewrites: a committed POST answered by 303 followed by a held
GET reconciles as `ThrottledMidFlight` for a non-idempotent operation, rather
than replaying the POST. The original acknowledged response remains in the
secondary evidence. It never replaces earlier uncertainty
with a synthetic unsent replacement attempt. Generic translation produces the
hold's server kind and a `RetryHint::After` from the remaining monotonic
duration; the protocol crate stamps its own budget identity.

All other error exits after earlier wire attempts carry
`Error::AttemptHistory { source, attempts, may_have_committed }`. A single
completion boundary covers deadline checks, retry sleeps, rate admission,
token lookup and refresh, cancellation errors, redirect classification and
hop limits, and diagnostic drains. The evidence also travels with a successful
response's body: buffering, ceiling checks, range validation and streaming
failures preserve it. Earlier attempts are retained even without a hold gate.
A response whose diagnostic drain fails still contributes its status and
headers. `Error::terminal()` exposes the failing stage for diagnostics without
removing the history; classification must consume the complete error.

If a non-idempotent attempt may have committed, generic translation uses
`Protocol(PartialResponse)` with an effective `Attempt(InFlight)` ahead of the
physical attempt evidence. This is the existing reconcile-required lane for
an incomplete logical exchange, including when the later failure is auth or
redirect policy. It prevents replaying a committed POST after its redirected
GET times out during token lookup, or when a quota retry wait exceeds the
total deadline. Histories containing only unsent attempts keep the terminal
classification. Ordinary acknowledged idempotent auth and policy failures
also retain their terminal classification.
Protocol consumers that read statuses as data must inspect `terminal()` while
retaining the complete error for failure classification. DAV reconstitutes a
terminal response from a replayable request (including DELETE followed by 404)
and preserves its response-overflow classification with the history attached.
Unreplayable DAV histories go through the shared reconciliation mapping. Graph's
status reader also sees terminal evidence inside the envelope, so subscription
cleanup and throttle-budget attribution retain their existing status handling.

`RequestBuilder::redirect_guard(predicate)` optionally restricts followed
redirects using their current and resolved target URLs. It runs after net's
method/trust classification and before draining the redirect or admitting the
target. Rejection retains `Error::RedirectRejected` as the terminal error inside the
attempt-evidence envelope. It does not change
success or pass-through streaming responses. Protocol crates can keep a
request's gate, observer, cost and concurrency attribution on one API surface.

`RequestBuilder::observe_errors(callback)` observes responses with status
>= 400 before authentication recovery or retry decisions. It receives a
`FinalResponse` containing the existing capped diagnostic prefix, including
any truncation or read-failure marker, rather than an unlimited body. The same
read supplies the eventual error evidence; the body is not drained twice.
A total deadline expiring during the diagnostic drain still invokes the
observer with the buffered prefix and timeout marker, then retains the original acknowledged timeout as the terminal error alongside
its attempt evidence. Successes and 3xx responses are never observed,
preserving redirect and pass-through streaming behavior. Headers are available
before those bodies finish; the observer does not pre-drain their prefix.
Protocol observers can record a provider hold before net attempts a retry. A response that cannot be classified from the
capped prefix supplies no protocol-specific hold.

### Deadlines and the buffered ceiling

`NetConfig::connect_timeout` bounds DNS, TCP, and TLS connection setup on the
shared client and defaults to 10s. Because reqwest owns this timer, an expiry is
classified `Unsent` and a POST can be replayed safely. Per account,
`response_headers_timeout` bounds one dispatch until response headers arrive,
and `read_timeout` bounds inactivity between response body chunks. The response
header and body defaults are 10s and 30s respectively. The published
`AccountSpec::connect_timeout` remains as a compatibility input for the response
header bound; `response_headers_timeout`, when set, takes precedence.
`RequestBuilder::timeout` is the per-request override for the total request
deadline. When it is absent, `AccountSpec::request_timeout` supplies the
per-account default; when both are `None`, the pipeline invents no total
deadline. This keeps a slow response that continues making progress distinct
from a stalled response.

The total deadline is computed once before the redirect walk. Every wire
attempt receives only the time remaining, and rate-limit admission, token
lookup or refresh, response-header waits, and retry sleeps are bounded by the
same deadline. Redirects reset their per-hop retry and 401 budgets but never
the deadline. Retry attempts, the one-shot 401 recovery, redirect hops, and the
overall deadline are represented by separate budget types so their arithmetic
cannot be reset or borrowed from one another. `RequestDeadline` owns all of its
own arithmetic - no call site reads the underlying instant - because the
per-attempt-versus-overall conflation is exactly the bug the split was made to
prevent.

The deadline reaches every response-body path through the shared delivery
adapter. It bounds both hyper reads and bandwidth admission and is checked
again before each buffered slice is delivered, including after a suspended
consumer resumes. A streaming response therefore cannot hand up an
already-buffered slice past the absolute deadline.

Expiry before dispatch of the replacement attempt is `Timeout { Unsent }` and
replay-safe for any method; a `response_headers_timeout` expiry while awaiting
headers is `Timeout { InFlight }` - bytes may have gone out, so a
non-idempotent operation reconciles instead of replaying. Expiry *after* headers
arrived is `Timeout { Acknowledged }`, which converts to
`Protocol(PartialResponse)` - the same class an inactivity `read_timeout`
mid-body produces, deriving to `Retry(SameRequest)` for an idempotent operation
and `Reconcile(PartialCompletionSignal)` otherwise. The distinction is
load-bearing: a body cut short by the deadline must reach the caller as an
error, never as a clean end-of-stream, or `send()` would hand back a prefix of
the payload as though it were the whole response.

Because `read_timeout` is the only inactivity bound and it is per-account, every
body drain must apply it. Terminal statuses go through
`read_capped_response_body(response, account, deadline)`, never
`reqwest::Response::bytes()`: a server that sends 4xx headers and then stalls
mid-body would otherwise block forever, and the google and graph accounts set no
`request_timeout` to rescue it. The capped reader also stops reading at
`STATUS_BODY_CAP`, where `bytes()` buffers the whole body before any cap
applies.

The capped reader uses the same delivery adapter as success bodies. It meters
each fetched hyper chunk, then admits bounded slices against the account's
shared inbound budget. Terminal statuses, failed attempts drained before retry,
rejected-token 401 bodies and followed redirects all use this path. Inactivity
is timed only while awaiting hyper, so bandwidth waiting cannot masquerade as
a stalled peer. Diagnostic evidence distinguishes the 4 KB ceiling, inactivity
or overall timeout, and connection failure with separate visible markers.

A diagnostic drain stops after the ceiling plus one byte, leaving the remainder
of its buffered hyper chunk unpaid. That chunk was still fetched and metered;
admission measures delivered plaintext, while metering measures fetched payload.
The cap does not cover buffering below the delivery adapter.

The same reasoning is why the knobs below are not exposed upward. If a
per-deployment value is genuinely needed, it belongs on the protocol
crate's own config struct - where JMAP's `timeout` /
`accept_invalid_certs` / trusted-hosts already live, and where CalDAV
and CardDAV take theirs - not on a transport config handed to
consumers. (`GraphClient::with_account_net` is `pub` and takes an
`AccountNet`, which does force a caller using it to depend on this
crate directly. That predates the rule above and is a hole in it.)

`AccountSpec::max_buffered_response` (`DEFAULT_MAX_BUFFERED_RESPONSE`,
64 MiB) caps what `send` will accumulate, failing with
`Error::ResponseTooLarge` before the allocation that would exceed it
and dropping the stream. Every JSON API call in google / graph / jmap
takes this path, so without a ceiling a provider outage page, a
mis-routed blob URL, or a hostile response OOMs the process; the error
path already had a 4 KB cap in `read_capped_response_body` and the
success path had nothing. `None` disables the check.
`send_streaming` has no buffered-size ceiling - the caller owns backpressure
there. Its payload still goes through bandwidth admission. The CalDAV and CardDAV clients take this ceiling from `send` like
everyone else; `Error::ResponseTooLarge` is what their dispatcher converts into
the `Protocol(PartialResponse)` an acknowledged-but-unreadable DAV body wants.

The loop sends through a crate-private `Dispatch` seam. Production
dispatch delegates to reqwest. Tests install a scripted dispatcher
that accepts real reqwest request builders and returns in-process
responses or typed transport errors. This covers retry,
authentication, redirect, range, metering, and cancellation behavior
without sockets or listeners.

### The `test-support` feature

The scripted dispatcher is published to downstream crates under the
`test-support` feature, as `bifrost_net::test_support`: `Canned`
(a `Response` / `Error` / `Pending` wire outcome), `ScriptedDispatch`
(answers a script in order, records every `RequestSnapshot`), the
`canned` / `canned_with_headers` constructors, and the `scripted_net`
/ `scripted_account` builders. Consumers enable it as a
dev-dependency.

The `Dispatch` trait itself stays crate-private, deliberately. Its
signature is in terms of `reqwest::RequestBuilder` and
`reqwest::Response`, and keeping reqwest out of the public API is why
the `RequestBuilder` wrapper exists at all. What is published is the
double, not the trait.

Why it exists: account crates cannot construct a `Response`
(`#[non_exhaustive]`, no public constructor), so each one grew a
private double *above* `AccountNet` - which meant each re-derived
this crate's status contract, and a mis-derivation is invisible until
it hides a live defect. Scripting at the wire boundary instead puts
the production retry budget, `Retry-After` honor, rate-limit permit,
redirect walk, and bandwidth meter between the script and the
assertion, which is exactly the layer no consumer-side seam can
reach.

Two contract facts a consumer scripting statuses must know, both
pinned in `tests/test_support_seam.rs`:

- The retry loop turns every 4xx and 5xx into `Err` before a response
  surfaces. Only a 2xx and a passed-through 3xx reach a caller as
  `Ok(Response)`. A branch that matches a 4xx off an `Ok` is dead on
  the production path.
- An exhausted script panics rather than falling through to the
  network, so an under-scripted test fails loudly instead of dialing
  a real socket.

`ScriptedDispatch::yielding` is the same double with one yield inserted before
each answer, paired with `peak_in_flight()` for measuring a caller's fan-out
bound. The yield is what makes concurrent legs genuinely outstanding together
rather than each completing before the next is polled, and it is a separate
constructor because that interleaving change is observable - most tests want the
deterministic immediate answer. Request snapshots record the initial exact
body size hint and emitted byte count separately; the dispatcher checks their
agreement and checks explicit Content-Length against the emitted bytes. It does
not synthesize a Content-Length header. Production uses Body::wrap(UploadBody):
the exact size hint is hyper's framing input; Body::wrap_stream would lose it.
Outcomes are consumed at arrival under the snapshot lock, even when the dispatch
is later cancelled. Both DAV crates use it to pin their multiget leg
concurrency; ablating the bound reads 10 against a bound of 4.

`AccountNet` exposes `get`, `post`, `put`, `patch`, `delete`, plus
`request(http::Method, url)` for extension methods such as WebDAV's
`PROPFIND`, `REPORT`, and `MKCALENDAR`. All constructors route into the
same `RequestBuilder`; the generic method does not expose reqwest.
Builder setters of note beyond the example: `body(Bytes)` for raw
payloads (used when `json()` is not the right encoding),
`idempotent(bool)` to override the method-derived replay-safety
default (see "Replay safety" below), and
`without_bearer_auth()` for the pre-authenticated-URL and
Basic-auth flows (Gmail upload URLs, JMAP Basic), which still want
the shared retry / rate-limit / metering pipeline. The retry loop's
token-source short-circuit skips `Authorization: Bearer` injection
when `bearer_auth` is false; caller-provided headers go through
unchanged.

`AccountSpec::token_source` is optional. `Some` is wrapped in the
single-flight refresher at attach time. `None` represents an account
that does not use bearer authentication; its requests opt out with
`without_bearer_auth()`. Leaving bearer auth enabled without a token
source fails locally as `InvalidRequest` before dispatch.

`Response` and `StreamingResponse` are `#[non_exhaustive]` structs
with `status()` and `headers()` accessors; `StreamingResponse.body`
is the `ByteStream` returned by `wrap_metered`, so every chunk
feeds the per-account meter and the bandwidth-cap throttle.

### Retry decision (RetryPolicy)

- 2xx -> return. 3xx is classified by the in-crate redirect loop
  (reqwest itself never follows: `Policy::none()` is installed at
  client construction). A surfacing 3xx means the caller disabled
  `FollowRedirects`, the classifier passed it through (304/305/306,
  or a followed status with no `Location`), or the hop limit errored.
  Conditional-request callers rely on the headers being exposed.
- 4xx not in `policy.statuses`, not 401 -> terminal
  `Error::Status`.
- 401 -> force refresh once; second 401 -> `Error::AuthLost` with
  `transmission_state: Some(Acknowledged)` and preserved 401 response
  evidence. The 401-recovery budget is **separate** from the network
  retry budget: a 401 retry no longer burns one of the `max_attempts`
  network attempts.
- 5xx or in `policy.statuses` -> retry with backoff. Honor
  `Retry-After` (delta-seconds + RFC 9110 HTTP-date via
  `httpdate`), capped by `policy.honor_retry_after_cap` (single
  source of truth). Without `Retry-After`: exponential backoff
  with **proportional** jitter (0..capped), capped at
  `policy.max_backoff`. Jitter entropy comes from the workspace
  UUID RNG (`Uuid::new_v4`), not `SystemTime::now`, so a clock jump
  cannot influence the wait.
- 429 -> same retry path; `Retry-After` honored.
- Network / timeout / decode errors -> retry if
  `policy.network_errors && attempt < max_attempts` **and the failure
  is replayable**. See "Replay safety" below.
- Past `max_attempts` -> `Error::RetryBudgetExhausted` (or
  `Error::RateLimited` on 429). The exhausted-attempt branch parses
  the *final* response's `Retry-After` and pushes it onto
  `retry_after_history` before surfacing, so consumers see the
  server's last hint.

### Replay safety

The transport-failure retry branch consults idempotency;
`transport_error_is_replayable` allows a retry when EITHER nothing was
transmitted (`TransmissionState::Unsent`) OR replaying cannot change
server state. Past `Unsent` on a non-idempotent request the side effect
may already have landed. A remote HTTP/2 `REFUSED_STREAM` reset guarantees
the stream was not processed (RFC 9113 section 8.7), so `send_error_to_error` classifies it
as `Network { Unsent }`. The retry loop rebuilds a fresh upload body through
admission on each attempt; reqwest cannot clone the wrapped body for its own
protocol retries. Other non-connect, non-timeout reqwest failures are
`Network { InFlight }`, preserving that uncertainty. Retrying regardless is how one Gmail
`/messages/send` becomes two delivered messages and one JMAP
`Email/set` create becomes two drafts, and it happened three times
before `into_account_error` ever ran, so the `Reconcile` classification
only ever described the final attempt.

Idempotency defaults to `reqwest::Method::is_idempotent()`: safe
methods plus PUT and DELETE are replayable, POST / PATCH / extension
methods are not. `RequestBuilder::idempotent(bool)` overrides it. The
override exists for two real shapes: a read carried over POST (JMAP's
single `/jmap/api` endpoint, when the payload's method calls are all
`*/get` or `*/query`) opts back in, and a GET a provider treats as an
action opts out. The JMAP transport does not set it today - its `send`
sees only method, URL, and opaque body bytes - so JMAP POSTs are
conservatively non-replayable at this layer.

This is not a net loss of resilience. The refused replay surfaces to
`into_account_error`, which holds the caller's real `AccountOperation`
and routes `InFlight` + idempotent to `Retry(SameRequest)` and
`InFlight` + non-idempotent to
`Reconcile(TransportDropAfterSend, [CheckTarget])`. The retry decision
moves up to the layer that knows what the request meant, instead of
being taken three times by the layer that only knows it is holding
bytes.

Status-driven retry (5xx, 429, `policy.statuses`) is deliberately NOT
gated on idempotency. A complete response is `Acknowledged` evidence
that the server is reporting it did not do the work, and
`recovery::derive`'s `transient_retry_or_reconcile` already treats
`Acknowledged` as replayable for any operation. Gating it here would
contradict the shared error model and turn every 503 on a POST into an
immediate failure.

Token-source failures returned from `current()` are passed through
to the caller unchanged: `Error::AuthLost` for true auth failures,
`Error::RefreshFailed { retry_after, source }` for transient
`Network`/`Timeout`/etc. failures (preserving the inner classification
for retry-vs-give-up decisions).

`policy.statuses` is the additive retry set on top of unconditional
5xx; explicitly documented to avoid double-counting.

The rate-limit slot is **refunded on every retried failure**, not
just 429-with-Retry-After, so 5xx retries don't burn quota for work
the server didn't perform.

Each acquired slot is held by an armed guard until a response arrives. The debit
carries the bucket generation returned by `acquire_generation`; both guard-drop
and acknowledged-response refunds require that generation, so detach and reopen
cannot credit the replacement bucket.
Dropping or cancelling the request future before server
acknowledgement refunds the slot automatically. Response-backed retry
and redirect paths retain their explicit refund rules.

## OAuth single-flight (`OAuthRefresher`)

State machine: `Empty` -> `Refreshing` -> `Fresh`. The first
`token()` call from `Empty` transitions to `Refreshing` and drives
the network refresh itself; concurrent callers register a
`oneshot::Sender` in `Refreshing.waiters` and await. On success the
driver transitions to `Fresh { token, refreshed_at }`. On failure
the driver fans `Arc<Error>` clones to every waiter (because
`Error` is not `Clone`).

`Error::RefreshFailed { retry_after, source: Arc<Error> }` preserves
transient-vs-permanent: `Network`/`Timeout` survive as themselves;
true auth failures (token-endpoint 401/403) map to `AuthLost`.
OAuth 429/503 `Retry-After` hints are projected to the wrapper's
absolute deadline.

A waiter whose driver drops the sender without answering - task
cancelled, panicked, or the runtime shutting down - also gets
`RefreshFailed`, with `Error::Cancelled` as the preserved source. That
event says nothing about the credential, so classifying it as
`AuthLost` (which `auth_lost` maps unconditionally to
`Authentication(ReauthorizationRequired)` and thence to the terminal
`RecoveryClass::AuthLost`) told the engine a shutdown race meant the
user must re-authorize. `RefreshTransient` into
`Retry(AfterAuthRefresh)` is what a lost driver warrants.

When a proactive refresh displaces a cached token with a known expiry,
`Refreshing` retains that token as a fallback. A transient refresh
failure restores and returns it if it is still valid, and defers the
next proactive attempt for 30 seconds so an endpoint outage does not
cause one refresh call per request. Token expiry overrides that delay.
Forced refreshes after a target 401 and terminal token-endpoint
authentication failures never use the fallback.

The 401 path passes the token used by that request back to the refresher. A
forced refresh starts only if that token is still cached. If another in-flight
request already replaced it, the newer token is returned without another
issuer call. The refresh driver is spawned independently of the requesting
task, so cancellation of the request that first observed 401 neither poisons
the state nor strands waiters.

A failure with no usable fallback enters an error backoff. Calls during
the quiet interval return the shared typed failure without contacting
the issuer; at the boundary one caller drives the next single-flight
attempt, and any success resets the escalation.

The interval depends on what failed. A transient failure starts at one
second and doubles per consecutive failure to a sixty-second ceiling, so
recovery from a blip costs at most one second of added latency while a
long outage settles at one issuer call per minute per account rather
than one per second. A failure the issuer answered authoritatively - a
401 or 403 from the token endpoint, or an `AuthLost` the source raised
itself - takes the sixty-second interval immediately: re-asking cannot
help, and hammering an IdP that is refusing is how an account gets
throttled or blocked there. Forced refreshes are subject to the same
quiet interval; the state, not the caller, decides.

Note what this does and does not promise. It bounds the issuer call
RATE, not the number of failing requests: every request during the
interval still fails, it just fails locally off the cached error.

`AccessToken` wraps `Zeroizing<String>`; `Debug` redacts the bytes
and surfaces only length + expiry. `AccessToken::from_zeroizing` moves an
existing zeroizing allocation into the wrapper without creating a plain
`String` copy, for protocol convenience constructors that already own secret
storage.

### Proactive-refresh window

- Tokens with an issuer-supplied `expires_at`: refresh 60 s before
  the deadline.
- Tokens without `expires_at` (opaque, no TTL hint): refresh when
  the cached token is older than the refresher's `max_age`. Default
  is `DEFAULT_TOKEN_MAX_AGE` (55 min, leaving a 5 min margin under
  the typical 60 min issuer TTL). Callers configure the value at
  construction via `AccountSpec::token_max_age`;
  `Net::attach_account` reads it and wires it into the per-account
  refresher with `OAuthRefresher::with_max_age`, so production
  callers do not touch the builder directly. The previous behaviour
  treated opaque tokens as fresh indefinitely and waited for a
  server 401; the max-age fallback closes that latency leak.

Every instant the refresher compares is on the **tokio clock**.
`RefreshState::Fresh::refreshed_at` and `refresh_not_before` are
`tokio::time::Instant`, matching `Backoff::retry_at`, and the
issuer-supplied expiry is lifted with `tokio::time::Instant::from_std`
at the point of comparison (`token_expiry`). `AccessToken::expires_at`
stays a published `std::time::Instant` - callers mint tokens from
whatever clock they have, and the two types share a timeline, so
production behavior is identical. The reason for the migration is
testability: with the refresher on `std::time::Instant` a paused-time
test could drive the failure backoff but not the proactive-refresh or
max-age windows, because `tokio::time::advance` moves only the tokio
clock and real time barely moves inside a test. Both windows are now
drivable exactly the way the rate governor and byte bucket already are.

**Do not nest `OAuthRefresher`s.** Wrapping one refresher around
another causes the outer's `force_refresh` to bypass the inner's
single-flight. Compose against the raw `TokenSource` instead.

### `StaticTokenSource`

In-memory `TokenSource` for callers that hand bifrost already-minted
access tokens and rotate them out-of-band (e.g. an external auth
service pushes a fresh token periodically). Backed by
`Arc<RwLock<AccessToken>>`; `set(AccessToken)` replaces the cached
token under the write lock, `token()` snapshots the current value,
and the trait's `refresh()` is the same as `current()` because there
is no refresh material inside bifrost-net for this shape. The
refresher above wraps a `StaticTokenSource` just like any other
provider; rotation just means the next `current()` reads the
swapped-in token.

## Rate-limit governor (`RateLimitGovernor`)

Buckets are keyed by `(host, quota_scope)`. `RateLimit::new` leaves the scope
empty for host-only compatibility; provider clients use `with_quota_scope` for
tenant or user quotas. `acquire(host, cost)` remains the empty-scope
compatibility path.

Selection carries the same two components as the key. Each request resolves a
scope before its first wire attempt and again at every redirect hop, since a hop
may land on a different host. `RequestBuilder::quota_scope(scope)` names it
explicitly and wins for every hop of that request; without it the request uses
the scope the account declared for the host. Keying registration by the pair
while selecting by host alone was a half-wired dimension: an account declaring
two scopes on one host registered two buckets, refcounted both, and could only
ever debit one - the other was live and unreachable. `attach_account` builds the
per-host default from the keys that actually REGISTERED (so a rejected
declaration cannot point requests at a bucket that does not exist), takes the
first declaration for each host, and `tracing::warn!`s when a host carries more
than one, naming `quota_scope` as the way to reach the rest. An account that
talks to one host under two quotas - the Gmail per-project and per-user shape -
must say which per request; the declaration order cannot know.

Scoped admission joins a FIFO ticket queue. Only
the head may debit; admission wakes its successor, a refund wakes the
head, and cancellation removes its ticket and hands off if necessary.
Each waiter holds its own `Notify`, so every wake is addressed rather
than broadcast.

A ticket names a bucket INSTANCE, not a host: each bucket carries a
`generation`, and `next_waiter_id` restarts at zero for a new one.
Removing the final host registration drops the bucket and wakes every
queued waiter; a waiter whose ticket generation no longer matches the
bucket sitting under its host - or whose ticket is simply no longer in
that queue - completes as unmetered. Host-name matching alone stranded
a woken waiter permanently when another account re-registered the same
host before it resumed, which is ordinary detach/open churn. The
cancellation guard is generation-scoped for the same reason: it must
never evict a recycled ticket id belonging to the replacement bucket.
The head polls refill at no more than 250 ms intervals. Refill
timestamps use `tokio::time::Instant`, so paused-time tests advance
deterministically.

`cost > burst` short-circuits to `Error::CostExceedsBurst` instead
of hanging: the bucket can never fill that high. Configuration bug,
not a runtime condition.

Unregistered hosts are unmetered (no-op `acquire`).

For a previously unregistered host, registration rejects non-finite,
zero, and negative `quota_per_second` values, plus `burst == 0` and
`burst < cost_default`, with a warning and
returns `false`. `Net` records only successful registrations in the
attachment token's host set, so a rejected account cannot later
unregister another account's valid bucket. A duplicate declaration,
even an invalid one, joins the first valid bucket, increments its
attach count, and returns `true`. Invalid quota configuration
therefore cannot panic, spin, park forever, or unbalance teardown.

### Cost defaults

Each `RateLimit::cost_default` is stored on the scoped host bucket at
registration. `RequestBuilder::send_streaming_inner` consults it via
`RateLimitGovernor::cost_default_for_scoped(host, scope)` - against the same
resolved scope the debit will use - when the builder did not call `.cost(n)`
explicitly; the precedence is
`RequestBuilder::cost` > host `cost_default` > 1.

### Duplicate registrations

The first registration of a `(host, quota_scope)` key wins. A subsequent
`register(RateLimit { ... })` for the same key with a different
`quota_per_second`, `burst`, or `cost_default` is **not** silently
dropped: the governor logs a `tracing::warn!` and keeps the
existing bucket. Per-host attach counts are incremented on every
`register` so `detach_account` can decrement symmetrically.

## Concurrency limit (`ConcurrencyLimit`)

The rate-limit governor meters request RATE; `ConcurrencyLimit` bounds admitted
request demand. Its cloneable handle chooses the scope: one account, several
accounts, or selected requests. `AccountSpec::concurrency_limit` contributes a
weight-1 claim to every request. `RequestBuilder::concurrency_limit` ADDS a
weight-1 claim, and `concurrency_claim(limit, weight)` adds weighted demand.
Claims on the same identity are summed with checked arithmetic before any
acquisition; overflow returns local `Error::InvalidRequest`. Zero demand is a
caller bug, debug-asserted and otherwise raised to one. Distinct limits are
acquired in increasing process-unique identity order to avoid lock-order cycles.
Cancellation or deadline expiry drops all claims acquired so far.

Each limit has one mutex protecting capacity, admitted demand, and a FIFO
waiter queue. Only the head is considered, deliberately blocking cheap followers
behind expensive heads. A head fits if its demand is within available capacity;
an oversized head can instead run exclusively when admitted demand is zero.
Admitted demand never exceeds capacity except for one exclusive oversized claim
or while previously admitted work drains after a shrink. The exclusive claim is
recorded at its full demand, so growing cannot expose fictitious capacity.
Growing and releasing pump the queue; shrinking revokes nothing and needs no
debt. A zero capacity is raised to one with a warning.

Admission is FIFO, but granted waiters may resume in any order. A granted future
that is neither polled nor dropped reserves capacity. Cancelling a queued future
removes it; cancelling an unpolled grant releases its full demand. Transfer to
an owned guard happens synchronously under the mutex. Wakers are collected
under the mutex and woken or dropped after unlocking.

Claims are acquired once before the first wire attempt and held across retries
and redirect hops until the response body ends. The request's total deadline
bounds admission and expires as `Timeout { Unsent }`. `send` releases on return;
`send_streaming` and `download_stream` carry the owned guard collection in the
body stream until it ends, fails or is dropped. Keeping a streamed body open
while awaiting more work on the same limit can deadlock.

`AccountNet::shared_concurrency_limit(key, limit)` accesses a registry on its
owning `Net`, shared across that Net's accounts, never process-global. Entries
are weak: the same live key returns the same handle, and after every handle and
claim drops the key gets a fresh identity. Dead entries are pruned on insert.
A live entry keeps its existing capacity when a lookup asks for another value,
with a warning naming the key and both capacities; resize only via `set_limit`.

JMAP gates its API POSTs by the session's `maxConcurrentRequests` using one
per-request weight-1 claim and no account claim, so its behavior is unchanged.
Uploads and downloads remain separately scoped per RFC 8620.

## Bandwidth meter

`BandwidthMeter` holds one `AccountCounters` per account; each
counter wraps a `RateWindow` (10 one-second buckets, ring buffer).
`AccountMeter::observed_bps()` returns total bytes over the
trailing 10 seconds.

Outbound metering records `body.len()` when each attempt is handed to
reqwest. Headers are excluded by design. This is an attempted-payload
estimate, not an on-wire byte counter: reqwest does not expose the
point at which an in-memory body is written, so an `Unsent` DNS/connect
failure can count bytes that never reached the wire. Moving the count
after dispatch would instead miss in-flight failures and cancellation
after a partial write. Inbound metering is per-chunk on every response
body the transport reads, including buffered and streaming successes, terminal
errors, retry drains, 401 recovery, and followed redirects. The cumulative
meter remains account-scoped. Buffered `Response` also exposes request-local
`bytes_in` and `bytes_out`; `StreamingResponse` exposes the outbound total plus
a cloneable `RequestByteCounter` that advances as its body is drained. These do
not sample cumulative counters, so concurrent requests cannot contaminate one
another.

One counter is created before the retry and redirect loop, so error-body drains,
401 recovery, followed redirects, retries and the final success body all
contribute to the same figure. Note the asymmetry the two shapes carry: a
buffered `Response::bytes_in` is FINAL, because `send` drained the body before
returning it, while a `StreamingResponse`'s counter is only as complete as the
caller's draining. A caller that abandons a stream part-way holds a partial
count, and the accessor documents that rather than pretending otherwise.

`Response::bytes_in` exists only on the success path, but the bytes do not:
non-2xx bodies, exhausted retries and repeated 401s are drained, metered and
throttled before they become an `Error`. `RequestBuilder::count_bytes_into`
takes a caller-owned `RequestByteCounter` and makes it the request's counter, so
the caller can read the figure back after an `Err`. That is the counter every
protocol-crate wire funnel records from, because the funnel's callers turn a
failed request into per-item failures and still emit a batch; recording from the
`Response` would report zero for exactly the batches that hit trouble.

Above this, each protocol crate owns a batch-scoped accumulator - `ByteTally` in
bifrost-google, bifrost-graph, and bifrost-jmap - because an engine batch
routinely covers several requests: a list page plus a hydration fan-out, a
`$batch` submission plus its etag preflight, an `Email/set` plus its state
probe and post-`stateMismatch` retry. A metered client handle is one `Arc` bump
over the same transport and reports every buffered response into one stream's
accumulator; each emitted batch takes and clears it, so consecutive batches
partition the traffic rather than each restating a running total. Deliberately
NOT a delta across the cumulative account meter: that meter is shared by every
concurrent request on the account, so a delta would attribute another scope's
traffic to this batch.

### Bodies abandoned before they are read are not metered

"Every body the transport reads is metered" is exact, and the word doing
the work is *reads*. Two returns deliberately drop a response body
without reading it, so those bytes appear in no counter and no bandwidth
window:

- `Error::ResponseTooLarge` (`read_capped_response_body`,
  `download_stream`) - the whole point is to stop reading rather than
  buffer gigabytes, so metering them would require doing the thing the
  cap exists to prevent.
- `Error::RangeNotHonored` - the body is usually the full resource the
  caller declined to fetch, and it is rejected before any byte is yielded
  so the caller never assembles a misaligned blob.

Both also mean the connection is likely torn down rather than returned
to the pool, unlike the error drains above, which read the body
precisely so the connection survives. This is an accepted asymmetry, not
a hole: the account meter is a payload-sizing tool, and both shapes are
terminal errors whose bytes the caller never receives. Do not re-file it
as missing coverage.

### OAuth issuer traffic is an explicit exception

Token-endpoint traffic driven by `OAuthRefresher` is not transport traffic owned
by bifrost-net. `TokenSource` is shared by HTTP, IMAP, and SMTP and deliberately
owns arbitrary provider exchange machinery. Bifrost-net therefore cannot count
or cap its wire bytes without replacing that provider abstraction with an
HTTP-specific request model. Issuer traffic is neither metered nor capped;
target API traffic remains fully metered and capped.

`AccountNet` caches its `AccountMeter` at attach or retag time, avoiding
an account-id allocation and meter-map lookup on each request attempt.
Meter registrations are counted per `AccountId`; one detach cannot
remove the live entry of a concurrent reopen. After the final detach,
an already-cached handle retains a frozen counter snapshot while new
lookups return zero.

`MeterSink` trait lets IMAP/SMTP (which bypass this crate's
transport) feed `record_bytes_in` / `record_bytes_out` into the
same meter when the engine wires them. `MeterSinkHandle` is the
account-scoped adapter raw-socket transports compose around a
`MeterSink`: it owns the `AccountId` so the connection-level call
site does not have to thread the id alongside every byte count.
`MeterSinkHandle::from_meter` wraps the process-wide
`BandwidthMeter` directly; the trait-object form
`MeterSinkHandle::new` accepts any `Arc<dyn MeterSink>` for test
doubles. Both the direct HTTP meter and `MeterSink` are lookup-only:
recording against an unknown or detached account is a no-op and never
re-registers it.

### Bandwidth cap

`BandwidthBudget` is a cloneable Arc handle holding one cap and two independent
fixed-slot GCRA schedules, inbound and outbound. `None` means unlimited;
`Some(0)` becomes `Some(1)` with a warning. Every reservation is bounded by the
requested length, one second of the cap and `MAX_OFFER` (256 KiB). Unlimited
offers are also bounded by `MAX_OFFER` and tracked until finished or dropped.

This is a PAYLOAD cap at the protocol plaintext layer: HTTP bodies and stream
bytes above TLS. TLS, HTTP headers and bytes buffered below the adapter are
outside it. It is a smoothing cap, not a wire-rate guarantee. Per direction,
for any window of length T seconds during which the cap c is constant and
finite:

```text
bytes <= c * (T + 1) + A
```

A is every unspent allowance outstanding at the window start: queued, ready or
offered reservations, including earlier-cap grants at their issued size and
outstanding unlimited offers. There is no bound across unlimited intervals.
Each sequential adapter operation holds at most one outstanding admission.
Stream adapters that reserve before a pending read pre-spend that slot: an idle
read can waste capacity but never blocks siblings. HTTP reserves only after it
has a hyper chunk to deliver.

A finite reservation advances the theoretical arrival time by its issued
bytes/cap duration, rounded up to a nanosecond. It is ready when that time is
within one second of now. An issued slot never moves. Dropping before offer, or
finishing a short successful I/O, pulls the tail back only when the reservation
id is still the tail in the current epoch. A non-tail cancellation refunds
nothing; a dropped offered I/O refunds nothing. A finite-to-finite cap change
prices only subsequent reservations; earlier slots keep their times and sizes.
Lifting to unlimited clears the schedules and advances the epoch; imposing a
cap advances it again with empty schedules. Waiters select on their ready time
and a watch of (cap, epoch), discard stale reservations and inspect the current
policy again. Installation and epoch validation use the same mutex. Setting an
unchanged cap is a no-op, including its watch notification. All admission times
use the Tokio clock.

`MeteredStream<S>` wraps an `AsyncRead + AsyncWrite + Unpin` stream with a
shared budget. Its read and write lanes each retain at most one admission
future or offered reservation across Pending. Inner I/O receives at most the
grant; success finishes with the actual byte count, including zero at EOF or
a zero write. Short success refunds only the current tail. Errors and dropping
an offered operation conservatively retain its charge. Flush and shutdown
pass through. Uncapped admission returns immediately through the budget's
existing fast path, without a timer wait.

The boundary is above S: wrapping TLS meters plaintext, including framing
above the wrapper. The JMAP WebSocket adapter therefore includes frame headers
and the HTTP upgrade exchange, unlike HTTP body admission. No observation sink
is attached by this adapter. It installs no peer timeout. Admission precedes
polling inner I/O, so a timer implemented inside S starts after admission;
a caller's timeout around the entire adapter includes admission waiting.

`BandwidthRegistry` is an injected Arc-shared AccountId-to-Weak map. Lookup and
insert share one lock. Final budget teardown
removes its own entry only if the pointer still matches, preserving any
replacement. `Net::with_bandwidth_registry` injects it;
`Net::bandwidth_registry` exposes it. `Net::new` creates a fresh private
registry. `AccountNet` gets its budget at attach, and its cap accessors use that
shared budget. Retag uses the destination id budget for future admission;
outstanding grants remain attributed to the source.

The consumer must wire ONE registry into every Net, the engine and the IMAP
factory. Matching ids in different registries do not coordinate. Stream
transport migration is separate from this HTTP implementation.

All HTTP response paths use one delivery adapter: buffered and streaming
successes, downloads, status diagnostics, redirect drains and pre-retry
discards. It holds the current hyper chunk, lazily admits bounded slices, keeps
its pending admission across polls and rechecks the request deadline before
delivery. Diagnostic drains stop admission at their prefix limit; the remainder
is unpaid. Metering still records fetched chunks before admission.

Each request attempt and body-preserving redirect gets a fresh `UploadBody`, an
`http_body::Body` whose exact remaining size hint preserves Content-Length
framing. It splits Bytes into bounded slices and reserves before yielding.
Cancellation before yield refunds only a current tail; after yield the charge
stays, because this is admission rather than transmission evidence. Retries pay
again. The response-header timer excludes request-body admission waits, while
the absolute request deadline continues to run. Response body inactivity timers
arm only while awaiting hyper, never while awaiting admission. An exhausted
response-header active-time budget expires even when the next dispatch poll
begins upload admission. Admission pauses only unspent time.

`RequestBuilder::without_timeout()` suppresses an account default total deadline
while retaining header and body inactivity bounds. JMAP uses it for long-lived
EventSource responses; ordinary JMAP requests keep the configured timeout.

## Range fetches (`download_stream`)

When a `Range` was requested, the response **MUST** be
`206 Partial Content` with a matching `Content-Range`; `200 OK`
returns `Error::RangeNotHonored` (the server collapsed the range,
so the bytes don't match the request). Bare requests accept
`200 OK` as before.

For a closed request whose end exceeds the resource length, a legal
shortened response is accepted when its end is exactly the known
resource tail. A shorter window before the resource tail remains a
`ContentRangeMismatch`.

### Open-ended `Content-Range` validation

For an open-ended request (`bytes=N-`), the response's
`Content-Range` is validated against the tail of the resource:

- `bytes N-M/T` with known total: `M == T - 1` (the response must
  cover bytes N through the final byte of the resource).
- `bytes N-M/*` with unknown total: any `M >= N` accepted (we
  cannot verify without the total).

The previous open-ended check accepted any `M >= N` even with a
known total, which silently admitted truncated bodies.

### Zero-length and overflowing ranges

- `ByteRange { length: Some(0), .. }` short-circuits in
  `download_stream` to an empty `ByteStream` without touching the
  network or the meter. The encoder also rejects `Some(0)` as
  `Error::RangeNotHonored { kind: LocalInvalid, message:
  "zero-length range" }` so hand-rolled callers see the error path.
- `start + length` overflow is rejected up front by `encode_range`
  with `Error::RangeNotHonored { kind: LocalInvalid, message:
  "range overflow: ..." }` rather than silently emitting
  `bytes=N-u64::MAX`.

## TLS

`native-tls` only. No `rustls` dep anywhere. `NetConfig` accepts a
`Vec<native_tls::Certificate>` for additional root certs and a
`dangerous_accept_invalid_certs` flag for self-signed fixtures.
`NetConfig::with_root_cert(cert)` is a builder helper that pushes
one `native_tls::Certificate` onto `root_certs` and returns `self`
so callers can chain.

These settings belong to the shared reqwest client and therefore cannot vary
per account. They do, however, decide *which* shared client an account attaches
to. `Net::shared_for_tls(accept_invalid_certs)` returns `shared_default()` or
`shared_accepting_invalid_certs()`, each a process-wide `OnceLock`, so sharing
is preserved within each trust class and at most two clients exist - not one per
account.

JMAP's protocol-level `accept_invalid_certs` routes through that selector, so it
governs HTTP as well as the separately built WebSocket transport. The
alternative - accepting the flag and discarding it for HTTP - was tried and is
wrong: a self-signed deployment failed every HTTP request while its WebSocket
succeeded, so the option appeared to work and not, which is worse than either
honoring it or refusing it outright.

This is not new configuration exposure through the protocol-level `Account`
API. `accept_invalid_certs` was already public on the JMAP factory; what changed
is that it now does what it says.

`AccountSpec::follow_redirects` is a `FollowRedirects` enum with two
variants: `Disabled` and `Enabled(RedirectPolicy)`. The default is
`Enabled` with an empty trusted-host allowlist and a ten-hop maximum.
Regardless of the variant, `Net::new` always installs
`reqwest::redirect::Policy::none()` on the underlying HTTP client -
`bifrost-net` owns the redirect loop unconditionally so RFC 7231
section 6.4 method rewriting, the trusted-host allowlist, and
`Authorization`-stripping on cross-host hops happen exactly once
and the same way for every HTTP protocol crate.

### Redirect loop (`redirect.rs`)

Inside `send_streaming_inner` the retry loop classifies every 3xx
response through `classify_redirect`:

- 304 / 305 / 306: `PassThrough`. Status + headers surface to the
  caller; conditional-request flows (`If-None-Match` -> 304,
  Graph's 304 on `$delta`) keep working.
- 301 / 302: rewrite to GET and drop the body when the prior method
  is not safe (POST / PUT / DELETE / PATCH); preserve method + body
  for GET / HEAD.
- 303 See Other: rewrite to GET and drop the body unconditionally.
- 307 / 308: preserve method and body.
- Any followed-redirect status whose `Location` header is **absent**:
  `PassThrough`. A redirect no one can follow is handed back as a
  terminal status the same way a 304 is. This is load-bearing for
  Google Drive resumable uploads: a `308 Resume Incomplete` carries a
  `Range` header and no `Location`, and the cloud chunk loop reads that
  status + `Range` itself. A present-but-malformed `Location` (invalid
  encoding / unresolvable) stays a hard `MalformedRedirect`; only the
  missing header passes through.

A passed-through 3xx hands its BODY up as well as its status and headers,
through the same `into_byte_stream` the redirects-disabled arm uses. The arm
used to drop the response and substitute an empty stream on the reasoning that
304/305/306 carry nothing interesting - which is true of those three and of
Drive's header-only 308, all of which simply yield an empty stream on their
own. It is not true of the fourth shape folded into this arm: a followed status
with no `Location` can carry a real explanatory document, and discarding it made
those bytes invisible to the caller AND to the request-local byte counter, which
counts only what a body reader actually reads. Whether a passed-through body is
interesting is the caller's decision, not the redirect loop's.

`same_origin` - the `keep_auth` / cross-host decision - compares all
three RFC 6454 origin components: scheme, host (case-insensitively, per
RFC 3986 section 3.2.2), and `port_or_known_default`. Scheme was previously
uncompared on the reasoning that the pipeline only issues `https`;
nothing enforced that, and an `https` -> `http` downgrade was
classified cross-origin only because the default ports happen to differ
(443 vs 80), so a downgrade spelled `http://h:443/` would have carried
the bearer onto a cleartext hop.

`RedirectPolicy::trusted_hosts` is an allowlist for cross-host hops:
empty means every host is acceptable; populated means only matching
hosts are admitted (case-insensitive host comparison per RFC 3986
section 3.2.2) and `Error::RedirectRejected` is returned otherwise.
`Authorization` headers are stripped on every cross-host hop
regardless of allowlist membership, covering both the
token-source-derived bearer the pipeline injects in `build_reqwest`
and any caller-set `Authorization` header sitting in the request's
`HeaderMap` (JMAP Basic-auth, custom HMAC schemes, etc.). The strip
runs in the redirect loop after `classify_redirect` flags
`keep_auth: false`, so a credential never travels to a host the
original request did not target. `max_hops` (default 10) caps the
chain; the pipeline uses a widened counter so even `max_hops = 255`
terminates on hop 256 with `Error::RedirectLoop`. Each redirect hop
resets the retry counter to 0 - hops are fresh logical requests, not
retries.

The admissibility decision for one hop is `RedirectPolicy::admits_hop(prior,
next)`, the single rule `classify_redirect` applies: a same-origin hop
(RFC 6454 scheme + host + port, the `same_origin` comparison above) is
always admitted, and only a hop that leaves the origin consults the
allowlist. A departing URL that is unknown is treated conservatively as
cross-origin.

There was once a second implementation of this rule - a
`RedirectPolicy::reqwest_policy()` builder returning a
`reqwest::redirect::Policy::custom` for callers assembling their own
`reqwest::Client`. It drifted (it checked every hop's target host rather
than only cross-origin ones, so a populated allowlist that omitted the
origin's own host stopped a plain same-host redirect there while the
pipeline followed it), and a reqwest `Policy` could not rewrite methods
or strip headers anyway. It was deleted on 2026-09-04 on the repository
owner's ruling; `admits_hop` is now the only implementation. The DAV crates need their redirect gate to
match their stricter credential gate, which compares scheme, host, and
effective port - and this crate strips `Authorization` on any origin change
with no way to restore it, so a followed cross-origin hop would arrive
unauthenticated. They therefore attach with `FollowRedirects::Disabled` and walk
every hop themselves in `DavDispatch::send_raw_request`, re-minting credentials
for each target origin against the set authenticated discovery admitted. The hop
cap they apply is the exported `DEFAULT_MAX_HOPS` constant, read by name rather
than through `RedirectPolicy::default().max_hops`: DAV carries no per-account
redirect config, and laundering the cap through a type `Default` hid the
coupling, so changing that default silently changed DAV behavior.

### A 401 on a hop the transport disarmed itself

`keep_auth` ANDs monotonically across the chain (`auth_for_next_hop =
step.keep_auth && auth_for_next_hop`): once a hop leaves the origin the
credential is gone and is never restored, which is the correct security
posture. It also means a chain that comes back - `A -> B -> A`, the CDN
bounce - arrives at its own origin unauthenticated, and the caller-set
`Authorization` header was removed from the `HeaderMap` on the same hop.

A 401 answered on such a hop is therefore not evidence about the
credential: the credential was never presented. The pipeline tracks that
provenance (`auth_self_stripped`, set when the walker removes a
credential the request was actually carrying) and returns
`Error::UnauthenticatedRedirectHop { message, final_response }` instead
of letting the 401 fall through to the terminal-4xx branch. That branch
mints `Error::Status { code: 401 }`, which `into_account_error` maps to
`Authentication(ReauthorizationRequired)` and thence to terminal
`RecoveryClass::AuthLost` - telling the engine to send the user to a
re-authorization prompt for an account whose token is fine.
`UnauthenticatedRedirectHop` classifies as
`Protocol(ContractViolation)` -> `RecoveryClass::ProviderContractViolation`,
carrying the rejecting hop's status, headers and body as diagnostics. A
401 on a request that really did present its credential is untouched and
still reaches `AuthLost` through the normal one-shot-refresh path.

### A per-request cost does not cross a host boundary

`RequestBuilder::cost(n)` is denominated in the quota units of the host
the caller addressed - Graph counts requests, Gmail counts quota units,
an unregistered host counts whatever it counts. `cost_override_for_hop`
drops the override when a redirect hop changes host (case-insensitively)
so the hop re-resolves the new host's registered `cost_default`, which
is exactly what the no-override path already did per hop. A same-host
hop keeps the override: same bucket, same units. Without this, an
override larger than the new host's `burst` failed the request outright
with `Error::CostExceedsBurst` on a cost the caller never applied to
that host. `quota_scope` is unaffected - an explicit
`RequestBuilder::quota_scope` is a deliberate cross-hop instruction and
still wins for every hop.

`FollowRedirects::Disabled` skips the loop entirely; 3xx surfaces
to the caller exactly as it did before the loop landed. Redirects are
disabled in reqwest for every `Net`; bifrost-net follows them itself because
the reqwest policy cannot rewrite methods, strip headers, or carry a different
trusted-host allowlist for each account sharing the client.

`RequestBuilder::follow_redirects(FollowRedirects)` is the per-request
override of the account's policy (`#[must_use]`, additive; unset uses
`AccountSpec::follow_redirects`). `Disabled` hands every 3xx back to that
one request's caller as a terminal response, status, headers and body,
and sends nothing to the `Location`; `Enabled(policy)` substitutes `policy`
for the account's for that request's walk. The account's own policy is
untouched for every other request. It exists for URLs admitted by a rule
the shared walk cannot apply to a hop, the standing case being an
anonymous provider-minted upload session URL (the Google Drive resumable
session and the Graph OneDrive upload session answer chunk PUTs on one):
a 307 or 308 there must not be walked through the shared redirect logic,
which was never checked for where such a hop can lead or for an http
downgrade, and would replay the chunk body to it.

`Net::new` returns `Error::InvalidRequest` for bad TLS / client
configuration data: corrupt native-tls root cert DER, reqwest
rejection of the re-encoded DER, or client-builder failure. These
route to the account error model as `Request(Malformed)`, not as a
retryable transport setup failure.

## traceparent

W3C `traceparent` injected on every outbound request. Span id is
read from `tracing::Span::current()` when available; trace id is
freshly minted via `uuid::Uuid::new_v4` per request as a stopgap.
Full trace propagation (reading trace_id from the current
`tracing-opentelemetry` context) is a follow-up.

## Error model

`Error` is `#[non_exhaustive]` and `thiserror`-derived. Notable
variants:

- `AttemptHistory { source, attempts, may_have_committed }` - terminal
  stage failure together with earlier wire outcomes. The complete error feeds
  classification; `terminal()` is a diagnostic view of the failing stage.
- `LocalHold { key, held, attempts, may_have_committed }` - registry gate
  refusal with the same accumulated transmission evidence.
- `Network { message, transmission_state, source }` - transport-level
  failures (DNS, TCP reset, generic transport). `transmission_state`
  is `Unsent`, `InFlight`, or `Acknowledged`; acknowledged network
  body failures convert to `Protocol(PartialResponse)`, never
  `Transport(_)`.
- `Timeout { transmission_state }` - per-request deadline, with the
  same transmission-state evidence.
- `Tls { message, transmission_state }` - TLS handshake or certificate
  validation failure. Handshake failures are `Unsent`; synthetic
  acknowledged TLS values are defensively converted as partial
  responses.
- `Status { code, body, headers }` - terminal non-retryable HTTP
  status. `body` is capped at 4 KB via `cap_status_body` with a
  truncation marker.
- `RetryBudgetExhausted { final_response, retry_after_history }` -
  retry budget exhausted. `final_response` preserves the last status,
  headers, and capped body when a response was received.
- `RateLimited { retry_after, final_response }` - 429 past the retry
  budget with preserved response evidence.
- `AuthLost { transmission_state, final_response }` - irrecoverable
  token failure. Local token-source loss has no final response;
  repeated target 401 after forced refresh preserves the 401 status,
  headers, and capped body with `transmission_state:
  Some(Acknowledged)`. Token-endpoint 401/403 during refresh also
  preserves its response evidence, but leaves `transmission_state`
  unset because it is not evidence for the target request.
- `RefreshFailed { retry_after, source: Arc<Error> }` - transient
  refresh failure preserving the original. OAuth `Retry-After`
  deadlines are stored on the wrapper for the account-error
  conversion.
- `ResponseTooLarge { limit }` - a buffered body exceeded
  `AccountSpec::max_buffered_response`. Maps to
  `Protocol(ContractViolation)`, not a transport class: the server
  answered, and the same answer comes back on a retry.
- `Cancelled` - request cancelled before completion. Produced by
  `wait_for_refresh` as the preserved `RefreshFailed` source when the
  single-flight refresh driver drops its sender without answering.
- `CostExceedsBurst { cost, burst }` - rate-limit configuration
  bug.
- `EncodeBody { message, source }` - `RequestBuilder::json`
  serialization failure; surfaced lazily on the next `send`.
- `InvalidHeader { message, source }` - `RequestBuilder::header`
  rejected the header name or value; deferred to `send` the same
  way `EncodeBody` is, so the fluent setter stays chainable.
- `InvalidRequest { field, detail }` - pre-wire local request or
  client configuration failure.
- `RangeNotHonored { kind, message }` - local invalid range input or
  a response that did not honor the requested range.
- `RedirectRejected { message }` - 3xx target host fell outside
  the configured `RedirectPolicy::trusted_hosts` allowlist.
- `MalformedRedirect { kind, message }` - acknowledged 3xx response
  with a present-but-broken `Location` (non-UTF-8 or unresolvable). A
  *missing* `Location` is not an error: it passes through (see the
  redirect loop above).
- `RedirectLoop { hops }` - redirect chain exceeded
  `RedirectPolicy::max_hops`.

`Status`/`Response` carry `reqwest::StatusCode` and `HeaderMap`
straight through. Documented coupling cost; acceptable for v1.

`account_error.rs` exposes:

```rust
pub struct NetErrorContext {
    pub provider: Option<Provider>,
    pub protocol: Protocol,
    pub operation: AccountOperation,
    pub scope: Option<ErrorScope>,
}

pub fn into_account_error(error: Error, ctx: NetErrorContext) -> AccountError;
```

`operation` is required so in-flight non-idempotent failures cannot
fall through the central recovery mapper as retry-safe. The
conversion attaches `Cause::Attempt` when target-attempt evidence
exists, preserves request / trace ids and capped body text from
`Status`, `RateLimited`, and status-backed `RetryBudgetExhausted`,
and leaves provider JSON interpretation to JMAP, Gmail, and Graph.

## Status-line helpers

`status_line::status_line_code(&str) -> Option<u16>` and
`status_line_is_success(&str) -> bool` parse an HTTP status line
(`HTTP-version SP status-code SP reason-phrase`, RFC 9112). They live
here rather than in a DAV crate because that grammar is HTTP's, not
WebDAV's; DAV merely carries status lines as element text.

Both DAV clients need them: RFC 4918 puts an HTTP status line inside
each `<D:propstat>`'s `<D:status>`, so a 207 Multi-Status body is only
meaningful once each is read. They were previously spelled out once per
crate, and the drift that produced is the reason for consolidating (see
dav-F5).

Two behaviours worth not regressing:

- The code is the first token in the protocol-less form (`200 OK`), or
  the token immediately after an `HTTP/` version. It must be in RFC
  9110's `100..=599` range, and the parser never scans later prose for
  a number. An invalid status position lands in the unreadable bucket
  that `status_line_is_success` fails closed on.
- A line with no parseable code is NOT success. An unreadable status is
  not evidence the property was returned, so treating it as success
  commits a value the server may have refused. This is the one place
  the two DAV crates had silently diverged: carddav failed closed here,
  caldav mapped unparseable to `None`, which its
  `propstat_success.unwrap_or(true)` then read as success. An ABSENT
  status remains success (RFC 4918 s14.22 requires one, so a server
  omitting it is describing a success) - only the present-but-unreadable
  case changed.

## URL helpers

`url::encode_path_component(value)` and
`url::encode_query_value(value)` share the RFC 3986 component escape
set: unreserved characters survive and every other ASCII character
plus all controls is escaped. The path form additionally
double-escapes a complete `.` or `..` component so the WHATWG parser
cannot resolve it as navigation while parsing the assembled URL. The
query form leaves those values literal because dots have no structural
meaning there. The HTTP protocol crates name the grammar at every call
site, preventing path hardening from corrupting search and filter
values. The double-escape is safety over addressability, not free: a
server percent-decodes once, so it receives the literal bytes `%2E` -
a provider id that is literally `.` or `..` is permanently
unaddressable through this encoder, and no correct spelling exists
under WHATWG parsing (single-escaped `%2E` still resolves as
navigation).

`url::parent_collection_url(resource_url)` returns the parent
collection of an absolute resource URL. It drops query and fragment
before popping the final path segment, so a slash in either cannot be
mistaken for a path separator. Both DAV crates use it rather than
carrying a private copy each.

`url::well_known_url(base_url, service)` builds the RFC 6764 discovery
endpoint by replacing the base URL's path with `/.well-known/<service>`
and clearing query and fragment. Well-known discovery is defined at the
ORIGIN root, so appending the suffix to a path-bearing configured base
is wrong: it addresses a resource that is not a discovery endpoint, and
because only a not-found classification triggers the base fallback, a
deployment answering that path with 401 or 403 would fail the open
before the configured base was ever tried. `None` means the base does
not parse or cannot carry a path; callers go straight to the configured
base.

## File map

```
crates/net/src/
  lib.rs          // re-exports; AccountFuture/AccountId/Priority/
                  // ByteRange from bifrost-types
  config.rs       // process-wide NetConfig + with_root_cert
  net.rs          // Net + AccountNet; shared_default; attach/detach;
                  // download_stream
  request.rs      // RequestBuilder + Response/StreamingResponse;
                  // Dispatch seam, retry loop, redirect walk
  redirect.rs     // FollowRedirects + RedirectPolicy + classify_redirect
                  // (RFC 7231 section 6.4 method rewriting, trusted-host
                  //  allowlist, Authorization stripping)
  account_error.rs // context-aware conversion to bifrost-types AccountError
  auth.rs         // TokenSource + OAuthRefresher state machine +
                  // StaticTokenSource
  hold.rs         // retained shared hold registry + paired deadlines
  retry.rs        // RetryPolicy
  rate.rs         // RateLimitGovernor + HostBucket
  status_line.rs  // HTTP status-line parsing shared by the DAV crates
  test_support.rs // scripted wire dispatcher (feature "test-support")
  metered_stream.rs // MeteredStream byte-stream admission
  budget.rs       // BandwidthBudget + BandwidthRegistry + Reservation
  upload.rs       // exact-sized outbound Body + admission-aware header clock
  bandwidth.rs    // BandwidthMeter + AccountMeter + MeterSink +
                  // MeterSinkHandle
  error.rs        // Error + cap_status_body
  trace.rs        // traceparent injection (current: uuid trace id)
  url.rs          // distinct path-component and query-value encoders;
                  // parent_collection_url; origin-rooted well_known_url
```
