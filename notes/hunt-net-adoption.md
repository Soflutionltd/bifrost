# Hunt: bifrost-net adoption

Scope: places where a provider crate hand-rolls, bypasses or re-implements a
rule `bifrost-net` owns, and where the provider's copy disagrees with the shared
one or escapes a guarantee the shared one makes. Reference read:
`reference/net.md`. Ordered roughly by severity. Defects first, then structural
proposals, then lateral findings. Nothing was edited.

## Defects

### N1. attach_account wraps every token source in an OAuthRefresher, and the providers tell consumers to pass an OAuthRefresher: refreshers nest by design

- Claim broken: `reference/net.md` "Do not nest `OAuthRefresher`s. Wrapping one
  refresher around another causes the outer's `force_refresh` to bypass the
  inner's single-flight", repeated in `crates/net/src/auth.rs` above
  `impl TokenSource for OAuthRefresher`.
- `Net::attach_account` (`crates/net/src/net.rs`, the `token_source` arm)
  unconditionally does `Arc::new(OAuthRefresher::new(source).with_max_age(..))`.
- The provider entry points document the consumer's source as "typically an
  `OAuthRefresher` over its own refresh-token store":
  `crates/jmap/src/sync/factory.rs` (`JmapCredentials::Bearer`),
  `crates/graph/src/client.rs` (`with_source`),
  `crates/google/src/account/mod.rs`. SMTP's docs say the same
  (`transport.rs`, "an already-fresh `OAuthRefresher`").
- Consequence: the outer refresher's proactive window (60 s before expiry, or
  `token_max_age` = 55 min for opaque tokens) calls `inner.refresh()`, which is
  `force_refresh()` - an unconditional issuer round-trip that ignores the inner's
  still-fresh cache. Every HTTP attachment (JMAP, Graph, Google each attach their
  own `AccountNet`, and reopen attaches again) is a separate outer refresher, so
  one consumer refresher is force-refreshed once per attachment per window, and
  the outer's error backoff stacks on the inner's. The 401 path is fine
  (`refresh_if_unchanged` is coordinated); the proactive path is not.
- The rule lives in net and net itself breaks it. Fix belongs in net: either do
  not wrap a source that is already a refresher (a marker method on
  `TokenSource`, e.g. `fn is_coordinating(&self) -> bool`), or make the outer
  proactive path call `current()` on the source first and only `refresh()` when
  that returns the same stale token.

### N2. The token lifecycle is split: HTTP requests use net's refresher, every other wire path reads the raw source

- Claim broken: `reference/net.md` OAuth single-flight section (proactive
  refresh, max-age fallback, 401 one-shot forced refresh, error backoff) is
  presented as THE token lifecycle; JMAP's `Authorization::header_value` doc says
  the bearer read is "live".
- The refreshed token lives inside the attach-time `OAuthRefresher`. These paths
  do not go through it and call `token_source.current()` on the consumer's raw
  source instead:
  - JMAP WebSocket handshake: `crates/jmap/src/client.rs`
    `Authorization::header_value` -> `source.current()`, used by
    `crates/jmap/src/client_ws.rs` `connect_ws`.
  - JMAP Basic path (harmless) and the SSE open path share the same helper for
    non-bearer only, so SSE is fine.
  - DAV: `crates/dav-core/src/dispatch.rs` `auth_headers` ->
    `token_source.current()`; DAV attaches with `AccountSpec::new(None)` so net
    holds no refresher for it at all.
- If the consumer hands a plain `TokenSource` (the trait contract says
  `current()` returns the cached token and `refresh()` is driven "by
  `OAuthRefresher`"), these paths NEVER see a refreshed token: HTTP requests on
  the same JMAP account succeed with the refresher's new token while the WS
  upgrade keeps presenting the expired one from the raw source. If the consumer
  hands a refresher, N1 applies instead. Neither shape gives one coherent
  lifecycle.
- Structural fix: net should own one coordinating token handle per account and
  expose it (`AccountNet::bearer()` or similar returning the refresher's
  `current()`), and WS / DAV / IMAP / SMTP should read through it rather than
  the raw source.

### N3. DAV never performs the 401 one-shot forced refresh

- Claim broken: `reference/net.md` "401 -> force refresh once; second 401 ->
  `Error::AuthLost`", the transport's auth-recovery contract for every HTTP
  protocol crate.
- DAV opts out of bearer injection (`without_bearer_auth`, documented, because
  the origin gate mints the header). That also opts it out of net's 401 recovery,
  and DAV does not replace it: `crates/dav-core/src/error.rs` `status_error` maps
  401 straight to `Authentication(ReauthorizationRequired)` (terminal
  `RecoveryClass::AuthLost`), and nothing in dav-core, caldav or carddav calls
  `refresh` / `refresh_if_unchanged`.
- An OAuth CalDAV/CardDAV account whose token expired between proactive
  refreshes (or an opaque token with no expiry hint, exactly the case net's
  forced refresh exists for) is sent to a re-authorization prompt instead of
  refreshing. The fix that keeps the credential gate: let DAV pass a per-request
  "credential minter" to net (or have `RequestBuilder` accept an
  `Authorization` provider closure re-invoked after a 401), so the gate stays in
  DAV and the recovery stays in net.

### N4. DAV's hand-walked redirects reset the total request deadline on every hop

- Claim broken: `reference/net.md` "The total deadline is computed once before
  the redirect walk ... Redirects reset their per-hop retry and 401 budgets but
  never the deadline", and the `RequestDeadline` rationale ("the
  per-attempt-versus-overall conflation is exactly the bug the split was made to
  prevent").
- `dav_account_spec` sets `request_timeout = Some(DAV_CLIENT_TIMEOUT)` and
  `FollowRedirects::Disabled`; `DavDispatch::send_raw_request` calls
  `dispatch_once` per hop, each a fresh `send()` with a fresh total deadline. A
  chain up to `DEFAULT_MAX_HOPS` (10) can therefore take up to 11 x the
  configured timeout.
- Fix: compute one deadline at the top of `send_raw_request` and pass the
  remainder with `RequestBuilder::timeout` per hop. Better: give net a
  per-hop authorization callback (see N3) so DAV can use net's walk and stop
  duplicating it.

### N5. DAV drops Retry-After and the doc comment on dispatch_once says the opposite of what the code does

- Claim broken: the doc comment on `DavDispatch::dispatch_once`
  (`crates/dav-core/src/dispatch.rs`): "Everything else - an exhausted retry
  budget, a rate limit past its budget, a transport failure - is left to
  `bifrost_net::into_account_error`, which classifies with the retry hints and
  transmission evidence the DAV status ladder does not carry."
- `wire_outcome` does the opposite: `RetryBudgetExhausted { final_response:
  Some(_) }` and `RateLimited { .. }` are reconstituted as `DavResponse` (both the
  bare variants and the terminal of an `AttemptHistory` on replayable requests).
  They then reach `status_error`, which takes only the status and body: every
  `ServerCause::{RateLimited, Unavailable, QuotaExhausted}` it mints has
  `retry_hint: None` (`crates/dav-core/src/error.rs`). No DAV crate reads
  `Retry-After` anywhere.
- So a 429/503 with `Retry-After: 300` from a CalDAV server reaches the engine
  with no hint, where net's own `into_account_error` would have produced
  `RetryHint::After(300s)`, and the retry history (`retry_after_history`) is
  discarded. Either thread the headers into `status_error` and parse with
  `bifrost_net::parse_retry_after`, or stop reconstituting these two variants
  for callers that do not read 429/503 as data (none do, as far as I found).

### N6. CardDAV hand-parses a status line, and gets the protocol-less form wrong

- Claim broken: `reference/net.md` status-line helpers: "the code is the first
  token in the protocol-less form (`200 OK`), or the token immediately after an
  `HTTP/` version"; the helpers exist because the per-crate copies drifted
  (dav-F5).
- `crates/carddav/src/parse.rs` `finish_response` (directory-search truncation,
  RFC 6352 6.3.2) tests `status.split_whitespace().nth(1) == Some("507")`.
  `507 Insufficient Storage` (protocol-less, accepted everywhere else through
  `status_line_code`) is not detected as truncation and falls through to the
  ordinary per-resource quota classification; any garbage first token followed
  by `507` is accepted. Fix: `bifrost_net::status_line_code(status) ==
  Some(507)`. This arrived with the recent directory-search commit, i.e. the
  drift the consolidation was meant to stop has restarted.

### N7. JMAP URL templates are encoded with a set that lets `.` and `..` through as path navigation

- Claim broken: `reference/net.md` URL helpers: the path encoder "double-escapes
  a complete `.` or `..` component so the WHATWG parser cannot resolve it as
  navigation"; "The HTTP protocol crates name the grammar at every call site".
- JMAP does not use `bifrost_net::url` at all. `crates/jmap/src/core/session.rs`
  `TEMPLATE_VALUE` keeps `.` unreserved (correct per RFC 6570, which is exactly
  why net's hardening is needed on top). `crates/jmap/src/blob/download.rs`
  substitutes `{name}` from the attachment's filename, which is
  sender-controlled: an attachment named `..` or `.` turns
  `/download/{accountId}/{blobId}/{name}` into a URL the parser collapses to a
  different path, so the GET addresses some other resource on the JMAP server
  with the account's credential. Ids are base64url in a well-behaved server,
  so `{name}` is the practical vector.
- Fix: substitute with `encode_path_component` when the template variable sits
  in the path, and `encode_query_value` when it sits in the query (the template
  parser already knows which side of `?` each `URLPart` is on). Same for
  upload and EventSource templates.

### N8. JMAP applies its 10 s total deadline to blob uploads and downloads

- Claim broken: `reference/net.md` deadlines section: the total deadline is
  distinct from the inactivity bounds so "a slow response that continues making
  progress [stays] distinct from a stalled response"; google and graph set no
  `request_timeout` for that reason.
- `crates/jmap/src/transport_reqwest.rs` sets `spec.request_timeout =
  Some(timeout)` AND `request.timeout(self.timeout)` on every request in
  `send_once`, including `upload` and `download`. `DEFAULT_TIMEOUT_MS` is 10 s
  (`crates/jmap/src/client.rs`). A raw-message or attachment download that is
  progressing normally but takes more than 10 s fails as
  `Timeout { Acknowledged }` -> `Protocol(PartialResponse)`, and is retried by
  the engine into the same wall. Blob traffic should rely on the
  header/inactivity bounds (or a size-proportional deadline), with the total
  deadline kept for API POSTs.

### N9. Bandwidth registries cannot be unified for JMAP accounts that accept invalid certs, and JMAP/Google take no injected Net

- Claim broken: `reference/net.md` bandwidth section: "The consumer must wire
  ONE registry into every Net, the engine and the IMAP factory. Matching ids in
  different registries do not coordinate."
- `Net::shared_default()` and `Net::shared_accepting_invalid_certs()`
  (`crates/net/src/net.rs`) each call `Net::new`, which mints a fresh private
  `BandwidthRegistry`. JMAP attaches through `Net::shared_for_tls` with no way to
  pass a Net (`ReqwestTransport::new`); Google always uses `Net::shared_default`
  (`crates/google/src/client.rs`). The only way a consumer can satisfy the rule
  is to pass `Net::shared_default().bandwidth_registry()` to the engine - which
  is undocumented - and even that leaves every JMAP account with
  `accept_invalid_certs = true` on a second registry the engine's per-account
  budget (`crates/sync/src/engine/attach.rs`) never reaches. IMAP, CalDAV and
  CardDAV accept `with_net`; JMAP and Google do not.
- Fix: the two shared Nets should share one process registry (and one hold
  registry and governor, for the same reason), and JMAP/Google factories should
  accept a `Net` like the others.

### N10. JMAP WebSocket and ManageSieve traffic is capped but never metered

- Claim broken: `reference/net.md` "IMAP/SMTP report bytes-in/out through
  `MeterSink` for unified bandwidth accounting", and the meter's
  `observed_bps()` being the account's figure.
- `MeteredStream` admits bytes against the budget but "No observation sink is
  attached by this adapter". Its two users,
  `crates/jmap/src/client_ws.rs` (RFC 8887 WebSocket, push and any API calls
  made over WS) and `crates/imap/src/account/sieve.rs` (ManageSieve), therefore
  never record into the `BandwidthMeter`. IMAP and SMTP each built a private
  `WireMetering` (`crates/imap/src/connection/wire.rs`,
  `crates/smtp/src/transport/smtp/client/metering.rs`) precisely to get both
  halves. Fix in net: let `MeteredStream` take an optional `MeterSinkHandle`
  (or an `AccountMeter`) and record actual bytes on each finished operation;
  then the two `WireMetering` copies can collapse onto it.

### N11. Graph's Retry-After doc claims a behavior the shared parser does not have

- Claim broken: doc on `parse_retry_after_header`
  (`crates/graph/src/account/graph_error.rs`): "HTTP-date values that have
  already elapsed ... collapse to `Duration::ZERO`".
- It delegates to `bifrost_net::parse_retry_after`, which returns `None` for an
  elapsed date (`when.duration_since(now).ok()?`). So an elapsed date yields no
  hint, not a zero hint. Either the comment is wrong or net should saturate to
  zero; net's other consumers (`into_account_error`) inherit whichever is
  chosen, so decide it in net.

### N12. JMAP's well-known rule contradicts net's well_known_url rule

- Claim broken: `reference/net.md` URL helpers: appending the well-known
  suffix "to a path-bearing configured base is wrong: it addresses a resource
  that is not a discovery endpoint". DAV uses `bifrost_net::url::well_known_url`.
- `crates/jmap/src/client.rs` `well_known_session_url` appends
  `/.well-known/jmap` to whatever path the caller gave, with a "Decision note"
  defending it. Two binding statements in the repo disagree about the same RFC
  6764/8620 rule. This needs an owner ruling; if JMAP's reasoning holds, it
  should be origin-rooted first with the path-appended form as a fallback, the
  same shape DAV uses with its configured base.

## Structural proposals (not defects)

- **DAV's redirect walk duplicates net's.** It exists because net strips
  `Authorization` on origin change with no way to re-mint. Net could accept a
  per-hop credential callback (`RequestBuilder::authorize_hop(|url| ...)`)
  invoked for each target; DAV's gate becomes that callback, and N3, N4 and
  the semantic drift (DAV ignores 303 and never rewrites 301/302 methods, where
  net's doc says rewriting "happens exactly once and the same way for every
  HTTP protocol crate"; DAV also loses `may_have_committed` across hops) all
  disappear together.
- **Two `WireMetering` copies** (imap, smtp) re-implement budget admission plus
  sink recording around `bifrost_net::Reservation`. See N10.
- **Body-excerpt capping is copied three times.** Graph `body_diagnostic`
  (`graph_error.rs`) re-implements `cap_status_body` inline; Google's
  `body_excerpt` (`crates/google/src/error.rs`, `MAX_BODY_EXCERPT_BYTES`)
  truncates without the marker net's doc promises ("Callers ... can detect
  truncation"); JMAP's `open_sse` passthrough drain
  (`PASSTHROUGH_BODY_CAP`) has no marker and silently stops on a read error
  where net emits a read-failure marker. All three should call
  `cap_status_body` or a shared capped drain.
- **Google keeps its own Retry-After parser** (`parse_retry_after_value`) to
  retain delta-versus-date syntax. Reasonable, but it is a second parser of a
  net-owned header; net could return the structured form and let
  `parse_retry_after` be a projection of it.
- **Token lifecycle ownership** (N1, N2): one coordinating token handle per
  account, owned by net and read by every transport, HTTP or not.

## Lateral findings

- `crates/jmap/src/client.rs` `Authorization::account_token_source` hands net a
  `StaticTokenSource::new("", None)` for Basic, with the comment "AccountNet
  requires a token source". Stale: `AccountSpec::token_source` is `Option` and
  net.md says `None` is the shape for non-bearer accounts. It also builds a
  pointless refresher over an empty token.
- `crates/jmap/src/transport_reqwest.rs` `redirect_policy_from_trusted_hosts`
  emulates "no cross-host redirects" by trusting the sentinel host
  `__bifrost_jmap_no_cross_host_redirects__`, because an empty allowlist means
  "every host". Works, but it is a missing `RedirectPolicy` mode (same-origin
  only) expressed as a magic string.
- `crates/jmap/src/transport_reqwest.rs` `open_sse` fabricates
  `bifrost_net::Error::Status` for a passed-through 3xx; net's doc defines
  `Status` as a terminal non-retryable 4xx/5xx-style status, so downstream
  classification sees a 3xx in a variant that elsewhere never carries one.
- `crates/graph/src/client.rs` `execute_wire` sets `Content-Type` on every
  request, including bodyless GET and DELETE. Google's `execute` carries an
  explicit comment that this "announced a JSON body on bodyless GET and DELETE
  requests" and was removed there; Graph still does it.
- `crates/jmap/src/client_ws.rs` `connect_ws` has no connect/TLS-handshake
  bound of its own (`TcpStream::connect` + `wrap`), unlike net's
  `NetConfig::connect_timeout`; it depends on the push layer's outer handshake
  bound, so a non-push caller of `connect_ws` can hang. It also sends no
  `User-Agent` or `traceparent`, which every net request carries.
- JMAP blob downloads go through buffered `send`, so a message or attachment
  above `DEFAULT_MAX_BUFFERED_RESPONSE` (64 MiB) fails as `ResponseTooLarge`;
  `download_stream` exists and is unused by JMAP.
- `crates/smtp/src/transport/smtp/client/metering.rs`
  `blocking_admit_with_wait` compares a Tokio-clock `ready_at().into_std()`
  against `std::time::Instant::now()`. Identical in production, but under
  paused Tokio time the two clocks diverge, so the blocking path cannot be
  tested the way the async path is.
