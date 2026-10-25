# bifrost-jmap reference

Generic net throttles explicitly use account scope for every JMAP provider.
The factory test transport parses Retry-After as a hint before resolving its bounded delay;
absolute dates remain in response headers for error recovery. SSE redirect
excerpts probe beyond the shared cap to identify truncation; read failures
retain a separate marker, including when the prefix exactly fills the cap.

Every request declares its `ReplayPolicy`; the table lives at the call sites.
Typed batches are Idempotent iff all methods declare REPLAY_SAFE. All writes,
including unguarded update-only sets, are Never because side effects and
sequential patches do not compose safely on replay. An unanswered guarded
Email/set id reconciles, including internally probed ifInState; unguarded ids
retain retry advice. Non-replayable requests stop on the first ambiguous status
or in-flight drop. 401/429 refusal retries remain. [503, 200 stateMismatch]
stops at 503 and reconciles; a genuine first-response conflict stays a conflict.

Current architecture of the JMAP implementation crate. The only external surface is `bifrost_jmap::sync` (account factory + config types). The protocol client, typed wire objects, transport, method macros, error type, and helper facade are crate-internal.

## Public surface

With the `sync` feature enabled, consumers may use:

- `sync::JmapAccountFactory` - registered as `Arc<dyn AccountFactory>`.
- `sync::JmapAccountFactoryBuilder` - open-time factory builder.
- `sync::JmapCredentials` - Basic or bearer credentials. `bearer(token)`
  / `bearer_source(Arc<dyn TokenSource>)`; a bearer source threads through
  `account_token_source()` into bifrost-net and is read live per request.
  Basic auth supplies its own `Authorization` header and attaches without a
  bearer token source. `Clone`, hand-written `Debug`, no `PartialEq`/`Eq`.
- `sync::ReconnectPolicy` - WebSocket reconnect backoff config.

Everything else in `crates/jmap/src/` is `pub(crate)` or narrower. Consumers reach JMAP by constructing the factory, opening an `Arc<dyn Account>`, and calling `bifrost-types::Account` methods.

Session-level Mail, Submission, Sieve, Calendars, Contacts and Principals
objects are support markers. Their typed configuration lives on session
accounts. Convenience session readers select the capability's primary account;
callers handling a foreign account use that account's readers directly. The
Absent / Malformed / Present distinction applies to the selected account's
object. Self-alias discovery reads the current user from the primary Principals
account, or the owner extension from the primary mail account, and uses that
owner extension's accountIdForPrincipal to route Principal/get.

## Internal method dispatch

Every JMAP method is a self-describing struct implementing `JmapMethod`:

```rust
pub(crate) trait JmapMethod: Serialize + Send {
    const NAME: &'static str;       // "Email/get"
    type Cap: Capability;           // capability::Mail
    type Response: DeserializeOwned; // GetResponse<Email<Get>>
}
```

Adding a new method: define a struct, use `define_get_method!` / `define_set_method!` etc. Zero central files touched.

`Request` takes one `SessionState` snapshot when it is constructed. Its
`maxCallsInRequest` guard, default account id, and HTTP `apiUrl` all come from
that snapshot, which remains attached through send. It refuses the first call
that would exceed the limit, before serializing or appending that method. The
guard covers tuple batches, explicit result-reference flows, and WebSocket
requests through the same `call` door. The WebSocket encoder's separate
`maxSizeRequest` check still reads the client's current session at encode time,
so a refresh between request construction and WebSocket encoding can pair the
request snapshot with the newer frame-size limit.

The snapshot is a three-state `CallLimit`, not a number: `Unadvertised` (no
`urn:ietf:params:jmap:core` block, or a block that omits `maxCallsInRequest`),
`Invalid` (the block advertises `maxCallsInRequest: 0`, which RFC 8620 forbids,
or the block is present and does not parse at all), and `Advertised`, which is
the only state that enforces.

Both halves of that widening are deliberate. Every core limit is an
`Option<usize>`, so an OMITTED field reads as "advertised nothing" rather than
as the zero a blanket `#[serde(default)]` used to fill in - two different server
bugs with two different classifications. And a present-but-unparseable core
block (`"maxCallsInRequest": "16"`) takes the `Capabilities::Malformed`
variant, read through `Session::core_capability_state()` (`Absent` /
`Malformed` / `Present`); falling back to `Capabilities::Other` made it
indistinguishable from absent at every reader, so a server sending a
string-typed limit was classified `SyncState(CapabilityChanged)` and the engine
reopened forever against a session that will never change. `core_capabilities()`
still collapses `Absent` and `Malformed` into `None`, which is correct for any
reader that only declines to enforce a bound; every reader that CLASSIFIES a bad
session must use the three-state. Treating the first two as a limit of zero would make
every request unable to hold a single call - and any caller probing such a
session would then see `Request(Malformed)` /
`ClientBug`, preempting the classifications the engine actually needs:
`SyncState(CapabilityChanged)` / `RestartAccount` for an absent core capability,
`Protocol(ContractViolation)` for a zero-valued one. Capability validation is
the gate for both bad sessions; the request builder enforces only a limit the
server actually advertised.

The method-generation macros are crate-private. They are re-exported
inside the crate root only so sibling modules can keep the existing
`crate::define_*` call sites without exporting macro names to consumers.

## Internal request/response flow

```rust
let mut request = client.build();
let handle = request.call(EmailGet::new(&account_id))?;  // typed CallHandle<EmailGet>
let mut response = request.send().await?;
let result = response.get(&handle)?;  // compile-time safe extraction
```

`CallHandle<M>` validates call_id and method name. A successful response with
the wrong echoed method name is rejected and maps to
`Protocol(ContractViolation)` at the account boundary. `Response::get()`
returns `Error::Method` for JMAP method-level errors. RFC 8620 s3.2 lets one
call produce several responses under the same call id; `get` consumes them in
the order the server sent them, so repeated reads on one handle walk the
sequence and unrelated handles are unaffected. RFC 8621 s7.5 does produce this
shape: a successful `EmailSubmission/set` with `onSuccessUpdateEmail` or
`onSuccessDestroyEmail` is followed by an implicit `Email/set` response under
the submission call id. `Response::get_method` reads that response by its
method type after the explicit submission result. In-place removal preserves
order; swapping the tail into the vacated slot reorders later lookups.

## Internal transport abstraction

`Client<T: HttpTransport = ReqwestTransport>` is generic over transport.

- `HttpTransport` - api_request, upload, download, get_session (returns `Bytes`).
- `ClientBuilder::connect` probes the RFC 8620 origin-rooted
  `/.well-known/jmap` URL first. If the configured base has a path, only an
  origin 404 triggers a second probe below that path. Other origin errors are
  preserved, and a successful HTTP response is parsed as the origin session;
  a malformed body does not trigger a path fallback. The URL that succeeds is
  retained for session refresh. Query and fragment components do not take part
  in discovery.
- `SseTransport` - open_sse (EventSource, with `last_event_id` support).
  `Client::event_source()` and everything in `event_source/` (WHATWG-conformant
  SSE parser plus stream driver, hermetically pinned) are `pub(crate)`: they are
  internal surface, not published API, and are kept built and tested even though
  the `sync/` Account impl does not call them. The reason they are built at all
  is that eventSourceUrl is a mandatory RFC 8620 session property, unlike the
  RFC 8887 WebSocket extension `sync/` push relies on. Wiring EventSource in as the sync-layer
  push fallback for servers without WebSocket push is tracked in
  `reference/jmap/DEFERRED.md`. The long-lived SSE response explicitly suppresses
  the ordinary JMAP request deadline while retaining response-header and body
  inactivity bounds.
  Each `{types}` occurrence expands the same requested list. `None` expands to
  `*`; an explicit empty list is rejected because RFC 8620 requires either `*`
  or a comma-separated list of type names. Each type name is encoded at its
  substitution position, so dot-only path components cannot navigate and
  reserved characters cannot add template delimiters.

  The stream driver's failure policy is per-case, not uniform. An undecodable
  `EventType::State` payload ends the stream, because the payload was
  authoritative and only a reconnect from `lastEventId` replays it; an
  undecodable `calendarAlert` and an unrecognised event name do not, because
  nothing downstream regenerates an alert and terminating would let one
  non-authoritative payload livelock the whole push lane. A parser error (the
  1 MiB `MAX_EVENT_SIZE` cap) ends the stream unconditionally, and not because
  the framing broke: the parser always resynchronises - every error site
  discards to the next blank line and returns to `Init`, and `discard(..)`
  deliberately preserves `last_event_id`. It ends because of what the discard
  destroys. The block may already have accumulated `data`, and delivering the
  next block's checkpoint would let a consumer resume past a state change it
  never saw. Every failure is yielded either way. The cap error is a
  `TransportError::limit_exceeded` (`LocalFailure::LimitExceeded`), which the
  error boundary classifies `Internal(LimitExceeded)`: the cap is this crate's,
  not the protocol's, so the server broke no rule, and a reconnect replaying
  the same block from `lastEventId` trips it again, so it is not a network
  fault to retry either.

  Flattening a push block emits the token-free notifications (`CalendarAlert`,
  `EmailPush`) first, in wire order, and the merged `StateChange` last, because
  only the state change carries the block id a consumer checkpoints. Interrupted
  processing can therefore only replay, never skip. The accepted consequence is
  that a block containing ONLY alerts advances no checkpoint, so after a
  reconnect the server resumes from the last checkpointed position and those
  alerts are delivered again. Repeated calendar alerts across a reconnect are
  expected, not a defect; closing the gap would mean giving an alert a
  checkpoint position of its own, which reshapes the notification type and its
  wire contract.

  Exempting a "content-free" overflow so a megabyte keepalive comment could not
  kill a working stream was tried and rejected; it is unsound in the unsafe
  direction and should not be rebuilt. The verdict is not computable where the
  cap trips, because the block is not over: an oversized comment (or `event:` /
  `id:` / unknown field name) ahead of a valid `data:` line looks content-free
  at the trip point, and then `discard(..)` consumes that `data:` line too - a
  state change destroyed while the code reports nothing lost. Deferring the
  verdict to the terminating blank line needs data scanning, `id` preservation
  (`discard(..)` also swallows a later `id:`, so the next event would carry a
  stale resume token), a discard budget plus EOF policy for a blank line that
  may never arrive, and a stream EOF branch that currently exits without asking
  whether an overflow is pending. That is far more machinery than surviving an
  over-1 MiB comment line is worth, and nothing suggests such comments are a
  normal interoperability requirement.
- `ReqwestTransport` - default implementation with a pooled reqwest::Client.
- `blob::download::BlobTransport` extends the buffered transport with an
  incremental download door. `Client::download_stream(blob, max_bytes)` yields
  byte chunks and records request-local inbound traffic as they are polled.
  Production uses net's `send_streaming` with JMAP headers and authorization,
  without the total request deadline. Both buffered production download doors
  collect this same path with a separate 512 MiB blob ceiling, independent of
  net's 64 MiB JSON buffer ceiling. Streaming callers supply their own ceiling;
  the first chunk exceeding it fails with a local limit error and terminates
  the stream. Earlier chunks are incomplete data and must be discarded on error.
  Buffered custom transports retain their existing contract. The engine-facing
  blob and raw-message facades consume this incremental door with the separate
  512 MiB ceiling. Each transport chunk becomes one batch; one prior chunk is
  held while polling its successor so the final successful batch can carry
  `PageBoundary::Final`. The facade takes request-local byte-count deltas per
  chunk, and a raw read's
  `Email/get` bytes are charged to its first chunk batch (or its empty final
  batch). A body error ends the stream without `Done`, so consumers discard
  any prefix already received.

- The configured total request deadline applies to the API request and the
  initial session fetch. Blob uploads and downloads explicitly call
  `without_timeout()`: upload sizes and bandwidth caps can make a complete
  transfer take longer than the small JSON request deadline, while bifrost-net
  still enforces its response-header and body-inactivity bounds. EventSource
  uses the same no-total-deadline policy for its long-lived stream.
- `Client::with_transport(transport, session, session_url)` - crate-internal custom transport injection. The session URL is required and rejected when empty (`Error::InvalidUrl`, the caller's fault): a client built without one could never re-fetch its session, so `refresh_session` was a silent no-op against the wrong (empty) URL.
- WebSocket remains reqwest-specific (documented).

The session and everything derived from it (`apiUrl`, the upload / download /
EventSource templates parsed into `URLPart`s, and the default account id) live
together in one `Arc<SessionState>` behind a single lock. RFC 8620 §2 lets any
Session property change, so `refresh_session` republishes the whole derived set
atomically; readers take one `session_state()` snapshot and build a whole URL
from it, so a concurrent refresh cannot splice two sessions into one request.
Every template comes from the server's session object, so one that does not
parse (an unbalanced or unknown template variable) is
`Error::MalformedSessionUrl` naming the session property (`uploadUrl`,
`downloadUrl`, `eventSourceUrl`), not the caller's `InvalidUrl`. `URLPart::parse`
itself returns a bare description because it cannot know who wrote the
template; `SessionState::derive` attaches the property.
A template that parses must also be an absolute `http` / `https` URL with a
host, checked on a probe expansion (each variable replaced by one unreserved
character) so a legitimate level-1 template, including one whose host label is
a variable, is never refused; `apiUrl` gets the same absoluteness check
unexpanded, naming `apiUrl`. The upload and download templates and `apiUrl` are
checked eagerly, because the account uses them throughout. `eventSourceUrl` is
parsed with the session but a failure is only stored: `event_source_url()`
returns a `Result` and raises `MalformedSessionUrl { property: "eventSourceUrl" }`
at use, so a server whose EventSource template this crate cannot parse can still
be opened for sync, which never touches EventSource.
At the sync boundary, a method response whose `sessionState` differs marks the
client stale and advances a watch generation. The always-driven scope lifecycle
stream selects on that generation and terminates promptly with
`SyncState(CapabilityChanged)`, causing the engine to reopen
the whole account. An in-place refresh is insufficient there because the
existing `JmapAccount` has already frozen limits, capability flags, primary and
foreign account routing, and push topology from the old session. The watch wakes
the lifecycle poll pause immediately, so stale derived URLs are not used for
another poll interval after divergence.
Concurrent `refresh_session` calls serialize their fetch and install, so the
first fetch cannot finish last and overwrite a later fetched session.
`refresh_session` exists but no sync path calls it - the flag latches false for
the life of a client, and a fresh client (with a freshly fetched session) is
what clears it.

Values substituted into upload and download URI templates use `bifrost_net::url`
path-component or query-value encoding according to their substitution position.
Complete `.` and `..` path values are double-escaped to survive WHATWG
normalization; query dots remain literal. Other values are percent-encoded per RFC
6570 §3.2.2 simple string expansion - everything outside ALPHA / DIGIT / `-` /
`.` / `_` / `~`. A deny list is not
sufficient here: `+` in a query-position `{type}` would otherwise decode
server-side as a space, turning `application/ld+json` into `application/ld json`.

All convenience helpers are `impl<Tr: HttpTransport> Client<Tr>` so custom transports get the full API.
`Client::build()` uses the lexicographically first primary capability as its
stable generic fallback; account-scoped callers use `Account::build()` and
therefore select their required capability explicitly. A session advertising no `primaryAccounts` has no such fallback, so the derived default account id is empty; `Request::call` inspects the serialized arguments and refuses an empty default only when the method carries an `accountId` with `Error::NoPrimaryAccount { capability }` naming the method's own capability. Accountless `PushSubscription/get` and `/set` remain legal without any primary account. The fault is local and knowable before a byte moves, so it is reported locally rather than shipped as `"accountId": ""` and answered with an opaque server error.

A contact `update` that carries `address_book_id` moves the card, which is an add of the new membership plus a clear of the old - and the old book is knowable only from the current card. `contact_update` therefore reads the card first and FAILS the update, as a retryable `Protocol(PartialResponse)`, when that read materializes no card. Treating the missing read as "no old book" would silently degrade the move into an add and leave the contact in both books; the unanswered-id lane is a transient, not evidence that the card has no book.

The contact projection follows RFC 9553's current object shape: postal fields
are `AddressComponent` entries under `Address.components`, and job titles are
separate `Title` objects linked to an `Organization` by `organizationId`.
The accepted component kinds are exactly the projected ones: the street-line
kinds and `locality` / `region` / `postcode` / `country`. Any other kind returns
`Unsupported`, including kinds RFC 9553 does define (`district`, `subdistrict`,
`separator`) but the shared contact address has no field for, so a recognized
component is never accepted and then dropped.

The two halves of the card projection fail differently, and a consumer sees
both. Postal addresses REJECT: an unplaceable component kind fails the whole
card as `Unsupported`, because a recognized kind with no shared field would
otherwise be accepted and then dropped out of a value whose parts only mean
anything together. Everything else - emails, phones, notes, media, the name -
SKIPS: an entry whose required string is missing or whose value is not an
object is discarded and the rest of the card is returned. So a `ContactCard`
may carry fewer emails or phones than the server holds, with no signal that
one was dropped; these are unordered multi-valued collections where one junk
entry says nothing about the others, and failing the contact over a single
malformed phone would cost its name and its addresses too. Surfacing skipped
values would need a per-value failure lane on the shared `ContactCard`.

RFC 9553 `pref` is a 1-100 RANKING in which the lower number is the more
preferred entry, not a boolean spelled `1`. The email / phone / address
projections therefore mark as primary the entry with the LOWEST rank
present, ties resolved by position, and none at all when no entry carries
one - an absent `pref` is least preferred, never promoted. Reading
`pref == 1` reported no primary for a card whose best address was ranked
`10`, and several primaries for a card that ranked two entries `1`. A value
outside 1-100 is not a ranking and reads as absent, so `pref: 0` cannot
beat every legal rank.

The sync request helpers likewise take `Account<Tr>` / `Client<Tr>` with
`Tr: HttpTransport`; the engine-facing `JmapAccount` remains the reqwest
production specialization because WebSocket push is reqwest-specific. Factory
tests use an armed scripted transport that records exact API JSON and derives
each reply's error shape from the same decision table `crates/net/src/request.rs`
walks. A 2xx body reaches the protocol decoder. At the net layer, a
passed-through 3xx carries status + headers AND its body (bifrost-net's redirect
`PassThrough` arm hands the body up through the same `into_byte_stream` the
redirects-disabled arm uses, rather than discarding it; a `Location`-less
redirect carrying an explanatory document therefore arrives intact). The
production `ReqwestTransport` preserves that evidence, then wraps
streamed 3xx results in `bifrost_net::Error::UnexpectedResponse`, as described
under HTTP redirect handling below. The scripted dispatcher pins the net decision and
response evidence, not the final classification through that JMAP wrapper. A
second 401 is `AuthLost`; statuses retryable under `RetryPolicy::default()` (429
plus the 5xx family, read off the policy rather than restated) come back as
`RateLimited` / `RetryBudgetExhausted` with their final response preserved;
only a genuinely non-retryable 4xx is returned by bifrost-net itself as
`Error::Status`. An exhausted script reports the request ordinal rather than
falling through to a network. Despite the test module's location,
`AccountFactory::open` itself is not hermetically reachable: its `connect()`
call hardwires `ReqwestTransport`. The seam covers the generic sync request
helpers beneath that boundary, not factory connection establishment.

## Module pattern

Every JMAP object type under `crates/jmap/src/<type>/`:

- `mod.rs` - struct with `<State = Get>` phantom, Property enum, method struct definitions via `define_*_method!` macros.
- `get.rs` - getters on `T<Get>`, GetObject impl.
- `set.rs` - builder methods on `T<Set>`, SetObject + SetObjectCreatable impls.
- `query.rs` - Filter/Comparator enums, QueryObject impl.
- `helpers.rs` - `impl<Tr: HttpTransport> Client<Tr>` convenience methods.

## Two data models

**Typed structs** (Mailbox, Calendar, AddressBook, etc.): serde derive, `Field<T>` for nullable properties.

**JSON map backing** (CalendarEvent, ContactCard): `serde_json::Map` via `json_object_struct!` macro. Property enum has `Other(String)`. Extension properties preserved on round-trip.

## Key types

- `Field<T>` - three-state nullable: `Omitted` / `Null` / `Value(T)`. Use instead of `Option<Option<T>>`.
- `Id<T>` - phantom-typed string ID: `AccountId`, `BlobId`, `State`. Available for incremental adoption.
- `Account<Tr>` - internal account-scoped view of `Client`. Use `account.build()` for scoped requests inside the crate.
- `Capability` trait - typed URIs with associated `Config` type.
- `TransportError` - crate-owned, `#[non_exhaustive]`, carries response body (`Bytes`) for ProblemDetails parsing. A local failure is marked explicitly (the private `LocalFailure`, set only by the dedicated constructors) and classifies as `Internal(_)` before the net evidence or the no-evidence `Transport(Network)` fallback is consulted; an unmarked error with no net evidence (for example, an in-crate test stub) keeps the fallback. A passed-through 3xx from the default streamed transport currently carries net `Status` evidence and is classified as a terminal HTTP failure (see HTTP redirect handling below). Two marks exist. `LocalFailure::InvariantViolated` (`TransportError::invariant` / `invariant_with_source`) is a state impossible by construction, raised before anything was sent, and classifies `Internal(InvariantViolated)`. `LocalFailure::LimitExceeded` (`TransportError::limit_exceeded`) is a bound this crate imposes on a response the protocol leaves unbounded - today only the EventSource over-long block cap - and classifies `Internal(LimitExceeded)`. The central mapping never retries an `Internal` kind. Without the mark, impossible-by-construction states (an unsupported method, an invalid default header, missing credentials) were retried as network faults forever, and a tripped EventSource cap classified as a retryable network fault too.

## Capabilities

`Capabilities` enum in `session.rs` uses `deserialize_capabilities_map` to dispatch on URI key string. When adding a new capability:

1. Add struct in `session.rs`.
2. Add variant to `Capabilities` enum (with `#[cfg]` if feature-gated).
3. Add match arm in deserializer (via `try_cap!`, which supplies the malformed lane).
4. Add `Capability` impl in `capability.rs` with `type Config`.
5. Add the `session_cap_accessor!` invocation, which generates both readers.

`Session::capability_config::<C>()` is a convenience bridge (serde round-trip).
It returns `None` for a malformed block before reparsing its raw value, so a
permissive config struct cannot turn the malformed lane into a present one.
Hand-written accessors are zero-cost and primary.

### Absent, Malformed, Present - for every capability, not just core

`try_cap!` falls back to `Capabilities::Malformed(value)`, never to
`Capabilities::Other`, so a URI this crate models whose object it refuses to
parse is never reported as unadvertised. It first refuses any value that is not
a JSON OBJECT, which is not redundant with the typed parse: serde's derived
struct deserializers also accept a SEQUENCE in field-declaration order, so a
block sent as `[]` deserialized into any capability struct whose fields all
default and read as a fully advertised Present block (the calendars block sent
as `[]` was the concrete case), while `["16"]` fed the core block a
limit positionally. RFC 8620 §2 makes every capability value an object, and the
guard sits at the one door every modelled URI passes through, so the array lane
is closed for all of them at once rather than per struct. `Other` now means exactly one thing: a
URI the crate does not model. `session_cap_accessor!` generates the two readers
each capability needs from one mechanism - `x_capabilities() -> Option<&T>` for
callers that only decline to act, and `x_capability_state() ->
CapabilityState<'_, T>` (`Absent` / `Malformed` / `Present`) for callers that
have to classify. `CoreCapabilityState` is the core block's alias of that type.

The Unadvertised-versus-Invalid rule then decides per reader, and the split is
whether the account DEPENDS on the block:

- **Required.** `urn:ietf:params:jmap:core` and `urn:ietf:params:jmap:mail`.
  A malformed block is `Protocol(ContractViolation)` at
  `sync::capabilities::validate_session`, refused at open BEFORE any probe goes
  out, with the URI in the diagnostic.
  Not `SyncState(CapabilityChanged)`: nothing about this session changes on a
  reopen, so that lane buys an endless reopen loop.

  Account open requires every RFC 8620 §2 core member:
  `maxSizeUpload`, `maxConcurrentUpload`, `maxSizeRequest`,
  `maxConcurrentRequests`, `maxCallsInRequest`, `maxObjectsInGet`,
  `maxObjectsInSet`, and `collationAlgorithms`. A missing member is refused as
  `Protocol(ContractViolation)`. Zero `maxSizeUpload` permits empty uploads;
  zero concurrency limits and an empty `collationAlgorithms` array are valid.
  The request-size, call-count and object-count limits must be positive.
- **Optional.** Everything else the crate models (websocket, submission, sieve,
  quota, blob, calendars, contacts, principals, principals:owner). The family
  degrades to off, and `build` warns per malformed URI via
  `Session::malformed_capabilities()`. Push has its own arm on top of that
  warning, because a malformed `urn:ietf:params:jmap:websocket` block deriving
  `PushCapability::None` silently is indistinguishable from a server that never
  offered push. It still degrades - there is no url to connect to - but the
  reason is on the record.
- **The family gate is where "degrades to off" is actually enforced.** A
  family's enable flag is NOT `primaryAccounts` membership alone.
  `sync::factory::resolve_optional_families` reads each family's block state
  and, on `Malformed`, drops the `primary_account` handle it just resolved,
  warning with the URI and the family name. Dropping the HANDLE rather than
  only clearing the `PimSupport` flag is the point: the derived flag is
  `handle.is_some()` and every family door on `JmapAccount` reads the same
  `Option`, so one decision moves both and a live handle can never sit behind
  a `false` capability. `Absent` is deliberately not gated here - a session
  naming a primary account for a URI it omits from `capabilities` is a
  different question from one that names it and then describes it wrongly, and
  answering it in this gate would drop families off servers that work today.
  `urn:ietf:params:jmap:vacationresponse` has no typed block and therefore no
  malformed lane: `primaryAccounts` is its whole signal.
- **At the door.** `Client::connect_ws` separates the two states as well:
  `Absent` stays `Error::WebSocketNotConnected` (which maps to `Unsupported`),
  while `Malformed` raises `Error::MalformedCapability { capability }`, mapped
  by `sync::error` to `Protocol(ContractViolation)` naming the URI. Filing a
  server contradicting its own advertisement under "feature not offered" hides
  it. A block that parses but whose `url` is unusable (not a valid URI, or a
  scheme other than `ws` / `wss`) is the same kind of fault one level down:
  `Error::MalformedSessionUrl` naming the websocket capability's `url`, also
  `Protocol(ContractViolation)`, raised before any connection is attempted.

### `maxConcurrentRequests` holds across the whole client

`api_request_concurrency` bounds the WIDTH of one fan-out, but the limit
RFC 8620 advertises is on the server's view of the account's concurrent API
requests, and several call sites - a sync engine running inventory, hydration
and mutation at once, plus the fan-outs themselves - overlap. So the HTTP
transport also holds one `bifrost_net::ConcurrencyLimit` sized to the session's
`maxConcurrentRequests`: `HttpTransport::set_api_concurrency` is called with
every session the client installs (`connect`, `with_transport`,
`refresh_session`), and `ReqwestTransport` gates every `api_request` by it. Only
the API endpoint is gated - uploads and downloads have their own RFC limits.
Both the gate and `api_request_concurrency` read the field through
`Session::api_request_concurrency`, which reads an omitted or zero
`maxConcurrentRequests` as one, so a non-conformant session that leaves it out
gets serial API traffic rather than an ungated transport beside serial
fan-outs. An API request is a buffered `send`, so it never holds its slot past
its own return.

The cost is a new failure mode on a busy account: time queued for a slot counts
against the request's total timeout, so against a server advertising a small
limit, a few slow calls holding every slot can make queued API requests expire
as `Timeout { Unsent }` without ever reaching the wire. `Unsent` is the honest
evidence - nothing was sent, so any method may be replayed - and the recovery
mapping treats it as such. Requests sent
over the RFC 8887 WebSocket do not go through `bifrost-net` and are not gated.
`session_limits_gate_api_and_upload_requests_separately` and
`every_installed_session_updates_the_transports_concurrency_limits` pin the
separate transport gates and their session refresh.

### Upload requests honor their own core limits

Separate account operations can upload at the same time even though each
individual upload door awaits one request. `Session::upload_concurrency` reads
`maxConcurrentUpload`, using one as the serial minimum when the member is
omitted or zero. The client installs that value on the transport with each
session, and `ReqwestTransport` applies it only to upload endpoint requests.
This gate is shared across tasks using the same client. API requests continue
to use `maxConcurrentRequests`, and downloads have no upload gate.

`Client::upload_to` checks the byte length against `maxSizeUpload` before it
calls the transport. An oversized upload returns `Request(Malformed)` with the
advertised and actual sizes, so the file body never reaches the server.
`maxSizeUpload` remains an `Option<usize>` at the protocol layer so absence is
distinguishable from an advertised zero, but account session validation
requires it, as mandated by RFC 8620 section 2: an omitted value is
`Protocol(ContractViolation)`. An advertised zero permits only an empty upload.
Attachment uploads check each chunk before copying or growing the buffer, stop
polling at the first overflow, and bound requested buffer capacity by the
advertised maximum. Caller-owned chunks and allocator overhead remain outside
that bound; the `upload_to` check still guards the transport boundary.

## Feature gates

Per-RFC features: `mail`, `calendars`, `contacts`, `blob`, `quota`. Each gates:

- Internal module declarations in `lib.rs`.
- DataType enum variants (with `#[serde(other)]` catch-all).
- Capabilities enum variants + session accessors + deserializer arms.
- PushObject/PushNotification variants.
- Test modules.

The Account layer under `crates/jmap/src/sync/` wires optional PIM capabilities at open. `contacts.rs` maps AddressBook/ContactCard methods onto the shared contact primitives (incl. JSContact postal addresses); `calendar_ops.rs` maps Calendar/CalendarEvent onto list/range/get/create/update/delete/RSVP/search. Create payloads stamp the mandatory top-level `@type` (`Card` / `Event`) and the `@type` on the nested RFC-defined objects (Name, EmailAddress, Phone, Organization, Title, Address, AddressComponent, Note; Participant, Location, RecurrenceRule, NDay). JSContact photo media is written with `kind: "photo"` (resource role, not a URI marker) so self-written photos read back; the read path filters on that kind.
Range queries send a server-side `AND(inCalendar, after, before)` filter and reapply the local overlap predicate after hydration. Recurrence maps common RRULE fields to JSCalendar `recurrenceRules` and back; simple RDATE/EXDATE map through `recurrenceOverrides`. Unsupported outbound RRULE parts are rejected before Set construction, and the payload builders reject a failed conversion again on their own - a rule that does not convert is an error, never a cleared or omitted recurrence - so the refusal does not depend on `validate_shared_recurrence` having run first. Modified overrides, multiple rules, excluded rules, and unrecognized inbound recurrence components return `Unsupported` rather than being silently discarded. A `recurrenceOverrides` entry projects only when it is a bare addition or a bare `excluded`; anything that patches the occurrence itself fails hydration instead of handing back the master event with the modified occurrence missing.
Calendar page walks (`events_in_range`, `search`) reconcile their submitted ids the way `contacts.rs::reconcile_cards` does. Every id the `CalendarEvent/get` answer covered in neither `list` nor `notFound`, and every returned event `event_from_jmap` refuses (a modified recurrence override, multiple or excluded recurrence rules, an unknown participant role or status), rides `Page::failed_ids`; the rest of the page still returns. One unrepresentable event therefore costs its own row, not the walk - before this it failed the whole call, permanently, since the event does not go away. The single-event `get` door is unchanged: with no neighbours to protect it still returns the conversion error itself. `search`'s calendar filter is client-side, so when `EventSearchRequest.calendar_id` is set the server's `total` (which counts the unfiltered result set) is suppressed rather than reported as this walk's total; the cursor stays live, and a page whose every hit belonged to another calendar is legitimately empty with more pages behind it. `EventSearchRequest::include_cancelled` (default `false`) is the same kind of client-side post-filter (draft-26 has no status filter condition and `get_events` always hydrates `status`), so the server `total` is likewise suppressed unless `include_cancelled` is set and no calendar filter is; an absent JSCalendar `status` reads as `Confirmed` (the draft-26 default), leaving `Unknown` for unrecognized values only. Both calendar page walks (and both contact ones) page by anchor with a pinned `queryState`, not by integer position - see "PIM page cursors".

`UNTIL` conversion and the start/duration write both need the event start timezone and all-day flag, which an `EventPatch` need not carry. `update` reads the current event (`CalendarEvent/get`, before hydration, so an event whose own recurrence is unrepresentable does not block the patch) for exactly the fields the patch leaves unset: a recurrence-only patch carrying `UNTIL`, or any time patch that does not restate `is_all_day`. Omitting `is_all_day` therefore means "unchanged", not "timed". RFC 5545 `UNTIL` is converted, not copied: basic DATE and DATE-TIME syntax becomes an extended JSCalendar `LocalDateTime`, a zoned event's UTC UNTIL is resolved into the event timezone, and the inbound direction restores DATE for all-day events, floating DATE-TIME for floating events, or UTC DATE-TIME for zoned events. The inclusive bound is unchanged in both directions.
Event time updates: JSCalendar derives end from `start` + `duration`, so a patch must carry both `start` and `end` (recomputing `duration`); a one-bound patch is rejected Unsupported rather than dropping the change or keeping a stale duration. Empty times: the projection yields an empty `start` for "the provider did not say", and this crate's own read path produces one (a server event with no `start` projects an empty start and `CalendarEvent.end` `None`; an absent `duration` is RFC 8984's `PT0S`, so an event with a start always has an end), so a read-modify-write can echo it. An empty (or whitespace-only) `start` is refused as `Request(Malformed)` naming `start` on create and patch, before any request (JSCalendar `start` is a mandatory LocalDateTime). An empty `end` on a patch means "no duration change": JSCalendar stores the end as `duration`, so the patch drops it, still applies `start`/`timeZone`, and leaves the stored `duration` alone (an empty end with no `start` writes no time fields). Encoding it would have sent `PT0S` and silently collapsed the event to zero length. A non-empty `start` or `end` that is not a date, a zoneless date-time or an RFC 3339 (UTC or offset) date-time is refused `Request(Malformed)` naming the field, before any request (`parse_time`, used by `validate_start`, `event_duration` and the `start` writer `jmap_time_from_shared`, which has no raw fallback, so no caller can put unparsed text on the wire); an all-day event additionally refuses offset date-times. An `end` before its `start`, or a timed pair mixing a zoneless date-time with an offset one (or a date with either), is refused naming `end`. `event_duration` has no fallback duration: nothing can write a `PT0S` for a time it could not read. An empty `end` on a create is refused `Unsupported` (an absent or `PT0S` duration is a zero-length event the caller never asked for), matching bifrost-google. All-day ends follow the exclusive `EventTime` contract: inbound end is `start + duration` days and outbound `duration` is `end - start` days (a single all-day event is start D / end D+1 / `P1D`), uniform with caldav/google/graph. JSCalendar has no DATE type, so an all-day start is written as midnight on its date (`2026-06-02T00:00:00`) with `showWithoutTime` carrying the all-day sense, and an inbound all-day time is truncated back to the shared bare date. `events_in_range` bounds become the query's `after` / `before` LocalDateTimes in the default Etc/UTC: an offset date-time is normalized to UTC, a bare date is midnight, a zoneless date-time passes as itself, and an empty or unparseable bound is refused `Request(Malformed)` naming `start` or `end` before any request, as the google and caldav range reads refuse theirs.
JSCalendar `alerts` project onto the read-only `CalendarEvent.reminders` surface (OffsetTrigger -> relative offset with `relativeTo`; AbsoluteTrigger -> absolute `when`; alert `action` carried through). Privacy maps JSCalendar `privacy` <-> visibility (`secret` -> `Confidential`); status maps `EventStatus` <-> `status`. Organizers map to/from JSCalendar owner participants (created events write the organizer as `owner`). `freeBusyStatus` standardizes `free`/`busy`, so shared Tentative/OutOfOffice serialize as busy.
An attendee patch is a whole-map `participants` write, and JSCalendar keeps the organizer in that same map, so `update` reads the current event whenever the patch carries attendees (`event_patch_needs_current`) and merges the owner-roled participants back in under their existing keys: an attendee whose email matches an owner entry updates its name and participation status in place (keeping its owner role - the read path surfaces an owner as a `Chair` attendee, so a read-modify-write round trip stays stable), and fresh attendee keys never collide with a kept one.
RSVP resolves authenticated email aliases from Basic credentials and RFC 9670 Principal/get when available, then patches the matching participant's `participationStatus` by dotted path; without a known email it falls back to the single non-owner attendee, and ambiguous events return unsupported. Shared attendees write `expectReply: true`; `ROLE` / `CUTYPE` information represented by the shared attendee role maps through JSCalendar roles. Unknown outbound shared roles or statuses and unknown enabled inbound JSCalendar roles or participation statuses return `Unsupported`, rather than acquiring a plausible default. If the server lacks the relevant capability, flags are false and calls return JMAP-stamped `Unsupported`.

## Error model

The crate-internal JMAP error type uses structured variants. No
`Error::Internal(String)`:

- `Transport(TransportError)` - wraps transport errors. `From<TransportError>` parses a carried body as ProblemDetails and, when it parses, produces `Problem { details, transport }` instead, keeping the transport error for its status and retry evidence.
- `Problem { .. }` - an RFC 7807 problem-details response. An omitted `type`
  decodes as `about:blank`, as RFC 7807 §3.1 requires, while retaining its
  status, title and detail.
- `Method(MethodError)` - JMAP method-level errors. `Set(SetError)` - per-object set errors.
- Local request refusals: `RequestEncode`, `RequestCallLimit { max }`, `RequestSizeLimit { max, size }`, `NoPrimaryAccount { capability }`.
- Response-shape failures: `ResponseDecode`, `CallNotFound`, `UnexpectedMethodResponse`, `IdNotFound`, `NotParsable`.
- `InvalidUrl` - a URL or URL-component value the crate's own caller supplied is unusable. Sources include an empty session URL and an empty EventSource type list.
- `MalformedSessionUrl { property, detail }` - a URL or URL template the SERVER published in its session is unusable: `apiUrl` and the `uploadUrl`, `downloadUrl` and `eventSourceUrl` templates (an unbalanced or unknown template variable, or not an absolute http or https URL; `eventSourceUrl` raises only when EventSource is used) and the WebSocket capability's `url` (an unparseable URI or a non-websocket scheme). The provider described itself wrongly, so this is never the caller's `InvalidUrl`.
- `MalformedCapability { capability }` - an advertised capability whose object does not parse.
- Behind the `websockets` feature: `WebSocketHandshake`, `WebSocketRuntime` (a read-side failure after the handshake), `WebSocketSend` (a failed frame write to the connection's sink), `WebSocketClosed`, `WebSocketSetup(WebSocketSetupError)`, `WebSocketNotConnected`.
- No `From<reqwest::Error>` - reqwest errors converted to TransportError at point of use. No `From<tokio_websockets::Error>` either - see "Error translation".

Every variant's classification is listed under "Error translation" below.

## PatchObject null semantics

RFC 8620: `null` removes map keys, not `false`. Email `patch` field uses `HashMap<String, serde_json::Value>` with `Value::Null` for removals.

## JMAP-specific code style

- `#[serde(skip_serializing_if = "...")]` on optional fields.
- `Field::is_omitted` for `skip_serializing_if` on `Field<T>` fields (with `#[serde(default)]`).
- `SetObjectCreatable::new()` initializes optional fields to `None`/`Omitted`, not empty collections.
- Helper impl blocks use `impl<Tr: HttpTransport> Client<Tr>` (not bare `impl Client`).

## Account layer

The `bifrost_types::Account` impl under `crates/jmap/src/sync/` (gated on `sync`) is engine-facing only: the sync tree depends on `bifrost-types`, not `bifrost-sync`. It wraps the JMAP client (`Client<ReqwestTransport>` + a `Mail`-capability `Account` handle) and maps each `Account` trait method onto one or more JMAP calls.

Cursors are protocol-tagged opaque bytes (hand-rolled length-prefixed format), scope-aware in establishment and change streams. Push uses the JMAP WebSocket subprotocol with a single reader task and broadcast fan-out.

### Module layout

```
crates/jmap/src/sync/
  mod.rs           - public keep-list re-exports (JmapAccountFactory,
                     JmapAccountFactoryBuilder, JmapCredentials, ReconnectPolicy)
  account.rs       - pub(crate) JmapAccount struct + impl Account
  factory.rs       - JmapAccountFactory + builder + JmapCredentials
  capabilities.rs  - AccountCapabilities builder, CoreLimits
  state.rs         - cursor envelope (V1 tag/length format)
  state_cache.rs   - per-accountId Email/Mailbox state maps
  foreign.rs       - foreign-account codec: mailbox (encode_foreign/parse_foreign/owner_tag)
                     and object ids (encode_object/parse_object/native_object)
  discover.rs      - cursor_scopes / memberships / scope_lifecycle_stream
  inventory.rs     - per-scope inventory streaming
  changes.rs       - per-scope change stream dispatch
  state_walk.rs    - shared progress guards, error contexts, and per-walk recovery policy
  hydrate.rs       - get_stream projection logic
  push.rs          - WebSocket push, ReconnectPolicy, subscribe/unsubscribe
  mutation.rs      - bulk_set_flags / bulk_move / bulk_destroy pipeline
  pim.rs           - unified PIM primitives: mail mutations, send,
                     drafts, search, containers, settings, hydration
  contacts.rs      - AddressBook / ContactCard primitives
  calendar_ops.rs  - Calendar / CalendarEvent primitives
  page_cursor.rs   - the anchored, state-pinned page cursor both of the
                     above share (see "PIM page cursors")
  filters.rs       - SieveScript-backed server-side filter scripts
  blob.rs          - open_blob / open_blob_range / open_raw_rfc822 (Email/get blobId + download,
                     correlated on the submitted id rather than the head of the echoed list)
  error.rs         - to_recovery / to_account_error mapping
```

### `JmapAccount` / `JmapAccountFactory` shape and lifecycle

Push close uses the same `subscriptions -> enabled -> push_state` lock order as
subscribe and unsubscribe. On its first poll, `close()` marks the account
closed before acquiring those locks, so a subscribe already holding them can
finish and is then disabled; a subscriber that acquires them later observes
the closed flag and is refused before it can re-enable push. Teardown runs in a
task stored behind a shared completion: dropping a close future before its
first poll starts nothing, while dropping one after its first poll does not
cancel shutdown and later callers await the same result.

`JmapAccountFactory` carries a `JmapAccountFactoryBuilder` config (URL,
`JmapCredentials::Basic`/`Bearer`, optional `Net` via `with_net`, timeout,
`accept_invalid_certs`, `ReconnectPolicy`). `with_net` is available on both the
factory and builder and supplies the HTTP transport used for every open.
Without it, HTTP attaches to `Net::shared_for_tls(accept_invalid_certs)`.
The factory's `accept_invalid_certs` setting also configures the separately
built WebSocket TLS connection when WebSocket support is enabled; when a
custom Net is supplied, its HTTP trust settings come from that Net.

`AccountFactory::open(account_id)` connects a `Client`, passing the engine
account id into the `bifrost-net` attachment so metering / priority / caps /
trace use the real key on reopen. Open resolves the primary `Mail` account
plus the optional `Submission`/`VacationResponse`/`Quota`/`Sieve`/`Contacts`/
`Calendars` handles through `resolve_optional_families` (which gates each on
its own capability block's lane; see "Absent, Malformed, Present" above),
reads the session, and then runs `validate_and_seed`: the session-only half
of the capability validation (`capabilities::validate_session` - the core
block's lane and its mandatory limits, plus the required mail block's lane)
FIRST, and only then the initial `Email/get` and `Mailbox/get` (including
names) probes, batched into one request for the primary and one request per
foreign account. The order is load-bearing, not incidental: validation used
to run only inside `capabilities::build`, after the whole seeding stage, so an
open destined to be refused as a contract violation first spent two primary
probes plus one per share against a server already known non-conformant and
discarded every answer. The half of the validation that genuinely needs
probe results - the `PimSupport` gates, which depend on which shares actually
seeded - stays in `build`, which still calls `validate_session` itself so the
refusal does not depend on the caller having run it. `build` then produces
`AccountCapabilities` + `CoreLimits`. It spawns the WebSocket reader with a
`CancellationToken`, and returns `OpenedAccount` (the handle plus the
foreign-account skip lane; see Foreign accounts below).

The batch is conditional on the session's own numbers. `maxCallsInRequest` and `maxSizeRequest` are hard limits (RFC 8620 §2) the server enforces by rejecting the whole request with a request-level `limit` error, and the commonly quoted 16 calls is the minimum a server is *recommended* to support, never a floor a client may assume. So `batched_open_probes` sends the two-call batch only when the advertised call count allows it and `Request::send_methods_within` finds the encoded request inside `maxSizeRequest`; otherwise nothing is sent and the probes go out one request each. `send_methods_within` takes the size limit as a required argument (and returns `Ok(None)` without sending when the batch does not fit) precisely so a future batching call site cannot forget the question.

`JmapAccount` (`pub(crate)`) owns the `Client`, the primary `Mail` `Account` handle, a `foreign_mail: Arc<HashMap<String, MailAccount>>` of shared/delegate-account handles keyed by JMAP `accountId`, optional `Submission`/`VacationResponse`/`Quota`/`Sieve` handles, the built capabilities, per-scope cursor seed states, the `WsState`, a subscription registry, and per-`accountId` Email/Mailbox state caches (each `Arc<Mutex<HashMap<String, Option<String>>>>` via `state_cache.rs`). The maps are keyed by accountId because JMAP state is per-`(accountId, type)`; the primary account's id is one ordinary key. Scope lifecycle has a separate primary-only Mailbox state map: delta polling and local `Mailbox/set` may fast-forward the general cache, but neither consumes the lifecycle poller's window. This lifecycle position is open-time process state, not an engine change cursor, so it does not enter the cursor envelope or alter its schema/version. `set_priority`/`set_bandwidth_cap` delegate to `bifrost-net::AccountNet`. The account also holds a shared close completion: the first polled `close()` starts teardown in a spawned task, and all later callers await that same result.

`Client` carries an optional `ByteTally`; `Client::metered()` / `Account::metered()` hand back a handle over the same transport and session state that reports inbound request bytes into a fresh accumulator, which each sync stream takes at construction and each emitted batch `take`s. `Client::metered_into(&tally)` / `Account::metered_into(&tally)` report into an existing accumulator when one batch crosses differently scoped handles. The tally lives outside `ClientInner` deliberately: forking the inner would fork the session-state mutex, and two clients with independent session state is a correctness bug rather than an accounting detail.

Buffered request counts reach the client through `HttpTransport::api_request_measured` and `HttpTransport::download_measured`, both defaulted trait methods. Their default reports decoded body length, and `ReqwestTransport` overrides them with bifrost-net's request-local counter across retries, redirects and 401 recovery. The incremental `BlobTransport::download_stream` door reports its request-local counter as body chunks are polled. The sync blob and raw-message facades choose the 512 MiB ceiling and take the tally once per emitted chunk batch; for raw reads, the preceding `Email/get` bytes are attributed to the first chunk or, for an empty body, its empty final batch. They retain one prior chunk while polling its successor for final-boundary lookahead. A transfer error terminates without `Done`, so the consumer discards any incomplete prefix. Uploads stay unmetered: the tally is inbound-only, and an upload's inbound leg is the small JSON acknowledgement already counted by the API door.

A guarded bulk batch can require up to four API requests: an initial state probe, `Email/set`, a re-probe after `stateMismatch`, and one retry. For JSON sync paths, each batch emitted after a method-level error carries the measured response bytes; a transport-level error terminates without a batch. Blob streams instead account complete chunks as they are yielded and leave any failed tail out of batch accounting. See "Foreign (shared/delegate) accounts" below.

Reopen is engine-delegated: on drop or `close()`, the engine calls `JmapAccountFactory::open` again. `close()` cancels the shutdown token (terminating the WebSocket reader and in-flight streams), disables push, and then JOINS the spawned reader task, so its return really is evidence the reader stopped - `WsState` retains the `JoinHandle` for exactly that. Both steps are bounded by `ReconnectPolicy::connect_timeout`: a WebSocket sink write and a task join both park indefinitely on a half-open socket, and teardown blocking there would block the engine's reopen on a connection that is already gone. An unsubscribe that times out is not a close failure (the server drops the subscription with the connection); a reader that outlives the bound is abandoned rather than aborted, since aborting at an arbitrary await point is no safer. The `closed` flag short-circuits later calls, and the join handle is consumed by the first close. Cancellation safety relies on the shared `CancellationToken` plus `tokio::select!`; no `Account` method holds non-cancel-safe state across an await.

### Capabilities advertised

`capabilities::build` reads the core and websocket capability STATES (see "Absent, Malformed, Present" above - it refuses a malformed required block, warns per malformed optional one, and derives push from the websocket three-state) to construct `AccountCapabilities`:

- `cursor_freshness: ServerIssued`; `blob_range: No`; `blob_digest_pre_download: false`.
- `push: InProcess` when the session advertises `urn:ietf:params:jmap:websocket` with `supportsPush: true`, else `None`.
- `mutation.concurrency: ExpectedVersion` enables mail flags, move and destroy,
  plus supported contact/event updates, deletes and event RSVP.
  Versioned bulk mail accepts caller `StateAt` and sends it as `ifInState` on
  the owning account's `Email/set`. The first `stateMismatch` is a per-target
  conflict, without a state probe or refresh/retry. With no caller token,
  provider-observed `ifInState` still protects whole-property replacements
  `FlagOp::Set` and `bulk_move`; dotted patches and destroy omit that guard.
  `mutation.replay_safety: None`. `mutation.reassertion` declares
  `set_flags` and `destroy`: ids are immutable and `Email/set` keyword patches,
  keyword replacement and destroy converge when reapplied (see "Flag
  observation and reassertion capability").
- `batching_policy`: `max_items = core.maxObjectsInSet` clamped `[1,500]`, `max_wait: 100ms`, `flush_on_input_close: true`.
- `rate_limit_class: Generous`; `quota_signal: None` (quota via `quota_get`); `requires_uidvalidity_recheck: false`; `historyid_/delta_token_expires_after: None`; `discovers_foreign_namespaces_on_rediscovery: true` unconditionally, because shared-account scopes are discovered from the session at open, there is no foreign scope-lifecycle stream, and no session signal can prove a server will never grant a share (the accounts list is only the current grants; RFC 9670 principals support is sufficient but not necessary evidence of sharing).
- `pim_methods` advertises mailbox membership add/remove, keyword/read-state mutation, `set_importance`, attachment upload, draft lifecycle, search, mailbox CRUD, identity list/update (Submission), vacation get/set (VacationResponse), quota get (Quota), and thread/message hydration. `scheduled_send` is true iff the selected submission account's `accountCapabilities` block is PRESENT, parseable, and advertises `maxDelayedSend > 0` (onto `PimSupport.max_delayed_send` for boundary validation). The session-level Submission object is an empty support marker. A malformed account-level Submission block does not degrade to `maxDelayedSend = 0`: `SubmissionCapabilities` is not `#[serde(default)]` at the container level, so a block omitting the mandatory RFC 8621 s1.3.2 `maxDelayedSend` fails to parse and lands in the malformed lane, where the family gate takes the whole submission family (send, drafts send, identities, `scheduled_send`) off with a named warning. A well-formed block advertising `maxDelayedSend: 0` is a different server and keeps the family, with only `scheduled_send` false. Gmail labels, Graph categories/extended properties are false.
- `filter_rule_shape: Scripts` and every filter flag true with a primary Sieve account; without Sieve they are false and the shape is `None`.
- `conveniences`: `starred = Keyword`, `replied`/`forwarded`/`mdn_sent` via keyword true, extended-property routes false. `set_starred`/`mark_replied`/`mark_forwarded`/`mark_mdn_sent` map to `$flagged`/`$answered`/`$forwarded`/`$MDNSent`. `set_importance` is two-valued: `High` sets `$important`, else clears; read maps `$important` presence onto `Message.importance`.

`CoreLimits` holds `maxObjectsInGet` and `maxObjectsInSet` - the only two limits the JMAP `Account` impl actually reads. `build` reads the three-state `core_capability_state()` and refuses on three grounds, in two different lanes: an ABSENT core block is `SyncState(CapabilityChanged)` -> `RestartAccount` (the server dropped a capability it used to advertise, so a reopen may fix it), while a PRESENT block that does not parse, and a present block whose `maxCallsInRequest` / `maxObjectsInGet` / `maxObjectsInSet` / `maxSizeRequest` is zero OR omitted, are both `Protocol(ContractViolation)`. Omitted counts because RFC 8620 §2 makes every one of those a mandatory member; classifying either as a capability change would buy a reopen loop against a session that will come back identical. `maxCallsInRequest` and `maxSizeRequest` are validated and discarded.

### Cursor envelope

`OpaqueChangeState` for JMAP is tagged with `ProtocolKind::Jmap` and `envelope_version = PAYLOAD_ENVELOPE_VERSION` (currently `2`). `OUTER_CURSOR_ENVELOPE_VERSION` is the matching `ChangeCursor.envelope_version` - the two consts sit on different axes, hence the `PAYLOAD_`/`OUTER_` prefixes. v2 is an OBJECT-ID encoding change, not a payload-shape change, exactly like graph's v2: v1 minted foreign thread ids bare, and a bare id still parses - as PRIMARY - so no additive field can detect one. A v1 cursor is therefore refused: `state::decode` distinguishes an OLDER envelope (`SchemaIncompatible` - reseed) from a FUTURE one (`CursorEnvelopeUnknown`), the refusal is pinned at the `changes_stream` door with its derived `Engine(SchemaIncompatible)` directive, and the engine's schema-clear deletes every durable checkpoint row of the account - change cursors and every backfill checkpoint, completion markers included, for scopes the engine does not track as well - so the next attach re-walks inventory and re-mints the ids - reseeding is the migration. The door classifies only genuine schema drift that way: a MIS-KEYED row (wrong protocol tag, or a payload scope disagreeing with the `ChangeCursor`) is a consumer/store bug, classified `SyncState(CursorInvalid)` -> `Engine(RestartScope(scope))` so the one bogus row is deleted and re-established without paying the account-wide clear and full re-hydration. Within the clearing session itself the re-established cursor covers changes from the open-time state onward only; the consumer's stored bare thread ids heal at that next-attach re-walk.

The payload is hand-rolled, length-prefixed bytes (little-endian `u32` lengths, single-byte tags):

```
state-tag:u8        // STATE_TAG_V1 = 1
scope-tag:u8        // 1=Email 2=Mailbox 3=Thread 4=Query 5=Folder
[query-id:length-prefixed-utf8 when Query]
[account-id + mailbox-id:length-prefixed-utf8 (each) when Folder]
state-string:length-prefixed-utf8
```

`JmapCursorState::V1 { scope: JmapScopeRepr, state_string }` is the only current variant. Its envelope can decode `Email`, `Mailbox`, legacy `Thread`/`Query(String)`, and `Folder { account_id, mailbox_id }` (a foreign account; round-trips through the `foreign.rs` codec). The SEEDED foreign shape is account-level - `encode_foreign_account(accountId)`, an empty `mailbox_id` (unambiguous: RFC 8620 ids are 1-255 chars) - one scope per share, because `Email/changes` state is per `(accountId, type)`. A legacy per-mailbox `Folder` cursor (non-empty `mailbox_id`) still decodes and still drives correctly if handed to `changes_stream`, but is no longer seeded or discovered. Discovery only exposes Email, Mailbox, and foreign Folder scopes; legacy Thread and Query cursors terminate unsupported rather than silently taking an unseeded or undefined path. `SCOPE_TAG_FOLDER = 5` was additive when it landed (tags 1-4 kept decoding, no bump); the later v2 bump was forced by the thread-id encoding change, not by this tag.

Validation rules in `state::decode`:

- Wrong `ProtocolKind` returns `Error::CursorProtocolMismatch`.
- Unknown `envelope_version` returns `Error::CursorEnvelopeUnknown`.
- Any decode failure (unknown state tag, unknown scope tag, truncated payload, trailing bytes, non-UTF-8 string) returns `Error::SchemaIncompatible`.
- `decode_cursor` additionally rejects a payload whose embedded scope does not match the `ChangeCursor.scope`, returning `Error::Other`.

`open()` probes and records which cursor scopes this account can drive. `establish_initial_cursor(scope)` uses that seed only as a membership gate, then probes a fresh `Email/get ids: []` state for Email and foreign `Folder` scopes or `Mailbox/get ids: []` for Mailbox immediately before returning `CursorEstablishment::Ready`. The per-account state cache advances compare-and-swap from the value captured before the probe, so a concurrent writer cannot be replaced by a stale probe result. Re-probing matters after `RestartScope`: returning an open-time state that may have expired can make `cannotCalculateChanges` restart the same scope against the same expired state indefinitely. Unseeded scopes return `Error::Unsupported` rather than a cursor over an empty state, which would present as a working cursor that returns nothing forever. `describe_cursor` reports `Cheap`/`ServerCursor` only for decodable cursors on scopes the change stream drives (Email, Mailbox, foreign `Folder`); legacy Thread/Query cursors still decode but terminate unsupported, so they - like undecodable cursors - report `Expensive`/`None` rather than promising a strategy that dies on its first poll.

### Per-scope inventory, changes, hydration

Supported scopes for `inventory_stream` and `changes_stream`:

- `CursorScope::Type(ObjectType::Email)` - inventory walks `Email/query` (`receivedAt` desc) then `Email/get` with the fixed inventory property set. The first query starts at position zero; every later page uses the previous page's last id as `anchor` with `anchorOffset: 1`. The walk allows `queryState` to move: immutable `receivedAt` plus the stable tie ordering required by RFC 8620 s5.5 keep surviving messages on the same side of the anchor. New arrivals or membership moves behind it are recovered through the change cursor established before inventory. `anchorNotFound` on an Email/query that sent an anchor is classified as `SyncState(CursorInvalid)` -> `RestartScope`, without `Done`; Email/get has the same cursor scope but sends no anchor and keeps the protocol contract-violation mapping. Other errors likewise terminate without `Done`; only an empty query completes the walk, even if its state moved. A short page does not prove completion: RFC 8620 s5.5 permits servers to clamp the requested query limit independently. Contact and calendar page walks still pin state because their sort properties can change - see "PIM page cursors". `inventory_partitioning` reports `Full`: a positional `PageCount` plan would lose the anchor between engine partition calls and reopen the deletion-shift skip at every boundary. Explicit page partitions are refused rather than claiming coverage. Primary and foreign-account inventory share ONE loop, `inventory::email_inventory_loop`, exactly as the change walks share `changes::changes_walk`: the only parameter that varies is the `owner` tag - `Some(accountId)` for a foreign scope, which qualifies the emitted ids and memberships into the owning account's namespace AND routes a permission denial into a per-scope quarantine, and with `None` reduces to the plain error mapping the primary walk always used, since `shared_scope_error` with no owner is `into_account_error`.
- `CursorScope::Type(ObjectType::Mailbox)` - inventory pages `Mailbox/query` (sortOrder, then name) by the previous page's last id as `anchor` with `anchorOffset: 1`, then fetches each page with `Mailbox/get` at `maxObjectsInGet` (Id, Name, ParentId, Role, SortOrder, totals, unread counts, IsSubscribed). It never sends an unbounded `Mailbox/get ids: null`, which can exceed the server's `maxObjectsInGet`. The walk continues after short query pages and completes only on an empty query, just like Email inventory. Changes use `Mailbox/changes`.
- `CursorScope::Type(ObjectType::Thread)` and `CursorScope::Query(_)` are not discovered. A legacy cursor for either terminates unsupported: thread inventory derives from email inventory, and the v1 trait has no registered query definition to supply an `Email/queryChanges` filter/sort.
- `CursorScope::Folder(FolderId(encode_foreign_account(account_id)))` - a foreign (shared/delegate) account, one account-level scope per share. Inventory paginates an UNFILTERED `Email/query` against the foreign account handle (one walk per share); changes use that account's `Email/changes`, which is account-wide and cannot be filtered by mailbox - which is exactly why the topology is one scope per account, never one per mailbox (a per-mailbox topology streamed the identical change set once per mailbox and fanned every foreign push out M ways). Per-mailbox membership is learned at hydration from the qualified `mailboxIds`, the same model the primary `Type(Email)` scope uses. A legacy per-mailbox `Folder` cursor still decodes and takes an `inMailbox`-filtered inventory walk, but is never seeded. See "Foreign (shared/delegate) accounts".

Every change-stream batch carries a `Checkpoint::Change(ChangeCursor)` whose state string is the post-call `newState`. The loop continues until `hasMoreChanges` is false, then emits `SyncEvent::Done`. A forward-progress guard terminates the loop as a `Protocol(ContractViolation)` when a server answers `hasMoreChanges: true` with `newState == sinceState` (RFC 8620 s5.2 requires `newState` to reflect the served changes) - the guard fires before that page's batch, so nothing is lost: the state never moved and the next drive replays from it. A second guard covers the case the first cannot see: a server alternating between two states moves the state at every single step, so each step looks like progress while the walk paginates forever. Each change walk therefore remembers the states it has served and terminates the same way when one comes back. The Email, Mailbox and foreign `Folder` scopes all run ONE paginated walk, `changes::changes_walk`, generic over the `*/changes` method (`core::changes::ChangesMethod`, implemented by `define_changes_method!` so a new `*/changes` object gets it for free). Three things vary and are parameters, not copies: the method (whose `NAME` is also the guard's diagnostic), the `owner` tag - `Some(accountId)` only for a foreign scope, which both qualifies the emitted change ids into the owning account's namespace AND routes a permission denial into a per-scope quarantine, and with `None` reduces to the plain error mapping the primary walks always used - and the per-`accountId` state cache the scope advances. `StateWalk` in `sync/state_walk.rs` owns the progress guards, recovery policy, and error context for changes, lifecycle, and inventory walks. Change and lifecycle walks share the guard for `hasMoreChanges: true` with an unmoved or repeated state; the lifecycle guard resets at each poll pause. Inventory does not send a prior state and accepts `queryState` churn, but the same type owns the anchor guard and refuses a page that re-serves the prior anchor, terminating as a `Protocol(ContractViolation)` WITHOUT a `Done` rather than recording unproved coverage. Change and lifecycle requests are marked state-based. Inventory query contexts also mark whether that specific query sent an anchor; get contexts carry only cursor scope. State expiry in durable changes restarts the scope through the engine, lifecycle reconciles a fresh mailbox snapshot locally, and an inventory anchor loss restarts its scope. `cannotCalculateChanges` or `tooManyChanges` on a stateless inventory query or get is a `Protocol(ContractViolation)`. The per-`accountId` `state_cache` maps advance compare-and-swap style (keyed by the scope's accountId) so a stale writer does not clobber a newer state.

The CAS is exact: an expected state advances only an entry containing that state. It cannot initialize an absent or explicitly empty entry. Only an advance with no expected state may initialize those shapes.

`changes_walk` advances the shared state cache before yielding its batch. This may put the cache ahead of the engine's durable checkpoint if the stream is dropped between those steps. That is intentional: the cache is only an observed-state hint for mutation `ifInState` calls, while the engine cursor advances from the checkpoint in the batch. Replaying from an older durable cursor still requests the same change page; a stale mutation precondition is rejected by JMAP and refreshed.

`get_stream` (hydration) supports `Projection::FlagsOnly` and `Metadata` for Email; raw-MIME projections fatal-unsupported (whole-message raw is `open_raw_rfc822`: one `Email/get` for `blobId`, then streamed blob batches). Batches size at `max_objects_in_get` and partial route buffers flush after the JMAP batching policy's 100 ms maximum wait, even while the input stream remains open. Hydrated emails emit `ItemOutcome::Succeeded`; locally-invalid ids or transport-drop ambiguity flow through `Failed` / `Uncertain` rather than terminating the stream. A foreign-qualified id's `Metadata` entry is qualified exactly as the foreign inventory mints it - memberships re-encoded `Folder(encode_foreign(accountId, mailboxId))` plus the `Mailbox(accountId)` owner tag, entry and blob ids in the object namespace. This is the attribution channel for foreign changes: the account-level change stream emits only qualified ids, and hydration is where the consumer learns which shared folder a message sits in.

Each `Email/get` answer is reconciled against the ids the batch submitted (`hydrate::reconcile_hydration`, pure and unit-pinned), so every submitted id leaves on exactly one lane. Repeated submitted ids remain repeated submissions: reconciliation drives from the submitted slice, and one correlated response object supplies one outcome for every occurrence. `notFound` alone cannot carry that: an absent `notFound` decodes as empty (see "`/get` response leniency"), and a present one can still omit an id the server also left out of `list`. Outcomes are keyed by the id the CALLER submitted - a response object whose id was not requested, was already answered, or is missing entirely is discarded rather than minted into an outcome. Ids named in `notFound` emit `Failed` with `NotFound(Message)`; ids answered in neither list emit `Failed` with `Protocol(PartialResponse)` + `Attempt(Acknowledged)`, which the shared recovery mapping retries rather than dropping (a terminal contract violation would lose the id for a condition the next `Email/get` usually clears). `contacts::get_cards` reconciles the same way, routing both classes into `Page::failed_ids` so the consumer preserves the row instead of reading absence as a deletion.

#### `/get` response leniency

`GetResponse::not_found` carries `#[serde(default)]`. RFC 8620 s5.1 makes `notFound` mandatory, but implementations omit it when empty often enough that rejecting the body would fail every sibling call in the same request over one absent empty array. Decoding it as empty is leniency, not proof that every requested id was answered - any caller with a closed per-item accounting contract must reconcile against its own submitted ids.

`SetResponse::new_state()` / `into_new_state()` return `Option`, not a fabricated `""`. RFC 8620 s5.3 makes `newState` mandatory, so absence means a non-conforming server, and folding it into an empty string made that indistinguishable from a state a server really did report as empty - a distinction the per-`accountId` state cache gives its own meaning ("probed, empty" versus "never probed"). Every caller now advances the cache only on a present, non-empty state; absent and empty are both "nothing to record", stated as such rather than inferred from a sentinel.

### The WebSocket request door

The WebSocket transport wraps its established TCP/TLS stream in
`bifrost_net::MeteredStream` and passes it to tokio-websockets `connect_on`.
Both directions share the JMAP transport's `AccountNet` budget with HTTP under
the same registry account id. Admission happens below framing: frame headers,
control frames, message payloads and the HTTP upgrade exchange are included;
TLS records are excluded. Successful reads and writes also feed the HTTP
account meter through its sink handle. Large sends reach the underlying stream in bounded,
paced writes rather than one prepaid message burst. The read path charges each
byte once before the library assembles messages; parsing, correlation routing
and push delivery add no admission. Library and TLS buffering below this
boundary remain outside the plaintext smoothing guarantee described in
`reference/net.md`.

`send_ws` puts an RFC 8887 `Request` frame on the socket and returns the `requestId` it assigned. Three properties that door needs, and now has:

- The id counter is on the `Client`, not on the per-connection `WsStream`, so ids do not restart at 0 on reconnect. A per-connection counter meant a late response from the dropped connection carried an id the new connection was about to reuse - correlation that is actively worse than none.
- `WebSocketResponse` decodes `requestId` (RFC 8887 s4.3.4) and `WebSocketMessage::Response` carries it, so a reader can match a response frame to the request it answers. It was previously dropped at decode, which made two in-flight WebSocket requests indistinguishable on the read stream.
- `frame_stream` runs the session-divergence comparison on every response frame's `sessionState`, through the same `Client::note_session_state` the HTTP door uses - including on a frame whose method responses fail to decode, whose session state was still truthful. The WS door previously handed responses up without ever looking, losing staleness detection on the connection that stays open longest.
- A `Response` frame decodes in a single pass. The frame is not an HTTP response body - it carries `@type` and `requestId` beside the envelope fields - so `WebSocketResponse` keeps `methodResponses` as a raw JSON slice and hands it to `Response::from_frame_parts`, which runs the same per-call success/`MethodError` split (`split_call_results`) the HTTP envelope deserializer runs. It previously decoded the array into a `serde_json::Value` tree, rebuilt a `json!` envelope from it, and deserialized that a second time, re-allocating every method response on every frame. The frame dispatch is hand-written (`decode_frame` reads the `@type` discriminator by itself, then deserializes the variant from the same bytes) rather than `#[serde(tag = "@type")]`, because an internally-tagged enum buffers the frame into serde's private `Content` tree before selecting a variant and a `RawValue` cannot survive that buffer - it re-emerges as the newtype token. The same trap applies to every `#[serde(tag)]` / `#[serde(untagged)]` type in this crate: none can hold a `RawValue`, and the failure is a runtime decode error, not a compile error. Unknown `@type` values are ignored for forward compatibility. A malformed known push payload is converted to an `UnknownFrame` notification, which the push reader turns into a broad invalidation so the bad frame cannot hide a state change or terminate the connection. Malformed text envelopes, unclaimed method-response decode failures, and binary frames close the current WebSocket as `WebSocketClosed`, which the push reader treats as a transient disconnect; a correlated method-response decode failure still fails its waiter. Session divergence is still checked on the frame's own `sessionState` before the method responses are touched, so a frame whose method responses fail to decode still reports its truthful session state and still fails its own waiter with `ResponseDecode`.

- A `maxSizeRequest` guard (RFC 8620 s2) on the encoded frame, matching the HTTP door's refusal to send an oversized batch. It is measured on the FRAME, envelope and `requestId` included, because over WebSocket the frame is the request. Only an advertised, non-zero limit is enforced, for the same reason `CallLimit` refuses to enforce its two unusable states - and since the limit is an `Option`, an omitted `maxSizeRequest` declines to enforce without being confused for an advertised zero, which the session validator refuses separately. Over-limit raises `Error::RequestSizeLimit { max, size }` -> `Request(Malformed)` / `ClientBug`; the HTTP door can answer `Ok(None)` against a caller-supplied bound instead because `send_methods_within` has that contract, and `send_ws` has none - it either writes the frame or it does not.

The await side is `PendingRequests`, a map of waiters keyed by `requestId`, held on the `Client` (not the `WsStream`) so it outlives any one connection. `Client::send_ws_awaiting` (and `Request::send_ws_awaiting`, the WebSocket counterpart of `Request::send`) registers a waiter and returns a `PendingResponse` that resolves to the correlated `Response`. `send_ws` is unchanged and still fire-and-forget.

Four properties hold that map together:

- **Registration happens under the WebSocket sink lock, before the frame is written.** A server that answers immediately cannot beat the registration, and a reconnect cannot slip between the two, because `connect_ws` installs the new sink and opens the new pending generation under that same lock.
- **A routed frame is not also yielded.** `frame_stream` hands a `Response` - or a `RequestError` naming a `requestId`, or a response frame whose method responses fail to decode - to the waiter for that id, and yields nothing. The outcome belongs to whoever asked for it; yielding a decode error onto the push stream instead would park that caller until the connection died. Everything else is yielded exactly as before, so the push reader is unaffected: all push traffic, pongs, unrouted responses, and - importantly - a `RequestError` with no `requestId`, which is how a push-enable frame's asynchronous rejection arrives and what the reconnect logic reads.
- **An unknown id wedges nothing.** `resolve` hands the outcome back when no waiter is registered (an id nobody awaits, or one whose caller was dropped between lookup and send), and the reader yields it as an ordinary response frame.
- **Teardown is generation-stamped.** A waiter is dropped -> its registration goes with it (`PendingResponse` has a `Drop` impl), so cancelled or timed-out calls cannot grow the map. A reconnect (`begin_connection`) bumps the generation and fails every waiter of the connection being replaced with `Error::WebSocketClosed` - `Protocol(PartialResponse)` -> `Retry(SameRequest)`, retryable rather than hanging. A read stream that reaches its end fails the waiters of ITS OWN generation only: an old stream drained to EOF *after* a reconnect must not reap a waiter belonging to the live connection, which is the same leak moved one layer over rather than closed.

### Push and reconnect

Push runs through one reader task spawned at `open()` when the session advertises WebSocket push. It connects via `Client::connect_ws`, validates `Sec-WebSocket-Protocol: jmap`, re-applies the subscribed `DataType` union together with the last observed RFC 8887 `pushState`, emits `Reconnected`, and forwards `PushObject::StateChange` as `Invalidated` (`PushSource::JmapStateChange` + `HintPayload::SpecificCursorScope`). A foreign `Folder` scope subscribes as `DataType::Email`, because JMAP subscriptions are data-type-wide across every visible account. On the emit side, `StateChange.changed` is keyed by `accountId` (RFC 8620 s7.1) and the reader routes each entry through a `PushRouting` snapshot built at `open` from the seeded scopes. Primary `Email` and defensive `EmailDelivery` entries map to the `Type(Email)` cursor; if both keys occur for one account in a notification, they emit one hint. RFC 8621 ties EmailDelivery state changes to newly added Email, so the Email cursor covers both. A conforming server filters notifications to the subscribed `dataTypes` under RFC 8887; since this client does not subscribe to EmailDelivery, that entry is not expected from a conforming server. `Mailbox` maps to `Type(Mailbox)`, and a known `Thread` entry is dropped because no Thread changes stream is supported. Other unrecognized primary types degrade to `Unknown` because the client cannot know which tracked scope they affect. A seeded foreign account's `Email` or `EmailDelivery` entry emits exactly ONE `SpecificCursorScope` hint - its account-level `Folder` scope (the routing map holds one scope per foreign account by construction, so a foreign push is one change-stream pass, not a per-mailbox fanout; the engine's reconciler resolves a `SpecificCursorScope` hint by exact cursor lookup and skips scopes with no registered cursor, so the routing needs no membership-index support and a hint for a quarantined scope is a no-op). A foreign `Mailbox`/`Thread` entry is dropped (no cursor tracks foreign mailbox/thread state, and count-only bumps ride alongside the Email entry that caused them); an unseeded accountId is dropped (JMAP state is per-(accountId, type), so no registered cursor moved). Unknown `@type` frames are ignored. A malformed known push payload yields `UnknownFrame`, which emits an `Unknown` invalidation and lets the reader continue. Binary frames and malformed text envelopes close the current WebSocket as a transient disconnect. Stream errors emit `Disconnected` and fall to the reconnect loop.

`ReconnectPolicy { initial: 1s, max: 60s, connect_timeout: 30s, keepalive: 120s }` controls exponential backoff. The backoff resets only when a pass actually READ a message off the connection, never merely for having reached the read loop: the push-enable frame's only failure signal is an asynchronous `RequestError` on that stream, so a server that will never accept the subscription (unknown `dataTypes` value, capability withdrawn, quota) otherwise completes handshake and sink write, gets `Reconnected` announced, rejects, and repeats at `initial` forever - an unbounded 1 Hz handshake storm with a `Disconnected`/`Reconnected` pair per second on the broadcast channel. A frame the peer actually served (a pong counts) is the evidence; an error is not. Every reader exit error is classified through `into_account_error(_, PushStream)`: terminal classes emit `Terminated(AccountError)` and stop the reader for engine reopen; retry classes emit `Disconnected` and continue backoff.

Every await in the reader's lifecycle is cancellation-covered, so `close()` is prompt from any point in it. The two setup awaits - the WebSocket handshake and the push re-enable frame that follows it - run through `bounded`, a `select!` over the shutdown token, a `connect_timeout` sleep, and the operation; the read loop and the backoff sleep select on the token directly. Neither setup await has a protocol-level deadline of its own, and both can park on a half-open socket forever: unbounded, a wedged handshake stalls push for the life of the session without ever reaching the backoff, and a wedged re-enable additionally holds the client's WebSocket sink lock and blocks every `push_subscribe` caller. Exceeding the bound is a transient disconnect (backoff not reset); cancellation stops the reader and drops the in-flight future so no detached task or client resource outlives the account. A re-enable that fails fast ends the pass as a transient disconnect too: the frame's only failure modes are a dead sink or a vanished connection (a server-side rejection arrives as a `RequestError` on the read stream), so reading on would announce `Reconnected` over a link with no applied subscription - silent dead push. `Reconnected` is therefore emitted only after a pass reaches a live connection whose subscription applied. The read loop carries a liveness deadline of its own: it is where the reader spends essentially all of its life, and JMAP defines no application-level keepalive, so on a half-open TCP connection (NAT or firewall silently dropping state, the common fate of a long-lived idle WebSocket) the stream never errors and never closes and the reader parks forever - push dead for the life of the account, indistinguishable to the engine from a quiet mailbox. After `keepalive` of total silence the reader sends a WebSocket ping; the answering pong must arrive within `connect_timeout` or the connection is dropped as a transient disconnect. Pongs are surfaced to the reader as `WebSocketMessage::Pong` rather than swallowed with the other control frames, because they are the only evidence a silent link is still carrying bytes. Any successful frame clears the outstanding probe. The reader talks to the client through the `PushTransport` seam (`connect_push` / `set_push_data_types` / `ping_push`), which exists so this behavior is pinned in-process - the rest of the sync layer still hardwires `ReqwestTransport`.

`push_stream` combines a broadcast for lossy hints with a watch value for terminal health. `Terminated` is latched before its broadcast send, so subscribers that arrive after a terminal first handshake failure still receive it and end; a lagged receiver re-reads the same latch before degrading to the usual coalesced `Invalidated { source: Coalesced, payload: Unknown }`. This preserves the terminal signal even if no subscriber existed when it was sent or the event fell out of the broadcast window.

`subscribe` and `unsubscribe` build the union of all live `SubscriptionHandle` -> `DataTypeSet` mappings and call `Client::enable_push_ws` / `disable_push_ws`. Registry and enabled-union state commit only after the frame send succeeds, with one exception: an `unsubscribe` whose frame hits a dead sink (`WebSocketSend`) commits the removal and returns `Ok`. Push is enabled per connection, so a connection whose sink is dead has no subscription left to narrow, and the reader re-applies the committed narrowed union on its next connection. Reporting it as a failure made the engine's reopen fail its old-side teardown after every WebSocket drop. A `subscribe` on a dead sink is the same deferred success as one before the first connect: in tokio-websockets every send error leaves the stream closed for good, so the reader's next keepalive ping fails, it reconnects, and the re-enable applies the committed set. Push for the new subscription can start up to one keepalive late; failing the call instead let a WebSocket that died during the engine's reopen abort the reopen and pause the account. The premise must be re-verified if tokio-websockets is bumped. Subscribe outcomes account for every submitted position, including repeated scopes; unmapped scopes occupy the failed lane. When NO requested scope maps, the whole call errors (nothing was subscribed) as `Request(Malformed)`, not `Unsupported(PushSubscribe)`: the account advertised `PushCapability::InProcess`, so the fault is these arguments, and `Unsupported` would invite a consumer keying off the kind to downgrade push for the account wholesale. A frame that cannot be sent because no WebSocket is connected yet (`WebSocketNotConnected`) is a deferred success for both `subscribe` and `unsubscribe`: the call commits and returns `Ok`. The sink is unset only before the reader's first handshake and never cleared, and `subscribe` has already refused a server without WebSocket support, so the error means the reader has not connected yet rather than that push is unavailable. The reader applies the committed `enabled` union and `push_state` on every connect, reading them only after installing its sink and behind the guards a subscribe holds until it commits. It used to be a terminal `Unsupported(PushSubscribe)`, and since the engine's reopen aborts on any error from the replacement's `push_subscribe`, a WebSocket endpoint slow to answer during reopen could exhaust `restart_account`'s attempts and pause the whole account, HTTP sync included. Push here is RFC 8887 WebSocket enable/disable only; no RFC 8620 section 7.2 `PushSubscription` object is ever created, so no server-side state outlives a connection. A `SubscriptionHandle` is only a key into the in-memory registry. An orphaned handle retried by the engine against a newer instance is therefore unknown there: `unsubscribe` removes nothing and re-sends the union the new connection already holds, which is idempotent and narrows no live subscription (pinned by a test). A `PushSubscription`-backed mode would have to make the handle carry the server id. `close()` absorbs `WebSocketNotConnected` and `WebSocketSend` from its best-effort push disable as success (`close_outcome`): no connection or a finished one means no subscription is left to tear down. Any other failure is still a close failure.

`scope_lifecycle_stream` polls `Mailbox/changes` against the primary mailbox state, hydrating changed names before advancing its state cache, then emits `ScopeLifecycle::Created`/`Renamed`/`Deleted`. A transient name-hydration failure leaves the state unchanged for replay; terminal/engine classes emit `Terminated`. Renames emit only when the stored and hydrated names differ and carry both names even when the stable scope id is unchanged.

On `cannotCalculateChanges` or `tooManyChanges`, lifecycle fetches a full
`Mailbox/get` name snapshot, diffs it against its known names, emits net
`Created` / `Deleted` / `Renamed` events, then installs the snapshot state and
resumes polling. Unchanged names emit nothing. Transient snapshot failure
retains the old names and state; permanent failure terminates visibly. The
normal poll pause bounds reseed attempts, and each reseed resets the burst
guard. Names and position commit together only after the diff has been yielded;
dropping a partial reseed leaves both unchanged so a new reader can replay it. History expiry is conforming and does not trigger that guard's
provider-contract termination. Changes entirely inside the expired interval
with no surviving scope cannot be reconstructed from the snapshot.

`StateWalk` is the shared policy object for delta, lifecycle, and anchored inventory. It centralizes guards and request context while preserving each recovery path: engine-owned delta scopes restart, lifecycle reconciles names locally on expiry, and inventory tolerates `queryState` churn but restarts on a lost anchor.

This poller uses `StateWalk`'s changes guard for each `Mailbox/changes` pagination burst. The engine drives this stream for the account lifetime, so an unguarded spin would become a permanent hot poll loop. The guard resets at each poll pause because a state may legitimately recur across separate polls. On a guard fault, the stream yields `ScopeLifecycleEvent::Terminated(Protocol(ContractViolation))` and ends, before emitting the page or committing its state.

The follow-up `Mailbox/get` batches at `maxObjectsInGet` on its own. It was previously bounded only incidentally, by the `maxChanges` the changes call happened to carry - which is fed from the same limit, but that is a fact about the call site, not a bound the read may assume. Its `notFound` and its silent omissions are ONE case and take one path: reconciliation drives from the submitted ids, so an id in either lane lands in the created-then-deleted rule below. RFC 8620 §5.1 gives both the same meaning here - the mailbox is not there any more - and a `notFound` id is by construction absent from `list`.

Two shapes decide which event a change is, and both are about what the CONSUMER has already been told, not about which change collection the server filed the id under:

- **An id the poller holds no name for is a discovery, not a rename.** `updated` is not evidence of a prior announcement: the poller's window opens at the state seeded at `open`, so a mailbox created before that state and renamed after it arrives as an `updated` id the names map has never seen. It used to emit `Renamed { old_name: "" }`, asserting a previous name that never existed and handing the consumer a scope to move rather than one to create. It now emits `Created`, which is both the honest event and the only one that establishes the scope. An id in `created` is `Created` regardless of the names map.
- **A create that vanished before it could be read is surfaced as `Created` then `Deleted`.** An id named in `created`/`updated` that the follow-up `Mailbox/get` answers in neither `list` nor at all was destroyed between the two calls. The state commit cannot be withheld for it - the mailbox is gone, so no later poll will ever mention that id again and there is nothing to replay - which is exactly why the create must be surfaced here or be lost to the lifecycle stream forever. Emitting the pair keeps the engine's per-folder cursor model sound: the scope is established and then torn down, which is what happened on the server, whereas a lone `Deleted` names a scope the consumer never had. Ids the same response already reports under `destroyed` are left to that loop, and an id the poller never knew and that was only `updated` produces nothing at all - it was never announced, so it has no lifecycle to close. The 300s poll pauses select on the shutdown token, so the stream ends promptly for a consumer still polling after `close()` (pull-based, so it cannot leak either way). Foreign-account mailbox lifecycle is not polled (see Foreign accounts).

### Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share a `mutation_stream` engine. Targets batch at `max_objects_in_set` clamped to `[1, 500]`. Dotted keyword operations (`Add`, `Remove`, `Patch`) and destroy omit `ifInState`: an account-wide Email state change does not invalidate these requested operations. `FlagOp::Set` and `bulk_move` retain `ifInState(current_state)` because they replace the whole `keywords` or `mailboxIds` property and could overwrite a concurrent edit. Only those guarded operations probe current state and retry once after `stateMismatch`; other errors abort with `SyncEvent::Terminated(AccountError)`. `set_flags` admits only the four modeled `FlagOp` variants after account validation, and the lower patch builder returns an error for any variant that reaches its fallback. A terminal batch error first emits any locally rejected outcomes already decided for that batch or pending routes, then emits `Terminated`; buffered wire-bound targets remain unprocessed.

An empty additive/subtractive flag operation is a caller error, not a no-op.
`FlagOp::validate_for_account` runs before any routing or batching and rejects
it - along with a wholly empty patch and a patch naming the same flag in both
sets - as a single `SyncEvent::Terminated(Request(Malformed))`. Nothing is
routed, no state is probed, and nothing reaches the wire. The former
`SkipFlags` `mutation_stream` kind, which answered such operations with
per-target `MutationSuccess::Skipped`, is gone with the contract that required
it.

Each bulk target is routed independently: a registered foreign-qualified
object id selects that account's `Mail` handle and its own Email-state cache,
then carries only its native id in the account-scoped `Email/set`. A batch
never mixes accounts. The returned outcome still uses the original qualified
id. An id for an account no longer registered deliberately remains literal on
the primary route so the server supplies the normal not-found result.

`bulk_move` additionally validates the destination against each target's
owner and fails the mismatched target on the item lane as
`Request(Malformed)` (`RecoveryClass::ClientBug`) before anything is sent,
the same call bifrost-graph makes for a cross-mailbox move. The destination
is a single `MembershipScope` for the whole call, so with mixed-owner targets
it can be right for some and wrong for others; owners are compared as
`Option<accountId>` (a bare id is the primary account, and no bare id is ever
minted for a share), independent of whether the named account is still
registered. This is not a politeness check: `Email/set` resolves the message
and the mailbox inside ONE `accountId`, so a bare primary mailbox id sent to
a shared account is not rejected by the server - it names whatever mailbox
that account holds under the same id, and the move reports `Applied` after
filing the message somewhere the caller never asked for.

Per-owner batching is bounded on both axes (`RouteBuffers`). Draining empties
a route in place rather than removing it, so the owner list holds one entry
per account rather than one per input target. A partial batch is flushed once
`batch_size` further targets have gone to other owners, so a quiet share's
short batch cannot be held to end-of-input by a busy one; the wait is capped
at the same input volume a full batch already accepts. A route holding only
locally-rejected targets emits them without probing Email state.

`IdempotencyKey` is a wire no-op, so replay safety stays `None` (read-back observes current state; eligible targets may reassert within bounds). Per-id outcomes flow from `SetResponse::updated`/`destroyed`; an id the server names in neither the success nor error collection becomes `Protocol(PartialResponse)` with `Attempt(Acknowledged)`, so unguarded idempotent flag work retries, guarded flag work reconciles with CheckTarget only, and possibly-applied moves/destroys reconcile rather than being fabricated as `NotFound`. A `stateMismatch` surviving retry emits `Failed(ConcurrencyConflict)`; real per-id set errors map through `classify_set_item`. Empty additive/subtractive flag operations, wholly empty patches, and patches naming the same flag in both sets are rejected up front as `Terminated(Request(Malformed))` rather than transmitting empty patches. `bulk_move` accepts only `MembershipScope::Mailbox`.

### PIM primitives and conveniences

`pim.rs` is the Stage 1 unified mail surface. Message/thread field mutations resolve `MutationTarget::Thread` via `Thread/get`, then use `Email/set` patches against `mailboxIds`/`keywords`/`$seen` without account-wide `ifInState`; the patch names the requested fields and preserves other properties, while an unrelated Email change cannot reject it. Thread keyword and membership updates and Trash destruction split large Email ID sets at the account's bounded `maxObjectsInSet`. The already-in-Trash `delete_thread` branch reads member `mailboxIds` at `maxObjectsInGet`, then sends each `Email/set destroy` with `ifInState`: the first batch uses the membership read's state, and each later batch uses the preceding set's `newState`. A membership change during a multi-get walk or before a destroy therefore refuses that destructive batch; because Email state is account-wide, an unrelated Email change can also reject it. Retrying reads membership again. If a multi-message set partially succeeds before a later batch fails, the operation returns `Protocol(PartialResponse)` with acknowledged transmission evidence so the caller reconciles before retrying. The email state cache advances compare-and-swap style from each response. Gmail labels, Graph categories/extended properties return `Unsupported`. `Thread.emailIds` decodes as optional, because a `/get` may project it away; both readers (`thread_hydrate` and the mutation-target expansion) reject a response that omits an explicitly requested `emailIds` rather than treating it as an empty thread. That expansion failure is reported under the MUTATION's `AccountOperation`, not `HydrateThread` - the thread lookup is an implementation detail of the mutation, and the operation drives the recovery class the engine derives.

`patch_mailbox_membership` (`add_to_container` / `remove_from_container`, and
the two legs of `move_thread` / `delete_thread`) applies the same owner check
`bulk_move` does, for the same reason: the account layer routes by the
TARGET's owner but takes the container id as given, so a bare container id on
a foreign route resolves in the share's namespace. Owners disagreeing is
`Request(Malformed)` raised before `resolve_target` runs. A thread's owner
is decoded from its owner-qualified id, exactly like a message's (the
thread-routing section below); other target shapes are left to
`resolve_target`'s `Unsupported`.

`attachment_upload` stores bytes through the upload URL in the same account selected for draft editing (Submission when present, otherwise Mail), and returns an opaque handle carrying both the blob id and its source account. Blob ids are account scoped: a send to another account uses `Blob/copy` and the destination id before constructing the Email body. Handles without a source account are refused because their owner cannot be established safely. `draft_create`/`update`/`discard` use `Email/set` against Drafts and route through the primary Submission account when it exists, matching `draft_send`'s bare Email id namespace; mail-only accounts retain draft editing on their primary Mail account. `draft_update` sends only its requested fields and omits account-wide `ifInState`, like the other field and path patches. `send_message` creates the draft `Email` + `EmailSubmission` in one result-referenced request, then `onSuccessUpdateEmail` to Sent (or `onSuccessDestroyEmail` when `save_to_sent == Some(false)`). `send_raw_message` imports the uploaded blob before submitting it. The submission result is examined before the preceding Email create/import result, so a malformed Email result cannot hide an acknowledged submission. After email creation or import succeeds, only a submission `notCreated` entry or a method-level error correlated to the submission permits best-effort `Email/set destroy`. Missing, undecodable, or incomplete submission results retain the Email and report `Protocol(PartialResponse)` with acknowledged transmission evidence, requiring reconciliation before another send; a missing or empty created Email id also cannot produce an empty `ObjectId`, and after an acknowledged submission reports a partial outcome. The email state cache advances before any cleanup. `draft_send` submits an existing draft and moves it to Sent, resolving Sent and Drafts from a single `Mailbox/get` via `role_mailboxes`. Its post-submit email-state probe is best-effort because a cache refresh cannot reverse a committed send; a later Email mutation advances the cache from its response. Submission creates omit `undoStatus`, which is server-set by RFC 8621. A scheduled send with no explicit `from` can use the caller-selected `identity`'s concrete email for its envelope; when neither is supplied JMAP has no default Identity to choose, so the request needs `from` or `identity` plus at least one recipient. A `SendRequest::send_as` routes both sets to a successfully seeded foreign account that advertises Submission, resolves a concrete foreign `Identity/get` identity, and forces its `identityId`; `As` forces From to that identity and `OnBehalfOf` adds the authenticated user's Sender when known. Scheduled foreign sends are rejected because their bare submission handles cannot be safely routed through cancel/reschedule. The send-as branch runs BEFORE the primary account's Submission check, so a session whose primary lacks Submission while a seeded foreign account has it reports `pim_methods.send_message == false` with `send_as == true`, and a send-as request through it works. That is the documented contract rather than a mismatch: `send_message` describes the ordinary path only (a request with `send_as: None`), and `send_as` answers for routed sends on its own terms.

After a send with an `onSuccessUpdateEmail` or `onSuccessDestroyEmail`, the
implicit `Email/set` response carries the post-move state. The send paths use
that state for their compare-and-swap cache advance. `draft_send` probes only
when the implicit response cannot supply a non-empty state.

When a Trash-thread destroy spans several `Email/set` requests, each response's
`newState` guards the next batch. A later refusal or incomplete response after
an earlier delete returns `Protocol(PartialResponse)` with acknowledged
transmission evidence, so the caller reconciles Trash membership before
retrying. Thread keyword and mailbox-membership patches use the same bounded
batching and partial-result classification.

A scheduled send validates the created `EmailSubmission` id before returning
it. A missing or empty id after acknowledged creation returns
`Protocol(PartialResponse)`; rescheduling applies the same check after the old
schedule has been canceled. `draft_create` also validates its created Email id
and reports a partial outcome instead of returning an empty `DraftHandle`.

Dotted patch paths are JSON Pointers (RFC 8620 s5.3 / RFC 6901), so every interpolated token routes through `core::set::escape_json_pointer_token` (`~` -> `~0`, `/` -> `~1`) at every path-construction site: `keywords/{keyword}` (IMAP keywords legally contain `/`), `mailboxIds/{id}`, `shareWith/{principal}`, `addressBookIds/{id}`, `calendarIds/{id}`, and the RSVP participant path. New `format!`-built paths must use the same helper.

The `onSuccessUpdateEmail` payload (`EmailPatch::submitted_to_sent`) is built entirely from dotted-path patches, matching RFC 8621 s7.5's own submission example: `mailboxIds/{draftsId}: null`, `mailboxIds/{sentId}: true`, `keywords/$draft: null`. Assigning `mailboxIds` as a whole value would be a *replacement* and would silently drop any unrelated membership the submitted message also holds (a label-as-mailbox filing, a shared folder). When the caller cannot resolve a Drafts mailbox the drafts path is omitted: the message gains Sent and keeps whatever it had, which is the recoverable degradation.

Scheduled send rides RFC 8621/4865 FUTURERELEASE: when `SendRequest::scheduled` is `Some(t)`, the boundary validates `t` against `max_delayed_send`, forces an envelope, and stamps `holduntil` (RFC 3339) as a `mailFrom` parameter. A scheduled `send_message` returns the **EmailSubmission id** (undo-addressable). `cancel_scheduled_send` sets `undoStatus: canceled`. `reschedule_send` first verifies the linked Email still exists, cancels the old submission in its own `EmailSubmission/set`, checks `updated`, then creates the replacement with a new `holduntil`; a refused cancel cannot leave a replacement submission live. A schedule whose Email was destroyed by `onSuccessDestroyEmail` fails this preflight before the existing submission is canceled. Any replacement failure after confirmed cancellation reports `Protocol(PartialResponse)` with acknowledged transmission evidence and reconciliation advice, explicitly stating that the old schedule was canceled and the replacement failed or is unconfirmed.

Search maps the shared `SearchRequest` AST to `Email/query` (query text as a JMAP `text` filter). `search_messages` returns native email ids; `search` uses `collapseThreads = true`, hydrates the emails' `threadId`, returns thread ids. That projection accounts for every submitted email id (`reconcile_search_threads`): an id declared `notFound`, an id answered in neither list, and an email returned without the mandatory `threadId` all ride `Page::failed_ids` instead of being dropped, which used to leave the page silently short of its own result count. Thread ids are deduplicated, since `collapseThreads` is not a guarantee that the server collapsed them. Every known `SearchFilter` variant is translated recursively; the required non-exhaustive catch-all returns `Unsupported(Search)` so a future shared variant cannot silently become an empty JMAP `AND` that matches everything. Page cursors are opaque position bytes, so the search query pins `receivedAt` desc exactly as the inventory walks do: RFC 8621 gives an unsorted `Email/query` a server-defined order with no cross-call stability guarantee, and paging an unstable order by integer position duplicates and skips results. `route_search` picks the account before the query is built, from the owners of the filter's `In` containers (`search_owner` walks And/Or/Not): no `In` or a primary `In` is primary-only, one foreign owner routes both legs to that share and re-qualifies the result ids, two owners or an unreachable owner are refused. The `In` arm therefore strips the owner qualifier on the wire - the native part is a mailbox id in exactly the account the query addresses - and the cursor carries the owner so a resume cannot cross accounts. See "Foreign (shared/delegate) accounts".

Container CRUD is `Mailbox/get`/`set`. Mailboxes surface as `ContainerKind::Folder` (native mailbox id); `Mailbox.role` maps to `FolderRole` (`inbox`->INBOX, `sent`->SENT, `drafts`->DRAFT, `trash`->TRASH, `junk`->SPAM). `container_delete` leaves `onDestroyRemoveEmails = false`, so non-empty deletion fails rather than dropping messages.

Settings use `Identity/get`/`set`, `VacationResponse/get`/`set` (`singleton` id), and `Quota/get`. `identity_update` supports name/signatures/reply-to; default-identity selection is unsupported.

`thread_hydrate` does `Thread/get` then `Email/get` in order. `message_hydrate` selects headers / preview / full projections and returns blob handles without pre-downloading. `move_thread` and `delete_thread` from elsewhere add-to-target then remove-from-source. When the caller says the thread is already in Trash, `delete_thread` expands the thread and reads each member's active `mailboxIds`; only members present in the resolved Trash mailbox are destroyed, so Sent, Inbox, and other copies survive. The move path resolves the thread once and both membership legs use that fixed id set (`patch_mailbox_membership_of`); the cross-account container check runs for both containers before the resolve. Each leg used to run its own `Thread/get`, so a message delivered to the thread between the two resolves was removed from the source having never been added to the target - it lost its source membership and gained nothing. The two-leg non-atomicity itself is the documented cross-provider shape and remains; each leg now applies the same fixed id set in bounded `Email/set` batches.

### PIM page cursors

The crate has TWO page-cursor encodings serving three families of walk. Both
are versioned with a `2:` tag; they are not interchangeable, and each is
decoded only by the module that mints it.

**Mail search (`pim.rs`)** is positional: `2:<position>:<queryState>`,
owner-qualified for a share. It is described under "Known limitations" below.

**The contact and calendar page walks** - `ContactCard/query` behind
`contacts_list` / `contact_search`, `CalendarEvent/query` behind
`events_in_range` / `event_search` - share ONE implementation in
`sync/page_cursor.rs`, and differ only in id type, query-builder type and
diagnostic wording (each supplies them through the `PageKind` trait). They use
an ANCHORED, state-pinned cursor
instead. The payload is `2:` followed by a JSON two-element array,
`[anchor, queryState]`. JSON rather than a delimited pair on purpose: a JMAP
id and a `queryState` are both opaque and either may contain any character a
delimiter could be, so a delimited pair has no unambiguous split, while JSON
escapes its own contents and both halves round-trip verbatim.

- A first page sends `position: 0` and captures the response's own
  `queryState` into the cursor it mints. A continuation sends `anchor` with
  `anchorOffset: 1` (RFC 8620 s5.5) and no position, so churn BEHIND the
  cursor cannot shift the window the way an integer offset would.
- The anchor alone is not enough, which is why the state is pinned too. An
  anchor survives reordering AROUND it: an item that moves from ahead of the
  anchor to behind it is returned twice, one that moves the other way is never
  returned, and neither is visible from the anchor, because the anchor is
  still exactly where the server says it is.
- `verify_query_state` compares the served state against the pin BEFORE the
  total, before minting a successor, and before hydration - unconditionally,
  including on an empty or apparently final page. A mismatch is
  `ConcurrencyConflict` -> `Retry(AfterStateRefresh)`, and the call returns
  `Err`: no items, no successor cursor, no hydration `/get` issued. Checking
  afterwards would have already handed the caller items from a list it just
  decided was the wrong one.
- The successor anchors on the LAST QUERY-RESULT id, never on a
  post-hydration survivor. So a page whose every hit was dropped by calendar
  search's client-side calendar filter, or by `events_in_range`'s local
  overlap predicate, or by hydration, still names where the server continues
  from and the walk goes on correctly.
- A non-v2 payload is refused `SyncState(SchemaIncompatible)` ("restart from
  the first page"), not reinterpreted: that covers the v1 bare integer (which
  under anchored paging would read as either a stale offset or an id named
  "100") and, just as deliberately, an anchor with no pinned state, since
  honouring one is exactly the unchecked paging the pin exists to end. An
  empty anchor id is refused the same way.
- `anchorNotFound` on an ANCHORED page is remapped to `ConcurrencyConflict`:
  an item deleted while a consumer pages is ordinary concurrent activity. On a
  first page, which never sent an anchor, it keeps the central
  `Protocol(ContractViolation)` reading.

Be precise about what the refusal buys. It prevents SILENT acceptance of an
inconsistent continuation - the caller learns the walk broke instead of
receiving a page it cannot tell is short. It does NOT recover the lost
results, and it does not guarantee the walk ever terminates: a sufficiently
busy query can move the state on every attempt and fail repeatedly, the same
limitation mail search already carries. A restart re-reads the earlier pages,
so the consumer must replace its prior result set or deduplicate ids. And a
stable `queryState` pins the ordered ID LIST only - the hydrated properties of
those objects can have changed underneath it. Nor does every edit invalidate a
walk: `queryState` describes the ordered matching ids, so an unrelated
property edit need not move it, though RFC 8620 s5.5 permits a server to
invalidate conservatively when it cannot tell.

**Why the email inventory walk differs.** `inventory::email_inventory_loop`
allows query-state churn while retaining anchor paging. Its sort property,
`receivedAt`, is immutable, and RFC 8620 s5.5 requires server-defined ties to
stay stable between query calls. Existing surviving messages cannot cross
the anchor. The change cursor seeded before inventory covers new messages
and membership changes behind it, including in legacy mailbox-filtered walks.
A destroyed anchor still causes `anchorNotFound` and a scope restart. Pinning
`queryState` here would instead restart from zero on ordinary delivery and
could indefinitely starve inventory. Contact/calendar list pages can sort on
mutable properties and have no seeded changes replay, so retain their pins.

**Termination on an absent `total`: continue-until-empty, in all three walks.**
Every one of these queries sets `calculateTotal: true`, and when the server
answers with a `total` that total is the authority: `position + served ==
total` ends the walk, exactly. When the server omits it - permitted, since
`calculateTotal` is a request, not a guarantee - each of the three walks
continues until an EMPTY page: every NONEMPTY page mints a successor whatever
its length, and only an empty response terminates.

Both alternatives are unsound, and both were live in this crate:

- **End the walk on an absent `total`** (what `contacts.rs` and
  `calendar_ops.rs` did, via a `?` on the `Option`) truncates at page ONE
  against such a server, and reports the truncation as completion.
- **Fall back to page fullness** (what `pim.rs::search_email_ids` did) is not
  a completion test. Note the reason, because this document and three code
  comments used to give the wrong one: RFC 8620 s5.5 does NOT permit an
  arbitrary short non-final page - `ids` runs to the end of the result list
  or to the effective limit, and a server that clamps the requested limit
  must RETURN the limit it used. The honest argument is that an absent
  `total` is not evidence of completion while a conforming EMPTY page is
  conclusive; fullness would additionally have to trust a `limit` echo a
  server may omit, for no gain over asking once more.
  `a_short_non_final_page_still_yields_a_next_cursor` pins the rule on the
  total-bearing branch.

Continue-until-empty is sound for the anchored contact and calendar walks
because the cursor names the last id of a nonempty page, so the next request
either returns more or returns nothing, and nothing is the honest end signal.
It transfers to `pim.rs`'s POSITIONAL cursor because the successor position is
the server's own `position` echo plus what it served. The cost is one extra
round trip against a `total`-less server whose last page landed exactly on
the end.

**An implausible envelope is refused, never read as completion.** A present
`total` used to end a walk on `position + served >= total`, which for a
NONEMPTY page inverts the relationship RFC 8620 s5.5 defines: `position` is
the index of the response's first id and `total` is the length of the whole
result list, so the last id sits at `position + served - 1` and
`position + served <= total` must hold. `>` is a CONTRADICTION, not evidence
that the walk finished, and reading it as completion is the same silent-loss
shape as every other termination defect here. All three walks now validate the
envelope before any termination test, and refuse
`Protocol(ContractViolation)` -> `RecoveryClass::ProviderContractViolation`
on: a negative echoed `position` (the response field is an UnsignedInt); a
nonempty page with `position + served > total`; and, in the anchored walks, an
empty id where a continuation anchor would go. `ProviderContractViolation` is
deliberate over the two neighbouring classes - `ConcurrencyConflict` and
`SyncState(SchemaIncompatible)` both direct the caller to restart, and a
restart re-issues the identical request to the identical broken server, so
the caller would spin. An EMPTY page is exempt from the content rules: with no
first id there is nothing for `position` to index, so a position past the end
of an empty page terminates the walk normally. The comparisons happen in `u64`
rather than `i32`, so an out-of-range `total` is compared instead of being
absorbed into the absent-total branch, and an overflowing position is caught
instead of saturating into a false completion.

**The non-termination residual is detectable, and this document used to say
otherwise.** The claim was that a server which omits `total` and ignores
`position` (or ignores `anchor`) is indistinguishable from an unbounded result
set. That is FALSE: the query state is PINNED and verified equal before any of
this runs, so the server has asserted the result list is one stable finite
ordered list, and a walk that does not advance under an unmoved state is a
contract violation rather than a long list. All three walks now detect it, and
refuse with the same `Protocol(ContractViolation)`:

- `pim.rs` compares the response's `position` against the position it SENT
  rather than trusting the echo. On a positive positional continuation - which
  only a decoded cursor produces, and which therefore always carries a
  verified pin - a nonempty response must BEGIN where it was asked to; there
  is nothing for a conforming server to clamp against under a state that did
  not move. It also refuses a successor position that fails to advance past
  the incoming cursor. That second rule is a real backstop (it independently
  catches a server echoing `0` for a page requested at 2) but the echo rule is
  strictly stronger and runs first, so nothing reaches it through
  `search_email_ids` today.
- The anchored walks refuse a continuation page that CONTAINS the anchor it
  was told to resume strictly after. `anchorOffset: 1` means strictly after,
  and the pinned state says the anchor has not moved, so re-serving it is the
  server re-serving the window it was asked to leave. This subsumes the
  narrower "the newly minted anchor equals the incoming anchor" rule, which is
  the same condition restricted to the last id.

What remains genuinely undetectable is narrower: a server that advances its
positions and anchors correctly while inventing an unbounded stream of new
ids. That is not a paging defect the cursor can see, and it is the same
exposure any consumer of an unbounded query has.

### Server-side filter scripts

`filters.rs` maps Stage 2 filter primitives onto JMAP Sieve:

- `filters_list` runs `SieveScript/query`, hydrates with `SieveScript/get`, downloads each script's blob, returns `ServerFilter::Script`. The query's id list is however many scripts the account holds, so hydration is BATCHED at the session's `maxObjectsInGet` (`factory::max_objects_in_get`, the reader for doors that hold a bare `Account` rather than the `CoreLimits` built at open) - one unbounded `/get` is a call a server may refuse wholesale with `requestTooLarge`. Each answer is then RECONCILED against the ids that batch submitted, the way `reconcile_hydration` and `contacts::get_cards` do. An id the server leaves out of `list` used to vanish from the returned `Vec`, which reads to the consumer as "that filter does not exist" - a deletion the response never claimed, and one an absent `notFound` cannot disprove (see the `/get` leniency rule). This door has no per-item lane, so the whole page fails, retryably: `error::get_id_unresolved_after_query` classifies both an unanswered id and a declared `notFound` as `Protocol(PartialResponse)` with `Attempt(Acknowledged)`, the same class `get_id_unanswered` uses and the same answer `contact_update` gives for a read that materializes nothing. `notFound` deliberately shares that lane rather than taking the terminal `get_id_not_found` one: at page level the honest reading of a script the query named and the get disclaimed is a delete that raced the two calls, and the retry's fresh `/query` will not name it again. The downloads are independent of each other, so they run CONCURRENTLY under `factory::api_request_concurrency` (the server's `maxConcurrentRequests`, clamped `[1, 8]` - the same reading the open-time foreign probes use, deliberately one function rather than a second bound that could drift). Serially this was N+2 round trips for N scripts. It is `buffered`, never `buffer_unordered`: yielding in submission order is what keeps the error accounting identical to the serial loop's, where the first failing script in list order is the error the whole call returns. Arrival order would report whichever download happened to fail first.
- `filter_create` uploads the body as `application/sieve`, creates a `SieveScript`, and `onSuccessActivateScript` when asked active.
- `filter_update` patches name/body via `SieveScript/set` (body uploads a fresh blob first); `is_active` toggles via `onSuccessActivate/DeactivateScript`.
- `filter_delete` destroys the id; `filter_validate` uploads then `SieveScript/validate`, mapping set errors to `FilterValidation` diagnostics.

Typed `ServerFilterCreate::Rule` / `ServerFilterPatch::Rule` return `Unsupported`.

### HTTP redirect handling

`ReqwestTransport` delegates redirects to `bifrost-net` (JMAP trusted-host
allowlist, five-hop limit). `bifrost-net` strips `Authorization` on every
cross-host hop, including injected Basic headers. With an empty JMAP trusted
host list, JMAP selects net's same-origin policy, which rejects changes to
scheme, host, or effective port. The bearer path routes through bifrost-net;
only Basic and the WebSocket handshake build a header via async
`header_value()`.

When `send_streaming` passes through a response that is not successful,
streamed downloads and EventSource preserve its status, headers and bounded
body in `bifrost_net::Error::UnexpectedResponse`. The shared mapper classifies
this as an acknowledged protocol contract violation. These bodies bypass JMAP
Problem Details parsing so they cannot replace that classification.

The WebSocket handshake uses the public `bifrost_net::trace::current_traceparent`
builder, sharing HTTP's current trace policy. Its ReqwestTransport exposes the
actual AccountNet internally for upgrade setup. Net owns the construction-time
proxy snapshot used by reqwest and by `AccountNet::connect_tunnel`: ws/wss use
HTTP/HTTPS routing and bypass rules, with CONNECT through an HTTP(S) proxy or
direct TCP when no proxy applies. Both proxy TLS and origin TLS use the owning
Net's trust configuration, including an injected Net's roots and certificate
policy. Proxy credentials are confined to CONNECT. Redirects remain refused.

### Error translation

`sync/error.rs::into_account_error(error, ctx)` is the single translation boundary: it consumes `crate::Error` plus a `JmapErrorContext { operation, scope, .. }` and emits an `AccountError` via `AccountErrorBuilder`, routing `(AccountErrorKind, Cause)` plus operation, scope, and any `AttemptCause` through `bifrost-types::recovery::derive` (central `RecoveryClass`; no private `to_recovery` table).

Mapping highlights for the JMAP signals the central table reads:

Every method error carries `Attempt(Acknowledged)` (the method response
arrived) and, unless its primary cause already is one, the typed
`WireCause::Jmap(JmapMethod::*)` as a forensic layer.

- `Method(stateMismatch | alreadyExists)` -> `ConcurrencyConflict` +
  `State(ConcurrencyConflict)` -> `Retry::AfterStateRefresh`.
- `Method(cannotCalculateChanges | tooManyChanges)` ->
  `SyncState(CursorInvalid)` -> `Engine(RestartScope)` only when the request
  context is marked state-based and carries an `ErrorScope::Cursor`. A
  state-based context without a cursor scope is a client
  `Internal(InvariantViolated)`, not evidence of provider misconduct; the same
  method errors on a stateless request are `Protocol(ContractViolation)`. The
  changes and lifecycle walks mark their requests state-based. Lifecycle
  handles `CursorInvalid` locally by reconciling a fresh name snapshot.
- `Method(anchorNotFound)` -> `SyncState(CursorInvalid)` ->
  `Engine(RestartScope)` only when the request context explicitly marks an
  anchored query and carries a cursor scope. A cursor-scoped `get` or an
  unanchored first-page query remains `Protocol(ContractViolation)`; an
  anchored-query context without a cursor scope is an internal invariant error.
- `Method(serverUnavailable | serverFail)` -> `Server(Unavailable)` ->
  `Retry::SameRequest`. `Method(serverPartialFail)` is different: some calls
  may have committed, so it is `Protocol(PartialResponse)` (the wire cause is
  the primary) -> `Retry::SameRequest` for an idempotent operation,
  `Reconcile` for a non-idempotent one.
- `Method(invalidArguments | invalidResultReference | requestTooLarge)` ->
  `Request(Malformed)` -> `ClientBug`.
- `Method(unknownMethod | unsupportedSort | unsupportedFilter)` ->
  `Unsupported(operation)`.
- `Method(accountNotFound | fromAccountNotFound | accountNotSupportedByMethod
  | fromAccountNotSupportedByMethod)` -> `SyncState(CapabilityChanged)` ->
  `Engine(RestartAccount)`. `Method(accountReadOnly)` ->
  `Authorization(PermissionDenied)`. An unrecognized method error type ->
  `Protocol(Unknown)` carrying `JmapMethod::Unknown { code }` ->
  `UnknownPermanent`.
- `Method(forbidden)` -> `Authorization(PermissionDenied)` ->
  `NoPermission`. For a foreign (shared) `Folder` scope,
  `shared_scope_error` intercepts this into `jmap_scope_revoked` ->
  `SyncState(ScopeRevoked)` -> `Engine(DisableScope(scope))` (quarantine
  one scope, not the account); a primary scope stays terminal.
- `Problem(limit)` -> `Server(RateLimited)` w/ `throttle_scope`;
  `Problem(unknownCapability)` -> `SyncState(CapabilityChanged)` ->
  `Engine(RestartAccount)`; `Problem(notJSON | notRequest)` ->
  `Protocol(ContractViolation)`. A problem document of any other type is
  classified by its HTTP status (the document's own `status`, else the
  carrying net error's): 400/422 `Request(Malformed)`, 401
  `Authentication(ReauthorizationRequired)`, 403
  `Authorization(PermissionDenied)`, 404 `NotFound(resource)` when the scope
  names one and `Server(Error { 404 })` otherwise, 409
  `ConcurrencyConflict`, 410 `SyncState(CursorInvalid)` only under a cursor
  scope, 429 `Server(RateLimited)` with an account throttle scope, 5xx
  `Server(Unavailable)`, any other status `Server(Error { status })`, and no
  status at all `Protocol(Unknown)`.
- Which net error shape carried the problem document decides where its
  evidence lives, and the crate reads all of them. `Error::Status` is the
  *only* variant bifrost-net produces for a terminal HTTP status, and a
  429 or 5xx never takes that path (the retry loop consumes it and hands
  back `RateLimited` / `RetryBudgetExhausted` with the final response
  preserved), so `TransportError::from_net` lifts the body back out of
  that evidence and `retry_after_from_net` reads the hint from the
  parsed field, the recorded history, *and* the response's own
  `Retry-After` header. `AuthLost` is the deliberate exception: its 401
  classification and transmission evidence stay bifrost-net's.
- `Transport(_)` is decided in three steps, in order. A `LocalFailure`
  mark wins: `InvariantViolated` -> `Internal(InvariantViolated)`,
  `LimitExceeded` -> `Internal(LimitExceeded)`, never retried. Otherwise
  bifrost-net evidence goes through
  `bifrost_net::into_account_error_with_throttle_scope` with JMAP's explicit
  account throttle scope, carrying `AttemptCause::transmission_state` from
  where the wire failed, so the central mapping picks `Retry::SameRequest`
  for idempotent ops and `Reconcile` for non-idempotent ops caught mid-flight. With
  neither, the fallback is `Transport(Network)` with no attempt cause.
- A handshake routed through a proxy CONNECT tunnel keeps net's typed
  `TunnelError` in the source chain, and it is classified before the generic
  handshake mapping: a 407 -> `Authentication(ReauthorizationRequired)`
  (`AuthLost`), with support detail naming the proxy as the credential owner,
  because the shared taxonomy has no proxy-specific credential kind; TLS to the
  proxy -> `Transport(Tls)`, retried as the same unsent request; any other
  CONNECT refusal -> `Server(Error { status })`, so recovery follows the status.
- WebSocket errors split by handshake position: `WebSocketHandshake`
  (pre-handshake, no typed tunnel source) -> `Transport(Network)` +
  `Attempt(Unsent)`;
  `WebSocketRuntime` (post-handshake read side) and `WebSocketClosed` ->
  `Protocol(PartialResponse)` + `Attempt(Acknowledged)`. `WebSocketSend` (every
  sink write) -> `Transport(Network)` with `Attempt(Unsent)` for
  `tokio_websockets::Error::AlreadyClosed`, which the library raises before
  queueing the frame, and `Attempt(InFlight)` for any other error, which came
  out of the flush; nothing was acknowledged, so it is never stamped
  `Acknowledged`. Push-set frames (`push_set_error`) additionally carry an
  idempotency override: an enable or disable frame carries the connection's
  complete desired data-type set, so replaying it is safe, unlike the
  central table's non-idempotent `PushSubscribe`, and the in-flight case
  stays a replay rather than a reconcile. No blanket
  `From<tokio_websockets::Error>`; call sites map explicitly.
  `WebSocketSetup` splits by cause: `Tls` -> `Transport(Tls)` +
  `Attempt(Unsent)`, `InvalidHeader` -> `Request(Malformed)`,
  `Subprotocol` -> `SyncState(CapabilityChanged)`.
  `WebSocketNotConnected` -> `Unsupported(operation)` where it reaches
  the mapping (a server with no WebSocket capability at `connect_ws`).
  Push `subscribe` and `unsubscribe` never map it: there it means the
  reader has not connected yet, and both absorb it as a deferred success.
  The push reader's re-enable cannot see it, since a successful handshake
  has already installed the sink.
- `NoPrimaryAccount` -> `Unsupported(operation)`, with the capability in the
  diagnostic: nothing was sent, and the server offers no account for that
  capability. Not `AuthLost` (re-authorizing cannot add an account to the
  session) and not `CapabilityChanged` (the reopen would re-read the same
  session forever).
- The server's own session describing itself wrongly is terminal
  `Protocol(ContractViolation)` -> `ProviderContractViolation`, naming what
  was wrong: `MalformedSessionUrl` (the session property) and
  `MalformedCapability` (the capability URI). `UnexpectedMethodResponse` takes
  the same kind.
- Response-shape errors are the provider's, not the caller's:
  `ResponseDecode` and `NotParsable` -> `Protocol(ParseFailed)`,
  `CallNotFound` -> `Protocol(MissingField)`, all
  `ProviderContractViolation`. `IdNotFound` -> `NotFound(resource)`
  (`ProviderRefused`) when the scope names a resource, else
  `Protocol(MissingField)`.
- Caller faults -> `Request(Malformed)` -> `ClientBug`: `RequestEncode`,
  `RequestCallLimit`, `RequestSizeLimit`, and `InvalidUrl` (field `url`, for
  an empty session URL or an unusable EventSource URL component).
- `Set` routes through the same per-item `set_error_to_account_error` as
  the `Email/set` lanes below.

Cursor-decode failures from `cursor::envelope` (protocol mismatch, unknown envelope, malformed payload) build their own AccountError with `SyncState(SchemaIncompatible)`, routed to `Engine(SchemaIncompatible)`.

Known JMAP `SetErrorType` vocabulary lands on typed
`WireCause::Jmap(JmapMethod::*)` variants. `SetErrorType::Other(code)` is
the only path to `JmapMethod::Unknown { code }`, carrying the actual wire
code (no synthesized `"other"` literal; string-matching unknown
vocabulary is forbidden by the gate-5 invariant).

`sync/error.rs` exposes `terminated_unsupported` (`Unsupported(op)`) and
`terminated_contract_violation` (`Protocol(ContractViolation)`), both taking
the caller's `AccountOperation`. The latter handles non-progressing protocol
responses such as re-served anchors; routine state expiry uses the common
cursor-scoped error classifier instead.

### Foreign (shared/delegate) accounts

JMAP auto-discovers shared/delegate accounts from the session: at `open`, `foreign_mail_account_ids` selects session accounts with `isPersonal: false` advertising `urn:ietf:params:jmap:mail`, excluding the primary; each becomes a scoped `Account::new(client, accountId)` handle in `foreign_mail`. The accounts are probed concurrently, then results are sorted by accountId before installation so open latency is bounded by the slowest independent share rather than their sum while topology and skip ordering stay deterministic. The concurrency is the server's own `maxConcurrentRequests` (RFC 8620 §2), clamped to `[1, 8]` by `api_request_concurrency`, and a session with no readable core capability probes serially. Exceeding that limit earns a request-level `limit` error, which the probe path would classify as a failure and turn a healthy share into a skipped scope. `api_request_concurrency` is the crate's single answer to how wide any fan-out may go; `filters_list` reads the same function for its blob downloads, so a second bound cannot drift from the session. `seed_foreign_account` runs the primary's two probes (`Email`/`Mailbox` state), and `apply_foreign_seed` inserts the per-accountId `state_cache` entries and seeds exactly ONE `CursorScope::Folder(encode_foreign_account(accountId))` - the account-level scope, seeded from the account's Email state - into `seed_states`. One scope per account, never one per mailbox: `Email/changes` is account-wide, so per-mailbox cursors would each replay the identical change set (the original B9 defect) and a foreign push would fan out to every one of them. A successfully seeded foreign account that also advertises Submission is included in `foreign_submission`; only that same routing set enables `pim_methods.send_as`, so a revoked share never produces an advertised but unreachable send path. A probe failure - revoked grant (terminal `NoPermission`) and exhausted transient retry (retryable) alike - skips that foreign account and records a `SkippedScope` naming its accountId, with the classified error, on `OpenedAccount::skipped_scopes` (`seed_foreign_account_or_skip`). Open itself neither fails (initial attach does not retry `factory.open`, so failing would block the user's own primary mail on a delegate outage) nor skips silently (the original G8 defect); the recovery class on the skip tells the consumer whether a reopen can heal it. Cost is O(foreign accounts) round-trips at open. Since foreign account discovery happens only here and there is no foreign scope lifecycle stream, the capabilities flag advertises that reopening can discover a newly granted share.

Owner-email resolution for those accounts is a two-level RFC 9670 gate
(`owner_email_plans`): no session `urn:ietf:params:jmap:principals`
capability means no plans at all, and per account the
`...:principals:owner` capability supplies the principal id.
`Principal/get` is authoritative; the account name is only ever a
fallback, and a narrow one. `account_name_as_address` PARSES the name
rather than sniffing it for an `@`, accepting only a bare addr-spec with
a dotted domain - display-name forms like `Support <support@example.com>`
are rejected outright rather than having an address guessed out of them,
because a wrong owner is worse than no owner. `fetch_principal_email`
returns a three-way `PrincipalEmail` so a lookup that never completed is
distinguishable from one that completed with no address:
`owner_email_from_lookup` permits the name fallback only on `Absent`.
On `Unavailable` the owner email is left unset, so a transport blip or an
unimplemented `Principal/get` cannot overwrite real ownership with a
label.

The foreign accountId rides in the `Folder` scope's `FolderId` (`foreign.rs` codec, `\u{1f}` separator; the account-level scope leaves the mailbox part empty, which no real RFC 8620 id can produce); `Type(_)` scopes cannot carry an account (`Type(Email)` is identical across accounts and would collide in the engine index), so the variant-free `Folder` shape is used. `mail_for_scope` / `account_id_for_scope` / `owner_of_scope` route a `Folder` scope to its `foreign_mail` handle, state-map key, and `MailboxId(accountId)` owner tag; primary scopes route to `self.mail` and `None`. `cursor_scopes` appends the seeded account-level foreign `Folder` scopes; `discover::memberships` appends one `Mailbox(accountId)` owner tag per foreign account, and `inventory_stream` stamps that tag onto every foreign-scope item - and re-encodes each native mailbox membership as `Folder(encode_foreign(accountId, mailboxId))` - so a foreign account's native mailbox ids cannot be conflated with the primary's in the membership index. The request layer is already per-account (`Account<Tr>::build` stamps the accountId), so routing is "hand `mail_for_scope` instead of `self.mail`", not a `core/request.rs` change.

Foreign OBJECT ids are qualified with the same codec (`encode_object` /
`parse_object` / `native_object`, same `\u{1f}` separator). The foreign
inventory qualifies both `InventoryEntry::id` and its whole-message
`blob_id`; the foreign changes leg qualifies its `ObjectChange` ids. This is
what makes hydration and blob reads self-routing: `get_stream` and
`open_blob` receive only an id - no scope - so a bare native id would run
`Email/get` / `/download/{accountId}/{blobId}` against the PRIMARY account
and either 404 or resolve an unrelated same-id primary object.
`hydrate::route_for_id` buckets ids per routing target (one `Email/get` per
account, since the call is accountId-scoped), strips to the native id on the
wire, and re-qualifies on the way out so an outcome id is byte-identical to
the id the caller handed in. `blob::foreign_split` does the same selection
for `open_blob` and both legs of `open_raw_rfc822` (the `blobId` fetch AND
the download). An id naming an account this session cannot reach falls back
to the primary handle deliberately, and - like the mutation pipeline's
`wire_email_id` - stays LITERAL on that wire (`hydrate::wire_object_id`
strips the qualification only for the routed owner), so the miss surfaces
as a real not-found rather than either a fabricated local error or, on a
native-id collision, an unrelated primary object hydrated under the
foreign id.

`pim::message_hydrate` (the one-id door, and the one
`SyncEngine::message_hydrate` funnels into) routes on the SAME
`hydrate::route_for_id` decision. It previously did not, so a foreign-encoded
id reached the primary `Email/get` verbatim and came back as a `NotFound`
naming the encoded string. On the way out, `qualify_foreign_message_ids`
re-qualifies the hydrated `Message`: `id` and each attachment `BlobId` in the
OBJECT namespace (`encode_object`, what `open_blob` decodes), each
`ContainerId` in the FOLDER namespace (`encode_foreign`, what
`containers_list` and the cursor scopes key on). The two namespaces are not
interchangeable and the projection must not confuse them.

Thread ids ride the same object namespace (nc-7's fix): foreign inventory
and hydration qualify `InventoryEntry::thread_id` / `Message.thread_id`
through the one `qualify_foreign_ids` hook, so a foreign thread id carries
its owner everywhere the consumer sees it, and every thread-keyed door
decodes it - `thread_hydrate` selects the owning handle, sends the bare
native id, and re-qualifies each returned member's id/thread/containers/
attachments; `MutationTarget::Thread` fan-outs, `move_thread`, and
`delete_thread` route the same way, with `delete_thread` minting its
resolved Trash id in the thread's own namespace so the cross-account guard
and the already-in-Trash comparison see one account. The handle-selection
boundary itself (`route_object_id` / `route_mutation_target`) is
transport-generic and pinned over string handles, because `JmapAccount`
hardwires `ReqwestTransport` (xc-3) and the account door cannot be driven
scripted; the account methods are one-line delegations to keep that pin
meaningful.

Three more decisions are lifted out for the same reason (jmap-O2), each
pure given its inputs, each with its trait method reduced to a one-line
delegation: `cursor_scopes_from_seeds` (discovery order - seeded `Type`
scopes in a fixed order, then foreign `Folder` scopes SORTED, because
`seed_states` is a `HashMap` and an unsorted lane would hand the engine a
different scope order every run), `partitioning_for_scope` (every scope is one
`Full` walk, because Email pages by anchor and an engine partition
boundary would discard it), and `establishment_for_seed` (seeded ->
`Ready`, unseeded -> `Unsupported` carrying the cursor scope). Prefer this
pattern over threading the transport generic through the sync layer: it
costs one function and no API surface.

A thread id qualified for an account this session no longer
holds stays LITERAL on the primary route - same rule as message ids - so
the server reports the miss instead of a collision-prone local strip.

`containers_list` appends each foreign account's mailboxes with
`namespace = Shared`, `owner = MailboxId(accountId)`,
`native_id = encode_foreign(accountId, mailboxId)` (byte-identical to the
`MembershipScope::Folder` qualification inventory and hydration stamp on
that account's messages - the container/membership join key; the sync
scope itself is the coarser account-level form),
`owner_local_id` = the bare mailbox id,
and the parent re-encoded in the same namespace so a foreign child never
points at a same-id primary mailbox. A per-account `Mailbox/get` failure
degrades to a `SkippedScope` (naming the foreign accountId, carrying the
classified error) on the returned `ContainerList::skipped_scopes`, plus the
remaining containers: one unreachable share neither blanks the sidebar nor
vanishes from it silently. A primary enumeration failure still fails the
call.

A share whose `Mailbox/get` answers WITHOUT `myRights` takes the same lane
per mailbox. `Container::rights` gates submit and the read-only-share
distinction, and `None` there is indistinguishable from a protocol with no
rights notion, so the omission would otherwise project as unreported rather
than as a degradation. `fetch_foreign_containers` reads `my_rights()` before
projection (afterwards the absence is unrecoverable), still lists the
container - a share with unknown rights beats a share missing from the
sidebar - and pushes a `SkippedScope` naming that container's qualified id
with a `Protocol(MissingField)` / `Wire(MalformedResponse)` error.
`fetch_mailboxes` asks for `MyRights` by name and RFC 8621 §2 makes it
server-set, so an answer omitting it is a missing field, not an absent
feature. Primary mailboxes are deliberately not classified: rights on the
user's own mailbox are not consulted, so an omission there hides nothing.

Foreign object mutations route through the same registered-account decision as
hydration: bulk operations partition by owner, and single-message PIM
operations select the owning `Mail` handle before stripping the qualified id
for `Email/set`. Their state-cache key is that selected accountId. Foreign
mailbox lifecycle remains out of scope: `scope_lifecycle` polls only the
primary. Under the account-level scope this costs less than it used to: a
foreign mailbox created after `open` needs no new cursor (the account scope
already covers its mail, and its qualified membership appears at hydration);
only the container row itself waits for the next `containers_list` call, and
a whole new share still waits for reopen.

### Known limitations

- Live foreign-mailbox lifecycle is not wired. `get_stream`, `open_blob`,
  `open_raw_rfc822`, bulk mutation, and single-message PIM mutation route to
  the foreign account via the qualified object-id codec. Foreign submission is
  supported, but scheduled foreign submission is not.
- Raw-MIME projections unsupported; only `FlagsOnly` and `Metadata` work. Sync-layer push is WebSocket-subprotocol only; against a server without RFC 8887 the engine falls back to polling. The client-level EventSource API exists but is not wired in as a push fallback (deliberate; see `reference/jmap/DEFERRED.md`).
- `BlobRangeSupport::No`; `open_blob_range` fatals `Error::Unsupported` even when the handle advertises range support (no transport `Range` hook). Its one non-capability refusal, a `range.start` past the known blob size, is `Request(Malformed)` (-> `ClientBug`) instead: that is a caller argument fault, and reporting it as `Unsupported` would tell the engine the protocol has no ranged read at all.
- `MutationReplaySafety::None`; `IdempotencyKey` is a wire no-op (the read-back guard observes current target state; it is no lost-update protection).
- `bulk_move` only `MembershipScope::Mailbox`; `inventory_partitioning` is `Full` for every scope, and any explicit
  `InventoryPartition` other than `Full` is refused as
  `Unsupported(SyncInventory)` without sending a request.
- Gmail labels, Graph categories/extended properties, and identity-default selection are unsupported. Attachment handles keep blob id + MIME but not uploaded filenames.
- Typed filter-rule CRUD is unsupported; JMAP exposes literal Sieve scripts instead.
- Mail search covers a share only when the caller names it. A
  `SearchFilter::In` whose `ContainerId` is the owner-qualified form
  `containers_list` mints routes both `Email/query` and the follow-up
  `Email/get` to that owner's handle with the native mailbox id, and the
  returned message and thread ids come back qualified in the object
  namespace. What stays unsupported is the implicit union: with no `In`, or
  with an `In` naming a primary container, a search is primary-only and never
  touches a share. Two `In` filters naming different owners are refused
  (`Request(Malformed)`), not unioned - one `Email/query` carries one
  `accountId` - and so is an `In` naming a share this session does not hold.
  That refusal is the one place foreign routing cannot use hydration's
  stay-literal fallback: the id is a filter operand, not the object identity
  the server can report a miss on, so the primary account would answer with
  an empty page instead of an error. The page cursor is qualified with the
  account it was minted against (the same object-id codec), and replaying it
  under a filter that routes elsewhere is refused rather than paging one
  account by another's offsets.
- The search page cursor payload is versioned: `2:<position>:<queryState>`,
  owner-qualified for a share. It is the POSITIONAL encoding; the contact and
  calendar page walks use a different `2:` payload (anchored, see "PIM page
  cursors"), and the two are never decoded by the same reader. Two things ride in it because a bare position
  means nothing without either. The account, because the order is
  account-scoped (above); and the `queryState`, because the order is not
  stable across calls - `Email/query` recomputes it per call, so page 2 taken
  under a moved state slides the window and silently duplicates one hit while
  dropping another. A moved state is refused as `ConcurrencyConflict` ->
  `Retry(AfterStateRefresh)` ("repeat the search"), never `Done`-shaped
  silence: it is not the caller's bug (one delivered message moves the state)
  and it is not terminal. `SyncState(CursorInvalid)` is unavailable here on
  purpose - it requires an `ErrorScope::Cursor`, and a search page cursor is
  not an engine cursor scope. A v1 cursor (the bare integer position that
  carried no pin) is refused `SyncState(SchemaIncompatible)` rather than
  resumed, since resuming it is exactly the unpinned paging the bump exists
  to stop; the two shapes are unambiguous (a v1 payload is all digits).
- Whether a next page exists is decided by `position` plus `total`, not by the
  page having come back full: reading fullness as "more remains" ended such a
  walk in `Done`-shaped silence with most of the hits unreported. (The old
  justification here - "RFC 8620 permits a short non-final page" - was wrong;
  the correct one is under "PIM page cursors".) The query always sets
  `calculateTotal`; a server that omits `total` anyway gets
  continue-until-empty. `validate_search_page` checks the envelope before any
  of that: the echoed `position` is compared against the position the request
  SENT rather than trusted, and a nonempty page that claims to have served
  past its own `total` is refused rather than read as a finished walk. See
  "PIM page cursors".

### Push failure routing

The WebSocket push reader retries locally only for `RecoveryClass::Retry`. Handshake, subscription write, ping and read errors are classified as `PushStream`; every other recovery class ends the reader with `WatchEvent::Terminated`, preserving engine directives for the sync reconciler.

## Unsupported error construction

Local refusal helpers use `AccountError::unsupported` through the shared error
funnel, retaining their provider, scope and diagnostic decoration.
`Account::protocol` returns `Protocol::Jmap`, including for inherited
default-method refusals.

## Caller versions on contact and event mutations

Contact and event update patches, conditional deletes and conditional RSVP
accept a nonempty `ServerVersion::StateAt` and send it as native `ifInState`.
RFC 8620 Section 5.3 requires a mismatching state to reject the entire set
without changes. State belongs to the selected account and object type, not
an individual object's ETag or query state. Reads needed to construct a patch
never replace the caller state; a mismatch propagates as `ConcurrencyConflict`
without a refresh or retry. Other version kinds and empty states are refused
before I/O. Absent versions retain the ordinary unconditional paths.
Capabilities advertise these doors only when the corresponding PIM account
is supported. Contact and event reads still expose no shared type-state field;
callers must already possess the appropriate native type state.

The bulk caller-state contract follows RFC 8620 Section 5.3:
https://www.rfc-editor.org/rfc/rfc8620.html#section-5.3
An Email state belongs to one account, not one message. Ready targets with the
same owner and optional caller state share one `Email/set`, bounded by
`maxObjectsInSet` and the local 500-object cap. Different owners or states
never share a set. A pending input flushes a partial chunk. A later chunk
carrying the same old state can still conflict after an earlier success;
no automatic token advance is allowed on the caller's behalf.

## Flag observation and reassertion capability

The account declares flag and destroy desired-state reassertion safe on stable
server ids. FlagsOnly hydration requires an explicit keywords property; its
absence is a per-item Protocol MissingField failure. The generic flag judge
uses the same ASCII-case-insensitive keyword identity as writes.

A server can re-add maintained keywords, such as attachment annotations,
after a Set. After an uncertain write, an unsatisfiable target can spend its
remaining attempt budget on writes that cannot stick. This bounded futility
is accepted; reassertion supplies no lost-update guarantee.
