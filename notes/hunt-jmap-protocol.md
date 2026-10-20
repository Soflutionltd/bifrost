# Hunt: bifrost-jmap protocol layer

Scope: `crates/jmap/` outside `src/sync/`. I crossed into `src/sync/` wherever a
protocol-layer value led there. Findings are ordered by severity. Each one
names the claim it breaks.

## P1: Session-level and account-level capability objects are confused

RFC 8621 s1.3.2 says the `urn:ietf:params:jmap:submission` value in the
Session `capabilities` map "is an empty object". `maxDelayedSend` and
`submissionExtensions` live only in each account's `accountCapabilities`.
RFC 8621 s1.3.1 (mail), RFC 9661 (sieve) and RFC 9670 (principals,
principals:owner) all split their objects the same way: the session-level
value is empty and the account-level value carries the data. The crate reads
every one of these from the session-level map.

### Submission: a conformant server loses send entirely

- `SubmissionCapabilities` (`crates/jmap/src/email/mod.rs`) makes
  `maxDelayedSend` mandatory and has no container default. A conformant
  session-level `{}` therefore fails to parse and lands in
  `Capabilities::Malformed`.
- `resolve_optional_families` (`crates/jmap/src/sync/factory.rs`) reads
  `session.submission_capability_state()`, gets `Malformed`, and drops the
  submission handle. Send, draft send, identities and `scheduled_send` all go
  off, with a warning that blames the server.
- `max_delayed_send` is read from the session map too, so even a server that
  happens to send a non-empty session block has its real per-account window
  ignored.
- The test `a_submission_block_without_max_delayed_send_disables_the_family`
  pins exactly the RFC-conformant shape (`{}`) as "malformed". The test
  encodes the bug.
- `reference/jmap.md` ("Capabilities advertised": "`scheduled_send` is true
  iff the Submission block is PRESENT, parseable, and advertises
  `maxDelayedSend > 0`... a block omitting the mandatory RFC 8621 §7
  `maxDelayedSend` fails to parse and lands in the malformed lane") restates
  the same misreading. The doc comment on `SubmissionCapabilities` does too.
- Claims broken: the crate implements RFC 8621 submission, and
  `pim_methods` advertises send, drafts and identities. Against a server that
  follows the RFC to the letter, every one of them is off.
- Fix shape: parse the session-level block as an opaque presence marker. Read
  `SubmissionCapabilities` from `session.account(primary_submission_id)
  .capability(URI)`. `Account::account_capabilities` already goes through
  `try_cap!`, so the typed value is already sitting there. Apply the
  Absent/Malformed/Present lane rule to the account-level object, which is
  where the mandatory members actually live.

### Principals: RSVP alias discovery never runs on conformant servers

- `fetch_self_emails` (`crates/jmap/src/sync/factory.rs`) reads
  `currentUserPrincipalId` from `session.principals_capabilities()` and
  `principalId` from `session.principals_owner_capabilities()`. Both come from
  the session-level map.
- RFC 9670 puts `currentUserPrincipalId` in the account-level `principals`
  object. `principals:owner` is an account-level capability only.
- Result: against a conformant server both lookups return `None`, and the
  `Principal/get` alias resolution that `reference/jmap.md` promises ("RSVP
  resolves authenticated email aliases from Basic credentials and RFC 9670
  Principal/get when available") silently never happens. RSVP then falls back
  to "the single non-owner attendee" or `Unsupported`.
- The fixtures in `crates/jmap/src/tests.rs` put these fields at session level,
  so the tests pass against a shape the RFC does not produce.

### The same misplacement elsewhere (latent: nothing reads these yet)

- `MailCapabilities` (`maxMailboxDepth`, `maxSizeMailboxName`, and the rest),
  `SieveCapabilities` (`max_script_name_size()` falls back to 512 because the
  session value is always empty), and `CalendarsCapabilities` /
  `ContactsCapabilities` (`mayCreateCalendar` / `mayCreateAddressBook` read as
  `false`) are all modelled as session-level structs. Every reader would get
  defaults. Today nothing in `sync/` reads them, so these are latent, but the
  accessors claim to report server limits.
- Structural recommendation: split the capability model in two. Make
  `SessionCapabilities` presence-only for everything except `core` and
  `websocket`, and put the typed per-account objects on `session::Account`.
  Route every "what does this account allow" question through the account
  that is about to be used. Foreign accounts benefit too: their limits can
  differ from the primary's, and today nothing can see that.

## P2: Blob upload and download run under the 10-second API deadline

- `ReqwestTransport::send_once` applies `request.timeout(self.timeout)` to
  every door, `upload` and `download` included. That is bifrost-net's TOTAL
  request deadline (`RequestBuilder::timeout`: "Override the per-request
  timeout", as opposed to `without_timeout`, which keeps only the inactivity
  bounds). The default is `DEFAULT_TIMEOUT_MS = 10_000`, which the factory
  keeps unless the consumer sets `timeout`.
- Whole-message (`open_raw_rfc822`), attachment and blob reads, and
  attachment uploads, are bounded by the same 10 seconds as a small JSON API
  call. A 25 MB attachment needs about 20 Mbit/s sustained to finish in time.
  With `set_bandwidth_cap` engaged (a documented `Account` control),
  `MeteredStream` and bifrost-net admission pace the body, so any blob larger
  than roughly `10 s * cap` is guaranteed to fail.
- The failure is a `Timeout`, and the recovery mapping retries it, so the
  same download is replayed until the retry budget runs out. It can never
  succeed.
- Claims broken: `open_blob` / `open_raw_rfc822` / attachment upload are
  advertised capabilities, and the bandwidth cap is advertised as throttling,
  not as "makes large blobs impossible". `reference/jmap.md` documents the
  SSE door suppressing the ordinary deadline "while retaining response-header
  and body inactivity bounds". That is the right shape, and it was applied to
  the stream door and not to the blob doors.
- Fix shape: give the blob doors their own deadline policy, either
  `without_timeout()` (keeping the inactivity bounds) or a deadline scaled to
  the expected size and the active cap. Keep the 10 s total deadline for the
  `/api` door only.

## P3: Smaller defects and document drift

1. **"No currently wired method produces multiple responses" is false.**
   `reference/jmap.md` (internal request/response flow) and the doc comment
   on `Response::get` both justify not modelling multiplicity with this
   claim. `sync/pim.rs` send and draft-send set `onSuccessUpdateEmail` /
   `onSuccessDestroyEmail` on `EmailSubmission/set`. RFC 8621 s7.5 then has
   the server emit an implicit `Email/set` response under the SAME call id,
   after the `EmailSubmission/set` response. The code happens to work,
   because `get` takes the first match and the implicit response sits
   unread. But the implicit `Email/set` carries the post-move `newState`,
   which `pim.rs` never feeds into `state_cache`: only the earlier explicit
   `Email/set` state is advanced, so the cache is behind by one state
   immediately after every send. The rationale for not building `get_all`
   rests on a false fact, and so does the state-cache argument in that send
   path.

2. **The `NoPrimaryAccount` guard refuses methods that carry no accountId.**
   `Request::call` refuses any method when the request's account id is empty,
   on the claim (in the code comment and in `reference/jmap.md`) that "every
   method this crate defines carries an `accountId`". `PushSubscription`
   declares `requires_account_id() == false`, and RFC 8620 s7.2 gives
   `PushSubscription/get` and `/set` no `accountId`. So on a session with no
   `primaryAccounts`, those calls are refused for a reason that does not
   apply to them. The guard should check `O::requires_account_id()` (or
   check the method's own serialized `accountId`), not the request default.

3. **`Request::new` reads the session twice.** `CallLimit::read(&client.session())`
   and `client.default_account_id()` each take their own `session_state()`
   snapshot. That contradicts `SessionState`'s stated purpose ("Hold the
   returned `Arc` for the duration of a URL build so a concurrent refresh
   cannot splice two sessions into one request"). A refresh between the two
   reads pairs one session's `maxCallsInRequest` with the other's account.
   `encode_ws_request` similarly reads `self.session()` for `maxSizeRequest`
   independently of the snapshot the request was built against. Today this
   is latent only because `refresh_session` is unwired. Take one snapshot
   and keep it on `Request`.

4. **Overlapping refreshes can install the older session last.**
   `refresh_session` fetches without holding the lock and installs
   unconditionally. If two refreshes overlap and the earlier GET returns
   later, the client ends up on the older session and sets
   `session_updated = true`. The comment only protects state/limit pairing,
   not recency. This is latent because the method is unwired, but it is kept
   deliberately (per `reference/jmap.md`), so it should be correct.
   Serialize refreshes, or compare against a fetch generation.

5. **ProblemDetails without `type` is not recognised.**
   `ProblemDetails.p_type` is mandatory, but RFC 7807 s3.1 says an absent
   `type` means `"about:blank"`. A server returning a problem document with
   only `status` / `title` / `detail` falls through to `Error::Transport`,
   and its explanation is lost to the problem-details mapping that
   `reference/jmap.md` describes. Default it.

6. **`maxConcurrentUpload` / `maxSizeUpload` are not consulted, and the
   rationale is already stale.** `reference/jmap.md` justifies leaving
   `maxConcurrentUpload` unread because "every upload door awaits one upload
   at a time". That is true per call site, but the engine can run several
   `attachment_upload` / send operations at once on one account, and uploads
   are deliberately not gated by the API `ConcurrencyLimit`. So concurrent
   uploads do happen, from different callers, the same overlap argument the
   doc itself uses to justify the client-wide API gate. Only a documentation
   inconsistency today, but the gate belongs on the transport for the same
   reason the API gate does.

7. **The EventSource `{types}` expansion has two edge cases.**
   `Client::event_source` uses `types.take()`, so a template naming
   `{types}` twice expands the second as `*` (subscribe to everything).
   `Some(empty_iter)` expands to an empty string, which servers read
   variously as "nothing" or an error. Neither is reachable from `sync/`
   today.

8. **`SessionState::derive` silently accepts a missing default account.**
   It maps "no primaryAccounts" to `AccountId::new("")`, a sentinel whose
   meaning is then recovered by an `is_empty()` check in `Request::call`.
   `Option<AccountId>` would let the type carry the state the comment
   already describes.

## Lateral observations (outside the ruled scope or not defects)

- `send_ws_awaiting`, `PendingRequests` and the whole WebSocket
  request/response correlation machinery have no caller in `sync/`. Only
  push frames ride the WebSocket. That is fine, but the generation/teardown
  logic is real complexity kept alive only by its own tests.
- `Client::download` buffers the entire blob into `Bytes` with no ceiling.
  Nothing promises a bound, but together with the timeout finding it means
  large-blob reads are both memory-unbounded and deadline-bounded.
- `connect_ws` opens its own `TcpStream` and TLS connector, bypassing
  bifrost-net. So the WebSocket ignores the trusted-host redirect policy and
  any proxy, and gets no 401 token-refresh retry at the handshake. The
  reference acknowledges that WebSocket is reqwest-specific, but these
  consequences are not written down.
- `Session::capability_config` round-trips the untagged `Capabilities` enum
  through `serde_json::Value`. For a `Malformed` entry this re-parses the raw
  value into `C::Config`, and can SUCCEED where `try_cap!` failed, when
  `C::Config` is laxer than the struct `try_cap!` used. That would make the
  convenience bridge disagree with the three-state accessors. I did not find
  a live caller.
