# bifrost-jmap defect findings

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

Filed from the October 2026 defect hunt: the jmap protocol scope, the jmap
account scope, and the net and sync adoption scopes where they reached JMAP.
Severity labels are the hunters' own.

## JMAP-001 - Session-level and account-level capability objects are confused

Hunter severity: P1. Scope: jmap protocol.

RFC 8621 s1.3.2 says the session-level `urn:ietf:params:jmap:submission`
value "is an empty object"; `maxDelayedSend` and `submissionExtensions` live
only in each account's `accountCapabilities`. RFC 8621 s1.3.1 (mail), RFC 9661
(sieve) and RFC 9670 (principals, principals:owner) split the same way. The
crate reads all of them from the session-level map.

**Submission: a conformant server loses send entirely.**

- `SubmissionCapabilities` (`crates/jmap/src/email/mod.rs`) makes
  `maxDelayedSend` mandatory with no container default, so a conformant
  session-level `{}` fails to parse and lands in `Capabilities::Malformed`.
- `resolve_optional_families` (`crates/jmap/src/sync/factory.rs`) reads
  `session.submission_capability_state()`, gets `Malformed`, and drops the
  submission handle. Send, draft send, identities and `scheduled_send` all go
  off, with a warning that blames the server.
- `max_delayed_send` is also read from the session map, so a server that does
  send a non-empty session block has its real per-account window ignored.
- The test `a_submission_block_without_max_delayed_send_disables_the_family`
  pins the RFC-conformant `{}` as malformed: the test encodes the bug.
- `reference/jmap.md` ("Capabilities advertised": scheduled_send is true iff
  the Submission block is present, parseable and advertises
  `maxDelayedSend > 0`; a block omitting the mandatory member lands in the
  malformed lane) and the doc comment on `SubmissionCapabilities` restate the
  misreading.
- Claim broken: RFC 8621 submission is implemented and `pim_methods`
  advertises send, drafts and identities; against a server that follows the
  RFC to the letter every one is off.
- Fix shape from the hunter: treat the session-level block as an opaque
  presence marker and read `SubmissionCapabilities` from
  `session.account(primary_submission_id).capability(URI)`.
  `Account::account_capabilities` already goes through `try_cap!`, so the
  typed value is already there. Apply the Absent/Malformed/Present lane rule
  to the account-level object.

**Principals: RSVP alias discovery never runs on conformant servers.**

- `fetch_self_emails` (`crates/jmap/src/sync/factory.rs`) reads
  `currentUserPrincipalId` from `session.principals_capabilities()` and
  `principalId` from `session.principals_owner_capabilities()`, both
  session-level.
- RFC 9670 puts `currentUserPrincipalId` in the account-level `principals`
  object, and `principals:owner` is account-level only.
- Against a conformant server both return `None`, so the `Principal/get` alias
  resolution `reference/jmap.md` promises ("RSVP resolves authenticated email
  aliases from Basic credentials and RFC 9670 Principal/get when available")
  silently never happens. RSVP falls back to the single non-owner attendee or
  `Unsupported`.
- Fixtures in `crates/jmap/src/tests.rs` put these fields at session level, so
  tests pass against a shape the RFC does not produce.

**The same misplacement elsewhere (latent: nothing reads these yet).**

- `MailCapabilities` (`maxMailboxDepth`, `maxSizeMailboxName` and the rest),
  `SieveCapabilities` (`max_script_name_size()` falls back to 512 because the
  session value is always empty), and `CalendarsCapabilities` /
  `ContactsCapabilities` (`mayCreateCalendar` / `mayCreateAddressBook` read
  `false`) are modelled session-level. Every reader would get defaults; the
  accessors claim to report server limits.
- Structural recommendation: make `SessionCapabilities` presence-only for
  everything except `core` and `websocket`, put typed per-account objects on
  `session::Account`, and route every "what does this account allow" question
  through the account about to be used. Foreign accounts benefit too, since
  their limits can differ from the primary's.

## JMAP-002 - Blob upload and download run under the 10-second total API deadline

Hunter severity: P2. Scopes: jmap protocol; also net adoption.

- `ReqwestTransport::send_once` (`crates/jmap/src/transport_reqwest.rs`)
  applies `request.timeout(self.timeout)` to every door, `upload` and
  `download` included, and the account spec also sets
  `spec.request_timeout = Some(timeout)`. That is bifrost-net's TOTAL request
  deadline (as opposed to `without_timeout`, which keeps only the inactivity
  bounds). The default is `DEFAULT_TIMEOUT_MS = 10_000`
  (`crates/jmap/src/client.rs`), which the factory keeps unless the consumer
  sets `timeout`.
- Whole-message (`open_raw_rfc822`), attachment and blob reads, and attachment
  uploads, are bounded by the same 10 s as a small JSON call. A 25 MB
  attachment needs about 20 Mbit/s sustained. With `set_bandwidth_cap` engaged
  (a documented `Account` control), `MeteredStream` and net admission pace the
  body, so any blob larger than roughly `10 s * cap` is guaranteed to fail.
- The failure is `Timeout { Acknowledged }` -> `Protocol(PartialResponse)`,
  which recovery retries, so the same download replays into the same wall
  until the budget runs out.
- Claims broken: `open_blob` / `open_raw_rfc822` / attachment upload are
  advertised; the bandwidth cap is advertised as throttling. `reference/jmap.md`
  documents the SSE door suppressing the ordinary deadline "while retaining
  response-header and body inactivity bounds", the right shape applied only to
  the stream door. `reference/net.md` keeps the total deadline distinct from
  inactivity bounds so a slow but progressing response stays distinct from a
  stalled one, and google and graph set no `request_timeout` for that reason.
- Fix shape: give blob doors their own deadline policy (`without_timeout()`
  keeping inactivity bounds, or a deadline scaled to size and the active cap);
  keep the 10 s total deadline for the `/api` door only.

## JMAP-003 - Blob downloads are fully buffered with no streaming path

Lateral, from jmap protocol and net adoption.

`Client::download` buffers the entire blob into `Bytes` with no ceiling of its
own and goes through net's buffered `send`, so a message or attachment above
`DEFAULT_MAX_BUFFERED_RESPONSE` (64 MiB) fails as `ResponseTooLarge`.
`download_stream` exists in net and JMAP does not use it. Together with
JMAP-002, large-blob reads are both memory-bound and deadline-bound.

## JMAP-004 - URL templates let `.` and `..` through as path navigation

Scope: net adoption.

- Claim broken: `reference/net.md` URL helpers: the path encoder
  "double-escapes a complete `.` or `..` component so the WHATWG parser cannot
  resolve it as navigation"; "The HTTP protocol crates name the grammar at
  every call site".
- JMAP does not use `bifrost_net::url`. `crates/jmap/src/core/session.rs`
  `TEMPLATE_VALUE` keeps `.` unreserved (correct per RFC 6570, which is why
  net's hardening is needed on top). `crates/jmap/src/blob/download.rs`
  substitutes `{name}` from the attachment filename, which is
  sender-controlled. An attachment named `..` or `.` turns
  `/download/{accountId}/{blobId}/{name}` into a URL the parser collapses to a
  different path, so the GET addresses some other resource on the JMAP server
  with the account's credential. Ids are base64url on a well-behaved server, so
  `{name}` is the practical vector.
- Fix: substitute with `encode_path_component` for path variables and
  `encode_query_value` for query variables (the template parser already knows
  which side of `?` each `URLPart` is on). Same for upload and EventSource
  templates.

## JMAP-005 - `draft_send` reports a committed send as failed when the cache refresh after it fails

Hunter severity: P1. Scope: jmap account.

`pim.rs::draft_send`: after `EmailSubmission/set` returns `created` (the
message is submitted and `onSuccessUpdateEmail` already moved the draft to
Sent), it runs
`mutation::probe_email_state(&mail).await.map_err(to_acct_err(DraftSend))?`
only to refresh `email_states`. If the probe fails (transport blip, 5xx, rate
limit), `draft_send` returns `Err` although the send went through. The error
class is retryable, so a consumer or engine retrying the "failed" `DraftSend`
submits the same email id again: a duplicate send.

Claim broken: `Account::draft_send` returns `Ok(ObjectId)` for a send that
happened; a cache refresh must never decide the outcome. Fix: make the probe
best-effort (log, or invalidate the cache entry) and return `Ok` once
`created` is confirmed. `send_message` gets this right by using the `Email/set`
`newState` it already has; `draft_send` could add an `Email/get ids: []` to
the same request, or just drop the cache entry.

## JMAP-006 - `reschedule_send` is not the double-send guard its comment claims, and can lose a scheduled message

Hunter severity: P1. Scope: jmap account.

`pim.rs::reschedule_send` sends the cancel of the old submission and the create
of the new one in ONE `EmailSubmission/set`. RFC 8620 s5.3 processes create,
then update, then destroy, so the new submission is created BEFORE the cancel
is tried:

- If the cancel is refused (`cannotUnsend`, relay already released), the new
  submission exists and is live, and the code returns `Err`. The in-code
  comment calls this "the double-send guard", but the double send is already
  committed and the caller is told nothing went through. The comment admits
  "the new submission, if any, is left in place".
- When the original send used `save_to_sent == Some(false)`, `send_message`
  attached `onSuccessDestroyEmail`, which fires when the scheduled submission
  is created, destroying the email object at once. A later `reschedule_send`
  creates against a destroyed `emailId` and fails, then the update cancels the
  original. The scheduled message is cancelled, nothing replaces it, and the
  caller gets `Err`: the message is lost.

Fix: cancel in its own call and confirm `updated`, then create in a second call
(or create first and destroy on failure, whichever ordering the product
wants). For the destroy-on-success shape, refuse reschedule up front or do not
destroy the email for a scheduled submission until delivery.

## JMAP-007 - Failed sends leave orphan drafts behind

Hunter severity: P1/P2. Scope: jmap account.

`send_message` and `send_raw_message` put the `Email/set` create (or
`Email/import`) and the `EmailSubmission/set` in one request. When the
submission is refused (`forbiddenFrom`, `invalidRecipients`,
`tooManyRecipients`, quota) while the email create succeeded, the function
returns `Err` and the created message stays in Drafts with `$draft`. Every
retry adds another. Nothing destroys it, and the `ObjectId` is not returned, so
the caller has no handle to clean up with.

Claim broken: `send_message` is a single send; on `Err` the caller may assume
nothing was persisted. Fix: on submission failure with a created email, issue a
best-effort `Email/set destroy`, or carry the draft id in the error.

## JMAP-008 - Email inventory livelocks on any mailbox that receives mail faster than one walk takes

Hunter severity: P2. Scope: jmap account.

`inventory.rs::email_inventory_loop` pins `queryState` across pages and ends
via `terminated_walk_superseded` (`RestartScope`) as soon as it moves.
`reference/jmap.md` itself says "one delivered message advances `queryState`".
The walk has no resumable position (`checkpoint: None`), so every restart
begins at position 0. On a large mailbox (100k messages at 256 per page is
about 400 query+get round trips) with mail every few minutes, the walk never
reaches the empty page; inventory never completes and the engine pays for the
full walk repeatedly.

The reference defends `RestartScope` over a terminal class because "a terminal
class there would let ordinary mail delivery permanently kill a scope's
inventory". Under steady delivery `RestartScope` does exactly that, more
expensively.

The pin is not needed for the claimed safety:
- an anchor destroyed under the walk already returns `anchorNotFound`, which
  `convert_method` maps (cursor scope present) to `SyncState(CursorInvalid)`
  -> `RestartScope`;
- with the unfiltered primary/foreign walk sorted by immutable `receivedAt`,
  new arrivals land before the anchor (already passed) or ahead of it (still
  walked), and the change cursor established before inventory reports them as
  `Created` either way;
- deletions ahead of the anchor just drop out.

Fix: drop the `queryState` pin for the unfiltered walk and let `anchorNotFound`
be the only "coverage unprovable" signal; optionally make the anchor a
resumable checkpoint. The legacy `inMailbox`-filtered walk has the same
livelock and remedy, since membership moves are covered by the change cursor.

## JMAP-009 - Push health events sent before `push_stream()` subscribes are lost, and push then looks alive forever

Hunter severity: P2. Scope: jmap account.

`push.rs`: `WsState::spawn` starts `reader_loop` inside `open()`. Events go out
on a `broadcast` channel, which delivers only to receivers existing at send
time. `push_stream()` subscribes only when the engine's push forwarder calls
it, after attach (`sync/src/engine/attach.rs`). So:

- If the first handshake fails terminally (auth lost, subprotocol refused ->
  `CapabilityChanged`), `WatchEvent::Terminated` goes to zero receivers and is
  dropped; the reader then breaks out of `reader_loop`.
- Every later `push_stream()` gets a receiver on a channel whose sender
  (`WsState::tx`) is alive, so `recv` never returns `Closed`. The stream is
  silent for the account's life: the "dead push the engine cannot tell apart
  from a quiet mailbox" state the keepalive machinery exists to prevent.
- A subscriber arriving after the reader terminated later in life (engine
  forwarder reconnect, a consumer's own `push_stream`) gets the same silence.
- `Lagged` can drop a `Terminated` in the overwritten window; it is degraded
  to a `Coalesced/Unknown` hint, which is not terminal.

Claim broken: `WatchEvent::Terminated` is the reader's terminal health signal
(`ReaderStep::Terminal` doc: "emit `Terminated` and stop the reader so the
engine can reopen the account"). Fix: latch terminal state (or current health)
in a `watch` inside `WsState`; `push::stream` replays the latched `Terminated`
on subscribe and ends, and a lagged receiver re-reads the latch. `Reconnected`
before the first subscriber is lost the same way, but the reconciler's
reconnect-is-Unknown rule makes that cheap.

Related shapes in other crates, filed separately: IMAP-020 (IMAP push events
lost before subscribe), GOOG-017 (Google `Lagged` drops health edges).

## JMAP-010 - One unrecognised WebSocket frame permanently kills push

Hunter severity: P2. Scope: jmap account.

`client_ws::decode_frame` turns an unknown `@type` into a `serde_json::Error`
(-> `Error::ResponseDecode`), and a binary frame into `Error::NotParsable`.
Both reach `push.rs::reader_pass` as `Err`, which `into_account_error`
classifies as `Protocol(ParseFailed)` -> `ProviderContractViolation`, and
`is_terminal()` is true. The reader emits `Terminated` and stops for good.

Triggers: a `calendars`-off build (the `jmap-calendars-off` /
`jmap-no-default` sweeps are promised to consumers) whose server sends
`CalendarAlert`; any server extension frame type; one malformed `StateChange`.
RFC 8887 servers may add frame types. Combined with JMAP-009, push can be
silently dead from first contact.

Fix: treat a frame-decode error as skip-and-continue (log), or at worst a
transient reconnect; keep terminal classification for real connection
failures. Skipping unknown `@type` in `frame_stream` fixes it for every
consumer.

## JMAP-011 - `scope_lifecycle` dies for good on a conforming `cannotCalculateChanges`

Hunter severity: P2. Scopes: jmap account and sync adoption (both reported it
independently).

`discover.rs::scope_lifecycle` classifies a failed `Mailbox/changes` with
`JmapErrorContext::new(AccountOperation::ScopeLifecycle)`, which has no cursor
scope. `error.rs::convert_method` maps `CannotCalculateChanges` (and
`TooManyChanges`) without a cursor scope to `Protocol(ContractViolation)`,
pinned by `cannot_calculate_changes_without_scope_is_contract_violation`, on
the theory that the server "returned a cursor-specific error for a
non-cursored request". But `Mailbox/changes` IS state-based, and
`cannotCalculateChanges` is the RFC-sanctioned answer once the server has
expired the open-time state (routine after a long session or days of laptop
suspend). The stream yields `Terminated(ProviderContractViolation)` and ends;
the engine's `lifecycle_termination` maps `Terminal` to
`LifecycleTermination::Terminate`, broadcasts a terminal `MultiplexerEvent`,
and ends the lifecycle reader. The account never again hears of folder
creates, renames or deletes, and the error blames the provider.

Claims broken: the in-code comment at the `ChangeWalkGuard` site reserves the
`ContractViolation` termination for "a non-conformant provider";
`reference/error-model.md`'s rule that a stale state is `CursorInvalid` /
restart, not terminal.

Fix: on this error the poller reseeds: full `Mailbox/get`, diff against
`mailbox_names` to emit `Created` / `Deleted` / `Renamed`, set the new state,
continue. At minimum classify with a scope so the restart lane applies. More
generally the "no cursor scope -> contract violation" rule in
`convert_method` is wrong for every state-based caller with no `CursorScope`.

## JMAP-012 - `draft_*` and `draft_send` target different accounts when Mail and Submission primaries differ

Hunter severity: P2. Scope: jmap account.

`account.rs`: `draft_create` / `draft_update` / `draft_discard` route to
`self.mail` (primary Mail account); `draft_send` routes to `self.submission`
(primary Submission account) with the bare draft id. The comments on
`send_message` / `draft_send` say the two "can resolve a distinct account".
When they do, `draft_send` asks the Submission account to submit an email id
that exists only in the Mail account: notFound, or on an id collision an
unrelated message is submitted. `capabilities.rs` advertises `draft_create`
unconditionally and `draft_send` on `support.submission`, so nothing gates
the combination.

Fix: pin the whole draft lifecycle to one account (the submission account when
it exists), or qualify `DraftHandle` with its owning account the way foreign
object ids are qualified.

## JMAP-013 - Scheduled sends are created with `undoStatus: final` while cancel and reschedule are advertised

Hunter severity: P2. Scope: jmap account.

`send_message` and `reschedule_send` set `undo_status(UndoStatus::Final)` on
every created submission, scheduled ones included. RFC 8621 s7 makes
`undoStatus` server-set on create, so a strict server can answer
`invalidProperties`; a lenient one that honours it is told the submission
cannot be recalled, contradicting `scheduled_send` / `cancel_scheduled_send` /
`reschedule_send` (which rely on `pending` -> `canceled`). Fix: omit
`undoStatus` on create.

## JMAP-014 - Primary `EmailDelivery` (or any unmapped type) push entry triggers a full-account `Unknown` repoll

Hunter severity: P3. Scope: jmap account.

`push.rs::emit_state_change`: for the primary account, a data type with no
`scope_for_data_type` mapping emits `HintPayload::Unknown`. The subscription
enables only Email/Mailbox/Thread, but servers commonly send `EmailDelivery`
alongside `Email` (RFC 8621 s1.5), so each delivery costs a narrow hint plus a
full-account reconcile. The foreign branch deliberately drops unmapped types
for this reason ("degrading to `Unknown` here would full-repoll the whole
account on every foreign delivery"); the primary branch contradicts it.
Primary `Thread` maps to `Type(Thread)`, a scope `changes::stream` refuses as
unsupported, so that hint is dead weight. Fix: ignore `EmailDelivery` (its
`Email` sibling carries the signal) and drop or remap `Thread`.

## JMAP-015 - Hydration repeats the route-starvation bug the mutation pipeline already fixed

Hunter severity: P3. Scope: jmap account.

`hydrate.rs::stream` buffers ids per route and flushes only at
`max_objects_in_get` or end of input. `mutation.rs::RouteBuffers` documents and
fixes exactly this ("an owner that trickles could be starved indefinitely by an
owner that streams") with `last_touch`. With foreign and primary ids mixed in a
long input, the foreign partial batch waits for end of input; with a slow
input a single route's partial batch never flushes, although the advertised
`BatchingPolicy::max_wait` is 100 ms. Fix: share `RouteBuffers` between the two
pipelines, with idle and `max_wait` flushing driven by `BatchingPolicy` so the
capability is honoured by construction.

## JMAP-016 - Mutation and PIM writes use `ifInState`, turning unrelated concurrency into failures

Hunter severity: P3. Scope: jmap account.

`mutation.rs::apply_batch` and `pim.rs::send_email_set_with_retry` send every
`Email/set` with `ifInState` = cached account-wide Email state. The patches
(keyword add/remove, `mailboxIds` path patches, destroy) are commutative. On a
busy account any delivery between probe and set yields `stateMismatch`, one
re-probe and retry, then per-item `Failed(ConcurrencyConflict)` (bulk) or `Err`
(single-target doors); the engine retries with `AfterStateRefresh` into the
same race. `MutationConcurrency::StateBased` is advertised, but only
`FlagOp::Set` (whole-keyword replace) gains anything. Proposal: drop
`ifInState` for commutative patches, keep it only for `FlagOp::Set` if
anywhere.

## JMAP-017 - `delete_thread` with `current == Trash` destroys messages that are not in Trash

Hunter severity: P3. Scope: jmap account.

`pim.rs::delete_thread`: when `current` is Trash it resolves the WHOLE thread
via `Thread/get` and `Email/set destroy`s every member, including members in
Inbox, Sent or elsewhere. The non-trash branch moves only by membership patch.
Destroying the user's Sent copy and live Inbox messages is irreversible loss
beyond what the call names. If the cross-provider contract means "destroy the
whole thread", it belongs in the reference; otherwise filter the destroy to
ids whose `mailboxIds` contain Trash. Compare GRAPH-001 (Graph's
destroy-if-in-Trash rests on a soft delete).

## JMAP-018 - The mutation stream drops already-decided local rejections when it terminates

Hunter severity: P3. Scope: jmap account.

`mutation.rs::mutation_stream`: on `Err` from `apply_batch` it yields
`Terminated` and returns. The `rejected` outcomes drained for that route
(cross-account destinations, decided locally with no wire involved), and
pending routes' rejections, are discarded. The module's accounting ("the
caller still sees exactly one outcome per input target") loses them. Emit them
before `Terminated`.

## JMAP-019 - Scheduled `send_message` builds an envelope only when `from` is set

Hunter severity: P3. Scope: jmap account.

With `request.from` None and recipients present there is no envelope; the
server derives one, which is fine for immediate sends. A scheduled send in that
shape fails with `scheduled_requires_envelope` although recipients exist and
the identity's address could stand in as `mailFrom`. The `scheduled_send`
capability flag does not say `from` is mandatory. Document it or fall back to
the identity email.

## JMAP-020 - Implicit `Email/set` responses exist, contradicting the "no multiple responses" claim, and leave the state cache one behind

Hunter severity: P3. Scope: jmap protocol.

`reference/jmap.md` (internal request/response flow) and the doc on
`Response::get` justify not modelling multiplicity with "no currently wired
method produces multiple responses". `sync/pim.rs` send and draft-send set
`onSuccessUpdateEmail` / `onSuccessDestroyEmail` on `EmailSubmission/set`, so
RFC 8621 s7.5 has the server emit an implicit `Email/set` response under the
SAME call id after the `EmailSubmission/set` response. It works because `get`
takes the first match, but the implicit `Email/set` carries the post-move
`newState`, which `pim.rs` never feeds into `state_cache`; only the earlier
explicit state is advanced, so the cache is one state behind after every send.

## JMAP-021 - The `NoPrimaryAccount` guard refuses methods that carry no accountId

Hunter severity: P3. Scope: jmap protocol.

`Request::call` refuses any method when the request account id is empty, on
the claim (code comment and `reference/jmap.md`) that "every method this crate
defines carries an `accountId`". `PushSubscription` declares
`requires_account_id() == false`, and RFC 8620 s7.2 gives
`PushSubscription/get` and `/set` no `accountId`. On a session with no
`primaryAccounts` those calls are refused for a reason that does not apply.
The guard should check `O::requires_account_id()` (or the method's own
serialized `accountId`).

Related smell: `SessionState::derive` maps "no primaryAccounts" to
`AccountId::new("")`, a sentinel recovered by `is_empty()` in `Request::call`;
`Option<AccountId>` would carry the state the comment already describes.

## JMAP-022 - `Request::new` reads the session twice

Hunter severity: P3 (latent while `refresh_session` is unwired). Scope: jmap
protocol.

`CallLimit::read(&client.session())` and `client.default_account_id()` each
take their own `session_state()` snapshot, contradicting `SessionState`'s
purpose ("Hold the returned `Arc` for the duration of a URL build so a
concurrent refresh cannot splice two sessions into one request"). A refresh
between them pairs one session's `maxCallsInRequest` with another's account.
`encode_ws_request` likewise reads `self.session()` for `maxSizeRequest`
independently of the request's snapshot. Take one snapshot and keep it on
`Request`.

## JMAP-023 - Overlapping session refreshes can install the older session last

Hunter severity: P3 (latent). Scope: jmap protocol.

`refresh_session` fetches without the lock and installs unconditionally. If
two refreshes overlap and the earlier GET returns later, the client ends on
the older session with `session_updated = true`. The comment protects
state/limit pairing, not recency. The method is unwired but deliberately kept
(per `reference/jmap.md`), so it should be correct: serialize refreshes or
compare a fetch generation.

## JMAP-024 - ProblemDetails without `type` is not recognised

Hunter severity: P3. Scope: jmap protocol.

`ProblemDetails.p_type` is mandatory, but RFC 7807 s3.1 says an absent `type`
means `"about:blank"`. A problem document with only `status` / `title` /
`detail` falls through to `Error::Transport`, losing its explanation to the
problem-details mapping `reference/jmap.md` describes. Default it.

## JMAP-027 - `connect_ws` bypasses bifrost-net and reads the raw token source

Lateral, from jmap protocol and net adoption. Related: SHARED-002.

`connect_ws` opens its own `TcpStream` and TLS connector. Consequences not
written down anywhere (the reference only says WebSocket is
reqwest-specific):
- it ignores the trusted-host redirect policy and any proxy, and gets no 401
  token-refresh retry at the handshake;
- it has no connect or TLS-handshake bound of its own, unlike net's
  `NetConfig::connect_timeout`, relying on the push layer's outer handshake
  bound, so a non-push caller can hang;
- it sends no `User-Agent` or `traceparent`, which every net request carries;
- its bearer comes from `Authorization::header_value` -> `source.current()` on
  the consumer's raw source, not net's refresher (SHARED-002).

## JMAP-028 - Mailbox inventory `Mailbox/get ids: null` can fail terminally on large accounts

Smell, jmap account.

`mailbox_inventory` issues `Mailbox/get` with `ids: null`. A server may answer
`requestTooLarge` when the mailbox count exceeds `maxObjectsInGet`, which
`convert_method` classifies as `Request(Malformed)` -> terminal `ClientBug`: a
permanent inventory failure on large shared-heavy accounts. Page via
`Mailbox/query` or batch the ids. More generally, `RequestTooLarge` /
`RequestSizeLimit` -> `Request(Malformed)` is terminal for the whole stream
although the cause is a batch size the account layer chose; shrinking and
retrying would heal it.

## JMAP-029 - `establish_initial_cursor` hands out a stale open-time seed

Smell, jmap account.

It returns the `open()`-time seed for any later `RestartScope`, possibly hours
or days old. Safe (it replays) but it raises the odds of
`cannotCalculateChanges` -> another restart with the same stale seed; if the
server has expired that state it loops until reopen. Probing a fresh state on
establish breaks the loop.

## JMAP-030 - `apply_flags` has a silent `_ => {}` arm

Smell, jmap account.

Under a `validate_for_account` that already narrows the variants, a future
`FlagOp` variant would compile into a silent no-op patch that still reports
`Applied`. Prefer an explicit refusal.

## JMAP-031 - Smaller jmap account observations

Smells, jmap account.

- `push::subscribe` / `unsubscribe` vs `close()`: `close` sends
  `disable_push_ws` without the `subscriptions -> enabled -> push_state`
  guards, so a racing `push_subscribe` can re-enable after the disable.
  Harmless once the socket drops, but it breaks the "one lock order
  everywhere" claim in the `WsState` doc.
- `changes_walk` advances `email_states` / `mailbox_states` BEFORE yielding the
  batch, so the cache can run ahead of the durable checkpoint if the consumer
  drops the stream. Harmless while the cache only feeds `ifInState`; worth a
  comment or a rename to `latest_known_state`.
- `reconcile_hydration` reports both `notFound` and the unanswered lane per
  submitted occurrence, and duplicate ids go on the wire as duplicates.

## JMAP-034 - Design proposals from the jmap account hunter

Not defects; recorded with the defects they would remove.

- Push health as state, not events: `watch<PushHealth>` plus a broadcast for
  hints, fixing JMAP-009 structurally and making `push_stream` a projection of
  current state.
- One state-walk policy: `changes_walk`, `scope_lifecycle` and the inventory
  loop each decide independently what a moved state or `cannotCalculateChanges`
  means. One "state walk" type owning the guard, the reseed-on-expiry policy
  and the error context (always carrying a scope or an explicit state-based
  flag into `convert_method`) removes JMAP-008 and JMAP-011 as a class.
- The PIM send path as a small state machine with compensations (create email
  -> submit -> on failure destroy; cancel -> confirm -> create) instead of
  single batched requests whose partial outcomes read as total failure
  (JMAP-005 to JMAP-007 share this shape).
