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

## JMAP-003 - Blob downloads are fully buffered with no streaming path

Lateral, from jmap protocol and net adoption.

`Client::download` buffers the entire blob into `Bytes` with no ceiling of its
own and goes through net's buffered `send`, so a message or attachment above
`DEFAULT_MAX_BUFFERED_RESPONSE` (64 MiB) fails as `ResponseTooLarge`.
`download_stream` exists in net and JMAP does not use it. Together with
JMAP-002, large-blob reads are both memory-bound and deadline-bound.

## JMAP-004 - The EventSource template still lets `.` and `..` through

Scope: net adoption. Residue: upload and download templates now use
context-aware encoding (`encode_template_value_at`) that protects dot-only path
values; `crates/jmap/src/event_source/stream.rs` still substitutes without it,
so a `{types}`-style or path variable of `.` / `..` navigates. Use the same
helper there.

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

## JMAP-035 - Smaller observations from the send-path fixes

- Cancel-first `reschedule_send` has a window: if the cancel succeeds and the
  replacement create fails, the old send is cancelled with nothing in its place.
  No test covers replacement-create failure.
- A test in `sync/capabilities.rs` still sets `UndoStatus::Final` on a synthetic
  scheduled create, contradicting the production shape.
- `Email::take_id()` yields an empty id when a created response omits `id`;
  send cleanup then skips the destroy and a successful send could return an
  empty `ObjectId`.
- In the send paths, an extraction failure for the preceding Email create or
  import returns before the submission result in the same response is
  examined, which can obscure a committed send.

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
