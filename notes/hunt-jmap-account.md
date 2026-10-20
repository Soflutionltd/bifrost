# Hunt: bifrost-jmap account layer (`crates/jmap/src/sync/`)

Scope: the `Account` impl - cursor envelope, inventory, changes, hydration,
WebSocket push, mutation pipeline, PIM doors in `pim.rs`, recovery taxonomy and
error translation. Severity is my own call: P1 = loses or duplicates user data
or user-visible actions; P2 = breaks a stated contract or kills a sync lane;
P3 = smaller contract gap, inconsistency or smell.

Defects first, then design proposals, kept apart on purpose.

---

## Defects

### D1 (P1) `draft_send` reports a committed send as failed when the cache refresh after it fails

`pim.rs::draft_send`: after `EmailSubmission/set` comes back with `created`
(so the message is already submitted and the draft already moved to Sent by
`onSuccessUpdateEmail`), the function runs
`mutation::probe_email_state(&mail).await.map_err(to_acct_err(DraftSend))?`
only to refresh `email_states`. If that probe fails (a transport blip, 5xx,
rate limit), `draft_send` returns `Err` even though the send went through.
The error is a retryable class, so a consumer or engine that retries the
"failed" `DraftSend` submits the same email id again: a duplicate send.

Broken claim: the `Account::draft_send` signature returns `Ok(ObjectId)` for a
send that happened. A cache refresh is an optimisation and must never decide
the outcome. Fix: make the probe best-effort (log it, or invalidate the cache
entry instead of probing), and return `Ok` once `created` is confirmed.
`send_message` gets this right by using the `Email/set` `newState` it already
has. `draft_send` could do the same by adding an `Email/get ids: []` to the
same request, or by just dropping the cache entry.

### D2 (P1) `reschedule_send` is not the double-send guard its comment says it is, and it can lose a scheduled message

`pim.rs::reschedule_send` sends the cancel of the old submission and the create
of the new one in ONE `EmailSubmission/set`. RFC 8620 s5.3 processes a `/set`
in a fixed order: create, then update, then destroy. So the server creates the
new submission BEFORE it tries the cancel:

- If the cancel is refused (`cannotUnsend`, relay already released), the new
  submission already exists and is live. The code then returns `Err`. The
  in-code comment calls this "the double-send guard", but the double send has
  already been committed on the server and the caller is told nothing went
  through. The comment even admits "the new submission, if any, is left in
  place".
- When the original send used `save_to_sent == Some(false)`,
  `send_message` attached `onSuccessDestroyEmail`. That fires when the
  scheduled submission is created, so the email object is destroyed right
  away. A later `reschedule_send` creates against a destroyed `emailId` and
  fails, then the update cancels the original. The result: the scheduled
  message is cancelled, nothing replaces it, and the caller gets `Err`. The
  message is lost.

Fix: cancel in its own call and confirm `updated`, then create in a second
call (or create first and destroy-on-failure, whichever ordering the product
wants). For the destroy-on-success shape, either refuse reschedule up front or
do not destroy the email for a scheduled submission until delivery.

### D3 (P1/P2) Failed sends leave orphan drafts behind

`send_message` and `send_raw_message` put the `Email/set` create (or
`Email/import`) and the `EmailSubmission/set` in one request. When the
submission is refused (`forbiddenFrom`, `invalidRecipients`,
`tooManyRecipients`, a quota limit) while the email create succeeded, the
function returns `Err`. The created message stays in Drafts with `$draft`.
Each retry adds another one. Nothing destroys the email on submission
failure, and the `ObjectId` is not returned to the caller, so the caller has
no handle to clean it up with.

Broken claim: `send_message` is a single send operation, and on `Err` the
caller is entitled to assume nothing was persisted. Fix: on a submission
failure with a created email, issue an `Email/set destroy` for it (best
effort), or carry the draft id in the error.

### D4 (P2) Email inventory livelocks on any mailbox that receives mail faster than one walk takes

`inventory.rs::email_inventory_loop` pins `queryState` across pages and ends
via `terminated_walk_superseded` (`RestartScope`) the moment it moves.
`reference/jmap.md` itself says "one delivered message advances
`queryState`". The walk carries no resumable position (`checkpoint: None`),
so every restart begins again at position 0. On a large mailbox (say 100k
messages at 256 per page, about 400 query+get round trips) with mail arriving
every few minutes, the walk never reaches the empty page. The scope's
inventory never completes and the engine pays for the full walk over and over.

The reference text (lines about `terminated_walk_superseded`) defends
`RestartScope` over a terminal class because "a terminal class there would let
ordinary mail delivery permanently kill a scope's inventory". Under steady
delivery `RestartScope` does exactly that, just more expensively. Document and
code disagree with the outcome they both claim.

The pin is also not needed for the claimed safety:
- an anchor destroyed under the walk already comes back as `anchorNotFound`,
  which `convert_method` maps (cursor scope present) to
  `SyncState(CursorInvalid)` -> `RestartScope`;
- with the unfiltered primary/foreign-account walk sorted by immutable
  `receivedAt`, new arrivals land before the anchor (already passed) or ahead
  of it (still walked). In either case the change cursor, established before
  the inventory, also reports them as `Created`;
- deletions ahead of the anchor just drop out.

Fix: drop the `queryState` pin for the unfiltered walk and let
`anchorNotFound` be the only "coverage unprovable" signal. Optionally make the
anchor a resumable checkpoint so a restart does not start from zero. The
legacy `inMailbox`-filtered walk has the same livelock and the same remedy,
since membership moves are covered by the change cursor.

### D5 (P2) Push health events sent before `push_stream()` subscribes are lost, and push then looks alive forever

`push.rs`: `WsState::spawn` starts `reader_loop` inside `open()`. Events go
out on a `broadcast` channel, and a broadcast only delivers to receivers that
exist at send time. `push_stream()` subscribes only when the engine's push
forwarder calls it, which happens after attach (`sync/src/engine/attach.rs`).
So:

- If the first handshake fails terminally (auth lost, subprotocol refused ->
  `CapabilityChanged`), `WatchEvent::Terminated` goes to zero receivers and is
  dropped. The reader then `break`s out of `reader_loop` and is gone.
- Every later `push_stream()` gets a receiver on a channel whose sender
  (`WsState::tx`) is still alive, so `recv` never returns `Closed`. The stream
  is silent for the life of the account. That is exactly the "dead push the
  engine cannot tell apart from a quiet mailbox" state the reader's keepalive
  machinery exists to prevent.
- A second subscriber that arrives after the reader terminated later in life
  (engine forwarder reconnect loop, a consumer's own `push_stream`) gets the
  same silent stream.
- `Lagged` can also drop a `Terminated` that sits in the overwritten window.
  It is degraded to a `Coalesced/Unknown` hint, which is not a terminal
  signal.

Broken claim: `WatchEvent::Terminated` is the reader's terminal health signal
(the `ReaderStep::Terminal` doc: "emit `Terminated` and stop the reader so the
engine can reopen the account"). Fix: latch the terminal state (or the whole
current health) in a `watch` channel inside `WsState`. `push::stream` replays
the latched `Terminated` on subscribe and then ends, and a lagged receiver
re-reads the latch instead of losing it. `Reconnected` emitted before the
first subscriber has the same loss, but the reconciler's reconnect-is-Unknown
rule makes that one cheap.

### D6 (P2) One unrecognised WebSocket frame permanently kills push

`client_ws::decode_frame` turns an unknown `@type` into a `serde_json::Error`,
which becomes `Error::ResponseDecode`. A binary frame becomes
`Error::NotParsable`. Both reach `push.rs::reader_pass` as `Err`, which
classifies through `into_account_error` to `Protocol(ParseFailed)` ->
`ProviderContractViolation`, and `is_terminal()` is true for that. So the
reader emits `Terminated` and stops for good.

Concrete triggers: a `calendars`-off build (the `jmap-calendars-off` /
`jmap-no-default` sweeps are promised to consumers) whose server sends
`CalendarAlert`; any server extension frame type; a single malformed
`StateChange`. RFC 8887 servers are allowed to add frame types, and one
unknown frame is not evidence the connection or account is broken. Combined
with D5, this can mean push is silently dead from first contact.

Fix: in the reader, treat a frame-decode error as skip-and-continue (log it),
or at worst as a transient reconnect. Keep terminal classification for real
connection-level failures. Moving this down into `frame_stream` (skip unknown
`@type`) would fix it for every consumer.

### D7 (P2) `scope_lifecycle` dies for good on a conforming `cannotCalculateChanges`

`discover.rs::scope_lifecycle` calls `into_account_error` with
`JmapErrorContext::new(ScopeLifecycle)`, with no cursor scope. In
`error.rs::convert_method`, `CannotCalculateChanges` (and `TooManyChanges`)
without a cursor scope map to `Protocol(ContractViolation)`, on the theory
that the server "returned a cursor-specific error for a non-cursored request".
But `Mailbox/changes` IS a state-based request. `cannotCalculateChanges` is
the RFC-sanctioned answer once the server has expired the open-time Mailbox
state, which is routine after a long-lived session or a laptop suspended for
days. The lifecycle stream then yields `Terminated(ProviderContractViolation)`
and ends. The account's only mailbox-lifecycle channel is gone, and the error
blames the provider.

Fix: handle `cannotCalculateChanges` in the poller by reseeding. Do a full
`Mailbox/get`, diff it against `mailbox_names` to emit
`Created`/`Deleted`/`Renamed`, set the new state, and continue. More generally,
the "no cursor scope -> contract violation" rule in `convert_method` is wrong
for every state-based caller that has no `CursorScope`.

### D8 (P2) `draft_*` and `draft_send` target different accounts when Mail and Submission primaries differ

`account.rs`: `draft_create` / `draft_update` / `draft_discard` route to
`self.mail` (primary Mail account). `draft_send` routes to `self.submission`
(primary Submission account) and passes the draft id bare. The comments on
`send_message` / `draft_send` say explicitly that the two "can resolve a
distinct account". When they do, every `draft_send` asks the Submission
account to submit an email id that only exists in the Mail account. It fails
with notFound, or on an id collision submits an unrelated message.
`capabilities.rs` advertises `draft_create` unconditionally and `draft_send`
on `support.submission`, so nothing gates the combination.

Fix: pin the whole draft lifecycle to one account (the submission account
when it exists), or qualify `DraftHandle` with its owning account the way
object ids are qualified for foreign accounts.

### D9 (P2) Scheduled sends are created with `undoStatus: final`, yet cancel and reschedule are advertised

`send_message` and `reschedule_send` set `undo_status(UndoStatus::Final)` on
every created submission, scheduled ones included. RFC 8621 s7 makes
`undoStatus` server-set on create, so a strict server can answer
`invalidProperties`. A lenient server that honours the value is told "this
submission cannot be recalled", which contradicts the `scheduled_send` /
`cancel_scheduled_send` / `reschedule_send` surface (those rely on
`pending` -> `canceled`). Either reading breaks a published door. Fix: omit
`undoStatus` on create.

### D10 (P3) A primary `EmailDelivery` (or any unmapped type) push entry turns into a full-account `Unknown` repoll

`push.rs::emit_state_change`: for the primary account, a data type with no
`scope_for_data_type` mapping emits `HintPayload::Unknown`. The subscription
only enables Email/Mailbox/Thread, but servers commonly send `EmailDelivery`
alongside `Email` (RFC 8621 s1.5). When one does, each delivery costs one
narrow hint plus one full-account reconcile. The foreign branch deliberately
drops unmapped types for exactly this reason ("degrading to `Unknown` here
would full-repoll the whole account on every foreign delivery"). The primary
branch contradicts that reasoning. Primary `Thread` also maps to
`Type(Thread)`, a scope `changes::stream` refuses as unsupported, so that hint
is dead weight. Fix: ignore `EmailDelivery` (its `Email` sibling carries the
signal), and drop or remap `Thread`.

### D11 (P3) Hydration repeats the route-starvation bug the mutation pipeline already fixed

`hydrate.rs::stream` buffers ids per route and flushes a route only when it
reaches `max_objects_in_get` or when input ends. `mutation.rs::RouteBuffers`
documents and fixes exactly this ("an owner that trickles could be starved
indefinitely by an owner that streams") with `last_touch`. Hydration has no
equivalent, so when foreign and primary ids are mixed in a long input stream,
the foreign partial batch waits for end of input. With a long-lived or slow
input, a single route's partial batch also never flushes, even though the
advertised `BatchingPolicy::max_wait` is 100ms. Same fix as the sibling:
share `RouteBuffers` between the two pipelines.

### D12 (P3) Mutation and PIM writes use `ifInState`, which turns unrelated concurrency into failures

`mutation.rs::apply_batch` and `pim.rs::send_email_set_with_retry` send every
`Email/set` with `ifInState` = the cached account-wide Email state. The
patches themselves (keyword add/remove, `mailboxIds` path patches, destroy)
are commutative and do not depend on anything that state guards. On a busy
account, any delivery between probe and set yields `stateMismatch`, then one
re-probe and retry, then per-item `Failed(ConcurrencyConflict)` (bulk) or
`Err` (single-target doors). The engine retries with `AfterStateRefresh` into
the same race. `MutationConcurrency::StateBased` is advertised, but nothing
here gains from it except for `FlagOp::Set` (whole-keyword replace). Raising
it as a defect because user-visible mutations fail under ordinary mail flow,
which the signatures do not suggest. Proposal: drop `ifInState` for
commutative patches and keep it only for `FlagOp::Set`, if anywhere.

### D13 (P3) `delete_thread` with `current == Trash` destroys messages that are not in Trash

`pim.rs::delete_thread`: when the caller's `current` container is Trash, it
resolves the WHOLE thread through `Thread/get` and `Email/set destroy`s every
member, including thread members that sit in Inbox, Sent or other folders.
The non-trash branch moves only by membership patch. "Delete this thread from
Trash" destroying the user's Sent copy and live Inbox messages is
irreversible data loss beyond what the call names. If the cross-provider
contract really means "destroy the whole thread", that belongs in the
reference. Otherwise filter the destroy to ids whose `mailboxIds` contain
Trash.

### D14 (P3) The mutation stream drops already-decided local rejections when it terminates

`mutation.rs::mutation_stream`: on `Err` from `apply_batch`, the stream yields
`Terminated` and returns. The `rejected` outcomes drained for that route
(cross-account destinations, already decided locally, no wire involved) are
discarded, as are pending routes' rejections. Those items had a definite
outcome, and the closed one-outcome-per-target accounting the module describes
("the caller still sees exactly one outcome per input target") loses them.
Emit them before `Terminated`.

### D15 (P3) `send_message` scheduled path: an envelope is only built when `from` is set

When `request.from` is None and recipients exist, there is no envelope, so
the server derives one, which is fine for immediate sends. A scheduled send in
that shape fails with `scheduled_requires_envelope` even though recipients
exist and the identity's address could stand in as `mailFrom`. Minor: the
`scheduled_send` capability flag does not mention that `from` is mandatory.
Either document it or fall back to the identity email.

---

## Smaller observations and smells

- `push::subscribe` / `unsubscribe` vs `close()`: `close` sends
  `disable_push_ws` without taking the `subscriptions -> enabled -> push_state`
  guards, so a `push_subscribe` racing `close` can re-enable after the
  disable. Harmless once the socket drops, but it breaks the "one lock order
  everywhere" claim in the `WsState` doc.
- `changes_walk` advances `email_states` / `mailbox_states` BEFORE yielding
  the batch, so the in-memory cache can run ahead of the durable checkpoint
  if the consumer drops the stream. Today the cache only feeds `ifInState`, so
  this is harmless. It becomes a bug the day anything treats that cache as
  "what the consumer has seen". Worth a comment, or a rename to
  `latest_known_state`.
- `mailbox_inventory` issues `Mailbox/get` with `ids: null`. A server may
  answer `requestTooLarge` when the mailbox count exceeds `maxObjectsInGet`,
  which `convert_method` classifies as `Request(Malformed)` -> terminal
  `ClientBug`. That is a permanent inventory failure on large shared-heavy
  accounts. Page via `Mailbox/query` or batch the ids.
- `establish_initial_cursor` hands out the `open()`-time seed for any later
  `RestartScope`, possibly hours or days old. That is safe (it replays), but
  it raises the odds of `cannotCalculateChanges` -> another restart that gets
  the same stale seed. If the server has expired that state, this loops until
  reopen. Probing a fresh state on establish would break the loop.
- `RequestTooLarge` / `RequestSizeLimit` -> `Request(Malformed)` -> terminal
  for the whole stream, even though the cause is a batch size the account
  layer chose. Shrinking and retrying would heal it.
- `reconcile_hydration` reports both `notFound` and the unanswered lane per
  submitted occurrence. Duplicate submitted ids also go on the wire as
  duplicates. Harmless, but `Email/get` with repeated ids is a needless load.
- `apply_flags` has a `_ => {}` arm under a `validate_for_account` that
  already narrows the variants. A future `FlagOp` variant would compile into a
  silent no-op patch that still reports `Applied`. Prefer an explicit refusal.

---

## Design proposals (not defects)

- **One route-buffer abstraction.** `mutation::RouteBuffers` and the ad-hoc
  `HashMap<HydrationRoute, Vec<_>>` in hydrate are the same per-owner
  batcher. Unify them, with idle and `max_wait` flushing driven by the
  advertised `BatchingPolicy`, so the capability flag is honoured by
  construction.
- **Push health as state, not events.** Replace the broadcast-only health
  signalling with a `watch<PushHealth>` plus a broadcast for hints. That fixes
  D5 structurally and makes `push_stream` a pure projection of current state.
- **State-based walk policy in one place.** `changes_walk`, `scope_lifecycle`
  and the inventory loop each decide independently what a moved state or
  `cannotCalculateChanges` means. One "state walk" type that owns the guard,
  the reseed-on-expiry policy and the error context (always carrying a scope
  or an explicit "state-based" flag into `convert_method`) would remove D4 and
  D7 as a class.
- **Make the PIM send path a small state machine with compensations** (create
  email -> submit -> on failure destroy; cancel -> confirm -> create), instead
  of single batched requests whose partial outcomes are reported as total
  failure (D1-D3 share this shape).
