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

## JMAP-036 - A foreign send-as can consume uploaded attachment handles from another account without `Blob/copy`

Lateral from the draft-routing fix. Uploaded attachment handles retain only the
blob id, and blob ids are scoped to the account that received the upload
(RFC 8620 s6.3 transfers across accounts with `Blob/copy`). A send-as through a
foreign account that reuses a handle uploaded to the primary account addresses a
blob the target account cannot see.

## JMAP-022 - The WebSocket encoder reads the live session for `maxSizeRequest`

Residue: `Request::new` now holds one session snapshot through the send and
session refreshes are serialized. `encode_ws_request` in `client_ws.rs` still
reads `self.session()` for `maxSizeRequest`, so a refresh can pair a request
snapshot with a newer frame-size limit.

## JMAP-037 - Thread destroy from Trash is not chunked to `maxObjectsInSet`

Lateral from the thread-delete fix: all selected Trash members still go in one
`Email/set`, which a very large thread could push past `maxObjectsInSet`. The
existing test `a_foreign_thread_already_in_the_shares_trash_is_destroyed_in_the_share`
(`sync/factory.rs`) scripts `Email/set` straight after `Thread/get` and needs the
new `Email/get` membership read.

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
