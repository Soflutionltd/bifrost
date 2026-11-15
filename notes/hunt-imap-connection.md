# Hunt: imap-connection (shape and placement)

Scope: `crates/imap/src/connection/` - driver task, dispatch and consumers,
pipeline, auth, idle, lifecycle and state, wire framing and metering,
literals, streaming FETCH, UID/sequence operations, sort/thread, search
validation, extensions, ergonomics, typed events. Followed into `types/`,
`codec/`, `account/`, `bifrost-smtp` and `bifrost-sasl` where a question led
there. Reading only; nothing was edited or run. No source line numbers are
cited; functions and files are named instead.

The report is ordered: a short ranked summary, then the three questions
(axes, duplicated decisions, structure), then lateral findings (bugs and
smells found on the way). Each lateral item is marked LIVE (wrong today),
LATENT (wrong under a reachable but unexercised sequence) or SMELL.

---

## Ranked summary

1. **The command-admission decision has three front doors and two clocks.**
   Whether a command may be sent (session state, capability, ENABLEd mode,
   saved-search operand, rev2 removals, LIST/STATUS/FETCH option gates) is
   decided per method on the handle against a stale snapshot, again (partly)
   in the encoder against live state, and not at all by the `Pipeline`
   builder, which bypasses every handle-side gate. Some gates exist only on
   the handle, some only in the encoder, some in both with different
   questions. One `admit(&Command, &SessionView)` owner, called early by the
   handle and authoritatively by the driver, removes roughly 60 inline gate
   blocks. (Q2, Q3)
2. **Pending session effects are six loose flags, armed in two places,
   with one already mis-cleared.** `ProtocolState` carries `in_logout`,
   `in_auth`, `in_select`, `in_close`, `in_notify_set`, `in_unauthenticate`;
   the driver arms them through a `matches!` chain in `run_prepared_command`,
   the pipeline arms NOTIFY separately in `apply_pipeline_pre_effects`, and
   `CommandKind::pipelinable` independently restates which commands move
   state. `in_logout` is never cleared on a NO/BAD, so a refused LOGOUT
   poisons the next command (lateral L1). One `PendingEffect` enum keyed by
   tag, derived from `Command`, is the fix. (Q1, Q2, L1)
3. **`(capabilities, enabled)` is an untyped pair carried by five views,
   and the enabled half is `Vec<String>`.** "Is QRESYNC ENABLEd" is a string
   scan at five sites, "is UTF8=ACCEPT enabled" at five, "is AUTH=X
   advertised" is answered by two different comparisons held in step by a
   pairwise test. `types::profile` says outright "CALL IT THROUGH A TYPED
   WRAPPER" and no wrapper exists. (Q1, Q2)
4. **UID versus sequence number is the axis IMAP most needs typed, and the
   connection layer does not use the types it has.** `Uid`, `Seq`,
   `UidValidity`, `ModSeq`, `UidSet`, `SeqSet` exist in `types/ids.rs`;
   `Command`, `SearchResult`, `SelectedMailbox`, `FetchResponse`,
   `IdleEvent`, `AppendConsumer` and every handle method still traffic in
   `u32`/`u64`/`SequenceSet`. `CommandKind` erases UID-ness, so a search
   result cannot say which it holds. (Q1)
5. **One server event is classified four times by four owners that already
   disagree.** `TypedEvent::priority`, `TypedEvent::impact` (in
   `types/events.rs`), `typed_event_to_idle_event`, and
   `account::push::map_idle_event` each decide what an untagged response
   means; RECENT is "no impact" in one and "invalidate the folder" in
   another, and the driver's own queue overflow is laundered into a server
   `ALERT` string on the IDLE path. (Q2, L5)
6. **The pipeline is ~1,700 lines with no production caller that shapes
   the driver's hardest invariants.** `ImapConnection::pipeline()` has no
   caller in `account/`; its routing requirements are why
   `wait_for_continuation` has two modes, why `PipelineRouting::sending`
   exists, and why NOTIFY pre-effects have a second copy. It is crate-private
   (the whole connection layer is), so this is an owner decision, not a
   published-API deletion. (Q3)
7. **SASL mechanism selection and channel-binding policy live in both IMAP
   and SMTP**, held in step by a comment ("the two crates must not differ
   here"), with the ladders and the `SaslError` mapping already diverged.
   bifrost-sasl is the owner, exactly as `26aa7c25` did for OAuth payloads.
   (Q2)
8. **Connection setup runs outside the driver.** Greeting, initial
   CAPABILITY and STARTTLS run on a stack-held `WireReader`/`ProtocolState`
   before the driver is spawned, with their own timeout and evidence model,
   their own ALERT path (raw channel, because `emit` is not visible there),
   and their own STARTTLS availability rule. (Q3)

---

## 1. Axes that should be types

### 1.1 Pending session effect: six fields, one meaning

`ProtocolState` (`state.rs`) holds `in_logout: bool`, `in_auth: bool`,
`in_select: Option<MailboxName>`, `in_close: bool`,
`in_notify_set: Option<NotifyFlags>`, `in_unauthenticate: bool`, with
`#[allow(clippy::struct_excessive_bools)]` justified as "each bool is an
independent in-flight command flag". They are not independent: at most one
command can arm one effect per exchange, and `apply_tagged` applies whichever
is armed to whatever tagged response arrives, without comparing tags.

Target type:

```rust
enum PendingEffect {
    Logout,
    Authenticate,
    Select(MailboxName),
    Close,
    Unauthenticate,
    Notify(NotifyFlags),
}
struct Armed { tag: Tag, effect: PendingEffect }
```

`ProtocolState` holds `Option<Armed>` (or a small map for the pipeline), and
`apply_tagged` applies the effect only when `t.tag == armed.tag`, and always
consumes it, on OK, NO and BAD alike. That makes the L1 bug (a refused LOGOUT
leaving `in_logout` armed) unrepresentable, folds `disarm_pending_effects`
into "drop the armed entry", and gives the pipeline the same mechanism
instead of its own pre-effect hook. The effect is derived from the command
by one function, see 2.2.

### 1.2 `enabled: Vec<String>` and the `(capabilities, enabled)` pair

`ProtocolState::enabled`, `ConnectionStateSnapshot::enabled`,
`EncodeOptions::enabled`, `ConsumerContext::enabled` and
`ServerProfile::enabled` are all `Vec<String>`. The question "is extension X
ENABLEd" is answered by `iter().any(|e| e.eq_ignore_ascii_case("X"))` at:

- `driver::utf8_mode` (UTF8=ACCEPT),
- `helpers::check_utf8_only_enforced` and `helpers::utf8_enabled`
  (UTF8=ACCEPT, twice),
- `extensions::enable_utf8` (UTF8=ACCEPT on the reply),
- `codec/encode/core.rs` (UTF8=ACCEPT),
- `uid_ops::uid_fetch_vanished` and `uid_ops::uid_fetch_vanished_stream`
  (QRESYNC, verbatim copies whose comment says "exactly as
  `ServerProfile::enabled` does" - a self-declared copy of a rule with an
  owner),
- `mailbox::validate_qresync_params` (QRESYNC, same comment),
- `account/changes.rs` (QRESYNC),
- `types::profile::imap4rev2_active` (IMAP4rev2),
- `ProtocolState::apply_untagged` (dedup on insert).

The pair itself is threaded as two slices into
`types::profile::supports(capabilities, enabled, cap)`, whose doc warns that
"nothing stops a caller pairing one view's capabilities with another view's
enabled list; the wrappers exist so that ordinary call sites cannot" - but
the only wrappers are `auth::snapshot_supports` and
`auth::is_rev2_from_snapshot` (snapshot-side), and the driver and encoder
call the slice functions directly.

Target: one `SessionCapabilities { advertised: Vec<Capability>, enabled:
EnabledSet }` where `EnabledSet` holds typed `Capability` values (ENABLE's
arguments are capability names), owned by `ProtocolState`, published in the
snapshot (as an `Arc`, it is cloned into the watch on every command today),
and embedded in `EncodeOptions` and `ConsumerContext`. Methods:
`supports(&Capability)`, `enabled(&Capability)`, `rev2()`, `utf8_mode()`,
`literal_mode()`, `binary_fetch_usable()`. `ServerProfile` becomes a
projection of it. The string scans and the slice-pair API go away, and
`Command::Enable { capabilities: Vec<String> }` takes typed capabilities.

### 1.3 SASL mechanism identity as a string

`Capability::Auth(String)`, `Command::Authenticate { mechanism: String }`,
`ServerProfile::auth_mechanisms: Vec<String>` (uppercased), and
`AuthMechanism` (the enum) coexist. "Is AUTH=X advertised" is answered by:

- `ServerProfile::supports_sasl_auth` - case-insensitive compare on the
  uppercased list;
- `authenticate_plain` / `authenticate_xoauth2` /
  `authenticate_oauthbearer` / `require_auth_mechanism` -
  `capabilities.contains(&Capability::Auth("PLAIN".into()))`, i.e.
  `Capability`'s `PartialEq`;
- `login()` - `Capability::Auth(m) if m.eq_ignore_ascii_case("PLAIN")`;
- `authenticate_best` and `password_mechanism_ladder` - three inline
  closures `profile.auth_mechanisms.iter().any(|m| m.eq_ignore_ascii_case(name))`.

The test `a_lowercase_auth_advertisement_satisfies_the_live_plain_gate`
exists precisely because the ladder's gate and the live gate are two
implementations that must agree - the brief's "two copies held in step by a
pairwise-agreement test". Target: `Capability::Auth(SaslMechanism)` where
`SaslMechanism` is a case-normalized newtype (or `AuthMechanism` plus an
`Other(String)` arm), one `SessionCapabilities::offers(AuthMechanism)`, and
`Command::Authenticate { mechanism: AuthMechanism }`. The mechanism-to-hash
mapping (`ScramSha256Plus -> Sha256` etc.) is written inline twice in
`authenticate_best` and once more in `password_mechanism_ladder`; it is a
method on the mechanism (`AuthMechanism::scram_hash() -> Option<(ScramHash,
ChannelBinding)>`), and SMTP has the same mapping as `scram_hash(mechanism)`.

### 1.4 UID versus sequence number

The types are there (`types/ids.rs`: `Uid`, `Seq`, `UidValidity`, `ModSeq`,
`UidSet`, `SeqSet`). The connection layer uses them only at the
ergonomics edge (`SyncFetchRequest::uids: UidSet`). Everywhere else:

- `Command::Fetch` and `Command::UidFetch` both take `SequenceSet`, so
  `uid_fetch(&SequenceSet, ..)` accepts a `SeqSet`'s inner set.
- `CommandKind` maps UID variants to their non-UID kind ("they produce
  identical solicited response sets"), so the UID-ness of a command is not
  available to consumers.
- `SearchResult { ids: Vec<u32>, mod_seq: Option<u64> }` holds sequence
  numbers for SEARCH and UIDs for UID SEARCH/UID SORT, distinguished only by
  which method the caller called. `SortConsumer` returns the same type.
- `EsearchResponse::all: Vec<UidRange>` holds sequence ranges when
  `uid: false`; `UntaggedResponse::Search { uids, .. }` is named `uids` for
  sequence numbers too; `IdleEvent::SearchUpdate`'s doc admits "the `uid`
  field may be inaccurate for legacy SEARCH".
- `SelectedMailbox::{uid_validity, uid_next}: Option<u32>`,
  `highest_mod_seq: Option<u64>`; `FetchResponse::{seq: u32, uid:
  Option<u32>, mod_seq: Option<u64>}`; `IdleEvent::{Exists, Expunge,
  Recent}(u32)`.
- `AppendConsumer::Output = Option<(u32, u32)>` and
  `MultiAppendConsumer::Output = Vec<(u32, u32)>` - a bare
  `(uid_validity, uid)` tuple of two `u32`s that swap silently. The doc
  comments have to say which is which.
- `uid_fetch_changed_since(.., mod_seq: u64)`, `uid_store(..,
  unchanged_since: Option<u64>)`.
- In `account/push.rs::absorb_idle_event`, `uidvalidity: Option<u32>`,
  `fetch.uid`, `fetch.mod_seq` are raw at the point they are written into
  the modseq cache.

Target: `Command::UidFetch { uids: UidSet, .. }` / `Command::Fetch { seqs:
SeqSet, .. }` (same for STORE/COPY/MOVE/EXPUNGE/SEARCH), consumers
parameterised by addressing (`SearchResult<Id>` with `Id = Uid | Seq`),
`AppendUid { validity: UidValidity, uid: Uid }`, and `ModSeq` in every
modseq position. `HIGHESTMODSEQ 0` normalization (see 1.5) becomes part of
`ModSeq` construction.

### 1.5 Modseq support as `Option<u64>` plus `bool`

`SelectedMailbox` carries `highest_mod_seq: Option<u64>` and
`no_mod_seq: bool`. They can contradict (`extract_selected_code` sets
`no_mod_seq = true` on `HIGHESTMODSEQ 0` or `[NOMODSEQ]` without clearing a
`highest_mod_seq` set by an earlier code in the same response set). The
question "does this mailbox carry modseqs" is then answered as
`highest_mod_seq.is_some() && !no_mod_seq` at seven account sites:
`mutate.rs`, `inventory.rs`, `get.rs`, `changes.rs` (twice, inverted form),
`mod.rs::cursor_from_select` (twice). Target:
`enum ModSeqSupport { Unknown, NoModSeq, Highest(ModSeq) }`, built once in
`build_selected_mailbox`, read through `SelectedMailbox::modseq() ->
Option<ModSeq>`.

### 1.6 QRESYNC parameters with an invalid combination representable

`QresyncParams` allows `seq_match_data: Some(..)` with `known_uids: None`;
`mailbox::validate_qresync_params` refuses it at runtime as
`InvalidInput`. The RFC 7162 ABNF nests one inside the other; the type
should too: `known: Option<(UidSet, Option<SeqMatchData>)>`.

### 1.7 Free-form strings where the grammar is closed

- **Search criteria.** `Command::Search { criteria: String }` and friends.
  The encoder decides capability requirements by re-lexing the string
  (`ImapConnection::search_criteria_contains_atom`, a hand-written SEARCH
  grammar walker in `search_validation.rs`, ~200 lines) to find `MODSEQ`,
  `OLDER`, `SAVEDBEFORE`, `EMAILID`, `$`. A `types::SearchCriteria` builder
  already exists and produces the string that is then re-parsed. A typed
  `SearchKey` tree on the command would answer `required_capabilities()`
  structurally and delete the scanner.
- **FETCH items.** Handle methods take `&[FetchAttr]` and validate them
  (`validate_requested_fetch_items`), then `format_fetch_attrs` flattens
  them to `items: String` in the `Command`. The `Pipeline` builder takes the
  `String` directly and so bypasses the validation (see 2.1). Keep
  `Vec<FetchAttr>` in the command.
- **STATUS items.** `status_items: String` / `items: &str`, tokenized and
  uppercased by `status_item_tokens` and matched as strings in
  `validate_requested_status_items`. A `StatusItemName` enum exists in
  spirit in `StatusItem`; the request side should be typed too.
- **LIST-EXTENDED options.** `selection_options: Vec<String>`,
  `return_options: Vec<String>`. "Is SUBSCRIBED selected" is a string scan
  in `mailbox::list_extended_names`, `pipeline::list_extended`, and
  `is_notify_selection_mismatch` (which also scans RECURSIVEMATCH, REMOTE,
  SPECIAL-USE); `validate_list_extended_request` scans SPECIAL-USE,
  RECURSIVEMATCH and REMOTE again. Typed `ListSelect` / `ListReturn` enums
  remove five independent spellings.
- **SEARCH RETURN options** (`return_opts: Vec<String>`,
  `search_return_requests_save` scans for `SAVE`).

### 1.8 Tags as `String`

`TagGenerator::next() -> String`; tags are compared as `&str` in six read
loops and keyed as `String` in `tag_to_idx`. A `Tag` newtype (Copy, two
`u32`s, formatted on write) would cost nothing, make "own tag" comparisons
typed, and be the natural key for `Armed` in 1.1.

### 1.9 Notify flags as a bare tuple

`extensions::compute_notify_flags(&NotifySetParams) -> (bool, bool, bool)`
returns `(list, status, metadata)`; both callers
(`driver::run_prepared_command`, `pipeline::apply_pipeline_pre_effects`)
immediately rebuild `NotifyFlags { list, status, metadata }` from it.
It should be `impl From<&NotifySetParams> for NotifyFlags`, living with
`NotifyFlags` (see 3.5 for where that should be).

### 1.10 Prose-only failures a caller should be able to branch on

- `Error::MissingCapability(String)` covers "server does not advertise X",
  "X advertised but not ENABLEd" (`"QRESYNC (not ENABLEd)"`), "removed in
  rev2" (`"STATUS item RECENT was removed in IMAP4rev2"`, CHECK on rev2),
  "LOGINDISABLED", and "literal exceeds the 4096-octet LITERAL- limit". The
  account layer maps all of them to `Unsupported(operation)` with the prose
  as a support-only diagnostic. A typed `MissingCapability { capability,
  reason: NotAdvertised | NotEnabled | RemovedInRev2 | ... }` lets the
  account distinguish "the server cannot" from "this session has not been
  put in the mode" (see L7 for where that already goes wrong).
- `Error::StateChangedBeforeSend(String)` and `Error::InvalidState(String)`
  carry the session state and the legal set only as `format!` text.
- `TypedEvent::QueueOverflow { dropped_count, since }` reaches IDLE callers
  as `IdleEvent::Alert(format!("event queue overflow: {dropped_count} events
  dropped"))` (L5).

### 1.11 Smaller tuple outputs

`NamespaceConsumer` stores a `(Vec<_>, Vec<_>, Vec<_>)` triple for
`(personal, other, shared)` instead of the `NamespaceResponse` it returns;
`IdConsumer` / `Command::Id` use `Vec<(String, Option<String>)>`;
`ListStatusConsumer::Output = Vec<(MailboxInfo, Vec<StatusItem>)>`;
`select_or_examine(.., is_examine: bool, condstore: bool, ..)` decomposes
the `SelectOptions` it was given.

---

## 2. Decisions made in more than one place

### 2.1 "May this command be sent now?" (admission)

Sites answering it:

1. **Handle methods**, against the watch snapshot: `require_state`,
   `require_condstore`, `require_searchres`, `check_utf8_only_enforced`,
   `require_notify`, `require_auth_mechanism`, the five hand-coded
   `state_refusal(.., &[SessionState::NotAuthenticated])` calls in
   `auth.rs` (a second copy of the `CommandKind::legal_states` row for
   LOGIN/AUTHENTICATE), the 17 `sequence_set.as_str().contains('$')` scans
   (`uid_ops.rs`, `seq_ops.rs`, `ergonomics.rs`), the three QRESYNC-ENABLEd
   scans, `validate_requested_fetch_items`, `validate_requested_status_items`,
   `validate_list_extended_request`, CHECK-on-rev2 in `helpers::check`,
   STARTTLS in `starttls_with_connector`, COMPRESS, UNAUTHENTICATE, ENABLE,
   ESEARCH.
2. **The driver**, against live state at the head of the queue:
   `live_state_refusal` (state table plus `refused_after_selection`) - this
   one is a deliberate re-check of the same table and is fine.
3. **The encoder**, against live state: `encode_command` gates STARTTLS,
   ENABLE, UNSELECT, CREATE-SPECIAL-USE, CONDSTORE on SELECT/EXAMINE/FETCH/
   STORE, QRESYNC (advertised) on SELECT/EXAMINE and VANISHED, MOVE,
   UIDPLUS for UID EXPUNGE, IDLE, NOTIFY, MULTIAPPEND/BINARY, and search
   atoms (via the connection-layer scanner).
4. **The `Pipeline` builder**: `execute_raw` calls `require_state` only.

They already disagree:

- **QRESYNC for VANISHED / SELECT QRESYNC**: the handle requires QRESYNC
  *ENABLEd*; the encoder requires QRESYNC *advertised*. Different questions,
  so a pipelined `uid_fetch(.., vanished: true)` passes the encoder on a
  server that advertises but has not enabled QRESYNC, which RFC 7162 forbids.
- **`$` in a sequence set**: only the handle checks it (encoder checks `$`
  only inside search criteria). The pipeline's FETCH/STORE/COPY/MOVE with
  `$` therefore reach the wire ungated.
- **FETCH item gates** (MODSEQ, PREVIEW, SAVEDATE, OBJECTID, X-GM-EXT-1,
  BINARY): handle-only, against a snapshot the encoder's own comment says
  "may predate a CAPABILITY/ENABLE update"; the pipeline's `items: String`
  bypasses them entirely.
- **STATUS item gates, LIST-EXTENDED option gates, CHECK on rev2, UTF8=ONLY**:
  handle-only; pipeline bypasses.
- **STARTTLS availability**: three rules, three outcomes - setup's
  `ensure_starttls_available` (`StartTlsUnavailable`), the handle's raw
  `Capability::StartTls` scan plus `tls_active` (`StartTlsUnavailable` /
  `InvalidInput`), the encoder (`MissingCapability`).
- **Capability spelling**: `snapshot_supports`'s doc says "EVERY handle gate
  ... goes through here", but `require_notify`, `login`'s LOGINDISABLED
  scan, the AUTH gates, the STARTTLS gate and the APPENDLIMIT gate use raw
  `capabilities.contains`/`iter().any`. Equivalent today only because none
  of those capabilities is in the rev2 baseline.

Owner: one `fn admit(command: &Command, session: &SessionView) ->
Result<(), Refusal>` (in `types/command.rs` beside `legal_states`, or a
`connection/admission.rs`), with `Refusal` carrying the typed reason from
1.10. The handle calls it against the snapshot for an early, cheap
`InvalidState`/`MissingCapability`; the driver calls it in
`prepare_command` against live state as the authority, mapping a refusal it
alone finds to `StateChangedBeforeSend`. The encoder becomes a pure encoder.
The pipeline builder gets every gate for free. This is the IMAP analogue of
`77df77df`: one decision, one owner.

### 2.2 "Which session state does this command move?"

Sites:

- `driver::run_prepared_command` - a chain of `matches!(cmd, ..)` arming
  `set_in_logout`, `set_in_auth`, `set_in_select`, `set_in_close`,
  `set_in_unauthenticate`, `set_in_notify_set` before the send.
- `pipeline::apply_pipeline_pre_effects` - NOTIFY SET/NONE again, armed at
  tagged-response arrival rather than before the send. Its doc says a
  second copy "is the drift shape this crate has paid for before"; it is the
  second copy.
- `CommandKind::pipelinable` - an independent list of "commands that move
  the session state the rest of the batch was encoded and admitted
  against", plus STARTTLS/COMPRESS/IDLE/ENABLE/APPEND for other reasons.
  NOTIFY moves state (notify flags) but is pipelinable, which is why the
  pipeline needs its own hook.
- `ProtocolState::apply_tagged` - the completion half, per flag, with
  per-flag clearing rules (L1).
- `wire_send::wait_for_continuation` (single) and the pipeline router's
  `early_own_ok` (pipeline) - two implementations of "an early OK before
  the `+` must not complete the armed effect".

They already disagree: a `* OK [NOTIFICATIONOVERFLOW]` arriving between a
NOTIFY SET's send and its tagged OK clears the pending registration on the
single-command path (armed before send, cleared by `apply_untagged`), but
not on the pipeline path (armed only when the tagged OK arrives, after the
overflow was processed), so the same server sequence leaves NOTIFY
registered in one path and unregistered in the other. Which one is right
depends on whether the overflow preceded the server's processing of NOTIFY
SET; the point is that the two copies answer differently and nothing
reconciles them.

Owner: `Command::pending_effect(&self) -> Option<PendingEffect>` in
`types/command.rs`, next to `legal_states` and `pipelinable`, with
`pipelinable` derived from it (`pending_effect().is_none_or(|e|
e.is_batch_safe())` plus the transport/continuation exclusions), and the
armed effect keyed by tag (1.1). Both the single and the pipeline path arm
through the one function at the one moment (before the first byte).

### 2.3 "Should this outcome retire the connection?"

Sites:

- `driver_task`'s `Run` arm: `(!refused_before_send && is_connection_fatal)
  || (is_err && session == Logout)`.
- `Upgrade` arm: `is_connection_fatal || (is_err && Logout)` (no
  `refused_before_send`; the upgrade's own refusal is inside `run_upgrade`).
- `Pipeline` arm: `failure.is_some()`, regardless of variant.
- `Idle` arm: `is_connection_fatal` only, no Logout clause.
- `upgrade::retire_after_failed_refresh` and the STARTTLS/COMPRESS paths
  that call `apply_infrastructure_failure` directly.
- `Error::is_connection_fatal` - a variant list that must be overridden by
  phase (`Closed` from `live_state_refusal` is "fatal" by variant yet
  framing-safe, which is why `refused_before_send` exists).
- The handle side: `abandoned` (set by `Pending::drop`, `InFlightGuard`),
  `is_reusable`, and the pool.

The variant does not decide fatality; the framing state does. The read
loops know it (they know whether the tagged completion was consumed). A
typed outcome from every loop - `enum Framing { Untouched, Synchronized,
Desynchronized }` returned alongside the result - lets one function in the
driver decide retirement and lets `is_connection_fatal` be deleted or
reduced to a default for errors raised outside a loop.

### 2.4 "Which SELECT/EXAMINE responses belong to the SELECT?"

Sites:

- `selected_mailbox_effective_responses` (mod.rs) - the `[CLOSED]` cut.
- `reclassify_select_responses` (dispatch/select.rs) - a second `rposition`
  of `[CLOSED]`, then `is_select_solicited_response` decides consumed versus
  event.
- `build_selected_mailbox` / `extract_selected_code` (mod.rs) - which
  responses and codes feed `SelectedMailbox`.
- `validate_select_responses` (dispatch/select.rs) - which count toward the
  mandatory set (FLAGS, EXISTS, RECENT or LIST).
- `codec::classification` for SELECT/EXAMINE (Either versus solicited).
- `account::selected_uidvalidity` - UIDVALIDITY's mandatoriness is judged in
  the account layer, the rest in the connection layer.

They disagree at the edges: `is_select_solicited_response` consumes every
`Status { code: Some(_) }` (so a `* NO [EXPUNGEISSUED]` or any unrecognized
coded status during SELECT is swallowed, never reaching the event stream),
while `build_selected_mailbox` uses only the select codes. Owner: one
`partition_select(responses, ctx) -> (SelectInputs, Vec<UntaggedResponse>)`
that cuts on `[CLOSED]` once, assigns each response to exactly one side, and
whose `SelectInputs` constructor is where mandatoriness (including
UIDVALIDITY) is judged. The helpers belong in `dispatch/select.rs`, not
`mod.rs` (3.1).

### 2.5 "What does this server event mean?" (event classification)

Chain: `UntaggedResponse -> TypedEvent` (`impl From` in `typed_event.rs`),
then `TypedEvent::priority()` (`typed_event.rs`), `TypedEvent::impact()`
(`types/events.rs` - a different module from the type), then
`typed_event_to_idle_event` / `untagged_to_idle_event` (`idle.rs`), then
`map_idle_event` / `absorb_idle_event` / `idle_event_ending`
(`account/push.rs`).

Disagreements today:

- RECENT: `impact()` says `EventImpact::None`; `map_idle_event` invalidates
  the folder scope.
- METADATA: `impact()` maps `TypedEvent::MetadataChange` to
  `SelectedMailboxChanged`, but that variant is never constructed (L9); the
  real METADATA arrives as `TypedEvent::Extension`, becomes
  `IdleEvent::MetadataChange`, and `map_idle_event` maps it to
  `HintPayload::Unknown`.
- Queue overflow: `impact()` says `EventQueueOverflow { dropped_count }`;
  the IDLE path turns it into `IdleEvent::Alert(String)` (L5).
- Priority and impact are two independent tables over the same variants:
  "Resyncable" in one is not derived from "requires resync" in the other.

`IdleEvent` duplicates `TypedEvent` variant for variant (with three extra:
Timeout, Cancelled, ServerTerminated) and adds its own `From`-like
conversion. Owner: one `ServerEvent` enum and one classification table
(`fn class(&self) -> EventClass { priority, impact, ends_idle_wait }`) in
one module; `IdleOutcome = Event(ServerEvent) | Timeout | Cancelled |
ServerTerminated`. The account's mapping then reads `impact`, not variants.

### 2.6 "Was this response's event already published?"

`driver/events.rs::has_critical_response_code` re-derives, from response
content, whether `emit_untagged_response_code_events` already published an
event for this response, so the reclassified-events loops (single dispatch
and pipeline router) can skip it. It is held in step with the emitter by
`the_predicate_agrees_with_the_emitter_except_on_bye`, a pairwise-agreement
test, plus a long TRIP-WIRE comment about a BYE exclusion. The comment's
premise is false (L2): BYE does reach `reclassified_as_events` today, on
LOGOUT. Owner: record the fact at emission. `process_untagged_prefix`
already returns `code_emitted`; carry it with the response
(`Routed { response, code_event_emitted: bool }`) into the consumer and back
out through `Finalized`, or keep a per-exchange set of emitted response
indices, and delete the predicate.

### 2.7 "Which tagged status maps to which error, and what does a foreign tag mean?"

The `OK/NO/BAD -> Ok/no_with_code/bad_with_code` match is written out in
`wire_send::wait_for_continuation`, `idle::run_committed_idle`,
`idle::wait_for_idle_grant`, `idle::drain_idle_responses`, `SelectConsumer`,
`NotifySetConsumer`, the fetch consumers and `dispatch/auth.rs`, beside the
owner `TaggedResponse::require_ok` that 24 consumers use. Harmless as code;
the decision that is actually duplicated is the foreign-tag policy, which the
six read loops answer differently: `dispatch_response_loop`,
`wait_for_continuation` and `wait_for_idle_grant` treat a foreign tag as
fatal `Protocol`; the IDLE loop and `drain_idle_responses` "ignore to
preserve the connection"; `logout_best_effort` ends the drain. Only one
command is ever outstanding in all six, so a foreign tag is the same fact
(desynchronization) in each. See 3.3 for the shared read-loop engine that
makes this one policy.

### 2.8 "What APPENDLIMIT applies?"

`ServerProfile::new` (public, `types/profile.rs`): on a contradictory
advertisement (bare and numeric forms together) reports
`AppendLimitPolicy::PerMailbox` and drops the numeric value ("preserve the
safer bare interpretation"). `append.rs::AdvertisedAppendLimits` plus
`append_limit_from_advertisement` (enforcement): applies
`min(global, mailbox)`. Same input, two answers; a consumer reading
`server_profile().append_limit` is told something different from what
`append` enforces. Owner: `AppendLimitPolicy` grows the shape enforcement
needs (`PerMailbox { global_ceiling: Option<u64> }`), and `append.rs` reads
it instead of re-folding capabilities. `validate_requested_status_items`'s
APPENDLIMIT gate is a third reader (`matches!(cap, AppendLimit(_))`).

### 2.9 "Which mailbox is selected on this connection?"

`ProtocolState::selected` is written by `apply_tagged` (OK sets it, NO
clears it, CLOSE/UNSELECT/UNAUTHENTICATE clear it) and never read: there is
no getter and it is not in `ConnectionStateSnapshot`. The account pool keeps
its own copy (`PooledConn::set_selected`, `set_selected_affinity`), and
`account::record_failed_select` reconstructs the driver's knowledge from
`session_state()` plus the error variant ("Selected and ProtocolMissing
means the tagged OK selected it"). Owner: the driver; publish
`selected: Option<MailboxName>` in the snapshot and have the pool read it.
The reconstruction, and the possibility of it drifting from
`apply_tagged`'s rules, goes away.

### 2.10 "Did the server ENABLE / advertise this?" - eager versus on-OK

`ProtocolState::apply_untagged` adopts `* ENABLED` and `* CAPABILITY` the
moment they arrive, before the tagged status is known. Two consumers state
the opposite policy as the reason for their shape:

- `EnableConsumer::finalize`: "an ENABLED list under a failed ENABLE must
  not reach anything that would treat those extensions as active" - the
  state machine already treats them as active.
- `CapabilityConsumer::finalize`: "capability data that arrived under a
  failed CAPABILITY must not be published as an event, because the
  dispatcher's capability cache would then adopt it" - the cache already
  adopted it on arrival.

One of the two is the policy; today the state machine wins silently and the
consumers' comments describe a guarantee nobody provides. Owner: the state
machine, with the policy stated there (and, if on-OK is wanted, the
`PendingEffect` mechanism from 1.1 can carry "adopt ENABLED on OK").

### 2.11 "How does a caller's departure get classified?" (submission)

`auth::submit_driver_command` is "the one submission body behind the four
helpers", with strict admission and the withdraw/commit race. There are
three more submission paths with their own rules:

- `Pipeline::execute_raw` - `cmd_tx.reserve().await` with no deadline; the
  only command-carrying call with no bound, contradicting the
  `ImapConnection` doc ("no infinite waits").
- `ImapConnection::idle` - its own `select!` over cancel, deadline and
  `reserve`.
- `set_keepalive` and `peer_certificate_der` - `cmd_tx.send(..).await` with
  no reply cell and no deadline (queued behind any command; the callers wrap
  them in `in_flight()` or `timeout_at`).

`idle` has a reason (the event wait is part of it), but its admission half
could be `submit_driver_command`'s. `execute_raw` and the two non-wire
requests have none.

### 2.12 SASL mechanism selection and binding policy (cross-crate)

- **Binding resolution**: `imap::connection::auth::scram_binding_from_der`
  and `smtp::authentication::resolve_scram_binding` are the same function
  (absent cert = skip, unusable cert = error). The IMAP doc says it "reads
  identically to `bifrost_smtp`'s" and "the two crates must not differ
  here". A comment is the only thing holding them in step.
- **Mechanism ladder**: IMAP `password_mechanism_ladder` (fixed order
  SCRAM-256-PLUS, SCRAM-1-PLUS, SCRAM-256, SCRAM-1, PLAIN, CRAM-MD5, LOGIN;
  `AuthPolicy` gates; cleartext gate; downgrade skip via
  `select_scram_rung`; falls through on live-snapshot skew) versus SMTP
  `password_mechanism` (order without CRAM-MD5; caller `allowed` list;
  single choice; no fall-through). The OAuth preference
  (OAUTHBEARER then XOAUTH2) is restated in IMAP `authenticate_best` with a
  comment "mirroring SMTP's `OAUTH2_MECHANISMS` order".
- **`SaslError` mapping**: IMAP's `impl From<SaslError> for Error` maps an
  unknown future variant to an auth failure; SMTP's maps it to
  `invalid_input`. `AuthFailed` is always auth in IMAP and conditional in
  SMTP. These already disagree.

Owner: bifrost-sasl, as `26aa7c25` did for OAuth payloads:
`resolve_tls_server_end_point(Option<&[u8]>)`, a
`MechanismLadder::build(credentials_kind, offers: impl Fn(&str) -> bool,
policy, tls_active) -> Vec<Candidate>` with the downgrade rule inside, and a
`SaslFailureClass` the protocol crates map once each. The protocol crates
keep only framing.

### 2.13 Smaller duplicated decisions

- `utf8_mode`: `driver::utf8_mode` and `codec/encode/core.rs` each compute
  "UTF8=ACCEPT enabled or rev2" (folded by 1.2).
- `is_notify_list_event(info, filter_extended_markers)` is called with
  `true` in `dispatch/select.rs` (twice) and with a value derived from a
  SUBSCRIBED string scan in `mailbox.rs` and `pipeline/mod.rs` (folded by
  1.7).
- `quota_resource_name` re-parses `Capability::Other("QUOTA=RES-...")` as a
  fallback beside `Capability::QuotaResource`, so "is this a quota resource
  capability" is decided by the codec's capability parser and again here.
- The `TaggedResponse -> Error` mapping for auth (`require_ok_auth`) versus
  generic (`require_ok`) is a legitimate refinement, not a finding.

---

## 3. Structure

### 3.1 `connection/mod.rs` does seven jobs

It holds: (a) protocol vocabulary (`TlsMode`, `TcpKeepalive`,
`SessionState`, `IdleEvent`, `SearchResult`, `NotifyFlags`); (b) the
`ImapConnection` handle, its `Drop`, and event draining; (c) driver-task
lifetime machinery (`DriverExit`, `ExitState`, `DriverScope`,
`LeaseHandoff`, `DriverTask`, `InFlightGuard`); (d) SELECT response assembly
(`build_selected_mailbox`, `extract_selected_code` with
`#[allow(clippy::too_many_arguments)]` and nine `&mut` out-params,
`selected_mailbox_effective_responses`); (e) NOTIFY LIST heuristics
(`is_notify_selection_mismatch`, `is_notify_list_event`); (f) ESEARCH
expansion (`expand_uid_ranges`); (g) STORE flag filtering. (d) to (g) are
consumer logic and belong under `dispatch/` beside their only callers
(`select.rs`, `list.rs`, `search.rs`); (c) belongs in `driver/`;
`SessionState` and `NotifyFlags` belong in `types/` (3.5).
`extract_selected_code` should be a builder (`SelectedMailboxBuilder::apply
(&ResponseCode)`) rather than nine out-params.

### 3.2 Twelve `impl ImapConnection` files, each restating its gates

`auth.rs`, `helpers.rs`, `mailbox.rs`, `seq_ops.rs`, `uid_ops.rs`,
`extensions.rs`, `sort_thread.rs`, `append.rs`, `idle.rs`, `lifecycle.rs`,
`ergonomics.rs`, `search_validation.rs` each `use super::*` and open
`impl ImapConnection`. The split is by command family, which is fine, but
every method is the same five steps: gate (2.1), build `Command`, pick a
consumer, compute a deadline, `submit_*`. The pairing of command and consumer
is decided twice - once here and once in `pipeline/mod.rs` (e.g. pipelined
`uid_fetch` always uses `FetchVanishedConsumer`, the handle picks among four
consumers). A request layer collapses both:

```rust
trait Request { type Consumer: Consumer; fn command(&self) -> Command; fn consumer(&self) -> Self::Consumer; }
```

with admission derived from `command()` by the 2.1 owner. Handle methods
become one-liners over `submit(request, timeout)`; the pipeline builder
pushes the same `Request` values; the "deadline inside the helper"
discipline is enforced once. `helpers.rs` additionally mixes snapshot
accessors, gates, STATUS/LIST option parsing, NOOP, CHECK and CAPABILITY -
it is the file of things that had no other home.

`search_validation.rs` is a SEARCH-grammar lexer hung off `ImapConnection`
as associated functions, used only by the codec encoder (and a
`#[cfg(test)]` adapter). It belongs in `codec/` or, better, disappears with
a typed `SearchKey` (1.7).

### 3.3 Six read loops where one engine with a policy would do

`dispatch_response_loop`, `route_pipeline_response` (two callers),
`wait_for_continuation` (two modes), `wait_for_idle_grant`, the IDLE loop in
`run_committed_idle`, `drain_idle_responses`, `logout_best_effort`. Each
re-implements: read with the right utf8 mode and budget, apply side effects,
run the untagged prologue, then a tagged/continuation/greeting policy. The
crate already extracted the untagged prologue (`process_untagged_prefix`,
`process_untagged_as_event`) after drift; the tagged half has not been, and
that is where they differ (2.7). A single `ReadLoop` driven by a
`LoopPolicy { on_own_tag, on_foreign_tag, on_continuation, route_untagged }`
would make the foreign-tag decision once, make literal budgets apply in
every loop (L4), and give 2.3's `Framing` result a single producer.
`logout_best_effort` also bypasses the "one sender" (`send_wire_command`'s
doc: "Every command ... reaches the socket through here") by writing
`format!("{tag} LOGOUT\r\n")` raw, and is the one tagged arm that does not
emit response-code events.

### 3.4 Connection setup belongs inside the driver

`lifecycle::connect_with_tls_connector_metered` runs the greeting, the
initial CAPABILITY and STARTTLS on stack-held `WireReader`, `ProtocolState`,
`TagGenerator` and `DriverEventSink`, then spawns the driver with them. That
produces a second execution context with:

- its own timeout model (`setup_timeout` maps expiry to
  `Error::Timeout { attempt: None }`, which reads as `Unsent` even after the
  STARTTLS line was written; L8);
- its own ALERT delivery (`events_tx.try_send(TypedEvent::Alert(..))` on the
  raw channel, because `DriverEventSink::emit` is
  `pub(in crate::connection::driver)` - a visibility accident shaping the
  design);
- an ungated flavour of every driver routine (`run_one_command`,
  `run_starttls_upgrade` with `tls_active: None`, `gate: None`), which is
  why `starttls_upgrade` takes `Option<Arc<AtomicBool>>` and
  `Option<&CommitGate>`;
- its own STARTTLS availability rule (2.1).

Moving setup into the driver task (spawn first, driver runs a `Setup` phase,
handle awaits a ready oneshot carrying the first snapshot or the setup
error) leaves one execution context, one evidence model, and removes the
Option parameters. `tls_active` is then written in one place (today the
driver writes it at the stream swap and `starttls_with_connector` writes it
again after the upgrade returns).

### 3.5 Dependency edges pointing the wrong way

`codec` and `types` depend on `connection`:

- `types/command.rs` imports `crate::connection::SessionState` for
  `legal_states`.
- `codec/classification.rs` imports `crate::connection::NotifyFlags`.
- `codec/encode/mod.rs` imports `crate::connection::literals` (a pure
  literal-marker scanner also used by `connection/wire.rs`).
- `codec/encode/dispatch.rs` calls
  `crate::connection::ImapConnection::search_criteria_contains_atom`.

`SessionState` and `NotifyFlags` are protocol vocabulary and belong in
`types/`; `literals.rs` belongs in `codec/`; the criteria scanner belongs in
`codec/` or goes away (1.7). After the move, `connection` depends on
`codec` and `types` and nothing depends back.

### 3.6 Write-only state

- `ProtocolState::notify_history: VecDeque<NotifyFlags>` - pushed on every
  untagged response, capped at 1024, cleared on UNAUTHENTICATE, never read.
  Its doc refers to a "D3 notify snapshot" design that the code no longer
  implements (the snapshot is taken as `notify_before` in each loop).
- `ProtocolState::selected` - written, never read (2.9).

Both are state split from any behaviour acting on it. Delete the first;
publish the second.

### 3.7 The pipeline: shape driven by a feature with no caller

`ImapConnection::pipeline()` has no caller in `account/` (nor anywhere
outside its own tests). Yet it shapes the driver: `PipelineRouting` with
`sending: Option<usize>` as an exclusive ownership bound,
`route_pipeline_response` called from inside the sender's continuation wait,
`correlated_esearch_owner`, `group_into_sub_batches` driven by
`CommandKind::untagged_routing_key` (a summary of the classifier kept in
step by a test that "checks every pair"), a second NOTIFY pre-effect copy
(2.2), and `SendOutcome::Rejected`/`ContinuationOutcome::Rejected` arms that
are unreachable outside it. `pipeline/mod.rs` itself carries an "audit
boundary: not line-audited" note. The builder also bypasses every handle
gate (2.1) and waits unbounded (2.11).

This is crate-private (the whole connection layer is; `lib.rs` exports only
`ImapConfig`), so it is not published API under the standing rule, but
removing it is still a product decision for the owner. The options are (a)
keep it and route it through the request/admission layer (3.2, 2.1) so it
stops being a gate bypass, or (b) remove it and simplify
`wait_for_continuation` to its single mode, drop `PipelineRouting`,
`untagged_routing_key`, and the router. Either is better than the current
state, where the most intricate code in the driver serves no production
path. Flagged for a ruling, not recommended unilaterally.

### 3.8 A library-grade surface nobody outside the crate can reach

The connection layer is written as a direct API: `pub fn` methods, a typed
`Pipeline` with tuple unfolding, `ergonomics.rs` whose every method says
"Direct API counterpart to Account ...", `IdleEvent` with exhaustive RFC
documentation, `drain_event_impacts`. None of it is reachable from outside
`bifrost-imap` (`ImapConnection` is `pub(crate)`), and `lib.rs` carries a
crate-wide `#![allow(dead_code)]` "because the dead surface is spread across
most of the connection module tree". The honest structure is one of two:
promote a deliberate direct-IMAP public API (then gates, typed IDs and
bounded waits matter for real external callers), or shape the layer as what
it is - the account's transport - and let the compiler find the dead
surface by removing the crate-wide allow from `connection/` (AGENTS.md
measured 105 warnings for the crate; a connection-only allow removal would
give the exact list). Owner decision; the current middle state pays the
complexity of a public API without its users.

### 3.9 Consumers each own an `Either` buffer they almost always surrender

Roughly 30 consumers carry `buffered: Vec<UntaggedResponse>` and surrender
it on every `finalize` arm; `Finalized::failure` takes the buffer as a
mandatory argument so the choice is visible. The design is right about the
failure mode (responses lost on `?`), but the decision "this response is not
mine" is made at `on_response` time in nearly every consumer and only
deferred to `finalize` by the shape. Letting `on_response` return
`Option<UntaggedResponse>` (declined) and having the driver hold declined
responses removes most buffers and the possibility of forgetting one; the
few consumers that must decide at finalize time (SELECT's `[CLOSED]` cut,
APPENDUID's index) keep their own. This also gives 2.6's "already emitted"
flag a natural home in the driver.

### 3.10 Test layout

`connection/tests.rs` is a 3,300-line catch-all beside per-file
`*_tests.rs`. Two inline modules are named after a review
(`lifecycle.rs::bandwidth_review_tests`, `driver/upgrade.rs::
bandwidth_review_tests`) rather than after a behaviour. The behaviours the
layer must pin - admission, pending effects, framing/retirement, event
classification - have no home of their own, because no module owns them
(that is the point of 2.1 to 2.5). Once they have owners, their tests move
with them.

---

## Lateral findings

**L1 - LIVE (latent in practice): a refused LOGOUT leaves `in_logout` armed
and the next successful command logs the session out.** In
`ProtocolState::apply_tagged`, `in_auth`, `in_close` and
`in_unauthenticate` each have an `else` branch clearing the flag on NO/BAD;
`in_logout` has only `if self.in_logout && t.status == Ok { .. }`. After
`LOGOUT -> NO` (a case `LogoutConsumer` explicitly handles, pinned by
`a_refused_logout_reports_the_refusal_not_a_missing_bye`), the flag stays
true; nothing resets it at the start of the next command (`run_prepared_
command` only sets it for LOGOUT). The next command's tagged OK - a NOOP,
say - moves the session to `Logout`, the driver loop breaks after answering
that NOOP `Ok`, and the connection silently dies. The existing test stops
after the refusal. A biting test: refused LOGOUT, then NOOP, then assert
`session_state() == Authenticated` and `is_alive()`. Root cause is 1.1's
shape; the one-line fix is an `else { self.in_logout = false }`.

**L2 - LIVE (comment): the BYE trip-wire in `has_critical_response_code`
is not dead code.** The comment says a BYE "can never reach a
`reclassified_as_events` list and this guard is dead code". On LOGOUT,
`dispatch_response_loop` routes the expected BYE to `LogoutConsumer`, which
buffers it and surrenders it from `finalize`, so it goes through the
reclassified loop, where the exclusion makes a `* BYE [ALERT] ..` publish
`TypedEvent::Bye` after the prologue already published `TypedEvent::Alert`.
That behaviour is arguably correct (two different events), but the comment
describes the guard as inert and tells a future editor it can be deleted
under conditions that already hold. See 2.6 for removing the predicate.

**L3 - LIVE: pipeline and single-command paths disagree on NOTIFY after an
interleaved NOTIFICATIONOVERFLOW.** See 2.2.

**L4 - LATENT: literal budgets are not enforced on the pipeline, IDLE or
continuation-wait paths.** Only `dispatch_response_loop` calls
`read_one_limited(utf8, consumer.literal_budget())`; every other loop calls
`read_one(utf8)`. No pipelined consumer has a budget today
(`FetchConsumer::new()`), so nothing breaks, but `ConsumerErased::
literal_budget` is part of the pipeline consumer contract and is silently
ignored there. Structural fix in 3.3.

**L5 - LIVE: the driver's own queue overflow reaches IDLE callers as a
server ALERT.** `typed_event_to_idle_event` maps
`TypedEvent::QueueOverflow { dropped_count, .. }` to
`IdleEvent::Alert(format!("event queue overflow: {dropped_count} events
dropped"))`. `IdleEvent::Alert` is documented as RFC 3501 Section 7.1 text
that "MUST be presented to the user"; a consumer honouring that shows the
user an internal back-pressure message as if the server said it, and cannot
branch on the drop count. `account/push.rs` maps it to an `Unknown`
invalidation, which is safe, but the information that it was a client-side
loss (a full resync trigger, per `impact()`) is gone.

**L6 - SMELL: SELECT swallows unrecognized coded statuses.** See 2.4.

**L7 - LIVE (classification): "advertised but not ENABLEd" has two error
answers.** `check_utf8_only_enforced` returns `InvalidState` ("A mode the
caller has not established, not a missing server capability: the same
request succeeds after the ENABLE"); the QRESYNC-not-ENABLEd gates return
`MissingCapability`, which `account/error.rs` maps to
`Unsupported(operation)`. Same situation, two recovery classes.

**L8 - SMELL: setup timeouts carry no transmission evidence.**
`lifecycle::setup_timeout` maps expiry to `Timeout { attempt: None }`
(reads as `Unsent`) even when the CAPABILITY or STARTTLS line was written.
Nothing non-idempotent runs at setup, so recovery is unaffected today; it is
a second evidence model (3.4).

**L9 - SMELL: dead and stale event variants.** `TypedEvent::MetadataChange
{}` and `TypedEvent::ServerMetadataChange {}` (empty braces) are never
constructed - METADATA arrives as `Extension` - yet `impact()` and
`typed_event_to_idle_event` both map them. `IdleEvent::Bye` is unreachable
on the live path (the prologue turns BYE into an error first), and
`account/push.rs` carries two defensive arms and a long comment for it.
`IdleTermination::ClientDone` arriving before DONE is mapped to
`ServerTerminated` "Shouldn't happen".

**L10 - SMELL: `notify_history` costs a push and a bounded pop on every
untagged response for a value nobody reads.** See 3.6.

**L11 - SMELL: snapshot publication clones both capability vectors after
every command** (`send_replace(state.snapshot())`), and every handle gate
re-borrows the watch, often several times per method
(`validate_list_extended_request` borrows four times). An
`Arc<SessionView>` snapshot (1.2) makes publication a pointer swap and lets
a method take one coherent view for all its gates - which also closes the
small window where two borrows in one method see different snapshots.

**L12 - SMELL: inconsistent eager versus deferred gating on streaming
FETCH.** `uid_fetch_changed_since_stream` checks `require_state` eagerly;
`uid_fetch_stream_budgeted` checks it inside the returned future, so the
same misuse fails at a different point depending on the method. Folded by
2.1.

**L13 - SMELL: APPEND preflight can spend up to twice the caller's
timeout.** `append_message` computes the APPEND deadline, then runs the
APPENDLIMIT STATUS preflight with a fresh `timeout`, then submits APPEND
against the already-partly-spent deadline. The bound is real but is not the
one documented.

**L14 - SMELL: `starttls_with_connector` refuses an encrypted connection
with `InvalidInput`.** It is a session-state refusal, and the other
STARTTLS refusals use `StartTlsUnavailable` (2.1).
