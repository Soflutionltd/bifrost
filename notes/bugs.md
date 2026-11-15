# Bugs - design hunt, November 2026

Defects the design-hunt hunters turned up on the way. They were not what the
hunt asked for; they are the findings a resolution loop can land first. Each
entry is the hunters' account, unverified; the fix pass verifies and may find
an entry wrong.

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

Each entry names the hunt scope(s) that reported it. The raw reports are in the
commit that precedes this file's.

## BUG-001 - JMAP and CalDAV disagree on whether an event overlaps a range window

**Reported by:** types (LIVE), jmap-account (live, P1 by its estimate).

The jmap-account hunter confirms the consequence: JMAP's `CalendarEvent/query`
does not request `expandRecurrences`, so the server returns the master and the
local filter then discards it - `events_in_range` silently omits recurring events
whose first occurrence precedes the window. It also notes shared
`EventRecurrence` / attendee validation is decided per provider
(`validate_shared_recurrence` in JMAP, `validate_recurrence` in Graph), and RRULE
parsing/printing lives in JMAP's `calendar_ops.rs`.

Question: does a `CalendarEvent` fall inside an `EventRange`? Both
`caldav/src/account.rs::event_in_range` and
`jmap/src/sync/calendar_ops.rs::event_in_range` apply a local guard to results
the server already filtered, with near-copy helpers (`time_interval`,
`comparable_time`). They disagree:

| Case | CalDAV | JMAP |
|---|---|---|
| all-day event | always kept (floating DATE has no instant; the server had the tz) | compared as UTC midnight |
| zero-length event on the window start | kept (RFC 4791 s9.9, the earlier dav ruling) | dropped (`event_end > range_start` is false) |
| recurring master starting before the window | kept unless UNTIL ends before it | dropped |
| zoneless local date-time | unparsed, so kept | parsed as UTC |

JMAP plausibly drops events a JMAP server's `after`/`before` filter correctly
returned (JMAP Calendars matches on any occurrence). Google and Graph answer the
same question a third and fourth way, by delegating to the server and
normalizing bounds (`google_range_bound`, Graph `events_in_range`).

Root cause is the untyped `EventTime` (see STR-003). Proposed owner:
`bifrost-types`, `EventRange::admits(&CalendarEvent) -> bool` carrying the RFC
4791 rules, used by both DAV and JMAP. Both crates carry same-named tests
(`range_filter_includes_all_day_on_window_start`) that would move with it.

## BUG-002 - Graph `open_blob_range` trusts caller-forgeable handle capability flags

**Reported by:** types.

`AccountCapabilities::blob_range` and `blob_digest_pre_download` restate at
account level what `BlobHandle::capabilities` says per handle. The handle is a
public, caller-constructible struct, and Graph's `open_blob_range` trusts
`handle.capabilities.supports_range` to decide whether to send a ranged
request, so a forged or stale handle steers the provider. Gmail and JMAP refuse
regardless. Proposal: providers re-derive handle capability from the `BlobId`
they minted rather than reading it off the caller's handle.

## BUG-006 - A freshly minted cursor with a bad envelope gets three classifications

**Reported by:** sync-engine (P2/P3, sites already disagree).

When an `Account` hands the engine a brand-new `ChangeCursor` whose envelope
fails `validate_envelope()`:

| Site | Classification |
|---|---|
| `multiplexer/changes.rs::drive_changes_stream` | `fresh_cursor_contract_error`: `Internal(AccountContract)`, terminal, scoped |
| `multiplexer/mod.rs::fuse_inventory_done` | same |
| `engine/reattach.rs::run_establish`, `Ready` arm | `cursor_decode_failure`: `SyncState(SchemaIncompatible)`, deriving the persisted-schema directive |
| `engine/attach.rs::persist_cursor` | bare `Error::SchemaIncompatible`, failing the whole attach |

`reference/sync.md` says fresh account-authored envelopes are contract failures
and schema recovery is reserved for persisted envelopes; two sites contradict
it. Owner: one function taking the cursor's origin,
`classify_envelope(cursor, Origin::Fresh | Origin::Stored)`, called by every
site. The two establishment paths that host two of these sites are CON-017.

## BUG-007 - Mutation campaigns trust the account's item ids with no exactly-once check

**Reported by:** sync-engine (P3).

`run_bulk_pipeline` inserts every `ItemOutcome` it receives into `outcomes`
keyed by `ObjectId(item.0)`, whether or not that id was submitted this round or
exists in the campaign at all. An outcome for an unrequested id becomes a new
counted entry; a duplicate is last-write-wins. `BatchOutcomeBuilder::finalize`
enforces exactly-once only for providers that use it; the streaming bulk path is
a `SyncEvent<ItemOutcome>` stream the engine does not validate, unlike inventory
batches (`validate_boundary`, terminating as `AccountContract`). Mutation
streams deserve the same guard. Structural fix: STR-005.

## BUG-009 - The orphan-teardown retrier and `repair_debt` skip most of the wire-work gate

**Reported by:** sync-engine (P3).

`engine/orphan_teardown.rs` peeks the raw boundary and skips on `Pause` or
`Stop`, but not `CheckpointNow`; it takes no `begin_activity` (so a pause can
report quiescence while a `push_unsubscribe` is in flight), no scheduler
admission, does not consult the throttle bucket, and records no hold when a
teardown fails. Its comment says "a paused account makes no provider calls on
the engine's own initiative", which the code does not keep. `repair_debt`
likewise does wire work with no activity guard, admission or throttle wait. The
gate itself is CON-014.

## BUG-015 - Proof generations restart at zero every attachment while ledger generations are durable

**Reported by:** sync-lanes (live, by reading).

`PendingCoverage::next_generation` is an `AtomicU64` starting at `0` in
`PendingCoverage::default`, and a fresh `PendingCoverage` is built per
attachment (`engine/attach.rs`). But every `LedgerEntry::generation`,
`BarrierIncident::generation` and retained `proved` pair is written to the
durable ledger (`cursor/ledger_envelope.rs` encodes all three) and reloaded on
the next attach. Every discharge rule in `cursor/ledger.rs` compares a new
generation against the stored one: `record_proof` discharges only through
`proof_union_covers(&self.proved, entry.generation, ..)`, which needs
`proof_generation >= entry.generation`; the same call drops a barrier only when
the proof generation is at least `barrier.generation`; the `proved.retain` pass
then drops the new proof at once, older than every entry it could help.

After a restart, a clean covering walk cannot discharge debt (or clear a
barrier) raised in an earlier session at a higher generation until the new
counter climbs past it, moving once per walk or repair attempt. Meanwhile
`completion_permitted` stays false and the marker is withheld: a full re-walk on
every attach for as long as the gap lasts, possibly many sessions. `upsert`
overwrites `existing.generation = generation` on rediscovery, which can LOWER an
entry's generation, so the durable ordering is not even monotonic.

Root cause: the counter ordering durable evidence is minted by an in-memory
object with attachment lifetime. Fix: a `ProofGeneration` minted by the
account's writer (which owns the durable ledger) from `max(generation)` in the
loaded ledger; `PendingCoverage` mints nothing persisted. Also, `repair.rs`
`plan_requests` mints `RepairAttemptId(coverage.next_generation())`, so repair
attempt ids share the sequence (also STR-025).

## BUG-016 - The backfill completion marker wins resume selection by a magic number, and can lose

**Reported by:** sync-lanes (latent; live once any provider adopts partitioning).

The durable "backfill is done" fact is a `BackfillCheckpoint` on partition key
`b"complete"` with `items_done = total_seen + 1`
(`engine/backfill.rs::emit_backfill_complete`), whose doc claims every
per-partition row records `<= total_seen`. The store selects the resume row by
greatest `items_done` (`CheckpointStore::get_backfill` contract,
`InMemoryCheckpointStore` via `backfill_checkpoint_is_later`). The guarantee
only covers rows from the same walk, and rows survive across walks:

- `items_done` is per partition and per walk (`run_partition` resets
  `seen_total`); `total_seen` counts only the current walk. In a multi-partition
  `Fixed` plan, if the scope shrank and a window now yields no page, its old row
  keeps its old count; if that exceeds the new total, the stale row beats the
  marker, `backfill_complete_recorded` reads false, and the scope is re-walked
  on every attach indefinitely.
- In an `OpenPages` walk resumed at `T`, `total_seen` covers only the resumed
  part; a final empty `Exhausted` probe stamps `items_done = 1`, and the earlier
  `page:F:T` row with up to `chunk` items beats it. The scope re-probes on every
  attach.
- `TimeWindowed` keys embed absolute instants from `Timestamp::now()` at plan
  time (`partition_key` renders `time:{from}:{to}`; `backfill_plan_for` calls
  `plan(&policy, Timestamp::now(), 0)`), so every attach writes fresh keys that
  never overwrite the last set: rows accumulate without bound in the consumer
  store, each competing with the marker. The partitioner's "keeping partition
  identity stable across runs" holds only for the `Timestamp::MIN` sentinel.

Every in-tree provider reports `InventoryPartitioning::Full` today (one key each
walk overwrites), so all three are latent.

The same fact has three encodings: the `b"complete"` key (read by
`backfill_complete_recorded`, by `persist_ack_request`, and by more than twenty
literal `checkpoint.partition.0 == b"complete"` comparisons in `tests/`; a
consumer has no typed way to ask), the `items_done` magic, and
`PublicationReceipt::walk_watermark.is_some()` (set only by
`register_walk_marker`, read in `completion_refusal`, which cross-checks the
second against the first). Partition keys are stringly too: `partition_key`
formats `full | time:F:T | uid:F:T | page:F:T` with a catch-all `"unknown"` arm,
so every future `InventoryPartition` variant collides on one key, and
`parse_page_partition` parses it back. And "which row is the resume position?"
is engine policy implemented in every consumer store: the `get_backfill`
contract (greatest `items_done`, then greatest parsed page upper bound, then
greatest raw bytes) forces each backend to parse the engine's `page:F:T` format
(`InMemoryCheckpointStore` borrows `partitioner::parse_page_partition`; an
external backend must copy it), and `open_pages_resume` re-parses the result.

Proposal: a typed durable key, `enum BackfillKey { Partition(PartitionKey),
Complete }`, `PartitionKey` an enum carrying the bounds with its own codec in
`cursor/envelope.rs`, the completion row its own row kind; `CheckpointStore`
gains `get_backfill_resume -> BackfillResume { None, Complete,
At(PartitionKey) }`, or the store returns rows and the engine selects. Trait
change, needs the owner. `BackfillProgress::items_done` goes back to meaning a
count.

## BUG-018 - The ledger and cursor codecs panic on a `bifrost-types` variant they have not learned

**Reported by:** sync-lanes (latent).

`cursor/ledger_envelope.rs::encode_operation` looks the operation up in the
positional table `OPERATIONS` and panics if it is missing. The table matches
`bifrost-types` today, but `AccountOperation` is `#[non_exhaustive]` in another
crate, so nothing at compile time ties them: the first new operation reaching a
persisted `last_error` or barrier `evidence` panics the account writer.
`cursor/envelope.rs` has the same shape for `ObjectType` and `ProtocolKind`
("encoding a variant the codec does not know panics"), and that codec runs
inside consumer stores. The structural cause is STR-045.

## BUG-024 - A server reporting UIDVALIDITY 0 makes every inventoried IMAP object unhydratable

**Reported by:** imap-protocol (latent; "is a zero UIDVALIDITY / MODSEQ / UIDNEXT usable" has no owner).

The decoder accepts 0 for UIDNEXT, UIDVALIDITY, UNSEEN, HIGHESTMODSEQ, MODSEQ,
APPENDUID and COPYUID uidvalidity ("Postel, Dovecot sends 0 for empty
mailboxes"). Then: connection SELECT folding stores `UidValidity(0)` as
`Some(0)` although the `SelectedMailbox::uid_validity` doc says `None` exists to
avoid the invalid sentinel 0 (HIGHESTMODSEQ 0 there is treated as NOMODSEQ);
`account::selected_uidvalidity` ("the one place the classification lives")
checks presence only and returns `Ok(0)`; `poll::observation` additionally
rejects 0; `folder_registry::record_modseq` silently skips uidvalidity 0 and
modseq 0; `select_options` drops QRESYNC when `UidValidity::new(cursor value)`
is `None`; the encoder rejects uidvalidity 0 and modseq 0 in QRESYNC as
`InvalidInput` (a caller-blaming class); `decode_object_id` rejects 0 via
`parse_nonzero_u32`.

Consequence: inventory, changes and hydration mint object ids
`imap2:..:0:<uid>` through `encode_object_id`, and `decode_object_id` rejects
every one, so nothing inventoried can be hydrated or mutated; the folder cursor
is written with uidvalidity 0, so every reconnect silently drops QRESYNC and
full-resyncs. Owner: the decoder (or the SELECT consumer) turns the wire number
into `Option<UidValidity>` / `Option<ModSeq>` once, stating the "0 means absent"
tolerance there; every downstream field is the typed value.

## BUG-029 - IMAP command admission has three front doors and two clocks, and they disagree

**Reported by:** imap-protocol (live), imap-connection (live; its top-ranked finding).

"Which capability (and ENABLE state) does this command need" is answered in the
encoder's per-arm `missing(..)` gates (`encode/dispatch.rs`) and again,
separately written, in the connection handle (`extensions.rs`
ACL/METADATA/QUOTA/NAMESPACE/ID/ENABLE/COMPRESS, `mailbox.rs`,
`helpers::require_condstore` / `require_searchres`, `uid_ops`, `sort_thread`,
`auth`) - not one answer re-checked at two chokepoints, but the predicate
restated, and it differs:

- SELECT/EXAMINE with QRESYNC parameters: the handle requires QRESYNC to be
  ENABLEd (`validate_qresync_params`, RFC 7162 section 3.2.3 MUST); the encoder,
  documented as the live authority at the head of the queue, only checks
  `has_capability(QResync)`, i.e. advertised.
- UID FETCH ... VANISHED: handle requires ENABLEd (`uid_ops`), encoder checks
  advertised. UID FETCH is pipelinable, so a pipelined CHANGEDSINCE+VANISHED
  fetch is gated only by the weaker encoder check.
- AUTHENTICATE PLAIN: `connection/auth.rs` tests
  `capabilities.contains(&Capability::Auth("PLAIN"))` directly, not through
  `supports`.

Owner: `Command::requirements() -> Requirements { capabilities, enabled,
states, refused_after_selection }` beside `CommandKind::legal_states`, read by
both handle and encoder against one `ConnectionCaps` (STR-055); search criteria
contribute through `SearchKey::required_capabilities()` (STR-053).

The imap-connection hunter mapped all four doors. (1) Handle methods, against
the watch snapshot: `require_state`, `require_condstore`, `require_searchres`,
`check_utf8_only_enforced`, `require_notify`, `require_auth_mechanism`, five
hand-coded `state_refusal(.., &[SessionState::NotAuthenticated])` calls in
`auth.rs` (a second copy of the `CommandKind::legal_states` row for
LOGIN/AUTHENTICATE), 17 `sequence_set.as_str().contains('$')` scans
(`uid_ops.rs`, `seq_ops.rs`, `ergonomics.rs`), three QRESYNC-ENABLEd scans,
`validate_requested_fetch_items`, `validate_requested_status_items`,
`validate_list_extended_request`, CHECK-on-rev2 in `helpers::check`, STARTTLS,
COMPRESS, UNAUTHENTICATE, ENABLE, ESEARCH. (2) The driver's
`live_state_refusal` (state table plus `refused_after_selection`) against live
state - a deliberate re-check, fine. (3) The encoder's gates, against live
state. (4) The `Pipeline` builder: `execute_raw` calls `require_state` only.
Further disagreements it found:

- `$` in a sequence set: only the handle checks it (the encoder checks `$` only
  inside search criteria), so the pipeline's FETCH/STORE/COPY/MOVE with `$`
  reach the wire ungated.
- FETCH item gates (MODSEQ, PREVIEW, SAVEDATE, OBJECTID, X-GM-EXT-1, BINARY):
  handle-only, against a snapshot the encoder's own comment says "may predate a
  CAPABILITY/ENABLE update"; the pipeline's `items: String` bypasses them.
  STATUS item gates, LIST-EXTENDED option gates, CHECK on rev2 and UTF8=ONLY are
  handle-only and bypassed the same way.
- STARTTLS availability: three rules, three outcomes - setup's
  `ensure_starttls_available` (`StartTlsUnavailable`), the handle's raw
  `Capability::StartTls` scan plus `tls_active` (`StartTlsUnavailable` /
  `InvalidInput` - `starttls_with_connector` refuses an encrypted connection
  with `InvalidInput`, a session-state refusal), the encoder
  (`MissingCapability`).
- Capability spelling: `snapshot_supports`'s doc says "EVERY handle gate ...
  goes through here", but `require_notify`, `login`'s LOGINDISABLED scan, the
  AUTH gates, the STARTTLS gate and the APPENDLIMIT gate use raw
  `capabilities.contains` / `iter().any` - equivalent today only because none of
  those is in the rev2 baseline.
- "Advertised but not ENABLEd" has two error answers:
  `check_utf8_only_enforced` returns `InvalidState` ("the same request succeeds
  after the ENABLE"); the QRESYNC-not-ENABLEd gates return `MissingCapability`,
  which `account/error.rs` maps to `Unsupported(operation)`. Same situation, two
  recovery classes.
- Eager vs deferred: `uid_fetch_changed_since_stream` checks `require_state`
  eagerly; `uid_fetch_stream_budgeted` checks it inside the returned future.

Its owner: one `fn admit(command: &Command, session: &SessionView) -> Result<(),
Refusal>` (in `types/command.rs` beside `legal_states`, or
`connection/admission.rs`), `Refusal` carrying a typed reason (STR-057). The
handle calls it against the snapshot for an early refusal; the driver calls it
in `prepare_command` against live state as the authority, mapping a refusal it
alone finds to `StateChangedBeforeSend`; the encoder becomes a pure encoder; the
pipeline builder gets every gate for free. About 60 inline gate blocks go - "the
IMAP analogue of the status-line consolidation".

## BUG-030 - SASL vocabulary, ladders and policy are split across IMAP, ManageSieve and SMTP

**Reported by:** imap-protocol, imap-connection, imap-account, smtp-sasl.

The payload defects are fixed: bifrost-sasl `plain_payload` (NUL-refusing,
`Secret`) now serves IMAP and ManageSieve PLAIN, and ManageSieve XOAUTH2 goes
through `xoauth2_payload`. What remains is the consolidation:

- SMTP still builds its own PLAIN payload in
  `Mechanism::response_with_token_at_index` rather than `plain_payload`.
- Mechanism vocabulary is three types: `bifrost-imap::AuthMechanism` (`Login`
  meaning the IMAP LOGIN command, SCRAM names hard-coded), public
  `bifrost-smtp::Mechanism` (`Login` meaning SASL LOGIN, no CRAM-MD5), and the
  SCRAM token in bifrost-sasl. IMAP's edge is stringly (`Capability::Auth(String)`,
  `Command::Authenticate { mechanism: String }`,
  `ServerProfile::auth_mechanisms: Vec<String>`), and "is AUTH=X advertised" is
  answered at several sites with different case rules.
- Ladders: IMAP `password_mechanism_ladder` (fixed order with CRAM-MD5 and LOGIN,
  `AuthPolicy`, downgrade skip, fall-through) vs SMTP `password_mechanism`
  (caller allow-list, single choice); the SCRAM hash mapping is written inline
  three times in IMAP. SMTP has `PASSWORD_MECHANISMS`, `MECHANISM_PREFERENCE`
  and `DEFAULT_MECHANISMS`.
- Cleartext gate has three answers: IMAP per mechanism (SCRAM allowed over
  plaintext), SMTP refuses all AUTH unencrypted unless
  `dangerous_allow_insecure_auth`, ManageSieve gates everything and
  re-implements the `AuthPolicy` gate. ManageSieve supports only PLAIN and
  XOAUTH2 (no SCRAM, no OAUTHBEARER).
- Channel-binding resolution is two comment-held copies
  (`imap::connection::auth::scram_binding_from_der`,
  `smtp::authentication::resolve_scram_binding`).
- `SaslError` mapping differs: IMAP maps an unknown variant to an auth failure,
  SMTP to `invalid_input`.
- `types/secret.rs::SecretString` is a near-copy of `bifrost_sasl::Secret` with a
  hand-rolled constant-time `eq`; crate-private, so deletable without a ruling.

Owner: bifrost-sasl gains the mechanism enum, the OAuth preference order, one
`negotiate(advertised, policy, encrypted, binding)` ladder and one binding
resolver; IMAP, Sieve and SMTP select from it (owner ruling on what SMTP's public
`Mechanism` becomes). The SASL client layer this lives in is STR-135; where
ManageSieve should live is STR-060.

## BUG-032 - IMAP FETCH merge collapses partial fetches of one section at different origins

**Reported by:** imap-protocol (latent).

`BodySection { section: String, origin }` and `FetchAttr::BodySection {
section: Option<String> }`. `account/get.rs::merge_fetch_response` matches body
sections case-insensitively by name and IGNORES `origin`, so two partial fetches
of the same section at different origins in one response collapse (the second
is dropped); binary sections compare `Vec<u32>` exactly. Proposal: a typed
`SectionSpec { parts: Vec<u32>, text: Option<Header | HeaderFields(..) | Text |
Mime> }`, with origin part of the key that owns "same section".

## BUG-039 - IMAP SELECT swallows unrecognized coded statuses instead of publishing them

**Reported by:** imap-connection.

"Which SELECT/EXAMINE responses belong to the SELECT?" is answered by
`selected_mailbox_effective_responses` (`mod.rs`, the `[CLOSED]` cut),
`reclassify_select_responses` (`dispatch/select.rs`, a second `rposition` of
`[CLOSED]`, then `is_select_solicited_response` decides consumed vs event),
`build_selected_mailbox` / `extract_selected_code` (which responses and codes
feed `SelectedMailbox`), `validate_select_responses` (the mandatory set: FLAGS,
EXISTS, RECENT or LIST), `codec::classification` for SELECT/EXAMINE, and
`account::selected_uidvalidity` (UIDVALIDITY's mandatoriness judged in the
account layer). They disagree at the edges: `is_select_solicited_response`
consumes every `Status { code: Some(_) }`, so a `* NO [EXPUNGEISSUED]` or any
unrecognized coded status during SELECT is swallowed and never reaches the event
stream, while `build_selected_mailbox` uses only the select codes. Owner: one
`partition_select(responses, ctx) -> (SelectInputs, Vec<UntaggedResponse>)` that
cuts on `[CLOSED]` once, assigns each response to exactly one side, and whose
`SelectInputs` constructor judges mandatoriness (UIDVALIDITY included); the
helpers belong in `dispatch/select.rs`, not `mod.rs` (STR-068).

## BUG-042 - IMAP `native_code` is `"OTHER"` for every unknown response code

**Reported by:** imap-account (live), imap-protocol.

The wire name of each response code is written in three tables: the codec
decoder (`codec/decode/flags_caps.rs`, string -> variant),
`account/error.rs::response_code_name` (variant -> string), and
`bifrost_types::ImapResponseCode::code` (private), reached through a 60-arm
`imap_response_code` mirror of `crate::types::ResponseCode`. `classify_status`
stamps `native_code` from `response_code_name`, which returns the literal
`"OTHER"` for every unknown code, while `ImapResponseCode::Unknown { code }`
(built by `imap_response_code` for the same error) returns the server's real
name. So `native_code` loses exactly the information an operator needs for an
unrecognized code. Owner: make `ImapResponseCode::code` public and derive
`native_code` from the already-built `imap_response_code(code)`, deleting the
middle table; the IMAP crate should own the conversion as an `impl From` beside
`ResponseCode` in the codec, with the name table in exactly one place (see
STR-013 on provider vocabularies in the neutral crate).

## BUG-045 - IMAP drops shared-namespace discovery failures and reports a degraded open twice

**Reported by:** imap-account, net-dav (adds that IMAP's `factory.rs` comment says both DAV crates sync exactly one collection and report the rest on `skipped_scopes`, while both DAV factories return an empty skip lane and `caldav::lib` says every collection is a cursor scope; `DavAttach::Degraded.transient` is computed and carried under `allow(dead_code)`).

`types.md` defines `OpenedAccount::skipped_scopes`, `ContainerList` and
`Page::skipped_scopes` for exactly this. IMAP: `discover_shared_folders` logs a
failed `LIST` under a namespace prefix, and a failed `NAMESPACE`, with
`tracing::debug!` only - no `SkippedScope` - and `containers_list` then returns
`ContainerList::complete(..)` with a comment saying the skips "were handled" at
open; they were dropped. `DavScopeIndex::build` collisions (a collection that
will never sync) go only to the prose warning lane, not to `skipped_scopes`. A
failed DAV open is reported twice: a prose `Warning` via a bespoke `dav_degraded:
Mutex<Vec<Warning>>` with peek / acknowledge-on-`Done` logic in
`discover_cursor_scopes`, and a `SkippedScope`; the bespoke lane predates the skip
lane and duplicates it, and `DavAttach::Degraded { transient }` is a dead field.
`classify_dav_open` encodes "transient vs needs config" as the choice between
`WarningKind::Other` and `OperatorAttentionNeeded` while discarding the
classified `AccountError` that decided it. The search half (one folder's failure
fails the whole page) is already an owner-ruling item in `notes/todo.md`.

## BUG-046 - Revoked shares on IMAP PIM, search and hydration paths escalate to account-wide loss; every quarantine helper drops the original evidence

**Reported by:** imap-account (live), graph.

The graph hunter reports the same four copies and adds two facts. All four
translate the error, test `kind == Authorization(PermissionDenied)`, and replace
the result with a freshly built `SyncState(ScopeRevoked)` - discarding the
original's status, request id, trace id, attempt cause and wire signal, so a
quarantined share exports no request id to support. And a fifth, different answer
sits in Graph's `scopes::discover_cursor_scopes_inner`: a foreign mailbox's
`PermissionDenied` during discovery becomes a skipped scope plus a
`Warning(OperatorAttentionNeeded)`, while every other error fails the whole
discovery, a primary-mailbox outage included. Its owner:
`AccountError::revoke_foreign_scope(self, scope, owner) -> AccountError` in
`bifrost-types`, keeping the original chain as the revoked error's cause.

Within IMAP, `error::shared_folder_error` is applied by each caller after it
looks up `folders.get(&folder).shared_owner` itself: `inventory::run_inventory`,
`get::run_folder_get`, `blob::run_fetch`, `mutate::flush_mutation_groups`,
`changes::select_error`, `poll::examine`. It is NOT applied by
`pim::copy_messages`, `delete_messages`, `set_flag`, `hydrate_decoded` (so
`set_is_read`, `set_keyword`, `set_importance`, `add/remove_from_container`,
`draft_discard`, `draft_send`'s discard, `message_hydrate`, `thread_hydrate`,
`move_thread`, `delete_thread`), `search`, `search_messages`,
`fetch_full_message`, or the push session's SELECT. On those paths a revoked
share's `NO [NOPERM]` derives terminal account-wide `NoPermission` - the
escalation the helper exists to prevent.

Across the workspace the same decision has four copies:
`imap::error::shared_folder_error`, `jmap::sync::error::shared_scope_error`,
`graph::graph_error::graph_shared_scope_error` and `ews_shared_scope_error`, each
with a sibling `*_scope_revoked` builder differing only in protocol stamp and
text. Owners at two levels: in IMAP, the SELECT is the chokepoint every lane
passes, so `ImapAccount::select_folder` (or a `SelectedFolder` checkout type it
returns) should carry the registry's ownership fact and return the
already-classified error; across crates, "PermissionDenied on a foreign-owned
scope derives DisableScope(scope)" is a recovery decision, and if `ErrorScope`
(or the builder) could carry "owned by another principal", `derive` could apply
it once and the four helpers collapse into a builder call.

## BUG-052 - IMAP `bulk_move` to a DAV-owned folder goes to the IMAP server while `throttle_keys` says IMAP does not own it

**Reported by:** imap-account (live).

The live defect is fixed: `bulk_move` and the membership throttle arm both go
through `DavScopeIndex::imap_mailbox`. Residue: nothing makes the routing rule a
compile error. `folder_from_scope` and `MailboxName::from_account_id` still turn
any `FolderId` into a mailbox without routing, and `route_scope` produces a
validated `MailboxName` that `inventory_stream` and `changes_stream` discard
(`Ok(ScopeHandler::Folder(_)) => {}`) and recompute inside the task. Owner: a
`RoutedScope` / `ImapFolder` type obtainable only from the router, for
`CursorScope` and `MembershipScope` alike, carried into the tasks.

## BUG-055 - IMAP `SearchFilter::Has` (attachment filename or type) compiles to a full-text `BODY` search

**Reported by:** imap-account.

It returns messages that merely mention the string. Refusing the filter as
`Unsupported` would take away a search path that works today; a correct fix
needs attachment-aware evaluation that composes with boolean filters and
position paging (for example BODYSTRUCTURE-based post-filtering of the
candidate set). The defect is now documented at the `pim.rs` arm and in
`reference/imap.md`.

## BUG-056 - "Was this JMAP id answered?" is decided per response and per caller, and an omitted `Blob/copy` answer is terminal

**Reported by:** jmap-protocol (live; its second-ranked move).

RFC 8620 promises one answer per submitted id; servers omit. What an omission
means is decided by each response type in `core/` and again by each caller in
`sync/`:

| Site | Omitted id becomes |
|---|---|
| `SetResponse::created` / `updated` / `destroyed` | `Error::SetIdUnanswered(id)` |
| `CopyResponse::created` (`*/copy`) | `Error::SetIdUnanswered(id)` |
| `EmailImportResponse::created` | `Error::SetIdUnanswered(id)` |
| `BlobUploadResponse::created` | `SetIdUnanswered`; a `notCreated` entry becomes `Error::NotParsable(format!("Blob {id} not created: {error:?}"))` |
| `CopyBlobResponse::copied` (`Blob/copy`) | `Error::IdNotFound(id)` |
| `ParseResponse::parsed`, `EmailParseResponse::parsed` | `IdNotFound`, or `NotParsable(id)` |
| `GetResponse` | nothing: `notFound` defaults to empty, and the comment tells every caller to reconcile `list + notFound` against what it submitted |

`sync/error.rs` re-decides: `into_account_error` maps a bare `SetIdUnanswered` to
`set_id_unanswered("*/set", id, ctx, guarded = true)`, guessing both method and
guard because the error does not carry them; `set_answer_error(op, method,
guarded)` exists so callers can re-supply what the error lost, and a caller that
forgets gets the guess; `get_id_unanswered`, `get_id_unresolved_after_query`,
`require_created_id` / `created_id_unusable` and `convert_id_not_found` are the
per-lane classifiers, referenced at roughly forty call sites across `pim.rs`,
`contacts.rs`, `calendar_ops.rs`, `filters.rs`, `hydrate.rs`, `page_cursor.rs`,
`discover.rs`.

They already disagree. The `SetIdUnanswered` doc says the create maps of
"`*/copy` and `Blob/upload`" take the partial-response lane; `Blob/copy` does not:
an omitted blob becomes `IdNotFound`, which `convert_id_not_found` (no scope on
the context) turns into terminal `Protocol(MissingField)`, live in production
through `copy_attachment_blob` in `sync/pim.rs`, while sibling write lookups give
retryable `Protocol(PartialResponse)`. The same function reports a response
naming the wrong accounts as `NotParsable` prose (`ParseFailed`) rather than a
contract violation. A `Blob/upload` refusal (`overQuota`, `tooLarge`,
`rateLimit`) loses its `SetError` type and classifies as `Protocol(ParseFailed)`,
a provider-fault class for a quota refusal (latent: `BlobUploadRequest` has no
production caller).

Owner: one generic in `core/` every response's id lanes decode into
(`IdAnswers<K, T, E>` with `take(&K) -> Answer<T, E>`, `Answer = Ok(T) |
Refused(E) | NotFound | Unanswered`, method name and guard stamped on the request
side). Better, the `Target` permit move: the request builder hands out a
non-`Clone` permit per submitted id (`SetRequest::update` / `destroy` / `create`,
`GetRequest::ids`) and the response's only accessor is `settle(permit) ->
Answer`, making exactly-once accounting a compile-time property; the "reconcile
yourself" instruction on `GetResponse` disappears, and `SetIdUnanswered` carries
`{ method: &'static str, id, guarded }`, so `set_answer_error` and the `"*/set"`
guess go.

## BUG-057 - What an absent or zero JMAP core limit means is decided in eight places, with live disagreements

**Reported by:** jmap-protocol (live disagreement).

RFC 8620 section 2 core limits are read raw (`CoreCapabilities` fields are
`Option<usize>`) and interpreted per reader:

| Reader | absent | zero |
|---|---|---|
| `core/request.rs::CallLimit` | `Unadvertised`: no limit enforced | `Invalid`: not enforced |
| `sync/factory.rs::batched_open_probes` | "not a licence to batch": one call per request | (`max_calls < 2`) one call per request |
| `client_ws.rs::encode_ws_request` (`maxSizeRequest`) | not enforced | not enforced (`max > 0`) |
| `blob/upload.rs::upload_to` (`maxSizeUpload`) | not enforced | enforced (every non-empty upload refused) |
| `Session::api_request_concurrency` / `upload_concurrency` | 1 | 1 |
| `sync/capabilities.rs::validate_session` | refuses open (calls, get, set, size, concurrency, collations) | refuses calls/get/set/size; accepts upload and concurrency zero |
| `validate_session` again, after refusing | `.unwrap_or(1).max(1)` on get/set (dead defaults restating a third reading) | |
| `sync/capabilities.rs::upload_size_limit` | refuses | accepts |

With `maxCallsInRequest` omitted, `CallLimit` lets a request grow without bound
while `batched_open_probes` refuses to put two calls in one; `validate_session`
then refuses the account, so the first two answers are reachable only before
validation (the `CallLimit` doc says this is deliberate, because probes run
before validation). Open probes running against an unvalidated session is itself
why three of these readers exist. Owner: validate once, at
`SessionState::derive` (which already runs on every installed session),
producing a typed `CoreLimits` with `NonZeroUsize` where the RFC or the crate
requires positive and an explicit `Unbounded`/`Serial` reading where it does
not; every reader takes it. The session interpretation itself should move out of
the engine-facing layer: `validate_session`, `upload_size_limit`, the
"which capabilities are malformed and which matter" policy, and the `CoreLimits`
type live in `sync/capabilities.rs`, interleaved with the `AccountCapabilities`
mapping, though they are a reading of RFC 8620 section 2 and run after `Client`
already acted on the session unvalidated; they belong in `core/session.rs` as
`SessionState::derive -> ValidatedSession`, with `sync/capabilities.rs` keeping
only the mapping.

## BUG-068 - The JMAP state cache advances from its cached state instead of `oldState`, laundering concurrent changes into the next guard

**Reported by:** jmap-account (latent, P2).

The laundering defect is fixed: Email cache advances now require the response's
`oldState -> newState` continuity. Residue, structural: `state_cache::StateMap =
Arc<Mutex<HashMap<String, Option<String>>>>` documents a tri-state (absent /
probed-empty / known) and `get` flattens it, and about fifteen writer sites each
still choose their own transition. Also `mailbox_states` is
write-only: container create/rename/move/delete, the Mailbox changes walk,
establishment and the factory maintain it, nothing reads it to decide anything
(establishment reads it only to CAS against itself; the lifecycle poller has its
own `lifecycle_mailbox_states`), and the shared accounts' entries are seeded for a
scope type (shared Mailbox) that does not exist. Owner: a `StateCell` per `(Slot,
ObjectType)` owning `observe_set(old, new)`, `observe_changes(since, new)`,
`observe_probe(s)`, `guard() -> Option<IfInState>`; delete `mailbox_states`.

## BUG-070 - The same JMAP `notFound` is success through `bulk_set_flags` and terminal through `set_keyword`; providers disagree on vanished targets

**Reported by:** jmap-account (live, P2).

`error::classify_set_item` turns a JMAP `notFound` / `blobNotFound` into
`Succeeded(Skipped)` for `UpdateFlags`, `BulkMove`, `BulkDestroy`, `SetKeyword`,
`SetIsRead`, `AddToContainer`, `RemoveFromContainer`. Graph absorbs a 404 as
`Skipped` only for destroy. So a flag update or a move of a vanished message is a
success on JMAP and `Failed(NotFound)` on Graph; a move that moved nothing
reported as success is questionable on its face. Inside JMAP the list is half
dead: only `mutation.rs` calls `classify_set_item`, with the three bulk
operations; `pim.rs`'s single doors route the same `notFound` through
`SetProgress::item -> set_answer_error -> into_account_error` with no item scope,
landing on `Protocol(Unknown)` (`UnknownPermanent`). Same wire answer, same
keyword patch: success via `bulk_set_flags`, terminal unknown error via
`set_keyword`. Owner: a per-`AccountOperation` "absent target" policy in
`bifrost-types`, applied by every provider. The read-back side of "is it gone" is
BUG-021.

## BUG-071 - JMAP partial-commit records name shared-account targets by native wire id; two `Email/set` pipelines

**Reported by:** jmap-account (live, P3).

`mutation.rs` (bulk doors) and `pim.rs` (single-target doors, threads, drafts,
send) both build and send `Email/set` for the same patches (keywords,
`mailboxIds`, destroy) and answer three questions differently. Guard:
`mutation::uses_if_in_state` guards `FlagOp::Set` and moves with the cached state
and retries once on `stateMismatch`; `pim::send_email_set` never guards (its
comment argues patches need no guard); `destroy_thread_emails_if_unchanged`
guards with a read state. Absent target: BUG-070. Outcome vocabulary: per-item
`ItemOutcome` lanes vs a `PartialCommit` record built by `SetProgress`.
`SetProgress::finish` names its targets with the WIRE id (`ErrorScope::Message {
id: EmailId::as_str() }`), which for a shared account is the native, unqualified
id, so a partial-commit record from `set_keyword` / `move_thread` /
`delete_thread` on a shared message names ids the consumer never saw and cannot
correlate - and the record is the only place the consumer learns which targets
committed. Proposal: one `mail_set.rs` executor shared by both (STR-104), and the
same question asked for IMAP in STR-080.

## BUG-072 - HTTP status -> error kind is decided by five ladders that already disagree

**Reported by:** jmap-account (live, P3), net-dav (live).

The net-dav hunter widened this to every HTTP crate. Sites: bifrost-net
`account_error.rs::status_kind_cause` (and, inside the same file,
`server_cause_from_status` answering a sub-question a second time: 408/502/503/504,
429, 507, 5xx); dav-core `error.rs::status_error` (two parallel if-chains, one for
the kind and one for the cause, held in step inside one function); Graph
`graph_error.rs::classify_by_status` (a near-copy of net's, down to the `(kind,
cause, bool)` tuple); Google `account/error.rs::classify_by_status`; JMAP
`sync/error.rs::convert_problem` (below).

| status | net | DAV | Graph | Google |
|---|---|---|---|---|
| 409 | ConcurrencyConflict | ConcurrencyConflict | ConcurrencyConflict | Server(Error 409) |
| 412 | Server(Error 412) | ConcurrencyConflict | ConcurrencyConflict | ConcurrencyConflict |
| 423 | Server(Error) | ConcurrencyConflict | Server(Error) | Server(Error) |
| 500 | Server(Error 500) | Server(Error 500) | Server(Error 500) | Server(Unavailable) |
| 507 | QuotaExhausted | QuotaExhausted | QuotaExhausted | Server(Unavailable) |
| 413 | Server(Error) | Server(Error) | Server(Error) | Request(Malformed) |
| 404, no scope | Server(Error 404) | NotFound(dialect resource) | Server(Error 404) | resource-dependent |
| 400/422 cause | `RequestCause::Malformed` | `RequestCause::InvalidArgument` | `Malformed` | `Malformed` |

Graph's own comment states the 412 split ("a bare 412 is `ConcurrencyConflict` here
and a bare `Server(Error)` in bifrost-net"). Every consumer falling through to net's
funnel - DAV's unreplayable histories and Graph's non-envelope bodies included -
gets net's table, so the same 412 is a conflict on one path and an unknown server
error on another inside one account. Owner: one classification function exported
by bifrost-net (or bifrost-types if IMAP-side callers want it), roughly
`classify_status(status, &StatusContext) -> StatusClass { kind, cause, throttles:
bool }`, with provider refinements passed in as a declared override set
(cursor-context 400/410, Gmail's history endpoint, DAV's 412/423, Graph's envelope
codes, JMAP's problem types) rather than copied tables. The scope projection beside
it is CON-044; throttle stamping is CON-061.

The jmap-account hunter's original account follows.

`error::convert_problem` classifies the HTTP status of a problem-details response
itself; `bifrost-net`'s `status_kind_cause` classifies the same statuses for every
other HTTP failure. 5xx: JMAP maps every 500-599 to `Server(Unavailable)`
(retryable), net maps only 502/503/504 to `Unavailable` and the rest to
`Server(Error{status})`. 507: net maps to `QuotaExhausted` with a throttle scope,
JMAP to `Unavailable` with none. 408: net maps to `Unavailable`, JMAP to
`Server(Error{408})`. An identical 500 from the same server is retried or not
depending on whether it carried an `application/problem+json` body. The
status-line precedent applies: net exports the status classification, and JMAP
overrides only the JMAP-typed problem types (`limit`, `unknownCapability`,
`notJSON`, `notRequest`).

## BUG-073 - Account crates classify the same cursor-decode failures differently; JMAP and Gmail are exactly inverted

**Reported by:** types (LATENT), jmap-account (live disagreement), graph, google.

The google hunter confirms the Google arm (`GmailCursorFailure::MalformedPayload`
-> `SyncState(CursorInvalid)` -> `RestartScope(Account)`) against Graph's and
JMAP's `SchemaIncompatible`, and proposes `OpaqueChangeState::decode::<T>(protocol,
version)` returning one typed `CursorDecodeError` with one mapping in the error
model. It also notes `changes.rs` re-implements
`decode_gmail_state_for_profile`'s identity check inline instead of calling it.

The graph hunter adds that Graph (`cursor::decode_cursor`) splits newer
(`EnvelopeUnknown`) from older (`SchemaIncompatible`) and then maps both to
`SchemaIncompatible` anyway, while IMAP, CalDAV and CardDAV use strict equality;
its owner is `OpaqueChangeState::open(expected_protocol, expected_version) ->
Result<&[u8], InnerEnvelopeMismatch>` with the one mapping onto `AccountError`.

The types hunter's sites: `imap/src/account/envelope.rs::decode_cursor`,
`carddav/src/account.rs::decode_cursor_snapshot`, `caldav/src/account.rs` (same
shape), `graph/src/account/cursor.rs::decode_cursor`,
`jmap/src/sync/state.rs::decode`, `google/src/account/cursor.rs::decode_gmail_state`.
It read them as converging on `SyncState(SchemaIncompatible)` by different routes:
Graph and JMAP split newer (`EnvelopeUnknown`) from older (`SchemaIncompatible`);
IMAP, DAV and Gmail do not distinguish direction; DAV merges protocol and version
into one message; IMAP stamps `AccountOperation::EstablishCursor` even when
decoding for `changes_stream`; Gmail mints its own message keys.

The jmap-account hunter found they do not converge:

| Failure | JMAP (`changes::stream`) | Gmail (`InvalidCursor`) | Graph (`cursor_error_to_account_error`) | IMAP (`envelope.rs`) |
|---|---|---|---|---|
| wrong `protocol` tag | `CursorInvalid` (restart scope) | `SchemaIncompatible` (account clear) | `SchemaIncompatible` | `SchemaIncompatible` |
| malformed payload bytes | `SchemaIncompatible` | `CursorInvalid` | `SchemaIncompatible` | `SchemaIncompatible` |

JMAP and Gmail are exactly inverted, each with a comment arguing its own choice.
The protocol-tag check is protocol-neutral (it compares
`OpaqueChangeState::protocol` with the account's own protocol), so four crates
should not answer it at all. Within JMAP the mapping is also built inline in
`changes::stream` rather than in `error.rs`, outside the module carrying
`#![warn(dead_code)]`.

Owner: `bifrost-types` (or the engine, which knows `account.protocol()` before
dispatch). The types hunter's shape: `OpaqueChangeState::payload_for(protocol,
version) -> Result<&[u8], PayloadMismatch { Protocol, Older, Newer }>` plus one
`AccountError` constructor, each crate supplying only operation and scope. The
jmap-account hunter's: a shared `CursorDecodeFault { ProtocolMismatch,
FutureVersion, RetiredVersion, Malformed, ScopeMismatch }` -> `AccountError`
mapping every crate calls. The interchangeable version integers beneath it are
STR-007.

## BUG-075 - Graph presents its Graph-audience token to EWS and Autodiscover

**Reported by:** graph (probable live; not observable hermetically).

`bifrost_net::TokenSource::current()` takes no resource or host.
`public_folder::ews_client` builds the EWS client over the Graph client's own
`AccountNet`, and Autodiscover posts through `execute_aux` with
`AuxTarget::Bearer` on that same net. Both present the Graph access token, whose
audience is `https://graph.microsoft.com`, to `outlook.office365.com`, which needs
an Outlook/EWS-audience token; Azure AD issues one audience per access token, and
the trait gives a consumer no way to answer differently per host. Unless
something outside the crate compensates, EWS streaming push, public-folder sync,
public contacts and calendars, and delegate Autodiscover cannot authenticate in
production; the scripted wire accepts any bearer. The axis belongs in net: either
`AccountSpec` carries a token source per trusted origin, or
`TokenSource::current(&Audience)`, with Graph constructing its Outlook-origin
requests with the Outlook audience. Related: STR-113 (cloud-aware origins).

## BUG-086 - Google stamps Calendar, People and Drive errors as Gmail, and names the wrong OAuth scope for them

**Reported by:** google (live; its headline), net-dav (independently flagged `execute_builder`'s string service with its `_ => ApiSurface::Drive` default, and asked that it take `ApiSurface`).

The crate is four Google API clients (Gmail, People, Calendar, Drive) sharing one
handle named after one of them, `GmailClient`, and nothing in the type system says
which API a request belongs to. The crate rebuilds that fact three times from three
inputs: `client.rs::surface_for_url` (longest-prefix match over three bases, with
`Drive` the catch-all for anything matching none); `client.rs::execute_builder`,
mapping a `&str` service name (`"Gmail API" | "People API" | "Calendar API" | _`)
and sending anything unrecognized to `Drive`, so a typo in `"Gmail API"` silently
drops the concurrency limit and the project hold; and
`account/error.rs::GmailErrorContext.surface`, defaulting to `Gmail` in `base()`,
overridden in the `calendar_*` and `contact*` constructors, set by hand in
`cloud.rs::ctx()`. `quota.rs::ApiSurface` is the right enum, but nothing carries it
on a request. The error model then forgets it: `parse_json_response` and
`check_response_status` take a `_service: &str` and ignore it, and `GmailService`
has one variant, `GmailApi`, so Calendar, People and Drive errors go out as
`Provider::Gmail` / `Protocol::Gmail` / `WireCause::Gmail(..)`. `classify_response`
treats every envelope as Gmail's (`FailedPrecondition` outside the history endpoint
maps to `ConcurrencyConflict`; `PubSubSubscriptionDeleted` is matched on
`operation == PushSubscribe`), though People reports `google.rpc` statuses and
Calendar uses its own reason set. Graph distinguishes `Protocol::Graph` from
`Protocol::Ews`; Google stamps one protocol for four. And `gmail_scope_for` is keyed
on `AccountOperation` with a `_ => "gmail.modify"` catch-all, so an
`insufficientPermissions` from Calendar or People tells the consumer to request
`gmail.modify`.

Whether a response may stamp the Gmail project budget is decided twice - at
observation time keyed on the URL's surface (`send_recorded`), and at translation
time keyed on the context's surface (`translate_response`, and
`translate_net_error`'s `LocalHold` arm) - held in step by
`quota::tests::check_surface`, which forges headers to prove the context-side copy
also refuses: a pairwise-agreement test over two copies. Owner: a typed handle per
API (STR-121), carrying the surface on every request and in its errors' type; each
API gets its own classifier feeding one builder funnel.

## BUG-099 - IMAP and ManageSieve turn every OAuth token-source failure into server-acknowledged `AuthLost`

**Reported by:** smtp-sasl (live; its top-ranked finding).

SMTP `oauth_token_error` projects the `bifrost_net::Error` through
`into_account_error_with_throttle_scope` and tunnels the typed result with
`with_preclassified_account_error`, explicitly so a transient refresh failure is not
flattened into a terminal one. IMAP `authenticate_best` maps it to
`Error::auth_with_mechanism(format!("failed to read OAuth access token: {e}"), None,
..)`; `classify_auth` with no response code yields
`Authentication(ReauthorizationRequired)` and stamps `TransmissionState::Acknowledged`
("auth failures are server-acknowledged tagged responses"). `account/sieve.rs` does
the same via `Error::auth_with_code(.., None)`. So on IMAP a token endpoint outage, a
`RefreshFailed { Network }`, or the `Cancelled` lost-driver case `reference/net.md`
says must NOT be `AuthLost` all become terminal `AuthLost` with false "the server
answered" evidence, sending the user to re-authorize an account whose token is
fine - net.md records fixing exactly this in bifrost-net; the IMAP consumer
reintroduces it one layer up. Owner: IMAP gets an `Error` variant carrying a
preclassified `AccountError` (SMTP's tunnel) or a typed token failure, and the
decision is made once - e.g. a bifrost-net helper `token_failure_account_error(error,
protocol, operation)` for IMAP, ManageSieve and SMTP. Related smell: SMTP's
`oauth2_token_blocking` turns `TokenSnapshot::Contended` into a fabricated
`bifrost_net::Error::RefreshFailed { source: Network { "OAuth token cache is busy" }
}` purely to obtain a transient classification; the classification of a snapshot
state belongs on `TokenSnapshot` (or in net's projection).

## BUG-100 - A SCRAM server error other than `invalid-proof` is `AuthLost` on IMAP and `PolicyBlocked` on SMTP

**Reported by:** smtp-sasl (drift).

SMTP `From<SaslError>`: `invalid-proof` -> `ErrorKind::Authentication` (AuthLost);
any other value -> `InvalidInput` + Auth phase -> `Authorization(PolicyBlocked)`,
pinned by `a_scram_policy_error_carries_its_rung_through_the_policy_lane`. IMAP
`From<SaslError>` and `AuthenticateScramConsumer::named_sasl_error`: every
`AuthFailed` -> `Error::Auth` -> `ReauthorizationRequired` -> AuthLost. So
`e=channel-binding-not-supported` is a policy block on SMTP and "re-enter your
password" on IMAP, for the same server and credential. IMAP's comment says it
"mirrors `bifrost_smtp`'s `named_sasl_error`; the two crates must not differ" - they
differ in the class and agree only on the message suffix. Root cause: the server
error is prose with a magic prefix (STR-133). Owner: bifrost-sasl, via a typed
`ScramServerError` and one `is_credential_rejection()` / `refusal_class()`.

## BUG-104 - The reverse path depends on which door sends the message

**Reported by:** smtp-sasl (drift).

Given RFC 5322 headers, which address is the reverse path: bifrost-smtp
`Envelope::try_from(&Headers)` lets `Sender` win over `From` (pinned by
`envelope_from_headers_sender`); IMAP `pim.rs::parse_draft_for_submission`
(draft_send, send_raw_message) takes first `From`, else first `Sender`, else the
configured default; bifrost-types `mime::render` (send_message) uses the resolved
`from` (request or default) and does not model `Sender`. The same raw message sent
through bifrost-smtp's `Transport::send` and through the IMAP account's
`send_raw_message` gets a different MAIL FROM; RFC 5322 makes `Sender` the agent
responsible for transmission, the conventional bounce target, and IMAP chose the
opposite without saying why. Owner: one `HeaderMap::submission_envelope()` in
bifrost-types used by IMAP and by `Envelope::try_from`. Adjacent: when a draft has
no Message-ID, `parse_draft_for_submission` falls back to the constant
`bifrost.draft@{domain}`, so every such draft from one domain gets the same id,
which then becomes the "stable fallback id" on the receipt
(`append_to_sent_or_fallback`) - not unique by construction.

## BUG-110 - "May this logical request have committed?" is decided four ways in net and DAV, and they disagree

**Reported by:** net-dav.

Given the attempts so far, what is the effective transmission state, and is replay
forbidden? The `request.rs` send loop sets `may_have_committed |= !replayable &&
status != 401`, sets `unreplayable_terminal` via `left_the_process`, and decides
"proven refusal" in `status_error_is_replayable` (401 and 429 only, with a comment
that 502/503/504 can follow a commit). `account_error.rs`'s `AttemptHistory` arm
re-derives everything from the translated cause chains: it excludes 401s
(`rejected_unauthorized`), computes `acknowledged_may_have_committed`, and
reclassifies to `PartialResponse` when `inflight || (acknowledged && terminal ==
Some(Unsent))`. Its `LocalHold` arm folds the same states differently, with no 401
exclusion. dav-core's `RedirectHistory::record_response` applies its own
`unreplayable && status != 401` rule, and `attach` reclassifies to
`PartialResponse`/`InFlight` when `unreplayable && may_have_committed` and the
terminal is `None | Some(Unsent)`. dav-core `wire_outcome`'s `refused_only` treats
earlier attempts of 401, 429, 503 and 501 as proven refusals. Disagreements: net
reclassifies on "acknowledged earlier attempt + `Unsent` terminal" even for a
replayable request, DAV only for an unreplayable one; on a terminal with no attempt
evidence (`None`, e.g. a credential error) DAV reclassifies while net pushes an
Acknowledged attempt and keeps the terminal's kind; DAV's refusal set includes 503,
which net's own comment says does not prove refusal (dead today for unreplayable
requests, because net never retries past a 503 on them - a second rule waiting to
drift). Owner: a public `AttemptLedger` in bifrost-net recording typed outcomes,
owning one `is_proven_refusal(StatusCode)` predicate and exposing a verdict
(`CommitEvidence::{Unsent, Acknowledged, MayHaveCommitted}`); net's loop and any
self-walking caller (DAV) feed it, and `account_error` reads the verdict rather than
re-mining `Cause::Attempt` entries out of translated errors. The evidence fields it
replaces are STR-144; the second walker is CON-060.

## BUG-048 - IMAP sync lanes cycle on a `UIDNOTSTICKY` folder

**Reported by:** imap-account; reopened by the wave 1 review.

Inventory and changes on a `UIDNOTSTICKY` folder mint cursors the next SELECT
voids, so the scope cycles CursorInvalid -> re-inventory. Refusing the folder
(as the poller does) was tried and reverted: it would make the folder's mail
vanish from the user's view, which removes a capability. The remaining fix is a
cursor-free sync mode for such folders (re-inventory on each poll, no durable
UID cursor, no CursorInvalid loop); the inventory site and `reference/imap.md`
say so.

## BUG-116 - The HTTP bandwidth meter cannot be handed to the engine or IMAP

**Reported by:** net-dav (likely live wiring defect; verify before acting).

The bandwidth registry can be injected (`Net::with_bandwidth_registry`), but the
meter cannot: `Net::new` always builds a private one, `Net::meter()` returns
`&BandwidthMeter`, and `BandwidthMeter` is not `Clone`, while
`SyncEngineBuilder::with_bandwidth_meter`, `ImapAccountFactory::with_bandwidth_meter`
and `MeterSinkHandle::from_meter` all require an `Arc<BandwidthMeter>`. As far as the
hunter can see, no consumer can hand the engine (or IMAP) the meter HTTP traffic is
recorded into, so `Control::bandwidth_observed` reads zero for JMAP, Graph, Google
and DAV accounts. More broadly, per-account state on `Net` is split across separately
keyed registries (the meter map refcounted per id, the bandwidth registry with weak
entries per id, `account_hosts` per id and per registration token, the concurrency
and hold registries by string key), and `attach_account`, `detach_registration` and
`retag` must each touch all of them correctly. Owner: one injected `BandwidthPlane {
registry, meter }` (or `NetResources`) shared by Net, the engine and the IMAP
factory, and behind it an `AccountRegistration` value owning every per-account
entry, so detach and retag are one operation each. The string-keyed registries are
STR-145.

## BUG-133 - A stale reattach abort can delete a row a scoped recovery resumed

**Reported by:** resolution wave review (lateral; needs a ruling).

`crates/sync/src/engine/ack.rs` now keeps a provisional reattach mark until the
owning write or delete succeeds. In the resume case the scoped re-establish reads
with `claim_for = None`, so a surviving row stays marked for the old reattach
attempt; if that attempt's `ReattachAbort` arrives after the live scope resumed the
row, it deletes the durable row. In-memory state is unaffected and the next durable
`Ack` rewrites and promotes it, so the exposure is losing the resume point across a
restart before that ack. Before the ordering fixes the mark was always dropped, so
the window is new. It needs a reattach attempt still pending while a scoped
recovery runs on the same scope; decide whether that interleaving is reachable and
whether a non-reattach resume should drop or transfer the mark.

## BUG-150 - The Gmail query encoder drops time of day and substitutes the epoch on failure

**Reported by:** wave 5 fixer (google, lateral).

Date filters reach Gmail `q` as dates only, and a conversion failure yields the
epoch rather than a refusal, so a bad bound silently searches from 1970.

## BUG-151 - A failed Graph hydration keeps the cached etag

**Reported by:** wave 5 fixer (graph, lateral).

`account/get.rs` leaves the previously cached etag in place when hydration
fails, a 404 included, so a later conditional write can carry the validator of
an item already known to be gone.

## BUG-152 - IMAP leaf matching and the quota fallback edge cases

**Reported by:** wave 5 fixer (imap-account, lateral).

A `NIL` hierarchy delimiter still triggers slash-based leaf matching in role
resolution, and the quota probe's fallback can still pick a shared folder when
the account has no personal INBOX.

## BUG-153 - A huge COPYUID range is expanded into one id per UID

**Reported by:** wave 5 fixer (imap-protocol, lateral).

`account/mutate.rs` expands every COPYUID destination range into individual
target references, so a very large server-supplied range costs unbounded CPU
and memory. Bound the expansion or keep the range as a range.

## BUG-154 - JMAP foreign-id and principal patch paths skip validation

**Reported by:** wave 5 fixers (jmap, lateral).

`sync/foreign.rs::parse_foreign` accepts an empty account id and repeated
separators. Separately, `principal/set.rs` alias and member paths interpolate
unescaped tokens and can coexist with a whole-property replacement, bypassing
the new `PatchPath` overlap rule.

## BUG-155 - Public coverage membership mutators can bypass subscription ownership

**Reported by:** wave 5 fixer (sync, lateral).

`PendingCoverage` now owns live-receiver membership, but its membership
mutators are still public, and one ledger can be shared by several delivery
gates, so a caller holding the coverage can break the ownership assumption the
delivery gate relies on. Nothing in production does this; narrowing the
mutators' visibility closes it.

