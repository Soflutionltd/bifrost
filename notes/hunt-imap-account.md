# Hunt: imap-account (`crates/imap/src/account/`)

Scope read: every module under `crates/imap/src/account/`, with excursions into
`crates/imap/src/types/` (ids, auth, StoreResult), `bifrost-types` (`account.rs`,
`capabilities`, error kinds, `ImapResponseCode`), `bifrost-sasl`, the JMAP and
Graph error translators, the CardDAV / CalDAV account impls, and the sync
multiplexer's lifecycle reader. No code was changed.

Findings are grouped by the three questions. Each finding says what the shape
is, where every copy lives, whether the copies already disagree, and where the
single owner should be. A separate section at the end lists bugs and smells
noticed along the way (several are consequences of the structural findings and
are cross-referenced).

Ranking, most valuable first:

1. The message identity `(folder, uidvalidity, uid)` travels as bare integers in
   at least seven shapes, while the nonzero newtypes for it already exist and
   go unused (1.1). Most of the duplicated decisions below hang off this.
2. "This id's UIDVALIDITY is no longer the folder's" is decided at seven sites
   with four different classifications (2.1).
3. "Permission denied on a shared scope means quarantine, not account loss" is
   decided per call site in IMAP (applied at six, missing at roughly ten) and
   re-implemented in JMAP and Graph (four copies workspace-wide) (2.2).
4. Folder role resolution is decided in four places, disagrees, and can pick
   another user's shared folder as this account's Sent / Trash / Drafts (2.3,
   B1).
5. `ThreadId` carries two incompatible namespaces inside one account (1.2).
6. `pim.rs` is a second mutation engine beside `mutate.rs`, missing the guards
   the first one has (3.2).

---

## 1. Axes that should be types

### 1.1 The message identity triple is a pile of `u32`s

The fact "message `uid` in folder `F` under epoch `uidvalidity`" is the central
domain fact of this crate. It is represented as:

- `DecodedObjectId { folder, uidvalidity: u32, uid: u32, id }` (`envelope.rs`)
- `DecodedThreadId { folder, uidvalidity: u32, uids: Vec<u32> }` (`envelope.rs`)
- `DecodedPartHandle { folder, uidvalidity: u32, uid: u32, .. }` (`parts.rs`)
- `SearchPosition { folder, uidvalidity: u32, key: u32 }` (`pim.rs`)
- `FolderEffect { folder, uids: Vec<u32>, uidvalidity: u32, .. }` (`pim.rs`)
- `ImapMessageRef { container, uid_validity: NonZeroU32, uid: NonZeroU32 }`
  (`poll.rs`, published) - std `NonZeroU32`, a third numeric representation
- `FolderCursor::{QResync, Condstore, Basic} { uidvalidity: u32, modseq: u64,
  uidnext: u32, .. }` (`envelope.rs`)
- `RawFetchOutcome::GenerationChanged { observed: u32 }` (`blob.rs`)
- `ModSeqCache { uidvalidity: Option<u32>, by_uid: HashMap<u32, u64> }`
  (`folder_registry.rs`)
- `MutationKind::ConditionalFlags(FlagOp, u64)` (`mutate.rs`)
- `append_message` returns `Option<(u32, u32)>` - a bare tuple of
  uidvalidity and uid, destructured positionally in `draft_create` and
  `append_to_sent_or_fallback`.

Meanwhile `crate::types::ids` already defines `Uid`, `UidValidity` (both
`NonZeroU32` newtypes) and `ModSeq`. The account layer uses none of them.
Consequences that are visible in the code today:

- `encode_object_id(folder, uidvalidity: u32, uid: u32)` and
  `encode_thread_id(folder, uidvalidity: u32, uids)` take two adjacent `u32`s of
  different meaning; every call site (and every test: `encode_object_id(&f, 7,
  42)`) can swap them silently.
- The "nonzero" rule is re-implemented by hand at least five times:
  `parse_nonzero_u32` (`envelope.rs`), `nonzero` (`parts.rs`), `operand_uid`
  (`targets.rs`), `CompactUidSet::from_uids` dropping zero, `record_modseq`'s
  `uidvalidity == 0` early return, and `poll.rs`'s `nonzero()` which
  `expect`s the decoder already did it.
- `fetch_to_inventory` mints `fetch.uid.unwrap_or_default()` - a UID 0 object id
  - because nothing in the type says the caller already checked.

Proposal: one `ImapRef { folder: MailboxName, epoch: UidValidity, uid: Uid }`
(plus `ImapThreadRef` for the UID-set form) owned by one codec module. Every
durable string IMAP mints - `imap1`/`imap2` object ids, `imapthread1`/`2`,
`imapsearch1` page cursors, `imappart2`/`3` part handles, the `imapmsgid1`
correlation token, `DraftHandle` (which is an object id in a costume),
`imap-idle-N` subscription handles - goes through that module. Today object-id
decoding (`decode_len_prefixed`) and part-handle decoding (`length_prefixed`)
each own their own length-prefix parser and their own "legacy version decodes
the folder literally (`MailboxName::new`), current version decodes wire identity
(`from_account_id`)" rule; that rule is the same decision made twice.
`ImapMessageRef` (public) should become a thin wrapper over the same type rather
than a third parallel struct. `FolderCursor` fields become `UidValidity`,
`ModSeq`, `Uid`; `CompactUidSet` becomes the UID-set type of record (see 3.4).

### 1.2 Two `ThreadId` namespaces inside one account

- `fetch_to_inventory` (`inventory.rs`) and `fetch_to_message` (`pim.rs`) set
  `thread_id` to the server's raw `THREADID` (RFC 8474) or the decimal X-GM-THRID.
- `search` returns `Page<ThreadId>` minted by `encode_thread_id`
  (`imapthread2:<len>:<folder>:<uidvalidity>:<uid,uid,..>`).
- `thread_hydrate`, `move_thread`, `delete_thread`, and every
  `MutationTarget::Thread` path (`decoded_targets`) accept only the
  `imapthread` form.

So `Message::thread_id` and `InventoryEntry::thread_id` from this account cannot
be handed back to this account's own thread methods: they fail
`Request(Malformed)`. JMAP has one thread namespace (the server `threadId`
redeems everywhere). Either the two need different types (a grouping key versus
a redeemable handle), or IMAP should mint one redeemable form for both lanes.
The type in `bifrost-types` currently cannot express the difference, which is
why nothing caught it.

### 1.3 `SendReceipt::id` is sometimes not an object id

`append_to_sent_or_fallback` returns either an APPENDUID-derived `ObjectId` or
`ObjectId("imapmsgid1:<Message-ID>")`, which `decode_object_id` rejects. The
`bifrost-types` doc on `SendReceipt::id` documents this in prose. The type should
say it: an enum such as `SentId::{Object(ObjectId), Correlation(MessageId),
Scheduled(..)}` in `bifrost-types`, since `SentCopy::Filed { id: Option<..> }`
already carries the addressable case separately. This is a cross-crate shape
(the field is shared by every provider's send path).

### 1.4 Strategy downgrades and other warnings travel as prose

`bifrost_types::StrategyDowngrade` exists (it is used by the dead
`error::strategy_failure`), but every live downgrade is
`Warning::support_only(WarningKind::StrategyDowngraded, reason)` with
`protocol_detail = format!("{from:?}->{to:?}")` - in `run_inventory`
(`inventory.rs`) and `send_strategy_downgrade` (`changes.rs`), and the scripted
tests assert on that Debug string (`downgrade_detail`). A consumer that wants to
know "this folder is now on Basic" must parse Rust Debug output. Make the kind
carry the typed value (`WarningKind::StrategyDowngraded(StrategyDowngrade)` or a
structured field on `Warning`).

Other warnings that a machine caller cannot branch on:

- `warn_if_uid_count_mismatch` - `WarningKind::Other` with counts in prose.
- `DavScopeIndex::build` collisions - `OperatorAttentionNeeded` with the
  collection href only in the message text.
- `classify_dav_open` - encodes "transient vs needs config" as the choice between
  `WarningKind::Other` and `OperatorAttentionNeeded`, while the classified
  `AccountError` that decided it is discarded from the warning (it survives only
  on the parallel skip entry, see 2.16).
- `push_stream`'s "NOTIFY SET refused: ..." - `WarningKind::Other`.

### 1.5 QRESYNC negotiation is three loose fields and a ticket

`QresyncNegotiation { enabled: bool, warning: Option<String> }` (factory) becomes
`ImapAccountInner::{qresync_negotiation_warning: Option<String>,
qresync_negotiation_warning_sent: AtomicBool}` plus the pool's
`qresync_policy` bool, plus `QresyncWarningTicket` to un-spend the one-shot.
`enabled == true` with a warning, or a warning that is not a downgrade reason,
are representable. One enum (`NotRequested | Enabled | Refused { reason:
QresyncRefusal }`) with a typed reason (`KnownBrokenServer`, `EnableNotConfirmed`,
`SessionDowngraded`) would also let `describe_cursor` and `run_changes` read one
value (see 2.5). The reason strings are currently restated in three places
(`negotiate_qresync`, `run_changes`, `run_qresync`).

### 1.6 Boolean parameters that name modes

- `select_folder(conn, folder, cursor, read_only: bool)` - every call reads
  `..., None, true)`. Combined with the cursor argument this is really a
  `SelectPurpose` (see 2.6).
- `FolderCursor::QResync { known_uids_complete: bool }` - "baseline complete" vs
  "legacy, must re-inventory" is a state, and `run_changes` special-cases it
  before dispatch. A `Baseline::{Complete(CompactUidSet), Legacy}` would make the
  incomplete QRESYNC cursor impossible to hand to `run_qresync` (which today
  `debug_assert!`s it).
- `blob::run_fetch(.., refuse_not_sticky: bool, ..)`, whose `NotSticky` outcome
  is `unreachable!` on one caller.
- `supports_notify` is stored on `ImapAccountInner` and again on `PushState`.

### 1.7 Distinct failures collapse into `Request(Malformed)`

A caller cannot tell these apart, and all derive `ClientBug`:

- `container_delete` on a non-empty mailbox (`pim_malformed`) - a precondition.
- `Error::AppendLimit`, `TOOBIG`, `MAXCONVERT*`, `METADATA MAXSIZE` - payload too
  large.
- `Error::FetchLimit` / `FetchLiteralLimit` / `SearchResultTruncated` - local
  budget exceeded on a server-acknowledged answer.
- sieve `ACTIVE` - "deactivate first" precondition.
- UIDVALIDITY changed (2.1) - not the caller's fault at all.
- `valid_uids` and `merge_folder` / `or_folder_restriction_error` - genuine
  caller errors, the only ones `Malformed` fits.

`RequestErrorKind` in `bifrost-types` has only `Malformed` and
`BatchInputInvalid`, so the protocol crates have nowhere else to put these. This
is a gap in the shared vocabulary, not just in IMAP: `PayloadTooLarge`,
`PreconditionFailed` (or reuse of `ConcurrencyConflict` for the
not-empty/active cases) would let a UI say something true.

### 1.8 Published knobs that decide nothing

- `ImapAccountConfig::{flag_sync_interval, deletion_check_interval,
  mutation_batch_size}` are read by nothing in the workspace.
  `mutation_batch_size` (default 1024) coexists with the hard-coded
  `BatchingPolicy::max_items: 1024` in `build_capabilities` and the
  `TARGET_BUFFER_ITEMS = 256` flush window - three numbers for one axis, one of
  them published and inert. These are published fields, so the fix is to wire
  them (or ask the owner), not to remove them.
- `ImapAccountInner::priority` is written by `set_priority` and never read.
- `AccountCapabilities::requires_uidvalidity_recheck` is set `true` by IMAP and
  read by nothing in the workspace (every producer sets it, no consumer exists).

### 1.9 Credentials and TLS mode, three copies each

- Credentials: `crate::types::Credentials` (with `CredentialsKind`),
  `SubmissionCredentials` (password is a plain `String`), and
  `bifrost_smtp::...::Credentials`, translated structurally in
  `resolve_credentials`. The token-source type already lives in `bifrost-net`;
  a single credentials type there or in `bifrost-sasl` would let IMAP, SMTP,
  ManageSieve and DAV share one secret-handling story.
- TLS mode: `SubmissionTls { Implicit, StartTls, Plaintext }`,
  `ManageSieveTlsMode { Implicit, StartTls, None }`, and `ImapConfig`'s own, each
  with its own default-port table.

---

## 2. Decisions made in more than one place

### 2.1 "The id's UIDVALIDITY is not the folder's" - seven sites, four answers

| Site | Classification |
|---|---|
| `get.rs::uidvalidity_changed_error` | `Request(Malformed)`, scope `Cursor(Folder)` |
| `mutate.rs::uidvalidity_changed_error` | `Request(Malformed)`, scope `Cursor(Folder)` |
| `blob.rs::run_raw` (inline builder) | `Request(Malformed)`, scope `Cursor(Folder)` |
| `pim.rs::valid_uids` | `Request(Malformed)`, no scope |
| `parts.rs::DecodedPartHandle::verify_uidvalidity` | `Request(Malformed)` via `envelope::malformed` |
| `pim.rs::SearchPager::check_epoch` | `ConcurrencyConflict` |
| `changes.rs::validate_uidvalidity` -> `error::uidvalidity_changed` | `SyncState(CursorInvalid)` |
| `poll.rs` (`UidListing` / `RawMessage`) | typed `GenerationChanged` outcome, no error |

They already disagree. The fact is the same each time: the server renumbered the
folder, the id names nothing (or names a different message). It is never the
caller's bug, so the `ClientBug` recovery the five `Malformed` sites derive is
wrong, and the engine's read-back or a UI cannot distinguish it from a corrupted
id. The poller already has the right shape: a typed outcome. Owner: the epoch
check belongs on the identity type from 1.1 (`ImapRef::check_epoch(live) ->
Result<(), EpochVoided>`), with exactly one translation of `EpochVoided` in
`error.rs` - most likely `NotFound(Message)` with a folder-scoped cause so the
engine also learns the scope needs re-inventory.

### 2.2 "Permission denied on a shared scope quarantines the scope"

Within IMAP, `error::shared_folder_error` is applied by each caller after it
looks up `folders.get(&folder).shared_owner` itself:
`inventory::run_inventory`, `get::run_folder_get`, `blob::run_fetch`,
`mutate::flush_mutation_groups`, `changes::select_error`, `poll::examine`. It is
NOT applied by: `pim::copy_messages`, `delete_messages`, `set_flag`,
`hydrate_decoded` (so `set_is_read`, `set_keyword`, `set_importance`,
`add/remove_from_container`, `draft_discard`, `draft_send`'s discard,
`message_hydrate`, `thread_hydrate`, `move_thread`, `delete_thread`), `search`,
`search_messages`, `fetch_full_message`, or the push session's SELECT. On those
paths a revoked share's `NO [NOPERM]` derives terminal account-wide
`NoPermission` - the exact escalation the helper exists to prevent.

Across the workspace the same decision has four copies:
`imap::error::shared_folder_error`, `jmap::sync::error::shared_scope_error`,
`graph::graph_error::graph_shared_scope_error` and `ews_shared_scope_error`,
each with a sibling `*_scope_revoked` builder whose only difference is the
protocol stamp and the text.

Owners, at two levels:

- In IMAP, the SELECT is the chokepoint every one of those lanes passes through.
  `ImapAccount::select_folder` (or a `SelectedFolder` checkout type it returns)
  should carry the registry's ownership fact and return the already-classified
  error, so no lane can forget.
- Across crates, the rule "PermissionDenied on a foreign-owned scope derives
  DisableScope(scope)" is a recovery decision, and recovery derivation is
  centralized in `bifrost-types`. If `ErrorScope` (or the builder) could carry
  "this scope is owned by another principal", `derive` could apply the rule once
  and the four helpers would collapse into a builder call.

### 2.3 "Which folder plays role R" - four deciders, two disagreements, one bug

- `pim::folder_role` / `role_folder_from_entries`: SPECIAL-USE attribute first,
  else the leaf name in `{"draft","drafts"}`, `{"sent","sent mail","sent
  messages"}`, etc.; iterates the whole registry, shared and other-user entries
  included; ties break on `cmp_account_order`, which is plain string order.
- `capabilities::build_capabilities` `has_drafts`: SPECIAL-USE `\Drafts` or leaf
  `"drafts"` only - not `"draft"` - and only over the personal LIST (it runs
  before shared folders are merged into the registry). Its comment claims it is
  "the same leaf rule `folder_role` uses"; the leaf split is shared, the name set
  is not. A server with a `Draft` folder resolves `role_folder(Drafts)` but
  advertises `draft_create = false`.
- `push::first_inbox_folder`: full mailbox name equals `INBOX`
  (case-insensitive) - not the role rule at all.
- `pim::quota_probe_folder`: `role_folder(Inbox)` then first selectable.

Because `role_folder` does not exclude foreign entries, a shared
`#shared/team/Sent` (sorts before `Sent`) or a shared folder carrying a `\Sent`
attribute when the personal one has none wins the role. See B1. Owner: the
registry should compute roles once at ingest (personal namespace only, with an
explicit policy for shared), and capabilities, send, draft, trash and push
should all read that one answer.

### 2.4 "Can this account push, and on how many lanes"

- `build_capabilities`: `PushCapability::InProcess` iff `Capability::Idle` and
  `push_capacity > 0`.
- `PushState::new`: lane count from `push_capacity` and `supports_notify`; no
  IDLE check.
- `push_subscribe` admission: `push_capacity > 0` and NOTIFY-or-budget; no IDLE
  check.
- `ImapAccountConfig::push_session_capacity`: the capacity rule itself.

On a server without IDLE and `pool_cap >= 2`, capabilities say "no push" while
`push_lanes()` is non-empty and a direct `push_subscribe` reports scopes in the
succeeded lane. The engine reads lanes only when the capability is not `None`,
so the engine path is protected; a direct consumer is not. Owner: one
`PushPlan { lanes, topology: Notify | PerFolder, capacity }` computed at open,
`None` when the server cannot IDLE, from which capabilities, lanes and admission
all read.

### 2.5 "Which strategy will serve this cursor"

`changes::describe_cursor` maps cursor tag plus `qresync_enabled()` to a
strategy and cost; `run_changes` makes the same mapping again to dispatch; and
`cursor_from_select` decides it a third time when minting. They agree today
because `describe_cursor_reports_the_strategy_the_run_will_actually_use` holds
two of them in step - and that test does not exercise the case where they
already disagree: a `QResync { known_uids_complete: false }` cursor is described
as `Cheap / QResync`, while `run_changes` refuses it with `CursorInvalid` before
any wire work. Owner: a pure `ChangePlan::for_cursor(&FolderCursor, policy)`
returning the strategy (and the `Refuse(incomplete baseline)` arm) that both
`describe_cursor` and `run_changes` consume.

### 2.6 "Does this SELECT replay the QRESYNC baseline"

Every caller of `select_folder` decides what to pass as the cursor:
inventory, mutation, poller, PIM primitives and search pass `None` (mutation
with an explicit comment that admission must not replay changes since the
consumer's checkpoint). `get::run_folder_get` and `blob::run_fetch` pass the
registry's hint cursor, so a hydration or raw read on a QRESYNC session issues a
QRESYNC SELECT whose VANISHED and changed-message payload is then discarded, and
the hint cursor is not advanced from it. That is wasted wire on every hydration
batch, and a policy the mutation path explicitly rejected. Owner: a
`SelectPurpose::{Sync(cursor), Read, Mutate}` argument decided per lane once,
replacing `(cursor, read_only)`.

### 2.7 "Does a dropped stream abort its task"

`guarded_receiver_stream` (abort on drop) is used by `get_stream`,
`inventory_stream` and `changes_stream`; `boxed_receiver_stream` (detached) by
`mutation_stream`, `open_raw_rfc822` and `scope_lifecycle_stream`. The rationale
written on `guarded_receiver_stream` ("two drivers issue IMAP commands
concurrently over one account while the older one sat on a pool checkout")
applies verbatim to `open_raw_rfc822`, which can keep fetching a 256 MiB message
for a receiver that is gone. Mutation may legitimately want to run to completion,
but nothing says so at the call site. Owner: one spawn helper taking an explicit
`OnDrop::{Abort, RunToCompletion}` so every lane states its choice.

### 2.8 IMAP response-code names: three tables, already disagreeing

The wire name of each response code is written in the codec decoder
(`codec/decode/flags_caps.rs`), in `account/error.rs::response_code_name`, and in
`bifrost_types::ImapResponseCode::code` (private). `classify_status` stamps
`native_code` from `response_code_name`, which returns the literal `"OTHER"` for
every unknown code, while `ImapResponseCode::Unknown { code }` (built by
`imap_response_code` for the same error) returns the server's real name. So
`native_code` loses exactly the information an operator needs for an
unrecognized code. Owner: make `ImapResponseCode::code` public and derive
`native_code` from the already-built `imap_response_code(code)`; delete the
middle table. (The 60-arm `ResponseCode -> ImapResponseCode` mirror is itself a
structural question, 3.11.)

### 2.9 SASL mechanism choice and payload construction

Commit `26aa7c25` moved XOAUTH2 / OAUTHBEARER payload construction into
`bifrost-sasl`. `account/sieve.rs::authenticate` was missed: it builds the PLAIN
payload and `format!("user={identity}\x01auth=Bearer {}\x01\x01", ..)` inline,
in plain `Vec<u8>` / `String` (not the zeroizing `Secret`), re-implements the
cleartext-without-TLS gate from `AuthPolicy`, and supports only PLAIN and
XOAUTH2 (no SCRAM, no OAUTHBEARER) where the IMAP ladder in
`connection/auth.rs` prefers SCRAM. SMTP has a third ladder. "Which mechanism
do these credentials use under this policy against this offer" is one decision
in three places. Owner: `bifrost-sasl` (a `select_mechanism(kind, offered,
policy, tls)` plus the payload builders it already has), with each protocol
keeping only its framing.

### 2.10 Throttle routing vs. operation routing for DAV-owned folders

`Account::throttle_keys` documents that "the answer must follow the same routing
decision the operation itself makes". IMAP's `ThrottleWork::Membership(Folder)`
arm consults `dav_scopes` and declares no key for a DAV-owned folder; the
operation it describes, `bulk_move` with that destination, never consults
`dav_scopes` (`validated_move_destination` accepts any sendable name) and sends a
`UID MOVE` to IMAP. The two answers to "who services this folder" disagree.
Smaller: the `Scope` arm normalizes through `MailboxName::from_account_id(..)
.account_id()` while the `Membership` arm keys on the raw `folder.0`.
Owner: the routing type in 3.6, applied to `MembershipScope` as well as
`CursorScope`.

### 2.11 "A flag state that cannot be established is a failure, not an empty set"

`get::run_folder_get` enforces the `bifrost-types` rule for
`Projection::FlagsOnly` (missing FLAGS -> `Failed(Protocol(MissingField))`).
`Projection::Metadata` and `inventory::fetch_to_inventory` carry the same flag
state as `Fingerprint::flags_hash`, and there a missing FLAGS silently becomes
`flags_hash(&[])` - a fingerprint that claims "no flags". Same rule, enforced on
one sibling arm and not the other.

Related: `get::selection_for_projection` and `get::fetch_to_hydrated` each match
`Projection` independently, and their wildcard arms disagree - the selection
asks only for FLAGS, the conversion emits `RawMime(decode_body(..)
.unwrap_or_default())`. A future projection variant would hydrate as a
successful empty MIME body. One `match` producing both the attributes and the
converter would close it.

### 2.12 Capability flag vs. runtime gate

`build_capabilities` decides `send_message`/`draft_send` from
`submission_configured`, `filters_*` from `has_sieve`, `draft_create` from
"Drafts exists and UIDPLUS"; the methods then decide again from
`account.submission.is_some()`, `config.sieve.is_some()`,
`role_folder(Drafts)` plus "APPENDUID came back". The PIM methods that read
`capabilities.pim_methods.X` directly are one decision; these pairs are two.
`capability_contract_tests.rs` is the pairwise-agreement test holding them in
step, and its own header records that it found three flags not gating their
methods. Container create / rename / move / delete read no flag at all.

### 2.13 Composition copies the sub-accounts' answers field by field

`build_capabilities` copies each contact and calendar `PimMethodSupport` field
and each `MutationConcurrency::ExpectedVersion` field from the sub-accounts by
hand, and `mod.rs` forwards each contact/calendar method by hand; `event_rsvp`
concurrency is hard-coded `false` and `event_rsvp_if_version` is not forwarded
(harmless while CalDAV refuses tokens, but nothing ties the two together). Every
new PIM field needs a remembered edit here. If `PimMethodSupport` and the
expected-version table grouped their fields by lane (`contacts:
ContactMethods`, `calendars: CalendarMethods`), composition would be one
assignment per lane and new fields would arrive automatically.

### 2.14 "A provider with no lifecycle events" - IMAP and DAV disagree

IMAP's `scope_lifecycle_stream` stays pending until shutdown, on the stated
ground that the engine treats an ended stream as a fault. CardDAV and CalDAV
return `stream::empty()`. The multiplexer's lifecycle reader confirms IMAP's
reading: on `None` it records a backoff failure, sleeps, and re-subscribes, so a
standalone DAV account cycles its lifecycle reader on backoff for its whole
life. (Inside an IMAP composition this is invisible, because IMAP does not merge
the sub-accounts' lifecycle streams.) Owner: a shared
`bifrost_types::idle_lifecycle_stream(shutdown)` helper that both use.

### 2.15 "Is a degraded part of the surface reported" - IMAP vs. JMAP/Graph

`types.md` defines `OpenedAccount::skipped_scopes`, `ContainerList` and
`Page::skipped_scopes` for exactly this. IMAP:

- `discover_shared_folders`: a failed `LIST` under a namespace prefix, and a
  failed `NAMESPACE`, are `tracing::debug!` only. No `SkippedScope`.
  `containers_list` then returns `ContainerList::complete(..)` with a comment
  saying the skips "were handled" at open - they were dropped.
- `DavScopeIndex::build` collisions (a collection that will never sync) go only
  to the prose warning lane, not to `skipped_scopes`.
- A failed DAV open is reported twice: a prose `Warning` via a bespoke
  `dav_degraded: Mutex<Vec<Warning>>` with peek / acknowledge-on-`Done` logic in
  `discover_cursor_scopes`, and a `SkippedScope`. The bespoke lane predates the
  skip lane and now duplicates it.
- `search` / `search_messages` walk every selectable folder and fail the whole
  call on one folder's SELECT failure; `Page::skipped_scopes` is always empty.
  Graph uses that lane for exactly the revoked-share case.

### 2.16 Smaller duplicated decisions

- Folding FETCH responses per requested UID: inline in `get::run_folder_get` and
  `pim::merge_requested_fetches`, both delegating the field merge to
  `get::merge_fetch_response`. The filter-to-requested rule is written twice.
- `importance_sets_important_keyword` (write) and `fetch_to_message`'s
  `flags.contains("$important")` (read) each name the importance keyword.
- `route_scope` produces a validated `MailboxName` that `inventory_stream`,
  `changes_stream` discard (`Ok(ScopeHandler::Folder(_)) => {}`) and then
  recompute with `folder_from_scope` inside the task.

---

## 3. Structure

### 3.1 `pim.rs` is not PIM

At roughly 4.6k lines it holds: the direct mail-mutation primitives and their
partial-commit records; SMTP send, raw send, draft create / send / discard,
byte-level RFC 5322 header surgery (`split_header_body`, `rewrite_header_block`,
`restamp_draft_date`, Bcc stripping); a search engine (criteria compiler,
provider-query validator, position-based pager with its own cursor codec);
container CRUD; folder-role derivation; MYRIGHTS projection; quota; typed message
and thread hydration including MIME selection; and about 1.4k lines of inline
tests. Contacts and calendars - the actual PIM - are not in it (they are
forwarding in `mod.rs`). Its own header says the long tail was never
line-audited, which is what a file this size invites. Suggested split:
`compose/` (send, draft, header rewriting - and the header surgery arguably
belongs beside `bifrost_types::mime`), `search.rs`, `containers.rs`, roles into
the registry (2.3), and the primitives folded into the mutation engine (3.2).

### 3.2 Two mutation engines

`mutate.rs` is the careful one: per-item three-lane outcomes through
`TargetBatch` permits (the `869ea04f` shape), opportunistic `UNCHANGEDSINCE`
from the MODSEQ cache with `MODIFIED` attribution, stale-epoch partitioning,
shared-folder `ScopeRevoked` mapping, the guarded EXPUNGE fallback, and
partial-commit records per group. `pim.rs` re-implements STORE, `\Deleted` +
EXPUNGE and COPY for the `Result<(), AccountError>` lanes (`set_flag`,
`delete_messages`, `copy_messages`) with its own `FolderProgress` record
builder - no MODSEQ guard, no shared mapping (2.2), a different stale-epoch
answer (2.1). The primitives could run `mutation_stream` and collapse the
per-item outcomes into one result plus partial-commit record; that would delete
a whole parallel engine and its divergences. (The workspace-wide pattern of a
`pim` module per provider beside the bulk lane suggests the same question is
worth asking in JMAP, Google and Graph.)

### 3.3 A ManageSieve protocol client lives in the account layer

`account/sieve.rs` (about 1.3k lines) is a complete RFC 5804 client: TCP
connect, implicit TLS and STARTTLS, SASL, line and literal framing, response-code
parsing, read timeouts, bandwidth metering - beside, and sharing nothing with,
the IMAP driver in `connection/`. Its failures are `crate::Error::Sieve` (an
IMAP error enum carrying another protocol's codes), classified in
`account/error.rs::classify_sieve`. It belongs in its own protocol module (a
sibling of `connection/`, or its own crate alongside `bifrost-smtp`), exposing a
client the account composes the way it composes SMTP. That move is also what
would make 2.9 natural.

### 3.4 The folder registry and UID sets

`CompactUidSet`, `UidSetDiff` and the range arithmetic live in
`folder_registry.rs`, while `UidSet`, `UidRange` and `SequenceSet` live in
`types/`. Two UID-set types with conversions between them
(`UidSet::from_ranges(known_uids.ranges())`, `CompactUidSet::from_ranges`). The
compact set belongs in `types/` as the account layer's set of record.
`FolderEntry` meanwhile bundles LIST facts, ownership, MYRIGHTS, a cursor hint,
a MODSEQ cache and a last-seen instant behind four locks; it is also the
implicit (and currently unexercised) owner of folder roles. Making roles,
ownership and the shared-error mapping explicit registry responsibilities
(2.2, 2.3) would give those decisions a home.

### 3.5 Error construction is not where `error.rs` says it is

`error.rs` opens by claiming to be "the only place in bifrost-imap that
constructs `bifrost_types::AccountError`", and opts back into `dead_code` so an
unreachable translation is caught. In fact about 36 `AccountErrorBuilder::new`
sites live in `mutate.rs` (10), `pim.rs` (7), `blob.rs` (3), `get.rs`,
`envelope.rs`, `factory.rs`, `poll.rs` (2 each), `targets.rs`, `scopes.rs`. Two
builders for `NotFound(Message)` (`get::message_not_found_error`,
`pim::expunged_message`, plus `blob::raw_message_not_found`) differ in whether
they stamp `Acknowledged`. Gathering the domain builders into `error.rs` would
make the claim true and put them under the armed `dead_code` lint.

### 3.6 DAV routing is a convention, not a type

`DavScopeIndex`'s doc says "EVERY `CursorScope::Folder` consumer must consult
this index" and records that a fifth consumer once forgot. `route_scope`
answers it, but nothing forces a lane through it: `folder_from_scope` and
`MailboxName::from_account_id` turn any `FolderId` into a mailbox without
routing, which is how `bulk_move` bypasses it (2.10). A `RoutedScope` /
`ImapFolder` type that can only be obtained from the router (for both
`CursorScope` and `MembershipScope`) would turn the rule into a compile error.

### 3.7 IMAP does not partition inventory

`inventory_partitioning` is not overridden, so IMAP folders inventory as one
`UID FETCH 1:*` that also accumulates every UID in a `Vec<u32>` before minting
the cursor. The half-open `u64`-ended `InventoryPartition::Uid` vocabulary in
`bifrost-types` was designed with IMAP in mind (`types.md` says as much), and the
bounded backfill lane depends on partitions, but no production crate mints a
`Uid` partition. Large folders therefore get no bounded backfill. Not a bug;
a gap between the shared vocabulary and its intended producer.

### 3.8 Test layout follows the harness, not the design

`scripted_tests.rs` (about 5.8k lines, ~140 functions) is organized by "needs a
scripted peer": send and Sent-append, draft send, mutation batching, push
admission and sessions, all three change strategies, the poller, raw reads,
search paging and PIM hydration share one file. Co-locating each transcript set
with the module it pins (a `scripted/` directory with one file per lane, the
harness staying in `test_support.rs`) would make a module's coverage visible
from the module. `pim.rs` carrying ~1.4k inline test lines is the same accident
in the other direction.

### 3.9 Account lifecycle state is four carriers

`closed: AtomicBool`, `shutdown: CancellationToken`, `close_completion:
OnceLock<Shared<..>>` and `send_gate: RwLock<()>` together encode
open / closing / closed. The `closed` flag is checked by the send paths,
`blob::run_fetch` and `poll::examine`, and left to the pool elsewhere. `close`
also `expect`s the teardown task, so a panic inside sub-account `close()`
re-panics in the engine's `detach`.

### 3.10 Protocol-specific enum in the neutral crate, mirrored

`bifrost_types::ImapResponseCode` mirrors `crate::types::ResponseCode`
one-to-one through a 60-arm `imap_response_code`. It exists for exported wire
evidence, which is a reasonable reason, but then the IMAP crate should own the
conversion as an `impl From` next to `ResponseCode` (codec scope), and the name
table should live in exactly one of the two (2.8).

---

## Bugs and smells noticed on the way

- **B1. Role resolution can pick another user's folder.** `role_folder` iterates
  shared and other-user registry entries and breaks ties by string order. On a
  server where the personal `Sent` / `Trash` / `Drafts` has no SPECIAL-USE
  attribute, `#shared/team/Sent` (or any shared path sorting first, or any shared
  folder with the attribute) becomes the account's Sent: `send_message` files the
  sender's copy there and `delete_thread` moves mail into another principal's
  Trash. (2.3)
- **B2. `draft_create` reports `Unsupported` after a committed APPEND.** When the
  server returns no APPENDUID - RFC 4315 forbids it for a `UIDNOTSTICKY` mailbox,
  and servers may omit it - the draft exists on the server and the caller gets
  `Err(Unsupported(DraftCreate))`, which reads as "nothing happened"; a retry
  duplicates the draft. The capability gate on UIDPLUS does not cover this.
- **B3. Sync lanes ignore `UIDNOTSTICKY`.** Only the poller and its raw read check
  `uid_not_sticky`. Inventory and changes on such a folder mint cursors that the
  next SELECT voids, so the scope cycles CursorInvalid -> re-inventory
  indefinitely. The poller's refusal (`Unsupported`, folder-scoped) is the
  answer the sync lanes should share.
- **B4. Revoked shares on the PIM, search and hydration paths escalate
  account-wide.** (2.2)
- **B5. `Message::thread_id` is not redeemable by this account.** (1.2)
- **B6. `native_code` is `"OTHER"` for every unknown IMAP response code.** (2.8)
- **B7. `describe_cursor` reports an incomplete QRESYNC cursor as Cheap QRESYNC.**
  (2.5)
- **B8. Hydration and raw reads issue QRESYNC SELECTs and discard the payload.**
  (2.6)
- **B9. Missing FLAGS on Metadata / inventory hashes as "no flags".** Unknown
  future `Projection` variants hydrate as an empty successful body. (2.11)
- **B10. ManageSieve credentials bypass `bifrost-sasl` and are not zeroized;**
  no SCRAM or OAUTHBEARER. (2.9)
- **B11. Shared-namespace LIST / NAMESPACE failures vanish into debug logs** and
  `containers_list` claims completeness. A search fails entirely on one folder.
  (2.15)
- **B12. `hold_until_rfc3339` falls back to an empty string** for an instant
  `jiff` cannot represent, which would be sent as an empty `HOLDUNTIL`
  parameter rather than refused locally.
- **B13. `SearchFilter::Has` (attachment filename / type) compiles to IMAP
  `BODY`**, a full-text body search, so it returns messages that merely mention
  the string.
- **B14. `open_raw_rfc822` is not abort-on-drop.** (2.7)
- **B15. Non-IDLE servers with `pool_cap >= 2` expose push lanes and accept
  direct `push_subscribe` scopes** while advertising `PushCapability::None`.
  (2.4)
- **B16. Standalone CardDAV / CalDAV lifecycle streams end immediately** and the
  engine cycles them on backoff. Outside this scope; found by comparison. (2.14)
- **B17. `bulk_move` to a DAV-owned folder goes to the IMAP server** while
  `throttle_keys` says IMAP does not own it. (2.10)
- Inert published config (`flag_sync_interval`, `deletion_check_interval`,
  `mutation_batch_size`), a write-only `priority`, and an unread
  `requires_uidvalidity_recheck` capability. (1.8)
- `dav_degraded`'s duplicate prose lane and the dead `DavAttach::Degraded {
  transient }` field. (2.15)

## What is already in the target shape

For calibration: `targets.rs` (`TargetBatch` / `Target` / `Sealed` permits) is
exactly the "axis turned into a type" move of `869ea04f` and makes the per-item
outcome accounting of the bulk and hydration lanes hard to get wrong;
`StoreWireOutcome::from_store_result` refuses to let a caller drop the tagged
status; `DavScopeIndex` resolves collisions once at open rather than at every
routing call; and the poller's `UidListing` / `RawMessage` outcomes show the
typed shape the rest of the crate's epoch handling (2.1) should adopt.
