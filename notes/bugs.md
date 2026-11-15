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

## BUG-016 - The backfill completion marker wins resume selection by a magic number, and can lose

**Reported by:** sync-lanes (latent; live once any provider adopts partitioning).

Mostly fixed: a typed `BackfillKey` with codecs for every existing encoding, one
public resume-comparison helper consumer stores can call, proven completion
beating counts, honest completion counts, and atomic completion compaction in
the in-memory store. Residue: incomplete `TimeWindowed` walks still mint fresh
absolute-instant keys each attach and accumulate rows; an unknown future
`InventoryPartition` variant still falls back to the historical shared key
(a safe opaque identity needs bifrost-types support); and external
`CheckpointStore` implementations must adopt the comparison helper and the
compaction contract, which nothing enforces. The original account:

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

## BUG-057 - What an absent or zero JMAP core limit means is decided in eight places, with live disagreements

**Reported by:** jmap-protocol (live disagreement).

Partly fixed in wave 11: `CoreLimits`, required-field validation and the upload
limit now live in `crates/jmap/src/core/session.rs`, the bare-handle batching
callers decode through it, and factory validation already precedes the open
probes. Upload-size zero and concurrency zero keep deliberately distinct
policies (commented there). Residue: `SessionState::derive` does not yet
validate once per installed session and cache the result, and the
`CallLimit` / request-size / concurrency readers still interpret raw fields.
The original account:

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

The JMAP-internal split is gone: `sync/mail_set.rs` is one executor for the bulk
and single-target doors, and a vanished target is `Skipped` on both. Residue,
cross-provider: JMAP answers a flag update or a move of a vanished message as
`Skipped`, while Graph absorbs a 404 as `Skipped` only for destroy and reports
`Failed(NotFound)` otherwise, so the same user action reads differently by
provider. Owner: a per-`AccountOperation` "absent target" policy in
`bifrost-types`, applied by every provider. The read-back side of "is it gone" is
BUG-021.

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

## BUG-099 - SMTP fabricates a network failure to classify a contended token cache

**Reported by:** smtp-sasl; residue after wave 11 fixed the IMAP and ManageSieve token paths.

SMTP's `oauth2_token_blocking`
(`crates/smtp/src/transport/smtp/authentication/credentials.rs`) turns
`TokenSnapshot::Contended` into a fabricated `bifrost_net::Error::RefreshFailed {
source: Network { "OAuth token cache is busy" } }` purely to obtain a transient
classification. The classification of a snapshot state belongs on `TokenSnapshot`
(or in net's projection). Token-lookup failures themselves are now classified
once, by `bifrost_net::token_source_account_error`, for IMAP, ManageSieve and
SMTP. IMAP's `Error::OAuthToken` no longer carries the SASL mechanism name.

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

## BUG-170 - Small wave 10 lateral findings across Google, Graph, JMAP and IMAP

**Reported by:** wave 10 fixers (lateral).

- Graph etag updates apply every insert before every removal, whatever the wire
  order of a repeated id, so a delete then re-add of one id within a page can
  leave the wrong final state.
- JMAP: a method response omitted entirely from a JMAP reply maps through
  `CallNotFound` to a terminal `MissingField` outside the send path, where the
  per-id lanes now report an omission as unanswered (retryable).
- IMAP: NOTIFY `MessageNew.fetch_attrs` still bypasses FETCH extension admission
  (`validate_fetch_wire`); its raw items get syntax validation only.

## BUG-171 - Small wave 11 lateral findings across IMAP, net uploads and sync

**Reported by:** wave 11 fixers (lateral).

- IMAP `account/mutate.rs`: the COPYUID diagnostic target construction can
  still encode a zero UIDVALIDITY epoch, although SELECT assembly now
  normalizes zero to absent.
- Resumable uploads (`crates/net/src/resumable.rs`, used by Google Drive and
  OneDrive): both paths accept a 200 or 201 after a non-final chunk without
  verifying that every payload byte was sent.
- Sync `recovery.rs`: documentation claims `plan_recovery` is exhaustive at
  compile time, but its helper-based classification can reach a runtime panic
  instead.
- Sync `recovery.rs::dispatch_stream_failure` stamps a 1 s
  `RetryHint::After` on every Retry and Reconcile plan, including per-item
  mutation failures (`bulk::classify_item_outcome`), so `keep_longer_advice`
  always sees a hint, and it logs once per failed item. The reconciler comment
  "skips hintless/unkeyed failures" now only describes throttle keys.
- Sync `reattach::handle_account_error` dispatches again for an error the drive
  or reconciler already dispatched before forwarding it, so holds are recorded
  twice (harmless under max-deadline merging).
- Sync proof generations: if the bounded `compacted` audit history evicts an
  entry while `proved` is empty, `DebtLedger::latest_generation` can fall back
  to an older baseline. No open debt sits above it today; nothing pins it.
- Graph: a missing Outlook token source (Outlook origin differing from Graph's)
  is refused by bifrost-net as `invalid_argument`, a caller-input Request error
  rather than a configuration lane. Unverified whether EWS streaming then ends
  push for the whole account or only drops the EWS fallback.
- `event_in_range` in `crates/caldav/src/account.rs` and
  `crates/jmap/src/sync/calendar_ops.rs` is the same test-only shim over
  `EventRange::admits`; a public window constructor would remove both.
- Graph: no account-open or factory configuration path supplies
  `GraphClient::with_outlook_source` yet, so on a cloud whose Outlook origin
  differs from Graph's, EWS streaming fallback, delegate Autodiscover and public
  folders are refused until a consumer can configure one. Wire the source
  through the factory.
- Sync `dispatch_stream_failure` rewrites every Retry/Reconcile advice to
  carry a hint, so a reader of the dispatched advice cannot tell a provider
  hint from the local 1 s floor (no current reader is affected).
- IMAP `oauth_token_error` stamps `Protocol::Imap` for ManageSieve too.
- IMAP SELECT assembly: a `UIDVALIDITY 0` (or `UIDNEXT 0`) after a valid one
  in the same response erases it; order-dependent, unlike the modseq rule.
- IMAP `native_code` for an unknown response code is now the server's own
  spelling, which gives that telemetry field unbounded cardinality.
- Google `flags::eq_flag` now compares label flags (id and advisory name)
  case-sensitively, where the whole string used to compare ASCII
  case-insensitively. Probably intended; no test pins it.

## BUG-167 - IMAP discovery still advertises DAV scopes that were excluded, and namespace-prefix failures use account scope

**Reported by:** wave 9 fixer (imap-account, lateral).

Scopes excluded by a DAV collision are now on the skip lane but are still
advertised by discovery. Separately, a failed `LIST` under a namespace prefix
is skipped with an account-level `ErrorScope`, because no scope variant names a
namespace.

## BUG-048 - IMAP sync lanes cycle on a `UIDNOTSTICKY` folder

**Reported by:** imap-account; reopened by the wave 1 review.

Inventory and changes on a `UIDNOTSTICKY` folder mint cursors the next SELECT
voids, so the scope cycles CursorInvalid -> re-inventory. Refusing the folder
(as the poller does) was tried and reverted: it would make the folder's mail
vanish from the user's view, which removes a capability. The remaining fix is a
cursor-free sync mode for such folders (re-inventory on each poll, no durable
UID cursor, no CursorInvalid loop); the inventory site and `reference/imap.md`
say so. It cannot be built inside IMAP alone: it needs a shared
`CursorEstablishment` variant in bifrost-types and engine scheduling of
repeated authoritative inventory (today `NoCursor` schedules no further poll),
plus safe hydration and mutation semantics for UID handles that do not survive
a re-SELECT. A wave of one owning types, sync and imap.

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

