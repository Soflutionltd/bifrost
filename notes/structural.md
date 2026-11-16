# Structural - design hunt, November 2026

Shape and placement: the axes that should be types, and the moves, splits and
rewrites, each with the payoff it buys. Each entry is the hunters' account,
unverified. Renaming, moving and reshaping published items is ruled fine; an
entry that would remove a capability entirely (something a consumer could do
that would no longer be possible) needs the repository owner's ruling before
anyone builds it, whatever the entry argues.

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

Each entry names the hunt scope(s) that reported it. Where hunters disagreed,
both readings are recorded. The raw reports are in the commit that precedes
this file's.

## STR-005 - `BatchItemId(String)` has three meanings

**Reported by:** types (LATENT), sync-engine.

The sync-engine hunter counted about 50 hand-minted `BatchItemId(id.0.clone())`
sites across imap, jmap, graph, google, types and the tests, and noted the
engine's per-object accounting (`outcomes: HashMap<ObjectId, MutationBucket>`)
is correct only if every implementor mints the item id byte-identically to the
submitted `ObjectId` - qualification layers such as JMAP foreign ids are where a
mismatch would appear. Its proposal: object mutations yield `ItemOutcome` keyed
by the submitted `ObjectId` (or a `BatchItemId` constructible only from
`&ObjectId`), and the campaign checks each outcome against the submitted set,
closing BUG-007 - the IMAP `Target` permit lifted into `bifrost-types` for every
mail mutation door. The sync-lanes hunter adds that `mutation/readback.rs`
asserts "`BatchItemId` is an `ObjectId`" by string coercion
(`ObjectId(success.item.0)`, `ObjectId(f.item.0)`) at four sites. The smtp-sasl
hunter adds the positional meaning's own round trip: IMAP `send_rfc5322` mints
`BatchItemId(index.to_string())` and `not_accepted_recipients` parses it back with
`id.0.parse::<usize>()`, skipping ids that do not parse; `SmtpBatchRecipient` could
carry the caller's own payload (a generic `BatchItem<T>` already exists in
bifrost-types), handing IMAP its `bifrost_types::Address` back from the lane.

Streaming mutation and hydration lanes mint `BatchItemId(object_id.0)`
(`mutation.rs::refused_mail_target`, every provider) and the engine turns it
back with `ObjectId(item.0)` in `sync/src/mutation/readback.rs` and many places
in `sync/src/engine/bulk.rs`. `PushSubscription::all_succeeded` uses
`BatchItemId(index.to_string())`, a position. Vec-batch surfaces use
caller-supplied ids checked by `validate_batch_input`. Nothing makes "the
streaming lane id is the requested ObjectId" true; the IMAP `Target` permit work
found a site stamping the hydrated id instead. Proposal: make the key a type
parameter (`ItemOutcome<K, T>` / `BatchSuccess<K, T>`), `K = ObjectId` on the
streaming lanes, a `SubmissionIndex` for positional ledgers, `BatchItemId` only
where the caller mints it.

## STR-012 - The engine's consumer API lives in the contract crate

**Reported by:** types (needs owner: moves published items).

`Control`, `DurableCheckpointSet`, `InvalidationSink`, `SinkPush`, `WatchEvent`,
`AccountControl` and `PauseReason` are produced and consumed only by
`bifrost-sync` and its consumer; no protocol crate touches them (JMAP uses
`WatchEvent` in a test). `SyncControl` is the only `Control` implementation.
They make `bifrost-types` the place an engine API change lands, and make
`events.rs` mix stream plumbing, inventory, push ingress and control. Proposal:
move them to `bifrost-sync`.

## STR-013 - Provider wire vocabularies in the neutral crate

**Reported by:** types.

`GraphSignal`, `JmapMethod`, `ImapResponseCode`, `GmailSignal`,
`EnhancedStatusCode` enumerate each provider's native error codes inside
`bifrost-types`, so adding a Graph error code changes the contract crate. Only
one central decision reads them (`smtp_reply`, CON-003). Options: a
`WireCause::Native { protocol, code }` with a provider-supplied typed code
behind a trait, or keep the enums and concede they are shared vocabulary; if
they stay, `derive_server` should key on `protocol`, not on the `WireCause`
variant.

The graph hunter adds that the tables are split across crates: Graph's
`error::classify_code` (string -> `GraphSignal`) lives in graph and
`GraphSignal::code` (variant -> string) in types, the two directions of one
bijection maintained by hand, with the `syncStateNotFound` alias in only one
direction; JMAP and Gmail have the same split (`JmapMethod`, `GmailSignal`), and
IMAP's three-table version is BUG-042. If the enums stay in types, their full
tables move with them (`GraphSignal::from_code` beside `code()`, plus a round-trip
test over every variant). It also lists `PushCapability::WebhookOrEwsStream`
(whose "OrEwsStream" is false, BUG-078) and `PushSource::GraphSubscription` as
Graph names in the neutral crate.

The google hunter shows the cost from the other side: `GmailSignal::Unknown { code
}` is used deliberately for reasons the crate RECOGNIZES - `invalidArgument`,
`insufficientPermissions`, `domainPolicy`, `conditionNotMet` and
`notAuthorizedToAccessThisResource` are routed by string match inside the `Unknown`
arm of `classify_response` - so a consumer branching on `WireCause::Gmail` sees
"unknown" for reasons Google documents; promoting them is a types change. It weighs
an opaque `(Provider, code)` plus a crate-owned typed decode, but calls the current
placement defensible if consumers branch on typed Gmail reasons, and the one place
it would not move without asking who reads it.

## STR-014 - MIME lives in `bifrost-types`, and there are two RFC 5322 renderers

**Reported by:** types.

`src/mime/` (parser, header decoder, transfer decoding, body selection,
renderer, about 4,000 lines with tests) is why the contract crate depends on
`base64`, `encoding_rs` and `jiff` for MIME. Its consumers are essentially IMAP
and Google. `bifrost-smtp` carries a second, lettre-derived builder
(`smtp/src/message/`), so header encoding, address formatting and boundary
generation are decided by two published renderers. Proposal: a `bifrost-mime`
crate (depended on by types for `Address`, or the reverse) and an explicit
ruling on how `smtp::message` relates to it.

The smtp-sasl hunter (needs owner on direction; no deletion implied) shows the two
composers already hold opposite Message-ID policies: bifrost-smtp's
`MessageBuilder::message_id(None)` generates `<36 alphanumerics@HOSTNAME>` from
`hostname::get()`, while bifrost-types' `mime::render` documents "a deterministic,
controlled-domain Message-ID (the sender's domain, never a leaked machine
hostname)". The workspace also has two address-list parsers (bifrost-smtp
`Mailbox`/`Mailboxes` FromStr with `message/mailbox/parsers/rfc2822.rs` vs
`bifrost_types::Address::parse_list`), and two Date formatters; `account_error.rs`
notes nothing in the workspace builds a bifrost-smtp `Message`. bifrost-types'
renderer is what every account crate's send path uses (IMAP, Google drafts);
bifrost-smtp's `message/` is a richer published builder (DKIM, attachments, typed
headers) used by nobody in the workspace. A plausible direction: bifrost-smtp's
builder becomes a typed facade over bifrost-types' header and body writers (it
already depends on bifrost-types), with the DKIM signer staying in bifrost-smtp, so
header encoding, folding, Message-ID, Date and Bcc stripping are decided once.

## STR-015 - `account.rs` mixes the contract with product policy

**Reported by:** types.

The trait file is about 1,750 lines and its default-impl tier encodes policy:
`dispatch_label` hard-codes a `ProtocolKind` switch (Gmail -> label membership,
Graph -> category, others -> keyword) while the next convenience,
`set_starred`, argues at length that capability dispatch is "the chosen design"
over a protocol switch. `set_starred` hard-codes Gmail's `"STARRED"` label id
and reuses `"$flagged"` as a Graph category; `mark_replied`/`mark_forwarded`
hard-code MAPI `PR_LAST_VERB_EXECUTED` codes. Proposal: conveniences in their
own module (an extension trait with a blanket impl, or free functions over `&dyn
Account`), dispatched uniformly through `ConvenienceShape` (add a `LabelShape`),
providers owning their sentinel strings.

## STR-017 - Write-only capability fields

**Reported by:** types (removal needs owner; wire-or-retire).

No reader outside producers and their tests: `cursor_freshness`,
`blob_digest_pre_download`, `rate_limit_class`, `quota_signal`,
`requires_uidvalidity_recheck`, `historyid_expires_after`,
`delta_token_expires_after` (the last two `None` in every provider).
`RateLimitClass`'s doc says "Used by `bifrost-net` and the engine"; neither
reads it. `BatchingPolicy` is read only by Graph, of its own value, and its
doc's per-protocol defaults are stale (IMAP declares 1024 items, not 5000;
Gmail's wait is 75 ms, not 200 ms). They may be consumed by ratatoskr, so the
question is wire-or-retire. The protocol-named fields are the wrong shape for a
protocol-neutral snapshot either way. The imap-account hunter confirms IMAP sets
`requires_uidvalidity_recheck = true` and nothing reads it.

## STR-018 - `IdempotencyKey` is threaded through every bulk method and read by no account

**Reported by:** types (wire-or-retire, needs owner).

No account reads `run_id`, `sequence` or `protocol_salt`; the salt's string
payloads are always empty. Its only function is engine bookkeeping, which need
not cross the account boundary.

## STR-042 - `PendingCoverage` is five subsystems behind one lock and one public surface

**Reported by:** sync-lanes.

Residue. The guarded state is split into sub-structs (`Claims`, `RingCharge`,
`Fences`, `LossLedger`, `PublicationMint`) under one mutex, and the ordering-
sensitive methods are `pub(crate)`. Not done: the named transition facade
(`register_page`, `register_marker`, `acknowledge`, `retire`,
`invalidate_scope`, `on_receiver_read`, `on_receiver_departed`) was not built,
so the crate-internal surface is still about forty methods; and walk-generation
minting (`PendingCoverage::next_generation`, still `pub`) did not move to the
writer. Worth doing only if a new ordering defect shows the wide internal
surface is the cause.

## STR-046 - A partitioning framework with no producer imposes its costs on every consumer

**Reported by:** sync-lanes (published; not a deletion recommendation).

`BackfillPolicy`, `BackfillStrategy`, `PartitionPlan`, `PartitionBounds`,
`plan_time` / `plan_uid` / `plan_page`, `OpenPages`, `parse_page_partition`,
the page tie-break in `get_backfill`, and `open_pages_resume` all serve
`TimeWindowed`, `UidRange` and `PageCount`; no in-tree provider returns any of
them. `BackfillPolicy` is documented as "configurable per account" but never
configured (`backfill_plan_for` builds `BackfillPolicy::default()` or a literal
with `clock_skew: Duration::ZERO`). `BackfillStrategy` duplicates
`InventoryPartitioning` and `PartitionBounds` duplicates `InventoryPartition`
(using a `Timestamp::MIN` sentinel for "open" where `InventoryPartition::Time`
uses `Option`). The hunter's narrower points: the chain
`InventoryPartitioning -> BackfillPolicy -> plan -> PartitionBounds ->
InventoryPartition` has two redundant middle layers, and collapsing it to
`InventoryPartitioning -> Vec<InventoryPartition>` (or `OpenPages`) in
`backfill/plan.rs` keeps every behaviour (owner ruling for the middle types);
and the durable cost of the unexercised machinery (the selection rule every
store implements, the time-key compaction contract) is paid now by every backend
for producers that do not exist yet.

## STR-062 - Protocol-value behaviour lives in IMAP `account/`, and account identity lives in `types/`

**Reported by:** imap-protocol, imap-account.

The set algebra, the FETCH merge and the `~imap-wire:` mailbox identity moved
to their owners. What remains is the registry side: `FolderEntry` bundles LIST
facts, ownership, MYRIGHTS, a cursor hint, a MODSEQ cache and a last-seen
instant behind four locks, and is the implicit owner of folder roles; making
roles, ownership and the shared-error mapping explicit registry
responsibilities (BUG-028, BUG-046) gives those decisions a home.

## STR-071 - The IMAP pipeline has no production caller yet shapes the driver's hardest invariants

**Reported by:** imap-connection (crate-private, but removal is an owner decision; flagged, not recommended).

`ImapConnection::pipeline()` (about 1,700 lines) has no caller in `account/` nor
anywhere outside its own tests. It shapes the driver: `PipelineRouting` with
`sending: Option<usize>` as an exclusive ownership bound,
`route_pipeline_response` called from inside the sender's continuation wait,
`correlated_esearch_owner`, `group_into_sub_batches` driven by
`CommandKind::untagged_routing_key` (CON-032), the second NOTIFY pre-effect copy
(BUG-035), and `SendOutcome::Rejected` / `ContinuationOutcome::Rejected` arms
unreachable outside it. `pipeline/mod.rs` carries an "audit boundary: not
line-audited" note. It bypasses every handle gate (BUG-029) and waits unbounded
(BUG-041). Options: (a) keep it and route it through the request/admission layer
(`connection/request.rs`, BUG-029) so it stops being a gate bypass; (b) remove it and simplify
`wait_for_continuation` to one mode, dropping `PipelineRouting`,
`untagged_routing_key` and the router.

## STR-078 - Credentials and TLS mode, three copies each

**Reported by:** imap-account, smtp-sasl, net-dav.

The net-dav hunter adds the HTTP side: `CalDavCredentials` and `CardDavCredentials`
are identical published enums, both converting into `DavCredentials`, identical
again; JMAP's Basic credential stores a pre-encoded value while DAV stores username
and password and encodes per request. A shared `HttpCredential` in bifrost-net would
carry the token-coordination rule (BUG-111). Removing or renaming the published
enums needs the owner; reshaping them into wrappers does not.

The smtp-sasl hunter details the three credential types: `bifrost_smtp::Credentials`
(public enum, `password: Zeroizing<String>`, re-exporting `zeroize::Zeroizing`),
`bifrost_imap::Credentials` (struct over a private `CredentialsKind`, using
`SecretString`), and `bifrost_imap::SubmissionCredentials` (public, `password:
String` - plain, not zeroizing); `resolve_credentials` copies between them with
`password.as_str().to_owned()`. All model the same two shapes (password; OAuth
identity plus token source). A shared `SaslCredentials` in bifrost-sasl (which
would need `TokenSource`, so either sasl depends on bifrost-net or the type lives in
bifrost-net's auth module) is the single owner; the public SMTP and IMAP types need
the owner, but at minimum `SubmissionCredentials::Password.password` should be a
secret type.

Credentials: `crate::types::Credentials` (with `CredentialsKind`),
`SubmissionCredentials` (password a plain `String`), and
`bifrost_smtp::...::Credentials`, translated structurally in
`resolve_credentials`. The token-source type already lives in `bifrost-net`; one
credentials type there or in `bifrost-sasl` would give IMAP, SMTP, ManageSieve and
DAV one secret-handling story. TLS mode: `SubmissionTls { Implicit, StartTls,
Plaintext }`, `ManageSieveTlsMode { Implicit, StartTls, None }`, and
`ImapConfig`'s own, each with its own default-port table.

## STR-082 - IMAP does not partition inventory, so large folders get no bounded backfill

**Reported by:** imap-account (not a bug; a gap between vocabulary and its intended producer).

`inventory_partitioning` is not overridden, so IMAP folders inventory as one `UID
FETCH 1:*` that also accumulates every UID in a `Vec<u32>` before minting the
cursor. The half-open `u64`-ended `InventoryPartition::Uid` vocabulary was
designed with IMAP in mind (`types.md` says as much), and the bounded backfill
lane depends on partitions, but no production crate mints a `Uid` partition.
Related: STR-046.

## STR-091 - Delete JMAP's `HttpTransport` generic; it is a test seam above `AccountNet`

**Reported by:** jmap-protocol (its top-ranked move; "the largest single simplification available in the crate").

`core/transport.rs::HttpTransport` has one production implementor
(`ReqwestTransport`) and about thirty-five test implementors across `client.rs`,
`core/tests.rs`, `blob/`, `event_source/`, and twenty more in `sync/`. Each double
stubs `upload`, `download`, `get_session` and `api_request` and returns
`TransportError::new("...")` with no `net` evidence - exactly the shape
`reference/net.md` says each crate grew and should not have. Consequences:

- A classification path only tests reach: `sync/error.rs::convert_transport`'s
  "custom transport without bifrost-net evidence" branch classifies those errors
  as `Transport(Network)`; production never produces a `TransportError` without
  `net` or `local`. The doubles exercise a copy of the status contract instead of
  bifrost-net's - the situation `bifrost_net::test_support` was published to end.
- The generic is why `JmapAccount` is "undrivable": the comment above
  `MailAccount` in `sync/account.rs` says no scripted transport can be installed
  at that level and closing it "means threading the transport generic through
  `JmapAccount`". It does not: `ReqwestTransport::scripted_with_retry`
  (cfg(test), in `transport_reqwest.rs`) already builds the production transport
  over `ScriptedDispatch`, and `capability_contract_tests.rs` already constructs a
  real `ReqwestTransport` for a whole `JmapAccount`. A scripted net there makes
  every `Account` door drivable hermetically through the real retry budget, replay
  policy, concurrency gate and bandwidth meter.
- Three traits for one transport: `SseTransport` and
  `blob::download::BlobTransport` exist so the generic can be partially
  implemented; both have only the reqwest implementor.
- Every type carries `T`: `Client<T>`, `Request<'x, T>`, `Account<Tr>`,
  `MethodTuple::add_to_request<T>`, and every generic helper in `sync/`.

Proposal: make `Client` concrete over `ReqwestTransport` (or `AccountNet`
directly), delete the three traits, `convert_transport`'s no-net branch and the
doubles, and port tests to `scripted_net`. The WebSocket push lane keeps its own
seam (`sync/push.rs::PushTransport`), the genuinely non-HTTP part.

**The jmap-account hunter reached the opposite remedy for the same symptom.** It
reports that `JmapAccount` hardwires `MailAccount = Account<ReqwestTransport>`
because WebSocket push is reqwest-specific, and that as a consequence
`capability_contract_tests.rs` can only check the false direction;
`route_object_id`, `route_mutation_target`, `draft_route_account` are generic over
a handle type `H` "purely so they can be exercised"; `foreign_account_id_for_object`
exists only under `#[cfg(test)]` as a test pin for a rule production implements
elsewhere (`route_object_id`) - a copy kept alive by its own test; and the account
methods are one-line delegations "to keep the pin meaningful", which is why logic
lives in free functions taking six to eight parameters. Its proposal: make
`JmapAccount<T: HttpTransport>` generic (with `Client<T>: PushTransport` as a
bound, or push behind its own injected `Arc<dyn>` seam), as the code comment
estimates, so whole-`Account` tests replace the scaffolding. The jmap-protocol
hunter's reading is that the generic is unnecessary because a scripted
`ReqwestTransport` already exists; both readings agree the current pin is what
makes `JmapAccount` hard to drive.

## STR-119 - Graph event and contact sync lanes have no scope producer

**Reported by:** graph (needs a ruling, not a deletion: the published-API rule applies to the behaviour these lanes promise).

`scopes::discover_cursor_scopes_inner` emits only `ObjectType::Email` folder scopes
plus public folders, and the engine drives only discovered scopes. Reachable only
through a scope nothing in production discovers: `GraphCursorKind::Events` and
`Contacts`; the calendarView and contacts delta paths;
`CursorError::CalendarWindowExpired` and the reseed logic; `EVENT_DELTA_PREFER`;
`resource_for_scope`'s events and contacts arms; `sync_object_id`'s event and
contact arms; and the inventory tests for all of these. Either calendar and contact
folder discovery is missing, or the lanes are built ahead of a producer; either
way a large surface compiles and is tested with no path to it.

## STR-129 - Google's mailbox watch registry is process-global

**Reported by:** google.

`push::MAILBOX_WATCHES` is a `static LazyLock<Mutex<HashMap<String, Weak<..>>>>`,
process-wide rather than scoped to the `Net` or the factory; the tests mint unique
mailboxes because of it (`NEXT_MAILBOX` in `mod.rs` tests). bifrost-net already
offers Net-scoped string-keyed registries (`shared_hold`,
`shared_concurrency_limit`) for this kind of cross-account coordination; the
coordinator belongs in one, keyed by the typed mailbox identity (STR-124), so two
independent `Net`s really are independent.

Blocked on bifrost-net: its hold and concurrency registries cannot store
arbitrary provider state, and `AccountNet` exposes no Net-owned slot for it. The
move needs a Net-scoped provider-state store (a typed, keyed slot owned by the
`Net` and reachable through `AccountNet`) first; the registry site records this.

## STR-138 - Smaller SMTP axes and placement notes

**Reported by:** smtp-sasl.

The reply-mode enum, `MechanismChoice`, the credentials and challenge-framing
split, and the shared message-error strings landed. One residue needs a
bifrost-net API first: `client/metering.rs`'s blocking admission loop belongs
beside `BandwidthBudget` in bifrost-net as a blocking budget API, which does not
exist yet. (The rest of `authentication.rs` moving to bifrost-sasl is STR-135.)


