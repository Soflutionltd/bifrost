# bifrost-types reference

`bifrost-types` is the protocol-neutral contract crate shared by every
account implementation and by `bifrost-sync`. It contains no transport
and no engine. Protocol crates implement `Account` and
`AccountFactory`; consumers program against the types in this crate.

The shared structured error contract is documented separately in
`reference/error-model.md`.

## Protocol identity

The identity vocabularies describe different domains. `ProtocolKind` is the
set of protocols that mint opaque change cursors; `Protocol` also names
submission and auxiliary APIs and has `Unknown` for unavailable identity.
`From<ProtocolKind> for Protocol` owns their exhaustive conversion here.
`ProtocolSalt::from(ProtocolKind)` owns default mutation bookkeeping salts,
also exhaustively. Custom salt factories retain their payload-bearing variants;
empty payloads are a default, not a restriction on consumers.

`Provider` names a vendor or vendor API, including the expression language of
`FilterCondition::ProviderExpression`. A vendor expression is not cursor
provenance. `PushSource` identifies a delivery mechanism, including synthetic
coalescing. DAV's private `DavProtocol` selects a closed dialect policy and its
resource kinds; its exhaustive conversion belongs beside that policy.

`Account::protocol()` is required and names the primary account protocol for
inherited refusals. Implementors can explicitly return `Unknown` when no
identity is available. It does not replace per-error protocol/provider stamps:
Google Calendar and People failures differ from Gmail failures on one account,
and IMAP can compose DAV and SMTP legs. An enterprise user needs the failing
service identified rather than every failure attributed to their mail service.
Cursor provenance likewise remains the source of identity for errors about a
particular checkpoint. A blanket account context would overwrite this evidence.

Cursor and ledger codecs keep their historical protocol tags independently.
Graph is tag 3 in a cursor and tag 6 in a ledger; semantic conversion never
converts those persisted bytes or changes either format.

## Async object-safe surface

`AccountFuture<T>` and `AccountStream<T>` are boxed, `Send`, and
`'static`. `Account` and `AccountFactory` are object-safe and used as
`Arc<dyn Account>` / `Arc<dyn AccountFactory>`. The workspace is
async-only.

`AccountFactory::open(account_id)` creates one live account handle,
returned as `OpenedAccount { account, skipped_scopes }`. The skip lane
names the parts of the account's discovered surface open could not
bring up and left out of the handle - a foreign JMAP account whose
seeding probe failed, an IMAP-composed DAV sub-account that did not
open, a failed Graph delegate-Autodiscover pass - each with its
classified `AccountError`. The lane exists because both alternative
shapes are wrong: failing the whole open blocks the user's primary
mail on someone else's shared mailbox being down (initial attach does
not retry `factory.open`), and skipping silently erases the share for
the session. `Err(_)` from `open` therefore means the PRIMARY surface
is unavailable. Like `Page`, `OpenedAccount` is deliberately not
`#[non_exhaustive]`: every factory constructs it, so a future lane
breaks every constructor.
`Account::close()` is idempotent local teardown. It closes protocol
connections and workers. It does not delete durable server-side push
subscriptions unless the provider documents that it does (Graph deletes its
webhook rows); explicit teardown uses `push_unsubscribe`.

## Account trait tiers

The trait is intentionally broad. Its methods group by independent lane as
follows:

| Lane | Methods |
|---|---:|
| capabilities, provider metadata, and scheduling controls | 5 |
| cursor discovery, lifecycle, inventory, hydration stream, and changes | 12 |
| push | 4 |
| blob and raw-message reads | 3 |
| streaming bulk mutation | 8 |
| mail mutation primitives | 8 |
| compose, attachment, draft, and scheduled-send operations | 10 |
| search | 2 |
| containers | 5 |
| settings | 5 |
| filters | 5 |
| contacts and directory | 10 |
| calendar | 9 |
| typed thread and message hydration | 2 |
| convenience operations | 11 |
| close | 1 |

Push, contacts and directory, calendar, filters, and settings are genuinely
independent optional lanes. They are already separated at the behavioral
boundary by `AccountCapabilities`, structured `Unsupported` results, and
default implementations where an implementation can safely be shared.

A supertrait split is object-safe, but does not reduce the cost that motivated
the audit. `Arc<dyn Account>` must retain every lane because this is the single
consumer handle, so `Account` would have to inherit every new supertrait. Each
existing implementation would then be divided into many impl blocks, and a new
lane would still change the main trait bound and every implementor that does not
receive a blanket implementation. Narrow optional traits exposed through
accessor methods avoid that bound, but add one accessor per lane, duplicate the
capability decision at runtime, and make consumers branch between capability
metadata and a second dynamic-trait presence check. Blanket forwarding traits
merely duplicate the 94 signatures and leave additions centralized on
`Account`.

The audited decision is therefore to keep one object-safe trait. The method
count is real but is not currently causing mechanical duplication: optional
lanes have defaults, required sync methods are coupled by engine invariants,
and consumers need one erased handle spanning all advertised capabilities. A
future split should be triggered only if a consumer can operate on a narrower
handle without retaining `dyn Account`; that would remove a dependency rather
than rearrange the same one.

The required protocol surface in `account.rs` covers:

- capability and scheduling controls;
- cursor-scope and membership discovery;
- lifecycle, inventory, partitioned inventory, and changes streams;
- hydration and blob access;
- push subscription CRUD and the in-process push stream;
- bulk mutation primitives;
- compose, draft, search, container, filter, settings, contact,
  directory, and calendar operations;
- typed message and thread hydration;
- graceful close.

Default implementations form the convenience tier. They either:

- dispatch through capability shapes, such as `set_starred`,
  `mark_replied`, `mark_forwarded`, and `mark_mdn_sent`;
- dispatch by identifier provenance, such as `apply_label` and
  `remove_label`;
- compose aliases, such as `set_read` over `set_is_read`;
- adapt a broad request to a narrower result, such as autocomplete;
- return a structured `Unsupported(AccountOperation)` when a safe
  object-safe default cannot compose multiple async calls.

`move_thread` and `delete_thread` are in the last group and report
`AccountOperation::MoveThread` and `AccountOperation::DeleteThread`.
Protocol crates with an internal `Arc<Self>` handle override them.

## Capabilities

`AccountCapabilities` is the declarative dispatch contract. Important
fields include:

- `cursor_freshness`;
- `push: PushCapability`;
- mutation concurrency, replay safety and reassertion (below);
- batching and rate-limit hints;
- blob-range support;
- protocol expiry hints;
- `pim_methods` for optional compose, contact, calendar, filter, and
  settings methods;
- `filter_rule_shape`;
- `conveniences`, including starred, replied, forwarded, and MDN
  dispatch shapes;
- rediscovery-time foreign namespace discovery
  (`discovers_foreign_namespaces_on_rediscovery`): discovery POTENTIAL, not
  current membership. True iff re-opening the account can surface
  shared / other-user namespaces granted after the last open (the
  account discovers its foreign surface only at open and no provider
  signal rules such namespaces out). IMAP derives it from NAMESPACE;
  JMAP is constitutively true; configuration-driven foreign surfaces
  (Graph, Gmail delegation, DAV) are false. Consumers act on it by
  scheduling `SyncEngine::reattach` at a cadence of their choosing.

Consumers should inspect capabilities before presenting an operation.
An implementation must still return a structured error if an advertised
operation fails or support changes at runtime.

`PimMethodSupport` remains the published snapshot. Its declaration generates
both its existing fields and `gate(operation, protocol)`, which returns the
canonical `Unsupported(operation)` for a disabled method. IMAP's snapshot
gates consume this helper. Operations without a PIM field are refused by the
helper, so it is not a general Account dispatcher. Provider prerequisites and
the false-direction capability contract tests remain necessary.

The mapping is deliberately not a method/field/operation bijection. `Send`
checks ordinary sending only; `send_as` can be true while `send_message` is
false. `scheduled_send` gates two scheduling methods and a request field;
`directory_enumerate` gates the empty-query path of directory search. Required
Account method declarations, composing convenience defaults and operation
variants therefore remain explicit rather than generating default refusals
from the flag list. A generated refusal would not gate provider overrides or
prove support, and replacing composing defaults would lose behavior.

`compose::classify_send_as_rejection` owns the flag-selected rejection for
Graph and JMAP: unavailable means Unsupported(Send) with SendAsUnavailable;
available but unroutable means Request(Malformed) with the `send_as.mailbox`
field pointer. Protocol identity and provider diagnostic text survive. Routing
and refusal order remain provider-owned, including JMAP's foreign scheduling
and per-share submission checks. An Outlook user should be asked to correct
a mailbox only when that account can send as another mailbox, and a rejected
route must never become a personal send.

`MutationCapabilities::reassertion` is a `MutationReassertion { set_flags,
destroy }`, default off (`MutationReassertion::NONE`). Declaring a door says
it may be reapplied, after a valid observation shows the target unsatisfied,
to the same object: it requires stable, never-reused object identity and a
door whose reapplication converges on the requested state without a second,
different side effect. It authorizes desired-state reassertion only. It is
not replay-token deduplication (`replay_safety`) and supplies no lost-update
guarantee: a reassertion may overwrite a concurrent writer's later change.
Moves have no field and are never reasserted.

## Cursor scopes and memberships

`CursorScope` answers where change state advances:

- Gmail: `Account`;
- JMAP: commonly `Type(ObjectType)` or `Query(QueryId)`;
- IMAP: `Folder(FolderId)`;
- Graph and DAV collections: `FolderType { folder, ty }`.

`MembershipScope` answers where an object belongs:
`Folder`, `Mailbox`, `Label`, `Query`, or `Owner(PrincipalId)`. The two enums must not be
collapsed. One account-wide cursor can cover many memberships, and one
object can have several memberships.

`MailboxId` identifies a native mail container. `PrincipalId` identifies its
owner or send-as route; `Container::owner` and `SendAs` carry `PrincipalId`.
These identities remain distinct even when their strings match. An enterprise
Outlook user opening a share expects its owner to stay separate from a personal
folder with the same server id.

`cursor::ScopeShapes` and `CursorScope::{covers, removal_projection}` own the
membership topology exhaustively in this crate. `ScopeShapes::scopes_for`
generates only advertised per-membership shapes that cover the membership.
`MembershipScope::folder_id` is the explicit native Mailbox-to-Folder bridge;
`canonical` supplies container index keys; `same_container` supplies the same
identity comparison for readback and removal
consumers. Owners never cross that bridge or generate lifecycle folder cursors.
They are annotations covered generically by account-wide cursors only;
a narrow owner hint needs provider knowledge and otherwise uses the engine's
existing unknown-membership fallback. No owner is inferred from an opaque
folder string. Mailbox memberships admit Email/Mailbox/Thread types; labels
admit Email/Thread. Folder and query memberships carry no object type,
so a type-wide cursor conservatively covers them. A type cursor cannot claim
an owner: JMAP primary Type(Email) and foreign-account folders coexist, and
linking a foreign owner to the primary cursor would miss its changes.
Removal projections destroy objects for account/type scopes and remove canonical Folder/Query memberships
for narrow scopes. Compare a canonical Folder removal with native mailbox
membership using `same_container`.

`ChangeCursor` combines a scope, protocol-tagged opaque server state,
an optional progress marker, and an outer envelope version. The outer version
comes from `CHANGE_CURSOR_ENVELOPE_VERSION`; `validate_envelope` is the gate the
engine applies before dispatching a stored cursor to a protocol account, and it
is a strict equality check, never a range. Migration of an older persisted
layout belongs entirely to `wire::cursor`'s envelope decoder, exposed through
`bifrost-sync`'s storage API, which stamps the current version on the way out.
A `ChangeCursor` a protocol crate is handed is always at the layout that crate
compiles against. `ChangeCursorEnvelopeMismatch` reports `expected` and `found`.
`OpaqueChangeState::envelope_version` is `PayloadVersion`, while the outer
change cursor and backfill checkpoint use `TraitEnvelopeVersion`. Their private
representations prevent substituting one version axis for another. Change and
backfill deliberately share their outer storage layout and version axis.
`OpaqueChangeState::open` validates the expected protocol and exact payload
version before a protocol decoder reads bytes. `CursorDecodeFault::builder`
owns the recovery policy described in `reference/error-model.md`; providers
supply only fault identity, operation, scope and diagnostic decoration.
`CursorEstablishment` is either `Ready(ChangeCursor)` or
`EstablishViaInventory`. In the latter case the terminal inventory
`Done` event carries the new change checkpoint.

## Stream vocabulary

`SyncEvent<T>` is the common stream protocol:

- `Batch(Batch<T>)`;
- `Progress`;
- `Warning`;
- `Done(Option<Checkpoint>)`;
- `Terminated(AccountError)`.

A batch carries items, page-boundary metadata, wire observations, and
an optional checkpoint. The item set and checkpoint are one consumer
transaction. A checkpoint must not be persisted unless the matching
items were persisted. `PageBoundary::Partial` carrying a checkpoint is the
one combination that cannot describe such a transaction, and `try_new` /
`validate_boundary` name it on both `Batch` and `InventoryBatch`
(`BatchBoundaryError::PartialCarriesCheckpoint`).

`InventoryBatch::removed` names objects removed from the inventory scope.
Entries, removals and the accepted checkpoint form one consumer transaction.
Folder and query removals remove membership; account and type removals destroy
the object. The constructor and `lift_complete_walk` initialize this lane empty;
providers with inventory tombstones fill it explicitly. `ObjectChangeKind::Updated`
is an upsert for both known and unknown ids; `Created` is an optional precision hint.

An inventory page or completion carries a `PageCheckpoint` rather than an
`Option<Checkpoint>`: `None`, `Advance(checkpoint)`, or `Withheld(Option<..>)`.
The variant is a function of the coverage beside it - `Withheld` exactly when
that coverage holds a checkpoint barrier - so a barrier page cannot be read as
an ordinary advancing one, which an `Option` could not prevent: it could not
tell "no checkpoint" from "a checkpoint the barrier forbids". `PageCheckpoint::new`,
`InventoryBatch::try_new` and `InventoryCompletion::new` derive the variant; a
literal that disagrees is `BatchBoundaryError::CheckpointDisagreesWithCoverage`
from `InventoryBatch::validate_boundary` or `InventoryCompletion::validate`. The
only way to take a position is `accepted(barriers_waived)`, which makes the
caller state whether an operator waived the barriers; `offered()` ignores the
barrier and is for diagnostics only.

`try_new` is a convenience, NOT a gate. `Batch`'s fields are public and stay
public - every protocol crate constructs one literally, and making them
private would delete published fields - so an implementor can always build
the nonsense shape without touching the constructor. The invariant is
therefore enforced where it can be: `bifrost-sync` validates every batch and
inventory completion it receives at the account boundary and, on a violation,
terminates the scope with a classified `Internal(AccountContract)` whose
recovery is `InternalFailure` - the `Account` implementation minted the shape,
so the provider is not blamed. That class is terminal, so the engine stops the
scope and publishes `SyncEvent::Terminated` instead of re-polling the
unchanged cursor forever. A guard that only refuses the batch would trade a
nonsense state for a silent non-progressing scope.

`ScopeLifecycleEvent` carries created, renamed, and deleted membership
events, `Polled` after a fully successful poll and state commit, or a
structured termination. Providers own successful poll cadence and end on
their first failure; engine reconnect backoff resets only on `Polled` or
a lifecycle change, and carries its count across account replacement. `WatchEvent` carries invalidation,
connection-health, warning, and termination signals. `InvalidationSink`
is synchronous so an out-of-process webhook or Pub/Sub receiver can
feed the engine from a non-async thread. Its `SinkPush` result returns refused
directives for resubmission after a pause or queue saturation. Invalidations
are retained as bounded intent and may widen into a full reconcile; controls
are handled synchronously and may overtake pending invalidations.

`Control::pause` and `Control::checkpoint_now` return
`Result<DurableCheckpointSet, AccountError>`. The set describes the account's
durable checkpoint boundary across cursor scopes and backfill partitions; an
empty set means no durable checkpoint has been produced.

### Inventory ranges and completeness

Every range in `InventoryPartition` and `CoverageCoordinate` is half-open,
`[from, to)`. `Uid` and `UidRange` used to be inclusive while `Time` and `Page`
were not; one enum with two conventions is an off-by-one magnet for every
implementor, so `Uid` is now half-open like the rest. Its endpoints are `u64`
so that an exclusive end of `u32::MAX + 1` is representable; the UID values
themselves remain `u32`.

A completeness claim always names its extent. `InventoryCompletion::complete`
and `InventoryCoverageReport::complete` take a `CoverageDomain`, not a
`CursorScope` - there is no scope-only shortcut anywhere in the crate, because
a partition walk that passes `CoverageDomain::full(scope)` discharges the
coverage debt for the whole scope on the strength of having enumerated one
window of it. `unsupported_inventory_stream` accordingly emits only
`Terminated`; a refusal proves nothing and now claims nothing.
`CoverageOutcome::Degraded` carries `NonEmptyInventoryObligations`, so an empty
ledger is represented only as `Complete`; `InventoryCoverageReport::degraded`
funnels through the same rule.

An `InventoryCompletion` also DECLARES whether the enumeration continues past
its partition: `PartitionEnd::Exhausted` or `MoreBeyond`. An open-ended page
walk stops on the declaration and never on an entry count, so a short or empty
window says nothing by itself. A whole-scope walk is `Exhausted` by definition.
`lift_complete_walk`, for producers whose `SyncEvent` stream cannot declare, is
the one place that infers it: `Exhausted` for a whole-scope domain, and for a
partition domain exactly when the stream yielded no entry at all.

`Fingerprint::flags_hash` is produced by `canonical_flags_hash` and by nothing
else. Producers with no flag set pass an empty iterator (the empty set has a
defined, non-zero hash) rather than hard-coding `0`; producers of objects with
tracked state but no flags encode that state as `key=value` pseudo-flags. The
value is comparable within one provider and object namespace only.

An inventory representation is "changed" when ANY field of `InventoryEntry`
differs: id, membership set, blob id, fingerprint, thread id, message id,
references, or in-reply-to. `InventoryEntry::differs_from` is the canonical
comparison. Its exhaustive destructuring forces a decision for every new field.
Membership order and duplicate memberships do not change the set. Size lives
only in `Fingerprint::size`; Graph leaves it absent. Comparing `Fingerprint` alone is insufficient: JMAP mailbox
membership can change without keyword state changing.

## Mutation outcomes

`Flag` owns flag identity and retains the original spelling for provider writes.
`FlagSet` uses that identity for membership, equality, insertion and set
operations; iteration yields immutable `Flag` values, preserving both spelling
and identity through collection and cloning.
`FlagOp`, `HydratedObjectKind::FlagsOnly`, `Message::flags` and
`Account::flag_target` all carry `FlagSet`. The single-keyword Account door
and filter set/clear-keyword actions carry `Flag`.

Ordinary keywords compare ASCII-case-insensitively, so `\Seen` and `\SEEN`
occupy one member. `Flag::literal` explicitly preserves opaque provider identity;
string prefixes never select an identity rule. Synthetic Gmail label tokens
use this constructor and retain exact spelling, including opaque ids and
advisory names; the provider judge still resolves renamed labels by id. Graph category flags, including their `category:` prefix, compare
ASCII-case-insensitively while retaining the category name sent to the provider. These rules preserve the
providers' existing write vocabulary: an enterprise Outlook user must not edit
a distinct label because its opaque id differs only in case. Fingerprinting, validation
and generic read-back use this same identity. Gmail label fingerprints now
distinguish case-only label changes; previously persisted fingerprints can
therefore produce a one-time update without requiring cursor migration.

`FlagOp::validate` is the account-boundary guard for flag mutations. Empty
add/remove deltas and an entirely empty patch are invalid; `Set(empty)` means
"clear all flags". A patch may not name the same flag in both sets under
`Flag` identity. Invalid operations are caller errors and must
be rejected before provider I/O.

`Account::flag_target(target, observed, op)` judges one complete `FlagsOnly`
observation against a flag operation and returns a `FlagTargetState`:
`Satisfied`, `Unsatisfied`, or `Indeterminate`. It is pure and judges in the
provider's own write vocabulary; a token the write does not model or the
observation cannot see is `Indeterminate`. The default is
`flag_target_state`: an invalid operation is `Indeterminate`, and flag
identity comes from `Flag`, so `Set({"\Seen", "\SEEN"})` equals an
observed `{"\seen"}`. Providers whose write translates flags override it.

The judge relies on a `get_stream` contract: a `FlagsOnly` success carries
the object's whole flag state. When a provider cannot establish that from
the response, the item is a per-item `Failed(Protocol(MissingField |
ParseFailed))`, never an empty set. A wire that legitimately omits an empty
flag collection (Gmail's `labelIds`) reports the empty set it means.

Batch and streaming mutations use the same closed three-lane model:

- succeeded;
- failed;
- uncertain.

`BatchOutcomeBuilder::finalize` enforces that every submitted
`BatchItemId` appears exactly once, counting repeated ids as repeated
submissions. A whole-operation `Err` means
nothing was transmitted. `Ok(BatchOutcome)` means every item is
accounted for. `ItemOutcome::Uncertain` means the write may have landed
and must be read back rather than replayed blindly.

`AccountOperation::is_idempotent` is the default retry input for the error
model. Absolute-state writes are idempotent. Sends, creates, moves,
destroys, uploads, and other side-effecting writers are not. A producer
can narrow it per request with `idempotency_override(false)`: bifrost-net
does so when a request whose replay policy forbids replay may have
committed. The operation table alone, never the override, decides whether
`PartialResponse` also carries `DedupeByClientId`
(`reference/error-model.md`).

## Paginated results

`PageCursor` (`page.rs`) is the opaque continuation type for `Page::next_cursor`,
all page-taking `Account` methods, and the search/contact/calendar request
shapes. It is distinct from bytes, change cursors and inventory progress.
Consumers pass it back to the same surface; `as_bytes` / `into_bytes` and
`from_bytes` support storage, and transparent serde preserves the byte-array
representation. Debug output reports only its length. Restoring bytes does not
validate them: the owning provider does that when resuming. Graph and Google
use versioned surface tags and refuse foreign or retired formats through
`AccountError::invalid_page_cursor`, so an enterprise user's stale results
reload without clearing durable account state. Other providers retain their
existing payload formats and validation.

`Page<T>` (`page.rs`) is the envelope `search`, `search_messages`, and the
other paged list surfaces return: `items`, an opaque protocol-owned
`next_cursor`, an optional `estimated_total`, and two degradation lanes
with distinct namespaces. `failed_ids` names RESOURCES (item-namespace
native ids) the provider fetched but could not materialize.
`skipped_scopes` names SCOPES a multi-scope walk quarantined instead of
visiting - each `SkippedScope` carries a mandatory `ErrorScope` (which
scope went unsearched) plus the classified `AccountError` that caused the
skip (for Graph, a shared mailbox whose delegate access was revoked:
terminal `NoPermission`). A skip entry is advisory - the walk continued
and `items` remain valid - but absence of results from a skipped scope is
not evidence of absence. Both `Page` and `SkippedScope` are deliberately
NOT `#[non_exhaustive]`: protocol impls construct them directly, and a new
lane must break every constructor so each one answers the new question
instead of silently defaulting it.

`SkippedScope` is the shared degradation vocabulary, not a search-only
one. Two sibling envelopes reuse it under the same
not-`#[non_exhaustive]` rule: `OpenedAccount` (`account.rs`), the
`AccountFactory::open` result described above, and `ContainerList`
(`container.rs`), the `containers_list` envelope pairing the
materialized containers with the namespaces a multi-namespace
enumeration skipped instead of listing. In all three the skip is
advisory - the returned data remains valid - and the classified error
lets a consumer distinguish a transient outage (a retry or reopen
heals it) from a revoked grant.

## Provenance and container vocabulary

Identifiers are typed newtypes. `Container`, `Label`, and related
values preserve provider-native provenance so convenience defaults can
choose the correct primitive:

- Gmail engine labels route through label membership;
- Graph categories route through category mutation;
- JMAP and Graph native container ids route through container
  membership;
- keyword-like shapes route through keyword mutation.

`Container::native_id()` borrows the native string from its private
provenance, so there is no independently mutable duplicate. `provenance()`
reads the metadata and `with_provenance` replaces it together with the native
id. The public `kind` remains a presentation classification, independent of
`provenance().kind`, which records the wire classification. A consumer can
present a native label as a folder without changing its protocol identity.
`Container::new` retains its existing arguments and every metadata builder.

The `"$flagged"` category sentinel is reserved by the convenience
dispatch contract for providers whose starred state is represented by
a category.

## Hydration and blobs

`Projection` controls bulk `get_stream` hydration. Typed
`HydrationProjection` drives single-message hydration.
`HydratedObject`, `Message`, and `ThreadHydration` keep parsed content
separate from inventory and change signals. `HydratedObject::blobs` remains
the engine-side `BlobHandle` shape. User-facing `Message::attachments` uses
`MessageAttachment`: `AttachmentSource::Blob` is an on-demand provider
handle, `AttachmentSource::Inline` is decoded RFC 5322 content already
carried by a hydration response, and `AttachmentSource::None` is metadata
from a provider with no redeemable attachment handle. `Message::incomplete`
marks a parser-limited body or attachment so consumers do not present a
prefix as a complete message.

Three unrelated "attachment" families live in this crate and only the naming
suggests kinship:

- `compose::AttachmentHandle` / `compose::AttachmentInline` - OUTBOUND, what a
  caller hands the serializer to send.
- `hydration::MessageAttachment` / `AttachmentSource::Inline` - INBOUND, what a
  hydrated `Message` carries to a consumer.
- `mime::DecodedAttachment` - the MIME selector's intermediate attachment
  representation, which consumers can project into `MessageAttachment`. It
  knows nothing about blobs.

`AttachmentInline` and `AttachmentSource::Inline` are not related.

Blob methods return byte streams. `open_blob_range` is only usable when
advertised by `blob_range`; `open_raw_rfc822` returns verbatim
server-assembled message bytes.

`Address::parse_list` parses unfolded address-list values using the same
parser as `HeaderMap::addresses`: quoted commas and quoted-pairs are handled
before display-name RFC 2047 decoding. Group colons and list separators count
only outside quoted strings, nested comments, angle addresses and domain
literals. Quoted local parts retain spaces, tabs, colons and quoted-pairs
verbatim. This remains tolerant parsing, not complete RFC 5322 validation.
`Address::parse_list_checked` and `HeaderMap::addresses_checked` return
`Result<Vec<Address>, Vec<String>>`: any rejected mailbox makes the whole
result an error containing the original rejected items, rather than a partial
recipient list. Empty lists and empty groups succeed. Unsupported syntax must
be treated as a rejection, not evidence that the mailbox is invalid.
The existing `parse_list` and `addresses` convenience methods return only
accepted mailboxes. IMAP submission uses the checked methods through `submission_envelope`;
Google draft hydration also propagates rejected addresses before rewriting.
`HeaderMap::parse` accepts a standalone header block, unfolding continuations
and preserving repeated fields in wire order. Its caller bounds the input;
unlike full MIME parsing, it applies no header-size limit itself.

Header decoding is bounded on two independent axes. `ENCODED_WORD_SCAN_LIMIT`
(998, the RFC 5322 line limit) caps what a SINGLE RFC 2047 candidate may scan,
so an overlong word is never recognized and is echoed verbatim.
`DECODED_OUTPUT_LIMIT` (64 KiB) caps the TOTAL decoded output of one
`decode_encoded_words` call, because chained in-window words in a legacy
single-byte charset expand about 2.2x with base64 contraction already applied.
Past the total cap the remaining words are emitted verbatim rather than
truncated - the RFC 2047 Section 6.3 display rule for a word the decoder will
not decode - so no bytes are lost and output stays under the cap plus the input
length.

## File map

```
crates/types/src/
  lib.rs              public module and re-export surface
  account.rs          Account, AccountFactory, OpenedAccount,
                      convenience defaults
  capabilities.rs     declarative feature and dispatch shapes
  cursor.rs           cursor scope, membership scope, cursor state
  cursor/topology.rs  coverage, container identity, lifecycle shapes, removals
  events.rs           SyncEvent, Batch, Checkpoint, Control, push hints
  push.rs             lanes, sessions, stop, maintenance gate, pacing and lease
  coverage.rs         inventory coverage domains, reports, obligations
  mutation.rs         mutation targets, flags, labels, fingerprints
  repair.rs           inventory repair requests and terminal outcomes
  hydration.rs        projections and hydrated object vocabulary
  blob.rs             blob handles, byte ranges, range support
  container.rs        containers, labels, roles, provenance,
                      ContainerList skip-lane envelope
  compose.rs          send and draft request vocabulary
  account_compose.rs  account routing, discovery fan-in and capability projection
  search.rs           message search requests and results
  filter.rs           server-filter model
  settings.rs         identities, vacation, quota
  wire/               versioned storage codecs and shared byte primitives
  contact.rs          contacts and address books
  directory.rs        directory search and groups
  calendar.rs         calendars and events
  cloud.rs            hosted attachment vocabulary
  mime/               outgoing renderer plus inbound RFC 5322/MIME parser,
                      header decoding, transfer decoding, and selection
  ids.rs              typed identifiers
  page.rs             Page envelope + SkippedScope skip lane
  error/              structured error, recovery, batch, warning model
```

## Caller versions on bulk mail

`MailMutationTarget { id, expected_version }` carries an independent optional
`ServerVersion` for each target. The `Account` methods
`bulk_set_flags_if_version`, `bulk_move_if_version`,
`bulk_move_from_if_version` and `bulk_destroy_if_version` take `self: Arc<Self>`
and a stream of these targets, plus the same operation arguments and
`IdempotencyKey` as their unversioned counterparts. Existing methods remain.
The versioned methods submit targets independently with bounded memory and
backpressure; callers that do not need preconditions can retain bulk batching
through the existing methods. Each pulled target gets its own outcome, local
refusals do not poison siblings, and a terminal error stops further admission.

`MutationConcurrency::ExpectedVersion(VersionedDoors)` names `MAIL_SET_FLAGS`,
`MAIL_MOVE` and `MAIL_DESTROY` separately from contact and event update/delete
and event RSVP. `VersionedDoors` is a private-bit set built from named constants
with `union`, queried with `contains` and `is_empty`. `VersionedDoors::NONE` is
the sole no-door value; there is no separate concurrency `None` variant.
`StateBased` remains pending the owner decision on legacy provider-observed
guards and does not advertise caller-version doors. An advertised door may
still refuse a compound form that cannot preserve one native atomic guard.
Unsupported version kinds are refused; a stale supported version is a
`ConcurrencyConflict`. No supplied version is refreshed and retried silently.

`MutationConcurrency::gate_version(operation, expected_version, protocol)`
admits absent versions and refuses supplied versions for unadvertised doors.
IMAP and Graph bulk flag writes apply it per target before token admission or
I/O, retaining unguarded siblings. A successful gate does not validate token
kind or implement a wire precondition. The trait's versioned defaults must
still refuse every supplied version: metadata cannot supply a missing guard
implementation. The engine dispatches caller versions to the provider rather
than rejecting a whole mixed batch from this snapshot.

IMAP accepts caller MODSEQ for single-STORE flags. Graph accepts caller ETag for
message PATCH. JMAP accepts caller account Email state for flags, move and
destroy (`bulk_move_from_if_version` ignores `source`, as the unversioned
JMAP move does); an unrelated account change can also
reject that state. Google mail and the standalone DAV accounts refuse supplied
bulk mail versions. An absent version uses the existing mutation behavior.

`event_rsvp_if_version(event, status, expected_version)` defaults to local
refusal for a supplied token and delegates to ordinary RSVP for `None`.
Google Calendar enforces the caller ETag on its attendee PATCH. CalDAV RSVP
sends a scheduling reply before a local resource PUT, so it cannot promise
one atomic caller precondition for the whole operation and refuses tokens.

## Partial write effects

`error/partial.rs` exports PartialCommit, PartialCommitBuilder, PrimaryLeg,
Step, StepOutcome, Compensation, CompensationOutcome, StepId, StepEffect,
StepReplay, Application, StepTargets, TargetRef, TargetRole, OperationPlan,
Prerequisites, PlannedDirective and ReplaySafety, plus the export shapes
PartialCommitTelemetry, PartialCommitExport, StepExport and CompensationExport,
from the error module and crate root.
AccountError exposes the record, failed-leg recovery and per-leg throttle
observations. The builder validates application and compensation evidence before
freezing the derived operation plan. Telemetry and support tiers export the
record at their respective consent levels; see `reference/error-model.md`.

`push.rs` defines normalized `BackoffPolicy` values for engine-owned pacing.
Its const constructor preserves the one-millisecond floor and raises a smaller
maximum to the initial delay.

## Push sessions

`PushSubscription` keeps its handle and outcome ledger private. `new` validates
that a handle exists exactly when the succeeded lane is nonempty;
`all_succeeded` uses the same gate. `handle()` and `outcomes()` borrow the
values, and `into_parts()` returns both owned values. Rebuilding a modified
result goes through `new`, so coverage cannot drift from the handle.

`Account::push_lanes` declares independent Connection or Registration slots.
`effective_push_lanes(&dyn Account)` is the shared availability decision:
`PushCapability::None` yields no lanes; every other capability uses the declared
list unchanged, including an empty list. Lane kind owns session delivery;
the capability's provider variants remain metadata. Health, supervision and
subscription carry-over all use this decision, so an enterprise user is never
shown push coverage for a lane that cannot run. Reattach tears down old
subscriptions without recreating them when the replacement has no effective
lanes; polling continues to cover its scopes.
`push_stream(lane, stop, gate)` drives one re-runnable session: establishment
is separate from demonstrated `Healthy` progress, all failures reach the
engine, and each terminal event follows cleanup. `LaneLease` enforces one
live driver per lane. Invalid and concurrently driven lanes fail with an
Internal(AccountContract) error. Direct consumers must drive and pace every
lane; subscribe only registers coverage. Registration maintenance checks
`PushGate` before each request and retains committed gap obligations in
account state until yielded. Effects needing to survive drop run in
account-owned tasks or actors. SessionStop resolves on explicit stop or
stopper drop. WatchEvent remains the external receiver-to-sink vocabulary.

## MIME Message-ID extraction

`mime::HeaderMap::message_ids` extracts nonempty structural angle-delimited
identifiers across all occurrences of a header, preserving order and ignoring
angle brackets inside quoted strings and nested comments. The free function
`mime::first_message_id(raw)` borrows its result from the raw value, uses the
same scanner and retains a trimmed bare-value
fallback when there is no structural opening angle bracket. Empty or unfinished
structural brackets do not become bare identifiers.

`ContainerRights::may_share` reports authority to change folder sharing. JMAP
projects `mayShare`, IMAP projects ACL administration, and EWS EffectiveRights
leaves it unknown. `PushCapability::Webhook` describes Graph webhooks; EWS
streaming uses `InProcess`.

## Calendar times and range guard

`EventTime` is immutable parsed state. `EventTimeKind` distinguishes a civil
Date, a Floating local date-time, a Zoned local date-time with a retained
provider timezone, and an Offset instant. `parse(value, timezone)` refuses
missing or malformed input; `From<Date>`, `From<DateTime>` and `From<Timestamp>`
also construct valid bounds. `value()` retains the original spelling, including
offset and fractional precision; `timezone()` retains unknown provider TZIDs.
`kind()`, `instant()` and `local_datetime()` read the parsed state without
repeating provider-local shape grammars. A named timezone is an annotation,
not proof that the local time resolves uniquely in an IANA database.

`from_observed` additionally preserves Unspecified and Invalid observations.
These let a consumer inspect a tombstone or edit metadata on a damaged meeting
without fabricating a time. Writers validate touched bounds; an untouched
malformed bound must not prevent an unrelated scalar patch. Empty-end policy
remains provider-specific. `validate_end` owns compatible bound shapes and
ordering: a date-only end is exclusive and must be later than the start; an
explicit timed instant may have equal bounds. Local times in different named
zones are not ordered by their wall clocks.

`CalendarEvent::is_all_day()` and `EventCreate::is_all_day()` derive their
answer from the typed start. Neither payload stores a sibling flag.
`EventPatch::is_all_day` remains an explicit conversion instruction for stored
bounds, including a patch that supplies no new times. It is not an alternative
type tag. `EventTime::converted` produces a date for an all-day conversion and
midnight in its retained timezone (floating when absent) for a date converted
to timed. Providers retain their
conversion limitations, such as Google's requirement for both bounds.

`ReminderTrigger::Relative` carries a parsed `ReminderOffset`; its `span()`
retains nominal days separately from exact hours. `Absolute` carries a
`Timestamp`. Invalid provider triggers remain explicit `Unparsed` observations,
so event detail can report damaged data without inventing or dropping an alarm.

`EventRange::new(calendar_id, start, end)` constructs a first window with no
page cursor or result limit. It preserves the supplied bounds without validation;
provider query boundaries retain responsibility for supported time forms.

`EventRange::admits` is the shared defensive guard used by CalDAV and JMAP
range reads after provider selection. It rejects provably disjoint timed,
nonrecurring intervals with explicit offset-bearing bounds and ends. Ends are
exclusive; an explicit zero-length event includes the window start and excludes
its end. It retains all-day dates, floating local times, unknown or malformed
ends and recurring masters (RRULE or RDATE), because this seam cannot reproduce
the provider's timezone and recurrence matching. UNTIL limits occurrence starts,
not their ends, and explicit dates can extend a series. Calendar users expect
matching meetings to stay visible rather than disappear under an incomplete
local comparison. This is a conservative guard, not occurrence expansion or
request validation. A CalDAV listing fallback can therefore include candidates
whose overlap cannot be established locally.

## Submission validation and reverse paths

`SendRequest::validate_recipients` and `compose::validate_send_recipients`
own the requirement for at least one To, Cc or Bcc recipient. Draft patches
are partial updates, so validate the resolved draft rather than a patch alone.
The MIME send renderer, Google send renderer, JMAP send boundary and shared
header submission resolver use this rule before submitting.

`mime::HeaderMap::submission_envelope` checks every From, Sender, To, Cc and
Bcc address and returns an optional reverse path plus ordered recipients.
For one author, From wins over Sender: an enterprise user sending on behalf
of a shared mailbox expects bounces at that mailbox rather than the delegate.
Without From, Sender wins; without either the caller chooses its default or
SMTP's null reverse path. Multiple authors require one Sender and use it as
the only unambiguous reverse path. Multiple Senders are refused. Unsupported
addresses and missing recipients are refused without dropping any address.
SMTP header conversion and IMAP raw/draft submission share this owner.
`mime::generate_message_id` owns fresh sender-domain correlation ids for both
composed messages and raw IMAP submissions missing a structural Message-ID.

## Shared entity validators

`bifrost_types::EntityTag` owns strong/weak classification and preserves the
original token bytes. `ServerVersion::ETag`, `ContactCard.etag` and
`CalendarEvent.etag` carry this type. HTTP caller-token validation lives on the
shared type; a token without a weak marker still needs syntax validation before
it is accepted as an HTTP precondition. `StrongTag` has a private field.
Weak marker recognition and HTTP admission both accept `W/` and `w/`.
Classification ignores surrounding whitespace conservatively, but HTTP admission
rejects it; `as_str` always retains the original bytes for opaque provider tokens.

## Durable evidence and tag ownership

`AccountError::diagnostics` borrows the complete `DiagnosticInfo`, including
support-only text. Callers must apply consent policy before exporting it. The
sync ledger clones this into its historical `ErrorDigest`, without reconstructing
causes or recovery advice on load.

`wire::cursor` owns the cursor envelope, scope and object/protocol tag codecs,
including checkpoint dispatch and the outer-version migration boundary.
`wire::ledger` owns the versioned error kind, error scope, operation, provider,
protocol, resource, diagnostic, coverage-domain and repair-target codecs.
All shared vocabulary encoders match exhaustively in this crate, including
nested error kinds and diagnostic visibility. A new variant therefore requires
an explicit storage representation at compile time rather than a runtime panic
in a consumer store. `AccountOperation::ledger_tag`, `ObjectType::cursor_tag`
and `ProtocolKind::cursor_tag` retain their existing exhaustive tag projections.

Preserve existing bytes and append new tags. Cursor changes update the shared
outer envelope version and migration window. Ledger vocabulary changes retain
historical decode gates and advance `wire::ledger::VOCABULARY_VERSION` beyond
the current enclosing sync ledger version; sync asserts that its version is at
least that value. Engine-owned ledger layout and migrations remain in sync.
`wire::Reader` and the length-prefixed primitives compose these codecs without
a dependency on the engine. `wire::DecodeError` distinguishes incompatible
schemas from malformed bytes; sync maps those to its existing schema recovery
and malformed-storage errors. Historical reserved cursor tags retain schema
recovery. This ownership move changes neither stored bytes nor readable versions.

`ConvenienceShape::replied` and `forwarded` use `RepliedFlagShape` and
`ForwardedFlagShape`, each selecting `Keyword`, `ExtendedProperty` or `None`.
Their defaults match on that single choice, so contradictory dispatch hints
cannot be constructed. `starred` retains `StarredFlagShape` with keyword,
label membership, category and unsupported choices; `set_starred` reads it.

DAV collection cursors use `FolderType` with Contact or CalendarEvent.
Composition routes those scopes by object type without rewriting cursor state.
`ErrorScope::Namespace { prefix }` names an unavailable IMAP namespace prefix;
it carries no object resource identity.

## Recurrence syntax and observations

`calendar::recurrence` owns `RecurrenceRule`, `RecurrenceUntil`,
`RecurrenceDay` and `RecurrenceRuleError`, also exported at the crate root.
The private parsed rule validates RFC 5545 parts, numeric bounds, duplicates,
COUNT/UNTIL exclusion and frequency-dependent BY constraints. UNTIL retains
DATE, floating DATE-TIME and UTC DATE-TIME separately; BYDAY carries a weekday
and optional ordinal. RFC 7529 scale/skip and leap-month syntax and opaque
X- parts are retained, leaving scale support to providers. The parser preserves
original text and part order. DTSTART-dependent constraints, such as matching
UNTIL's value shape and timezone to the event start, remain at the event adapter.

`EventRecurrence::rrule` deliberately retains observed text, including malformed
server text. Replacing the observation with an infallibly valid value would
make a malformed rule erase a meeting or block an unrelated scalar edit.
`parsed_rule()` is the interpretation boundary. Graph and JMAP project the
shared parsed rule into their supported provider vocabulary; CalDAV validates
it before writing RECUR text. Malformed caller rules are request faults;
valid rules a provider cannot express remain Unsupported. This does not change
`EventRange::admits`, which retains recurring masters without expanding them.

## Account composition ownership

`account_compose::merge_capabilities` owns contact/directory and calendar PIM
projection and caller-version door merging. Its exhaustive 60-field PIM literal
requires an explicit owner for new fields; there is no default update that can
silently drop a delegate capability. Mail fields stay primary-owned, 12 contact
and directory fields come from contacts, and nine calendar fields from calendars.
Absent delegates contribute false fields and no versioned doors. Versioned doors
are selected by lane, including calendar RSVP, never unioned across accounts.
This protocol-neutral owner lives beside typed routing and discovery fan-in
because these are account-contract decisions, rather than IMAP wire policy.
Factory attach failures retain the typed `OpenedAccount::skipped_scopes` lane;
discovery fan-in emits `WarningKind::ScopeSkipped` with the originating
`AccountError` and its optional scope. The warning is advisory; the merged
stream still returns primary and healthy delegate items followed by `Done`.

## Validated windows and account policy values

`TimeWindow`, `UidWindow` and `PageWindow` have private endpoints and fallible
`new` constructors enforcing `from <= to`. Every window is half-open; missing
time bounds are unbounded and equal endpoints form an empty window. UID
endpoints use u64 to represent u32::MAX + 1, reject larger ends, and carry the
UIDVALIDITY epoch. `InventoryPartition` and `CoverageCoordinate` carry these
same windows. `CoverageDomain::for_partition` copies the window and never takes
an unrelated epoch or a non-UID sentinel. `InventoryPartitioning` describes
planning hints, not requested windows: an inclusive maximum UID remains u32,
with its epoch, while page totals and page sizes remain counts.

`InventoryPartition::durable_key` owns checkpoint identity alongside the enum.
Its exhaustive match forces every new partition variant to choose a durable
encoding instead of colliding on an external consumer's fallback key. Existing
`full`, `time:F:T`, `uid:E:F:T` and `page:F:T` bytes are unchanged; omitted time
bounds encode as empty fields. The engine separately owns completion keys and
the decoder for older or unknown stored keys.

`SnapshotIdentity` retains value `Eq` for durable-record round trips and
record deduplication. It is not a coverage proof. Only `same_snapshot_as`
compares provider snapshots, and it refuses two unstable observations. Making
value equality non-reflexive would violate the derived record `Eq` contract.

`ByteRange` describes `[start, start + length)`, or an unbounded tail when
length is absent. HTTP adapters translate to an inclusive last byte and check
arithmetic overflow. The representation was already length based; its former
inclusive documentation described the wire endpoint rather than the API.

`BandwidthCap` is a positive byte rate backed by `NonZeroU64`. Account and control
setters take `Option<BandwidthCap>`; `None` is unlimited. Zero is rejected by
construction, rather than selecting a 1 B/s trickle. Net-budget adapters extract
its numeric rate when applying account policy.

`CloudUploadMeta` carries name, declared MIME type and share scope. Upload
length comes from the payload bytes; consumers cannot supply a conflicting size.
Attachment media-type strings preserve the declared wire value. The existing
`mime::parse_content_type` is deliberately tolerant and substitutes text/plain
on malformed input, so it cannot validate outbound declarations or replace raw
inbound evidence without losing information. The attachment fields therefore
retain their strings rather than silently adopting that parser's fallback.

`CapabilityDelta::between` compares all 15 top-level negotiated snapshot fields
in declaration order, with exhaustive destructuring guarding additions. Keys
name those fields; values are diagnostic Debug renderings, not a wire codec.
The fixed snapshot schema expresses transitions in `changed`, leaving `added`
and `removed` empty. A committed reattach publishes a nonempty delta through
`AccountControl::CapabilitiesChanged`; a provider detecting a change before a
replacement open may still have no second snapshot for its error's delta.

## Absent mutation subjects

`AccountOperation::skips_absent_target` owns an exhaustive per-operation ruling.
Flag, starred, replied, forwarded, MDN, keyword, label, category, extended-property,
importance and read-state edits, bulk and thread moves/deletes, container membership
edits and draft discard skip a confirmed absent subject. An enterprise Outlook
user expects mail deleted elsewhere gone from view, without an error or a false
claim that this call changed it. Reads, sends, uploads, creates, scheduling changes,
settings and non-mail edits retain NotFound: absence cannot supply requested
content, deliver mail or confirm those edits. Missing destinations and input blobs
are never evidence that the subject is absent. This policy does not authorize
blind replay after an ambiguous write.

Graph bulk and PIM message writes and JMAP's shared mail-set classifier consume
this owner. A skip is `MutationSuccess::Skipped`, never an applied write or a
committed partial-effect step. Other provider adapters must explicitly apply the
policy where they establish subject absence.

Recipient validation accepts the calling `Protocol` and stamps empty-send
refusals. Provider-neutral MIME rendering and header resolution pass `Unknown`;
account boundaries stamp their protocol when propagating renderer failures.

## Request refusals

`RequestErrorKind::{PayloadTooLarge, PreconditionFailed}` distinguish provider
input limits and unsatisfied target state from malformed caller requests. Their
matching `RequestCause` variants retain support diagnostics. Both derive terminal
`ProviderRefused`, stable request message keys and `FixClientRequest` remediation.
`UnsupportedReason` is optional metadata on Unsupported errors, set by the
builder and read through `AccountError::unsupported_reason`; decoration preserves
it and `try_build` rejects it on other kinds. It lets an enterprise client explain
which feature or request shape is unavailable without parsing support prose.
Ledger vocabulary version 14 appends request subkind tags 2 and 3. Historical
rows keep their existing bytes and reject those tags before version 14.
