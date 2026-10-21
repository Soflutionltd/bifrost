# bifrost-types reference

`bifrost-types` is the protocol-neutral contract crate shared by every
account implementation and by `bifrost-sync`. It contains no transport
and no engine. Protocol crates implement `Account` and
`AccountFactory`; consumers program against the types in this crate.

The shared structured error contract is documented separately in
`reference/error-model.md`.

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
connections and workers but does not delete durable server-side push
subscriptions, which use `push_unsubscribe`.

## Account trait tiers

The trait is intentionally broad. Its methods group by independent lane as
follows:

| Lane | Methods |
|---|---:|
| capabilities, provider metadata, and scheduling controls | 5 |
| cursor discovery, lifecycle, inventory, hydration stream, and changes | 12 |
| push | 3 |
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
- mutation concurrency and replay safety;
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

## Cursor scopes and memberships

`CursorScope` answers where change state advances:

- Gmail: `Account`;
- JMAP: commonly `Type(ObjectType)` or `Query(QueryId)`;
- IMAP: `Folder(FolderId)`;
- Graph: `FolderType { folder, ty }`.

`MembershipScope` answers where an object belongs:
`Folder`, `Mailbox`, `Label`, or `Query`. The two enums must not be
collapsed. One account-wide cursor can cover many memberships, and one
object can have several memberships.

`ChangeCursor` combines a scope, protocol-tagged opaque server state,
an optional progress marker, and an outer envelope version. The outer version
comes from `CHANGE_CURSOR_ENVELOPE_VERSION`; `validate_envelope` is the gate the
engine applies before dispatching a stored cursor to a protocol account, and it
is a strict equality check, never a range. Migration of an older persisted
layout belongs entirely to `bifrost-sync`'s envelope decoder, which stamps the
current version on the way out, so a `ChangeCursor` a protocol crate is handed
is always at the layout that crate compiles against. `ChangeCursorEnvelopeMismatch`
reports `expected` and `found`.
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
events or a structured termination. `WatchEvent` carries invalidation,
connection-health, warning, and termination signals. `InvalidationSink`
is synchronous so an out-of-process webhook or Pub/Sub receiver can
feed the engine from a non-async thread.

`Control::pause` and `Control::checkpoint_now` return
`Result<Option<Checkpoint>, AccountError>`. `None` means the stream is
at a safe boundary but has never produced a durable checkpoint.

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
differs: id, memberships, size, blob id, fingerprint, thread id, message id,
references, or in-reply-to. `InventoryEntry::differs_from` is the canonical
comparison. Comparing `Fingerprint` alone is insufficient: JMAP mailbox
membership can change without keyword state changing.

## Mutation outcomes

`FlagOp::validate` is the account-boundary guard for flag mutations. Empty
add/remove deltas and an entirely empty patch are invalid; `Set(empty)` means
"clear all flags". A patch may not name the same flag in both sets under
ASCII-case-insensitive identity. Invalid operations are caller errors and must
be rejected before provider I/O.

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

`AccountOperation::is_idempotent` is the authoritative retry input for
the error model. Absolute-state writes are idempotent. Sends, creates,
moves, destroys, uploads, and other side-effecting writers are not.

## Paginated results

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
- `mime::DecodedAttachment` - the PARSER's intermediate, produced by
  `select_body` and mapped onto `MessageAttachment` by each protocol crate. It
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
accepted mailboxes. IMAP submission and Google draft hydration currently use
these convenience methods; callers that submit or rewrite recipients must use
the checked methods and propagate rejection before writing.
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
  events.rs           SyncEvent, Batch, Checkpoint, Control, push hints
  coverage.rs         inventory coverage domains, reports, obligations
  mutation.rs         mutation targets, flags, labels, fingerprints
  repair.rs           inventory repair requests and terminal outcomes
  hydration.rs        projections and hydrated object vocabulary
  blob.rs             blob handles, byte ranges, range support
  container.rs        containers, labels, roles, provenance,
                      ContainerList skip-lane envelope
  compose.rs          send and draft request vocabulary
  account_compose.rs  compose validation and helpers
  search.rs           message search requests and results
  filter.rs           server-filter model
  settings.rs         identities, vacation, quota
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

`MutationConcurrency::ExpectedVersion` names `mail_set_flags`, `mail_move` and
`mail_destroy` separately from contact and event update/delete and event RSVP. A true door may
still refuse a compound form that cannot preserve one native atomic guard.
Unsupported version kinds are refused; a stale supported version is a
`ConcurrencyConflict`. No supplied version is refreshed and retried silently.

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
