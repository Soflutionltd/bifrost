# Defect hunt: bifrost-graph

Scope: `crates/graph/`. Read against `reference/graph.md`, `reference/error-model.md`
and the engine contract in `reference/sync.md`. Coverage was selective: the sync
lanes (`inventory.rs`, `changes.rs`, `cursor.rs`), the mutation funnel
(`mutate.rs`), the webhook arm (`webhooks.rs`, `push/webhook.rs`,
`push/renewal.rs`), and spot checks of `get.rs`, `contacts.rs` and
`public_folder.rs`. Public-folder EWS, autodiscover, cloud upload and search
were not audited in depth.

Findings are ordered by confidence times impact. Each names the claim it breaks.
Where the defect depends on Graph wire behaviour I could not verify inside the
repo, that is said explicitly.

---

## 1. `bulk_destroy` is a soft delete reported as `Applied`

`mutate.rs request_for_mutation`, `MutationKind::Destroy`: `DELETE
{prefix}/messages/{id}`. On Graph, `DELETE /messages/{id}` moves the message to
Deleted Items (from Deleted Items it moves on to Recoverable Items). It does not
remove the message. Permanent removal is `POST /messages/{id}/permanentDelete`,
which Microsoft added for exactly this reason. Neither the crate nor
`reference/graph.md` mentions `permanentDelete`.

`graph_error::mutation_item_outcome` maps every 2xx to
`MutationSuccess::Applied`.

Claim broken: `MutationSuccess`'s own documentation in
`crates/types/src/error/stream.rs`. Its motivating case is Gmail's TRASH
fallback: "the messages move, they do not cease to exist. Reported as `Applied`,
they came back on the next inventory pass and were destroyed again forever."
Graph has the same shape. The destroyed id reappears in the Deleted Items scope
through delta (with a new id once it moves, which is worse, because the engine
cannot correlate it). `delete_thread`'s "destroy if already in Trash" logic in
`pim/threads.rs` also rests on this DELETE, so a thread "destroyed" from Deleted
Items lands in Recoverable Items rather than being gone.

Fix: send `permanentDelete` (a POST inside `$batch`) for destroy. If the soft
path is kept anywhere, report `Downgraded`. Wire behaviour: well established
publicly, but not pinned anywhere in this repo.

## 2. The etag cache mixes two ETag forms, and one is not a valid `If-Match`

`inventory.rs graph_etag` prefers `changeKey` over `@odata.etag`. `MESSAGE_SELECT`
(`types.rs`) explicitly selects `changeKey`, so every inventory and delta page
caches the RAW change key (`CQAAABYAAA...`). Two other paths cache something
different:

- `refresh_missing_etags` GETs `?$select=id`. `changeKey` is absent there, so it
  falls back to `@odata.etag`, which is `W/"CQAAABYAAA..."`.
- The post-write cache update in `update_etag_cache_from_responses` reads the
  `ETag` response header first, which is the quoted weak form.

`request_for_mutation` sends whatever is cached as `If-Match`. A bare change key
is not an RFC 9110 entity-tag (it has no quotes). The same message gets a
different `If-Match` depending on which path last refreshed it. On the common
path (an item seen by inventory or delta, then flagged) the header is the
unquoted form. Depending on how Exchange parses it, the result is either a 412
on every first write, which costs a preflight-and-retry loop and possibly a
permanent `ConcurrencyConflict`, or a precondition that is silently ignored.

Claims broken: the reference says the etag index is what powers `If-Match`, and
that `MESSAGE_SELECT` "explicitly requests `changeKey`" for that purpose. It also
undermines `Fingerprint.server_version`, which is `ETag(changeKey)` from
inventory but would compare against `W/"..."` from any other path.

Fix: normalise at one point. Either always store `@odata.etag`, or always wrap
the change key as `W/"<changeKey>"`. Pin it with a test that feeds the same item
through delta and through refresh and asserts the same header bytes. Wire
behaviour of the unquoted form against Exchange is unverified here.

## 3. Inventory and delta event ids cannot be used at the typed calendar doors

`inventory_entry_from_value` and `changes_stream` mint every object id through
`foreign::encode_message_id(scope, id)`, including for `ObjectType::Event` and
`ObjectType::Contact` scopes. For an event scope the result is the bare Graph
event id. The typed calendar surface only accepts composite
`{calendar}::{event}` ids: `split_event_id` refuses anything without the
separator as `Request(Malformed)` (reference, "An `EventId` without the
calendar separator was never minted here"). `get_stream` is documented as
mail-only (`get.rs hydrate_url_for_id`). So an event the sync engine reports has
no door that accepts its id. A consumer has to know to rebuild
`{scope.folder}::{id}` itself, and nothing documents that.

Claims broken:
- The reference says composite `EventId`s are THE identity scheme.
- `reference/sync.md`: an id emitted by inventory or changes names an object the
  account can read.

The id mint is also mail-shaped for foreign scopes. A foreign contact or event
scope, if discovery ever produces one, gets a `{mailbox}\u{1f}{id}` tag that
`contacts.rs` (which always uses `account.client`, the primary) never decodes.

A side effect: event and contact change keys are written into the mail
`etag_index` LRU, where nothing reads them. They evict live mail etags and force
extra preflight GETs on later mail mutations.

Fix: mint per-type ids at the sync lanes (composite `EventId` for events), or
document that the lane's event ids are bare and give the typed doors an
accepting form. Keep non-mail etags out of `etag_index`.

## 4. The calendar delta request likely sends parameters Graph rejects or ignores (verify)

`initial_delta_request` builds
`/calendars/{id}/calendarView/delta?startDateTime=..&endDateTime=..&$select={EVENT_SELECT}`.
Graph's event-delta documentation lists only `startDateTime`, `endDateTime`,
`$deltatoken` and `$skiptoken` for calendarView delta, says `$select` is not
supported, and says page size is controlled by `Prefer: odata.maxpagesize`. Two
possible outcomes:

- A 400 on every event scope's first inventory. That is a whole capability
  silently dead, and the hermetic tests only assert the URL string.
- The `$select` is ignored and delta returns its reduced property set. Then
  `changeKey` (and so items 2 and 3) and fields like `recurrence` are absent,
  and the fingerprint degrades to `Unavailable`.

The reference also says calendar reads send `Prefer: outlook.timezone="UTC"`.
The delta lane goes through plain `client.get` with no Prefer header, so delta
start and end values arrive in the mailbox zone.

Claim at risk: event scopes are advertised as delta-synced ("event deltas per
`calendars/{id}`"). This is not provable in-repo. It needs one live check
downstream.

## 5. A failed `$batch` POST drops the chunk's already-decided outcomes and misclassifies the rest

`mutate.rs submit_batch`: preflight failures (no etag, stale owner, cross-mailbox
move) collect into `preflight_outcomes`, then
`account.client.post_batch(...).await?` propagates. `bulk_mutation_stream` turns
that `Err` into one `Terminated` for the whole stream. The consequences:

- The per-id `Failed` outcomes already computed for this chunk are discarded,
  never emitted.
- An `InFlight` failure of the POST (timeout or reset after the body was sent)
  becomes a stream-level terminal error. The per-item `Uncertain` lane exists
  for exactly the case "the write may have landed, read it back", and
  `reconcile_mutation_responses` uses it for missing subresponses, yet the whole
  chunk's submitted ids lose their read-back.
- The remaining target stream is never consumed, and its ids get no outcome.

Claim broken: the three-lane `BatchOutcome` contract in
`reference/error-model.md`, and this module's own doc comment on missing
subresponses ("a `move` or `DELETE` that committed and then lost its
subresponse is byte-identical to one that never ran"). A lost POST response is
the same case one level up.

Fix: on a POST error, emit the chunk as preflight outcomes plus every submitted
id on `Uncertain` (if `InFlight`) or `Failed` (if `Unsent`, or acknowledged with
an error status). Terminate only for faults that are truly account-wide.

## 6. Contact and calendar `If-Match` protects nothing the caller can see

`contacts.rs update` / `delete` and `calendar.rs` update / delete GET the object,
then immediately send `If-Match` with the etag they just fetched. The
precondition only covers the few milliseconds between the GET and the PATCH. A
caller doing read-modify-write from an older read (the normal UI flow, and the
echo pattern the reference describes for events) overwrites a concurrent
server-side edit with no conflict. When the fetch returns no etag, the write
goes unconditional with no signal.

The reference calls this "fetch-then-`If-Match`", so it is documented, but the
module summary sells it as "`If-Match` etag mutations". The neighbouring mail
lane gets real optimistic concurrency from the cached etag, so the two lanes
under one heading behave differently. This is a contract-versus-name gap more
than a crash. Either thread the caller's version (`ContactCard` / `CalendarEvent`
etag) through the patch, or drop the claim of concurrency protection.

## 7. Webhook expiry policy is stale

`webhooks.rs`: `MAX_EXPIRATION_MINUTES = 4230`, `DEFAULT_EXPIRATION_MINUTES =
1440`. Graph's documented maximum for Outlook message, event and contact
subscriptions without resource data is 10080 minutes (7 days); 4230 was the old
limit. This is not wrong, only more renewal traffic than needed. The real issue
is coupling: the renewal tick is 10 minutes and the threshold 30 minutes, so a
worker that misses three consecutive ticks (a process suspend, a laptop sleep)
lets a subscription expire. That falls into the 404-then-recreate path, which
has a notification gap by construction. A longer expiry widens the margin for
free. Low severity.

## 8. Renewal recreate classifies a 404 for a missing resource as "subscription gone"

`subscription_is_gone` treats any 404 or 410 on `PATCH /subscriptions/{id}` as
"recreate". A recreate POST for a resource whose folder was deleted then 404s
too. That surfaces as `phase = "recreate"`, `Server(Error 404)` or
`NotFound(Mailbox)`, which may classify as terminal or as retry-forever
depending on mapping. If it is retryable, the worker issues a doomed PATCH plus
POST pair every tick until reopen, because the stale row is deliberately kept.
Worth one test that pins the folder-deleted recreate as terminal (with
`mark_subscription_terminated`), so a deleted folder stops being retried every
10 minutes.

## 9. A mid-inventory cursor with its outer marker cleared reseeds the whole account

`cursor.rs decode_cursor` makes the outer `ChangeCursor::advanced_through`
authoritative, including clearing it. `inventory_stream_from` then sees
`inventory_in_progress = true` with no marker and terminates with
`CursorError::SchemaIncompatible`. Per the v2 comment in `cursor.rs`, that
"derives `Engine(SchemaIncompatible)`: the engine drops every durable cursor".
One scope's ambiguous position therefore wipes every scope on the account.

`changes_stream` deliberately uses `InventoryInProgress` (scoped restart) for
the sibling case, and `CursorError`'s own doc for that variant says scope-local
restart is the point. Using `InventoryInProgress` (or another scope-local arm)
here would be consistent. This is the "sibling arm in the same match shows the
right treatment" shape from AGENTS.md.

## 10. Bandwidth smell: delta inventory selects full bodies

`MESSAGE_SELECT` is shared by inventory, delta and hydration, and includes
`body`, `uniqueBody` and `internetMessageHeaders`. The inventory and changes
lanes use only id, parent, flags, categories, changeKey, conversationId and the
Message-ID, References and In-Reply-To headers. Every delta page of 50 messages
therefore downloads 50 full HTML bodies twice over (`body` plus `uniqueBody`)
that are thrown away. On a first sync of a large mailbox this is most of the
bytes. Not a contract defect. It is the largest optimisation visible in scope:
give the sync lanes a narrow select.

## 11. Smaller notes

- `changes_stream` emits `ObjectChange::Updated` plus `ScopeChange::Added` for
  every non-removed delta value, including items that are new to the scope, and
  never emits `Created`. If the engine treats `Updated` on an unknown id
  differently from creation, new mail is read as an update. Check against
  `reference/sync.md`'s `ObjectChangeKind` semantics.
- `inventory_stream_from` ignores `@removed` values except for etag eviction. On
  a RESUMED inventory (v4 page cursor), a removal that happened between pages
  never reaches the engine during inventory. It relies on the following delta
  walk to repeat it, which Graph does not guarantee for an item the enumeration
  already reported as removed.
- `changes.rs` stores the BARE id as `last_seen_id` in the page marker, while
  inventory stores the encoded id. The difference is documented as deliberate,
  and nothing reads it for resume, but two fields with one name and two
  encodings in one persisted type will bite whoever first reads it.
