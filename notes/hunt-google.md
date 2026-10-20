# Defect hunt: bifrost-google

Scope: `crates/google/`, read against `reference/google.md`, `reference/error-model.md`
and the `reference/sync.md` engine contract. Findings are ordered by severity. Each one
names the claim it breaks. Areas covered: mutation driver, flags, changes, inventory and
hydration, scopes and lifecycle, push actor, account shell, PIM, contacts, blobs, and
spot checks of calendar. `cloud.rs`, `client.rs` (quota and redirect plumbing) and most of
`calendar.rs` got only a light read, so a clean result there means little.

---

## P1: live defects

### 1. A 404 on the second or later label-chunk body silently drops the rest of the patch and reports `Applied`

`mutation.rs::apply_label_patch` loops over `batch_modify_bodies(ids, &patch)`. That
function splits a label-heavy patch (every exact-set `FlagOp::Set` on an account with more
than 100 user labels) into several bodies. If body *k* answers 404, the loop does
`return apply_label_patch_bisected(client, ids, &body, ...)`:

- The bisection replays only **body k's** label lists.
- Bodies *k+1..n* are never sent.
- The bisection's successes go through `label_patch_successes`, which emits
  `MutationSuccess::Applied`, or `Downgraded` only for `unsupported_flags`, for every live
  id.

So one deleted message in a 1000-id exact-set batch on a label-heavy account reports
"applied" for 999 messages. Up to two thirds of the removals never reached Gmail.

Claims broken:
- The driver's own doc on `batch_modify_bodies`: "applying the same id list across several
  requests converges on the same final label set as one oversized request would have".
  That only holds if every body is sent.
- `MutationSuccess::Applied`.

The engine's read-back guard may catch it later, but the lane is a lie. The fix should not
bisect per body. Either resolve the absent ids once (bisect on the first 404, then send
every body to the live set), or restructure so the id partition is computed once and every
body is applied to each live partition.

### 2. A non-404 permanent failure on a later chunk body reports `Failed` for ids that body 1 already mutated

Same loop. If body 1 succeeds and body 2 fails with an error that `mutation_error` fans out
per id (`Server(Error{..})`, i.e. any kind `terminates_mutation_stream` exempts), every id
gets `ItemOutcome::Failed`. But body 1's labels are on the server.

The driver's own comment on the shutdown branch says: "`Failed` would assert they did not
land". The honest lane after a partially sent multi-body patch is `Uncertain`, or a
`Downgraded` partial-application effect, not `Failed`.

Claims broken: the error-model three-lane contract (`Failed` means not applied), and the
driver's stated rule.

### 3. Inventory turns throttling, quota holds, auth loss and transport faults into per-object obligations and keeps walking

`inventory.rs::inventory_stream_cancellable`, hydration loop. Every hydration error other
than `NotFound(Message)` becomes an `InventoryObligation::Object`, and the walk continues
whatever the `RecoveryClass` is. Under each of these:

- a project quota hold (`LocalHold`, reported locally with no wire call),
- a 429 or `userRateLimitExceeded`,
- an expired token (`AuthLost`),
- a network drop,

every remaining message of the mailbox (500 per page, across every page) is recorded as an
"object this walk could not represent". The walk then reports completion as `Degraded`
coverage instead of terminating so the engine can back off.

Claims broken:
- The obligation's own justification in this file: "the obligation was raised because this
  object could not be represented". A rate limit says nothing about the object.
- The error-model rule, which the mutation lane applies through
  `terminates_mutation_stream`, that `Retry`, `AuthLost`, `Engine` and `Reconcile` classes
  end the stream rather than being laundered into per-item failures.

The `mod.rs` comment on the bisection path calls that laundering "a worse lie". It also
floods the sync ledger with thousands of obligations, each with its own retry budget. Fix:
apply the same `RecoveryClass` gate the mutation driver uses. Per-object obligations only
for object-attributable permanent classes; everything else terminates.

The same question applies to `get_stream`. It deliberately sends every per-id error to a
`Failed` lane (documented in `reference/google.md`). A 429 or `AuthLost` across 32 ids then
becomes 32 `Failed` lanes instead of one terminator. The reference records this as a
design choice ("a hydration item named its own id and affects no other"), but for
account-wide classes that premise is false. Worth a ruling.

### 4. Push subscription handles are not unique, so a second subscriber's unsubscribe can stop a watch the first still holds

`push.rs::actor_subscribe` mints the handle as `JSON{topic, history_id, expiration}`, taken
from the `users.watch` response, and stores it in `handles: HashSet<String>`.

Two subscribes that Gmail answers identically produce the same string:
- the history id is unchanged when the mailbox is quiet;
- the expiration is a millisecond timestamp.

The set collapses them into one entry. The first `push_unsubscribe` then sees
`HandleRemoval::Last` and issues `users.stop`, while the other subscriber still holds an
identical handle. Its next unsubscribe sees `NotPresent` with an empty set and calls stop
again.

Claim broken: the handle-counting contract in `actor_unsubscribe` (stop only when the last
handle is removed), and `SubscriptionHandle` as a subscription identity. Fix: put a nonce
or counter in the handle.

---

## P2: contract and doc mismatches with real effect

### 5. Label CRUD never invalidates the shared label vocabulary

`pim::container_create`, `container_rename` and `container_delete` take no `ScopeCache`.
For up to `SCOPE_CACHE_STALE_AFTER` (5 minutes) after a create, a
`$gmail-label:<new id>:<name>` flag:
- fails `user_label_id_from_flag`;
- lands in `unsupported_flags`;
- produces a per-id `Unsupported` failure, or a `Downgraded` partial result.

Hydration meanwhile renders the new label as `$gmail-label:ID:ID` (the name fallback). It
re-renders once the cache refreshes, which changes `Fingerprint.flags_hash` with no change
on the server. After a rename, the old name sticks in flags for the same window.

Consequence: "create a label, then apply it" (the normal UI flow) fails for five minutes.
A cache miss on a write does not trigger a refresh either.

Claim broken: `labels_for_flags` as the authoritative vocabulary ("canonicalizing against
an empty vocabulary is silently wrong in both directions"). A stale one is wrong the same
way. Fix: invalidate (or write through) the cache on label CRUD, and refresh once on an
unresolved `$gmail-label:` id before declaring it unsupported.

### 6. The inventory's profile `historyId` parse failure is classified as the caller's fault

`inventory.rs::inventory_checkpoint` maps a non-numeric `profile.historyId` to
`Error::invalid_request`, i.e. `Request(Malformed)` and `ClientBug`. The same condition in
`GoogleAccount::open`, and the history response in `changes.rs`, map to
`Protocol(ParseFailed)` with `Acknowledged` evidence.

Claim broken: `reference/google.md` ("a value that is present but not a number is
`Protocol(ParseFailed)`") and the error-model rule that local refusals of provider data
blame the provider.

### 7. Mid-bisection shutdown discards resolved outcomes and reports them `Uncertain`

`mutation_stream`'s shutdown arm replaces whatever `operation_future` had accumulated with
`uncertain_outcomes(&ids, ..)` for the whole batch. Ids the bisection already proved
`Applied` or `NotFound` are reported `Uncertain`. This is less harmful than a false
success (it only causes read-backs), but it contradicts `MutationApply::Terminate`'s doc
("Ids the bisection already resolved get their lane before the stream ends"). Fix: carry
the accumulated outcomes out of the future (shared state or a channel) and mark only the
in-flight sub-batch uncertain.

### 8. `reference/google.md` says lifecycle rename events carry no names; the code carries them

The reference says: "Rename events carry the same stable label id in `old` and `new`; they
are invalidation signals ... not a carrier for the old and new display names."
`scopes::diff_snapshots` fills `old_name` and `new_name`. One of the two is wrong. The doc
looks stale.

### 9. `discover_memberships` has no cache-hit path

The comment in `scopes::discover_memberships` ("A cache hit performs no request, and then
the tally is legitimately zero") and `reference/google.md` ("a `discover_memberships`
cache hit performs no request") both describe a cache hit. The code calls
`refresh_scope_snapshot` unconditionally: always one `labels.list`, never a hit.

Side effects:
- It writes the shared snapshot outside the single-flight `refresh` mutex.
- An older in-flight `labels_for_flags` refresh finishing later can overwrite a newer
  snapshot (last writer wins, not newest fetch).
- `containers_list` has the same unlocked write.

### 10. `open_raw_rfc822` reports decoded size as `bytes_in`

`blobs::open_raw_rfc822` sets `bytes_in = bytes.len()`, the decoded MIME. The wire carried
a base64url JSON envelope, about a third larger. `open_blob` reports the true transferred
size.

Claim broken: `reference/google.md`, "Blob batches keep their own exact transferred size",
and the "no lane reports a number it did not measure" rule.

### 11. `contact_create` returns an empty `ContactId` on a response with no `resourceName`

`contacts::create` does `person.resource_name.unwrap_or_default()`, so a malformed 200
yields `Ok(ContactId(""))`. Calendar's create treats the analogous missing id as
`Protocol(MissingField)` with `Acknowledged` evidence (`local_error_with_field`). This
should too. An empty id fed back into get/update/delete builds `/v1/:updateContact`-shaped
URLs.

### 12. `contact_update` is two writes with no partial-failure reporting

`contacts::update` sends `updateContact` for fields, then `updateContactPhoto` or
`deleteContactPhoto`. If the photo call fails after the field write landed, the caller gets
the photo's plain error, which reads as "nothing happened".

The calendar update path reclassifies the identical shape (move succeeded, then patch
failed) as `Protocol(PartialResponse)` with `Acknowledged` evidence. The reference spends a
paragraph on why. The contacts path has the same non-atomic shape without that treatment.

### 13. RSVP quietly writes `needsAction` for `Delegated` and `Unknown`

`calendar::rsvp_value` maps `RsvpStatus::Delegated`, `Unknown` and any future variant to
`"needsAction"`. `event_rsvp(Delegated)` therefore resets the user's response on the server
and returns `Ok(())`. It should be `Unsupported(EventRsvp)` before any request.

Related: the attendee is matched only by `email.eq_ignore_ascii_case(profile email)`.
Google marks the account's own attendee with `self: true`. On an alias, a secondary
calendar, or a delegated calendar, the email match fails. The failure is then classified
`Request(Malformed)` / `ClientBug`, but the input was valid and the event state simply
differs.

### 14. `identity_update` writes `signature_text` into Gmail's HTML `signature` field unescaped

`pim::identity_update` falls back from `signature_html` to `signature_text` and sends the
text verbatim as `signature`. Gmail's sendAs `signature` is HTML. Plain text containing `<`
or `&`, or newlines, renders wrong or injects markup. `identities_list` maps the field back
only to `signature_html` (`signature_text: None`), so the round trip changes the field's
type. The field name `signature_text` claims plain-text semantics the code does not honor.
Fix: HTML-escape and convert newlines to `<br>`, or refuse `signature_text`.

---

## P3: smaller defects and smells

### 15. Some read streams and futures ignore the shutdown token

`repair_inventory`, `open_blob`, `open_raw_rfc822`, `discover_memberships` and every PIM,
contacts and calendar future ignore `shutdown`. The reference's own rationale for adding
cancellation to the read streams applies to `repair_inventory` (a stream) just as much: "a
stream that outlived `close()` kept calling Gmail through a deregistered `AccountNet` -
unmetered, and reporting the result as a transport fault rather than as a closed account".
The reference names only four streams, so this is not a broken claim. It is the same hole,
one stream over.

### 16. The lifecycle poll request is not raced against shutdown

`scope_lifecycle_stream` selects on shutdown only during the wait. An in-flight
`labels.list` (possibly waiting for admission behind a long project hold) holds the stream
open after `close()`.

### 17. `push_stream` drops health events on `Lagged` silently

A lag of more than 32 events (unlikely, but a burst of renewal `Warning`s with a slow
consumer is possible) can swallow a `Terminated` or `Disconnected`. Nothing tells the
consumer that health edges were lost.

### 18. Unbounded bisection cost

When many ids in a batch are gone (for example a mass delete raced by a bulk move), the 404
bisection can issue on the order of 2n `batchModify` calls at 50 quota units each, with no
budget cap. That is up to about 100k units for one 1000-id batch.

There is a related unconfirmed risk: if Gmail answers 404 for an unknown LABEL id, not just
a message id, the bisection would blame every message as `NotFound(Message)`. Worth
pinning with a captured transcript.

### 19. A malformed message id poisons the batch

Gmail answers 400 for a syntactically invalid id. That is not bisected, so it fans out
`Request(Malformed)` across all 1000 ids. The 404 bisection exists for exactly this
"one id fails the whole call" shape; 400-on-invalid-id has the same shape.

### 20. `FlagOp::Add` can be used to move messages

`flags::user_label_id_from_flag` deliberately resolves system ids, for `CATEGORY_*`. So a
crafted flag such as `$gmail-label:INBOX:INBOX`, `$gmail-label:TRASH:TRASH` or
`$gmail-label:UNREAD:UNREAD` passed to `FlagOp::Add` adds INBOX, TRASH or UNREAD through
the flag lane:
- This bypasses the relocation rule (`move_placement_patch`): it adds TRASH without
  removing INBOX.
- It can contradict `\Seen` in the same op.

`canonical_flags` never emits those spellings, so only a caller-forged flag gets there. The
"never reach this spelling" argument in the doc comment covers reads, not writes.

### 21. A label rename churns every message's fingerprint

Canonical flags embed the display name (`$gmail-label:ID:NAME`). Renaming one label changes
`flags_hash` for every message carrying it, with no server-side change to those messages,
and can trigger a mass rehydrate. That is a design consequence, not a broken claim, but it
interacts badly with finding 5.

### 22. `content_disposition_inline` is a substring match

`blobs::content_disposition_inline` matches `"inline"` anywhere in the header value, so
`attachment; filename="inline-chart.png"` is reported `inline: true`.

### 23. Contact search warmup is fatal; directory search warmup is not

`contacts::search` fails the whole search on a transient warmup failure.
`directory_search` explicitly tolerates one ("warmup only primes the cache"). The two
paths contradict each other's rationale.

---

## Structural observation

Findings 1, 2 and 7 share one root. The mutation driver's unit of reporting is "one id
list", but the unit of transmission has become "(id partition) x (label chunk)", and the
outcome bookkeeping never caught up. A rewrite worth considering:
1. Compute the id partition first (live versus absent, via bisection on 404).
2. Apply every label body to each live partition.
3. Track per-(id, body) evidence, so the lane per id is derived as: all bodies acked gives
   `Applied`, some acked gives `Uncertain` or partial, none sent gives unreported.

Shutdown and terminal errors would then read the same ledger instead of guessing.
