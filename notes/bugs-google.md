# bifrost-google defect findings

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

Filed from the October 2026 defect hunt: the google scope, plus the Gmail
per-scope push finding from the types adoption scope. Severity labels are the
hunter's. Coverage note: `cloud.rs`, `client.rs` (quota and redirect plumbing)
and most of `calendar.rs` got only a light read, so a clean result there means
little. Gmail's read-back of trashed and partial-flag outcomes is SYNC-008;
lifecycle and renewer recovery triage is SHARED-012 / SHARED-013; the private
address parser is SHARED-004; the Retry-After parser copy is SHARED-006.

## GOOG-001 - A 404 on a later label-chunk body drops the rest of the patch and reports `Applied`

Hunter severity: P1.

`mutation.rs::apply_label_patch` loops over `batch_modify_bodies(ids, &patch)`,
which splits a label-heavy patch (every exact-set `FlagOp::Set` on an account
with over 100 user labels) into several bodies. If body k answers 404 the loop
does `return apply_label_patch_bisected(client, ids, &body, ...)`:
- the bisection replays only body k's label lists;
- bodies k+1..n are never sent;
- bisection successes go through `label_patch_successes`, emitting
  `MutationSuccess::Applied` (or `Downgraded` only for `unsupported_flags`) for
  every live id.

One deleted message in a 1000-id exact-set batch on a label-heavy account
reports "applied" for 999, with up to two thirds of the removals never sent.
Claims broken: `batch_modify_bodies` doc ("applying the same id list across
several requests converges on the same final label set as one oversized request
would have"), which holds only if every body is sent; `MutationSuccess::Applied`.
The read-back guard may catch it later, but the lane is false. Fix: do not
bisect per body; resolve absent ids once (bisect on the first 404, then send
every body to the live set), or compute the id partition once and apply every
body to each live partition.

## GOOG-002 - A non-404 permanent failure on a later chunk reports `Failed` for ids body 1 already mutated

Hunter severity: P1.

Same loop. If body 1 succeeds and body 2 fails with an error `mutation_error`
fans out per id (`Server(Error{..})`, any kind `terminates_mutation_stream`
exempts), every id gets `ItemOutcome::Failed`, but body 1's labels are on the
server. The driver's own shutdown-branch comment: "`Failed` would assert they
did not land". The honest lane after a partially sent multi-body patch is
`Uncertain` or a `Downgraded` partial effect. Claims broken: the error-model
three-lane contract and the driver's stated rule.

Structural note from the hunter, covering GOOG-001, GOOG-002 and GOOG-007: the
unit of reporting is "one id list" while the unit of transmission became
"(id partition) x (label chunk)". Compute the id partition first (live vs
absent via 404 bisection), apply every body to each live partition, and track
per-(id, body) evidence so each id's lane derives as all-acked -> `Applied`,
some-acked -> `Uncertain`/partial, none-sent -> unreported. Shutdown and
terminal errors then read the same ledger.

## GOOG-004 - Push subscription handles are not unique, so one subscriber's unsubscribe can stop another's watch

Hunter severity: P1.

`push.rs::actor_subscribe` mints the handle as `JSON{topic, history_id,
expiration}` from the `users.watch` response and stores it in
`handles: HashSet<String>`. Two subscribes Gmail answers identically (unchanged
history id on a quiet mailbox, millisecond expiration) produce the same string
and collapse to one entry. The first `push_unsubscribe` sees
`HandleRemoval::Last` and issues `users.stop` while the other subscriber still
holds an identical handle; its later unsubscribe sees `NotPresent` with an empty
set and calls stop again. Claims broken: the handle-counting contract in
`actor_unsubscribe` and `SubscriptionHandle` as an identity. Fix: a nonce or
counter in the handle.

## GOOG-005 - Gmail `push_subscribe` refuses a mixed scope list whole, not per scope

Scope: types adoption.

`crates/google/src/account/mod.rs` `push_subscribe` -> `push::push_subscribe`:
any non-`Account` scope in the list returns `Err` (uncorrelated
`Unsupported(PushSubscribe)`), and success always reports
`PushSubscription::all_succeeded`. `reference/error-model.md`: "refused scopes
carry their own scope-correlated `AccountError` in the failed lane ... an
`Err(_)` from `push_subscribe` still means no scope was subscribed at all". For
`[Account, Folder(x)]` the watch could cover `Account` and report `Folder(x)`
failed; instead nothing is subscribed and the caller cannot tell which scope was
the problem. IMAP, JMAP and Graph produce per-scope lanes.

## GOOG-006 - Label CRUD never invalidates the shared label vocabulary

Hunter severity: P2.

`pim::container_create`, `container_rename` and `container_delete` take no
`ScopeCache`. For up to `SCOPE_CACHE_STALE_AFTER` (5 minutes) after a create, a
`$gmail-label:<new id>:<name>` flag fails `user_label_id_from_flag`, lands in
`unsupported_flags`, and produces a per-id `Unsupported` failure or a
`Downgraded` partial. Hydration renders the new label as `$gmail-label:ID:ID`
(name fallback) and re-renders once the cache refreshes, changing
`Fingerprint.flags_hash` with no server change. After a rename the old name
sticks for the same window. "Create a label, then apply it" fails for five
minutes, and a write-side miss does not trigger a refresh. Claim broken:
`labels_for_flags` as the authoritative vocabulary ("canonicalizing against an
empty vocabulary is silently wrong in both directions"); a stale one is wrong
the same way. Fix: invalidate or write through on label CRUD, and refresh once
on an unresolved `$gmail-label:` id before declaring it unsupported.

Related design consequence (P3, not a broken claim): canonical flags embed the
display name, so renaming one label changes `flags_hash` for every message
carrying it with no server change to those messages, and can trigger a mass
rehydrate.

## GOOG-007 - Mid-bisection shutdown discards resolved outcomes and reports them `Uncertain`

Hunter severity: P2.

`mutation_stream`'s shutdown arm replaces whatever `operation_future` had
accumulated with `uncertain_outcomes(&ids, ..)` for the whole batch, so ids the
bisection already proved `Applied` or `NotFound` are reported `Uncertain`. Less
harmful than a false success (only read-backs), but it contradicts
`MutationApply::Terminate` ("Ids the bisection already resolved get their lane
before the stream ends"). Fix: carry accumulated outcomes out of the future
(shared state or channel) and mark only the in-flight sub-batch uncertain.

## GOOG-008 - Inventory profile `historyId` parse failure is classified as the caller's fault

Hunter severity: P2.

`inventory.rs::inventory_checkpoint` maps a non-numeric `profile.historyId` to
`Error::invalid_request` (`Request(Malformed)`, `ClientBug`). The same condition
in `GoogleAccount::open`, and the history response in `changes.rs`, map to
`Protocol(ParseFailed)` with `Acknowledged` evidence. Claims broken:
`reference/google.md` ("a value that is present but not a number is
`Protocol(ParseFailed)`") and the rule that local refusals of provider data
blame the provider.

## GOOG-009 - `reference/google.md` says lifecycle rename events carry no names; the code carries them

Hunter severity: P2.

The reference: "Rename events carry the same stable label id in `old` and
`new`; they are invalidation signals ... not a carrier for the old and new
display names." `scopes::diff_snapshots` fills `old_name` and `new_name`. One
is wrong; the doc looks stale.

## GOOG-010 - `discover_memberships` has no cache-hit path, and snapshot writes bypass the single-flight lock

Hunter severity: P2.

The comment in `scopes::discover_memberships` ("A cache hit performs no
request, and then the tally is legitimately zero") and `reference/google.md`
both describe a cache hit; the code calls `refresh_scope_snapshot`
unconditionally, always one `labels.list`. It writes the shared snapshot
outside the single-flight `refresh` mutex, so an older in-flight
`labels_for_flags` refresh finishing later can overwrite a newer snapshot (last
writer wins, not newest fetch). `containers_list` has the same unlocked write.

## GOOG-011 - `open_raw_rfc822` reports decoded size as `bytes_in`

Hunter severity: P2.

`blobs::open_raw_rfc822` sets `bytes_in = bytes.len()`, the decoded MIME; the
wire carried a base64url JSON envelope about a third larger. `open_blob`
reports the true transferred size. Claims broken: `reference/google.md` ("Blob
batches keep their own exact transferred size") and the "no lane reports a
number it did not measure" rule.

## GOOG-012 - `contact_create` returns an empty `ContactId` on a response with no `resourceName`

Hunter severity: P2.

`contacts::create` does `person.resource_name.unwrap_or_default()`, so a
malformed 200 yields `Ok(ContactId(""))`. Calendar create treats the analogous
missing id as `Protocol(MissingField)` with `Acknowledged` evidence
(`local_error_with_field`); this should too. An empty id fed back into get /
update / delete builds `/v1/:updateContact`-shaped URLs.

## GOOG-013 - `contact_update` is two writes with no partial-failure reporting

Hunter severity: P2.

`contacts::update` sends `updateContact` for fields, then `updateContactPhoto`
or `deleteContactPhoto`. If the photo call fails after the field write landed,
the caller gets the photo's plain error, which reads as nothing happened. The
calendar update path reclassifies the identical shape (move succeeded, patch
failed) as `Protocol(PartialResponse)` with `Acknowledged` evidence, and the
reference spends a paragraph on why; contacts has the same shape without it.

## GOOG-014 - RSVP writes `needsAction` for `Delegated` and `Unknown`, and misattributes attendee mismatch

Hunter severity: P2.

`calendar::rsvp_value` maps `RsvpStatus::Delegated`, `Unknown` and any future
variant to `"needsAction"`, so `event_rsvp(Delegated)` resets the user's
response on the server and returns `Ok(())`. It should be
`Unsupported(EventRsvp)` before any request.

The attendee is matched only by `email.eq_ignore_ascii_case(profile email)`.
Google marks the account's own attendee with `self: true`; on an alias, a
secondary or a delegated calendar the email match fails, and that failure is
classified `Request(Malformed)` / `ClientBug` although the input was valid and
the event state simply differs.

## GOOG-015 - `identity_update` writes `signature_text` into Gmail's HTML `signature` field unescaped

Hunter severity: P2.

`pim::identity_update` falls back from `signature_html` to `signature_text` and
sends the text verbatim as `signature`, which Gmail treats as HTML. Text with
`<`, `&` or newlines renders wrong or injects markup. `identities_list` maps
the field back only to `signature_html` (`signature_text: None`), so the round
trip changes the field's type; `signature_text` claims plain-text semantics the
code does not honor. Fix: HTML-escape and convert newlines to `<br>`, or refuse
`signature_text`.

## GOOG-016 - Some read streams and futures ignore the shutdown token

Hunter severity: P3.

`repair_inventory`, `open_blob`, `open_raw_rfc822`, `discover_memberships` and
every PIM, contacts and calendar future ignore `shutdown`. The reference's
rationale for cancelling the read streams ("a stream that outlived `close()`
kept calling Gmail through a deregistered `AccountNet` - unmetered, and
reporting the result as a transport fault rather than as a closed account")
applies to `repair_inventory` just as much. The reference names only four
streams, so no claim is broken; it is the same hole one stream over. Relatedly,
`scope_lifecycle_stream` selects on shutdown only during the wait: an in-flight
`labels.list` (possibly waiting for admission behind a long project hold) holds
the stream open after `close()`.

## GOOG-017 - `push_stream` drops health events on `Lagged` silently

Hunter severity: P3. Same shape as JMAP-009's lagged case.

A lag of more than 32 events (a burst of renewal `Warning`s with a slow
consumer) can swallow a `Terminated` or `Disconnected`, and nothing tells the
consumer health edges were lost.

## GOOG-018 - Unbounded 404 bisection cost, and a possible label-id 404 misattribution

Hunter severity: P3.

When many ids in a batch are gone (a mass delete raced by a bulk move), the
404 bisection can issue on the order of 2n `batchModify` calls at 50 quota
units each with no budget cap, up to about 100k units for one 1000-id batch.
Unconfirmed related risk: if Gmail answers 404 for an unknown LABEL id, the
bisection would blame every message as `NotFound(Message)`. Worth pinning with
a captured transcript.

## GOOG-019 - A malformed message id poisons the batch

Hunter severity: P3.

Gmail answers 400 for a syntactically invalid id. That is not bisected, so it
fans out `Request(Malformed)` across all 1000 ids. The 404 bisection exists for
this "one id fails the whole call" shape, and 400-on-invalid-id has it too.

## GOOG-020 - `FlagOp::Add` can be used to move messages

Hunter severity: P3.

`flags::user_label_id_from_flag` deliberately resolves system ids (for
`CATEGORY_*`), so a crafted `$gmail-label:INBOX:INBOX`,
`$gmail-label:TRASH:TRASH` or `$gmail-label:UNREAD:UNREAD` passed to
`FlagOp::Add` adds INBOX, TRASH or UNREAD through the flag lane, bypassing the
relocation rule (`move_placement_patch`: adds TRASH without removing INBOX) and
possibly contradicting `\Seen` in the same op. `canonical_flags` never emits
those spellings, so only a caller-forged flag gets there; the doc's "never
reach this spelling" argument covers reads, not writes.

## GOOG-021 - `content_disposition_inline` is a substring match

Hunter severity: P3.

`blobs::content_disposition_inline` matches `"inline"` anywhere in the header,
so `attachment; filename="inline-chart.png"` is reported `inline: true`.

## GOOG-022 - Contact search warmup is fatal; directory search warmup is not

Hunter severity: P3.

`contacts::search` fails the whole search on a transient warmup failure, while
`directory_search` explicitly tolerates one ("warmup only primes the cache").
The two paths contradict each other's rationale.
