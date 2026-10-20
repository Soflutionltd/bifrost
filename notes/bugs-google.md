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

## GOOG-013 - `contact_update` is two writes with no partial-failure reporting

Hunter severity: P2.

`contacts::update` sends `updateContact` for fields, then `updateContactPhoto`
or `deleteContactPhoto`. If the photo call fails after the field write landed,
the caller gets the photo's plain error, which reads as nothing happened. The
calendar update path reclassifies the identical shape (move succeeded, patch
failed) as `Protocol(PartialResponse)` with `Acknowledged` evidence, and the
reference spends a paragraph on why; contacts has the same shape without it.

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

## GOOG-023 - Event create and update still map `Delegated` and `Unknown` attendee status to `needsAction`

Lateral from the RSVP fix. `google_attendee` reuses the RSVP status mapper for
attendee payloads on event create and update, so `Delegated`, `Unknown` and
future statuses are written as `needsAction`. Decide whether those should be
refused or passed through, separately from the RSVP door.
