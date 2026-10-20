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

## GOOG-023 - Event create and update still map `Delegated` and `Unknown` attendee status to `needsAction`

Lateral from the RSVP fix. `google_attendee` reuses the RSVP status mapper for
attendee payloads on event create and update, so `Delegated`, `Unknown` and
future statuses are written as `needsAction`. Decide whether those should be
refused or passed through, separately from the RSVP door.

## GOOG-006 - An unresolved `$gmail-label:` id does not trigger one refresh before it is declared unsupported

Residue: label create, rename and delete now invalidate the vocabulary cache.
The write-side miss is still open: a fresh cache can lack a caller-supplied
`$gmail-label:` id (a label created by another client), and translation in
`mutation.rs` declares it unsupported without one forced, serialized refresh and
retry.

## GOOG-025 - Calendar update replaces a failed second leg with `PartialResponse`

Lateral from the contact-update fix, which stopped doing this because it
overrode the original recovery class (a permanent 403 became a retry, auth loss
and rate-limit timing were lost). The calendar update path reclassifies the
identical shape (move succeeded, patch failed) as `Protocol(PartialResponse)`
with `Acknowledged` evidence, and `reference/google.md` justifies it at length.
Check whether it has the same recovery-class defect and, if so, decorate
instead of replacing.

## GOOG-024 - `update_contact_photo` HTTP method is unverified

Lateral. The wave fixer reports `contacts::update_contact_photo` sends POST
while the People API reference specifies PATCH. Unconfirmed; check the
documented method for `:updateContactPhoto` before changing anything.
