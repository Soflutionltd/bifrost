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

## GOOG-024 - `update_contact_photo` HTTP method is unverified

Lateral. The wave fixer reports `contacts::update_contact_photo` sends POST
while the People API reference specifies PATCH. Unconfirmed; check the
documented method for `:updateContactPhoto` before changing anything.

## GOOG-018 - A 404 on a label id may be misattributed to every message

Residue: the 404 bisection is now capped by a shared 128-call budget (it was
about 2n calls and up to roughly 100k quota units for one 1000-id body). Still
unconfirmed: whether Gmail answers 404 for an unknown LABEL id, in which case
the bisection would blame every message as `NotFound(Message)`. Needs a captured
provider response; the ambiguity is documented in code and the reference.

## GOOG-027 - The error model has no typed continuation for a failure after a committed first leg

Needs a ruling. A calendar update that moves an event and then fails its
destination PATCH now keeps the original recovery class and carries diagnostic
text telling a person to retry from the destination id. An automated caller
acting on `Retry(SameRequest)` without reading the diagnostic can still replay
the source request, where the event no longer lives. A typed continuation
carrier (the rewritten target request or id) in the shared error model would let
the engine do it correctly; the same shape applies to the contact update's
second leg.

## GOOG-028 - Smaller observations after the adapter shutdown wrapping

- `host_attachment` is outside the shutdown wrapper; cancelling its multi-request
  future could skip Drive resumable-session cleanup. Needs token-aware cleanup.
- A cancelled `AccountFuture` yields a generic in-flight error with the
  operation but no resource-specific scope, which limits recovery for
  multi-step writes (event moves, contact updates).
- Comments in `inventory.rs`, `blobs.rs` and `scopes.rs` still say the adapter
  does not wrap those streams; that is stale.

## GOOG-024 follow-up

`update_contact_photo` is confirmed to send POST at `contacts.rs` while Google
documents PATCH for `:updateContactPhoto`. Change it and pin the method with a
test.
