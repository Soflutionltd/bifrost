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

Filed from the October 2026 defect hunt: the google scope. Coverage note:
`cloud.rs`, `client.rs` (quota and redirect plumbing) and most of `calendar.rs`
got only a light read, so a clean result there means little.

## GOOG-028 - People contact writes use the top-level `Person.etag`

Lateral from the bulk caller-version work. The People `updateContact`
documentation specifies the concurrency token as `metadata.sources.etag` on the
contact source, but the account uses the top-level `Person.etag`. Verify which
the API actually enforces before wiring caller versions into People writes;
until then Google contact writes refuse a supplied version.

## GOOG-027 - The error model has no typed continuation for a failure after a committed first leg

**Needs a technical implementation specification** (owner, 2026-10-21): too
consequential to resolve by ruling. Specify together with the JMAP PIM send
path compensation proposal (JMAP-034), which answers the same question.

Needs a ruling. A calendar update that moves an event and then fails its
destination PATCH now keeps the original recovery class and carries diagnostic
text telling a person to retry from the destination id. An automated caller
acting on `Retry(SameRequest)` without reading the diagnostic can still replay
the source request, where the event no longer lives. A typed continuation
carrier (the rewritten target request or id) in the shared error model would let
the engine do it correctly; the same shape applies to the contact update's
second leg.

