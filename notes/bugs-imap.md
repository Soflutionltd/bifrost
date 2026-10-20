# bifrost-imap defect findings

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

Filed from the October 2026 defect hunt: the imap protocol and imap account
scopes. Push-error triage in the IDLE worker is filed with the cross-provider
entry SHARED-013; the private address parser in `pim.rs` with SHARED-004.
"Live" and "latent" are the hunters' labels.

## IMAP-005 - Mailbox identity is decoded lossily and never round-trips

Live. Scope: imap protocol.

`codec/decode/mailbox.rs` `decode_mailbox_from_wire`, `codec/utf7.rs`
`decode_utf7`, `MailboxName::from_decoded`. A mailbox is kept only in decoded
form, and the encoder re-derives wire bytes with `encode_utf7`. The deliberately
lenient decoder is non-injective and non-invertible:

- raw 8-bit names (which the code says Exchange sends) decode to UTF-8 and
  re-encode as `&...-`, different octets;
- base64 segments encoding printable ASCII (Exchange, per the comment) decode
  to plain ASCII and re-encode differently;
- malformed segments and control bytes become literal text or U+FFFD, which
  cannot be re-encoded;
- in UTF-8 mode, `from_utf8_lossy` maps invalid bytes to U+FFFD.

A LISTed mailbox can be impossible to SELECT, or two wire names can collapse
onto one `MailboxName`, so an operation lands on the wrong folder. The module
doc ("the decoder's output is always in decoded user-facing form") is true; the
defect is that this form is the only one kept while also used as the address.
`MailboxName` as an identity implies addressability. Fix: keep the raw wire
octets alongside the display form in codec-constructed names and send them
back when the name came from the server.

Latent sibling: `gmail_label` (`codec/decode/envelope_fetch.rs`) always runs
`decode_utf7`, even in UTF-8 mode. Gmail advertises `UTF8=ACCEPT`; the account
layer does not ENABLE it today.

## IMAP-022 - The MODSEQ cache records server state the consumer has not seen, so `UNCHANGEDSINCE` guards less than documented

Scope: imap account.

`push.rs` `absorb_idle_event` (records from IDLE FETCH), `mutate.rs`
`open_folder_for_mutation` (from the mutation's own SELECT QRESYNC
`changed_messages`), and inventory and changes recording as they stream.
`UNCHANGEDSINCE` exists to reject a write based on a stale view, but the cache
advances from any server read, including the SELECT immediately before the
STORE and push events not yet reconciled. A concurrent change by another
client after the consumer's last hydration is normally folded into the guard
value just before the STORE, which then succeeds over it. Capabilities
honestly advertise `MutationConcurrency::None`, so no promise is broken, but
`reference/imap.md` "Concurrency conflicts and UNCHANGEDSINCE" describes a
guard that, as wired, protects only the sub-second SELECT-to-STORE window.
Take the guard value from the consumer (`ServerVersion::ModSeq` already travels
in inventory entries), or say plainly how narrow the window is.

## IMAP-018 - IDLE sessions are still uncounted against `pool_cap`

Scope: imap account. Residue: the config docs and reference now state the real
total (`pool_cap - 1 + idle_connection_budget`). Whether IDLE workers should
instead draw from a shared session budget, so the footprint stays inside
`pool_cap` on providers with a low per-user connection limit, is a product
decision.

## IMAP-004 - FETCH budget handoff and copy-free literals

Residue of the per-command literal ceiling (the reader now checks cumulative
literal declarations against the budget before reading payload, and a breach is
a fatal `Parse`). Still open:
- `account/blob.rs` must call `uid_fetch_stream_budgeted` with its existing
  budget; until then raw-message streaming checks its budget only after
  decoding.
- FETCH response fields in `crates/types` (`fetch.rs`) own `Vec<u8>`, so the
  literal is still copied and nothing is emitted incrementally; splitting the
  `BytesMut` alone cannot change that.
- Non-literal syntax and parser overhead are outside the literal ceiling.

## IMAP-017 - `close()` still cannot await a dial that is inside authentication

Residue: account close now has shared completion, protected pool teardown, send
gating and SMTP submission shutdown. IMAP dials still inside authentication are
not registered with the pool, so `close()` cannot wait for them (needs
`account/pool.rs`), and closing the pool directly is still not
cancellation-safe; only the account-level close is protected.

## IMAP-002 - `Error::Auth` cannot carry transmission evidence

Scope: imap protocol. The SASL continuation failure now cancels with `*` and
keeps the connection, but `Error::Auth` has no attempt field, so the failure
cannot be stamped although AUTHENTICATE and client-first are on the wire. Add
the field and let the exact `Error::Auth` match in `account/error.rs` carry it;
the constraint is documented in `error.rs`.

## IMAP-029 - Smaller observations after the account fixes

- `account/inventory.rs` advances the in-memory cursor before sending the
  checkpoint, the ordering the changes path just fixed.
- `account/error.rs` still words the STARTTLS refusal as "server does not
  advertise STARTTLS", which is wrong for a PREAUTH greeting with the capability
  advertised.
- Residual limits documented rather than fixable: the plain `EXPUNGE` fallback
  window without UIDPLUS, bulk mutation ignoring `IdempotencyKey`, and Basic
  folders being unable to observe flag changes without CONDSTORE.
