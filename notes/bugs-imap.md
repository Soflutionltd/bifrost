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

## IMAP-002 - A failed SASL continuation auth error carries no transmission evidence

Scope: imap protocol. Residue of a closed fix (the connection is now answered
with `*`, the tagged NO is read, and the connection stays usable).

`Error::Auth` has no attempt field and `with_attempt` cannot add one, so the
authentication failure raised mid-exchange cannot be stamped although the
AUTHENTICATE line and client-first are on the wire. Documented at the code site
and in `reference/imap.md`; the error type itself was outside the fix's files.

## IMAP-003 - `DriverEventSink` hard cap is documented as poisoning the connection, but every caller discards the result

Live. Scope: imap protocol.

`connection/driver/event_sink.rs`; every `emit` call site in `driver/mod.rs`,
`driver/pipeline.rs`, `driver/idle.rs`, `driver/upgrade.rs` uses
`let _ = event_sink.emit(...)`. The docs say exceeding the cap returns
`EventOverflow::HardCap`, "The connection is poisoned - the server is producing
critical events faster than the caller can consume them". Nothing matches
`HardCap`. The 4097th pending critical event (ALERT, BYE,
NOTIFICATIONOVERFLOW) is dropped, and `try_emit` does not call `record_drop_*`
on that path, so no `QueueOverflow` marker fires: the one loss path the marker
was built to make visible. Also breaks the sink doc that only "Requeryable /
Resyncable events are dropped silently".

Fix: act on `HardCap` (retire the connection, as documented) or count the
drop so the marker fires.

Ordering wrinkle in the same file: when `drain_pending` stops on `Full` with
critical events pending, `emit` goes on to `try_emit` the new event; if a slot
freed in between, the new event overtakes older pending critical events, and
the `QueueOverflow` marker can land after events it should precede.

## IMAP-004 - Per-response body memory is unbounded; FETCH budgets run only after the literal is buffered twice

Live. Scope: imap protocol.

`connection/wire.rs` `read_one` / `buffer_may_contain_complete_response`,
`codec/decode/primitives.rs` `literal` (`data.to_vec()`),
`connection/dispatch/fetch.rs` (`FetchConsumer::with_limit`,
`BoundedStreamingFetchConsumer`). The framing pre-check accepts any literal
count up to `i64::MAX` and reads until the octets arrive; `BytesMut` grows
without limit. `literal()` then copies into a new `Vec<u8>`, so the peak is
about twice the literal. `FetchConsumer`'s `hard_limit` and the account's
`RAW_FETCH_BUDGET` / `HYDRATION_FETCH_BUDGET` are checked only in
`on_response`, after full parse.

Claims broken: `reference/imap.md` blob section ("Like every other body path it
carries a client-side byte budget ... the unbudgeted buffered read this path
used to do could be made arbitrarily large by a corrupt or adversarial peer");
the budget limits the total across responses, not the single response a peer
controls. Its "each body section is emitted as its own `Bytes` page as it comes
off the socket" also overstates: emission happens after the whole FETCH
response is buffered and copied.

Fix direction (structural): a per-command octet ceiling in the reader (handed
down from the consumer's budget), failing with `FetchLimit` / `Parse` once a
declared literal count would exceed it, before reading the octets, and
retiring the connection. Separately, decode literals as `Bytes` slices split
off the `BytesMut` instead of `to_vec()`, halving the peak and making the page
claim true.

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

## IMAP-006 - The "one literal marker parser" claim is false

Latent. Scope: imap protocol.

`reference/imap.md`: `literal_marker_at` (`connection/literals.rs`) "is shared
by the read side's framing pre-check and the encoder's walk over caller-written
SEARCH / SORT / THREAD criteria". Two more independent `{n[+]}\r\n` parsers
exist: `codec/encode/mod.rs` `validate_search_criteria_crlf` (parses the count
as `usize`, no `number64` ceiling, handles `+` its own way; it is the validator
`write_criteria` relies on, "The validator has already proven the payload is
inside `criteria`"), and `connection/search_validation.rs`
`search_criteria_literal_end` (the handle-side capability-gate scanner). They
agree on every traced input, but `write_criteria` correctness rests on
validator and writer agreeing, and the reference promises one parser. Route
both through `literal_marker_at`, or correct the reference.

## IMAP-007 - SEARCH criteria capability gates run against the handle snapshot, not live state

Latent. Scope: imap protocol.

`connection/search_validation.rs` `validate_search_criteria_capabilities` reads
`state_rx` (the handle's snapshot). `reference/imap.md`: "Every decision the
wire bytes depend on is made here [prepare_command], at the moment the command
reaches the head of the queue ... capability gates". MODSEQ, WITHIN, SAVEDATE,
OBJECTID and SEARCHRES checks for free-form criteria are stale-snapshot
decisions. They are refusals, so the worst case is a spurious
`MissingCapability` or a server BAD, but they are a second gate site the "one
command path" story says does not exist.

## IMAP-008 - PREAUTH greeting with `TlsMode::StartTls` fails with a misleading retryable error class

Latent. Scope: imap protocol.

`connection/lifecycle.rs` `connect_with_tls_connector_metered` ->
`run_starttls_upgrade` -> `prepare_command`. A cleartext PREAUTH puts the
session in Authenticated, so STARTTLS is refused as
`StateChangedBeforeSend("session moved to Authenticated ...")`. Secure (nothing
sent in clear), but it says the session moved while the command waited, a
retry-after-refresh class for what is a TLS-policy refusal (RFC 9051 s6.1.1:
the client must close). A reconnect loop retries it forever. It should be a
dedicated refusal such as `StartTlsUnavailable`.

## IMAP-009 - A failed post-STARTTLS CAPABILITY leaves a live TLS connection reporting cleartext and no capabilities

Latent. Scope: imap protocol.

`connection/driver/upgrade.rs` `run_starttls_upgrade` clears capabilities
before the re-fetch. If that CAPABILITY fails non-fatally (`ProtocolMissing`,
tagged NO), the driver keeps the connection, and `starttls_with_connector`
returns `Err` before `tls_active.store(true)`. The result is a TLS-wrapped
connection with `is_encrypted() == false` and empty capabilities;
`authenticate_best` then refuses every rung on cleartext grounds or with
`MissingCapability`. Make the upgrade fatal once the stream is swapped, or have
`tls_active` follow the stream rather than the call result.

## IMAP-016 - VANISHED ranges are expanded UID by UID, and every expanded UID becomes a `Removed` change

Scope: imap account.

`changes.rs` `run_qresync` (SELECT-side `vanished` and the
`FetchStreamItem::VanishedEarlier` arm), `record_removed_change`; `mutate.rs`
`open_folder_for_mutation`; `push.rs` `absorb_idle_event`;
`folder_registry.rs` `expand_range`.

a) Correctness. `record_removed_change` pushes `ScopeChange::Removed` for every
UID in the range whether or not it is in `known_uids`. The second command,
`UID FETCH 1:* (...) (CHANGEDSINCE m VANISHED)`, runs over `1:*`, and RFC 7162
s3.2.6 lets that reply name any UID expunged since `m`, including messages that
arrived and were expunged between syncs. Servers also coalesce VANISHED into
ranges covering non-existent UIDs. The result is `Removed` for ids with no
membership record. The CONDSTORE path explicitly guards the symmetric hazard
(`basic_updated_change` doc; `reference/imap.md` CONDSTORE bullet); QRESYNC
does not. Fix: intersect each range with `live_uids` before emitting.

b) Resource. `expand_range` materialises `start..=end` into a `Vec<u32>`.
`VANISHED (EARLIER) 1:4000000000` (legal syntax) allocates on the order of
16 GB and emits billions of changes, in the changes stream, every mutation
SELECT and every IDLE VANISHED event. `CompactUidSet` is already range-based;
removal, modseq-cache clear and the diff should operate on ranges intersected
with the known set. The same expansion runs on `ResponseCode::Modified` in
`modified_uids` (`mutate.rs`), bounded by the request's set but still
better range-based.

## IMAP-017 - `Account::close()` is not cancellation-safe or concurrency-safe, and leaves SMTP submission open

Scope: imap account.

`close.rs`.
- `closed.swap(true)` comes first. If the close future is dropped after the
  swap and before `pool.close().await` finishes (the drain awaits LOGOUT under
  a timeout), every later `close()` returns `Ok(())` at once while sessions
  remain connected; a concurrent second `close()` also returns `Ok(())` while
  the first is still draining. Both break `reference/imap.md`
  ("`Account::close()` therefore means no session owned by that account is
  still connected when it returns"). Standing-lesson shape: a teardown flag
  set before teardown is done.
- `submission: Option<Arc<SubmissionTransport>>` is never closed. The SMTP
  transport is pooled (`SmtpSubmissionConfig::with_pool`), so its sessions
  outlive `close()`, and `send_message` / `send_raw_message` / `draft_send`
  never check `closed`. After `close()` they still dial SMTP and deliver; only
  the Sent APPEND fails, logged only, so the caller gets `Ok` with a fallback
  `imapmsgid1:` id.

## IMAP-018 - IDLE sessions are uncounted, so the connection footprint exceeds `pool_cap`

Scope: imap account.

`factory.rs` (`data_cap = pool_cap - 1`), `pool.rs` `dial_idle`, `push.rs`
`ensure_idle_tasks`. `dial_idle` takes no permit, and a non-NOTIFY account
starts `idle_connection_budget` (default 4) workers, each with its own session.
Defaults give 3 data plus 4 IDLE sessions, 7 against `pool_cap` 4, plus
`replace_selected_member` re-dials.

Claims broken: `reference/imap.md` `pool.rs` bullet ("Push lane reserves one
slot; data lanes share the rest") and Push lifecycle ("Four matches `pool_cap`
... keeping the account's total connection footprint inside the low per-user
allowance many providers enforce"). On a provider with a per-user limit of
about 5, the eighth dial is refused, usually a data checkout during sync.
Either IDLE workers draw from a shared session budget, or the docs and
`ImapAccountConfig` state the real total (`pool_cap - 1 +
idle_connection_budget`).

## IMAP-020 - Push events broadcast before a `push_stream` receiver exists are lost

Smell, imap account. Same shape as JMAP-009.

`push.rs` `push_stream`: `broadcast::Sender::send` with zero receivers drops the
event, so a `push_subscribe` that starts workers before the engine attaches its
stream loses the first events silently. bifrost-sync may always subscribe the
stream first; if so, that ordering is an undocumented requirement.

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

## IMAP-023 - QRESYNC runs a redundant second round trip over `1:*`

Scope: imap account.

`changes.rs` `run_qresync`. A QRESYNC SELECT (RFC 7162 s3.2.5) already returns
`VANISHED (EARLIER)` and the changed FETCHes since `modseq`. The code consumes
both, then issues `UID FETCH 1:* (UID FLAGS MODSEQ) (CHANGEDSINCE modseq
VANISHED)` returning the same data, deduplicated by `fetch_change_seen` /
`removed_seen`. Cost: one extra round trip per folder per poll on the cheap
strategy, plus IMAP-016a's widened VANISHED over `1:*`. If it is a deliberate
guard for non-conformant servers, the reference should say so; it lists both
sources without explaining why both are needed.

## IMAP-024 - Mutation SELECT carries the full QRESYNC baseline on every mutation

Scope: imap account.

`mutate.rs` `open_folder_for_mutation` passes the folder's in-memory cursor to
`select_folder`, so every mutation batch on a QRESYNC account sends the full
known-UID set and gets back every VANISHED and changed FETCH since the last
sync, used only to warm the MODSEQ cache (and expanded per UID, IMAP-016b). A
one-message flag flip after a long idle gap pays for a full resync payload. A
plain SELECT suffices.

## IMAP-025 - Smaller imap account observations

Smells, imap account.

- `run_qresync` / `finish_changes` call `folders.set_cursor` before the
  `Final` / `Done` send. An abort in between advances the in-memory cursor past
  what the consumer checkpointed. Read today only for mutation SELECT
  parameters (IMAP-024), but the ordering is backwards for future readers.
- `expunge_uids_or_fall_back`: the `UID SEARCH DELETED` then plain `EXPUNGE`
  fallback is check-then-act; a `\Deleted` set by another client between the
  two is expunged. The doc's guarantee holds at SEARCH time, not EXPUNGE time;
  the reference should name the window, which cannot close without UIDPLUS.
- `mutate.rs` ignores `IdempotencyKey` on all three bulk paths (`_key`).
  Acceptable for IMAP, but `reference/imap.md` does not say so while other
  providers' docs explain their key handling.
- Basic strategy cannot observe flag changes on a non-CONDSTORE server. The
  reference describes only additions and expunges for Basic, but
  `CursorDescriptor` for Basic does not signal "flags stale", and nothing in the
  engine-facing types says a Basic folder's flags drift until re-inventory.
