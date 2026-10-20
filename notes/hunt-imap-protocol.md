# Hunt: bifrost-imap protocol layer

Scope: `crates/imap/` outside `src/account/` (driver, wire reader, streams,
codec, dispatch consumers, auth), read against `reference/imap.md`,
`reference/sasl.md` and `reference/error-model.md`. No code was changed.

Findings are ordered by severity. Each one names the claim it breaks.
"Live" means a conforming server reaches it today. "Latent" means it needs a
code path, a server behaviour or a configuration the workspace does not
exercise yet.

---

## Live defects

### 1. `ImapConnection::logout()` cannot succeed against a conforming server

`connection/auth.rs` `logout()`, `connection/dispatch.rs` `LogoutConsumer`,
`connection/driver/mod.rs` `dispatch_response_loop`.

RFC 3501 Section 6.1.3 requires `* BYE` before the tagged OK. In
`dispatch_response_loop` every untagged response goes through
`process_untagged_prefix` first, and that function returns `Err(Bye)` as soon
as the side-effect digest says `had_bye`. Nothing special-cases LOGOUT. So:

- On a conforming server, `logout()` returns `Err(Error::Bye { attempt: InFlight })`.
  The BYE never reaches `LogoutConsumer::on_response`, and the tagged OK is
  never read.
- On a server that skips BYE, it returns `Err(ProtocolMissing("did not send
  mandatory BYE"))`.
- `LogoutConsumer::saw_bye` is dead state, and `Finalized::success` in that
  consumer cannot be reached.

Claims broken: the `logout()` doc ("validates the required response sequence:
the server MUST send `* BYE` followed by a tagged OK"), and
`LogoutConsumer`'s own doc ("Tracks whether the mandatory `* BYE` response was
received").

The only test is `a_refused_logout_reports_the_refusal_not_a_missing_bye`.
No test covers a successful LOGOUT, which is why this has survived.

Impact today is limited, because every in-workspace caller writes
`let _ = conn.logout().await` (`account/pool.rs`, `account/push.rs`). But the
error is a `Bye` stamped `InFlight`, which is the shape the account layer maps
to a provider-shutdown recovery class. Any caller that checks the result will
treat a clean logout as a server-initiated disconnect.

Fix direction: the BYE guard needs a notion of "a BYE is this command's
expected answer". For example, `short_circuit_on_bye` could take the
command's kind and pass a BYE to the consumer when the command is LOGOUT,
then let the tagged OK finalize. This is the "fix that opens a hole one layer
up" shape, so the IDLE, pipeline and literal-wait loops must keep treating
BYE as fatal. Only the LOGOUT dispatch path should relax.

### 2. A SASL failure raised inside a continuation leaves the AUTHENTICATE exchange open, and the connection is not retired

`connection/driver/mod.rs` `dispatch_response_loop`, the `Continuation` arm
(`consumer.on_continuation(c, &ctx)?`); `connection/dispatch/auth.rs`
`AuthenticateScramConsumer::on_continuation`; the `From<SaslError>` mapping.

When `on_continuation` returns `Err`, the `?` exits the response loop
straight away. No reply is written to the server's `+`, and the tagged
completion is never read. Whether the connection survives depends only on the
error's variant (`is_connection_fatal`):

- `verify_server_final` on a server-final with `e=...` gives
  `SaslError::AuthFailed`, which becomes `Error::Auth`. That is not
  connection-fatal.
- `scram_client_final` failures are mapped through `named_sasl_error`. The
  current `bifrost-sasl` only raises `Protocol` there (fatal), but the mapping
  routes any future `AuthFailed` or `InvalidCredential` to non-fatal variants.

For the `e=` case, the server is waiting for the client's response line (an
empty line, or `*` to cancel per RFC 3501 Section 6.2.2) and still owes the
tagged NO. The driver loop treats the error as non-fatal and keeps the
connection. The next command's line is then consumed by the server as the
SASL response. Its reply is the AUTHENTICATE tag's `NO`, which surfaces as
`Protocol("unexpected tag")`, or worse.

Claim broken: `reference/sasl.md` "Error mapping contract": "Everything a
protocol crate still maps from `Protocol` is either the peer's fault or raised
mid-exchange, where retiring the connection is required." `AuthFailed` is
raised mid-exchange here but is not retired. The error-model rule that tagged
NO/BAD "leave the connection reusable" assumes the exchange has completed;
here it has not.

The error also carries no transmission stamp. Auth errors are not `Unsent` in
fact, since the AUTHENTICATE line and the client-first are on the wire.

Fix direction: either (a) make every `on_continuation` error mid-exchange
fatal, by wrapping it as `InternalMidExchange` or stamping it and closing in
the driver, or (b) on a consumer error, answer the continuation with `*\r\n`
(the RFC 3501 cancel), then keep reading to the tagged NO and return the auth
error with the connection intact. (b) is the protocol-correct answer and
keeps the auth-failure lane meaningful.

Lateral: check `bifrost-smtp`'s SCRAM path for the same shape. `sasl.md`
says the two crates "must not differ".

### 3. `DriverEventSink`'s critical-event hard cap is documented as poisoning the connection, but every caller discards the result, so critical events are silently dropped

`connection/driver/event_sink.rs`. Every `emit` call site in
`driver/mod.rs`, `driver/pipeline.rs`, `driver/idle.rs` and
`driver/upgrade.rs` uses `let _ = event_sink.emit(...)`.

The docs say: "Exceeding the cap returns `EventOverflow::HardCap`", and
`HardCap` is "The connection is poisoned - the server is producing critical
events faster than the caller can consume them". No code ever matches
`HardCap`. The 4097th pending critical event (ALERT, BYE,
NOTIFICATIONOVERFLOW) is dropped. `try_emit` does not call
`record_drop_*` on that path, so no `QueueOverflow` marker is emitted either.
This is the one loss path the marker mechanism was built to make visible.

Claims broken: the `EventOverflow::HardCap` doc, and the sink's doc that only
"Requeryable / Resyncable events are dropped silently".

Fix direction: either act on `HardCap` (retire the connection, as documented)
or count the drop so the marker fires. The doc currently promises the first.

Smaller ordering wrinkle in the same file: when `drain_pending` stops on
`Full` with critical events still pending, `emit` goes on to `try_emit` the
new event. If the receiver freed a slot in between, the new event overtakes
older pending critical events, and the `QueueOverflow` marker can likewise
land after events it was meant to precede.

### 4. Body memory is unbounded per response; the FETCH budgets run only after the whole literal is buffered (twice)

`connection/wire.rs` `read_one` / `buffer_may_contain_complete_response`,
`codec/decode/primitives.rs` `literal` (`data.to_vec()`),
`connection/dispatch/fetch.rs` (`FetchConsumer::with_limit`,
`BoundedStreamingFetchConsumer`).

The framing pre-check accepts any literal count up to `i64::MAX` and keeps
reading until those octets arrive. `BytesMut` grows without limit, and nothing
in the wire reader caps a single response. Then `literal()` copies the
payload into a fresh `Vec<u8>`, so the peak is about twice the literal.
`FetchConsumer`'s `hard_limit` and the account's `RAW_FETCH_BUDGET` /
`HYDRATION_FETCH_BUDGET` are only checked in `on_response`, after
the response is fully parsed.

Claims broken: `reference/imap.md` (blob section): "Like every other body path
it carries a client-side byte budget ... crossing which is
`Error::FetchLimit`; the response size is the server's to choose, so the
unbudgeted buffered read this path used to do could be made arbitrarily large
by a corrupt or adversarial peer." The budget limits the total across
responses, not the single response a corrupt peer controls. The same doc's
"each body section is emitted as its own `Bytes` page as it comes off the
socket" also overstates: the section is emitted only after the whole FETCH
response is buffered and copied.

Fix direction: this is a structural rewrite. Give the reader a per-command
octet ceiling (the driver can hand it down from the consumer's budget) and
fail with `FetchLimit` / `Parse` once a declared literal count would exceed
it, before reading the octets. Retire the connection, since the remaining
octets will never be read. Separately, decode literals as `Bytes` slices
split off the `BytesMut` (zero-copy) instead of `to_vec()`. That halves the
peak and makes the `Bytes` page claim true.

### 5. Mailbox identity is decoded lossily and never round-trips: some server mailboxes are unaddressable or alias another

`codec/decode/mailbox.rs` `decode_mailbox_from_wire`, `codec/utf7.rs`
`decode_utf7`, and `MailboxName::from_decoded`.

A mailbox is kept only in decoded form, and the encoder re-derives wire bytes
with `encode_utf7`. `decode_utf7` is deliberately lenient (Postel). That
makes it non-injective and non-invertible:

- Raw 8-bit names, which the code says Exchange sends, decode to UTF-8 and
  re-encode as `&...-`. These are different octets on the wire.
- Base64 segments that encode printable ASCII (the comment cites Exchange)
  decode to plain ASCII and re-encode differently.
- Malformed segments and control bytes become literal text or U+FFFD, which
  cannot be re-encoded at all.
- In UTF-8 mode, `from_utf8_lossy` maps invalid bytes to U+FFFD.

So a mailbox listed by LIST can be impossible to SELECT, or two distinct wire
names can collapse onto one `MailboxName`, so an operation lands on the wrong
folder.

Claims broken: the module doc, "the decoder's output is always in decoded
user-facing form", is true. The defect is that this form is the only one kept
while it is also used as the address. Nothing in the reference states that
every listed mailbox is addressable, but `MailboxName` as an identity implies
it.

Fix direction: keep the raw wire octets alongside the display form in the
codec-constructed `MailboxName`, and send the original octets back when the
name came from the server.

Latent sibling: `gmail_label` (`codec/decode/envelope_fetch.rs`) always runs
`decode_utf7`, even in UTF-8 mode. Gmail advertises `UTF8=ACCEPT`. The
account layer does not ENABLE it today, so this is not reachable yet.

---

## Latent defects and contract drift

### 6. The "one literal marker parser" claim is false

`reference/imap.md`: "The literal marker parser (`connection/literals.rs`,
`literal_marker_at`) is shared by the read side's framing pre-check and the
encoder's walk over caller-written SEARCH / SORT / THREAD criteria."

Two more independent `{n[+]}\r\n` parsers exist:

- `codec/encode/mod.rs` `validate_search_criteria_crlf`. It parses the count
  as `usize`, has no `number64` ceiling, and handles `+` its own way. It is
  the validator `write_criteria` relies on ("The validator has already proven
  the payload is inside `criteria`").
- `connection/search_validation.rs` `search_criteria_literal_end`. It is the
  handle-side capability-gate scanner.

They agree today on every input I traced. But `write_criteria`'s correctness
rests on the validator and the writer agreeing about where payloads begin and
end, and the reference promises one parser. This is the "local restatement"
shape from AGENTS.md's standing lessons.

Fix: route the validator, and ideally the handle-side scanner, through
`literal_marker_at`, or correct the reference.

### 7. Capability gates for SEARCH criteria run against the handle snapshot, not live state

`connection/search_validation.rs` `validate_search_criteria_capabilities`
reads `state_rx` (the handle's snapshot). `reference/imap.md`: "Every
decision the wire bytes depend on is made here [prepare_command], at the
moment the command reaches the head of the queue ... capability gates". The
MODSEQ, WITHIN, SAVEDATE, OBJECTID and SEARCHRES checks for free-form criteria
are therefore stale-snapshot decisions. These are refusals, not byte choices,
so the worst case is a spurious `MissingCapability` or a server BAD. But they
are a second gate site that the "one command path" story says does not exist.

### 8. PREAUTH greeting with `TlsMode::StartTls` fails with a misleading error class

`connection/lifecycle.rs` `connect_with_tls_connector_metered` then
`run_starttls_upgrade` then `prepare_command`. A cleartext PREAUTH greeting
puts the session in Authenticated, so STARTTLS is refused as
`StateChangedBeforeSend("session moved to Authenticated ...")`. The refusal is
secure (nothing is sent in clear), but the error says the session "moved
while the command waited". That is a retry-after-refresh class for what is
really a TLS-policy refusal (RFC 9051 Section 6.1.1: the client must close).
A reconnect loop will retry it forever. It should be a dedicated refusal,
such as `StartTlsUnavailable` or a TLS-policy error.

### 9. A failed post-STARTTLS CAPABILITY leaves a live TLS connection with empty capabilities and `tls_active == false`

`connection/driver/upgrade.rs` `run_starttls_upgrade` clears capabilities
before the re-fetch. If the CAPABILITY that follows fails non-fatally
(`ProtocolMissing`, or a tagged NO), the driver arm in `driver_task` keeps the
connection. The handle's `starttls_with_connector` returns `Err` before
`tls_active.store(true)`. The result is a TLS-wrapped connection that reports
`is_encrypted() == false` and an empty capability set. `authenticate_best`
then refuses every rung on cleartext grounds, or with
`MissingCapability`, on a connection that is in fact encrypted.

Either the whole upgrade should be fatal once the stream is swapped, or
`tls_active` should follow the stream rather than the call's result.

### 10. `InFlightGuard` poisons a connection whose command was never submitted

`connection/auth.rs`: in `submit_regular`, `submit_streaming`,
`submit_with_continuations` and `submit_upgrade`, the guard is created before
`cmd_tx.send(dcmd).await`. If the caller's future is dropped while that send
is pending (the channel is full at capacity 16), the guard marks the
connection abandoned although the driver never received the command. A
healthy connection is retired and its last handle aborts the driver.

This breaks the `abandoned` field's own doc ("Set when a submitted command's
future is dropped before its result arrives"). It is the conservative
direction, but it is wrong evidence. Create the guard after the send
completes; `idle()` already does this.

### 11. Commands whose caller has gone are still executed

`driver_task`'s `Run` arm runs `prepare_command` and sends whatever it
dequeues, without checking `result_tx.is_closed()`. A command queued behind
a slow one whose caller timed out (stamped `timeout_inflight`, so reconcile is
the honest answer) is still written later, including non-idempotent
APPEND / MOVE / STORE. This is consistent with the documented
"cancellation-safe, keeps executing", so it is not a contract break. But for
a command that has not reached the wire, skipping it would turn an `InFlight`
reconcile into a true `Unsent` and remove a duplicate-append window. Filed as
an opportunity, not a defect.

### 12. Setup timeout semantics: the "cumulative" peer budget is reset per phase

`connection/wire.rs` `PeerBudget` doc: "One cumulative peer wait allowance,
shared across stream upgrades." `lifecycle.rs` calls
`set_peer_timeout(Some(timeout))` three times (greeting, the CAPABILITY
fetch, STARTTLS), and each call builds a fresh `PeerBudget`. Connection setup
can therefore take roughly `connect + TLS + 3 x timeout`, and the budget is
not cumulative across the phases where it is reset. Either share one budget
across setup or reword the doc.

---

## Observations (not defects)

- `LogoutConsumer` aside, `logout_best_effort` also stops at the BYE (the
  prologue errors) without reading the tagged OK. That is harmless at
  teardown and noted only for completeness with finding 1.
- `CompressedStream::write_all` allocates a fresh 16 KiB `deflate_buf` on
  every write call, and the wire sender calls it once per admitted chunk.
  Keep one scratch buffer in the struct.
- `expand_uid_ranges` uses `u32::MAX` as the `*` sentinel, so a legal UID of
  4294967295 is refused as "truncated". This is documented, but it is the
  parser conflating a value with a token. A typed `SeqBound::Star` in
  `UidRange` would remove it.
- `CommandWriter::string` silently strips NUL bytes from any string,
  mailbox names included, and only logs a warning. A NUL-bearing name is
  rewritten into a different name rather than refused. `MailboxName`
  validation probably rejects NUL first; I did not verify every `string()`
  caller.
- The authenticate ladder's `Err(other) => return Err(other)` aborts on the
  first non-`MissingCapability` failure, so a server-side NO on
  SCRAM-SHA-256-PLUS (for example a broken channel-binding configuration)
  never falls through to the next rung. This looks deliberate (downgrade
  safety), but the rationale is not written down next to the arm.
