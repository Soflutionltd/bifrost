# bifrost-smtp reference

Current architecture of the SMTP client crate. Descended from lettre 0.11.22; the bifrost fork has substantially diverged. Native-tls only.

## Port defaults and local configuration

`default_port(Dialect, PortTls)` owns the TCP defaults used by blocking and
async builders and connection URLs: SMTP plaintext 25 (relay),
STARTTLS (required or opportunistic) 587, implicit TLS 465. LMTP retains its
port 24 convention. Explicit port overrides take precedence.
`default_submission_port(PortTls)` owns the client message-submission defaults
(RFC 6409) that IMAP-composed accounts use: plaintext and STARTTLS 587,
implicit TLS 465. Plaintext submission deliberately does not fall to the relay
port 25: a user configuring a mail account against an internal non-TLS relay
expects the submission service, and client networks commonly block outbound 25.

Malformed connection URLs, missing hosts, unsupported schemes or SMTP TLS
query values, usernames without URL passwords, invalid percent escapes or UTF-8
in decoded credentials or hello names, malformed certificates and identities,
and TLS connector construction failures are local
`InvalidInput` errors, translated as `Request(Malformed)`. They need repair by
the user or administrator; an enterprise send queue must not retry a typo as a
network outage. `smtps` always selects implicit TLS. Both schemes reject unrecognized `tls`
query values as `InvalidInput`: an enterprise administrator expects a security
setting typo to be reported. Recognized values are `required` and
`opportunistic`; with `smtps` both retain implicit TLS. Repeated `tls` query
parameters are also `InvalidInput`, even when their values agree: an enterprise
administrator expects ambiguous security configuration to be refused rather
than silently choosing one value. Duplicate detection uses decoded query keys,
so percent-encoding a key does not bypass it. `tls` is the only query parameter
the parser consumes; unrecognized query parameters remain ignored.
Runtime TLS handshakes
remain transport errors. IMAP's builder
conversion to its own `InvalidInput` is intentional: those builder calls only
construct local TLS parameters and perform no network I/O.

## Submission progress and reply classes

`SendProgress` holds one phase: `Envelope`, `DataIssued`, `BodyStarted`,
`BodySent`, or `FinalReply(Response)`. Resolution classifies still-pending or
accepted recipients before body admission as failed with `Unsent` evidence,
even when an exit forgot to mark them. A lost DATA-command reply is likewise
unsent: the machine has issued no body write. Once a body write is issued,
missing final replies remain uncertain. This lets an Outlook user retry an
unsent message without risking duplicates after an ambiguous upload.

The send core requires exactly 354 before writing a DATA body, and 2yz
completion replies at envelope, reset and final-completion boundaries.
`Code` and `Response` expose `is_completion` and `is_intermediate` alongside
`is_positive`, which remains useful for authentication exchanges. A 3yz final
reply never proves delivery: SMTP batch resolution returns uncertain lanes,
and direct SMTP and LMTP abort with a parse error carrying in-flight evidence.
LMTP batches preserve earlier definitive statuses and mark unresolved recipients
uncertain before aborting on an intermediate final status.

## Feature boundaries

The default `account-error` feature enables shared account-error translation,
recipient batch sends, `SubmissionStatus` / `submission_status`, and checked
conversion from shared account addresses. Disabling it retains direct blocking
SMTP/LMTP and, with `tokio`, async SMTP/LMTP, including password and OAuth
credentials. The local transmission-state vocabulary remains independent of
bifrost-types so direct clients can retain send evidence without account APIs.
The shared bifrost-types dependency is unconditional so header envelope
resolution uses the same owner with or without account-error.
Mixed account/wire test suites require `account-error`; independent parser,
encoder, framing and network suites remain available without it.

`test-support` exposes fresh `Transcript` factories through `ScriptedConnector`
on both blocking and async SMTP/LMTP builders. Scripted connections require
plaintext TCP configuration (`Tls::None` and no Unix socket route); both
runtimes apply the real dial plan's platform and TLS/socket validation first,
then refuse unsupported scripted configurations before invoking the factory.
Accepted scripted connections retain normal greeting, hello and authentication
processing, and open no sockets.
The async-only OAuth resolver is compiled only with `tokio`; the blocking
snapshot resolver remains available in every build.

## Transport types

Sync and async, SMTP and LMTP:

- `SmtpTransport` / `AsyncSmtpTransport` - submission to MX/relay.
- `LmtpTransport` / `AsyncLmtpTransport` - local delivery via LMTP.

`Transport::Ok` is `Response` for SMTP, `Vec<Response>` for LMTP (one per envelope recipient, original order preserved).

`transport::smtp::Builder<D, R>` owns `SmtpInfo` and pool configuration once.
`Transport<D, R>` owns the runtime's pool. The dialect markers are `Smtp` and
`Lmtp`; the runtime markers are `Blocking` and, with `tokio`, `Tokio`.
The four existing transport names and four builder names are type aliases of
this product. Async transports have no executor type argument, and `build()`
requires no executor selection:

```rust,ignore
let transport = AsyncSmtpTransport::relay("smtp.example.com")?
    .password("username", "password")
    .build();
```

Every builder setter has one implementation in `surface.rs`. Selecting `port`
or `tls` clears an earlier Unix socket route; selecting `unix_socket` clears
TLS. This rule is identical for blocking and Tokio LMTP. An explicit TCP
configuration must not silently retain an old local-delivery destination.
Only LMTP exposes Unix socket configuration; only SMTP exposes relay and URL
constructors. The URL parser consumes the shared builder directly.

The transport method body is written once in `transport_methods!`, including
batch validation before checkout and recipient resolution afterward. Expansion
selects the dialect's connection operation and the runtime's calling convention:
blocking returns a result directly, while Tokio awaits its pooled connection.
A future-only connection trait would force blocking callers to poll a future;
syntactic runtime substitution keeps the two native I/O contracts without
copying their sequencing. Direct SMTP still returns one `Response`, and direct
LMTP returns ordered per-recipient responses. VRFY, EXPN, NOOP, DATA, BDAT,
batch sends, shutdown, pooling and the message trait's DKIM handling remain
available through the aliases. The `tokio` feature remains optional.

Tokio pools and clients are concrete rather than generic over the sealed
executor. `Executor` and `TokioExecutor` retain their task/timer service surface;
they no longer configure a transport or own dialing. `dial::connect_async`
takes `&SmtpInfo`, applies the TLS/socket plan, and performs async dialing.
There is no separate connection-options record.

Async transports support Tokio. TLS and plaintext URL parsing via `from_url` use the Tokio backend.

`BoxedTransport<Ok, Error>` and `BoxedAsyncTransport<Ok, Error>` erase the concrete type. `Box<T>` and `Arc<T>` get blanket `Transport` / `AsyncTransport` forwarding.

**The blocking half stays. Do not re-delete it.** `SmtpTransport`,
`LmtpTransport`, the `Transport` trait, the blocking pool and socket funnel,
`oauth2_token_blocking`, the blocking examples and the `tokio` cargo feature were
once removed wholesale on the reasoning that the only IN-WORKSPACE consumer is
async, and were restored at the repository owner's instruction. That reasoning
does not hold for a library crate, whose consumers are outside this workspace by
definition; "nothing calls it" established by grepping here is a fact about the
workspace, not about who uses `SmtpTransport`.

The two halves share session sequencing, transaction sequencing and reply
framing. See "Protocol core and I/O adapters". Dialing, token resolution, TLS
handshakes, byte I/O, bandwidth admission and deadlines remain adapter-owned.
The session and send conformance suite defines each transcript once and runs it
through both adapters; adapter-specific timeout and cancellation tests stay
separate.
There are two deliberate behavioural differences:
the `abort()` timeout (see "Connection lifecycle") and what the configured
timeout covers during SETUP, immediately below. (Outbound throttling used to be
a third: the blocking half slept outbound debt after a write, on the thread
that would issue the next read. Both halves now wait for outbound admission
before the write instead - see "Bandwidth metering".) Any proposal to remove or
reshape this surface is the owner's call - see the standing lessons in
`AGENTS.md`.

The second one is easy to miss because each half is documented by its MECHANISM
rather than by its BOUND. The async half runs DNS, connect, implicit TLS handshake,
banner and initial EHLO under one shared `AsyncDeadline`. Later STARTTLS and
AUTH run outside that deadline with per-operation budgets; OAuth token-source
resolution has no SMTP timeout. Error teardown can also add its bounded wait.
The configured timeout is therefore not a ceiling on the whole transport
checkout. The blocking half has no shared setup deadline:
address resolution gets the full value to itself (see below), `connect_timeout`
applies the full value to EACH candidate address in turn, the implicit-TLS and
`STARTTLS` handshakes each get the value per socket read or write (see "Async
setup deadline"), and the banner and EHLO reads are bounded, per reply, by
`SO_RCVTIMEO`. Every phase is bounded, but by its own budget and never by a
shared one, so the worst case is the sum of the budgets, multiplied by the
candidate-address count for connect and by the round trips for the handshake.
Neither runtime promises that "timeout" bounds the whole process of producing
an authenticated connection; only the async initial connect and greeting phase
has a shared deadline.
The difference is structural rather than an oversight: a blocking connect has no
cancellation point to hang a shared deadline on, and giving the blocking half
one would mean a non-blocking socket and a poll loop, which is not what that
half is for. A blocking caller who needs a ceiling on setup must impose it
outside the call.

The write timeout is deliberately NOT a further difference, and the async half
had to be built to keep it that way. The configured timeout bounds one write
that makes no progress, never the whole transfer: the blocking half gets that
for free because `SO_SNDTIMEO` is per `write(2)`, and the async half re-arms
its `tokio::time::timeout` around each write that accepted bytes. Armed once
around a `write_all` of the entire body, the bound is a function of message
size and link speed rather than of peer health - a 12 MB message over a
5 Mbit/s uplink needs ~19 s - so a healthy slow upload timed out inside the
body write, tagged `InFlight` / `DataBody`, and turned every accepted recipient
into an `uncertain` lane on the async half while the blocking half delivered
the message. The property to keep: a stalled peer still times out, a
slow-but-progressing upload does not. Pinned on the async side by
`a_slow_but_progressing_body_upload_outlives_the_operation_timeout` and
`a_stalled_body_upload_still_hits_the_write_timeout`, both under paused tokio
time. A `SetupDeadline` budget still shrinks across the loop, so the shared
connect deadline is unaffected.

The contract that property implies is worth stating in full, because it is a
guarantee about a WRITE and callers read it as a guarantee about a TRANSFER:
**there is no total bound on a body upload, by design.** A peer that accepts one
byte per (timeout minus epsilon) keeps a send alive indefinitely, and that is
the same rule the two pinned tests describe, not a hole in it. Bounding the
whole transfer would need a second, size-aware budget nothing configures, and
any value picked for it would fail exactly the healthy slow uploads the
re-arming loop was built to keep - a 12 MB message on a domestic uplink. The
read side differs for a reason that does not carry over: a REPLY has a size cap
(`MAX_RESPONSE_BYTES`), so "one reply" is a bounded unit and a per-reply
deadline is expressible; a message body has no such ceiling. A caller who needs
a wall-clock ceiling on a whole send imposes it above the transport, by wrapping
the send in its own timeout - which is also the only place that knows what the
message size makes reasonable.

Re-arming covers the slow LINK. It does not by itself cover a bandwidth CAP,
and the two failures look identical from inside the loop: a write held back by
the cap and a write held back by the peer both simply take long. So each
iteration of the loop has two phases. ADMISSION comes first, before the timeout
is armed: `admit_outbound_within` reserves at most one second of cap against the
outbound schedule every connection of the transport shares and waits for its
slot (see "Bandwidth metering"). The socket write then runs under the timeout
against the allowance admission installed, and never consults the schedule
again. So a wait this crate imposed at the consumer's request is never charged
to the budget that exists to judge the peer, and a peer that accepts nothing
always reaches its timeout - nothing pauses or stretches the timer once armed.
A short successful write finishes the grant with the accepted count and refunds
its unused remainder only when it is still the current tail. The next iteration
reserves again. The cap itself is untouched - a
throttled upload still takes exactly as long as the cap says, it just does not
fail. Pinned by
`a_capped_upload_outlives_a_write_timeout_shorter_than_its_throttle_debt`, under
paused time with a 200 ms timeout and a 1000 B/s cap; before admission existed,
any configured timeout at or below a second failed every capped write after the
first with `Timeout` / `DataBody` and every recipient `uncertain`.

The timeout guarantee therefore reads: a write that makes no progress times out
once ADMITTED. Admission waits for already issued reservations on the shared
schedule, which can also belong to other pools or protocols. Retuning a finite
cap preserves those reservations, so the current cap and this pool's connection
count alone do not bound that wait (see "Bandwidth metering"). Admission is
untimed under a per-operation timeout - it is the cap, not the peer.

Waiting outside the timer is right for ONE of the two budgets, and the write
loop matches on which. `PerOperation` is a bound on one write's dealings with
the peer, so admission runs entirely outside it, as above. `SetupDeadline` is an
ABSOLUTE ceiling shared by DNS, connect, TLS, banner and EHLO, and there
"outside the budget" is not a saving but the failure: waiting outside it does
not SPEND the deadline, it WAITS PAST it. Under a cap small enough to throttle a
greeting, an EHLO write waits for its slot while the remaining setup budget is
milliseconds, and the operation would report a timeout only after overrunning a
ceiling the contract says it cannot exceed. So admission is bounded by the slack
left on the deadline, and admission that cannot come inside the setup budget
fails as a setup timeout. An unoffered current tail can be refunded; already
offered grants stay charged. The write and flush that
follow re-read the slack for themselves, because `with_timeout` resolves a
`SetupDeadline` when it arms the timer; computing a remaining duration once and
reusing it across the wait and the write would hand back the same overrun in
smaller pieces. Pinned by
`a_setup_deadline_bounds_the_admission_wait_rather_than_waiting_past_it` (the
elapsed time is the finding; an unbounded wait also ends in a timeout, just
1021 ms into a 300 ms ceiling) and
`the_write_after_a_bounded_admission_gets_the_recomputed_slack`. There is no
third budget shape to consider: `teardown_budget()` builds a `PerOperation`
value and applies it directly to `stream.shutdown()`, never to a write.

Bounding the admission wait closes the overrun it would otherwise introduce. It
does not on its own prove that the public connect future returns by the setup
deadline:
setup error handling goes on to call `abort()`, which carries its own teardown
timeout (see "Connection lifecycle"), so the wall-clock bound a caller
experiences is the setup deadline plus at most that teardown cap.

## Protocol core and I/O adapters

`client/core.rs` is the sans-I/O protocol core for every send path. It owns
command sequencing, reply-group accounting (the PIPELINING window and the LMTP
final-status drain), `SmtpCommandPhase` decoration, `SendProgress` transitions,
and the RSET-and-keep versus abort decision. It takes typed outcomes in and
yields byte writes plus typed ops out. It never touches a socket, a timeout or
a clock, so it can be driven to completion with no I/O at all.

The shape is a step function, not a straight-line routine, because the two
adapters cannot share control flow - one blocks, one awaits. A machine is asked
for its next `Op`; the adapter performs it and feeds back an `OpOutcome`
(`Done`, `Token(Secret)`, `Reply(Response)`, or `Failed(Error)`). A negative
reply is a `Reply`, never a `Failed`: telling a routine rejection apart from a transport
failure is exactly the decision the core exists to make in one place.

The shared effect interface admits outcomes for all operation kinds. A read
receiving a non-reply success, token resolution receiving a non-token success,
or a session authentication stage holding the wrong exchange returns the existing
`ErrorKind::Internal` instead of panicking. These invariant failures abort the
connection, including failures before AUTH, and retain command phase and
transaction transmission evidence through the normal error projection. An
enterprise send queue needs that evidence to decide whether retry is safe.

One `Transaction<P>` covers the send surface. Its `Dialect` selects one SMTP
final reply or one LMTP final status per accepted recipient. `RecipientPolicy`
is explicit: `FailFast` stops direct SMTP before the body on any recipient
refusal; `PerRecipient` retains refusals while sending to accepted recipients.
`DirectSmtp` and `DirectLmtp` are projection aliases, not separate machines.
`BatchTransaction` uses the same sequencer for either dialect. Each transaction
records one `TransactionProgress`, including original recipient indexes and a
single phase (`Envelope`, `DataIssued`, `BodyStarted`, `BodySent`, or
`FinalReply`). Direct `Response`, ordered LMTP responses, and account-aware
`SendProgress` are terminal projections of that record. Direct progress exists
with `account-error` disabled too.

The op vocabulary covers commands, body framing and session effects:
`Write(String)`, `WriteSecret(Secret)`, `WriteBody`, `WriteBdat`,
`OpenReplyGroup`, `ReadGrouped`, `ReadSingle`, `CloseReplyGroup`,
`CloseLmtpDrain { restore_ok }`, `UpgradeTls(TlsParameters)`,
`ResolveToken(Credentials)`, and `Abort`. `WriteSecret` and token outcomes redact
Debug output and wipe storage on drop. `OpenReplyGroup` / `CloseReplyGroup`
hold the stream `Broken` until the complete expected group drains.
`ReadGrouped` defers surplus checking to the close; `ReadSingle` checks itself.
Command write failures reach the same stage as the command's reply, so both
carry the same boundary's phase.

`session.rs` owns greeting, EHLO/LHLO, STARTTLS and the AUTH ladder. Its entry
points share one hello continuation and one authentication walk. Greeting and
STARTTLS require 220, hello requires 2yz completion, and authentication requires
235 to finish. SCRAM accepts either a 334 server-final followed by an empty
client acknowledgement and 235, or the server-final folded into 235, including
an enhanced status prefix. PLAIN, LOGIN, XOAUTH2 and OAUTHBEARER run on
`bifrost_sasl::SaslClient`, which owns the challenge budget: PLAIN answers no
challenge, LOGIN answers exactly two (username, then password, by position),
and an OAuth mechanism answers one error challenge; any further `334`, or a
`235` before the mechanism completed, is a parse-class refusal that aborts the
connection. STARTTLS upgrades precede a fresh hello;
post-TLS and post-AUTH capabilities replace the old set. Token resolution is
one adapter effect per OAuth exchange: async calls `current()`, blocking uses
`snapshot()` without creating or polling an async token future. The transport's
dial policy still determines whether to request STARTTLS and configure AUTH.

`reply.rs` owns `ReplyAssembler`: bounded byte-chunk assembly, per-line and
total caps, UTF-8 validation, parser incomplete handling, and the shared
surplus-byte refusal, including the STARTTLS plaintext boundary. EOF, including
mid-line EOF, is `Network` with an `UnexpectedEof` source. The transaction
stamps `Unsent` before body admission or `InFlight` after it. An enterprise send
queue must retry a disconnected, unsent message without classifying a normal
connection loss as a provider contract violation.

The `Epilogue` type holds the two terminal sequences every machine shares. It
carries the value the machine will finish with, so a boundary cannot start an
abort or a reset and forget to return. RSET-and-keep is expressed there as ops -
write `RSET`, read the reply, keep the connection on a 2yz completion
acknowledgement and abort otherwise - so no adapter knows the rule.

Each adapter has a `drive` loop and an effect match. What stays adapter-side
is I/O: dialing, buffer fills, byte writes, TLS handshakes, token-source calls,
read/write deadlines, bandwidth admission and shutdown. Async session startup
uses the same shared setup deadline across the greeting and hello effects.
The local validation that runs
before `MAIL FROM` lives in the sans-I/O sibling `client/envelope.rs`.
`EnvelopeBuilder` takes `ServerInfo` and the connection's encryption state;
both adapters delegate `build_envelope` and `build_batch` to it. Direct and
batch sends share one MAIL option builder and one RCPT option builder.
Explicit SIZE requires server advertisement, and the completed MAIL option
list rejects duplicate keywords case-insensitively. `Other` and `OtherRaw` are
reserved for custom keywords without typed `MailParameter` variants: raw
spellings of typed keywords such as SIZE are rejected, so they cannot bypass
capability or value validation. Unknown custom parameters remain available.
RCPT options retain their last-value-wins normalization and recipient-specific
overrides.

## Connection lifecycle

```
greeting -> EHLO/LHLO -> [STARTTLS -> EHLO/LHLO] -> [AUTH -> EHLO/LHLO] -> ready
```

EHLO is re-sent after STARTTLS and AUTH because capabilities can change.

`ServerInfo` ignores empty EHLO/LHLO extension lines, while whitespace-only
lines have no keyword and produce a parse error. Malformed peer input stays on
the typed error path.

STARTTLS upgrades are fail-closed at the buffered-reader boundary. If the peer
coalesces any plaintext bytes after its positive STARTTLS reply, the connection
is marked broken and the upgrade is refused. Pre-TLS bytes can never become a
post-TLS reply.

- `abort()` closes the connection and marks it `Broken` on both sides, so an
  aborted connection can never pass `state().verify()` again. There is no live
  QUIT path; the Quit command builder is gone. If a graceful shutdown is wanted
  later it will need to be re-added with a real call site.
- The async `abort()` is bounded by the operation timeout or the teardown cap,
  whichever is SMALLER. `poll_shutdown` on a TLS stream sends `close_notify`
  and waits for the peer's, so an unresponsive peer would otherwise hang the
  caller *after* the timeout that already fired. The cap
  (`TEARDOWN_TIMEOUT`, 5 s, in `async_connection.rs`) is independent of the
  configured timeout because the per-operation budget does not bind at all when
  a transport is built with `timeout(None)`: that setting means "do not limit
  my protocol operations", not "hang forever tearing down a connection I have
  finished with", and past `abort()` there is nothing left to accomplish but a
  courtesy `close_notify`. This mattered most at `Pool::shutdown`, which runs
  its closes concurrently but WAITS for them, so one wedged TLS peer hung pool
  shutdown. The blocking `abort()` needs no such bound and deliberately has
  none: `Shutdown::Both` on a blocking socket is a syscall that returns
  immediately, so that half already satisfies "teardown returns in bounded
  time". The rule holds on every teardown path; the two halves reach it by
  different means. Pinned by
  `abort_is_bounded_when_tls_style_shutdown_never_completes` (configured
  timeout) and
  `abort_is_bounded_by_the_teardown_cap_when_no_operation_timeout_is_set`
  (`timeout(None)`, which the first cannot reach).

## Internal errors and local assertions

TLS stream extraction failures in both network adapters return `Internal`
with the stream left `Broken`. A SCRAM selection paired with a non-SCRAM
mechanism likewise returns `Internal` through the session's normal abort path.
These failures must preserve ordinary error handling for an enterprise send
queue rather than unwind through its send operation. Async bandwidth admission
uses a private exhaustive inbound/outbound selector; extending the shared
bandwidth vocabulary cannot introduce a panic fallback in this adapter.

The retained ownership assertions enforce local invariants. Session server
info and transaction progress are installed at construction and taken only at
terminal projection; both private drivers return immediately on `Step::Finish`.
Batch group-failure projection passes `Ok(())`, which cannot enter its
error-result branch. Pooled wrappers install a connection (and an async permit)
that only `Drop` takes; safe callers cannot dereference the wrapper during or
after that drop. The async pool installs `handle` and `worker_completion` once
in the constructor when the worker is enabled. The pool's shutdown completion is initialized lazily by
`get_or_init()` during shutdown, so concurrent and later callers share the same
teardown work. Ready bandwidth admission installs a grant, and polling the
peer only borrows the inner stream, leaving that grant available for completion.
No server input or cancelled caller future can violate these ownership
assertions. Shutdown separately asserts that its owned teardown task signals
completion; if that task exits without signaling, awaiting completion panics.
Pool checkout reports poisoned mutex state as an `Internal` error. Worker,
recycle, shutdown and destructor paths recover the guarded ownership with
`PoisonError::into_inner`; poison is not cleared, so recovered idle connections
cannot be checked out. Cleanup must remain possible during unwinding without a
second panic. Test-only parking assertions require an open pool with capacity.
The blocking builder remains infallible: OS worker creation failure is retained
by the pool and returned as a connection error on every checkout before dialing.
An enterprise send queue needs a typed local resource failure instead of a
process panic.

## Connection state and cancel-safety

Sync and async streams carry an explicit state: `Ok` / `Broken` / `Closed`. Writes, flushes, reads, and TLS upgrades set `Broken` before the await; only success restores `Ok`. Dropped futures leave state `Broken`; the pool drops the connection.

LMTP final delivery-status loop holds `Broken` until every accepted recipient's status has been read. A surplus final status that already reached the read buffer is a protocol violation: the connection is marked `Broken` and the drain fails (the batch driver keeps the per-recipient outcomes it already has, since the surplus changes only the stream's reusability).

The direct and batch LMTP paths differ in one respect, preserved rather than
unified when the drain moved into the core (`Op::CloseLmtpDrain { restore_ok }`):
a direct LMTP send restores the stream to `Ok` after a clean drain without a
421, while an LMTP batch leaves it `Broken`. A drain containing a 421 leaves
the direct connection `Broken` too. Neither connection is reusable in practice,
since every drain retires it, so this is a difference in bookkeeping rather than in
behaviour - but it is a real difference and it is now visible in one place.

Surplus bytes that have not yet crossed into the `BufReader` - still in the socket or a TLS record - cannot be observed without a read that would block against a well-behaved peer, and native-tls exposes no nonblocking peek that would make such a probe honest. So cleanliness is never positively established: every LMTP final-status drain sets a retirement flag (`should_retire()`), and the pool discards those connections at recycle instead of parking them. The cost is one reconnect per LMTP transaction; LMTP is local delivery, so that is cheaper than recycling a possibly desynchronized stream. SMTP connections are unaffected and still pool normally.

`test_connected()` (pooled NOOP probe) aborts on failure so a stale connection cannot be recycled.

An ordinary SMTP reply must also end with an empty read buffer. Surplus bytes
prove that the peer spoke out of turn, so the driver marks the connection broken
and reports a parse error before another command can consume the stale reply.
PIPELINING and LMTP final-status groups defer this check until their exact
expected reply count has drained.

With the `account-error` feature, `Address::try_from(&bifrost_types::Address)`
checks the bare addr-spec through the same local-part and domain validation as
`Address::new`. Display names are excluded from envelope conversion.

Envelope commands are constructed, and therefore validated, before `MAIL FROM`
is written. Every send path - sync and async, DATA and BDAT, SMTP and LMTP,
pipelined and not, single-envelope and batch - builds the whole `Mail` plus
`Rcpt` set up front (`build_transaction_commands` / `build_recipient_commands`)
and only then opens the transaction. The recipient builder also requires one
options vector per address; a mismatch is an internal construction error
rather than a silently shortened RCPT list. This is a connection-state
invariant, not a stylistic one: a construction failure raised from inside the
transaction would unwind through the caller's `?` without running the abort
that a wire-level failure runs, so the connection would stay `Ok`, return to
the pool, and hand the next send a half-open transaction. Building first keeps
every address rejection on the clean side of `MAIL FROM`, where no abort is
needed and the connection stays reusable.

`Transaction` also enforces "no `DATA` before `MAIL FROM`" itself rather than
inheriting it from its callers. `Envelope::new` rejects an empty recipient list,
so an empty envelope is unreachable from the public API, but the pipelined
branch used to call `start_window(0)`, find nothing to write, fall through to
`after_envelope` and open the transaction with a bare `DATA`. The machine is the
authority on its own command sequence and cannot see the type that forbids the
input, so the empty case is routed through the sequential branch, which writes
`MAIL FROM` before it looks at the list. Pinned by
`a_pipelined_send_with_no_recipients_still_opens_with_mail_from`. Empty batch
and LMTP inputs now use that same MAIL-first branch before resetting an
empty transaction.

## Async setup deadline

`AsyncDeadline` is a single shared deadline across DNS, connect, TLS handshake, banner read, and initial EHLO. The deadline lives only until `connect_impl` returns; established connections use the per-operation timeout. `starttls(...)` on an established connection uses the per-operation timeout because it is an explicit command, not setup.

**The TLS handshake is never unbounded, whatever the timeout.** `timeout(None)`
means "do not time out my SMTP conversation", not "let a TLS negotiation hang",
and `AsyncDeadline::new(None)` yields no slack at all, so on its own it would
leave a `ClientHello` that is never answered parked forever. The handshake
therefore goes through `AsyncNetworkStream::upgrade_tls_until`, which replaces
an unbounded deadline with a fresh `DEFAULT_TLS_HANDSHAKE_TIMEOUT` (30 s,
`client/mod.rs`) for the handshake alone. That covers explicit `STARTTLS` (via
`upgrade_tls`) and the implicit-TLS leg of `connect_until`, whose setup deadline
is the `None` shape under `timeout(None)`. The default is 30 s because it sits
an order of magnitude above a healthy handshake (two round trips plus platform
certificate verification, which can include revocation fetches) yet far below
RFC 5321's five-minute peer command timeouts, so it does not fail a working
server on a poor link. It substitutes for a MISSING timeout only: a configured
one is used as given, shorter or longer, so the default is neither a floor nor a
second timer. That is also what keeps it clear of a timer-inside-timer defect -
a bounded deadline already IS the timer for the operation and is used as it
stands, and the substitution happens only where there is no timer to nest in;
`starttls` is not wrapped in any per-operation timer of its own. Everything
else under `timeout(None)` stays unbounded except teardown (see "Connection
lifecycle"). Pinned under paused time against `StalledPeer` by
`an_unconfigured_tls_handshake_is_bounded_by_the_default`,
`a_configured_tls_handshake_timeout_is_not_replaced_by_the_default` and
`an_unbounded_setup_deadline_still_bounds_the_handshake`.

The blocking half applies the same rule to explicit `STARTTLS` only
(`SmtpConnection::upgrade_tls_bounded`). Its handshake blocks on the socket, so
its only bound is `SO_RCVTIMEO` / `SO_SNDTIMEO`: with a configured timeout those
are already armed and govern it, and with none the driver arms the default for
the handshake and restores the unbounded setting afterwards. This is a per-read
bound, like every blocking timeout, not a shared deadline. Pinned without a
socket by reading back the armed timeouts on the transcript stream
(`an_unconfigured_starttls_handshake_is_bounded_and_then_unbounded_again`,
`a_configured_starttls_timeout_is_left_alone`); a real stalled handshake needs a
socket and stays out of scope.

The blocking implicit-TLS handshake at connect time draws from the same budget
(`NetworkStream::upgrade_tls_bounded`). It runs before `set_timeout` has armed
the socket, so the dialer arms the configured timeout itself for the handshake
(the default when none is configured, restored to unbounded afterwards). Pinned
without a socket by `the_implicit_tls_handshake_draws_the_configured_timeout`
and `an_unconfigured_implicit_tls_handshake_is_bounded_and_then_unbounded_again`.

Blocking address resolution is bounded by `resolve_within`. `ToSocketAddrs` has
no timeout parameter and `getaddrinfo` cannot be cancelled, so with a configured
timeout the resolution runs on a helper thread and the caller waits for it at
most that long; the budget is the configured timeout for resolution alone, and
each connect attempt then draws its own. **A resolution that exceeds the bound
leaves its helper thread running** until the OS resolver returns or gives up,
after which the thread drops its result and exits, so a stalled resolver leaks
one thread per timed-out attempt for the length of the stall. With
`timeout(None)` resolution runs inline, unbounded. This is why the blocking
connect path requires the address value to be `Send + 'static`. Pinned by
`a_stalled_resolution_times_out_at_the_bound`, which parks the resolver on a
channel the test never releases before the wait ends.

A blocking handshake that runs into the socket timeout is classified `Timeout`
(`error::tls_handshake`), matching the async half's "TLS handshake timed out";
any other handshake failure stays `Tls`. This applies to STARTTLS and implicit
TLS alike. On a blocking socket `HandshakeError::WouldBlock` can only mean the
armed timeout expired. The `WouldBlock` arm itself cannot be built without a
live handshake, so only the source-chain timeout detection is pinned
(`a_tls_failure_caused_by_an_io_timeout_is_found_in_the_source_chain`).

It measures on `tokio::time::Instant`, not `std`'s. Every timeout the deadline
hands out is a `tokio::time::timeout`, and a deadline that reads a different
clock from the timers it arms is only accidentally right - it disagrees
wherever tokio's clock is not the system one, which under `start_paused` test
time meant the deadline never expired at all.

## Per-reply read deadline

The configured timeout bounds one REPLY, not each line of it. Both drivers
enforce that, by different mechanics for the same reason: the natural unit on
each side is smaller than a reply, so a multi-line reply armed naively gets a
fresh full timeout per line and a peer trickling one line per period stretches
one reply to `MAX_RESPONSE_BYTES / line` times the configured timeout, bounded
only by the size caps.

- The async reader keeps a remaining peer-time duration for the whole reply.
  Before each buffer fill it admits an inbound reservation outside that timer,
  then spends only the time awaiting peer I/O. Individual reads never reset the
  remaining duration. A setup deadline remains absolute and includes admission.
- The blocking stream keeps the same cumulative remaining peer time, re-arming
  `SO_RCVTIMEO` before each syscall after admission. It spends only syscall time,
  so multiple `fill_buf` calls cannot reset the reply budget. The
  configured socket timeout is restored when the reply ends. Zero remaining time
  fails rather than setting a zero socket timeout, which would block forever.

`a_trickled_multi_line_reply_cannot_outrun_the_operation_timeout` pins whole-reply
protection. `a_trickled_reply_across_reads_spends_only_cumulative_peer_time`
pins the same protection across partial-line reads after a shared admission wait
longer than the peer timeout. `a_capped_reply_read_is_not_charged_its_own_throttle_debt`
and `a_setup_deadline_is_not_postponed_by_inbound_throttle_debt` retain their
historical names while exercising reservation waits rather than post-read debt.
`multi_line_reply_reads_share_one_deadline` observes the blocking timeout re-arms.

## PIPELINING

When the server advertises PIPELINING, `MAIL FROM` and `RCPT TO` commands use
bounded recipient windows with every reply drained before the next window.
Both dialects and both DATA and BDAT use this envelope sequence. Body admission
comes only after the envelope completes; body bytes are never pipelined.
`FailFast` resets before the body on a recipient refusal; `PerRecipient`
continues for accepted recipients and resets only when none were accepted.
Original recipient indexes survive every window.

The window bracket is `Op::OpenReplyGroup` / `Op::CloseReplyGroup`, so both
drivers hold the stream `Broken` from a successful window write until the
complete reply group has drained, by construction rather than by mirroring.

Every transaction error exit carries `PhasedError`, which has no phase-less
constructor. The shared `TransactionStage` handles the write and reply for
each boundary; terminal projections stamp the phase when converting to the
wire `Error`. This covers direct SMTP, direct LMTP and batch errors alike,
with or without pipelining or BDAT.

Batch error translation also has one phase authority. `SmtpErrorContext` cannot
carry a command phase, so a batch site converting a low-level `Error` must use
the phase already attached to that value and cannot supply a second, disagreeing
phase. Per-recipient negative replies, which have a `Response` rather than an
`Error`, pass one explicit phase directly to the response classifier. This shape
is identical in the blocking and async drivers.

No SMTP-originated `AccountError` carries an `ErrorScope`, and that is
structural rather than conventional: `SmtpErrorContext` has no scope field and
one constructor, so no call site can attach one. `ErrorScope::Account` would
claim an account-wide fault for what is a single transaction on a single
connection, and the id-bearing variants take typed account-surface ids, none of
which represents an SMTP envelope address. Batch lanes correlate through their
`BatchItemId` and support-only envelope-recipient text instead. Every context
construction in the crate goes through `SmtpErrorContext::send`, and since the
send paths run on one core there is now exactly one set of those call sites for
both halves; both still pin the scope-less shape on the non-pipelined recipient
path with a paired transcript test.

That explicit argument is the part of the arrangement that is behaviour rather
than a type property, and it is pinned by a bare-550 recipient rejection: with
no enhanced status code the reply is classified by status alone, where the
`RcptTo` phase is what separates `NotFound(Mailbox)` from
`Authorization(PermissionDenied)`. Enhanced-code rejections such as `5.1.1`
classify identically for every phase, so a test built on one cannot pin this.

This covers normal negative replies as well as transport failures, which is the
case a per-call-site decoration missed: a server rejecting `MAIL FROM` is
`MailFrom`, a rejected recipient is `RcptTo`, a rejected `DATA` is
`DataCommand`, and a body-upload failure is `DataBody`. Those phases feed
`classify_response`, including the recipient-lane split, so an undecorated
rejection would make the PIPELINING path classify a plain relay rejection
differently from the non-pipelined path for the same wire exchange. Both halves
are pinned by transcript tests over all four boundaries.

The phase names the boundary the CALLER's outcome came from, not the last op
the machine happened to run. After a pipelined `MAIL FROM` is rejected,
`DirectSmtp` still has to drain the window's RCPT replies and close the group
(`MailRejectedDrain`, then `MailRejectedClosing`), and a transport failure in
either of those is bookkeeping for the STREAM, not for the transaction: the
send's outcome was already decided by the rejection. Both stages therefore
finish with the carried `MAIL FROM` rejection under `SmtpCommandPhase::MailFrom`
and abort the stream. Reporting the drain's own error instead handed the caller
a retryable `RcptTo` network error for an envelope the server had permanently
refused, with the reply text that said why thrown away (2026-09-07).
`MailRejectedClosing` is the identical defect one stage later and was fixed with
it. A 421 among the drained RCPT replies also aborts immediately while
preserving the MAIL refusal, rather than waiting for replies after the peer
announces closure. Pinned by
`a_failed_window_drain_still_reports_the_mail_from_rejection` and
`a_closing_reply_while_draining_rejected_mail_keeps_the_mail_refusal`.

A negative reply to the DATA end-of-data terminator follows the same rule from
the other end. The peer read the whole message and refused it, so the
transaction is complete (RFC 5321 4.1.1.4), there is nothing to reset, and the
connection stays reusable: `DirectSmtp` finishes with the status error and no
abort, and the batch projection (whose public send APIs use DATA) records the reply through
`set_data_response` after recording body completion at the write boundary and finishes `Ok(progress)`, so
`resolve` fans it out as `DataFinal` `failed` lanes carrying `Acknowledged`.
Routing it through the transport-failure arm instead put every accepted
recipient in the `uncertain` lane - asking the caller to reconcile a delivery
the server had explicitly refused - and aborted a healthy connection. An I/O failure, parse failure or intermediate reply at that boundary aborts. Pinned in both halves by
`a_rejected_data_final_reply_fails_the_recipients_and_keeps_the_connection` and
`a_rejected_data_final_reply_keeps_the_direct_connection_reusable`.

The rule is scoped to DATA, and deliberately. `DirectSmtp`'s `FinalReply` stage
also serves BDAT - `BDAT n LAST` has no 354 step, so the machine goes
`after_envelope` -> `BodyWritten` -> `FinalReply` - but RFC 3030 makes no
equivalent transaction-complete promise for a refused chunk, and its own
failure example sends `RSET` after the negative reply. The crate's conservative
rule is to keep a connection only where the protocol guarantees its state, so a
negative reply to `BDAT ... LAST` goes through `Epilogue::reset`: RSET-and-keep,
aborting unless the peer acknowledges the reset. Finishing it the DATA way
parked the connection `Ok` inside a transaction a strict server still considers
open, and the next checkout's `MAIL FROM` drew `503 bad sequence`.
`a_rejected_bdat_last_resets_the_transaction_before_the_connection_is_reused`
pins it in both halves.

Outside the LMTP per-recipient final drain, one reply overrides those
transaction boundary decisions: `421`
means "closing transmission channel" (RFC 5321 4.2.3), and `core::closing_channel`
is asked ahead of every reset-or-keep decision a negative reply reaches. The
reset-or-keep rules reason about the transaction; a 421 is about the
connection, so whatever the boundary - `MAIL FROM`, `RCPT TO`, `DATA`, the
end-of-data terminator, `BDAT ... LAST` - it goes through `Epilogue::abort`
carrying the same value the arm would otherwise have produced. The direct projections carry the status error; a pipelined `MAIL FROM`
answered 421 aborts without draining the window, since the RCPT replies may
never come. The batch projection still records the answer first, so its lanes
resolve from it:
at `RCPT TO` the answered recipient is rejected with the 421 and every other
unanswered or accepted recipient is `Unsent`, because `DATA` was never issued
(the envelope phase); at `DATA` the accepted recipients fail with it; at
the end-of-data terminator the lanes resolve `DataFinal` `failed` /
`Acknowledged` exactly as any other rejection does. Only the connection is
retired. In a pipelined RCPT window the direct path keeps only the FIRST
refusal as the reported error, but asks `closing_channel` of EVERY reply in the
window (`TransactionStage::RcptWindowReply` carries a `closing` flag beside the
retained failure), so a 550 followed by a 421 reports the 550 and aborts with no
RSET, as `BatchTransaction` does; pinned by
`a_421_later_in_an_rcpt_window_aborts_and_reports_the_first_refusal`. Parking a connection the peer just said it is closing hands the next
checkout a dead socket - and on the batch path, which is the account-level
`send_smtp_batch`, that dead connection is pooled, so under
`test_on_checkout(false)` the NEXT send writes `MAIL FROM` into it and fails
`Unsent`. The check used to live only on the end-of-data arms, and a 421 to
`MAIL FROM` parked the dying connection (2026-09-07). Pinned by
`a_421_at_any_envelope_boundary_aborts_instead_of_parking` in the core tests
and, at the end-of-data boundary,
`a_421_data_final_reply_aborts_instead_of_parking_a_dying_connection` and
`a_421_data_final_reply_aborts_the_batch_connection_too`, both in both halves.

The direct path tags a rejected DATA end-of-data reply `DataFinal`, matching
what `BatchTransaction` already produced; a transport FAILURE at the same boundary
keeps `body_phase()` (`DataBody` / `BdatBody`), because that is an upload
failure rather than an answer. BDAT has no distinct final phase, so a rejected
`BDAT ... LAST` stays `BdatBody`.

`DirectLmtp` reads the same `body_phase()` at its own `BodyWritten` boundary,
so an LMTP `BDAT` chunk-write failure reports `BdatBody`. It hardcoded
`DataBody`, which made the direct LMTP path the only send path that mislabelled
its own body framing (2026-09-07). Nothing in `classify_response` splits on the
two today, so this changes the reported phase and the diagnostics that carry it,
not the recovery classification. Pinned by
`a_failed_direct_lmtp_bdat_chunk_carries_the_bdat_phase`.

A rejected `MAIL FROM` deliberately sends no `RSET`: it opened no transaction.
Direct reporting keeps the connection reusable; batch reporting preserves its
conservative abort policy while returning the acknowledged batch-level error.
After an accepted `MAIL FROM`, recipient or DATA refusal uses
`Epilogue::reset`, keeping the connection only after a completion reply.
Direct LMTP inherits those direct SMTP rules: a rejected MAIL keeps the stream,
and a rejected DATA resets it. Both previously aborted unnecessarily; the
regression is pinned by
`a_rejected_lmtp_envelope_follows_the_smtp_reset_or_keep_rules`.

This is not a PIPELINING-only rule. The direct sends (`send_with_options` and
`send_bdat_with_options`) take the same shape when the server advertises no
PIPELINING: `MAIL FROM`, `RCPT TO` and `DATA` replies reach the core as
`OpOutcome::Reply`, so a routine 4xx/5xx is classified by the core instead of
arriving as a transport-shaped failure that would abort. Only genuine I/O or
parse failures abort. Both adapters drive the `DirectSmtp` projection of `Transaction` for the
`MAIL FROM` + `RCPT TO` sequence, pipelined or not, and are pinned by
`unpipelined_server_rejections_keep_the_connection_reusable`, which asserts the
exact RSET traffic and that the connection is still unbroken afterwards. A
server rejection therefore costs no reconnect on either the pipelined or the
non-pipelined path.

## DSN and SendOptions

`SendOptions` is the per-message API for advanced ESMTP parameters:

- MAIL FROM params: `REQUIRETLS`, `FUTURERELEASE` (HOLDFOR / HOLDUNTIL), `DELIVERBY`, `MT-PRIORITY` (RFC 6710), DSN `RET`, DSN `ENVID`.
- Uniform RCPT TO params: DSN `NOTIFY`.
- Recipient-specific RCPT TO params: DSN `NOTIFY` overrides per recipient, `ORCPT` (default addr-type `rfc822`).

Server-advertised `DSN` is verified before DSN parameters are emitted. Recipient-specific params override matching global keywords (RFC 3461 §4.1 no-duplicate-keyword). Automatic DATA `SIZE` declarations reuse a message's existing final CRLF or account for the conditional CRLF the writer adds, and exclude the terminator line itself and transparency dots as RFC 1870 requires; BDAT uses its raw payload length.

Batch sends validate every recipient's RCPT parameters before `MAIL FROM`, in both sync and async paths. A local parameter error is an unsent `RcptTo` failure, never a silent downgrade to an unparameterized RCPT command.

Batch `MAIL FROM` construction errors are likewise stamped `Unsent` / `MailFrom`; no command has crossed the socket at that point.

Public entry points: `send_raw_with_options(...)` on SMTP and LMTP, sync and async.

## xtext encoding

RFC 3461 §4.1: any byte outside `%x21-%x7E`, plus `+`, plus `=`, is encoded as `+HH` with uppercase two-digit hex. UTF-8 multi-byte sequences are encoded byte-by-byte. Used for ENVID, ORCPT address part, and unknown ESMTP parameter values. Centralized in `util.rs`.

Unknown `OtherRaw` custom values and typed `HOLDUNTIL` values bypass xtext and
are validated as raw ESMTP values: printable ASCII without spaces, with `=`
excluded by RFC 5321. EHLO domain values use the same raw-value check.

## VRFY / EXPN

`verify(addr, ...)` and `expand(list, ...)` on SMTP transports. Negative replies return as `Response`, not `Err`, because they are normal outcomes for these privacy-sensitive commands. Inputs are sanitized for CRLF and control characters before any wire write.

`MAIL FROM` and `RCPT TO` apply the same single-line control-character check
when their command values are constructed. This is a wire-boundary defense for
addresses created through the public unchecked constructor as well as parsed
addresses; hostile values fail before either command is written.

## BDAT

Opt-in via explicit `send_raw_bdat` / `send_raw_bdat_with_options`. Defaults still use DATA so dot-stuffing and BINARYMIME behavior are not changed silently. BDAT requires `CHUNKING` to be advertised.

## SIZE enforcement

If the server advertises SIZE, message size is checked client-side against any finite limit before MAIL FROM. The inferred `SIZE=<bytes>` on MAIL FROM is the computed message size, not the server's limit; a caller-supplied SIZE is retained and validated instead. If EHLO repeats SIZE with multiple finite limits, the client enforces the smallest one, independent of hash iteration order. An unbounded `SIZE 0` entry does not relax a finite limit from another repeated entry.

`SIZE 0` means "no declared maximum" per RFC 1870 section 3 and is stored as no limit. An unparseable limit is a hard `Parse` error on the EHLO reply rather than a silently dropped ceiling.

For DATA the declared size is `message.len()` when the buffer already ends in CRLF and `message.len() + 2` otherwise. The writer emits only `.\r\n` after an existing final CRLF; for an empty buffer, a missing final CRLF, or a buffer ending in a lone CR or LF, it first emits the required CRLF and then `.\r\n`. Thus the leading CRLF in the RFC 5321 terminator is the message's final CRLF, not an appended empty line. BDAT declares the raw chunk length because it has no terminator or transparency layer.

The chunked writer (`message_iter`, and the LMTP equivalent) makes the same decision without buffering the message: it tracks the last two bytes written across chunk boundaries, so a CRLF split across two iterator items is still recognized as the message's final CRLF.

A consequence: `Message::formatted()` is byte-identical to the delivered data section for any message ending in CRLF - delivery no longer appends an empty line the sender did not write. `body_raw()` still appends an unconditional CRLF before DKIM canonicalization, which is harmless in both directions because RFC 6376 simple and relaxed body canonicalization each strip trailing empty lines; signing and delivery still agree.

## Auth

`Credentials` aliases `bifrost_net::Credentials`: `Password { username, password: bifrost_sasl::Secret }` and `OAuth2 { identity, token_source: Arc<dyn TokenSource> }` (bifrost-net's trait). The OAuth token is read live from the shared source at each connect, so a token rotated on the source is presented on reconnect without rebuilding the transport. `Credentials` is `Clone` only - no `PartialEq`/`Eq`/serde derives (a live token source is neither comparable nor serializable; rotation material is the consumer's to persist). The `AUTH` command struct (`Auth`) likewise dropped those derives. The async transport reads the token via `oauth2_token().await`, so token sources may lock, join an in-flight refresh, or perform network refresh work without a polling shortcut; the blocking transport reads it via `oauth2_token_blocking()`, using only the synchronous, non-refreshing `TokenSource::snapshot()`. Static sources and fresh refresher caches resolve without a runtime. Cache-lock contention returns a typed transient refresh error so callers can retry; refresh-required snapshots reject with an instruction to use the async SMTP transport. The safe trait default also requires async resolution, so custom sources must implement snapshot support to serve blocking SMTP. No async token future is created or polled, and an empty or stale refresher is left unchanged. With `account-error`, token-source errors retain bifrost-net's typed account-error projection, including terminal auth loss versus transient refresh failure. Without it, the original net error remains in the low-level connection error's source chain. Cache contention likewise remains a retryable connection failure without the account feature.

Mechanisms (`Mechanism`, `#[non_exhaustive]`): PLAIN, LOGIN, XOAUTH2, OAUTHBEARER, SCRAM-SHA-1, SCRAM-SHA-256, SCRAM-SHA-1-PLUS, SCRAM-SHA-256-PLUS. SCRAM computation is consumed from `bifrost-sasl` (the `-PLUS` suffix and token spelling have a single authority there); SMTP owns only the wire sequencing. `ScramExchange` keeps its client-first and client-final lines in `Secret` values through the continuation write. The client-first is base64-framed once with a zeroizing encoder; the shared `SaslClient` returns raw replies, which this adapter base64-frames once.

Password selection (`password_mechanism` in `authentication.rs`) adapts SASL-owned `negotiate` to choose one mechanism from the advertised and allowed sets in the fixed order `SCRAM-SHA-256-PLUS > SCRAM-SHA-1-PLUS > SCRAM-SHA-256 > SCRAM-SHA-1 > PLAIN > LOGIN`. A 535 is final and never walks a retry ladder, because descending from a bound mechanism to an unbound mechanism or PLAIN would be a silent security downgrade. RFC 5802 Section 6 downgrade protection drops the unbound `SCRAM-SHA-N` candidate when `SCRAM-SHA-N-PLUS` is advertised. A PLUS candidate is skipped only when no TLS peer certificate exists. If a certificate is present but its channel binding cannot be computed, the typed parse error propagates and authentication cannot silently fall through to PLAIN. The SASL-owned `binding_from_peer_certificate` resolver computes opaque `ChannelBindingData` at most once per authentication. Selection returns a `PasswordSelection` containing the actual `ScramRung`; the session machine passes its GS2 input directly into the exchange with no eligibility re-selection or fake empty binding. SCRAM runs as a no-IR `334` challenge exchange driven by `ScramExchange`, the SMTP base64 framing adapter over the shared `SaslClient`; `Mechanism::password_client` refuses SCRAM so a SCRAM exchange cannot be built without its binding-aware constructor. A server-final folded onto `235` may follow a leading enhanced status code, which the decoder skips before base64 decoding.

OAuth credentials never use SCRAM: `oauth_mechanism` picks the first allowed,
advertised rung in shared OAUTHBEARER-before-XOAUTH2 order. The session requests
one token-resolution effect from its adapter, then constructs `SaslClient` with
the identity and required resolved token. Its pending-token state is distinct
from an active client; there is no optional token in payload encoding.

PLAIN, LOGIN, XOAUTH2 and OAUTHBEARER use one retained `SaslClient` per attempt.
`Auth` only frames raw initial responses and challenge replies as base64. The
shared client owns LOGIN's username/password position, OAuth's mechanism-specific
error acknowledgement, and the validity of terminal success. SMTP validates
`235` through `finish()` before refreshing EHLO/LHLO. An early LOGIN success,
success after an OAuth error answer, or an extra mechanism challenge is a parse
fault and retires the connection. Enterprise users expect a completed, verified
exchange rather than a server's unsupported claim of success.

OAuth SOH, NUL, CR and LF in either identity or token are refused locally before
AUTH is written. A credential-preparation or token-resolution failure leaves the
connection usable, so the caller can repair credentials or retry without an
unnecessary disconnect. PLAIN's NUL validation also runs before AUTH.

AUTH payloads and SCRAM client lines remain `bifrost_sasl::Secret` values through
the command boundary. Initial and continuation base64 framing returns a
`Zeroizing<String>`; SCRAM client-final is base64-framed once by its SMTP adapter.
The session serializes each AUTH command into a zeroizing, redacted
`WriteSecret` op, which is wiped after the adapter's write attempt or when a
cancelled future drops it. Generic command buffers are also zeroizing and wiped
before reuse and after each write attempt. The wipe covers the
buffer the command was serialized into; it cannot cover an allocation the
serializing `write!` outgrew and replaced, so a longer-than-usual AUTH line can
still leave one stale heap copy behind. The `Auth`
representation is an enum whose start, initial
response, and continuation variants carry their formatting invariants
structurally, so formatting has no optional response to unwrap. LOGIN ignores
the server's human-readable prompt and answers its first and second challenges
with username and password respectively.

**Default behavior change (migration note).** `PASSWORD_MECHANISMS` now defaults to SCRAM (strongest first) then PLAIN, and LOGIN is no longer in it (opt-in legacy, mirroring IMAP's `allow_login = false`). A server advertising only `AUTH LOGIN` therefore yields an empty attempt order under the default and fails with "no compatible authentication mechanism" instead of silently downgrading to LOGIN. Callers that need LOGIN must pass it explicitly via `authentication(vec![Mechanism::Login, ..])`.

Plaintext AUTH is refused by default for all mechanisms through SASL-owned `authentication_allowed`, including SCRAM and OAuth bearer tokens. Trusted local relays opt in with `dangerous_allow_insecure_auth(true)`.

AUTH continuation formatting treats challenge responses as continuation lines even for mechanisms supporting initial response. Required for OAUTHBEARER failed-auth dummy-cancel exchange (`AQ==` on the wire).

Challenge limits follow the shared mechanism state: PLAIN has no challenge
after its initial response, LOGIN has two replies, and OAuth acknowledges one
error challenge before terminal refusal. There is no driver-maintained LOGIN
index or generic ten-answer loop.

`SaslError` maps at the `From<SaslError> for Error` boundary. Typed
`ScramServerError::refusal_class()` in `reference/sasl.md` owns the table:
Credential becomes Authentication, Policy becomes Policy, Protocol becomes
Parse, and Unavailable becomes ServerUnavailable (shared Server Unavailable,
retryable). LocalRuntime stays local runtime; InvalidCredential remains
InvalidInput. `ScramExchange::new` prepares both credentials before AUTH.
`ScramExchange` obtains its typed Mechanism from the shared client rather than
storing a second wire-name string. Mechanism stamps retain the enum until
support diagnostic rendering.

Builder helpers: `.password(user, password)` for password auth,
`.oauth2(identity, access_token)` for a raw OAuth bearer string, and
`.oauth2_source(identity, Arc<dyn TokenSource>)` for a shared rotation
source.

## LMTP

`LHLO` instead of EHLO. Default TCP port 24 (Postfix/Dovecot convention, not RFC-assigned).

Per-recipient response model: vector length equals envelope recipient count. RCPT-time rejection responses are preserved at the original recipient index; accepted recipients receive the post-DATA delivery response in original order.

A final 421 is definitive for its recipient but does not end the final-status
drain. RFC 2033 sections 4.2, 4.3 and 5 require processing each received reply
in accepted-recipient order, for DATA and BDAT LAST alike. The client continues
reading the remaining finals; only recipients whose final reply was not received
remain uncertain with in-flight evidence. A completed drain containing a 421
keeps the connection broken and retired. Batch reporting retains every received
answer even when a later read fails. An enterprise queue must not retry an
already delivered recipient just because an earlier recipient was refused.

Direct LMTP sends expose only that ordered response vector. Use the account-oriented batch send API when the caller needs each recipient's RCPT-versus-final-status phase and recovery classification.

Unix-domain LMTP constructors are `#[cfg(unix)]` on sync and Tokio. Unix sockets refuse STARTTLS explicitly.

## Native-tls only

The TLS backend matrix (rustls, boring-tls, rustls provider/verifier) has been removed. `CertificateStore::Default` always means the native-tls platform verifier.

`TlsParameters` carries the connector and SNI hostname. Its builder exposes
`dangerous_*` options for accepting invalid certificates or hostnames; these are
public configuration options, not test-only APIs.

## Message builder

`Message::builder()` produces a builder for typed headers (From, To, Cc, Bcc, Reply-To, Subject, Date, Message-Id, In-Reply-To, References, MIME-Version, Content-Type, ...). `reply_to_many(...)` for RFC 5322 address-list `Reply-To`.

Display-name encoding emits RFC 5322 phrase text when the name is atom-shaped, quotes and escapes ASCII names that need it, and uses RFC 2047 encoded words for names requiring encoding.

Header folding is space-driven, so an unbreakable all-ASCII token (no spaces,
hundreds of characters) is written verbatim and its line can exceed 78 octets:
the writer does not split such a token or enforce the RFC 5322 Section 2.1.1
998-octet hard limit. A sufficiently long token can therefore exceed that limit
as well as the recommended 78 octets. Folding inside an atom would change the
value; callers must keep unbreakable tokens within the hard line limit. Long
non-ASCII display names are unaffected - encoded words are individually short
and the writer folds between them.

`MultiPart` kinds: `Mixed`, `Alternative`, `Related`, `Signed`, `Encrypted`, `Report { report_type }`. `Mixed` is the default for `MultiPart::builder().build()`.

Multipart builders ensure a `boundary` is present even when a caller supplies a boundary-less multipart `Content-Type`. `try_boundary`, `try_encrypted`, and `try_signed` are the fallible validation entry points; the infallible `boundary`, `MultiPart::encrypted`, and `MultiPart::signed` keep their signatures and panic on values that would break out of the MIME parameter (a deliberate runtime behavior change for callers passing unvalidated strings); default boundaries use OS randomness, and a caller-supplied boundary is regenerated if it appears at a MIME delimiter position in an added part.

A regeneration replaces the `boundary` PARAMETER and keeps every other
parameter on the multipart `Content-Type` verbatim, including ones outside this
crate's vocabulary. Rebuilding the header from `MultiPartKind` alone would
reproduce only the kind's own parameters, and since a re-roll is data-driven,
the same message would keep or lose a caller's vendor parameter depending on
its body. The fallback to a kind-rebuilt header remains for the one case a
lossless rebuild cannot cover: a parameter value containing a `"` or a `\`.

`SinglePartBuilder::body(String)` and `MessageBuilder::body(String)` infer `Content-Type: text/plain; charset=utf-8` when no content type is set.

`MessageBuilder::body` CRLF-normalizes bare LF at the builder boundary, so a byte body never reaches the DATA writer's defensive bare-LF dot-stuffing and gains a literal extra dot on a CRLF-strict relay. Normalization is scoped by the encoding that will actually be emitted: `String` input always normalizes, and `Vec<u8>` input normalizes only when it goes on the wire as literal `7bit`/`8bit` text. Byte input that is re-encoded (base64, `binary`) is opaque payload and is passed through untouched, so `0x0A` inside a binary attachment survives the round trip. `body_raw()` (DKIM) and the DATA writer both see the normalized buffer, so signing and delivery still agree. A pre-encoded `Body` is never rewritten.

One encoding decision governs both steps: the encoding chosen from the input
decides whether the buffer is normalized, and that same encoding is what the
resulting `Body` carries. Choosing again from the post-normalization bytes
would leave a window - narrow, and not reachable today - where a buffer was
CRLF-rewritten as text and then encoded as opaque payload, mutating bytes the
caller handed over untouchable. `crlf_normalization_never_changes_the_chosen_encoding`
pins the property the old two-decision shape rested on.

`Headers` cannot represent REPEATED header fields. `insert_raw` overwrites by
name, so a second `Received`, `Comments` or other repeated trace header
replaces the first rather than appending. That is correct for what this crate
is for - building fresh mail for submission - but a caller routing an existing
message through `Message` will lose repeated fields. Preserve such headers
outside `Headers`, or submit the original bytes with `send_raw`.

Typed headers for list management: `List-ID`, `List-Help`, `List-Unsubscribe`, `List-Unsubscribe-Post` (fixed value `List-Unsubscribe=One-Click` per RFC 8058), `List-Subscribe`, `List-Post`, `List-Owner`, `List-Archive`.

Content-type helpers: `text_plain_flowed()`, `text_plain_flowed_delsp()` (RFC 3676).

Batch builders: `MultiPart::multiparts(...)` and `.singleparts(...)`.

## DKIM

Default canonicalization is `relaxed/relaxed`. `DkimSigningKey` implements `From<rsa::RsaPrivateKey>` and `From<ed25519_compact::KeyPair>`; `new` also parses an RSA PKCS#1 PEM or a base64-encoded Ed25519 seed. RSA signing passes `sha2::Sha256` to `rsa::pkcs1v15::Pkcs1v15Sign::new`, so compatibility follows the digest traits required by that API rather than a fixed pair of crate versions.

Canonicalization follows RFC 6376 for empty bodies and missing final CRLFs. Relaxed headers are normalized iteratively, and `DKIM-Signature` is folded before `b=` before hashing so simple header canonicalization signs the exact emitted bytes. The signer intentionally does not support the `l=` body-length tag because it enables content-append attacks.

## Pool

Blocking setup owns a live-slot reservation guard until the connection is
parked or handed to checkout. Setup failure, unwinding, and shutdown during
replenishment release that reservation exactly once; successful handoff leaves
release to the connection's existing recycle or shutdown path. The guard is
created before the idle-set lock so releasing admission cannot re-lock a mutex
still held by the same thread. Replenishment that observes shutdown stops its
pass through normal cleanup, also releasing expired connections removed earlier
in that pass. A stopped transport must not retain phantom live connections.
Async setup already owns a semaphore permit with the same release-on-drop rule.

`PoolConfig` configures min idle, max size, idle timeout, and the checkout NOOP probe. `max_size` bounds all live connections, checked out and idle, in both the blocking and async pools; checkout waits for a slot instead of dialing past the bound. Zero is rejected at checkout. Recycling refuses connections that are `Broken` and connections flagged for retirement by an LMTP final-status drain (see "Connection state and cancel-safety"). `min_idle` defaults to 0: no background pool worker is started, and expired connections are discarded at checkout. A positive `min_idle` enables the worker that expires and replenishes warm connections without exceeding `max_size`. An idle timeout of zero disables expiration; a positive-`min_idle` worker checks for replacements once per second so a zero timeout cannot create a zero-delay loop. `test_on_checkout(false)` opts out of the default NOOP probe for callers willing to retry one stale idle connection; it does not permit a connection already marked `Broken` to be reused. It buys nothing for LMTP transports, whose connections are retired at recycle and therefore never checked out idle.

Async `PooledConnection::drop` calls synchronous recycle: a healthy, non-retired
connection is parked under the idle-set mutex and an availability waiter is
notified. A broken or retired connection, or any connection returned to a closed
pool, is instead moved with its admission permit into a spawned task. That task
awaits bounded `abort()` teardown before releasing the permit, so a replacement
cannot exceed the live-connection bound while teardown is pending. Recycling
itself has no await point or network I/O; ownership moves into the task before
its first poll. Abort closes the stream without sending QUIT, with the timeout
bound described in "Connection lifecycle".

Async shutdown stores one shared completion and runs idle-connection aborts and
worker termination in an owned task. Dropping a shutdown caller does not cancel
those aborts; concurrent and later callers await the same completion. Admission
and idle checkout close before that task starts. Checked-out connections still
belong to their callers and are aborted when returned to the closed pool;
shutdown does not wait for their sends. An account that needs send completion
must serialize its own sends with shutdown.

The admission gate participates in shutdown. Because `max_size` is enforced by a
semaphore (async) and a live counter plus condvar (blocking), a checkout blocked
on admission is reachable *only* through that gate: `shutdown()` closes the
semaphore and wakes the availability waiters on the async side, and notifies the
condvar on the blocking side. Blocking-side releases notify the condvar only
while holding the pool mutex, because the checkout's failed reserve and its wait
are atomic only against notifiers holding that mutex - a lock-free notify could
land between the two and strand the waiter. Async checkout additionally re-reads the pool state
after winning admission and before dialing. Without both halves of this a
checkout could park forever, or - once a connection checked out before shutdown
was returned and released its permit - dial a NEW connection through a pool that
was already closed.

## Bandwidth metering

All four builders, `SmtpTransportBuilder`, `LmtpTransportBuilder`,
`AsyncSmtpTransportBuilder` and `AsyncLmtpTransportBuilder`, expose
`bandwidth_metering(sink, Option<BandwidthBudget>)`. The optional
`bifrost_net::MeterSinkHandle` records successful plaintext bytes. A missing
budget creates an unlimited budget. Cloned configurations and all connections
built from them share the budget, including across pools. Callers can supply the
same budget to separate transports. `UNLIMITED_BANDWIDTH` remains exported at
its existing path as the legacy atomic encoding; budgets use `None` for unlimited.

`WireMetering` holds only the optional sink and shared `bifrost_net::BandwidthBudget`.
Both directions reserve before I/O. Each stream holds at most one admission per
direction. Reservations grant at most the requested length, one second of cap,
and `MAX_OFFER` (256 KiB). Reads wait for admission, offer the reservation, and
limit the read buffer to its grant before touching the peer. A fresh connection's
first read therefore waits behind outstanding inbound reservations. Outbound
writes follow the same admission path; two connections cannot each spend the
same initial credit. Inbound and outbound schedules are independent.

Successful I/O finishes its reservation with the actual byte count, refunding an
unused remainder only under the shared budget's current-tail rule. Dropping an
offered reservation refunds nothing, because I/O may already have accepted bytes.
An unoffered cancellation refunds only the current tail; issued sibling slots
never move. Finite retunes price future reservations without changing issued
sizes or times. Crossing unlimited invalidates queued reservations; async waiters
wake on policy changes. Pending TLS writes retain the original offered length
across retunes, as required for native-tls retries, clamped only to a shorter
buffer the caller re-polls with.

For a constant finite cap `c`, the per-direction bound over `T` seconds is
`c * (T + 1) + A`, where `A` is all unspent allowances at the window start,
including queued, ready, offered and outstanding unlimited grants. There is no
bound across unlimited intervals. Idle pending reads can waste capacity by
pre-spending a slot. This is payload smoothing above TLS, not a wire-rate promise:
TLS headers, MACs, padding and bytes buffered below the adapter are excluded.

Absolute async setup deadlines include admission waits. Per-operation writes
admit before arming their peer timer. Replies use cumulative peer time as
described above; admission is excluded by construction, without extending a
deadline after waiting. Blocking admission uses the synchronous reserve/offer
API and sleeps the calling thread in slices of at most one second, checking
policy between slices. No Tokio runtime is required by the blocking transport.

IMAP submission and IMAP connections share the account meter and one
`BandwidthBudget`, including its inbound and outbound admission schedules.
`set_bandwidth_cap` updates that shared budget, so traffic on either protocol
contends with the other and observes live cap changes.

The non-default `test-support` feature implies `tokio` and exposes
`transport::smtp::Transcript` and `ScriptedConnector`. Both async transport
builders accept `scripted_connector`; its factory must create a fresh transcript
for each pooled connection because transcript clones share consumption state.
The peer accumulates partial client writes before releasing a scripted reply.
It replaces stream creation before the normal metering installation and retains
the shared setup deadline, greeting, EHLO/LHLO and authentication path. Scripted
connectors require plaintext configuration; TLS and Unix socket configurations
are rejected.

## Error model

The account boundary classifies into `PrimaryCause`, including the protocol
subkind for wire failures. Rate and quota causes carry their account throttle
payload. Transport causes and kinds share `TransportErrorKind`; no conversion
between duplicate transport enums remains. See `reference/error-model.md`.

Direct SMTP and LMTP sends deliberately return the wire-level `Error`; batch
submission returns shared `AccountError` values at the batch and recipient
boundaries. This is a product choice: a transport integrator needs full SMTP
replies and a stable direct error type even without `account-error`, while an
enterprise Outlook user expects successful recipients to remain sent and failed
or uncertain recipients to retain their own recovery information. Changing the
direct error type alone cannot provide those recipient outcomes. Client
developers implementing a send queue should use batch submission and
`submission_status`, preserving the lanes on the receipt. The direct contracts
remain useful for callers implementing their own wire-level policy.

Token-source failures use the direct `Connection` error class with and without
`account-error`. An enterprise Outlook user expects a token endpoint outage to
retry, rather than report invalid caller input or demand a replacement password.
This wrapper class does not override the account-aware recovery projection:
actual credential loss remains terminal and transient refresh failure remains
retryable. Unsupported synchronous resolution remains local `InvalidInput`.
The feature-independent credential test pins the direct class and AUTH phase.

The typed OAuth projection stored inside the wire error is intentional. Direct
and batch sends share credential resolution, but bifrost-net owns the distinction
between terminal credential loss and transient token refresh failure. Typed SASL
errors do not replace that information. Keeping it through the transport wrapper
lets batch recovery retry a transient refresh failure instead of asking the user
for another password. Local auth classification reads typed error kinds; source
Display text remains support diagnostics rather than a local auth discriminator.

`Error::kind()` returns `&ErrorKind`. SMTP reply failures are `Transient(Response)` or `Permanent(Response)`, so callers can inspect the full server reply through `smtp_response()`, `status()`, and `enhanced_status_code()`. Non-reply buckets are `Parse`, `Authentication`, `InvalidInput`, `FeatureUnsupported`, `ParameterOverLimit`, `Internal`, `LocalRuntime`, `ServerUnavailable`, `Policy`, `Connection`, `Network`, `Timeout`, `Tls`, and `TransportShutdown`. `LocalRuntime` covers local facility failures such as SCRAM nonce entropy failure and maps to `Internal(RuntimeFailure)`, including on the AUTH lane. `Policy` covers local refusals such as plaintext-AUTH refusal. `FeatureUnsupported` / `ParameterOverLimit` distinguish missing extension support from exceeded advertised parameter limits (for example, missing FUTURERELEASE vs HOLDFOR over its maximum interval), kept distinct from generic `InvalidInput` so the account-error mapping can tell scheduled-send-unsupported apart from a malformed parameter. SMTP replies are line-bounded before allocation and decoded from bytes, so oversized or non-UTF-8 replies are `Parse`, not retryable network errors.

Internal pipeline errors carry value-side classification data:

- `SmtpTransmissionState` (`Unsent` / `InFlight` / `Acknowledged`), attached via `with_attempt`.
- `SmtpCommandPhase`, attached via `with_phase`. The 16 variants cover every wire boundary the driver crosses: `Connect`, `Greeting`, `Hello`, `StartTls`, `Auth`, `MailFrom`, `RcptTo`, `DataCommand`, `DataBody`, `DataFinal`, `BdatBody`, `LmtpFinalStatus`, `Noop`, `Vrfy`, `Expn`, `Rset`. `Auth` alone carries a payload, `Auth { mechanism: Option<Mechanism> }`: the name sits on the variant rather than on `Error` so that stamping the auth phase forces a decision about which rung was in play, the same argument that puts `phase` on the value rather than on `SmtpErrorContext`. `Some` comes from the session machine's selected authentication, which binds the `Mechanism` once per exchange, so a `-PLUS` rung cannot desync from the AUTH line that framed it. `None` is for refusals before a mechanism is retained: declining to pick a mechanism, refusing plaintext AUTH, or nonce generation during SCRAM preparation. OAuth token resolution happens after selection, so the session stamps token-source failures with the selected mechanism. `or_phase` is set-if-absent with exactly one exception - an unnamed `Auth` stamp accepts the mechanism from a named one, because otherwise the inner `From<SaslError>` stamp would outrank the driver's named outer stamp and the SCRAM `e=` path would lose the name it had. `DataCommand` covers the `DATA` command write/read; `DataBody` covers body upload; `DataFinal` covers the final reply after the dot terminator. Phase lives on `Error::Inner`, and every send-driver wire boundary attaches it before returning - transport failures and server rejections alike. On the pipelined path that is a compile-time property of `PhasedError` rather than a convention (see "PIPELINING"). Paired transcript tests assert the value directly on both I/O halves for the `MailFrom`, `RcptTo`, `DataCommand` and `DataBody` boundaries, so the default PIPELINING path cannot silently lose the classifier input.

With `account-error`, OAuth token-source failures also carry bifrost-net's already-built `AccountError` projection through the low-level SMTP error. The SMTP translation boundary reapplies its actual SMTP or LMTP protocol and fixed send operation and idempotency context while preserving the source kind and cause chain. The token-source conversion passes no throttle scope because it has no SMTP transaction scope to name. SMTP reply classifications of `RateLimited` or `QuotaExhausted` explicitly carry `ThrottleScope::Account`. This keeps `AuthLost` and `RefreshTransient` distinct.

`crates/smtp/src/transport/smtp/account_error.rs` is the single translation boundary into the shared `AccountError`. It funnels through `AccountErrorBuilder::try_build` (never the removed `.build()`), reads command phase only from `error.phase()`, and classifies local failures by their explicit kind. Negotiation and channel-binding policy refusals use `Policy`, becoming `Authorization(PolicyBlocked)`; an unusable peer certificate during channel-binding extraction uses `Parse`, becoming `Protocol(ParseFailed)`; unsupported blocking token resolution and invalid credentials use `InvalidInput`, becoming `Request(Malformed)` regardless of AUTH phase. Unexpected PLAIN or excess LOGIN challenges and exhausted challenge budgets use `Parse`, becoming `Protocol(ParseFailed)`. An enterprise Outlook user expects policy repair for incompatible authentication settings, configuration repair for the wrong token-source transport, and a server protocol error for malformed challenges; none should demand a replacement password. Nonce entropy failures remain `LocalRuntime`, becoming `Internal(RuntimeFailure)`. Typed OAuth token-source projections pass through this boundary, so refresh failures stay retryable and terminal credential loss stays `AuthLost`. The auth lane is also where reply codes stop meaning what they mean elsewhere: `is_auth_lane` sends `504` and `504 5.5.4` to `Authorization(PolicyBlocked)` rather than to `Unsupported(Send)` or `Request(Malformed)`, since a server refusing a SASL mechanism has not said anything about whether it can send mail, and it refused before any credential crossed the wire, so `Authentication(_)` would send the user to re-enter credentials that were never presented. That is the same kind the local half of the condition already produced, so one condition now gets one answer whichever side noticed it. `454` and `4.7.0` on the same lane become `Authentication(RefreshTransient)`, which is RFC 4954's "temporary authentication failure" and the only kind meaning retry-but-refresh-first. Off the auth lane all four keep their RFC 5321 meanings. Where the phase carries a mechanism name, both auth lanes fold it into the `DiagnosticText::support_only` text; classification never reads it, and the support tier is not a preference but the only tier that can hold free-form text at all (`TelemetryView` carries none, pinned by `telemetry_has_no_free_form_text`). It also maps `FeatureUnsupported` to `Unsupported(AccountOperation::Send)` (so the IMAP layer surfaces a stable `Unsupported(Send)` kind when a relay lacks FUTURERELEASE) and `ParameterOverLimit` to `Request(Malformed)` (the hold time is outside the allowed window). `message_error_to_account_error(MessageError, Protocol) -> AccountError` is the boundary for builder-side validation failures (`MissingFrom`, `MissingTo`, `EmailMissingAt`, ...); every variant maps to `Request(Malformed)`.

Evidence must match what actually crossed the wire. A transport failure in the envelope phase (`MAIL FROM` / `RCPT TO`), including a failed write of a later PIPELINING recipient window, is `Unsent` and resolves through `SendProgress::mark_unresolved_unsent`: `DATA` has not been issued, so no message content can have reached the peer and an `Uncertain` lane would falsely claim a possible delivery. RCPT rejections the server already gave are preserved; accepted and unanswered recipients are both rewritten (an accepted RCPT with no `DATA` would otherwise resolve as a delivery that never happened), into `failed` lanes that `RecoveryClass` derives as `Retry(SameRequest)`. The `DATA` command itself is still pre-body: a failed `DATA` write or a lost reply to it is `Unsent` / `DataCommand` and resolves the same way (`SendPhase::DataIssued` counts as pre-body in `SendProgress::resolve`), because no message content was offered until the server's `354` started the body write. `InFlight` is reserved for failures from the body write onward (`DataBody`, `BdatBody`, `DataFinal`, `LmtpFinalStatus`).

The dividing line for batch sends is whether the body has left. A batch-level
`Err((error, progress))` means "nothing was transmitted, resend the whole
request"; once `set_body_started` / `set_body_finished` has run, that claim is
false and the machine must finish `Ok(progress)` with per-recipient lanes even
when the failure is a transport one. `BatchTransaction`'s `GroupOpened` used to finish
`Err` on a failed reply-group open, one stage after `BodyWritten` terminated the
body, so an engine reading it would have resent a non-idempotent message that
may well have been delivered. It now marks the accepted recipients `uncertain`
(`InFlight` / `LmtpFinalStatus`) and finishes `Ok(progress)`, which is the same
answer its `FinalStatus` arm gives (2026-09-07). It still performs no abort: the
failure comes from the stream's own `verify()`, so the stream is already
unusable. Pinned by
`a_failed_lmtp_group_open_leaves_the_recipients_uncertain_not_the_batch_unsent`.

`BatchTransaction`'s `WindowOpened` and `WindowClosing` sit on the other side of that
line and deliberately keep the batch-level `Err`: every recipient window is
before `DATA`, so nothing was transmitted and the whole request genuinely is
retryable. The cost is accepted and worth knowing before writing retry logic
against these lanes: both transports discard the `progress` half of the tuple
(`Err((e, _progress)) => Err(batch_level_error(e, ctx))`), so RCPT answers
already collected in earlier windows - including 550s the server gave - fold
into one `Unsent` batch-level retry and the caller learns nothing about the
recipients already refused, relearning them on the resend. Turning those answers
into lanes would mean returning `Ok(progress)` for a request that never reached
the peer, which is a worse lie than the lost diagnostics. Both arms carry a
comment saying so.

`batch.rs` `SendProgress::resolve` is the per-recipient lane resolver. LMTP `DATA`-command negative replies route through `mark_accepted_rejected_with_response` so accepted recipients become per-recipient `Failed` lanes - never a batch-level `Err` (the previous shape let the engine resend the entire non-idempotent `Send` after the server rejected it). DATA-final-negative replies tag `SmtpCommandPhase::DataFinal`,
and that arm is live rather than latent: `BatchTransaction` records a negative
end-of-data reply through `set_data_response` exactly as it records a positive
one (see "PIPELINING"), so `DataFinal` is a phase a driver actually produces.
Those lanes derive as a PERMANENT refusal: an SMTP 5xx is RFC 5321's permanent
negative completion, and `bifrost-types`' `derive_server` reads the error's
`protocol` field, which the SMTP translator stamps as SMTP or LMTP, so a
554 at the terminator is `ProviderRefused` rather than the HTTP 5xx
`Retry(SameRequest)` it derived before (2026-09-07; see
`reference/error-model.md`, the Server row). After the body starts, LMTP `Accepted` at resolve time without a per-recipient `Final` is a programming bug caught by `debug_assert!` in debug builds and falls back to an `Uncertain` lane in release. Every failed and uncertain lane carries the envelope recipient as `DiagnosticText::support_only` so support exports preserve per-recipient correlation when N lanes share the same wire response text.

## Connection test harness

Two layers, and both still bite. The core-level tests in
`client/core/core_tests.rs` drive the transaction machine with a scripted list of outcomes
and record its op stream, so window reply accounting, coalesced-reply handling,
RSET-and-keep, abort-on-I/O-failure, LMTP per-recipient final status and BDAT
sequencing are each pinned once, in the place the rule now lives, with no I/O
at all. Every one of them was confirmed to bite by reintroducing the bug it
describes. `client/session.rs` also tests successful TLS-effect ordering without a
handshake, and `client/reply.rs` tests chunk splits, caps, UTF-8 and EOF.
`client/conformance.rs` defines the session and send transcripts once and runs each
through both adapters when `tokio` is enabled (blocking only otherwise).
It covers both SCRAM server-final shapes, token resolution, STARTTLS refusal
and injection, capability refresh, envelope validation and parameters,
PIPELINING windows and coalesced replies, DATA/BDAT framing, recipient lanes,
EOF evidence, reset and abort decisions, and LMTP drain retirement and stream
state. Cases include sequential and pipelined recipient read failures and
split-CRLF body chunks.
The connection files retain private I/O, certificate, timeout, cancellation and
standalone lifecycle tests. The shared tests call the real connection entry
points; their adapter facade only forwards calls and never interprets outcomes.

A scripted outcome list alone cannot reach every boundary. Writes and group
boundaries consume the next scripted failure whichever one it is, so a failure
meant for a later boundary is swallowed by the first non-reply op that runs -
which left `OpenReplyGroup`, `CloseReplyGroup`, `CloseLmtpDrain` and the
epilogue's own `RSET` write untargetable. The core harness therefore also takes
an *aimed* failure, `Harness::aiming(Aim)`, matched against the op stream rather
than queued in the script: `Aim::Position(n)` fails the nth op outright, and
`Aim::Kind(selector, nth)` fails the nth op matching a selector - an exact op
tag (`OPEN`, `CLOSE`, `DRAIN`, `READG`, `READ`, `BODY`, `BDAT`, `ABORT`) or a
`W:` prefix matched against the rendered write, so `Aim::Kind("W:RSET", 0)`
names the reset write and nothing else. The replies before the aimed op are
untouched, so the machine reaches the boundary in the state production would.
That is what pins the failed-RSET-write abort, the two pipelined group
boundaries returning their error without a second abort and with the recipient
lanes they had collected (which the transports then discard - see "Error
model"), and an LMTP drain close that fails after every final
status was recorded (the batch result stands; only the stream is lost).

The conformance suite carries the other half of that pair: a requested op is not
a performed op, so each adapter's run observes the stream state after the
exchange, not only its result. `an_aborting_batch_failure_leaves_the_stream_broken` (abort,
and not retired), `a_rejected_pipelined_mail_from_restores_the_stream_at_the_group_close`
(the group bracket restores `Ok` when every reply drained),
`a_clean_lmtp_batch_drain_leaves_the_stream_broken_and_retired`
(`restore_ok: false` reaching the wire) and the direct-LMTP drain pins
(`Ok` plus retired) are defined once and run through both halves. Each was
confirmed to bite by breaking the adapter's handling of the op it targets - dropping the `Op::Abort` arm,
dropping the `Ok` restore in `finish_reply_group`, and ignoring `restore_ok` -
rather than only by reverting the core. One limit of that observable is worth
knowing before trusting it: asserting `has_broken()` after an abort does NOT
bite against removing `abort()`'s own `set_state(Broken)`, because the
shutdown it also performs sets `Closed`, which fails `verify()` just as well.
It is sharp only against the adapter dropping the op entirely, which is the
thing worth pinning.

`test_support::Transcript` is the in-process scripted peer both connection
drivers test against; it replaced the socket-listener tests, which were neither
hermetic nor deterministic. `test_support` binds no `TcpListener`
and no `UnixListener` and spawns no threads; timed async peers use Tokio timers
under paused test time. The transport-level LMTP delivery pins (`lmtp_transport_returns_per_recipient_statuses` and its
`tokio_` twin) run against a parked transcript connection, and the Unix-socket
case pins the routing decision - builder wiring plus the TLS-over-Unix refusal
raised inside `connection()` before any dial - since the kernel's socket is not
the part a test can prove in-process. `hermetic_transport_tests.rs` replaces
the former ignored live-server transport tests with parked transcripts: blocking
and Tokio message sends, blocking raw sends, and cloned blocking transports
sending from two client threads before reuse on the main thread. The thread
case uses a barrier and one pool slot, with no sleeps or sockets. The pre-AUTH refusal ladder uses
transcripts: the post-greeting authentication stage is factored into
`SmtpClient::authenticate_if_configured` (and its async twin) precisely so
`plaintext_auth_is_refused_before_auth_command` can drive it against a
transcript that scripts no AUTH step, where an AUTH command reaching the wire
fails the write instead of producing the policy error. The escape-hatch mirror
scripts the exact `AUTH PLAIN` line and the post-AUTH EHLO. A transcript is a
greeting plus an ordered list of
`expect(client_bytes, server_bytes)` steps. These properties make it bite:

- Each client write must match one scripted step byte-for-byte, so command
  batching shape (which commands share a write) is pinned, not just command
  order.
- Server bytes become readable only after their step's write.
- Reads hand out at most one reply line. Without this, a `BufReader` prefetches
  a whole pipelined reply group, and a driver that skipped replies would still
  pass. Writing while replies are pending is an error, which is how the
  PIPELINING window drain and the LMTP surplus-final-reply desync are detected.
- `expect_coalesced(...)` opts one step out of the line split so its replies
  arrive as a single segment, the way a real peer's TCP stack coalesces
  adjacent replies. That is what a `BufReader` prefetches, so it drives the
  same surplus-detection path production has. The harness has no visibility
  below the buffer that a real transport lacks.
- `expect_generated(...)` accepts one write and derives the peer reply from its
  bytes. The SCRAM exchange uses it to echo the client nonce, check the
  once-decoded client-final, and return a valid server signature on an enhanced
  `235` reply.

- `expect_then_close(...)` models a peer that answers with the given bytes and
  then hangs up. The bytes may be a truncated reply line, so this is the
  close-mid-response case: reads report EOF once the scripted bytes are gone
  and any later client write fails the way a write to a closed socket does.
  A truncated reply must surface as an `incomplete SMTP response` network error,
  never a hang or a provider contract violation. The `MAX_RESPONSE_LINE_BYTES` cap is pinned the same way, by
  `an_oversized_greeting_line_is_a_parse_error_not_a_hang` in both halves. Note
  that the greeting it scripts is well-formed and merely over-long: a line of
  garbage fails to parse whether or not the cap is enforced, so it pins nothing
  about the cap.

The transcript stream carries no TLS, so `peer_certificate_der()` is `None`
under test. A test-only seam on `AsyncNetworkStream`
(`set_test_peer_certificate_der`, surfaced as
`AsyncSmtpConnection::from_transcript_with_peer_certificate`) injects a DER so
the connection-level channel-binding gate in `auth` is pinned hermetically in
both directions: a PLUS-advertising server with a usable certificate must be
answered with the PLUS mechanism, a present-but-unusable certificate is a hard
error with no fallback AUTH written, and only an absent certificate falls
through to a weaker mechanism.

`expect_then_stall` and `Transcript::silent()` model a peer that accepts and
then never answers; reads park with no waker, so only the caller's own timeout
or cancellation resumes the task. That is what the async timeout, setup-deadline
and cancellation tests observe, under `start_paused` tokio time.

`test_support::StalledPeer` is the same idea one layer lower: a bare
`AsyncRead`/`AsyncWrite` that swallows every byte and parks every read. It
exists for the setup-deadline cases whose traffic is not scriptable - a TLS
`ClientHello` is opaque bytes, so `expect(...)` cannot match it - and it is what
`tokio_tls_handshake_uses_deadline` drives `upgrade_tls_stream` against, in
place of the listener plus sleeping thread that test used to need.

Pool-level tests park a transcript-backed connection directly
(`Pool::park_for_test`) and drive the public batch entry points through it, so
the LMTP retirement rule and SMTP connection reuse are pinned end to end
without dialing anything. A real TLS handshake stays out of scope: the
`starttls` transcripts follow the upgrade to the handshake boundary (command
written, positive reply read) and stop there, since the in-process stream is
not a TCP socket.

## Example validation

Examples live under `crates/smtp/examples`. Sync examples require no crate
features. Tokio examples are gated by `tokio`. Validate examples with the SMTP
crate checks under both the default feature set and `tokio`.

## Module layout

```
crates/smtp/src/
├── message/             - builder, headers, MIME parts, address parsers
├── transport/
│   └── smtp/
│       ├── client/      - core (transaction), session, reply, connection and
│       │                  async_connection (the two I/O adapters), net,
│       │                  async_net, metering, tls
│       ├── surface.rs     - shared builder, transport product, aliases and methods
│       ├── transport.rs / async_transport.rs - connection factories and tests
│       ├── dial.rs        - Tokio TLS/socket dial sequence
│       ├── commands.rs  - EHLO, MAIL, RCPT, DATA, BDAT, AUTH, NOOP, RSET, VRFY, EXPN, STARTTLS, LHLO
│       ├── extension.rs - ServerInfo, SendOptions, MAIL/RCPT parameters
│       ├── response.rs  - parser, enhanced status codes
│       └── test_support.rs - in-process `Transcript` harness and timed peers
└── error.rs             - message-builder Error (SMTP Error/ErrorKind live in transport/smtp/error.rs)
```

## SMTP-specific code style

- `client::` is private; low-level connection/network stream types are crate-internal.
- TLS uses native-tls unconditionally; rustls/boring features have been removed.
- `#[non_exhaustive]` on message-builder `error::Error`, SMTP `ErrorKind`, DKIM enums, and `CertificateStore`; the opaque SMTP `Error` struct has private fields.
- `AsyncTransport` requires `Sync` so borrowed methods return `Send` futures.
- Async traits use native `impl Future + Send` return types; no `async-trait`.

## Unsupported error construction

Local refusal helpers use `AccountError::unsupported` through the shared error
funnel, retaining their provider, scope and diagnostic decoration.
SMTP and LMTP final assembly use the shared constructor for unsupported
classifications, retaining the transport protocol and transmission evidence.

## Multi-step producer audit

SMTP/LMTP submission is one transaction. Post-DATA per-recipient effects are
already represented by the succeeded, failed and uncertain lanes in batch.rs
and partial_completion_error. There is no applicable multi-step failure path
requiring a partial-commit record above those lanes.

## Dial policy and parameter identity

`DialPlan` in the SMTP transport module owns the TLS/socket policy used by both
blocking connection setup and the Tokio dialer: implicit TLS parameters,
required versus advertised-only STARTTLS, and refusal of TLS on Unix sockets.
The adapters perform I/O and keep their existing timeout semantics. Invalid
local combinations remain `InvalidInput` errors.

MAIL parameters have an exhaustive `keyword()` projection to `MailKeyword`.
One keyword vocabulary supplies rendering and the reserved typed-keyword check;
duplicate detection reads this projection without rendering a parameter first.
RCPT rendering uses its existing keyword projection and keeps last-value-wins
normalization. Direct and batch envelope validation share
`Envelope::addresses_need_smtputf8` for sender and recipient address inspection.

Reply reads use `ReplyMode::{Positive, AnyStatus, Grouped}` rather than two
booleans. Grouped reads leave state restoration and surplus detection to the
reply-group close. Positive mode retains authentication's positive-reply rule;
transaction completion and DATA admission retain their stricter phase rules.

Authentication stores `MechanismChoice::{Default, Explicit}`. Defaults resolve
against the current credential kind at use; explicit lists, including an empty
list, survive credential replacement. Credential storage and live token-source
resolution live in `authentication/credentials.rs`, re-exported at the existing
public paths. SMTP challenge decoding lives in `authentication/framing.rs`. Mechanism
selection and the SCRAM exchange driver remain in `authentication.rs`; the cryptographic computation
already delegates to bifrost-sasl. The exchange interprets SMTP challenges and
errors and is not a protocol-independent computation to move into that crate.

The retained message-builder error translation uses `MessageError::Display`
for both diagnostic copies, keeping all message construction failures in the
existing malformed-input classification. Blocking bandwidth admission remains
in the SMTP adapter until bifrost-net supplies a blocking budget API; removing
that wait loop would remove blocking transport throttling.

## Wire dialect and submission interpretation

`Dialect::{Smtp, Lmtp}` is the two-valued wire configuration type, including
`default_port`, connection options, both connection adapters, batch progress,
and `SmtpErrorContext`. Other account protocols cannot configure an SMTP
connection. Conversion to the shared error-model `Protocol` happens when
constructing shared errors or validating shared batch input. The context carries
only the dialect: operation is always `Send`, idempotency is always false,
provider decoration is preserved from a preclassified source, and transmission
evidence comes only from the low-level error.

`submission_status(&BatchOutcome<(), K>)` owns submission interpretation and borrows
the outcome without consuming its recipient lanes. `SubmissionStatus::Committed`
means at least one recipient acknowledged delivery, even if others failed or
remain uncertain. An enterprise Outlook user expects that message to remain
sent, with those other recipients reported on its receipt; retrying the entire
message would duplicate delivery. With none acknowledged,
`AcceptanceUncertain(&AccountError)` preserves the first uncertain error,
`Rejected(&AccountError)` preserves the first failed error only when there is no
uncertainty, and `Empty` identifies a batch without outcomes. The helper leaves
the `Result` projection to its caller. The account submission facade returns
`Err` with a clone of the first uncertain error when no recipient acknowledged
acceptance, preserving uncertainty so the caller can reconcile before retrying.
It does not return an uncertain receipt in that case. Other direct batch callers
retain their own UI and retry policy.

The direct APIs retain their distinct contracts. Direct SMTP's single
`Response` cannot retain recipient refusals alongside partial delivery, so it
stops before sending a body when any RCPT is refused. Direct LMTP returns every
recipient's `Response`; `Ok` means the exchange completed and may contain only
negative statuses. Neither contract is removed or silently reinterpreted.
Message-submission consumers should use the batch APIs and `submission_status`
for the shared committed-message rule, retaining all lanes on their receipt.

Shared SMTP wire causes retain enhanced status components as
`EnhancedStatusCode::components: Option<(u8, u16, u16)>` (class, subject,
detail). Machine consumers need no consent-tier text parsing. The constructor
also derives the existing dotted diagnostic spelling from those components.
Fallback prose hints live in `RATE_HINTS`, `QUOTA_HINTS` and `POLICY_HINTS` in
the account translator; they preserve first-line ASCII-insensitive substring
matching and apply only after recognized enhanced codes have been considered.

Header-derived `Envelope::try_from` delegates reverse-path selection and
checked recipient extraction to `bifrost_types::mime::HeaderMap::submission_envelope`.
A single From takes precedence over Sender so a shared mailbox receives its
own bounces when a delegate sends on its behalf. Multiple authors require a
single Sender; missing From and Sender retain the null reverse path.
Unsupported addresses fail conversion rather than silently losing recipients.

## Account submission facade

With `tokio` and `account-error`, `submission::Submission` owns checked shared
`SubmissionEnvelope` address conversion, absolute HOLDUNTIL formatting, and
three-lane recipient projection. `SubmissionConfig::new(host, SubmissionTls)`
selects submission ports and TLS; its public `transport` builder retains every
SMTP option, including credentials, pool, timeout, meter, shared bandwidth budget
and scripted connector. `SubmissionCredentials` preserves rotating token sources.
`submit` returns `SubmissionReceipt::recipients_not_accepted` after any recipient
commits; when none commits, uncertainty takes precedence over rejection. This
prevents an enterprise send queue from duplicating mail to accepted recipients.
IMAP keeps credential inheritance, default From and Sent-folder policy.

Blocking OAuth snapshot contention uses `TokenSnapshot::contention_account_error`
for transient authentication recovery, without fabricating a network failure.

OAuth credentials with no allowed advertised OAuth mechanism fail locally with
a policy error in both drivers before certificate resolution or password
selection. A token must never enter the password authentication ladder.

`error::Error::TooManyFrom` (locally aliased as `MessageError`) remains public
and is still produced by the message builder for multiple authors without Sender. Multiple authors with one Sender
are valid; header-derived envelope conversion instead returns `InvalidInput`
for invalid author/Sender combinations.

The submission facade stamps its own malformed-envelope and HOLDUNTIL failures
with `Protocol::Smtp`: SMTP owns those validations even in an IMAP-composed
account. With the optional `tracing` feature, committed partial submissions
emit a warning with delivered, failed and uncertain counts and no addresses.
`jiff` is optional and enabled by `tokio` for HOLDUNTIL formatting; the facade
itself additionally requires `account-error`.


## Shared credentials and TLS mode

SMTP `Credentials` and `SubmissionCredentials` alias `bifrost_net::Credentials`.
Password constructors accept strings, `Zeroizing<String>` and `Secret`; cloning
keeps the password zeroizing. Debug never formats password bytes or token sources.
SMTP retains its protocol-specific token error mapping and blocking snapshot path.
`SubmissionTls` and `PortTls` alias net's `credentials::TlsMode`; plaintext is
`None`. `TlsMode::port_for(TlsService)` owns the port table. Relay plaintext uses
25, submission plaintext uses 587, and LMTP keeps 24 in every mode. Low-level
SMTP `Tls` retains required versus opportunistic upgrade and connector parameters;
those are transport capabilities beyond the shared encryption mode.

Recipient batches can retain caller payloads with `send_raw_batch_with_payload`:
inputs are `BatchItem<(Address, I)>` and outcome keys are `BatchItem<I>` in all
three lanes. Caller ids are validated before sending; the closed ledger attaches
payloads without parsing those ids. The account submission facade carries the
original shared `Address`, including its display name, so failed and uncertain
recipients are returned directly even when LMTP skips RCPT refusals during its
final-status drain. Enterprise users receive the intended recipient on each
failure receipt instead of a recipient inferred from lane position.
