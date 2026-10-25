# bifrost-imap reference

Current architecture of the IMAP client crate. Daaki-derived. Tokio + native-tls only.

The public consumer surface is intentionally small after S1-W3: `ImapAccountFactory`, `ImapAccountConfig`, `ImapConfig`, `ManageSieveConfig`, `Credentials`, `AuthPolicy`. Consumers construct the factory and use it through `Arc<dyn bifrost_types::AccountFactory>`; raw IMAP, ManageSieve, parser, command, protocol-error, and sync-helper surfaces are crate-internal. CardDAV/CalDAV composition (`with_carddav`/`with_caldav`) is under "Account layer".

## Driver-owned I/O

One tokio task owns the socket, parser state, and a watch channel exposing connection state. `DriverTask` keeps its join handle for close and an exit slot for retained session leases. `DriverScope` destroys the driver future and transport before publishing exit or releasing those leases, including abort before first poll, panic and runtime teardown. `ImapConnection` is a cheap internal handle around the driver. Connection methods take `&self` and talk to the driver over channels.

The driver model is load-bearing for cancellation safety, atomic stream upgrades (STARTTLS, COMPRESS), typed event-queue draining without contending with command I/O, and streaming FETCH without raw stream access.

No boxed-transport escape hatch. Supported transport is `tokio::net::TcpStream` plus optional `tokio_native_tls::TlsStream`.

Before the nom parser runs, `WireReader` asks a framing pre-check whether the buffer can hold a whole response, because the parser is in `complete` mode and cannot distinguish "truncated" from "malformed". The pre-check has three answers, not two: complete, need more bytes, and *unreachable*. Unreachable is a literal count this process can never be handed the octets for  -  above the RFC 9051 `number64` ceiling, or above `usize` on a narrow target, or a chain of counts whose running offset leaves the addressable buffer. Those return `Error::Parse` (connection-fatal, `ProviderContractViolation`) rather than "need more bytes", which would park the read loop until the peer closed. The bound matches `codec::decode::literal`, so the pre-check never accepts a count the decoder would reject and never rejects one the decoder would take.

The literal marker parser (`connection/literals.rs`, `literal_marker_at`) is shared by the read side's framing pre-check and the encoder's walk over caller-written SEARCH / SORT / THREAD criteria. It answers three ways: a counted marker (count guaranteed `<= i64::MAX`), a well-formed marker whose count is above the RFC 9051 `number64` ceiling, and "not a marker". Each call site decides what the middle answer means for it - fatal framing on the read path; on the criteria walk it cannot arise, because the validator has already refused a count whose payload is not inside the criteria. A digit run too long even for `u64` is not recognizable framing at all and is handed to the decoder. The encoder never scans its OWN output: see "One command path" below.

FETCH body and binary fields use `bytes::Bytes`. `WireReader` parses responses smaller than 4096 bytes with owned literals and advances a consumed-prefix offset, reusing its read allocation. It compacts the unread suffix in place only after the consumed prefix exceeds half the buffer capacity, so many co-read small responses do not move the suffix once per response. Larger responses transfer their backing to the parser, copying only when capacity exceeds twice the frame length. Co-read suffix bytes move to a separate read buffer with capacity at least 8 KiB. Literals smaller than 4096 bytes or smaller than one quarter of their frame are copied into independent storage; large proportionate literals retain slices without copying the payload. Standalone parser entry points that receive only `&[u8]` copy literal data because they have no owned backing allocation. The response still has to be fully framed and parsed before it is emitted, so this does not stream a literal incrementally. The literal ceiling still counts declarations before reading payload, and non-literal syntax and parser object overhead remain outside it.

SEARCH-family extension criteria are checked by the encoder using live capabilities, including pipelined commands. The handle scans no criteria capabilities before queuing. The grammar walk skips quoted and literal operands, with literal counts parsed by the same marker parser used for framing and criteria validation.

Mailbox decoding keeps a lenient display string and the original wire octets plus
encoding mode in `MailboxName`. Typed mailbox command arguments replay those
octets when the connection uses the same mode, preserving raw 8-bit names,
noncanonical modified UTF-7 and malformed segments. Caller-created names use
canonical encoding. A mode change re-encodes only
canonical names; a noncanonical name is refused rather than reconstructed
from lossy display text.
Equality and hashing distinguish noncanonical wire identities even when their
display strings coincide, while canonical names compare equal to caller-created
names. NUL-bearing mailbox wire strings are refused rather than silently
stripped. Gmail FETCH labels use the negotiated mailbox decoding mode.

The account registry keys on `MailboxName`. Folder/container ids and the
folder component of object, thread and part ids retain noncanonical wire
identity in a `~imap-wire:` base64url envelope containing the mode and octets.
Escaped ids use canonical identity octets and mode, so equal names have equal
ids regardless of the LIST connection's encoding mode. Object ids use `imap2`
and thread ids use `imapthread2` for escaped folder identities; legacy `imap1`
and `imapthread1` folders always decode literally. Part handles likewise use
`imappart3`, with legacy `imappart2` folders decoded literally.
Search concatenates selectable folders in display-name order with canonical
wire octets and mode breaking ties, keeping offset pagination stable across
registry rebuilds.
Canonical validated names retain their existing string ids; literal names
beginning with the reserved prefix and control-bearing names are escaped too. Scope routing, grouping, command-response correlation, push admission
and error/throttle keys use this same identity. LIST-STATUS pairs duplicate
responses in arrival order only when their retained mailbox identities match.
Display names remain readable.
Mailbox command helpers accept typed names alongside validated caller text;
APPEND, LIST patterns and NOTIFY filters retain typed addresses through encoding.
Hierarchy edits preserve a noncanonical parent's wire prefix for ASCII
hierarchy delimiters; unsupported non-ASCII delimiter combinations are refused.

### One command path

Every IMAP command - APPEND and MULTIAPPEND included - is a `types::Command`, encoded by one encoder (`codec::encode::encode_command`) into one form (`WireCommand`), and written by one sender (`driver::wire_send::send_wire_command`). There is no second payload type, no second encoder and no second send path.

`WireCommand` is a list of segments, each a list of `Bytes` chunks. A segment ends exactly on a synchronizing literal marker; the sender writes every chunk of a segment, flushes once, and waits for `+` before the next segment (RFC 3501 Section 4.3). The boundaries are STRUCTURAL: `CommandWriter` records each one as it emits the marker, so no code rescans encoded bytes to rediscover framing, and marker-shaped bytes inside a literal payload cannot be mistaken for a boundary because nothing looks. `CommandWriter` is the one place literal framing is decided: `string(data, utf8)` makes the quoted-versus-literal choice (NUL is rejected when the whole command is finished, so mailbox and metadata identities are never rewritten; printable ASCII is quoted, plus valid UTF-8 in UTF-8 mode, with control characters and DEL always a literal), and `literal` / `literal_bytes` choose the `+` modifier once, from the live `EncodeOptions` - LITERAL+ any size, LITERAL- and rev2 up to 4096 octets (RFC 7888 Sections 4-5, RFC 9051 Section 4.3) - with the literal8 form (`~{n}`) additionally needing an advertised BINARY on a non-rev2 connection (RFC 7888 Section 6, RFC 9051 Section 9). What a command may CARRY stays with that command's encoder (APPEND's BINARY rule, METADATA's literal8 values); the writer owns marker choice and boundaries and nothing else. `literal_bytes` carries a payload by reference (a reference-count bump), which is how a message body reaches the socket uncopied. `WireCommand`'s `Debug` prints only its shape, and `AppendMessage`'s prints the body's length, so `?cmd` tracing cannot log a message or a credential literal.

Caller-written literals in SEARCH-family criteria are the one place literal syntax enters from outside the encoder. `validate_search_criteria_crlf` judges the caller's ORIGINAL markers first (a `{n+}` the connection cannot take is `MissingCapability`, never silently rewritten); the walk (`write_criteria`) then copies every non-marker byte and every payload verbatim, skipping each payload by its declared length, and re-emits only the marker through the writer - a `~{n}` as literal8, so the literal8 rule applies. Two spellings can therefore differ from the caller's: a count with leading zeros is written canonically, and a `~{n+}` the connection may not send non-synchronizing loses its `+` and becomes a real segment boundary.

Encoder refusals are the crate's own `Error` variants (`MissingCapability`, worded `<cmd> requires <cap>` for every capability gate; `InvalidInput`; `InvalidAppendDate`; ...), passed to the driver unchanged.

**Session legality** is a property of the command: `CommandKind::legal_states` is the one table (RFC 3501 Section 6 and each extension's section; ID, CAPABILITY, NOOP and LOGOUT in every live state, LOGIN / AUTHENTICATE / STARTTLS in NotAuthenticated, ENABLE in Authenticated only, IDLE and the mailbox, APPEND, METADATA, NOTIFY, QUOTA, ACL, COMPRESS and UNAUTHENTICATE commands in Authenticated or Selected, message commands in Selected; no command in Logout). It is read twice, and the two reads report different facts. The handle reads it to refuse a command the caller should not have issued (`require_state(kind)`, `InvalidState`; `Pipeline::execute_raw` checks every command of a batch at execution and `idle()` checks IDLE). The driver reads it again when the command reaches the head of its queue (`prepare_command`, and `run_pipeline` for every command of a batch before any byte), and a refusal there means the session moved while the command waited: `StateChangedBeforeSend`, or `Closed` with `Unsent` in Logout. The driver's own internal commands (connection setup's CAPABILITY, the STARTTLS and COMPRESS upgrades and their post-upgrade CAPABILITY) run under the stated invariant that their admission follows from state the driver owns; the check still runs for them. Session HISTORY is read beside the table at both sites: RFC 5161 Section 3.1 forbids ENABLE once any mailbox has been selected in the session, even after CLOSE or UNSELECT returns it to Authenticated. `ProtocolState::mailbox_selected` records that a SELECT or EXAMINE has succeeded (a refused one selects nothing), the snapshot publishes it, and `CommandKind::refused_after_selection` names the commands it gates (ENABLE only): the handle refuses with `InvalidState`, the driver with `StateChangedBeforeSend`. UNAUTHENTICATE starts a new session and clears it (RFC 8437 Section 2).

`run_one_command` is `prepare_command` then `run_prepared_command`. Preparation (legality, then encoding) writes nothing and arms nothing; only after it succeeds are the command's pending state effects armed (`in_auth`, `in_select`, `in_close`, `in_logout`, `in_unauthenticate`, the NOTIFY registration). A command refused before its first byte therefore cannot leave, say, a pending selection for the next command's tagged OK to complete. Every preparation refusal leaves the framing intact, and the driver loop does not apply `is_connection_fatal` to it.

**Transmission evidence for a failed write** comes from socket progress, not from which write failed. Every transport socket is wrapped at construction in `stream::Tracked`, which counts the octets the socket accepts; it sits BELOW TLS and below COMPRESS, because neither layer above reports its own partial progress (OpenSSL can push part of an encrypted record before its `poll_write` returns `Pending`, and a later poll can fail without ever having returned `Ok(n)`). The sender reads the counter when a command begins (for a coalesced pipeline batch, when the batch begins, never reset between its commands) and stamps any write or flush failure `Unsent` exactly when the counter has not moved, else `InFlight` (`write_failure_evidence`). A saturated counter reads as progress. TLS handshake and control traffic can only inflate the count, the conservative direction. The rule rests on two invariants: a failed write retires the connection, so nothing buffered above the socket is ever sent later, and COMPRESS drains its sync-flush output inside a successful write, so a new command cannot inherit undrained output. This is what lets a stale pooled connection's first-byte EPIPE stay `Unsent` (a plain retry) while a failure after any byte of a non-idempotent command reached the socket is `InFlight` (reconcile). Writes made inside an already-started exchange - IDLE's `DONE`, an AUTHENTICATE continuation reply - stay unconditionally `InFlight`, because their command is already on the wire.

ESEARCH `ALL` sets are normalized before expansion into the flat compatibility `SearchResult`. Parsed sequence bounds retain `*` as `SeqBound::Star`, distinct from the legal numeric UID `4294967295`; `*` is refused wherever it appears because its concrete value is unknowable from the response. Legal numeric maximum values can therefore be expanded normally. Legal overlap or repetition is sorted and merged, so both the 1e6 safety cap and the expanded ids count each UID once. An expansion that cannot be completed is `Error::SearchResultTruncated`, never a partial list presented as complete, and the solicited ESEARCH stays consumed by the command rather than being republished as an asynchronous event.

APPEND and MULTIAPPEND are `Command::Append { mailbox, messages, multi }`, encoded, validated and sent by the driver from live protocol state when the command reaches the head of the queue, exactly like every other command (see "One command path"). The handle (`ImapConnection::append` / `append_message` / `multi_append`) does only the client-side APPENDLIMIT check, then submits the command through `submit_regular` with `AppendConsumer` / `MultiAppendConsumer`; it makes no wire decision. That is the invariant: an earlier design built the bytes handle-side and queued them, which froze the mailbox encoding (modified UTF-7 versus UTF-8), the RFC 6855 `UTF8 (` wrapper, the RFC 7888 `+` markers and literal8 eligibility while the command sat behind another one. APPEND uses the driver's tag generator like every command. APPEND is not pipelinable (`CommandKind::pipelinable`): a batch does not run its APPENDLIMIT preflight, and the batch router does not hand an untagged `* OK [APPENDUID ...]` to an APPEND consumer that a head consumer claims first.

The APPEND arm of the encoder (`commands/append.rs`) validates against the live options: `MissingCapability("MULTIAPPEND")` when `multi` and the capability is absent, `InvalidInput` for an empty or wrongly-shaped message list, `InvalidAppendDate`, and flag-keyword errors; `\Recent` and `\*` are dropped from flags. The mailbox uses `opts.utf8_mode` (`UTF8=ACCEPT` or active rev2) like every other command. The body form is chosen WRAPPER FIRST: once `UTF8=ACCEPT` is enabled every message rides the RFC 6855 Section 4 `UTF8 (~{n}` ... `)` wrapper (active rev2 alone does not select it), and its literal8 payload is defined by RFC 6855 independently of the BINARY advertisement, so a NUL-bearing body inside the wrapper needs no BINARY. Outside the wrapper a NUL-bearing body takes `~{n}` and requires an advertised BINARY (RFC 3516 Section 4.4; active `IMAP4rev2` does not stand in, since rev2 folds in only BINARY's FETCH side), else `MissingCapability`. Every marker is the writer's choice: non-synchronizing per RFC 7888 (LITERAL+ any size, LITERAL- and rev2 up to 4096), and both literal8 forms additionally need BINARY and a non-rev2 connection.

Bodies are not copied. `AppendMessage::data` is `bytes::Bytes`, carried into the `WireCommand` by `literal_bytes` (a reference-count bump); only the small command syntax is allocated. A synchronizing literal ends its segment on the marker, so N synchronizing literals (bodies, or a non-quotable mailbox or date) give N+1 segments. `ImapConnection::append` borrows `&[u8]`, so it copies once into a `Bytes` to cross the channel; `append_message` takes an owned `AppendMessage` and copies nothing. The account layer's APPENDs (`draft_create`, and the Sent copy after `send_message`, `send_raw_message` and `draft_send`) all own their message and go through `append_message`. A failed write is stamped from socket progress like any other command's. Local refusals carry no transmission evidence of their own (a missing `Attempt` cause reads as `Unsent`), and `StateChangedBeforeSend` reports `Unsent` by construction; all are safe to re-issue because no byte has been written.

`AppendConsumer` and `MultiAppendConsumer` keep an untagged `* OK [APPENDUID ...]` whole, in arrival order in their buffer, remembering only its index. On success the tagged code wins and an unused untagged OK is surrendered as an event; otherwise the untagged OK is consumed as the answer and not republished. On failure everything, the untagged OK included, is surrendered.

A MULTIAPPEND's APPENDUID uid-set is believed only when it names exactly as many UIDs as messages were submitted (`MultiAppendConsumer::new(submitted)`), counted BEFORE anything is expanded, so `[APPENDUID 7 1:4294967295]` costs a subtraction, not four billion pairs. A range may be written in either direction (RFC 3501 Section 9) and expands ascending. A count that disagrees discards the code, as if the server had no UIDPLUS: the messages were appended, so an error would invite a retry that duplicates them. A single APPEND expands nothing and takes the first UID of the set, unchecked on purpose: `draft_create` reports a missing UID as `Unsupported`, which for a draft already appended would invite a duplicating retry.

`CopyConsumer` and `MoveConsumer` handle an untagged `* OK [COPYUID ...]` differently, because the tagged OK can carry a code of its own (under QRESYNC typically `HIGHESTMODSEQ`, RFC 7162) and a single slot let that code hide the COPYUID. `CopyResult` and `MoveResult` therefore carry two fields, filled by one shared `resolve_copyuid` on the success arm: `code` is the tagged code when it has one, else the retained untagged OK's code; `copy_uid` is the COPYUID wherever it came from (the tagged code when that is COPYUID, else the retained untagged OK), always a `ResponseCode::CopyUid` when present. The retained untagged OK is consumed exactly when it supplies `copy_uid`, i.e. whenever the tagged code is not itself COPYUID; when the tagged code is COPYUID the untagged one was not this command's answer and is surrendered at its wire position among the buffered responses. For MOVE an untagged COPYUID is the expected shape, not a quirk: RFC 6851 Section 4.3 asks a UIDPLUS server to send it in an untagged OK before the expunges, though some servers put it on the tagged OK instead. `MoveConsumer` retains only the first such OK, whole, with the length of its buffer at arrival; a second one is an ordinary untagged OK and is buffered. On a tagged NO the retained OK goes back into the buffer at its wire position (a COPYUID naming a move the server then failed is not this command's answer), and the EXPUNGE / VANISHED mutations are surrendered after the buffer, verbatim, as before.

On the UID MOVE fallback (COPY, STORE, UID EXPUNGE, for servers without MOVE) `MoveResult::code` mirrors the native path's tagged code: it is the final UID EXPUNGE's tagged code (under QRESYNC the source mailbox's new `HIGHESTMODSEQ`), falling back to the COPY's `code` only when the EXPUNGE carried none, and `copy_uid` is the COPY's. The EXPUNGE runs through `ExpungeConsumer::with_tagged_code` so the plain consumer's discarded tagged code is available; the plain `uid_expunge` is unchanged. Once the COPY is acknowledged, the STORE and UID EXPUNGE errors pass through an evidence floor (`after_acknowledged_side_effect` in `connection/uid_ops.rs`): evidence that reads `Unsent` (explicit, or absent on a variant that can carry it) is restamped `InFlight`. The fallback reports a later step's leaf evidence as the outcome of the whole MOVE, and the account layer settles `Unsent` as `Failed` (retryable), so without the floor a STORE withdrawn while queued, dropped unclaimed by a dying driver, refused at admission with its deadline gone, or failing its first write with the socket counter unmoved would let the engine replay the MOVE, repeat the acknowledged COPY and duplicate messages. `InFlight` sends it to read-back instead. Acknowledged evidence is left alone, and variants that cannot carry evidence (`StateChangedBeforeSend`, local refusals such as `MissingCapability`) pass through unchanged; those cases, and composite partial completion generally, need an aggregate-progress representation that leaf transmission evidence cannot express.

APPEND resolves APPENDLIMIT (RFC 7889) before writing anything: a numeric `APPENDLIMIT=<n>` capability is a global limit, a bare `APPENDLIMIT` sends a preflight `STATUS <mailbox> (APPENDLIMIT)`, and a server advertising both gets the most restrictive of the two. The STATUS reply is read from `items` *and* `ambiguous`, since NOTIFY STATUS makes the solicited reply wire-identical to a notification. Because that STATUS is a preflight, its failures are restamped `TransmissionState::Unsent` relative to the APPEND, as is a deadline that expires before submission: APPEND is non-idempotent, and leaking the helper's `InFlight` / `Acknowledged` evidence would send `DraftCreate` recovery down the reconcile path for a message no server ever saw.

## Connection state machine

The driver-owned command channel is the connection liveness signal.
`ImapConnection::is_alive()` reports whether the driver still owns the
receiver; this is separate from RFC session state, which can still say
`Selected` after a socket failure.

Driver-side I/O loss, BYE, parse failure, and protocol desynchronization
are connection-fatal. Before returning such an error, the driver closes
the command receiver and moves the session to `Logout`. The pool filters
dead parked members during checkout and never returns a dead checked-out
member to the idle list. Tagged NO/BAD responses and local validation or
capability errors leave the connection reusable.

A timed-out or cancelled command is a third case, decided by admission. Each
wire command (`connection/reply.rs`) is queued, claimed, or withdrawn: one
compare-exchange shared by the caller's `Pending` and the driver's `Reply`
picks exactly one winner, and the command's payload is reachable only by the
winner, so the driver can neither write nor answer a command it did not
claim. The driver claims at the head of its queue, before `prepare_command`.
The submit helpers own the deadline, because they are the only code that
knows the phase, and admit strictly: reserve a channel slot under the
deadline, re-check it, and only then send, so a deadline gone at or during
admission submits nothing and is `Timeout` / `Unsent`. A caller that leaves
before the driver claims its command withdraws it: it is never written, its
payload (an APPEND's bytes, a streaming consumer and with it the caller's
item sender) is released at once, the evidence is `Unsent`, and the
connection is not abandoned. Once claimed, the driver completes the exchange
(cancellation safety by socket ownership); a caller that leaves with no
answer in hand reports `InFlight` and marks the connection `abandoned`. An
answer already buffered when the deadline fires wins the tie. A reply
dropped by a dead driver is `Unsent` when never claimed and `InFlight` once
claimed, and does not mark the connection abandoned (it is already not
reusable). A withdrawn command's shell keeps its channel slot until the
driver dequeues it, so a channel full of withdrawn commands behind a stalled
head still blocks new admissions until their deadlines (each then
truthfully `Unsent`), while the connection reads reusable unless something
abandoned it.

`PooledConn::drop` parks only a member that `is_reusable()` (alive AND not
abandoned). Without this, the next checkout queues behind the still running
claimed command and times out in turn, so one slow command cascades into a
run of spurious timeouts on the same connection. `Pipeline` execution admits
the same way without a deadline: dropping it while it waits for channel
capacity submits nothing and does not abandon the connection, and dropping
it while queued withdraws the batch. IDLE follows the same arbiter: its
admission races capacity against the cancel token and the event deadline,
and every departure from its wait that would send DONE (an event, `Cancelled`,
`Timeout`) first withdraws, returning the departure's value with nothing
written when the IDLE was never claimed. A claimed IDLE must complete DONE; a
DONE handshake that times out, or an `idle()` future dropped after the claim,
leaves the connection abandoned, while token cancellation that completes
DONE leaves it reusable (push re-IDLEs on it).

An abandoned connection's LAST handle aborts its driver (`ImapConnection`'s
`Drop`). The driver claimed an exchange whose caller is gone; closing
the command channel cannot reach a driver blocked in that exchange, so
against a stalled peer it and its socket would otherwise outlive every
handle, unreachable by `close()`. A connection that is not abandoned keeps
the graceful path, the bounded best-effort LOGOUT. `abort` only requests
cancellation; `terminate().await` remains the guarantee `Account::close()`
relies on.

The driver's terminal best-effort LOGOUT (run when the last handle drops
and the command channel closes) is time-bounded (`LOGOUT_DRAIN_TIMEOUT`,
5s): it runs on a detached task, so an unbounded BYE/OK drain against a
stalled or half-open peer would leak the task and its socket for the process
lifetime, the same hazard the IDLE DONE handshake bounds. The pool retains a
join handle for each registered driver after its connection handle disappears.
`Pool::close` waits for that detached LOGOUT to finish, then aborts and joins
the driver if the bound expires. On timeout the transport is dropped without
the graceful exchange.

An untagged `* BYE` is fatal in every driver read loop except single-command
dispatch while servicing LOGOUT. RFC 3501 Section 6.1.3 requires that BYE
before LOGOUT's tagged OK, so dispatch passes it to `LogoutConsumer` and keeps
reading for the tagged completion. A missing BYE on tagged OK remains
`ProtocolMissing`; a tagged NO/BAD remains the server's refusal. BYE stays
fatal for APPEND, pipeline batches, IDLE's wait for its `+` grant, IDLE (and
its drain), literal continuation waits, and the terminal best-effort LOGOUT
drain. Regular and APPEND commands use one response loop parameterized by
continuation policy. Every untagged response still passes through one
prologue, `process_untagged_prefix`, which emits critical response-code events
before applying the BYE guard. The command kind is supplied by single-command
dispatch (including APPEND), which is the only path allowed to accept LOGOUT's
expected BYE.
That order keeps an `ALERT` carried on a fatal `* BYE` from being lost when
the error unwinds the command. `short_circuit_on_bye` is private to the driver
module and reachable only through that prologue, so no read loop can re-derive
the ordering and get it wrong. The loops that have no consumer to route to
(IDLE and its `+` grant wait, the post-DONE IDLE drain, the best-effort LOGOUT drain, and the
synchronizing-literal continuation wait *when no pipeline is in flight*) share
their entire untagged arm as `process_untagged_as_event`: prologue, then
forward the response as a typed event exactly when the prologue did not already
publish its critical code as one. They remain separate machines below that arm -
they disagree on termination, on the foreign-tag rule, and on whether a
continuation or greeting is fatal - so only the arm is shared. The loops that do
have a consumer (command dispatch, the pipeline batch, and the continuation wait
under a pipeline) call the prologue directly, because for them the forward is
conditional on classification. The guard is fed by
`ProtocolState::apply_side_effects`'s
`SideEffectDigest`. The digest is `#[must_use]` and its `had_bye` field is
private, so a new loop cannot silently ignore it. BYE is recognized from
the status tag alone, independently of the response code it carries, so
`* BYE [CAPABILITY ...]` cannot be routed away by the capability
side-effect arm. The error preserves the BYE text and structured response
code (`Error::bye_with_code`), which is what the account layer maps to a
specific `RecoveryClass`; response-code events (e.g. ALERT) are emitted
before the short-circuit fires. The prologue takes the `TransmissionState` of
the command the loop is serving and stamps it on the BYE error. Every read loop
reads only after writing, so each passes `InFlight`: a BYE does not complete the
active command, and an unstamped BYE read as `Unsent`, which let a non-idempotent
command the server may have executed be retried blind. Single-command dispatch
passes its kind to the prologue; only LOGOUT suppresses the short-circuit and
routes the BYE to its consumer. Other loops supply no such exception. The
best-effort LOGOUT drain states the phase too, though its errors are discarded
at teardown. Command results are published to the state watch channel *before*
the caller's oneshot is answered, so a caller that observes the result always
sees the matching snapshot.

A pipelined batch is admitted before anything happens to it: `run_pipeline`
refuses, over the whole original command vector and before grouping or any
byte, a batch holding a command whose `CommandKind::pipelinable` is false
(SELECT, EXAMINE, CLOSE, UNSELECT, LOGIN, AUTHENTICATE, LOGOUT, UNAUTHENTICATE,
STARTTLS, COMPRESS, IDLE, ENABLE, APPEND - an `Internal` error, since only a
crate bug can build one: the `Pipeline` builder has no method for them), a
command the live session does not permit (`live_state_refusal`, as for a single
command), and a command the encoder refuses. EVERY command, of every
sub-batch, is encoded before the first byte of the first sub-batch, so no
refusal can land after an earlier sub-batch has run. The batch is encoded and
admitted against one state snapshot, which holds because no pipelinable command
changes the session state or the encode options.

Sub-batches split on `CommandKind::untagged_routing_key`, not on the kind:
two commands share a sub-batch only when no untagged response can be solicited
by both, since head routing would hand one's answer to the other. Distinct
kinds share a key where `classify` makes a response `OnlySolicited` for both:
GETQUOTA / GETQUOTAROOT / SETQUOTA (`QUOTA`), the three search forms (tagless
`SEARCH`, uncorrelated `ESEARCH`), LIST / LIST-STATUS / STATUS (`LIST`,
`STATUS`), and FETCH / EXPUNGE (`VANISHED (EARLIER)`). A classifier test checks
every pair of pipelinable kinds with distinct keys against `classify`, so a new
shared row fails until the key groups it.

The send phase follows the encoded segments, never the negotiated literal
mode. When no command of the batch has a segment boundary, the batch is one
coalesced write, flushed once; otherwise `run_pipeline_batch` sends command by
command through `send_wire_command`, so a synchronizing literal (no literal
extension, a LITERAL- literal past 4096 octets, a literal8 the connection may
not send non-synchronizing) is always granted before its payload.

The batch's result is per command, in three lanes (`PipelineEnd`). A refusal
before the first byte is the whole batch's error (`PipelineError::Driver`) and
nothing ran. Once the batch is on the wire, a failure that ends it (a lost
connection, a BYE, a desynchronized response) never replaces a result the
server already gave: a command that was answered keeps its result (completed),
one that may have reached the server but was not answered carries the failure
stamped `InFlight` (outstanding), and one of which no byte was written carries
it stamped `Unsent` (unsent). Which commands may have been written is judged
from the socket counter: every command of an earlier sub-batch, every command
of a coalesced write that moved the counter at all, and in a command-by-command
send every command before the failing one, plus that one if the counter moved
since it began. The failure's own error is stamped against the counter read
once before the first sub-batch. A batch that ended early retires the
connection whatever the failure was, since commands may be left on the wire
whose responses nobody will read. The stamps ride `Error::with_attempt`, a
no-op for `Protocol` and `Parse`, so such a slot reads as `Unsent` in either
lane; both are provider-contract violations whose recovery does not depend on
it.

A pipelined batch reads from the socket in two places, not one, and both use
the same router. Whenever it is sent command by command, commands already on
the wire have tags still pending and their responses arrive during a later
command's continuation wait. The routing tables are therefore built before the
send phase, and `wait_for_continuation` takes an optional `PipelineRouting`:
an earlier command's tagged response is its own completion, an untagged
response solicited by an earlier command reaches that command's consumer, and
side effects are applied exactly once at read time. Two rules govern the
command currently negotiating its literal. It is the EXCLUSIVE upper bound on
untagged ownership - the server has not received it, so it cannot have executed
it, so no solicited response for it can exist and neither it nor any later
command is an eligible owner. And its own tagged `NO`/`BAD` is its ordinary
per-command result rather than a batch error: the sender abandons the rest of
that command's bytes (the server has ended its parsing state, so the remainder
would read as a new command) and the batch continues. A tagged `OK` there is
the same non-fatal `ProtocolMissing` as outside a pipeline, recorded in that
command's result slot (the consumer is discarded, never finalized, because
finalizing would report success for a command that never ran; its pre-effects
such as a NOTIFY registration are skipped) and the remainder abandoned like a
`NO`. It is safe because the send phase is sequential while a literal is
synchronizing: the only batch bytes on the wire are earlier commands (complete)
and this command's first line, and no later command has been written for the
server to read as the missing literal. This is what keeps a `NO` for command #1 from being
reported as a failure of command #2, and what stops a `* MYRIGHTS` solicited by
command #1 from being downgraded to an anonymous event while the batch
completes with a silently short result.

Outside a pipeline the continuation wait is told its command's own tag (`own_tag`, threaded through the sender), because exactly one command is on the wire and the wait can therefore separate three tagged cases. The command's own `NO`/`BAD` is an ordinary refusal, `Acknowledged`, and the unsent remainder is never written. Its own `OK` with no `+` first is the server completing the command without granting the literal it owed: the exchange is over and the framing intact, so it is the non-fatal `ProtocolMissing`, and the remainder is likewise never written (it would be read as a new command line). The command did not execute, so the effects armed for it before the send (`in_auth`, `in_select`, the NOTIFY registration) are disarmed (`ProtocolState::disarm_pending_effects`) before that `OK` is applied; its response codes still apply. Without this a LOGIN whose credential literal never reached the server moved the session to Authenticated. A `NO`/`BAD` keeps the effects, since a refused SELECT legitimately deselects. Any other tag can belong to nothing, so it is a desynchronization, `Protocol`, connection-fatal. Under a pipeline the router owns tag interpretation but reaches the same verdicts (`early_ok_error` is the one shared definition of the own-tag `OK` error). One accepted limit applies to both modes, for a tagged reply of any status at a LATER literal of a multi-literal command (a MULTIAPPEND's second message): the inter-literal text has already been written, but the sender ends every segment on its synchronizing marker before waiting, and a conforming server answers only at a marker (RFC 3502 permits a `NO` there), so it has read everything sent and the wire is in sync, as for the first literal. Only a server replying straight after a literal body with no marker would desync it; that is not made fatal, because doing so would drop the connection on the legitimate mid-MULTIAPPEND `NO`. Transcript tests pin the conforming case: `a_later_multiappend_message_refused_at_its_marker_leaves_the_wire_in_sync` for MULTIAPPEND; `a_later_literal_refused_at_its_marker_leaves_the_wire_in_sync` and `a_later_literal_over_the_literal_minus_limit_refused_at_its_marker_leaves_the_wire_in_sync` (a two-literal SETMETADATA, with no literal extension and under LITERAL- with literals above 4096 octets) for single-command dispatch; and `a_pipelined_command_refused_at_its_second_marker_is_its_own_result_without_a_literal_extension` and `..._over_the_literal_minus_limit` for the same command inside a pipelined batch, where the refused slot holds the `NO`, the neighbours complete, and none of the second body reaches the wire. IDLE does not use this wait: `wait_for_idle_grant` applies the same three rules to IDLE's `+` (own `NO`/`BAD` refuse IDLE, own `OK` without `+` is `ProtocolMissing`, a foreign tag is `Protocol`), with no literal to abandon, and stamps transport failures `InFlight` because the IDLE command is already on the wire.

Untagged ownership normally goes to the head consumer (the first still-active
command eligible to own the response), with one exception taken first. An
ESEARCH carrying a search-correlator names its command outright (RFC 4466,
RFC 4731 Section 3.1), so `correlated_esearch_owner` routes it to that command
when the tag is one of this batch's commands and `classify` makes ESEARCH
solicited for its kind, and then decides by that command's state: still active
and below the untagged-ownership bound, deliver to it; already finalized, drop
the ESEARCH as surplus solicited data for a completed command (the same answer
`SearchConsumer` gives surplus SEARCH/ESEARCH), since `classify` makes ESEARCH
`Impossible` as an asynchronous event and the correlator says outright whose it
is. Head routing alone handed a
second pipelined search's ESEARCH to the first search's consumer, which
buffers a foreign-tagged ESEARCH and surrenders it as an event, so the second
search finalized on its tagged OK with no result: a spurious "OK but no
ESEARCH" omission error for a server that merely interleaved two pipelined
searches. Anything that fails a condition (a tagless ESEARCH, a tag from
outside the batch, a tag naming a not-yet-received command, a tag naming a
non-search command) takes the ordinary head-consumer path unchanged.

The codec draws the line that decides which of those two buckets a bad
response lands in. A response opening with a keyword the codec claims to
parse cannot degrade to `UntaggedResponse::Unknown`: it becomes
`nom::Err::Failure`, which the connection surfaces as `Error::Parse` and
the account boundary maps to `Protocol(ParseFailed)` /
`ProviderContractViolation`. Inside FETCH the gate is per attribute
(`codec/decode/envelope_fetch.rs::has_closed_grammar`): attributes with a
closed grammar (`UID`, `FLAGS`, `RFC822.*`, `INTERNALDATE`, `MODSEQ`,
`SAVEDATE`, `PREVIEW`, `EMAILID`, `THREADID`, `X-GM-*`, the sectioned
`BODY[...]` / `BINARY[...]` / `BINARY.SIZE[...]` forms) hard-fail.
`ENVELOPE`, `BODYSTRUCTURE`, and bare `BODY` additionally gate their safe
fixed prefixes (ENVELOPE's ten fields; single-part and multipart body
prefixes, applied recursively to every multipart child up to the typed
decoder's depth cap) *and* the balanced close of the outer structure, since
an extension tail is open in its contents but never in its framing. They
retain tolerance for the contents of those tails at every nesting level;
only fixed-arity violations hard-fail. Any entirely unmodelled attribute
also stays on the tolerant skip path: a failure there is at least as likely to
be a modelling gap of ours, and parse failure is connection-fatal. Separator
handling is uniformly multi-space for the same reason.

The strict lane must therefore never fire on a shape a real server sends.
`X-GM-LABELS` is the worked example: Gmail sends system labels as bare
backslash-prefixed atoms (`(\Inbox \Sent Important "Muy Importante")`), which
`astring` cannot express, so the label grammar is `"\" atom / astring` and
`X-GM-MSGID` / `X-GM-THRID` accept the quoted decimal some proxies emit.
Labels are decoded into `FetchResponse::gmail_labels` (modified UTF-7 decoded
for astring labels, system labels verbatim); requesting them needs
`X-GM-EXT-1`, and they count toward the buffered-FETCH byte estimate. The
generic IMAP account layer assigns them no folder semantics.

From the claim onward, cancellation safety comes from socket ownership:
dropping a caller's future does not cancel the wire exchange. The driver
completes the command and preserves framing before accepting the next
command. Before the claim nothing of the command exists on the wire, so a
departing caller withdraws it instead (see "Connection state machine").

## Typed events

`TypedEvent` carries every unsolicited response (EXISTS, EXPUNGE, FETCH, FLAGS, STATUS, METADATA, NOTIFY-routed events). `drain_events()` and `next_event()` give callers ordered access.

`TypedEvent::impact()` returns `EventImpact` for cache-coherence: `SelectedMailboxChanged`, `SelectedMailboxResync`, `MailboxStateChanged`, `ServerMetadataChanged`, `Heartbeat`, `Extension`, `None`. RECENT maps to `None` (informational; deprecated by IMAP4rev2). ServerMetadataChange is server-scoped, not selected-mailbox scoped.

`DriverEventSink` never waits on the event receiver. It buffers critical
events up to 4096 by default; requeryable and resyncable events are dropped
under backpressure. Every backpressure drop while the receiver is live,
including a critical event rejected at the buffer cap, contributes to a
`QueueOverflow` marker. The marker follows older pending critical events. Once
a loss is pending, later events join that loss window and are counted until the
marker is delivered, so neither a newer event nor the marker can overtake the
event sequence it summarizes.

## Streaming FETCH

Streaming never drops responses. The driver-side consumer is a bounded pipe (`BoundedStreamingPipe`, default capacity 64) that pre-reserves an `OwnedPermit` *before* each socket read, so a slow consumer applies TCP backpressure instead of either buffering without limit or silently discarding under a failed `try_send`.

The unit of delivery and backpressure is a complete `FetchResponse`, not a
literal chunk. `WireReader::read_one_limited` waits for
`response_frame_len_limited` to find the response's final CRLF before invoking
the complete-mode parser. `dispatch_response_loop` then applies side effects
and routes that response to the consumer. `account::blob::run_fetch` splits
that completed response into body sections; `open_raw_rfc822` therefore also
buffers a whole response before emitting any body bytes. A bounded channel
limits response count, not the memory or latency of one response.

Incremental body delivery requires a separate chunk-aware decoding and
dispatch contract that preserves UID/section identity, response side-effect
ordering, tagged completion failures, and framing when a receiver disappears
mid-literal. Removing the framing pre-check alone cannot provide it: the nom
parser builds an owned response and treats truncated input as a parse error.
Likewise, standalone borrowed-input parsers must copy body octets to return
owned `Bytes`; callers with owned backing already have `parse_response_bytes`.
Generic string fields still decode into owned values.

The literal budget deliberately measures declared payload octets. Charging
syntax against that same number would change its meaning, including refusing
an exact-budget body because of its FETCH wrapper. There is currently no
separate syntax or parser-allocation ceiling: long quoted strings, repeated
attributes and structured metadata can allocate outside the literal budget.
The buffered consumer's estimate is applied after parsing and is not a bound
on those allocations. A total-memory guarantee needs independent framing and
parser limits as well as accounting for retained backing capacity.

Three entry points:

- `uid_fetch_streaming()` / `fetch_streaming()` - caller supplies an unbounded sender; the bounded receiver is drained onto it internally.
- `uid_fetch_each(...)` - callback-based wrapper over the same bounded receiver.
- `uid_fetch_limited(budget, ...)` - retained-object estimate enforced inside the driver consumer; returns `Error::FetchLimit` when crossed after parsing and drains to tagged OK. Before each response, the consumer also passes its remaining budget to the reader: the sum of declared literal lengths in that response cannot exceed it. A larger declaration is `Error::FetchLiteralLimit` and retires the connection without waiting for the payload. Reads can prefetch up to one 8 KiB chunk before a marker is inspected. This literal ceiling does not bound non-literal syntax or parser object overhead.

Each shared FETCH literal pins at most eight times its payload length in backing capacity: its frame is at most four times its length, and the allocation at most twice the frame length. Smaller or disproportionate sections own compact copies. Small responses reuse the read allocation while retained outputs remain alive; large shared responses transfer ownership and require a replacement read allocation. The consumer estimate still counts payload lengths and a fixed response allowance, so it is not an exact allocator-memory ceiling. The declared-literal ceiling and shared measured-inbound counter are unchanged.

**Early consumer stop.** When a callback returns an error (or the caller's own sender is gone), the shared drain helper *drops* the bounded receiver rather than closing and draining it. The distinction is load-bearing: with an `OwnedPermit` outstanding, `recv()` on a merely closed receiver stays pending forever, so a server that stalls mid-response would outlive the command timeout and hang the joined future. Dropping makes the driver's next `reserve_owned` fail; it marks the pipe drained and keeps reading through the tagged completion so IMAP framing stays synchronized. Items already buffered when the consumer stopped are discarded, and the callback's error is the returned error.

**Termination is decided by the future, never by the channel.** A caller
driving a `(fetch_rx, fetch_fut)` pair through `tokio::select!` sees the
item receiver close *before* the future resolves - the driver drops the
consumer synchronously and only then answers the oneshot - so the two
branches are both ready and the choice between them is random. Every such
loop (`run_inventory`, the CONDSTORE changes loop) therefore awaits `fetch_fut` on the
`recv() -> None` arm and propagates its result unconditionally. Reading
whatever `select!` happened to have stored would let a tagged NO, a read
error, or a timeout break on the success path and checkpoint a truncated
mailbox as complete.

Buffered `uid_fetch()` uses the driver buffer path directly, not the streaming channel. `uid_fetch_full_messages(budget, ...)` requires an explicit budget and routes through the limited path.

## Capability and ENABLE state

`ServerProfile` answers capability questions without ad-hoc CAPABILITY parsing, built from `state_rx.borrow()` at call time. **Snapshot semantics**: no auto-refresh - refetch after STARTTLS, AUTH, or ENABLE.

"Is capability X usable on this connection?" has ONE authority, `types/profile.rs::supports(capabilities, enabled, capability)`, layered on the rev2-ACTIVE authority `imap4rev2_active`. X is usable when it is advertised, or it is CONDSTORE and QRESYNC is advertised (RFC 7162 Section 3.2.3), or rev2 is active and the RFC 9051 baseline folds it in: ESEARCH, IDLE, MOVE, NAMESPACE, SASL-IR, SEARCHRES, UIDPLUS, UNSELECT, LIST-EXTENDED, LIST-STATUS, STATUS=SIZE, STATUS=DELETED, LITERAL-, SPECIAL-USE, ENABLE. OBJECTID and SAVEDATE are deliberately NOT in the baseline (RFC 9051 defines none of their FETCH items, SEARCH keys, STATUS items or response codes, so a rev2 server that does not advertise them owes none of it), and neither is the BINARY token: rev2 folds in only the FETCH half of RFC 3516 (RFC 9051 Appendix B), not its APPEND extension, so answering the token from rev2 would let an APPEND carry a NUL-bearing `literal8` body a rev2 server may reject. The FETCH half has its own question, `binary_fetch_usable` (BINARY advertised, or rev2 active), which `validate_requested_fetch_items` asks for `BINARY[...]` / `BINARY.PEEK[...]` / `BINARY.SIZE[...]`; `EMAILID` / `THREADID` and `SAVEDATE` FETCH items still require their own advertised capability on rev2. LITERAL+ is deliberately NOT in the baseline: RFC 9051 Appendix E folds in LITERAL- only, so a pure rev2 server takes non-synchronizing literals up to 4096 octets (RFC 9051 Section 4.3) and unbounded ones only when it also advertises LITERAL+ (RFC 7888 Section 4); listing it would tell every caller that such a server accepts a `{N+}` literal of any size. That baseline list is a private function next to `supports` and is the only copy; extensions outside it (CONDSTORE, QRESYNC, SORT, WITHIN, PREVIEW, NOTIFY, MULTIAPPEND, LITERAL+, ...) get no rev2 clause. Every consumer is a thin view over it: `ServerProfile::supports`, `EncodeOptions::has_capability` / `has_condstore` (encoder command admission, including APPEND's MULTIAPPEND and BINARY checks), the connection-handle gates through `connection/auth.rs::snapshot_supports`, and the driver's literal negotiation (`driver::literal_mode` asks the authority for LITERAL+ first, then LITERAL-, else synchronizing, so pure rev2 lands on the 4096-octet LITERAL- behaviour). New gates call one of those views, never `capabilities.contains(..) || rev2`. Checks that deliberately ask something else are the exception and say why at the call site: the non-synchronizing `literal8` form needs an advertised BINARY (which `supports` already answers advertised-only) and additionally a non-rev2 connection (RFC 9051 Section 9 makes rev2 `literal8` synchronizing), and the STATUS `DELETED` item is gated on active rev2 or `QUOTA=RES-MESSAGE` rather than on `supports(StatusDeleted)`. The two agree under rev2 and differ only on rev1, where the authority would also accept an advertised `STATUS=DELETED` token; no RFC defines that token (RFC 9051 Section 6.3.11 makes DELETED a base rev2 item, and the only rev1 route is RFC 9208 Section 4.1.4's `QUOTA=RES-MESSAGE`), so the gate names exactly those two sources. Tests: a test-only oracle in `profile.rs::capability_matrix` classifies every `Capability` variant with an exhaustive match (a new variant does not compile until classified), and matrix tests check the authority, `ServerProfile`, `EncodeOptions` and `snapshot_supports` against it across every rev2-ACTIVE state.

## Auth

`Credentials` is a public opaque wrapper with `password(username, password)`,
`oauth2(identity, access_token)` (raw string), and
`oauth2_source(identity, Arc<dyn TokenSource>)` (shared rotation source)
constructors. The OAuth variant holds an `Arc<dyn TokenSource>` (bifrost-net's
trait), read via `current().await` at every connect/reconnect (in
`authenticate_best` and the ManageSieve auth path), so a rotated token is
presented fresh on reconnect. Password material stays in the internal
`SecretString` wrapper (zeroizes + redacts under `Debug`). `Credentials` is
`Clone` only (no `PartialEq`/`Eq` - a live token source is not `Eq`), no
`From<(String, String)>`.

Internal `AuthMechanism`: PLAIN, LOGIN, XOAUTH2, OAUTHBEARER, CRAM-MD5, SCRAM-SHA-1, SCRAM-SHA-256, and the channel-bound SCRAM-SHA-1-PLUS / SCRAM-SHA-256-PLUS. Each variant's `name()` is the wire token; the PLUS variants match the `-PLUS` advertisement and the SASL crate (`ScramHash::mechanism_name`) is the single authority for the token emitted on the socket.

SCRAM and CRAM-MD5 computation (`scram_client_final` / `verify_server_final`
transitions, per-hash proofs, CRAM-MD5 response) lives in the private
`bifrost-sasl` crate; the dispatch consumers (`AuthenticateScramConsumer`, `AuthenticateCramMd5Consumer`) drive the `+` continuation flow and map `bifrost_sasl::SaslError` into the IMAP error model at the boundary.

If a continuation consumer returns a non-fatal error, dispatch answers the
outstanding SASL challenge with `*` and reads through the command's tagged
`NO` before returning its failure. If the continuation error is an auth
error, a structured authentication code from the tagged response takes
precedence when present; otherwise the error from the SASL continuation is
preserved. The tagged completion closes the exchange, so an authentication
refusal can keep its auth error class without leaving the next command to be
parsed as a SASL response. Errors already
classified as connection-fatal retire the stream immediately; another
continuation or a non-`NO` completion after cancellation is a protocol error.
`Error::Auth` carries optional attempt evidence like tagged NO/BAD errors.
The driver records the acknowledged cancellation phase without losing the auth
class, mechanism or response code, and account translation preserves it.

`AuthPolicy` TLS-gates cleartext mechanisms by default: PLAIN/LOGIN refuse over plaintext unless `allow_cleartext_without_tls` is set. CRAM-MD5 is opt-in (`with_cram_md5`) AND TLS-gated (a MITM can pick the challenge and brute-force `HMAC-MD5(password, challenge)` offline). LOGIN-the-IMAP-command is opt-in (`with_login`).

`authenticate_best(credentials, policy)` intersects server-advertised, policy-allowed, and credentials-supported mechanisms, then runs the strongest. SASL-IR is used when advertised or implied by IMAP4rev2; malformed mechanism names are rejected before any wire write. `AuthOutcome` carries the selected mechanism.

The password ladder is a pure helper `password_mechanism_ladder(profile, policy, is_encrypted)` returning `PasswordCandidate::{Attempt, Reject}` in the fixed order SCRAM-SHA-256-PLUS > SCRAM-SHA-1-PLUS > SCRAM-SHA-256 > SCRAM-SHA-1 > PLAIN > CRAM-MD5 > LOGIN. RFC 5802 Section 6 downgrade protection: when the server advertises `SCRAM-SHA-N-PLUS`, the matching unbound `SCRAM-SHA-N` rung becomes `Reject(ChannelBindingUnavailable)` so a MITM cannot strip the binding. For a PLUS `Attempt`, `authenticate_best` resolves the `tls-server-end-point` binding once via `resolve_scram_binding()` (peer-cert DER + `bifrost_sasl::tls_server_end_point`), which delegates the policy to the pure `scram_binding_from_der(Option<&[u8]>)`. Only the *absence* of a peer certificate (plaintext, dead driver, no DER from native-tls) is a `ChannelBindingUnavailable` rejection that lets the ladder continue. A certificate that is present but whose binding cannot be computed (EdDSA leaf, unsupported sig-alg, malformed DER) is a hard `Error::Protocol` that aborts the ladder. Likewise, a server rejection of an attempted mechanism is terminal; only `MissingCapability` from live capability skew falls through to a weaker rung, because retrying after an explicit rejection risks a downgrade and another credential attempt. SMTP's `resolve_scram_binding` applies the identical channel-binding rule; the two crates must not diverge here.

## Typed IDs and sets

Newtypes with explicit `::new` constructors. No `From<u32>`/`From<u64>` to prevent accidental mixing across UID, sequence, message-id, and thread-id contexts:

- `Uid`, `Seq` - `NonZeroU32`-backed.
- `UidValidity` - `NonZeroU32`.
- `ModSeq` - `u64` (0 means "no CONDSTORE state").
- `GmailMessageId`, `GmailThreadId` - `u64`.

`UidSet` and `SeqSet` wrap the validated sequence-set encoder (`from_uids`, `from_seqs`, `all()`, `saved_search()` (`$`), or range constructors). `parse(&str)` is an escape hatch bypassing the typed discipline. Parsed `UidRange` bounds expose `SeqBound::{Number, Star}` so the legal numeric maximum is never confused with `*`; numeric fields remain the compatibility projection for existing consumers.

The account layer's string-typed object/thread ids (`imap2:` / `imapthread2:`, with literal-folder v1 decoding)
enforce the same `nz-number` rule at the decode boundary:
`decode_object_id` / `decode_thread_id` reject a 0 uid or uidvalidity as
`Request(Malformed)`. The crate never mints a 0, and accepting one used to
leak past `uid_set_from_u32`'s silent zero-filtering into fabricated
`Succeeded(Applied)` mutation outcomes, ids with no outcome at all, and an
empty-but-successful `open_raw_rfc822` stream.

Every locally-malformed `AccountError` the account layer mints carries the
operation of the call that produced it. `envelope::malformed` (id decode) and
`pim::pim_malformed` (mailbox-name validation, UIDVALIDITY drift, operand
refusal, search-criteria assembly, page-cursor parsing) both take an
`AccountOperation`, and the private helpers between them and their public
entry points thread it down rather than guessing: `decode_object_id`,
`decode_thread_id`, `decoded_targets`, `pim_operand`, `valid_uids`,
`folder_from_container`, `search_plan` and its criteria combinators,
`page_from_items`, `child_name`, `renamed_sibling`,
`parse_draft_for_submission`. The recovery class is `ClientBug` on all of
them and does not depend on the operation; what the threading buys is
telemetry that attributes a client bug to the lane that hit it. It also
removes three hardcodings that were outright wrong: `merge_folder`,
`or_folder_restriction_error`, and `criteria_from_filter`'s unsupported-filter
arm stamped `SearchMessages` (or `Search`) regardless of which of the two
search surfaces was calling, and `child_name`'s missing-delimiter refusal
stamped `ContainerCreate` even when reached from `container_move`.
`envelope::schema_incompatible` keeps its fixed `EstablishCursor`: a cursor
that will not decode is a cursor-establishment fault whichever call noticed.

### One outcome per id (`account/targets.rs`)

The one-outcome-per-id contract the sync engine holds is enforced by a type,
not by convention. `TargetBatch` is the single owner of the derivation
"requested ids -> wire UID operand -> outcome attribution", and the four paths
that used to hand-roll it - flag mutation and its two-sided patch group,
destroy, move, and hydration in `get.rs` - all route through it:

- `TargetBatch::new(ids)` splits the decoded ids into the ids the operand
  carries and an explicit *excluded* lane for ids it cannot represent (the
  only such UID is 0). The operand is built from the target ids and nothing
  else; there is no constructor that takes UIDs from anywhere but the batch.
- `subset_uid_set` is the only way to build a follow-up operand (the applied
  subset of a guarded STORE, the UIDs to EXPUNGE after a `\Deleted` mark) and
  refuses any UID the batch was not given, so a second wire command can never
  name a message the first was not accountable for.
- `settle` is the only way to mint the batch's outcomes, and exactly-once is
  a property of the types rather than of an assertion. `settle` consumes the
  batch, mints the excluded lane itself as `Failed(Request(Malformed))` - a
  UID the server never saw may become neither a fabricated success nor a
  silent drop - and then *drives the classification itself*: it walks its own
  targets and calls the caller once per target with a `Target`, returning the
  `Sealed` outcome that call produced.
  - `Target` is a non-`Clone` permit carrying the id and the `BatchItemId`
    stamped from it. It is consumed by `Target::seal(Verdict)`, which is the
    only constructor of `Sealed`.
  - So a classifier cannot answer twice for an id (the permit has moved),
    cannot skip one (it owes a `Sealed` per call and has no other way to make
    one), and cannot stamp an outcome with an id the batch did not give it
    (the batch stamps it; `Verdict` is the id-free half - `Succeeded(T)`,
    `Failed(error)`, `Uncertain(error)`).
  - There is therefore no length check, no uid-keyed token lookup that could
    fail at runtime, and nothing a release build skips. A settle that drops or
    duplicates an id does not compile, which is why no test asserts it: the
    testable residue is that the id on the outcome is the batch's own.
  - `settle_unsent` is the degenerate case: `UidSet::from_uids` returns `None`
    only for an empty input, so `uid_set()` is `None` exactly when the target
    lane is empty, and the path that sends no command needs no classifier.
  - `settle_streaming` is the hydration variant for a path that publishes down
    a channel: it hands back the target permits and the already-minted excluded
    lane. Each permit still seals at most once; what this lane gives up is
    total coverage, which it must, because the caller may abandon the walk (a
    dropped channel, a failed FETCH) and leave the rest to its own unresolved
    set.
- The one property Rust cannot express is that a `TargetBatch` must be consumed
  at all - a value can always be dropped. That single residue is still a debug
  assertion in `Drop`: a batch dropped with either lane non-empty panics in
  debug builds, so an early return that leaves an id unaccounted for cannot
  pass unnoticed. It has its own biting test.

The classification each caller supplies is a per-UID decision against the
server's answer, which is what made the shape fit: `mutation_verdict`,
`patch_mutation_verdict`, and `mutation_error_verdict` in `mutate.rs` each map
one UID to one `Verdict`, and the folder-level side effects (MODSEQ-cache
clears) are resolved before the settle rather than inside it.

Underneath it, `UidOperand` is the single constructor for a wire UID operand:
`UidOperand::build(uids)` keeps the UIDs a `UidSet` can carry, records the ones
it cannot in `excluded()`, and is what `TargetBatch::new` and `subset_uid_set`
build from. It exists so the lanes that owe no per-id outcome can share one
definition of "what the operand carries" instead of re-deriving it:

- The PIM primitives (`copy_messages`, `delete_messages`, `set_flag`,
  `hydrate_decoded`, and the draft `fetch_full_message`) answer
  `Result<(), AccountError>` for the whole request, so they have nowhere to
  report a per-id exclusion. `pim_operand` therefore refuses the command with
  `Request(Malformed)` when `excluded()` is non-empty, rather than sending a
  narrower operand and returning `Ok(())` for messages the server never saw.
  An empty group is not an exclusion and still skips its command.
- The streaming body read (`blob.rs::run_fetch`, behind `open_raw_rfc822`;
  `open_blob` itself is unsupported) refuses the same way instead of skipping the FETCH and
  ending the stream with `Done`, which presented "nothing was asked" as "the
  message has no body".

Both refusals are unreachable through the public API today, since
`decode_object_id` rejects UID 0 - which is the point: the invariant belongs to
the operand, not to the decoder, and neither boundary depends on the other.

This is the same move `StoreConsumer::new(unchanged_since)` and
`SideEffectDigest` made for their classes: the accounting hole that the
uid-0 decode bug's blast radius rode on is now unrepresentable rather than
merely absent.

Account cursors use `CompactUidSet`, a sorted, disjoint range list with
range-native construction, membership, insertion, removal, and linear diff.
QRESYNC mutates its live snapshot in that representation. VANISHED removal
subtracts ranges directly and emits only the intersection with known UIDs;
MODSEQ eviction checks cached keys rather than expanding server ranges.
`CompactUidSet::diff` subtracts sorted ranges directly, so an unchanged large
span is skipped without visiting each UID; only changed UIDs are expanded into
the required output vectors. Because `UidRange::range(n, n)` and
`UidRange::single(n)` are distinct values and `CompactUidSet` compares by its
range vector, every construction and mutation path normalizes a one-element run
to `single`. Two sets holding the same UIDs must compare equal and encode to
the same cursor payload no matter which path built them.

## Configuration and connect

`ImapConfig` centralizes TLS mode (Implicit / StartTls / Plaintext), connect/command timeouts, keepalive, and optional native-tls connector. Ports default per mode (993 / 143 / 143). Public constructors are `tls`, `starttls`, `plaintext`; builders cover port, timeouts, keepalive disablement, and custom TLS connectors. `ImapAccountConfig::pool_cap` caps physical IMAP sessions, clamped to at least one, from connector start through driver exit, including push, replacement dials and LOGOUT tails. `idle_connection_budget` is the non-NOTIFY push ceiling (see its field doc for the default), carved out of that cap. With `headroom = pool_cap.max(1) - 1`, push capacity is `min(idle_connection_budget.max(1), headroom)` without NOTIFY and `min(1, headroom)` with it. The last session stays available to data; `pool_cap = 1` disables push and advertises `PushCapability::None`. SMTP submission, DAV and ManageSieve have separate limits. The raw `connect` / `connect_authenticated` entry points were deleted in S1-W3.

Internally, `connect_authenticated_metered(credentials, policy, meter, budget)`
composes connect + STARTTLS-if-needed + `authenticate_best` for the factory,
returning `(ImapConnection, AuthOutcome)` crate-only. Pool dials register the
connection between connect and authentication, then initialize QRESYNC.
Direct connection setup guards the post-spawn keepalive command as in-flight;
cancellation there marks the connection abandoned so dropping its last handle
aborts the driver instead of detaching a command that may still be queued.

Connect-time STARTTLS requires both an unauthenticated greeting and an
advertised `STARTTLS`. A cleartext PREAUTH greeting is refused as
`StartTlsUnavailable` before sending a command, as RFC 9051 requires closing
an already authenticated cleartext session. The error maps to
`Authorization(PolicyBlocked)` because the configured TLS requirement cannot
be met either when the capability is absent or when the greeting is PREAUTH.
Its account-boundary diagnostic says the required STARTTLS upgrade is
unavailable in this session, covering both cases. At runtime, `is_encrypted()` is
updated when the TLS stream is installed. If the required post-upgrade
CAPABILITY exchange fails, the driver retires the connection instead of
returning a reusable half-upgraded session, while preserving the exchange's
error for the caller.

## Sync helpers

`SyncSelectOptions` / `SyncSelectResult` wrap SELECT/EXAMINE with CONDSTORE and QRESYNC parameters (UIDVALIDITY, last-known MODSEQ, known UIDs, VANISHED parsed off the SELECT response).

`SyncFetchRequest` / `SyncFetchResult` wrap UID FETCH with `CHANGEDSINCE` and `VANISHED`; `sync_fetch()` surfaces both VANISHED ranges and per-UID FETCH responses.

`SelectedMailbox` carries `highest_mod_seq`, `uid_validity`, `uid_next`, `no_mod_seq`, and the SELECT-side VANISHED list. A missing `UIDVALIDITY` is a protocol error rather than a silent 0: the account layer judges it through one helper, `selected_uidvalidity`, at every site (sync lanes, PIM primitives, search, raw-body read), as `Error::ProtocolMissing` (`Protocol(MissingField)`, `ProviderContractViolation`) - the server's omission, never a caller-blaming `Request(Malformed)`; a `MODSEQ 0` in a FETCH response is rejected for the same reason.

## Account layer

The shared `bifrost_types::Account` impl lives under `crates/imap/src/account/`. `ImapAccountFactory::open(account_id)` opens a connection pool, lists folders, builds capabilities, and threads the engine account id into plaintext bandwidth admission and optional byte accounting. `ImapAccount` backs both the sync methods and Stage 1 PIM primitives.

The public account module re-exports only `ImapAccountFactory`, `ImapAccountConfig`, and `ManageSieveConfig` (preserving the conformance-test path `bifrost_imap::account::{...}`); the crate root re-exports the factory and config types directly. `ImapAccount` itself is `pub(crate)`.

Submodules:

- `factory.rs` - `ImapAccountConfig`, `AccountFactory::open(account_id)`, optional `BandwidthMeter`/`MeterSink` wiring, fail-soft CardDAV/CalDAV sub-account open (`DavAttach`), open-time construction of the `DavScopeIndex` from each attached DAV account's collection-folder cursor scopes, `ID` probe, QRESYNC negotiation, folder LIST, and A5c NAMESPACE/ACL shared-folder discovery (`discover_shared_folders`).
- `pool.rs` - per-folder data checkout under one physical session budget. Every session owns a permit from connector start through driver exit; parked members retain theirs. Checkouts adopt parked members (affinity first) or dial. Returned members go directly to the oldest waiter, parking only when nobody waits, so a re-checkout cannot barge. Fresh acquisition for push and replacement never adopts: it queues once on the session semaphore before retiring its own or a parked member (bounded LOGOUT, then terminate and join) to make room. Both queues are FIFO; a simultaneous member and permit grant keeps the member and releases the permit. The connector seam spawns as its final step, taking the lease handoff without an intervening await and returning immediately, so cancellation cannot leave an unregistered live driver. Every dial receives the account meter and bandwidth budget and is brought to the account's QRESYNC policy before handout; see "CONDSTORE / QRESYNC strategy". At the cap, discarded and non-reusable sessions can delay the next dial through the driver's bounded LOGOUT. Replacement on pre-UNSELECT servers can wait behind older acquisitions; it holds no session while waiting.
- `folder_registry.rs` - mailbox map plus per-folder cursor and MODSEQ caches (`record_modseq`, `modseq`, `clear_modseqs`, `clear_modseq_ranges`), keyed by `(folder, uidvalidity, uid)`, storing LIST delimiter/attributes and the A5c `shared_owner` tag, clearing on UIDVALIDITY change, delete, or rename.
- `envelope.rs` - `FolderCursor` (QResync/Condstore/Basic) plus `encode_cursor`/`decode_cursor` over `OpaqueChangeState`.
- `capabilities.rs`, `inventory.rs`, `changes.rs`, `get.rs`, `blob.rs`, `mutate.rs`, `push.rs`, `close.rs`, `scopes.rs` - one file per `Account` method group. `mod.rs` holds `route_scope`/`ScopeHandler`; typed legacy DAV scopes dispatch by object type, while per-collection DAV folder scopes dispatch through `DavScopeIndex`. `FolderId` carries no protocol namespace and IMAP mailbox names are arbitrary strings, so the index is built once at open and made unambiguous there: a collection href equal to a known IMAP mailbox name is not indexed (the mailbox keeps its scope), and a href claimed by both sub-accounts is not indexed (the tie is never resolved silently in favour of contacts). Both exclusions are reported as open-time warnings on the degraded-DAV lane, so a dropped entry routes to IMAP or to `Unsupported` rather than syncing the wrong collection quietly. Push admission consults the same index: an indexed DAV collection scope is a syntactically valid mailbox name but names no mailbox, so it is refused per-item (the DAV sub-accounts have no push lane) instead of being reported as pushed and burning a push slot on a folder no worker can SELECT. `scopes.rs` holds the discovery fan-in.
- `pim.rs` - Stage 1 mail actions: container membership, keyword/read mutations, search, folder CRUD, quota, draft create/discard, message/thread hydration, IMAP-specific thread move/delete conveniences.
- `sieve.rs` - optional ManageSieve client plus Stage 2 literal script filter list/create/update/delete/validate.

### CONDSTORE / QRESYNC strategy

`FolderCursor` has three variants and `changes_stream()` dispatches diff strategy by variant:

- `QResync { uidvalidity, modseq, known_uids, known_uids_complete }`: the QRESYNC SELECT supplies VANISHED and changed-FETCH data since the cached MODSEQ. No second `UID FETCH ... CHANGEDSINCE ... VANISHED` is issued. Because the known-UID parameter can restrict SELECT responses to the old baseline (RFC 7162 Section 3.2.5.1), a live UID count that disagrees with EXISTS triggers `UID SEARCH ALL` and a membership diff before checkpointing. A failed SEARCH cannot advance the cursor; a successful reconciliation retains SELECT's HIGHESTMODSEQ so concurrent changes remain eligible for the next poll. VANISHED ranges are subtracted from the live known set before removals are emitted, so unknown UIDs and coalesced holes produce no membership changes.
- `Condstore { uidvalidity, modseq, known_uids }`: bounded streaming flag diff via `CHANGEDSINCE`; expunge detection via UID-list diff against `known_uids`. `CHANGEDSINCE` returns arrivals as well as updates, so a UID absent from `known_uids` is not emitted as `Updated` - the baseline diff reports it as `Added`, once. The QRESYNC path guards the same hazard through `record_fetch_change` / `fetch_change_seen`.
- `Basic { uidvalidity, uidnext, known_uids }`: neither extension; UID-list diff for additions and expunges. When SELECT returns the cursor's unchanged nonzero UIDNEXT and EXISTS still equals the baseline count, the exact snapshot is reused and `UID SEARCH ALL` is skipped. Server-authored `changed_messages` UIDs are classified against both sets before they can become changes: a UID in the baseline is `Updated`, a UID that is only in the live snapshot is an arrival and is left to the diff's `Added` lane so it is never announced twice, and a UID in neither set is contradictory and invalidates the cursor. The last case is also what makes the UIDNEXT/EXISTS skip safe, since there the live snapshot is the baseline. Without CONDSTORE, SELECT's changed-message FETCH responses are only hints; flag changes on an existing UID can go unobserved until re-inventory. `CursorDescriptor` has no flag-freshness field, so consumers cannot distinguish this limitation from a current flag view.

All three strategies emit at most `BATCH_ITEMS` changes per page. CONDSTORE
drives its bounded FETCH receiver and command future together, and treats the
receiver closing before the command result the same way as inventory:
the command future remains authoritative, so a failed or truncated
FETCH cannot checkpoint success.

The folder entry's in-memory cursor is committed only after the final change
page and `Done` carrying its checkpoint are accepted by the stream channel.
That cursor is a hint for later SELECTs; the engine-provided cursor remains the
durable synchronization checkpoint.

Negotiation in `factory.rs`:

1. `ID` probe runs when advertised; failures are non-fatal. iCloud is preconfigured off via exact-match `name` checks (`"icloud"` / `"icloud imap"`); substring matches do not trip the downgrade.
2. If `enable_qresync` and the server advertises QRESYNC, the factory brings the primed connection to QRESYNC through `pool::enable_qresync`, the same initialization every pool dial runs. The account policy is seeded from that member's COMMITTED profile (`ServerProfile::enabled("QRESYNC")`), never from ENABLE's return value: a server that echoes `* ENABLED QRESYNC` and then refuses the command has still told us its wire behaviour changed. When the member is not enabled the session continues CONDSTORE-only with a one-shot `OperatorAttentionNeeded` warning; a server advertising QRESYNC without ENABLE, or refusing the ENABLE, lands there too rather than failing open. That warning is emitted by the INVENTORY walk, not the changes stream - the changes path reads the non-consuming `qresync_negotiation_reason()` and never spends the one-shot, because doing so could not change a byte of what it emitted while burning the warning out from under inventory. The one-shot is also RESTORED if the inventory task ends with its output channel already closed, which is the only case where the warning provably reached nobody; with a live receiver an mpsc keeps buffered items past sender drop, so delivery is not in doubt and the no-repeat property holds.
3. The QRESYNC capability check is exact (not substring) to avoid false positives on capabilities with `QRESYNC` as a suffix.

QRESYNC is a property a connection is BORN with, never acquired later. ENABLE is legal only before any mailbox has been selected in the session (RFC 5161 Section 3.1), and inventory and the PIM primitives select with no cursor, so checkout affinity routinely hands a QResync-cursor run a member that has already selected the folder. Hence:

- The policy (`ImapAccount::qresync_enabled`, stored on the pool) is a strategy-admission flag only: it decides whether new work REQUESTS QRESYNC. Whether a connection does QRESYNC is its own committed profile, and wire interpretation reads that, or the form a response arrived in, never the policy. `disable_qresync_for_session` stops new requests and new dial-time ENABLEs; it cannot undo QRESYNC on members already enabled (RFC 7162 Section 3.2.3 gives no way back short of closing the connection), and nothing needs it to.
- `Pool::dial` registers a new member and then, while the policy is set, runs `enable_qresync` before handing it out. That helper negotiates only when the profile supports both QRESYNC and ENABLE. A completed refusal on a reusable connection admits a healthy non-QRESYNC member; an ENABLE that times out (withdrawn or abandoned) or a dead connection retires it, terminated without LOGOUT. A pool closed during the dial also terminates without LOGOUT, since LOGOUT is unbounded.
- `select_for_sync` never issues ENABLE. It sends QRESYNC parameters only when the options carry them AND the connection has QRESYNC enabled, and reports which happened as `QresyncOutcome` (`Used`, `NotRequested`, `NotEnabledOnConnection`), decided when the command was built rather than read back from the policy later.

Runtime downgrades:

- A QResync-cursor run whose SELECT did not use QRESYNC (`QresyncOutcome` other than `Used`) is validated as usual (UIDVALIDITY, NOMODSEQ, MODSEQ reset) and continues as CONDSTORE on the SAME checkout (`run_condstore_from_selected`), with one `StrategyDowngraded` warning naming the reason. The policy is not withdrawn for one member's shortfall. Every CONDSTORE run mints its next cursor through `cursor_from_select`, the policy-based rule inventory uses, so a member that could not do QRESYNC never demotes the folder for good: with the policy set and persistent MODSEQ, the checkpoint is a QResync cursor at the SELECT's HIGHESTMODSEQ over the complete UID SEARCH snapshot.
- A QRESYNC SELECT response that fails to parse calls `disable_qresync_for_session()` (one-shot), discards the suspect pooled connection, and retries on CONDSTORE.
- A legacy QRESYNC cursor lacking a complete UID baseline (`known_uids_complete == false`) cannot be diffed against consumer state. It terminates with `CursorInvalid`, deriving `RestartScope`, so bifrost-sync deletes the cursor and re-establishes exact membership through inventory. Current server membership is never substituted for the missing historical baseline.
- A mailbox that answers a QRESYNC or CONDSTORE SELECT with `[NOMODSEQ]` finishes as Basic on the connection it already holds. That SELECT carried changed-message FETCH data, so the Basic run consumes it under the classification above rather than discarding the flag updates it describes.
- VANISHED and FETCH may report the same UID on non-conformant servers; the change stream de-duplicates so a message never surfaces as both expunge and update.

### Mutations

Every mutation path builds its operand and mints its outcomes through
`TargetBatch` (see "One outcome per id" above); the per-path rules below
describe what each mint closure decides, not a second copy of the accounting.

`bulk_set_flags` and `bulk_destroy` partition targets by cached MODSEQ: cache hits go out under `STORE UNCHANGEDSINCE <modseq>`, misses fall back to unprotected STORE, and protected batches run first so their success updates the cache before the unprotected pass.

All three bulk operations accept the Account API's `IdempotencyKey`, but IMAP
has no wire token for server-side request deduplication, so the provider does
not inspect or persist the key. Uncertain per-UID outcomes are reconciled by
the engine's read-back path.

- The MODSEQ cache is populated from inventory, get, changes, and push IDLE FETCH events; cleared by successful flag mutations, expunges, VANISHED, moves, and folder delete/rename. A tolerated server MODSEQ value of zero is ignored: it cannot guard a later STORE and the opportunistic cache must not turn it into a failed sync or mutation. A zero UIDVALIDITY is also ignored because, although the response parser tolerates it from some servers, it is not a usable cache epoch. The cache is bounded per folder (50k entries, pruned back to 40k): inventory walks every UID in a mailbox, and an opportunistic cache must not grow with mailbox size. Eviction keeps the highest UIDs - arrival order is the cheap stand-in for "most likely to be mutated" - and a miss costs exactly what a cold cache costs, an unprotected STORE.
- `bulk_destroy` partial-failure accounting separates conflicts (UNCHANGEDSINCE rejected) from expunge failures; an expunge failure applies only to that round's UIDs, not the whole batch.
- A two-sided `FlagOp::Patch` (non-empty add and remove) is two STOREs, and every id is accounted per UID across both. The guard rides the first (add) STORE; UIDs it rejects with `MODIFIED` are `Failed(ConcurrencyConflict)` and are excluded from the second STORE. Only the add-applied subset receives the unguarded remove, and only that subset may report `Succeeded(Applied)` - a second STORE that errors or comes back non-applied never reports success for those ids: transmission evidence decides the lane (`InFlight` evidence is `Uncertain`, an `Unsent` or server-acknowledged failure is `Failed`), per the mutation error-lane rule below. A wire error on the first STORE is confined to its own MODSEQ group: outcomes already established for earlier groups survive it and are never downgraded by the folder-level error path.
- `bulk_move` validates the destination mailbox NAME once (syntax via `MailboxName::new`), before any source folder is opened; a syntactically invalid destination fails every target `Request(Malformed)` up front. Existence and selectability are not preflighted: a well-formed but nonexistent destination flows into the per-folder path and surfaces as per-item outcomes from the server's `NO [TRYCREATE]`.

Capabilities advertise `MutationConcurrency::None`: the MODSEQ cache is opportunistic (cold cache = unprotected STORE), so `StateBased` would let the engine assume UNCHANGEDSINCE is always wired. The engine's read-back-after-retry path is the lost-update safety net.

### PIM primitives

Generic `Projection` hydration and PIM `HydrationProjection` share
`account/hydration.rs` for FETCH attribute selection and body-byte decoding.
Their outer results intentionally differ: generic hydration can return raw MIME
and has flags-only/metadata/text-only modes, while PIM hydration always carries
message metadata and parses selected bytes through `bifrost-types::mime`.

`capabilities.rs` fills `AccountCapabilities::pim_methods` and `conveniences` at open time, and sets `AccountCapabilities::discovers_foreign_namespaces_on_rediscovery` from the open-time NAMESPACE response (`namespaces_advertise_foreign`, pure, `factory.rs`): true iff any `other`/`shared` descriptor carries a non-empty prefix, independent of whether any folders are currently shared. IMAP emits no scope-lifecycle events and discovers foreign folders only at open, so consumers read this flag to decide whether a periodic `SyncEngine::reattach` (the only way a post-open ACL grant becomes visible) can ever surface anything on this server. IMAP advertises real support for:

- Container membership: `add_to_container` via UID COPY, `remove_from_container` via `+FLAGS.SILENT \Deleted` plus UID EXPUNGE. `add_to_container` and `message_hydrate` check their own `pim_methods` flag first and answer `Unsupported` when it is false, like the other primitives.

The per-folder PIM primitives (`copy_messages`, `delete_messages`, `set_flag`, `hydrate_decoded`) stamp failures in the folder's PREPARATION (checkout, SELECT/EXAMINE, UIDVALIDITY read) `Unsent` through `preflight_err`, relative to the operation: the SELECT's own evidence (a tagged NO is `Acknowledged`) is about the SELECT, not the COPY / STORE / FETCH that was never sent, and publishing it would make recovery reconcile a non-idempotent mutation that never reached the server. It speaks for the folder being prepared only; earlier folders of a multi-folder call may already have been mutated, which these `Result<(), _>` lanes cannot express.

Every path that removes messages (sync-side `bulk_destroy`, PIM `delete_messages`) goes through `expunge_uids_or_fall_back`. It issues `UID EXPUNGE` (RFC 4315 Section 2), and on `MissingCapability` - a server with neither UIDPLUS nor IMAP4rev2 - falls back to plain `EXPUNGE` only when `UID SEARCH DELETED` shows the mailbox's `\Deleted` set is a subset of the UIDs this batch just marked. Plain EXPUNGE names no messages: running it with a foreign `\Deleted` present would silently destroy another client's mail, so that case keeps the `MissingCapability` failure with the batch left flagged.

The SEARCH check is not atomic with the following `EXPUNGE`. Another client
can mark a message `\Deleted` in that interval, and IMAP has no command to
restrict plain EXPUNGE to the checked UID set. Without UIDPLUS, the remaining
race cannot be closed by this client.
- Keywords: `set_keyword` via UID STORE; convenience keywords `$flagged`/`$answered`/`$seen` map to `\Flagged`/`\Answered`/`\Seen`. `$forwarded` and `$MDNSent` stay IMAP keywords; `mdn_sent_via_keyword` is true, so `mark_mdn_sent` flips `$MDNSent`.
- Read state: `set_is_read` via `\Seen`.
- Importance: `set_importance` maps onto the `$important` keyword (no native IMAP field). Two-valued/exclusive: `High` sets it, `Normal`/`Low` clear it (one STORE); the read side maps `$important` presence to `Message.importance`.
- Search messages: UID SEARCH across selectable folders, `SearchFilter::In` restricting the selected mailbox. Thread search is advertised only with `THREAD=REFERENCES`, returning synthetic thread ids containing folder, UIDVALIDITY, and member UIDs.
- Containers: LIST-backed enumeration plus CREATE, RENAME-as-rename, RENAME-as-move, and guarded DELETE (checks `STATUS MESSAGES`, refuses non-empty mailboxes).
- Quota: `GETQUOTAROOT`, mapped from STORAGE units to bytes when QUOTA is advertised. The account probes the Inbox role when it exists; otherwise it chooses the first selectable folder by display name, wire octets, and encoding mode. Hydration: one-shot message FETCH and synthetic thread hydration.
- Draft create/discard: APPEND to Drafts with `\Draft` when a delimiter-aware Drafts role and UIDPLUS or active IMAP4rev2 are present; discard is advertised only when UID EXPUNGE is available. Draft bodies use the shared `bifrost-types::mime` assembler (inline attachments supported; uploaded-attachment handles rejected as `AttachmentUpload`), preserving the `Bcc:` header in the saved draft.
- Send / draft-send: real when `ImapAccountConfig::with_submission(SmtpSubmissionConfig)` is set (see "Submission" below); `Unsupported` otherwise.

Unsupported PIM methods return `Error::Unsupported` with false flags: attachment upload, `host_attachment` (Gmail/Graph only), draft update, Gmail label membership, Graph categories/extended properties, identities + identity update, vacation get/set, `scheduled_send` (cancel/reschedule too). `scheduled_send` is statically false even with submission because FUTURERELEASE is a per-connection EHLO truth unknown at open; the send is attempted and the relay's EHLO decides. SMTP send/draft-send are unsupported only without submission config. IMAP identities and vacation responders are not in Stage 1.

### Submission (SMTP send)

`ImapAccountConfig::with_submission(SmtpSubmissionConfig)` makes the account own a `bifrost-smtp` transport (`submission.rs`, `SubmissionTransport`), built at open and stored on `ImapAccountInner.submission`. `build_capabilities(.., submission_configured)` flips `send_message` / `draft_send` true together with the impls. `attachment_upload` stays false (A6).

- `SmtpSubmissionConfig` carries host, `SubmissionTls` (Implicit 465 / StartTls 587 / Plaintext 587, mapped to SMTP `relay` / `starttls_relay` / `builder_dangerous`), optional port/timeout/pool, default From, `save_to_sent_default`, optional `SubmissionCredentials`. `Clone`, not `Eq`/serde.
- Credential reuse: when `credentials` is `None`, SMTP creds derive from the IMAP
  `Credentials` via `credentials.kind()` (password->password, or OAuth identity +
  the *same* `Arc<dyn TokenSource>` cloned across the boundary); an override
  supplies explicit submission auth.
- The send path stays in `bifrost_types::Address` space; the only
  `Address -> bifrost_smtp::Address` conversion is the bare addr-spec
  reverse-path / recipient conversion at `SubmissionTransport::send_rfc5322`,
  which drives `AsyncSmtpTransport::send_raw_batch_with_options` and returns
  SMTP's already-translated `AccountError` (a build failure maps to
  `InvalidInput`). `draft_send` re-stamps the operation to `DraftSend`.
- Scheduled send is one-shot SMTP FUTURERELEASE: `SendRequest::scheduled`
  threads `hold: Option<SystemTime>` into `send_rfc5322` ->
  `SendOptions::hold_until(rfc3339(t))` (absolute-time, never races `now()`).
  No IMAP-side pre-validation - the relay's EHLO at send time is authoritative.
  A relay without FUTURERELEASE yields a stable `Unsupported(Send)`; a HOLD over
  the advertised max arrives as `Request(Malformed)`. RFC 4865 has no recall
  verb, so `cancel_scheduled_send` / `reschedule_send` are `Unsupported`.
- `pim_methods.send_as` is false, so a `Some(send_as)` request is the
  `Unsupported(Send)` branch of the rule stated on `SendRequest::send_as`.
- MIME assembly is the shared `bifrost-types::mime` serializer
  (`send_request_to_rfc5322` for send, `render_rfc5322` for drafts; shared with
  Google's `MailDocument`): text/html/`multipart/alternative` bodies, a
  `multipart/mixed` wrapper for inline attachments, RFC 2047 display names, and a
  controlled-domain `Message-ID` (sender domain, never `hostname::get()`).
- Draft submission reads its header block through `HeaderMap::parse`, with
  shared address-list parsing (quoted-pairs and RFC 2047 display names) and
  `HeaderMap::message_ids`. `HeaderMap::addresses_checked` validates From,
  Sender, To, Cc, and Bcc; any rejected item returns `Request(Malformed)` before
  SMTP submission. Repeated recipient headers retain wire order. Message IDs
  without angle brackets are not recognized and use the existing sender-domain
  fallback. Header decoding does not rewrite the transmitted draft bytes; only
  the Bcc header is stripped from the SMTP copy.
- `ObjectId` rule: `send_message` / `draft_send` return the APPENDUID-derived id
  from the Sent APPEND when UIDPLUS yields one, else the generated/parsed
  `Message-ID` (`imapmsgid1:<id>`). The send result is authoritative for `Ok`.
- Sent-APPEND contract: after a committed send, a failed (or no-UIDPLUS, or
  no-Sent-folder) APPEND is non-fatal (SMTP is never re-driven), logs an
  uncertain-Sent reconcile warning, and falls back to the generated `Message-ID`.
- `draft_send` is "fetch + send + discard": a one-shot raw `BODY[]` fetch
  (bounded by `DRAFT_FETCH_BUDGET` 64 MiB, through `FetchLimit`) recovers the
  verbatim octets, the headers build the envelope, `Bcc:` is folded into RCPT but
  stripped from the body. A failed post-send discard is logged, not fatal.
- Sent-copy Bcc retention: the SMTP body strips `Bcc:` but the Sent APPEND
  retains it. `send_message` uses the assembler's `sent_copy` variant (Bcc-bearing,
  only when the request has a Bcc); `draft_send` APPENDs the saved-draft octets,
  which already carry `Bcc:`.

`bifrost-imap` depends on `bifrost-smtp` (feature `tokio`; `account-error` is a
default feature). SMTP has no path back to IMAP, so this is acyclic.

With `ImapAccountConfig::with_manage_sieve(ManageSieveConfig)`, IMAP advertises `filter_rule_shape: Scripts` and all five filter flags true; `sieve.rs` speaks ManageSieve over implicit TLS / STARTTLS / plaintext, reuses the account credentials with SASL PLAIN or XOAUTH2, and maps scripts to `FilterScript { language: Sieve }`. Without it, filter flags stay false and calls return `Unsupported`.

With `with_carddav(CardDavConfig)` / `with_caldav(CalDavConfig)`, IMAP opens a sibling `bifrost-carddav` / `bifrost-caldav` account at factory open and delegates the contact / calendar PIM primitives to it. Directory lookup and
its `directory_search` / `directory_enumerate` capabilities forward from the
CardDAV contact backend too; lookup is enabled only when CardDAV admitted a
directory gateway, and CardDAV does not support empty-query enumeration.
CalDAV contributes no directory capabilities. Without the config, the matching flags stay false and primitives return `Unsupported`.

Composition is first-class for sync, not just primitives (subs are full `Arc<dyn Account>`s): `build_capabilities` copies the sub's real contact/calendar `pim_methods` subsets; `route_scope` maps a `CursorScope` to `ScopeHandler::{Folder, Delegate}` and the four sync entry points forward to the sub on `Delegate`; discovery fans sub scopes in (sub errors fold to `Warning`). Routing + fan-in use the reusable `bifrost_types::account_compose` helpers. `open_carddav`/`open_caldav` are fail-soft (`DavAttach::Degraded`, classified via `RecoveryClass`): a DAV open failure degrades to IMAP-only for the cycle and the next reopen retries, so a DAV outage never takes mail offline. The two run JOINED, not in sequence - each is an independent multi-round-trip discovery against its own endpoint with its own client, so awaiting them in turn made every IMAP open pay both latencies. They may complete in either order; `into_attached` is applied afterwards in a fixed order, so the `dav_degraded` and `skipped_scopes` lanes stay deterministic regardless. The degradation is recorded twice on purpose: as the discovery-surfaced `Warning` (live signal) and as a `SkippedScope` on `OpenedAccount::skipped_scopes` (`ContactCollection` / `CalendarCollection` scope + the classified error), the queryable open-time record.

The `Warning` half is delivered ACKNOWLEDGE-DRIVEN, not drained. `discover_cursor_scopes` reads the recorded warnings without consuming them and clears them only once the discovery stream has actually yielded its terminal `Done` to the consumer. Draining at stream-construction time put them inside a future the consumer is free to drop un-polled, and unlike the QRESYNC one-shot there is no second copy to fall back on - the account would then present as mail-only forever, which is exactly the state this lane exists to report. The cost of the safer order is at most one duplicate report per stream that is built and dropped before `Done`; the no-repeat property still holds on every path that completes.

`ConvenienceShape` declares IMAP starred/replied/forwarded as keyword-shaped. `move_thread`/`delete_thread` override the trait defaults via the cloneable account handle (add-then-remove); delete moves to the Trash role unless already in Trash, where it expunges from that mailbox.

Containers use native mailbox paths as primitive/provenance ids. `containers_list` maps SPECIAL-USE attributes to `FolderRole` (`\Sent`, `\Drafts`, `\Archive`, `\Trash`, `\Junk`, custom `\Inbox`), falling back to name-based INBOX/Sent/Drafts/Archive/Trash/Spam detection on the leaf selected with the LIST hierarchy delimiter. `folder_registry::leaf_name` is the single owner of that split, shared with the `draft_create` Drafts probe so a `.`-delimited server cannot advertise a draft capability whose APPEND target `role_folder` then fails to resolve; a NIL delimiter keeps the legacy `/` fallback rather than treating the whole name as the leaf. When several folders map to one role, SPECIAL-USE wins over name fallback, and display name, wire octets, and encoding mode break remaining ties deterministically.

`container_from_folder_entry` also projects the shared-namespace metadata: a folder with a `shared_owner` gets `namespace = Shared`, `owner = MailboxId(owner)`, and `owner_local_id` = the full mailbox path (IMAP has no separate per-owner id space - the path IS the native id in every namespace, so `native_id` stays byte-identical to the `CursorScope::Folder` string discovery emits). `rights` projects the MYRIGHTS set `discover_shared_folders` captured at open (`rights_from_myrights`, RFC 4314 Section 4: `l`+`r` -> read, `i` -> add, `t` -> remove, `s` -> seen, `w` -> keywords, `k`/`c` -> create child, `x`/`d` -> rename+delete, `p` -> submit). The rights set is now RETAINED on `FolderEntry`, not just used as the discovery read-gate - without it a read-only share is indistinguishable from a writable one downstream. `None` means unreported (a personal folder we never probed, or a server without ACL), which is distinct from an explicit empty rights set. A rename/recreate inherits both the owner tag and the rights, since a LIST/IDLE `MailboxInfo` carries neither.

### Hydration accounting (`get.rs`)

`get_stream` and the bulk mutation streams retain at most `TARGET_BUFFER_ITEMS` (256, in `account/mod.rs`) decoded target ids at once. A full window flushes immediately and a partial window flushes when input closes; 256 is two `BATCH_ITEMS` pages, small enough that a caller streaming millions of ids never materializes them all and large enough that a per-folder UID command still covers a useful range rather than degenerating into one command per id. Each window is grouped into per-folder UID commands, with folders visited by display name and wire identity within that window.

Output ordering is therefore input-window order, then folder display-name order with wire identity breaking ties within a window; FETCH response order within a folder remains server-driven. This is weaker than the previous guarantee, which drained the whole input before visiting folders lexically, so ids for one folder could previously never be split across batches. bifrost-sync tolerates the weaker contract because it keys results by `ObjectId` rather than by arrival position, and `BatchOutcome` carries its own submission-order index (`reference/error-model.md`); nothing downstream may start depending on cross-window ordering. Every requested id is still answered exactly once. Command response bodies retain their separate byte budgets.

- Ids whose UIDVALIDITY no longer matches the selected mailbox are `Failed(Request(Malformed))` and are published *before* the hydration FETCH is issued. They are known truth already; buffering them behind a fallible command would relabel them uncertain whenever that command fails. The per-folder error path in turn only downgrades ids that still lack a published outcome (`run_folder_get` prunes the caller's unresolved set as it publishes), so a FETCH failure after the stale batch cannot put one id in two lanes.
- A requested UID the server never returns is `Failed(NotFound(Message))` rather than being silently dropped.
- The valid (non-stale) ids of a folder become a `TargetBatch`, so the FETCH operand and the set the loop attributes results to are the same object. Its excluded lane is published alongside the stale failures, before the FETCH: like them it is known truth already, and unlike them it must never be able to reach the FETCH and be reported as hydrated.
- FETCH responses are merged per UID before conversion. A server may follow the solicited response with unsolicited FLAGS-only FETCHes for the same UID; the merge adopts later `FLAGS` / `MODSEQ` and Gmail labels (mutable state, so the fresher report wins) and fills gaps in every other item (preview, EMAILID / THREADID, the Gmail message and thread ids, body and BINARY sections, per-section `BINARY.SIZE`), but never blanks a data item or body section the earlier response carried, so a trailing partial response cannot turn a complete hydration into an empty one. `merge_fetch_response` is shared with the PIM lane: `pim::hydrate_decoded` folds the FETCH's responses into exactly one per REQUESTED UID (`merge_requested_fetches`), dropping unsolicited FETCHes for UIDs nobody asked about and responses without a UID, so a split or FLAGS-only-follow-up response can no longer hydrate as a duplicate, possibly bodiless, message.
- `pim::message_hydrate` picks its answer by the canonical id rather than by position, and a UID the tagged OK never answered for (its UIDVALIDITY already matched, so the UID is a live name in this epoch) is `NotFound(Message)` with `Acknowledged` evidence - the message was expunged between the caller learning the id and the FETCH - not the caller's malformed request.
- `Projection::Preview` and `Projection::TextOnly` both ask for `BODY.PEEK[]` (whole message, not the TEXT section) - preview bounded to a prefix, text-only unbounded. `BODY[TEXT]` is never requested by either: the hydrated value is raw MIME the consumer parses, and the TEXT section of a multipart message is boundaries and base64 with no headers to decode them by. `get.rs` and `pim.rs` use the same 64 KiB preview floor (`PREVIEW_FETCH_BYTES`), raised to the caller's limit when that is larger.
- A projection that asks for a body section runs through `uid_fetch_limited` under `HYDRATION_FETCH_BUDGET` (256 MiB per batch); metadata and flag projections keep the unbudgeted `uid_fetch`, being bounded by their own response shape. The budget is a guard against a corrupt or adversarial server, not a per-message limit: crossing the retained-object estimate surfaces as `Error::FetchLimit` after the tagged completion. A literal declaration above the remaining budget instead fails before payload accumulation with `Error::FetchLiteralLimit` and retires the connection.
- Full and preview hydration parse the fetched RFC 5322 source through
  `bifrost-types::mime`. Full exposes text and HTML plus attachment metadata;
  `FullWithBlobs` carries decoded attachment bytes inline because IMAP has no
  redeemable per-part handle. Preview fetches a whole-message prefix (at least
  64 KiB, or the requested limit when larger) rather than `BODY[TEXT]`, so
  multipart framing is not shown as prose - in `pim.rs` and `get.rs` alike.
  RFC 2047 and RFC 2231 decoding now
  lives in `bifrost-types::mime`, shared with this parser.

### Blob openers

`AccountCapabilities::blob_range` is `BlobRangeSupport::No` and `open_blob` / `open_blob_range` return `Unsupported`. IMAP can fetch a `BODY[]` section by range, but nothing in inventory or hydration mints a `BlobHandle` for a MIME part, so there is no handle a caller could hand back to an opener. `blob.rs` therefore serves only `open_raw_rfc822`, which streams the whole message via `BODY.PEEK[]`: the FETCH runs through the bounded streaming lane (`uid_fetch_stream_budgeted`), passing the remaining `RAW_FETCH_BUDGET` to the reader. The wire parser charges all literals in each FETCH response against that remaining amount before decoding, so an over-budget response is rejected before the literal is materialized. The consumer's back-pressure still reaches the socket through the driver's `OwnedPermit` reservation. The account drain retains a post-decode `Error::FetchLimit` check as a defensive guard. Each body section is emitted as its own `Bytes` page after the complete FETCH response has been buffered and decoded. The command future stays authoritative for the wire outcome, as in inventory / CONDSTORE. Decoded attachment bytes nevertheless reach `FullWithBlobs` inline; that is not a resumable blob lane. Building the real capability is filed as imap-G1 and is being built in stages.

**imap-G1 stage 1 (`account/parts.rs`) - foundation, deliberately unwired.**
Two pieces exist and are exercised by tests, but nothing calls them: no
projection mints a handle, `blob_range` stays `BlobRangeSupport::No`, and the
openers stay `Unsupported`. Flipping the capability before a projection mints
handles would advertise a byte path the crate cannot serve, which is the
defect shape this crate has already paid for once.

- `walk_bodystructure` flattens a parsed `BodyStructure` into the
  individually fetchable parts, each carrying the IMAP part path, lowercased
  media type/subtype, a `PartEncoding`, the server-reported encoded octet
  size, and the identity the server gave (content-id verbatim,
  content-location, filename). Numbering follows RFC 3501 Section 6.4.5 per
  *message context*, not per node: a single-part message is part `1`; a
  `multipart/*` container has no number of its own and is not emitted, its
  path being only a prefix of its children's; and a `message/rfc822` part is
  emitted at its own path AND opens a fresh context, so its inner multipart
  numbers `P.1`/`P.2` and its inner *single* part is `P.1`. That last
  composition is the case wrong implementations miss. Tests parse real wire
  BODYSTRUCTURE bytes through the crate's own decoder (widened to
  `pub(crate)` for exactly this) and pin the exact paths for text,
  alternative, mixed-with-attachment, nested multipart, both embedded-message
  shapes, and `multipart/signed`.
- It returns a `PartWalk { parts, truncated }`, not a bare `Vec`, so
  exhaustion cannot be silent: a caller building a projection that claims to
  enumerate a message's parts can tell a depth-limited walk from a complete
  one. `MAX_PART_DEPTH` is 64 and is counted exactly as the BODYSTRUCTURE
  parser counts its own `MAX_BODY_NESTING_DEPTH` (one level per `multipart/*`
  container and per `message/rfc822`, bound applied as `depth > MAX`), so for
  anything that came off the wire the PARSER refuses over-deep input before
  the traversal ever sees it and `truncated` is unreachable in production;
  only a hand-built structure can reach the bound. The parity is pinned
  behaviourally by two tests - a 64-deep `message/rfc822` chain walks complete,
  a 65-deep one is rejected by the parser - rather than by naming the parser's
  private constant, so either side drifting fails a test.
- `PartEncoding` carries this crate's `TransferEncoding` classification AND
  the server's own token. An encoding the crate does not model (`x-uuencode`
  and the long `x-` tail) classifies as `Unknown`, and the token is the only
  remaining description of how those octets are encoded; folding it away
  would leave a later layer unable to tell a consumer either how to decode
  the bytes or what the server claimed. The token is the parser's output, so
  it is ASCII-lowercased (RFC 2045 Section 5.1 makes the token
  case-insensitive) and otherwise untouched. What a consumer-facing surface
  does with an unmodelled token - `bifrost-types`' `BlobEncoding` has no
  unknown variant - is a stage-two decision; stage one owes the information,
  not the policy.
- `encode_part_handle` / `decode_part_handle` are the durable handle format,
  now at version 2:
  `imappart<version>:<folder-len>:<folder>:<uidvalidity>:<uid>:<path>:<encoding>:<token-len>:<token>`.
  The version rides in the prefix token so an unknown version is refused as a
  *version* rather than falling through to "not a handle", and it is never
  reinterpreted under this version's field rules. v1 (which lacked the token
  field) is refused outright rather than upgraded with a default: nothing has
  ever minted a v1 handle, since the module is unwired and `blob_range` still
  reports `No`, so a migration path would exist only for handles that cannot
  exist. The folder is length-prefixed rather than escaped (mailbox names may
  contain `:`), the encoding token is length-prefixed for the same reason (it
  arrives as a quoted string and can contain `:`), UIDs and path components
  are `nz-number`-checked, and the classification is a closed token set -
  `TransferEncoding::from_token`'s tolerant `Unknown` fallback is deliberately
  NOT used on decode, since it would launder a corrupted field into "hand back
  undecoded octets". Decode also cross-checks the two encoding fields, but
  only in the direction that cannot misfire: a modelled classification must
  agree with `from_token` of the original token (catching `base64` paired with
  a token of `b64`), while an `unknown` classification accepts any token, so a
  handle minted by a build that models more encodings than this one is not
  refused. `verify_uidvalidity` is the redemption check: a handle minted under
  a prior mailbox epoch names a UID that now means a different message, and is
  refused rather than fetched.

Stage 2 wires it: a hydration/inventory projection that attaches handles, the
`open_blob` / `open_blob_range` implementations over `BODY.PEEK[<path>]` with
`<origin.count>` partials, and only then the capability flip.

### Bandwidth metering

`ImapAccountFactory::with_net(bifrost_net::Net)` selects the shared HTTP
transport used by composed DAV. Before bootstrap, the factory obtains the
account's `BandwidthBudget` from that Net's registry by account id. The engine
and other protocol clients must use the same Net or its registry. Overlapping
incarnations with the same id then share admission. Without `with_net`, the
factory uses `Net::shared_default()` and resolves the IMAP budget from that
same shared transport's registry. Sharing therefore also holds by default.
`Account::set_bandwidth_cap` retunes this budget directly; there is no cap
atomic, sentinel, connection-local token bucket or submission-only budget.
`None` is unlimited and `Some(0)` clamps to 1 B/s in the shared budget.

The bootstrap connection, every pool dial and redial, ManageSieve and SMTP
submission all receive clones of that handle. `WireReader` performs admission
above TLS and COMPRESS. Each read offers at most its grant, including bounded
inflation output; each write reserves before offering a bounded plaintext
slice. Inflation drains retained decoded output even when compressed input is
empty, so a small grant cannot strand already received bytes. There is at most
one outstanding reservation per direction on a connection. Short success
finishes with the actual byte count and refunds only a current tail. Error or cancellation after offer retains the full charge.
STARTTLS and COMPRESS rebuild the reader with cloned metering state, preserving
the budget and observation sink. The underlying socket progress counter remains
below TLS and COMPRESS for transmission evidence, independently of admission.

Account stream batches report inbound IMAP protocol octets measured by the
connection reader after TLS and COMPRESS decoding. Each operation snapshots the
counter after checkout and advances it when it emits a batch, so bytes read
since the preceding batch are reported once. Inventory, changes, hydration,
mutations and raw-message chunks use this delta; local validation failures
with no wire read carry zero. An operation that emits no batch has no batch
field in which to report its reads. The delta relies on exclusive data
checkouts: a `PoolMember` is removed from the idle list until its `PooledConn`
drops, and IDLE workers dial separate connections. Concurrent account streams
therefore sample different connection counters. A cancelled command the
driver had claimed marks its connection abandoned, preventing reuse by
another checkout (one withdrawn before its claim wrote nothing). The shared
counter is not a per-command attribution API for arbitrary concurrent users of
one `ImapConnection`. Read-ahead can put several responses into one batch's byte
count, and trailing reads after the last emitted batch have no batch field;
these figures are batch observations, not complete command or TCP/TLS totals.

An IDLE read pre-spends a slot before waiting on the peer; it holds no budget
lock or exclusive turn. Bootstrap greeting, CAPABILITY and STARTTLS each have
one absolute exchange deadline that includes admission, unsolicited responses,
and (for STARTTLS) the handshake and post-upgrade CAPABILITY. Each exchange also
has a cumulative peer-wait allowance: admitted I/O spends it, admission does
not, and individual reads cannot reset it. The allowance is shared across
stream upgrades and charges cancelled pending I/O as well. Bootstrap creates
one peer budget before reading the greeting and reuses it for an initial
CAPABILITY fetch and STARTTLS, including the TLS handshake and post-upgrade
CAPABILITY. Re-entering a setup phase cannot reset the allowance.
ManageSieve places a cumulative peer-read allowance below `MeteredStream`,
covering one whole line or literal while excluding admission waits. Explicit
command deadlines still bound the whole operation, including admission.
Time arithmetic uses the Tokio clock. The smoothing bound and
outstanding-allowance rules are in `reference/net.md`.

At the COMPRESS boundary, the final read reservation stays outstanding until
the tagged OK identifies the plaintext prefix. A coalesced compressed suffix
is excluded from completion and byte observation, then handed to the inflater.
Its decoded plaintext pays admission when read above COMPRESS. Short completion
still follows the shared budget's tail-only refund rule; no second inbound
reservation is issued while the deferred one remains outstanding.

`ImapAccountConfig` carries an optional process `BandwidthMeter` or generic
`MeterSink`. The factory creates the account-tagged `MeterSinkHandle` for the
initial connection, pool dials, submission and ManageSieve. IMAP records successful admitted
plaintext reads and writes. ManageSieve wraps each sequential read/write
operation in `MeteredStream`, charging the account budget above TLS, including
its plaintext greeting and STARTTLS exchange. Successful completions report
actual bytes through the same account sink.

Composed CardDAV and CalDAV attach through the factory's selected Net before
discovery. Opens and reopens clone that Net; they never construct a new HTTP
transport. The reqwest connection pool, per-host rate-limit governor and budget
registry stay shared across accounts and incarnations. The default continues
to be the process-wide `Net::shared_default()`, including for standalone DAV.

Paused-clock, duplex and scripted-HTTP tests pin shared pool admission,
redial credit, a real IDLE session alongside sibling work, bounded reads, short
writes, conservative cancellation, peer timers, Sieve charging, submission/FETCH
admission, account cap wakeups, the real default Net identity, and repeated account opens sharing
scripted DAV discovery and IMAP FETCH admission with or without `with_net`.
The default-provider seam substitutes scripted HTTP in that combined test; a
separate assertion pins the production fallback to `Net::shared_default()`.

### Folder lifecycle

`FolderRegistry::apply_mailbox_event` handles delete, rename, and delete-then-recreate: delete/rename drop the in-memory entry; a recreate (fresh UIDVALIDITY at the same name) installs a fresh `FolderEntry` with empty modseq cache and cursor, so a recreated mailbox cannot reuse the prior epoch's state.

A same-name LIST/IDLE re-announcement is the existing mailbox epoch, but its
attributes are live lifecycle state. The registry refreshes them (`\Noselect`,
`\NonExistent`, SPECIAL-USE) so a folder whose access or role changed does not
keep syncing under a stale projection until reopen. The refresh happens IN
PLACE: attributes, delimiter, and the derived `selectable` flag live behind
`FolderEntry::listing` (a `RwLock<FolderListing>`, read through
`selectable()` / `delimiter()` / `attributes()`), and the map keeps the same
`Arc<FolderEntry>`. Copying the state into a replacement entry and swapping
the map would drop every write a task made through an `Arc` it cloned before
the swap - a sync run holds its entry across awaits and commits a cursor and
MODSEQs at the end, so the lost-update window is real, not theoretical.

Mailbox management on a mailbox this account has SELECTed goes through
`PooledConn::deselect_target`: an affinity-free `checkout_any` can hand back a
connection still selected on the RENAME/DELETE target, and RFC 3501 lets a
server reject either for the selected mailbox (RFC 2683 2.2.2 asks the client
to close first). `deselect_target` trusts the member's recorded selected mailbox, so that record follows the session through failures as well as successful SELECTs: after a failed `select_for_sync`, `record_failed_select` records none when the session is no longer `Selected` (a tagged NO to SELECT/EXAMINE deselects the previous mailbox), records the new folder when the failure is `ProtocolMissing` on a still-`Selected` session (the SELECT got its tagged OK and the response was then found incomplete), and otherwise leaves the previous selection standing. A stale record made `deselect_target` skip an UNSELECT the server needed. UNSELECT is preferred - it keeps the checkout on its
connection. A pre-UNSELECT server falls back to replacing the member, and the
old connection is LOGOUT'd BEFORE the replacement is dialed: dialing first
would temporarily add a physical connection across the handshake, which a
server enforcing a per-user connection limit rejects - precisely on the old
servers that need the fallback.

### Shared / other-user folders (A5c)

`factory::open` issues NAMESPACE after the personal LIST and enumerates each non-personal `other`/`shared` prefix via `LIST "" "<prefix>*"` (`discover_shared_folders`, prefix in the PATTERN - RFC 3501 6.3.8 leaves reference/pattern concatenation implementation-defined and a server that ignores the reference answers the reference form with the personal namespace), tagging each with its owning `MailboxId`. An other-user root (`#user/`) carries one descriptor for all users, so the principal is read per folder (`mailbox_owner_from_other_user_path`: the segment after the root, e.g. `#user/alice/INBOX` -> `alice`); a shared root collapses to itself (`mailbox_owner_for`). Under ACL, candidates are probed with MYRIGHTS and the parsed set rides out on the entry; a MYRIGHTS failure is non-fatal and defers to SELECT. Discovery no longer DROPS an unreadable candidate: the personal `LIST "" "*"` may echo the same path (RFC 2342), so dropping it left that echo behind as a bare personal entry and a read-only share presented downstream as a writable personal folder. The read decision moves one layer down to `shared_folder_is_selectable` (pure, `folder_registry.rs`): `\Noselect`/`\NonExistent` or a reported rights set without `l`+`r` clears `FolderEntry::selectable()`, which is exactly the flag `discover_cursor_scopes` filters on, so an unreadable share keeps its owner / `Shared` namespace / rights in `containers_list` and still never becomes a cursor scope. The owner rides on `FolderEntry.shared_owner` (via `FolderRegistry::from_lists`). Shared folders are ordinary `CursorScope::Folder` scopes (no new variant, no `route_typed_scope` change); discovery and inventory also emit `MembershipScope::Mailbox(owner)` per shared entry - explicitly, because `scope_covers_membership` does not link them (FolderId and MailboxId strings differ).

Two registry rules keep the shared tagging (and therefore the owner and the rights) attached to the entry:

- **Overlap precedence** (`shared_overrides_personal`, pure). RFC 2342 leaves `LIST "" "*"` free to include the non-personal namespaces and several servers do, so the same path can arrive from both the personal LIST and its own namespace LIST. `ingest_shared` lets the namespace candidate WIN that overlap when its path actually lies under its non-empty NAMESPACE prefix - the server's own declaration that the path is not personal. Skipping the overlap (the prior rule) left the folder registered bare: no owner, no `Shared` namespace, and `Container::rights == None` for a folder whose MYRIGHTS had been parsed and then discarded. A candidate NOT under its prefix is still skipped, which is what stops a reference-ignoring server from demoting a real personal INBOX.
- **Personal-only refresh** (`FolderRegistry::replace_personal`). A mid-session re-LIST after a create / rename / move / delete enumerates only the personal root, so `refresh_folders` must not `replace_all`: clearing the map would drop every shared entry, its owner tag, and its rights, blanking the shared half of `containers_list` until the next reopen. The refresh is also IN PLACE for the personal half, on the same reasoning as `apply_mailbox_event`: a personal folder whose name survives the re-LIST keeps its existing `Arc<FolderEntry>` with only `refresh_listing` applied, so its cursor and 50k-entry MODSEQ cache are not zeroed on every folder CRUD and no concurrently held entry is orphaned mid-run. Only names the re-LIST no longer reports are removed, and only names it newly reports are added.

Revocation quarantines, not escalates. A shared-folder SELECT permission denial in the inventory, changes, and mutation preparation paths routes through `error::shared_folder_error` to `SyncState(ScopeRevoked)` scoped to `Cursor(Folder(id))`, deriving `Engine(DisableScope(scope))`: the engine deletes that scope's cursor and broadcasts a scoped warning while siblings keep syncing. Mutation reports the denied folder's submitted ids as per-item failures and continues to later folders. The same denial on a personal folder (`shared_owner == None`) stays the account-level terminal `NoPermission`. ACL rights are advisory (gate discovery only); `NO [ACL]` stays authoritative per-mutation.

## Error model

`Error` is `pub(crate)` and `#[non_exhaustive]` (variants include `AuthPolicy`, `FetchLimit`, `FetchLiteralLimit`, the server-status, transport and protocol variants, and the local-refusal variants below). The account boundary converts protocol errors into `bifrost_types::AccountError` via `error::into_account_error(error, ctx)`; consumers never see the crate-internal taxonomy.

`ImapErrorContext` carries the calling `AccountOperation` (required), optional `ErrorScope`, `Provider`, explicit `transmission_state`, and `idempotency_override`. Every `pim.rs` method threads its operation via a local `op_err` closure stamping `ImapErrorContext::operation(<op>)` on every `map_err`; multi-call helpers (`copy_messages`, `delete_messages`, `set_flag`, `hydrate_decoded`, `refresh_folders`, `folder_from_scope`) take an explicit `op: AccountOperation`. This lets recovery distinguish `Reconcile` from `Retry::SameRequest` for non-idempotent ops like `AddToContainer` and `DraftCreate`.

`Error::response_code()` returns the structured `ResponseCode` from `[CODE ...]` brackets (first only); recovery consumes those via the typed `ImapResponseCode` wire variants.

`Error::Protocol` is for the SERVER violating the wire contract, and is connection-fatal; it maps to `Protocol(ContractViolation)`, deriving `ProviderContractViolation`. `Error::ProtocolMissing` is its narrower case where the evidence is an OMISSION: the server completed a command without a response or field the protocol makes mandatory (a tagged OK without the untagged CAPABILITY / STATUS / QUOTA / QUOTAROOT / ACL / LISTRIGHTS / MYRIGHTS / METADATA / ID / NAMESPACE / SEARCH / ESEARCH it owes, a LOGOUT that succeeded without its BYE, SELECT/EXAMINE without FLAGS, EXISTS, or the revision's LIST/RECENT, a SELECT without UIDVALIDITY, a STATUS reply without the requested APPENDLIMIT, a tagged OK where a `+` continuation was owed). It shares `Protocol`'s display text and `ProviderContractViolation` recovery and differs in the account kind, `Protocol(MissingField)`, so diagnostics can tell "the server left something out" from "the server sent something wrong". It is NOT connection-fatal and is deliberately absent from `is_connection_fatal`: every producer raises it only once the exchange has completed (from a consumer's `finalize` on the command's own tagged OK, or from the account layer after the command returned), so the framing is intact and the next byte begins a fresh response. A producer that could raise it while the exchange is still open must use `Protocol` instead. The completion-time consumer failures are still real `Error`s in `Finalized::output`, so the driver makes the fatality decision before publishing. LOGOUT reads the tagged status first: RFC 3501 Section 6.1.3 owes the BYE only ahead of a tagged OK, so a tagged NO/BAD is the server's refusal and the answer, and a missing BYE is an omission only on a LOGOUT that succeeded.

Failures on the client side of the wire map to `Internal(_)` (`reference/error-model.md` owns its recovery: terminal `InternalFailure`, except `InFlight` on a non-idempotent operation, which reconciles), never to a provider fault. `Error::Internal` is a local invariant violation raised where the command framing is intact (before the first byte, or after the tagged completion), so it is not connection-fatal; it maps to `Internal(InvariantViolated)`. `Error::InternalMidExchange { message, attempt }` is a local invariant failure raised after bytes of the same exchange are on the wire: the SCRAM server signature missing from client state, the STARTTLS and COMPRESS refusals after the server's tagged OK (the stream is already swapped out), the post-STARTTLS CAPABILITY downcast, a literal continuation refused outside a pipeline, and the BYE digest without a status response. It also maps to `Internal(InvariantViolated)`, but the driver's view of the exchange can no longer be trusted, so it is connection-fatal, and its attempt state is REQUIRED (`Error::internal_mid_exchange(message, state)`), because some of the exchange was sent by construction and a missing state would read as `Unsent`. `Error::LocalRuntime` is a local facility failing at runtime (the system entropy source for a SCRAM nonce): neither the server's fault nor the caller's, not connection-fatal, `Internal(RuntimeFailure)`. `Error::DriverPanicked` maps to `Internal(Panicked)`; `Error::DriverGone` stays `Transport(Network)`. The phaseless `Error::driver_gone()` constructor is test-only: a missing phase reads as `Unsent`, so production mints through `driver_gone_at` or `observe_driver_panic`.

A driver death carries the caller's phase. `ImapConnection::observe_driver_panic(phase)` turns a dead driver into `DriverPanicked` (with the panic message) or `DriverGone`, and stamps `phase` on every outcome, including when another caller already took the join handle, since the evidence cannot depend on being first. Only the caller knows the phase: `Unsent` when the driver never claimed the command, `InFlight` once it had claimed it. Admission failures and post-admission withdrawals are also `Unsent`. The submit helpers in `connection/auth.rs` (`submit_regular`, `submit_streaming`, `submit_with_continuations`, `submit_upgrade`), `set_keepalive`, IDLE's entry and completion waits, and `Pipeline` execution all pass it, so a non-idempotent command the driver had claimed when it died reconciles rather than being declared failed, and a `DriverGone` no longer skips that reconciliation for lack of a phase. `PipelineError::Disconnected` carries the stamped error rather than being a bare variant.

IDLE is the case where the phase is easy to get wrong. `next_event` submits no command, so it stamps a driver death `Unsent`, which is not IDLE's phase. When `ImapConnection::idle` sees the event channel close (`DriverGone` / `DriverPanicked`), it first does a non-blocking `Pending::try_take` on the IDLE result channel: the driver answers the IDLE command before exiting whenever it can, so the real outcome (a BYE, an I/O failure) is usually already there and is returned as the error (or `ServerTerminated` for an `Ok`), instead of being replaced by a generic `DriverGone`. With no answer it withdraws the IDLE and restamps the death `Unsent` if never claimed, or `InFlight` once claimed. Inside the driver, IDLE's own reads (the `+` grant wait, the loop, the DONE write, the post-DONE drain) stamp transport failures `InFlight` for the same reason; an EOF mid-IDLE used to carry no phase and read as `Unsent`.

Local refusals made before the first byte follow the cross-crate rule in `reference/error-model.md` ("Local refusals") and each has its own variant, none of them connection-fatal: `InvalidInput` (caller input the encoder or a handle validator refuses) -> `Request(Malformed)`; `InvalidState` (a command the connection's local state does not permit - wrong session state, or UTF8=ONLY before `ENABLE UTF8=ACCEPT`) -> `Request(Malformed)`; `MissingCapability` (the server lacks an extension, or its revision removed the command, e.g. LSUB, STATUS RECENT or CHECK on rev2, or STATUS DELETED without rev2 or `QUOTA=RES-MESSAGE`) -> `Unsupported(op)`; `UnsupportedOperation` (a variant of a published `#[non_exhaustive]` request type this crate has not learned) -> `Unsupported(op)`; `StateChangedBeforeSend` (the driver's head-of-queue check found the session had moved) -> `ConcurrencyConflict` with `Unsent`. A session already in Logout is `Closed` with `Unsent` at either check, through the shared `helpers::state_refusal`. `MissingCapability` stays distinct from `UnsupportedOperation` because `should_disable_qresync` reads `MissingCapability` strings as evidence about the server.

The scope readers inside `classify_response_code` (`resource_from_scope`, `id_from_scope`, `mailbox_throttle`) must each accept BOTH mailbox scope shapes. Folder producers build `ErrorScope::Cursor(Folder(id))` via `with_folder_scope`, not `ErrorScope::Mailbox { id }` - `with_mailbox` is reached only from tests. A reader matching just the latter degrades on every real folder-scoped failure while its mailbox-scoped test keeps passing, which is exactly how `ThrottleScope::Mailbox` became unreachable (widening per-mailbox `[LIMIT]` to account-wide pauses) and how the folder id was dropped from `RequestCause::NotFound`. Both fixed and pinned folder-side; keep new readers symmetric.

The engine records a folder-scoped `[LIMIT]` as `ThrottleKey::Mailbox { account, mailbox }` over the folder's name, and reaches the work it should hold only through `Account::throttle_keys`: `ImapAccount` declares that same key for a folder scope (`ThrottleWork::Scope`, routed through `route_scope`, so a composed DAV collection answers for its sub-account and never as a mailbox), for each object in the folder (`ThrottleWork::Objects`, the folder decoded from the versioned object id) and for a folder membership that is not DAV-owned (`ThrottleWork::Membership`, a move destination or source). Every arm builds the key from the same `MailboxName::account_id()` `with_folder_scope` scopes the error with, so the recorded hold and the held work meet; `every_work_shape_on_a_folder_declares_the_key_its_limit_records` pins it. Before this declaration existed, the engine had no scope-to-mailbox mapping and a recorded per-folder hold reached no wait site at all.

ManageSieve carries its own response-code vocabulary (RFC 5804 1.3), parsed into `SieveResponseCode` and surfaced as `Error::Sieve { code, message }` - deliberately NOT folded into the IMAP `ResponseCode`, since recording a code the server never sent would corrupt the wire evidence support exports exist to preserve. The code rides out as `native_code`. `split_response_code` lifts the parenthesized code off the status line BEFORE the human-readable string, which is the ordering RFC 5804 specifies and the step whose absence caused the original defect: the code stayed inside the message as opaque text, so every rejection reached `fallback_status` and derived terminal `ProviderRefused`.

`classify_sieve` overrides that default only where it is actively wrong: `TRYLATER` -> `Server(Unavailable)` (retryable - it explicitly asks to be retried, and was previously permanent), `QUOTA[/MAXSCRIPTS|/MAXSIZE]` -> `Server(QuotaExhausted)` + account throttle, `NONEXISTENT` -> `NotFound(Filter)`, `ALREADYEXISTS` -> `ConcurrencyConflict`, `ACTIVE` -> `Request(Malformed)` (a precondition the caller must satisfy), `AUTH-TOO-WEAK` / `ENCRYPT-NEEDED` -> `Authorization(PolicyBlocked)`, `TRANSITION-NEEDED` / `SASL` -> `Authentication(ReauthorizationRequired)`, `REFERRAL` -> `Unsupported(op)`. An absent, advisory (`WARNINGS`, `TAG`), or unmodelled extension code keeps the terminal default - "the server refused and did not say why" - and `Other(_)` preserves the raw spelling for diagnostics.

CHECKSCRIPT is the one path where a `NO` is normally not an error: it IS the validation verdict, so it becomes `FilterValidation` diagnostics. `validation_outcome` makes the transient codes the exception - a `TRYLATER` means the server never compiled the script, so reporting it as a validation failure would tell the user their script is broken when it may be fine, and would discard the retryable classification.

A `Transport(_)` kind must never carry an `Acknowledged` attempt - `try_build` rejects the pair (`TransportAcknowledged`) and this boundary `.expect`s, so producing it panics. `into_account_error` demotes it to `InFlight` rather than dropping the cause, since dropping would leave `derive` on its `Unsent` default and blind-retry a non-idempotent operation. Nothing builds the pair today, but `Error::with_attempt` permits it on the transport variants and the driver stamps `Acknowledged` after a tagged response.

`Error::No` and `Error::Bad` carry `attempt: Option<ImapAttempt>`; `no_with_code`/`bad_with_code` default it to `Some(Acknowledged)` because a tagged `NO`/`BAD` is a server-acknowledged terminal response. Without this, recovery rows keyed on `Acknowledged` (e.g. `Server(Error { status: None }) + Acknowledged -> ProviderRefused`) collapse to the `Unsent` arm and misclassify provider refusals as retryable drops.

`ImapErrorContext::with_extra_text` appends support-only diagnostics the call site knows and the wire does not, WITHOUT touching the classification. The destroy path uses it: a UID EXPUNGE that fails after its `UID STORE +FLAGS.SILENT \Deleted` was acknowledged leaves a confirmed partial effect (those messages stay flagged `\Deleted`), but the failure itself keeps its own kind and cause chain. Collapsing it into `Protocol(PartialResponse)` would flatten an auth loss, an ACL denial, a quota throttle and a capability loss into one generic reconcile - the exact classification `reference/error-model.md` requires the producer to preserve.

### Push lifecycle

Dial, SELECT, NOTIFY registration and IDLE errors are classified as `PushStream` with the selected folder scope. Only `Retry` emits transport disconnects and uses the redial backoff. Every other class is latched as `WatchEvent::Terminated` for late or lagged readers, and the failed worker releases its connection and parks until subscription reconfiguration or shutdown. A replacement subscription clears the latch. Deleted, renamed and revoked folders therefore do not cause timer-driven redials. The slot remains alive so reconfiguration can assign it another folder.

Events published while no `push_stream` receiver exists are retained in a
bounded queue and delivered when a receiver attaches. If that queue fills, the
pending history collapses to one account-wide coalesced invalidation and the
latest connection state.

The IDLE workers are account-owned, not subscription-owned. With NOTIFY one worker covers the selected folder plus every other accepted folder through `NOTIFY SET`; without it the account starts one worker per unit of carved-out push capacity, each covering one folder. At zero push capacity no worker starts. Defaults allow two push sessions without NOTIFY or one with it, inside four physical sessions. At the default non-NOTIFY push ceiling, two sessions remain for data. With no push subscribed, data checkouts may use every session `pool_cap` allows; push establishment then depends on retiring a parked data member or waiting for a checked-out one to come back. A worker without an assignment parks. Each push session holds both a shared session permit and a push slot through driver exit, so reassignment and reconnect tails cannot consume the last data slot. A redial waits for the previous push driver's LOGOUT tail; NOTIFY reassignment serializes behind that single slot.

Push always dials fresh and never adopts or parks a data member. A previously selected data connection may carry unsolicited mailbox-less FETCH, VANISHED or EXISTS events; adopting it would absorb those into the wrong folder's cache. To make room, fresh acquisition retires a parked member or waits for a checked-out member to return. That wait may be as long as a data stream; polling continues meanwhile. Capacity and worker topology are fixed at open from the admission profile, even if a later dial advertises different capabilities.

Acquisition has two phases. During the capacity wait, subscription changes re-choose the assignment while preserving the same pending reservation, or drop it if no folder remains. During dial and authentication, only shutdown and cancellation interrupt; a subscription change never wastes a dial, and the assignment is re-chosen before SELECT. A session that waited for either a push slot or shared capacity publishes a per-folder invalidation after SELECT and NOTIFY handling for its selected folder and, if registration succeeded, every other watched folder. Immediate acquisition emits no coverage invalidation. The invalidation compensates for the latency window the wait stretched (polling already covers it; bifrost-sync never suppresses polling on push coverage); same-session re-IDLE does not emit it. A wait, and so this invalidation, is routine in two accepted places: a reassignment redial usually waits for its own previous session's LOGOUT tail and publishes right after the account-wide `Unknown` of the interrupt (redundant, harmless), and a first push session opened while discovery and inventory hold every data session publishes right after subscribe.

Admission is per scope, under the same lock as the insert, so two concurrent subscribes cannot both take the last slot. A folder already pushed costs no new session and is always admitted; without NOTIFY, a new distinct folder is admitted in request order while `covered.len() < push_capacity`; nothing is admitted at zero push capacity. Admission accepts exactly what worker assignment can watch: a folder scope whose name fails `MailboxName::new` (NUL/CR/LF) is refused into the failed lane, because `subscribed_idle_folders` drops such names and admitting one would report it as pushed while burning a slot on a mailbox no worker can SELECT. Every admitted folder always has a worker, because without NOTIFY admission caps the admitted set at the push capacity, which is also the worker count - but the folder-to-slot *index* is recomputed lexically from the whole subscribed set on every wake, so a folder's slot number is not stable across subscription changes even though its coverage is. When push capacity is exhausted, `push_subscribe` puts excess scopes in `PushSubscription.outcomes.failed` as `Unsupported(PushSubscribe)` carrying budget-specific diagnostic text; a subscribe that accepts nothing returns no handle. bifrost-sync records only the succeeded scopes and keeps the rest in the polling lane - it never suppresses polling on push coverage, which is what keeps a refused folder visible. Unsubscribe frees a folder slot once its last handle leaves.

Workers are woken by a `watch` generation counter rather than a `Notify`. With several workers, `notify_one` would wake one and leave the rest on a stale assignment, and `notify_waiters` stores nothing, so a worker in the window between finding no folder and awaiting the wake would park forever. The `watch` generation latches; each worker marks it seen *before* reading the scope set, so a change landing in that window still wakes it.

IDLE alone reports only the SELECTed mailbox. On NOTIFY servers, `NOTIFY SET` (RFC 5465 Section 3) extends that one session to every accepted folder: `(selected (MessageNew MessageExpunge FlagChange))` plus the same events under a `mailboxes` filter naming every other watched folder. Events on those arrive as STATUS responses (Section 4), which `map_idle_event` turns into per-folder invalidations. The registration asks for no fetch attributes and no initial STATUS snapshot. A session that waited for capacity publishes its own per-folder coverage invalidations; the immediate-acquisition path still avoids a snapshot burst. Servers without NOTIFY use the bounded worker pool above. A server that advertises NOTIFY and has positive push capacity admits its whole requested scope list because one session covers it, so a runtime `NOTIFY SET` rejection leaves the account with one worker and push for the selected mailbox only, while `push_subscribe` has already reported every scope as succeeded.

That report cannot be retracted - `BatchOutcome` is settled and returned before the first dial, and the `Account` surface has no per-scope demotion event - so the collapse is handled the way `reference/sync.md` requires a missed push to be handled: it degrades to a coarser invalidation rather than to silence. `register_notify` returns whether the registration took, or an error that the worker classifies and forwards unless it is Retry. A missing NOTIFY capability and retryable registration failures retain the coarse coverage fallback, and `signal_notify_coverage_loss` turns a `false` on a NOTIFY-advertising account into one account-wide `HintPayload::Unknown`. Registration is re-attempted on every redial and on every resubscribe-interrupted round, so the uncovered folders keep receiving a coarse invalidation at the IDLE cadence instead of nothing. It is gated on the account-level NOTIFY capability deliberately: without NOTIFY the account runs one worker per admitted folder and a `false` means only "this worker had no others to fold in", so firing there would invalidate the whole account once per IDLE round on every non-NOTIFY server. `register_notify` still logs the warning naming how many folders the rejection leaves unpushed, and those folders remain polled, since polling is never suppressed on push coverage.

A scope-set change cancels the in-flight IDLE round through a child token so the folder choice can be re-evaluated. `ImapConnection::idle` gives cancellation strict priority over queued server events, so the round returns `Cancelled` while events may still be queued. The DONE drain itself does not throw those away - `drain_idle_responses` emits them to the event sink and they survive into the next `idle()` round - so what loses them is dropping the connection, not the cancellation.

The interrupt therefore re-chooses BEFORE deciding (`resubscribe_action`, pure). When the new scope set assigns this slot the same mailbox - the common case, since subscribing or unsubscribing some other folder does not move slot 0 - the worker re-IDLEs on the SAME connection: no redial (a full connect+auth+SELECT per subscription change is what providers with per-user connection-rate limits punish), no coarse invalidation (nothing was lost), and the event the round did return is published normally. `NOTIFY SET` is re-issued for the new watched set on that same session. Only a different assignment, or none, breaks out to redial, and only that path absorbs the round's event and emits the coarse `HintPayload::Unknown` invalidation (`signal_idle_interrupt_loss`). A `BYE` or server termination that lands in the interrupted round is not a re-IDLE candidate at all: it takes the ordinary closed-connection path (`Disconnected`, redial with backoff), because the socket is gone regardless of what the scope set now says. Reconfiguring subscriptions degrades to a broader reconcile, never to silent push loss for the scopes that stayed.

The DONE handshake carries the account's `command_timeout`, the same bound every other command has. A peer whose TCP is alive but which never answers the tagged OK for IDLE would otherwise park the push loop past cancellation, past `account.shutdown`, and past `close()`. The bound turns that into an `Error::Timeout`, a `Disconnected` event, and a redial.

Every connection minted by the pool is registered with a weak connection handle and a shared driver-task owner, including parked, checked-out, and dedicated IDLE sessions. `Pool::close` sets the closed flag, closes the session semaphore, clears member waiters, drains the parked list and registry, concurrently requests LOGOUT on every live connection under one `command_timeout`, then joins each driver. Drivers whose connection handles already disappeared are already running the driver's bounded best-effort LOGOUT; close waits for it up to `LOGOUT_DRAIN_TIMEOUT` and aborts and joins on expiry.

Account close records admission separately from completion. The first polled
close starts one owned teardown task; concurrent and later callers await its
shared result, including any composed-account error. Dropping a caller cannot
cancel the pool registry drain. All three send entry points hold a shared gate
and refuse a closed account before rendering, fetching a draft, or submitting.
Teardown takes the exclusive gate, allowing admitted sends to finish, shuts down
the owned SMTP submission pool, then closes IMAP and the composed accounts.
An unpolled close future starts no work. Direct `Pool::close` also shares one
owned teardown task, so dropping a direct caller cannot cancel its drain.
Each dial runs in an owned task holding read admission through connect,
authentication and QRESYNC initialization. Close shuts admission and cancels
in-flight dials, waits for their cleanup through exclusive admission, then
drains registered sessions. Dials observe receiver abandonment during connect
and authentication and terminate any unclaimed driver before releasing
admission. Before spawn, a handoff clone keeps leases alive until the connector
future and transport are destroyed. At spawn they move into the driver exit
slot, so abandoned receivers, cancelled authentication and successful handout
all retain capacity until transport destruction. Push slots follow that same
exit lifetime.
Connect and authentication retain their configured timeouts; OAuth token
lookup is also bounded by the authentication command timeout.

The `registry` mutex is the linearization point that protects pool registration against a completed close. `close` stores the flag before it drains; registration re-reads the flag while holding that same lock. A dial that reaches the lock first is seen by the drain; one that finishes after the drain sees the flag, is refused registration, and tears its own connection down. Both acquisition paths re-check the flag after receiving capacity. Close closes the session semaphore and clears the member waiter queue under the idle lock; registration of a waiter checks the same flag under that lock. Blocked acquisitions receive `Error::Closed`.

The return side takes the matching linearization under the idle lock. Returning a reusable member checks `closed` while holding that lock, before either handing to the oldest waiter or parking. A return either lands before the drain and is drained, or observes the flag and parks nothing. Cancellation removes a waiter synchronously and returns an already handed member through the same FIFO path, so dropping an acquisition cannot strand capacity.

Registration prunes completed driver tasks. A missing connection handle alone does not make a registration stale: the driver may still be completing its bounded best-effort LOGOUT, and the registry keeps its join handle until that finishes. This also bounds registry growth when a non-NOTIFY push worker reconnects across a network outage.

A failed dial or SELECT backs off from 5s, doubling to a 300s ceiling, and resets on the first successful SELECT. The wait races the cancel and shutdown tokens rather than sleeping through them, and `Disconnected` is emitted once per disconnection rather than once per failed attempt.

### Concurrency conflicts and UNCHANGEDSINCE

Mutation preparation uses a plain read-write SELECT with no QRESYNC baseline:
it needs the live UIDVALIDITY, not all changes since the last sync. It neither
replays VANISHED history nor warms the MODSEQ cache from that SELECT. Absent a caller version, existing
cached MODSEQs remain opportunistic STORE guards. They can advance through push
or reads ahead of the consumer; they are not caller-version protection. MODIFIED response sets
stay compact; per-item classification tests only the submitted UIDs for
membership, even when the server names a very broad range. Because MODIFIED
does not include the mailbox maximum or preserve endpoint order, any `*` bound
is conservatively mapped to the full possible UID space.

IMAP diverges from the `MutationSuccess::Skipped` lane other crates use for the engine's "already in this state" short-circuit: the opportunistic MODSEQ cache sends cold-cache STOREs without `UNCHANGEDSINCE`, so "already in state" cannot be observed without a full SELECT+FETCH that defeats the guard. So `STORE UNCHANGEDSINCE <modseq>` rejecting with `MODIFIED` surfaces as `ItemOutcome::Failed { kind: ConcurrencyConflict }` for the conflicting UIDs, never `Succeeded(Skipped)`.

`StoreConsumer::new` takes the command's own `unchanged_since` and has no argument-less form, so how a tagged `NO` is treated is a property of the command rather than a per-call-site choice. Under `UNCHANGEDSINCE` the consumer preserves tagged `NO` as a `StoreResult` with its status and response code, making the legal `NO [MODIFIED ...]` CONDSTORE rejection available to the mutation layer instead of discarding its per-UID conflict evidence. Without `UNCHANGEDSINCE` a tagged `NO` is a plain refusal and stays `Error::No`: the unconditional STORE callers (`delete_messages` marks `\Deleted` and then EXPUNGEs) read `Ok` as acceptance, and a silent success there would expunge messages another client had marked. Tagged `BAD` is a command error on both paths. `StoreWireOutcome::from_store_result` takes the whole result rather than a `(code, tagged_ok)` pair, so the status cannot be dropped on the way to classification. `concurrency_conflict_error` / `store_failed_error` / `uidvalidity_changed_error` each take the caller's `AccountOperation` and produce `ErrorScope::Cursor(Folder(id))`, matching the folder-producer scope contract above. A STORE refusal without a more specific response code is `Server(Error)`, not the client-bug `Request(Malformed)` classification.

Mutation error lanes follow transmission evidence. An `InFlight` attempt makes the affected items `Uncertain`; `Unsent` or `Acknowledged` evidence makes them `Failed`. This rule is shared by grouped STORE, MOVE, and EXPUNGE error paths, so the loop that observes an error does not decide whether the server may have applied it.

### Strategy downgrade derivation

`EngineDirective::DowngradeStrategy` is reserved for "all strategies exhausted", which IMAP never derives at runtime: `changes.rs` handles every QRESYNC -> CONDSTORE -> Basic downgrade inline via `Warning::StrategyDowngraded` plus a direct retry on the lower strategy. The `strategy_failure` helper is funnel-wired and tested so the directive is producible, but no runtime path reaches it while Basic is the universal fallback.

### Per-folder mutation failure contract

`mutate::mutation_stream` emits no trailing global `SyncEvent::Terminated` for a recoverable folder failure: a preparation failure surfaces as one per-item outcome for every target in that folder (carrying the classified `AccountError`; `Uncertain` on `InFlight` evidence, `Failed` otherwise), and the loop continues to the next folder. Preparation-command evidence is restamped `Unsent` relative to the mutation, since the STORE / MOVE / EXPUNGE has not been sent. Shared-folder permission denials are first converted to scoped `ScopeRevoked`, so they remain per-folder failures. Stream-level `Terminated` is reserved for failures that prevent any further folder attempt (personal authorization loss, schema/capability break); `stream_terminating` is the gate. The decoded ids are moved into `run_folder_mutation`, not cloned per batch: every failure it can raise (`FolderMutationFailure`) happens in `open_folder_for_mutation`, before a single outcome is minted, so the error hands the whole id vector back and the fallback lane mints from that. Once the folder is open, the per-group paths are infallible by type - they return outcomes, not `Result` - which is what makes moving the ids in safe.

Inventory commits its in-memory cursor only after both its final batch (when
nonempty) and terminal checkpoint have entered the output channel, matching
the changes path. A failed send leaves the previous cursor hint intact.

### Mutation batch shape

Every per-item `mutation_stream` outcome rides a batch sized by the same
`TARGET_BUFFER_ITEMS` window, on all three lanes; no lane emits one batch per
id. A stream-level `Terminated` is the account-wide exception described
above: it ends the stream before later groups or input windows can be answered.
The grouped lane flushes one batch per folder group per window. The two
lanes that never reach a folder are accumulated to the same window and flushed
through `flush_items`: ids rejected by `validated_move_destination` (a bulk move
whose destination is not a folder scope, refused before any source folder is
opened), and ids `decode_object_id` refuses, which are buffered alongside the
decodable ones and emitted just before each grouped flush. Undecodable ids count
toward `buffered`, so an input that is entirely undecodable still flushes on the
window rather than growing without bound behind a folder group that never fills.
`flush_items` sends nothing when its accumulator is empty, so a flush point costs
no send unless it has something to say.

Batch shape carries no meaning downstream: bifrost-sync's mutation consumer keys
per-item outcomes by `ObjectId` and reads `BatchOutcome`'s own submission-order
index, never batch position or batch size. The invariant that holds is per-id:
every submitted id is answered exactly once, across all three lanes.

### Output-channel-dropped contract

Every streaming task (`inventory_stream`, `changes_stream`, `get_stream`, `open_raw_rfc822`, `mutation_stream`) treats a `tx.send` failure on a dropped output receiver as silent termination: the task returns without synthesizing any `crate::Error` or fatal `Terminated`. The error funnel is reserved for wire failures and structural invariant breaks; a consumer walking away from its stream is not an error.

### Terminated-event helper

`account/mod.rs` exposes one `terminated_event::<T, _>(cause)` helper for surfacing fatal stream causes as `SyncEvent::Terminated`. It accepts any `Into<TerminatedCause>`: a pre-built `AccountError` (UIDVALIDITY change, modseq reset, pre-classified failures - including the shared-folder `ScopeRevoked`), or an `(Error, ImapErrorContext)` pair to classify on the way out (the `fatal_event(err, ctx)` alias). Callers do not choose a lane.

`SyncEvent::Terminated` ends the stream, so it is never followed by `Done`. Statically unsupported blob streams emit that single terminal event.

## Module layout

```
crates/imap/src/
|-- codec/           - parser + encoder (nom 8)
|-- connection/      - driver, auth, lifecycle, dispatch
|   |-- auth.rs      - PLAIN/LOGIN/XOAUTH2/OAUTHBEARER/CRAM-MD5/SCRAM wire
|   |-- config.rs    - ImapConfig, internal account dial path
|   |-- dispatch.rs  - command dispatch, FETCH consumer
|   |-- driver/      - driver task
|   |-- ergonomics.rs - uid_fetch_each, _limited, _full_messages
|   |-- helpers.rs   - server_profile, drain_events, ...
|   |-- lifecycle.rs - greeting, STARTTLS, ENABLE
|   |-- pipeline/    - command pipelining
|   |-- reply.rs     - queued/claimed/withdrawn admission arbiter
|   |-- seq_ops.rs   - sequence-number command surface
|   `-- uid_ops.rs   - UID command surface
|-- account/         - bifrost_types::Account implementation
|   |-- blob.rs            - open_raw_rfc822 (BODY.PEEK[]); no blob openers
|   |-- capabilities.rs    - AccountCapabilities builder
|   |-- changes.rs         - QRESYNC / CONDSTORE / Basic diff dispatch
|   |-- close.rs           - graceful shutdown
|   |-- envelope.rs        - FolderCursor encode/decode
|   |-- factory.rs         - ImapAccountConfig, QRESYNC negotiation
|   |-- folder_registry.rs - mailbox map, cursor cache, MODSEQ cache
|   |-- get.rs             - hydration
|   |-- inventory.rs       - inventory + initial cursor establishment
|   |-- mod.rs             - ImapAccount trait impl, helpers
|   |-- mutate.rs          - bulk_set_flags, bulk_move, bulk_destroy
|   |-- parts.rs           - BODYSTRUCTURE part traversal + part-handle codec (unwired)
|   |-- pim.rs             - unified PIM primitives + conveniences
|   |-- pool.rs            - per-folder connection checkout
|   |-- push.rs            - IDLE-driven WatchEvents
|   |-- sieve.rs           - ManageSieve script filters
|   `-- scopes.rs          - folder discovery, lifecycle stream
|-- types/           - internal protocol types plus public Credentials / AuthPolicy
`-- error.rs         - internal Error, ErrorCategory, Recovery
```

## IMAP-specific code style

- Connection methods take `&self`.
- Internal raw protocol operations take an explicit `Duration` or document the timeout policy.
- Public enums/structs are limited to the factory/config surface and are `#[non_exhaustive]` absent a strong reason.
- Credentials and SASL intermediate strings use `Zeroizing` and redact under `Debug`; malformed SASL mechanism names are rejected before any wire write.

## Unsupported error construction

Local refusal helpers use `AccountError::unsupported` through the shared error
funnel, retaining their provider, scope and diagnostic decoration.
`Account::protocol` returns `Protocol::Imap`, including for inherited
default-method refusals.

## Caller versions on contact and event updates

The shared `ContactPatch` and `EventPatch` carry optional `expected_version`.
The composed DAV legs forward these patches intact and advertise their
`ExpectedVersion` update and conditional delete capabilities when present.
`contact_delete_if_version` and `event_delete_if_version` delegate to the
composed account with the caller version unchanged.

`bulk_set_flags_if_version` accepts `MailMutationTarget` with a positive
`ServerVersion::ModSeq`. `UNCHANGEDSINCE` comes directly from that target,
regardless of the MODSEQ cache. A `MODIFIED` rejection is a per-target
`ConcurrencyConflict`. Mail flag concurrency is advertised when CONDSTORE is
supported (including QRESYNC implication); a selected NOMODSEQ mailbox is
refused before STORE. Two-sided `FlagOp::Patch` is refused with a version:
its second STORE cannot reuse the caller MODSEQ after the first write, and
sending it unguarded would lose the atomic guarantee. MOVE and destroy likewise
refuse caller versions because their complete operation has no atomic MODSEQ
precondition. No-version operations retain the cached guards and existing
compound behavior.
