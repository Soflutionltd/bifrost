# Spec: withdraw IMAP commands whose caller left before the driver claimed them

Technical implementation specification for the `bifrost-imap` deferred finding
"Commands whose caller has gone are still executed" (IMAP-011 in
`notes/bugs-deferred.md`). Transient: it lives in `notes/` and dies when the
work lands.

## Standing references (read them, do not just note them)

Implementers and reviewers must READ these before touching code. They are the
ground this is built on and judged against.

- `reference/technical-implementation-spec.md` - the contract this document is
  written against.
- `reference/error-model.md` - the cross-cutting `AccountError` contract. This
  work changes transmission evidence (`Unsent` / `InFlight`), which is exactly
  what `recovery::derive` reads to choose retry versus reconcile ("Central
  derivation", the Transport row, and "Cause chain").
- `reference/imap.md` - in particular "Driver-owned I/O", "One command path",
  "Connection state machine", the cancellation paragraph that closes the
  untagged-routing section, the driver-death paragraph of the error section,
  the pool bullets, and "Bandwidth metering".
- `notes/bugs-deferred.md`, entry IMAP-011 - the source item.

## The source item, and how this spec reads it

The entry says: `driver_task`'s `Run` arm runs `prepare_command` and sends
whatever it dequeues without checking whether the caller is still there, so a
command queued behind a slow one whose caller timed out (stamped
`timeout_inflight`) is still written later, including non-idempotent APPEND /
MOVE / STORE; skipping a command that has not reached the wire would turn an
`InFlight` reconcile into a true `Unsent` and close a duplicate-append window.
The owner ruled it needs a spec because it changes the documented cancellation
contract.

Two corrections to the item's mechanism, both verified against the code:

1. **A driver-side `result_tx.is_closed()` check alone does not deliver the
   item.** The caller has already returned `Error::timeout_inflight()` by the
   time the driver notices; skipping the command makes the world `Unsent` while
   the caller reported `InFlight`. That still closes the duplicate window (a
   reconcile reads back "absent" and that answer stays true), but it does not
   turn the reconcile into a retry, which the item also claims. Reporting
   `Unsent` truthfully needs the caller to KNOW, at the moment it gives up,
   that the driver will never write the command. That is a two-party
   decision, so it needs a single atomic arbiter shared by caller and driver:
   a claim / withdraw handshake. This spec builds that.
2. **The timeout cannot stay at the call sites.** Every command method today
   wraps its submit helper in `tokio::time::timeout(..)` and maps expiry to
   `Error::timeout_inflight()` blindly. The call site cannot see which phase
   the submission was in, so the stamp is wrong in three distinct cases, not
   one: expiry while waiting for channel capacity (never submitted), expiry
   while queued (never claimed), and expiry after the driver claimed it. Only
   the last is a case in which `InFlight` cannot be ruled out; it is
   conservative there, not established (the driver may still be waiting on
   the bandwidth budget before its first byte, see the stopping rule, or be
   about to refuse in `prepare_command`). The deadline moves into the submit
   helpers, which are the only code that can see the phase.

### Reachability (why this is a live defect, not a theoretical one)

Exclusive pool checkouts make concurrent commands on one connection rare, but
a command does queue behind a still-running one in production:

- `account/mutate.rs` `run_destroy_mutation_groups` and
  `run_flag_mutation_groups` loop over MODSEQ groups on ONE `PooledConn`. When
  a group's `uid_store` times out, the loop records the group's outcome and
  `continue`s, submitting the next group's STORE on the same connection while
  the driver is still inside the timed-out one. That STORE queues, times out
  in turn (stamped `InFlight`, settled `Uncertain`), and is then written to the
  server later, after the engine has already been told its outcome is unknown
  and may have read back "not applied".
- `account/pool.rs` `PoolInner::drain` submits LOGOUT to every registered
  session, including checked-out ones mid-command, under one
  `command_timeout`.
- Driver death with commands still buffered in the channel: after a
  connection-fatal error the driver closes `cmd_rx` and leaves its loop
  (session state `Logout`), dropping every buffered command. Each such caller
  sees its oneshot drop and stamps `InFlight` (`observe_driver_panic`), though
  the driver never touched the command.
- Expiry while `cmd_tx.send` waits for a slot (the channel is bounded):
  never submitted, stamped `InFlight`.
- Direct `ImapConnection` users who share one connection across tasks (the
  connection API is published; nothing forbids it).

## Survey of the ground

Current structure and everything depending on it.

- `connection/driver/mod.rs`
  - `DriverCommand::{Run, Upgrade, Pipeline, Idle}` each carry a bare
    `oneshot::Sender` (`result_tx`). `SetKeepalive` and `PeerCertificate` do no
    wire I/O and also carry one.
  - `driver_task` dequeues and executes unconditionally. `publish_then_answer`
    publishes the state snapshot, then answers the oneshot. Its doc discusses
    `result_tx.send` and records a ruling that making answer-without-publish
    unrepresentable was not worth the machinery.
  - After a connection-fatal error: `apply_infrastructure_failure` (session to
    `Logout`), `cmd_rx.close()`, then the `Logout` check breaks the loop and
    buffered commands are dropped unexecuted.
  - `run_prepared_command` routes `Either`-classified untagged responses
    (EXISTS, RECENT, FETCH, EXPUNGE, ...) to the running command's consumer;
    they reach the event queue only at that command's tagged completion
    (`reclassified_as_events`). Only `OnlyUnsolicited` / `Impossible`
    responses, and critical response codes (ALERT, NOTIFICATIONOVERFLOW,
    emitted by `process_untagged_prefix` before classification), reach the
    event queue while a command is still open. This matters for the IDLE
    tests below.
- `connection/driver/mod_tests.rs` `command_answer_follows_state_publication`
  builds a raw `oneshot` and calls `publish_then_answer` with it.
- `connection/auth.rs` submit helpers `submit_regular`, `submit_streaming`,
  `submit_with_continuations`, `submit_upgrade`: send, arm `InFlightGuard`
  only after the send succeeds, await the oneshot, call `guard.completed()`
  before matching on the received result (so a dropped oneshot does NOT mark
  the connection abandoned); a dropped oneshot is
  `observe_driver_panic(InFlight)`. None takes a deadline.
- `connection/idle.rs` `idle()`: awaits `cmd_tx.send` BEFORE its deadline
  exists and before the cancel token is polled; arms the guard; selects over
  cancel / `next_event` / `result_rx`; then DONE under `done_timeout` (its
  expiry is a genuine `InFlight` once the IDLE is on the wire). Three
  completion/death branches: the `next_event` driver-death arm (stamps
  `InFlight` when `try_recv` finds no answer), the `result_rx` arm, and the
  DONE wait. All three assume a submitted IDLE is owned by the driver, and the
  in-body comments say so ("The IDLE exchange is the driver's from here"; the
  death arm's "the send above succeeded, so it may have been written").
- `connection/pipeline/mod.rs` `Pipeline::execute_raw`: `require_state` on
  every command, then guard, send, await. The guard is armed BEFORE `send`, so
  dropping a pipeline that is still waiting for channel capacity marks the
  connection abandoned today; strict admission below changes that (a behaviour
  change the reference rewrite names). No deadline (only test callers today;
  the builder is crate-internal).
- `connection/mod.rs`: `abandoned: AtomicBool`, `InFlightGuard` (doc: "Guard
  around one submitted command"), `in_flight()`, `is_reusable()` (alive and
  not abandoned), `Drop for ImapConnection` (abort the driver when abandoned).
- `connection/config.rs` `setup_keepalive_cancel_safe` uses `InFlightGuard`
  around `SetKeepalive` so a cancelled direct setup aborts the driver on last
  drop. No wire bytes are involved.
- `connection/lifecycle.rs`: `observe_driver_panic` (doc states the
  send-succeeded rule this spec replaces); `peer_certificate_der` (unbounded
  `send` and `result_rx.await`, no wire I/O).
- `error.rs`: `Error::timeout_inflight()` (doc claims "the command bytes have
  already been sent ... by the time the outer timeout fires", false for a
  queued command), `Error::timeout()`, `with_attempt` (a no-op for variants
  that cannot carry evidence, including `StateChangedBeforeSend`, whose
  variant IS its `Unsent` evidence), `attempt()`, `driver_gone_at` and the
  test-only `driver_gone` (both docs state "never received" / "owned").
- Call sites mapping expiry to `timeout_inflight()`: `connection/mailbox.rs`,
  `extensions.rs`, `uid_ops.rs`, `auth.rs` (login, every AUTHENTICATE method,
  `complete_auth`, `unauthenticate`), `append.rs`, `sort_thread.rs`,
  `helpers.rs`, `seq_ops.rs`, `ergonomics.rs`, `lifecycle.rs` (STARTTLS),
  `idle.rs` (DONE). `grep -rn timeout_inflight crates/imap/src` is the
  authoritative list at implementation time. `logout()` and
  `fetch_capabilities_via_driver` submit with no deadline of their own.
- Direct test calls to the submit helpers (they gain a parameter):
  `connection/tests.rs` `invariant_driver_panic_surfaces_as_error` and
  `invariant_driver_panic_subsequent_commands_fail` (`submit_regular`, three
  calls), `a_scram_server_final_error_cancels_and_leaves_the_connection_reusable`
  (`submit_with_continuations`), `a_refused_select_leaves_no_pending_selection_behind`
  (`submit_regular`); plus `connection/tests.rs`'s direct
  `DriverCommand::Pipeline` construction.
- `connection/append.rs`: `remaining_timeout` stamps an already-expired
  deadline `Unsent` before submission; `unsent_preflight` restamps the
  APPENDLIMIT STATUS preflight `Unsent`. `remaining_timeout` uses
  `checked_duration_since`, which returns `Some(ZERO)` when the deadline
  equals now; today that submits the APPEND under a zero timeout and stamps
  `timeout_inflight()`. Deleting the function removes this, and it confirms
  that the zero-timeout test below bites against today's code.
- `connection/uid_ops.rs` `uid_move_messages`: without MOVE but with UIDPLUS
  it runs COPY, then `STORE +FLAGS.SILENT \Deleted`, then UID EXPUNGE, and
  propagates the failing step's error (and its evidence) as the outcome of
  the whole MOVE. The account layer (`account/mutate.rs`, bulk move) settles
  that error per target: `Unsent` evidence settles `Failed` (retryable),
  `InFlight` settles `Uncertain` (read back). A retry repeats the COPY.
- `account/mod.rs` `guarded_receiver_stream` doc comment argues the abort of
  a stream task is safe BECAUSE every submit helper's `InFlightGuard` marks
  the connection abandoned.
- `account/pool.rs` `enable_qresync` treats any non-NO/BAD error as "retire"
  (doc: "a timeout leaves it abandoned, with the driver still inside the
  ENABLE"); `PooledConn::drop` parks only `is_reusable()` members;
  `PoolInner::drain` wraps concurrent `logout()` calls in one external timeout
  and then terminates every session (no evidence is read from those calls, so
  a withdrawn LOGOUT there is harmless). `PooledConn::replace_selected_member`
  also calls `logout()`, with no bound; so does the push loop in
  `account/push.rs` on cancellation.
- Composite operations (several commands, one operation-level result):
  `uid_move_messages`' UIDPLUS fallback, `complete_auth`, `unauthenticate`,
  `run_starttls_upgrade`, `run_patch_mutation_group`,
  `run_destroy_mutation_groups`. Their partial-completion reporting is a
  separate defect class; see the composite safeguard below for what this
  landing must do about the triggers it adds, and the stopping rule for the
  rest.
- Tests that pin the current shape and must keep passing (or are rewritten
  below): `dropping_a_command_waiting_for_channel_capacity_does_not_abandon_connection`,
  `an_idle_whose_done_times_out_is_abandoned_and_its_last_drop_releases_the_socket`,
  `an_idle_cancelled_through_its_token_stays_reusable`,
  `idle_done_handshake_is_bounded_when_the_server_never_completes`,
  `cancellation_during_keepalive_setup_marks_the_connection_abandoned`,
  `a_queued_command_is_refused_when_the_session_left_its_legal_states`.

## Target design

### The admission arbiter: `connection/reply.rs` (new)

One shared cell per submitted command, held by the caller's handle and the
driver's reply. It carries the admission state AND the command's payload, so
that whichever party wins the state transition is the only one that can take
the payload. Three states:

```rust
const QUEUED: u8 = 0;
const CLAIMED: u8 = 1;
const WITHDRAWN: u8 = 2;

/// Shared by both halves. Only the winner of the CAS on `state` ever touches
/// `payload`, so the mutex is uncontended; it exists to make the cell `Sync`.
struct Admission<P> {
    state: std::sync::atomic::AtomicU8,
    payload: std::sync::Mutex<Option<P>>,
}

/// Driver half. Travels inside the `DriverCommand`. Holds no payload of its
/// own: the payload is reachable only by winning `claim`.
pub(super) struct Reply<P, T> {
    tx: tokio::sync::oneshot::Sender<T>,
    admission: std::sync::Arc<Admission<P>>,
}

/// What the driver holds once it owns the command. The ONLY way to answer.
pub(super) struct ClaimedReply<T> {
    tx: tokio::sync::oneshot::Sender<T>,
}

/// Caller half. Lives in the submit helper's frame.
pub(super) struct Pending<'conn, P, T> {
    rx: tokio::sync::oneshot::Receiver<T>,
    admission: std::sync::Arc<Admission<P>>,
    abandoned: &'conn std::sync::atomic::AtomicBool,
    /// Set once the exchange is over from the caller's side: an answer was
    /// taken (`recv`, `try_take`), or `recv` reported the reply dropped.
    settled: bool,
    /// Set once the receiver has reported closure, so later operations do
    /// not poll a terminated oneshot.
    closed: bool,
}

pub(super) enum Withdrawal {
    /// The state is WITHDRAWN (this call won the CAS, or an earlier call
    /// did): the driver will never write this command, and the payload has
    /// already been dropped in the caller's frame.
    Withdrawn,
    /// The driver claimed it first; it is or will be executed.
    Claimed,
}

pub(super) enum Unanswered {
    /// The reply was dropped while still QUEUED (driver gone before claim).
    NeverClaimed,
    /// The reply was dropped after the driver claimed it.
    Claimed,
}

pub(super) fn reply_channel<P, T>(
    payload: P,
    abandoned: &std::sync::atomic::AtomicBool,
) -> (Reply<P, T>, Pending<'_, P, T>);

impl<P, T> Reply<P, T> {
    /// CAS QUEUED -> CLAIMED, then take the payload. `None` means the caller
    /// withdrew (and already dropped the payload); the driver drops the
    /// shell without writing a byte and without answering.
    pub(super) fn claim(self) -> Option<(P, ClaimedReply<T>)>;
}

impl<T> ClaimedReply<T> {
    pub(super) fn answer(self, value: T); // oneshot send, result ignored
}

impl<P, T> Pending<'_, P, T> {
    /// Await the answer. Cancel-safe (a dropped `recv` future loses nothing).
    /// On closure, `withdraw()` decides `NeverClaimed` versus `Claimed`, and
    /// the pending is marked settled either way: the driver is gone, so there
    /// is no exchange left for `Drop` to abandon.
    pub(super) async fn recv(&mut self) -> Result<T, Unanswered>;
    /// Non-blocking take; marks the pending settled when it returns `Some`.
    /// Safe after closure (returns `None`).
    pub(super) fn try_take(&mut self) -> Option<T>;
    /// CAS QUEUED -> WITHDRAWN; on winning, take the payload out of the cell
    /// and drop it here (after releasing the lock). An observed WITHDRAWN is
    /// `Withdrawn`, an observed CLAIMED is `Claimed`. Idempotent.
    pub(super) fn withdraw(&mut self) -> Withdrawal;
}

impl<P, T> Drop for Pending<'_, P, T> {
    /// If settled: nothing. Otherwise withdraw. `Withdrawn` leaves the
    /// connection alone. On `Claimed`, an answer already sitting in the
    /// oneshot (`try_take`) means the exchange finished and the connection is
    /// left alone; otherwise the driver is still inside an exchange nobody
    /// will read, and `abandoned` is set.
    fn drop(&mut self);
}
```

The implementation may erase `P` on the caller side (for example behind a
small private trait object over `Admission<P>`) if the extra type parameter on
`Pending` proves noisy; the behaviour above is what is specified.

Orderings: `claim` and `withdraw` are `compare_exchange(QUEUED, _, AcqRel,
Acquire)`. Exactly one of them can win for a given command; that is the whole
mechanism. The payload (the `Command` and its consumer, the upgrade payload,
the pipeline's commands and consumers, IDLE's `done_rx`) is reachable only
through a won `claim`, and `ClaimedReply` is the only answering type, so the
driver can neither RUN nor ANSWER a command it did not claim: invariant 1
below is a property of the types, not a convention every arm must follow.

Taking the payload at withdrawal also releases it at the moment the caller
gives up rather than when the driver eventually dequeues the shell. A
withdrawn streaming command's consumer, which owns the caller's item sender,
is dropped in the caller's frame, so a caller that drains its receiver
alongside the command future (`uid_fetch_streaming`, `blob.rs`, the
`tokio::join!` shape) is released at its deadline instead of waiting behind a
stalled head command. A withdrawn APPEND's `Bytes` are freed the same way. The
`DriverCommand` shell itself still occupies its channel slot until the driver
dequeues it (see the reference text below).

`Pending` replaces `InFlightGuard` for every wire-command submission.
`InFlightGuard` stays, unchanged, for its single non-wire user
(`setup_keepalive_cancel_safe`), whose purpose is to make a cancelled direct
setup abort the driver at last drop; no claim is meaningful there. Its doc is
restated for that single purpose.

### Driver: claim at the head of the queue

`DriverCommand` becomes:

```rust
Run { reply: Reply<(Command, DriverConsumer), Result<Box<dyn Any + Send>, Error>> },
Upgrade { reply: Reply<UpgradePayload, Result<Box<dyn Any + Send>, Error>> },
Pipeline { reply: Reply<(Vec<Command>, Vec<Box<dyn ConsumerErased>>),
                        Result<PipelineResults, Error>> },
Idle { reply: Reply<oneshot::Receiver<()>, Result<IdleTermination, Error>> },
SetKeepalive { keepalive, result_tx: oneshot::Sender<Result<(), Error>> },   // unchanged
PeerCertificate { result_tx: oneshot::Sender<Option<Vec<u8>>> },              // unchanged
```

In `driver_task`, the first statement of each of the four wire arms is:

```rust
let Some(((command, consumer), reply)) = reply.claim() else {
    trace!("driver: caller withdrew the command before it reached the head of the queue");
    continue;
};
```

(with the payload pattern adjusted per arm). `continue` is correct: nothing
was prepared, no tag was drawn (`prepare_command` draws the tag), no pending
effect armed, no byte written, no state changed, so there is no snapshot to
publish and the `Logout` check has nothing new to see. The payload was already
dropped by the withdrawing caller; the driver drops only the shell.

`publish_then_answer` takes a `ClaimedReply<T>` instead of a `oneshot::Sender`.
Its doc is restated in terms of `ClaimedReply::answer`; the recorded ruling on
making answer-without-publish unrepresentable is unchanged by this spec.

The claim is taken BEFORE `prepare_command`, so a command the driver claims
and then refuses before its first byte (`StateChangedBeforeSend`, an encoder
refusal) is still answered with its precise `Unsent` refusal when the caller is
there, and is `InFlight` (conservative) only when the caller had already given
up after the claim. Skipping a withdrawn command before preparation cannot
desynchronize the wire: nothing of it was ever framed.

### Submit helpers own the deadline and the phase

Signatures (all in `connection/auth.rs` beside today's helpers):

```rust
pub(super) async fn submit_regular<C: Consumer + 'static>(
    &self, cmd: Command, consumer: C, deadline: Option<tokio::time::Instant>,
) -> Result<C::Output, Error>;
// submit_streaming, submit_with_continuations: same extra parameter.
pub(super) async fn submit_upgrade(
    &self, payload: UpgradePayload, deadline: Option<tokio::time::Instant>,
) -> Result<(), Error>;
```

One private body, `submit_driver_command`, shared by all four (the four
helpers differ only in how they wrap the consumer and downcast).

**Admission is strict.** `tokio::time::timeout_at` polls its inner future
before its timer, so wrapping `cmd_tx.send` in it can submit a command whose
deadline is already gone. Admission is therefore three steps with no suspension
between the last two:

1. `cmd_tx.reserve()` under `timeout_at(deadline, ..)` (unbounded when
   `deadline` is `None`). Expiry: `Error::timeout().with_attempt(Unsent)`,
   nothing submitted. A closed channel: `observe_driver_panic(Unsent)`.
2. Re-check with `may_send(deadline, Instant::now(), false)` (defined under
   IDLE below, shared). Past: drop the permit, return
   `Timeout` / `Unsent`, nothing submitted. This also covers a deadline
   already past on entry and a zero timeout.
3. `permit.send(dcmd)` (synchronous).

After admission the helper awaits `pending.recv()` under
`timeout_at(deadline, ..)` (the borrowing `recv` future is dropped before
`pending` is touched again). On expiry it calls one non-awaiting decision
function in `reply.rs`, which is also used by IDLE's DONE-timeout branch, so
the tie policy has one implementation and one test:

```rust
pub(super) enum Expired<T> {
    /// The answer was already buffered: completion wins the tie.
    Answer(T),
    /// Withdrawal won: the driver will never write the command.
    Withdrawn,
    /// Claimed and unanswered: the exchange is the driver's.
    Claimed,
}

/// `withdraw()` first; `Withdrawn` is final (an unclaimed command has no
/// answer). On `Claimed`, `try_take()`: `Some` is `Answer`, `None` is
/// `Claimed`.
pub(super) fn on_expiry<P, T>(pending: &mut Pending<'_, P, T>) -> Expired<T>;
```

Withdraw-then-take leaves no window in which an answer that lands between two
checks is thrown away: once `withdraw()` has observed `Claimed`, any answer
that has landed by the `try_take()` wins. An answer landing after that
`try_take()` is the conservative case invariant 4 names.

The phase table:

| What happened | Evidence returned | Connection |
|---|---|---|
| deadline passed during or at admission (steps 1-2) | `Timeout`, `Unsent` | untouched |
| channel closed at admission | `observe_driver_panic(Unsent)` | n/a |
| answer received | the answer | untouched |
| deadline passed after admission; `withdraw()` is `Withdrawn` | `Timeout`, `Unsent` | untouched; the driver will skip it |
| deadline passed; `withdraw()` is `Claimed`; `try_take()` has the answer | the answer (completion wins the tie) | untouched |
| deadline passed; `withdraw()` is `Claimed`, no answer | `Error::timeout_inflight()` | abandoned by `Pending::drop`, unless the answer lands before the drop (then untouched) |
| reply dropped, `Unanswered::NeverClaimed` | `observe_driver_panic(Unsent)` | not marked abandoned (pending settled); the driver is dead, so `is_reusable()` is already false |
| reply dropped, `Unanswered::Claimed` | `observe_driver_panic(InFlight)` | not marked abandoned (pending settled), as today; `is_reusable()` is already false |
| caller future dropped during admission | none returned | untouched (no permit sent; no `Pending` exists yet, or it withdraws trivially) |
| caller future dropped after admission | none returned | `Pending::drop` rules |

`Unsent` is never inferred from a timer alone after admission: it is returned
only when `withdraw()` won (or an earlier call did).

`deadline: None` means unbounded. Only `logout()` passes `None` from its own
frame: LOGOUT has no deadline of its own by design, and each caller decides
whether to bound it. The pool drain bounds it externally and reads no evidence
from it; `PooledConn::replace_selected_member` and the push loop's
cancellation path do not bound it (pre-existing, filed under the stopping
rule, unchanged here). `fetch_capabilities_via_driver` takes the caller's
deadline as a `tokio::time::Instant` parameter (both callers hold a concrete
one) instead of being wrapped.

### Call-site migration

Every `tokio::time::timeout(timeout, self.submit_X(cmd, c)).await.map_err(|_|
Error::timeout_inflight())?` becomes
`self.submit_X(cmd, c, Some(Instant::now() + timeout)).await` (or the
caller's existing deadline where it already computes one). This is
mechanical across the files listed in the survey. Specific shapes:

- `append_message` / `multi_append` already compute `deadline` before the
  APPENDLIMIT preflight: pass it straight to `submit_regular`. Delete
  `remaining_timeout` (its "expired before submission is `Unsent`" rule is now
  the helper's admission step 2). Keep `unsent_preflight`.
- `complete_auth` and `unauthenticate` pass their existing `deadline` to
  `fetch_capabilities_via_driver`; the `remaining.is_zero()` pre-checks are
  deleted (admission step 2 covers them). This does change one case: a
  refresh whose deadline is already gone now reports `Timeout` / `Unsent`
  (nothing submitted) where it reported `timeout_inflight()`. That is a
  later-step evidence case of the composite class; see the composite
  safeguard for why it needs no floor.
- Streaming builders (`uid_fetch_vanished_stream`, `uid_fetch_stream_budgeted`
  and their kin): the returned future computes its deadline when first polled
  and calls `submit_streaming(.., Some(deadline))`, preserving today's "the
  timeout starts when the future runs".
- `starttls_with_connector` and `compress`: `submit_upgrade(.., Some(deadline))`.
- `Pipeline::execute_raw`: uses `reply_channel` and `Pending`, with strict
  unbounded admission (`reserve` then `send`); no deadline parameter is added
  (none of its callers has one); dropping its future withdraws an unclaimed
  batch; a reply dropped before claim is `Disconnected` with `Unsent`.
  Dropping it while it waits for channel capacity no longer marks the
  connection abandoned (today's guard is armed before `send`).
- Direct test callers of the submit helpers (survey list) pass `None`, which
  keeps their current behaviour. In
  `invariant_driver_panic_subsequent_commands_fail` the external `timeout`
  wrapper then remains the only bound, deliberately: the test asserts a fast
  failure, and the wrapper is what turns a regression into an assertion.
- `connection/driver/mod_tests.rs` `command_answer_follows_state_publication`
  builds its answer through `reply_channel((), &flag)` and `claim()` (both
  reachable from `driver::tests` under `pub(super)` visibility, since
  `driver` is a descendant of `connection`).

`Error::timeout_inflight()` stays, with its doc corrected to "the driver had
claimed the command", and is used only by the claimed-expiry row and IDLE's
DONE wait.

### Composite safeguard: the UID MOVE fallback

The leaf evidence above is truthful, but `uid_move_messages`' UIDPLUS fallback
propagates a later step's leaf evidence as the outcome of the whole MOVE. This
landing makes three new `Unsent` outcomes reachable for a later step on a live
or dying connection: a STORE or UID EXPUNGE withdrawn after expiring queued
behind another command (the pool drain's LOGOUT stalled on a silent peer, or a
direct user's concurrent command), one dropped unclaimed by a dying driver,
and one whose deadline is gone at admission. Today each of these reads
`InFlight` and settles bulk move `Uncertain`. With the leaf evidence alone they
would settle `Failed`, the engine could retry, and the retry would repeat the
already-acknowledged COPY and duplicate messages. A regression the landing
itself introduces is in scope, whatever the wider class does.

The safeguard is an evidence floor, applied inside the fallback once the COPY
has been acknowledged:

```rust
/// A step of a composite that runs after a non-idempotent step was
/// acknowledged must not report the composite as unsent: the earlier side
/// effect happened. Restamp evidence that reads `Unsent` (explicit, or absent
/// on a variant that can carry it) as `InFlight`, so the operation is read
/// back rather than replayed. Acknowledged evidence is left alone.
fn after_acknowledged_side_effect(error: Error) -> Error {
    match error.attempt() {
        None | Some(TransmissionState::Unsent) => error.with_attempt(TransmissionState::InFlight),
        Some(_) => error,
    }
}
```

Both the STORE and the UID EXPUNGE errors pass through it (`.map_err`). It
lives beside `uid_move_messages` in `connection/uid_ops.rs`, `fn` private to
that module. `with_attempt` is a no-op for variants that cannot carry
evidence, so `StateChangedBeforeSend` (variant-is-evidence) and local
refusals such as `MissingCapability` pass through unchanged; those are
pre-existing triggers of the excluded class (stopping rule, triggers (3) and
the `StateChangedBeforeSend` half of (4)), not something this landing adds.
The floor also lifts two pre-existing triggers for this path as a side effect
(trigger (1), the unmoved-counter first-write `Io` / `Unsent`, and the
`Closed` / `Unsent` half of (4)); that is strictly more conservative and is
named here so the aggregate spec knows it.

The other composites were checked for the same regression and need no floor,
because a retry they could trigger repeats nothing non-idempotent:

- `complete_auth` / `unauthenticate`: the earlier step is LOGIN /
  AUTHENTICATE / UNAUTHENTICATE; a retry on the same connection is refused
  locally by the session-state gate, and an account-level retry dials a fresh
  connection. Their connection-disposition question stays in the excluded
  class.
- `run_destroy_mutation_groups`: STORE `\Deleted` and EXPUNGE are idempotent
  under retry.
- `run_patch_mutation_group`: both STOREs are flag edits; a retry after the
  guarded add applied meets a MODSEQ conflict, a misreport that belongs to the
  excluded class, not a duplicate.
- `run_starttls_upgrade`: the post-upgrade CAPABILITY runs inside the driver's
  upgrade arm and is never withdrawable.

### IDLE

`idle()` keeps its own body (it is not a request/response submission) but
follows the same arbiter, with `done_rx` as the IDLE reply's payload:

1. **Admission** creates the event deadline (`Instant::now() + timeout`)
   BEFORE submitting and races `cmd_tx.reserve()` against the cancel token
   and that deadline in a `biased` `select!` ordered cancel, deadline,
   reserve. Cancel wins: return `Ok(IdleEvent::Cancelled)`, nothing
   submitted. Deadline wins: return `Ok(IdleEvent::Timeout)`, nothing
   submitted. Closed channel: `observe_driver_panic(Unsent)`. A reserved
   permit then gets the same strict re-check as the submit helpers, through
   the same non-awaiting function in `reply.rs` that admission step 2 calls:

   ```rust
   /// Whether a reserved permit may still be sent: `false` when the
   /// deadline is at or before `now`, or `cancelled` is set.
   pub(super) fn may_send(deadline: Option<Instant>, now: Instant, cancelled: bool) -> bool;
   ```

   `false` drops the permit and returns `Cancelled` / `Timeout` with nothing
   submitted; otherwise `permit.send` (synchronous). (With the biased order a
   deadline already gone selects the deadline arm before reserving, so this
   re-check matters only for a deadline or cancellation that lands while
   `reserve` was pending; it is pinned at unit level, below.)
2. **Every departure from the wait loop that would send DONE** - a meaningful
   event, `Cancelled`, or `Timeout` - first calls `withdraw()`. `Withdrawn`:
   IDLE was never written (and `done_rx` is already dropped); return the
   departure's value (the event already consumed from the connection-wide
   queue is a real server event and is returned as such), connection
   untouched. `Claimed`: proceed to DONE as today.
3. **The `next_event` driver-death arm**: `try_take()`; with an answer, today's
   handling; with none, the phase comes from `withdraw()`: `Withdrawn` ->
   `Unsent`, `Claimed` -> `InFlight`.
4. **The `pending.recv()` arm** (driver finished IDLE on its own): an answer
   is handled as today; `Unanswered::NeverClaimed` -> `observe_driver_panic(Unsent)`,
   `Unanswered::Claimed` -> `observe_driver_panic(InFlight)`.
5. **The DONE wait**: answer -> today's handling. On `done_timeout` expiry,
   `on_expiry` decides: `Answer` -> today's handling of that answer;
   `Claimed` -> `timeout_inflight()` (`Pending::drop` abandons the connection
   as today). `Withdrawn` cannot occur here, because step 2 only reaches DONE
   after `withdraw()` returned `Claimed`; it is a `debug_assert!` and is
   treated as `Claimed` in release.

The in-body comments that state the old ownership rule (after submission, and
in the death arm) are rewritten with the code.

### Invariants the implementation must hold

1. A command is written to the wire only after its `Reply` was claimed. The
   payload is reachable only through a won claim, so this is enforced by the
   types.
2. Claim and withdraw are mutually exclusive per command (single CAS
   arbiter); the winner alone takes the payload.
3. `Unsent` evidence the CALLER generates for a submitted command (a
   post-admission timeout, or a reply dropped unanswered) requires that the
   withdrawal won, or that the driver dropped the reply before any claim;
   nothing of that command is ever written later. `Unsent` the DRIVER
   produces after a claim (a `prepare_command` refusal, a first-write failure
   with an unmoved socket counter) keeps its independently established
   evidence, unchanged by this spec.
4. A withdrawn command never abandons the connection. A caller leaving a
   claimed command with no answer in hand abandons it. `Pending::drop` takes
   one final look for an answer; one that lands after that look still
   abandons the connection, conservatively. A reply dropped by a dead
   driver does not mark the connection abandoned; the connection is already
   not reusable.
5. Driver arms that do no wire I/O (`SetKeepalive`, `PeerCertificate`) are
   untouched.
6. In `uid_move_messages`' UIDPLUS fallback, no error of a step after the
   acknowledged COPY leaves the operation with evidence that reads `Unsent`
   where the variant can carry evidence.

## Bricks: one landing

The whole change is one coherent landing (the arbiter, the driver claim, the
helper phase table, the call-site migration, the composite safeguard, IDLE,
pipeline, tests, documentation, and the filed exclusions). No intermediate
state is useful: a driver that claims without callers that withdraw does
nothing, callers that report `Unsent` without a driver that honours the
withdrawal would lie, and precise leaf evidence without the MOVE floor would
authorize duplicating replays. `brokkr check` must be green at the landing
boundary.

1. `connection/reply.rs` with the types above.
2. `DriverCommand` arms carry `Reply<P, T>`; the four wire arms claim first;
   `publish_then_answer` takes `ClaimedReply<T>`.
3. Submit helpers take `deadline`, admit strictly, and implement the phase
   table through one shared body; `InFlightGuard` is no longer used by them.
4. Migrate every call site (survey list). Delete `remaining_timeout`. Correct
   `Error::timeout_inflight`'s doc.
5. The composite safeguard in `uid_move_messages`.
6. `idle()` per the IDLE section; `Pipeline::execute_raw` per migration.
7. Test compile fixes: the direct submit-helper callers (pass `None`),
   `connection/tests.rs`'s direct `DriverCommand::Pipeline` construction
   (`reply_channel`), and `driver/mod_tests.rs`'s `publish_then_answer` test
   (`reply_channel` + `claim`).
8. Documentation (below).
9. File the stopping rule's exclusions and lateral items in `notes/todo.md`,
   each with its evidence; the aggregate-progress item carries the triggers
   this spec names.

### Tests (the instruments, laid with the change)

All hermetic: `driver_pair` over an in-memory duplex, no sockets.
`#[tokio::test(start_paused = true)]` for every behavioural test below (all of
them involve a deadline; the paused runtime is current-thread, which the
zero-timeout tests also rely on). Every test that depends on a command being
buffered or claimed establishes that milestone BEFORE letting time advance:
buffered is proven by `cmd_tx.capacity()` dropping, claimed is proven by the
script reading the command's line. Awaiting a queued task or an unanswered
script read too early lets paused time auto-advance to the 60 s timeout of the
command ahead and destroys the setup. The command ahead (A) and any fillers
stay alive until the assertions finish. Unless stated otherwise the greeting
is `preauth_greeting(..)` (session Authenticated).

Unit, `connection/reply.rs`:

- `a_withdrawn_reply_cannot_be_claimed`
- `a_claimed_reply_cannot_be_withdrawn`
- `withdrawing_twice_reports_withdrawn_both_times`
- `claiming_hands_over_the_payload` (the `claim` result carries the payload
  given to `reply_channel`)
- `withdrawing_drops_the_payload_at_once` (payload is a type whose `Drop` sets
  a flag, or an `mpsc::Sender` whose receiver then reads `None`; assert it
  was dropped by `withdraw()` while the `Reply` is still alive)
- `a_reply_dropped_before_its_claim_reads_never_claimed`
- `a_reply_dropped_after_its_claim_reads_claimed`
- `a_pending_whose_claimed_reply_was_dropped_does_not_abandon` (claim, drop
  the `ClaimedReply`, `recv` returns `Unanswered::Claimed`, drop the pending;
  `abandoned` stays false)
- `on_expiry_takes_an_answer_that_arrived_before_expiry` (claim, answer,
  then `on_expiry` returns `Answer` and the pending is settled; this is the
  production decision both expiry branches call, so a branch that ignored a
  buffered answer would have to bypass `on_expiry`)
- `may_send_refuses_an_expired_deadline_or_a_cancellation` (deadline equal to
  `now`, before `now`, after `now`, `None`, and `cancelled` set)
- `on_expiry_withdraws_a_queued_command` and
  `on_expiry_reports_a_claimed_unanswered_command_as_claimed`
- `pending_operations_are_safe_after_the_receiver_closed`
- `dropping_an_unanswered_claimed_pending_marks_the_connection_abandoned`
- `dropping_a_claimed_pending_whose_answer_arrived_does_not_abandon`
- `dropping_a_queued_pending_withdraws_without_abandoning`

Behavioural, `connection/tests.rs` unless noted:

- `a_queued_command_whose_caller_timed_out_is_never_written`: an `Arc`
  connection; task A issues `noop` (60 s); the script reads A's NOOP and does
  not answer; `append` (1 s) on the same connection after confirming it is
  buffered. Assert the APPEND fails `Timeout` with `attempt() ==
  Some(Unsent)` and `conn.is_reusable()`. Then answer A; issue `capability`
  as C; assert the next line the script reads is C's CAPABILITY, not an
  APPEND. Bites: without the driver claim the next line is the APPEND;
  without the helper's phase table the attempt is `InFlight`.
- `dropping_a_queued_command_withdraws_it_and_keeps_the_connection_reusable`:
  same setup; the APPEND future is pinned, polled until it is buffered, then
  dropped. Assert `is_reusable()` and that C's line follows A. Bites: an
  always-abandon `Pending::drop` fails the reusability assertion; a driver
  without the claim writes the APPEND.
- `dropping_a_queued_streaming_fetch_writes_nothing_and_ends_its_item_channel`:
  selected session (`select_inbox`); A on the wire; a `uid_fetch_stream`
  future polled until buffered, then dropped while its item receiver is
  kept. Assert `is_reusable()` and that the receiver yields `None` BEFORE A
  is answered (withdrawal drops the consumer, and with it the item sender, in
  the caller's frame); then answer A and assert C's line follows A. Pins
  streaming consumer disposal on the withdraw path.
- `a_claimed_command_whose_caller_timed_out_is_in_flight_and_abandoned`: no
  command ahead; `append` (1 s) with no literal extension; the script reads
  the APPEND line (proving the claim and wire progress) and never sends `+`.
  Assert `Timeout`, `attempt() == Some(InFlight)`, `!is_reusable()`. Pins that
  withdrawal cannot win after a claim.
- `a_command_queued_behind_a_connection_fatal_failure_is_unsent`: A's NOOP
  read; a second command (`append`, 60 s) confirmed buffered; the script
  answers A with `* BYE going away`. Assert the second fails with `attempt()
  == Some(Unsent)`. Bites: today it is `InFlight`. The BYE must carry NO
  `[ALERT]` code; say so in a comment in the test: an ALERT is published as
  an event before the BYE short-circuit, which in the IDLE twin below would
  make the IDLE leave through the event-departure path instead.
- `a_claimed_command_whose_peer_vanishes_is_in_flight`: `append` claimed (its
  line read, no `+` sent); the script drops its end. Assert `attempt() ==
  Some(InFlight)`. This is a driver-produced answer (the continuation wait's
  `Closed`, stamped `InFlight`), kept as a regression gate for that path.
- `a_claimed_command_whose_driver_is_aborted_unanswered_is_in_flight`: `append`
  submitted from a spawned task and claimed (its line read, no `+` sent);
  then `conn.terminate().await` aborts and joins the driver, so the reply is
  dropped unanswered after its claim. Assert the task's error is a driver
  death (`DriverGone` / `DriverPanicked`) with `attempt() == Some(InFlight)`.
  This is the test that pins `Unanswered::Claimed`.
- `a_submission_that_times_out_before_channel_admission_is_unsent`: A on the
  wire, the channel filled with queued `noop`s (60 s) until `capacity() == 0`,
  then a further `noop` with 1 s. Assert `Timeout` with `Unsent` and
  `is_reusable()`. Bites: today it is `InFlight`.
- `a_zero_timeout_submits_nothing_and_is_unsent`: free channel, `noop` with
  `Duration::ZERO`; record `cmd_tx.capacity()` before the call; assert
  `Timeout` with `Unsent` AND, immediately after the call returns and before
  any yield (the runtime is current-thread, so the driver cannot have
  dequeued anything), that `cmd_tx.capacity()` is unchanged; then issue C and
  assert C's line is the first the script reads. The capacity assertion is
  what bites against admission that sends before re-checking the deadline:
  such an admission submits, then the reply wait expires, withdrawal wins and
  the driver later skips the command, so the wire assertion alone stays
  green.
- `a_move_fallback_whose_store_is_withdrawn_after_the_copy_is_in_flight`
  (`uid_ops_tests.rs` or `tests.rs`): greeting with `UIDPLUS` and no `MOVE`;
  `select_inbox`; an `Arc` connection. Spawn `uid_move_messages` (1 s); the
  script reads the UID COPY line. Before answering it, spawn task A (`noop`,
  60 s) and confirm A is buffered. Answer the COPY (`OK [COPYUID ...]`); the
  script reads A's NOOP (A claimed) and does not answer; the STORE is then
  buffered behind A (confirm via `capacity()`), and its deadline passes.
  Assert the move's error is `Timeout` with `attempt() == Some(InFlight)`
  (the floor) and `is_reusable()` (the STORE was withdrawn, not abandoned).
  Answer A, issue C, assert C's line is next (no STORE, no EXPUNGE). This is
  a regression gate: today's code also yields `InFlight`, so the bite is
  ablation (i).
- `idle_tests.rs` or `tests.rs`:
  - `an_idle_cancelled_before_its_claim_is_never_written`: A unanswered; IDLE
    buffered; cancel. Assert `Ok(IdleEvent::Cancelled)`, `is_reusable()`;
    answer A; C's line is next (no IDLE, no DONE).
  - `an_idle_cancelled_while_waiting_for_channel_capacity_returns_cancelled`:
    A on the wire, channel full; `idle()` started, cancel fired. The `idle()`
    call is wrapped in a bounded `tokio::time::timeout` (shorter than the
    fillers' 60 s) so that against today's code, which awaits `send` without
    polling the token, the test fails on an assertion rather than through the
    test runner's wall-clock watchdog. Assert `Ok(IdleEvent::Cancelled)`
    without A being answered.
  - `an_idle_leaving_on_another_commands_alert_before_its_claim_is_never_written`:
    A's NOOP read; IDLE buffered; the script sends `* OK [ALERT] maintenance`
    and does not complete A. The ALERT is published to the event queue as
    soon as it arrives (`process_untagged_prefix`, before classification);
    an `Either` response such as `* 4 EXISTS` would NOT work, because it is
    routed to A's consumer and reaches the event queue only at A's tagged
    completion. Assert `idle()` returns `Ok(IdleEvent::Alert(..))` without a
    DONE handshake, `is_reusable()`; answer A; C's line is next.
  - `an_idle_queued_behind_a_connection_fatal_failure_is_unsent`: A's NOOP
    read; IDLE buffered; `* BYE going away` (no `[ALERT]`, see above, with
    the same comment). Assert the IDLE error has `attempt() ==
    Some(Unsent)`.
  - `an_idle_with_zero_timeout_submits_nothing`: free channel; `idle` with
    `Duration::ZERO`; assert `Ok(IdleEvent::Timeout)` and, before any yield,
    unchanged `cmd_tx.capacity()`; then C's line is the first the script
    reads.
- `pipeline/tests.rs` `a_dropped_pipeline_queued_behind_a_command_is_never_written`:
  A on the wire; a two-command pipeline of NOOP and `LIST "" "*"` (both legal
  in the Authenticated session, both pipelinable, distinct routing keys, no
  capability gate, both distinct from C) polled until buffered, dropped;
  answer A; issue `capability` as C; the next line is C's CAPABILITY. Not
  CHECK: it is Selected-only, so `execute_raw`'s `require_state` refuses the
  batch before it is ever buffered. Not NAMESPACE without the capability in
  the greeting: under ablation (a) the driver would refuse it in the encoder
  before writing a byte, and the test would stay green against the ablation.
- `a_compress_withdrawn_before_its_claim_is_never_written`: greeting with
  `COMPRESS=DEFLATE`; A on the wire; `compress` (1 s) times out queued. Assert
  `Unsent` and `is_reusable()`; answer A; C's line arrives as plain text.

Existing tests that must still pass unchanged:
`dropping_a_command_waiting_for_channel_capacity_does_not_abandon_connection`,
`an_idle_whose_done_times_out_is_abandoned_and_its_last_drop_releases_the_socket`,
`an_idle_cancelled_through_its_token_stays_reusable`,
`idle_done_handshake_is_bounded_when_the_server_never_completes`,
`cancellation_during_keepalive_setup_marks_the_connection_abandoned`,
`a_queued_command_is_refused_when_the_session_left_its_legal_states`.

Not deterministically testable, stated rather than faked: the exact
simultaneous-ready tie between channel capacity freeing and the deadline (step
1 of admission). The strict design makes it irrelevant: whichever wins step 1,
step 2 re-checks the deadline before any send, and that step is pinned by
`a_zero_timeout_submits_nothing_and_is_unsent` (and its IDLE twin). The
answer-versus-expiry tie cannot be staged through the driver either; its
policy lives entirely in `on_expiry`, which both production expiry branches
call and the `on_expiry_*` unit tests pin. An answer landing inside
`on_expiry` itself cannot be staged on a single thread; withdraw-then-take
leaves no gap for it to fall into.

### Gates (exact)

```
brokkr test -p bifrost-imap reply::tests
brokkr test -p bifrost-imap a_queued_command_whose_caller_timed_out_is_never_written
brokkr test -p bifrost-imap dropping_a_queued_command_withdraws_it_and_keeps_the_connection_reusable
brokkr test -p bifrost-imap dropping_a_queued_streaming_fetch_writes_nothing_and_ends_its_item_channel
brokkr test -p bifrost-imap a_claimed_command_whose_caller_timed_out_is_in_flight_and_abandoned
brokkr test -p bifrost-imap a_command_queued_behind_a_connection_fatal_failure_is_unsent
brokkr test -p bifrost-imap a_claimed_command_whose_peer_vanishes_is_in_flight
brokkr test -p bifrost-imap a_claimed_command_whose_driver_is_aborted_unanswered_is_in_flight
brokkr test -p bifrost-imap a_submission_that_times_out_before_channel_admission_is_unsent
brokkr test -p bifrost-imap a_zero_timeout_submits_nothing_and_is_unsent
brokkr test -p bifrost-imap a_move_fallback_whose_store_is_withdrawn_after_the_copy_is_in_flight
brokkr test -p bifrost-imap an_idle_with_zero_timeout_submits_nothing
brokkr test -p bifrost-imap an_idle_cancelled_before_its_claim_is_never_written
brokkr test -p bifrost-imap an_idle_cancelled_while_waiting_for_channel_capacity_returns_cancelled
brokkr test -p bifrost-imap an_idle_leaving_on_another_commands_alert_before_its_claim_is_never_written
brokkr test -p bifrost-imap an_idle_queued_behind_a_connection_fatal_failure_is_unsent
brokkr test -p bifrost-imap a_dropped_pipeline_queued_behind_a_command_is_never_written
brokkr test -p bifrost-imap a_compress_withdrawn_before_its_claim_is_never_written
brokkr test -p bifrost-imap dropping_a_command_waiting_for_channel_capacity_does_not_abandon_connection
brokkr test -p bifrost-imap an_idle_whose_done_times_out_is_abandoned
brokkr test -p bifrost-imap an_idle_cancelled_through_its_token_stays_reusable
brokkr test -p bifrost-imap idle_done_handshake_is_bounded_when_the_server_never_completes
brokkr test -p bifrost-imap cancellation_during_keepalive_setup_marks_the_connection_abandoned
brokkr test -p bifrost-imap a_queued_command_is_refused_when_the_session_left_its_legal_states
brokkr check
```

Check each name filter matched the intended test before believing a PASS (the
filter is a substring match; a filter matching nothing is the known
false-green). Ablations, each restored after: (a) make `Reply::claim` always
succeed (and hand over the payload even after a withdrawal, e.g. by not
taking it in `withdraw`): every "never written" test must fail; (b) make the
helper's post-admission expiry always return `timeout_inflight()`: every
queued `Unsent` assertion must fail; (c) make `Pending::drop` always set
`abandoned`: the reusability assertions must fail; (d) replace admission with
`timeout_at(deadline, cmd_tx.send(..))`: the zero-timeout test must fail on
its capacity assertion; (e) drop IDLE's withdraw-on-departure: the
alert-departure and cancel-before-claim IDLE tests must fail; (f) make
`may_send` ignore its deadline or its `cancelled` argument:
`may_send_refuses_an_expired_deadline_or_a_cancellation` must fail (the IDLE
zero-timeout test does NOT pin the IDLE re-check, since the biased select
takes the deadline arm before reserving; it pins that ordering instead); (g)
map `Unanswered::Claimed` to `Unsent`: the aborted-driver test must fail; (h)
make `on_expiry` skip `try_take`: `on_expiry_takes_an_answer_that_arrived_before_expiry`
must fail; (i) remove `after_acknowledged_side_effect` from the MOVE fallback:
the move-fallback test must fail on `Unsent`; (j) leave the payload in the
cell on withdrawal (driver drops it at dequeue): `withdrawing_drops_the_payload_at_once`
and the streaming test's `None`-before-answer assertion must fail; (k) do not
mark the pending settled when `recv` reports closure:
`a_pending_whose_claimed_reply_was_dropped_does_not_abandon` must fail.

### Documentation, in the same landing

- `reference/imap.md`, "Connection state machine", the paragraph beginning
  "A timed-out or cancelled command is a third case": rewrite to state the
  three admission states; a caller that leaves before the driver claims its
  command withdraws it (never written, its payload released at once, evidence
  `Unsent`, connection not abandoned); once claimed, the driver completes the
  exchange (cancellation safety by socket ownership is unchanged) and a
  departing caller reports `InFlight` and abandons the connection; the
  deadline is owned by the submit helpers, which admit strictly and are the
  only code that knows the phase; a reply dropped by a dead driver is `Unsent`
  when never claimed and `InFlight` when claimed; IDLE withdraws on every
  departure before DONE; dropping a pipeline that waits for channel capacity
  no longer abandons the connection. Add one sentence on slots: a withdrawn
  command's shell keeps its channel slot until the driver dequeues it, so a
  channel full of withdrawn commands behind a stalled head still blocks new
  admissions until their deadlines (each then truthfully `Unsent`), while the
  connection reads reusable unless something abandoned it. Keep the
  abandoned-connection and last-handle-abort paragraphs, restated in terms of
  claimed commands.
- `reference/imap.md`, the paragraph "Cancellation safety comes from socket
  ownership": it applies from the claim onward.
- `reference/imap.md`, the error section's paragraph beginning "A driver
  death carries the caller's phase": replace "`Unsent` when `cmd_tx.send`
  failed (the driver never received the command), `InFlight` when the send
  succeeded and the reply channel dropped" with the claim-based rule
  (`Unsent` when the driver never claimed the command, `InFlight` once it
  had), and mention that admission failures and post-admission withdrawals
  are `Unsent` too.
- `reference/imap.md`, the pool bullet on `enable_qresync` ("an ENABLE that
  times out (abandoned) ... retires it"): an ENABLE that times out retires
  the member whether it was withdrawn or abandoned.
- `reference/imap.md`, "Bandwidth metering": "A cancelled in-flight command
  marks its connection abandoned" becomes "A cancelled command the driver had
  claimed ...".
- `reference/imap.md`, the paragraph beginning "On the UID MOVE fallback
  (COPY, STORE, UID EXPUNGE, ...)": add the evidence floor after an
  acknowledged COPY and why it exists.
- `reference/error-model.md`, Internal row: "(IMAP's driver death stamps the
  caller's phase - `Unsent` when the driver never received the command,
  `InFlight` when it owned it)" becomes "... `Unsent` when the driver never
  claimed the command, `InFlight` once it had".
- `crates/imap/src/account/mod.rs` `guarded_receiver_stream` doc: the abort is
  safe because an unclaimed command is withdrawn (never written, connection
  reusable) and a claimed one abandons the connection.
- `crates/imap/src/account/pool.rs` `enable_qresync` doc: "a timeout leaves it
  abandoned, with the driver still inside the ENABLE" becomes "a timeout (the
  ENABLE withdrawn unsent, or abandoned with the driver still inside it)".
- `crates/imap/src/connection/lifecycle.rs` `observe_driver_panic` doc:
  "`InFlight` when the send succeeded and the reply channel dropped" becomes
  "when the driver had claimed the command".
- `crates/imap/src/error.rs`: `timeout_inflight` (above), `driver_gone_at`
  ("never received it" / "owned it" become "never claimed it" / "had claimed
  it"), and the test-only `driver_gone` doc beside it.
- `crates/imap/src/connection/mod.rs` `InFlightGuard` doc: no longer "one
  submitted command"; it guards keepalive setup, which submits no wire
  command.
- `crates/imap/src/connection/driver/mod.rs` `publish_then_answer` doc, per
  the driver section.
- `crates/imap/src/connection/idle.rs` `idle()` "Cancellation safety" doc:
  dropping the future before the driver claims the IDLE withdraws it; plus
  the in-body comments named in the IDLE section.
- `notes/bugs-deferred.md`: remove the IMAP-011 entry entirely (rule 1 of that
  file). No code comment or reference names the finding ID.

## Stopping rule

In scope: everything above, and nothing else. Named and excluded, each a
separate item for `notes/todo.md` (filed by brick 9) rather than a deferral of
this one:

- **Composite partial completion.** An operation made of several commands
  reports the evidence of whichever later step failed, and leaf transmission
  evidence cannot express "an earlier step was acknowledged". This was sparred
  and agreed to be a separate defect class needing an aggregate-progress
  representation (one candidate: driver-owned composite operations that own
  their sequencing and report partial completion). It is already reachable
  today without this spec: in `uid_move_messages`' UIDPLUS fallback, after the
  COPY is acknowledged, (1) the connection dies and the STORE's first write
  fails with an unmoved socket counter (`Unsent`); (2) the STORE gets `NO
  [UNAVAILABLE]` (`Acknowledged`, derived retryable); (3) a CAPABILITY
  response on the COPY drops UIDPLUS and the UID EXPUNGE refuses locally
  (`MissingCapability`, no evidence); (4) the pool drain's LOGOUT interleaves
  (`Closed` / `StateChangedBeforeSend`, `Unsent`). Any of these settles
  bulk_move `Failed`, the engine may retry, and the COPY runs again,
  duplicating messages. This spec adds three triggers to the class: a later
  step withdrawn after its deadline expired while queued on a live
  connection; a later step dropped unclaimed by a dying driver (`Unsent`
  where today it reads `InFlight`, the same moment as today's dead-driver
  `cmd_tx.send` failure one poll earlier); and a later step whose deadline is
  already gone at admission (`complete_auth` / `unauthenticate` refresh:
  `Timeout` / `Unsent` where the deleted `remaining.is_zero()` check returned
  `timeout_inflight()`). For the one composite where they could duplicate a
  side effect, the MOVE fallback, the composite safeguard above neutralizes
  all three (and, as a side effect, triggers (1) and the `Closed` half of
  (4)); what remains of the class for the MOVE fallback is (2), (3) and the
  `StateChangedBeforeSend` half of (4). The other composites in the class:
  `complete_auth` and `unauthenticate` (including the connection's
  disposition after an incomplete required capability refresh, cf.
  `post_starttls_capability_failure` in `driver/upgrade.rs`),
  `run_starttls_upgrade`, `run_patch_mutation_group`, and
  `run_destroy_mutation_groups`. The aggregate spec must include the three
  triggers named here and must replace (or absorb) the MOVE floor.
- **Bandwidth-admission wait after the claim.** `send_wire_command` can wait
  on the account `BandwidthBudget` before its first byte reaches the socket.
  A caller that gives up there still reports `InFlight`. Making that window
  withdrawable means cancelling an exchange the driver has started, which can
  strand bytes already handed to TLS (OpenSSL may buffer an encrypted record
  with the socket counter unmoved) and therefore needs connection retirement
  rules of its own. Different mechanism, different failure modes.
- **The abandoned-checkout cascade.** The mutation group loops keep submitting
  on a checkout that is already abandoned, so each later group waits a full
  `command_timeout`. With this spec those waits report `Unsent` truthfully
  (settled `Failed`, retryable) instead of `Uncertain`, but they are still
  slow. Failing fast on an abandoned connection, or re-checking out per group,
  is a behaviour decision about the account layer.

Lateral findings from review, outside this scope, filed by brick 9 and not
built here:

- **Unbounded LOGOUT outside the pool drain.**
  `PooledConn::replace_selected_member` (`account/pool.rs`) runs `let _ =
  old.conn.logout().await;` with no bound inside a live operation, and the
  push loop (`account/push.rs`) does the same on cancellation. Against a
  stalled peer, that operation (and its permit) or that push task hangs
  indefinitely. Pre-existing.
- **`peer_certificate_der` runs outside the auth deadline.** It uses an
  unbounded `cmd_tx.send` and `result_rx.await` before SCRAM-PLUS
  AUTHENTICATE (`resolve_scram_binding`). It does no wire I/O, but it can
  wait behind a stalled head command for as long as that command takes.
  Pre-existing.

No change to `bifrost-types`, `bifrost-sync`, or any published item: every
type and helper touched is `pub(crate)` or narrower. `uid_move_messages`
keeps its signature; only the evidence on its fallback errors changes.

## Review record

Two cold reviews (r1, r2) were folded into this revision. Findings not
adopted, or adopted in a different form, and why:

- r1, lateral "the APPENDLIMIT preflight is not bound by the APPEND's
  deadline, so one APPEND can take up to twice its timeout": rejected. The
  preflight STATUS runs under `timeout` starting at (effectively) the same
  instant as the APPEND's `deadline`, so it ends by that deadline, and the
  APPEND is then bounded by what remains (today `remaining_timeout`, after
  this landing admission step 2). Total wall time is about one `timeout`, not
  two.
- r1 #6, "add a unit test that answers between `try_take` and `withdraw`":
  not adoptable as written, because on a single thread nothing can run
  between two synchronous calls inside `on_expiry`. Fixed instead by
  reordering `on_expiry` to withdraw first and take second, which removes the
  window; the existing unit test and ablation (h) pin the result.
- r1 #5's fallback ("reword the type claim, keep invariant 1 a convention"):
  superseded; the primary proposal (payload behind the claim, taken by the
  withdrawal winner) is adopted.
- r1 #4, `publish_then_answer`: the doc is restated, but the recorded ruling
  against making answer-without-publish unrepresentable is not reopened; no
  finding showed a defect in it.
- r2's first remedy ("land aggregate-progress handling first"): not adopted.
  Its second remedy, a bounded safeguard, is (the composite safeguard), with
  the hermetic transcript r2 asked for.
