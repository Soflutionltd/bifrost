use bifrost_types::TransmissionState;
use tokio::sync::oneshot;
use tracing::{debug, trace};

use crate::error::Error;
use crate::types::Command;
use crate::types::response::StatusKind;

use super::IdleTermination;
use super::event_sink;
use super::wire_send::{SendOutcome, send_wire_command};

/// Enter IDLE mode, read and publish events, exit on DONE or server
/// termination (RFC 2177 Sections 2-4).
///
/// 1. Sends the IDLE command on the wire (tagged, via the encoder).
/// 2. Waits for the `+` continuation (RFC 2177 Section 3).
/// 3. Enters a reading loop: `select!` between wire reads and `done_rx`.
///    - Untagged responses are emitted as events via `event_sink`.
///    - The tagged OK means the server terminated IDLE.
///    - `done_rx` fires when the handle wants to exit IDLE.
/// 4. On `done_rx`: sends `DONE\r\n` (untagged, RFC 2177 Section 3),
///    reads remaining responses until the tagged OK, returns `ClientDone`.
/// 5. On server-sent tagged OK: returns `ServerTerminated`.
pub(super) async fn run_idle(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    done_rx: oneshot::Receiver<()>,
    gate: &super::super::reply::CommitGate,
) -> Result<super::Gated<IdleTermination>, Error> {
    // 1. Send IDLE command (RFC 2177 Section 2), checked against live state
    //    like every other command: a session that left Authenticated/Selected
    //    while IDLE was queued is refused before a byte is written. The
    //    caller may withdraw until the commit before the first byte.
    let super::PreparedCommand { tag, wire } =
        super::prepare_command(state, tag_gen, &Command::Idle)?;
    if !super::commit_before_first_write(wire_reader, gate, [&wire]).await {
        return Ok(super::Gated::Withdrawn);
    }
    run_committed_idle(wire_reader, state, event_sink, &tag, &wire, done_rx)
        .await
        .map(super::Gated::Ran)
}

/// Steps 1 to 5 of [`run_idle`] for an IDLE that is committed: its first byte
/// is about to be offered, so it is the driver's exchange to the end.
async fn run_committed_idle(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    tag: &str,
    wire: &crate::codec::encode::WireCommand,
    done_rx: oneshot::Receiver<()>,
) -> Result<IdleTermination, Error> {
    // IDLE has no retaining consumer and therefore no consumer literal budget.
    // Keep its event reads unbudgeted rather than inventing a per-event ceiling
    // unrelated to the caller's FETCH memory budget.
    let baseline = wire_reader.written();
    // IDLE has no literal, so there is no continuation wait in the send and
    // the outcome is always `Sent`.
    let SendOutcome::Sent = send_wire_command(
        wire_reader,
        state,
        event_sink,
        wire,
        tag,
        baseline,
        None,
        None,
    )
    .await?
    else {
        return Err(Error::internal_mid_exchange(
            "IDLE reported a refused literal it does not have",
            TransmissionState::InFlight,
        ));
    };

    // 2. Wait for `+` continuation (RFC 2177 Section 3).
    wait_for_idle_grant(wire_reader, state, event_sink, tag).await?;

    trace!(tag, "driver: entered IDLE mode");

    // 3. IDLE reading loop.
    let mut done_rx = done_rx;
    loop {
        // Opportunistic drain of pending critical events (D7).
        let _ = event_sink.drain_pending_nonblocking();

        // Use a flag to signal which branch won, avoiding borrow
        // conflicts between the done_rx handler and wire_reader.
        let done_signaled = tokio::select! {
            biased;
            _ = &mut done_rx => true,
            result = wire_reader.read_one(super::utf8_mode(state)) => {
                // The IDLE command is on the wire and the server granted it,
                // so a transport failure here is InFlight, as in
                // `dispatch_response_loop`. Without the stamp an EOF mid-IDLE
                // carried no phase and read as Unsent.
                let resp = result.map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
                let digest = state.apply_side_effects(&resp);
                match resp {
                    crate::types::Response::Tagged(t) if t.tag == tag => {
                        // RFC 2177 Section 3: server terminated IDLE.
                        super::emit_tagged_response_code_events(&t, event_sink);
                        // Check status. NO/BAD is an error, not a
                        // successful server termination.
                        match t.status {
                            StatusKind::Ok => {
                                trace!(tag, "driver: server terminated IDLE");
                                return Ok(IdleTermination::ServerTerminated);
                            }
                            StatusKind::No => {
                                return Err(Error::no_with_code(t.text, t.code));
                            }
                            StatusKind::Bad => {
                                return Err(Error::bad_with_code(t.text, t.code));
                            }
                        }
                    }
                    crate::types::Response::Tagged(t) => {
                        // Foreign tagged response during IDLE. This is a
                        // protocol anomaly; emit tagged response code events
                        // and otherwise ignore to preserve the connection.
                        super::emit_tagged_response_code_events(&t, event_sink);
                    }
                    crate::types::Response::Untagged(u) => {
                        super::process_untagged_as_event(digest, u, event_sink, TransmissionState::InFlight)?;
                    }
                    crate::types::Response::Continuation(_) => {
                        // A second continuation during IDLE is invalid;
                        // ignore it and keep waiting for events/DONE.
                        debug!(tag, "ignoring unexpected continuation during IDLE");
                    }
                    crate::types::Response::Greeting(_) => {
                        return Err(Error::Protocol("unexpected greeting during IDLE".into()));
                    }
                }
                false
            }
        };

        if done_signaled {
            // 4. Client requested exit. Send DONE (untagged, RFC 2177 Section3).
            trace!(tag, "driver: sending DONE");
            wire_reader
                .write_all(b"DONE\r\n")
                .await
                .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
            drain_idle_responses(wire_reader, state, event_sink, tag).await?;
            trace!(tag, "driver: exited IDLE mode");
            return Ok(IdleTermination::ClientDone);
        }
    }
}

/// Wait for the server's `+` granting IDLE (RFC 2177 Section 3, RFC 9051
/// Section 6.3.13).
///
/// Not the shared literal `wait_for_continuation`: that wait answers every
/// tagged response as the command's own and words a premature OK as a
/// literal-continuation fault, and its `ContinuationOutcome` has no meaning
/// for IDLE (there is no literal to abandon). IDLE's own rules:
///
/// * `+` grants IDLE.
/// * Its own tagged `NO` / `BAD` is the refusal of IDLE.
/// * Its own tagged `OK` with no `+` first means the server completed IDLE
///   without the continuation it owes. The exchange is over and the framing
///   intact, so this is the non-fatal `ProtocolMissing`, not `Protocol`.
/// * Any other tag cannot exist - IDLE is the only command outstanding - so
///   it is a desynchronization (`Protocol`, connection-fatal), as in
///   `dispatch_response_loop`.
/// * Untagged responses become events through the shared arm, BYE included.
async fn wait_for_idle_grant(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    tag: &str,
) -> Result<(), Error> {
    loop {
        // The IDLE command is on the wire: a transport failure is InFlight.
        let resp = wire_reader
            .read_one(super::utf8_mode(state))
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
        let digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Continuation(_) => return Ok(()),
            crate::types::Response::Tagged(t) if t.tag == tag => {
                super::emit_tagged_response_code_events(&t, event_sink);
                return Err(match t.status {
                    StatusKind::No => Error::no_with_code(t.text, t.code),
                    StatusKind::Bad => Error::bad_with_code(t.text, t.code),
                    StatusKind::Ok => Error::ProtocolMissing(
                        "IDLE completed with a tagged OK before the server sent the `+` \
                         continuation it owes (RFC 2177 Section 3, RFC 9051 Section 6.3.13)"
                            .into(),
                    ),
                });
            }
            crate::types::Response::Tagged(t) => {
                return Err(Error::Protocol(format!(
                    "unexpected tag {:?} while waiting for the IDLE continuation (expected {tag:?})",
                    t.tag,
                )));
            }
            crate::types::Response::Untagged(u) => {
                super::process_untagged_as_event(
                    digest,
                    u,
                    event_sink,
                    TransmissionState::InFlight,
                )?;
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol(
                    "unexpected greeting while waiting for the IDLE continuation".into(),
                ));
            }
        }
    }
}

/// After sending DONE, read until the tagged OK for the IDLE command
/// (RFC 2177 Section3).
async fn drain_idle_responses(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    tag: &str,
) -> Result<(), Error> {
    loop {
        let resp = wire_reader
            .read_one(super::utf8_mode(state))
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
        let digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => {
                super::emit_tagged_response_code_events(&t, event_sink);
                return match t.status {
                    StatusKind::Ok => Ok(()),
                    StatusKind::No => Err(Error::no_with_code(t.text, t.code)),
                    StatusKind::Bad => Err(Error::bad_with_code(t.text, t.code)),
                };
            }
            crate::types::Response::Tagged(t) => {
                // Foreign tagged response while draining IDLE. Emit
                // critical response code events and ignore otherwise.
                super::emit_tagged_response_code_events(&t, event_sink);
            }
            crate::types::Response::Untagged(u) => {
                super::process_untagged_as_event(
                    digest,
                    u,
                    event_sink,
                    TransmissionState::InFlight,
                )?;
            }
            crate::types::Response::Continuation(_) => {
                // Ignore unexpected continuation while draining DONE.
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol(
                    "unexpected greeting during IDLE drain".into(),
                ));
            }
        }
    }
}
