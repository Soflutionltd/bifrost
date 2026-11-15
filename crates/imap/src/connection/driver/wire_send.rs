use bifrost_types::TransmissionState;

use crate::codec::encode::WireCommand;
use crate::error::Error;
use crate::types::response::StatusKind;

use super::event_sink;
use super::pipeline::{PipelineRouting, Routed, route_pipeline_response};

/// Whether a command's bytes all reached the wire, or the server refused a
/// synchronizing literal first.
///
/// Only reachable as `Rejected` in pipeline context: a refusal there is the
/// rejected command's ordinary per-command result, already routed into its
/// result slot, and the batch goes on to the next command. Outside the
/// pipeline the refusal is still returned as an error, so single-command
/// dispatch and IDLE only ever see `Sent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SendOutcome {
    Sent,
    Rejected,
}

/// Whether the server granted the literal or refused the command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContinuationOutcome {
    Granted,
    Rejected,
}

/// The transmission evidence for a write that failed, judged from the socket
/// progress counter (`stream::Tracked`) against the reading taken when the
/// command - or, for a coalesced pipeline batch, the batch - began.
///
/// `Unsent` exactly when the counter has not moved: no octet of this command
/// was accepted by the socket, so the peer cannot have received any of it, and
/// the failure retires the connection so nothing buffered above the socket is
/// ever sent later. Any movement is `InFlight`, whichever layer failed and
/// whether the failure was a write or a flush: a partial write, ciphertext a
/// TLS layer pushed before failing, or a flush failure after full delivery all
/// leave the server able to have received the command. A saturated counter
/// cannot tell movement from rest and reads as `InFlight`.
///
/// This is about progress, never chunk position. A rule keyed on "first
/// segment" called a failure `Unsent` after part of the command had already
/// reached the socket; one keyed on "any write attempted" would call a stale
/// pooled connection's first-byte EPIPE `InFlight` and send a non-idempotent
/// operation to reconcile when nothing landed.
pub(super) fn write_failure_evidence(
    wire_reader: &super::super::wire::WireReader,
    baseline: u64,
) -> TransmissionState {
    let now = wire_reader.written();
    if now == baseline && now != u64::MAX {
        TransmissionState::Unsent
    } else {
        TransmissionState::InFlight
    }
}

/// Commit a withdrawable command (or pipelined batch) through `gate` just
/// before its first byte is offered: wait for the first outbound bandwidth
/// grant, racing the caller's withdrawal, then commit. `false` means the
/// caller withdrew first and nothing may be written or answered.
///
/// Call it after every refusal-before-send decision and BEFORE arming any
/// pending state effect, and send immediately after it returns `true`, with
/// no await in between: the grant it leaves on the reader is sized for the
/// first nonempty chunk of `wires`, which is exactly what the send offers
/// first (a command's first segment always opens with its tag).
pub(super) async fn commit_before_first_write<'w>(
    wire_reader: &mut super::super::wire::WireReader,
    gate: &super::super::reply::CommitGate,
    wires: impl IntoIterator<Item = &'w WireCommand>,
) -> bool {
    let first = wires
        .into_iter()
        .flat_map(|wire| wire.segments().iter().flatten())
        .map(bytes::Bytes::len)
        .find(|&len| len > 0);
    match first {
        Some(len) => wire_reader.admit_first_write(len, gate).await,
        None => gate.commit(),
    }
}

/// Send one encoded command, waiting for a `+` continuation between each
/// pair of consecutive segments (RFC 3501 Section 4.3).
///
/// The one sender. Every command - APPEND, a pipelined command, IDLE, the
/// commands of a stream upgrade - reaches the socket through here, so the
/// write pattern always follows the command's encoded segments: a segment
/// boundary is exactly a synchronizing marker, and nothing past a marker is
/// written before the server grants it. Each segment's chunks are written from
/// their own allocations (a message body is never copied into a command
/// buffer) and flushed once.
///
/// `baseline` is the [`written`](super::super::wire::WireReader::written)
/// reading when the command (or its batch) began; a write failure is stamped
/// by [`write_failure_evidence`].
///
/// `routing` is `Some` only inside a pipelined batch; see
/// [`wait_for_continuation`]. `literal_budget` is the single command's
/// remaining budget; pipeline routing derives its own from active consumers.
/// A refused literal there is the command's own
/// result, already in its slot, and the sender answers `Rejected` after
/// abandoning the rest of the command. Outside a pipeline the wait returns the
/// refusal as an error instead, so `Rejected` is reported only with routing.
#[allow(clippy::too_many_arguments)]
pub(super) async fn send_wire_command(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    wire: &WireCommand,
    own_tag: &str,
    baseline: u64,
    mut routing: Option<&mut PipelineRouting<'_>>,
    literal_budget: Option<usize>,
) -> Result<SendOutcome, Error> {
    let segments = wire.segments();
    for (i, segment) in segments.iter().enumerate() {
        if let Err(e) = wire_reader.write_chunks(segment).await {
            let evidence = write_failure_evidence(wire_reader, baseline);
            return Err(e.with_attempt(evidence));
        }
        if i + 1 < segments.len()
            && wait_for_continuation(
                wire_reader,
                state,
                event_sink,
                own_tag,
                routing.as_deref_mut(),
                literal_budget,
            )
            .await?
                == ContinuationOutcome::Rejected
        {
            // The server refused this literal. Its remaining bytes - the body
            // and every trailing byte of command syntax after it - must not
            // be written: the server has ended this command's parsing state
            // and would read them as a new command line (RFC 3501 Section 4.3).
            if routing.is_none() {
                // Unreachable: without routing the wait raises the refusal as
                // an error. Raised after the server answered part of the
                // command, so the connection retires.
                return Err(Error::internal_mid_exchange(
                    "literal continuation refused outside a pipeline",
                    TransmissionState::Acknowledged,
                ));
            }
            return Ok(SendOutcome::Rejected);
        }
    }
    Ok(SendOutcome::Sent)
}

/// The error for a command whose own tagged `OK` arrived before the `+`
/// continuation its synchronizing literal is owed.
///
/// One definition for the single-command wait and the pipeline router, so the
/// two cannot drift apart on what this server behaviour means: the non-fatal
/// `ProtocolMissing`, because the server finished the exchange, the framing is
/// intact, and the unsent remainder is never written.
pub(super) fn early_ok_error() -> Error {
    Error::ProtocolMissing(
        "command completed with a tagged OK before the server sent \
         the `+` continuation its synchronizing literal is owed \
         (RFC 3501 Section 4.3); the literal was not sent"
            .into(),
    )
}

/// Wait for a server continuation response (`+ ...`) during
/// synchronizing-literal sends (RFC 3501 Section4.3, Section7.5).
///
/// Reads responses until the definitive signal arrives. Untagged
/// responses are emitted as events. BYE transitions state to Logout.
///
/// The driver owns this wait outright. It was lifted out of the pre-driver
/// connection layer, which retains no copy of it.
///
/// This wait has two modes, and which one applies is decided entirely by
/// whether the caller has other commands outstanding.
///
/// With `routing: None` - single-command dispatch - exactly one
/// command is on the wire, `own_tag`, and any untagged response has no
/// consumer to route to and becomes an event. A tagged response ends the
/// wait:
///
/// * its own `NO`/`BAD` is the refusal of the command, an ordinary error that
///   leaves the connection usable (the server ended the command, and the
///   unsent remainder is never written);
/// * its own `OK` means the server completed the command without granting
///   the literal it was owed a `+` for. The exchange is over and the framing
///   intact, so this is the non-fatal `ProtocolMissing`, and the remainder is
///   likewise never written (it would be read as a new command line);
/// * any other tag cannot belong to anything, since nothing else is
///   outstanding: a desynchronization, `Protocol`, connection-fatal.
///
/// In pipeline mode `own_tag` is the command being written; the router
/// carries the same fact in `sending` and decides by its own rules.
///
/// With `routing: Some(..)` the caller is `run_pipeline_batch`, which sends
/// command by command whenever any command of the batch has a synchronizing
/// literal (a segment boundary in its encoded form). Commands already on
/// the wire then have tags still pending, and their responses arrive HERE. So
/// this wait routes them through the pipeline's own router rather than
/// answering them itself:
///
/// * a tagged response for an earlier command is that command's completion,
///   routed into its result slot;
/// * an untagged response solicited by an earlier command reaches the
///   consumer that asked for it;
/// * the tagged `NO`/`BAD` of the command being written is its own ordinary
///   per-command result, and the wait answers `Rejected` so the sender
///   abandons the rest of that command rather than failing the batch;
/// * a tagged `OK` for the command being written is that command's result too,
///   as the same non-fatal `ProtocolMissing` the single-command mode raises
///   (see [`early_ok_error`]), and the wait answers `Rejected`. Nothing of any
///   later command has been written - the send phase is sequential whenever a
///   literal is synchronizing - so the server cannot read a later command's
///   bytes as the missing literal, and the next command goes out as a fresh
///   command line exactly as after a `NO`.
///
/// Both halves matter. Answering a foreign tagged response here destroyed an
/// earlier command's real result and reported its `NO` as a failure of the
/// command being written; answering a foreign untagged response as an
/// anonymous event was worse, because the batch then COMPLETED and the caller
/// got a silently short result with no error at all.
pub(super) async fn wait_for_continuation(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    own_tag: &str,
    mut routing: Option<&mut PipelineRouting<'_>>,
    literal_budget: Option<usize>,
) -> Result<ContinuationOutcome, Error> {
    loop {
        let utf8 = super::utf8_mode(state);
        // Accepted limit: an own-tag reply here is classified non-fatal even
        // when an EARLIER literal of the same command was already granted, so
        // the inter-literal text has been written. That holds because the one
        // caller, `send_wire_command`, ends every segment on its synchronizing
        // marker, so the marker is the last byte written before this wait, and
        // a conforming server answers only
        // at a marker (RFC 3502 allows a `NO` there for a later MULTIAPPEND
        // message), so it has read everything sent and the wire is in sync,
        // exactly as for the first literal. A server that replied straight
        // after a literal body with no marker would leave the unread
        // remainder to be parsed as a new command line. Making every reply
        // after a granted literal fatal would close that but drop the
        // connection on the legitimate mid-MULTIAPPEND `NO`. This changes only
        // if a real server is shown replying without a marker, or if a caller
        // ever writes past the marker before waiting.
        //
        // We have already sent the pre-literal bytes; a transport failure
        // reading the server's continuation grant is InFlight.
        let resp = wire_reader
            .read_one_limited(
                utf8,
                routing
                    .as_deref()
                    .map_or(literal_budget, PipelineRouting::literal_budget),
            )
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;

        if let Some(routing) = routing.as_deref_mut() {
            // The router applies side effects itself, exactly once.
            match route_pipeline_response(state, event_sink, routing, resp)? {
                Routed::Continuation => return Ok(ContinuationOutcome::Granted),
                Routed::OwnTagRejected => return Ok(ContinuationOutcome::Rejected),
                Routed::Continue => continue,
            }
        }

        // Our own tagged OK before the `+`: the command did not execute, so
        // the effects armed for it (`in_auth`, `in_select`, the NOTIFY
        // registration) must not complete on this OK. The pipeline router
        // withholds them the same way; a NO/BAD keeps them.
        if matches!(
            resp,
            crate::types::Response::Tagged(ref t)
                if t.tag == own_tag && t.status == StatusKind::Ok
        ) {
            state.disarm_pending_effects();
        }
        let digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Continuation(_) => return Ok(ContinuationOutcome::Granted),
            crate::types::Response::Tagged(t) if t.tag == own_tag => {
                // The server ended the command before the literal was sent.
                // It processed the prefix, so this is Acknowledged. Side
                // effects already applied (RFC 3501 Section4.3), except an
                // early OK's, disarmed above.
                super::emit_tagged_response_code_events(&t, event_sink);
                return match t.status {
                    // NO/BAD are tagged responses - inherently Acknowledged.
                    StatusKind::No => Err(Error::no_with_code(t.text, t.code)),
                    StatusKind::Bad => Err(Error::bad_with_code(t.text, t.code)),
                    StatusKind::Ok => Err(early_ok_error()),
                };
            }
            crate::types::Response::Tagged(t) => {
                return Err(Error::Protocol(format!(
                    "unexpected tag {:?} while waiting for a literal continuation \
                     (expected {own_tag:?})",
                    t.tag,
                )));
            }
            crate::types::Response::Untagged(u) => {
                // (I13): the shared arm emits alert/notification overflow
                // before BYE handling, so ALERT codes on BYE responses are
                // not lost.
                // The pre-literal bytes are on the wire: a BYE is InFlight.
                super::process_untagged_as_event(
                    digest,
                    u,
                    event_sink,
                    TransmissionState::InFlight,
                )?;
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol("unexpected greeting".into()));
            }
        }
    }
}

#[cfg(test)]
#[path = "wire_send_tests.rs"]
mod tests;
