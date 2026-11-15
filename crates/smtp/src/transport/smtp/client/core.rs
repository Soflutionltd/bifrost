//! Sans-I/O protocol core for the SMTP and LMTP send paths.
//!
//! Every rule about WHAT the client says next, HOW MANY replies it must drain
//! before the stream is reusable again, WHICH `SmtpCommandPhase` decorates a
//! failure, WHEN a transaction is cleared with `RSET` versus abandoned with an
//! abort, and how each outcome lands in `SendProgress` lives here, once. The
//! blocking and async drivers are adapters: they move bytes, apply deadlines,
//! and perform the small vocabulary of [`Op`]s this module asks for.
//!
//! The shape is a step function rather than a straight-line routine because
//! the two adapters cannot share control flow - one blocks, one awaits. A
//! machine is asked for its next [`Op`], the adapter performs it, and the
//! result is fed back as an [`OpOutcome`]. Nothing here touches a socket, a
//! timeout, or a clock; a machine can be driven to completion in a unit test
//! with no I/O at all, which is what the `core_tests` module does.
//!
//! What deliberately stays adapter-side, because it is I/O rather than
//! protocol: connecting, the greeting and EHLO/LHLO exchange, the AUTH ladder,
//! the STARTTLS upgrade, reply-line reading and parsing, read/write deadlines,
//! throttling, and the socket shutdown that `Op::Abort` performs.

use super::PhasedError;
use crate::transport::smtp::{
    Protocol,
    account_error::{SmtpErrorContext, into_account_error},
    batch::{RcptProgress, SendProgress},
    commands::{Bdat, Data, Mail, Rcpt, Rset},
    error,
    error::{Error, SmtpCommandPhase, SmtpTransmissionState},
    response::Response,
};

/// One unit of I/O the adapter performs on the machine's behalf.
///
/// The vocabulary is deliberately small, and every variant maps to a primitive
/// both drivers already had. Nothing in it names a timeout, a socket, or a
/// clock.
#[derive(Debug)]
pub(super) enum Op {
    /// Write these command bytes and flush.
    Write(String),
    /// Write the DATA body (transparency-encoded) followed by the terminator.
    WriteBody,
    /// Write a `BDAT <len> LAST` header, then the raw payload.
    WriteBdat,
    /// Verify the stream is usable, then hold it `Broken` for a reply group
    /// whose replies must all drain before it is reusable again.
    OpenReplyGroup,
    /// Read one reply inside an open group: negative statuses are values, and
    /// the surplus check is deferred to `CloseReplyGroup`.
    ReadGrouped,
    /// Read one reply for a single outstanding command, managing the stream
    /// state and the surplus check itself. Negative statuses are values.
    ReadSingle,
    /// Close a reply group: fail on surplus bytes, otherwise restore the
    /// stream.
    CloseReplyGroup,
    /// Close an LMTP final-status drain: retire the connection, fail on
    /// surplus bytes, and restore the stream only when asked.
    CloseLmtpDrain { restore_ok: bool },
    /// Close the connection and mark it `Broken`.
    Abort,
}

/// What came back from an [`Op`].
#[derive(Debug)]
pub(super) enum OpOutcome {
    /// The op completed and produced no reply. Also the kick-off value for the
    /// machine's first step.
    Done,
    /// A reply was read. Negative statuses arrive here, not as `Failed`.
    Reply(Response),
    /// The op failed at the transport or parse level.
    Failed(Error),
}

/// What the machine wants next.
#[derive(Debug)]
pub(super) enum Step<T> {
    Run(Op),
    Finish(T),
}

/// A protocol state machine driven by an I/O adapter.
pub(super) trait ProtocolMachine {
    type Output;

    /// Feed the previous op's outcome (or [`OpOutcome::Done`] to start) and
    /// get the next op, or the finished result.
    fn step(&mut self, outcome: OpOutcome) -> Step<Self::Output>;
}

/// The two terminal sequences every machine shares.
///
/// `Abort` closes the connection; `Reset` is the RSET-and-keep rule - the
/// connection survives when the peer positively acknowledges the reset and is
/// aborted otherwise. Both carry the value the machine will finish with, so a
/// caller cannot forget to return after starting one.
enum Epilogue<T> {
    Aborting(T),
    ResetWriting(T),
    ResetReading(T),
}

impl<T> Epilogue<T> {
    /// Abort the connection, then finish with `value`.
    fn abort(value: T) -> (Self, Op) {
        (Epilogue::Aborting(value), Op::Abort)
    }

    /// Clear the open transaction with RSET, keeping the connection if the
    /// peer acknowledges it, then finish with `value`.
    fn reset(value: T) -> (Self, Op) {
        (Epilogue::ResetWriting(value), Op::Write(Rset.to_string()))
    }

    fn advance(self, outcome: OpOutcome) -> Result<(Self, Op), T> {
        match self {
            Epilogue::Aborting(value) => Err(value),
            Epilogue::ResetWriting(value) => match outcome {
                OpOutcome::Failed(_) => Ok(Epilogue::abort(value)),
                _ => Ok((Epilogue::ResetReading(value), Op::ReadSingle)),
            },
            Epilogue::ResetReading(value) => match outcome {
                OpOutcome::Reply(response) if response.is_completion() => Err(value),
                _ => Ok(Epilogue::abort(value)),
            },
        }
    }
}

/// Every direct-SMTP exit is built here, and [`PhasedError`] has no
/// phase-less constructor, so a boundary added later cannot ship undecorated.
fn phased(phase: SmtpCommandPhase, error: Error) -> Result<Response, PhasedError> {
    Err(PhasedError::new(phase, error))
}

/// Whether a negative reply is the peer announcing that it is hanging up.
///
/// `421` is "service not available, closing transmission channel" (RFC 5321
/// 4.2.3), and it overrides every reset-or-keep rule below: those rules reason
/// about the TRANSACTION - a rejected `MAIL FROM` opened none, so nothing to
/// reset; a rejected `RCPT TO` or `DATA` is cleared with RSET - while a 421 is
/// about the CONNECTION. Parking a socket the peer just said it is closing
/// hands the next checkout a dead connection, which under
/// `test_on_checkout(false)` surfaces as an `Unsent` failure on a send that had
/// nothing to do with this one. So every arm that decides what to do with a
/// negative reply asks this FIRST and aborts on `true`, carrying the same status
/// error (or the same per-recipient record) it would otherwise have produced.
/// One check in one place: the end-of-data arms had it and the envelope arms
/// did not, and a 421 to `MAIL FROM` parked the dying connection.
fn closing_channel(response: &Response) -> bool {
    response.has_code(421)
}

/// Which body framing a send uses. `Bdat` skips the `DATA` command entirely.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BodyKind {
    Data,
    Bdat,
}

impl BodyKind {
    fn write_op(self) -> Op {
        match self {
            BodyKind::Data => Op::WriteBody,
            BodyKind::Bdat => Op::WriteBdat,
        }
    }

    /// The phase a body-upload failure carries. BDAT has its own, because the
    /// classifier distinguishes a chunked upload from a DATA section. A
    /// *rejected* DATA end-of-data reply is `DataFinal` instead; BDAT has no
    /// distinct final phase and keeps this one.
    fn body_phase(self) -> SmtpCommandPhase {
        match self {
            BodyKind::Data => SmtpCommandPhase::DataBody,
            BodyKind::Bdat => SmtpCommandPhase::BdatBody,
        }
    }
}

/// Recipient windows for the PIPELINING path.
///
/// RFC 2920 requires respecting the peer's TCP window, so recipients go out in
/// bounded windows and every reply in a window drains before the next window
/// is written. The original recipient indexes are preserved across windows,
/// which is what keeps `SendProgress` lanes aligned with the caller's list.
fn window_bounds(total: usize, start: usize) -> usize {
    (start + super::PIPELINING_RECIPIENT_WINDOW).min(total)
}

fn window_commands(mail: &Mail, recipients: &[Rcpt], start: usize, end: usize) -> String {
    let mut commands = String::new();
    if start == 0 {
        commands.push_str(&mail.to_string());
    }
    for recipient in &recipients[start..end] {
        commands.push_str(&recipient.to_string());
    }
    commands
}

// ---------------------------------------------------------------------------
// Direct SMTP send
// ---------------------------------------------------------------------------

/// `SmtpConnection::send_with_options` / `send_bdat_with_options`, pipelined or
/// not.
///
/// The output error is built as a [`PhasedError`], which has no phase-less
/// constructor, so no exit from this machine can ship undecorated.
pub(super) struct DirectSmtp {
    mail: Mail,
    recipients: Vec<Rcpt>,
    pipelined: bool,
    body: BodyKind,
    stage: DirectSmtpStage,
    epilogue: Option<Epilogue<Result<Response, PhasedError>>>,
}

enum DirectSmtpStage {
    Start,
    /// A command write is outstanding. On success the reply for it is read and
    /// the inner stage handles it; a write failure is handed to that same
    /// stage, which owns the phase for this boundary either way.
    Writing(Box<DirectSmtpStage>),
    MailReply,
    RcptReply(usize),
    WindowWritten(usize),
    WindowOpened(usize),
    MailWindowReply(usize),
    /// A rejected pipelined `MAIL FROM`: the window's RCPT replies still have
    /// to drain before the stream is reusable, and only then is the rejection
    /// reported. No RSET - a rejected `MAIL FROM` opened no transaction.
    MailRejectedDrain {
        remaining: usize,
        response: Option<Response>,
    },
    MailRejectedClosing(Response),
    RcptWindowReply {
        end: usize,
        next: usize,
        failure: Option<Response>,
        /// Whether ANY reply so far in the window was a 421, kept apart from
        /// `failure` because only the first negative reply is retained.
        closing: bool,
    },
    WindowClosing {
        end: usize,
        failure: Option<Response>,
        closing: bool,
    },
    DataReply,
    BodyWritten,
    FinalReply,
}

impl DirectSmtp {
    pub(super) fn new(mail: Mail, recipients: Vec<Rcpt>, pipelined: bool, body: BodyKind) -> Self {
        Self {
            mail,
            recipients,
            pipelined,
            body,
            stage: DirectSmtpStage::Start,
            epilogue: None,
        }
    }

    /// Envelope done: issue `DATA`, or go straight to the body for BDAT.
    fn after_envelope(&mut self) -> Step<Result<Response, Error>> {
        match self.body {
            BodyKind::Data => self.write_then(DirectSmtpStage::DataReply, Data.to_string()),
            BodyKind::Bdat => {
                self.stage = DirectSmtpStage::BodyWritten;
                Step::Run(self.body.write_op())
            }
        }
    }

    fn start_window(&mut self, start: usize) -> Step<Result<Response, Error>> {
        if start >= self.recipients.len() {
            return self.after_envelope();
        }
        let end = window_bounds(self.recipients.len(), start);
        let commands = window_commands(&self.mail, &self.recipients, start, end);
        self.stage = DirectSmtpStage::WindowWritten(start);
        Step::Run(Op::Write(commands))
    }

    fn begin_epilogue(
        &mut self,
        epilogue: (Epilogue<Result<Response, PhasedError>>, Op),
    ) -> Step<Result<Response, Error>> {
        let (epilogue, op) = epilogue;
        self.epilogue = Some(epilogue);
        Step::Run(op)
    }
}

impl DirectSmtp {
    /// Write a command, then read the single reply it owes into `next`.
    fn write_then(
        &mut self,
        next: DirectSmtpStage,
        bytes: String,
    ) -> Step<Result<Response, Error>> {
        self.stage = DirectSmtpStage::Writing(Box::new(next));
        Step::Run(Op::Write(bytes))
    }
}

impl ProtocolMachine for DirectSmtp {
    type Output = Result<Response, Error>;

    fn step(&mut self, outcome: OpOutcome) -> Step<Self::Output> {
        if let Some(epilogue) = self.epilogue.take() {
            return match epilogue.advance(outcome) {
                Ok((epilogue, op)) => {
                    self.epilogue = Some(epilogue);
                    Step::Run(op)
                }
                Err(value) => Step::Finish(value.map_err(PhasedError::into_error)),
            };
        }

        match std::mem::replace(&mut self.stage, DirectSmtpStage::Start) {
            DirectSmtpStage::Writing(next) => {
                self.stage = *next;
                match outcome {
                    OpOutcome::Failed(error) => self.step(OpOutcome::Failed(error)),
                    _ => Step::Run(Op::ReadSingle),
                }
            }
            DirectSmtpStage::Start => {
                // The empty recipient list is excluded from the pipelined
                // branch rather than left to the caller: `start_window(0)`
                // would find nothing to send, fall straight through to
                // `after_envelope`, and write `DATA` with no `MAIL FROM` ahead
                // of it. `Envelope::new` rejects an empty list, so no
                // production caller reaches this - but the machine is the
                // authority on its own command sequence, and "a type upstream
                // happens to forbid it" is not an invariant this file can see.
                // The sequential branch opens the transaction correctly for
                // any recipient count.
                if self.pipelined && !self.recipients.is_empty() {
                    return self.start_window(0);
                }
                let bytes = self.mail.to_string();
                self.write_then(DirectSmtpStage::MailReply, bytes)
            }
            DirectSmtpStage::MailReply => match outcome {
                OpOutcome::Failed(error) => {
                    let value = phased(SmtpCommandPhase::MailFrom, error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                OpOutcome::Reply(response) if response.is_completion() => {
                    if self.recipients.is_empty() {
                        return self.after_envelope();
                    }
                    let bytes = self.recipients[0].to_string();
                    self.write_then(DirectSmtpStage::RcptReply(0), bytes)
                }
                OpOutcome::Reply(response) => {
                    let closing = closing_channel(&response);
                    let value = phased(SmtpCommandPhase::MailFrom, error::status(response));
                    if closing {
                        return self.begin_epilogue(Epilogue::abort(value));
                    }
                    // No RSET and no abort: a rejected MAIL FROM opened no
                    // transaction, so the connection stays reusable as it is.
                    Step::Finish(value.map_err(PhasedError::into_error))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            DirectSmtpStage::RcptReply(index) => match outcome {
                OpOutcome::Failed(error) => {
                    let value = phased(SmtpCommandPhase::RcptTo, error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                OpOutcome::Reply(response) if response.is_completion() => {
                    let next = index + 1;
                    if next >= self.recipients.len() {
                        return self.after_envelope();
                    }
                    let bytes = self.recipients[next].to_string();
                    self.write_then(DirectSmtpStage::RcptReply(next), bytes)
                }
                OpOutcome::Reply(response) => {
                    let closing = closing_channel(&response);
                    let value = phased(SmtpCommandPhase::RcptTo, error::status(response));
                    if closing {
                        return self.begin_epilogue(Epilogue::abort(value));
                    }
                    self.begin_epilogue(Epilogue::reset(value))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            DirectSmtpStage::WindowWritten(start) => match outcome {
                OpOutcome::Failed(error) => {
                    let phase = if start == 0 {
                        SmtpCommandPhase::MailFrom
                    } else {
                        SmtpCommandPhase::RcptTo
                    };
                    let value = phased(phase, error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                _ => {
                    self.stage = DirectSmtpStage::WindowOpened(start);
                    Step::Run(Op::OpenReplyGroup)
                }
            },
            DirectSmtpStage::WindowOpened(start) => match outcome {
                OpOutcome::Failed(error) => {
                    let phase = if start == 0 {
                        SmtpCommandPhase::MailFrom
                    } else {
                        SmtpCommandPhase::RcptTo
                    };
                    let value = phased(phase, error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                _ => {
                    let end = window_bounds(self.recipients.len(), start);
                    if start == 0 {
                        self.stage = DirectSmtpStage::MailWindowReply(end);
                    } else {
                        self.stage = DirectSmtpStage::RcptWindowReply {
                            end,
                            next: start,
                            failure: None,
                            closing: false,
                        };
                    }
                    Step::Run(Op::ReadGrouped)
                }
            },
            DirectSmtpStage::MailWindowReply(end) => match outcome {
                OpOutcome::Failed(error) => {
                    let value = phased(SmtpCommandPhase::MailFrom, error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                OpOutcome::Reply(response) if response.is_completion() => {
                    self.stage = DirectSmtpStage::RcptWindowReply {
                        end,
                        next: 0,
                        failure: None,
                        closing: false,
                    };
                    Step::Run(Op::ReadGrouped)
                }
                OpOutcome::Reply(response) => {
                    if closing_channel(&response) {
                        // The peer is hanging up; the window's RCPT replies
                        // may never arrive, and draining them would only turn
                        // the status error into a transport failure.
                        let value = phased(SmtpCommandPhase::MailFrom, error::status(response));
                        return self.begin_epilogue(Epilogue::abort(value));
                    }
                    self.stage = DirectSmtpStage::MailRejectedDrain {
                        remaining: end,
                        response: Some(response),
                    };
                    self.step(OpOutcome::Done)
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            DirectSmtpStage::MailRejectedDrain {
                remaining,
                mut response,
            } => match outcome {
                OpOutcome::Failed(_) => {
                    // The send's outcome is already decided: the peer rejected
                    // `MAIL FROM`, so nothing was transmitted and the rest of
                    // the window's replies were bookkeeping for the STREAM, not
                    // for the transaction. Reporting the drain's transport
                    // failure instead threw that rejection away - the caller
                    // got a retryable network error where the server had
                    // permanently refused the envelope, and never saw the reply
                    // text saying why. The stream is still aborted.
                    let response = response.take().expect("the rejection is carried through");
                    let value = phased(SmtpCommandPhase::MailFrom, error::status(response));
                    self.begin_epilogue(Epilogue::abort(value))
                }
                _ => {
                    if remaining == 0 {
                        let response = response.take().expect("the rejection is carried through");
                        self.stage = DirectSmtpStage::MailRejectedClosing(response);
                        return Step::Run(Op::CloseReplyGroup);
                    }
                    self.stage = DirectSmtpStage::MailRejectedDrain {
                        remaining: remaining - 1,
                        response,
                    };
                    Step::Run(Op::ReadGrouped)
                }
            },
            DirectSmtpStage::MailRejectedClosing(response) => match outcome {
                // Same rule as the drain above, and the same value this
                // stage's success arm produces: a surplus-bytes or verify
                // failure at the group close costs the connection, not the
                // answer the peer already gave to `MAIL FROM`.
                OpOutcome::Failed(_) => {
                    let value = phased(SmtpCommandPhase::MailFrom, error::status(response));
                    self.begin_epilogue(Epilogue::abort(value))
                }
                _ => Step::Finish(
                    phased(SmtpCommandPhase::MailFrom, error::status(response))
                        .map_err(PhasedError::into_error),
                ),
            },
            DirectSmtpStage::RcptWindowReply {
                end,
                next,
                failure,
                closing,
            } => match outcome {
                OpOutcome::Failed(error) => {
                    let value = phased(SmtpCommandPhase::RcptTo, error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                OpOutcome::Reply(response) => {
                    // Only the FIRST negative reply in the window is kept; a
                    // later one is read, counted, and dropped. Deliberate and
                    // lossy, on the same reasoning as `BatchSmtpStage::
                    // WindowOpened`: this machine's output is a single
                    // `Response`, and the direct path is all-or-nothing - any
                    // refused recipient ends the envelope for every recipient,
                    // so there is no lane a second refusal could land in. The
                    // caller learns that a recipient was refused and the text of
                    // the first refusal; it never learns WHICH recipient, in
                    // this arm or any other, because the output carries no
                    // per-recipient identity at all. A caller that needs the
                    // full per-recipient answer uses the batch machine, which
                    // records every reply into `SendProgress`.
                    //
                    // The retention rule does not extend to the 421 question:
                    // whether the channel is closing is about the CONNECTION,
                    // not about which refusal is reported, so every reply in
                    // the window is asked, as `BatchSmtpStage::RcptWindowReply`
                    // does. A 550 followed by a 421 reports the 550 and still
                    // aborts, rather than spending an RSET on a channel the
                    // peer has said it is closing.
                    //
                    // What would change the retention answer: giving this path
                    // a per-recipient output (the batch machine's
                    // `SendProgress`, or a multi-response error). Until the
                    // output shape can carry a second refusal, retaining one is
                    // the whole of what is reportable.
                    let closing = closing || closing_channel(&response);
                    let failure = match failure {
                        Some(failure) => Some(failure),
                        None if !response.is_completion() => Some(response),
                        None => None,
                    };
                    let next = next + 1;
                    if next < end {
                        self.stage = DirectSmtpStage::RcptWindowReply {
                            end,
                            next,
                            failure,
                            closing,
                        };
                        return Step::Run(Op::ReadGrouped);
                    }
                    self.stage = DirectSmtpStage::WindowClosing {
                        end,
                        failure,
                        closing,
                    };
                    Step::Run(Op::CloseReplyGroup)
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            DirectSmtpStage::WindowClosing {
                end,
                failure,
                closing,
            } => match outcome {
                OpOutcome::Failed(error) => {
                    let value = phased(SmtpCommandPhase::RcptTo, error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                _ => match failure {
                    Some(response) => {
                        let value = phased(SmtpCommandPhase::RcptTo, error::status(response));
                        if closing {
                            return self.begin_epilogue(Epilogue::abort(value));
                        }
                        self.begin_epilogue(Epilogue::reset(value))
                    }
                    None => self.start_window(end),
                },
            },
            DirectSmtpStage::DataReply => match outcome {
                OpOutcome::Failed(error) => {
                    let value = phased(SmtpCommandPhase::DataCommand, error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                OpOutcome::Reply(response) if response.has_code(354) => {
                    self.stage = DirectSmtpStage::BodyWritten;
                    Step::Run(self.body.write_op())
                }
                OpOutcome::Reply(response) => {
                    let closing = closing_channel(&response);
                    let value = phased(SmtpCommandPhase::DataCommand, error::status(response));
                    if closing {
                        return self.begin_epilogue(Epilogue::abort(value));
                    }
                    self.begin_epilogue(Epilogue::reset(value))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            DirectSmtpStage::BodyWritten => match outcome {
                OpOutcome::Failed(error) => {
                    let phase = self.body.body_phase();
                    let value = phased(phase, error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                _ => {
                    self.stage = DirectSmtpStage::FinalReply;
                    Step::Run(Op::ReadSingle)
                }
            },
            DirectSmtpStage::FinalReply => match outcome {
                OpOutcome::Reply(response) if response.is_intermediate() => {
                    let value = phased(
                        match self.body {
                            BodyKind::Data => SmtpCommandPhase::DataFinal,
                            BodyKind::Bdat => SmtpCommandPhase::BdatBody,
                        },
                        error::parse("intermediate reply after message body")
                            .with_attempt(SmtpTransmissionState::InFlight),
                    );
                    self.begin_epilogue(Epilogue::abort(value))
                }
                OpOutcome::Reply(response) if response.is_completion() => {
                    Step::Finish(Ok(response))
                }
                OpOutcome::Reply(response) => {
                    // `DataFinal` names the reply to the dot terminator, which
                    // is what the batch path already tags. BDAT has no
                    // distinct final phase - `BDAT ... LAST` is the body write
                    // - so a rejected chunk stays `BdatBody`.
                    let phase = match self.body {
                        BodyKind::Data => SmtpCommandPhase::DataFinal,
                        BodyKind::Bdat => SmtpCommandPhase::BdatBody,
                    };
                    let closing = closing_channel(&response);
                    let value = phased(phase, error::status(response));
                    if closing {
                        // Whatever the body framing, the peer is hanging up
                        // (`closing_channel`), so parking this connection
                        // would hand the next checkout a dead socket. The
                        // status error is still what the caller sees.
                        self.begin_epilogue(Epilogue::abort(value))
                    } else {
                        match self.body {
                            // DATA: the peer answered the end-of-data
                            // terminator and refused the message. The
                            // transaction is complete (RFC 5321 4.1.1.4),
                            // there is nothing to reset, and the connection
                            // stays reusable - the same rule a rejected
                            // `MAIL FROM` follows.
                            BodyKind::Data => Step::Finish(value.map_err(PhasedError::into_error)),
                            // BDAT: RFC 3030 makes no equivalent
                            // transaction-complete promise for a refused
                            // chunk, and its own failure example sends RSET
                            // after the negative reply, so the transaction may
                            // still be open on a strict server. Keep the
                            // connection only when the protocol guarantees its
                            // state: RSET-and-keep, aborting unless the peer
                            // acknowledges the reset.
                            BodyKind::Bdat => self.begin_epilogue(Epilogue::reset(value)),
                        }
                    }
                }
                OpOutcome::Failed(error) => {
                    let value = phased(self.body.body_phase(), error);
                    self.begin_epilogue(Epilogue::abort(value))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Direct LMTP send
// ---------------------------------------------------------------------------

/// `send_lmtp_with_options` / `send_lmtp_bdat_with_options`.
///
/// LMTP answers once per accepted recipient after the body, so the reply group
/// is opened before the drain and closed after it, and the connection is
/// retired either way.
pub(super) struct DirectLmtp {
    mail: Mail,
    recipients: Vec<Rcpt>,
    body: BodyKind,
    statuses: Vec<Option<Response>>,
    accepted: usize,
    delivery: Vec<Response>,
    stage: DirectLmtpStage,
    epilogue: Option<Epilogue<Result<Vec<Response>, Error>>>,
}

enum DirectLmtpStage {
    Start,
    /// A command write is outstanding. On success the reply for it is read and
    /// the inner stage handles it; a write failure is handed to that same
    /// stage, which owns the phase for this boundary either way.
    Writing(Box<DirectLmtpStage>),
    MailReply,
    RcptReply(usize),
    DataReply,
    BodyWritten,
    GroupOpened,
    FinalStatus(usize),
    DrainClosing,
}

impl DirectLmtp {
    pub(super) fn new(mail: Mail, recipients: Vec<Rcpt>, body: BodyKind) -> Self {
        let capacity = recipients.len();
        Self {
            mail,
            recipients,
            body,
            statuses: Vec::with_capacity(capacity),
            accepted: 0,
            delivery: Vec::new(),
            stage: DirectLmtpStage::Start,
            epilogue: None,
        }
    }

    fn begin_epilogue(
        &mut self,
        epilogue: (Epilogue<Result<Vec<Response>, Error>>, Op),
    ) -> Step<Result<Vec<Response>, Error>> {
        let (epilogue, op) = epilogue;
        self.epilogue = Some(epilogue);
        Step::Run(op)
    }

    fn abort_with(
        &mut self,
        phase: SmtpCommandPhase,
        error: Error,
    ) -> Step<Result<Vec<Response>, Error>> {
        let value = Err(error.or_phase(phase));
        self.begin_epilogue(Epilogue::abort(value))
    }

    fn after_envelope(&mut self) -> Step<Result<Vec<Response>, Error>> {
        if self.accepted == 0 {
            // Every recipient was rejected: clear the transaction and report
            // the per-recipient statuses. Not a failure of the send.
            let rejected = self
                .statuses
                .iter()
                .map(|status| {
                    status
                        .clone()
                        .expect("no recipient was accepted, so every status is a rejection")
                })
                .collect();
            return self.begin_epilogue(Epilogue::reset(Ok(rejected)));
        }
        match self.body {
            BodyKind::Data => self.write_then(DirectLmtpStage::DataReply, Data.to_string()),
            BodyKind::Bdat => {
                self.stage = DirectLmtpStage::BodyWritten;
                Step::Run(self.body.write_op())
            }
        }
    }
}

impl DirectLmtp {
    /// Write a command, then read the single reply it owes into `next`.
    fn write_then(
        &mut self,
        next: DirectLmtpStage,
        bytes: String,
    ) -> Step<Result<Vec<Response>, Error>> {
        self.stage = DirectLmtpStage::Writing(Box::new(next));
        Step::Run(Op::Write(bytes))
    }
}

impl ProtocolMachine for DirectLmtp {
    type Output = Result<Vec<Response>, Error>;

    fn step(&mut self, outcome: OpOutcome) -> Step<Self::Output> {
        if let Some(epilogue) = self.epilogue.take() {
            return match epilogue.advance(outcome) {
                Ok((epilogue, op)) => {
                    self.epilogue = Some(epilogue);
                    Step::Run(op)
                }
                Err(value) => Step::Finish(value),
            };
        }

        match std::mem::replace(&mut self.stage, DirectLmtpStage::Start) {
            DirectLmtpStage::Writing(next) => {
                self.stage = *next;
                match outcome {
                    OpOutcome::Failed(error) => self.step(OpOutcome::Failed(error)),
                    _ => Step::Run(Op::ReadSingle),
                }
            }
            DirectLmtpStage::Start => {
                let bytes = self.mail.to_string();
                self.write_then(DirectLmtpStage::MailReply, bytes)
            }
            DirectLmtpStage::MailReply => match outcome {
                OpOutcome::Failed(error) => self.abort_with(SmtpCommandPhase::MailFrom, error),
                OpOutcome::Reply(response) if response.is_completion() => {
                    if self.recipients.is_empty() {
                        return self.after_envelope();
                    }
                    let bytes = self.recipients[0].to_string();
                    self.write_then(DirectLmtpStage::RcptReply(0), bytes)
                }
                OpOutcome::Reply(response) => {
                    if closing_channel(&response) {
                        return self
                            .abort_with(SmtpCommandPhase::MailFrom, error::status(response));
                    }
                    // The SMTP rule, which LMTP (RFC 2033) inherits unchanged:
                    // a rejected MAIL FROM opened no transaction, so there is
                    // nothing to reset and nothing to abort. Aborting here
                    // cost one reconnect per rejected envelope on a
                    // local-delivery socket, and made the direct LMTP path
                    // differ from both `DirectSmtp` and `BatchLmtp`.
                    Step::Finish(Err(
                        error::status(response).or_phase(SmtpCommandPhase::MailFrom)
                    ))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            DirectLmtpStage::RcptReply(index) => match outcome {
                OpOutcome::Failed(error) => self.abort_with(SmtpCommandPhase::RcptTo, error),
                OpOutcome::Reply(response) => {
                    if closing_channel(&response) {
                        return self.abort_with(SmtpCommandPhase::RcptTo, error::status(response));
                    }
                    if response.is_completion() {
                        self.accepted += 1;
                        self.statuses.push(None);
                    } else {
                        self.statuses.push(Some(response));
                    }
                    let next = index + 1;
                    if next >= self.recipients.len() {
                        return self.after_envelope();
                    }
                    let bytes = self.recipients[next].to_string();
                    self.write_then(DirectLmtpStage::RcptReply(next), bytes)
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            DirectLmtpStage::DataReply => match outcome {
                OpOutcome::Failed(error) => self.abort_with(SmtpCommandPhase::DataCommand, error),
                OpOutcome::Reply(response) if response.has_code(354) => {
                    self.stage = DirectLmtpStage::BodyWritten;
                    Step::Run(self.body.write_op())
                }
                OpOutcome::Reply(response) => {
                    if closing_channel(&response) {
                        return self
                            .abort_with(SmtpCommandPhase::DataCommand, error::status(response));
                    }
                    // RSET-and-keep, as `DirectSmtp` and `BatchLmtp` do at this
                    // boundary: the transaction is open, the peer refused to
                    // take a body, and a positively acknowledged reset leaves
                    // the connection reusable.
                    let value =
                        Err(error::status(response).or_phase(SmtpCommandPhase::DataCommand));
                    self.begin_epilogue(Epilogue::reset(value))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            DirectLmtpStage::BodyWritten => match outcome {
                OpOutcome::Failed(error) => {
                    // The framing decides the phase, exactly as `DirectSmtp`
                    // does: a failed `BDAT` chunk is `BdatBody`, not
                    // `DataBody`. Hardcoding one of the two made the direct
                    // LMTP path the only send path that mislabels its own body
                    // upload.
                    let phase = self.body.body_phase();
                    self.abort_with(phase, error)
                }
                _ => {
                    self.stage = DirectLmtpStage::GroupOpened;
                    Step::Run(Op::OpenReplyGroup)
                }
            },
            DirectLmtpStage::GroupOpened => match outcome {
                OpOutcome::Failed(error) => {
                    self.abort_with(SmtpCommandPhase::LmtpFinalStatus, error)
                }
                _ => {
                    self.delivery = Vec::with_capacity(self.accepted);
                    self.stage = DirectLmtpStage::FinalStatus(0);
                    Step::Run(Op::ReadGrouped)
                }
            },
            DirectLmtpStage::FinalStatus(index) => match outcome {
                OpOutcome::Reply(response) if response.is_intermediate() => self.abort_with(
                    SmtpCommandPhase::LmtpFinalStatus,
                    error::parse("intermediate LMTP final status")
                        .with_attempt(SmtpTransmissionState::InFlight),
                ),
                OpOutcome::Failed(error) => {
                    self.abort_with(SmtpCommandPhase::LmtpFinalStatus, error)
                }
                OpOutcome::Reply(response) => {
                    self.delivery.push(response);
                    let next = index + 1;
                    if next < self.accepted {
                        self.stage = DirectLmtpStage::FinalStatus(next);
                        return Step::Run(Op::ReadGrouped);
                    }
                    self.stage = DirectLmtpStage::DrainClosing;
                    Step::Run(Op::CloseLmtpDrain { restore_ok: true })
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            DirectLmtpStage::DrainClosing => match outcome {
                OpOutcome::Failed(error) => {
                    self.abort_with(SmtpCommandPhase::LmtpFinalStatus, error)
                }
                _ => {
                    let statuses = std::mem::take(&mut self.statuses);
                    let delivery = std::mem::take(&mut self.delivery);
                    // A count mismatch is an internal error, not a wire
                    // failure: the connection is not aborted for it.
                    Step::Finish(super::merge_lmtp_statuses(statuses, delivery))
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Account-oriented batch sends
// ---------------------------------------------------------------------------

type BatchResult = Result<SendProgress, (Error, SendProgress)>;

/// Batch outcomes never leave the machine without their progress tracker, so
/// this pairs the two the way the driver signature does.
enum BatchExit {
    Progress,
    Error(Error),
}

/// `send_smtp_batch`, pipelined or not.
pub(super) struct BatchSmtp {
    mail: Mail,
    recipients: Vec<Rcpt>,
    pipelined: bool,
    progress: Option<SendProgress>,
    stage: BatchSmtpStage,
    epilogue: Option<Epilogue<BatchExit>>,
}

enum BatchSmtpStage {
    Start,
    /// A command write is outstanding. On success the reply for it is read and
    /// the inner stage handles it; a write failure is handed to that same
    /// stage, which owns the phase for this boundary either way.
    Writing(Box<BatchSmtpStage>),
    MailReply,
    RcptReply(usize),
    WindowWritten(usize),
    WindowOpened(usize),
    MailWindowReply(usize),
    RcptWindowReply {
        end: usize,
        next: usize,
    },
    WindowClosing {
        end: usize,
    },
    DataReply,
    BodyWritten,
    FinalReply,
}

impl BatchSmtp {
    pub(super) fn new(
        mail: Mail,
        recipients: Vec<Rcpt>,
        pipelined: bool,
        progress: SendProgress,
    ) -> Self {
        Self {
            mail,
            recipients,
            pipelined,
            progress: Some(progress),
            stage: BatchSmtpStage::Start,
            epilogue: None,
        }
    }

    fn progress(&mut self) -> &mut SendProgress {
        self.progress
            .as_mut()
            .expect("the progress tracker lives until the machine finishes")
    }

    fn take_progress(&mut self) -> SendProgress {
        self.progress
            .take()
            .expect("the progress tracker lives until the machine finishes")
    }

    fn any_accepted(&mut self) -> bool {
        self.progress()
            .recipients
            .iter()
            .any(|recipient| matches!(recipient.rcpt, RcptProgress::Accepted))
    }

    fn account_error(error: Error) -> bifrost_types::error::AccountError {
        into_account_error(error, SmtpErrorContext::send(Protocol::Smtp))
    }

    fn begin_epilogue(&mut self, epilogue: (Epilogue<BatchExit>, Op)) -> Step<BatchResult> {
        let (epilogue, op) = epilogue;
        self.epilogue = Some(epilogue);
        Step::Run(op)
    }

    /// A `421` answered a `RCPT TO`: the peer is closing the channel with the
    /// envelope half-built. The recipient it answered is rejected with that
    /// reply; every other unanswered or accepted recipient is `Unsent`, since
    /// `DATA` was never issued and nothing can have been delivered; and the
    /// connection is aborted rather than parked. See `closing_channel`.
    fn closing_during_envelope(&mut self, index: usize, response: Response) -> Step<BatchResult> {
        let unsent = Self::account_error(
            error::status(response.clone())
                .with_attempt(SmtpTransmissionState::Unsent)
                .with_phase(SmtpCommandPhase::RcptTo),
        );
        self.progress().record_rcpt_rejected(index, response);
        self.progress().mark_unresolved_unsent(|| unsent.clone());
        self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
    }

    fn after_envelope(&mut self) -> Step<BatchResult> {
        if !self.any_accepted() {
            return self.begin_epilogue(Epilogue::reset(BatchExit::Progress));
        }
        self.progress().set_data_issued();
        self.write_then(BatchSmtpStage::DataReply, Data.to_string())
    }

    fn start_window(&mut self, start: usize) -> Step<BatchResult> {
        if start >= self.recipients.len() {
            return self.after_envelope();
        }
        let end = window_bounds(self.recipients.len(), start);
        let commands = window_commands(&self.mail, &self.recipients, start, end);
        self.stage = BatchSmtpStage::WindowWritten(start);
        Step::Run(Op::Write(commands))
    }
}

impl BatchSmtp {
    /// Write a command, then read the single reply it owes into `next`.
    fn write_then(&mut self, next: BatchSmtpStage, bytes: String) -> Step<BatchResult> {
        self.stage = BatchSmtpStage::Writing(Box::new(next));
        Step::Run(Op::Write(bytes))
    }
}

impl ProtocolMachine for BatchSmtp {
    type Output = BatchResult;

    fn step(&mut self, outcome: OpOutcome) -> Step<Self::Output> {
        if let Some(epilogue) = self.epilogue.take() {
            return match epilogue.advance(outcome) {
                Ok((epilogue, op)) => {
                    self.epilogue = Some(epilogue);
                    Step::Run(op)
                }
                Err(exit) => {
                    let progress = self.take_progress();
                    Step::Finish(match exit {
                        BatchExit::Progress => Ok(progress),
                        BatchExit::Error(error) => Err((error, progress)),
                    })
                }
            };
        }

        match std::mem::replace(&mut self.stage, BatchSmtpStage::Start) {
            BatchSmtpStage::Writing(next) => {
                self.stage = *next;
                match outcome {
                    OpOutcome::Failed(error) => self.step(OpOutcome::Failed(error)),
                    _ => Step::Run(Op::ReadSingle),
                }
            }
            BatchSmtpStage::Start => {
                if self.pipelined {
                    return self.start_window(0);
                }
                let bytes = self.mail.to_string();
                self.write_then(BatchSmtpStage::MailReply, bytes)
            }
            BatchSmtpStage::MailReply => match outcome {
                OpOutcome::Reply(response) if response.is_completion() => {
                    if self.recipients.is_empty() {
                        return self.after_envelope();
                    }
                    let bytes = self.recipients[0].to_string();
                    self.write_then(BatchSmtpStage::RcptReply(0), bytes)
                }
                OpOutcome::Reply(response) => {
                    // A server rejection is Acknowledged, not Unsent: the peer
                    // answered.
                    let error = error::status(response)
                        .with_attempt(SmtpTransmissionState::Acknowledged)
                        .with_phase(SmtpCommandPhase::MailFrom);
                    self.begin_epilogue(Epilogue::abort(BatchExit::Error(error)))
                }
                OpOutcome::Failed(error) => {
                    let error = error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom);
                    self.begin_epilogue(Epilogue::abort(BatchExit::Error(error)))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            BatchSmtpStage::RcptReply(index) => match outcome {
                OpOutcome::Reply(response) => {
                    if response.is_completion() {
                        self.progress().record_rcpt_accepted(index);
                    } else if closing_channel(&response) {
                        return self.closing_during_envelope(index, response);
                    } else {
                        self.progress().record_rcpt_rejected(index, response);
                    }
                    let next = index + 1;
                    if next >= self.recipients.len() {
                        return self.after_envelope();
                    }
                    let bytes = self.recipients[next].to_string();
                    self.write_then(BatchSmtpStage::RcptReply(next), bytes)
                }
                OpOutcome::Failed(error) => {
                    // DATA has not been issued, so no content reached the peer:
                    // received RCPT answers stand and every still-open
                    // recipient is a retryable `Unsent` failure.
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::RcptTo),
                    );
                    self.progress()
                        .mark_unresolved_unsent(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            BatchSmtpStage::WindowWritten(start) => match outcome {
                OpOutcome::Failed(error) => {
                    if start == 0 {
                        let error = error
                            .with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::MailFrom);
                        return self.begin_epilogue(Epilogue::abort(BatchExit::Error(error)));
                    }
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::RcptTo),
                    );
                    self.progress()
                        .mark_unresolved_unsent(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                _ => {
                    self.stage = BatchSmtpStage::WindowOpened(start);
                    Step::Run(Op::OpenReplyGroup)
                }
            },
            BatchSmtpStage::WindowOpened(start) => match outcome {
                OpOutcome::Failed(error) => {
                    // A broken stream at the window boundary is reported
                    // without an abort: the caller sees the batch-level error
                    // and the stream is already unusable.
                    //
                    // The batch-level `Err` is deliberate and correct here.
                    // Every window is on the clean side of `DATA`, so nothing
                    // was transmitted and the whole request is retryable, which
                    // is exactly what `Err` promises. It is lossy, though, and
                    // knowingly so: both transports discard the `progress`, so
                    // RCPT answers already collected in earlier windows -
                    // including 550s the server gave - fold into one `Unsent`
                    // retry and the caller relearns them on the resend. Turning
                    // those into lanes would mean returning `Ok(progress)` for
                    // a request that never reached the peer, which is a worse
                    // lie than the lost diagnostics. Same at `WindowClosing`.
                    let error = error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::RcptTo);
                    let progress = self.take_progress();
                    Step::Finish(Err((error, progress)))
                }
                _ => {
                    let end = window_bounds(self.recipients.len(), start);
                    if start == 0 {
                        self.stage = BatchSmtpStage::MailWindowReply(end);
                    } else {
                        self.stage = BatchSmtpStage::RcptWindowReply { end, next: start };
                    }
                    Step::Run(Op::ReadGrouped)
                }
            },
            BatchSmtpStage::MailWindowReply(end) => match outcome {
                OpOutcome::Failed(error) => {
                    let error = error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom);
                    self.begin_epilogue(Epilogue::abort(BatchExit::Error(error)))
                }
                OpOutcome::Reply(response) if response.is_completion() => {
                    self.stage = BatchSmtpStage::RcptWindowReply { end, next: 0 };
                    Step::Run(Op::ReadGrouped)
                }
                OpOutcome::Reply(response) => {
                    let error = error::status(response)
                        .with_attempt(SmtpTransmissionState::Acknowledged)
                        .with_phase(SmtpCommandPhase::MailFrom);
                    self.begin_epilogue(Epilogue::abort(BatchExit::Error(error)))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            BatchSmtpStage::RcptWindowReply { end, next } => match outcome {
                OpOutcome::Reply(response) => {
                    if response.is_completion() {
                        self.progress().record_rcpt_accepted(next);
                    } else if closing_channel(&response) {
                        return self.closing_during_envelope(next, response);
                    } else {
                        self.progress().record_rcpt_rejected(next, response);
                    }
                    let next = next + 1;
                    if next < end {
                        self.stage = BatchSmtpStage::RcptWindowReply { end, next };
                        return Step::Run(Op::ReadGrouped);
                    }
                    self.stage = BatchSmtpStage::WindowClosing { end };
                    Step::Run(Op::CloseReplyGroup)
                }
                OpOutcome::Failed(error) => {
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::RcptTo),
                    );
                    self.progress()
                        .mark_unresolved_unsent(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            BatchSmtpStage::WindowClosing { end } => match outcome {
                OpOutcome::Failed(error) => {
                    // Batch-level `Err`, for the reason spelled out at
                    // `WindowOpened`: still before `DATA`, so nothing was
                    // transmitted, and the collected RCPT answers are dropped
                    // with the progress rather than claimed as lanes.
                    let error = error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::RcptTo);
                    let progress = self.take_progress();
                    Step::Finish(Err((error, progress)))
                }
                _ => self.start_window(end),
            },
            BatchSmtpStage::DataReply => match outcome {
                OpOutcome::Reply(response) if response.has_code(354) => {
                    self.progress().set_body_started();
                    self.stage = BatchSmtpStage::BodyWritten;
                    Step::Run(Op::WriteBody)
                }
                OpOutcome::Reply(response) => {
                    // DATA rejected before the body: every accepted recipient
                    // failed with this response, per-recipient rather than as a
                    // batch-level error the engine would resend.
                    let closing = closing_channel(&response);
                    self.progress()
                        .mark_accepted_rejected_with_response(response);
                    if closing {
                        return self.begin_epilogue(Epilogue::abort(BatchExit::Progress));
                    }
                    self.begin_epilogue(Epilogue::reset(BatchExit::Progress))
                }
                OpOutcome::Failed(error) => {
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::DataCommand),
                    );
                    // No body write was issued, so retry cannot duplicate delivery.
                    self.progress()
                        .mark_unresolved_unsent(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            BatchSmtpStage::BodyWritten => match outcome {
                OpOutcome::Failed(error) => {
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::InFlight)
                            .with_phase(SmtpCommandPhase::DataBody),
                    );
                    self.progress()
                        .mark_uncertain_unresolved(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                _ => {
                    self.progress().set_body_finished();
                    self.stage = BatchSmtpStage::FinalReply;
                    Step::Run(Op::ReadSingle)
                }
            },
            BatchSmtpStage::FinalReply => {
                let error = match outcome {
                    OpOutcome::Reply(response) => {
                        // Positive or negative, the peer answered the
                        // end-of-data terminator, so the transaction is
                        // complete (RFC 5321 4.1.1.4) and the lanes are
                        // resolved from the answer rather than left
                        // `uncertain`. The response is recorded either way and
                        // `resolve` fans it out - succeeded on a positive
                        // reply, `DataFinal` `failed` lanes on a rejection.
                        let closing = closing_channel(&response) || response.is_intermediate();
                        self.progress().set_data_response(response);
                        if !closing {
                            let progress = self.take_progress();
                            return Step::Finish(Ok(progress));
                        }
                        // The lanes still resolve from the answer, but a peer
                        // that is hanging up (`closing_channel`) must not be
                        // parked - with `test_on_checkout(false)` that shows
                        // up as an `Unsent` failure on the NEXT send. Same
                        // rule the direct machine applies at this boundary.
                        return self.begin_epilogue(Epilogue::abort(BatchExit::Progress));
                    }
                    OpOutcome::Failed(error) => error,
                    OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
                };
                let account_error = Self::account_error(
                    error
                        .with_attempt(SmtpTransmissionState::InFlight)
                        .with_phase(SmtpCommandPhase::DataBody),
                );
                self.progress()
                    .mark_uncertain_unresolved(|| account_error.clone());
                self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
            }
        }
    }
}

/// `send_lmtp_batch`.
pub(super) struct BatchLmtp {
    mail: Mail,
    recipients: Vec<Rcpt>,
    accepted: usize,
    progress: Option<SendProgress>,
    stage: BatchLmtpStage,
    epilogue: Option<Epilogue<BatchExit>>,
}

enum BatchLmtpStage {
    Start,
    /// A command write is outstanding. On success the reply for it is read and
    /// the inner stage handles it; a write failure is handed to that same
    /// stage, which owns the phase for this boundary either way.
    Writing(Box<BatchLmtpStage>),
    MailReply,
    RcptReply(usize),
    DataReply,
    BodyWritten,
    GroupOpened,
    FinalStatus(usize),
    DrainClosing,
}

impl BatchLmtp {
    pub(super) fn new(mail: Mail, recipients: Vec<Rcpt>, progress: SendProgress) -> Self {
        Self {
            mail,
            recipients,
            accepted: 0,
            progress: Some(progress),
            stage: BatchLmtpStage::Start,
            epilogue: None,
        }
    }

    fn progress(&mut self) -> &mut SendProgress {
        self.progress
            .as_mut()
            .expect("the progress tracker lives until the machine finishes")
    }

    fn take_progress(&mut self) -> SendProgress {
        self.progress
            .take()
            .expect("the progress tracker lives until the machine finishes")
    }

    fn account_error(error: Error) -> bifrost_types::error::AccountError {
        into_account_error(error, SmtpErrorContext::send(Protocol::Lmtp))
    }

    fn begin_epilogue(&mut self, epilogue: (Epilogue<BatchExit>, Op)) -> Step<BatchResult> {
        let (epilogue, op) = epilogue;
        self.epilogue = Some(epilogue);
        Step::Run(op)
    }

    /// Twin of `BatchSmtp::closing_during_envelope`; see `closing_channel`.
    fn closing_during_envelope(&mut self, index: usize, response: Response) -> Step<BatchResult> {
        let unsent = Self::account_error(
            error::status(response.clone())
                .with_attempt(SmtpTransmissionState::Unsent)
                .with_phase(SmtpCommandPhase::RcptTo),
        );
        self.progress().record_rcpt_rejected(index, response);
        self.progress().mark_unresolved_unsent(|| unsent.clone());
        self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
    }

    /// The next accepted recipient index at or after `from`, which is the
    /// order LMTP final statuses arrive in.
    fn next_accepted(&mut self, from: usize) -> Option<usize> {
        let progress = self.progress();
        (from..progress.recipients.len())
            .find(|index| matches!(progress.recipients[*index].rcpt, RcptProgress::Accepted))
    }

    fn after_envelope(&mut self) -> Step<BatchResult> {
        if self.accepted == 0 {
            return self.begin_epilogue(Epilogue::reset(BatchExit::Progress));
        }
        self.progress().set_data_issued();
        self.write_then(BatchLmtpStage::DataReply, Data.to_string())
    }
}

impl BatchLmtp {
    /// Write a command, then read the single reply it owes into `next`.
    fn write_then(&mut self, next: BatchLmtpStage, bytes: String) -> Step<BatchResult> {
        self.stage = BatchLmtpStage::Writing(Box::new(next));
        Step::Run(Op::Write(bytes))
    }
}

impl ProtocolMachine for BatchLmtp {
    type Output = BatchResult;

    fn step(&mut self, outcome: OpOutcome) -> Step<Self::Output> {
        if let Some(epilogue) = self.epilogue.take() {
            return match epilogue.advance(outcome) {
                Ok((epilogue, op)) => {
                    self.epilogue = Some(epilogue);
                    Step::Run(op)
                }
                Err(exit) => {
                    let progress = self.take_progress();
                    Step::Finish(match exit {
                        BatchExit::Progress => Ok(progress),
                        BatchExit::Error(error) => Err((error, progress)),
                    })
                }
            };
        }

        match std::mem::replace(&mut self.stage, BatchLmtpStage::Start) {
            BatchLmtpStage::Writing(next) => {
                self.stage = *next;
                match outcome {
                    OpOutcome::Failed(error) => self.step(OpOutcome::Failed(error)),
                    _ => Step::Run(Op::ReadSingle),
                }
            }
            BatchLmtpStage::Start => {
                let bytes = self.mail.to_string();
                self.write_then(BatchLmtpStage::MailReply, bytes)
            }
            BatchLmtpStage::MailReply => match outcome {
                OpOutcome::Reply(response) if response.is_completion() => {
                    if self.recipients.is_empty() {
                        return self.after_envelope();
                    }
                    let bytes = self.recipients[0].to_string();
                    self.write_then(BatchLmtpStage::RcptReply(0), bytes)
                }
                OpOutcome::Reply(response) => {
                    let error = error::status(response)
                        .with_attempt(SmtpTransmissionState::Acknowledged)
                        .with_phase(SmtpCommandPhase::MailFrom);
                    self.begin_epilogue(Epilogue::abort(BatchExit::Error(error)))
                }
                OpOutcome::Failed(error) => {
                    let error = error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom);
                    self.begin_epilogue(Epilogue::abort(BatchExit::Error(error)))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            BatchLmtpStage::RcptReply(index) => match outcome {
                OpOutcome::Reply(response) => {
                    if response.is_completion() {
                        self.progress().record_rcpt_accepted(index);
                        self.accepted += 1;
                    } else if closing_channel(&response) {
                        return self.closing_during_envelope(index, response);
                    } else {
                        self.progress().record_rcpt_rejected(index, response);
                    }
                    let next = index + 1;
                    if next >= self.recipients.len() {
                        return self.after_envelope();
                    }
                    let bytes = self.recipients[next].to_string();
                    self.write_then(BatchLmtpStage::RcptReply(next), bytes)
                }
                OpOutcome::Failed(error) => {
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::RcptTo),
                    );
                    self.progress()
                        .mark_unresolved_unsent(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            BatchLmtpStage::DataReply => match outcome {
                OpOutcome::Reply(response) if response.has_code(354) => {
                    self.progress().set_body_started();
                    self.stage = BatchLmtpStage::BodyWritten;
                    Step::Run(Op::WriteBody)
                }
                OpOutcome::Reply(response) => {
                    // Per-recipient `Failed`, never a batch-level `Err`: that
                    // would collapse the RCPT acceptances and let the engine
                    // resend a non-idempotent `Send` the server already
                    // rejected.
                    let closing = closing_channel(&response);
                    self.progress()
                        .mark_accepted_rejected_with_response(response);
                    if closing {
                        return self.begin_epilogue(Epilogue::abort(BatchExit::Progress));
                    }
                    self.begin_epilogue(Epilogue::reset(BatchExit::Progress))
                }
                OpOutcome::Failed(error) => {
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::Unsent)
                            .with_phase(SmtpCommandPhase::DataCommand),
                    );
                    self.progress()
                        .mark_unresolved_unsent(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            BatchLmtpStage::BodyWritten => match outcome {
                OpOutcome::Failed(error) => {
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::InFlight)
                            .with_phase(SmtpCommandPhase::DataBody),
                    );
                    self.progress()
                        .mark_uncertain_unresolved(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                _ => {
                    self.progress().set_body_finished();
                    self.stage = BatchLmtpStage::GroupOpened;
                    Step::Run(Op::OpenReplyGroup)
                }
            },
            BatchLmtpStage::GroupOpened => match outcome {
                OpOutcome::Failed(error) => {
                    // The body has already been written and terminated
                    // (`set_body_finished` ran one stage ago), so a batch-level
                    // `Err` is the wrong shape here whatever the cause: it
                    // means "nothing was transmitted", and the engine reading
                    // it would resend a non-idempotent message that may well
                    // have been delivered. The accepted recipients are
                    // `uncertain` instead, which is the same answer the final
                    // status arm below gives. No abort: this failure comes from
                    // the stream's own verify, so it is already unusable.
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::InFlight)
                            .with_phase(SmtpCommandPhase::LmtpFinalStatus),
                    );
                    self.progress()
                        .mark_uncertain_unresolved(|| account_error.clone());
                    let progress = self.take_progress();
                    Step::Finish(Ok(progress))
                }
                _ => match self.next_accepted(0) {
                    Some(index) => {
                        self.stage = BatchLmtpStage::FinalStatus(index);
                        Step::Run(Op::ReadGrouped)
                    }
                    None => {
                        self.stage = BatchLmtpStage::DrainClosing;
                        Step::Run(Op::CloseLmtpDrain { restore_ok: false })
                    }
                },
            },
            BatchLmtpStage::FinalStatus(index) => match outcome {
                OpOutcome::Reply(response) if response.is_intermediate() => {
                    let account_error = Self::account_error(
                        error::parse("intermediate LMTP final status")
                            .with_attempt(SmtpTransmissionState::InFlight)
                            .with_phase(SmtpCommandPhase::LmtpFinalStatus),
                    );
                    self.progress()
                        .mark_uncertain_unresolved(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                OpOutcome::Reply(response) => {
                    self.progress().record_lmtp_final(index, response);
                    match self.next_accepted(index + 1) {
                        Some(index) => {
                            self.stage = BatchLmtpStage::FinalStatus(index);
                            Step::Run(Op::ReadGrouped)
                        }
                        None => {
                            self.stage = BatchLmtpStage::DrainClosing;
                            Step::Run(Op::CloseLmtpDrain { restore_ok: false })
                        }
                    }
                }
                OpOutcome::Failed(error) => {
                    let account_error = Self::account_error(
                        error
                            .with_attempt(SmtpTransmissionState::InFlight)
                            .with_phase(SmtpCommandPhase::LmtpFinalStatus),
                    );
                    self.progress()
                        .mark_uncertain_unresolved(|| account_error.clone());
                    self.begin_epilogue(Epilogue::abort(BatchExit::Progress))
                }
                OpOutcome::Done => unreachable!("a read op yields a reply or a failure"),
            },
            // Every recipient outcome is already recorded, so a surplus final
            // status does not change the batch result - it only means this
            // stream must never be reused, which the drain itself enforces.
            BatchLmtpStage::DrainClosing => {
                let progress = self.take_progress();
                Step::Finish(Ok(progress))
            }
        }
    }
}

/// The BDAT header a `WriteBdat` op writes before the payload. Kept here so
/// both adapters frame the chunk identically.
pub(super) fn bdat_header(len: usize) -> String {
    Bdat::last(len).to_string()
}

#[cfg(test)]
mod core_tests;
