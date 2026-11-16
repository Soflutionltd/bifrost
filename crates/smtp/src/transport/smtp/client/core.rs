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
//! Session sequencing lives in the sibling `session` machine and reply framing
//! in `reply`. Adapters own dialing, byte movement, token resolution, TLS I/O,
//! deadlines, throttling and shutdown.

use super::PhasedError;
use crate::transport::smtp::{
    Dialect,
    commands::{Bdat, Data, Mail, Rcpt, Rset},
    error,
    error::{Error, SmtpCommandPhase, SmtpTransmissionState},
    response::Response,
};
#[cfg(feature = "account-error")]
use crate::transport::smtp::{
    account_error::{SmtpErrorContext, into_account_error},
    batch::SendProgress,
};

/// One unit of I/O the adapter performs on the machine's behalf.
///
/// The vocabulary is deliberately small, and every variant maps to a primitive
/// both drivers already had. Nothing in it names a timeout, a socket, or a
/// clock.
pub(super) enum Op {
    /// Write these command bytes and flush.
    Write(String),
    /// Credential-bearing command bytes, redacted and wiped on drop.
    WriteSecret(bifrost_sasl::Secret),
    /// Upgrade only after the STARTTLS reply and plaintext-buffer check.
    UpgradeTls(super::TlsParameters),
    /// Resolve through the adapter: `current()` asynchronously, `snapshot()` blocking.
    ResolveToken(crate::transport::smtp::authentication::Credentials),
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

impl std::fmt::Debug for Op {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Write(bytes) => formatter.debug_tuple("Write").field(bytes).finish(),
            Self::WriteSecret(_) => formatter.write_str("WriteSecret(<redacted>)"),
            Self::ResolveToken(_) => formatter.write_str("ResolveToken"),
            Self::UpgradeTls(_) => formatter.write_str("UpgradeTls"),
            Self::WriteBody => formatter.write_str("WriteBody"),
            Self::WriteBdat => formatter.write_str("WriteBdat"),
            Self::OpenReplyGroup => formatter.write_str("OpenReplyGroup"),
            Self::ReadGrouped => formatter.write_str("ReadGrouped"),
            Self::ReadSingle => formatter.write_str("ReadSingle"),
            Self::CloseReplyGroup => formatter.write_str("CloseReplyGroup"),
            Self::CloseLmtpDrain { restore_ok } => formatter
                .debug_struct("CloseLmtpDrain")
                .field("restore_ok", restore_ok)
                .finish(),
            Self::Abort => formatter.write_str("Abort"),
        }
    }
}

/// What came back from an [`Op`].
#[derive(Debug)]
pub(super) enum OpOutcome {
    /// The op completed and produced no reply. Also the kick-off value for the
    /// machine's first step.
    Done,
    /// A token resolved exactly once for this authentication exchange.
    Token(bifrost_sasl::Secret),
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
pub(super) enum Epilogue<T> {
    Aborting(T),
    ResetWriting(T),
    ResetReading(T),
}

impl<T> Epilogue<T> {
    /// Abort the connection, then finish with `value`.
    pub(super) fn abort(value: T) -> (Self, Op) {
        (Epilogue::Aborting(value), Op::Abort)
    }

    /// Clear the open transaction with RSET, keeping the connection if the
    /// peer acknowledges it, then finish with `value`.
    fn reset(value: T) -> (Self, Op) {
        (Epilogue::ResetWriting(value), Op::Write(Rset.to_string()))
    }

    pub(super) fn advance(self, outcome: OpOutcome) -> Result<(Self, Op), T> {
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
    fn final_phase(self) -> SmtpCommandPhase {
        match self {
            Self::Data => SmtpCommandPhase::DataFinal,
            Self::Bdat => SmtpCommandPhase::BdatBody,
        }
    }

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

/// Per-recipient wire progress exists even without the account-error feature.
/// Public results are projections of this record, never alternate sequencers.
#[derive(Clone)]
enum RecipientStatus {
    Pending,
    Accepted,
    Rejected(Response),
    Final(Response),
}

pub(super) struct TransactionProgress {
    recipients: Vec<RecipientStatus>,
    phase: TransactionPhase,
}

enum TransactionPhase {
    Envelope,
    DataIssued,
    BodyStarted,
    BodySent,
    FinalReply(Response),
}

impl TransactionProgress {
    fn new(count: usize) -> Self {
        Self {
            recipients: vec![RecipientStatus::Pending; count],
            phase: TransactionPhase::Envelope,
        }
    }

    fn body_started(&self) -> bool {
        matches!(
            self.phase,
            TransactionPhase::BodyStarted
                | TransactionPhase::BodySent
                | TransactionPhase::FinalReply(_)
        )
    }

    fn next_accepted(&self, start: usize) -> Option<usize> {
        (start..self.recipients.len())
            .find(|&index| matches!(self.recipients[index], RecipientStatus::Accepted))
    }
}

/// Reporting policy is independent of the wire dialect. Direct SMTP cannot
/// retain refusals alongside delivery in its single Response, so an enterprise
/// caller using that API expects it to stop before the body on any refusal.
/// LMTP and batch results can retain every recipient's answer.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum RecipientPolicy {
    FailFast,
    PerRecipient,
}

pub(super) trait Projection {
    type Output;
    const RECIPIENTS: RecipientPolicy;
    const BATCH: bool;
    fn group_failure(&mut self, progress: TransactionProgress, error: PhasedError) -> Self::Output {
        self.finish(progress, Err(error))
    }
    fn finish(
        &mut self,
        progress: TransactionProgress,
        result: Result<(), PhasedError>,
    ) -> Self::Output;
}

pub(super) struct SmtpResponse;
pub(super) struct LmtpResponses;

impl Projection for SmtpResponse {
    type Output = Result<Response, Error>;
    const RECIPIENTS: RecipientPolicy = RecipientPolicy::FailFast;
    const BATCH: bool = false;
    fn finish(
        &mut self,
        progress: TransactionProgress,
        result: Result<(), PhasedError>,
    ) -> Self::Output {
        result.map_err(PhasedError::into_error)?;
        match progress.phase {
            TransactionPhase::FinalReply(response) => Ok(response),
            _ => Err(error::internal("SMTP completed without a final reply")),
        }
    }
}

impl Projection for LmtpResponses {
    type Output = Result<Vec<Response>, Error>;
    const RECIPIENTS: RecipientPolicy = RecipientPolicy::PerRecipient;
    const BATCH: bool = false;
    fn finish(
        &mut self,
        progress: TransactionProgress,
        result: Result<(), PhasedError>,
    ) -> Self::Output {
        result.map_err(PhasedError::into_error)?;
        progress
            .recipients
            .into_iter()
            .map(|status| match status {
                RecipientStatus::Rejected(response) | RecipientStatus::Final(response) => {
                    Ok(response)
                }
                _ => Err(error::internal("LMTP completed without a recipient status")),
            })
            .collect()
    }
}

#[cfg(feature = "account-error")]
pub(super) struct BatchProgress(Option<SendProgress>);

#[cfg(feature = "account-error")]
impl Projection for BatchProgress {
    type Output = Result<SendProgress, (Error, SendProgress)>;
    const RECIPIENTS: RecipientPolicy = RecipientPolicy::PerRecipient;
    const BATCH: bool = true;
    fn group_failure(&mut self, wire: TransactionProgress, error: PhasedError) -> Self::Output {
        let progress = self.finish(wire, Ok(())).expect("no failure to project");
        Err((error.into_error(), progress))
    }
    fn finish(
        &mut self,
        wire: TransactionProgress,
        result: Result<(), PhasedError>,
    ) -> Self::Output {
        let mut progress = self.0.take().expect("project a transaction only once");
        let body_started = wire.body_started();
        for (index, status) in wire.recipients.into_iter().enumerate() {
            match status {
                RecipientStatus::Pending => {}
                RecipientStatus::Accepted => progress.record_rcpt_accepted(index),
                RecipientStatus::Rejected(response) => {
                    progress.record_rcpt_rejected(index, response);
                }
                RecipientStatus::Final(response) => progress.record_lmtp_final(index, response),
            }
        }
        match wire.phase {
            TransactionPhase::Envelope => {}
            TransactionPhase::DataIssued => progress.set_data_issued(),
            TransactionPhase::BodyStarted => progress.set_body_started(),
            TransactionPhase::BodySent => progress.set_body_finished(),
            TransactionPhase::FinalReply(response) => progress.set_data_response(response),
        }
        if let Err(failure) = result {
            let error = failure.into_error();
            // Whole-request failures before recipient attribution retain the
            // published batch-level shape. Reply-group boundary failures are
            // also whole-request failures: no body has been offered yet.
            if matches!(error.phase(), Some(SmtpCommandPhase::MailFrom)) {
                return Err((error, progress));
            }
            let error = error.with_attempt(if body_started {
                SmtpTransmissionState::InFlight
            } else {
                SmtpTransmissionState::Unsent
            });
            let account_error =
                into_account_error(error, SmtpErrorContext::send(progress.protocol));
            if body_started {
                progress.mark_uncertain_unresolved(|| account_error.clone());
            } else {
                progress.mark_unresolved_unsent(|| account_error.clone());
            }
        }
        Ok(progress)
    }
}

/// One sequencer for DATA/BDAT, SMTP/LMTP and both recipient policies.
/// Dialect determines final reply cardinality; projection determines reporting.
/// Neither gets its own command or reply-count state machine.
pub(super) struct Transaction<P> {
    mail: Mail,
    recipients: Vec<Rcpt>,
    dialect: Dialect,
    pipelined: bool,
    body: BodyKind,
    progress: Option<TransactionProgress>,
    projection: P,
    stage: TransactionStage,
    epilogue: Option<Epilogue<Result<(), PhasedError>>>,
}

enum TransactionStage {
    Start,
    Writing(Box<TransactionStage>),
    MailReply,
    RcptReply(usize),
    WindowWritten(usize),
    WindowOpened(usize),
    MailWindowReply(usize),
    MailRejectedDrain {
        remaining: usize,
        response: Response,
    },
    MailRejectedClosing(Response),
    RcptWindowReply {
        end: usize,
        next: usize,
        failure: Option<Response>,
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
    GroupOpened,
    FinalStatus(usize),
    DrainClosing,
}

pub(super) type DirectSmtp = Transaction<SmtpResponse>;
pub(super) type DirectLmtp = Transaction<LmtpResponses>;
#[cfg(feature = "account-error")]
pub(super) type BatchTransaction = Transaction<BatchProgress>;

impl DirectSmtp {
    pub(super) fn new(mail: Mail, recipients: Vec<Rcpt>, pipelined: bool, body: BodyKind) -> Self {
        Self::configured(
            mail,
            recipients,
            Dialect::Smtp,
            pipelined,
            body,
            SmtpResponse,
        )
    }
}
impl DirectLmtp {
    pub(super) fn new(mail: Mail, recipients: Vec<Rcpt>, pipelined: bool, body: BodyKind) -> Self {
        Self::configured(
            mail,
            recipients,
            Dialect::Lmtp,
            pipelined,
            body,
            LmtpResponses,
        )
    }
}
#[cfg(feature = "account-error")]
impl Transaction<BatchProgress> {
    pub(super) fn batch(
        mail: Mail,
        recipients: Vec<Rcpt>,
        pipelined: bool,
        body: BodyKind,
        progress: SendProgress,
    ) -> Self {
        Self::configured(
            mail,
            recipients,
            progress.protocol,
            pipelined,
            body,
            BatchProgress(Some(progress)),
        )
    }
}

impl<P: Projection> Transaction<P> {
    fn configured(
        mail: Mail,
        recipients: Vec<Rcpt>,
        dialect: Dialect,
        pipelined: bool,
        body: BodyKind,
        projection: P,
    ) -> Self {
        Self {
            progress: Some(TransactionProgress::new(recipients.len())),
            mail,
            recipients,
            dialect,
            pipelined,
            body,
            projection,
            stage: TransactionStage::Start,
            epilogue: None,
        }
    }
    fn progress(&mut self) -> &mut TransactionProgress {
        self.progress
            .as_mut()
            .expect("progress lives until projection")
    }
    fn finish(&mut self, result: Result<(), PhasedError>) -> Step<P::Output> {
        Step::Finish(
            self.projection
                .finish(self.progress.take().expect("finish only once"), result),
        )
    }
    fn begin_epilogue(
        &mut self,
        epilogue: (Epilogue<Result<(), PhasedError>>, Op),
    ) -> Step<P::Output> {
        let (epilogue, op) = epilogue;
        self.epilogue = Some(epilogue);
        Step::Run(op)
    }
    fn failure(&mut self, phase: SmtpCommandPhase, error: Error) -> Result<(), PhasedError> {
        let attempt = if self.progress().body_started() {
            SmtpTransmissionState::InFlight
        } else {
            SmtpTransmissionState::Unsent
        };
        Err(PhasedError::new(phase, error.with_attempt(attempt)))
    }
    fn abort_with(&mut self, phase: SmtpCommandPhase, error: Error) -> Step<P::Output> {
        let result = self.failure(phase, error);
        self.begin_epilogue(Epilogue::abort(result))
    }
    fn write_then(&mut self, next: TransactionStage, bytes: String) -> Step<P::Output> {
        self.stage = TransactionStage::Writing(Box::new(next));
        Step::Run(Op::Write(bytes))
    }
    fn after_envelope(&mut self) -> Step<P::Output> {
        if self.progress().next_accepted(0).is_none()
            && (P::RECIPIENTS == RecipientPolicy::PerRecipient)
        {
            return self.begin_epilogue(Epilogue::reset(Ok(())));
        }
        match self.body {
            BodyKind::Data => {
                self.progress().phase = TransactionPhase::DataIssued;
                self.write_then(TransactionStage::DataReply, Data.to_string())
            }
            BodyKind::Bdat => self.start_body(),
        }
    }
    fn start_body(&mut self) -> Step<P::Output> {
        self.progress().phase = TransactionPhase::BodyStarted;
        self.stage = TransactionStage::BodyWritten;
        Step::Run(self.body.write_op())
    }
    fn start_window(&mut self, start: usize) -> Step<P::Output> {
        if start >= self.recipients.len() {
            return self.after_envelope();
        }
        let end = window_bounds(self.recipients.len(), start);
        let bytes = window_commands(&self.mail, &self.recipients, start, end);
        self.stage = TransactionStage::WindowWritten(start);
        Step::Run(Op::Write(bytes))
    }
    fn record_rcpt(&mut self, index: usize, response: &Response) {
        self.progress().recipients[index] = if response.is_completion() {
            RecipientStatus::Accepted
        } else {
            RecipientStatus::Rejected(response.clone())
        };
    }
    fn rejection(
        &mut self,
        phase: SmtpCommandPhase,
        response: Response,
        reset: bool,
    ) -> Step<P::Output> {
        let closing = closing_channel(&response);
        let result = if P::BATCH && phase == SmtpCommandPhase::DataCommand {
            for status in &mut self.progress().recipients {
                if matches!(status, RecipientStatus::Accepted) {
                    *status = RecipientStatus::Rejected(response.clone());
                }
            }
            Ok(())
        } else {
            Err(PhasedError::new(
                phase,
                error::status(response).with_attempt(SmtpTransmissionState::Acknowledged),
            ))
        };
        // Preserve the conservative batch MAIL refusal policy. This is a
        // reporting policy, not a second interpretation of the SMTP sequence.
        if closing || (P::BATCH && phase == SmtpCommandPhase::MailFrom) {
            self.begin_epilogue(Epilogue::abort(result))
        } else if reset {
            self.begin_epilogue(Epilogue::reset(result))
        } else {
            self.finish(result)
        }
    }
    fn group_failure(&mut self, error: Error, opening: bool, start: usize) -> Step<P::Output> {
        let phase = if !P::BATCH && opening && start == 0 {
            SmtpCommandPhase::MailFrom
        } else {
            SmtpCommandPhase::RcptTo
        };
        if P::BATCH {
            // Already broken, before DATA. Preserve the whole-request error
            // and the collected RCPT answers without issuing a second abort.
            let progress = self.progress.take().expect("finish only once");
            let error = error
                .with_phase(phase)
                .with_attempt(SmtpTransmissionState::Unsent);
            // Use the projection's batch-level entry point rather than pretending
            // this boundary error belongs to a recipient.
            self.finish_group_failure(progress, error)
        } else {
            self.abort_with(phase, error)
        }
    }
    fn finish_group_failure(
        &mut self,
        progress: TransactionProgress,
        error: Error,
    ) -> Step<P::Output> {
        Step::Finish(
            self.projection
                .group_failure(progress, PhasedError::new(SmtpCommandPhase::RcptTo, error)),
        )
    }
}

// The adapter interface admits outcomes from every effect. Reject a mismatched
// read as an internal error and abort, retaining send evidence for the caller;
// unwinding here would lose the receipt an enterprise send queue needs.
impl<P: Projection> ProtocolMachine for Transaction<P> {
    type Output = P::Output;
    fn step(&mut self, outcome: OpOutcome) -> Step<Self::Output> {
        if let Some(epilogue) = self.epilogue.take() {
            return match epilogue.advance(outcome) {
                Ok((epilogue, op)) => {
                    self.epilogue = Some(epilogue);
                    Step::Run(op)
                }
                Err(result) => self.finish(result),
            };
        }
        match std::mem::replace(&mut self.stage, TransactionStage::Start) {
            TransactionStage::Start => {
                // Even an internal empty envelope must issue MAIL before DATA.
                if self.pipelined && !self.recipients.is_empty() {
                    return self.start_window(0);
                }
                self.write_then(TransactionStage::MailReply, self.mail.to_string())
            }
            TransactionStage::Writing(next) => {
                self.stage = *next;
                match outcome {
                    OpOutcome::Failed(error) => self.step(OpOutcome::Failed(error)),
                    _ => Step::Run(Op::ReadSingle),
                }
            }
            TransactionStage::MailReply => match outcome {
                OpOutcome::Failed(error) => self.abort_with(SmtpCommandPhase::MailFrom, error),
                OpOutcome::Reply(response) if response.is_completion() => {
                    if self.recipients.is_empty() {
                        return self.after_envelope();
                    }
                    self.write_then(
                        TransactionStage::RcptReply(0),
                        self.recipients[0].to_string(),
                    )
                }
                OpOutcome::Reply(response) => {
                    self.rejection(SmtpCommandPhase::MailFrom, response, false)
                }
                _ => self.abort_with(
                    SmtpCommandPhase::MailFrom,
                    error::internal("read effect did not return a reply"),
                ),
            },
            TransactionStage::RcptReply(index) => match outcome {
                OpOutcome::Failed(error) => self.abort_with(SmtpCommandPhase::RcptTo, error),
                OpOutcome::Reply(response) => {
                    self.record_rcpt(index, &response);
                    if closing_channel(&response)
                        || (!response.is_completion() && P::RECIPIENTS == RecipientPolicy::FailFast)
                    {
                        return self.rejection(SmtpCommandPhase::RcptTo, response, true);
                    }
                    let next = index + 1;
                    if next == self.recipients.len() {
                        return self.after_envelope();
                    }
                    self.write_then(
                        TransactionStage::RcptReply(next),
                        self.recipients[next].to_string(),
                    )
                }
                _ => self.abort_with(
                    SmtpCommandPhase::RcptTo,
                    error::internal("read effect did not return a reply"),
                ),
            },
            TransactionStage::WindowWritten(start) => match outcome {
                OpOutcome::Failed(error) => self.abort_with(
                    if start == 0 {
                        SmtpCommandPhase::MailFrom
                    } else {
                        SmtpCommandPhase::RcptTo
                    },
                    error,
                ),
                _ => {
                    self.stage = TransactionStage::WindowOpened(start);
                    Step::Run(Op::OpenReplyGroup)
                }
            },
            TransactionStage::WindowOpened(start) => match outcome {
                OpOutcome::Failed(error) => self.group_failure(error, true, start),
                _ => {
                    let end = window_bounds(self.recipients.len(), start);
                    self.stage = if start == 0 {
                        TransactionStage::MailWindowReply(end)
                    } else {
                        TransactionStage::RcptWindowReply {
                            end,
                            next: start,
                            failure: None,
                            closing: false,
                        }
                    };
                    Step::Run(Op::ReadGrouped)
                }
            },
            TransactionStage::MailWindowReply(end) => match outcome {
                OpOutcome::Failed(error) => self.abort_with(SmtpCommandPhase::MailFrom, error),
                OpOutcome::Reply(response) if response.is_completion() => {
                    self.stage = TransactionStage::RcptWindowReply {
                        end,
                        next: 0,
                        failure: None,
                        closing: false,
                    };
                    Step::Run(Op::ReadGrouped)
                }
                OpOutcome::Reply(response) => {
                    if closing_channel(&response) || P::BATCH {
                        return self.rejection(SmtpCommandPhase::MailFrom, response, false);
                    }
                    self.stage = TransactionStage::MailRejectedDrain {
                        remaining: end,
                        response,
                    };
                    self.step(OpOutcome::Done)
                }
                _ => self.abort_with(
                    SmtpCommandPhase::MailFrom,
                    error::internal("read effect did not return a reply"),
                ),
            },
            TransactionStage::MailRejectedDrain {
                remaining,
                response,
            } => match outcome {
                OpOutcome::Reply(reply) if closing_channel(&reply) => {
                    // The MAIL refusal remains authoritative, but a later 421
                    // closes the stream. Do not wait for replies it will never
                    // send or park it merely because the group drained.
                    let result = Err(PhasedError::new(
                        SmtpCommandPhase::MailFrom,
                        error::status(response),
                    ));
                    self.begin_epilogue(Epilogue::abort(result))
                }
                OpOutcome::Failed(_) => {
                    // A failed drain costs the stream, never the already-known
                    // MAIL rejection. Preserve that definitive refusal.
                    let result = Err(PhasedError::new(
                        SmtpCommandPhase::MailFrom,
                        error::status(response),
                    ));
                    self.begin_epilogue(Epilogue::abort(result))
                }
                _ if remaining == 0 => {
                    self.stage = TransactionStage::MailRejectedClosing(response);
                    Step::Run(Op::CloseReplyGroup)
                }
                _ => {
                    self.stage = TransactionStage::MailRejectedDrain {
                        remaining: remaining - 1,
                        response,
                    };
                    Step::Run(Op::ReadGrouped)
                }
            },
            TransactionStage::MailRejectedClosing(response) => {
                if matches!(outcome, OpOutcome::Failed(_)) {
                    let result = Err(PhasedError::new(
                        SmtpCommandPhase::MailFrom,
                        error::status(response),
                    ));
                    self.begin_epilogue(Epilogue::abort(result))
                } else {
                    self.rejection(SmtpCommandPhase::MailFrom, response, false)
                }
            }
            TransactionStage::RcptWindowReply {
                end,
                next,
                mut failure,
                closing,
            } => match outcome {
                OpOutcome::Failed(error) => self.abort_with(SmtpCommandPhase::RcptTo, error),
                OpOutcome::Reply(response) => {
                    self.record_rcpt(next, &response);
                    if P::RECIPIENTS == RecipientPolicy::PerRecipient && closing_channel(&response)
                    {
                        return self.rejection(SmtpCommandPhase::RcptTo, response, true);
                    }
                    let closing = closing || closing_channel(&response);
                    if !response.is_completion() && failure.is_none() {
                        failure = Some(response);
                    }
                    let next = next + 1;
                    if next < end {
                        self.stage = TransactionStage::RcptWindowReply {
                            end,
                            next,
                            failure,
                            closing,
                        };
                        Step::Run(Op::ReadGrouped)
                    } else {
                        self.stage = TransactionStage::WindowClosing {
                            end,
                            failure,
                            closing,
                        };
                        Step::Run(Op::CloseReplyGroup)
                    }
                }
                _ => self.abort_with(
                    SmtpCommandPhase::RcptTo,
                    error::internal("read effect did not return a reply"),
                ),
            },
            TransactionStage::WindowClosing {
                end,
                failure,
                closing,
            } => match outcome {
                OpOutcome::Failed(error) => self.group_failure(error, false, end),
                _ => {
                    if let Some(response) = failure
                        && P::RECIPIENTS == RecipientPolicy::FailFast
                    {
                        let result = Err(PhasedError::new(
                            SmtpCommandPhase::RcptTo,
                            error::status(response),
                        ));
                        return self.begin_epilogue(if closing {
                            Epilogue::abort(result)
                        } else {
                            Epilogue::reset(result)
                        });
                    }
                    self.start_window(end)
                }
            },
            TransactionStage::DataReply => match outcome {
                OpOutcome::Failed(error) => self.abort_with(SmtpCommandPhase::DataCommand, error),
                OpOutcome::Reply(response) if response.has_code(354) => self.start_body(),
                OpOutcome::Reply(response) => {
                    self.rejection(SmtpCommandPhase::DataCommand, response, true)
                }
                _ => self.abort_with(
                    SmtpCommandPhase::DataCommand,
                    error::internal("read effect did not return a reply"),
                ),
            },
            TransactionStage::BodyWritten => match outcome {
                OpOutcome::Failed(error) => self.abort_with(self.body.body_phase(), error),
                _ => {
                    self.progress().phase = TransactionPhase::BodySent;
                    self.stage = if self.dialect == Dialect::Smtp {
                        TransactionStage::FinalReply
                    } else {
                        TransactionStage::GroupOpened
                    };
                    Step::Run(if self.dialect == Dialect::Smtp {
                        Op::ReadSingle
                    } else {
                        Op::OpenReplyGroup
                    })
                }
            },
            TransactionStage::FinalReply => match outcome {
                OpOutcome::Failed(error) => self.abort_with(self.body.body_phase(), error),
                OpOutcome::Reply(response) => {
                    let intermediate = response.is_intermediate();
                    let closing = closing_channel(&response);
                    let rejected = !response.is_completion();
                    self.progress().phase = TransactionPhase::FinalReply(response.clone());
                    if intermediate && !P::BATCH {
                        return self.abort_with(
                            self.body.final_phase(),
                            error::parse("intermediate reply after message body"),
                        );
                    }
                    if P::BATCH {
                        if closing || intermediate {
                            return self.begin_epilogue(Epilogue::abort(Ok(())));
                        }
                        if rejected && self.body == BodyKind::Bdat {
                            return self.begin_epilogue(Epilogue::reset(Ok(())));
                        }
                        self.finish(Ok(()))
                    } else if rejected {
                        self.rejection(
                            self.body.final_phase(),
                            response,
                            self.body == BodyKind::Bdat,
                        )
                    } else {
                        self.finish(Ok(()))
                    }
                }
                _ => self.abort_with(
                    self.body.final_phase(),
                    error::internal("read effect did not return a reply"),
                ),
            },
            TransactionStage::GroupOpened => match outcome {
                OpOutcome::Failed(error) if P::BATCH => {
                    let result = self.failure(SmtpCommandPhase::LmtpFinalStatus, error);
                    self.finish(result)
                }
                OpOutcome::Failed(error) => {
                    self.abort_with(SmtpCommandPhase::LmtpFinalStatus, error)
                }
                _ => {
                    if let Some(index) = self.progress().next_accepted(0) {
                        self.stage = TransactionStage::FinalStatus(index);
                        Step::Run(Op::ReadGrouped)
                    } else {
                        self.stage = TransactionStage::DrainClosing;
                        Step::Run(Op::CloseLmtpDrain {
                            restore_ok: !P::BATCH,
                        })
                    }
                }
            },
            TransactionStage::FinalStatus(index) => match outcome {
                OpOutcome::Failed(error) => {
                    self.abort_with(SmtpCommandPhase::LmtpFinalStatus, error)
                }
                OpOutcome::Reply(response) if response.is_intermediate() => self.abort_with(
                    SmtpCommandPhase::LmtpFinalStatus,
                    error::parse("intermediate LMTP final status"),
                ),
                OpOutcome::Reply(response) => {
                    let closing = closing_channel(&response);
                    self.progress().recipients[index] = RecipientStatus::Final(response.clone());
                    if closing {
                        // A final 421 is definitive for this recipient only.
                        // Preserve preceding answers; remaining recipients are
                        // ambiguous because the body has already been offered.
                        return self.abort_with(
                            SmtpCommandPhase::LmtpFinalStatus,
                            error::status(response),
                        );
                    }
                    if let Some(next) = self.progress().next_accepted(index + 1) {
                        self.stage = TransactionStage::FinalStatus(next);
                        Step::Run(Op::ReadGrouped)
                    } else {
                        self.stage = TransactionStage::DrainClosing;
                        Step::Run(Op::CloseLmtpDrain {
                            restore_ok: !P::BATCH,
                        })
                    }
                }
                _ => self.abort_with(
                    SmtpCommandPhase::LmtpFinalStatus,
                    error::internal("read effect did not return a reply"),
                ),
            },
            TransactionStage::DrainClosing => match outcome {
                OpOutcome::Failed(error) if !P::BATCH => {
                    self.abort_with(SmtpCommandPhase::LmtpFinalStatus, error)
                }
                // Outcomes already recorded stand, even if surplus bytes cost
                // the stream. The adapter has retired it in either case.
                _ => self.finish(Ok(())),
            },
        }
    }
}

/// The BDAT header both adapters put before the payload.
pub(super) fn bdat_header(len: usize) -> String {
    Bdat::last(len).to_string()
}

#[cfg(all(test, feature = "account-error"))]
mod core_tests;
