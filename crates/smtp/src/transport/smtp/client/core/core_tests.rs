//! Protocol rules pinned once, against the core, with no I/O.
//!
//! The transcript suites in both drivers still drive the full wire exchange
//! through their adapter; these tests pin the rules the adapters no longer
//! own, in the one place they now live. A machine is driven by a scripted
//! sequence of outcomes and its op stream is recorded, so "which command, how
//! many reply reads, RSET or abort, which phase" are all directly assertable.

use bifrost_types::error::BatchItemId;

use super::{super::PIPELINING_RECIPIENT_WINDOW, *};
use crate::{
    address::Address,
    transport::smtp::{
        batch::{RcptProgress, SmtpBatchRecipient},
        commands::{Mail, Rcpt},
        error::ErrorKind,
        response::{Category, Code, Detail, Response, Severity},
    },
};

fn address(local: &str) -> Address {
    format!("{local}@example.com")
        .parse()
        .expect("valid address")
}

fn mail() -> Mail {
    Mail::new(Some(address("sender")), Vec::new()).expect("valid MAIL FROM")
}

fn rcpts(count: usize) -> Vec<Rcpt> {
    (0..count)
        .map(|index| Rcpt::new(address(&format!("rcpt-{index}")), Vec::new()).expect("valid RCPT"))
        .collect()
}

fn reply(severity: Severity, message: &str) -> Response {
    Response::new(
        Code::new(severity, Category::MailSystem, Detail::Zero),
        vec![message.to_owned()],
    )
}

fn ok_reply() -> Response {
    reply(Severity::PositiveCompletion, "ok")
}

fn rejection() -> Response {
    reply(Severity::PermanentNegativeCompletion, "rejected")
}

fn intermediate() -> Response {
    "354 send body\r\n".parse().expect("valid DATA invitation")
}

/// `421`: the peer is closing the transmission channel.
fn closing() -> Response {
    "421 closing transmission channel\r\n"
        .parse()
        .expect("a valid 421 reply")
}

/// The direct path keeps only the FIRST refusal in a pipelined RCPT window, but
/// whether the channel is closing is asked of EVERY reply: a 550 followed by a
/// 421 reports the 550 and aborts with no RSET, as the batch machine does. A
/// 421 followed by a 550 (the 421 is the retained refusal) and a lone 550 (the
/// RSET-and-keep control) pin the neighbours. Ablation: with the `closing` flag
/// dropped so only the retained failure is asked, the 550-then-421 case writes
/// `RSET` and ends without a bare `ABORT`.
#[test]
fn a_421_later_in_an_rcpt_window_aborts_and_reports_the_first_refusal() {
    let run = |script: Vec<OpOutcome>| {
        let mut machine = DirectSmtp::new(mail(), rcpts(2), true, BodyKind::Data);
        let mut harness = Harness::new(script);
        let error = harness.run(&mut machine).expect_err("a recipient refused");
        (error, harness.ops)
    };

    let (error, ops) = run(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(closing()),
    ]);
    assert_eq!(error.phase(), Some(SmtpCommandPhase::RcptTo));
    assert_eq!(
        error.status().map(u16::from),
        Some(550),
        "the first refusal stays the reported one"
    );
    assert!(
        !ops.iter().any(|op| op.starts_with("W:RSET")),
        "no RSET into a closing channel: {ops:?}"
    );
    assert_eq!(ops.last().map(String::as_str), Some("ABORT"), "{ops:?}");

    let (error, ops) = run(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(closing()),
        OpOutcome::Reply(rejection()),
    ]);
    assert_eq!(error.status().map(u16::from), Some(421));
    assert!(!ops.iter().any(|op| op.starts_with("W:RSET")), "{ops:?}");
    assert_eq!(ops.last().map(String::as_str), Some("ABORT"), "{ops:?}");

    // Control: a plain refusal still resets and keeps the connection.
    let (error, ops) = run(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
    ]);
    assert_eq!(error.status().map(u16::from), Some(550));
    assert!(
        ops.iter().any(|op| op.starts_with("W:RSET")),
        "a non-421 refusal must still take RSET-and-keep: {ops:?}"
    );
}

/// A 421 answering an envelope command aborts the connection at every
/// boundary, in every machine, instead of parking it (a rejected `MAIL FROM`)
/// or resetting it (a rejected `RCPT TO` or `DATA`). The reset-or-keep rules
/// are about the transaction; a 421 is about the connection, and the
/// end-of-data arms already knew it while the envelope arms did not. Ablation:
/// with `closing_channel` answering `false` the direct MAIL FROM case ends
/// with no ABORT and the RCPT and DATA cases write `RSET` first.
#[test]
fn a_421_at_any_envelope_boundary_aborts_instead_of_parking() {
    // Direct SMTP, unpipelined and pipelined, at MAIL FROM, RCPT TO and DATA.
    for pipelined in [false, true] {
        for (script, phase) in [
            (
                vec![OpOutcome::Reply(closing())],
                SmtpCommandPhase::MailFrom,
            ),
            (
                vec![OpOutcome::Reply(ok_reply()), OpOutcome::Reply(closing())],
                SmtpCommandPhase::RcptTo,
            ),
            (
                vec![
                    OpOutcome::Reply(ok_reply()),
                    OpOutcome::Reply(ok_reply()),
                    OpOutcome::Reply(closing()),
                ],
                SmtpCommandPhase::DataCommand,
            ),
        ] {
            let mut machine = DirectSmtp::new(mail(), rcpts(1), pipelined, BodyKind::Data);
            let mut harness = Harness::new(script);
            let error = harness.run(&mut machine).expect_err("the peer is closing");
            assert_eq!(error.phase(), Some(phase), "pipelined={pipelined}");
            assert_eq!(error.status().map(u16::from), Some(421));
            assert!(
                !harness.ops.iter().any(|op| op.starts_with("W:RSET")),
                "pipelined={pipelined} {phase:?}: no RSET into a closing channel: {:?}",
                harness.ops
            );
            assert_eq!(
                harness.ops.last().map(String::as_str),
                Some("ABORT"),
                "pipelined={pipelined} {phase:?}: {:?}",
                harness.ops
            );
        }
    }

    // Direct LMTP, at the same three boundaries.
    for (script, phase) in [
        (
            vec![OpOutcome::Reply(closing())],
            SmtpCommandPhase::MailFrom,
        ),
        (
            vec![OpOutcome::Reply(ok_reply()), OpOutcome::Reply(closing())],
            SmtpCommandPhase::RcptTo,
        ),
        (
            vec![
                OpOutcome::Reply(ok_reply()),
                OpOutcome::Reply(ok_reply()),
                OpOutcome::Reply(closing()),
            ],
            SmtpCommandPhase::DataCommand,
        ),
    ] {
        let mut machine = DirectLmtp::new(mail(), rcpts(1), false, BodyKind::Data);
        let mut harness = Harness::new(script);
        let error = harness.run(&mut machine).expect_err("the peer is closing");
        assert_eq!(error.phase(), Some(phase));
        assert!(!harness.ops.iter().any(|op| op.starts_with("W:RSET")));
        assert_eq!(
            harness.ops.last().map(String::as_str),
            Some("ABORT"),
            "lmtp {phase:?}: {:?}",
            harness.ops
        );
    }

    // Batch SMTP and LMTP: a 421 to RCPT TO rejects that recipient with the
    // reply, leaves the rest Unsent (no DATA was issued), and aborts; a 421 to
    // DATA fails the accepted recipients and aborts without a reset.
    for protocol in [Dialect::Smtp, Dialect::Lmtp] {
        let run = |script: Vec<OpOutcome>| {
            let progress = SendProgress::new(protocol, batch_recipients(2));
            let mut harness = Harness::new(script);
            let mut machine =
                BatchTransaction::batch(mail(), rcpts(2), false, BodyKind::Data, progress);
            let progress = harness
                .run(&mut machine)
                .expect("a closing peer is not a batch-level error");
            (progress.resolve(), harness.ops)
        };

        let (outcome, ops) = run(vec![
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(closing()),
        ]);
        assert_eq!(
            outcome.failed().len(),
            2,
            "{protocol:?}: one rejected, one unsent"
        );
        assert!(
            outcome.uncertain().is_empty(),
            "{protocol:?}: nothing was sent"
        );
        assert!(
            !ops.iter().any(|op| op.starts_with("W:RSET")),
            "{protocol:?}: {ops:?}"
        );
        assert_eq!(
            ops.last().map(String::as_str),
            Some("ABORT"),
            "{protocol:?}: {ops:?}"
        );

        let (outcome, ops) = run(vec![
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(closing()),
        ]);
        assert_eq!(
            outcome.failed().len(),
            2,
            "{protocol:?}: DATA refused for both"
        );
        assert!(
            !ops.iter().any(|op| op.starts_with("W:RSET")),
            "{protocol:?}: {ops:?}"
        );
        assert_eq!(
            ops.last().map(String::as_str),
            Some("ABORT"),
            "{protocol:?}: {ops:?}"
        );
    }
}

/// The direct LMTP path follows the SMTP rules at the envelope boundaries: a
/// rejected `MAIL FROM` opened no transaction, so it is finished with no RSET
/// and no abort, and a rejected `DATA` is RSET-and-keep. Both used to abort,
/// which cost a reconnect per rejected envelope on a local-delivery socket
/// and made the direct path differ from both `DirectSmtp` and `BatchLmtp`.
/// Ablation: restoring either abort fails the matching case on its last op.
#[test]
fn a_rejected_lmtp_envelope_follows_the_smtp_reset_or_keep_rules() {
    let mut machine = DirectLmtp::new(mail(), rcpts(1), false, BodyKind::Data);
    let mut harness = Harness::new(vec![OpOutcome::Reply(rejection())]);
    let error = harness
        .run(&mut machine)
        .expect_err("MAIL FROM was rejected");
    assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
    assert!(
        !harness
            .ops
            .iter()
            .any(|op| op.starts_with("W:RSET") || op == "ABORT"),
        "a rejected MAIL FROM opened no transaction: {:?}",
        harness.ops
    );

    let mut machine = DirectLmtp::new(mail(), rcpts(1), false, BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
    ]);
    let error = harness.run(&mut machine).expect_err("DATA was rejected");
    assert_eq!(error.phase(), Some(SmtpCommandPhase::DataCommand));
    assert!(
        harness.ops.iter().any(|op| op == "W:RSET|"),
        "a rejected DATA clears the open transaction: {:?}",
        harness.ops
    );
    assert!(
        !harness.ops.iter().any(|op| op == "ABORT"),
        "an acknowledged RSET keeps the connection: {:?}",
        harness.ops
    );
}

/// Where a scripted failure lands.
///
/// A plain script cannot express "fail at the third grouped read" or "fail the
/// RSET write": writes and group boundaries consume the next scripted failure
/// whichever one it is, so a failure aimed at a later boundary is swallowed by
/// the first non-reply op that runs. An aim is matched against the op stream
/// instead, so any boundary - `OpenReplyGroup`, `CloseReplyGroup`,
/// `CloseLmtpDrain`, or the epilogue's own `RSET` write - can be targeted
/// without perturbing the replies that precede it.
#[derive(Clone, Copy, Debug)]
enum Aim {
    /// Fail the op at this 0-based position in the whole op stream.
    Position(usize),
    /// Fail the `nth` (0-based) op matching a selector: an exact op tag
    /// (`OPEN`, `CLOSE`, `DRAIN`, `READG`, `READ`, `ABORT`, `BODY`, `BDAT`),
    /// or a `W:` prefix matched against the rendered write, so
    /// `Kind("W:RSET", 0)` aims at the reset write specifically.
    Kind(&'static str, usize),
}

/// The string an [`Aim::Kind`] pattern is matched against: the rendering for a
/// write (so `W:RSET` picks out one command), the bare tag otherwise.
///
/// Non-write patterns match exactly, so `READ` never selects a `READG`.
fn selector(op: &Op) -> String {
    match op {
        Op::Write(_) | Op::WriteSecret(_) => render(op),
        Op::UpgradeTls(_) => "TLS".to_owned(),
        Op::ResolveToken(_) => "TOKEN".to_owned(),
        Op::WriteBody => "BODY".to_owned(),
        Op::WriteBdat => "BDAT".to_owned(),
        Op::OpenReplyGroup => "OPEN".to_owned(),
        Op::ReadGrouped => "READG".to_owned(),
        Op::ReadSingle => "READ".to_owned(),
        Op::CloseReplyGroup => "CLOSE".to_owned(),
        Op::CloseLmtpDrain { .. } => "DRAIN".to_owned(),
        Op::Abort => "ABORT".to_owned(),
    }
}

fn selected(selector: &str, pattern: &str) -> bool {
    if pattern.starts_with("W:") {
        selector.starts_with(pattern)
    } else {
        selector == pattern
    }
}

/// A recording driver: performs no I/O, answers each op from a script, and
/// keeps the op stream for assertions.
struct Harness {
    script: Vec<OpOutcome>,
    automatic_data_reply: bool,
    ops: Vec<String>,
    /// A failure aimed at one op rather than queued in the script.
    aim: Option<Aim>,
    /// Every op's selector, so `Aim::Kind` can name the nth match.
    seen: Vec<String>,
}

impl Harness {
    fn new(script: Vec<OpOutcome>) -> Self {
        Self {
            script,
            automatic_data_reply: false,
            ops: Vec::new(),
            aim: None,
            seen: Vec::new(),
        }
    }

    /// A script of positive replies long enough that reads never run out.
    fn positive(len: usize) -> Self {
        let mut harness = Self::new((0..len).map(|_| OpOutcome::Reply(ok_reply())).collect());
        harness.automatic_data_reply = true;
        harness
    }

    /// Fail exactly the op `aim` names, leaving the reply script untouched.
    fn aiming(mut self, aim: Aim) -> Self {
        self.aim = Some(aim);
        self
    }

    /// Whether the aim lands on this op, given where it sits in the stream and
    /// how many earlier ops shared its selector.
    fn aimed_at(&self, selector: &str, position: usize) -> bool {
        match self.aim {
            None => false,
            Some(Aim::Position(target)) => position == target,
            Some(Aim::Kind(pattern, nth)) => {
                if !selected(selector, pattern) {
                    return false;
                }
                let earlier = self
                    .seen
                    .iter()
                    .filter(|seen| selected(seen, pattern))
                    .count();
                earlier == nth
            }
        }
    }

    fn run<M: ProtocolMachine>(&mut self, machine: &mut M) -> M::Output {
        let mut outcome = OpOutcome::Done;
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 512, "machine did not terminate: {:?}", self.ops);
            match machine.step(outcome) {
                Step::Finish(output) => return output,
                Step::Run(op) => {
                    let position = self.ops.len();
                    let selector = selector(&op);
                    let aimed = self.aimed_at(&selector, position);
                    self.ops.push(render(&op));
                    self.seen.push(selector);
                    if aimed {
                        // The aimed failure replaces this op's outcome and
                        // leaves the reply script untouched.
                        self.aim = None;
                        outcome = network_failure();
                        continue;
                    }
                    outcome = match op {
                        // Writes and group boundaries answer `Done` unless the
                        // script says otherwise; reads always consume a step.
                        Op::ReadGrouped | Op::ReadSingle => {
                            let reply = self.next().unwrap_or_else(|| OpOutcome::Reply(ok_reply()));
                            if self.automatic_data_reply
                                && self
                                    .ops
                                    .get(self.ops.len().saturating_sub(2))
                                    .is_some_and(|op| op == "W:DATA|")
                            {
                                OpOutcome::Reply(intermediate())
                            } else {
                                reply
                            }
                        }
                        _ => self.next_non_reply(),
                    };
                }
            }
        }
    }

    fn next(&mut self) -> Option<OpOutcome> {
        if self.script.is_empty() {
            return None;
        }
        Some(self.script.remove(0))
    }

    /// Writes and group boundaries only consume a scripted outcome when it is
    /// a failure, so a script of replies stays aligned with the reads.
    fn next_non_reply(&mut self) -> OpOutcome {
        if matches!(self.script.first(), Some(OpOutcome::Failed(_))) {
            self.script.remove(0)
        } else {
            OpOutcome::Done
        }
    }
}

fn render(op: &Op) -> String {
    match op {
        Op::Write(bytes) => format!("W:{}", bytes.replace("\r\n", "|")),
        Op::WriteSecret(_) => "SECRET".to_owned(),
        Op::UpgradeTls(_) => "TLS".to_owned(),
        Op::ResolveToken(_) => "TOKEN".to_owned(),
        Op::WriteBody => "BODY".to_owned(),
        Op::WriteBdat => "BDAT".to_owned(),
        Op::OpenReplyGroup => "OPEN".to_owned(),
        Op::ReadGrouped => "READG".to_owned(),
        Op::ReadSingle => "READ".to_owned(),
        Op::CloseReplyGroup => "CLOSE".to_owned(),
        Op::CloseLmtpDrain { restore_ok } => format!("DRAIN:{restore_ok}"),
        Op::Abort => "ABORT".to_owned(),
    }
}

fn network_failure() -> OpOutcome {
    OpOutcome::Failed(crate::transport::smtp::error::network(
        std::io::Error::other("peer went away"),
    ))
}

fn batch_recipients(count: usize) -> Vec<SmtpBatchRecipient> {
    (0..count)
        .map(|index| SmtpBatchRecipient {
            id: BatchItemId(format!("item-{index}")),
            address: address(&format!("rcpt-{index}")),
        })
        .collect()
}

/// One window's worth of RCPT commands go out in a single write, and exactly
/// one reply per command in that window is drained before the group closes.
/// Getting the count wrong is how a driver reads the next window's replies as
/// this window's.
#[test]
fn a_pipelined_window_drains_exactly_one_reply_per_command() {
    let count = PIPELINING_RECIPIENT_WINDOW + 3;
    let mut machine = DirectSmtp::new(mail(), rcpts(count), true, BodyKind::Data);
    let mut harness = Harness::positive(count + 8);
    harness.run(&mut machine).expect("the send succeeds");

    let first_window = harness.ops.first().expect("a window is written first");
    assert_eq!(
        first_window.matches("RCPT TO").count(),
        PIPELINING_RECIPIENT_WINDOW,
        "the first window is bounded: {:?}",
        harness.ops
    );
    assert!(
        first_window.starts_with("W:MAIL FROM"),
        "MAIL FROM shares the first window's write: {first_window}"
    );

    // Window 0: MAIL FROM's reply plus one per recipient, then the close.
    let reads_before_first_close = harness
        .ops
        .iter()
        .take_while(|op| *op != "CLOSE")
        .filter(|op| *op == "READG")
        .count();
    assert_eq!(
        reads_before_first_close,
        PIPELINING_RECIPIENT_WINDOW + 1,
        "one reply per command in the window, plus MAIL FROM: {:?}",
        harness.ops
    );

    // Window 1 carries only the remaining recipients.
    let second_window = harness
        .ops
        .iter()
        .rfind(|op| op.starts_with("W:RCPT"))
        .expect("a second window is written");
    assert_eq!(second_window.matches("RCPT TO").count(), 3);
}

/// No send path may write `DATA` before `MAIL FROM`, whatever the recipient
/// count.
///
/// `Envelope::new` refuses an empty recipient list, so this is unreachable from
/// the public API - but the machine is the authority on its own command
/// sequence, and it did not enforce it: with no recipients the pipelined branch
/// called `start_window(0)`, found nothing to send, fell straight through to
/// `after_envelope`, and opened with a bare `DATA`. The sequential branch never
/// had the hole, because it writes `MAIL FROM` before it looks at the list.
#[test]
fn a_pipelined_send_with_no_recipients_still_opens_with_mail_from() {
    let mut machine = DirectSmtp::new(mail(), rcpts(0), true, BodyKind::Data);
    let mut harness = Harness::positive(8);
    let _ = harness.run(&mut machine);

    let first = harness.ops.first().expect("something is written");
    assert!(
        first.starts_with("W:MAIL FROM"),
        "the transaction must be opened before the body: {:?}",
        harness.ops
    );
}

/// The surplus check is deferred to the end of a window, not applied per
/// reply. A peer whose replies arrive coalesced is normal; only bytes left
/// after the whole group has drained prove it spoke out of turn.
#[test]
fn coalesced_window_replies_are_checked_once_at_the_group_boundary() {
    let mut machine = DirectSmtp::new(mail(), rcpts(2), true, BodyKind::Data);
    let mut harness = Harness::positive(8);
    harness.run(&mut machine).expect("the send succeeds");

    let group: Vec<&str> = harness
        .ops
        .iter()
        .map(String::as_str)
        .take_while(|op| *op != "CLOSE")
        .collect();
    assert_eq!(
        group,
        vec![
            "W:MAIL FROM:<sender@example.com>|RCPT TO:<rcpt-0@example.com>|RCPT TO:<rcpt-1@example.com>|",
            "OPEN",
            "READG",
            "READG",
            "READG"
        ],
        "every reply in a window is a grouped read, with no surplus check \
         between them: a coalesced pair is normal, not an unsolicited reply"
    );
}

/// A rejected `MAIL FROM` opened no transaction: the outstanding window
/// replies still have to drain, but no RSET is sent and the connection is
/// neither reset nor aborted.
#[test]
fn a_rejected_pipelined_mail_from_drains_the_window_without_reset_or_abort() {
    let mut machine = DirectSmtp::new(mail(), rcpts(2), true, BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
    ]);
    let error = harness
        .run(&mut machine)
        .expect_err("MAIL FROM was rejected");

    assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
    assert_eq!(
        harness.ops.iter().filter(|op| *op == "READG").count(),
        3,
        "both recipient replies are drained: {:?}",
        harness.ops
    );
    assert!(
        !harness.ops.iter().any(|op| op.starts_with("W:RSET")),
        "a rejected MAIL FROM opened no transaction: {:?}",
        harness.ops
    );
    assert!(
        !harness.ops.iter().any(|op| op == "ABORT"),
        "the connection stays reusable: {:?}",
        harness.ops
    );
    assert_eq!(harness.ops.last().map(String::as_str), Some("CLOSE"));
}

/// Finding 2: a routine negative reply after an accepted `MAIL FROM` is
/// cleared with RSET and the connection survives. Only a peer that fails to
/// acknowledge the reset costs the connection.
#[test]
fn a_rejected_recipient_resets_and_keeps_the_connection() {
    for pipelined in [false, true] {
        let mut machine = DirectSmtp::new(mail(), rcpts(1), pipelined, BodyKind::Data);
        let mut harness = Harness::new(vec![
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(rejection()),
            OpOutcome::Reply(ok_reply()),
        ]);
        let error = harness
            .run(&mut machine)
            .expect_err("the recipient was rejected");

        assert_eq!(error.phase(), Some(SmtpCommandPhase::RcptTo));
        assert!(
            harness.ops.iter().any(|op| op == "W:RSET|"),
            "pipelined={pipelined}: {:?}",
            harness.ops
        );
        assert!(
            !harness.ops.iter().any(|op| op == "ABORT"),
            "an acknowledged RSET keeps the connection: {:?}",
            harness.ops
        );
    }
}

#[test]
fn an_unacknowledged_reset_aborts_the_connection() {
    let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        // The RSET itself is refused.
        OpOutcome::Reply(rejection()),
    ]);
    harness
        .run(&mut machine)
        .expect_err("the recipient was rejected");

    assert_eq!(
        harness.ops.last().map(String::as_str),
        Some("ABORT"),
        "a refused RSET leaves the connection unusable: {:?}",
        harness.ops
    );
}

/// An I/O failure is not a protocol answer: every boundary that hits one
/// aborts the connection rather than resetting it, and carries the phase of
/// the boundary it failed at.
#[test]
fn an_io_failure_aborts_instead_of_resetting() {
    for (script, phase) in [
        (vec![network_failure()], SmtpCommandPhase::MailFrom),
        (
            vec![OpOutcome::Reply(ok_reply()), network_failure()],
            SmtpCommandPhase::RcptTo,
        ),
        (
            vec![
                OpOutcome::Reply(ok_reply()),
                OpOutcome::Reply(ok_reply()),
                network_failure(),
            ],
            SmtpCommandPhase::DataCommand,
        ),
    ] {
        let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Data);
        let mut harness = Harness::new(script);
        let error = harness.run(&mut machine).expect_err("the transport failed");

        assert_eq!(error.phase(), Some(phase));
        assert_eq!(harness.ops.last().map(String::as_str), Some("ABORT"));
        assert!(
            !harness.ops.iter().any(|op| op.starts_with("W:RSET")),
            "a broken stream cannot be reset: {:?}",
            harness.ops
        );
    }
}

/// BDAT skips the `DATA` command entirely, frames the chunk itself, and
/// carries its own phase on a body failure.
#[test]
fn bdat_sends_no_data_command_and_carries_its_own_phase() {
    let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Bdat);
    let mut harness = Harness::positive(4);
    harness.run(&mut machine).expect("the send succeeds");
    assert!(
        !harness.ops.iter().any(|op| op == "W:DATA|"),
        "BDAT replaces DATA: {:?}",
        harness.ops
    );
    assert!(harness.ops.iter().any(|op| op == "BDAT"));

    let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Bdat);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        network_failure(),
    ]);
    let error = harness
        .run(&mut machine)
        .expect_err("the chunk upload failed");
    assert_eq!(error.phase(), Some(SmtpCommandPhase::BdatBody));
}

/// LMTP answers once per ACCEPTED recipient after the body, in envelope
/// order, and the drain retires the connection either way.
#[test]
fn lmtp_reads_one_final_status_per_accepted_recipient() {
    let mut machine = DirectLmtp::new(mail(), rcpts(3), false, BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(intermediate()),
        OpOutcome::Reply(reply(Severity::PositiveCompletion, "delivered-0")),
        OpOutcome::Reply(reply(Severity::PositiveCompletion, "delivered-2")),
    ]);
    let statuses = harness.run(&mut machine).expect("the delivery completes");

    assert_eq!(statuses.len(), 3);
    assert_eq!(statuses[0].message().next().unwrap(), "delivered-0");
    assert!(
        !statuses[1].is_positive(),
        "the RCPT rejection is preserved"
    );
    assert_eq!(statuses[2].message().next().unwrap(), "delivered-2");

    let grouped = harness.ops.iter().filter(|op| *op == "READG").count();
    assert_eq!(
        grouped, 2,
        "one status per accepted recipient: {:?}",
        harness.ops
    );
    assert_eq!(
        harness.ops.last().map(String::as_str),
        Some("DRAIN:true"),
        "the drain closes the group: {:?}",
        harness.ops
    );
}

/// The LMTP batch path records a per-recipient final status against the
/// ORIGINAL recipient index, skipping the rejected ones, and never restores
/// the stream: an LMTP connection is retired rather than pooled.
#[test]
fn lmtp_batch_finals_land_on_the_accepted_recipient_indexes() {
    let progress = SendProgress::new(Dialect::Lmtp, batch_recipients(3));
    let mut machine = BatchTransaction::batch(mail(), rcpts(3), false, BodyKind::Data, progress);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(intermediate()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
    ]);
    let progress = harness.run(&mut machine).expect("the batch completes");
    let outcome = progress.resolve();

    assert_eq!(outcome.succeeded().len(), 1, "one recipient was delivered");
    assert_eq!(
        outcome.failed().len(),
        2,
        "the RCPT rejection and the failed final status"
    );
    assert_eq!(
        harness.ops.last().map(String::as_str),
        Some("DRAIN:false"),
        "an LMTP batch never restores the stream: {:?}",
        harness.ops
    );
}

/// A DATA-command rejection after RCPT acceptances is per-recipient, never a
/// batch-level error: a batch-level error would let the engine resend a
/// non-idempotent send the server already refused.
#[test]
fn a_rejected_data_command_fails_recipients_rather_than_the_batch() {
    for protocol in [Dialect::Smtp, Dialect::Lmtp] {
        let script = vec![
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(rejection()),
            OpOutcome::Reply(ok_reply()),
        ];
        let progress = SendProgress::new(protocol, batch_recipients(1));
        let mut harness = Harness::new(script);
        let mut machine =
            BatchTransaction::batch(mail(), rcpts(1), false, BodyKind::Data, progress);
        let progress = harness
            .run(&mut machine)
            .expect("a rejected DATA is not a batch-level failure");

        assert_eq!(progress.resolve().failed().len(), 1);
        assert!(
            harness.ops.iter().any(|op| op == "W:RSET|"),
            "{protocol:?}: the transaction is cleared: {:?}",
            harness.ops
        );
        assert!(!harness.ops.iter().any(|op| op == "ABORT"));
    }
}

/// Before DATA nothing can have been delivered, so a transport drop leaves
/// every unanswered recipient `Unsent` rather than uncertain - including
/// recipients the server already accepted.
#[test]
fn a_drop_before_data_marks_recipients_unsent() {
    let progress = SendProgress::new(Dialect::Smtp, batch_recipients(2));
    let mut machine = BatchTransaction::batch(mail(), rcpts(2), false, BodyKind::Data, progress);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        network_failure(),
    ]);
    let progress = harness.run(&mut machine).expect("recipient outcomes stand");
    let outcome = progress.resolve();

    assert!(outcome.succeeded().is_empty());
    assert_eq!(outcome.failed().len(), 2, "no lane may claim a delivery");
    assert_eq!(harness.ops.last().map(String::as_str), Some("ABORT"));
}

/// A rejected batch `MAIL FROM` is a batch-level error and costs the
/// connection: unlike the direct path, the batch driver aborts.
#[test]
fn a_rejected_batch_mail_from_aborts() {
    let progress = SendProgress::new(Dialect::Smtp, batch_recipients(1));
    let mut machine = BatchTransaction::batch(mail(), rcpts(1), false, BodyKind::Data, progress);
    let mut harness = Harness::new(vec![OpOutcome::Reply(rejection())]);
    let (error, _progress) = harness
        .run(&mut machine)
        .expect_err("a batch-level failure");

    assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
    assert!(matches!(error.kind(), ErrorKind::Permanent(_)));
    assert_eq!(harness.ops.last().map(String::as_str), Some("ABORT"));
}

/// The RSET-and-keep rule has a third case beyond "acknowledged" and
/// "refused": the reset write itself fails. A stream that cannot even carry
/// `RSET` is not a stream whose transaction was cleared, so the epilogue must
/// abort - and must not go on to read a reply the peer will never send.
#[test]
fn a_failed_rset_write_aborts_instead_of_keeping_the_connection() {
    let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
    ])
    .aiming(Aim::Kind("W:RSET", 0));
    let error = harness
        .run(&mut machine)
        .expect_err("the recipient was rejected");

    assert_eq!(error.phase(), Some(SmtpCommandPhase::RcptTo));
    let after_reset: Vec<&str> = harness
        .ops
        .iter()
        .skip_while(|op| !op.starts_with("W:RSET"))
        .skip(1)
        .map(String::as_str)
        .collect();
    assert_eq!(
        after_reset,
        vec!["ABORT"],
        "a failed RSET write aborts without reading for it: {:?}",
        harness.ops
    );
}

/// The reply-group bracket has two boundaries a plain script cannot reach.
/// Both hold the stream `Broken` already, so the batch path reports the
/// failure and returns rather than aborting a second time - and the recipient
/// answers it did collect stay in the tracker.
#[test]
fn a_failure_at_a_pipelined_group_boundary_returns_without_a_second_abort() {
    // OpenReplyGroup: nothing has been read yet, so no recipient is resolved.
    let progress = SendProgress::new(Dialect::Smtp, batch_recipients(2));
    let mut machine = BatchTransaction::batch(mail(), rcpts(2), true, BodyKind::Data, progress);
    let mut harness = Harness::positive(8).aiming(Aim::Kind("OPEN", 0));
    let (error, progress) = harness
        .run(&mut machine)
        .expect_err("the window boundary failed");

    assert_eq!(error.phase(), Some(SmtpCommandPhase::RcptTo));
    assert_eq!(error.attempt(), Some(SmtpTransmissionState::Unsent));
    assert!(
        progress
            .recipients
            .iter()
            .all(|recipient| matches!(recipient.rcpt, RcptProgress::Pending)),
        "no reply was read, so no recipient is resolved"
    );
    assert!(
        !harness.ops.iter().any(|op| op == "ABORT"),
        "the stream is already broken: {:?}",
        harness.ops
    );
    assert_eq!(harness.ops.last().map(String::as_str), Some("OPEN"));

    // CloseReplyGroup: every reply in the window was read, so the acceptances
    // survive the failure that closes it.
    let progress = SendProgress::new(Dialect::Smtp, batch_recipients(2));
    let mut machine = BatchTransaction::batch(mail(), rcpts(2), true, BodyKind::Data, progress);
    let mut harness = Harness::positive(8).aiming(Aim::Kind("CLOSE", 0));
    let (error, progress) = harness
        .run(&mut machine)
        .expect_err("the window close failed");

    assert_eq!(error.phase(), Some(SmtpCommandPhase::RcptTo));
    assert_eq!(error.attempt(), Some(SmtpTransmissionState::Unsent));
    assert!(
        progress
            .recipients
            .iter()
            .all(|recipient| matches!(recipient.rcpt, RcptProgress::Accepted)),
        "the window's replies were read before the close failed"
    );
    assert!(
        !harness.ops.iter().any(|op| op == "ABORT"),
        "the stream is already broken: {:?}",
        harness.ops
    );
    assert_eq!(harness.ops.last().map(String::as_str), Some("CLOSE"));
}

/// A surplus final status is a fact about the stream, not about the delivery:
/// every recipient's final reply has already been read and recorded, so the
/// batch result stands even though the drain fails.
#[test]
fn a_failure_closing_the_lmtp_drain_preserves_the_recipient_outcomes() {
    let progress = SendProgress::new(Dialect::Lmtp, batch_recipients(2));
    let mut machine = BatchTransaction::batch(mail(), rcpts(2), false, BodyKind::Data, progress);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(intermediate()),
        OpOutcome::Reply(reply(Severity::PositiveCompletion, "delivered-0")),
        OpOutcome::Reply(rejection()),
    ])
    .aiming(Aim::Kind("DRAIN", 0));
    let progress = harness
        .run(&mut machine)
        .expect("a failed drain close is not a batch-level failure");
    let outcome = progress.resolve();

    assert_eq!(outcome.succeeded().len(), 1);
    assert_eq!(outcome.failed().len(), 1, "the rejected final status");
    assert!(outcome.uncertain().is_empty(), "both finals were read");
    assert!(
        !harness.ops.iter().any(|op| op == "ABORT"),
        "a surplus status costs the stream, not the transaction: {:?}",
        harness.ops
    );

    // The direct path has no per-recipient tracker to preserve, so the same
    // failure is simply reported, with the drain's phase.
    let mut machine = DirectLmtp::new(mail(), rcpts(1), false, BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(intermediate()),
        OpOutcome::Reply(ok_reply()),
    ])
    .aiming(Aim::Kind("DRAIN", 0));
    let error = harness
        .run(&mut machine)
        .expect_err("the drain close failed");

    assert_eq!(error.phase(), Some(SmtpCommandPhase::LmtpFinalStatus));
    assert_eq!(harness.ops.last().map(String::as_str), Some("ABORT"));
}

/// The LMTP batch drain opens its reply group AFTER the body has been written
/// and terminated, so a failure there can never be a batch-level `Err`: that
/// shape means "nothing was transmitted", and the engine reading it would
/// resend a message the peer may already have delivered. The accepted
/// recipients are `uncertain`, the same answer a failed final-status read
/// gives. Reachable only through an aimed failure - in production
/// `open_reply_group` fails on `verify()` alone, and a completed body write
/// leaves the stream `Ok` - so this pins the SHAPE of an arm that is latent
/// today. Ablation: restoring `Step::Finish(Err((error, progress)))` fails the
/// `expect` below.
#[test]
fn a_failed_lmtp_group_open_leaves_the_recipients_uncertain_not_the_batch_unsent() {
    let progress = SendProgress::new(Dialect::Lmtp, batch_recipients(2));
    let mut machine = BatchTransaction::batch(mail(), rcpts(2), false, BodyKind::Data, progress);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(intermediate()),
    ])
    .aiming(Aim::Kind("OPEN", 0));
    let progress = harness
        .run(&mut machine)
        .expect("the body already left: this is not a whole-request failure");
    let outcome = progress.resolve();

    assert_eq!(
        outcome.uncertain().len(),
        2,
        "both accepted recipients are in doubt: {:?}",
        harness.ops
    );
    assert!(outcome.succeeded().is_empty());
    assert!(outcome.failed().is_empty());
    assert_eq!(harness.ops.last().map(String::as_str), Some("OPEN"));
}

/// A direct LMTP body upload carries the phase of its own framing, exactly as
/// `DirectSmtp` does. The phase was hardcoded to `DataBody`, so a failed BDAT
/// chunk on the LMTP path reported the wrong boundary. Ablation: hardcode
/// `SmtpCommandPhase::DataBody` again and this fails.
#[test]
fn a_failed_direct_lmtp_bdat_chunk_carries_the_bdat_phase() {
    for (body, phase) in [
        (BodyKind::Bdat, SmtpCommandPhase::BdatBody),
        (BodyKind::Data, SmtpCommandPhase::DataBody),
    ] {
        let mut machine = DirectLmtp::new(mail(), rcpts(1), false, body);
        let mut harness = Harness::positive(6).aiming(Aim::Kind(
            match body {
                BodyKind::Bdat => "BDAT",
                BodyKind::Data => "BODY",
            },
            0,
        ));
        let error = harness
            .run(&mut machine)
            .expect_err("the body upload failed");
        assert_eq!(error.phase(), Some(phase), "{body:?}: {:?}", harness.ops);
    }
}

/// A failure while draining the window a rejected pipelined `MAIL FROM` left
/// behind does not erase the rejection. The drain is bookkeeping for the
/// STREAM; the transaction's outcome was settled by the peer's answer, and
/// reporting the drain's transport failure instead handed the caller a
/// retryable network error for an envelope the server had permanently refused,
/// with the reply text gone. Both boundaries of the drain behave the same way.
/// Ablation: report `phased(SmtpCommandPhase::RcptTo, error)` at either arm and
/// that case fails on the phase.
#[test]
fn a_failed_window_drain_still_reports_the_mail_from_rejection() {
    // READG #1 is the first read of the leftover window; CLOSE #0 is the
    // group close after the whole window drained.
    for aim in [Aim::Kind("READG", 1), Aim::Kind("CLOSE", 0)] {
        let mut machine = DirectSmtp::new(mail(), rcpts(2), true, BodyKind::Data);
        let mut harness = Harness::new(vec![
            OpOutcome::Reply(rejection()),
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(ok_reply()),
        ])
        .aiming(aim);
        let error = harness
            .run(&mut machine)
            .expect_err("MAIL FROM was rejected");

        assert_eq!(
            error.phase(),
            Some(SmtpCommandPhase::MailFrom),
            "{aim:?}: {:?}",
            harness.ops
        );
        assert!(
            matches!(error.kind(), ErrorKind::Permanent(_)),
            "{aim:?}: the server's refusal, not a retryable transport error"
        );
        assert_eq!(
            harness.ops.last().map(String::as_str),
            Some("ABORT"),
            "{aim:?}: the stream is still lost: {:?}",
            harness.ops
        );
    }
}

/// The aim itself, pinned: a failure aimed by position lands on that op and
/// on no earlier one, which is what lets the tests above target a boundary a
/// scripted failure would have been consumed long before.
#[test]
fn an_aimed_failure_lands_on_the_op_it_names() {
    let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Data);
    let mut harness = Harness::positive(8).aiming(Aim::Position(4));
    let error = harness.run(&mut machine).expect_err("the aimed op failed");

    // 0: MAIL FROM write, 1: its reply, 2: RCPT write, 3: its reply,
    // 4: the DATA write.
    assert_eq!(harness.ops[4], "W:DATA|", "ops: {:?}", harness.ops);
    assert_eq!(error.phase(), Some(SmtpCommandPhase::DataCommand));
    assert_eq!(harness.ops.last().map(String::as_str), Some("ABORT"));
}

#[test]
fn data_requires_354_instead_of_any_positive_reply() {
    for response in [ok_reply(), "350 continue\r\n".parse().unwrap()] {
        let mut machine = DirectSmtp::new(mail(), rcpts(1), false, BodyKind::Data);
        let mut harness = Harness::new(vec![
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(ok_reply()),
            OpOutcome::Reply(response),
            OpOutcome::Reply(ok_reply()),
        ]);
        assert!(harness.run(&mut machine).is_err());
        assert!(!harness.ops.iter().any(|op| op == "BODY"));
    }
}

#[test]
fn intermediate_reset_reply_aborts_the_connection() {
    let epilogue = Epilogue::ResetReading(());
    let result = epilogue.advance(OpOutcome::Reply(intermediate()));
    assert!(matches!(result, Ok((Epilogue::Aborting(()), Op::Abort))));
}

// The old tracker-only regression now drives the real DATA rejection path.
#[test]
fn lmtp_data_command_negative_reply_marks_recipients_failed() {
    let progress = SendProgress::new(Dialect::Lmtp, batch_recipients(2));
    let mut machine = BatchTransaction::batch(mail(), rcpts(2), false, BodyKind::Data, progress);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(ok_reply()),
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(ok_reply()),
    ]);
    let outcome = harness
        .run(&mut machine)
        .expect("DATA rejection keeps recipient lanes")
        .resolve();
    assert!(outcome.succeeded().is_empty());
    assert!(outcome.uncertain().is_empty());
    assert_eq!(outcome.failed().len(), 2);
    for failed in outcome.failed() {
        assert!(!failed.error.recovery().requires_reconciliation());
        assert!(failed.error.chain().iter().any(|cause| matches!(cause,
            bifrost_types::Cause::Attempt(attempt) if attempt.transmission_state == bifrost_types::TransmissionState::Acknowledged
        )), "the DATA rejection must survive, not become a generic unsent fallback");
    }
}

#[test]
fn dialect_and_body_framing_share_envelope_and_recipient_accounting() {
    for dialect in [Dialect::Smtp, Dialect::Lmtp] {
        for body in [BodyKind::Data, BodyKind::Bdat] {
            for pipelined in [false, true] {
                let mut script = vec![
                    OpOutcome::Reply(ok_reply()),
                    OpOutcome::Reply(ok_reply()),
                    OpOutcome::Reply(rejection()),
                    OpOutcome::Reply(ok_reply()),
                ];
                if body == BodyKind::Data {
                    script.push(OpOutcome::Reply(intermediate()));
                }
                script.push(OpOutcome::Reply(ok_reply()));
                if dialect == Dialect::Lmtp {
                    script.push(OpOutcome::Reply(rejection()));
                }
                let progress = SendProgress::new(dialect, batch_recipients(3));
                let mut machine =
                    BatchTransaction::batch(mail(), rcpts(3), pipelined, body, progress);
                let mut harness = Harness::new(script);
                let result = harness.run(&mut machine).unwrap().resolve();
                assert_eq!(
                    result.succeeded().len(),
                    if dialect == Dialect::Smtp { 2 } else { 1 }
                );
                assert_eq!(result.failed()[0].item.0, "item-1");
                if dialect == Dialect::Lmtp {
                    assert_eq!(result.failed()[1].item.0, "item-2");
                }
                assert!(result.uncertain().is_empty());
                assert_eq!(
                    harness
                        .ops
                        .iter()
                        .filter(|op| op.as_str() == "W:DATA|")
                        .count(),
                    usize::from(body == BodyKind::Data)
                );
                if pipelined {
                    assert!(
                        harness.ops[0].contains("RCPT TO:<rcpt-2@example.com>"),
                        "{:?}",
                        harness.ops
                    );
                }
            }
        }
    }
}

/// A per-recipient 421 is that recipient's final answer (RFC 2033): the drain
/// keeps reading the remaining recipients' finals, and only a read that fails
/// after it leaves the unanswered recipients uncertain. The connection is
/// never restored to Ok once any final reply announced the channel closing.
#[test]
fn a_closing_lmtp_final_preserves_answers_and_keeps_draining() {
    for pipelined in [false, true] {
        for tail_fails in [false, true] {
            let script = || {
                vec![
                    OpOutcome::Reply(ok_reply()),
                    OpOutcome::Reply(ok_reply()),
                    OpOutcome::Reply(ok_reply()),
                    OpOutcome::Reply(ok_reply()),
                    OpOutcome::Reply(intermediate()),
                    OpOutcome::Reply(ok_reply()),
                    OpOutcome::Reply(closing()),
                    if tail_fails {
                        network_failure()
                    } else {
                        OpOutcome::Reply(ok_reply())
                    },
                ]
            };
            let progress = SendProgress::new(Dialect::Lmtp, batch_recipients(3));
            let mut machine =
                BatchTransaction::batch(mail(), rcpts(3), pipelined, BodyKind::Data, progress);
            let mut harness = Harness::new(script());
            let result = harness.run(&mut machine).unwrap().resolve();
            assert_eq!(result.succeeded()[0].item.0, "item-0");
            assert_eq!(result.failed()[0].item.0, "item-1");
            if tail_fails {
                assert_eq!(result.uncertain()[0].item.0, "item-2");
                assert_eq!(harness.ops.last().unwrap(), "ABORT");
            } else {
                assert_eq!(result.succeeded()[1].item.0, "item-2");
                assert!(result.uncertain().is_empty());
                assert_eq!(harness.ops.last().unwrap(), "DRAIN:false");
            }
            assert_eq!(
                harness
                    .ops
                    .iter()
                    .filter(|op| op.as_str() == "READG")
                    .count(),
                if pipelined { 7 } else { 3 }
            );
            let mut machine = DirectLmtp::new(mail(), rcpts(3), pipelined, BodyKind::Data);
            let mut harness = Harness::new(script());
            let output = harness.run(&mut machine);
            if tail_fails {
                let error = output.unwrap_err();
                assert_eq!(error.phase(), Some(SmtpCommandPhase::LmtpFinalStatus));
            } else {
                let statuses = output.expect("every final status was read");
                assert_eq!(statuses.len(), 3);
                assert!(statuses[1].has_code(421));
                assert_eq!(harness.ops.last().unwrap(), "DRAIN:false");
            }
        }
    }
}

#[test]
fn a_closing_reply_while_draining_rejected_mail_keeps_the_mail_refusal() {
    let mut machine = DirectSmtp::new(mail(), rcpts(3), true, BodyKind::Data);
    let mut harness = Harness::new(vec![
        OpOutcome::Reply(rejection()),
        OpOutcome::Reply(closing()),
    ]);
    let error = harness.run(&mut machine).unwrap_err();
    assert_eq!(error.status().map(u16::from), Some(550));
    assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
    assert_eq!(harness.ops.last().unwrap(), "ABORT");
    assert_eq!(
        harness
            .ops
            .iter()
            .filter(|op| op.as_str() == "READG")
            .count(),
        2
    );
}
