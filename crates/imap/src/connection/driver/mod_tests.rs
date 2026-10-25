#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::validated::{MailboxName, SequenceSet};

/// Dummy consumer for testing sub-batch grouping. Implements
/// `Consumer` with a unit `Output`  -  we only care about grouping
/// logic, not response routing.
struct DummyConsumer;

impl super::super::dispatch::Consumer for DummyConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        _resp: crate::types::response::UntaggedResponse,
        _notify: super::super::NotifyFlags,
        _ctx: &super::super::dispatch::ConsumerContext,
    ) {
    }

    fn finalize(
        self: Box<Self>,
        _tagged: crate::types::response::TaggedResponse,
        _ctx: &super::super::dispatch::ConsumerContext,
    ) -> super::super::dispatch::Finalized<Self::Output> {
        super::super::dispatch::Finalized::success((), Vec::new())
    }
}

fn dummy_consumer() -> Box<dyn ConsumerErased> {
    Box::new(DummyConsumer) as Box<dyn ConsumerErased>
}

// ---------------------------------------------------------------------------
// group_into_sub_batches
// ---------------------------------------------------------------------------

#[test]
fn group_unique_kinds_single_batch() {
    // All unique kinds -> single sub-batch.
    let commands = vec![Command::Noop, Command::Capability, Command::Check];
    let consumers = vec![dummy_consumer(), dummy_consumer(), dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 3);
    // Original indices preserved.
    assert_eq!(batches[0][0].0, 0);
    assert_eq!(batches[0][1].0, 1);
    assert_eq!(batches[0][2].0, 2);
}

#[test]
fn group_duplicate_kinds_split_into_two_batches() {
    // Two FETCH commands -> split into two batches.
    let commands = vec![
        Command::UidFetch {
            sequence_set: SequenceSet::new("1:10").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
            vanished: false,
        },
        Command::Noop,
        Command::UidFetch {
            sequence_set: SequenceSet::new("11:20").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
            vanished: false,
        },
    ];
    let consumers = vec![dummy_consumer(), dummy_consumer(), dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 2);
    // First batch: UidFetch(1:10) at idx 0, Noop at idx 1.
    assert_eq!(batches[0].len(), 2);
    assert_eq!(batches[0][0].0, 0); // original index
    assert_eq!(batches[0][1].0, 1); // original index
    // Second batch: UidFetch(11:20) at idx 2.
    assert_eq!(batches[1].len(), 1);
    assert_eq!(batches[1][0].0, 2); // original index
}

#[test]
fn group_three_same_kind_three_batches() {
    // Three STATUS commands -> three batches (one per STATUS).
    let commands = vec![
        Command::Status {
            mailbox: MailboxName::new("INBOX").unwrap(),
            items: "(MESSAGES)".into(),
        },
        Command::Status {
            mailbox: MailboxName::new("Sent").unwrap(),
            items: "(MESSAGES)".into(),
        },
        Command::Status {
            mailbox: MailboxName::new("Drafts").unwrap(),
            items: "(MESSAGES)".into(),
        },
    ];
    let consumers = vec![dummy_consumer(), dummy_consumer(), dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0][0].0, 0);
    assert_eq!(batches[1][0].0, 1);
    assert_eq!(batches[2][0].0, 2);
}

#[test]
fn group_mixed_duplicates_preserves_indices() {
    // Mix: Fetch, Status, Fetch, Status, Noop.
    // Batch 0: Fetch(0), Status(1)
    // Batch 1: Fetch(2), Status(3), Noop(4)
    let commands = vec![
        Command::UidFetch {
            sequence_set: SequenceSet::new("1:5").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
            vanished: false,
        },
        Command::Status {
            mailbox: MailboxName::new("INBOX").unwrap(),
            items: "(MESSAGES)".into(),
        },
        Command::UidFetch {
            sequence_set: SequenceSet::new("6:10").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
            vanished: false,
        },
        Command::Status {
            mailbox: MailboxName::new("Sent").unwrap(),
            items: "(MESSAGES)".into(),
        },
        Command::Noop,
    ];
    let consumers = vec![
        dummy_consumer(),
        dummy_consumer(),
        dummy_consumer(),
        dummy_consumer(),
        dummy_consumer(),
    ];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 2);
    // Batch 0: original indices 0, 1.
    let batch0_indices: Vec<usize> = batches[0].iter().map(|(i, _, _)| *i).collect();
    assert_eq!(batch0_indices, vec![0, 1]);
    // Batch 1: original indices 2, 3, 4.
    let batch1_indices: Vec<usize> = batches[1].iter().map(|(i, _, _)| *i).collect();
    assert_eq!(batch1_indices, vec![2, 3, 4]);
}

#[test]
fn group_empty_input() {
    let batches = group_into_sub_batches(Vec::new(), Vec::new());
    // Single empty batch from initialization.
    assert_eq!(batches.len(), 1);
    assert!(batches[0].is_empty());
}

#[test]
fn group_single_command() {
    let commands = vec![Command::Noop];
    let consumers = vec![dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 1);
    assert_eq!(batches[0][0].0, 0);
}

#[test]
fn group_searchres_pipeline_respects_save_ordering() {
    // SEARCHRES scenario (RFC 5182): two SearchSave commands followed by a
    // Fetch referencing `$`. The Fetch must land in the same batch as the
    // *second* SearchSave so `$` reflects the most recent SAVE result.
    //
    // Without the `max_batch` tracking fix (fc6ccd0), the Fetch would have
    // been placed in batch 0 (because batch 0 had no Fetch kind yet),
    // causing `FETCH $` to observe the first SAVE result instead of the
    // second.
    let commands = vec![
        Command::SearchSave {
            criteria: "SEEN".into(),
        },
        Command::SearchSave {
            criteria: "FLAGGED".into(),
        },
        Command::Fetch {
            sequence_set: SequenceSet::new("$").unwrap(),
            items: "(FLAGS)".into(),
            changed_since: None,
        },
    ];
    let consumers = vec![dummy_consumer(), dummy_consumer(), dummy_consumer()];
    let batches = group_into_sub_batches(commands, consumers);

    // Batch 0: SearchSave("SEEN") only.
    // Batch 1: SearchSave("FLAGGED") + Fetch("$").
    assert_eq!(batches.len(), 2);

    assert_eq!(batches[0].len(), 1);
    assert_eq!(batches[0][0].0, 0); // original index 0

    assert_eq!(batches[1].len(), 2);
    assert_eq!(batches[1][0].0, 1); // original index 1
    assert_eq!(batches[1][1].0, 2); // original index 2
}

/// Distinct kinds that solicit a common untagged response never share a
/// sub-batch: GETQUOTA and GETQUOTAROOT both own `* QUOTA`, STATUS and
/// LIST-STATUS both own `* STATUS`, SEARCH and SEARCH RETURN both own a
/// tagless `* SEARCH`, and FETCH and EXPUNGE both own `* VANISHED (EARLIER)`.
///
/// Grouping on the plain `CommandKind` fails every pair: each lands in one
/// sub-batch, where the head consumer takes the other's answer.
#[test]
fn group_splits_distinct_kinds_that_share_an_untagged_response() {
    let inbox = || MailboxName::new("INBOX").unwrap();
    let pairs = [
        (
            Command::GetQuota {
                root: String::new(),
            },
            Command::GetQuotaRoot { mailbox: inbox() },
        ),
        (
            Command::Status {
                mailbox: inbox(),
                items: "(MESSAGES)".into(),
            },
            Command::ListStatus {
                reference: String::new().into(),
                pattern: "*".into(),
                status_items: "(MESSAGES)".into(),
            },
        ),
        (
            Command::UidSearch {
                criteria: "ALL".into(),
            },
            Command::UidSearchReturn {
                criteria: "ALL".into(),
                return_opts: vec!["ALL".into()],
            },
        ),
        (
            Command::UidFetch {
                sequence_set: SequenceSet::new("1:*").unwrap(),
                items: "(FLAGS)".into(),
                changed_since: Some(1),
                vanished: true,
            },
            Command::UidExpunge {
                sequence_set: SequenceSet::new("1").unwrap(),
            },
        ),
    ];
    for (first, second) in pairs {
        let label = format!("{:?} + {:?}", first.kind(), second.kind());
        let batches = group_into_sub_batches(
            vec![first, second],
            vec![dummy_consumer(), dummy_consumer()],
        );
        assert_eq!(batches.len(), 2, "{label} must not share a sub-batch");
    }
}

// ---------------------------------------------------------------------------
// Driver completion ordering
// ---------------------------------------------------------------------------

#[test]
fn command_answer_follows_state_publication() {
    let order = std::cell::RefCell::new(Vec::new());
    let flag = std::sync::atomic::AtomicBool::new(false);
    let (reply, mut result_rx) = crate::connection::reply::reply_channel((), &flag);
    let ((), result_tx) = reply.claim().unwrap();

    publish_then_answer(|| order.borrow_mut().push("published"), result_tx, ());

    assert_eq!(*order.borrow(), ["published"]);
    assert!(
        result_rx.try_take().is_some(),
        "the caller is answered after publication"
    );
}

// ---------------------------------------------------------------------------
// Shared untagged prologue
// ---------------------------------------------------------------------------
//
// `process_untagged_prefix` is the single application point every driver read
// loop (command dispatch, pre-built command dispatch, pipeline, IDLE, literal
// sync, upgrade) now shares. The ordering it enforces is load-bearing: an
// ALERT carried on a `* BYE` must reach the event queue BEFORE the BYE error
// unwinds the command, or the user-visible reason for the disconnect is lost.
// These pin that contract at the helper, so the six call sites cannot each
// re-derive it.

use crate::connection::typed_event::TypedEvent;
use crate::types::response::{ResponseCode, UntaggedResponse, UntaggedStatus};

fn prefix_sink() -> (
    event_sink::DriverEventSink,
    tokio::sync::mpsc::Receiver<TypedEvent>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    (event_sink::DriverEventSink::new(tx, Some(64)), rx)
}

/// Run the prologue exactly as a read loop does: apply side effects to real
/// protocol state first, then hand the resulting digest to the helper.
fn run_prefix(response: UntaggedResponse) -> (Result<bool, crate::error::Error>, Vec<TypedEvent>) {
    let (mut sink, mut rx) = prefix_sink();
    let mut state = super::super::state::ProtocolState::new();
    let wrapped = crate::types::Response::Untagged(Box::new(response.clone()));
    let digest = state.apply_side_effects(&wrapped);
    let result = process_untagged_prefix(
        digest,
        &response,
        &mut sink,
        bifrost_types::TransmissionState::InFlight,
        None,
    );
    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    (result, events)
}

#[test]
fn untagged_prologue_emits_the_alert_carried_by_a_fatal_bye() {
    let (result, events) = run_prefix(UntaggedResponse::Status {
        status: UntaggedStatus::Bye,
        text: "server shutting down for maintenance".to_owned(),
        code: Some(ResponseCode::Alert),
    });

    assert!(
        matches!(result, Err(crate::error::Error::Bye { .. })),
        "a BYE must fail the in-flight command, got {result:?}"
    );
    // The command was on the wire when the BYE arrived, and the BYE does not
    // complete it: the outcome is unknown, so the evidence must say InFlight
    // rather than carry nothing (which reads as Unsent).
    assert_eq!(
        result.as_ref().unwrap_err().attempt(),
        Some(bifrost_types::TransmissionState::InFlight)
    );
    assert!(
        matches!(
            events.as_slice(),
            [TypedEvent::Alert(text)] if text == "server shutting down for maintenance"
        ),
        "the ALERT is emitted before the BYE error unwinds the command, got {events:?}"
    );
}

#[test]
fn untagged_prologue_reports_whether_a_code_event_was_emitted() {
    // A code that produces its own typed event: the caller must NOT also
    // emit the raw response, so the prologue reports `true`.
    let (result, events) = run_prefix(UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        text: "mailbox is over quota".to_owned(),
        code: Some(ResponseCode::Alert),
    });
    assert!(result.unwrap(), "an ALERT code was turned into an event");
    assert!(
        matches!(
            events.as_slice(),
            [TypedEvent::Alert(text)] if text == "mailbox is over quota"
        ),
        "got {events:?}"
    );

    // A plain untagged response emits nothing here and reports `false`, so
    // the caller still forwards it as an event of its own.
    let (result, events) = run_prefix(UntaggedResponse::Exists(42));
    assert!(
        !result.unwrap(),
        "no code event, so the caller must forward the response itself"
    );
    assert!(
        events.is_empty(),
        "the prologue does not forward plain responses; its caller does"
    );
}

// ---------------------------------------------------------------------------
// Shared consumer-less untagged arm
// ---------------------------------------------------------------------------
//
// The read loops with nowhere to route a response (IDLE's grant wait, IDLE,
// the post-DONE IDLE drain, the literal continuation wait, and the LOGOUT
// drain) answer an
// untagged response through `process_untagged_as_event`. These pin the tail
// the helper owns: forward exactly once, never on top of a code event, and
// never at all once the BYE guard has fired.

/// Run the consumer-less arm exactly as those loops do.
fn run_as_event(response: UntaggedResponse) -> (Result<(), crate::error::Error>, Vec<TypedEvent>) {
    let (mut sink, mut rx) = prefix_sink();
    let mut state = super::super::state::ProtocolState::new();
    let boxed = Box::new(response);
    let wrapped = crate::types::Response::Untagged(boxed.clone());
    let digest = state.apply_side_effects(&wrapped);
    let result = process_untagged_as_event(
        digest,
        boxed,
        &mut sink,
        bifrost_types::TransmissionState::InFlight,
    );
    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    (result, events)
}

#[test]
fn consumerless_arm_forwards_a_plain_response_as_its_own_event() {
    let (result, events) = run_as_event(UntaggedResponse::Exists(42));
    assert!(result.is_ok());
    assert!(
        matches!(events.as_slice(), [TypedEvent::Exists(42)]),
        "a loop with no consumer publishes the response itself, got {events:?}"
    );
}

#[test]
fn consumerless_arm_does_not_double_publish_a_code_event() {
    let (result, events) = run_as_event(UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        text: "mailbox is over quota".to_owned(),
        code: Some(ResponseCode::Alert),
    });
    assert!(result.is_ok());
    assert!(
        matches!(
            events.as_slice(),
            [TypedEvent::Alert(text)] if text == "mailbox is over quota"
        ),
        "the ALERT is published once, not also as a raw status event, got {events:?}"
    );
}

#[test]
fn consumerless_arm_fails_on_bye_after_publishing_its_alert() {
    let (result, events) = run_as_event(UntaggedResponse::Status {
        status: UntaggedStatus::Bye,
        text: "server shutting down".to_owned(),
        code: Some(ResponseCode::Alert),
    });
    assert!(
        matches!(result, Err(crate::error::Error::Bye { .. })),
        "a BYE is fatal in every read loop, got {result:?}"
    );
    assert!(
        matches!(
            events.as_slice(),
            [TypedEvent::Alert(text)] if text == "server shutting down"
        ),
        "the ALERT reaches the queue and the BYE is not also forwarded, got {events:?}"
    );
}

// ---------------------------------------------------------------------------
// Literal-mode selection (RFC 7888, RFC 9051 Section 4.3)
// ---------------------------------------------------------------------------

fn literal_mode_for(caps: Vec<Capability>) -> LiteralMode {
    let mut state = super::super::state::ProtocolState::new();
    state.apply_capability_fetch(caps);
    literal_mode(&state)
}

/// The driver's literal negotiation, which decides the wire bytes. Pure rev2
/// lands on the 4096-octet LITERAL- behaviour (RFC 9051 Appendix E folds in
/// LITERAL-, not LITERAL+), and unbounded LITERAL+ needs the advertised token
/// on either revision.
#[test]
fn driver_literal_mode_follows_the_capability_authority() {
    assert_eq!(
        literal_mode_for(vec![Capability::LiteralPlus, Capability::LiteralMinus]),
        LiteralMode::LiteralPlus
    );
    assert_eq!(
        literal_mode_for(vec![Capability::LiteralMinus]),
        LiteralMode::LiteralMinus
    );
    assert_eq!(
        literal_mode_for(vec![Capability::Imap4Rev2]),
        LiteralMode::LiteralMinus
    );
    assert_eq!(
        literal_mode_for(vec![Capability::Imap4Rev2, Capability::LiteralPlus]),
        LiteralMode::LiteralPlus
    );
    assert_eq!(
        literal_mode_for(vec![Capability::Imap4Rev1]),
        LiteralMode::Synchronizing
    );
}
