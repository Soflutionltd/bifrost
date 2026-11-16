#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::match_wildcard_for_single_variants,
    clippy::single_match_else,
    clippy::match_wild_err_arm,
    clippy::single_match,
    clippy::wildcard_imports
)]
use super::*;
use crate::types::response::TaggedResponse;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

// ===========================================================================
// Driver-panic supervision tests
//
// These tests verify invariant I7: a consumer that panics inside the
// driver task surfaces as `crate::error::InternalFailure::Panicked` on the caller side,
// with the panic message preserved. Post-panic commands fail fast with
// `DriverPanicked` or `DriverGone` rather than hanging.
//
// These live in the lib-test module (not tests/invariants.rs) because
// they need access to internal types: `Consumer`, `Command`,
// `submit_regular`.
// ===========================================================================

/// Create a test `ImapConnection` backed by the driver task with an
/// in-memory `DuplexStream` pair. Returns the connection and the
/// server-side stream for scripting responses.
///
/// Constructs the driver-based architecture: the `ImapConnection`
/// holds `cmd_tx`, `state_rx`, `events_rx`, and a `JoinHandle` for
/// the driver task.
async fn make_driver_test_pair() -> (ImapConnection, tokio::io::DuplexStream) {
    let (client, mut server) = tokio::io::duplex(65536);

    // Write greeting from server side.
    server
        .write_all(b"* OK [CAPABILITY IMAP4rev1] ready\r\n")
        .await
        .unwrap();
    server.flush().await.unwrap();

    // --- Pre-driver phase: mirror connect_with_tls_config's init ---
    let mut wire_reader = wire::WireReader::new(ImapStream::Memory(stream::Tracked::new(client)));
    let mut proto_state = state::ProtocolState::new();
    let tag_gen = tag::TagGenerator::new();

    let (events_tx, events_rx) = tokio::sync::mpsc::channel::<typed_event::TypedEvent>(256);
    let event_sink = driver::event_sink::DriverEventSink::new(events_tx, None);

    // Read and process the greeting.
    let greeting = wire_reader.read_greeting().await.unwrap();
    if let Response::Greeting(ref g) = greeting {
        proto_state.apply_greeting(g).unwrap();
    }

    // Spawn the driver task.
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(16);
    let (state_tx, state_rx) = tokio::sync::watch::channel(proto_state.snapshot());
    let inbound_bytes = wire_reader.metering().inbound_counter();
    let driver_task = super::DriverTask::spawn(
        driver::driver_task(
            wire_reader,
            proto_state,
            tag_gen,
            cmd_rx,
            state_tx,
            event_sink,
        ),
        inbound_bytes,
        Vec::new(),
    );

    let conn = ImapConnection {
        cmd_tx,
        state_rx,
        events_rx: tokio::sync::Mutex::new(events_rx),
        driver_handle: std::sync::Arc::new(driver_task),
        tls_active: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        abandoned: std::sync::atomic::AtomicBool::new(false),
        host: "test".into(),
    };

    (conn, server)
}

/// Consumer that intentionally panics in `on_response`. Used by the
/// driver-panic supervision test to inject a panic into the driver task.
struct PanickingConsumer;

impl dispatch::Consumer for PanickingConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        _resp: UntaggedResponse,
        _notify: NotifyFlags,
        _ctx: &dispatch::ConsumerContext,
    ) -> Option<UntaggedResponse> {
        panic!("intentional panic for test");
    }

    fn finalize(
        self: Box<Self>,
        _tagged: TaggedResponse,
        _ctx: &dispatch::ConsumerContext,
    ) -> dispatch::Finalized<()> {
        dispatch::Finalized::success((), Vec::new())
    }
}

/// A consumer that cannot interpret the server's well-framed answer: it
/// finalizes the tagged OK with a desynchronization-class error.
struct UninterpretableConsumer;

impl dispatch::Consumer for UninterpretableConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        _resp: UntaggedResponse,
        _notify: NotifyFlags,
        _ctx: &dispatch::ConsumerContext,
    ) -> Option<UntaggedResponse> {
        None
    }

    fn finalize(
        self: Box<Self>,
        _tagged: TaggedResponse,
        _ctx: &dispatch::ConsumerContext,
    ) -> dispatch::Finalized<()> {
        dispatch::Finalized::failure(
            Error::Provider(crate::error::ProviderFailure::Parse(
                "uninterpretable answer".into(),
            )),
            Vec::new(),
        )
    }
}

// ---------------------------------------------------------------------------
// selected_mailbox_effective_responses (RFC 7162 Section 3.2.11)
// ---------------------------------------------------------------------------

fn ok_code(code: ResponseCode) -> UntaggedResponse {
    UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        code: Some(code),
        text: String::new(),
    }
}

// ---------------------------------------------------------------------------
// build_selected_mailbox (RFC 3501 Section 7 / RFC 7162 Section 3)
// ---------------------------------------------------------------------------

fn tagged(code: Option<ResponseCode>) -> TaggedResponse {
    TaggedResponse {
        tag: "A001".to_owned(),
        status: crate::types::response::StatusKind::Ok,
        code,
        text: "done".to_owned(),
    }
}

// ---------------------------------------------------------------------------
// NOTIFY classification helpers (RFC 5465 Section 5.4, RFC 5258 Section 3)
// ---------------------------------------------------------------------------

fn mailbox_info(attrs: Vec<MailboxAttribute>) -> MailboxInfo {
    MailboxInfo {
        name: MailboxName::new("INBOX").unwrap(),
        delimiter: Some('/'),
        attributes: attrs,
        old_name: None,
        child_info: Vec::new(),
    }
}

// ===========================================================================
// Byte-level command transcripts over the in-memory duplex
// ===========================================================================

use crate::connection::test_support::{preauth_greeting, read_line, respond, tag_of};

/// Wait until the driver task has fully exited, without polling anything
/// else of the connection. Once it has, IDLE's event channel and its result
/// channel are BOTH ready, so the next poll of an `idle()` future takes the
/// event-channel arm deterministically (it is polled first).
async fn wait_for_driver_exit(conn: &ImapConnection) {
    loop {
        let finished = conn
            .driver_handle
            .handle
            .lock()
            .await
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished);
        if finished {
            return;
        }
        tokio::task::yield_now().await;
    }
}

mod withdrawal;

/// The CAPABILITY refresh a committed LOGIN / AUTHENTICATE / UNAUTHENTICATE
/// owes runs inside the driver arm that claimed the transition.
mod transition_refresh;

mod admission;

mod append;

mod auth;

mod dispatch_policy;

mod events;

mod fetch;

mod idle;

mod lifecycle;

mod list;

mod select;
