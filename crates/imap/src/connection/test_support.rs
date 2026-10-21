//! In-crate test doubles for `crate::connection`.
//!
//! Two shapes, both hermetic - no listener, no port, no daemon:
//!
//! * [`detached`] builds an [`ImapConnection`] with no driver task behind
//!   it. Every method that only reads the state snapshot (capability
//!   gates, session-state gates, literal-mode selection) is exercisable
//!   with zero I/O. A stray wire call fails fast with `DriverGone`
//!   instead of hanging, because the command receiver is dropped.
//! * [`driver_pair`] spawns the real driver task over a
//!   [`tokio::io::duplex`] pair, so a canned server transcript drives the
//!   full encode/send/parse/dispatch path byte for byte.

#![allow(
    clippy::wildcard_imports,
    clippy::unwrap_used,
    clippy::expect_used,
    dead_code
)]

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

/// Build an [`ImapConnection`] with no driver behind it.
///
/// The state snapshot is fixed at construction: this is the right double
/// for the pure capability / session-state gates, which read
/// `state_rx.borrow()` and never touch the socket.
pub(crate) fn detached(
    session_state: SessionState,
    capabilities: Vec<Capability>,
    enabled: &[&str],
) -> ImapConnection {
    let snapshot = driver::ConnectionStateSnapshot {
        session_state,
        capabilities,
        enabled: enabled.iter().map(|e| (*e).to_owned()).collect(),
        mailbox_selected: session_state == SessionState::Selected,
    };

    // Dropping the command receiver makes any accidental wire call fail
    // immediately rather than block the test.
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1);
    drop(cmd_rx);
    // `watch::Receiver::borrow` keeps serving the last value after the
    // sender is gone, which is exactly the snapshot semantics we want.
    let (state_tx, state_rx) = tokio::sync::watch::channel(snapshot);
    drop(state_tx);
    let (events_tx, events_rx) = tokio::sync::mpsc::channel(1);
    drop(events_tx);

    ImapConnection {
        cmd_tx,
        state_rx,
        events_rx: tokio::sync::Mutex::new(events_rx),
        driver_handle: std::sync::Arc::new(crate::connection::DriverTask::new(None)),
        host: "test.invalid".to_owned(),
        tls_active: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        abandoned: std::sync::atomic::AtomicBool::new(false),
    }
}

/// Spawn the real driver task over an in-memory duplex pair, after
/// feeding it `greeting` (RFC 3501 Section 7.1).
///
/// Returns the connection handle plus the server end of the duplex, which
/// the test scripts as a canned transcript. A `* PREAUTH` greeting lands
/// the session directly in the Authenticated state (RFC 3501 Section 3.2),
/// which is what most command-level transcripts need.
pub(crate) async fn driver_pair(greeting: &[u8]) -> (ImapConnection, tokio::io::DuplexStream) {
    driver_pair_metered(greeting, bifrost_net::BandwidthBudget::default()).await
}

pub(crate) async fn driver_pair_metered(
    greeting: &[u8],
    budget: bifrost_net::BandwidthBudget,
) -> (ImapConnection, tokio::io::DuplexStream) {
    driver_pair_with_capacity_and_budget(greeting, 1 << 16, budget).await
}

/// [`driver_pair`] over a duplex that buffers at most `capacity` octets in
/// each direction, so a transcript can make a client write stall part-way.
pub(crate) async fn driver_pair_with_capacity(
    greeting: &[u8],
    capacity: usize,
) -> (ImapConnection, tokio::io::DuplexStream) {
    driver_pair_with_capacity_and_budget(
        greeting,
        capacity,
        bifrost_net::BandwidthBudget::default(),
    )
    .await
}

async fn driver_pair_with_capacity_and_budget(
    greeting: &[u8],
    capacity: usize,
    budget: bifrost_net::BandwidthBudget,
) -> (ImapConnection, tokio::io::DuplexStream) {
    let (client, mut server) = tokio::io::duplex(capacity);

    server.write_all(greeting).await.unwrap();
    server.flush().await.unwrap();

    let mut wire_reader = wire::WireReader::new_metered(
        ImapStream::Memory(stream::Tracked::new(client)),
        None,
        Some(budget),
    );
    let mut proto_state = state::ProtocolState::new();
    let tag_gen = tag::TagGenerator::new();

    let (events_tx, events_rx) = tokio::sync::mpsc::channel::<typed_event::TypedEvent>(256);
    let event_sink = driver::event_sink::DriverEventSink::new(events_tx, None);

    let parsed = wire_reader.read_greeting().await.unwrap();
    if let Response::Greeting(ref g) = parsed {
        proto_state.apply_greeting(g).unwrap();
    }

    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(16);
    let (state_tx, state_rx) = tokio::sync::watch::channel(proto_state.snapshot());
    let inbound_bytes = wire_reader.metering().inbound_counter();
    let handle = tokio::spawn(driver::driver_task(
        wire_reader,
        proto_state,
        tag_gen,
        cmd_rx,
        state_tx,
        event_sink,
    ));

    let conn = ImapConnection {
        cmd_tx,
        state_rx,
        events_rx: tokio::sync::Mutex::new(events_rx),
        driver_handle: std::sync::Arc::new(crate::connection::DriverTask::with_inbound_counter(
            Some(handle),
            inbound_bytes,
        )),
        host: "test.invalid".to_owned(),
        tls_active: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        abandoned: std::sync::atomic::AtomicBool::new(false),
    };

    (conn, server)
}

/// Move a [`driver_pair`] session into the Selected state with a real
/// `SELECT INBOX` exchange, so a transcript can issue Selected-only commands
/// (RFC 3501 Section 6.4) that the handle and the driver would otherwise
/// refuse as illegal for the session.
pub(crate) async fn select_inbox(conn: &ImapConnection, server: &mut tokio::io::DuplexStream) {
    let select = conn.select("INBOX", std::time::Duration::from_secs(5));
    let script = async {
        let line = read_line(server).await;
        let tag = tag_of(&line).to_owned();
        respond(
            server,
            &format!(
                "* FLAGS (\\Seen)\r\n* 0 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 1] valid\r\n\
                 {tag} OK [READ-WRITE] selected\r\n"
            ),
        )
        .await;
    };
    let (selected, ()) = tokio::join!(select, script);
    selected.unwrap();
}

/// A `* PREAUTH` greeting advertising the given capability atoms.
pub(crate) fn preauth_greeting(caps: &str) -> Vec<u8> {
    format!("* PREAUTH [CAPABILITY {caps}] ready\r\n").into_bytes()
}

/// Read one CRLF-terminated line from the server end of the duplex.
///
/// Reads a byte at a time so the caller can interleave line reads with
/// exact-length literal reads without an intermediate buffer stealing
/// bytes that belong to the next read.
pub(crate) async fn read_line(server: &mut tokio::io::DuplexStream) -> String {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = server.read(&mut byte).await.unwrap();
        assert_eq!(n, 1, "server end hit EOF mid-line: {out:?}");
        out.push(byte[0]);
        if out.ends_with(b"\r\n") {
            return String::from_utf8(out).unwrap();
        }
    }
}

/// Read exactly `n` bytes from the server end of the duplex.
pub(crate) async fn read_exact(server: &mut tokio::io::DuplexStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    server.read_exact(&mut buf).await.unwrap();
    buf
}

/// The command tag of a client line (RFC 3501 Section 2.2.1).
pub(crate) fn tag_of(line: &str) -> &str {
    line.split(' ').next().unwrap_or_default()
}

/// Write a canned server transcript chunk and flush it.
pub(crate) async fn respond(server: &mut tokio::io::DuplexStream, bytes: &str) {
    server.write_all(bytes.as_bytes()).await.unwrap();
    server.flush().await.unwrap();
}
