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
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

// ===========================================================================
// Driver-panic supervision tests
//
// These tests verify invariant I7: a consumer that panics inside the
// driver task surfaces as `Error::DriverPanicked` on the caller side,
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
    ) {
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

// ---------------------------------------------------------------------------
// Driver-panic  -  consumer panic surfaces as DriverPanicked (I7)
// ---------------------------------------------------------------------------

/// The peer-certificate handle routes through the full driver path
/// (handle method -> `DriverCommand::PeerCertificate` -> driver-loop arm
/// -> `WireReader` -> `ImapStream::Memory`) and returns `None` over the
/// non-TLS in-memory transport. This pins the dispatch wiring: a missing
/// driver-loop arm would hang the oneshot and the test would time out.
#[tokio::test]
async fn peer_certificate_der_none_over_memory_stream() {
    let (conn, _server) = make_driver_test_pair().await;
    assert!(
        conn.peer_certificate_der().await.is_none(),
        "in-memory (non-TLS) transport must have no peer certificate DER"
    );
}

/// Dropping the last handle detaches the driver task, which then runs
/// the terminal LOGOUT drain with nothing able to abort it. Against a
/// peer that stays connected but never answers, an unbounded drain
/// leaks the task and its socket for the process lifetime - so the
/// drain must be time-bounded. Paused time: the bound elapses
/// instantly, and a regression (unbounded read loop) fails the outer
/// timeout instead of hanging the suite.
#[tokio::test(start_paused = true)]
async fn driver_logout_drain_is_bounded_against_a_silent_peer() {
    let (conn, mut server) = make_driver_test_pair().await;
    let handle = conn
        .driver_handle
        .handle
        .lock()
        .await
        .take()
        .expect("driver handle present");
    drop(conn);

    // The driver writes its best-effort LOGOUT; the peer reads it and
    // then goes silent while keeping the connection open.
    let mut buf = [0u8; 256];
    let n = server.read(&mut buf).await.unwrap();
    assert!(
        String::from_utf8_lossy(&buf[..n]).contains("LOGOUT"),
        "driver must attempt the best-effort LOGOUT"
    );

    tokio::time::timeout(std::time::Duration::from_secs(300), handle)
        .await
        .expect("bounded LOGOUT drain must let the detached driver exit")
        .expect("driver task must not panic");
    drop(server);
}

/// Invariant: a consumer that panics inside the driver task surfaces
/// as `Error::DriverPanicked` on the caller side. The panic message
/// is extracted from the `JoinError` and included in the error.
#[tokio::test]
async fn invariant_driver_panic_surfaces_as_error() {
    let (conn, mut server) = make_driver_test_pair().await;

    let server_task = tokio::spawn(async move {
        // Receive the CAPABILITY command from the driver.
        let mut buf = vec![0u8; 4096];
        let n = server.read(&mut buf).await.unwrap();
        let _cmd = String::from_utf8_lossy(&buf[..n]).to_string();

        // Send an untagged CAPABILITY response. This triggers
        // on_response on the PanickingConsumer, which panics.
        server
            .write_all(b"* CAPABILITY IMAP4rev1\r\n")
            .await
            .unwrap();
        // Also send the tagged OK in case the driver processes it
        // before the panic unwinds (it won't, but be safe).
        server
            .write_all(b"A001 OK CAPABILITY done\r\n")
            .await
            .unwrap();
        server.flush().await.unwrap();
        server
    });

    let result = conn
        .submit_regular(Command::Capability, PanickingConsumer, None)
        .await;
    let _server = server_task.await.unwrap();

    match result {
        Err(error @ Error::DriverPanicked { .. }) => {
            let Error::DriverPanicked { message: msg, .. } = &error else {
                unreachable!()
            };
            assert!(
                msg.contains("intentional panic for test"),
                "panic message not propagated: {msg}"
            );
            // The driver owned this command when it died, so it may have
            // been written: `InFlight`, which a non-idempotent operation
            // reads back rather than declaring failed.
            assert_eq!(
                error.attempt(),
                Some(bifrost_types::TransmissionState::InFlight)
            );
        }
        other => panic!("expected DriverPanicked, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Driver-panic  -  subsequent commands fail fast (I7)
// ---------------------------------------------------------------------------

/// After the driver panics, subsequent commands must fail promptly
/// with `DriverPanicked` or `DriverGone`  -  not hang forever.
#[tokio::test]
async fn invariant_driver_panic_subsequent_commands_fail() {
    let (conn, mut server) = make_driver_test_pair().await;

    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        let _ = server.read(&mut buf).await;
        server
            .write_all(b"* CAPABILITY IMAP4rev1\r\nA001 OK done\r\n")
            .await
            .unwrap();
        server.flush().await.unwrap();
        server
    });

    // Trigger the panic via PanickingConsumer.
    let _ = conn
        .submit_regular(Command::Capability, PanickingConsumer, None)
        .await;
    let _server = server_task.await.unwrap();

    // Post-panic: the next command must not hang.
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        conn.submit_regular(Command::Noop, dispatch::TaggedOkConsumer::default(), None),
    )
    .await;

    assert!(
        result.is_ok(),
        "post-panic command must complete (not hang)"
    );
    let inner = result.unwrap();
    assert!(
        matches!(
            inner,
            Err(Error::DriverPanicked { .. } | Error::DriverGone { .. })
        ),
        "post-panic command should be DriverPanicked or DriverGone, got: {inner:?}"
    );
    // This command never reached the dead driver: `Unsent`, whichever of the
    // two the caller observed and whether or not it was first to observe.
    assert_eq!(
        inner.expect_err("post-panic command fails").attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
}

// ===========================================================================
// Pure helpers defined in `connection/mod.rs`
// ===========================================================================

// ---------------------------------------------------------------------------
// validate_tls_server_name
// ---------------------------------------------------------------------------

#[test]
fn tls_server_name_rejects_empty_and_whitespace_and_nul() {
    assert!(validate_tls_server_name("imap.example.test").is_ok());
    assert!(validate_tls_server_name("").is_err());
    assert!(validate_tls_server_name("imap example.test").is_err());
    assert!(validate_tls_server_name("imap.example.test\n").is_err());
    assert!(validate_tls_server_name("imap\0.example.test").is_err());
}

// ---------------------------------------------------------------------------
// filter_store_flags (RFC 3501 Section 2.3.2)
// ---------------------------------------------------------------------------

#[test]
fn store_flag_filter_drops_server_only_and_wildcard_flags() {
    let out = filter_store_flags(&[Flag::Recent, Flag::Seen, Flag::Wildcard, Flag::Deleted]);
    assert_eq!(out, vec![Flag::Seen, Flag::Deleted]);
}

// ---------------------------------------------------------------------------
// expand_uid_ranges (RFC 4731 Section 3.1)
// ---------------------------------------------------------------------------

#[test]
fn expand_uid_ranges_singles_and_ranges() {
    let uids = expand_uid_ranges(&[UidRange::single(1), UidRange::range(4, 6)])
        .expect("small ranges expand");
    assert_eq!(uids, vec![1, 4, 5, 6]);
}

#[test]
fn expand_uid_ranges_empty_input() {
    let uids = expand_uid_ranges(&[]).expect("empty range list expands");
    assert!(uids.is_empty());
}

#[test]
fn expand_uid_ranges_star_bound_is_not_expanded() {
    // RFC 4731 Section 3.1: `*` is the highest UID in the mailbox, which
    // the ESEARCH response alone does not reveal.
    let err = expand_uid_ranges(&[UidRange::from_sequence_bounds(
        crate::types::SeqBound::Number(10),
        Some(crate::types::SeqBound::Star),
    )])
    .expect_err("an unexpandable `*` range must not look complete");
    assert!(matches!(
        err,
        Error::SearchResultTruncated {
            returned: 0,
            omitted: None
        }
    ));
}

#[test]
fn expand_uid_ranges_accepts_the_largest_numeric_uid() {
    assert_eq!(
        expand_uid_ranges(&[UidRange::single(u32::MAX)])
            .expect("the maximum numeric UID is concrete"),
        vec![u32::MAX]
    );
}

#[test]
fn expand_uid_ranges_caps_at_one_million() {
    let err = expand_uid_ranges(&[UidRange::range(1, 3_000_000)])
        .expect_err("an over-cap range must not be returned partially");
    assert!(matches!(
        err,
        Error::SearchResultTruncated {
            returned: 1_000_000,
            omitted: Some(2_000_000)
        }
    ));
}

#[test]
fn expand_uid_ranges_inverted_range_yields_nothing() {
    // Defensive: a non-conformant server sending `10:5` must not panic.
    let uids = expand_uid_ranges(&[UidRange::range(10, 5)])
        .expect("inverted range is empty, not incomplete");
    assert!(uids.is_empty());
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

#[test]
fn closed_response_code_splits_off_the_previous_mailbox() {
    let responses = vec![
        UntaggedResponse::Exists(1),
        ok_code(ResponseCode::Closed),
        UntaggedResponse::Exists(7),
    ];
    let effective = selected_mailbox_effective_responses(&responses);
    assert_eq!(effective.len(), 1);
    assert!(matches!(effective[0], UntaggedResponse::Exists(7)));
}

#[test]
fn without_closed_every_response_is_effective() {
    let responses = vec![UntaggedResponse::Exists(1), UntaggedResponse::Recent(0)];
    assert_eq!(selected_mailbox_effective_responses(&responses).len(), 2);
}

#[test]
fn only_the_last_closed_marker_counts() {
    let responses = vec![
        UntaggedResponse::Exists(1),
        ok_code(ResponseCode::Closed),
        UntaggedResponse::Exists(2),
        ok_code(ResponseCode::Closed),
        UntaggedResponse::Exists(3),
    ];
    let effective = selected_mailbox_effective_responses(&responses);
    assert_eq!(effective.len(), 1);
    assert!(matches!(effective[0], UntaggedResponse::Exists(3)));
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

#[test]
fn selected_mailbox_collects_counts_flags_and_codes() {
    let untagged = vec![
        UntaggedResponse::Exists(17),
        UntaggedResponse::Recent(2),
        UntaggedResponse::Flags(vec![Flag::Seen, Flag::Draft]),
        ok_code(ResponseCode::UidValidity(42)),
        ok_code(ResponseCode::UidNext(100)),
        ok_code(ResponseCode::PermanentFlags(vec![Flag::Seen])),
        ok_code(ResponseCode::Unseen(3)),
        ok_code(ResponseCode::MailboxId("mb-1".to_owned())),
        ok_code(ResponseCode::UidNotSticky),
    ];
    let mailbox = build_selected_mailbox(&untagged, &tagged(None), false);

    assert_eq!(mailbox.exists, 17);
    assert_eq!(mailbox.recent, 2);
    assert_eq!(mailbox.flags, vec![Flag::Seen, Flag::Draft]);
    assert_eq!(mailbox.uid_validity, Some(42));
    assert_eq!(mailbox.uid_next, Some(100));
    assert_eq!(mailbox.permanent_flags, vec![Flag::Seen]);
    assert_eq!(mailbox.unseen, Some(3));
    assert_eq!(mailbox.mailbox_id.as_deref(), Some("mb-1"));
    assert!(mailbox.uid_not_sticky);
    assert!(!mailbox.read_only);
}

#[test]
fn selected_mailbox_reads_the_tagged_response_code_too() {
    // RFC 3501 Section 6.3.1: SELECT reports [READ-WRITE] / [UIDVALIDITY]
    // on the tagged line on some servers.
    let mailbox = build_selected_mailbox(&[], &tagged(Some(ResponseCode::UidValidity(9))), true);
    assert_eq!(mailbox.uid_validity, Some(9));
    assert!(mailbox.read_only);
}

#[test]
fn selected_mailbox_missing_uidvalidity_stays_none() {
    // Never fabricate the invalid sentinel 0 (RFC 3501 Section 9: nz-number).
    let mailbox = build_selected_mailbox(&[UntaggedResponse::Exists(1)], &tagged(None), false);
    assert_eq!(mailbox.uid_validity, None);
}

#[test]
fn selected_mailbox_highest_modseq_zero_is_treated_as_nomodseq() {
    // RFC 7162 Section 3.1.2.1: mod-sequence-value >= 1, so HIGHESTMODSEQ 0
    // is the server meaning [NOMODSEQ].
    let zero = build_selected_mailbox(
        &[ok_code(ResponseCode::HighestModSeq(0))],
        &tagged(None),
        false,
    );
    assert_eq!(zero.highest_mod_seq, None);
    assert!(zero.no_mod_seq);

    let real = build_selected_mailbox(
        &[ok_code(ResponseCode::HighestModSeq(77))],
        &tagged(None),
        false,
    );
    assert_eq!(real.highest_mod_seq, Some(77));
    assert!(!real.no_mod_seq);

    let none = build_selected_mailbox(&[ok_code(ResponseCode::NoModSeq)], &tagged(None), false);
    assert!(none.no_mod_seq);
}

#[test]
fn selected_mailbox_keeps_only_earlier_vanished() {
    // RFC 7162 Section 3.2.5.2: only VANISHED (EARLIER) belongs to the
    // initial QRESYNC sync; a bare VANISHED is an unsolicited expunge.
    let untagged = vec![
        UntaggedResponse::Vanished {
            earlier: true,
            uids: vec![UidRange::range(1, 3)],
        },
        UntaggedResponse::Vanished {
            earlier: false,
            uids: vec![UidRange::single(9)],
        },
    ];
    let mailbox = build_selected_mailbox(&untagged, &tagged(None), false);
    assert_eq!(mailbox.vanished.len(), 1);
    assert_eq!(mailbox.vanished[0].start, 1);
    assert_eq!(mailbox.vanished[0].end, Some(3));
}

#[test]
fn selected_mailbox_ignores_state_from_before_closed() {
    let untagged = vec![
        UntaggedResponse::Exists(99),
        ok_code(ResponseCode::UidValidity(1)),
        ok_code(ResponseCode::Closed),
        UntaggedResponse::Exists(4),
        ok_code(ResponseCode::UidValidity(2)),
    ];
    let mailbox = build_selected_mailbox(&untagged, &tagged(None), false);
    assert_eq!(mailbox.exists, 4);
    assert_eq!(mailbox.uid_validity, Some(2));
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

#[test]
fn oldname_always_marks_a_notify_list_event() {
    let mut info = mailbox_info(Vec::new());
    info.old_name = Some(MailboxName::new("Old").unwrap());
    assert!(is_notify_list_event(&info, true));
    assert!(
        is_notify_list_event(&info, false),
        "OLDNAME is a marker regardless of LIST-EXTENDED context"
    );
}

#[test]
fn nonexistent_and_noaccess_are_markers_only_outside_subscribed_listings() {
    for attr in [MailboxAttribute::NonExistent, MailboxAttribute::NoAccess] {
        let info = mailbox_info(vec![attr]);
        assert!(is_notify_list_event(&info, true));
        assert!(
            !is_notify_list_event(&info, false),
            "LIST-EXTENDED with SUBSCRIBED may legitimately return these"
        );
    }
}

#[test]
fn a_plain_list_response_is_not_a_notify_event() {
    let info = mailbox_info(vec![MailboxAttribute::HasNoChildren]);
    assert!(!is_notify_list_event(&info, true));
}

#[test]
fn selection_mismatch_detects_missing_subscribed() {
    let plain = mailbox_info(Vec::new());
    assert!(is_notify_selection_mismatch(&plain, &["SUBSCRIBED"]));

    let subscribed = mailbox_info(vec![MailboxAttribute::Subscribed]);
    assert!(!is_notify_selection_mismatch(&subscribed, &["SUBSCRIBED"]));
}

#[test]
fn selection_mismatch_accepts_childinfo_under_recursivematch() {
    // RFC 5258 Section 3.5: a parent with subscribed children is returned
    // without \Subscribed but with CHILDINFO.
    let mut info = mailbox_info(Vec::new());
    info.child_info = vec!["SUBSCRIBED".to_owned()];
    assert!(!is_notify_selection_mismatch(
        &info,
        &["SUBSCRIBED", "RECURSIVEMATCH"]
    ));
    assert!(
        is_notify_selection_mismatch(&info, &["SUBSCRIBED"]),
        "CHILDINFO only excuses the missing attribute under RECURSIVEMATCH"
    );
}

#[test]
fn selection_mismatch_checks_remote_and_special_use() {
    let plain = mailbox_info(Vec::new());
    assert!(is_notify_selection_mismatch(&plain, &["REMOTE"]));
    assert!(is_notify_selection_mismatch(&plain, &["SPECIAL-USE"]));

    let remote = mailbox_info(vec![MailboxAttribute::Remote]);
    assert!(!is_notify_selection_mismatch(&remote, &["remote"]));

    let archive = mailbox_info(vec![MailboxAttribute::Archive]);
    assert!(!is_notify_selection_mismatch(&archive, &["SPECIAL-USE"]));
}

#[test]
fn selection_mismatch_with_no_options_is_never_a_mismatch() {
    assert!(!is_notify_selection_mismatch(
        &mailbox_info(Vec::new()),
        &[]
    ));
}

#[tokio::test]
async fn driver_channel_liveness_is_independent_of_session_state() {
    let detached =
        crate::connection::test_support::detached(SessionState::Selected, Vec::new(), &[]);
    assert_eq!(detached.session_state(), SessionState::Selected);
    assert!(!detached.is_alive(), "the command receiver was dropped");

    let (live, _server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    assert!(
        live.is_alive(),
        "the driver still owns the command receiver"
    );
}

// ===========================================================================
// Byte-level command transcripts over the in-memory duplex
// ===========================================================================

use crate::connection::test_support::{preauth_greeting, read_line, respond, tag_of};

#[tokio::test]
async fn select_round_trip_parses_the_untagged_block() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("SELECT"), "unexpected command: {line}");
        assert!(line.contains("INBOX"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 3 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * FLAGS (\\Seen \\Deleted)\r\n\
                 * OK [UIDVALIDITY 4242] UIDs valid\r\n\
                 * OK [UIDNEXT 9] Predicted next UID\r\n\
                 {tag} OK [READ-WRITE] SELECT completed\r\n"
            ),
        )
        .await;
        server
    });

    let mailbox = conn.select("INBOX", Duration::from_secs(5)).await.unwrap();
    let _server = script.await.unwrap();

    assert_eq!(mailbox.exists, 3);
    assert_eq!(mailbox.uid_validity, Some(4242));
    assert_eq!(mailbox.uid_next, Some(9));
    assert_eq!(mailbox.flags, vec![Flag::Seen, Flag::Deleted]);
    assert!(!mailbox.read_only);
    assert_eq!(conn.session_state(), SessionState::Selected);
}

#[tokio::test]
async fn select_no_response_leaves_the_session_authenticated() {
    // RFC 3501 Section 6.3.1: a tagged NO deselects.
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("{tag} NO [NONEXISTENT] no such mailbox\r\n"),
        )
        .await;
        server
    });

    let err = conn
        .select("Missing", Duration::from_secs(5))
        .await
        .expect_err("SELECT NO must surface as an error");
    let _server = script.await.unwrap();

    assert!(matches!(err, Error::No { .. }), "got {err:?}");
    assert_eq!(conn.session_state(), SessionState::Authenticated);
    assert!(conn.is_alive(), "a tagged NO leaves the wire reusable");
}

/// RFC 3501 Section 6.3.1: a SELECT that completes with a tagged OK but
/// without the mandatory FLAGS response is the server's omission, reported as
/// `ProtocolMissing`. The exchange is over and the framing intact, so the
/// connection must stay usable: the next command on it round-trips.
#[tokio::test]
async fn a_missing_mandatory_response_leaves_the_connection_usable() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT"), "unexpected command: {select}");
        let tag = tag_of(&select).to_owned();
        respond(
            &mut server,
            &format!(
                "* 3 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 4242] UIDs valid\r\n\
                 {tag} OK [READ-WRITE] SELECT completed\r\n"
            ),
        )
        .await;
        let noop = read_line(&mut server).await;
        assert!(noop.contains("NOOP"), "unexpected command: {noop}");
        let tag = tag_of(&noop).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let err = conn
        .select("INBOX", Duration::from_secs(5))
        .await
        .expect_err("a SELECT without FLAGS must fail");
    assert!(matches!(err, Error::ProtocolMissing(_)), "got {err:?}");
    assert!(
        conn.is_alive(),
        "an omission after the tagged OK leaves the wire synchronized"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection must still carry the next command");
    let _server = script.await.unwrap();
}

/// RFC 3501 Section 6.1.3 owes a BYE only ahead of a tagged OK. A server that
/// refuses LOGOUT has not started closing, so the caller must see the
/// refusal itself, not a "missing BYE" protocol error that blames the server
/// for an omission it never made.
#[tokio::test]
async fn a_refused_logout_reports_the_refusal_not_a_missing_bye() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let logout = read_line(&mut server).await;
        assert!(logout.contains("LOGOUT"), "unexpected command: {logout}");
        let tag = tag_of(&logout).to_owned();
        respond(&mut server, &format!("{tag} NO not now\r\n")).await;
        server
    });

    let err = conn.logout().await.expect_err("a refused LOGOUT fails");
    assert!(
        matches!(err, Error::No { ref text, .. } if text.contains("not now")),
        "got {err:?}"
    );
    let _server = script.await.unwrap();
}

/// RFC 3501 Section 6.1.3 requires the server's BYE before LOGOUT's tagged OK.
/// The BYE is part of this command's response sequence and must reach its
/// consumer instead of aborting dispatch as a server-initiated disconnect.
#[tokio::test]
async fn a_successful_logout_consumes_bye_before_its_tagged_ok() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let logout = read_line(&mut server).await;
        assert!(logout.contains("LOGOUT"), "unexpected command: {logout}");
        let tag = tag_of(&logout).to_owned();
        respond(
            &mut server,
            &format!("* BYE logging out\r\n{tag} OK LOGOUT completed\r\n"),
        )
        .await;
        server
    });

    conn.logout()
        .await
        .expect("a conforming BYE followed by tagged OK completes LOGOUT");
    assert_eq!(
        conn.state_rx.borrow().session_state,
        SessionState::Logout,
        "the completed LOGOUT must publish the terminal session state"
    );
    let _server = script.await.unwrap();
}

/// The channel capacity is 16. A caller dropped while its command waits for a
/// slot never submitted that command and must not abandon the live connection.
#[tokio::test]
async fn dropping_a_command_waiting_for_channel_capacity_does_not_abandon_connection() {
    use std::future::Future;
    use std::sync::Arc;

    const QUEUED: usize = 16;
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let conn = Arc::new(conn);

    let first_conn = Arc::clone(&conn);
    let first = tokio::spawn(async move { first_conn.noop(Duration::from_secs(5)).await });
    let first_line = read_line(&mut server).await;
    assert!(
        first_line.contains("NOOP"),
        "unexpected command: {first_line}"
    );
    let first_tag = tag_of(&first_line).to_owned();

    let queued: Vec<_> = (0..QUEUED)
        .map(|_| {
            let conn = Arc::clone(&conn);
            tokio::spawn(async move { conn.noop(Duration::from_secs(5)).await })
        })
        .collect();
    for _ in 0..128 {
        if conn.cmd_tx.capacity() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        conn.cmd_tx.capacity(),
        0,
        "all 16 channel slots must be occupied before polling the blocked send"
    );

    let mut blocked = Box::pin(conn.noop(Duration::from_secs(5)));
    let send_is_pending =
        std::future::poll_fn(|cx| std::task::Poll::Ready(blocked.as_mut().poll(cx).is_pending()))
            .await;
    assert!(
        send_is_pending,
        "the seventeenth queued command must wait to submit"
    );
    drop(blocked);
    assert!(
        conn.is_reusable(),
        "dropping before channel admission must not mark a submitted command abandoned"
    );

    respond(&mut server, &format!("{first_tag} OK first completed\r\n")).await;
    first
        .await
        .unwrap()
        .expect("the in-flight command should complete");
    for _ in 0..QUEUED {
        let line = read_line(&mut server).await;
        assert!(line.contains("NOOP"), "unexpected queued command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(&mut server, &format!("{tag} OK queued completed\r\n")).await;
    }
    for task in queued {
        task.await
            .unwrap()
            .expect("each submitted command should complete");
    }
    assert!(
        conn.is_reusable(),
        "draining accepted commands must leave the connection reusable"
    );
}

/// The channel-binding lookup a SCRAM rung makes before AUTHENTICATE is a
/// driver request, so it queues behind a stalled head command. It must spend
/// the attempt's deadline and fail `Timeout` / `Unsent` there, not wait out
/// the stalled command and then authenticate past the deadline.
#[tokio::test(start_paused = true)]
async fn a_scram_binding_lookup_behind_a_stalled_command_honours_the_auth_deadline() {
    use std::sync::Arc;

    let greeting = b"* OK [CAPABILITY IMAP4rev1 AUTH=SCRAM-SHA-256] ready\r\n";
    let (conn, mut server) = crate::connection::test_support::driver_pair(greeting).await;
    let conn = Arc::new(conn);
    let stalled = Arc::clone(&conn);
    let _stalled = tokio::spawn(async move { stalled.noop(Duration::from_secs(600)).await });
    let noop = read_line(&mut server).await;
    assert!(noop.contains("NOOP"), "unexpected command: {noop}");

    let started = tokio::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        conn.authenticate_best(
            &crate::types::Credentials::password("user", "pass"),
            &crate::types::AuthPolicy::default(),
            Duration::from_secs(1),
        ),
    )
    .await
    .expect("authentication must end at its own deadline");
    let error = outcome.expect_err("the binding lookup cannot complete");
    assert!(matches!(error, Error::Timeout { .. }), "got {error:?}");
    assert_eq!(
        error.attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(1)
    );
}

/// A SCRAM server-final error is returned while AUTHENTICATE still awaits its
/// tagged completion. Cancel and drain that completion before the next command.
#[tokio::test]
async fn a_scram_server_final_error_cancels_and_leaves_the_connection_reusable() {
    use base64::Engine;
    use bifrost_sasl::{ScramChannelBinding, ScramHash};

    let greeting = b"* OK [CAPABILITY IMAP4rev1 AUTH=SCRAM-SHA-256] ready\r\n";
    let (conn, mut server) = crate::connection::test_support::driver_pair(greeting).await;
    let consumer = dispatch::AuthenticateScramConsumer::new(
        ScramHash::Sha256,
        "user".to_owned(),
        "wrong".to_owned().into(),
        "nonce123".to_owned(),
        false,
        ScramChannelBinding::None,
    )
    .unwrap();
    let command = Command::Authenticate {
        mechanism: "SCRAM-SHA-256".to_owned(),
        initial_response: None,
    };

    let script = tokio::spawn(async move {
        let authenticate = read_line(&mut server).await;
        assert!(
            authenticate.contains("AUTHENTICATE SCRAM-SHA-256"),
            "unexpected command: {authenticate}"
        );
        let auth_tag = tag_of(&authenticate).to_owned();
        respond(&mut server, "+\r\n").await;

        let client_first_line = read_line(&mut server).await;
        let client_first = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(client_first_line.trim())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(client_first, "n,,n=user,r=nonce123");
        let server_first = "r=nonce123server,s=c2FsdA==,i=4096";
        let server_first = base64::engine::general_purpose::STANDARD.encode(server_first);
        respond(&mut server, &format!("+ {server_first}\r\n")).await;

        let _client_final = read_line(&mut server).await;
        let server_final = base64::engine::general_purpose::STANDARD.encode("e=invalid-proof");
        respond(&mut server, &format!("+ {server_final}\r\n")).await;
        assert_eq!(
            read_line(&mut server).await,
            "*\r\n",
            "a local SASL failure must cancel the open exchange"
        );
        respond(
            &mut server,
            &format!("{auth_tag} NO authentication failed\r\n"),
        )
        .await;

        let noop = read_line(&mut server).await;
        assert!(noop.contains("NOOP"), "unexpected next command: {noop}");
        let noop_tag = tag_of(&noop).to_owned();
        respond(&mut server, &format!("{noop_tag} OK NOOP completed\r\n")).await;
        server
    });

    let error = conn
        .submit_with_continuations(command, consumer, None)
        .await
        .expect_err("the server-final error must reject authentication");
    assert!(matches!(error, Error::Auth { .. }), "got {error:?}");
    assert!(
        conn.is_alive(),
        "a completed SASL cancellation keeps the driver alive"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the next command must follow the consumed tagged NO");
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn uid_fetch_round_trip_with_a_literal_body() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    // Get into Selected state first.
    let select_script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * FLAGS (\\Seen)\r\n\
                 * OK [UIDVALIDITY 1] UIDs valid\r\n\
                 {tag} OK [READ-WRITE] SELECT completed\r\n"
            ),
        )
        .await;
        server
    });
    conn.select("INBOX", Duration::from_secs(5)).await.unwrap();
    let mut server = select_script.await.unwrap();

    let fetch_script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("UID FETCH"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 7 FLAGS (\\Seen) BODY[TEXT] {{11}}\r\nHELLO WORLD)\r\n\
                 {tag} OK UID FETCH completed\r\n"
            ),
        )
        .await;
        server
    });

    let set = SequenceSet::new("7").unwrap();
    let items = [
        FetchAttr::Uid,
        FetchAttr::Flags,
        FetchAttr::BodySection {
            peek: true,
            section: Some("TEXT".to_owned()),
            partial: None,
        },
    ];
    let fetched = conn
        .uid_fetch(&set, &items, Duration::from_secs(5))
        .await
        .unwrap();
    let _server = fetch_script.await.unwrap();

    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].uid, Some(7));
    assert_eq!(fetched[0].flags.as_deref(), Some(&[Flag::Seen][..]));
    assert_eq!(fetched[0].body_sections.len(), 1);
    assert_eq!(
        fetched[0].body_sections[0].data.as_deref(),
        Some(&b"HELLO WORLD"[..])
    );
}

#[tokio::test]
async fn unsolicited_responses_during_noop_become_typed_events() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("NOOP"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("* 12 EXISTS\r\n* 4 EXPUNGE\r\n{tag} OK NOOP completed\r\n"),
        )
        .await;
        server
    });

    conn.noop(Duration::from_secs(5)).await.unwrap();
    let _server = script.await.unwrap();

    let events = conn.drain_events().await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, typed_event::TypedEvent::Exists(12))),
        "EXISTS was not surfaced: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, typed_event::TypedEvent::Expunge(4))),
        "EXPUNGE was not surfaced: {events:?}"
    );
}

#[tokio::test]
async fn capability_response_updates_the_cached_snapshot() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    assert!(!conn.capabilities().contains(&Capability::Idle));

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("CAPABILITY"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 IDLE MOVE\r\n{tag} OK CAPABILITY completed\r\n"),
        )
        .await;
        server
    });

    let caps = conn.capability(Duration::from_secs(5)).await.unwrap();
    let _server = script.await.unwrap();

    assert!(caps.contains(&Capability::Idle));
    assert!(conn.capabilities().contains(&Capability::Move));
}

#[tokio::test]
async fn bye_mid_command_preserves_the_response_code_without_waiting_for_close() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _line = read_line(&mut server).await;
        respond(&mut server, "* BYE [UNAVAILABLE] server shutting down\r\n").await;
        let _ = release_rx.await;
    });

    let err = conn
        .noop(Duration::from_secs(5))
        .await
        .expect_err("the command cannot complete after BYE");

    assert!(
        matches!(
            err,
            Error::Bye {
                code: Some(ResponseCode::Unavailable),
                ..
            }
        ),
        "BYE must preserve its structured response code: {err:?}"
    );
    // The NOOP was on the wire and the BYE did not complete it, so its
    // outcome is unknown: InFlight, not the missing evidence that reads as
    // Unsent.
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "{err:?}"
    );
    assert!(
        !conn.is_alive(),
        "BYE must close the driver command channel even while the peer stays open"
    );
    let _ = release_tx.send(());
    script.await.unwrap();
}

#[tokio::test]
async fn bye_carrying_a_capability_code_still_ends_the_command() {
    // `* BYE [CAPABILITY ...]` is a real shutdown shape (servers repeat their
    // pre-login capabilities on the way out). The response code must not
    // route the response away from the fatal-BYE lane.
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _line = read_line(&mut server).await;
        respond(
            &mut server,
            "* BYE [CAPABILITY IMAP4rev1 MOVE] shutting down\r\n",
        )
        .await;
        let _ = release_rx.await;
    });

    let err = tokio::time::timeout(Duration::from_secs(5), conn.noop(Duration::from_secs(30)))
        .await
        .expect("BYE must end the command without waiting for the peer to close")
        .expect_err("the command cannot complete after BYE");

    assert!(
        matches!(
            err,
            Error::Bye {
                code: Some(ResponseCode::Capability(_)),
                ..
            }
        ),
        "BYE must preserve its structured response code: {err:?}"
    );
    assert!(
        !conn.is_alive(),
        "BYE must close the driver command channel"
    );
    let _ = release_tx.send(());
    script.await.unwrap();
}

#[tokio::test]
async fn starttls_rejects_an_empty_capability_snapshot_before_touching_the_driver() {
    // This models the plaintext `TlsMode::None` connection state. A missing
    // capability is not permission to attempt a downgrade-adjacent upgrade.
    let conn =
        crate::connection::test_support::detached(SessionState::NotAuthenticated, Vec::new(), &[]);
    let connector = native_tls::TlsConnector::builder().build().unwrap();

    assert!(matches!(
        conn.starttls_with_connector(connector, Duration::from_secs(1))
            .await,
        Err(Error::StartTlsUnavailable)
    ));
}

#[tokio::test]
async fn append_waits_for_the_continuation_before_sending_the_literal() {
    // No LITERAL+/LITERAL- advertised, so the APPEND literal is
    // synchronizing (RFC 3501 Section 4.3): the driver must stop after
    // `{n}\r\n` and wait for `+`.
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;

    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(header.contains("APPEND"), "unexpected command: {header}");
        assert!(
            header.trim_end().ends_with("{5}"),
            "expected a synchronizing literal marker, got: {header}"
        );
        let tag = tag_of(&header).to_owned();
        // The driver tags APPEND from its own generator, like every command.
        assert!(
            tag.len() == 16 && tag.bytes().all(|b| b.is_ascii_hexdigit()),
            "APPEND must carry a driver-generated tag, got: {header}"
        );

        // Nothing more may arrive until we grant the continuation.
        respond(&mut server, "+ Ready for literal data\r\n").await;

        let body = crate::connection::test_support::read_exact(&mut server, 5).await;
        assert_eq!(&body[..], b"HELLO");
        let trailer = read_line(&mut server).await;
        assert_eq!(trailer, "\r\n");

        respond(
            &mut server,
            &format!("{tag} OK [APPENDUID 4242 12] APPEND completed\r\n"),
        )
        .await;
        server
    });

    let uid = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();

    assert_eq!(uid, Some((4242, 12)));
}

#[tokio::test]
async fn append_uses_a_non_synchronizing_literal_under_literal_plus() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 LITERAL+ UIDPLUS",
    ))
    .await;

    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(
            header.trim_end().ends_with("{5+}"),
            "expected a LITERAL+ marker, got: {header}"
        );
        let tag = tag_of(&header).to_owned();
        // The payload follows without a continuation.
        let body = crate::connection::test_support::read_exact(&mut server, 5).await;
        assert_eq!(&body[..], b"HELLO");
        let trailer = read_line(&mut server).await;
        assert_eq!(trailer, "\r\n");
        respond(&mut server, &format!("{tag} OK APPEND completed\r\n")).await;
        server
    });

    let uid = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();
    assert_eq!(uid, None, "no APPENDUID means no id");
}

#[tokio::test]
async fn literal_plus_append_does_not_wait_on_a_marker_shaped_body_line() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 LITERAL+ UIDPLUS",
    ))
    .await;

    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(header.trim_end().ends_with("{11+}"));
        let tag = tag_of(&header).to_owned();
        let body = crate::connection::test_support::read_exact(&mut server, 11).await;
        assert_eq!(&body[..], b"abc{3}\r\ndef");
        assert_eq!(read_line(&mut server).await, "\r\n");
        respond(&mut server, &format!("{tag} OK APPEND completed\r\n")).await;
        server
    });

    conn.append("INBOX", &[], None, b"abc{3}\r\ndef", Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn append_rejects_oversized_messages_before_the_wire() {
    // RFC 7889: APPENDLIMIT is enforced client-side.
    let (conn, _server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 APPENDLIMIT=4"))
            .await;
    match conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
    {
        Err(Error::AppendLimit { size, limit }) => {
            assert_eq!(size, 5);
            assert_eq!(limit, 4);
        }
        other => panic!("expected AppendLimit, got {other:?}"),
    }
}

#[tokio::test]
async fn append_uses_the_most_restrictive_global_appendlimit() {
    let conn = crate::connection::test_support::detached(
        SessionState::Authenticated,
        vec![
            Capability::AppendLimit(Some(10)),
            Capability::AppendLimit(Some(4)),
        ],
        &[],
    );

    assert!(matches!(
        conn.append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
            .await,
        Err(Error::AppendLimit { size: 5, limit: 4 })
    ));
}

#[tokio::test]
async fn append_checks_the_mailbox_specific_appendlimit_before_writing() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 APPENDLIMIT"))
            .await;
    let script = tokio::spawn(async move {
        let status = read_line(&mut server).await;
        assert!(
            status.ends_with(" STATUS \"INBOX\" (APPENDLIMIT)\r\n"),
            "expected an APPENDLIMIT STATUS query, got {status:?}"
        );
        let tag = tag_of(&status).to_owned();
        respond(
            &mut server,
            &format!("* STATUS INBOX (APPENDLIMIT 4)\r\n{tag} OK STATUS completed\r\n"),
        )
        .await;
        server
    });

    assert!(matches!(
        conn.append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
            .await,
        Err(Error::AppendLimit { size: 5, limit: 4 })
    ));
    let _server = script.await.expect("STATUS transcript");
}

/// A capability that disappears while the APPENDLIMIT preflight is in flight
/// must not leave MULTIAPPEND emitting bytes encoded for the old one.
///
/// This needs NO second handle and no task interleaving, which is what makes it
/// the sharp case. `multi_append` used to snapshot `allow_literal8` BEFORE
/// awaiting the `STATUS`, then patch its literal markers with that stale value.
/// A server may send an unsolicited `* CAPABILITY` at any time (RFC 3501
/// Section 7.2.1), so answering the STATUS with one that drops BINARY left the
/// command still emitting the non-synchronizing `~{n+}` form. A server that does
/// not accept non-synchronizing literal8 then reads the MESSAGE BODY as command
/// lines - a desynchronized parse, not a clean rejection.
///
/// The driver now encodes and validates from its own live state when it
/// executes the command, which is necessarily after the `STATUS` completed. So
/// the vanished BINARY is seen there and the command is refused before a byte
/// is written. The test after this one stages the harder variant, where the
/// state moves while the command sits QUEUED behind another.
#[tokio::test]
async fn multiappend_refuses_bytes_built_before_a_capability_vanished() {
    use crate::types::AppendMessage;

    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 MULTIAPPEND BINARY LITERAL+ APPENDLIMIT",
    ))
    .await;
    let script = tokio::spawn(async move {
        let status = read_line(&mut server).await;
        let tag = tag_of(&status).to_owned();
        // The unsolicited CAPABILITY drops BINARY, so literal8 loses its `+`
        // eligibility (RFC 7888 Section 6) the instant this lands.
        respond(
            &mut server,
            &format!(
                "* STATUS INBOX (APPENDLIMIT 9999)\r\n\
                 * CAPABILITY IMAP4rev1 MULTIAPPEND LITERAL+ APPENDLIMIT\r\n\
                 {tag} OK STATUS completed\r\n"
            ),
        )
        .await;
        server
    });

    // NUL forces literal8, which is the form whose `+` eligibility BINARY
    // governs.
    let messages = vec![AppendMessage::new(b"a\0b".to_vec())];
    let result = conn
        .multi_append("INBOX", &messages, Duration::from_secs(5))
        .await;
    let _server = script.await.expect("STATUS transcript");

    let err = result.expect_err("bytes built against the pre-CAPABILITY state must not be sent");
    assert!(
        matches!(err, Error::MissingCapability(ref m) if m.contains("requires BINARY literal8 support")),
        "the literal kind must be derived from the state at execution, so a \
         vanished BINARY is caught by the driver; got {err:?}"
    );
}

/// A queued APPEND is validated against the state at EXECUTION, not the state
/// when it was submitted.
///
/// Handle A's NOOP is in flight with its reply withheld. Handle B's APPEND of
/// a NUL-bearing body is submitted while BINARY is still advertised and queues
/// behind A. A's reply then carries a CAPABILITY that drops BINARY. The driver
/// encodes B only when it reaches it, so it sees BINARY gone and refuses
/// before writing a byte; a handle-side encoding would have promised a
/// `~{n+}` literal8 the server no longer accepts. (This test used to pin the
/// driver's `WireAssumptions` comparison, which existed only because the bytes
/// were built handle-side and could go stale in the queue.)
///
/// Paused time is what makes the staging deterministic. A 1 ms sleep can only
/// complete by auto-advance, which the runtime performs only once every task
/// is idle - so when it returns, B has necessarily reached its wait on the
/// driver's reply, i.e. it is queued.
#[tokio::test(start_paused = true)]
async fn a_queued_append_is_validated_against_the_state_at_execution() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 BINARY LITERAL+",
    ))
    .await;
    let conn = std::sync::Arc::new(conn);
    let (noop_seen_tx, noop_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let script = tokio::spawn(async move {
        let noop = read_line(&mut server).await;
        assert!(noop.ends_with(" NOOP\r\n"), "expected NOOP, got {noop:?}");
        let tag = tag_of(&noop).to_owned();
        noop_seen_tx.send(()).expect("test is waiting");
        release_rx.await.expect("test releases the reply");
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 LITERAL+\r\n{tag} OK NOOP completed\r\n"),
        )
        .await;
        // Everything the client writes from here until it hangs up. Any
        // APPEND in it means the stale bytes reached the wire. Read to EOF
        // rather than expecting a particular next command, so the assertion
        // holds however the refusal leaves the connection.
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest)
            .await
            .expect("duplex read");
        let rest = String::from_utf8_lossy(&rest);
        assert!(
            !rest.contains("APPEND"),
            "the stale APPEND must never be written, got {rest:?}"
        );
    });

    let first = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.noop(Duration::from_secs(5)).await }
    });
    noop_seen_rx.await.expect("server saw the NOOP");
    let append = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        // NUL forces literal8, whose `+` eligibility BINARY governs.
        async move {
            conn.append("INBOX", &[], None, b"a\0b", Duration::from_secs(5))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    release_tx.send(()).expect("server is waiting");

    first.await.expect("noop task").expect("NOOP completes");
    let err = append
        .await
        .expect("append task")
        .expect_err("an APPEND the live state cannot carry must not be sent");
    assert!(
        matches!(err, Error::MissingCapability(ref m) if m.contains("requires BINARY literal8 support")),
        "the driver must validate the queued APPEND against the state it \
         executes under; got {err:?}"
    );
    // Dropping the last handle closes the client half, which ends the
    // server's read.
    drop(conn);
    script.await.expect("NOOP transcript");
}

/// The same staging, for an APPEND the live state CAN still carry: it must be
/// encoded for the state at execution, not the state at submission.
///
/// LITERAL+ is advertised when the APPEND is submitted, so a handle-side
/// encoding would emit `{5+}` and stream the body without waiting. A's reply
/// drops LITERAL+, and the driver, encoding when it reaches the APPEND, must
/// instead emit the synchronizing `{5}` and wait for `+` (RFC 3501 Section 4.3).
/// Writing the `+` form to a server without LITERAL+ would make it read the body
/// as command lines.
#[tokio::test(start_paused = true)]
async fn a_queued_append_is_encoded_for_the_state_at_execution() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 LITERAL+ UIDPLUS",
    ))
    .await;
    let conn = std::sync::Arc::new(conn);
    let (noop_seen_tx, noop_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let script = tokio::spawn(async move {
        let noop = read_line(&mut server).await;
        assert!(noop.ends_with(" NOOP\r\n"), "expected NOOP, got {noop:?}");
        let tag = tag_of(&noop).to_owned();
        noop_seen_tx.send(()).expect("test is waiting");
        release_rx.await.expect("test releases the reply");
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 UIDPLUS\r\n{tag} OK NOOP completed\r\n"),
        )
        .await;

        let header = read_line(&mut server).await;
        assert!(
            header.ends_with(" APPEND \"INBOX\" {5}\r\n"),
            "LITERAL+ is gone by execution time, so the marker must be \
             synchronizing; got {header:?}"
        );
        let tag = tag_of(&header).to_owned();
        respond(&mut server, "+ Ready for literal data\r\n").await;
        let body = crate::connection::test_support::read_exact(&mut server, 5).await;
        assert_eq!(&body[..], b"HELLO");
        assert_eq!(read_line(&mut server).await, "\r\n");
        respond(
            &mut server,
            &format!("{tag} OK [APPENDUID 7 3] APPEND completed\r\n"),
        )
        .await;
        server
    });

    let first = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.noop(Duration::from_secs(5)).await }
    });
    noop_seen_rx.await.expect("server saw the NOOP");
    let append = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move {
            conn.append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    release_tx.send(()).expect("server is waiting");

    first.await.expect("noop task").expect("NOOP completes");
    let uid = append
        .await
        .expect("append task")
        .expect("the APPEND is encoded for the live state and succeeds");
    assert_eq!(uid, Some((7, 3)));
    let _server = script.await.expect("transcript");
}

/// Session legality is re-checked by the driver at execution. An APPEND that
/// was legal when submitted is refused if a command ahead of it moved the
/// session out of Authenticated/Selected, nothing is written, and the
/// connection stays usable: the refusal happens before the first byte, so it
/// must not retire the connection. The variant is `StateChangedBeforeSend`,
/// with `Unsent` evidence: local state moved under a queued command, which is
/// transient, never a provider fault.
#[tokio::test(start_paused = true)]
async fn a_queued_append_is_refused_when_the_session_left_the_legal_states() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 UNAUTHENTICATE"))
            .await;
    let conn = std::sync::Arc::new(conn);
    let (unauth_seen_tx, unauth_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let script = tokio::spawn(async move {
        let unauth = read_line(&mut server).await;
        assert!(
            unauth.ends_with(" UNAUTHENTICATE\r\n"),
            "expected UNAUTHENTICATE, got {unauth:?}"
        );
        let tag = tag_of(&unauth).to_owned();
        unauth_seen_tx.send(()).expect("test is waiting");
        release_rx.await.expect("test releases the reply");
        respond(&mut server, &format!("{tag} OK Unauthenticated\r\n")).await;

        // The very next thing on the wire is the capability refresh that
        // `unauthenticate` issues. The queued APPEND sat ahead of it in the
        // driver's queue, so if it had been written it would be here instead,
        // and if its refusal had retired the connection nothing would be.
        let next = read_line(&mut server).await;
        assert!(
            next.ends_with(" CAPABILITY\r\n"),
            "the queued APPEND must not have been written; got {next:?}"
        );
        let tag = tag_of(&next).to_owned();
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1\r\n{tag} OK CAPABILITY completed\r\n"),
        )
        .await;
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest)
            .await
            .expect("duplex read");
        let rest = String::from_utf8_lossy(&rest);
        assert!(!rest.contains("APPEND"), "APPEND was written: {rest:?}");
    });

    let first = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.unauthenticate(Duration::from_secs(5)).await }
    });
    unauth_seen_rx.await.expect("server saw UNAUTHENTICATE");
    let append = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move {
            conn.append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    release_tx.send(()).expect("server is waiting");

    first
        .await
        .expect("unauthenticate task")
        .expect("UNAUTHENTICATE and its CAPABILITY refresh complete");
    let err = append
        .await
        .expect("append task")
        .expect_err("the session is no longer legal for APPEND");
    assert!(
        matches!(err, Error::StateChangedBeforeSend(_)),
        "session legality must be refused as StateChangedBeforeSend; got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent),
        "nothing was written, and the error must say so"
    );
    drop(conn);
    script.await.expect("transcript");
}

/// An APPEND the driver refuses during validation leaves the connection
/// usable. The refusal (here a NUL-bearing body on a server without BINARY) is
/// made before the first byte, so the framing is intact and retiring the
/// connection would throw away a healthy one. The follow-up NOOP is the proof.
#[tokio::test(start_paused = true)]
async fn a_refused_append_leaves_the_connection_usable() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let script = tokio::spawn(async move {
        let next = read_line(&mut server).await;
        assert!(
            next.ends_with(" NOOP\r\n"),
            "the refused APPEND must not reach the wire; got {next:?}"
        );
        let tag = tag_of(&next).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let err = conn
        .append("INBOX", &[], None, b"a\0b", Duration::from_secs(5))
        .await
        .expect_err("a NUL body without BINARY must be refused");
    assert!(
        matches!(err, Error::MissingCapability(ref m) if m.contains("BINARY")),
        "expected the BINARY refusal; got {err:?}"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection survives a refusal made before the first byte");
    let _server = script.await.expect("NOOP transcript");
}

/// MULTIAPPEND validates the MULTIAPPEND capability in the driver, at
/// execution, and writes no APPEND when it is missing.
///
/// Paused time because dropping the last handle makes the driver attempt a
/// best-effort LOGOUT against a server that never answers, bounded by a
/// five-second drain timeout that would otherwise run in real time.
#[tokio::test(start_paused = true)]
async fn multiappend_without_the_capability_is_refused_before_the_wire() {
    use crate::types::AppendMessage;

    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 LITERAL+")).await;
    let messages = vec![
        AppendMessage::new(&b"one"[..]),
        AppendMessage::new(&b"two"[..]),
    ];
    let err = conn
        .multi_append("INBOX", &messages, Duration::from_secs(5))
        .await
        .expect_err("MULTIAPPEND is not advertised");
    assert!(
        matches!(err, Error::MissingCapability(ref m) if m.contains("MULTIAPPEND")),
        "got {err:?}"
    );
    drop(conn);
    let mut rest = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest)
        .await
        .expect("duplex read");
    let rest = String::from_utf8_lossy(&rest);
    assert!(
        !rest.contains("APPEND"),
        "no APPEND may be written, got {rest:?}"
    );
}

/// MULTIAPPEND over the wire with synchronizing literals: one continuation per
/// message body, the second message's syntax follows the first body directly,
/// and the command ends with a single CRLF (RFC 3502 Section 3).
#[tokio::test]
async fn multiappend_waits_for_a_continuation_before_each_body() {
    use crate::types::AppendMessage;

    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 MULTIAPPEND UIDPLUS",
    ))
    .await;
    let script = tokio::spawn(async move {
        let first = read_line(&mut server).await;
        assert!(
            first.ends_with(" APPEND \"INBOX\" (\\Seen) {5}\r\n"),
            "{first:?}"
        );
        let tag = tag_of(&first).to_owned();
        respond(&mut server, "+ go\r\n").await;
        assert_eq!(
            &crate::connection::test_support::read_exact(&mut server, 5).await[..],
            b"HELLO"
        );
        let second = read_line(&mut server).await;
        assert_eq!(second, " {3}\r\n", "second message follows the first body");
        respond(&mut server, "+ go\r\n").await;
        assert_eq!(
            &crate::connection::test_support::read_exact(&mut server, 3).await[..],
            b"BYE"
        );
        assert_eq!(read_line(&mut server).await, "\r\n");
        respond(
            &mut server,
            &format!("{tag} OK [APPENDUID 9 10:11] APPEND completed\r\n"),
        )
        .await;
        server
    });

    let mut first = AppendMessage::new(&b"HELLO"[..]);
    first.flags = vec![crate::types::Flag::Seen];
    let messages = vec![first, AppendMessage::new(&b"BYE"[..])];
    let uids = conn
        .multi_append("INBOX", &messages, Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();
    assert_eq!(uids, vec![(9, 10), (9, 11)]);
}

/// The APPENDLIMIT STATUS is a preflight. Its own transmission evidence is
/// about the STATUS, not about the APPEND, and APPEND is non-idempotent: a
/// preflight failure that reported `InFlight` would send DraftCreate recovery
/// down the reconcile path for a message no server ever saw.
#[tokio::test]
async fn append_preflight_failure_is_unsent_evidence_for_the_append() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 APPENDLIMIT"))
            .await;
    let script = tokio::spawn(async move {
        let status = read_line(&mut server).await;
        let tag = tag_of(&status).to_owned();
        // A tagged NO is Acknowledged evidence for the STATUS.
        respond(&mut server, &format!("{tag} NO STATUS failed\r\n")).await;
        server
    });

    let err = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .expect_err("a failed preflight must fail the APPEND");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent),
        "no APPEND octet was written, so the evidence must say so: {err:?}"
    );
    let _server = script.await.expect("STATUS transcript");
}

/// RFC 5465 Section 4: with NOTIFY STATUS active the solicited reply may land
/// in `ambiguous` instead of `items`. Reading only `items` invents a protocol
/// error, or applies a stale higher limit.
#[test]
fn mailbox_append_limit_reads_every_plausible_status_reply() {
    use crate::connection::append::mailbox_append_limit;
    use crate::types::StatusItem;
    use crate::types::mailbox::StatusResult;

    // The heuristic picked a notification; the real answer is ambiguous.
    let status = StatusResult {
        items: vec![StatusItem::Messages(3)],
        ambiguous: vec![vec![StatusItem::AppendLimit(Some(4))]],
    };
    assert_eq!(mailbox_append_limit(&status).unwrap(), Some(4));

    // Both plausible replies name a limit: the smallest one governs.
    let status = StatusResult {
        items: vec![StatusItem::AppendLimit(Some(9))],
        ambiguous: vec![vec![StatusItem::AppendLimit(Some(4))]],
    };
    assert_eq!(mailbox_append_limit(&status).unwrap(), Some(4));

    // An explicit NIL is "no limit", not a missing item.
    let status = StatusResult {
        items: vec![StatusItem::AppendLimit(None)],
        ambiguous: vec![],
    };
    assert_eq!(mailbox_append_limit(&status).unwrap(), None);

    // No reply named APPENDLIMIT at all: the server contradicts itself.
    let status = StatusResult {
        items: vec![StatusItem::Messages(3)],
        ambiguous: vec![vec![StatusItem::Messages(3)]],
    };
    assert!(matches!(
        mailbox_append_limit(&status),
        Err(Error::ProtocolMissing(_))
    ));
}

/// RFC 9051 Section 9: `seq-number = nz-number / "*"`, so `*` is special
/// wherever it appears, and a sequence-set may repeat or overlap elements.
#[test]
fn expand_uid_ranges_handles_bare_star_and_overlap() {
    let err = expand_uid_ranges(&[UidRange::from_sequence_bounds(
        crate::types::SeqBound::Star,
        None,
    )])
    .expect_err("a standalone `*` is not the literal id 4294967295");
    assert!(matches!(
        err,
        Error::SearchResultTruncated {
            returned: 0,
            omitted: None
        }
    ));

    // Overlap is legal and must be counted once, both for the cap and for the
    // expansion itself.
    let uids = expand_uid_ranges(&[UidRange::range(1, 600_000), UidRange::range(1, 600_000)])
        .expect("600000 distinct UIDs are under the 1e6 cap");
    assert_eq!(uids.len(), 600_000);

    // Partial overlap merges rather than duplicating, and the result is
    // ascending.
    let uids = expand_uid_ranges(&[
        UidRange::range(5, 9),
        UidRange::range(1, 6),
        UidRange::single(7),
    ])
    .expect("small overlapping ranges expand");
    assert_eq!(uids, (1..=9).collect::<Vec<u32>>());
}

// ---------------------------------------------------------------------------
// IDLE transcripts (RFC 2177): the DONE handshake and server termination
// ---------------------------------------------------------------------------

#[tokio::test]
async fn idle_returns_the_first_event_and_completes_the_done_handshake() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let idle = read_line(&mut server).await;
        assert!(idle.ends_with(" IDLE\r\n"), "expected IDLE, got {idle:?}");
        let tag = tag_of(&idle).to_owned();
        respond(&mut server, "+ idling\r\n").await;
        respond(&mut server, "* 3 EXISTS\r\n").await;
        // RFC 2177 Section 3: the client ends the session with an untagged
        // DONE, and only then does the tagged completion arrive.
        assert_eq!(read_line(&mut server).await, "DONE\r\n");
        respond(&mut server, &format!("{tag} OK IDLE terminated\r\n")).await;
        server
    });

    let event = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("IDLE must complete through the DONE handshake");
    assert!(matches!(event, IdleEvent::Exists(3)), "got {event:?}");
    let _server = script.await.unwrap();

    assert!(
        conn.is_alive(),
        "a completed IDLE session leaves the connection reusable"
    );
}

#[tokio::test]
async fn idle_surfaces_a_server_terminated_session_without_done() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let idle = read_line(&mut server).await;
        let tag = tag_of(&idle).to_owned();
        respond(&mut server, "+ idling\r\n").await;
        // RFC 2177 Section 3: the server MAY end IDLE unilaterally by
        // sending the tagged completion before any DONE.
        respond(&mut server, &format!("{tag} OK IDLE ended by server\r\n")).await;
        server
    });

    let event = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("a server-terminated IDLE is not an error");
    assert!(
        matches!(event, IdleEvent::ServerTerminated),
        "got {event:?}"
    );
    let _server = script.await.unwrap();
}

/// The driver owned the IDLE command when it died - it had already written
/// it - so the error must say `InFlight`. The event channel and the result
/// channel close together; IDLE's wait reads the event channel first, and that
/// arm used to mint a phase-less `DriverGone`, which reads as `Unsent`.
#[tokio::test]
async fn a_driver_death_mid_idle_is_in_flight() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let idle = read_line(&mut server).await;
        assert!(idle.ends_with(" IDLE\r\n"), "expected IDLE, got {idle:?}");
        respond(&mut server, "+ idling\r\n").await;
        let _ = entered_tx.send(());
        server
    });

    let (result, ()) = tokio::join!(
        conn.idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        ),
        async {
            entered_rx.await.unwrap();
            conn.terminate().await;
        }
    );
    let err = result.expect_err("a dead driver must fail the IDLE");
    assert!(
        matches!(err, Error::DriverGone { .. } | Error::DriverPanicked { .. }),
        "got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "the IDLE command was on the wire: {err:?}"
    );
    let _server = script.await.unwrap();
}

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

/// A BYE mid-IDLE ends the driver after it has answered the IDLE command
/// with the BYE. IDLE's event-channel arm is polled first and used to replace
/// that answer with a generic `DriverGone`, losing the server's text and code.
#[tokio::test]
async fn a_bye_mid_idle_surfaces_as_the_bye_not_as_driver_gone() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n* BYE server shutting down\r\n").await;
        let _ = sent_tx.send(());
        server
    });

    let mut idle = std::pin::pin!(conn.idle(
        Duration::from_secs(5),
        Duration::from_secs(5),
        tokio_util::sync::CancellationToken::new(),
    ));
    assert!(
        futures::FutureExt::now_or_never(&mut idle).is_none(),
        "IDLE is waiting"
    );
    sent_rx.await.unwrap();
    wait_for_driver_exit(&conn).await;

    let err = idle.await.expect_err("a BYE ends the IDLE");
    assert!(
        matches!(err, Error::Bye { ref text, .. } if text.contains("shutting down")),
        "got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "the IDLE was on the wire: {err:?}"
    );
    let _server = script.await.unwrap();
}

/// RFC 2177 Section 3: the server grants IDLE with `+`. A tagged OK for IDLE
/// with no `+` first is an omission by a server that finished the exchange:
/// the error must say so in IDLE's terms (it used to be worded as a literal
/// continuation fault) and, the framing being intact, must not retire the
/// connection.
#[tokio::test]
async fn an_idle_completed_without_its_continuation_is_an_idle_omission() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let idle = read_line(&mut server).await;
        let tag = tag_of(&idle).to_owned();
        respond(&mut server, &format!("{tag} OK IDLE done already\r\n")).await;
        let noop = read_line(&mut server).await;
        assert!(noop.contains("NOOP"), "unexpected command: {noop}");
        let tag = tag_of(&noop).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let err = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("IDLE without its continuation is a server omission");
    assert!(
        matches!(err, Error::ProtocolMissing(ref m) if m.contains("IDLE") && !m.contains("literal")),
        "got {err:?}"
    );
    assert!(
        conn.is_alive(),
        "the exchange completed; the wire is in sync"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection still carries the next command");
    let _server = script.await.unwrap();
}

/// The IDLE command is on the wire once the server grants it, so an EOF in
/// the IDLE read loop is `InFlight`. It used to carry no phase and read as
/// `Unsent`.
#[tokio::test]
async fn an_eof_mid_idle_is_in_flight() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n").await;
        drop(server);
        let _ = sent_tx.send(());
    });

    let mut idle = std::pin::pin!(conn.idle(
        Duration::from_secs(5),
        Duration::from_secs(5),
        tokio_util::sync::CancellationToken::new(),
    ));
    assert!(
        futures::FutureExt::now_or_never(&mut idle).is_none(),
        "IDLE is waiting"
    );
    sent_rx.await.unwrap();
    wait_for_driver_exit(&conn).await;

    let err = idle.await.expect_err("an EOF ends the IDLE");
    assert!(
        matches!(err, Error::Closed { .. } | Error::Io { .. }),
        "the driver's own error, not a generic DriverGone: {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "{err:?}"
    );
    script.await.unwrap();
}

/// An EOF while draining after DONE is `InFlight` too: the IDLE command and
/// the DONE are both on the wire.
#[tokio::test]
async fn an_eof_while_draining_after_done_is_in_flight() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n* 3 EXISTS\r\n").await;
        assert_eq!(read_line(&mut server).await, "DONE\r\n");
        // No tagged completion: the peer goes away instead.
        drop(server);
    });

    let err = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_secs(5),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("an EOF during the DONE drain fails the IDLE");
    assert!(
        matches!(err, Error::Closed { .. } | Error::Io { .. }),
        "got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "{err:?}"
    );
    script.await.unwrap();
}

/// `next_event` submits nothing, so a dead driver is `Unsent` from its point
/// of view - stated, not left to the missing-evidence default.
#[tokio::test]
async fn next_event_on_a_dead_driver_carries_a_phase() {
    let dead = crate::connection::test_support::detached(SessionState::Selected, Vec::new(), &[]);
    let err = dead
        .next_event(Duration::from_secs(5))
        .await
        .expect_err("a closed event channel is a dead driver");
    assert!(matches!(err, Error::DriverGone { .. }), "got {err:?}");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
}

// A peer whose TCP is alive but which never answers the tagged OK for IDLE
// used to park the caller in `result_rx.await` forever: past the cancel
// token, past account shutdown, past `close()`. The DONE handshake carries
// the same bound as every other command.
#[tokio::test]
async fn idle_done_handshake_is_bounded_when_the_server_never_completes() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = tokio::spawn(async move {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n").await;
        respond(&mut server, "* 3 EXISTS\r\n").await;
        assert_eq!(read_line(&mut server).await, "DONE\r\n");
        // No tagged completion, ever. The socket stays open.
        tokio::time::sleep(Duration::from_secs(30)).await;
        server
    });

    let err = conn
        .idle(
            Duration::from_secs(5),
            Duration::from_millis(100),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("an unanswered DONE must not park the caller");
    assert!(matches!(err, crate::Error::Timeout { .. }), "got {err:?}");
    script.abort();
}

/// A DONE handshake that times out leaves the driver inside the IDLE
/// exchange: the connection is abandoned, so it is not reusable, and dropping
/// its last handle aborts the driver, releasing the socket. Without that, a
/// driver blocked in the post-DONE drain against a silent peer would hold the
/// transport for the life of the process.
///
/// Removing IDLE's in-flight guard fails the reusability assertion; removing
/// `ImapConnection`'s `Drop` fails the EOF assertion (the server end never
/// sees the client close).
#[tokio::test(start_paused = true)]
async fn an_idle_whose_done_times_out_is_abandoned_and_its_last_drop_releases_the_socket() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;

    let script = async {
        let _idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n").await;
        respond(&mut server, "* 3 EXISTS\r\n").await;
        assert_eq!(read_line(&mut server).await, "DONE\r\n");
        // No tagged completion.
    };
    let idle = conn.idle(
        Duration::from_secs(5),
        Duration::from_millis(100),
        tokio_util::sync::CancellationToken::new(),
    );
    let (result, ()) = tokio::join!(idle, script);
    assert!(
        matches!(result, Err(crate::Error::Timeout { .. })),
        "got {result:?}"
    );
    assert!(
        !conn.is_reusable(),
        "the driver is still inside the IDLE exchange"
    );

    drop(conn);
    let mut rest = Vec::new();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest),
    )
    .await;
    assert!(
        matches!(closed, Ok(Ok(_))),
        "the abandoned driver kept the socket open after its last handle dropped"
    );
}

/// Ending IDLE through the cancel token with a completed DONE handshake is a
/// completed exchange: the connection stays reusable, which push relies on to
/// re-IDLE on the same session after a subscription change.
#[tokio::test]
async fn an_idle_cancelled_through_its_token_stays_reusable() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let cancel = tokio_util::sync::CancellationToken::new();

    let script = {
        let cancel = cancel.clone();
        async move {
            let idle = read_line(&mut server).await;
            let tag = tag_of(&idle).to_owned();
            respond(&mut server, "+ idling\r\n").await;
            cancel.cancel();
            assert_eq!(read_line(&mut server).await, "DONE\r\n");
            respond(&mut server, &format!("{tag} OK IDLE terminated\r\n")).await;
            server
        }
    };
    let idle = conn.idle(Duration::from_secs(5), Duration::from_secs(5), cancel);
    let (event, _server) = tokio::join!(idle, script);
    assert!(matches!(event, Ok(IdleEvent::Cancelled)), "got {event:?}");
    assert!(
        conn.is_reusable(),
        "a completed DONE leaves the connection reusable"
    );
}

// ===========================================================================
// One command path: live session legality, pending effects, write evidence
// ===========================================================================

/// Live session legality is a property of every command, not an APPEND
/// special case: a NAMESPACE queued behind an UNAUTHENTICATE that completes
/// first is refused at the head of the queue, before a byte is written, and
/// the refusal says the state moved rather than blaming the caller.
#[tokio::test(start_paused = true)]
async fn a_queued_command_is_refused_when_the_session_left_its_legal_states() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 UNAUTHENTICATE NAMESPACE",
    ))
    .await;
    let conn = std::sync::Arc::new(conn);
    let (unauth_seen_tx, unauth_seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let script = tokio::spawn(async move {
        let unauth = read_line(&mut server).await;
        assert!(unauth.ends_with(" UNAUTHENTICATE\r\n"), "{unauth:?}");
        let tag = tag_of(&unauth).to_owned();
        unauth_seen_tx.send(()).expect("test is waiting");
        release_rx.await.expect("test releases the reply");
        respond(&mut server, &format!("{tag} OK Unauthenticated\r\n")).await;

        // The capability refresh `unauthenticate` issues is next. The queued
        // NAMESPACE sat ahead of it, so had it been written it would be here.
        let next = read_line(&mut server).await;
        assert!(
            next.ends_with(" CAPABILITY\r\n"),
            "the queued NAMESPACE must not have been written; got {next:?}"
        );
        let tag = tag_of(&next).to_owned();
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 NAMESPACE\r\n{tag} OK done\r\n"),
        )
        .await;
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut rest)
            .await
            .expect("duplex read");
        let rest = String::from_utf8_lossy(&rest);
        assert!(
            !rest.contains("NAMESPACE"),
            "NAMESPACE was written: {rest:?}"
        );
    });

    let first = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.unauthenticate(Duration::from_secs(5)).await }
    });
    unauth_seen_rx.await.expect("server saw UNAUTHENTICATE");
    let namespace = tokio::spawn({
        let conn = std::sync::Arc::clone(&conn);
        async move { conn.namespace(Duration::from_secs(5)).await }
    });
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    release_tx.send(()).expect("server is waiting");

    first
        .await
        .expect("unauthenticate task")
        .expect("UNAUTHENTICATE completes");
    let err = namespace
        .await
        .expect("namespace task")
        .expect_err("the session is no longer legal for NAMESPACE");
    assert!(
        matches!(err, Error::StateChangedBeforeSend(_)),
        "got {err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent)
    );
    drop(conn);
    script.await.expect("transcript");
}

/// A SELECT refused before the first byte arms nothing. The pending-selection
/// flag used to be set before encoding, so an encoder refusal left it behind
/// and the NEXT command's tagged OK completed a selection the server never
/// made, flipping the session to Selected.
#[tokio::test(start_paused = true)]
async fn a_refused_select_leaves_no_pending_selection_behind() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 CONDSTORE QRESYNC",
    ))
    .await;
    let script = tokio::spawn(async move {
        let next = read_line(&mut server).await;
        assert!(
            next.ends_with(" NOOP\r\n"),
            "the refused SELECT must not reach the wire; got {next:?}"
        );
        let tag = tag_of(&next).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    // RFC 3501 Section 9: a QRESYNC uidvalidity is nz-number, so the encoder
    // refuses 0 before writing anything.
    let refused = conn
        .submit_regular(
            Command::Select {
                mailbox: MailboxName::new("INBOX").unwrap(),
                condstore: false,
                qresync: Some(QresyncParams {
                    uid_validity: 0,
                    mod_seq: 1,
                    known_uids: None,
                    seq_match_data: None,
                }),
            },
            super::dispatch::TaggedOkConsumer::default(),
            None,
        )
        .await
        .expect_err("a zero uidvalidity is refused");
    assert!(matches!(refused, Error::InvalidInput(_)), "{refused:?}");

    conn.noop(Duration::from_secs(5)).await.expect("NOOP");
    assert_eq!(
        conn.session_state(),
        SessionState::Authenticated,
        "a refused SELECT must not leave a selection for the next OK to complete"
    );
    let _server = script.await.expect("NOOP transcript");
}

/// Transmission evidence comes from socket progress, not from which write
/// failed. Here the APPEND is one segment (LITERAL+), its first write is
/// accepted, and the peer vanishes while the body is still going out: bytes of
/// the command are on the wire, so the failure is `InFlight` and a
/// non-idempotent APPEND goes to reconcile. Stamping by segment position
/// called this `Unsent` and invited a blind retry that could duplicate it.
#[tokio::test(start_paused = true)]
async fn a_write_that_reached_the_socket_before_failing_is_in_flight() {
    let (conn, mut server) = crate::connection::test_support::driver_pair_with_capacity(
        &preauth_greeting("IMAP4rev1 LITERAL+"),
        64,
    )
    .await;
    let script = tokio::spawn(async move {
        // Take the command line, then vanish with the body still unsent.
        let line = read_line(&mut server).await;
        assert!(line.contains(" APPEND "), "{line:?}");
        drop(server);
    });

    let err = conn
        .append("INBOX", &[], None, &[b'x'; 4096], Duration::from_secs(5))
        .await
        .expect_err("the peer is gone mid-body");
    script.await.expect("transcript");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "bytes of the APPEND reached the socket; got {err:?}"
    );
}

/// The other half of the same rule: a write that fails with no octet of the
/// command accepted by the socket is `Unsent`, so recovery may simply retry.
/// A peer that closed before the command is the stale-pool case.
#[tokio::test(start_paused = true)]
async fn a_write_that_reached_nothing_is_unsent() {
    let (conn, server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 LITERAL+")).await;
    drop(server);

    let err = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .expect_err("the peer is gone");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent),
        "no octet reached the socket; got {err:?}"
    );
}

/// The driver refuses a batch holding a command that may not be pipelined,
/// before writing a byte of the batch, and the connection stays usable. Only
/// a crate bug can build such a batch, so it is reached here directly.
#[tokio::test(start_paused = true)]
async fn the_driver_refuses_a_batch_holding_a_command_that_cannot_be_pipelined() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let script = tokio::spawn(async move {
        let next = read_line(&mut server).await;
        assert!(
            next.ends_with(" NOOP\r\n"),
            "no byte of the refused batch may reach the wire; got {next:?}"
        );
        let tag = tag_of(&next).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let (reply, mut pending) = super::reply::reply_channel(
        (
            vec![
                Command::Noop,
                Command::Append {
                    mailbox: "INBOX".into(),
                    messages: vec![AppendMessage::new(b"HELLO".to_vec())],
                    multi: false,
                },
            ],
            vec![
                Box::new(super::dispatch::TaggedOkConsumer::default())
                    as Box<dyn driver::ConsumerErased>,
                Box::new(super::dispatch::AppendConsumer::default()),
            ],
        ),
        &conn.abandoned,
    );
    conn.cmd_tx
        .send(driver::DriverCommand::Pipeline { reply })
        .await
        .expect("driver is running");
    let Err(err) = pending.recv().await.expect("driver answers") else {
        panic!("the batch must be refused");
    };
    assert!(matches!(err, Error::Internal(_)), "got {err:?}");

    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection survives the refusal");
    let _server = script.await.expect("NOOP transcript");
}

/// A pipeline is admitted when it executes: a Selected-only command in an
/// Authenticated session is caller sequencing, refused before anything is
/// submitted.
#[tokio::test]
async fn a_pipeline_is_admitted_against_the_session_state_at_execution() {
    let conn = crate::connection::test_support::detached(
        SessionState::Authenticated,
        vec![Capability::Imap4Rev1],
        &[],
    );
    let err = conn
        .pipeline()
        .noop()
        .uid_search("ALL".to_owned())
        .execute_dynamic()
        .await
        .expect_err("UID SEARCH needs a selected mailbox");
    assert!(
        matches!(
            err,
            super::pipeline::PipelineError::Driver(Error::InvalidState(_))
        ),
        "got {err:?}"
    );
}

/// IDLE is admitted on the handle like every other command: outside
/// Authenticated/Selected it is caller sequencing, refused before submission.
#[tokio::test]
async fn idle_is_admitted_against_the_session_state() {
    let conn = crate::connection::test_support::detached(
        SessionState::NotAuthenticated,
        vec![Capability::Imap4Rev1, Capability::Idle],
        &[],
    );
    let err = conn
        .idle(
            Duration::from_secs(1),
            Duration::from_secs(1),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("IDLE is not legal before authentication");
    assert!(matches!(err, Error::InvalidState(_)), "got {err:?}");
}

mod withdrawal {
    use super::*;
    use crate::connection::test_support::{driver_pair, select_inbox};
    use bifrost_types::TransmissionState::{InFlight, Unsent};
    use std::sync::Arc;
    use tokio::task::JoinHandle;

    const LONG: Duration = Duration::from_secs(60);
    const SHORT: Duration = Duration::from_secs(1);

    async fn pair(caps: &str) -> (Arc<ImapConnection>, tokio::io::DuplexStream) {
        let (conn, server) = driver_pair(&preauth_greeting(caps)).await;
        (Arc::new(conn), server)
    }
    async fn head(
        conn: &Arc<ImapConnection>,
        server: &mut tokio::io::DuplexStream,
    ) -> (JoinHandle<Result<(), Error>>, String) {
        let conn = Arc::clone(conn);
        let task = tokio::spawn(async move { conn.noop(LONG).await });
        let line = read_line(server).await;
        assert!(line.contains(" NOOP"));
        (task, tag_of(&line).to_owned())
    }
    /// Channel capacity with exactly one command buffered (the head command,
    /// once claimed, no longer occupies a slot).
    fn one_queued(conn: &ImapConnection) -> usize {
        conn.cmd_tx.max_capacity() - 1
    }
    async fn buffered(conn: &ImapConnection, capacity: usize) {
        // Yielding remains runnable, so paused time cannot jump to the head's deadline.
        for _ in 0..100 {
            if conn.cmd_tx.capacity() == capacity {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "command was not buffered: expected {capacity}, got {}",
            conn.cmd_tx.capacity()
        );
    }
    fn append_task(
        conn: &Arc<ImapConnection>,
        timeout: Duration,
    ) -> JoinHandle<Result<Option<(u32, u32)>, Error>> {
        let conn = Arc::clone(conn);
        tokio::spawn(async move { conn.append("INBOX", &[], None, b"hello", timeout).await })
    }
    async fn next_capability(
        conn: &Arc<ImapConnection>,
        server: &mut tokio::io::DuplexStream,
        a: JoinHandle<Result<(), Error>>,
        tag: &str,
    ) {
        respond(server, &format!("{tag} OK done\r\n")).await;
        a.await.unwrap().unwrap();
        let conn = Arc::clone(conn);
        let c = tokio::spawn(async move { conn.capability(LONG).await });
        let line = read_line(server).await;
        assert!(
            line.contains(" CAPABILITY"),
            "withdrawn command reached wire: {line:?}"
        );
        respond(
            server,
            &format!("* CAPABILITY IMAP4rev1\r\n{} OK done\r\n", tag_of(&line)),
        )
        .await;
        c.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_queued_command_whose_caller_timed_out_is_never_written() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        let (a, tag) = head(&conn, &mut server).await;
        let b = append_task(&conn, SHORT);
        buffered(&conn, one_queued(&conn)).await;
        let err = b.await.unwrap().unwrap_err();
        assert!(matches!(err, Error::Timeout { .. }));
        assert_eq!(err.attempt(), Some(Unsent));
        assert!(conn.is_reusable());
        next_capability(&conn, &mut server, a, &tag).await;
    }
    #[tokio::test(start_paused = true)]
    async fn dropping_a_queued_command_withdraws_it_and_keeps_the_connection_reusable() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        let (a, tag) = head(&conn, &mut server).await;
        let mut b = Box::pin(conn.append("INBOX", &[], None, b"hello", LONG));
        assert!(futures::poll!(&mut b).is_pending());
        assert_eq!(conn.cmd_tx.capacity(), one_queued(&conn));
        drop(b);
        assert!(conn.is_reusable());
        next_capability(&conn, &mut server, a, &tag).await;
    }
    #[tokio::test(start_paused = true)]
    async fn dropping_a_queued_streaming_fetch_writes_nothing_and_ends_its_item_channel() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        select_inbox(&conn, &mut server).await;
        let (a, tag) = head(&conn, &mut server).await;
        let sequence_set = SequenceSet::new("1:*").unwrap();
        let (mut rx, fut) = conn
            .uid_fetch_stream(&sequence_set, &[FetchAttr::Uid], LONG)
            .unwrap();
        let mut fut = Box::pin(fut);
        assert!(futures::poll!(&mut fut).is_pending());
        assert_eq!(conn.cmd_tx.capacity(), one_queued(&conn));
        drop(fut);
        assert!(conn.is_reusable());
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
        next_capability(&conn, &mut server, a, &tag).await;
    }
    #[tokio::test(start_paused = true)]
    async fn a_claimed_command_whose_caller_timed_out_is_in_flight_and_abandoned() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        let b = append_task(&conn, SHORT);
        assert!(read_line(&mut server).await.contains(" APPEND"));
        let err = b.await.unwrap().unwrap_err();
        assert!(matches!(err, Error::Timeout { .. }));
        assert_eq!(err.attempt(), Some(InFlight));
        assert!(!conn.is_reusable());
    }
    #[tokio::test(start_paused = true)]
    async fn a_command_queued_behind_a_connection_fatal_failure_is_unsent() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        let (a, _) = head(&conn, &mut server).await;
        let b = append_task(&conn, LONG);
        buffered(&conn, one_queued(&conn)).await;
        // No ALERT: it is published before BYE, changing the IDLE twin's departure.
        respond(&mut server, "* BYE going away\r\n").await;
        assert_eq!(b.await.unwrap().unwrap_err().attempt(), Some(Unsent));
        assert!(a.await.unwrap().is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn a_claimed_command_whose_peer_vanishes_is_in_flight() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        let b = append_task(&conn, LONG);
        assert!(read_line(&mut server).await.contains(" APPEND"));
        drop(server);
        assert_eq!(b.await.unwrap().unwrap_err().attempt(), Some(InFlight));
    }
    #[tokio::test(start_paused = true)]
    async fn a_claimed_command_whose_driver_is_aborted_unanswered_is_in_flight() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        let b = append_task(&conn, LONG);
        assert!(read_line(&mut server).await.contains(" APPEND"));
        conn.terminate().await;
        let err = b.await.unwrap().unwrap_err();
        assert!(matches!(
            err,
            Error::DriverGone { .. } | Error::DriverPanicked { .. }
        ));
        assert_eq!(err.attempt(), Some(InFlight));
    }
    async fn fill(conn: &Arc<ImapConnection>) -> Vec<JoinHandle<Result<(), Error>>> {
        let mut tasks = Vec::new();
        for capacity in (0..conn.cmd_tx.max_capacity()).rev() {
            let conn2 = Arc::clone(conn);
            tasks.push(tokio::spawn(async move { conn2.noop(LONG).await }));
            buffered(conn, capacity).await;
        }
        tasks
    }
    #[tokio::test(start_paused = true)]
    async fn a_submission_that_times_out_before_channel_admission_is_unsent() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        let (a, _) = head(&conn, &mut server).await;
        let fillers = fill(&conn).await;
        let err = conn.noop(SHORT).await.unwrap_err();
        assert_eq!(err.attempt(), Some(Unsent));
        assert!(matches!(err, Error::Timeout { .. }));
        assert!(conn.is_reusable());
        conn.terminate().await;
        let _ = a.await;
        for filler in fillers {
            let _ = filler.await;
        }
    }
    #[tokio::test(start_paused = true)]
    async fn a_zero_timeout_submits_nothing_and_is_unsent() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        let capacity = conn.cmd_tx.capacity();
        let err = conn.noop(Duration::ZERO).await.unwrap_err();
        assert_eq!(conn.cmd_tx.capacity(), capacity);
        assert!(matches!(err, Error::Timeout { .. }));
        assert_eq!(err.attempt(), Some(Unsent));
        let conn2 = Arc::clone(&conn);
        let c = tokio::spawn(async move { conn2.capability(LONG).await });
        let line = read_line(&mut server).await;
        assert!(line.contains(" CAPABILITY"));
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1\r\n{} OK done\r\n", tag_of(&line)),
        )
        .await;
        c.await.unwrap().unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn a_move_fallback_whose_store_is_withdrawn_keeps_copy_and_unsent_leaf() {
        let (conn, mut server) = pair("IMAP4rev1 UIDPLUS").await;
        select_inbox(&conn, &mut server).await;
        let conn2 = Arc::clone(&conn);
        let b = tokio::spawn(async move {
            conn2
                .uid_move_messages(&SequenceSet::new("1:*").unwrap(), "Archive", SHORT)
                .await
        });
        let copy = read_line(&mut server).await;
        assert!(copy.contains(" UID COPY"));
        let conn2 = Arc::clone(&conn);
        let a = tokio::spawn(async move { conn2.noop(LONG).await });
        buffered(&conn, one_queued(&conn)).await;
        respond(
            &mut server,
            &format!("{} OK [COPYUID 1 1 2] copied\r\n", tag_of(&copy)),
        )
        .await;
        let noop = read_line(&mut server).await;
        assert!(noop.contains(" NOOP"));
        buffered(&conn, one_queued(&conn)).await;
        let err = b.await.unwrap().unwrap_err();
        assert!(
            matches!(&err, Error::MoveFallbackIncomplete {stored_deleted:false,source,..} if matches!(source.as_ref(),Error::Timeout {..}))
        );
        assert_eq!(err.attempt(), Some(Unsent));
        assert!(conn.is_reusable());
        next_capability(&conn, &mut server, a, tag_of(&noop)).await;
    }
    #[tokio::test(start_paused = true)]
    async fn an_idle_cancelled_before_its_claim_is_never_written() {
        let (conn, mut server) = pair("IMAP4rev1 IDLE").await;
        let (a, tag) = head(&conn, &mut server).await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let mut b = Box::pin(conn.idle(LONG, SHORT, cancel.clone()));
        assert!(futures::poll!(&mut b).is_pending());
        assert_eq!(conn.cmd_tx.capacity(), one_queued(&conn));
        cancel.cancel();
        assert!(matches!(b.await.unwrap(), IdleEvent::Cancelled));
        assert!(conn.is_reusable());
        next_capability(&conn, &mut server, a, &tag).await;
    }
    #[tokio::test(start_paused = true)]
    async fn an_idle_cancelled_while_waiting_for_channel_capacity_returns_cancelled() {
        let (conn, mut server) = pair("IMAP4rev1 IDLE").await;
        let (a, _) = head(&conn, &mut server).await;
        let fillers = fill(&conn).await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let mut b = Box::pin(conn.idle(LONG, SHORT, cancel.clone()));
        assert!(futures::poll!(&mut b).is_pending());
        cancel.cancel();
        assert!(matches!(
            tokio::time::timeout(SHORT, b).await.unwrap().unwrap(),
            IdleEvent::Cancelled
        ));
        assert!(conn.is_reusable());
        conn.terminate().await;
        let _ = a.await;
        for filler in fillers {
            let _ = filler.await;
        }
    }
    #[tokio::test(start_paused = true)]
    async fn an_idle_leaving_on_another_commands_alert_before_its_claim_is_never_written() {
        let (conn, mut server) = pair("IMAP4rev1 IDLE").await;
        let (a, tag) = head(&conn, &mut server).await;
        let mut b = Box::pin(conn.idle(LONG, SHORT, tokio_util::sync::CancellationToken::new()));
        assert!(futures::poll!(&mut b).is_pending());
        assert_eq!(conn.cmd_tx.capacity(), one_queued(&conn));
        respond(&mut server, "* OK [ALERT] maintenance\r\n").await;
        assert!(matches!(b.await.unwrap(), IdleEvent::Alert(_)));
        assert!(conn.is_reusable());
        next_capability(&conn, &mut server, a, &tag).await;
    }
    #[tokio::test(start_paused = true)]
    async fn an_idle_queued_behind_a_connection_fatal_failure_is_unsent() {
        let (conn, mut server) = pair("IMAP4rev1 IDLE").await;
        let (a, _) = head(&conn, &mut server).await;
        let mut b = Box::pin(conn.idle(LONG, SHORT, tokio_util::sync::CancellationToken::new()));
        assert!(futures::poll!(&mut b).is_pending());
        assert_eq!(conn.cmd_tx.capacity(), one_queued(&conn));
        // No ALERT: it would publish an event before the fatal BYE short-circuit.
        respond(&mut server, "* BYE going away\r\n").await;
        assert_eq!(b.await.unwrap_err().attempt(), Some(Unsent));
        assert!(a.await.unwrap().is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn an_idle_with_zero_timeout_submits_nothing() {
        let (conn, mut server) = pair("IMAP4rev1 IDLE").await;
        let capacity = conn.cmd_tx.capacity();
        assert!(matches!(
            conn.idle(
                Duration::ZERO,
                SHORT,
                tokio_util::sync::CancellationToken::new()
            )
            .await
            .unwrap(),
            IdleEvent::Timeout
        ));
        assert_eq!(conn.cmd_tx.capacity(), capacity);
        let conn2 = Arc::clone(&conn);
        let c = tokio::spawn(async move { conn2.capability(LONG).await });
        let line = read_line(&mut server).await;
        assert!(line.contains(" CAPABILITY"));
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1\r\n{} OK done\r\n", tag_of(&line)),
        )
        .await;
        c.await.unwrap().unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn a_dropped_pipeline_queued_behind_a_command_is_never_written() {
        let (conn, mut server) = pair("IMAP4rev1").await;
        let (a, tag) = head(&conn, &mut server).await;
        let mut b = Box::pin(
            conn.pipeline()
                .noop()
                .list(String::new(), "*".into())
                .execute(),
        );
        assert!(futures::poll!(&mut b).is_pending());
        assert_eq!(conn.cmd_tx.capacity(), one_queued(&conn));
        drop(b);
        assert!(conn.is_reusable());
        next_capability(&conn, &mut server, a, &tag).await;
    }
    #[tokio::test(start_paused = true)]
    async fn a_compress_withdrawn_before_its_claim_is_never_written() {
        let (conn, mut server) = pair("IMAP4rev1 COMPRESS=DEFLATE").await;
        let (a, tag) = head(&conn, &mut server).await;
        let conn2 = Arc::clone(&conn);
        let b = tokio::spawn(async move { conn2.compress(SHORT).await });
        buffered(&conn, one_queued(&conn)).await;
        assert_eq!(b.await.unwrap().unwrap_err().attempt(), Some(Unsent));
        assert!(conn.is_reusable());
        next_capability(&conn, &mut server, a, &tag).await;
    }
}

struct ExitFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Drop for ExitFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn a_value_retained_by_a_running_driver_is_released_only_at_driver_exit() {
    let (conn, peer) =
        test_support::driver_pair(&test_support::preauth_greeting("IMAP4rev1")).await;
    let driver = conn.driver_task();
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    driver.retain_until_exit(Box::new(ExitFlag(Arc::clone(&flag))));
    assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
    drop(peer);
    driver.finish_or_abort(LOGOUT_DRAIN_TIMEOUT).await;
    assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
    assert!(driver.has_exited());
}

#[test]
fn a_value_retained_after_driver_exit_is_released_at_once() {
    let driver = DriverTask::absent();
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    driver.retain_until_exit(Box::new(ExitFlag(Arc::clone(&flag))));
    assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
    assert!(driver.has_exited());
}

#[tokio::test]
async fn aborting_a_driver_before_its_first_poll_releases_its_retained_values() {
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let driver = DriverTask::spawn(
        std::future::pending(),
        Arc::default(),
        vec![Box::new(ExitFlag(Arc::clone(&flag)))],
    );
    driver.abort();
    driver.abort_and_join().await;
    assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
    assert!(driver.has_exited());
}

struct ExitOrder(Arc<std::sync::Mutex<Vec<&'static str>>>, &'static str);
impl Drop for ExitOrder {
    fn drop(&mut self) {
        self.0.lock().unwrap().push(self.1);
    }
}

struct ExitOrderFuture {
    _guard: ExitOrder,
    pending: bool,
}
impl std::future::Future for ExitOrderFuture {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        if self.pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    }
}

#[tokio::test]
async fn the_driver_future_is_destroyed_before_its_retained_values() {
    for abort in [false, true] {
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let guard = ExitOrder(Arc::clone(&order), "driver");
        let driver = DriverTask::spawn(
            ExitOrderFuture {
                _guard: guard,
                pending: abort,
            },
            Arc::default(),
            vec![Box::new(ExitOrder(Arc::clone(&order), "lease"))],
        );
        if abort {
            driver.abort_and_join().await;
        } else {
            driver.finish_or_abort(LOGOUT_DRAIN_TIMEOUT).await;
        }
        assert_eq!(*order.lock().unwrap(), ["driver", "lease"]);
    }
}
