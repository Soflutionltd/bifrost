#![allow(clippy::unwrap_used, clippy::expect_used)]

use tokio::io::AsyncWriteExt;

use super::*;
use crate::connection::TcpKeepalive;
use crate::types::response::UntaggedResponse;

// ===========================================================================
// buffer_may_contain_complete_response
//
// This is the framing heuristic every read goes through: a false negative
// stalls the read loop waiting for bytes that already arrived, a false
// positive hands truncated bytes to the nom parser.
// ===========================================================================

/// Shims for the two fallible framing helpers. The cases below all expect a
/// decision rather than a framing error; the error lane has its own tests.
fn buffer_may_contain_complete_response(buf: &[u8]) -> bool {
    super::buffer_may_contain_complete_response(buf).expect("framing decision, not an error")
}

fn try_parse_literal_marker(buf: &[u8], crlf_pos: usize) -> Option<usize> {
    super::try_parse_literal_marker(buf, crlf_pos).expect("framing decision, not an error")
}

#[test]
fn framing_no_crlf_is_incomplete() {
    assert!(!buffer_may_contain_complete_response(b""));
    assert!(!buffer_may_contain_complete_response(b"* 3 EXIST"));
}

#[test]
fn framing_plain_untagged_is_complete() {
    assert!(buffer_may_contain_complete_response(b"* 3 EXISTS\r\n"));
    assert!(buffer_may_contain_complete_response(b"* 1 EXPUNGE\r\n"));
}

#[test]
fn framing_status_text_braces_are_not_literals() {
    // RFC 3501 Section 9: `resp-text` is 1*TEXT-CHAR, so `{42}` in an
    // untagged status line is text, not a literal marker.
    assert!(buffer_may_contain_complete_response(
        b"* OK [ALERT] quota limit {42}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"* BYE closing {7}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"* NO trouble {7}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"* BAD syntax {7}\r\n"
    ));
}

#[test]
fn framing_status_keyword_is_case_insensitive() {
    // RFC 3501 Section 9: alphabetic characters are case-insensitive; a
    // lowercasing server must not push us onto the literal path.
    assert!(buffer_may_contain_complete_response(
        b"* ok [ALERT] limit {42}\r\n"
    ));
}

#[test]
fn framing_tagged_status_braces_are_not_literals() {
    assert!(buffer_may_contain_complete_response(
        b"A001 OK done {42}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"A001 NO nope {42}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"A001 BAD bad {42}\r\n"
    ));
}

#[test]
fn framing_tagged_status_without_resp_text() {
    assert!(buffer_may_contain_complete_response(b"A001 OK\r\n"));
    assert!(buffer_may_contain_complete_response(b"A001 NO\r\n"));
    assert!(buffer_may_contain_complete_response(b"A001 BAD\r\n"));
}

#[test]
fn framing_continuation_is_complete() {
    // RFC 3501 Section 7.5: continuation requests carry no literal.
    assert!(buffer_may_contain_complete_response(b"+ ready for {5}\r\n"));
    assert!(buffer_may_contain_complete_response(b"+ \r\n"));
}

#[test]
fn framing_fetch_literal_body_missing_is_incomplete() {
    assert!(!buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[] {5}\r\nAB"
    ));
}

#[test]
fn framing_fetch_literal_body_complete() {
    assert!(buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[] {5}\r\nHELLO)\r\n"
    ));
}

#[test]
fn framing_fetch_multiple_literals() {
    assert!(buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[HEADER] {5}\r\nHELLO BODY[TEXT] {5}\r\nWORLD)\r\n"
    ));
    assert!(!buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[HEADER] {5}\r\nHELLO BODY[TEXT] {5}\r\nWOR"
    ));
}

#[test]
fn framing_non_synchronizing_literal_marker_is_honored() {
    // RFC 7888: `{N+}` on the server side still declares N octets.
    assert!(buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[] {5+}\r\nHELLO)\r\n"
    ));
    assert!(!buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[] {5+}\r\nHEL"
    ));
}

#[test]
fn framing_quoted_brace_digits_are_not_a_literal_marker() {
    let buf = b"* LIST (\\HasNoChildren) \"/\" \"Order {12}\"\r\n";
    assert!(buffer_may_contain_complete_response(buf));
}

#[test]
fn framing_brace_digits_not_before_crlf_still_completes_when_more_follows() {
    // Same shape as the bug above, but with a following tagged line the
    // skip lands inside it and the scan recovers. This is why the defect
    // usually presents as a stall only on the last buffered response.
    let buf = b"* LIST () \"/\" \"Order {2}\"\r\nA001 OK LIST completed\r\n";
    assert!(buffer_may_contain_complete_response(buf));
}

#[test]
fn framing_unparenthesized_untagged_without_space_prefix() {
    // Not `* ` and not tagged-status: the conservative literal path.
    assert!(buffer_may_contain_complete_response(b"garbage\r\n"));
}

// ---------------------------------------------------------------------------
// try_parse_literal_marker
// ---------------------------------------------------------------------------

#[test]
fn marker_parses_plain_and_plus_forms() {
    // `crlf_pos` is the index of the `\r`.
    assert_eq!(try_parse_literal_marker(b"* X {12}\r\n", 8), Some(12));
    assert_eq!(try_parse_literal_marker(b"* X {12+}\r\n", 9), Some(12));
}

#[test]
fn marker_rejects_non_digits() {
    assert_eq!(try_parse_literal_marker(b"* X {ab}\r\n", 8), None);
    assert_eq!(try_parse_literal_marker(b"* X {}\r\n", 6), None);
    assert_eq!(try_parse_literal_marker(b"* X nothing\r\n", 11), None);
}

#[test]
fn marker_uses_the_last_open_brace() {
    // rposition, so an earlier decorative brace does not shadow the real
    // marker.
    assert_eq!(try_parse_literal_marker(b"* X {a} {7}\r\n", 11), Some(7));
}

/// A count that cannot be delivered must be fatal, not "wait for more".
/// Both values are out of range on every target width  -  `u64::MAX` and one
/// past the RFC 9051 `number64` ceiling  -  so the assertion does not depend
/// on the pointer size.
#[test]
fn framing_unreachable_literal_size_is_a_parse_error_not_a_stall() {
    for marker in [
        &b"* 1 FETCH (BODY[] {18446744073709551615}\r\n"[..],
        &b"* 1 FETCH (BODY[] {9223372036854775808}\r\n"[..],
    ] {
        assert!(
            matches!(
                super::buffer_may_contain_complete_response(marker),
                Err(Error::Parse(_))
            ),
            "expected a fatal framing error for {}",
            String::from_utf8_lossy(marker)
        );
    }
    // A digit run too long for `u64` is not a marker at all, so framing hands
    // the line to the decoder, which rejects it. Fatal either way; what must
    // never happen is `Ok(false)`.
    assert!(matches!(
        super::buffer_may_contain_complete_response(
            b"* 1 FETCH (BODY[] {99999999999999999999}\r\n"
        ),
        Ok(true)
    ));
}

/// The largest count this target can actually address stays on the ordinary
/// "need more bytes" path: a real server may legitimately send a big literal.
#[test]
fn framing_addressable_literal_size_is_merely_incomplete() {
    let biggest = u64::try_from(usize::MAX)
        .unwrap_or(u64::MAX)
        .min(i64::MAX as u64);
    let marker = format!("* 1 FETCH (BODY[] {{{biggest}}}\r\n");
    assert!(!buffer_may_contain_complete_response(marker.as_bytes()));
}

// ===========================================================================
// WireReader over an in-memory duplex
// ===========================================================================

fn memory_reader() -> (WireReader, tokio::io::DuplexStream) {
    let (client, server) = tokio::io::duplex(1 << 16);
    (
        WireReader::new(ImapStream::Memory(super::super::stream::Tracked::new(
            client,
        ))),
        server,
    )
}

#[tokio::test]
async fn read_greeting_parses_ok_greeting() {
    let (mut reader, mut server) = memory_reader();
    server
        .write_all(b"* OK [CAPABILITY IMAP4rev1] ready\r\n")
        .await
        .unwrap();
    server.flush().await.unwrap();

    let resp = reader.read_greeting().await.unwrap();
    match resp {
        Response::Greeting(g) => {
            assert!(matches!(
                g.status,
                crate::types::response::GreetingStatus::Ok
            ));
        }
        other => panic!("expected greeting, got {other:?}"),
    }
    assert!(reader.buffer_is_empty());
}

#[tokio::test]
async fn wire_metering_accumulates_inbound_protocol_octets() {
    let (mut reader, mut server) = memory_reader();
    let counter = reader.metering().inbound_counter();
    let greeting = b"* OK ready\r\n";
    server.write_all(greeting).await.unwrap();

    reader.read_greeting().await.expect("valid greeting");

    assert_eq!(
        counter.load(std::sync::atomic::Ordering::Relaxed),
        greeting.len() as u64
    );
}

/// The whole point of making the framing helper fallible: `read_one` must
/// return, not park in its read loop, when the declared literal can never
/// arrive.
#[tokio::test]
async fn read_one_fails_fast_on_an_undeliverable_literal_count() {
    let (mut reader, mut server) = memory_reader();
    server
        .write_all(b"* 1 FETCH (BODY[] {9223372036854775808}\r\n")
        .await
        .unwrap();
    server.flush().await.unwrap();

    let err = tokio::time::timeout(std::time::Duration::from_secs(5), reader.read_one(false))
        .await
        .expect("read_one must not wait for octets that cannot exist")
        .expect_err("an unreachable literal count is fatal");
    assert!(matches!(err, Error::Parse(_)), "got {err:?}");
    let _server = server;
}

#[tokio::test]
async fn read_one_reassembles_a_response_split_across_writes() {
    let (mut reader, mut server) = memory_reader();

    let feeder = tokio::spawn(async move {
        // Deliberately split mid-token so the CRLF heuristic has to hold
        // the partial line back.
        server.write_all(b"* 3 EXI").await.unwrap();
        server.flush().await.unwrap();
        tokio::task::yield_now().await;
        server.write_all(b"STS\r\n").await.unwrap();
        server.flush().await.unwrap();
        server
    });

    let resp = reader.read_one(false).await.unwrap();
    let _server = feeder.await.unwrap();

    match resp {
        Response::Untagged(u) => assert!(matches!(*u, UntaggedResponse::Exists(3))),
        other => panic!("expected untagged EXISTS, got {other:?}"),
    }
}

#[tokio::test]
async fn read_one_reassembles_a_literal_split_across_writes() {
    let (mut reader, mut server) = memory_reader();

    let feeder = tokio::spawn(async move {
        server
            .write_all(b"* 1 FETCH (UID 7 BODY[TEXT] {11}\r\nHELLO")
            .await
            .unwrap();
        server.flush().await.unwrap();
        tokio::task::yield_now().await;
        server.write_all(b" WORLD)\r\n").await.unwrap();
        server.flush().await.unwrap();
        server
    });

    let resp = reader.read_one(false).await.unwrap();
    let _server = feeder.await.unwrap();

    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::Fetch(fetch) => {
                assert_eq!(fetch.uid, Some(7));
                assert_eq!(fetch.body_sections.len(), 1);
                assert_eq!(
                    fetch.body_sections[0].data.as_deref(),
                    Some(&b"HELLO WORLD"[..])
                );
            }
            other => panic!("expected FETCH, got {other:?}"),
        },
        other => panic!("expected untagged, got {other:?}"),
    }
}

#[tokio::test]
async fn read_one_returns_two_responses_from_one_segment() {
    let (mut reader, mut server) = memory_reader();
    server
        .write_all(b"* 3 EXISTS\r\nA001 OK NOOP completed\r\n")
        .await
        .unwrap();
    server.flush().await.unwrap();

    let first = reader.read_one(false).await.unwrap();
    assert!(matches!(first, Response::Untagged(_)));
    assert!(!reader.buffer_is_empty(), "tagged line stays buffered");

    let second = reader.read_one(false).await.unwrap();
    match second {
        Response::Tagged(t) => assert_eq!(t.tag, "A001"),
        other => panic!("expected tagged, got {other:?}"),
    }
    assert!(reader.buffer_is_empty());
}

#[tokio::test]
async fn read_one_reports_eof_as_closed() {
    let (mut reader, server) = memory_reader();
    drop(server);
    match reader.read_one(false).await {
        Err(Error::Closed { .. }) => {}
        other => panic!("expected Closed on EOF, got {other:?}"),
    }
}

#[tokio::test]
async fn read_one_reports_a_hard_parse_error() {
    let (mut reader, mut server) = memory_reader();
    // Terminated with CRLF so the incomplete-data heuristic cannot
    // swallow it, but not a valid response.
    server.write_all(b"%%%\r\n").await.unwrap();
    server.flush().await.unwrap();
    match reader.read_one(false).await {
        Err(Error::Parse(msg)) => assert!(msg.contains("parse error"), "unexpected text: {msg}"),
        other => panic!("expected Parse, got {other:?}"),
    }
}

#[tokio::test]
async fn read_one_rejects_a_malformed_recognized_fetch_attribute() {
    for response in [
        &b"* 1 FETCH (UID 0)\r\n"[..],
        b"* 1 FETCH (BODYSTRUCTURE (\"TEXT\"))\r\n",
    ] {
        let (mut reader, mut server) = memory_reader();
        server.write_all(response).await.unwrap();
        server.flush().await.unwrap();
        assert!(matches!(reader.read_one(false).await, Err(Error::Parse(_))));
    }
}

#[tokio::test]
async fn write_all_reaches_the_peer() {
    use tokio::io::AsyncReadExt;

    let (mut reader, mut server) = memory_reader();
    reader.write_all(b"A001 NOOP\r\n").await.unwrap();

    let mut buf = vec![0u8; 11];
    server.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf[..], b"A001 NOOP\r\n");
}

#[tokio::test]
async fn take_buffer_hands_over_unparsed_bytes() {
    // RFC 4978 Section 3: on COMPRESS the post-OK bytes already read are
    // compressed data that must survive the stream swap.
    let (mut reader, mut server) = memory_reader();
    server
        .write_all(b"A001 OK COMPRESS active\r\nLEFTOVER")
        .await
        .unwrap();
    server.flush().await.unwrap();

    let resp = reader.read_one(false).await.unwrap();
    assert!(matches!(resp, Response::Tagged(_)));
    assert_eq!(&reader.take_buffer()[..], b"LEFTOVER");
    assert!(reader.buffer_is_empty());
}

#[tokio::test]
async fn keepalive_and_peer_certificate_are_unavailable_on_memory_streams() {
    let (reader, _server) = memory_reader();
    assert!(reader.peer_certificate_der().is_none());
    let ka = TcpKeepalive::new(
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    );
    assert!(reader.set_keepalive(&ka).is_err());
}

fn bandwidth_reader(budget: bifrost_net::BandwidthBudget) -> (WireReader, tokio::io::DuplexStream) {
    let (client, peer) = tokio::io::duplex(64);
    (
        WireReader::new_metered(
            ImapStream::Memory(super::super::stream::Tracked::new(client)),
            None,
            Some(budget),
        ),
        peer,
    )
}

#[tokio::test(start_paused = true)]
async fn bandwidth_reads_are_bounded_and_redial_keeps_the_schedule() {
    use tokio::time::{Duration, Instant};
    let budget = bifrost_net::BandwidthBudget::new(Some(4));
    let (mut reader, mut peer) = bandwidth_reader(budget.clone());
    peer.write_all(b"abcdefgh").await.unwrap();
    assert_eq!(reader.read_admitted().await.unwrap(), 4);
    assert_eq!(&reader.buf[..], b"abcd");
    drop(reader);
    let (mut redial, mut peer) = bandwidth_reader(budget);
    peer.write_all(b"ijklmnop").await.unwrap();
    let start = Instant::now();
    assert_eq!(redial.read_admitted().await.unwrap(), 4);
    assert_eq!(start.elapsed(), Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn bandwidth_pending_idle_read_does_not_hold_a_turn() {
    use futures::FutureExt;
    use tokio::time::{Duration, Instant};
    let budget = bifrost_net::BandwidthBudget::new(Some(4));
    let (mut idle, _silent_peer) = bandwidth_reader(budget.clone());
    let mut pending = Box::pin(idle.read_one(false));
    assert!((&mut pending).now_or_never().is_none());
    let (mut active, mut peer) = bandwidth_reader(budget);
    peer.write_all(b"abcd").await.unwrap();
    let start = Instant::now();
    assert_eq!(active.read_admitted().await.unwrap(), 4);
    assert_eq!(start.elapsed(), Duration::from_secs(1));
    assert!((&mut pending).now_or_never().is_none());
}

#[tokio::test(start_paused = true)]
async fn bandwidth_peer_timeout_starts_after_admission() {
    use tokio::time::{Duration, Instant};
    let budget = bifrost_net::BandwidthBudget::new(Some(4));
    budget
        .admit(bifrost_net::BandwidthDirection::Inbound, 4)
        .await
        .finish(4);
    let (mut reader, mut peer) = bandwidth_reader(budget);
    reader.set_peer_timeout(Some(Duration::from_millis(10)));
    peer.write_all(b"abcd").await.unwrap();
    let start = Instant::now();
    assert_eq!(reader.read_admitted().await.unwrap(), 4);
    assert_eq!(start.elapsed(), Duration::from_secs(1));
    assert!(matches!(
        reader.read_admitted().await,
        Err(Error::Timeout { .. })
    ));
    assert_eq!(start.elapsed(), Duration::from_millis(2010));
}

#[tokio::test(start_paused = true)]
async fn setup_peer_budget_keeps_elapsed_wait_when_the_reader_is_rebuilt() {
    use tokio::time::{Duration, Instant, advance};

    let timeout = Duration::from_millis(100);
    let budget = PeerBudget::new(timeout);
    let (client, _peer) = tokio::io::duplex(64);
    let mut first = WireReader::new(ImapStream::Memory(super::super::stream::Tracked::new(
        client,
    )));
    first.set_peer_budget(Some(budget.clone()));

    let mut first_wait = Box::pin(peer_io(
        first.peer_budget(),
        std::future::pending::<std::io::Result<()>>(),
    ));
    tokio::select! {
        biased;
        result = &mut first_wait => panic!("peer budget future completed unexpectedly: {result:?}"),
        () = async {} => {}
    }
    advance(Duration::from_millis(60)).await;
    drop(first_wait);

    // STARTTLS installs a fresh reader from cloned metering state. Setup
    // explicitly carries the same budget into each phase as well.
    let metering = first.metering();
    let (client, _peer) = tokio::io::duplex(64);
    let mut next = WireReader::with_metering(
        ImapStream::Memory(super::super::stream::Tracked::new(client)),
        metering,
    );
    next.set_peer_budget(Some(budget));

    let start = Instant::now();
    let result = peer_io(
        next.peer_budget(),
        std::future::pending::<std::io::Result<()>>(),
    )
    .await;
    assert!(matches!(result, Err(Error::Timeout { .. })));
    assert_eq!(
        start.elapsed(),
        Duration::from_millis(40),
        "the rebuilt reader must inherit the first phase's spent allowance"
    );
}

#[tokio::test(start_paused = true)]
async fn bandwidth_short_writes_refund_only_the_tail() {
    use bifrost_net::{BandwidthBudget, BandwidthDirection};
    use futures::FutureExt;
    use tokio::io::AsyncReadExt;
    use tokio::time::{Duration, Instant};
    for tail in [true, false] {
        let budget = BandwidthBudget::new(Some(4));
        let (mut client, mut peer) = tokio::io::duplex(1);
        client.write_all(b"x").await.unwrap();
        let mut reader = WireReader::new_metered(
            ImapStream::Memory(super::super::stream::Tracked::new(client)),
            None,
            Some(budget.clone()),
        );
        let mut write = Box::pin(reader.write_admitted(b"abcd"));
        assert!((&mut write).now_or_never().is_none());
        let later = (!tail).then(|| budget.reserve(BandwidthDirection::Outbound, 4));
        let mut byte = [0];
        peer.read_exact(&mut byte).await.unwrap();
        assert_eq!(write.await.unwrap(), 1);
        let grant = budget.reserve(BandwidthDirection::Outbound, 4);
        assert_eq!(
            grant.ready_at().duration_since(Instant::now()),
            if tail {
                Duration::from_millis(250)
            } else {
                Duration::from_secs(2)
            }
        );
        peer.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, [b'a']);
        drop(grant);
        drop(later);
    }
}

#[tokio::test(start_paused = true)]
async fn bandwidth_cancelled_offered_read_keeps_its_charge() {
    use bifrost_net::{BandwidthBudget, BandwidthDirection};
    use futures::FutureExt;
    use tokio::time::{Duration, Instant};
    let budget = BandwidthBudget::new(Some(4));
    let (mut reader, _peer) = bandwidth_reader(budget.clone());
    let mut read = Box::pin(reader.read_admitted());
    assert!((&mut read).now_or_never().is_none());
    drop(read);
    let next = budget.reserve(BandwidthDirection::Inbound, 4);
    assert_eq!(
        next.ready_at().duration_since(Instant::now()),
        Duration::from_secs(1)
    );
}

#[tokio::test(start_paused = true)]
async fn bandwidth_compress_preserves_the_budget_and_bounds_inflation() {
    use super::super::stream::Tracked;
    use super::super::{CompressedStream, InnerStream};
    use tokio::time::{Duration, Instant};
    let budget = bifrost_net::BandwidthBudget::new(Some(4));
    let (mut reader, mut peer) = bandwidth_reader(budget);
    peer.write_all(b"abcd").await.unwrap();
    assert_eq!(reader.read_admitted().await.unwrap(), 4);
    reader.take_buffer();
    let metering = reader.metering();
    let old = reader.into_stream();
    let ImapStream::Memory(client) = old else {
        panic!("memory stream")
    };
    let mut reader = WireReader::with_metering(
        ImapStream::Compressed(CompressedStream::new(InnerStream::Memory(client))),
        metering,
    );
    let mut peer = ImapStream::Compressed(CompressedStream::new(InnerStream::Memory(
        Tracked::new(peer),
    )));
    peer.write_all(b"abcdefgh").await.unwrap();
    let start = Instant::now();
    assert_eq!(reader.read_admitted().await.unwrap(), 4);
    assert_eq!(&reader.buf[..], b"abcd");
    assert_eq!(reader.read_admitted().await.unwrap(), 4);
    assert_eq!(&reader.buf[..], b"abcdefgh");
    assert_eq!(start.elapsed(), Duration::from_secs(2));
}

#[tokio::test(start_paused = true)]
async fn bandwidth_idle_session_allows_a_sibling_command_before_done() {
    use crate::connection::test_support::{
        driver_pair_metered, preauth_greeting, read_line, respond, tag_of,
    };
    use tokio::time::{Duration, Instant};
    use tokio_util::sync::CancellationToken;
    let budget = bifrost_net::BandwidthBudget::default();
    let (idle_conn, mut idle_peer) =
        driver_pair_metered(&preauth_greeting("IMAP4rev1 IDLE"), budget.clone()).await;
    let (active_conn, mut active_peer) =
        driver_pair_metered(&preauth_greeting("IMAP4rev1"), budget.clone()).await;
    budget.set_cap(Some(64));
    let cancel = CancellationToken::new();
    let (start_tx, start_rx) = tokio::sync::oneshot::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let active = async {
        start_rx.await.unwrap();
        let start = Instant::now();
        let result = active_conn.noop(Duration::from_secs(5)).await;
        finished_tx.send(()).unwrap();
        result.unwrap();
        assert!(!start.elapsed().is_zero());
        assert!(start.elapsed() < Duration::from_secs(2));
    };
    let script = async {
        let command = read_line(&mut idle_peer).await;
        assert!(command.contains(" IDLE"));
        let idle_tag = tag_of(&command).to_owned();
        respond(&mut idle_peer, "+ idling\r\n").await;
        // Let the IDLE driver enter its silent, admitted peer read before
        // issuing work on the other connection. Only virtual time advances.
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        start_tx.send(()).unwrap();
        let command = read_line(&mut active_peer).await;
        respond(
            &mut active_peer,
            &format!("{} OK done\r\n", tag_of(&command)),
        )
        .await;
        finished_rx.await.unwrap();
        cancel.cancel();
        assert_eq!(read_line(&mut idle_peer).await, "DONE\r\n");
        respond(&mut idle_peer, &format!("{idle_tag} OK done\r\n")).await;
    };
    let (idle, (), ()) = tokio::join!(
        idle_conn.idle(
            Duration::from_secs(60),
            Duration::from_secs(5),
            cancel.clone()
        ),
        active,
        script
    );
    idle.unwrap();
}

#[tokio::test(start_paused = true)]
async fn bandwidth_cancelled_peer_io_keeps_its_spent_time() {
    use tokio::time::{Duration, Instant};
    let (mut reader, _peer) = bandwidth_reader(bifrost_net::BandwidthBudget::default());
    reader.set_peer_timeout(Some(Duration::from_millis(100)));
    let start = Instant::now();
    let result =
        super::super::lifecycle::setup_timeout(Duration::from_millis(30), reader.read_one(false))
            .await;
    assert!(matches!(result, Err(Error::Timeout { .. })));
    assert_eq!(start.elapsed(), Duration::from_millis(30));
    assert!(matches!(
        reader.read_one(false).await,
        Err(Error::Timeout { .. })
    ));
    assert_eq!(start.elapsed(), Duration::from_millis(100));
}

#[tokio::test]
async fn literal_budget_refuses_a_marker_without_reading_its_payload() {
    let (mut reader, mut peer) = memory_reader();
    peer.write_all(b"* 1 FETCH (UID 7 BODY[] {1025}\r\n")
        .await
        .unwrap();
    peer.shutdown().await.unwrap();
    // Without the pre-read ceiling this is Closed, after trying to read the
    // absent payload. The marker itself must suffice for the refusal.
    assert!(matches!(
        reader.read_one_limited(false, Some(1024)).await,
        Err(Error::FetchLiteralLimit {
            estimated: 1025,
            limit: 1024
        })
    ));
}

#[tokio::test]
async fn literal_budget_counts_sections_together_and_skips_payload_syntax() {
    let (mut reader, mut peer) = memory_reader();
    peer.write_all(b"* 1 FETCH (BODY[1] {5}\r\n{99}x BODY[2] ~{4}\r\n")
        .await
        .unwrap();
    peer.shutdown().await.unwrap();
    assert!(matches!(
        reader.read_one_limited(false, Some(8)).await,
        Err(Error::FetchLiteralLimit {
            estimated: 9,
            limit: 8
        })
    ));
}

#[tokio::test]
async fn literal_budget_accepts_exact_payload_and_does_not_charge_next_response() {
    let (mut reader, mut peer) = memory_reader();
    peer.write_all(b"* 1 FETCH (BODY[] {3}\r\nabc)\r\n* 2 FETCH (BODY[] {4}\r\ndefg)\r\n")
        .await
        .unwrap();
    assert!(reader.read_one_limited(false, Some(3)).await.is_ok());
    assert!(reader.read_one_limited(false, Some(4)).await.is_ok());
}

#[tokio::test]
async fn status_braces_do_not_spend_the_literal_budget() {
    let (mut reader, mut peer) = memory_reader();
    peer.write_all(b"* OK limit {999}\r\n").await.unwrap();
    assert!(reader.read_one_limited(false, Some(0)).await.is_ok());
}
