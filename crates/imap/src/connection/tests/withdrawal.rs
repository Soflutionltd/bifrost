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
) -> JoinHandle<Result<Option<crate::types::AppendUid>, Error>> {
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
    assert!(matches!(
        err,
        Error::Transport {
            kind: crate::error::TransportFailure::Timeout,
            ..
        }
    ));
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
    assert!(matches!(
        err,
        Error::Transport {
            kind: crate::error::TransportFailure::Timeout,
            ..
        }
    ));
    assert_eq!(err.attempt(), Some(InFlight));
    assert!(!conn.is_reusable());
}
/// A connection whose outbound bandwidth is booked solid for ten seconds,
/// so the next command the driver claims waits for its first grant.
async fn starved_pair() -> (
    Arc<ImapConnection>,
    tokio::io::DuplexStream,
    bifrost_net::BandwidthBudget,
    Vec<bifrost_net::Reservation>,
) {
    let budget = bifrost_net::BandwidthBudget::new(Some(10));
    let (conn, server) = crate::connection::test_support::driver_pair_metered(
        &preauth_greeting("IMAP4rev1"),
        budget.clone(),
    )
    .await;
    let hog = (0..10)
        .map(|_| budget.reserve(bifrost_net::BandwidthDirection::Outbound, 10))
        .collect();
    (Arc::new(conn), server, budget, hog)
}
async fn capability_reaches_the_wire_next(
    conn: &Arc<ImapConnection>,
    server: &mut tokio::io::DuplexStream,
) {
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
async fn a_claimed_command_withdrawn_during_its_first_bandwidth_wait_is_unsent_and_reusable() {
    let (conn, mut server, budget, hog) = starved_pair().await;
    let err = append_task(&conn, SHORT).await.unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            Error::Transport {
                kind: crate::error::TransportFailure::Timeout,
                ..
            }
        ),
        "got {err:?}"
    );
    assert_eq!(err.attempt(), Some(Unsent));
    assert!(conn.is_reusable());
    drop(hog);
    budget.set_cap(None);
    capability_reaches_the_wire_next(&conn, &mut server).await;
}
#[tokio::test(start_paused = true)]
async fn a_dropped_command_waiting_for_its_first_bandwidth_grant_does_not_abandon() {
    let (conn, mut server, budget, hog) = starved_pair().await;
    let mut b = Box::pin(conn.append("INBOX", &[], None, b"hello", LONG));
    assert!(futures::poll!(&mut b).is_pending());
    // Let the driver claim it and park on the grant.
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(conn.cmd_tx.capacity(), conn.cmd_tx.max_capacity());
    drop(b);
    assert!(conn.is_reusable());
    drop(hog);
    budget.set_cap(None);
    capability_reaches_the_wire_next(&conn, &mut server).await;
}
#[tokio::test(start_paused = true)]
async fn a_command_whose_driver_dies_during_its_first_bandwidth_wait_is_unsent() {
    let (conn, _server, _budget, _hog) = starved_pair().await;
    let b = append_task(&conn, LONG);
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(conn.cmd_tx.capacity(), conn.cmd_tx.max_capacity());
    conn.terminate().await;
    let err = b.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        Error::Transport {
            kind: crate::error::TransportFailure::DriverGone,
            ..
        } | Error::Internal {
            kind: crate::error::InternalFailure::Panicked(_),
            ..
        }
    ));
    assert_eq!(err.attempt(), Some(Unsent));
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
        Error::Transport {
            kind: crate::error::TransportFailure::DriverGone,
            ..
        } | Error::Internal {
            kind: crate::error::InternalFailure::Panicked(_),
            ..
        }
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
    assert!(matches!(
        err,
        Error::Transport {
            kind: crate::error::TransportFailure::Timeout,
            ..
        }
    ));
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
    assert!(matches!(
        err,
        Error::Transport {
            kind: crate::error::TransportFailure::Timeout,
            ..
        }
    ));
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
        matches!(&err, Error::MoveFallbackIncomplete {stored_deleted:false,source,..} if matches!(source.as_ref(),Error::Transport { kind: crate::error::TransportFailure::Timeout, .. }))
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
    assert!(matches!(
        b.await.unwrap(),
        IdleEvent::Event(crate::connection::typed_event::TypedEvent::Alert(_))
    ));
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
            .execute(Duration::from_secs(30)),
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
