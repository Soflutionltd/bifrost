use super::*;
use crate::connection::test_support::{driver_pair, read_line, respond, tag_of};
use std::sync::Arc;

const GREETING: &[u8] = b"* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN] ready\r\n";
const LONG: Duration = Duration::from_secs(60);

async fn pair() -> (Arc<ImapConnection>, tokio::io::DuplexStream) {
    let (conn, server) = driver_pair(GREETING).await;
    (Arc::new(conn), server)
}

fn login_task(conn: &Arc<ImapConnection>) -> tokio::task::JoinHandle<Result<(), Error>> {
    let conn = Arc::clone(conn);
    tokio::spawn(async move { conn.login("bob", "secret", LONG).await })
}

async fn until_queued(conn: &ImapConnection) {
    let capacity = conn.cmd_tx.max_capacity() - 1;
    for _ in 0..100 {
        if conn.cmd_tx.capacity() == capacity {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("command was not queued");
}

#[tokio::test(start_paused = true)]
async fn a_command_queued_behind_login_is_encoded_after_the_refresh() {
    let (conn, mut server) = pair().await;
    let login = login_task(&conn);
    let line = read_line(&mut server).await;
    assert!(line.contains(" LOGIN "), "{line:?}");
    let noop = {
        let conn = Arc::clone(&conn);
        tokio::spawn(async move { conn.noop(LONG).await })
    };
    until_queued(&conn).await;
    respond(&mut server, &format!("{} OK logged in\r\n", tag_of(&line))).await;

    let next = read_line(&mut server).await;
    assert!(
        next.ends_with(" CAPABILITY\r\n"),
        "the queued NOOP ran between LOGIN and its refresh: {next:?}"
    );
    respond(
        &mut server,
        &format!(
            "* CAPABILITY IMAP4rev1 IDLE\r\n{} OK done\r\n",
            tag_of(&next)
        ),
    )
    .await;
    let next = read_line(&mut server).await;
    assert!(next.ends_with(" NOOP\r\n"), "{next:?}");
    respond(&mut server, &format!("{} OK done\r\n", tag_of(&next))).await;

    login.await.unwrap().unwrap();
    noop.await.unwrap().unwrap();
    let snap = conn.state_rx.borrow();
    assert_eq!(snap.session_state, SessionState::Authenticated);
    assert!(snap.capabilities.contains(&Capability::Idle));
}

#[tokio::test(start_paused = true)]
async fn an_untagged_capability_before_the_login_ok_still_owes_the_refresh() {
    let (conn, mut server) = pair().await;
    let login = login_task(&conn);
    let line = read_line(&mut server).await;
    respond(
        &mut server,
        &format!(
            "* CAPABILITY IMAP4rev1 AUTH=PLAIN\r\n{} OK logged in\r\n",
            tag_of(&line)
        ),
    )
    .await;
    let next = read_line(&mut server).await;
    assert!(
        next.ends_with(" CAPABILITY\r\n"),
        "a capability list from inside the exchange is not the \
         post-authentication one: {next:?}"
    );
    respond(
        &mut server,
        &format!(
            "* CAPABILITY IMAP4rev1 MOVE\r\n{} OK done\r\n",
            tag_of(&next)
        ),
    )
    .await;
    login.await.unwrap().unwrap();
    assert!(
        conn.state_rx
            .borrow()
            .capabilities
            .contains(&Capability::Move)
    );
}

#[tokio::test(start_paused = true)]
async fn a_login_ok_carrying_capabilities_needs_no_refresh() {
    let (conn, mut server) = pair().await;
    let login = login_task(&conn);
    let line = read_line(&mut server).await;
    respond(
        &mut server,
        &format!(
            "{} OK [CAPABILITY IMAP4rev1 IDLE] logged in\r\n",
            tag_of(&line)
        ),
    )
    .await;
    login.await.unwrap().unwrap();
    let noop = {
        let conn = Arc::clone(&conn);
        tokio::spawn(async move { conn.noop(LONG).await })
    };
    let next = read_line(&mut server).await;
    assert!(next.ends_with(" NOOP\r\n"), "{next:?}");
    respond(&mut server, &format!("{} OK done\r\n", tag_of(&next))).await;
    noop.await.unwrap().unwrap();
    assert!(
        conn.state_rx
            .borrow()
            .capabilities
            .contains(&Capability::Idle)
    );
}

#[tokio::test(start_paused = true)]
async fn a_login_whose_refresh_is_refused_retires_the_connection() {
    let (conn, mut server) = pair().await;
    let login = login_task(&conn);
    let line = read_line(&mut server).await;
    respond(&mut server, &format!("{} OK logged in\r\n", tag_of(&line))).await;
    let next = read_line(&mut server).await;
    assert!(next.ends_with(" CAPABILITY\r\n"), "{next:?}");
    respond(&mut server, &format!("{} NO not now\r\n", tag_of(&next))).await;

    let err = login.await.unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            Error::ServerStatus(crate::error::ServerStatus::No { .. })
        ),
        "{err:?}"
    );
    assert!(
        !conn.is_reusable(),
        "an authenticated session with the pre-authentication capability \
         set must not be reusable"
    );
    assert_eq!(conn.state_rx.borrow().session_state, SessionState::Logout);
}

#[tokio::test(start_paused = true)]
async fn a_login_whose_caller_leaves_during_the_refresh_is_in_flight() {
    let (conn, mut server) = pair().await;
    let login = {
        let conn = Arc::clone(&conn);
        tokio::spawn(async move { conn.login("bob", "secret", Duration::from_secs(1)).await })
    };
    let line = read_line(&mut server).await;
    respond(&mut server, &format!("{} OK logged in\r\n", tag_of(&line))).await;
    let next = read_line(&mut server).await;
    assert!(next.ends_with(" CAPABILITY\r\n"), "{next:?}");

    let err = login.await.unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            Error::Transport {
                kind: crate::error::TransportFailure::Timeout,
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "the transition committed; the refresh is never withdrawable"
    );
    assert!(!conn.is_reusable());
}

#[tokio::test(start_paused = true)]
async fn an_unauthenticate_whose_refresh_is_refused_retires_the_connection() {
    let (conn, mut server) = driver_pair(&crate::connection::test_support::preauth_greeting(
        "IMAP4rev1 UNAUTHENTICATE",
    ))
    .await;
    let conn = Arc::new(conn);
    let task = {
        let conn = Arc::clone(&conn);
        tokio::spawn(async move { conn.unauthenticate(LONG).await })
    };
    let line = read_line(&mut server).await;
    assert!(line.ends_with(" UNAUTHENTICATE\r\n"), "{line:?}");
    respond(&mut server, &format!("{} OK reset\r\n", tag_of(&line))).await;
    let next = read_line(&mut server).await;
    assert!(next.ends_with(" CAPABILITY\r\n"), "{next:?}");
    respond(&mut server, &format!("{} BAD no\r\n", tag_of(&next))).await;

    let err = task.await.unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            Error::ServerStatus(crate::error::ServerStatus::Bad { .. })
        ),
        "{err:?}"
    );
    assert!(!conn.is_reusable());
}
