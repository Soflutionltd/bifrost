#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use super::*;
use crate::connection::test_support::{driver_pair, preauth_greeting, read_line, respond, tag_of};
use crate::types::{FetchAttr, UidSet};

fn fetch_response(seq: u32) -> FetchResponse {
    FetchResponse {
        seq,
        ..FetchResponse::default()
    }
}

#[tokio::test]
async fn early_fetch_consumer_close_discards_buffered_items_and_unblocks_the_driver() {
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    tx.send(Ok(fetch_response(1))).await.unwrap();
    tx.send(Ok(fetch_response(2))).await.unwrap();

    let error = drain_fetch_stream(rx, |_| Err(Error::InvalidInput("stop fetching".into())))
        .await
        .expect_err("the callback error is preserved");

    assert!(matches!(error, Error::InvalidInput(message) if message == "stop fetching"));
    assert!(
        tx.send(Ok(fetch_response(3))).await.is_err(),
        "closing the receiver tells the bounded driver consumer to drain to tagged completion"
    );
}

#[tokio::test]
async fn uid_fetch_each_finishes_after_the_callback_stops_a_full_bounded_stream() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        let select_tag = tag_of(&select);
        respond(
            &mut server,
            &format!(
                "* 0 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * FLAGS (\\Seen)\r\n\
                 * OK [UIDVALIDITY 1] UIDs valid\r\n\
                 * OK [UIDNEXT 1] Predicted next UID\r\n\
                 {select_tag} OK selected\r\n"
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        let fetch_tag = tag_of(&fetch);
        let mut responses = String::new();
        for sequence in 1..=65 {
            responses.push_str(&format!("* {sequence} FETCH (UID {sequence})\r\n"));
        }
        respond(&mut server, &responses).await;
        respond(&mut server, &format!("{fetch_tag} OK fetched\r\n")).await;
    });

    conn.select("INBOX", Duration::from_secs(5)).await.unwrap();
    let uids = UidSet::parse("1:65").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        conn.uid_fetch_each(&uids, &[FetchAttr::Uid], Duration::from_secs(5), |_| {
            Err(Error::InvalidInput("stop fetching".into()))
        }),
    )
    .await
    .expect("closing the receiver must let the driver reach the tagged completion");

    assert!(matches!(result, Err(Error::InvalidInput(message)) if message == "stop fetching"));
    script.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn uid_fetch_each_gives_up_when_the_server_stalls_after_an_early_consumer_stop() {
    // The driver pre-reserves an mpsc permit before every socket read, so a
    // closed-but-not-dropped receiver never yields None while the driver is
    // parked on a stalled socket. The early-exit path must therefore release
    // the receiver outright and let the command timeout be the only bound.
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        let select_tag = tag_of(&select);
        respond(
            &mut server,
            &format!(
                "* 0 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * FLAGS (\\Seen)\r\n\
                 * OK [UIDVALIDITY 1] UIDs valid\r\n\
                 * OK [UIDNEXT 1] Predicted next UID\r\n\
                 {select_tag} OK selected\r\n"
            ),
        )
        .await;

        let _fetch = read_line(&mut server).await;
        let mut responses = String::new();
        for sequence in 1..=3 {
            responses.push_str(&format!("* {sequence} FETCH (UID {sequence})\r\n"));
        }
        respond(&mut server, &responses).await;
        // No tagged completion: the server stalls mid-command, holding the
        // socket open while the driver sits on an unused reserved permit.
        let _ = release_rx.await;
    });

    conn.select("INBOX", Duration::from_secs(5)).await.unwrap();
    let uids = UidSet::parse("1:3").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(600),
        conn.uid_fetch_each(&uids, &[FetchAttr::Uid], Duration::from_secs(2), |_| {
            Err(Error::InvalidInput("stop fetching".into()))
        }),
    )
    .await
    .expect("a stalled server must not outlive the command timeout");

    assert!(matches!(result, Err(Error::InvalidInput(message)) if message == "stop fetching"));
    let _ = release_tx.send(());
    script.await.unwrap();
}

#[tokio::test]
async fn limited_fetch_retires_connection_on_oversized_literal_declaration() {
    use crate::connection::test_support::select_inbox;
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    select_inbox(&conn, &mut server).await;
    let script = async {
        let command = read_line(&mut server).await;
        assert!(command.contains("UID FETCH"));
        respond(&mut server, "* 1 FETCH (UID 1 BODY[] {1025}\r\n").await;
        tokio::io::AsyncWriteExt::shutdown(&mut server)
            .await
            .unwrap();
    };
    let uids = UidSet::parse("1").unwrap();
    let attrs = [FetchAttr::BodySection {
        peek: true,
        section: None,
        partial: None,
    }];
    let (result, ()) = tokio::join!(
        conn.uid_fetch_limited(&uids, &attrs, 1024, Duration::from_secs(5)),
        script,
    );
    assert!(matches!(result, Err(Error::FetchLiteralLimit { .. })));
    assert!(!conn.is_alive());
}

#[tokio::test]
async fn budgeted_fetch_stream_rejects_payload_before_channel_delivery() {
    use crate::connection::test_support::select_inbox;
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    select_inbox(&conn, &mut server).await;
    let uids = UidSet::parse("1").unwrap();
    let attrs = [FetchAttr::BodySection {
        peek: true,
        section: None,
        partial: None,
    }];
    let (mut rx, fetch) = conn
        .uid_fetch_stream_budgeted(
            uids.as_sequence_set(),
            &attrs,
            1,
            Some(2),
            Duration::from_secs(5),
        )
        .unwrap();
    let script = async {
        let _ = read_line(&mut server).await;
        respond(&mut server, "* 1 FETCH (BODY[] {3}\r\n").await;
        tokio::io::AsyncWriteExt::shutdown(&mut server)
            .await
            .unwrap();
    };
    let (result, ()) = tokio::join!(fetch, script);
    assert!(matches!(result, Err(Error::FetchLiteralLimit { .. })));
    assert!(rx.recv().await.is_none());
    assert!(!conn.is_alive());
}
