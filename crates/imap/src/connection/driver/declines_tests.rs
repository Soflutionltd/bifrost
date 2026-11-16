#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use crate::connection::{test_support, typed_event::TypedEvent};
use crate::types::{ResponseCode, UntaggedResponse};

#[tokio::test]
async fn declined_events_survive_transport_failure_without_duplicate_alerts() {
    for pipeline in [false, true] {
        let (conn, mut peer) =
            test_support::driver_pair(&test_support::preauth_greeting("IMAP4rev1")).await;
        let command = async {
            if pipeline {
                let results = conn
                    .pipeline()
                    .capability()
                    .execute_dynamic(Duration::from_secs(1))
                    .await
                    .unwrap();
                assert!(results[0].is_err());
            } else {
                assert!(conn.capability(Duration::from_secs(1)).await.is_err());
            }
        };
        let script = async {
            test_support::read_line(&mut peer).await;
            test_support::respond(
                &mut peer,
                "* OK [ALERT] attention\r\n* OK [X-TEST] unrelated\r\n",
            )
            .await;
            drop(peer);
        };
        tokio::join!(command, script);
        let events = conn.drain_events().await;
        assert!(
            matches!(&events[..], [TypedEvent::Alert(text), TypedEvent::Extension(response)]
            if text == "attention" && matches!(response.as_ref(), UntaggedResponse::Status {
                code: Some(ResponseCode::Other { .. }), text, ..
            } if text == "unrelated")),
            "pipeline={pipeline}: {events:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn declined_events_arrive_before_tagged_completion() {
    let (conn, mut peer) =
        test_support::driver_pair(&test_support::preauth_greeting("IMAP4rev1")).await;
    let command = conn.capability(Duration::from_secs(1));
    let script = async {
        let line = test_support::read_line(&mut peer).await;
        test_support::respond(&mut peer, "* OK [X-TEST] unrelated\r\n").await;
        assert!(matches!(
            conn.next_event(Duration::from_millis(100)).await.unwrap(),
            Some(TypedEvent::Extension(_))
        ));
        test_support::respond(
            &mut peer,
            &format!(
                "* CAPABILITY IMAP4rev1\r\n{} OK done\r\n",
                test_support::tag_of(&line)
            ),
        )
        .await;
    };
    let (result, ()) = tokio::join!(command, script);
    result.unwrap();
    assert!(conn.drain_events().await.is_empty());
}
