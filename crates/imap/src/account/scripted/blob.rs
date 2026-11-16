use super::test_support::*;

/// The two blob openers are statically unsupported (`blob_range` is
/// `BlobRangeSupport::No`, and nothing mints a `BlobHandle`), so both must
/// terminate the stream with `Unsupported(<their own operation>)` and then
/// end immediately - never append `Done` after `Terminated`, hang, or
/// mis-tag the operation, which is what the engine's recovery mapping keys on.
#[tokio::test]
async fn the_blob_openers_terminate_unsupported_with_their_own_operation() {
    use bifrost_types::{
        Account, AccountErrorKind, AccountOperation, BlobCapabilities, BlobEncoding, BlobHandle,
        BlobId, ByteRange,
    };
    use futures::StreamExt;

    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);

    let handle = || BlobHandle {
        id: BlobId("imapblob1:INBOX:1:2:1.2".to_owned()),
        size: None,
        content_type: None,
        digest: None,
        capabilities: BlobCapabilities {
            supports_range: false,
            supports_parallel: false,
            digest_available_pre_download: false,
            encoding: BlobEncoding::Raw8Bit,
        },
    };

    for (mut stream, op) in [
        (account.open_blob(handle()), AccountOperation::OpenBlob),
        (
            account.open_blob_range(
                handle(),
                ByteRange {
                    start: 0,
                    length: Some(16),
                },
            ),
            AccountOperation::OpenBlobRange,
        ),
    ] {
        let first = stream.next().await.expect("the stream emits a terminal");
        let SyncEvent::Terminated(err) = first else {
            panic!("expected Terminated, got {first:?}");
        };
        assert_eq!(err.kind(), &AccountErrorKind::Unsupported(op));
        assert_eq!(err.operation(), Some(op));
        assert!(stream.next().await.is_none(), "Terminated ends the stream");
    }
}

/// `open_raw_rfc822` must STREAM, not buffer the whole message.
///
/// The transcript gates the rest of the FETCH (a FLAGS-only fragment for the
/// same UID and the tagged OK) behind the test having already received the
/// body, so a buffered `uid_fetch` - which emits nothing until the tagged
/// completion - deadlocks and fails on the timeout instead of quietly holding
/// an arbitrarily large message in one `Vec<FetchResponse>`. The fragment
/// also pins that a body-less response for the UID beside the body is not a
/// missing body.
#[tokio::test]
async fn open_raw_rfc822_emits_chunks_before_the_tagged_completion() {
    use bytes::Bytes;
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE") || select.contains("SELECT"),
            "expected a select, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(fetch.contains("FETCH"), "expected UID FETCH, got {fetch}");
        respond(&mut server, "* 1 FETCH (UID 7 BODY[] {5}\r\nhello)\r\n").await;
        // Nothing more until the consumer has seen the first chunk.
        gate_rx.await.expect("the consumer must reach the gate");
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 7 FLAGS (\\Seen))\r\n{} OK FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
        server
    });

    let message = crate::account::encode_object_id(
        &folder,
        crate::types::UidValidity::new(5).expect("nonzero epoch"),
        crate::types::Uid::new(7).expect("valid protocol test value"),
    );
    let mut stream = crate::account::blob::open_raw_rfc822(account, message);

    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("a chunk must arrive before the tagged OK")
        .expect("the stream emits a batch");
    let SyncEvent::Batch(batch) = first else {
        panic!("expected a Batch, got {first:?}");
    };
    assert_eq!(batch.items, vec![Bytes::from_static(b"hello")]);
    gate_tx.send(()).expect("the script is still running");

    let mut rest = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the stream must finish")
    {
        match event {
            SyncEvent::Batch(batch) => rest.extend(batch.items),
            SyncEvent::Done(_) => break,
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert!(rest.is_empty(), "one message body, emitted once: {rest:?}");
    let _server = script.await.unwrap();
}

/// An expunged UID (tagged OK, no FETCH response for it) is `NotFound`, keyed
/// by the submitted id, not a successful empty read.
#[tokio::test]
async fn a_raw_read_of_an_expunged_uid_is_not_found() {
    let events = raw_read_events("").await;
    let [SyncEvent::Terminated(err)] = events.as_slice() else {
        panic!("expected one Terminated, got {events:?}");
    };
    assert_eq!(
        err.kind(),
        &AccountErrorKind::NotFound(ResourceKind::Message)
    );
    assert_eq!(err.operation(), Some(AccountOperation::OpenRawRfc822));
}

/// A response for another UID is not this message's body.
#[tokio::test]
async fn a_raw_read_ignores_responses_for_other_uids() {
    let events = raw_read_events("* 2 FETCH (UID 8 BODY[] {3}\r\nabc)\r\n").await;
    assert!(
        matches!(
            events.as_slice(),
            [SyncEvent::Terminated(err)]
                if err.kind() == &AccountErrorKind::NotFound(ResourceKind::Message)
        ),
        "{events:?}"
    );
}

/// A UID answered without its body, or with two different bodies, is the
/// server's protocol violation.
#[tokio::test]
async fn a_raw_read_refuses_a_missing_or_conflicting_body() {
    for answer in [
        "* 1 FETCH (UID 7 FLAGS (\\Seen))\r\n",
        "* 1 FETCH (UID 7 BODY[] NIL)\r\n",
        "* 1 FETCH (UID 7 BODY[] {3}\r\nabc)\r\n* 1 FETCH (UID 7 BODY[] {3}\r\nxyz)\r\n",
    ] {
        let events = raw_read_events(answer).await;
        assert!(
            matches!(
                events.as_slice(),
                [.., SyncEvent::Terminated(err)]
                    if matches!(err.kind(), AccountErrorKind::Protocol(_))
            ),
            "{answer:?}: {events:?}"
        );
    }
}

/// A zero-octet body is a delivered message; an identical repeat is emitted
/// once.
#[tokio::test]
async fn a_raw_read_delivers_an_empty_body_and_tolerates_an_identical_repeat() {
    let events = raw_read_events("* 1 FETCH (UID 7 BODY[] {0}\r\n)\r\n").await;
    assert!(
        matches!(events.as_slice(), [SyncEvent::Batch(b), SyncEvent::Done(_)] if b.items == vec![bytes::Bytes::new()]),
        "{events:?}"
    );
    let events = raw_read_events(
        "* 1 FETCH (UID 7 BODY[] {3}\r\nabc)\r\n* 1 FETCH (UID 7 BODY[] {3}\r\nabc)\r\n",
    )
    .await;
    assert!(
        matches!(events.as_slice(), [SyncEvent::Batch(_), SyncEvent::Done(_)]),
        "{events:?}"
    );
}
