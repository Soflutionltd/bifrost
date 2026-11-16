use super::test_support::*;

/// A hydration FETCH failure after the stale-UIDVALIDITY batch was
/// published must downgrade only the still-unresolved ids. Re-emitting the
/// stale ids as `Uncertain` would put one id in two lanes: the engine
/// would terminally fail it and simultaneously queue it for read-back.
#[tokio::test]
async fn folder_get_failure_does_not_relabel_published_stale_ids() {
    use bifrost_types::{ItemOutcome, Projection};

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let stale_id = crate::account::encode_object_id(
        &folder,
        crate::types::UidValidity::new(4).expect("nonzero epoch"),
        crate::types::Uid::new(7).expect("valid protocol test value"),
    );
    let valid_id = crate::account::encode_object_id(
        &folder,
        crate::types::UidValidity::new(5).expect("nonzero epoch"),
        crate::types::Uid::new(3).expect("valid protocol test value"),
    );

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
        respond(
            &mut server,
            &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&fetch)),
        )
        .await;
        server
    });

    let ids: bifrost_types::AccountStream<bifrost_types::ObjectId> =
        Box::pin(futures::stream::iter(vec![
            stale_id.clone(),
            valid_id.clone(),
        ]));
    let mut stream = crate::account::get::get_stream(account, ids, Projection::Metadata);

    let mut failed = Vec::new();
    let mut uncertain = Vec::new();
    let mut succeeded = Vec::new();
    use futures::StreamExt;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("stream must make progress")
    {
        match event {
            SyncEvent::Batch(batch) => {
                for outcome in batch.items {
                    match outcome {
                        ItemOutcome::Failed(item) => failed.push(item.item.0.clone()),
                        ItemOutcome::Uncertain(item) => uncertain.push(item.item.0.clone()),
                        ItemOutcome::Succeeded(item) => succeeded.push(item.item.0.clone()),
                    }
                }
            }
            SyncEvent::Done(_) => break,
            other => panic!("unexpected event: {other:?}"),
        }
    }

    assert_eq!(
        failed,
        vec![stale_id.0.clone()],
        "the stale id fails exactly once, before the FETCH"
    );
    assert_eq!(
        uncertain,
        vec![valid_id.0.clone()],
        "only the unresolved id may fall to the uncertain lane"
    );
    assert!(succeeded.is_empty());
    let _server = script.await.unwrap();
}

/// The target buffer must flush at its exact ceiling even while the input
/// producer remains open. Removing the production flush makes this test time
/// out before the server sees SELECT.
#[tokio::test]
async fn get_flushes_at_target_buffer_boundary_before_input_closes() {
    use bifrost_types::{Projection, SyncEvent};
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\""),
            "expected SELECT, got {select}"
        );
        respond(&mut server, &format!(
            "* FLAGS (\\Seen)\r\n* 256 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 257] ok\r\n{} OK [READ-ONLY] done\r\n",
            tag_of(&select),
        )).await;
        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("UID FETCH"),
            "expected bounded-window FETCH, got {fetch}"
        );
        respond(&mut server, &format!("{} OK done\r\n", tag_of(&fetch))).await;
    });

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(257);
    for uid in 1..=crate::account::TARGET_BUFFER_ITEMS {
        input_tx
            .send(crate::account::encode_object_id(
                &crate::types::MailboxName::new("INBOX").unwrap(),
                crate::types::UidValidity::new(5).expect("nonzero epoch"),
                crate::types::Uid::new(u32::try_from(uid).unwrap()).unwrap(),
            ))
            .await
            .unwrap();
    }
    let ids = Box::pin(futures::stream::unfold(input_rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    let mut output = crate::account::get::get_stream(account, ids, Projection::FlagsOnly);
    let first = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the full buffer must flush before input closes")
        .expect("one output batch");
    assert!(
        matches!(&first, SyncEvent::Batch(batch) if batch.items.iter().all(|item| !matches!(item, bifrost_types::ItemOutcome::Uncertain(_)))),
        "SELECT failed before the boundary FETCH: {first:?}"
    );
    drop(input_tx);
    script.await.unwrap();
}

/// A UID FETCH that completes without data for a UID valid in the current
/// UIDVALIDITY epoch is how IMAP reports an expunged message (RFC 9051 6.4.9:
/// a non-existent UID is ignored). That is `NotFound(Message)`, answered by
/// the server, not the caller's malformed request. An unsolicited FETCH for a
/// DIFFERENT uid in the same exchange must not stand in for the missing one:
/// the answer used to be whatever sorted last in the collected responses.
#[tokio::test]
async fn message_hydrate_of_an_expunged_uid_is_not_found() {
    use bifrost_types::{
        AccountErrorKind, AccountOperation, Cause, ErrorScope, RecoveryClass, ResourceKind,
        TransmissionState,
    };

    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let wanted = crate::account::encode_object_id(
        &inbox,
        crate::types::UidValidity::new(5).expect("nonzero epoch"),
        crate::types::Uid::new(3).expect("valid protocol test value"),
    );

    for (label, data) in [
        ("no FETCH data at all", ""),
        (
            "only an unsolicited FETCH for another uid",
            "* 9 FETCH (UID 9 FLAGS (\\Seen))\r\n",
        ),
    ] {
        let err = hydrate_uid_3_against(data).await.expect_err(label);
        assert_eq!(
            err.kind(),
            &AccountErrorKind::NotFound(ResourceKind::Message),
            "{label}: got {err:?}"
        );
        assert_eq!(err.recovery(), &RecoveryClass::ProviderRefused, "{label}");
        assert_eq!(
            err.operation(),
            Some(AccountOperation::HydrateMessage),
            "{label}"
        );
        assert_eq!(
            err.scope(),
            Some(&ErrorScope::Message { id: wanted.clone() }),
            "{label}"
        );
        assert!(
            err.chain().iter().any(|cause| matches!(
                cause,
                Cause::Attempt(a) if a.transmission_state == TransmissionState::Acknowledged
            )),
            "{label}: the server answered, so the attempt is Acknowledged: {err:?}"
        );
    }

    // Present alongside an unsolicited FETCH for a higher uid: the requested
    // message is the answer, not the one that sorts last.
    let message =
        hydrate_uid_3_against("* 3 FETCH (UID 3 FLAGS ())\r\n* 9 FETCH (UID 9 FLAGS (\\Seen))\r\n")
            .await
            .expect("uid 3 was returned");
    assert_eq!(message.id, wanted);
}

/// `thread_hydrate` answers exactly one message per REQUESTED uid. The UID
/// FETCH here carries, besides the two members: a trailing FLAGS-only
/// response for uid 3 after its body-bearing one, and an unsolicited FETCH
/// for uid 9, which is not in the thread. Each response used to become a
/// `Message` of its own, so the thread came back with four messages - uid 3
/// twice, one copy bodiless - and a stranger.
#[tokio::test]
async fn thread_hydrate_answers_once_per_requested_uid() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let mut caps = crate::account::test_support::stub_capabilities();
    caps.pim_methods.thread_hydrate = true;
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });
    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let thread = crate::account::encode_thread_id(
        &inbox,
        crate::types::UidValidity::new(5).expect("nonzero epoch"),
        &crate::types::set::UidSet::from_numbers([3, 4]),
    );

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\""),
            "expected EXAMINE, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Seen)\r\n\
                 * 9 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 10] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("UID FETCH"),
            "expected UID FETCH, got {fetch}"
        );
        let body = "From: a@b.test\r\nSubject: hi\r\n\r\nhello there\r\n";
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 3 FLAGS () BODY[] {{{}}}\r\n{body})\r\n\
                 * 1 FETCH (UID 3 FLAGS (\\Seen))\r\n\
                 * 2 FETCH (UID 4 FLAGS ())\r\n\
                 * 9 FETCH (UID 9 FLAGS (\\Seen))\r\n\
                 {} OK done\r\n",
                body.len(),
                tag_of(&fetch)
            ),
        )
        .await;
        server
    });

    let hydrated = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::message::thread_hydrate(account, thread),
    )
    .await
    .expect("thread_hydrate must not hang")
    .expect("the thread hydrates");
    let _server = script.await.unwrap();

    let ids: Vec<_> = hydrated.messages.iter().map(|m| m.id.clone()).collect();
    assert_eq!(
        ids,
        vec![
            crate::account::encode_object_id(
                &inbox,
                crate::types::UidValidity::new(5).expect("nonzero epoch"),
                crate::types::Uid::new(3).expect("valid protocol test value")
            ),
            crate::account::encode_object_id(
                &inbox,
                crate::types::UidValidity::new(5).expect("nonzero epoch"),
                crate::types::Uid::new(4).expect("valid protocol test value")
            ),
        ],
        "one message per requested uid, and nothing else"
    );
    let first = &hydrated.messages[0];
    assert!(
        first
            .body_text
            .as_deref()
            .is_some_and(|text| text.contains("hello there")),
        "the trailing FLAGS-only response must not replace the body: {first:?}"
    );
}

#[tokio::test]
async fn flags_only_fetch_without_flags_is_a_missing_field_failure() {
    use bifrost_types::{AccountErrorKind, ItemOutcome, Projection, ProtocolErrorKind, SyncEvent};
    use futures::StreamExt;
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(&mut server, &format!("* FLAGS (\\Seen)\r\n* 2 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 3] ok\r\n{} OK [READ-ONLY] done\r\n", tag_of(&select))).await;
        let fetch = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 1)\r\n* 2 FETCH (UID 2 FLAGS (\\Seen))\r\n{} OK done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
    });
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let ids = (1..=2)
        .map(|uid| {
            crate::account::encode_object_id(
                &folder,
                crate::types::UidValidity::new(5).expect("nonzero epoch"),
                crate::types::Uid::new(uid).expect("nonzero test UID"),
            )
        })
        .collect::<Vec<_>>();
    let mut output = crate::account::get::get_stream(
        account,
        Box::pin(futures::stream::iter(ids)),
        Projection::FlagsOnly,
    );
    let mut items = Vec::new();
    while let Some(event) = output.next().await {
        if let SyncEvent::Batch(batch) = event {
            items.extend(batch.items);
        }
    }
    assert_eq!(items.len(), 2);
    assert!(
        matches!(&items[0],ItemOutcome::Failed(f) if matches!(f.error.kind(),AccountErrorKind::Protocol(ProtocolErrorKind::MissingField)))
    );
    assert!(matches!(&items[1], ItemOutcome::Succeeded(_)));
    script.await.unwrap();
}

/// Consumers correlate outcomes by exact string, so a legacy `imap1` id and a
/// non-canonical `imap2` spelling must each come back under the string that
/// was submitted - lane id and hydrated id alike - even when both name the
/// same message and the miss lane answers for a third.
#[tokio::test]
async fn hydration_answers_under_the_submitted_id_spelling() {
    use bifrost_types::{ItemOutcome, ObjectId, Projection, SyncEvent};
    use futures::StreamExt;
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(&mut server, &format!("* FLAGS (\\Seen)\r\n* 2 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 3] ok\r\n{} OK [READ-ONLY] done\r\n", tag_of(&select))).await;
        let fetch = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 1 FLAGS (\\Seen))\r\n{} OK done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
    });
    let submitted = [
        ObjectId("imap1:5:INBOX:5:1".to_owned()),
        ObjectId("imap2:05:INBOX:005:001".to_owned()),
        ObjectId("imap1:5:INBOX:5:2".to_owned()),
    ];
    let mut output = crate::account::get::get_stream(
        account,
        Box::pin(futures::stream::iter(submitted.clone())),
        Projection::FlagsOnly,
    );
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    while let Some(event) = output.next().await {
        if let SyncEvent::Batch(batch) = event {
            for item in batch.items {
                match item {
                    ItemOutcome::Succeeded(s) => {
                        assert_eq!(s.output.id.0, s.item.0, "hydrated id must echo the lane id");
                        succeeded.push(s.item.0);
                    }
                    ItemOutcome::Failed(f) => failed.push(f.item.0),
                    ItemOutcome::Uncertain(u) => panic!("unexpected uncertain {u:?}"),
                }
            }
        }
    }
    succeeded.sort();
    let mut expected = vec![submitted[0].0.clone(), submitted[1].0.clone()];
    expected.sort();
    assert_eq!(succeeded, expected);
    assert_eq!(failed, vec![submitted[2].0.clone()]);
    script.await.unwrap();
}
