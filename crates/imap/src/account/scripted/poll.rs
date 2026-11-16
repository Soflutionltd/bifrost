use super::test_support::*;

#[tokio::test]
async fn observe_reports_uidvalidity_and_uidnext_of_the_selection() {
    let (observation, seen) = poller_transcript(POLL_SELECT, vec![], |poller| async move {
        poller.observe(&ContainerId("INBOX".into())).await.unwrap()
    })
    .await;
    assert!(seen.is_empty());
    assert_eq!(observation.uid_validity, nz(5));
    assert_eq!(observation.uid_next, nz(9));
    assert_eq!(observation.exists, 4);
    assert_eq!(observation.container, ContainerId("INBOX".into()));
}

#[tokio::test]
async fn observe_refuses_a_missing_uidnext_and_a_uidnotsticky_folder() {
    let (missing, _) = poller_transcript(
        "* 4 EXISTS\r\n* OK [UIDVALIDITY 5] ok\r\n",
        vec![],
        |poller| async move { poller.observe(&ContainerId("INBOX".into())).await },
    )
    .await;
    assert!(
        matches!(missing.unwrap_err().kind(), AccountErrorKind::Protocol(_)),
        "a server that omits UIDNEXT is not guessed at"
    );
    let (sticky, _) = poller_transcript(
        "* 4 EXISTS\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 9] ok\r\n* OK [UIDNOTSTICKY] non-persistent UIDs\r\n",
        vec![],
        |poller| async move { poller.observe(&ContainerId("INBOX".into())).await },
    )
    .await;
    assert_eq!(
        sticky.unwrap_err().kind(),
        &AccountErrorKind::Unsupported(AccountOperation::SearchMessages)
    );
}

/// The listing searches an explicit range bounded by UIDNEXT - 1 (never
/// `*`), drops UIDs the server reported outside it, and when the limit cuts
/// the window short it completes through the last kept UID only.
#[tokio::test]
async fn list_uids_bounds_the_range_and_resumes_after_the_last_kept_uid() {
    let (listing, seen) = poller_transcript(
        POLL_SELECT,
        vec!["* SEARCH 8 2 3 5 7 3\r\n"],
        |poller| async move {
            poller
                .list_uids(
                    &ContainerId("INBOX".into()),
                    crate::account::UidScan::new(
                        nz(5),
                        nz(3),
                        std::num::NonZeroUsize::new(2).unwrap(),
                    ),
                )
                .await
                .unwrap()
        },
    )
    .await;
    assert_eq!(seen.len(), 1);
    assert!(seen[0].contains("UID SEARCH UID 3:8"), "{}", seen[0]);
    let crate::account::UidListing::Listed(page) = listing else {
        panic!("expected Listed, got {listing:?}");
    };
    let uids: Vec<u32> = page.refs.iter().map(|r| r.uid().get()).collect();
    assert_eq!(uids, vec![3, 5]);
    assert_eq!(page.completed_through, Some(nz(5)));
    assert_eq!(page.next_from, Some(nz(6)));
}

#[tokio::test]
async fn list_uids_completes_the_bound_when_everything_fits() {
    let (listing, seen) =
        poller_transcript(POLL_SELECT, vec!["* SEARCH 4 6\r\n"], |poller| async move {
            poller
                .list_uids(
                    &ContainerId("INBOX".into()),
                    crate::account::UidScan::new(
                        nz(5),
                        nz(3),
                        std::num::NonZeroUsize::new(10).unwrap(),
                    )
                    .through(nz(6)),
                )
                .await
                .unwrap()
        })
        .await;
    assert!(seen[0].contains("UID SEARCH UID 3:6"), "{}", seen[0]);
    let crate::account::UidListing::Listed(page) = listing else {
        panic!("expected Listed, got {listing:?}");
    };
    assert_eq!(page.refs.len(), 2);
    assert_eq!(page.completed_through, Some(nz(6)));
    assert_eq!(page.next_from, None);
}

/// From at or past UIDNEXT has nothing to scan and sends no SEARCH at all,
/// so the `n:*` rule cannot hand back the highest UID below `n`.
#[tokio::test]
async fn list_uids_past_uidnext_sends_no_search() {
    let (listing, seen) = poller_transcript(POLL_SELECT, vec![], |poller| async move {
        poller
            .list_uids(
                &ContainerId("INBOX".into()),
                crate::account::UidScan::new(
                    nz(5),
                    nz(9),
                    std::num::NonZeroUsize::new(10).unwrap(),
                ),
            )
            .await
            .unwrap()
    })
    .await;
    assert!(seen.is_empty());
    let crate::account::UidListing::Listed(page) = listing else {
        panic!("expected Listed, got {listing:?}");
    };
    assert!(page.refs.is_empty());
    assert_eq!(page.completed_through, None);
    assert_eq!(page.next_from, None);
}

#[tokio::test]
async fn list_uids_in_a_changed_generation_lists_nothing() {
    let (listing, seen) = poller_transcript(POLL_SELECT, vec![], |poller| async move {
        poller
            .list_uids(
                &ContainerId("INBOX".into()),
                crate::account::UidScan::new(
                    nz(4),
                    nz(1),
                    std::num::NonZeroUsize::new(10).unwrap(),
                ),
            )
            .await
            .unwrap()
    })
    .await;
    assert!(seen.is_empty());
    assert!(
        matches!(&listing, crate::account::UidListing::GenerationChanged { observation } if observation.uid_validity == nz(5)),
        "{listing:?}"
    );
}

/// The typed read tells a delivered body, an expunged UID and a voided
/// generation apart.
#[tokio::test]
async fn fetch_raw_distinguishes_message_absent_and_generation() {
    let inbox = ContainerId("INBOX".into());
    let message = crate::account::ImapMessageRef::new(&inbox, nz(5), nz(7)).unwrap();
    let (body, _) = poller_transcript(
        POLL_SELECT,
        vec!["* 1 FETCH (UID 7 BODY[] {3}\r\nabc)\r\n"],
        |poller| {
            let message = message.clone();
            async move { poller.fetch_raw(&message).await.unwrap() }
        },
    )
    .await;
    assert_eq!(
        body,
        crate::account::RawMessage::Message(bytes::Bytes::from_static(b"abc"))
    );
    let (absent, _) = poller_transcript(POLL_SELECT, vec![""], |poller| {
        let message = message.clone();
        async move { poller.fetch_raw(&message).await.unwrap() }
    })
    .await;
    assert_eq!(absent, crate::account::RawMessage::Absent);
    let stale = crate::account::ImapMessageRef::new(&inbox, nz(4), nz(7)).unwrap();
    let (changed, seen) = poller_transcript(POLL_SELECT, vec![], |poller| async move {
        poller.fetch_raw(&stale).await.unwrap()
    })
    .await;
    assert!(seen.is_empty(), "a voided generation fetches nothing");
    assert_eq!(
        changed,
        crate::account::RawMessage::GenerationChanged { observed: nz(5) }
    );
}

/// A UIDNOTSTICKY folder is refused on the read too, before its (always
/// fresh) generation is judged, so a caller cannot loop on
/// `GenerationChanged`.
#[tokio::test]
async fn fetch_raw_refuses_a_uidnotsticky_folder() {
    let message =
        crate::account::ImapMessageRef::new(&ContainerId("INBOX".into()), nz(4), nz(7)).unwrap();
    let (outcome, seen) = poller_transcript(
        "* 4 EXISTS\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 9] ok\r\n* OK [UIDNOTSTICKY] non-persistent UIDs\r\n",
        vec![],
        |poller| async move { poller.fetch_raw(&message).await },
    )
    .await;
    assert!(seen.is_empty());
    assert_eq!(
        outcome.unwrap_err().kind(),
        &AccountErrorKind::Unsupported(AccountOperation::OpenRawRfc822)
    );
}

/// The raw-message lane carries a byte budget like every other body path.
///
/// Without one, a corrupt or adversarial server can answer a single
/// `BODY.PEEK[]` with an unbounded number of octets and this lane forwards
/// all of them. The budget is exercised at 3 bytes against a 5-byte body;
/// the reader refuses it at the literal marker before materializing the body;
/// the connection is retired because the unread literal cannot be skipped.
#[tokio::test]
async fn a_raw_message_read_stops_at_its_byte_budget() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
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
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 7 BODY[] {{5}}\r\nhello)\r\n{} OK FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
        server
    });

    let (tx, _rx) = tokio::sync::mpsc::channel(crate::account::STREAM_CAPACITY);
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::blob::run_fetch(
            &account,
            &folder,
            crate::types::UidValidity::new(5).expect("valid protocol test value"),
            7,
            bifrost_types::AccountOperation::OpenRawRfc822,
            3,
            crate::account::blob::UidPersistence::Session,
            &tx,
        ),
    )
    .await
    .expect("the budgeted read must finish");

    match outcome {
        Err(crate::account::blob::BlobError::Imap(crate::Error::ResultLimit(
            crate::error::ResultLimit::Fetch { limit, uid, .. },
        ))) => {
            assert_eq!(limit, 3);
            assert_eq!(
                uid,
                Some(crate::types::Uid::new(7).expect("valid protocol test value"))
            );
        }
        Err(crate::account::blob::BlobError::Imap(other)) => {
            panic!("unexpected imap error: {other:?}")
        }
        Err(crate::account::blob::BlobError::Account(err)) => {
            panic!("unexpected account error: {err:?}")
        }
        Err(crate::account::blob::BlobError::ChannelDropped) => panic!("the receiver was held"),
        Ok(_) => panic!("a body past the budget must not stream in full"),
    }
    let _server = script.await.unwrap();
}

/// A UID the shared operand cannot carry never reaches the wire, and never
/// completes as a successful empty read.
///
/// `decode_object_id` rejects UID 0 today, so this is reachable only by
/// calling the fetch core directly - which is the point: the invariant is
/// the operand's, not the decoder's. Before the operand was shared, this
/// lane skipped the FETCH and returned `Ok(())`, presenting "nothing was
/// asked" as "the message has no body".
#[tokio::test]
async fn a_uid_the_operand_cannot_carry_never_reaches_the_fetch() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
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
        server
    });

    let (tx, _rx) = tokio::sync::mpsc::channel(crate::account::STREAM_CAPACITY);
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::blob::run_fetch(
            &account,
            &folder,
            crate::types::UidValidity::new(5).expect("valid protocol test value"),
            0,
            bifrost_types::AccountOperation::OpenRawRfc822,
            1024,
            crate::account::blob::UidPersistence::Session,
            &tx,
        ),
    )
    .await
    .expect("the refused read must finish");

    match outcome {
        Err(crate::account::blob::BlobError::Account(err)) => assert!(
            matches!(
                err.kind(),
                bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                )
            ),
            "a UID the operand cannot carry is a client-side request defect: {err:?}"
        ),
        Ok(_) => panic!("a UID the server never saw must not complete as a successful read"),
        Err(_) => panic!("unexpected error variant"),
    }
    let _server = script.await.unwrap();
}
