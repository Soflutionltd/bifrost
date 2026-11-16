use super::*;

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
    assert_eq!(
        mailbox.uid_validity,
        Some(crate::types::UidValidity::new(42).expect("valid protocol test value"))
    );
    assert_eq!(
        mailbox.uid_next,
        Some(crate::types::Uid::new(100).expect("valid protocol test value"))
    );
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
    assert_eq!(
        mailbox.uid_validity,
        Some(crate::types::UidValidity::new(9).expect("valid protocol test value"))
    );
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
    assert_eq!(zero.modseq().map(crate::types::ModSeq::get), None);
    assert!(matches!(
        zero.modseq_support,
        crate::types::mailbox::ModSeqSupport::NoModSeq
    ));

    let real = build_selected_mailbox(
        &[ok_code(ResponseCode::HighestModSeq(77))],
        &tagged(None),
        false,
    );
    assert_eq!(real.modseq().map(crate::types::ModSeq::get), Some(77));
    assert!(real.modseq().is_some());

    let none = build_selected_mailbox(&[ok_code(ResponseCode::NoModSeq)], &tagged(None), false);
    assert!(matches!(
        none.modseq_support,
        crate::types::mailbox::ModSeqSupport::NoModSeq
    ));
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
    assert_eq!(mailbox.vanished[0].start(), 1);
    assert_eq!(mailbox.vanished[0].end(), Some(3));
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
    let mailbox = build_selected_mailbox(
        selected_mailbox_effective_responses(&untagged),
        &tagged(None),
        false,
    );
    assert_eq!(mailbox.exists, 4);
    assert_eq!(
        mailbox.uid_validity,
        Some(crate::types::UidValidity::new(2).expect("valid protocol test value"))
    );
}

#[test]
fn selection_mismatch_detects_missing_subscribed() {
    let plain = mailbox_info(Vec::new());
    assert!(is_notify_selection_mismatch(&plain, &["SUBSCRIBED".into()]));

    let subscribed = mailbox_info(vec![MailboxAttribute::Subscribed]);
    assert!(!is_notify_selection_mismatch(
        &subscribed,
        &["SUBSCRIBED".into()]
    ));
}

#[test]
fn selection_mismatch_accepts_childinfo_under_recursivematch() {
    // RFC 5258 Section 3.5: a parent with subscribed children is returned
    // without \Subscribed but with CHILDINFO.
    let mut info = mailbox_info(Vec::new());
    info.child_info = vec!["SUBSCRIBED".to_owned()];
    assert!(!is_notify_selection_mismatch(
        &info,
        &["SUBSCRIBED".into(), "RECURSIVEMATCH".into()]
    ));
    assert!(
        is_notify_selection_mismatch(&info, &["SUBSCRIBED".into()]),
        "CHILDINFO only excuses the missing attribute under RECURSIVEMATCH"
    );
}

#[test]
fn selection_mismatch_checks_remote_and_special_use() {
    let plain = mailbox_info(Vec::new());
    assert!(is_notify_selection_mismatch(&plain, &["REMOTE".into()]));
    assert!(is_notify_selection_mismatch(
        &plain,
        &["SPECIAL-USE".into()]
    ));

    let remote = mailbox_info(vec![MailboxAttribute::Remote]);
    assert!(!is_notify_selection_mismatch(&remote, &["remote".into()]));

    let archive = mailbox_info(vec![MailboxAttribute::Archive]);
    assert!(!is_notify_selection_mismatch(
        &archive,
        &["SPECIAL-USE".into()]
    ));
}

#[test]
fn selection_mismatch_with_no_options_is_never_a_mismatch() {
    assert!(!is_notify_selection_mismatch(
        &mailbox_info(Vec::new()),
        &[]
    ));
}

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
    assert_eq!(
        mailbox.uid_validity,
        Some(crate::types::UidValidity::new(4242).expect("valid protocol test value"))
    );
    assert_eq!(
        mailbox.uid_next,
        Some(crate::types::Uid::new(9).expect("valid protocol test value"))
    );
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

    assert!(
        matches!(
            err,
            Error::ServerStatus(crate::error::ServerStatus::No { .. })
        ),
        "got {err:?}"
    );
    assert_eq!(conn.session_state(), SessionState::Authenticated);
    assert!(conn.is_alive(), "a tagged NO leaves the wire reusable");
}

/// A SELECT refused before the first byte arms nothing. The pending-selection
/// flag used to be set before encoding, so a pre-wire refusal left it behind
/// and the NEXT command's tagged OK completed a selection the server never
/// made, flipping the session to Selected. QRESYNC operands are now validated
/// at construction, so the SELECT encoder has no refusal left; the pre-wire
/// refusal exercised here is the admission check for an un-ENABLEd QRESYNC.
#[tokio::test(start_paused = true)]
async fn a_refused_select_leaves_no_pending_selection_behind() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 ENABLE CONDSTORE QRESYNC",
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
    // QRESYNC is advertised but never ENABLEd, so admission refuses the
    // SELECT before writing anything.
    let refused = conn
        .submit_regular(
            Command::Select {
                mailbox: MailboxName::new("INBOX").unwrap(),
                condstore: false,
                qresync: Some(QresyncParams::from_raw(1, 1, None, None).unwrap()),
            },
            crate::connection::dispatch::TaggedOkConsumer::default(),
            None,
        )
        .await
        .expect_err("an un-ENABLEd QRESYNC SELECT is refused");
    assert!(
        matches!(
            refused,
            Error::LocalRefusal(crate::error::LocalRefusal::InvalidState(_))
        ),
        "{refused:?}"
    );

    conn.noop(Duration::from_secs(5)).await.expect("NOOP");
    assert_eq!(
        conn.session_state(),
        SessionState::Authenticated,
        "a refused SELECT must not leave a selection for the next OK to complete"
    );
    let _server = script.await.expect("NOOP transcript");
}

#[test]
fn selected_mailbox_modseq_denial_wins_in_both_orders() {
    for denial in [ResponseCode::NoModSeq, ResponseCode::HighestModSeq(0)] {
        for codes in [
            vec![ResponseCode::HighestModSeq(77), denial.clone()],
            vec![denial.clone(), ResponseCode::HighestModSeq(77)],
        ] {
            let responses: Vec<_> = codes.into_iter().map(ok_code).collect();
            let mailbox = build_selected_mailbox(&responses, &tagged(None), false);
            assert_eq!(mailbox.modseq(), None);
            assert!(matches!(
                mailbox.modseq_support,
                crate::types::mailbox::ModSeqSupport::NoModSeq
            ));
        }
        let mailbox = build_selected_mailbox(
            &[ok_code(ResponseCode::HighestModSeq(77))],
            &tagged(Some(denial)),
            false,
        );
        assert_eq!(mailbox.modseq(), None);
    }
}

#[test]
fn selected_mailbox_zero_identity_values_are_absent() {
    let mailbox = build_selected_mailbox(
        &[
            ok_code(ResponseCode::UidValidity(0)),
            ok_code(ResponseCode::UidNext(0)),
        ],
        &tagged(None),
        false,
    );
    assert_eq!(mailbox.uid_validity, None);
    assert_eq!(mailbox.uid_next, None);
}
