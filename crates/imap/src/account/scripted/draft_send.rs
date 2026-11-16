use super::test_support::*;

/// The draft was marked deleted but the expunge failed: the send still
/// succeeds, and the discard failure is attributed to `DraftDiscard` with the
/// committed MarkedDeleted step: the follow-up is `draft_discard`, never a
/// re-driven send.
#[tokio::test]
async fn draft_send_reports_a_failed_expunge_with_its_partial_commit() {
    let receipt = draft_send_with_discard("OK stored", Some("NO [UNAVAILABLE] busy"))
        .await
        .expect("the message was sent");
    let error = assert_discard_failure(&receipt);
    assert_eq!(
        error
            .partial_commit()
            .expect("marked deleted is recorded")
            .steps()[0]
            .effect(),
        Some(bifrost_types::StepEffect::MarkedDeleted)
    );
}

/// A refused STORE committed nothing, so the discard failure is a plain error.
#[tokio::test]
async fn draft_send_reports_a_refused_discard_without_a_partial_commit() {
    let receipt = draft_send_with_discard("NO [UNAVAILABLE] busy", None)
        .await
        .expect("the message was sent");
    assert!(assert_discard_failure(&receipt).partial_commit().is_none());
}

/// A clean discard leaves the receipt without a discard failure.
#[tokio::test]
async fn draft_send_with_a_clean_discard_reports_none() {
    let receipt = draft_send_with_discard("OK stored", Some("OK expunged"))
        .await
        .expect("the message was sent");
    assert!(receipt.draft_discard.is_none());
}

/// Without UIDPLUS, `draft_send` still discards: `UID SEARCH DELETED` finds
/// only the draft just marked, so the plain EXPUNGE runs and the receipt is
/// clean.
#[tokio::test]
async fn draft_send_without_uidplus_discards_through_the_guarded_expunge() {
    let tail = DiscardTail::Fallback {
        deleted: "1",
        expunge: true,
    };
    let receipt = draft_send_scripted("IMAP4rev1", "OK stored", tail)
        .await
        .expect("the message was sent");
    assert!(receipt.draft_discard.is_none());
}

/// Without UIDPLUS and with another `\Deleted` message in Drafts, no EXPUNGE
/// is sent (the script would hang the send past its timeout): the draft stays
/// flagged, and the receipt carries an `Unsupported(DraftDiscard)` failure with
/// the MarkedDeleted step.
#[tokio::test]
async fn draft_send_without_uidplus_never_expunges_a_foreign_deleted_message() {
    let tail = DiscardTail::Fallback {
        deleted: "1 9",
        expunge: false,
    };
    let receipt = draft_send_scripted("IMAP4rev1", "OK stored", tail)
        .await
        .expect("the message was sent");
    let error = assert_discard_failure(&receipt);
    assert_eq!(
        error.kind(),
        &AccountErrorKind::Unsupported(AccountOperation::DraftDiscard)
    );
    assert_eq!(
        error
            .partial_commit()
            .expect("marked deleted is recorded")
            .steps()[0]
            .effect(),
        Some(bifrost_types::StepEffect::MarkedDeleted)
    );
}

/// The follow-up a failed post-send discard names is reachable on a bare
/// server: `draft_discard`, with the capabilities a server without UIDPLUS or
/// IMAP4rev2 really gets, goes to the wire and completes once the foreign
/// `\Deleted` message is gone.
#[tokio::test]
async fn draft_discard_on_a_bare_server_retries_a_failed_post_send_discard() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let profile = crate::types::ServerProfile::new(Vec::new(), Vec::new());
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: crate::account::capabilities::build_capabilities(
            &profile,
            &[],
            false,
            None,
            None,
            true,
            false,
            1,
        ),
        ..scripted_account_parts(conn, 1)
    });
    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("SELECT"), "expected SELECT, got {line}");
        respond(
            &mut server,
            &format!(
                "* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Deleted)\r\n\
                 * OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",
                tag_of(&line)
            ),
        )
        .await;
        let line = read_line(&mut server).await;
        assert!(line.contains("UID STORE"), "expected UID STORE, got {line}");
        respond(&mut server, &format!("{} OK stored\r\n", tag_of(&line))).await;
        let line = read_line(&mut server).await;
        assert!(
            line.contains("UID SEARCH DELETED"),
            "expected UID SEARCH DELETED, got {line}"
        );
        respond(
            &mut server,
            &format!("* SEARCH 1\r\n{} OK searched\r\n", tag_of(&line)),
        )
        .await;
        let line = read_line(&mut server).await;
        assert!(
            line.ends_with(" EXPUNGE\r\n") && !line.contains("UID"),
            "expected a plain EXPUNGE, got {line}"
        );
        respond(
            &mut server,
            &format!("* 1 EXPUNGE\r\n{} OK expunged\r\n", tag_of(&line)),
        )
        .await;
        server
    });
    let draft = crate::account::encode_object_id(
        &crate::types::MailboxName::new("Drafts").unwrap(),
        crate::types::UidValidity::new(7).expect("nonzero epoch"),
        crate::types::Uid::new(1).expect("valid protocol test value"),
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::compose::draft_discard(account, bifrost_types::DraftHandle(draft.0)),
    )
    .await
    .expect("draft_discard must finish")
    .expect("the retried discard completes");
    let _server = script.await.unwrap();
}
