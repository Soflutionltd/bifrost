use super::test_support::*;

#[tokio::test]
async fn sent_append_with_appenduid_reports_the_filed_copy() {
    let receipt = sent_append_receipt(Some("OK [APPENDUID 7 3] done")).await;
    let filed = crate::account::encode_object_id(
        &crate::types::MailboxName::new("Sent").unwrap(),
        crate::types::UidValidity::new(7).expect("nonzero epoch"),
        crate::types::Uid::new(3).expect("valid protocol test value"),
    );
    assert_eq!(receipt.id, filed);
    assert!(matches!(
        receipt.sent_copy,
        bifrost_types::SentCopy::Filed { id: Some(ref id) } if *id == filed
    ));
}

#[tokio::test]
async fn sent_append_without_appenduid_is_filed_but_unidentified() {
    let receipt = sent_append_receipt(Some("OK done")).await;
    assert_eq!(receipt.id.0, "imapmsgid1:m1@example.com");
    assert!(matches!(
        receipt.sent_copy,
        bifrost_types::SentCopy::Filed { id: None }
    ));
}

#[tokio::test]
async fn refused_sent_append_reports_the_copy_not_filed() {
    let receipt = sent_append_receipt(Some("NO [OVERQUOTA] full")).await;
    assert_eq!(receipt.id.0, "imapmsgid1:m1@example.com");
    assert!(
        matches!(
            receipt.sent_copy,
            bifrost_types::SentCopy::NotFiled {
                reason: bifrost_types::SentCopyNotFiled::AppendFailed,
                detail: Some(_),
            }
        ),
        "{:?}",
        receipt.sent_copy
    );
}

/// The connection drops after the whole message was written, so the server
/// may have stored it: appending again could duplicate the copy.
#[tokio::test]
async fn sent_append_cut_off_after_the_write_is_uncertain() {
    let receipt = sent_append_receipt(None).await;
    assert_eq!(receipt.id.0, "imapmsgid1:m1@example.com");
    assert!(
        matches!(receipt.sent_copy, bifrost_types::SentCopy::Uncertain { .. }),
        "{:?}",
        receipt.sent_copy
    );
}

#[tokio::test]
async fn sent_copy_without_a_sent_folder_or_request_writes_nothing() {
    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1 LITERAL+ UIDPLUS")).await;
    let account = scripted_account(conn, 1);
    let raw = bytes::Bytes::from_static(b"HELLO");
    let missing = crate::account::compose::append_to_sent_or_fallback(
        &account,
        raw.clone(),
        true,
        "m1@example.com",
    )
    .await;
    assert!(matches!(
        missing.sent_copy,
        bifrost_types::SentCopy::NotFiled {
            reason: bifrost_types::SentCopyNotFiled::NoSentFolder,
            ..
        }
    ));
    let declined =
        crate::account::compose::append_to_sent_or_fallback(&account, raw, false, "m1@example.com")
            .await;
    assert!(matches!(
        declined.sent_copy,
        bifrost_types::SentCopy::NotRequested
    ));
    assert_eq!(declined.id.0, "imapmsgid1:m1@example.com");
}

/// The wire drops the Bcc while the filed Sent copy keeps it; both carry the
/// renderer's single `Date` first; an explicit `save_to_sent` overrides a
/// declining default; the receipt id is the APPENDUID-derived copy id.
#[tokio::test]
async fn send_message_files_the_bcc_bearing_copy_and_transmits_without_it() {
    let mut request = text_request("a@example.com");
    request.bcc = vec![bifrost_types::Address::bare("b@example.com")];
    request.save_to_sent = Some(true);
    let sent = send_through_peers(
        SendCall::Message(Box::new(request)),
        &[
            ("a@example.com", "250 recipient\r\n"),
            ("b@example.com", "250 recipient\r\n"),
        ],
        false,
        SentPeer::Reply("OK [APPENDUID 7 3] filed"),
    )
    .await;
    let receipt = sent.outcome.expect("the message was sent");
    let rendered = sent.rendered.unwrap();
    let appended = sent.appended.unwrap();
    assert_eq!(appended, rendered.sent_copy.clone().unwrap());
    assert!(
        header_block(&appended).contains("\r\nBcc: b@example.com"),
        "the Sent copy keeps the Bcc: {}",
        header_block(&appended)
    );
    assert!(
        !header_block(&rendered.raw).contains("Bcc:"),
        "the wire drops the Bcc: {}",
        header_block(&rendered.raw)
    );
    assert_eq!(sole_first_date(&rendered.raw), sole_first_date(&appended));
    let filed = crate::account::encode_object_id(
        &crate::types::MailboxName::new("Sent").unwrap(),
        crate::types::UidValidity::new(7).expect("nonzero epoch"),
        crate::types::Uid::new(3).expect("valid protocol test value"),
    );
    assert_eq!(receipt.id, filed);
    assert!(matches!(
        receipt.sent_copy,
        bifrost_types::SentCopy::Filed { id: Some(ref id) } if *id == filed
    ));
    assert!(receipt.recipients_not_accepted.is_empty());
}

/// A partial accept is a success that names the refused recipient, stamped
/// with the call's operation, and still files the Sent copy. With no Bcc the
/// copy is the transmitted body itself; the account default requests it; no
/// APPENDUID leaves the copy unidentified and the id the Message-ID.
#[tokio::test]
async fn send_message_partial_accept_reports_the_refused_recipient_and_still_files() {
    let mut request = text_request("a@example.com");
    request.cc = vec![bifrost_types::Address::bare("b@example.com")];
    let sent = send_through_peers(
        SendCall::Message(Box::new(request)),
        &[
            ("a@example.com", "250 recipient\r\n"),
            ("b@example.com", "550 no such user\r\n"),
        ],
        true,
        SentPeer::Reply("OK filed"),
    )
    .await;
    let receipt = sent.outcome.expect("a recipient accepted the message");
    let rendered = sent.rendered.unwrap();
    assert!(rendered.sent_copy.is_none());
    assert_eq!(sent.appended.unwrap(), rendered.raw);
    assert_refused(&receipt, "b@example.com");
    assert_eq!(receipt.id.0, format!("imapmsgid1:{}", rendered.message_id));
    assert!(matches!(
        receipt.sent_copy,
        bifrost_types::SentCopy::Filed { id: None }
    ));
}

/// The caller's bytes go out as given (Bcc stripped, `Date` never
/// restamped), the Sent copy is the verbatim original with its Bcc, and a
/// partial accept rides the receipt.
#[tokio::test]
async fn send_raw_message_transmits_as_given_and_files_the_verbatim_original() {
    let sent = send_through_peers(
        SendCall::Raw {
            raw: RAW_WITH_BCC,
            wire: RAW_ON_THE_WIRE,
            save_to_sent: Some(true),
        },
        &[
            ("a@example.com", "250 recipient\r\n"),
            ("b@example.com", "550 no such user\r\n"),
        ],
        false,
        SentPeer::Reply("OK filed"),
    )
    .await;
    let receipt = sent.outcome.expect("a recipient accepted the message");
    assert_eq!(sent.appended.unwrap(), RAW_WITH_BCC.as_bytes());
    assert_refused(&receipt, "b@example.com");
    assert_eq!(receipt.id.0, "imapmsgid1:r1@example.com");
    assert!(matches!(
        receipt.sent_copy,
        bifrost_types::SentCopy::Filed { id: None }
    ));
}

/// A declined Sent copy writes nothing to IMAP, whether the call declines
/// over a requesting default or leaves it to a declining one.
#[tokio::test]
async fn send_raw_message_declined_copy_writes_nothing() {
    for (save_to_sent, default) in [(Some(false), true), (None, false)] {
        let sent = send_through_peers(
            SendCall::Raw {
                raw: RAW_WITH_BCC,
                wire: RAW_ON_THE_WIRE,
                save_to_sent,
            },
            &[
                ("a@example.com", "250 recipient\r\n"),
                ("b@example.com", "250 recipient\r\n"),
            ],
            default,
            SentPeer::Silent,
        )
        .await;
        let receipt = sent.outcome.expect("the message was sent");
        assert!(matches!(
            receipt.sent_copy,
            bifrost_types::SentCopy::NotRequested
        ));
        assert_eq!(receipt.id.0, "imapmsgid1:r1@example.com");
    }
}

/// A refused Sent APPEND after a committed send is still `Ok`, the copy
/// reported not filed, SMTP not re-driven.
#[tokio::test]
async fn send_message_refused_sent_append_is_still_a_success() {
    let request = text_request("a@example.com");
    let sent = send_through_peers(
        SendCall::Message(Box::new(request)),
        &[("a@example.com", "250 recipient\r\n")],
        true,
        SentPeer::Reply("NO [OVERQUOTA] full"),
    )
    .await;
    let receipt = sent.outcome.expect("the message was sent");
    assert!(
        matches!(
            receipt.sent_copy,
            bifrost_types::SentCopy::NotFiled {
                reason: bifrost_types::SentCopyNotFiled::AppendFailed,
                detail: Some(_),
            }
        ),
        "{:?}",
        receipt.sent_copy
    );
}

/// A Sent APPEND cut off after its literal is `Uncertain` on a successful
/// receipt, SMTP not re-driven.
#[tokio::test]
async fn send_raw_message_cut_off_sent_append_is_uncertain() {
    let sent = send_through_peers(
        SendCall::Raw {
            raw: RAW_WITH_BCC,
            wire: RAW_ON_THE_WIRE,
            save_to_sent: None,
        },
        &[
            ("a@example.com", "250 recipient\r\n"),
            ("b@example.com", "250 recipient\r\n"),
        ],
        true,
        SentPeer::HangUp,
    )
    .await;
    let receipt = sent.outcome.expect("the message was sent");
    assert!(
        matches!(receipt.sent_copy, bifrost_types::SentCopy::Uncertain { .. }),
        "{:?}",
        receipt.sent_copy
    );
}

/// With every recipient refused nothing was sent: the call is `Err`, stamped
/// `Send`, and no Sent copy is attempted.
#[tokio::test]
async fn send_raw_message_with_every_recipient_refused_files_nothing() {
    let sent = send_through_peers(
        SendCall::Raw {
            raw: RAW_WITH_BCC,
            wire: RAW_ON_THE_WIRE,
            save_to_sent: Some(true),
        },
        &[
            ("a@example.com", "550 no such user\r\n"),
            ("b@example.com", "550 no such user\r\n"),
        ],
        true,
        SentPeer::Silent,
    )
    .await;
    let Err(err) = sent.outcome else {
        panic!("no recipient accepted the message");
    };
    assert_eq!(err.operation(), Some(AccountOperation::Send));
}
