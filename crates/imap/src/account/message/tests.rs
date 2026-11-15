use super::*;
use crate::account::containers::folder_from_container;
use crate::account::hydration::PREVIEW_FETCH_BYTES;
use crate::account::message_targets::{decoded_targets, resolved_targets};
use crate::account::test_support::scripted_account_parts;
use crate::connection::test_support::{driver_pair, preauth_greeting, read_line, respond, tag_of};
use crate::types::Flag;
use bifrost_types::container::MutationTarget;

#[test]
fn locally_malformed_message_errors_carry_the_calling_operation() {
    // Id / cursor / mailbox-name decode.
    let err = folder_from_container(
        &super::super::DavScopeIndex::default(),
        &ContainerId("INBOX\r\nNOOP".to_owned()),
        AccountOperation::BulkMove,
    )
    .expect_err("a CRLF-bearing mailbox name is not valid");
    assert_eq!(err.operation(), Some(AccountOperation::BulkMove));

    let err = decoded_targets(
        &MutationTarget::Message(ObjectId("not-an-imap-id".to_owned())),
        AccountOperation::SetIsRead,
    )
    .expect_err("a foreign object id must not decode");
    assert_eq!(err.operation(), Some(AccountOperation::SetIsRead));

    let Err(err) = pim_operand([0u32], AccountOperation::UpdateFlags) else {
        panic!("uid 0 cannot ride an operand");
    };
    assert_eq!(err.operation(), Some(AccountOperation::UpdateFlags));

    let err = valid_uids(
        vec![DecodedObjectId::canonical(
            MailboxName::new("INBOX").expect("valid mailbox"),
            crate::types::UidValidity::new(2).expect("nonzero epoch"),
            crate::types::Uid::new(5).expect("valid protocol test value"),
        )],
        crate::types::UidValidity::new(3).expect("valid protocol test value"),
        AccountOperation::BulkDestroy,
    )
    .expect_err("a UIDVALIDITY change is refused");
    assert_eq!(err.operation(), Some(AccountOperation::BulkDestroy));
}

#[test]
fn hydration_attrs_track_the_projection() {
    for projection in [
        HydrationProjection::Headers,
        HydrationProjection::Preview(64),
        HydrationProjection::Full,
        HydrationProjection::FullWithBlobs,
    ] {
        let attrs = attrs_for_hydration(projection);
        assert!(attrs.iter().any(|attr| matches!(attr, FetchAttr::Envelope)));
        assert!(attrs.iter().any(|attr| matches!(attr, FetchAttr::Uid)));
    }

    assert!(
        attrs_for_hydration(HydrationProjection::Headers)
            .iter()
            .all(|attr| !matches!(attr, FetchAttr::BodySection { .. })),
        "a headers-only hydration must not pull a body",
    );

    let preview = attrs_for_hydration(HydrationProjection::Preview(64));
    assert!(preview.iter().any(|attr| matches!(
        attr,
        FetchAttr::BodySection {
            peek: true,
            section: None,
            partial: Some((0, PREVIEW_FETCH_BYTES)),
        }
    )));

    let full = attrs_for_hydration(HydrationProjection::Full);
    assert!(full.iter().any(|attr| matches!(
        attr,
        FetchAttr::BodySection {
            peek: true,
            section: None,
            partial: None,
        }
    )));
}

#[test]
fn hydrated_message_maps_importance_from_the_important_keyword() {
    let with = crate::types::FetchResponse {
        uid: Some(2),
        flags: Some(vec![Flag::from_imap_str("$Important")]),
        ..Default::default()
    };
    let folder = MailboxName::new("INBOX").expect("valid mailbox");
    let message = fetch_to_message(
        &folder,
        crate::types::UidValidity::new(9).expect("valid protocol test value"),
        with,
        HydrationProjection::Headers,
    )
    .expect("uid present");
    assert_eq!(message.importance, Importance::High);
    assert_eq!(message.containers, vec![ContainerId("INBOX".to_owned())]);
    assert!(
        message.body_text.is_none(),
        "a headers projection carries no body"
    );

    let without = crate::types::FetchResponse {
        uid: Some(2),
        ..Default::default()
    };
    let message = fetch_to_message(
        &folder,
        crate::types::UidValidity::new(9).expect("valid protocol test value"),
        without,
        HydrationProjection::Headers,
    )
    .expect("uid present");
    assert_eq!(message.importance, Importance::Normal);
}

#[test]
fn full_hydration_decodes_multipart_into_text_html_and_attachments() {
    let fetch = crate::types::FetchResponse {
        uid: Some(2),
        body_sections: vec![crate::types::fetch::BodySection {
            section: String::new(),
            origin: None,
            data: Some(b"Subject: hi\r\nDate: Fri, 21 Nov 1997 09:55:06 -0600\r\nReferences: <a@test> <b@test>\r\nContent-Type: multipart/mixed; boundary=mix\r\n\r\n--mix\r\nContent-Type: multipart/alternative; boundary=alt\r\n\r\n--alt\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\ncaf=C3=A9\r\n--alt\r\nContent-Type: text/html\r\nContent-Transfer-Encoding: base64\r\n\r\nPGI+aHRtbDwvYj4=\r\n--alt--\r\n--mix\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename*=utf-8''report%20%C3%A6.pdf\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0=\r\n--mix--\r\n".to_vec().into()),
        }],
        ..Default::default()
    };
    let folder = MailboxName::new("INBOX").expect("valid mailbox");
    let message = fetch_to_message(
        &folder,
        crate::types::UidValidity::new(9).expect("valid protocol test value"),
        fetch,
        HydrationProjection::FullWithBlobs,
    )
    .expect("uid present");
    assert_eq!(message.body_text.as_deref(), Some("café"));
    assert_eq!(message.body_html.as_deref(), Some("<b>html</b>"));
    assert_eq!(message.references, ["a@test", "b@test"]);
    assert!(message.date.is_some());
    assert_eq!(message.attachments.len(), 1);
    let attachment = &message.attachments[0];
    assert_eq!(attachment.filename.as_deref(), Some("report æ.pdf"));
    assert_eq!(attachment.content_type.as_deref(), Some("application/pdf"));
    assert!(
        matches!(&attachment.source, AttachmentSource::Inline(bytes) if bytes.as_ref() == b"%PDF-")
    );
}

#[test]
fn preview_projection_extracts_text_from_a_multipart_prefix() {
    let fetch = crate::types::FetchResponse { uid: Some(2), body_sections: vec![crate::types::fetch::BodySection { section: String::new(), origin: Some(0), data: Some(b"Content-Type: multipart/alternative; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\npreview prose\r\n--x--\r\n".to_vec().into()) }], ..Default::default() };
    let folder = MailboxName::new("INBOX").expect("valid mailbox");
    let message = fetch_to_message(
        &folder,
        crate::types::UidValidity::new(9).expect("valid protocol test value"),
        fetch,
        HydrationProjection::Preview(200),
    )
    .expect("uid present");
    assert_eq!(message.body_text.as_deref(), Some("preview prose"));
    assert!(
        message
            .body_text
            .as_deref()
            .is_none_or(|body| !body.contains("--x"))
    );
    assert!(message.attachments.is_empty());
}

#[test]
fn preview_projection_requests_at_least_the_caller_limit() {
    assert!(
        attrs_for_hydration(HydrationProjection::Preview(256 * 1024))
            .iter()
            .any(|attr| matches!(
                attr,
                FetchAttr::BodySection {
                    partial: Some((0, 262_144)),
                    ..
                }
            ))
    );
}

#[test]
fn truncated_fetch_marks_the_message_incomplete() {
    let fetch = crate::types::FetchResponse { uid: Some(2), body_sections: vec![crate::types::fetch::BodySection { section: String::new(), origin: None, data: Some(b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: application/octet-stream\r\n\r\nprefix".to_vec().into()) }], ..Default::default() };
    let folder = MailboxName::new("INBOX").expect("valid mailbox");
    let message = fetch_to_message(
        &folder,
        crate::types::UidValidity::new(9).expect("valid protocol test value"),
        fetch,
        HydrationProjection::FullWithBlobs,
    )
    .expect("uid present");
    assert!(message.incomplete);
    assert_eq!(message.attachments.len(), 1);
    assert!(message.attachments[0].truncated);
}

#[tokio::test]
async fn fetched_server_thread_handles_resolve_current_members() {
    for (objectid, gmail, expected) in [
        (
            Some("T-objectid"),
            Some(1234),
            "UID SEARCH THREADID T-objectid",
        ),
        (None, Some(1234), "UID SEARCH X-GM-THRID 1234"),
    ] {
        let (conn, mut server) =
            driver_pair(&preauth_greeting("IMAP4rev1 OBJECTID X-GM-EXT-1")).await;
        let account = ImapAccount::new(scripted_account_parts(conn, 1));
        let folder = MailboxName::new("INBOX").unwrap();
        account
            .folders
            .replace_personal(vec![crate::types::MailboxInfo {
                name: folder.clone(),
                ..Default::default()
            }]);
        let fetch = crate::types::FetchResponse {
            uid: Some(1),
            flags: Some(Vec::new()),
            thread_id: objectid.map(|id| crate::types::validated::ObjectId::new(id).unwrap()),
            gmail_thread_id: gmail.map(crate::types::GmailThreadId::new),
            ..Default::default()
        };
        let inventory = super::super::inventory::fetch_to_inventory(
            &folder,
            crate::types::UidValidity::new(7).expect("valid protocol test value"),
            fetch.clone(),
            None,
        )
        .unwrap();
        // The same conversation seen in another folder and epoch carries
        // the same account-scoped id, so a consumer can group on it.
        let elsewhere = super::super::inventory::fetch_to_inventory(
            &MailboxName::new("Sent").unwrap(),
            crate::types::UidValidity::new(42).expect("valid protocol test value"),
            fetch.clone(),
            None,
        )
        .unwrap();
        let message = fetch_to_message(
            &folder,
            crate::types::UidValidity::new(7).expect("valid protocol test value"),
            fetch,
            HydrationProjection::Headers,
        )
        .unwrap();
        assert_eq!(inventory.thread_id, message.thread_id);
        assert_eq!(elsewhere.thread_id, message.thread_id);
        let thread = message.thread_id.unwrap();
        let script = tokio::spawn(async move {
            let select = read_line(&mut server).await;
            respond(&mut server, &format!(
                "* 2 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",
                tag_of(&select),
            )).await;
            let search = read_line(&mut server).await;
            assert!(search.contains(expected), "unexpected command: {search}");
            respond(
                &mut server,
                &format!("* SEARCH 1 9\r\n{} OK searched\r\n", tag_of(&search),),
            )
            .await;
            server
        });
        let ids = resolved_targets(
            &account,
            &MutationTarget::Thread(thread),
            ThreadScope::Account,
            AccountOperation::SetIsRead,
        )
        .await
        .unwrap();
        assert_eq!(
            ids.iter().map(|id| id.uid.get()).collect::<Vec<_>>(),
            [1, 9]
        );
        assert!(ids.iter().all(|id| id.folder == folder
            && id.uidvalidity
                == crate::types::UidValidity::new(7).expect("valid protocol test value")));
        let _server = script.await.unwrap();
    }
}
