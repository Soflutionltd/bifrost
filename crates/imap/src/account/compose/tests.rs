use super::*;
use crate::account::test_support::scripted_account_parts;
use crate::connection::test_support::{driver_pair, preauth_greeting, read_line, respond, tag_of};
use crate::types::MailboxName;
use bifrost_types::StepEffect;

#[test]
fn send_as_rejected_unsupported() {
    let mut request = SendRequest::default();
    request.send_as = Some(bifrost_types::SendAs::As(bifrost_types::PrincipalId(
        "shared@contoso.com".to_string(),
    )));
    let err = send_as_guard(&request).expect("send_as request must be rejected");
    assert!(matches!(
        err.kind(),
        bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
    ));
    assert_eq!(err.operation(), Some(AccountOperation::Send));

    // A personal send passes the guard.
    assert!(send_as_guard(&SendRequest::default()).is_none());
}

#[test]
fn scheduled_send_restamp_preserves_unsupported_kind() {
    // The smtp boundary already produces `Unsupported(Send)` for an
    // unsupporting relay (brick 4.6a). The IMAP send path re-stamps
    // only the operation/protocol via `restamp`; it must not change
    // the kind. Pin that the kind survives unchanged.
    let smtp_derived = crate::account::error::test_support::smtp_unsupported();

    let restamped = restamp(smtp_derived, AccountOperation::Send);
    assert_eq!(
        restamped.kind(),
        &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Send)
    );
    assert_eq!(restamped.operation(), Some(AccountOperation::Send));
}

#[tokio::test]
async fn partial_commit_draft_discard_expunge_failure_marks_deleted() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
    let mut parts = scripted_account_parts(conn, 1);
    parts.capabilities.pim_methods.draft_discard = true;
    let account = ImapAccount::new(parts);
    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        respond(&mut server,&format!("* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Deleted)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",tag_of(&line))).await;
        let line = read_line(&mut server).await;
        assert!(line.contains("UID STORE"));
        respond(&mut server, &format!("{} OK stored\r\n", tag_of(&line))).await;
        let line = read_line(&mut server).await;
        assert!(line.contains("UID EXPUNGE"));
        respond(
            &mut server,
            &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&line)),
        )
        .await;
        server
    });
    let id = encode_object_id(
        &MailboxName::new("Drafts").unwrap(),
        crate::types::UidValidity::new(7).expect("nonzero epoch"),
        crate::types::Uid::new(1).expect("valid protocol test value"),
    );
    let error = draft_discard(account, DraftHandle(id.0)).await.unwrap_err();
    assert_eq!(
        error.partial_commit().unwrap().steps()[0].effect(),
        Some(StepEffect::MarkedDeleted)
    );
    let _server = script.await.unwrap();
}
