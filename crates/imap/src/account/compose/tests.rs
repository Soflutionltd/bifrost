use super::*;
use crate::account::test_support::scripted_account_parts;
use crate::connection::test_support::{driver_pair, preauth_greeting, read_line, respond, tag_of};
use bifrost_types::StepEffect;

#[test]
fn send_as_rejected_unsupported() {
    let mut request = SendRequest::default();
    request.send_as = Some(bifrost_types::SendAs::As(bifrost_types::PrincipalId(
        "shared@contoso.com".to_string(),
    )));
    let err = send_as_guard(&request).expect("send_as request must be rejected");
    assert_eq!(
        err.unsupported_reason(),
        Some(bifrost_types::UnsupportedReason::SendAsUnavailable)
    );
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
    // only the operation via `restamp`; it must not change
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
    let id = ObjectId("imap2:06:Drafts:007:001".into());
    let error = draft_discard(account, DraftHandle(id.0.clone()))
        .await
        .unwrap_err();
    assert_eq!(
        error.partial_commit().unwrap().steps()[0].effect(),
        Some(StepEffect::MarkedDeleted)
    );
    assert_eq!(
        error.partial_commit().unwrap().steps()[0].targets().refs[0].scope,
        bifrost_types::ErrorScope::Message { id },
    );
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn draft_actions_refuse_ordinary_messages_before_any_write() {
    use bifrost_smtp::transport::smtp::ScriptedConnector;

    for send in [false, true] {
        let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
        let mut parts = scripted_account_parts(conn, 1);
        parts.capabilities.pim_methods.draft_discard = true;
        let cfg = crate::account::SmtpSubmissionConfig::new(
            "smtp.invalid",
            crate::account::SubmissionTls::None,
            bifrost_types::Address::bare("user@example.com"),
        );
        let submission = super::super::submission::SubmissionTransport::build_with_connector(
            &cfg,
            &crate::types::Credentials::password("user", "pass"),
            None,
            bifrost_net::BandwidthBudget::default(),
            Some(ScriptedConnector::new(|| {
                panic!("non-draft must not submit")
            })),
        )
        .unwrap();
        parts.submission = Some(std::sync::Arc::new(submission));
        let account = ImapAccount::new(parts);
        // A closed in-memory peer makes accidental wire access fail as transport,
        // rather than hang. Only a local malformed-input refusal passes.
        drop(server);
        let handle = DraftHandle("imap1:5:INBOX:7:1".into());
        let error = if send {
            draft_send(account, handle).await.unwrap_err()
        } else {
            draft_discard(account, handle).await.unwrap_err()
        };
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(
            error.operation(),
            Some(if send {
                AccountOperation::DraftSend
            } else {
                AccountOperation::DraftDiscard
            }),
        );
        assert!(error.partial_commit().is_none());
    }
}
