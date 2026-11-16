use super::*;
use crate::account::encode_thread_id;
use crate::account::folder_registry::FolderRegistry;
use crate::account::test_support::scripted_account_parts;
use crate::connection::test_support::{driver_pair, preauth_greeting, read_line, respond, tag_of};
use crate::types::MailboxInfo;
use bifrost_types::AccountErrorKind;

#[test]
fn importance_high_sets_keyword_others_clear() {
    // High -> set `$important`; Normal/Low -> clear it. One STORE op,
    // never an expand-into-two.
    assert!(importance_sets_important_keyword(Importance::High));
    assert!(!importance_sets_important_keyword(Importance::Normal));
    assert!(!importance_sets_important_keyword(Importance::Low));
}

#[test]
fn convenience_keywords_map_onto_system_flags() {
    assert_eq!(imap_flag_for_keyword("$flagged"), Flag::Flagged);
    assert_eq!(imap_flag_for_keyword("$FLAGGED"), Flag::Flagged);
    assert_eq!(imap_flag_for_keyword("\\Flagged"), Flag::Flagged);
    assert_eq!(imap_flag_for_keyword("$answered"), Flag::Answered);
    assert_eq!(imap_flag_for_keyword("$seen"), Flag::Seen);
    // Everything else stays a keyword, verbatim: `$forwarded` and
    // `$MDNSent` have no system-flag equivalent.
    assert_eq!(
        imap_flag_for_keyword("$MDNSent"),
        Flag::from_imap_str("$MDNSent")
    );
    assert_eq!(
        imap_flag_for_keyword(IMPORTANCE_KEYWORD),
        Flag::from_imap_str("$important")
    );
}

// These transcripts pin the Result adapter at the actual engine boundary:
// a direct unguarded STORE would fail the command assertion, and expunging
// the conflicting UID would fail the delete transcript.
#[tokio::test]
async fn result_mutations_share_modseq_conflicts_and_committed_subsets() {
    for delete in [false, true] {
        let (conn, mut server) =
            driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS CONDSTORE")).await;
        let mut parts = scripted_account_parts(conn, 1);
        let folder = MailboxName::new("INBOX").unwrap();
        parts.folders = std::sync::Arc::new(FolderRegistry::from_lists(
            vec![MailboxInfo {
                name: folder.clone(),
                delimiter: Some('/'),
                ..Default::default()
            }],
            Vec::new(),
        ));
        for uid in [1, 2] {
            parts
                .folders
                .record_modseq(
                    &folder,
                    crate::types::UidValidity::new(7).expect("valid protocol test value"),
                    uid,
                    crate::types::ModSeq::new(99).expect("positive test MODSEQ"),
                )
                .unwrap();
        }
        let account = ImapAccount::new(parts);
        let script = tokio::spawn(async move {
            let select = read_line(&mut server).await;
            respond(&mut server, &format!(
                "* 2 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen \\Deleted)\r\n* OK [UIDVALIDITY 7] epoch\r\n* OK [HIGHESTMODSEQ 100] modseq\r\n{} OK selected\r\n",
                tag_of(&select),
            )).await;
            let store = read_line(&mut server).await;
            assert!(store.contains("UID STORE"), "{store}");
            assert!(store.contains("UNCHANGEDSINCE 99"), "{store}");
            respond(
                &mut server,
                &format!("{} OK [MODIFIED 2] partial\r\n", tag_of(&store),),
            )
            .await;
            if delete {
                let expunge = read_line(&mut server).await;
                assert!(expunge.contains("UID EXPUNGE 1\r\n"), "{expunge}");
                respond(&mut server, &format!("{} OK gone\r\n", tag_of(&expunge))).await;
            }
            server
        });
        let ids = [1, 2]
            .into_iter()
            .map(|uid| {
                DecodedObjectId::canonical(
                    folder.clone(),
                    crate::types::UidValidity::new(7).expect("nonzero epoch"),
                    crate::types::Uid::new(uid).expect("nonzero test UID"),
                )
            })
            .collect();
        let op = if delete {
            AccountOperation::RemoveFromContainer
        } else {
            AccountOperation::SetIsRead
        };
        let result = if delete {
            delete_messages(&account, ids, op).await
        } else {
            set_flag(&account, ids, Flag::Seen, true, op).await
        };
        let failure = match result {
            Err(failure) => failure,
            Ok(_) => panic!("MODIFIED member must fail the Result"),
        };
        let error = folder_progress_error(failure);
        assert_eq!(error.operation(), Some(op));
        assert!(matches!(
            error.kind(),
            AccountErrorKind::ConcurrencyConflict
        ));
        let steps = error.partial_commit().unwrap().steps();
        assert_eq!(steps.len(), 1);
        assert_eq!(
            steps[0].effect(),
            Some(if delete {
                StepEffect::Deleted
            } else {
                StepEffect::Updated
            })
        );
        assert_eq!(steps[0].targets().refs.len(), 1);
        assert_eq!(
            account
                .folders
                .modseq(
                    &folder,
                    crate::types::UidValidity::new(7).expect("valid protocol test value"),
                    1
                )
                .map(crate::types::ModSeq::get),
            None
        );
        assert_eq!(
            account
                .folders
                .modseq(
                    &folder,
                    crate::types::UidValidity::new(7).expect("valid protocol test value"),
                    2
                )
                .map(crate::types::ModSeq::get),
            Some(99)
        );
        let _server = script.await.unwrap();
    }
}

#[tokio::test]
async fn result_copy_preserves_live_epoch_commit_beside_stale_target() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
    let account = ImapAccount::new(scripted_account_parts(conn, 1));
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(&mut server, &format!(
            "* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",
            tag_of(&select),
        )).await;
        let copy = read_line(&mut server).await;
        assert!(copy.contains("UID COPY 2 "), "{copy}");
        respond(&mut server, &format!("{} OK copied\r\n", tag_of(&copy))).await;
        server
    });
    let folder = MailboxName::new("INBOX").unwrap();
    let result = copy_messages(
        &account,
        vec![
            DecodedObjectId::canonical(
                folder.clone(),
                crate::types::UidValidity::new(6).expect("nonzero epoch"),
                crate::types::Uid::new(1).expect("valid protocol test value"),
            ),
            DecodedObjectId::canonical(
                folder.clone(),
                crate::types::UidValidity::new(6).expect("nonzero epoch"),
                crate::types::Uid::new(3).expect("valid protocol test value"),
            ),
            DecodedObjectId::canonical(
                folder,
                crate::types::UidValidity::new(7).expect("nonzero epoch"),
                crate::types::Uid::new(2).expect("valid protocol test value"),
            ),
        ],
        &MailboxName::new("Archive").unwrap(),
        AccountOperation::AddToContainer,
    )
    .await;
    let failure = match result {
        Err(failure) => failure,
        Ok(_) => panic!("stale target must fail the Result"),
    };
    let error = folder_progress_error(failure);
    assert!(matches!(
        error.kind(),
        AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CursorInvalid)
    ));
    let record = error.partial_commit().unwrap();
    assert_eq!(record.primary().effect, None);
    assert_eq!(record.steps().len(), 2);
    assert!(
        record
            .steps()
            .iter()
            .any(|step| step.effect() == Some(StepEffect::Copied))
    );
    assert!(record.steps().iter().any(|step| step.effect().is_none()));
    let restart =
        bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(
            super::super::folder_scope(&MailboxName::new("INBOX").unwrap()),
        ));
    assert_eq!(error.leg_recovery(), &restart);
    // The committed live copy would duplicate on replay, so the operation
    // recovery stays the reconcile; the restart lives on the failed leg.
    match error.recovery() {
        bifrost_types::RecoveryClass::Reconcile(advice) => assert!(
            advice
                .guidance
                .actions
                .contains(&bifrost_types::ReconcileAction::ObserveOnly)
        ),
        other => panic!("expected reconcile, got {other:?}"),
    }
    assert!(
        record
            .plan()
            .directives
            .iter()
            .any(|d| bifrost_types::RecoveryClass::Engine(d.directive.clone()) == restart)
    );
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn partial_commit_copy_messages_second_folder_failure_keeps_first() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
    let account = ImapAccount::new(scripted_account_parts(conn, 1));
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(&mut server,&format!("* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",tag_of(&select))).await;
        let copy = read_line(&mut server).await;
        assert!(copy.contains("UID COPY"));
        respond(&mut server, &format!("{} OK copied\r\n", tag_of(&copy))).await;
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT"));
        respond(
            &mut server,
            &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&select)),
        )
        .await;
        server
    });
    let ids = ["A", "B"]
        .into_iter()
        .map(|name| {
            DecodedObjectId::canonical(
                MailboxName::new(name).unwrap(),
                crate::types::UidValidity::new(7).expect("nonzero epoch"),
                crate::types::Uid::new(1).expect("valid protocol test value"),
            )
        })
        .collect();
    let failure = match copy_messages(
        &account,
        ids,
        &MailboxName::new("Archive").unwrap(),
        AccountOperation::AddToContainer,
    )
    .await
    {
        Err(f) => f,
        Ok(_) => panic!("second folder refused"),
    };
    let error = folder_progress_error(failure);
    assert_eq!(error.partial_commit().unwrap().steps().len(), 1);
    assert_eq!(error.partial_commit().unwrap().primary().effect, None);
    assert!(error.recovery().requires_reconciliation());
    let _server = script.await.unwrap();
}

async fn composed_failure(delete: bool) -> AccountError {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
    let mut parts = scripted_account_parts(conn, 1);
    parts.capabilities.pim_methods.add_to_container = true;
    parts.capabilities.pim_methods.remove_from_container = true;
    parts.folders = std::sync::Arc::new(super::super::folder_registry::FolderRegistry::from_lists(
        ["INBOX", "Trash"]
            .into_iter()
            .map(|name| crate::types::MailboxInfo {
                name: MailboxName::new(name).unwrap(),
                ..Default::default()
            })
            .collect(),
        vec![],
    ));
    let account = ImapAccount::new(parts);
    let script = tokio::spawn(async move {
        loop {
            let line = read_line(&mut server).await;
            if line.contains("SELECT") {
                respond(&mut server,&format!("* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",tag_of(&line))).await;
            } else if line.contains("UID COPY") || line.contains("NOOP") {
                respond(&mut server, &format!("{} OK copied\r\n", tag_of(&line))).await;
            } else {
                assert!(line.contains("UID STORE"), "{line}");
                respond(
                    &mut server,
                    &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&line)),
                )
                .await;
                break;
            }
        }
        server
    });
    let thread = encode_thread_id(
        &MailboxName::new("INBOX").unwrap(),
        crate::types::UidValidity::new(7).expect("nonzero epoch"),
        &crate::types::set::UidSet::from_numbers([1]),
    );
    let result = if delete {
        delete_thread(account, thread, Some(ContainerId("INBOX".into()))).await
    } else {
        move_thread(
            account,
            thread,
            ContainerId("Archive".into()),
            Some(ContainerId("INBOX".into())),
        )
        .await
    };
    let error = result.unwrap_err();
    let _server = script.await.unwrap();
    error
}

#[tokio::test]
async fn partial_commit_move_thread_attaches_once_across_legs() {
    let error = composed_failure(false).await;
    assert_eq!(
        error.partial_commit().unwrap().steps()[0].effect(),
        Some(StepEffect::Copied)
    );
}

#[tokio::test]
async fn partial_commit_delete_thread_attaches_once() {
    let error = composed_failure(true).await;
    assert_eq!(error.partial_commit().unwrap().steps().len(), 1);
}

/// A bare server (no UIDPLUS, no IMAP4rev2) advertises removal, so
/// `move_thread` reaches the guarded plain-EXPUNGE fallback. A foreign
/// `\Deleted` message found by `UID SEARCH DELETED` stops it before any
/// EXPUNGE, and the failure keeps both the COPY and the MarkedDeleted step.
#[tokio::test]
async fn bare_server_move_thread_refuses_expunge_over_foreign_deleted() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let mut parts = scripted_account_parts(conn, 1);
    let profile = crate::types::ServerProfile::new(Vec::new(), Vec::new());
    parts.capabilities = super::super::capabilities::build_capabilities(
        &profile,
        &[],
        false,
        None,
        None,
        false,
        false,
        1,
    );
    assert!(parts.capabilities.pim_methods.remove_from_container);
    let account = ImapAccount::new(parts);
    let script = tokio::spawn(async move {
        loop {
            let line = read_line(&mut server).await;
            if line.contains("SELECT") {
                respond(&mut server,&format!("* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Deleted)\r\n* OK [UIDVALIDITY 7] epoch\r\n{} OK selected\r\n",tag_of(&line))).await;
            } else if line.contains("UID COPY") || line.contains("NOOP") {
                respond(&mut server, &format!("{} OK copied\r\n", tag_of(&line))).await;
            } else if line.contains("UID STORE") {
                respond(&mut server, &format!("{} OK stored\r\n", tag_of(&line))).await;
            } else {
                assert!(line.contains("UID SEARCH DELETED"), "{line}");
                respond(
                    &mut server,
                    &format!("* SEARCH 1 9\r\n{} OK searched\r\n", tag_of(&line)),
                )
                .await;
                break;
            }
        }
        server
    });
    let thread = encode_thread_id(
        &MailboxName::new("INBOX").unwrap(),
        crate::types::UidValidity::new(7).expect("nonzero epoch"),
        &crate::types::set::UidSet::from_numbers([1]),
    );
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        move_thread(
            account,
            thread,
            ContainerId("Archive".into()),
            Some(ContainerId("INBOX".into())),
        ),
    )
    .await
    .expect("no EXPUNGE may follow a foreign \\Deleted")
    .unwrap_err();
    let effects: Vec<_> = error
        .partial_commit()
        .unwrap()
        .steps()
        .iter()
        .map(bifrost_types::Step::effect)
        .collect();
    assert!(effects.contains(&Some(StepEffect::Copied)), "{effects:?}");
    assert!(
        effects.contains(&Some(StepEffect::MarkedDeleted)),
        "{effects:?}"
    );
    let _server = script.await.unwrap();
}

/// With nothing committed, an uncertain COPY is recorded (it may have
/// made a copy); a refused COPY and a preparation failure stay plain.
#[test]
fn partial_commit_lone_uncertain_write_leg_is_recorded() {
    let folder = MailboxName::new("INBOX").unwrap();
    let progress = |error: AccountError, stage| FolderProgress {
        done: Vec::new(),
        error,
        stage,
        targets: super::super::mutate::folder_targets(&folder, TargetRole::Subject),
        siblings: Vec::new(),
    };
    let leaf = |attempt| {
        super::super::account_error_with(
            crate::Error::timeout().with_attempt(attempt),
            super::super::error::ImapErrorContext::operation(AccountOperation::AddToContainer)
                .with_folder_scope(&folder),
        )
    };
    let dropped = leaf(bifrost_types::TransmissionState::InFlight);
    assert_eq!(Application::from_leaf(&dropped), Application::Uncertain);
    let error = folder_progress_error(progress(dropped.clone(), Some(StepEffect::Copied)));
    let record = error
        .partial_commit()
        .expect("an uncertain copy is recorded");
    assert_eq!(record.primary().application, Application::Uncertain);
    assert_eq!(record.plan().gating.len(), 1);
    assert!(
        folder_progress_error(progress(dropped, None))
            .partial_commit()
            .is_none()
    );
    let unsent = leaf(bifrost_types::TransmissionState::Unsent);
    assert!(
        folder_progress_error(progress(unsent, Some(StepEffect::Copied)))
            .partial_commit()
            .is_none()
    );
}
