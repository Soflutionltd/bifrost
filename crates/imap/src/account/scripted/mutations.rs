use super::test_support::*;

#[tokio::test]
async fn mutation_flushes_at_target_buffer_boundary_before_input_closes() {
    use bifrost_types::{FlagOp, SyncEvent};
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("SELECT \"INBOX\""),
            "expected SELECT, got {select}"
        );
        respond(&mut server, &format!(
            "* FLAGS (\\Seen)\r\n* 256 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 257] ok\r\n{} OK [READ-WRITE] done\r\n",
            tag_of(&select),
        )).await;
        let store = read_line(&mut server).await;
        assert!(
            store.contains("UID STORE"),
            "expected bounded-window STORE, got {store}"
        );
        respond(&mut server, &format!("{} OK done\r\n", tag_of(&store))).await;
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
    let mut output = crate::account::mutate::mutation_stream(
        account,
        ids,
        crate::account::mutate::MutationKind::Flags(FlagOp::Add(bifrost_types::FlagSet::from([
            "$flagged".to_owned(),
        ]))),
    );
    let first = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the full mutation buffer must flush before input closes")
        .expect("one output batch");
    assert!(
        matches!(&first, SyncEvent::Batch(batch) if batch.items.iter().all(|item| !matches!(item, bifrost_types::ItemOutcome::Uncertain(_)))),
        "SELECT failed before the boundary STORE: {first:?}"
    );
    drop(input_tx);
    script.await.unwrap();
}

/// A folder-level mutation failure raised before any outcome was minted -
/// here a refused SELECT - is `Failed`, because the mutation command was
/// never sent. The SELECT's acknowledged attempt is restamped `Unsent` for
/// the operation, and every requested id still receives exactly one result.
#[tokio::test]
async fn mutation_select_failure_reports_every_id_failed_once_as_unsent() {
    use bifrost_types::{FlagOp, ItemOutcome, SyncEvent};
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let first_id = crate::account::encode_object_id(
        &folder,
        crate::types::UidValidity::new(5).expect("nonzero epoch"),
        crate::types::Uid::new(3).expect("valid protocol test value"),
    );
    let second_id = crate::account::encode_object_id(
        &folder,
        crate::types::UidValidity::new(5).expect("nonzero epoch"),
        crate::types::Uid::new(4).expect("valid protocol test value"),
    );

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("SELECT \"INBOX\""),
            "expected SELECT, got {select}"
        );
        respond(
            &mut server,
            &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&select)),
        )
        .await;
        server
    });

    let ids: bifrost_types::AccountStream<bifrost_types::ObjectId> =
        Box::pin(futures::stream::iter(vec![
            first_id.clone(),
            second_id.clone(),
        ]));
    let mut output = crate::account::mutate::mutation_stream(
        account,
        ids,
        crate::account::mutate::MutationKind::Flags(FlagOp::Add(bifrost_types::FlagSet::from([
            "$flagged".to_owned(),
        ]))),
    );

    let mut failed = Vec::new();
    let mut other = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("stream must make progress")
    {
        match event {
            SyncEvent::Batch(batch) => {
                for outcome in batch.items {
                    match outcome {
                        ItemOutcome::Failed(item) => failed.push((item.item.0.clone(), item.error)),
                        ItemOutcome::Uncertain(item) => other.push(item.item.0.clone()),
                        ItemOutcome::Succeeded(item) => other.push(item.item.0.clone()),
                    }
                }
            }
            SyncEvent::Done(_) => break,
            unexpected => panic!("unexpected event: {unexpected:?}"),
        }
    }

    failed.sort_by(|left, right| left.0.cmp(&right.0));
    let mut expected = vec![first_id.0.clone(), second_id.0.clone()];
    expected.sort();
    assert_eq!(
        failed.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        expected,
        "every requested id owes exactly one failed outcome"
    );
    assert!(other.is_empty(), "no id may reach a second lane: {other:?}");
    for (_, error) in failed {
        assert_eq!(
            error.operation(),
            Some(bifrost_types::AccountOperation::UpdateFlags)
        );
        assert!(
            error.chain().iter().any(|cause| matches!(
                cause,
                bifrost_types::Cause::Attempt(attempt)
                    if attempt.transmission_state == bifrost_types::TransmissionState::Unsent
            )),
            "the mutation was never sent: {error:?}"
        );
    }
    let _server = script.await.unwrap();
}

/// A shared-folder SELECT denial is a scoped revocation, not an account
/// authorization loss. Keep answering later ids, including the id beyond the
/// first target window, so the stream's per-id accounting remains complete.
#[tokio::test]
async fn shared_folder_select_denial_is_scoped_and_later_mutation_targets_continue() {
    use bifrost_types::{
        AccountErrorKind, CursorScope, ErrorScope, FlagOp, FolderId, ItemOutcome, PrincipalId,
        SyncEvent, SyncStateErrorKind,
    };
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let shared = crate::types::MailboxName::new("Shared/Denied").unwrap();
    let personal = crate::types::MailboxName::new("Work").unwrap();
    let folders = FolderRegistry::from_lists(
        vec![crate::types::MailboxInfo {
            name: personal.clone(),
            ..Default::default()
        }],
        vec![crate::account::folder_registry::SharedFolderEntry {
            info: crate::types::MailboxInfo {
                name: shared.clone(),
                ..Default::default()
            },
            owner: PrincipalId("alice".to_owned()),
            rights: None,
            namespace_prefix: "Shared/".to_owned(),
        }],
    );
    let account = scripted_sync_account(conn, 1, false, None, folders);
    let shared_id = crate::account::encode_object_id(
        &shared,
        crate::types::UidValidity::new(5).expect("nonzero epoch"),
        crate::types::Uid::new(3).expect("valid protocol test value"),
    );
    let personal_ids: Vec<_> = (1..=crate::account::TARGET_BUFFER_ITEMS)
        .map(|uid| {
            crate::account::encode_object_id(
                &personal,
                crate::types::UidValidity::new(5).expect("nonzero epoch"),
                crate::types::Uid::new(u32::try_from(uid).unwrap()).expect("nonzero test UID"),
            )
        })
        .collect();
    let mut submitted = vec![shared_id.clone()];
    submitted.extend(personal_ids.iter().cloned());
    let mut expected_ids: Vec<_> = submitted.iter().map(|id| id.0.clone()).collect();

    let script = tokio::spawn(async move {
        let denied_select = read_line(&mut server).await;
        assert!(
            denied_select.contains("SELECT \"Shared/Denied\""),
            "expected shared-folder SELECT, got {denied_select}"
        );
        respond(
            &mut server,
            &format!("{} NO [NOPERM] access revoked\r\n", tag_of(&denied_select)),
        )
        .await;

        for expected_uids in ["1:255", "256"] {
            let select = read_line(&mut server).await;
            assert!(
                select.contains("SELECT \"Work\""),
                "expected later personal-folder SELECT, got {select}"
            );
            respond(
                &mut server,
                &format!(
                    "* FLAGS (\\Seen)\r\n\
                     * 1 EXISTS\r\n\
                     * 0 RECENT\r\n\
                     * OK [UIDVALIDITY 5] valid\r\n\
                     * OK [UIDNEXT 257] next\r\n\
                     {} OK [READ-WRITE] selected\r\n",
                    tag_of(&select)
                ),
            )
            .await;

            let store = read_line(&mut server).await;
            assert!(
                store.contains("UID STORE"),
                "expected UID STORE, got {store}"
            );
            assert!(
                store.contains(expected_uids),
                "expected STORE for {expected_uids}, got {store}"
            );
            respond(&mut server, &format!("{} OK stored\r\n", tag_of(&store))).await;
        }
        server
    });

    let mut output = crate::account::mutate::mutation_stream(
        account,
        Box::pin(futures::stream::iter(submitted)),
        crate::account::mutate::MutationKind::Flags(FlagOp::Add(bifrost_types::FlagSet::from([
            "$flagged".to_owned(),
        ]))),
    );

    let mut seen_ids = Vec::new();
    let mut revoked = 0;
    let mut succeeded = 0;
    let mut saw_done = false;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("mutation stream must continue after the shared denial")
    {
        match event {
            SyncEvent::Batch(batch) => {
                for outcome in batch.items {
                    match outcome {
                        ItemOutcome::Failed(failure) => {
                            seen_ids.push(failure.item.0.clone());
                            assert_eq!(
                                failure.error.kind(),
                                &AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
                            );
                            assert_eq!(
                                failure.error.scope(),
                                Some(&ErrorScope::Cursor(CursorScope::Folder(FolderId(
                                    shared.as_str().to_owned()
                                ))))
                            );
                            revoked += 1;
                        }
                        ItemOutcome::Succeeded(success) => {
                            seen_ids.push(success.item.0.clone());
                            succeeded += 1;
                        }
                        ItemOutcome::Uncertain(item) => {
                            panic!("preparation and completed STOREs are not uncertain: {item:?}")
                        }
                    }
                }
            }
            SyncEvent::Done(_) => {
                saw_done = true;
                break;
            }
            SyncEvent::Terminated(error) => {
                panic!("shared-folder revocation must not terminate siblings: {error:?}")
            }
            other => panic!("unexpected mutation event: {other:?}"),
        }
    }

    seen_ids.sort();
    expected_ids.sort();
    assert_eq!(
        seen_ids, expected_ids,
        "every submitted id is answered once"
    );
    assert_eq!(revoked, 1, "the denied shared-folder target is scoped");
    assert_eq!(succeeded, crate::account::TARGET_BUFFER_ITEMS);
    assert!(saw_done, "the stream completes after all target windows");
    let _server = script.await.unwrap();
}

/// A bulk move whose destination fails our own request validation rejects
/// every target without touching the wire. Those rejections must ride the
/// same buffering window the grouped path flushes on: emitting one
/// single-item batch per id costs one channel send per target, which is
/// unbounded in the size of the caller's target set.
#[tokio::test]
async fn invalid_move_destination_batches_its_rejections() {
    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let target_count = account.config.mutation_batch_size.max(1) * 2 + 5;
    let ids: Vec<bifrost_types::ObjectId> = (1..=target_count)
        .map(|uid| {
            crate::account::encode_object_id(
                &folder,
                crate::types::UidValidity::new(5).expect("nonzero epoch"),
                crate::types::Uid::new(u32::try_from(uid).unwrap()).expect("nonzero test UID"),
            )
        })
        .collect();
    let expected: Vec<String> = ids.iter().map(|id| id.0.clone()).collect();

    let output = crate::account::mutate::mutation_stream(
        account,
        Box::pin(futures::stream::iter(ids)),
        // Not a folder scope, so `validated_move_destination` refuses it
        // before any source folder is opened.
        crate::account::mutate::MutationKind::Move(bifrost_types::MembershipScope::Label(
            bifrost_types::LabelId("important".to_owned()),
        )),
    );

    let batches = drain_mutation_batches(output).await;
    // Ceiling of target_count / mutation_batch_size: three windows here.
    assert_batched_exactly_once(&batches, &expected, 3);
}

/// Ids that fail to decode never reach a folder group, but they must not be
/// emitted one batch at a time either: they accumulate against the same
/// window and ride one batch per flush.
#[tokio::test]
async fn undecodable_ids_ride_one_batch_per_window() {
    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let target_count = account.config.mutation_batch_size.max(1) + 7;
    let ids: Vec<bifrost_types::ObjectId> = (1..=target_count)
        .map(|n| bifrost_types::ObjectId(format!("not-an-imap-object-id-{n}")))
        .collect();
    let expected: Vec<String> = ids.iter().map(|id| id.0.clone()).collect();

    let output = crate::account::mutate::mutation_stream(
        account,
        Box::pin(futures::stream::iter(ids)),
        crate::account::mutate::MutationKind::Flags(bifrost_types::FlagOp::Add(
            bifrost_types::FlagSet::from(["$flagged".to_owned()]),
        )),
    );

    let batches = drain_mutation_batches(output).await;
    assert_batched_exactly_once(&batches, &expected, 2);
}

/// `add_to_container` and `message_hydrate` read their `pim_methods` flag like
/// every sibling primitive: a false flag refuses before any wire work. The
/// server half is dropped, so a method that reached the pool would surface a
/// transport error instead of `Unsupported`.
#[tokio::test]
async fn add_to_container_and_message_hydrate_honour_their_capability_flags() {
    use bifrost_types::{AccountErrorKind, AccountOperation, MutationTarget};

    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    drop(server);
    let caps = crate::account::test_support::stub_capabilities();
    assert!(!caps.pim_methods.add_to_container);
    assert!(!caps.pim_methods.message_hydrate);
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });
    // A well-formed id, so a missing gate proceeds toward the wire rather
    // than failing id decode first.
    let id = crate::account::encode_object_id(
        &crate::types::MailboxName::new("INBOX").unwrap(),
        crate::types::UidValidity::new(5).expect("nonzero epoch"),
        crate::types::Uid::new(3).expect("valid protocol test value"),
    );

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::mail_actions::add_to_container(
            account.clone(),
            MutationTarget::Message(id.clone()),
            ContainerId("Archive".to_owned()),
        ),
    )
    .await
    .expect("must not hang")
    .expect_err("a false add_to_container flag refuses");
    assert_eq!(
        err.kind(),
        &AccountErrorKind::Unsupported(AccountOperation::AddToContainer)
    );

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::message::message_hydrate(
            account,
            id,
            bifrost_types::hydration::HydrationProjection::Headers,
        ),
    )
    .await
    .expect("must not hang")
    .expect_err("a false message_hydrate flag refuses");
    assert_eq!(
        err.kind(),
        &AccountErrorKind::Unsupported(AccountOperation::HydrateMessage)
    );
}

/// A tagged `NO` to the SELECT that PREPARES a mutation is the server's answer
/// to the SELECT, not to the mutation: the COPY was never sent. The error must
/// not carry the SELECT's `Acknowledged` evidence for the non-idempotent
/// operation, or recovery would reconcile a copy that never happened.
#[tokio::test]
async fn a_refused_preflight_select_leaves_the_mutation_unsent() {
    use bifrost_types::{AccountOperation, Cause, MutationTarget, TransmissionState};

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let mut caps = crate::account::test_support::stub_capabilities();
    caps.pim_methods.add_to_container = true;
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });
    let inbox = crate::types::MailboxName::new("INBOX").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("SELECT \"INBOX\""),
            "expected SELECT, got {select}"
        );
        respond(
            &mut server,
            &format!("{} NO mailbox busy\r\n", tag_of(&select)),
        )
        .await;
        server
    });

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::mail_actions::add_to_container(
            account,
            MutationTarget::Message(crate::account::encode_object_id(
                &inbox,
                crate::types::UidValidity::new(5).expect("nonzero epoch"),
                crate::types::Uid::new(3).expect("valid protocol test value"),
            )),
            ContainerId("Archive".to_owned()),
        ),
    )
    .await
    .expect("add_to_container must not hang")
    .expect_err("a refused SELECT fails the copy");
    let _server = script.await.unwrap();

    assert_eq!(err.operation(), Some(AccountOperation::AddToContainer));
    let attempts: Vec<TransmissionState> = err
        .chain()
        .iter()
        .filter_map(|cause| match cause {
            Cause::Attempt(a) => Some(a.transmission_state),
            _ => None,
        })
        .collect();
    assert_eq!(
        attempts,
        vec![TransmissionState::Unsent],
        "the COPY was never sent: {err:?}"
    );
}

/// Mutation SELECT must not carry the cached QRESYNC baseline or refresh the
/// guard from that baseline before the STORE tests the consumer's stale view.
#[tokio::test]
async fn mutation_select_omits_qresync_baseline_and_keeps_cached_store_guard() {
    use bifrost_types::FlagOp;

    let (conn, mut server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    let (enabled, ()) = tokio::join!(
        crate::account::pool::enable_qresync(&conn, Duration::from_secs(5)),
        async {
            let enable = read_line(&mut server).await;
            respond(
                &mut server,
                &format!("* ENABLED QRESYNC\r\n{} OK enabled\r\n", tag_of(&enable)),
            )
            .await;
        }
    );
    enabled.unwrap();
    let account = scripted_sync_account(conn, 1, true, None, inbox_registry());
    let entry = account.folders.get(&inbox()).unwrap();
    entry.set_cursor(FolderCursor::QResync {
        uidvalidity: crate::types::UidValidity::new(5).expect("nonzero epoch"),
        modseq: crate::types::ModSeq::new(100).expect("valid protocol test value"),
        baseline: crate::account::envelope::UidBaseline::Complete(UidSet::from_ranges(vec![
            crate::types::UidRange::range(1, 4000000000),
        ])),
    });
    entry
        .record_modseq(
            crate::types::UidValidity::new(5).expect("valid protocol test value"),
            2,
            crate::types::ModSeq::new(100).expect("positive test MODSEQ"),
        )
        .unwrap();
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("SELECT ") && !select.contains("QRESYNC"),
            "{select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}* 1 EXISTS\r\n* OK [UIDVALIDITY 5] ok\r\n\
             * OK [UIDNEXT 3] ok\r\n* OK [HIGHESTMODSEQ 200] ok\r\n\
             {} OK [READ-WRITE] selected\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let store = read_line(&mut server).await;
        assert!(
            store.contains("UID STORE 2 (UNCHANGEDSINCE 100)"),
            "{store}"
        );
        respond(&mut server, &format!("{} OK stored\r\n", tag_of(&store))).await;
        server
    });
    let output = crate::account::mutate::mutation_stream(
        account,
        Box::pin(futures::stream::iter([crate::account::encode_object_id(
            &inbox(),
            crate::types::UidValidity::new(5).expect("nonzero epoch"),
            crate::types::Uid::new(2).expect("valid protocol test value"),
        )])),
        crate::account::mutate::MutationKind::Flags(FlagOp::Add(bifrost_types::FlagSet::from([
            "$flagged".to_owned(),
        ]))),
    );
    assert_eq!(
        drain_mutation_batches(output).await,
        vec![vec![id(
            crate::types::UidValidity::new(5).expect("valid protocol test value"),
            2
        )]]
    );
    let _server = script.await.unwrap();
}

/// A cache that has advanced beyond the consumer cannot replace its guard.
#[tokio::test]
async fn caller_modseq_rejects_stale_flags_while_absent_keeps_cached_guard() {
    use bifrost_types::{
        Account, AccountErrorKind, FlagOp, ItemOutcome, MailMutationTarget, ServerVersion,
        SyncEvent,
    };
    use futures::StreamExt;

    for expected in [Some(100), None] {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
        let account = scripted_sync_account(conn, 1, false, None, inbox_registry());
        account
            .folders
            .get(&inbox())
            .unwrap()
            .record_modseq(
                crate::types::UidValidity::new(5).expect("valid protocol test value"),
                2,
                crate::types::ModSeq::new(900).expect("positive test MODSEQ"),
            )
            .unwrap();
        let script = tokio::spawn(async move {
            let select = read_line(&mut server).await;
            respond(&mut server, &format!(
                "{SELECT_PREAMBLE}* 1 EXISTS\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 3] ok\r\n* OK [HIGHESTMODSEQ 900] ok\r\n{} OK [READ-WRITE] selected\r\n",
                tag_of(&select))).await;
            let store = read_line(&mut server).await;
            assert!(
                store.contains(&format!(
                    "UID STORE 2 (UNCHANGEDSINCE {})",
                    expected.unwrap_or(900)
                )),
                "{store}"
            );
            let status = if expected.is_some() {
                "OK [MODIFIED 2] stale"
            } else {
                "OK stored"
            };
            respond(&mut server, &format!("{} {status}\r\n", tag_of(&store))).await;
            server
        });
        let events = Arc::new(account)
            .bulk_set_flags_if_version(
                Box::pin(futures::stream::iter([MailMutationTarget {
                    id: crate::account::encode_object_id(
                        &inbox(),
                        crate::types::UidValidity::new(5).expect("nonzero epoch"),
                        crate::types::Uid::new(2).expect("valid protocol test value"),
                    ),
                    expected_version: expected.map(ServerVersion::ModSeq),
                }])),
                FlagOp::Add(bifrost_types::FlagSet::from(["$flagged".into()])),
            )
            .collect::<Vec<_>>()
            .await;
        let [SyncEvent::Batch(batch), SyncEvent::Done(None)] = events.as_slice() else {
            panic!("{events:?}");
        };
        assert_eq!(batch.items.len(), 1);
        match (&batch.items[0], expected) {
            (ItemOutcome::Failed(failure), Some(_)) => {
                assert_eq!(failure.error.kind(), &AccountErrorKind::ConcurrencyConflict);
            }
            (ItemOutcome::Succeeded(_), None) => {}
            other => panic!("unexpected result: {other:?}"),
        }
        let _server = script.await.unwrap();
    }
}

/// Without CONDSTORE the caller-version door is unadvertised, so a ModSeq
/// target is refused locally before any command, not after a SELECT.
#[tokio::test]
async fn caller_modseq_without_condstore_is_refused_before_the_wire() {
    use bifrost_types::{
        Account, AccountErrorKind, FlagOp, ItemOutcome, MailMutationTarget, ServerVersion,
        SyncEvent,
    };
    use futures::StreamExt;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let saw_command = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&saw_command);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        seen.store(true, Ordering::SeqCst);
        respond(&mut server, &format!(
            "{SELECT_PREAMBLE}* 1 EXISTS\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 3] ok\r\n{} OK [READ-WRITE] selected\r\n",
            tag_of(&select))).await;
        server
    });
    let events = Arc::new(account)
        .bulk_set_flags_if_version(
            Box::pin(futures::stream::iter([MailMutationTarget {
                id: crate::account::encode_object_id(
                    &inbox(),
                    crate::types::UidValidity::new(5).expect("nonzero epoch"),
                    crate::types::Uid::new(2).expect("valid protocol test value"),
                ),
                expected_version: Some(ServerVersion::ModSeq(100)),
            }])),
            FlagOp::Add(bifrost_types::FlagSet::from(["$flagged".into()])),
        )
        .collect::<Vec<_>>()
        .await;
    let failed = events.iter().any(|event| {
        matches!(event, SyncEvent::Batch(batch) if matches!(
            batch.items.as_slice(),
            [ItemOutcome::Failed(failure)]
                if matches!(failure.error.kind(), AccountErrorKind::Unsupported(_))
        ))
    });
    assert!(failed, "{events:?}");
    assert!(
        !saw_command.load(Ordering::SeqCst),
        "the gate must refuse before SELECT"
    );
    script.abort();
}

/// A mutation against a stored legacy id reports its outcome under that id.
#[tokio::test]
async fn mutation_answers_under_the_submitted_id_spelling() {
    use bifrost_types::{FlagOp, ItemOutcome, ObjectId, SyncEvent};
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(&mut server, &format!(
            "* FLAGS (\\Seen)\r\n* 3 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 4] ok\r\n{} OK [READ-WRITE] done\r\n",
            tag_of(&select),
        )).await;
        let store = read_line(&mut server).await;
        assert!(store.contains("UID STORE"), "expected STORE, got {store}");
        respond(&mut server, &format!("{} OK done\r\n", tag_of(&store))).await;
        server
    });
    let submitted = ObjectId("imap1:5:INBOX:5:3".to_owned());
    let mut output = crate::account::mutate::mutation_stream(
        account,
        Box::pin(futures::stream::iter(vec![submitted.clone()])),
        crate::account::mutate::MutationKind::Flags(FlagOp::Add(bifrost_types::FlagSet::from([
            "$flagged".to_owned(),
        ]))),
    );
    let mut ids = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("stream must make progress")
    {
        match event {
            SyncEvent::Batch(batch) => {
                for outcome in batch.items {
                    match outcome {
                        ItemOutcome::Succeeded(item) => ids.push(item.item.0),
                        other => panic!("unexpected outcome {other:?}"),
                    }
                }
            }
            SyncEvent::Done(_) => break,
            unexpected => panic!("unexpected event: {unexpected:?}"),
        }
    }
    assert_eq!(ids, vec![submitted.0]);
    let _server = script.await.unwrap();
}
