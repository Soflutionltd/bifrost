use super::test_support::*;

/// Selection discovers an obsolete identity before any UID command. Each lane
/// must request re-inventory of the folder, preserving the submitted target and
/// operation instead of reporting terminal failure or reading a reused UID.
#[tokio::test]
async fn stale_uidvalidity_accesses_request_folder_reinventory_before_uid_commands() {
    use bifrost_types::{EngineDirective, ItemOutcome, RecoveryClass, SyncEvent};
    use futures::{FutureExt, StreamExt};
    use tokio::io::AsyncReadExt;

    async fn failed_target<T: std::fmt::Debug>(
        mut stream: bifrost_types::AccountStream<SyncEvent<ItemOutcome<T>>>,
        id: &bifrost_types::ObjectId,
    ) -> bifrost_types::AccountError {
        let mut failures = Vec::new();
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Batch(batch) => {
                    for item in batch.items {
                        let ItemOutcome::Failed(failure) = item else {
                            panic!("stale target: {item:?}")
                        };
                        failures.push(failure);
                    }
                }
                SyncEvent::Done(_) => {}
                other => panic!("stale target: {other:?}"),
            }
        }
        assert_eq!(failures.len(), 1);
        let failure = failures.pop().unwrap();
        assert_eq!(failure.item.0, id.0);
        failure.error
    }

    for operation in [
        AccountOperation::BulkMove,
        AccountOperation::BulkDestroy,
        AccountOperation::UpdateFlags,
        AccountOperation::Hydrate,
        AccountOperation::HydrateMessage,
        AccountOperation::OpenRawRfc822,
    ] {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
        let mut parts = scripted_account_parts(conn, 1);
        parts.capabilities.pim_methods.message_hydrate = true;
        let account = ImapAccount::new(parts);
        let script = tokio::spawn(async move {
            let select = read_line(&mut server).await;
            assert!(
                select.contains("SELECT") || select.contains("EXAMINE"),
                "{select}"
            );
            respond(&mut server, &format!(
                "* FLAGS (\\Seen)\r\n* 1 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 8] rebuilt\r\n{} OK selected\r\n",
                tag_of(&select),
            )).await;
            server
        });
        let folder = crate::types::MailboxName::new("INBOX").unwrap();
        // Retain a noncanonical submitted spelling to pin per-target correlation.
        let id = bifrost_types::ObjectId("imap1:5:INBOX:7:03".into());
        let targets = Box::pin(futures::stream::iter([id.clone()]));
        let error = match operation {
            AccountOperation::OpenRawRfc822 => {
                let events: Vec<_> = crate::account::blob::open_raw_rfc822(account.clone(), id)
                    .collect()
                    .await;
                match events.as_slice() {
                    [SyncEvent::Terminated(error)] => error.clone(),
                    other => panic!("stale raw read: {other:?}"),
                }
            }
            AccountOperation::HydrateMessage => crate::account::message::message_hydrate(
                account.clone(),
                id,
                bifrost_types::HydrationProjection::Headers,
            )
            .await
            .expect_err("stale PIM identity"),
            AccountOperation::Hydrate => {
                failed_target(
                    crate::account::get::get_stream(
                        account.clone(),
                        targets,
                        bifrost_types::Projection::FlagsOnly,
                    ),
                    &id,
                )
                .await
            }
            _ => {
                let kind = match operation {
                    AccountOperation::BulkMove => crate::account::mutate::MutationKind::Move(
                        bifrost_types::MembershipScope::Folder(bifrost_types::FolderId(
                            "Archive".into(),
                        )),
                    ),
                    AccountOperation::BulkDestroy => crate::account::mutate::MutationKind::Destroy,
                    _ => crate::account::mutate::MutationKind::Flags(bifrost_types::FlagOp::Add(
                        bifrost_types::FlagSet::from(["$seen".to_owned()]),
                    )),
                };
                failed_target(
                    crate::account::mutate::mutation_stream(account.clone(), targets, kind),
                    &id,
                )
                .await
            }
        };
        assert_eq!(error.operation(), Some(operation));
        assert_eq!(
            error.recovery(),
            &RecoveryClass::Engine(EngineDirective::RestartScope(crate::account::folder_scope(
                &folder
            )))
        );
        let mut server = script.await.unwrap();
        let mut bytes = [0; 1];
        assert!(
            server.read(&mut bytes).now_or_never().is_none(),
            "stale UID command sent for {operation:?}"
        );
    }
}

/// A SELECT that gets its tagged OK and is then found incomplete (here: no
/// EXISTS) has still moved the session onto the new mailbox. The checkout's
/// affinity must follow, or `deselect_target` for that mailbox believes the
/// OLD one is selected and skips the UNSELECT a DELETE or RENAME needs.
#[tokio::test]
async fn an_incomplete_select_still_moves_the_mailbox_affinity() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev2")).await;
    let account = scripted_account(conn, 1);
    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let archive = crate::types::MailboxName::new("Archive").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT \"INBOX\""), "got {select}");
        respond(&mut server, &rev2_select_ok(tag_of(&select), "INBOX")).await;

        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT \"Archive\""), "got {select}");
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Deleted \\Seen)\r\n\
                 * LIST () \"/\" Archive\r\n\
                 * OK [UIDVALIDITY 1] selected\r\n\
                 {} OK [READ-WRITE] SELECT done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let unselect = read_line(&mut server).await;
        assert!(
            unselect.contains("UNSELECT"),
            "Archive is selected, so it must be deselected, got {unselect}"
        );
        respond(
            &mut server,
            &format!("{} OK UNSELECT done\r\n", tag_of(&unselect)),
        )
        .await;
        server
    });

    let mut pooled = account.pool.checkout_any().await.unwrap();
    account
        .select_folder(&mut pooled, &inbox, crate::account::SelectPurpose::Mutate)
        .await
        .unwrap();
    let err = account
        .select_folder(&mut pooled, &archive, crate::account::SelectPurpose::Mutate)
        .await
        .expect_err("a SELECT without EXISTS is incomplete");
    assert!(
        matches!(
            err,
            crate::Error::Provider(crate::error::ProviderFailure::Missing(_))
        ),
        "expected the post-OK omission, got {err:?}"
    );
    assert_eq!(pooled.selected_affinity(), Some(&archive));

    pooled
        .deselect_target(&archive, Duration::from_secs(5))
        .await
        .expect("UNSELECT succeeds");
    let _server = tokio::time::timeout(Duration::from_secs(5), script)
        .await
        .expect("the UNSELECT must reach the server")
        .unwrap();
}

/// A tagged NO to SELECT deselects the previous mailbox (RFC 3501 6.3.1). The
/// affinity must drop it, or `deselect_target` for that mailbox issues an
/// UNSELECT from the Authenticated state, which the connection refuses, and
/// the DELETE or RENAME behind it fails for no reason.
#[tokio::test]
async fn a_refused_select_clears_the_mailbox_affinity() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev2")).await;
    let account = scripted_account(conn, 1);
    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let archive = crate::types::MailboxName::new("Archive").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT \"INBOX\""), "got {select}");
        respond(&mut server, &rev2_select_ok(tag_of(&select), "INBOX")).await;

        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT \"Archive\""), "got {select}");
        respond(
            &mut server,
            &format!("{} NO [NONEXISTENT] no such mailbox\r\n", tag_of(&select)),
        )
        .await;
        server
    });

    let mut pooled = account.pool.checkout_any().await.unwrap();
    account
        .select_folder(&mut pooled, &inbox, crate::account::SelectPurpose::Mutate)
        .await
        .unwrap();
    account
        .select_folder(&mut pooled, &archive, crate::account::SelectPurpose::Mutate)
        .await
        .expect_err("the server refused the SELECT");
    assert_eq!(
        pooled.connection().session_state(),
        crate::connection::SessionState::Authenticated
    );
    assert_eq!(pooled.selected_affinity(), None);

    pooled
        .deselect_target(&inbox, Duration::from_secs(5))
        .await
        .expect("nothing is selected, so there is nothing to deselect");
    let _server = script.await.unwrap();
}

/// On a pre-UNSELECT server the deselect fallback replaces the checked-out
/// connection. It must release the old one BEFORE dialing the replacement:
/// dialing first holds `pool_cap + 1` physical connections across the whole
/// handshake, and a server enforcing its per-user connection limit rejects
/// exactly the fallback these old servers need. It also leaves the target
/// mailbox SELECTed on a live session while DELETE goes out (RFC 2683 2.2.2).
///
/// The pool stays open and dials through a scripted connector, so the
/// LOGOUT the script reads is the fallback's own (not close's drain) and
/// the connector has not been called when it arrives. The replacement
/// queues for capacity before retiring, so a checkout that arrives while
/// that LOGOUT is in flight does not take the room the old member frees.
/// A checkout queued BEFORE the fallback started would take it by design,
/// which is why the second checkout is created only after the LOGOUT.
#[tokio::test(start_paused = true)]
async fn deselect_fallback_releases_the_old_connection_before_redialing() {
    let (account, mut peer, mut dials) = session_account(1, 1, false).await;
    let folder = crate::types::MailboxName::new("Archive").unwrap();
    let mut held = account.pool.checkout_any().await.unwrap();
    let script = async {
        session_select(&mut peer, "Archive").await;
    };
    let (result, ()) = tokio::join!(
        account.select_folder(&mut held, &folder, crate::account::SelectPurpose::Mutate),
        script
    );
    result.unwrap();
    let mut replace = Box::pin(held.deselect_target(&folder, Duration::from_secs(5)));
    assert!(futures::poll!(&mut replace).is_pending());
    let logout = read_line(&mut peer).await;
    assert!(logout.contains("LOGOUT"));
    assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let mut second = Box::pin(account.pool.checkout_any());
    assert!(futures::poll!(&mut second).is_pending());
    respond(
        &mut peer,
        &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
    )
    .await;
    let script = async {
        let mut peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut peer).await;
        session_auth(&mut peer, &auth).await;
        peer
    };
    let (result, _dial_peer) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(5), replace),
        script
    );
    result.unwrap().unwrap();
    assert!(
        held.connection()
            .server_profile()
            .supports(crate::types::Capability::Binary)
    );
    assert_eq!(held.selected_affinity(), None);
    assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(futures::poll!(&mut second).is_pending());
}

/// Another PARKED member left selected on the target must be brought off it
/// before DELETE or RENAME goes out (RFC 2683 2.2.2), not only the caller's
/// own checkout. An UNSELECT the server accepts returns the member to the
/// pool unselected; one it refuses retires the member, since nothing else
/// proves the session has left the target.
#[tokio::test(start_paused = true)]
async fn deselect_target_brings_other_parked_members_off_the_target() {
    use bifrost_types::Account as _;
    for unselect_ok in [true, false] {
        let (account, peer, mut dials) = session_account(2, 1, false).await;
        let folder = crate::types::MailboxName::new("Archive").unwrap();
        let mut held = account.pool.checkout_any().await.unwrap();
        let script = async {
            let mut peer = dials.servers.recv().await.unwrap();
            let auth = read_line(&mut peer).await;
            session_auth(&mut peer, &auth).await;
            peer
        };
        let (other, mut other_peer) = tokio::join!(account.pool.checkout_any(), script);
        let mut other = other.unwrap();
        let (result, ()) = tokio::join!(
            account.select_folder(&mut other, &folder, crate::account::SelectPurpose::Mutate),
            session_select(&mut other_peer, "Archive")
        );
        result.unwrap();
        drop(other);
        assert_eq!(account.pool.parked_selection(), vec![Some(folder.clone())]);

        let script = async {
            let unselect = tokio::time::timeout(Duration::from_secs(5), read_line(&mut other_peer))
                .await
                .expect("the parked member selected on the target must be deselected");
            assert!(unselect.contains("UNSELECT"), "{unselect}");
            if unselect_ok {
                respond(
                    &mut other_peer,
                    &format!("{} OK unselected\r\n", tag_of(&unselect)),
                )
                .await;
            } else {
                respond(
                    &mut other_peer,
                    &format!("{} NO not now\r\n", tag_of(&unselect)),
                )
                .await;
                let logout =
                    tokio::time::timeout(Duration::from_secs(5), read_line(&mut other_peer))
                        .await
                        .expect("a member that refused UNSELECT must be retired");
                assert!(logout.contains("LOGOUT"), "{logout}");
                respond(
                    &mut other_peer,
                    &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
                )
                .await;
            }
        };
        let (result, ()) = tokio::join!(
            held.deselect_target(&folder, Duration::from_secs(5)),
            script
        );
        result.unwrap();
        let expected = if unselect_ok { vec![None] } else { Vec::new() };
        assert_eq!(account.pool.parked_selection(), expected, "{unselect_ok}");
        drop(held);
        drop(other_peer);
        drop(peer);
        account.close().await.unwrap();
    }
}

/// A SELECT that omits UIDVALIDITY is the server's omission. Every PIM
/// primitive that reads it (copy, delete, flag, hydrate, and both search
/// lanes) used to answer `Request(Malformed)` - `ClientBug`, blaming the
/// caller - while the sync lanes reading the same field answered
/// `Protocol(MissingField)`. All of them now agree on the provider's fault.
#[tokio::test]
async fn pim_select_without_uidvalidity_is_the_providers_missing_field() {
    use bifrost_types::hydration::HydrationProjection;
    use bifrost_types::{
        AccountErrorKind, AccountOperation, MutationTarget, ProtocolErrorKind, RecoveryClass,
        SearchFilter, SearchRequest,
    };

    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let id = crate::account::encode_object_id(
        &inbox,
        crate::types::UidValidity::new(5).expect("nonzero epoch"),
        crate::types::Uid::new(3).expect("valid protocol test value"),
    );
    let in_inbox = || SearchRequest::filter(SearchFilter::In(ContainerId("INBOX".to_owned())));

    let mut cases: Vec<(AccountOperation, bifrost_types::AccountError)> = Vec::new();
    {
        let id = id.clone();
        cases.push((
            AccountOperation::AddToContainer,
            pim_error_on_select_without_uidvalidity(move |account| {
                crate::account::mail_actions::add_to_container(
                    account,
                    MutationTarget::Message(id),
                    ContainerId("Archive".to_owned()),
                )
            })
            .await,
        ));
    }
    {
        let id = id.clone();
        cases.push((
            AccountOperation::RemoveFromContainer,
            pim_error_on_select_without_uidvalidity(move |account| {
                crate::account::mail_actions::remove_from_container(
                    account,
                    MutationTarget::Message(id),
                    ContainerId("INBOX".to_owned()),
                )
            })
            .await,
        ));
    }
    {
        let id = id.clone();
        cases.push((
            AccountOperation::SetIsRead,
            pim_error_on_select_without_uidvalidity(move |account| {
                crate::account::mail_actions::set_is_read(
                    account,
                    MutationTarget::Message(id),
                    true,
                )
            })
            .await,
        ));
    }
    {
        let id = id.clone();
        cases.push((
            AccountOperation::HydrateMessage,
            pim_error_on_select_without_uidvalidity(move |account| {
                crate::account::message::message_hydrate(account, id, HydrationProjection::Full)
            })
            .await,
        ));
    }
    cases.push((
        AccountOperation::Search,
        pim_error_on_select_without_uidvalidity(move |account| {
            crate::account::search::search(account, in_inbox())
        })
        .await,
    ));
    cases.push((
        AccountOperation::SearchMessages,
        pim_error_on_select_without_uidvalidity(move |account| {
            crate::account::search::search_messages(account, in_inbox())
        })
        .await,
    ));

    for (op, err) in cases {
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
            "{op:?}: the omission is the server's, got {err:?}"
        );
        assert_eq!(
            err.recovery(),
            &RecoveryClass::ProviderContractViolation,
            "{op:?}: got {err:?}"
        );
        assert_eq!(
            err.operation(),
            Some(op),
            "{op:?}: the calling op rides along"
        );
    }
}

/// A failed SELECT refreshes affinity from the driver even when the pool's
/// previous cache is wrong. BAD preserves the actual mailbox, not that cache.
#[tokio::test]
async fn failed_select_refreshes_affinity_from_driver_identity() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev2")).await;
    let account = scripted_account(conn, 1);
    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let archive = crate::types::MailboxName::new("Archive").unwrap();
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT \"INBOX\""), "got {select}");
        respond(&mut server, &rev2_select_ok(tag_of(&select), "INBOX")).await;
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT \"Archive\""), "got {select}");
        respond(
            &mut server,
            &format!("{} BAD SELECT refused\r\n", tag_of(&select)),
        )
        .await;
        server
    });
    let mut pooled = account.pool.checkout_any().await.unwrap();
    account
        .select_folder(&mut pooled, &inbox, crate::account::SelectPurpose::Mutate)
        .await
        .unwrap();
    pooled.set_selected_affinity(Some(archive.clone()));
    account
        .select_folder(&mut pooled, &archive, crate::account::SelectPurpose::Mutate)
        .await
        .expect_err("BAD preserves the driver's previous mailbox");
    assert_eq!(pooled.selected_affinity(), Some(&inbox));
    let _server = script.await.unwrap();
}
