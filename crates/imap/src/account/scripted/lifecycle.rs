use super::test_support::*;

/// Admission must accept exactly what worker assignment can watch. A folder
/// name containing CR/LF fails `MailboxName::new`, so `subscribed_idle_folders`
/// silently drops it and no IDLE worker can ever SELECT it. Admitting it
/// anyway would report it as pushed (a misreport bifrost-sync trusts) and
/// consume a budget slot that a valid folder later in the same request should
/// have received.
#[tokio::test]
async fn a_folder_scope_with_an_unsendable_name_is_refused_not_misreported() {
    use bifrost_types::{Account, CursorScope, FolderId};

    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1 IDLE")).await;
    let account = scripted_account(conn, 2);
    let scopes = vec![
        CursorScope::Folder(FolderId("bad\r\nname".to_owned())),
        CursorScope::Folder(FolderId("Valid".to_owned())),
    ];
    let result = account
        .push_subscribe(&scopes)
        .await
        .expect("an invalid folder name is a failed item, never a whole-request Err");
    let succeeded: Vec<_> = result
        .outcomes()
        .succeeded()
        .iter()
        .map(|item| item.item.0)
        .collect();
    assert_eq!(
        succeeded,
        vec![1],
        "the invalid name must not be reported as pushed, and must not block the valid sibling",
    );
    assert_eq!(result.outcomes().failed().len(), 1);
    assert_eq!(result.outcomes().failed()[0].item.0, 0);
    assert!(
        result.outcomes().failed()[0]
            .error
            .user_safe_text()
            .any(|text| text.contains("invalid mailbox name")),
        "the refusal names the cause",
    );
    drop(server);
    account.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Account-lifetime conformance: the trait-surface contracts a transcript test
// cannot express, because they are about what the account does WITHOUT
// speaking to a server - closing, refusing an unsupported lane, and holding a
// lifecycle stream open.
// ---------------------------------------------------------------------------

/// `Account::close` is idempotent and terminal.
///
/// The engine may close an account it already closed (shutdown racing a
/// failed reopen). The second call must be a no-op `Ok`, not a second
/// LOGOUT sweep, and after either call the pool must refuse every
/// checkout instead of dialing a replacement session that would outlive
/// the account handle.
#[tokio::test]
async fn close_is_idempotent_and_leaves_the_pool_refusing() {
    use bifrost_types::Account;

    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    // No responder for the LOGOUT close issues: drop the server end so it
    // fails fast rather than blocking on a read that never completes.
    drop(server);

    let first = tokio::time::timeout(Duration::from_secs(5), account.close())
        .await
        .expect("close must not block on an unresponsive peer");
    assert!(first.is_ok(), "close reports success: {first:?}");

    let second = tokio::time::timeout(Duration::from_secs(5), account.close())
        .await
        .expect("the second close must return immediately");
    assert!(second.is_ok(), "close is idempotent: {second:?}");

    assert!(
        account.shutdown.is_cancelled(),
        "close cancels the account shutdown token",
    );
    let Err(err) = account
        .checkout_for_folder(&crate::types::MailboxName::new("INBOX").unwrap())
        .await
    else {
        panic!("a closed account must not hand out connections");
    };
    assert!(
        matches!(
            err,
            crate::Error::Transport {
                kind: crate::error::TransportFailure::Closed,
                ..
            }
        ),
        "got {err:?}"
    );
}

/// The account knob retunes the budget held by every protocol leg.
#[tokio::test]
async fn the_bandwidth_cap_knob_updates_the_budget_and_rejects_zero() {
    use bifrost_types::Account;
    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let shared = account.bandwidth_budget.clone();
    account.set_bandwidth_cap(bifrost_types::BandwidthCap::new(4096));
    assert_eq!(shared.cap(), Some(4096));
    account.set_bandwidth_cap(None);
    assert_eq!(shared.cap(), None);
    assert!(bifrost_types::BandwidthCap::new(0).is_none());
    assert_eq!(shared.cap(), None);
}

/// IMAP emits no scope-lifecycle events, but the stream must stay OPEN:
/// the engine's lifecycle worker treats an early close as a fault. It ends
/// only when the account shuts down.
#[tokio::test]
async fn the_scope_lifecycle_stream_stays_open_until_shutdown() {
    use bifrost_types::Account;
    use futures::{FutureExt, StreamExt};

    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);

    let mut stream = account.scope_lifecycle_stream();
    assert!(
        stream.next().now_or_never().is_none(),
        "the lifecycle stream must be pending, not closed, while the account lives",
    );

    account.shutdown.cancel();
    let end = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("shutdown must end the lifecycle stream");
    assert!(end.is_none(), "shutdown closes the stream, got {end:?}");
}

/// A MODSEQ group whose STORE times out abandons the checkout. The next
/// group must settle at once as unsent rather than queue its own STORE
/// behind the stalled exchange and spend a second full timeout withdrawing
/// it. Both flag and destroy mutations group this way.
#[tokio::test(start_paused = true)]
async fn a_group_after_an_abandoning_timeout_settles_unsent_without_waiting() {
    use bifrost_types::{
        AccountErrorKind, Cause, FlagOp, ItemOutcome, SyncEvent, TransmissionState,
        TransportErrorKind,
    };
    use futures::StreamExt;

    for destroy in [false, true] {
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
        let timeout = account.command_timeout();
        let script = tokio::spawn(async move {
            let select = read_line(&mut server).await;
            respond(&mut server, &format!(
                "{SELECT_PREAMBLE}* 2 EXISTS\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 4] ok\r\n* OK [HIGHESTMODSEQ 900] ok\r\n{} OK [READ-WRITE] selected\r\n",
                tag_of(&select))).await;
            let store = read_line(&mut server).await;
            assert!(
                store.contains("UID STORE 2 (UNCHANGEDSINCE 900)"),
                "{store}"
            );
            // Never answered: the caller times out with the driver inside it.
            server
        });
        let kind = if destroy {
            crate::account::mutate::MutationKind::Destroy
        } else {
            crate::account::mutate::MutationKind::Flags(FlagOp::Add(bifrost_types::FlagSet::from(
                ["$flagged".to_owned()],
            )))
        };
        let started = tokio::time::Instant::now();
        let events = crate::account::mutate::mutation_stream(
            account,
            Box::pin(futures::stream::iter([
                crate::account::encode_object_id(
                    &inbox(),
                    crate::types::UidValidity::new(5).expect("nonzero epoch"),
                    crate::types::Uid::new(2).expect("valid protocol test value"),
                ),
                crate::account::encode_object_id(
                    &inbox(),
                    crate::types::UidValidity::new(5).expect("nonzero epoch"),
                    crate::types::Uid::new(3).expect("valid protocol test value"),
                ),
            ])),
            kind,
        )
        .collect::<Vec<_>>()
        .await;
        assert_eq!(
            started.elapsed(),
            timeout,
            "one timeout for the stalled group, none for the next (destroy = {destroy})"
        );
        let third = crate::account::encode_object_id(
            &inbox(),
            crate::types::UidValidity::new(5).expect("nonzero epoch"),
            crate::types::Uid::new(3).expect("valid protocol test value"),
        );
        let error = events
            .iter()
            .filter_map(|event| match event {
                SyncEvent::Batch(batch) => Some(batch.items.iter()),
                _ => None,
            })
            .flatten()
            .find_map(|item| match item {
                ItemOutcome::Failed(f) if f.item.0 == third.0 => Some(f.error.clone()),
                ItemOutcome::Uncertain(u) if u.item.0 == third.0 => Some(u.error.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("uid 3 must fail: {events:?}"));
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Transport(TransportErrorKind::Network),
            "{error:?}"
        );
        let attempts: Vec<TransmissionState> = error
            .chain()
            .iter()
            .filter_map(|cause| match cause {
                Cause::Attempt(a) => Some(a.transmission_state),
                _ => None,
            })
            .collect();
        assert_eq!(attempts, vec![TransmissionState::Unsent], "{error:?}");
        drop(script.await.unwrap());
    }
}
