//! Byte-level account tests over the in-memory driver pair.
//!
//! These build a real `ImapAccount` around a `Pool` primed with a
//! `connection::test_support::driver_pair` connection, so an account
//! entry point is driven end to end against a canned server transcript.
//! Hermetic: `tokio::io::duplex`, no listener, no port, no daemon.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{ContainerId, ContainerKind};

use crate::connection::test_support::{driver_pair, preauth_greeting, read_line, respond, tag_of};

use super::folder_registry::FolderRegistry;
use super::test_support::scripted_account_parts;
use super::{ImapAccount, ImapAccountParts};

/// Build an account whose pool allows `pool_cap` physical sessions,
/// the primed scripted connection included. Fresh dials are refused locally.
/// At a single-session cap a nested checkout waits for the held member,
/// exposing accidental nested acquisition.
fn scripted_account(conn: crate::ImapConnection, pool_cap: usize) -> ImapAccount {
    scripted_sync_account(conn, pool_cap, false, None, FolderRegistry::default())
}

/// `scripted_account` with the two knobs the sync half needs: whether
/// QRESYNC survived negotiation, and a registry pre-seeded with the folder
/// the transcript selects (an unregistered folder makes every cursor and
/// MODSEQ cache write a silent no-op).
fn scripted_sync_account(
    conn: crate::ImapConnection,
    pool_cap: usize,
    qresync_enabled: bool,
    qresync_negotiation_warning: Option<String>,
    folders: FolderRegistry,
) -> ImapAccount {
    scripted_dav_account(
        conn,
        pool_cap,
        qresync_enabled,
        qresync_negotiation_warning,
        folders,
        Default::default(),
    )
}

/// `scripted_sync_account` with a composed-DAV ownership index, for pinning
/// how the mail lanes treat scopes a DAV sub-account owns.
fn scripted_dav_account(
    conn: crate::ImapConnection,
    pool_cap: usize,
    qresync_enabled: bool,
    qresync_negotiation_warning: Option<String>,
    folders: FolderRegistry,
    dav_scopes: super::DavScopeIndex,
) -> ImapAccount {
    ImapAccount::new(ImapAccountParts {
        folders: Arc::new(folders),
        qresync_enabled,
        qresync_negotiation_warning,
        dav_scopes,
        ..scripted_account_parts(conn, pool_cap)
    })
}

/// A container mutation must complete with a single-permit pool.
///
/// `container_create` holds its pooled checkout across the post-mutation
/// `refresh_folders` re-LIST. If that refresh acquired a second permit,
/// this deadlocks forever at `pool_cap = 1` even though CREATE already
/// committed server-side. The timeout turns that regression into a fast
/// failure instead of a hung suite.
#[tokio::test]
async fn container_create_completes_under_a_single_permit_pool() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);

    let script = tokio::spawn(async move {
        let create = read_line(&mut server).await;
        assert!(create.contains("CREATE"), "expected CREATE, got {create}");
        respond(
            &mut server,
            &format!("{} OK CREATE done\r\n", tag_of(&create)),
        )
        .await;

        // The re-LIST must arrive on the same connection the CREATE used.
        let list = read_line(&mut server).await;
        assert!(list.contains("LIST"), "expected LIST, got {list}");
        respond(
            &mut server,
            &format!(
                "* LIST (\\HasNoChildren) \".\" INBOX\r\n\
                 * LIST (\\HasNoChildren) \".\" Archive\r\n\
                 {} OK LIST done\r\n",
                tag_of(&list)
            ),
        )
        .await;
        server
    });

    let created = tokio::time::timeout(
        Duration::from_secs(5),
        super::pim::container_create(
            account.clone(),
            ContainerKind::Folder,
            "Archive".to_owned(),
            None,
            None,
        ),
    )
    .await
    .expect("container_create must not deadlock on the pool permit")
    .expect("scripted CREATE + LIST succeed");

    assert_eq!(created.0, "Archive");
    // The refresh landed: the re-LIST replaced the personal snapshot.
    assert!(account.folders.get(&created_name()).is_some());
    let _server = script.await.unwrap();
}

/// A hydration FETCH failure after the stale-UIDVALIDITY batch was
/// published must downgrade only the still-unresolved ids. Re-emitting the
/// stale ids as `Uncertain` would put one id in two lanes: the engine
/// would terminally fail it and simultaneously queue it for read-back.
#[tokio::test]
async fn folder_get_failure_does_not_relabel_published_stale_ids() {
    use bifrost_types::{ItemOutcome, Projection};

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let stale_id = super::encode_object_id(&folder, 4, 7);
    let valid_id = super::encode_object_id(&folder, 5, 3);

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE") || select.contains("SELECT"),
            "expected a select, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(fetch.contains("FETCH"), "expected UID FETCH, got {fetch}");
        respond(
            &mut server,
            &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&fetch)),
        )
        .await;
        server
    });

    let ids: bifrost_types::AccountStream<bifrost_types::ObjectId> =
        Box::pin(futures::stream::iter(vec![
            stale_id.clone(),
            valid_id.clone(),
        ]));
    let mut stream = super::get::get_stream(account, ids, Projection::Metadata);

    let mut failed = Vec::new();
    let mut uncertain = Vec::new();
    let mut succeeded = Vec::new();
    use futures::StreamExt;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("stream must make progress")
    {
        match event {
            SyncEvent::Batch(batch) => {
                for outcome in batch.items {
                    match outcome {
                        ItemOutcome::Failed(item) => failed.push(item.item.0.clone()),
                        ItemOutcome::Uncertain(item) => uncertain.push(item.item.0.clone()),
                        ItemOutcome::Succeeded(item) => succeeded.push(item.item.0.clone()),
                    }
                }
            }
            SyncEvent::Done(_) => break,
            other => panic!("unexpected event: {other:?}"),
        }
    }

    assert_eq!(
        failed,
        vec![stale_id.0.clone()],
        "the stale id fails exactly once, before the FETCH"
    );
    assert_eq!(
        uncertain,
        vec![valid_id.0.clone()],
        "only the unresolved id may fall to the uncertain lane"
    );
    assert!(succeeded.is_empty());
    let _server = script.await.unwrap();
}

fn created_name() -> crate::types::MailboxName {
    crate::types::MailboxName::new("Archive".to_owned()).unwrap()
}

/// The target buffer must flush at its exact ceiling even while the input
/// producer remains open. Removing the production flush makes this test time
/// out before the server sees SELECT.
#[tokio::test]
async fn get_flushes_at_target_buffer_boundary_before_input_closes() {
    use bifrost_types::{Projection, SyncEvent};
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\""),
            "expected SELECT, got {select}"
        );
        respond(&mut server, &format!(
            "* FLAGS (\\Seen)\r\n* 256 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 257] ok\r\n{} OK [READ-ONLY] done\r\n",
            tag_of(&select),
        )).await;
        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("UID FETCH"),
            "expected bounded-window FETCH, got {fetch}"
        );
        respond(&mut server, &format!("{} OK done\r\n", tag_of(&fetch))).await;
    });

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(257);
    for uid in 1..=super::TARGET_BUFFER_ITEMS {
        input_tx
            .send(super::encode_object_id(
                &crate::types::MailboxName::new("INBOX").unwrap(),
                5,
                u32::try_from(uid).unwrap(),
            ))
            .await
            .unwrap();
    }
    let ids = Box::pin(futures::stream::unfold(input_rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    let mut output = super::get::get_stream(account, ids, Projection::FlagsOnly);
    let first = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the full buffer must flush before input closes")
        .expect("one output batch");
    assert!(
        matches!(&first, SyncEvent::Batch(batch) if batch.items.iter().all(|item| !matches!(item, bifrost_types::ItemOutcome::Uncertain(_)))),
        "SELECT failed before the boundary FETCH: {first:?}"
    );
    drop(input_tx);
    script.await.unwrap();
}

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
    for uid in 1..=super::TARGET_BUFFER_ITEMS {
        input_tx
            .send(super::encode_object_id(
                &crate::types::MailboxName::new("INBOX").unwrap(),
                5,
                u32::try_from(uid).unwrap(),
            ))
            .await
            .unwrap();
    }
    let ids = Box::pin(futures::stream::unfold(input_rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    let mut output = super::mutate::mutation_stream(
        account,
        ids,
        super::mutate::MutationKind::Flags(FlagOp::Add(std::collections::HashSet::from([
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
    let first_id = super::encode_object_id(&folder, 5, 3);
    let second_id = super::encode_object_id(&folder, 5, 4);

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
    let mut output = super::mutate::mutation_stream(
        account,
        ids,
        super::mutate::MutationKind::Flags(FlagOp::Add(std::collections::HashSet::from([
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
        AccountErrorKind, CursorScope, ErrorScope, FlagOp, FolderId, ItemOutcome, MailboxId,
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
        vec![super::folder_registry::SharedFolderEntry {
            info: crate::types::MailboxInfo {
                name: shared.clone(),
                ..Default::default()
            },
            owner: MailboxId("alice".to_owned()),
            rights: None,
            namespace_prefix: "Shared/".to_owned(),
        }],
    );
    let account = scripted_sync_account(conn, 1, false, None, folders);
    let shared_id = super::encode_object_id(&shared, 5, 3);
    let personal_ids: Vec<_> = (1..=super::TARGET_BUFFER_ITEMS)
        .map(|uid| super::encode_object_id(&personal, 5, u32::try_from(uid).unwrap()))
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

    let mut output = super::mutate::mutation_stream(
        account,
        Box::pin(futures::stream::iter(submitted)),
        super::mutate::MutationKind::Flags(FlagOp::Add(std::collections::HashSet::from([
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
    assert_eq!(succeeded, super::TARGET_BUFFER_ITEMS);
    assert!(saw_done, "the stream completes after all target windows");
    let _server = script.await.unwrap();
}

/// Drain a mutation stream to `Done`, returning one entry per emitted
/// `Batch` holding that batch's item ids. Batch count and per-id
/// multiplicity are both readable from the result.
async fn drain_mutation_batches(
    mut output: bifrost_types::AccountStream<
        bifrost_types::SyncEvent<bifrost_types::ItemOutcome<bifrost_types::MutationSuccess>>,
    >,
) -> Vec<Vec<String>> {
    use bifrost_types::SyncEvent;
    use futures::StreamExt;

    let mut batches = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("stream must make progress")
    {
        match event {
            SyncEvent::Batch(batch) => {
                batches.push(
                    batch
                        .items
                        .iter()
                        .map(|outcome| match outcome {
                            bifrost_types::ItemOutcome::Failed(item) => item.item.0.clone(),
                            bifrost_types::ItemOutcome::Uncertain(item) => item.item.0.clone(),
                            bifrost_types::ItemOutcome::Succeeded(item) => item.item.0.clone(),
                        })
                        .collect(),
                );
            }
            SyncEvent::Done(_) => break,
            unexpected => panic!("unexpected event: {unexpected:?}"),
        }
    }
    batches
}

/// Assert every id in `expected` appears in exactly one batch item across
/// `batches`, and that the emitted batch count stayed within the buffering
/// window rather than degenerating to one batch per id.
fn assert_batched_exactly_once(batches: &[Vec<String>], expected: &[String], max_batches: usize) {
    let mut seen: Vec<String> = batches.iter().flatten().cloned().collect();
    seen.sort();
    let mut want = expected.to_vec();
    want.sort();
    assert_eq!(seen, want, "every id owes exactly one outcome");
    assert!(
        batches.len() <= max_batches,
        "expected at most {max_batches} batches for {} ids, got {}",
        expected.len(),
        batches.len()
    );
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
    let target_count = super::TARGET_BUFFER_ITEMS * 2 + 5;
    let ids: Vec<bifrost_types::ObjectId> = (1..=target_count)
        .map(|uid| super::encode_object_id(&folder, 5, u32::try_from(uid).unwrap()))
        .collect();
    let expected: Vec<String> = ids.iter().map(|id| id.0.clone()).collect();

    let output = super::mutate::mutation_stream(
        account,
        Box::pin(futures::stream::iter(ids)),
        // Not a folder scope, so `validated_move_destination` refuses it
        // before any source folder is opened.
        super::mutate::MutationKind::Move(bifrost_types::MembershipScope::Label(
            bifrost_types::LabelId("important".to_owned()),
        )),
    );

    let batches = drain_mutation_batches(output).await;
    // Ceiling of target_count / TARGET_BUFFER_ITEMS: three windows here.
    assert_batched_exactly_once(&batches, &expected, 3);
}

/// Ids that fail to decode never reach a folder group, but they must not be
/// emitted one batch at a time either: they accumulate against the same
/// window and ride one batch per flush.
#[tokio::test]
async fn undecodable_ids_ride_one_batch_per_window() {
    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let target_count = super::TARGET_BUFFER_ITEMS + 7;
    let ids: Vec<bifrost_types::ObjectId> = (1..=target_count)
        .map(|n| bifrost_types::ObjectId(format!("not-an-imap-object-id-{n}")))
        .collect();
    let expected: Vec<String> = ids.iter().map(|id| id.0.clone()).collect();

    let output = super::mutate::mutation_stream(
        account,
        Box::pin(futures::stream::iter(ids)),
        super::mutate::MutationKind::Flags(bifrost_types::FlagOp::Add(
            std::collections::HashSet::from(["$flagged".to_owned()]),
        )),
    );

    let batches = drain_mutation_batches(output).await;
    assert_batched_exactly_once(&batches, &expected, 2);
}

/// A non-NOTIFY server gets one dedicated IDLE session per pushed folder,
/// carved out of `pool_cap`, so the third folder at the default budget of
/// two cannot be pushed. It must be refused in the failed lane rather than
/// silently accepted: bifrost-sync records only the succeeded scopes as
/// pushed and keeps the rest polling, so a silent acceptance would make that
/// folder invisible - neither pushed nor polled.
///
/// Pinned at the exact boundary in both directions. The second folder (item
/// "1") must succeed and the third (item "2") must fail; an off-by-one in
/// either direction moves one of those two. `pool_cap = 4` leaves headroom
/// above the budget, so the budget, not the headroom clamp, is what binds.
#[tokio::test]
async fn non_notify_push_reports_the_folder_beyond_its_budget_as_poll_only() {
    use bifrost_types::{Account, CursorScope, FolderId};

    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 4);
    assert_eq!(
        account.config.idle_connection_budget, 2,
        "this transcript is written against the default budget",
    );
    let scopes: Vec<_> = ["A", "B", "C"]
        .into_iter()
        .map(|name| CursorScope::Folder(FolderId(name.to_owned())))
        .collect();
    let result = account
        .push_subscribe(&scopes)
        .await
        .expect("a refused scope is a failed item, never a whole-request Err");
    let succeeded: Vec<_> = result
        .outcomes
        .succeeded()
        .iter()
        .map(|item| item.item.0.clone())
        .collect();
    assert_eq!(
        succeeded,
        vec!["0", "1"],
        "every folder up to and including the budget is pushed",
    );
    assert_eq!(result.outcomes.failed().len(), 1);
    assert_eq!(
        result.outcomes.failed()[0].item.0,
        "2",
        "the first folder past the budget, and only it, is rejected",
    );
    assert!(
        result.outcomes.failed()[0]
            .error
            .user_safe_text()
            .any(|text| text.contains("budget")),
        "a capacity refusal must not read as an unsupported scope shape",
    );
    assert!(
        result.handle.is_some(),
        "accepted scopes retain one subscription handle"
    );
    // Workers are capacity-many, not folder-many: the pool is started once
    // and idle slots park until a folder is admitted to them. The point of
    // the assertion is that a non-NOTIFY account runs its whole push
    // capacity rather than the single worker a NOTIFY account needs.
    assert_eq!(
        account.push_lanes().len(),
        2,
        "a non-NOTIFY account runs one IDLE worker per push session"
    );
    drop(server);
    account.close().await.unwrap();
}

/// Admission must accept exactly what worker assignment can watch. A folder
/// name containing CR/LF fails `MailboxName::new`, so `subscribed_idle_folders`
/// silently drops it and no IDLE worker can ever SELECT it. Admitting it
/// anyway would report it as pushed (a misreport bifrost-sync trusts) and
/// consume a budget slot that a valid folder later in the same request should
/// have received.
#[tokio::test]
async fn a_folder_scope_with_an_unsendable_name_is_refused_not_misreported() {
    use bifrost_types::{Account, CursorScope, FolderId};

    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
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
        .outcomes
        .succeeded()
        .iter()
        .map(|item| item.item.0.clone())
        .collect();
    assert_eq!(
        succeeded,
        vec!["1"],
        "the invalid name must not be reported as pushed, and must not block the valid sibling",
    );
    assert_eq!(result.outcomes.failed().len(), 1);
    assert_eq!(result.outcomes.failed()[0].item.0, "0");
    assert!(
        result.outcomes.failed()[0]
            .error
            .user_safe_text()
            .any(|text| text.contains("invalid mailbox name")),
        "the refusal names the cause",
    );
    drop(server);
    account.close().await.unwrap();
}

/// A composed DAV collection scope is a syntactically valid mailbox name but
/// names no mailbox. Push admission must refuse it per-item: admitting it
/// would report the collection as pushed (a misreport bifrost-sync trusts,
/// while the DAV sub-accounts have no push lane at all) and burn an IDLE
/// budget slot on a folder no worker can ever SELECT, displacing a real
/// mailbox later in the same request.
#[tokio::test]
async fn a_composed_dav_collection_scope_is_refused_by_push_admission() {
    use bifrost_types::{Account, CursorScope, FolderId};

    let collection = "https://dav.example.test/books/work/";
    let (dav_scopes, warnings) = super::DavScopeIndex::build(
        vec![FolderId(collection.to_owned())],
        Vec::new(),
        &std::collections::HashSet::new(),
    );
    assert!(warnings.is_empty());
    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_dav_account(conn, 2, false, None, FolderRegistry::default(), dav_scopes);
    let scopes = vec![
        CursorScope::Folder(FolderId(collection.to_owned())),
        CursorScope::Folder(FolderId("INBOX".to_owned())),
    ];
    let result = account
        .push_subscribe(&scopes)
        .await
        .expect("a refused scope is a failed item, never a whole-request Err");
    let succeeded: Vec<_> = result
        .outcomes
        .succeeded()
        .iter()
        .map(|item| item.item.0.clone())
        .collect();
    assert_eq!(
        succeeded,
        vec!["1"],
        "the DAV collection must not be reported as pushed, and must not block the mail sibling",
    );
    assert_eq!(result.outcomes.failed().len(), 1);
    assert_eq!(result.outcomes.failed()[0].item.0, "0");
    drop(server);
    account.close().await.unwrap();
}

/// DELETE must not be sent on a pooled connection that still has its target
/// selected. IMAP4rev2 provides UNSELECT, so this transcript proves the
/// affinity-free checkout explicitly deselects before STATUS/DELETE rather
/// than relying on a server accepting the selected-mailbox operation.
#[tokio::test]
async fn container_delete_deselects_a_target_left_selected_in_the_pool() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev2")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("Archive").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT"), "expected SELECT, got {select}");
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 0 EXISTS\r\n\
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
            "expected UNSELECT before DELETE, got {unselect}"
        );
        respond(
            &mut server,
            &format!("{} OK UNSELECT done\r\n", tag_of(&unselect)),
        )
        .await;

        let status = read_line(&mut server).await;
        assert!(status.contains("STATUS"), "expected STATUS, got {status}");
        respond(
            &mut server,
            &format!(
                "* STATUS Archive (MESSAGES 0)\r\n{} OK STATUS done\r\n",
                tag_of(&status)
            ),
        )
        .await;

        let delete = read_line(&mut server).await;
        assert!(delete.contains("DELETE"), "expected DELETE, got {delete}");
        respond(
            &mut server,
            &format!("{} OK DELETE done\r\n", tag_of(&delete)),
        )
        .await;

        let list = read_line(&mut server).await;
        assert!(list.contains("LIST"), "expected LIST, got {list}");
        respond(&mut server, &format!("{} OK LIST done\r\n", tag_of(&list))).await;
        server
    });

    let mut pooled = account.pool.checkout_any().await.unwrap();
    account
        .select_folder(&mut pooled, &folder, None, false)
        .await
        .unwrap();
    drop(pooled);

    super::pim::container_delete(account.clone(), ContainerId("Archive".to_owned()))
        .await
        .expect("selected target must be deselected before delete");
    let _server = script.await.unwrap();
}

fn rev2_select_ok(tag: &str, mailbox: &str) -> String {
    format!(
        "* FLAGS (\\Deleted \\Seen)\r\n\
         * 0 EXISTS\r\n\
         * LIST () \"/\" {mailbox}\r\n\
         * OK [UIDVALIDITY 1] selected\r\n\
         {tag} OK [READ-WRITE] SELECT done\r\n"
    )
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
        .select_folder(&mut pooled, &inbox, None, false)
        .await
        .unwrap();
    let err = account
        .select_folder(&mut pooled, &archive, None, false)
        .await
        .expect_err("a SELECT without EXISTS is incomplete");
    assert!(
        matches!(err, crate::Error::ProtocolMissing(_)),
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
        .select_folder(&mut pooled, &inbox, None, false)
        .await
        .unwrap();
    account
        .select_folder(&mut pooled, &archive, None, false)
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
        account.select_folder(&mut held, &folder, None, false),
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

// ---------------------------------------------------------------------------
// Sync half: strategy dispatch, VANISHED/FETCH dedup, downgrades, checkpoints.
//
// Every test below drives the real `changes_stream` over a canned transcript,
// so the strategy a cursor variant selects is proven by the bytes that reach
// the server, not by reading the dispatch match.
// ---------------------------------------------------------------------------

use bifrost_types::{
    Change, ChangeCursor, Checkpoint, DiagnosticText, ObjectChange, ObjectChangeKind, PageBoundary,
    ScopeChange, ScopeChangeKind, SyncEvent, SyncStrategy, Warning, WarningKind,
};

use crate::types::MailboxInfo;

use super::{CompactUidSet, FolderCursor};

fn inbox() -> crate::types::MailboxName {
    crate::types::MailboxName::new("INBOX").unwrap()
}

/// A registry holding just INBOX, so cursor and MODSEQ cache writes land.
/// An unregistered folder makes every registry write a silent no-op.
fn inbox_registry() -> FolderRegistry {
    FolderRegistry::from_list(vec![MailboxInfo {
        name: inbox(),
        ..MailboxInfo::default()
    }])
}

/// The mandatory SELECT/EXAMINE preamble (RFC 3501 Sections 6.3.1-6.3.2).
///
/// `SelectConsumer` validates FLAGS + EXISTS + RECENT before it reads a
/// single response code, so a transcript that omits them fails the command
/// with `Error::Protocol` and the whole changes stream terminates before
/// any strategy work runs. Every select transcript below starts here.
const SELECT_PREAMBLE: &str =
    "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n* 0 RECENT\r\n";

fn cursor_for(cursor: &FolderCursor) -> ChangeCursor {
    super::encode_cursor(super::folder_scope(&inbox()), cursor)
}

/// Drive `changes_stream` to completion, bounded so a regression that stalls
/// the pipeline fails fast instead of hanging the suite.
async fn collect_changes(account: ImapAccount, cursor: ChangeCursor) -> Vec<SyncEvent<Change>> {
    use futures::StreamExt;

    let mut stream = super::changes::changes_stream(account, cursor);
    let mut events = Vec::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("changes stream must make progress");
        let Some(event) = next else { break };
        let terminal = matches!(event, SyncEvent::Done(_) | SyncEvent::Terminated(_));
        events.push(event);
        if terminal {
            break;
        }
    }
    events
}

/// The change lanes flattened in emission order: `(object id, lane)`.
fn change_labels(events: &[SyncEvent<Change>]) -> Vec<(String, &'static str)> {
    let mut out = Vec::new();
    for event in events {
        let SyncEvent::Batch(batch) = event else {
            continue;
        };
        for change in &batch.items {
            match change {
                Change::ScopeChange(ScopeChange {
                    id,
                    kind: ScopeChangeKind::Added,
                    ..
                }) => out.push((id.0.clone(), "added")),
                Change::ScopeChange(ScopeChange {
                    id,
                    kind: ScopeChangeKind::Removed,
                    ..
                }) => out.push((id.0.clone(), "removed")),
                Change::ObjectChange(ObjectChange {
                    id,
                    kind: ObjectChangeKind::Updated,
                }) => out.push((id.0.clone(), "updated")),
                other => panic!("unexpected change: {other:?}"),
            }
        }
    }
    out
}

fn warnings_of(events: &[SyncEvent<Change>]) -> Vec<&Warning> {
    events
        .iter()
        .filter_map(|event| match event {
            SyncEvent::Warning(warning) => Some(warning),
            _ => None,
        })
        .collect()
}

/// The cursor carried by the terminal `Done`, decoded back to a `FolderCursor`.
fn done_cursor(events: &[SyncEvent<Change>]) -> FolderCursor {
    let last = events.last().expect("stream emits a terminal event");
    let SyncEvent::Done(checkpoint) = last else {
        panic!("expected Done, got {last:?}");
    };
    let Some(Checkpoint::Change(cursor)) = checkpoint else {
        panic!("changes must checkpoint a change cursor");
    };
    super::decode_cursor(cursor).expect("emitted checkpoint must decode")
}

fn id(uidvalidity: u32, uid: u32) -> String {
    super::encode_object_id(&inbox(), uidvalidity, uid).0
}

fn downgrade_detail(from: SyncStrategy, to: SyncStrategy) -> String {
    format!("{from:?}->{to:?}")
}

/// QRESYNC SELECT is the whole diff: broad VANISHED ranges intersect the
/// baseline, duplicate FETCHes collapse, and no second command is needed.
#[tokio::test]
async fn qresync_dedupes_vanished_against_fetch_and_checkpoints_the_live_set() {
    let (conn, mut server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    // The member is brought to QRESYNC when it is minted, as the factory does
    // for the primed connection and the pool for every dial.
    let enable = super::pool::enable_qresync(&conn, Duration::from_secs(5));
    let answer = async {
        let enable = read_line(&mut server).await;
        assert!(enable.contains("ENABLE QRESYNC"), "got {enable}");
        respond(
            &mut server,
            &format!(
                "* ENABLED QRESYNC\r\n{} OK ENABLE done\r\n",
                tag_of(&enable)
            ),
        )
        .await;
    };
    let (enabled, ()) = tokio::join!(enable, answer);
    enabled.unwrap();
    let account = scripted_sync_account(conn, 1, true, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE") && select.contains("QRESYNC (5 100 1:3)"),
            "a QRESYNC cursor must select with its uidvalidity/modseq/known-uid \
             baseline, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 2 EXISTS\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 * OK [HIGHESTMODSEQ 200] ok\r\n\
                 * VANISHED (EARLIER) 1:4000000000\r\n\
                 * 1 FETCH (UID 3 FLAGS (\\Seen) MODSEQ (150))\r\n\
                 * 2 FETCH (UID 2 FLAGS (\\Seen) MODSEQ (180))\r\n\
                 * 1 FETCH (UID 3 FLAGS (\\Seen) MODSEQ (150))\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::QResync {
            uidvalidity: 5,
            modseq: 100,
            known_uids: CompactUidSet::from_uids([1, 2, 3]),
            known_uids_complete: true,
        }),
    )
    .await;

    assert!(
        warnings_of(&events).is_empty(),
        "a clean QRESYNC round must not warn: {:?}",
        warnings_of(&events)
    );
    assert_eq!(
        change_labels(&events),
        vec![
            (id(5, 1), "removed"),
            (id(5, 3), "updated"),
            (id(5, 2), "updated"),
        ],
        "UID 2 was VANISHED then re-FETCHed, so it must land in one lane only, \
         and the twice-FETCHed UID 3 must be emitted once"
    );

    match done_cursor(&events) {
        FolderCursor::QResync {
            uidvalidity,
            modseq,
            known_uids,
            known_uids_complete,
        } => {
            assert_eq!((uidvalidity, modseq), (5, 200));
            assert_eq!(known_uids.to_uids(), vec![2, 3]);
            assert!(known_uids_complete);
        }
        other => panic!("QRESYNC must checkpoint a QRESYNC cursor, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

/// The known-UID SELECT need not FETCH arrivals, even when UIDNEXT and
/// HIGHESTMODSEQ advance. Membership discovery must precede the checkpoint.
#[tokio::test]
async fn qresync_discovers_delivery_outside_the_select_known_uids() {
    qresync_delivery_transcript(false, false).await;
}

/// An arrival can replace an expunge without changing the baseline count.
/// Compare EXISTS with the set after applying VANISHED, not the old count.
#[tokio::test]
async fn qresync_discovers_delivery_when_an_expunge_keeps_exists_unchanged() {
    qresync_delivery_transcript(true, false).await;
}

#[tokio::test]
async fn qresync_delivery_search_failure_does_not_checkpoint_new_modseq() {
    qresync_delivery_transcript(false, true).await;
}

async fn qresync_delivery_transcript(expunge: bool, search_fails: bool) {
    let (conn, mut server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    let enable = super::pool::enable_qresync(&conn, Duration::from_secs(5));
    let answer = async {
        let command = read_line(&mut server).await;
        assert!(command.contains("ENABLE QRESYNC"), "{command}");
        respond(
            &mut server,
            &format!("* ENABLED QRESYNC\r\n{} OK enabled\r\n", tag_of(&command)),
        )
        .await;
    };
    let (enabled, ()) = tokio::join!(enable, answer);
    enabled.unwrap();
    let account = scripted_sync_account(conn, 1, true, None, inbox_registry());
    let baseline = FolderCursor::QResync {
        uidvalidity: 5,
        modseq: 100,
        known_uids: CompactUidSet::from_uids([1, 2]),
        known_uids_complete: true,
    };
    account.folders.set_cursor(&inbox(), baseline.clone());
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("QRESYNC (5 100 1:2)"), "{select}");
        let exists = if expunge { 2 } else { 3 };
        let vanished = if expunge {
            "* VANISHED (EARLIER) 1\r\n"
        } else {
            ""
        };
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * {exists} EXISTS\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 4] ok\r\n\
                 * OK [HIGHESTMODSEQ 200] ok\r\n\
                 {vanished}{} OK [READ-ONLY] selected\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let search = read_line(&mut server).await;
        assert!(search.contains("UID SEARCH ALL"), "{search}");
        let reply = if search_fails {
            format!("{} NO discovery failed\r\n", tag_of(&search))
        } else {
            let uids = if expunge { "2 3" } else { "1 2 3" };
            format!("* SEARCH {uids}\r\n{} OK searched\r\n", tag_of(&search))
        };
        respond(&mut server, &reply).await;
        server
    });
    let events = collect_changes(account.clone(), cursor_for(&baseline)).await;
    if search_fails {
        assert!(matches!(events.last(), Some(SyncEvent::Terminated(_))));
        assert!(!events.iter().any(|event| match event {
            SyncEvent::Done(_) => true,
            SyncEvent::Batch(batch) => batch.checkpoint.is_some(),
            _ => false,
        }));
        assert!(matches!(
            account.folders.get(&inbox()).unwrap().cursor(),
            Some(FolderCursor::QResync { modseq: 100, .. })
        ));
    } else {
        let expected = if expunge {
            vec![(id(5, 1), "removed"), (id(5, 3), "added")]
        } else {
            vec![(id(5, 3), "added")]
        };
        assert_eq!(change_labels(&events), expected);
        assert!(warnings_of(&events).is_empty());
        match done_cursor(&events) {
            FolderCursor::QResync {
                modseq,
                known_uids,
                known_uids_complete,
                ..
            } => {
                assert_eq!(modseq, 200);
                let expected_uids = if expunge { vec![2, 3] } else { vec![1, 2, 3] };
                assert_eq!(known_uids.to_uids(), expected_uids);
                assert!(known_uids_complete);
            }
            other => panic!("expected QRESYNC checkpoint, got {other:?}"),
        }
    }
    let _server = script.await.unwrap();
}

/// A legacy QRESYNC cursor without an exact UID baseline cannot be diffed.
/// Current server membership cannot reconstruct what the consumer persisted,
/// so the stream requests a scope restart before issuing any command.
#[tokio::test]
async fn incomplete_qresync_baseline_requests_inventory_restart_without_wire_io() {
    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(
        conn,
        1,
        false,
        Some("server did not echo ENABLED QRESYNC".to_owned()),
        inbox_registry(),
    );

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::QResync {
            uidvalidity: 5,
            modseq: 100,
            known_uids: CompactUidSet::from_uids([1, 2]),
            known_uids_complete: false,
        }),
    )
    .await;

    let Some(SyncEvent::Terminated(error)) = events.last() else {
        panic!("incomplete baseline must terminate with a restart request: {events:?}");
    };
    assert!(matches!(
        error.recovery(),
        bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(_))
    ));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SyncEvent::Batch(_)))
    );
    drop(server);
}

/// CONDSTORE with a complete baseline: flags come from CHANGEDSINCE, expunges
/// and arrivals from diffing `UID SEARCH ALL` against the cursor's UID set.
/// The SEARCH follows the FETCH here (unlike the seeding round above), which
/// is what makes the checkpointed UID snapshot current.
#[tokio::test]
async fn condstore_cursor_diffs_changedsince_flags_against_a_uid_search_snapshot() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE") && select.contains("CONDSTORE"),
            "a CONDSTORE cursor selects with the CONDSTORE parameter, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 3 EXISTS\r\n\
                 * OK [UIDVALIDITY 7] ok\r\n\
                 * OK [UIDNEXT 12] ok\r\n\
                 * OK [HIGHESTMODSEQ 60] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("CHANGEDSINCE 50"),
            "expected CHANGEDSINCE against the cursor modseq, got {fetch}"
        );
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 2 FLAGS (\\Seen) MODSEQ (55))\r\n\
                 {} OK UID FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH ALL"),
            "CONDSTORE detects expunges by UID-list diff, got {search}"
        );
        respond(
            &mut server,
            &format!(
                "* SEARCH 2 3 4\r\n{} OK UID SEARCH done\r\n",
                tag_of(&search)
            ),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Condstore {
            uidvalidity: 7,
            modseq: 50,
            known_uids: CompactUidSet::from_uids([1, 2, 3]),
        }),
    )
    .await;

    assert!(
        warnings_of(&events).is_empty(),
        "{:?}",
        warnings_of(&events)
    );
    assert_eq!(
        change_labels(&events),
        vec![
            (id(7, 2), "updated"),
            (id(7, 4), "added"),
            (id(7, 1), "removed"),
        ]
    );
    match done_cursor(&events) {
        FolderCursor::Condstore {
            uidvalidity,
            modseq,
            known_uids,
        } => {
            assert_eq!((uidvalidity, modseq), (7, 60));
            assert_eq!(known_uids.to_uids(), vec![2, 3, 4]);
        }
        other => panic!("expected a CONDSTORE checkpoint, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn condstore_changes_crossing_batch_limit_are_paged() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}* 129 EXISTS\r\n* OK [UIDVALIDITY 7] ok\r\n\
                 * OK [UIDNEXT 130] ok\r\n* OK [HIGHESTMODSEQ 60] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let fetch = read_line(&mut server).await;
        let mut response = String::new();
        for uid in 1..=129 {
            response.push_str(&format!(
                "* {uid} FETCH (UID {uid} FLAGS (\\Seen) MODSEQ (55))\r\n"
            ));
        }
        response.push_str(&format!("{} OK UID FETCH done\r\n", tag_of(&fetch)));
        respond(&mut server, &response).await;
        let search = read_line(&mut server).await;
        let ids = (1..=129)
            .map(|uid| uid.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        respond(
            &mut server,
            &format!(
                "* SEARCH {ids}\r\n{} OK UID SEARCH done\r\n",
                tag_of(&search)
            ),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Condstore {
            uidvalidity: 7,
            modseq: 50,
            known_uids: CompactUidSet::from_uids(1..=129),
        }),
    )
    .await;
    let boundaries = events
        .iter()
        .filter_map(|event| match event {
            SyncEvent::Batch(batch) => Some(batch.page_boundary),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(boundaries, vec![PageBoundary::Page, PageBoundary::Final]);
    assert_eq!(change_labels(&events).len(), 129);
    let _server = script.await.unwrap();
}

/// A QResync cursor run on a member that does not have QRESYNC enabled (here
/// the server advertises QRESYNC without ENABLE, so no member can) selects
/// with CONDSTORE instead - `select_for_sync` never issues ENABLE - and the
/// run continues as CONDSTORE on the SAME checkout, with one downgrade
/// warning. At `pool_cap = 1` a re-checkout would hang (and fail on
/// `collect_changes`' timeout).
///
/// The checkpoint is minted by policy, not by this member's shortfall: the
/// account policy allows QRESYNC, the mailbox has persistent mod-sequences
/// and the UID baseline is a complete SEARCH snapshot, so it is a QResync
/// cursor at the SELECT's HIGHESTMODSEQ. A Condstore checkpoint here would
/// demote the folder for good because one member could not do QRESYNC.
#[tokio::test]
async fn a_qresync_run_on_a_member_without_qresync_continues_as_condstore_on_the_same_checkout() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 QRESYNC CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, true, None, inbox_registry());

    let script = tokio::spawn(async move {
        // No ENABLE: the first command on the wire is the CONDSTORE EXAMINE.
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE")
                && select.contains("CONDSTORE")
                && !select.contains("QRESYNC"),
            "expected a CONDSTORE EXAMINE as the first command, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 3 EXISTS\r\n\
                 * OK [UIDVALIDITY 7] ok\r\n\
                 * OK [UIDNEXT 12] ok\r\n\
                 * OK [HIGHESTMODSEQ 60] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("CHANGEDSINCE 50") && !fetch.contains("VANISHED"),
            "the CONDSTORE continuation diffs against the QRESYNC cursor's modseq, got {fetch}"
        );
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 2 FLAGS (\\Seen) MODSEQ (55))\r\n\
                 {} OK UID FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH ALL"),
            "CONDSTORE detects expunges by UID-list diff, got {search}"
        );
        respond(
            &mut server,
            &format!(
                "* SEARCH 2 3 4\r\n{} OK UID SEARCH done\r\n",
                tag_of(&search)
            ),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::QResync {
            uidvalidity: 7,
            modseq: 50,
            known_uids: CompactUidSet::from_uids([1, 2, 3]),
            known_uids_complete: true,
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(warnings.len(), 1, "one downgrade warning: {warnings:?}");
    assert_eq!(warnings[0].kind, WarningKind::StrategyDowngraded);
    assert_eq!(
        warnings[0]
            .protocol_detail
            .as_ref()
            .map(DiagnosticText::as_str),
        Some(downgrade_detail(SyncStrategy::QResync, SyncStrategy::Condstore).as_str())
    );
    assert_eq!(
        change_labels(&events),
        vec![
            (id(7, 2), "updated"),
            (id(7, 4), "added"),
            (id(7, 1), "removed"),
        ]
    );
    match done_cursor(&events) {
        FolderCursor::QResync {
            uidvalidity,
            modseq,
            known_uids,
            known_uids_complete,
        } => {
            assert_eq!((uidvalidity, modseq), (7, 60));
            assert_eq!(known_uids.to_uids(), vec![2, 3, 4]);
            assert!(known_uids_complete);
        }
        other => panic!("expected a policy-minted QRESYNC checkpoint, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

/// The defect this pins: a pool member that has already selected a mailbox
/// (inventory selects with no cursor) and does not have QRESYNC enabled is
/// handed a QResync-cursor run, which checkout affinity makes likely. ENABLE
/// is illegal there (RFC 5161 Section 3.1), and the run used to issue it
/// lazily from `select_for_sync`, be refused, and fail. Now no ENABLE ever
/// reaches the wire: the member selects with CONDSTORE and the run completes
/// on it, checkpointing by policy.
///
/// Restoring the lazy ENABLE in `select_for_sync` fails this: the ENABLE is
/// refused with `InvalidState` and the stream terminates instead of
/// checkpointing.
#[tokio::test]
async fn a_qresync_run_on_a_member_that_already_selected_never_issues_enable() {
    let (conn, mut server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    let account = scripted_sync_account(conn, 1, true, None, inbox_registry());

    let script = tokio::spawn(async move {
        let examine = |tag: &str| {
            format!(
                "{SELECT_PREAMBLE}\
                 * 3 EXISTS\r\n\
                 * OK [UIDVALIDITY 7] ok\r\n\
                 * OK [UIDNEXT 12] ok\r\n\
                 * OK [HIGHESTMODSEQ 60] ok\r\n\
                 {tag} OK [READ-ONLY] EXAMINE done\r\n"
            )
        };
        // The inventory-style selection, with no cursor.
        let first = read_line(&mut server).await;
        assert!(first.contains("EXAMINE"), "got {first}");
        respond(&mut server, &examine(tag_of(&first))).await;

        // The QResync-cursor run on the same, already-selected member.
        let second = read_line(&mut server).await;
        assert!(
            second.contains("EXAMINE")
                && second.contains("CONDSTORE")
                && !second.contains("QRESYNC"),
            "no ENABLE may precede the run's select, and it may not ask for \
             QRESYNC on a member without it; got {second}"
        );
        respond(&mut server, &examine(tag_of(&second))).await;

        let fetch = read_line(&mut server).await;
        assert!(fetch.contains("CHANGEDSINCE 50"), "got {fetch}");
        respond(
            &mut server,
            &format!("{} OK UID FETCH done\r\n", tag_of(&fetch)),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(search.contains("UID SEARCH ALL"), "got {search}");
        respond(
            &mut server,
            &format!(
                "* SEARCH 1 2 3\r\n{} OK UID SEARCH done\r\n",
                tag_of(&search)
            ),
        )
        .await;
        server
    });

    {
        let mut pooled = account.pool.checkout_for_folder(&inbox()).await.unwrap();
        account
            .select_folder(&mut pooled, &inbox(), None, true)
            .await
            .unwrap();
    }

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::QResync {
            uidvalidity: 7,
            modseq: 50,
            known_uids: CompactUidSet::from_uids([1, 2, 3]),
            known_uids_complete: true,
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(warnings.len(), 1, "one downgrade warning: {warnings:?}");
    assert_eq!(warnings[0].kind, WarningKind::StrategyDowngraded);
    assert!(
        matches!(
            done_cursor(&events),
            FolderCursor::QResync { modseq: 60, .. }
        ),
        "the run must checkpoint, by policy, at the SELECT's HIGHESTMODSEQ"
    );
    let _server = script.await.unwrap();
}

/// A Basic cursor on a server with neither extension: no CONDSTORE parameter,
/// no CHANGEDSINCE, both lanes derived from the UID-list diff, and the
/// checkpoint carrying UIDNEXT rather than a mod-sequence.
#[tokio::test]
async fn basic_cursor_derives_both_lanes_from_a_uid_search_diff() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE")
                && !select.contains("CONDSTORE")
                && !select.contains("QRESYNC"),
            "a server advertising neither extension gets a bare EXAMINE, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 2 EXISTS\r\n\
                 * OK [UIDVALIDITY 3] ok\r\n\
                 * OK [UIDNEXT 11] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH ALL") && !search.contains("CHANGEDSINCE"),
            "expected the plain UID SEARCH ALL diff, got {search}"
        );
        respond(
            &mut server,
            &format!("* SEARCH 2 5\r\n{} OK UID SEARCH done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Basic {
            uidvalidity: 3,
            uidnext: 10,
            known_uids: CompactUidSet::from_uids([1, 2]),
        }),
    )
    .await;

    assert!(
        warnings_of(&events).is_empty(),
        "{:?}",
        warnings_of(&events)
    );
    assert_eq!(
        change_labels(&events),
        vec![(id(3, 5), "added"), (id(3, 1), "removed")]
    );
    match done_cursor(&events) {
        FolderCursor::Basic {
            uidvalidity,
            uidnext,
            known_uids,
        } => {
            assert_eq!((uidvalidity, uidnext), (3, 11));
            assert_eq!(known_uids.to_uids(), vec![2, 5]);
        }
        other => panic!("expected a Basic checkpoint, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn basic_unchanged_uidnext_and_count_skip_uid_search() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}* 2 EXISTS\r\n* OK [UIDVALIDITY 3] ok\r\n\
                 * OK [UIDNEXT 10] ok\r\n{} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        server
    });
    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Basic {
            uidvalidity: 3,
            uidnext: 10,
            known_uids: CompactUidSet::from_uids([1, 2]),
        }),
    )
    .await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SyncEvent::Batch(_)))
    );
    assert_eq!(done_cursor(&events).known_uids().to_uids(), vec![1, 2]);
    let _server = script.await.unwrap();
}

/// A mailbox that answers the CONDSTORE select with `[NOMODSEQ]` downgrades to
/// Basic on the connection it already holds - two commands total, no second
/// checkout - and checkpoints a Basic cursor so the next round does not ask
/// for mod-sequences again.
#[tokio::test]
async fn nomodseq_mailbox_downgrades_condstore_to_basic_on_the_selected_connection() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("EXAMINE"), "expected EXAMINE, got {select}");
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 2 EXISTS\r\n\
                 * OK [UIDVALIDITY 9] ok\r\n\
                 * OK [UIDNEXT 7] ok\r\n\
                 * OK [NOMODSEQ] no mod-sequences\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH ALL"),
            "the Basic fallback must reuse the already selected connection, got {search}"
        );
        respond(
            &mut server,
            &format!("* SEARCH 1 2\r\n{} OK UID SEARCH done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Condstore {
            uidvalidity: 9,
            modseq: 40,
            known_uids: CompactUidSet::from_uids([1, 2]),
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(
        warnings.len(),
        1,
        "expected one downgrade warning: {warnings:?}"
    );
    assert_eq!(warnings[0].kind, WarningKind::StrategyDowngraded);
    assert_eq!(
        warnings[0]
            .protocol_detail
            .as_ref()
            .map(DiagnosticText::as_str),
        Some(downgrade_detail(SyncStrategy::Condstore, SyncStrategy::Basic).as_str())
    );
    assert!(
        !events.iter().any(|e| matches!(e, SyncEvent::Batch(_))),
        "nothing moved, so the downgrade must not manufacture a batch"
    );
    match done_cursor(&events) {
        FolderCursor::Basic {
            uidvalidity,
            uidnext,
            known_uids,
        } => {
            assert_eq!((uidvalidity, uidnext), (9, 7));
            assert_eq!(known_uids.to_uids(), vec![1, 2]);
        }
        other => panic!("a NOMODSEQ mailbox must checkpoint Basic, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

/// The `[NOMODSEQ]` SELECT carries changed-message FETCH data, and RFC 7162
/// has the server include messages that arrived above the client's MODSEQ. The
/// Basic run must report the pre-existing UID as `Updated` and the arrival as
/// `Added` exactly once - never `Updated` for a UID the consumer has no
/// membership record of, and never a scope restart, which would throw away the
/// whole folder on ordinary traffic.
#[tokio::test]
async fn nomodseq_downgrade_splits_selected_fetches_between_updated_and_added() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 2 EXISTS\r\n\
                 * OK [UIDVALIDITY 9] ok\r\n\
                 * OK [UIDNEXT 8] ok\r\n\
                 * 1 FETCH (UID 1 FLAGS (\\Seen))\r\n\
                 * 2 FETCH (UID 7 FLAGS (\\Recent))\r\n\
                 * OK [NOMODSEQ] no mod-sequences\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let search = read_line(&mut server).await;
        respond(
            &mut server,
            &format!("* SEARCH 1 7\r\n{} OK UID SEARCH done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Condstore {
            uidvalidity: 9,
            modseq: 40,
            known_uids: CompactUidSet::from_uids([1]),
        }),
    )
    .await;

    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SyncEvent::Terminated(_))),
        "an arrival in the downgrade SELECT is routine, not a cursor fault: {events:?}"
    );
    assert_eq!(
        change_labels(&events),
        vec![(id(9, 1), "updated"), (id(9, 7), "added")],
        "the arrival belongs to the Added lane alone"
    );
    let _server = script.await.unwrap();
}

/// A UIDVALIDITY change is terminal for the scope, not a diff: the stream
/// stops after the SELECT with a `RestartScope` directive and publishes no
/// batch, because every id minted under the old epoch is now stale.
#[tokio::test]
async fn uidvalidity_change_terminates_the_stream_without_publishing_a_diff() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("EXAMINE"), "expected EXAMINE, got {select}");
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 1 EXISTS\r\n\
                 * OK [UIDVALIDITY 8] rebuilt\r\n\
                 * OK [UIDNEXT 2] ok\r\n\
                 * OK [HIGHESTMODSEQ 3] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Condstore {
            uidvalidity: 7,
            modseq: 50,
            known_uids: CompactUidSet::from_uids([1]),
        }),
    )
    .await;

    let last = events.last().expect("terminal event");
    let SyncEvent::Terminated(error) = last else {
        panic!("a UIDVALIDITY change must terminate the stream, got {last:?}");
    };
    assert!(
        matches!(
            error.recovery(),
            bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(
                bifrost_types::CursorScope::Folder(_)
            ))
        ),
        "expected RestartScope, got {:?}",
        error.recovery()
    );
    assert!(
        !events.iter().any(|e| matches!(e, SyncEvent::Batch(_))),
        "no diff may be published across a UIDVALIDITY epoch break"
    );
    let _server = script.await.unwrap();
}

/// `describe_cursor` is the engine's scheduling hint, so it must agree with
/// what `changes_stream` will actually do. The load-bearing case is the
/// QRESYNC cursor on a session where QRESYNC was disabled: the run downgrades
/// to CONDSTORE, and the descriptor has to say Condstore/Medium rather than
/// promising the cheap QRESYNC round.
#[tokio::test]
async fn describe_cursor_reports_the_strategy_the_run_will_actually_use() {
    use bifrost_types::CostClass;

    let qresync_cursor = cursor_for(&FolderCursor::QResync {
        uidvalidity: 1,
        modseq: 1,
        known_uids: CompactUidSet::default(),
        known_uids_complete: true,
    });
    let condstore_cursor = cursor_for(&FolderCursor::Condstore {
        uidvalidity: 1,
        modseq: 1,
        known_uids: CompactUidSet::default(),
    });
    let basic_cursor = cursor_for(&FolderCursor::Basic {
        uidvalidity: 1,
        uidnext: 1,
        known_uids: CompactUidSet::default(),
    });

    let (conn, _server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    let enabled = scripted_sync_account(conn, 1, true, None, inbox_registry());
    let described = super::changes::describe_cursor(&enabled, &qresync_cursor);
    assert_eq!(described.strategy, SyncStrategy::QResync);
    assert_eq!(described.cost_class, CostClass::Cheap);

    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let disabled = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let described = super::changes::describe_cursor(&disabled, &qresync_cursor);
    assert_eq!(
        described.strategy,
        SyncStrategy::Condstore,
        "a QRESYNC cursor on a downgraded session must be described as CONDSTORE"
    );
    assert_eq!(described.cost_class, CostClass::Medium);

    let described = super::changes::describe_cursor(&disabled, &condstore_cursor);
    assert_eq!(described.strategy, SyncStrategy::Condstore);
    assert_eq!(described.cost_class, CostClass::Medium);

    let described = super::changes::describe_cursor(&disabled, &basic_cursor);
    assert_eq!(described.strategy, SyncStrategy::Basic);
    assert_eq!(described.cost_class, CostClass::Expensive);
}

/// EXISTS disagreeing with the derived live UID set is a server-consistency
/// signal, not a fatal: the round still checkpoints, but it carries a warning
/// naming both counts so the drift is visible to the engine.
#[tokio::test]
async fn uid_count_disagreeing_with_exists_warns_but_still_checkpoints() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 9 EXISTS\r\n\
                 * OK [UIDVALIDITY 3] ok\r\n\
                 * OK [UIDNEXT 4] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let search = read_line(&mut server).await;
        respond(
            &mut server,
            &format!("* SEARCH 1 2\r\n{} OK UID SEARCH done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Basic {
            uidvalidity: 3,
            uidnext: 3,
            known_uids: CompactUidSet::from_uids([1, 2]),
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0].kind, WarningKind::Other);
    assert!(
        warnings[0].message.as_str().contains("SELECT EXISTS 9"),
        "the warning must carry both counts: {}",
        warnings[0].message.as_str()
    );
    assert!(matches!(done_cursor(&events), FolderCursor::Basic { .. }));
    let _server = script.await.unwrap();
}

/// `PageBoundary::Final` plus the checkpoint on the last batch, and the same
/// checkpoint repeated on `Done`. A consumer that commits on either one must
/// land on the identical cursor.
#[tokio::test]
async fn the_final_batch_and_done_carry_the_same_checkpoint() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 1 EXISTS\r\n\
                 * OK [UIDVALIDITY 3] ok\r\n\
                 * OK [UIDNEXT 6] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let search = read_line(&mut server).await;
        respond(
            &mut server,
            &format!("* SEARCH 5\r\n{} OK UID SEARCH done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Basic {
            uidvalidity: 3,
            uidnext: 5,
            known_uids: CompactUidSet::default(),
        }),
    )
    .await;

    let batch = events
        .iter()
        .find_map(|event| match event {
            SyncEvent::Batch(batch) => Some(batch),
            _ => None,
        })
        .expect("one batch");
    assert!(matches!(batch.page_boundary, PageBoundary::Final));
    let Some(Checkpoint::Change(batch_cursor)) = batch.checkpoint.as_ref() else {
        panic!("the final batch must carry the checkpoint");
    };
    let batch_cursor = super::decode_cursor(batch_cursor).expect("decodes");
    assert_eq!(
        batch_cursor,
        done_cursor(&events),
        "committing on the final batch and committing on Done must agree"
    );
    let _server = script.await.unwrap();
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
    assert!(matches!(err, crate::Error::Closed { .. }), "got {err:?}");
}

/// The two blob openers are statically unsupported (`blob_range` is
/// `BlobRangeSupport::No`, and nothing mints a `BlobHandle`), so both must
/// terminate the stream with `Unsupported(<their own operation>)` and then
/// end immediately - never append `Done` after `Terminated`, hang, or
/// mis-tag the operation, which is what the engine's recovery mapping keys on.
#[tokio::test]
async fn the_blob_openers_terminate_unsupported_with_their_own_operation() {
    use bifrost_types::{
        Account, AccountErrorKind, AccountOperation, BlobCapabilities, BlobEncoding, BlobHandle,
        BlobId, ByteRange,
    };
    use futures::StreamExt;

    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);

    let handle = || BlobHandle {
        id: BlobId("imapblob1:INBOX:1:2:1.2".to_owned()),
        size: None,
        content_type: None,
        digest: None,
        capabilities: BlobCapabilities {
            supports_range: false,
            supports_parallel: false,
            digest_available_pre_download: false,
            encoding: BlobEncoding::Raw8Bit,
        },
    };

    for (mut stream, op) in [
        (account.open_blob(handle()), AccountOperation::OpenBlob),
        (
            account.open_blob_range(
                handle(),
                ByteRange {
                    start: 0,
                    length: Some(16),
                },
            ),
            AccountOperation::OpenBlobRange,
        ),
    ] {
        let first = stream.next().await.expect("the stream emits a terminal");
        let SyncEvent::Terminated(err) = first else {
            panic!("expected Terminated, got {first:?}");
        };
        assert_eq!(err.kind(), &AccountErrorKind::Unsupported(op));
        assert_eq!(err.operation(), Some(op));
        assert!(stream.next().await.is_none(), "Terminated ends the stream");
    }
}

/// `open_raw_rfc822` must STREAM, not buffer the whole message.
///
/// The transcript gates the second half of the FETCH behind the test having
/// already received the first chunk, so a buffered `uid_fetch` - which
/// emits nothing until the tagged completion - deadlocks and fails on the
/// timeout instead of quietly holding an arbitrarily large message in one
/// `Vec<FetchResponse>`.
#[tokio::test]
async fn open_raw_rfc822_emits_chunks_before_the_tagged_completion() {
    use bytes::Bytes;
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE") || select.contains("SELECT"),
            "expected a select, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(fetch.contains("FETCH"), "expected UID FETCH, got {fetch}");
        respond(&mut server, "* 1 FETCH (UID 7 BODY[] {5}\r\nhello)\r\n").await;
        // Nothing more until the consumer has seen the first chunk.
        gate_rx.await.expect("the consumer must reach the gate");
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 7 BODY[] {{5}}\r\nworld)\r\n{} OK FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
        server
    });

    let message = super::encode_object_id(&folder, 5, 7);
    let mut stream = super::blob::open_raw_rfc822(account, message);

    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("a chunk must arrive before the tagged OK")
        .expect("the stream emits a batch");
    let SyncEvent::Batch(batch) = first else {
        panic!("expected a Batch, got {first:?}");
    };
    assert_eq!(batch.items, vec![Bytes::from_static(b"hello")]);
    gate_tx.send(()).expect("the script is still running");

    let mut rest = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the stream must finish")
    {
        match event {
            SyncEvent::Batch(batch) => rest.extend(batch.items),
            SyncEvent::Done(_) => break,
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert_eq!(rest, vec![Bytes::from_static(b"world")]);
    let _server = script.await.unwrap();
}

/// The raw-message lane carries a byte budget like every other body path.
///
/// Without one, a corrupt or adversarial server can answer a single
/// `BODY.PEEK[]` with an unbounded number of octets and this lane forwards
/// all of them. The budget is exercised at 3 bytes against a 5-byte body;
/// the reader refuses it at the literal marker before materializing the body;
/// the connection is retired because the unread literal cannot be skipped.
#[tokio::test]
async fn a_raw_message_read_stops_at_its_byte_budget() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let fetch = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 7 BODY[] {{5}}\r\nhello)\r\n{} OK FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
        server
    });

    let (tx, _rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        super::blob::run_fetch(
            &account,
            &folder,
            5,
            7,
            crate::types::FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: None,
            },
            bifrost_types::AccountOperation::OpenRawRfc822,
            3,
            &tx,
        ),
    )
    .await
    .expect("the budgeted read must finish");

    match outcome {
        Err(super::blob::BlobError::Imap(crate::Error::FetchLimit { limit, uid, .. })) => {
            assert_eq!(limit, 3);
            assert_eq!(uid, Some(7));
        }
        Err(super::blob::BlobError::Imap(other)) => panic!("unexpected imap error: {other:?}"),
        Err(super::blob::BlobError::Account(err)) => panic!("unexpected account error: {err:?}"),
        Err(super::blob::BlobError::ChannelDropped) => panic!("the receiver was held"),
        Ok(()) => panic!("a body past the budget must not stream in full"),
    }
    let _server = script.await.unwrap();
}

/// A UID the shared operand cannot carry never reaches the wire, and never
/// completes as a successful empty read.
///
/// `decode_object_id` rejects UID 0 today, so this is reachable only by
/// calling the fetch core directly - which is the point: the invariant is
/// the operand's, not the decoder's. Before the operand was shared, this
/// lane skipped the FETCH and returned `Ok(())`, presenting "nothing was
/// asked" as "the message has no body".
#[tokio::test]
async fn a_uid_the_operand_cannot_carry_never_reaches_the_fetch() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        server
    });

    let (tx, _rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        super::blob::run_fetch(
            &account,
            &folder,
            5,
            0,
            crate::types::FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: None,
            },
            bifrost_types::AccountOperation::OpenRawRfc822,
            1024,
            &tx,
        ),
    )
    .await
    .expect("the refused read must finish");

    match outcome {
        Err(super::blob::BlobError::Account(err)) => assert!(
            matches!(
                err.kind(),
                bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                )
            ),
            "a UID the operand cannot carry is a client-side request defect: {err:?}"
        ),
        Ok(()) => panic!("a UID the server never saw must not complete as a successful read"),
        Err(_) => panic!("unexpected error variant"),
    }
    let _server = script.await.unwrap();
}

/// The account knob retunes the budget held by every protocol leg.
#[tokio::test]
async fn the_bandwidth_cap_knob_updates_the_budget_and_clamps_zero() {
    use bifrost_types::Account;
    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let shared = account.bandwidth_budget.clone();
    account.set_bandwidth_cap(Some(4096));
    assert_eq!(shared.cap(), Some(4096));
    account.set_bandwidth_cap(None);
    assert_eq!(shared.cap(), None);
    account.set_bandwidth_cap(Some(0));
    assert_eq!(shared.cap(), Some(1));
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

/// Drive one PIM entry point against a server whose SELECT/EXAMINE completes
/// without the mandatory `UIDVALIDITY`, and return the error it surfaces.
async fn pim_error_on_select_without_uidvalidity<T: std::fmt::Debug>(
    call: impl FnOnce(
        ImapAccount,
    ) -> bifrost_types::AccountFuture<Result<T, bifrost_types::AccountError>>,
) -> bifrost_types::AccountError {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let mut caps = super::test_support::stub_capabilities();
    caps.pim_methods.add_to_container = true;
    caps.pim_methods.message_hydrate = true;
    caps.pim_methods.search = true;
    caps.pim_methods.search_messages = true;
    caps.pim_methods.remove_from_container = true;
    caps.pim_methods.set_is_read = true;
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\"") || select.contains("SELECT \"INBOX\""),
            "expected a select of INBOX, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 {} OK done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        server
    });

    let err = tokio::time::timeout(Duration::from_secs(5), call(account))
        .await
        .expect("the PIM call must not hang")
        .expect_err("a SELECT without UIDVALIDITY cannot yield usable ids");
    let _server = script.await.unwrap();
    err
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
    let id = super::encode_object_id(&inbox, 5, 3);
    let in_inbox = || SearchRequest::filter(SearchFilter::In(ContainerId("INBOX".to_owned())));

    let mut cases: Vec<(AccountOperation, bifrost_types::AccountError)> = Vec::new();
    {
        let id = id.clone();
        cases.push((
            AccountOperation::AddToContainer,
            pim_error_on_select_without_uidvalidity(move |account| {
                super::pim::add_to_container(
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
                super::pim::remove_from_container(
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
                super::pim::set_is_read(account, MutationTarget::Message(id), true)
            })
            .await,
        ));
    }
    {
        let id = id.clone();
        cases.push((
            AccountOperation::HydrateMessage,
            pim_error_on_select_without_uidvalidity(move |account| {
                super::pim::message_hydrate(account, id, HydrationProjection::Full)
            })
            .await,
        ));
    }
    cases.push((
        AccountOperation::Search,
        pim_error_on_select_without_uidvalidity(move |account| {
            super::pim::search(account, in_inbox())
        })
        .await,
    ));
    cases.push((
        AccountOperation::SearchMessages,
        pim_error_on_select_without_uidvalidity(move |account| {
            super::pim::search_messages(account, in_inbox())
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

/// Run `message_hydrate` for INBOX uid 3 (UIDVALIDITY 5) against a server that
/// selects INBOX at UIDVALIDITY 5 and answers the UID FETCH with `fetch_data`
/// (untagged FETCH lines, possibly none) before its tagged OK.
async fn hydrate_uid_3_against(
    fetch_data: &'static str,
) -> Result<bifrost_types::hydration::Message, bifrost_types::AccountError> {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let mut caps = super::test_support::stub_capabilities();
    caps.pim_methods.message_hydrate = true;
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\""),
            "expected EXAMINE, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Seen)\r\n\
                 * 9 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 10] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("UID FETCH 3"),
            "expected UID FETCH 3, got {fetch}"
        );
        respond(
            &mut server,
            &format!("{fetch_data}{} OK done\r\n", tag_of(&fetch)),
        )
        .await;
        server
    });

    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        super::pim::message_hydrate(
            account,
            super::encode_object_id(&inbox, 5, 3),
            bifrost_types::hydration::HydrationProjection::Headers,
        ),
    )
    .await
    .expect("message_hydrate must not hang");
    let _server = script.await.unwrap();
    result
}

/// A UID FETCH that completes without data for a UID valid in the current
/// UIDVALIDITY epoch is how IMAP reports an expunged message (RFC 9051 6.4.9:
/// a non-existent UID is ignored). That is `NotFound(Message)`, answered by
/// the server, not the caller's malformed request. An unsolicited FETCH for a
/// DIFFERENT uid in the same exchange must not stand in for the missing one:
/// the answer used to be whatever sorted last in the collected responses.
#[tokio::test]
async fn message_hydrate_of_an_expunged_uid_is_not_found() {
    use bifrost_types::{
        AccountErrorKind, AccountOperation, Cause, ErrorScope, RecoveryClass, ResourceKind,
        TransmissionState,
    };

    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let wanted = super::encode_object_id(&inbox, 5, 3);

    for (label, data) in [
        ("no FETCH data at all", ""),
        (
            "only an unsolicited FETCH for another uid",
            "* 9 FETCH (UID 9 FLAGS (\\Seen))\r\n",
        ),
    ] {
        let err = hydrate_uid_3_against(data).await.expect_err(label);
        assert_eq!(
            err.kind(),
            &AccountErrorKind::NotFound(ResourceKind::Message),
            "{label}: got {err:?}"
        );
        assert_eq!(err.recovery(), &RecoveryClass::ProviderRefused, "{label}");
        assert_eq!(
            err.operation(),
            Some(AccountOperation::HydrateMessage),
            "{label}"
        );
        assert_eq!(
            err.scope(),
            Some(&ErrorScope::Message { id: wanted.clone() }),
            "{label}"
        );
        assert!(
            err.chain().iter().any(|cause| matches!(
                cause,
                Cause::Attempt(a) if a.transmission_state == TransmissionState::Acknowledged
            )),
            "{label}: the server answered, so the attempt is Acknowledged: {err:?}"
        );
    }

    // Present alongside an unsolicited FETCH for a higher uid: the requested
    // message is the answer, not the one that sorts last.
    let message =
        hydrate_uid_3_against("* 3 FETCH (UID 3 FLAGS ())\r\n* 9 FETCH (UID 9 FLAGS (\\Seen))\r\n")
            .await
            .expect("uid 3 was returned");
    assert_eq!(message.id, wanted);
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
    let caps = super::test_support::stub_capabilities();
    assert!(!caps.pim_methods.add_to_container);
    assert!(!caps.pim_methods.message_hydrate);
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });
    // A well-formed id, so a missing gate proceeds toward the wire rather
    // than failing id decode first.
    let id = super::encode_object_id(&crate::types::MailboxName::new("INBOX").unwrap(), 5, 3);

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        super::pim::add_to_container(
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
        super::pim::message_hydrate(
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

/// `thread_hydrate` answers exactly one message per REQUESTED uid. The UID
/// FETCH here carries, besides the two members: a trailing FLAGS-only
/// response for uid 3 after its body-bearing one, and an unsolicited FETCH
/// for uid 9, which is not in the thread. Each response used to become a
/// `Message` of its own, so the thread came back with four messages - uid 3
/// twice, one copy bodiless - and a stranger.
#[tokio::test]
async fn thread_hydrate_answers_once_per_requested_uid() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let mut caps = super::test_support::stub_capabilities();
    caps.pim_methods.thread_hydrate = true;
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });
    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let thread = super::encode_thread_id(&inbox, 5, &[3, 4]);

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\""),
            "expected EXAMINE, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Seen)\r\n\
                 * 9 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 10] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("UID FETCH"),
            "expected UID FETCH, got {fetch}"
        );
        let body = "From: a@b.test\r\nSubject: hi\r\n\r\nhello there\r\n";
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 3 FLAGS () BODY[] {{{}}}\r\n{body})\r\n\
                 * 1 FETCH (UID 3 FLAGS (\\Seen))\r\n\
                 * 2 FETCH (UID 4 FLAGS ())\r\n\
                 * 9 FETCH (UID 9 FLAGS (\\Seen))\r\n\
                 {} OK done\r\n",
                body.len(),
                tag_of(&fetch)
            ),
        )
        .await;
        server
    });

    let hydrated = tokio::time::timeout(
        Duration::from_secs(5),
        super::pim::thread_hydrate(account, thread),
    )
    .await
    .expect("thread_hydrate must not hang")
    .expect("the thread hydrates");
    let _server = script.await.unwrap();

    let ids: Vec<_> = hydrated.messages.iter().map(|m| m.id.clone()).collect();
    assert_eq!(
        ids,
        vec![
            super::encode_object_id(&inbox, 5, 3),
            super::encode_object_id(&inbox, 5, 4),
        ],
        "one message per requested uid, and nothing else"
    );
    let first = &hydrated.messages[0];
    assert!(
        first
            .body_text
            .as_deref()
            .is_some_and(|text| text.contains("hello there")),
        "the trailing FLAGS-only response must not replace the body: {first:?}"
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
    let mut caps = super::test_support::stub_capabilities();
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
        super::pim::add_to_container(
            account,
            MutationTarget::Message(super::encode_object_id(&inbox, 5, 3)),
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
        super::pool::enable_qresync(&conn, Duration::from_secs(5)),
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
        uidvalidity: 5,
        modseq: 100,
        known_uids: CompactUidSet::from_ranges(vec![crate::types::UidRange::range(1, 4000000000)]),
        known_uids_complete: true,
    });
    entry.record_modseq(5, 2, 100).unwrap();
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
    let output = super::mutate::mutation_stream(
        account,
        Box::pin(futures::stream::iter([super::encode_object_id(
            &inbox(),
            5,
            2,
        )])),
        super::mutate::MutationKind::Flags(FlagOp::Add(std::collections::HashSet::from([
            "$flagged".to_owned(),
        ]))),
    );
    assert_eq!(drain_mutation_batches(output).await, vec![vec![id(5, 2)]]);
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
            .record_modseq(5, 2, 900)
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
        let key = bifrost_types::IdempotencyKey {
            run_id: bifrost_types::RunId("caller-version".into()),
            sequence: 0,
            protocol_salt: bifrost_types::ProtocolSalt::Imap,
        };
        let events = Arc::new(account)
            .bulk_set_flags_if_version(
                Box::pin(futures::stream::iter([MailMutationTarget {
                    id: super::encode_object_id(&inbox(), 5, 2),
                    expected_version: expected.map(ServerVersion::ModSeq),
                }])),
                FlagOp::Add(std::collections::HashSet::from(["$flagged".into()])),
                key,
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

const SESSION_GREETING: &[u8] = b"* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN SASL-IR] ready\r\n";

pub(super) async fn session_account(
    cap: usize,
    count: usize,
    notify: bool,
) -> (
    ImapAccount,
    tokio::io::DuplexStream,
    super::test_support::ScriptedDials,
) {
    let (conn, peer) = driver_pair(&preauth_greeting("IMAP4rev1 ACL")).await;
    let (connector, dials) =
        super::test_support::scripted_connector(vec![SESSION_GREETING.to_vec(); count]);
    let config = super::factory::ImapAccountConfig {
        idle_connection_budget: 1,
        ..super::test_support::scripted_account_config(cap)
    };
    let mut parts = super::test_support::scripted_account_parts_from(conn, config, connector);
    parts.supports_notify = notify;
    (ImapAccount::new(parts), peer, dials)
}

async fn session_auth(peer: &mut tokio::io::DuplexStream, auth: &str) {
    assert!(auth.contains("AUTHENTICATE PLAIN"), "{auth}");
    respond(
        peer,
        &format!(
            "{} OK [CAPABILITY IMAP4rev1 IDLE UNSELECT BINARY] done\r\n",
            tag_of(auth)
        ),
    )
    .await;
}

async fn session_select(peer: &mut tokio::io::DuplexStream, folder: &str) {
    let select = read_line(peer).await;
    assert!(
        select.contains("SELECT") && select.contains(folder),
        "{select}"
    );
    respond(peer, &format!("* FLAGS (\\Seen)\r\n* 1 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 1] valid\r\n{} OK [READ-WRITE] selected\r\n", tag_of(&select))).await;
}

async fn session_idle(peer: &mut tokio::io::DuplexStream) {
    let idle = read_line(peer).await;
    assert!(idle.contains("IDLE"), "{idle}");
    respond(peer, "+ idling\r\n").await;
}

fn push_folder(name: &str) -> bifrost_types::CursorScope {
    bifrost_types::CursorScope::Folder(bifrost_types::FolderId(name.into()))
}

/// Push never takes the last session, so at `pool_cap = 1` every scope lands
/// in the failed lane, with or without NOTIFY, and no worker starts.
#[tokio::test]
async fn push_is_refused_entirely_when_pool_cap_leaves_no_room() {
    use bifrost_types::Account as _;
    for notify in [false, true] {
        let (account, peer, dials) = session_account(1, 0, notify).await;
        let result = account
            .push_subscribe(&[push_folder("INBOX"), push_folder("Archive")])
            .await
            .unwrap();
        assert_eq!(result.outcomes.failed().len(), 2);
        assert!(result.outcomes.succeeded().is_empty());
        assert!(result.handle.is_none());
        assert!(account.push_lanes().is_empty());
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(peer);
        account.close().await.unwrap();
    }
}

/// NOTIFY folds every folder onto one session, so the smallest cap with
/// push room admits any number of folders on a single lane.
#[tokio::test]
async fn a_notify_account_admits_every_folder_on_one_lane() {
    use bifrost_types::Account as _;
    let (account, peer, _) = session_account(2, 0, true).await;
    let scopes: Vec<_> = ["A", "B", "C", "D", "E"]
        .into_iter()
        .map(push_folder)
        .collect();
    let result = account.push_subscribe(&scopes).await.unwrap();
    assert_eq!(result.outcomes.succeeded().len(), 5);
    assert!(result.outcomes.failed().is_empty());
    assert_eq!(account.push_lanes().len(), 1);
    drop(peer);
    account.close().await.unwrap();
}

/// A live IDLE session holds a session permit and a push slot: with the
/// primed session checked out, a second data checkout waits instead of
/// dialing a third session.
#[tokio::test(start_paused = true)]
async fn an_idle_worker_session_counts_against_pool_cap() {
    use bifrost_types::Account as _;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let driver = drive_push_until_shutdown(&account);
    let mut push_peer = dials.servers.recv().await.unwrap();
    let auth = read_line(&mut push_peer).await;
    session_auth(&mut push_peer, &auth).await;
    session_select(&mut push_peer, "INBOX").await;
    session_idle(&mut push_peer).await;
    assert_eq!(account.pool.available_sessions(), 0);
    assert_eq!(account.push.available_push_slots(), 0);
    let _held = account.pool.checkout_any().await.unwrap();
    let mut second = Box::pin(account.pool.checkout_any());
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(second);
    drop(push_peer);
    drop(peer);
    account.close().await.unwrap();
    driver.await.unwrap();
}

/// A generation bump while the worker authenticates must not cancel the
/// dial: the one-greeting connector refuses a second call. Leg 1 bumps with
/// an accepted same-folder subscribe; leg 2 moves the assignment, which the
/// post-dial re-choice picks up before SELECT.
#[tokio::test(start_paused = true)]
async fn a_subscription_change_mid_dial_keeps_the_pending_session() {
    use bifrost_types::Account as _;
    for reassign in [false, true] {
        let (account, peer, mut dials) = session_account(2, 1, false).await;
        let first = account
            .push_subscribe(&[push_folder("INBOX")])
            .await
            .unwrap()
            .handle
            .unwrap();
        let driver = drive_push_until_shutdown(&account);
        let mut push_peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut push_peer).await;
        if reassign {
            account.push_unsubscribe(first).await.unwrap();
        }
        let selected = if reassign { "Archive" } else { "INBOX" };
        account
            .push_subscribe(&[push_folder(selected)])
            .await
            .unwrap();
        session_auth(&mut push_peer, &auth).await;
        session_select(&mut push_peer, selected).await;
        session_idle(&mut push_peer).await;
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        drop(push_peer);
        drop(peer);
        account.close().await.unwrap();
        driver.await.unwrap();
    }
}

/// Both sessions are held by data checkouts when the worker asks, so it
/// waits; once covering, it invalidates the folder for the stretched window.
#[tokio::test(start_paused = true)]
async fn a_push_session_that_waited_for_capacity_invalidates_the_folder_it_starts_covering() {
    use bifrost_types::Account as _;
    use futures::StreamExt as _;
    let (account, peer, mut dials) = session_account(2, 2, false).await;
    let mut events = account.push_stream(
        bifrost_types::PushLane(0),
        bifrost_types::SessionStop::never(),
        bifrost_types::PushGate::open(),
    );
    let _held = account.pool.checkout_any().await.unwrap();
    let script = async {
        let mut peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut peer).await;
        session_auth(&mut peer, &auth).await;
        peer
    };
    let (second, mut data_peer) = tokio::join!(account.pool.checkout_any(), script);
    let second = second.unwrap();
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    drop(second);
    let script = async {
        let logout = read_line(&mut data_peer).await;
        assert!(logout.contains("LOGOUT"));
        respond(
            &mut data_peer,
            &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
        )
        .await;
        let mut push_peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut push_peer).await;
        session_auth(&mut push_peer, &auth).await;
        session_select(&mut push_peer, "INBOX").await;
        push_peer
    };
    let (event, push_peer) = tokio::join!(events.next(), script);
    assert!(
        matches!(event, Some(bifrost_types::PushSessionEvent::Invalidated(bifrost_types::InvalidationHint { payload: bifrost_types::HintPayload::SpecificCursorScope(scope), .. })) if scope == push_folder("INBOX"))
    );
    drop(push_peer);
    drop(peer);
    account.close().await.unwrap();
}

/// Capacity available at once: no coverage invalidation, which keeps the
/// `register_notify` no-snapshot decision for the common case.
#[tokio::test(start_paused = true)]
async fn a_push_session_dialed_at_once_publishes_no_coverage_invalidation() {
    use bifrost_types::Account as _;
    use futures::StreamExt as _;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    let mut events = account.push_stream(
        bifrost_types::PushLane(0),
        bifrost_types::SessionStop::never(),
        bifrost_types::PushGate::open(),
    );
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let script = async {
        let mut push_peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut push_peer).await;
        session_auth(&mut push_peer, &auth).await;
        session_select(&mut push_peer, "INBOX").await;
        push_peer
    };
    let (event, push_peer) = tokio::join!(events.next(), script);
    assert!(matches!(
        event,
        Some(bifrost_types::PushSessionEvent::Established)
    ));
    drop(push_peer);
    assert!(
        matches!(events.next().await, Some(bifrost_types::PushSessionEvent::Failed(e)) if e.recovery().is_retryable())
    );
    drop(peer);
    account.close().await.unwrap();
}

/// The refusing connector stands in for an unreachable host, so a push
/// session whose dial it refuses must end `Failed` with a retryable class
/// and never redial on its own: the engine owns the backoff and the next
/// session. Every push admission test built on `scripted_account_parts`
/// relies on this.
#[tokio::test(start_paused = true)]
async fn a_refused_push_dial_fails_the_session_without_redialing() {
    use bifrost_types::Account as _;
    use futures::StreamExt as _;
    let (account, peer, dials) = session_account(2, 0, false).await;
    let mut events = account.push_stream(
        bifrost_types::PushLane(0),
        bifrost_types::SessionStop::never(),
        bifrost_types::PushGate::open(),
    );
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    assert!(matches!(
        events.next().await,
        Some(bifrost_types::PushSessionEvent::Failed(e)) if e.recovery().is_retryable()
    ));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(
        dials.calls.load(std::sync::atomic::Ordering::SeqCst) == 1,
        "the session must not redial"
    );
    drop(peer);
    account.close().await.unwrap();
}

#[tokio::test]
async fn flags_only_fetch_without_flags_is_a_missing_field_failure() {
    use bifrost_types::{AccountErrorKind, ItemOutcome, Projection, ProtocolErrorKind, SyncEvent};
    use futures::StreamExt;
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(&mut server, &format!("* FLAGS (\\Seen)\r\n* 2 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 3] ok\r\n{} OK [READ-ONLY] done\r\n", tag_of(&select))).await;
        let fetch = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 1)\r\n* 2 FETCH (UID 2 FLAGS (\\Seen))\r\n{} OK done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
    });
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let ids = (1..=2)
        .map(|uid| super::encode_object_id(&folder, 5, uid))
        .collect::<Vec<_>>();
    let mut output = super::get::get_stream(
        account,
        Box::pin(futures::stream::iter(ids)),
        Projection::FlagsOnly,
    );
    let mut items = Vec::new();
    while let Some(event) = output.next().await {
        if let SyncEvent::Batch(batch) = event {
            items.extend(batch.items);
        }
    }
    assert_eq!(items.len(), 2);
    assert!(
        matches!(&items[0],ItemOutcome::Failed(f) if matches!(f.error.kind(),AccountErrorKind::Protocol(ProtocolErrorKind::MissingField)))
    );
    assert!(matches!(&items[1], ItemOutcome::Succeeded(_)));
    script.await.unwrap();
}

fn drive_push_until_shutdown(account: &super::ImapAccount) -> tokio::task::JoinHandle<()> {
    use bifrost_types::Account;
    use futures::StreamExt;
    let mut session = account.push_stream(
        bifrost_types::PushLane(0),
        bifrost_types::SessionStop::never(),
        bifrost_types::PushGate::open(),
    );
    tokio::spawn(async move { while session.next().await.is_some() {} })
}

#[tokio::test(start_paused = true)]
async fn a_nonexistent_selected_folder_is_excluded_and_the_session_reconfigures() {
    use bifrost_types::{
        Account, PushGate, PushLane, PushSessionEnd as End, PushSessionEvent as E, SessionStop,
    };
    use futures::StreamExt;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account
        .push_subscribe(&[push_folder("gone")])
        .await
        .unwrap();
    let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let script = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth(&mut server, &auth).await;
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!("{} NO [NONEXISTENT] gone\r\n", tag_of(&select)),
        )
        .await;
        server
    };
    let (event, server) = tokio::join!(session.next(), script);
    assert!(matches!(event, Some(E::UnitFailed(_))));
    assert!(matches!(
        session.next().await,
        Some(E::Ended(End::Reconfigure))
    ));
    drop(session);
    let mut parked = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let mut next = Box::pin(parked.next());
    assert!(futures::poll!(&mut next).is_pending());
    assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(next);
    drop(parked);
    drop(server);
    drop(peer);
    account.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_second_session_on_a_driven_lane_fails_the_lease() {
    use bifrost_types::{Account, PushGate, PushLane, PushSessionEvent as E, SessionStop};
    use futures::StreamExt;
    let (account, peer, _) = session_account(2, 0, false).await;
    let mut first = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let mut next = Box::pin(first.next());
    assert!(futures::poll!(&mut next).is_pending());
    let mut second = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    assert!(
        matches!(second.next().await, Some(E::Failed(e)) if matches!(e.kind(), bifrost_types::AccountErrorKind::Internal(bifrost_types::InternalErrorKind::AccountContract)))
    );
    drop(next);
    drop(first);
    let mut third = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let mut next = Box::pin(third.next());
    assert!(futures::poll!(&mut next).is_pending());
    drop(next);
    drop(third);
    drop(peer);
    account.close().await.unwrap();
}

async fn completed_push_round_and_stop(healthy: bool) {
    use bifrost_types::{
        Account, PushGate, PushLane, PushSessionEnd as End, PushSessionEvent as E, SessionStop,
    };
    use futures::StreamExt;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let (stopper, stop) = SessionStop::pair();
    let mut stopper = Some(stopper);
    let mut session = account.push_stream(PushLane(0), stop, PushGate::open());
    let setup = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth(&mut server, &auth).await;
        session_select(&mut server, "INBOX").await;
        server
    };
    let (event, mut server) = tokio::join!(session.next(), setup);
    assert!(matches!(event, Some(E::Established)));
    let script = async {
        let idle = read_line(&mut server).await;
        assert!(idle.contains("IDLE"));
        respond(&mut server, "+ idling\r\n").await;
        if healthy {
            respond(&mut server, "* 2 EXISTS\r\n").await;
        } else {
            stopper.take().unwrap().stop();
        }
        let done = read_line(&mut server).await;
        assert!(done.contains("DONE"), "{done}");
        respond(
            &mut server,
            &format!("{} OK idle complete\r\n", tag_of(&idle)),
        )
        .await;
        if !healthy {
            let logout = read_line(&mut server).await;
            assert!(logout.contains("LOGOUT"));
            respond(
                &mut server,
                &format!("* BYE done\r\n{} OK logged out\r\n", tag_of(&logout)),
            )
            .await;
        }
    };
    let (event, ()) = tokio::join!(session.next(), script);
    if healthy {
        assert!(matches!(event, Some(E::Healthy)));
    } else {
        assert!(matches!(event, Some(E::Ended(End::Stopped))));
    }
    drop(session);
    drop(server);
    drop(peer);
    account.close().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn a_completed_idle_round_reports_healthy() {
    completed_push_round_and_stop(true).await;
}
#[tokio::test(start_paused = true)]
async fn a_stopped_session_logs_out_before_ending() {
    completed_push_round_and_stop(false).await;
}

/// A BYE whose code classifies terminal ends the session `Failed` without
/// excluding the selected folder. The BYE arrives before the `+` grant, so the
/// driver's grant wait raises it as `Error::Bye` and the session sees it
/// through `idle()`'s error arm - the only way a BYE reaches the live session.
/// The defensive `IdleEvent::Bye` arm is pinned by a unit test in `push.rs`.
#[tokio::test(start_paused = true)]
async fn a_bye_never_excludes_the_selected_folder() {
    use bifrost_types::{Account, PushGate, PushLane, PushSessionEvent as E, SessionStop};
    use futures::StreamExt;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let setup = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth(&mut server, &auth).await;
        session_select(&mut server, "INBOX").await;
        server
    };
    let (event, mut server) = tokio::join!(session.next(), setup);
    assert!(matches!(event, Some(E::Established)));
    let script = async {
        let idle = read_line(&mut server).await;
        assert!(idle.contains("IDLE"));
        respond(&mut server, "* BYE [NOPERM] denied\r\n").await;
    };
    let (event, ()) = tokio::join!(session.next(), script);
    assert!(matches!(event, Some(E::Failed(e)) if e.recovery().is_terminal()));
    drop(session);
    drop(server);
    // The next dial is refused by the connector. An excluded assignment would
    // park instead of attempting it, so this also pins the session's wiring.
    let mut next = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let event = tokio::time::timeout(Duration::from_secs(30), next.next())
        .await
        .unwrap();
    assert!(matches!(event, Some(E::Failed(e)) if e.recovery().is_retryable()));
    drop(next);
    drop(peer);
    account.close().await.unwrap();
}

/// A server BYE and a server-initiated IDLE termination both leave the push
/// connection unusable, so the live session ends on the round that saw them
/// instead of reporting the round healthy and re-issuing IDLE on a dead
/// socket. They end differently: the BYE is classified like any other
/// connection failure (a plain BYE is `Server(Unavailable)`) and ends
/// `Failed`, while a tagged completion the client never asked for carries no
/// error and ends `Lost`. Either way the session releases its push slot.
///
/// The driver short-circuits an untagged BYE read during IDLE into
/// `Error::Bye` before emitting any event, so on the wire the BYE reaches the
/// session through `idle()`'s error arm, not as `IdleEvent::Bye`. The test
/// pins the outcome on that live path.
#[tokio::test(start_paused = true)]
async fn bye_and_server_termination_close_the_connection() {
    use bifrost_types::{
        Account, AccountErrorKind, PushGate, PushLane, PushSessionEnd as End,
        PushSessionEvent as E, ServerErrorKind, SessionStop,
    };
    use futures::StreamExt;
    for bye in [true, false] {
        let (account, peer, mut dials) = session_account(2, 1, false).await;
        account
            .push_subscribe(&[push_folder("INBOX")])
            .await
            .unwrap();
        let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
        let setup = async {
            let mut server = dials.servers.recv().await.unwrap();
            let auth = read_line(&mut server).await;
            session_auth(&mut server, &auth).await;
            session_select(&mut server, "INBOX").await;
            server
        };
        let (event, mut server) = tokio::join!(session.next(), setup);
        assert!(matches!(event, Some(E::Established)));
        assert_eq!(account.push.available_push_slots(), 0);
        let script = async {
            let idle = read_line(&mut server).await;
            assert!(idle.contains("IDLE"), "{idle}");
            respond(&mut server, "+ idling\r\n").await;
            if bye {
                respond(&mut server, "* BYE logging out\r\n").await;
            } else {
                respond(
                    &mut server,
                    &format!("{} OK IDLE ended by server\r\n", tag_of(&idle)),
                )
                .await;
            }
        };
        let (event, ()) = tokio::join!(session.next(), script);
        if bye {
            assert!(
                matches!(&event, Some(E::Failed(e)) if matches!(e.kind(), AccountErrorKind::Server(ServerErrorKind::Unavailable))),
                "a BYE ends the session Failed with its classification, got {event:?}"
            );
        } else {
            assert!(
                matches!(event, Some(E::Ended(End::Lost))),
                "a server-terminated IDLE ends the session Lost, got {event:?}"
            );
        }
        assert!(session.next().await.is_none(), "the session has ended");
        drop(session);
        // The slot is held until the dropped connection's driver exits, which
        // may tail a LOGOUT nobody answers; paused time lets that run out.
        let mut released = false;
        for _ in 0..600 {
            if account.push.available_push_slots() == 1 {
                released = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(
            released,
            "the ended session released its push connection (bye = {bye})"
        );
        drop(server);
        drop(peer);
        account.close().await.unwrap();
    }
}
