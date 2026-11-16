use super::test_support::*;

/// QRESYNC SELECT is the whole diff: broad VANISHED ranges intersect the
/// baseline, duplicate FETCHes collapse, and no second command is needed.
#[tokio::test]
async fn qresync_dedupes_vanished_against_fetch_and_checkpoints_the_live_set() {
    let (conn, mut server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    // The member is brought to QRESYNC when it is minted, as the factory does
    // for the primed connection and the pool for every dial.
    let enable = crate::account::pool::enable_qresync(&conn, Duration::from_secs(5));
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
            uidvalidity: crate::types::UidValidity::new(5).expect("nonzero epoch"),
            modseq: crate::types::ModSeq::new(100).expect("valid protocol test value"),
            baseline: crate::account::envelope::UidBaseline::Complete(UidSet::from_numbers([
                1, 2, 3,
            ])),
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
            (
                id(
                    crate::types::UidValidity::new(5).expect("valid protocol test value"),
                    1
                ),
                "removed"
            ),
            (
                id(
                    crate::types::UidValidity::new(5).expect("valid protocol test value"),
                    3
                ),
                "updated"
            ),
            (
                id(
                    crate::types::UidValidity::new(5).expect("valid protocol test value"),
                    2
                ),
                "updated"
            ),
        ],
        "UID 2 was VANISHED then re-FETCHed, so it must land in one lane only, \
         and the twice-FETCHed UID 3 must be emitted once"
    );

    match done_cursor(&events) {
        FolderCursor::QResync {
            uidvalidity,
            modseq,
            baseline: crate::account::envelope::UidBaseline::Complete(known_uids),
        } => {
            assert_eq!((uidvalidity.get(), modseq.get()), (5, 200));
            assert_eq!(known_uids.to_uids(), vec![2, 3]);
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
            uidvalidity: crate::types::UidValidity::new(5).expect("nonzero epoch"),
            modseq: crate::types::ModSeq::new(100).expect("valid protocol test value"),
            baseline: crate::account::envelope::UidBaseline::Legacy(UidSet::from_numbers([1, 2])),
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
            uidvalidity: crate::types::UidValidity::new(7).expect("nonzero epoch"),
            modseq: crate::types::ModSeq::new(50).expect("valid protocol test value"),
            known_uids: UidSet::from_numbers([1, 2, 3]),
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
            (
                id(
                    crate::types::UidValidity::new(7).expect("valid protocol test value"),
                    2
                ),
                "updated"
            ),
            (
                id(
                    crate::types::UidValidity::new(7).expect("valid protocol test value"),
                    4
                ),
                "added"
            ),
            (
                id(
                    crate::types::UidValidity::new(7).expect("valid protocol test value"),
                    1
                ),
                "removed"
            ),
        ]
    );
    match done_cursor(&events) {
        FolderCursor::Condstore {
            uidvalidity,
            modseq,
            known_uids,
        } => {
            assert_eq!((uidvalidity.get(), modseq.get()), (7, 60));
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
            uidvalidity: crate::types::UidValidity::new(7).expect("nonzero epoch"),
            modseq: crate::types::ModSeq::new(50).expect("valid protocol test value"),
            known_uids: UidSet::from_numbers(1..=129),
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
            uidvalidity: crate::types::UidValidity::new(7).expect("nonzero epoch"),
            modseq: crate::types::ModSeq::new(50).expect("valid protocol test value"),
            baseline: crate::account::envelope::UidBaseline::Complete(UidSet::from_numbers([
                1, 2, 3,
            ])),
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(warnings.len(), 1, "one downgrade warning: {warnings:?}");
    assert_eq!(warnings[0].kind, WarningKind::StrategyDowngraded);
    assert_eq!(
        warnings[0].strategy_downgrade,
        Some(bifrost_types::StrategyDowngrade::QResyncToCondstore)
    );
    assert_eq!(
        change_labels(&events),
        vec![
            (
                id(
                    crate::types::UidValidity::new(7).expect("valid protocol test value"),
                    2
                ),
                "updated"
            ),
            (
                id(
                    crate::types::UidValidity::new(7).expect("valid protocol test value"),
                    4
                ),
                "added"
            ),
            (
                id(
                    crate::types::UidValidity::new(7).expect("valid protocol test value"),
                    1
                ),
                "removed"
            ),
        ]
    );
    match done_cursor(&events) {
        FolderCursor::QResync {
            uidvalidity,
            modseq,
            baseline: crate::account::envelope::UidBaseline::Complete(known_uids),
        } => {
            assert_eq!((uidvalidity.get(), modseq.get()), (7, 60));
            assert_eq!(known_uids.to_uids(), vec![2, 3, 4]);
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
            .select_folder(&mut pooled, &inbox(), crate::account::SelectPurpose::Read)
            .await
            .unwrap();
    }

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::QResync {
            uidvalidity: crate::types::UidValidity::new(7).expect("nonzero epoch"),
            modseq: crate::types::ModSeq::new(50).expect("valid protocol test value"),
            baseline: crate::account::envelope::UidBaseline::Complete(UidSet::from_numbers([
                1, 2, 3,
            ])),
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(warnings.len(), 1, "one downgrade warning: {warnings:?}");
    assert_eq!(warnings[0].kind, WarningKind::StrategyDowngraded);
    assert!(
        matches!(
            done_cursor(&events),
            FolderCursor::QResync { modseq, .. } if modseq.get() == 60
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
            uidvalidity: crate::types::UidValidity::new(3).expect("nonzero epoch"),
            uidnext: crate::types::Uid::new(10),
            known_uids: UidSet::from_numbers([1, 2]),
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
            (
                id(
                    crate::types::UidValidity::new(3).expect("valid protocol test value"),
                    5
                ),
                "added"
            ),
            (
                id(
                    crate::types::UidValidity::new(3).expect("valid protocol test value"),
                    1
                ),
                "removed"
            )
        ]
    );
    match done_cursor(&events) {
        FolderCursor::Basic {
            uidvalidity,
            uidnext,
            known_uids,
        } => {
            assert_eq!(
                (uidvalidity.get(), uidnext.map(crate::types::Uid::get)),
                (3, Some(11))
            );
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
            uidvalidity: crate::types::UidValidity::new(3).expect("nonzero epoch"),
            uidnext: crate::types::Uid::new(10),
            known_uids: UidSet::from_numbers([1, 2]),
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
            uidvalidity: crate::types::UidValidity::new(9).expect("nonzero epoch"),
            modseq: crate::types::ModSeq::new(40).expect("valid protocol test value"),
            known_uids: UidSet::from_numbers([1, 2]),
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
        warnings[0].strategy_downgrade,
        Some(bifrost_types::StrategyDowngrade::CondstoreToBasic)
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
            assert_eq!(
                (uidvalidity.get(), uidnext.map(crate::types::Uid::get)),
                (9, Some(7))
            );
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
            uidvalidity: crate::types::UidValidity::new(9).expect("nonzero epoch"),
            modseq: crate::types::ModSeq::new(40).expect("valid protocol test value"),
            known_uids: UidSet::from_numbers([1]),
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
        vec![
            (
                id(
                    crate::types::UidValidity::new(9).expect("valid protocol test value"),
                    1
                ),
                "updated"
            ),
            (
                id(
                    crate::types::UidValidity::new(9).expect("valid protocol test value"),
                    7
                ),
                "added"
            )
        ],
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
            uidvalidity: crate::types::UidValidity::new(7).expect("nonzero epoch"),
            modseq: crate::types::ModSeq::new(50).expect("valid protocol test value"),
            known_uids: UidSet::from_numbers([1]),
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
        uidvalidity: crate::types::UidValidity::new(1).expect("nonzero epoch"),
        modseq: crate::types::ModSeq::new(1).expect("valid protocol test value"),
        baseline: crate::account::envelope::UidBaseline::Complete(UidSet::default()),
    });
    let condstore_cursor = cursor_for(&FolderCursor::Condstore {
        uidvalidity: crate::types::UidValidity::new(1).expect("nonzero epoch"),
        modseq: crate::types::ModSeq::new(1).expect("valid protocol test value"),
        known_uids: UidSet::default(),
    });
    let basic_cursor = cursor_for(&FolderCursor::Basic {
        uidvalidity: crate::types::UidValidity::new(1).expect("nonzero epoch"),
        uidnext: crate::types::Uid::new(1),
        known_uids: UidSet::default(),
    });

    let (conn, _server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    let enabled = scripted_sync_account(conn, 1, true, None, inbox_registry());
    let described = crate::account::changes::describe_cursor(&enabled, &qresync_cursor);
    assert_eq!(described.strategy, SyncStrategy::QResync);
    assert_eq!(described.cost_class, CostClass::Cheap);

    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let disabled = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let described = crate::account::changes::describe_cursor(&disabled, &qresync_cursor);
    assert_eq!(
        described.strategy,
        SyncStrategy::Condstore,
        "a QRESYNC cursor on a downgraded session must be described as CONDSTORE"
    );
    assert_eq!(described.cost_class, CostClass::Medium);

    let described = crate::account::changes::describe_cursor(&disabled, &condstore_cursor);
    assert_eq!(described.strategy, SyncStrategy::Condstore);
    assert_eq!(described.cost_class, CostClass::Medium);

    let described = crate::account::changes::describe_cursor(&disabled, &basic_cursor);
    assert_eq!(described.strategy, SyncStrategy::Basic);
    assert_eq!(described.cost_class, CostClass::Expensive);

    let incomplete = cursor_for(&FolderCursor::QResync {
        uidvalidity: crate::types::UidValidity::new(1).expect("nonzero epoch"),
        modseq: crate::types::ModSeq::new(1).expect("valid protocol test value"),
        baseline: crate::account::envelope::UidBaseline::Legacy(UidSet::default()),
    });
    for account in [enabled, disabled] {
        let described = crate::account::changes::describe_cursor(&account, &incomplete);
        assert_eq!(described.strategy, SyncStrategy::None);
        assert_eq!(described.cost_class, CostClass::Expensive);
        assert!(described.freshness.is_none());
        let events = collect_changes(account, incomplete.clone()).await;
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], SyncEvent::Terminated(_)));
    }
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
            uidvalidity: crate::types::UidValidity::new(3).expect("nonzero epoch"),
            uidnext: crate::types::Uid::new(3),
            known_uids: UidSet::from_numbers([1, 2]),
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(
        warnings[0].kind,
        WarningKind::UidCountMismatch {
            expected: 9,
            actual: 2
        }
    );
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
            uidvalidity: crate::types::UidValidity::new(3).expect("nonzero epoch"),
            uidnext: crate::types::Uid::new(5),
            known_uids: UidSet::default(),
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
    let batch_cursor = crate::account::decode_cursor(batch_cursor).expect("decodes");
    assert_eq!(
        batch_cursor,
        done_cursor(&events),
        "committing on the final batch and committing on Done must agree"
    );
    let _server = script.await.unwrap();
}
