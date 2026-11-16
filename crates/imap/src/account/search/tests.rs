use super::*;
use crate::account::folder_registry::FolderRegistry;
use crate::types::MailboxInfo;
use bifrost_types::container::ContainerId;
use bifrost_types::{AccountErrorKind, Cause, RequestCause, RequestErrorKind};

#[test]
fn search_folder_order_survives_registry_rebuild_with_display_ties() {
    let names = [
        MailboxName::new("a").unwrap(),
        MailboxName::from_wire("a".into(), b"&AGE-", false),
        MailboxName::from_wire("a".into(), b"&AAGE-", false),
    ];
    let expected = vec![
        names[2].account_id(),
        names[1].account_id(),
        names[0].account_id(),
    ];
    for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
        let registry = FolderRegistry::from_lists(
            order
                .into_iter()
                .map(|i| MailboxInfo {
                    name: names[i].clone(),
                    ..Default::default()
                })
                .collect(),
            vec![],
        );
        let folders = sorted_search_folders(&registry);
        assert_eq!(
            folders
                .iter()
                .map(MailboxName::account_id)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(folders[1], names[1]);
    }
}

// imap-F2: the locally-malformed helpers used to build an
// operation-less `Request(Malformed)`, so every PIM lane's client-bug
// errors arrived indistinguishable in telemetry. Each helper now takes
// the operation its caller is servicing. The recovery class was already
// `ClientBug` and is unchanged; what this pins is that the operation
// rides along, and that it is the *caller's* operation rather than a
// hardcoded one.
#[test]
fn pim_containers_and_nested_search_refuse_dav_ownership() {
    let container = ContainerId("https://example.test/contacts/".into());
    let (scopes, _) = super::super::DavScopeIndex::build(
        vec![bifrost_types::FolderId(container.0.clone())],
        Vec::new(),
        &std::collections::HashSet::new(),
    );
    let err = folder_from_container(&scopes, &container, AccountOperation::AddToContainer)
        .expect_err("a DAV destination must not reach IMAP");
    assert_eq!(err.operation(), Some(AccountOperation::AddToContainer));
    let filter = SearchFilter::And(vec![SearchFilter::In(container)]);
    assert!(criteria_from_filter(&scopes, &filter, AccountOperation::Search).is_err());
    assert!(
        folder_from_container(
            &scopes,
            &ContainerId("INBOX".into()),
            AccountOperation::AddToContainer,
        )
        .is_ok()
    );
}

#[test]
fn locally_malformed_pim_errors_carry_the_calling_operation() {
    // The search lane is the one that had two operations to confuse:
    // `search` and `search_messages` share every helper below, and the
    // criteria/folder-merge builders used to stamp `SearchMessages`
    // unconditionally. Drive each through `Search` so a reverted
    // threading shows up as the wrong op, not merely as `None`.
    let err = search_plan(
        &super::super::DavScopeIndex::default(),
        &SearchRequest::provider("(UNSEEN"),
        AccountOperation::Search,
    )
    .expect_err("unbalanced provider query");
    assert_eq!(err.operation(), Some(AccountOperation::Search));

    let err = combine_and(
        &super::super::DavScopeIndex::default(),
        &[
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::In(ContainerId("Sent".to_owned())),
        ],
        AccountOperation::Search,
    )
    .expect_err("a two-folder conjunction is refused");
    assert_eq!(err.operation(), Some(AccountOperation::Search));

    let err = combine_or(
        &super::super::DavScopeIndex::default(),
        &[
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::From("a@b.test".to_owned()),
        ],
        AccountOperation::Search,
    )
    .expect_err("a folder restriction inside an Or is refused");
    assert_eq!(err.operation(), Some(AccountOperation::Search));

    let Err(err) = SearchPager::<u32>::new(
        &search_request(None, Some("not-a-number")),
        None,
        AccountOperation::Search,
    ) else {
        panic!("a page cursor this crate did not mint");
    };
    assert_eq!(err.operation(), Some(AccountOperation::Search));
}

#[test]
fn or_with_nested_in_folder_is_rejected() {
    // An `In(folder)` nested inside an `Or` cannot be expressed as a
    // single-mailbox IMAP SEARCH; reject rather than silently leak
    // the folder restriction to the whole OR group.
    use bifrost_types::search::SearchFilter;
    let filter = SearchFilter::Or(vec![
        SearchFilter::In(ContainerId("Archive".to_owned())),
        SearchFilter::From("a@b.test".to_owned()),
    ]);
    let err = match criteria_from_filter(
        &super::super::DavScopeIndex::default(),
        &filter,
        AccountOperation::SearchMessages,
    ) {
        Err(err) => err,
        Ok(_) => panic!("OR with In must be rejected"),
    };
    assert_eq!(
        *err.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    );

    // An `In` inside an `And` is still accepted (the folder narrows
    // the whole conjunction - defensible).
    let filter = SearchFilter::And(vec![
        SearchFilter::In(ContainerId("Archive".to_owned())),
        SearchFilter::From("a@b.test".to_owned()),
    ]);
    let part = match criteria_from_filter(
        &super::super::DavScopeIndex::default(),
        &filter,
        AccountOperation::SearchMessages,
    ) {
        Ok(part) => part,
        Err(_) => panic!("AND with In is accepted"),
    };
    assert_eq!(
        part.folder.as_ref().map(MailboxName::as_str),
        Some("Archive")
    );
}

fn search_request(limit: Option<u32>, cursor: Option<&str>) -> SearchRequest {
    let mut request = SearchRequest::default();
    request.limit = limit;
    request.page_cursor = cursor.map(|c| c.as_bytes().to_vec().into());
    request
}

/// One page call over `folders` (each `(name, uidvalidity, uids)`, in
/// walk order), driven the way `search_messages` drives the pager.
fn search_page(
    folders: &[(&str, u32, &[u32])],
    restriction: Option<&str>,
    limit: u32,
    cursor: Option<bifrost_types::PageCursor>,
) -> Result<Page<(String, u32)>, AccountError> {
    let mut request = search_request(Some(limit), None);
    request.page_cursor = cursor;
    let restriction = restriction.map(|name| MailboxName::new(name).unwrap());
    let mut pager = SearchPager::new(
        &request,
        restriction.as_ref(),
        AccountOperation::SearchMessages,
    )?;
    let names = folders
        .iter()
        .map(|(name, _, _)| MailboxName::new(*name).unwrap())
        .collect();
    for folder in pager.folders(names) {
        if pager.is_full() {
            break;
        }
        let (_, uidvalidity, uids) = folders
            .iter()
            .find(|(name, _, _)| *name == folder.as_str())
            .unwrap();
        let entries = uids.iter().map(|uid| (*uid, *uid)).collect();
        pager.accept(
            &folder,
            crate::types::UidValidity::new(*uidvalidity).unwrap(),
            entries,
            |uid| (folder.as_str().to_owned(), uid),
        )?;
    }
    Ok(pager.finish())
}

fn uids_of(page: &Page<(String, u32)>) -> Vec<u32> {
    page.items.iter().map(|(_, uid)| *uid).collect()
}

#[test]
fn a_search_walk_does_not_skip_a_survivor_when_an_earlier_result_is_expunged() {
    let first =
        search_page(&[("INBOX", 7, &[101, 102, 103, 104])], None, 2, None).expect("first page");
    assert_eq!(uids_of(&first), vec![101, 102]);
    assert_eq!(first.estimated_total, None, "the walk was not enumerated");

    // 101 is expunged before the next page is asked for. An offset of 2
    // into the fresh [102, 103, 104] would hand back 104 and lose 103.
    let second = search_page(
        &[("INBOX", 7, &[102, 103, 104])],
        None,
        2,
        first.next_cursor,
    )
    .expect("second page");
    assert_eq!(uids_of(&second), vec![103, 104]);
    assert_eq!(second.next_cursor, None, "the final page ends the walk");
}

#[test]
fn a_search_walk_crosses_folders_in_order_and_skips_folders_it_passed() {
    let folders: &[(&str, u32, &[u32])] = &[("A", 1, &[3, 1]), ("B", 2, &[5]), ("C", 3, &[9, 8])];
    let first = search_page(folders, None, 3, None).expect("first page");
    assert_eq!(
        first.items,
        vec![
            ("A".to_owned(), 1),
            ("A".to_owned(), 3),
            ("B".to_owned(), 5)
        ],
        "a folder's results are walked in UID order"
    );
    // The cursor now stands in B, so folder A changing UIDVALIDITY is
    // not observed: passed folders are not searched again.
    let passed: &[(&str, u32, &[u32])] = &[("A", 99, &[1]), ("B", 2, &[5]), ("C", 3, &[9, 8])];
    let second = search_page(passed, None, 1, first.next_cursor).expect("second page");
    assert_eq!(second.items, vec![("C".to_owned(), 8)]);
    let third = search_page(passed, None, 1, second.next_cursor).expect("third page");
    assert_eq!(third.items, vec![("C".to_owned(), 9)]);
    assert_eq!(third.next_cursor, None);
    assert_eq!(third.estimated_total, None, "a resumed call saw a suffix");

    let whole = search_page(folders, None, 10, None).expect("one page");
    assert_eq!(whole.next_cursor, None);
    assert_eq!(whole.estimated_total, Some(5));
}

#[test]
fn a_search_cursor_whose_folder_vanished_resumes_at_the_next_folder() {
    let first =
        search_page(&[("A", 1, &[1, 2]), ("C", 3, &[4])], None, 1, None).expect("first page");
    assert_eq!(uids_of(&first), vec![1]);
    let second = search_page(&[("C", 3, &[4])], None, 5, first.next_cursor).expect("second page");
    assert_eq!(uids_of(&second), vec![4]);
}

#[test]
fn a_changed_uidvalidity_in_the_cursor_folder_voids_the_walk() {
    let first = search_page(&[("INBOX", 7, &[1, 2])], None, 1, None).expect("first page");
    let err = search_page(&[("INBOX", 8, &[1, 2])], None, 1, first.next_cursor)
        .expect_err("the cursor's UIDs name nothing under a new UIDVALIDITY");
    assert_eq!(
        err.recovery(),
        &bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(
            super::super::folder_scope(&MailboxName::new("INBOX").unwrap()),
        ))
    );
    assert_eq!(err.operation(), Some(AccountOperation::SearchMessages));
}

#[test]
fn a_search_page_refuses_cursors_and_limits_it_cannot_honour() {
    let malformed = AccountErrorKind::Request(RequestErrorKind::Malformed);
    let is_page_cursor_fault = |err: &AccountError| {
        matches!(
            err.chain().outermost(),
            Cause::Request(RequestCause::InvalidArgument {
                field: Some("page_cursor"),
                ..
            })
        )
    };
    // The retired offset spelling, garbage, and a legacy imap1 payload.
    for cursor in ["2", "not-a-number", "imapsearch1:imap1:5:INBOX:7:9"] {
        let err = search_page(&[], None, 2, Some(cursor.as_bytes().to_vec().into()))
            .expect_err("a cursor this crate did not mint");
        assert_eq!(*err.kind(), malformed, "{cursor}");
        assert!(is_page_cursor_fault(&err), "{cursor}");
    }
    let err = search_page(&[], None, 0, None).expect_err("a zero limit");
    assert_eq!(*err.kind(), malformed);
    assert!(
        !is_page_cursor_fault(&err),
        "a zero limit is not a cursor fault"
    );

    let first = search_page(&[("A", 1, &[1, 2])], Some("A"), 1, None).expect("first page");
    let err = search_page(&[("B", 1, &[1, 2])], Some("B"), 1, first.next_cursor)
        .expect_err("a cursor from another folder restriction");
    assert_eq!(*err.kind(), malformed);
    assert!(is_page_cursor_fault(&err));
}

#[test]
fn the_search_cursor_encoding_is_frozen() {
    let position = SearchPosition {
        folder: MailboxName::new("INBOX").unwrap(),
        uidvalidity: crate::types::UidValidity::new(7).expect("nonzero epoch"),
        key: crate::types::Uid::new(9).expect("valid protocol test value"),
    };
    let bytes = position.encode();
    assert_eq!(
        String::from_utf8(bytes.clone()).unwrap(),
        format!(
            "imapsearch1:{}",
            encode_object_id(
                &position.folder,
                crate::types::UidValidity::new(7).expect("nonzero epoch"),
                crate::types::Uid::new(9).expect("valid protocol test value")
            )
            .0
        )
    );
    assert_eq!(
        SearchPosition::decode(&bytes, AccountOperation::Search).unwrap(),
        position
    );
}

#[test]
fn imap_date_uses_the_rfc3501_date_text_production() {
    // date-text = date-day "-" date-month "-" date-year, and
    // date-day is 1*2DIGIT, so a single-digit day is not zero-padded.
    assert_eq!(imap_date(SystemTime::UNIX_EPOCH), "1-Jan-1970");
    assert_eq!(
        imap_date(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(60 * 60 * 24 * 364)),
        "31-Dec-1970"
    );
}

#[test]
fn search_plan_defaults_to_all_and_carries_the_folder_restriction() {
    let plan = search_plan(
        &super::super::DavScopeIndex::default(),
        &SearchRequest::default(),
        AccountOperation::SearchMessages,
    )
    .expect("plan");
    assert_eq!(plan.criteria, "ALL");
    assert!(plan.folder.is_none());

    let plan = search_plan(
        &super::super::DavScopeIndex::default(),
        &SearchRequest::filter(SearchFilter::In(ContainerId("Archive".to_owned()))),
        AccountOperation::SearchMessages,
    )
    .expect("plan");
    assert_eq!(plan.criteria, "ALL", "In contributes no criteria");
    assert_eq!(
        plan.folder.as_ref().map(MailboxName::as_str),
        Some("Archive")
    );
}

#[test]
fn search_plan_quotes_filter_operands() {
    let plan = search_plan(
        &super::super::DavScopeIndex::default(),
        &SearchRequest::filter(SearchFilter::Subject("quarterly \"report\"".to_owned())),
        AccountOperation::SearchMessages,
    )
    .expect("plan");
    assert_eq!(plan.criteria, "SUBJECT \"quarterly \\\"report\\\"\"");
}

#[test]
fn search_plan_substitutes_or_conjoins_the_provider_query() {
    // With no structured filter the raw query replaces the ALL.
    let plan = search_plan(
        &super::super::DavScopeIndex::default(),
        &SearchRequest::provider("  UNSEEN  "),
        AccountOperation::SearchMessages,
    )
    .expect("plan");
    assert_eq!(plan.criteria, "UNSEEN", "the raw query is trimmed");

    // With a structured filter the two are juxtaposed, which is
    // IMAP's implicit AND (RFC 3501 Section 6.4.4).
    let mut request = SearchRequest::filter(SearchFilter::From("a@b.test".to_owned()));
    request.provider_query = Some("UNSEEN".to_owned());
    let plan = search_plan(
        &super::super::DavScopeIndex::default(),
        &request,
        AccountOperation::SearchMessages,
    )
    .expect("plan");
    assert_eq!(plan.criteria, "FROM \"a@b.test\" UNSEEN");

    // A whitespace-only raw query is ignored entirely.
    let plan = search_plan(
        &super::super::DavScopeIndex::default(),
        &SearchRequest::provider("   "),
        AccountOperation::SearchMessages,
    )
    .expect("plan");
    assert_eq!(plan.criteria, "ALL");
}

#[test]
fn search_plan_rejects_structurally_unsafe_provider_queries() {
    for raw in [")", "(UNSEEN", "SUBJECT \"unterminated", "BODY {4}"] {
        let err = search_plan(
            &super::super::DavScopeIndex::default(),
            &SearchRequest::provider(raw),
            AccountOperation::SearchMessages,
        )
        .expect_err("malformed query");
        assert_eq!(
            *err.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }
    let plan = search_plan(
        &super::super::DavScopeIndex::default(),
        &SearchRequest::provider("OR (FROM \"a\\\\\\\"b\") (SUBJECT \"{4}\")"),
        AccountOperation::SearchMessages,
    )
    .expect("balanced query");
    assert_eq!(plan.criteria, "OR (FROM \"a\\\\\\\"b\") (SUBJECT \"{4}\")");
    let plan = search_plan(
        &super::super::DavScopeIndex::default(),
        &SearchRequest::provider("FROM"),
        AccountOperation::SearchMessages,
    )
    .expect("plan");
    assert_eq!(
        plan.criteria, "FROM",
        "the structural guard does not attempt operand-arity parsing"
    );
}

#[test]
fn and_and_or_combinators_produce_the_expected_criteria_shapes() {
    let and = combine_and(
        &super::super::DavScopeIndex::default(),
        &[
            SearchFilter::From("a@b.test".to_owned()),
            SearchFilter::Subject("hi".to_owned()),
        ],
        AccountOperation::SearchMessages,
    )
    .expect("and");
    assert_eq!(and.criteria, "FROM \"a@b.test\" SUBJECT \"hi\"");

    // An `In`-only conjunction degenerates to ALL plus the folder.
    let folder_only = combine_and(
        &super::super::DavScopeIndex::default(),
        &[SearchFilter::In(ContainerId("Archive".to_owned()))],
        AccountOperation::SearchMessages,
    )
    .expect("and");
    assert_eq!(folder_only.criteria, "ALL");
    assert_eq!(
        folder_only.folder.as_ref().map(MailboxName::as_str),
        Some("Archive")
    );

    // RFC 3501 OR takes exactly two search-keys, so a three-way OR
    // has to nest.
    let or = combine_or(
        &super::super::DavScopeIndex::default(),
        &[
            SearchFilter::From("a@b.test".to_owned()),
            SearchFilter::From("c@d.test".to_owned()),
            SearchFilter::From("e@f.test".to_owned()),
        ],
        AccountOperation::SearchMessages,
    )
    .expect("or");
    assert_eq!(
        or.criteria,
        "OR (OR (FROM \"a@b.test\") (FROM \"c@d.test\")) (FROM \"e@f.test\")"
    );

    assert_eq!(
        combine_or(
            &super::super::DavScopeIndex::default(),
            &[],
            AccountOperation::SearchMessages
        )
        .expect("empty or")
        .criteria,
        "ALL"
    );
}

#[test]
fn and_across_two_folders_is_rejected_rather_than_silently_narrowed() {
    // IMAP SEARCH runs against one selected mailbox, so a filter
    // naming two folders has no faithful single-command encoding.
    let err = combine_and(
        &super::super::DavScopeIndex::default(),
        &[
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::In(ContainerId("Sent".to_owned())),
        ],
        AccountOperation::SearchMessages,
    )
    .expect_err("two folders");
    assert_eq!(
        *err.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    );
    // The same folder twice is not a conflict.
    assert!(
        combine_and(
            &super::super::DavScopeIndex::default(),
            &[
                SearchFilter::In(ContainerId("Archive".to_owned())),
                SearchFilter::In(ContainerId("Archive".to_owned())),
            ],
            AccountOperation::SearchMessages,
        )
        .is_ok()
    );
}

#[test]
fn not_wraps_its_inner_criteria_and_keeps_the_folder() {
    let part = criteria_from_filter(
        &super::super::DavScopeIndex::default(),
        &SearchFilter::Not(Box::new(SearchFilter::And(vec![
            SearchFilter::In(ContainerId("Archive".to_owned())),
            SearchFilter::From("a@b.test".to_owned()),
        ]))),
        AccountOperation::SearchMessages,
    )
    .expect("not");
    assert_eq!(part.criteria, "NOT (FROM \"a@b.test\")");
    assert_eq!(
        part.folder.as_ref().map(MailboxName::as_str),
        Some("Archive")
    );
}

#[test]
fn flatten_thread_deep_chain_preserves_preorder() {
    let mut root = ThreadNode::new(Some(50_000), Vec::new());
    for uid in (1..50_000).rev() {
        root = ThreadNode::new(Some(uid), vec![root]);
    }
    let root = ThreadNode::new(None, vec![root, ThreadNode::new(Some(50_001), Vec::new())]);
    let mut uids = Vec::new();
    flatten_thread(&root, &mut uids);
    assert!(uids.into_iter().eq(1..=50_001));
    drop(root);
}
