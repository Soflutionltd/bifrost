use super::test_support::*;

/// Every page call re-runs the search. Resuming by offset into the fresh
/// result skipped the boundary survivor when an earlier result was expunged
/// between pages; the cursor is a position, so the survivor is returned.
#[tokio::test]
async fn search_messages_returns_the_boundary_survivor_after_an_earlier_expunge() {
    use bifrost_types::{SearchFilter, SearchRequest};
    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let request = |cursor: Option<bifrost_types::PageCursor>| {
        let mut request = SearchRequest::filter(SearchFilter::In(ContainerId("INBOX".to_owned())));
        request.limit = Some(2);
        request.page_cursor = cursor;
        request
    };
    let ids = |uids: &[u32]| {
        uids.iter()
            .map(|uid| {
                crate::account::encode_object_id(
                    &inbox,
                    crate::types::UidValidity::new(7).expect("nonzero epoch"),
                    crate::types::Uid::new(*uid).expect("nonzero test UID"),
                )
            })
            .collect::<Vec<_>>()
    };

    let first = search_page_against("* SEARCH 101 102 103 104", |account| {
        crate::account::search::search_messages(account, request(None))
    })
    .await;
    assert_eq!(first.items, ids(&[101, 102]));

    // 101 is expunged before page two.
    let cursor = first.next_cursor.clone();
    let second = search_page_against("* SEARCH 102 103 104", move |account| {
        crate::account::search::search_messages(account, request(cursor))
    })
    .await;
    assert_eq!(second.items, ids(&[103, 104]));
    assert_eq!(second.next_cursor, None);
}

/// Thread search pages by each thread's lowest member UID, not by the
/// server's thread order, so the same expunge cannot skip a thread.
#[tokio::test]
async fn thread_search_pages_by_lowest_member_uid() {
    use bifrost_types::{SearchFilter, SearchRequest};
    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let request = |cursor: Option<bifrost_types::PageCursor>| {
        let mut request = SearchRequest::filter(SearchFilter::In(ContainerId("INBOX".to_owned())));
        request.limit = Some(1);
        request.page_cursor = cursor;
        request
    };

    let first = search_page_against("* THREAD (104)(102 105)(101)(103)", |account| {
        crate::account::search::search(account, request(None))
    })
    .await;
    assert_eq!(
        first.items,
        vec![crate::account::encode_thread_id(
            &inbox,
            crate::types::UidValidity::new(7).expect("nonzero epoch"),
            &crate::types::set::UidSet::from_numbers([101])
        )]
    );

    // Thread 101 is expunged; an offset of 1 would land past thread 102.
    let cursor = first.next_cursor.clone();
    let second = search_page_against("* THREAD (104)(102 105)(103)", move |account| {
        crate::account::search::search(account, request(cursor))
    })
    .await;
    assert_eq!(
        second.items,
        vec![crate::account::encode_thread_id(
            &inbox,
            crate::types::UidValidity::new(7).expect("nonzero epoch"),
            &crate::types::set::UidSet::from_numbers([102, 105])
        )]
    );
}
