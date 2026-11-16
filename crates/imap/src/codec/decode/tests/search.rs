use super::*;

#[test]
fn untagged_search() {
    let (_, resp) = parse_response(b"* SEARCH 1 5 10\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search {
            numbers: uids,
            mod_seq,
        } = &*u
        {
            assert_eq!(uids, &[1, 5, 10]);
            assert!(mod_seq.is_none());
        } else {
            panic!("expected Search");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn untagged_search_empty() {
    let (_, resp) = parse_response(b"* SEARCH\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { numbers: uids, .. } = &*u {
            assert!(uids.is_empty());
        } else {
            panic!("expected Search");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// Zoho sends `* SEARCH \r\n` (trailing space, no results)
/// after UID MOVE empties a mailbox. The parser must tolerate trailing
/// whitespace between the SEARCH keyword and CRLF.
///
/// RFC 3501 Section 7.2.5 formal syntax:
///   `mailbox-data = "SEARCH" *(SP nz-number)`
/// The trailing SP without a subsequent nz-number is technically
/// non-conformant, but common enough to require tolerance (Postel's law).
#[test]
fn search_trailing_space_empty() {
    let (_, resp) = parse_response(b"* SEARCH \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search {
            numbers: uids,
            mod_seq,
        } = &*u
        {
            assert!(
                uids.is_empty(),
                "trailing space should not produce phantom results"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// Regression test: UID 0 in SEARCH results must be filtered out.
///
/// RFC 3501 Section 7.2.5 / Section 9: SEARCH results are `nz-number`.
/// UIDs and sequence numbers are never zero. Some non-conformant
/// servers include 0 in results; the parser accepts it during parsing
/// (to avoid `many0` silently dropping subsequent numbers) but must
/// discard it before returning results to the caller.
///
/// # References
/// - RFC 3501 Section 9 (`nz-number = digit-nz *DIGIT`)
/// - RFC 3501 Section 7.2.5 (`mailbox-data = "SEARCH" *(SP nz-number)`)
#[test]
fn search_filters_uid_zero() {
    let (_, resp) = parse_response(b"* SEARCH 5 0 10\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { numbers: uids, .. } = &*u {
            assert_eq!(
                *uids,
                vec![5, 10],
                "UID 0 must be filtered from SEARCH results"
            );
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// Regression test: UID 0 in SORT results must also be filtered out.
///
/// # References
/// - RFC 5256 Section 4 (SORT response uses same format as SEARCH)
#[test]
fn sort_filters_uid_zero() {
    let (_, resp) = parse_response(b"* SORT 3 0 7\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, .. } = &*u {
            assert_eq!(
                *nums,
                vec![3, 7],
                "UID 0 must be filtered from SORT results"
            );
        } else {
            panic!("expected Sort, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// multiple trailing spaces after SEARCH with results.
/// Ensures the trailing-space fix doesn't break normal SEARCH responses.
#[test]
fn search_trailing_spaces_with_results() {
    let (_, resp) = parse_response(b"* SEARCH 5 10 15  \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { numbers: uids, .. } = &*u {
            assert_eq!(*uids, vec![5, 10, 15]);
        } else {
            panic!("expected Search, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// trailing space in a full response with tag.
/// Simulates the exact Zoho wire sequence: `* SEARCH \r\nA001 OK Success\r\n`
#[test]
fn search_trailing_space_followed_by_tagged() {
    // Parse just the untagged part
    let (rest, resp) = parse_response(b"* SEARCH \r\nA001 OK Success\r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search { numbers: uids, .. } = &*u {
            assert!(uids.is_empty());
        } else {
            panic!("expected Search");
        }
    } else {
        panic!("expected Untagged");
    }
    // The tagged response should remain for subsequent parsing
    assert_eq!(rest, b"A001 OK Success\r\n");
}

/// SORT has the same `*(SP nz-number)` pattern as SEARCH.
/// A trailing space in `* SORT \r\n` must be tolerated.
///
/// RFC 5256 Section 4: `sort-data = "SORT" *(SP nz-number)`
#[test]
fn sort_trailing_space_empty() {
    let (_, resp) = parse_response(b"* SORT \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert!(
                nums.is_empty(),
                "trailing space should not produce phantom results"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Sort, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// SORT with results and trailing space.
#[test]
fn sort_trailing_space_with_results() {
    let (_, resp) = parse_response(b"* SORT 3 1 2 \r\n").unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert_eq!(*nums, vec![3, 1, 2]);
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Sort, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_with_all() {
    let input = b"* ESEARCH (TAG \"A001\") UID ALL 1:3,5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A001"));
            assert!(esearch.is_uid());
            assert_eq!(
                esearch.all(),
                vec![UidRange::range(1, 3), UidRange::single(5)]
            );
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_with_min_max_count() {
    let input = b"* ESEARCH (TAG \"A002\") UID MIN 1 MAX 100 COUNT 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A002"));
            assert!(esearch.is_uid());
            assert_eq!(esearch.min(), Some(1));
            assert_eq!(esearch.max(), Some(100));
            assert_eq!(esearch.count, Some(5));
            assert!(esearch.all().is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_empty() {
    let input = b"* ESEARCH (TAG \"A003\")\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A003"));
            assert!(!esearch.is_uid());
            assert!(esearch.all().is_empty());
            assert_eq!(esearch.min(), None);
            assert_eq!(esearch.max(), None);
            assert_eq!(esearch.count, None);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_all_fields_together() {
    let input = b"* ESEARCH (TAG \"A004\") UID MIN 2 MAX 50 COUNT 10 ALL 2:5,10,20:50\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A004"));
            assert!(esearch.is_uid());
            assert_eq!(esearch.min(), Some(2));
            assert_eq!(esearch.max(), Some(50));
            assert_eq!(esearch.count, Some(10));
            assert_eq!(
                esearch.all(),
                vec![
                    UidRange::range(2, 5),
                    UidRange::single(10),
                    UidRange::range(20, 50),
                ]
            );
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_no_uid_indicator() {
    // ESEARCH without UID indicator  -  sequence numbers, not UIDs.
    let input = b"* ESEARCH (TAG \"A005\") COUNT 3 ALL 1,5,10\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.tag.as_deref(), Some("A005"));
            assert!(!esearch.is_uid());
            assert_eq!(esearch.count, Some(3));
            assert_eq!(
                esearch.all(),
                vec![
                    UidRange::single(1),
                    UidRange::single(5),
                    UidRange::single(10),
                ]
            );
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_no_tag() {
    // Some servers may omit the TAG correlator.
    let input = b"* ESEARCH UID COUNT 0\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert!(esearch.tag.is_none());
            assert!(esearch.is_uid());
            assert_eq!(esearch.count, Some(0));
            assert!(esearch.all().is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_only_min() {
    let input = b"* ESEARCH (TAG \"A1\") UID MIN 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.min(), Some(5));
            assert_eq!(esearch.max(), None);
            assert_eq!(esearch.count, None);
            assert!(esearch.all().is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_only_max() {
    let input = b"* ESEARCH (TAG \"A1\") UID MAX 99\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.min(), None);
            assert_eq!(esearch.max(), Some(99));
            assert_eq!(esearch.count, None);
            assert!(esearch.all().is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_only_count() {
    let input = b"* ESEARCH (TAG \"A1\") UID COUNT 0\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.min(), None);
            assert_eq!(esearch.max(), None);
            assert_eq!(esearch.count, Some(0));
            assert!(esearch.all().is_empty());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_all_single_uid() {
    let input = b"* ESEARCH (TAG \"A1\") UID ALL 42\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.all(), vec![UidRange::single(42)]);
            assert_eq!(esearch.min(), None);
            assert_eq!(esearch.max(), None);
            assert_eq!(esearch.count, None);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_all_and_count_no_minmax() {
    let input = b"* ESEARCH (TAG \"A1\") UID COUNT 3 ALL 1,5,10\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(esearch.count, Some(3));
            assert_eq!(
                esearch.all(),
                vec![
                    UidRange::single(1),
                    UidRange::single(5),
                    UidRange::single(10),
                ]
            );
            assert_eq!(esearch.min(), None);
            assert_eq!(esearch.max(), None);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_keywords_case_insensitive() {
    // Result-data keywords are parsed via to_ascii_uppercase(); verify
    // lowercase and mixed-case variants work.
    let input = b"* ESEARCH (TAG \"A1\") uid min 1 Max 100 count 5 all 1:3\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert!(esearch.is_uid());
            assert_eq!(esearch.min(), Some(1));
            assert_eq!(esearch.max(), Some(100));
            assert_eq!(esearch.count, Some(5));
            assert_eq!(esearch.all(), vec![UidRange::range(1, 3)]);
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn esearch_unknown_key_skipped() {
    // ESEARCH with unknown keys should not fail
    let input = b"ESEARCH (TAG \"A001\") UID ALL 1:3 XFUTURE foo\r\n";
    let (_, resp) = parse_untagged_esearch(input).unwrap();
    if let UntaggedResponse::Esearch(esearch) = resp {
        assert_eq!(esearch.all(), vec![UidRange::range(1, 3)]);
    } else {
        panic!("expected Esearch, got {resp:?}");
    }
}

#[test]
fn parse_thread_simple() {
    // Simple linear thread: (1 2 3)
    let input = b"* THREAD (1 2 3)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[0].children.len(), 1);
        assert_eq!(threads[0].children[0].id, Some(2));
        assert_eq!(threads[0].children[0].children.len(), 1);
        assert_eq!(threads[0].children[0].children[0].id, Some(3));
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_multiple_roots() {
    // Two separate threads: (1)(2)
    let input = b"* THREAD (1)(2)\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 2);
        assert_eq!(threads[0].id, Some(1));
        assert!(threads[0].children.is_empty());
        assert_eq!(threads[1].id, Some(2));
        assert!(threads[1].children.is_empty());
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_nested() {
    // Thread with branching: (1 (2)(3))
    let input = b"* THREAD (1 (2)(3))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[0].children.len(), 2);
        assert_eq!(threads[0].children[0].id, Some(2));
        assert_eq!(threads[0].children[1].id, Some(3));
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_dummy_parent() {
    // Dummy parent (no UID): ((1)(2))
    let input = b"* THREAD ((1)(2))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        // Dummy parent has id=None (RFC 5256 Section 4)
        assert_eq!(threads[0].id, None);
        assert_eq!(threads[0].children.len(), 2);
        assert_eq!(threads[0].children[0].id, Some(1));
        assert_eq!(threads[0].children[1].id, Some(2));
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_empty() {
    // Empty thread response
    let input = b"* THREAD\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert!(threads.is_empty());
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_complex() {
    // Complex: (1 (2 3)(4 5 6))
    let input = b"* THREAD (1 (2 3)(4 5 6))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[0].children.len(), 2);
        // First child: (2 3)  -  2 with child 3
        assert_eq!(threads[0].children[0].id, Some(2));
        assert_eq!(threads[0].children[0].children.len(), 1);
        assert_eq!(threads[0].children[0].children[0].id, Some(3));
        // Second child: (4 5 6)  -  4 with child 5 with child 6
        assert_eq!(threads[0].children[1].id, Some(4));
        assert_eq!(threads[0].children[1].children.len(), 1);
        assert_eq!(threads[0].children[1].children[0].id, Some(5));
        assert_eq!(threads[0].children[1].children[0].children.len(), 1);
        assert_eq!(threads[0].children[1].children[0].children[0].id, Some(6));
        return;
    }
    panic!("expected Thread response");
}

#[test]
fn parse_thread_rfc_example() {
    // Example from RFC 5256 Section 4: (2)(3 6 (4 23)(44 7 96))
    let input = b"* THREAD (2)(3 6 (4 23)(44 7 96))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 2);
        // First thread: just message 2
        assert_eq!(threads[0].id, Some(2));
        assert!(threads[0].children.is_empty());
        // Second thread: 3 -> 6 -> [(4 -> 23), (44 -> 7 -> 96)]
        assert_eq!(threads[1].id, Some(3));
        assert_eq!(threads[1].children.len(), 1);
        assert_eq!(threads[1].children[0].id, Some(6));
        assert_eq!(threads[1].children[0].children.len(), 2);
        // Branch 1: (4 23)
        assert_eq!(threads[1].children[0].children[0].id, Some(4));
        assert_eq!(threads[1].children[0].children[0].children.len(), 1);
        assert_eq!(threads[1].children[0].children[0].children[0].id, Some(23));
        // Branch 2: (44 7 96)
        assert_eq!(threads[1].children[0].children[1].id, Some(44));
        assert_eq!(threads[1].children[0].children[1].children.len(), 1);
        assert_eq!(threads[1].children[0].children[1].children[0].id, Some(7));
        assert_eq!(
            threads[1].children[0].children[1].children[0]
                .children
                .len(),
            1
        );
        assert_eq!(
            threads[1].children[0].children[1].children[0].children[0].id,
            Some(96)
        );
        return;
    }
    panic!("expected Thread response");
}

/// RFC 5256 Section 4: dummy parents (empty parentheses with no UID)
/// must be distinguishable from real messages. Using `Option<u32>`
/// (None for dummy) is more precise than the u32/0 sentinel.
#[test]
fn spec_audit_thread_dummy_parent_uses_option() {
    // Thread with dummy parent: ((1)(2))
    let input = b"* THREAD ((1)(2))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        // Dummy parent must have id == None per RFC 5256 Section 4
        assert_eq!(
            threads[0].id, None,
            "RFC 5256 Section 4: dummy parent has no UID, expected None, got {:?}",
            threads[0].id
        );
        assert_eq!(threads[0].children.len(), 2);
        assert_eq!(threads[0].children[0].id, Some(1));
        assert_eq!(threads[0].children[1].id, Some(2));
        return;
    }
    panic!("expected Thread response");
}

/// RFC 5256 Section 5: `thread-nested = 2*thread-list` requires at least 2
/// groups inside a dummy parent.  `((1))` has only 1 group, making it
/// syntactically invalid.  Per Postel's law (RFC 1122 Section 1.2.2) we
/// accept the input, but collapse the unnecessary dummy parent so the
/// result is equivalent to the valid form `(1)`.
#[test]
fn spec_audit_thread_single_child_dummy_collapsed() {
    let input = b"* THREAD ((1))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1, "should have one top-level thread");
        // After collapsing, the thread should be a direct message
        // (id=Some(1)), NOT a dummy parent wrapping one child.
        assert_eq!(
            threads[0].id,
            Some(1),
            "RFC 5256 Section 5: single-child dummy parent must be \
                 collapsed  -  expected id=Some(1), got id={:?}",
            threads[0].id
        );
        assert!(
            threads[0].children.is_empty(),
            "collapsed thread should have no children"
        );
        return;
    }
    panic!("expected Thread response");
}

/// Same as above but with a chain inside: `((1 2))` should collapse to
/// `ThreadNode { id: 1, children: [ThreadNode { id: 2 }] }`.
#[test]
fn spec_audit_thread_single_child_dummy_collapsed_chain() {
    let input = b"* THREAD ((1 2))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(
            threads[0].id,
            Some(1),
            "RFC 5256 Section 5: single-child dummy parent must be \
                 collapsed  -  expected id=Some(1), got id={:?}",
            threads[0].id
        );
        assert_eq!(threads[0].children.len(), 1);
        assert_eq!(threads[0].children[0].id, Some(2));
        return;
    }
    panic!("expected Thread response");
}

/// Dummy parent with 2+ children is valid (`thread-nested = 2*thread-list`)
/// and must NOT be collapsed.
#[test]
fn spec_audit_thread_multi_child_dummy_not_collapsed() {
    let input = b"* THREAD ((1)(2))\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Thread(threads) = *boxed
    {
        assert_eq!(threads.len(), 1);
        assert_eq!(
            threads[0].id, None,
            "valid dummy parent (2+ children) must keep id=None"
        );
        assert_eq!(threads[0].children.len(), 2);
        return;
    }
    panic!("expected Thread response");
}

/// SEARCH response with no results.
#[test]
fn search_empty_result() {
    let input = b"* SEARCH\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Search { numbers: uids, .. } = *boxed
    {
        assert!(uids.is_empty());
        return;
    }
    panic!("expected empty SEARCH");
}

/// SEARCH response with multiple UIDs.
#[test]
fn search_multiple_uids() {
    let input = b"* SEARCH 1 5 10 42\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Search { numbers: uids, .. } = *boxed
    {
        assert_eq!(uids, vec![1, 5, 10, 42]);
        return;
    }
    panic!("expected SEARCH with UIDs");
}

/// ESEARCH with ALL expands uid-set into individual UIDs.
#[test]
fn esearch_all_preserves_ranges() {
    let input = b"* ESEARCH (TAG \"A001\") ALL 1:3,5,10:12\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert_eq!(esearch.tag.as_deref(), Some("A001"));
        assert!(!esearch.is_uid());
        assert_eq!(
            esearch.all(),
            vec![
                UidRange::range(1, 3),
                UidRange::single(5),
                UidRange::range(10, 12),
            ]
        );
        return;
    }
    panic!("expected ESEARCH with ALL ranges");
}

// ===== Task 4: THREAD parser tests (RFC 5256 Section 4) =====

/// Single flat thread: (5)  -  one message with no children (RFC 5256 Section 4).
#[test]
fn thread_single_flat() {
    let input = b"THREAD (5)\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(5));
        assert!(threads[0].children.is_empty());
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Chained thread: (10 20 30)  -  linear chain (RFC 5256 Section 4).
#[test]
fn thread_chained() {
    let input = b"THREAD (10 20 30)\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(10));
        assert_eq!(threads[0].children.len(), 1);
        assert_eq!(threads[0].children[0].id, Some(20));
        assert_eq!(threads[0].children[0].children.len(), 1);
        assert_eq!(threads[0].children[0].children[0].id, Some(30));
        assert!(threads[0].children[0].children[0].children.is_empty());
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Nested with branches: (1 2 (3)(4)) (RFC 5256 Section 4).
#[test]
fn thread_nested_with_branches() {
    let input = b"THREAD (1 2 (3)(4))\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[0].children.len(), 1);
        let child = &threads[0].children[0];
        assert_eq!(child.id, Some(2));
        assert_eq!(child.children.len(), 2);
        assert_eq!(child.children[0].id, Some(3));
        assert_eq!(child.children[1].id, Some(4));
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Multiple top-level threads: (1)(2)(3) (RFC 5256 Section 4).
#[test]
fn thread_multiple_top_level() {
    let input = b"THREAD (1)(2)(3)\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 3);
        assert_eq!(threads[0].id, Some(1));
        assert_eq!(threads[1].id, Some(2));
        assert_eq!(threads[2].id, Some(3));
        assert!(threads[0].children.is_empty());
        assert!(threads[1].children.is_empty());
        assert!(threads[2].children.is_empty());
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Empty thread response (RFC 5256 Section 4).
#[test]
fn thread_empty_response() {
    let input = b"THREAD\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert!(threads.is_empty());
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Dummy root node: ((5)(10))  -  parent with no UID (RFC 5256 Section 4).
#[test]
fn thread_dummy_root_nodes() {
    let input = b"THREAD ((5)(10)(15))\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, None); // dummy parent (RFC 5256 Section 4)
        assert_eq!(threads[0].children.len(), 3);
        assert_eq!(threads[0].children[0].id, Some(5));
        assert_eq!(threads[0].children[1].id, Some(10));
        assert_eq!(threads[0].children[2].id, Some(15));
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Deeply nested thread: (1 (2 (3 (4 5)))) (RFC 5256 Section 4).
#[test]
fn thread_deeply_nested() {
    let input = b"THREAD (1 (2 (3 (4 5))))\r\n";
    let (_, resp) = parse_untagged_thread(input).unwrap();
    if let UntaggedResponse::Thread(threads) = resp {
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].id, Some(1));
        // 1 -> (2 -> (3 -> (4 -> 5)))
        let n2 = &threads[0].children[0];
        assert_eq!(n2.id, Some(2));
        let n3 = &n2.children[0];
        assert_eq!(n3.id, Some(3));
        let n4 = &n3.children[0];
        assert_eq!(n4.id, Some(4));
        assert_eq!(n4.children.len(), 1);
        assert_eq!(n4.children[0].id, Some(5));
    } else {
        panic!("expected Thread, got {resp:?}");
    }
}

/// Thread with truncated input should fail (RFC 5256 Section 4).
#[test]
fn thread_truncated_input() {
    // Missing closing paren and CRLF
    assert!(parse_untagged_thread(b"THREAD (1 2").is_err());
    // Missing CRLF
    assert!(parse_untagged_thread(b"THREAD (1 2)").is_err());
}

/// `build_thread_tree` with an empty chain and empty branch groups (RFC 5256 Section 4).
#[test]
fn build_thread_tree_empty() {
    let result = build_thread_tree(&[], &[]);
    assert!(result.is_empty());
}

/// `build_thread_tree` with a single UID and no branches (RFC 5256 Section 4).
#[test]
fn build_thread_tree_single_uid() {
    let result = build_thread_tree(&[Some(42)], &[vec![]]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, Some(42));
    assert!(result[0].children.is_empty());
}

/// Excessively nested THREAD response must be rejected to prevent stack
/// overflow (defense-in-depth; RFC 5256 Section 4 does not cap depth).
#[test]
fn thread_rejects_excessive_nesting_depth() {
    // Build a deeply nested thread: (1 (2 (3 (4 ... ))))
    // with 256 levels of nesting  -  well beyond MAX_THREAD_NESTING_DEPTH (64).
    let depth = 256;
    let mut input = String::from("THREAD ");
    for i in 1..=depth {
        input.push('(');
        input.push_str(&i.to_string());
        input.push(' ');
    }
    for _ in 1..=depth {
        input.push(')');
    }
    input.push_str("\r\n");

    let result = parse_untagged_thread(input.as_bytes());
    assert!(
        result.is_err(),
        "deeply nested THREAD response (depth {depth}) should be rejected"
    );
}

/// ESEARCH with unknown key is skipped gracefully (stress variant).
#[test]
fn esearch_unknown_key_skipped_stress() {
    let input = b"* ESEARCH (TAG \"A001\") UID XFUTURE somevalue\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert!(esearch.all().is_empty(), "unknown key should be skipped");
        assert!(esearch.is_uid());
        return;
    }
    panic!("expected Esearch response");
}

// ===== ESEARCH MIN/MAX must reject zero =====
// RFC 4731 Section 3.1: MIN SP nz-number / MAX SP nz-number
// Zero is not a valid message number or UID.

#[test]
fn esearch_min_rejects_zero() {
    // "* ESEARCH (TAG \"A1\") MIN 0\r\n"  -  MIN 0 is invalid per RFC 4731.
    let input = b"* ESEARCH (TAG \"A1\") MIN 0\r\n";
    assert!(parse_response(input).is_err());
}

#[test]
fn esearch_max_rejects_zero() {
    // "* ESEARCH (TAG \"A1\") MAX 0\r\n"  -  MAX 0 is invalid per RFC 4731.
    let input = b"* ESEARCH (TAG \"A1\") MAX 0\r\n";
    assert!(parse_response(input).is_err());
}

#[test]
fn esearch_min_accepts_one() {
    // MIN 1 is the smallest valid nz-number
    let input = b"* ESEARCH (TAG \"A1\") MIN 1\r\n";
    let result = parse_response(input);
    assert!(result.is_ok());
}

/// RFC 4731 Section 3.1: ESEARCH ALL uses `sequence-set` which allows `*`.
/// previously used `uid_set()` which only accepts `nz-number`.
#[test]
fn spec_audit_esearch_all_accepts_wildcard() {
    let input = b"* ESEARCH (TAG \"A1\") ALL 1:*\r\n";
    let (rem, resp) = parse_response(input).unwrap();
    assert!(rem.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(es) = &*u {
            assert_eq!(es.all().len(), 1);
            assert_eq!(es.all()[0].start(), 1);
            assert_eq!(es.all()[0].end_bound(), Some(crate::types::SeqBound::Star));
        } else {
            panic!("Expected Esearch, got {u:?}");
        }
    } else {
        panic!("Expected Untagged response");
    }
}

/// Regression: `tag_no_case(&b"UID"[..])` without a token boundary check
/// greedily matches the prefix of atoms like "UIDFOO", setting `uid = true`
/// and leaving the remaining "FOO" fragment to corrupt subsequent parsing.
/// The UID indicator must be a standalone keyword per RFC 4731 Section 3.1.
#[test]
fn esearch_uid_keyword_boundary() {
    // The unknown extension keyword "UIDFOO" starts with "UID".
    // Without a token boundary check, the parser greedily consumes "UID",
    // sets uid=true, and then fails on the leftover "FOO".
    // Correct behaviour: "UIDFOO" is NOT the UID indicator; it should be
    // parsed as an unknown key (and its value skipped) with uid=false.
    let input = b"ESEARCH (TAG \"A1\") UIDFOO 42 COUNT 3\r\n";
    let result = parse_untagged_esearch(input);
    assert!(
        result.is_ok(),
        "ESEARCH with atom starting with 'UID' must not fail \
             (RFC 4731 Section 3.1 token boundary), got: {result:?}"
    );
    let (_, resp) = result.unwrap();
    if let UntaggedResponse::Esearch(es) = resp {
        assert!(
            !es.is_uid(),
            "uid must be false when the atom is 'UIDFOO', not 'UID'"
        );
        assert_eq!(
            es.count,
            Some(3),
            "COUNT must still be parsed after skipping unknown 'UIDFOO' key"
        );
    } else {
        panic!("expected Esearch, got {resp:?}");
    }
}

// ===== nz_number enforcement (RFC 3501 Section 9) =====

#[test]
fn search_accepts_zero_in_results() {
    // RFC 3501 Section 7.2.5 ABNF says `nz-number`. Per Postel's law
    // (RFC 1122 Section 1.2.2) we accept 0 during parsing (to avoid
    // `many0` stopping early and losing subsequent valid results), but
    // filter it out before returning since UID 0 is semantically invalid.
    let input = b"* SEARCH 1 0 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search {
            numbers: uids,
            mod_seq,
        } = &*u
        {
            assert_eq!(
                *uids,
                vec![1, 5],
                "0 must be filtered; valid UIDs must be preserved"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Search response, got {u:?}  -  valid UIDs must not be lost");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn search_empty_results_still_works() {
    let input = b"* SEARCH\r\n";
    let (_, resp) = parse_response(input).unwrap();
    // empty SEARCH is fine (many0 accepts zero results)
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Search { numbers: uids, .. } = *boxed
    {
        assert!(uids.is_empty());
        return;
    }
    panic!("expected Search");
}

/// M2: ESEARCH unknown keys with parenthesized values are not skipped correctly.
/// The current skip logic only consumes a single token, so a parenthesized group
/// like `(1 2)` causes a parse failure because after consuming `(1` the inner
/// `2` becomes an "unknown key" whose `sp` call hits `)` and errors out.
#[test]
fn spec_audit_m2_esearch_parenthesized_unknown_value() {
    let input = b"* ESEARCH (TAG \"A001\") XFOO (1 2) COUNT 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(
                esearch.count,
                Some(5),
                "COUNT after an unknown key with parenthesized value should be parsed"
            );
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// M5: `* SORT 2 3 6` should parse into `UntaggedResponse::Sort { nums: vec![2, 3, 6], mod_seq: None }`
/// per RFC 5256 Section 4.
#[test]
fn spec_audit_m5_sort_response() {
    let input = b"* SORT 2 3 6\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert_eq!(nums, &[2, 3, 6], "SORT response should contain [2, 3, 6]");
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Sort, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// M1: ESEARCH `tag-string` must accept `astring`.
/// RFC 9051 Section 9: `tag-string = astring`.
#[test]
fn audit_m1_esearch_tag_string_accepts_astring() {
    let input = b"* ESEARCH (TAG A001) UID COUNT 5\r\n";
    let (rest, resp) = parse_response(input).unwrap();
    assert!(rest.is_empty());
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(e) = &*u {
            assert_eq!(e.tag.as_deref(), Some("A001"));
            assert!(e.is_uid());
            assert_eq!(e.count, Some(5));
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// M4: THREAD member UIDs must use `nz-number`  -  UID 0 is invalid.
/// RFC 5256 Section 5.
#[test]
fn audit_m4_thread_rejects_zero_uid() {
    // THREAD must reject UID 0 per RFC 5256.
    let input = b"* THREAD (5 0 3)\r\n";
    assert!(parse_response(input).is_err());
}

#[test]
fn audit_sort_unknown_variant_preserved_as_other() {
    let (_, cap) = capability(b"SORT=XYZZY").unwrap();
    assert!(
        matches!(cap, Capability::Other(ref raw) if raw == "SORT=XYZZY"),
        "unknown SORT=* capability must be preserved as Other, got {cap:?}"
    );
}

// ===== ESEARCH/STATUS unknown-value skip must handle quoted strings and literals =====
// RFC 9051 Section9: tagged-ext-val = tagged-ext-simple / "(" [tagged-ext-comp] ")"
// tagged-ext-simple = sequence-set / number / number64 / astring
// astring = 1*ASTRING-CHAR / string
// string = quoted / literal
// The previous skip logic used take_while1 which only handles atom-like
// values, breaking on spaces (in quoted strings) or \r\n (in literals).

#[test]
fn regression_esearch_unknown_key_quoted_string_value() {
    // Unknown ESEARCH key with a quoted string value containing spaces.
    // The quoted string's internal spaces must not prematurely terminate
    // the skip, corrupting parsing of subsequent known keys.
    let input = b"* ESEARCH (TAG \"A001\") UID XFUTURE \"hello world\" COUNT 5\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(
                esearch.count,
                Some(5),
                "COUNT must be parsed after skipping unknown quoted-string value"
            );
            assert!(esearch.is_uid());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

#[test]
fn regression_esearch_unknown_key_literal_value() {
    // Unknown ESEARCH key with a literal value.
    // The literal's {N}\r\n prefix must be handled correctly.
    let input = b"* ESEARCH (TAG \"A001\") UID XBLOB {5}\r\nhello COUNT 3\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Esearch(esearch) = &*u {
            assert_eq!(
                esearch.count,
                Some(3),
                "COUNT must be parsed after skipping unknown literal value"
            );
            assert!(esearch.is_uid());
        } else {
            panic!("expected Esearch, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

// ── 9. Loop break on incomplete (L1550, L2009) ──

/// RFC 4731 Section 3.1: ESEARCH with trailing SP after last result data
/// Trailing whitespace before CRLF in an ESEARCH response must be
/// tolerated per Postel's law (RFC 1122 Section 1.2.2), consistent
/// with every other parser in the crate (FLAGS, CAPABILITY, LIST,
/// VANISHED, ENABLED, ID, SEARCH, SORT, QUOTA, ACL).
///
/// Regression: previously the loop broke when `rest.first() == Some(&b'\r')`
/// but did not advance `input` past the trailing space, causing `crlf()`
/// to fail on ` \r\n`. The structured ESEARCH data was silently lost.
#[test]
fn esearch_trailing_space_tolerated() {
    let input = b"ESEARCH (TAG \"A001\") UID COUNT 3 \r\n";
    let (_, resp) = parse_untagged_esearch(input)
        .expect("ESEARCH with trailing space must parse (Postel's law)");
    if let UntaggedResponse::Esearch(esearch) = resp {
        assert_eq!(esearch.tag.as_deref(), Some("A001"));
        assert!(esearch.is_uid());
        assert_eq!(esearch.count, Some(3));
    } else {
        panic!("expected Esearch, got {resp:?}");
    }
}

/// RFC 4731 Section 3.1: ESEARCH with no result data at all  -  just
/// tag and UID, followed immediately by CRLF. The while loop at L1548
/// should not enter the body because `sp()` fails on `\r\n`.
#[test]
fn esearch_no_result_data_immediate_crlf() {
    let input = b"* ESEARCH (TAG \"A001\") UID\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert!(esearch.is_uid());
        assert!(esearch.all().is_empty());
        assert_eq!(esearch.min(), None);
        assert_eq!(esearch.max(), None);
        assert_eq!(esearch.count, None);
        return;
    }
    panic!("expected Esearch response");
}

/// Regression: non-conformant server sends 0 in SEARCH results.
/// RFC 3501 Section 7.2.5 ABNF says `nz-number`, but some servers
/// emit 0. Per Postel's law (RFC 1122 Section 1.2.2), the parser
/// must accept 0 rather than silently dropping it and all subsequent
/// results.
#[test]
fn search_zero_in_results_filtered() {
    // 0 is accepted during parsing (Postel's law) to avoid losing
    // subsequent valid results, but filtered from the final output
    // since UID/sequence-number 0 is invalid (RFC 3501 Section 9).
    let input = b"* SEARCH 1 0 3\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Search {
            numbers: uids,
            mod_seq,
        } = &*u
        {
            assert_eq!(
                *uids,
                vec![1, 3],
                "0 must be filtered; valid UIDs (1, 3) preserved"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Search response, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// Regression: non-conformant server sends 0 in SORT results.
/// 0 is accepted during parsing but filtered from the final output.
#[test]
fn sort_zero_in_results_filtered() {
    let input = b"* SORT 2 0 7\r\n";
    let (_, resp) = parse_response(input).unwrap();
    if let Response::Untagged(u) = resp {
        if let UntaggedResponse::Sort { nums, mod_seq } = &*u {
            assert_eq!(
                *nums,
                vec![2, 7],
                "0 must be filtered; valid UIDs (2, 7) preserved"
            );
            assert_eq!(*mod_seq, None);
        } else {
            panic!("expected Sort response, got {u:?}");
        }
    } else {
        panic!("expected Untagged");
    }
}

/// RFC 4731 Section 3.1: ESEARCH with an unknown key that has no value
/// (immediately followed by CRLF) must not cause a parse failure.
/// Servers may extend ESEARCH with new keys, and clients should skip
/// unknown keys gracefully per Postel's law (RFC 1122 Section 1.2.2).
#[test]
fn regression_esearch_unknown_key_no_value() {
    let input = b"* ESEARCH (TAG \"t1\") UID COUNT 5 XFUTURE\r\n";
    let resp = parse_response(input);
    let (_, resp) = resp.expect(
        "ESEARCH with trailing unknown key (no value) must parse \
             per Postel's law (RFC 1122 Section 1.2.2)",
    );
    if let Response::Untagged(boxed) = resp
        && let UntaggedResponse::Esearch(esearch) = *boxed
    {
        assert_eq!(
            esearch.count,
            Some(5),
            "COUNT must be preserved when trailing unknown key has no value"
        );
        assert!(esearch.is_uid(), "UID indicator must be preserved");
        assert_eq!(esearch.tag.as_deref(), Some("t1"), "TAG must be preserved");
        return;
    }
    panic!("expected ESEARCH response");
}
