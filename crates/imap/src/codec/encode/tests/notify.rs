use super::*;

/// RFC 5465 Section 3: `NOTIFY NONE` cancels all event subscriptions.
#[test]
fn encode_notify_none() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifyNone;
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 NOTIFY NONE\r\n");
}

/// RFC 5465 Section 3: basic NOTIFY SET with selected mailbox and message events.
#[test]
fn encode_notify_set_selected_basic() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Selected,
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        )],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (selected (MessageNew MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 5.2: `MessageNew` with fetch attributes for selected mailbox.
#[test]
fn encode_notify_set_with_fetch_attrs() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Selected,
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![
                        "uid".into(),
                        "body.peek[header.fields (from to subject)]".into(),
                    ],
                },
                NotifyEvent::MessageExpunge,
            ],
        )],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (selected (MessageNew \
          (uid body.peek[header.fields (from to subject)]) MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 4: STATUS indicator triggers initial STATUS responses.
#[test]
fn encode_notify_set_with_status_indicator() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: true,
        event_groups: vec![
            NotifyEventGroup::new(
                MailboxFilter::Selected,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            ),
            NotifyEventGroup::new(
                MailboxFilter::Subtree(vec!["Lists".into()]),
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            ),
        ],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // Matches RFC 5465 Section 9.1 example (without fetch-attrs):
    // b NOTIFY SET STATUS (selected (MessageNew MessageExpunge))
    //   (subtree Lists (MessageNew MessageExpunge))
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET STATUS (selected (MessageNew MessageExpunge)) \
          (subtree \"Lists\" (MessageNew MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 6.2: selected-delayed filter.
#[test]
fn encode_notify_set_selected_delayed() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::SelectedDelayed,
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
                NotifyEvent::FlagChange,
            ],
        )],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (selected-delayed (MessageNew MessageExpunge FlagChange))\r\n"
    );
}

/// RFC 5465 Section 6: all simple mailbox filter types.
#[test]
fn encode_notify_set_simple_filters() {
    for (filter, expected) in [
        (MailboxFilter::Inboxes, "inboxes"),
        (MailboxFilter::Personal, "personal"),
        (MailboxFilter::Subscribed, "subscribed"),
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup::new(
                filter,
                vec![NotifyEvent::MailboxName],
            )],
        });
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
        let expected_str = format!("A001 NOTIFY SET ({expected} (MailboxName))\r\n");
        assert_eq!(&buf[..], expected_str.as_bytes(), "filter: {expected}");
    }
}

/// RFC 5465 Section 6.7: mailboxes filter with multiple mailbox names.
#[test]
fn encode_notify_set_mailboxes_multiple() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Mailboxes(vec!["INBOX".into(), "Sent Items".into()]),
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        )],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (mailboxes (\"INBOX\" \"Sent Items\") (MessageNew MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 8: single mailbox in subtree/mailboxes is encoded bare.
#[test]
fn encode_notify_set_subtree_single() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Subtree(vec!["Lists".into()]),
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        )],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (subtree \"Lists\" (MessageNew MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 8: events = "NONE" for an event group with no events.
#[test]
fn encode_notify_set_events_none() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(MailboxFilter::Selected, vec![])],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 NOTIFY SET (selected NONE)\r\n");
}

/// All event types encode correctly (RFC 5465 Section 5).
#[test]
fn encode_notify_set_all_event_types() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![
            NotifyEventGroup::new(
                MailboxFilter::Selected,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::FlagChange,
                    NotifyEvent::AnnotationChange,
                ],
            ),
            NotifyEventGroup::new(
                MailboxFilter::Personal,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::MailboxName,
                    NotifyEvent::SubscriptionChange,
                    NotifyEvent::MailboxMetadataChange,
                    NotifyEvent::ServerMetadataChange,
                ],
            ),
        ],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET \
          (selected (MessageNew MessageExpunge FlagChange AnnotationChange)) \
          (personal (MessageNew MessageExpunge MailboxName SubscriptionChange \
          MailboxMetadataChange ServerMetadataChange))\r\n"
    );
}

/// Extension event types are encoded verbatim (RFC 5465 Section 8: event-ext = atom).
#[test]
fn encode_notify_set_extension_event() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Personal,
            vec![NotifyEvent::Other("VendorSpecificEvent".into())],
        )],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (personal (VendorSpecificEvent))\r\n"
    );
}

/// NOTIFY SET with empty `event_groups` must fail (RFC 5465 Section 8:
/// event-groups requires at least one event-group).
#[test]
fn encode_notify_set_empty_event_groups_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "NOTIFY SET with empty event_groups must fail (RFC 5465 Section 8)"
    );
}

/// NOTIFY SET subtree with empty mailbox list must fail
/// (RFC 5465 Section 8: one-or-more-mailbox).
#[test]
fn encode_notify_set_subtree_empty_mailboxes_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Subtree(vec![]),
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        )],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "subtree with empty mailbox list must fail (RFC 5465 Section 8)"
    );
}

/// NOTIFY SET mailboxes with empty mailbox list must fail
/// (RFC 5465 Section 8: one-or-more-mailbox).
#[test]
fn encode_notify_set_mailboxes_empty_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Mailboxes(vec![]),
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        )],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "mailboxes with empty mailbox list must fail (RFC 5465 Section 8)"
    );
}

/// `NotifyEvent::Other` must be a valid IMAP atom (RFC 5465 Section 8: event-ext = atom).
#[test]
fn encode_notify_set_rejects_invalid_other_event() {
    for invalid in [
        "",
        "has space",
        "has\ttab",
        "(parens)",
        "cr\rlf",
        "lf\nonly",
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup::new(
                MailboxFilter::Personal,
                vec![NotifyEvent::Other(invalid.into())],
            )],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "Other(\"{invalid}\") must be rejected as invalid IMAP atom"
        );
    }
}

/// CRLF in `MessageNew` fetch attributes must be rejected to prevent command
/// injection (RFC 3501 Section 2.2).
#[test]
fn encode_notify_set_rejects_crlf_in_fetch_attrs() {
    for payload in ["uid\r\nA002 LOGOUT", "flags\r", "body\n"] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup::new(
                MailboxFilter::Selected,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![payload.into()],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            )],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "fetch-att containing CRLF must be rejected: {payload:?}"
        );
    }
}

/// `MessageNew` fetch attributes with unbalanced delimiters must be rejected
/// (RFC 3501 Section 6.4.5 fetch-att grammar).
#[test]
fn encode_notify_set_rejects_malformed_fetch_attrs() {
    for malformed in [
        "BODY[TEXT",                         // unclosed bracket
        "FLAGS)",                            // unmatched closing paren
        "BODY.PEEK]",                        // unmatched closing bracket
        "BODY.PEEK[HEADER.FIELDS (From To)", // unclosed bracket
        "(UID FLAGS",                        // unclosed paren
        "\"unterminated quote",              // unclosed quote
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup::new(
                MailboxFilter::Selected,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![malformed.into()],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            )],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "malformed fetch-att must be rejected: {malformed:?}"
        );
    }
}

/// Valid `MessageNew` fetch attributes with balanced delimiters must be accepted.
#[test]
fn encode_notify_set_accepts_valid_fetch_attrs() {
    for valid in [
        "UID",
        "FLAGS",
        "ENVELOPE",
        "BODY.PEEK[HEADER.FIELDS (From To Subject)]",
        "BODY[TEXT]<0.100>",
        "BODYSTRUCTURE",
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup::new(
                MailboxFilter::Selected,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![valid.into()],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            )],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_ok(),
            "valid fetch-att must be accepted: {valid:?}, got: {result:?}"
        );
    }
}

/// Non-message events on selected filter must be rejected (RFC 5465 Section 6.1).
#[test]
fn encode_notify_set_rejects_non_message_event_on_selected() {
    for (filter, filter_name) in [
        (MailboxFilter::Selected, "selected"),
        (MailboxFilter::SelectedDelayed, "selected-delayed"),
    ] {
        for non_msg_event in [
            NotifyEvent::MailboxName,
            NotifyEvent::SubscriptionChange,
            NotifyEvent::MailboxMetadataChange,
            NotifyEvent::ServerMetadataChange,
        ] {
            let mut buf = BytesMut::new();
            let cmd = crate::types::Command::NotifySet(NotifySetParams {
                status: false,
                event_groups: vec![NotifyEventGroup::new(
                    filter.clone(),
                    vec![
                        NotifyEvent::MessageNew {
                            fetch_attrs: vec![],
                        },
                        NotifyEvent::MessageExpunge,
                        non_msg_event.clone(),
                    ],
                )],
            });
            let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
            assert!(
                result.is_err(),
                "{filter_name} must reject non-message events (RFC 5465 Section 6.1)"
            );
        }
    }
}

/// `MessageExpunge` without `MessageNew` must be rejected (RFC 5465 Section 5).
#[test]
fn encode_notify_set_rejects_expunge_without_new() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Personal,
            vec![NotifyEvent::MessageExpunge],
        )],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "MessageExpunge without MessageNew must fail (RFC 5465 Section 5)"
    );
}

/// `MessageNew` without `MessageExpunge` must be rejected on selected filters
/// (RFC 5465 Section 5).
#[test]
fn encode_notify_set_rejects_new_without_expunge_on_selected() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Selected,
            vec![NotifyEvent::MessageNew {
                fetch_attrs: vec![],
            }],
        )],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "MessageNew without MessageExpunge must fail on selected filter (RFC 5465 Section 5)"
    );
}

/// RFC 5465 Section 5: "If one of `MessageNew` or `MessageExpunge` is
/// specified, then both events MUST be specified"  -  this applies globally,
/// regardless of mailbox filter.  `MessageNew` alone on a non-selected filter
/// must be rejected.
#[test]
fn encode_notify_set_rejects_new_without_expunge_on_non_selected() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Personal,
            vec![
                NotifyEvent::MailboxName,
                NotifyEvent::SubscriptionChange,
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
            ],
        )],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "MessageNew without MessageExpunge must fail on non-selected filters (RFC 5465 Section 5)"
    );
}

/// Empty fetch-att in `MessageNew` must be rejected (RFC 3501 Section 9).
#[test]
fn encode_notify_set_rejects_empty_fetch_attr() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Selected,
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec!["UID".into(), String::new().into()],
                },
                NotifyEvent::MessageExpunge,
            ],
        )],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty fetch-att string must be rejected (RFC 3501 Section 9)"
    );
}

/// `FlagChange` without `MessageNew`+`MessageExpunge` must be rejected (RFC 5465 Section 5).
#[test]
fn encode_notify_set_rejects_flag_change_without_new_and_expunge() {
    // FlagChange with only MessageNew (missing MessageExpunge)
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Personal,
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::FlagChange,
            ],
        )],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "FlagChange without MessageExpunge must fail (RFC 5465 Section 5)"
    );

    // FlagChange alone
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Personal,
            vec![NotifyEvent::FlagChange],
        )],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "FlagChange alone must fail (RFC 5465 Section 5)"
    );
}

/// `AnnotationChange` without `MessageNew`+`MessageExpunge` must be rejected
/// (RFC 5465 Section 5).
#[test]
fn encode_notify_set_rejects_annotation_change_without_new_and_expunge() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup::new(
            MailboxFilter::Personal,
            vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::AnnotationChange,
            ],
        )],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "AnnotationChange without MessageExpunge must fail (RFC 5465 Section 5)"
    );
}

/// Both `selected` and `selected-delayed` in the same NOTIFY SET must be
/// rejected (RFC 5465 Section 3).
#[test]
fn encode_notify_set_rejects_both_selected_and_selected_delayed() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![
            NotifyEventGroup::new(
                MailboxFilter::Selected,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            ),
            NotifyEventGroup::new(
                MailboxFilter::SelectedDelayed,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            ),
        ],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "selected + selected-delayed must fail (RFC 5465 Section 3)"
    );
}

/// Duplicate `selected` event-groups must be rejected  -  at most one
/// selected/selected-delayed event-group is allowed (RFC 5465 Section 3).
#[test]
fn encode_notify_set_rejects_duplicate_selected() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![
            NotifyEventGroup::new(
                MailboxFilter::Selected,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            ),
            NotifyEventGroup::new(
                MailboxFilter::Selected,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::FlagChange,
                ],
            ),
        ],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "duplicate selected event-groups must fail (RFC 5465 Section 3)"
    );
}

/// Duplicate `selected-delayed` event-groups must be rejected  -  at most one
/// selected/selected-delayed event-group is allowed (RFC 5465 Section 3).
#[test]
fn encode_notify_set_rejects_duplicate_selected_delayed() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![
            NotifyEventGroup::new(
                MailboxFilter::SelectedDelayed,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            ),
            NotifyEventGroup::new(
                MailboxFilter::SelectedDelayed,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::FlagChange,
                ],
            ),
        ],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "duplicate selected-delayed event-groups must fail (RFC 5465 Section 3)"
    );
}

/// `MessageNew` fetch attributes on non-selected filter must be rejected
/// (RFC 5465 Section 8 ABNF).
#[test]
fn encode_notify_set_rejects_fetch_attrs_on_non_selected_filter() {
    for filter in [
        MailboxFilter::Inboxes,
        MailboxFilter::Personal,
        MailboxFilter::Subscribed,
        MailboxFilter::Subtree(vec!["INBOX".into()]),
        MailboxFilter::Mailboxes(vec!["INBOX".into()]),
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup::new(
                filter,
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec!["UID".into()],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            )],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "fetch-attrs on non-selected filter must fail (RFC 5465 Section 8)"
        );
    }
}

/// Extension events (`Other(...)`) must be rejected under `selected` and
/// `selected-delayed` filters because RFC 5465 Section 8 defines `event-ext`
/// as a separate ABNF production from `message-event`, and Section 6.1
/// restricts selected filters to message events only.
#[test]
fn encode_notify_set_rejects_extension_event_on_selected() {
    for (filter, filter_name) in [
        (MailboxFilter::Selected, "selected"),
        (MailboxFilter::SelectedDelayed, "selected-delayed"),
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup::new(
                filter.clone(),
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::Other("X-CUSTOM".into()),
                ],
            )],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "{filter_name} must reject extension events (RFC 5465 Section 6.1, Section 8)"
        );
    }
}
