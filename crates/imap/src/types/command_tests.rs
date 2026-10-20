#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::StoreOperation;

// --- Authentication commands ---

#[test]
fn command_login() {
    let cmd = Command::Login {
        user: "alice".into(),
        pass: "secret".into(),
    };
    match &cmd {
        Command::Login { user, pass } => {
            assert_eq!(user, "alice");
            assert_eq!(pass.as_str(), "secret");
        }
        _ => panic!("expected Login"),
    }
}

#[test]
fn command_authenticate_plain() {
    let cmd = Command::Authenticate {
        mechanism: "PLAIN".into(),
        initial_response: Some("AGFsaWNlAHNlY3JldA==".into()),
    };
    match &cmd {
        Command::Authenticate {
            mechanism,
            initial_response,
        } => {
            assert_eq!(mechanism, "PLAIN");
            assert!(initial_response.is_some());
        }
        _ => panic!("expected Authenticate"),
    }
}

#[test]
fn command_authenticate_no_sasl_ir() {
    let cmd = Command::Authenticate {
        mechanism: "PLAIN".into(),
        initial_response: None,
    };
    match &cmd {
        Command::Authenticate {
            initial_response, ..
        } => {
            assert!(initial_response.is_none());
        }
        _ => panic!("expected Authenticate"),
    }
}

#[test]
fn command_starttls() {
    let cmd = Command::StartTls;
    assert!(matches!(cmd, Command::StartTls));
}

#[test]
fn command_logout() {
    let cmd = Command::Logout;
    assert!(matches!(cmd, Command::Logout));
}

// --- Mailbox management commands ---

#[test]
fn command_list() {
    let cmd = Command::List {
        reference: String::new().into(),
        pattern: "*".into(),
    };
    match &cmd {
        Command::List { reference, pattern } => {
            assert!(reference.0.as_str().is_empty());
            assert_eq!(pattern.0.as_str(), "*");
        }
        _ => panic!("expected List"),
    }
}

#[test]
fn command_list_extended() {
    let cmd = Command::ListExtended {
        selection_options: vec!["SUBSCRIBED".into(), "RECURSIVEMATCH".into()],
        reference: String::new().into(),
        patterns: vec!["*".into(), "%".into()],
        return_options: vec!["CHILDREN".into(), "STATUS (MESSAGES UNSEEN)".into()],
    };
    match &cmd {
        Command::ListExtended {
            selection_options,
            reference,
            patterns,
            return_options,
        } => {
            assert_eq!(selection_options, &["SUBSCRIBED", "RECURSIVEMATCH"]);
            assert!(reference.0.as_str().is_empty());
            assert_eq!(
                patterns
                    .iter()
                    .map(|name| name.0.as_str())
                    .collect::<Vec<_>>(),
                ["*", "%"]
            );
            assert_eq!(return_options, &["CHILDREN", "STATUS (MESSAGES UNSEEN)"]);
        }
        _ => panic!("expected ListExtended"),
    }
}

#[test]
fn command_list_status() {
    let cmd = Command::ListStatus {
        reference: String::new().into(),
        pattern: "*".into(),
        status_items: "MESSAGES UNSEEN".into(),
    };
    match &cmd {
        Command::ListStatus {
            reference,
            pattern,
            status_items,
        } => {
            assert!(reference.0.as_str().is_empty());
            assert_eq!(pattern.0.as_str(), "*");
            assert_eq!(status_items, "MESSAGES UNSEEN");
        }
        _ => panic!("expected ListStatus"),
    }
}

#[test]
fn command_select() {
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: None,
    };
    match &cmd {
        Command::Select { mailbox, .. } => assert_eq!(mailbox.as_str(), "INBOX"),
        _ => panic!("expected Select"),
    }
}

#[test]
fn command_examine() {
    let cmd = Command::Examine {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: None,
    };
    assert!(matches!(cmd, Command::Examine { .. }));
}

#[test]
fn command_select_qresync() {
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(super::super::QresyncParams {
            uid_validity: 67890,
            mod_seq: 12345,
            known_uids: Some("1:500".into()),
            seq_match_data: None,
        }),
    };
    match &cmd {
        Command::Select {
            mailbox,
            condstore: _,
            qresync,
        } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            let q = qresync.as_ref().unwrap();
            assert_eq!(q.uid_validity, 67890);
            assert_eq!(q.mod_seq, 12345);
            assert_eq!(q.known_uids.as_deref(), Some("1:500"));
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn command_create() {
    let cmd = Command::Create {
        mailbox: MailboxName::new("TestFolder").unwrap(),
    };
    match &cmd {
        Command::Create { mailbox } => assert_eq!(mailbox.as_str(), "TestFolder"),
        _ => panic!("expected Create"),
    }
}

#[test]
fn command_create_special_use() {
    let cmd = Command::CreateSpecialUse {
        mailbox: MailboxName::new("MySent").unwrap(),
        special_use: vec![
            crate::types::MailboxAttribute::Sent,
            crate::types::MailboxAttribute::Important,
        ],
    };
    match &cmd {
        Command::CreateSpecialUse {
            mailbox,
            special_use,
        } => {
            assert_eq!(mailbox.as_str(), "MySent");
            assert_eq!(special_use.len(), 2);
            assert_eq!(special_use[0], crate::types::MailboxAttribute::Sent);
            assert_eq!(special_use[1], crate::types::MailboxAttribute::Important);
        }
        _ => panic!("expected CreateSpecialUse"),
    }
}

#[test]
fn command_delete() {
    let cmd = Command::Delete {
        mailbox: MailboxName::new("OldFolder").unwrap(),
    };
    match &cmd {
        Command::Delete { mailbox } => assert_eq!(mailbox.as_str(), "OldFolder"),
        _ => panic!("expected Delete"),
    }
}

#[test]
fn command_rename() {
    let cmd = Command::Rename {
        mailbox: MailboxName::new("Old").unwrap(),
        new_name: MailboxName::new("New").unwrap(),
    };
    match &cmd {
        Command::Rename { mailbox, new_name } => {
            assert_eq!(mailbox.as_str(), "Old");
            assert_eq!(new_name.as_str(), "New");
        }
        _ => panic!("expected Rename"),
    }
}

#[test]
fn command_subscribe_unsubscribe() {
    let sub = Command::Subscribe {
        mailbox: MailboxName::new("Lists").unwrap(),
    };
    assert!(matches!(sub, Command::Subscribe { .. }));

    let unsub = Command::Unsubscribe {
        mailbox: MailboxName::new("Lists").unwrap(),
    };
    assert!(matches!(unsub, Command::Unsubscribe { .. }));
}

#[test]
fn command_lsub() {
    let cmd = Command::Lsub {
        reference: String::new().into(),
        pattern: "*".into(),
    };
    assert!(matches!(cmd, Command::Lsub { .. }));
}

#[test]
fn command_close_unselect() {
    assert!(matches!(Command::Close, Command::Close));
}

#[test]
fn command_status() {
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "(MESSAGES RECENT UNSEEN)".into(),
    };
    match &cmd {
        Command::Status { mailbox, items } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert!(items.contains("MESSAGES"));
        }
        _ => panic!("expected Status"),
    }
}

#[test]
fn command_namespace() {
    assert!(matches!(Command::Namespace, Command::Namespace));
}

#[test]
fn command_check() {
    assert!(matches!(Command::Check, Command::Check));
}

// --- Message operation commands ---

#[test]
fn command_search() {
    let cmd = Command::Search {
        criteria: "UNSEEN FROM alice".into(),
    };
    match &cmd {
        Command::Search { criteria } => {
            assert_eq!(criteria, "UNSEEN FROM alice");
        }
        _ => panic!("expected Search"),
    }
}

#[test]
fn command_search_save() {
    let cmd = Command::SearchSave {
        criteria: "ALL".into(),
    };
    assert!(matches!(cmd, Command::SearchSave { .. }));
}

#[test]
fn command_search_return() {
    let cmd = Command::SearchReturn {
        criteria: "UNSEEN".into(),
        return_opts: vec!["MIN".into(), "MAX".into(), "COUNT".into()],
    };
    match &cmd {
        Command::SearchReturn {
            criteria,
            return_opts,
        } => {
            assert_eq!(criteria, "UNSEEN");
            assert_eq!(return_opts.len(), 3);
            assert_eq!(return_opts[0], "MIN");
        }
        _ => panic!("expected SearchReturn"),
    }
}

#[test]
fn command_uid_search_return() {
    let cmd = Command::UidSearchReturn {
        criteria: "ALL".into(),
        return_opts: vec!["ALL".into(), "COUNT".into()],
    };
    match &cmd {
        Command::UidSearchReturn {
            criteria,
            return_opts,
        } => {
            assert_eq!(criteria, "ALL");
            assert_eq!(return_opts.len(), 2);
        }
        _ => panic!("expected UidSearchReturn"),
    }
}

#[test]
fn command_fetch() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(UID FLAGS)".into(),
        changed_since: None,
    };
    match &cmd {
        Command::Fetch {
            sequence_set,
            items,
            changed_since,
        } => {
            assert_eq!(sequence_set.as_str(), "1:*");
            assert_eq!(items, "(UID FLAGS)");
            assert!(changed_since.is_none());
        }
        _ => panic!("expected Fetch"),
    }
}

#[test]
fn command_store_add_flags() {
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: StoreOperation::Add,
        flags: vec![Flag::Seen],
        unchanged_since: None,
    };
    match &cmd {
        Command::Store {
            operation,
            flags,
            unchanged_since,
            ..
        } => {
            assert_eq!(*operation, StoreOperation::Add);
            assert_eq!(flags.len(), 1);
            assert!(unchanged_since.is_none());
        }
        _ => panic!("expected Store"),
    }
}

#[test]
fn command_store_with_condstore() {
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: StoreOperation::Replace,
        flags: vec![Flag::Flagged, Flag::Seen],
        unchanged_since: Some(12345),
    };
    match &cmd {
        Command::Store {
            unchanged_since, ..
        } => {
            assert_eq!(*unchanged_since, Some(12345));
        }
        _ => panic!("expected Store"),
    }
}

#[test]
fn command_copy() {
    let cmd = Command::Copy {
        sequence_set: SequenceSet::new("1:10").unwrap(),
        mailbox: MailboxName::new("Archive").unwrap(),
    };
    match &cmd {
        Command::Copy {
            sequence_set,
            mailbox,
        } => {
            assert_eq!(sequence_set.as_str(), "1:10");
            assert_eq!(mailbox.as_str(), "Archive");
        }
        _ => panic!("expected Copy"),
    }
}

// --- UID commands ---

#[test]
fn command_uid_search() {
    let cmd = Command::UidSearch {
        criteria: "ALL".into(),
    };
    assert!(matches!(cmd, Command::UidSearch { .. }));
}

#[test]
fn command_uid_search_save() {
    let cmd = Command::UidSearchSave {
        criteria: "FLAGGED".into(),
    };
    assert!(matches!(cmd, Command::UidSearchSave { .. }));
}

#[test]
fn command_uid_fetch() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("100:200").unwrap(),
        items: "(ENVELOPE)".into(),
        changed_since: None,
        vanished: false,
    };
    assert!(matches!(cmd, Command::UidFetch { .. }));
}

#[test]
fn command_uid_store() {
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("500").unwrap(),
        operation: StoreOperation::Remove,
        flags: vec![Flag::Deleted],
        unchanged_since: None,
    };
    match &cmd {
        Command::UidStore {
            operation, flags, ..
        } => {
            assert_eq!(*operation, StoreOperation::Remove);
            assert_eq!(flags[0], Flag::Deleted);
        }
        _ => panic!("expected UidStore"),
    }
}

#[test]
fn command_uid_move() {
    let cmd = Command::UidMove {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        mailbox: MailboxName::new("Trash").unwrap(),
    };
    match &cmd {
        Command::UidMove {
            sequence_set,
            mailbox,
        } => {
            assert_eq!(sequence_set.as_str(), "1:*");
            assert_eq!(mailbox.as_str(), "Trash");
        }
        _ => panic!("expected UidMove"),
    }
}

#[test]
fn command_uid_copy() {
    let cmd = Command::UidCopy {
        sequence_set: SequenceSet::new("1").unwrap(),
        mailbox: MailboxName::new("Backup").unwrap(),
    };
    assert!(matches!(cmd, Command::UidCopy { .. }));
}

#[test]
fn command_uid_expunge() {
    let cmd = Command::UidExpunge {
        sequence_set: SequenceSet::new("1:5").unwrap(),
    };
    match &cmd {
        Command::UidExpunge { sequence_set } => {
            assert_eq!(sequence_set.as_str(), "1:5");
        }
        _ => panic!("expected UidExpunge"),
    }
}

#[test]
fn command_expunge() {
    assert!(matches!(Command::Expunge, Command::Expunge));
}

// --- IDLE commands ---

#[test]
fn command_idle_done() {
    assert!(matches!(Command::Idle, Command::Idle));
}

// --- Misc commands ---

#[test]
fn command_capability() {
    assert!(matches!(Command::Capability, Command::Capability));
}

#[test]
fn command_noop() {
    assert!(matches!(Command::Noop, Command::Noop));
}

#[test]
fn command_id() {
    let cmd = Command::Id(vec![
        ("name".into(), Some("MyClient".into())),
        ("version".into(), Some("1.0".into())),
    ]);
    match &cmd {
        Command::Id(pairs) => {
            assert_eq!(pairs.len(), 2);
            assert_eq!(pairs[0].0, "name");
            assert_eq!(pairs[0].1, Some("MyClient".into()));
        }
        _ => panic!("expected Id"),
    }
}

// --- METADATA commands ---

#[test]
fn command_get_metadata() {
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/comment".into()],
        max_size: None,
        depth: None,
    };
    match &cmd {
        Command::GetMetadata {
            mailbox, entries, ..
        } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(entries.len(), 1);
        }
        _ => panic!("expected GetMetadata"),
    }
}

#[test]
fn command_set_metadata() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![
            ("/private/comment".into(), Some(b"note".to_vec())),
            ("/private/old".into(), None),
        ],
    };
    match &cmd {
        Command::SetMetadata { mailbox, entries } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].1.as_deref(), Some(b"note".as_slice()));
            assert!(entries[1].1.is_none()); // deletion
        }
        _ => panic!("expected SetMetadata"),
    }
}

// --- THREAD commands ---

#[test]
fn command_thread() {
    let cmd = Command::Thread {
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    match &cmd {
        Command::Thread {
            algorithm,
            charset,
            criteria,
        } => {
            assert_eq!(algorithm, "REFERENCES");
            assert_eq!(charset, "UTF-8");
            assert_eq!(criteria, "ALL");
        }
        _ => panic!("expected Thread"),
    }
}

#[test]
fn command_uid_thread() {
    let cmd = Command::UidThread {
        algorithm: "ORDEREDSUBJECT".into(),
        charset: "US-ASCII".into(),
        criteria: "SINCE 1-Jan-2024".into(),
    };
    assert!(matches!(cmd, Command::UidThread { .. }));
}

// --- COMPRESS command ---

#[test]
fn command_compress() {
    assert!(matches!(Command::Compress, Command::Compress));
}

// --- QUOTA commands ---

#[test]
fn command_get_quota() {
    let cmd = Command::GetQuota {
        root: String::new(),
    };
    match &cmd {
        Command::GetQuota { root } => assert!(root.is_empty()),
        _ => panic!("expected GetQuota"),
    }
}

#[test]
fn command_get_quota_root() {
    let cmd = Command::GetQuotaRoot {
        mailbox: MailboxName::new("INBOX").unwrap(),
    };
    match &cmd {
        Command::GetQuotaRoot { mailbox } => assert_eq!(mailbox.as_str(), "INBOX"),
        _ => panic!("expected GetQuotaRoot"),
    }
}

// --- SETQUOTA command (RFC 2087 Section 4.1) ---

#[test]
fn command_set_quota() {
    let cmd = Command::SetQuota {
        root: "user.alice".into(),
        resources: vec![("STORAGE".into(), 51200), ("MESSAGE".into(), 1000)],
    };
    match &cmd {
        Command::SetQuota { root, resources } => {
            assert_eq!(root, "user.alice");
            assert_eq!(resources.len(), 2);
            assert_eq!(resources[0].0, "STORAGE");
            assert_eq!(resources[0].1, 51200);
            assert_eq!(resources[1].0, "MESSAGE");
            assert_eq!(resources[1].1, 1000);
        }
        _ => panic!("expected SetQuota"),
    }
}

#[test]
fn command_set_quota_empty_root() {
    let cmd = Command::SetQuota {
        root: String::new(),
        resources: vec![("STORAGE".into(), 0)],
    };
    match &cmd {
        Command::SetQuota { root, resources } => {
            assert!(root.is_empty());
            assert_eq!(resources.len(), 1);
        }
        _ => panic!("expected SetQuota"),
    }
}

// L14: FETCH with CHANGEDSINCE modifier (RFC 7162 Section 3.1.4.1)

#[test]
fn command_fetch_changedsince() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(12345),
    };
    match &cmd {
        Command::Fetch { changed_since, .. } => {
            assert_eq!(*changed_since, Some(12345));
        }
        _ => panic!("expected Fetch"),
    }
}

#[test]
fn command_uid_fetch_changedsince() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(UID FLAGS)".into(),
        changed_since: Some(99999),
        vanished: false,
    };
    match &cmd {
        Command::UidFetch { changed_since, .. } => {
            assert_eq!(*changed_since, Some(99999));
        }
        _ => panic!("expected UidFetch"),
    }
}

// RFC 7162 Section 3.2.6: UID FETCH with VANISHED modifier

#[test]
fn command_uid_fetch_vanished() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(12345),
        vanished: true,
    };
    match &cmd {
        Command::UidFetch {
            changed_since,
            vanished,
            ..
        } => {
            assert_eq!(*changed_since, Some(12345));
            assert!(*vanished);
        }
        _ => panic!("expected UidFetch"),
    }
}

// --- ACL commands ---

#[test]
fn command_set_acl() {
    let cmd = Command::SetAcl {
        mailbox: MailboxName::new("INBOX").unwrap(),
        identifier: "bob".into(),
        rights: "lrs".into(),
    };
    match &cmd {
        Command::SetAcl {
            mailbox,
            identifier,
            rights,
        } => {
            assert_eq!(mailbox.as_str(), "INBOX");
            assert_eq!(identifier, "bob");
            assert_eq!(rights, "lrs");
        }
        _ => panic!("expected SetAcl"),
    }
}

#[test]
fn command_delete_acl() {
    let cmd = Command::DeleteAcl {
        mailbox: MailboxName::new("INBOX").unwrap(),
        identifier: "bob".into(),
    };
    assert!(matches!(cmd, Command::DeleteAcl { .. }));
}

#[test]
fn command_get_acl() {
    let cmd = Command::GetAcl {
        mailbox: MailboxName::new("INBOX").unwrap(),
    };
    assert!(matches!(cmd, Command::GetAcl { .. }));
}

#[test]
fn command_list_rights() {
    let cmd = Command::ListRights {
        mailbox: MailboxName::new("INBOX").unwrap(),
        identifier: "alice".into(),
    };
    assert!(matches!(cmd, Command::ListRights { .. }));
}

#[test]
fn command_my_rights() {
    let cmd = Command::MyRights {
        mailbox: MailboxName::new("INBOX").unwrap(),
    };
    match &cmd {
        Command::MyRights { mailbox } => assert_eq!(mailbox.as_str(), "INBOX"),
        _ => panic!("expected MyRights"),
    }
}

// --- UNAUTHENTICATE (RFC 8437) ---

#[test]
fn command_unauthenticate_kind() {
    assert_eq!(Command::Unauthenticate.kind(), CommandKind::Unauthenticate);
}

#[test]
fn command_unauthenticate_mailbox_target_is_none() {
    assert!(Command::Unauthenticate.mailbox_target().is_none());
}

// --- Clone / Debug ---

#[test]
fn command_clone() {
    let cmd = Command::Login {
        user: "alice".into(),
        pass: "secret".into(),
    };
    let cloned = cmd.clone();
    // Verify both the original and clone are the expected variant.
    assert!(matches!(cmd, Command::Login { .. }));
    assert!(matches!(cloned, Command::Login { .. }));
}

#[test]
fn command_debug() {
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: None,
    };
    let debug = format!("{cmd:?}");
    assert!(debug.contains("Select"));
    assert!(debug.contains("INBOX"));
}

// --- APPEND ---

#[test]
fn command_append_kind_and_target() {
    let cmd = Command::Append {
        mailbox: "Drafts".into(),
        messages: vec![super::super::AppendMessage::new(b"hi".to_vec())],
        multi: false,
    };
    assert_eq!(cmd.kind(), CommandKind::Append);
    assert!(cmd.mailbox_target().is_none());
}

/// The driver traces every command with `?cmd`; an APPEND's Debug must name
/// the body's length and never its content.
#[test]
fn command_append_debug_never_prints_the_body() {
    let cmd = Command::Append {
        mailbox: "Drafts".into(),
        messages: vec![super::super::AppendMessage::new(
            b"Subject: secret plans".to_vec(),
        )],
        multi: false,
    };
    let debug = format!("{cmd:?}");
    assert!(!debug.contains("secret plans"), "{debug}");
    assert!(debug.contains("data_len: 21"), "{debug}");
}

// --- Session legality and pipeline admission ---

/// Spot rows of the one legality table against RFC 3501 Section 6 and the
/// extension sections, including the rows that are easy to get wrong.
#[test]
fn legal_states_follow_the_rfc_rows() {
    use crate::connection::SessionState::{Authenticated, NotAuthenticated, Selected};

    assert_eq!(
        CommandKind::Noop.legal_states(),
        &[NotAuthenticated, Authenticated, Selected]
    );
    assert_eq!(
        CommandKind::Id.legal_states(),
        &[NotAuthenticated, Authenticated, Selected]
    );
    assert_eq!(CommandKind::Login.legal_states(), &[NotAuthenticated]);
    assert_eq!(CommandKind::StartTls.legal_states(), &[NotAuthenticated]);
    // RFC 5161 Section 3.1: ENABLE is Authenticated only.
    assert_eq!(CommandKind::Enable.legal_states(), &[Authenticated]);
    assert_eq!(
        CommandKind::Append.legal_states(),
        &[Authenticated, Selected]
    );
    // IDLE is an Authenticated-state command too, not Selected-only.
    assert_eq!(CommandKind::Idle.legal_states(), &[Authenticated, Selected]);
    assert_eq!(CommandKind::Fetch.legal_states(), &[Selected]);
    assert_eq!(CommandKind::Close.legal_states(), &[Selected]);
}

/// No command is legal in Logout, and every command is legal somewhere.
#[test]
fn no_command_is_legal_in_logout() {
    use crate::connection::SessionState;

    let every_kind = every_command_kind();
    for kind in every_kind {
        let legal = kind.legal_states();
        assert!(!legal.is_empty(), "{kind:?} has no legal state");
        assert!(
            !legal.contains(&SessionState::Logout),
            "{kind:?} is legal in Logout"
        );
    }
}

/// The batch excludes exactly the commands that change session state, swap
/// the transport, take continuations, or need preparation of their own.
#[test]
fn pipelinable_excludes_state_changers_and_append() {
    for kind in [
        CommandKind::Select,
        CommandKind::Examine,
        CommandKind::Close,
        CommandKind::Unselect,
        CommandKind::Login,
        CommandKind::Authenticate,
        CommandKind::Logout,
        CommandKind::Unauthenticate,
        CommandKind::StartTls,
        CommandKind::Compress,
        CommandKind::Idle,
        CommandKind::Enable,
        CommandKind::Append,
    ] {
        assert!(!kind.pipelinable(), "{kind:?} must not be pipelinable");
    }
    for kind in [
        CommandKind::Noop,
        CommandKind::Fetch,
        CommandKind::Store,
        CommandKind::Search,
        CommandKind::Status,
        CommandKind::NotifySet,
        CommandKind::MyRights,
    ] {
        assert!(kind.pipelinable(), "{kind:?} must be pipelinable");
    }
}

/// Every `CommandKind`, by an exhaustive match: a new kind does not compile
/// until it is listed here and so reaches the table checks above.
fn every_command_kind() -> Vec<CommandKind> {
    use CommandKind as K;
    let all = vec![
        K::Capability,
        K::Noop,
        K::Logout,
        K::Login,
        K::Authenticate,
        K::StartTls,
        K::Enable,
        K::Select,
        K::Examine,
        K::Create,
        K::Delete,
        K::Rename,
        K::Subscribe,
        K::Unsubscribe,
        K::List,
        K::ListStatus,
        K::Lsub,
        K::Status,
        K::Namespace,
        K::Append,
        K::Check,
        K::Close,
        K::Unselect,
        K::Expunge,
        K::Search,
        K::SearchReturn,
        K::SearchSave,
        K::Fetch,
        K::Store,
        K::Copy,
        K::Move,
        K::Idle,
        K::Id,
        K::GetMetadata,
        K::SetMetadata,
        K::Thread,
        K::Sort,
        K::NotifySet,
        K::NotifyNone,
        K::Compress,
        K::Unauthenticate,
        K::GetQuota,
        K::GetQuotaRoot,
        K::SetQuota,
        K::SetAcl,
        K::DeleteAcl,
        K::GetAcl,
        K::ListRights,
        K::MyRights,
    ];
    for kind in &all {
        // Exhaustiveness guard: adding a variant breaks this match.
        match kind {
            K::Capability
            | K::Noop
            | K::Logout
            | K::Login
            | K::Authenticate
            | K::StartTls
            | K::Enable
            | K::Select
            | K::Examine
            | K::Create
            | K::Delete
            | K::Rename
            | K::Subscribe
            | K::Unsubscribe
            | K::List
            | K::ListStatus
            | K::Lsub
            | K::Status
            | K::Namespace
            | K::Append
            | K::Check
            | K::Close
            | K::Unselect
            | K::Expunge
            | K::Search
            | K::SearchReturn
            | K::SearchSave
            | K::Fetch
            | K::Store
            | K::Copy
            | K::Move
            | K::Idle
            | K::Id
            | K::GetMetadata
            | K::SetMetadata
            | K::Thread
            | K::Sort
            | K::NotifySet
            | K::NotifyNone
            | K::Compress
            | K::Unauthenticate
            | K::GetQuota
            | K::GetQuotaRoot
            | K::SetQuota
            | K::SetAcl
            | K::DeleteAcl
            | K::GetAcl
            | K::ListRights
            | K::MyRights => {}
        }
    }
    all
}
