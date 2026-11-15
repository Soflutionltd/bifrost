use super::*;
use bifrost_types::AccountErrorKind;

// The PIM primitives answer for the whole request, so they have nowhere
// to report a per-id exclusion. Sending the narrowed operand anyway
// would return `Ok(())` for messages the server was never asked about.
#[test]
fn a_pim_operand_refuses_a_uid_it_cannot_carry_instead_of_narrowing() {
    let Err(refused) = super::pim_operand([4u32, 0, 6], AccountOperation::SearchMessages) else {
        panic!("a dropped UID must be refused, not silently narrowed");
    };
    assert!(matches!(
        refused.kind(),
        AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
    ));

    let operand =
        super::pim_operand([4u32, 6], AccountOperation::SearchMessages).expect("carryable UIDs");
    assert_eq!(operand.uids(), &[4, 6]);
    assert_eq!(
        operand
            .uid_set()
            .expect("non-empty operand")
            .as_sequence_set()
            .as_str(),
        "4,6"
    );

    // An empty group is not an exclusion: nothing was asked for, so
    // there is no command to send and nothing to refuse.
    let empty = super::pim_operand([], AccountOperation::SearchMessages)
        .expect("an empty request is not malformed");
    assert!(empty.uid_set().is_none());
}

#[test]
fn native_thread_folders_cover_the_conversation_once() {
    use super::super::envelope::NativeThreadKind;
    use crate::types::MailboxAttribute;
    let entry = |name: &str, attributes: Vec<MailboxAttribute>| {
        std::sync::Arc::new(super::super::folder_registry::FolderEntry::from_mailbox(
            crate::types::MailboxInfo {
                name: MailboxName::new(name).unwrap(),
                attributes,
                ..Default::default()
            },
        ))
    };
    let entries = || {
        vec![
            entry("Sent", Vec::new()),
            entry("INBOX", Vec::new()),
            entry("[Gmail]/All Mail", vec![MailboxAttribute::All]),
            entry("[Gmail]", vec![MailboxAttribute::NoSelect]),
        ]
    };
    let names = |folders: Vec<MailboxName>| {
        folders
            .iter()
            .map(|f| f.as_str().to_owned())
            .collect::<Vec<_>>()
    };
    // Gmail: one search in \All, not one per label.
    assert_eq!(
        names(native_thread_folders(
            entries(),
            NativeThreadKind::Gmail,
            ThreadScope::Account
        )),
        ["[Gmail]/All Mail"]
    );
    // OBJECTID: every selectable folder, never a \Noselect parent.
    let all = names(native_thread_folders(
        entries(),
        NativeThreadKind::ObjectId,
        ThreadScope::Account,
    ));
    assert_eq!(all.len(), 3);
    assert!(!all.iter().any(|name| name == "[Gmail]"));
    let inbox = MailboxName::new("INBOX").unwrap();
    let except = names(native_thread_folders(
        entries(),
        NativeThreadKind::ObjectId,
        ThreadScope::Except(&inbox),
    ));
    assert!(!except.iter().any(|name| name == "INBOX"));
    assert_eq!(except.len(), 2);
    assert_eq!(
        names(native_thread_folders(
            entries(),
            NativeThreadKind::Gmail,
            ThreadScope::Only(&inbox)
        )),
        ["INBOX"]
    );
    // Gmail without a listed \All falls back to every selectable folder.
    let no_all = vec![entry("INBOX", Vec::new()), entry("Sent", Vec::new())];
    assert_eq!(
        native_thread_folders(no_all, NativeThreadKind::Gmail, ThreadScope::Account).len(),
        2
    );
}
