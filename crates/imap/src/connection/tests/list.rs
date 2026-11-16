use super::*;

#[test]
fn oldname_always_marks_a_notify_list_event() {
    let mut info = mailbox_info(Vec::new());
    info.old_name = Some(MailboxName::new("Old").unwrap());
    assert!(is_notify_list_event(&info, true));
    assert!(
        is_notify_list_event(&info, false),
        "OLDNAME is a marker regardless of LIST-EXTENDED context"
    );
}

#[test]
fn nonexistent_and_noaccess_are_markers_only_outside_subscribed_listings() {
    for attr in [MailboxAttribute::NonExistent, MailboxAttribute::NoAccess] {
        let info = mailbox_info(vec![attr]);
        assert!(is_notify_list_event(&info, true));
        assert!(
            !is_notify_list_event(&info, false),
            "LIST-EXTENDED with SUBSCRIBED may legitimately return these"
        );
    }
}

#[test]
fn a_plain_list_response_is_not_a_notify_event() {
    let info = mailbox_info(vec![MailboxAttribute::HasNoChildren]);
    assert!(!is_notify_list_event(&info, true));
}
