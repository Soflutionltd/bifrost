use super::*;
use crate::account::folder_registry::role_folder_from_entries;
use crate::types::{MailboxAttribute, MailboxInfo};

#[tokio::test]
async fn host_attachment_unsupported() {
    // IMAP has no cloud-drive hosting; the leg returns
    // `Unsupported(HostAttachment)`.
    let err = unsupported_hosted(AccountOperation::HostAttachment)
        .await
        .expect_err("imap host_attachment is unsupported");
    assert_eq!(
        err.kind(),
        &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::HostAttachment)
    );
}

fn role_entry(
    name: &str,
    delimiter: Option<char>,
    attributes: Vec<MailboxAttribute>,
) -> std::sync::Arc<crate::account::folder_registry::FolderEntry> {
    role_entry_with_name(
        MailboxName::new(name).expect("valid mailbox"),
        delimiter,
        attributes,
    )
}

fn role_entry_with_name(
    name: MailboxName,
    delimiter: Option<char>,
    attributes: Vec<MailboxAttribute>,
) -> std::sync::Arc<crate::account::folder_registry::FolderEntry> {
    std::sync::Arc::new(crate::account::folder_registry::FolderEntry::from_mailbox(
        MailboxInfo {
            name,
            delimiter,
            attributes,
            ..Default::default()
        },
    ))
}

#[test]
fn role_and_quota_folder_choices_are_deterministic() {
    let entries = vec![
        role_entry("Z/Sent", Some('/'), Vec::new()),
        role_entry("A/Sent", Some('/'), Vec::new()),
        role_entry("Special", Some('/'), vec![MailboxAttribute::Sent]),
    ];
    assert_eq!(
        role_folder_from_entries(entries, FolderRole::Sent)
            .as_ref()
            .map(MailboxName::as_str),
        Some("Special"),
        "SPECIAL-USE wins over lexical name fallback",
    );

    let entries = vec![
        role_entry("Zeta", Some('/'), Vec::new()),
        role_entry("Hidden", Some('/'), vec![MailboxAttribute::NoSelect]),
        role_entry("Archive", Some('/'), Vec::new()),
    ];
    assert_eq!(
        first_selectable_folder(entries)
            .as_ref()
            .map(MailboxName::as_str),
        Some("Archive"),
    );
}

#[test]
fn quota_fallback_uses_only_personal_selectable_folders() {
    let mut shared = role_entry("A-shared", Some('/'), Vec::new());
    std::sync::Arc::get_mut(&mut shared).unwrap().shared_owner =
        Some(bifrost_types::PrincipalId("colleague".into()));
    assert!(first_selectable_folder(vec![std::sync::Arc::clone(&shared)]).is_none());
    assert_eq!(
        first_selectable_folder(vec![
            shared,
            role_entry("Z-personal", Some('/'), Vec::new()),
        ])
        .unwrap()
        .as_str(),
        "Z-personal",
    );
}

#[test]
fn role_folder_ties_use_wire_octets_and_mode_after_display_name() {
    let canonical = MailboxName::new("a").expect("valid mailbox");
    let noncanonical = MailboxName::from_wire("a".into(), b"&AGE-", false);
    for names in [
        [canonical.clone(), noncanonical.clone()],
        [noncanonical.clone(), canonical.clone()],
    ] {
        let selected = role_folder_from_entries(
            names
                .into_iter()
                .map(|name| role_entry_with_name(name, Some('/'), vec![MailboxAttribute::Sent]))
                .collect(),
            FolderRole::Sent,
        )
        .expect("a Sent folder");
        assert_eq!(selected, noncanonical);
    }

    // The same malformed octets can decode to the same replacement
    // character in either mode. The mode is the final identity tie-break.
    let legacy = MailboxName::from_wire("\u{fffd}".into(), b"\xff", false);
    let utf8 = MailboxName::from_wire("\u{fffd}".into(), b"\xff", true);
    for names in [
        [utf8.clone(), legacy.clone()],
        [legacy.clone(), utf8.clone()],
    ] {
        let selected = role_folder_from_entries(
            names
                .into_iter()
                .map(|name| role_entry_with_name(name, Some('/'), vec![MailboxAttribute::Sent]))
                .collect(),
            FolderRole::Sent,
        )
        .expect("a Sent folder");
        assert_eq!(selected, legacy);
    }
}

#[test]
fn quota_fallback_ties_use_wire_identity_after_display_name() {
    let canonical = MailboxName::new("a").expect("valid mailbox");
    let noncanonical = MailboxName::from_wire("a".into(), b"&AGE-", false);
    for names in [
        [canonical.clone(), noncanonical.clone()],
        [noncanonical.clone(), canonical.clone()],
    ] {
        let selected = first_selectable_folder(
            names
                .into_iter()
                .map(|name| role_entry_with_name(name, Some('/'), Vec::new()))
                .collect(),
        )
        .expect("a selectable folder");
        assert_eq!(selected, noncanonical);
    }
}
