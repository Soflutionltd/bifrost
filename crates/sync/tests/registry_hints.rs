//! CursorRegistry membership-index and push-hint routing tests.
//!
//! The push reconciler's targeting depends on two pure pieces the
//! suite did not previously pin: `scopes_for_hint` (hint -> affected
//! cursor scopes) and the registry's membership index maintenance
//! (`link_membership` dedupe, `delete` pruning). Also pins
//! `membership_to_cursor_scopes`, the lifecycle-event mapping the
//! multiplexer uses to synthesize `RestartScope` recoveries.

use bifrost_sync::CursorRegistry;
use bifrost_sync::cursor::membership::membership_to_cursor_scopes;
use bifrost_sync::push::scopes_for_hint;
use bifrost_types::{
    ChangeCursor, CursorScope, FolderId, HintPayload, LabelId, MailboxId, MembershipScope,
    ObjectType, OpaqueChangeState, ProtocolKind, QueryId,
};

fn cursor(scope: &CursorScope) -> ChangeCursor {
    ChangeCursor {
        scope: scope.clone(),
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: bifrost_types::PayloadVersion::new(1),
            bytes: b"state".to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
    }
}

#[test]
fn specific_cursor_scope_hint_routes_to_exactly_that_scope() {
    let registry = CursorRegistry::new();
    registry.put(cursor(&CursorScope::Type(ObjectType::Email)));
    registry.put(cursor(&CursorScope::Type(ObjectType::Mailbox)));

    let hint = HintPayload::SpecificCursorScope(CursorScope::Type(ObjectType::Email));
    let scopes = scopes_for_hint(&registry, &hint);
    assert_eq!(scopes, vec![CursorScope::Type(ObjectType::Email)]);
}

#[test]
fn specific_cursor_scope_hint_is_returned_even_when_unregistered() {
    // The hint arm does not consult the registry; the reconciler's
    // snapshot() check is what drops unknown scopes later. Pinned so a
    // change to filter here is deliberate.
    let registry = CursorRegistry::new();
    let hint = HintPayload::SpecificCursorScope(CursorScope::Account);
    assert_eq!(
        scopes_for_hint(&registry, &hint),
        vec![CursorScope::Account]
    );
}

#[test]
fn membership_hint_routes_through_the_side_index() {
    let registry = CursorRegistry::new();
    let email_scope = CursorScope::Type(ObjectType::Email);
    registry.put(cursor(&email_scope));
    let inbox = MembershipScope::Mailbox(MailboxId("inbox".into()));
    registry.link_membership(inbox.clone(), email_scope.clone());

    let scopes = scopes_for_hint(&registry, &HintPayload::SpecificMembership(inbox));
    assert_eq!(scopes, vec![email_scope]);

    // An unlinked membership resolves to nothing (NOT a full fan-out).
    let other = MembershipScope::Mailbox(MailboxId("archive".into()));
    assert!(scopes_for_hint(&registry, &HintPayload::SpecificMembership(other)).is_empty());
}

#[test]
fn unknown_hint_fans_out_to_every_registered_scope() {
    let registry = CursorRegistry::new();
    registry.put(cursor(&CursorScope::Folder(FolderId("INBOX".into()))));
    registry.put(cursor(&CursorScope::Folder(FolderId("Sent".into()))));

    let mut scopes = scopes_for_hint(&registry, &HintPayload::Unknown);
    scopes.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(
        scopes,
        vec![
            CursorScope::Folder(FolderId("INBOX".into())),
            CursorScope::Folder(FolderId("Sent".into())),
        ]
    );
}

#[test]
fn link_membership_deduplicates_edges() {
    let registry = CursorRegistry::new();
    let scope = CursorScope::Folder(FolderId("INBOX".into()));
    let membership = MembershipScope::Folder(FolderId("INBOX".into()));
    registry.link_membership(membership.clone(), scope.clone());
    registry.link_membership(membership.clone(), scope.clone());
    registry.link_membership(membership.clone(), scope.clone());
    assert_eq!(registry.scopes_for_membership(&membership), vec![scope]);
}

#[test]
fn delete_prunes_only_the_deleted_scope_from_shared_memberships() {
    // One membership covered by two scopes (a folder cursor and a
    // query cursor); deleting one scope leaves the other edge intact.
    let registry = CursorRegistry::new();
    let folder_scope = CursorScope::Folder(FolderId("INBOX".into()));
    let query_scope = CursorScope::Query(QueryId("unread".into()));
    registry.put(cursor(&folder_scope));
    registry.put(cursor(&query_scope));
    let membership = MembershipScope::Folder(FolderId("INBOX".into()));
    registry.link_membership(membership.clone(), folder_scope.clone());
    registry.link_membership(membership.clone(), query_scope.clone());

    registry.delete(&folder_scope);

    assert!(registry.snapshot(&folder_scope).is_none());
    assert!(registry.snapshot(&query_scope).is_some());
    assert_eq!(
        registry.scopes_for_membership(&membership),
        vec![query_scope]
    );
}

#[test]
fn put_replaces_the_cursor_for_an_existing_scope() {
    let registry = CursorRegistry::new();
    let scope = CursorScope::Account;
    registry.put(cursor(&scope));
    let mut advanced = cursor(&scope);
    advanced.server_state.bytes = b"state-2".to_vec();
    registry.put(advanced);
    let got = registry.snapshot(&scope).expect("cursor present");
    assert_eq!(got.server_state.bytes, b"state-2".to_vec());
    assert_eq!(registry.all_scopes().len(), 1);
}

#[test]
fn membership_to_cursor_scope_mapping_respects_registered_shapes() {
    let folder_registry = CursorRegistry::new();
    folder_registry.put(cursor(&CursorScope::Folder(FolderId("existing".into()))));
    // Folder membership -> per-folder cursor scope.
    assert_eq!(
        membership_to_cursor_scopes(
            &folder_registry,
            &MembershipScope::Folder(FolderId("F".into()))
        ),
        vec![CursorScope::Folder(FolderId("F".into()))]
    );
    assert_eq!(
        membership_to_cursor_scopes(
            &folder_registry,
            &MembershipScope::Mailbox(MailboxId("mb-1".into()))
        ),
        vec![CursorScope::Folder(FolderId("mb-1".into()))]
    );

    let query_registry = CursorRegistry::new();
    query_registry.put(cursor(&CursorScope::Query(QueryId("existing".into()))));
    // Query membership -> query cursor scope.
    assert_eq!(
        membership_to_cursor_scopes(
            &query_registry,
            &MembershipScope::Query(QueryId("q".into()))
        ),
        vec![CursorScope::Query(QueryId("q".into()))]
    );

    let type_registry = CursorRegistry::new();
    type_registry.put(cursor(&CursorScope::Type(ObjectType::Email)));
    assert_eq!(
        membership_to_cursor_scopes(
            &type_registry,
            &MembershipScope::Mailbox(MailboxId("mb-1".into()))
        ),
        Vec::<CursorScope>::new(),
        "type-wide cursors already cover new mailboxes"
    );
    assert_eq!(
        membership_to_cursor_scopes(
            &type_registry,
            &MembershipScope::Label(LabelId("STARRED".into()))
        ),
        Vec::<CursorScope>::new()
    );
}

#[test]
fn membership_index_bridges_container_spellings_and_keeps_same_named_owners_separate() {
    let registry = CursorRegistry::new();
    let scope = CursorScope::Folder(FolderId("a".into()));
    let mailbox = MembershipScope::Mailbox(MailboxId("a".into()));
    let folder = MembershipScope::Folder(FolderId("a".into()));
    let owner = MembershipScope::Owner(bifrost_types::PrincipalId("a".into()));
    registry.put(cursor(&scope));
    registry.link_membership(mailbox.clone(), scope.clone());
    assert_eq!(registry.scopes_for_membership(&folder), vec![scope.clone()]);
    registry.link_membership(folder.clone(), scope.clone());
    assert_eq!(registry.scopes_for_membership(&folder), vec![scope.clone()]);
    assert_eq!(registry.scopes_for_membership(&mailbox), vec![scope]);
    assert!(registry.scopes_for_membership(&owner).is_empty());
    assert!(membership_to_cursor_scopes(&registry, &owner).is_empty());
    registry.delete(&CursorScope::Folder(FolderId("a".into())));
    assert!(registry.scopes_for_membership(&folder).is_empty());
}
