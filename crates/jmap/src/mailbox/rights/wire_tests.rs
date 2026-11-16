mod mailbox_acl_vocabulary {
    use crate::mailbox_rights::ACL;
    use serde_json::json;

    #[test]
    fn acl_serialises_with_the_share_with_property_names() {
        for (acl, wire) in [
            (ACL::Rename, "mayRename"),
            (ACL::Delete, "mayDelete"),
            (ACL::ReadItems, "mayReadItems"),
            (ACL::AddItems, "mayAddItems"),
            (ACL::SetKeywords, "maySetKeywords"),
            (ACL::RemoveItems, "mayRemoveItems"),
            (ACL::CreateChild, "mayCreateChild"),
            (ACL::Administer, "mayShare"),
            (ACL::Submit, "maySubmit"),
            (ACL::SetSeen, "maySetSeen"),
        ] {
            assert_eq!(serde_json::to_value(acl).unwrap(), json!(wire));
            assert_eq!(serde_json::from_value::<ACL>(json!(wire)).unwrap(), acl);
        }
    }

    #[test]
    fn acl_display_matches_the_wire_names() {
        assert_eq!(ACL::ReadItems.to_string(), "mayReadItems");
        assert_eq!(ACL::Administer.to_string(), "mayShare");
        for acl in [
            ACL::Rename,
            ACL::Delete,
            ACL::ReadItems,
            ACL::AddItems,
            ACL::SetKeywords,
            ACL::RemoveItems,
            ACL::CreateChild,
            ACL::Administer,
            ACL::Submit,
            ACL::SetSeen,
        ] {
            let wire = serde_json::to_value(acl).unwrap();
            assert_eq!(
                json!(acl.to_string()),
                wire,
                "Display and Serialize must agree for every ACL variant"
            );
        }
    }
}
