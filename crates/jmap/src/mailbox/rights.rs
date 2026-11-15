// Full RFC 8621 rights vocabulary; readers are used by a subset of fields.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

// One mailbox vocabulary defines shareWith keys, myRights fields and readers.
// Older mail-only servers omit mayShare; absence grants no sharing authority.
macro_rules! mailbox_rights {
    ($( $variant:ident = $number:literal => $field:ident : $wire:literal ),+ $(,)?) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
        #[non_exhaustive]
        pub(crate) enum ACL {
            $(#[serde(rename = $wire)] $variant = $number,)+
        }
        impl ACL {
            pub(crate) const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $wire,)+ }
            }
        }
        impl std::fmt::Display for ACL {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        pub(crate) struct MailboxRights {
            $(#[serde(rename = $wire, default)] pub(super) $field: bool,)+
        }
        impl MailboxRights {
            $(pub(crate) fn $field(&self) -> bool { self.$field })+
            pub(crate) fn acl_list(&self) -> Vec<ACL> {
                [$( (self.$field, ACL::$variant), )+]
                    .into_iter()
                    .filter_map(|(enabled, right)| enabled.then_some(right))
                    .collect()
            }
        }
    };
}

mailbox_rights! {
    ReadItems = 3 => may_read_items: "mayReadItems",
    AddItems = 4 => may_add_items: "mayAddItems",
    RemoveItems = 6 => may_remove_items: "mayRemoveItems",
    SetSeen = 11 => may_set_seen: "maySetSeen",
    SetKeywords = 5 => may_set_keywords: "maySetKeywords",
    CreateChild = 7 => may_create_child: "mayCreateChild",
    Rename = 1 => may_rename: "mayRename",
    Delete = 2 => may_delete: "mayDelete",
    Submit = 10 => may_submit: "maySubmit",
    Administer = 8 => may_share: "mayShare",
}

#[cfg(test)]
mod tests {
    use super::{ACL, MailboxRights};
    use serde_json::json;

    #[test]
    fn sharing_authority_survives_my_rights_decode_and_acl_projection() {
        let rights: MailboxRights = serde_json::from_value(json!({"mayShare": true})).unwrap();
        assert!(rights.may_share());
        assert_eq!(rights.acl_list(), vec![ACL::Administer]);
        assert_eq!(serde_json::to_value(rights).unwrap()["mayShare"], true);

        let legacy: MailboxRights = serde_json::from_value(json!({})).unwrap();
        assert!(!legacy.may_share());
        assert!(legacy.acl_list().is_empty());
    }
}
