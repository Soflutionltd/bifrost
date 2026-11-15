use super::{ACLPatch, MailboxCreate, MailboxId, MailboxPatch, Role, SetArguments};
use crate::core::creation::{CreationId, IdOrRef};
use crate::core::field::Field;
use crate::core::patch::{self, PatchPath};
use crate::mailbox::ACL;
use std::collections::HashMap;

impl MailboxCreate {
    pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn parent_id(&mut self, parent_id: Option<impl Into<MailboxId>>) -> &mut Self {
        self.parent_id = Field::from(parent_id.map(|id| IdOrRef::Id(id.into())));
        self
    }

    pub(crate) fn parent_id_ref(&mut self, parent_id_ref: &str) -> &mut Self {
        self.parent_id = Field::from(Some(IdOrRef::Reference(
            CreationId::named(parent_id_ref).reference(),
        )));
        self
    }

    pub(crate) fn role(&mut self, role: Option<Role>) -> &mut Self {
        self.role = Field::from(role);
        self
    }

    pub(crate) fn sort_order(&mut self, sort_order: u32) -> &mut Self {
        self.sort_order = sort_order.into();
        self
    }

    pub(crate) fn is_subscribed(&mut self, is_subscribed: bool) -> &mut Self {
        self.is_subscribed = is_subscribed.into();
        self
    }

    pub(crate) fn acls<T, U, V>(&mut self, acls: T) -> &mut Self
    where
        T: IntoIterator<Item = (U, V)>,
        U: Into<String>,
        V: IntoIterator<Item = ACL>,
    {
        self.share_with = Field::from(Some(
            acls.into_iter()
                .map(|(id, acls)| (id.into(), acls.into_iter().map(|acl| (acl, true)).collect()))
                .collect(),
        ));
        self
    }
}

impl MailboxPatch {
    pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn parent_id(&mut self, parent_id: Option<impl Into<MailboxId>>) -> &mut Self {
        self.parent_id = match parent_id {
            Some(parent_id) => Field::Value(IdOrRef::Id(parent_id.into())),
            None => Field::Null,
        };
        self
    }

    pub(crate) fn parent_id_ref(&mut self, parent_id_ref: &str) -> &mut Self {
        self.parent_id = Field::Value(IdOrRef::Reference(
            CreationId::named(parent_id_ref).reference(),
        ));
        self
    }

    pub(crate) fn role(&mut self, role: Option<Role>) -> &mut Self {
        self.role = Field::from(role);
        self
    }

    pub(crate) fn sort_order(&mut self, sort_order: u32) -> &mut Self {
        self.sort_order = sort_order.into();
        self
    }

    pub(crate) fn is_subscribed(&mut self, is_subscribed: bool) -> &mut Self {
        self.is_subscribed = is_subscribed.into();
        self
    }

    pub(crate) fn acls<T, U, V>(&mut self, acls: T) -> &mut Self
    where
        T: IntoIterator<Item = (U, V)>,
        U: Into<String>,
        V: IntoIterator<Item = ACL>,
    {
        self.acl_patch = None;
        self.share_with = Field::from(Some(
            acls.into_iter()
                .map(|(id, acls)| (id.into(), acls.into_iter().map(|acl| (acl, true)).collect()))
                .collect(),
        ));
        self
    }

    pub(crate) fn acl(&mut self, id: &str, acl: impl IntoIterator<Item = ACL>) -> &mut Self {
        self.share_with = Field::Omitted;
        patch::insert(
            self.acl_patch.get_or_insert_with(HashMap::new),
            PatchPath::new(["shareWith", id]),
            ACLPatch::Replace(acl.into_iter().map(|acl| (acl, true)).collect()),
        );
        self
    }

    pub(crate) fn acl_set(&mut self, id: &str, acl: ACL, set: bool) -> &mut Self {
        self.share_with = Field::Omitted;
        patch::insert(
            self.acl_patch.get_or_insert_with(HashMap::new),
            PatchPath::new(["shareWith", id, acl.as_str()]),
            ACLPatch::Set(set),
        );
        self
    }
}

impl SetArguments {
    pub(crate) fn on_destroy_remove_emails(&mut self, value: bool) -> &mut Self {
        self.on_destroy_remove_emails = value.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn acl_setters_supersede_ancestors_and_keep_siblings() {
        let mut patch = MailboxPatch::default();
        patch.acls([("old", [ACL::ReadItems])]);
        patch.acl("a/b", [ACL::ReadItems]);
        patch.acl_set("a/b", ACL::AddItems, true);
        patch.acl_set("other", ACL::ReadItems, false);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({
                "shareWith/a~1b/mayAddItems": true,
                "shareWith/other/mayReadItems": false
            })
        );
        patch.acls(std::iter::empty::<(String, Vec<ACL>)>());
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({"shareWith": {}})
        );
    }
}
