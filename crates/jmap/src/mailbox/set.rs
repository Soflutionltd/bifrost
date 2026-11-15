use super::{ACLPatch, MailboxCreate, MailboxId, MailboxPatch, Role, SetArguments};
use crate::core::creation::{CreationId, IdOrRef};
use crate::core::field::Field;
use crate::mailbox::ACL;
use std::collections::HashMap;

impl MailboxCreate {
    pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub(crate) fn parent_id(&mut self, parent_id: Option<impl Into<MailboxId>>) -> &mut Self {
        self.parent_id = parent_id.map(|id| IdOrRef::Id(id.into()));
        self
    }

    pub(crate) fn parent_id_ref(&mut self, parent_id_ref: &str) -> &mut Self {
        self.parent_id = Some(IdOrRef::Reference(
            CreationId::named(parent_id_ref).reference(),
        ));
        self
    }

    pub(crate) fn role(&mut self, role: Role) -> &mut Self {
        if !matches!(role, Role::None) {
            self.role = Some(role);
        } else {
            self.role = None;
        }
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
        self.share_with = Some(
            acls.into_iter()
                .map(|(id, acls)| (id.into(), acls.into_iter().map(|acl| (acl, true)).collect()))
                .collect(),
        );
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

    pub(crate) fn role(&mut self, role: Role) -> &mut Self {
        if !matches!(role, Role::None) {
            self.role = Some(role);
        } else {
            self.role = None;
        }
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
        self.share_with = Some(
            acls.into_iter()
                .map(|(id, acls)| (id.into(), acls.into_iter().map(|acl| (acl, true)).collect()))
                .collect(),
        );
        self
    }

    pub(crate) fn acl(&mut self, id: &str, acl: impl IntoIterator<Item = ACL>) -> &mut Self {
        self.acl_patch.get_or_insert_with(HashMap::new).insert(
            format!(
                "shareWith/{}",
                crate::core::set::escape_json_pointer_token(id)
            ),
            ACLPatch::Replace(acl.into_iter().map(|acl| (acl, true)).collect()),
        );
        self
    }

    pub(crate) fn acl_set(&mut self, id: &str, acl: ACL, set: bool) -> &mut Self {
        self.acl_patch.get_or_insert_with(HashMap::new).insert(
            format!(
                "shareWith/{}/{}",
                crate::core::set::escape_json_pointer_token(id),
                acl.as_str()
            ),
            ACLPatch::Set(set),
        );
        self
    }
}

pub(crate) fn role_not_set(role: &Option<Role>) -> bool {
    matches!(role, Some(Role::None))
}

impl SetArguments {
    pub(crate) fn on_destroy_remove_emails(&mut self, value: bool) -> &mut Self {
        self.on_destroy_remove_emails = value.into();
        self
    }
}
