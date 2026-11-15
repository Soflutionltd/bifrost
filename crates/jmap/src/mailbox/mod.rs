// The Account impl uses Mailbox/get + Mailbox/set CRUD; Query,
// QueryChanges, and ACL helpers stay built for the sharing-aware
// stage that may consume them.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod query;
pub(crate) mod set;

use crate::core::field::Field;
use crate::core::set::skip_if_empty_map;
use crate::mailbox::set::role_not_set;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod marker {
    pub(crate) enum Mailbox {}
}
/// Strongly-typed Mailbox ID.
pub(crate) type MailboxId = crate::core::id::Id<marker::Mailbox>;

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct SetArguments {
    #[serde(rename = "onDestroyRemoveEmails")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_destroy_remove_emails: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct QueryArguments {
    #[serde(rename = "sortAsTree")]
    sort_as_tree: bool,
    #[serde(rename = "filterAsTree")]
    filter_as_tree: bool,
}

// -- Lifted method arguments --

impl MailboxSet {
    #[must_use]
    pub(crate) fn on_destroy_remove_emails(mut self, value: bool) -> Self {
        self.arguments().on_destroy_remove_emails(value);
        self
    }
}

impl MailboxQuery {
    #[must_use]
    pub(crate) fn sort_as_tree(mut self, value: bool) -> Self {
        self.arguments().sort_as_tree(value);
        self
    }

    #[must_use]
    pub(crate) fn filter_as_tree(mut self, value: bool) -> Self {
        self.arguments().filter_as_tree(value);
        self
    }
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ChangesResponse {
    #[serde(rename = "updatedProperties")]
    updated_properties: Option<Vec<Property>>,
}

/// Server-returned Mailbox object (RFC 8621 §2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Mailbox {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<MailboxId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "parentId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) parent_id: Option<MailboxId>,

    #[serde(rename = "role")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) role: Option<Role>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "totalEmails")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) total_emails: Option<usize>,

    #[serde(rename = "unreadEmails")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) unread_emails: Option<usize>,

    #[serde(rename = "totalThreads")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) total_threads: Option<usize>,

    #[serde(rename = "unreadThreads")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) unread_threads: Option<usize>,

    #[serde(rename = "myRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) my_rights: Option<MailboxRights>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) share_with: Option<HashMap<String, HashMap<ACL, bool>>>,
}

/// Client-sent Mailbox/set `create` payload.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct MailboxCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<crate::core::creation::CreationId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "parentId")]
    #[serde(skip_serializing_if = "crate::core::creation::skip_if_empty_id_or_ref")]
    pub(super) parent_id: Option<crate::core::creation::IdOrRef<MailboxId>>,

    #[serde(rename = "role")]
    #[serde(skip_serializing_if = "role_not_set")]
    pub(super) role: Option<Role>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "skip_if_empty_map")]
    pub(super) share_with: Option<HashMap<String, HashMap<ACL, bool>>>,
}

/// Client-sent Mailbox/set `update` patch payload.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct MailboxPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "parentId")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) parent_id: Field<crate::core::creation::IdOrRef<MailboxId>>,

    #[serde(rename = "role")]
    #[serde(skip_serializing_if = "role_not_set")]
    pub(super) role: Option<Role>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "skip_if_empty_map")]
    pub(super) share_with: Option<HashMap<String, HashMap<ACL, bool>>>,

    #[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) acl_patch: Option<HashMap<String, ACLPatch>>,
}

impl Default for MailboxPatch {
    fn default() -> Self {
        Self {
            name: None,
            parent_id: Field::Omitted,
            role: Some(Role::None),
            sort_order: None,
            is_subscribed: None,
            share_with: Some(HashMap::new()),
            acl_patch: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum ACLPatch {
    Replace(HashMap<ACL, bool>),
    Set(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub(crate) enum Role {
    Archive,
    Drafts,
    Important,
    Inbox,
    Junk,
    Sent,
    Trash,
    Other(String),
    #[default]
    None,
}

pub(crate) use crate::mailbox_rights::{ACL, MailboxRights};

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Name => "name",
        ParentId => "parentId",
        Role => "role",
        SortOrder => "sortOrder",
        TotalEmails => "totalEmails",
        UnreadEmails => "unreadEmails",
        TotalThreads => "totalThreads",
        UnreadThreads => "unreadThreads",
        MyRights => "myRights",
        IsSubscribed => "isSubscribed",
        ShareWith => "shareWith",
    }
}

impl Property {
    pub(crate) fn is_count(&self) -> bool {
        matches!(
            self,
            Property::TotalEmails
                | Property::UnreadEmails
                | Property::TotalThreads
                | Property::UnreadThreads
        )
    }
}

impl ChangesResponse {
    pub(crate) fn updated_properties(&self) -> Option<&[Property]> {
        self.updated_properties.as_deref()
    }
}

// -- Trait impls --

impl crate::core::Object for Mailbox {
    type Property = Property;
    type Id = MailboxId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Mailbox {
    type ChangesResponse = ChangesResponse;
}

impl crate::core::get::GetObject for Mailbox {
    type GetArguments = ();
}

impl crate::core::set::SetObject for Mailbox {
    type Create = MailboxCreate;
    type Patch = MailboxPatch;
    type SetArguments = SetArguments;
}

impl crate::core::SetCreate for MailboxCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.as_ref().map(ToString::to_string)
    }
    fn set_create_id(&mut self, id: crate::core::creation::CreationId) {
        self._create_id = Some(id);
    }

    fn new(create_id: Option<crate::core::creation::CreationId>) -> Self {
        MailboxCreate {
            _create_id: create_id,
            name: None,
            parent_id: Some(crate::core::creation::IdOrRef::Id(MailboxId::new(""))),
            role: Role::None.into(),
            sort_order: None,
            is_subscribed: None,
            share_with: HashMap::with_capacity(0).into(),
        }
    }
}

// Method struct definitions

crate::define_get_method!(
    MailboxGet,
    Mailbox,
    "Mailbox/get",
    crate::core::capability::Mail
);
crate::define_set_method!(
    MailboxSet,
    Mailbox,
    "Mailbox/set",
    crate::core::capability::Mail
);
crate::define_changes_method!(
    MailboxChanges,
    Mailbox,
    "Mailbox/changes",
    crate::core::capability::Mail
);
crate::define_query_method!(
    MailboxQuery,
    Mailbox,
    "Mailbox/query",
    crate::core::capability::Mail
);
crate::define_query_changes_method!(
    MailboxQueryChanges,
    Mailbox,
    "Mailbox/queryChanges",
    crate::core::capability::Mail
);

impl<'de> Deserialize<'de> for Role {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match String::deserialize(deserializer)?
            .to_ascii_lowercase()
            .as_str()
        {
            "inbox" => Ok(Role::Inbox),
            "sent" => Ok(Role::Sent),
            "trash" => Ok(Role::Trash),
            "drafts" => Ok(Role::Drafts),
            "junk" => Ok(Role::Junk),
            "archive" => Ok(Role::Archive),
            "important" => Ok(Role::Important),
            other => Ok(Role::Other(other.to_string())),
        }
    }
}

impl Serialize for Role {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            Role::Inbox => "inbox",
            Role::Sent => "sent",
            Role::Trash => "trash",
            Role::Drafts => "drafts",
            Role::Junk => "junk",
            Role::Archive => "archive",
            Role::Important => "important",
            Role::Other(other) => other,
            Role::None => "",
        })
    }
}
