//! JMAP ShareNotification (RFC 9670). Destroy-only.

// No Account method consumes ShareNotification today; the RFC types
// are kept built and covered by tests for a future sharing-aware stage.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod query;
pub(crate) mod set;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

mod marker {
    pub(crate) enum ShareNotification {}
}
/// Strongly-typed ShareNotification ID.
pub(crate) type ShareNotificationId = crate::core::id::Id<marker::ShareNotification>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ShareNotification {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<ShareNotificationId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) created: Option<String>,

    #[serde(rename = "changedBy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) changed_by: Option<ChangedBy>,

    #[serde(rename = "objectType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) object_type: Option<String>,

    #[serde(rename = "objectAccountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) object_account_id: Option<crate::core::id::AccountId>,

    /// Server-side ID of the shared object. Its concrete type depends
    /// on `objectType` (Mailbox, Calendar, AddressBook, ...) so this
    /// stays `String`-typed at the schema layer.
    #[serde(rename = "objectId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) object_id: Option<String>,

    #[serde(rename = "oldRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) old_rights: Option<HashMap<String, bool>>,

    #[serde(rename = "newRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) new_rights: Option<HashMap<String, bool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,
}

/// Uninhabitable Create-shape: ShareNotifications cannot be created.
/// `SetRequest::create()` will not resolve because this type does not
/// impl `SetCreate`.
#[derive(Debug, Clone, Serialize)]
pub(crate) enum ShareNotificationCreate {}

/// Uninhabitable Patch-shape: ShareNotifications cannot be updated.
/// `SetRequest::update()` will not resolve because this type does not
/// impl `Default`.
#[derive(Debug, Clone, Serialize)]
pub(crate) enum ShareNotificationPatch {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChangedBy {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,

    #[serde(rename = "principalId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    principal_id: Option<crate::principal::PrincipalId>,
}

impl ChangedBy {
    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    pub(crate) fn principal_id(&self) -> Option<&crate::principal::PrincipalId> {
        self.principal_id.as_ref()
    }
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Created => "created",
        ChangedBy => "changedBy",
        ObjectType => "objectType",
        ObjectAccountId => "objectAccountId",
        ObjectId => "objectId",
        OldRights => "oldRights",
        NewRights => "newRights",
        Name => "name",
    }
}

impl crate::core::Object for ShareNotification {
    crate::core::method::object_metadata!(ShareNotification);
    type Property = Property;
    type Id = ShareNotificationId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for ShareNotification {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for ShareNotification {
    type GetArguments = ();
}

impl crate::core::set::SetObject for ShareNotification {
    type Create = ShareNotificationCreate;
    type Patch = ShareNotificationPatch;
    type SetArguments = ();
}

crate::define_get_method!(ShareNotificationGet, ShareNotification);
crate::define_set_method!(ShareNotificationSet, ShareNotification);
crate::define_changes_method!(ShareNotificationChanges, ShareNotification);
crate::define_query_method!(ShareNotificationQuery, ShareNotification);
crate::define_query_changes_method!(ShareNotificationQueryChanges, ShareNotification);

#[cfg(test)]
mod wire_tests;
