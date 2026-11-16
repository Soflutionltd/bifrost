// AddressBook methods and builders are RFC 9610 surface the contacts sync uses in part.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod set;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::field::Field;

mod marker {
    pub(crate) enum AddressBook {}
}
/// Strongly-typed AddressBook ID.
pub(crate) type AddressBookId = crate::core::id::Id<marker::AddressBook>;

/// Server-returned AddressBook (RFC 8887).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AddressBook {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<AddressBookId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "description")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) description: Field<String>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_default: Option<bool>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) share_with: Field<HashMap<String, AddressBookRights>>,

    #[serde(rename = "myRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) my_rights: Option<AddressBookRights>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AddressBookCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<crate::core::creation::CreationId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) name: Field<String>,

    #[serde(rename = "description")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) description: Field<String>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) share_with: Field<HashMap<String, AddressBookRights>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct AddressBookPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "description")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) description: Field<String>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) share_with: Field<HashMap<String, AddressBookRights>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AddressBookRights {
    #[serde(rename = "mayRead")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_read: Option<bool>,

    #[serde(rename = "mayWrite")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_write: Option<bool>,

    #[serde(rename = "mayShare")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_share: Option<bool>,

    #[serde(rename = "mayDelete")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_delete: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct AddressBookSetArguments {
    #[serde(rename = "onDestroyRemoveContents")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) on_destroy_remove_contents: Option<bool>,

    #[serde(rename = "onSuccessSetIsDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) on_success_set_is_default: Option<String>,
}

impl AddressBookSetArguments {
    pub(crate) fn on_destroy_remove_contents(&mut self, remove: bool) -> &mut Self {
        self.on_destroy_remove_contents = Some(remove);
        self
    }

    pub(crate) fn on_success_set_is_default(&mut self, id: impl Into<String>) -> &mut Self {
        self.on_success_set_is_default = Some(id.into());
        self
    }
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Name => "name",
        Description => "description",
        SortOrder => "sortOrder",
        IsDefault => "isDefault",
        IsSubscribed => "isSubscribed",
        ShareWith => "shareWith",
        MyRights => "myRights",
    }
}

impl crate::core::Object for AddressBook {
    crate::core::method::object_metadata!(AddressBook);
    type Property = Property;
    type Id = AddressBookId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for AddressBook {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for AddressBook {
    type GetArguments = ();
}

impl crate::core::set::SetObject for AddressBook {
    type Create = AddressBookCreate;
    type Patch = AddressBookPatch;
    type SetArguments = AddressBookSetArguments;
}

impl crate::core::SetCreate for AddressBookCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.as_ref().map(ToString::to_string)
    }
    fn set_create_id(&mut self, id: crate::core::creation::CreationId) {
        self._create_id = Some(id);
    }

    fn new(create_id: Option<crate::core::creation::CreationId>) -> Self {
        AddressBookCreate {
            _create_id: create_id,
            name: Field::Omitted,
            description: Field::Omitted,
            sort_order: None,
            is_subscribed: None,
            share_with: Field::Omitted,
        }
    }
}

crate::define_get_method!(AddressBookGet, AddressBook);
crate::define_set_method!(AddressBookSet, AddressBook);
crate::define_changes_method!(AddressBookChanges, AddressBook);

impl AddressBookSet {
    #[must_use]
    pub(crate) fn on_destroy_remove_contents(mut self, remove: bool) -> Self {
        self.arguments().on_destroy_remove_contents(remove);
        self
    }

    #[must_use]
    pub(crate) fn on_success_set_is_default(mut self, id: impl Into<String>) -> Self {
        self.arguments().on_success_set_is_default(id);
        self
    }
}

#[cfg(test)]
mod wire_tests;
