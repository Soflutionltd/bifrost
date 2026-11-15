// Thread/get and Thread/changes are RFC 8621 surface retained beyond what the mail sync calls.
#![allow(dead_code)]

pub(crate) mod get;

use serde::{Deserialize, Serialize};

mod marker {
    pub(crate) enum Thread {}
}
/// Strongly-typed Thread ID.
pub(crate) type ThreadId = crate::core::id::Id<marker::Thread>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Thread {
    id: ThreadId,
    #[serde(rename = "emailIds", default, skip_serializing_if = "Option::is_none")]
    email_ids: Option<Vec<crate::email::EmailId>>,
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        EmailIds => "emailIds",
    }
}

impl crate::core::Object for Thread {
    crate::core::method::object_metadata!(Thread);
    type Property = Property;
    type Id = ThreadId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Thread {
    type ChangesResponse = ();
}

crate::define_get_method!(ThreadGet, Thread);
crate::define_changes_method!(ThreadChanges, Thread);
