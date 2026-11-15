//! ContactCard wraps a JSContact Card (RFC 9553) object.

// Query, copy and JSContact accessors are protocol surface the sync uses in part.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod parse;
pub(crate) mod query;
pub(crate) mod set;

mod marker {
    pub(crate) enum ContactCard {}
}
/// Strongly-typed ContactCard ID.
pub(crate) type ContactCardId = crate::core::id::Id<marker::ContactCard>;

crate::json_object_struct!(
    ContactCard,
    ContactCardCreate,
    ContactCardPatch,
    "a JSContact object"
);

crate::define_open_property_enum! {
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Uid => "uid",
        AddressBookIds => "addressBookIds",
        Kind => "kind",
        Name => "name",
        Nicknames => "nicknames",
        Emails => "emails",
        Phones => "phones",
        Addresses => "addresses",
        Organizations => "organizations",
        OnlineServices => "onlineServices",
        Notes => "notes",
        Media => "media",
        Created => "created",
        Updated => "updated",
    }
}

impl crate::core::Object for ContactCard {
    crate::core::method::object_metadata!(ContactCard);
    type Property = Property;
    type Id = ContactCardId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for ContactCard {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for ContactCard {
    type GetArguments = ();
}

impl crate::core::set::SetObject for ContactCard {
    type Create = ContactCardCreate;
    type Patch = ContactCardPatch;
    type SetArguments = ();
}

crate::define_get_method!(ContactCardGet, ContactCard);
crate::define_set_method!(ContactCardSet, ContactCard);
crate::define_changes_method!(ContactCardChanges, ContactCard);
crate::define_query_method!(ContactCardQuery, ContactCard);
crate::define_query_changes_method!(ContactCardQueryChanges, ContactCard);
crate::define_copy_method!(ContactCardCopy, ContactCard);
