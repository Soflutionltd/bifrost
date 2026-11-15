use serde_json::json;

use super::{ContactCardCreate, ContactCardPatch};
use crate::address_book::AddressBookId;

impl ContactCardCreate {
    pub(crate) fn address_book_ids<U, V>(&mut self, address_book_ids: U) -> &mut Self
    where
        U: IntoIterator<Item = V>,
        V: Into<AddressBookId>,
    {
        let map: serde_json::Map<String, serde_json::Value> = address_book_ids
            .into_iter()
            .map(|id| (id.into().into_string(), json!(true)))
            .collect();
        self.properties
            .insert("addressBookIds".into(), serde_json::Value::Object(map));
        self
    }

    pub(crate) fn address_book_id(
        &mut self,
        address_book_id: impl Into<AddressBookId>,
        set: bool,
    ) -> &mut Self {
        let entry = self
            .properties
            .entry("addressBookIds")
            .or_insert_with(|| json!({}));
        if let Some(map) = entry.as_object_mut() {
            // A create carries a complete Id[Boolean] set, not patch operations.
            let id = address_book_id.into().into_string();
            if set {
                map.insert(id, serde_json::Value::Bool(true));
            } else {
                map.remove(&id);
            }
        }
        self
    }
}

impl ContactCardPatch {
    pub(crate) fn address_book_ids<U, V>(&mut self, address_book_ids: U) -> &mut Self
    where
        U: IntoIterator<Item = V>,
        V: Into<AddressBookId>,
    {
        let map: serde_json::Map<String, serde_json::Value> = address_book_ids
            .into_iter()
            .map(|id| (id.into().into_string(), json!(true)))
            .collect();
        self.properties
            .retain(|name, _| !name.starts_with("addressBookIds/"));
        self.properties
            .insert("addressBookIds".into(), serde_json::Value::Object(map));
        self
    }

    /// Set or clear one address-book membership without replacing unrelated ones.
    pub(crate) fn address_book_id(
        &mut self,
        address_book_id: impl Into<AddressBookId>,
        set: bool,
    ) -> &mut Self {
        self.properties.remove("addressBookIds");
        self.properties.insert(
            format!(
                "addressBookIds/{}",
                crate::core::set::escape_json_pointer_token(&address_book_id.into().into_string())
            ),
            if set {
                serde_json::Value::Bool(true)
            } else {
                serde_json::Value::Null
            },
        );
        self
    }
}

macro_rules! cc_setters {
    ($t:ty) => {
        impl $t {
            pub(crate) fn uid(&mut self, uid: impl Into<String>) -> &mut Self {
                self.properties
                    .insert("uid".into(), serde_json::Value::String(uid.into()));
                self
            }

            pub(crate) fn kind(&mut self, kind: impl Into<String>) -> &mut Self {
                self.properties
                    .insert("kind".into(), serde_json::Value::String(kind.into()));
                self
            }

            pub(crate) fn name(
                &mut self,
                name: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("name".into(), serde_json::Value::Object(name));
                self
            }

            pub(crate) fn nicknames(
                &mut self,
                nicknames: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("nicknames".into(), serde_json::Value::Object(nicknames));
                self
            }

            pub(crate) fn emails(
                &mut self,
                emails: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("emails".into(), serde_json::Value::Object(emails));
                self
            }

            pub(crate) fn phones(
                &mut self,
                phones: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("phones".into(), serde_json::Value::Object(phones));
                self
            }

            pub(crate) fn addresses(
                &mut self,
                addresses: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("addresses".into(), serde_json::Value::Object(addresses));
                self
            }

            pub(crate) fn organizations(
                &mut self,
                organizations: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties.insert(
                    "organizations".into(),
                    serde_json::Value::Object(organizations),
                );
                self
            }

            pub(crate) fn online_services(
                &mut self,
                online_services: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties.insert(
                    "onlineServices".into(),
                    serde_json::Value::Object(online_services),
                );
                self
            }

            pub(crate) fn notes(
                &mut self,
                notes: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("notes".into(), serde_json::Value::Object(notes));
                self
            }
        }
    };
}

cc_setters!(ContactCardCreate);
cc_setters!(ContactCardPatch);
