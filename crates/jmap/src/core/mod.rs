// Core carries the generic RFC 8620 framework (every method shape, session and
// response accessors, set and query builders); objects use different subsets.
#![allow(dead_code)]

use std::fmt::Display;
use std::hash::Hash;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub(crate) mod answer;
pub(crate) mod capability;
pub(crate) mod changes;
pub(crate) mod copy;
pub(crate) mod creation;
pub(crate) mod error;
pub(crate) mod field;
pub(crate) mod get;
pub(crate) mod id;
pub(crate) mod method;
pub(crate) mod parse;
pub(crate) mod patch;
pub(crate) mod query;
pub(crate) mod query_changes;
pub(crate) mod request;
pub(crate) mod response;
pub(crate) mod session;
pub(crate) mod set;
pub(crate) mod transport;

#[cfg(test)]
mod tests;

/// The base trait for a JMAP object's server-returned (Get) shape.
pub(crate) trait Object: Sized {
    // Protocol identity does not determine the account's cursor scope:
    // Email and EmailDelivery, for example, both invalidate the Email scope.
    // That routing stays with the account consumer rather than this wire table.
    const DATA_TYPE: crate::DataType;
    type Cap: capability::Capability;
    const METHODS: method::ObjectMethods;
    type Property: Display + Serialize + DeserializeOwned;
    /// The strongly-typed ID for this object (`EmailId`, `MailboxId`,
    /// etc.). Used as the typed key/value type in generic core
    /// `SetRequest`/`SetResponse`/`GetRequest`/`GetResponse`/`Changes`/
    /// `Query`. Per-object impls set this to their `Id<marker::Foo>`
    /// typedef.
    type Id: Clone + Hash + Eq + Display + Serialize + DeserializeOwned + From<String> + Send;
    fn requires_account_id() -> bool;
}

/// The trait implemented by a Create-shape input struct (e.g.
/// `MailboxCreate`, `EmailCreate`).
pub(crate) trait SetCreate: Sized {
    fn create_id(&self) -> Option<String>;
    fn set_create_id(&mut self, id: crate::core::creation::CreationId);
    fn new(create_id: Option<crate::core::creation::CreationId>) -> Self;
}

/// Generates the trio of JSON-map-backed JMAP types: Get-shape
/// (deserializable, holds whatever the server sent), Create-shape
/// (settable, no `_id`, carries `_create_id`), Patch-shape (settable,
/// allows dotted-path keys for nested patches).
///
/// Each type wraps a `serde_json::Map` so vendor extension properties
/// survive round-trip. Used for CalendarEvent and ContactCard, and compiled
/// only for those features, so a reduced-feature build carries no dead macro.
#[cfg(any(feature = "contacts", feature = "calendars"))]
macro_rules! json_object_struct {
    ($name:ident, $create:ident, $patch:ident, $expecting:expr) => {
        #[derive(Debug, Clone)]
        pub(crate) struct $name {
            /// The raw properties map. Every key/value from the server
            /// is preserved, including vendor extension properties.
            pub(crate) properties: serde_json::Map<String, serde_json::Value>,
        }

        #[derive(Debug, Clone)]
        pub(crate) struct $create {
            pub(super) _create_id: Option<crate::core::creation::CreationId>,
            pub(crate) properties: serde_json::Map<String, serde_json::Value>,
        }

        #[derive(Debug, Clone, Default)]
        pub(crate) struct $patch {
            /// Dotted-path patch keys (e.g. `participants/p1/name`) are
            /// permitted by JMAP /set update semantics.
            pub(crate) properties: serde_json::Map<String, serde_json::Value>,
        }

        $crate::__json_object_serde!($name, $expecting, with_deserialize);
        $crate::__json_object_serde!($create, $expecting, no_deserialize);
        $crate::__json_object_serde!($patch, $expecting, no_deserialize);

        impl $crate::core::SetCreate for $create {
            fn create_id(&self) -> Option<String> {
                self._create_id.as_ref().map(ToString::to_string)
            }
            fn set_create_id(&mut self, id: crate::core::creation::CreationId) {
                self._create_id = Some(id);
            }
            fn new(_create_id: Option<crate::core::creation::CreationId>) -> Self {
                Self {
                    _create_id,
                    properties: serde_json::Map::new(),
                }
            }
        }

        impl $create {
            pub(crate) fn set_property(
                &mut self,
                name: impl Into<String>,
                value: serde_json::Value,
            ) -> &mut Self {
                self.properties.insert(name.into(), value);
                self
            }
            pub(crate) fn properties_mut(
                &mut self,
            ) -> &mut serde_json::Map<String, serde_json::Value> {
                &mut self.properties
            }
        }

        impl $patch {
            pub(crate) fn set_property(
                &mut self,
                name: impl Into<$crate::core::patch::PatchPath>,
                value: serde_json::Value,
            ) -> &mut Self {
                $crate::core::patch::insert(&mut self.properties, name, value);
                self
            }
            // Raw extension access intentionally remains possible. Callers editing
            // paths here own RFC 8620 overlap validation; typed setters and
            // set_property use the central path policy instead.
            pub(crate) fn properties_mut(
                &mut self,
            ) -> &mut serde_json::Map<String, serde_json::Value> {
                &mut self.properties
            }
        }
    };
}

#[cfg(any(feature = "contacts", feature = "calendars"))]
macro_rules! __json_object_serde {
    ($name:ident, $expecting:expr, with_deserialize) => {
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(self.properties.len()))?;
                for (k, v) in &self.properties {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct V;
                impl<'de> serde::de::Visitor<'de> for V {
                    type Value = $name;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str($expecting)
                    }
                    fn visit_map<M: serde::de::MapAccess<'de>>(
                        self,
                        mut map: M,
                    ) -> Result<Self::Value, M::Error> {
                        let mut properties = serde_json::Map::new();
                        while let Some((key, value)) =
                            map.next_entry::<String, serde_json::Value>()?
                        {
                            properties.insert(key, value);
                        }
                        Ok($name { properties })
                    }
                }
                deserializer.deserialize_map(V)
            }
        }
    };
    ($name:ident, $expecting:expr, no_deserialize) => {
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(self.properties.len()))?;
                for (k, v) in &self.properties {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    };
}

#[cfg(any(feature = "contacts", feature = "calendars"))]
pub(crate) use __json_object_serde;
#[cfg(any(feature = "contacts", feature = "calendars"))]
pub(crate) use json_object_struct;
