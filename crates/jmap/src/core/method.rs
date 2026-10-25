use serde::{Serialize, de::DeserializeOwned};

use super::capability::Capability;
use super::id::AccountId;

/// A self-describing JMAP method call.
pub(crate) trait JmapMethod: Serialize + Send {
    const NAME: &'static str;
    /// Whether the transport may replay a batch made only of calls like this
    /// one: `true` exactly for reads. Every write is `false`, including an
    /// update-only `*/set` without `ifInState`: updates can carry side
    /// effects (`EmailSubmission/set` success actions, one-way `undoStatus`,
    /// `CalendarEvent/set` scheduling messages), and individually safe
    /// patches do not compose within one batch. No default, so every
    /// implementation states it.
    const REPLAY_SAFE: bool;
    type Cap: Capability;
    type Response: DeserializeOwned + Send;

    fn set_account_id(&mut self, _account_id: &AccountId) {}
}

/// Generates a JMAP /get method struct that wraps `GetRequest<O>`.
macro_rules! define_get_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub(crate) struct $name {
            #[serde(flatten)]
            inner: $crate::core::get::GetRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            const REPLAY_SAFE: bool = true;
            type Cap = $cap;
            type Response = $crate::core::get::GetResponse<$obj>;

            fn set_account_id(&mut self, account_id: &$crate::core::id::AccountId) {
                self.inner.account_id(account_id.clone());
            }
        }

        impl $name {
            pub(crate) fn new() -> Self {
                Self {
                    inner: $crate::core::get::GetRequest::new(),
                }
            }

            #[must_use]
            pub(crate) fn ids<U, V>(mut self, ids: U) -> Self
            where
                U: IntoIterator<Item = V>,
                V: Into<<$obj as $crate::core::Object>::Id>,
            {
                self.inner.ids(ids);
                self
            }

            #[must_use]
            pub(crate) fn ids_ref(
                mut self,
                reference: $crate::core::request::ResultReference,
            ) -> Self {
                self.inner.ids_ref(reference);
                self
            }

            #[must_use]
            pub(crate) fn properties(
                mut self,
                properties: impl IntoIterator<Item = <$obj as $crate::core::Object>::Property>,
            ) -> Self {
                self.inner.properties(properties);
                self
            }

            #[must_use]
            pub(crate) fn properties_ref(
                mut self,
                reference: $crate::core::request::ResultReference,
            ) -> Self {
                self.inner.properties_ref(reference);
                self
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::get::GetRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /set method struct that wraps `SetRequest<O>`.
macro_rules! define_set_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub(crate) struct $name {
            #[serde(flatten)]
            inner: $crate::core::set::SetRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            const REPLAY_SAFE: bool = false;
            type Cap = $cap;
            type Response = $crate::core::set::SetResponse<$obj>;

            fn set_account_id(&mut self, account_id: &$crate::core::id::AccountId) {
                self.inner.account_id(account_id.clone());
            }
        }

        impl $name {
            pub(crate) fn new() -> Self {
                Self {
                    inner: $crate::core::set::SetRequest::new(),
                }
            }

            #[must_use]
            pub(crate) fn if_in_state(mut self, if_in_state: impl Into<String>) -> Self {
                self.inner.if_in_state(if_in_state);
                self
            }

            #[must_use]
            pub(crate) fn destroy<U, V>(mut self, ids: U) -> Self
            where
                U: IntoIterator<Item = V>,
                V: Into<<$obj as $crate::core::Object>::Id>,
            {
                self.inner.destroy(ids);
                self
            }

            #[must_use]
            pub(crate) fn destroy_ref(
                mut self,
                reference: $crate::core::request::ResultReference,
            ) -> Self {
                self.inner.destroy_ref(reference);
                self
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::set::SetRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /changes method struct that wraps `ChangesRequest`.
macro_rules! define_changes_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub(crate) struct $name {
            #[serde(flatten)]
            inner: $crate::core::changes::ChangesRequest,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            const REPLAY_SAFE: bool = true;
            type Cap = $cap;
            type Response = $crate::core::changes::ChangesResponse<$obj>;

            fn set_account_id(&mut self, account_id: &$crate::core::id::AccountId) {
                self.inner.account_id(account_id.clone());
            }
        }

        impl $name {
            pub(crate) fn new(since_state: impl Into<String>) -> Self {
                Self {
                    inner: $crate::core::changes::ChangesRequest::new(since_state),
                }
            }

            #[must_use]
            pub(crate) fn max_changes(mut self, max_changes: std::num::NonZeroUsize) -> Self {
                self.inner.max_changes(max_changes);
                self
            }
        }

        impl $crate::core::changes::ChangesMethod for $name {
            fn since(since_state: String, max_changes: std::num::NonZeroUsize) -> Self {
                Self::new(since_state).max_changes(max_changes)
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::changes::ChangesRequest;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /query method struct that wraps `QueryRequest<O>`.
macro_rules! define_query_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub(crate) struct $name {
            #[serde(flatten)]
            inner: $crate::core::query::QueryRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            const REPLAY_SAFE: bool = true;
            type Cap = $cap;
            type Response = $crate::core::query::QueryResponse<$obj>;

            fn set_account_id(&mut self, account_id: &$crate::core::id::AccountId) {
                self.inner.account_id(account_id.clone());
            }
        }

        impl $name {
            pub(crate) fn new() -> Self {
                Self {
                    inner: $crate::core::query::QueryRequest::new(),
                }
            }

            #[must_use]
            pub(crate) fn filter(
                mut self,
                filter: impl Into<
                    $crate::core::query::Filter<<$obj as $crate::core::query::QueryObject>::Filter>,
                >,
            ) -> Self {
                self.inner.filter(filter);
                self
            }

            #[must_use]
            pub(crate) fn sort(
                mut self,
                sort: impl IntoIterator<
                    Item = $crate::core::query::Comparator<
                        <$obj as $crate::core::query::QueryObject>::Sort,
                    >,
                >,
            ) -> Self {
                self.inner.sort(sort);
                self
            }

            #[must_use]
            pub(crate) fn position(mut self, position: i32) -> Self {
                self.inner.position(position);
                self
            }

            #[must_use]
            pub(crate) fn anchor(mut self, anchor: impl Into<String>) -> Self {
                self.inner.anchor(anchor);
                self
            }

            #[must_use]
            pub(crate) fn anchor_offset(mut self, anchor_offset: i32) -> Self {
                self.inner.anchor_offset(anchor_offset);
                self
            }

            #[must_use]
            pub(crate) fn limit(mut self, limit: usize) -> Self {
                self.inner.limit(limit);
                self
            }

            #[must_use]
            pub(crate) fn calculate_total(mut self, calculate_total: bool) -> Self {
                self.inner.calculate_total(calculate_total);
                self
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::query::QueryRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /queryChanges method struct.
macro_rules! define_query_changes_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub(crate) struct $name {
            #[serde(flatten)]
            inner: $crate::core::query_changes::QueryChangesRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            const REPLAY_SAFE: bool = true;
            type Cap = $cap;
            type Response = $crate::core::query_changes::QueryChangesResponse<$obj>;

            fn set_account_id(&mut self, account_id: &$crate::core::id::AccountId) {
                self.inner.account_id(account_id.clone());
            }
        }

        impl $name {
            pub(crate) fn new(since_query_state: impl Into<String>) -> Self {
                Self {
                    inner: $crate::core::query_changes::QueryChangesRequest::new(since_query_state),
                }
            }

            #[must_use]
            pub(crate) fn filter(
                mut self,
                filter: impl Into<
                    $crate::core::query::Filter<<$obj as $crate::core::query::QueryObject>::Filter>,
                >,
            ) -> Self {
                self.inner.filter(filter);
                self
            }

            #[must_use]
            pub(crate) fn sort(
                mut self,
                sort: impl IntoIterator<
                    Item = $crate::core::query::Comparator<
                        <$obj as $crate::core::query::QueryObject>::Sort,
                    >,
                >,
            ) -> Self {
                self.inner.sort(sort);
                self
            }

            #[must_use]
            pub(crate) fn max_changes(mut self, max_changes: std::num::NonZeroUsize) -> Self {
                self.inner.max_changes(max_changes);
                self
            }

            #[must_use]
            pub(crate) fn up_to_id(
                mut self,
                up_to_id: impl Into<<$obj as $crate::core::Object>::Id>,
            ) -> Self {
                self.inner.up_to_id(up_to_id);
                self
            }

            #[must_use]
            pub(crate) fn calculate_total(mut self, calculate_total: bool) -> Self {
                self.inner.calculate_total(calculate_total);
                self
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::query_changes::QueryChangesRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a JMAP /copy method struct that wraps `CopyRequest<O>`.
///
/// Compiled only for the features whose object modules invoke it, so a
/// reduced-feature build of the crate carries no dead macro.
#[cfg(any(feature = "mail", feature = "contacts", feature = "calendars"))]
macro_rules! define_copy_method {
    ($name:ident, $obj:ty, $method_name:expr, $cap:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub(crate) struct $name {
            #[serde(flatten)]
            inner: $crate::core::copy::CopyRequest<$obj>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            const REPLAY_SAFE: bool = false;
            type Cap = $cap;
            type Response = $crate::core::copy::CopyResponse<$obj>;

            fn set_account_id(&mut self, account_id: &$crate::core::id::AccountId) {
                self.inner.account_id(account_id.clone());
            }
        }

        impl $name {
            pub(crate) fn new(from_account_id: impl Into<$crate::core::id::AccountId>) -> Self {
                Self {
                    inner: $crate::core::copy::CopyRequest::new(from_account_id),
                }
            }

            #[must_use]
            pub(crate) fn if_from_in_state(mut self, if_from_in_state: impl Into<String>) -> Self {
                self.inner.if_from_in_state(if_from_in_state);
                self
            }

            #[must_use]
            pub(crate) fn if_in_state(mut self, if_in_state: impl Into<String>) -> Self {
                self.inner.if_in_state(if_in_state);
                self
            }

            #[must_use]
            pub(crate) fn on_success_destroy_original(mut self, value: bool) -> Self {
                self.inner.on_success_destroy_original(value);
                self
            }

            #[must_use]
            pub(crate) fn destroy_from_if_in_state(
                mut self,
                destroy_from_if_in_state: impl Into<String>,
            ) -> Self {
                self.inner
                    .destroy_from_if_in_state(destroy_from_if_in_state);
                self
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::core::copy::CopyRequest<$obj>;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
    };
}

/// Generates a Property enum with `as_str()`, `Display`, `Serialize`,
/// `Deserialize`, and `From<&str>` impls, plus an `Other(String)` catch-all.
#[cfg(any(feature = "contacts", feature = "calendars"))]
macro_rules! define_open_property_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident => $wire:expr ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        $vis enum $name {
            $( $(#[$variant_meta])* $variant, )*
            Other(String),
        }

        impl $name {
            pub(crate) fn as_str(&self) -> &str {
                match self {
                    $( Self::$variant => $wire, )*
                    Self::Other(s) => s.as_str(),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct PropertyVisitor;
                impl serde::de::Visitor<'_> for PropertyVisitor {
                    type Value = $name;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str("a property name")
                    }
                    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<$name, E> {
                        Ok($name::from(v))
                    }
                }
                deserializer.deserialize_str(PropertyVisitor)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                match s {
                    $( $wire => Self::$variant, )*
                    other => Self::Other(other.to_string()),
                }
            }
        }
    };
}

/// Generates a JMAP /parse method struct.
#[cfg(any(feature = "contacts", feature = "calendars"))]
macro_rules! define_parse_method {
    ($name:ident, $property:ty, $method_name:expr, $cap:ty, $response:ty) => {
        #[derive(Debug, Clone, serde::Serialize)]
        pub(crate) struct $name {
            #[serde(rename = "accountId")]
            account_id: $crate::core::id::AccountId,

            #[serde(rename = "blobIds")]
            blob_ids: Vec<$crate::core::id::BlobId>,

            #[serde(rename = "properties")]
            #[serde(skip_serializing_if = "Option::is_none")]
            properties: Option<Vec<$property>>,
        }

        impl $crate::core::method::JmapMethod for $name {
            const NAME: &'static str = $method_name;
            const REPLAY_SAFE: bool = true;
            type Cap = $cap;
            type Response = $response;

            fn set_account_id(&mut self, account_id: &$crate::core::id::AccountId) {
                self.account_id = account_id.clone();
            }
        }

        impl $name {
            pub(crate) fn new() -> Self {
                Self {
                    account_id: $crate::core::id::AccountId::new(""),
                    blob_ids: Vec::new(),
                    properties: None,
                }
            }

            #[must_use]
            pub(crate) fn blob_ids<U, V>(mut self, blob_ids: U) -> Self
            where
                U: IntoIterator<Item = V>,
                V: Into<$crate::core::id::BlobId>,
            {
                self.blob_ids = blob_ids.into_iter().map(std::convert::Into::into).collect();
                self
            }

            #[must_use]
            pub(crate) fn properties(
                mut self,
                properties: impl IntoIterator<Item = $property>,
            ) -> Self {
                self.properties = Some(properties.into_iter().collect());
                self
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

pub(crate) use define_changes_method;
#[cfg(any(feature = "mail", feature = "contacts", feature = "calendars"))]
pub(crate) use define_copy_method;
pub(crate) use define_get_method;
#[cfg(any(feature = "contacts", feature = "calendars"))]
pub(crate) use define_open_property_enum;
#[cfg(any(feature = "contacts", feature = "calendars"))]
pub(crate) use define_parse_method;
pub(crate) use define_query_changes_method;
pub(crate) use define_query_method;
pub(crate) use define_set_method;
