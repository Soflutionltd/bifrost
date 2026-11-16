use std::fmt;

use serde::{Deserialize, Serialize};

/// A strongly-typed JMAP identifier.
///
/// Wraps a `String` with a phantom type parameter to prevent mixing
/// IDs from different object types at compile time. The phantom is a
/// module-private uninhabited marker; consumers spell the public
/// typedef (`AccountId`, `BlobId`, `EmailId`, etc.) and never see the
/// marker name.
///
/// ```ignore
/// let email_id: crate::email::EmailId = "msg-123".into();
/// let mailbox_id: crate::mailbox::MailboxId = "mbox-1".into();
/// // email_id == mailbox_id  // compile error - different types
/// ```
// types: separate from bifrost-types IDs because JMAP wire models need per-object phantom typing.
#[derive(Serialize)]
#[serde(transparent)]
pub(crate) struct Id<T: ?Sized, const NATIVE: bool = true>(
    String,
    #[serde(skip)] std::marker::PhantomData<T>,
);

// Native ids cross into the consumer's qualified namespace during projection.
// Reject its reserved separator before any server observation can become a
// routable identity. Persisted consumer ids still use the legacy decoder; an
// Outlook user must never have malformed server data redirect a mail operation.
// Reject the entire decoded response, not just the offending row: dropping
// identities can silently complete an incomplete inventory or erase a write's
// outcome. An Outlook user expects a visible failed read over missing mail or
// an unaccounted write. Callers must not advance a cursor from that response.
// State tokens are opaque positions, not native object/account identities.
impl<'de, T: ?Sized, const NATIVE: bool> Deserialize<'de> for Id<T, NATIVE> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if NATIVE {
            validate_native(&value).map_err(serde::de::Error::custom)?;
        }
        Ok(Self::new(value))
    }
}

pub(crate) fn validate_native(value: &str) -> Result<(), &'static str> {
    if value.contains('\u{1f}') {
        Err("JMAP native id contains the reserved owner separator")
    } else {
        Ok(())
    }
}

/// Response ledgers echo submitted ids, including legacy consumer spellings
/// deliberately left literal on a departed/primary route. They must still
/// correlate (especially notFound), without admitting malformed newly minted
/// identities in object properties, query results or changes observations.
pub(crate) mod echo {
    use serde::{Deserialize, Deserializer};
    use std::collections::HashMap;
    use std::hash::Hash;

    pub(crate) fn map<'de, D, I, V>(deserializer: D) -> Result<Option<HashMap<I, V>>, D::Error>
    where
        D: Deserializer<'de>,
        I: From<String> + Eq + Hash,
        V: Deserialize<'de>,
    {
        Ok(
            Option::<HashMap<String, V>>::deserialize(deserializer)?.map(|values| {
                values
                    .into_iter()
                    .map(|(id, value)| (I::from(id), value))
                    .collect()
            }),
        )
    }

    pub(crate) fn optional_vec<'de, D, I>(deserializer: D) -> Result<Option<Vec<I>>, D::Error>
    where
        D: Deserializer<'de>,
        I: From<String>,
    {
        Ok(Option::<Vec<String>>::deserialize(deserializer)?
            .map(|values| values.into_iter().map(I::from).collect()))
    }

    pub(crate) fn vec<'de, D, I>(deserializer: D) -> Result<Vec<I>, D::Error>
    where
        D: Deserializer<'de>,
        I: From<String>,
    {
        Ok(Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .map(I::from)
            .collect())
    }
}

// Manual Clone / PartialEq / Eq / Hash impls: the derived forms each
// require `T: Trait`, but `T` is a phantom marker (often an
// uninhabited enum), so any trait bound on it is bogus. The string
// already implements all of these and `PhantomData` implements them
// unconditionally.
impl<T: ?Sized, const NATIVE: bool> Clone for Id<T, NATIVE> {
    fn clone(&self) -> Self {
        Id(self.0.clone(), std::marker::PhantomData)
    }
}

impl<T: ?Sized, const NATIVE: bool> PartialEq for Id<T, NATIVE> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: ?Sized, const NATIVE: bool> Eq for Id<T, NATIVE> {}

impl<T: ?Sized, const NATIVE: bool> std::hash::Hash for Id<T, NATIVE> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T: ?Sized, const NATIVE: bool> Id<T, NATIVE> {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Id(id.into(), std::marker::PhantomData)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

impl<T: ?Sized, const NATIVE: bool> fmt::Debug for Id<T, NATIVE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id({:?})", self.0)
    }
}

impl<T: ?Sized, const NATIVE: bool> fmt::Display for Id<T, NATIVE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<T: ?Sized, const NATIVE: bool> AsRef<str> for Id<T, NATIVE> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<T: ?Sized, const NATIVE: bool> From<String> for Id<T, NATIVE> {
    fn from(s: String) -> Self {
        Id::new(s)
    }
}

impl<T: ?Sized, const NATIVE: bool> From<&str> for Id<T, NATIVE> {
    fn from(s: &str) -> Self {
        Id::new(s)
    }
}

impl<T: ?Sized, const NATIVE: bool> From<&Id<T, NATIVE>> for Id<T, NATIVE> {
    fn from(id: &Id<T, NATIVE>) -> Self {
        id.clone()
    }
}

impl<T: ?Sized, const NATIVE: bool> From<Id<T, NATIVE>> for String {
    fn from(id: Id<T, NATIVE>) -> Self {
        id.0
    }
}

/// Per-object phantom marker types. The module is private to the
/// crate; only the typedefs below (`AccountId`, `BlobId`, and state tokens) are
/// part of the public API. Per-object typedefs (`EmailId`,
/// `MailboxId`, etc.) live in their own modules under the same
/// "marker private, typedef public" pattern.
mod marker {
    pub(crate) enum Account {}
    pub(crate) enum Blob {}
    pub(crate) struct ObjectState<O: ?Sized>(std::marker::PhantomData<fn() -> O>);
    pub(crate) struct QueryState<O: ?Sized>(std::marker::PhantomData<fn() -> O>);
    pub(crate) enum SessionState {}
    pub(crate) enum PushState {}
}

pub(crate) type AccountId = Id<marker::Account>;
pub(crate) type BlobId = Id<marker::Blob>;
/// State tokens are opaque, but their protocol domains are not interchangeable.
/// Construct from strings only at wire/persistence boundaries; keep the domain
/// through request construction so a query or another object's state cannot guard
/// a write by accident.
pub(crate) type ObjectState<O> = Id<marker::ObjectState<O>, false>;
pub(crate) type QueryState<O> = Id<marker::QueryState<O>, false>;
pub(crate) type SessionState = Id<marker::SessionState, false>;
pub(crate) type PushState = Id<marker::PushState, false>;

impl<T: ?Sized, const NATIVE: bool> std::ops::Deref for Id<T, NATIVE> {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl<T: ?Sized, const NATIVE: bool> PartialEq<str> for Id<T, NATIVE> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<T: ?Sized, const NATIVE: bool> PartialEq<&str> for Id<T, NATIVE> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<T: ?Sized, const NATIVE: bool> PartialEq<Id<T, NATIVE>> for str {
    fn eq(&self, other: &Id<T, NATIVE>) -> bool {
        self == other.as_str()
    }
}

impl<T: ?Sized, const NATIVE: bool> std::borrow::Borrow<str> for Id<T, NATIVE> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn native_ids_reject_the_owner_separator_in_values_and_map_keys() {
        let bad = "shared\u{1f}M1";
        let json = serde_json::to_string(bad).unwrap();
        assert!(serde_json::from_str::<AccountId>(&json).is_err());
        assert!(serde_json::from_str::<BlobId>(&json).is_err());
        let map = serde_json::json!({ bad: true }).to_string();
        assert!(serde_json::from_str::<HashMap<AccountId, bool>>(&map).is_err());
        assert_eq!(
            serde_json::from_str::<AccountId>("\"shared\"")
                .unwrap()
                .as_str(),
            "shared"
        );
    }

    #[test]
    fn opaque_states_do_not_acquire_native_identity_rules() {
        let state = "opaque\u{1f}state";
        let json = serde_json::to_string(state).unwrap();
        assert_eq!(
            serde_json::from_str::<SessionState>(&json)
                .unwrap()
                .as_str(),
            state
        );
        assert_eq!(
            serde_json::from_str::<ObjectState<()>>(&json)
                .unwrap()
                .as_str(),
            state
        );
        assert_eq!(
            serde_json::from_str::<QueryState<()>>(&json)
                .unwrap()
                .as_str(),
            state
        );
        assert_eq!(
            serde_json::from_str::<PushState>(&json).unwrap().as_str(),
            state
        );
    }
    #[cfg(feature = "mail")]
    #[test]
    fn fresh_mail_identities_reject_separator_but_answer_echoes_remain_readable() {
        use crate::email::{Email, EmailId};
        let bad = "shared\u{1f}M1";
        for properties in [
            serde_json::json!({"id": bad}),
            serde_json::json!({"id": "M1", "threadId": bad}),
            serde_json::json!({"id": "M1", "blobId": bad}),
            serde_json::json!({"id": "M1", "mailboxIds": {bad: true}}),
        ] {
            assert!(serde_json::from_value::<Email>(properties).is_err());
        }
        let query = serde_json::json!({"accountId": "primary", "queryState": "q1",
            "canCalculateChanges": false, "position": 0, "ids": [bad]});
        assert!(serde_json::from_value::<crate::core::query::QueryResponse<Email>>(query).is_err());
        let get = serde_json::json!({"accountId": "primary", "state": "s1", "list": [], "notFound": [bad]});
        let get = serde_json::from_value::<crate::core::get::GetResponse<Email>>(get).unwrap();
        assert_eq!(get.not_found(), &[EmailId::new(bad)]);
        let set = serde_json::json!({"accountId": "primary", "oldState": "s1", "newState": "s2",
            "updated": {bad: null}});
        let mut set = serde_json::from_value::<crate::core::set::SetResponse<Email>>(set).unwrap();
        assert!(set.updated(&EmailId::new(bad)).is_ok());
    }
    #[cfg(any(feature = "calendars", feature = "contacts"))]
    #[test]
    fn extensible_object_identities_share_native_validation() {
        let properties = serde_json::json!({"id": "shared\u{1f}native"});
        #[cfg(feature = "calendars")]
        assert!(
            serde_json::from_value::<crate::calendar_event::CalendarEvent>(properties.clone())
                .is_err()
        );
        #[cfg(feature = "contacts")]
        assert!(serde_json::from_value::<crate::contact_card::ContactCard>(properties).is_err());
    }
}
