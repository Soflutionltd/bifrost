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
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct Id<T: ?Sized>(String, #[serde(skip)] std::marker::PhantomData<T>);

// Manual Clone / PartialEq / Eq / Hash impls: the derived forms each
// require `T: Trait`, but `T` is a phantom marker (often an
// uninhabited enum), so any trait bound on it is bogus. The string
// already implements all of these and `PhantomData` implements them
// unconditionally.
impl<T: ?Sized> Clone for Id<T> {
    fn clone(&self) -> Self {
        Id(self.0.clone(), std::marker::PhantomData)
    }
}

impl<T: ?Sized> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: ?Sized> Eq for Id<T> {}

impl<T: ?Sized> std::hash::Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T: ?Sized> Id<T> {
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

impl<T: ?Sized> fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id({:?})", self.0)
    }
}

impl<T: ?Sized> fmt::Display for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<T: ?Sized> AsRef<str> for Id<T> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<T: ?Sized> From<String> for Id<T> {
    fn from(s: String) -> Self {
        Id::new(s)
    }
}

impl<T: ?Sized> From<&str> for Id<T> {
    fn from(s: &str) -> Self {
        Id::new(s)
    }
}

impl<T: ?Sized> From<&Id<T>> for Id<T> {
    fn from(id: &Id<T>) -> Self {
        id.clone()
    }
}

impl<T: ?Sized> From<Id<T>> for String {
    fn from(id: Id<T>) -> Self {
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
pub(crate) type ObjectState<O> = Id<marker::ObjectState<O>>;
pub(crate) type QueryState<O> = Id<marker::QueryState<O>>;
pub(crate) type SessionState = Id<marker::SessionState>;
pub(crate) type PushState = Id<marker::PushState>;

impl<T: ?Sized> std::ops::Deref for Id<T> {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl<T: ?Sized> PartialEq<str> for Id<T> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<T: ?Sized> PartialEq<&str> for Id<T> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<T: ?Sized> PartialEq<Id<T>> for str {
    fn eq(&self, other: &Id<T>) -> bool {
        self == other.as_str()
    }
}

impl<T: ?Sized> std::borrow::Borrow<str> for Id<T> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
