use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A three-state field for JMAP properties that distinguish between
/// "not set", "explicitly null", and "has a value".
///
/// This replaces `Option<Option<T>>` with clearer semantics:
/// - `Field::Omitted` - property was not included (skip serialization)
/// - `Field::Null` - property was explicitly set to null
/// - `Field::Value(T)` - property has a value
///
/// Serializes as: omitted → absent, null → JSON null, value → JSON value.
/// Deserializes from: absent → Omitted, null → Null, value → Value(T).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub(crate) enum Field<T> {
    /// Property not included / not requested.
    #[default]
    Omitted,
    /// Property explicitly set to null.
    Null,
    /// Property has a value.
    Value(T),
}

impl<T> Field<T> {
    /// Returns `true` if the field is `Omitted`.
    pub(crate) fn is_omitted(&self) -> bool {
        matches!(self, Field::Omitted)
    }

    /// Returns `true` if the field is `Null`.
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Field::Null)
    }

    /// Returns the contained value, or `None` if `Omitted` or `Null`.
    pub(crate) fn as_value(&self) -> Option<&T> {
        match self {
            Field::Value(v) => Some(v),
            _ => None,
        }
    }

    /// Converts to `Option<Option<&T>>` for backward compatibility.
    /// `Omitted` → `None`, `Null` → `Some(None)`, `Value` → `Some(Some(&v))`.
    pub(crate) fn as_option(&self) -> Option<Option<&T>> {
        match self {
            Field::Omitted => None,
            Field::Null => Some(None),
            Field::Value(v) => Some(Some(v)),
        }
    }

    pub(crate) fn map<U>(self, f: impl FnOnce(T) -> U) -> Field<U> {
        match self {
            Field::Omitted => Field::Omitted,
            Field::Null => Field::Null,
            Field::Value(v) => Field::Value(f(v)),
        }
    }
}

impl<T: Serialize> Serialize for Field<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Field::Omitted => Err(serde::ser::Error::custom(
                "Field::Omitted must be skipped via #[serde(skip_serializing_if = \"Field::is_omitted\")]",
            )),
            Field::Null => serializer.serialize_none(),
            Field::Value(v) => v.serialize(serializer),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Field<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // When serde calls this, the field is present in the JSON.
        // If the value is null, Option<T> deserializes to None.
        // If the value is non-null, Option<T> deserializes to Some(T).
        let opt = Option::<T>::deserialize(deserializer)?;
        Ok(match opt {
            None => Field::Null,
            Some(v) => Field::Value(v),
        })
    }
}

/// Use as `#[serde(skip_serializing_if = "Field::is_omitted")]`
impl<T> Field<T> {
    // The is_omitted method above works for skip_serializing_if
}

/// Helper for skip_serializing_if on Field<T>
pub(crate) fn field_is_omitted<T>(field: &Field<T>) -> bool {
    field.is_omitted()
}

impl<T> From<T> for Field<T> {
    fn from(value: T) -> Self {
        Self::Value(value)
    }
}

// At a setter boundary None explicitly clears a nullable property; untouched
// payloads use Omitted. Empty strings, collections and epoch dates are values.
impl<T> From<Option<T>> for Field<T> {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => Self::Value(value),
            None => Self::Null,
        }
    }
}
