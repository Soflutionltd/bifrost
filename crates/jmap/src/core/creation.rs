//! Request-owned creation ids and their wire references, separate from server ids.

use std::collections::HashMap;

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub(crate) struct CreationId(String);

impl CreationId {
    /// Allocate against the owning map, including caller-selected ids.
    pub(crate) fn next<V>(prefix: char, existing: Option<&HashMap<String, V>>) -> Self {
        let mut index = existing.map_or(0, HashMap::len);
        loop {
            let id = format!("{prefix}{index}");
            if !existing.is_some_and(|map| map.contains_key(&id)) {
                return Self(id);
            }
            index += 1;
        }
    }

    /// A caller-selected id, or one returned by another request in this batch.
    pub(crate) fn named(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub(crate) fn reference(&self) -> CreationRef {
        CreationRef(format!("#{}", self.0))
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for CreationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub(crate) struct CreationRef(String);

/// Only client-sent fields may contain references. Server-returned ids retain
/// their object-specific Id<T> types and never use this union.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(untagged)]
pub(crate) enum IdOrRef<I> {
    Id(I),
    Reference(CreationRef),
}

// Preserve the create payload's existing empty-id omission policy for real ids.
// None is an explicit null parent; the untouched create uses an empty id.
pub(crate) fn skip_if_empty_id_or_ref<I: AsRef<str>>(id: &Option<IdOrRef<I>>) -> bool {
    match id {
        None => false,
        Some(IdOrRef::Id(id)) => id.as_ref().is_empty(),
        Some(IdOrRef::Reference(_)) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{CreationId, IdOrRef};
    use crate::core::id::AccountId;
    use serde_json::json;

    #[test]
    fn a_creation_reference_serializes_as_a_string_including_in_map_keys() {
        let reference = CreationId::named("c0").reference();
        assert_eq!(serde_json::to_value(&reference).unwrap(), "#c0");
        let key = IdOrRef::<AccountId>::Reference(reference);
        let map = std::collections::HashMap::from([(key, true)]);
        assert_eq!(serde_json::to_value(map).unwrap(), json!({"#c0": true}));
        assert_eq!(
            serde_json::to_value(IdOrRef::Id(AccountId::new("server0"))).unwrap(),
            "server0"
        );
    }
}
