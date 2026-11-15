use std::collections::HashMap;
use std::hash::Hash;

use serde_json::{Map, Value};

/// A wire PatchObject path. Build dynamic paths from literal segments so an
/// id containing '/' or '~' cannot change the property being edited.
#[derive(Debug, Clone)]
pub(crate) struct PatchPath(String);

impl PatchPath {
    pub(crate) fn new(segments: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        Self(
            segments
                .into_iter()
                .map(|s| super::set::escape_json_pointer_token(s.as_ref()))
                .collect::<Vec<_>>()
                .join("/"),
        )
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

/// Existing callers may already supply RFC 6901 encoded paths (including the
/// account layer's participant edits). Preserve that door; dynamic tokens
/// should use new(), rather than escaping a pre-encoded path a second time.
impl std::fmt::Display for PatchPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PatchPath {
    fn from(path: String) -> Self {
        Self(path)
    }
}

impl From<&str> for PatchPath {
    fn from(path: &str) -> Self {
        Self(path.to_owned())
    }
}

pub(crate) trait PatchEntries<V> {
    fn retain_entries(&mut self, keep: impl FnMut(&String, &mut V) -> bool);
    fn put(&mut self, key: String, value: V);
}

impl<V> PatchEntries<V> for HashMap<String, V> {
    fn retain_entries(&mut self, keep: impl FnMut(&String, &mut V) -> bool) {
        self.retain(keep);
    }
    fn put(&mut self, key: String, value: V) {
        self.insert(key, value);
    }
}

impl PatchEntries<Value> for Map<String, Value> {
    fn retain_entries(&mut self, mut keep: impl FnMut(&String, &mut Value) -> bool) {
        // serde_json's retain passes an immutable value.
        let keys: Vec<_> = self.keys().cloned().collect();
        for key in keys {
            if !keep(&key, self.get_mut(&key).expect("existing key")) {
                self.remove(&key);
            }
        }
    }
    fn put(&mut self, key: String, value: Value) {
        self.insert(key, value);
    }
}

/// RFC 8620 forbids both a property and a path inside it. The latest setter
/// wins over overlapping edits; sibling paths survive, preserving unrelated
/// memberships as an enterprise mail user expects when filing one message.
pub(crate) fn insert<V>(entries: &mut impl PatchEntries<V>, path: impl Into<PatchPath>, value: V) {
    let path = path.into().into_string();
    entries.retain_entries(|key, _| !overlaps(key, &path));
    entries.put(path, value);
}

fn overlaps(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|tail| tail.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|tail| tail.starts_with('/'))
}

pub(crate) fn remove<V>(entries: &mut impl PatchEntries<V>, path: &str) {
    entries.retain_entries(|key, _| !overlaps(key, path));
}

/// Complete create-time Id[Boolean] sets contain only true members.
pub(crate) struct IdSet;

impl IdSet {
    pub(crate) fn collect<I: Eq + Hash>(
        ids: impl IntoIterator<Item = I>,
    ) -> super::object_set::ObjectSet<I> {
        ids.into_iter().collect()
    }

    pub(crate) fn replace(
        properties: &mut Map<String, Value>,
        property: &str,
        ids: impl IntoIterator<Item = String>,
    ) {
        properties.insert(
            property.into(),
            Value::Object(ids.into_iter().map(|id| (id, Value::Bool(true))).collect()),
        );
    }

    pub(crate) fn member(
        properties: &mut Map<String, Value>,
        property: &str,
        id: String,
        present: bool,
    ) {
        let entry = properties
            .entry(property)
            .or_insert_with(|| Value::Object(Map::new()));
        // A typed setter supersedes a raw null or malformed extension value.
        if !entry.is_object() {
            *entry = Value::Object(Map::new());
        }
        let map = entry.as_object_mut().expect("object installed above");
        if present {
            map.insert(id, Value::Bool(true));
        } else {
            map.remove(&id);
        }
    }
}

pub(crate) struct IdSetPatch;

impl IdSetPatch {
    pub(crate) fn member_entries(
        entries: &mut impl PatchEntries<Value>,
        property: &str,
        id: &str,
        present: bool,
    ) {
        insert(
            entries,
            PatchPath::new([property, id]),
            if present {
                Value::Bool(true)
            } else {
                Value::Null
            },
        );
    }

    pub(crate) fn replace(
        properties: &mut Map<String, Value>,
        property: &str,
        ids: impl IntoIterator<Item = String>,
    ) {
        remove(properties, property);
        IdSet::replace(properties, property, ids);
    }

    pub(crate) fn member(
        properties: &mut Map<String, Value>,
        property: &str,
        id: String,
        present: bool,
    ) {
        Self::member_entries(properties, property, &id, present);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_membership_recovers_from_raw_null_and_patch_keeps_siblings() {
        let mut map = Map::from_iter([("calendarIds".into(), Value::Null)]);
        IdSet::member(&mut map, "calendarIds", "a/b".into(), true);
        assert_eq!(
            Value::Object(map.clone()),
            json!({"calendarIds": {"a/b": true}})
        );
        IdSetPatch::member(&mut map, "calendarIds", "a/b".into(), false);
        IdSetPatch::member(&mut map, "calendarIds", "other".into(), true);
        assert_eq!(
            Value::Object(map),
            json!({"calendarIds/a~1b": null, "calendarIds/other": true})
        );
    }

    #[test]
    fn paths_escape_tokens_and_replace_only_overlapping_edits() {
        let mut map = Map::new();
        insert(&mut map, "members", json!({"old": true}));
        insert(&mut map, PatchPath::new(["members", "a/b~c"]), json!(true));
        insert(
            &mut map,
            PatchPath::new(["members", "sibling"]),
            Value::Null,
        );
        assert_eq!(
            Value::Object(map.clone()),
            json!({"members/a~1b~0c": true, "members/sibling": null})
        );
        insert(&mut map, "members", json!({}));
        assert_eq!(Value::Object(map), json!({"members": {}}));
    }
}
