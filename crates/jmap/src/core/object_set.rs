use serde::{Deserialize, Serialize, ser::SerializeMap};
use std::{collections::HashSet, hash::Hash};

/// JMAP's object-shaped set. Boolean values are a wire encoding, never state.
#[derive(Debug, Clone)]
pub(crate) struct ObjectSet<I: Eq + Hash>(HashSet<I>);

impl<I: Eq + Hash> Default for ObjectSet<I> {
    fn default() -> Self {
        Self(HashSet::new())
    }
}

impl<I: Eq + Hash> FromIterator<I> for ObjectSet<I> {
    fn from_iter<T: IntoIterator<Item = I>>(values: T) -> Self {
        Self(values.into_iter().collect())
    }
}

impl<I: Eq + Hash> IntoIterator for ObjectSet<I> {
    type Item = I;
    type IntoIter = std::collections::hash_set::IntoIter<I>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<I: Eq + Hash> ObjectSet<I> {
    pub(crate) fn iter(&self) -> impl Iterator<Item = &I> {
        self.0.iter()
    }
}

impl<I: Eq + Hash + Serialize> Serialize for ObjectSet<I> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for id in &self.0 {
            map.serialize_entry(id, &true)?;
        }
        map.end()
    }
}

impl<'de, I: Eq + Hash + Deserialize<'de>> Deserialize<'de> for ObjectSet<I> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SetVisitor<I>(std::marker::PhantomData<I>);
        impl<'de, I: Eq + Hash + Deserialize<'de>> serde::de::Visitor<'de> for SetVisitor<I> {
            type Value = ObjectSet<I>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JMAP object set")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut members = HashSet::new();
                while let Some((id, present)) = map.next_entry::<I, bool>()? {
                    // Tolerate false entries as absent, preserving the mail
                    // reader's behavior. Canonical aliases of a true member
                    // cannot erase it through hash-map insertion order.
                    if present {
                        members.insert(id);
                    }
                }
                Ok(ObjectSet(members))
            }
        }
        deserializer.deserialize_map(SetVisitor(std::marker::PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectSet;

    #[test]
    fn wire_booleans_do_not_become_stored_memberships() {
        let set: ObjectSet<String> =
            serde_json::from_value(serde_json::json!({"yes": true, "no": false})).unwrap();
        assert_eq!(
            set.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["yes"]
        );
        assert_eq!(
            serde_json::to_value(set).unwrap(),
            serde_json::json!({"yes": true})
        );
        #[cfg(feature = "mail")]
        let keywords: ObjectSet<crate::email::keyword::Keyword> =
            serde_json::from_value(serde_json::json!({"$Seen": true, "$seen": false})).unwrap();
        #[cfg(feature = "mail")]
        assert_eq!(
            serde_json::to_value(keywords).unwrap(),
            serde_json::json!({"$seen": true})
        );
    }
}
