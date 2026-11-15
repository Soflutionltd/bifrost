//! Projection-aware views of a Graph item. Decode individual items after batch
//! correlation, never the whole delta page into a mandatory message schema.
//! Deltas also contain events, contacts and sparse tombstones. A malformed item
//! must retain its siblings and checkpoint barrier; an Outlook user must not
//! silently lose a page because one provider value cannot be represented.

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy)]
pub(super) enum DeltaState {
    Live,
    Removed,
}

/// Tolerant observation used by sparse delta and metadata projections. Missing
/// and invalid optional fields retain the existing absent-field semantics.
/// Keep provider text (including unknown flag statuses) for fingerprints;
/// writable flag canonicalization has a different contract.
pub(super) struct GraphItem<'a> {
    pub(super) id: Option<&'a str>,
    pub(super) parent_folder: Option<&'a str>,
    pub(super) conversation: Option<&'a str>,
    pub(super) etag: Option<&'a str>,
    pub(super) change_key: Option<&'a str>,
    pub(super) state: DeltaState,
    pub(super) flags: ObservedFlags<'a>,
}

pub(super) struct ObservedFlags<'a> {
    pub(super) read: Option<bool>,
    pub(super) status: Option<&'a str>,
    pub(super) categories: Categories<'a>,
}

/// Borrow the provider array without allocating during identity-only reads.
/// Iteration exposes only the string observations admitted by sparse decoding.
pub(super) struct Categories<'a>(&'a [Value]);

impl<'a> IntoIterator for Categories<'a> {
    type Item = &'a str;
    type IntoIter =
        std::iter::FilterMap<std::slice::Iter<'a, Value>, fn(&'a Value) -> Option<&'a str>>;

    fn into_iter(self) -> Self::IntoIter {
        // A fn item does not coerce to the fn-pointer type parameter inside
        // FilterMap on its own; name the pointer type explicitly.
        let as_str: fn(&'a Value) -> Option<&'a str> = Value::as_str;
        self.0.iter().filter_map(as_str)
    }
}

impl<'a> GraphItem<'a> {
    pub(super) fn decode(value: &'a Value) -> Self {
        let text = |name| value.get(name).and_then(Value::as_str);
        Self {
            id: text("id"),
            parent_folder: text("parentFolderId"),
            conversation: text("conversationId"),
            etag: text("@odata.etag"),
            change_key: text("changeKey"),
            // Preserve historical presence-based tombstones, including a null
            // marker. Tightening this must not turn old removals into upserts.
            state: if value.get("@removed").is_some() {
                DeltaState::Removed
            } else {
                DeltaState::Live
            },
            flags: ObservedFlags {
                read: value.get("isRead").and_then(Value::as_bool),
                status: value
                    .get("flag")
                    .and_then(|flag| flag.get("flagStatus"))
                    .and_then(Value::as_str),
                categories: Categories(
                    value
                        .get("categories")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]),
                ),
            },
        }
    }
}

/// A FlagsOnly observation promises completeness, unlike a sparse delta item.
/// Deserialization enforces types and presence before the etag is trusted.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CompleteFlags {
    pub(super) is_read: bool,
    pub(super) flag: CompleteFlag,
    pub(super) categories: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CompleteFlag {
    pub(super) flag_status: FlagStatus,
}

#[derive(Clone, Copy)]
pub(super) enum FlagStatus {
    NotFlagged,
    Flagged,
    Complete,
}

impl<'de> Deserialize<'de> for FlagStatus {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let status = String::deserialize(deserializer)?;
        if status.eq_ignore_ascii_case("notFlagged") {
            Ok(Self::NotFlagged)
        } else if status.eq_ignore_ascii_case("flagged") {
            Ok(Self::Flagged)
        } else if status.eq_ignore_ascii_case("complete") {
            Ok(Self::Complete)
        } else {
            Err(serde::de::Error::custom("unknown flagStatus"))
        }
    }
}

impl CompleteFlags {
    pub(super) fn tokens(self) -> std::collections::HashSet<String> {
        use super::flags::GraphFlag;
        let mut flags = std::collections::HashSet::new();
        if self.is_read {
            flags.insert(GraphFlag::Read.token());
        }
        if matches!(self.flag.flag_status, FlagStatus::Flagged) {
            flags.insert(GraphFlag::Flagged.token());
        }
        flags.extend(
            self.categories
                .iter()
                .map(|category| GraphFlag::Category(category).token()),
        );
        flags
    }
}
