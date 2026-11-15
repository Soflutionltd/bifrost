//! Generic paginated result envelope.
//!
//! Used by account search, contact, directory and calendar primitives to return
//! a slice of results plus an opaque cursor a follow-up call can pass
//! back to fetch the next page. The cursor is protocol-owned bytes;
//! the engine and consumers treat it as opaque the same way they
//! treat `OpaqueProgressBytes` in the inventory pipeline.

use crate::error::{AccountError, ErrorScope};

/// Opaque continuation for one caller-owned page walk.
///
/// Pass this value back to the surface that returned it. These bytes are not
/// durable sync state or an inventory checkpoint. Providers own validation of
/// their encoding and surface; a refused cursor restarts only this
/// walk, as an enterprise user expects when saved results become stale.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PageCursor(Vec<u8>);

impl PageCursor {
    /// Restore opaque bytes from consumer storage. Validation belongs to the
    /// provider when the continuation is used.
    #[must_use]
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Borrow the opaque representation for storage or provider decoding.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Take the opaque representation for storage or provider decoding.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for PageCursor {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_bytes(bytes)
    }
}

impl AsRef<[u8]> for PageCursor {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl std::ops::Deref for PageCursor {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl std::fmt::Debug for PageCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCursor")
            .field("len", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// Paginated result envelope.
///
/// Not `#[non_exhaustive]` because protocol Account impls construct
/// `Page` values directly - and deliberately so: adding a lane to this
/// envelope must break every constructor, so each one answers the new
/// question instead of silently defaulting it.
#[derive(Debug, Clone)]
pub struct Page<T> {
    /// True when results were cut short without a continuation. A terminal
    /// page can therefore be non-exhaustive. Independent of failed/skipped lanes.
    pub truncated: bool,
    /// Items in this page.
    pub items: Vec<T>,
    /// Opaque cursor to pass to the next call to resume after the
    /// last item in `items`. `None` when the underlying primitive
    /// returned its final page.
    pub next_cursor: Option<PageCursor>,
    /// Total size of the whole result when known: server-reported, or
    /// counted by a call that enumerated the entire result itself. `None`
    /// when the protocol does not expose an estimate (most do not for
    /// search) or when this call saw only part of the result.
    pub estimated_total: Option<u64>,
    /// Native identifiers of resources the provider fetched for this
    /// page but could not materialize into an item - for example a
    /// per-resource parse failure inside an otherwise successful
    /// multi-status response. Surfacing them lets a consumer tell a
    /// transient per-resource failure apart from a real remote
    /// deletion. Empty for primitives that have no such notion.
    pub failed_ids: Vec<String>,
    /// Scopes a multi-scope walk quarantined instead of visiting - for
    /// example a shared mailbox whose delegate access has been revoked
    /// mid-walk. An entry means the walk did not search (or did not
    /// finish searching) that scope, which is a different fact from
    /// "no matches there": `items` (and every earlier page's items)
    /// remain valid, but absence of results from a skipped scope is
    /// not evidence of absence. Distinct from
    /// `failed_ids`, which names RESOURCES in the item id namespace;
    /// these name the scope that was skipped and carry the classified
    /// failure that caused the skip. Empty for single-scope
    /// primitives and for walks where every scope answered.
    pub skipped_scopes: Vec<SkippedScope>,
}

/// One scope a paginated walk skipped rather than visited.
///
/// Like `Page`, deliberately not `#[non_exhaustive]`: protocol Account
/// impls construct it directly.
#[derive(Debug, Clone)]
pub struct SkippedScope {
    /// Where the skip happened - e.g. `ErrorScope::Owner` for a
    /// shared mailbox the account has lost delegate access to. Kept
    /// mandatory and separate from `error`'s own optional scope: the
    /// whole point of the entry is WHICH scope went unsearched.
    pub scope: ErrorScope,
    /// The classified failure that caused the skip, carrying the full
    /// recovery classification and diagnostics (for a revoked shared
    /// mailbox: terminal `NoPermission` plus the provider's native
    /// code). Advisory here - the walk continued - so it never rides
    /// the call's `Err` arm.
    pub error: AccountError,
}

impl<T> Page<T> {
    /// Single-page result.
    #[must_use]
    pub fn single(
        items: Vec<T>,
        failed_ids: Vec<String>,
        skipped_scopes: Vec<SkippedScope>,
    ) -> Self {
        Self {
            truncated: false,
            items,
            next_cursor: None,
            estimated_total: None,
            failed_ids,
            skipped_scopes,
        }
    }

    /// True iff the protocol indicated this page is the last. Does not imply
    /// exhaustive results: inspect `truncated` and the failure lanes.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.next_cursor.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::{Page, PageCursor};

    #[test]
    fn opaque_page_cursor_preserves_storage_bytes_and_redacts_debug() {
        let bytes = b"opaque-secret\0\xff".to_vec();
        let cursor = PageCursor::from_bytes(bytes.clone());
        assert_eq!(cursor.as_bytes(), bytes);
        let stored = serde_json::to_vec(&cursor).expect("serialize");
        assert_eq!(stored, serde_json::to_vec(&bytes).unwrap());
        let restored: PageCursor = serde_json::from_slice(&stored).expect("restore");
        assert_eq!(restored.into_bytes(), bytes);
        assert!(!format!("{cursor:?}").contains("opaque-secret"));
    }

    #[test]
    fn single_preserves_failed_ids() {
        let page = Page::<()>::single(vec![], vec!["bad".to_owned()], vec![]);
        assert_eq!(page.failed_ids, ["bad"]);
        assert!(!page.truncated);
    }
}
