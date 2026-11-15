//! Search request AST.
//!
//! Canonical intersection of JMAP `Email/query` `FilterCondition`,
//! IMAP `UID SEARCH`, Gmail query strings, and Graph OData
//! `$filter`. Operators outside this set are reached via the
//! protocol-specific `provider_query` escape on the request.
//!
//! Each protocol Account impl translates the AST into the
//! provider's native shape; `provider_query`, when set, is
//! concatenated as the protocol expects (Gmail and Graph append a
//! raw query string; JMAP submits it as a `text` filter; IMAP
//! ignores it unless the consumer wired a Sieve-flavored
//! interpretation in their adapter).

use std::time::SystemTime;

use crate::container::ContainerId;
use crate::ids::LabelId;

/// Search filter AST.
///
/// `And`, `Or`, and `Not` recurse for boolean composition. Leaf
/// variants are the canonical intersection of provider operators.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SearchFilter {
    /// Free-text in the `From` header.
    From(String),
    /// Free-text in `To` / `Cc` / `Bcc`.
    To(String),
    /// Free-text in the `Subject` header.
    Subject(String),
    /// Free-text anywhere in the body.
    Body(String),
    /// Filename or content-type match on attachments. The string is
    /// matched against either the filename suffix or the MIME type
    /// depending on protocol; consumers that need precision use
    /// `provider_query` instead.
    Has(String),
    /// Restrict to a specific container (folder, mailbox, or label
    /// rendered as a container).
    In(ContainerId),
    /// Restrict to messages carrying a specific label.
    Labeled(LabelId),
    /// Inclusive-exclusive date range against the message's send
    /// time. Either bound may be `None` for an open-ended range.
    DateRange {
        after: Option<SystemTime>,
        before: Option<SystemTime>,
    },
    /// All branches must match.
    And(Vec<SearchFilter>),
    /// At least one branch must match.
    Or(Vec<SearchFilter>),
    /// Negate the inner branch.
    Not(Box<SearchFilter>),
}

/// Search request shape passed to `Account::search` and
/// `Account::search_messages`.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SearchRequest {
    /// Structured filter. `None` means "every message"; protocols
    /// that disallow that escape via `provider_query`.
    pub filter: Option<SearchFilter>,
    /// Max items per page. `None` lets the protocol pick its
    /// default page size.
    pub limit: Option<u32>,
    /// Opaque cursor returned by a prior call's `Page::next_cursor`.
    /// `None` starts a fresh search.
    pub page_cursor: Option<crate::PageCursor>,
    /// Provider-specific raw query, appended to or substituted for
    /// the structured filter at the protocol layer. Lets consumers
    /// reach Gmail-specific operators like `larger:5M` or Graph
    /// `$search` strings without forcing every other provider to
    /// model them.
    pub provider_query: Option<String>,
}

impl SearchRequest {
    /// Shorthand for "structured filter only".
    #[must_use]
    pub fn filter(f: SearchFilter) -> Self {
        Self {
            filter: Some(f),
            ..Default::default()
        }
    }

    /// Shorthand for "raw provider query only".
    #[must_use]
    pub fn provider(query: impl Into<String>) -> Self {
        Self {
            provider_query: Some(query.into()),
            ..Default::default()
        }
    }
}
