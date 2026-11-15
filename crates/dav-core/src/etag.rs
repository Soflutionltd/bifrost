//! ETag normalization and conditional-write preparation.
//!
//! Both DAV crates carried these three functions byte for byte.

use reqwest::header::HeaderMap;

/// A validator parsed at a DAV or shared-account string boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EntityTag {
    Strong(StrongTag),
    Weak(String),
}

/// A strong validator. The private field prevents weak If-Match conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrongTag(String);

impl StrongTag {
    #[must_use]
    pub fn to_header(&self) -> String {
        format!("\"{}\"", self.0)
    }
}

impl EntityTag {
    #[must_use]
    pub fn parse(value: &str) -> Self {
        let value = value.trim();
        if value
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
        {
            Self::Weak(value[2..].trim().to_string())
        } else {
            Self::Strong(StrongTag(value.trim_matches('"').to_string()))
        }
    }

    #[must_use]
    pub fn normalized(self) -> String {
        match self {
            Self::Strong(tag) => tag.0,
            Self::Weak(value) => format!("W/{value}"),
        }
    }
}

/// How a PUT is conditioned.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PutCondition {
    /// The resource must not already exist.
    IfNoneMatch,
    /// The resource must still carry this strong validator.
    IfMatch(StrongTag),
    /// Unconditional.
    None,
}

impl PutCondition {
    #[must_use]
    pub fn header(&self) -> Option<(&'static str, String)> {
        match self {
            Self::IfNoneMatch => Some(("If-None-Match", "*".to_string())),
            Self::IfMatch(tag) => Some(("If-Match", tag.to_header())),
            Self::None => None,
        }
    }

    /// A server offering only weak tags cannot protect against lost updates.
    /// Keep saving the enterprise user's changes, as before, rather than
    /// refusing every edit on such a server.
    #[must_use]
    pub fn from_etag(etag: Option<&str>) -> Self {
        match etag.map(EntityTag::parse) {
            Some(EntityTag::Strong(tag)) => Self::IfMatch(tag),
            Some(EntityTag::Weak(_)) | None => Self::None,
        }
    }
}

#[must_use]
pub fn response_etag(headers: &HeaderMap) -> Option<String> {
    headers
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(normalize_http_etag)
}

/// Normalize a wire ETag for snapshot comparison.
///
/// A weak validator KEEPS its `W/` marker, because the marker is what
/// `prepare_if_match` later reads to refuse an unsafe conditional write. A
/// strong one is unquoted so snapshot equality does not depend on quoting.
#[must_use]
pub fn normalize_http_etag(value: &str) -> String {
    EntityTag::parse(value).normalized()
}

/// The `If-Match` value for an etag, or `None` when no conforming one exists.
///
/// RFC 7232 requires STRONG comparison for `If-Match`, so a weak validator has
/// no conforming conditional form and the write must go out unconditional.
/// Against a server that only ever emits weak ETags this means updates have no
/// lost-update protection at all; no better option exists inside HTTP, and a
/// consumer needing the guarantee needs an application-level revision check.
#[must_use]
pub fn prepare_if_match(etag: &str) -> Option<String> {
    match EntityTag::parse(etag) {
        EntityTag::Strong(tag) => Some(tag.to_header()),
        EntityTag::Weak(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_conditions_admit_only_strong_validators() {
        assert_eq!(PutCondition::from_etag(Some("W/\"abc\"")).header(), None);
        assert_eq!(
            PutCondition::from_etag(Some("\"abc\"")).header(),
            Some(("If-Match", "\"abc\"".to_string())),
        );
        assert_eq!(
            PutCondition::IfNoneMatch.header(),
            Some(("If-None-Match", "*".to_string())),
        );
    }

    #[test]
    fn weak_etag_is_never_sent_in_if_match() {
        assert_eq!(prepare_if_match("W/\"abc\""), None);
        assert_eq!(prepare_if_match("w/\"abc\""), None);
        assert_eq!(prepare_if_match("abc").as_deref(), Some("\"abc\""));
        assert_eq!(prepare_if_match("\"abc\"").as_deref(), Some("\"abc\""));
    }

    #[test]
    fn normalization_keeps_the_weak_marker_and_drops_strong_quotes() {
        assert_eq!(normalize_http_etag("  \"abc\" "), "abc");
        assert_eq!(normalize_http_etag("W/ \"abc\""), "W/\"abc\"");
        assert_eq!(normalize_http_etag("w/\"abc\""), "W/\"abc\"");
    }
}
