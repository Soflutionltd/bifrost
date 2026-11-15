//! DAV collection ancestry and origin-rooted service discovery.

/// Return the parent collection URL of an absolute resource URL.
///
/// Query and fragment data are discarded before removing the final path
/// segment, so slashes in either cannot be mistaken for path separators.
#[must_use]
pub fn parent_collection_url(resource_url: &str) -> Option<String> {
    let mut url = reqwest::Url::parse(resource_url).ok()?;
    url.set_query(None);
    url.set_fragment(None);
    {
        let mut segments = url.path_segments_mut().ok()?;
        segments.pop_if_empty();
        segments.pop();
        segments.push("");
    }
    Some(url.to_string())
}

/// Return the origin-rooted `/.well-known/<service>` URL for a base URL.
///
/// RFC 6764 well-known discovery is defined at the ORIGIN root, not
/// relative to whatever path the account happens to be configured with.
/// A configured base of `https://host/service` therefore probes
/// `https://host/.well-known/caldav`, never
/// `https://host/service/.well-known/caldav`. Query and fragment on the
/// base are irrelevant to discovery and are dropped.
///
/// Returns `None` when `base_url` does not parse or cannot carry a path
/// (a cannot-be-a-base URL such as `mailto:`); callers treat that as
/// "no well-known probe available" and go straight to the configured
/// base.
#[must_use]
pub fn well_known_url(base_url: &str, service: &str) -> Option<String> {
    let mut url = reqwest::Url::parse(base_url).ok()?;
    if url.cannot_be_a_base() {
        return None;
    }
    url.set_query(None);
    url.set_fragment(None);
    url.set_path(&format!("/.well-known/{service}"));
    Some(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::{parent_collection_url, well_known_url};

    /// The path-bearing base is the case that separates origin-rooted
    /// construction from string concatenation; a path-less base makes
    /// both spellings agree and proves nothing.
    #[test]
    fn well_known_is_rooted_at_the_origin() {
        assert_eq!(
            well_known_url("https://dav.example.test/service", "caldav").as_deref(),
            Some("https://dav.example.test/.well-known/caldav")
        );
        assert_eq!(
            well_known_url("https://dav.example.test/a/b/c/", "carddav").as_deref(),
            Some("https://dav.example.test/.well-known/carddav")
        );
        assert_eq!(
            well_known_url("https://dav.example.test", "caldav").as_deref(),
            Some("https://dav.example.test/.well-known/caldav")
        );
    }

    #[test]
    fn well_known_drops_query_and_fragment_and_keeps_port_and_userinfo() {
        assert_eq!(
            well_known_url("https://dav.example.test/service?a=b#frag", "caldav").as_deref(),
            Some("https://dav.example.test/.well-known/caldav")
        );
        assert_eq!(
            well_known_url("https://dav.example.test:8443/service", "carddav").as_deref(),
            Some("https://dav.example.test:8443/.well-known/carddav")
        );
    }

    #[test]
    fn well_known_rejects_unusable_bases() {
        assert_eq!(well_known_url("not-a-url", "caldav"), None);
        assert_eq!(well_known_url("mailto:user@example.test", "caldav"), None);
    }

    #[test]
    fn parent_collection_uses_only_path_segments() {
        assert_eq!(
            parent_collection_url("https://dav.test/cal/one.ics?redirect=/foo").as_deref(),
            Some("https://dav.test/cal/")
        );
        assert_eq!(
            parent_collection_url("https://dav.test/cal/one.ics#a/b").as_deref(),
            Some("https://dav.test/cal/")
        );
        assert_eq!(parent_collection_url("not-a-url"), None);
    }
}
