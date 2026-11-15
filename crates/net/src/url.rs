//! URL encoding helpers shared by HTTP protocol crates.

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

const COMPONENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'=')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b']')
    .add(b'!')
    .add(b'<')
    .add(b'>')
    .add(b'\\')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Percent-encode one URL query value while preserving RFC 3986
/// unreserved characters.
#[must_use]
pub fn encode_query_value(value: &str) -> String {
    utf8_percent_encode(value, COMPONENT).to_string()
}

/// Percent-encode one URL path component.
///
/// Complete `.` and `..` components are double-escaped because the
/// WHATWG URL parser treats both literal and singly escaped forms as
/// path navigation. Query values must use [`encode_query_value`]
/// instead, where dots have no structural meaning.
#[must_use]
pub fn encode_path_component(value: &str) -> String {
    // WHATWG URL parsing treats literal and percent-encoded `.` and
    // `..` path segments as navigation. Double-escape the percent
    // signs for those two complete components so parsing the full URL
    // cannot resolve a provider id as a parent/current segment.
    if value == "." {
        return "%252E".to_string();
    }
    if value == ".." {
        return "%252E%252E".to_string();
    }
    encode_query_value(value)
}

/// Exact credential origins, independently owned by each protocol leg.
/// Cloning copies the set: discovery must not widen a sibling leg's authority.
#[derive(Clone, Debug, Default)]
pub struct TrustedOrigins(Vec<reqwest::Url>);

impl TrustedOrigins {
    /// Trust the complete origin of an absolute HTTP(S) URL.
    pub fn trust(&mut self, raw: &str) -> Result<(), crate::Error> {
        let mut url = credential_url(raw)?;
        if !self.0.iter().any(|base| base.origin() == url.origin()) {
            url.set_path("/");
            url.set_query(None);
            url.set_fragment(None);
            self.0.push(url);
        }
        Ok(())
    }

    /// Mint proof that this URL belongs to this leg's credential destinations.
    pub fn admit(&self, raw: &str) -> Result<AdmittedUrl, crate::Error> {
        let url = credential_url(raw)?;
        if !self.0.iter().any(|base| base.origin() == url.origin()) {
            return Err(crate::Error::InvalidRequest {
                field: "url",
                detail: "credential destination is not a trusted origin".into(),
            });
        }
        Ok(AdmittedUrl(url))
    }
}

/// A URL minted by net's exact-origin credential admission.
/// This proves membership in the minting policy, not in another account's set.
#[derive(Clone, PartialEq, Eq)]
pub struct AdmittedUrl(reqwest::Url);

impl AdmittedUrl {
    /// The canonical serialization that must be sent without reinterpretation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for AdmittedUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Provider query tokens are credentials too.
        f.write_str("AdmittedUrl(..)")
    }
}

fn credential_url(raw: &str) -> Result<reqwest::Url, crate::Error> {
    let refuse = || crate::Error::InvalidRequest {
        field: "url",
        detail: "credential destination must be an absolute HTTP(S) URL without userinfo".into(),
    };
    let url = reqwest::Url::parse(raw).map_err(|_| refuse())?;
    let authority = raw.split_once(':').map(|(_, rest)| {
        let rest = rest.trim_start_matches(['/', '\\']);
        let end = rest.find(['/', '\\', '?', '#']).unwrap_or(rest.len());
        &rest[..end]
    });
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || authority.is_some_and(|authority| authority.contains('@'))
    {
        return Err(refuse());
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::{encode_path_component, encode_query_value};

    #[test]
    fn credential_origins_compare_scheme_host_and_effective_port() {
        let mut origins = super::TrustedOrigins::default();
        origins.trust("https://EXAMPLE.test:443/base").unwrap();
        assert!(origins.admit("HTTPS://example.test/other?q=token").is_ok());
        for raw in [
            "http://example.test/",
            "https://example.test:444/",
            "https://other.test/",
            "https://@example.test/",
            "https://user@example.test/",
        ] {
            assert!(origins.admit(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn cloned_credential_origins_do_not_widen_sibling_legs() {
        let mut original = super::TrustedOrigins::default();
        original.trust("https://principal.test/").unwrap();
        let mut discovered = original.clone();
        discovered.trust("https://home.test/").unwrap();
        assert!(discovered.admit("https://home.test/cal/").is_ok());
        assert!(original.admit("https://home.test/cal/").is_err());
    }

    #[test]
    fn admitted_url_debug_hides_provider_tokens() {
        let mut origins = super::TrustedOrigins::default();
        origins.trust("https://example.test/").unwrap();
        let admitted = origins.admit("https://example.test/?secret=token").unwrap();
        assert_eq!(format!("{admitted:?}"), "AdmittedUrl(..)");
        assert_eq!(admitted.as_str(), "https://example.test/?secret=token");
    }

    #[test]
    fn unreserved_characters_survive_untouched() {
        assert_eq!(
            encode_query_value("aZ0-._~"),
            "aZ0-._~",
            "RFC 3986 unreserved characters must never be escaped"
        );
        assert_eq!(encode_query_value(""), "");
    }

    /// The delimiters that would otherwise change the meaning of the
    /// spliced URL. `/` splits a path segment, `?` opens a query, `#`
    /// opens a fragment, `&` and `=` split query pairs, `%` would start
    /// a bogus escape.
    #[test]
    fn structural_delimiters_are_escaped() {
        assert_eq!(encode_query_value("a/b"), "a%2Fb");
        assert_eq!(encode_query_value("a?b"), "a%3Fb");
        assert_eq!(encode_query_value("a#b"), "a%23b");
        assert_eq!(encode_query_value("a&b=c"), "a%26b%3Dc");
        assert_eq!(encode_query_value("100%"), "100%25");
        assert_eq!(encode_query_value("a b"), "a%20b");
        assert_eq!(encode_query_value("a+b"), "a%2Bb");
        assert_eq!(encode_query_value("user@host"), "user%40host");
        assert_eq!(encode_query_value("a:b"), "a%3Ab");
    }

    #[test]
    fn control_characters_and_non_ascii_are_escaped() {
        assert_eq!(encode_query_value("a\nb"), "a%0Ab");
        assert_eq!(encode_query_value("a\0b"), "a%00b");
        assert_eq!(encode_query_value("\u{7f}"), "%7F");
        assert_eq!(
            encode_query_value("naïve"),
            "na%C3%AFve",
            "non-ASCII is UTF-8 percent-encoded byte by byte"
        );
        assert_eq!(encode_query_value("日本"), "%E6%97%A5%E6%9C%AC");
    }

    #[test]
    fn every_non_unreserved_ascii_character_is_escaped() {
        assert_eq!(encode_query_value("a\\b"), "a%5Cb");
        assert_eq!(encode_query_value("a<b>c"), "a%3Cb%3Ec");
        assert_eq!(encode_query_value("a{b}c"), "a%7Bb%7Dc");
        assert_eq!(encode_query_value("a|b"), "a%7Cb");
        assert_eq!(encode_query_value("a^b`c"), "a%5Eb%60c");
        assert_eq!(encode_query_value("a!b"), "a%21b");
    }

    #[test]
    fn complete_dot_segments_cannot_navigate_the_parsed_url() {
        assert_eq!(encode_path_component("."), "%252E");
        assert_eq!(encode_path_component(".."), "%252E%252E");
        let resolved = reqwest::Url::parse("https://h.example/a/b/")
            .expect("base parses")
            .join(&encode_path_component(".."))
            .expect("join succeeds");
        assert_eq!(
            resolved.path(),
            "/a/b/%252E%252E",
            "the encoded provider id remains in its original path position"
        );
    }

    #[test]
    fn complete_dot_query_values_are_not_double_escaped() {
        assert_eq!(encode_query_value("."), ".");
        assert_eq!(encode_query_value(".."), "..");
    }

    /// Idempotence check: encoding an already-encoded value double
    /// escapes the `%`, so callers must encode exactly once.
    #[test]
    fn encoding_is_not_idempotent() {
        assert_eq!(encode_query_value(&encode_query_value("a/b")), "a%252Fb");
    }
}
