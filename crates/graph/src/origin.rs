//! Which URLs may carry the account bearer.
//!
//! Net owns exact-origin admission and mints the URL proof used here. Graph
//! supplies the configured base; upload sessions retain their separate policy.
//! AccountNet's unrestricted default still requires this per-leg gate: a
//! composed handle's account bearer authority cannot replace Outlook admission.
//!
//! The rule is exact origin equality with the configured base (scheme, host
//! and port, default ports normalized by the URL parser), with no userinfo in
//! the authority. A URL the crate did not build - a caller's page cursor, a
//! persisted delta link, a server's `@odata.nextLink`, an Autodiscover
//! `RedirectUrl` - reaches the wire only as an [`AdmittedUrl`], and the only
//! way to get one is [`Base::admit`]. Graph documents its links as complete
//! URLs on the service root that issued them, and a national cloud is a
//! separately configured base, so a link on any other origin is not followed.
//!
//! The one URL that may leave for another origin is a OneDrive upload
//! session URL, which never carries the bearer; it is admitted here too, under
//! its own rule ([`Base::admit_upload`]), and reaches the wire only as an
//! [`UploadUrl`].
//!
//! Server links arrive as [`ProviderLink`], which deliberately has no
//! accessor that yields a requestable string: the compiler, not review,
//! stops one from being followed, minted into a caller cursor, or persisted
//! into a checkpoint without admission.

use std::fmt;

use reqwest::Url;
use serde::Deserialize;

/// A configured base URL (the Graph api-base or the Outlook origin), parsed
/// and validated once at construction.
///
/// The client constructors are published and infallible, so an unusable base
/// is carried as a stored refusal instead of a constructor error: every
/// request on that surface fails closed, before any byte is sent.
#[derive(Clone, Debug)]
pub(crate) struct Base {
    /// The base text with trailing slashes trimmed, kept verbatim because
    /// crate-built paths are CONCATENATED onto it (a `/v1.0` path prefix
    /// must survive, which an RFC 3986 join would drop).
    text: String,
    /// The parsed base, or why it is unusable.
    parsed: Result<Url, String>,
}

/// Why a target was not admitted. Carries no URL text: skip and delta tokens
/// are credential-like, so a refusal names origins, not links.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The configured base itself is unusable; nothing on this surface may be
    /// sent.
    InvalidBase(String),
    /// The target resolved to a URL the bearer must not go to.
    Target(String),
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBase(reason) => write!(f, "configured base URL is unusable: {reason}"),
            Self::Target(reason) => write!(f, "request target refused: {reason}"),
        }
    }
}

/// A URL admitted onto a configured origin. Only [`Base::admit`] builds one,
/// and the bearer-carrying send paths accept nothing else.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AdmittedUrl(bifrost_net::AdmittedUrl);

impl AdmittedUrl {
    /// A fixture that trusts its own origin, for tests that build cursor payloads
    /// without a client.
    #[cfg(test)]
    pub(crate) fn for_tests(link: &str) -> Self {
        let mut origins = bifrost_net::TrustedOrigins::default();
        origins.trust(link).expect("test fixture is a URL");
        Self(origins.admit(link).expect("fixture origin was trusted"))
    }

    /// The serialized URL. This exact string is what goes on the wire:
    /// a serialized `Url` reparses to itself, so the URL that was admitted
    /// and the URL `reqwest` sends are the same.
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for AdmittedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A OneDrive upload session URL (`createUploadSession`'s `uploadUrl`),
/// admitted for the anonymous chunk PUTs and the session DELETE. Only
/// [`Base::admit_upload`] builds one, and the anonymous send path accepts
/// nothing else, so no string reaches it without admission. It never carries
/// the bearer: the URL is its own credential.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct UploadUrl(Url);

impl UploadUrl {
    /// The serialized URL, which is exactly what goes on the wire.
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for UploadUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The session's pre-authentication rides in the query; a Debug names
        // the origin and path only.
        let query = if self.0.query().is_some() { "?.." } else { "" };
        write!(
            f,
            "UploadUrl({}{}{query})",
            self.0.origin().ascii_serialization(),
            self.0.path()
        )
    }
}

/// A URL a Graph response handed back (`@odata.nextLink`,
/// `@odata.deltaLink`). Untrusted: it yields a request target only through
/// admission.
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct ProviderLink(String);

impl ProviderLink {
    #[cfg(test)]
    pub(crate) fn for_tests(link: &str) -> Self {
        Self(link.to_string())
    }
}

impl fmt::Debug for ProviderLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Delta and skip tokens are credential-like; a Debug of a whole page
        // must not spill them.
        f.write_str("ProviderLink(..)")
    }
}

impl Base {
    pub(crate) fn parse(raw: impl Into<String>) -> Self {
        let text = raw.into().trim_end_matches('/').to_string();
        // Paths are concatenated onto the base text, so a base carrying a
        // query or fragment would swallow every path appended after it.
        let parsed = validate(&text).and_then(|url| {
            if url.query().is_some() || url.fragment().is_some() || text.contains(['?', '#']) {
                Err("a base URL cannot carry a query or fragment".to_string())
            } else {
                Ok(url)
            }
        });
        Self { text, parsed }
    }

    /// A base that refuses everything, for a surface derived from an
    /// unusable one: an invalid Graph base must not leave a production
    /// Outlook origin standing in for it.
    pub(crate) fn invalid(text: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            parsed: Err(reason.into()),
        }
    }

    /// The base text as configured (trailing slashes trimmed).
    pub(crate) fn as_str(&self) -> &str {
        &self.text
    }

    /// The parsed base, when it is usable.
    pub(crate) fn url(&self) -> Option<&Url> {
        self.parsed.as_ref().ok()
    }

    /// Resolve `target` against this base and admit it, or refuse.
    ///
    /// A target starting with `/` is a path and is concatenated onto the
    /// base; any other target that parses as an absolute URL is taken as
    /// is; anything else is concatenated with a separating slash. The
    /// decision is then made on the FINAL parsed URL, never on the input
    /// text, so a scheme-relative `//elsewhere`, an upper-case `HTTPS://`,
    /// or an authority smuggled through userinfo cannot pass on spelling.
    pub(crate) fn admit(&self, target: &str) -> Result<AdmittedUrl, Refusal> {
        let base = self
            .parsed
            .as_ref()
            .map_err(|reason| Refusal::InvalidBase(reason.clone()))?;
        let resolved = if target.starts_with('/') {
            format!("{}{target}", self.text)
        } else if Url::parse(target).is_ok() {
            target.to_string()
        } else {
            format!("{}/{target}", self.text)
        };
        let url = validate(&resolved).map_err(Refusal::Target)?;
        let mut origins = bifrost_net::TrustedOrigins::default();
        origins
            .trust(base.as_str())
            .map_err(|_| Refusal::InvalidBase("credential origin is unusable".into()))?;
        origins.admit(url.as_str()).map(AdmittedUrl).map_err(|_| {
            Refusal::Target("target is not on the configured credential origin".into())
        })
    }

    /// Admit a server-supplied link. Graph documents these as complete URLs,
    /// so a link that is not an absolute URL is refused rather than resolved
    /// onto the base, where it would become a request Graph never named.
    pub(crate) fn admit_link(&self, link: &ProviderLink) -> Result<AdmittedUrl, Refusal> {
        if self.parsed.is_ok() && Url::parse(&link.0).is_err() {
            return Err(Refusal::Target("link is not an absolute URL".to_string()));
        }
        self.admit(&link.0)
    }

    /// Admit an upload session URL a `createUploadSession` answer named, at
    /// receipt and before any byte is sent to it. `self` is the Graph
    /// api-base.
    ///
    /// The URL carries no bearer, but it receives the attachment bytes and is
    /// itself a credential, so where it points still matters. It must have
    /// the shape every URL here must have ([`validate`]: http or https, a
    /// host, no userinfo, as parsed or as written) and be absolute. Then
    /// `https` is admitted on any host, and plain `http` only on this base's
    /// own origin, which the consumer already trusts with the account bearer
    /// itself, so admitting it widens nothing (a local harness serves Graph
    /// that way). Plain `http` anywhere else is refused: the bytes and the
    /// credential would cross the network in the clear.
    ///
    /// Same-origin admission, the rule for every bearer-carrying URL, does
    /// not fit: real session URLs live on SharePoint and OneDrive hosts,
    /// never on the Graph host. Nor does an allowlist of upload hosts:
    /// Microsoft documents `uploadUrl` as an opaque URL, not a host set, and
    /// the hosts in use vary by tenant, product and national cloud
    /// (`*-my.sharepoint.com`, vanity SharePoint domains, consumer OneDrive,
    /// the sovereign clouds), so a list would refuse working tenants on a
    /// guess. The URL arrives inside a Graph answer the client fetched over
    /// the same TLS channel as its bearer, so a party able to choose the host
    /// could already read that answer; what admission can and does prevent is
    /// the credential leaving over plain http or behind a misleading
    /// authority.
    ///
    /// Every refusal is [`Refusal::Target`] and never quotes the URL; the
    /// caller classifies it (the provider named a destination this client
    /// will not send to).
    pub(crate) fn admit_upload(&self, raw: &str) -> Result<UploadUrl, Refusal> {
        let url = validate(raw).map_err(Refusal::Target)?;
        match url.scheme() {
            "https" => Ok(UploadUrl(url)),
            _ if self.url().is_some_and(|base| base.origin() == url.origin()) => Ok(UploadUrl(url)),
            _ => Err(Refusal::Target(
                "plain http off the configured Graph origin".to_string(),
            )),
        }
    }
}

/// The shape every URL that may carry the bearer must have: http or https,
/// a host, and no userinfo at all.
fn validate(text: &str) -> Result<Url, String> {
    let url = Url::parse(text).map_err(|error| format!("not a URL: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!("scheme {} is not http or https", url.scheme()));
    }
    if url.host().is_none() {
        return Err("URL has no host".to_string());
    }
    // The parser drops an EMPTY userinfo (`https://@host/`), so the parsed
    // fields alone cannot prove the authority carried none; the raw text is
    // checked too.
    if !url.username().is_empty() || url.password().is_some() || raw_authority_has_userinfo(text) {
        return Err("URL carries userinfo".to_string());
    }
    Ok(url)
}

/// Whether the authority as WRITTEN contains an `@`. The authority starts
/// after the scheme's `:` and any run of `/` or `\` (the URL parser accepts
/// both, and a special scheme with no slashes at all), and ends at the first
/// `/`, `\`, `?` or `#`.
fn raw_authority_has_userinfo(text: &str) -> bool {
    let Some((_, rest)) = text.split_once(':') else {
        return false;
    };
    let rest = rest.trim_start_matches(['/', '\\']);
    let end = rest.find(['/', '\\', '?', '#']).unwrap_or(rest.len());
    rest[..end].contains('@')
}

#[cfg(test)]
mod tests {
    use super::{Base, ProviderLink, Refusal};

    fn graph() -> Base {
        Base::parse("https://graph.microsoft.com/v1.0/")
    }

    #[test]
    fn paths_are_concatenated_onto_the_base_path() {
        let base = graph();
        assert_eq!(base.as_str(), "https://graph.microsoft.com/v1.0");
        assert_eq!(
            base.admit("/me/messages?$top=5").expect("path").as_str(),
            "https://graph.microsoft.com/v1.0/me/messages?$top=5"
        );
        assert_eq!(
            base.admit("me/messages").expect("bare path").as_str(),
            "https://graph.microsoft.com/v1.0/me/messages"
        );
    }

    #[test]
    fn a_same_origin_absolute_link_is_admitted() {
        let base = graph();
        let link = "https://graph.microsoft.com/v1.0/me/messages?$skiptoken=abc";
        assert_eq!(base.admit(link).expect("same origin").as_str(), link);
        // The explicit default port is the same origin.
        assert!(
            base.admit("https://graph.microsoft.com:443/v1.0/me")
                .is_ok()
        );
        // Host case is normalized by the parser.
        assert!(base.admit("https://GRAPH.microsoft.com/v1.0/me").is_ok());
    }

    #[test]
    fn a_foreign_origin_is_refused_however_it_is_spelled() {
        let base = graph();
        for target in [
            "https://elsewhere.example/v1.0/me",
            "HTTPS://elsewhere.example/v1.0/me",
            "http://graph.microsoft.com/v1.0/me",
            "https://graph.microsoft.com:8443/v1.0/me",
            "https://graph.microsoft.com.elsewhere.example/v1.0/me",
            "https:elsewhere.example/v1.0/me",
            "https:\\\\elsewhere.example/v1.0/me",
            "ftp://graph.microsoft.com/v1.0/me",
            "https://[::1]/v1.0/me",
        ] {
            assert!(
                matches!(base.admit(target), Err(Refusal::Target(_))),
                "{target} must be refused"
            );
        }
    }

    #[test]
    fn userinfo_is_refused_even_when_the_host_matches() {
        let base = graph();
        for target in [
            "https://graph.microsoft.com@elsewhere.example/v1.0/me",
            "https://user@graph.microsoft.com/v1.0/me",
            "https://user:secret@graph.microsoft.com/v1.0/me",
            "https://@graph.microsoft.com/v1.0/me",
            "https:@graph.microsoft.com/v1.0/me",
        ] {
            assert!(
                matches!(base.admit(target), Err(Refusal::Target(_))),
                "{target} must be refused"
            );
        }
    }

    /// Concatenation keeps a scheme-relative target on the base: it becomes a
    /// path segment, not an authority.
    #[test]
    fn a_scheme_relative_target_stays_on_the_base() {
        let admitted = graph()
            .admit("//elsewhere.example/x")
            .expect("concatenated onto the base");
        assert!(
            admitted
                .as_str()
                .starts_with("https://graph.microsoft.com/v1.0/")
        );
    }

    #[test]
    fn an_unusable_base_refuses_everything() {
        for raw in [
            "not a url",
            "ftp://graph.microsoft.com/v1.0",
            "https://user@graph.microsoft.com/v1.0",
            "https://@graph.microsoft.com/v1.0",
            "mailto:someone@example.com",
            "https://graph.microsoft.com/v1.0?tenant=x",
            "https://graph.microsoft.com/v1.0#frag",
            "https://graph.microsoft.com/v1.0?",
        ] {
            let base = Base::parse(raw);
            assert!(base.url().is_none(), "{raw} must be unusable");
            assert!(
                matches!(base.admit("/me"), Err(Refusal::InvalidBase(_))),
                "{raw} must refuse a path"
            );
        }
    }

    #[test]
    fn ipv6_and_port_bases_compare_by_origin() {
        let base = Base::parse("http://[::1]:8181/graph");
        assert!(base.admit("http://[::1]:8181/graph/next").is_ok());
        assert!(base.admit("http://[::1]:8182/graph/next").is_err());
        assert!(base.admit("http://127.0.0.1:8181/graph/next").is_err());
    }

    /// A server link is a complete URL by Graph's contract; a path or bare
    /// text would otherwise resolve onto the base as a request Graph never
    /// named.
    #[test]
    fn a_provider_link_must_be_absolute() {
        for link in ["/me/messages", "garbage", "", "me/messages?$skip=2"] {
            assert!(
                matches!(
                    graph().admit_link(&ProviderLink::for_tests(link)),
                    Err(Refusal::Target(_))
                ),
                "{link:?} must be refused"
            );
        }
    }

    /// Upload session URLs: https on any host (real session hosts are
    /// SharePoint and OneDrive, never the Graph host), plain http only on
    /// the base's own origin, and the shape rule every other URL here obeys.
    #[test]
    fn upload_admission_takes_https_anywhere_and_http_only_on_the_base_origin() {
        for admitted in [
            "https://contoso-my.sharepoint.com/personal/u/_api/v2.0/uploadSession?tempauth=x",
            "HTTPS://api.onedrive.com/rup/abc",
        ] {
            assert!(graph().admit_upload(admitted).is_ok(), "{admitted}");
        }
        let local = Base::parse("http://127.0.0.1:8181/v1.0");
        assert_eq!(
            local
                .admit_upload("http://127.0.0.1:8181/upload/abc")
                .expect("same origin")
                .as_str(),
            "http://127.0.0.1:8181/upload/abc"
        );
        for refused in [
            "http://127.0.0.1:8182/upload/abc",
            "http://localhost:8181/upload/abc",
            "http://user@127.0.0.1:8181/upload/abc",
            "https://user:secret@upload.example/session/abc",
            "https://upload.example@attacker.example/session/abc",
            // An empty userinfo is dropped by the parser, so only the
            // authority as written shows it; the shared shape rule reads
            // both.
            "https://@upload.example/session/abc",
            "ftp://upload.example/session/abc",
            "/session/abc",
            "not a url",
        ] {
            assert!(
                matches!(local.admit_upload(refused), Err(Refusal::Target(_))),
                "{refused} must be refused"
            );
        }
        // Plain http is refused everywhere against an unusable base, which
        // has no origin to trust.
        assert!(
            Base::parse("not a url")
                .admit_upload("http://127.0.0.1:8181/upload/abc")
                .is_err()
        );
    }

    #[test]
    fn an_upload_url_debug_does_not_spill_the_session_credential() {
        let url = graph()
            .admit_upload("https://upload.example/session/abc?tempauth=secret")
            .expect("https");
        assert!(!format!("{url:?}").contains("secret"), "{url:?}");
    }

    #[test]
    fn a_provider_link_debug_does_not_spill_the_token() {
        let link =
            ProviderLink::for_tests("https://graph.microsoft.com/v1.0/me?$deltatoken=secret");
        assert!(!format!("{link:?}").contains("secret"));
        let admitted = graph().admit_link(&link).expect("same origin");
        assert!(!format!("{admitted:?}").contains("secret"), "{admitted:?}");
    }
}
