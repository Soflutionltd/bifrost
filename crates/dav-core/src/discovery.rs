//! The `current-user-principal` discovery walk both dialects run.
//!
//! CalDAV and CardDAV find the principal the same way - an RFC 6764 well-known
//! probe at the origin root, then the configured base URL - and the two crates
//! each carried a copy of the walk, identical apart from their local helper
//! wrappers. The copies had already diverged once: the CardDAV one parsed the
//! probe body outside the lookup and lifted the failure with `?`, so a probe
//! answering `200 text/html` never reached the fallback predicate and failed
//! the open, while the same deployment answering an empty 207 fell back and
//! worked. One walk here removes the second copy that could drift.
//!
//! What stays in the protocol crates is what follows the principal: CalDAV's
//! multi-property principal PROPFIND (calendar home, scheduling addresses,
//! outbox) and CardDAV's addressbook-home lookup.

use bifrost_types::{AccountError, AccountOperation};

use crate::dispatch::DavDispatch;
use crate::error::{missing_field_error, parse_error, should_fallback_discovery};
use crate::multistatus::extract_href_property_in_namespace;
use crate::xml::{DAV_NAMESPACE, resolve_href};

const PROPFIND_PRINCIPAL: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\">\n\
  <D:prop>\n\
    <D:current-user-principal/>\n\
  </D:prop>\n\
</D:propfind>";

impl DavDispatch {
    /// Find the account's `current-user-principal`, absolute.
    ///
    /// Well-known discovery lives at the ORIGIN root (RFC 6764), so the probe
    /// URL is built from the configured base's origin rather than by appending
    /// the suffix to a configured path: the appended URL is not a discovery
    /// endpoint, and a deployment answering it with 401/403 rather than 404
    /// would fail the open before the configured base was ever tried.
    ///
    /// The probe falls back to the configured base when it names no principal,
    /// or when [`should_fallback_discovery`] reads its failure as "this is not a
    /// discovery endpoint". Any other probe failure fails the open. The base leg
    /// never consults the predicate: a failure there is the account's own, and a
    /// base that names no principal is a `Protocol(MissingField)`.
    ///
    /// # Errors
    /// The probe's failure when it is not a fallback answer, the base leg's
    /// failure, or a base answer naming no principal.
    pub async fn discover_current_user_principal(&self) -> Result<String, AccountError> {
        let protocol = self.protocol();
        let from_well_known = match bifrost_net::url::well_known_url(
            self.base_url(),
            protocol.well_known_service(),
        ) {
            Some(well_known) => match self.discover_principal(&well_known).await {
                Ok(principal) => principal,
                Err(error) if should_fallback_discovery(&error, protocol) => None,
                Err(error) => return Err(error),
            },
            None => None,
        };
        if let Some(principal) = from_well_known {
            return Ok(principal);
        }
        // The base answered and its document parsed; it simply names no
        // principal. That is a missing field, not a parse failure.
        self.discover_principal(self.base_url())
            .await?
            .ok_or_else(|| {
                missing_field_error(
                    AccountOperation::Discover,
                    "missing current-user-principal",
                    protocol,
                )
            })
    }

    /// One principal PROPFIND, decode included.
    ///
    /// The decode lives inside the lookup so both the well-known probe and the
    /// configured-base leg produce the same `Result<Option<String>, _>` shape,
    /// and the probe's fallback predicate sees a body that will not parse as the
    /// probe answer it is. The principal href resolves against the EFFECTIVE
    /// request URI, since the walk may have followed a redirect.
    async fn discover_principal(&self, root: &str) -> Result<Option<String>, AccountError> {
        let response = self
            .propfind_raw(root, "0", PROPFIND_PRINCIPAL, AccountOperation::Discover)
            .await?;
        Ok(extract_href_property_in_namespace(
            &response.text,
            DAV_NAMESPACE,
            "current-user-principal",
        )
        .map_err(|error| parse_error(AccountOperation::Discover, error, self.protocol()))?
        .map(|href| resolve_href(&response.url, &href)))
    }
}

#[cfg(test)]
mod tests {
    use bifrost_net::AccountNet;
    use bifrost_net::test_support::Canned;
    use bifrost_types::{AccountErrorKind, AuthErrorKind, ProtocolErrorKind};
    use bytes::Bytes;
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;

    use super::*;
    use crate::DavCredentials;
    use crate::error::DavProtocol;
    use crate::test_support::{dav_redirect, dav_script, scripted_dav_net, transcripts};

    const BASE: &str = "https://dav.example.test/service";
    /// An index page as a front end serves it. The unclosed `<p>` is what makes
    /// it fail the XML parse; a well-formed HTML document decodes as naming no
    /// principal, which is a different fallback arm.
    const INDEX_PAGE: &str = "<html><body><p>It works!</body></html>";
    const PRINCIPAL: &str = "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>";

    fn dispatch(net: AccountNet, protocol: DavProtocol) -> DavDispatch {
        DavDispatch::with_account_net(
            net,
            BASE,
            DavCredentials::Basic {
                username: "user".to_string(),
                password: "pass".to_string(),
            },
            protocol,
        )
    }

    fn answer(status: StatusCode, body: &str) -> Canned {
        Canned::Response {
            status,
            headers: HeaderMap::new(),
            body: Bytes::from(body.to_string()),
        }
    }

    fn well_known(protocol: DavProtocol) -> String {
        format!(
            "https://dav.example.test/.well-known/{}",
            protocol.well_known_service()
        )
    }

    /// Every "not a discovery endpoint" answer falls back to the configured
    /// base, for both dialects, and the probe is the dialect's own origin-rooted
    /// well-known.
    ///
    /// The redirect loop is the case the reclassification of the walk's
    /// redirect failures could have silently dropped: it was
    /// `Request(Malformed)` and fell back through the refused-redirect arm, and
    /// is now `Protocol(ContractViolation)`. Without the predicate's matching
    /// arm it fails the open.
    #[tokio::test]
    async fn every_fallback_answer_retries_the_configured_base() {
        let max_hops = usize::from(bifrost_net::DEFAULT_MAX_HOPS);
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            let probes: Vec<(Vec<Canned>, usize, &str)> = vec![
                (
                    vec![answer(StatusCode::NOT_FOUND, "")],
                    1,
                    "a 404 from the probe",
                ),
                (
                    vec![answer(StatusCode::METHOD_NOT_ALLOWED, "")],
                    1,
                    "a 405 from a front end",
                ),
                (
                    vec![answer(StatusCode::OK, INDEX_PAGE)],
                    1,
                    "an index page that will not parse",
                ),
                (
                    vec![answer(
                        StatusCode::MULTI_STATUS,
                        "<D:multistatus xmlns:D=\"DAV:\"/>",
                    )],
                    1,
                    "an answer naming no principal",
                ),
                (
                    vec![dav_redirect(StatusCode::FOUND, "https://evil.test/dav/")],
                    1,
                    "a redirect to an origin discovery never admitted",
                ),
                (
                    (0..=max_hops)
                        .map(|_| dav_redirect(StatusCode::TEMPORARY_REDIRECT, "/loop"))
                        .collect(),
                    max_hops + 1,
                    "a probe that redirects past the hop cap",
                ),
            ];
            for (mut steps, probe_requests, label) in probes {
                steps.push(answer(StatusCode::MULTI_STATUS, PRINCIPAL));
                let script = dav_script(steps);
                let dav = dispatch(scripted_dav_net(&script), protocol);

                let principal = dav
                    .discover_current_user_principal()
                    .await
                    .unwrap_or_else(|error| panic!("{protocol:?}, {label}: {error:?}"));

                assert_eq!(principal, "https://dav.example.test/principals/ada/");
                let urls = transcripts(&script)
                    .into_iter()
                    .map(|request| request.url)
                    .collect::<Vec<_>>();
                assert_eq!(urls.len(), probe_requests + 1, "{protocol:?}, {label}");
                assert_eq!(urls[0], well_known(protocol), "{protocol:?}, {label}");
                assert_eq!(
                    urls[probe_requests], BASE,
                    "{protocol:?}, {label}: the fallback asks the configured base"
                );
            }
        }
    }

    /// A probe that names the principal ends the walk: the configured base is
    /// never asked.
    #[tokio::test]
    async fn a_probe_naming_the_principal_ends_the_walk() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            let script = dav_script([answer(StatusCode::MULTI_STATUS, PRINCIPAL)]);
            let dav = dispatch(scripted_dav_net(&script), protocol);

            let principal = dav
                .discover_current_user_principal()
                .await
                .expect("the probe answered");

            assert_eq!(principal, "https://dav.example.test/principals/ada/");
            assert_eq!(transcripts(&script).len(), 1, "{protocol:?}");
        }
    }

    /// A credential refusal from the probe fails the open rather than being
    /// buried under a retry of the base. The empty script tail makes a
    /// fallback starve and panic rather than pass.
    #[tokio::test]
    async fn a_refused_credential_on_the_probe_fails_the_open() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            let script = dav_script([answer(StatusCode::UNAUTHORIZED, "")]);
            let dav = dispatch(scripted_dav_net(&script), protocol);

            let error = dav
                .discover_current_user_principal()
                .await
                .expect_err("a 401 is not a fallback answer");

            assert!(
                matches!(
                    error.kind(),
                    AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
                ),
                "{protocol:?}: {:?}",
                error.kind()
            );
            assert_eq!(transcripts(&script).len(), 1, "{protocol:?}");
        }
    }

    /// The base leg never consults the fallback predicate: a body there that
    /// will not parse, or one naming no principal, is the account's own failure.
    ///
    /// Each is classified by what went wrong: a body that will not parse is
    /// `ParseFailed`, and a document that parsed but names no principal is
    /// `MissingField` (it was `ParseFailed` too, which told an operator the XML
    /// was broken when it was merely silent).
    #[tokio::test]
    async fn the_base_leg_fails_on_what_the_probe_would_have_survived() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            for (base_answer, expected, label) in [
                (
                    answer(StatusCode::OK, INDEX_PAGE),
                    ProtocolErrorKind::ParseFailed,
                    "an index page at the configured base",
                ),
                (
                    answer(
                        StatusCode::MULTI_STATUS,
                        "<D:multistatus xmlns:D=\"DAV:\"/>",
                    ),
                    ProtocolErrorKind::MissingField,
                    "a configured base naming no principal",
                ),
            ] {
                let script = dav_script([answer(StatusCode::NOT_FOUND, ""), base_answer]);
                let dav = dispatch(scripted_dav_net(&script), protocol);

                let error = dav
                    .discover_current_user_principal()
                    .await
                    .expect_err(label);

                assert_eq!(
                    error.kind(),
                    &AccountErrorKind::Protocol(expected),
                    "{protocol:?}, {label}"
                );
                assert_eq!(error.protocol(), Some(protocol.protocol()));
                assert_eq!(transcripts(&script).len(), 2, "{protocol:?}, {label}");
            }
        }
    }
}
