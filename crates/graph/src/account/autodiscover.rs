//! Exchange Autodiscover layer: public-folder routing discovery and
//! delegate (alternative-mailbox) enumeration.
//!
//! The two response parsers derive from ratatoskr's `autodiscover.rs` (pure
//! quick-xml), since hardened to refuse a malformed or truncated document
//! rather than read it as an empty answer. The HTTP entry
//! points are reshaped onto `AccountNet` (the Bearer is supplied by the
//! net layer, never a hand-built header) and return `AccountError`
//! through the existing REST error path - Autodiscover is REST-over-HTTP
//! (SOAP body, but classified by HTTP status), not SOAP-faulting EWS, so
//! it does not route through `ews_error_to_account_error`.

use bifrost_net::error::cap_status_body;
use bifrost_types::{AccountError, AccountOperation, ProtocolErrorKind};
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::Event;

use super::GraphAccount;
use super::cursor::PublicFolderRouting;
use super::graph_error::{
    GraphErrorContext, autodiscover_error_code_to_account_error,
    autodiscover_pox_error_to_account_error, into_account_error, provider_answer_violation,
    response_to_account_error_pub,
};
use crate::client::{AuxTarget, Resource};
use crate::error::{GraphError, GraphResponseError};
use crate::ews::try_push_general_ref;
use crate::origin::AdmittedUrl;

/// The POX (`autodiscover.xml`) Autodiscover path under the Outlook origin.
/// Resolved against the client's Outlook base rather than hardcoded so the
/// harness api-base override reaches Autodiscover too: production
/// Autodiscover lives on `outlook.office365.com`, not on the Graph host, so
/// redirecting only the Graph base left delegate/public-folder discovery
/// hitting the real service.
const AUTODISCOVER_XML_PATH: &str = "/autodiscover/autodiscover.xml";

/// The SOAP (`autodiscover.svc` / `GetUserSettings`) Autodiscover path under
/// the Outlook origin. See [`AUTODISCOVER_XML_PATH`].
const AUTODISCOVER_SOAP_PATH: &str = "/autodiscover/autodiscover.svc";

const REDIRECT_ADDRESS: &str = "RedirectAddress";
const REDIRECT_URL: &str = "RedirectUrl";

/// How many in-body redirects one `GetUserSettings` lookup follows before
/// giving up: a safety cap this crate imposes on itself.
const MAX_REDIRECTS: usize = 5;

/// The POX Autodiscover endpoint under a given Outlook origin.
#[cfg(test)]
pub(crate) fn autodiscover_xml_url(outlook_base: &str) -> String {
    format!(
        "{}{AUTODISCOVER_XML_PATH}",
        outlook_base.trim_end_matches('/')
    )
}

/// The SOAP Autodiscover endpoint under a given Outlook origin.
#[cfg(test)]
pub(crate) fn autodiscover_soap_url(outlook_base: &str) -> String {
    format!(
        "{}{AUTODISCOVER_SOAP_PATH}",
        outlook_base.trim_end_matches('/')
    )
}

/// What one `GetUserSettings` answer asks the lookup to do next.
enum SoapStep {
    /// The answer carries the settings.
    Settings,
    /// `RedirectAddress`: query this mailbox against the same endpoint.
    Mailbox(String),
    /// `RedirectUrl`: query the same mailbox at this admitted endpoint.
    Endpoint(AdmittedUrl),
}

/// POX `<Action>` values that redirect.
const POX_REDIRECT_ADDR: &str = "redirectAddr";
const POX_REDIRECT_URL: &str = "redirectUrl";

/// What one POX (`autodiscover.xml`) answer asks the delegate lookup to do.
enum PoxStep {
    /// The answer is the mailbox list (possibly empty).
    Mailboxes(Vec<SharedMailbox>),
    /// `redirectAddr`: ask again for this mailbox at the same endpoint.
    Mailbox(String),
    /// `redirectUrl`: ask again for the same mailbox at this endpoint.
    Endpoint(AdmittedUrl),
}

/// The (mailbox, endpoint) pairs one Autodiscover lookup has asked, so a
/// redirect back to one of them is caught as the loop it is before the
/// repeat is sent. Shared by the SOAP and POX lookups, which follow
/// redirects under the same rules.
#[derive(Default)]
struct RedirectWalk {
    asked: Vec<(String, AdmittedUrl)>,
}

impl RedirectWalk {
    /// Record the next request, or refuse it as a redirect loop: a pair
    /// already asked is a loop the provider built
    /// (`Protocol(ContractViolation)`).
    fn visit(
        &mut self,
        lookup: &str,
        email: &str,
        url: &AdmittedUrl,
        ctx: &GraphErrorContext,
    ) -> Result<(), AccountError> {
        if self
            .asked
            .iter()
            .any(|(seen_email, seen_url)| seen_email == email && seen_url == url)
        {
            return Err(contradictory_answer(
                format!(
                    "{lookup} redirected back to a mailbox and endpoint already asked \
                     (a redirect loop)"
                ),
                ctx,
            ));
        }
        self.asked.push((email.to_string(), url.clone()));
        Ok(())
    }
}

/// A redirect chain of distinct hops longer than [`MAX_REDIRECTS`]: this
/// crate's own safety cap, `Internal(LimitExceeded)` like a pagination
/// walk's page budget. Not the caller's input, and not proof of a provider
/// fault either, since a long chain may be a legitimate deployment.
fn redirect_limit(lookup: &str, ctx: GraphErrorContext) -> AccountError {
    into_account_error(
        GraphError::LimitExceeded {
            message: format!(
                "{lookup} redirected more than {MAX_REDIRECTS} times without an answer"
            ),
        },
        ctx,
    )
}

/// The POX `alternativeMailboxes` request for one mailbox.
fn build_pox_request(email: &str) -> String {
    let escaped_email = quick_xml::escape::escape(email);
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover xmlns="http://schemas.microsoft.com/exchange/autodiscover/outlook/requestschema/2006">
  <Request>
    <EMailAddress>{escaped_email}</EMailAddress>
    <AcceptableResponseSchema>http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a</AcceptableResponseSchema>
  </Request>
</Autodiscover>"#
    )
}

/// The code, when it reports anything other than success.
fn failing_code(code: Option<&str>) -> Option<&str> {
    code.filter(|code| !code.is_empty() && !code.eq_ignore_ascii_case("NoError"))
}

fn is_redirect(code: &str) -> bool {
    code.eq_ignore_ascii_case(REDIRECT_ADDRESS) || code.eq_ignore_ascii_case(REDIRECT_URL)
}

/// Whether a redirect target is written as an http or https URL, judged by
/// the URL parser (so the scheme's case does not matter).
fn is_http_url(target: &str) -> bool {
    reqwest::Url::parse(target).is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
}

/// An Autodiscover HTTP 200 whose body the provider got wrong: the
/// provider's malformed response (`Protocol(ParseFailed)`, `Acknowledged`).
/// The body is not attached: Autodiscover answers name mailboxes.
fn malformed_answer(message: String, ctx: GraphErrorContext) -> AccountError {
    into_account_error(
        GraphError::Json {
            message,
            body: None,
        },
        ctx,
    )
}

/// An Autodiscover answer that parsed but contradicts itself or the lookup
/// (a redirect code with no target, a redirect loop): the provider's
/// contract breach (`Protocol(ContractViolation)`, `Acknowledged`), not a
/// parse failure - the document was read in full.
fn contradictory_answer(message: String, ctx: &GraphErrorContext) -> AccountError {
    provider_answer_violation(ProtocolErrorKind::ContractViolation, message, ctx)
}

/// A `GetUserSettings` answer that resolved without the setting the lookup
/// needs. Nothing about this is the caller's input: the setting name is this
/// crate's, and the answer is the provider's.
///
/// Exchange reports a setting it will not give out per setting, in
/// `UserSettingErrors` (`SettingIsNotAvailable` for a tenant with no public
/// folders, say), and that code is classified like any other in-body code.
/// A setting that is simply absent, with no per-setting error to explain
/// it, is a `NoError` answer missing what it was asked for:
/// `Protocol(MissingField)`.
fn missing_setting(
    parsed: &UserSettingsResponse,
    setting: &str,
    ctx: &GraphErrorContext,
) -> AccountError {
    if let Some(error) = parsed
        .setting_errors
        .iter()
        .find(|error| error.setting.eq_ignore_ascii_case(setting))
        && let Some(code) = failing_code(Some(error.code.as_str()))
    {
        return autodiscover_error_code_to_account_error(
            &format!("Autodiscover setting {setting}"),
            code,
            error.message.as_deref(),
            ctx,
        );
    }
    provider_answer_violation(
        ProtocolErrorKind::MissingField,
        format!("Autodiscover GetUserSettings answer is missing {setting}"),
        ctx,
    )
}

/// A shared/delegate mailbox discovered via Exchange Autodiscover.
/// Routing keys on `smtp_address` alone; `display_name` and
/// `mailbox_type` are parsed for wire fidelity (and asserted by the
/// parser tests) but the seeding path deliberately does not filter on
/// type, so they are unread in non-test builds.
#[derive(Debug, Clone)]
pub(crate) struct SharedMailbox {
    pub(crate) smtp_address: String,
    #[allow(dead_code)]
    pub(crate) display_name: Option<String>,
    /// E.g. "Delegate", "TeamMailbox", etc.
    #[allow(dead_code)]
    pub(crate) mailbox_type: String,
}

/// Construct a synthetic SMTP address from a `PR_REPLICA_LIST` GUID and
/// domain, the address Autodiscover resolves to the real content mailbox.
pub(crate) fn construct_replica_smtp(guid: &str, domain: &str) -> String {
    format!("{guid}@{domain}")
}

/// Reduce the `GetUserSettings` name/value pairs into a
/// `PublicFolderRouting`: `PublicFolderInformation` is the hierarchy
/// (anchor) mailbox, `InternalRpcClientServer` is the public-folder
/// mailbox server. Errors (returns `None`) when `PublicFolderInformation`
/// is absent - there is no usable routing without it.
pub(crate) fn public_folder_routing_from_settings(
    settings: &[(String, String)],
) -> Option<PublicFolderRouting> {
    let mut anchor_mailbox: Option<String> = None;
    let mut public_folder_mailbox: Option<String> = None;
    for (name, value) in settings {
        match name.as_str() {
            "PublicFolderInformation" => anchor_mailbox = Some(value.clone()),
            "InternalRpcClientServer" => public_folder_mailbox = Some(value.clone()),
            _ => {}
        }
    }
    Some(PublicFolderRouting {
        anchor_mailbox: anchor_mailbox?,
        public_folder_mailbox,
    })
}

impl GraphAccount {
    /// `GetUserSettings` SOAP for `PublicFolderInformation` +
    /// `InternalRpcClientServer` -> the hierarchy routing.
    pub(crate) async fn discover_public_folder_routing(
        &self,
        user_email: &str,
    ) -> Result<PublicFolderRouting, AccountError> {
        let parsed = self
            .soap_get_user_settings(
                user_email,
                &["PublicFolderInformation", "InternalRpcClientServer"],
            )
            .await?;
        public_folder_routing_from_settings(&parsed.settings).ok_or_else(|| {
            missing_setting(
                &parsed,
                "PublicFolderInformation",
                &GraphErrorContext::graph(AccountOperation::Discover),
            )
        })
    }

    /// `GetUserSettings` for `AutoDiscoverSMTPAddress` -> the real
    /// content mailbox SMTP for a replica GUID's synthetic address.
    pub(crate) async fn discover_content_mailbox(
        &self,
        replica_smtp: &str,
    ) -> Result<String, AccountError> {
        let parsed = self
            .soap_get_user_settings(replica_smtp, &["AutoDiscoverSMTPAddress"])
            .await?;
        parsed
            .settings
            .iter()
            .find_map(|(name, value)| (name == "AutoDiscoverSMTPAddress").then(|| value.clone()))
            .ok_or_else(|| {
                missing_setting(
                    &parsed,
                    "AutoDiscoverSMTPAddress",
                    &GraphErrorContext::graph(AccountOperation::Discover),
                )
            })
    }

    /// `alternativeMailboxes` Autodiscover XML -> delegate mailbox list.
    /// Consumed by delegate auto-discovery at `open` when the factory's
    /// `with_delegate_discovery()` flag is set.
    ///
    /// An HTTP 200 whose body is malformed, truncated, or not an
    /// `Autodiscover` document is the provider's malformed response
    /// (`Protocol(ParseFailed)`), not an answer with no delegates: it used
    /// to read as an empty list, indistinguishable from a user who has
    /// none. The best-effort policy lives at the call site, not here: `open`
    /// degrades to the config-supplied mailboxes on any `Err` and records
    /// the skipped pass on `skipped_scopes`, so a bad answer is now
    /// reportable instead of silent.
    ///
    /// The POX protocol also answers inside an HTTP 200 with things that are
    /// not a mailbox list, and each used to read as "no delegates":
    ///
    /// - An `<Error>` fails the lookup, classified by its code
    ///   (`graph_error::autodiscover_pox_error_to_account_error`).
    /// - `<Action>redirectAddr</Action>` / `redirectUrl` are FOLLOWED under
    ///   exactly the rules the SOAP lookup applies: a new mailbox against the
    ///   same endpoint, or the same mailbox at an endpoint that must admit
    ///   onto the Outlook origin (a cross-origin one is
    ///   `Unsupported(Discover)`, since the POST carries the bearer), with
    ///   the same loop detection and [`MAX_REDIRECTS`] cap. Following rather
    ///   than refusing is what a hybrid tenant needs to find its delegates at
    ///   all, and the admission rule already makes a hop safe; reporting the
    ///   redirect instead would fail every such tenant's pass for nothing.
    /// - A redirect that contradicts itself (an action with no target, a
    ///   target with no redirect action, a URL named as a mailbox, an
    ///   endpoint that is not a URL, an action this protocol does not
    ///   define) is the provider's contract violation.
    pub(crate) async fn discover_shared_mailboxes(
        &self,
        user_email: &str,
    ) -> Result<Vec<SharedMailbox>, AccountError> {
        const LOOKUP: &str = "Autodiscover alternativeMailboxes";
        let ctx = GraphErrorContext::graph(AccountOperation::Discover);
        let mut url = self
            .client
            .outlook_url(AUTODISCOVER_XML_PATH)
            .map_err(|error| into_account_error(error, ctx.clone()))?;
        let mut email = user_email.to_string();
        let mut walk = RedirectWalk::default();

        for _ in 0..=MAX_REDIRECTS {
            walk.visit(LOOKUP, &email, &url, &ctx)?;
            let body = build_pox_request(&email);
            let xml = self.autodiscover_post(&url, "text/xml", None, body).await?;
            let answer = parse_pox_response(&xml)
                .map_err(|reason| malformed_answer(format!("{LOOKUP}: {reason}"), ctx.clone()))?;
            match self.pox_next_step(LOOKUP, answer, &ctx)? {
                PoxStep::Mailboxes(mailboxes) => return Ok(mailboxes),
                PoxStep::Mailbox(next) => email = next,
                PoxStep::Endpoint(next) => url = next,
            }
        }

        Err(redirect_limit(LOOKUP, ctx))
    }

    /// Decide what one POX answer asks for. See `discover_shared_mailboxes`.
    fn pox_next_step(
        &self,
        lookup: &str,
        answer: PoxAnswer,
        ctx: &GraphErrorContext,
    ) -> Result<PoxStep, AccountError> {
        if let Some(error) = answer.error {
            return Err(autodiscover_pox_error_to_account_error(
                lookup,
                &error.code,
                error.message.as_deref(),
                ctx,
            ));
        }
        let contradiction = |message: String| contradictory_answer(message, ctx);
        let action = answer
            .action
            .as_deref()
            .map(str::trim)
            .filter(|action| !action.is_empty());
        let is_redirect = action.is_some_and(|action| {
            action.eq_ignore_ascii_case(POX_REDIRECT_ADDR)
                || action.eq_ignore_ascii_case(POX_REDIRECT_URL)
        });
        if !is_redirect && (answer.redirect_addr.is_some() || answer.redirect_url.is_some()) {
            return Err(contradiction(format!(
                "{lookup} named a redirect target without a redirect Action"
            )));
        }
        match action {
            None => Ok(PoxStep::Mailboxes(answer.mailboxes)),
            Some(action) if action.eq_ignore_ascii_case("settings") => {
                Ok(PoxStep::Mailboxes(answer.mailboxes))
            }
            Some(action) if action.eq_ignore_ascii_case(POX_REDIRECT_ADDR) => {
                let Some(target) = answer.redirect_addr else {
                    return Err(contradiction(format!(
                        "{lookup} {action} carried no RedirectAddr"
                    )));
                };
                if is_http_url(&target) {
                    return Err(contradiction(format!(
                        "{lookup} redirectAddr named a URL, not a mailbox"
                    )));
                }
                Ok(PoxStep::Mailbox(target))
            }
            Some(action) if action.eq_ignore_ascii_case(POX_REDIRECT_URL) => {
                let Some(target) = answer.redirect_url else {
                    return Err(contradiction(format!(
                        "{lookup} {action} carried no RedirectUrl"
                    )));
                };
                if !is_http_url(&target) {
                    return Err(contradiction(format!(
                        "{lookup} redirectUrl did not name an http or https URL"
                    )));
                }
                self.admit_redirect_endpoint(&target).map(PoxStep::Endpoint)
            }
            Some(action) => Err(contradiction(format!(
                "{lookup} answered with Action {action}, which the protocol does not define"
            ))),
        }
    }

    /// Admit an Autodiscover redirect endpoint onto the configured Outlook
    /// origin, or refuse it as `Unsupported(Discover)`: the POST carries the
    /// account bearer, so a cross-origin endpoint could only leak it.
    fn admit_redirect_endpoint(&self, target: &str) -> Result<AdmittedUrl, AccountError> {
        self.client.admit_outlook_target(target).map_err(|refusal| {
            super::graph_error::unsupported_account_error(AccountOperation::Discover)
                .into_builder()
                .text(bifrost_types::DiagnosticText::support_only(format!(
                    "Autodiscover redirected to an endpoint this client will not \
                     send the account credential to: {refusal}"
                )))
                .try_build()
                .expect("valid account error classification")
        })
    }

    /// Run one `GetUserSettings` lookup to its answer, following in-body
    /// redirects, and return the parsed answer that carries the settings.
    ///
    /// Autodiscover redirects (`RedirectAddress` to a new email,
    /// `RedirectUrl` to a new endpoint) are common for hybrid / on-prem
    /// tenants and ride in-body, not as an HTTP 3xx. The chain ends two ways
    /// short of an answer, and they are told apart because the blame
    /// differs:
    ///
    /// - A redirect back to a (mailbox, endpoint) pair already asked is a
    ///   loop the provider built: `Protocol(ContractViolation)`, detected
    ///   before the repeat is sent.
    /// - A chain of distinct hops longer than [`MAX_REDIRECTS`] may be a
    ///   legitimate if unusual deployment; stopping there is this crate's own
    ///   safety cap, so it is `Internal(LimitExceeded)`, like a pagination
    ///   walk's page budget. Neither is the caller's input, which is what
    ///   the `Request(Malformed)` this used to raise claimed.
    async fn soap_get_user_settings(
        &self,
        email: &str,
        settings: &[&str],
    ) -> Result<UserSettingsResponse, AccountError> {
        let ctx = GraphErrorContext::graph(AccountOperation::Discover);
        let mut url = self
            .client
            .outlook_url(AUTODISCOVER_SOAP_PATH)
            .map_err(|error| into_account_error(error, ctx.clone()))?;
        let mut email = email.to_string();
        let mut walk = RedirectWalk::default();

        for _ in 0..=MAX_REDIRECTS {
            walk.visit("Autodiscover GetUserSettings", &email, &url, &ctx)?;
            let body = build_get_user_settings_soap(&email, settings);
            let xml = self
                .autodiscover_post(
                    &url,
                    "text/xml; charset=utf-8",
                    Some((
                        "SOAPAction",
                        "\"http://schemas.microsoft.com/exchange/2010/Autodiscover/Autodiscover/GetUserSettings\"",
                    )),
                    body,
                )
                .await?;
            let parsed = parse_user_settings_response(&xml).map_err(|reason| {
                malformed_answer(
                    format!("Autodiscover GetUserSettings: {reason}"),
                    ctx.clone(),
                )
            })?;

            match self.next_step(&parsed, &ctx)? {
                SoapStep::Settings => return Ok(parsed),
                SoapStep::Mailbox(next) => email = next,
                SoapStep::Endpoint(next) => url = next,
            }
        }

        Err(redirect_limit("Autodiscover GetUserSettings", ctx))
    }

    /// Decide what one `GetUserSettings` answer asks for.
    ///
    /// The redirect KIND is read from the protocol's own `ErrorCode`, never
    /// guessed from the shape of the target text: `RedirectAddress` names a
    /// mailbox to query against the same endpoint, `RedirectUrl` names an
    /// endpoint for the same mailbox. The user-level answer wins over the
    /// response-level one, because that is where Exchange puts a per-user
    /// redirect or refusal.
    ///
    /// A `RedirectUrl` endpoint must sit on the configured Outlook origin.
    /// The request carries the account bearer, which bifrost-net attaches
    /// whatever the host, and that token was issued for the configured
    /// resource: following a cross-origin endpoint with it could only hand
    /// it to another host. A hybrid tenant whose Autodiscover points at
    /// another origin needs trust this client does not establish, so that
    /// is `Unsupported(Discover)`, not a provider fault; a redirect answer
    /// that contradicts itself (a code with no target, a target with no
    /// redirect code, a URL named as a mailbox, an endpoint that is not a
    /// URL) is the provider's contract breach. Any other failing code is
    /// classified by what it says
    /// (`graph_error::autodiscover_error_code_to_account_error`).
    fn next_step(
        &self,
        parsed: &UserSettingsResponse,
        ctx: &GraphErrorContext,
    ) -> Result<SoapStep, AccountError> {
        let malformed = |message: String| contradictory_answer(message, ctx);
        let failure = |code: &str, message: Option<&str>| {
            autodiscover_error_code_to_account_error(
                "Autodiscover GetUserSettings",
                code,
                message,
                ctx,
            )
        };

        // A response-level failure other than a redirect ends the lookup.
        if let Some(code) = failing_code(parsed.response.code.as_deref())
            && !is_redirect(code)
        {
            return Err(failure(code, parsed.response.message.as_deref()));
        }
        // A user element that says anything - a code OR a target - is the
        // answer, so a user-level target with no user-level code is judged
        // as the contradiction it is rather than masked by a response-level
        // `NoError`.
        let answer = if parsed.user.code.is_some() || parsed.user.redirect_target.is_some() {
            &parsed.user
        } else {
            &parsed.response
        };
        let code = failing_code(answer.code.as_deref());
        match (code, answer.redirect_target.as_deref()) {
            (None, None) => Ok(SoapStep::Settings),
            (None, Some(_)) => Err(malformed(
                "Autodiscover named a redirect target without a redirect code".to_string(),
            )),
            (Some(code), target) if is_redirect(code) => {
                let Some(target) = target else {
                    return Err(malformed(format!(
                        "Autodiscover {code} carried no redirect target"
                    )));
                };
                if code.eq_ignore_ascii_case(REDIRECT_ADDRESS) {
                    if is_http_url(target) {
                        return Err(malformed(
                            "Autodiscover RedirectAddress named a URL, not a mailbox".to_string(),
                        ));
                    }
                    return Ok(SoapStep::Mailbox(target.to_string()));
                }
                if !is_http_url(target) {
                    return Err(malformed(
                        "Autodiscover RedirectUrl did not name an http or https URL".to_string(),
                    ));
                }
                self.admit_redirect_endpoint(target).map(SoapStep::Endpoint)
            }
            (Some(code), _) => Err(failure(code, answer.message.as_deref())),
        }
    }

    /// Shared transport for both Autodiscover endpoints. Routes the
    /// Bearer through `AccountNet`; classifies failures through the REST
    /// error path with `Protocol::Graph`.
    async fn autodiscover_post(
        &self,
        url: &AdmittedUrl,
        content_type: &str,
        extra_header: Option<(&str, &str)>,
        body: String,
    ) -> Result<String, AccountError> {
        let ctx = GraphErrorContext::graph(AccountOperation::Discover);
        let mut headers = vec![("Content-Type", content_type)];
        if let Some((name, value)) = extra_header {
            headers.push((name, value));
        }
        let resp = self
            .client
            .execute_aux(
                Resource::Unmetered,
                "POST",
                AuxTarget::Bearer(url),
                &headers,
                bytes::Bytes::from(body),
                None,
                bifrost_net::ReplayPolicy::Idempotent,
            )
            .await
            .map_err(|error| into_account_error(error, ctx.clone()))?;

        let status = resp.status;
        if !status.is_success() {
            // The real response headers, not an empty map: `Retry-After` and
            // `WWW-Authenticate` are part of the classification input.
            let response =
                GraphResponseError::from_response(status, resp.headers, cap_status_body(resp.body));
            return Err(response_to_account_error_pub(response, &ctx));
        }
        Ok(String::from_utf8_lossy(resp.body.as_ref()).into_owned())
    }
}

fn build_get_user_settings_soap(email: &str, settings: &[&str]) -> String {
    let escaped_email = quick_xml::escape::escape(email);
    let settings_xml: String = settings
        .iter()
        .map(|s| format!("          <a:Setting>{s}</a:Setting>"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <soap:Header>
    <a:RequestedServerVersion>Exchange2016</a:RequestedServerVersion>
  </soap:Header>
  <soap:Body>
    <a:GetUserSettingsRequestMessage>
      <a:Request>
        <a:Users>
          <a:User>
            <a:Mailbox>{escaped_email}</a:Mailbox>
          </a:User>
        </a:Users>
        <a:RequestedSettings>
{settings_xml}
        </a:RequestedSettings>
      </a:Request>
    </a:GetUserSettingsRequestMessage>
  </soap:Body>
</soap:Envelope>"#
    )
}

// ── Pure parsers ────────────────────────────────────────────

/// The parsed shape of a `GetUserSettings` response. EWS Autodiscover
/// returns failures and redirects INSIDE an HTTP 200 (the in-body
/// `<a:ErrorCode>` / `<a:RedirectTarget>`), so a status-only check reads
/// a failed or redirecting response as an empty settings vec. This
/// captures all three so the caller can act on them.
#[derive(Debug, Default)]
pub(crate) struct UserSettingsResponse {
    pub(crate) settings: Vec<(String, String)>,
    /// The `<a:Response>`-level markers. In a real answer this level is
    /// usually `NoError` even when the user-level answer is a redirect or
    /// a refusal, which is why the two levels are kept apart: reading only
    /// the first `ErrorCode` in document order saw `NoError` and missed
    /// every per-user error and redirect.
    pub(crate) response: SoapAnswer,
    /// The first `<a:UserResponse>`'s markers (one user is requested).
    pub(crate) user: SoapAnswer,
    /// The first `<a:UserResponse>`'s `<a:UserSettingErrors>`: why a
    /// requested setting is absent, when the server says.
    pub(crate) setting_errors: Vec<SettingError>,
}

/// One `<a:UserSettingError>`: the server's reason for not returning one
/// requested setting.
#[derive(Debug, Default)]
pub(crate) struct SettingError {
    pub(crate) setting: String,
    pub(crate) code: String,
    pub(crate) message: Option<String>,
}

/// One level's in-body error and redirect markers.
#[derive(Debug, Default)]
pub(crate) struct SoapAnswer {
    /// `<a:ErrorCode>` (`NoError` / absent on success).
    pub(crate) code: Option<String>,
    pub(crate) message: Option<String>,
    /// `<a:RedirectTarget>`: the SMTP address or URL to retry against;
    /// common for hybrid / on-prem tenants.
    pub(crate) redirect_target: Option<String>,
}

/// Parse `UserSetting` `<Name>`/`<Value>` pairs from a
/// `GetUserSettings` SOAP response. Thin wrapper over
/// `parse_user_settings_response` for the settings-only tests; the
/// production path consumes the full response (error/redirect markers).
#[cfg(test)]
fn parse_user_settings(xml: &str) -> Vec<(String, String)> {
    parse_user_settings_response(xml)
        .expect("well-formed test document")
        .settings
}

/// Tracks that an Autodiscover answer is one complete document with the
/// expected root, so a malformed or truncated HTTP 200 cannot read as a
/// valid answer that happens to be empty.
///
/// quick-xml reports a syntax error as `Err`, but input that simply STOPS
/// (a truncated body) reaches `Eof` with elements still open and no error,
/// and a body that is not XML at all (an HTML error page, an empty body)
/// can produce no element whatever. Both used to end the walk exactly like
/// a complete answer.
struct DocumentShape {
    root: &'static str,
    seen_root: bool,
    depth: usize,
}

impl DocumentShape {
    fn new(root: &'static str) -> Self {
        Self {
            root,
            seen_root: false,
            depth: 0,
        }
    }

    /// An element opened (`self_closing`: and closed at once).
    fn open(&mut self, local_name: &str, self_closing: bool) -> Result<(), String> {
        if self.depth == 0 {
            if self.seen_root {
                return Err("a second root element follows the document".to_string());
            }
            if local_name != self.root {
                return Err(format!(
                    "root element is {local_name}, expected {}",
                    self.root
                ));
            }
            self.seen_root = true;
        }
        if !self_closing {
            self.depth += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.depth = self
            .depth
            .checked_sub(1)
            .ok_or_else(|| "an end tag closes nothing".to_string())?;
        Ok(())
    }

    /// The input ended: fine only after one whole root element.
    fn finish(&self) -> Result<(), String> {
        if !self.seen_root {
            return Err(format!("no {} element in the response", self.root));
        }
        if self.depth != 0 {
            return Err("the response ended inside an open element (truncated)".to_string());
        }
        Ok(())
    }
}

/// Parse a `GetUserSettings` response into settings plus the in-body
/// error / redirect markers. A document that is malformed, truncated, or not
/// a SOAP envelope at all is `Err` with the reason, never an empty answer.
fn parse_user_settings_response(xml: &str) -> Result<UserSettingsResponse, String> {
    let mut reader = Reader::from_str(xml);
    let mut out = UserSettingsResponse::default();
    let mut shape = DocumentShape::new("Envelope");

    let mut in_user_setting = false;
    // Inside the first `<a:UserResponse>`; later ones are ignored, since one
    // user is requested.
    let mut in_user_response = false;
    let mut seen_user_response = false;
    // Inside `<a:UserSettingErrors>`, whose per-setting `ErrorCode`s are
    // not the user's answer; they are collected per setting instead.
    let mut in_setting_errors = false;
    let mut setting_error = SettingError::default();
    let mut current_name = String::new();
    let mut current_value = String::new();
    let mut current_tag = String::new();
    let mut buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().local_name().as_ref().to_owned();
                shape.open(&name, false)?;
                match name.as_str() {
                    "UserSetting" => {
                        in_user_setting = true;
                        current_name.clear();
                        current_value.clear();
                    }
                    "UserResponse" if !seen_user_response => in_user_response = true,
                    "UserSettingErrors" => in_setting_errors = true,
                    "UserSettingError" => setting_error = SettingError::default(),
                    _ => {}
                }
                current_tag = name;
                buf.clear();
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf)?,
            Ok(Event::GeneralRef(ref e)) => try_push_general_ref(e, &mut buf)?,
            Ok(Event::Empty(ref e)) => {
                shape.open(e.name().local_name().as_ref(), true)?;
            }
            Ok(Event::End(ref e)) => {
                shape.close()?;
                let name = e.name().local_name().as_ref().to_owned();
                let trimmed = buf.trim();
                if in_user_setting {
                    match current_tag.as_str() {
                        "Name" => current_name = trimmed.to_string(),
                        // A legitimately-empty setting Value is retained
                        // (a present-but-empty setting is not the same as
                        // an absent one).
                        "Value" => current_value = trimmed.to_string(),
                        _ => {}
                    }
                } else if in_setting_errors {
                    if in_user_response {
                        match current_tag.as_str() {
                            "SettingName" => setting_error.setting = trimmed.to_string(),
                            "ErrorCode" => setting_error.code = trimmed.to_string(),
                            "ErrorMessage" if !trimmed.is_empty() => {
                                setting_error.message = Some(trimmed.to_string());
                            }
                            _ => {}
                        }
                    }
                } else if in_user_response || !seen_user_response {
                    // Response-level or user-level markers, never a
                    // per-setting error and never a second user's answer.
                    let answer = if in_user_response {
                        &mut out.user
                    } else {
                        &mut out.response
                    };
                    match current_tag.as_str() {
                        "ErrorCode" if answer.code.is_none() => {
                            answer.code = Some(trimmed.to_string());
                        }
                        "ErrorMessage" if answer.message.is_none() && !trimmed.is_empty() => {
                            answer.message = Some(trimmed.to_string());
                        }
                        "RedirectTarget"
                            if answer.redirect_target.is_none() && !trimmed.is_empty() =>
                        {
                            answer.redirect_target = Some(trimmed.to_string());
                        }
                        _ => {}
                    }
                }
                match name.as_str() {
                    "UserSetting" => {
                        in_user_setting = false;
                        // Keep the pair when a Name is present; an empty
                        // Value is a legitimate setting, not a reason to
                        // drop it.
                        if !current_name.is_empty() {
                            out.settings
                                .push((current_name.clone(), current_value.clone()));
                        }
                    }
                    "UserResponse" if in_user_response => {
                        in_user_response = false;
                        seen_user_response = true;
                    }
                    "UserSettingError"
                        if in_setting_errors
                            && in_user_response
                            && !setting_error.setting.is_empty() =>
                    {
                        out.setting_errors.push(std::mem::take(&mut setting_error));
                    }
                    "UserSettingErrors" => in_setting_errors = false,
                    _ => {}
                }
                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("malformed XML: {error}")),
            _ => {}
        }
    }

    shape.finish()?;
    Ok(out)
}

/// Merge config-supplied shared-mailbox routing keys with the
/// Autodiscover-enumerated delegates into a single deduplicated list.
/// Config entries come first and win ordering; every entry in the merged
/// set (config first, discovered appended) is empty-dropped and deduped
/// by exact string uniformly - a blank config entry seeds no client, and
/// a delegate named both in config and by discovery (or discovered
/// twice) is not seeded twice. Every discovered alternative mailbox is
/// kept regardless of `mailbox_type` - the routing layer keys on the
/// address alone.
pub(crate) fn merge_shared_mailboxes(
    config: &[String],
    discovered: &[SharedMailbox],
) -> Vec<String> {
    let candidates = config.iter().cloned().chain(
        discovered
            .iter()
            .map(|mailbox| mailbox.smtp_address.clone()),
    );
    let mut merged: Vec<String> = Vec::new();
    for candidate in candidates {
        if !candidate.is_empty() && !merged.iter().any(|existing| existing == &candidate) {
            merged.push(candidate);
        }
    }
    merged
}

/// The parsed shape of a POX (`autodiscover.xml`) answer. Like SOAP, POX
/// reports failures and redirects inside an HTTP 200 - an `<Error>` element,
/// or an `<Action>` of `redirectAddr` / `redirectUrl` - so the mailbox list
/// alone cannot tell "no delegates" from "no answer".
#[derive(Debug, Default)]
struct PoxAnswer {
    mailboxes: Vec<SharedMailbox>,
    /// The first `<Error>`, if the answer carries one.
    error: Option<PoxError>,
    /// `<Account><Action>`: `settings`, `redirectAddr`, or `redirectUrl`.
    action: Option<String>,
    redirect_addr: Option<String>,
    redirect_url: Option<String>,
}

/// One POX `<Error>`: a numeric `<ErrorCode>` and a `<Message>`.
#[derive(Debug, Default)]
struct PoxError {
    code: String,
    message: Option<String>,
}

/// The mailbox list of a POX answer, for the parser tests.
#[cfg(test)]
fn parse_alternative_mailboxes(xml: &str) -> Result<Vec<SharedMailbox>, String> {
    parse_pox_response(xml).map(|answer| answer.mailboxes)
}

/// Parse a POX Autodiscover answer: its `AlternativeMailbox` elements plus
/// the in-body error and redirect markers. A document that is malformed,
/// truncated, or not an `Autodiscover` answer at all is `Err` with the
/// reason, never an empty mailbox list.
fn parse_pox_response(xml: &str) -> Result<PoxAnswer, String> {
    let mut reader = Reader::from_str(xml);
    let mut answer = PoxAnswer::default();
    let mut mailboxes = Vec::new();
    let mut shape = DocumentShape::new("Autodiscover");

    // Inside the first `<Error>`; a later one is ignored.
    let mut in_error = false;
    let mut in_alternative_mailbox = false;
    let mut current_type = String::new();
    let mut current_display_name = String::new();
    let mut current_smtp = String::new();
    let mut current_tag = String::new();
    let mut buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().local_name().as_ref().to_owned();
                shape.open(&name, false)?;
                if name == "AlternativeMailbox" {
                    in_alternative_mailbox = true;
                    current_type.clear();
                    current_display_name.clear();
                    current_smtp.clear();
                }
                if name == "Error" && answer.error.is_none() {
                    in_error = true;
                    answer.error = Some(PoxError::default());
                }
                current_tag = name;
                buf.clear();
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf)?,
            Ok(Event::GeneralRef(ref e)) => try_push_general_ref(e, &mut buf)?,
            Ok(Event::Empty(ref e)) => {
                let name = e.name().local_name().as_ref().to_owned();
                shape.open(&name, true)?;
                // `<Error/>` still reports a failure, one with no code.
                if name == "Error" && answer.error.is_none() {
                    answer.error = Some(PoxError::default());
                }
            }
            Ok(Event::End(ref e)) => {
                shape.close()?;
                let name = e.name().local_name().as_ref().to_owned();
                let trimmed = buf.trim();
                if in_alternative_mailbox {
                    match current_tag.as_str() {
                        "Type" => current_type = trimmed.to_string(),
                        "DisplayName" => current_display_name = trimmed.to_string(),
                        "SmtpAddress" => current_smtp = trimmed.to_string(),
                        _ => {}
                    }
                } else if in_error {
                    if let Some(error) = answer.error.as_mut() {
                        match current_tag.as_str() {
                            "ErrorCode" => error.code = trimmed.to_string(),
                            "Message" if !trimmed.is_empty() => {
                                error.message = Some(trimmed.to_string());
                            }
                            _ => {}
                        }
                    }
                } else if !trimmed.is_empty() {
                    let slot = match current_tag.as_str() {
                        "Action" => Some(&mut answer.action),
                        "RedirectAddr" => Some(&mut answer.redirect_addr),
                        "RedirectUrl" => Some(&mut answer.redirect_url),
                        _ => None,
                    };
                    if let Some(slot) = slot
                        && slot.is_none()
                    {
                        *slot = Some(trimmed.to_string());
                    }
                }
                if name == "Error" && in_error {
                    in_error = false;
                }
                if name == "AlternativeMailbox" {
                    in_alternative_mailbox = false;
                    if !current_smtp.is_empty() {
                        mailboxes.push(SharedMailbox {
                            smtp_address: current_smtp.clone(),
                            display_name: if current_display_name.is_empty() {
                                None
                            } else {
                                Some(current_display_name.clone())
                            },
                            mailbox_type: current_type.clone(),
                        });
                    }
                }
                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("malformed XML: {error}")),
            _ => {}
        }
    }

    shape.finish()?;
    answer.mailboxes = mailboxes;
    Ok(answer)
}

/// A text run that does not unescape is a malformed answer, not an empty
/// one: dropping it would splice the text on either side into a different
/// value (an address, say).
fn push_text(e: &quick_xml::events::BytesText<'_>, buf: &mut String) -> Result<(), String> {
    let text = unescape(e.as_ref()).map_err(|error| format!("malformed XML text: {error}"))?;
    buf.push_str(&text);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::account::PushMode;
    use crate::client::{GraphClient, ScriptedRestResponse};

    fn test_account(client: GraphClient) -> GraphAccount {
        GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions)
    }

    fn user_settings_xml(name: &str, value: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>NoError</a:ErrorCode>
        <a:UserResponses>
          <a:UserResponse>
            <a:UserSettings>
              <a:UserSetting>
                <a:Name>{name}</a:Name>
                <a:Value>{value}</a:Value>
              </a:UserSetting>
            </a:UserSettings>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#
        )
    }

    /// A per-user answer in the shape Exchange sends it: the response level
    /// reports `NoError` and the user level carries the code and target.
    fn user_answer_xml(code: &str, target: Option<&str>) -> String {
        let target = target
            .map(|target| format!("<a:RedirectTarget>{target}</a:RedirectTarget>"))
            .unwrap_or_default();
        format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>NoError</a:ErrorCode>
        <a:ErrorMessage />
        <a:UserResponses>
          <a:UserResponse>
            <a:ErrorCode>{code}</a:ErrorCode>
            <a:ErrorMessage>per-user answer</a:ErrorMessage>
            {target}
            <a:UserSettingErrors />
            <a:UserSettings />
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#
        )
    }

    fn redirect_xml(code: &str, target: &str) -> String {
        user_answer_xml(code, Some(target))
    }

    async fn content_mailbox_error(answer: String) -> (bifrost_types::AccountError, usize) {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(reqwest::StatusCode::OK, &answer)]);
        let account = test_account(client.clone());
        let error = account
            .discover_content_mailbox("replica@contoso.com")
            .await
            .expect_err("the answer must fail the lookup");
        (error, client.take_aux_requests().len())
    }

    /// The Autodiscover POST itself, which is not a Graph REST call: it
    /// goes to the Autodiscover origin (NOT the Graph host), posts XML, and
    /// carries the bearer. The delegate-discovery variant hits
    /// `/autodiscover/autodiscover.xml` with no `SOAPAction`, and the
    /// request body must name the mailbox and the response schema the
    /// parser expects.
    #[tokio::test]
    async fn delegate_discovery_posts_autodiscover_xml_and_parses_the_mailboxes() {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(
            reqwest::StatusCode::OK,
            r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover>
  <Response>
    <Account>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <DisplayName>Sales Team</DisplayName>
        <SmtpAddress>sales@contoso.com</SmtpAddress>
      </AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#,
        )]);
        let account = test_account(client.clone());

        let mailboxes = account
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect("discovery succeeds");
        assert_eq!(mailboxes.len(), 1);
        assert_eq!(mailboxes[0].smtp_address, "sales@contoso.com");

        let requests = client.take_aux_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(
            requests[0].url,
            format!(
                "{}/autodiscover/autodiscover.xml",
                crate::client::OUTLOOK_BASE
            )
        );
        assert_eq!(requests[0].header("Content-Type"), Some("text/xml"));
        assert_eq!(requests[0].header("SOAPAction"), None);
        assert!(
            requests[0].bearer,
            "Autodiscover is authenticated with the Graph bearer"
        );
        let body = String::from_utf8(requests[0].body.to_vec()).expect("UTF-8 request body");
        assert!(
            body.contains("<EMailAddress>user@contoso.com</EMailAddress>"),
            "{body}"
        );
        assert!(body.contains("responseschema/2006a"), "{body}");
    }

    /// bifrost-net returns `Err` for every 4xx/5xx before a response
    /// surfaces, so the only status that can reach `autodiscover_post`'s
    /// non-2xx branch is a passed-through 3xx - and when one does, the
    /// response's OWN headers are the classification and telemetry input.
    /// They used to be replaced with an empty map here, which discarded
    /// `Retry-After`, `WWW-Authenticate`, and the request id support reads.
    #[tokio::test]
    async fn a_passed_through_redirect_classifies_with_the_response_headers() {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(reqwest::StatusCode::FOUND, "")
            .with_header("Retry-After", "30")
            .with_header("request-id", "req-77")
            .with_header("client-request-id", "crid-77")]);
        let account = test_account(client);

        let error = account
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect_err("a redirect is not a discovery result");

        let telemetry = error.telemetry_fields();
        assert_eq!(telemetry.request_id, Some("req-77"));
        assert_eq!(telemetry.trace_id, Some("crid-77"));
    }

    /// A hybrid tenant answers `GetUserSettings` with an in-body
    /// `RedirectAddress` inside an HTTP 200 - bifrost-net never sees a 3xx,
    /// so nothing below this crate follows it. The walk must reissue the
    /// SOAP call for the NEW mailbox against the SAME endpoint, and a
    /// `RedirectUrl` on the configured Outlook origin must move the endpoint
    /// instead. Both redirects ride at the USER level under a response-level
    /// `NoError`, which is how Exchange sends them.
    #[tokio::test]
    async fn a_soap_redirect_chain_reissues_against_the_new_mailbox_then_endpoint() {
        let client = GraphClient::new("token");
        client.script_aux([
            ScriptedRestResponse::text(
                reqwest::StatusCode::OK,
                &redirect_xml("RedirectAddress", "replica@contoso.mail.onmicrosoft.com"),
            ),
            ScriptedRestResponse::text(
                reqwest::StatusCode::OK,
                &redirect_xml(
                    "RedirectUrl",
                    "HTTPS://outlook.office365.com/autodiscover/tenant/autodiscover.svc",
                ),
            ),
            ScriptedRestResponse::text(
                reqwest::StatusCode::OK,
                &user_settings_xml("AutoDiscoverSMTPAddress", "content@contoso.com"),
            ),
        ]);
        let account = test_account(client.clone());

        let mailbox = account
            .discover_content_mailbox("replica@contoso.com")
            .await
            .expect("the redirect chain resolves");
        assert_eq!(mailbox, "content@contoso.com");

        let requests = client.take_aux_requests();
        assert_eq!(requests.len(), 3);
        let soap_url = format!(
            "{}/autodiscover/autodiscover.svc",
            crate::client::OUTLOOK_BASE
        );
        assert_eq!(requests[0].url, soap_url);
        // The address redirect keeps the endpoint and changes the mailbox.
        assert_eq!(requests[1].url, soap_url);
        // The URL redirect moves the endpoint and keeps the mailbox; the
        // admitted URL is the parser's serialization, scheme lower-cased.
        assert_eq!(
            requests[2].url,
            "https://outlook.office365.com/autodiscover/tenant/autodiscover.svc"
        );
        let mailboxes: Vec<String> = requests
            .iter()
            .map(|request| {
                let body = String::from_utf8(request.body.to_vec()).expect("UTF-8 body");
                let start =
                    body.find("<a:Mailbox>").expect("mailbox element") + "<a:Mailbox>".len();
                let end = body.find("</a:Mailbox>").expect("mailbox element end");
                body[start..end].to_string()
            })
            .collect();
        assert_eq!(
            mailboxes,
            vec![
                "replica@contoso.com".to_string(),
                "replica@contoso.mail.onmicrosoft.com".to_string(),
                "replica@contoso.mail.onmicrosoft.com".to_string(),
            ]
        );
        for request in &requests {
            assert_eq!(
                request.header("Content-Type"),
                Some("text/xml; charset=utf-8")
            );
            assert!(
                request
                    .header("SOAPAction")
                    .is_some_and(|action| action.contains("GetUserSettings")),
                "the SOAP endpoint requires the action header"
            );
        }
    }

    /// An in-body `ErrorCode` other than `NoError` is a real failure, not
    /// an empty settings vec: without this the caller reports "response
    /// missing PublicFolderInformation" for what is actually an invalid
    /// user, and the Autodiscover error text is lost.
    #[tokio::test]
    async fn an_in_body_error_code_fails_the_lookup_with_its_text() {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(
            reqwest::StatusCode::OK,
            r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>InvalidUser</a:ErrorCode>
        <a:ErrorMessage>The user could not be found.</a:ErrorMessage>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#,
        )]);
        let account = test_account(client.clone());

        let error = account
            .discover_public_folder_routing("ghost@contoso.com")
            .await
            .expect_err("an in-body error fails the lookup");
        // The server does not know the mailbox: the provider's answer about
        // it, never the caller's malformed input.
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::NotFound(bifrost_types::ResourceKind::Mailbox)
        );
        assert_eq!(client.take_aux_requests().len(), 1);
    }

    /// Every in-body `ErrorCode` used to land on `Request(Malformed)`, so a
    /// throttled or failing Autodiscover server told the consumer to fix a
    /// request it never shaped, and was never retried. The code says what
    /// happened; the kind follows it.
    #[tokio::test]
    async fn an_in_body_error_code_is_classified_by_what_it_says() {
        use bifrost_types::{
            AccountErrorKind, RecoveryClass, RequestErrorKind, ServerErrorKind, ThrottleScope,
        };
        for (code, want) in [
            (
                "ServerBusy",
                AccountErrorKind::Server(ServerErrorKind::RateLimited),
            ),
            (
                "InternalServerError",
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
            ),
            (
                "InvalidRequest",
                AccountErrorKind::Request(RequestErrorKind::Malformed),
            ),
            (
                "NotFederated",
                AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            ),
            (
                "InvalidDomain",
                AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            ),
            (
                "SomeCodeFromTheFuture",
                AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            ),
        ] {
            let (error, requests) = content_mailbox_error(user_answer_xml(code, None)).await;
            assert_eq!(requests, 1, "{code}");
            assert_eq!(error.kind(), &want, "{code}");
            assert!(format!("{error:?}").contains(code), "{code}: {error:?}");
            match code {
                "ServerBusy" => match error.recovery() {
                    RecoveryClass::Retry(advice) => {
                        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
                    }
                    other => panic!("ServerBusy must retry, got {other:?}"),
                },
                "InternalServerError" => assert!(error.recovery().is_retryable()),
                "InvalidRequest" => {}
                _ => assert_eq!(error.recovery(), &RecoveryClass::ProviderRefused, "{code}"),
            }
        }
    }

    /// A `NoError` answer that lacks the setting the lookup asked for used to
    /// be `Request(Malformed)`. With no reason given it is the provider's
    /// incomplete answer; with a per-setting `UserSettingErrors` entry the
    /// server said why, and that code decides.
    #[tokio::test]
    async fn a_missing_setting_is_the_providers_answer_not_the_callers_input() {
        let (error, requests) =
            content_mailbox_error(user_settings_xml("SomethingElse", "unrelated@contoso.com"))
                .await;
        assert_eq!(requests, 1);
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::MissingField
            )
        );

        let refused = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body><a:GetUserSettingsResponseMessage><a:Response>
    <a:ErrorCode>NoError</a:ErrorCode>
    <a:UserResponses><a:UserResponse>
      <a:ErrorCode>NoError</a:ErrorCode>
      <a:UserSettingErrors><a:UserSettingError>
        <a:ErrorCode>SettingIsNotAvailable</a:ErrorCode>
        <a:ErrorMessage>No public folders.</a:ErrorMessage>
        <a:SettingName>PublicFolderInformation</a:SettingName>
      </a:UserSettingError></a:UserSettingErrors>
      <a:UserSettings />
    </a:UserResponse></a:UserResponses>
  </a:Response></a:GetUserSettingsResponseMessage></s:Body>
</s:Envelope>"#;
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(reqwest::StatusCode::OK, refused)]);
        let error = test_account(client)
            .discover_public_folder_routing("user@contoso.com")
            .await
            .expect_err("no routing without PublicFolderInformation");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Server(bifrost_types::ServerErrorKind::Error {
                status: None
            })
        );
        assert!(
            format!("{error:?}").contains("SettingIsNotAvailable"),
            "{error:?}"
        );
    }

    /// A redirect back to a mailbox already asked is a loop the provider
    /// built: refused before the repeat is sent, as a contract violation. The
    /// old code sent every hop up to the cap and then blamed the caller.
    #[tokio::test]
    async fn a_redirect_loop_is_the_providers_contract_violation() {
        let client = GraphClient::new("token");
        client.script_aux([
            ScriptedRestResponse::text(
                reqwest::StatusCode::OK,
                &redirect_xml("RedirectAddress", "other@contoso.com"),
            ),
            ScriptedRestResponse::text(
                reqwest::StatusCode::OK,
                &redirect_xml("RedirectAddress", "replica@contoso.com"),
            ),
        ]);
        let error = test_account(client.clone())
            .discover_content_mailbox("replica@contoso.com")
            .await
            .expect_err("a loop never answers");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
        assert_eq!(client.take_aux_requests().len(), 2);
    }

    /// A chain of distinct hops past the cap is this crate's own limit, not
    /// the caller's malformed request and not proof of a provider fault.
    #[tokio::test]
    async fn a_redirect_chain_past_the_cap_is_a_local_limit() {
        let client = GraphClient::new("token");
        client.script_aux((0..=MAX_REDIRECTS).map(|hop| {
            ScriptedRestResponse::text(
                reqwest::StatusCode::OK,
                &redirect_xml("RedirectAddress", &format!("hop{hop}@contoso.com")),
            )
        }));
        let error = test_account(client.clone())
            .discover_content_mailbox("replica@contoso.com")
            .await
            .expect_err("the chain never answers");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Internal(
                bifrost_types::InternalErrorKind::LimitExceeded
            )
        );
        assert_eq!(client.take_aux_requests().len(), MAX_REDIRECTS + 1);
    }

    /// The token leak this closes: the Autodiscover POST carries the account
    /// bearer, and bifrost-net attaches it to the first request whatever the
    /// host, so following a `RedirectUrl` off the Outlook origin would hand
    /// the token to whoever the response named. However the endpoint is
    /// spelled, the lookup stops after the one request that returned it.
    #[tokio::test]
    async fn a_cross_origin_redirect_url_is_refused_before_any_request() {
        for target in [
            "https://autodiscover.contoso.com/autodiscover/autodiscover.svc",
            "HTTPS://attacker.example/autodiscover.svc",
            "https://outlook.office365.com@attacker.example/autodiscover.svc",
            "http://outlook.office365.com/autodiscover/autodiscover.svc",
            "https://@outlook.office365.com/autodiscover/autodiscover.svc",
        ] {
            let (error, requests) =
                content_mailbox_error(redirect_xml("RedirectUrl", target)).await;
            assert_eq!(requests, 1, "{target} must not be requested");
            assert!(
                matches!(
                    error.kind(),
                    bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Discover)
                ),
                "{target}: {:?}",
                error.kind()
            );
        }
    }

    /// A redirect answer that contradicts itself is the provider's malformed
    /// response, never guessed into a mailbox or an endpoint.
    #[tokio::test]
    async fn an_inconsistent_redirect_answer_is_a_provider_fault() {
        for answer in [
            // A URL named as a mailbox.
            redirect_xml("RedirectAddress", "https://attacker.example/x"),
            // An endpoint that is not a URL (it must not become a mailbox).
            redirect_xml("RedirectUrl", "https://"),
            redirect_xml("RedirectUrl", "someone@contoso.com"),
            // A redirect code with no target.
            user_answer_xml("RedirectUrl", None),
            // A target with no redirect code.
            redirect_xml("NoError", "someone@contoso.com"),
            // A user-level target with no user-level code at all, under a
            // response-level `NoError` that must not mask it.
            redirect_xml("Omitted", "https://attacker.example/x")
                .replace("<a:ErrorCode>Omitted</a:ErrorCode>", ""),
        ] {
            let (error, requests) = content_mailbox_error(answer).await;
            assert_eq!(requests, 1);
            assert!(
                matches!(error.kind(), bifrost_types::AccountErrorKind::Protocol(_)),
                "{:?}",
                error.kind()
            );
        }
    }

    /// Exchange reports a per-user refusal under a response-level `NoError`.
    /// Reading only the first `ErrorCode` in the document saw `NoError`, so
    /// an unknown user came back as an empty settings list and the caller
    /// reported a missing setting instead of the refusal.
    #[tokio::test]
    async fn a_user_level_error_under_a_response_level_no_error_fails_the_lookup() {
        let (error, requests) = content_mailbox_error(user_answer_xml("InvalidUser", None)).await;
        assert_eq!(requests, 1);
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::NotFound(bifrost_types::ResourceKind::Mailbox)
        );
        assert!(
            format!("{error:?}").contains("InvalidUser"),
            "the refusal names its code: {error:?}"
        );
    }

    /// `text` cut just before the first occurrence of `marker`.
    fn cut_before(text: &str, marker: &str) -> String {
        text[..text.find(marker).expect("marker present")].to_string()
    }

    fn assert_parse_failed(error: &bifrost_types::AccountError) {
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ParseFailed
            ),
            "{error:?}"
        );
    }

    /// Bodies an HTTP 200 can carry that are not a `GetUserSettings` answer.
    /// The old parser treated a quick-xml error like end of input and never
    /// checked that the document was complete, so every one of these came
    /// back as an answer with no settings and no error.
    #[test]
    fn a_malformed_user_settings_document_is_an_error_not_an_empty_answer() {
        let whole = user_settings_xml("AutoDiscoverSMTPAddress", "content@contoso.com");
        for (label, body) in [
            ("empty", String::new()),
            ("plain text", "Bad Gateway".to_string()),
            ("wrong root", "<html><body>ok</body></html>".to_string()),
            (
                "truncated inside a setting",
                cut_before(&whole, "</a:UserSetting>"),
            ),
            (
                "truncated after the settings",
                cut_before(&whole, "</a:UserResponse>"),
            ),
            (
                "mismatched end tag",
                whole.replace("</a:Value>", "</a:Name>"),
            ),
            ("two roots", format!("{whole}<s:Envelope/>")),
            (
                "unknown entity in a value",
                whole.replace("content@", "content&bogus;@"),
            ),
        ] {
            assert!(
                parse_user_settings_response(&body).is_err(),
                "{label} must not parse as an answer"
            );
        }
    }

    /// The same for the delegate document: a truncated answer used to yield
    /// the mailboxes before the cut as if they were the whole list, and a
    /// non-Autodiscover body an empty list.
    #[test]
    fn a_malformed_alternative_mailboxes_document_is_an_error_not_a_short_list() {
        let whole = r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover>
  <Response>
    <Account>
      <AlternativeMailbox><SmtpAddress>sales@contoso.com</SmtpAddress></AlternativeMailbox>
      <AlternativeMailbox><SmtpAddress>eng@contoso.com</SmtpAddress></AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#;
        assert_eq!(
            parse_alternative_mailboxes(whole)
                .expect("the whole document parses")
                .len(),
            2
        );
        for (label, body) in [
            ("empty", String::new()),
            ("wrong root", "<html><body>ok</body></html>".to_string()),
            (
                "truncated after one mailbox",
                cut_before(whole, "<AlternativeMailbox><SmtpAddress>eng"),
            ),
            (
                "truncated before the root closes",
                cut_before(whole, "</Autodiscover>"),
            ),
            (
                "mismatched end tag",
                whole.replace("</Account>", "</Response>"),
            ),
            // Dropping the reference would read `sales@contoso.com`, a
            // different mailbox from the one the server named.
            ("unknown entity", whole.replace("sales@", "sales&bogus;@")),
            (
                "invalid character reference",
                whole.replace("sales@", "sales&#xD800;@"),
            ),
        ] {
            assert!(
                parse_alternative_mailboxes(&body).is_err(),
                "{label} must not parse as a mailbox list"
            );
        }
    }

    /// End to end on both `GetUserSettings` lookups: a truncated HTTP 200 is
    /// the provider's malformed response. The old code read the truncated
    /// answer as one with no settings and reported the missing setting as
    /// `Request(Malformed)`, blaming the caller for the provider's body.
    #[tokio::test]
    async fn a_truncated_user_settings_answer_is_the_providers_parse_failure() {
        let truncated = cut_before(
            &user_settings_xml("AutoDiscoverSMTPAddress", "content@contoso.com"),
            "</a:UserSetting>",
        );
        let (error, requests) = content_mailbox_error(truncated).await;
        assert_eq!(requests, 1);
        assert_parse_failed(&error);

        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(
            reqwest::StatusCode::OK,
            &cut_before(
                &user_settings_xml("PublicFolderInformation", "pf@contoso.com"),
                "</a:UserSetting>",
            ),
        )]);
        let error = test_account(client.clone())
            .discover_public_folder_routing("user@contoso.com")
            .await
            .expect_err("a truncated answer fails the lookup");
        assert_parse_failed(&error);
        assert_eq!(client.take_aux_requests().len(), 1);
    }

    /// Delegate discovery on a malformed HTTP 200 fails instead of reporting
    /// "no delegates", so `open`'s best-effort degradation records a skipped
    /// pass rather than silently opening without them. The old code returned
    /// `Ok` with an empty list.
    #[tokio::test]
    async fn a_malformed_delegate_answer_fails_discovery_instead_of_finding_none() {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(
            reqwest::StatusCode::OK,
            "<Autodiscover><Response><Account><AlternativeMailbox>",
        )]);
        let error = test_account(client)
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect_err("a truncated answer is not an empty one");
        assert_parse_failed(&error);
    }

    /// A POX answer body: `inner` inside `<Autodiscover><Response>`.
    fn pox(inner: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover xmlns="http://schemas.microsoft.com/exchange/autodiscover/responseschema/2006">
  <Response xmlns="http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a">{inner}</Response>
</Autodiscover>"#
        )
    }

    fn pox_redirect(action: &str, element: &str, target: &str) -> String {
        pox(&format!(
            "<Account><Action>{action}</Action><{element}>{target}</{element}></Account>"
        ))
    }

    fn pox_mailbox(smtp: &str) -> String {
        pox(&format!(
            "<Account><Action>settings</Action><AlternativeMailbox><Type>Delegate</Type>\
             <SmtpAddress>{smtp}</SmtpAddress></AlternativeMailbox></Account>"
        ))
    }

    fn ok(body: &str) -> ScriptedRestResponse {
        ScriptedRestResponse::text(reqwest::StatusCode::OK, body)
    }

    /// Run delegate discovery over `answers`; the error plus how many POSTs
    /// went out.
    async fn delegate_error(
        answers: Vec<ScriptedRestResponse>,
    ) -> (bifrost_types::AccountError, usize) {
        let client = GraphClient::new("token");
        client.script_aux(answers);
        let error = test_account(client.clone())
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect_err("the answer must fail discovery");
        (error, client.take_aux_requests().len())
    }

    /// A POX `<Error>` inside an HTTP 200 used to parse as an answer with
    /// no `AlternativeMailbox`, so a refused lookup read as "this user has
    /// no delegates" and `open` recorded nothing skipped. The numeric code
    /// is classified; only the two codes with a pinned meaning map onto a
    /// specific kind.
    #[tokio::test]
    async fn a_pox_error_fails_delegate_discovery_classified_by_its_code() {
        use bifrost_types::{AccountErrorKind, RequestErrorKind, ResourceKind, ServerErrorKind};
        for (error, want, native) in [
            (
                "<Error Time=\"1\" Id=\"2\"><ErrorCode>500</ErrorCode>\
                 <Message>The email address can't be found.</Message><DebugData /></Error>",
                AccountErrorKind::NotFound(ResourceKind::Mailbox),
                Some("500"),
            ),
            (
                "<Error><ErrorCode>600</ErrorCode><Message>Invalid Request</Message></Error>",
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Some("600"),
            ),
            (
                "<Error><ErrorCode>601</ErrorCode></Error>",
                AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
                Some("601"),
            ),
            (
                "<Error/>",
                AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
                None,
            ),
        ] {
            let (err, requests) = delegate_error(vec![ok(&pox(error))]).await;
            assert_eq!(requests, 1, "{error}");
            assert_eq!(err.kind(), &want, "{error}");
            if let Some(native) = native {
                assert!(format!("{err:?}").contains(native), "{error}: {err:?}");
            }
        }
    }

    /// POX redirects used to be ignored, so a hybrid tenant's delegate pass
    /// found nothing. `redirectAddr` asks again for the new mailbox at the
    /// same endpoint; `redirectUrl` on the Outlook origin asks again for the
    /// same mailbox there.
    #[tokio::test]
    async fn pox_redirects_are_followed_to_the_mailbox_list() {
        let client = GraphClient::new("token");
        client.script_aux([
            ok(&pox_redirect(
                "redirectAddr",
                "RedirectAddr",
                "user@contoso.mail.onmicrosoft.com",
            )),
            ok(&pox_redirect(
                "redirectUrl",
                "RedirectUrl",
                "https://outlook.office365.com/autodiscover/tenant/autodiscover.xml",
            )),
            ok(&pox_mailbox("sales@contoso.com")),
        ]);
        let mailboxes = test_account(client.clone())
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect("the redirect chain resolves");
        assert_eq!(mailboxes.len(), 1);
        assert_eq!(mailboxes[0].smtp_address, "sales@contoso.com");

        let requests = client.take_aux_requests();
        assert_eq!(requests.len(), 3);
        let xml_url = format!(
            "{}/autodiscover/autodiscover.xml",
            crate::client::OUTLOOK_BASE
        );
        assert_eq!(requests[0].url, xml_url);
        assert_eq!(requests[1].url, xml_url);
        assert_eq!(
            requests[2].url,
            "https://outlook.office365.com/autodiscover/tenant/autodiscover.xml"
        );
        let asked: Vec<bool> = requests
            .iter()
            .map(|request| {
                String::from_utf8(request.body.to_vec())
                    .expect("UTF-8 body")
                    .contains("<EMailAddress>user@contoso.mail.onmicrosoft.com</EMailAddress>")
            })
            .collect();
        assert_eq!(asked, vec![false, true, true]);
    }

    /// The bearer rides on the POX POST too, so a cross-origin `redirectUrl`
    /// is refused before any request, exactly as on the SOAP lookup.
    #[tokio::test]
    async fn a_cross_origin_pox_redirect_url_is_refused() {
        let (error, requests) = delegate_error(vec![ok(&pox_redirect(
            "redirectUrl",
            "RedirectUrl",
            "https://attacker.example/autodiscover/autodiscover.xml",
        ))])
        .await;
        assert_eq!(requests, 1);
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Discover)
        );
    }

    /// A POX redirect that contradicts itself is the provider's contract
    /// violation, never guessed into a mailbox list, a mailbox, or an
    /// endpoint.
    #[tokio::test]
    async fn an_inconsistent_pox_answer_is_a_contract_violation() {
        for answer in [
            pox("<Account><Action>redirectAddr</Action></Account>"),
            pox("<Account><Action>redirectUrl</Action></Account>"),
            pox_redirect("redirectAddr", "RedirectAddr", "https://attacker.example/x"),
            pox_redirect("redirectUrl", "RedirectUrl", "someone@contoso.com"),
            pox_redirect("settings", "RedirectAddr", "someone@contoso.com"),
            pox("<Account><RedirectUrl>https://outlook.office365.com/x</RedirectUrl></Account>"),
            pox("<Account><Action>somethingNew</Action></Account>"),
        ] {
            let (error, requests) = delegate_error(vec![ok(&answer)]).await;
            assert_eq!(requests, 1, "{answer}");
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ContractViolation
                ),
                "{answer}"
            );
        }
    }

    /// POX redirects share the SOAP lookup's loop and cap rules.
    #[tokio::test]
    async fn pox_redirect_loops_and_long_chains_end_the_walk() {
        let (error, requests) = delegate_error(vec![
            ok(&pox_redirect(
                "redirectAddr",
                "RedirectAddr",
                "other@contoso.com",
            )),
            ok(&pox_redirect(
                "redirectAddr",
                "RedirectAddr",
                "user@contoso.com",
            )),
        ])
        .await;
        assert_eq!(requests, 2);
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );

        let (error, requests) = delegate_error(
            (0..=MAX_REDIRECTS)
                .map(|hop| {
                    ok(&pox_redirect(
                        "redirectAddr",
                        "RedirectAddr",
                        &format!("hop{hop}@contoso.com"),
                    ))
                })
                .collect(),
        )
        .await;
        assert_eq!(requests, MAX_REDIRECTS + 1);
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Internal(
                bifrost_types::InternalErrorKind::LimitExceeded
            )
        );
    }

    /// A per-setting `ErrorCode` inside `UserSettingErrors` is not the
    /// user's answer: the requested setting that did resolve is returned.
    #[test]
    fn a_per_setting_error_is_not_the_user_answer() {
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body><a:GetUserSettingsResponseMessage><a:Response>
    <a:ErrorCode>NoError</a:ErrorCode>
    <a:UserResponses><a:UserResponse>
      <a:ErrorCode>NoError</a:ErrorCode>
      <a:UserSettingErrors><a:UserSettingError>
        <a:ErrorCode>SettingIsNotAvailable</a:ErrorCode>
        <a:SettingName>InternalRpcClientServer</a:SettingName>
      </a:UserSettingError></a:UserSettingErrors>
      <a:UserSettings><a:UserSetting>
        <a:Name>PublicFolderInformation</a:Name><a:Value>pf@contoso.com</a:Value>
      </a:UserSetting></a:UserSettings>
    </a:UserResponse></a:UserResponses>
  </a:Response></a:GetUserSettingsResponseMessage></s:Body>
</s:Envelope>"#;
        let parsed = parse_user_settings_response(xml).expect("well-formed");
        assert_eq!(parsed.user.code.as_deref(), Some("NoError"));
        assert_eq!(parsed.response.code.as_deref(), Some("NoError"));
        assert_eq!(
            parsed.settings,
            vec![(
                "PublicFolderInformation".to_string(),
                "pf@contoso.com".to_string()
            )]
        );
    }

    /// An unusable Graph api-base must not leave Autodiscover pointing at the
    /// production Outlook origin: the derived origin is unusable too, and
    /// nothing is sent.
    #[tokio::test]
    async fn an_unusable_api_base_sends_no_autodiscover_request() {
        let client = GraphClient::with_api_base("not a url", "token");
        client.script_aux([]);
        let account = test_account(client.clone());
        let error = account
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect_err("an unusable base refuses");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(client.wire_attempts(), 0);
    }

    #[test]
    fn parse_single_alternative_mailbox() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover xmlns="http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a">
  <Response>
    <Account>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <DisplayName>Sales Team</DisplayName>
        <SmtpAddress>sales@contoso.com</SmtpAddress>
      </AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#;
        let result = parse_alternative_mailboxes(xml).expect("well-formed");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].smtp_address, "sales@contoso.com");
        assert_eq!(result[0].display_name.as_deref(), Some("Sales Team"));
        assert_eq!(result[0].mailbox_type, "Delegate");
    }

    #[test]
    fn parse_multiple_alternative_mailboxes() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover>
  <Response>
    <Account>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <DisplayName>Sales Team</DisplayName>
        <SmtpAddress>sales@contoso.com</SmtpAddress>
      </AlternativeMailbox>
      <AlternativeMailbox>
        <Type>TeamMailbox</Type>
        <DisplayName>Engineering</DisplayName>
        <SmtpAddress>eng@contoso.com</SmtpAddress>
      </AlternativeMailbox>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <SmtpAddress>noreply@contoso.com</SmtpAddress>
      </AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#;
        let result = parse_alternative_mailboxes(xml).expect("well-formed");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].smtp_address, "sales@contoso.com");
        assert_eq!(result[0].mailbox_type, "Delegate");
        assert_eq!(result[1].smtp_address, "eng@contoso.com");
        assert_eq!(result[1].display_name.as_deref(), Some("Engineering"));
        assert_eq!(result[1].mailbox_type, "TeamMailbox");
        assert_eq!(result[2].smtp_address, "noreply@contoso.com");
        assert_eq!(result[2].display_name, None);
    }

    #[test]
    fn parse_alternative_mailboxes_empty() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover>
  <Response>
    <Account>
    </Account>
  </Response>
</Autodiscover>"#;
        assert!(
            parse_alternative_mailboxes(xml)
                .expect("well-formed")
                .is_empty()
        );
    }

    #[test]
    fn skip_mailbox_without_smtp_address() {
        let xml = r#"<Autodiscover>
  <Response>
    <Account>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <DisplayName>Broken Entry</DisplayName>
      </AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#;
        assert!(
            parse_alternative_mailboxes(xml)
                .expect("well-formed")
                .is_empty()
        );
    }

    fn shared(smtp: &str, ty: &str) -> SharedMailbox {
        SharedMailbox {
            smtp_address: smtp.to_string(),
            display_name: None,
            mailbox_type: ty.to_string(),
        }
    }

    #[test]
    fn merge_appends_discovered_to_config() {
        let config = vec!["configured@contoso.com".to_string()];
        let discovered = vec![shared("sales@contoso.com", "Delegate")];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["configured@contoso.com", "sales@contoso.com"]);
    }

    #[test]
    fn merge_dedups_overlap_keeping_config_order() {
        let config = vec![
            "shared@contoso.com".to_string(),
            "eng@contoso.com".to_string(),
        ];
        // `shared@` is both config-supplied and discovered: it must not
        // seed a second client. `sales@` is new and appends.
        let discovered = vec![
            shared("shared@contoso.com", "Delegate"),
            shared("sales@contoso.com", "TeamMailbox"),
        ];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(
            merged,
            vec!["shared@contoso.com", "eng@contoso.com", "sales@contoso.com"]
        );
    }

    #[test]
    fn merge_ignores_empty_smtp_and_keeps_all_types() {
        let config: Vec<String> = Vec::new();
        // An empty smtp_address is dropped; a non-Delegate type is kept
        // (no type filtering - the routing layer keys on the address).
        let discovered = vec![
            shared("", "Delegate"),
            shared("team@contoso.com", "TeamMailbox"),
        ];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["team@contoso.com"]);
    }

    #[test]
    fn merge_drops_empty_config_entries() {
        // A blank `with_shared_mailbox("")` entry must not seed a
        // `/users/` client with an empty routing key.
        let config = vec![
            String::new(),
            "configured@contoso.com".to_string(),
            String::new(),
        ];
        let discovered: Vec<SharedMailbox> = Vec::new();
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["configured@contoso.com"]);
    }

    #[test]
    fn merge_dedups_repeated_config_entries() {
        let config = vec![
            "configured@contoso.com".to_string(),
            "configured@contoso.com".to_string(),
        ];
        let discovered = vec![shared("sales@contoso.com", "Delegate")];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["configured@contoso.com", "sales@contoso.com"]);
    }

    #[test]
    fn merge_dedups_repeated_discovered_entries() {
        let config: Vec<String> = Vec::new();
        let discovered = vec![
            shared("dup@contoso.com", "Delegate"),
            shared("dup@contoso.com", "Delegate"),
        ];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["dup@contoso.com"]);
    }

    #[test]
    fn parse_public_folder_routing_settings() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:UserResponses>
          <a:UserResponse>
            <a:UserSettings>
              <a:UserSetting>
                <a:Name>PublicFolderInformation</a:Name>
                <a:Value>publicfolders@contoso.com</a:Value>
              </a:UserSetting>
              <a:UserSetting>
                <a:Name>InternalRpcClientServer</a:Name>
                <a:Value>server01.contoso.com</a:Value>
              </a:UserSetting>
            </a:UserSettings>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let settings = parse_user_settings(xml);
        assert_eq!(settings.len(), 2);
        assert_eq!(settings[0].0, "PublicFolderInformation");
        assert_eq!(settings[0].1, "publicfolders@contoso.com");
        assert_eq!(settings[1].0, "InternalRpcClientServer");
        assert_eq!(settings[1].1, "server01.contoso.com");
    }

    #[test]
    fn parse_autodiscover_smtp_address() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:UserResponses>
          <a:UserResponse>
            <a:UserSettings>
              <a:UserSetting>
                <a:Name>AutoDiscoverSMTPAddress</a:Name>
                <a:Value>contentmailbox@contoso.com</a:Value>
              </a:UserSetting>
            </a:UserSettings>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let settings = parse_user_settings(xml);
        assert_eq!(settings.len(), 1);
        assert_eq!(settings[0].0, "AutoDiscoverSMTPAddress");
        assert_eq!(settings[0].1, "contentmailbox@contoso.com");
    }

    #[test]
    fn parse_user_settings_empty() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <GetUserSettingsResponseMessage>
      <Response>
        <UserResponses>
          <UserResponse>
            <UserSettings />
          </UserResponse>
        </UserResponses>
      </Response>
    </GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        assert!(parse_user_settings(xml).is_empty());
    }

    #[test]
    fn user_settings_in_body_error_is_captured() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>InvalidUser</a:ErrorCode>
        <a:ErrorMessage>The user could not be found.</a:ErrorMessage>
        <a:UserResponses>
          <a:UserResponse>
            <a:UserSettings/>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let parsed = parse_user_settings_response(xml).expect("well-formed");
        assert!(parsed.settings.is_empty());
        assert_eq!(parsed.response.code.as_deref(), Some("InvalidUser"));
        assert_eq!(
            parsed.response.message.as_deref(),
            Some("The user could not be found.")
        );
        // The user element carried no code of its own.
        assert_eq!(parsed.user.code, None);
    }

    #[test]
    fn user_settings_redirect_target_is_captured() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>RedirectAddress</a:ErrorCode>
        <a:RedirectTarget>user@redirected.contoso.com</a:RedirectTarget>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let parsed = parse_user_settings_response(xml).expect("well-formed");
        assert_eq!(
            parsed.response.redirect_target.as_deref(),
            Some("user@redirected.contoso.com")
        );
    }

    #[test]
    fn user_settings_retains_empty_value() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>NoError</a:ErrorCode>
        <a:UserResponses>
          <a:UserResponse>
            <a:UserSettings>
              <a:UserSetting>
                <a:Name>PresentButEmpty</a:Name>
                <a:Value></a:Value>
              </a:UserSetting>
            </a:UserSettings>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let parsed = parse_user_settings_response(xml).expect("well-formed");
        assert_eq!(parsed.settings.len(), 1);
        assert_eq!(parsed.settings[0].0, "PresentButEmpty");
        assert_eq!(parsed.settings[0].1, "");
    }

    #[test]
    fn construct_replica_smtp_helper() {
        assert_eq!(
            construct_replica_smtp("1A2B3C4D-5E6F-7A8B-9C0D-1E2F3A4B5C6D", "contoso.com"),
            "1A2B3C4D-5E6F-7A8B-9C0D-1E2F3A4B5C6D@contoso.com"
        );
    }

    #[test]
    fn public_folder_routing_from_settings_maps_fields() {
        let settings = vec![
            (
                "PublicFolderInformation".to_string(),
                "publicfolders@contoso.com".to_string(),
            ),
            (
                "InternalRpcClientServer".to_string(),
                "server01.contoso.com".to_string(),
            ),
        ];
        let routing = public_folder_routing_from_settings(&settings).expect("routing");
        assert_eq!(routing.anchor_mailbox, "publicfolders@contoso.com");
        assert_eq!(
            routing.public_folder_mailbox.as_deref(),
            Some("server01.contoso.com")
        );

        // Absent PublicFolderInformation -> None.
        let only_server = vec![(
            "InternalRpcClientServer".to_string(),
            "server01.contoso.com".to_string(),
        )];
        assert!(public_folder_routing_from_settings(&only_server).is_none());
    }
}
