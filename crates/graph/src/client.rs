#[cfg(test)]
mod test_support;
#[cfg(test)]
use test_support::ScriptedRest;
#[cfg(test)]
pub(crate) use test_support::{ScriptedDownload, ScriptedRestResponse};

use std::sync::{Arc, RwLock};

use bifrost_net::{
    AccountId, AccountNet, AccountSpec, Net, RateLimit, StaticTokenSource, TokenSource,
};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{GraphError, GraphResponseError};
use crate::etag::GraphETag;
use crate::origin::{AdmittedUrl, Base, ProviderLink, Refusal, UploadUrl};

pub(crate) const GRAPH_API_BASE: &str = "https://graph.microsoft.com/v1.0";

/// Production origin for the two non-Graph Microsoft surfaces this crate
/// talks to: Exchange Autodiscover (`/autodiscover/autodiscover.xml`,
/// `/autodiscover/autodiscover.svc`) and EWS (`/EWS/Exchange.asmx`). Both
/// live on `outlook.office365.com`, NOT on the Graph host, so the Graph
/// api-base override alone never redirected them.
pub(crate) const OUTLOOK_BASE: &str = "https://outlook.office365.com";

/// Production Graph host. Used only to decide whether an api-base is the
/// real Graph endpoint or a harness redirect.
const GRAPH_HOST: &str = "graph.microsoft.com";

/// Paired Microsoft service endpoints, including private deployments.
///
/// Configure the Graph and Outlook token sources separately when these service
/// origins differ. Selecting a cloud configures endpoints and does not acquire
/// or exchange credentials.
#[derive(Clone, Debug)]
pub struct MicrosoftCloud {
    graph: Base,
    outlook: Base,
}

impl MicrosoftCloud {
    pub fn global() -> Self {
        Self::custom(GRAPH_API_BASE, OUTLOOK_BASE)
    }

    /// US Government L4 (GCC High). GCC uses the global cloud.
    pub fn us_gov() -> Self {
        Self::custom(
            "https://graph.microsoft.us/v1.0",
            "https://outlook.office365.us",
        )
    }

    pub fn us_gov_dod() -> Self {
        Self::custom(
            "https://dod-graph.microsoft.us/v1.0",
            "https://outlook-dod.office365.us",
        )
    }

    pub fn china() -> Self {
        Self::custom(
            "https://microsoftgraph.chinacloudapi.cn/v1.0",
            "https://partner.outlook.cn",
        )
    }

    /// Bases are validated independently; unusable bases fail closed on use.
    pub fn custom(graph: impl Into<String>, outlook: impl Into<String>) -> Self {
        Self {
            graph: Base::parse(graph),
            outlook: Base::parse(outlook),
        }
    }

    pub fn graph_base(&self) -> &str {
        self.graph.as_str()
    }

    pub fn outlook_base(&self) -> &str {
        self.outlook.as_str()
    }
}

/// Resource ownership is declared by the caller, including continuation walks.
pub(crate) enum Resource {
    Outlook,
    Batch(Vec<GraphClient>),
    Unmetered,
}

// pub: GraphAccountFactory consumers need a constructible Graph client handle.
#[derive(Clone)]
pub struct GraphClient {
    inner: Arc<ClientInner>,
    /// Where this handle reports request-local inbound bytes, if
    /// anywhere. `None` on every ordinary client; a stream that needs
    /// per-batch accounting takes a metered handle via `metered()`.
    /// Kept OUTSIDE `ClientInner` so installing accounting is one `Arc`
    /// bump over the same transport and script rather than a
    /// fork of the client's configuration.
    tally: Option<ByteTally>,
}

/// Both admission and throttle attribution are views of this same mailbox.
struct MailboxIdentity {
    budget: bifrost_types::ThrottleBudget,
    concurrency_key: bifrost_net::LimitKey,
}

struct ClientInner {
    net: Option<Net>,
    account_net: RwLock<Option<AccountNet>>,
    /// One configuration owns both bearer-carrying service bases.
    cloud: MicrosoftCloud,
    rate_limit_host: String,
    token_source: Arc<dyn TokenSource>,
    outlook_source: Option<bifrost_net::CoordinatedTokenSource>,
    mailbox_id: Option<String>,
    /// The primary mailbox's address (profile `mail`, else UPN), learned at
    /// `open`. Shared clients read the same slot to resolve primary aliases;
    /// the primary also uses it for its throttle budget. Refreshed on every open.
    primary_address: Arc<RwLock<Option<String>>>,
    primary_account: Arc<RwLock<Option<String>>>,
    /// Shared with every client derived from this one
    /// (`for_shared_mailbox`, `with_outlook_base`), like `AccountNet`. These derivatives
    /// issue their requests down the same transport, so a seam they did not
    /// share would leave every foreign-mailbox path unscriptable and
    /// silently outside the recorded request list.
    #[cfg(test)]
    scripted: Arc<std::sync::Mutex<ScriptedRest>>,
}

/// Inbound payload bytes attributed to one engine batch.
///
/// An engine batch here routinely covers several requests - a `$delta`
/// page plus its follow-ups, or a `$batch` submission plus the
/// per-item retries it spawned - so per-response totals have to be
/// summed somewhere. They are summed HERE, on an accumulator owned by
/// one stream, rather than sampled off the account-cumulative
/// bandwidth meter: that meter is shared by every concurrent request on
/// the account, so a delta across it would attribute another scope's
/// traffic to this batch.
///
/// `take` reads and resets, so consecutive batches from one stream
/// partition the bytes rather than each reporting a running total.
#[derive(Clone, Default)]
pub(crate) struct ByteTally(Arc<std::sync::atomic::AtomicU64>);

impl ByteTally {
    pub(crate) fn add(&self, n: u64) {
        self.0.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// Bytes recorded since the previous `take`.
    pub(crate) fn take(&self) -> u64 {
        self.0.swap(0, std::sync::atomic::Ordering::Relaxed)
    }
}

/// Graph-owned response shape at the one REST funnel. Adapts the
/// production `bifrost_net::Response` (which is `#[non_exhaustive]`) into
/// a shape this crate owns, so the helpers above the funnel destructure
/// it freely.
///
/// It carries no byte count: the funnel attributes inbound bytes from
/// the request-local counter instead, because that number also exists
/// when the request ends in an error, and a response-carried count does
/// not.
pub(crate) struct RestResponse {
    pub(crate) status: reqwest::StatusCode,
    pub(crate) headers: reqwest::header::HeaderMap,
    pub(crate) body: Bytes,
}

impl From<bifrost_net::Response> for RestResponse {
    fn from(response: bifrost_net::Response) -> Self {
        Self {
            status: response.status(),
            headers: response.headers,
            body: response.body,
        }
    }
}

/// Request body plus the content type it travels under, at the one wire
/// funnel. JSON callers serialize up front instead of handing
/// `RequestBuilder::json` a `Serialize` value, which keeps the funnel
/// non-generic so the raw-MIME caller (`post_mime`) can share it - and
/// therefore share the test seam - rather than opening a second,
/// unscriptable path to `AccountNet`.
struct WireBody {
    content_type: &'static str,
    bytes: Bytes,
}

/// Content type every JSON call sends, and the one a bodiless call still
/// sends (unchanged from before the funnel merge).
const JSON_CONTENT_TYPE: &str = "application/json";

/// Graph's import-from-MIME content type: the body is the base64 of the
/// RFC 5322 octets, not JSON.
const MIME_CONTENT_TYPE: &str = "text/plain";

impl WireBody {
    fn json<B: Serialize + ?Sized>(body: &B) -> Result<Self, GraphError> {
        // Mirrors `RequestBuilder::json`'s failure shape so a body whose
        // `Serialize` impl fails still surfaces as `EncodeBody`.
        let bytes = serde_json::to_vec(body).map_err(|error| {
            GraphError::Net(bifrost_net::Error::EncodeBody {
                message: format!("serde_json::to_vec failed: {error}"),
                source: Some(Box::new(error)),
            })
        })?;
        Ok(Self {
            content_type: JSON_CONTENT_TYPE,
            bytes: Bytes::from(bytes),
        })
    }

    fn mime(bytes: Bytes) -> Self {
        Self {
            content_type: MIME_CONTENT_TYPE,
            bytes,
        }
    }
}

/// Where an auxiliary request goes, and therefore whether it carries the
/// account bearer: only an admitted URL may, so the choice is made by type,
/// not by a flag a caller could set next to any string.
pub(crate) enum AuxTarget<'a> {
    /// Carries the bearer; admitted onto a configured origin.
    Bearer(&'a AdmittedUrl),
    /// Carries no bearer: a URL that is its own credential (the OneDrive
    /// pre-authenticated upload session), admitted under the upload rule.
    Anonymous(&'a UploadUrl),
}

#[derive(Default)]
struct RequestOptions<'a> {
    prefer: Option<&'a str>,
    eventual_consistency: bool,
}

impl GraphClient {
    // pub: ergonomic constructor for the default Microsoft Graph endpoint.
    pub fn new(access_token: impl Into<String>) -> Self {
        Self::with_api_base(GRAPH_API_BASE, access_token)
    }

    /// Configure Graph and Exchange together before registering the account.
    pub fn with_cloud(cloud: MicrosoftCloud, source: Arc<dyn TokenSource>) -> Self {
        let mut client = Self::with_source(cloud.graph_base(), source);
        Arc::get_mut(&mut client.inner)
            .expect("new client is exclusive")
            .cloud = cloud;
        client
    }

    /// Inject an account transport with an explicit pair of cloud endpoints.
    pub fn with_cloud_account_net(
        net: AccountNet,
        cloud: MicrosoftCloud,
        source: Arc<dyn TokenSource>,
    ) -> Self {
        let mut client = Self::with_account_net(net, cloud.graph_base(), source);
        Arc::get_mut(&mut client.inner)
            .expect("new client is exclusive")
            .cloud = cloud;
        client
    }

    // pub: consumers may need sovereign-cloud or test Graph API endpoints before registration.
    pub fn with_api_base(api_base: impl Into<String>, access_token: impl Into<String>) -> Self {
        // Bases are trimmed once in `with_bases_and_source`.
        let token_source: Arc<dyn TokenSource> =
            Arc::new(StaticTokenSource::new(access_token, None));
        Self::with_source(api_base, token_source)
    }

    // pub: source-accepting constructor. ratatoskr hands in a shared
    // `Arc<dyn TokenSource>` (typically an `OAuthRefresher` over its own
    // refresh-token store) so a token it refreshes and persists is read
    // live at every wire authentication without reopening the client.
    pub fn with_source(api_base: impl Into<String>, source: Arc<dyn TokenSource>) -> Self {
        let api_base = Base::parse(api_base);
        let rate_limit_host = host_from_api_base(&api_base);
        let outlook_base = derive_outlook_base(&api_base);
        Self {
            tally: None,
            inner: Arc::new(ClientInner {
                net: Some(Net::shared_default()),
                account_net: RwLock::new(None),
                cloud: MicrosoftCloud {
                    graph: api_base,
                    outlook: outlook_base,
                },
                rate_limit_host,
                token_source: source,
                outlook_source: None,
                mailbox_id: None,
                primary_address: Arc::new(RwLock::new(None)),
                primary_account: Arc::new(RwLock::new(None)),
                #[cfg(test)]
                scripted: Arc::new(std::sync::Mutex::new(ScriptedRest::default())),
            }),
        }
    }

    // pub: custom Net injection lets consumers opt out of Net::shared_default host buckets.
    pub fn with_account_net(
        net: AccountNet,
        api_base: impl Into<String>,
        token_source: Arc<dyn TokenSource>,
    ) -> Self {
        let api_base = Base::parse(api_base);
        let outlook_base = derive_outlook_base(&api_base);
        // Derived from the supplied base like every other constructor,
        // not hardcoded: an injected net against a redirected base must
        // meter under its own host bucket, not the production Graph
        // host's.
        let rate_limit_host = host_from_api_base(&api_base);
        Self {
            tally: None,
            inner: Arc::new(ClientInner {
                net: None,
                account_net: RwLock::new(Some(net.clone())),
                cloud: MicrosoftCloud {
                    graph: api_base,
                    outlook: outlook_base,
                },
                rate_limit_host,
                token_source,
                outlook_source: None,
                mailbox_id: None,
                primary_address: Arc::new(RwLock::new(None)),
                primary_account: Arc::new(RwLock::new(Some(net.account().0.clone()))),
                #[cfg(test)]
                scripted: Arc::new(std::sync::Mutex::new(ScriptedRest::default())),
            }),
        }
    }

    /// Bind this client to the engine's account id so metering, priority,
    /// caps and tracing use the real key.
    ///
    /// Audit boundary: the 2026-07 google+net bug sweep line-audited this
    /// crate only along the reattach path through here; everything else in
    /// `bifrost-graph` was covered by its own tests and the later 2026-09-04
    /// hunt, not by that sweep. Listed so a future auditor knows where that
    /// sweep's coverage stopped.
    pub(crate) fn attach_account(&self, account_id: AccountId) {
        self.fix_primary_account(&account_id.0);
        if let Some(net) = self.inner.net.as_ref() {
            let token_source = Arc::clone(&self.inner.token_source);
            let mut spec = AccountSpec::new(Some(token_source));
            spec.hosts = vec![
                RateLimit::new(self.inner.rate_limit_host.clone(), 10.0, 1, 10).with_quota_scope(
                    bifrost_net::QuotaScope::namespaced("graph.quota", &[&account_id.0]),
                ),
            ];
            let account_net = net.attach_account(account_id, spec);
            // Install the replacement first, then tear down whatever
            // registration it displaced. `Net::attach_account` mints a
            // fresh token on every call and no longer unregisters a
            // previous attachment for the same id, so a reopen that
            // dropped its old handle without detaching would leak one
            // meter attachment and one governor attach count per
            // cycle - the host bucket would never be reclaimed.
            // Detaching after the install keeps the shared counts from
            // dipping to zero, so in-flight requests on the old handle
            // keep metering against the same counters.
            let displaced = match self.inner.account_net.write() {
                Ok(mut slot) => slot.replace(account_net),
                Err(poisoned) => {
                    tracing::warn!("account_net lock poisoned; recovering attachment slot");
                    poisoned.into_inner().replace(account_net)
                }
            };
            if let Some(displaced) = displaced {
                displaced.detach();
            }
            return;
        }
        // No parent `Net`: the consumer constructed us via
        // `with_account_net`. Retag the existing `AccountNet` so
        // per-account metering and host bookkeeping move under the
        // engine id; in-flight requests on the old handle continue.
        let existing = self.account_net();
        let Some(existing) = existing else {
            return;
        };
        if existing.account() == &account_id {
            return;
        }
        let retagged = existing.retag(account_id);
        let mut slot = self.inner.account_net.write().unwrap_or_else(|poisoned| {
            tracing::warn!("account_net lock poisoned; recovering attachment slot");
            poisoned.into_inner()
        });
        *slot = Some(retagged);
    }

    pub(crate) fn account_net(&self) -> Option<AccountNet> {
        self.inner
            .account_net
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn api_base(&self) -> &str {
        self.inner.cloud.graph.as_str()
    }

    /// Origin for the Autodiscover + EWS surfaces.
    #[cfg(test)]
    pub(crate) fn outlook_base(&self) -> &str {
        self.inner.cloud.outlook.as_str()
    }

    /// A crate-built Graph path, admitted onto the api-base. The only way
    /// it fails is an unusable configured base, which is the consumer's
    /// configuration, so it surfaces as `GraphError::Configuration`.
    pub(crate) fn api_url(&self, path: &str) -> Result<AdmittedUrl, GraphError> {
        self.inner
            .cloud
            .graph
            .admit(path)
            .map_err(configuration_refusal)
    }

    /// Admit a link a Graph response supplied. Called at RECEIPT - before
    /// the link is followed, minted into a caller cursor, or persisted - so
    /// a refusal is attributed to the response that carried it.
    pub(crate) fn admit_link(&self, link: &ProviderLink) -> Result<AdmittedUrl, GraphError> {
        self.inner
            .cloud
            .graph
            .admit_link(link)
            .map_err(|refusal| match refusal {
                Refusal::InvalidBase(_) => configuration_refusal(refusal),
                Refusal::Target(reason) => GraphError::ProviderLinkRefused { reason },
            })
    }

    /// Admit a page's optional `@odata.nextLink`, the step every walk takes
    /// before following it.
    pub(crate) fn admit_next(
        &self,
        link: Option<&ProviderLink>,
    ) -> Result<Option<AdmittedUrl>, GraphError> {
        link.map(|link| self.admit_link(link)).transpose()
    }

    /// Admit a target whose provenance the CALLER knows (a caller page
    /// cursor, a persisted sync cursor) and will classify itself.
    pub(crate) fn admit_target(&self, target: &str) -> Result<AdmittedUrl, Refusal> {
        self.inner.cloud.graph.admit(target)
    }

    /// Admit a OneDrive upload session URL under the upload rule (see
    /// `Base::admit_upload`). The caller classifies the refusal.
    pub(crate) fn admit_upload(&self, raw: &str) -> Result<UploadUrl, Refusal> {
        self.inner.cloud.graph.admit_upload(raw)
    }

    /// A crate-built Autodiscover / EWS path, admitted onto the Outlook
    /// origin.
    pub(crate) fn outlook_url(&self, path: &str) -> Result<AdmittedUrl, GraphError> {
        self.inner
            .cloud
            .outlook
            .admit(path)
            .map_err(configuration_refusal)
    }

    /// The EWS SOAP endpoint on the Outlook origin, or why there is none.
    pub(crate) fn ews_url(&self) -> Result<AdmittedUrl, String> {
        self.inner
            .cloud
            .outlook
            .admit(crate::ews::EWS_PATH)
            .map_err(|refusal| refusal.to_string())
    }

    /// Admit an endpoint an Autodiscover response named, onto the Outlook
    /// origin. The caller classifies the refusal.
    pub(crate) fn admit_outlook_target(&self, target: &str) -> Result<AdmittedUrl, Refusal> {
        self.inner.cloud.outlook.admit(target)
    }

    /// Supply the OAuth source for the configured Outlook service audience.
    /// Configure this before opening accounts; endpoint selection does not
    /// acquire or exchange the Graph token for an Exchange token.
    #[must_use]
    pub fn with_outlook_source(&self, source: Arc<dyn TokenSource>) -> Self {
        let mut client = self.with_outlook_base(self.inner.cloud.outlook.as_str());
        Arc::get_mut(&mut client.inner)
            .expect("new client is exclusive")
            .outlook_source = Some(bifrost_net::coordinate(source));
        client
    }

    pub(crate) fn outlook_auth(&self) -> Arc<dyn bifrost_net::HopPolicy> {
        // A user enabling Exchange expects its own credential to be used.
        // A missing source must never disclose a Graph token to another origin.
        Arc::new(OutlookAuth {
            base: self.inner.cloud.outlook.clone(),
            source: self.inner.outlook_source.clone().or_else(|| {
                self.inner
                    .cloud
                    .graph
                    .admit(self.inner.cloud.outlook.as_str())
                    .ok()
                    .map(|_| bifrost_net::coordinate(Arc::clone(&self.inner.token_source)))
            }),
        })
    }

    /// Override the Autodiscover / EWS base while retaining the Graph base.
    /// Named Microsoft clouds already pair the two surfaces; this also supports
    /// custom deployments whose service endpoints live on different origins.
    #[must_use]
    pub fn with_outlook_base(&self, outlook_base: impl Into<String>) -> Self {
        Self {
            tally: self.tally.clone(),
            inner: Arc::new(ClientInner {
                net: self.inner.net.clone(),
                account_net: RwLock::new(self.account_net()),
                cloud: MicrosoftCloud {
                    graph: self.inner.cloud.graph.clone(),
                    outlook: Base::parse(outlook_base),
                },
                rate_limit_host: self.inner.rate_limit_host.clone(),
                token_source: Arc::clone(&self.inner.token_source),
                outlook_source: self.inner.outlook_source.clone(),
                mailbox_id: self.inner.mailbox_id.clone(),
                primary_address: Arc::clone(&self.inner.primary_address),
                primary_account: Arc::clone(&self.inner.primary_account),
                #[cfg(test)]
                scripted: Arc::clone(&self.inner.scripted),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) async fn access_token(&self) -> String {
        self.inner
            .token_source
            .current()
            .await
            .expect("static token source is infallible")
            .as_str()
            .to_string()
    }

    pub(crate) fn api_path_prefix(&self) -> String {
        match &self.inner.mailbox_id {
            Some(id) => format!("/users/{}", bifrost_net::url::encode_path_component(id)),
            None => "/me".to_string(),
        }
    }

    fn primary_address(&self) -> Option<String> {
        self.inner
            .primary_address
            .read()
            .ok()
            .and_then(|slot| slot.clone())
    }

    /// Record the primary mailbox's address, learned from the profile at
    /// `open`, so the primary client's throttle budget names that mailbox.
    /// Derived mailbox clients may read this slot but cannot overwrite it.
    pub(crate) fn set_primary_address(&self, address: Option<String>) {
        if self.inner.mailbox_id.is_some() {
            return;
        }
        if let Ok(mut slot) = self.inner.primary_address.write() {
            *slot = address;
        }
    }

    /// The provider rate budget every Outlook request this client issues is
    /// charged to: Microsoft meters Outlook mail, calendar and contacts per
    /// app per MAILBOX, and one mailbox's throttle does not affect another.
    ///
    /// The id is the API origin plus the mailbox the client routes to: the
    /// `/users/{key}` routing key for a shared-mailbox client, the profile
    /// address for the primary (whose requests go to `/me`). Both are
    /// lowercased, since Exchange addresses are case-insensitive. With no
    /// profile address the primary falls back to the engine account id,
    /// which is unique per account and so never shares.
    ///
    /// It is an approximation of the provider's identity, stated rather than
    /// hidden. A mailbox reached through two address forms gets two budgets
    /// (missed sharing); an address reassigned to another mailbox while a
    /// deadline is pending, or a stale profile address, can hold the wrong
    /// mailbox (false sharing). The app id that also keys Microsoft's budget
    /// is behind an opaque `TokenSource`, so accounts on different apps share
    /// too widely. Each deadline is in memory and expires on its own; the
    /// cost is pacing, never a misrouted request.
    pub(crate) fn mailbox_budget(&self) -> bifrost_types::ThrottleBudget {
        self.mailbox_identity().budget
    }

    fn fix_primary_account(&self, account: &str) {
        let mut slot = self
            .inner
            .primary_account
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        slot.get_or_insert_with(|| account.to_owned());
    }

    // Both throttle declarations and response stamps call mailbox_budget,
    // which mints only here. Keep this ownership with the registry identity:
    // an Outlook user expects a shared mailbox refusal to hold that mailbox.
    fn mailbox_identity(&self) -> MailboxIdentity {
        // Retagging changes transport accounting, not mailbox identity. Both
        // admission and throttle fallback retain the first attached account.
        let primary = self
            .inner
            .primary_account
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            // Before the first admission has fixed the primary account, fall
            // back to the attached account rather than an empty tag that every
            // unaddressed client on this origin would share.
            .or_else(|| self.account_net().map(|net| net.account().0.clone()))
            .unwrap_or_default();
        let address = self
            .primary_address()
            .map(|address| address.to_ascii_lowercase());
        let owner = self.inner.mailbox_id.as_ref();
        let budget_owner = owner.or(address.as_ref()).map_or_else(
            || format!("account:{primary}"),
            |id| format!("mailbox:{id}"),
        );
        let concurrency_owner = match owner {
            Some(id) if Some(id) != address.as_ref() => format!("mailbox:{id}"),
            _ => format!("primary|{primary}"),
        };
        // Origin only: v1.0 and beta share one provider mailbox budget.
        let origin = self.inner.cloud.graph.url().map_or_else(
            || self.inner.cloud.graph.as_str().to_owned(),
            |url| url.origin().ascii_serialization(),
        );
        MailboxIdentity {
            budget: bifrost_types::ThrottleBudget::new(format!(
                "graph-outlook|{origin}|{budget_owner}"
            )),
            concurrency_key: bifrost_net::LimitKey::namespaced(
                "graph.outlook",
                &[&origin, &concurrency_owner],
            ),
        }
    }

    fn concurrency_key(&self, net: &AccountNet) -> bifrost_net::LimitKey {
        self.fix_primary_account(&net.account().0);
        self.mailbox_identity().concurrency_key
    }

    fn claims(
        &self,
        resource: &Resource,
        net: &AccountNet,
    ) -> Vec<(bifrost_net::ConcurrencyLimit, usize)> {
        let mut groups = std::collections::BTreeMap::<bifrost_net::LimitKey, usize>::new();
        match resource {
            Resource::Outlook => {
                groups.insert(self.concurrency_key(net), 1);
            }
            Resource::Batch(clients) => {
                for client in clients {
                    *groups.entry(client.concurrency_key(net)).or_default() += 1;
                }
            }
            Resource::Unmetered => {}
        }
        groups
            .into_iter()
            .map(|(key, count)| (net.shared_concurrency_limit(key, 4), count.min(4)))
            .collect()
    }

    /// Outer batch failures have no single owning mailbox; subresponses
    /// are stamped by their per-item classifiers instead.
    fn with_mailbox_context(&self, resource: &Resource, error: GraphError) -> GraphError {
        // Carry routing context on every Outlook failure. Only classification
        // decides whether it is a throttle; an unavailable server must retry
        // without holding unrelated work as if a mailbox quota were exhausted.
        if !matches!(resource, Resource::Outlook) {
            return error;
        }
        GraphError::MailboxContext {
            budget: self.mailbox_budget(),
            source: Box::new(error),
        }
    }

    // pub: shared-mailbox consumers derive a scoped client before building the factory.
    pub fn for_shared_mailbox(&self, mailbox_id: impl Into<String>) -> Self {
        Self {
            tally: self.tally.clone(),
            inner: Arc::new(ClientInner {
                net: self.inner.net.clone(),
                account_net: RwLock::new(self.account_net()),
                cloud: self.inner.cloud.clone(),
                rate_limit_host: self.inner.rate_limit_host.clone(),
                token_source: Arc::clone(&self.inner.token_source),
                outlook_source: self.inner.outlook_source.clone(),
                mailbox_id: Some(mailbox_id.into().to_ascii_lowercase()),
                primary_address: Arc::clone(&self.inner.primary_address),
                primary_account: Arc::clone(&self.inner.primary_account),
                #[cfg(test)]
                scripted: Arc::clone(&self.inner.scripted),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_shared_mailbox(&self) -> bool {
        self.inner.mailbox_id.is_some()
    }

    pub(crate) fn uses_default_mailbox(&self) -> bool {
        self.inner.mailbox_id.is_none()
    }

    #[cfg(test)]
    pub(crate) fn mailbox_id(&self) -> Option<&str> {
        self.inner.mailbox_id.as_deref()
    }

    pub(crate) async fn get_json<T: DeserializeOwned>(
        &self,
        resource: Resource,
        path: &str,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        self.get(resource, &url).await
    }

    pub(crate) async fn get_json_prefer<T: DeserializeOwned>(
        &self,
        resource: Resource,
        path: &str,
        prefer: &str,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        self.get_prefer(resource, &url, prefer).await
    }

    /// GET an admitted URL: a crate-built path (`api_url`) or an admitted
    /// cursor / server link. There is no string-taking twin on purpose.
    pub(crate) async fn get<T: DeserializeOwned>(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
    ) -> Result<T, GraphError> {
        self.request::<T, ()>(
            resource,
            url,
            reqwest::Method::GET,
            None,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
    }

    pub(crate) async fn get_eventual<T: DeserializeOwned>(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
        eventual: bool,
    ) -> Result<T, GraphError> {
        let response = self
            .execute_wire(
                resource,
                url,
                reqwest::Method::GET,
                None,
                RequestOptions {
                    prefer: None,
                    eventual_consistency: eventual,
                },
                None,
                bifrost_net::ReplayPolicy::Idempotent,
            )
            .await?;
        parse_json_response(response)
    }

    pub(crate) async fn get_prefer<T: DeserializeOwned>(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
        prefer: &str,
    ) -> Result<T, GraphError> {
        self.request_prefer::<T, ()>(
            resource,
            url,
            reqwest::Method::GET,
            prefer,
            None,
            bifrost_net::ReplayPolicy::Idempotent,
        )
        .await
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        resource: Resource,
        path: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        self.request(resource, &url, reqwest::Method::POST, Some(body), replay)
            .await
    }

    pub(crate) async fn post_empty(
        &self,
        resource: Resource,
        path: &str,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute(resource, &url, reqwest::Method::POST, None::<&()>, replay)
            .await?;
        check_response_status(response)
    }

    pub(crate) async fn post_no_response<B: Serialize>(
        &self,
        resource: Resource,
        path: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute(resource, &url, reqwest::Method::POST, Some(body), replay)
            .await?;
        check_response_status(response)
    }

    /// POST a base64-encoded RFC 5322 message to create a draft from raw
    /// MIME (Graph's import-from-MIME path: `Content-Type: text/plain`,
    /// body = the base64 of the MIME octets). Returns the created message
    /// resource. Distinct from `post` because that path hardcodes a JSON
    /// content type and serializes its body; raw MIME needs neither.
    pub(crate) async fn post_mime<T: DeserializeOwned>(
        &self,
        resource: Resource,
        path: &str,
        base64_mime: Bytes,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute_wire(
                resource,
                &url,
                reqwest::Method::POST,
                None,
                RequestOptions::default(),
                Some(WireBody::mime(base64_mime)),
                replay,
            )
            .await?;
        parse_json_response(response)
    }

    pub(crate) async fn patch<B: Serialize>(
        &self,
        resource: Resource,
        path: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute(resource, &url, reqwest::Method::PATCH, Some(body), replay)
            .await?;
        check_response_status(response)
    }

    pub(crate) async fn patch_if_match<B: Serialize>(
        &self,
        resource: Resource,
        path: &str,
        etag: &GraphETag,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute_if_match(
                resource,
                &url,
                reqwest::Method::PATCH,
                etag,
                Some(body),
                replay,
            )
            .await?;
        check_response_status(response)
    }

    pub(crate) async fn patch_json<T: DeserializeOwned, B: Serialize>(
        &self,
        resource: Resource,
        path: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path)?;
        self.request(resource, &url, reqwest::Method::PATCH, Some(body), replay)
            .await
    }

    pub(crate) async fn delete(
        &self,
        resource: Resource,
        path: &str,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute(resource, &url, reqwest::Method::DELETE, None::<&()>, replay)
            .await?;
        check_response_status(response)
    }

    pub(crate) async fn delete_if_match(
        &self,
        resource: Resource,
        path: &str,
        etag: &GraphETag,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path)?;
        let response = self
            .execute_if_match(
                resource,
                &url,
                reqwest::Method::DELETE,
                etag,
                None::<&()>,
                replay,
            )
            .await?;
        check_response_status(response)
    }

    pub(crate) async fn post_batch(
        &self,
        resource: Resource,
        batch: &crate::types::BatchRequest,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<crate::types::BatchResponse, GraphError> {
        self.post(resource, "/$batch", batch, replay).await
    }

    async fn request<T: DeserializeOwned, B: Serialize>(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
        method: reqwest::Method,
        body: Option<&B>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T, GraphError> {
        let response = self.execute(resource, url, method, body, replay).await?;
        parse_json_response(response)
    }

    async fn request_prefer<T: DeserializeOwned, B: Serialize>(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
        method: reqwest::Method,
        prefer: &str,
        body: Option<&B>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T, GraphError> {
        let response = self
            .execute_request(resource, url, method, None, Some(prefer), body, replay)
            .await?;
        parse_json_response(response)
    }

    async fn execute<B: Serialize>(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
        method: reqwest::Method,
        body: Option<&B>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<RestResponse, GraphError> {
        self.execute_request(resource, url, method, None, None, body, replay)
            .await
    }

    async fn execute_if_match<B: Serialize>(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
        method: reqwest::Method,
        etag: &GraphETag,
        body: Option<&B>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<RestResponse, GraphError> {
        self.execute_request(resource, url, method, Some(etag), None, body, replay)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_request<B: Serialize>(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
        method: reqwest::Method,
        if_match: Option<&GraphETag>,
        prefer: Option<&str>,
        body: Option<&B>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<RestResponse, GraphError> {
        let body = match body {
            Some(body) => Some(WireBody::json(body)?),
            None => None,
        };
        self.execute_wire(
            resource,
            url,
            method,
            if_match,
            RequestOptions {
                prefer,
                eventual_consistency: false,
            },
            body,
            replay,
        )
        .await
    }

    /// The one place a Graph REST request leaves this crate. Every helper
    /// above funnels through here, raw MIME included, so the scripted seam
    /// covers the whole surface and no path can quietly bypass it.
    ///
    /// The URL must be an `AdmittedUrl`: bifrost-net attaches the
    /// bearer to the first request whatever its host, so the origin decision
    /// has to be made before this point, and the type proves it was.
    #[allow(clippy::too_many_arguments)]
    async fn execute_wire(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
        method: reqwest::Method,
        if_match: Option<&GraphETag>,
        options: RequestOptions<'_>,
        body: Option<WireBody>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<RestResponse, GraphError> {
        let RequestOptions {
            prefer,
            eventual_consistency,
        } = options;
        let url = url.as_str();
        // Records the request in this crate's richer shape and does not
        // answer it: the response comes back from the scripted transport
        // below, through the production retry loop.
        #[cfg(test)]
        self.record_wire(
            method.as_str(),
            url,
            if_match.map(GraphETag::as_str),
            prefer,
            body.as_ref(),
        );
        let account_net = self.attached_net()?;

        // This funnel has no AccountOperation, and needs none: the caller
        // knows the request and its payload, and declares its replay policy
        // (applied below). Batch POSTs, conditional PATCHes and absolute
        // updates all pass through here, so no verb-level default would do.
        let mut builder = account_net.request(method, url);

        for (limit, weight) in self.claims(&resource, &account_net) {
            builder = builder.concurrency_claim(limit, weight);
        }

        if eventual_consistency {
            builder = builder.header("ConsistencyLevel", "eventual");
        }
        if let Some(etag) = if_match {
            builder = builder.header("If-Match", etag.as_str());
        }
        if let Some(prefer) = prefer {
            builder = builder.header("Prefer", prefer);
        }

        // Content-Type describes the representation carried by the body.
        // Bodyless GET and DELETE requests must not announce a JSON body.
        if let Some(body) = body {
            builder = builder
                .header("Content-Type", body.content_type)
                .body(body.bytes);
        }

        builder = builder.replay(replay);
        let counter = bifrost_net::RequestByteCounter::new();
        let sent = builder.count_bytes_into(counter.clone()).send().await;
        self.record_bytes(&counter);
        let response =
            sent.map_err(|error| self.with_mailbox_context(&resource, GraphError::Net(error)))?;
        Ok(RestResponse::from(response))
    }

    /// The one place the two NON-REST wire paths leave this crate: the
    /// pre-authenticated OneDrive upload session (chunk PUTs and the cancel
    /// DELETE, bearer suppressed, because the session URL carries its own
    /// credential and forwarding the Graph token to it would leak it) and
    /// the Autodiscover POST (XML to the Autodiscover origin, which is not
    /// the Graph host).
    ///
    /// `timeout`, when given, is the request's TOTAL deadline in bifrost-net
    /// (admission, every attempt, retry backoff, redirects, body drain), so a
    /// caller spending one budget across several requests passes what is
    /// left of it and bifrost-net classifies an expiry by the stage it hit.
    /// `None` keeps the account default.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_aux(
        &self,
        method: reqwest::Method,
        target: AuxTarget<'_>,
        headers: &[(&str, &str)],
        body: Bytes,
        timeout: Option<std::time::Duration>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<RestResponse, GraphError> {
        // Autodiscover is ungated: EWS throttling is per calling user under
        // its own policy, and streaming notifications have a separate
        // subscription budget. The REST per-mailbox four does not apply.
        #[cfg(test)]
        let (url, bearer) = match &target {
            AuxTarget::Bearer(url) => (url.as_str(), true),
            AuxTarget::Anonymous(url) => (url.as_str(), false),
        };
        #[cfg(test)]
        self.record_aux(method.as_str(), url, headers, bearer, &body);
        let account_net = self.attached_net()?;
        let mut builder = match target {
            AuxTarget::Bearer(url) => account_net
                .request(method, url.as_str())
                .hop_policy(self.outlook_auth()),
            AuxTarget::Anonymous(url) => account_net
                .session_request(method, &bifrost_net::SessionUrl::new(url.as_str()))
                .into(),
        };
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        builder = builder.replay(replay);
        let counter = bifrost_net::RequestByteCounter::new();
        let sent = builder
            .body(body)
            .count_bytes_into(counter.clone())
            .send()
            .await;
        self.record_bytes(&counter);
        // The aux paths are never mailbox-metered, so no mailbox context
        // applies to their failures.
        let response = sent.map_err(|error| {
            self.with_mailbox_context(&Resource::Unmetered, GraphError::Net(error))
        })?;
        Ok(RestResponse::from(response))
    }

    /// Attribute one request's inbound bytes to this handle's batch
    /// accumulator, if it has one. Applied at BOTH wire funnels: the
    /// pre-authenticated chunk PUT and the Autodiscover POST are real
    /// inbound traffic on the account and would otherwise be free.
    ///
    /// Takes the request-local counter rather than the `Response`, so
    /// the bytes are attributed whether the request succeeded or
    /// failed. `bifrost-net` drains non-2xx bodies, exhausted retries
    /// and repeated 401s before converting them to an error, and the
    /// mutation and hydration lanes turn such an error into a per-item
    /// failure while still emitting a batch - so recording only on
    /// success would under-report exactly the batches that hit trouble.
    fn record_bytes(&self, counter: &bifrost_net::RequestByteCounter) {
        if let Some(tally) = self.tally.as_ref() {
            tally.add(counter.bytes_in());
        }
    }

    /// A handle over the same client that reports every buffered
    /// response's inbound bytes into a fresh accumulator.
    ///
    /// One accumulator per stream, not per client: the returned handle
    /// is the only one recording into it, so concurrent work on other
    /// scopes cannot contaminate the total. Clients DERIVED from this
    /// handle (`for_shared_mailbox`, `with_outlook_base`) inherit it,
    /// because in production those derivatives issue their requests
    /// down the same transport on behalf of the same batch.
    pub(crate) fn metered(&self) -> (Self, ByteTally) {
        let tally = ByteTally::default();
        (self.with_tally(tally.clone()), tally)
    }

    /// The same handle reporting into an EXISTING accumulator. Used to
    /// enroll the shared-mailbox clients of one account into the batch
    /// accumulator the primary client already carries, so a
    /// foreign-mailbox request is attributed to the batch that made it.
    pub(crate) fn with_tally(&self, tally: ByteTally) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            tally: Some(tally),
        }
    }

    /// The batch accumulator carried by this handle, when the caller is
    /// operating on a metered account view. EWS composes `AccountNet`
    /// directly, so its buffered funnel enrolls in the same accumulator
    /// through this accessor rather than passing through a REST funnel.
    pub(crate) fn batch_tally(&self) -> Option<ByteTally> {
        self.tally.clone()
    }

    /// The one place a Graph blob byte stream is opened. Production is the
    /// `AccountNet` download (retry, rate limit, bandwidth metering, the
    /// `Range` / `206` contract); tests script the chunk sequence or the
    /// open failure.
    pub(crate) async fn download_stream(
        &self,
        resource: Resource,
        url: &AdmittedUrl,
        range: Option<bifrost_types::ByteRange>,
    ) -> Result<bifrost_net::ByteStream, GraphError> {
        let url = url.as_str();
        #[cfg(test)]
        self.record_download(url, range);
        let account_net = self.attached_net()?;
        // Downloads stay ungated: a streamed body holds its permit until
        // drained or dropped, and Graph has no admission deadline. Gating
        // risks an indefinite local stall where a 429 would be recoverable.
        account_net
            .download_stream(url, range)
            .await
            .map_err(|error| self.with_mailbox_context(&resource, GraphError::Net(error)))
    }

    /// The transport a wire call should use: the scripted one under test
    /// when a script is installed, otherwise this client's own attached
    /// `AccountNet`. In production this is always the latter.
    ///
    /// An unattached client is unreachable: `GraphAccountFactory::open`
    /// attaches before any `GraphAccount` exists, `with_account_net` fills
    /// the slot at construction, derived clients copy a filled slot, and
    /// nothing empties it. So an empty or poisoned slot is the client's own
    /// invariant failing (`GraphError::Internal`), and it must not be
    /// dressed as a network fault: a retry of the same request never
    /// re-attaches, so a retryable classification could only loop.
    fn attached_net(&self) -> Result<AccountNet, GraphError> {
        #[cfg(test)]
        if let Some(scripted) = self.scripted_net() {
            return Ok(scripted);
        }
        match self.inner.account_net.read() {
            Ok(slot) => slot.clone().ok_or_else(|| GraphError::Internal {
                message: "Graph client is not attached to an account".to_string(),
            }),
            Err(_) => Err(GraphError::Internal {
                message: "Graph client account_net lock poisoned".to_string(),
            }),
        }
    }
}

/// Where a request that could not name its own target goes: an unusable
/// configured base is the consumer's configuration, so it is a malformed
/// request, not a transport or provider condition.
fn configuration_refusal(refusal: Refusal) -> GraphError {
    GraphError::Configuration {
        message: refusal.to_string(),
    }
}

/// The rate-limit bucket host. An unusable base falls back to the production
/// Graph host only for bookkeeping: no request can be admitted against it.
fn host_from_api_base(api_base: &Base) -> String {
    api_base
        .url()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| GRAPH_HOST.to_string())
}

/// Compatibility inference for the single-base constructors. Known Microsoft
/// hosts select the paired cloud; an unknown deployment retains the historical
/// same-origin harness behavior. Explicit clouds avoid inference altogether.
/// An enterprise Outlook user expects Exchange reads to remain in their cloud.
fn derive_outlook_base(api_base: &Base) -> Base {
    let Some(url) = api_base.url() else {
        return Base::invalid(
            api_base.as_str(),
            "Outlook origin derived from an unusable Graph api-base",
        );
    };
    let cloud = match url.host_str() {
        Some(GRAPH_HOST) => Some(MicrosoftCloud::global()),
        Some("graph.microsoft.us") => Some(MicrosoftCloud::us_gov()),
        Some("dod-graph.microsoft.us") => Some(MicrosoftCloud::us_gov_dod()),
        Some("microsoftgraph.chinacloudapi.cn") => Some(MicrosoftCloud::china()),
        _ => None,
    };
    if let Some(cloud) = cloud {
        return cloud.outlook;
    }
    // `validate` guarantees a host on a usable base.
    Base::parse(url.origin().ascii_serialization())
}

/// Parse a Graph JSON success response. On a non-success status,
/// constructs a `GraphResponseError` from the raw response so the
/// account boundary receives structured evidence, not a formatted
/// string.
///
/// The reachable non-success statuses here are the 3xx `bifrost-net`
/// passes through (304 on a conditional read, and a redirect no one can
/// follow); 4xx and 5xx never arrive as a response at all, because the
/// transport converts them into `bifrost_net::Error` first. Those are
/// re-decoded against Graph's error envelope at the account boundary
/// (`graph_error::into_account_error`), not here.
fn parse_json_response<T: DeserializeOwned>(response: RestResponse) -> Result<T, GraphError> {
    let status = response.status;
    let RestResponse { headers, body, .. } = response;
    if !status.is_success() {
        let err = GraphResponseError::from_response(status, headers, body);
        return Err(GraphError::Response(err));
    }

    // `GraphError::Json` is shared with the Autodiscover XML and OneDrive
    // upload readers, and its diagnostic label names no format, so the
    // message says which body failed.
    serde_json::from_slice(body.as_ref()).map_err(|e| GraphError::Json {
        message: format!("Graph REST JSON body: {e}"),
        body: if body.is_empty() { None } else { Some(body) },
    })
}

/// Check a Graph response for success status. On failure, constructs
/// a `GraphResponseError` from the raw response.
fn check_response_status(response: RestResponse) -> Result<(), GraphError> {
    let status = response.status;
    if status.is_success() {
        return Ok(());
    }
    let body = Bytes::copy_from_slice(response.body.as_ref());
    let err = GraphResponseError::from_response(status, response.headers, body);
    Err(GraphError::Response(err))
}

/// Exchange credentials are resolved only after exact configured-origin admission.
struct OutlookAuth {
    base: Base,
    source: Option<bifrost_net::CoordinatedTokenSource>,
}

/// The one refusal for Outlook service configuration an administrator must
/// fix: a missing Outlook-audience token source or an invalid Outlook origin.
/// An enterprise Outlook user cannot repair either by changing caller input or
/// reauthorizing the Graph token, so both are `SyncState(OperatorOverrideNeeded)`.
fn outlook_configuration_refusal(reason: String) -> bifrost_net::Error {
    let error = bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::OperatorOverrideNeeded,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::OperatorOverrideNeeded {
            reason: bifrost_types::OperatorOverrideReason::OutlookConfiguration {
                detail: bifrost_types::DiagnosticText::support_only(reason),
            },
        }),
    )
    .protocol(bifrost_types::Protocol::Ews)
    .provider(bifrost_types::Provider::Microsoft)
    .try_build()
    .expect("valid configuration refusal");
    bifrost_net::Error::HopPolicy(Box::new(error))
}

impl bifrost_net::HopPolicy for OutlookAuth {
    fn admit(
        &self,
        _prior: Option<&reqwest::Url>,
        next: &reqwest::Url,
    ) -> Result<bifrost_net::HopCredential, bifrost_net::Error> {
        self.base
            .admit(next.as_str())
            .map_err(|reason| match reason {
                Refusal::InvalidBase(detail) => outlook_configuration_refusal(format!(
                    "Configure a valid Outlook origin: {detail}"
                )),
                reason @ Refusal::Target(_) => bifrost_net::Error::InvalidRequest {
                    field: "outlook_origin",
                    detail: reason.to_string(),
                },
            })?;
        let source = self.source.clone().ok_or_else(|| {
            outlook_configuration_refusal(
                "Configure an Outlook-audience token source for Exchange features".into(),
            )
        })?;
        Ok(bifrost_net::HopCredential::Bearer(source))
    }
}

#[cfg(test)]
mod tests;
