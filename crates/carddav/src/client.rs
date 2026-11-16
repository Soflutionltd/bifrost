use crate::dialect::CardDavDialect;
#[cfg(test)]
use bifrost_dav_core::DavRequest;
use bifrost_dav_core::collection::{DavCollectionClient, Dialect, MultigetFetch, QUERY_HREF_PROPS};
use std::fmt;

use bifrost_dav_core::{
    CARDDAV_NAMESPACE, DavDispatch, DavProtocol, escape_xml, filter_unsupported,
};
pub(crate) use bifrost_dav_core::{FilteredHrefs, HrefQuery, PutCondition};
use bifrost_net::{AccountId, AccountNet};
use bifrost_types::{AccountError, AccountOperation, ErrorScope};
use reqwest::Method;
#[cfg(test)]
use reqwest::header::ETAG;

use crate::CardDavConfig;
use crate::parse::{
    AddressBookCollection, CardDavContactListing, CardDavFetchedVCard, CardDavMultigetReport,
    extract_href_property_in_namespace, parse_addressbook_collections, parse_propfind_contacts,
    resolve_href,
};

#[derive(Debug)]
pub(crate) struct AddressBookDiscovery {
    pub(crate) home: String,
    pub(crate) gateways: Vec<String>,
}

/// This crate's dialect binding for the shared DAV layer.
///
/// Every shared constructor takes it, so a CardDAV error can never be stamped
/// with a CalDAV `Protocol` or name a `ResourceKind::Calendar`.
const DAV: DavProtocol = CardDavDialect::DAV;

bifrost_dav_core::dav_error_constructors!(CardDavDialect);

#[derive(Clone)]
pub(crate) struct CardDavClient {
    /// Transport, credentials, origin gate and the generic WebDAV verbs, all
    /// shared with `bifrost-caldav` through `bifrost-dav-core`.
    dav: DavDispatch,
    directory_urls: std::sync::Arc<DirectoryGuard>,
}

/// Monotone directory exclusions shared by the Account doors and dav-core's
/// per-hop guard. Observing a marker may tighten access, never delegate trust
/// or add a searchable gateway; those decisions remain open-time snapshots.
#[derive(Default)]
struct DirectoryGuard {
    urls: std::sync::RwLock<Vec<String>>,
    search_gateways: std::sync::OnceLock<Vec<String>>,
}

impl DirectoryGuard {
    fn observe(&self, urls: impl IntoIterator<Item = String>) {
        let mut known = self.urls.write().expect("directory lock poisoned");
        for url in urls {
            if !known
                .iter()
                .any(|old| bifrost_dav_core::same_dav_url(old, &url))
            {
                known.push(url);
            }
        }
    }

    fn contains_url(&self, url: &str) -> bool {
        let Ok(mut url) = reqwest::Url::parse(url) else {
            return false;
        };
        // Queries do not change collection membership. Compare whole ancestor
        // URLs so percent-encoded unreserved characters use the same identity
        // rule as DAV discovery, without treating an encoded slash as a boundary.
        url.set_query(None);
        self.urls
            .read()
            .expect("directory lock poisoned")
            .iter()
            .any(|gateway| {
                let Ok(mut gateway) = reqwest::Url::parse(gateway) else {
                    return false;
                };
                gateway.set_query(None);
                let mut ancestor = url.clone();
                loop {
                    if bifrost_dav_core::same_dav_url(ancestor.as_str(), gateway.as_str()) {
                        return true;
                    }
                    let path = ancestor.path().trim_end_matches('/');
                    let Some((parent, _)) = path.rsplit_once('/') else {
                        return false;
                    };
                    let parent = format!("{parent}/");
                    if ancestor.path() == parent {
                        return false;
                    }
                    ancestor.set_path(&parent);
                }
            })
    }
}

impl bifrost_dav_core::DavRequestGuard for DirectoryGuard {
    fn check_target(
        &self,
        method: &Method,
        url: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        if !self.contains_url(url) {
            return Ok(());
        }
        // The only exemption. New methods and operations are refused by
        // default, including pre-fetch GETs and every sync request.
        if operation == AccountOperation::DirectorySearch
            && *method == *bifrost_dav_core::REPORT
            && self.search_gateways.get().is_some_and(|gateways| {
                gateways
                    .iter()
                    .any(|gateway| bifrost_dav_core::same_dav_url(gateway, url))
            })
        {
            return Ok(());
        }
        Err(directory_refusal(operation, url))
    }
}

impl fmt::Debug for CardDavClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardDavClient")
            .field("dav", &self.dav)
            .finish_non_exhaustive()
    }
}

impl CardDavClient {
    #[cfg(test)]
    pub(crate) async fn move_resource(
        &self,
        from: &str,
        to: &str,
        operation: AccountOperation,
    ) -> Result<bool, AccountError> {
        self.dav.move_resource(from, to, operation).await
    }
    pub(crate) fn collection_dispatch(&self) -> &DavDispatch {
        &self.dav
    }

    /// A client view whose buffered requests feed only this stream's tally.
    pub(crate) fn metered(&self) -> (Self, bifrost_dav_core::DavByteTally) {
        let (dav, tally) = self.dav.metered();
        let mut client = self.clone();
        client.dav = dav;
        (client, tally)
    }

    pub(crate) fn new_on_net(
        account_id: AccountId,
        config: &CardDavConfig,
        net: Option<&bifrost_net::Net>,
    ) -> Self {
        let net = net
            .cloned()
            .unwrap_or_else(bifrost_net::Net::shared_default);
        Self::with_dispatch(DavDispatch::on_net(
            &net,
            account_id,
            &config.base_url,
            config.credentials.to_shared(),
            DAV,
        ))
    }

    fn with_dispatch(mut dav: DavDispatch) -> Self {
        let directory_urls = std::sync::Arc::new(DirectoryGuard::default());
        dav.set_request_guard(std::sync::Arc::clone(&directory_urls)
            as std::sync::Arc<dyn bifrost_dav_core::DavRequestGuard>);
        Self {
            dav,
            directory_urls,
        }
    }

    /// The account handle carrying this account's meter, priority and cap.
    pub(crate) fn net(&self) -> &AccountNet {
        self.dav.net()
    }

    #[cfg(test)]
    pub(crate) fn with_account_net(base_url: &str, net: AccountNet) -> Self {
        Self::with_dispatch(DavDispatch::with_account_net(
            net,
            base_url,
            crate::CardDavCredentials::bearer("token").to_shared(),
            DAV,
        ))
    }

    /// The principal walk (well-known probe, fallback predicate, configured
    /// base) is `bifrost-dav-core`'s, shared with `bifrost-caldav`; only the
    /// addressbook-home lookup after it is CardDAV's own. This crate's copy of
    /// the walk is the one that once diverged, parsing the probe body outside
    /// the lookup so a probe answering `200 text/html` never reached the
    /// fallback predicate.
    pub(crate) async fn discover_addressbook_home(
        &self,
    ) -> Result<AddressBookDiscovery, AccountError> {
        let principal = self.dav.discover_current_user_principal().await?;
        self.addressbook_home_for_principal(principal).await
    }

    async fn addressbook_home_for_principal(
        &self,
        principal: String,
    ) -> Result<AddressBookDiscovery, AccountError> {
        let response = self
            .dav
            .propfind_raw(
                &principal,
                bifrost_dav_core::Depth::Zero,
                PROPFIND_ADDRESSBOOK_HOME,
                AccountOperation::Discover,
            )
            .await?;
        let home = extract_href_property_in_namespace(
            &response.text,
            CARDDAV_NAMESPACE,
            "addressbook-home-set",
        )
        .map_err(|error| parse_error(AccountOperation::Discover, error))?
        .map(|href| resolve_href(&response.url, &href))
        .ok_or_else(|| {
            bifrost_dav_core::missing_field_error(
                AccountOperation::Discover,
                "missing addressbook-home-set",
                DAV,
            )
        })?;
        // CARDDAV:directory-gateway is draft-daboo-carddav-directory-gateway-02,
        // not RFC 6352. There is no context-root directory fallback.
        let mut gateways = bifrost_dav_core::extract_href_properties_in_namespace(
            &response.text,
            CARDDAV_NAMESPACE,
            "directory-gateway",
        )
        .map_err(|error| parse_error(AccountOperation::Discover, error))?
        .into_iter()
        .map(|href| resolve_href(&response.url, &href))
        .collect::<Vec<_>>();
        dedup_directory_urls(&mut gateways);
        Ok(AddressBookDiscovery { home, gateways })
    }

    pub(crate) async fn list_addressbooks(
        &self,
        home_url: &str,
    ) -> Result<Vec<AddressBookCollection>, AccountError> {
        self.list_addressbooks_for_operation(home_url, AccountOperation::AddressBooksList)
            .await
    }

    pub(crate) async fn list_addressbooks_for_operation(
        &self,
        home_url: &str,
        operation: AccountOperation,
    ) -> Result<Vec<AddressBookCollection>, AccountError> {
        let response = self
            .dav
            .propfind_raw(
                home_url,
                bifrost_dav_core::Depth::One,
                PROPFIND_ADDRESSBOOKS,
                operation,
            )
            .await?;
        let mut collections = parse_addressbook_collections(&response.text)
            .map_err(|error| parse_error(operation, format!("addressbook list: {error}")))?;
        for collection in &mut collections {
            collection.resolve_href(&response.url);
        }
        self.directory_urls.observe(
            collections
                .iter()
                .filter(|c| c.is_directory)
                .map(|c| c.href.clone()),
        );
        Ok(collections)
    }

    /// Depth-0 PROPFIND for the collection `getctag` and `resourcetype`.
    /// Returns `None` when the server omits `getctag`, so the caller falls
    /// through to a full snapshot + diff. A directory marker tightens the
    /// guard before a subsequent contact listing can be sent.
    pub(crate) async fn collection_ctag(
        &self,
        addressbook_url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let response = self
            .dav
            .propfind_raw(
                addressbook_url,
                bifrost_dav_core::Depth::Zero,
                PROPFIND_CTAG,
                operation,
            )
            .await?;
        let mut collections = parse_addressbook_collections(&response.text)
            .map_err(|error| parse_error(operation, format!("collection metadata: {error}")))?;
        let mut directories = Vec::new();
        for collection in &mut collections {
            collection.resolve_href(&response.url);
            if collection.is_directory {
                directories.push(collection.href.clone());
            }
        }
        // The depth-0 read also asks for resourcetype, so it can tighten the
        // directory guard for this selected collection without enumerating
        // every address book in its home.
        self.directory_urls.observe(directories);
        crate::parse::parse_collection_ctag(&response.text)
            .map_err(|error| parse_error(operation, format!("collection ctag: {error}")))
    }

    /// Depth-1 contact PROPFIND returning both the committed entries and
    /// the hrefs the server reported *failed* within the 207, so the
    /// snapshot diff can preserve transiently-failed resources rather
    /// than destroying them (brick 7).
    pub(crate) async fn list_contacts_listing(
        &self,
        addressbook_url: &str,
        operation: AccountOperation,
    ) -> Result<CardDavContactListing, AccountError> {
        let response = self
            .dav
            .propfind_raw(
                addressbook_url,
                bifrost_dav_core::Depth::One,
                PROPFIND_CONTACTS,
                operation,
            )
            .await?;
        let mut listing = parse_propfind_contacts(&response.text)
            .map_err(|error| parse_error(operation, format!("contact list: {error}")))?;
        listing.resolve_hrefs_with(&response.url, CardDavDialect::resolve_href);
        if let Some(error) = listing.failure::<CardDavDialect>(operation) {
            return Err(error);
        }
        Ok(listing)
    }

    pub(crate) async fn fetch_vcards(
        &self,
        addressbook_url: &str,
        uris: &[String],
        operation: AccountOperation,
    ) -> Result<MultigetFetch, AccountError> {
        DavCollectionClient::<CardDavDialect>::new(&self.dav)
            .fetch(addressbook_url, uris, operation)
            .await
    }

    /// The text-search lane's server-side prefilter, one REPORT per property,
    /// asking for `getetag` only.
    ///
    /// The union of the eight legs is a PREFILTER, not the answer: the account
    /// layer still runs `contact_matches` over the hydrated page, because the
    /// local match reads PROJECTED fields (and folds case with Rust's full
    /// Unicode rules) rather than the raw vCard properties an
    /// `i;unicode-casemap` text-match sees. Widening the server side and
    /// narrowing locally is the safe direction; the reverse would silently drop
    /// matches. Twin of `bifrost-caldav::query_event_hrefs_text`.
    ///
    /// A leg that reports the filter unsupported degrades the WHOLE lane rather
    /// than the one property: answering out of the properties a server happened
    /// to accept would narrow the search with no signal to the consumer.
    pub(crate) async fn query_vcard_hrefs_text(
        &self,
        addressbook_url: &str,
        query: &str,
    ) -> Result<FilteredHrefs, AccountError> {
        let bodies = ["FN", "N", "EMAIL", "TEL", "ADR", "ORG", "TITLE", "NOTE"]
            .map(|property| addressbook_text_query_body(property, query));
        DavCollectionClient::<CardDavDialect>::new(&self.dav)
            .query_text(addressbook_url, &bodies, AccountOperation::ContactSearch)
            .await
    }

    /// Directory queries never enumerate or locally narrow the server's answer.
    pub(crate) async fn query_directory(
        &self,
        url: &str,
        query: &str,
        limit: u32,
    ) -> Result<(CardDavMultigetReport, bool), AccountError> {
        let operation = AccountOperation::DirectorySearch;
        let body = directory_query_body(query, limit);
        let response = self
            .dav
            .report_raw_response(url, bifrost_dav_core::Depth::One, &body, operation)
            .await?;
        if !response.status.is_success() {
            return Err(if filter_unsupported(response.status, &response.body) {
                unsupported_error(operation)
                    .into_builder()
                    .push_cause(bifrost_types::Cause::Attempt(
                        bifrost_types::AttemptCause::new(
                            bifrost_types::TransmissionState::Acknowledged,
                        ),
                    ))
                    .try_build()
                    .expect("valid answered filter refusal")
            } else {
                status_error(&response.headers, operation, response.status, response.body)
            });
        }
        let (report, truncated) =
            crate::parse::parse_directory_report(&response.body, &response.url)
                .map_err(|error| parse_error(operation, error))?;
        if let Some(error) = report.failure::<CardDavDialect>(operation) {
            return Err(error);
        }
        Ok((report, truncated))
    }

    /// Fetch one vCard resource with a plain `GET`, reading the validator from
    /// the `ETag` response header.
    ///
    /// Single-resource reads use the shared collection client. Their
    /// reads used to go through `addressbook-multiget` against the collection
    /// DERIVED from the resource URL, which is strictly more fragile: it depends
    /// on the derived parent actually being the collection the server believes
    /// holds the resource (nested collections and split principal namespaces
    /// break that), and some servers reject absolute-URI hrefs in a multiget
    /// body. The multiget's only advantage was carrying the etag back in a
    /// prop, and a GET reports the same validator in its `ETag` header.
    pub(crate) async fn get_vcard(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<CardDavFetchedVCard, AccountError> {
        DavCollectionClient::<CardDavDialect>::new(&self.dav)
            .get(url, operation)
            .await
    }

    pub(crate) async fn put_vcard(
        &self,
        url: &str,
        body: String,
        condition: PutCondition,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        self.refuse_directory_access(url, operation)?;
        DavCollectionClient::<CardDavDialect>::new(&self.dav)
            .put(url, body, condition, operation)
            .await
    }

    /// Conditional delete never silently drops a caller's validator.
    pub(crate) async fn delete_vcard_if_match(
        &self,
        url: &str,
        etag: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        self.refuse_directory_access(url, operation)?;
        DavCollectionClient::<CardDavDialect>::new(&self.dav)
            .delete_if_match(url, etag, operation)
            .await
    }

    /// `DELETE` one vCard resource. Replayable, and a target already gone is a
    /// success - the dav-D1 ruling, stated in full at
    /// `DavDispatch::delete_resource`; twin of `bifrost-caldav`'s
    /// `delete_event`, and the two must move together. Not a call into
    /// `delete_resource` itself only because this side hands the response ETag
    /// back, which a gone target has none of.
    pub(crate) async fn delete_vcard(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        self.refuse_directory_access(url, operation)?;
        DavCollectionClient::<CardDavDialect>::new(&self.dav)
            .delete(url, operation)
            .await
    }

    pub(crate) fn observe_directories(&self, urls: impl IntoIterator<Item = String>) {
        self.directory_urls.observe(urls);
    }

    pub(crate) fn initialize_directories(
        &mut self,
        urls: impl IntoIterator<Item = String>,
        search_gateways: Vec<String>,
    ) {
        self.directory_urls.observe(urls);
        self.directory_urls
            .search_gateways
            .set(search_gateways)
            .expect("search gateways are frozen once at open");
    }

    pub(crate) fn is_directory_url(&self, url: &str) -> bool {
        self.directory_urls.contains_url(&self.resolve_url(url))
    }

    pub(crate) fn refuse_directory_access(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        if self.is_directory_url(url) {
            return Err(directory_refusal(operation, &self.resolve_url(url)));
        }
        Ok(())
    }

    pub(crate) fn resolve_url(&self, href: &str) -> String {
        self.dav.resolve_url(href)
    }

    pub(crate) fn is_trusted_url(&self, url: &str) -> bool {
        self.dav.is_trusted_url(url)
    }

    pub(crate) fn admit_discovered_urls(&mut self, urls: impl IntoIterator<Item = String>) {
        self.dav.admit_discovered_urls(urls);
    }
}

/// Identify a directory refusal structurally so sync boundaries can retire
/// their original scope without confusing an unrelated Unsupported response.
fn directory_refusal(operation: AccountOperation, url: &str) -> AccountError {
    unsupported_error(operation)
        .into_builder()
        .scope(ErrorScope::DirectoryGateway {
            url: url.to_string(),
        })
        .try_build()
        .expect("directory scope preserves Unsupported classification")
}

fn addressbook_text_query_body(property: &str, query: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:addressbook-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
{QUERY_HREF_PROPS}\
  <C:filter>\n\
    <C:prop-filter name=\"{}\">\n\
      <C:text-match collation=\"i;unicode-casemap\">{}</C:text-match>\n\
    </C:prop-filter>\n\
  </C:filter>\n\
</C:addressbook-query>",
        property,
        escape_xml(query)
    )
}

/// Fold one listing's committed and refused hrefs into a candidate set.
///
/// The listing lane merges one, the text lane eight, and they differ in nothing
/// else. Twin of the CalDAV function of the same name.
pub(crate) fn extend_candidates(
    query: &mut HrefQuery,
    listing: crate::parse::CardDavContactListing,
) {
    listing.extend_candidates(query);
}

pub(crate) fn contact_scope(id: impl Into<String>) -> ErrorScope {
    CardDavDialect::item_scope(id.into())
}

/// Scope the server's own not-found answer to the contact it names.
///
/// DECORATES the classified 404 rather than minting a fresh one. The fresh
/// `status_error(.., 404, String::new())` this used to build threw away the
/// response body the server sent with the 404 - the one diagnostic an operator
/// has for why a contact vanished - while keeping everything else. Decorating
/// keeps the kind, the status, the acknowledged attempt and the body text, and
/// adds only the scope.
pub(crate) fn not_found_error(error: AccountError, id: impl Into<String>) -> AccountError {
    error
        .into_builder()
        .scope(contact_scope(id))
        .try_build()
        .expect("adding a scope cannot invalidate a classified error")
}

/// Sort once for stable merge precedence, then dedup by DAV URL identity even
/// when equivalent encoded spellings were not adjacent in lexical order.
pub(crate) fn dedup_directory_urls(urls: &mut Vec<String>) {
    urls.sort();
    let mut unique = Vec::new();
    for url in urls.drain(..) {
        if !unique
            .iter()
            .any(|old: &String| bifrost_dav_core::same_dav_url(old, &url))
        {
            unique.push(url);
        }
    }
    *urls = unique;
}

/// RFC 6352 addressbook-query, text-match and result-limit grammar. Gateway
/// discovery itself is draft-daboo-carddav-directory-gateway-02, not that RFC.
fn directory_query_body(query: &str, limit: u32) -> String {
    let query = escape_xml(query);
    let filters = ["FN", "EMAIL", "N", "NICKNAME"].map(|name| format!(
        "<C:prop-filter name=\"{name}\"><C:text-match collation=\"i;unicode-casemap\" match-type=\"contains\">{query}</C:text-match></C:prop-filter>"
    )).join("");
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><C:addressbook-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:prop><D:resourcetype/><C:address-data/></D:prop><C:filter test=\"anyof\">{filters}</C:filter><C:limit><C:nresults>{limit}</C:nresults></C:limit></C:addressbook-query>"
    )
}

const PROPFIND_ADDRESSBOOK_HOME: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
  <D:prop>\n\
    <C:addressbook-home-set/>\n\
    <C:directory-gateway/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_ADDRESSBOOKS: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\" xmlns:CS=\"http://calendarserver.org/ns/\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:displayname/>\n\
    <D:current-user-privilege-set/>\n\
    <CS:getctag/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_CTAG: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\" xmlns:CS=\"http://calendarserver.org/ns/\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <CS:getctag/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_CONTACTS: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
    <D:getcontenttype/>\n\
  </D:prop>\n\
</D:propfind>";

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    use bifrost_types::{
        AccountErrorKind, Protocol, ProtocolErrorKind, RecoveryClass, RequestErrorKind,
        ResourceKind,
    };

    /// Every error this crate mints is stamped CardDAV, and names contacts.
    ///
    /// The shared ladder in `bifrost-dav-core` is parameterized by `DAV`, so a
    /// wrong binding here would silently restamp the whole crate's error
    /// surface as CalDAV - a consumer routing on `Protocol` or `ResourceKind`
    /// would misfile every failure. Ablating `DAV` to `CalDav` before this
    /// existed failed exactly one unrelated test, which is not the coverage
    /// that mistake deserves.
    #[test]
    fn every_error_this_crate_mints_is_stamped_carddav() {
        let errors = [
            status_error(
                &reqwest::header::HeaderMap::new(),
                AccountOperation::ContactGet,
                StatusCode::NOT_FOUND,
                String::new(),
            ),
            local_error(AccountOperation::ContactGet, "bad"),
            parse_error(AccountOperation::ContactGet, "bad"),
            transport_error(AccountOperation::ContactGet, "bad"),
            unsupported_error(AccountOperation::ContactGet),
        ];
        for error in errors {
            assert_eq!(
                error.protocol(),
                Some(Protocol::CardDav),
                "a CardDAV error must not be stamped otherwise: {error:?}"
            );
        }
        let missing = status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::ContactGet,
            StatusCode::NOT_FOUND,
            String::new(),
        );
        assert!(
            matches!(
                missing.kind(),
                AccountErrorKind::NotFound(ResourceKind::Contact)
            ),
            "a CardDAV 404 names a contact, not a calendar: {missing:?}"
        );
    }
    use bifrost_dav_core::DavResponse;
    use bifrost_dav_core::test_support::{
        dav_dropped_after_send, dav_redirect, dav_retried, dav_script, dav_script_empty,
        scripted_dav_net, transcripts,
    };
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_types::ReconcileAction;
    use std::sync::Arc;

    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

    #[tokio::test]
    async fn unenforceable_contact_versions_are_refused_before_io() {
        use bifrost_types::account::Account as _;

        for (version, moving) in [
            (
                bifrost_types::ServerVersion::etag(
                    bifrost_types::Protocol::CardDav,
                    bifrost_types::ObjectType::Contact,
                    "W/\"weak\"",
                ),
                false,
            ),
            (bifrost_types::ServerVersion::ModSeq(1), false),
            (
                bifrost_types::ServerVersion::etag(
                    bifrost_types::Protocol::CardDav,
                    bifrost_types::ObjectType::Contact,
                    "strong",
                ),
                true,
            ),
        ] {
            let script = dav_script_empty();
            let client = Arc::new(CardDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            ));
            let account =
                crate::account::CardDavAccount::for_tests(client, "https://dav.example.test/book/");
            let error = account
                .contact_update(
                    bifrost_types::ContactId("https://dav.example.test/book/one".into()),
                    bifrost_types::ContactPatch {
                        expected_version: Some(version),
                        address_book_id: moving.then(|| {
                            bifrost_types::AddressBookId("https://dav.example.test/other/".into())
                        }),
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(
                *error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::ContactUpdate)
            );
            assert!(transcripts(&script).is_empty());
        }
    }

    #[tokio::test]
    async fn caller_contact_version_controls_the_put() {
        use bifrost_types::account::Account as _;

        for expected in [Some("stale"), None] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "etag",
                reqwest::header::HeaderValue::from_static("\"fresh\""),
            );
            let script = dav_script([
                DavResponse {
                    status: StatusCode::OK,
                    headers,
                    body: "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:One\r\nEND:VCARD\r\n".to_string(),
                    url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                        .expect("test DAV URL"),
                },
                DavResponse {
                    status: if expected.is_some() {
                        StatusCode::PRECONDITION_FAILED
                    } else {
                        StatusCode::NO_CONTENT
                    },
                    headers: HeaderMap::new(),
                    body: String::new(),
                    url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                        .expect("test DAV URL"),
                },
            ]);
            let client = Arc::new(CardDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            ));
            let account =
                crate::account::CardDavAccount::for_tests(client, "https://dav.example.test/book/");
            let result = account
                .contact_update(
                    bifrost_types::ContactId("https://dav.example.test/book/one.vcf".into()),
                    bifrost_types::ContactPatch {
                        expected_version: expected.map(|etag| {
                            bifrost_types::ServerVersion::etag(
                                bifrost_types::Protocol::CardDav,
                                bifrost_types::ObjectType::Contact,
                                etag,
                            )
                        }),
                        ..Default::default()
                    },
                )
                .await;
            if expected.is_some() {
                assert_eq!(
                    *result.unwrap_err().kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                );
            } else {
                result.expect("an absent version keeps ordinary writes working");
            }
            let requests = transcripts(&script);
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[1].method.as_str(), "PUT");
            assert_eq!(
                requests[1]
                    .headers
                    .get("If-Match")
                    .and_then(|value| value.to_str().ok()),
                Some(if expected.is_some() {
                    "\"stale\""
                } else {
                    "\"fresh\""
                })
            );
        }
    }

    #[tokio::test]
    async fn caller_contact_delete_refuses_weak_and_foreign_versions_before_io() {
        use bifrost_types::Account;

        for version in [
            bifrost_types::ServerVersion::etag(
                bifrost_types::Protocol::CardDav,
                bifrost_types::ObjectType::Contact,
                "W/\"weak\"",
            ),
            bifrost_types::ServerVersion::ModSeq(42),
            bifrost_types::ServerVersion::Unavailable,
        ] {
            let script = dav_script_empty();
            let client = Arc::new(CardDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            ));
            let account =
                crate::account::CardDavAccount::for_tests(client, "https://dav.example.test/book/");
            let error = account
                .contact_delete_if_version(
                    bifrost_types::ContactId("https://dav.example.test/book/one.vcf".into()),
                    Some(version),
                )
                .await
                .unwrap_err();
            assert_eq!(
                *error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::ContactDelete)
            );
            assert!(transcripts(&script).is_empty());
        }
    }

    #[tokio::test]
    async fn caller_contact_delete_version_is_conditional_and_absence_stays_unconditional() {
        use bifrost_types::Account;

        for expected in [Some("stale"), None] {
            let script = dav_script([DavResponse {
                status: if expected.is_some() {
                    StatusCode::PRECONDITION_FAILED
                } else {
                    StatusCode::NO_CONTENT
                },
                headers: HeaderMap::new(),
                body: String::new(),
                url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                    .expect("test DAV URL"),
            }]);
            let client = Arc::new(CardDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            ));
            let account =
                crate::account::CardDavAccount::for_tests(client, "https://dav.example.test/book/");
            let result = account
                .contact_delete_if_version(
                    bifrost_types::ContactId("https://dav.example.test/book/one.vcf".into()),
                    expected.map(|value| {
                        bifrost_types::ServerVersion::etag(
                            bifrost_types::Protocol::CardDav,
                            bifrost_types::ObjectType::Contact,
                            value,
                        )
                    }),
                )
                .await;
            if expected.is_some() {
                assert_eq!(
                    *result.unwrap_err().kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                );
            } else {
                result.expect("absent version preserves ordinary deletion");
            }
            let requests = transcripts(&script);
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].method.as_str(), "DELETE");
            assert_eq!(
                requests[0]
                    .headers
                    .get("If-Match")
                    .and_then(|value| value.to_str().ok()),
                expected.map(|_| "\"stale\""),
            );
        }
    }

    /// A create-PUT must not be replayed after a mid-flight drop.
    ///
    /// HTTP calls PUT idempotent. That is true of an absolute-state write to a
    /// known URL and false of a create, which is why the create-PUT is declared
    /// `ReplayPolicy::Never` (net's PUT default also refuses replay once
    /// `If-None-Match` is present). A replayed create whose first attempt
    /// committed answers 412 -> `ConcurrencyConflict` ->
    /// `Retry(AfterStateRefresh)`, and the consumer re-creates the contact
    /// under a fresh UUID, leaving two copies.
    ///
    /// One drop is scripted, so a request that IS replayed exhausts the script
    /// and panics rather than passing quietly.
    #[tokio::test]
    async fn a_create_put_is_not_replayed_after_a_mid_flight_drop() {
        let script = dav_script([dav_dropped_after_send()]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let error = client
            .put_vcard(
                "https://dav.example.test/book/new.vcf",
                "BEGIN:VCARD\r\nEND:VCARD\r\n".to_string(),
                PutCondition::IfNoneMatch,
                AccountOperation::ContactCreate,
            )
            .await
            .expect_err("a dropped create must fail");

        assert_eq!(
            transcripts(&script).len(),
            1,
            "the dropped create must not be replayed"
        );
        assert!(
            matches!(
                error.recovery(),
                RecoveryClass::Reconcile(advice)
                    if advice.guidance.actions.contains(&ReconcileAction::CheckTarget)
            ),
            "a drop after send must reconcile against the target: {:?}",
            error.recovery()
        );
    }

    /// A DELETE is replayed after a mid-flight drop, and the 404 a replayed
    /// landed DELETE answers is a success, reported with no validator - the
    /// dav-D1 ruling, at `DavDispatch::delete_resource`. Twin of
    /// `bifrost-caldav`'s test of the same name; keep them in step.
    #[tokio::test]
    async fn a_dropped_delete_replays_and_a_gone_target_is_a_success() {
        let answer = |status: StatusCode, etag: Option<&str>| {
            let mut headers = HeaderMap::new();
            if let Some(etag) = etag {
                headers.insert(ETAG, HeaderValue::from_str(etag).expect("etag"));
            }
            DavResponse {
                status,
                headers,
                body: String::new(),
                url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                    .expect("test DAV URL"),
            }
        };
        for (script, requests, expected, label) in [
            (
                dav_script([
                    dav_dropped_after_send(),
                    answer(StatusCode::NO_CONTENT, Some("\"v9\"")).into(),
                ]),
                2,
                Some("v9".to_string()),
                "a drop before the server saw the DELETE replays and lands",
            ),
            (
                dav_script([
                    dav_dropped_after_send(),
                    answer(StatusCode::NOT_FOUND, None).into(),
                ]),
                2,
                None,
                "a drop after the DELETE landed replays to a 404 that reads as done",
            ),
        ] {
            let client = CardDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            );
            let etag = client
                .delete_vcard(
                    "https://dav.example.test/book/one.vcf",
                    AccountOperation::ContactDelete,
                )
                .await
                .unwrap_or_else(|error| panic!("{label}: {error:?}"));
            assert_eq!(etag, expected, "{label}");
            assert_eq!(transcripts(&script).len(), requests, "{label}");
        }
    }

    /// A dropped create names the target the consumer is told to check.
    ///
    /// Twin of `bifrost-caldav`'s
    /// `a_dropped_create_names_the_target_to_reconcile_against`. The URL and the
    /// UUID are minted locally in `contact_create`, so nothing on the wire knows
    /// them; without the scope, the `Reconcile(CheckTarget)` the unreplayable
    /// declaration buys is unactionable and the consumer can only re-create -
    /// the duplicate the declaration exists to prevent.
    #[tokio::test]
    async fn a_dropped_create_names_the_target_to_reconcile_against() {
        use bifrost_types::account::Account as _;

        let script = dav_script([dav_dropped_after_send()]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/work/",
        );

        let error = account
            .contact_create(bifrost_types::ContactCreate {
                address_book_id: Some(bifrost_types::AddressBookId(
                    "https://dav.example.test/books/work/".to_string(),
                )),
                display_name: Some("One".to_string()),
                ..Default::default()
            })
            .await
            .expect_err("a dropped create must fail");

        let Some(bifrost_types::ErrorScope::Contact { id }) = error.scope() else {
            panic!("a dropped create must name its minted target: {error:?}");
        };
        assert!(
            id.0.starts_with("https://dav.example.test/books/work/") && id.0.ends_with(".vcf"),
            "the scope must carry the minted resource URL: {id:?}"
        );
    }

    /// The `MOVE` leg of a relocate must not be replayed either.
    ///
    /// Twin of `bifrost-caldav`'s
    /// `a_dropped_relocate_move_reconciles_rather_than_replaying`; keep them in
    /// step. A relocate runs under `ContactUpdate`, which the
    /// `AccountOperation` table calls idempotent, so a mid-flight drop on the
    /// MOVE would derive `Retry(SameRequest)`: the engine re-issues the update,
    /// which reads the source URL the committed MOVE already emptied, and a
    /// move that SUCCEEDED is reported as a permanent refusal. The
    /// `move_resource` request declares itself unreplayable so the drop
    /// reconciles against the destination instead.
    #[tokio::test]
    async fn a_dropped_relocate_move_reconciles_rather_than_replaying() {
        let script = dav_script([dav_dropped_after_send()]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let error = client
            .move_resource(
                "https://dav.example.test/book/a/one.vcf",
                "https://dav.example.test/book/b/one.vcf",
                AccountOperation::ContactUpdate,
            )
            .await
            .expect_err("a dropped MOVE must fail");

        assert_eq!(
            transcripts(&script).len(),
            1,
            "the dropped MOVE must not be replayed"
        );
        assert!(
            matches!(
                error.recovery(),
                RecoveryClass::Reconcile(advice)
                    if advice.guidance.actions.contains(&ReconcileAction::CheckTarget)
            ),
            "a dropped MOVE must reconcile against the destination: {:?}",
            error.recovery()
        );
    }

    /// Versioned mutations read back after ambiguity rather than replaying.
    #[tokio::test]
    async fn a_versioned_delete_reconciles_after_a_mid_flight_drop() {
        let script = dav_script([
            dav_dropped_after_send(),
            DavResponse {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: String::new(),
                url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                    .expect("test DAV URL"),
            }
            .into(),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let error = client
            .delete_vcard_if_match(
                "https://dav.example.test/book/one.vcf",
                "v1",
                AccountOperation::ContactDelete,
            )
            .await
            .unwrap_err();
        assert_eq!(transcripts(&script).len(), 1);
        assert!(
            matches!(error.recovery(), RecoveryClass::Reconcile(a) if a.guidance.actions.contains(&ReconcileAction::CheckTarget))
        );
    }

    #[tokio::test]
    async fn an_unconditional_vcard_put_replays_after_a_mid_flight_drop() {
        let script = dav_script([
            dav_dropped_after_send(),
            DavResponse {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: String::new(),
                url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                    .expect("test DAV URL"),
            }
            .into(),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let result = client
            .put_vcard(
                "https://dav.example.test/book/one.vcf",
                String::new(),
                PutCondition::None,
                AccountOperation::ContactUpdate,
            )
            .await;
        assert_eq!(transcripts(&script).len(), 2);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn an_if_match_update_put_reconciles_after_a_mid_flight_drop() {
        let mut headers = HeaderMap::new();
        headers.insert(ETAG, HeaderValue::from_static("\"v2\""));
        let script = dav_script([
            dav_dropped_after_send(),
            Canned::from(DavResponse {
                status: StatusCode::NO_CONTENT,
                headers,
                body: String::new(),
                url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                    .expect("test DAV URL"),
            }),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let error = client
            .put_vcard(
                "https://dav.example.test/book/one.vcf",
                "BEGIN:VCARD\r\nEND:VCARD\r\n".to_string(),
                PutCondition::from_etag(Some("v1")),
                AccountOperation::ContactUpdate,
            )
            .await
            .expect_err("conditional write must reconcile");

        assert_eq!(
            transcripts(&script).len(),
            1,
            "the conditional PUT must not replay"
        );
        assert!(
            matches!(error.recovery(), RecoveryClass::Reconcile(a) if a.guidance.actions == [ReconcileAction::CheckTarget])
        );
    }

    /// A poll-path snapshot spends the depth-0 ctag request and the depth-1
    /// contact listing, and never re-lists the address book home.
    ///
    /// `CtagSource::Known` is what makes that true: the poll already asked the
    /// collection for its ctag in order to decide whether to short-circuit, so
    /// re-deriving the same value from a depth-1 PROPFIND over the home cost a
    /// third round trip - one that grows with the number of address books
    /// rather than staying a single collection wide. CalDAV's `event_snapshot`
    /// has taken the cheap path for a while; this is the CardDAV twin catching
    /// up, and the assertion is written against the transcript so the two
    /// cannot drift back apart silently.
    #[tokio::test]
    async fn poll_snapshot_never_relists_the_address_book_home() {
        let multistatus = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        // Only two responses are scripted. A regression that re-lists the home
        // starves the script and fails loudly here rather than silently
        // spending a third request.
        let script = dav_script([multistatus(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/books/ada/personal/</D:href>
    <D:propstat>
      <D:prop><CS:getctag>ctag-7</CS:getctag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#,
        )]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let ctag = client
            .collection_ctag(
                "https://dav.example.test/books/ada/personal/",
                AccountOperation::SyncChanges,
            )
            .await
            .expect("ctag resolves");
        assert_eq!(ctag.as_deref(), Some("ctag-7"));

        let script = dav_script([multistatus(
            r#"<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/books/ada/personal/one.vcf</D:href>
    <D:propstat>
      <D:prop><D:getetag>"etag-1"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#,
        )]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let snapshot = crate::account::CardDavAccount::contact_snapshot(
            &client,
            crate::account::CtagSource::Known(ctag),
            "https://dav.example.test/books/ada/personal/",
            AccountOperation::SyncChanges,
        )
        .await
        .expect("snapshot");

        // The known ctag survived into the snapshot without being refetched.
        assert_eq!(snapshot.token.as_deref(), Some("ctag-7"));

        let requests = transcripts(&script);
        assert_eq!(
            requests.len(),
            1,
            "a Known ctag must cost the listing request and nothing else"
        );
        assert_eq!(
            requests[0].url, "https://dav.example.test/books/ada/personal/",
            "the one request addresses the collection, never the home"
        );
        assert_eq!(
            requests[0]
                .headers
                .get("depth")
                .and_then(|value| value.to_str().ok()),
            Some("1"),
            "the listing is the depth-1 request"
        );
    }

    #[tokio::test]
    async fn depth_zero_ctag_read_observes_a_directory_marker() {
        let addressbook = "https://dav.example.test/books/ada/personal/";
        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav" xmlns:CS="http://calendarserver.org/ns/">
              <D:response><D:href>/books/ada/personal/</D:href><D:propstat><D:prop>
                <D:resourcetype><D:collection/><C:directory/></D:resourcetype>
                <CS:getctag>directory-ctag</CS:getctag>
              </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
            </D:multistatus>"#
                .to_string(),
            url: bifrost_dav_core::DavHref::parse(addressbook).expect("test DAV URL"),
        }]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        assert_eq!(
            client
                .collection_ctag(addressbook, AccountOperation::SyncInventory)
                .await
                .expect("ctag response parses")
                .as_deref(),
            Some("directory-ctag")
        );
        assert!(client.is_directory_url("https://dav.example.test/books/ada/personal/one.vcf"));

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, addressbook);
        assert_eq!(
            requests[0]
                .headers
                .get("depth")
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
        assert!(requests[0].body.contains("<D:resourcetype/>"));
    }

    /// A header the request record could not carry fails the request LOCALLY.
    ///
    /// The empty script is the assertion: a regression that goes back to
    /// dropping the header silently would put a request on the wire and panic
    /// on the exhausted script instead of returning `Request(Malformed)`. The
    /// dropped-`If-Match` case is the one that matters - a conditional write
    /// demoted to an unconditional one loses the lost-update guard without
    /// telling anyone.
    #[tokio::test]
    async fn a_header_the_record_could_not_carry_fails_before_the_wire() {
        let script = dav_script_empty();
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let request = DavRequest::new(Method::PUT, "https://dav.example.test/book/one.vcf")
            .header("If-Match", "\"etag\u{7f}\"");
        let error = client
            .dav
            .send_raw_request(request, AccountOperation::ContactUpdate)
            .await
            .expect_err("a header-invalid value refuses locally");
        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            ),
            "expected a local malformed-request refusal: {error:?}"
        );
        assert!(transcripts(&script).is_empty(), "no request may go out");
    }

    /// A header value the transport cannot express as text is the same refusal,
    /// caught one layer lower: the record holds it, the copy into the
    /// `bifrost-net` builder (which takes `&str`) cannot.
    #[tokio::test]
    async fn a_non_ascii_header_value_fails_before_the_wire() {
        let script = dav_script_empty();
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let mut headers = HeaderMap::new();
        headers.insert(
            "If-Match",
            HeaderValue::from_bytes(b"\"etag-\xff\"")
                .expect("opaque bytes are a valid HeaderValue"),
        );
        let request =
            DavRequest::new(Method::PUT, "https://dav.example.test/book/one.vcf").headers(headers);
        assert!(
            request.invalid_header().is_none(),
            "the record accepted this value; the transport is the layer that cannot"
        );
        let error = client
            .dav
            .send_raw_request(request, AccountOperation::ContactUpdate)
            .await
            .expect_err("a non-ASCII header value refuses locally");
        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Request(RequestErrorKind::Malformed)
            ),
            "expected a local malformed-request refusal: {error:?}"
        );
        assert!(transcripts(&script).is_empty(), "no request may go out");
    }

    /// `contact_get` addresses the RESOURCE with a plain GET and reads the
    /// validator out of the `ETag` header.
    ///
    /// It used to REPORT an `addressbook-multiget` against the collection
    /// derived from the resource URL - a request that only works when that
    /// derivation matches the collection the server believes holds the card
    /// (nested collections and split principal namespaces break it), and whose
    /// body carries an absolute-URI href some servers reject. CalDAV's
    /// `get_event` had the simpler shape all along. The transcript is asserted
    /// so a revert to the REPORT is loud.
    #[tokio::test]
    async fn contact_get_addresses_the_resource_with_a_plain_get() {
        use bifrost_types::account::Account as _;

        let mut headers = HeaderMap::new();
        headers.insert(ETAG, HeaderValue::from_static("\"etag-9\""));
        let script = dav_script([DavResponse {
            status: StatusCode::OK,
            headers,
            body: "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada Lovelace\r\nEND:VCARD\r\n".to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        }]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/ada/personal/",
        );

        let card = account
            .contact_get(bifrost_types::ContactId(
                "https://dav.example.test/books/ada/personal/one.vcf".into(),
            ))
            .await
            .expect("contact_get");
        // Normalized exactly as the multiget `getetag` was, so the snapshot
        // diff keeps comparing like with like.
        assert_eq!(
            card.version.as_ref().map(|version| version
                .expected_etag(
                    bifrost_types::Protocol::CardDav,
                    AccountOperation::ContactUpdate,
                    true
                )
                .unwrap()),
            Some("etag-9")
        );

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 1, "one request, not a REPORT plus anything");
        assert_eq!(requests[0].method, Method::GET);
        assert_eq!(
            requests[0].url, "https://dav.example.test/books/ada/personal/one.vcf",
            "the request addresses the card, never its derived parent collection"
        );
    }

    /// A missing contact keeps what the server said about it.
    ///
    /// `fetch_contact_resource` scopes the 404 to the contact. It used to do so
    /// by REPLACING the classified error with a fresh 404 minted from an empty
    /// body, so the server's response text - the only diagnostic an operator
    /// has - was dropped. Against that code the text assertion fails. The kind,
    /// scope and acknowledged attempt are pinned beside it so the decoration
    /// cannot lose what the replacement kept.
    #[tokio::test]
    async fn a_missing_contact_keeps_the_servers_response_text() {
        use bifrost_types::account::Account as _;

        const EXPLANATION: &str = "contact was moved to the archive book";
        let url = "https://dav.example.test/books/ada/personal/one.vcf";
        let script = dav_script([DavResponse {
            status: StatusCode::NOT_FOUND,
            headers: HeaderMap::new(),
            body: EXPLANATION.to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        }]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/ada/personal/",
        );

        let error = account
            .contact_get(bifrost_types::ContactId(url.into()))
            .await
            .expect_err("a 404 is a missing contact");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::NotFound(ResourceKind::Contact)
        );
        assert!(
            matches!(
                error.scope(),
                Some(ErrorScope::Contact { id }) if id.0 == url
            ),
            "the 404 is scoped to the contact: {:?}",
            error.scope()
        );
        assert!(
            error
                .support_consented()
                .support_text
                .iter()
                .any(|text| text.contains(EXPLANATION)),
            "the server's response text survives: {error:?}"
        );
        assert!(
            error.chain().iter().any(|cause| matches!(
                cause,
                bifrost_types::Cause::Attempt(attempt)
                    if attempt.transmission_state
                        == bifrost_types::TransmissionState::Acknowledged
            )),
            "a 404 is a server answer: {error:?}"
        );
        assert_eq!(transcripts(&script).len(), 1);
    }

    /// A card the SERVER sent that will not parse is the provider's malformed
    /// response, on both single-resource doors that project it. Twin of
    /// `bifrost-caldav`'s `an_unparseable_event_from_the_server_is_the_providers_fault`.
    ///
    /// Against the code before the fix both calls fail the kind assertion:
    /// `project_error` went through `local_error`, which is `Request(Malformed)`
    /// -> `ClientBug`. One response is scripted per call, so an update that went
    /// on to PUT would starve the script and panic.
    #[tokio::test]
    async fn an_unparseable_card_from_the_server_is_the_providers_fault() {
        use bifrost_types::account::Account as _;

        const MALFORMED: &str =
            "BEGIN:VCARD\r\nFN:Ada\r\nEMAIL;TYPE=\"unterminated:ada@example.test\r\nEND:VCARD\r\n";
        let url = "https://dav.example.test/books/ada/personal/one.vcf";
        let malformed = || DavResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: MALFORMED.to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        let script = dav_script([malformed(), malformed()]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/ada/personal/",
        );

        let got = account
            .contact_get(bifrost_types::ContactId(url.into()))
            .await
            .expect_err("a card that will not parse cannot project");
        let updated = account
            .contact_update(
                bifrost_types::ContactId(url.into()),
                bifrost_types::ContactPatch::default(),
            )
            .await
            .expect_err("an update cannot patch a card that will not parse");

        for (door, error) in [("contact_get", got), ("contact_update", updated)] {
            assert!(
                matches!(
                    error.kind(),
                    AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
                ),
                "{door}: {:?}",
                error.kind()
            );
            assert_eq!(
                error.recovery(),
                &RecoveryClass::ProviderContractViolation,
                "{door}"
            );
            assert!(
                error.chain().iter().any(|cause| matches!(
                    cause,
                    bifrost_types::Cause::Attempt(attempt)
                        if attempt.transmission_state
                            == bifrost_types::TransmissionState::Acknowledged
                )),
                "{door}: the server answered the GET"
            );
        }
        assert_eq!(transcripts(&script).len(), 2, "one GET per door, no PUT");
    }

    /// An empty address book home lists NOTHING - no fabricated placeholder.
    ///
    /// The phantom this pins the absence of pointed at the home itself and
    /// advertised `can_create_contacts: true`, so a consumer that trusted it and
    /// POSTed a vCard to the home URL got a 404 or 405 from a spec-correct
    /// server. An empty list also lets a consumer tell a genuinely empty backend
    /// from a real single book, and so reap stale ones.
    ///
    /// `bifrost-caldav::calendars_list` removed the identical shape for the
    /// identical reasons, and the two crates drifted on it for a long time
    /// precisely because neither side pinned it. Both are pinned now; keep them
    /// in step.
    #[tokio::test]
    async fn an_empty_home_lists_no_address_books_rather_than_a_phantom() {
        use bifrost_types::account::Account as _;

        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"/>".to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        }]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/ada/",
        );

        let books = account.address_books_list().await.expect("list");
        assert!(
            books.is_empty(),
            "an empty home must not fabricate an address book: {books:?}"
        );
    }

    /// A discovery that enumerates no address books leaves the OPENED account
    /// with no default, rather than the addressbook home standing in for one.
    ///
    /// Drives the real discovery-to-account path, so it bites where a pure test
    /// of the selection helper cannot: reintroducing the old
    /// `unwrap_or_else(|| resolve_url(&home))` at the `open` call site is
    /// invisible to a test that constructs the `None` itself. Twin of the
    /// CalDAV assertion; keep them in step.
    #[tokio::test]
    async fn an_empty_discovery_opens_an_account_with_no_default_address_book() {
        let script = discovery_script("/books/ada/");
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let account = crate::account::CardDavAccount::open_with_client(client)
            .await
            .expect("discovery succeeds against an empty backend");

        assert_eq!(
            account.default_addressbook_url, None,
            "an empty addressbook home must leave no default, not the home itself"
        );
        assert!(
            account.addressbook_urls.is_empty(),
            "an empty addressbook home advertises no collections"
        );
    }

    /// An empty backend refuses a collection-less call locally instead of
    /// addressing the addressbook home.
    ///
    /// Twin of `bifrost-caldav`'s
    /// `an_empty_backend_refuses_collection_less_calls_before_the_wire`; keep
    /// them in step. The home is not a collection when the walk came back empty
    /// (`list_addressbooks` returns the home itself when it genuinely is one),
    /// so the old `unwrap_or_else(|| resolve_url(&home))` default sent every one
    /// of these to a resource a spec-correct server 404s - reporting a local
    /// routing failure as a remote `NotFound`. The empty script is the bite:
    /// restore the fallback and these calls reach the transport and panic on
    /// exhaustion rather than failing quietly.
    #[tokio::test]
    async fn an_empty_backend_refuses_collection_less_calls_before_the_wire() {
        use bifrost_types::account::Account as _;

        let script = dav_script_empty();
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests_without_collections(
            client,
            "https://dav.example.test/books/",
        );

        account
            .contacts_list(None, None)
            .await
            .expect_err("a listing naming no address book has nothing to list");
        account
            .contact_create(bifrost_types::ContactCreate::default())
            .await
            .expect_err("a create naming no address book has nowhere to write");
        // Only the doors taking an `Option<AddressBookId>` route through the
        // default. `contact_get` / `contact_update` derive the collection from
        // the resource's own URL, and `bifrost_dav_core::url::parent_collection_url`
        // answers for every id that resolves absolute, so their fallback is
        // unreachable in practice.

        assert!(
            transcripts(&script).is_empty(),
            "an unroutable call must reach no transport at all"
        );
    }

    /// A principal whose answer parses but names no addressbook home is a
    /// MISSING FIELD, not a parse failure. Twin of `bifrost-caldav`'s test of
    /// the same name. Against the code before the reclassification the kind
    /// assertion fails on `ParseFailed`.
    #[tokio::test]
    async fn a_principal_naming_no_home_is_a_missing_field() {
        let response = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        let script = dav_script([
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>",
            ),
            response("<D:multistatus xmlns:D=\"DAV:\"/>"),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let error = client
            .discover_addressbook_home()
            .await
            .expect_err("a principal naming no home cannot open");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::MissingField)
        );
        assert_eq!(error.recovery(), &RecoveryClass::ProviderContractViolation);
        assert_eq!(error.protocol(), Some(Protocol::CardDav));
        assert!(
            error.chain().iter().any(|cause| matches!(
                cause,
                bifrost_types::Cause::Attempt(attempt)
                    if attempt.transmission_state
                        == bifrost_types::TransmissionState::Acknowledged
            )),
            "the principal answered: {error:?}"
        );
        assert_eq!(transcripts(&script).len(), 2);
    }

    fn discovery_script(home_href: &str) -> Arc<ScriptedDispatch> {
        let response = |body: String| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body,
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        dav_script([
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>"
                    .to_string(),
            ),
            response(format!(
                "<C:addressbook-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:href>{home_href}</D:href></C:addressbook-home-set>"
            )),
            response("<D:multistatus xmlns:D=\"DAV:\"/>".to_string()),
        ])
    }

    /// The legitimate deployment the origin allowlist must not break: the
    /// address book home lives on a different host than the principal. It is
    /// discovered over HTTPS, so it is credential-bearing.
    #[tokio::test]
    async fn discovered_cross_origin_https_home_receives_credentials() {
        bifrost_dav_core::test_support::discovery::discovered_home(
            bifrost_dav_core::DavProtocol::CardDav,
            false,
            |net, expected_home| async move {
                let mut client = CardDavClient::with_account_net("https://dav.example.test", net);
                let home = client.discover_addressbook_home().await?.home;
                assert_eq!(home, expected_home);
                client.admit_discovered_urls(std::iter::once(home.clone()));
                client.list_addressbooks(&home).await?;
                Ok(home)
            },
        )
        .await;
    }

    /// The reviewer-found hole this pins: a cross-origin hop followed inside
    /// reqwest arrives with `Authorization` stripped and cannot get it back.
    /// The manual re-dispatch must therefore carry fresh credentials to the
    /// admitted target - the assertion that bites is the auth header on the
    /// second transcript entry in the shared scenario. Its href assertion
    /// also keeps resolution tied to the post-redirect URI.
    #[tokio::test]
    async fn cross_origin_redirect_is_redispatched_with_credentials() {
        bifrost_dav_core::test_support::discovery::redirected_home(
            bifrost_dav_core::DavProtocol::CardDav,
            |net, home| async move {
                let mut client = CardDavClient::with_account_net("https://dav.example.test", net);
                client.admit_discovered_urls(std::iter::once(home.to_string()));
                let collections = client
                    .list_addressbooks("https://dav.example.test/collections/ada/")
                    .await?;
                Ok(collections[0].href.clone())
            },
        )
        .await;
    }

    /// RFC 4918 resolves a relative href against the EFFECTIVE request URI.
    /// The dispatcher follows same-origin hops, so a PROPFIND submitted to
    /// `/addressbook` can be served from `/dav/users/ada/addressbook/`.
    /// Resolving `one.vcf` against the submitted URI mints `/one.vcf` - a
    /// native id that does not exist, and a follow-up GET that 404s.
    ///
    /// Twin of bifrost-caldav's. The hop is scripted as the 301 it is, so the
    /// walk that produces the effective URI is on the path under test.
    #[tokio::test]
    async fn contact_hrefs_resolve_against_the_post_redirect_url() {
        let script = dav_script([
            dav_redirect(
                StatusCode::MOVED_PERMANENTLY,
                "https://dav.example.test/dav/users/ada/addressbook/",
            ),
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>one.vcf</D:href><D:propstat><D:prop><D:getetag>\"e1\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
                url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                    .expect("test DAV URL"),
            }
            .into(),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let listing = client
            .list_contacts_listing(
                "https://dav.example.test/addressbook",
                AccountOperation::ContactsList,
            )
            .await
            .expect("redirected listing succeeds");

        assert_eq!(
            listing.entries[0].uri,
            "https://dav.example.test/dav/users/ada/addressbook/one.vcf"
        );
    }

    /// Discovery is server-steered, so a discovered home must never weaken the
    /// transport guarantee the configured HTTPS base URL established. The
    /// assertion that matters is the destination: no request at all reaches the
    /// plaintext origin, credential-bearing or otherwise.
    #[tokio::test]
    async fn discovered_plaintext_home_never_receives_credentials() {
        bifrost_dav_core::test_support::discovery::discovered_home(
            bifrost_dav_core::DavProtocol::CardDav,
            true,
            |net, expected_home| async move {
                let mut client = CardDavClient::with_account_net("https://dav.example.test", net);
                let home = client.discover_addressbook_home().await?.home;
                assert_eq!(home, expected_home);
                client.admit_discovered_urls(std::iter::once(home.clone()));
                client.list_addressbooks(&home).await?;
                Ok(home)
            },
        )
        .await;
    }

    /// A cross-address-book `contact_update` MOVES the resource, and a restated
    /// address book still updates in place.
    ///
    /// Twin of `bifrost-caldav`'s
    /// `event_update_moves_across_calendars_and_updates_in_place_otherwise`;
    /// keep them in step. This crate refused the relocation before dav-B11 and,
    /// unlike its CalDAV twin, never pinned the refusal - which is why the
    /// behaviour change here broke no test. The pair is pinned now.
    #[tokio::test]
    async fn contact_update_moves_across_address_books_and_updates_in_place_otherwise() {
        use bifrost_types::account::Account as _;

        // The current resource is read with a plain GET now, not a multiget.
        let fetched = || DavResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:One\r\nEND:VCARD\r\n".to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        let contact =
            || bifrost_types::ContactId("https://dav.example.test/books/work/one.vcf".to_string());
        let patch_to = |book: &str| bifrost_types::ContactPatch {
            address_book_id: Some(bifrost_types::AddressBookId(book.to_string())),
            ..Default::default()
        };

        // A move: GET the current resource, then MOVE it.
        let script = dav_script([
            fetched(),
            DavResponse {
                status: StatusCode::CREATED,
                headers: HeaderMap::new(),
                body: String::new(),
                url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                    .expect("test DAV URL"),
            },
        ]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/work/",
        );
        account
            .contact_update(contact(), patch_to("https://dav.example.test/books/home/"))
            .await
            .expect("a move between address books is performed");
        let requests = transcripts(&script);
        assert_eq!(
            requests.len(),
            2,
            "a move-only patch is a GET and a MOVE, with no content write"
        );
        assert_eq!(requests[1].method.as_str(), "MOVE");
        assert_eq!(
            requests[1]
                .headers
                .get("Destination")
                .and_then(|value| value.to_str().ok()),
            Some("https://dav.example.test/books/home/one.vcf"),
            "the destination keeps the resource's own file name"
        );
        assert_eq!(
            requests[1]
                .headers
                .get("Overwrite")
                .and_then(|value| value.to_str().ok()),
            Some("F"),
            "a name collision at the destination must refuse, not overwrite"
        );

        // Restating the contact's own address book is not a move.
        let script = dav_script([
            fetched(),
            DavResponse {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: String::new(),
                url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                    .expect("test DAV URL"),
            },
        ]);
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/work/",
        );
        account
            .contact_update(contact(), patch_to("https://dav.example.test/books/work/"))
            .await
            .expect("restating the current address book is not a move");
        let methods = transcripts(&script)
            .into_iter()
            .map(|request| request.method.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(methods, vec!["GET", "PUT"]);
    }

    /// A server without MOVE falls back to copy-then-delete, and a failure of
    /// the delete leg reports the move as HALF applied. Twin of the CalDAV
    /// assertion; keep them in step.
    #[tokio::test]
    async fn a_contact_move_without_server_move_support_copies_then_deletes() {
        use bifrost_types::account::Account as _;

        let response = |status: StatusCode, body: &str| DavResponse {
            status,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        let script = dav_script(
            [
                response(
                    StatusCode::OK,
                    "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:One\r\nEND:VCARD\r\n",
                ),
                response(StatusCode::METHOD_NOT_ALLOWED, ""),
                response(StatusCode::CREATED, ""),
            ]
            .into_iter()
            .map(Canned::from)
            // The DELETE of the original 500s, and a 500 is now retried to
            // exhaustion before it surfaces.
            .chain(dav_retried(response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "boom",
            )))
            .collect::<Vec<_>>(),
        );
        let client = Arc::new(CardDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CardDavAccount::for_tests(
            client,
            "https://dav.example.test/books/work/",
        );

        let error = account
            .contact_update(
                bifrost_types::ContactId("https://dav.example.test/books/work/one.vcf".to_string()),
                bifrost_types::ContactPatch {
                    address_book_id: Some(bifrost_types::AddressBookId(
                        "https://dav.example.test/books/home/".to_string(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a failed cleanup is not a success");

        assert!(matches!(error.kind(), AccountErrorKind::Server(_)));
        let record = error.partial_commit().expect("committed copy");
        assert_eq!(
            record.steps()[0].effect(),
            Some(bifrost_types::StepEffect::Copied)
        );
        assert_eq!(record.plan().replay, bifrost_types::ReplaySafety::Never);
        assert!(
            matches!(
                error.recovery(),
                RecoveryClass::Reconcile(advice)
                    if advice.guidance.actions.contains(&ReconcileAction::CheckTarget)
            ),
            "a half-applied move must reconcile, not replay: {:?}",
            error.recovery()
        );
        let methods = transcripts(&script)
            .into_iter()
            .map(|request| request.method.as_str().to_string())
            .collect::<Vec<_>>();
        // The trailing DELETEs are the retry budget being spent on the 500.
        assert_eq!(
            methods,
            vec!["GET", "MOVE", "PUT", "DELETE", "DELETE", "DELETE"]
        );
    }

    #[tokio::test]
    async fn fetch_vcards_uses_scripted_report_transcript() {
        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>/book/one.vcf</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:One\nEND:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        }]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let fetched = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &["https://dav.example.test/book/one.vcf".to_string()],
                AccountOperation::ContactsList,
            )
            .await
            .expect("scripted multiget succeeds");

        assert_eq!(
            fetched.report.items[0].uri,
            "https://dav.example.test/book/one.vcf"
        );
        let requests = transcripts(&script);
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].method,
            Method::from_bytes(b"REPORT").expect("REPORT method")
        );
        assert_eq!(requests[0].url, "https://dav.example.test/book/");
        assert_eq!(
            requests[0]
                .headers
                .get("depth")
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
        assert_eq!(
            requests[0]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    /// Multiget fan-out is bounded by `MULTIGET_LEG_CONCURRENCY`, not by the
    /// caller's uri list: one large address book must not open hundreds of
    /// simultaneous REPORTs, and no layer below this one bounds them.
    ///
    /// Twin of bifrost-caldav's bound. The probe sits at the wire: the yielding
    /// script raises its in-flight count for each outstanding dispatch, so the
    /// mark measures what the net pipeline holds open.
    #[tokio::test]
    async fn multiget_never_holds_more_legs_open_than_the_concurrency_bound() {
        bifrost_dav_core::test_support::collection::multiget_concurrency::<CardDavDialect>().await;
    }

    #[tokio::test]
    async fn chunked_multiget_keeps_prior_chunk_when_later_http_leg_fails() {
        bifrost_dav_core::test_support::collection::partial_multiget::<CardDavDialect>(true).await;
    }

    /// The adjacent leg-failure path, mirroring the CalDAV twin: a later chunk
    /// whose body will not parse must degrade like a later chunk that returned
    /// 503, not discard the chunks that already materialized.
    #[tokio::test]
    async fn chunked_multiget_keeps_prior_chunk_when_later_body_is_malformed() {
        bifrost_dav_core::test_support::collection::partial_multiget::<CardDavDialect>(false).await;
    }

    /// Nothing usable anywhere is still a failed call, malformed or not.
    #[tokio::test]
    async fn an_only_leg_that_will_not_parse_is_still_an_error() {
        bifrost_dav_core::test_support::collection::wholly_malformed_multiget::<CardDavDialect>()
            .await;
    }

    /// Shared-shape guard against the CalDAV regression: a non-2xx REPORT must
    /// classify, never decode into an authoritative empty multiget. An empty
    /// result treated as truth is a downstream deletion.
    #[tokio::test]
    async fn unauthorized_report_classifies_as_reauthorization() {
        // A bearer 401 gets one refresh and replay before the terminal
        // reauthorization error. Script both wire answers so this exercises
        // that path rather than exhausting the transport double.
        let unauthorized = || DavResponse {
            status: StatusCode::UNAUTHORIZED,
            headers: HeaderMap::new(),
            body: "<html><body>401 Unauthorized</body></html>".to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        };
        let script = dav_script([unauthorized(), unauthorized()]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let Err(error) = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &["https://dav.example.test/book/one.vcf".to_string()],
                AccountOperation::ContactsList,
            )
            .await
        else {
            panic!("401 REPORT must not be reported as an empty result");
        };

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Authentication(
                bifrost_types::AuthErrorKind::ReauthorizationRequired
            )
        );
    }

    #[test]
    fn resolve_url_fallback_preserves_separator() {
        let client =
            CardDavClient::with_account_net("not a url", scripted_dav_net(&dav_script_empty()));

        assert_eq!(
            client.resolve_url("addressbook/one.vcf"),
            "not a url/addressbook/one.vcf"
        );
        assert_eq!(
            client.resolve_url("/addressbook/one.vcf"),
            "not a url/addressbook/one.vcf"
        );
    }

    #[test]
    fn addressbook_text_query_body_uses_property_text_match() {
        let body = addressbook_text_query_body("EMAIL", "ada & team");

        assert!(body.contains("<C:addressbook-query"));
        assert!(body.contains("<D:resourcetype/>"));
        assert!(body.contains("<D:getetag/>"));
        // The whole point of the filtered lane: the REPORT names hrefs, and
        // only the sliced page is hydrated. Asking for bodies here puts the
        // entire matching result set on the wire once per page.
        assert!(
            !body.contains("<C:address-data/>"),
            "the filtered query must not hydrate: {body}"
        );
        assert!(body.contains("<C:prop-filter name=\"EMAIL\">"));
        assert!(body.contains("ada &amp; team"));
    }

    #[test]
    fn addressbook_text_query_escapes_quotes_consistently() {
        let body = addressbook_text_query_body("EMAIL", "Ada's \"team\"");
        assert!(body.contains("Ada&apos;s &quot;team&quot;"));
    }

    #[tokio::test]
    async fn addressbook_multiget_uses_depth_zero() {
        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"/>".to_string(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        }]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &["https://dav.example.test/book/opaque-id".to_string()],
                AccountOperation::ContactsList,
            )
            .await
            .expect("empty multistatus is usable");

        let requests = transcripts(&script);
        assert_eq!(requests[0].headers["Depth"], "0");
        assert!(requests[0].body.contains("<D:resourcetype/>"));
    }

    #[test]
    fn complete_multiget_failure_uses_embedded_status_classification() {
        let report = CardDavMultigetReport {
            items: Vec::new(),
            failed: vec![crate::parse::CardDavFailedResource {
                href: "/contacts/one.vcf".to_string(),
                status: Some(401),
            }],
            missing_data: Vec::new(),
        };

        let error = report
            .failure::<CardDavDialect>(AccountOperation::ContactsList)
            .expect("all-401 report is a complete failure");
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Authentication(
                bifrost_types::AuthErrorKind::ReauthorizationRequired
            )
        );
    }

    #[test]
    fn accumulated_multiget_success_keeps_a_later_refusal_per_resource() {
        let report = CardDavMultigetReport {
            items: vec![crate::parse::CardDavFetchedVCard {
                uri: "/contacts/one.vcf".to_string(),
                etag: None,
                data: "BEGIN:VCARD\nEND:VCARD".to_string(),
            }],
            failed: vec![crate::parse::CardDavFailedResource {
                href: "/contacts/two.vcf".to_string(),
                status: Some(401),
            }],
            missing_data: Vec::new(),
        };

        assert!(
            report
                .failure::<CardDavDialect>(AccountOperation::ContactsList)
                .is_none()
        );
    }

    fn usable_report() -> CardDavMultigetReport {
        CardDavMultigetReport {
            items: vec![crate::parse::CardDavFetchedVCard {
                uri: "/contacts/one.vcf".to_string(),
                etag: None,
                data: "BEGIN:VCARD\nEND:VCARD".to_string(),
            }],
            failed: Vec::new(),
            missing_data: Vec::new(),
        }
    }

    #[test]
    fn a_refused_leg_after_a_usable_one_keeps_the_cards_and_the_recovery_class() {
        let refusal = status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::ContactSearch,
            StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );

        let fetch = MultigetFetch::settle(usable_report(), Some(refusal))
            .expect("a partial result is not a failed call");

        assert_eq!(fetch.report.items.len(), 1);
        assert_eq!(
            fetch.degraded.expect("the refusal survives").recovery(),
            &RecoveryClass::AuthLost
        );
    }

    #[test]
    fn a_refused_leg_with_nothing_usable_stays_an_error() {
        let refusal = status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::ContactSearch,
            StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );

        let error = MultigetFetch::settle(CardDavMultigetReport::default(), Some(refusal))
            .err()
            .expect("nothing usable came back");

        assert_eq!(error.recovery(), &RecoveryClass::AuthLost);
    }
}

#[cfg(test)]
mod directory_guard_contract_tests {
    use super::*;
    use bifrost_dav_core::DavResponse;
    use bifrost_dav_core::test_support::{dav_script, scripted_dav_net, transcripts};
    use reqwest::StatusCode;

    #[tokio::test]
    async fn directory_round_three_every_account_operation_is_guarded_by_default() {
        // Check the typed matrix against the authoritative enum source, so a
        // newly added operation cannot silently fall outside this test.
        let operations = [
            AccountOperation::CategoryDefinitionsList,
            AccountOperation::MessageReactionsRead,
            AccountOperation::Discover,
            AccountOperation::EstablishCursor,
            AccountOperation::DiscoverCursorScopes,
            AccountOperation::DiscoverMemberships,
            AccountOperation::ScopeLifecycle,
            AccountOperation::SyncInventory,
            AccountOperation::SyncChanges,
            AccountOperation::Hydrate,
            AccountOperation::HydrateThread,
            AccountOperation::HydrateMessage,
            AccountOperation::OpenBlob,
            AccountOperation::OpenBlobRange,
            AccountOperation::OpenRawRfc822,
            AccountOperation::PushSubscribe,
            AccountOperation::PushUnsubscribe,
            AccountOperation::PushStream,
            AccountOperation::UpdateFlags,
            AccountOperation::SetStarred,
            AccountOperation::MarkReplied,
            AccountOperation::MarkForwarded,
            AccountOperation::MarkMdnSent,
            AccountOperation::BulkMove,
            AccountOperation::BulkDestroy,
            AccountOperation::MoveThread,
            AccountOperation::DeleteThread,
            AccountOperation::AddToContainer,
            AccountOperation::RemoveFromContainer,
            AccountOperation::SetKeyword,
            AccountOperation::SetLabelMembership,
            AccountOperation::SetCategory,
            AccountOperation::SetExtendedProperty,
            AccountOperation::SetImportance,
            AccountOperation::SetIsRead,
            AccountOperation::Send,
            AccountOperation::AttachmentUpload,
            AccountOperation::HostAttachment,
            AccountOperation::DraftCreate,
            AccountOperation::DraftUpdate,
            AccountOperation::DraftDiscard,
            AccountOperation::DraftSend,
            AccountOperation::CancelScheduledSend,
            AccountOperation::RescheduleSend,
            AccountOperation::Search,
            AccountOperation::SearchMessages,
            AccountOperation::ContainersList,
            AccountOperation::ContainerCreate,
            AccountOperation::ContainerRename,
            AccountOperation::ContainerMove,
            AccountOperation::ContainerDelete,
            AccountOperation::IdentitiesList,
            AccountOperation::IdentityUpdate,
            AccountOperation::VacationGet,
            AccountOperation::VacationSet,
            AccountOperation::QuotaGet,
            AccountOperation::FiltersList,
            AccountOperation::FilterCreate,
            AccountOperation::FilterUpdate,
            AccountOperation::FilterDelete,
            AccountOperation::FilterValidate,
            AccountOperation::AddressBooksList,
            AccountOperation::ContactsList,
            AccountOperation::ContactGet,
            AccountOperation::ContactCreate,
            AccountOperation::ContactUpdate,
            AccountOperation::ContactDelete,
            AccountOperation::ContactSearch,
            AccountOperation::ContactAutocomplete,
            AccountOperation::DirectorySearch,
            AccountOperation::DirectoryGroupsList,
            AccountOperation::DirectoryGroupExpand,
            AccountOperation::CalendarsList,
            AccountOperation::EventsInRange,
            AccountOperation::EventGet,
            AccountOperation::EventCreate,
            AccountOperation::EventUpdate,
            AccountOperation::EventDelete,
            AccountOperation::EventRsvp,
            AccountOperation::EventSearch,
            AccountOperation::EventAutocomplete,
            AccountOperation::Close,
            AccountOperation::Expunge,
        ];
        let source = include_str!("../../types/src/error/scope.rs");
        let declared = source
            .split("pub enum AccountOperation {")
            .nth(1)
            .expect("enum")
            .split("\n}")
            .next()
            .expect("enum body")
            .lines()
            .filter_map(|line| line.trim().strip_suffix(','))
            .filter(|name| {
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            })
            .collect::<Vec<_>>();
        assert_eq!(
            operations
                .iter()
                .map(|op| format!("{op:?}"))
                .collect::<Vec<_>>(),
            declared
        );
        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: Default::default(),
            body: String::new(),
            url: bifrost_dav_core::DavHref::parse("https://dav.example.test/")
                .expect("test DAV URL"),
        }]);
        let mut client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let gateway = "https://dav.example.test/directory/";
        client.initialize_directories([gateway.to_string()], vec![gateway.to_string()]);
        client
            .directory_urls
            .observe(["https://dav.example.test/later/".to_string()]);
        for operation in operations {
            for verb in [
                "GET", "HEAD", "OPTIONS", "PROPFIND", "REPORT", "PUT", "DELETE", "MOVE", "FUTURE",
            ] {
                let method = Method::from_bytes(verb.as_bytes()).expect("method");
                for url in [
                    gateway,
                    "https://dav.example.test/directory/sub/",
                    "https://dav.example.test/later/",
                ] {
                    let result = client
                        .dav
                        .send_raw_request(DavRequest::new(method.clone(), url), operation)
                        .await;
                    if operation == AccountOperation::DirectorySearch
                        && verb == "REPORT"
                        && url == gateway
                    {
                        assert!(result.is_ok(), "only the search gateway REPORT is exempt");
                    } else {
                        let error = result
                            .expect_err("all other directory requests are refused before dispatch");
                        assert!(
                            matches!(error.kind(), bifrost_types::AccountErrorKind::Unsupported(op) if *op == operation),
                            "{operation:?} {verb} {url}: {error:?}"
                        );
                    }
                }
            }
        }
        let sent = transcripts(&script);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].method.as_str(), "REPORT");
        assert_eq!(sent[0].url, gateway);
    }
}

#[cfg(test)]
mod unsupported_constructor_tests {
    use super::*;

    #[test]
    fn unsupported_constructor_stamps_protocol_and_kind() {
        let operation = bifrost_types::AccountOperation::Send;
        let protocol = bifrost_types::Protocol::CardDav;
        let error = unsupported_error(operation);
        assert_eq!(error.protocol(), Some(protocol));
        assert_eq!(error.operation(), Some(operation));
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(operation)
        );
    }
}
