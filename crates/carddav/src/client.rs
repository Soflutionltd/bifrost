use std::fmt;

use bifrost_dav_core::{
    DavDispatch, DavProtocol, DavRequest, escape_xml, filter_unsupported, normalize_http_etag,
    prepare_if_match, worse_recovery,
};
pub(crate) use bifrost_dav_core::{FilteredHrefs, HrefQuery, PutCondition};
use bifrost_net::{AccountId, AccountNet};
use bifrost_types::{AccountError, AccountOperation, ErrorScope};
use reqwest::header::{CONTENT_TYPE, ETAG};
use reqwest::{Method, StatusCode};

use crate::CardDavConfig;
use crate::parse::{
    AddressBookCollection, CardDavContactListing, CardDavFetchedVCard, CardDavMultigetReport,
    MultigetOutcome, extract_href_property, parse_addressbook_collections, parse_multiget_report,
    parse_propfind_contacts, resolve_href,
};

const MULTIGET_BATCH_SIZE: usize = 50;

/// In-flight REPORT legs a single multiget or text search may hold open.
///
/// The chunk count is driven by the caller's uri list, so an unbounded
/// `join_all` over it lets one large collection launch hundreds of
/// simultaneous REPORTs at a server that never agreed to that. This per-call
/// local policy bounds one input-sized fan-out. No account-wide DAV bound is
/// needed yet, and no server advertises one; bifrost-net's `ConcurrencyLimit`
/// is available if one is wanted. Ordered (`buffered`, not
/// `buffer_unordered`) so the merged report and the surviving degraded error
/// stay deterministic regardless of completion order.
const MULTIGET_LEG_CONCURRENCY: usize = 4;

/// This crate's dialect binding for the shared DAV layer.
///
/// Every shared constructor takes it, so a CardDAV error can never be stamped
/// with a CalDAV `Protocol` or name a `ResourceKind::Calendar`.
const DAV: DavProtocol = DavProtocol::CardDav;

#[derive(Clone)]
pub(crate) struct CardDavClient {
    /// Transport, credentials, origin gate and the generic WebDAV verbs, all
    /// shared with `bifrost-caldav` through `bifrost-dav-core`.
    dav: DavDispatch,
}

impl fmt::Debug for CardDavClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardDavClient")
            .field("dav", &self.dav)
            .finish_non_exhaustive()
    }
}

impl CardDavClient {
    pub(crate) fn new(account_id: AccountId, config: &CardDavConfig) -> Self {
        Self {
            dav: DavDispatch::new(
                account_id,
                &config.base_url,
                config.credentials.to_shared(),
                DAV,
            ),
        }
    }

    /// The account handle carrying this account's meter, priority and cap.
    pub(crate) fn net(&self) -> &AccountNet {
        self.dav.net()
    }

    #[cfg(test)]
    pub(crate) fn with_account_net(base_url: &str, net: AccountNet) -> Self {
        Self {
            dav: DavDispatch::with_account_net(
                net,
                base_url,
                crate::CardDavCredentials::bearer("token").to_shared(),
                DAV,
            ),
        }
    }

    /// The principal walk (well-known probe, fallback predicate, configured
    /// base) is `bifrost-dav-core`'s, shared with `bifrost-caldav`; only the
    /// addressbook-home lookup after it is CardDAV's own. This crate's copy of
    /// the walk is the one that once diverged, parsing the probe body outside
    /// the lookup so a probe answering `200 text/html` never reached the
    /// fallback predicate.
    pub(crate) async fn discover_addressbook_home(&self) -> Result<String, AccountError> {
        let principal = self.dav.discover_current_user_principal().await?;
        self.addressbook_home_for_principal(principal).await
    }

    async fn addressbook_home_for_principal(
        &self,
        principal: String,
    ) -> Result<String, AccountError> {
        let response = self
            .dav
            .propfind_raw(
                &principal,
                "0",
                PROPFIND_ADDRESSBOOK_HOME,
                AccountOperation::Discover,
            )
            .await?;
        extract_href_property(&response.text, "addressbook-home-set")
            .map_err(|error| parse_error(AccountOperation::Discover, error))?
            .map(|href| resolve_href(&response.url, &href))
            .ok_or_else(|| {
                bifrost_dav_core::missing_field_error(
                    AccountOperation::Discover,
                    "missing addressbook-home-set",
                    DAV,
                )
            })
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
            .propfind_raw(home_url, "1", PROPFIND_ADDRESSBOOKS, operation)
            .await?;
        let mut collections = parse_addressbook_collections(&response.text)
            .map_err(|error| parse_error(operation, format!("addressbook list: {error}")))?;
        for collection in &mut collections {
            collection.resolve_href(&response.url);
        }
        Ok(collections)
    }

    /// Cheap depth-0 PROPFIND for the collection `getctag`. Returns
    /// `None` when the server omits it, so the caller falls through to a
    /// full snapshot + diff (brick 8 ctag short-circuit).
    pub(crate) async fn collection_ctag(
        &self,
        addressbook_url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let response = self
            .dav
            .propfind_raw(addressbook_url, "0", PROPFIND_CTAG, operation)
            .await?;
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
            .propfind_raw(addressbook_url, "1", PROPFIND_CONTACTS, operation)
            .await?;
        let mut listing = parse_propfind_contacts(&response.text)
            .map_err(|error| parse_error(operation, format!("contact list: {error}")))?;
        listing.resolve_hrefs(&response.url);
        if let Some(error) = listing_failure(&listing, operation) {
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
        let mut all_results = CardDavMultigetReport::default();
        let mut degraded = None;
        let bodies = uris
            .chunks(MULTIGET_BATCH_SIZE)
            .map(|chunk| {
                let mut href_elements = String::new();
                for uri in chunk {
                    href_elements.push_str("  <D:href>");
                    href_elements.push_str(&escape_xml(uri));
                    href_elements.push_str("</D:href>\n");
                }

                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:addressbook-multiget xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
    <C:address-data/>\n\
  </D:prop>\n\
{href_elements}</C:addressbook-multiget>"
                );
                body
            })
            .collect::<Vec<_>>();
        let legs: Vec<_> = bodies
            .iter()
            .map(|body| {
                self.run_leg(MultigetLeg {
                    url: addressbook_url,
                    depth: "0",
                    body,
                    operation,
                    context: "multiget",
                })
            })
            .collect();
        let mut legs =
            futures::StreamExt::buffered(futures::stream::iter(legs), MULTIGET_LEG_CONCURRENCY);
        while let Some((report, error)) = futures::StreamExt::next(&mut legs).await {
            all_results.extend(report);
            if let Some(error) = error {
                degraded = worse_recovery(degraded, error);
            }
        }
        MultigetFetch::settle(all_results, degraded)
    }

    /// Run one REPORT leg of a multi-leg fetch and fold its outcome into the
    /// caller's accumulators.
    ///
    /// This is the only way a leg result enters `all_results`, and it is what
    /// makes the partial-result contract structural rather than a habit. A leg
    /// can fail four ways - transport, a non-2xx status, a body that will not
    /// parse, and a 207 that describes complete failure - and all four land in
    /// `degraded` here. Routing only some of them (the shape this replaced
    /// classified the HTTP failure but kept `?` on the parse) meant a malformed
    /// body on chunk 3 of 40 threw away chunks 1 and 2, which is exactly the
    /// loss the degraded lane exists to prevent. The function returns nothing,
    /// so a leg added later has no unrouted path available to it.
    ///
    /// A malformed body is account-authored data, so it is classified and
    /// survived, never asserted on. `MultigetFetch::settle` is what turns "every
    /// leg failed and nothing materialized anywhere" back into an `Err` carrying
    /// the worst recovery class seen.
    ///
    /// The CalDAV twin of this function must stay in step with it.
    async fn accumulate_leg(
        &self,
        leg: MultigetLeg<'_>,
        all_results: &mut CardDavMultigetReport,
        degraded: &mut Option<AccountError>,
    ) {
        let MultigetLeg {
            url,
            depth,
            body,
            operation,
            context,
        } = leg;
        let response = match self.dav.report_raw(url, depth, body, operation).await {
            Ok(response) => response,
            Err(error) => {
                *degraded = worse_recovery(degraded.take(), error);
                return;
            }
        };
        let mut parsed = match parse_multiget_report(&response.text) {
            Ok(parsed) => parsed,
            Err(error) => {
                *degraded = worse_recovery(
                    degraded.take(),
                    parse_error(operation, format!("{context}: {error}")),
                );
                return;
            }
        };
        parsed.resolve_hrefs(&response.url);
        if let Some(error) = multiget_failure(&parsed, operation) {
            *degraded = worse_recovery(degraded.take(), error);
        }
        all_results.extend(parsed);
    }

    async fn run_leg(&self, leg: MultigetLeg<'_>) -> (CardDavMultigetReport, Option<AccountError>) {
        let mut report = CardDavMultigetReport::default();
        let mut degraded = None;
        self.accumulate_leg(leg, &mut report, &mut degraded).await;
        (report, degraded)
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
        let legs: Vec<_> = bodies
            .iter()
            .map(|body| self.href_query_leg(addressbook_url, body, AccountOperation::ContactSearch))
            .collect();
        let mut legs =
            futures::StreamExt::buffered(futures::stream::iter(legs), MULTIGET_LEG_CONCURRENCY);
        let mut merged = HrefQuery::default();
        let mut unsupported = false;
        while let Some(leg) = futures::StreamExt::next(&mut legs).await {
            match leg {
                HrefLeg::Listing(listing) => extend_candidates(&mut merged, listing),
                HrefLeg::FilterUnsupported => unsupported = true,
                HrefLeg::Failed(error) => {
                    merged.degraded = worse_recovery(merged.degraded.take(), error);
                }
            }
        }
        if unsupported {
            return Ok(FilteredHrefs::FilterUnsupported);
        }
        merged.settle().map(FilteredHrefs::Matched)
    }

    /// Run one filtered `addressbook-query` leg, separating "the server will not
    /// run this filter" from every other way the leg can fail.
    ///
    /// The status has to be read before it becomes an error, which is why this
    /// goes through `report_raw_response` rather than `report_raw`: a 403 naming
    /// `CARDDAV:supported-filter` is a degrade signal, and a 403 naming nothing
    /// is still a permission refusal.
    async fn href_query_leg(
        &self,
        addressbook_url: &str,
        body: &str,
        operation: AccountOperation,
    ) -> HrefLeg {
        let response = match self
            .dav
            .report_raw_response(addressbook_url, "1", body, operation)
            .await
        {
            Ok(response) => response,
            Err(error) => return HrefLeg::Failed(error),
        };
        if !response.status.is_success() {
            return if filter_unsupported(response.status, &response.body) {
                HrefLeg::FilterUnsupported
            } else {
                HrefLeg::Failed(status_error(operation, response.status, response.body))
            };
        }
        let mut listing = match parse_propfind_contacts(&response.body) {
            Ok(listing) => listing,
            Err(error) => {
                return HrefLeg::Failed(parse_error(operation, format!("query: {error}")));
            }
        };
        listing.resolve_hrefs(&response.url);
        // A 207 in which EVERY response failed is a complete failure, not an
        // empty candidate set: the candidate lane is read by the listing
        // parser, and until its failure lane carried statuses this arm could
        // only report the hrefs, so an all-refused query came back as an empty
        // page and a consumer recorded a completed walk over a collection it
        // had been refused.
        match listing_failure(&listing, operation) {
            Some(error) => HrefLeg::Failed(error),
            None => HrefLeg::Listing(listing),
        }
    }

    /// Fetch one vCard resource with a plain `GET`, reading the validator from
    /// the `ETag` response header.
    ///
    /// Twin of `bifrost-caldav`'s `get_event`; keep them in step. Single-resource
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
        let request = self
            .dav
            .request(Method::GET, url)
            .headers(self.dav.auth_headers(url, operation).await?);
        let response = self.dav.send_raw_request(request, operation).await?;
        let status = response.status;
        let etag = response
            .headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(normalize_http_etag);
        if status.is_success() {
            Ok(CardDavFetchedVCard {
                uri: url.to_string(),
                etag,
                data: response.body,
            })
        } else {
            Err(status_error(operation, status, response.body))
        }
    }

    pub(crate) async fn put_vcard(
        &self,
        url: &str,
        body: String,
        condition: PutCondition<'_>,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let mut request = self
            .dav
            .request(Method::PUT, url)
            .header(CONTENT_TYPE, "text/vcard; charset=utf-8")
            .headers(self.dav.auth_headers(url, operation).await?)
            .body(body);
        match condition {
            // Twin of `bifrost-caldav`'s `put_vcard` conditional; see there for
            // why a create-PUT must not be replayed after a mid-flight drop.
            PutCondition::IfNoneMatch => {
                request = request.header("If-None-Match", "*").idempotent(false);
            }
            PutCondition::IfMatch(etag) => {
                if let Some(etag) = prepare_if_match(etag) {
                    request = request.header("If-Match", etag);
                }
            }
            PutCondition::None => {}
        }
        self.send_status_request(request, operation).await
    }

    /// WebDAV `MOVE` of one resource into another collection.
    ///
    /// Twin of `bifrost-caldav`'s `move_resource`; keep them in step. See there
    /// for why `Overwrite: F`, why the destination is credential-gated, and why
    /// only 405/501 are fallback triggers.
    pub(crate) async fn move_resource(
        &self,
        from: &str,
        to: &str,
        operation: AccountOperation,
    ) -> Result<bool, AccountError> {
        self.dav.move_resource(from, to, operation).await
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
        let request = self
            .dav
            .request(Method::DELETE, url)
            .headers(self.dav.auth_headers(url, operation).await?);
        let response = self.dav.send_raw_request(request, operation).await?;
        if bifrost_dav_core::delete_target_already_gone(response.status) {
            return Ok(None);
        }
        let etag = response
            .headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(normalize_http_etag);
        if response.status.is_success() {
            Ok(etag)
        } else {
            Err(status_error(operation, response.status, response.body))
        }
    }

    /// A status-only request that also hands back the response ETag.
    ///
    /// Deliberately NOT `DavDispatch::send_status_request`, which discards the
    /// response. This is a real divergence from `bifrost-caldav` rather than
    /// drift: CardDAV's `put_vcard` and `delete_vcard` return the new validator
    /// to their callers, and CalDAV's equivalents return `()`. Collapsing the
    /// two during the dav-core extraction would have silently dropped the etag
    /// every CardDAV write reports.
    async fn send_status_request(
        &self,
        request: DavRequest,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let response = self.dav.send_raw_request(request, operation).await?;
        let etag = response
            .headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(normalize_http_etag);
        if response.status.is_success() {
            Ok(etag)
        } else {
            Err(status_error(operation, response.status, response.body))
        }
    }

    pub(crate) fn resolve_url(&self, href: &str) -> String {
        self.dav.resolve_url(href)
    }

    pub(crate) fn admit_discovered_urls(&mut self, urls: impl IntoIterator<Item = String>) {
        self.dav.admit_discovered_urls(urls);
    }
}

/// Whether a URL's scheme carries an authenticated, encrypted transport.
///
/// Only `https` qualifies; an unparseable URL is treated as insecure so the
/// downgrade check fails closed.
/// The prop skeleton the filtered query asks for.
///
/// Deliberately WITHOUT `address-data`: the filtered lane pages on the href and
/// hydrates only the page, so a query that answered with bodies would put the
/// whole matching result set on the wire once per page. Twin of the CalDAV
/// constant of the same name.
const QUERY_HREF_PROPS: &str = "  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
  </D:prop>\n";

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

/// A multi-REPORT fetch: everything that came back usable, plus the
/// recovery classification of any single REPORT that failed wholly.
///
/// Multiget is chunked and text search runs one REPORT per property, so a
/// walk can meet a 401, 403, or 503 on one leg after other legs already
/// returned cards. Aborting the whole call throws those cards away;
/// folding the refusal into anonymous `failed_ids` throws the RECOVERY
/// signal away, and the consumer can no longer tell "reauthorize" from
/// "retry later" from "this resource is gone". So each REPORT is
/// classified where it happens, and the worst class survives to the
/// caller in `degraded`, which the account layer publishes as a
/// `Page::skipped_scopes` entry: the walk did not finish this collection.
/// One REPORT leg of a multi-leg fetch, as handed to `accumulate_leg`.
struct MultigetLeg<'a> {
    url: &'a str,
    depth: &'a str,
    body: &'a str,
    operation: AccountOperation,
    /// Names the leg in a parse-failure message ("multiget", "query").
    context: &'a str,
}

pub(crate) struct MultigetFetch {
    pub(crate) report: CardDavMultigetReport,
    pub(crate) degraded: Option<AccountError>,
}

/// Fold one listing's committed and refused hrefs into a candidate set.
///
/// The listing lane merges one, the text lane eight, and they differ in nothing
/// else. Twin of the CalDAV function of the same name.
pub(crate) fn extend_candidates(query: &mut HrefQuery, listing: CardDavContactListing) {
    let failed_hrefs = listing.failed_hrefs();
    query.extend(
        listing.entries.into_iter().map(|entry| entry.uri),
        failed_hrefs,
    );
}

/// One leg of a filtered query, before the lane decides what to do with it.
enum HrefLeg {
    Listing(CardDavContactListing),
    FilterUnsupported,
    Failed(AccountError),
}

impl MultigetFetch {
    /// A wholly-failed fetch with nothing usable anywhere is still a failed
    /// call: there is no partial result to preserve, so it keeps riding the
    /// `Err` arm with its original classification. "Nothing usable" means no
    /// observations of ANY kind - a successful leg that reported all its
    /// resources 404 (genuine deletions in `failed`) or data-less
    /// (`missing_data`) has still answered for those resources, and a
    /// degraded sibling leg must not throw those verdicts away and force the
    /// caller to re-walk them.
    fn settle(
        report: CardDavMultigetReport,
        degraded: Option<AccountError>,
    ) -> Result<Self, AccountError> {
        let no_observations =
            report.cards.is_empty() && report.failed.is_empty() && report.missing_data.is_empty();
        match degraded {
            Some(error) if no_observations => Err(error),
            degraded => Ok(Self { report, degraded }),
        }
    }
}

/// Keep whichever failure demands the more drastic recovery, so a 503 on
/// one chunk cannot hide a 401 on another.
fn multiget_failure(
    report: &CardDavMultigetReport,
    operation: AccountOperation,
) -> Option<AccountError> {
    complete_failure_error(report.classify(), report.failed.len(), operation)
}

/// The same ladder for a LISTING 207 - the depth-1 PROPFIND, the snapshot poll,
/// and the `addressbook-query` candidate lane.
///
/// A listing with any committed entry never reaches here, so a member refused
/// beside members that answered stays a per-id failure on `Page::failed_ids`
/// and the page is served. Only a 207 in which every response failed, for a
/// reason other than the resource being gone, becomes an error. Twin of the
/// CalDAV function of the same name.
fn listing_failure(
    listing: &CardDavContactListing,
    operation: AccountOperation,
) -> Option<AccountError> {
    complete_failure_error(listing.classify(), listing.failed.len(), operation)
}

fn complete_failure_error(
    outcome: MultigetOutcome,
    failed: usize,
    operation: AccountOperation,
) -> Option<AccountError> {
    match outcome {
        MultigetOutcome::Usable => None,
        MultigetOutcome::CompleteFailure { status } => {
            let code = status
                .and_then(|code| StatusCode::from_u16(code).ok())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Some(status_error(
                operation,
                code,
                format!("multi-status body reported failure for all {failed} resources"),
            ))
        }
    }
}

pub(crate) fn unsupported_error(operation: AccountOperation) -> AccountError {
    bifrost_dav_core::unsupported_error(operation, DAV)
}

pub(crate) fn local_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
    bifrost_dav_core::local_error(operation, message, DAV)
}

pub(crate) fn parse_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
    bifrost_dav_core::parse_error(operation, message, DAV)
}

/// Only the account layer's tests mint one directly now; production transport
/// failures come back already classified from `DavDispatch`.
#[cfg(test)]
pub(crate) fn transport_error(
    operation: AccountOperation,
    message: impl Into<String>,
) -> AccountError {
    bifrost_dav_core::transport_error(operation, message, DAV)
}

pub(crate) fn status_error(
    operation: AccountOperation,
    status: reqwest::StatusCode,
    body: String,
) -> AccountError {
    bifrost_dav_core::status_error(operation, status, body, DAV)
}

pub(crate) fn contact_scope(id: impl Into<String>) -> ErrorScope {
    ErrorScope::Contact {
        id: (id.into()).into(),
    }
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

const PROPFIND_ADDRESSBOOK_HOME: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\">\n\
  <D:prop>\n\
    <C:addressbook-home-set/>\n\
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
<D:propfind xmlns:D=\"DAV:\" xmlns:CS=\"http://calendarserver.org/ns/\">\n\
  <D:prop>\n\
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
        dav_script_yielding, scripted_dav_net, transcripts,
    };
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_types::ReconcileAction;
    use std::sync::Arc;

    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

    /// A create-PUT must not be replayed after a mid-flight drop.
    ///
    /// `bifrost-net` derives replay safety from the METHOD, and HTTP calls PUT
    /// idempotent. That is true of an absolute-state write to a known URL and
    /// false of a create. A replayed create whose first attempt committed
    /// answers 412 -> `ConcurrencyConflict` -> `Retry(AfterStateRefresh)`, and
    /// the consumer re-creates the contact under a fresh UUID, leaving two
    /// copies.
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
                url: String::new(),
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

    /// The `If-Match` update PUT stays replayable.
    ///
    /// It addresses a known URL with absolute state, and a replay landing after
    /// the first attempt committed answers 412 - refresh-and-retry, which is
    /// already the right handling. Losing that resilience is the cost the
    /// no-replay fix above must NOT impose on the update lane.
    #[tokio::test]
    async fn an_if_match_update_put_still_replays_after_a_mid_flight_drop() {
        let mut headers = HeaderMap::new();
        headers.insert(ETAG, HeaderValue::from_static("\"v2\""));
        let script = dav_script([
            dav_dropped_after_send(),
            Canned::from(DavResponse {
                status: StatusCode::NO_CONTENT,
                headers,
                body: String::new(),
                url: String::new(),
            }),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let etag = client
            .put_vcard(
                "https://dav.example.test/book/one.vcf",
                "BEGIN:VCARD\r\nEND:VCARD\r\n".to_string(),
                PutCondition::IfMatch("v1"),
                AccountOperation::ContactUpdate,
            )
            .await
            .expect("the replay succeeds");

        assert_eq!(transcripts(&script).len(), 2, "the update PUT must replay");
        assert_eq!(etag.as_deref(), Some("v2"));
    }

    #[tokio::test]
    async fn credentials_never_reach_a_resource_href_origin() {
        // Two canned responses, so a neutered guard reaches the transport and
        // fails on the destination assertion below rather than on a starved
        // script - the failure has to name the credential leak.
        let deleted = || DavResponse {
            status: StatusCode::NO_CONTENT,
            headers: HeaderMap::new(),
            body: String::new(),
            url: String::new(),
        };
        let script = dav_script([deleted(), deleted()]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .delete_vcard(
                "https://dav.example.test/book/one.vcf",
                AccountOperation::ContactDelete,
            )
            .await
            .expect("trusted request succeeds");
        client
            .delete_vcard(
                "https://evil.test/stolen.vcf",
                AccountOperation::ContactDelete,
            )
            .await
            .expect_err("foreign resource origin is rejected");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://dav.example.test/book/one.vcf");
        assert_eq!(
            requests[0]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
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
            url: String::new(),
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
        assert_eq!(snapshot.ctag.as_deref(), Some("ctag-7"));

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
            url: String::new(),
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
        assert_eq!(card.etag.as_deref(), Some("etag-9"));

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
            url: String::new(),
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
            url: String::new(),
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
            url: String::new(),
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
        // the resource's own URL, and `bifrost_net::url::parent_collection_url`
        // answers for every id that resolves absolute, so their fallback is
        // unreachable in practice.

        assert!(
            transcripts(&script).is_empty(),
            "an unroutable call must reach no transport at all"
        );
    }

    #[tokio::test]
    async fn discovery_falls_back_to_base_after_empty_well_known_response() {
        let response = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = dav_script([
            response("<D:multistatus xmlns:D=\"DAV:\"/>"),
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>",
            ),
            response(
                "<C:addressbook-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:href>/books/ada/</D:href></C:addressbook-home-set>",
            ),
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let home = client
            .discover_addressbook_home()
            .await
            .expect("base fallback discovers home");

        assert_eq!(home, "https://dav.example.test/books/ada/");
        let urls = transcripts(&script)
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/carddav".to_string(),
                "https://dav.example.test/".to_string(),
                "https://dav.example.test/principals/ada/".to_string(),
            ]
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
            url: String::new(),
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

    /// Twin of `bifrost-caldav`'s
    /// `discovery_falls_back_to_base_after_a_well_known_index_page`. A front end
    /// answering the origin-root PROPFIND with `200` and its index page is the
    /// same deployment the empty-207 test above covers, and must reach the same
    /// place. Two things stood in the way here and BOTH are load-bearing: the
    /// probe predicate did not admit `Protocol(ParseFailed)`, and the decode ran
    /// inside the `Ok` arm and lifted its failure with `?`, so it never reached
    /// the predicate at all.
    #[tokio::test]
    async fn discovery_falls_back_to_base_after_a_well_known_index_page() {
        let script = dav_script([
            DavResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: "<html><body><p>It works!</body></html>".to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>".to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<C:addressbook-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:href>/books/ada/</D:href></C:addressbook-home-set>".to_string(),
                url: String::new(),
            },
        ]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let home = client
            .discover_addressbook_home()
            .await
            .expect("an index page at the origin root is not a discovery answer");

        assert_eq!(home, "https://dav.example.test/books/ada/");
        let urls = transcripts(&script)
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/carddav".to_string(),
                "https://dav.example.test/".to_string(),
                "https://dav.example.test/principals/ada/".to_string(),
            ]
        );
    }

    /// RFC 6764 puts well-known discovery at the origin root. A base URL
    /// carrying a path is the only input that distinguishes an
    /// origin-rooted construction from suffix concatenation, and getting
    /// it wrong is not merely a wasted request: only the crate's
    /// not-found classification falls back, so a deployment answering
    /// the bogus path with 401/403 would fail the open outright.
    #[tokio::test]
    async fn well_known_probe_is_origin_rooted_for_a_path_bearing_base() {
        let response = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = dav_script([
            response("<D:multistatus xmlns:D=\"DAV:\"/>"),
            response(
                "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>",
            ),
            response(
                "<C:addressbook-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:href>/books/ada/</D:href></C:addressbook-home-set>",
            ),
        ]);
        let client = CardDavClient::with_account_net(
            "https://dav.example.test/service",
            scripted_dav_net(&script),
        );

        let home = client
            .discover_addressbook_home()
            .await
            .expect("base fallback discovers home");

        assert_eq!(home, "https://dav.example.test/books/ada/");
        let urls = transcripts(&script)
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/carddav".to_string(),
                "https://dav.example.test/service".to_string(),
                "https://dav.example.test/principals/ada/".to_string(),
            ],
            "the well-known probe is rooted at the origin, and only the fallback uses the configured path"
        );
    }

    fn discovery_script(home_href: &str) -> Arc<ScriptedDispatch> {
        let response = |body: String| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body,
            url: String::new(),
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
        let script = discovery_script("https://books.example.test/homes/ada/");
        let mut client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let home = client
            .discover_addressbook_home()
            .await
            .expect("cross-origin home is discovered");
        assert_eq!(home, "https://books.example.test/homes/ada/");
        client.admit_discovered_urls(std::iter::once(home.clone()));
        client
            .list_addressbooks(&home)
            .await
            .expect("cross-origin home is credential-bearing");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].url, "https://books.example.test/homes/ada/");
        assert_eq!(
            requests[2]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    /// The reviewer-found hole this pins: a cross-origin hop followed inside
    /// reqwest arrives with `Authorization` stripped and cannot get it back.
    /// The manual re-dispatch must therefore carry fresh credentials to the
    /// admitted target - the assertion that bites is the auth header on the
    /// SECOND transcript entry, and the href assertion additionally keeps
    /// round 3's guarantee that resolution uses the post-redirect URI.
    #[tokio::test]
    async fn cross_origin_redirect_is_redispatched_with_credentials() {
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(
            reqwest::header::LOCATION,
            HeaderValue::from_static("https://books.example.test/dav/homes/ada/"),
        );
        let script = dav_script([
            DavResponse {
                status: StatusCode::MOVED_PERMANENTLY,
                headers: redirect_headers,
                body: String::new(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>team/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:addressbook/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
                url: String::new(),
            },
        ]);
        let mut client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        client.admit_discovered_urls(std::iter::once(
            "https://books.example.test/homes/ada/".to_string(),
        ));

        let books = client
            .list_addressbooks("https://dav.example.test/books/ada/")
            .await
            .expect("cross-origin redirect is followed with credentials");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].url, "https://books.example.test/dav/homes/ada/");
        assert_eq!(
            requests[1].method,
            Method::from_bytes(b"PROPFIND").expect("PROPFIND method")
        );
        assert_eq!(
            requests[1]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
        assert_eq!(
            books[0].href,
            "https://books.example.test/dav/homes/ada/team/"
        );
    }

    /// A redirect is server-controlled input: an origin discovery never
    /// admitted must fail locally, and no request at all may reach it.
    #[tokio::test]
    async fn redirect_to_an_unadmitted_origin_is_refused_locally() {
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(
            reqwest::header::LOCATION,
            HeaderValue::from_static("https://evil.test/dav/"),
        );
        let script = dav_script([DavResponse {
            status: StatusCode::FOUND,
            headers: redirect_headers,
            body: String::new(),
            url: String::new(),
        }]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .list_addressbooks("https://dav.example.test/books/ada/")
            .await
            .expect_err("an unadmitted redirect target is refused");

        assert_eq!(transcripts(&script).len(), 1);
    }

    #[tokio::test]
    async fn manual_redirect_walk_allows_the_configured_hop_count() {
        let max_hops = usize::from(bifrost_net::RedirectPolicy::default().max_hops);
        let mut responses = Vec::new();
        for _ in 0..max_hops {
            let mut headers = HeaderMap::new();
            headers.insert(reqwest::header::LOCATION, HeaderValue::from_static("/next"));
            responses.push(DavResponse {
                status: StatusCode::TEMPORARY_REDIRECT,
                headers,
                body: String::new(),
                url: String::new(),
            });
        }
        responses.push(DavResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: "ok".to_string(),
            url: String::new(),
        });
        let script = dav_script(responses);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .delete_vcard(
                "https://dav.example.test/start",
                AccountOperation::ContactDelete,
            )
            .await
            .expect("the configured number of redirects is allowed");

        assert_eq!(transcripts(&script).len(), max_hops + 1);
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
                url: String::new(),
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
        let script = discovery_script("http://books.example.test/homes/ada/");
        let mut client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let home = client
            .discover_addressbook_home()
            .await
            .expect("home href is still reported");
        assert_eq!(home, "http://books.example.test/homes/ada/");
        client.admit_discovered_urls(std::iter::once(home.clone()));
        client
            .list_addressbooks(&home)
            .await
            .expect_err("a downgraded discovered origin is refused");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.url.starts_with("https://dav.example.test/")),
            "no request reached the plaintext origin: {:?}",
            requests.iter().map(|r| &r.url).collect::<Vec<_>>()
        );
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
            url: String::new(),
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
                url: String::new(),
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
                url: String::new(),
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
            url: String::new(),
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

        assert!(
            matches!(
                error.kind(),
                AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
            ),
            "a copied-but-not-removed contact is a partial response: {error:?}"
        );
        // The classification, not just the kind. `ContactUpdate` is idempotent
        // in the `AccountOperation` table, so `derive_protocol` answers
        // `Retry(SameRequest)` for a `PartialResponse` unless the rebuilt error
        // re-states the unreplayable declaration - and that retry re-runs a
        // create-PUT against the destination the first attempt already
        // occupied, forever.
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
            url: String::new(),
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
            fetched.report.cards[0].uri,
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
        let chunks = 10;
        let script = dav_script_yielding((0..chunks).map(|_| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"></D:multistatus>".to_string(),
            url: String::new(),
        }));
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let uris = (0..MULTIGET_BATCH_SIZE * chunks)
            .map(|index| format!("https://dav.example.test/book/{index}.vcf"))
            .collect::<Vec<_>>();

        client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &uris,
                AccountOperation::ContactsList,
            )
            .await
            .expect("empty multistatus legs are usable");

        assert_eq!(script.peak_in_flight(), MULTIGET_LEG_CONCURRENCY);
    }

    #[tokio::test]
    async fn chunked_multiget_keeps_prior_chunk_when_later_http_leg_fails() {
        let good = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>/book/one.vcf</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:One\nEND:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/book/".to_string(),
        };
        let refused = DavResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: HeaderMap::new(),
            body: String::new(),
            url: String::new(),
        };
        // The refused leg burns the whole retry budget before it degrades.
        let script = dav_script(
            std::iter::once(Canned::from(good))
                .chain(dav_retried(refused))
                .collect::<Vec<_>>(),
        );
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let uris = (0..=MULTIGET_BATCH_SIZE)
            .map(|index| format!("https://dav.example.test/book/{index}.vcf"))
            .collect::<Vec<_>>();

        let fetched = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &uris,
                AccountOperation::ContactsList,
            )
            .await
            .expect("the usable first chunk survives");

        assert_eq!(fetched.report.cards.len(), 1);
        assert!(fetched.degraded.is_some());
    }

    /// The adjacent leg-failure path, mirroring the CalDAV twin: a later chunk
    /// whose body will not parse must degrade like a later chunk that returned
    /// 503, not discard the chunks that already materialized.
    #[tokio::test]
    async fn chunked_multiget_keeps_prior_chunk_when_later_body_is_malformed() {
        let good = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:carddav\"><D:response><D:href>/book/one.vcf</D:href><D:propstat><D:prop><C:address-data>BEGIN:VCARD\nVERSION:4.0\nFN:One\nEND:VCARD</C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/book/".to_string(),
        };
        let malformed = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"><D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/book/".to_string(),
        };
        let script = dav_script([good, malformed]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let uris = (0..=MULTIGET_BATCH_SIZE)
            .map(|index| format!("https://dav.example.test/book/{index}.vcf"))
            .collect::<Vec<_>>();

        let fetched = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &uris,
                AccountOperation::ContactsList,
            )
            .await
            .expect("the usable first chunk survives a malformed later chunk");

        assert_eq!(fetched.report.cards.len(), 1);
        assert!(fetched.degraded.is_some());
    }

    /// Nothing usable anywhere is still a failed call, malformed or not.
    #[tokio::test]
    async fn an_only_leg_that_will_not_parse_is_still_an_error() {
        let malformed = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"><D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/book/".to_string(),
        };
        let script = dav_script([malformed]);
        let client =
            CardDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let outcome = client
            .fetch_vcards(
                "https://dav.example.test/book/",
                &["https://dav.example.test/book/one.vcf".to_string()],
                AccountOperation::ContactsList,
            )
            .await
            .map(|fetched| fetched.report.cards.len());
        let Err(error) = outcome else {
            panic!("no usable result anywhere must stay an error");
        };
        assert_eq!(error.operation(), Some(AccountOperation::ContactsList));
    }

    /// Shared-shape guard against the CalDAV regression: a non-2xx REPORT must
    /// classify, never decode into an authoritative empty multiget. An empty
    /// result treated as truth is a downstream deletion.
    #[tokio::test]
    async fn unauthorized_report_classifies_as_reauthorization() {
        let script = dav_script([DavResponse {
            status: StatusCode::UNAUTHORIZED,
            headers: HeaderMap::new(),
            body: "<html><body>401 Unauthorized</body></html>".to_string(),
            url: String::new(),
        }]);
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
            url: String::new(),
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
            cards: Vec::new(),
            failed: vec![crate::parse::CardDavFailedResource {
                href: "/contacts/one.vcf".to_string(),
                status: Some(401),
            }],
            missing_data: Vec::new(),
        };

        let error = multiget_failure(&report, AccountOperation::ContactsList)
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
            cards: vec![crate::parse::CardDavFetchedVCard {
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

        assert!(multiget_failure(&report, AccountOperation::ContactsList).is_none());
    }

    fn usable_report() -> CardDavMultigetReport {
        CardDavMultigetReport {
            cards: vec![crate::parse::CardDavFetchedVCard {
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
            AccountOperation::ContactSearch,
            StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );

        let fetch = MultigetFetch::settle(usable_report(), Some(refusal))
            .expect("a partial result is not a failed call");

        assert_eq!(fetch.report.cards.len(), 1);
        assert_eq!(
            fetch.degraded.expect("the refusal survives").recovery(),
            &RecoveryClass::AuthLost
        );
    }

    #[test]
    fn a_refused_leg_with_nothing_usable_stays_an_error() {
        let refusal = status_error(
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
