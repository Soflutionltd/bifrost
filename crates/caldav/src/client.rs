use std::fmt;

pub(crate) use bifrost_dav_core::PutCondition;
use bifrost_dav_core::{
    CALDAV_NAMESPACE, DavDispatch, DavProtocol, escape_xml, filter_unsupported, prepare_if_match,
    response_etag, worse_recovery,
};
pub(crate) use bifrost_dav_core::{FilteredHrefs, HrefQuery};
use bifrost_net::{AccountId, AccountNet};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    DiagnosticText, ErrorScope, FolderId, Protocol, StateCause, SyncStateErrorKind,
};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, StatusCode};

use crate::CalDavConfig;
use crate::parse::{
    CalDavFetchedEvent, CalDavMultigetReport, CalDavSyncReport, CalendarCollection,
    MultigetOutcome, extract_href_properties_in_namespace, extract_href_property_in_namespace,
    parse_calendar_collections, parse_multiget_report, parse_propfind_events,
    parse_sync_collection_report, resolve_href,
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
/// Every shared constructor takes it, so a CalDAV error can never be stamped
/// with a CardDAV `Protocol` or name a `ResourceKind::Contact`.
const DAV: DavProtocol = DavProtocol::CalDav;

#[derive(Clone)]
pub(crate) struct CalDavClient {
    /// Transport, credentials, origin gate and the generic WebDAV verbs, all
    /// shared with `bifrost-carddav` through `bifrost-dav-core`.
    dav: DavDispatch,
}

#[derive(Debug)]
pub(crate) struct CalDavDiscovery {
    pub(crate) calendar_home: String,
    pub(crate) calendar_user_email: Option<String>,
    pub(crate) schedule_outbox_url: Option<String>,
}

impl fmt::Debug for CalDavClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CalDavClient")
            .field("dav", &self.dav)
            .finish_non_exhaustive()
    }
}

impl CalDavClient {
    /// A client view whose buffered requests feed only this stream's tally.
    pub(crate) fn metered(&self) -> (Self, bifrost_dav_core::DavByteTally) {
        let (dav, tally) = self.dav.metered();
        let mut client = self.clone();
        client.dav = dav;
        (client, tally)
    }

    pub(crate) fn resolve_url(&self, href: &str) -> String {
        self.dav.resolve_url(href)
    }

    pub(crate) fn admit_discovered_urls(&mut self, urls: impl IntoIterator<Item = String>) {
        self.dav.admit_discovered_urls(urls);
    }

    pub(crate) async fn move_resource(
        &self,
        from: &str,
        to: &str,
        operation: AccountOperation,
    ) -> Result<bool, AccountError> {
        self.dav.move_resource(from, to, operation).await
    }

    pub(crate) fn new_on_net(
        account_id: AccountId,
        config: &CalDavConfig,
        net: Option<&bifrost_net::Net>,
    ) -> Self {
        let net = net
            .cloned()
            .unwrap_or_else(bifrost_net::Net::shared_default);
        Self {
            dav: DavDispatch::on_net(
                &net,
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
                crate::CalDavCredentials::bearer("token").to_shared(),
                DAV,
            ),
        }
    }

    /// The principal walk (well-known probe, fallback predicate, configured
    /// base) is `bifrost-dav-core`'s, shared with `bifrost-carddav`; only the
    /// multi-property principal PROPFIND after it is CalDAV's own.
    pub(crate) async fn discover_account(&self) -> Result<CalDavDiscovery, AccountError> {
        let principal = self.dav.discover_current_user_principal().await?;
        self.discover_account_for_principal(&principal).await
    }

    async fn discover_account_for_principal(
        &self,
        principal: &str,
    ) -> Result<CalDavDiscovery, AccountError> {
        let response = self
            .dav
            .propfind_raw(principal, "0", PROPFIND_ACCOUNT, AccountOperation::Discover)
            .await?;
        let body = response.text;
        let base = response.url;
        let calendar_home =
            extract_href_property_in_namespace(&body, CALDAV_NAMESPACE, "calendar-home-set")
                .map_err(|error| parse_error(AccountOperation::Discover, error))?
                .map(|href| resolve_href(&base, &href))
                .ok_or_else(|| {
                    bifrost_dav_core::missing_field_error(
                        AccountOperation::Discover,
                        "missing calendar-home-set",
                        DAV,
                    )
                })?;
        let hrefs = extract_href_properties_in_namespace(
            &body,
            CALDAV_NAMESPACE,
            "calendar-user-address-set",
        )
        .map_err(|error| parse_error(AccountOperation::Discover, error))?;
        let calendar_user_email = hrefs.iter().find_map(|href| mailto_email(href));
        let schedule_outbox_url =
            extract_href_property_in_namespace(&body, CALDAV_NAMESPACE, "schedule-outbox-URL")
                .map_err(|error| parse_error(AccountOperation::Discover, error))
                .map(|href| href.map(|href| resolve_href(&base, &href)))?;
        Ok(CalDavDiscovery {
            calendar_home,
            calendar_user_email,
            schedule_outbox_url,
        })
    }

    pub(crate) async fn list_calendars(
        &self,
        home_url: &str,
    ) -> Result<Vec<CalendarCollection>, AccountError> {
        self.list_calendars_for_operation(home_url, AccountOperation::CalendarsList)
            .await
    }

    pub(crate) async fn list_calendars_for_operation(
        &self,
        home_url: &str,
        operation: AccountOperation,
    ) -> Result<Vec<CalendarCollection>, AccountError> {
        let response = self
            .dav
            .propfind_raw(home_url, "1", PROPFIND_CALENDARS, operation)
            .await?;
        let mut collections = parse_calendar_collections(&response.text)
            .map_err(|error| parse_error(operation, format!("calendar list: {error}")))?;
        for collection in &mut collections {
            collection.resolve_href(&response.url);
        }
        Ok(collections)
    }

    /// Depth-1 event PROPFIND returning both the committed entries and
    /// the hrefs the server reported *failed* within the 207, so the
    /// snapshot diff can preserve transiently-failed resources rather
    /// than destroying them (brick 7).
    pub(crate) async fn list_events_listing(
        &self,
        calendar_url: &str,
        operation: AccountOperation,
    ) -> Result<crate::parse::CalDavEventListing, AccountError> {
        let response = self
            .dav
            .propfind_raw(calendar_url, "1", PROPFIND_EVENTS, operation)
            .await?;
        let mut listing =
            parse_propfind_events(&response.text).map_err(|error| parse_error(operation, error))?;
        listing.resolve_hrefs(&response.url);
        if let Some(error) = listing_failure(&listing, operation) {
            return Err(error);
        }
        Ok(listing)
    }

    /// The event listing the CURSOR lanes take: the same href/etag listing
    /// `list_events_listing` produces, but restricted to VEVENT resources by a
    /// server-side `comp-filter`.
    ///
    /// A calendar collection may hold VTODO and VJOURNAL resources beside its
    /// events (RFC 4791 s4.2 leaves that to
    /// `supported-calendar-component-set`, which many servers never restrict).
    /// The depth-1 PROPFIND cannot tell them apart - it carries no component
    /// type - so a task resource entered the event snapshot, was emitted as a
    /// created event change, and hydrated to nothing. An empty
    /// `calendar_query_body` is exactly the "every VEVENT in this collection"
    /// filter, answers with `getetag` only, and costs the same single round trip
    /// the PROPFIND did.
    ///
    /// A server that will not run the filter degrades to the unfiltered
    /// PROPFIND, which is what this lane did unconditionally before: a narrower
    /// listing is only safe when the server actually applied the predicate, and
    /// an event dropped here would be reported as a deletion by the snapshot
    /// diff.
    pub(crate) async fn list_event_hrefs_filtered(
        &self,
        calendar_url: &str,
        operation: AccountOperation,
    ) -> Result<crate::parse::CalDavEventListing, AccountError> {
        let body = calendar_query_body(None, None);
        match self.href_query_leg(calendar_url, &body, operation).await {
            HrefLeg::Listing(listing) => Ok(listing),
            HrefLeg::FilterUnsupported => self.list_events_listing(calendar_url, operation).await,
            HrefLeg::Failed(error) => Err(error),
        }
    }

    /// Cheap depth-0 PROPFIND used by snapshot polling to refresh the
    /// collection sync token without re-listing every calendar collection.
    pub(crate) async fn collection_sync_token(
        &self,
        calendar_url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let response = self
            .dav
            .propfind_raw(calendar_url, "0", PROPFIND_SYNC_TOKEN, operation)
            .await?;
        crate::parse::parse_collection_sync_token(&response.text)
            .map_err(|error| parse_error(operation, format!("collection sync token: {error}")))
    }

    /// The time-range lane's server-side filter, asking for `getetag` only.
    ///
    /// This is what bounds `events_in_range` to one page of hydration: the
    /// REPORT answers with the matching hrefs and nothing else, so the account
    /// layer can sort, slice at the watermark, and multiget just the page.
    /// Requesting `calendar-data` here would put the whole matching result set
    /// on the wire again on every page, which is the cost dav-B8 is about.
    pub(crate) async fn query_event_hrefs_in_range(
        &self,
        calendar_url: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<FilteredHrefs, AccountError> {
        let body = calendar_query_body(start, end);
        match self
            .href_query_leg(calendar_url, &body, AccountOperation::EventsInRange)
            .await
        {
            HrefLeg::Listing(listing) => {
                let mut query = HrefQuery::default();
                extend_candidates(&mut query, listing);
                Ok(FilteredHrefs::Matched(query))
            }
            HrefLeg::FilterUnsupported => Ok(FilteredHrefs::FilterUnsupported),
            HrefLeg::Failed(error) => Err(error),
        }
    }

    /// The text-search lane's server-side prefilter, one REPORT per property,
    /// asking for `getetag` only.
    ///
    /// The union of the four legs is a PREFILTER, not the answer: the account
    /// layer still runs `event_matches` over the hydrated page, because the
    /// local match reads projected fields (and folds case with Rust's full
    /// Unicode rules) in ways `i;unicode-casemap` on four raw properties does
    /// not exactly reproduce. Widening the server side and narrowing locally is
    /// the safe direction; the reverse would silently drop matches.
    ///
    /// A leg that reports the filter unsupported degrades the WHOLE lane rather
    /// than the one property: answering out of the three properties a server
    /// happened to accept would narrow the search with no signal to the
    /// consumer.
    pub(crate) async fn query_event_hrefs_text(
        &self,
        calendar_url: &str,
        query: &str,
    ) -> Result<FilteredHrefs, AccountError> {
        let bodies = ["SUMMARY", "DESCRIPTION", "LOCATION", "ATTENDEE"]
            .map(|property| calendar_text_query_body(property, query));
        let legs: Vec<_> = bodies
            .iter()
            .map(|body| self.href_query_leg(calendar_url, body, AccountOperation::EventSearch))
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

    /// Run one filtered `calendar-query` leg, separating "the server will not
    /// run this filter" from every other way the leg can fail.
    ///
    /// The status has to be read before it becomes an error, which is why this
    /// goes through `report_raw_response` rather than `report_raw`: a 403
    /// naming `CALDAV:supported-filter` is a degrade signal, and a 403 naming
    /// nothing is still a permission refusal.
    async fn href_query_leg(
        &self,
        calendar_url: &str,
        body: &str,
        operation: AccountOperation,
    ) -> HrefLeg {
        let response = match self
            .dav
            .report_raw_response(calendar_url, "1", body, operation)
            .await
        {
            Ok(response) => response,
            Err(error) => return HrefLeg::Failed(error),
        };
        if !response.status.is_success() {
            return if filter_unsupported(response.status, &response.body) {
                HrefLeg::FilterUnsupported
            } else {
                HrefLeg::Failed(status_error(
                    &response.headers,
                    operation,
                    response.status,
                    response.body,
                ))
            };
        }
        let mut listing = match parse_propfind_events(&response.body) {
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
        // had been refused. The lane and the multiget lanes now share one
        // ladder, so a 507 or a 403 here reauthorizes or retries exactly as it
        // does on the hydration leg.
        match listing_failure(&listing, operation) {
            Some(error) => HrefLeg::Failed(error),
            None => HrefLeg::Listing(listing),
        }
    }

    pub(crate) async fn fetch_events(
        &self,
        calendar_url: &str,
        uris: &[String],
        operation: AccountOperation,
    ) -> Result<MultigetFetch, AccountError> {
        let mut all_results = CalDavMultigetReport::default();
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
<C:calendar-multiget xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
    <C:calendar-data/>\n\
  </D:prop>\n\
{href_elements}</C:calendar-multiget>"
                );
                body
            })
            .collect::<Vec<_>>();
        let legs: Vec<_> = bodies
            .iter()
            .map(|body| {
                self.run_leg(MultigetLeg {
                    url: calendar_url,
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
    async fn accumulate_leg(
        &self,
        leg: MultigetLeg<'_>,
        all_results: &mut CalDavMultigetReport,
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

    async fn run_leg(&self, leg: MultigetLeg<'_>) -> (CalDavMultigetReport, Option<AccountError>) {
        let mut report = CalDavMultigetReport::default();
        let mut degraded = None;
        self.accumulate_leg(leg, &mut report, &mut degraded).await;
        (report, degraded)
    }

    pub(crate) async fn sync_events(
        &self,
        calendar_url: &str,
        sync_token: &str,
    ) -> Result<CalDavSyncReport, AccountError> {
        let body = sync_collection_body(sync_token);
        let operation = AccountOperation::SyncChanges;
        let response = self
            .dav
            .report_raw_response(calendar_url, "0", &body, operation)
            .await?;
        let status = response.status;
        let effective_url = response.url;
        let body = response.body;
        if status == StatusCode::GONE
            || (status == StatusCode::FORBIDDEN
                && body.to_ascii_lowercase().contains("valid-sync-token"))
        {
            return Err(cursor_invalid_error(calendar_url, status, body));
        }
        if !status.is_success() {
            return Err(status_error(&response.headers, operation, status, body));
        }
        let mut report = parse_sync_collection_report(&body).map_err(|error| {
            parse_error(AccountOperation::SyncChanges, format!("sync: {error}"))
        })?;
        report.resolve_hrefs(&effective_url);
        report.take_collection_response(calendar_url, &effective_url);
        Ok(report)
    }

    pub(crate) async fn get_event(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<CalDavFetchedEvent, AccountError> {
        let request = self
            .dav
            .request(Method::GET, url)
            .replay(bifrost_net::ReplayPolicy::Idempotent);
        let response = self.dav.send_raw_request(request, operation).await?;
        let status = response.status;
        let etag = response_etag(&response.headers);
        let body = response.body;
        if status.is_success() {
            Ok(CalDavFetchedEvent {
                uri: url.to_string(),
                etag,
                data: body,
            })
        } else {
            Err(status_error(&response.headers, operation, status, body))
        }
    }

    pub(crate) async fn put_event(
        &self,
        url: &str,
        body: String,
        condition: PutCondition<'_>,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        // Every event PUT is `Never`, whatever its condition. A create-PUT
        // replayed after its first attempt committed answers 412, which reads as
        // a conflict and invites re-creating the event under a new UUID. An
        // `If-Match` update replayed the same way answers 412 against the
        // caller's own write. And an unconditional PUT is still a calendar
        // write that RFC 6638 implicit scheduling can turn into an iTIP
        // message; nothing documents that a replay cannot send a second one.
        // A drop therefore reconciles (read-back) instead of replaying, even
        // when the validator was fetched by the account itself.
        let mut request = self
            .dav
            .request(Method::PUT, url)
            .replay(bifrost_net::ReplayPolicy::Never)
            .header(CONTENT_TYPE, "text/calendar; charset=utf-8")
            .body(body);
        match condition {
            PutCondition::IfNoneMatch => {
                request = request.header("If-None-Match", "*");
            }
            PutCondition::IfMatch(etag) => {
                if let Some(etag) = prepare_if_match(etag) {
                    request = request.header("If-Match", etag);
                }
            }
            PutCondition::None => {}
        }
        let response = self.dav.send_raw_request(request, operation).await?;
        let status = response.status;
        let etag = response_etag(&response.headers);
        let body = response.body;
        if status.is_success() {
            Ok(etag)
        } else {
            Err(status_error(&response.headers, operation, status, body))
        }
    }

    /// Conditional delete never silently drops a caller's validator.
    pub(crate) async fn delete_event_if_match(
        &self,
        url: &str,
        etag: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        let etag = bifrost_types::ServerVersion::ETag(etag.to_string());
        let etag = etag.expected_etag(bifrost_types::error::Protocol::CalDav, operation, true)?;
        let etag = prepare_if_match(etag).expect("validated strong entity tag");
        let request = self
            .dav
            .request(Method::DELETE, url)
            .replay(bifrost_net::ReplayPolicy::Never)
            .header("If-Match", etag);
        self.dav.send_status_request(request, operation).await
    }

    /// `DELETE` one event resource. Replayable, and a target already gone is a
    /// success - the dav-D1 ruling, stated in full at
    /// `DavDispatch::delete_resource`; the two must move together. The
    /// copy-then-delete leg of the MOVE fallback relies on it as well: a
    /// replayed delete of a source the first attempt already removed is the
    /// move's end state, not a failed leg.
    pub(crate) async fn delete_event(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        self.dav.delete_resource(url, operation).await
    }

    pub(crate) async fn post_schedule_reply(
        &self,
        outbox_url: &str,
        originator: &str,
        recipient: &str,
        body: String,
    ) -> Result<(), AccountError> {
        // RFC 6638 outbox POSTs route iTIP via the `Originator` (the
        // replying calendar user) and `Recipient` (the organizer) headers;
        // servers reject the POST without them.
        //
        // Both go through `DavRequest::header` with the raw address rather than
        // a pre-validated `HeaderValue`, so an address the HTTP grammar cannot
        // carry - a non-ASCII calendar-user address, most plausibly - is
        // RECORDED on the request and refused by `dispatch_once` as
        // `Request(Malformed)` before any I/O. Filtering with `if let Ok(..)`
        // instead put the POST on the wire with NO routing headers, and the
        // server's inevitable 400 reached the consumer as `ProviderRefused`:
        // a local caller error dressed up as a remote refusal, and the exact
        // silently-dropped-header defect the record exists to prevent.
        let request = self
            .dav
            .request(Method::POST, outbox_url)
            .replay(bifrost_net::ReplayPolicy::Never)
            .header(CONTENT_TYPE, "text/calendar; charset=utf-8")
            .header("Originator", schedule_address(originator).as_str())
            .header("Recipient", schedule_address(recipient).as_str())
            .body(body);
        self.dav
            .send_status_request(request, AccountOperation::EventRsvp)
            .await
    }
}

fn mailto_email(href: &str) -> Option<String> {
    href.get(.."mailto:".len())
        .filter(|prefix| prefix.eq_ignore_ascii_case("mailto:"))
        .and_then(|_| href.get("mailto:".len()..))
        .filter(|email| email.contains('@'))
        .map(str::to_ascii_lowercase)
}

fn schedule_address(address: &str) -> String {
    // iTIP calendar-user addresses are URIs; bare emails (the form both
    // discovery and the organizer field carry) become `mailto:` URIs.
    if address.contains(':') {
        address.to_string()
    } else {
        format!("mailto:{address}")
    }
}

/// The prop skeleton every filtered query asks for.
///
/// Deliberately WITHOUT `calendar-data`: the filtered lanes page on the href
/// and hydrate only the page, so a query that answered with bodies would put
/// the whole matching result set on the wire once per page.
const QUERY_HREF_PROPS: &str = "  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
  </D:prop>\n";

fn calendar_query_body(start: Option<&str>, end: Option<&str>) -> String {
    let time_range = match (start, end) {
        (Some(start), Some(end)) => format!(
            "      <C:time-range start=\"{}\" end=\"{}\"/>\n",
            escape_xml(start),
            escape_xml(end)
        ),
        (Some(start), None) => format!("      <C:time-range start=\"{}\"/>\n", escape_xml(start)),
        (None, Some(end)) => format!("      <C:time-range end=\"{}\"/>\n", escape_xml(end)),
        (None, None) => String::new(),
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:calendar-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
{QUERY_HREF_PROPS}\
  <C:filter>\n\
    <C:comp-filter name=\"VCALENDAR\">\n\
      <C:comp-filter name=\"VEVENT\">\n\
{time_range}      </C:comp-filter>\n\
    </C:comp-filter>\n\
  </C:filter>\n\
</C:calendar-query>"
    )
}

fn calendar_text_query_body(property: &str, query: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<C:calendar-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
{QUERY_HREF_PROPS}\
  <C:filter>\n\
    <C:comp-filter name=\"VCALENDAR\">\n\
      <C:comp-filter name=\"VEVENT\">\n\
        <C:prop-filter name=\"{}\">\n\
          <C:text-match collation=\"i;unicode-casemap\">{}</C:text-match>\n\
        </C:prop-filter>\n\
      </C:comp-filter>\n\
    </C:comp-filter>\n\
  </C:filter>\n\
</C:calendar-query>",
        escape_xml(property),
        escape_xml(query)
    )
}

/// The `sync-collection` REPORT body. Asks for `getcontenttype` beside the
/// etag: RFC 6578 has no filter grammar, so a `component=` parameter on the
/// content type (RFC 4791 s5.2.6) is the only way this lane can learn that a
/// changed member is a task and not an event. See `parse::declared_non_vevent`.
fn sync_collection_body(sync_token: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:sync-collection xmlns:D=\"DAV:\">\n\
  <D:sync-token>{}</D:sync-token>\n\
  <D:sync-level>1</D:sync-level>\n\
  <D:prop>\n\
    <D:getetag/>\n\
    <D:getcontenttype/>\n\
  </D:prop>\n\
</D:sync-collection>",
        escape_xml(sync_token)
    )
}

fn cursor_invalid_error(calendar_url: &str, status: StatusCode, body: String) -> AccountError {
    // The error scope routes the engine's RestartScope directive. Live
    // cursors are per-calendar (`CursorScope::Folder(collection url)`), so
    // the invalidation must name the folder scope of the calendar whose
    // token went stale - a type-wide scope would restart a cursor that does
    // not exist while the invalid one fails identically on every poll.
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .protocol(Protocol::CalDav)
    .operation(AccountOperation::SyncChanges)
    .scope(ErrorScope::Cursor(CursorScope::Folder(FolderId(
        calendar_url.to_string(),
    ))))
    .status(Some(status.as_u16()));
    let body = body.trim();
    if !body.is_empty() {
        builder = builder.text(DiagnosticText::support_only(body.to_string()));
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// A multi-REPORT fetch: everything that came back usable, plus the
/// recovery classification of any single REPORT that failed wholly.
///
/// Multiget is chunked and text search runs one REPORT per property, so a
/// walk can meet a 401, 403, or 503 on one leg after other legs already
/// returned events. Aborting the whole call throws those events away;
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
    pub(crate) report: CalDavMultigetReport,
    pub(crate) degraded: Option<AccountError>,
}

/// Fold one listing's committed and refused hrefs into a candidate set.
///
/// The two lanes differ in how many listings they merge - one for a time-range
/// query, four for a text search - and in nothing else.
pub(crate) fn extend_candidates(query: &mut HrefQuery, listing: crate::parse::CalDavEventListing) {
    let failed_hrefs = listing.failed_hrefs();
    query.extend(
        listing.entries.into_iter().map(|entry| entry.uri),
        failed_hrefs,
    );
}

/// One leg of a filtered query, before the lane decides what to do with it.
enum HrefLeg {
    Listing(crate::parse::CalDavEventListing),
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
        report: CalDavMultigetReport,
        degraded: Option<AccountError>,
    ) -> Result<Self, AccountError> {
        let no_observations =
            report.events.is_empty() && report.failed.is_empty() && report.missing_data.is_empty();
        match degraded {
            Some(error) if no_observations => Err(error),
            degraded => Ok(Self { report, degraded }),
        }
    }
}

/// Keep whichever failure demands the more drastic recovery, so a 503 on
/// one chunk cannot hide a 401 on another.
fn multiget_failure(
    report: &crate::parse::CalDavMultigetReport,
    operation: AccountOperation,
) -> Option<AccountError> {
    complete_failure_error(report.classify(), report.failed.len(), operation)
}

/// The same ladder for a LISTING 207 - the depth-1 PROPFIND, the snapshot poll,
/// and the `calendar-query` candidate lane.
///
/// A listing with any committed entry never reaches here, so a member refused
/// beside members that answered stays a per-id failure on `Page::failed_ids`
/// and the page is served. Only a 207 in which every response failed, for a
/// reason other than the resource being gone, becomes an error.
fn listing_failure(
    listing: &crate::parse::CalDavEventListing,
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
                &reqwest::header::HeaderMap::new(),
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

pub(crate) fn missing_event_error(operation: AccountOperation, id: String) -> AccountError {
    bifrost_dav_core::not_found_error(operation, id, DAV)
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
    headers: &reqwest::header::HeaderMap,
    operation: AccountOperation,
    status: StatusCode,
    body: String,
) -> AccountError {
    bifrost_dav_core::status_error(headers, operation, status, body, DAV)
}

/// The scope for a failure on ONE event resource.
///
/// `ErrorScope::Calendar` carries a COLLECTION href everywhere else in this
/// crate (`reference/caldav.md`, "A CalDAV `CalendarId` IS the resolved
/// collection href"), and here it carries the event's URL. That is a deliberate
/// overload, ruled dav-F8 on 2026-09-07: `bifrost-types` has no
/// `ErrorScope::Event`, adding a public variant to a `#[non_exhaustive]` enum
/// every consumer matches on was judged not worth it for a diagnostic, and the
/// single-resource paths (`fetch_event_from_url`, the dropped create in
/// `event_create`) need SOME scope so a `Reconcile(CheckTarget)` names a
/// checkable target. A consumer routing on this scope must not assume the id
/// is a collection; the URL's final segment is the resource. What would reopen
/// it: an engine-side use of the scope that has to tell the two apart.
pub(crate) fn event_scope(id: impl Into<String>) -> ErrorScope {
    ErrorScope::Calendar {
        id: (id.into()).into(),
    }
}

const PROPFIND_ACCOUNT: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:prop>\n\
    <C:calendar-home-set/>\n\
    <C:calendar-user-address-set/>\n\
    <C:schedule-outbox-URL/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_CALENDARS: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\" xmlns:A=\"http://apple.com/ns/ical/\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:displayname/>\n\
    <A:calendar-color/>\n\
    <D:current-user-privilege-set/>\n\
    <D:sync-token/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_EVENTS: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\">\n\
  <D:prop>\n\
    <D:resourcetype/>\n\
    <D:getetag/>\n\
    <D:getcontenttype/>\n\
  </D:prop>\n\
</D:propfind>";

const PROPFIND_SYNC_TOKEN: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n\
<D:propfind xmlns:D=\"DAV:\">\n\
  <D:prop>\n\
    <D:sync-token/>\n\
  </D:prop>\n\
</D:propfind>";

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use bifrost_dav_core::DavResponse;
    use bifrost_dav_core::test_support::{
        dav_dropped_after_send, dav_redirect, dav_retried, dav_script, dav_script_empty,
        dav_script_yielding, scripted_dav_net, transcripts,
    };
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_types::{
        ProtocolErrorKind, ReconcileAction, RecoveryClass, RequestErrorKind, ResourceKind,
        ServerErrorKind,
    };
    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

    /// Every error this crate mints is stamped CalDAV, and names calendars.
    ///
    /// Twin of `bifrost-carddav`'s `every_error_this_crate_mints_is_stamped_carddav`;
    /// keep them in step. The shared ladder in `bifrost-dav-core` is
    /// parameterized by `DAV`, so a wrong binding here would silently restamp
    /// the whole crate's error surface as CardDAV.
    #[test]
    fn every_error_this_crate_mints_is_stamped_caldav() {
        let errors = [
            status_error(
                &reqwest::header::HeaderMap::new(),
                AccountOperation::EventGet,
                StatusCode::NOT_FOUND,
                String::new(),
            ),
            local_error(AccountOperation::EventGet, "bad"),
            parse_error(AccountOperation::EventGet, "bad"),
            transport_error(AccountOperation::EventGet, "bad"),
            unsupported_error(AccountOperation::EventGet),
            missing_event_error(AccountOperation::EventGet, "one.ics".to_string()),
        ];
        for error in errors {
            assert_eq!(
                error.protocol(),
                Some(Protocol::CalDav),
                "a CalDAV error must not be stamped otherwise: {error:?}"
            );
        }
        let missing = status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::EventGet,
            StatusCode::NOT_FOUND,
            String::new(),
        );
        assert!(
            matches!(
                missing.kind(),
                AccountErrorKind::NotFound(ResourceKind::Calendar)
            ),
            "a CalDAV 404 names a calendar, not a contact: {missing:?}"
        );
    }

    #[tokio::test]
    async fn unenforceable_event_versions_are_refused_before_io() {
        use bifrost_types::account::Account as _;

        for (version, moving) in [
            (
                bifrost_types::ServerVersion::ETag("W/\"weak\"".into()),
                false,
            ),
            (bifrost_types::ServerVersion::ModSeq(1), false),
            (bifrost_types::ServerVersion::ETag("strong".into()), true),
        ] {
            let script = dav_script_empty();
            let client = Arc::new(CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            ));
            let account =
                crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/book/");
            let error = account
                .event_update(
                    bifrost_types::EventId("https://dav.example.test/book/one".into()),
                    bifrost_types::EventPatch {
                        expected_version: Some(version),
                        calendar_id: moving.then(|| {
                            bifrost_types::CalendarId("https://dav.example.test/other/".into())
                        }),
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(
                *error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventUpdate)
            );
            assert!(transcripts(&script).is_empty());
        }
    }

    #[tokio::test]
    async fn caller_event_version_controls_the_put() {
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
                    body: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:One\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n".to_string(),
                    url: String::new(),
                },
                DavResponse {
                    status: if expected.is_some() { StatusCode::PRECONDITION_FAILED } else { StatusCode::NO_CONTENT },
                    headers: HeaderMap::new(),
                    body: String::new(),
                    url: String::new(),
                },
            ]);
            let client = Arc::new(CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            ));
            let account =
                crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/book/");
            let result = account
                .event_update(
                    bifrost_types::EventId("https://dav.example.test/book/one.ics".into()),
                    bifrost_types::EventPatch {
                        expected_version: expected
                            .map(|etag| bifrost_types::ServerVersion::ETag(etag.into())),
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
    async fn caller_event_delete_refuses_weak_and_foreign_versions_before_io() {
        use bifrost_types::Account;

        for version in [
            bifrost_types::ServerVersion::ETag("W/\"weak\"".into()),
            bifrost_types::ServerVersion::ModSeq(42),
            bifrost_types::ServerVersion::Unavailable,
        ] {
            let script = dav_script_empty();
            let client = Arc::new(CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            ));
            let account =
                crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/book/");
            let error = account
                .event_delete_if_version(
                    bifrost_types::EventId("https://dav.example.test/book/one.ics".into()),
                    Some(version),
                )
                .await
                .unwrap_err();
            assert_eq!(
                *error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::EventDelete)
            );
            assert!(transcripts(&script).is_empty());
        }
    }

    #[tokio::test]
    async fn caller_event_delete_version_is_conditional_and_absence_stays_unconditional() {
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
                url: String::new(),
            }]);
            let client = Arc::new(CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            ));
            let account =
                crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/book/");
            let result = account
                .event_delete_if_version(
                    bifrost_types::EventId("https://dav.example.test/book/one.ics".into()),
                    expected.map(|value| bifrost_types::ServerVersion::ETag(value.into())),
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
    /// Twin of `bifrost-carddav`'s
    /// `a_create_put_is_not_replayed_after_a_mid_flight_drop`; keep them in
    /// step. HTTP calls PUT idempotent - true of an absolute-state write to a
    /// known URL, false of a create, which is why the create-PUT is declared
    /// `ReplayPolicy::Never` (and why net's PUT default refuses replay once
    /// `If-None-Match` is present). A replayed create whose first attempt
    /// committed answers 412 ->
    /// `ConcurrencyConflict` -> `Retry(AfterStateRefresh)`, and `event_create`
    /// mints a fresh UUID on the retry, so the calendar ends up holding two
    /// copies of one event.
    ///
    /// One drop is scripted, so a request that IS replayed exhausts the script
    /// and panics rather than passing quietly.
    #[tokio::test]
    async fn a_create_put_is_not_replayed_after_a_mid_flight_drop() {
        let script = dav_script([dav_dropped_after_send()]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let error = client
            .put_event(
                "https://dav.example.test/cal/new.ics",
                "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n".to_string(),
                PutCondition::IfNoneMatch,
                AccountOperation::EventCreate,
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
    /// landed DELETE answers is a success - the dav-D1 ruling, at
    /// `DavDispatch::delete_resource`. Twin of `bifrost-carddav`'s test of the
    /// same name; keep them in step. Ablation: declaring the DELETE
    /// unreplayable starves the first case on its script; dropping the
    /// gone-target arm fails the second with `NotFound`.
    #[tokio::test]
    async fn a_dropped_delete_replays_and_a_gone_target_is_a_success() {
        let answer = |status: StatusCode| DavResponse {
            status,
            headers: HeaderMap::new(),
            body: String::new(),
            url: String::new(),
        };
        for (script, requests, label) in [
            (
                dav_script([
                    dav_dropped_after_send(),
                    answer(StatusCode::NO_CONTENT).into(),
                ]),
                2,
                "a drop before the server saw the DELETE replays and lands",
            ),
            (
                dav_script([
                    dav_dropped_after_send(),
                    answer(StatusCode::NOT_FOUND).into(),
                ]),
                2,
                "a drop after the DELETE landed replays to a 404 that reads as done",
            ),
        ] {
            let client = CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            );
            client
                .delete_event(
                    "https://dav.example.test/cal/one.ics",
                    AccountOperation::EventDelete,
                )
                .await
                .unwrap_or_else(|error| panic!("{label}: {error:?}"));
            assert_eq!(transcripts(&script).len(), requests, "{label}");
        }
    }

    /// A dropped create names the target the consumer is told to check.
    ///
    /// The URL and the UID are minted locally in `event_create`, so nothing on
    /// the wire and nothing in the status ladder knows them. Without the scope,
    /// the `Reconcile(CheckTarget)` that the unreplayable declaration buys is
    /// unactionable - a consumer that cannot identify the target can only
    /// re-create, which is the duplicate the declaration exists to prevent.
    #[tokio::test]
    async fn a_dropped_create_names_the_target_to_reconcile_against() {
        use bifrost_types::account::Account as _;

        let script = dav_script([dav_dropped_after_send()]);
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/work/");

        let error = account
            .event_create(bifrost_types::EventCreate {
                calendar_id: bifrost_types::CalendarId(
                    "https://dav.example.test/cal/work/".to_string(),
                ),
                title: Some("one".to_string()),
                description: None,
                location: None,
                start: bifrost_types::EventTime {
                    value: "2026-06-02T09:00:00Z".to_string(),
                    timezone: None,
                },
                end: bifrost_types::EventTime {
                    value: "2026-06-02T10:00:00Z".to_string(),
                    timezone: None,
                },
                is_all_day: false,
                status: bifrost_types::EventStatus::Confirmed,
                availability: bifrost_types::EventAvailability::Busy,
                visibility: bifrost_types::EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: bifrost_types::EventRecurrence::default(),
            })
            .await
            .expect_err("a dropped create must fail");

        let Some(bifrost_types::ErrorScope::Calendar { id }) = error.scope() else {
            panic!("a dropped create must name its minted target: {error:?}");
        };
        assert!(
            id.0.starts_with("https://dav.example.test/cal/work/") && id.0.ends_with(".ics"),
            "the scope must carry the minted resource URL: {id:?}"
        );
    }

    /// The `MOVE` leg of a relocate must not be replayed either.
    ///
    /// Twin of `bifrost-carddav`'s
    /// `a_dropped_relocate_move_reconciles_rather_than_replaying`; keep them in
    /// step. A relocate runs under `EventUpdate`, which the `AccountOperation`
    /// table calls idempotent, so a mid-flight drop on the MOVE would derive
    /// `Retry(SameRequest)`: the engine re-issues the update, `event_update`
    /// GETs the source URL the committed MOVE already emptied, and the answer
    /// is `NotFound` -> `ProviderRefused` for a move that SUCCEEDED. The
    /// `move_resource` request declares itself unreplayable so the drop
    /// reconciles against the destination instead.
    #[tokio::test]
    async fn a_dropped_relocate_move_reconciles_rather_than_replaying() {
        let script = dav_script([dav_dropped_after_send()]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let error = client
            .move_resource(
                "https://dav.example.test/cal/a/one.ics",
                "https://dav.example.test/cal/b/one.ics",
                AccountOperation::EventUpdate,
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
                url: String::new(),
            }
            .into(),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let error = client
            .delete_event_if_match(
                "https://dav.example.test/book/one.ics",
                "v1",
                AccountOperation::EventDelete,
            )
            .await
            .unwrap_err();
        assert_eq!(transcripts(&script).len(), 1);
        assert!(
            matches!(error.recovery(), RecoveryClass::Reconcile(a) if a.guidance.actions.contains(&ReconcileAction::CheckTarget))
        );
    }

    #[tokio::test]
    async fn an_unconditional_event_put_is_not_replayed_after_a_mid_flight_drop() {
        let script = dav_script([
            dav_dropped_after_send(),
            DavResponse {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            }
            .into(),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let result = client
            .put_event(
                "https://dav.example.test/book/one.ics",
                String::new(),
                PutCondition::None,
                AccountOperation::EventUpdate,
            )
            .await;
        assert_eq!(transcripts(&script).len(), 1);
        assert!(result.unwrap_err().recovery().requires_reconciliation());
    }

    #[tokio::test]
    async fn an_if_match_update_put_reconciles_after_a_mid_flight_drop() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::ETAG,
            reqwest::header::HeaderValue::from_static("\"v2\""),
        );
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
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let error = client
            .put_event(
                "https://dav.example.test/cal/one.ics",
                "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n".to_string(),
                PutCondition::IfMatch("v1"),
                AccountOperation::EventUpdate,
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

    /// A recurrence-instance `EventId` is refused before anything is sent by the
    /// two doors that act on the whole resource.
    ///
    /// `events_from_ical` mints `"{uri}#{recurrence_id}"` for override VEVENTs,
    /// and a URL fragment never goes on the wire - so unguarded, every one of
    /// these ids addressed the master resource. `event_delete` was the severe
    /// case: deleting one occurrence DELETEd the whole `.ics` and destroyed the
    /// entire series. (`event_get` and `event_update` resolve the qualified id
    /// instead; see `a_listed_override_id_reads_and_updates_the_vevent_it_named`.)
    ///
    /// The script is deliberately EMPTY. Any request at all starves it and
    /// panics, so this fails loudly if a guard is removed rather than quietly
    /// asserting on an error some other layer produced.
    #[tokio::test]
    async fn recurrence_instance_ids_are_refused_before_reaching_the_wire() {
        use bifrost_types::account::Account as _;

        let script = dav_script_empty();
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/calendar/");
        let instance = bifrost_types::EventId(
            "https://dav.example.test/calendar/series.ics#20260609T120000Z".to_string(),
        );

        account
            .event_delete(instance.clone())
            .await
            .expect_err("deleting one occurrence must not delete the series");
        account
            .event_rsvp(instance, bifrost_types::RsvpStatus::Accepted)
            .await
            .expect_err("answering for one occurrence must not answer for the series");

        assert!(
            transcripts(&script).is_empty(),
            "a refused instance id must reach no transport at all"
        );
    }

    /// An unwritable consumer value is `Request(Malformed)` naming its field.
    /// A create sends nothing (the script is empty, so any request starves it),
    /// and an update stops before the PUT (the script holds only the GET).
    /// Reverting `plain_value` to a pass-through writes the injected line and
    /// the create reaches the starved script instead of refusing.
    #[tokio::test]
    async fn unwritable_event_values_are_malformed_before_any_write() {
        use bifrost_types::account::Account as _;

        let account_over = |script: &Arc<_>| {
            let client = Arc::new(CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(script),
            ));
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/calendar/")
        };
        let create = |mutate: fn(&mut bifrost_types::EventCreate)| {
            let mut event = bifrost_types::EventCreate {
                calendar_id: bifrost_types::CalendarId(
                    "https://dav.example.test/calendar/".to_string(),
                ),
                title: Some("one".to_string()),
                description: None,
                location: None,
                start: bifrost_types::EventTime {
                    value: "2026-06-02T09:00:00Z".to_string(),
                    timezone: None,
                },
                end: bifrost_types::EventTime {
                    value: "2026-06-02T10:00:00Z".to_string(),
                    timezone: None,
                },
                is_all_day: false,
                status: bifrost_types::EventStatus::Confirmed,
                availability: bifrost_types::EventAvailability::Busy,
                visibility: bifrost_types::EventVisibility::Default,
                organizer: None,
                attendees: Vec::new(),
                recurrence: bifrost_types::EventRecurrence::default(),
            };
            mutate(&mut event);
            event
        };

        let script = dav_script_empty();
        let account = account_over(&script);
        for event in [
            create(|event| {
                event.recurrence.rrule = Some("FREQ=DAILY\r\nATTENDEE:mailto:x@y".to_string());
            }),
            create(|event| event.start.value = String::new()),
        ] {
            let error = account
                .event_create(event)
                .await
                .expect_err("an unwritable event must be refused");
            assert_eq!(
                error.kind(),
                &AccountErrorKind::Request(RequestErrorKind::Malformed),
                "{error:?}"
            );
        }
        assert!(transcripts(&script).is_empty());

        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART:20260602T120000Z\r\n\
                   DTEND:20260602T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let script = dav_script([DavResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: ics.to_string(),
            url: String::new(),
        }]);
        let account = account_over(&script);
        let error = account
            .event_update(
                bifrost_types::EventId("https://dav.example.test/calendar/one.ics".to_string()),
                bifrost_types::EventPatch {
                    recurrence: Some(bifrost_types::EventRecurrence {
                        rrule: Some("FREQ=DAILY\nX-EVIL:1".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect_err("an injected RRULE must be refused");
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed),
            "{error:?}"
        );
        let requests = transcripts(&script);
        assert_eq!(requests.len(), 1, "only the GET went out");
        assert_eq!(requests[0].method.as_str(), "GET");
    }

    /// An id the listing lanes minted for an override reads and updates the
    /// VEVENT it named, over a resource that holds only overrides.
    ///
    /// The wire address is the resource alone (no fragment), and the PUT body
    /// changes the named VEVENT and no other. Reverting `split_event_id` in
    /// `fetch_event_from_url` (passing `None` as the instance) reads "First"
    /// for the second id; reverting it in `event_update` PUTs to a fragment URL.
    /// The bare id over several overrides is refused rather than answered with
    /// the first, and a move on behalf of one override is refused before any
    /// I/O (the empty script starves if the guard is removed).
    #[tokio::test]
    async fn a_listed_override_id_reads_and_updates_the_vevent_it_named() {
        use bifrost_types::account::Account as _;

        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260609T120000Z\r\n\
                   SUMMARY:First\r\nDTSTART:20260609T140000Z\r\nDTEND:20260609T150000Z\r\n\
                   END:VEVENT\r\nBEGIN:VEVENT\r\nUID:u1\r\nRECURRENCE-ID:20260616T120000Z\r\n\
                   SUMMARY:Second\r\nDTSTART:20260616T140000Z\r\nDTEND:20260616T150000Z\r\n\
                   END:VEVENT\r\nEND:VCALENDAR\r\n";
        let ok = |status: StatusCode, body: &str| DavResponse {
            status,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let second = bifrost_types::EventId(
            "https://dav.example.test/calendar/series.ics#20260616T120000Z".to_string(),
        );
        let account_over = |script: &Arc<_>| {
            let client = Arc::new(CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(script),
            ));
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/calendar/")
        };

        let script = dav_script([
            ok(StatusCode::OK, ics),
            ok(StatusCode::OK, ics),
            ok(StatusCode::NO_CONTENT, ""),
        ]);
        let account = account_over(&script);
        let read = account.event_get(second.clone()).await.expect("resolves");
        assert_eq!(read.title.as_deref(), Some("Second"));
        assert_eq!(
            read.id, second,
            "the id read back is the id the listing gave"
        );

        let patch = bifrost_types::EventPatch {
            title: Some(Some("Edited".to_string())),
            ..Default::default()
        };
        account
            .event_update(second.clone(), patch)
            .await
            .expect("an override is updated in place");
        let requests = transcripts(&script);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].method.as_str(), "PUT");
        assert_eq!(
            requests[2].url, "https://dav.example.test/calendar/series.ics",
            "the fragment never reaches the wire"
        );
        assert!(requests[2].body.contains("SUMMARY:First\r\n"));
        assert!(requests[2].body.contains("SUMMARY:Edited\r\n"));
        assert!(!requests[2].body.contains("SUMMARY:Second"));

        // The bare id names none of two overrides.
        let script = dav_script([ok(StatusCode::OK, ics)]);
        let account = account_over(&script);
        account
            .event_get(bifrost_types::EventId(
                "https://dav.example.test/calendar/series.ics".to_string(),
            ))
            .await
            .expect_err("the bare id must not answer with the first override");
        // An instance the resource no longer carries is absent.
        let script = dav_script([ok(StatusCode::OK, ics)]);
        let account = account_over(&script);
        account
            .event_get(bifrost_types::EventId(
                "https://dav.example.test/calendar/series.ics#20990101T000000Z".to_string(),
            ))
            .await
            .expect_err("an unknown instance is not the first override");

        // A move relocates the whole resource, so it is refused before I/O.
        let script = dav_script_empty();
        let account = account_over(&script);
        let moving = bifrost_types::EventPatch {
            calendar_id: Some(bifrost_types::CalendarId(
                "https://dav.example.test/other/".to_string(),
            )),
            ..Default::default()
        };
        account
            .event_update(second, moving)
            .await
            .expect_err("one override cannot be moved out of its series");
        assert!(transcripts(&script).is_empty());
    }

    /// An event resource the SERVER sent that will not tokenize is the
    /// provider's malformed response, on both single-resource doors that project
    /// it.
    ///
    /// Against the code before the fix both calls fail the kind assertion: the
    /// projection failure went through `local_error`, which is
    /// `Request(Malformed)` -> `ClientBug` - the caller told to fix a request it
    /// had nothing to do with. One response is scripted per call, so an update
    /// that went on to PUT would starve the script and panic.
    #[tokio::test]
    async fn an_unparseable_event_from_the_server_is_the_providers_fault() {
        use bifrost_types::account::Account as _;

        const MALFORMED: &str = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nDTSTART;TZID=\"unterminated:20260602T120000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let url = "https://dav.example.test/calendar/one.ics";
        let malformed = || DavResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: MALFORMED.to_string(),
            url: String::new(),
        };
        let script = dav_script([malformed(), malformed()]);
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/calendar/");

        let got = account
            .event_get(bifrost_types::EventId(url.to_string()))
            .await
            .expect_err("a body that will not tokenize cannot project");
        let updated = account
            .event_update(
                bifrost_types::EventId(url.to_string()),
                bifrost_types::EventPatch::default(),
            )
            .await
            .expect_err("an update cannot patch a body that will not tokenize");

        for (door, error) in [("event_get", got), ("event_update", updated)] {
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
                matches!(
                    error.scope(),
                    Some(ErrorScope::Calendar { id }) if id.0 == url
                ),
                "{door}: scoped to the event like the GET failure: {:?}",
                error.scope()
            );
            assert!(
                error.chain().iter().any(|cause| matches!(
                    cause,
                    Cause::Attempt(attempt)
                        if attempt.transmission_state
                            == bifrost_types::TransmissionState::Acknowledged
                )),
                "{door}: the server answered the GET"
            );
        }
        assert_eq!(transcripts(&script).len(), 2, "one GET per door, no PUT");
    }

    /// A GET that returns a resource holding no VEVENT (a VTODO sharing the
    /// collection) is `NotFound(Calendar)` read out of the SERVER's answer, so
    /// it carries an acknowledged attempt - on both doors that project it.
    ///
    /// Against the code before the stamp the evidence assertion fails:
    /// `bifrost_dav_core::not_found_error` pushed no attempt, so the error read
    /// as `Unsent`, while CardDAV's `not_found_error` - built on `status_error`
    /// - already stamped `Acknowledged` for the same kind of answer. One
    /// response per call, so an update that went on to PUT starves the script.
    #[tokio::test]
    async fn a_resource_with_no_vevent_is_a_not_found_the_server_answered() {
        use bifrost_types::account::Account as _;

        const TASK: &str = "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:t1\r\nSUMMARY:Task\r\nEND:VTODO\r\nEND:VCALENDAR\r\n";
        let url = "https://dav.example.test/calendar/task.ics";
        let task = || DavResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: TASK.to_string(),
            url: String::new(),
        };
        let script = dav_script([task(), task()]);
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/calendar/");

        let got = account
            .event_get(bifrost_types::EventId(url.to_string()))
            .await
            .expect_err("a task is not an event");
        let updated = account
            .event_update(
                bifrost_types::EventId(url.to_string()),
                bifrost_types::EventPatch::default(),
            )
            .await
            .expect_err("a task cannot be updated as an event");

        for (door, error) in [("event_get", got), ("event_update", updated)] {
            assert_eq!(
                error.kind(),
                &AccountErrorKind::NotFound(ResourceKind::Calendar),
                "{door}"
            );
            assert_eq!(error.recovery(), &RecoveryClass::ProviderRefused, "{door}");
            // Scoped to the event like the GET failure beside it; it carried
            // the id only inside its cause.
            assert!(
                matches!(
                    error.scope(),
                    Some(ErrorScope::Calendar { id }) if id.0 == url
                ),
                "{door}: {:?}",
                error.scope()
            );
            assert!(
                error.chain().iter().any(|cause| matches!(
                    cause,
                    Cause::Attempt(attempt)
                        if attempt.transmission_state
                            == bifrost_types::TransmissionState::Acknowledged
                )),
                "{door}: the server answered the GET: {error:?}"
            );
        }
        assert_eq!(transcripts(&script).len(), 2, "one GET per door, no PUT");
    }

    /// An event whose `END:VEVENT` delimiter is folded - legal, RFC 5545 s3.1
    /// lets a fold fall anywhere - is updated, not refused.
    ///
    /// The projection unfolds and read the event fine; the splice read only
    /// the first physical line, found no landing point, and `event_update`
    /// answered `Unsupported` for an ordinary title edit. Against that code this
    /// fails on the update itself. The PUT body is asserted, so a write-back
    /// that dropped the patch would fail too.
    #[tokio::test]
    async fn an_event_with_a_folded_delimiter_is_updated() {
        use bifrost_types::account::Account as _;

        const FOLDED: &str = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nEND:VEV\r\n ENT\r\nEND:VCALENDAR\r\n";
        let url = "https://dav.example.test/calendar/one.ics";
        let script = dav_script([
            DavResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: FOLDED.to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/calendar/");

        account
            .event_update(
                bifrost_types::EventId(url.to_string()),
                bifrost_types::EventPatch {
                    title: Some(Some("New".to_string())),
                    ..bifrost_types::EventPatch::default()
                },
            )
            .await
            .expect("a legal folded body is updated");

        let sent = transcripts(&script);
        assert_eq!(sent.len(), 2, "the GET, then the PUT");
        assert_eq!(sent[1].method, Method::PUT);
        let body = &sent[1].body;
        assert!(body.contains("SUMMARY:New\r\n"), "{body}");
        assert!(!body.contains("SUMMARY:Old"), "{body}");
    }

    /// A body the projection reads is a body the writer can patch, even where
    /// the body bends the grammar. The shape here is a blank line inside the
    /// fold of `END:VEVENT`: RFC 5545 permits no empty content line, and
    /// caldata's `LineReader` skips it and unfolds the delimiter, so the event
    /// projects. The writer re-read the body with its own line grouping, took
    /// the blank line for a line of its own, never found the end of the event,
    /// and refused the update; it now splices by the projection's own parse.
    ///
    /// Against that code this fails on the update itself. The PUT body is
    /// asserted, including the preserved blank-line fold, so a write-back that
    /// dropped the patch or mangled the delimiter fails too.
    #[tokio::test]
    async fn an_event_with_a_blank_line_inside_a_fold_is_updated() {
        use bifrost_types::account::Account as _;

        const BLANK_IN_FOLD: &str = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:Old\r\nDTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\nEND:VEV\r\n\r\n ENT\r\nEND:VCALENDAR\r\n";
        let url = "https://dav.example.test/calendar/one.ics";
        let script = dav_script([
            DavResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: BLANK_IN_FOLD.to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/calendar/");

        account
            .event_update(
                bifrost_types::EventId(url.to_string()),
                bifrost_types::EventPatch {
                    title: Some(Some("New".to_string())),
                    ..bifrost_types::EventPatch::default()
                },
            )
            .await
            .expect("a body the projection reads is a body the writer patches");

        let sent = transcripts(&script);
        assert_eq!(sent.len(), 2, "the GET, then the PUT");
        assert_eq!(sent[1].method, Method::PUT);
        let body = &sent[1].body;
        assert!(
            body.contains("SUMMARY:New\r\nEND:VEV\r\n\r\n ENT\r\n"),
            "the patch lands before the preserved delimiter: {body}"
        );
        assert!(!body.contains("SUMMARY:Old"), "{body}");
    }

    /// A discovery that enumerates no calendars leaves the OPENED account with
    /// no default, rather than the calendar home standing in for one.
    ///
    /// Drives the real discovery-to-account path, so it bites where a pure test
    /// of the selection helper cannot: reintroducing the old
    /// `unwrap_or_else(|| resolve_url(&home))` at the `open` call site is
    /// invisible to a test that constructs the `None` itself.
    #[tokio::test]
    async fn an_empty_discovery_opens_an_account_with_no_default_calendar() {
        let script = discovery_script("/cal/ada/");
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let account = crate::account::CalDavAccount::open_with_client(client, None)
            .await
            .expect("discovery succeeds against an empty backend");

        assert_eq!(
            account.default_calendar_url, None,
            "an empty calendar home must leave no default, not the home itself"
        );
        assert!(
            account.calendar_urls.is_empty(),
            "an empty calendar home advertises no collections"
        );
    }

    /// An empty backend refuses a collection-less call locally instead of
    /// addressing the calendar home.
    ///
    /// The home is not a collection when the walk came back empty
    /// (`list_calendars` returns the home itself when it genuinely is one), so
    /// the old `unwrap_or_else(|| resolve_url(&home))` default sent every one
    /// of these to a resource a spec-correct server 404s - reporting a local
    /// routing failure as a remote `NotFound`. The empty script is the bite:
    /// restore the fallback and these calls reach the transport and panic on
    /// exhaustion rather than failing quietly.
    #[tokio::test]
    async fn an_empty_backend_refuses_collection_less_calls_before_the_wire() {
        use bifrost_types::account::Account as _;

        let script = dav_script_empty();
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account = crate::account::CalDavAccount::for_tests_without_collections(
            client,
            "https://dav.example.test/cal/",
        );

        account
            .event_search(bifrost_types::EventSearchRequest::new("standup"))
            .await
            .expect_err("a search naming no calendar has nothing to search");
        // Only the doors taking an `Option<CalendarId>` route through the
        // default. `event_get` / `event_update` / `event_rsvp` derive the
        // collection from the resource's own URL, and
        // `bifrost_net::url::parent_collection_url` answers for every id that
        // resolves absolute, so their fallback is unreachable in practice.

        let mut scopes = account.discover_cursor_scopes();
        let mut discovered = Vec::new();
        while let Some(event) = futures::StreamExt::next(&mut scopes).await {
            if let bifrost_types::SyncEvent::Batch(batch) = event {
                discovered.extend(batch.items);
            }
        }
        assert!(
            discovered.is_empty(),
            "an empty backend advertises no cursor scope, so none can be established"
        );

        assert!(
            transcripts(&script).is_empty(),
            "an unroutable call must reach no transport at all"
        );
    }

    /// Every discovered calendar gets an independent cursor scope.
    #[tokio::test]
    async fn every_calendar_is_discovered_as_a_cursor_scope() {
        use bifrost_types::account::Account as _;
        use futures::StreamExt as _;
        let script = dav_script_empty();
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));

        let many = crate::account::CalDavAccount::for_tests_with_collections(
            client,
            "https://dav.example.test/cal/work/",
            vec![
                "https://dav.example.test/cal/personal/".to_string(),
                "https://dav.example.test/cal/holidays/".to_string(),
            ],
        );
        let mut stream = many.discover_cursor_scopes();
        let bifrost_types::SyncEvent::Batch(batch) = stream.next().await.expect("scope batch")
        else {
            panic!("expected scope batch");
        };
        assert_eq!(batch.items.len(), 3);
        assert!(
            batch
                .items
                .iter()
                .all(|scope| matches!(scope, CursorScope::Folder(_)))
        );
    }

    #[tokio::test]
    async fn unavailable_rsvp_is_rejected_before_reaching_the_wire() {
        use bifrost_types::account::Account as _;

        let script = dav_script_empty();
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/work/");

        account
            .event_rsvp(
                bifrost_types::EventId("https://dav.example.test/cal/work/one.ics".to_string()),
                bifrost_types::RsvpStatus::Accepted,
            )
            .await
            .expect_err("scheduling-less accounts cannot RSVP");
        assert!(transcripts(&script).is_empty());
    }

    /// A calendar-user address the HTTP grammar cannot carry refuses LOCALLY.
    ///
    /// The RFC 6638 `Originator` / `Recipient` headers used to be filtered
    /// through `if let Ok(HeaderValue::from_str(..))`, so a non-ASCII address
    /// put the outbox POST on the wire carrying NEITHER routing header, and the
    /// server's 400 reached the consumer as `ProviderRefused` - a caller error
    /// reported as a remote refusal. The script is EMPTY, so a regression that
    /// sends the POST anyway starves it rather than failing quietly.
    #[tokio::test]
    async fn a_non_ascii_schedule_address_fails_before_the_wire() {
        let script = dav_script_empty();
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let error = client
            .post_schedule_reply(
                "https://dav.example.test/cal/ada/outbox/",
                "adá@example.test",
                "organizer@example.test",
                "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n".to_string(),
            )
            .await
            .expect_err("an address no header can carry must refuse locally");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed),
            "a header the request could not carry is a local caller error"
        );
        assert_eq!(*error.recovery(), RecoveryClass::ClientBug);
        assert!(
            transcripts(&script).is_empty(),
            "no POST may go out missing the headers RFC 6638 routes on"
        );
    }

    /// An empty calendar home lists NOTHING - no fabricated placeholder.
    ///
    /// This crate removed its phantom home-calendar long ago but never pinned
    /// the absence, which is exactly how bifrost-carddav kept an identical
    /// phantom for as long as it did. The twin is
    /// `an_empty_home_lists_no_address_books_rather_than_a_phantom`; keep the
    /// pair in step.
    #[tokio::test]
    async fn an_empty_home_lists_no_calendars_rather_than_a_phantom() {
        use bifrost_types::account::Account as _;

        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"/>".to_string(),
            url: String::new(),
        }]);
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/ada/");

        let calendars = account.calendars_list().await.expect("list");
        assert!(
            calendars.is_empty(),
            "an empty home must not fabricate a calendar: {calendars:?}"
        );
    }

    /// A server without MOVE falls back to copy-then-delete, and a failure of
    /// the delete leg reports the move as HALF applied.
    ///
    /// The order is the recoverable one: a failed copy leaves the event exactly
    /// where it was, while a failed delete leaves it readable in two places.
    /// The second is the one that needs the `Protocol(PartialResponse)` +
    /// acknowledged `Attempt` verdict, because replaying the whole update
    /// against a source that may already be gone is not the consumer's best
    /// move - reconciling is.
    #[tokio::test]
    async fn a_move_without_server_move_support_copies_then_deletes() {
        use bifrost_types::account::Account as _;

        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:One\r\n\
                   DTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\n\
                   END:VEVENT\r\nEND:VCALENDAR\r\n";
        let response = |status: StatusCode, body: &str| DavResponse {
            status,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = dav_script(
            [
                response(StatusCode::OK, ics),
                // MOVE unimplemented.
                response(StatusCode::METHOD_NOT_ALLOWED, ""),
                // PUT to the destination succeeds.
                response(StatusCode::CREATED, ""),
            ]
            .into_iter()
            .map(Into::into)
            // DELETE of the original fails, and a 500 is now retried to
            // exhaustion before it surfaces.
            .chain(dav_retried(response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "boom",
            )))
            .collect::<Vec<_>>(),
        );
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/work/");

        let error = account
            .event_update(
                bifrost_types::EventId("https://dav.example.test/cal/work/one.ics".to_string()),
                bifrost_types::EventPatch {
                    calendar_id: Some(bifrost_types::CalendarId(
                        "https://dav.example.test/cal/personal/".to_string(),
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
            "a copied-but-not-removed event is a partial response: {error:?}"
        );
        // The classification, not just the kind. `EventUpdate` is idempotent in
        // the `AccountOperation` table, so `derive_protocol` answers
        // `Retry(SameRequest)` for a `PartialResponse` unless the rebuilt error
        // re-states the unreplayable declaration. That retry is the loop this
        // asserts against: the re-issued update finds no MOVE, create-PUTs the
        // destination it already occupied, takes a 412, refreshes, and repeats.
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

    /// A cross-calendar `event_update` MOVES the resource, and a restated
    /// calendar still updates in place.
    ///
    /// History, because the assertion has now been inverted twice: this shape
    /// originally returned `Ok(())` having moved nothing, was then refused
    /// outright as better than a silent drop, and is now performed. The
    /// move-only patch is deliberately ONE request - no content changed, so the
    /// event must not be charged a second write or the partial-failure verdict
    /// that would come with it.
    ///
    /// The second half matters as much as the first: a patch that RESTATES the
    /// event's current calendar is not a move and must still take the ordinary
    /// GET-plus-PUT path. A guard that treated any `calendar_id` as a
    /// relocation would pass the first assertion and break every ordinary
    /// update.
    #[tokio::test]
    async fn event_update_moves_across_calendars_and_updates_in_place_otherwise() {
        use bifrost_types::account::Account as _;

        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:u1\r\nSUMMARY:One\r\n\
                   DTSTART:20260602T120000Z\r\nDTEND:20260602T130000Z\r\n\
                   END:VEVENT\r\nEND:VCALENDAR\r\n";
        let event =
            || bifrost_types::EventId("https://dav.example.test/cal/work/one.ics".to_string());
        let patch_to = |calendar: &str| bifrost_types::EventPatch {
            calendar_id: Some(bifrost_types::CalendarId(calendar.to_string())),
            ..Default::default()
        };

        // A move: GET the current resource, then MOVE it. The destination keeps
        // the resource's own file name.
        let script = dav_script([
            DavResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: ics.to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::CREATED,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/work/");
        account
            .event_update(event(), patch_to("https://dav.example.test/cal/personal/"))
            .await
            .expect("a move between calendars is performed");
        let requests = transcripts(&script);
        assert_eq!(
            requests.len(),
            2,
            "a move-only patch is a GET and a MOVE, with no content write"
        );
        assert_eq!(requests[1].method.as_str(), "MOVE");
        assert_eq!(
            requests[1].url, "https://dav.example.test/cal/work/one.ics",
            "MOVE addresses the source resource"
        );
        assert_eq!(
            requests[1]
                .headers
                .get("Destination")
                .and_then(|value| value.to_str().ok()),
            Some("https://dav.example.test/cal/personal/one.ics"),
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

        // Restating the event's own calendar is not a move, and still updates.
        let script = dav_script([
            DavResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: ics.to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::NO_CONTENT,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let client = Arc::new(CalDavClient::with_account_net(
            "https://dav.example.test",
            scripted_dav_net(&script),
        ));
        let account =
            crate::account::CalDavAccount::for_tests(client, "https://dav.example.test/cal/work/");
        account
            .event_update(event(), patch_to("https://dav.example.test/cal/work/"))
            .await
            .expect("restating the current calendar is not a move");
        assert_eq!(
            transcripts(&script).len(),
            2,
            "an ordinary update is still a GET plus a PUT"
        );
    }

    #[tokio::test]
    async fn credentials_never_reach_a_resource_href_origin() {
        // Two canned responses, so a neutered guard reaches the transport and
        // fails on the destination assertion below rather than on a starved
        // script - the failure has to name the credential leak.
        let event = || DavResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:one\nEND:VEVENT\nEND:VCALENDAR".to_string(),
            url: String::new(),
        };
        let script = dav_script([event(), event()]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .get_event(
                "https://dav.example.test/calendar/one.ics",
                AccountOperation::EventGet,
            )
            .await
            .expect("trusted request succeeds");
        client
            .get_event("https://evil.test/stolen.ics", AccountOperation::EventGet)
            .await
            .expect_err("foreign resource origin is rejected");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://dav.example.test/calendar/one.ics");
        assert_eq!(
            requests[0]
                .headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
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
                "<C:calendar-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:href>{home_href}</D:href></C:calendar-home-set>"
            )),
            response("<D:multistatus xmlns:D=\"DAV:\"/>".to_string()),
        ])
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
                "<C:calendar-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:href>/cal/ada/</D:href></C:calendar-home-set>",
            ),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let discovery = client.discover_account().await.expect("fallback succeeds");

        assert_eq!(discovery.calendar_home, "https://dav.example.test/cal/ada/");
        let urls = transcripts(&script)
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/caldav".to_string(),
                "https://dav.example.test/".to_string(),
                "https://dav.example.test/principals/ada/".to_string(),
            ]
        );
    }

    /// A principal whose answer parses but names no calendar home is a MISSING
    /// FIELD, not a parse failure: the document was well-formed and silent.
    /// Twin of `bifrost-carddav`'s
    /// `a_principal_naming_no_home_is_a_missing_field`. Against the code before
    /// the reclassification the kind assertion fails on `ParseFailed`.
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
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let error = client
            .discover_account()
            .await
            .expect_err("a principal naming no home cannot open");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::MissingField)
        );
        assert_eq!(error.recovery(), &RecoveryClass::ProviderContractViolation);
        assert_eq!(error.protocol(), Some(Protocol::CalDav));
        assert!(
            error.chain().iter().any(|cause| matches!(
                cause,
                Cause::Attempt(attempt)
                    if attempt.transmission_state
                        == bifrost_types::TransmissionState::Acknowledged
            )),
            "the principal answered: {error:?}"
        );
        assert_eq!(transcripts(&script).len(), 2);
    }

    /// A front end sitting on the origin root answers a PROPFIND with `200` and
    /// its index page. That is the SAME deployment the empty-207 test above
    /// covers - a base URL that works, behind an origin root that knows nothing
    /// about DAV - and it must reach the same place. It did not: the HTML fails
    /// the XML decode, which mints `Protocol(ParseFailed)`, and the probe
    /// predicate admitted only 404, 405 and a refused redirect, so the open
    /// FAILED on a deployment one status code away from one that works.
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
                body: "<C:calendar-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:href>/cal/ada/</D:href></C:calendar-home-set>".to_string(),
                url: String::new(),
            },
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let discovery = client
            .discover_account()
            .await
            .expect("an index page at the origin root is not a discovery answer");

        assert_eq!(discovery.calendar_home, "https://dav.example.test/cal/ada/");
        let urls = transcripts(&script)
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/caldav".to_string(),
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
                "<C:calendar-home-set xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:href>/cal/ada/</D:href></C:calendar-home-set>",
            ),
        ]);
        let client = CalDavClient::with_account_net(
            "https://dav.example.test/service",
            scripted_dav_net(&script),
        );

        let discovery = client.discover_account().await.expect("fallback succeeds");

        assert_eq!(discovery.calendar_home, "https://dav.example.test/cal/ada/");
        let urls = transcripts(&script)
            .into_iter()
            .map(|request| request.url)
            .collect::<Vec<_>>();
        assert_eq!(
            urls,
            vec![
                "https://dav.example.test/.well-known/caldav".to_string(),
                "https://dav.example.test/service".to_string(),
                "https://dav.example.test/principals/ada/".to_string(),
            ],
            "the well-known probe is rooted at the origin, and only the fallback uses the configured path"
        );
    }

    #[tokio::test]
    async fn principal_404_does_not_restart_discovery_at_base() {
        let script = dav_script([
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:current-user-principal xmlns:D=\"DAV:\"><D:href>/principals/ada/</D:href></D:current-user-principal>".to_string(),
                url: String::new(),
            },
            DavResponse {
                status: StatusCode::NOT_FOUND,
                headers: HeaderMap::new(),
                body: String::new(),
                url: String::new(),
            },
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .discover_account()
            .await
            .expect_err("principal failure is terminal");

        assert_eq!(transcripts(&script).len(), 2);
    }

    /// The legitimate deployment the origin allowlist must not break: the
    /// calendar home lives on a different host than the principal. It is
    /// discovered over HTTPS, so it is credential-bearing.
    #[tokio::test]
    async fn discovered_cross_origin_https_home_receives_credentials() {
        let script = discovery_script("https://cal.example.test/homes/ada/");
        let mut client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let home = client
            .discover_account()
            .await
            .expect("cross-origin home is discovered")
            .calendar_home;
        assert_eq!(home, "https://cal.example.test/homes/ada/");
        client.admit_discovered_urls(std::iter::once(home.clone()));
        client
            .list_calendars(&home)
            .await
            .expect("cross-origin home is credential-bearing");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].url, "https://cal.example.test/homes/ada/");
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
            HeaderValue::from_static("https://cal.example.test/dav/homes/ada/"),
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
                body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:response><D:href>team/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:calendar/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
                url: String::new(),
            },
        ]);
        let mut client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        client.admit_discovered_urls(std::iter::once(
            "https://cal.example.test/homes/ada/".to_string(),
        ));

        let calendars = client
            .list_calendars("https://dav.example.test/calendars/ada/")
            .await
            .expect("cross-origin redirect is followed with credentials");

        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].url, "https://cal.example.test/dav/homes/ada/");
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
            calendars[0].href,
            "https://cal.example.test/dav/homes/ada/team/"
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
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .list_calendars("https://dav.example.test/calendars/ada/")
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
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .get_event("https://dav.example.test/start", AccountOperation::EventGet)
            .await
            .expect("the configured number of redirects is allowed");

        assert_eq!(transcripts(&script).len(), max_hops + 1);
    }

    /// RFC 4918 resolves a relative href against the EFFECTIVE request URI.
    /// The dispatcher follows same-origin hops, so a PROPFIND submitted to
    /// `/calendar` can be served from `/dav/users/ada/calendar/`. Resolving
    /// `one.ics` against the submitted URI mints `/one.ics` - a native id that
    /// does not exist, and a follow-up GET that 404s.
    ///
    /// The hop is scripted as the 301 it is, so the walk that produces the
    /// effective URI is on the path under test. Previously the transport double
    /// was handed the post-redirect URI directly, which asserted that href
    /// resolution uses whatever URI it is given - true, and not the question.
    #[tokio::test]
    async fn event_hrefs_resolve_against_the_post_redirect_url() {
        let script = dav_script([
            dav_redirect(
                StatusCode::MOVED_PERMANENTLY,
                "https://dav.example.test/dav/users/ada/calendar/",
            ),
            DavResponse {
                status: StatusCode::MULTI_STATUS,
                headers: HeaderMap::new(),
                body: "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>one.ics</D:href><D:propstat><D:prop><D:getetag>\"e1\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
                url: String::new(),
            }
            .into(),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let listing = client
            .list_events_listing(
                "https://dav.example.test/calendar",
                AccountOperation::EventsInRange,
            )
            .await
            .expect("redirected listing succeeds");

        assert_eq!(
            listing.entries[0].uri,
            "https://dav.example.test/dav/users/ada/calendar/one.ics"
        );
    }

    /// Discovery is server-steered, so a discovered home must never weaken the
    /// transport guarantee the configured HTTPS base URL established. The
    /// assertion that matters is the destination: no request at all reaches the
    /// plaintext origin, credential-bearing or otherwise.
    #[tokio::test]
    async fn discovered_plaintext_home_never_receives_credentials() {
        let script = discovery_script("http://cal.example.test/homes/ada/");
        let mut client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let home = client
            .discover_account()
            .await
            .expect("home href is still reported")
            .calendar_home;
        assert_eq!(home, "http://cal.example.test/homes/ada/");
        client.admit_discovered_urls(std::iter::once(home.clone()));
        client
            .list_calendars(&home)
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

    #[tokio::test]
    async fn account_discovery_reads_all_principal_properties_in_two_requests() {
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
            response(
                "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><C:calendar-home-set><D:href>/cal/</D:href></C:calendar-home-set><C:calendar-user-address-set><D:href>mailto:ada@example.test</D:href></C:calendar-user-address-set><C:schedule-outbox-URL><D:href>/outbox/</D:href></C:schedule-outbox-URL></D:multistatus>",
            ),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let discovery = client.discover_account().await.expect("discovery succeeds");

        assert_eq!(discovery.calendar_home, "https://dav.example.test/cal/");
        assert_eq!(
            discovery.calendar_user_email.as_deref(),
            Some("ada@example.test")
        );
        assert_eq!(
            discovery.schedule_outbox_url.as_deref(),
            Some("https://dav.example.test/outbox/")
        );
        assert_eq!(transcripts(&script).len(), 2);
    }

    #[tokio::test]
    async fn scheduling_discovery_failure_is_not_downgraded_to_no_capability() {
        // A 503 is retried to exhaustion first; the surviving classification
        // is still the server's own status, not a transport error.
        let script = dav_script(dav_retried(DavResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: HeaderMap::new(),
            body: "try later".to_string(),
            url: String::new(),
        }));
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let error = client
            .discover_account()
            .await
            .expect_err("a transient discovery failure must fail the open, not be swallowed");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Server(ServerErrorKind::Unavailable)
        );
    }

    #[tokio::test]
    async fn sync_events_uses_depth_zero_report_transcript() {
        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body:
                "<D:multistatus xmlns:D=\"DAV:\"><D:sync-token>next</D:sync-token><D:response><D:href>/calendar/one.ics</D:href><D:status>HTTP/1.1 200 OK</D:status></D:response></D:multistatus>"
                    .to_string(),
            url: String::new(),
        }]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let report = client
            .sync_events("https://dav.example.test/calendar/", "previous")
            .await
            .expect("scripted sync succeeds");

        assert_eq!(report.sync_token.as_deref(), Some("next"));
        assert_eq!(
            report.entries[0].uri,
            "https://dav.example.test/calendar/one.ics"
        );
        let requests = transcripts(&script);
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].method,
            Method::from_bytes(b"REPORT").expect("REPORT method")
        );
        assert_eq!(requests[0].url, "https://dav.example.test/calendar/");
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

    /// A 401 on an ordinary REPORT must classify as reauthorization. Without
    /// the status check in `report_raw` the empty/HTML error body either parses
    /// as an authoritative empty multiget or degrades into a parse failure, and
    /// a consumer treats "no events" as truth.
    #[tokio::test]
    async fn unauthorized_report_classifies_as_reauthorization() {
        // The DAV bearer wrapper refreshes and replays once after a 401. Keep
        // both responses in the scripted wire transcript so the test reaches
        // the terminal reauthorization classification.
        let unauthorized = || DavResponse {
            status: StatusCode::UNAUTHORIZED,
            headers: HeaderMap::new(),
            body: "<html><body>401 Unauthorized</body></html>".to_string(),
            url: String::new(),
        };
        let script = dav_script([unauthorized(), unauthorized()]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let error = client
            .query_event_hrefs_in_range(
                "https://dav.example.test/calendar/",
                Some("20260101T000000Z"),
                Some("20260201T000000Z"),
            )
            .await
            .err()
            .expect("401 REPORT must not be reported as an empty result");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Authentication(bifrost_types::AuthErrorKind::ReauthorizationRequired)
        ));
        assert_eq!(*error.recovery(), RecoveryClass::AuthLost);
    }

    /// A 401 must never be read as "this server will not run the filter": the
    /// degrade lane would re-issue the same credential as a PROPFIND and lose
    /// the reauthorize signal in whatever that answered.
    #[tokio::test]
    async fn a_refused_filter_degrades_where_a_refused_credential_does_not() {
        for (status, body) in [
            (StatusCode::BAD_REQUEST, ""),
            (
                StatusCode::FORBIDDEN,
                "<D:error xmlns:D=\"DAV:\"><C:supported-filter/></D:error>",
            ),
        ] {
            let script = dav_script([DavResponse {
                status,
                headers: HeaderMap::new(),
                body: body.to_string(),
                url: String::new(),
            }]);
            let client = CalDavClient::with_account_net(
                "https://dav.example.test",
                scripted_dav_net(&script),
            );
            let answer = client
                .query_event_hrefs_in_range(
                    "https://dav.example.test/calendar/",
                    Some("20260101T000000Z"),
                    None,
                )
                .await
                .expect("a refused filter is a degrade, not a failure");
            assert!(matches!(answer, FilteredHrefs::FilterUnsupported));
        }
    }

    #[test]
    fn resolve_url_fallback_preserves_separator() {
        let client =
            CalDavClient::with_account_net("not a url", scripted_dav_net(&dav_script_empty()));

        assert_eq!(
            client.resolve_url("calendar/one.ics"),
            "not a url/calendar/one.ics"
        );
        assert_eq!(
            client.resolve_url("/calendar/one.ics"),
            "not a url/calendar/one.ics"
        );
    }

    #[test]
    fn calendar_query_body_uses_caldav_time_range() {
        let body = calendar_query_body(Some("20260602T000000Z"), Some("20260603T000000Z"));

        assert!(body.contains("<C:calendar-query"));
        assert!(body.contains("<D:resourcetype/>"));
        assert!(body.contains("<D:getetag/>"));
        assert!(
            body.contains("<C:time-range start=\"20260602T000000Z\" end=\"20260603T000000Z\"/>")
        );
        // The whole point of the filtered lane: the REPORT names hrefs, and
        // only the sliced page is hydrated. Asking for bodies here puts the
        // entire matching result set on the wire once per page.
        assert!(
            !body.contains("<C:calendar-data/>"),
            "the filtered query must not hydrate: {body}"
        );
    }

    #[test]
    fn a_text_query_names_hrefs_rather_than_hydrating() {
        let body = calendar_text_query_body("SUMMARY", "plan");

        assert!(body.contains("<D:getetag/>"));
        assert!(
            !body.contains("<C:calendar-data/>"),
            "the filtered query must not hydrate: {body}"
        );
    }

    #[test]
    fn calendar_query_body_escapes_time_range_attributes() {
        let body = calendar_query_body(Some("20260602T000000Z"), Some("bad\"&value"));

        assert!(body.contains("end=\"bad&quot;&amp;value\""));
    }

    #[test]
    fn calendar_query_body_preserves_one_sided_ranges() {
        let start_only = calendar_query_body(Some("20260602T000000Z"), None);
        let end_only = calendar_query_body(None, Some("20260603T000000Z"));

        assert!(start_only.contains("<C:time-range start=\"20260602T000000Z\"/>"));
        assert!(end_only.contains("<C:time-range end=\"20260603T000000Z\"/>"));
    }

    #[test]
    fn calendar_text_query_body_uses_property_text_match() {
        let body = calendar_text_query_body("SUMMARY", "plan & meet");

        assert!(body.contains("<C:prop-filter name=\"SUMMARY\">"));
        assert!(body.contains("<D:resourcetype/>"));
        assert!(body.contains("<C:text-match collation=\"i;unicode-casemap\">"));
        assert!(body.contains("plan &amp; meet"));
    }

    #[tokio::test]
    async fn calendar_multiget_uses_depth_zero() {
        let script = dav_script([DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"/>".to_string(),
            url: String::new(),
        }]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        client
            .fetch_events(
                "https://dav.example.test/cal/",
                &["https://dav.example.test/cal/opaque-id".to_string()],
                AccountOperation::EventSearch,
            )
            .await
            .expect("empty multistatus is usable");

        let requests = transcripts(&script);
        assert_eq!(requests[0].headers["Depth"], "0");
        assert!(requests[0].body.contains("<D:resourcetype/>"));
    }

    /// Multiget fan-out is bounded by `MULTIGET_LEG_CONCURRENCY`, not by the
    /// caller's uri list.
    ///
    /// The chunk count is input-sized, so an unbounded dispatch lets one large
    /// calendar open hundreds of simultaneous REPORTs against a server that
    /// never agreed to that. This per-call policy bounds that input-sized
    /// fan-out; no account-wide DAV bound is configured.
    ///
    /// The probe now sits at the wire rather than above the transport: the
    /// yielding script raises its in-flight count for each dispatch that is
    /// actually outstanding, so the mark measures what the net pipeline holds
    /// open, not what the caller handed to a double.
    #[tokio::test]
    async fn multiget_never_holds_more_legs_open_than_the_concurrency_bound() {
        // Ten chunks: comfortably more than the bound, so an unbounded
        // dispatch is distinguishable from a bounded one.
        let chunks = 10;
        let script = dav_script_yielding((0..chunks).map(|_| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"></D:multistatus>".to_string(),
            url: String::new(),
        }));
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let uris = (0..MULTIGET_BATCH_SIZE * chunks)
            .map(|index| format!("https://dav.example.test/cal/{index}.ics"))
            .collect::<Vec<_>>();

        client
            .fetch_events(
                "https://dav.example.test/cal/",
                &uris,
                AccountOperation::EventSearch,
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
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:response><D:href>/cal/one.ics</D:href><D:propstat><D:prop><C:calendar-data>BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:one\nEND:VEVENT\nEND:VCALENDAR</C:calendar-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/cal/".to_string(),
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
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let uris = (0..=MULTIGET_BATCH_SIZE)
            .map(|index| format!("https://dav.example.test/cal/{index}.ics"))
            .collect::<Vec<_>>();

        let fetched = client
            .fetch_events(
                "https://dav.example.test/cal/",
                &uris,
                AccountOperation::EventSearch,
            )
            .await
            .expect("the usable first chunk survives");

        assert_eq!(fetched.report.events.len(), 1);
        assert!(fetched.degraded.is_some());
    }

    /// The adjacent leg-failure path: a later chunk whose body will not parse
    /// must degrade like a later chunk that returned 503, not discard the
    /// chunks that already materialized. A malformed body is account-authored
    /// data, so it is classified and survived rather than asserted on.
    #[tokio::test]
    async fn chunked_multiget_keeps_prior_chunk_when_later_body_is_malformed() {
        let good = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\"><D:response><D:href>/cal/one.ics</D:href><D:propstat><D:prop><C:calendar-data>BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:one\nEND:VEVENT\nEND:VCALENDAR</C:calendar-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/cal/".to_string(),
        };
        let malformed = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"><D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/cal/".to_string(),
        };
        let script = dav_script([good, malformed]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let uris = (0..=MULTIGET_BATCH_SIZE)
            .map(|index| format!("https://dav.example.test/cal/{index}.ics"))
            .collect::<Vec<_>>();

        let fetched = client
            .fetch_events(
                "https://dav.example.test/cal/",
                &uris,
                AccountOperation::EventSearch,
            )
            .await
            .expect("the usable first chunk survives a malformed later chunk");

        assert_eq!(fetched.report.events.len(), 1);
        assert!(fetched.degraded.is_some());
    }

    /// Nothing usable anywhere is still a failed call, malformed or not.
    #[tokio::test]
    async fn an_only_leg_that_will_not_parse_is_still_an_error() {
        let malformed = DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: "<D:multistatus xmlns:D=\"DAV:\"><D:response></D:multistatus>".to_string(),
            url: "https://dav.example.test/cal/".to_string(),
        };
        let script = dav_script([malformed]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));

        let outcome = client
            .fetch_events(
                "https://dav.example.test/cal/",
                &["https://dav.example.test/cal/one.ics".to_string()],
                AccountOperation::EventSearch,
            )
            .await
            .map(|fetched| fetched.report.events.len());
        let Err(error) = outcome else {
            panic!("no usable result anywhere must stay an error");
        };
        assert_eq!(error.operation(), Some(AccountOperation::EventSearch));
    }

    #[test]
    fn schedule_outbox_propfind_requests_caldav_outbox_url() {
        assert!(PROPFIND_ACCOUNT.contains("<C:schedule-outbox-URL/>"));
    }

    #[test]
    fn sync_collection_body_posts_sync_token_and_getetag() {
        let body = sync_collection_body("token&1");

        assert!(body.contains("<D:sync-collection"));
        assert!(body.contains("<D:sync-token>token&amp;1</D:sync-token>"));
        assert!(body.contains("<D:sync-level>1</D:sync-level>"));
        assert!(body.contains("<D:getetag/>"));
        assert!(
            body.contains("<D:getcontenttype/>"),
            "the content type is the sync lane's only component evidence"
        );
    }

    #[test]
    fn mailto_email_extracts_case_insensitive_mailto_address() {
        assert_eq!(
            mailto_email("MAILTO:Ada@Example.Test").as_deref(),
            Some("ada@example.test")
        );
        assert_eq!(
            mailto_email("Mailto:Ada@Example.Test").as_deref(),
            Some("ada@example.test")
        );
        assert_eq!(mailto_email("/principals/ada"), None);
    }

    /// The engine routes `CursorInvalid`'s RestartScope by the error's
    /// cursor scope, and live CalDAV cursors are one per calendar
    /// (`CursorScope::Folder`). A type-wide scope here restarted a cursor
    /// that does not exist while the stale folder cursor livelocked.
    #[test]
    fn stale_sync_token_maps_to_the_invalid_calendars_folder_scope() {
        let error = cursor_invalid_error(
            "https://cal.example/cal/work/",
            StatusCode::FORBIDDEN,
            "<D:valid-sync-token/>".to_string(),
        );

        assert_eq!(
            error.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        );
        assert_eq!(
            error.scope(),
            Some(&ErrorScope::Cursor(CursorScope::Folder(FolderId(
                "https://cal.example/cal/work/".to_string()
            ))))
        );
    }

    #[test]
    fn schedule_address_wraps_bare_email_as_mailto_uri() {
        assert_eq!(
            schedule_address("ada@example.test"),
            "mailto:ada@example.test"
        );
        assert_eq!(
            schedule_address("mailto:ada@example.test"),
            "mailto:ada@example.test"
        );
        assert_eq!(schedule_address("urn:uuid:ada"), "urn:uuid:ada");
    }

    #[test]
    fn accumulated_multiget_success_keeps_a_later_refusal_per_resource() {
        let report = crate::parse::CalDavMultigetReport {
            events: vec![crate::parse::CalDavFetchedEvent {
                uri: "/cal/one.ics".to_string(),
                etag: None,
                data: "BEGIN:VCALENDAR\nEND:VCALENDAR".to_string(),
            }],
            failed: vec![crate::parse::CalDavFailedResource {
                href: "/cal/two.ics".to_string(),
                status: Some(401),
            }],
            missing_data: Vec::new(),
        };

        assert!(multiget_failure(&report, AccountOperation::EventsInRange).is_none());
    }

    fn usable_report() -> CalDavMultigetReport {
        CalDavMultigetReport {
            events: vec![crate::parse::CalDavFetchedEvent {
                uri: "/cal/one.ics".to_string(),
                etag: None,
                data: "BEGIN:VCALENDAR\nEND:VCALENDAR".to_string(),
            }],
            failed: Vec::new(),
            missing_data: Vec::new(),
        }
    }

    #[test]
    fn a_refused_leg_after_a_usable_one_keeps_the_events_and_the_recovery_class() {
        let refusal = status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::EventSearch,
            StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );

        let fetch = MultigetFetch::settle(usable_report(), Some(refusal))
            .expect("a partial result is not a failed call");

        assert_eq!(fetch.report.events.len(), 1);
        assert_eq!(
            fetch.degraded.expect("the refusal survives").recovery(),
            &RecoveryClass::AuthLost
        );
    }

    #[test]
    fn a_refused_leg_with_nothing_usable_stays_an_error() {
        let refusal = status_error(
            &reqwest::header::HeaderMap::new(),
            AccountOperation::EventSearch,
            StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );

        let error = MultigetFetch::settle(CalDavMultigetReport::default(), Some(refusal))
            .err()
            .expect("nothing usable came back");

        assert_eq!(error.recovery(), &RecoveryClass::AuthLost);
    }
}

#[cfg(test)]
mod unsupported_constructor_tests {
    use super::*;

    #[test]
    fn unsupported_constructor_stamps_protocol_and_kind() {
        let operation = bifrost_types::AccountOperation::Send;
        let protocol = bifrost_types::Protocol::CalDav;
        let error = unsupported_error(operation);
        assert_eq!(error.protocol(), Some(protocol));
        assert_eq!(error.operation(), Some(operation));
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Unsupported(operation)
        );
    }
}
