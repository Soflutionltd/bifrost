//! Collection-level DAV operations. Dialects supply syntax and payload parsing;
//! batching, partial results, HTTP classification and conditional CRUD live here.

use std::marker::PhantomData;

use bifrost_net::ReplayPolicy;
use bifrost_types::{AccountError, AccountOperation, ErrorScope, ObjectType, ProtocolKind};
use futures::{StreamExt, stream};
use reqwest::{Method, header::CONTENT_TYPE};

use crate::{
    DavDispatch, DavProtocol, Depth, FailedResource, FilteredHrefs, HrefQuery, MultiStatusOutcome,
    MultistatusError, PutCondition, classify_207, escape_xml, filter_unsupported,
    item_status_error, parse_error, prepare_if_match, response_etag, status_error, worse_recovery,
};

pub const MULTIGET_BATCH_SIZE: usize = 50;
/// Ordered buffering bounds input-sized fan-out and keeps aggregation deterministic.
pub const MULTIGET_LEG_CONCURRENCY: usize = 4;
pub const COLLECTION_PAGE_SIZE: usize = 250;
pub const QUERY_HREF_PROPS: &str =
    "  <D:prop>\n    <D:resourcetype/>\n    <D:getetag/>\n  </D:prop>\n";

/// The syntax and identity that differ between calendar and contact collections.
pub trait Dialect: Clone + Send + Sync + 'static {
    const DAV: DavProtocol;
    const OBJECT_TYPE: ObjectType;
    const PROTOCOL_KIND: ProtocolKind;
    const MULTIGET_ELEMENT: &'static str;
    const NAMESPACE: &'static str;
    const DATA_PROPERTY: &'static str;
    const CONTENT_TYPE: &'static str;
    const FILE_EXTENSION: &'static str;
    const CURSOR_MAGIC: &'static [u8];
    // Version 2 fixes the native-id namespace from base-relative to
    // request-relative. Version 1 has the same byte shape and cannot be
    // migrated reliably, so the shared decoder requires one full resync.
    const CURSOR_VERSION: u32 = 2;

    fn item_scope(id: String) -> ErrorScope;
    fn skipped_scope(collection: &str) -> ErrorScope;
    fn resolve_href(base: &str, href: &str) -> String;
    fn parse_listing(body: &str) -> Result<ResourceListing, MultistatusError>;
    fn parse_multiget(body: &str) -> Result<MultigetReport, MultistatusError>;
    /// Calendar writes can send scheduling messages even without a precondition.
    fn put_replay(condition: &PutCondition) -> ReplayPolicy;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceEntry {
    pub uri: String,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceListing {
    pub entries: Vec<ResourceEntry>,
    pub failed: Vec<FailedResource>,
}

impl ResourceListing {
    /// A complete refusal fails the call; mixed results retain per-id failures.
    pub fn failure<D: Dialect>(&self, operation: AccountOperation) -> Option<AccountError> {
        complete_failure_error::<D>(self.classify(), self.failed.len(), operation)
    }

    pub fn resolve_hrefs_with(&mut self, base: &str, resolve: impl Fn(&str, &str) -> String) {
        for entry in &mut self.entries {
            entry.uri = resolve(base, &entry.uri);
        }
        for failure in &mut self.failed {
            failure.href = resolve(base, &failure.href);
        }
    }

    pub fn failed_hrefs(&self) -> Vec<String> {
        self.failed
            .iter()
            .map(|failure| failure.href.clone())
            .collect()
    }

    pub fn classify(&self) -> MultiStatusOutcome {
        classify_207(!self.entries.is_empty(), &self.failed)
    }

    pub fn extend_candidates(self, query: &mut HrefQuery) {
        let failed = self.failed_hrefs();
        query.extend(self.entries.into_iter().map(|entry| entry.uri), failed);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedResource {
    pub uri: String,
    pub etag: Option<String>,
    pub data: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultigetReport {
    pub items: Vec<FetchedResource>,
    pub failed: Vec<FailedResource>,
    /// Successful members without the requested data are absences, not refusals.
    pub missing_data: Vec<String>,
}

impl MultigetReport {
    /// A complete refusal fails the call; mixed results retain per-id failures.
    pub fn failure<D: Dialect>(&self, operation: AccountOperation) -> Option<AccountError> {
        complete_failure_error::<D>(self.classify(), self.failed.len(), operation)
    }

    pub fn extend(&mut self, other: Self) {
        self.items.extend(other.items);
        self.failed.extend(other.failed);
        self.missing_data.extend(other.missing_data);
    }

    pub fn failed_hrefs(&self) -> Vec<String> {
        self.failed
            .iter()
            .map(|failure| failure.href.clone())
            .chain(self.missing_data.iter().cloned())
            .collect()
    }

    pub fn classify(&self) -> MultiStatusOutcome {
        classify_207(!self.items.is_empty(), &self.failed)
    }

    pub fn resolve_hrefs(&mut self, base: &str) {
        self.resolve_hrefs_with(base, crate::resolve_href);
    }

    pub fn resolve_hrefs_with(&mut self, base: &str, resolve: impl Fn(&str, &str) -> String) {
        for item in &mut self.items {
            item.uri = resolve(base, &item.uri);
        }
        for failure in &mut self.failed {
            failure.href = resolve(base, &failure.href);
        }
        for href in &mut self.missing_data {
            *href = resolve(base, href);
        }
    }
}

pub struct MultigetFetch {
    pub report: MultigetReport,
    pub degraded: Option<AccountError>,
}

impl MultigetFetch {
    /// Even missing or refused members are observations worth preserving beside
    /// an unavailable leg. Only a fetch with no observations at all is an Err.
    pub fn settle(
        report: MultigetReport,
        degraded: Option<AccountError>,
    ) -> Result<Self, AccountError> {
        let empty =
            report.items.is_empty() && report.failed.is_empty() && report.missing_data.is_empty();
        match degraded {
            Some(error) if empty => Err(error),
            degraded => Ok(Self { report, degraded }),
        }
    }
}

pub fn complete_failure_error<D: Dialect>(
    outcome: MultiStatusOutcome,
    failed: usize,
    operation: AccountOperation,
) -> Option<AccountError> {
    match outcome {
        MultiStatusOutcome::Usable => None,
        MultiStatusOutcome::CompleteFailure { status } => Some(status_error(
            &reqwest::header::HeaderMap::new(),
            operation,
            status,
            format!("multi-status body reported failure for all {failed} resources"),
            D::DAV,
        )),
    }
}

/// A borrowed collection view retains the dispatcher's credential gate and any
/// per-hop directory guard. No operation opens a second transport path.
pub struct DavCollectionClient<'a, D: Dialect> {
    dispatch: &'a DavDispatch,
    dialect: PhantomData<D>,
}

impl<'a, D: Dialect> DavCollectionClient<'a, D> {
    pub fn new(dispatch: &'a DavDispatch) -> Self {
        Self {
            dispatch,
            dialect: PhantomData,
        }
    }

    pub async fn fetch(
        &self,
        url: &str,
        uris: &[String],
        operation: AccountOperation,
    ) -> Result<MultigetFetch, AccountError> {
        let bodies: Vec<_> = uris
            .chunks(MULTIGET_BATCH_SIZE)
            .map(multiget_body::<D>)
            .collect();
        let pending: Vec<_> = bodies
            .iter()
            .map(|body| self.multiget_leg(url, body, operation))
            .collect();
        let mut legs = stream::iter(pending).buffered(MULTIGET_LEG_CONCURRENCY);
        let mut report = MultigetReport::default();
        let mut degraded = None;
        while let Some(result) = legs.next().await {
            match result {
                Ok(parsed) => {
                    degraded = worse_recovery_option(degraded, parsed.failure::<D>(operation));
                    report.extend(parsed);
                }
                Err(error) => degraded = worse_recovery(degraded, error),
            }
        }
        MultigetFetch::settle(report, degraded)
    }

    async fn multiget_leg(
        &self,
        url: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<MultigetReport, AccountError> {
        let response = self
            .dispatch
            .report_raw(url, Depth::Zero, body, operation)
            .await?;
        let mut parsed = D::parse_multiget(&response.text)
            .map_err(|error| parse_error(operation, format!("multiget: {error}"), D::DAV))?;
        parsed.resolve_hrefs_with(&response.url, D::resolve_href);
        Ok(parsed)
    }

    pub async fn href_query_leg(
        &self,
        url: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<Option<ResourceListing>, AccountError> {
        let response = self
            .dispatch
            .report_raw_response(url, Depth::One, body, operation)
            .await?;
        if !response.status.is_success() {
            if filter_unsupported(response.status, &response.body) {
                return Ok(None);
            }
            return Err(status_error(
                &response.headers,
                operation,
                response.status,
                response.body,
                D::DAV,
            ));
        }
        let mut listing = D::parse_listing(&response.body)
            .map_err(|error| parse_error(operation, format!("query: {error}"), D::DAV))?;
        listing.resolve_hrefs_with(&response.url, D::resolve_href);
        if let Some(error) = listing.failure::<D>(operation) {
            return Err(error);
        }
        Ok(Some(listing))
    }

    pub async fn query_text(
        &self,
        url: &str,
        bodies: &[String],
        operation: AccountOperation,
    ) -> Result<FilteredHrefs, AccountError> {
        let pending: Vec<_> = bodies
            .iter()
            .map(|body| self.href_query_leg(url, body, operation))
            .collect();
        let mut legs = stream::iter(pending).buffered(MULTIGET_LEG_CONCURRENCY);
        let mut merged = HrefQuery::default();
        let mut unsupported = false;
        while let Some(leg) = legs.next().await {
            match leg {
                Ok(Some(listing)) => listing.extend_candidates(&mut merged),
                Ok(None) => unsupported = true,
                Err(error) => merged.degraded = worse_recovery(merged.degraded.take(), error),
            }
        }
        // One refused property makes the entire prefilter incomplete. Enterprise
        // search must fall back to a wide listing, never silently lose matches.
        if unsupported {
            return Ok(FilteredHrefs::FilterUnsupported);
        }
        merged.settle().map(FilteredHrefs::Matched)
    }

    pub async fn get(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<FetchedResource, AccountError> {
        let request = self
            .dispatch
            .request(Method::GET, url)
            .replay(ReplayPolicy::Idempotent);
        let response = self.dispatch.send_raw_request(request, operation).await?;
        if !response.status.is_success() {
            return Err(item_status_error(
                &response.headers,
                operation,
                response.status,
                response.body,
                D::DAV,
            ));
        }
        Ok(FetchedResource {
            uri: url.to_owned(),
            etag: response_etag(&response.headers),
            data: response.body,
        })
    }

    pub async fn put(
        &self,
        url: &str,
        body: String,
        condition: PutCondition,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let mut request = self
            .dispatch
            .request(Method::PUT, url)
            .replay(D::put_replay(&condition))
            .header(CONTENT_TYPE, D::CONTENT_TYPE)
            .body(body);
        if let Some((name, value)) = condition.header() {
            request = request.header(name, value);
        }
        let response = self.dispatch.send_raw_request(request, operation).await?;
        if response.status.is_success() {
            Ok(response_etag(&response.headers))
        } else {
            Err(item_status_error(
                &response.headers,
                operation,
                response.status,
                response.body,
                D::DAV,
            ))
        }
    }

    pub async fn delete_if_match(
        &self,
        url: &str,
        etag: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        let version = bifrost_types::ServerVersion::ETag(etag.into());
        let etag = version.expected_etag(D::DAV.protocol(), operation, true)?;
        let etag = prepare_if_match(etag).expect("validated strong entity tag");
        let request = self
            .dispatch
            .request(Method::DELETE, url)
            .replay(ReplayPolicy::Never)
            .header("If-Match", etag);
        self.dispatch
            .send_item_status_request(request, operation)
            .await
    }

    pub async fn delete(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        let request = self
            .dispatch
            .request(Method::DELETE, url)
            .replay(ReplayPolicy::IdempotentUnlessConditional);
        let response = self.dispatch.send_raw_request(request, operation).await?;
        if crate::delete_target_already_gone(response.status) {
            return Ok(None);
        }
        if response.status.is_success() {
            Ok(response_etag(&response.headers))
        } else {
            Err(item_status_error(
                &response.headers,
                operation,
                response.status,
                response.body,
                D::DAV,
            ))
        }
    }
}

fn multiget_body<D: Dialect>(uris: &[String]) -> String {
    let hrefs: String = uris
        .iter()
        .map(|uri| format!("  <D:href>{}</D:href>\n", escape_xml(uri)))
        .collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<C:{element} xmlns:D=\"DAV:\" xmlns:C=\"{namespace}\">\n  <D:prop>\n    <D:resourcetype/>\n    <D:getetag/>\n    <C:{property}/>\n  </D:prop>\n{hrefs}</C:{element}>",
        element = D::MULTIGET_ELEMENT,
        namespace = D::NAMESPACE,
        property = D::DATA_PROPERTY
    )
}

pub fn worse_recovery_option(
    current: Option<AccountError>,
    candidate: Option<AccountError>,
) -> Option<AccountError> {
    match candidate {
        Some(candidate) => worse_recovery(current, candidate),
        None => current,
    }
}
