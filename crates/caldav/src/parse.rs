use bifrost_dav_core::{
    APPLE_CALENDAR_NAMESPACE, CALDAV_NAMESPACE, DAV_NAMESPACE, MultiStatusSink, PropSet,
    ResponseParts, XmlName, classify_207, commit_if_present, normalize_etag,
    parse_collection_property, parse_multistatus, same_dav_url, trimmed,
};
pub(crate) use bifrost_dav_core::{extract_href_properties, extract_href_property};

/// Rebase a response href against its request URI, the one funnel every href
/// this crate reads goes through.
///
/// A `#` in an href would be a URL fragment delimiter, so a conforming server
/// percent-encodes a literal one as `%23` and never sends it raw. A server that
/// does send one raw has still named a resource, and it means the character:
/// `Url::join` would instead read it as a fragment, keep it in the resolved
/// string, and drop it from every request, addressing the wrong resource. It is
/// encoded here so that no resolved URL, and therefore no native id, ever holds
/// a `#` other than the one `event_id_for` adds to qualify an override. That
/// invariant is what `split_event_id` relies on.
pub(crate) fn resolve_href(request_url: &str, href: &str) -> String {
    if href.contains('#') {
        return bifrost_dav_core::resolve_href(request_url, &href.replace('#', "%23"));
    }
    bifrost_dav_core::resolve_href(request_url, href)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalendarCollection {
    pub(crate) href: String,
    pub(crate) display_name: Option<String>,
    pub(crate) color: Option<String>,
    pub(crate) can_edit: Option<bool>,
    pub(crate) sync_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavEventEntry {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
}

/// Outcome of a depth-1 event PROPFIND or a `calendar-query`: the resources
/// whose propstat succeeded (`entries`) plus the resources the server reported
/// *failed* within the 207. A failed resource is a transiently-failed one, not
/// an absent one - the snapshot diff preserves the local copy rather than
/// emitting a Destroyed (brick 7).
///
/// The failure lane carries each resource's STATUS, not just its href, so this
/// listing runs through the same RFC 4918 s13 ladder the multiget lanes use.
/// Without it an all-refused 207 - which the `calendar-query` candidate lane
/// meets whenever a server refuses every member - came back as an empty
/// candidate set plus a bare list of hrefs, and the recovery class of that
/// refusal was unreachable: a consumer saw an empty page and recorded a
/// completed walk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CalDavEventListing {
    pub(crate) entries: Vec<CalDavEventEntry>,
    pub(crate) failed: Vec<CalDavFailedResource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavFetchedEvent {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
    pub(crate) data: String,
}

/// One resource inside a 207 that yielded nothing usable, with the status the
/// server gave it (when it gave one). Both member lanes use it - the multiget
/// for a resource with no `calendar-data`, the listing for a resource whose
/// every propstat failed - so both classify through the same ladder.
pub(crate) type CalDavFailedResource = bifrost_dav_core::FailedResource;

/// Outcome of a `calendar-multiget` / `calendar-query` REPORT: the
/// resources that came back with usable `calendar-data` (`events`) plus
/// the ones that did not (`failed`).
///
/// A 207 Multi-Status is per-resource by construction, so a single bad
/// propstat inside it is a per-resource failure and nothing more.
/// Failing the whole parse on one of them turned a one-event problem
/// into a dead pull. But the converse is equally wrong: RFC 4918 s13
/// says a 207 body may describe success, partial success, OR complete
/// failure, so a body in which every resource failed must not be
/// handed back as an empty success. See `classify`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CalDavMultigetReport {
    pub(crate) events: Vec<CalDavFetchedEvent>,
    pub(crate) failed: Vec<CalDavFailedResource>,
    /// Resources that returned a successful response but omitted the
    /// requested `calendar-data`. They are per-resource absences, not DAV
    /// failures, so they surface through `Page::failed_ids` but cannot turn
    /// a 207 into a synthetic server error.
    pub(crate) missing_data: Vec<String>,
}

/// What a parsed 207 body actually represents, per RFC 4918 s13.
pub(crate) type MultigetOutcome = bifrost_dav_core::MultiStatusOutcome;

impl CalDavMultigetReport {
    /// Fold another chunk's report into this one. Multiget is chunked
    /// and text search runs one REPORT per property, so both lanes have
    /// to accumulate across calls.
    pub(crate) fn extend(&mut self, other: CalDavMultigetReport) {
        self.events.extend(other.events);
        self.failed.extend(other.failed);
        self.missing_data.extend(other.missing_data);
    }

    pub(crate) fn failed_hrefs(&self) -> Vec<String> {
        self.failed
            .iter()
            .map(|failure| failure.href.clone())
            .chain(self.missing_data.iter().cloned())
            .collect()
    }

    /// Classify the body rather than merely parsing it.
    ///
    /// An empty body is `Usable`: a query that matched nothing is a
    /// legitimate empty result, not a failure. A body with at least one
    /// usable resource is `Usable` too - the failures are real, and the
    /// caller still reports them, but the request as a whole worked.
    ///
    /// A body where EVERY resource failed is only benign if every
    /// failure was a missing resource (404/410), which is what happens
    /// when hrefs are deleted between the listing and the multiget.
    /// If any of them was an auth, permission, or server condition,
    /// the body is a complete failure and the caller must surface it as
    /// an error instead of an empty page.
    pub(crate) fn classify(&self) -> MultigetOutcome {
        classify_207(!self.events.is_empty(), &self.failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavSyncReport {
    pub(crate) sync_token: Option<String>,
    pub(crate) entries: Vec<CalDavSyncEntry>,
    /// The server truncated this result (RFC 6578 s3.6): it answered with a
    /// `<response>` for the COLLECTION URI itself carrying `507 Insufficient
    /// Storage`, and the accompanying sync-token represents only partial
    /// progress. The remaining changes arrive only if the client issues
    /// another sync REPORT with the returned token.
    pub(crate) truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CalDavSyncEntry {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
    pub(crate) status: Option<u16>,
    /// The server POSITIVELY declared this member something other than a
    /// VEVENT, through a `component=` parameter on its `getcontenttype`
    /// (RFC 4791 s5.2.6; SabreDAV and Baikal emit it). The sync-collection
    /// lane cannot be component-filtered - RFC 6578 has no filter grammar - so
    /// this is the only evidence it gets, and it is honoured only in the
    /// positive: a bare `text/calendar`, or no content type at all, says
    /// nothing and the member is admitted as before. See
    /// [`declared_non_vevent`].
    pub(crate) non_vevent: bool,
}

/// Whether a `getcontenttype` value names a component other than VEVENT.
///
/// `text/calendar; component=vtodo` is positive evidence that the resource is
/// not an event, and it is the only evidence the unfilterable lanes have: the
/// `sync-collection` REPORT (RFC 6578 has no filter grammar) and the depth-1
/// PROPFIND the cursor listing degrades to when a server refuses the VEVENT
/// `comp-filter`. Dropping a member on this evidence loses nothing: a server
/// that labels a resource `component=vtodo` will never return it from the
/// filtered query either, so the two lanes agree instead of flapping. Absent
/// or unparameterised content types are NOT evidence, and answer `false`; the
/// residual leak on servers that emit no parameter is documented in
/// `reference/caldav.md`.
pub(crate) fn declared_non_vevent(content_type: Option<&str>) -> bool {
    let Some(content_type) = content_type else {
        return false;
    };
    content_type.split(';').skip(1).any(|parameter| {
        let Some((name, value)) = parameter.split_once('=') else {
            return false;
        };
        name.trim().eq_ignore_ascii_case("component")
            && !value
                .trim()
                .trim_matches('"')
                .eq_ignore_ascii_case("vevent")
    })
}

impl CalendarCollection {
    pub(crate) fn resolve_href(&mut self, request_url: &str) {
        self.href = resolve_href(request_url, &self.href);
    }
}

impl CalDavEventListing {
    pub(crate) fn resolve_hrefs(&mut self, request_url: &str) {
        for entry in &mut self.entries {
            entry.uri = resolve_href(request_url, &entry.uri);
        }
        for failure in &mut self.failed {
            failure.href = resolve_href(request_url, &failure.href);
        }
    }

    /// The refused hrefs, for the lanes that only carry ids (`Page::failed_ids`
    /// and the snapshot-diff preservation guard).
    pub(crate) fn failed_hrefs(&self) -> Vec<String> {
        self.failed
            .iter()
            .map(|failure| failure.href.clone())
            .collect()
    }

    /// Read this 207 through the RFC 4918 s13 ladder, the same one the multiget
    /// lanes use. A listing with any committed entry is `Usable` however many
    /// siblings were refused - a per-member failure beside successes stays a
    /// per-id failure and the page is served.
    pub(crate) fn classify(&self) -> MultigetOutcome {
        classify_207(!self.entries.is_empty(), &self.failed)
    }
}

impl CalDavMultigetReport {
    pub(crate) fn resolve_hrefs(&mut self, request_url: &str) {
        for event in &mut self.events {
            event.uri = resolve_href(request_url, &event.uri);
        }
        for failed in &mut self.failed {
            failed.href = resolve_href(request_url, &failed.href);
        }
        for href in &mut self.missing_data {
            *href = resolve_href(request_url, href);
        }
    }
}

impl CalDavSyncReport {
    pub(crate) fn resolve_hrefs(&mut self, request_url: &str) {
        for entry in &mut self.entries {
            entry.uri = resolve_href(request_url, &entry.uri);
        }
    }

    /// Remove the response describing the COLLECTION itself and record whether
    /// it announced truncation.
    ///
    /// The `sync-collection` REPORT requests only `getetag`, so no
    /// `resourcetype` distinguishes the collection's own response from a
    /// member's - the href does, and it is resolved absolute by the time this
    /// runs. Left in place, that response is an ordinary member entry: it is
    /// inserted into the snapshot as if the collection were an event, and a
    /// phantom `Created` is emitted for it. Its status is also the RFC 6578
    /// s3.6 truncation marker (507), which is a statement about the REPORT and
    /// not about any resource.
    ///
    /// `request_url` is the URI the REPORT was actually served from and
    /// `collection_url` the one it was addressed to; a redirect can make them
    /// differ, and either spelling identifies the collection.
    pub(crate) fn take_collection_response(&mut self, collection_url: &str, request_url: &str) {
        let mut truncated = false;
        self.entries.retain(|entry| {
            if same_dav_url(&entry.uri, collection_url) || same_dav_url(&entry.uri, request_url) {
                truncated |= entry.status == Some(507);
                false
            } else {
                true
            }
        });
        self.truncated = truncated;
    }
}

/// Properties a calendar-collection PROPFIND stages. The `<calendar/>`
/// resourcetype marker and the privilege markers are staged like every value
/// property: a `resourcetype` or `current-user-privilege-set` the server
/// REFUSED says nothing about the collection.
#[derive(Default)]
struct CalendarCollectionProps {
    is_calendar: bool,
    privilege_seen: bool,
    write_seen: bool,
    display_name: Option<String>,
    color: Option<String>,
    sync_token: Option<String>,
}

impl PropSet for CalendarCollectionProps {
    fn commit_from(&mut self, staged: Self) {
        self.is_calendar |= staged.is_calendar;
        self.privilege_seen |= staged.privilege_seen;
        self.write_seen |= staged.write_seen;
        commit_if_present(&mut self.display_name, staged.display_name);
        commit_if_present(&mut self.color, staged.color);
        commit_if_present(&mut self.sync_token, staged.sync_token);
    }
}

/// Properties an event-resource lane stages: the depth-1 listing, the
/// multiget/query REPORT and the sync REPORT all read from this set.
#[derive(Default)]
struct EventProps {
    is_collection: bool,
    etag: Option<String>,
    content_type: Option<String>,
    calendar_data: Option<String>,
}

impl PropSet for EventProps {
    fn commit_from(&mut self, staged: Self) {
        self.is_collection |= staged.is_collection;
        commit_if_present(&mut self.etag, staged.etag);
        commit_if_present(&mut self.content_type, staged.content_type);
        commit_if_present(&mut self.calendar_data, staged.calendar_data);
    }

    fn is_collection(&self) -> bool {
        self.is_collection
    }

    fn resource_data(&self) -> Option<&str> {
        self.calendar_data.as_deref()
    }
}

/// Mark `<collection/>` inside a `<resourcetype>`, the guard both member lanes
/// share.
fn mark_collection(name: &XmlName, stack: &[XmlName], parts: &mut ResponseParts<EventProps>) {
    if name.is(DAV_NAMESPACE, "collection")
        && stack
            .iter()
            .any(|item| item.is(DAV_NAMESPACE, "resourcetype"))
    {
        parts.marker_mut().is_collection = true;
    }
}

#[derive(Default)]
struct CalendarCollectionSink {
    collections: Vec<CalendarCollection>,
}

impl MultiStatusSink for CalendarCollectionSink {
    type Props = CalendarCollectionProps;

    fn element(
        &mut self,
        name: &XmlName,
        stack: &[XmlName],
        parts: &mut ResponseParts<Self::Props>,
    ) {
        if name.is(CALDAV_NAMESPACE, "calendar")
            && stack
                .iter()
                .any(|item| item.is(DAV_NAMESPACE, "resourcetype"))
        {
            parts.marker_mut().is_calendar = true;
        }
        if name.is(DAV_NAMESPACE, "privilege") {
            parts.marker_mut().privilege_seen = true;
        }
        if (name.is(DAV_NAMESPACE, "write")
            || name.is(DAV_NAMESPACE, "write-content")
            || name.is(DAV_NAMESPACE, "all"))
            && stack.iter().any(|item| item.is(DAV_NAMESPACE, "privilege"))
        {
            parts.marker_mut().write_seen = true;
        }
    }

    fn property(
        &mut self,
        parent: Option<&XmlName>,
        name: &XmlName,
        text: &str,
        parts: &mut ResponseParts<Self::Props>,
    ) {
        if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
            && name.is(DAV_NAMESPACE, "displayname")
        {
            parts.staged_mut().display_name = trimmed(text);
        } else if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
            && name.is(APPLE_CALENDAR_NAMESPACE, "calendar-color")
        {
            parts.staged_mut().color = trimmed(text).map(|value| value.trim().to_string());
        } else if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
            && name.is(DAV_NAMESPACE, "sync-token")
        {
            parts.staged_mut().sync_token = trimmed(text);
        }
    }

    fn finish_response(&mut self, parts: &ResponseParts<Self::Props>) {
        let props = parts.props();
        if !props.is_calendar {
            return;
        }
        let Some(href) = parts.href() else {
            return;
        };
        self.collections.push(CalendarCollection {
            href: href.to_string(),
            display_name: props.display_name.clone(),
            color: props.color.clone(),
            can_edit: props.privilege_seen.then_some(props.write_seen),
            sync_token: props.sync_token.clone(),
        });
    }
}

pub(crate) fn parse_calendar_collections(xml: &str) -> Result<Vec<CalendarCollection>, String> {
    let mut sink = CalendarCollectionSink::default();
    parse_multistatus(xml, &mut sink)?;
    Ok(sink.collections)
}

#[derive(Default)]
struct EventListingSink {
    listing: CalDavEventListing,
}

impl MultiStatusSink for EventListingSink {
    type Props = EventProps;

    fn element(
        &mut self,
        name: &XmlName,
        stack: &[XmlName],
        parts: &mut ResponseParts<Self::Props>,
    ) {
        mark_collection(name, stack, parts);
    }

    fn property(
        &mut self,
        parent: Option<&XmlName>,
        name: &XmlName,
        text: &str,
        parts: &mut ResponseParts<Self::Props>,
    ) {
        // Staged, never written straight to the committed field: a property
        // inside a REFUSED propstat is not evidence about the resource. A
        // server echoing the requested prop skeleton back inside a 404 - the
        // same shape the `collection` marker is staged against - would
        // otherwise poison the snapshot etag with a stale value, and the
        // snapshot diff and the inventory fingerprint both read that etag.
        if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
            && name.is(DAV_NAMESPACE, "getetag")
        {
            parts.staged_mut().etag = normalize_etag(text);
        } else if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
            && name.is(DAV_NAMESPACE, "getcontenttype")
        {
            parts.staged_mut().content_type = trimmed(text);
        }
    }

    fn finish_response(&mut self, parts: &ResponseParts<Self::Props>) {
        if let Some(href) = parts.entry_href() {
            // The degrade lane: this listing runs only when the server refused
            // the VEVENT `comp-filter`, so the content type is the one piece of
            // component evidence left. Honoured in the positive only.
            if declared_non_vevent(parts.props().content_type.as_deref()) {
                return;
            }
            self.listing.entries.push(CalDavEventEntry {
                uri: href.to_string(),
                etag: parts.props().etag.clone(),
            });
        } else if let Some(failure) = parts.failed_member() {
            self.listing.failed.push(failure);
        }
    }
}

pub(crate) fn parse_propfind_events(xml: &str) -> Result<CalDavEventListing, String> {
    let mut sink = EventListingSink::default();
    parse_multistatus(xml, &mut sink)?;
    Ok(sink.listing)
}

#[derive(Default)]
struct MultigetSink {
    report: CalDavMultigetReport,
}

impl MultiStatusSink for MultigetSink {
    type Props = EventProps;

    fn element(
        &mut self,
        name: &XmlName,
        stack: &[XmlName],
        parts: &mut ResponseParts<Self::Props>,
    ) {
        mark_collection(name, stack, parts);
    }

    fn property(
        &mut self,
        parent: Option<&XmlName>,
        name: &XmlName,
        text: &str,
        parts: &mut ResponseParts<Self::Props>,
    ) {
        // Properties are collected PROPSTAT-scoped and only promoted by the
        // shared machine if that propstat's own status was 2xx. Writing the
        // response-level fields here instead made the result depend on
        // propstat order: data from a failed block could be adopted, and a
        // trailing non-2xx block could discard data a successful one supplied.
        if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
            && name.is(DAV_NAMESPACE, "getetag")
        {
            parts.staged_mut().etag = normalize_etag(text);
        } else if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
            && name.is(CALDAV_NAMESPACE, "calendar-data")
        {
            parts.staged_mut().calendar_data = trimmed(text);
        }
    }

    fn finish_response(&mut self, parts: &ResponseParts<Self::Props>) {
        if let Some((href, data)) = parts.fetched() {
            self.report.events.push(CalDavFetchedEvent {
                uri: href.to_string(),
                etag: parts.props().etag.clone(),
                data: data.to_string(),
            });
        } else if let Some((href, status)) = parts.failed_resource() {
            // Nothing usable came back for this one resource. That says
            // nothing about its siblings, so the rest of the page survives;
            // the carried status lets the caller tell a vanished resource
            // apart from a refusal.
            self.report.failed.push(CalDavFailedResource {
                href: href.to_string(),
                status: Some(status),
            });
        } else if let Some(href) = parts.missing_data_href() {
            self.report.missing_data.push(href.to_string());
        }
    }
}

/// Parse a `calendar-multiget` / `calendar-query` 207 response.
///
/// `Err` is reserved for a malformed document - the whole payload is
/// unusable and there is nothing per-resource to salvage. A well-formed
/// 207 whose individual responses carry non-2xx propstats (or 2xx with
/// no `calendar-data`) is NOT an error: those hrefs land in
/// `failed_hrefs` and every sibling that did come back is returned, so
/// the caller can report the casualties through `Page::failed_ids`
/// while the rest of the pull succeeds.
pub(crate) fn parse_multiget_report(xml: &str) -> Result<CalDavMultigetReport, String> {
    let mut sink = MultigetSink::default();
    parse_multistatus(xml, &mut sink)?;
    Ok(sink.report)
}

#[derive(Default)]
struct SyncReportSink {
    sync_token: Option<String>,
    entries: Vec<CalDavSyncEntry>,
}

impl MultiStatusSink for SyncReportSink {
    type Props = EventProps;

    fn property(
        &mut self,
        parent: Option<&XmlName>,
        name: &XmlName,
        text: &str,
        parts: &mut ResponseParts<Self::Props>,
    ) {
        if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
            && name.is(DAV_NAMESPACE, "getetag")
        {
            parts.staged_mut().etag = normalize_etag(text);
        } else if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
            && name.is(DAV_NAMESPACE, "getcontenttype")
        {
            parts.staged_mut().content_type = trimmed(text);
        }
    }

    fn document_property(&mut self, parent: Option<&XmlName>, name: &XmlName, text: &str) {
        if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "multistatus"))
            && name.is(DAV_NAMESPACE, "sync-token")
        {
            self.sync_token = trimmed(text);
        }
    }

    /// Every response with an href is a sync member, including the
    /// collection's own (dropped later by `take_collection_response`, which
    /// reads its 507 as the RFC 6578 s3.6 truncation marker) and including
    /// deletions, which carry a response-level 404 and no propstat at all.
    fn finish_response(&mut self, parts: &ResponseParts<Self::Props>) {
        let Some(href) = parts.href() else {
            return;
        };
        self.entries.push(CalDavSyncEntry {
            uri: href.to_string(),
            etag: parts.props().etag.clone(),
            status: parts.member_status_code(),
            non_vevent: declared_non_vevent(parts.props().content_type.as_deref()),
        });
    }
}

pub(crate) fn parse_sync_collection_report(xml: &str) -> Result<CalDavSyncReport, String> {
    let mut sink = SyncReportSink::default();
    parse_multistatus(xml, &mut sink)?;
    Ok(CalDavSyncReport {
        sync_token: sink.sync_token,
        entries: sink.entries,
        truncated: false,
    })
}

/// Extract a collection sync token from a depth-0 PROPFIND. The token is
/// committed only from a successful propstat so a stale value in a refused
/// property cannot revive an invalid cursor.
pub(crate) fn parse_collection_sync_token(xml: &str) -> Result<Option<String>, String> {
    parse_collection_property(xml, "sync-token")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hrefs_resolve_against_the_request_uri() {
        assert_eq!(
            resolve_href("https://cal.example.test/homes/ada/", "team/one.ics"),
            "https://cal.example.test/homes/ada/team/one.ics"
        );
    }

    /// A raw `#` in an href is read as the character, not a fragment: joined
    /// as-is it stays in the resolved string yet never reaches a request, and
    /// would make `split_event_id` cut the resource in two. A `%23` from a
    /// conforming server is left alone, and no resolved href holds a raw `#`.
    #[test]
    fn a_raw_hash_in_an_href_resolves_as_a_literal_character() {
        for href in [
            "/cal/a#b.ics",
            "cal/a#b.ics",
            "https://h.example.test/cal/a#b.ics",
        ] {
            let resolved = resolve_href("https://h.example.test/cal/", href);
            assert!(!resolved.contains('#'), "{href} -> {resolved}");
            assert!(resolved.ends_with("/cal/a%23b.ics"), "{href} -> {resolved}");
        }
        assert_eq!(
            resolve_href("https://h.example.test/cal/", "/cal/a%23b.ics"),
            "https://h.example.test/cal/a%23b.ics"
        );
    }

    #[test]
    fn nested_property_status_does_not_refuse_href_property() {
        let hrefs = extract_href_properties(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:response><D:propstat><D:status>HTTP/1.1 200 OK</D:status><D:prop><C:calendar-user-address-set><D:href>mailto:right@example.test</D:href><C:extension><D:status>HTTP/1.1 404 Not Found</D:status></C:extension></C:calendar-user-address-set></D:prop></D:propstat></D:response></D:multistatus>"#,
            "calendar-user-address-set",
        )
        .expect("valid property");

        assert_eq!(hrefs, vec!["mailto:right@example.test".to_string()]);
    }

    /// The migration to request-URI resolution must not respell ids that
    /// were already correct. Most servers emit absolute-path hrefs, and the
    /// pre-migration base was `CalDavConfig::base_url` with its trailing
    /// slash trimmed. Pin both spellings against the post-migration request
    /// URI, and pin the literal so a regression cannot pass by changing both
    /// sides at once.
    #[test]
    fn absolute_path_href_keeps_the_common_deployment_id() {
        let href = "/calendars/ada/one.ics";
        let previous = resolve_href("https://dav.example.test", href);
        assert_eq!(previous, "https://dav.example.test/calendars/ada/one.ics");
        assert_eq!(
            resolve_href("https://dav.example.test/calendars/ada/", href),
            previous
        );
        assert_eq!(
            resolve_href("https://dav.example.test/dav/users/ada/calendar/", href),
            previous
        );
    }

    #[test]
    fn calendar_collections_read_display_metadata_and_privileges() {
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:A="http://apple.com/ns/ical/">
  <D:response>
    <D:href>/cal/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:calendar/></D:resourcetype>
        <D:displayname>Personal</D:displayname>
        <A:calendar-color>#112233</A:calendar-color>
        <D:current-user-privilege-set>
          <D:privilege><D:read/></D:privilege>
          <D:privilege><D:write-content/></D:privilege>
        </D:current-user-privilege-set>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");
        assert_eq!(
            calendars,
            vec![CalendarCollection {
                href: "/cal/personal/".to_string(),
                display_name: Some("Personal".to_string()),
                color: Some("#112233".to_string()),
                can_edit: Some(true),
                sync_token: None,
            }]
        );
    }

    #[test]
    fn a_foreign_calendar_marker_does_not_classify_a_collection() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:X="urn:vendor">
          <D:response><D:href>/cal/not-a-calendar/</D:href>
          <D:propstat><D:prop><D:resourcetype><D:collection/><X:calendar/></D:resourcetype>
          </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
        </D:multistatus>"#;

        assert!(
            parse_calendar_collections(xml)
                .expect("valid XML")
                .is_empty()
        );
    }

    #[test]
    fn calendar_collections_read_sync_token() {
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:calendar/></D:resourcetype>
        <D:sync-token>token-1</D:sync-token>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");

        assert_eq!(calendars[0].sync_token.as_deref(), Some("token-1"));
    }

    #[test]
    fn calendar_collections_ignore_failed_propstat_values() {
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:A="http://apple.com/ns/ical/">
  <D:response>
    <D:href>/cal/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:calendar/></D:resourcetype>
        <D:displayname>Personal</D:displayname>
        <A:calendar-color>#112233</A:calendar-color>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");
        assert!(calendars.is_empty());
    }

    #[test]
    fn calendar_home_without_calendar_children_lists_empty() {
        // Depth-1 PROPFIND on a calendar-home that is a plain collection with
        // no calendar children: the home's own response carries no <calendar/>
        // and there are no other collections. `calendars_list` maps this
        // parse output 1:1, so an empty result here is what surfaces to the
        // consumer as an empty calendar list (no fabricated placeholder).
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/home/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
        <D:displayname>Home</D:displayname>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");
        assert!(calendars.is_empty());
    }

    #[test]
    fn calendar_home_that_is_itself_a_calendar_lists_one() {
        // A CalDAV server whose calendar-home is itself a calendar collection
        // (resourcetype includes <calendar/>) is returned by the same depth-1
        // parse. This is the genuine case the removed placeholder-fallback was
        // conflated with: it needs no fabrication because the home's own
        // response already maps to one calendar keyed on the home href.
        let xml = r##"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/home/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:calendar/></D:resourcetype>
        <D:displayname>Home</D:displayname>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"##;

        let calendars = parse_calendar_collections(xml).expect("valid XML");
        assert_eq!(
            calendars,
            vec![CalendarCollection {
                href: "/cal/home/".to_string(),
                display_name: Some("Home".to_string()),
                color: None,
                can_edit: None,
                sync_token: None,
            }]
        );
    }

    #[test]
    fn propfind_events_extracts_ics_resources() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat><D:prop>
      <D:getetag>"abc"</D:getetag>
      <D:getcontenttype>text/calendar</D:getcontenttype>
    </D:prop></D:propstat>
  </D:response>
</D:multistatus>"#;

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(
            listing.entries,
            vec![CalDavEventEntry {
                uri: "/cal/one.ics".to_string(),
                etag: Some("abc".to_string()),
            }]
        );
        assert!(listing.failed.is_empty());
    }

    #[test]
    fn propfind_events_accepts_extensionless_resources() {
        let xml = r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/cal/opaque-id</D:href><D:propstat><D:prop>
          <D:resourcetype/><D:getetag>"abc"</D:getetag>
          </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(listing.entries[0].uri, "/cal/opaque-id");
    }

    #[test]
    fn propfind_events_ignores_nested_href_properties() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat><D:prop>
      <C:calendar-user-address-set>
        <D:href>mailto:ada@example.test</D:href>
      </C:calendar-user-address-set>
      <D:getetag>"abc"</D:getetag>
      <D:getcontenttype>text/calendar</D:getcontenttype>
    </D:prop></D:propstat>
  </D:response>
</D:multistatus>"#;

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(listing.entries[0].uri, "/cal/one.ics");
    }

    #[test]
    fn propfind_events_ignores_collections_even_when_calendar_typed() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/archive.ics/</D:href>
    <D:propstat><D:prop>
      <D:resourcetype><D:collection/></D:resourcetype>
      <D:getetag>"abc"</D:getetag>
      <D:getcontenttype>text/calendar</D:getcontenttype>
    </D:prop></D:propstat>
  </D:response>
</D:multistatus>"#;

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert!(listing.entries.is_empty());
    }

    #[test]
    fn propfind_events_surfaces_failed_propstat_as_failed_href() {
        // Brick 7: an event resource whose only propstat is a 404 is not
        // committed as an entry, but IS surfaced as a failed href so the
        // snapshot diff preserves the local copy.
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"missing"</D:getetag>
        <D:getcontenttype>text/calendar</D:getcontenttype>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert!(listing.entries.is_empty());
        assert_eq!(listing.failed_hrefs(), vec!["/cal/one.ics".to_string()]);
        // The lane carries the STATUS as well as the href, which is what makes
        // an all-refused 207 classifiable. A 404 is the benign per-resource
        // case, so this listing still reads as usable.
        assert_eq!(listing.failed[0].status, Some(404));
        assert_eq!(listing.classify(), MultigetOutcome::Usable);
    }

    /// A `resourcetype` block the server REFUSED says nothing about the
    /// resource. Servers routinely echo the requested prop skeleton back
    /// inside a 404 propstat, and some echo a `<collection/>` child with
    /// it; treating that as authoritative discarded a real event whose own
    /// properties came back 200.
    #[test]
    fn propfind_events_ignores_collection_marker_in_a_failed_propstat() {
        let xml = r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/cal/opaque-id</D:href>
          <D:propstat><D:prop><D:getetag>"abc"</D:getetag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].uri, "/cal/opaque-id");
        assert_eq!(listing.entries[0].etag, Some("abc".to_string()));
    }

    /// Same rule on the multiget lane: a refused `resourcetype` must not
    /// suppress calendar data a successful propstat supplied.
    #[test]
    fn multiget_ignores_collection_marker_in_a_failed_propstat() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
          <D:response><D:href>/cal/opaque-id</D:href>
          <D:propstat><D:prop><D:getetag>"abc"</D:getetag>
          <C:calendar-data>BEGIN:VCALENDAR
END:VCALENDAR</C:calendar-data></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.events[0].uri, "/cal/opaque-id");
    }

    /// An etag echoed back inside a REFUSED propstat is not evidence about
    /// the resource. Committing it poisons the snapshot etag, which drives
    /// both the change diff and the inventory fingerprint.
    #[test]
    fn propfind_events_ignores_an_etag_inside_a_failed_propstat() {
        let xml = r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/cal/opaque-id</D:href>
          <D:propstat><D:prop><D:getcontenttype>text/calendar</D:getcontenttype></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:getetag>"stale"</D:getetag></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].etag, None);
    }

    #[test]
    fn propfind_events_preserves_failed_extensionless_resource() {
        let xml = r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/cal/opaque-id</D:href><D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 503 Unavailable</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        let listing = parse_propfind_events(xml).expect("valid XML");
        assert_eq!(listing.failed_hrefs(), vec!["/cal/opaque-id"]);
    }

    #[test]
    fn multiget_does_not_fetch_an_echoed_collection() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
          <D:response><D:href>/cal/</D:href><D:propstat><D:prop>
          <D:resourcetype><D:collection/></D:resourcetype>
          <C:calendar-data>not an event</C:calendar-data></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
          </D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert!(report.events.is_empty());
    }

    #[test]
    fn multiget_report_reports_embedded_failures_per_resource() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/missing.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("a 207 is not a parse failure");
        assert!(report.events.is_empty());
        assert_eq!(report.failed_hrefs(), vec!["/cal/missing.ics".to_string()]);
        assert_eq!(report.failed[0].status, Some(404));
        // Every resource failed, but the only failure was a vanished
        // resource, so the body is still usable.
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn one_bad_propstat_does_not_abort_the_rest_of_the_multiget() {
        // The whole point of a 207: per-resource outcomes. A refused
        // resource must not cost the caller its siblings.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/missing.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 403 Forbidden</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/cal/good.ics</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"g1"</D:getetag>
        <C:calendar-data>BEGIN:VCALENDAR
END:VCALENDAR</C:calendar-data>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("a 207 is not a parse failure");
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.events[0].uri, "/cal/good.ics");
        assert_eq!(report.failed_hrefs(), vec!["/cal/missing.ics".to_string()]);
        // Partial success: the 403 is reported, not fatal.
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn multiget_report_treats_a_2xx_without_calendar_data_as_failed() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/empty.ics</D:href>
    <D:propstat>
      <D:prop><D:getetag>"e1"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("a 207 is not a parse failure");
        assert!(report.events.is_empty());
        assert_eq!(report.failed_hrefs(), vec!["/cal/empty.ics".to_string()]);
        assert!(report.failed.is_empty());
        assert_eq!(report.missing_data, vec!["/cal/empty.ics"]);
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn multiget_report_ignores_an_echoed_collection_response() {
        // Some servers echo the collection alongside the requested
        // resources. That is not a failed event resource.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("a 207 is not a parse failure");
        assert!(report.events.is_empty());
        assert!(report.failed.is_empty());
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn multiget_report_accepts_embedded_success() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"abc"</D:getetag>
        <C:calendar-data>BEGIN:VCALENDAR
END:VCALENDAR</C:calendar-data>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("embedded 200 should pass");
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.events[0].etag.as_deref(), Some("abc"));
        assert!(report.failed.is_empty());
    }

    /// A response split across two propstat blocks - the RFC 4918
    /// shape where found and not-found properties are reported
    /// separately - in both orders. Equivalent input must give an
    /// equivalent answer.
    fn two_propstat_multiget(data_first: bool) -> String {
        let ok_block = "    <D:propstat>\n\
      <D:prop>\n\
        <D:getetag>\"ok\"</D:getetag>\n\
        <C:calendar-data>BEGIN:VCALENDAR\nEND:VCALENDAR</C:calendar-data>\n\
      </D:prop>\n\
      <D:status>HTTP/1.1 200 OK</D:status>\n\
    </D:propstat>\n";
        let missing_block = "    <D:propstat>\n\
      <D:prop><D:displayname/></D:prop>\n\
      <D:status>HTTP/1.1 404 Not Found</D:status>\n\
    </D:propstat>\n";
        let blocks = if data_first {
            format!("{ok_block}{missing_block}")
        } else {
            format!("{missing_block}{ok_block}")
        };
        format!(
            "<D:multistatus xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">\n\
  <D:response>\n\
    <D:href>/cal/split.ics</D:href>\n\
{blocks}  </D:response>\n\
</D:multistatus>"
        )
    }

    #[test]
    fn multiget_is_not_order_dependent_across_propstats() {
        // A trailing 404 propstat for an unrelated property must not
        // retract calendar-data a 200 propstat legitimately supplied,
        // and the answer must not depend on which block came first.
        for data_first in [true, false] {
            let xml = two_propstat_multiget(data_first);
            let report = parse_multiget_report(&xml).expect("valid 207");
            assert_eq!(
                report.events.len(),
                1,
                "data_first={data_first}: usable calendar-data was dropped"
            );
            assert_eq!(report.events[0].uri, "/cal/split.ics");
            assert_eq!(report.events[0].etag.as_deref(), Some("ok"));
            assert!(report.failed.is_empty());
        }
    }

    #[test]
    fn calendar_data_from_a_failed_propstat_is_never_adopted() {
        // The mirror image: data sitting inside a non-2xx propstat is
        // not usable, even when a later propstat succeeds.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/refused.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data>BEGIN:VCALENDAR
END:VCALENDAR</C:calendar-data></D:prop>
      <D:status>HTTP/1.1 403 Forbidden</D:status>
    </D:propstat>
    <D:propstat>
      <D:prop><D:getetag>"x"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(
            report.events.is_empty(),
            "calendar-data from a 403 propstat must not be served as an event"
        );
        assert_eq!(report.failed_hrefs(), vec!["/cal/refused.ics".to_string()]);
        assert_eq!(report.failed[0].status, Some(403));
    }

    #[test]
    fn a_wholly_refused_body_is_a_complete_failure_not_an_empty_success() {
        // RFC 4918 s13: a 207 can describe complete failure. Handing
        // this back as an empty page lets a consumer mark the
        // collection walked and drop both resources for good.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 401 Unauthorized</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/cal/two.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 401 Unauthorized</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert_eq!(
            report.classify(),
            MultigetOutcome::CompleteFailure { status: Some(401) }
        );
    }

    /// A `<D:status>` that is PRESENT but unreadable must not commit the
    /// properties beside it.
    ///
    /// `propstat_success.unwrap_or(true)` treats `None` as success, which
    /// is right for an ABSENT status (RFC 4918 s14.22 requires one, and a
    /// server omitting it is describing a success). It is wrong for a
    /// status that is there and cannot be parsed: that is not evidence
    /// the property was returned, so committing it accepts a value the
    /// server may have refused.
    ///
    /// This crate previously mapped unparseable to `None` (success) while
    /// bifrost-carddav mapped it to failure - a divergence that survived
    /// precisely because each crate spelled the check itself. Both now
    /// fail closed through `bifrost_net::status_line_is_success`.
    #[test]
    fn a_present_but_unparseable_propstat_status_does_not_commit() {
        // Real calendar-data, so the old behaviour genuinely committed an
        // event here - the assertion would pass vacuously against an
        // empty prop.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"abc"</D:getetag>
        <C:calendar-data>BEGIN:VCALENDAR
END:VCALENDAR</C:calendar-data>
      </D:prop>
      <D:status>HTTP/1.1 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(
            report.events.is_empty(),
            "an unreadable status must not commit the property beside it"
        );
    }

    #[test]
    fn multiget_response_level_status_reports_failed_resource() {
        // Some servers report a vanished resource with a response-level
        // status and no propstat at all (the sync-collection shape). The
        // status still classifies the failure as a benign missing
        // resource.
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/gone.ics</D:href>
    <D:status>HTTP/1.1 404 Not Found</D:status>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.events.is_empty());
        assert_eq!(report.failed_hrefs(), vec!["/cal/gone.ics".to_string()]);
        assert_eq!(report.failed[0].status, Some(404));
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    /// `status_line_code` reads the status POSITION, not the first
    /// in-range number anywhere in the line. That moves inputs between
    /// buckets here, so both directions are pinned: the protocol-less
    /// form servers really emit inside `<D:status>` still classifies,
    /// and free text a proxy dropped in no longer fabricates a code the
    /// server never sent. The failure lane keeps its `href` either way -
    /// this is about the reported status, not about losing the resource.
    #[test]
    fn a_prose_response_status_does_not_fabricate_a_resource_status_code() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/cal/terse.ics</D:href>
    <D:status>404 Not Found</D:status>
  </D:response>
  <D:response>
    <D:href>/cal/prose.ics</D:href>
    <D:propstat>
      <D:prop><D:getetag>"p"</D:getetag></D:prop>
      <D:status>Resource error 404 reported by upstream</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.events.is_empty());
        let terse = report
            .failed
            .iter()
            .find(|resource| resource.href == "/cal/terse.ics")
            .expect("a protocol-less status line still classifies");
        assert_eq!(terse.status, Some(404));
        // The refusal is still honoured - no property is committed - but
        // with no readable code the resource carries no status to report,
        // so it surfaces in the absent-data lane rather than being
        // labelled with a code the server never sent.
        assert!(
            !report
                .failed
                .iter()
                .any(|resource| resource.href == "/cal/prose.ics"),
            "prose must not be mined for a status the server never sent"
        );
        assert_eq!(report.missing_data, vec!["/cal/prose.ics"]);
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn absent_calendar_data_does_not_make_a_missing_resource_systemic() {
        // A 2xx propstat with no calendar-data is an absent-data outcome,
        // not a server failure. Combined with a benign 404 the body remains
        // usable and both hrefs stay in the per-resource failed-id lane.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response>
    <D:href>/cal/gone.ics</D:href>
    <D:propstat>
      <D:prop><C:calendar-data/></D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/cal/empty.ics</D:href>
    <D:propstat>
      <D:prop><D:getetag>"e"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.events.is_empty());
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.missing_data, vec!["/cal/empty.ics"]);
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn an_empty_body_is_usable_not_a_failure() {
        // A query that matched nothing is a legitimate empty result.
        let xml = r#"<D:multistatus xmlns:D="DAV:"></D:multistatus>"#;
        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.events.is_empty());
        assert!(report.failed.is_empty());
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn sync_collection_report_reads_token_and_changed_entries() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:sync-token>token-2</D:sync-token>
  <D:response>
    <D:href>/cal/one.ics</D:href>
    <D:propstat>
      <D:prop><D:getetag>"new"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/cal/two.ics</D:href>
    <D:status>HTTP/1.1 404 Not Found</D:status>
  </D:response>
</D:multistatus>"#;

        let report = parse_sync_collection_report(xml).expect("valid XML");

        assert_eq!(report.sync_token.as_deref(), Some("token-2"));
        assert_eq!(
            report.entries,
            vec![
                // A member with a successful propstat and no response-level
                // status reports NO member status: the propstat code speaks
                // for one property, not the resource.
                CalDavSyncEntry {
                    uri: "/cal/one.ics".to_string(),
                    etag: Some("new".to_string()),
                    status: None,
                    non_vevent: false,
                },
                CalDavSyncEntry {
                    uri: "/cal/two.ics".to_string(),
                    etag: None,
                    status: Some(404),
                    non_vevent: false,
                },
            ]
        );
    }

    /// A `component=` parameter on `getcontenttype` is the only component
    /// evidence the unfilterable lanes get, and it is read in the positive
    /// only: `vtodo` marks the member, `vevent`, a bare `text/calendar` and no
    /// content type at all do not.
    #[test]
    fn a_declared_component_marks_non_vevent_members_on_both_unfiltered_lanes() {
        assert!(declared_non_vevent(Some("text/calendar; component=vtodo")));
        assert!(declared_non_vevent(Some(
            "text/calendar; charset=utf-8; Component=\"VJOURNAL\""
        )));
        assert!(!declared_non_vevent(Some(
            "text/calendar; component=VEVENT"
        )));
        assert!(!declared_non_vevent(Some("text/calendar")));
        assert!(!declared_non_vevent(None));

        let sync = parse_sync_collection_report(
            r#"<D:multistatus xmlns:D="DAV:"><D:sync-token>t</D:sync-token><D:response><D:href>/cal/task.ics</D:href><D:propstat><D:prop><D:getetag>"a"</D:getetag><D:getcontenttype>text/calendar; component=vtodo</D:getcontenttype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response><D:response><D:href>/cal/one.ics</D:href><D:propstat><D:prop><D:getetag>"b"</D:getetag><D:getcontenttype>text/calendar</D:getcontenttype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid sync report");
        assert_eq!(
            sync.entries
                .iter()
                .map(|entry| (entry.uri.as_str(), entry.non_vevent))
                .collect::<Vec<_>>(),
            vec![("/cal/task.ics", true), ("/cal/one.ics", false)],
            "the sync lane carries the evidence to the snapshot, which decides"
        );

        let listing = parse_propfind_events(
            r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href>/cal/task.ics</D:href><D:propstat><D:prop><D:getetag>"a"</D:getetag><D:getcontenttype>text/calendar; component=vtodo</D:getcontenttype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response><D:response><D:href>/cal/one.ics</D:href><D:propstat><D:prop><D:getetag>"b"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid listing");
        assert_eq!(
            listing
                .entries
                .iter()
                .map(|entry| entry.uri.as_str())
                .collect::<Vec<_>>(),
            vec!["/cal/one.ics"],
            "the degrade listing drops a declared task and keeps an unlabelled member"
        );
        assert!(listing.failed.is_empty(), "a dropped task is not a failure");
    }

    #[test]
    fn cdata_is_read_by_listing_sync_and_href_parsers() {
        let listing = parse_propfind_events(
            r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href><![CDATA[/cal/one.ics]]></D:href><D:propstat><D:prop><D:getetag><![CDATA["one"]]></D:getetag><D:getcontenttype>text/calendar</D:getcontenttype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid listing");
        assert_eq!(listing.entries[0].uri, "/cal/one.ics");
        assert_eq!(listing.entries[0].etag.as_deref(), Some("one"));

        let sync = parse_sync_collection_report(
            r#"<D:multistatus xmlns:D="DAV:"><D:sync-token><![CDATA[token-2]]></D:sync-token><D:response><D:href><![CDATA[/cal/one.ics]]></D:href><D:propstat><D:prop><D:getetag><![CDATA["two"]]></D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid sync report");
        assert_eq!(sync.sync_token.as_deref(), Some("token-2"));
        assert_eq!(sync.entries[0].etag.as_deref(), Some("two"));

        let href = extract_href_property(
            r#"<D:current-user-principal xmlns:D="DAV:"><D:href><![CDATA[/principals/ada/]]></D:href></D:current-user-principal>"#,
            "current-user-principal",
        )
        .expect("valid property");
        assert_eq!(href.as_deref(), Some("/principals/ada/"));
    }

    #[test]
    fn sync_report_accepts_extensionless_resource_ids() {
        let report = parse_sync_collection_report(
            r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href>/cal/opaque-id</D:href><D:status>HTTP/1.1 404 Not Found</D:status></D:response></D:multistatus>"#,
        )
        .expect("valid XML");

        assert_eq!(report.entries[0].uri, "/cal/opaque-id");
        assert_eq!(report.entries[0].status, Some(404));
    }

    #[test]
    fn address_set_extractor_returns_every_href() {
        let hrefs = extract_href_properties(
            r#"<C:calendar-user-address-set xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:href>/principals/ada/</D:href><D:href>Mailto:Ada@example.test</D:href></C:calendar-user-address-set>"#,
            "calendar-user-address-set",
        )
        .expect("valid property");

        assert_eq!(
            hrefs,
            vec![
                "/principals/ada/".to_string(),
                "Mailto:Ada@example.test".to_string()
            ]
        );
    }

    #[test]
    fn href_extractors_ignore_failed_propstats() {
        let hrefs = extract_href_properties(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:response><D:propstat><D:prop><C:calendar-user-address-set><D:href>mailto:wrong@example.test</D:href></C:calendar-user-address-set></D:prop><D:status>HTTP/1.1 404 Not Found</D:status></D:propstat><D:propstat><D:prop><C:calendar-user-address-set><D:href>mailto:right@example.test</D:href></C:calendar-user-address-set></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
            "calendar-user-address-set",
        )
        .expect("valid XML");

        assert_eq!(hrefs, vec!["mailto:right@example.test".to_string()]);
    }

    #[test]
    fn weak_etag_keeps_weakness_marker() {
        assert_eq!(normalize_etag("W/\"abc\"").as_deref(), Some("W/\"abc\""));
        assert_eq!(normalize_etag("\"abc\"").as_deref(), Some("abc"));
    }

    #[test]
    fn calendar_element_outside_resourcetype_does_not_mark_collection() {
        let collections = parse_calendar_collections(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:response><D:href>/not-a-calendar/</D:href><D:propstat><D:prop><D:owner><C:calendar/></D:owner></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid multistatus");

        assert!(collections.is_empty());
    }
}
