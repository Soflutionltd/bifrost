//! The one WebDAV 207 Multi-Status state machine.
//!
//! CalDAV and CardDAV parse the same document. `<response>` carries an
//! `<href>`, an optional response-level `<status>`, and zero or more
//! `<propstat>` blocks each pairing a `<prop>` bag with its own `<status>`.
//! Only the property NAMES and the entry types the caller builds differ, and
//! for a long time each crate hand-mirrored the whole machine. Eight defects
//! were recorded in those mirrored lines - every one of them a fix that landed
//! in one crate and not its twin - so the machine lives here once and each
//! crate supplies only a [`PropSet`] naming which properties it stages plus the
//! constructors that turn a finished response into its own entry type.
//!
//! Three rules are load-bearing and are enforced here rather than restated by
//! each caller:
//!
//! - **Stage under the status.** A property is read into the STAGED set while
//!   its `<propstat>` is open and promoted to the committed set only when that
//!   propstat's own status was 2xx. A server echoing the requested prop
//!   skeleton back inside a 404 - which real servers do - must not poison the
//!   committed etag, the `resourcetype`, or the privilege markers.
//! - **An absent status is success.** RFC 4918 s14.22 requires a status, but
//!   servers omit it and the properties beside it are genuinely there. A status
//!   that is PRESENT and unparseable is a failure, because it is not evidence
//!   that the property was returned.
//! - **Entry versus failed href.** A response whose ONLY propstat failed is not
//!   an entry; it is a transiently-failed resource whose href the caller
//!   preserves. A response carrying NO propstat at all is still a member the
//!   server named, and commits as an entry with whatever it supplied - dropping
//!   it out of both lanes makes the snapshot diff destroy a resource that
//!   exists.

use bifrost_net::{status_line_code, status_line_is_success};
use quick_xml::events::Event;
use quick_xml::reader::NsReader;

use crate::xml::{
    DAV_NAMESPACE, XmlName, property_namespace, push_text, resolved_xml_name, trimmed,
};

/// The staged property bag one crate reads out of a `<prop>`.
///
/// Implementors hold only the properties that crate asked the server for; the
/// response-level plumbing (href, statuses, which propstats succeeded) belongs
/// to [`ResponseParts`] and is not repeated here.
pub trait PropSet: Default {
    /// Promote a SUCCESSFUL propstat's staged properties onto the committed
    /// set. Boolean markers are OR-ed; a property the propstat did not carry
    /// must leave the committed value alone (see [`commit_if_present`]).
    fn commit_from(&mut self, staged: Self);

    /// True when the committed `resourcetype` said this response describes a
    /// collection rather than a member resource. Collections are excluded from
    /// every member lane: some servers echo the collection itself alongside the
    /// resources a multiget asked for.
    fn is_collection(&self) -> bool {
        false
    }

    /// The committed body property (`calendar-data` / `address-data`), when the
    /// crate stages one at all.
    fn resource_data(&self) -> Option<&str> {
        None
    }
}

/// One resource a 207 named but did not answer usably for, with the status the
/// server gave it (when it gave a parseable one).
///
/// Both member lanes produce these: the multiget lanes from
/// [`ResponseParts::failed_resource`], the listing lanes from
/// [`ResponseParts::failed_href`] paired with
/// [`ResponseParts::member_status_code`]. Carrying the status is what makes
/// [`classify_207`] reachable from a listing, which is the whole reason the
/// failure lane is not a bare `Vec<String>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedResource {
    pub href: String,
    pub status: Option<u16>,
}

impl FailedResource {
    /// True when the status means "this particular resource is not there" - the
    /// benign, genuinely per-resource case that `Page::failed_ids` exists to
    /// carry. Anything else (auth, permission, server error, or no status at
    /// all) can just as easily be a condition affecting the whole request.
    #[must_use]
    pub fn is_missing_resource(&self) -> bool {
        matches!(self.status, Some(404 | 410))
    }
}

/// What a parsed 207 body actually represents, per RFC 4918 s13.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiStatusOutcome {
    /// Nothing failed, or enough succeeded that the failures are per-resource
    /// news the caller should report but not fail on.
    Usable,
    /// Every resource in the body failed and at least one of them failed for a
    /// reason that is not "this resource is missing". Reporting this as an empty
    /// success lets a consumer record the collection as fully walked and drop
    /// the resources permanently.
    CompleteFailure { status: Option<u16> },
}

/// The one RFC 4918 s13 status ladder, applied to any 207 that has a usable
/// lane and a failed lane.
///
/// `any_usable` is whatever the caller's success lane produced - hydrated
/// bodies for a multiget, committed entries for a listing. An empty body is
/// `Usable`: a query that matched nothing is a legitimate empty result. A body
/// where EVERY resource failed is benign only if every failure was a missing
/// resource (404/410), which is what happens when hrefs are deleted between the
/// listing and the fetch; anything else is a condition on the request and must
/// reach the consumer with its recovery class rather than as an empty page.
#[must_use]
pub fn classify_207(any_usable: bool, failed: &[FailedResource]) -> MultiStatusOutcome {
    if any_usable || failed.is_empty() {
        return MultiStatusOutcome::Usable;
    }
    match failed.iter().find(|failure| !failure.is_missing_resource()) {
        Some(failure) => MultiStatusOutcome::CompleteFailure {
            status: failure.status,
        },
        None => MultiStatusOutcome::Usable,
    }
}

/// Commit a staged property over its committed slot, leaving the committed
/// value alone when this propstat did not carry the property.
pub fn commit_if_present<T>(committed: &mut Option<T>, staged: Option<T>) {
    if staged.is_some() {
        *committed = staged;
    }
}

/// One `<response>` being read, with its staged and committed property sets.
#[derive(Debug, Default, Clone)]
pub struct ResponseParts<P> {
    in_response: bool,
    in_propstat: bool,
    has_success_propstat: bool,
    saw_failed_propstat: bool,
    href: Option<String>,
    response_status: Option<String>,
    /// Status codes of every non-2xx propstat in this response, in document
    /// order. Drives failure classification.
    failed_statuses: Vec<u16>,
    staged_status: Option<String>,
    staged_success: Option<bool>,
    committed: P,
    staged: P,
}

impl<P: PropSet> ResponseParts<P> {
    /// True between `<response>` and `</response>`.
    pub fn in_response(&self) -> bool {
        self.in_response
    }

    /// Start a fresh `<response>`, discarding anything left from the last one.
    pub fn begin_response(&mut self) {
        *self = Self {
            in_response: true,
            ..Self::default()
        };
    }

    fn end_response(&mut self) {
        self.in_response = false;
    }

    /// Start a `<propstat>`; its properties are staged until its status is
    /// known.
    pub fn begin_propstat(&mut self) {
        self.in_propstat = true;
        self.staged = P::default();
        self.staged_status = None;
        self.staged_success = None;
    }

    /// Close a `<propstat>`, promoting its staged properties only on 2xx.
    pub fn commit_propstat(&mut self) {
        let staged = std::mem::take(&mut self.staged);
        let status = self.staged_status.take();
        let success = self.staged_success.take();
        self.in_propstat = false;

        if success == Some(false) {
            self.saw_failed_propstat = true;
            if let Some(code) = status.as_deref().and_then(status_line_code) {
                self.failed_statuses.push(code);
            }
        }
        // An absent status is success (RFC 4918 s14.22 requires one, but
        // servers omit it and the properties are still there).
        if success.unwrap_or(true) {
            self.has_success_propstat = true;
            self.committed.commit_from(staged);
        }
    }

    /// The property bag of the propstat currently open. Property VALUES are
    /// always staged: a `<prop>` outside a propstat commits nothing.
    pub fn staged_mut(&mut self) -> &mut P {
        &mut self.staged
    }

    /// The bag a bare MARKER element should be written to: the staged set
    /// inside a propstat, the committed set outside one. A `resourcetype` or
    /// `privilege` the server refused says nothing about the resource, so it is
    /// staged; the same element outside any propstat has no status to be gated
    /// on and commits directly.
    pub fn marker_mut(&mut self) -> &mut P {
        if self.in_propstat {
            &mut self.staged
        } else {
            &mut self.committed
        }
    }

    /// The committed properties of this response.
    pub fn props(&self) -> &P {
        &self.committed
    }

    /// The response's own `<href>`, as written by the server.
    pub fn href(&self) -> Option<&str> {
        self.href.as_deref()
    }

    /// The response-level `<status>` line, when the server sent one.
    pub fn response_status(&self) -> Option<&str> {
        self.response_status.as_deref()
    }

    /// The status a `sync-collection` member should report.
    ///
    /// A response-level code wins: RFC 6578 s3.3 reports a removed member as
    /// a response carrying its own `404`/`410` and no propstat. Below that, a
    /// propstat status describes ONE property lookup, not the resource - a
    /// server answers `404 Not Found` for any requested property it lacks,
    /// beside a `200` propstat that carries the etag. So any successful
    /// propstat makes the member successful (`None`), and only a response
    /// whose every propstat failed reports a failed code; reading the
    /// first propstat's code regardless turned a per-property miss into a
    /// `Destroyed` for a resource that exists.
    ///
    /// Which failed code is [`worst_failed_status`](Self::worst_failed_status):
    /// document order alone let a `<propstat 404: getcontenttype>` written
    /// ahead of a `<propstat 403: getetag>` report the member as a benign
    /// missing resource, and an all-refused 207 then classified `Usable` with
    /// no entries - an empty snapshot for a collection the server refused,
    /// whose diff destroys every resource in it.
    pub fn member_status_code(&self) -> Option<u16> {
        if let Some(code) = self.response_status.as_deref().and_then(status_line_code) {
            return Some(code);
        }
        if self.has_success_propstat {
            return None;
        }
        self.worst_failed_status()
    }

    /// The failed propstat code that describes the RESOURCE rather than one
    /// property lookup: the first refusal that is not a 404/410, falling back
    /// to document order when every refusal was a missing-property answer.
    ///
    /// Not a numeric maximum. `404`/`410` are the only codes
    /// [`FailedResource::is_missing_resource`] reads as benign, and among real
    /// refusals a larger number is not a worse one - ranking `507` over `401`
    /// would bury a reauthorization signal under a storage complaint. So the
    /// ladder has exactly two rungs, and inside the upper rung the server's own
    /// order stands.
    fn worst_failed_status(&self) -> Option<u16> {
        self.failed_statuses
            .iter()
            .find(|code| !matches!(**code, 404 | 410))
            .or_else(|| self.failed_statuses.first())
            .copied()
    }

    /// The href of a response that commits as a member entry.
    ///
    /// A response whose ONLY propstat failed is withheld - it belongs in
    /// [`failed_href`](Self::failed_href). A response with no propstat at all
    /// commits: the server named the resource, and dropping it out of both
    /// lanes makes the snapshot diff destroy a resource that exists.
    pub fn entry_href(&self) -> Option<&str> {
        if self.committed.is_collection() {
            return None;
        }
        if self.saw_failed_propstat && !self.has_success_propstat {
            return None;
        }
        self.href.as_deref()
    }

    /// The href of a resource the server reported *failed* within the 207 (a
    /// non-2xx propstat and no successful one). A transiently-failed resource
    /// is not an absent one, so the caller preserves the local copy rather than
    /// emitting a `Destroyed`.
    pub fn failed_href(&self) -> Option<&str> {
        if self.committed.is_collection() || self.has_success_propstat || !self.saw_failed_propstat
        {
            return None;
        }
        self.href.as_deref()
    }

    /// The failed member of a LISTING lane, carrying the status the server gave
    /// it so [`classify_207`] can read the lane.
    ///
    /// The status is [`member_status_code`](Self::member_status_code), which is
    /// exactly the rule the sync lane already applies: a response-level code
    /// wins, and below it only a response whose every propstat failed reports
    /// a failed code. Without the status the listing lanes could report
    /// WHICH resources a 207 refused but never WHY, so an all-refused 207 came
    /// back as an empty page carrying a list of hrefs instead of a classified
    /// error, and a consumer recorded a completed walk over a collection it had
    /// been refused.
    pub fn failed_member(&self) -> Option<FailedResource> {
        Some(FailedResource {
            href: self.failed_href()?.to_string(),
            status: self.member_status_code(),
        })
    }

    /// The href and body of a multiget response that yielded usable data.
    pub fn fetched(&self) -> Option<(&str, &str)> {
        if self.committed.is_collection() {
            return None;
        }
        Some((self.href.as_deref()?, self.committed.resource_data()?))
    }

    /// A multiget response with an actual non-2xx status: the refused propstat
    /// code that describes the resource (see
    /// [`worst_failed_status`](Self::worst_failed_status)), or a non-2xx
    /// response-level code.
    // Accepted edge: a failed propstat whose status line is absent or
    // unparseable yields no numeric code here, so the resource degrades to the
    // benign missing-data lane and cannot contribute to a complete failure. A
    // pathological server failing every resource with garbage status text thus
    // reads as an empty success; tolerated because such a server violates RFC
    // 4918's required status line and the honest lanes still preserve the
    // resource.
    pub fn failed_resource(&self) -> Option<(&str, u16)> {
        if self.committed.is_collection() {
            return None;
        }
        let href = self.href.as_deref()?;
        let status = self.worst_failed_status().or_else(|| {
            self.response_status
                .as_deref()
                .and_then(status_line_code)
                .filter(|status| !(200..=299).contains(status))
        })?;
        Some((href, status))
    }

    /// A multiget response that succeeded but omitted the body property. That
    /// is a per-resource absence, not a DAV failure.
    pub fn missing_data_href(&self) -> Option<&str> {
        if self.committed.is_collection() || self.committed.resource_data().is_some() {
            return None;
        }
        self.href.as_deref()
    }
}

/// What a crate does with the elements the shared machine does not own.
///
/// The driver reads the document, keeps the element stack, accumulates text
/// (including CDATA), and owns `<response>`, `<propstat>`, `<href>` and both
/// `<status>` positions. Everything else is handed to the sink.
pub trait MultiStatusSink {
    /// The staged property bag this crate reads.
    type Props: PropSet;

    /// A start or empty element inside a `<response>`. Names include their
    /// resolved namespace, and `stack` holds the ancestors of `name`.
    fn element(
        &mut self,
        name: &XmlName,
        stack: &[XmlName],
        parts: &mut ResponseParts<Self::Props>,
    ) {
        let _ = (name, stack, parts);
    }

    /// A closing element inside a `<response>` with its accumulated text.
    /// `parent` is the enclosing element's resolved name.
    fn property(
        &mut self,
        parent: Option<&XmlName>,
        name: &XmlName,
        text: &str,
        parts: &mut ResponseParts<Self::Props>,
    ) {
        let _ = (parent, name, text, parts);
    }

    /// A closing element OUTSIDE any `<response>` - the document-level
    /// `<sync-token>`, for instance.
    fn document_property(&mut self, parent: Option<&XmlName>, name: &XmlName, text: &str) {
        let _ = (parent, name, text);
    }

    /// A `<response>` just closed. This is where the crate picks its lane.
    fn finish_response(&mut self, parts: &ResponseParts<Self::Props>);
}

/// Drive a 207 Multi-Status document through `sink`.
///
/// `Err` is reserved for a malformed document. A well-formed body whose
/// individual responses failed is NOT an error: those are per-resource
/// outcomes, and RFC 4918 s13 makes them the whole point of a 207.
pub fn parse_multistatus<S: MultiStatusSink>(xml: &str, sink: &mut S) -> Result<(), String> {
    let mut reader = NsReader::from_str(xml);
    let mut parts: ResponseParts<S::Props> = ResponseParts::default();
    let mut stack: Vec<XmlName> = Vec::new();
    let mut text = String::new();

    loop {
        let (resolved, event) = reader
            .read_resolved_event()
            .map_err(|error| format!("XML parse error: {error}"))?;
        let name = match &event {
            Event::Start(element) | Event::Empty(element) => {
                Some(resolved_xml_name(resolved, element.name().as_ref()))
            }
            Event::End(element) => Some(resolved_xml_name(resolved, element.name().as_ref())),
            _ => None,
        };
        match event {
            Event::Start(_) => {
                let name = name.as_ref().expect("element event has a resolved name");
                if name.is(DAV_NAMESPACE, "response") {
                    parts.begin_response();
                }
                if parts.in_response() {
                    if name.is(DAV_NAMESPACE, "propstat") {
                        parts.begin_propstat();
                    }
                    sink.element(name, &stack, &mut parts);
                }
                stack.push(name.clone());
                text.clear();
            }
            Event::Empty(_) => {
                let name = name.as_ref().expect("element event has a resolved name");
                if parts.in_response() {
                    // `<status/>` is the driver's element in either position,
                    // and an empty one never reaches the `End` arm that reads
                    // it. Left to the sink it set nothing, so `commit_propstat`
                    // saw an ABSENT status and committed the propstat as a
                    // success - the exact inverse of the rule: a status that is
                    // present and unparseable is a refusal, because it is not
                    // evidence that the property was returned.
                    if name.is(DAV_NAMESPACE, "status")
                        && stack
                            .last()
                            .is_some_and(|parent| parent.is(DAV_NAMESPACE, "propstat"))
                    {
                        parts.staged_status = None;
                        parts.staged_success = Some(false);
                    } else {
                        sink.element(name, &stack, &mut parts);
                    }
                }
                // Then the same text reset `Start` and `End` do, unconditionally
                // and after the status branch above, which reads no text. An
                // empty child is a child: `<getetag>abc<foo/>def</getetag>` must
                // yield the same `def` a non-empty `<foo></foo>` yields, or the
                // accumulated value depends on whether the server chose the
                // self-closing spelling. Safe for every property read through
                // this driver - all of them are PCDATA-only by their schema
                // (etag, ctag, sync-token, displayname, colour, content type,
                // calendar-data, address-data); the structured ones
                // (`resourcetype`, `privilege`, home sets) are read through
                // `element` and hrefs, never through accumulated text.
                text.clear();
            }
            Event::Text(value) => push_text(&mut text, value.as_ref())?,
            Event::CData(value) => text.push_str(&value),
            Event::End(_) => {
                let name = name.as_ref().expect("element event has a resolved name");
                let parent = stack.iter().rev().nth(1);
                if parts.in_response() {
                    if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "response"))
                        && name.is(DAV_NAMESPACE, "href")
                    {
                        parts.href = trimmed(&text);
                    } else if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "response"))
                        && name.is(DAV_NAMESPACE, "status")
                    {
                        parts.response_status = trimmed(&text);
                    } else if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "propstat"))
                        && name.is(DAV_NAMESPACE, "status")
                    {
                        parts.staged_status = trimmed(&text);
                        parts.staged_success = Some(status_line_is_success(&text));
                    } else {
                        sink.property(parent, name, &text, &mut parts);
                    }
                } else {
                    sink.document_property(parent, name, &text);
                }
                if name.is(DAV_NAMESPACE, "propstat") {
                    parts.commit_propstat();
                }
                if name.is(DAV_NAMESPACE, "response") {
                    parts.end_response();
                    sink.finish_response(&parts);
                }
                stack.pop();
                text.clear();
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(())
}

/// Extract every `<href>` nested inside `property_name`, honouring propstat
/// status: hrefs found inside a REFUSED propstat are discarded.
///
/// This source-compatible helper infers namespaces for the standard properties
/// known to the DAV clients. Use [`extract_href_properties_in_namespace`] for
/// an extension property or any name outside that set.
///
/// Nested `<status>` elements belonging to some other property inside the same
/// prop bag are ignored - only a `<status>` whose PARENT is the propstat says
/// anything about it.
pub fn extract_href_properties(xml: &str, property_name: &str) -> Result<Vec<String>, String> {
    extract_href_properties_in_namespace(xml, property_namespace(property_name), property_name)
}

/// Namespace-explicit form of [`extract_href_properties`]. Use this for a
/// property outside the standard name table, including vendor extensions.
pub fn extract_href_properties_in_namespace(
    xml: &str,
    property_namespace: &str,
    property_name: &str,
) -> Result<Vec<String>, String> {
    let mut reader = NsReader::from_str(xml);
    let mut stack: Vec<XmlName> = Vec::new();
    let mut text = String::new();
    let mut hrefs = Vec::new();
    let mut propstat_hrefs = Vec::new();
    let mut propstat_success = None;
    let target_namespace = property_namespace;
    loop {
        let (resolved, event) = reader
            .read_resolved_event()
            .map_err(|error| format!("XML parse error: {error}"))?;
        let name = match &event {
            Event::Start(element) | Event::Empty(element) => {
                Some(resolved_xml_name(resolved, element.name().as_ref()))
            }
            Event::End(element) => Some(resolved_xml_name(resolved, element.name().as_ref())),
            _ => None,
        };
        match event {
            Event::Start(_) => {
                stack.push(name.expect("element event has a resolved name"));
                text.clear();
            }
            // An empty `<status/>` is present and unparseable, so it refuses
            // the propstat. It never reaches the `End` arm below, and without
            // this the propstat committed under the absent-status-is-success
            // rule and published hrefs the server had refused.
            Event::Empty(_) => {
                let name = name.as_ref().expect("element event has a resolved name");
                if name.is(DAV_NAMESPACE, "status")
                    && stack
                        .last()
                        .is_some_and(|parent| parent.is(DAV_NAMESPACE, "propstat"))
                {
                    propstat_success = Some(false);
                }
                // Same reset the `Start` arm does. An empty child element opens
                // and closes a nested element, so text accumulated before it
                // belongs to that child's parent and not to the property being
                // read - without this, `<href>abc<x/>def</href>` reads `abcdef`
                // where the paired spelling of the same child reads `def`.
                text.clear();
            }
            Event::Text(value) => push_text(&mut text, value.as_ref())?,
            Event::CData(value) => text.push_str(&value),
            Event::End(_) => {
                let name = name.as_ref().expect("element event has a resolved name");
                if name.is(DAV_NAMESPACE, "href")
                    && stack
                        .iter()
                        .any(|tag| tag.is(target_namespace, property_name))
                    && let Some(href) = trimmed(&text)
                {
                    if stack.iter().any(|tag| tag.is(DAV_NAMESPACE, "propstat")) {
                        propstat_hrefs.push(href);
                    } else {
                        hrefs.push(href);
                    }
                }
                let parent = stack.iter().rev().nth(1);
                if name.is(DAV_NAMESPACE, "status")
                    && parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "propstat"))
                {
                    propstat_success = Some(
                        trimmed(&text)
                            .as_deref()
                            .is_some_and(status_line_is_success),
                    );
                }
                if name.is(DAV_NAMESPACE, "propstat") {
                    if propstat_success.unwrap_or(true) {
                        hrefs.append(&mut propstat_hrefs);
                    } else {
                        propstat_hrefs.clear();
                    }
                    propstat_success = None;
                }
                stack.pop();
                text.clear();
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(hrefs)
}

/// The first `<href>` inside `property_name`, under the same propstat rule as
/// [`extract_href_properties`]. Use [`extract_href_property_in_namespace`]
/// when the property is outside the standard name table.
pub fn extract_href_property(xml: &str, property_name: &str) -> Result<Option<String>, String> {
    extract_href_property_in_namespace(xml, property_namespace(property_name), property_name)
}

/// Namespace-explicit form of [`extract_href_property`].
pub fn extract_href_property_in_namespace(
    xml: &str,
    property_namespace: &str,
    property_name: &str,
) -> Result<Option<String>, String> {
    Ok(
        extract_href_properties_in_namespace(xml, property_namespace, property_name)?
            .into_iter()
            .next(),
    )
}

/// Read one text property out of a depth-0 PROPFIND, committing it only from a
/// successful propstat.
///
/// A stale value returned inside a refused propstat must not feed a cursor or a
/// change short-circuit: CalDAV reads `sync-token` this way and CardDAV reads
/// `getctag`, and a stale token committed from a 403 block suppresses a real
/// change.
///
/// This source-compatible helper infers namespaces for the standard properties
/// known to the DAV clients. Use [`parse_collection_property_in_namespace`]
/// for an extension property or any name outside that set.
pub fn parse_collection_property(xml: &str, property_name: &str) -> Result<Option<String>, String> {
    parse_collection_property_in_namespace(xml, property_namespace(property_name), property_name)
}

/// Namespace-explicit form of [`parse_collection_property`].
pub fn parse_collection_property_in_namespace(
    xml: &str,
    property_namespace: &str,
    property_name: &str,
) -> Result<Option<String>, String> {
    let mut reader = NsReader::from_str(xml);
    let mut stack: Vec<XmlName> = Vec::new();
    let mut text = String::new();
    let mut staged = None;
    let mut staged_success = None;
    let mut committed = None;
    let target_namespace = property_namespace;

    loop {
        let (resolved, event) = reader
            .read_resolved_event()
            .map_err(|error| format!("XML parse error: {error}"))?;
        let name = match &event {
            Event::Start(element) | Event::Empty(element) => {
                Some(resolved_xml_name(resolved, element.name().as_ref()))
            }
            Event::End(element) => Some(resolved_xml_name(resolved, element.name().as_ref())),
            _ => None,
        };
        match event {
            Event::Start(_) => {
                let name = name.as_ref().expect("element event has a resolved name");
                if name.is(DAV_NAMESPACE, "propstat") {
                    staged = None;
                    staged_success = None;
                }
                stack.push(name.clone());
                text.clear();
            }
            // Same rule as the driver: an empty `<status/>` is a present,
            // unparseable status, so the propstat is refused rather than
            // committing a stale token under the absent-status allowance.
            Event::Empty(_) => {
                let name = name.as_ref().expect("element event has a resolved name");
                if name.is(DAV_NAMESPACE, "status")
                    && stack
                        .last()
                        .is_some_and(|parent| parent.is(DAV_NAMESPACE, "propstat"))
                {
                    staged_success = Some(false);
                }
                // Same reset the `Start` arm does; see `extract_href_properties`
                // for why an empty child element must not leave its parent's
                // text glued to the text that follows it.
                text.clear();
            }
            Event::Text(value) => push_text(&mut text, value.as_ref())?,
            Event::CData(value) => text.push_str(&value),
            Event::End(_) => {
                let name = name.as_ref().expect("element event has a resolved name");
                let parent = stack.iter().rev().nth(1);
                if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
                    && name.is(target_namespace, property_name)
                {
                    staged = trimmed(&text);
                } else if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "propstat"))
                    && name.is(DAV_NAMESPACE, "status")
                {
                    staged_success = Some(status_line_is_success(&text));
                }
                if name.is(DAV_NAMESPACE, "propstat") {
                    if staged_success.unwrap_or(true)
                        && let Some(value) = staged.take()
                    {
                        committed = Some(value);
                    }
                    staged = None;
                    staged_success = None;
                }
                stack.pop();
                text.clear();
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(committed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xml::normalize_etag;

    /// A minimal member-listing prop set: the shape both crates' depth-1
    /// listings have, with nothing protocol-specific in it.
    #[derive(Default)]
    struct MemberProps {
        is_collection: bool,
        etag: Option<String>,
        data: Option<String>,
        truncation_marker: bool,
    }

    impl PropSet for MemberProps {
        fn commit_from(&mut self, staged: Self) {
            self.is_collection |= staged.is_collection;
            self.truncation_marker |= staged.truncation_marker;
            commit_if_present(&mut self.etag, staged.etag);
            commit_if_present(&mut self.data, staged.data);
        }

        fn is_collection(&self) -> bool {
            self.is_collection
        }

        fn resource_data(&self) -> Option<&str> {
            self.data.as_deref()
        }
    }

    #[derive(Default)]
    struct MemberSink {
        entries: Vec<(String, Option<String>)>,
        failed_hrefs: Vec<String>,
        failed_members: Vec<FailedResource>,
        collections: Vec<(String, Option<u16>)>,
        member_statuses: Vec<(String, Option<u16>)>,
        fetched: Vec<(String, String)>,
        missing_data: Vec<String>,
        failed_resources: Vec<(String, u16)>,
    }

    impl MultiStatusSink for MemberSink {
        type Props = MemberProps;

        fn element(
            &mut self,
            name: &XmlName,
            stack: &[XmlName],
            parts: &mut ResponseParts<Self::Props>,
        ) {
            if name.is(DAV_NAMESPACE, "collection")
                && stack
                    .iter()
                    .any(|item| item.is(DAV_NAMESPACE, "resourcetype"))
            {
                parts.marker_mut().is_collection = true;
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
                && name.is(DAV_NAMESPACE, "getetag")
            {
                parts.staged_mut().etag = normalize_etag(text);
            } else if parent.is_some_and(|parent| parent.is(DAV_NAMESPACE, "prop"))
                && name.is(DAV_NAMESPACE, "data")
            {
                parts.staged_mut().data = trimmed(text);
            }
        }

        // Deliberately consults every lane rather than short-circuiting on the
        // collection: the lane helpers themselves must exclude the
        // collection's own response, and a sink that returned early here would
        // hide it if they stopped.
        fn finish_response(&mut self, parts: &ResponseParts<Self::Props>) {
            if parts.props().is_collection() {
                self.collections.push((
                    parts.href().unwrap_or_default().to_string(),
                    parts.member_status_code(),
                ));
            } else {
                self.member_statuses.push((
                    parts.href().unwrap_or_default().to_string(),
                    parts.member_status_code(),
                ));
            }
            if let Some((href, data)) = parts.fetched() {
                self.fetched.push((href.to_string(), data.to_string()));
            }
            if let Some(href) = parts.entry_href() {
                self.entries
                    .push((href.to_string(), parts.props().etag.clone()));
            } else if let Some(href) = parts.failed_href() {
                self.failed_hrefs.push(href.to_string());
            }
            if let Some(failure) = parts.failed_member() {
                self.failed_members.push(failure);
            }
            if let Some((href, status)) = parts.failed_resource() {
                self.failed_resources.push((href.to_string(), status));
            }
            if let Some(href) = parts.missing_data_href() {
                self.missing_data.push(href.to_string());
            }
        }
    }

    fn run(xml: &str) -> MemberSink {
        let mut sink = MemberSink::default();
        parse_multistatus(xml, &mut sink).expect("valid multistatus");
        sink
    }

    /// Rule one: a property echoed back inside a REFUSED propstat is not
    /// evidence about the resource, so it never reaches the committed set.
    #[test]
    fn a_staged_property_under_a_failed_propstat_is_discarded() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/one</D:href>
          <D:propstat><D:prop><D:data>body</D:data></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:getetag>"stale"</D:getetag></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(sink.entries, vec![("/c/one".to_string(), None)]);
        assert!(sink.failed_hrefs.is_empty());
    }

    /// Rule three, first half: a response with NO propstat at all is still a
    /// member the server named. Withholding it drops the resource out of both
    /// lanes and the snapshot diff then destroys a resource that exists.
    #[test]
    fn a_propstat_less_response_commits_as_an_entry() {
        let sink = run(
            r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href>/c/bare</D:href></D:response></D:multistatus>"#,
        );

        assert_eq!(sink.entries, vec![("/c/bare".to_string(), None)]);
        assert!(sink.failed_hrefs.is_empty());
    }

    /// Rule three, second half: a response whose ONLY propstat failed lands in
    /// the failed lane, not the entry lane.
    #[test]
    fn a_failed_only_response_lands_in_the_failed_href_lane() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/gone</D:href>
          <D:propstat><D:prop><D:getetag>"x"</D:getetag></D:prop>
          <D:status>HTTP/1.1 503 Unavailable</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert!(sink.entries.is_empty());
        assert_eq!(sink.failed_hrefs, vec!["/c/gone".to_string()]);
    }

    /// The collection's own response is not a member, and its RFC 6578 s3.6
    /// `507` is a statement about the REPORT rather than about any resource.
    /// Left in the member lanes it becomes a phantom entry whose truncation
    /// marker is never read.
    #[test]
    fn the_collection_self_response_carries_the_truncation_marker() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/</D:href>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:status>HTTP/1.1 507 Insufficient Storage</D:status>
          </D:response></D:multistatus>"#);

        assert!(sink.entries.is_empty());
        assert!(sink.failed_hrefs.is_empty());
        assert_eq!(sink.collections, vec![("/c/".to_string(), Some(507))]);
    }

    /// A propstat `404` is a per-PROPERTY miss, not a removed member. A server
    /// answers it for any requested property it lacks, beside the `200`
    /// propstat carrying the etag; reading the first propstat's code as the
    /// member status turned that into a `Destroyed` for a live resource.
    /// Existence is decided by a response-level status; below that, any
    /// successful propstat makes the member successful, and only an
    /// all-failed response reports its first failed code.
    #[test]
    fn a_failed_property_propstat_beside_a_successful_one_is_not_a_member_failure() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/live</D:href>
          <D:propstat><D:prop><D:displayname/></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          <D:propstat><D:prop><D:getetag>"e1"</D:getetag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          </D:response><D:response>
          <D:href>/c/removed</D:href>
          <D:status>HTTP/1.1 404 Not Found</D:status>
          </D:response><D:response>
          <D:href>/c/refused</D:href>
          <D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 403 Forbidden</D:status></D:propstat>
          <D:propstat><D:prop><D:displayname/></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(
            sink.member_statuses,
            vec![
                ("/c/live".to_string(), None),
                ("/c/removed".to_string(), Some(404)),
                ("/c/refused".to_string(), Some(403)),
            ]
        );
        // The entry lane does not read status (a propstat-less response is
        // still a named member); the sync caller consults the member status
        // above before upserting, which is why the two are reported apart.
        assert_eq!(
            sink.entries,
            vec![
                ("/c/live".to_string(), Some("e1".to_string())),
                ("/c/removed".to_string(), None),
            ]
        );
        assert_eq!(sink.failed_hrefs, vec!["/c/refused".to_string()]);
    }

    /// A `<collection/>` inside a REFUSED propstat is a prop skeleton echo,
    /// not a resourcetype: the resource stays a member.
    #[test]
    fn a_collection_marker_under_a_failed_propstat_is_not_authoritative() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/real</D:href>
          <D:propstat><D:prop><D:getetag>"live"</D:getetag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(
            sink.entries,
            vec![("/c/real".to_string(), Some("live".to_string()))]
        );
    }

    /// A present-but-unparseable status is a refusal: it is not evidence that
    /// the property beside it was returned.
    #[test]
    fn a_present_but_unparseable_status_does_not_commit() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/one</D:href>
          <D:propstat><D:prop><D:data>body</D:data></D:prop>
          <D:status>HTTP/1.1 OK</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert!(sink.fetched.is_empty());
        assert_eq!(sink.failed_hrefs, vec!["/c/one".to_string()]);
    }

    /// A 2xx response that simply omitted the body property is a per-resource
    /// absence, not a DAV failure.
    #[test]
    fn a_successful_response_without_data_is_a_missing_data_href() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/empty</D:href>
          <D:propstat><D:prop><D:getetag>"e"</D:getetag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert!(sink.fetched.is_empty());
        assert_eq!(sink.missing_data, vec!["/c/empty".to_string()]);
    }

    /// The listing failure lane carries the member STATUS, not just the href,
    /// which is what makes an all-refused 207 classifiable from a lane that
    /// hydrates nothing. Without it the candidate lane of a filtered query
    /// could report WHICH resources a server refused but never WHY, so an
    /// all-refused query read as an empty page.
    #[test]
    fn a_failed_member_carries_the_status_that_classifies_it() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/refused</D:href>
          <D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 507 Insufficient Storage</D:status></D:propstat>
          </D:response><D:response>
          <D:href>/c/gone</D:href>
          <D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(
            sink.failed_members,
            vec![
                FailedResource {
                    href: "/c/refused".to_string(),
                    status: Some(507),
                },
                FailedResource {
                    href: "/c/gone".to_string(),
                    status: Some(404),
                },
            ]
        );
    }

    /// The one RFC 4918 s13 ladder, which both member lanes now read. The
    /// mixed case is the one that must not over-reach: a refusal beside a
    /// success stays per-resource news.
    #[test]
    fn an_all_failed_207_classifies_where_a_mixed_one_stays_usable() {
        let refused = FailedResource {
            href: "/c/refused".to_string(),
            status: Some(403),
        };
        let gone = FailedResource {
            href: "/c/gone".to_string(),
            status: Some(410),
        };

        assert_eq!(
            classify_207(false, std::slice::from_ref(&refused)),
            MultiStatusOutcome::CompleteFailure { status: Some(403) }
        );
        // A resource deleted between the listing and the fetch is the benign
        // per-resource case, however many of them there are.
        assert_eq!(
            classify_207(false, std::slice::from_ref(&gone)),
            MultiStatusOutcome::Usable
        );
        // Anything usable in the body makes the failures per-resource news.
        assert_eq!(
            classify_207(true, &[refused, gone]),
            MultiStatusOutcome::Usable
        );
        // An empty body is a legitimate empty result, not a failure.
        assert_eq!(classify_207(false, &[]), MultiStatusOutcome::Usable);
    }

    /// Document order must not decide the member status. A server that writes
    /// its per-property `404` propstat ahead of the `403` that describes the
    /// resource made the member read as a benign missing resource, so an
    /// all-refused 207 classified `Usable` with no entries - an empty snapshot
    /// whose diff destroys every resource in the collection.
    #[test]
    fn a_missing_property_propstat_does_not_mask_the_refusal_beside_it() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/refused</D:href>
          <D:propstat><D:prop><D:getcontenttype/></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          <D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 403 Forbidden</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(
            sink.member_statuses,
            vec![("/c/refused".to_string(), Some(403))]
        );
        assert_eq!(
            sink.failed_members,
            vec![FailedResource {
                href: "/c/refused".to_string(),
                status: Some(403),
            }]
        );
        // The consequence the ordering bug produced: read as 404 this lane
        // classified Usable and the caller minted an empty snapshot.
        assert_eq!(
            classify_207(false, &sink.failed_members),
            MultiStatusOutcome::CompleteFailure { status: Some(403) }
        );
        // The multiget lane reads the same ladder from the same codes.
        assert_eq!(sink.failed_resources, vec![("/c/refused".to_string(), 403)]);
    }

    /// An all-404 response has no refusal to prefer, so document order stands
    /// and the benign missing-resource reading survives.
    #[test]
    fn an_all_missing_response_still_reports_its_missing_status() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/gone</D:href>
          <D:propstat><D:prop><D:getcontenttype/></D:prop>
          <D:status>HTTP/1.1 410 Gone</D:status></D:propstat>
          <D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(
            sink.member_statuses,
            vec![("/c/gone".to_string(), Some(410))]
        );
        assert_eq!(
            classify_207(false, &sink.failed_members),
            MultiStatusOutcome::Usable
        );
    }

    /// An empty-element `<status/>` is PRESENT and unparseable, so it refuses
    /// its propstat. Routed to the sink as a bare element it set nothing, and
    /// `commit_propstat` then committed the block under the
    /// absent-status-is-success allowance.
    #[test]
    fn an_empty_status_element_refuses_its_propstat() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/one</D:href>
          <D:propstat><D:prop><D:data>body</D:data></D:prop>
          <D:status/></D:propstat>
          </D:response></D:multistatus>"#);

        assert!(sink.entries.is_empty());
        assert!(sink.fetched.is_empty());
        assert_eq!(sink.failed_hrefs, vec!["/c/one".to_string()]);
    }

    #[test]
    fn a_foreign_status_does_not_override_the_missing_dav_status_rule() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:" xmlns:X="urn:vendor">
          <D:response><D:href>/c/one</D:href>
          <D:propstat><D:prop><D:getetag>"live"</D:getetag></D:prop>
          <X:status>HTTP/1.1 403 Forbidden</X:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(
            sink.entries,
            vec![("/c/one".to_string(), Some("live".to_string()))]
        );
        assert!(sink.failed_hrefs.is_empty());
    }

    #[test]
    fn default_namespace_less_dav_elements_are_accepted() {
        let sink = run(r#"<multistatus><response><href>/c/one</href>
          <propstat><prop><getetag>live</getetag></prop>
          <status>HTTP/1.1 200 OK</status></propstat>
          </response></multistatus>"#);

        assert_eq!(
            sink.entries,
            vec![("/c/one".to_string(), Some("live".to_string()))]
        );
    }

    #[test]
    fn undeclared_dav_prefix_is_tolerated_without_claiming_other_prefixes() {
        let sink = run(r#"<D:multistatus><D:response><D:href>/c/one</D:href>
          <D:propstat><D:prop><D:getetag>live</D:getetag></D:prop>
          <X:status>HTTP/1.1 403 Forbidden</X:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(
            sink.entries,
            vec![("/c/one".to_string(), Some("live".to_string()))],
            "the conventional undeclared D prefix resolves as DAV, while an unknown X prefix does not"
        );
        assert!(sink.failed_hrefs.is_empty());
    }

    /// The same hole in the href extractor: hrefs inside a propstat whose
    /// status is an empty element were published as though the block had
    /// succeeded, which is how a refused home-set or address-set reads as a
    /// discovered one.
    #[test]
    fn an_empty_status_element_discards_the_propstat_hrefs() {
        let xml = r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href>/p/</D:href>
          <D:propstat><D:prop><D:calendar-home-set><D:href>/home/</D:href></D:calendar-home-set></D:prop>
          <D:status/></D:propstat>
          </D:response></D:multistatus>"#;

        assert!(
            extract_href_properties_in_namespace(
                xml,
                "urn:ietf:params:xml:ns:caldav",
                "calendar-home-set",
            )
            .expect("valid XML")
            .is_empty()
        );
    }

    #[test]
    fn href_extraction_requires_the_property_and_href_namespaces() {
        let xml = r#"<D:multistatus xmlns:D="DAV:"
            xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:X="urn:vendor">
          <D:response><D:propstat><D:prop>
            <X:calendar-home-set><D:href>/wrong-property</D:href></X:calendar-home-set>
            <C:calendar-home-set><X:href>/wrong-href</X:href><D:href>/right</D:href></C:calendar-home-set>
          </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
        </D:multistatus>"#;

        assert_eq!(
            extract_href_properties_in_namespace(
                xml,
                "urn:ietf:params:xml:ns:caldav",
                "calendar-home-set",
            )
            .expect("valid XML"),
            vec!["/right".to_string()]
        );
    }

    #[test]
    fn href_extraction_accepts_a_caller_supplied_extension_namespace() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:V="urn:vendor:future">
          <D:response><D:propstat><D:prop>
            <D:future-home><D:href>/wrong</D:href></D:future-home>
            <V:future-home><D:href>/right</D:href></V:future-home>
          </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
        </D:multistatus>"#;

        assert_eq!(
            extract_href_properties_in_namespace(xml, "urn:vendor:future", "future-home")
                .expect("valid XML"),
            vec!["/right".to_string()]
        );
    }

    #[test]
    fn collection_property_accepts_a_caller_supplied_extension_namespace() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:V="urn:vendor:future">
          <D:response><D:propstat><D:prop>
            <D:future-token>wrong</D:future-token>
            <V:future-token>right</V:future-token>
          </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
        </D:multistatus>"#;

        assert_eq!(
            parse_collection_property_in_namespace(xml, "urn:vendor:future", "future-token")
                .expect("valid XML")
                .as_deref(),
            Some("right")
        );
    }

    /// And in the depth-0 token read, where committing under a refused
    /// propstat is a stale `sync-token` / `getctag` that suppresses a real
    /// change.
    #[test]
    fn an_empty_status_element_discards_the_collection_property() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
          <D:response><D:href>/c/</D:href>
          <D:propstat><D:prop><CS:getctag>stale</CS:getctag></D:prop>
          <D:status/></D:propstat>
          </D:response></D:multistatus>"#;

        assert_eq!(
            parse_collection_property_in_namespace(xml, "http://calendarserver.org/ns/", "getctag")
                .expect("valid XML"),
            None
        );
    }

    /// An empty child element resets the text accumulator exactly as a
    /// `Start` does. Without it the spelling of a child decides the value:
    /// `<data>abc<x/>def</data>` accumulated `abcdef` while the identical
    /// `<data>abc<x></x>def</data>` yielded `def`.
    ///
    /// Ablation: drop the `text.clear()` from the `Empty` arm and the first
    /// assertion reads `abcdef`.
    #[test]
    fn an_empty_child_element_resets_the_text_accumulator() {
        let sink = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/one</D:href>
          <D:propstat><D:prop><D:data>abc<D:x/>def</D:data></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(
            sink.fetched,
            vec![("/c/one".to_string(), "def".to_string())],
            "the empty child resets the accumulator, as a non-empty one does"
        );

        let paired = run(r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:href>/c/one</D:href>
          <D:propstat><D:prop><D:data>abc<D:x></D:x>def</D:data></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          </D:response></D:multistatus>"#);

        assert_eq!(
            sink.fetched, paired.fetched,
            "the self-closing spelling of a child cannot change the value"
        );
    }

    /// The same reset, in the two functions that keep their own accumulator.
    ///
    /// `parse_multistatus` is pinned above; these two were the sibling arms
    /// that had the identical hole, which is the drift shape a fix to one
    /// reader and not its neighbours leaves behind.
    #[test]
    fn an_empty_child_element_resets_the_text_of_the_standalone_readers() {
        let href = r#"<D:multistatus xmlns:D="DAV:"><D:response>
          <D:propstat><D:prop><D:owner><D:href>/p/<D:x/>real</D:href></D:owner></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        assert_eq!(
            extract_href_properties_in_namespace(href, DAV_NAMESPACE, "owner").expect("valid XML"),
            vec!["real".to_string()],
            "the empty child resets the accumulator, as a non-empty one does"
        );

        let ctag = r#"<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
          <D:response><D:href>/c/</D:href>
          <D:propstat><D:prop><CS:getctag>stale<D:x/>fresh</CS:getctag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        assert_eq!(
            parse_collection_property_in_namespace(
                ctag,
                "http://calendarserver.org/ns/",
                "getctag"
            )
            .expect("valid XML"),
            Some("fresh".to_string()),
            "and the collection reader agrees with both of its siblings"
        );
    }

    #[test]
    fn collection_property_commits_only_from_a_successful_propstat() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
          <D:response><D:href>/c/</D:href>
          <D:propstat><D:prop><CS:getctag>stale</CS:getctag></D:prop>
          <D:status>HTTP/1.1 403 Forbidden</D:status></D:propstat>
          <D:propstat><D:prop><CS:getctag>live</CS:getctag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        assert_eq!(
            parse_collection_property_in_namespace(xml, "http://calendarserver.org/ns/", "getctag")
                .expect("valid XML"),
            Some("live".to_string())
        );
    }
}
