pub(crate) use bifrost_dav_core::extract_href_property;
pub(crate) use bifrost_dav_core::resolve_href;
use bifrost_dav_core::{
    MultiStatusSink, PropSet, ResponseParts, classify_207, commit_if_present, normalize_etag,
    parse_collection_property, parse_multistatus, trimmed,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CardDavContactEntry {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
}

/// Outcome of a depth-1 contact PROPFIND or an `addressbook-query`: the
/// resources whose propstat succeeded (`entries`) plus the resources the server
/// reported *failed* within the 207 (a non-2xx propstat). A failed resource is
/// a transiently-failed one, not an absent one - the snapshot diff preserves
/// the local copy rather than emitting a Destroyed.
///
/// The failure lane carries each resource's STATUS, not just its href, so this
/// listing runs through the same RFC 4918 s13 ladder the multiget lanes use;
/// see the CalDAV twin for why an all-refused candidate 207 must not read as an
/// empty page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CardDavContactListing {
    pub(crate) entries: Vec<CardDavContactEntry>,
    pub(crate) failed: Vec<CardDavFailedResource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CardDavFetchedVCard {
    pub(crate) uri: String,
    pub(crate) etag: Option<String>,
    pub(crate) data: String,
}

/// One resource inside a 207 that yielded nothing usable, with the status the
/// server gave it. Both member lanes use it - the multiget for a resource with
/// no `address-data`, the listing for a resource whose every propstat failed -
/// so both classify through the same ladder.
pub(crate) type CardDavFailedResource = bifrost_dav_core::FailedResource;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CardDavMultigetReport {
    pub(crate) cards: Vec<CardDavFetchedVCard>,
    pub(crate) failed: Vec<CardDavFailedResource>,
    /// Resources that returned a successful response but omitted the
    /// requested `address-data`. They are per-resource absences, not DAV
    /// failures, so they surface through `Page::failed_ids` but cannot turn
    /// a 207 into a synthetic server error.
    pub(crate) missing_data: Vec<String>,
}

/// What a parsed 207 body actually represents, per RFC 4918 s13.
pub(crate) type MultigetOutcome = bifrost_dav_core::MultiStatusOutcome;

impl CardDavMultigetReport {
    pub(crate) fn extend(&mut self, other: CardDavMultigetReport) {
        self.cards.extend(other.cards);
        self.failed.extend(other.failed);
        self.missing_data.extend(other.missing_data);
    }

    pub(crate) fn classify(&self) -> MultigetOutcome {
        classify_207(!self.cards.is_empty(), &self.failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AddressBookCollection {
    pub(crate) href: String,
    pub(crate) display_name: Option<String>,
    pub(crate) ctag: Option<String>,
    /// Writability derived from `current-user-privilege-set`: `Some(false)`
    /// when the server answered the property and named no write privilege,
    /// `None` when it did not answer it at all (in which case the account
    /// assumes writable, as CalDAV does). A read-only shared address book
    /// used to advertise writable unconditionally, so a consumer's capability
    /// gate passed a PUT the server was always going to 403.
    pub(crate) can_edit: Option<bool>,
    pub(crate) is_directory: bool,
}

impl AddressBookCollection {
    pub(crate) fn resolve_href(&mut self, request_url: &str) {
        self.href = resolve_href(request_url, &self.href);
    }
}

impl CardDavContactListing {
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
    /// siblings were refused.
    pub(crate) fn classify(&self) -> MultigetOutcome {
        classify_207(!self.entries.is_empty(), &self.failed)
    }
}

impl CardDavMultigetReport {
    pub(crate) fn resolve_hrefs(&mut self, request_url: &str) {
        for card in &mut self.cards {
            card.uri = resolve_href(request_url, &card.uri);
        }
        for failed in &mut self.failed {
            failed.href = resolve_href(request_url, &failed.href);
        }
        for href in &mut self.missing_data {
            *href = resolve_href(request_url, href);
        }
    }
}

/// Properties an address-book PROPFIND stages. The `<addressbook/>`
/// resourcetype marker and the privilege markers are staged like every value
/// property: a `resourcetype` or `current-user-privilege-set` the server
/// REFUSED says nothing about the collection.
#[derive(Default)]
struct AddressBookProps {
    is_addressbook: bool,
    is_directory: bool,
    privilege_seen: bool,
    write_seen: bool,
    display_name: Option<String>,
    ctag: Option<String>,
}

impl PropSet for AddressBookProps {
    fn commit_from(&mut self, staged: Self) {
        self.is_addressbook |= staged.is_addressbook;
        self.is_directory |= staged.is_directory;
        self.privilege_seen |= staged.privilege_seen;
        self.write_seen |= staged.write_seen;
        commit_if_present(&mut self.display_name, staged.display_name);
        commit_if_present(&mut self.ctag, staged.ctag);
    }
}

/// Properties the contact-resource lanes stage: the depth-1 listing and the
/// `addressbook-multiget` REPORT both read from this set.
#[derive(Default)]
struct ContactProps {
    is_collection: bool,
    etag: Option<String>,
    content_type: Option<String>,
    address_data: Option<String>,
}

impl PropSet for ContactProps {
    fn commit_from(&mut self, staged: Self) {
        self.is_collection |= staged.is_collection;
        commit_if_present(&mut self.etag, staged.etag);
        commit_if_present(&mut self.content_type, staged.content_type);
        commit_if_present(&mut self.address_data, staged.address_data);
    }

    fn is_collection(&self) -> bool {
        self.is_collection
    }

    fn resource_data(&self) -> Option<&str> {
        self.address_data.as_deref()
    }
}

/// Mark `<collection/>` inside a `<resourcetype>`, the guard both member lanes
/// share.
fn mark_collection(name: &str, stack: &[String], parts: &mut ResponseParts<ContactProps>) {
    if name == "collection" && stack.iter().any(|item| item == "resourcetype") {
        parts.marker_mut().is_collection = true;
    }
}

#[derive(Default)]
struct AddressBookSink {
    collections: Vec<AddressBookCollection>,
}

impl MultiStatusSink for AddressBookSink {
    type Props = AddressBookProps;

    fn element(&mut self, name: &str, stack: &[String], parts: &mut ResponseParts<Self::Props>) {
        // The `resourcetype` ancestor guard is not decoration: without it an
        // `<addressbook/>` named anywhere else in the prop bag - inside
        // `<D:owner>`, or in a server extension element - reads as the
        // collection's own resourcetype and mints a phantom address book. The
        // CalDAV twin has always guarded its `<calendar/>` marker; this side
        // had not, which is the drift the collapse removes.
        if name == "addressbook" && stack.iter().any(|item| item == "resourcetype") {
            parts.marker_mut().is_addressbook = true;
        }
        // Directory resourcetype is defined by
        // draft-daboo-carddav-directory-gateway-02, not RFC 6352.
        if name == "directory" && stack.iter().any(|item| item == "resourcetype") {
            parts.marker_mut().is_directory = true;
        }
        if name == "privilege" {
            parts.marker_mut().privilege_seen = true;
        }
        if (name == "write" || name == "write-content" || name == "all")
            && stack.iter().any(|item| item == "privilege")
        {
            parts.marker_mut().write_seen = true;
        }
    }

    fn property(
        &mut self,
        parent: Option<&str>,
        name: &str,
        text: &str,
        parts: &mut ResponseParts<Self::Props>,
    ) {
        match (parent, name) {
            (Some("prop"), "displayname") => parts.staged_mut().display_name = trimmed(text),
            (Some("prop"), "getctag") => parts.staged_mut().ctag = trimmed(text),
            _ => {}
        }
    }

    fn finish_response(&mut self, parts: &ResponseParts<Self::Props>) {
        let props = parts.props();
        if !props.is_addressbook && !props.is_directory {
            return;
        }
        let Some(href) = parts.href() else {
            return;
        };
        self.collections.push(AddressBookCollection {
            href: href.to_string(),
            display_name: props.display_name.clone(),
            ctag: props.ctag.clone(),
            can_edit: props.privilege_seen.then_some(props.write_seen),
            is_directory: props.is_directory,
        });
    }
}

pub(crate) fn parse_addressbook_collections(
    xml: &str,
) -> Result<Vec<AddressBookCollection>, String> {
    let mut sink = AddressBookSink::default();
    parse_multistatus(xml, &mut sink)?;
    Ok(sink.collections)
}

#[derive(Default)]
struct ContactListingSink {
    listing: CardDavContactListing,
}

impl MultiStatusSink for ContactListingSink {
    type Props = ContactProps;

    fn element(&mut self, name: &str, stack: &[String], parts: &mut ResponseParts<Self::Props>) {
        mark_collection(name, stack, parts);
    }

    fn property(
        &mut self,
        parent: Option<&str>,
        name: &str,
        text: &str,
        parts: &mut ResponseParts<Self::Props>,
    ) {
        match (parent, name) {
            (Some("prop"), "getetag") => parts.staged_mut().etag = normalize_etag(text),
            (Some("prop"), "getcontenttype") => parts.staged_mut().content_type = trimmed(text),
            _ => {}
        }
    }

    fn finish_response(&mut self, parts: &ResponseParts<Self::Props>) {
        if let Some(href) = parts.entry_href() {
            self.listing.entries.push(CardDavContactEntry {
                uri: href.to_string(),
                etag: parts.props().etag.clone(),
            });
        } else if let Some(failure) = parts.failed_member() {
            self.listing.failed.push(failure);
        }
    }
}

pub(crate) fn parse_propfind_contacts(xml: &str) -> Result<CardDavContactListing, String> {
    let mut sink = ContactListingSink::default();
    parse_multistatus(xml, &mut sink)?;
    Ok(sink.listing)
}

#[derive(Default)]
struct MultigetSink {
    report: CardDavMultigetReport,
    directory_request_url: Option<String>,
    truncated: bool,
}

impl MultiStatusSink for MultigetSink {
    type Props = ContactProps;

    fn element(&mut self, name: &str, stack: &[String], parts: &mut ResponseParts<Self::Props>) {
        mark_collection(name, stack, parts);
    }

    fn property(
        &mut self,
        parent: Option<&str>,
        name: &str,
        text: &str,
        parts: &mut ResponseParts<Self::Props>,
    ) {
        match (parent, name) {
            (Some("prop"), "getetag") => parts.staged_mut().etag = normalize_etag(text),
            (Some("prop"), "address-data") => parts.staged_mut().address_data = trimmed(text),
            _ => {}
        }
    }

    fn finish_response(&mut self, parts: &ResponseParts<Self::Props>) {
        // RFC 6352 section 6.3.2: a response-level 507 for the request-URI
        // is truncation, whether or not number-of-matches-within-limits appears.
        // Consume it before ordinary per-resource quota/error classification.
        if let (Some(base), Some(href), Some(status)) = (
            self.directory_request_url.as_deref(),
            parts.href(),
            parts.response_status(),
        ) && status.split_whitespace().nth(1) == Some("507")
            && bifrost_dav_core::same_dav_url(base, &resolve_href(base, href))
        {
            self.truncated = true;
            return;
        }
        if let Some((href, data)) = parts.fetched() {
            self.report.cards.push(CardDavFetchedVCard {
                uri: href.to_string(),
                etag: parts.props().etag.clone(),
                data: data.to_string(),
            });
        } else if let Some((href, status)) = parts.failed_resource() {
            self.report.failed.push(CardDavFailedResource {
                href: href.to_string(),
                status: Some(status),
            });
        } else if let Some(href) = parts.missing_data_href() {
            self.report.missing_data.push(href.to_string());
        }
    }
}

pub(crate) fn parse_multiget_report(xml: &str) -> Result<CardDavMultigetReport, String> {
    let mut sink = MultigetSink::default();
    parse_multistatus(xml, &mut sink)?;
    Ok(sink.report)
}

pub(crate) fn parse_directory_report(
    xml: &str,
    request_url: &str,
) -> Result<(CardDavMultigetReport, bool), String> {
    let mut sink = MultigetSink {
        directory_request_url: Some(request_url.to_string()),
        ..Default::default()
    };
    parse_multistatus(xml, &mut sink)?;
    sink.report.resolve_hrefs(request_url);
    Ok((sink.report, sink.truncated))
}

/// Extract the collection `getctag` value from a depth-0 PROPFIND
/// response. Returns `None` when the server omits `getctag` (the caller
/// then falls through to a full snapshot + diff).
///
/// A stale `getctag` returned inside a failed (non-2xx) propstat must not feed
/// the short-circuit, or a server emitting an old ctag in a failed block would
/// suppress a real change; the shared reader commits only from a successful
/// propstat, exactly as the depth-1 parser does.
pub(crate) fn parse_collection_ctag(xml: &str) -> Result<Option<String>, String> {
    parse_collection_property(xml, "getctag")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hrefs_resolve_against_the_request_uri() {
        assert_eq!(
            resolve_href("https://books.example.test/homes/ada/", "team/one.vcf"),
            "https://books.example.test/homes/ada/team/one.vcf"
        );
    }

    /// The migration to request-URI resolution must not respell ids that
    /// were already correct. Most servers emit absolute-path hrefs, and the
    /// pre-migration base was `CardDavConfig::base_url` with its trailing
    /// slash trimmed. Pin both spellings against the post-migration request
    /// URI, and pin the literal so a regression cannot pass by changing both
    /// sides at once.
    #[test]
    fn absolute_path_href_keeps_the_common_deployment_id() {
        let href = "/addressbooks/ada/one.vcf";
        let previous = resolve_href("https://dav.example.test", href);
        assert_eq!(
            previous,
            "https://dav.example.test/addressbooks/ada/one.vcf"
        );
        assert_eq!(
            resolve_href("https://dav.example.test/addressbooks/ada/", href),
            previous
        );
        assert_eq!(
            resolve_href("https://dav.example.test/dav/users/ada/addressbook/", href),
            previous
        );
    }

    #[test]
    fn propfind_contacts_extracts_vcards_and_etags() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"abc"</D:getetag>
        <D:getcontenttype>text/vcard; charset=utf-8</D:getcontenttype>
      </D:prop>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/contacts/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
        <D:getetag>"collection"</D:getetag>
        <D:getcontenttype>httpd/unix-directory</D:getcontenttype>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(
            listing.entries,
            vec![CardDavContactEntry {
                uri: "/contacts/card-1.vcf".to_string(),
                etag: Some("abc".to_string()),
            }]
        );
        assert!(listing.failed.is_empty());
    }

    #[test]
    fn propfind_contacts_accepts_extensionless_resources_and_skips_collections() {
        let xml = r#"<D:multistatus xmlns:D="DAV:">
          <D:response><D:href>/contacts/opaque-id</D:href><D:propstat><D:prop>
          <D:resourcetype/><D:getetag>"abc"</D:getetag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
          <D:response><D:href>/contacts/child</D:href><D:propstat><D:prop>
          <D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
          </D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].uri, "/contacts/opaque-id");
    }

    #[test]
    fn propfind_contacts_ignores_failed_propstat_values() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"missing"</D:getetag>
        <D:getcontenttype>text/vcard</D:getcontenttype>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        // A 404-propstat resource is not committed as an entry, but it
        // IS surfaced as a failed href so the snapshot diff can preserve
        // the local copy instead of destroying it.
        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert!(listing.entries.is_empty());
        assert_eq!(
            listing.failed_hrefs(),
            vec!["/contacts/card-1.vcf".to_string()]
        );
        // The lane carries the STATUS as well as the href, which is what makes
        // an all-refused 207 classifiable. A 404 is the benign per-resource
        // case, so this listing still reads as usable.
        assert_eq!(listing.failed[0].status, Some(404));
        assert_eq!(listing.classify(), MultigetOutcome::Usable);
    }

    #[test]
    fn propfind_contacts_preserves_failed_extensionless_resource_but_not_collection() {
        let xml = r#"<D:multistatus xmlns:D="DAV:">
          <D:response><D:href>/contacts/opaque-id</D:href><D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 503 Unavailable</D:status></D:propstat></D:response>
          <D:response><D:href>/contacts/child</D:href>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:getetag/></D:prop>
          <D:status>HTTP/1.1 503 Unavailable</D:status></D:propstat></D:response>
          </D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.failed_hrefs(), vec!["/contacts/opaque-id"]);
    }

    /// A `resourcetype` block the server REFUSED says nothing about the
    /// resource. Servers routinely echo the requested prop skeleton back
    /// inside a 404 propstat, and some echo a `<collection/>` child with
    /// it; treating that as authoritative discarded a real contact whose
    /// own properties came back 200.
    #[test]
    fn propfind_contacts_ignores_collection_marker_in_a_failed_propstat() {
        let xml = r#"<D:multistatus xmlns:D="DAV:">
          <D:response><D:href>/contacts/opaque-id</D:href>
          <D:propstat><D:prop><D:getetag>"abc"</D:getetag></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat></D:response>
          </D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].uri, "/contacts/opaque-id");
        assert_eq!(listing.entries[0].etag, Some("abc".to_string()));
    }

    /// A response carrying NO propstat at all names a resource the server
    /// still holds. It used to land in neither lane - not an entry (no
    /// success propstat) and not a failed href (no failed propstat) - so the
    /// contact vanished from the snapshot and the diff destroyed a row that
    /// exists. It commits as an etag-less entry, as the CalDAV twin already
    /// read it.
    #[test]
    fn a_propstat_less_response_is_an_entry_rather_than_a_vanished_contact() {
        let xml = r#"<D:multistatus xmlns:D="DAV:">
          <D:response><D:href>/contacts/opaque-id</D:href></D:response>
          </D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.entries.len(), 1, "{listing:?}");
        assert_eq!(listing.entries[0].uri, "/contacts/opaque-id");
        assert_eq!(listing.entries[0].etag, None);
        assert!(listing.failed.is_empty());
    }

    /// Same rule on the multiget lane: a refused `resourcetype` must not
    /// suppress address data a successful propstat supplied.
    #[test]
    fn multiget_ignores_collection_marker_in_a_failed_propstat() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
          <D:response><D:href>/contacts/opaque-id</D:href>
          <D:propstat><D:prop><D:getetag>"abc"</D:getetag>
          <C:address-data>BEGIN:VCARD
VERSION:4.0
FN:One
END:VCARD</C:address-data></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
          <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
          <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
          </D:response></D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert_eq!(report.cards.len(), 1);
        assert_eq!(report.cards[0].uri, "/contacts/opaque-id");
    }

    /// The CalDAV twin guards this; the two crates had drifted. A server
    /// echoing the collection itself alongside the requested resources must
    /// not surface the collection URL as a card id.
    #[test]
    fn multiget_does_not_fetch_an_echoed_collection() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
          <D:response><D:href>/contacts/</D:href><D:propstat><D:prop>
          <D:resourcetype><D:collection/></D:resourcetype>
          <C:address-data>not a card</C:address-data></D:prop>
          <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
          </D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert!(report.cards.is_empty());
    }

    #[test]
    fn propfind_contacts_ignores_nested_href_properties() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:owner><D:href>/principals/ada/</D:href></D:owner>
        <D:getetag>"abc"</D:getetag>
        <D:getcontenttype>text/vcard</D:getcontenttype>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let listing = parse_propfind_contacts(xml).expect("valid XML");
        assert_eq!(listing.entries[0].uri, "/contacts/card-1.vcf");
    }

    #[test]
    fn multiget_extracts_href_etag_and_vcard() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"abc"</D:getetag>
        <C:address-data>BEGIN:VCARD
FN:Ada Lovelace
END:VCARD</C:address-data>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert_eq!(report.cards.len(), 1);
        assert_eq!(report.cards[0].uri, "/contacts/card-1.vcf");
        assert_eq!(report.cards[0].etag.as_deref(), Some("abc"));
        assert!(report.cards[0].data.contains("FN:Ada Lovelace"));
        assert!(report.failed.is_empty());
    }

    #[test]
    fn multiget_ignores_failed_propstat_values() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/card-1.vcf</D:href>
    <D:propstat>
      <D:prop>
        <D:getetag>"abc"</D:getetag>
        <C:address-data>BEGIN:VCARD
FN:Ada Lovelace
END:VCARD</C:address-data>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid XML");
        assert!(report.cards.is_empty());
        assert_eq!(report.failed[0].status, Some(404));
    }

    #[test]
    fn multiget_complete_failure_is_classified() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/one.vcf</D:href>
    <D:propstat>
      <D:prop><C:address-data/></D:prop>
      <D:status>HTTP/1.1 401 Unauthorized</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/contacts/two.vcf</D:href>
    <D:propstat>
      <D:prop><C:address-data/></D:prop>
      <D:status>HTTP/1.1 401 Unauthorized</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.cards.is_empty());
        assert_eq!(report.failed.len(), 2);
        assert_eq!(
            report.classify(),
            MultigetOutcome::CompleteFailure { status: Some(401) }
        );
    }

    #[test]
    fn multiget_200_without_address_data_is_not_a_complete_failure() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/empty.vcf</D:href>
    <D:propstat>
      <D:prop><D:getetag>"e1"</D:getetag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let report = parse_multiget_report(xml).expect("valid 207");
        assert!(report.failed.is_empty());
        assert_eq!(report.missing_data, vec!["/contacts/empty.vcf"]);
        assert_eq!(report.classify(), MultigetOutcome::Usable);
    }

    // The depth-0 getctag PROPFIND backing the brick-8 ctag
    // short-circuit. The short-circuit decision (current == previous)
    // is exercised end-to-end downstream against a real server, per the
    // no-mock-server testing rule; here the deterministic piece is that
    // the helper extracts the collection ctag the decision keys off.
    #[test]
    fn ctag_short_circuit_parses_depth0_getctag() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop>
        <CS:getctag>ctag-7</CS:getctag>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let ctag = parse_collection_ctag(xml).expect("valid XML");
        assert_eq!(ctag.as_deref(), Some("ctag-7"));
    }

    // The status-line parsing this used to pin now lives in
    // `bifrost_net::status_line`, tested there against both DAV crates'
    // cases. A copy here would be the drift this consolidation removed.

    #[test]
    fn parse_collection_ctag_ignores_failed_propstat() {
        // A getctag returned inside a non-2xx propstat is stale and must
        // not feed the short-circuit; only a success propstat commits.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop>
        <CS:getctag>stale-ctag</CS:getctag>
      </D:prop>
      <D:status>HTTP/1.1 403 Forbidden</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        assert!(parse_collection_ctag(xml).expect("valid XML").is_none());
    }

    #[test]
    fn parse_collection_ctag_prefers_success_propstat() {
        // Failed propstat carries a stale ctag; the success propstat
        // carries the live one - the live one wins.
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop><CS:getctag>stale-ctag</CS:getctag></D:prop>
      <D:status>HTTP/1.1 403 Forbidden</D:status>
    </D:propstat>
    <D:propstat>
      <D:prop><CS:getctag>live-ctag</CS:getctag></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        assert_eq!(
            parse_collection_ctag(xml).expect("valid XML").as_deref(),
            Some("live-ctag")
        );
    }

    #[test]
    fn parse_collection_ctag_absent_returns_none() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop><D:displayname>Personal</D:displayname></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        assert!(parse_collection_ctag(xml).expect("valid XML").is_none());
    }

    #[test]
    fn extract_href_property_finds_nested_href() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:propstat>
      <D:prop>
        <D:current-user-principal>
          <D:href>/principals/user/</D:href>
        </D:current-user-principal>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let href = extract_href_property(xml, "current-user-principal").expect("valid XML");
        assert_eq!(href.as_deref(), Some("/principals/user/"));
    }

    #[test]
    fn nested_property_status_does_not_refuse_href_property() {
        let href = extract_href_property(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:response><D:propstat><D:status>HTTP/1.1 200 OK</D:status><D:prop><C:addressbook-home-set><D:href>/right/</D:href><C:extension><D:status>HTTP/1.1 404 Not Found</D:status></C:extension></C:addressbook-home-set></D:prop></D:propstat></D:response></D:multistatus>"#,
            "addressbook-home-set",
        )
        .expect("valid property");

        assert_eq!(href.as_deref(), Some("/right/"));
    }

    #[test]
    fn href_extractor_ignores_failed_propstat() {
        let href = extract_href_property(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:response><D:propstat><D:prop><C:addressbook-home-set><D:href>/wrong/</D:href></C:addressbook-home-set></D:prop><D:status>HTTP/1.1 404 Not Found</D:status></D:propstat><D:propstat><D:prop><C:addressbook-home-set><D:href>/right/</D:href></C:addressbook-home-set></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
            "addressbook-home-set",
        )
        .expect("valid XML");

        assert_eq!(href.as_deref(), Some("/right/"));
    }

    #[test]
    fn addressbook_collections_read_display_names() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav" xmlns:CS="http://calendarserver.org/ns/">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
        <D:displayname>Personal</D:displayname>
        <CS:getctag>42</CS:getctag>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let books = parse_addressbook_collections(xml).expect("valid XML");
        assert_eq!(
            books,
            vec![AddressBookCollection {
                is_directory: false,
                href: "/contacts/personal/".to_string(),
                display_name: Some("Personal".to_string()),
                ctag: Some("42".to_string()),
                can_edit: None,
            }]
        );
    }

    /// Writability comes from `current-user-privilege-set`, as CalDAV's does.
    /// A read-only shared book that advertises writable makes a consumer's
    /// capability gate pass a PUT the server will 403.
    #[test]
    fn addressbook_collections_derive_writability_from_the_privilege_set() {
        let book = |privileges: &str| {
            let xml = format!(
                r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response><D:href>/contacts/shared/</D:href><D:propstat><D:prop>
    <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
    <D:current-user-privilege-set>{privileges}</D:current-user-privilege-set>
  </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
</D:multistatus>"#
            );
            parse_addressbook_collections(&xml).expect("valid XML")[0].can_edit
        };

        assert_eq!(
            book("<D:privilege><D:read/></D:privilege>"),
            Some(false),
            "a read-only book must report read-only"
        );
        assert_eq!(
            book(
                "<D:privilege><D:read/></D:privilege><D:privilege><D:write-content/></D:privilege>"
            ),
            Some(true)
        );
    }

    /// A server that never answers the property leaves the answer unknown,
    /// and the account assumes writable rather than locking the user out.
    #[test]
    fn an_unanswered_privilege_set_leaves_writability_unknown() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response><D:href>/contacts/personal/</D:href><D:propstat><D:prop>
    <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
  </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
</D:multistatus>"#;

        assert_eq!(
            parse_addressbook_collections(xml).expect("valid XML")[0].can_edit,
            None
        );
    }

    #[test]
    fn addressbook_collections_ignore_failed_propstat_values() {
        let xml = r#"
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:response>
    <D:href>/contacts/personal/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/><C:addressbook/></D:resourcetype>
        <D:displayname>Personal</D:displayname>
      </D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let books = parse_addressbook_collections(xml).expect("valid XML");
        assert!(books.is_empty());
    }

    /// The CalDAV twin has always required its `<calendar/>` marker to sit
    /// inside a `<resourcetype>`; this side did not, so an `<addressbook/>`
    /// named anywhere else in the prop bag minted a phantom address book whose
    /// href is not a collection at all. The collapse onto the shared machine
    /// closes that drift.
    #[test]
    fn addressbook_element_outside_resourcetype_does_not_mark_a_collection() {
        let books = parse_addressbook_collections(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:response><D:href>/not-a-book/</D:href><D:propstat><D:prop><D:owner><C:addressbook/></D:owner></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid multistatus");

        assert!(books.is_empty());
    }

    #[test]
    fn cdata_is_read_by_carddav_parsers() {
        let books = parse_addressbook_collections(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:response><D:href><![CDATA[/contacts/]]></D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:addressbook/></D:resourcetype><D:displayname><![CDATA[Personal]]></D:displayname></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid collections");
        assert_eq!(books[0].display_name.as_deref(), Some("Personal"));

        let listing = parse_propfind_contacts(
            r#"<D:multistatus xmlns:D="DAV:"><D:response><D:href><![CDATA[/contacts/one.vcf]]></D:href><D:propstat><D:prop><D:getetag><![CDATA["one"]]></D:getetag><D:getcontenttype>text/vcard</D:getcontenttype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid listing");
        assert_eq!(listing.entries[0].etag.as_deref(), Some("one"));

        let report = parse_multiget_report(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:response><D:href><![CDATA[/contacts/one.vcf]]></D:href><D:propstat><D:prop><C:address-data><![CDATA[BEGIN:VCARD
FN:Ada
END:VCARD]]></C:address-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid multiget");
        assert!(report.cards[0].data.contains("FN:Ada"));

        let ctag = parse_collection_ctag(
            r#"<D:multistatus xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/"><D:response><D:propstat><D:prop><CS:getctag><![CDATA[tag-2]]></CS:getctag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>"#,
        )
        .expect("valid ctag");
        assert_eq!(ctag.as_deref(), Some("tag-2"));

        let href = extract_href_property(
            r#"<D:current-user-principal xmlns:D="DAV:"><D:href><![CDATA[/principals/ada/]]></D:href></D:current-user-principal>"#,
            "current-user-principal",
        )
        .expect("valid property");
        assert_eq!(href.as_deref(), Some("/principals/ada/"));
    }

    #[test]
    fn weak_etag_keeps_weakness_marker() {
        assert_eq!(normalize_etag("W/\"abc\"").as_deref(), Some("W/\"abc\""));
        assert_eq!(normalize_etag("\"abc\"").as_deref(), Some("abc"));
    }
}
