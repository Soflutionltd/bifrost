//! Calendar/contact syntax and policy for the shared collection kernel.
use bifrost_dav_core::collection::{Dialect, MultigetReport, ResourceListing};
use bifrost_dav_core::{DavProtocol, MultistatusError, PutCondition};
use bifrost_types::{ErrorScope, ObjectType, ProtocolKind};

#[derive(Debug, Clone)]
pub(crate) struct CardDavDialect;

impl Dialect for CardDavDialect {
    const DAV: DavProtocol = DavProtocol::CardDav;
    const OBJECT_TYPE: ObjectType = ObjectType::Contact;
    const PROTOCOL_KIND: ProtocolKind = ProtocolKind::CardDav;
    const MULTIGET_ELEMENT: &'static str = "addressbook-multiget";
    const NAMESPACE: &'static str = bifrost_dav_core::CARDDAV_NAMESPACE;
    const DATA_PROPERTY: &'static str = "address-data";
    const CONTENT_TYPE: &'static str = "text/vcard; charset=utf-8";
    const FILE_EXTENSION: &'static str = "vcf";
    const CURSOR_MAGIC: &'static [u8] = b"CDAVCTAG1";

    fn item_scope(id: String) -> ErrorScope {
        ErrorScope::Contact {
            id: bifrost_types::ContactId(id),
        }
    }

    fn skipped_scope(_collection: &str) -> ErrorScope {
        ErrorScope::ContactCollection
    }

    fn resolve_href(base: &str, href: &str) -> String {
        crate::parse::resolve_href(base, href)
    }

    fn parse_listing(body: &str) -> Result<ResourceListing, MultistatusError> {
        crate::parse::parse_propfind_contacts(body)
    }

    fn parse_multiget(body: &str) -> Result<MultigetReport, MultistatusError> {
        crate::parse::parse_multiget_report(body)
    }

    // A repeated calendar PUT may send a second scheduling message. A vCard
    // write has no scheduling side effects; its preconditions govern replay.
    fn put_replay(condition: &PutCondition) -> bifrost_net::ReplayPolicy {
        if matches!(condition, bifrost_dav_core::PutCondition::IfNoneMatch) {
            bifrost_net::ReplayPolicy::Never
        } else {
            bifrost_net::ReplayPolicy::IdempotentUnlessConditional
        }
    }
}
