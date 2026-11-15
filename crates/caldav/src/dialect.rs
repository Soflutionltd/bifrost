//! Calendar/contact syntax and policy for the shared collection kernel.
use bifrost_dav_core::collection::{Dialect, MultigetReport, ResourceListing};
use bifrost_dav_core::{DavProtocol, MultistatusError, PutCondition};
use bifrost_types::{ErrorScope, ObjectType, ProtocolKind};

#[derive(Debug, Clone)]
pub(crate) struct CalDavDialect;

impl Dialect for CalDavDialect {
    const DAV: DavProtocol = DavProtocol::CalDav;
    const OBJECT_TYPE: ObjectType = ObjectType::CalendarEvent;
    const PROTOCOL_KIND: ProtocolKind = ProtocolKind::CalDav;
    const MULTIGET_ELEMENT: &'static str = "calendar-multiget";
    const NAMESPACE: &'static str = bifrost_dav_core::CALDAV_NAMESPACE;
    const DATA_PROPERTY: &'static str = "calendar-data";
    const CONTENT_TYPE: &'static str = "text/calendar; charset=utf-8";
    const FILE_EXTENSION: &'static str = "ics";
    const CURSOR_MAGIC: &'static [u8] = b"CALDAVET1";

    fn item_scope(id: String) -> ErrorScope {
        ErrorScope::Event {
            id: bifrost_types::EventId(id),
        }
    }

    fn skipped_scope(collection: &str) -> ErrorScope {
        ErrorScope::Calendar {
            id: bifrost_types::CalendarId(collection.to_owned()),
        }
    }

    fn resolve_href(base: &str, href: &str) -> String {
        crate::parse::resolve_href(base, href)
    }

    fn parse_listing(body: &str) -> Result<ResourceListing, MultistatusError> {
        crate::parse::parse_propfind_events(body)
    }

    fn parse_multiget(body: &str) -> Result<MultigetReport, MultistatusError> {
        crate::parse::parse_multiget_report(body)
    }

    // A repeated calendar PUT may send a second scheduling message. A vCard
    // write has no scheduling side effects; its preconditions govern replay.
    fn put_replay(_condition: &PutCondition) -> bifrost_net::ReplayPolicy {
        bifrost_net::ReplayPolicy::Never
    }
}
