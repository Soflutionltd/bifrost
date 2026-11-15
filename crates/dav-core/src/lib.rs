//! Private shared WebDAV layer for `bifrost-caldav` and `bifrost-carddav`.
//!
//! These two crates are near-duplicates by construction: CalDAV and CardDAV are
//! the same protocol with different property names, query bodies and payload
//! projections. Everything below that difference - the transport seam, the
//! redirect policy, the credential-origin gate, the status-to-`AccountError`
//! ladder, etag handling - was hand-mirrored between them, and nothing compared
//! the copies, so divergence was silent. Eight separate defects were exactly
//! that: a fix landing in one crate and not its twin.
//!
//! This crate exists to delete the mirror rather than to keep policing it. It
//! is PRIVATE - the precedent is `bifrost-sasl` - so neither published surface
//! moves: `CalDavConfig`, `CardDavConfig`, both factories and both `Account`
//! impls are exactly what they were. Consumers see nothing.
//!
//! What stays in the protocol crates is what genuinely differs: property
//! constants, query XML, the iCalendar and vCard projections, the lanes only
//! one side has (CalDAV's `sync-collection` and scheduling, CardDAV's
//! `getctag`), and the discovery steps after the principal. The principal walk
//! itself - well-known probe, fallback predicate, configured base - is
//! `DavDispatch::discover_current_user_principal`.

pub mod content_line;
mod discovery;
mod dispatch;
mod error;
mod etag;
mod multistatus;
mod query;
mod snapshot;
// `test` as well as the feature: this crate's own unit tests script the wire
// seam too (the dispatcher's replay classification is decided here, not in
// either protocol crate), and a crate cannot turn its own feature on for its
// own test build.
#[cfg(any(feature = "test-support", test))]
pub mod test_support;
mod transport;
mod xml;

pub use dispatch::{
    DavByteTally, DavCredentials, DavDispatch, DavRequestGuard, delete_target_already_gone,
};

pub use error::{
    DavProtocol, filter_unsupported, item_status_error, local_error, missing_field_error,
    not_found_error, parse_error, response_read_error, should_fallback_discovery, status_error,
    transport_error, unsupported_error, worse_recovery,
};
pub use etag::{
    EntityTag, PutCondition, StrongTag, normalize_http_etag, prepare_if_match, response_etag,
};
pub use multistatus::{
    FailedResource, MultiStatusOutcome, MultiStatusSink, MultistatusError, PropSet, ResponseParts,
    classify_207, commit_if_present, extract_href_properties, extract_href_properties_in_namespace,
    extract_href_property, extract_href_property_in_namespace, parse_collection_property,
    parse_collection_property_in_namespace, parse_multistatus,
};
pub use query::{FilteredHrefs, HrefQuery, sorted_candidate_hrefs};
pub use snapshot::{
    DecodedSnapshot, PageSlice, SnapshotDecodeError, SnapshotDecodeErrorKind, SnapshotEntry,
    decode_snapshot, decode_watermark_cursor, diff_snapshots, encode_snapshot,
    encode_watermark_cursor, inventory_entry, object_change, preserve_unobserved_entries,
    slice_after_watermark,
};
pub use transport::{
    DAV_CLIENT_TIMEOUT, DavBody, DavRequest, DavResponse, origin_is_secure, settle_body, url_origin,
};
pub use xml::{
    APPLE_CALENDAR_NAMESPACE, CALDAV_NAMESPACE, CALENDAR_SERVER_NAMESPACE, CARDDAV_NAMESPACE,
    DAV_NAMESPACE, DavHrefKey, XmlName, append_path, dav_href_key, escape_xml, local_name,
    normalize_etag, push_text, resolve_href, same_dav_url, trimmed,
};

/// DAV request depth, including the recursive capability used by DAV servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Depth {
    Zero,
    One,
    Infinity,
}

impl Depth {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "0",
            Self::One => "1",
            Self::Infinity => "infinity",
        }
    }
}

pub static PROPFIND: std::sync::LazyLock<reqwest::Method> = std::sync::LazyLock::new(|| {
    reqwest::Method::from_bytes(b"PROPFIND").expect("valid DAV method")
});
pub static REPORT: std::sync::LazyLock<reqwest::Method> =
    std::sync::LazyLock::new(|| reqwest::Method::from_bytes(b"REPORT").expect("valid DAV method"));
pub static MOVE: std::sync::LazyLock<reqwest::Method> =
    std::sync::LazyLock::new(|| reqwest::Method::from_bytes(b"MOVE").expect("valid DAV method"));
