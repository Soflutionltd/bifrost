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

pub use dispatch::{DavCredentials, DavDispatch, DavRequestGuard, delete_target_already_gone};

pub use error::{
    DavProtocol, filter_unsupported, local_error, missing_field_error, not_found_error,
    parse_error, recovery_rank, response_read_error, should_fallback_discovery, status_error,
    transport_error, unsupported_error, worse_recovery,
};
pub use etag::{PutCondition, normalize_http_etag, prepare_if_match, response_etag};
pub use multistatus::{
    FailedResource, MultiStatusOutcome, MultiStatusSink, PropSet, ResponseParts, classify_207,
    commit_if_present, extract_href_properties, extract_href_property, parse_collection_property,
    parse_multistatus,
};
pub use query::{FilteredHrefs, HrefQuery, sorted_candidate_hrefs};
pub use snapshot::{
    DecodedSnapshot, PageSlice, SnapshotEntry, decode_snapshot, decode_watermark_cursor,
    diff_snapshots, encode_snapshot, encode_watermark_cursor, inventory_entry, object_change,
    preserve_unobserved_entries, slice_after_watermark,
};
pub use transport::{
    DAV_CLIENT_TIMEOUT, DavBody, DavRequest, DavResponse, origin_is_secure, settle_body, url_origin,
};
pub use xml::{
    DavHrefKey, append_path, dav_href_key, escape_xml, local_name, normalize_etag, push_text,
    resolve_href, same_dav_url, trimmed,
};
