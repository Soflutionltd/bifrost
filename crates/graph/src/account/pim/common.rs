//! Helpers shared by more than one PIM module: id and error
//! construction, extended-property naming, and the Graph time formats.

use crate::account::graph_error::protocol_violation;
use bifrost_types::{AccountError, AccountOperation, ErrorScope, ObjectId, ProtocolErrorKind};
use jiff::tz::Offset;
use jiff::{Timestamp, civil};
use serde_json::Value;
use std::time::SystemTime;

use super::messages::*;

pub(super) fn object_id_from_value(
    value: &Value,
    operation: AccountOperation,
    owner: Option<&str>,
) -> Result<ObjectId, AccountError> {
    value
        .get("id")
        .and_then(Value::as_str)
        .map(|id| ObjectId(crate::account::foreign::qualify_with_owner(owner, id)))
        .ok_or_else(|| pim_protocol_error(operation, None, "Graph message did not include an id"))
}

/// Build a `Protocol(ContractViolation)` `AccountError` for pim-layer
/// data-shape violations (missing id, missing etag, etc.). The
/// caller threads its `AccountOperation` and the scope of the
/// affected resource (typically `ErrorScope::Message { id }`); both
/// flow through into telemetry and support exports.
pub(super) fn pim_protocol_error(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    msg: impl Into<String>,
) -> AccountError {
    protocol_violation(ProtocolErrorKind::ContractViolation, operation, scope, msg)
}

pub(super) fn graph_extended_property_id(property_id: &str) -> String {
    if property_id.eq_ignore_ascii_case(PR_LAST_VERB_EXECUTED_ALIAS)
        || property_id.eq_ignore_ascii_case("PidTagLastVerbExecuted")
    {
        PR_LAST_VERB_EXECUTED_GRAPH_ID.to_string()
    } else {
        property_id.to_string()
    }
}

/// Format an absolute instant as ISO-8601 UTC for the Graph
/// `singleValueExtendedProperty` value (matches Graph's PT_SYSTIME wire
/// shape, e.g. `2026-06-16T10:00:00Z`).
pub(super) fn graph_iso8601_utc(at: std::time::SystemTime) -> String {
    utc_civil(at).strftime("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// KQL date literal (`YYYY-MM-DD`) for the `received` range predicates.
pub(super) fn system_time_date(value: SystemTime) -> String {
    utc_civil(value).strftime("%Y-%m-%d").to_string()
}

pub(super) fn parse_graph_datetime(value: &str) -> Option<SystemTime> {
    if let Ok(ts) = value.parse::<Timestamp>() {
        return Some(ts.into());
    }
    // Graph also emits zoneless date-times; those are UTC by contract.
    let naive = civil::DateTime::strptime("%Y-%m-%dT%H:%M:%S%.f", value).ok()?;
    Offset::UTC.to_timestamp(naive).ok().map(SystemTime::from)
}

/// A `SystemTime` as the UTC wall clock it names. Instants outside the
/// representable range clamp to the epoch rather than failing the caller.
pub(super) fn utc_civil(value: SystemTime) -> civil::DateTime {
    Offset::UTC.to_datetime(Timestamp::try_from(value).unwrap_or(Timestamp::UNIX_EPOCH))
}

pub(super) fn system_time_rfc3339(value: SystemTime) -> String {
    utc_civil(value).strftime("%Y-%m-%dT%H:%M:%SZ").to_string()
}

pub(super) fn system_time_naive_utc(value: SystemTime) -> String {
    utc_civil(value).strftime("%Y-%m-%dT%H:%M:%S").to_string()
}

pub(super) fn finish_partial(
    error: AccountError,
    record: bifrost_types::PartialCommitBuilder,
) -> AccountError {
    error
        .into_builder()
        .partial_commit(record)
        .try_build()
        .expect("valid Graph partial commitment")
}
