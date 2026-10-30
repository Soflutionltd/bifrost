//! Outlook's "pin to top" state.
//!
//! Graph has no pin field. Outlook (web, new Outlook, mobile) stamps the
//! undocumented MAPI property `0x0F01` (`PT_SYSTIME`) with 4500-09-01 when a
//! message is pinned, and the folder view sorts on it, which is what floats
//! pinned mail to the top. `0x0F02` carries the same date. Unpinning writes
//! the current time back. The state therefore reads as "the year is far in
//! the future", not as "the property exists".
//!
//! Read: the hydration `$expand` pulls `0x0F01` alone, [`is_pinned`] decodes
//! it into the `$pinned` engine flag. Write: the `$pinned` flag projects onto
//! both properties in the same PATCH as the other flags.

use serde_json::{Value, json};

/// The engine flag the pin state travels as, both directions.
pub(crate) const PINNED_FLAG: &str = "$pinned";

const PIN_PROPERTY_ID: &str = "SystemTime 0x0F01";
const PIN_SORT_PROPERTY_ID: &str = "SystemTime 0x0F02";
const PINNED_AT: &str = "4500-09-01T00:00:00Z";
/// Any year from here on is a pin marker, never a real date.
const PINNED_FROM_YEAR: u32 = 4000;

/// Whether a hydrated Graph message value carries Outlook's pin marker.
pub(crate) fn is_pinned(value: &Value) -> bool {
    value
        .get("singleValueExtendedProperties")
        .and_then(Value::as_array)
        .is_some_and(|properties| {
            properties.iter().any(|property| {
                property
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(is_pin_property_id)
                    && property
                        .get("value")
                        .and_then(Value::as_str)
                        .and_then(|value| value.get(..4)?.parse::<u32>().ok())
                        .is_some_and(|year| year >= PINNED_FROM_YEAR)
            })
        })
}

/// Graph echoes the property id in its own normalized spelling
/// (`SystemTime 0xF01`), not necessarily the one the request used.
fn is_pin_property_id(id: &str) -> bool {
    let Some((kind, tag)) = id.split_once(' ') else {
        return false;
    };
    let Some(hex) = tag.strip_prefix("0x").or_else(|| tag.strip_prefix("0X")) else {
        return false;
    };
    kind.eq_ignore_ascii_case("SystemTime") && u32::from_str_radix(hex, 16) == Ok(0x0F01)
}

/// The `singleValueExtendedProperties` PATCH value that pins or unpins.
pub(crate) fn pin_properties(pinned: bool) -> Value {
    let value = if pinned {
        PINNED_AT.to_string()
    } else {
        jiff::Timestamp::now()
            .strftime("%Y-%m-%dT%H:%M:%SZ")
            .to_string()
    };
    json!([
        { "id": PIN_PROPERTY_ID, "value": value },
        { "id": PIN_SORT_PROPERTY_ID, "value": value },
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message_with(id: &str, value: &str) -> Value {
        json!({ "singleValueExtendedProperties": [{ "id": id, "value": value }] })
    }

    #[test]
    fn the_far_future_marker_reads_as_pinned_in_either_id_spelling() {
        assert!(is_pinned(&message_with(
            "SystemTime 0x0F01",
            "4500-09-01T00:00:00Z"
        )));
        assert!(is_pinned(&message_with(
            "SystemTime 0xF01",
            "4500-09-01T00:00:00Z"
        )));
    }

    #[test]
    fn a_real_timestamp_or_no_property_reads_as_not_pinned() {
        assert!(!is_pinned(&message_with(
            "SystemTime 0xF01",
            "2026-10-30T09:12:00Z"
        )));
        assert!(!is_pinned(&json!({ "id": "m1" })));
        assert!(!is_pinned(&message_with(
            "SystemTime 0xF02",
            "4500-09-01T00:00:00Z"
        )));
    }

    #[test]
    fn unpinning_writes_a_present_date_on_both_properties() {
        let properties = pin_properties(false);
        let values: Vec<&str> = properties
            .as_array()
            .unwrap()
            .iter()
            .map(|property| property["value"].as_str().unwrap())
            .collect();
        assert_eq!(values.len(), 2);
        assert!(values.iter().all(|value| value.starts_with("20")));
        assert!(!is_pinned(&json!({
            "singleValueExtendedProperties": properties
        })));
        assert!(is_pinned(&json!({
            "singleValueExtendedProperties": pin_properties(true)
        })));
    }
}
