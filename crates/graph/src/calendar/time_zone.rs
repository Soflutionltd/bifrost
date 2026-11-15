//! Graph Windows and IANA time-zone projection.

use jiff::tz::TimeZone;

/// The outbound IANA -> Windows time zone mapping, one row per Windows id.
/// The first IANA id of a row is the one that Windows id resolves back to
/// when an event's own zone is needed (reading a UTC RRULE `UNTIL` as a local
/// date), so it is the row's canonical zone.
pub(crate) const WINDOWS_ZONES: &[(&str, &[&str])] = &[
    ("UTC", &["Etc/UTC", "UTC"]),
    (
        "W. Europe Standard Time",
        &[
            "Europe/Berlin",
            "Europe/Amsterdam",
            "Europe/Busingen",
            "Europe/Oslo",
            "Europe/Rome",
            "Europe/Stockholm",
            "Europe/Vienna",
            "Europe/Zurich",
        ],
    ),
    (
        "Romance Standard Time",
        &[
            "Europe/Paris",
            "Europe/Brussels",
            "Europe/Copenhagen",
            "Europe/Madrid",
        ],
    ),
    (
        "Central Europe Standard Time",
        &[
            "Europe/Budapest",
            "Europe/Bratislava",
            "Europe/Ljubljana",
            "Europe/Prague",
            "Europe/Warsaw",
            "Europe/Zagreb",
        ],
    ),
    (
        "GMT Standard Time",
        &["Europe/London", "Europe/Dublin", "Europe/Lisbon"],
    ),
    ("GTB Standard Time", &["Europe/Bucharest", "Europe/Athens"]),
    (
        "FLE Standard Time",
        &[
            "Europe/Helsinki",
            "Europe/Kyiv",
            "Europe/Riga",
            "Europe/Sofia",
            "Europe/Tallinn",
            "Europe/Vilnius",
        ],
    ),
    ("Turkey Standard Time", &["Europe/Istanbul"]),
    ("Russian Standard Time", &["Europe/Moscow"]),
    (
        "Eastern Standard Time",
        &[
            "America/New_York",
            "America/Detroit",
            "America/Indiana/Indianapolis",
            "America/Toronto",
        ],
    ),
    ("Central Standard Time", &["America/Chicago"]),
    ("Mountain Standard Time", &["America/Denver"]),
    ("US Mountain Standard Time", &["America/Phoenix"]),
    (
        "Pacific Standard Time",
        &["America/Los_Angeles", "America/Vancouver"],
    ),
    ("Alaskan Standard Time", &["America/Anchorage"]),
    ("Hawaiian Standard Time", &["Pacific/Honolulu"]),
    ("Central Standard Time (Mexico)", &["America/Mexico_City"]),
    (
        "SA Pacific Standard Time",
        &["America/Bogota", "America/Lima", "America/Guayaquil"],
    ),
    ("Pacific SA Standard Time", &["America/Santiago"]),
    ("E. South America Standard Time", &["America/Sao_Paulo"]),
    (
        "Argentina Standard Time",
        &["America/Argentina/Buenos_Aires"],
    ),
    ("Egypt Standard Time", &["Africa/Cairo"]),
    ("South Africa Standard Time", &["Africa/Johannesburg"]),
    ("Arabian Standard Time", &["Asia/Dubai"]),
    ("Israel Standard Time", &["Asia/Jerusalem"]),
    ("Tokyo Standard Time", &["Asia/Tokyo"]),
    ("Korea Standard Time", &["Asia/Seoul"]),
    ("China Standard Time", &["Asia/Shanghai"]),
    ("Hong Kong Standard Time", &["Asia/Hong_Kong"]),
    ("Singapore Standard Time", &["Asia/Singapore"]),
    ("India Standard Time", &["Asia/Kolkata"]),
    ("E. Australia Standard Time", &["Australia/Brisbane"]),
    ("W. Australia Standard Time", &["Australia/Perth"]),
    (
        "AUS Eastern Standard Time",
        &["Australia/Sydney", "Australia/Melbourne"],
    ),
    ("New Zealand Standard Time", &["Pacific/Auckland"]),
];

pub(crate) fn graph_time_zone_name(timezone: Option<&str>) -> Option<&str> {
    let value = timezone.unwrap_or("UTC");
    if let Some((windows, _)) = WINDOWS_ZONES.iter().find(|(_, iana)| iana.contains(&value)) {
        return Some(*windows);
    }
    // An IANA id we don't have a Windows mapping for: reject locally.
    if value.contains('/') {
        return None;
    }
    // A slash-free string is treated as an already-Windows tz id so they
    // round-trip. Windows tz ids are multi-word (e.g. "W. Europe Standard
    // Time"); the sole single-token id is "UTC" (in the table). Reject a
    // slash-free single token as junk rather than forwarding an unresolvable
    // tz to Graph.
    value.contains(' ').then_some(value)
}

/// The IANA id an event zone name stands for: an IANA id as is, a Windows id
/// through its `WINDOWS_ZONES` row. `None` for anything else, including a
/// Windows id outside the table and Graph's `tzone://Microsoft/Custom`.
pub(crate) fn iana_zone_name(name: &str) -> Option<&str> {
    let iana = WINDOWS_ZONES
        .iter()
        .find(|(windows, _)| *windows == name)
        .and_then(|(_, iana)| iana.first().copied())
        .unwrap_or(name);
    (iana.contains('/') && !iana.starts_with("tzone:")).then_some(iana)
}

/// Resolve an event zone name against the tz database. `None` when the name
/// is unknown or the database lacks it.
pub(crate) fn resolve_time_zone(name: &str) -> Option<TimeZone> {
    if matches!(name, "UTC" | "Etc/UTC") {
        return Some(TimeZone::UTC);
    }
    TimeZone::get(iana_zone_name(name)?).ok()
}
