//! RFC 8984 scalar values. Constructors validate before request assembly.

use serde::{Deserialize, Serialize};
use std::str::FromStr;

macro_rules! scalar {
    ($name:ident, $validate:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub(crate) struct $name(String);
        impl $name {
            pub(crate) fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl FromStr for $name {
            type Err = &'static str;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                $validate(value)
                    .then(|| Self(value.into()))
                    .ok_or(concat!("invalid ", stringify!($name)))
            }
        }
        impl TryFrom<String> for $name {
            type Error = &'static str;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                value.parse()
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.0 == *other
            }
        }
    };
}

fn valid_local(value: &str) -> bool {
    let Some(base) = value.get(..19) else {
        return false;
    };
    let bytes = base.as_bytes();
    if ![(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')]
        .into_iter()
        .all(|(at, expected)| bytes[at] == expected)
        || bytes
            .iter()
            .enumerate()
            .any(|(at, b)| ![4, 7, 10, 13, 16].contains(&at) && !b.is_ascii_digit())
        || jiff::civil::DateTime::strptime("%Y-%m-%dT%H:%M:%S", base).is_err()
    {
        return false;
    }
    let fraction = &value[19..];
    fraction.is_empty()
        || fraction.strip_prefix('.').is_some_and(|digits| {
            !digits.is_empty() && digits.len() <= 9 && digits.bytes().all(|b| b.is_ascii_digit())
        })
}
fn valid_utc(value: &str) -> bool {
    value.strip_suffix('Z').is_some_and(valid_local)
}

scalar!(LocalDateTime, valid_local);
scalar!(UtcDateTime, valid_utc);
scalar!(Duration, valid_duration);

impl LocalDateTime {
    pub(crate) fn from_civil(value: jiff::civil::DateTime) -> Result<Self, &'static str> {
        value.to_string().parse()
    }
    pub(crate) fn civil(&self) -> jiff::civil::DateTime {
        self.0.parse().expect("validated LocalDateTime")
    }
}

/// Unsigned duration in RFC 8984: ordered days and time units, or weeks
/// alone. Seconds can carry up to nine fractional decimal places.
fn duration_nanos(value: &str) -> Option<u128> {
    let mut rest = value.strip_prefix('P')?;
    let mut total = 0_u128;
    let mut in_time = false;
    let mut rank = 0;
    let mut fields = 0;
    let mut time_fields = 0;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('T') {
            if in_time {
                return None;
            }
            in_time = true;
            rest = after;
            if rest.is_empty() {
                return None;
            }
            continue;
        }
        let end = rest.find(|ch: char| !ch.is_ascii_digit() && ch != '.')?;
        if end == 0 {
            return None;
        }
        let number = &rest[..end];
        let unit = *rest.as_bytes().get(end)?;
        rest = &rest[end + 1..];
        let (next, multiplier) = match (unit, in_time) {
            (b'W', false) if fields == 0 && rest.is_empty() => (1, 604_800_u128),
            (b'D', false) => (2, 86_400),
            (b'H', true) => (3, 3_600),
            (b'M', true) => (4, 60),
            (b'S', true) => (5, 1),
            _ => return None,
        };
        if next <= rank {
            return None;
        }
        rank = next;
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        if whole.is_empty()
            || (!fraction.is_empty() && unit != b'S')
            || (number.contains('.') && (fraction.is_empty() || fraction.len() > 9))
        {
            return None;
        }
        let mut nanos = whole
            .parse::<u128>()
            .ok()?
            .checked_mul(multiplier)?
            .checked_mul(1_000_000_000)?;
        if !fraction.is_empty() {
            nanos = nanos.checked_add(
                fraction
                    .parse::<u128>()
                    .ok()?
                    .checked_mul(10_u128.pow(9 - u32::try_from(fraction.len()).ok()?))?,
            )?;
        }
        total = total.checked_add(nanos)?;
        fields += 1;
        if in_time {
            time_fields += 1;
        }
    }
    (fields > 0 && (!in_time || time_fields > 0)).then_some(total)
}
fn valid_duration(value: &str) -> bool {
    duration_nanos(value).is_some()
}
impl Duration {
    pub(crate) fn signed(&self) -> Option<jiff::SignedDuration> {
        let nanos = duration_nanos(&self.0)?;
        let seconds = i64::try_from(nanos / 1_000_000_000).ok()?;
        let fraction = i32::try_from(nanos % 1_000_000_000).ok()?;
        Some(jiff::SignedDuration::new(seconds, fraction))
    }
    #[cfg(test)]
    pub(crate) fn whole_seconds(&self) -> Option<u64> {
        u64::try_from(duration_nanos(&self.0)? / 1_000_000_000).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scalar_grammars_are_distinct() {
        assert!("2026-06-02T12:00:00.123".parse::<LocalDateTime>().is_ok());
        assert!("2026-06-02T12:00:00Z".parse::<LocalDateTime>().is_err());
        assert!("2026-06-02T12:00:00Z".parse::<UtcDateTime>().is_ok());
        assert!("2026-06-02T12:00:00".parse::<UtcDateTime>().is_err());
        for value in [
            "P", "PT", "PT1H2H", "PT1M1H", "P1W1D", "PT1D", "P1DT", "PT.5S", "PT1.S",
        ] {
            assert!(value.parse::<Duration>().is_err(), "{value}");
        }
        for value in ["PT0S", "P1D", "P2W", "P1DT2H3M4.123S"] {
            assert!(value.parse::<Duration>().is_ok(), "{value}");
        }
    }
}
