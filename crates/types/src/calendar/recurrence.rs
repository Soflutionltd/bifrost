//! Protocol-neutral RFC 5545 recurrence syntax. Provider projections own
//! expressibility and DTSTART-dependent checks, never the grammar.

use std::str::FromStr;

/// A weekday and optional ordinal in a BYDAY list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecurrenceDay {
    pub ordinal: Option<i8>,
    pub weekday: jiff::civil::Weekday,
}

/// UNTIL preserves DATE, floating DATE-TIME and UTC DATE-TIME distinctly.
/// Seconds may be 60: RFC 5545 permits a leap second even though provider
/// date-time libraries may not be able to express it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecurrenceUntil {
    Date(jiff::civil::Date),
    DateTime {
        date: jiff::civil::Date,
        hour: u8,
        minute: u8,
        second: u8,
        utc: bool,
    },
}

/// A syntactically valid rule, retaining part order and spelling for lossless
/// storage. Extension parts remain opaque; a provider must explicitly support
/// them before writing. Event-time-dependent validity belongs with DTSTART.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecurrenceRule {
    text: String,
    parts: Vec<(String, String)>,
    until: Option<RecurrenceUntil>,
    days: Vec<RecurrenceDay>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid recurrence rule: {0}")]
pub struct RecurrenceRuleError(pub String);

fn invalid(message: impl Into<String>) -> RecurrenceRuleError {
    RecurrenceRuleError(message.into())
}

impl RecurrenceRule {
    pub fn as_str(&self) -> &str {
        &self.text
    }
    pub fn value(&self, name: &str) -> Option<&str> {
        self.parts
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
    pub fn parts(&self) -> impl Iterator<Item = (&str, &str)> {
        self.parts
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
    pub fn until(&self) -> Option<RecurrenceUntil> {
        self.until
    }
    pub fn by_day(&self) -> &[RecurrenceDay] {
        &self.days
    }
    pub fn count(&self) -> Option<u32> {
        self.value("COUNT").and_then(|v| v.parse().ok())
    }
    pub fn interval(&self) -> u32 {
        self.value("INTERVAL")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1)
    }
}

impl std::fmt::Display for RecurrenceRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

fn weekday(value: &str) -> Option<jiff::civil::Weekday> {
    use jiff::civil::Weekday::*;
    Some(match value {
        "MO" => Monday,
        "TU" => Tuesday,
        "WE" => Wednesday,
        "TH" => Thursday,
        "FR" => Friday,
        "SA" => Saturday,
        "SU" => Sunday,
        _ => return None,
    })
}

fn integers(value: &str, min: i32, max: i32, nonzero: bool) -> bool {
    value.split(',').all(|v| {
        let digits = v.strip_prefix(['+', '-']).unwrap_or(v);
        let width = if max <= 99 {
            2
        } else if max <= 366 {
            3
        } else {
            usize::MAX
        };
        !digits.is_empty()
            && digits.len() <= width
            && (min < 0 || digits.len() == v.len())
            && digits.bytes().all(|b| b.is_ascii_digit())
            && v.parse::<i32>()
                .ok()
                .is_some_and(|n| (min..=max).contains(&n) && (!nonzero || n != 0))
    })
}

impl FromStr for RecurrenceUntil {
    type Err = RecurrenceRuleError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.to_ascii_uppercase();
        let bad = || invalid("UNTIL is not a DATE or DATE-TIME");
        let number = |start, end| {
            value
                .get(start..end)
                .filter(|v| v.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|v| v.parse::<i16>().ok())
        };
        let date = jiff::civil::Date::new(
            number(0, 4).ok_or_else(bad)?,
            i8::try_from(number(4, 6).ok_or_else(bad)?).map_err(|_| bad())?,
            i8::try_from(number(6, 8).ok_or_else(bad)?).map_err(|_| bad())?,
        )
        .map_err(|_| bad())?;
        if value.len() == 8 {
            return Ok(Self::Date(date));
        }
        if !matches!(value.len(), 15 | 16)
            || value.as_bytes().get(8) != Some(&b'T')
            || (value.len() == 16 && !value.ends_with('Z'))
        {
            return Err(bad());
        }
        let hour = number(9, 11)
            .filter(|n| (0..=23).contains(n))
            .ok_or_else(bad)?;
        let hour = u8::try_from(hour).map_err(|_| bad())?;
        let minute = number(11, 13)
            .filter(|n| (0..=59).contains(n))
            .ok_or_else(bad)?;
        let minute = u8::try_from(minute).map_err(|_| bad())?;
        let second = number(13, 15)
            .filter(|n| (0..=60).contains(n))
            .ok_or_else(bad)?;
        let second = u8::try_from(second).map_err(|_| bad())?;
        Ok(Self::DateTime {
            date,
            hour,
            minute,
            second,
            utc: value.len() == 16,
        })
    }
}

impl FromStr for RecurrenceRule {
    type Err = RecurrenceRuleError;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut rule = Self {
            text: text.into(),
            parts: Vec::new(),
            until: None,
            days: Vec::new(),
        };
        let non_gregorian = text
            .split(';')
            .filter_map(|part| part.split_once('='))
            .any(|(key, _)| key.eq_ignore_ascii_case("RSCALE"));
        for part in text.split(';') {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| invalid("part is not NAME=VALUE"))?;
            let key = key.to_ascii_uppercase();
            let upper = value.to_ascii_uppercase();
            if value.is_empty()
                || !value.is_ascii()
                || value
                    .bytes()
                    .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
            {
                return Err(invalid(format!("{key} has an invalid value")));
            }
            if rule.value(&key).is_some() {
                return Err(invalid(format!("repeated {key}")));
            }
            let valid = match key.as_str() {
                "FREQ" => [
                    "SECONDLY", "MINUTELY", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "YEARLY",
                ]
                .contains(&upper.as_str()),
                "COUNT" | "INTERVAL" => {
                    integers(value, 1, i32::MAX, true)
                        && !value.contains(',')
                        && !value.starts_with(['+', '-'])
                }
                "UNTIL" => {
                    rule.until = Some(upper.parse()?);
                    true
                }
                "WKST" => weekday(&upper).is_some(),
                "BYSECOND" => integers(value, 0, 60, false),
                "BYMINUTE" => integers(value, 0, 59, false),
                "BYHOUR" => integers(value, 0, 23, false),
                "BYMONTHDAY" => integers(value, -31, 31, true),
                "BYYEARDAY" | "BYSETPOS" => integers(value, -366, 366, true),
                "BYWEEKNO" => integers(value, -53, 53, true),
                "BYMONTH" if non_gregorian => upper.split(',').all(|month| {
                    let month = month.strip_suffix('L').unwrap_or(month);
                    !month.starts_with(['+', '-']) && integers(month, 1, 99, true)
                }),
                "BYMONTH" => integers(value, 1, 12, true),
                "BYDAY" => {
                    for item in upper.split(',') {
                        let at = item
                            .len()
                            .checked_sub(2)
                            .ok_or_else(|| invalid("invalid BYDAY"))?;
                        let (ordinal, day) = item.split_at(at);
                        let day = weekday(day).ok_or_else(|| invalid("invalid BYDAY weekday"))?;
                        let ordinal = if ordinal.is_empty() {
                            None
                        } else {
                            if !integers(ordinal, -53, 53, true) {
                                return Err(invalid("invalid BYDAY ordinal"));
                            }
                            Some(
                                ordinal
                                    .parse::<i8>()
                                    .map_err(|_| invalid("invalid BYDAY ordinal"))?,
                            )
                        };
                        rule.days.push(RecurrenceDay {
                            ordinal,
                            weekday: day,
                        });
                    }
                    true
                }
                // RFC 7529 scale names are registry tokens. Whether a scale
                // exists and can be expanded is a provider expressibility check.
                "RSCALE" => upper
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
                "SKIP" => ["OMIT", "BACKWARD", "FORWARD"].contains(&upper.as_str()),
                _ if key.starts_with("X-") => {
                    key.len() > 2 && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                }
                _ => false,
            };
            if !valid {
                return Err(invalid(format!("invalid {key}")));
            }
            rule.parts.push((key, value.into()));
        }
        let frequency = rule
            .value("FREQ")
            .ok_or_else(|| invalid("missing FREQ"))?
            .to_ascii_uppercase();
        if rule.count().is_some() && rule.until.is_some() {
            return Err(invalid("COUNT and UNTIL are mutually exclusive"));
        }
        if rule.days.iter().any(|d| d.ordinal.is_some())
            && (!matches!(frequency.as_str(), "MONTHLY" | "YEARLY")
                || (frequency == "YEARLY" && rule.value("BYWEEKNO").is_some()))
        {
            return Err(invalid(
                "ordinal BYDAY requires MONTHLY or YEARLY without BYWEEKNO",
            ));
        }
        for (part, invalid_frequency) in [
            ("BYWEEKNO", frequency != "YEARLY"),
            ("BYMONTHDAY", frequency == "WEEKLY"),
            (
                "BYYEARDAY",
                matches!(frequency.as_str(), "DAILY" | "WEEKLY" | "MONTHLY"),
            ),
        ] {
            if invalid_frequency && rule.value(part).is_some() {
                return Err(invalid(format!("{part} is invalid with FREQ={frequency}")));
            }
        }
        if rule.value("BYSETPOS").is_some()
            && !rule
                .parts
                .iter()
                .any(|(k, _)| k.starts_with("BY") && k != "BYSETPOS")
        {
            return Err(invalid("BYSETPOS requires another BY part"));
        }
        if rule.value("SKIP").is_some() && rule.value("RSCALE").is_none() {
            return Err(invalid("SKIP requires RSCALE"));
        }
        Ok(rule)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grammar_is_independent_of_provider_expressibility() {
        for value in [
            "FREQ=DAILY",
            "freq=weekly;byday=mo,we",
            "FREQ=DAILY;BYHOUR=9",
            "FREQ=MONTHLY;BYDAY=-1FR",
            "FREQ=YEARLY;BYWEEKNO=2;BYDAY=MO",
            "FREQ=YEARLY;RSCALE=HEBREW;BYMONTH=5L;SKIP=OMIT",
            "FREQ=DAILY;X-NAME=1",
        ] {
            assert!(value.parse::<RecurrenceRule>().is_ok(), "{value}");
        }
        for value in [
            "",
            "FREQ=DAILY;",
            "FREQ=DAILY;FREQ=WEEKLY",
            "FREQ=DAILY;COUNT=0",
            "FREQ=DAILY;INTERVAL=0",
            "FREQ=DAILY;COUNT=2;UNTIL=20260602",
            "FREQ=DAILY;BYDAY=1MO",
            "FREQ=WEEKLY;BYMONTHDAY=1",
            "FREQ=MONTHLY;BYYEARDAY=1",
            "FREQ=DAILY;BYWEEKNO=1",
            "FREQ=YEARLY;BYWEEKNO=1;BYDAY=1MO",
            "FREQ=MONTHLY;BYDAY=0MO",
            "FREQ=MONTHLY;BYDAY=54MO",
            "FREQ=DAILY;BYHOUR=24",
            "FREQ=DAILY;BYHOUR=+1",
            "FREQ=DAILY;BYMINUTE=60",
            "FREQ=DAILY;BYSECOND=61",
            "FREQ=DAILY;BYSETPOS=1",
            "FREQ=YEARLY;BYMONTH=13",
            "FREQ=DAILY;UNTIL=20260230",
            "FREQ=DAILY;WKST=XX",
            "FREQ=DAILY;UNKNOWN=1",
            "FREQ=DAILY;SKIP=OMIT",
        ] {
            assert!(value.parse::<RecurrenceRule>().is_err(), "{value}");
        }
    }

    #[test]
    fn typed_parts_preserve_the_original_rule() {
        let text = "FREQ=MONTHLY;BYDAY=-1FR;COUNT=3";
        let rule: RecurrenceRule = text.parse().unwrap();
        assert_eq!(rule.as_str(), text);
        assert_eq!(rule.count(), Some(3));
        assert_eq!(rule.interval(), 1);
        assert_eq!(
            rule.by_day(),
            &[RecurrenceDay {
                ordinal: Some(-1),
                weekday: jiff::civil::Weekday::Friday
            }]
        );
        assert!(matches!(
            "20260602".parse::<RecurrenceUntil>(),
            Ok(RecurrenceUntil::Date(_))
        ));
        assert!(matches!(
            "20260602T235960Z".parse::<RecurrenceUntil>(),
            Ok(RecurrenceUntil::DateTime {
                second: 60,
                utc: true,
                ..
            })
        ));
    }

    #[test]
    fn raw_observations_survive_a_failed_parse() {
        let recurrence = crate::EventRecurrence {
            rrule: Some("FREQ=UNKNOWN".into()),
            ..Default::default()
        };
        assert!(recurrence.parsed_rule().is_err());
        assert_eq!(recurrence.rrule.as_deref(), Some("FREQ=UNKNOWN"));
    }
}
