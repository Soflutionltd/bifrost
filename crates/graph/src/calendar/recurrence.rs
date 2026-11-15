//! Graph patternedRecurrence projection over shared RFC 5545 validity.

use super::time_zone::resolve_time_zone;
use super::wire::{GraphRecurrence, GraphRecurrencePattern, GraphRecurrenceRange};
use bifrost_types::EventTime;
use jiff::civil;
use jiff::tz::Offset;
use jiff::tz::TimeZone;

pub(crate) fn rrule_from_graph(recurrence: &GraphRecurrence) -> Option<String> {
    let pattern = recurrence.pattern.as_ref()?;
    let mut parts = Vec::new();
    match pattern.kind.as_deref()? {
        "daily" => parts.push("FREQ=DAILY".to_string()),
        "weekly" => {
            parts.push("FREQ=WEEKLY".to_string());
            if let Some(days) = pattern.days_of_week.as_ref() {
                let byday = days
                    .iter()
                    .filter_map(|day| graph_day_to_rrule(day))
                    .collect::<Vec<_>>();
                if !byday.is_empty() {
                    parts.push(format!("BYDAY={}", byday.join(",")));
                }
            }
        }
        "absoluteMonthly" => {
            parts.push("FREQ=MONTHLY".to_string());
            if let Some(day) = pattern.day_of_month {
                parts.push(format!("BYMONTHDAY={day}"));
            }
        }
        "relativeMonthly" => {
            parts.push("FREQ=MONTHLY".to_string());
            push_relative_rule_parts(pattern, &mut parts);
        }
        "absoluteYearly" => {
            parts.push("FREQ=YEARLY".to_string());
            if let Some(month) = pattern.month {
                parts.push(format!("BYMONTH={month}"));
            }
            if let Some(day) = pattern.day_of_month {
                parts.push(format!("BYMONTHDAY={day}"));
            }
        }
        "relativeYearly" => {
            parts.push("FREQ=YEARLY".to_string());
            if let Some(month) = pattern.month {
                parts.push(format!("BYMONTH={month}"));
            }
            push_relative_rule_parts(pattern, &mut parts);
        }
        _ => return None,
    }
    if let Some(interval) = pattern.interval.filter(|interval| *interval > 1) {
        parts.push(format!("INTERVAL={interval}"));
        // The week start changes a weekly series only when it skips weeks.
        // RFC 5545 assumes Monday; Graph's documented default is Sunday.
        if pattern.kind.as_deref() == Some("weekly") {
            let week_start = pattern.first_day_of_week.as_deref().unwrap_or("sunday");
            if let Some(day) = graph_day_to_rrule(week_start).filter(|day| *day != "MO") {
                parts.push(format!("WKST={day}"));
            }
        }
    }
    if let Some(range) = recurrence.range.as_ref() {
        match range.kind.as_deref() {
            Some("numbered") => {
                if let Some(count) = range.number_of_occurrences {
                    parts.push(format!("COUNT={count}"));
                }
            }
            Some("endDate") => {
                if let Some(end) = range.end_date.as_deref() {
                    parts.push(format!("UNTIL={}", end.replace('-', "")));
                }
            }
            _ => {}
        }
    }
    Some(parts.join(";"))
}

pub(crate) fn push_relative_rule_parts(pattern: &GraphRecurrencePattern, parts: &mut Vec<String>) {
    if let Some(days) = pattern.days_of_week.as_ref() {
        let byday = days
            .iter()
            .filter_map(|day| graph_day_to_rrule(day))
            .collect::<Vec<_>>();
        if !byday.is_empty() {
            parts.push(format!("BYDAY={}", byday.join(",")));
        }
    }
    if let Some(index) = pattern.index.as_deref().and_then(graph_index_to_setpos) {
        parts.push(format!("BYSETPOS={index}"));
    }
}

/// Why an RRULE cannot be written to Graph, classified by what the refusal is
/// about. A well-formed rule that Graph's `patternedRecurrence` (or this
/// mapping) cannot express is `Unsupported`: the gap is the provider's or this
/// client's, not the caller's. A value that is not an RFC 5545 RRULE at all is
/// the caller's malformed input, `Request(Malformed)` naming
/// `recurrence.rrule`. Either way the write is refused: dropping the rule would
/// turn a recurring event into a one-off, and dropping one part of it would
/// write a different series than the one asked for.
#[derive(Debug)]
pub(crate) enum RecurrenceRefusal {
    Unsupported(String),
    Malformed {
        field: &'static str,
        message: String,
    },
}

pub(crate) fn unexpressible(message: impl Into<String>) -> RecurrenceRefusal {
    RecurrenceRefusal::Unsupported(message.into())
}

pub(crate) fn malformed_rrule(message: impl Into<String>) -> RecurrenceRefusal {
    RecurrenceRefusal::Malformed {
        field: "recurrence.rrule",
        message: message.into(),
    }
}

/// Graph's pattern shape for a validated RRULE. A `None` is a value RFC 5545
/// takes from DTSTART when the rule leaves it out; `anchored` fills it from
/// the event start.
#[derive(Debug)]
pub(crate) enum PatternShape {
    Daily,
    /// `None` is the start's weekday.
    Weekly(Option<Vec<&'static str>>),
    /// `None` is the start's day of the month.
    AbsoluteMonthly(Option<u32>),
    RelativeMonthly {
        days: Vec<&'static str>,
        index: &'static str,
    },
    /// `None`s are the start's month and day.
    AbsoluteYearly {
        month: Option<u32>,
        day: Option<u32>,
    },
    RelativeYearly {
        month: u32,
        days: Vec<&'static str>,
        index: &'static str,
    },
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RRuleUntil {
    /// A DATE, or a floating DATE-TIME: already the event's local date.
    Local(civil::Date),
    /// A UTC DATE-TIME, whose local date depends on the event's time zone.
    Utc(civil::DateTime),
}

/// A validated RRULE in Graph's terms, still waiting for the event start it
/// is anchored on.
#[derive(Debug)]
pub(crate) struct GraphRecurrenceRule {
    pub(crate) shape: PatternShape,
    pub(crate) interval: u32,
    /// Graph name of the RRULE's WKST, RFC 5545's default Monday when absent.
    pub(crate) first_day_of_week: &'static str,
    pub(crate) count: Option<u32>,
    pub(crate) until: Option<RRuleUntil>,
}

/// What a rule is anchored on: the event's local start date, and the zone a
/// UTC `UNTIL` is read in. `zone` is `None` when the event's zone cannot be
/// resolved, which leaves such an `UNTIL` on its UTC date.
#[derive(Debug, Clone)]
pub(crate) struct RecurrenceAnchor {
    pub(crate) start: civil::Date,
    pub(crate) zone: Option<TimeZone>,
}

impl RecurrenceAnchor {
    pub(crate) fn from_event_time(time: &EventTime) -> Result<Self, RecurrenceRefusal> {
        let start = parse_start_date(&time.value).ok_or_else(|| RecurrenceRefusal::Malformed {
            field: "start",
            message: "Graph recurrence needs the event start to begin with a YYYY-MM-DD date"
                .to_string(),
        })?;
        Ok(Self {
            start,
            zone: resolve_time_zone(time.timezone.as_deref().unwrap_or("UTC")),
        })
    }
}

pub(crate) fn parse_start_date(value: &str) -> Option<civil::Date> {
    value.get(..10)?.parse().ok()
}

pub(crate) const GRAPH_WEEKDAYS: [&str; 7] = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
];

pub(crate) fn graph_weekday(date: civil::Date) -> &'static str {
    GRAPH_WEEKDAYS[usize::from(date.weekday().to_sunday_zero_offset().unsigned_abs())]
}

/// The Gregorian calendar repeats every 400 years: a month's length depends
/// only on its month and on its year modulo 400.
pub(crate) const GREGORIAN_CYCLE_MONTHS: i64 = 400 * 12;

/// Days in a month, counted by its index `year * 12 + month - 1`. The year is
/// reduced into one Gregorian cycle first, which leaves the answer unchanged
/// and keeps any index inside the range `civil::Date` accepts.
pub(crate) fn days_in_indexed_month(index: i64) -> i64 {
    let year = 2000 + (index.div_euclid(12) - 2000).rem_euclid(400);
    let month = index.rem_euclid(12) + 1;
    match (i16::try_from(year), i8::try_from(month)) {
        (Ok(year), Ok(month)) => civil::Date::new(year, month, 1)
            .map(|first| i64::from(first.days_in_month()))
            .unwrap_or(31),
        _ => 31,
    }
}

/// Refuse an absolute series that visits a month lacking its day.
///
/// RFC 5545 skips a recurrence date that does not exist (the 31st of a
/// 30-day month, February 29th outside a leap year). Graph's absolute
/// patterns do not skip: Exchange moves the occurrence to the month's last
/// day. Writing such a series would add occurrences the rule never asked for,
/// so it is refused. Only the months the series actually visits count: the
/// 31st every sixth month from July lands on July and January only and is
/// written as is.
///
/// The walk runs from the start's month in `step_months` steps; a candidate
/// before the start is not an occurrence, `COUNT` counts only real
/// occurrences, and `UNTIL` ends the series. A month's length depends only on
/// its position in the 400-year cycle, and the positions a series visits
/// repeat after at most one cycle's worth of steps, so walking that many
/// steps sees every month the series can ever reach: an open-ended or long
/// series is decided exactly, not by a horizon.
pub(crate) fn check_absolute_series(
    start: civil::Date,
    first_month: u32,
    step_months: u32,
    day: u32,
    count: Option<u32>,
    until: Option<civil::Date>,
) -> Result<(), RecurrenceRefusal> {
    let month_index = |date: civil::Date| i64::from(date.year()) * 12 + i64::from(date.month()) - 1;
    let first = i64::from(start.year()) * 12 + i64::from(first_month) - 1;
    let start_index = month_index(start);
    let until = until.map(|until| (month_index(until), i64::from(until.day())));
    let step = i64::from(step_months.max(1));
    let day = i64::from(day);
    let mut occurrences = 0u32;
    for steps in 0..=GREGORIAN_CYCLE_MONTHS {
        if count.is_some_and(|count| occurrences >= count) {
            break;
        }
        let index = first + steps * step;
        if until.is_some_and(|(until_index, _)| index > until_index) {
            break;
        }
        // A yearly series starts its walk at BYMONTH in the start's year,
        // which can lie before the start's month: not an occurrence, whether
        // or not that month has the day.
        if index < start_index {
            continue;
        }
        let in_until_month = until.filter(|(until_index, _)| index == *until_index);
        let last_day = days_in_indexed_month(index);
        if day > last_day {
            // Graph puts this occurrence on the month's last day; past UNTIL
            // it would not be written at all, and the series is over.
            if in_until_month.is_some_and(|(_, until_day)| last_day > until_day) {
                break;
            }
            // A date the month does not have always falls after a start in
            // that month, so it is a skipped occurrence, never a pre-start one.
            let (year, month) = (index.div_euclid(12), index.rem_euclid(12) + 1);
            return Err(unexpressible(format!(
                "Graph recurrence cannot express day {day} in a series that reaches \
                 {year}-{month:02}, which lacks it: Graph moves such an occurrence to the \
                 month's last day where RFC 5545 skips it"
            )));
        }
        let before_start = index == start_index && day < i64::from(start.day());
        let after_until = in_until_month.is_some_and(|(_, until_day)| day > until_day);
        if !before_start && !after_until {
            occurrences += 1;
        }
    }
    Ok(())
}

impl GraphRecurrenceRule {
    pub(crate) fn anchored(
        self,
        anchor: &RecurrenceAnchor,
    ) -> Result<GraphRecurrence, RecurrenceRefusal> {
        let start = anchor.start;
        let start_day = u32::from(start.day().unsigned_abs());
        let start_month = u32::from(start.month().unsigned_abs());
        let names =
            |days: Vec<&'static str>| Some(days.into_iter().map(ToString::to_string).collect());
        let mut pattern = GraphRecurrencePattern {
            kind: None,
            interval: Some(self.interval),
            month: None,
            day_of_month: None,
            days_of_week: None,
            first_day_of_week: None,
            index: None,
        };
        let until = self.until.map(|until| match until {
            RRuleUntil::Local(date) => date,
            RRuleUntil::Utc(at) => anchor
                .zone
                .as_ref()
                .and_then(|zone| {
                    let instant = Offset::UTC.to_timestamp(at).ok()?;
                    Some(zone.to_datetime(instant).date())
                })
                .unwrap_or_else(|| at.date()),
        });
        let kind = match self.shape {
            PatternShape::Daily => "daily",
            PatternShape::Weekly(days) => {
                pattern.days_of_week = names(days.unwrap_or_else(|| vec![graph_weekday(start)]));
                pattern.first_day_of_week = Some(self.first_day_of_week.to_string());
                "weekly"
            }
            PatternShape::AbsoluteMonthly(day) => {
                let day = day.unwrap_or(start_day);
                check_absolute_series(start, start_month, self.interval, day, self.count, until)?;
                pattern.day_of_month = Some(day);
                "absoluteMonthly"
            }
            PatternShape::RelativeMonthly { days, index } => {
                pattern.days_of_week = names(days);
                pattern.index = Some(index.to_string());
                "relativeMonthly"
            }
            PatternShape::AbsoluteYearly { month, day } => {
                let month = month.unwrap_or(start_month);
                let day = day.unwrap_or(start_day);
                check_absolute_series(
                    start,
                    month,
                    self.interval.saturating_mul(12),
                    day,
                    self.count,
                    until,
                )?;
                pattern.month = Some(month);
                pattern.day_of_month = Some(day);
                "absoluteYearly"
            }
            PatternShape::RelativeYearly { month, days, index } => {
                pattern.month = Some(month);
                pattern.days_of_week = names(days);
                pattern.index = Some(index.to_string());
                "relativeYearly"
            }
        };
        pattern.kind = Some(kind.to_string());
        let range_kind = if self.count.is_some() {
            "numbered"
        } else if until.is_some() {
            "endDate"
        } else {
            "noEnd"
        };
        Ok(GraphRecurrence {
            pattern: Some(pattern),
            range: Some(GraphRecurrenceRange {
                kind: Some(range_kind.to_string()),
                start_date: Some(start.to_string()),
                end_date: until.as_ref().map(ToString::to_string),
                recurrence_time_zone: None,
                number_of_occurrences: self.count,
            }),
        })
    }
}

#[cfg(test)]
pub(crate) fn graph_recurrence_from_rrule(
    rrule: &str,
    start_date: &str,
) -> Result<GraphRecurrence, RecurrenceRefusal> {
    let anchor = RecurrenceAnchor {
        start: parse_start_date(start_date)
            .ok_or_else(|| malformed_rrule("test start is not a date"))?,
        zone: None,
    };
    graph_recurrence_rule(rrule)?.anchored(&anchor)
}

/// Map an RRULE onto a Graph recurrence pattern, or refuse it.
///
/// Every part must land in the pattern with its meaning intact. Equivalent
/// shapes are rewritten: every-day-on-these-weekdays (DAILY with BYDAY) and
/// every-such-weekday of each month or year are weekly patterns, a lone
/// ordinal weekday (`2MO`, `-1FR`) is a relative pattern's index, and values
/// RFC 5545 takes from DTSTART come from the event start. A part Graph would
/// ignore for the chosen pattern type (BYDAY on an absolute pattern, BYMONTH
/// on anything but yearly, BYSETPOS on an absolute pattern) would widen or
/// narrow the series without the caller's knowledge, so it is refused rather
/// than sent.
pub(crate) fn graph_recurrence_rule(rrule: &str) -> Result<GraphRecurrenceRule, RecurrenceRefusal> {
    let parsed = rrule
        .parse::<bifrost_types::calendar::RecurrenceRule>()
        .map_err(|error| malformed_rrule(error.to_string()))?;
    for (key, _) in parsed.parts() {
        if ![
            "FREQ",
            "INTERVAL",
            "BYMONTH",
            "BYMONTHDAY",
            "BYDAY",
            "BYSETPOS",
            "COUNT",
            "UNTIL",
            "WKST",
        ]
        .contains(&key)
        {
            return Err(unexpressible(format!(
                "Graph recurrence cannot express RRULE part {key}"
            )));
        }
    }
    let frequency = parsed
        .value("FREQ")
        .ok_or_else(|| malformed_rrule("RRULE has no FREQ"))?
        .to_ascii_uppercase();
    if !["DAILY", "WEEKLY", "MONTHLY", "YEARLY"].contains(&frequency.as_str()) {
        return Err(unexpressible(format!(
            "Graph recurrence has no FREQ={frequency}"
        )));
    }
    let interval = parsed.interval();
    let count = parsed.count();
    let until = parsed.until().map(project_until).transpose()?;
    let month = rrule_single_number(&parsed, "BYMONTH")?;
    let day_of_month = rrule_single_number(&parsed, "BYMONTHDAY")?;
    if day_of_month.is_some_and(|day| day < 0) {
        return Err(unexpressible(
            "Graph recurrence cannot count BYMONTHDAY from the end of the month",
        ));
    }
    let set_pos = rrule_single_number(&parsed, "BYSETPOS")?;
    let index = set_pos
        .map(|position| {
            rrule_setpos_to_graph(position).ok_or_else(|| {
                unexpressible(format!(
                    "Graph recurrence index has no BYSETPOS={position}; it holds 1 to 4 or -1"
                ))
            })
        })
        .transpose()?;
    let month = month.map(i32::unsigned_abs);
    let day_of_month = day_of_month.map(i32::unsigned_abs);
    let days = parsed.value("BYDAY").map(|_| {
        parsed
            .by_day()
            .iter()
            .map(|day| {
                let name =
                    GRAPH_WEEKDAYS[usize::from(day.weekday.to_sunday_zero_offset().unsigned_abs())];
                RRuleByDay {
                    ordinal: day.ordinal.map(i32::from),
                    day: name,
                }
            })
            .collect::<Vec<_>>()
    });
    // WKST is carried as Graph's `firstDayOfWeek` on weekly patterns, the
    // only Graph pattern it changes; elsewhere RFC 5545 gives it meaning only
    // alongside BYWEEKNO, which is refused above.
    // The shared grammar already validates WKST. Its API retains only text,
    // so this lookup projects it to Graph spelling; keep a fallible boundary
    // rather than panic or silently substitute Monday if that contract changes.
    let first_day_of_week = parsed
        .value("WKST")
        .map(|week_start| {
            rrule_day_to_graph(week_start).ok_or_else(|| {
                malformed_rrule(format!("RRULE WKST {week_start:?} is not a weekday"))
            })
        })
        .transpose()?
        .unwrap_or("monday");

    let forbid = |key: &str, present: bool| {
        if present {
            Err(unexpressible(format!(
                "Graph recurrence cannot express {key} with FREQ={frequency} in this shape"
            )))
        } else {
            Ok(())
        }
    };
    let shape = match frequency.as_str() {
        "DAILY" => {
            forbid("BYMONTH", month.is_some())?;
            forbid("BYMONTHDAY", day_of_month.is_some())?;
            forbid("BYSETPOS", index.is_some())?;
            match days {
                None => PatternShape::Daily,
                // Every day, kept to the listed weekdays, is a weekly pattern
                // on those days. With INTERVAL > 1 the day stride and the week
                // no longer line up.
                Some(days) if interval == 1 => PatternShape::Weekly(Some(plain_days(&days))),
                Some(_) => {
                    return Err(unexpressible(
                        "Graph recurrence cannot express BYDAY on a FREQ=DAILY rule with \
                         INTERVAL > 1",
                    ));
                }
            }
        }
        "WEEKLY" => {
            forbid("BYMONTH", month.is_some())?;
            forbid("BYMONTHDAY", day_of_month.is_some())?;
            forbid("BYSETPOS", index.is_some())?;
            PatternShape::Weekly(days.as_deref().map(plain_days))
        }
        "MONTHLY" => {
            // BYMONTH on a MONTHLY rule keeps some months only; Graph's
            // monthly patterns run every month.
            forbid("BYMONTH", month.is_some())?;
            if let Some(day) = day_of_month {
                forbid("BYDAY", days.is_some())?;
                forbid("BYSETPOS", index.is_some())?;
                PatternShape::AbsoluteMonthly(Some(day))
            } else if let Some(days) = days {
                match relative_days(&days, index)? {
                    Some((days, index)) => PatternShape::RelativeMonthly { days, index },
                    // Every listed weekday of every month is every such
                    // weekday: a weekly pattern.
                    None if interval == 1 => PatternShape::Weekly(Some(plain_days(&days))),
                    None => {
                        return Err(unexpressible(
                            "Graph recurrence cannot express every BYDAY weekday of every n-th \
                             month",
                        ));
                    }
                }
            } else {
                forbid("BYSETPOS", index.is_some())?;
                PatternShape::AbsoluteMonthly(None)
            }
        }
        // YEARLY; FREQ was validated above.
        _ => {
            if let Some(day) = day_of_month {
                forbid("BYDAY", days.is_some())?;
                forbid("BYSETPOS", index.is_some())?;
                let Some(month) = month else {
                    return Err(unexpressible(
                        "Graph recurrence cannot express BYMONTHDAY in every month of a \
                         FREQ=YEARLY rule without BYMONTH",
                    ));
                };
                PatternShape::AbsoluteYearly {
                    month: Some(month),
                    day: Some(day),
                }
            } else if let Some(days) = days {
                let plain = index.is_none() && days.iter().all(|day| day.ordinal.is_none());
                match month {
                    // With BYMONTH, a BYDAY ordinal counts within the month.
                    Some(month) => match relative_days(&days, index)? {
                        Some((days, index)) => PatternShape::RelativeYearly { month, days, index },
                        None => {
                            return Err(unexpressible(
                                "Graph recurrence cannot express every BYDAY weekday of one \
                                 month",
                            ));
                        }
                    },
                    // Every listed weekday of every year is a weekly pattern.
                    None if plain && interval == 1 => PatternShape::Weekly(Some(plain_days(&days))),
                    None => {
                        return Err(unexpressible(
                            "Graph recurrence cannot express a BYDAY weekday counted through a \
                             whole year",
                        ));
                    }
                }
            } else {
                forbid("BYSETPOS", index.is_some())?;
                PatternShape::AbsoluteYearly { month, day: None }
            }
        }
    };
    Ok(GraphRecurrenceRule {
        shape,
        interval,
        first_day_of_week,
        count,
        until,
    })
}

/// One BYDAY item: a Graph weekday name and its RFC 5545 ordinal, if any.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RRuleByDay {
    pub(crate) ordinal: Option<i32>,
    pub(crate) day: &'static str,
}

pub(crate) fn plain_days(days: &[RRuleByDay]) -> Vec<&'static str> {
    days.iter().map(|day| day.day).collect()
}

/// The `daysOfWeek` and `index` of a Graph relative pattern, or `None` when
/// the BYDAY list names every such weekday (no position at all).
///
/// Graph's `index` picks ONE day out of the `daysOfWeek` set, which is exactly
/// BYSETPOS over plain weekdays. An ordinal weekday maps the same way only on
/// its own: `1MO,1TU` is the first Monday AND the first Tuesday, two
/// occurrences, where Graph's first of Monday-or-Tuesday is one.
pub(crate) fn relative_days(
    days: &[RRuleByDay],
    index: Option<&'static str>,
) -> Result<Option<(Vec<&'static str>, &'static str)>, RecurrenceRefusal> {
    if days.iter().all(|day| day.ordinal.is_none()) {
        return Ok(index.map(|index| (plain_days(days), index)));
    }
    match days {
        [
            RRuleByDay {
                ordinal: Some(ordinal),
                day,
            },
        ] if index.is_none() => rrule_setpos_to_graph(*ordinal)
            .map(|index| Some((vec![*day], index)))
            .ok_or_else(|| {
                unexpressible(format!(
                    "Graph recurrence index has no position {ordinal}; it holds 1 to 4 or -1"
                ))
            }),
        _ => Err(unexpressible(
            "Graph recurrence cannot express several ordinal BYDAY values, or an ordinal BYDAY \
             with BYSETPOS",
        )),
    }
}

/// These numeric lists were validated by the shared grammar. Graph can
/// carry only one item; cardinality is provider expressibility, not validity.
fn rrule_single_number(
    parsed: &bifrost_types::calendar::RecurrenceRule,
    key: &str,
) -> Result<Option<i32>, RecurrenceRefusal> {
    let Some(value) = parsed.value(key) else {
        return Ok(None);
    };
    if value.contains(',') {
        return Err(unexpressible(format!(
            "Graph recurrence holds a single {key} value"
        )));
    }
    value
        .parse::<i32>()
        .map(Some)
        .map_err(|_| unexpressible(format!("Graph cannot express {key}={value}")))
}

// Same Sunday-first indices as GRAPH_WEEKDAYS and jiff's weekday offsets.
const RRULE_WEEKDAYS: [&str; 7] = ["SU", "MO", "TU", "WE", "TH", "FR", "SA"];

pub(crate) fn graph_day_to_rrule(day: &str) -> Option<&'static str> {
    GRAPH_WEEKDAYS
        .iter()
        .position(|name| *name == day)
        .map(|index| RRULE_WEEKDAYS[index])
}

pub(crate) fn rrule_day_to_graph(day: &str) -> Option<&'static str> {
    RRULE_WEEKDAYS
        .iter()
        .position(|name| name.eq_ignore_ascii_case(day))
        .map(|index| GRAPH_WEEKDAYS[index])
}

pub(crate) fn graph_index_to_setpos(index: &str) -> Option<i32> {
    match index {
        "first" => Some(1),
        "second" => Some(2),
        "third" => Some(3),
        "fourth" => Some(4),
        "last" => Some(-1),
        _ => None,
    }
}

pub(crate) fn rrule_setpos_to_graph(position: i32) -> Option<&'static str> {
    match position {
        1 => Some("first"),
        2 => Some("second"),
        3 => Some("third"),
        4 => Some("fourth"),
        -1 => Some("last"),
        _ => None,
    }
}

/// RFC 5545 UNTIL is a DATE (`YYYYMMDD`), a floating DATE-TIME
/// (`YYYYMMDDTHHMMSS`) or a UTC DATE-TIME (the same with `Z`). Graph's range
/// end is a local date, which a UTC value only yields once the event's time
/// zone is known.
fn project_until(
    until: bifrost_types::calendar::RecurrenceUntil,
) -> Result<RRuleUntil, RecurrenceRefusal> {
    use bifrost_types::calendar::RecurrenceUntil;
    match until {
        RecurrenceUntil::Date(date) => Ok(RRuleUntil::Local(date)),
        RecurrenceUntil::DateTime {
            date,
            hour,
            minute,
            second,
            utc,
        } => {
            let hour = i8::try_from(hour)
                .map_err(|_| unexpressible("Graph UNTIL hour is out of range"))?;
            let minute = i8::try_from(minute)
                .map_err(|_| unexpressible("Graph UNTIL minute is out of range"))?;
            let second = i8::try_from(second)
                .map_err(|_| unexpressible("Graph UNTIL second is out of range"))?;
            let time = civil::Time::new(hour, minute, second, 0)
                .map_err(|_| unexpressible("Graph cannot express an UNTIL leap second"))?;
            Ok(if utc {
                RRuleUntil::Utc(date.to_datetime(time))
            } else {
                RRuleUntil::Local(date)
            })
        }
        _ => Err(unexpressible("Graph cannot express this UNTIL shape")),
    }
}
