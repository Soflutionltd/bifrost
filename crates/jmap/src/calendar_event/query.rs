use super::value::LocalDateTime;

use serde::Serialize;

use crate::core::query::{self, QueryObject};

use super::{CalendarEvent, QueryArguments};
use crate::calendar::CalendarId;

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Filter {
    /// Filter by calendar ID (singular). Used by Stalwart.
    InCalendar {
        #[serde(rename = "inCalendar")]
        value: CalendarId,
    },
    /// Filter by calendar IDs (plural, spec draft).
    InCalendars {
        #[serde(rename = "inCalendars")]
        value: Vec<CalendarId>,
    },
    Uid {
        #[serde(rename = "uid")]
        value: String,
    },
    After {
        #[serde(rename = "after")]
        value: LocalDateTime,
    },
    Before {
        #[serde(rename = "before")]
        value: LocalDateTime,
    },
    Text {
        #[serde(rename = "text")]
        value: String,
    },
    Title {
        #[serde(rename = "title")]
        value: String,
    },
    Description {
        #[serde(rename = "description")]
        value: String,
    },
    Location {
        #[serde(rename = "location")]
        value: String,
    },
    Owner {
        #[serde(rename = "owner")]
        value: String,
    },
    Attendee {
        #[serde(rename = "attendee")]
        value: String,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub(crate) enum Comparator {
    #[serde(rename = "start")]
    Start,
    #[serde(rename = "uid")]
    Uid,
    #[serde(rename = "recurrenceId")]
    RecurrenceId,
    #[serde(rename = "created")]
    Created,
    #[serde(rename = "updated")]
    Updated,
}

impl Filter {
    /// Filter by a single calendar ID. Used by Stalwart.
    pub(crate) fn in_calendar(value: impl Into<CalendarId>) -> Self {
        Filter::InCalendar {
            value: value.into(),
        }
    }

    /// Filter by multiple calendar IDs (spec draft).
    pub(crate) fn in_calendars<U, V>(value: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<CalendarId>,
    {
        Filter::InCalendars {
            value: value.into_iter().map(std::convert::Into::into).collect(),
        }
    }

    pub(crate) fn uid(value: impl Into<String>) -> Self {
        Filter::Uid {
            value: value.into(),
        }
    }

    pub(crate) fn after(value: LocalDateTime) -> Self {
        Filter::After { value }
    }

    pub(crate) fn before(value: LocalDateTime) -> Self {
        Filter::Before { value }
    }

    pub(crate) fn text(value: impl Into<String>) -> Self {
        Filter::Text {
            value: value.into(),
        }
    }

    pub(crate) fn title(value: impl Into<String>) -> Self {
        Filter::Title {
            value: value.into(),
        }
    }

    pub(crate) fn description(value: impl Into<String>) -> Self {
        Filter::Description {
            value: value.into(),
        }
    }

    pub(crate) fn location(value: impl Into<String>) -> Self {
        Filter::Location {
            value: value.into(),
        }
    }

    pub(crate) fn owner(value: impl Into<String>) -> Self {
        Filter::Owner {
            value: value.into(),
        }
    }

    pub(crate) fn attendee(value: impl Into<String>) -> Self {
        Filter::Attendee {
            value: value.into(),
        }
    }
}

impl Comparator {
    pub(crate) fn start() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Start)
    }

    pub(crate) fn uid() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Uid)
    }

    pub(crate) fn recurrence_id() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::RecurrenceId)
    }

    pub(crate) fn created() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Created)
    }

    pub(crate) fn updated() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Updated)
    }
}

impl QueryObject for CalendarEvent {
    type QueryArguments = QueryArguments;
    type Filter = Filter;
    type Sort = Comparator;
}
