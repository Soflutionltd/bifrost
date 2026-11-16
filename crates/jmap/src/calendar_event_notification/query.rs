use serde::Serialize;

use crate::core::query::{self, QueryObject};

use super::CalendarEventNotification;

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Filter {
    Type {
        #[serde(rename = "type")]
        value: String,
    },
    CalendarEventId {
        #[serde(rename = "calendarEventId")]
        value: crate::calendar_event::CalendarEventId,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub(crate) enum Comparator {
    #[serde(rename = "created")]
    Created,
}

impl Filter {
    pub(crate) fn type_(value: impl Into<String>) -> Self {
        Filter::Type {
            value: value.into(),
        }
    }

    pub(crate) fn calendar_event_id(
        value: impl Into<crate::calendar_event::CalendarEventId>,
    ) -> Self {
        Filter::CalendarEventId {
            value: value.into(),
        }
    }
}

impl Comparator {
    pub(crate) fn created() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Created)
    }
}

impl QueryObject for CalendarEventNotification {
    type QueryArguments = ();
    type Filter = Filter;
    type Sort = Comparator;
}

#[cfg(test)]
mod wire_tests;
