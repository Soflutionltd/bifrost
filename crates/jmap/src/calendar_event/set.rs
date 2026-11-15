use super::value::{Duration, LocalDateTime};

use serde_json::json;

use super::{CalendarEventCreate, CalendarEventPatch};
use crate::calendar::CalendarId;

impl CalendarEventCreate {
    pub(crate) fn calendar_ids<U, V>(&mut self, ids: U) -> &mut Self
    where
        U: IntoIterator<Item = V>,
        V: Into<CalendarId>,
    {
        crate::core::patch::IdSet::replace(
            &mut self.properties,
            "calendarIds",
            ids.into_iter().map(|id| id.into().into_string()),
        );
        self
    }

    pub(crate) fn calendar_id(&mut self, id: impl Into<CalendarId>, set: bool) -> &mut Self {
        crate::core::patch::IdSet::member(
            &mut self.properties,
            "calendarIds",
            id.into().into_string(),
            set,
        );
        self
    }
}

impl CalendarEventPatch {
    pub(crate) fn calendar_ids<U, V>(&mut self, ids: U) -> &mut Self
    where
        U: IntoIterator<Item = V>,
        V: Into<CalendarId>,
    {
        crate::core::patch::IdSetPatch::replace(
            &mut self.properties,
            "calendarIds",
            ids.into_iter().map(|id| id.into().into_string()),
        );
        self
    }

    pub(crate) fn calendar_id(&mut self, id: impl Into<CalendarId>, set: bool) -> &mut Self {
        crate::core::patch::IdSetPatch::member(
            &mut self.properties,
            "calendarIds",
            id.into().into_string(),
            set,
        );
        self
    }
}

macro_rules! ce_setters {
    ($t:ty) => {
        impl $t {
            pub(crate) fn uid(&mut self, uid: impl Into<String>) -> &mut Self {
                self.properties
                    .insert("uid".into(), serde_json::Value::String(uid.into()));
                self
            }

            pub(crate) fn title(&mut self, title: impl Into<String>) -> &mut Self {
                self.properties
                    .insert("title".into(), serde_json::Value::String(title.into()));
                self
            }

            pub(crate) fn description(&mut self, description: impl Into<String>) -> &mut Self {
                self.properties.insert(
                    "description".into(),
                    serde_json::Value::String(description.into()),
                );
                self
            }

            pub(crate) fn description_content_type(
                &mut self,
                content_type: impl Into<String>,
            ) -> &mut Self {
                self.properties.insert(
                    "descriptionContentType".into(),
                    serde_json::Value::String(content_type.into()),
                );
                self
            }

            pub(crate) fn start(&mut self, start: LocalDateTime) -> &mut Self {
                self.properties
                    .insert("start".into(), serde_json::Value::String(start.into()));
                self
            }

            pub(crate) fn duration(&mut self, duration: Duration) -> &mut Self {
                self.properties.insert(
                    "duration".into(),
                    serde_json::Value::String(duration.into()),
                );
                self
            }

            pub(crate) fn time_zone(&mut self, time_zone: Option<impl Into<String>>) -> &mut Self {
                self.properties.insert(
                    "timeZone".into(),
                    match time_zone {
                        Some(tz) => serde_json::Value::String(tz.into()),
                        None => serde_json::Value::Null,
                    },
                );
                self
            }

            pub(crate) fn show_without_time(&mut self, show: bool) -> &mut Self {
                self.properties
                    .insert("showWithoutTime".into(), json!(show));
                self
            }

            pub(crate) fn status(&mut self, status: impl Into<String>) -> &mut Self {
                self.properties
                    .insert("status".into(), serde_json::Value::String(status.into()));
                self
            }

            pub(crate) fn free_busy_status(&mut self, status: impl Into<String>) -> &mut Self {
                self.properties.insert(
                    "freeBusyStatus".into(),
                    serde_json::Value::String(status.into()),
                );
                self
            }

            pub(crate) fn recurrence_rules(&mut self, rules: Vec<serde_json::Value>) -> &mut Self {
                self.properties
                    .insert("recurrenceRules".into(), serde_json::Value::Array(rules));
                self
            }

            pub(crate) fn recurrence_overrides(
                &mut self,
                overrides: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties.insert(
                    "recurrenceOverrides".into(),
                    serde_json::Value::Object(overrides),
                );
                self
            }

            pub(crate) fn excluded_recurrence_rules(
                &mut self,
                rules: Vec<serde_json::Value>,
            ) -> &mut Self {
                self.properties.insert(
                    "excludedRecurrenceRules".into(),
                    serde_json::Value::Array(rules),
                );
                self
            }

            pub(crate) fn priority(&mut self, priority: u8) -> &mut Self {
                self.properties.insert("priority".into(), json!(priority));
                self
            }

            pub(crate) fn color(&mut self, color: Option<impl Into<String>>) -> &mut Self {
                self.properties.insert(
                    "color".into(),
                    match color {
                        Some(c) => serde_json::Value::String(c.into()),
                        None => serde_json::Value::Null,
                    },
                );
                self
            }

            pub(crate) fn locale(&mut self, locale: Option<impl Into<String>>) -> &mut Self {
                self.properties.insert(
                    "locale".into(),
                    match locale {
                        Some(l) => serde_json::Value::String(l.into()),
                        None => serde_json::Value::Null,
                    },
                );
                self
            }

            pub(crate) fn keywords(
                &mut self,
                keywords: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("keywords".into(), serde_json::Value::Object(keywords));
                self
            }

            pub(crate) fn categories(
                &mut self,
                categories: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("categories".into(), serde_json::Value::Object(categories));
                self
            }

            pub(crate) fn reply_to(
                &mut self,
                reply_to: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("replyTo".into(), serde_json::Value::Object(reply_to));
                self
            }

            pub(crate) fn participants(
                &mut self,
                participants: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties.insert(
                    "participants".into(),
                    serde_json::Value::Object(participants),
                );
                self
            }

            pub(crate) fn use_default_alerts(&mut self, use_default: bool) -> &mut Self {
                self.properties
                    .insert("useDefaultAlerts".into(), json!(use_default));
                self
            }

            pub(crate) fn alerts(
                &mut self,
                alerts: Option<serde_json::Map<String, serde_json::Value>>,
            ) -> &mut Self {
                self.properties.insert(
                    "alerts".into(),
                    match alerts {
                        Some(a) => serde_json::Value::Object(a),
                        None => serde_json::Value::Null,
                    },
                );
                self
            }

            pub(crate) fn locations(
                &mut self,
                locations: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("locations".into(), serde_json::Value::Object(locations));
                self
            }

            pub(crate) fn virtual_locations(
                &mut self,
                virtual_locations: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties.insert(
                    "virtualLocations".into(),
                    serde_json::Value::Object(virtual_locations),
                );
                self
            }

            pub(crate) fn links(
                &mut self,
                links: serde_json::Map<String, serde_json::Value>,
            ) -> &mut Self {
                self.properties
                    .insert("links".into(), serde_json::Value::Object(links));
                self
            }
        }
    };
}

ce_setters!(CalendarEventCreate);
ce_setters!(CalendarEventPatch);
