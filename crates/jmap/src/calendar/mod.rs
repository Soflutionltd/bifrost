pub(crate) mod get;
pub(crate) mod set;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::field::Field;

use crate::calendar_event::Alert;

mod marker {
    pub(crate) enum Calendar {}
}
/// Strongly-typed Calendar ID.
pub(crate) type CalendarId = crate::core::id::Id<marker::Calendar>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Calendar {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<CalendarId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "description")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) description: Field<String>,

    #[serde(rename = "color")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) color: Field<String>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "isVisible")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_visible: Option<bool>,

    #[serde(rename = "isDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_default: Option<bool>,

    #[serde(rename = "includeInAvailability")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) include_in_availability: Option<IncludeInAvailability>,

    #[serde(rename = "defaultAlertsWithTime")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) default_alerts_with_time: Field<HashMap<String, Alert>>,

    #[serde(rename = "defaultAlertsWithoutTime")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) default_alerts_without_time: Field<HashMap<String, Alert>>,

    #[serde(rename = "timeZone")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) time_zone: Field<String>,

    #[serde(rename = "shareWith")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) share_with: Field<HashMap<String, CalendarRights>>,

    #[serde(rename = "myRights")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) my_rights: Option<CalendarRights>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CalendarCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<crate::core::creation::CreationId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "description")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) description: Field<String>,

    #[serde(rename = "color")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) color: Field<String>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "isVisible")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_visible: Option<bool>,

    #[serde(rename = "includeInAvailability")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) include_in_availability: Option<IncludeInAvailability>,

    #[serde(rename = "defaultAlertsWithTime")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) default_alerts_with_time: Field<HashMap<String, Alert>>,

    #[serde(rename = "defaultAlertsWithoutTime")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) default_alerts_without_time: Field<HashMap<String, Alert>>,

    #[serde(rename = "timeZone")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) time_zone: Field<String>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) share_with: Field<HashMap<String, CalendarRights>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct CalendarPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "description")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) description: Field<String>,

    #[serde(rename = "color")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) color: Field<String>,

    #[serde(rename = "sortOrder")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sort_order: Option<u32>,

    #[serde(rename = "isSubscribed")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_subscribed: Option<bool>,

    #[serde(rename = "isVisible")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_visible: Option<bool>,

    #[serde(rename = "includeInAvailability")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) include_in_availability: Option<IncludeInAvailability>,

    #[serde(rename = "defaultAlertsWithTime")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) default_alerts_with_time: Field<HashMap<String, Alert>>,

    #[serde(rename = "defaultAlertsWithoutTime")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) default_alerts_without_time: Field<HashMap<String, Alert>>,

    #[serde(rename = "timeZone")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) time_zone: Field<String>,

    #[serde(rename = "shareWith")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) share_with: Field<HashMap<String, CalendarRights>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum IncludeInAvailability {
    #[serde(rename = "all")]
    All,
    #[serde(rename = "attending")]
    Attending,
    #[serde(rename = "none")]
    None,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CalendarRights {
    #[serde(rename = "mayReadFreeBusy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_read_free_busy: Option<bool>,

    #[serde(rename = "mayReadItems")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_read_items: Option<bool>,

    #[serde(rename = "mayWriteAll")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_write_all: Option<bool>,

    #[serde(rename = "mayWriteOwn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_write_own: Option<bool>,

    #[serde(rename = "mayUpdatePrivate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_update_private: Option<bool>,

    #[serde(rename = "mayRSVP")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_rsvp: Option<bool>,

    #[serde(rename = "mayShare")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_share: Option<bool>,

    #[serde(rename = "mayDelete")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) may_delete: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct CalendarSetArguments {
    #[serde(rename = "onDestroyRemoveEvents")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) on_destroy_remove_events: Option<bool>,

    #[serde(rename = "onSuccessSetIsDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) on_success_set_is_default: Option<String>,
}

impl CalendarSetArguments {
    pub(crate) fn on_destroy_remove_events(&mut self, remove: bool) -> &mut Self {
        self.on_destroy_remove_events = Some(remove);
        self
    }

    pub(crate) fn on_success_set_is_default(&mut self, id: impl Into<String>) -> &mut Self {
        self.on_success_set_is_default = Some(id.into());
        self
    }
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Name => "name",
        Description => "description",
        Color => "color",
        SortOrder => "sortOrder",
        IsSubscribed => "isSubscribed",
        IsVisible => "isVisible",
        IsDefault => "isDefault",
        IncludeInAvailability => "includeInAvailability",
        DefaultAlertsWithTime => "defaultAlertsWithTime",
        DefaultAlertsWithoutTime => "defaultAlertsWithoutTime",
        TimeZone => "timeZone",
        ShareWith => "shareWith",
        MyRights => "myRights",
    }
}

impl crate::core::Object for Calendar {
    type Property = Property;
    type Id = CalendarId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Calendar {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for Calendar {
    type GetArguments = ();
}

impl crate::core::set::SetObject for Calendar {
    type Create = CalendarCreate;
    type Patch = CalendarPatch;
    type SetArguments = CalendarSetArguments;
}

impl crate::core::SetCreate for CalendarCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.as_ref().map(ToString::to_string)
    }
    fn set_create_id(&mut self, id: crate::core::creation::CreationId) {
        self._create_id = Some(id);
    }

    fn new(create_id: Option<crate::core::creation::CreationId>) -> Self {
        CalendarCreate {
            _create_id: create_id,
            name: None,
            description: Field::Omitted,
            color: Field::Omitted,
            sort_order: None,
            is_subscribed: None,
            is_visible: None,
            include_in_availability: None,
            default_alerts_with_time: Field::Omitted,
            default_alerts_without_time: Field::Omitted,
            time_zone: Field::Omitted,
            share_with: Field::Omitted,
        }
    }
}

crate::define_get_method!(
    CalendarGet,
    Calendar,
    "Calendar/get",
    crate::core::capability::Calendars
);
crate::define_set_method!(
    CalendarSet,
    Calendar,
    "Calendar/set",
    crate::core::capability::Calendars
);
crate::define_changes_method!(
    CalendarChanges,
    Calendar,
    "Calendar/changes",
    crate::core::capability::Calendars
);

impl CalendarSet {
    #[must_use]
    pub(crate) fn on_destroy_remove_events(mut self, remove: bool) -> Self {
        self.arguments().on_destroy_remove_events(remove);
        self
    }

    #[must_use]
    pub(crate) fn on_success_set_is_default(mut self, id: impl Into<String>) -> Self {
        self.arguments().on_success_set_is_default(id);
        self
    }
}
