// Stage 4 (calendar conveniences) will wire these RFC types into the
// Account impl; until then the module is fully built but unused.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod set;

use serde::{Deserialize, Serialize};

mod marker {
    pub(crate) enum ParticipantIdentity {}
}
/// Strongly-typed ParticipantIdentity ID.
pub(crate) type ParticipantIdentityId = crate::core::id::Id<marker::ParticipantIdentity>;

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct ParticipantIdentitySetArguments {
    #[serde(rename = "onSuccessSetIsDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) on_success_set_is_default: Option<ParticipantIdentityId>,
}

impl ParticipantIdentitySetArguments {
    pub(crate) fn on_success_set_is_default(
        &mut self,
        id: impl Into<ParticipantIdentityId>,
    ) -> &mut Self {
        self.on_success_set_is_default = Some(id.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ParticipantIdentity {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<ParticipantIdentityId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "calendarAddress")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) calendar_address: Option<String>,

    #[serde(rename = "isDefault")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_default: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ParticipantIdentityCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "calendarAddress")]
    pub(super) calendar_address: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct ParticipantIdentityPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    // Omitted-or-String, deliberately not `Field<String>`: draft-26 §3
    // makes `calendarAddress` required and non-nullable, so the null
    // (removal) state a `Field` would offer is always an
    // `invalidProperties` error on the wire.
    #[serde(rename = "calendarAddress")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) calendar_address: Option<String>,
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Name => "name",
        CalendarAddress => "calendarAddress",
        IsDefault => "isDefault",
    }
}

impl crate::core::Object for ParticipantIdentity {
    type Property = Property;
    type Id = ParticipantIdentityId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for ParticipantIdentity {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for ParticipantIdentity {
    type GetArguments = ();
}

impl crate::core::set::SetObject for ParticipantIdentity {
    type Create = ParticipantIdentityCreate;
    type Patch = ParticipantIdentityPatch;
    type SetArguments = ParticipantIdentitySetArguments;
}

impl crate::core::SetCreate for ParticipantIdentityCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        ParticipantIdentityCreate {
            _create_id: create_id,
            name: None,
            calendar_address: String::new(),
        }
    }
}

crate::define_get_method!(
    ParticipantIdentityGet,
    ParticipantIdentity,
    "ParticipantIdentity/get",
    crate::core::capability::Calendars
);
crate::define_set_method!(
    ParticipantIdentitySet,
    ParticipantIdentity,
    "ParticipantIdentity/set",
    crate::core::capability::Calendars
);
crate::define_changes_method!(
    ParticipantIdentityChanges,
    ParticipantIdentity,
    "ParticipantIdentity/changes",
    crate::core::capability::Calendars
);

impl ParticipantIdentitySet {
    #[must_use]
    pub(crate) fn on_success_set_is_default(
        mut self,
        id: impl Into<ParticipantIdentityId>,
    ) -> Self {
        self.arguments().on_success_set_is_default(id);
        self
    }
}
