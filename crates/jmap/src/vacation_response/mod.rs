// The Account impl reads/writes the singleton vacation response; the
// builder helpers on the get/set request structs not driven by the
// Account path are retained for symmetry with the rest of the crate.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod set;

use crate::core::field::Field;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

mod marker {
    pub(crate) enum VacationResponse {}
}
/// Strongly-typed VacationResponse ID (singleton; spec mandates "singleton").
pub(crate) type VacationResponseId = crate::core::id::Id<marker::VacationResponse>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VacationResponse {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<VacationResponseId>,

    #[serde(rename = "isEnabled")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_enabled: Option<bool>,

    #[serde(rename = "fromDate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) from_date: Option<Timestamp>,

    #[serde(rename = "toDate")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) to_date: Option<Timestamp>,

    #[serde(rename = "subject")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) subject: Option<String>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_body: Option<String>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_body: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VacationResponseCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<crate::core::creation::CreationId>,

    #[serde(rename = "isEnabled")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_enabled: Option<bool>,

    #[serde(rename = "fromDate")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) from_date: Field<Timestamp>,

    #[serde(rename = "toDate")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) to_date: Field<Timestamp>,

    #[serde(rename = "subject")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) subject: Field<String>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) text_body: Field<String>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) html_body: Field<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct VacationResponsePatch {
    #[serde(rename = "isEnabled")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_enabled: Option<bool>,

    #[serde(rename = "fromDate")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) from_date: Field<Timestamp>,

    #[serde(rename = "toDate")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) to_date: Field<Timestamp>,

    #[serde(rename = "subject")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) subject: Field<String>,

    #[serde(rename = "textBody")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) text_body: Field<String>,

    #[serde(rename = "htmlBody")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) html_body: Field<String>,
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        IsEnabled => "isEnabled",
        FromDate => "fromDate",
        ToDate => "toDate",
        Subject => "subject",
        TextBody => "textBody",
        HtmlBody => "htmlBody",
    }
}

impl crate::core::Object for VacationResponse {
    type Property = Property;
    type Id = VacationResponseId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for VacationResponse {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for VacationResponse {
    type GetArguments = ();
}

impl crate::core::set::SetObject for VacationResponse {
    type Create = VacationResponseCreate;
    type Patch = VacationResponsePatch;
    type SetArguments = ();
}

impl crate::core::SetCreate for VacationResponseCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.as_ref().map(ToString::to_string)
    }
    fn set_create_id(&mut self, id: crate::core::creation::CreationId) {
        self._create_id = Some(id);
    }

    fn new(create_id: Option<crate::core::creation::CreationId>) -> Self {
        VacationResponseCreate {
            _create_id: create_id,
            is_enabled: None,
            from_date: Field::Omitted,
            to_date: Field::Omitted,
            subject: Field::Omitted,
            text_body: Field::Omitted,
            html_body: Field::Omitted,
        }
    }
}

crate::define_get_method!(
    VacationResponseGet,
    VacationResponse,
    "VacationResponse/get",
    crate::core::capability::VacationResponseCap
);
crate::define_set_method!(
    VacationResponseSet,
    VacationResponse,
    "VacationResponse/set",
    crate::core::capability::VacationResponseCap
);
