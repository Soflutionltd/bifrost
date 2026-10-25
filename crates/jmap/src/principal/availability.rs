use serde::{Deserialize, Serialize};

use crate::core::id::AccountId;
use crate::principal::PrincipalId;

/// Request for `Principal/getAvailability`.
///
/// Given a principal and time range, returns free/busy availability.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct PrincipalGetAvailabilityRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "id")]
    id: PrincipalId,

    #[serde(rename = "utcStart")]
    utc_start: String,

    #[serde(rename = "utcEnd")]
    utc_end: String,

    #[serde(rename = "showDetails")]
    #[serde(skip_serializing_if = "Option::is_none")]
    show_details: Option<bool>,
}

/// Response for `Principal/getAvailability`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PrincipalGetAvailabilityResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "list")]
    list: Vec<AvailabilityEntry>,
}

/// A single availability entry (busy period).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AvailabilityEntry {
    #[serde(rename = "utcStart")]
    pub(crate) utc_start: String,

    #[serde(rename = "utcEnd")]
    pub(crate) utc_end: String,

    #[serde(rename = "busyStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) busy_status: Option<String>,

    #[serde(rename = "event")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) event: Option<serde_json::Value>,
}

impl crate::core::method::JmapMethod for PrincipalGetAvailabilityRequest {
    const REPLAY_SAFE: bool = true;
    const NAME: &'static str = "Principal/getAvailability";
    type Cap = crate::core::capability::Principals;
    type Response = PrincipalGetAvailabilityResponse;

    fn set_account_id(&mut self, account_id: &AccountId) {
        self.account_id = account_id.clone();
    }
}

impl PrincipalGetAvailabilityRequest {
    pub(crate) fn new(
        id: impl Into<PrincipalId>,
        utc_start: impl Into<String>,
        utc_end: impl Into<String>,
    ) -> Self {
        PrincipalGetAvailabilityRequest {
            account_id: AccountId::new(""),
            id: id.into(),
            utc_start: utc_start.into(),
            utc_end: utc_end.into(),
            show_details: None,
        }
    }

    #[must_use]
    pub(crate) fn show_details(mut self, show: bool) -> Self {
        self.show_details = Some(show);
        self
    }
}

impl PrincipalGetAvailabilityResponse {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn list(&self) -> &[AvailabilityEntry] {
        &self.list
    }

    pub(crate) fn into_list(self) -> Vec<AvailabilityEntry> {
        self.list
    }
}
