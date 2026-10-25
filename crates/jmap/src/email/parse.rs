use serde::{Deserialize, Serialize};

use super::{BodyProperty, Email, Property};
use crate::Error;
use crate::core::id::{AccountId, BlobId};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EmailParseRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "blobIds")]
    blob_ids: Vec<BlobId>,

    #[serde(rename = "properties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<Vec<Property>>,

    #[serde(rename = "bodyProperties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    body_properties: Option<Vec<BodyProperty>>,

    #[serde(rename = "fetchTextBodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_text_body_values: Option<bool>,

    #[serde(rename = "fetchHTMLBodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_html_body_values: Option<bool>,

    #[serde(rename = "fetchAllBodyValues")]
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_all_body_values: Option<bool>,

    #[serde(rename = "maxBodyValueBytes")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_body_value_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct EmailParseResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    /// Successfully parsed emails, keyed by the source blob id.
    #[serde(rename = "parsed")]
    parsed: Option<HashMap<BlobId, Email>>,

    #[serde(rename = "notParsable")]
    not_parsable: Option<Vec<BlobId>>,

    #[serde(rename = "notFound")]
    not_found: Option<Vec<BlobId>>,
}

impl crate::core::method::JmapMethod for EmailParseRequest {
    const REPLAY_SAFE: bool = true;
    const NAME: &'static str = "Email/parse";
    type Cap = crate::core::capability::Mail;
    type Response = EmailParseResponse;

    fn set_account_id(&mut self, account_id: &AccountId) {
        self.account_id = account_id.clone();
    }
}

impl Default for EmailParseRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl EmailParseRequest {
    pub(crate) fn new() -> Self {
        EmailParseRequest {
            account_id: AccountId::new(""),
            blob_ids: Vec::new(),
            properties: None,
            body_properties: None,
            fetch_text_body_values: None,
            fetch_html_body_values: None,
            fetch_all_body_values: None,
            max_body_value_bytes: None,
        }
    }

    #[must_use]
    pub(crate) fn blob_ids<U, V>(mut self, blob_ids: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<BlobId>,
    {
        self.blob_ids = blob_ids.into_iter().map(std::convert::Into::into).collect();
        self
    }

    #[must_use]
    pub(crate) fn properties(mut self, properties: impl IntoIterator<Item = Property>) -> Self {
        self.properties = Some(properties.into_iter().collect());
        self
    }

    #[must_use]
    pub(crate) fn body_properties(
        mut self,
        body_properties: impl IntoIterator<Item = BodyProperty>,
    ) -> Self {
        self.body_properties = Some(body_properties.into_iter().collect());
        self
    }

    #[must_use]
    pub(crate) fn fetch_text_body_values(mut self, fetch_text_body_values: bool) -> Self {
        self.fetch_text_body_values = fetch_text_body_values.into();
        self
    }

    #[must_use]
    pub(crate) fn fetch_html_body_values(mut self, fetch_html_body_values: bool) -> Self {
        self.fetch_html_body_values = fetch_html_body_values.into();
        self
    }

    #[must_use]
    pub(crate) fn fetch_all_body_values(mut self, fetch_all_body_values: bool) -> Self {
        self.fetch_all_body_values = fetch_all_body_values.into();
        self
    }

    #[must_use]
    pub(crate) fn max_body_value_bytes(mut self, max_body_value_bytes: usize) -> Self {
        self.max_body_value_bytes = max_body_value_bytes.into();
        self
    }
}

impl EmailParseResponse {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn parsed(&mut self, blob_id: &BlobId) -> crate::Result<Email> {
        if let Some(result) = self.parsed.as_mut().and_then(|r| r.remove(blob_id)) {
            Ok(result)
        } else if self
            .not_parsable
            .as_ref()
            .map(|np| np.iter().any(|id| id == blob_id))
            .unwrap_or(false)
        {
            Err(Error::NotParsable(blob_id.to_string()))
        } else {
            Err(Error::IdNotFound(blob_id.to_string()))
        }
    }

    pub(crate) fn parsed_list(&self) -> Option<impl Iterator<Item = (&BlobId, &Email)>> {
        self.parsed.as_ref().map(|map| map.iter())
    }

    pub(crate) fn not_parsable(&self) -> Option<&[BlobId]> {
        self.not_parsable.as_deref()
    }

    pub(crate) fn not_found(&self) -> Option<&[BlobId]> {
        self.not_found.as_deref()
    }
}
