//! RFC 9404 JMAP Blob Management - Blob/upload, Blob/get, Blob/lookup.
//!
//! These are JMAP method calls (not HTTP endpoint operations).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Error;
use crate::core::id::{AccountId, BlobId};

// ---- Blob/upload (RFC 9404 §4.1) ----

/// Request for `Blob/upload` - create blobs via JMAP method call.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BlobUploadRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    /// Create entries keyed by consumer-provided create-id (e.g. "b1").
    #[serde(rename = "create")]
    create: HashMap<String, BlobUploadCreate>,
}

/// A single blob creation entry.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BlobUploadCreate {
    #[serde(rename = "data")]
    pub(crate) data: Vec<DataSource>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) type_: Option<String>,
}

/// Data source for blob assembly (RFC 9404 §4.1 DataSourceObject).
///
/// Each source provides either inline data or a reference to an existing
/// blob. Multiple sources are concatenated in order.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum DataSource {
    /// Reference an existing blob by ID, with optional byte range.
    Blob(DataSourceBlob),
    /// Inline text data.
    Text(DataSourceText),
    /// Inline base64-encoded binary data.
    Base64(DataSourceBase64),
}

/// Reference another blob (with optional range) as a data source.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DataSourceBlob {
    #[serde(rename = "blobId")]
    pub(crate) blob_id: BlobId,

    #[serde(rename = "offset")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) offset: Option<u64>,

    #[serde(rename = "length")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) length: Option<u64>,
}

/// Inline text data source.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DataSourceText {
    #[serde(rename = "data:asText")]
    pub(crate) value: String,
}

/// Inline base64-encoded binary data source.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DataSourceBase64 {
    #[serde(rename = "data:asBase64")]
    pub(crate) value: String,
}

/// Response for `Blob/upload`. Created/notCreated maps are keyed by
/// the consumer-provided create-id (e.g. "b1"), not real blob IDs.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BlobUploadResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "created")]
    created: Option<HashMap<String, BlobUploadCreated>>,

    #[serde(rename = "notCreated")]
    not_created: Option<HashMap<String, serde_json::Value>>,
}

/// Result of a successfully created blob.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BlobUploadCreated {
    #[serde(rename = "id")]
    pub(crate) id: BlobId,

    #[serde(rename = "type")]
    pub(crate) type_: Option<String>,

    #[serde(rename = "size")]
    pub(crate) size: Option<u64>,
}

impl crate::core::method::JmapMethod for BlobUploadRequest {
    const REPLAY_SAFE: bool = false;
    const NAME: &'static str = "Blob/upload";
    type Cap = crate::core::capability::Blob;
    type Response = BlobUploadResponse;

    fn set_account_id(&mut self, account_id: &AccountId) {
        self.account_id = account_id.clone();
    }
}

impl crate::core::method::JmapMethod for BlobGetRequest {
    const REPLAY_SAFE: bool = true;
    const NAME: &'static str = "Blob/get";
    type Cap = crate::core::capability::Blob;
    type Response = BlobGetResponse;

    fn set_account_id(&mut self, account_id: &AccountId) {
        self.account_id = account_id.clone();
    }
}

impl crate::core::method::JmapMethod for BlobLookupRequest {
    const REPLAY_SAFE: bool = true;
    const NAME: &'static str = "Blob/lookup";
    type Cap = crate::core::capability::Blob;
    type Response = BlobLookupResponse;

    fn set_account_id(&mut self, account_id: &AccountId) {
        self.account_id = account_id.clone();
    }
}

impl Default for BlobUploadRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobUploadRequest {
    pub(crate) fn new() -> Self {
        BlobUploadRequest {
            account_id: AccountId::new(""),
            create: HashMap::new(),
        }
    }

    /// Add a blob creation entry from inline text. Returns the create id.
    pub(crate) fn create_from_text(
        &mut self,
        text: impl Into<String>,
        type_: Option<impl Into<String>>,
    ) -> String {
        let create_id = format!("b{}", self.create.len());
        self.create.insert(
            create_id.clone(),
            BlobUploadCreate {
                data: vec![DataSource::Text(DataSourceText { value: text.into() })],
                type_: type_.map(std::convert::Into::into),
            },
        );
        create_id
    }

    /// Add a blob creation entry from inline base64 data. Returns the create id.
    pub(crate) fn create_from_base64(
        &mut self,
        base64: impl Into<String>,
        type_: Option<impl Into<String>>,
    ) -> String {
        let create_id = format!("b{}", self.create.len());
        self.create.insert(
            create_id.clone(),
            BlobUploadCreate {
                data: vec![DataSource::Base64(DataSourceBase64 {
                    value: base64.into(),
                })],
                type_: type_.map(std::convert::Into::into),
            },
        );
        create_id
    }

    /// Add a blob creation entry from a reference to an existing blob.
    /// Returns the create id.
    pub(crate) fn create_from_blob(
        &mut self,
        blob_id: impl Into<BlobId>,
        offset: Option<u64>,
        length: Option<u64>,
        type_: Option<impl Into<String>>,
    ) -> String {
        let create_id = format!("b{}", self.create.len());
        self.create.insert(
            create_id.clone(),
            BlobUploadCreate {
                data: vec![DataSource::Blob(DataSourceBlob {
                    blob_id: blob_id.into(),
                    offset,
                    length,
                })],
                type_: type_.map(std::convert::Into::into),
            },
        );
        create_id
    }

    /// Add a blob creation entry with arbitrary data sources (concatenated
    /// in order). Returns the create id.
    pub(crate) fn create_with_sources(
        &mut self,
        data: Vec<DataSource>,
        type_: Option<impl Into<String>>,
    ) -> String {
        let create_id = format!("b{}", self.create.len());
        self.create.insert(
            create_id.clone(),
            BlobUploadCreate {
                data,
                type_: type_.map(std::convert::Into::into),
            },
        );
        create_id
    }
}

impl BlobUploadResponse {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    /// Look up a successful or failed create by its create-id (e.g. "b1").
    pub(crate) fn created(&mut self, id: &str) -> crate::Result<BlobUploadCreated> {
        if let Some(result) = self.created.as_mut().and_then(|r| r.remove(id)) {
            Ok(result)
        } else if let Some(error) = self.not_created.as_ref().and_then(|r| r.get(id)) {
            Err(Error::NotParsable(format!(
                "Blob {id} not created: {error:?}"
            )))
        } else {
            Err(Error::IdNotFound(id.to_string()))
        }
    }

    /// Iterate the create-ids of successful uploads (consumer-provided
    /// "b1"/"b2"/..., not real blob IDs - real IDs live on each
    /// `BlobUploadCreated.id`).
    pub(crate) fn created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.created.as_ref().map(|map| map.keys())
    }
}

// ---- Blob/get (RFC 9404 §4.2) ----

/// Request for `Blob/get` - retrieve blob content.
///
/// Unlike most JMAP /get methods, the `properties` here are dynamic
/// names like `"data:asText"`, `"data:asBase64"`, `"digest:sha-256"`,
/// and `"size"`. The `offset` and `length` apply to all requested blobs.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BlobGetRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "ids")]
    ids: Vec<BlobId>,

    #[serde(rename = "properties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<Vec<String>>,

    #[serde(rename = "offset")]
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<u64>,

    #[serde(rename = "length")]
    #[serde(skip_serializing_if = "Option::is_none")]
    length: Option<u64>,
}

/// Response for `Blob/get`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BlobGetResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "list")]
    list: Vec<BlobGetResult>,

    #[serde(rename = "notFound")]
    not_found: Option<Vec<BlobId>>,
}

/// Result for a single blob get entry.
///
/// Response properties use dynamic keys (`data:asText`, `data:asBase64`,
/// `digest:sha-256`, etc.) so they are captured in a flat map via
/// `serde(flatten)`. Use the typed accessor methods to read common
/// fields, or access the `properties` map directly for digest values.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BlobGetResult {
    #[serde(rename = "id")]
    pub(crate) id: BlobId,

    #[serde(rename = "size")]
    pub(crate) size: Option<u64>,

    #[serde(rename = "isEncodingProblem")]
    pub(crate) is_encoding_problem: Option<bool>,

    #[serde(rename = "isTruncated")]
    pub(crate) is_truncated: Option<bool>,

    /// All dynamic properties including `data:asText`, `data:asBase64`,
    /// and `digest:<algorithm>` values.
    #[serde(flatten)]
    pub(crate) properties: HashMap<String, serde_json::Value>,
}

impl BlobGetResult {
    /// Get the blob data as text (`data:asText` property).
    pub(crate) fn data_as_text(&self) -> Option<&str> {
        self.properties.get("data:asText")?.as_str()
    }

    /// Get the blob data as base64 (`data:asBase64` property).
    pub(crate) fn data_as_base64(&self) -> Option<&str> {
        self.properties.get("data:asBase64")?.as_str()
    }

    /// Get a computed digest value by algorithm name.
    ///
    /// For example, `digest("sha-256")` reads the `digest:sha-256` property.
    pub(crate) fn digest(&self, algorithm: &str) -> Option<&str> {
        self.properties
            .get(&format!("digest:{algorithm}"))?
            .as_str()
    }
}

impl Default for BlobGetRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobGetRequest {
    pub(crate) fn new() -> Self {
        BlobGetRequest {
            account_id: AccountId::new(""),
            ids: Vec::new(),
            properties: None,
            offset: None,
            length: None,
        }
    }

    /// Add blob IDs to retrieve.
    #[must_use]
    pub(crate) fn ids<U, V>(mut self, ids: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<BlobId>,
    {
        self.ids
            .extend(ids.into_iter().map(std::convert::Into::into));
        self
    }

    /// Set which properties to return. Valid values include `"data:asText"`,
    /// `"data:asBase64"`, `"data"` (auto-detect), `"digest:sha"`,
    /// `"digest:sha-256"`, `"digest:sha-512"`, `"size"`.
    #[must_use]
    pub(crate) fn properties<U, V>(mut self, properties: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<String>,
    {
        self.properties = Some(
            properties
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        );
        self
    }

    /// Set the byte offset for all requested blobs.
    #[must_use]
    pub(crate) fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set the byte length for all requested blobs.
    #[must_use]
    pub(crate) fn length(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }
}

impl BlobGetResponse {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn list(&self) -> &[BlobGetResult] {
        &self.list
    }

    pub(crate) fn into_list(self) -> Vec<BlobGetResult> {
        self.list
    }

    pub(crate) fn not_found(&self) -> Option<&[BlobId]> {
        self.not_found.as_deref()
    }
}

// ---- Blob/lookup (RFC 9404 §4.3) ----

/// Request for `Blob/lookup` - reverse lookup which objects reference a blob.
///
/// **Important**: The JMAP request's `using` array must include the
/// capability that defines each type listed in `typeNames`. For example,
/// looking up `"Email"` requires `urn:ietf:params:jmap:mail`. Use
/// [`Request::add_capability`] to add the required capabilities, or use
/// the `lookup_blob_with_capabilities` helper which handles this
/// automatically for known types.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BlobLookupRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "typeNames")]
    type_names: Vec<String>,

    #[serde(rename = "ids")]
    ids: Vec<BlobId>,
}

/// Response for `Blob/lookup`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BlobLookupResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "list")]
    list: Vec<BlobLookupResult>,

    #[serde(rename = "notFound")]
    not_found: Option<Vec<BlobId>>,
}

/// Result for a single blob lookup entry.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BlobLookupResult {
    #[serde(rename = "id")]
    pub(crate) id: BlobId,

    /// Map of type name → list of object IDs that reference this blob.
    #[serde(rename = "matchedIds")]
    pub(crate) matched_ids: HashMap<String, Vec<String>>,
}

impl Default for BlobLookupRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobLookupRequest {
    pub(crate) fn new() -> Self {
        BlobLookupRequest {
            account_id: AccountId::new(""),
            type_names: Vec::new(),
            ids: Vec::new(),
        }
    }

    /// Set the JMAP type names to search (e.g. `"Email"`, `"CalendarEvent"`).
    ///
    /// The caller must ensure the corresponding capabilities are added
    /// to the request's `using` array.
    #[must_use]
    pub(crate) fn type_names<U, V>(mut self, type_names: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<String>,
    {
        self.type_names = type_names
            .into_iter()
            .map(std::convert::Into::into)
            .collect();
        self
    }

    /// Set the blob IDs to look up.
    #[must_use]
    pub(crate) fn ids<U, V>(mut self, ids: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<BlobId>,
    {
        self.ids = ids.into_iter().map(std::convert::Into::into).collect();
        self
    }
}

impl BlobLookupResponse {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn list(&self) -> &[BlobLookupResult] {
        &self.list
    }

    pub(crate) fn into_list(self) -> Vec<BlobLookupResult> {
        self.list
    }

    pub(crate) fn not_found(&self) -> Option<&[BlobId]> {
        self.not_found.as_deref()
    }
}
