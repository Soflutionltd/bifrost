use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{
    Error,
    core::{
        id::{AccountId, BlobId},
        set::SetError,
    },
};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CopyBlobRequest {
    #[serde(rename = "fromAccountId")]
    from_account_id: AccountId,
    #[serde(rename = "accountId")]
    account_id: AccountId,
    #[serde(rename = "blobIds")]
    blob_ids: Vec<BlobId>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CopyBlobResponse {
    #[serde(rename = "fromAccountId")]
    from_account_id: AccountId,
    #[serde(rename = "accountId")]
    account_id: AccountId,
    #[serde(rename = "copied")]
    copied: Option<HashMap<BlobId, BlobId>>,
    #[serde(rename = "notCopied")]
    not_copied: Option<HashMap<BlobId, SetError<String>>>,
}

impl crate::core::method::JmapMethod for CopyBlobRequest {
    const REPLAY_SAFE: bool = false;
    const NAME: &'static str = "Blob/copy";
    type Cap = crate::core::capability::Core;
    type Response = CopyBlobResponse;

    fn set_account_id(&mut self, account_id: &AccountId) {
        self.account_id = account_id.clone();
    }
}

impl CopyBlobRequest {
    /// Construct a `CopyBlobRequest`. The destination `accountId` is
    /// left empty and filled in by
    /// [`crate::core::request::Request::call`] when the method is
    /// added to a request batch (the destination is the account that
    /// owns the request). The source `fromAccountId` is the only
    /// account argument here, since it is genuinely a per-call value.
    pub(crate) fn new(from_account_id: impl Into<AccountId>) -> Self {
        CopyBlobRequest {
            from_account_id: from_account_id.into(),
            account_id: AccountId::new(""),
            blob_ids: vec![],
        }
    }

    #[must_use]
    pub(crate) fn blob_id(mut self, blob_id: impl Into<BlobId>) -> Self {
        self.blob_ids.push(blob_id.into());
        self
    }
}

impl CopyBlobResponse {
    pub(crate) fn from_account_id(&self) -> &AccountId {
        &self.from_account_id
    }

    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn copied(&mut self, id: &BlobId) -> crate::Result<BlobId> {
        if let Some(result) = self.copied.as_mut().and_then(|r| r.remove(id)) {
            Ok(result)
        } else if let Some(error) = self.not_copied.as_mut().and_then(|r| r.remove(id)) {
            Err(error.to_string_error().into())
        } else {
            Err(Error::IdNotFound(id.to_string()))
        }
    }

    pub(crate) fn copied_ids(&self) -> Option<impl Iterator<Item = &BlobId>> {
        self.copied.as_ref().map(|map| map.keys())
    }

    pub(crate) fn not_copied_ids(&self) -> Option<impl Iterator<Item = &BlobId>> {
        self.not_copied.as_ref().map(|map| map.keys())
    }

    pub(crate) fn not_copied_reason(&self, id: &BlobId) -> Option<&SetError<String>> {
        self.not_copied.as_ref().and_then(|map| map.get(id))
    }
}
