use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::{
    id::{AccountId, BlobId},
    set::SetError,
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
    #[serde(skip)]
    pub(crate) answer_context: crate::core::answer::AnswerContext,
    #[serde(rename = "fromAccountId")]
    from_account_id: AccountId,
    #[serde(rename = "accountId")]
    account_id: AccountId,
    #[serde(rename = "copied")]
    #[serde(default, deserialize_with = "crate::core::id::echo::map")]
    copied: Option<HashMap<BlobId, BlobId>>,
    #[serde(rename = "notCopied")]
    #[serde(default, deserialize_with = "crate::core::id::echo::map")]
    not_copied: Option<HashMap<BlobId, SetError<String>>>,
}

impl crate::core::method::JmapMethod for CopyBlobRequest {
    const REPLAY_SAFE: bool = false;
    const NAME: &'static str = "Blob/copy";
    type Cap = crate::core::capability::Core;
    type Response = CopyBlobResponse;

    fn stamp_response(response: &mut Self::Response, context: crate::core::answer::AnswerContext) {
        response.answer_context = context;
    }

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
        crate::core::answer::IdAnswer::from_parts(
            self.copied.as_mut().and_then(|values| values.remove(id)),
            self.not_copied
                .as_mut()
                .and_then(|values| values.remove(id))
                .map(|error| crate::Error::Set(error.to_string_error())),
            false,
        )
        .into_result(id.as_str(), self.answer_context)
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
