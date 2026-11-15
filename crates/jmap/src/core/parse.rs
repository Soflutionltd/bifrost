use std::collections::HashMap;

use serde::Deserialize;

use super::id::{AccountId, BlobId};

#[derive(Debug, Clone, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub(crate) struct ParseResponse<T> {
    #[serde(skip)]
    pub(crate) answer_context: crate::core::answer::AnswerContext,
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "parsed")]
    #[serde(default, deserialize_with = "crate::core::id::echo::map")]
    parsed: Option<HashMap<BlobId, T>>,

    #[serde(rename = "notParsable")]
    #[serde(default, deserialize_with = "crate::core::id::echo::optional_vec")]
    not_parsable: Option<Vec<BlobId>>,

    #[serde(rename = "notFound")]
    #[serde(default, deserialize_with = "crate::core::id::echo::optional_vec")]
    not_found: Option<Vec<BlobId>>,
}

impl<T> ParseResponse<T> {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn parsed(&mut self, blob_id: &BlobId) -> crate::Result<T> {
        crate::core::answer::IdAnswer::from_parts(
            self.parsed
                .as_mut()
                .and_then(|values| values.remove(blob_id)),
            self.not_parsable
                .as_ref()
                .filter(|ids| ids.contains(blob_id))
                .map(|_| crate::Error::NotParsable(blob_id.to_string())),
            self.not_found
                .as_ref()
                .is_some_and(|ids| ids.contains(blob_id)),
        )
        .into_result(blob_id.as_str(), self.answer_context)
    }

    pub(crate) fn parsed_list(&self) -> Option<impl Iterator<Item = (&BlobId, &T)>> {
        self.parsed.as_ref().map(|map| map.iter())
    }

    pub(crate) fn not_parsable(&self) -> Option<&[BlobId]> {
        self.not_parsable.as_deref()
    }

    pub(crate) fn not_found(&self) -> Option<&[BlobId]> {
        self.not_found.as_deref()
    }
}
