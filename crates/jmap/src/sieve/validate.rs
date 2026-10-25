use serde::{Deserialize, Serialize};

use crate::core::id::{AccountId, BlobId};
use crate::core::set::SetError;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SieveScriptValidateRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "blobId")]
    blob_id: BlobId,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SieveScriptValidateResponse {
    error: Option<SetError<String>>,
}

impl crate::core::method::JmapMethod for SieveScriptValidateRequest {
    const REPLAY_SAFE: bool = true;
    const NAME: &'static str = "SieveScript/validate";
    type Cap = crate::core::capability::Sieve;
    type Response = SieveScriptValidateResponse;

    fn set_account_id(&mut self, account_id: &AccountId) {
        self.account_id = account_id.clone();
    }
}

impl SieveScriptValidateRequest {
    pub(crate) fn new(blob_id: impl Into<BlobId>) -> Self {
        SieveScriptValidateRequest {
            account_id: AccountId::new(""),
            blob_id: blob_id.into(),
        }
    }
}

impl SieveScriptValidateResponse {
    pub(crate) fn into_error(self) -> Option<SetError<String>> {
        self.error
    }
}
