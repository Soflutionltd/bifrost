use serde::Deserialize;

use crate::{
    account::Account,
    blob::BlobRef,
    client::Client,
    core::{
        id::{AccountId, BlobId},
        session::{URLPart, encode_template_value_at},
        transport::HttpTransport,
    },
};

/// Server response shape for an HTTP upload (RFC 8620 §6.1).
///
/// The deserialized form is mostly internal: callers receive a
/// [`BlobRef`] from [`Account::upload`] and don't need to handle
/// `UploadResponse` directly. It is `pub` so the lower-level
/// [`Client::upload_to`] is usable when the caller needs the raw
/// `accountId` echo or `size` field.
#[derive(Debug, Deserialize)]
pub(crate) struct UploadResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "blobId")]
    blob_id: BlobId,

    #[serde(rename = "type")]
    type_: String,

    #[serde(rename = "size")]
    size: usize,
}

impl<Tr: HttpTransport> Client<Tr> {
    /// Upload `data` to the named account's blob store. Lower-level
    /// counterpart to [`Account::upload`] for callers that need the
    /// raw [`UploadResponse`] (echoing server-reported size, etc.).
    pub(crate) async fn upload_to(
        &self,
        account_id: &AccountId,
        data: impl Into<Vec<u8>>,
        content_type: Option<&str>,
    ) -> crate::Result<UploadResponse> {
        let state = self.session_state();
        let mut upload_url =
            String::with_capacity(state.session().upload_url().len() + account_id.as_str().len());

        for part in state.upload_url() {
            match part {
                URLPart::Value(value) => upload_url.push_str(value),
                URLPart::Parameter(param) => {
                    if let super::URLParameter::AccountId = param {
                        upload_url
                            .push_str(&encode_template_value_at(account_id.as_str(), &upload_url));
                    }
                }
            }
        }

        let bytes = self
            .transport()
            .upload(&upload_url, data.into(), content_type)
            .await
            .map_err(crate::Error::from)?;
        serde_json::from_slice::<UploadResponse>(&bytes).map_err(std::convert::Into::into)
    }
}

impl<Tr: HttpTransport> Account<Tr> {
    /// Upload `data` to this account's blob store.
    ///
    /// The returned [`BlobRef`] is bound to this account and carries
    /// the server-assigned `blobId` and content type. The hint name
    /// (used by `Client::download` to populate
    /// `Content-Disposition`) defaults to `None`; chain
    /// [`BlobRef::with_name`] if you have one.
    ///
    /// Asymmetric placement vs. [`Client::download`] is intentional:
    /// the upload URL is templated with `accountId`, so it needs an
    /// account context that does not yet exist in `BlobRef` form.
    /// After the upload the returned `BlobRef` records the binding,
    /// and `Client::download(&blob_ref)` works against any account
    /// the ref points at.
    pub(crate) async fn upload(
        &self,
        data: impl Into<Vec<u8>>,
        content_type: Option<&str>,
    ) -> crate::Result<BlobRef> {
        let response = self
            .client()
            .upload_to(self.id(), data, content_type)
            .await?;

        let mut blob = BlobRef::new(response.account_id, response.blob_id);
        if !response.type_.is_empty() {
            blob.content_type = Some(response.type_);
        }
        Ok(blob)
    }
}

impl UploadResponse {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn blob_id(&self) -> &BlobId {
        &self.blob_id
    }

    pub(crate) fn content_type(&self) -> &str {
        &self.type_
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn into_blob_id(self) -> BlobId {
        self.blob_id
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use serde_json::json;

    use crate::{
        client::Client,
        core::{
            id::AccountId,
            session::Session,
            transport::{HttpTransport, TransportError},
        },
    };

    struct UrlCapture(Arc<Mutex<Option<String>>>);

    impl HttpTransport for UrlCapture {
        async fn api_request(&self, _url: &str, _body: Vec<u8>) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not call the API"))
        }

        async fn upload(
            &self,
            url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            *self.0.lock().expect("capture lock") = Some(url.to_string());
            Ok(Bytes::from_static(
                br#"{"accountId":"a","blobId":"b","type":"text/plain","size":4}"#,
            ))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not download"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport has no session route"))
        }
    }

    fn session() -> Session {
        serde_json::from_value(json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/dl/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload?account={accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session fixture parses")
    }

    /// Same RFC 6570 §3.2.2 rule as the download template: the account
    /// id is expanded with everything outside the unreserved set
    /// percent-encoded, not just the two query delimiters.
    #[tokio::test]
    async fn account_id_is_rfc6570_level_one_encoded() {
        let captured = Arc::new(Mutex::new(None));
        let client = Client::with_transport(
            UrlCapture(Arc::clone(&captured)),
            session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");

        client
            .upload_to(&AccountId::new("a&b=c+d;e@f"), b"data".to_vec(), None)
            .await
            .expect("upload runs");

        let url = captured.lock().expect("capture lock").clone().expect("url");
        assert_eq!(
            url,
            "https://example.test/upload?account=a%26b%3Dc%2Bd%3Be%40f"
        );
    }
}
