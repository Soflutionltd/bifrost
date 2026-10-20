use crate::{
    blob::BlobRef,
    client::Client,
    core::{
        session::{URLPart, encode_template_value_at},
        transport::HttpTransport,
    },
};

impl<Tr: HttpTransport> Client<Tr> {
    /// Download a blob.
    ///
    /// The account ID, blob ID, and (when the server's download URL
    /// template references them) `name` and `type` come from the
    /// `BlobRef`. RFC 8620 §6 specifies `accountId` as required, and
    /// `name` / `type` as advisory hints used to populate
    /// `Content-Disposition` and `Content-Type` on the response. When
    /// the ref leaves them as `None` and the template needs a value,
    /// neutral defaults (`"download"`, `"application/octet-stream"`)
    /// are substituted.
    ///
    /// Every substituted value is percent-encoded per RFC 6570 §3.2.2
    /// simple string expansion, so a content type like `"image/png"` or
    /// `"application/ld+json"` reaches the server intact rather than
    /// splitting a path segment or decoding `+` as a space.
    pub(crate) async fn download(&self, blob: &BlobRef) -> crate::Result<bytes::Bytes> {
        let state = self.session_state();
        let mut download_url = String::with_capacity(state.session().download_url().len() + 64);

        for part in state.download_url() {
            match part {
                URLPart::Value(value) => download_url.push_str(value),
                URLPart::Parameter(param) => match param {
                    super::URLParameter::AccountId => {
                        download_url.push_str(&encode_template_value_at(
                            blob.account_id.as_str(),
                            &download_url,
                        ));
                    }
                    super::URLParameter::BlobId => {
                        download_url.push_str(&encode_template_value_at(
                            blob.blob_id.as_str(),
                            &download_url,
                        ));
                    }
                    super::URLParameter::Name => {
                        let name = blob.name.as_deref().unwrap_or("download");
                        download_url.push_str(&encode_template_value_at(name, &download_url));
                    }
                    super::URLParameter::Type => {
                        let ctype = blob
                            .content_type
                            .as_deref()
                            .unwrap_or("application/octet-stream");
                        download_url.push_str(&encode_template_value_at(ctype, &download_url));
                    }
                },
            }
        }

        // Measured, not plain: blob octets are the bulk of the traffic a
        // raw-message or blob read causes, and a metered handle whose
        // batch omitted them would report a count that leaves out
        // traffic it caused.
        let (bytes, bytes_in) = self
            .transport()
            .download_measured(&download_url)
            .await
            .map_err(crate::Error::from)?;
        self.record_bytes_in(bytes_in);
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use serde_json::json;

    use crate::{
        blob::BlobRef,
        client::Client,
        core::{
            id::{AccountId, BlobId},
            session::Session,
            transport::{HttpTransport, TransportError},
        },
    };

    /// Captures the URL the client asked to download from.
    struct UrlCapture(Arc<Mutex<Option<String>>>);

    impl HttpTransport for UrlCapture {
        async fn api_request(&self, _url: &str, _body: Vec<u8>) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not call the API"))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not upload"))
        }

        async fn download(&self, url: &str) -> Result<Bytes, TransportError> {
            *self.0.lock().expect("capture lock") = Some(url.to_string());
            Ok(Bytes::from_static(b"blob"))
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
            "downloadUrl": "https://example.test/dl/{accountId}/{blobId}?name={name}&type={type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session fixture parses")
    }

    /// RFC 6570 §3.2.2: everything outside the unreserved set is
    /// percent-encoded. A deny list that stops at `&` and `=` leaves
    /// `+`, `;`, `@`, `!`, `,` and friends to be reinterpreted by the
    /// server - `application/ld+json` would arrive as
    /// `application/ld json`.
    #[tokio::test]
    async fn template_values_are_rfc6570_level_one_encoded() {
        let captured = Arc::new(Mutex::new(None));
        let client = Client::with_transport(
            UrlCapture(Arc::clone(&captured)),
            session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");

        let blob = BlobRef::new(AccountId::new("a c!count"), BlobId::new("b;lob@1"))
            .with_name("re,port+final.txt")
            .with_content_type("application/ld+json");

        client.download(&blob).await.expect("download runs");

        let url = captured.lock().expect("capture lock").clone().expect("url");
        assert_eq!(
            url,
            "https://example.test/dl/a%20c%21count/b%3Blob%401\
             ?name=re%2Cport%2Bfinal.txt&type=application%2Fld%2Bjson"
        );
    }
}
