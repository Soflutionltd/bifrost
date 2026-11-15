use crate::Result;
use crate::client::{GmailClient, parse_json_response};
use crate::quota::ApiSurface;
use serde::{Serialize, de::DeserializeOwned};

/// A service-bound view: request attribution never depends on URL spelling.
pub(crate) struct DriveApi<'a>(pub(crate) &'a GmailClient);

impl DriveApi<'_> {
    pub(crate) async fn execute_builder(
        &self,
        builder: impl Into<bifrost_net::RequestBuilder>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<bifrost_net::Response> {
        self.0
            .execute_builder(builder.into(), ApiSurface::Drive, replay)
            .await
    }

    pub(crate) fn admits_session(&self, url: &reqwest::Url) -> bool {
        let Ok(base) = reqwest::Url::parse(&self.session_url()) else {
            return false;
        };
        // A configured destination admits only its own upload subtree. Provider
        // Location headers cannot widen the origin trusted with credentials.
        // Origin and userinfo admission is net's exact-origin rule; only the
        // upload-subtree path restriction is Drive's own.
        let mut origins = bifrost_net::TrustedOrigins::default();
        if origins.trust(base.as_str()).is_err() || origins.admit(url.as_str()).is_err() {
            return false;
        }
        url.path() == base.path()
            || url
                .path()
                .strip_prefix(base.path())
                .is_some_and(|tail| tail.starts_with('/'))
    }

    pub(crate) fn session_url(&self) -> String {
        format!(
            "{}/upload/drive/v3/files?uploadType=resumable",
            self.0.drive_base()
        )
    }

    pub(crate) fn permissions_url(&self, encoded_file_id: &str) -> String {
        format!(
            "{}/drive/v3/files/{encoded_file_id}/permissions?fields=id",
            self.0.drive_base()
        )
    }

    pub(crate) fn web_link_url(&self, encoded_file_id: &str) -> String {
        format!(
            "{}/drive/v3/files/{encoded_file_id}?fields=webViewLink",
            self.0.drive_base()
        )
    }

    pub(crate) async fn execute<B: Serialize>(
        &self,
        url: &str,
        method: reqwest::Method,
        body: Option<&B>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<bifrost_net::Response> {
        self.execute_if_match(url, method, body, None, replay).await
    }

    pub(crate) async fn execute_if_match<B: Serialize>(
        &self,
        url: &str,
        method: reqwest::Method,
        body: Option<&B>,
        expected: Option<&str>,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<bifrost_net::Response> {
        self.0
            .execute_on(ApiSurface::Drive, url, method, body, expected, replay)
            .await
    }

    pub(crate) async fn get<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        let response = self
            .execute(
                url,
                reqwest::Method::GET,
                None::<&()>,
                bifrost_net::ReplayPolicy::Idempotent,
            )
            .await?;
        parse_json_response(response, ApiSurface::Drive).await
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T> {
        let response = self
            .execute(url, reqwest::Method::POST, Some(body), replay)
            .await?;
        parse_json_response(response, ApiSurface::Drive).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_drive_origin_owns_upload_and_metadata_urls() {
        let client = GmailClient::new("token").with_drive_base("https://drive.test/proxy/");
        let api = client.drive();
        assert_eq!(
            api.session_url(),
            "https://drive.test/proxy/upload/drive/v3/files?uploadType=resumable"
        );
        assert_eq!(
            api.permissions_url("f1"),
            "https://drive.test/proxy/drive/v3/files/f1/permissions?fields=id"
        );
        assert_eq!(
            api.web_link_url("f1"),
            "https://drive.test/proxy/drive/v3/files/f1?fields=webViewLink"
        );
        assert!(api.admits_session(
            &reqwest::Url::parse("https://drive.test/proxy/upload/drive/v3/files/session").unwrap()
        ));
        for rejected in [
            "https://other.test/proxy/upload/drive/v3/files/session",
            "https://drive.test/proxy/upload/drive/v3/files-other",
            "https://drive.test/proxy/gmail/v1/users/me",
            "https://user@drive.test/proxy/upload/drive/v3/files/session",
        ] {
            assert!(!api.admits_session(&reqwest::Url::parse(rejected).unwrap()));
        }
        assert_eq!(client.api_base(), crate::client::GMAIL_API_BASE);
    }
}
