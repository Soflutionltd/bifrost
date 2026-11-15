use crate::Result;
use crate::client::{GmailClient, check_response_status, parse_json_response};
use crate::people::types::UpdateContactPhotoRequest;
use crate::quota::ApiSurface;
use base64::Engine;
use serde::{Serialize, de::DeserializeOwned};

/// A service-bound view: request attribution never depends on URL spelling.
pub(crate) struct PeopleApi<'a>(pub(crate) &'a GmailClient);

impl PeopleApi<'_> {
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
            .execute_on(ApiSurface::People, url, method, body, expected, replay)
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
        parse_json_response(response, ApiSurface::People).await
    }

    pub(crate) async fn delete(&self, url: &str, replay: bifrost_net::ReplayPolicy) -> Result<()> {
        let response = self
            .execute(url, reqwest::Method::DELETE, None::<&()>, replay)
            .await?;
        check_response_status(response, ApiSurface::People).await
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
        parse_json_response(response, ApiSurface::People).await
    }
    pub(crate) async fn patch<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        body: &B,
        replay: bifrost_net::ReplayPolicy,
    ) -> Result<T> {
        let response = self
            .execute(url, reqwest::Method::PATCH, Some(body), replay)
            .await?;
        parse_json_response(response, ApiSurface::People).await
    }
}

pub(crate) const PERSON_FIELDS: &str = "names,emailAddresses,phoneNumbers,addresses,organizations,photos,biographies,memberships,metadata";
pub(crate) const DIRECTORY_READ_MASK: &str = "names,emailAddresses,phoneNumbers,organizations";
pub(crate) const DIRECTORY_SOURCES: &str = "DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE";
use crate::account::error::{self, GmailErrorContext};
use bifrost_types::{AccountError, AccountOperation, ContactSearchRequest};
pub(crate) fn caller_input_error(operation: AccountOperation, message: String) -> AccountError {
    error::into_account_error(
        crate::error::Error::invalid_request(operation, message),
        GmailErrorContext::contact_collection(operation),
    )
}
pub(crate) fn update_contact_photo_url(people_base: &str, encoded_resource_name: &str) -> String {
    // `updateContactPhoto` takes its field mask in the request body, not the
    // query string; the response is discarded here, so no `personFields`
    // query param is needed.
    format!("{people_base}/{encoded_resource_name}:updateContactPhoto")
}

pub(crate) fn delete_contact_photo_url(people_base: &str, encoded_resource_name: &str) -> String {
    format!(
        "{people_base}/{encoded_resource_name}:deleteContactPhoto?personFields={}",
        bifrost_net::url::encode_query_value(PERSON_FIELDS)
    )
}

/// Percent-encode a People resource name (e.g. `people/c1`) for placement
/// in a URL path segment. A People resource name's `/` separators are real
/// REST path separators - the People API serves
/// `/v1/people/c1:updateContact`, not `/v1/people%2Fc1:updateContact`
/// (which 404s) - so each `/`-delimited part is component-encoded and the
/// parts are rejoined on a literal `/`. Any genuinely unsafe character
/// inside a part is still escaped. This is deliberately not folded into
/// `bifrost_net::url::encode_path_component`, whose other callers rely on it
/// escaping `/` as a normal component character.
pub(crate) fn encode_resource_name_path(resource_name: &str) -> String {
    resource_name
        .split('/')
        .map(bifrost_net::url::encode_path_component)
        .collect::<Vec<_>>()
        .join("/")
}

pub(crate) fn directory_search_warmup_url(people_base: &str) -> String {
    format!(
        "{people_base}/people:searchDirectoryPeople?query=&readMask={}&sources={DIRECTORY_SOURCES}",
        bifrost_net::url::encode_query_value(DIRECTORY_READ_MASK)
    )
}

pub(crate) fn directory_search_url(
    people_base: &str,
    query: &str,
    limit: Option<u32>,
    page_cursor: Option<&[u8]>,
) -> std::result::Result<String, AccountError> {
    let page_size = limit.unwrap_or(1000).min(1000);
    let mut url = if query.is_empty() {
        format!(
            "{people_base}/people:listDirectoryPeople?readMask={}&sources={DIRECTORY_SOURCES}&pageSize={page_size}",
            bifrost_net::url::encode_query_value(DIRECTORY_READ_MASK)
        )
    } else {
        format!(
            "{people_base}/people:searchDirectoryPeople?query={}&readMask={}&sources={DIRECTORY_SOURCES}&pageSize={page_size}",
            bifrost_net::url::encode_query_value(query),
            bifrost_net::url::encode_query_value(DIRECTORY_READ_MASK)
        )
    };
    if let Some(cursor) = page_cursor {
        let token = crate::account::page_cursor::token(cursor, AccountOperation::DirectorySearch)?;
        url.push_str("&pageToken=");
        url.push_str(&bifrost_net::url::encode_query_value(token));
    }
    Ok(url)
}

pub(crate) fn contact_groups_url(people_base: &str, page_token: Option<&str>) -> String {
    let mut url = format!(
        "{people_base}/contactGroups?groupFields={}&pageSize=1000",
        bifrost_net::url::encode_query_value("metadata,name")
    );
    if let Some(token) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&bifrost_net::url::encode_query_value(token));
    }
    url
}

pub(crate) fn search_warmup_url(people_base: &str) -> String {
    format!(
        "{people_base}/people:searchContacts?query=&readMask={}",
        bifrost_net::url::encode_query_value(PERSON_FIELDS)
    )
}

pub(crate) fn search_url(
    people_base: &str,
    request: &ContactSearchRequest,
) -> std::result::Result<String, AccountError> {
    let mut url = format!(
        "{people_base}/people:searchContacts?query={}&readMask={}",
        bifrost_net::url::encode_query_value(&request.query),
        bifrost_net::url::encode_query_value(PERSON_FIELDS)
    );
    if let Some(limit) = request.limit {
        url.push_str("&pageSize=");
        url.push_str(&limit.min(30).to_string());
    }
    if let Some(cursor) = request.page_cursor.as_ref() {
        let token = crate::account::page_cursor::token(cursor, AccountOperation::ContactSearch)?;
        url.push_str("&pageToken=");
        url.push_str(&bifrost_net::url::encode_query_value(token));
    }
    Ok(url)
}

pub(crate) fn update_contact_photo_request(
    photo: bifrost_types::ContactPhoto,
) -> UpdateContactPhotoRequest {
    UpdateContactPhotoRequest {
        photo_bytes: base64::engine::general_purpose::STANDARD.encode(photo.data),
    }
}

#[cfg(test)]
mod url_tests {
    use super::*;
    use serde_json::Value;
    #[test]
    fn resource_name_path_keeps_slash_literal() {
        // A People resource name's `/` is a real REST path separator: the
        // People API serves `/v1/people/c1:updateContact`, and the buggy
        // `%2F`-escaped form (`/v1/people%2Fc1...`) 404s. The helper must
        // keep the slash literal while still escaping unsafe characters.
        assert_eq!(encode_resource_name_path("people/c1"), "people/c1");
        assert_eq!(
            encode_resource_name_path("otherContacts/x1"),
            "otherContacts/x1"
        );
        assert_eq!(
            encode_resource_name_path("people/a b#c"),
            "people/a%20b%23c"
        );
    }

    #[test]
    fn contact_photo_patch_uses_people_photo_endpoint_payload() {
        let people_base = "https://people.googleapis.com/v1";
        let encoded = encode_resource_name_path("people/c1");
        // The resource name renders with a literal `/`, matching the real
        // People verb path (`/v1/people/c1:updateContactPhoto`).
        assert!(
            update_contact_photo_url(people_base, &encoded)
                .contains("people/c1:updateContactPhoto")
        );
        assert!(
            delete_contact_photo_url(people_base, &encoded)
                .contains("people/c1:deleteContactPhoto")
        );
        assert!(!update_contact_photo_url(people_base, &encoded).contains("people%2Fc1"));

        let payload = update_contact_photo_request(bifrost_types::ContactPhoto {
            data: vec![1, 2, 3, 4],
            media_type: Some("image/jpeg".to_string()),
        });
        let value = serde_json::to_value(payload).expect("photo payload");
        assert_eq!(
            value.get("photoBytes").and_then(Value::as_str),
            Some("AQIDBA==")
        );
    }

    #[test]
    fn contact_groups_url_includes_fields_and_page_token() {
        let url = contact_groups_url("https://people.mock.test/v1", Some("next token"));

        assert!(url.starts_with("https://people.mock.test/v1/contactGroups"));
        assert!(url.contains("contactGroups?groupFields=metadata%2Cname"));
        assert!(url.contains("pageSize=1000"));
        assert!(url.contains("pageToken=next%20token"));
    }

    #[test]
    fn search_url_includes_page_token_and_capped_page_size() {
        let url = search_url(
            "https://people.mock.test/v1",
            &ContactSearchRequest {
                query: "Ada Lovelace".to_string(),
                address_book_id: None,
                page_cursor: Some(crate::account::page_cursor::encode(
                    AccountOperation::ContactSearch,
                    "next token",
                )),
                limit: Some(100),
            },
        )
        .expect("search url");

        assert!(url.starts_with("https://people.mock.test/v1/people:searchContacts"));
        assert!(url.contains("query=Ada%20Lovelace"));
        assert!(url.contains("pageSize=30"));
        assert!(url.contains("pageToken=next%20token"));
    }

    #[test]
    fn search_warmup_url_uses_empty_query_and_read_mask() {
        let url = search_warmup_url("https://people.mock.test/v1");

        assert!(url.starts_with("https://people.mock.test/v1/people:searchContacts"));
        assert!(url.contains("people:searchContacts?query=&readMask="));
        assert!(url.contains("names%2CemailAddresses"));
        assert!(!url.contains("pageSize="));
    }

    #[test]
    fn directory_search_empty_query_lists_directory_people() {
        let url =
            directory_search_url("https://people.mock.test/v1", "", Some(2000), None).expect("url");

        assert!(url.starts_with("https://people.mock.test/v1/people:listDirectoryPeople"));
        assert!(url.contains("people:listDirectoryPeople"));
        assert!(url.contains("sources=DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE"));
        // limit caps at 1000.
        assert!(url.contains("pageSize=1000"));
        assert!(!url.contains("searchDirectoryPeople"));
    }

    #[test]
    fn directory_search_nonempty_query_searches_directory_people() {
        let url = directory_search_url(
            "https://people.mock.test/v1",
            "Ada Lovelace",
            None,
            Some(&crate::account::page_cursor::encode(
                AccountOperation::DirectorySearch,
                "tok",
            )),
        )
        .expect("url");

        assert!(url.starts_with("https://people.mock.test/v1/people:searchDirectoryPeople"));
        assert!(url.contains("people:searchDirectoryPeople"));
        assert!(url.contains("query=Ada%20Lovelace"));
        assert!(url.contains("sources=DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE"));
        assert!(url.contains("pageToken=tok"));
    }

    #[test]
    fn directory_search_warmup_url_uses_empty_query() {
        let url = directory_search_warmup_url("https://people.mock.test/v1");

        assert!(url.starts_with("https://people.mock.test/v1/people:searchDirectoryPeople"));
        assert!(url.contains("people:searchDirectoryPeople?query=&"));
        assert!(url.contains("sources=DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE"));
        assert!(!url.contains("pageSize="));
    }
}
