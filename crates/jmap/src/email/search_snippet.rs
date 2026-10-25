use serde::{Deserialize, Serialize};

use super::EmailId;
use crate::core::{id::AccountId, query::Filter, request::ResultReference};

#[derive(Deserialize, Clone, Debug)]
pub(crate) struct SearchSnippet {
    #[serde(rename = "emailId")]
    email_id: EmailId,
    subject: Option<String>,
    preview: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SearchSnippetGetRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "filter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<Filter<super::query::Filter>>,

    #[serde(rename = "emailIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    email_ids: Option<Vec<EmailId>>,

    #[serde(rename = "#emailIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    email_ids_ref: Option<ResultReference>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SearchSnippetGetResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "list")]
    list: Vec<SearchSnippet>,

    #[serde(rename = "notFound")]
    not_found: Option<Vec<EmailId>>,
}

impl crate::core::method::JmapMethod for SearchSnippetGetRequest {
    const REPLAY_SAFE: bool = true;
    const NAME: &'static str = "SearchSnippet/get";
    type Cap = crate::core::capability::Mail;
    type Response = SearchSnippetGetResponse;

    fn set_account_id(&mut self, account_id: &AccountId) {
        self.account_id = account_id.clone();
    }
}

impl Default for SearchSnippetGetRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl SearchSnippetGetRequest {
    pub(crate) fn new() -> Self {
        SearchSnippetGetRequest {
            account_id: AccountId::new(""),
            filter: None,
            email_ids: None,
            email_ids_ref: None,
        }
    }

    #[must_use]
    pub(crate) fn filter(mut self, filter: impl Into<Filter<super::query::Filter>>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    #[must_use]
    pub(crate) fn email_id(mut self, email_id: impl Into<EmailId>) -> Self {
        self.email_ids
            .get_or_insert_with(Vec::new)
            .push(email_id.into());
        self
    }

    #[must_use]
    pub(crate) fn email_ids(
        mut self,
        email_ids: impl IntoIterator<Item = impl Into<EmailId>>,
    ) -> Self {
        self.email_ids
            .get_or_insert_with(Vec::new)
            .extend(email_ids.into_iter().map(std::convert::Into::into));
        self
    }

    #[must_use]
    pub(crate) fn email_ids_ref(mut self, reference: ResultReference) -> Self {
        self.email_ids_ref = reference.into();
        self.email_ids = None;
        self
    }
}

impl SearchSnippet {
    pub(crate) fn email_id(&self) -> &EmailId {
        &self.email_id
    }

    pub(crate) fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    pub(crate) fn preview(&self) -> Option<&str> {
        self.preview.as_deref()
    }
}

impl SearchSnippetGetResponse {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn snippet(&self, id: &EmailId) -> Option<&SearchSnippet> {
        self.list.iter().find(|snippet| &snippet.email_id == id)
    }

    pub(crate) fn list(&self) -> &[SearchSnippet] {
        &self.list
    }

    pub(crate) fn not_found(&self) -> Option<&[EmailId]> {
        self.not_found.as_deref()
    }
}
