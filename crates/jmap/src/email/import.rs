use crate::{
    Error,
    core::{
        id::{AccountId, BlobId},
        request::ResultReference,
        set::{SetError, from_timestamp},
    },
    mailbox::MailboxId,
};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{Email, Property};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EmailImportRequest {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "ifInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    if_in_state: Option<String>,

    emails: HashMap<String, EmailImport>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EmailImport {
    #[serde(skip)]
    create_id: usize,

    #[serde(rename = "blobId")]
    blob_id: BlobId,

    #[serde(rename = "mailboxIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    mailbox_ids: Option<HashMap<MailboxId, bool>>,

    #[serde(rename = "#mailboxIds")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    mailbox_ids_ref: Option<ResultReference>,

    #[serde(rename = "keywords")]
    keywords: HashMap<String, bool>,

    #[serde(rename = "receivedAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    received_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct EmailImportResponse {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "oldState")]
    old_state: Option<String>,

    #[serde(rename = "newState")]
    new_state: String,

    #[serde(rename = "created")]
    created: Option<HashMap<String, Email>>,

    #[serde(rename = "notCreated")]
    not_created: Option<HashMap<String, SetError<Property>>>,
}

impl crate::core::method::JmapMethod for EmailImportRequest {
    const REPLAY_SAFE: bool = false;
    const NAME: &'static str = "Email/import";
    type Cap = crate::core::capability::Mail;
    type Response = EmailImportResponse;

    fn set_account_id(&mut self, account_id: &AccountId) {
        self.account_id = account_id.clone();
    }
}

impl Default for EmailImportRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl EmailImportRequest {
    pub(crate) fn new() -> Self {
        EmailImportRequest {
            account_id: AccountId::new(""),
            if_in_state: None,
            emails: HashMap::new(),
        }
    }

    /// Internal account-id setter. Prefer `JmapMethod::set_account_id`
    /// (called automatically by `Request::call`); kept on the struct
    /// for the few call sites that build a request directly.
    pub(crate) fn set_account_id_inplace(&mut self, account_id: impl Into<AccountId>) -> &mut Self {
        self.account_id = account_id.into();
        self
    }

    #[must_use]
    pub(crate) fn if_in_state(mut self, if_in_state: impl Into<String>) -> Self {
        self.if_in_state = Some(if_in_state.into());
        self
    }

    /// Add an email entry. Stays imperative because the returned
    /// `&mut EmailImport` writes into a HashMap entry.
    pub(crate) fn email(&mut self, blob_id: impl Into<BlobId>) -> &mut EmailImport {
        let create_id = self.emails.len();
        let create_id_str = format!("i{create_id}");
        self.emails.insert(
            create_id_str.clone(),
            EmailImport::new(blob_id.into(), create_id),
        );
        self.emails.get_mut(&create_id_str).unwrap()
    }
}

impl EmailImport {
    fn new(blob_id: BlobId, create_id: usize) -> Self {
        EmailImport {
            create_id,
            blob_id,
            mailbox_ids: None,
            mailbox_ids_ref: None,
            keywords: HashMap::new(),
            received_at: None,
        }
    }

    pub(crate) fn mailbox_ids<T, U>(&mut self, mailbox_ids: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<MailboxId>,
    {
        self.mailbox_ids = Some(mailbox_ids.into_iter().map(|s| (s.into(), true)).collect());
        self.mailbox_ids_ref = None;
        self
    }

    pub(crate) fn mailbox_ids_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.mailbox_ids_ref = reference.into();
        self.mailbox_ids = None;
        self
    }

    pub(crate) fn keywords<T, U>(&mut self, keywords: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.keywords = keywords.into_iter().map(|s| (s.into(), true)).collect();
        self
    }

    pub(crate) fn received_at(&mut self, received_at: i64) -> &mut Self {
        self.received_at = Some(from_timestamp(received_at));
        self
    }

    pub(crate) fn create_id(&self) -> String {
        format!("i{}", self.create_id)
    }
}

impl EmailImportResponse {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn old_state(&self) -> Option<&str> {
        self.old_state.as_deref()
    }

    pub(crate) fn new_state(&self) -> &str {
        &self.new_state
    }

    pub(crate) fn into_new_state(self) -> String {
        self.new_state
    }

    pub(crate) fn created(&mut self, id: &str) -> crate::Result<Email> {
        if let Some(result) = self.created.as_mut().and_then(|r| r.remove(id)) {
            Ok(result)
        } else if let Some(error) = self.not_created.as_mut().and_then(|r| r.remove(id)) {
            Err(error.to_string_error().into())
        } else {
            Err(Error::IdNotFound(id.to_string()))
        }
    }

    pub(crate) fn created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.created.as_ref().map(|map| map.keys())
    }

    pub(crate) fn not_created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.not_created.as_ref().map(|map| map.keys())
    }
}
