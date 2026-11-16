use crate::core::id::ObjectState;
use crate::{
    core::{
        id::{AccountId, BlobId},
        request::ResultReference,
        set::SetError,
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
    if_in_state: Option<ObjectState<Email>>,

    emails: HashMap<String, EmailImport>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EmailImport {
    #[serde(skip)]
    create_id: crate::core::creation::CreationId,

    #[serde(rename = "blobId")]
    blob_id: BlobId,

    #[serde(rename = "mailboxIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    mailbox_ids: Option<crate::core::object_set::ObjectSet<MailboxId>>,

    #[serde(rename = "#mailboxIds")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    mailbox_ids_ref: Option<ResultReference>,

    #[serde(rename = "keywords")]
    keywords: crate::core::object_set::ObjectSet<super::keyword::Keyword>,

    #[serde(rename = "receivedAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    received_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct EmailImportResponse {
    #[serde(skip)]
    pub(crate) answer_context: crate::core::answer::AnswerContext,
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "oldState")]
    old_state: Option<ObjectState<Email>>,

    #[serde(rename = "newState")]
    new_state: ObjectState<Email>,

    #[serde(rename = "created")]
    created: Option<HashMap<String, Email>>,

    #[serde(rename = "notCreated")]
    not_created: Option<HashMap<String, SetError<Property>>>,
}

impl crate::core::method::JmapMethod for EmailImportRequest {
    const REPLAY_SAFE: bool = false;
    const NAME: &'static str = <super::Email as crate::core::Object>::METHODS.import;
    type Cap = <super::Email as crate::core::Object>::Cap;
    type Response = EmailImportResponse;

    fn stamp_response(response: &mut Self::Response, context: crate::core::answer::AnswerContext) {
        response.answer_context = context;
    }

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
    pub(crate) fn if_in_state(mut self, if_in_state: impl Into<ObjectState<Email>>) -> Self {
        self.if_in_state = Some(if_in_state.into());
        self
    }

    /// Add an email entry. Stays imperative because the returned
    /// `&mut EmailImport` writes into a HashMap entry.
    pub(crate) fn email(&mut self, blob_id: impl Into<BlobId>) -> &mut EmailImport {
        let create_id = crate::core::creation::CreationId::next('i', Some(&self.emails));
        let create_id_str = create_id.to_string();
        self.emails.insert(
            create_id_str.clone(),
            EmailImport::new(blob_id.into(), create_id),
        );
        self.emails.get_mut(&create_id_str).unwrap()
    }
}

impl EmailImport {
    fn new(blob_id: BlobId, create_id: crate::core::creation::CreationId) -> Self {
        EmailImport {
            create_id,
            blob_id,
            mailbox_ids: None,
            mailbox_ids_ref: None,
            keywords: Default::default(),
            received_at: None,
        }
    }

    pub(crate) fn mailbox_ids<T, U>(&mut self, mailbox_ids: T) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<MailboxId>,
    {
        self.mailbox_ids = Some(mailbox_ids.into_iter().map(Into::into).collect());
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
        U: Into<super::keyword::Keyword>,
    {
        self.keywords = keywords.into_iter().map(Into::into).collect();
        self
    }

    // A validated timestamp prevents an invalid send date becoming 1970.
    pub(crate) fn received_at(&mut self, received_at: jiff::Timestamp) -> &mut Self {
        self.received_at = Some(received_at);
        self
    }

    pub(crate) fn create_id(&self) -> String {
        self.create_id.to_string()
    }
}

impl EmailImportResponse {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn old_state(&self) -> Option<&ObjectState<Email>> {
        self.old_state.as_ref()
    }

    pub(crate) fn new_state(&self) -> &ObjectState<Email> {
        &self.new_state
    }

    pub(crate) fn into_new_state(self) -> ObjectState<Email> {
        self.new_state
    }

    pub(crate) fn created(&mut self, id: &str) -> crate::Result<Email> {
        crate::core::answer::IdAnswer::from_parts(
            self.created.as_mut().and_then(|values| values.remove(id)),
            self.not_created
                .as_mut()
                .and_then(|values| values.remove(id))
                .map(|error| crate::Error::Set(error.to_string_error())),
            false,
        )
        .into_result(id, self.answer_context)
    }

    pub(crate) fn created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.created.as_ref().map(|map| map.keys())
    }

    pub(crate) fn not_created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.not_created.as_ref().map(|map| map.keys())
    }
}

#[cfg(test)]
mod wire_tests;
