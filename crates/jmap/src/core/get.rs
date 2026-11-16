use serde::{Deserialize, Serialize};

use super::Object;
use super::id::{AccountId, ObjectState};
use super::request::ResultReference;

pub(crate) trait GetObject: Object {
    type GetArguments: Default + Serialize;
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GetRequest<O: GetObject> {
    #[serde(rename = "accountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<AccountId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    ids: Option<Vec<O::Id>>,

    #[serde(rename = "#ids")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    ids_ref: Option<ResultReference>,

    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<Vec<O::Property>>,

    #[serde(rename = "#properties")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    properties_ref: Option<ResultReference>,

    #[serde(flatten)]
    arguments: O::GetArguments,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct GetResponse<O: Object> {
    #[serde(skip)]
    pub(crate) answer_context: crate::core::answer::AnswerContext,
    #[serde(rename = "accountId")]
    account_id: Option<AccountId>,

    state: ObjectState<O>,

    list: Vec<O>,

    // Missing notFound is tolerated as an empty lane, never as evidence that
    // all submitted ids were answered. `answer` reconciles each requested id.
    #[serde(rename = "notFound")]
    #[serde(default, deserialize_with = "crate::core::id::echo::vec")]
    not_found: Vec<O::Id>,
}

impl<O: GetObject> GetRequest<O> {
    pub(crate) fn new() -> Self {
        GetRequest {
            account_id: if O::requires_account_id() {
                Some(AccountId::new(""))
            } else {
                None
            },
            ids: None,
            ids_ref: None,
            properties: None,
            properties_ref: None,
            arguments: O::GetArguments::default(),
        }
    }

    pub(crate) fn account_id(&mut self, account_id: impl Into<AccountId>) -> &mut Self {
        if O::requires_account_id() {
            self.account_id = Some(account_id.into());
        }
        self
    }

    pub(crate) fn ids<U, V>(&mut self, ids: U) -> &mut Self
    where
        U: IntoIterator<Item = V>,
        V: Into<O::Id>,
    {
        self.ids = Some(ids.into_iter().map(std::convert::Into::into).collect());
        self.ids_ref = None;
        self
    }

    pub(crate) fn ids_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.ids_ref = reference.into();
        self.ids = None;
        self
    }

    pub(crate) fn properties(
        &mut self,
        properties: impl IntoIterator<Item = O::Property>,
    ) -> &mut Self {
        self.properties = Some(properties.into_iter().collect());
        self.properties_ref = None;
        self
    }

    pub(crate) fn properties_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.properties_ref = Some(reference);
        self.properties = None;
        self
    }

    pub(crate) fn arguments(&mut self) -> &mut O::GetArguments {
        &mut self.arguments
    }
}

impl<O: GetObject> Default for GetRequest<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Object> GetResponse<O> {
    pub(crate) fn answer(
        &self,
        id: &O::Id,
        matches_id: impl Fn(&O) -> bool,
    ) -> super::answer::IdAnswer<&O> {
        super::answer::IdAnswer::from_parts(
            self.list.iter().find(|object| matches_id(object)),
            None,
            self.not_found.contains(id),
        )
    }

    pub(crate) fn account_id(&self) -> Option<&AccountId> {
        self.account_id.as_ref()
    }

    pub(crate) fn state(&self) -> &ObjectState<O> {
        &self.state
    }

    pub(crate) fn into_state(self) -> ObjectState<O> {
        self.state
    }

    pub(crate) fn list(&self) -> &[O] {
        &self.list
    }

    pub(crate) fn not_found(&self) -> &[O::Id] {
        &self.not_found
    }

    pub(crate) fn into_list(self) -> Vec<O> {
        self.list
    }

    pub(crate) fn pop(&mut self) -> Option<O> {
        self.list.pop()
    }

    pub(crate) fn into_not_found(self) -> Vec<O::Id> {
        self.not_found
    }
}

#[cfg(test)]
mod wire_tests;
