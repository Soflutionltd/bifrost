use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Error;

use super::id::AccountId;
use super::set::{SetError, SetObject};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CopyRequest<O: SetObject> {
    #[serde(rename = "fromAccountId")]
    from_account_id: AccountId,

    #[serde(rename = "ifFromInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    if_from_in_state: Option<String>,

    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "ifInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    if_in_state: Option<String>,

    /// Create entries keyed by consumer-provided create-id (e.g. "c1").
    #[serde(rename = "create")]
    create: HashMap<String, O::Create>,

    #[serde(rename = "onSuccessDestroyOriginal")]
    on_success_destroy_original: bool,

    #[serde(rename = "destroyFromIfInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    destroy_from_if_in_state: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CopyResponse<O: SetObject> {
    #[serde(rename = "fromAccountId")]
    from_account_id: AccountId,

    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "oldState")]
    old_state: Option<String>,

    #[serde(rename = "newState")]
    new_state: String,

    /// Successful copies, keyed by the consumer-provided create-id.
    #[serde(rename = "created")]
    created: Option<HashMap<String, O>>,

    /// Failed copies, keyed by the consumer-provided create-id.
    #[serde(rename = "notCreated")]
    not_created: Option<HashMap<String, SetError<O::Property>>>,
}

impl<O: SetObject> CopyRequest<O> {
    pub(crate) fn new(from_account_id: impl Into<AccountId>) -> Self {
        CopyRequest {
            from_account_id: from_account_id.into(),
            if_from_in_state: None,
            account_id: AccountId::new(""),
            if_in_state: None,
            create: HashMap::new(),
            on_success_destroy_original: false,
            destroy_from_if_in_state: None,
        }
    }

    pub(crate) fn account_id(&mut self, account_id: impl Into<AccountId>) -> &mut Self {
        self.account_id = account_id.into();
        self
    }

    pub(crate) fn if_from_in_state(&mut self, if_from_in_state: impl Into<String>) -> &mut Self {
        self.if_from_in_state = Some(if_from_in_state.into());
        self
    }

    pub(crate) fn if_in_state(&mut self, if_in_state: impl Into<String>) -> &mut Self {
        self.if_in_state = Some(if_in_state.into());
        self
    }

    pub(crate) fn on_success_destroy_original(
        &mut self,
        on_success_destroy_original: bool,
    ) -> &mut Self {
        self.on_success_destroy_original = on_success_destroy_original;
        self
    }

    pub(crate) fn destroy_from_if_in_state(
        &mut self,
        destroy_from_if_in_state: impl Into<String>,
    ) -> &mut Self {
        self.destroy_from_if_in_state = Some(destroy_from_if_in_state.into());
        self
    }
}

impl<O: SetObject> CopyRequest<O>
where
    O::Create: crate::core::SetCreate,
{
    pub(crate) fn create(&mut self, id: impl Into<String>) -> &mut O::Create {
        use crate::core::SetCreate;
        // Get-or-insert, like SetRequest::create_with_id: a repeated id must
        // hand back the existing entry, not silently overwrite an object.
        let id = crate::core::creation::CreationId::named(id);
        self.create
            .entry(id.to_string())
            .or_insert_with(|| O::Create::new(Some(id)))
    }
}

impl<O: SetObject> CopyResponse<O> {
    pub(crate) fn from_account_id(&self) -> &AccountId {
        &self.from_account_id
    }

    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn old_state(&self) -> Option<&str> {
        self.old_state.as_deref()
    }

    pub(crate) fn new_state(&self) -> &str {
        &self.new_state
    }

    pub(crate) fn created(&mut self, id: &str) -> crate::Result<O> {
        if let Some(result) = self.created.as_mut().and_then(|r| r.remove(id)) {
            Ok(result)
        } else if let Some(error) = self.not_created.as_mut().and_then(|r| r.remove(id)) {
            Err(error.to_string_error().into())
        } else {
            Err(Error::SetIdUnanswered(id.to_string()))
        }
    }

    pub(crate) fn into_created(self) -> Option<Vec<O>> {
        self.created.map(|map| map.into_values().collect())
    }

    pub(crate) fn created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.created.as_ref().map(|map| map.keys())
    }

    pub(crate) fn not_created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.not_created.as_ref().map(|map| map.keys())
    }
}
