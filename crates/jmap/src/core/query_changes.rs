use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use super::Object;
use super::id::{AccountId, QueryState};
use super::query::{Comparator, Filter, QueryObject};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QueryChangesRequest<O: QueryObject> {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "filter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<Filter<O::Filter>>,

    #[serde(rename = "sort")]
    #[serde(skip_serializing_if = "Option::is_none")]
    sort: Option<Vec<Comparator<O::Sort>>>,

    #[serde(rename = "sinceQueryState")]
    since_query_state: QueryState<O>,

    #[serde(rename = "maxChanges")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_changes: Option<NonZeroUsize>,

    #[serde(rename = "upToId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    up_to_id: Option<O::Id>,

    #[serde(rename = "calculateTotal")]
    calculate_total: bool,

    #[serde(flatten)]
    arguments: O::QueryArguments,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct QueryChangesResponse<O: Object> {
    #[serde(rename = "accountId")]
    account_id: AccountId,
    #[serde(rename = "oldQueryState")]
    old_query_state: QueryState<O>,
    #[serde(rename = "newQueryState")]
    new_query_state: QueryState<O>,
    #[serde(rename = "total")]
    total: Option<usize>,
    #[serde(rename = "removed")]
    removed: Vec<O::Id>,
    #[serde(rename = "added")]
    added: Vec<AddedItem<O>>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AddedItem<O: Object> {
    id: O::Id,
    index: usize,
}

impl<O: QueryObject> QueryChangesRequest<O> {
    /// Construct a `QueryChangesRequest` with `accountId` left empty;
    /// the account ID is filled in by
    /// [`crate::core::request::Request::call`] when the method is
    /// added to a request batch.
    pub(crate) fn new(since_query_state: impl Into<QueryState<O>>) -> Self {
        QueryChangesRequest {
            account_id: AccountId::new(""),
            filter: None,
            sort: None,
            since_query_state: since_query_state.into(),
            max_changes: None,
            up_to_id: None,
            calculate_total: false,
            arguments: O::QueryArguments::default(),
        }
    }

    pub(crate) fn account_id(&mut self, account_id: impl Into<AccountId>) -> &mut Self {
        self.account_id = account_id.into();
        self
    }

    pub(crate) fn filter(&mut self, filter: impl Into<Filter<O::Filter>>) -> &mut Self {
        self.filter = Some(filter.into());
        self
    }

    pub(crate) fn sort(
        &mut self,
        sort: impl IntoIterator<Item = Comparator<O::Sort>>,
    ) -> &mut Self {
        self.sort = Some(sort.into_iter().collect());
        self
    }

    /// Cap the response at most `max_changes` ID entries. RFC 8620
    /// requires this to be a positive integer; using `NonZeroUsize`
    /// rejects `0` at compile time.
    pub(crate) fn max_changes(&mut self, max_changes: NonZeroUsize) -> &mut Self {
        self.max_changes = Some(max_changes);
        self
    }

    pub(crate) fn up_to_id(&mut self, up_to_id: impl Into<O::Id>) -> &mut Self {
        self.up_to_id = Some(up_to_id.into());
        self
    }

    pub(crate) fn calculate_total(&mut self, calculate_total: bool) -> &mut Self {
        self.calculate_total = calculate_total;
        self
    }

    pub(crate) fn arguments(&mut self) -> &mut O::QueryArguments {
        &mut self.arguments
    }
}

impl<O: Object> QueryChangesResponse<O> {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn old_query_state(&self) -> &QueryState<O> {
        &self.old_query_state
    }

    pub(crate) fn new_query_state(&self) -> &QueryState<O> {
        &self.new_query_state
    }

    pub(crate) fn total(&self) -> Option<usize> {
        self.total
    }

    pub(crate) fn removed(&self) -> &[O::Id] {
        &self.removed
    }

    pub(crate) fn added(&self) -> &[AddedItem<O>] {
        &self.added
    }
}

impl<O: Object> AddedItem<O> {
    pub(crate) fn id(&self) -> &O::Id {
        &self.id
    }

    pub(crate) fn index(&self) -> usize {
        self.index
    }
}
