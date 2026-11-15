use serde::{Deserialize, Serialize};

use super::Object;
use super::id::AccountId;

pub(crate) trait QueryObject: Object {
    type QueryArguments: Default + Serialize;
    type Filter: Serialize;
    type Sort: Serialize;
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QueryRequest<O: QueryObject> {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "filter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<Filter<O::Filter>>,

    #[serde(rename = "sort")]
    #[serde(skip_serializing_if = "Option::is_none")]
    sort: Option<Vec<Comparator<O::Sort>>>,

    #[serde(rename = "position")]
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<i32>,

    #[serde(rename = "anchor")]
    #[serde(skip_serializing_if = "Option::is_none")]
    anchor: Option<String>,

    #[serde(rename = "anchorOffset")]
    #[serde(skip_serializing_if = "Option::is_none")]
    anchor_offset: Option<i32>,

    #[serde(rename = "limit")]
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,

    #[serde(rename = "calculateTotal")]
    #[serde(skip_serializing_if = "Option::is_none")]
    calculate_total: Option<bool>,

    #[serde(flatten)]
    arguments: O::QueryArguments,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Filter<T> {
    FilterOperator(FilterOperator<T>),
    FilterCondition(T),
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FilterOperator<T> {
    operator: Operator,
    conditions: Vec<Filter<T>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum Operator {
    #[serde(rename = "AND")]
    And,
    #[serde(rename = "OR")]
    Or,
    #[serde(rename = "NOT")]
    Not,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Comparator<A> {
    #[serde(rename = "isAscending")]
    is_ascending: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    collation: Option<String>,

    #[serde(flatten)]
    arguments: A,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct QueryResponse<O: Object> {
    #[serde(rename = "accountId")]
    account_id: AccountId,

    #[serde(rename = "queryState")]
    query_state: String,

    #[serde(rename = "canCalculateChanges")]
    can_calculate_changes: Option<bool>,

    // The wire response is UnsignedInt, unlike the signed request offset.
    // Widening this must also widen the anchored-page and search-cursor
    // validators; a narrowing accessor would hide valid large result positions.
    #[serde(rename = "position")]
    position: i32,

    #[serde(rename = "ids")]
    ids: Vec<O::Id>,

    #[serde(rename = "total")]
    total: Option<usize>,

    #[serde(rename = "limit")]
    limit: Option<usize>,
}

impl<O: QueryObject> QueryRequest<O> {
    /// Construct an empty `QueryRequest`. The `accountId` field is
    /// left empty; it is filled in by
    /// [`crate::core::request::Request::call`] when the method is
    /// added to a request batch.
    pub(crate) fn new() -> Self {
        QueryRequest {
            account_id: AccountId::new(""),
            filter: None,
            sort: None,
            position: None,
            anchor: None,
            anchor_offset: None,
            limit: None,
            calculate_total: None,
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

    pub(crate) fn position(&mut self, position: i32) -> &mut Self {
        self.position = position.into();
        self
    }

    pub(crate) fn anchor(&mut self, anchor: impl Into<String>) -> &mut Self {
        self.anchor = Some(anchor.into());
        self
    }

    pub(crate) fn anchor_offset(&mut self, anchor_offset: i32) -> &mut Self {
        self.anchor_offset = anchor_offset.into();
        self
    }

    pub(crate) fn limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub(crate) fn calculate_total(&mut self, calculate_total: bool) -> &mut Self {
        self.calculate_total = Some(calculate_total);
        self
    }

    pub(crate) fn arguments(&mut self) -> &mut O::QueryArguments {
        &mut self.arguments
    }
}

impl<O: QueryObject> Default for QueryRequest<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Object> QueryResponse<O> {
    pub(crate) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(crate) fn ids(&self) -> &[O::Id] {
        &self.ids
    }

    pub(crate) fn id(&self, pos: usize) -> Option<&O::Id> {
        self.ids.get(pos)
    }

    pub(crate) fn into_ids(self) -> Vec<O::Id> {
        self.ids
    }

    pub(crate) fn total(&self) -> Option<usize> {
        self.total
    }

    pub(crate) fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub(crate) fn position(&self) -> i32 {
        self.position
    }

    pub(crate) fn into_query_state(self) -> String {
        self.query_state
    }

    pub(crate) fn query_state(&self) -> &str {
        &self.query_state
    }

    pub(crate) fn can_calculate_changes(&self) -> bool {
        self.can_calculate_changes.unwrap_or(false)
    }
}

impl<A> Comparator<A> {
    pub(crate) fn new(arguments: A) -> Self {
        Comparator {
            is_ascending: true,
            collation: None,
            arguments,
        }
    }

    pub(crate) fn descending(mut self) -> Self {
        self.is_ascending = false;
        self
    }

    pub(crate) fn ascending(mut self) -> Self {
        self.is_ascending = true;
        self
    }

    pub(crate) fn is_ascending(mut self, is_ascending: bool) -> Self {
        self.is_ascending = is_ascending;
        self
    }

    pub(crate) fn collation(mut self, collation: String) -> Self {
        self.collation = Some(collation);
        self
    }
}

impl<T> From<FilterOperator<T>> for Filter<T> {
    fn from(filter: FilterOperator<T>) -> Self {
        Filter::FilterOperator(filter)
    }
}

impl<T> From<T> for Filter<T> {
    fn from(filter: T) -> Self {
        Filter::FilterCondition(filter)
    }
}

impl<T> Filter<T> {
    pub(crate) fn operator(operator: Operator, conditions: Vec<Filter<T>>) -> Self {
        Filter::FilterOperator(FilterOperator {
            operator,
            conditions,
        })
    }

    pub(crate) fn and<U, V>(conditions: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<Filter<T>>,
    {
        Filter::FilterOperator(FilterOperator {
            operator: Operator::And,
            conditions: conditions
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        })
    }

    pub(crate) fn or<U, V>(conditions: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<Filter<T>>,
    {
        Filter::FilterOperator(FilterOperator {
            operator: Operator::Or,
            conditions: conditions
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        })
    }

    pub(crate) fn not<U, V>(conditions: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<Filter<T>>,
    {
        Filter::FilterOperator(FilterOperator {
            operator: Operator::Not,
            conditions: conditions
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        })
    }
}
