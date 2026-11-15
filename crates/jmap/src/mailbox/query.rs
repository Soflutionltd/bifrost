use serde::Serialize;

use crate::core::query::{self, QueryObject};

use super::{Mailbox, MailboxId, QueryArguments, Role};

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Filter {
    ParentId {
        #[serde(rename = "parentId")]
        value: Option<MailboxId>,
    },
    Name {
        #[serde(rename = "name")]
        value: String,
    },
    Role {
        #[serde(rename = "role")]
        value: Option<Role>,
    },
    HasAnyRole {
        #[serde(rename = "hasAnyRole")]
        value: bool,
    },
    IsSubscribed {
        #[serde(rename = "isSubscribed")]
        value: bool,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub(crate) enum Comparator {
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "sortOrder")]
    SortOrder,
    #[serde(rename = "parentId")]
    ParentId,
}

impl Filter {
    pub(crate) fn parent_id(value: Option<impl Into<MailboxId>>) -> Self {
        Filter::ParentId {
            value: value.map(Into::into),
        }
    }

    pub(crate) fn name(value: impl Into<String>) -> Self {
        Filter::Name {
            value: value.into(),
        }
    }

    pub(crate) fn role(value: Option<Role>) -> Self {
        Filter::Role { value }
    }

    pub(crate) fn has_any_role(value: bool) -> Self {
        Filter::HasAnyRole { value }
    }

    pub(crate) fn is_subscribed(value: bool) -> Self {
        Filter::IsSubscribed { value }
    }
}

impl Comparator {
    pub(crate) fn name() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Name)
    }

    pub(crate) fn sort_order() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::SortOrder)
    }

    pub(crate) fn parent_id() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::ParentId)
    }
}

impl QueryArguments {
    pub(crate) fn sort_as_tree(&mut self, value: bool) -> &mut Self {
        self.sort_as_tree = value;
        self
    }

    pub(crate) fn filter_as_tree(&mut self, value: bool) -> &mut Self {
        self.filter_as_tree = value;
        self
    }
}

impl QueryObject for Mailbox {
    type QueryArguments = QueryArguments;

    type Filter = Filter;

    type Sort = Comparator;
}
