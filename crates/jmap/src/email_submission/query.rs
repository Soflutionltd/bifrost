use jiff::Timestamp;
use serde::Serialize;

use crate::core::query::{self, QueryObject};
use crate::email::EmailId;
use crate::identity::IdentityId;
use crate::thread::ThreadId;

use super::{EmailSubmission, UndoStatus};

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Filter {
    IdentityIds {
        #[serde(rename = "identityIds")]
        value: Vec<IdentityId>,
    },
    EmailIds {
        #[serde(rename = "emailIds")]
        value: Vec<EmailId>,
    },
    ThreadIds {
        #[serde(rename = "threadIds")]
        value: Vec<ThreadId>,
    },
    UndoStatus {
        #[serde(rename = "undoStatus")]
        value: UndoStatus,
    },
    Before {
        #[serde(rename = "before")]
        value: Timestamp,
    },
    After {
        #[serde(rename = "after")]
        value: Timestamp,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub(crate) enum Comparator {
    #[serde(rename = "emailId")]
    EmailId,
    #[serde(rename = "threadId")]
    ThreadId,
    #[serde(rename = "sentAt")]
    SentAt,
}

impl Filter {
    pub(crate) fn identity_ids<U, V>(value: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<IdentityId>,
    {
        Filter::IdentityIds {
            value: value.into_iter().map(std::convert::Into::into).collect(),
        }
    }

    pub(crate) fn email_ids<U, V>(value: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<EmailId>,
    {
        Filter::EmailIds {
            value: value.into_iter().map(std::convert::Into::into).collect(),
        }
    }

    pub(crate) fn thread_ids<U, V>(value: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<ThreadId>,
    {
        Filter::ThreadIds {
            value: value.into_iter().map(std::convert::Into::into).collect(),
        }
    }

    pub(crate) fn undo_status(value: UndoStatus) -> Self {
        Filter::UndoStatus { value }
    }

    pub(crate) fn before(value: Timestamp) -> Self {
        Filter::Before { value }
    }

    pub(crate) fn after(value: Timestamp) -> Self {
        Filter::After { value }
    }
}

impl Comparator {
    pub(crate) fn email_id() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::EmailId)
    }

    pub(crate) fn thread_id() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::ThreadId)
    }

    pub(crate) fn sent_at() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::SentAt)
    }
}

impl QueryObject for EmailSubmission {
    type QueryArguments = ();

    type Filter = Filter;

    type Sort = Comparator;
}
