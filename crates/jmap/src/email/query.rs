use super::keyword::Keyword;
use jiff::Timestamp;
use serde::Serialize;

use crate::core::query::{self, QueryObject};
use crate::mailbox::MailboxId;
use crate::thread::ThreadId;

use super::{Email, EmailId, QueryArguments};

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Filter {
    InMailbox {
        #[serde(rename = "inMailbox")]
        value: MailboxId,
    },
    InMailboxOtherThan {
        #[serde(rename = "inMailboxOtherThan")]
        value: Vec<MailboxId>,
    },
    Before {
        #[serde(rename = "before")]
        value: Timestamp,
    },
    After {
        #[serde(rename = "after")]
        value: Timestamp,
    },
    MinSize {
        #[serde(rename = "minSize")]
        value: u32,
    },
    MaxSize {
        #[serde(rename = "maxSize")]
        value: u32,
    },
    AllInThreadHaveKeyword {
        #[serde(rename = "allInThreadHaveKeyword")]
        value: Keyword,
    },
    SomeInThreadHaveKeyword {
        #[serde(rename = "someInThreadHaveKeyword")]
        value: Keyword,
    },
    NoneInThreadHaveKeyword {
        #[serde(rename = "noneInThreadHaveKeyword")]
        value: Keyword,
    },
    HasKeyword {
        #[serde(rename = "hasKeyword")]
        value: Keyword,
    },
    NotKeyword {
        #[serde(rename = "notKeyword")]
        value: Keyword,
    },
    HasAttachment {
        #[serde(rename = "hasAttachment")]
        value: bool,
    },
    Text {
        #[serde(rename = "text")]
        value: String,
    },
    From {
        #[serde(rename = "from")]
        value: String,
    },
    To {
        #[serde(rename = "to")]
        value: String,
    },
    Cc {
        #[serde(rename = "cc")]
        value: String,
    },
    Bcc {
        #[serde(rename = "bcc")]
        value: String,
    },
    Subject {
        #[serde(rename = "subject")]
        value: String,
    },
    Body {
        #[serde(rename = "body")]
        value: String,
    },
    Header {
        #[serde(rename = "header")]
        value: Vec<String>,
    },

    // Non-standard
    Id {
        #[serde(rename = "id")]
        value: Vec<EmailId>,
    },
    SentBefore {
        #[serde(rename = "sentBefore")]
        value: Timestamp,
    },
    SentAfter {
        #[serde(rename = "sentAfter")]
        value: Timestamp,
    },
    InThread {
        #[serde(rename = "inThread")]
        value: ThreadId,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub(crate) enum Comparator {
    #[serde(rename = "receivedAt")]
    ReceivedAt,
    #[serde(rename = "size")]
    Size,
    #[serde(rename = "from")]
    From,
    #[serde(rename = "to")]
    To,
    #[serde(rename = "subject")]
    Subject,
    #[serde(rename = "sentAt")]
    SentAt,
    #[serde(rename = "hasKeyword")]
    HasKeyword { keyword: Keyword },
    #[serde(rename = "allInThreadHaveKeyword")]
    AllInThreadHaveKeyword { keyword: Keyword },
    #[serde(rename = "someInThreadHaveKeyword")]
    SomeInThreadHaveKeyword { keyword: Keyword },

    // Non-standard
    #[serde(rename = "cc")]
    Cc,
}

impl Filter {
    pub(crate) fn in_mailbox(value: impl Into<MailboxId>) -> Self {
        Filter::InMailbox {
            value: value.into(),
        }
    }

    pub(crate) fn in_mailbox_other_than<U, V>(value: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<MailboxId>,
    {
        Filter::InMailboxOtherThan {
            value: value.into_iter().map(std::convert::Into::into).collect(),
        }
    }

    pub(crate) fn before(value: Timestamp) -> Self {
        Filter::Before { value }
    }

    pub(crate) fn after(value: Timestamp) -> Self {
        Filter::After { value }
    }

    pub(crate) fn min_size(value: u32) -> Self {
        Filter::MinSize { value }
    }

    pub(crate) fn max_size(value: u32) -> Self {
        Filter::MaxSize { value }
    }

    pub(crate) fn all_in_thread_have_keyword(value: Keyword) -> Self {
        Filter::AllInThreadHaveKeyword { value }
    }

    pub(crate) fn some_in_thread_have_keyword(value: Keyword) -> Self {
        Filter::SomeInThreadHaveKeyword { value }
    }

    pub(crate) fn none_in_thread_have_keyword(value: Keyword) -> Self {
        Filter::NoneInThreadHaveKeyword { value }
    }

    pub(crate) fn has_keyword(value: Keyword) -> Self {
        Filter::HasKeyword { value }
    }

    pub(crate) fn not_keyword(value: Keyword) -> Self {
        Filter::NotKeyword { value }
    }

    pub(crate) fn has_attachment(value: bool) -> Self {
        Filter::HasAttachment { value }
    }

    pub(crate) fn text(value: impl Into<String>) -> Self {
        Filter::Text {
            value: value.into(),
        }
    }

    pub(crate) fn from(value: impl Into<String>) -> Self {
        Filter::From {
            value: value.into(),
        }
    }

    pub(crate) fn to(value: impl Into<String>) -> Self {
        Filter::To {
            value: value.into(),
        }
    }

    pub(crate) fn cc(value: impl Into<String>) -> Self {
        Filter::Cc {
            value: value.into(),
        }
    }

    pub(crate) fn bcc(value: impl Into<String>) -> Self {
        Filter::Bcc {
            value: value.into(),
        }
    }

    pub(crate) fn subject(value: impl Into<String>) -> Self {
        Filter::Subject {
            value: value.into(),
        }
    }

    pub(crate) fn body(value: impl Into<String>) -> Self {
        Filter::Body {
            value: value.into(),
        }
    }

    pub(crate) fn header(header: impl Into<String>, v: Option<impl Into<String>>) -> Self {
        let mut value = Vec::with_capacity(2);
        value.push(header.into());
        if let Some(v) = v {
            value.push(v.into());
        }
        Filter::Header { value }
    }

    // Stalwart JMAP specific
    pub(crate) fn id<U, V>(value: U) -> Self
    where
        U: IntoIterator<Item = V>,
        V: Into<EmailId>,
    {
        Filter::Id {
            value: value.into_iter().map(std::convert::Into::into).collect(),
        }
    }

    pub(crate) fn sent_before(value: Timestamp) -> Self {
        Filter::SentBefore { value }
    }

    pub(crate) fn sent_after(value: Timestamp) -> Self {
        Filter::SentAfter { value }
    }

    pub(crate) fn in_thread(value: impl Into<ThreadId>) -> Self {
        Filter::InThread {
            value: value.into(),
        }
    }
}

impl Comparator {
    pub(crate) fn received_at() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::ReceivedAt)
    }

    pub(crate) fn size() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Size)
    }

    pub(crate) fn from() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::From)
    }

    pub(crate) fn to() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::To)
    }

    pub(crate) fn cc() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Cc)
    }

    pub(crate) fn subject() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Subject)
    }

    pub(crate) fn sent_at() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::SentAt)
    }

    pub(crate) fn has_keyword(keyword: Keyword) -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::HasKeyword { keyword })
    }

    pub(crate) fn all_in_thread_have_keyword(keyword: Keyword) -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::AllInThreadHaveKeyword { keyword })
    }

    pub(crate) fn some_in_thread_have_keyword(keyword: Keyword) -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::SomeInThreadHaveKeyword { keyword })
    }
}

impl QueryObject for Email {
    type QueryArguments = QueryArguments;

    type Filter = Filter;

    type Sort = Comparator;
}
