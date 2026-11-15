use serde::Serialize;

use crate::core::query::{self, QueryObject};

use super::{Principal, PrincipalId, Type};
use crate::core::id::AccountId;

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Filter {
    /// RFC 9670: Match principals owning the specified accounts.
    /// Placed first because its `accountIds` array value is unambiguous
    /// in serde's untagged trial order (no other variant uses an array).
    AccountIds {
        #[serde(rename = "accountIds")]
        value: Vec<AccountId>,
    },
    Email {
        #[serde(rename = "email")]
        value: String,
    },
    Name {
        #[serde(rename = "name")]
        value: String,
    },
    DomainName {
        #[serde(rename = "domainName")]
        value: String,
    },
    Text {
        #[serde(rename = "text")]
        value: String,
    },
    Type {
        #[serde(rename = "type")]
        value: Type,
    },
    Timezone {
        #[serde(rename = "timeZone")]
        value: String,
    },
    Members {
        #[serde(rename = "members")]
        value: PrincipalId,
    },
    QuotaLt {
        #[serde(rename = "quotaLowerThan")]
        value: u32,
    },
    QuotaGt {
        #[serde(rename = "quotaGreaterThan")]
        value: u32,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub(crate) enum Comparator {
    #[serde(rename = "type")]
    Type,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "email")]
    Email,
}

impl Filter {
    /// RFC 9670: Match principals owning the specified accounts.
    pub(crate) fn account_ids(value: impl IntoIterator<Item = impl Into<AccountId>>) -> Self {
        Filter::AccountIds {
            value: value.into_iter().map(Into::into).collect(),
        }
    }

    pub(crate) fn name(value: impl Into<String>) -> Self {
        Filter::Name {
            value: value.into(),
        }
    }

    pub(crate) fn domain_name(value: impl Into<String>) -> Self {
        Filter::DomainName {
            value: value.into(),
        }
    }

    pub(crate) fn email(value: impl Into<String>) -> Self {
        Filter::Email {
            value: value.into(),
        }
    }

    pub(crate) fn text(value: impl Into<String>) -> Self {
        Filter::Text {
            value: value.into(),
        }
    }

    pub(crate) fn timezone(value: impl Into<String>) -> Self {
        Filter::Timezone {
            value: value.into(),
        }
    }

    pub(crate) fn members(value: impl Into<PrincipalId>) -> Self {
        Filter::Members {
            value: value.into(),
        }
    }

    pub(crate) fn ptype(value: Type) -> Self {
        Filter::Type { value }
    }

    pub(crate) fn quota_lower_than(value: u32) -> Self {
        Filter::QuotaLt { value }
    }

    pub(crate) fn quota_greater_than(value: u32) -> Self {
        Filter::QuotaGt { value }
    }
}

impl Comparator {
    pub(crate) fn name() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Name)
    }

    pub(crate) fn email() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Email)
    }

    pub(crate) fn ptype() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Type)
    }
}

impl QueryObject for Principal {
    type QueryArguments = ();

    type Filter = Filter;

    type Sort = Comparator;
}
