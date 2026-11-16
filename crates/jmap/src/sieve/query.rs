use serde::Serialize;

use crate::core::query::{self, QueryObject};

use super::SieveScript;

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Filter {
    Name {
        #[serde(rename = "name")]
        value: String,
    },
    IsActive {
        #[serde(rename = "isActive")]
        value: bool,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "property")]
#[non_exhaustive]
pub(crate) enum Comparator {
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "isActive")]
    IsActive,
}

impl Filter {
    pub(crate) fn name(value: impl Into<String>) -> Self {
        Filter::Name {
            value: value.into(),
        }
    }

    pub(crate) fn is_active(value: bool) -> Self {
        Filter::IsActive { value }
    }
}

impl Comparator {
    pub(crate) fn name() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::Name)
    }

    pub(crate) fn is_active() -> query::Comparator<Comparator> {
        query::Comparator::new(Comparator::IsActive)
    }
}

impl QueryObject for SieveScript {
    type QueryArguments = ();

    type Filter = Filter;

    type Sort = Comparator;
}

#[cfg(test)]
mod wire_tests;
