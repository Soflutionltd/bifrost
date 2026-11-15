// The Account impl uses `QuotaGet` + a few properties; `QuotaChanges`,
// `QuotaQuery`, and the query AST are retained for completeness.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod query;

use serde::{Deserialize, Serialize};

use crate::core::field::Field;

mod marker {
    pub(crate) enum Quota {}
}
/// Strongly-typed Quota ID.
pub(crate) type QuotaId = crate::core::id::Id<marker::Quota>;

/// A quota object representing a storage or count limit (RFC 9425).
/// Quota is read-only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Quota {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<QuotaId>,

    #[serde(rename = "resourceType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) resource_type: Option<String>,

    #[serde(rename = "used")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) used: Option<u64>,

    #[serde(rename = "hardLimit")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) hard_limit: Option<u64>,

    #[serde(rename = "scope")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) scope: Option<String>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "types")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) types: Option<Vec<String>>,

    #[serde(rename = "warnLimit")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) warn_limit: Field<u64>,

    #[serde(rename = "softLimit")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) soft_limit: Field<u64>,

    #[serde(rename = "description")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) description: Field<String>,
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        ResourceType => "resourceType",
        Used => "used",
        HardLimit => "hardLimit",
        Scope => "scope",
        Name => "name",
        Types => "types",
        WarnLimit => "warnLimit",
        SoftLimit => "softLimit",
        Description => "description",
    }
}

impl crate::core::Object for Quota {
    type Property = Property;
    type Id = QuotaId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Quota {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for Quota {
    type GetArguments = ();
}

crate::define_get_method!(QuotaGet, Quota, "Quota/get", crate::core::capability::Quota);
crate::define_changes_method!(
    QuotaChanges,
    Quota,
    "Quota/changes",
    crate::core::capability::Quota
);
crate::define_query_method!(
    QuotaQuery,
    Quota,
    "Quota/query",
    crate::core::capability::Quota
);
crate::define_query_changes_method!(
    QuotaQueryChanges,
    Quota,
    "Quota/queryChanges",
    crate::core::capability::Quota
);
