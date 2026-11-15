// The Account impl only uses `PrincipalId` and `ACL` from this module;
// the full RFC 8621 §6 surface is retained for completeness and for the
// JMAP Principal/* methods a future stage may expose.
#![allow(dead_code)]

pub(crate) mod availability;
pub(crate) mod get;
pub(crate) mod query;
pub(crate) mod set;

use crate::core::set::{skip_if_empty_list, skip_if_empty_map, skip_if_empty_str};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

mod marker {
    pub(crate) enum Principal {}
}
/// Strongly-typed Principal ID.
pub(crate) type PrincipalId = crate::core::id::Id<marker::Principal>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Principal {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<PrincipalId>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ptype: Option<Type>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "timeZone")]
    pub(super) timezone: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) capabilities: Option<HashMap<String, serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) accounts: Option<HashMap<crate::core::id::AccountId, PrincipalAccount>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) aliases: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) dkim: Option<DKIM>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) quota: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) picture: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) members: Option<Vec<PrincipalId>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) acl: Option<HashMap<PrincipalId, Vec<ACL>>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PrincipalCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ptype: Option<Type>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) name: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) description: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) email: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    #[serde(rename = "timeZone")]
    pub(super) timezone: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) capabilities: Option<HashMap<String, serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) accounts: Option<HashMap<crate::core::id::AccountId, PrincipalAccount>>,

    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) aliases: Option<Vec<String>>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) dkim: Option<DKIM>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) quota: Option<u32>,

    #[serde(skip_serializing_if = "skip_if_empty_str")]
    pub(super) picture: Option<String>,

    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) members: Option<Vec<PrincipalId>>,

    #[serde(skip_serializing_if = "skip_if_empty_map")]
    pub(super) acl: Option<HashMap<PrincipalId, Vec<ACL>>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct PrincipalPatch {
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ptype: Option<Type>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "timeZone")]
    pub(super) timezone: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) capabilities: Option<HashMap<String, serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) accounts: Option<HashMap<crate::core::id::AccountId, PrincipalAccount>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) aliases: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) secret: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) dkim: Option<DKIM>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) quota: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) picture: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) members: Option<Vec<PrincipalId>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) acl: Option<HashMap<PrincipalId, Vec<ACL>>>,

    #[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) property_patch: Option<HashMap<String, bool>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrincipalAccount {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,

    #[serde(rename = "isPersonal")]
    #[serde(default)]
    is_personal: bool,

    #[serde(rename = "isReadOnly")]
    #[serde(default)]
    is_read_only: bool,

    #[serde(rename = "accountCapabilities")]
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    account_capabilities: HashMap<String, serde_json::Value>,
}

impl PrincipalAccount {
    pub(crate) fn new(name: impl Into<String>, is_personal: bool, is_read_only: bool) -> Self {
        PrincipalAccount {
            name: Some(name.into()),
            is_personal,
            is_read_only,
            account_capabilities: HashMap::new(),
        }
    }

    pub(crate) fn account_capability(
        mut self,
        uri: impl Into<String>,
        config: serde_json::Value,
    ) -> Self {
        self.account_capabilities.insert(uri.into(), config);
        self
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn is_personal(&self) -> bool {
        self.is_personal
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    pub(crate) fn account_capabilities(&self) -> &HashMap<String, serde_json::Value> {
        &self.account_capabilities
    }
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id = 0 => "id",
        Type = 1 => "type",
        Name = 2 => "name",
        Description = 3 => "description",
        Email = 4 => "email",
        Timezone = 5 => "timeZone",
        Capabilities = 6 => "capabilities",
        Aliases = 7 => "aliases",
        Secret = 8 => "secret",
        DKIM = 9 => "dkim",
        Quota = 10 => "quota",
        Picture = 11 => "picture",
        Members = 12 => "members",
        Accounts = 13 => "accounts",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub(crate) enum ACL {
    #[serde(rename = "mayRename")]
    Rename = 1,
    #[serde(rename = "mayDelete")]
    Delete = 2,
    #[serde(rename = "mayReadItems")]
    ReadItems = 3,
    #[serde(rename = "mayAddItems")]
    AddItems = 4,
    #[serde(rename = "maySetKeywords")]
    SetKeywords = 5,
    #[serde(rename = "mayRemoveItems")]
    RemoveItems = 6,
    #[serde(rename = "mayCreateChild")]
    CreateChild = 7,
    #[serde(rename = "mayShare")]
    Administer = 8,
    #[serde(rename = "maySubmit")]
    Submit = 10,
    #[serde(rename = "maySetSeen")]
    SetSeen = 11,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) enum Type {
    #[serde(rename = "individual")]
    Individual,
    #[serde(rename = "group")]
    Group,
    #[serde(rename = "resource")]
    Resource,
    #[serde(rename = "location")]
    Location,
    #[serde(rename = "domain")]
    Domain,
    #[serde(rename = "list")]
    List,
    #[serde(rename = "other")]
    Other,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DKIM {
    #[serde(rename = "dkimSelector")]
    dkim_selector: Option<String>,
    #[serde(rename = "dkimExpiration")]
    dkim_expiration: Option<i64>,
}

impl DKIM {
    pub(crate) fn new(
        dkim_selector: Option<impl Into<String>>,
        dkim_expiration: Option<i64>,
    ) -> DKIM {
        DKIM {
            dkim_selector: dkim_selector.map(Into::into),
            dkim_expiration,
        }
    }

    pub(crate) fn selector(&self) -> Option<&str> {
        self.dkim_selector.as_deref()
    }

    pub(crate) fn expiration(&self) -> Option<i64> {
        self.dkim_expiration
    }
}

impl Display for ACL {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl ACL {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            ACL::Rename => "mayRename",
            ACL::Delete => "mayDelete",
            ACL::ReadItems => "mayReadItems",
            ACL::AddItems => "mayAddItems",
            ACL::SetKeywords => "maySetKeywords",
            ACL::RemoveItems => "mayRemoveItems",
            ACL::CreateChild => "mayCreateChild",
            ACL::Administer => "mayShare",
            ACL::Submit => "maySubmit",
            ACL::SetSeen => "maySetSeen",
        }
    }
}

impl crate::core::Object for Principal {
    type Property = Property;
    type Id = PrincipalId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Principal {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for Principal {
    type GetArguments = ();
}

impl crate::core::set::SetObject for Principal {
    type Create = PrincipalCreate;
    type Patch = PrincipalPatch;
    type SetArguments = ();
}

impl crate::core::SetCreate for PrincipalCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        PrincipalCreate {
            _create_id: create_id,
            ptype: None,
            name: String::new().into(),
            description: String::new().into(),
            email: String::new().into(),
            timezone: String::new().into(),
            capabilities: None,
            accounts: None,
            aliases: Vec::with_capacity(0).into(),
            secret: String::new().into(),
            dkim: None,
            quota: None,
            picture: String::new().into(),
            members: Vec::with_capacity(0).into(),
            acl: HashMap::with_capacity(0).into(),
        }
    }
}

crate::define_get_method!(
    PrincipalGet,
    Principal,
    "Principal/get",
    crate::core::capability::Principals
);
crate::define_set_method!(
    PrincipalSet,
    Principal,
    "Principal/set",
    crate::core::capability::Principals
);
crate::define_changes_method!(
    PrincipalChanges,
    Principal,
    "Principal/changes",
    crate::core::capability::Principals
);
crate::define_query_method!(
    PrincipalQuery,
    Principal,
    "Principal/query",
    crate::core::capability::Principals
);
crate::define_query_changes_method!(
    PrincipalQueryChanges,
    Principal,
    "Principal/queryChanges",
    crate::core::capability::Principals
);

#[cfg(test)]
mod time_zone_tests {
    use super::{Principal, PrincipalCreate, PrincipalPatch, Property};
    use crate::core::SetCreate;
    use serde_json::json;

    #[test]
    fn time_zone_uses_the_same_wire_name_for_reads_and_writes() {
        let principal: Principal =
            serde_json::from_value(json!({"timeZone": "Europe/Oslo"})).unwrap();
        assert_eq!(principal.timezone(), Some("Europe/Oslo"));
        assert_eq!(
            serde_json::to_value(&principal).unwrap()["timeZone"],
            "Europe/Oslo"
        );
        assert_eq!(
            serde_json::to_value(Property::Timezone).unwrap(),
            "timeZone"
        );
        assert_eq!(Property::Timezone.to_string(), "timeZone");

        let mut create = PrincipalCreate::new(None);
        create.timezone(Some("Europe/Oslo"));
        let mut patch = PrincipalPatch::default();
        patch.timezone(Some("Europe/Oslo"));
        for value in [
            serde_json::to_value(create).unwrap(),
            serde_json::to_value(patch).unwrap(),
        ] {
            assert_eq!(value["timeZone"], "Europe/Oslo");
            assert!(value.get("timezone").is_none());
        }
    }
}
