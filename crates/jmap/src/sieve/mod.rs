// SieveScript/* is wired into the Account filter surface in
// `sync/filters.rs`. Parts of the full Sieve CRUD scaffolding here are
// not yet exercised by that path, so the module keeps a dead-code
// allowance for the upcoming Sieve work.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod query;
pub(crate) mod set;
pub(crate) mod validate;

use serde::{Deserialize, Serialize};

use crate::core::id::BlobId;

mod marker {
    pub(crate) enum SieveScript {}
}
/// Strongly-typed SieveScript ID.
pub(crate) type SieveScriptId = crate::core::id::Id<marker::SieveScript>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SieveScript {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<SieveScriptId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) blob_id: Option<BlobId>,

    #[serde(rename = "isActive")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_active: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SieveScriptCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<crate::core::creation::CreationId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) blob_id: Option<BlobId>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct SieveScriptPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "blobId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) blob_id: Option<BlobId>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct SetArguments {
    #[serde(rename = "onSuccessActivateScript")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_activate_script: Option<crate::core::creation::IdOrRef<SieveScriptId>>,
    #[serde(rename = "onSuccessDeactivateScript")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_deactivate_script: Option<bool>,
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Name => "name",
        BlobId => "blobId",
        IsActive => "isActive",
    }
}

impl crate::core::Object for SieveScript {
    type Property = Property;
    type Id = SieveScriptId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for SieveScript {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for SieveScript {
    type GetArguments = ();
}

impl crate::core::set::SetObject for SieveScript {
    type Create = SieveScriptCreate;
    type Patch = SieveScriptPatch;
    type SetArguments = SetArguments;
}

impl crate::core::SetCreate for SieveScriptCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.as_ref().map(ToString::to_string)
    }
    fn set_create_id(&mut self, id: crate::core::creation::CreationId) {
        self._create_id = Some(id);
    }

    fn new(create_id: Option<crate::core::creation::CreationId>) -> Self {
        SieveScriptCreate {
            _create_id: create_id,
            name: None,
            blob_id: None,
        }
    }
}

crate::define_get_method!(
    SieveScriptGet,
    SieveScript,
    "SieveScript/get",
    crate::core::capability::Sieve
);
crate::define_set_method!(
    SieveScriptSet,
    SieveScript,
    "SieveScript/set",
    crate::core::capability::Sieve
);
crate::define_query_method!(
    SieveScriptQuery,
    SieveScript,
    "SieveScript/query",
    crate::core::capability::Sieve
);
