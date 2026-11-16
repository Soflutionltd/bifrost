// The Account impl wires identities_list / identity_update; the
// IdentityChanges shape and a few patch helpers (`bcc`, etc.) are
// kept built for completeness.
#![allow(dead_code)]

use crate::core::field::Field;

pub(crate) mod get;
pub(crate) mod set;

use crate::email::EmailAddress;
use serde::{Deserialize, Serialize};

mod marker {
    pub(crate) enum Identity {}
}
/// Strongly-typed Identity ID.
pub(crate) type IdentityId = crate::core::id::Id<marker::Identity>;

/// Server-returned Identity object (RFC 8621 §6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Identity {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<IdentityId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "email")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email: Option<String>,

    #[serde(rename = "replyTo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reply_to: Option<Vec<EmailAddress>>,

    #[serde(rename = "bcc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) bcc: Option<Vec<EmailAddress>>,

    #[serde(rename = "textSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_signature: Option<String>,

    #[serde(rename = "htmlSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_signature: Option<String>,

    #[serde(rename = "mayDelete")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) may_delete: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IdentityCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<crate::core::creation::CreationId>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "email")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email: Option<String>,

    #[serde(rename = "replyTo")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) reply_to: Field<Vec<EmailAddress>>,

    #[serde(rename = "bcc")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) bcc: Field<Vec<EmailAddress>>,

    #[serde(rename = "textSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_signature: Option<String>,

    #[serde(rename = "htmlSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IdentityPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "replyTo")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) reply_to: Field<Vec<EmailAddress>>,

    #[serde(rename = "bcc")]
    #[serde(skip_serializing_if = "Field::is_omitted")]
    pub(super) bcc: Field<Vec<EmailAddress>>,

    #[serde(rename = "textSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_signature: Option<String>,

    #[serde(rename = "htmlSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_signature: Option<String>,
}

impl Default for IdentityPatch {
    fn default() -> Self {
        Self {
            name: None,
            reply_to: Field::Omitted,
            bcc: Field::Omitted,
            text_signature: None,
            html_signature: None,
        }
    }
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        Name => "name",
        Email => "email",
        ReplyTo => "replyTo",
        Bcc => "bcc",
        TextSignature => "textSignature",
        HtmlSignature => "htmlSignature",
        MayDelete => "mayDelete",
    }
}

impl crate::core::Object for Identity {
    crate::core::method::object_metadata!(Identity);
    type Property = Property;
    type Id = IdentityId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Identity {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for Identity {
    type GetArguments = ();
}

impl crate::core::set::SetObject for Identity {
    type Create = IdentityCreate;
    type Patch = IdentityPatch;
    type SetArguments = ();
}

impl crate::core::SetCreate for IdentityCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.as_ref().map(ToString::to_string)
    }
    fn set_create_id(&mut self, id: crate::core::creation::CreationId) {
        self._create_id = Some(id);
    }

    fn new(create_id: Option<crate::core::creation::CreationId>) -> Self {
        IdentityCreate {
            _create_id: create_id,
            name: None,
            email: None,
            reply_to: Field::Omitted,
            bcc: Field::Omitted,
            text_signature: None,
            html_signature: None,
        }
    }
}

crate::define_get_method!(IdentityGet, Identity);
crate::define_set_method!(IdentitySet, Identity);
crate::define_changes_method!(IdentityChanges, Identity);

#[cfg(test)]
mod wire_tests;
