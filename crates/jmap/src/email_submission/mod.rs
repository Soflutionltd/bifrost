// The Account impl drives EmailSubmission/set via the send_message
// path; Get / Changes / Query / QueryChanges and most query AST
// variants are unused today but kept for completeness.
#![allow(dead_code)]

pub(crate) mod get;
pub(crate) mod query;
pub(crate) mod set;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::request::ResultReference;
use crate::email::{EmailId, EmailPatch};
use crate::identity::IdentityId;
use crate::thread::ThreadId;

mod marker {
    pub(crate) enum EmailSubmission {}
}
/// Strongly-typed EmailSubmission ID.
pub(crate) type EmailSubmissionId = crate::core::id::Id<marker::EmailSubmission>;

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct SetArguments {
    /// Patches to apply to the referenced email on successful submit,
    /// keyed by an `EmailSubmission` reference: prefix a create-id with `#`
    /// and use an existing server id without a prefix, per RFC 8621.
    #[serde(rename = "onSuccessUpdateEmail")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_update_email: Option<HashMap<String, EmailPatch>>,
    #[serde(rename = "onSuccessDestroyEmail")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_destroy_email: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EmailSubmission {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<EmailSubmissionId>,

    #[serde(rename = "identityId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) identity_id: Option<IdentityId>,

    #[serde(rename = "emailId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email_id: Option<EmailId>,

    #[serde(rename = "threadId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) thread_id: Option<ThreadId>,

    #[serde(rename = "envelope")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) envelope: Option<Envelope>,

    #[serde(rename = "sendAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) send_at: Option<Timestamp>,

    #[serde(rename = "undoStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) undo_status: Option<UndoStatus>,

    #[serde(rename = "deliveryStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) delivery_status: Option<HashMap<String, DeliveryStatus>>,

    #[serde(rename = "dsnBlobIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) dsn_blob_ids: Option<Vec<crate::core::id::BlobId>>,

    #[serde(rename = "mdnBlobIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) mdn_blob_ids: Option<Vec<crate::core::id::BlobId>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EmailSubmissionCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "identityId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) identity_id: Option<IdentityId>,

    #[serde(rename = "emailId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email_id: Option<EmailId>,

    #[serde(rename = "#emailId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email_id_ref: Option<ResultReference>,

    #[serde(rename = "envelope")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) envelope: Option<Envelope>,

    #[serde(rename = "undoStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) undo_status: Option<UndoStatus>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct EmailSubmissionPatch {
    #[serde(rename = "undoStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) undo_status: Option<UndoStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Envelope {
    #[serde(rename = "mailFrom")]
    pub(super) mail_from: Address,

    #[serde(rename = "rcptTo")]
    pub(super) rcpt_to: Vec<Address>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Address {
    pub(super) email: String,
    // RFC 8621 s7 types this `String[String|null]|null` - a nullable
    // member, not an optional one, and RFC 8620 s5.3 only lets a create
    // omit a property that has a defined default. Keep emitting the
    // explicit `null` so strict servers accept the envelope.
    pub(super) parameters: Option<HashMap<String, Option<String>>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum UndoStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "final")]
    Final,
    #[serde(rename = "canceled")]
    Canceled,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DeliveryStatus {
    #[serde(rename = "smtpReply")]
    smtp_reply: String,

    #[serde(rename = "delivered")]
    delivered: Delivered,

    #[serde(rename = "displayed")]
    displayed: Displayed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum Delivered {
    #[serde(rename = "queued")]
    Queued,
    #[serde(rename = "yes")]
    Yes,
    #[serde(rename = "no")]
    No,
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum Displayed {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "yes")]
    Yes,
    #[serde(other)]
    Other,
}

crate::core::method::define_property_enum! {
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
    #[non_exhaustive]
    pub(crate) enum Property {
        Id => "id",
        IdentityId => "identityId",
        EmailId => "emailId",
        ThreadId => "threadId",
        Envelope => "envelope",
        SendAt => "sendAt",
        UndoStatus => "undoStatus",
        DeliveryStatus => "deliveryStatus",
        DsnBlobIds => "dsnBlobIds",
        MdnBlobIds => "mdnBlobIds",
    }
}

impl crate::core::Object for EmailSubmission {
    type Property = Property;
    type Id = EmailSubmissionId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for EmailSubmission {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for EmailSubmission {
    type GetArguments = ();
}

impl crate::core::set::SetObject for EmailSubmission {
    type Create = EmailSubmissionCreate;
    type Patch = EmailSubmissionPatch;
    type SetArguments = SetArguments;
}

impl crate::core::SetCreate for EmailSubmissionCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        EmailSubmissionCreate {
            _create_id: create_id,
            identity_id: None,
            email_id: None,
            email_id_ref: None,
            envelope: None,
            undo_status: None,
        }
    }
}

crate::define_get_method!(
    EmailSubmissionGet,
    EmailSubmission,
    "EmailSubmission/get",
    crate::core::capability::Submission
);
crate::define_set_method!(
    EmailSubmissionSet,
    EmailSubmission,
    "EmailSubmission/set",
    crate::core::capability::Submission
);
crate::define_changes_method!(
    EmailSubmissionChanges,
    EmailSubmission,
    "EmailSubmission/changes",
    crate::core::capability::Submission
);
crate::define_query_method!(
    EmailSubmissionQuery,
    EmailSubmission,
    "EmailSubmission/query",
    crate::core::capability::Submission
);
crate::define_query_changes_method!(
    EmailSubmissionQueryChanges,
    EmailSubmission,
    "EmailSubmission/queryChanges",
    crate::core::capability::Submission
);
