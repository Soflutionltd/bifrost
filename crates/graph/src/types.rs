use serde::{Deserialize, Serialize};

use crate::origin::ProviderLink;

/// Both links are untrusted server output: they become request targets,
/// caller cursors or persisted checkpoints only through admission (see
/// `crate::origin`).
#[derive(Debug, Deserialize)]
pub(crate) struct ODataCollection<T> {
    pub(crate) value: Vec<T>,
    #[serde(rename = "@odata.nextLink")]
    pub(crate) next_link: Option<ProviderLink>,
    #[serde(rename = "@odata.deltaLink")]
    pub(crate) delta_link: Option<ProviderLink>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphMailFolder {
    pub(crate) id: String,
    pub(crate) display_name: Option<String>,
    pub(crate) child_folder_count: Option<i32>,
    pub(crate) parent_folder_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphProfile {
    pub(crate) display_name: Option<String>,
    pub(crate) mail: Option<String>,
    pub(crate) user_principal_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphMessageRule {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) is_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "GraphMessageRulePredicates::is_empty")]
    pub(crate) conditions: GraphMessageRulePredicates,
    #[serde(default, skip_serializing_if = "GraphMessageRulePredicates::is_empty")]
    pub(crate) exceptions: GraphMessageRulePredicates,
    #[serde(default, skip_serializing_if = "GraphMessageRuleActions::is_empty")]
    pub(crate) actions: GraphMessageRuleActions,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphMessageRulePredicates {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) body_contains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) categories: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) from_addresses: Vec<GraphRecipient>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) has_attachments: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) header_contains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) recipient_contains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) sender_contains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) sent_to_addresses: Vec<GraphRecipient>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) subject_contains: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) within_size_range: Option<GraphSizeRange>,
}

impl GraphMessageRulePredicates {
    pub(crate) fn is_empty(&self) -> bool {
        self.body_contains.is_empty()
            && self.categories.is_empty()
            && self.from_addresses.is_empty()
            && self.has_attachments.is_none()
            && self.header_contains.is_empty()
            && self.recipient_contains.is_empty()
            && self.sender_contains.is_empty()
            && self.sent_to_addresses.is_empty()
            && self.subject_contains.is_empty()
            && self.within_size_range.is_none()
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphMessageRuleActions {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) assign_categories: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) delete: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) forward_to: Vec<GraphRecipient>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mark_as_read: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) move_to_folder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) permanent_delete: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) redirect_to: Vec<GraphRecipient>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stop_processing_rules: Option<bool>,
}

impl GraphMessageRuleActions {
    pub(crate) fn is_empty(&self) -> bool {
        self.assign_categories.is_empty()
            && self.delete.is_none()
            && self.forward_to.is_empty()
            && self.mark_as_read.is_none()
            && self.move_to_folder.is_none()
            && self.permanent_delete.is_none()
            && self.redirect_to.is_empty()
            && self.stop_processing_rules.is_none()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphRecipient {
    pub(crate) email_address: GraphEmailAddress,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphEmailAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    pub(crate) address: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphSizeRange {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) minimum_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) maximum_size: Option<u64>,
}

/// The `$select` fields we request for sync messages.
pub(crate) const MESSAGE_SELECT: &str = "\
id,conversationId,subject,bodyPreview,body,uniqueBody,from,\
toRecipients,ccRecipients,bccRecipients,replyTo,\
receivedDateTime,sentDateTime,isRead,isDraft,hasAttachments,\
importance,parentFolderId,categories,flag,changeKey,\
inferenceClassification,isReadReceiptRequested,internetMessageHeaders,internetMessageId";

/// The `$select` fields we request for contact sync.
pub(crate) const CONTACT_SELECT: &str = "id,displayName,emailAddresses,parentFolderId";

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BatchRequestItem {
    pub(crate) id: String,
    pub(crate) method: String,
    pub(crate) url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) body: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) headers: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BatchRequest {
    pub(crate) requests: Vec<BatchRequestItem>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BatchResponseItem {
    pub(crate) id: String,
    pub(crate) status: u16,
    #[serde(default)]
    pub(crate) headers: Option<std::collections::HashMap<String, String>>,
    pub(crate) body: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BatchResponse {
    pub(crate) responses: Vec<BatchResponseItem>,
}
