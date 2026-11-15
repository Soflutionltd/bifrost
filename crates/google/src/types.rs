use serde::{Deserialize, Serialize};

/// A Gmail history sequence, decoded from the provider's decimal string once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryId(pub(crate) u64);

impl std::fmt::Display for HistoryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl Serialize for HistoryId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for HistoryId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map(Self).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailMessage {
    pub(crate) id: String,
    pub(crate) thread_id: String,
    #[serde(default)]
    pub(crate) label_ids: Vec<String>,
    #[serde(default)]
    pub(crate) snippet: String,
    pub(crate) history_id: Option<HistoryId>,
    pub(crate) internal_date: Option<String>,
    pub(crate) payload: Option<GmailPayload>,
    pub(crate) size_estimate: Option<i64>,
    pub(crate) raw: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailPayload {
    pub(crate) mime_type: String,
    #[serde(default)]
    pub(crate) filename: String,
    #[serde(default)]
    pub(crate) headers: Vec<GmailHeader>,
    pub(crate) body: Option<GmailBody>,
    #[serde(default)]
    pub(crate) parts: Vec<GmailPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GmailHeader {
    pub(crate) name: String,
    pub(crate) value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailBody {
    pub(crate) attachment_id: Option<String>,
    #[serde(default)]
    pub(crate) size: i64,
    pub(crate) data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailThread {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) messages: Vec<GmailMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailThreadStub {
    pub(crate) id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailLabel {
    pub(crate) id: String,
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) label_type: Option<String>,
    /// Label swatch, present only for user labels that have been
    /// assigned a color. Gmail omits it for uncolored / system labels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) color: Option<GmailLabelColor>,
}

/// Gmail label `color` object: a background / text color pair, each a
/// `#rrggbb` hex string drawn from Gmail's fixed palette.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailLabelColor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) background_color: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) text_color: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    pub(crate) criteria: GmailFilterCriteria,
    pub(crate) action: GmailFilterAction,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailFilterCriteria {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) negated_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) has_attachment: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) exclude_chats: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) size_comparison: Option<GmailFilterSizeComparison>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum GmailFilterSizeComparison {
    Smaller,
    Larger,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailFilterAction {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) add_label_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) remove_label_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) forward: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ListFiltersResponse {
    #[serde(default)]
    pub(crate) filter: Vec<GmailFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailHistoryResponse {
    #[serde(default)]
    pub(crate) history: Vec<GmailHistoryItem>,
    pub(crate) history_id: HistoryId,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailHistoryItem {
    #[serde(default)]
    pub(crate) messages_added: Vec<GmailHistoryMessageWrapper>,
    #[serde(default)]
    pub(crate) messages_deleted: Vec<GmailHistoryMessageWrapper>,
    #[serde(default)]
    pub(crate) labels_added: Vec<GmailHistoryLabelWrapper>,
    #[serde(default)]
    pub(crate) labels_removed: Vec<GmailHistoryLabelWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GmailHistoryMessageWrapper {
    pub(crate) message: GmailMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailHistoryLabelWrapper {
    pub(crate) message: GmailMessage,
    pub(crate) label_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListThreadsResponse {
    #[serde(default)]
    pub(crate) threads: Vec<GmailThreadStub>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListMessagesResponse {
    #[serde(default)]
    pub(crate) messages: Vec<GmailMessageStub>,
    pub(crate) next_page_token: Option<String>,
    pub(crate) result_size_estimate: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailMessageStub {
    pub(crate) id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ListLabelsResponse {
    #[serde(default)]
    pub(crate) labels: Vec<GmailLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailAttachmentData {
    pub(crate) data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GmailDraft {
    pub(crate) id: String,
    pub(crate) message: GmailMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailSendAs {
    pub(crate) send_as_email: String,
    pub(crate) display_name: Option<String>,
    pub(crate) is_default: Option<bool>,
    pub(crate) signature: Option<String>,
    pub(crate) reply_to_address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListSendAsResponse {
    #[serde(default)]
    pub(crate) send_as: Vec<GmailSendAs>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailProfile {
    pub(crate) email_address: String,
    pub(crate) history_id: HistoryId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailVacationSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) enable_auto_reply: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response_subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response_body_plain_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response_body_html: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end_time: Option<String>,
}

#[cfg(test)]
mod history_id_tests {
    use super::*;

    #[test]
    fn response_history_ids_reject_malformed_and_overflowing_strings() {
        for value in ["not-a-number", "-1", "18446744073709551616", ""] {
            assert!(
                serde_json::from_value::<GmailProfile>(serde_json::json!({
                    "emailAddress": "a@example.test", "historyId": value
                }))
                .is_err()
            );
            assert!(
                serde_json::from_value::<GmailHistoryResponse>(serde_json::json!({
                    "historyId": value
                }))
                .is_err()
            );
            assert!(
                serde_json::from_value::<GmailMessage>(serde_json::json!({
                    "id": "m", "threadId": "t", "historyId": value
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn history_id_preserves_the_full_unsigned_range_as_a_wire_string() {
        let value: HistoryId =
            serde_json::from_str(r#""18446744073709551615""#).expect("maximum history id");
        assert_eq!(value.0, u64::MAX);
        assert_eq!(
            serde_json::to_string(&value).expect("serialize"),
            r#""18446744073709551615""#
        );
    }
}
