//! Graph validators retain their provenance until serialized at the wire.

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GraphETag(bifrost_types::EntityTag);

impl GraphETag {
    pub(crate) fn from_odata_annotation(value: &str) -> Self {
        Self(value.into())
    }

    pub(crate) fn from_change_key(value: &str) -> Self {
        Self(format!("W/\"{value}\"").into())
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn into_entity_tag(self) -> bifrost_types::EntityTag {
        self.0
    }

    pub(crate) fn into_string(self) -> String {
        self.0.into_string()
    }
}

impl std::ops::Deref for GraphETag {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::GraphETag;

    #[test]
    fn change_keys_are_wrapped_and_annotations_are_preserved() {
        let key = GraphETag::from_change_key("CK1");
        assert_eq!(key.as_str(), "W/\"CK1\"");
        assert_ne!(key.as_str(), "CK1");
        assert_eq!(GraphETag::from_odata_annotation(key.as_str()), key);
        assert_eq!(
            GraphETag::from_odata_annotation("\"strong\"").as_str(),
            "\"strong\""
        );
    }
}

#[cfg(test)]
mod wire_tests {
    use super::GraphETag;
    use crate::client::{GraphClient, Resource, ScriptedRestResponse};

    #[tokio::test]
    async fn conditional_patch_serializes_a_typed_change_key_as_an_entity_tag() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT)]);
        let validator = GraphETag::from_change_key("CK1");
        client
            .patch_if_match(
                Resource::Outlook,
                "/me/messages/m1",
                &validator,
                &serde_json::json!({"isRead": true}),
                bifrost_net::ReplayPolicy::Never,
            )
            .await
            .expect("conditional PATCH");
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].if_match.as_deref(), Some("W/\"CK1\""));
    }
}
