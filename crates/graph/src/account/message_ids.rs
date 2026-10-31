use bifrost_types::{AccountError, AccountOperation, CursorScope, FolderId, ObjectId, ObjectType};

use super::GraphAccount;
use super::foreign::{encode_message_id, parse_folder};
use super::graph_error::{GraphErrorContext, into_account_error};

/// Ids come back in the same wire form the inventory and change streams
/// mint, so a consumer can compare them with what it stored.
pub(crate) async fn folder_message_ids(
    account: &GraphAccount,
    folder: FolderId,
) -> Result<Vec<ObjectId>, AccountError> {
    let native = parse_folder(&folder).native_id().to_string();
    let scope = CursorScope::FolderType {
        folder,
        ty: ObjectType::Email,
    };
    let to_account_error = |error| {
        into_account_error(
            error,
            GraphErrorContext::graph(AccountOperation::SyncInventory),
        )
    };
    let ids = account
        .client_for_scope(&scope)
        .map_err(to_account_error)?
        .list_folder_message_ids(&native)
        .await
        .map_err(to_account_error)?;
    Ok(ids.iter().map(|id| encode_message_id(&scope, id)).collect())
}

#[cfg(test)]
mod tests {
    use super::super::PushMode;
    use super::*;
    use crate::client::{GraphClient, ScriptedRestResponse};

    #[tokio::test]
    async fn lists_every_page_without_message_content() {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                serde_json::json!({
                    "value": [{ "id": "AAMk1" }, { "id": "AAMk2" }],
                    "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$select=id&$top=1000&$skip=2"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                serde_json::json!({ "value": [{ "id": "AAMk3" }] }),
            ),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);

        let ids = folder_message_ids(&account, FolderId("inbox".to_string()))
            .await
            .expect("both pages list");

        assert_eq!(
            ids,
            ["AAMk1", "AAMk2", "AAMk3"].map(|id| ObjectId(id.to_string()))
        );
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .url
                .ends_with("/me/mailFolders/inbox/messages?$select=id&$top=1000"),
            "{}",
            requests[0].url
        );
    }

    #[tokio::test]
    async fn a_failed_page_fails_the_whole_listing() {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                serde_json::json!({
                    "value": [{ "id": "AAMk1" }],
                    "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$skip=1"
                }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({ "error": { "code": "ServiceUnavailable", "message": "busy" } }),
            ),
        ]);
        let account = GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions);

        assert!(
            folder_message_ids(&account, FolderId("inbox".to_string()))
                .await
                .is_err()
        );
    }
}
