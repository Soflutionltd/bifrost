//! Outlook master category definitions.
//!
//! Reads are one GET against `{prefix}/outlook/masterCategories`. Writes
//! address a definition by its display name, because that is what a message
//! carries; the definition id is looked up per write. The projection is
//! deliberately thin: `color` carries the Graph preset token (`preset0`,
//! `preset23`, or the literal `None` some tenants emit) verbatim - the
//! consumer owns preset-to-color resolution, because the preset palette is
//! a UI concern and not a protocol fact. Categories are message flags, not
//! containers, so this is a definitions LIST and never a container
//! projection.

use bifrost_types::{AccountError, AccountOperation, CategoryDefinition};
use serde::Deserialize;
use serde_json::json;

use super::GraphAccount;
use super::graph_error::{GraphErrorContext, into_account_error};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OutlookCategory {
    #[serde(default)]
    id: String,
    display_name: String,
    color: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CategoryListResponse {
    value: Vec<OutlookCategory>,
}

/// Fetch the account's master category definitions.
///
/// Whole-call success or a single `AccountError` scoped to
/// [`AccountOperation::CategoryDefinitionsList`]: it is one GET, there is
/// no partial lane.
pub(crate) async fn category_definitions_list(
    account: GraphAccount,
) -> Result<Vec<CategoryDefinition>, AccountError> {
    let prefix = account.client.api_path_prefix();
    let response: CategoryListResponse = account
        .client
        .get_json(&format!("{prefix}/outlook/masterCategories"))
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::CategoryDefinitionsList),
            )
        })?;
    Ok(response.value.into_iter().map(definition).collect())
}

/// Colour token Graph accepts on a write: a `presetN`, or `none`.
fn write_color(color: Option<&str>) -> &str {
    match color {
        Some(preset) if !preset.eq_ignore_ascii_case("none") => preset,
        _ => "none",
    }
}

/// Outlook compares category names without regard to case.
fn same_name(a: &str, b: &str) -> bool {
    a.to_lowercase() == b.to_lowercase()
}

async fn find_category(
    account: &GraphAccount,
    name: &str,
    operation: AccountOperation,
) -> Result<Option<OutlookCategory>, AccountError> {
    let prefix = account.client.api_path_prefix();
    let response: CategoryListResponse = account
        .client
        .get_json(&format!("{prefix}/outlook/masterCategories"))
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))?;
    Ok(response
        .value
        .into_iter()
        .find(|category| same_name(&category.display_name, name)))
}

/// Create the category, or set its colour when one with that name exists.
pub(crate) async fn category_definition_upsert(
    account: GraphAccount,
    definition: CategoryDefinition,
) -> Result<(), AccountError> {
    let operation = AccountOperation::CategoryDefinitionUpsert;
    let color = write_color(definition.color.as_deref());
    let prefix = account.client.api_path_prefix();
    let result = match find_category(&account, &definition.name, operation).await? {
        Some(existing) => {
            if write_color(existing.color.as_deref()).eq_ignore_ascii_case(color) {
                return Ok(());
            }
            account
                .client
                .patch(
                    &format!(
                        "{prefix}/outlook/masterCategories/{}",
                        bifrost_net::url::encode_path_component(&existing.id)
                    ),
                    &json!({ "color": color }),
                )
                .await
        }
        None => account
            .client
            .post::<OutlookCategory, _>(
                &format!("{prefix}/outlook/masterCategories"),
                &json!({ "displayName": definition.name, "color": color }),
            )
            .await
            .map(drop),
    };
    result.map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))
}

/// Delete the category definition. An absent category is already deleted.
pub(crate) async fn category_definition_delete(
    account: GraphAccount,
    name: String,
) -> Result<(), AccountError> {
    let operation = AccountOperation::CategoryDefinitionDelete;
    let Some(existing) = find_category(&account, &name, operation).await? else {
        return Ok(());
    };
    let prefix = account.client.api_path_prefix();
    account
        .client
        .delete(&format!(
            "{prefix}/outlook/masterCategories/{}",
            bifrost_net::url::encode_path_component(&existing.id)
        ))
        .await
        .map_err(|e| into_account_error(e, GraphErrorContext::graph(operation)))
}

fn definition(category: OutlookCategory) -> CategoryDefinition {
    CategoryDefinition {
        name: category.display_name,
        color: category.color,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_projects_name_and_verbatim_preset_token() {
        let category: OutlookCategory = serde_json::from_value(serde_json::json!({
            "id": "abc-123",
            "displayName": "Urgent",
            "color": "preset0",
        }))
        .expect("category deserializes");
        let def = definition(category);
        assert_eq!(def.name, "Urgent");
        assert_eq!(def.color.as_deref(), Some("preset0"));
    }

    #[test]
    fn missing_color_projects_none_and_literal_none_survives_verbatim() {
        let missing: OutlookCategory =
            serde_json::from_value(serde_json::json!({ "displayName": "Plain" }))
                .expect("category without color deserializes");
        assert_eq!(definition(missing).color, None);

        // Graph emits the literal string "None" for colorless categories;
        // the consumer's preset mapping treats it specially, so it must
        // pass through untouched rather than being coerced to `None`.
        let literal: OutlookCategory = serde_json::from_value(serde_json::json!({
            "displayName": "Colorless",
            "color": "None",
        }))
        .expect("category with literal None color deserializes");
        assert_eq!(definition(literal).color.as_deref(), Some("None"));
    }

    #[test]
    fn category_list_response_reads_the_value_envelope() {
        let response: CategoryListResponse = serde_json::from_value(serde_json::json!({
            "value": [
                { "displayName": "A", "color": "preset1" },
                { "displayName": "B" },
            ]
        }))
        .expect("list envelope deserializes");
        assert_eq!(response.value.len(), 2);
    }

    fn scripted(
        steps: Vec<bifrost_net::test_support::Canned>,
    ) -> (
        GraphAccount,
        std::sync::Arc<bifrost_net::test_support::ScriptedDispatch>,
    ) {
        use bifrost_net::test_support::{ScriptedDispatch, scripted_account};
        use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource, TokenSource};
        use std::sync::Arc;

        let script = ScriptedDispatch::new(steps);
        let token_source: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("token", None));
        let net = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::clone(&token_source),
            RetryPolicy::disabled(),
        );
        let client = crate::client::GraphClient::with_account_net(
            net,
            "https://graph.contoso.test/v1.0",
            token_source,
        );
        let account =
            GraphAccount::new_for_tests(client, crate::account::PushMode::GraphSubscriptions);
        (account, script)
    }

    fn json_response(body: serde_json::Value) -> bifrost_net::test_support::Canned {
        bifrost_net::test_support::Canned::Response {
            status: reqwest::StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: bytes::Bytes::from(body.to_string()),
        }
    }

    fn no_content() -> bifrost_net::test_support::Canned {
        bifrost_net::test_support::Canned::Response {
            status: reqwest::StatusCode::NO_CONTENT,
            headers: reqwest::header::HeaderMap::new(),
            body: bytes::Bytes::new(),
        }
    }

    fn existing_list() -> serde_json::Value {
        json!({ "value": [
            { "id": "id-urgent", "displayName": "Urgent", "color": "preset0" },
            { "id": "id-todo", "displayName": "À traiter", "color": "preset1" },
        ] })
    }

    fn body_json(request: &bifrost_net::test_support::RequestSnapshot) -> serde_json::Value {
        serde_json::from_slice(request.body.as_ref().expect("request body")).expect("json body")
    }

    #[tokio::test]
    async fn upsert_creates_a_missing_category_with_its_color() {
        let (account, script) = scripted(vec![
            json_response(existing_list()),
            json_response(json!({ "id": "id-new", "displayName": "VIP", "color": "preset9" })),
        ]);
        category_definition_upsert(
            account,
            CategoryDefinition {
                name: "VIP".to_string(),
                color: Some("preset9".to_string()),
            },
        )
        .await
        .expect("create succeeds");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method, reqwest::Method::POST);
        assert!(requests[1].url.path().ends_with("/outlook/masterCategories"));
        assert_eq!(
            body_json(&requests[1]),
            json!({ "displayName": "VIP", "color": "preset9" })
        );
    }

    #[tokio::test]
    async fn upsert_recolors_an_existing_category_matched_without_case() {
        let (account, script) = scripted(vec![json_response(existing_list()), no_content()]);
        category_definition_upsert(
            account,
            CategoryDefinition {
                name: "à TRAITER".to_string(),
                color: Some("preset4".to_string()),
            },
        )
        .await
        .expect("recolor succeeds");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method, reqwest::Method::PATCH);
        assert!(requests[1].url.path().ends_with("/outlook/masterCategories/id-todo"));
        assert_eq!(body_json(&requests[1]), json!({ "color": "preset4" }));
    }

    #[tokio::test]
    async fn upsert_with_the_current_color_writes_nothing() {
        let (account, script) = scripted(vec![json_response(existing_list())]);
        category_definition_upsert(
            account,
            CategoryDefinition {
                name: "Urgent".to_string(),
                color: Some("preset0".to_string()),
            },
        )
        .await
        .expect("no-op succeeds");
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test]
    async fn delete_removes_the_definition_by_its_id() {
        let (account, script) = scripted(vec![json_response(existing_list()), no_content()]);
        category_definition_delete(account, "Urgent".to_string())
            .await
            .expect("delete succeeds");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].method, reqwest::Method::DELETE);
        assert!(requests[1].url.path().ends_with("/outlook/masterCategories/id-urgent"));
    }

    #[tokio::test]
    async fn deleting_an_absent_category_only_reads_the_list() {
        let (account, script) = scripted(vec![json_response(existing_list())]);
        category_definition_delete(account, "Gone".to_string())
            .await
            .expect("absent category is already deleted");
        assert_eq!(script.requests().len(), 1);
    }
}
