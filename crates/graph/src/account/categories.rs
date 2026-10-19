//! Outlook master category definitions read surface.
//!
//! One GET against `{prefix}/outlook/masterCategories`. The projection is
//! deliberately thin: `color` carries the Graph preset token (`preset0`,
//! `preset23`, or the literal `None` some tenants emit) verbatim - the
//! consumer owns preset-to-color resolution, because the preset palette is
//! a UI concern and not a protocol fact. Categories are message flags, not
//! containers, so this is a definitions LIST and never a container
//! projection.

use bifrost_types::{AccountError, AccountOperation, CategoryDefinition};
use serde::Deserialize;

use crate::client::Resource;

use super::GraphAccount;
use super::graph_error::{GraphErrorContext, into_account_error};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OutlookCategory {
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
        .get_json(
            Resource::Outlook,
            &format!("{prefix}/outlook/masterCategories"),
        )
        .await
        .map_err(|e| {
            into_account_error(
                e,
                GraphErrorContext::graph(AccountOperation::CategoryDefinitionsList),
            )
        })?;
    Ok(response.value.into_iter().map(definition).collect())
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
}
