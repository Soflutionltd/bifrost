use bifrost_types::{
    AccountError, AccountOperation, ContainerId, FilterAction, FilterCondition, FilterDiagnostic,
    FilterDiagnosticSeverity, FilterRule, FilterRuleCreate, FilterRulePatch, FilterValidation,
    ProtocolErrorKind, ServerFilter, ServerFilterCreate, ServerFilterId, ServerFilterPatch,
};
use serde_json::{Map, Value, json};

use crate::types::{
    GraphEmailAddress, GraphMessageRule, GraphMessageRuleActions, GraphMessageRulePredicates,
    GraphRecipient, GraphSizeRange,
};

use super::GraphAccount;
use super::graph_error::{
    GraphErrorContext, into_account_error, invalid_account_error, protocol_violation,
    unsupported_account_error,
};

const DEFAULT_RULE_NAME: &str = "Bifrost rule";
const GRAPH_HEADER_SENTINEL: &str = "*";

pub(crate) async fn list(account: GraphAccount) -> Result<Vec<ServerFilter>, AccountError> {
    let rules = account
        .mailboxes
        .primary()
        .list_message_rules()
        .await
        .map_err(|error| {
            into_account_error(
                error,
                GraphErrorContext::graph(AccountOperation::FiltersList),
            )
        })?;
    rules
        .into_iter()
        .map(rule_from_graph)
        .map(|result| result.map(ServerFilter::Rule))
        .collect()
}

pub(crate) async fn create(
    account: GraphAccount,
    filter: ServerFilterCreate,
) -> Result<ServerFilterId, AccountError> {
    let ServerFilterCreate::Rule(rule) = filter else {
        return Err(unsupported_account_error(AccountOperation::FilterCreate));
    };
    let request = message_rule_from_create(rule, AccountOperation::FilterCreate)?;
    let created = account
        .mailboxes
        .primary()
        .create_message_rule(&request)
        .await
        .map_err(|error| {
            into_account_error(
                error,
                GraphErrorContext::graph(AccountOperation::FilterCreate),
            )
        })?;
    let id = created.id.ok_or_else(|| {
        protocol_violation(
            ProtocolErrorKind::ContractViolation,
            AccountOperation::FilterCreate,
            None,
            "created Graph message rule did not include an id",
        )
    })?;
    Ok(ServerFilterId(id))
}

pub(crate) async fn update(
    account: GraphAccount,
    filter: ServerFilterId,
    patch: ServerFilterPatch,
) -> Result<(), AccountError> {
    let ServerFilterPatch::Rule(patch) = patch else {
        return Err(unsupported_account_error(AccountOperation::FilterUpdate));
    };
    let body = message_rule_patch(patch, AccountOperation::FilterUpdate)?;
    if body.as_object().is_some_and(Map::is_empty) {
        return Ok(());
    }
    account
        .mailboxes
        .primary()
        .update_message_rule(&filter.0, &body)
        .await
        .map_err(|error| {
            into_account_error(
                error,
                GraphErrorContext::graph(AccountOperation::FilterUpdate),
            )
        })?;
    Ok(())
}

pub(crate) async fn delete(
    account: GraphAccount,
    filter: ServerFilterId,
) -> Result<(), AccountError> {
    account
        .mailboxes
        .primary()
        .delete_message_rule(&filter.0)
        .await
        .map_err(|error| {
            into_account_error(
                error,
                GraphErrorContext::graph(AccountOperation::FilterDelete),
            )
        })
}

pub(crate) fn validate(filter: ServerFilterCreate) -> Result<FilterValidation, AccountError> {
    let ServerFilterCreate::Rule(rule) = filter else {
        return Ok(validation_error(
            "Graph inbox rules only support typed rules",
        ));
    };
    match message_rule_from_create(rule, AccountOperation::FilterValidate) {
        Ok(_) => Ok(FilterValidation::default()),
        Err(error) => Ok(validation_error(error.to_string())),
    }
}

fn validation_error(message: impl Into<String>) -> FilterValidation {
    FilterValidation {
        diagnostics: vec![FilterDiagnostic {
            severity: FilterDiagnosticSeverity::Error,
            message: message.into(),
            line: None,
            column: None,
        }],
    }
}

fn invalid(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    invalid_account_error(op, detail)
}

fn rule_from_graph(rule: GraphMessageRule) -> Result<FilterRule, AccountError> {
    let id = rule.id.ok_or_else(|| {
        protocol_violation(
            ProtocolErrorKind::ContractViolation,
            AccountOperation::FiltersList,
            None,
            "listed Graph message rule did not include an id",
        )
    })?;
    let condition = condition_from_predicates(rule.conditions, rule.exceptions);
    let mut actions = actions_from_graph(&rule.actions);
    let stop_processing = rule.actions.stop_processing_rules.unwrap_or(false);
    if actions.is_empty() {
        actions.push(FilterAction::Keep);
    }
    Ok(FilterRule {
        id: ServerFilterId(id),
        name: rule.display_name,
        is_enabled: rule.is_enabled.unwrap_or(true),
        condition,
        actions,
        stop_processing,
    })
}

fn condition_from_predicates(
    conditions: GraphMessageRulePredicates,
    exceptions: GraphMessageRulePredicates,
) -> FilterCondition {
    let mut items = predicate_conditions(conditions);
    let exception_items = predicate_conditions(exceptions);
    if !exception_items.is_empty() {
        let exception = match exception_items.len() {
            1 => exception_items.into_iter().next().expect("one exception"),
            _ => FilterCondition::And(exception_items),
        };
        items.push(FilterCondition::Not(Box::new(exception)));
    }
    match items.len() {
        0 => FilterCondition::And(Vec::new()),
        1 => items.remove(0),
        _ => FilterCondition::And(items),
    }
}

fn predicate_conditions(predicates: GraphMessageRulePredicates) -> Vec<FilterCondition> {
    let mut conditions = Vec::new();
    for value in predicates.body_contains {
        conditions.push(FilterCondition::Body(value));
    }
    for value in predicates.categories {
        conditions.push(FilterCondition::InContainer(ContainerId(value)));
    }
    for recipient in predicates.from_addresses {
        conditions.push(FilterCondition::From(recipient.email_address.address));
    }
    if let Some(value) = predicates.has_attachments {
        conditions.push(FilterCondition::HasAttachment(value));
    }
    for value in predicates.header_contains {
        conditions.push(FilterCondition::HeaderContains {
            name: GRAPH_HEADER_SENTINEL.to_string(),
            value,
        });
    }
    for value in predicates.recipient_contains {
        conditions.push(FilterCondition::Recipient(value));
    }
    for value in predicates.sender_contains {
        conditions.push(FilterCondition::From(value));
    }
    for recipient in predicates.sent_to_addresses {
        conditions.push(FilterCondition::To(recipient.email_address.address));
    }
    for value in predicates.subject_contains {
        conditions.push(FilterCondition::Subject(value));
    }
    if let Some(range) = predicates.within_size_range {
        if let Some(minimum) = range.minimum_size {
            conditions.push(FilterCondition::SizeGreaterThan(
                minimum.saturating_mul(1024),
            ));
        }
        if let Some(maximum) = range.maximum_size {
            conditions.push(FilterCondition::SizeLessThan(maximum.saturating_mul(1024)));
        }
    }
    conditions
}

fn actions_from_graph(actions: &GraphMessageRuleActions) -> Vec<FilterAction> {
    let mut out = Vec::new();
    for category in &actions.assign_categories {
        out.push(FilterAction::AddLabel(ContainerId(category.clone())));
    }
    if actions.delete.unwrap_or(false) {
        out.push(FilterAction::Delete);
    }
    if actions.permanent_delete.unwrap_or(false) {
        out.push(FilterAction::Discard);
    }
    if let Some(folder) = &actions.move_to_folder {
        out.push(FilterAction::MoveTo(ContainerId(folder.clone())));
    }
    if actions.mark_as_read.unwrap_or(false) {
        out.push(FilterAction::MarkRead);
    }
    for recipient in &actions.forward_to {
        out.push(FilterAction::ForwardTo(
            recipient.email_address.address.clone(),
        ));
    }
    for recipient in &actions.redirect_to {
        out.push(FilterAction::RedirectTo(
            recipient.email_address.address.clone(),
        ));
    }
    out
}

fn message_rule_from_create(
    rule: FilterRuleCreate,
    op: AccountOperation,
) -> Result<GraphMessageRule, AccountError> {
    let (conditions, exceptions) = predicates_from_condition(&rule.condition, op)?;
    let mut actions = actions_from_filter_actions(&rule.actions, op)?;
    actions.stop_processing_rules = Some(rule.stop_processing);
    Ok(GraphMessageRule {
        id: None,
        display_name: Some(rule.name.unwrap_or_else(|| DEFAULT_RULE_NAME.to_string())),
        is_enabled: Some(rule.is_enabled),
        conditions,
        exceptions,
        actions,
    })
}

fn message_rule_patch(patch: FilterRulePatch, op: AccountOperation) -> Result<Value, AccountError> {
    let mut body = Map::new();
    if let Some(name) = patch.name {
        let Some(name) = name else {
            return Err(invalid(op, "Graph message rules require a display name"));
        };
        body.insert("displayName".to_string(), json!(name));
    }
    if let Some(is_enabled) = patch.is_enabled {
        body.insert("isEnabled".to_string(), json!(is_enabled));
    }
    if let Some(condition) = patch.condition {
        let (conditions, exceptions) = predicates_from_condition(&condition, op)?;
        body.insert("conditions".to_string(), json_value(&conditions, op)?);
        body.insert("exceptions".to_string(), json_value(&exceptions, op)?);
    }
    let mut action_patch = match patch.actions {
        Some(actions) => Some(actions_from_filter_actions(&actions, op)?),
        None => None,
    };
    if let Some(stop_processing) = patch.stop_processing {
        action_patch
            .get_or_insert_with(GraphMessageRuleActions::default)
            .stop_processing_rules = Some(stop_processing);
    }
    if let Some(actions) = action_patch {
        body.insert("actions".to_string(), json_value(&actions, op)?);
    }
    Ok(Value::Object(body))
}

fn json_value<T: serde::Serialize>(value: &T, op: AccountOperation) -> Result<Value, AccountError> {
    serde_json::to_value(value)
        .map_err(|error| invalid(op, format!("failed to encode Graph message rule: {error}")))
}

fn predicates_from_condition(
    condition: &FilterCondition,
    op: AccountOperation,
) -> Result<(GraphMessageRulePredicates, GraphMessageRulePredicates), AccountError> {
    let mut conditions = GraphMessageRulePredicates::default();
    let mut exceptions = GraphMessageRulePredicates::default();
    apply_condition(&mut conditions, &mut exceptions, condition, false, op)?;
    Ok((conditions, exceptions))
}

fn apply_condition(
    conditions: &mut GraphMessageRulePredicates,
    exceptions: &mut GraphMessageRulePredicates,
    condition: &FilterCondition,
    inverted: bool,
    op: AccountOperation,
) -> Result<(), AccountError> {
    match condition {
        FilterCondition::From(value) => {
            let target = predicate_target(conditions, exceptions, inverted);
            target.from_addresses.push(recipient(value));
        }
        FilterCondition::To(value) => {
            let target = predicate_target(conditions, exceptions, inverted);
            target.sent_to_addresses.push(recipient(value));
        }
        FilterCondition::Recipient(value) => {
            let target = predicate_target(conditions, exceptions, inverted);
            target.recipient_contains.push(value.clone());
        }
        FilterCondition::Cc(_) => {
            // Graph `messageRulePredicates` has no Cc-address predicate;
            // only `recipientContains` (any To/Cc recipient). Folding Cc
            // into `recipientContains` silently widens a Cc-specific rule
            // to match any recipient, so reject rather than mis-translate.
            return Err(invalid(
                op,
                "Graph message rules cannot match the Cc field specifically",
            ));
        }
        FilterCondition::Subject(value) => {
            let target = predicate_target(conditions, exceptions, inverted);
            target.subject_contains.push(value.clone());
        }
        FilterCondition::Body(value) => {
            let target = predicate_target(conditions, exceptions, inverted);
            target.body_contains.push(value.clone());
        }
        FilterCondition::HeaderContains { name, value } => {
            let target = predicate_target(conditions, exceptions, inverted);
            if name == GRAPH_HEADER_SENTINEL {
                target.header_contains.push(value.clone());
            } else {
                target.header_contains.push(format!("{name}: {value}"));
            }
        }
        FilterCondition::HasAttachment(value) => {
            let target = predicate_target(conditions, exceptions, inverted);
            set_bool(&mut target.has_attachments, *value, op, "hasAttachments")?;
        }
        FilterCondition::InContainer(container) => {
            let target = predicate_target(conditions, exceptions, inverted);
            target.categories.push(container.0.clone());
        }
        FilterCondition::SizeGreaterThan(size) => {
            let target = predicate_target(conditions, exceptions, inverted);
            let range = target.within_size_range.get_or_insert(GraphSizeRange {
                minimum_size: None,
                maximum_size: None,
            });
            range.minimum_size = Some(size.div_ceil(1024));
        }
        FilterCondition::SizeLessThan(size) => {
            let target = predicate_target(conditions, exceptions, inverted);
            let range = target.within_size_range.get_or_insert(GraphSizeRange {
                minimum_size: None,
                maximum_size: None,
            });
            range.maximum_size = Some(size / 1024);
        }
        FilterCondition::And(items) => {
            for item in items {
                apply_condition(conditions, exceptions, item, inverted, op)?;
            }
        }
        FilterCondition::Not(item) => {
            apply_condition(conditions, exceptions, item, !inverted, op)?;
        }
        FilterCondition::Or(_)
        | FilterCondition::DateRange { .. }
        | FilterCondition::ProviderExpression { .. } => {
            return Err(invalid(op, "unsupported Graph message-rule condition"));
        }
        _ => return Err(invalid(op, "unsupported Graph message-rule condition")),
    }
    Ok(())
}

fn predicate_target<'a>(
    conditions: &'a mut GraphMessageRulePredicates,
    exceptions: &'a mut GraphMessageRulePredicates,
    inverted: bool,
) -> &'a mut GraphMessageRulePredicates {
    if inverted { exceptions } else { conditions }
}

fn set_bool(
    slot: &mut Option<bool>,
    value: bool,
    op: AccountOperation,
    field: &str,
) -> Result<(), AccountError> {
    if slot.is_some_and(|existing| existing != value) {
        return Err(invalid(
            op,
            format!("conflicting Graph message-rule predicate {field}"),
        ));
    }
    *slot = Some(value);
    Ok(())
}

fn actions_from_filter_actions(
    actions: &[FilterAction],
    op: AccountOperation,
) -> Result<GraphMessageRuleActions, AccountError> {
    let mut graph = GraphMessageRuleActions::default();
    for action in actions {
        match action {
            FilterAction::Keep => {}
            FilterAction::Delete => graph.delete = Some(true),
            FilterAction::Discard => graph.permanent_delete = Some(true),
            FilterAction::MoveTo(container) => graph.move_to_folder = Some(container.0.clone()),
            FilterAction::AddLabel(container) => {
                push_unique(&mut graph.assign_categories, &container.0);
            }
            FilterAction::MarkRead => graph.mark_as_read = Some(true),
            FilterAction::ForwardTo(address) => graph.forward_to.push(recipient(address)),
            FilterAction::RedirectTo(address) => graph.redirect_to.push(recipient(address)),
            FilterAction::RemoveLabel(_)
            | FilterAction::MarkUnread
            | FilterAction::Star
            | FilterAction::Unstar
            | FilterAction::SetKeyword(_)
            | FilterAction::ClearKeyword(_)
            | FilterAction::Reject { .. } => {
                return Err(invalid(op, "unsupported Graph message-rule action"));
            }
            _ => return Err(invalid(op, "unsupported Graph message-rule action")),
        }
    }
    if graph.is_empty() {
        return Err(invalid(
            op,
            "Graph message rules require at least one action",
        ));
    }
    Ok(graph)
}

fn recipient(address: &str) -> GraphRecipient {
    GraphRecipient {
        email_address: GraphEmailAddress {
            name: None,
            address: address.to_string(),
        },
    }
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_rule_list_maps_exceptions_to_not_condition() {
        let rule = rule_from_graph(GraphMessageRule {
            id: Some("r1".to_string()),
            display_name: Some("Rule".to_string()),
            is_enabled: Some(true),
            conditions: GraphMessageRulePredicates {
                subject_contains: vec!["invoice".to_string()],
                ..GraphMessageRulePredicates::default()
            },
            exceptions: GraphMessageRulePredicates {
                sender_contains: vec!["boss".to_string()],
                ..GraphMessageRulePredicates::default()
            },
            actions: GraphMessageRuleActions {
                delete: Some(true),
                stop_processing_rules: Some(true),
                ..GraphMessageRuleActions::default()
            },
        })
        .expect("graph rule maps");

        assert_eq!(rule.id.0, "r1");
        assert!(rule.stop_processing);
        assert!(matches!(rule.actions.as_slice(), [FilterAction::Delete]));
        assert!(matches!(rule.condition, FilterCondition::And(_)));
    }

    #[test]
    fn create_mapping_builds_conditions_exceptions_and_actions() {
        let rule = FilterRuleCreate {
            name: Some("Move".to_string()),
            is_enabled: true,
            condition: FilterCondition::And(vec![
                FilterCondition::From("ada@example.com".to_string()),
                FilterCondition::Not(Box::new(FilterCondition::Subject("spam".to_string()))),
            ]),
            actions: vec![
                FilterAction::MoveTo(ContainerId("archive".to_string())),
                FilterAction::MarkRead,
            ],
            stop_processing: true,
        };
        let graph =
            message_rule_from_create(rule, AccountOperation::FilterCreate).expect("rule maps");

        assert_eq!(graph.display_name.as_deref(), Some("Move"));
        assert_eq!(graph.conditions.from_addresses.len(), 1);
        assert_eq!(graph.exceptions.subject_contains, vec!["spam".to_string()]);
        assert_eq!(graph.actions.move_to_folder.as_deref(), Some("archive"));
        assert_eq!(graph.actions.mark_as_read, Some(true));
        assert_eq!(graph.actions.stop_processing_rules, Some(true));
    }

    #[test]
    fn recipient_maps_to_recipient_contains() {
        let (conditions, _) = predicates_from_condition(
            &FilterCondition::Recipient("team@example.com".to_string()),
            AccountOperation::FilterCreate,
        )
        .expect("recipient maps");
        assert_eq!(
            conditions.recipient_contains,
            vec!["team@example.com".to_string()]
        );
    }

    #[test]
    fn cc_condition_is_rejected_not_widened() {
        // Graph has no Cc-specific predicate; folding Cc into
        // recipientContains would silently widen the rule to any
        // recipient, so it must be rejected.
        let err = predicates_from_condition(
            &FilterCondition::Cc("team@example.com".to_string()),
            AccountOperation::FilterCreate,
        )
        .expect_err("cc rejected");
        assert_eq!(err.operation(), Some(AccountOperation::FilterCreate));
    }

    #[test]
    fn condition_patch_sends_empty_exceptions_to_clear_existing_rule() {
        let body = message_rule_patch(
            FilterRulePatch {
                condition: Some(FilterCondition::Subject("invoice".to_string())),
                ..FilterRulePatch::default()
            },
            AccountOperation::FilterUpdate,
        )
        .expect("patch maps");

        assert_eq!(
            body.get("conditions")
                .and_then(|value| value.get("subjectContains")),
            Some(&json!(["invoice"]))
        );
        assert_eq!(body.get("exceptions"), Some(&json!({})));
    }
}
