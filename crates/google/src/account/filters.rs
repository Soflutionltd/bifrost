//! Gmail settings filters: list, create, delete, and typed-rule mapping.
//!
//! Audit boundary: the 2026-07 google bug sweep did NOT line-audit this
//! module; it was skipped as already test-dense. A later auditor should read
//! it fresh rather than assuming that sweep covered it.

use std::sync::Arc;
use std::time::SystemTime;

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, ContainerId, FilterAction, FilterCondition,
    FilterDiagnostic, FilterDiagnosticSeverity, FilterRule, FilterRuleCreate, FilterValidation,
    Provider, ServerFilter, ServerFilterCreate, ServerFilterId,
};
use jiff::Timestamp;
use jiff::tz::Offset;

use crate::client::GmailClient;
use crate::types::{
    GmailFilter, GmailFilterAction, GmailFilterCriteria, GmailFilterSizeComparison,
};

use super::error::{GmailErrorContext, into_account_error};
use super::flags;
use super::flags::{LABEL_STARRED, LABEL_TRASH, LABEL_UNREAD};

pub(crate) fn list(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
    Box::pin(async move {
        let filters = client
            .list_filters()
            .await
            .map_err(to_acct_err(AccountOperation::FiltersList))?;
        filters
            .into_iter()
            .map(rule_from_gmail)
            .map(|result| result.map(ServerFilter::Rule))
            .collect()
    })
}

pub(crate) fn create(
    client: Arc<GmailClient>,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<ServerFilterId, AccountError>> {
    Box::pin(async move {
        let ServerFilterCreate::Rule(rule) = filter else {
            return Err(unsupported(
                AccountOperation::FilterCreate,
                "Gmail filters only support typed rules",
            ));
        };
        let request = gmail_filter_from_rule(rule, AccountOperation::FilterCreate)?;
        let created = client
            .create_filter(&request)
            .await
            .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        let id = created.id.ok_or_else(|| {
            into_account_error(
                crate::error::Error::missing_field("id", "created Gmail filter has no id"),
                GmailErrorContext::base(AccountOperation::FilterCreate),
            )
        })?;
        Ok(ServerFilterId(id))
    })
}

pub(crate) fn delete(
    client: Arc<GmailClient>,
    filter: ServerFilterId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        client
            .delete_filter(&filter.0)
            .await
            .map_err(to_acct_err(AccountOperation::FilterDelete))
    })
}

pub(crate) fn validate(
    filter: ServerFilterCreate,
) -> AccountFuture<Result<FilterValidation, AccountError>> {
    Box::pin(async move {
        let ServerFilterCreate::Rule(rule) = filter else {
            return Ok(validation_error("Gmail filters only support typed rules"));
        };
        match gmail_filter_from_rule(rule, AccountOperation::FilterValidate) {
            Ok(_) => Ok(FilterValidation::default()),
            Err(error) => Ok(validation_error(error.to_string())),
        }
    })
}

fn to_acct_err(op: AccountOperation) -> impl Fn(crate::error::Error) -> AccountError {
    move |err| into_account_error(err, GmailErrorContext::base(op))
}

fn unsupported(op: AccountOperation, detail: &'static str) -> AccountError {
    into_account_error(
        crate::error::Error::unsupported_with(op, detail),
        GmailErrorContext::base(op),
    )
}

fn invalid(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    into_account_error(
        crate::error::Error::invalid_request(op, detail),
        GmailErrorContext::base(op),
    )
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

fn rule_from_gmail(filter: GmailFilter) -> Result<FilterRule, AccountError> {
    let id = filter.id.ok_or_else(|| {
        into_account_error(
            crate::error::Error::missing_field("id", "listed Gmail filter has no id"),
            GmailErrorContext::base(AccountOperation::FiltersList),
        )
    })?;
    Ok(FilterRule {
        id: ServerFilterId(id),
        name: None,
        is_enabled: true,
        condition: condition_from_criteria(filter.criteria),
        actions: actions_from_gmail(filter.action),
        stop_processing: false,
    })
}

fn condition_from_criteria(criteria: GmailFilterCriteria) -> FilterCondition {
    let mut conditions = Vec::new();
    if let Some(value) = criteria.from {
        conditions.push(FilterCondition::From(value));
    }
    if let Some(value) = criteria.to {
        conditions.push(FilterCondition::Recipient(value));
    }
    if let Some(value) = criteria.subject {
        conditions.push(FilterCondition::Subject(value));
    }
    if let Some(value) = criteria.query {
        conditions.push(FilterCondition::ProviderExpression {
            provider: Provider::Gmail,
            expression: value,
        });
    }
    if let Some(value) = criteria.negated_query {
        conditions.push(FilterCondition::Not(Box::new(
            FilterCondition::ProviderExpression {
                provider: Provider::Gmail,
                expression: value,
            },
        )));
    }
    if let Some(value) = criteria.has_attachment {
        conditions.push(FilterCondition::HasAttachment(value));
    }
    if let (Some(size), Some(comparison)) = (criteria.size, criteria.size_comparison) {
        match comparison {
            GmailFilterSizeComparison::Larger => {
                conditions.push(FilterCondition::SizeGreaterThan(size));
            }
            GmailFilterSizeComparison::Smaller => {
                conditions.push(FilterCondition::SizeLessThan(size));
            }
        }
    }
    match conditions.len() {
        0 => FilterCondition::And(Vec::new()),
        1 => conditions.remove(0),
        _ => FilterCondition::And(conditions),
    }
}

fn actions_from_gmail(action: GmailFilterAction) -> Vec<FilterAction> {
    let mut actions = Vec::new();
    for id in action.add_label_ids {
        actions.push(FilterAction::AddLabel(ContainerId(id)));
    }
    for id in action.remove_label_ids {
        actions.push(FilterAction::RemoveLabel(ContainerId(id)));
    }
    if let Some(address) = action.forward {
        actions.push(FilterAction::RedirectTo(address));
    }
    if actions.is_empty() {
        actions.push(FilterAction::Keep);
    }
    actions
}

fn gmail_filter_from_rule(
    rule: FilterRuleCreate,
    op: AccountOperation,
) -> Result<GmailFilter, AccountError> {
    if rule.name.is_some() {
        return Err(invalid(op, "Gmail filters do not store rule names"));
    }
    if !rule.is_enabled {
        return Err(invalid(op, "Gmail filters cannot be created disabled"));
    }
    if rule.stop_processing {
        return Err(invalid(op, "Gmail filters do not support stop-processing"));
    }

    let criteria = criteria_from_condition(&rule.condition, op)?;
    let action = action_from_filter_actions(&rule.actions, op)?;
    if action.add_label_ids.is_empty()
        && action.remove_label_ids.is_empty()
        && action.forward.is_none()
    {
        return Err(invalid(op, "Gmail filters require at least one action"));
    }
    Ok(GmailFilter {
        id: None,
        criteria,
        action,
    })
}

#[derive(Default)]
struct CriteriaBuilder {
    criteria: GmailFilterCriteria,
    query: Vec<String>,
    negated_query: Vec<String>,
}

fn criteria_from_condition(
    condition: &FilterCondition,
    op: AccountOperation,
) -> Result<GmailFilterCriteria, AccountError> {
    let mut builder = CriteriaBuilder::default();
    apply_condition(&mut builder, condition, op)?;
    if !builder.query.is_empty() {
        builder.criteria.query = Some(builder.query.join(" "));
    }
    if !builder.negated_query.is_empty() {
        builder.criteria.negated_query = Some(builder.negated_query.join(" "));
    }
    Ok(builder.criteria)
}

fn apply_condition(
    builder: &mut CriteriaBuilder,
    condition: &FilterCondition,
    op: AccountOperation,
) -> Result<(), AccountError> {
    match condition {
        FilterCondition::From(value) => set_or_query(
            &mut builder.criteria.from,
            sanitize_term(value),
            format!("from:{}", query_term(value)),
            &mut builder.query,
        ),
        FilterCondition::To(value)
        | FilterCondition::Cc(value)
        | FilterCondition::Recipient(value) => {
            set_or_query(
                &mut builder.criteria.to,
                sanitize_term(value),
                format!("to:{}", query_term(value)),
                &mut builder.query,
            );
        }
        FilterCondition::Subject(value) => set_or_query(
            &mut builder.criteria.subject,
            sanitize_term(value),
            format!("subject:{}", query_term(value)),
            &mut builder.query,
        ),
        FilterCondition::Body(value) => builder.query.push(query_term(value)),
        FilterCondition::HasAttachment(value) => {
            if builder.criteria.has_attachment.is_none() {
                builder.criteria.has_attachment = Some(*value);
            } else {
                builder.query.push(if *value {
                    "has:attachment".to_string()
                } else {
                    "-has:attachment".to_string()
                });
            }
        }
        FilterCondition::InContainer(container) => {
            builder.query.push(container_query(&container.0));
        }
        FilterCondition::SizeGreaterThan(size) => set_size_or_query(
            &mut builder.criteria,
            *size,
            GmailFilterSizeComparison::Larger,
            format!("larger:{size}"),
            &mut builder.query,
        ),
        FilterCondition::SizeLessThan(size) => set_size_or_query(
            &mut builder.criteria,
            *size,
            GmailFilterSizeComparison::Smaller,
            format!("smaller:{size}"),
            &mut builder.query,
        ),
        FilterCondition::DateRange { after, before } => {
            if let Some(after) = after {
                builder.query.push(format!("after:{}", gmail_date(*after)));
            }
            if let Some(before) = before {
                builder
                    .query
                    .push(format!("before:{}", gmail_date(*before)));
            }
        }
        FilterCondition::ProviderExpression {
            provider,
            expression,
        } => {
            if *provider != Provider::Gmail {
                return Err(invalid(
                    op,
                    "Gmail filters cannot store another provider's expression",
                ));
            }
            builder.query.push(sanitize_term(expression));
        }
        FilterCondition::And(items) => {
            for item in items {
                apply_condition(builder, item, op)?;
            }
        }
        FilterCondition::Or(items) => {
            let parts = items
                .iter()
                .map(|item| condition_query(item, op))
                .collect::<Result<Vec<_>, _>>()?;
            builder.query.push(format!("({})", parts.join(" OR ")));
        }
        FilterCondition::Not(item) => {
            builder.negated_query.push(condition_query(item, op)?);
        }
        FilterCondition::HeaderContains { .. } => {
            return Err(invalid(
                op,
                "Gmail filters do not support arbitrary header conditions",
            ));
        }
        _ => return Err(invalid(op, "unsupported Gmail filter condition")),
    }
    Ok(())
}

fn condition_query(
    condition: &FilterCondition,
    op: AccountOperation,
) -> Result<String, AccountError> {
    match condition {
        FilterCondition::From(value) => Ok(format!("from:{}", query_term(value))),
        FilterCondition::To(value)
        | FilterCondition::Cc(value)
        | FilterCondition::Recipient(value) => Ok(format!("to:{}", query_term(value))),
        FilterCondition::Subject(value) => Ok(format!("subject:{}", query_term(value))),
        FilterCondition::Body(value) => Ok(query_term(value)),
        FilterCondition::HasAttachment(true) => Ok("has:attachment".to_string()),
        FilterCondition::HasAttachment(false) => Ok("-has:attachment".to_string()),
        FilterCondition::InContainer(container) => Ok(container_query(&container.0)),
        FilterCondition::SizeGreaterThan(size) => Ok(format!("larger:{size}")),
        FilterCondition::SizeLessThan(size) => Ok(format!("smaller:{size}")),
        FilterCondition::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!("after:{}", gmail_date(*after)));
            }
            if let Some(before) = before {
                parts.push(format!("before:{}", gmail_date(*before)));
            }
            Ok(parts.join(" "))
        }
        FilterCondition::ProviderExpression {
            provider,
            expression,
        } => {
            if *provider == Provider::Gmail {
                Ok(sanitize_term(expression))
            } else {
                Err(invalid(
                    op,
                    "Gmail filters cannot store another provider's expression",
                ))
            }
        }
        FilterCondition::And(items) => items
            .iter()
            .map(|item| condition_query(item, op))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| items.join(" ")),
        FilterCondition::Or(items) => items
            .iter()
            .map(|item| condition_query(item, op))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| format!("({})", items.join(" OR "))),
        FilterCondition::Not(item) => Ok(format!("-({})", condition_query(item, op)?)),
        FilterCondition::HeaderContains { .. } => Err(invalid(
            op,
            "Gmail filters do not support arbitrary header conditions",
        )),
        _ => Err(invalid(op, "unsupported Gmail filter condition")),
    }
}

fn set_or_query(
    slot: &mut Option<String>,
    value: String,
    query: String,
    queries: &mut Vec<String>,
) {
    if slot.is_none() {
        *slot = Some(value);
    } else {
        queries.push(query);
    }
}

fn set_size_or_query(
    criteria: &mut GmailFilterCriteria,
    size: u64,
    comparison: GmailFilterSizeComparison,
    query: String,
    queries: &mut Vec<String>,
) {
    if criteria.size.is_none() {
        criteria.size = Some(size);
        criteria.size_comparison = Some(comparison);
    } else {
        queries.push(query);
    }
}

fn action_from_filter_actions(
    actions: &[FilterAction],
    op: AccountOperation,
) -> Result<GmailFilterAction, AccountError> {
    let mut gmail = GmailFilterAction::default();
    for action in actions {
        match action {
            FilterAction::Keep => {}
            FilterAction::Delete | FilterAction::MoveTo(_) => {
                let destination = match action {
                    FilterAction::MoveTo(container) => container.0.as_str(),
                    _ => LABEL_TRASH,
                };
                let patch = flags::move_placement_patch(destination, None);
                for id in patch.add_label_ids {
                    push_unique(&mut gmail.add_label_ids, &id);
                }
                for id in patch.remove_label_ids {
                    push_unique(&mut gmail.remove_label_ids, &id);
                }
            }
            FilterAction::AddLabel(container) => {
                push_unique(&mut gmail.add_label_ids, &container.0);
            }
            FilterAction::RemoveLabel(container) => {
                push_unique(&mut gmail.remove_label_ids, &container.0);
            }
            FilterAction::MarkRead => push_unique(&mut gmail.remove_label_ids, LABEL_UNREAD),
            FilterAction::MarkUnread => push_unique(&mut gmail.add_label_ids, LABEL_UNREAD),
            FilterAction::Star => push_unique(&mut gmail.add_label_ids, LABEL_STARRED),
            FilterAction::Unstar => push_unique(&mut gmail.remove_label_ids, LABEL_STARRED),
            FilterAction::ForwardTo(address) | FilterAction::RedirectTo(address) => {
                if gmail
                    .forward
                    .as_deref()
                    .is_some_and(|existing| existing != address)
                {
                    return Err(invalid(
                        op,
                        "Gmail filters support only one forwarding address",
                    ));
                }
                gmail.forward = Some(address.clone());
            }
            FilterAction::Discard
            | FilterAction::SetKeyword(_)
            | FilterAction::ClearKeyword(_)
            | FilterAction::Reject { .. } => {
                return Err(invalid(op, "unsupported Gmail filter action"));
            }
            _ => return Err(invalid(op, "unsupported Gmail filter action")),
        }
    }
    Ok(gmail)
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}

fn sanitize_term(value: &str) -> String {
    value
        .chars()
        .map(|ch| if matches!(ch, '\r' | '\n') { ' ' } else { ch })
        .collect::<String>()
        .trim()
        .to_string()
}

fn query_term(value: &str) -> String {
    let sanitized = sanitize_term(value);
    if sanitized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '@' | '/' | ':'))
    {
        sanitized
    } else {
        format!(
            "\"{}\"",
            sanitized.replace('\\', "\\\\").replace('"', "\\\"")
        )
    }
}

fn container_query(id: &str) -> String {
    super::flags::container_search_operator(id)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("label:{}", query_term(id)))
}

fn gmail_date(time: SystemTime) -> String {
    let dt = Offset::UTC
        .to_datetime(Timestamp::try_from(time).unwrap_or(Timestamp::UNIX_EPOCH))
        .date();
    format!("{:04}/{:02}/{:02}", dt.year(), dt.month(), dt.day())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_queries_share_case_insensitive_system_vocabulary() {
        for id in ["inbox", "sent", "draft", "trash", "spam", "Archive"] {
            assert_eq!(
                container_query(id),
                container_query(&id.to_ascii_uppercase())
            );
        }
        assert!(container_query("Archive").starts_with("-in:inbox"));
        assert_eq!(container_query("Label_Case"), "label:Label_Case");
    }

    #[test]
    fn filter_moves_use_canonical_placement() {
        for (action, add, remove) in [
            (
                FilterAction::MoveTo(ContainerId("archive".into())),
                vec![],
                vec!["INBOX", "SPAM", "TRASH"],
            ),
            (
                FilterAction::MoveTo(ContainerId("INBOX".into())),
                vec!["INBOX"],
                vec!["SPAM", "TRASH"],
            ),
            (
                FilterAction::MoveTo(ContainerId("Label_42".into())),
                vec!["Label_42"],
                vec!["INBOX", "SPAM", "TRASH"],
            ),
            (FilterAction::Delete, vec!["TRASH"], vec!["INBOX", "SPAM"]),
        ] {
            let patch =
                action_from_filter_actions(&[action], AccountOperation::FilterCreate).unwrap();
            assert_eq!(patch.add_label_ids, add);
            assert_eq!(patch.remove_label_ids, remove);
        }
    }

    #[test]
    fn list_mapping_preserves_native_query_as_provider_expression() {
        let rule = rule_from_gmail(GmailFilter {
            id: Some("f1".to_string()),
            criteria: GmailFilterCriteria {
                query: Some("larger:5M".to_string()),
                ..GmailFilterCriteria::default()
            },
            action: GmailFilterAction {
                add_label_ids: vec!["Label_1".to_string()],
                ..GmailFilterAction::default()
            },
        })
        .expect("gmail filter maps");

        assert_eq!(rule.id.0, "f1");
        assert!(matches!(
            rule.condition,
            FilterCondition::ProviderExpression {
                provider: Provider::Gmail,
                ..
            }
        ));
        assert!(matches!(
            rule.actions.as_slice(),
            [FilterAction::AddLabel(ContainerId(id))] if id == "Label_1"
        ));
    }

    #[test]
    fn create_mapping_rejects_unstored_name() {
        let rule = FilterRuleCreate {
            name: Some("named".to_string()),
            is_enabled: true,
            condition: FilterCondition::From("ada@example.com".to_string()),
            actions: vec![FilterAction::MarkRead],
            stop_processing: false,
        };
        assert!(gmail_filter_from_rule(rule, AccountOperation::FilterCreate).is_err());
    }

    #[test]
    fn create_mapping_builds_gmail_criteria_and_labels() {
        let rule = FilterRuleCreate {
            name: None,
            is_enabled: true,
            condition: FilterCondition::And(vec![
                FilterCondition::From("ada@example.com".to_string()),
                FilterCondition::ProviderExpression {
                    provider: Provider::Gmail,
                    expression: "larger:5M".to_string(),
                },
            ]),
            actions: vec![
                FilterAction::MarkRead,
                FilterAction::AddLabel(ContainerId("Label_1".to_string())),
            ],
            stop_processing: false,
        };
        let gmail = gmail_filter_from_rule(rule, AccountOperation::FilterCreate)
            .expect("rule maps to gmail");
        assert_eq!(gmail.criteria.from.as_deref(), Some("ada@example.com"));
        assert_eq!(gmail.criteria.query.as_deref(), Some("larger:5M"));
        assert_eq!(
            gmail.action.remove_label_ids,
            vec![LABEL_UNREAD.to_string()]
        );
        assert_eq!(gmail.action.add_label_ids, vec!["Label_1".to_string()]);
    }
}
