//! Shared Gmail query encoding for search and stored filter criteria.

use std::time::SystemTime;

use bifrost_types::{AccountError, AccountOperation, FilterCondition, Provider, SearchFilter};
use jiff::Timestamp;
use jiff::tz::Offset;

use super::error::{GmailErrorContext, into_account_error};

fn invalid(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    into_account_error(
        crate::Error::invalid_request(op, detail),
        GmailErrorContext::base(op),
    )
}

// Keep the public predicate algebras at the boundary: search has filename and
// label predicates while stored rules also have sizes and arbitrary headers.
// Unifying those public contracts belongs in their owning crate. Here both
// surfaces use one encoder so escaping, grouping and containers cannot drift.
pub(super) fn filter_query(filter: &SearchFilter) -> Result<String, AccountError> {
    let condition = search_condition(filter)?;
    condition_query(&condition, AccountOperation::Search)
}

fn search_condition(filter: &SearchFilter) -> Result<FilterCondition, AccountError> {
    Ok(match filter {
        SearchFilter::From(value) => FilterCondition::From(value.clone()),
        SearchFilter::To(value) => FilterCondition::To(value.clone()),
        SearchFilter::Subject(value) => FilterCondition::Subject(value.clone()),
        SearchFilter::Body(value) => FilterCondition::Body(value.clone()),
        SearchFilter::Has(value) if value.eq_ignore_ascii_case("attachment") => {
            FilterCondition::HasAttachment(true)
        }
        SearchFilter::Has(value) => FilterCondition::ProviderExpression {
            provider: Provider::Gmail,
            expression: format!("filename:{}", query_term(value)),
        },
        SearchFilter::In(container) => FilterCondition::InContainer(container.clone()),
        SearchFilter::Labeled(label) => FilterCondition::ProviderExpression {
            provider: Provider::Gmail,
            expression: format!("label:{}", query_term(&label.0)),
        },
        SearchFilter::DateRange { after, before } => FilterCondition::DateRange {
            after: *after,
            before: *before,
        },
        SearchFilter::And(items) => FilterCondition::And(
            items
                .iter()
                .map(search_condition)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        SearchFilter::Or(items) => FilterCondition::Or(
            items
                .iter()
                .map(search_condition)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        SearchFilter::Not(item) => FilterCondition::Not(Box::new(search_condition(item)?)),
        _ => {
            return Err(into_account_error(
                crate::Error::unsupported(AccountOperation::Search),
                GmailErrorContext::base(AccountOperation::Search),
            ));
        }
    })
}

pub(super) fn condition_query(
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

pub(super) fn sanitize_term(value: &str) -> String {
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

pub(super) fn container_query(id: &str) -> String {
    super::flags::container_search_operator(id)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("label:{}", query_term(id)))
}

pub(super) fn gmail_date(time: SystemTime) -> String {
    let dt = Offset::UTC
        .to_datetime(Timestamp::try_from(time).unwrap_or(Timestamp::UNIX_EPOCH))
        .date();
    format!("{:04}/{:02}/{:02}", dt.year(), dt.month(), dt.day())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::ContainerId;

    #[test]
    fn search_and_rule_queries_share_nested_encoding() {
        let search = SearchFilter::And(vec![
            SearchFilter::From("Ada Example".into()),
            SearchFilter::Or(vec![
                SearchFilter::In(ContainerId("Archive".into())),
                SearchFilter::Not(Box::new(SearchFilter::Has("attachment".into()))),
            ]),
        ]);
        let rule = FilterCondition::And(vec![
            FilterCondition::From("Ada Example".into()),
            FilterCondition::Or(vec![
                FilterCondition::InContainer(ContainerId("Archive".into())),
                FilterCondition::Not(Box::new(FilterCondition::HasAttachment(true))),
            ]),
        ]);
        let encoded = filter_query(&search).unwrap();
        assert_eq!(
            encoded,
            condition_query(&rule, AccountOperation::FilterCreate).unwrap(),
        );
        assert_eq!(
            encoded,
            format!(
                "from:\"Ada Example\" ({} OR -(has:attachment))",
                container_query("Archive"),
            ),
        );
    }
}
