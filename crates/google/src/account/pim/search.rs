//! Gmail search operations.

use super::super::error;
use super::super::query::filter_query;
use super::{account_error_for, non_negative_i64, other_error};
use crate::client::GmailClient;
use bifrost_types::{AccountError, AccountFuture, ObjectId, Page, SearchRequest, ThreadId};
use std::sync::Arc;

const MAX_GMAIL_PAGE_SIZE: u32 = 500;

pub(crate) fn search(
    client: Arc<GmailClient>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        let query = gmail_query(&request)?;
        let page_token = page_token(&request)?;
        let max = request
            .limit
            .unwrap_or(MAX_GMAIL_PAGE_SIZE)
            .min(MAX_GMAIL_PAGE_SIZE);
        // Gmail treats maxResults=0 as "use the default page size", so a
        // zero limit must short-circuit locally to mean "return nothing".
        if max == 0 {
            return Ok(Page {
                truncated: false,
                items: Vec::new(),
                next_cursor: None,
                estimated_total: None,
                failed_ids: Vec::new(),
                skipped_scopes: Vec::new(),
            });
        }
        let (threads, next) = client
            .list_threads(query.as_deref(), Some(max), page_token.as_deref())
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::search()))?;
        Ok(Page {
            truncated: false,
            items: threads
                .into_iter()
                .map(|thread| ThreadId(thread.id))
                .collect(),
            next_cursor: next.map(String::into_bytes),
            estimated_total: None,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

pub(crate) fn search_messages(
    client: Arc<GmailClient>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        let query = gmail_query(&request)?;
        let page_token = page_token(&request)?;
        let max = request
            .limit
            .unwrap_or(MAX_GMAIL_PAGE_SIZE)
            .min(MAX_GMAIL_PAGE_SIZE);
        // Gmail treats maxResults=0 as "use the default page size", so a
        // zero limit must short-circuit locally to mean "return nothing".
        if max == 0 {
            return Ok(Page {
                truncated: false,
                items: Vec::new(),
                next_cursor: None,
                estimated_total: None,
                failed_ids: Vec::new(),
                skipped_scopes: Vec::new(),
            });
        }
        let response = client
            .list_messages(query.as_deref(), Some(max), page_token.as_deref())
            .await
            .map_err(|e| account_error_for(e, error::GmailErrorContext::search_messages()))?;
        Ok(Page {
            truncated: false,
            items: response
                .messages
                .into_iter()
                .map(|message| ObjectId(message.id))
                .collect(),
            next_cursor: response.next_page_token.map(String::into_bytes),
            estimated_total: response.result_size_estimate.and_then(non_negative_i64),
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

fn gmail_query(request: &SearchRequest) -> Result<Option<String>, AccountError> {
    let mut parts = Vec::new();
    if let Some(filter) = &request.filter {
        parts.push(filter_query(filter)?);
    }
    if let Some(provider) = &request.provider_query
        && !provider.trim().is_empty()
    {
        parts.push(provider.trim().to_string());
    }
    Ok((!parts.is_empty()).then(|| parts.join(" ")))
}

fn page_token(request: &SearchRequest) -> Result<Option<String>, AccountError> {
    request
        .page_cursor
        .as_ref()
        .map(|cursor| {
            String::from_utf8(cursor.clone()).map_err(|error| {
                other_error(
                    bifrost_types::AccountOperation::Search,
                    format!("invalid gmail page cursor: {error}"),
                )
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::super::super::query::container_query;
    use super::*;
    use bifrost_types::SearchFilter;
    use std::time::{Duration, UNIX_EPOCH};

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
    fn search_query_combines_structured_and_provider_terms() {
        let mut request = SearchRequest::default();
        request.filter = Some(SearchFilter::From("ada@example.com".to_string()));
        request.provider_query = Some("larger:5M".to_string());
        let query = gmail_query(&request).expect("query").expect("some query");
        assert_eq!(query, "from:ada@example.com larger:5M");
    }

    #[test]
    fn date_query_uses_gmail_epoch_seconds() {
        let time = UNIX_EPOCH + Duration::from_secs(86_400);
        assert_eq!(
            super::super::super::query::gmail_date(
                time,
                super::super::super::query::BoundSide::After,
                bifrost_types::AccountOperation::Search,
            )
            .unwrap(),
            "86400",
        );
    }
}
