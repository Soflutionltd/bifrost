//! Message search: the KQL and OData filter builders, the shared
//! mailbox walk, and the versioned opaque search cursor codec.

use crate::account::GraphAccount;
use crate::account::graph_error::{
    GraphErrorContext, into_account_error, invalid_account_error, unsupported_account_error,
};
use crate::client::Resource;
use crate::types::ODataCollection;
use bifrost_types::{
    AccessErrorKind, AccountError, AccountErrorKind, AccountOperation, ErrorScope, LabelId,
    ObjectId, Page, SearchFilter, SearchRequest, SkippedScope, ThreadId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

use super::common::*;

pub(crate) async fn search(
    account: GraphAccount,
    request: SearchRequest,
) -> Result<Page<ThreadId>, AccountError> {
    let page = search_message_rows(&account, request).await?;
    let mut seen = HashSet::new();
    let mut threads = Vec::new();
    for row in page.items {
        let thread = row.thread_id.unwrap_or(ThreadId(row.id.0));
        if seen.insert(thread.0.clone()) {
            threads.push(thread);
        }
    }
    Ok(Page {
        truncated: false,
        items: threads,
        next_cursor: page.next_cursor,
        estimated_total: page.estimated_total,
        failed_ids: page.failed_ids,
        skipped_scopes: page.skipped_scopes,
    })
}

pub(crate) async fn search_messages(
    account: GraphAccount,
    request: SearchRequest,
) -> Result<Page<ObjectId>, AccountError> {
    let page = search_message_rows(&account, request).await?;
    Ok(Page {
        truncated: false,
        items: page.items.into_iter().map(|row| row.id).collect(),
        next_cursor: page.next_cursor,
        estimated_total: page.estimated_total,
        failed_ids: page.failed_ids,
        skipped_scopes: page.skipped_scopes,
    })
}

#[derive(Debug)]
pub(super) struct SearchRow {
    pub(super) id: ObjectId,
    pub(super) thread_id: Option<ThreadId>,
}

pub(super) const SEARCH_CURSOR_PREFIX: &str = "bifrost-graph-search-v1:";

/// Graph's `nextLink` is only meaningful in the mailbox that minted it.
/// Wrap it with that mailbox so one `Page` cursor can walk the primary and
/// every configured shared mailbox in deterministic order.
///
/// The cursor also carries the shared-mailbox set it was minted against.
/// A search cursor is a POSITION IN A WALK, not a position in one result
/// set: `owner` says which mailbox to resume and `next_search_owner` says
/// which mailbox follows it, and both answers are only meaningful relative
/// to the mailbox list that produced them. Resuming a cursor against a
/// different list silently skips a mailbox added before the current
/// position, or hands the remainder of the walk to mailboxes the caller
/// never asked about - a wrong answer that reports success. So the set is
/// pinned in the cursor and a mismatch is rejected, which costs the caller
/// a restart from page one and nothing else (unlike a sync cursor, a
/// search cursor holds no durable state). The version prefix covers the
/// same ground for a cursor whose SHAPE predates this encoding.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct SearchCursor {
    /// Sorted routing keys of every shared mailbox configured when this
    /// cursor was minted, primary excluded (it is always walked first).
    pub(super) mailboxes: Vec<String>,
    pub(super) owner: Option<String>,
    pub(super) next_link: Option<String>,
}

pub(super) async fn search_message_rows(
    account: &GraphAccount,
    request: SearchRequest,
) -> Result<Page<SearchRow>, AccountError> {
    let cursor = decode_search_cursor(account, request.page_cursor.as_deref())?;
    let mut owner = cursor
        .as_ref()
        .and_then(|cursor| cursor.owner.as_deref())
        .map(str::to_string);
    let mut next_link = cursor.and_then(|cursor| cursor.next_link);
    // Shared mailboxes the walk quarantined instead of searching, reported
    // on the returned page. See the skip arm below for why they are not the
    // call's `Err`.
    let mut skipped_scopes: Vec<SkippedScope> = Vec::new();
    let (client, page) = loop {
        let client = account
            .client_for_owner(owner.as_deref())
            .map_err(|error| {
                into_account_error(error, GraphErrorContext::graph(AccountOperation::Search))
            })?;
        let ctx = GraphErrorContext::graph(AccountOperation::Search);
        let url = match next_link.take() {
            // The caller's cursor bytes: admitted onto the owning mailbox
            // client's origin before anything is sent, or refused as the
            // caller's malformed input.
            Some(next_link) => client.admit_target(&next_link).map_err(|refusal| {
                invalid_account_error(
                    AccountOperation::Search,
                    format!("Graph search cursor {refusal}"),
                )
            })?,
            // No `next_link` means "start this mailbox at its first page":
            // either the whole search, or the mailbox after one whose own
            // pagination ran out.
            None => client
                .api_url(&search_url(&client.api_path_prefix(), &request)?)
                .map_err(|error| into_account_error(error, ctx.clone()))?,
        };
        let result: Result<ODataCollection<Value>, _> = client.get(Resource::Outlook, &url).await;
        match result {
            Ok(page) => break (client, page),
            Err(error) => {
                // A shared mailbox this account has lost delegate access to
                // must not end the walk for the mailboxes behind it. The
                // dead mailbox stays configured, so retrying this cursor -
                // or restarting the search from page one - would 403 at the
                // same position forever, leaving the whole search surface
                // dead over one revoked share. Quarantine it the way every
                // other shared-mailbox door does, but report the skip on the
                // page (`Page::skipped_scopes`) instead of raising an engine
                // directive: a search walk has no cursor scope to disable,
                // and skipping SILENTLY would leave "no matches there"
                // indistinguishable from "never searched there". A primary
                // failure (`owner == None`) is a genuine account-level
                // signal, and a non-permission failure is transient enough
                // that retrying this same cursor can succeed; both still
                // fail the call.
                let Some(mailbox) = owner.as_deref() else {
                    return Err(into_account_error(error, ctx));
                };
                let scope = ErrorScope::Mailbox {
                    id: (mailbox.to_string()).into(),
                };
                let error = into_account_error(error, ctx.with_scope(scope.clone()));
                if !matches!(
                    error.kind(),
                    AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
                ) {
                    return Err(error);
                }
                let next_owner = next_search_owner(account, Some(mailbox));
                skipped_scopes.push(SkippedScope { scope, error });
                match next_owner {
                    Some(next_owner) => owner = Some(next_owner),
                    // The dead mailbox was the walk's last: the search is
                    // complete, minus the quarantined scopes it reports.
                    None => {
                        return Ok(Page {
                            truncated: false,
                            items: Vec::new(),
                            next_cursor: None,
                            estimated_total: None,
                            failed_ids: Vec::new(),
                            skipped_scopes,
                        });
                    }
                }
            }
        }
    };
    let mut rows = Vec::new();
    for value in page.value {
        let id = object_id_from_value(&value, AccountOperation::Search, None)?;
        let id = ObjectId(crate::account::foreign::qualify_with_owner(
            owner.as_deref(),
            &id.0,
        ));
        let thread_id = value
            .get("conversationId")
            .and_then(Value::as_str)
            .map(|id| {
                ThreadId(crate::account::foreign::qualify_with_owner(
                    owner.as_deref(),
                    id,
                ))
            });
        rows.push(SearchRow { id, thread_id });
    }
    // Admitted at receipt by the client that fetched the page, before it
    // can be minted into the caller's cursor.
    let next_link = client
        .admit_next(page.next_link.as_ref())
        .map_err(|error| {
            into_account_error(error, GraphErrorContext::graph(AccountOperation::Search))
        })?;
    let next_cursor = match next_link {
        Some(next_link) => Some(encode_search_cursor(
            account,
            owner.as_deref(),
            Some(next_link.as_str().to_string()),
        )),
        None => next_search_owner(account, owner.as_deref())
            .map(|owner| encode_search_cursor(account, Some(&owner), None)),
    };
    Ok(Page {
        truncated: false,
        items: rows,
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes,
    })
}

/// Decode a caller-supplied search page cursor.
///
/// Every rejection here is `Request(Malformed)`, not `Protocol(...)`: these
/// bytes are request INPUT the caller handed back, so corrupt or stale
/// client state must recommend fixing the request (`ClientBug` ->
/// `FixClientRequest`) rather than accusing Graph of a contract violation.
pub(super) fn decode_search_cursor(
    account: &GraphAccount,
    cursor: Option<&[u8]>,
) -> Result<Option<SearchCursor>, AccountError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let cursor = std::str::from_utf8(cursor).map_err(|error| {
        invalid_account_error(
            AccountOperation::Search,
            format!("Graph search cursor is not UTF-8: {error}"),
        )
    })?;
    // No compatibility arm for a bare Graph nextLink: an unrecognized
    // cursor is never fetched as a URL, because "whatever bytes the caller
    // held" is not a request this crate is willing to issue.
    let payload = cursor.strip_prefix(SEARCH_CURSOR_PREFIX).ok_or_else(|| {
        invalid_account_error(
            AccountOperation::Search,
            "Graph search cursor predates the mailbox-aware search encoding",
        )
    })?;
    let cursor: SearchCursor = serde_json::from_str(payload).map_err(|error| {
        invalid_account_error(
            AccountOperation::Search,
            format!("Graph search cursor is malformed: {error}"),
        )
    })?;
    if cursor.mailboxes != search_mailboxes(account) {
        return Err(invalid_account_error(
            AccountOperation::Search,
            "Graph search cursor was minted against a different shared-mailbox set",
        ));
    }
    // Every link this crate mints into a search cursor is an admitted,
    // absolute server link. Anything else is not a cursor it produced; the
    // origin itself is checked against the owning mailbox's client when the
    // walk resumes.
    if cursor
        .next_link
        .as_deref()
        .is_some_and(|link| reqwest::Url::parse(link).is_err())
    {
        return Err(invalid_account_error(
            AccountOperation::Search,
            "Graph search cursor link is not an absolute URL",
        ));
    }
    Ok(Some(cursor))
}

pub(super) fn encode_search_cursor(
    account: &GraphAccount,
    owner: Option<&str>,
    next_link: Option<String>,
) -> Vec<u8> {
    let cursor = SearchCursor {
        mailboxes: search_mailboxes(account),
        owner: owner.map(str::to_string),
        next_link,
    };
    let payload = serde_json::to_string(&cursor).expect("SearchCursor serializes");
    format!("{SEARCH_CURSOR_PREFIX}{payload}").into_bytes()
}

/// The shared mailboxes the search walk visits after the primary, in the
/// order it visits them. Sorted, so the walk order does not ride on
/// `HashMap` iteration order and two runs of the same account agree.
pub(super) fn search_mailboxes(account: &GraphAccount) -> Vec<String> {
    let mut mailboxes: Vec<String> = account.shared_clients.keys().cloned().collect();
    mailboxes.sort_unstable();
    mailboxes
}

pub(super) fn next_search_owner(account: &GraphAccount, owner: Option<&str>) -> Option<String> {
    let mailboxes = search_mailboxes(account);
    match owner {
        None => mailboxes.into_iter().next(),
        Some(owner) => mailboxes
            .into_iter()
            .skip_while(|mailbox| mailbox != owner)
            .nth(1),
    }
}

/// Build the FIRST-page URL for one mailbox. Continuations never come
/// through here: `request.page_cursor` is decoded once, by
/// `decode_search_cursor`, and a decoded cursor either carries the
/// mailbox's own `nextLink` or asks for this mailbox's first page.
pub(super) fn search_url(prefix: &str, request: &SearchRequest) -> Result<String, AccountError> {
    let mut params = vec![
        "$select=id,conversationId".to_string(),
        format!("$top={}", request.limit.unwrap_or(50).clamp(1, 250)),
    ];
    // Graph `/messages` forbids combining `$search` with `$filter` in one
    // request (it answers 400). A structured filter that needs `$search`
    // (any From/To substring leaf, since `$filter` `contains()` on the
    // sender/recipient navigation properties is rejected) therefore forces
    // the *whole* request onto `$search`/KQL: the structured filter is
    // expressed as KQL and AND-combined with any raw `provider_query`.
    // Otherwise the OData `$filter` path stays in force, and a bare
    // `provider_query` (no structured filter) still goes through `$search`.
    let needs_search = request.filter.as_ref().is_some_and(filter_requires_search);
    if needs_search {
        let mut kql_parts = Vec::new();
        if let Some(filter) = &request.filter {
            let kql = kql_filter(filter)?;
            if !kql.is_empty() {
                kql_parts.push(kql);
            }
        }
        if let Some(provider_query) = &request.provider_query {
            kql_parts.push(graph_search_escape(provider_query));
        }
        let search = if kql_parts.len() == 1 {
            kql_parts.remove(0)
        } else {
            kql_parts
                .into_iter()
                .map(|part| format!("({part})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        params.push(format!(
            "$search={}",
            bifrost_net::url::encode_query_value(&format!("\"{search}\""))
        ));
    } else {
        if let Some(filter) = &request.filter {
            let filter = odata_filter(filter)?;
            if !filter.is_empty() {
                params.push(format!(
                    "$filter={}",
                    bifrost_net::url::encode_query_value(&filter)
                ));
            }
        }
        if let Some(provider_query) = &request.provider_query {
            params.push(format!(
                "$search={}",
                bifrost_net::url::encode_query_value(&format!(
                    "\"{}\"",
                    graph_search_escape(provider_query)
                ))
            ));
        }
    }
    Ok(format!("{prefix}/messages?{}", params.join("&")))
}

/// True if any leaf of the filter tree is a `From`/`To` substring match.
/// Graph `$filter` rejects `contains()` on the sender/recipient navigation
/// properties, so the whole request must route through `$search`/KQL when
/// one of these is present anywhere in the boolean composition.
pub(super) fn filter_requires_search(filter: &SearchFilter) -> bool {
    match filter {
        SearchFilter::From(_) | SearchFilter::To(_) => true,
        SearchFilter::And(filters) | SearchFilter::Or(filters) => {
            filters.iter().any(filter_requires_search)
        }
        SearchFilter::Not(inner) => filter_requires_search(inner),
        _ => false,
    }
}

/// Express the whole filter tree as a KQL `$search` string.
///
/// Used only when `filter_requires_search` holds, because Graph cannot mix
/// `$search` with `$filter`. KQL property restrictions cover sender,
/// recipient, subject, body, attachment presence, category, and a send-date
/// range; KQL terms are AND/OR/NOT-composed. The one structured leaf with no
/// KQL equivalent on `/messages?$search` is `In` (folder scoping), which has
/// no KQL property - rather than silently drop it (returning matches from
/// other folders) or emit an invalid mixed request, it is a clean
/// `Request(Malformed)` so the caller learns the combination is unsupported.
pub(super) fn kql_filter(filter: &SearchFilter) -> Result<String, AccountError> {
    match filter {
        SearchFilter::From(value) => Ok(format!("from:{}", kql_quoted(value))),
        // KQL `to:` and `cc:` cover the recipient set; there is no KQL
        // bcc property, matching Graph's search surface.
        SearchFilter::To(value) => Ok(format!("(to:{0} OR cc:{0})", kql_quoted(value))),
        SearchFilter::Subject(value) => Ok(format!("subject:{}", kql_quoted(value))),
        SearchFilter::Body(value) => Ok(format!("body:{}", kql_quoted(value))),
        SearchFilter::Has(value) => {
            if value.is_empty() {
                Ok("hasattachment:true".to_string())
            } else {
                Err(unsupported_account_error(AccountOperation::Search))
            }
        }
        SearchFilter::Labeled(label) => {
            Ok(format!("category:{}", kql_quoted(&label_id_native(label))))
        }
        SearchFilter::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!("received>={}", system_time_date(*after)));
            }
            if let Some(before) = before {
                parts.push(format!("received<{}", system_time_date(*before)));
            }
            Ok(parts.join(" AND "))
        }
        SearchFilter::And(filters) => kql_join(filters, "AND"),
        SearchFilter::Or(filters) => kql_join(filters, "OR"),
        SearchFilter::Not(filter) => Ok(format!("NOT ({})", kql_filter(filter)?)),
        // `In` (folder scoping) has no KQL property; failing cleanly beats
        // shipping a request that would silently search every folder.
        SearchFilter::In(_) => Err(invalid_account_error(
            AccountOperation::Search,
            "Graph search cannot combine a folder restriction with a \
             sender/recipient substring match (no KQL folder property); \
             use a folder-scoped search or drop the From/To term",
        )),
        _ => Err(unsupported_account_error(AccountOperation::Search)),
    }
}

pub(super) fn kql_join(filters: &[SearchFilter], op: &str) -> Result<String, AccountError> {
    let mut parts = Vec::new();
    for filter in filters {
        let part = kql_filter(filter)?;
        if !part.is_empty() {
            parts.push(format!("({part})"));
        }
    }
    Ok(parts.join(&format!(" {op} ")))
}

/// KQL-quoted string value: wrap in double quotes (so multi-word values are
/// one phrase, not OR-ed tokens) and escape embedded double quotes.
pub(super) fn kql_quoted(value: &str) -> String {
    format!("\"{}\"", graph_search_escape(value))
}

pub(super) fn odata_filter(filter: &SearchFilter) -> Result<String, AccountError> {
    // `From`/`To` are never reached here: any filter tree containing a
    // sender/recipient substring leaf is detected by `filter_requires_search`
    // in `search_url` and routed onto `$search`/KQL instead (Graph `$filter`
    // rejects `contains()` on those navigation properties with a 400). The
    // arms below are kept as a defensive fallback for a direct `odata_filter`
    // call and use the rejected `contains()` shape, but the live `search_url`
    // path no longer emits them.
    match filter {
        SearchFilter::From(value) => Ok(format!(
            "(contains(from/emailAddress/address,{0}) or contains(from/emailAddress/name,{0}))",
            odata_quoted(value)
        )),
        SearchFilter::To(value) => Ok(format!(
            "(toRecipients/any(r:contains(r/emailAddress/address,{0}) or contains(r/emailAddress/name,{0})) or ccRecipients/any(r:contains(r/emailAddress/address,{0}) or contains(r/emailAddress/name,{0})) or bccRecipients/any(r:contains(r/emailAddress/address,{0}) or contains(r/emailAddress/name,{0})))",
            odata_quoted(value)
        )),
        SearchFilter::Subject(value) => Ok(format!("contains(subject,{})", odata_quoted(value))),
        SearchFilter::Body(value) => Ok(format!("contains(body/content,{})", odata_quoted(value))),
        SearchFilter::Has(value) => {
            if value.is_empty() {
                Ok("hasAttachments eq true".to_string())
            } else {
                Err(unsupported_account_error(AccountOperation::Search))
            }
        }
        SearchFilter::In(container) => {
            Ok(format!("parentFolderId eq {}", odata_quoted(&container.0)))
        }
        SearchFilter::Labeled(label) => Ok(format!(
            "categories/any(c:c eq {})",
            odata_quoted(&label_id_native(label))
        )),
        SearchFilter::DateRange { after, before } => {
            let mut parts = Vec::new();
            if let Some(after) = after {
                parts.push(format!("sentDateTime ge {}", system_time_rfc3339(*after)));
            }
            if let Some(before) = before {
                parts.push(format!("sentDateTime lt {}", system_time_rfc3339(*before)));
            }
            Ok(parts.join(" and "))
        }
        SearchFilter::And(filters) => join_filters(filters, "and"),
        SearchFilter::Or(filters) => join_filters(filters, "or"),
        SearchFilter::Not(filter) => Ok(format!("not ({})", odata_filter(filter)?)),
        _ => Err(unsupported_account_error(AccountOperation::Search)),
    }
}

pub(super) fn join_filters(filters: &[SearchFilter], op: &str) -> Result<String, AccountError> {
    let mut parts = Vec::new();
    for filter in filters {
        let part = odata_filter(filter)?;
        if !part.is_empty() {
            parts.push(format!("({part})"));
        }
    }
    Ok(parts.join(&format!(" {op} ")))
}

pub(super) fn label_id_native(label: &LabelId) -> String {
    label.0.clone()
}

pub(super) fn odata_quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) fn graph_search_escape(value: &str) -> String {
    value.replace('"', "\\\"")
}
