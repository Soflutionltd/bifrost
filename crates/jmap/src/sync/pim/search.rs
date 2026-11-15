//! Mail search routing, query projection and positional page validation.

use super::checked_timestamp;
use super::{MailAccount, to_acct_err};
use crate::core::query;
use crate::core::transport::HttpTransport;
use crate::email::{Email, EmailGet, EmailId, Property as EmailProperty};
use crate::mailbox::MailboxId;
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, LabelId, ObjectId, Page, SearchFilter,
    SearchRequest, ThreadId,
};
use std::sync::Arc;

/// The JMAP account a search runs against, derived from the `In`
/// containers in its filter.
///
/// `Email/query` addresses exactly one `accountId`, and a foreign
/// container's `ContainerId` is the owner-qualified form `containers_list`
/// mints (`encode_foreign`), so the owner has to be decided BEFORE the
/// query is built: the qualified id is not a mailbox id in any account's
/// namespace. `None` is the primary account - no `In` at all, or an `In`
/// naming a primary container.
///
/// Two `In` filters naming different owners are rejected rather than
/// unioned. One request cannot carry two `accountId`s, and an implicit
/// cross-account union is a deliberate non-goal.
fn search_owner(
    filter: Option<&SearchFilter>,
    op: AccountOperation,
) -> Result<Option<String>, AccountError> {
    fn walk<'a>(
        filter: &'a SearchFilter,
        found: &mut Option<Option<&'a str>>,
        op: AccountOperation,
    ) -> Result<(), AccountError> {
        match filter {
            SearchFilter::In(container) => {
                let owner = crate::sync::foreign::JmapRef::parse(&container.0)
                    .owner
                    .shared();
                match found {
                    Some(seen) if *seen != owner => {
                        return Err(crate::sync::error::search_cross_account_filter(
                            op, *seen, owner,
                        ));
                    }
                    Some(_) => {}
                    None => *found = Some(owner),
                }
            }
            SearchFilter::And(filters) | SearchFilter::Or(filters) => {
                for inner in filters {
                    walk(inner, found, op)?;
                }
            }
            SearchFilter::Not(inner) => walk(inner, found, op)?,
            _ => {}
        }
        Ok(())
    }

    let Some(filter) = filter else {
        return Ok(None);
    };
    let mut found = None;
    walk(filter, &mut found, op)?;
    Ok(found.flatten().map(ToString::to_string))
}

/// Select the `Mail` handle a search routes to.
///
/// An `In` naming a share this session does not hold fails honestly. Unlike
/// hydration, there is no stay-literal fallback available here: the id is a
/// filter operand, so the primary account would answer "no such mailbox"
/// as an empty page rather than a miss.
fn route_search<'a, T: HttpTransport>(
    foreign_mail: &'a crate::sync::registry::Registry<T>,
    filter: Option<&SearchFilter>,
    op: AccountOperation,
) -> Result<(&'a MailAccount<T>, Option<String>), AccountError> {
    let owner = search_owner(filter, op)?;
    match owner {
        None => Ok((&foreign_mail.primary, None)),
        Some(account_id) => {
            match foreign_mail.route_owner(crate::sync::foreign::Owner::Shared(&account_id)) {
                crate::sync::registry::Route::Shared(handle) => Ok((handle, Some(account_id))),
                crate::sync::registry::Route::Departed
                | crate::sync::registry::Route::Primary(_) => {
                    Err(crate::sync::error::search_unknown_account(
                        op,
                        &qualified_container_label(filter, &account_id),
                        &account_id,
                    ))
                }
            }
        }
    }
}

/// The qualified container id that selected an unreachable owner, for the
/// error text. Falls back to the account id when the filter shape changed
/// under us.
fn qualified_container_label(filter: Option<&SearchFilter>, account_id: &str) -> String {
    fn find(filter: &SearchFilter, account_id: &str) -> Option<String> {
        match filter {
            SearchFilter::In(container) => (crate::sync::foreign::JmapRef::parse(&container.0)
                .owner
                .shared()
                == Some(account_id))
            .then(|| container.0.clone()),
            SearchFilter::And(filters) | SearchFilter::Or(filters) => {
                filters.iter().find_map(|inner| find(inner, account_id))
            }
            SearchFilter::Not(inner) => find(inner, account_id),
            _ => None,
        }
    }
    filter
        .and_then(|filter| find(filter, account_id))
        .unwrap_or_else(|| account_id.to_string())
}

/// Re-qualify a native id from `owner`'s namespace back into the
/// consumer-facing one. A primary result stays bare - one wire form per
/// logical object.
pub(super) fn qualify(owner: Option<&str>, native: String) -> String {
    match owner {
        Some(account_id) => crate::sync::foreign::encode_object(account_id, &native),
        None => native,
    }
}

pub(crate) fn search<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        let op = AccountOperation::Search;
        let (routed, owner) = route_search(&foreign_mail, request.filter.as_ref(), op)?;
        let owner = owner.as_deref();
        let page = search_email_ids(routed, request, true, owner, op).await?;
        if page.items.is_empty() {
            return Ok(Page {
                truncated: false,
                items: Vec::new(),
                next_cursor: page.next_cursor,
                estimated_total: page.estimated_total,
                failed_ids: Vec::new(),
                skipped_scopes: Vec::new(),
            });
        }
        let requested = page
            .items
            .iter()
            .cloned()
            .map(EmailId::into_string)
            .collect::<Vec<_>>();
        let response = routed
            .call(
                EmailGet::new()
                    .ids(page.items)
                    .properties([EmailProperty::Id, EmailProperty::ThreadId]),
            )
            .await
            .map_err(to_acct_err(AccountOperation::Search))?;
        let not_found = response.not_found().to_vec();
        let (items, failed_ids) =
            reconcile_search_threads(requested, &not_found, response.into_list());
        // Thread ids ride the object namespace, exactly as foreign inventory
        // and hydration mint them: a bare foreign thread id asserts primary
        // ownership and every thread-keyed door would then act on an
        // unrelated primary thread.
        let items = items
            .into_iter()
            .map(|thread| ThreadId(qualify(owner, thread.0)))
            .collect();
        let failed_ids = failed_ids
            .into_iter()
            .map(|id| qualify(owner, id))
            .collect();
        Ok(Page {
            truncated: false,
            items,
            next_cursor: page.next_cursor,
            estimated_total: page.estimated_total,
            failed_ids,
            skipped_scopes: Vec::new(),
        })
    })
}

/// Project a thread-id search answer, accounting for every submitted
/// email id.
///
/// A thread search is a query for emails followed by a projection onto
/// their threads, so an email the projection cannot place is a hit the
/// caller asked for and does not receive. Three ways that happens - the
/// id named in `notFound`, the id answered in neither list, and an email
/// returned WITHOUT a `threadId` (which RFC 8621 makes mandatory) - are
/// all "the provider fetched it but could not materialize it", which is
/// what `Page::failed_ids` means. Dropping them left the page silently
/// short of its own result count.
///
/// Thread ids are deduplicated: a query that hits several messages of one
/// thread describes one thread, and `collapseThreads` is not guaranteed
/// to have collapsed them.
pub(super) fn reconcile_search_threads(
    requested: Vec<String>,
    not_found: &[EmailId],
    list: Vec<Email>,
) -> (Vec<ThreadId>, Vec<String>) {
    let mut answered: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut failed_ids: Vec<String> = Vec::new();
    for missing in not_found {
        let missing = missing.clone().into_string();
        if answered.insert(missing.clone()) {
            failed_ids.push(missing);
        }
    }

    let mut seen_threads: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut items = Vec::new();
    for email in list {
        let Some(id) = email.id().map(ToString::to_string) else {
            continue;
        };
        if id.is_empty() {
            continue;
        }
        match email.thread_id() {
            Some(thread) => {
                answered.insert(id);
                if seen_threads.insert(thread.to_string()) {
                    items.push(ThreadId(thread.to_string()));
                }
            }
            None => {
                if answered.insert(id.clone()) {
                    failed_ids.push(id);
                }
            }
        }
    }

    for id in requested {
        if answered.insert(id.clone()) {
            failed_ids.push(id);
        }
    }
    (items, failed_ids)
}

pub(crate) fn search_messages<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        let op = AccountOperation::SearchMessages;
        let (routed, owner) = route_search(&foreign_mail, request.filter.as_ref(), op)?;
        let owner = owner.as_deref();
        let page = search_email_ids(routed, request, false, owner, op).await?;
        Ok(Page {
            truncated: false,
            items: page
                .items
                .into_iter()
                .map(|id| ObjectId(qualify(owner, id.into_string())))
                .collect(),
            next_cursor: page.next_cursor,
            estimated_total: page.estimated_total,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

pub(super) async fn search_email_ids<T: HttpTransport>(
    mail: &MailAccount<T>,
    request: SearchRequest,
    collapse_threads: bool,
    owner: Option<&str>,
    op: AccountOperation,
) -> Result<Page<EmailId>, AccountError> {
    let cursor = decode_position(request.page_cursor.as_deref(), owner, op)?;
    let limit = request.limit.unwrap_or(50).max(1);
    let query_request = build_search_query(request, collapse_threads, cursor.position, limit)?;
    let response = mail.call(query_request).await.map_err(to_acct_err(op))?;

    // The result set must not have moved between the page that minted the
    // cursor and this one. `Email/query` positions index an order the server
    // recomputes per call, so a `queryState` change means page 2 is being
    // taken from a DIFFERENT list than page 1: at a single delivery the
    // window slides by one and the caller silently loses a hit or sees a
    // duplicate. One delivered message is enough to move it, so this is not
    // terminal - it is a concurrency conflict: repeat the search.
    if let Some(pinned) = cursor.query_state.as_deref()
        && pinned != response.query_state()
    {
        return Err(crate::sync::error::search_result_set_superseded(op));
    }

    // A `total` that does not fit `u64` is reported, not dropped: silently
    // converting it to `None` would switch the walk's termination mode
    // without saying so.
    let total = response
        .total()
        .map(|total| {
            u64::try_from(total).map_err(|error| {
                crate::sync::error::unsupported_error(
                    op,
                    None,
                    format!("JMAP search total does not fit u64: {error}"),
                )
            })
        })
        .transpose()?;
    let served_from = response.position();
    let query_state = response.query_state().to_string();
    let ids = response.into_ids();
    let next_position = validate_search_page(cursor.position, served_from, ids.len(), total, op)?;

    // "The page came back full" is not evidence that more remains, and a
    // short page is not evidence that nothing does. `total` (which this query
    // always requests via `calculateTotal`) is the authoritative answer; with
    // no total the walk continues until an EMPTY page, the same rule the
    // anchored contact and calendar walks use. See `validate_search_page` for
    // why the envelope is checked before this test rather than after, and for
    // why the "server ignores position" non-termination residual this comment
    // used to accept is in fact detectable.
    let more = if ids.is_empty() {
        false
    } else if let Some(total) = total {
        // Both the served position and total retain the unsigned wire domain.
        next_position < total
    } else {
        true
    };
    let next_cursor = more.then(|| encode_position(next_position, &query_state, owner).into());

    Ok(Page {
        truncated: false,
        items: ids,
        next_cursor,
        estimated_total: total,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

/// Check a search page's envelope against the invariants RFC 8620 s5.5 gives
/// it, and return the position a successor cursor would carry.
///
/// Four rules, each of which a conforming server satisfies for free. All are
/// checked BEFORE the termination test, so a broken envelope can never
/// present as a completed search - which is what the old
/// `next_position >= total` arm did on its own.
///
/// - The echoed `position` is an UnsignedInt in the response schema; a
///   negative one indexes nothing.
/// - On a NONEMPTY page `position` is the index of the first returned id and
///   `total` is the length of the whole result list, so
///   `position + served <= total`. A response claiming to have served past
///   the end of the list it just measured is CONTRADICTORY - it is not
///   evidence that the walk finished, which is exactly how `>= total` alone
///   read it.
/// - On a NONEMPTY continuation (a positive requested position, which only a
///   decoded cursor produces) the page must BEGIN where it was asked to. The
///   pinned `queryState` was verified equal just above, so the result list is
///   the same finite ordered list the previous page came from and there is
///   nothing for a conforming server to clamp against; an echo that
///   disagrees with the request means the window moved while the state did
///   not. Trusting the echo instead is what let a server silently re-serve
///   its own idea of the page.
/// - The successor must advance past the position this page resumed from.
///   Given the echo rule this is implied on a continuation and trivial on a
///   first page (`served >= 1` from position 0), so it does not bite at this
///   seam today; it is stated because it is the property TERMINATION rests
///   on, and it is the rule a relaxation of the echo check would otherwise
///   take away silently.
///
/// The last two are why the non-termination residual is detectable. It was
/// documented as undetectable - "a server that omits `total` and ignores
/// `position` is indistinguishable from an unbounded result set" - and that
/// is false. The query state is PINNED, so the result list is stable and
/// finite for the length of the walk; a walk that does not advance under an
/// unmoved state is a server contract violation, not a long list.
///
/// A count or sum that does not fit the cursor's `u64` position is
/// `Request(Malformed)`: it is the CURSOR representation that
/// cannot carry the value, and the caller's answer is to restart the search
/// rather than to conclude anything about the server.
pub(super) fn validate_search_page(
    requested: u64,
    served_from: u64,
    served: usize,
    total: Option<u64>,
    op: AccountOperation,
) -> Result<u64, AccountError> {
    if served > 0 && requested > 0 && served_from != requested {
        return Err(crate::sync::error::contract_violation(
            op,
            None,
            format!(
                "Email/query served page from position {served_from} for a continuation \
                 requested at {requested}, under an unmoved queryState"
            ),
        ));
    }
    let served_count = u64::try_from(served).map_err(|_| invalid_search_cursor(op))?;
    let next_position = served_from
        .checked_add(served_count)
        .ok_or_else(|| invalid_search_cursor(op))?;
    if served > 0 {
        if let Some(total) = total
            && next_position > total
        {
            return Err(crate::sync::error::contract_violation(
                op,
                None,
                format!(
                    "Email/query served {served} ids from position {served_from} of a result \
                     list it reports as {total} long"
                ),
            ));
        }
        if next_position <= requested {
            return Err(crate::sync::error::contract_violation(
                op,
                None,
                format!(
                    "Email/query did not advance past position {requested}: the successor \
                     would resume at {next_position}, under an unmoved queryState"
                ),
            ));
        }
    }
    Ok(next_position)
}

/// A decoded search page cursor: where in the result order the next page
/// starts, plus the `queryState` that order belonged to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SearchCursor {
    pub(super) position: u64,
    /// `None` only for the synthetic first page (no cursor supplied).
    pub(super) query_state: Option<String>,
}

/// Build the `Email/query` a search page issues.
///
/// Split out from `search_email_ids` so the request shape is pinnable without
/// a live session; the sort in particular is load-bearing. RFC 8621 leaves the
/// order of an unsorted `Email/query` server-defined and guarantees nothing
/// about its stability across calls, while the page cursor here is a bare
/// integer position into that order. With no explicit comparator, paging a
/// search can duplicate and skip results on any server whose default order is
/// not stable, and returns a different order per server. Pin the same
/// `receivedAt desc` every inventory query in the crate pins.
pub(super) fn build_search_query(
    request: SearchRequest,
    collapse_threads: bool,
    position: u64,
    limit: u32,
) -> Result<crate::email::EmailQuery, AccountError> {
    let mut query_request = crate::email::EmailQuery::new()
        .collapse_threads(collapse_threads)
        .sort([query::Comparator::new(crate::email::query::Comparator::ReceivedAt).descending()])
        .position(
            query::QueryPosition::try_from(position)
                .map_err(|_| invalid_search_cursor(AccountOperation::Search))?,
        )
        .limit(usize::try_from(limit).unwrap_or(usize::MAX))
        .calculate_total(true);
    if let Some(filter) = build_search_filter(request.filter, request.provider_query)? {
        query_request = query_request.filter(filter);
    }
    Ok(query_request)
}

/// Search and anchored walks share the persisted cursor codec.
pub(super) fn encode_position(position: u64, query_state: &str, owner: Option<&str>) -> Vec<u8> {
    crate::sync::page_cursor::encode_search(position, query_state, owner)
}

pub(super) fn decode_position(
    cursor: Option<&[u8]>,
    owner: Option<&str>,
    op: AccountOperation,
) -> Result<SearchCursor, AccountError> {
    let decoded = crate::sync::page_cursor::decode_search(cursor, owner, op)?;
    Ok(SearchCursor {
        position: decoded.0,
        query_state: decoded.1,
    })
}

fn build_search_filter(
    filter: Option<SearchFilter>,
    provider_query: Option<String>,
) -> Result<Option<query::Filter<crate::email::query::Filter>>, AccountError> {
    let mut filters = Vec::new();
    if let Some(filter) = filter {
        filters.push(search_filter_to_jmap(filter)?);
    }
    if let Some(provider_query) = provider_query
        && !provider_query.is_empty()
    {
        filters.push(crate::email::query::Filter::text(provider_query).into());
    }
    Ok(match filters.len() {
        0 => None,
        1 => filters.pop(),
        _ => Some(query::Filter::and(filters)),
    })
}

fn search_filter_to_jmap(
    filter: SearchFilter,
) -> Result<query::Filter<crate::email::query::Filter>, AccountError> {
    Ok(match filter {
        SearchFilter::From(value) => crate::email::query::Filter::from(value).into(),
        SearchFilter::To(value) => query::Filter::or([
            crate::email::query::Filter::to(value.clone()),
            crate::email::query::Filter::cc(value.clone()),
            crate::email::query::Filter::bcc(value),
        ]),
        SearchFilter::Subject(value) => crate::email::query::Filter::subject(value).into(),
        SearchFilter::Body(value) => crate::email::query::Filter::body(value).into(),
        SearchFilter::Has(value) if value.is_empty() => {
            crate::email::query::Filter::has_attachment(true).into()
        }
        SearchFilter::Has(value) => {
            let filters: Vec<query::Filter<crate::email::query::Filter>> = vec![
                crate::email::query::Filter::has_attachment(true).into(),
                crate::email::query::Filter::text(value).into(),
            ];
            query::Filter::and(filters)
        }
        // The owner qualifier is stripped here, never sent: `route_search`
        // already selected the owning account handle and refused a filter
        // that names two, so the native part is a mailbox id in exactly the
        // account this query addresses. Sending the qualified form is the
        // old defect - it matched nothing in the primary account and the
        // consumer got an empty page with no error.
        SearchFilter::In(container) => crate::email::query::Filter::in_mailbox(MailboxId::new(
            crate::sync::foreign::native_object(&container.0).to_string(),
        ))
        .into(),
        SearchFilter::Labeled(LabelId(label)) => crate::email::query::Filter::has_keyword(
            crate::email::keyword::Keyword::parse(label)
                .map_err(to_acct_err(AccountOperation::Search))?,
        )
        .into(),
        SearchFilter::DateRange { after, before } => {
            let mut filters: Vec<query::Filter<crate::email::query::Filter>> = Vec::new();
            if let Some(after) = after {
                filters.push(
                    crate::email::query::Filter::sent_after(checked_timestamp(
                        after,
                        AccountOperation::Search,
                    )?)
                    .into(),
                );
            }
            if let Some(before) = before {
                filters.push(
                    crate::email::query::Filter::sent_before(checked_timestamp(
                        before,
                        AccountOperation::Search,
                    )?)
                    .into(),
                );
            }
            query::Filter::and(filters)
        }
        SearchFilter::And(filters) => query::Filter::and(
            filters
                .into_iter()
                .map(search_filter_to_jmap)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        SearchFilter::Or(filters) => query::Filter::or(
            filters
                .into_iter()
                .map(search_filter_to_jmap)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        SearchFilter::Not(filter) => query::Filter::not([search_filter_to_jmap(*filter)?]),
        _ => {
            return Err(crate::sync::error::unsupported_error(
                AccountOperation::Search,
                None,
                "JMAP does not support this SearchFilter variant",
            ));
        }
    })
}

/// Page cursors belong to the caller, never the engine's durable sync state.
/// An Outlook user expects a broken search to restart without clearing mail.
fn invalid_search_cursor(op: AccountOperation) -> AccountError {
    crate::sync::error::page_cursor_error(
        op,
        "search page cursor is malformed, retired or out of range".to_string(),
    )
}
