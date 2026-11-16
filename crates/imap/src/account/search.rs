//! Account search compilation and position-based paging.

use super::containers::folder_from_container;
use super::error::{folder_err, or_folder_restriction_error, pim_malformed, search_cursor_voided};
use super::{
    ImapAccount, decode_object_id, encode_object_id, encode_thread_id, selected_uidvalidity,
};
use crate::types::{MailboxName, SearchCriteria, ThreadNode};
use bifrost_types::ids::{ObjectId, ThreadId};
use bifrost_types::page::Page;
use bifrost_types::search::{SearchFilter, SearchRequest};
use bifrost_types::{AccountError, AccountFuture, AccountOperation, LabelId, Protocol};
use jiff::Timestamp;
use jiff::tz::Offset;
use std::time::SystemTime;

pub(crate) fn search(
    account: ImapAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
    Box::pin(async move {
        account
            .capabilities
            .pim_methods
            .gate(AccountOperation::Search, Protocol::Imap)?;
        let plan = search_plan(&account.dav_scopes, &request, AccountOperation::Search)?;
        let mut pager = SearchPager::new(&request, plan.folder.as_ref(), AccountOperation::Search)?;
        for folder in pager.folders(search_folders(&account, plan.folder.as_ref())) {
            if pager.is_full() {
                break;
            }
            let err = folder_err(&account, &folder, AccountOperation::Search);
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let selected = account
                .select_folder(&mut conn, &folder, super::SelectPurpose::Read)
                .await
                .map_err(err)?;
            let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(err)?;
            pager.check_epoch(&folder, uidvalidity)?;
            let roots = conn
                .connection()
                .uid_thread(
                    "REFERENCES",
                    "UTF-8",
                    &plan.criteria,
                    account.command_timeout(),
                )
                .await
                .map_err(err)?;
            // A thread's position is its lowest member UID: member sets are
            // disjoint, so the minima order the folder's threads totally,
            // and an expunge elsewhere cannot move an untouched thread.
            let mut threads = Vec::new();
            for root in roots {
                let mut uids = Vec::new();
                flatten_thread(&root, &mut uids);
                if let Some(&key) = uids.iter().min() {
                    threads.push((key, uids));
                }
            }
            pager.accept(&folder, uidvalidity, threads, |uids| {
                encode_thread_id(&folder, uidvalidity, &super::UidSet::from_numbers(uids))
            })?;
        }
        Ok(pager.finish())
    })
}

pub(crate) fn search_messages(
    account: ImapAccount,
    request: SearchRequest,
) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
    Box::pin(async move {
        account
            .capabilities
            .pim_methods
            .gate(AccountOperation::SearchMessages, Protocol::Imap)?;
        let plan = search_plan(
            &account.dav_scopes,
            &request,
            AccountOperation::SearchMessages,
        )?;
        let mut pager = SearchPager::new(
            &request,
            plan.folder.as_ref(),
            AccountOperation::SearchMessages,
        )?;
        for folder in pager.folders(search_folders(&account, plan.folder.as_ref())) {
            if pager.is_full() {
                break;
            }
            let err = folder_err(&account, &folder, AccountOperation::SearchMessages);
            let mut conn = account.checkout_for_folder(&folder).await.map_err(err)?;
            let selected = account
                .select_folder(&mut conn, &folder, super::SelectPurpose::Read)
                .await
                .map_err(err)?;
            let uidvalidity = selected_uidvalidity(&selected.mailbox).map_err(err)?;
            pager.check_epoch(&folder, uidvalidity)?;
            let result = conn
                .connection()
                .uid_search(&plan.criteria, account.command_timeout())
                .await
                .map_err(err)?;
            let uids = result.ids.into_iter().map(|uid| (uid, uid)).collect();
            pager.accept(&folder, uidvalidity, uids, |uid| {
                encode_object_id(
                    &folder,
                    uidvalidity,
                    crate::types::Uid::new(uid).expect("wire UID is nonzero"),
                )
            })?;
        }
        Ok(pager.finish())
    })
}

#[derive(Debug)]
struct SearchPlan {
    criteria: SearchCriteria,
    folder: Option<MailboxName>,
}

fn search_plan(
    scopes: &super::DavScopeIndex,
    request: &SearchRequest,
    op: AccountOperation,
) -> Result<SearchPlan, AccountError> {
    let mut plan = match &request.filter {
        Some(filter) => criteria_from_filter(scopes, filter, op)?,
        None => CriteriaPart {
            criteria: SearchCriteria::new().all(),
            folder: None,
        },
    };
    if let Some(raw) = request
        .provider_query
        .as_deref()
        .filter(|raw| !raw.trim().is_empty())
    {
        validate_provider_query(raw, op)?;
        if plan.criteria == "ALL" {
            plan.criteria = SearchCriteria::raw(raw.trim().to_owned(), Vec::new());
        } else {
            plan.criteria = plan
                .criteria
                .and(SearchCriteria::raw(raw.trim().to_owned(), Vec::new()));
        }
    }
    if plan.criteria.trim().is_empty() {
        plan.criteria = SearchCriteria::new().all();
    }
    Ok(SearchPlan {
        criteria: plan.criteria,
        folder: plan.folder,
    })
}

fn validate_provider_query(raw: &str, op: AccountOperation) -> Result<(), AccountError> {
    let bytes = raw.as_bytes();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut pos = 0usize;

    while pos < bytes.len() {
        let byte = bytes[pos];
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            pos += 1;
            continue;
        }

        match byte {
            b'"' => quoted = true,
            b'(' => depth += 1,
            b')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| pim_malformed(op, "provider query has an unmatched ')'"))?;
            }
            b'{' => {
                let mut end = pos + 1;
                let digits_start = end;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if end > digits_start {
                    if bytes.get(end) == Some(&b'+') {
                        end += 1;
                    }
                    if bytes.get(end) == Some(&b'}') {
                        return Err(pim_malformed(
                            op,
                            "provider query must not contain an IMAP literal",
                        ));
                    }
                }
            }
            _ => {}
        }
        pos += 1;
    }

    if quoted {
        return Err(pim_malformed(
            op,
            "provider query has an unterminated quoted string",
        ));
    }
    if depth != 0 {
        return Err(pim_malformed(op, "provider query has an unmatched '('"));
    }
    Ok(())
}

#[derive(Debug)]
struct CriteriaPart {
    criteria: SearchCriteria,
    folder: Option<MailboxName>,
}

fn criteria_from_filter(
    scopes: &super::DavScopeIndex,

    filter: &SearchFilter,
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    match filter {
        SearchFilter::From(value) => leaf(SearchCriteria::new().from(value), op),
        SearchFilter::To(value) => leaf(SearchCriteria::new().to(value), op),
        SearchFilter::Subject(value) => leaf(SearchCriteria::new().subject(value), op),
        // Has currently preserves the existing BODY approximation, which can
        // match prose and miss encoded attachment filenames. It does not meet
        // the attachment filename/type contract: enterprise users expect actual
        // attachments. Correct support needs attachment-aware evaluation (also
        // through AND/OR/NOT and pagination), not another SEARCH string alias.
        // Do not replace this with Unsupported without owner authorization:
        // that would remove the existing search path rather than repair it.
        SearchFilter::Body(value) | SearchFilter::Has(value) => {
            leaf(SearchCriteria::new().body(value), op)
        }
        SearchFilter::In(container) => Ok(CriteriaPart {
            criteria: SearchCriteria::new().all(),
            folder: Some(folder_from_container(scopes, container, op)?),
        }),
        SearchFilter::Labeled(LabelId(label)) => leaf(SearchCriteria::new().keyword(label), op),
        SearchFilter::DateRange { after, before } => {
            let mut criteria = SearchCriteria::new();
            if let Some(after) = after {
                criteria = criteria
                    .sent_since(&imap_date(*after))
                    .map_err(|e| pim_malformed(op, e.to_string()))?;
            }
            if let Some(before) = before {
                criteria = criteria
                    .sent_before(&imap_date(*before))
                    .map_err(|e| pim_malformed(op, e.to_string()))?;
            }
            Ok(CriteriaPart {
                criteria: empty_to_all(criteria),
                folder: None,
            })
        }
        SearchFilter::And(filters) => combine_and(scopes, filters, op),
        SearchFilter::Or(filters) => combine_or(scopes, filters, op),
        SearchFilter::Not(filter) => {
            let inner = criteria_from_filter(scopes, filter, op)?;
            Ok(CriteriaPart {
                criteria: SearchCriteria::new().not(&SearchCriteria::new().group(inner.criteria)),
                folder: inner.folder,
            })
        }
        _ => Err(super::error::unsupported(op)),
    }
}

fn leaf(
    result: Result<SearchCriteria, crate::Error>,
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    let criteria = result.map_err(|e| pim_malformed(op, e.to_string()))?;
    Ok(CriteriaPart {
        criteria: empty_to_all(criteria),
        folder: None,
    })
}

fn combine_and(
    scopes: &super::DavScopeIndex,

    filters: &[SearchFilter],
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    let mut criteria = Vec::new();
    let mut folder = None;
    for filter in filters {
        let part = criteria_from_filter(scopes, filter, op)?;
        folder = merge_folder(folder, part.folder, op)?;
        if part.criteria != "ALL" {
            criteria.push(part.criteria);
        }
    }
    Ok(CriteriaPart {
        criteria: if criteria.is_empty() {
            SearchCriteria::new().all()
        } else {
            criteria
                .into_iter()
                .fold(SearchCriteria::new(), SearchCriteria::and)
        },
        folder,
    })
}

fn combine_or(
    scopes: &super::DavScopeIndex,

    filters: &[SearchFilter],
    op: AccountOperation,
) -> Result<CriteriaPart, AccountError> {
    if filters.is_empty() {
        return Ok(CriteriaPart {
            criteria: SearchCriteria::new().all(),
            folder: None,
        });
    }
    let mut parts = Vec::new();
    for filter in filters {
        let part = criteria_from_filter(scopes, filter, op)?;
        // A folder restriction (`In`) nested inside an `Or` cannot be
        // expressed in a single-mailbox IMAP SEARCH: SEARCH runs against
        // the currently-selected mailbox, so hoisting the `In` folder to
        // the whole OR group would silently restrict the *other* OR
        // branches to that folder too, changing the query's meaning. The
        // AND case is defensible (the folder narrows the whole
        // conjunction); the OR case is not, so reject it explicitly
        // rather than leak the restriction.
        if part.folder.is_some() {
            return Err(or_folder_restriction_error(op));
        }
        parts.push(part.criteria);
    }
    let folder = None;
    let mut iter = parts.into_iter();
    let mut criteria = iter.next().unwrap_or_else(|| SearchCriteria::new().all());
    for next in iter {
        criteria = SearchCriteria::new().or(
            &SearchCriteria::new().group(criteria),
            &SearchCriteria::new().group(next),
        );
    }
    Ok(CriteriaPart { criteria, folder })
}

#[allow(clippy::unwrap_in_result)]
fn merge_folder(
    current: Option<MailboxName>,
    next: Option<MailboxName>,
    op: AccountOperation,
) -> Result<Option<MailboxName>, bifrost_types::AccountError> {
    match (current, next) {
        (Some(a), Some(b)) if a != b => Err(super::error::search_folders_conflict(&a, &b, op)),
        (Some(a), _) => Ok(Some(a)),
        (_, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

fn empty_to_all(criteria: SearchCriteria) -> SearchCriteria {
    if criteria.trim().is_empty() {
        SearchCriteria::new().all()
    } else {
        criteria
    }
}

fn imap_date(time: SystemTime) -> String {
    let datetime = Offset::UTC
        .to_datetime(Timestamp::try_from(time).unwrap_or(Timestamp::UNIX_EPOCH))
        .date();
    let month = match datetime.month() {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        _ => "Dec",
    };
    format!("{}-{month}-{}", datetime.day(), datetime.year())
}

fn search_folders(account: &ImapAccount, restriction: Option<&MailboxName>) -> Vec<MailboxName> {
    if let Some(folder) = restriction {
        return vec![folder.clone()];
    }
    // `entries()` iterates a `HashMap`, so its order is not stable
    // run-to-run. The search page cursor is a position in a walk ordered
    // by folder and then UID, and `SearchPager` resumes by skipping every
    // folder ordered before the cursor's; a nondeterministic folder order
    // would skip folders the walk never visited. Sort by display name and
    // wire identity, the same total order the pager compares by.
    sorted_search_folders(&account.folders)
}

fn sorted_search_folders(registry: &super::folder_registry::FolderRegistry) -> Vec<MailboxName> {
    let mut folders: Vec<MailboxName> = registry
        .entries()
        .into_iter()
        .filter(|entry| entry.selectable())
        .map(|entry| entry.name.clone())
        .collect();
    folders.sort_by(MailboxName::cmp_account_order);
    folders
}

/// Prefix of the search page cursor. The payload after it is the canonical
/// `imap2` object id of the walk position, so the folder rides in the same
/// length-prefixed wire-identity encoding object ids use.
const SEARCH_CURSOR_PREFIX: &str = "imapsearch1:";

/// Where a search walk stands: the folder, its UIDVALIDITY, and the key UID
/// of the last item handed back (the message UID, or a thread's lowest
/// member UID).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchPosition {
    folder: MailboxName,
    uidvalidity: crate::types::UidValidity,
    key: crate::types::Uid,
}

impl SearchPosition {
    fn encode(&self) -> Vec<u8> {
        let id = encode_object_id(&self.folder, self.uidvalidity, self.key);
        format!("{SEARCH_CURSOR_PREFIX}{}", id.0).into_bytes()
    }

    fn decode(bytes: &[u8], op: AccountOperation) -> Result<Self, AccountError> {
        let invalid = || search_page_cursor_error(op, "invalid IMAP page cursor");
        let payload = std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.strip_prefix(SEARCH_CURSOR_PREFIX))
            .filter(|payload| payload.starts_with("imap2:"))
            .ok_or_else(invalid)?;
        let decoded = decode_object_id(&ObjectId(payload.to_owned()), op).map_err(|_| invalid())?;
        Ok(Self {
            folder: decoded.folder.clone(),
            uidvalidity: decoded.uidvalidity,
            key: decoded.uid,
        })
    }
}

/// Pages a multi-folder search by position, not by offset.
///
/// IMAP has no server-side result state a page cursor could pin, so every
/// page call re-runs the search. An offset into that fresh result is not
/// stable: expunging an item on an earlier page shifts every later item down
/// one place and the item at the boundary is never returned. The cursor is
/// instead the position of the last item handed back, in a walk ordered by
/// folder (`MailboxName::cmp_account_order`) and then key UID ascending. UIDs
/// within one UIDVALIDITY never move, so "key greater than the cursor's" names
/// the same survivors whatever was expunged meanwhile, and folders ordered
/// before the cursor's are not searched again.
///
/// What a position walk does not promise: an item moved into, or a folder
/// created or renamed into, a position the walk already passed is not
/// returned; a thread whose membership changed between pages can come back
/// under a new id or be passed. A changed UIDVALIDITY on the cursor's folder
/// voids every UID in the cursor. Request folder re-inventory and have the
/// caller repeat the walk against the fresh generation.
struct SearchPager<T> {
    resume: Option<SearchPosition>,
    limit: usize,
    collected: Vec<(SearchPosition, T)>,
    op: AccountOperation,
}

impl<T> SearchPager<T> {
    fn new(
        request: &SearchRequest,
        restriction: Option<&MailboxName>,
        op: AccountOperation,
    ) -> Result<Self, AccountError> {
        let resume = request
            .page_cursor
            .as_deref()
            .map(|bytes| SearchPosition::decode(bytes, op))
            .transpose()?;
        if let (Some(resume), Some(restriction)) = (&resume, restriction)
            && resume.folder.cmp_account_order(restriction) != std::cmp::Ordering::Equal
        {
            return Err(search_page_cursor_error(
                op,
                "IMAP page cursor names a folder outside the search restriction",
            ));
        }
        // A zero-item page has no last item to mint a position from: reusing
        // the incoming cursor would loop, and advancing would skip.
        let limit = request.limit.unwrap_or(500);
        if limit == 0 {
            return Err(pim_malformed(op, "IMAP search page limit must be nonzero"));
        }
        Ok(Self {
            resume,
            limit: usize::try_from(limit).unwrap_or(usize::MAX),
            collected: Vec::new(),
            op,
        })
    }

    /// The folders still ahead of the cursor, in walk order. `all` must
    /// already be sorted by `cmp_account_order`.
    fn folders(&self, all: Vec<MailboxName>) -> Vec<MailboxName> {
        match &self.resume {
            None => all,
            Some(resume) => all
                .into_iter()
                .filter(|folder| folder.cmp_account_order(&resume.folder).is_ge())
                .collect(),
        }
    }

    /// One item past the page is known, so the page is settled and later
    /// folders need not be searched.
    fn is_full(&self) -> bool {
        self.collected.len() > self.limit
    }

    /// The UID floor this folder resumes above, or a `ConcurrencyConflict`
    /// when it is the cursor's folder under a different UIDVALIDITY. Call it
    /// straight after SELECT, so a changed epoch is reported as itself rather
    /// than as whatever the following search command did.
    fn check_epoch(
        &self,
        folder: &MailboxName,
        uidvalidity: crate::types::UidValidity,
    ) -> Result<Option<crate::types::Uid>, AccountError> {
        match &self.resume {
            Some(resume) if folder.cmp_account_order(&resume.folder).is_eq() => {
                if resume.uidvalidity != uidvalidity {
                    return Err(search_cursor_voided(self.op, folder));
                }
                Ok(Some(resume.key))
            }
            _ => Ok(None),
        }
    }

    /// Take one folder's results, `(key, item)` in any order.
    fn accept<K>(
        &mut self,
        folder: &MailboxName,
        uidvalidity: crate::types::UidValidity,
        mut entries: Vec<(u32, K)>,
        mint: impl Fn(K) -> T,
    ) -> Result<(), AccountError> {
        let floor = self.check_epoch(folder, uidvalidity)?;
        entries.sort_by_key(|(key, _)| *key);
        entries.dedup_by_key(|(key, _)| *key);
        for (key, entry) in entries {
            if self.is_full() {
                break;
            }
            if floor.is_some_and(|floor| key <= floor.get()) {
                continue;
            }
            let position = SearchPosition {
                folder: folder.clone(),
                uidvalidity,
                key: crate::types::Uid::new(key)
                    .ok_or_else(|| pim_malformed(self.op, "zero search position"))?,
            };
            self.collected.push((position, mint(entry)));
        }
        Ok(())
    }

    fn finish(mut self) -> Page<T> {
        let (next_cursor, estimated_total) = if self.collected.len() > self.limit {
            // The lookahead item stays ahead of the cursor for the next call.
            self.collected.truncate(self.limit);
            let last = self
                .collected
                .last()
                .map(|(position, _)| position.encode().into());
            (last, None)
        } else if self.resume.is_none() {
            // This call enumerated the whole result from the start.
            (
                None,
                Some(u64::try_from(self.collected.len()).unwrap_or(u64::MAX)),
            )
        } else {
            (None, None)
        };
        Page {
            truncated: false,
            items: self.collected.into_iter().map(|(_, item)| item).collect(),
            next_cursor,
            estimated_total,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        }
    }
}

/// A page cursor this account cannot use (undecodable, or naming a folder
/// outside the search restriction) requires restarting the walk, not
/// retrying the same page. The shared page_cursor field is the signal JMAP
/// uses too, so consumers restart paging the same way across providers.
fn search_page_cursor_error(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    AccountError::invalid_page_cursor(op, Protocol::Imap, detail)
}

fn flatten_thread(node: &ThreadNode, out: &mut Vec<u32>) {
    // Reply depth is not bounded by the wire grammar's parenthesis cap.
    let mut pending = vec![node];
    while let Some(node) = pending.pop() {
        if let Some(uid) = node.id {
            out.push(uid);
        }
        pending.extend(node.children.iter().rev());
    }
}

#[cfg(test)]
mod tests;
