use bifrost_types::{
    AccountError, AccountOperation, AddressBook, AddressBookId, ContactAddress, ContactCard,
    ContactCorpus, ContactCreate, ContactEmail, ContactId, ContactOrganization, ContactPatch,
    ContactPhone, ContactProvenance, ContactSearchRequest, DirectoryCard, Page, ProtocolKind,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::origin::AdmittedUrl;
use crate::types::ODataCollection;

use super::GraphAccount;
use super::graph_error::{self, GraphErrorContext};

const DEFAULT_CONTACTS_ID: &str = "contacts";
const CONTACT_SELECT: &str = "id,displayName,emailAddresses,businessPhones,homePhones,mobilePhone,homeAddress,businessAddress,otherAddress,companyName,jobTitle,personalNotes,parentFolderId";

pub(crate) async fn address_books_list(
    account: GraphAccount,
) -> Result<Vec<AddressBook>, AccountError> {
    let prefix = account.client.api_path_prefix();
    let mut books = vec![AddressBook {
        id: AddressBookId(DEFAULT_CONTACTS_ID.to_string()),
        native_id: DEFAULT_CONTACTS_ID.to_string(),
        name: "Contacts".to_string(),
        provenance: ContactProvenance {
            provider: ProtocolKind::Graph,
            native: DEFAULT_CONTACTS_ID.to_string(),
            address_book_native: None,
        },
        corpus: ContactCorpus::Main,
        is_default: true,
        can_create_contacts: true,
        can_update_contacts: true,
        can_delete_contacts: true,
    }];
    let operation = AccountOperation::AddressBooksList;
    let mut walk = crate::paging::PageWalk::new("contactFolders");
    let mut next = Some(
        account
            .client
            .api_url(&format!(
                "{prefix}/contactFolders?$select=id,displayName,parentFolderId&$top=250"
            ))
            .map_err(|error| into_error(error, operation))?,
    );
    while let Some(url) = next {
        walk.enter(&url)
            .map_err(|error| into_error(error, operation))?;
        let page: ODataCollection<GraphContactFolder> = get_page(&account, &url, operation).await?;
        next = admit_next(&account, &page, operation)?;
        books.extend(page.value.into_iter().map(folder_to_address_book));
    }
    Ok(books)
}

pub(crate) async fn list(
    account: GraphAccount,
    address_book: Option<AddressBookId>,
    page_cursor: Option<Vec<u8>>,
) -> Result<Page<ContactCard>, AccountError> {
    contact_page(account, address_book, page_cursor, 250).await
}

async fn contact_page(
    account: GraphAccount,
    address_book: Option<AddressBookId>,
    page_cursor: Option<Vec<u8>>,
    top: u32,
) -> Result<Page<ContactCard>, AccountError> {
    let operation = AccountOperation::ContactsList;
    let url = match decode_link_cursor(&account, page_cursor, operation)? {
        Some(url) => url,
        None => account
            .client
            .api_url(&contacts_url(&account, address_book.as_ref(), top))
            .map_err(|error| into_error(error, operation))?,
    };
    let page: ODataCollection<GraphContact> = get_page(&account, &url, operation).await?;
    let next_cursor = link_cursor(admit_next(&account, &page, operation)?);
    Ok(Page {
        items: page.value.into_iter().map(contact_from_graph).collect(),
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

pub(crate) async fn get(
    account: GraphAccount,
    contact: ContactId,
) -> Result<ContactCard, AccountError> {
    let prefix = account.client.api_path_prefix();
    let encoded = bifrost_net::url::encode_path_component(&contact.0);
    let path = format!("{prefix}/contacts/{encoded}?$select={CONTACT_SELECT}");
    let contact = account
        .client
        .get_json::<GraphContact>(&path)
        .await
        .map_err(|error| into_error(error, AccountOperation::ContactGet))?;
    Ok(contact_from_graph(contact))
}

pub(crate) async fn create(
    account: GraphAccount,
    contact: ContactCreate,
) -> Result<ContactId, AccountError> {
    let path = create_url(&account, contact.address_book_id.as_ref());
    let created = account
        .client
        .post::<GraphContact, _>(&path, &graph_contact_from_create(&contact))
        .await
        .map_err(|error| into_error(error, AccountOperation::ContactCreate))?;
    Ok(ContactId(created.id))
}

pub(crate) async fn update(
    account: GraphAccount,
    contact: ContactId,
    patch: ContactPatch,
) -> Result<(), AccountError> {
    if patch.photo.is_some() {
        return Err(
            graph_error::unsupported_account_error(AccountOperation::ContactUpdate)
                .into_builder()
                .text(bifrost_types::DiagnosticText::support_only(
                    "Graph contact raw photo updates are unsupported".to_string(),
                ))
                .try_build()
                .expect("valid account error classification"),
        );
    }
    let current = get(account.clone(), contact.clone()).await?;
    let etag = current.etag.clone();
    let prefix = account.client.api_path_prefix();
    let encoded = bifrost_net::url::encode_path_component(&contact.0);
    let path = format!("{prefix}/contacts/{encoded}");
    let body = graph_contact_from_patch(&patch);
    let result = if let Some(etag) = etag.as_deref() {
        account.client.patch_if_match(&path, etag, &body).await
    } else {
        account.client.patch(&path, &body).await
    };
    result.map_err(|error| into_error(error, AccountOperation::ContactUpdate))
}

pub(crate) async fn delete(account: GraphAccount, contact: ContactId) -> Result<(), AccountError> {
    let current = get(account.clone(), contact.clone()).await?;
    let prefix = account.client.api_path_prefix();
    let encoded = bifrost_net::url::encode_path_component(&contact.0);
    let path = format!("{prefix}/contacts/{encoded}");
    let result = if let Some(etag) = current.etag.as_deref() {
        account.client.delete_if_match(&path, etag).await
    } else {
        account.client.delete(&path).await
    };
    result.map_err(|error| into_error(error, AccountOperation::ContactDelete))
}

pub(crate) async fn search(
    account: GraphAccount,
    request: ContactSearchRequest,
) -> Result<Page<ContactCard>, AccountError> {
    let limit = request
        .limit
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(250)
        .max(1);
    let operation = AccountOperation::ContactSearch;
    let (mut url, mut skip) = match decode_paged_cursor(&account, request.page_cursor, operation)? {
        Some(position) => position,
        None => {
            let first = account
                .client
                .api_url(&contact_search_url(
                    &account,
                    request.address_book_id.as_ref(),
                    &request.query,
                    top_for_limit(request.limit, 250),
                ))
                .map_err(|error| into_error(error, operation))?;
            (first, 0)
        }
    };
    let needle = request.query.to_ascii_lowercase();
    let mut items = Vec::new();
    let mut walk = crate::paging::PageWalk::new("contact search");
    let next_cursor;
    loop {
        walk.enter(&url).map_err(|error| {
            graph_error::into_account_error(
                error,
                graph_error::GraphErrorContext::graph(AccountOperation::ContactSearch),
            )
        })?;
        let page: ODataCollection<GraphContact> = get_page(&account, &url, operation).await?;
        let next_link = admit_next(&account, &page, operation)?;
        let page_len = page.value.len();
        for (index, value) in page.value.into_iter().enumerate().skip(skip) {
            let contact = contact_from_graph(value);
            if contact_matches(&contact, &needle) {
                items.push(contact);
                if items.len() == limit {
                    let consumed = index + 1;
                    next_cursor = if consumed < page_len {
                        Some(crate::paging::encode_paged_cursor(&url, consumed))
                    } else {
                        next_link
                            .as_ref()
                            .map(|next| crate::paging::encode_paged_cursor(next, 0))
                    };
                    return Ok(Page {
                        items,
                        next_cursor,
                        estimated_total: None,
                        failed_ids: Vec::new(),
                        skipped_scopes: Vec::new(),
                    });
                }
            }
        }
        let Some(next) = next_link else {
            next_cursor = None;
            break;
        };
        url = next;
        skip = 0;
    }
    Ok(Page {
        items,
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

const DIRECTORY_SELECT: &str =
    "displayName,mail,otherMails,proxyAddresses,businessPhones,companyName,jobTitle,department";

/// Organization-directory (Global Address List) search over `/users`.
///
/// An empty `query` enumerates the directory; a non-empty `query` pushes
/// a provider-side `startswith(displayName,..) or startswith(mail,..)`
/// `$filter`. A 403 from a tenant that has not granted directory read
/// (`User.ReadBasic.All` / `User.Read.All`) maps through `into_error` to
/// a real `NoPermission` `AccountError` - unlike Google, an unauthorized
/// directory is an error here, not an empty result.
pub(crate) async fn directory_search(
    account: GraphAccount,
    query: String,
    limit: Option<u32>,
    page_cursor: Option<Vec<u8>>,
) -> Result<Page<DirectoryCard>, AccountError> {
    let limit_cap = limit
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(250)
        .max(1);
    let operation = AccountOperation::DirectorySearch;
    let (mut url, mut skip) = match decode_paged_cursor(&account, page_cursor, operation)? {
        Some(position) => position,
        None => {
            let top = top_for_limit(limit, 999);
            let first = account
                .client
                .api_url(&directory_search_path(&query, top))
                .map_err(|error| into_error(error, operation))?;
            (first, 0)
        }
    };
    let mut items = Vec::new();
    let mut walk = crate::paging::PageWalk::new("directory search");
    let next_cursor;
    loop {
        walk.enter(&url).map_err(|error| {
            graph_error::into_account_error(
                error,
                graph_error::GraphErrorContext::graph(AccountOperation::DirectorySearch),
            )
        })?;
        let page: ODataCollection<GraphDirectoryUser> = get_page(&account, &url, operation).await?;
        let next_link = admit_next(&account, &page, operation)?;
        let page_len = page.value.len();
        for (index, value) in page.value.into_iter().enumerate().skip(skip) {
            if let Some(card) = directory_user_to_card(value) {
                items.push(card);
                if items.len() == limit_cap {
                    let consumed = index + 1;
                    next_cursor = if consumed < page_len {
                        Some(crate::paging::encode_paged_cursor(&url, consumed))
                    } else {
                        next_link
                            .as_ref()
                            .map(|next| crate::paging::encode_paged_cursor(next, 0))
                    };
                    return Ok(Page {
                        items,
                        next_cursor,
                        estimated_total: None,
                        failed_ids: Vec::new(),
                        skipped_scopes: Vec::new(),
                    });
                }
            }
        }
        let Some(next) = next_link else {
            next_cursor = None;
            break;
        };
        url = next;
        skip = 0;
    }
    Ok(Page {
        items,
        next_cursor,
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    })
}

/// The directory is tenant-wide: `/users`, never under `/me` (which has no
/// `users` navigation and answers 400).
fn directory_search_path(query: &str, top: u32) -> String {
    let base = format!("/users?$select={DIRECTORY_SELECT}&$top={top}");
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return base;
    }
    let escaped = trimmed.replace('\'', "''");
    let filter = format!("startswith(displayName,'{escaped}') or startswith(mail,'{escaped}')");
    format!(
        "{base}&$filter={}",
        bifrost_net::url::encode_query_value(&filter)
    )
}

/// Which half of the mailbox one SMTP `proxyAddresses` entry names.
///
/// The distinction is carried by the CASE of the type prefix and nothing
/// else, so it has to be read at parse time and preserved: once the prefix
/// is stripped, `SMTP:ada@x` and `smtp:ada@x` are indistinguishable, and
/// the `mail`-versus-primary rule below needs to tell them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyKind {
    /// Upper-case `SMTP:` - the mailbox's primary SMTP address.
    Primary,
    /// Lower-case `smtp:` - a secondary alias.
    Alias,
}

/// Strip the address-type prefix off one `proxyAddresses` entry, keeping
/// only the SMTP ones and reporting which kind the prefix named.
///
/// A Graph proxy address is `TYPE:value`, and the type prefix is not
/// decoration: `SMTP:` (upper) marks the mailbox's PRIMARY address, `smtp:`
/// (lower) a secondary alias, and the remaining types - `X500:`, `x500:`,
/// `SIP:`, `EUM:`, `SPO:` and friends - are directory/routing identifiers
/// that are not email addresses at all and must never reach a field a
/// consumer will put in a To: header.
///
/// Only an exactly upper-case `SMTP` is read as primary; every other SMTP
/// spelling is an alias. Guessing "primary" from a mixed-case prefix would
/// feed the suppression rule below on evidence the provider never gave.
///
/// An entry with no `:` at all carries no type, so its kind is unknowable
/// and it is DROPPED rather than guessed at - the same silent-drop rule the
/// mail-less row takes. So is an empty value after the prefix.
fn smtp_proxy_address(entry: &str) -> Option<(ProxyKind, &str)> {
    let (kind, value) = entry.split_once(':')?;
    if !kind.eq_ignore_ascii_case("smtp") {
        return None;
    }
    let kind = if kind == "SMTP" {
        ProxyKind::Primary
    } else {
        ProxyKind::Alias
    };
    let value = value.trim();
    (!value.is_empty()).then_some((kind, value))
}

/// Comparison key for the UNIVERSAL de-duplication pass: the domain half
/// ASCII-lowercased, the local part left exactly as spelled.
///
/// The split is the whole point. A domain is case-insensitive by DNS, but
/// RFC 5321 s2.4 requires the local part's case to be PRESERVED and leaves
/// its interpretation to the destination host - so `User@ext.example` and
/// `user@ext.example` may be two different mailboxes. `otherMails` is a
/// free-form list that can name any external system, so folding its local
/// parts would silently drop one of a pair.
///
/// The two error directions are not symmetric, and that is the tie-breaker
/// wherever case-sensitivity is uncertain. Folding too aggressively DROPS
/// an address, and this layer cannot reconstruct it: the consumer never
/// learns it existed. Folding too little emits a near-duplicate, which is
/// visible and which a consumer can collapse using knowledge this layer
/// does not have. So when in doubt, keep both.
///
/// Only ASCII case is folded (see `directory_additional_emails` for why),
/// and an address with no `@` has no domain half, so it is compared whole
/// and unfolded - the same conservative direction.
fn address_fold_key(address: &str) -> String {
    match address.rsplit_once('@') {
        Some((local, domain)) => format!("{local}@{}", domain.to_ascii_lowercase()),
        None => address.to_string(),
    }
}

/// Build `DirectoryCard::additional_emails` from `otherMails` plus the SMTP
/// half of `proxyAddresses`.
///
/// Order is deterministic and provider-driven: every `otherMails` entry in
/// server order, then every SMTP `proxyAddresses` entry in server order.
/// `otherMails` leads because it is the tenant-curated "other addresses"
/// list an admin typed, while `proxyAddresses` is the routing table Exchange
/// maintains, so the former is the more useful head of a truncated list.
///
/// De-duplication runs two comparisons, because the two source lists carry
/// different guarantees:
///
/// - UNIVERSAL, across everything: `address_fold_key`, which folds the
///   domain only and compares the local part as spelled. This is the safe
///   rule for `otherMails`, which can name an external system where the
///   local part really is case-sensitive.
/// - PROXY-ONLY, between `proxyAddresses` entries: whole-address ASCII
///   case-insensitive. Exchange refuses two proxy entries that differ only
///   by case, so a fold here can only ever collapse what the provider
///   already considers one address. That guarantee is about Exchange's own
///   proxy table and does not extend to `otherMails`.
///
/// A proxy entry that is suppressed still enters the proxy comparison set.
/// Otherwise "de-duplicate within proxies" would quietly become
/// "de-duplicate within EMITTED proxies", and a case variant of an entry
/// already covered by `otherMails` would survive.
///
/// `mail` versus the primary proxy entry gets its own explicit ASCII
/// case-insensitive comparison: the universal rule alone would leave
/// `mail = Ada@example.test` and `SMTP:ada@example.test` standing as two
/// entries for one mailbox. Only an entry that actually matches under that
/// comparison is suppressed - Graph documents guest accounts whose `SMTP:`
/// entry is NOT `mail`, so dropping every primary on the assumption would
/// lose a real address. That exception is scoped to the proxy list and does
/// not reach back into `otherMails`, which keeps a local-part variant of
/// the primary under the universal rule.
///
/// Folding is ASCII-only, and Unicode/IDNA equivalence is explicitly
/// outside the promise: ASCII folding is right for ASCII spellings
/// (punycode A-labels included), while general Unicode case folding is not
/// a correct substitute for IDNA processing, and half-done normalisation
/// would turn into address loss. Unexpected non-ASCII spellings are
/// preserved rather than folded.
///
/// The FIRST retained spelling wins - after surrounding whitespace is
/// trimmed; only the comparison is folded. The literal `mail` string can
/// never recur in `additional_emails`, though a local-part case VARIANT of
/// it legitimately can when `otherMails` names one.
fn directory_additional_emails(
    primary: &str,
    other_mails: Option<Vec<String>>,
    proxy_addresses: Option<Vec<String>>,
) -> Vec<String> {
    // Universal set, seeded with the primary so no list can restate it.
    let mut seen = vec![address_fold_key(primary)];
    // Proxy-only set, whole-address folded, fed by every proxy entry
    // considered - emitted or suppressed.
    let mut proxy_seen: Vec<String> = Vec::new();
    let mut out = Vec::new();
    let others = other_mails.unwrap_or_default();
    let proxies = proxy_addresses.unwrap_or_default();

    for candidate in others.iter().map(|mail| mail.trim()) {
        if candidate.is_empty() {
            continue;
        }
        let key = address_fold_key(candidate);
        if seen.contains(&key) {
            continue;
        }
        seen.push(key);
        out.push(candidate.to_string());
    }

    for (kind, candidate) in proxies.iter().filter_map(|entry| smtp_proxy_address(entry)) {
        let proxy_key = candidate.to_ascii_lowercase();
        let key = address_fold_key(candidate);
        // The primary proxy is the same mailbox as `mail` only when the
        // values actually match; guest accounts are the documented case
        // where they do not.
        let restates_primary =
            kind == ProxyKind::Primary && candidate.eq_ignore_ascii_case(primary);
        let duplicate = restates_primary || proxy_seen.contains(&proxy_key) || seen.contains(&key);
        // Recorded even when suppressed, so the proxy-only fold sees the
        // whole proxy list rather than only its emitted part.
        if !proxy_seen.contains(&proxy_key) {
            proxy_seen.push(proxy_key);
        }
        if duplicate {
            continue;
        }
        seen.push(key);
        out.push(candidate.to_string());
    }
    out
}

/// Project one `/users` directory row into a `DirectoryCard`. Returns
/// `None` when `mail` is absent or empty (matching ratatoskr's mail-less
/// drop). `additional_emails` carries `otherMails` plus the SMTP
/// `proxyAddresses`, de-duplicated against `mail` - see
/// `directory_additional_emails`.
fn directory_user_to_card(user: GraphDirectoryUser) -> Option<DirectoryCard> {
    let email = user.mail.filter(|mail| !mail.is_empty())?;
    let additional_emails =
        directory_additional_emails(&email, user.other_mails, user.proxy_addresses);
    Some(DirectoryCard {
        email,
        display_name: user.display_name,
        additional_emails,
        phones: user.business_phones.unwrap_or_default(),
        company: user.company_name,
        title: user.job_title,
        department: user.department,
        provider: ProtocolKind::Graph,
    })
}

pub(crate) async fn get_page<T: serde::de::DeserializeOwned>(
    account: &GraphAccount,
    url: &AdmittedUrl,
    operation: AccountOperation,
) -> Result<ODataCollection<T>, AccountError> {
    account
        .client
        .get(url)
        .await
        .map_err(|error| into_error(error, operation))
}

/// Admit a page's `@odata.nextLink` at receipt, before it is followed or
/// minted into the caller's cursor.
pub(crate) fn admit_next<T>(
    account: &GraphAccount,
    page: &ODataCollection<T>,
    operation: AccountOperation,
) -> Result<Option<AdmittedUrl>, AccountError> {
    account
        .client
        .admit_next(page.next_link.as_ref())
        .map_err(|error| into_error(error, operation))
}

/// A list surface's `next_cursor`: the admitted link's bytes.
pub(crate) fn link_cursor(next: Option<AdmittedUrl>) -> Option<Vec<u8>> {
    next.map(|next| next.as_str().as_bytes().to_vec())
}

/// Decode a caller's bare link cursor; a refusal is the caller's malformed
/// input.
pub(crate) fn decode_link_cursor(
    account: &GraphAccount,
    page_cursor: Option<Vec<u8>>,
    operation: AccountOperation,
) -> Result<Option<AdmittedUrl>, AccountError> {
    crate::paging::decode_link_cursor(&account.client, page_cursor)
        .map_err(|detail| graph_error::invalid_account_error(operation, detail))
}

fn decode_paged_cursor(
    account: &GraphAccount,
    page_cursor: Option<Vec<u8>>,
    operation: AccountOperation,
) -> Result<Option<(AdmittedUrl, usize)>, AccountError> {
    crate::paging::decode_paged_cursor(&account.client, page_cursor)
        .map_err(|detail| graph_error::invalid_account_error(operation, detail))
}

fn contacts_url(account: &GraphAccount, address_book: Option<&AddressBookId>, top: u32) -> String {
    let prefix = account.client.api_path_prefix();
    contacts_path(&prefix, address_book, top)
}

fn contact_search_url(
    account: &GraphAccount,
    address_book: Option<&AddressBookId>,
    query: &str,
    top: u32,
) -> String {
    let prefix = account.client.api_path_prefix();
    contact_search_path(&prefix, address_book, query, top)
}

fn contacts_path(prefix: &str, address_book: Option<&AddressBookId>, top: u32) -> String {
    match address_book.map(|id| id.0.as_str()) {
        Some(DEFAULT_CONTACTS_ID) | None => {
            format!("{prefix}/contacts?$select={CONTACT_SELECT}&$top={top}")
        }
        Some(folder) => {
            let encoded = bifrost_net::url::encode_path_component(folder);
            format!(
                "{prefix}/contactFolders/{encoded}/contacts?$select={CONTACT_SELECT}&$top={top}"
            )
        }
    }
}

fn contact_search_path(
    prefix: &str,
    address_book: Option<&AddressBookId>,
    query: &str,
    top: u32,
) -> String {
    let base = contacts_path(prefix, address_book, top);
    let trimmed = query.trim();
    if !is_exact_email_query(trimmed) {
        return base;
    }
    let escaped = trimmed.replace('\'', "''");
    let filter = format!("emailAddresses/any(a:a/address eq '{escaped}')");
    format!(
        "{base}&$filter={}",
        bifrost_net::url::encode_query_value(&filter)
    )
}

fn is_exact_email_query(query: &str) -> bool {
    !query.is_empty() && query.contains('@') && !query.chars().any(char::is_whitespace)
}

/// Resolve the Graph `$top` page size from a caller `limit`. Floors at 1
/// (a `limit=Some(0)` would otherwise request `$top=0` and fetch nothing)
/// and caps at the provider ceiling. The local truncation cap floors
/// separately on the consuming side.
fn top_for_limit(limit: Option<u32>, ceiling: u32) -> u32 {
    limit.unwrap_or(250).clamp(1, ceiling)
}

fn create_url(account: &GraphAccount, address_book: Option<&AddressBookId>) -> String {
    let prefix = account.client.api_path_prefix();
    match address_book.map(|id| id.0.as_str()) {
        Some(DEFAULT_CONTACTS_ID) | None => format!("{prefix}/contacts"),
        Some(folder) => {
            let encoded = bifrost_net::url::encode_path_component(folder);
            format!("{prefix}/contactFolders/{encoded}/contacts")
        }
    }
}

fn folder_to_address_book(folder: GraphContactFolder) -> AddressBook {
    AddressBook {
        id: AddressBookId(folder.id.clone()),
        native_id: folder.id.clone(),
        name: folder.display_name.unwrap_or(folder.id.clone()),
        provenance: ContactProvenance {
            provider: ProtocolKind::Graph,
            native: folder.id,
            address_book_native: None,
        },
        corpus: ContactCorpus::Main,
        is_default: false,
        can_create_contacts: true,
        can_update_contacts: true,
        can_delete_contacts: true,
    }
}

fn contact_from_graph(contact: GraphContact) -> ContactCard {
    let addresses = graph_addresses(&contact);
    let mut phones = contact
        .business_phones
        .unwrap_or_default()
        .into_iter()
        .filter(|value| !value.is_empty())
        .map(|value| ContactPhone {
            value,
            kind: Some("business".to_string()),
            is_primary: false,
        })
        .collect::<Vec<_>>();
    phones.extend(
        contact
            .home_phones
            .unwrap_or_default()
            .into_iter()
            .filter(|value| !value.is_empty())
            .map(|value| ContactPhone {
                value,
                kind: Some("home".to_string()),
                is_primary: false,
            }),
    );
    if let Some(mobile) = contact.mobile_phone.filter(|value| !value.is_empty()) {
        phones.push(ContactPhone {
            value: mobile,
            kind: Some("mobile".to_string()),
            is_primary: phones.is_empty(),
        });
    }
    ContactCard {
        id: ContactId(contact.id.clone()),
        address_book_id: contact.parent_folder_id.clone().map(AddressBookId),
        native_id: contact.id.clone(),
        etag: contact.etag,
        provenance: ContactProvenance {
            provider: ProtocolKind::Graph,
            native: contact.id,
            address_book_native: contact.parent_folder_id,
        },
        // Graph has no auto-collected corpus; every contact is personal.
        corpus: ContactCorpus::Main,
        display_name: contact.display_name,
        emails: contact
            .email_addresses
            .unwrap_or_default()
            .into_iter()
            .filter_map(|email| {
                Some(ContactEmail {
                    value: email.address?,
                    // ACCEPTED CONFLATION. `ContactEmail.kind` is a type
                    // label ("work", "home"); Graph's `emailAddress.name`
                    // is a display name for the address. Graph's contact
                    // schema carries no per-address type at all, so there is
                    // nothing better to map onto, and the alternative -
                    // dropping `kind` on the Graph backend - would lose the
                    // only per-address string the provider round-trips.
                    // The round trip IS consistent (`graph_contact_from_*`
                    // writes `kind` straight back into `name`, so nothing is
                    // lost or rewritten), it is only semantically wrong: a
                    // consumer must not read a Graph contact's `kind` as a
                    // type label, and a `kind` written here surfaces as the
                    // display name in Outlook. Revisit only if Graph grows a
                    // typed address field.
                    kind: email.name,
                    is_primary: false,
                })
            })
            .collect(),
        phones,
        organizations: contact
            .company_name
            .map(|name| {
                vec![ContactOrganization {
                    name,
                    title: contact.job_title,
                }]
            })
            .unwrap_or_default(),
        addresses,
        notes: contact.personal_notes,
        photo_url: None,
        photo: None,
    }
}

fn graph_contact_from_create(contact: &ContactCreate) -> GraphContactPatch {
    GraphContactPatch {
        display_name: contact.display_name.clone(),
        email_addresses: non_empty(contact.emails.iter().map(|email| GraphContactEmail {
            // The outbound half of the accepted `kind` <-> `name` conflation
            // documented at the inbound projection in `contact_from_graph`.
            name: email.kind.clone(),
            address: Some(email.value.clone()),
        })),
        business_phones: non_empty(
            contact
                .phones
                .iter()
                .filter(|phone| !matches!(phone.kind.as_deref(), Some("mobile") | Some("home")))
                .map(|phone| phone.value.clone()),
        ),
        home_phones: non_empty(
            contact
                .phones
                .iter()
                .filter(|phone| phone.kind.as_deref() == Some("home"))
                .map(|phone| phone.value.clone()),
        ),
        mobile_phone: contact
            .phones
            .iter()
            .find(|phone| phone.kind.as_deref() == Some("mobile"))
            .map(|phone| phone.value.clone()),
        home_address: graph_address_from_shared(contact, "home"),
        business_address: graph_address_from_shared(contact, "work"),
        other_address: contact
            .addresses
            .iter()
            .find(|address| !matches!(address.kind.as_deref(), Some("home" | "work")))
            .map(graph_address),
        company_name: contact.organizations.first().map(|org| org.name.clone()),
        job_title: contact
            .organizations
            .first()
            .and_then(|org| org.title.clone()),
        personal_notes: contact.notes.clone(),
    }
}

fn graph_addresses(contact: &GraphContact) -> Vec<ContactAddress> {
    [
        ("home", contact.home_address.as_ref()),
        ("work", contact.business_address.as_ref()),
        ("other", contact.other_address.as_ref()),
    ]
    .into_iter()
    .filter_map(|(kind, address)| {
        let address = address?;
        Some(ContactAddress {
            kind: Some(kind.to_string()),
            formatted: None,
            street: address
                .street
                .as_deref()
                .map(|street| street.lines().map(ToString::to_string).collect())
                .unwrap_or_default(),
            locality: address.city.clone(),
            region: address.state.clone(),
            postal_code: address.postal_code.clone(),
            country: address.country_or_region.clone(),
            is_primary: false,
        })
    })
    .collect()
}

fn graph_address_from_shared(contact: &ContactCreate, kind: &str) -> Option<GraphPhysicalAddress> {
    contact
        .addresses
        .iter()
        .find(|address| address.kind.as_deref() == Some(kind))
        .map(graph_address)
}

fn graph_address(address: &ContactAddress) -> GraphPhysicalAddress {
    GraphPhysicalAddress {
        street: (!address.street.is_empty()).then(|| address.street.join("\n")),
        city: address.locality.clone(),
        state: address.region.clone(),
        postal_code: address.postal_code.clone(),
        country_or_region: address.country.clone(),
    }
}

/// Build a sparse Graph PATCH body from a `ContactPatch`.
///
/// Graph PATCH leaves omitted properties untouched. A `None` field on
/// the patch is omitted; a `Some(None)` scalar clear emits JSON `null`
/// so the server actually drops the old value; a present repeated field
/// replaces the corresponding Graph property (or properties) wholesale,
/// emitting `null` / `[]` for buckets the new collection leaves empty.
fn graph_contact_from_patch(patch: &ContactPatch) -> GraphContactPatchBody {
    let mut body = GraphContactPatchBody::default();
    if let Some(display_name) = &patch.display_name {
        body.display_name = Some(scalar_or_null(display_name.clone()));
    }
    if let Some(notes) = &patch.notes {
        body.personal_notes = Some(scalar_or_null(notes.clone()));
    }
    if let Some(emails) = &patch.emails {
        let addresses = emails
            .iter()
            .map(|email| GraphContactEmail {
                // Same accepted `kind` <-> `name` conflation as the create
                // path; see `contact_from_graph`.
                name: email.kind.clone(),
                address: Some(email.value.clone()),
            })
            .collect::<Vec<_>>();
        body.email_addresses = Some(json!(addresses));
    }
    if let Some(phones) = &patch.phones {
        body.business_phones = Some(json!(
            phones
                .iter()
                .filter(|phone| !matches!(phone.kind.as_deref(), Some("mobile") | Some("home")))
                .map(|phone| phone.value.clone())
                .collect::<Vec<_>>()
        ));
        body.home_phones = Some(json!(
            phones
                .iter()
                .filter(|phone| phone.kind.as_deref() == Some("home"))
                .map(|phone| phone.value.clone())
                .collect::<Vec<_>>()
        ));
        body.mobile_phone = Some(
            phones
                .iter()
                .find(|phone| phone.kind.as_deref() == Some("mobile"))
                .map(|phone| Value::String(phone.value.clone()))
                .unwrap_or(Value::Null),
        );
    }
    if let Some(organizations) = &patch.organizations {
        let first = organizations.first();
        body.company_name = Some(
            first
                .map(|org| Value::String(org.name.clone()))
                .unwrap_or(Value::Null),
        );
        body.job_title = Some(
            first
                .and_then(|org| org.title.clone())
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
    }
    if let Some(addresses) = &patch.addresses {
        body.home_address = Some(address_bucket(addresses, "home"));
        body.business_address = Some(address_bucket(addresses, "work"));
        body.other_address = Some(
            addresses
                .iter()
                .find(|address| !matches!(address.kind.as_deref(), Some("home" | "work")))
                .map(graph_address)
                .map(|address| json!(address))
                .unwrap_or(Value::Null),
        );
    }
    body
}

fn scalar_or_null(value: Option<String>) -> Value {
    value.map(Value::String).unwrap_or(Value::Null)
}

fn address_bucket(addresses: &[ContactAddress], kind: &str) -> Value {
    addresses
        .iter()
        .find(|address| address.kind.as_deref() == Some(kind))
        .map(graph_address)
        .map(|address| json!(address))
        .unwrap_or(Value::Null)
}

fn non_empty<T>(iter: impl Iterator<Item = T>) -> Option<Vec<T>> {
    let values = iter.collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
}

fn contact_matches(contact: &ContactCard, needle: &str) -> bool {
    needle.is_empty()
        || contact
            .display_name
            .as_deref()
            .is_some_and(|value| contains(value, needle))
        || contact
            .emails
            .iter()
            .any(|email| contains(&email.value, needle))
        || contact
            .phones
            .iter()
            .any(|phone| contains(&phone.value, needle))
        || contact.organizations.iter().any(|org| {
            contains(&org.name, needle)
                || org
                    .title
                    .as_deref()
                    .is_some_and(|title| contains(title, needle))
        })
        || contact.addresses.iter().any(|address| {
            address
                .formatted
                .as_deref()
                .is_some_and(|value| contains(value, needle))
                || address.street.iter().any(|value| contains(value, needle))
                || address
                    .locality
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .region
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .postal_code
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .country
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
        })
}

fn contains(value: &str, needle: &str) -> bool {
    value.to_ascii_lowercase().contains(needle)
}

fn into_error(error: crate::error::GraphError, operation: AccountOperation) -> AccountError {
    graph_error::into_account_error(error, GraphErrorContext::graph(operation))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphContactFolder {
    id: String,
    display_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphDirectoryUser {
    display_name: Option<String>,
    mail: Option<String>,
    other_mails: Option<Vec<String>>,
    proxy_addresses: Option<Vec<String>>,
    business_phones: Option<Vec<String>>,
    company_name: Option<String>,
    job_title: Option<String>,
    department: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphContact {
    id: String,
    #[serde(rename = "@odata.etag")]
    etag: Option<String>,
    display_name: Option<String>,
    email_addresses: Option<Vec<GraphContactEmail>>,
    business_phones: Option<Vec<String>>,
    home_phones: Option<Vec<String>>,
    mobile_phone: Option<String>,
    home_address: Option<GraphPhysicalAddress>,
    business_address: Option<GraphPhysicalAddress>,
    other_address: Option<GraphPhysicalAddress>,
    company_name: Option<String>,
    job_title: Option<String>,
    personal_notes: Option<String>,
    parent_folder_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphContactEmail {
    name: Option<String>,
    address: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphPhysicalAddress {
    street: Option<String>,
    city: Option<String>,
    state: Option<String>,
    postal_code: Option<String>,
    country_or_region: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphContactPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email_addresses: Option<Vec<GraphContactEmail>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    business_phones: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    home_phones: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mobile_phone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    home_address: Option<GraphPhysicalAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    business_address: Option<GraphPhysicalAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    other_address: Option<GraphPhysicalAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    company_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    personal_notes: Option<String>,
}

/// Sparse PATCH body for contact updates.
///
/// Distinct from `GraphContactPatch` (used on create) because update
/// must express scalar clears as JSON `null`. `None` omits the property
/// from the PATCH (untouched); `Some(Value::Null)` clears it on the
/// server; any other `Some` sets it.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphContactPatchBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email_addresses: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    business_phones: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    home_phones: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mobile_phone: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    home_address: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    business_address: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    other_address: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    company_name: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_title: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    personal_notes: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_for_limit_floors_zero_to_one() {
        // limit=Some(0) must not produce $top=0 (which fetches nothing).
        assert_eq!(top_for_limit(Some(0), 250), 1);
        assert_eq!(top_for_limit(Some(10), 250), 10);
        assert_eq!(top_for_limit(Some(9999), 250), 250);
        assert_eq!(top_for_limit(None, 999), 250);
    }

    /// A page can over-deliver: Graph returns `$top` rows and several of
    /// them match, so the limit is reached PART WAY through a page. The
    /// walk used to hand back the page's `@odata.nextLink`, silently
    /// dropping every later match in the page it had already read. The
    /// resume cursor must point back INTO that page and the second call
    /// must return the matches the first one stopped short of.
    #[tokio::test]
    async fn a_search_resumes_inside_an_over_delivered_page_without_losing_matches() {
        fn row(id: &str) -> serde_json::Value {
            serde_json::json!({ "id": id, "displayName": "Ada Match" })
        }
        let page = serde_json::json!({
            "value": [row("c1"), row("c2"), row("c3")],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/next"
        });
        let client = crate::client::GraphClient::new("token");
        client.script_rest([
            crate::client::ScriptedRestResponse::json(reqwest::StatusCode::OK, page.clone()),
            crate::client::ScriptedRestResponse::json(reqwest::StatusCode::OK, page),
        ]);
        let account =
            GraphAccount::new_for_tests(client.clone(), super::super::PushMode::GraphSubscriptions);

        let first = search(
            account.clone(),
            ContactSearchRequest {
                limit: Some(1),
                ..ContactSearchRequest::new("ada")
            },
        )
        .await
        .expect("first page");
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].id.0, "c1");
        let cursor = first.next_cursor.expect("two matches remain in this page");

        let second = search(
            account,
            ContactSearchRequest {
                limit: Some(1),
                page_cursor: Some(cursor),
                ..ContactSearchRequest::new("ada")
            },
        )
        .await
        .expect("second page");
        assert_eq!(
            second.items[0].id.0, "c2",
            "the resume picks up the match the first call stopped before, \
             not the first row of the NEXT page"
        );
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].url, requests[1].url,
            "the resume re-reads the same page rather than following nextLink"
        );
    }

    fn test_account() -> (crate::client::GraphClient, GraphAccount) {
        let client = crate::client::GraphClient::new("token");
        let account =
            GraphAccount::new_for_tests(client.clone(), super::super::PushMode::GraphSubscriptions);
        (client, account)
    }

    /// The token leak: a caller's page cursor naming another origin was
    /// fetched verbatim with the account bearer. Every cursor-taking list
    /// and search surface refuses it before any request, as the caller's
    /// malformed input.
    #[tokio::test]
    async fn a_foreign_origin_page_cursor_is_refused_before_any_request() {
        let foreign = b"https://elsewhere.example/v1.0/me/contacts?$skiptoken=x".to_vec();
        let paged = br#"{"url":"HTTPS://elsewhere.example/v1.0/users","skip":1}"#.to_vec();
        let (client, account) = test_account();
        client.script_rest([]);

        let errors = [
            list(account.clone(), None, Some(foreign.clone()))
                .await
                .expect_err("contacts list"),
            search(
                account.clone(),
                ContactSearchRequest {
                    page_cursor: Some(paged.clone()),
                    ..ContactSearchRequest::new("ada")
                },
            )
            .await
            .expect_err("contact search"),
            directory_search(account.clone(), "ada".to_string(), None, Some(paged))
                .await
                .expect_err("directory search"),
            super::super::groups::directory_groups_list(account.clone(), Some(foreign.clone()))
                .await
                .expect_err("groups list"),
            super::super::calendar::events_in_range(
                account,
                bifrost_types::EventRange {
                    calendar_id: bifrost_types::CalendarId("calendar".to_string()),
                    start: bifrost_types::EventTime {
                        value: "2026-01-01T00:00:00Z".to_string(),
                        timezone: None,
                    },
                    end: bifrost_types::EventTime {
                        value: "2026-02-01T00:00:00Z".to_string(),
                        timezone: None,
                    },
                    page_cursor: Some(foreign),
                    limit: None,
                },
            )
            .await
            .expect_err("events in range"),
        ];
        for error in errors {
            assert!(
                matches!(
                    error.kind(),
                    bifrost_types::AccountErrorKind::Request(
                        bifrost_types::RequestErrorKind::Malformed
                    )
                ),
                "{:?}",
                error.kind()
            );
        }
        assert_eq!(client.wire_attempts(), 0, "nothing may be sent");
    }

    /// A server `@odata.nextLink` on another origin is refused at receipt:
    /// it is neither followed nor handed to the caller as a cursor, and the
    /// refusal blames the response that carried it.
    #[tokio::test]
    async fn a_foreign_origin_next_link_is_refused_at_receipt() {
        let (client, account) = test_account();
        client.script_rest([
            crate::client::ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                serde_json::json!({
                    "value": [{ "id": "c1", "displayName": "Ada" }],
                    "@odata.nextLink": "https://elsewhere.example/v1.0/me/contacts?$skiptoken=x"
                }),
            ),
            crate::client::ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                serde_json::json!({
                    "value": [],
                    "@odata.nextLink": "https://graph.microsoft.com@elsewhere.example/v1.0/x"
                }),
            ),
        ]);

        for error in [
            list(account.clone(), None, None)
                .await
                .expect_err("a foreign next link must not become the caller's cursor"),
            address_books_list(account)
                .await
                .expect_err("a foreign next link must not be followed"),
        ] {
            assert!(
                matches!(
                    error.kind(),
                    bifrost_types::AccountErrorKind::Protocol(
                        bifrost_types::ProtocolErrorKind::ContractViolation
                    )
                ),
                "{:?}",
                error.kind()
            );
            assert!(
                format!("{error:?}").contains("Acknowledged"),
                "the provider answered: {error:?}"
            );
        }
        assert_eq!(
            client.wire_attempts(),
            2,
            "one request per surface, nothing followed"
        );
    }

    #[test]
    fn graph_contact_maps_to_contact_card() {
        let card = contact_from_graph(GraphContact {
            id: "contact-1".to_string(),
            etag: Some("etag-1".to_string()),
            display_name: Some("Ada Lovelace".to_string()),
            email_addresses: Some(vec![GraphContactEmail {
                name: Some("work".to_string()),
                address: Some("ada@example.test".to_string()),
            }]),
            business_phones: Some(vec!["+123".to_string()]),
            home_phones: Some(vec!["+124".to_string()]),
            mobile_phone: Some("+456".to_string()),
            home_address: Some(GraphPhysicalAddress {
                street: Some("1 Example St\nUnit 2".to_string()),
                city: Some("London".to_string()),
                state: Some("England".to_string()),
                postal_code: Some("N1".to_string()),
                country_or_region: Some("UK".to_string()),
            }),
            business_address: None,
            other_address: None,
            company_name: Some("Analytical Engines".to_string()),
            job_title: Some("Programmer".to_string()),
            personal_notes: Some("notes".to_string()),
            parent_folder_id: Some("folder-1".to_string()),
        });

        assert_eq!(card.id.0, "contact-1");
        assert_eq!(card.display_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(card.etag.as_deref(), Some("etag-1"));
        assert_eq!(card.emails[0].kind.as_deref(), Some("work"));
        assert_eq!(card.phones[0].kind.as_deref(), Some("business"));
        assert_eq!(card.phones[1].kind.as_deref(), Some("home"));
        assert_eq!(card.phones[2].kind.as_deref(), Some("mobile"));
        assert_eq!(card.addresses[0].kind.as_deref(), Some("home"));
        assert_eq!(card.addresses[0].street, vec!["1 Example St", "Unit 2"]);
        assert_eq!(card.addresses[0].locality.as_deref(), Some("London"));
        assert_eq!(card.organizations[0].title.as_deref(), Some("Programmer"));
        assert_eq!(card.notes.as_deref(), Some("notes"));
    }

    #[test]
    fn create_payload_maps_contact_fields() {
        let payload = graph_contact_from_create(&ContactCreate {
            display_name: Some("Ada".to_string()),
            emails: vec![ContactEmail {
                value: "ada@example.test".to_string(),
                kind: Some("work".to_string()),
                is_primary: true,
            }],
            phones: vec![
                ContactPhone {
                    value: "+456".to_string(),
                    kind: Some("mobile".to_string()),
                    is_primary: true,
                },
                ContactPhone {
                    value: "+123".to_string(),
                    kind: Some("home".to_string()),
                    is_primary: false,
                },
            ],
            notes: Some("notes".to_string()),
            addresses: vec![ContactAddress {
                kind: Some("work".to_string()),
                formatted: None,
                street: vec!["1 Analytical Way".to_string()],
                locality: Some("London".to_string()),
                region: None,
                postal_code: Some("N1".to_string()),
                country: Some("UK".to_string()),
                is_primary: false,
            }],
            ..ContactCreate::default()
        });

        assert_eq!(payload.display_name.as_deref(), Some("Ada"));
        assert_eq!(
            payload.email_addresses.unwrap()[0].address.as_deref(),
            Some("ada@example.test")
        );
        assert_eq!(payload.mobile_phone.as_deref(), Some("+456"));
        assert_eq!(
            payload
                .business_address
                .as_ref()
                .and_then(|address| address.street.as_deref()),
            Some("1 Analytical Way")
        );
        assert_eq!(payload.home_phones.unwrap()[0], "+123");
        assert_eq!(payload.personal_notes.as_deref(), Some("notes"));
    }

    #[test]
    fn graph_contact_patch_emits_null_for_scalar_clears() {
        let body = graph_contact_from_patch(&ContactPatch {
            display_name: Some(None),
            notes: Some(None),
            organizations: Some(Vec::new()),
            ..ContactPatch::default()
        });
        let value = serde_json::to_value(&body).expect("patch json");

        assert!(value.get("displayName").is_some_and(Value::is_null));
        assert!(value.get("personalNotes").is_some_and(Value::is_null));
        assert!(value.get("companyName").is_some_and(Value::is_null));
        assert!(value.get("jobTitle").is_some_and(Value::is_null));
    }

    #[test]
    fn graph_contact_patch_omits_untouched_fields() {
        let body = graph_contact_from_patch(&ContactPatch {
            display_name: Some(Some("Ada".to_string())),
            ..ContactPatch::default()
        });
        let value = serde_json::to_value(&body).expect("patch json");

        assert_eq!(
            value.get("displayName").and_then(Value::as_str),
            Some("Ada")
        );
        assert!(value.get("personalNotes").is_none());
        assert!(value.get("emailAddresses").is_none());
        assert!(value.get("mobilePhone").is_none());
    }

    #[test]
    fn graph_contact_patch_replaces_collections_and_clears_empty_buckets() {
        let body = graph_contact_from_patch(&ContactPatch {
            emails: Some(vec![ContactEmail {
                value: "ada@example.test".to_string(),
                kind: Some("work".to_string()),
                is_primary: false,
            }]),
            phones: Some(vec![ContactPhone {
                value: "+123".to_string(),
                kind: Some("home".to_string()),
                is_primary: false,
            }]),
            addresses: Some(Vec::new()),
            ..ContactPatch::default()
        });
        let value = serde_json::to_value(&body).expect("patch json");

        assert_eq!(
            value["emailAddresses"][0]["address"].as_str(),
            Some("ada@example.test")
        );
        assert_eq!(value["homePhones"][0].as_str(), Some("+123"));
        assert_eq!(value["businessPhones"], json!([]));
        assert!(value.get("mobilePhone").is_some_and(Value::is_null));
        assert!(value.get("homeAddress").is_some_and(Value::is_null));
        assert!(value.get("businessAddress").is_some_and(Value::is_null));
        assert!(value.get("otherAddress").is_some_and(Value::is_null));
    }

    #[test]
    fn contacts_path_uses_requested_page_size() {
        assert_eq!(
            contacts_path("/me", None, 25),
            format!("/me/contacts?$select={CONTACT_SELECT}&$top=25")
        );
        assert_eq!(
            contacts_path(
                "/me",
                Some(&AddressBookId("folder with spaces".to_string())),
                10
            ),
            format!(
                "/me/contactFolders/folder%20with%20spaces/contacts?$select={CONTACT_SELECT}&$top=10"
            )
        );
    }

    #[test]
    fn contact_search_path_filters_exact_email_queries() {
        assert_eq!(
            contact_search_path("/me", None, "ada@example.test", 25),
            format!(
                "/me/contacts?$select={CONTACT_SELECT}&$top=25&$filter=emailAddresses%2Fany%28a%3Aa%2Faddress%20eq%20%27ada%40example.test%27%29"
            )
        );
        assert_eq!(
            contact_search_path(
                "/me",
                Some(&AddressBookId("folder".to_string())),
                "ada@example.test",
                10
            ),
            format!(
                "/me/contactFolders/folder/contacts?$select={CONTACT_SELECT}&$top=10&$filter=emailAddresses%2Fany%28a%3Aa%2Faddress%20eq%20%27ada%40example.test%27%29"
            )
        );
    }

    #[test]
    fn contact_search_path_keeps_local_scan_for_substring_queries() {
        assert_eq!(
            contact_search_path("/me", None, "ada", 25),
            format!("/me/contacts?$select={CONTACT_SELECT}&$top=25")
        );
        assert_eq!(
            contact_search_path("/me", None, "ada lovelace@example.test", 25),
            format!("/me/contacts?$select={CONTACT_SELECT}&$top=25")
        );
    }

    #[test]
    fn directory_search_url_escapes_query_into_startswith_filter() {
        let path = directory_search_path("O'Hara", 50);
        assert!(path.starts_with(&format!("/users?$select={DIRECTORY_SELECT}&$top=50")));
        // The single quote is doubled, and the whole filter is URL-encoded.
        let filter = "startswith(displayName,'O''Hara') or startswith(mail,'O''Hara')";
        assert!(path.contains(&format!(
            "&$filter={}",
            bifrost_net::url::encode_query_value(filter)
        )));
    }

    #[test]
    fn directory_search_empty_query_omits_filter() {
        assert_eq!(
            directory_search_path("", 999),
            format!("/users?$select={DIRECTORY_SELECT}&$top=999")
        );
        // Whitespace-only is treated as empty.
        assert_eq!(
            directory_search_path("   ", 999),
            format!("/users?$select={DIRECTORY_SELECT}&$top=999")
        );
    }

    #[test]
    fn directory_user_to_card_drops_mailless_and_maps_fields() {
        // No mail -> dropped.
        let mailless = GraphDirectoryUser {
            display_name: Some("No Mail".to_string()),
            mail: None,
            other_mails: None,
            proxy_addresses: None,
            business_phones: None,
            company_name: None,
            job_title: None,
            department: None,
        };
        assert!(directory_user_to_card(mailless).is_none());

        let user = GraphDirectoryUser {
            display_name: Some("Ada Lovelace".to_string()),
            mail: Some("ada@example.test".to_string()),
            other_mails: None,
            proxy_addresses: None,
            business_phones: Some(vec!["+15551234".to_string()]),
            company_name: Some("Analytical Engines".to_string()),
            job_title: Some("Programmer".to_string()),
            department: Some("Research".to_string()),
        };
        let card = directory_user_to_card(user).expect("maps to card");
        assert_eq!(card.email, "ada@example.test");
        assert_eq!(card.display_name.as_deref(), Some("Ada Lovelace"));
        assert!(card.additional_emails.is_empty());
        assert_eq!(card.phones, vec!["+15551234".to_string()]);
        assert_eq!(card.company.as_deref(), Some("Analytical Engines"));
        assert_eq!(card.title.as_deref(), Some("Programmer"));
        assert_eq!(card.department.as_deref(), Some("Research"));
        assert_eq!(card.provider, ProtocolKind::Graph);
    }

    #[test]
    fn directory_select_requests_the_alias_fields() {
        // Projecting a field nobody selected is a silent no-op: Graph omits
        // both of these from a /users row unless they are asked for.
        assert!(DIRECTORY_SELECT.contains("otherMails"));
        assert!(DIRECTORY_SELECT.contains("proxyAddresses"));
        assert!(directory_search_path("", 999).contains("otherMails,proxyAddresses"));
    }

    #[test]
    fn directory_card_collects_aliases_from_a_canned_users_page() {
        // One canned Graph /users response carrying the whole shape:
        // the primary repeated as SMTP:, secondaries, non-SMTP routing
        // identifiers, an otherMails/proxyAddresses overlap, a local-part
        // case variant on each side, and an entry with no type prefix.
        let body = serde_json::json!({
            "value": [{
                "displayName": "Ada Lovelace",
                "mail": "ada@example.test",
                "otherMails": ["ada.personal@elsewhere.test", "shared@example.test"],
                "proxyAddresses": [
                    "SMTP:ada@example.test",
                    "smtp:a.lovelace@example.test",
                    "X500:/o=Contoso/ou=Exchange/cn=ada",
                    "SIP:ada@example.test",
                    "smtp:SHARED@example.test",
                    "smtp:A.Lovelace@example.test",
                    "smtp:",
                    "ada.bare@example.test"
                ],
                "businessPhones": ["+15551234"],
                "companyName": "Analytical Engines",
                "jobTitle": "Programmer",
                "department": "Research"
            }]
        });
        let page: ODataCollection<GraphDirectoryUser> =
            serde_json::from_value(body).expect("deserializes");
        let user = page.value.into_iter().next().expect("one row");
        let card = directory_user_to_card(user).expect("maps to card");

        assert_eq!(card.email, "ada@example.test");
        assert_eq!(
            card.additional_emails,
            vec![
                // otherMails in server order first, then SMTP proxies.
                "ada.personal@elsewhere.test".to_string(),
                "shared@example.test".to_string(),
                "a.lovelace@example.test".to_string(),
                // A local-part case variant is a possibly-distinct mailbox
                // and survives; the whole-address variant that follows it
                // inside proxyAddresses does not, because Exchange refuses
                // two proxy entries differing only by case.
                "SHARED@example.test".to_string(),
            ]
        );
        // The literal primary string never repeats, whichever list it came
        // from.
        assert!(!card.additional_emails.contains(&card.email));
        // X500/SIP are directory identifiers, not addresses.
        assert!(
            !card
                .additional_emails
                .iter()
                .any(|mail| mail.contains("X500") || mail.contains("/o="))
        );
    }

    #[test]
    fn smtp_proxy_address_reads_only_the_smtp_types_and_keeps_the_kind() {
        // Both SMTP casings are addresses; the prefix distinguishes primary
        // from alias, not address from non-address, and that distinction
        // has to survive parsing - nothing downstream can recover it.
        assert_eq!(
            smtp_proxy_address("SMTP:primary@example.test"),
            Some((ProxyKind::Primary, "primary@example.test"))
        );
        assert_eq!(
            smtp_proxy_address("smtp:alias@example.test"),
            Some((ProxyKind::Alias, "alias@example.test"))
        );
        // Only an exactly upper-case prefix asserts "primary".
        assert_eq!(
            smtp_proxy_address("Smtp:mixed@example.test"),
            Some((ProxyKind::Alias, "mixed@example.test"))
        );
        for entry in [
            "X500:/o=Contoso/ou=Exchange/cn=Recipients/cn=ada",
            "x500:/o=Contoso",
            "SIP:ada@example.test",
            "EUM:12345;phone-context=x",
            "SPO:SPO_abc@SPO_def",
        ] {
            assert_eq!(smtp_proxy_address(entry), None, "kept non-SMTP {entry}");
        }
        // No type prefix at all, and an empty value, both drop silently.
        assert_eq!(smtp_proxy_address("bare@example.test"), None);
        assert_eq!(smtp_proxy_address("smtp:"), None);
        assert_eq!(smtp_proxy_address("smtp:   "), None);
    }

    #[test]
    fn universal_dedup_folds_the_domain_and_keeps_local_part_case() {
        // RFC 5321 s2.4: the local part's case is preserved and its meaning
        // belongs to the destination host, so two otherMails entries that
        // differ only there may be two mailboxes on an external system.
        // Dropping one is unrecoverable here; emitting both is visible and
        // collapsible upstream, so both are kept.
        let emails = directory_additional_emails(
            "Ada@Example.TEST",
            Some(vec![
                "User@external.example".to_string(),
                "user@external.example".to_string(),
                // Domain-only case difference: one address, folded away.
                "user@EXTERNAL.example".to_string(),
                // A local-part variant of the primary is likewise kept.
                "ada@example.test".to_string(),
            ]),
            None,
        );
        assert_eq!(
            emails,
            vec![
                "User@external.example".to_string(),
                "user@external.example".to_string(),
                "ada@example.test".to_string(),
            ]
        );
    }

    #[test]
    fn proxy_entries_fold_on_the_whole_address() {
        // Exchange refuses two proxy entries differing only by case, so a
        // whole-address fold BETWEEN proxies can only collapse what the
        // provider already calls one address.
        //
        // Scope note, because the name used to claim more than this pins: with
        // no `otherMails` input, nothing here shows the licence STOPPING at the
        // proxy table. That boundary is pinned by
        // `universal_dedup_folds_the_domain_and_keeps_local_part_case`, where
        // the same case-variant pair inside `otherMails` survives, and by
        // `the_primary_proxy_exception_does_not_reach_into_other_mails`.
        let emails = directory_additional_emails(
            "ada@example.test",
            None,
            Some(vec![
                "smtp:One@example.test".to_string(),
                "smtp:ONE@example.test".to_string(),
                "smtp:one@Example.TEST".to_string(),
            ]),
        );
        assert_eq!(emails, vec!["One@example.test".to_string()]);
    }

    #[test]
    fn primary_proxy_is_suppressed_only_when_it_matches_mail() {
        // The universal rule alone leaves `mail` and a case-variant SMTP:
        // entry standing as two entries for one mailbox, so the pair gets
        // its own whole-address comparison.
        let matching = directory_additional_emails(
            "Ada@example.test",
            None,
            Some(vec!["SMTP:ada@EXAMPLE.test".to_string()]),
        );
        assert!(
            matching.is_empty(),
            "kept a restatement of mail: {matching:?}"
        );

        // ... but Graph documents guest accounts whose primary proxy is NOT
        // `mail`, so "it is SMTP:, therefore it is mail" would lose a real
        // address.
        let guest = directory_additional_emails(
            "ada@external.example",
            None,
            Some(vec![
                "SMTP:ada_external.example#EXT#@contoso.test".to_string(),
            ]),
        );
        assert_eq!(
            guest,
            vec!["ada_external.example#EXT#@contoso.test".to_string()]
        );
    }

    #[test]
    fn the_primary_proxy_exception_does_not_reach_into_other_mails() {
        // otherMails names a local-part variant of the primary, which the
        // universal rule keeps; the proxy entry that really does restate
        // the primary is the one suppressed. Suppressing the otherMails
        // entry instead would drop the only spelling a consumer had.
        let emails = directory_additional_emails(
            "Ada@example.test",
            Some(vec!["ada@example.test".to_string()]),
            Some(vec!["SMTP:ada@example.test".to_string()]),
        );
        assert_eq!(emails, vec!["ada@example.test".to_string()]);
    }

    #[test]
    fn a_suppressed_proxy_still_feeds_the_proxy_comparison_set() {
        // The first proxy is already represented by otherMails and is not
        // emitted - but it must still enter the proxy-only fold, or the
        // second one survives and "de-duplicate within proxies" has
        // quietly become "de-duplicate within EMITTED proxies".
        let emails = directory_additional_emails(
            "ada@example.test",
            Some(vec!["Alias@example.test".to_string()]),
            Some(vec![
                "smtp:Alias@example.test".to_string(),
                "smtp:ALIAS@example.test".to_string(),
            ]),
        );
        assert_eq!(emails, vec!["Alias@example.test".to_string()]);
    }

    #[test]
    fn folding_is_ascii_only_and_leaves_non_ascii_spellings_alone() {
        // IDNA equivalence is outside the promise: ASCII folding is right
        // for ASCII spellings including punycode A-labels, and general
        // Unicode case folding is not a substitute for IDNA processing.
        // Half-done normalisation would turn into address loss, so
        // unexpected non-ASCII spellings are preserved.
        let emails = directory_additional_emails(
            "ada@example.test",
            Some(vec![
                // Escaped rather than literal so the source stays ASCII:
                // U+00DC / U+00FC, the U-label pair for the A-label below.
                "user@B\u{00dc}CHER.test".to_string(),
                "user@b\u{00fc}cher.test".to_string(),
                // The punycode A-label is ASCII, so it folds normally.
                "user@XN--BCHER-KVA.test".to_string(),
                "user@xn--bcher-kva.test".to_string(),
            ]),
            None,
        );
        assert_eq!(
            emails,
            vec![
                "user@B\u{00dc}CHER.test".to_string(),
                "user@b\u{00fc}cher.test".to_string(),
                "user@XN--BCHER-KVA.test".to_string(),
            ]
        );
    }

    #[test]
    fn first_retained_spelling_wins_after_trimming() {
        // The promise is the first RETAINED spelling after surrounding
        // whitespace is trimmed - not the byte-for-byte server spelling.
        let emails = directory_additional_emails(
            "ada@example.test",
            Some(vec![
                "  Alias@Example.test  ".to_string(),
                "  ".to_string(),
                "Alias@example.TEST".to_string(),
            ]),
            Some(vec!["smtp:  third@example.test  ".to_string()]),
        );
        assert_eq!(
            emails,
            vec![
                "Alias@Example.test".to_string(),
                "third@example.test".to_string(),
            ]
        );
    }
}
