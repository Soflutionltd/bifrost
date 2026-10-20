//! Google People contacts and directory search.
//!
//! Audit boundary: the 2026-07 google bug sweep did NOT line-audit this
//! module; it was skipped as already test-dense. A later auditor should read
//! it fresh rather than assuming that sweep covered it.

use std::collections::HashSet;
use std::sync::Arc;

use base64::Engine;
use bifrost_types::{
    AccessErrorKind, AccountError, AccountErrorKind, AccountFuture, AccountOperation, AddressBook,
    AddressBookId, ContactCard, ContactCorpus, ContactCreate, ContactEmail, ContactId,
    ContactOrganization, ContactPatch, ContactPhone, ContactProvenance, ContactSearchRequest,
    DirectoryCard, Page, ProtocolKind,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::client::GmailClient;

use super::error::{self, GmailErrorContext};
use super::non_empty;

const CONTACTS_BOOK_ID: &str = "google:contacts";
/// Synthetic address book that surfaces Google People `otherContacts`
/// (auto-collected addresses harvested from mail traffic) as a distinct,
/// `OtherAutoCollected`-corpus book so the consumer routes it to its own
/// local store without matching on the provider.
const OTHER_CONTACTS_BOOK_ID: &str = "google:other-contacts";
const PERSON_FIELDS: &str = "names,emailAddresses,phoneNumbers,addresses,organizations,photos,biographies,memberships,metadata";
/// `otherContacts.list` supports only this read-mask subset (no
/// addresses / organizations / photos / biographies / memberships).
const OTHER_CONTACTS_READ_MASK: &str = "names,emailAddresses,phoneNumbers,metadata";
const MAX_ADDRESS_BOOK_PAGES: usize = 10_000;

pub(crate) fn address_books_list(
    client: Arc<GmailClient>,
) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
    Box::pin(async move {
        let mut books = vec![
            AddressBook {
                id: AddressBookId(CONTACTS_BOOK_ID.to_string()),
                native_id: CONTACTS_BOOK_ID.to_string(),
                name: "Google Contacts".to_string(),
                provenance: ContactProvenance {
                    provider: ProtocolKind::Gmail,
                    native: CONTACTS_BOOK_ID.to_string(),
                    address_book_native: None,
                },
                corpus: ContactCorpus::Main,
                is_default: true,
                can_create_contacts: true,
                can_update_contacts: true,
                can_delete_contacts: true,
            },
            other_contacts_address_book(),
        ];
        let mut page_token = None;
        let mut pages_walked = 0;
        let mut seen_page_tokens = HashSet::new();
        loop {
            let response: ContactGroupsResponse = client
                .get(&contact_groups_url(
                    client.people_base(),
                    page_token.as_deref(),
                ))
                .await
                .map_err(|error| collection_error(error, AccountOperation::AddressBooksList))?;
            books.extend(
                response
                    .contact_groups
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(address_book_from_group),
            );
            pages_walked += 1;
            let Some(next) = response.next_page_token else {
                break;
            };
            if let Some(refusal) =
                address_book_walk_refusal(&mut seen_page_tokens, pages_walked, &next)
            {
                // Neither guard is a missing field: a repeated token is the
                // provider's contract breach, the budget this crate's limit.
                return Err(error::into_account_error(
                    crate::error::Error::from(refusal),
                    GmailErrorContext::contact_collection(AccountOperation::AddressBooksList),
                ));
            }
            page_token = Some(next);
        }
        Ok(books)
    })
}

/// Decides whether the walk must refuse to follow `next_page_token`,
/// returning the diagnostic detail for the terminating error.
///
/// Guard ORDER is deliberate and matches `inventory::inventory_walk_refusal`,
/// `changes::walk_refusal` and `calendar::calendars_list`: the repeated-token
/// check runs FIRST. Both guards terminate the walk with a terminal recovery
/// class, but they blame different parties, and a page that trips both deserves the sharper
/// diagnosis. A repeated token names a provider contract breach - the server
/// is cycling and no amount of further paging makes progress - where the
/// budget only reports "too many pages", which is also what an honestly huge
/// address book looks like.
fn address_book_walk_refusal(
    seen_page_tokens: &mut HashSet<String>,
    pages_walked: usize,
    next_page_token: &str,
) -> Option<crate::error::PageRefusal> {
    if !seen_page_tokens.insert(next_page_token.to_string()) {
        return Some(crate::error::PageRefusal::RepeatedToken(format!(
            "google contactGroups.list repeated page token {next_page_token:?}"
        )));
    }
    (pages_walked >= MAX_ADDRESS_BOOK_PAGES).then(|| {
        crate::error::PageRefusal::BudgetExceeded(format!(
            "google contactGroups.list exceeded {MAX_ADDRESS_BOOK_PAGES} pages in one walk"
        ))
    })
}

pub(crate) fn list(
    client: Arc<GmailClient>,
    address_book: Option<AddressBookId>,
    page_cursor: Option<Vec<u8>>,
) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
    Box::pin(async move {
        // The `otherContacts` corpus is served by a distinct People endpoint
        // and carries the `OtherAutoCollected` discriminator; route to it
        // before the personal-book validation, which does not accept the
        // synthetic other-contacts id (it is read-only, not creatable/
        // searchable through the personal paths).
        if is_other_contacts_book(address_book.as_ref()) {
            return list_other_contacts(&client, page_cursor).await;
        }
        validate_address_book(address_book.as_ref(), AccountOperation::ContactsList)?;
        let group_filter = contact_group_filter(address_book.as_ref());
        let page_token = page_cursor
            .map(String::from_utf8)
            .transpose()
            .map_err(|error| {
                caller_input_error(AccountOperation::ContactsList, error.to_string())
            })?;
        let mut url = format!(
            "{}/people/me/connections?personFields={}&pageSize=1000",
            client.people_base(),
            bifrost_net::url::encode_query_value(PERSON_FIELDS)
        );
        if let Some(token) = page_token {
            url.push_str("&pageToken=");
            url.push_str(&bifrost_net::url::encode_query_value(&token));
        }
        let response: PeopleConnectionsResponse = client
            .get(&url)
            .await
            .map_err(|error| collection_error(error, AccountOperation::ContactsList))?;
        Ok(page_from_people(
            response.connections.unwrap_or_default(),
            response.next_page_token,
            group_filter,
            ContactCorpus::Main,
        ))
    })
}

/// Page the People `otherContacts.list` corpus. otherContacts are
/// auto-collected (never explicitly saved) addresses; they carry no
/// groups and support only the `OTHER_CONTACTS_READ_MASK` subset, so
/// there is no group filter here and the projected cards land under the
/// synthetic other-contacts book with the `OtherAutoCollected` corpus.
async fn list_other_contacts(
    client: &GmailClient,
    page_cursor: Option<Vec<u8>>,
) -> Result<Page<ContactCard>, AccountError> {
    let page_token = page_cursor
        .map(String::from_utf8)
        .transpose()
        .map_err(|error| caller_input_error(AccountOperation::ContactsList, error.to_string()))?;
    let mut url = format!(
        "{}/otherContacts?readMask={}&pageSize=1000",
        client.people_base(),
        bifrost_net::url::encode_query_value(OTHER_CONTACTS_READ_MASK)
    );
    if let Some(token) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&bifrost_net::url::encode_query_value(&token));
    }
    let response: OtherContactsResponse = client
        .get(&url)
        .await
        .map_err(|error| collection_error(error, AccountOperation::ContactsList))?;
    Ok(page_from_people(
        response.other_contacts.unwrap_or_default(),
        response.next_page_token,
        None,
        ContactCorpus::OtherAutoCollected,
    ))
}

pub(crate) fn get(
    client: Arc<GmailClient>,
    contact: ContactId,
) -> AccountFuture<Result<ContactCard, AccountError>> {
    Box::pin(async move {
        let person = get_person(&client, &contact, AccountOperation::ContactGet).await?;
        // Derive the corpus from the resource name: an `otherContacts/*`
        // id is auto-collected, everything else is a personal contact.
        let corpus = corpus_for_native(&contact.0);
        Ok(contact_from_person(person, corpus))
    })
}

pub(crate) fn create(
    client: Arc<GmailClient>,
    contact: ContactCreate,
) -> AccountFuture<Result<ContactId, AccountError>> {
    Box::pin(async move {
        validate_address_book(
            contact.address_book_id.as_ref(),
            AccountOperation::ContactCreate,
        )?;
        let person = person_from_create(&contact);
        let url = format!(
            "{}/people:createContact?personFields={}",
            client.people_base(),
            bifrost_net::url::encode_query_value(PERSON_FIELDS)
        );
        let person: Person = client
            .post(&url, &person)
            .await
            .map_err(|error| collection_error(error, AccountOperation::ContactCreate))?;
        let resource_name = person.resource_name.ok_or_else(|| {
            // A successful People response must identify the created person;
            // returning an empty id would make every follow-up URL invalid.
            local_error_with_field(
                AccountOperation::ContactCreate,
                "resourceName",
                "created Google contact has no resource name".to_string(),
            )
        })?;
        if resource_name.is_empty() {
            // The field was present, but cannot identify a People resource.
            return Err(error::into_account_error(
                crate::error::Error::provider_response(
                    crate::error::ProviderFault::ParseFailed,
                    "created Google contact has an empty resource name",
                ),
                GmailErrorContext::contact_collection(AccountOperation::ContactCreate),
            ));
        }
        Ok(ContactId(resource_name))
    })
}

pub(crate) fn update(
    client: Arc<GmailClient>,
    contact: ContactId,
    patch: ContactPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        // Pure-local guards first: fail an invalid request cheaply, before
        // the network GET + etag round trip.
        validate_address_book(
            patch.address_book_id.as_ref(),
            AccountOperation::ContactUpdate,
        )?;
        reject_photo_url_patch(&patch)?;
        let mut person = get_person(&client, &contact, AccountOperation::ContactUpdate).await?;
        let update_fields = update_fields_for_patch(&patch);
        if !update_fields.is_empty() {
            // The People `updateContact` call needs the server etag; a
            // photo-only patch (handled below via `updateContactPhoto`)
            // does not, so only require it when a modeled field is written.
            let etag = require_person_etag(&person)?;
            apply_patch_to_person(&mut person, &patch);
            person.resource_name = Some(contact.0.clone());
            person.etag = Some(etag);
            let encoded = encode_resource_name_path(&contact.0);
            let url = format!(
                "{}/{encoded}:updateContact?updatePersonFields={}&personFields={}",
                client.people_base(),
                bifrost_net::url::encode_query_value(&update_fields),
                bifrost_net::url::encode_query_value(PERSON_FIELDS)
            );
            let _: Person = client.patch(&url, &person).await.map_err(|error| {
                contact_error(error, AccountOperation::ContactUpdate, contact.0.clone())
            })?;
        }
        if let Some(photo) = patch.photo {
            update_contact_photo(&client, &contact, photo).await?;
        }
        Ok(())
    })
}

async fn update_contact_photo(
    client: &GmailClient,
    contact: &ContactId,
    photo: Option<bifrost_types::ContactPhoto>,
) -> Result<(), AccountError> {
    let encoded = encode_resource_name_path(&contact.0);
    if let Some(photo) = photo {
        let url = update_contact_photo_url(client.people_base(), &encoded);
        // `updateContactPhoto` wraps its response as `{ "person": {...} }`,
        // not a bare `Person`. The value is discarded, so decode into a raw
        // Value rather than a type that only "works" because every Person
        // field is optional.
        let _: serde_json::Value = client
            .post(&url, &update_contact_photo_request(photo))
            .await
            .map_err(|error| {
                contact_error(error, AccountOperation::ContactUpdate, contact.0.clone())
            })?;
    } else {
        let url = delete_contact_photo_url(client.people_base(), &encoded);
        let _: Person = client
            .delete(&url)
            .await
            .map(|_| Person::default())
            .map_err(|error| {
                contact_error(error, AccountOperation::ContactUpdate, contact.0.clone())
            })?;
    }
    Ok(())
}

fn update_contact_photo_url(people_base: &str, encoded_resource_name: &str) -> String {
    // `updateContactPhoto` takes its field mask in the request body, not the
    // query string; the response is discarded here, so no `personFields`
    // query param is needed.
    format!("{people_base}/{encoded_resource_name}:updateContactPhoto")
}

fn delete_contact_photo_url(people_base: &str, encoded_resource_name: &str) -> String {
    format!(
        "{people_base}/{encoded_resource_name}:deleteContactPhoto?personFields={}",
        bifrost_net::url::encode_query_value(PERSON_FIELDS)
    )
}

fn update_contact_photo_request(photo: bifrost_types::ContactPhoto) -> UpdateContactPhotoRequest {
    UpdateContactPhotoRequest {
        photo_bytes: base64::engine::general_purpose::STANDARD.encode(photo.data),
    }
}

/// Percent-encode a People resource name (e.g. `people/c1`) for placement
/// in a URL path segment. A People resource name's `/` separators are real
/// REST path separators - the People API serves
/// `/v1/people/c1:updateContact`, not `/v1/people%2Fc1:updateContact`
/// (which 404s) - so each `/`-delimited part is component-encoded and the
/// parts are rejoined on a literal `/`. Any genuinely unsafe character
/// inside a part is still escaped. This is deliberately not folded into
/// `bifrost_net::url::encode_path_component`, whose other callers rely on it
/// escaping `/` as a normal component character.
fn encode_resource_name_path(resource_name: &str) -> String {
    resource_name
        .split('/')
        .map(bifrost_net::url::encode_path_component)
        .collect::<Vec<_>>()
        .join("/")
}

async fn get_person(
    client: &GmailClient,
    contact: &ContactId,
    operation: AccountOperation,
) -> Result<Person, AccountError> {
    let encoded = encode_resource_name_path(&contact.0);
    let url = format!(
        "{}/{encoded}?personFields={}",
        client.people_base(),
        bifrost_net::url::encode_query_value(PERSON_FIELDS)
    );
    client
        .get(&url)
        .await
        .map_err(|error| contact_error(error, operation, contact.0.clone()))
}

pub(crate) fn delete(
    client: Arc<GmailClient>,
    contact: ContactId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let encoded = encode_resource_name_path(&contact.0);
        let url = format!("{}/{encoded}:deleteContact", client.people_base());
        client
            .delete(&url)
            .await
            .map_err(|error| contact_error(error, AccountOperation::ContactDelete, contact.0))
    })
}

pub(crate) fn search(
    client: Arc<GmailClient>,
    request: ContactSearchRequest,
) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
    Box::pin(async move {
        validate_address_book(
            request.address_book_id.as_ref(),
            AccountOperation::ContactSearch,
        )?;
        let _: PeopleSearchResponse = client
            .get(&search_warmup_url(client.people_base()))
            .await
            .map_err(|error| collection_error(error, AccountOperation::ContactSearch))?;
        let url = search_url(client.people_base(), &request)?;
        let response: PeopleSearchResponse = client
            .get(&url)
            .await
            .map_err(|error| collection_error(error, AccountOperation::ContactSearch))?;
        let group_filter = contact_group_filter(request.address_book_id.as_ref());
        let items = response
            .results
            .unwrap_or_default()
            .into_iter()
            .filter(|result| person_in_group(&result.person, group_filter))
            .map(|result| contact_from_person(result.person, ContactCorpus::Main))
            .collect();
        Ok(Page {
            truncated: false,
            items,
            next_cursor: response.next_page_token.map(String::into_bytes),
            estimated_total: None,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

/// Organization directory (Global Address List) search.
///
/// An empty `query` enumerates the domain directory via
/// `people:listDirectoryPeople` (the ratatoskr path, no warmup); a
/// non-empty `query` runs `people:searchDirectoryPeople`, which - like
/// the `searchContacts` family - requires a cache-priming warmup call
/// before the real query. The warmup is non-fatal on a transient blip
/// (it only primes the cache); a directory-absence 403 there short-
/// circuits to an empty page, and a terminal non-absence failure (e.g.
/// `PolicyBlocked`) propagates. Personal Gmail accounts (and accounts
/// without the directory scope) have no directory and answer 403; on the
/// first page that surfaces as `PermissionDenied` or `InsufficientScope`,
/// both swallowed to an empty page, while a `PolicyBlocked` 403 (a genuine
/// admin refusal) propagates. Past the first page all failures propagate:
/// a follow-up-page 403 means the directory existed and a later page
/// failed, so masking it would silently truncate results.
pub(crate) fn directory_search(
    client: Arc<GmailClient>,
    query: String,
    limit: Option<u32>,
    page_cursor: Option<Vec<u8>>,
) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
    Box::pin(async move {
        if !query.is_empty() {
            // search* family cache-priming warmup; its own 403 is itself
            // the "no directory" signal, so a directory-absence 403 short-
            // circuits to an empty page. A transient blip (429/500/network)
            // is non-fatal: warmup only primes the cache, so fall through
            // to the real query and let it produce the authoritative result
            // rather than failing the whole search on a warmup hiccup.
            if let Err(error) = client
                .get::<DirectoryPeopleResponse>(&directory_search_warmup_url(client.people_base()))
                .await
            {
                let err = collection_error(error, AccountOperation::DirectorySearch);
                if is_directory_absence(&err) {
                    return Ok(Page::single(vec![], vec![], vec![]));
                } else if err.recovery().is_terminal() {
                    // A terminal non-absence failure (e.g. PolicyBlocked) is
                    // a real refusal; surface it.
                    return Err(err);
                }
                // Transient: ignore and proceed to the real query.
            }
        }
        // The directory-absence swallow only makes sense on the first page:
        // a 403 there means the account has no directory at all. A 403 on a
        // follow-up page (e.g. an expired/invalid page token) means the
        // directory existed - we already returned earlier pages - so masking
        // it as a clean empty page would silently truncate results. Past the
        // first page, all failures propagate.
        let is_first_page = page_cursor.is_none();
        let url =
            directory_search_url(client.people_base(), &query, limit, page_cursor.as_deref())?;
        let response: DirectoryPeopleResponse = match client.get(&url).await {
            Ok(response) => response,
            Err(error) => {
                let err = collection_error(error, AccountOperation::DirectorySearch);
                if is_first_page {
                    return directory_absence_to_empty(err);
                }
                return Err(err);
            }
        };
        let items = response
            .people
            .unwrap_or_default()
            .into_iter()
            .filter_map(person_to_directory_card)
            .collect();
        Ok(Page {
            truncated: false,
            items,
            next_cursor: response.next_page_token.map(String::into_bytes),
            estimated_total: None,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
}

/// Swallow the two directory-absence classifications (`PermissionDenied`
/// and `InsufficientScope`) into an empty page; propagate everything
/// else (notably `PolicyBlocked`, a real admin refusal). Classify first,
/// then branch on the typed kind, per the error-model contract.
fn directory_absence_to_empty(err: AccountError) -> Result<Page<DirectoryCard>, AccountError> {
    if is_directory_absence(&err) {
        Ok(Page::single(vec![], vec![], vec![]))
    } else {
        Err(err)
    }
}

/// True for the two classifications that mean "this account has no
/// directory" (`PermissionDenied`, `InsufficientScope`). `PolicyBlocked`
/// is a real admin refusal and is deliberately excluded so it propagates.
///
/// Note: Gmail's `notAuthorizedToAccessThisResource` admin refusal
/// classifies as `PermissionDenied`, so it is treated as absence here on a
/// first-page probe. That matches the documented swallow set; the
/// first-page gate (see `directory_search`) keeps it from masking a
/// mid-pagination refusal.
fn is_directory_absence(err: &AccountError) -> bool {
    matches!(
        err.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
            | AccountErrorKind::Authorization(AccessErrorKind::InsufficientScope)
    )
}

const DIRECTORY_READ_MASK: &str = "names,emailAddresses,phoneNumbers,organizations";
const DIRECTORY_SOURCES: &str = "DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE";

fn directory_search_warmup_url(people_base: &str) -> String {
    format!(
        "{people_base}/people:searchDirectoryPeople?query=&readMask={}&sources={DIRECTORY_SOURCES}",
        bifrost_net::url::encode_query_value(DIRECTORY_READ_MASK)
    )
}

fn directory_search_url(
    people_base: &str,
    query: &str,
    limit: Option<u32>,
    page_cursor: Option<&[u8]>,
) -> Result<String, AccountError> {
    let page_size = limit.unwrap_or(1000).min(1000);
    let mut url = if query.is_empty() {
        format!(
            "{people_base}/people:listDirectoryPeople?readMask={}&sources={DIRECTORY_SOURCES}&pageSize={page_size}",
            bifrost_net::url::encode_query_value(DIRECTORY_READ_MASK)
        )
    } else {
        format!(
            "{people_base}/people:searchDirectoryPeople?query={}&readMask={}&sources={DIRECTORY_SOURCES}&pageSize={page_size}",
            bifrost_net::url::encode_query_value(query),
            bifrost_net::url::encode_query_value(DIRECTORY_READ_MASK)
        )
    };
    if let Some(cursor) = page_cursor {
        let token = std::str::from_utf8(cursor).map_err(|error| {
            caller_input_error(AccountOperation::DirectorySearch, error.to_string())
        })?;
        url.push_str("&pageToken=");
        url.push_str(&bifrost_net::url::encode_query_value(token));
    }
    Ok(url)
}

/// Project one directory `Person` into a `DirectoryCard`. Returns `None`
/// when the row has no email (matching ratatoskr's mail-less drop). The
/// first email is the key; the rest land in `additional_emails`. The org
/// tuple comes from `organizations[0]`.
fn person_to_directory_card(person: Person) -> Option<DirectoryCard> {
    let display_name = person
        .names
        .as_ref()
        .and_then(|names| names.first())
        .and_then(|name| name.display_name.clone());
    let mut emails = person
        .email_addresses
        .unwrap_or_default()
        .into_iter()
        .filter_map(|email| email.value)
        .filter(|value| !value.is_empty());
    let email = emails.next()?;
    let additional_emails = emails.collect();
    let phones = person
        .phone_numbers
        .unwrap_or_default()
        .into_iter()
        .filter_map(|phone| phone.value)
        .collect();
    let org = person.organizations.unwrap_or_default().into_iter().next();
    let (company, title, department) = match org {
        Some(org) => (org.name, org.title, org.department),
        None => (None, None, None),
    };
    Some(DirectoryCard {
        email,
        display_name,
        additional_emails,
        phones,
        company,
        title,
        department,
        provider: ProtocolKind::Gmail,
    })
}

fn reject_photo_url_patch(patch: &ContactPatch) -> Result<(), AccountError> {
    if patch.photo_url.is_some() {
        return Err(error::into_account_error(
            crate::error::Error::unsupported_with(
                AccountOperation::ContactUpdate,
                "Google People contact photos require updateContactPhoto with image bytes; ContactPatch.photo_url is read-only",
            ),
            GmailErrorContext::contact_collection(AccountOperation::ContactUpdate),
        ));
    }
    Ok(())
}

fn validate_address_book(
    address_book: Option<&AddressBookId>,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if address_book.is_some_and(|address_book| {
        address_book.0 != CONTACTS_BOOK_ID && !address_book.0.starts_with("contactGroups/")
    }) {
        return Err(caller_input_error(
            operation,
            "Google People supports google:contacts or contactGroups/* address books".to_string(),
        ));
    }
    Ok(())
}

fn contact_groups_url(people_base: &str, page_token: Option<&str>) -> String {
    let mut url = format!(
        "{people_base}/contactGroups?groupFields={}&pageSize=1000",
        bifrost_net::url::encode_query_value("metadata,name")
    );
    if let Some(token) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&bifrost_net::url::encode_query_value(token));
    }
    url
}

fn search_warmup_url(people_base: &str) -> String {
    format!(
        "{people_base}/people:searchContacts?query=&readMask={}",
        bifrost_net::url::encode_query_value(PERSON_FIELDS)
    )
}

fn search_url(people_base: &str, request: &ContactSearchRequest) -> Result<String, AccountError> {
    let mut url = format!(
        "{people_base}/people:searchContacts?query={}&readMask={}",
        bifrost_net::url::encode_query_value(&request.query),
        bifrost_net::url::encode_query_value(PERSON_FIELDS)
    );
    if let Some(limit) = request.limit {
        url.push_str("&pageSize=");
        url.push_str(&limit.min(30).to_string());
    }
    if let Some(cursor) = request.page_cursor.as_ref() {
        let token = std::str::from_utf8(cursor).map_err(|error| {
            caller_input_error(AccountOperation::ContactSearch, error.to_string())
        })?;
        url.push_str("&pageToken=");
        url.push_str(&bifrost_net::url::encode_query_value(token));
    }
    Ok(url)
}

fn page_from_people(
    people: Vec<Person>,
    next_page_token: Option<String>,
    group_filter: Option<&str>,
    corpus: ContactCorpus,
) -> Page<ContactCard> {
    Page {
        truncated: false,
        items: people
            .into_iter()
            .filter(|person| person_in_group(person, group_filter))
            .map(|person| contact_from_person(person, corpus))
            .collect(),
        next_cursor: next_page_token.map(String::into_bytes),
        estimated_total: None,
        failed_ids: Vec::new(),
        skipped_scopes: Vec::new(),
    }
}

/// Synthetic address book for the `otherContacts` corpus. Read-only:
/// auto-collected addresses cannot be created, updated, or deleted
/// through the People contact endpoints, so every capability flag is
/// false and the corpus discriminator is `OtherAutoCollected`.
fn other_contacts_address_book() -> AddressBook {
    AddressBook {
        id: AddressBookId(OTHER_CONTACTS_BOOK_ID.to_string()),
        native_id: OTHER_CONTACTS_BOOK_ID.to_string(),
        name: "Other Contacts".to_string(),
        provenance: ContactProvenance {
            provider: ProtocolKind::Gmail,
            native: OTHER_CONTACTS_BOOK_ID.to_string(),
            address_book_native: None,
        },
        corpus: ContactCorpus::OtherAutoCollected,
        is_default: false,
        can_create_contacts: false,
        can_update_contacts: false,
        can_delete_contacts: false,
    }
}

fn is_other_contacts_book(address_book: Option<&AddressBookId>) -> bool {
    address_book.is_some_and(|book| book.0 == OTHER_CONTACTS_BOOK_ID)
}

/// The corpus a People resource name belongs to. `otherContacts/*`
/// resource names are auto-collected; everything else is a personal
/// contact.
fn corpus_for_native(native: &str) -> ContactCorpus {
    if native.starts_with("otherContacts/") {
        ContactCorpus::OtherAutoCollected
    } else {
        ContactCorpus::Main
    }
}

/// The synthetic address book id a contact of `corpus` is filed under.
fn book_id_for_corpus(corpus: ContactCorpus) -> &'static str {
    match corpus {
        ContactCorpus::OtherAutoCollected => OTHER_CONTACTS_BOOK_ID,
        _ => CONTACTS_BOOK_ID,
    }
}

fn address_book_from_group(group: ContactGroup) -> Option<AddressBook> {
    let native = group.resource_name?;
    Some(AddressBook {
        id: AddressBookId(native.clone()),
        native_id: native.clone(),
        name: group.name.unwrap_or_else(|| native.clone()),
        provenance: ContactProvenance {
            provider: ProtocolKind::Gmail,
            native,
            address_book_native: None,
        },
        corpus: ContactCorpus::Main,
        is_default: false,
        can_create_contacts: true,
        can_update_contacts: true,
        can_delete_contacts: true,
    })
}

fn contact_group_filter(address_book: Option<&AddressBookId>) -> Option<&str> {
    address_book
        .map(|address_book| address_book.0.as_str())
        .filter(|address_book| address_book.starts_with("contactGroups/"))
}

fn person_in_group(person: &Person, group: Option<&str>) -> bool {
    let Some(group) = group else {
        return true;
    };
    person
        .memberships
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|membership| {
            membership
                .contact_group_membership
                .as_ref()
                .and_then(|membership| membership.contact_group_resource_name.as_deref())
                == Some(group)
        })
}

fn contact_from_person(person: Person, corpus: ContactCorpus) -> ContactCard {
    let native = person.resource_name.unwrap_or_default();
    let book_id = book_id_for_corpus(corpus);
    let display_name = person
        .names
        .as_ref()
        .and_then(|names| names.first())
        .and_then(|name| name.display_name.clone());
    ContactCard {
        id: ContactId(native.clone()),
        address_book_id: Some(AddressBookId(book_id.to_string())),
        native_id: native.clone(),
        etag: person.etag,
        provenance: ContactProvenance {
            provider: ProtocolKind::Gmail,
            native,
            address_book_native: Some(book_id.to_string()),
        },
        corpus,
        display_name,
        emails: person
            .email_addresses
            .unwrap_or_default()
            .into_iter()
            .filter_map(|email| {
                Some(ContactEmail {
                    value: email.value?,
                    kind: people_kind(email.kind, email.formatted_type),
                    is_primary: email
                        .metadata
                        .and_then(|meta| meta.primary)
                        .unwrap_or(false),
                })
            })
            .collect(),
        phones: person
            .phone_numbers
            .unwrap_or_default()
            .into_iter()
            .filter_map(|phone| {
                Some(ContactPhone {
                    value: phone.value?,
                    kind: people_kind(phone.kind, phone.formatted_type),
                    is_primary: phone
                        .metadata
                        .and_then(|meta| meta.primary)
                        .unwrap_or(false),
                })
            })
            .collect(),
        organizations: person
            .organizations
            .unwrap_or_default()
            .into_iter()
            .filter_map(|org| {
                Some(ContactOrganization {
                    name: org.name?,
                    title: org.title,
                })
            })
            .collect(),
        addresses: person
            .addresses
            .unwrap_or_default()
            .into_iter()
            .map(address_from_people)
            .collect(),
        notes: person
            .biographies
            .unwrap_or_default()
            .into_iter()
            .find_map(|bio| bio.value),
        photo_url: person
            .photos
            .unwrap_or_default()
            .into_iter()
            .find_map(|photo| photo.url),
        photo: None,
    }
}

fn person_from_create(contact: &ContactCreate) -> Person {
    Person {
        resource_name: None,
        etag: None,
        names: contact
            .display_name
            .as_ref()
            .map(|display_name| vec![name_from_display_name(display_name)]),
        email_addresses: non_empty(contact.emails.iter().map(|email| EmailAddress {
            value: Some(email.value.clone()),
            kind: people_type(email.kind.as_deref()).map(ToString::to_string),
            formatted_type: people_formatted_type(email.kind.as_deref()).map(ToString::to_string),
            metadata: email.is_primary.then_some(FieldMetadata {
                primary: Some(true),
            }),
        })),
        phone_numbers: non_empty(contact.phones.iter().map(|phone| PhoneNumber {
            value: Some(phone.value.clone()),
            kind: people_type(phone.kind.as_deref()).map(ToString::to_string),
            formatted_type: people_formatted_type(phone.kind.as_deref()).map(ToString::to_string),
            metadata: phone.is_primary.then_some(FieldMetadata {
                primary: Some(true),
            }),
        })),
        organizations: non_empty(contact.organizations.iter().map(|org| Organization {
            name: Some(org.name.clone()),
            title: org.title.clone(),
            department: None,
        })),
        addresses: non_empty(contact.addresses.iter().map(address_to_people)),
        photos: None,
        biographies: contact.notes.as_ref().map(|notes| {
            vec![Biography {
                value: Some(notes.clone()),
            }]
        }),
        memberships: contact_group_filter(contact.address_book_id.as_ref()).map(|group| {
            vec![Membership {
                contact_group_membership: Some(ContactGroupMembership {
                    contact_group_resource_name: Some(group.to_string()),
                }),
            }]
        }),
    }
}

fn name_from_display_name(display_name: &str) -> Name {
    let mut parts = display_name.split_whitespace().collect::<Vec<_>>();
    let family_name = (parts.len() > 1).then(|| parts.pop().unwrap_or_default().to_string());
    let given_name = (!parts.is_empty()).then(|| parts.join(" "));
    Name {
        display_name: None,
        given_name,
        family_name,
        extra: Map::new(),
    }
}

fn apply_patch_to_person(person: &mut Person, patch: &ContactPatch) {
    if let Some(display_name) = &patch.display_name {
        person.names = display_name
            .as_deref()
            .map(|display_name| patched_names(person.names.take(), display_name));
    }
    if let Some(emails) = &patch.emails {
        person.email_addresses = non_empty(emails.iter().map(|email| EmailAddress {
            value: Some(email.value.clone()),
            kind: people_type(email.kind.as_deref()).map(ToString::to_string),
            formatted_type: people_formatted_type(email.kind.as_deref()).map(ToString::to_string),
            metadata: email.is_primary.then_some(FieldMetadata {
                primary: Some(true),
            }),
        }));
    }
    if let Some(phones) = &patch.phones {
        person.phone_numbers = non_empty(phones.iter().map(|phone| PhoneNumber {
            value: Some(phone.value.clone()),
            kind: people_type(phone.kind.as_deref()).map(ToString::to_string),
            formatted_type: people_formatted_type(phone.kind.as_deref()).map(ToString::to_string),
            metadata: phone.is_primary.then_some(FieldMetadata {
                primary: Some(true),
            }),
        }));
    }
    if let Some(organizations) = &patch.organizations {
        person.organizations = non_empty(organizations.iter().map(|org| Organization {
            name: Some(org.name.clone()),
            title: org.title.clone(),
            department: None,
        }));
    }
    if let Some(addresses) = &patch.addresses {
        person.addresses = non_empty(addresses.iter().map(address_to_people));
    }
    if let Some(notes) = &patch.notes {
        person.biographies = notes.as_ref().map(|notes| {
            vec![Biography {
                value: Some(notes.clone()),
            }]
        });
    }
}

fn patched_names(names: Option<Vec<Name>>, display_name: &str) -> Vec<Name> {
    let replacement = name_from_display_name(display_name);
    if let Some(mut names) = names
        && let Some(first) = names.first_mut()
    {
        first.display_name = None;
        first.given_name = replacement.given_name;
        first.family_name = replacement.family_name;
        return names;
    }
    vec![replacement]
}

fn require_person_etag(person: &Person) -> Result<String, AccountError> {
    person.etag.clone().ok_or_else(|| {
        local_error_with_field(
            AccountOperation::ContactUpdate,
            "contactEtag",
            "Google People updateContact requires an etag".to_string(),
        )
    })
}

// COUPLING: every field named here must also be carried by the `Person` DTO,
// because `update` re-serializes the fetched `Person` as the PATCH body and
// relies on this mask to fence off the unmodeled People fields the DTO
// dropped. Naming a field here that the body does not carry would clear it
// server-side.
fn update_fields_for_patch(patch: &ContactPatch) -> String {
    let mut fields = Vec::new();
    if patch.display_name.is_some() {
        fields.push("names");
    }
    if patch.emails.is_some() {
        fields.push("emailAddresses");
    }
    if patch.phones.is_some() {
        fields.push("phoneNumbers");
    }
    if patch.organizations.is_some() {
        fields.push("organizations");
    }
    if patch.addresses.is_some() {
        fields.push("addresses");
    }
    if patch.notes.is_some() {
        fields.push("biographies");
    }
    fields.join(",")
}

fn people_kind(kind: Option<String>, formatted_type: Option<String>) -> Option<String> {
    match (kind.as_deref(), formatted_type) {
        (Some("custom"), Some(label)) if !label.is_empty() => Some(label),
        _ => kind,
    }
}

fn people_type(kind: Option<&str>) -> Option<&str> {
    kind.map(|kind| match kind {
        "home" | "work" | "other" | "mobile" | "main" | "homeFax" | "workFax" | "pager" => kind,
        _ => "custom",
    })
}

fn people_formatted_type(kind: Option<&str>) -> Option<&str> {
    kind.filter(|kind| people_type(Some(kind)) == Some("custom"))
}

fn address_from_people(address: PeopleAddress) -> bifrost_types::ContactAddress {
    bifrost_types::ContactAddress {
        kind: people_kind(address.kind, address.formatted_type),
        formatted: address.formatted_value,
        street: address
            .street_address
            .map(|street| street.lines().map(ToString::to_string).collect())
            .unwrap_or_default(),
        locality: address.city,
        region: address.region,
        postal_code: address.postal_code,
        country: address.country,
        is_primary: address
            .metadata
            .and_then(|meta| meta.primary)
            .unwrap_or(false),
    }
}

fn address_to_people(address: &bifrost_types::ContactAddress) -> PeopleAddress {
    PeopleAddress {
        formatted_value: address.formatted.clone(),
        street_address: (!address.street.is_empty()).then(|| address.street.join("\n")),
        city: address.locality.clone(),
        region: address.region.clone(),
        postal_code: address.postal_code.clone(),
        country: address.country.clone(),
        kind: people_type(address.kind.as_deref()).map(ToString::to_string),
        formatted_type: people_formatted_type(address.kind.as_deref()).map(ToString::to_string),
        metadata: address.is_primary.then_some(FieldMetadata {
            primary: Some(true),
        }),
    }
}

fn collection_error(error: crate::Error, operation: AccountOperation) -> AccountError {
    error::into_account_error(error, GmailErrorContext::contact_collection(operation))
}

fn contact_error(error: crate::Error, operation: AccountOperation, id: String) -> AccountError {
    error::into_account_error(error, GmailErrorContext::contact(operation, id))
}

/// The CALLER's input cannot be used - a page cursor that is not UTF-8, an
/// address book this backend does not have. Refused before the request it
/// would shape is sent, so never a provider fault: `Request(Malformed)`, per
/// the local-refusal rule in `reference/error-model.md`.
fn caller_input_error(operation: AccountOperation, message: String) -> AccountError {
    error::into_account_error(
        crate::error::Error::invalid_request(operation, message),
        GmailErrorContext::contact_collection(operation),
    )
}

fn local_error_with_field(
    operation: AccountOperation,
    field: &'static str,
    message: String,
) -> AccountError {
    error::into_account_error(
        crate::error::Error::missing_field(field, message),
        GmailErrorContext::contact_collection(operation),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Person {
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    names: Option<Vec<Name>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email_addresses: Option<Vec<EmailAddress>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phone_numbers: Option<Vec<PhoneNumber>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    addresses: Option<Vec<PeopleAddress>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    organizations: Option<Vec<Organization>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    photos: Option<Vec<Photo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    biographies: Option<Vec<Biography>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memberships: Option<Vec<Membership>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeopleConnectionsResponse {
    connections: Option<Vec<Person>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct OtherContactsResponse {
    other_contacts: Option<Vec<Person>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateContactPhotoRequest {
    photo_bytes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContactGroupsResponse {
    contact_groups: Option<Vec<ContactGroup>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContactGroup {
    resource_name: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeopleSearchResponse {
    results: Option<Vec<SearchResult>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SearchResult {
    person: Person,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct DirectoryPeopleResponse {
    people: Option<Vec<Person>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Name {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    given_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    family_name: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    formatted_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<FieldMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PhoneNumber {
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    formatted_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<FieldMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeopleAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    formatted_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    street_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    city: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    postal_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    formatted_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<FieldMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FieldMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    primary: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Organization {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    department: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Photo {
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Biography {
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Membership {
    #[serde(skip_serializing_if = "Option::is_none")]
    contact_group_membership: Option<ContactGroupMembership>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContactGroupMembership {
    #[serde(skip_serializing_if = "Option::is_none")]
    contact_group_resource_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use bytes::Bytes;
    use reqwest::StatusCode;

    use super::*;

    fn scripted_client(body: &'static [u8]) -> (Arc<GmailClient>, Arc<ScriptedDispatch>) {
        let script = ScriptedDispatch::new([Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from_static(body),
        }]);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        (
            Arc::new(GmailClient::with_account_net("https://gmail.test", net)),
            script,
        )
    }

    #[tokio::test]
    async fn contact_create_requires_the_provider_resource_name() {
        let (client, script) = scripted_client(b"{}");
        let error = create(client, ContactCreate::default())
            .await
            .expect_err("a successful response without resourceName is malformed");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::MissingField
            )
        );
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            bifrost_types::Cause::Attempt(attempt)
                if attempt.transmission_state == bifrost_types::TransmissionState::Acknowledged
        )));
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test]
    async fn contact_create_rejects_an_empty_provider_resource_name() {
        let (client, _script) = scripted_client(br#"{"resourceName":""}"#);
        let error = create(client, ContactCreate::default())
            .await
            .expect_err("an empty resourceName cannot identify a contact");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ParseFailed
            )
        );
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            bifrost_types::Cause::Attempt(attempt)
                if attempt.transmission_state == bifrost_types::TransmissionState::Acknowledged
        )));
    }

    #[test]
    fn address_book_page_guard_refuses_repetition_and_budget_exhaustion() {
        let mut seen = HashSet::new();
        assert!(address_book_walk_refusal(&mut seen, 1, "next").is_none());
        let repeated =
            address_book_walk_refusal(&mut seen, 2, "next").expect("repeated token must refuse");
        assert!(repeated.detail().contains("repeated page token"));

        let mut fresh = HashSet::new();
        let exhausted = address_book_walk_refusal(&mut fresh, MAX_ADDRESS_BOOK_PAGES, "fresh")
            .expect("budget must refuse");
        assert!(exhausted.detail().contains("exceeded"));

        // Each guard carries its own blame: the provider for a cycling token,
        // this crate's own limit for the budget. Neither is a missing field,
        // which is what both used to report.
        let classify = |refusal| {
            error::into_account_error(
                crate::error::Error::from(refusal),
                GmailErrorContext::contact_collection(AccountOperation::AddressBooksList),
            )
            .kind()
            .clone()
        };
        assert_eq!(
            classify(repeated),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
        assert_eq!(
            classify(exhausted),
            bifrost_types::AccountErrorKind::Internal(
                bifrost_types::InternalErrorKind::LimitExceeded
            )
        );
    }

    /// A page that trips BOTH guards is diagnosed as the repeated token,
    /// not as budget exhaustion: the repeated token is the sharper claim,
    /// and this lane must agree with `inventory_walk_refusal` and
    /// `changes::walk_refusal`.
    #[test]
    fn a_page_tripping_both_guards_is_diagnosed_as_the_repeated_token() {
        let mut seen = HashSet::new();
        seen.insert("cycling".to_string());
        let refusal = address_book_walk_refusal(&mut seen, MAX_ADDRESS_BOOK_PAGES, "cycling")
            .expect("a page tripping both guards must refuse");
        let detail = refusal.detail();
        assert!(
            detail.contains("repeated page token"),
            "repeated-token detection must win over the budget: {detail}",
        );
        assert!(
            !detail.contains("exceeded"),
            "the budget must not claim a page the repeated-token guard owns: {detail}",
        );
    }

    #[test]
    fn person_maps_to_contact_card() {
        let person = Person {
            resource_name: Some("people/c1".to_string()),
            etag: Some("etag".to_string()),
            names: Some(vec![Name {
                display_name: Some("Ada Lovelace".to_string()),
                given_name: None,
                family_name: None,
                extra: Map::new(),
            }]),
            email_addresses: Some(vec![
                EmailAddress {
                    value: Some("ada@example.test".to_string()),
                    kind: Some("work".to_string()),
                    formatted_type: None,
                    metadata: Some(FieldMetadata {
                        primary: Some(true),
                    }),
                },
                EmailAddress {
                    value: Some("lab@example.test".to_string()),
                    kind: Some("custom".to_string()),
                    formatted_type: Some("lab".to_string()),
                    metadata: None,
                },
            ]),
            phone_numbers: Some(vec![PhoneNumber {
                value: Some("+123".to_string()),
                kind: Some("mobile".to_string()),
                formatted_type: None,
                metadata: None,
            }]),
            organizations: Some(vec![Organization {
                name: Some("Analytical Engines".to_string()),
                title: Some("Programmer".to_string()),
                department: None,
            }]),
            addresses: Some(vec![PeopleAddress {
                formatted_value: Some("1 Example St, London".to_string()),
                street_address: Some("1 Example St\nUnit 2".to_string()),
                city: Some("London".to_string()),
                region: Some("England".to_string()),
                postal_code: Some("N1".to_string()),
                country: Some("UK".to_string()),
                kind: Some("home".to_string()),
                formatted_type: None,
                metadata: Some(FieldMetadata {
                    primary: Some(true),
                }),
            }]),
            photos: Some(vec![Photo {
                url: Some("https://example.test/a.jpg".to_string()),
            }]),
            biographies: Some(vec![Biography {
                value: Some("notes".to_string()),
            }]),
            memberships: None,
        };

        let card = contact_from_person(person, ContactCorpus::Main);
        assert_eq!(card.id.0, "people/c1");
        assert_eq!(card.corpus, ContactCorpus::Main);
        assert_eq!(
            card.address_book_id.as_ref().map(|book| book.0.as_str()),
            Some(CONTACTS_BOOK_ID)
        );
        assert_eq!(card.display_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(card.emails[0].kind.as_deref(), Some("work"));
        assert_eq!(card.emails[1].kind.as_deref(), Some("lab"));
        assert!(card.emails[0].is_primary);
        assert_eq!(card.phones[0].kind.as_deref(), Some("mobile"));
        assert_eq!(card.addresses[0].kind.as_deref(), Some("home"));
        assert_eq!(card.addresses[0].street, vec!["1 Example St", "Unit 2"]);
        assert!(card.addresses[0].is_primary);
        assert_eq!(card.organizations[0].title.as_deref(), Some("Programmer"));
        assert_eq!(card.notes.as_deref(), Some("notes"));
        assert_eq!(
            card.photo_url.as_deref(),
            Some("https://example.test/a.jpg")
        );
    }

    #[test]
    fn create_maps_contact_fields_to_person_payload() {
        let payload = person_from_create(&ContactCreate {
            display_name: Some("Ada".to_string()),
            photo_url: Some("https://example.test/a.jpg".to_string()),
            emails: vec![ContactEmail {
                value: "ada@example.test".to_string(),
                kind: Some("home".to_string()),
                is_primary: true,
            }],
            addresses: vec![bifrost_types::ContactAddress {
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

        let name = &payload.names.unwrap()[0];
        assert_eq!(name.display_name.as_deref(), None);
        assert_eq!(name.given_name.as_deref(), Some("Ada"));
        assert_eq!(name.family_name.as_deref(), None);
        assert!(payload.photos.is_none());
        let email = &payload.email_addresses.unwrap()[0];
        assert_eq!(email.value.as_deref(), Some("ada@example.test"));
        assert_eq!(email.kind.as_deref(), Some("home"));
        assert_eq!(email.formatted_type.as_deref(), None);
        assert_eq!(
            email.metadata.as_ref().and_then(|meta| meta.primary),
            Some(true)
        );
        let address = &payload.addresses.unwrap()[0];
        assert_eq!(address.kind.as_deref(), Some("work"));
        assert_eq!(address.street_address.as_deref(), Some("1 Analytical Way"));
    }

    #[test]
    fn create_maps_custom_labels_to_formatted_type() {
        let payload = person_from_create(&ContactCreate {
            emails: vec![ContactEmail {
                value: "ada@example.test".to_string(),
                kind: Some("lab".to_string()),
                is_primary: false,
            }],
            phones: vec![ContactPhone {
                value: "+123".to_string(),
                kind: Some("radio".to_string()),
                is_primary: false,
            }],
            ..ContactCreate::default()
        });
        let email = &payload.email_addresses.unwrap()[0];
        let phone = &payload.phone_numbers.unwrap()[0];

        assert_eq!(email.kind.as_deref(), Some("custom"));
        assert_eq!(email.formatted_type.as_deref(), Some("lab"));
        assert_eq!(phone.kind.as_deref(), Some("custom"));
        assert_eq!(phone.formatted_type.as_deref(), Some("radio"));
    }

    #[test]
    fn name_from_display_name_splits_given_and_family() {
        let name = name_from_display_name("Ada Byron Lovelace");
        assert_eq!(name.display_name.as_deref(), None);
        assert_eq!(name.given_name.as_deref(), Some("Ada Byron"));
        assert_eq!(name.family_name.as_deref(), Some("Lovelace"));
    }

    #[test]
    fn display_name_patch_preserves_unmodeled_name_fields() {
        let mut extra = Map::new();
        extra.insert("middleName".to_string(), serde_json::json!("Byron"));
        extra.insert("phoneticGivenName".to_string(), serde_json::json!("Ada"));
        let mut person = Person {
            etag: Some("etag".to_string()),
            names: Some(vec![
                Name {
                    display_name: Some("Ada Byron Lovelace".to_string()),
                    given_name: Some("Ada".to_string()),
                    family_name: Some("Lovelace".to_string()),
                    extra,
                },
                Name {
                    display_name: Some("Ada King".to_string()),
                    given_name: None,
                    family_name: None,
                    extra: Map::new(),
                },
            ]),
            ..Person::default()
        };

        apply_patch_to_person(
            &mut person,
            &ContactPatch {
                display_name: Some(Some("Augusta Ada King".to_string())),
                ..ContactPatch::default()
            },
        );

        let names = person.names.expect("names");
        assert_eq!(names[0].given_name.as_deref(), Some("Augusta Ada"));
        assert_eq!(names[0].family_name.as_deref(), Some("King"));
        assert_eq!(names[0].extra["middleName"].as_str(), Some("Byron"));
        assert_eq!(names[0].extra["phoneticGivenName"].as_str(), Some("Ada"));
        assert_eq!(names[1].display_name.as_deref(), Some("Ada King"));
    }

    #[test]
    fn scalar_clear_patch_removes_modeled_people_fields() {
        let mut person = Person {
            etag: Some("etag".to_string()),
            biographies: Some(vec![Biography {
                value: Some("notes".to_string()),
            }]),
            ..Person::default()
        };

        apply_patch_to_person(
            &mut person,
            &ContactPatch {
                notes: Some(None),
                photo_url: Some(None),
                ..ContactPatch::default()
            },
        );

        assert!(person.biographies.is_none());
        assert!(person.photos.is_none());
    }

    #[test]
    fn photo_url_patch_is_rejected_as_unsupported() {
        let error = reject_photo_url_patch(&ContactPatch {
            photo_url: Some(Some("https://example.test/a.jpg".to_string())),
            ..ContactPatch::default()
        })
        .expect_err("photo url patch should be unsupported");

        assert_eq!(error.operation(), Some(AccountOperation::ContactUpdate));
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::ContactUpdate)
        ));
    }

    #[test]
    fn resource_name_path_keeps_slash_literal() {
        // A People resource name's `/` is a real REST path separator: the
        // People API serves `/v1/people/c1:updateContact`, and the buggy
        // `%2F`-escaped form (`/v1/people%2Fc1...`) 404s. The helper must
        // keep the slash literal while still escaping unsafe characters.
        assert_eq!(encode_resource_name_path("people/c1"), "people/c1");
        assert_eq!(
            encode_resource_name_path("otherContacts/x1"),
            "otherContacts/x1"
        );
        assert_eq!(
            encode_resource_name_path("people/a b#c"),
            "people/a%20b%23c"
        );
    }

    #[test]
    fn contact_photo_patch_uses_people_photo_endpoint_payload() {
        let people_base = "https://people.googleapis.com/v1";
        let encoded = encode_resource_name_path("people/c1");
        // The resource name renders with a literal `/`, matching the real
        // People verb path (`/v1/people/c1:updateContactPhoto`).
        assert!(
            update_contact_photo_url(people_base, &encoded)
                .contains("people/c1:updateContactPhoto")
        );
        assert!(
            delete_contact_photo_url(people_base, &encoded)
                .contains("people/c1:deleteContactPhoto")
        );
        assert!(!update_contact_photo_url(people_base, &encoded).contains("people%2Fc1"));

        let payload = update_contact_photo_request(bifrost_types::ContactPhoto {
            data: vec![1, 2, 3, 4],
            media_type: Some("image/jpeg".to_string()),
        });
        let value = serde_json::to_value(payload).expect("photo payload");
        assert_eq!(
            value.get("photoBytes").and_then(Value::as_str),
            Some("AQIDBA==")
        );
    }

    #[test]
    fn update_fields_follow_present_patch_fields() {
        assert_eq!(
            update_fields_for_patch(&ContactPatch {
                emails: Some(Vec::new()),
                notes: Some(None),
                ..ContactPatch::default()
            }),
            "emailAddresses,biographies"
        );
        assert_eq!(
            update_fields_for_patch(&ContactPatch {
                photo_url: Some(None),
                photo: Some(None),
                address_book_id: Some(AddressBookId(CONTACTS_BOOK_ID.to_string())),
                ..ContactPatch::default()
            }),
            ""
        );
    }

    #[test]
    fn validate_address_book_accepts_google_book_and_contact_groups() {
        validate_address_book(None, AccountOperation::ContactsList).expect("default book");
        validate_address_book(
            Some(&AddressBookId(CONTACTS_BOOK_ID.to_string())),
            AccountOperation::ContactsList,
        )
        .expect("google contacts book");
        validate_address_book(
            Some(&AddressBookId("contactGroups/friends".to_string())),
            AccountOperation::ContactsList,
        )
        .expect("google contact group");

        let error = validate_address_book(
            Some(&AddressBookId("people/contactGroups/friends".to_string())),
            AccountOperation::ContactsList,
        )
        .expect_err("unsupported book should fail");

        assert_eq!(error.operation(), Some(AccountOperation::ContactsList));
    }

    #[test]
    fn contact_groups_url_includes_fields_and_page_token() {
        let url = contact_groups_url("https://people.mock.test/v1", Some("next token"));

        assert!(url.starts_with("https://people.mock.test/v1/contactGroups"));
        assert!(url.contains("contactGroups?groupFields=metadata%2Cname"));
        assert!(url.contains("pageSize=1000"));
        assert!(url.contains("pageToken=next%20token"));
    }

    #[test]
    fn contact_group_maps_to_address_book() {
        let book = address_book_from_group(ContactGroup {
            resource_name: Some("contactGroups/friends".to_string()),
            name: Some("Friends".to_string()),
        })
        .expect("address book");

        assert_eq!(book.id.0, "contactGroups/friends");
        assert_eq!(book.name, "Friends");
        assert!(!book.is_default);
        // A real group is a personal-corpus book.
        assert_eq!(book.corpus, ContactCorpus::Main);
    }

    #[test]
    fn other_contacts_book_is_read_only_auto_collected_corpus() {
        let book = other_contacts_address_book();

        assert_eq!(book.id.0, OTHER_CONTACTS_BOOK_ID);
        assert_eq!(book.corpus, ContactCorpus::OtherAutoCollected);
        assert!(!book.is_default);
        // otherContacts is read-only: no create / update / delete.
        assert!(!book.can_create_contacts);
        assert!(!book.can_update_contacts);
        assert!(!book.can_delete_contacts);
        assert!(is_other_contacts_book(Some(&AddressBookId(
            OTHER_CONTACTS_BOOK_ID.to_string()
        ))));
        assert!(!is_other_contacts_book(Some(&AddressBookId(
            CONTACTS_BOOK_ID.to_string()
        ))));
        assert!(!is_other_contacts_book(None));
    }

    #[test]
    fn other_contacts_person_maps_to_auto_collected_card() {
        // otherContacts.list returns bare People with only names/emails/phones;
        // the projection routes them to the synthetic other-contacts book with
        // the OtherAutoCollected corpus.
        let response: OtherContactsResponse = serde_json::from_value(serde_json::json!({
            "otherContacts": [{
                "resourceName": "otherContacts/x1",
                "emailAddresses": [{"value": "collected@example.test"}],
                "names": [{"displayName": "Collected Person"}]
            }],
            "nextPageToken": "tok2"
        }))
        .expect("otherContacts response");

        let page = page_from_people(
            response.other_contacts.unwrap_or_default(),
            response.next_page_token,
            None,
            ContactCorpus::OtherAutoCollected,
        );

        assert_eq!(page.next_cursor.as_deref(), Some(b"tok2".as_slice()));
        let card = &page.items[0];
        assert_eq!(card.id.0, "otherContacts/x1");
        assert_eq!(card.corpus, ContactCorpus::OtherAutoCollected);
        assert_eq!(
            card.address_book_id.as_ref().map(|book| book.0.as_str()),
            Some(OTHER_CONTACTS_BOOK_ID)
        );
        assert_eq!(card.emails[0].value, "collected@example.test");
    }

    #[test]
    fn corpus_for_native_reads_other_contacts_resource_names() {
        assert_eq!(
            corpus_for_native("otherContacts/x1"),
            ContactCorpus::OtherAutoCollected
        );
        assert_eq!(corpus_for_native("people/c1"), ContactCorpus::Main);
    }

    #[test]
    fn create_payload_writes_contact_group_membership() {
        let payload = person_from_create(&ContactCreate {
            address_book_id: Some(AddressBookId("contactGroups/friends".to_string())),
            ..ContactCreate::default()
        });

        let memberships = payload.memberships.expect("memberships");
        assert_eq!(
            memberships[0]
                .contact_group_membership
                .as_ref()
                .and_then(|membership| membership.contact_group_resource_name.as_deref()),
            Some("contactGroups/friends")
        );
    }

    #[test]
    fn person_in_group_matches_contact_group_membership() {
        let person = Person {
            memberships: Some(vec![Membership {
                contact_group_membership: Some(ContactGroupMembership {
                    contact_group_resource_name: Some("contactGroups/friends".to_string()),
                }),
            }]),
            ..Person::default()
        };

        assert!(person_in_group(&person, Some("contactGroups/friends")));
        assert!(!person_in_group(&person, Some("contactGroups/coworkers")));
        assert!(person_in_group(&person, None));
    }

    #[test]
    fn search_url_includes_page_token_and_capped_page_size() {
        let url = search_url(
            "https://people.mock.test/v1",
            &ContactSearchRequest {
                query: "Ada Lovelace".to_string(),
                address_book_id: None,
                page_cursor: Some(b"next token".to_vec()),
                limit: Some(100),
            },
        )
        .expect("search url");

        assert!(url.starts_with("https://people.mock.test/v1/people:searchContacts"));
        assert!(url.contains("query=Ada%20Lovelace"));
        assert!(url.contains("pageSize=30"));
        assert!(url.contains("pageToken=next%20token"));
    }

    #[test]
    fn search_warmup_url_uses_empty_query_and_read_mask() {
        let url = search_warmup_url("https://people.mock.test/v1");

        assert!(url.starts_with("https://people.mock.test/v1/people:searchContacts"));
        assert!(url.contains("people:searchContacts?query=&readMask="));
        assert!(url.contains("names%2CemailAddresses"));
        assert!(!url.contains("pageSize="));
    }

    #[test]
    fn require_person_etag_rejects_missing_etag() {
        let error = require_person_etag(&Person::default()).expect_err("missing etag should fail");

        assert_eq!(error.operation(), Some(AccountOperation::ContactUpdate));
    }

    fn directory_403(reason: &str) -> AccountError {
        let body = format!(
            r#"{{"error":{{"code":403,"message":"x","errors":[{{"domain":"global","reason":"{reason}"}}]}}}}"#
        );
        let error = crate::error::Error::response_from_parts(
            crate::error::GmailService::GmailApi,
            403,
            crate::error::GmailResponseHeaders::default(),
            bytes::Bytes::copy_from_slice(body.as_bytes()),
        );
        collection_error(error, AccountOperation::DirectorySearch)
    }

    #[test]
    fn directory_search_empty_query_lists_directory_people() {
        let url =
            directory_search_url("https://people.mock.test/v1", "", Some(2000), None).expect("url");

        assert!(url.starts_with("https://people.mock.test/v1/people:listDirectoryPeople"));
        assert!(url.contains("people:listDirectoryPeople"));
        assert!(url.contains("sources=DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE"));
        // limit caps at 1000.
        assert!(url.contains("pageSize=1000"));
        assert!(!url.contains("searchDirectoryPeople"));
    }

    #[test]
    fn directory_search_nonempty_query_searches_directory_people() {
        let url = directory_search_url(
            "https://people.mock.test/v1",
            "Ada Lovelace",
            None,
            Some(b"tok".as_slice()),
        )
        .expect("url");

        assert!(url.starts_with("https://people.mock.test/v1/people:searchDirectoryPeople"));
        assert!(url.contains("people:searchDirectoryPeople"));
        assert!(url.contains("query=Ada%20Lovelace"));
        assert!(url.contains("sources=DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE"));
        assert!(url.contains("pageToken=tok"));
    }

    #[test]
    fn directory_search_warmup_url_uses_empty_query() {
        let url = directory_search_warmup_url("https://people.mock.test/v1");

        assert!(url.starts_with("https://people.mock.test/v1/people:searchDirectoryPeople"));
        assert!(url.contains("people:searchDirectoryPeople?query=&"));
        assert!(url.contains("sources=DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE"));
        assert!(!url.contains("pageSize="));
    }

    #[test]
    fn directory_card_from_person_drops_emailless_and_maps_org() {
        // No email -> dropped.
        let emailless = Person {
            names: Some(vec![Name {
                display_name: Some("No Email".to_string()),
                given_name: None,
                family_name: None,
                extra: Map::new(),
            }]),
            ..Person::default()
        };
        assert!(person_to_directory_card(emailless).is_none());

        let person = Person {
            names: Some(vec![Name {
                display_name: Some("Ada Lovelace".to_string()),
                given_name: None,
                family_name: None,
                extra: Map::new(),
            }]),
            email_addresses: Some(vec![
                EmailAddress {
                    value: Some("ada@example.com".to_string()),
                    kind: None,
                    formatted_type: None,
                    metadata: None,
                },
                EmailAddress {
                    value: Some("ada.alt@example.com".to_string()),
                    kind: None,
                    formatted_type: None,
                    metadata: None,
                },
            ]),
            phone_numbers: Some(vec![PhoneNumber {
                value: Some("+15551234".to_string()),
                kind: None,
                formatted_type: None,
                metadata: None,
            }]),
            organizations: Some(vec![Organization {
                name: Some("Analytical Engines".to_string()),
                title: Some("Programmer".to_string()),
                department: Some("Research".to_string()),
            }]),
            ..Person::default()
        };
        let card = person_to_directory_card(person).expect("maps to card");
        assert_eq!(card.email, "ada@example.com");
        assert_eq!(
            card.additional_emails,
            vec!["ada.alt@example.com".to_string()]
        );
        assert_eq!(card.phones, vec!["+15551234".to_string()]);
        assert_eq!(card.company.as_deref(), Some("Analytical Engines"));
        assert_eq!(card.title.as_deref(), Some("Programmer"));
        assert_eq!(card.department.as_deref(), Some("Research"));
        assert_eq!(card.provider, ProtocolKind::Gmail);
    }

    #[test]
    fn directory_absence_403_returns_empty_page() {
        // insufficientPermissions (missing scope) -> empty page.
        let empty = directory_absence_to_empty(directory_403("insufficientPermissions"))
            .expect("swallowed");
        assert!(empty.items.is_empty());

        // bare forbidden / PERMISSION_DENIED -> empty page.
        let empty = directory_absence_to_empty(directory_403("forbidden")).expect("swallowed");
        assert!(empty.items.is_empty());

        // domainPolicy is a real admin refusal -> propagates.
        let err = directory_absence_to_empty(directory_403("domainPolicy"))
            .expect_err("policy block propagates");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        );
    }

    #[test]
    fn is_directory_absence_classifies_absence_vs_refusal() {
        // PermissionDenied / InsufficientScope == "no directory" (absence).
        assert!(is_directory_absence(&directory_403("forbidden")));
        assert!(is_directory_absence(&directory_403(
            "insufficientPermissions"
        )));
        // notAuthorizedToAccessThisResource classifies as PermissionDenied,
        // so on a first-page probe it reads as absence (documented swallow
        // set); the first-page gate keeps it from masking later pages.
        assert!(is_directory_absence(&directory_403(
            "notAuthorizedToAccessThisResource"
        )));
        // domainPolicy -> PolicyBlocked is a real refusal, not absence.
        assert!(!is_directory_absence(&directory_403("domainPolicy")));
    }

    #[test]
    fn transient_directory_failure_is_not_absence() {
        // A 429/5xx warmup or query blip must not be mistaken for absence;
        // these are retryable and the directory-search logic falls through
        // (warmup) or propagates (paginated query) rather than swallowing.
        let body = r#"{"error":{"code":429,"message":"x","errors":[{"domain":"usageLimits","reason":"rateLimitExceeded"}]}}"#;
        let error = crate::error::Error::response_from_parts(
            crate::error::GmailService::GmailApi,
            429,
            crate::error::GmailResponseHeaders::default(),
            bytes::Bytes::copy_from_slice(body.as_bytes()),
        );
        let err = collection_error(error, AccountOperation::DirectorySearch);
        assert!(!is_directory_absence(&err));
        assert!(err.recovery().is_retryable());
    }

    #[test]
    fn photo_only_patch_writes_no_modeled_fields() {
        // A photo-only patch needs `updateContactPhoto` (no etag), so
        // `update()` must not require an etag for it: `update_fields` is
        // empty, which is what gates the etag requirement.
        let fields = update_fields_for_patch(&ContactPatch {
            photo: Some(Some(bifrost_types::ContactPhoto {
                data: vec![1, 2, 3],
                media_type: Some("image/png".to_string()),
            })),
            ..ContactPatch::default()
        });
        assert_eq!(fields, "");
    }
}
