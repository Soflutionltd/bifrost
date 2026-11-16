//! Google People contacts and directory search.
//!
//! Audit boundary: the 2026-07 google bug sweep did NOT line-audit this
//! module; it was skipped as already test-dense. A later auditor should read
//! it fresh rather than assuming that sweep covered it.

use bifrost_types::{
    Application, ErrorScope, PartialCommitBuilder, PrimaryLeg, StepEffect, StepReplay, StepTargets,
    TargetRef, TargetRole,
};
use std::collections::HashSet;
use std::sync::Arc;

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AddressBook, AddressBookId, ContactCard,
    ContactCorpus, ContactCreate, ContactEmail, ContactId, ContactOrganization, ContactPatch,
    ContactPhone, ContactProvenance, ContactSearchRequest, DirectoryCard, Page, ProtocolKind,
};
use serde_json::{Map, Value};

use crate::client::GmailClient;
use crate::people::api::{
    PERSON_FIELDS, caller_input_error, contact_groups_url, delete_contact_photo_url,
    directory_search_url, directory_search_warmup_url, encode_resource_name_path, search_url,
    search_warmup_url, update_contact_photo_request, update_contact_photo_url,
};
use crate::people::types::*;

use super::error::{self, GmailErrorContext};
use super::non_empty;

const CONTACTS_BOOK_ID: &str = "google:contacts";
/// Synthetic address book that surfaces Google People `otherContacts`
/// (auto-collected addresses harvested from mail traffic) as a distinct,
/// `OtherAutoCollected`-corpus book so the consumer routes it to its own
/// local store without matching on the provider.
const OTHER_CONTACTS_BOOK_ID: &str = "google:other-contacts";
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
                .people()
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

/// Translate shared pagination admission into this API's diagnostics.
/// Repeat precedence and page-budget policy live in bifrost-net.
fn address_book_walk_refusal(
    seen_page_tokens: &mut HashSet<String>,
    pages_walked: usize,
    next_page_token: &str,
) -> Option<crate::error::PageRefusal> {
    match bifrost_net::pagination::check_next(
        seen_page_tokens,
        pages_walked,
        MAX_ADDRESS_BOOK_PAGES,
        Some(next_page_token.to_string()),
    ) {
        Ok(()) => None,
        Err(bifrost_net::pagination::PageRefusal::Repeated) => {
            Some(crate::error::PageRefusal::RepeatedToken(format!(
                "google contactGroups.list repeated page token {next_page_token:?}"
            )))
        }
        Err(_) => Some(crate::error::PageRefusal::BudgetExceeded(format!(
            "google contactGroups.list exceeded {MAX_ADDRESS_BOOK_PAGES} pages in one walk"
        ))),
    }
}

pub(crate) fn list(
    client: Arc<GmailClient>,
    address_book: Option<AddressBookId>,
    page_cursor: Option<bifrost_types::PageCursor>,
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
        let page_token = super::page_cursor::decode(page_cursor, AccountOperation::ContactsList)?;
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
            .people()
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
    page_cursor: Option<bifrost_types::PageCursor>,
) -> Result<Page<ContactCard>, AccountError> {
    let page_token = super::page_cursor::decode(page_cursor, AccountOperation::ContactsList)?;
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
        .people()
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
            .people()
            .post(&url, &person, bifrost_net::ReplayPolicy::Never)
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

// A first write may commit before the next leg finishes. On shutdown the
// adapter loses that progress, so never replay the original update blindly:
// users expect a moved event or edited contact to be reconciled first. The
// PartialCommit builder below retains the continuation on ordinary failures.
pub(crate) const UPDATE_SHUTDOWN_IDEMPOTENT: bool = false;

pub(crate) fn update(
    client: Arc<GmailClient>,
    contact: ContactId,
    patch: ContactPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        // People carries an opaque source token in JSON, not an If-Match
        // header. Preserve it verbatim rather than applying HTTP quoting.
        // ServerVersion::expected_etag validates HTTP entity-tag syntax;
        // sharing it with Calendar would remove support for opaque People
        // tokens such as caller-old. Empty tokens remain Unsupported here.
        let expected = patch
            .expected_version
            .as_ref()
            .map(|version| {
                version
                    .expected_opaque_etag(
                        bifrost_types::Protocol::GooglePeople,
                        AccountOperation::ContactUpdate,
                    )
                    .map(str::to_owned)
            })
            .transpose()?;
        let update_fields = update_fields_for_patch(&patch);
        if expected.is_some() && (patch.photo.is_some() || update_fields.is_empty()) {
            return Err(AccountError::unsupported(
                AccountOperation::ContactUpdate,
                bifrost_types::error::Protocol::GooglePeople,
            ));
        }
        // Pure-local guards first: fail an invalid request cheaply, before
        // the network GET + etag round trip.
        validate_address_book(
            patch.address_book_id.as_ref(),
            AccountOperation::ContactUpdate,
        )?;
        reject_photo_url_patch(&patch)?;
        let mut person = get_person(&client, &contact, AccountOperation::ContactUpdate).await?;
        let fields_updated = !update_fields.is_empty();
        if !update_fields.is_empty() {
            // updateContact requires source metadata and compares the CONTACT
            // source etag. The docs do not say which source to choose when
            // there are several CONTACT sources, so unversioned updates keep
            // the prior merged-Person-etag behavior when no unique source
            // token is available. Person.etag is documented only for resource
            // cache validation, so omit it when sending a source token.
            // https://developers.google.com/people/api/rest/v1/people/updateContact
            // https://developers.google.com/people/api/rest/v1/people#Person
            // https://developers.google.com/people/api/rest/v1/people#Source
            // A caller token is never replaced with the GET's newer token or
            // refreshed after a rejected update.
            if expected.is_some() || contact_source_etag(&person).is_some() {
                let observed = require_person_etag(&person)?;
                let etag = expected.unwrap_or(observed);
                let source = unique_contact_source_mut(&mut person)
                    .expect("validated unique contact source");
                source["etag"] = Value::String(etag);
                person.etag = None;
            } else {
                if has_contact_source(&person) {
                    // This preserves the old unversioned request when source
                    // metadata has CONTACT sources but no unique token. Do
                    // not invent a source etag from the merged Person cache
                    // validator.
                    require_merged_person_etag(&person)?;
                } else {
                    // updateContact documents a refusal when the person has
                    // no CONTACT source. In particular, OTHER_CONTACT is
                    // not writable through this method.
                    return Err(local_error_with_field(
                        AccountOperation::ContactUpdate,
                        "contactEtag",
                        "Google People updateContact requires a CONTACT source etag".to_string(),
                    ));
                }
            }
            apply_patch_to_person(&mut person, &patch);
            person.resource_name = Some(contact.0.clone());
            let encoded = encode_resource_name_path(&contact.0);
            let url = format!(
                "{}/{encoded}:updateContact?updatePersonFields={}&personFields={}",
                client.people_base(),
                bifrost_net::url::encode_query_value(&update_fields),
                bifrost_net::url::encode_query_value(PERSON_FIELDS)
            );
            let _: Person = client
                .people()
                .patch(&url, &person, bifrost_net::ReplayPolicy::Never)
                .await
                .map_err(|error| {
                    contact_error(error, AccountOperation::ContactUpdate, contact.0.clone())
                })?;
        }
        if let Some(photo) = patch.photo
            && let Err(error) = update_contact_photo(&client, &contact, photo).await
        {
            return Err(if fields_updated {
                fields_then_photo_error(error, &contact)
            } else {
                error
            });
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
        // People documents PATCH for this action, despite the colon verb path:
        // https://developers.google.com/people/api/rest/v1/people/updateContactPhoto
        let _: serde_json::Value = client
            .people()
            .patch(
                &url,
                &update_contact_photo_request(photo),
                bifrost_net::ReplayPolicy::Idempotent,
            )
            .await
            .map_err(|error| {
                contact_error(error, AccountOperation::ContactUpdate, contact.0.clone())
            })?;
    } else {
        let url = delete_contact_photo_url(client.people_base(), &encoded);
        let _: Person = client
            .people()
            .delete(&url, bifrost_net::ReplayPolicy::Never)
            .await
            .map(|_| Person::default())
            .map_err(|error| {
                contact_error(error, AccountOperation::ContactUpdate, contact.0.clone())
            })?;
    }
    Ok(())
}

/// The field write landed, then the photo write failed. Both are `Repeatable`:
/// an unversioned replay re-fetches source metadata and re-applies the same
/// values, and versioned photo patches are refused before any I/O. The photo
/// leaf keeps its classification and transmission evidence, so `recovery()`
/// equals the photo leg's recovery.
fn fields_then_photo_error(error: AccountError, contact: &ContactId) -> AccountError {
    let targets = StepTargets::complete(vec![TargetRef::new(
        TargetRole::Subject,
        ErrorScope::Contact {
            id: contact.clone(),
        },
    )]);
    let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
        StepEffect::Updated,
        targets.clone(),
        StepReplay::Repeatable,
        Application::from_leaf(&error),
    ));
    record.committed(StepEffect::Updated, targets, StepReplay::Repeatable);
    error
        .into_builder()
        .partial_commit(record)
        .try_build()
        .expect("valid contact photo record")
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
        .people()
        .get(&url)
        .await
        .map_err(|error| contact_error(error, operation, contact.0.clone()))
}

pub(crate) fn delete(
    client: Arc<GmailClient>,
    contact: ContactId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        // deleteContact accepts only resourceName and an empty body, with
        // no documented ETag precondition. Keep the inherited versioned
        // delete refusal; a GET comparison would leave a write race.
        // https://developers.google.com/people/api/rest/v1/people/deleteContact
        let encoded = encode_resource_name_path(&contact.0);
        let url = format!("{}/{encoded}:deleteContact", client.people_base());
        client
            .people()
            .delete(&url, bifrost_net::ReplayPolicy::Never)
            .await
            .map_err(|error| contact_error(error, AccountOperation::ContactDelete, contact.0))
    })
}

pub(crate) fn search(
    client: Arc<GmailClient>,
    request: ContactSearchRequest,
) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
    Box::pin(async move {
        if let Some(cursor) = &request.page_cursor {
            super::page_cursor::token(cursor, AccountOperation::ContactSearch)?;
        }
        validate_address_book(
            request.address_book_id.as_ref(),
            AccountOperation::ContactSearch,
        )?;
        if let Err(error) = client
            .people()
            .get::<PeopleSearchResponse>(&search_warmup_url(client.people_base()))
            .await
        {
            let error = collection_error(error, AccountOperation::ContactSearch);
            if error.recovery().is_terminal() {
                return Err(error);
            }
            // This request only primes the search cache. A transient failure
            // does not make the actual contact query less authoritative.
        }
        let url = search_url(client.people_base(), &request)?;
        let response: PeopleSearchResponse = client
            .people()
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
            next_cursor: response
                .next_page_token
                .map(|token| super::page_cursor::encode(AccountOperation::ContactSearch, &token)),
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
/// before the real query. Transient warmup failures are non-fatal. Authorization
/// refusals on either request propagate: enterprise users expect lost directory
/// access to be actionable, rather than an apparently empty organization.
pub(crate) fn directory_search(
    client: Arc<GmailClient>,
    query: String,
    limit: Option<u32>,
    page_cursor: Option<bifrost_types::PageCursor>,
) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
    Box::pin(async move {
        if let Some(cursor) = &page_cursor {
            super::page_cursor::token(cursor, AccountOperation::DirectorySearch)?;
        }
        if !query.is_empty() {
            // A transient cache-priming failure can defer to the real query.
            // A terminal refusal must remain visible even on the first page.
            if let Err(error) = client
                .people()
                .get::<DirectoryPeopleResponse>(&directory_search_warmup_url(client.people_base()))
                .await
            {
                let err = collection_error(error, AccountOperation::DirectorySearch);
                if err.recovery().is_terminal() {
                    // Authorization and policy refusals need user-visible recovery.
                    return Err(err);
                }
                // Transient: ignore and proceed to the real query.
            }
        }
        let url =
            directory_search_url(client.people_base(), &query, limit, page_cursor.as_deref())?;
        let response: DirectoryPeopleResponse = client
            .people()
            .get(&url)
            .await
            .map_err(|error| collection_error(error, AccountOperation::DirectorySearch))?;
        let items = response
            .people
            .unwrap_or_default()
            .into_iter()
            .filter_map(person_to_directory_card)
            .collect();
        Ok(Page {
            truncated: false,
            items,
            next_cursor: response
                .next_page_token
                .map(|token| super::page_cursor::encode(AccountOperation::DirectorySearch, &token)),
            estimated_total: None,
            failed_ids: Vec::new(),
            skipped_scopes: Vec::new(),
        })
    })
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
        next_cursor: next_page_token
            .map(|token| super::page_cursor::encode(AccountOperation::ContactsList, &token)),
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
    let version = contact_source_etag(&person).map(|tag| {
        bifrost_types::ServerVersion::etag(
            bifrost_types::Protocol::GooglePeople,
            bifrost_types::ObjectType::Contact,
            tag,
        )
    });
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
        version,
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
        metadata: None,
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

// Person and Source expose distinct ETags. A unique CONTACT source token is
// the update precondition; the merged Person token is only a cache validator.
// https://developers.google.com/people/api/rest/v1/people#Person
// https://developers.google.com/people/api/rest/v1/people#Source
fn contact_source_etag(person: &Person) -> Option<&str> {
    unique_contact_source(person)?
        .get("etag")?
        .as_str()
        .filter(|etag| !etag.is_empty())
}

fn has_contact_source(person: &Person) -> bool {
    person
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("sources"))
        .and_then(Value::as_array)
        .is_some_and(|sources| sources.iter().any(|source| source["type"] == "CONTACT"))
}

fn unique_contact_source(person: &Person) -> Option<&Value> {
    let sources = person.metadata.as_ref()?.get("sources")?.as_array()?;
    sources.get(unique_contact_source_index(sources)?)
}

fn unique_contact_source_mut(person: &mut Person) -> Option<&mut Value> {
    let sources = person
        .metadata
        .as_mut()?
        .get_mut("sources")?
        .as_array_mut()?;
    let index = unique_contact_source_index(sources)?;
    sources.get_mut(index)
}

fn unique_contact_source_index(sources: &[Value]) -> Option<usize> {
    let mut contact_index = None;
    for (index, source) in sources.iter().enumerate() {
        if source["type"] == "CONTACT" {
            if contact_index.is_some() {
                return None;
            }
            contact_index = Some(index);
        }
    }
    contact_index
}

fn require_person_etag(person: &Person) -> Result<String, AccountError> {
    contact_source_etag(person)
        .map(str::to_owned)
        .ok_or_else(|| {
            local_error_with_field(
                AccountOperation::ContactUpdate,
                "contactEtag",
                "Google People updateContact requires a CONTACT source etag".to_string(),
            )
        })
}

fn require_merged_person_etag(person: &Person) -> Result<(), AccountError> {
    person.etag.as_ref().map(|_| ()).ok_or_else(|| {
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

#[cfg(test)]
mod tests {
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use bifrost_types::{AccessErrorKind, AccountErrorKind};
    use bytes::Bytes;
    use reqwest::StatusCode;

    use super::*;

    fn scripted_client(body: &'static [u8]) -> (Arc<GmailClient>, Arc<ScriptedDispatch>) {
        scripted_client_steps(vec![Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from_static(body),
        }])
    }

    fn scripted_client_steps(steps: Vec<Canned>) -> (Arc<GmailClient>, Arc<ScriptedDispatch>) {
        let script = ScriptedDispatch::new(steps);
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
    async fn field_update_sends_contact_source_and_preserves_caller_precondition() {
        for expected in [None, Some("caller-old")] {
            let (client, script) = scripted_client_steps(vec![
                Canned::Response {
                    status: StatusCode::OK,
                    headers: reqwest::header::HeaderMap::new(),
                    body: Bytes::from_static(br#"{"resourceName":"people/c1","etag":"merged","metadata":{"sources":[{"type":"PROFILE","id":"p1","etag":"profile"},{"type":"CONTACT","id":"c1","etag":"source-new"}]}}"#),
                },
                Canned::Response {
                    status: StatusCode::BAD_REQUEST,
                    headers: reqwest::header::HeaderMap::new(),
                    body: Bytes::from_static(br#"{"error":{"code":400,"message":"stale","errors":[{"reason":"failedPrecondition"}]}}"#),
                },
            ]);
            let error = update(
                client,
                ContactId("people/c1".into()),
                ContactPatch {
                    display_name: Some(Some("Ada".into())),
                    expected_version: expected.map(|etag| {
                        bifrost_types::ServerVersion::etag(
                            bifrost_types::Protocol::GooglePeople,
                            bifrost_types::ObjectType::Contact,
                            etag,
                        )
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
            assert_eq!(*error.kind(), AccountErrorKind::ConcurrencyConflict);
            let requests = script.requests();
            assert_eq!(
                requests.len(),
                2,
                "stale tokens must not be refreshed and retried"
            );
            let body: Value = serde_json::from_slice(requests[1].body.as_ref().unwrap()).unwrap();
            assert_eq!(
                body["metadata"]["sources"][1]["etag"],
                expected.unwrap_or("source-new")
            );
            assert!(body.get("etag").is_none());
            assert_eq!(body["metadata"]["sources"][1]["id"], "c1");
            assert_eq!(body["metadata"]["sources"][0]["etag"], "profile");
        }
    }

    #[tokio::test]
    async fn unversioned_field_update_falls_back_when_contact_source_token_is_not_unique() {
        for sources in [
            serde_json::json!([
                {"type":"CONTACT","id":"c1","etag":"source-one"},
                {"type":"CONTACT","id":"c2","etag":"source-two"}
            ]),
            serde_json::json!([
                {"type":"CONTACT","id":"c1"}
            ]),
        ] {
            let get_body = serde_json::json!({
                "resourceName": "people/c1",
                "etag": "merged",
                "metadata": {"sources": sources}
            })
            .to_string();
            let (client, script) = scripted_client_steps(vec![
                Canned::Response {
                    status: StatusCode::OK,
                    headers: reqwest::header::HeaderMap::new(),
                    body: Bytes::from(get_body),
                },
                Canned::Response {
                    status: StatusCode::OK,
                    headers: reqwest::header::HeaderMap::new(),
                    body: Bytes::from_static(b"{}"),
                },
            ]);

            update(
                client,
                ContactId("people/c1".into()),
                ContactPatch {
                    display_name: Some(Some("Ada".into())),
                    ..Default::default()
                },
            )
            .await
            .expect("unversioned update retains the prior fallback");

            let requests = script.requests();
            assert_eq!(requests.len(), 2);
            let body: Value = serde_json::from_slice(requests[1].body.as_ref().unwrap()).unwrap();
            assert_eq!(body["etag"], "merged");
            assert_eq!(body["metadata"]["sources"], sources);
        }
    }

    #[tokio::test]
    async fn versioned_field_update_does_not_fall_back_for_ambiguous_contact_sources() {
        let (client, script) = scripted_client_steps(vec![Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from_static(br#"{"resourceName":"people/c1","etag":"merged","metadata":{"sources":[{"type":"CONTACT","id":"c1","etag":"one"},{"type":"CONTACT","id":"c2","etag":"two"}]}}"#),
        }]);

        let error = update(
            client,
            ContactId("people/c1".into()),
            ContactPatch {
                display_name: Some(Some("Ada".into())),
                expected_version: Some(bifrost_types::ServerVersion::etag(
                    bifrost_types::Protocol::GooglePeople,
                    bifrost_types::ObjectType::Contact,
                    "caller-old",
                )),
                ..Default::default()
            },
        )
        .await
        .expect_err("caller version cannot be mapped to one CONTACT source");

        assert_eq!(error.operation(), Some(AccountOperation::ContactUpdate));
        assert_eq!(script.requests().len(), 1);
    }

    #[test]
    fn contact_cards_expose_source_tokens_and_never_fall_back_to_person_etag() {
        for (metadata, expected) in [
            (
                serde_json::json!({"sources":[{"type":"PROFILE","etag":"profile"},{"type":"CONTACT","etag":"source"}]}),
                Some("source"),
            ),
            (
                serde_json::json!({"sources":[{"type":"CONTACT","etag":"source-one"},{"type":"CONTACT","etag":"source-two"}]}),
                None,
            ),
            (
                serde_json::json!({"sources":[{"type":"PROFILE","etag":"profile"}]}),
                None,
            ),
            (serde_json::json!({"sources":[{"type":"CONTACT"}]}), None),
            (
                serde_json::json!({"sources":[{"type":"OTHER_CONTACT","etag":"other"}]}),
                None,
            ),
        ] {
            let person = Person {
                etag: Some("merged".into()),
                metadata: Some(metadata),
                ..Default::default()
            };
            assert_eq!(contact_source_etag(&person), expected);
            assert_eq!(require_person_etag(&person).is_ok(), expected.is_some());
            assert_eq!(
                contact_from_person(person, ContactCorpus::Main)
                    .version
                    .as_ref()
                    .map(|version| version
                        .expected_opaque_etag(
                            bifrost_types::Protocol::GooglePeople,
                            AccountOperation::ContactUpdate
                        )
                        .unwrap()),
                expected
            );
        }
    }

    #[tokio::test]
    async fn versioned_photo_patch_is_refused_before_io() {
        let (client, script) = scripted_client_steps(Vec::new());
        let error = update(
            client,
            ContactId("people/c1".into()),
            ContactPatch {
                display_name: Some(Some("Ada".into())),
                photo: Some(None),
                expected_version: Some(bifrost_types::ServerVersion::etag(
                    bifrost_types::Protocol::GooglePeople,
                    bifrost_types::ObjectType::Contact,
                    "old",
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            *error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::ContactUpdate)
        );
        assert!(script.requests().is_empty());
    }

    #[tokio::test]
    async fn versioned_empty_contact_patch_is_refused_before_io() {
        let (client, script) = scripted_client_steps(Vec::new());
        let error = update(
            client,
            ContactId("calendar::one".into()),
            ContactPatch {
                expected_version: Some(bifrost_types::ServerVersion::etag(
                    bifrost_types::Protocol::GooglePeople,
                    bifrost_types::ObjectType::Contact,
                    "\"old\"",
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            *error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::ContactUpdate)
        );
        assert!(script.requests().is_empty());
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

    #[tokio::test]
    async fn photo_failure_after_field_update_preserves_permission_failure() {
        let mut photo_error_headers = reqwest::header::HeaderMap::new();
        photo_error_headers.insert(
            reqwest::header::HeaderName::from_static("x-goog-request-id"),
            reqwest::header::HeaderValue::from_static("people-req-9"),
        );
        let (client, script) = scripted_client_steps(vec![
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from_static(br#"{"resourceName":"people/c1","etag":"etag-1","metadata":{"sources":[{"type":"CONTACT","id":"c1","etag":"source-1"}]}}"#),
            },
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from_static(br#"{"resourceName":"people/c1","etag":"etag-2"}"#),
            },
            Canned::Response {
                status: StatusCode::FORBIDDEN,
                headers: photo_error_headers,
                body: Bytes::from_static(
                    br#"{"error":{"code":403,"message":"photo denied","errors":[{"reason":"forbidden","message":"photo denied"}]}}"#,
                ),
            },
        ]);
        let error = update(
            client,
            ContactId("people/c1".to_string()),
            ContactPatch {
                display_name: Some(Some("Ada Lovelace".to_string())),
                photo: Some(Some(bifrost_types::ContactPhoto {
                    data: vec![1, 2, 3],
                    media_type: Some("image/png".to_string()),
                })),
                ..ContactPatch::default()
            },
        )
        .await
        .expect_err("photo write fails after the field update succeeded");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ));
        assert!(matches!(
            error.recovery(),
            bifrost_types::RecoveryClass::NoPermission { .. }
        ));
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            bifrost_types::Cause::Attempt(attempt)
                if attempt.transmission_state == bifrost_types::TransmissionState::Acknowledged
        )));
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Contact {
                id: ContactId("people/c1".to_string())
            })
        );
        let telemetry = error.telemetry_fields();
        assert_eq!(telemetry.status, Some(403));
        assert_eq!(telemetry.request_id, Some("people-req-9"));
        // A People failure carries the People wire signal, not Gmail's: it is
        // not a Gmail fault and must not send the user to grant mail permissions.
        assert!(error.chain().iter().any(|cause| matches!(
            cause.wire(),
            Some(bifrost_types::WireCause::GooglePeople(_))
        )));
        assert!(
            error
                .support_consented()
                .support_text
                .iter()
                .any(|text| text.contains("photo denied"))
        );
        assert_eq!(
            error.partial_commit().unwrap().steps()[0].effect(),
            Some(StepEffect::Updated)
        );

        let requests = script.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method, reqwest::Method::GET);
        assert_eq!(requests[1].method, reqwest::Method::PATCH);
        assert!(requests[2].url.path().ends_with(":updateContactPhoto"));
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
            metadata: None,
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

    #[tokio::test]
    async fn contact_photo_update_sends_patch() {
        let (client, script) = scripted_client(b"{}");
        update_contact_photo(
            &client,
            &ContactId("people/c1".into()),
            Some(bifrost_types::ContactPhoto {
                data: vec![1, 2, 3, 4],
                media_type: Some("image/jpeg".into()),
            }),
        )
        .await
        .expect("photo update");
        let requests = script.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, reqwest::Method::PATCH);
        assert!(
            requests[0]
                .url
                .as_str()
                .ends_with("/people/c1:updateContactPhoto")
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

        assert_eq!(
            super::super::page_cursor::decode(page.next_cursor, AccountOperation::ContactsList)
                .unwrap()
                .as_deref(),
            Some("tok2")
        );
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

    #[tokio::test]
    async fn contact_cursor_is_refused_by_directory_before_warmup() {
        let (client, script) = scripted_client(br#"{"connections":[],"nextPageToken":"next"}"#);
        let cursor = list(Arc::clone(&client), None, None)
            .await
            .expect("contacts page")
            .next_cursor
            .expect("continuation");
        let error = directory_search(client, "Ada".into(), None, Some(cursor))
            .await
            .expect_err("foreign cursor must be refused before warmup");
        assert_eq!(error.operation(), Some(AccountOperation::DirectorySearch));
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            bifrost_types::Cause::Request(bifrost_types::RequestCause::InvalidArgument {
                field: Some("page_cursor"),
                ..
            })
        )));
        assert_eq!(
            script.requests().len(),
            1,
            "only the contacts page was fetched"
        );
    }

    #[tokio::test]
    async fn transient_contact_search_warmup_failure_falls_through_to_query() {
        let (client, script) = scripted_client_steps(vec![
            Canned::Response {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from_static(
                    br#"{"error":{"code":500,"errors":[{"reason":"backendError"}]}}"#,
                ),
            },
            Canned::Response {
                status: StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::from_static(br#"{"results":[]}"#),
            },
        ]);

        let page = search(client, ContactSearchRequest::new("Ada"))
            .await
            .expect("the actual search remains authoritative after a warmup blip");

        assert!(page.items.is_empty());
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].url.as_str().contains("query=&readMask="));
        assert!(requests[1].url.as_str().contains("query=Ada"));
    }

    #[tokio::test]
    async fn terminal_contact_search_warmup_failure_still_propagates() {
        let (client, script) = scripted_client_steps(vec![Canned::Response {
            status: StatusCode::FORBIDDEN,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from_static(
                br#"{"error":{"code":403,"errors":[{"reason":"domainPolicy"}]}}"#,
            ),
        }]);

        let error = search(client, ContactSearchRequest::new("Ada"))
            .await
            .expect_err("terminal authorization refusal is not a cache-prime blip");

        assert!(matches!(
            error.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
        assert_eq!(script.requests().len(), 1);
    }

    #[test]
    fn require_person_etag_rejects_missing_etag() {
        let error = require_person_etag(&Person::default()).expect_err("missing etag should fail");

        assert_eq!(error.operation(), Some(AccountOperation::ContactUpdate));
    }

    #[tokio::test]
    async fn directory_authorization_refusals_propagate_on_warmup_and_query() {
        for query in ["", "Ada"] {
            for reason in ["forbidden", "insufficientPermissions", "domainPolicy"] {
                let body =
                    format!(r#"{{"error":{{"code":403,"errors":[{{"reason":"{reason}"}}]}}}}"#);
                let (client, script) = scripted_client_steps(vec![Canned::Response {
                    status: StatusCode::FORBIDDEN,
                    headers: reqwest::header::HeaderMap::new(),
                    body: Bytes::from(body),
                }]);
                let error = directory_search(client, query.to_string(), None, None)
                    .await
                    .expect_err("lost access must not appear as an empty directory");
                assert!(matches!(error.kind(), AccountErrorKind::Authorization(_)));
                assert_eq!(script.requests().len(), 1);
            }
        }
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
