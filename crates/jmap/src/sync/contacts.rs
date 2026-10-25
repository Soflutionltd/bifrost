//! JSContact (RFC 9553) <-> shared contact types.
//!
//! As with the calendar mapper, every shape here was derived STATICALLY from
//! the RFC and the crate's own types - the project's testing rules keep live
//! servers out of this workspace, so "a conforming server accepts this" is a
//! reading of the spec, never an observation. An in-process round trip
//! through these functions proves the two directions agree with each other,
//! which is strictly weaker than proving either agrees with a real server.

use bifrost_types::AddressBookId as SharedAddressBookId;
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, AddressBook, ContactAddress, ContactCard,
    ContactCorpus, ContactCreate, ContactEmail, ContactId, ContactOrganization, ContactPatch,
    ContactPhone, ContactProvenance, ContactSearchRequest, Page, ProtocolKind,
};
use serde_json::{Map, Value, json};

use crate::account::Account as JmapProtoAccount;
use crate::address_book::{AddressBookGet, AddressBookId as JmapAddressBookId};
use crate::contact_card::query::Filter as ContactFilter;
use crate::contact_card::{
    ContactCard as JmapContactCard, ContactCardCreate, ContactCardGet, ContactCardId,
    ContactCardPatch, ContactCardQuery, ContactCardSet,
};
use crate::core::SetCreate;
use crate::core::transport::HttpTransport;
use crate::sync::page_cursor::PageKind;

type ContactAccount<T> = JmapProtoAccount<T>;

const PAGE_LIMIT: usize = 100;

pub(crate) fn address_books_list<T: HttpTransport>(
    contacts: Option<ContactAccount<T>>,
) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::AddressBooksList)?;
        let response = contacts
            .call(AddressBookGet::new())
            .await
            .map_err(to_acct_err(AccountOperation::AddressBooksList))?;
        Ok(response
            .into_list()
            .into_iter()
            .map(address_book_from_jmap)
            .collect())
    })
}

pub(crate) fn list<T: HttpTransport>(
    contacts: Option<ContactAccount<T>>,
    address_book: Option<SharedAddressBookId>,
    page_cursor: Option<Vec<u8>>,
) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactsList)?;
        let cursor = ContactPages::decode_page_cursor(page_cursor, AccountOperation::ContactsList)?;
        let mut query = ContactCardQuery::new()
            .limit(PAGE_LIMIT)
            .calculate_total(true);
        query = ContactPages::anchor_query(query, cursor.as_ref());
        if let Some(book) = address_book {
            query = query.filter(ContactFilter::in_address_book(JmapAddressBookId::new(
                book.0,
            )));
        }
        let query_response = contacts
            .call(query)
            .await
            .map_err(ContactPages::page_call_err(
                AccountOperation::ContactsList,
                cursor.is_some(),
            ))?;
        // Before the total, before the cursor, before hydration: an
        // inconsistent continuation must expose no items and no successor.
        ContactPages::verify_query_state(
            cursor.as_ref(),
            query_response.query_state(),
            AccountOperation::ContactsList,
        )?;
        let total = query_response
            .total()
            .map(|total| usize_to_u64(total, AccountOperation::ContactsList))
            .transpose()?;
        let next_cursor = ContactPages::next_cursor(
            cursor.as_ref(),
            query_response.position(),
            query_response.ids(),
            total,
            query_response.query_state(),
            AccountOperation::ContactsList,
        )?;
        let hydrated = get_cards(
            &contacts,
            query_response.into_ids(),
            AccountOperation::ContactsList,
        )
        .await?;
        Ok(Page {
            truncated: false,
            items: hydrated.cards,
            next_cursor,
            estimated_total: total,
            failed_ids: hydrated.failed_ids,
            skipped_scopes: Vec::new(),
        })
    })
}

pub(crate) fn get<T: HttpTransport>(
    contacts: Option<ContactAccount<T>>,
    contact: ContactId,
) -> AccountFuture<Result<ContactCard, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactGet)?;
        let hydrated = get_cards(
            &contacts,
            vec![ContactCardId::new(contact.0)],
            AccountOperation::ContactGet,
        )
        .await?;
        hydrated
            .cards
            .into_iter()
            .next()
            .ok_or_else(|| unsupported(AccountOperation::ContactGet, "JMAP contact was not found"))
    })
}

pub(crate) fn create<T: HttpTransport>(
    contacts: Option<ContactAccount<T>>,
    contact: ContactCreate,
) -> AccountFuture<Result<ContactId, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactCreate)?;
        let mut set = ContactCardSet::new();
        let create_id = set.create_item(jmap_create_from_contact(&contact));
        let mut response = contacts
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContactCreate))?;
        let card = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::ContactCreate))?;
        let id = card
            .id()
            .map(ContactCardId::into_string)
            .unwrap_or_default();
        Ok(ContactId(id))
    })
}

pub(crate) fn update<T: HttpTransport>(
    contacts: Option<ContactAccount<T>>,
    contact: ContactId,
    patch: ContactPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let expected = super::mutation::caller_state(
            patch.expected_version.as_ref(),
            AccountOperation::ContactUpdate,
        )?;
        let contacts = require_contacts(contacts, AccountOperation::ContactUpdate)?;
        if patch.photo.is_some() {
            return Err(unsupported(
                AccountOperation::ContactUpdate,
                "JMAP contact raw photo updates are unsupported; use photo_url media instead",
            ));
        }
        let id = ContactCardId::new(contact.0);
        // An address-book move is add-new plus remove-old, and the old
        // book is only knowable from the current card. If the read did
        // not materialize one, `None` here would silently degrade the
        // move into an ADD - leaving the contact in both books, with no
        // error and nothing to reconcile against later. The read failing
        // says nothing about the card (this module documents the
        // unanswered-id lane as a transient, not a deletion), so fail the
        // update as a retryable `Protocol(PartialResponse)` rather than
        // performing a different write than the caller asked for.
        let current_address_book = if patch.address_book_id.is_some() {
            get_cards(&contacts, vec![id.clone()], AccountOperation::ContactUpdate)
                .await?
                .cards
                .into_iter()
                .next()
                .ok_or_else(|| {
                    super::error::get_id_unanswered(
                        id.as_str(),
                        super::error::JmapErrorContext::new(AccountOperation::ContactUpdate),
                    )
                })?
                .address_book_id
        } else {
            None
        };
        let mut set = ContactCardSet::new();
        let guarded = expected.is_some();
        if let Some(state) = expected {
            set = set.if_in_state(state);
        }
        set.update_item(
            id.clone(),
            jmap_patch_from_contact_patch(&patch, current_address_book.as_ref()),
        );
        let mut response = contacts
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContactUpdate))?;
        response
            .updated(&id)
            .map_err(super::error::set_answer_error(
                AccountOperation::ContactUpdate,
                "ContactCard/set",
                guarded,
            ))?;
        Ok(())
    })
}

pub(crate) fn delete<T: HttpTransport>(
    contacts: Option<ContactAccount<T>>,
    contact: ContactId,
) -> AccountFuture<Result<(), AccountError>> {
    delete_if_version(contacts, contact, None)
}

pub(crate) fn delete_if_version<T: HttpTransport>(
    contacts: Option<ContactAccount<T>>,
    contact: ContactId,
    expected_version: Option<bifrost_types::ServerVersion>,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let expected = super::mutation::caller_state(
            expected_version.as_ref(),
            AccountOperation::ContactDelete,
        )?;
        let contacts = require_contacts(contacts, AccountOperation::ContactDelete)?;
        let id = ContactCardId::new(contact.0);
        let mut set = ContactCardSet::new().destroy([id.clone()]);
        let guarded = expected.is_some();
        if let Some(state) = expected {
            set = set.if_in_state(state);
        }
        let mut response = contacts
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::ContactDelete))?;
        response
            .destroyed(&id)
            .map_err(super::error::set_answer_error(
                AccountOperation::ContactDelete,
                "ContactCard/set",
                guarded,
            ))
    })
}

pub(crate) fn search<T: HttpTransport>(
    contacts: Option<ContactAccount<T>>,
    request: ContactSearchRequest,
) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
    Box::pin(async move {
        let contacts = require_contacts(contacts, AccountOperation::ContactSearch)?;
        let cursor =
            ContactPages::decode_page_cursor(request.page_cursor, AccountOperation::ContactSearch)?;
        let limit = request
            .limit
            .and_then(|limit| usize::try_from(limit).ok())
            .unwrap_or(PAGE_LIMIT);
        let query = ContactCardQuery::new()
            .limit(limit)
            .filter(ContactFilter::text(request.query))
            .calculate_total(true);
        let query = ContactPages::anchor_query(query, cursor.as_ref());
        let query_response = contacts
            .call(query)
            .await
            .map_err(ContactPages::page_call_err(
                AccountOperation::ContactSearch,
                cursor.is_some(),
            ))?;
        // Before the total, before the cursor, before hydration: an
        // inconsistent continuation must expose no items and no successor.
        ContactPages::verify_query_state(
            cursor.as_ref(),
            query_response.query_state(),
            AccountOperation::ContactSearch,
        )?;
        let total = query_response
            .total()
            .map(|total| usize_to_u64(total, AccountOperation::ContactSearch))
            .transpose()?;
        let next_cursor = ContactPages::next_cursor(
            cursor.as_ref(),
            query_response.position(),
            query_response.ids(),
            total,
            query_response.query_state(),
            AccountOperation::ContactSearch,
        )?;
        let hydrated = get_cards(
            &contacts,
            query_response.into_ids(),
            AccountOperation::ContactSearch,
        )
        .await?;
        Ok(Page {
            truncated: false,
            items: hydrated.cards,
            next_cursor,
            estimated_total: total,
            failed_ids: hydrated.failed_ids,
            skipped_scopes: Vec::new(),
        })
    })
}

/// A hydrated page of cards plus the native ids `ContactCard/get` did not
/// answer with a card. An id that the query returned but the get could not
/// materialize is a transient per-resource hydration failure, not a
/// deletion; it rides `Page::failed_ids` so the consumer preserves the
/// row rather than destroying it.
struct HydratedCards {
    cards: Vec<ContactCard>,
    failed_ids: Vec<String>,
}

async fn get_cards<T: HttpTransport>(
    contacts: &ContactAccount<T>,
    ids: Vec<ContactCardId>,
    operation: AccountOperation,
) -> Result<HydratedCards, AccountError> {
    if ids.is_empty() {
        return Ok(HydratedCards {
            cards: Vec::new(),
            failed_ids: Vec::new(),
        });
    }
    let requested: Vec<String> = ids
        .iter()
        .cloned()
        .map(ContactCardId::into_string)
        .collect();
    let response = contacts
        .call(ContactCardGet::new().ids(ids))
        .await
        .map_err(to_acct_err(operation))?;
    let not_found = response.not_found().to_vec();
    let (cards, unanswered) =
        reconcile_cards(requested, &not_found, response.into_list(), operation)?;
    Ok(HydratedCards {
        cards,
        failed_ids: unanswered,
    })
}

/// Split a `ContactCard/get` answer into hydrated cards and the ids the
/// server did not hand back a card for.
///
/// `notFound` alone is not that set. An absent `notFound` decodes as
/// empty (RFC 8620 s5.1 requires the property, but the decoder is lenient
/// so one missing empty array cannot fail the whole request), and even a
/// present list can omit an id the server also left out of `list`. Either
/// way the id must reach `Page::failed_ids`, or the consumer reads its
/// absence from `items` as a deletion and destroys a row that still
/// exists.
fn reconcile_cards(
    requested: Vec<String>,
    not_found: &[ContactCardId],
    list: Vec<JmapContactCard>,
    operation: AccountOperation,
) -> Result<(Vec<ContactCard>, Vec<String>), AccountError> {
    let mut answered: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut failed_ids: Vec<String> = Vec::new();
    for missing in not_found {
        let missing = missing.clone().into_string();
        if answered.insert(missing.clone()) {
            failed_ids.push(missing);
        }
    }

    let mut cards = Vec::with_capacity(list.len());
    for card in list {
        // An object the server returned without a usable id is not a card:
        // `ContactId("")` is a handle to nothing, and the crate's other
        // walks (inventory, hydration, container discovery) already refuse
        // the shape. Dropping it leaves the submitted id unanswered, so it
        // still reaches `failed_ids` below instead of vanishing.
        let Some(id) = card.id().map(ContactCardId::into_string) else {
            continue;
        };
        if id.is_empty() {
            continue;
        }
        answered.insert(id);
        cards.push(contact_from_jmap(card, operation)?);
    }

    for id in requested {
        if answered.insert(id.clone()) {
            failed_ids.push(id);
        }
    }
    Ok((cards, failed_ids))
}

fn address_book_from_jmap(book: crate::address_book::AddressBook) -> AddressBook {
    let native = book
        .id
        .map(JmapAddressBookId::into_string)
        .unwrap_or_default();
    let rights = book.my_rights;
    let can_write = address_book_can_write(rights.as_ref());
    AddressBook {
        id: SharedAddressBookId(native.clone()),
        native_id: native.clone(),
        name: book.name.unwrap_or_else(|| "Address Book".to_string()),
        provenance: ContactProvenance {
            provider: ProtocolKind::Jmap,
            native,
            address_book_native: None,
        },
        corpus: ContactCorpus::Main,
        is_default: book.is_default.unwrap_or(false),
        can_create_contacts: can_write,
        can_update_contacts: can_write,
        can_delete_contacts: address_book_can_delete(rights.as_ref()),
    }
}

fn address_book_can_write(rights: Option<&crate::address_book::AddressBookRights>) -> bool {
    rights.and_then(|rights| rights.may_write).unwrap_or(false)
}

fn address_book_can_delete(rights: Option<&crate::address_book::AddressBookRights>) -> bool {
    rights.and_then(|rights| rights.may_delete).unwrap_or(false)
}

/// Project a JSContact card onto the shared `ContactCard`.
///
/// Two different failure policies live here on purpose. Postal addresses
/// REJECT: `addresses` returns an `Err` that becomes `Unsupported`, because a
/// recognized RFC 9553 component kind the shared address has no field for
/// would otherwise be accepted and then dropped, silently moving a house
/// number out of the address.
///
/// Everything else - emails, phones, notes, media, the name - SKIPS: each
/// projector is a `filter_map` whose `?` discards an entry it cannot read
/// (a non-object value, a missing `address` / `number` / `name` string) and
/// keeps the rest of the card. That asymmetry is accepted, not an
/// inconsistency waiting to be tidied. These are unordered multi-valued
/// collections in which one junk entry says nothing about the others, and a
/// server that emits one malformed phone would otherwise make the whole
/// contact unreadable - including its name and its addresses. An address is
/// a single structured value whose parts only mean anything together, so a
/// component it cannot place makes the address itself untrustworthy.
///
/// The accepted cost is that a skipped value is invisible: the caller sees a
/// card with one fewer email and no signal that one was dropped. Changing
/// that means giving these projectors a per-value failure lane on the shared
/// `ContactCard` shape, which is a `bifrost-types` surface question, not a
/// local fix here.
fn contact_from_jmap(
    card: JmapContactCard,
    operation: AccountOperation,
) -> Result<ContactCard, AccountError> {
    let native = card
        .id()
        .map(ContactCardId::into_string)
        .unwrap_or_default();
    let address_book_id = card
        .address_book_ids()
        .and_then(|ids| ids.keys().next().cloned())
        .map(SharedAddressBookId);
    let addresses =
        addresses(card.addresses()).map_err(|message| unsupported(operation, message))?;
    Ok(ContactCard {
        id: ContactId(native.clone()),
        address_book_id: address_book_id.clone(),
        native_id: native.clone(),
        etag: None,
        provenance: ContactProvenance {
            provider: ProtocolKind::Jmap,
            native,
            address_book_native: address_book_id.map(|id| id.0),
        },
        // JMAP has no auto-collected corpus; every card is personal.
        corpus: ContactCorpus::Main,
        display_name: display_name(card.name()),
        emails: emails(card.emails()),
        phones: phones(card.phones()),
        organizations: organizations(
            card.organizations(),
            card.property("titles").and_then(Value::as_object),
        ),
        addresses,
        notes: notes(card.notes()),
        photo_url: photo_url(card.media()),
        photo: None,
    })
}

fn jmap_create_from_contact(contact: &ContactCreate) -> ContactCardCreate {
    let mut create = ContactCardCreate::new(None);
    write_contact_create(&mut create.properties, contact);
    create
}

fn jmap_patch_from_contact_patch(
    patch: &ContactPatch,
    current_address_book: Option<&SharedAddressBookId>,
) -> ContactCardPatch {
    let mut out = ContactCardPatch::default();
    if let Some(address_book_id) = &patch.address_book_id {
        if let Some(current) = current_address_book
            && current.0 != address_book_id.0
        {
            out.address_book_id(JmapAddressBookId::new(current.0.clone()), false);
        }
        out.address_book_id(JmapAddressBookId::new(address_book_id.0.clone()), true);
    }
    if let Some(display_name) = &patch.display_name {
        match display_name {
            Some(name) => {
                out.name(name_object(name));
            }
            None => {
                out.set_property("name", Value::Null);
            }
        }
    }
    if let Some(emails) = &patch.emails {
        out.emails(email_object(emails));
    }
    if let Some(phones) = &patch.phones {
        out.phones(phone_object(phones));
    }
    if let Some(orgs) = &patch.organizations {
        out.organizations(organization_object(orgs));
        out.set_property("titles", Value::Object(titles_object(orgs)));
    }
    if let Some(addresses) = &patch.addresses {
        out.addresses(address_object(addresses));
    }
    if let Some(notes) = &patch.notes {
        match notes {
            Some(notes) => out.notes(notes_object(notes)),
            None => out.set_property("notes", Value::Null),
        };
    }
    if let Some(photo_url) = &patch.photo_url {
        match photo_url {
            Some(url) => {
                let mut media = Map::new();
                media.insert("photo".to_string(), photo_media_object(url));
                out.set_property("media", Value::Object(media));
            }
            None => {
                out.set_property("media", Value::Null);
            }
        }
    }
    out
}

fn write_contact_create(target: &mut Map<String, Value>, contact: &ContactCreate) {
    target.insert("@type".to_string(), Value::String("Card".to_string()));
    target.insert("kind".to_string(), Value::String("individual".to_string()));
    if let Some(address_book_id) = &contact.address_book_id {
        target.insert(
            "addressBookIds".to_string(),
            json!({ address_book_id.0.clone(): true }),
        );
    }
    if let Some(display_name) = &contact.display_name {
        target.insert("name".to_string(), Value::Object(name_object(display_name)));
    }
    if !contact.emails.is_empty() {
        target.insert(
            "emails".to_string(),
            Value::Object(email_object(&contact.emails)),
        );
    }
    if !contact.phones.is_empty() {
        target.insert(
            "phones".to_string(),
            Value::Object(phone_object(&contact.phones)),
        );
    }
    if !contact.organizations.is_empty() {
        target.insert(
            "organizations".to_string(),
            Value::Object(organization_object(&contact.organizations)),
        );
        target.insert(
            "titles".to_string(),
            Value::Object(titles_object(&contact.organizations)),
        );
    }
    if !contact.addresses.is_empty() {
        target.insert(
            "addresses".to_string(),
            Value::Object(address_object(&contact.addresses)),
        );
    }
    if let Some(notes) = &contact.notes {
        target.insert("notes".to_string(), Value::Object(notes_object(notes)));
    }
    if let Some(url) = &contact.photo_url {
        let mut media = Map::new();
        media.insert("photo".to_string(), photo_media_object(url));
        target.insert("media".to_string(), Value::Object(media));
    }
}

/// JSContact (RFC 9610) media resource for a photo. `kind` is the resource
/// role (`photo`), not a URI marker; the read path filters on it, so a wrong
/// value reads back as no photo.
fn photo_media_object(url: &str) -> Value {
    json!({"@type": "Media", "kind": "photo", "uri": url})
}

fn name_object(display_name: &str) -> Map<String, Value> {
    let mut name = Map::new();
    name.insert("@type".to_string(), json!("Name"));
    name.insert("full".to_string(), Value::String(display_name.to_string()));
    name
}

fn email_object(emails: &[ContactEmail]) -> Map<String, Value> {
    emails
        .iter()
        .enumerate()
        .map(|(idx, email)| {
            (
                format!("e{idx}"),
                json!({
                    "@type": "EmailAddress",
                    "address": email.value,
                    "contexts": context_object(email.kind.as_deref()),
                    "pref": email.is_primary.then_some(1),
                }),
            )
        })
        .collect()
}

fn phone_object(phones: &[ContactPhone]) -> Map<String, Value> {
    phones
        .iter()
        .enumerate()
        .map(|(idx, phone)| {
            let (contexts, features) = phone_kind_fields(phone.kind.as_deref());
            (
                format!("p{idx}"),
                json!({
                    "@type": "Phone",
                    "number": phone.value,
                    "contexts": contexts,
                    "features": features,
                    "pref": phone.is_primary.then_some(1),
                }),
            )
        })
        .collect()
}

fn organization_object(orgs: &[ContactOrganization]) -> Map<String, Value> {
    orgs.iter()
        .enumerate()
        .map(|(idx, org)| {
            (
                format!("o{idx}"),
                json!({
                    "@type": "Organization",
                    "name": org.name,
                }),
            )
        })
        .collect()
}

fn titles_object(orgs: &[ContactOrganization]) -> Map<String, Value> {
    orgs.iter()
        .enumerate()
        .filter_map(|(idx, org)| {
            org.title.as_ref().map(|title| {
                (
                    format!("t{idx}"),
                    json!({
                        "@type": "Title",
                        "name": title,
                        "kind": "title",
                        "organizationId": format!("o{idx}"),
                    }),
                )
            })
        })
        .collect()
}

fn address_object(addresses: &[ContactAddress]) -> Map<String, Value> {
    addresses
        .iter()
        .enumerate()
        .map(|(idx, address)| {
            let mut components = Vec::new();
            components.extend(
                address.street.iter().map(
                    |line| json!({ "@type": "AddressComponent", "kind": "name", "value": line }),
                ),
            );
            for (kind, value) in [
                ("locality", address.locality.as_ref()),
                ("region", address.region.as_ref()),
                ("postcode", address.postal_code.as_ref()),
                ("country", address.country.as_ref()),
            ] {
                if let Some(value) = value {
                    components.push(json!({
                        "@type": "AddressComponent",
                        "kind": kind,
                        "value": value,
                    }));
                }
            }
            (
                format!("a{idx}"),
                json!({
                    "@type": "Address",
                    "contexts": context_object(address.kind.as_deref()),
                    "pref": address.is_primary.then_some(1),
                    "full": address.formatted.clone(),
                    "components": components,
                    "isOrdered": false,
                }),
            )
        })
        .collect()
}

fn notes_object(notes: &str) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert("n0".to_string(), json!({"@type": "Note", "note": notes}));
    out
}

fn context_object(kind: Option<&str>) -> Value {
    match kind {
        Some(kind) => json!({ kind: true }),
        None => Value::Null,
    }
}

fn phone_kind_fields(kind: Option<&str>) -> (Value, Value) {
    match kind {
        Some("home" | "work") => (context_object(kind), Value::Null),
        Some(kind) => (Value::Null, json!({ kind: true })),
        None => (Value::Null, Value::Null),
    }
}

fn display_name(name: Option<&Map<String, Value>>) -> Option<String> {
    let name = name?;
    name.get("full")
        .and_then(Value::as_str)
        .or_else(|| name.get("given").and_then(Value::as_str))
        .map(ToString::to_string)
}

/// RFC 9553 s1.5.4: `pref` is a RANKING from 1 to 100 in which the LOWER
/// number is the more preferred entry, not a boolean flag whose true value
/// happens to be spelled `1`. Reading `pref == 1` therefore reported no
/// primary at all for a card whose most-preferred address was ranked `10`,
/// and reported several primaries for a card that ranked two entries `1`.
///
/// A value outside 1-100 is not a ranking, so it reads as absent rather
/// than as an extremely strong preference (`pref: 0` would otherwise beat
/// every legal rank).
fn pref_rank(object: &Map<String, Value>) -> Option<i64> {
    object
        .get("pref")
        .and_then(Value::as_i64)
        .filter(|rank| (1..=100).contains(rank))
}

/// The single entry the shared contact model calls primary: the lowest
/// rank present, ties resolved by position, and none at all when no entry
/// carries a rank (an absent `pref` is least preferred, never primary).
fn apply_preferred<T>(
    mut entries: Vec<(Option<i64>, T)>,
    primary: fn(&mut T) -> &mut bool,
) -> Vec<T> {
    let winner = entries
        .iter()
        .enumerate()
        .filter_map(|(index, (rank, _))| rank.map(|rank| (rank, index)))
        .min();
    if let Some((_, index)) = winner {
        *primary(&mut entries[index].1) = true;
    }
    entries.into_iter().map(|(_, entry)| entry).collect()
}

fn emails(values: Option<&Map<String, Value>>) -> Vec<ContactEmail> {
    let entries = values
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| {
            let object = value.as_object()?;
            Some((
                pref_rank(object),
                ContactEmail {
                    value: object
                        .get("address")
                        .or_else(|| object.get("email"))
                        .and_then(Value::as_str)?
                        .to_string(),
                    kind: first_context(object.get("contexts")),
                    is_primary: false,
                },
            ))
        })
        .collect();
    apply_preferred(entries, |email| &mut email.is_primary)
}

fn phones(values: Option<&Map<String, Value>>) -> Vec<ContactPhone> {
    let entries = values
        .into_iter()
        .flat_map(Map::values)
        .filter_map(|value| {
            let object = value.as_object()?;
            Some((
                pref_rank(object),
                ContactPhone {
                    value: object
                        .get("number")
                        .or_else(|| object.get("phone"))
                        .and_then(Value::as_str)?
                        .to_string(),
                    kind: phone_kind(object),
                    is_primary: false,
                },
            ))
        })
        .collect();
    apply_preferred(entries, |phone| &mut phone.is_primary)
}

fn organizations(
    values: Option<&Map<String, Value>>,
    titles: Option<&Map<String, Value>>,
) -> Vec<ContactOrganization> {
    values
        .into_iter()
        .flat_map(Map::iter)
        .filter_map(|(id, value)| {
            let object = value.as_object()?;
            Some(ContactOrganization {
                name: object.get("name").and_then(Value::as_str)?.to_string(),
                title: titles.into_iter().flat_map(Map::values).find_map(|title| {
                    let title = title.as_object()?;
                    (title.get("organizationId").and_then(Value::as_str) == Some(id.as_str()))
                        .then(|| title.get("name").and_then(Value::as_str))
                        .flatten()
                        .map(ToString::to_string)
                }),
            })
        })
        .collect()
}

/// RFC 9553 address-component kinds that make up a street line, in the order
/// the shared `ContactAddress.street` lines preserve.
const STREET_COMPONENTS: &[&str] = &[
    "room",
    "apartment",
    "floor",
    "building",
    "number",
    "name",
    "block",
    "direction",
    "landmark",
    "postOfficeBox",
];

/// RFC 9553 address-component kinds that map onto a dedicated shared scalar.
const SCALAR_COMPONENTS: &[&str] = &["locality", "region", "postcode", "country"];

fn addresses(values: Option<&Map<String, Value>>) -> Result<Vec<ContactAddress>, &'static str> {
    let entries: Vec<(Option<i64>, ContactAddress)> = values
        .into_iter()
        .flat_map(Map::values)
        .filter_map(Value::as_object)
        .map(|object| {
            let components = object
                .get("components")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_object)
                .collect::<Vec<_>>();
            // The accepted set is exactly the projected set. RFC 9553 defines
            // further kinds (`district`, `subdistrict`, `separator`) that the
            // shared `ContactAddress` has no field for and that folding into
            // `street` would misplace, so they are rejected rather than
            // accepted and dropped.
            if components.iter().any(|component| {
                component
                    .get("kind")
                    .and_then(Value::as_str)
                    .is_none_or(|kind| {
                        !STREET_COMPONENTS.contains(&kind) && !SCALAR_COMPONENTS.contains(&kind)
                    })
            }) {
                return Err("JMAP address contains an unsupported address component");
            }
            let component = |kind: &str| {
                components.iter().find_map(|component| {
                    (component.get("kind").and_then(Value::as_str) == Some(kind))
                        .then(|| component.get("value").and_then(Value::as_str))
                        .flatten()
                        .map(ToString::to_string)
                })
            };
            let street = components
                .iter()
                .filter_map(|component| {
                    component
                        .get("kind")
                        .and_then(Value::as_str)
                        .filter(|kind| STREET_COMPONENTS.contains(kind))
                        .and_then(|_| component.get("value").and_then(Value::as_str))
                        .map(ToString::to_string)
                })
                .collect();
            Ok((
                pref_rank(object),
                ContactAddress {
                    kind: first_context(object.get("contexts")),
                    formatted: object
                        .get("full")
                        .and_then(Value::as_str)
                        .map(ToString::to_string),
                    street,
                    locality: component("locality"),
                    region: component("region"),
                    postal_code: component("postcode"),
                    country: component("country"),
                    is_primary: false,
                },
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(apply_preferred(entries, |address| &mut address.is_primary))
}

fn notes(values: Option<&Map<String, Value>>) -> Option<String> {
    values.into_iter().flat_map(Map::values).find_map(|value| {
        value
            .as_object()?
            .get("note")?
            .as_str()
            .map(ToString::to_string)
    })
}

fn photo_url(values: Option<&Map<String, Value>>) -> Option<String> {
    values.into_iter().flat_map(Map::values).find_map(|value| {
        let object = value.as_object()?;
        (object.get("kind").and_then(Value::as_str) == Some("photo")).then_some(())?;
        object.get("uri")?.as_str().map(ToString::to_string)
    })
}

fn first_context(value: Option<&Value>) -> Option<String> {
    value?
        .as_object()?
        .iter()
        .find_map(|(key, value)| value.as_bool().unwrap_or(false).then(|| key.clone()))
}

fn phone_kind(object: &Map<String, Value>) -> Option<String> {
    first_feature(object.get("features")).or_else(|| first_context(object.get("contexts")))
}

fn first_feature(value: Option<&Value>) -> Option<String> {
    value?
        .as_object()?
        .iter()
        .find_map(|(key, value)| value.as_bool().unwrap_or(false).then(|| key.clone()))
}

/// The contact page walk's parameters for the shared anchored, state-pinned
/// page cursor (`page_cursor`): the id marker, the query builder, and the
/// wording of its diagnostics. Behaviour is not a parameter.
struct ContactPages;

impl PageKind for ContactPages {
    type ItemId = ContactCardId;
    type Query = ContactCardQuery;

    const COLLECTION: &'static str = "contact";
    const ITEM: &'static str = "contact";
    const ID_LABEL: &'static str = "card";
    const METHOD: &'static str = "ContactCard/query";
    const REPEAT: &'static str = "listing";

    fn query_from_start(query: ContactCardQuery) -> ContactCardQuery {
        query.position(0)
    }

    fn query_after(query: ContactCardQuery, anchor: &str) -> ContactCardQuery {
        query.anchor(anchor).anchor_offset(1)
    }
}

fn usize_to_u64(value: usize, operation: AccountOperation) -> Result<u64, AccountError> {
    u64::try_from(value).map_err(|error| {
        super::error::unsupported_error(
            operation,
            None,
            format!("JMAP contact total does not fit u64: {error}"),
        )
    })
}

fn require_contacts<T: HttpTransport>(
    contacts: Option<ContactAccount<T>>,
    operation: AccountOperation,
) -> Result<ContactAccount<T>, AccountError> {
    contacts.ok_or_else(|| unsupported(operation, "JMAP contacts capability is unavailable"))
}

fn unsupported(operation: AccountOperation, message: &str) -> AccountError {
    super::error::unsupported_error(operation, None, message)
}

fn to_acct_err(operation: AccountOperation) -> impl Fn(crate::Error) -> AccountError {
    move |err| super::error::into_account_error(err, super::error::JmapErrorContext::new(operation))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card(id: &str) -> JmapContactCard {
        JmapContactCard {
            properties: serde_json::from_value(json!({ "id": id })).expect("object"),
        }
    }

    /// Answers `ContactCard/get` with an empty `list` and an empty
    /// `notFound` - the unanswered-id lane this module documents as a
    /// transient - and records every `ContactCard/set` it is asked to
    /// perform, so a write issued despite the failed read is visible.
    #[derive(Clone)]
    struct UnansweredGetTransport {
        sets: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl crate::core::transport::HttpTransport for UnansweredGetTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            let name = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            let arguments = match name.as_str() {
                "ContactCard/get" => json!({
                    "accountId": "primary",
                    "state": "s1",
                    "list": [],
                    "notFound": []
                }),
                "ContactCard/set" => {
                    self.sets.lock().expect("sets").push(call[1].clone());
                    json!({
                        "accountId": "primary",
                        "oldState": "s1",
                        "newState": "s2",
                        "updated": {"c1": null}
                    })
                }
                other => panic!("unexpected method {other}"),
            };
            let response = json!({
                "sessionState": "session-1",
                "methodResponses": [[name, arguments, call_id]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    /// One scripted `ContactCard/query` answer: the ids it serves, the
    /// position it claims to have served from, the server-side total, and
    /// the `queryState` that result order belongs to.
    #[derive(Clone)]
    struct ScriptedPage {
        ids: Vec<&'static str>,
        position: i32,
        total: usize,
        query_state: &'static str,
    }

    /// A `ContactCard/query` server that answers from a script, one entry per
    /// query, and records every method call it received.
    ///
    /// A script rather than a simulated list because the interesting cases
    /// are not "what would a correct server serve" - they are the answers a
    /// server gives when its result ORDER moved between two pages, which is
    /// precisely the situation no single list can represent.
    #[derive(Clone)]
    struct ScriptedQueryTransport {
        pages: std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<ScriptedPage>>>,
        calls: std::sync::Arc<std::sync::Mutex<Vec<(String, Value)>>>,
    }

    impl ScriptedQueryTransport {
        fn new(pages: Vec<ScriptedPage>) -> Self {
            Self {
                pages: std::sync::Arc::new(std::sync::Mutex::new(pages.into())),
                calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<(String, Value)> {
            self.calls.lock().expect("calls").clone()
        }

        fn queries(&self) -> Vec<Value> {
            self.calls()
                .into_iter()
                .filter(|(name, _)| name == "ContactCard/query")
                .map(|(_, args)| args)
                .collect()
        }

        fn account(&self) -> ContactAccount<Self> {
            let client = crate::client::Client::with_transport(
                self.clone(),
                contacts_session(),
                "https://example.test/.well-known/jmap",
            )
            .expect("client builds");
            ContactAccount::new(client, "primary")
        }
    }

    impl crate::core::transport::HttpTransport for ScriptedQueryTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: Value = serde_json::from_slice(&body).expect("request json");
            let call = request["methodCalls"][0].clone();
            let name = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            self.calls
                .lock()
                .expect("calls")
                .push((name.clone(), call[1].clone()));
            let arguments =
                match name.as_str() {
                    "ContactCard/query" => {
                        let page = self
                            .pages
                            .lock()
                            .expect("pages")
                            .pop_front()
                            .expect("script has a page for this query");
                        json!({
                            "accountId": "primary",
                            "queryState": page.query_state,
                            "canCalculateChanges": false,
                            "position": page.position,
                            "total": page.total,
                            "ids": page.ids
                        })
                    }
                    "ContactCard/get" => {
                        let list: Vec<Value> = call[1]["ids"]
                            .as_array()
                            .expect("ids")
                            .iter()
                            .map(|id| json!({"id": id}))
                            .collect();
                        json!({
                            "accountId": "primary",
                            "state": "s1",
                            "list": list,
                            "notFound": []
                        })
                    }
                    "ContactCard/set" => {
                        // A fixed server state, independent of the production guard wiring.
                        if call[1]
                            .get("ifInState")
                            .is_some_and(|state| state.as_str() != Some("s1"))
                        {
                            return Ok(bytes::Bytes::from(json!({
                            "sessionState": "session-1",
                            "methodResponses": [["error", {"type": "stateMismatch"}, call_id]]
                        }).to_string()));
                        }
                        json!({
                            "accountId": "primary", "oldState": "s1", "newState": "s2",
                            "updated": {"one": null}, "destroyed": ["one"]
                        })
                    }
                    other => panic!("unexpected method {other}"),
                };
            let response = json!({
                "sessionState": "session-1",
                "methodResponses": [[name, arguments, call_id]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    /// Build a page cursor the way a real page would have minted it, so no
    /// test hand-writes the encoding.
    fn page_cursor(anchor: &str, query_state: &str) -> Vec<u8> {
        ContactPages::next_cursor(
            None,
            0,
            &ids(&[anchor]),
            Some(1_000),
            query_state,
            AccountOperation::ContactsList,
        )
        .expect("valid envelope")
        .expect("cursor")
    }

    fn contacts_session() -> crate::core::session::Session {
        serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:contacts": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:contacts": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:contacts": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses")
    }

    /// The scripted server answers `ContactCard/set` for "one" only, so a
    /// write to "two" comes back omitted from every map. That omission is a
    /// partial response, never the terminal `MissingField` a bare
    /// `IdNotFound` classified as: it does not prove the write failed.
    #[tokio::test]
    async fn an_omitted_contact_set_answer_is_a_partial_response() {
        let partial = bifrost_types::AccountErrorKind::Protocol(
            bifrost_types::ProtocolErrorKind::PartialResponse,
        );
        for expected in [
            None,
            Some(bifrost_types::ServerVersion::StateAt("s1".into())),
        ] {
            let transport = ScriptedQueryTransport::new(Vec::new());
            let update = update(
                Some(transport.account()),
                ContactId("two".into()),
                ContactPatch {
                    expected_version: expected.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
            assert_eq!(update.kind(), &partial, "{update:?}");
            let delete = delete_if_version(
                Some(transport.account()),
                ContactId("two".into()),
                expected.clone(),
            )
            .await
            .unwrap_err();
            assert_eq!(delete.kind(), &partial, "{delete:?}");
        }
    }

    #[tokio::test]
    async fn caller_contact_state_is_enforced_without_replacing_it() {
        for state in [Some("old"), Some("s1"), None] {
            let transport = ScriptedQueryTransport::new(Vec::new());
            let expected = state.map(|state| bifrost_types::ServerVersion::StateAt(state.into()));
            let stale = state == Some("old");
            let result = update(
                Some(transport.account()),
                ContactId("one".into()),
                ContactPatch {
                    expected_version: expected.clone(),
                    ..Default::default()
                },
            )
            .await;
            if stale {
                assert_eq!(
                    *result.unwrap_err().kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                );
            } else {
                result.unwrap();
            }
            let result = delete_if_version(
                Some(transport.account()),
                ContactId("one".into()),
                expected.clone(),
            )
            .await;
            if stale {
                assert_eq!(
                    *result.unwrap_err().kind(),
                    bifrost_types::AccountErrorKind::ConcurrencyConflict
                );
            } else {
                result.unwrap();
            }

            let calls = transport.calls();
            for (_, args) in calls
                .iter()
                .filter(|(method, _)| method == "ContactCard/set")
            {
                assert_eq!(args.get("ifInState").and_then(Value::as_str), state);
            }
            assert_eq!(
                calls
                    .iter()
                    .filter(|(method, _)| method == "ContactCard/set")
                    .count(),
                2
            );
        }
    }

    #[tokio::test]
    async fn wrong_contact_version_kind_is_refused_before_io() {
        let transport = ScriptedQueryTransport::new(Vec::new());
        for expected_version in [
            bifrost_types::ServerVersion::ETag("s1".into()),
            bifrost_types::ServerVersion::StateAt(String::new()),
        ] {
            let error = update(
                Some(transport.account()),
                ContactId("one".into()),
                ContactPatch {
                    expected_version: Some(expected_version),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
            assert_eq!(
                *error.kind(),
                bifrost_types::AccountErrorKind::Unsupported(AccountOperation::ContactUpdate)
            );
        }
        assert!(transport.calls().is_empty());
    }

    /// The happy path, and the only shape a continuation is allowed to
    /// succeed on: the result set did NOT move, so both pages report the
    /// same `queryState`. The walk sees every card exactly once, and it got
    /// there by anchoring on page one's last id rather than by offsetting.
    ///
    /// The earlier version of this fixture deleted a card between the pages
    /// while continuing to answer `q1`. That models a server claiming its
    /// ordered result list did not change while it did - a contradiction, not
    /// a scenario worth pinning.
    #[tokio::test]
    async fn an_unmoved_result_set_pages_through_every_card_once() {
        let transport = ScriptedQueryTransport::new(vec![
            ScriptedPage {
                ids: vec!["c0", "c1"],
                position: 0,
                total: 4,
                query_state: "q1",
            },
            ScriptedPage {
                ids: vec!["c2", "c3"],
                position: 2,
                total: 4,
                query_state: "q1",
            },
        ]);

        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = list(Some(transport.account()), None, cursor.take())
                .await
                .expect("page");
            seen.extend(page.items.iter().map(|card| card.id.0.clone()));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(seen, vec!["c0", "c1", "c2", "c3"], "walk saw {seen:?}");

        let queries = transport.queries();
        assert_eq!(queries.len(), 2, "expected two pages");
        assert_eq!(queries[0].get("position"), Some(&json!(0)));
        assert_eq!(queries[1].get("anchor"), Some(&json!("c1")));
        assert_eq!(queries[1].get("anchorOffset"), Some(&json!(1)));
        assert_eq!(queries[1].get("position"), None);
    }

    /// The defect the state pin exists for. Page one serves `[c0, c1]`; `c2`
    /// is then edited so the server's order becomes `[c2, c0, c1, c3]` and
    /// the state moves to `q2`. Resuming after `c1` legitimately returns
    /// `[c3]` - the anchor is exactly where the server says it is - and `c2`
    /// is gone from the walk forever.
    ///
    /// The anchor cannot see this: nothing about `c1`'s position reveals that
    /// something crossed it. Only the moved `queryState` does, so the page is
    /// refused as a `ConcurrencyConflict` instead of silently dropping `c2`.
    #[tokio::test]
    async fn an_item_reordered_across_the_anchor_refuses_the_continuation() {
        let transport = ScriptedQueryTransport::new(vec![
            ScriptedPage {
                ids: vec!["c0", "c1"],
                position: 0,
                total: 4,
                query_state: "q1",
            },
            ScriptedPage {
                ids: vec!["c3"],
                position: 3,
                total: 4,
                query_state: "q2",
            },
        ]);

        let first = list(Some(transport.account()), None, None)
            .await
            .expect("first page");
        let cursor = first.next_cursor.expect("a second page is promised");

        let error = list(Some(transport.account()), None, Some(cursor))
            .await
            .expect_err("a moved result set must not page on");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
    }

    /// The check runs BEFORE anything is exposed. An implementation that
    /// compared the state after hydrating would pass the test above and still
    /// hand the caller rows out of a list it had already decided was the
    /// wrong one, so the bite here is the absence of the `ContactCard/get`:
    /// the refused page hydrates nothing and, being an `Err`, carries neither
    /// items nor a successor cursor.
    #[tokio::test]
    async fn a_moved_result_set_is_refused_before_the_page_is_hydrated() {
        let transport = ScriptedQueryTransport::new(vec![ScriptedPage {
            ids: vec!["c7", "c8"],
            position: 2,
            total: 9,
            query_state: "q2",
        }]);

        let error = list(
            Some(transport.account()),
            None,
            Some(page_cursor("c1", "q1")),
        )
        .await
        .expect_err("a moved result set must not page on");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );

        let methods: Vec<String> = transport
            .calls()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(
            methods,
            vec!["ContactCard/query".to_string()],
            "the refused page must not hydrate: {methods:?}"
        );
    }

    /// A page that looks final - no ids at all, and a total the walk has
    /// already reached - is still refused when the state moved. Otherwise a
    /// walk whose tail was reordered away reports clean completion, which is
    /// the silent version of the same loss.
    #[tokio::test]
    async fn an_empty_final_page_under_a_moved_state_is_still_refused() {
        let transport = ScriptedQueryTransport::new(vec![ScriptedPage {
            ids: vec![],
            position: 2,
            total: 2,
            query_state: "q2",
        }]);

        let error = list(
            Some(transport.account()),
            None,
            Some(page_cursor("c1", "q1")),
        )
        .await
        .expect_err("an empty page under a moved state must not read as done");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
    }

    /// The successor is minted from the QUERY result, not from what
    /// hydration or a local filter left behind. Here the page's last id
    /// hydrates fine, so the distinction is pinned directly against
    /// `next_cursor` in the unit tests below; what this pins is that a first
    /// page captures the response's own `queryState` into the cursor it
    /// mints, so the continuation has something to compare against at all.
    #[tokio::test]
    async fn a_first_page_captures_the_response_query_state() {
        let transport = ScriptedQueryTransport::new(vec![ScriptedPage {
            ids: vec!["c0", "c1"],
            position: 0,
            total: 4,
            query_state: "q-first",
        }]);

        let page = list(Some(transport.account()), None, None)
            .await
            .expect("first page");
        let minted = page.next_cursor.expect("a second page is promised");
        assert_eq!(
            ContactPages::decode_page_cursor(Some(minted), AccountOperation::ContactsList)
                .expect("decodes"),
            Some(cursor("c1", "q-first"))
        );
    }

    /// Answers every `ContactCard/query` with `anchorNotFound`: the card the
    /// cursor anchored on was destroyed between pages.
    #[derive(Clone)]
    struct AnchorLostTransport;

    impl crate::core::transport::HttpTransport for AnchorLostTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: Value = serde_json::from_slice(&body).expect("request json");
            let call_id = request["methodCalls"][0][2]
                .as_str()
                .expect("call id")
                .to_string();
            let response = json!({
                "sessionState": "session-1",
                "methodResponses": [["error", {"type": "anchorNotFound"}, call_id]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    fn anchor_lost_account() -> ContactAccount<AnchorLostTransport> {
        let client = crate::client::Client::with_transport(
            AnchorLostTransport,
            contacts_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        ContactAccount::new(client, "primary")
    }

    /// A contact deleted while a consumer pages its address book is ordinary
    /// concurrent activity, so losing the anchor is a `ConcurrencyConflict`
    /// (retry after refreshing: list again from page one), NOT the
    /// `Protocol(ContractViolation)` the central mapping gives an
    /// unanchored caller.
    #[tokio::test]
    async fn a_lost_anchor_on_an_anchored_page_is_a_concurrency_conflict() {
        let anchored = list(
            Some(anchor_lost_account()),
            None,
            Some(page_cursor("c1", "q1")),
        )
        .await
        .expect_err("anchorNotFound fails the page");
        assert_eq!(
            anchored.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
    }

    /// The other half of the same rule: the reclassification is SCOPED to
    /// anchored calls, so `page_call_err` with `anchored: false` must leave
    /// the central `Protocol(ContractViolation)` reading alone.
    ///
    /// Driven through the classifier directly rather than through a transport.
    /// A first page sends no anchor, so no conforming server can answer it
    /// `anchorNotFound`; a fixture that made one do so would be modelling an
    /// impossible response, and would pin the mapping against a wire shape
    /// that cannot occur instead of against the branch that reads it.
    #[test]
    fn a_lost_anchor_off_an_anchored_page_keeps_the_contract_violation() {
        let wire: crate::core::error::MethodError =
            serde_json::from_value(json!({"type": "anchorNotFound"})).expect("method error parses");
        let mapped = ContactPages::page_call_err(AccountOperation::ContactsList, false)(
            crate::Error::Method(wire),
        );
        assert_eq!(
            mapped.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
    }

    /// An address-book move needs the current book to clear it. When the
    /// read does not materialize the card, writing anyway performs an ADD
    /// and leaves the contact in both books, silently. The update must
    /// fail - and retryably, since the unanswered-id lane says nothing
    /// about the card.
    #[tokio::test]
    async fn a_book_move_fails_when_the_read_did_not_materialize_the_card() {
        let sets = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let session: crate::core::session::Session = serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:contacts": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:contacts": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:contacts": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses");
        let client = crate::client::Client::with_transport(
            UnansweredGetTransport {
                sets: std::sync::Arc::clone(&sets),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let account = ContactAccount::new(client, "primary");

        let error = update(
            Some(account),
            ContactId("c1".to_string()),
            ContactPatch {
                address_book_id: Some(SharedAddressBookId("book-new".to_string())),
                ..ContactPatch::default()
            },
        )
        .await
        .expect_err("a book move without the current book must fail");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::PartialResponse
            )
        );
        assert!(
            sets.lock().expect("sets").is_empty(),
            "no half-move may reach the wire: {:?}",
            sets.lock().expect("sets")
        );
    }

    /// `Page::failed_ids` is what stops a consumer reading "absent from
    /// `items`" as "deleted, destroy the row". A `ContactCard/get` that
    /// answers an id in neither `list` nor `notFound` therefore has to
    /// land there too - and it cannot be detected from `notFound` alone,
    /// which decodes as empty whenever the server omits it.
    #[test]
    fn an_id_answered_in_neither_list_nor_not_found_rides_failed_ids() {
        let requested = vec!["c0".to_string(), "c1".to_string(), "c2".to_string()];
        let (cards, failed_ids) = reconcile_cards(
            requested,
            // Empty is exactly what an omitted `notFound` decodes to.
            &[],
            vec![card("c1")],
            AccountOperation::ContactsList,
        )
        .expect("supported cards");

        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].id.0, "c1");
        let mut failed_ids = failed_ids;
        failed_ids.sort();
        assert_eq!(failed_ids, vec!["c0".to_string(), "c2".to_string()]);
    }

    /// A declared `notFound` and an unanswered id both mean "no card for
    /// this row", and neither may be double-counted when the two overlap.
    #[test]
    fn declared_not_found_and_unanswered_ids_are_each_reported_once() {
        let requested = vec!["c0".to_string(), "c1".to_string(), "c2".to_string()];
        let (cards, failed_ids) = reconcile_cards(
            requested,
            &[ContactCardId::new("c0"), ContactCardId::new("c0")],
            vec![card("c1")],
            AccountOperation::ContactsList,
        )
        .expect("supported cards");

        assert_eq!(cards.len(), 1);
        assert_eq!(failed_ids, vec!["c0".to_string(), "c2".to_string()]);
    }

    /// A card the server returned without a usable id is not an item: it
    /// must not surface as `ContactId("")`, and the requested id it fails
    /// to answer lands on `failed_ids` rather than vanishing.
    #[test]
    fn an_id_less_card_is_dropped_and_its_request_rides_failed_ids() {
        let id_less = JmapContactCard {
            properties: serde_json::from_value(json!({ "name": {"full": "Nobody"} }))
                .expect("object"),
        };
        let (cards, failed_ids) = reconcile_cards(
            vec!["c0".to_string()],
            &[],
            vec![id_less],
            AccountOperation::ContactsList,
        )
        .expect("supported cards");

        assert!(cards.is_empty(), "no ContactId(\"\") row may be minted");
        assert_eq!(failed_ids, vec!["c0".to_string()]);
    }

    #[test]
    fn jscontact_maps_to_shared_contact() {
        let card = JmapContactCard {
            properties: serde_json::from_value(json!({
                "id": "c1",
                "addressBookIds": {"ab1": true},
                "name": {"full": "Ada Lovelace"},
                "emails": {"e1": {"address": "ada@example.test", "contexts": {"work": true}, "pref": 1}},
                "phones": {"p1": {"number": "+123", "features": {"mobile": true}}},
                "addresses": {"a1": {"components": [
                    {"kind": "name", "value": "1 Example St"},
                    {"kind": "apartment", "value": "Unit 2"},
                    {"kind": "locality", "value": "London"},
                    {"kind": "region", "value": "England"},
                    {"kind": "postcode", "value": "N1"},
                    {"kind": "country", "value": "UK"}
                ], "contexts": {"home": true}, "pref": 1}},
                "organizations": {"o1": {"name": "Analytical Engines"}},
                "titles": {"t1": {"name": "Programmer", "kind": "title", "organizationId": "o1"}},
                "notes": {"n1": {"note": "notes"}},
                "media": {
                    "m1": {"kind": "logo", "uri": "https://example.test/logo.jpg"},
                    "m2": {"kind": "photo", "uri": "https://example.test/a.jpg"}
                }
            }))
            .expect("object"),
        };

        let contact =
            contact_from_jmap(card, AccountOperation::ContactGet).expect("supported card");
        assert_eq!(contact.id.0, "c1");
        // JMAP has no auto-collected corpus; every card routes to Main.
        assert_eq!(contact.corpus, ContactCorpus::Main);
        assert_eq!(contact.address_book_id.unwrap().0, "ab1");
        assert_eq!(contact.display_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(contact.emails[0].kind.as_deref(), Some("work"));
        assert!(contact.emails[0].is_primary);
        assert_eq!(contact.phones[0].kind.as_deref(), Some("mobile"));
        assert_eq!(contact.addresses[0].kind.as_deref(), Some("home"));
        assert_eq!(contact.addresses[0].street, vec!["1 Example St", "Unit 2"]);
        assert_eq!(contact.addresses[0].postal_code.as_deref(), Some("N1"));
        assert!(contact.addresses[0].is_primary);
        assert_eq!(
            contact.organizations[0].title.as_deref(),
            Some("Programmer")
        );
        assert_eq!(contact.notes.as_deref(), Some("notes"));
        assert_eq!(
            contact.photo_url.as_deref(),
            Some("https://example.test/a.jpg")
        );
    }

    /// RFC 9553 `pref` ranks 1-100 with lower more preferred. A card whose
    /// best email is ranked `10` has a primary email; a card ranking two
    /// entries equally has exactly one, the first; an unranked entry never
    /// wins; and an out-of-range value is not a rank at all.
    #[test]
    fn the_lowest_pref_rank_is_the_primary_entry() {
        let card = JmapContactCard {
            properties: serde_json::from_value(json!({
                "id": "c1",
                "emails": {
                    "e1": {"address": "low@example.test", "pref": 40},
                    "e2": {"address": "best@example.test", "pref": 10},
                    "e3": {"address": "none@example.test"}
                },
                "phones": {
                    "p1": {"number": "+1", "pref": 5},
                    "p2": {"number": "+2", "pref": 5}
                },
                "addresses": {
                    "a1": {"components": [{"kind": "locality", "value": "Oslo"}], "pref": 0},
                    "a2": {"components": [{"kind": "locality", "value": "Bergen"}], "pref": 100}
                }
            }))
            .expect("object"),
        };

        let contact =
            contact_from_jmap(card, AccountOperation::ContactGet).expect("supported card");

        let primary_emails: Vec<&str> = contact
            .emails
            .iter()
            .filter(|email| email.is_primary)
            .map(|email| email.value.as_str())
            .collect();
        assert_eq!(
            primary_emails,
            vec!["best@example.test"],
            "the lowest rank present wins, even when nothing is ranked 1"
        );

        let primary_phones: Vec<&str> = contact
            .phones
            .iter()
            .filter(|phone| phone.is_primary)
            .map(|phone| phone.value.as_str())
            .collect();
        assert_eq!(
            primary_phones,
            vec!["+1"],
            "a tie resolves to the first entry, and only one entry is primary"
        );

        let primary_localities: Vec<&str> = contact
            .addresses
            .iter()
            .filter(|address| address.is_primary)
            .filter_map(|address| address.locality.as_deref())
            .collect();
        assert_eq!(
            primary_localities,
            vec!["Bergen"],
            "pref 0 is outside the 1-100 ranking and reads as unranked"
        );
    }

    #[test]
    fn an_unranked_contact_has_no_primary_entry() {
        let card = JmapContactCard {
            properties: serde_json::from_value(json!({
                "id": "c1",
                "emails": {"e1": {"address": "a@example.test"}}
            }))
            .expect("object"),
        };
        let contact =
            contact_from_jmap(card, AccountOperation::ContactGet).expect("supported card");
        assert!(
            !contact.emails[0].is_primary,
            "an absent pref is least preferred, never promoted to primary"
        );
    }

    #[test]
    fn contact_create_writes_card_type_and_photo_kind() {
        let create = jmap_create_from_contact(&ContactCreate {
            display_name: Some("Ada".to_string()),
            photo_url: Some("https://example.test/a.jpg".to_string()),
            ..ContactCreate::default()
        });

        assert_eq!(create.properties.get("@type"), Some(&json!("Card")));
        assert_eq!(create.properties["name"]["@type"], json!("Name"));
        let media = create.properties["media"]["photo"]
            .as_object()
            .expect("photo media object");
        assert_eq!(media.get("kind"), Some(&json!("photo")));
        assert_eq!(media.get("uri"), Some(&json!("https://example.test/a.jpg")));
    }

    #[test]
    fn contact_photo_round_trips_through_write_path() {
        // Exercise the write output through the read path rather than
        // hand-crafted JSON, so a wrong `kind` would surface as None.
        let create = jmap_create_from_contact(&ContactCreate {
            photo_url: Some("https://example.test/a.jpg".to_string()),
            ..ContactCreate::default()
        });
        let media = create
            .properties
            .get("media")
            .and_then(Value::as_object)
            .expect("media object");

        assert_eq!(
            photo_url(Some(media)).as_deref(),
            Some("https://example.test/a.jpg")
        );

        let patch = jmap_patch_from_contact_patch(
            &ContactPatch {
                photo_url: Some(Some("https://example.test/b.jpg".to_string())),
                ..ContactPatch::default()
            },
            None,
        );
        let media = patch.properties["media"]
            .as_object()
            .expect("patch media object");
        assert_eq!(
            photo_url(Some(media)).as_deref(),
            Some("https://example.test/b.jpg")
        );
    }

    fn ids(values: &[&str]) -> Vec<ContactCardId> {
        values.iter().map(|id| ContactCardId::new(*id)).collect()
    }

    fn cursor(anchor: &str, query_state: &str) -> crate::sync::page_cursor::PageCursor {
        crate::sync::page_cursor::PageCursor {
            anchor: anchor.to_string(),
            query_state: query_state.to_string(),
        }
    }

    /// The first page positions at zero; a continuation carries the anchor
    /// and NO position, so a card destroyed behind the cursor cannot shift
    /// the window.
    #[test]
    fn a_continuation_page_queries_by_anchor_not_position() {
        let first = serde_json::to_value(ContactPages::anchor_query(
            ContactCardQuery::new().limit(2),
            None,
        ))
        .expect("query serializes");
        assert_eq!(first.get("position"), Some(&json!(0)));
        assert_eq!(first.get("anchor"), None);

        let next = serde_json::to_value(ContactPages::anchor_query(
            ContactCardQuery::new().limit(2),
            Some(&cursor("c2", "q1")),
        ))
        .expect("query serializes");
        assert_eq!(next.get("anchor"), Some(&json!("c2")));
        assert_eq!(next.get("anchorOffset"), Some(&json!(1)));
        assert_eq!(next.get("position"), None);
    }

    #[test]
    fn contact_create_maps_phone_type_to_features() {
        let create = jmap_create_from_contact(&ContactCreate {
            phones: vec![
                ContactPhone {
                    value: "+123".to_string(),
                    kind: Some("mobile".to_string()),
                    is_primary: true,
                },
                ContactPhone {
                    value: "+456".to_string(),
                    kind: Some("work".to_string()),
                    is_primary: false,
                },
            ],
            ..ContactCreate::default()
        });
        let phones = create
            .properties
            .get("phones")
            .and_then(Value::as_object)
            .expect("phones object");

        assert_eq!(phones["p0"]["features"]["mobile"].as_bool(), Some(true));
        assert!(phones["p0"]["contexts"].is_null());
        assert_eq!(phones["p1"]["contexts"]["work"].as_bool(), Some(true));
        assert!(phones["p1"]["features"].is_null());
    }

    #[test]
    fn contact_create_writes_addresses() {
        let create = jmap_create_from_contact(&ContactCreate {
            addresses: vec![ContactAddress {
                kind: Some("work".to_string()),
                formatted: None,
                street: vec!["1 Analytical Way".to_string()],
                locality: Some("London".to_string()),
                region: None,
                postal_code: Some("N1".to_string()),
                country: Some("UK".to_string()),
                is_primary: true,
            }],
            ..ContactCreate::default()
        });
        let addresses = create
            .properties
            .get("addresses")
            .and_then(Value::as_object)
            .expect("addresses object");

        assert_eq!(addresses["a0"]["contexts"]["work"].as_bool(), Some(true));
        assert_eq!(addresses["a0"]["components"][0]["kind"], "name");
        assert_eq!(
            addresses["a0"]["components"][0]["value"],
            "1 Analytical Way"
        );
        assert_eq!(addresses["a0"]["components"][2]["kind"], "postcode");
        assert_eq!(addresses["a0"]["components"][2]["value"], "N1");
        assert!(addresses["a0"].get("street").is_none());
        assert_eq!(addresses["a0"]["pref"].as_i64(), Some(1));
    }

    #[test]
    fn contact_create_writes_titles_separately_from_organizations() {
        let create = jmap_create_from_contact(&ContactCreate {
            organizations: vec![ContactOrganization {
                name: "Analytical Engines".to_string(),
                title: Some("Programmer".to_string()),
            }],
            ..ContactCreate::default()
        });

        assert_eq!(
            create.properties["organizations"]["o0"],
            json!({"@type": "Organization", "name": "Analytical Engines"})
        );
        assert!(
            create.properties["organizations"]["o0"]
                .get("title")
                .is_none()
        );
        assert_eq!(
            create.properties["titles"]["t0"],
            json!({
                "@type": "Title",
                "name": "Programmer",
                "kind": "title",
                "organizationId": "o0"
            })
        );
    }

    #[test]
    fn address_components_without_a_shared_field_reject_rather_than_vanish() {
        // These are valid RFC 9553 kinds, so a "did we recognize it" check
        // passes them; the shared ContactAddress still has nowhere to put
        // them, and accepting them silently would hand back a lossy address.
        for kind in ["district", "subdistrict", "separator"] {
            let addresses: Map<String, Value> = serde_json::from_value(json!({
                "a1": {"components": [
                    {"kind": "name", "value": "1 Example St"},
                    {"kind": kind, "value": "opaque"}
                ]}
            }))
            .expect("addresses");
            assert_eq!(
                super::addresses(Some(&addresses)),
                Err("JMAP address contains an unsupported address component"),
                "{kind} must not be accepted and dropped"
            );
        }
    }

    #[test]
    fn unknown_address_component_rejects_contact_hydration() {
        let card = JmapContactCard {
            properties: serde_json::from_value(json!({
                "id": "c1",
                "addresses": {"a1": {"components": [
                    {"kind": "future-component", "value": "opaque"}
                ]}}
            }))
            .expect("card"),
        };
        let error = contact_from_jmap(card, AccountOperation::ContactGet)
            .expect_err("unknown component must reject");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::ContactGet)
        ));
    }

    #[test]
    fn contact_patch_clears_nullable_scalars_with_null() {
        let patch = jmap_patch_from_contact_patch(
            &ContactPatch {
                display_name: Some(None),
                notes: Some(None),
                photo_url: Some(None),
                ..ContactPatch::default()
            },
            None,
        );

        assert_eq!(patch.properties.get("name"), Some(&Value::Null));
        assert_eq!(patch.properties.get("notes"), Some(&Value::Null));
        assert_eq!(patch.properties.get("media"), Some(&Value::Null));
    }

    #[test]
    fn contact_patch_moves_address_book_by_nulling_old_book() {
        let patch = jmap_patch_from_contact_patch(
            &ContactPatch {
                address_book_id: Some(SharedAddressBookId("new".to_string())),
                ..ContactPatch::default()
            },
            Some(&SharedAddressBookId("old".to_string())),
        );
        assert_eq!(
            patch.properties.get("addressBookIds/old"),
            Some(&Value::Null)
        );
        assert_eq!(
            patch
                .properties
                .get("addressBookIds/new")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(patch.properties.get("addressBookIds").is_none());
    }

    #[test]
    fn contact_patch_keeps_same_address_book_membership() {
        let patch = jmap_patch_from_contact_patch(
            &ContactPatch {
                address_book_id: Some(SharedAddressBookId("book".to_string())),
                ..ContactPatch::default()
            },
            Some(&SharedAddressBookId("book".to_string())),
        );
        assert_eq!(
            patch
                .properties
                .get("addressBookIds/book")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(patch.properties.get("addressBookIds").is_none());
    }

    #[test]
    fn absent_address_book_rights_are_not_writable() {
        assert!(!address_book_can_write(None));
        assert!(!address_book_can_delete(None));

        let rights = crate::address_book::AddressBookRights {
            may_read: Some(true),
            may_write: Some(true),
            may_share: None,
            may_delete: Some(false),
        };
        assert!(address_book_can_write(Some(&rights)));
        assert!(!address_book_can_delete(Some(&rights)));
    }
}
