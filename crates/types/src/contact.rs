//! Address book and contact-card primitives for the unified PIM surface.
//!
//! Providers expose different native shapes: JMAP has AddressBook and
//! JSContact ContactCard objects, Google has People API resources, Graph
//! has contact folders and contacts, and CardDAV has address books and
//! vCard resources. These types keep the shared surface small while
//! preserving native ids, etags, and protocol provenance for round trips.

use crate::cursor::ProtocolKind;

/// Engine-facing address book identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AddressBookId(pub String);

/// Engine-facing contact identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContactId(pub String);

impl From<String> for ContactId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for ContactId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Which corpus an address book - and every contact under it - belongs
/// to.
///
/// Distinguishes a provider's primary personal contacts from
/// auto-collected "other" contacts. Google People exposes the latter as
/// the `otherContacts` corpus: addresses harvested from mail traffic
/// that the user never explicitly saved. A consumer routes each corpus
/// to its own local store by reading this flag - never by matching on
/// the provider. Every provider without a distinct auto-collected corpus
/// reports `Main`; Google reports `Main` for its `connections` and group
/// contacts and `OtherAutoCollected` for `otherContacts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ContactCorpus {
    /// The provider's primary personal contacts (the default for every
    /// provider without an auto-collected corpus).
    #[default]
    Main,
    /// Auto-collected addresses harvested from mail traffic (Google
    /// People `otherContacts`), never explicitly saved by the user.
    OtherAutoCollected,
}

/// Wire-level provenance for an address book or contact id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContactProvenance {
    /// Which protocol family minted the native id.
    pub provider: ProtocolKind,
    /// Native id string used by the provider on the wire.
    pub native: String,
    /// Native address-book or folder id, when the provider scopes
    /// contacts under a collection.
    pub address_book_native: Option<String>,
}

/// Address book or contact folder exposed by a provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressBook {
    /// Engine-facing identifier.
    pub id: AddressBookId,
    /// Native id string, duplicated from `provenance.native` for
    /// ergonomic access.
    pub native_id: String,
    /// Display name surfaced by the provider.
    pub name: String,
    /// Wire-level metadata for round trips.
    pub provenance: ContactProvenance,
    /// Corpus this address book belongs to. Lets a consumer route the
    /// book (and the contacts it holds) to a distinct local store
    /// without matching on the provider - a synthetic auto-collected
    /// book carries `OtherAutoCollected`, every real personal book
    /// carries `Main`.
    pub corpus: ContactCorpus,
    /// True when this is the provider's default personal address book.
    pub is_default: bool,
    /// True when contacts can be created in this address book.
    pub can_create_contacts: bool,
    /// True when contacts in this address book can be updated.
    pub can_update_contacts: bool,
    /// True when contacts in this address book can be deleted.
    pub can_delete_contacts: bool,
}

/// A typed email address on a contact card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactEmail {
    pub value: String,
    /// Provider-native label such as `home`, `work`, or `other`.
    pub kind: Option<String>,
    pub is_primary: bool,
}

/// A typed phone number on a contact card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactPhone {
    pub value: String,
    /// Provider-native label such as `mobile`, `home`, or `work`.
    pub kind: Option<String>,
    pub is_primary: bool,
}

/// Organization data on a contact card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactOrganization {
    pub name: String,
    pub title: Option<String>,
}

/// Postal address on a contact card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactAddress {
    /// Provider-native label such as `home`, `work`, or `other`.
    pub kind: Option<String>,
    pub formatted: Option<String>,
    pub street: Vec<String>,
    pub locality: Option<String>,
    pub region: Option<String>,
    pub postal_code: Option<String>,
    pub country: Option<String>,
    pub is_primary: bool,
}

/// Inline binary photo data on a contact card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactPhoto {
    pub data: Vec<u8>,
    /// Provider-native media type or image type hint, when present.
    pub media_type: Option<String>,
}

/// Unified contact card returned by contact primitives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactCard {
    /// Engine-facing identifier.
    pub id: ContactId,
    /// Address book containing this contact, when known.
    pub address_book_id: Option<AddressBookId>,
    /// Native contact id string, duplicated from `provenance.native`.
    pub native_id: String,
    /// Provider etag, change key, or similar optimistic-concurrency
    /// token when available.
    pub etag: Option<String>,
    /// Wire-level metadata for round trips.
    pub provenance: ContactProvenance,
    /// Corpus this contact belongs to. Mirrors the `corpus` of its
    /// address book so a consumer can route a streamed page of cards to
    /// the right local store without a side lookup - `OtherAutoCollected`
    /// for Google `otherContacts`, `Main` everywhere else.
    pub corpus: ContactCorpus,
    pub display_name: Option<String>,
    pub emails: Vec<ContactEmail>,
    pub phones: Vec<ContactPhone>,
    pub organizations: Vec<ContactOrganization>,
    pub addresses: Vec<ContactAddress>,
    pub notes: Option<String>,
    pub photo_url: Option<String>,
    pub photo: Option<ContactPhoto>,
}

/// Contact creation payload.
///
/// Deliberately carries no inline `photo` (unlike `ContactCard` and
/// `ContactPatch`): Google People and Microsoft Graph both model photo
/// upload as a separate call against an EXISTING contact, so an inline
/// photo at create time would force those providers into a hidden
/// create-then-update whose second half can fail after the contact
/// exists. A consumer that wants an inline photo does the two-step
/// explicitly - `contact_create`, then `contact_update` with
/// `ContactPatch::photo` - and owns the partial-failure handling.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContactCreate {
    pub address_book_id: Option<AddressBookId>,
    pub display_name: Option<String>,
    pub emails: Vec<ContactEmail>,
    pub phones: Vec<ContactPhone>,
    pub organizations: Vec<ContactOrganization>,
    pub addresses: Vec<ContactAddress>,
    pub notes: Option<String>,
    pub photo_url: Option<String>,
}

/// Partial contact update payload.
///
/// `None` means leave the field untouched. `Some(None)` clears a
/// nullable scalar. Repeated fields replace the full provider-side
/// collection when present.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContactPatch {
    /// Caller-observed version. Providers must enforce this atomically or
    /// refuse with `Unsupported`; they must never silently discard it.
    /// `None` retains the provider's ordinary update behavior.
    pub expected_version: Option<crate::mutation::ServerVersion>,
    pub address_book_id: Option<AddressBookId>,
    pub display_name: Option<Option<String>>,
    pub emails: Option<Vec<ContactEmail>>,
    pub phones: Option<Vec<ContactPhone>>,
    pub organizations: Option<Vec<ContactOrganization>>,
    pub addresses: Option<Vec<ContactAddress>>,
    pub notes: Option<Option<String>>,
    pub photo_url: Option<Option<String>>,
    pub photo: Option<Option<ContactPhoto>>,
}

/// Search request for provider-side contact lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactSearchRequest {
    pub query: String,
    pub address_book_id: Option<AddressBookId>,
    pub page_cursor: Option<Vec<u8>>,
    pub limit: Option<u32>,
}

impl ContactSearchRequest {
    #[must_use]
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            address_book_id: None,
            page_cursor: None,
            limit: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contact_corpus_defaults_to_main() {
        // Providers without an auto-collected corpus rely on this default
        // so the consumer's provider-agnostic routing lands them in the
        // primary store.
        assert_eq!(ContactCorpus::default(), ContactCorpus::Main);
    }
}
