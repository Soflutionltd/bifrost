//! Session-frozen routing and the facts owned by each JMAP account.
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;

use bifrost_types::{CursorScope, MembershipScope, OpaqueChangeState};

use crate::account::Account;
use crate::core::capability::{self, Capability};
use crate::core::id::AccountId;
use crate::core::transport::HttpTransport;
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::foreign;
use super::foreign::{JmapRef, Owner};
use super::state_cache::StateCache;
#[cfg(test)]
use super::state_cache::{self, StateMap};

/// Account ids are opaque but registry iteration has a stable lexical order.
#[derive(Clone, Eq, PartialEq)]
struct AccountKey(AccountId);
impl Ord for AccountKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_str().cmp(other.0.as_str())
    }
}
impl PartialOrd for AccountKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl std::borrow::Borrow<str> for AccountKey {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

/// A handle and its observation cache travel together. Network metering may
/// fork a handle, but must never fork its account's observations.
pub(crate) struct Slot<T: HttpTransport = ReqwestTransport, C: Capability = capability::Mail> {
    pub(crate) mail: Account<T, C>,
    pub(crate) limits: CoreLimits,
    pub(crate) email_states: StateCache,
    pub(crate) scopes: Vec<(CursorScope, OpaqueChangeState)>,
    pub(crate) submission: bool,
    shared: bool,
}

impl<T: HttpTransport, C: Capability> Clone for Slot<T, C> {
    fn clone(&self) -> Self {
        Self {
            mail: self.mail.clone(),
            limits: self.limits,
            email_states: self.email_states.clone(),
            scopes: self.scopes.clone(),
            submission: self.submission,
            shared: self.shared,
        }
    }
}

impl<T: HttpTransport, C: Capability> Slot<T, C> {
    pub(crate) fn from_mail(mail: Account<T, C>, limits: CoreLimits) -> Self {
        Self {
            mail,
            limits,
            email_states: StateCache::seeded(None),
            scopes: Vec::new(),
            submission: false,
            shared: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn new(client: crate::client::Client<T>, id: impl Into<AccountId>) -> Self {
        let mail = Account::new(client, id);
        let limits = mail.client().session().object_limits();
        let primary = mail
            .client()
            .primary_account::<crate::core::capability::Mail>()
            .ok();
        let shared = primary
            .as_ref()
            .is_some_and(|primary| primary.id() != mail.id());
        let mut slot = Self::from_mail(mail, limits);
        slot.shared = shared;
        slot
    }

    /// Cross-family views keep the exact account, limits and observation cell.
    pub(crate) fn view<D: Capability>(&self) -> crate::Result<Slot<T, D>> {
        Ok(Slot {
            mail: self.mail.view()?,
            limits: self.limits,
            email_states: self.email_states.clone(),
            scopes: self.scopes.clone(),
            submission: self.submission,
            shared: self.shared,
        })
    }

    pub(crate) fn metered(&self) -> (Self, crate::client::ByteTally) {
        let tally = crate::client::ByteTally::default();
        (self.metered_into(&tally), tally)
    }

    pub(crate) fn metered_into(&self, tally: &crate::client::ByteTally) -> Self {
        let mut slot = self.clone();
        slot.mail = self.mail.metered_into(tally);
        slot
    }

    #[cfg(test)]
    pub(crate) fn from_test(mail: impl Into<Self>, states: StateMap) -> Self {
        mail.into().with_states(states)
    }

    #[cfg(test)]
    pub(crate) fn with_states(mut self, states: StateMap) -> Self {
        self.email_states = state_cache::cell(&states, self.mail.id_str());
        self
    }

    pub(crate) fn owner(&self) -> Owner<'_> {
        if self.shared {
            Owner::Shared(self.mail.id_str())
        } else {
            Owner::Primary
        }
    }

    pub(crate) fn wire<'a>(&self, raw: &'a str) -> &'a str {
        JmapRef::parse(raw).wire(self.owner())
    }
}

pub(crate) struct Families<T: HttpTransport = ReqwestTransport> {
    pub(crate) submission: Option<Slot<T, capability::Submission>>,
    pub(crate) vacation: Option<Slot<T, capability::VacationResponseCap>>,
    pub(crate) quota: Option<Slot<T, capability::Quota>>,
    pub(crate) sieve: Option<Slot<T, capability::Sieve>>,
    pub(crate) contacts: Option<Slot<T, capability::Contacts>>,
    pub(crate) calendars: Option<Slot<T, capability::Calendars>>,
    pub(crate) max_delayed_send: usize,
}

impl<T: HttpTransport> Default for Families<T> {
    fn default() -> Self {
        Self {
            submission: None,
            vacation: None,
            quota: None,
            sieve: None,
            contacts: None,
            calendars: None,
            max_delayed_send: 0,
        }
    }
}

impl<T: HttpTransport> Families<T> {
    fn states(&self, id: &AccountId) -> Option<StateCache> {
        macro_rules! find {
            ($($field:ident),+) => { $(if let Some(slot) = &self.$field {
                if slot.id() == id { return Some(slot.email_states.clone()); }
            })+ };
        }
        find!(submission, vacation, quota, sieve, contacts, calendars);
        None
    }
}

/// The only owner lookup. Departed is deliberately distinct from Primary:
/// scope and search doors refuse it, object doors preserve the literal id.
pub(crate) enum Route<'a, T: HttpTransport> {
    Primary(&'a Slot<T>),
    Shared(&'a Slot<T>),
    Departed,
}

impl<'a, T: HttpTransport> Route<'a, T> {
    pub(crate) fn reachable(self) -> Option<&'a Slot<T>> {
        match self {
            Self::Primary(slot) | Self::Shared(slot) => Some(slot),
            Self::Departed => None,
        }
    }
}

pub(crate) struct Registry<T: HttpTransport = ReqwestTransport> {
    pub(crate) primary: Slot<T>,
    accounts: BTreeMap<AccountKey, Slot<T>>,
    pub(crate) families: Families<T>,
}

impl<T: HttpTransport> Registry<T> {
    pub(crate) fn new(primary: Slot<T>) -> Self {
        Self {
            primary,
            accounts: BTreeMap::new(),
            families: Families::default(),
        }
    }

    pub(crate) fn insert_shared(&mut self, mut slot: Slot<T>) {
        slot.shared = true;
        self.accounts
            .insert(AccountKey(slot.mail.id().clone()), slot);
    }

    /// Optional families keep typed handles, sharing an observation cell with
    /// any family already installed for the same accountId. Never manufacture a
    /// Mail handle for a contacts-only or calendars-only account.
    pub(crate) fn install_family<C: Capability>(&mut self, handle: Account<T, C>) -> Slot<T, C> {
        let mut slot = Slot::from_mail(handle, self.primary.limits);
        if let Some(existing) = self.slot_by_id(slot.id()) {
            slot.email_states = existing.email_states.clone();
            slot.scopes = existing.scopes.clone();
            slot.shared = existing.shared;
        } else if let Some(states) = self.families.states(slot.id()) {
            slot.email_states = states;
        }
        slot
    }

    pub(crate) fn slot_by_id(&self, id: &AccountId) -> Option<&Slot<T>> {
        if id == self.primary.mail.id() {
            Some(&self.primary)
        } else {
            self.accounts.get(id.as_str())
        }
    }

    pub(crate) fn family<'a, C: Capability>(
        &self,
        family: &'a Option<Slot<T, C>>,
    ) -> Option<&'a Slot<T, C>> {
        family.as_ref()
    }

    pub(crate) fn shared(&self) -> impl Iterator<Item = (&AccountId, &Slot<T>)> {
        self.accounts
            .iter()
            .filter(|(_, slot)| slot.shared)
            .map(|(key, slot)| (&key.0, slot))
    }

    pub(crate) fn shared_slot(&self, id: &str) -> Option<&Slot<T>> {
        self.accounts.get(id).filter(|slot| slot.shared)
    }

    pub(crate) fn route(&self, id: JmapRef<'_>) -> Route<'_, T> {
        self.route_owner(id.owner)
    }

    pub(crate) fn route_owner(&self, owner: Owner<'_>) -> Route<'_, T> {
        match owner {
            Owner::Primary => Route::Primary(&self.primary),
            Owner::Shared(owner) => match self.shared_slot(owner) {
                Some(slot) => Route::Shared(slot),
                None => Route::Departed,
            },
        }
    }

    /// Object lookup's departed policy: keep the complete consumer id on the
    /// primary wire. An Outlook user must never see a colliding primary item
    /// as the vanished shared item.
    pub(crate) fn object(&self, raw: &str) -> &Slot<T> {
        match self.route(JmapRef::parse(raw)) {
            Route::Primary(slot) | Route::Shared(slot) => slot,
            Route::Departed => &self.primary,
        }
    }

    pub(crate) fn scope(&self, scope: &CursorScope) -> Route<'_, T> {
        match scope {
            CursorScope::Folder(folder) => self.route(JmapRef::parse(&folder.0)),
            _ => Route::Primary(&self.primary),
        }
    }

    pub(crate) fn cursor_scopes(&self) -> Vec<CursorScope> {
        self.primary
            .scopes
            .iter()
            .chain(self.shared().flat_map(|(_, slot)| slot.scopes.iter()))
            .map(|(scope, _)| scope.clone())
            .collect()
    }

    pub(crate) fn seeded(&self, scope: &CursorScope) -> bool {
        self.scope(scope)
            .reachable()
            .is_some_and(|slot| slot.scopes.iter().any(|(seed, _)| seed == scope))
    }

    pub(crate) fn memberships(&self) -> Vec<MembershipScope> {
        self.shared()
            .map(|(id, _)| foreign::owner_tag(id.as_str()))
            .collect()
    }

    pub(crate) fn has_shared_submission(&self) -> bool {
        self.shared().any(|(_, slot)| slot.submission)
    }

    pub(crate) fn push_scope(&self, account_id: &str) -> Option<&CursorScope> {
        self.shared_slot(account_id)?
            .scopes
            .iter()
            .find_map(|(scope, _)| matches!(scope, CursorScope::Folder(_)).then_some(scope))
    }
}

impl<T: HttpTransport, C: Capability> std::ops::Deref for Slot<T, C> {
    type Target = Account<T, C>;
    fn deref(&self) -> &Self::Target {
        &self.mail
    }
}

#[cfg(test)]
impl<T: HttpTransport, C: Capability> From<Account<T, C>> for Slot<T, C> {
    fn from(mail: Account<T, C>) -> Self {
        Self::new(mail.client().clone(), mail.id().clone())
    }
}

#[cfg(test)]
impl<T: HttpTransport> Registry<T> {
    pub(crate) fn from_test<H: Clone + Into<Slot<T>>>(
        primary: impl Into<Slot<T>>,
        shared: Arc<HashMap<AccountId, H>>,
    ) -> Self {
        let mut primary = primary.into();
        primary.shared = false;
        let mut registry = Self::new(primary);
        for handle in shared.values() {
            registry.insert_shared(handle.clone().into());
        }
        registry
    }

    pub(crate) fn with_limits(mut self, limits: CoreLimits) -> Self {
        self.primary.limits = limits;
        for slot in self.accounts.values_mut() {
            slot.limits = limits;
        }
        self
    }

    pub(crate) fn from_test_states<H: Clone + Into<Slot<T>>>(
        primary: impl Into<Slot<T>>,
        shared: Arc<HashMap<AccountId, H>>,
        limits: CoreLimits,
        states: StateMap,
    ) -> Self {
        let mut registry = Self::from_test(primary, shared).with_limits(limits);
        registry.primary.email_states = state_cache::cell(&states, registry.primary.mail.id_str());
        for slot in registry.accounts.values_mut() {
            slot.email_states = state_cache::cell(&states, slot.mail.id_str());
        }
        registry
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct RegistryTransport;

#[cfg(test)]
impl HttpTransport for RegistryTransport {
    async fn api_request(
        &self,
        _: &str,
        _: Vec<u8>,
        _: bifrost_net::ReplayPolicy,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        panic!("registry fixture must not perform I/O")
    }
    async fn upload(
        &self,
        _: &str,
        _: Vec<u8>,
        _: Option<&str>,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        panic!("registry fixture must not upload")
    }
    async fn download(
        &self,
        _: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        panic!("registry fixture must not download")
    }
    async fn get_session(
        &self,
        _: &str,
    ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
        panic!("registry fixture must not fetch a session")
    }
}

#[cfg(test)]
pub(crate) fn test_registry(primary: &str, scopes: &[CursorScope]) -> Registry<RegistryTransport> {
    let session = serde_json::from_value(serde_json::json!({
        "capabilities": {}, "accounts": {}, "primaryAccounts": {"urn:ietf:params:jmap:mail": primary},
        "username": "test", "apiUrl": "https://example.test/jmap",
        "downloadUrl": "https://example.test/{accountId}/{blobId}",
        "uploadUrl": "https://example.test/{accountId}",
        "eventSourceUrl": "https://example.test/events", "state": "s1"
    })).unwrap();
    let client = crate::client::Client::with_transport(
        RegistryTransport,
        session,
        "https://example.test/session",
    )
    .unwrap();
    let mut registry = Registry::new(Slot::new(client.clone(), primary));
    for kind in [
        bifrost_types::ObjectType::Email,
        bifrost_types::ObjectType::Mailbox,
    ] {
        let scope = CursorScope::Type(kind);
        if scopes.contains(&scope) {
            registry.primary.scopes.push((
                scope.clone(),
                super::state::encode_for_scope(&scope, "s1".to_owned()).unwrap(),
            ));
        }
    }
    for scope in scopes {
        if let CursorScope::Folder(folder) = scope {
            let parsed = JmapRef::parse(&folder.0);
            if let Owner::Shared(owner) = parsed.owner {
                let key = AccountKey(AccountId::new(owner));
                let slot = registry.accounts.entry(key).or_insert_with(|| {
                    let mut slot = Slot::new(client.clone(), owner);
                    slot.shared = true;
                    slot
                });
                slot.scopes.push((
                    scope.clone(),
                    super::state::encode_for_scope(scope, "s1".to_owned()).unwrap(),
                ));
            }
        }
    }
    registry
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{FolderId, ObjectType, PrincipalId};

    fn scope(owner: &str) -> CursorScope {
        CursorScope::Folder(foreign::encode_foreign_account(owner))
    }

    #[test]
    fn cursor_scopes_lead_with_seeded_type_scopes_then_sorted_foreign() {
        let scopes = [
            scope("zeta"),
            CursorScope::Type(ObjectType::Mailbox),
            scope("alpha"),
            CursorScope::Type(ObjectType::Email),
        ];
        assert_eq!(
            test_registry("primary", &scopes).cursor_scopes(),
            vec![
                CursorScope::Type(ObjectType::Email),
                CursorScope::Type(ObjectType::Mailbox),
                scope("alpha"),
                scope("zeta")
            ]
        );
    }
    #[test]
    fn cursor_scope_order_does_not_depend_on_seed_iteration_order() {
        let forward = [scope("a"), scope("b"), scope("c")];
        let reverse: Vec<_> = forward.iter().rev().cloned().collect();
        assert_eq!(
            test_registry("primary", &forward).cursor_scopes(),
            test_registry("primary", &reverse).cursor_scopes()
        );
    }
    #[test]
    fn an_unseeded_type_scope_is_not_discovered() {
        assert_eq!(
            test_registry("primary", &[CursorScope::Type(ObjectType::Email)]).cursor_scopes(),
            vec![CursorScope::Type(ObjectType::Email)]
        );
        assert!(test_registry("primary", &[]).cursor_scopes().is_empty());
    }
    #[test]
    fn discovery_uses_owner_memberships_and_deduplicates_legacy_mailboxes() {
        let scopes = [
            scope("b"),
            scope("a"),
            CursorScope::Folder(foreign::encode_foreign("a", "inbox")),
        ];
        assert_eq!(
            test_registry("primary", &scopes).memberships(),
            vec![
                MembershipScope::Owner(PrincipalId("a".into())),
                MembershipScope::Owner(PrincipalId("b".into()))
            ]
        );
    }
    #[test]
    fn object_and_scope_routes_distinguish_departed_from_primary() {
        let registry = test_registry("primary", &[scope("shared")]);
        for native in ["M1", "T1", "B1", "inbox", ""] {
            let raw = foreign::encode_object("shared", native);
            let slot = registry.object(&raw);
            assert_eq!(slot.id_str(), "shared");
            assert_eq!(slot.wire(&raw), native);
            assert_eq!(registry.object(native).id_str(), "primary");
            let departed = foreign::encode_object("gone", native);
            assert!(matches!(
                registry.route(JmapRef::parse(&departed)),
                Route::Departed
            ));
            assert_eq!(registry.object(&departed).wire(&departed), departed);
        }
        assert!(matches!(registry.scope(&scope("shared")), Route::Shared(_)));
        assert!(matches!(registry.scope(&scope("gone")), Route::Departed));
        assert!(matches!(
            registry.scope(&CursorScope::Type(ObjectType::Email)),
            Route::Primary(_)
        ));
        assert!(matches!(
            registry.scope(&CursorScope::Folder(FolderId("bare".into()))),
            Route::Primary(_)
        ));
        assert_eq!(registry.push_scope("shared"), Some(&scope("shared")));
        assert_eq!(registry.push_scope("gone"), None);
    }
    #[test]
    fn primary_qualification_is_never_stripped() {
        let registry = test_registry("primary", &[scope("shared")]);
        let qualified = foreign::encode_object("primary", "M1");
        assert!(matches!(
            registry.route(JmapRef::parse(&qualified)),
            Route::Departed
        ));
        assert_eq!(registry.object(&qualified).wire(&qualified), qualified);
        assert_eq!(
            registry.shared_slot("shared").unwrap().wire(&qualified),
            qualified
        );
    }
    #[tokio::test]
    async fn slots_keep_state_and_family_selection_with_their_account() {
        let mut registry = test_registry("primary", &[scope("shared")]);
        registry
            .primary
            .email_states
            .observe_probe(None, "primary-state".into())
            .await;
        let shared = registry.shared_slot("shared").unwrap();
        shared
            .email_states
            .observe_probe(None, "shared-state".into())
            .await;
        let metered = shared.metered().0;
        metered
            .email_states
            .observe_set(Some(("shared-state".into(), "shared-next".into())))
            .await;
        assert_eq!(
            shared.email_states.guard().await.as_deref(),
            Some("shared-next")
        );
        assert_eq!(
            registry.primary.email_states.guard().await.as_deref(),
            Some("primary-state")
        );
        registry.families.submission = Some(registry.install_family(Account::<
            _,
            capability::Submission,
        >::new(
            registry.primary.client().clone(),
            "primary",
        )));
        assert_eq!(
            registry
                .family(&registry.families.submission)
                .unwrap()
                .email_states
                .guard()
                .await
                .as_deref(),
            Some("primary-state")
        );
        let other = Account::<_, capability::Submission>::new(
            registry.primary.client().clone(),
            "submission-only",
        );
        registry.families.submission = Some(registry.install_family(other));
        assert_eq!(
            registry
                .family(&registry.families.submission)
                .unwrap()
                .id_str(),
            "submission-only"
        );
        assert_eq!(registry.shared().count(), 1);
        assert_eq!(registry.cursor_scopes(), vec![scope("shared")]);
    }
}
