use futures::{FutureExt as _, StreamExt as _};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFuture, AccountOperation, AccountStream,
    AddressBook, AddressBookId, AttachmentHandle, BlobHandle, ByteRange, Calendar, CalendarEvent,
    ChangeCursor, CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch,
    ContactSearchRequest, ContainerId, ContainerKind, ContainerList, CostClass, CursorDescriptor,
    CursorEstablishment, CursorScope, DirectoryCard, DirectoryGroup, DirectoryGroupId,
    DirectoryGroupMember, DraftHandle, DraftPatch, ErrorScope, EventCreate, EventId, EventPatch,
    EventRange, EventSearchRequest, FilterValidation, HostedAttachment, HydratedObject,
    HydrationProjection, IdempotencyKey, Identity, IdentityId, IdentityPatch, Importance,
    InventoryEvent, InventoryPartition, InventoryPartitioning, ItemOutcome, Label, MembershipScope,
    Message, MutationSuccess, MutationTarget, ObjectId, Page, Priority, Projection, QuotaInfo,
    RsvpStatus, ScopeLifecycleEvent, SearchRequest, SendAs, SendRequest, ServerFilter,
    ServerFilterCreate, ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent,
    SyncStrategy, ThreadHydration, ThreadId, VacationConfig, WatchEvent,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::core::transport::HttpTransport;
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::state_cache::{self, StateMap};
use super::{
    blob, calendar_ops, changes, contacts, discover, filters, foreign, hydrate, inventory,
    mutation, pim, push, state,
};

pub(crate) type CloseCompletion =
    futures::future::Shared<futures::future::BoxFuture<'static, Result<(), AccountError>>>;

/// Start account teardown once, in a task that outlives any caller waiting on
/// it. The initializer is invoked only when the returned future is first
/// polled, so a future dropped before polling cannot mark the account closed
/// without starting shutdown.
fn close_once<F>(
    completion: Arc<std::sync::OnceLock<CloseCompletion>>,
    start: impl FnOnce() -> F + Send + 'static,
) -> AccountFuture<Result<(), AccountError>>
where
    F: Future<Output = Result<(), AccountError>> + Send + 'static,
{
    Box::pin(async move {
        let completion = completion
            .get_or_init(|| {
                let task = tokio::spawn(start());
                async move { task.await.expect("JMAP account teardown task panicked") }
                    .boxed()
                    .shared()
            })
            .clone();
        completion.await
    })
}

/// The engine-facing account is pinned to the production transport, not
/// generic over one: WebSocket push is reqwest-specific, so `JmapAccount`
/// cannot be parameterized without either dropping push or growing a second
/// transport trait for it.
///
/// The accepted cost is a testing one, and it is a known, bounded residual
/// rather than an oversight. Because no scripted transport can be installed
/// at this level, a WHOLE `Account` call - `capabilities()`,
/// `describe_cursor`, the mutation doors - is undrivable hermetically; only
/// the false direction (a refusal that never reaches the wire) can be pinned
/// here, which is what `capability_contract_tests.rs` documents and does.
/// Everything pure is instead extracted into free functions and tested
/// directly, and the sync request helpers below this struct stay generic over
/// `HttpTransport` so a scripted transport reaches them. Closing the residual
/// means threading the transport generic through `JmapAccount` (and giving
/// push its own seam), not adding a test-support type elsewhere.
type MailAccount<T = ReqwestTransport> = crate::account::Account<T>;

pub(crate) struct JmapAccount {
    pub(crate) client: Client,
    pub(crate) mail: MailAccount,
    /// Foreign (shared/delegate) mail accounts, keyed by JMAP
    /// `accountId`. Enumerated at `open` from the session's non-personal
    /// accounts that advertise the mail capability. A foreign `Folder`
    /// scope routes here via `mail_for_scope`.
    pub(crate) foreign_mail: Arc<HashMap<String, MailAccount>>,
    /// Foreign mail accounts that were successfully seeded and advertise
    /// `urn:ietf:params:jmap:submission`.
    pub(crate) foreign_submission: Arc<HashSet<String>>,
    pub(crate) submission: Option<MailAccount>,
    /// `urn:ietf:params:jmap:submission` `maxDelayedSend` (seconds);
    /// `0` when the server advertises no scheduled-send window. Read by
    /// `send_message` / `reschedule_send` for boundary validation.
    pub(crate) max_delayed_send: usize,
    pub(crate) vacation: Option<MailAccount>,
    pub(crate) quota: Option<MailAccount>,
    pub(crate) sieve: Option<MailAccount>,
    pub(crate) contacts: Option<MailAccount>,
    pub(crate) calendars: Option<MailAccount>,
    pub(crate) self_emails: Vec<String>,
    pub(crate) caps: AccountCapabilities,
    pub(crate) core_limits: CoreLimits,
    pub(crate) seed_states: HashMap<CursorScope, bifrost_types::OpaqueChangeState>,
    pub(crate) ws: push::WsState,
    pub(crate) subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, push::DataTypeSet>>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) closed: Arc<AtomicBool>,
    /// Shared teardown result. The first polled `close()` starts one
    /// background teardown; later callers join that same completion.
    pub(crate) close_completion: Arc<std::sync::OnceLock<CloseCompletion>>,
    pub(crate) subscription_seq: AtomicU64,
    /// Per-`accountId` `Email/changes` state cache (the mutation
    /// `ifInState` cache and changes side-cache). Keyed by accountId so
    /// the primary and each foreign account hold distinct state - a
    /// single `Option<String>` would clobber one with the other.
    pub(crate) email_states: StateMap,
    pub(crate) mailbox_states: StateMap,
    /// Private Mailbox/changes position for scope lifecycle polling. This is
    /// deliberately separate from `mailbox_states`, whose consumers may
    /// fast-forward it after a delta pass or local Mailbox/set.
    pub(crate) lifecycle_mailbox_states: StateMap,
    pub(crate) mailbox_names: Arc<Mutex<HashMap<String, String>>>,
}

impl JmapAccount {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: Client,
        mail: MailAccount,
        foreign_mail: HashMap<String, MailAccount>,
        foreign_submission: HashSet<String>,
        submission: Option<MailAccount>,
        max_delayed_send: usize,
        vacation: Option<MailAccount>,
        quota: Option<MailAccount>,
        sieve: Option<MailAccount>,
        contacts: Option<MailAccount>,
        calendars: Option<MailAccount>,
        self_emails: Vec<String>,
        caps: AccountCapabilities,
        core_limits: CoreLimits,
        seed_states: HashMap<CursorScope, bifrost_types::OpaqueChangeState>,
        ws: push::WsState,
        shutdown: CancellationToken,
        email_states: HashMap<String, Option<String>>,
        mailbox_states: HashMap<String, Option<String>>,
        lifecycle_mailbox_states: HashMap<String, Option<String>>,
        mailbox_names: HashMap<String, String>,
    ) -> Self {
        Self {
            client,
            mail,
            foreign_mail: Arc::new(foreign_mail),
            foreign_submission: Arc::new(foreign_submission),
            submission,
            max_delayed_send,
            vacation,
            quota,
            sieve,
            contacts,
            calendars,
            self_emails,
            caps,
            core_limits,
            seed_states,
            ws,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            shutdown,
            closed: Arc::new(AtomicBool::new(false)),
            close_completion: Arc::new(std::sync::OnceLock::new()),
            subscription_seq: AtomicU64::new(1),
            email_states: Arc::new(Mutex::new(email_states)),
            mailbox_states: Arc::new(Mutex::new(mailbox_states)),
            lifecycle_mailbox_states: Arc::new(Mutex::new(lifecycle_mailbox_states)),
            mailbox_names: Arc::new(Mutex::new(mailbox_names)),
        }
    }

    /// Resolve the `MailAccount` handle that routes a cursor scope. A
    /// foreign `Folder` scope (whose `FolderId` carries a JMAP
    /// `accountId`) routes to that account's handle; every primary
    /// `Type(_)` / `Query(_)` scope routes to `self.mail`.
    pub(crate) fn mail_for_scope(&self, scope: &CursorScope) -> &MailAccount {
        match resolve_foreign_account_id(scope, |id| self.foreign_mail.contains_key(id)) {
            Some(account_id) => self
                .foreign_mail
                .get(&account_id)
                .expect("resolve_foreign_account_id only returns a known foreign id"),
            None => &self.mail,
        }
    }

    /// The accountId that keys the state cache for a scope: the parsed
    /// foreign accountId for a `Folder` scope, else the primary account's
    /// id. Both forms key the same per-accountId maps.
    pub(crate) fn account_id_for_scope(&self, scope: &CursorScope) -> String {
        self.mail_for_scope(scope).id_str().to_string()
    }

    /// Resolve a thread-keyed operation to its owning account.
    ///
    /// A foreign thread id is owner-qualified in the object namespace (the
    /// foreign inventory and hydration mint it that way), and `Thread/get`
    /// is accountId-scoped with the thread id as its only operand. Routed
    /// to the primary account, a foreign thread id either finds nothing
    /// or, on an id collision, resolves an UNRELATED primary thread whose
    /// messages the caller's mutation then rewrites. As with a message id,
    /// an id naming an account this session no longer holds stays on the
    /// primary route in its literal form so the server, not a guessed
    /// local route, reports the miss.
    fn mail_for_thread(&self, thread: &ThreadId) -> MailAccount {
        route_object_id(&thread.0, &self.mail, &self.foreign_mail).clone()
    }

    fn mail_for_mutation_target(&self, target: &MutationTarget) -> MailAccount {
        route_mutation_target(target, &self.mail, &self.foreign_mail).clone()
    }

    /// Draft handles contain only an Email id, so the whole draft lifecycle
    /// must use one account namespace. Submission is selected when present,
    /// matching draft_send; Mail remains the usable fallback on accounts
    /// without Submission, where sending a draft is unavailable.
    fn draft_account(&self) -> &MailAccount {
        draft_route_account(&self.mail, self.submission.as_ref())
    }

    /// The owning shared-account identity for a foreign `Folder` scope,
    /// or `None` for a primary scope. Drives the revocation-isolation
    /// decision (quarantine the foreign scope vs escalate account-wide).
    pub(crate) fn owner_of_scope(&self, scope: &CursorScope) -> Option<bifrost_types::MailboxId> {
        resolve_foreign_account_id(scope, |id| self.foreign_mail.contains_key(id))
            .map(bifrost_types::MailboxId)
    }

    /// True when `scope` is a `Folder` scope that parses as a foreign
    /// (shared/delegate) mailbox but whose account is no longer
    /// registered in `foreign_mail`. This happens when a foreign scope
    /// was seeded at a prior `open` but the foreign account has since
    /// disappeared from the session (revoked delegation, removed share).
    /// Such a scope must NOT silently fall back to the primary account in
    /// `mail_for_scope`: the foreign mailbox id (e.g. `inbox`) would then
    /// resolve against the *primary* mailbox, conflating two accounts.
    /// Callers route it to a terminal error instead.
    pub(crate) fn is_unregistered_foreign_scope(&self, scope: &CursorScope) -> bool {
        is_unregistered_foreign(scope, |id| self.foreign_mail.contains_key(id))
    }

    pub(crate) fn cursor_scopes(&self) -> Vec<CursorScope> {
        cursor_scopes_from_seeds(self.seed_states.keys())
    }

    /// The owner-tag memberships every foreign account contributes:
    /// `Mailbox(accountId)` per foreign account. Emitted alongside the
    /// primary account's per-mailbox memberships during discovery.
    pub(crate) fn foreign_owner_memberships(&self) -> Vec<MembershipScope> {
        foreign_owner_memberships_from_scopes(self.seed_states.keys())
    }

    pub(crate) fn next_subscription_handle(&self) -> SubscriptionHandle {
        let next = self.subscription_seq.fetch_add(1, Ordering::AcqRel);
        SubscriptionHandle(format!("jmap-ws-{next}"))
    }
}

// The three functions below, like `route_object_id`, exist as free
// functions purely so they can be exercised: `JmapAccount` pins
// `MailAccount = Account<ReqwestTransport>`, so the trait methods that
// wrap them cannot be driven without a live transport. Each is pure
// given its inputs, so lifting the decision out is enough - the trait
// methods stay one-line delegations to keep the pin meaningful.

/// Discovery order for the seeded cursor scopes.
///
/// The two `Type` scopes come first in a FIXED order (Email, then
/// Mailbox) and only when seeded; the foreign account-level `Folder`
/// scopes follow, sorted by their encoded `FolderId`. The sort is not
/// cosmetic: `seed_states` is a `HashMap`, so without it discovery order
/// would vary run to run and the engine's scope set would churn.
fn cursor_scopes_from_seeds<'a>(
    seeded: impl IntoIterator<Item = &'a CursorScope>,
) -> Vec<CursorScope> {
    let seeded: Vec<&CursorScope> = seeded.into_iter().collect();

    let ordered = [
        CursorScope::Type(bifrost_types::ObjectType::Email),
        CursorScope::Type(bifrost_types::ObjectType::Mailbox),
    ];
    let mut scopes: Vec<CursorScope> = ordered
        .into_iter()
        .filter(|scope| seeded.contains(&scope))
        .collect();

    // Each foreign (shared/delegate) account surfaces as ONE seeded
    // account-level `Folder` scope (the foreign accountId rides in the
    // FolderId; the mailbox part is empty).
    let mut foreign: Vec<CursorScope> = seeded
        .into_iter()
        .filter(|scope| matches!(scope, CursorScope::Folder(_)))
        .cloned()
        .collect();
    foreign.sort_by(|a, b| match (a, b) {
        (CursorScope::Folder(x), CursorScope::Folder(y)) => x.0.cmp(&y.0),
        _ => std::cmp::Ordering::Equal,
    });
    scopes.extend(foreign);
    scopes
}

/// Every scope is one full walk. Email pages within that walk by anchor;
/// splitting it into positional partitions would discard that anchor at
/// every engine boundary and make deletion shifts lossy again.
fn partitioning_for_scope(
    _scope: &CursorScope,
    _max_objects_in_get: usize,
) -> InventoryPartitioning {
    InventoryPartitioning::Full
}

/// Probe the selected account immediately before returning a cursor.
/// The open-time seed is only the membership gate: reusing its state for a
/// later `RestartScope` can repeat `cannotCalculateChanges` forever after the
/// server expires that old position.
async fn fresh_cursor_establishment<T: HttpTransport>(
    mail: MailAccount<T>,
    scope: CursorScope,
    email_states: StateMap,
    mailbox_states: StateMap,
) -> Result<CursorEstablishment, AccountError> {
    let (states, account_id) = match &scope {
        CursorScope::Type(bifrost_types::ObjectType::Email) | CursorScope::Folder(_) => {
            (&email_states, mail.id_str().to_string())
        }
        CursorScope::Type(bifrost_types::ObjectType::Mailbox) => {
            (&mailbox_states, mail.id_str().to_string())
        }
        _ => return Err(unseeded_scope_error(scope)),
    };
    // Capture the cache value before the network read. If a concurrent
    // mutation advances it while the probe is in flight, the CAS below keeps
    // that newer state instead of replacing it with this probe's older one.
    let cached_before_probe = state_cache::get(states, &account_id).await;
    let state_string = match &scope {
        CursorScope::Type(bifrost_types::ObjectType::Email) | CursorScope::Folder(_) => mail
            .call(crate::email::EmailGet::new().ids(Vec::<crate::email::EmailId>::new()))
            .await
            .map(crate::core::get::GetResponse::<crate::email::Email>::into_state),
        CursorScope::Type(bifrost_types::ObjectType::Mailbox) => mail
            .call(crate::mailbox::MailboxGet::new().ids(Vec::<crate::mailbox::MailboxId>::new()))
            .await
            .map(crate::core::get::GetResponse::<crate::mailbox::Mailbox>::into_state),
        _ => unreachable!("scope kind was checked above"),
    }
    .map_err(|error| {
        super::error::into_account_error(
            error,
            super::error::JmapErrorContext::cursor(
                AccountOperation::EstablishCursor,
                scope.clone(),
            ),
        )
    })?;
    state_cache::advance(
        states,
        &account_id,
        cached_before_probe.as_deref(),
        state_string.clone(),
    )
    .await;
    Ok(cursor_from_state(scope, state_string))
}

fn cursor_from_state(scope: CursorScope, state_string: String) -> CursorEstablishment {
    let server_state = state::encode_for_scope(&scope, state_string)
        .expect("seeded JMAP cursor scopes have encodable payloads");
    CursorEstablishment::Ready(ChangeCursor {
        scope,
        server_state,
        advanced_through: None,
        envelope_version: state::OUTER_CURSOR_ENVELOPE_VERSION,
    })
}

/// An unseeded scope is unsupported rather than a cursor over an empty
/// state, which would present as a working cursor that returns nothing.
fn unseeded_scope_error(scope: CursorScope) -> AccountError {
    super::error::unsupported_error(
        AccountOperation::EstablishCursor,
        Some(ErrorScope::Cursor(scope)),
        "JMAP did not seed a cursor for this scope",
    )
}

/// Production-dead: kept only as the test pin for the object-id routing
/// rule (a registered foreign id resolves, a departed share stays literal
/// on the primary route). The production route is `route_object_id`.
#[cfg(test)]
fn foreign_account_id_for_object<F>(id: &ObjectId, is_registered: F) -> Option<String>
where
    F: Fn(&str) -> bool,
{
    super::foreign::parse_object(&id.0)
        .filter(|(account_id, _)| is_registered(account_id))
        .map(|(account_id, _)| account_id.to_string())
}

/// Select the handle an object-namespace id routes to. Message and
/// thread ids ride the same `accountId\u{1f}native` encoding, so one
/// selector covers both; an id whose account is not registered (a
/// departed share) stays on the primary route in its literal form.
///
/// Generic over the handle type because `JmapAccount` hardwires
/// `ReqwestTransport` and cannot be built over a scripted transport, so
/// this exact boundary - the one a primary-fallback regression would
/// cross - is pinned over plain string handles instead. The account
/// methods above are one-line delegations to keep that pin meaningful.
fn route_object_id<'a, H>(raw: &str, primary: &'a H, foreign: &'a HashMap<String, H>) -> &'a H {
    super::foreign::parse_object(raw)
        .and_then(|(account_id, _)| foreign.get(account_id))
        .unwrap_or(primary)
}

/// Handle selection for a `MutationTarget`: message and thread targets
/// route by their id's owner; any other (future) target shape stays
/// primary, the conservative default for an id form this codec cannot
/// attribute.
fn route_mutation_target<'a, H>(
    target: &MutationTarget,
    primary: &'a H,
    foreign: &'a HashMap<String, H>,
) -> &'a H {
    match target {
        MutationTarget::Message(id) => route_object_id(&id.0, primary, foreign),
        MutationTarget::Thread(thread) => route_object_id(&thread.0, primary, foreign),
        _ => primary,
    }
}

/// Draft handles are bare Email ids, so draft create/update/discard must
/// agree with draft_send's account. Prefer Submission when available, while
/// preserving draft editing on mail-only accounts.
fn draft_route_account<'a, H>(mail: &'a H, submission: Option<&'a H>) -> &'a H {
    submission.unwrap_or(mail)
}

/// Uploaded blob ids are account scoped, just like the drafts consuming them.
fn upload_draft_attachment<T: crate::core::transport::HttpTransport>(
    mail: &crate::account::Account<T>,
    submission: Option<&crate::account::Account<T>>,
    bytes: AccountStream<Result<bytes::Bytes, AccountError>>,
    mime: String,
) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
    pim::attachment_upload(draft_route_account(mail, submission).clone(), bytes, mime)
}

/// Pure descriptor logic behind `Account::describe_cursor`, split out
/// for direct testing.
///
/// Decoding alone is not support: legacy `Thread` and `Query` cursors
/// still decode (their scope tags survive the envelope bump so old
/// durable rows stay readable) but `changes::stream` terminates both
/// `Unsupported(SyncChanges)` - thread inventory derives from Email
/// inventory, and the v1 contract registers no query definitions to
/// turn a query id into a filter. Reporting `Cheap`/`ServerCursor` for
/// one would promise a strategy that dies on its first poll, so only
/// the scopes the change stream actually drives report one.
fn describe_cursor_support(cursor: &ChangeCursor) -> CursorDescriptor {
    let supported = matches!(
        state::decode_cursor(cursor),
        Ok((
            state::JmapScopeRepr::Email
                | state::JmapScopeRepr::Mailbox
                | state::JmapScopeRepr::Folder { .. },
            _
        ))
    );
    CursorDescriptor {
        cost_class: if supported {
            CostClass::Cheap
        } else {
            CostClass::Expensive
        },
        strategy: if supported {
            SyncStrategy::ServerCursor
        } else {
            SyncStrategy::None
        },
        freshness: None,
    }
}

impl Account for JmapAccount {
    fn protocol(&self) -> bifrost_types::Protocol {
        bifrost_types::Protocol::Jmap
    }

    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, priority: Priority) {
        self.client.transport().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.client.transport().set_bandwidth_cap(bps);
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        describe_cursor_support(cursor)
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        discover::cursor_scopes(self.cursor_scopes())
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        discover::memberships(self.mail.clone(), self.foreign_owner_memberships())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        discover::scope_lifecycle(
            self.mail.clone(),
            self.core_limits,
            Arc::clone(&self.lifecycle_mailbox_states),
            self.mail.id_str().to_string(),
            Arc::clone(&self.mailbox_names),
            self.shutdown.clone(),
            self.client.clone(),
        )
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        if !self.seed_states.contains_key(&scope) {
            return Box::pin(async move { Err(unseeded_scope_error(scope)) });
        }
        let mail = self.mail_for_scope(&scope).clone();
        Box::pin(fresh_cursor_establishment(
            mail,
            scope,
            Arc::clone(&self.email_states),
            Arc::clone(&self.mailbox_states),
        ))
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        if self.is_unregistered_foreign_scope(&scope) {
            let domain = bifrost_types::CoverageDomain::full(scope.clone());
            let err =
                super::error::unregistered_foreign_scope(scope, AccountOperation::SyncInventory);
            return Box::pin(
                async_stream::stream! { yield super::error::terminated(err); }
                    .map(bifrost_types::lift_complete_walk(domain)),
            );
        }
        let mail = self.mail_for_scope(&scope).clone();
        let owner = self.owner_of_scope(&scope);
        let domain = bifrost_types::CoverageDomain::full(scope.clone());
        Box::pin(
            inventory::stream(mail, self.core_limits, scope, owner)
                .map(bifrost_types::lift_complete_walk(domain)),
        )
    }

    fn inventory_partitioning(&self, scope: &CursorScope) -> InventoryPartitioning {
        partitioning_for_scope(scope, self.core_limits.max_objects_in_get)
    }

    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<InventoryEvent> {
        if self.is_unregistered_foreign_scope(&scope) {
            let domain = bifrost_types::CoverageDomain::full(scope.clone());
            let err =
                super::error::unregistered_foreign_scope(scope, AccountOperation::SyncInventory);
            return Box::pin(
                async_stream::stream! { yield super::error::terminated(err); }
                    .map(bifrost_types::lift_complete_walk(domain)),
            );
        }
        let mail = self.mail_for_scope(&scope).clone();
        let owner = self.owner_of_scope(&scope);
        // The domain is the PARTITION's extent, not the scope's. A partition
        // that finished cleanly proves nothing about its neighbours, and a
        // full-scope claim here would discharge their debt for free.
        //
        // `uid_validity` is unused: JMAP advertises `Time` / `PageCount`
        // partitioning, never `Uid`, so the UID arm is unreachable from this
        // account.
        let domain = bifrost_types::CoverageDomain::for_partition(scope.clone(), &partition, 0);
        Box::pin(
            inventory::stream_partition(mail, self.core_limits, scope, partition, owner)
                .map(bifrost_types::lift_complete_walk(domain)),
        )
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        // A foreign-qualified id must hydrate against its OWNING account
        // (the same selection `mail_for_scope` makes on the changes path);
        // `Email/get` is accountId-scoped, so routing it through the primary
        // account cannot reach a shared mailbox's mail.
        hydrate::stream(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            self.core_limits,
            ids,
            projection,
        )
    }

    fn changes_stream(
        &self,
        cursor: ChangeCursor,
    ) -> AccountStream<SyncEvent<bifrost_types::Change>> {
        if self.is_unregistered_foreign_scope(&cursor.scope) {
            let err = super::error::unregistered_foreign_scope(
                cursor.scope,
                AccountOperation::SyncChanges,
            );
            return Box::pin(async_stream::stream! { yield super::error::terminated(err); });
        }
        let mail = self.mail_for_scope(&cursor.scope).clone();
        let account_id = self.account_id_for_scope(&cursor.scope);
        let owner = self.owner_of_scope(&cursor.scope);
        changes::stream(
            mail,
            account_id,
            self.core_limits,
            cursor,
            owner,
            Arc::clone(&self.email_states),
            Arc::clone(&self.mailbox_states),
        )
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        let scopes = scopes.to_vec();
        let future = push::subscribe(
            self.client.clone(),
            self.caps.push,
            self.next_subscription_handle(),
            scopes.clone(),
            push::SubscribeState {
                subscriptions: Arc::clone(&self.subscriptions),
                enabled: Arc::clone(&self.ws.enabled),
                push_state: Arc::clone(&self.ws.push_state),
                closed: Arc::clone(&self.closed),
            },
        );
        Box::pin(async move {
            let (handle, accepted) = future.await?;
            let expected = (0..scopes.len())
                .map(|index| bifrost_types::BatchItemId(index.to_string()))
                .collect::<Vec<_>>();
            let mut outcomes = bifrost_types::BatchOutcomeBuilder::new();
            for ((id, scope), accepted) in expected.iter().cloned().zip(scopes).zip(accepted) {
                if accepted {
                    outcomes.push_succeeded(id, scope);
                } else {
                    outcomes.push_failed(
                        id,
                        super::error::unsupported_error(
                            AccountOperation::PushSubscribe,
                            Some(bifrost_types::ErrorScope::Cursor(scope)),
                            "JMAP push does not map this cursor scope to a data type",
                        ),
                    );
                }
            }
            bifrost_types::PushSubscription::new(
                Some(handle),
                outcomes
                    .finalize(&expected)
                    .expect("every requested push scope is accounted for once"),
            )
            .map_err(|failure| {
                super::error::internal_invariant(
                    AccountOperation::PushSubscribe,
                    format!("JMAP push subscription invariant failed: {failure}"),
                )
            })
        })
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        push::unsubscribe(
            self.client.clone(),
            handle,
            Arc::clone(&self.subscriptions),
            Arc::clone(&self.ws.enabled),
            Arc::clone(&self.ws.push_state),
        )
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push::stream(self.ws.tx.subscribe(), self.shutdown.clone())
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open(
            self.client.clone(),
            self.mail.id().clone(),
            Arc::clone(&self.foreign_mail),
            handle,
        )
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_range(handle, range)
    }

    fn open_raw_rfc822(&self, message: ObjectId) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_raw_rfc822(
            self.client.clone(),
            self.mail.id().clone(),
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            message,
        )
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::set_flags(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            self.core_limits,
            Arc::clone(&self.email_states),
            targets,
            op,
            key,
        )
    }

    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::move_to(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            self.core_limits,
            Arc::clone(&self.email_states),
            targets,
            destination,
            key,
        )
    }

    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::destroy(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            self.core_limits,
            Arc::clone(&self.email_states),
            targets,
            key,
        )
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::add_to_container(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            container,
        )
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::remove_from_container(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            container,
        )
    }

    fn set_keyword(
        &self,
        target: MutationTarget,
        keyword: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::set_keyword(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            keyword,
            value,
        )
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::SetLabelMembership,
            None,
            "JMAP does not support label membership",
        );
        Box::pin(async move { Err(err) })
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::SetCategory,
            None,
            "JMAP does not support categories",
        );
        Box::pin(async move { Err(err) })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::SetExtendedProperty,
            None,
            "JMAP does not support extended properties",
        );
        Box::pin(async move { Err(err) })
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::set_is_read(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            is_read,
        )
    }

    fn set_importance(
        &self,
        target: MutationTarget,
        level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::set_importance(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            level,
        )
    }

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        if let Some(send_as) = request.send_as.as_ref() {
            let id = send_as.mailbox().0.clone();
            if let Err(err) = route_send_as(
                send_as,
                !self.foreign_submission.is_empty(),
                request.scheduled.is_some(),
                self.foreign_mail.contains_key(&id),
                self.foreign_submission.contains(&id),
            ) {
                return Box::pin(async move { Err(err) });
            }
            let mail = self
                .foreign_mail
                .get(&id)
                .cloned()
                .expect("route_send_as validated foreign_mail membership");
            let self_address = self
                .self_emails
                .first()
                .cloned()
                .map(bifrost_types::Address::bare);
            return pim::send_message(
                mail,
                Arc::clone(&self.email_states),
                id,
                0,
                Some(pim::ForeignSubmission {
                    mode: send_as.clone(),
                    self_address,
                }),
                request,
            );
        }
        let Some(submission) = self.submission.clone() else {
            let err = super::error::unsupported_error(
                AccountOperation::Send,
                None,
                "JMAP submission capability not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // Route through the resolved `Submission`-capability account
        // handle, not `self.mail`. They are usually the same accountId,
        // but `primary_account::<Submission>()` can resolve a distinct
        // account; the draft `Email/set` and the `EmailSubmission/set`
        // must both target the submission account.
        let account_id = submission.id_str().to_string();
        pim::send_message(
            submission,
            Arc::clone(&self.email_states),
            account_id,
            self.max_delayed_send,
            None,
            request,
        )
    }

    fn send_raw_message(
        &self,
        raw: bytes::Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let Some(submission) = self.submission.clone() else {
            let err = super::error::unsupported_error(
                AccountOperation::Send,
                None,
                "JMAP submission capability not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // Same submission-account routing as `send_message`: the import +
        // EmailSubmission must both target the submission-capable account.
        let account_id = submission.id_str().to_string();
        pim::send_raw_message(
            submission,
            Arc::clone(&self.email_states),
            account_id,
            raw,
            save_to_sent,
        )
    }

    fn attachment_upload(
        &self,
        bytes: AccountStream<Result<bytes::Bytes, AccountError>>,
        mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        upload_draft_attachment(&self.mail, self.submission.as_ref(), bytes, mime)
    }

    fn host_attachment(
        &self,
        _bytes: bytes::Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::HostAttachment,
            None,
            "JMAP has no cloud-drive attachment hosting",
        );
        Box::pin(async move { Err(err) })
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        let draft_account = self.draft_account().clone();
        let account_id = draft_account.id_str().to_string();
        pim::draft_create(
            draft_account,
            Arc::clone(&self.email_states),
            account_id,
            patch,
        )
    }

    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let draft_account = self.draft_account().clone();
        let account_id = draft_account.id_str().to_string();
        pim::draft_update(
            draft_account,
            Arc::clone(&self.email_states),
            account_id,
            draft,
            patch,
        )
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        let draft_account = self.draft_account().clone();
        let account_id = draft_account.id_str().to_string();
        pim::draft_discard(
            draft_account,
            Arc::clone(&self.email_states),
            account_id,
            draft,
        )
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        let Some(submission) = self.submission.clone() else {
            let err = super::error::unsupported_error(
                AccountOperation::DraftSend,
                None,
                "JMAP submission capability not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // The `EmailSubmission/set` and the Sent-folder move both target
        // the resolved submission account, not necessarily `self.mail`.
        let account_id = submission.id_str().to_string();
        pim::draft_send(
            submission,
            Arc::clone(&self.email_states),
            account_id,
            draft,
        )
    }

    fn cancel_scheduled_send(&self, handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        let cancellable = self
            .submission
            .clone()
            .filter(|_| self.max_delayed_send != 0);
        let Some(submission) = cancellable else {
            let err = super::error::unsupported_error(
                AccountOperation::CancelScheduledSend,
                None,
                "JMAP scheduled send not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // The submission id was minted on the `Submission` account; the
        // `EmailSubmission/set` cancel must go to that account.
        pim::cancel_scheduled_send(submission, handle)
    }

    fn reschedule_send(
        &self,
        handle: ObjectId,
        scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let reschedulable = self
            .submission
            .clone()
            .filter(|_| self.max_delayed_send != 0);
        let Some(submission) = reschedulable else {
            let err = super::error::unsupported_error(
                AccountOperation::RescheduleSend,
                None,
                "JMAP scheduled send not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // The submission id was minted on the `Submission` account; the
        // cancel + recreate must both target that account.
        pim::reschedule_send(submission, self.max_delayed_send, handle, scheduled)
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        pim::search(self.mail.clone(), Arc::clone(&self.foreign_mail), request)
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        pim::search_messages(self.mail.clone(), Arc::clone(&self.foreign_mail), request)
    }

    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>> {
        pim::containers_list(self.mail.clone(), Arc::clone(&self.foreign_mail))
    }

    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        pim::container_create(
            self.mail.clone(),
            Arc::clone(&self.mailbox_states),
            self.mail.id_str().to_string(),
            kind,
            name,
            parent,
            style,
        )
    }

    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_rename(
            self.mail.clone(),
            Arc::clone(&self.mailbox_states),
            self.mail.id_str().to_string(),
            container,
            name,
            style,
        )
    }

    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_move(
            self.mail.clone(),
            Arc::clone(&self.mailbox_states),
            self.mail.id_str().to_string(),
            container,
            new_parent,
        )
    }

    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        pim::container_delete(
            self.mail.clone(),
            Arc::clone(&self.mailbox_states),
            self.mail.id_str().to_string(),
            container,
        )
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        pim::identities_list(self.submission.clone())
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::identity_update(self.submission.clone(), identity, patch)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        pim::vacation_get(self.vacation.clone())
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        pim::vacation_set(self.vacation.clone(), config)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        pim::quota_get(self.quota.clone())
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        filters::list(self.sieve.clone())
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        filters::create(self.sieve.clone(), filter)
    }

    fn filter_update(
        &self,
        filter: ServerFilterId,
        patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        filters::update(self.sieve.clone(), filter, patch)
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        filters::delete(self.sieve.clone(), filter)
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        filters::validate(self.sieve.clone(), filter)
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        contacts::address_books_list(self.contacts.clone())
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::list(self.contacts.clone(), address_book, page_cursor)
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        contacts::get(self.contacts.clone(), contact)
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        contacts::create(self.contacts.clone(), contact)
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        contacts::update(self.contacts.clone(), contact, patch)
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        contacts::delete(self.contacts.clone(), contact)
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::search(self.contacts.clone(), request)
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::DirectorySearch,
            None,
            "JMAP has no organization-directory concept",
        );
        Box::pin(async move { Err(err) })
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::DirectoryGroupsList,
            None,
            "JMAP has no organization-directory concept",
        );
        Box::pin(async move { Err(err) })
    }

    fn directory_group_expand(
        &self,
        _group: DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::DirectoryGroupExpand,
            None,
            "JMAP has no organization-directory concept",
        );
        Box::pin(async move { Err(err) })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        calendar_ops::calendars_list(self.calendars.clone())
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar_ops::events_in_range(self.calendars.clone(), range)
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        calendar_ops::get(self.calendars.clone(), event)
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        calendar_ops::create(self.calendars.clone(), event)
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::update(self.calendars.clone(), event, patch)
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::delete(self.calendars.clone(), event)
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::rsvp(
            self.calendars.clone(),
            self.self_emails.clone(),
            event,
            status,
        )
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar_ops::search(self.calendars.clone(), request)
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        pim::thread_hydrate(self.mail.clone(), Arc::clone(&self.foreign_mail), thread)
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        pim::message_hydrate(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            message,
            projection,
        )
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: ContainerId,
        source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_thread(&thread);
        pim::move_thread(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            thread,
            target,
            source,
        )
    }

    fn apply_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        match label.provenance.kind {
            ContainerKind::Label => self.set_keyword(target, label.provenance.native, true),
            ContainerKind::Folder => self.add_to_container(target, label.id),
            _ => {
                let err = super::error::unsupported_error(
                    AccountOperation::SetLabelMembership,
                    None,
                    "JMAP does not support this label kind",
                );
                Box::pin(async move { Err(err) })
            }
        }
    }

    fn remove_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        match label.provenance.kind {
            ContainerKind::Label => self.set_keyword(target, label.provenance.native, false),
            ContainerKind::Folder => self.remove_from_container(target, label.id),
            _ => {
                let err = super::error::unsupported_error(
                    AccountOperation::SetLabelMembership,
                    None,
                    "JMAP does not support this label kind",
                );
                Box::pin(async move { Err(err) })
            }
        }
    }

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_thread(&thread);
        pim::delete_thread(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            thread,
            current,
        )
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        let closed = Arc::clone(&self.closed);
        let close_completion = Arc::clone(&self.close_completion);
        let client = self.client.clone();
        let shutdown = self.shutdown.clone();
        let reader = self.ws.reader_handle();
        let subscriptions = Arc::clone(&self.subscriptions);
        let enabled = Arc::clone(&self.ws.enabled);
        let push_state = Arc::clone(&self.ws.push_state);
        // The same bound the reader applies to its own setup awaits. Both
        // steps below are WebSocket sink writes or a task join, and a
        // half-open socket parks either indefinitely - which would block
        // the engine's teardown and reopen on a connection that is already
        // gone. Teardown must be bounded on exactly the paths that motivated
        // bounding the reader.
        let teardown_timeout = self.ws.teardown_timeout();

        close_once(close_completion, move || {
            // This transition must precede the push locks: a subscribe
            // already holding them may finish and then be disabled below,
            // while later subscribers observe the closed gate.
            closed.store(true, Ordering::Release);
            shutdown.cancel();
            async move {
                let result = match tokio::time::timeout(
                    teardown_timeout,
                    super::push::disable_for_close(client, subscriptions, enabled, push_state),
                )
                .await
                {
                    Ok(result) => result,
                    // The unsubscribe never made it out. The connection is
                    // being dropped anyway and the server drops the
                    // subscription with it, so this is not a close failure.
                    Err(_) => Ok(()),
                };
                // The task is joined before this shared completion resolves,
                // so every close caller observes the reader's stop.
                super::push::join_reader(&reader, teardown_timeout).await;
                close_outcome(result)
            }
        })
    }
}

/// What `close()` reports for its best-effort push disable.
///
/// Three outcomes are not close failures, for one reason: RFC 8887 push is
/// enabled per connection, so a subscription cannot outlive its
/// connection. No connection ever made (`WebSocketNotConnected`) has no
/// subscription. A sink write that failed (`WebSocketSend`) found the
/// connection finished - every failed write leaves the stream closed for
/// good - so its subscription is already gone; this used to fail `close()`
/// as a `Protocol(PartialResponse)` after every WebSocket drop, for a
/// teardown that had nothing left to tear down. The timeout arm in
/// `close()` is the same argument for a wedged sink.
fn close_outcome(result: crate::Result<()>) -> Result<(), AccountError> {
    match result {
        Ok(()) | Err(crate::Error::WebSocketNotConnected | crate::Error::WebSocketSend(_)) => {
            Ok(())
        }
        Err(err) => Err(super::error::into_account_error(
            err,
            super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Close),
        )),
    }
}

/// One `Mailbox(accountId)` owner tag per distinct foreign account among
/// the given scopes. Pure, so discovery's owner-tag emission is
/// unit-testable without a live account.
fn foreign_owner_memberships_from_scopes<'a, I>(scopes: I) -> Vec<MembershipScope>
where
    I: IntoIterator<Item = &'a CursorScope>,
{
    let mut owners: Vec<String> = scopes
        .into_iter()
        .filter_map(|scope| match scope {
            CursorScope::Folder(folder) => {
                foreign::parse_foreign(folder).map(|parsed| parsed.account_id)
            }
            _ => None,
        })
        .collect();
    owners.sort();
    owners.dedup();
    owners
        .into_iter()
        .map(|id| foreign::owner_tag(&id))
        .collect()
}

/// The foreign JMAP `accountId` a scope routes to, or `None` for a
/// primary scope. Only a `CursorScope::Folder` whose `FolderId` parses
/// as a foreign mailbox AND whose account is registered (per
/// `is_registered`) routes foreign; every other scope is primary. Pure,
/// so the routing decision is unit-testable without a live `Client`.
fn resolve_foreign_account_id<F>(scope: &CursorScope, is_registered: F) -> Option<String>
where
    F: Fn(&str) -> bool,
{
    if let CursorScope::Folder(folder) = scope
        && let Some(parsed) = foreign::parse_foreign(folder)
        && is_registered(&parsed.account_id)
    {
        Some(parsed.account_id)
    } else {
        None
    }
}

/// True when `scope` is a foreign-mailbox `Folder` scope whose account is
/// NOT registered. Pure counterpart to `resolve_foreign_account_id`:
/// where that returns `None` for both "primary scope" and "foreign but
/// unregistered", this isolates the second case so callers can route it
/// to a terminal error instead of misrouting it to the primary account.
fn is_unregistered_foreign<F>(scope: &CursorScope, is_registered: F) -> bool
where
    F: Fn(&str) -> bool,
{
    match scope {
        CursorScope::Folder(folder) => {
            foreign::parse_foreign(folder).is_some_and(|parsed| !is_registered(&parsed.account_id))
        }
        _ => false,
    }
}

/// Pure send-as routing decision. Returns `Ok(())` when the request may be
/// dispatched to the foreign account named by `send_as`, or the boundary
/// rejection otherwise. Precedence: the feature being absent outright is
/// answered first; then scheduled foreign sends (their bare submission handles
/// cannot be safely cancelled or rescheduled through the primary account); then
/// an id absent from the seeded routing table, which is a malformed request
/// (the consumer got it from foreign membership ownership); then a
/// known-but-not-submission-capable id, which is unsupported.
///
/// `feature_available` is `pim_methods.send_as`, i.e. whether ANY seeded
/// foreign account advertises submission. It has to be consulted here because
/// `SendRequest::send_as` makes the capability flag decide between the two
/// rejection classes, and without it an account that advertises `send_as ==
/// false` still answered `Request(Malformed)` - telling a consumer to correct
/// an argument when no argument would have worked.
///
/// This cannot refuse a request that previously routed: anything that reached
/// `Ok(())` had its id in `foreign_submission`, which makes that set non-empty,
/// which is exactly the condition that makes `feature_available` true.
fn route_send_as(
    send_as: &SendAs,
    feature_available: bool,
    scheduled: bool,
    known: bool,
    submission_capable: bool,
) -> Result<(), AccountError> {
    if !feature_available {
        return Err(super::error::unsupported_error(
            AccountOperation::Send,
            None,
            "no seeded foreign account advertises submission, \
             so send-as is unavailable on this account",
        ));
    }
    if scheduled {
        return Err(super::error::unsupported_error(
            AccountOperation::Send,
            None,
            "scheduled foreign send is not supported",
        ));
    }
    if !known {
        return Err(super::error::send_as_unknown_account(send_as.mailbox()));
    }
    if !submission_capable {
        return Err(super::error::unsupported_error(
            AccountOperation::Send,
            None,
            "foreign account does not advertise submission",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use bifrost_types::{
        AccountError, AccountErrorKind, AccountOperation, CursorScope, MailboxId, MembershipScope,
        ObjectId, ObjectType, QueryId, SendAs,
    };
    use tokio_util::sync::CancellationToken;

    use super::super::{foreign, state};
    use super::{
        cursor_scopes_from_seeds, describe_cursor_support, draft_route_account,
        foreign_account_id_for_object, foreign_owner_memberships_from_scopes,
        fresh_cursor_establishment, is_unregistered_foreign, partitioning_for_scope,
        resolve_foreign_account_id, route_mutation_target, route_object_id, route_send_as,
        unseeded_scope_error,
    };
    use bifrost_types::{ChangeCursor, CostClass, MutationTarget, SyncStrategy, ThreadId};

    fn folder_scope(account_id: &str) -> CursorScope {
        CursorScope::Folder(foreign::encode_foreign_account(account_id))
    }

    /// A close after a WebSocket drop finds the sink dead. The push
    /// subscription died with that connection, so the disable that could
    /// not be written is not a close failure. Anything else still is.
    #[test]
    fn a_dead_websocket_sink_is_not_a_close_failure() {
        super::close_outcome(Err(crate::Error::WebSocketSend(
            tokio_websockets::Error::AlreadyClosed,
        )))
        .expect("a refused frame on a closed connection");
        super::close_outcome(Err(crate::Error::WebSocketSend(
            tokio_websockets::Error::Io(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        )))
        .expect("a write that failed mid-flush closes the connection too");
        super::close_outcome(Err(crate::Error::WebSocketNotConnected))
            .expect("no connection, no subscription");

        let error = super::close_outcome(Err(crate::Error::NotParsable("frame".to_string())))
            .expect_err("an unrelated failure is still reported");
        assert_eq!(error.operation(), Some(AccountOperation::Close));
    }

    /// Close starts only when a caller polls it. Once started, the spawned
    /// teardown outlives that caller and later close calls join its result.
    #[tokio::test]
    async fn close_starts_on_poll_and_survives_caller_cancellation() {
        let completion = Arc::new(std::sync::OnceLock::new());
        let starts = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();

        let unpolled_starts = Arc::clone(&starts);
        let unpolled_closed = Arc::clone(&closed);
        let unpolled_shutdown = shutdown.clone();
        drop(super::close_once(Arc::clone(&completion), move || {
            unpolled_starts.fetch_add(1, Ordering::Relaxed);
            unpolled_closed.store(true, Ordering::Release);
            unpolled_shutdown.cancel();
            std::future::ready(Ok::<(), AccountError>(()))
        }));
        assert_eq!(starts.load(Ordering::Relaxed), 0);
        assert!(!closed.load(Ordering::Acquire));
        assert!(!shutdown.is_cancelled());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let first_starts = Arc::clone(&starts);
        let first_closed = Arc::clone(&closed);
        let first_shutdown = shutdown.clone();
        let mut first = super::close_once(Arc::clone(&completion), move || {
            first_starts.fetch_add(1, Ordering::Relaxed);
            first_closed.store(true, Ordering::Release);
            first_shutdown.cancel();
            async move {
                let _ = started_tx.send(());
                release_rx.await.expect("teardown release");
                Ok::<(), AccountError>(())
            }
        });
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(closed.load(Ordering::Acquire));
        assert!(shutdown.is_cancelled());
        started_rx.await.expect("teardown was spawned");
        drop(first);

        let unexpected_starts = Arc::new(AtomicUsize::new(0));
        let second_starts = Arc::clone(&unexpected_starts);
        let mut second = super::close_once(completion, move || {
            second_starts.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok::<(), AccountError>(()))
        });
        assert!(futures::poll!(second.as_mut()).is_pending());
        release_tx.send(()).expect("teardown still waits");
        second.await.expect("shared teardown completes");

        assert_eq!(starts.load(Ordering::Relaxed), 1);
        assert_eq!(unexpected_starts.load(Ordering::Relaxed), 0);
    }

    /// Discovery order. The `Type` scopes lead in a fixed order and only
    /// when seeded; foreign `Folder` scopes follow, sorted.
    #[test]
    fn cursor_scopes_lead_with_seeded_type_scopes_then_sorted_foreign() {
        let seeded = [
            folder_scope("zeta"),
            CursorScope::Type(ObjectType::Mailbox),
            folder_scope("alpha"),
            CursorScope::Type(ObjectType::Email),
        ];

        assert_eq!(
            cursor_scopes_from_seeds(seeded.iter()),
            vec![
                CursorScope::Type(ObjectType::Email),
                CursorScope::Type(ObjectType::Mailbox),
                folder_scope("alpha"),
                folder_scope("zeta"),
            ]
        );
    }

    /// `seed_states` is a `HashMap`, so an unsorted foreign lane would
    /// hand the engine a different scope ORDER on every run. Feeding the
    /// same set in the opposite sequence must produce the same answer.
    #[test]
    fn cursor_scope_order_does_not_depend_on_seed_iteration_order() {
        let forward = vec![folder_scope("a"), folder_scope("b"), folder_scope("c")];
        let mut reversed = forward.clone();
        reversed.reverse();

        assert_eq!(
            cursor_scopes_from_seeds(forward.iter()),
            cursor_scopes_from_seeds(reversed.iter())
        );
    }

    /// An unseeded `Type` scope must not appear. Emitting it would have
    /// the engine drive a scope whose `establish_initial_cursor` then
    /// reports `Unsupported`.
    #[test]
    fn an_unseeded_type_scope_is_not_discovered() {
        let seeded = [CursorScope::Type(ObjectType::Email)];

        assert_eq!(
            cursor_scopes_from_seeds(seeded.iter()),
            vec![CursorScope::Type(ObjectType::Email)],
            "Mailbox was never seeded, so it must not be discovered"
        );
        assert!(cursor_scopes_from_seeds(std::iter::empty()).is_empty());
    }

    /// A full inventory is one anchored walk. Splitting it into positional
    /// page partitions would lose the anchor at every partition boundary.
    #[test]
    fn inventory_is_not_split_across_unstable_positional_boundaries() {
        for scope in [
            CursorScope::Type(ObjectType::Email),
            CursorScope::Type(ObjectType::Mailbox),
            folder_scope("shared"),
            CursorScope::Account,
        ] {
            assert!(
                matches!(
                    partitioning_for_scope(&scope, 42),
                    bifrost_types::InventoryPartitioning::Full
                ),
                "{scope:?} enumerates in one pass"
            );
        }
    }

    #[derive(Clone)]
    struct CursorProbeTransport {
        calls: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl crate::core::transport::HttpTransport for CursorProbeTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value = serde_json::from_slice(&body).expect("request JSON");
            let call = request["methodCalls"][0].clone();
            self.calls.lock().expect("calls").push(call.clone());
            let method = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            let account = call[1]["accountId"].as_str().expect("account id");
            let state = match method.as_str() {
                "Email/get" => format!("fresh-email-{account}"),
                "Mailbox/get" => format!("fresh-mailbox-{account}"),
                other => panic!("unexpected method {other}"),
            };
            let response = serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[method, {
                    "accountId": account,
                    "state": state,
                    "list": [],
                    "notFound": []
                }, call_id]]
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

    fn cursor_probe_session() -> crate::core::session::Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 2,
                    "maxObjectsInSet": 2,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                "shared": {"name": "Shared", "isPersonal": false, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses")
    }

    #[tokio::test]
    async fn cursor_establishment_probes_fresh_state_for_each_seeded_scope() {
        let transport = CursorProbeTransport {
            calls: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let client = crate::client::Client::with_transport(
            transport.clone(),
            cursor_probe_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let email_states: super::super::state_cache::StateMap =
            Arc::new(tokio::sync::Mutex::new(HashMap::from([
                ("primary".to_string(), Some("open-email".to_string())),
                ("shared".to_string(), Some("open-shared-email".to_string())),
            ])));
        let mailbox_states: super::super::state_cache::StateMap =
            Arc::new(tokio::sync::Mutex::new(HashMap::from([(
                "primary".to_string(),
                Some("open-mailbox".to_string()),
            )])));
        let cases = [
            (
                crate::account::Account::new(client.clone(), "primary"),
                CursorScope::Type(ObjectType::Email),
                "fresh-email-primary",
                "primary",
            ),
            (
                crate::account::Account::new(client.clone(), "primary"),
                CursorScope::Type(ObjectType::Mailbox),
                "fresh-mailbox-primary",
                "primary",
            ),
            (
                crate::account::Account::new(client, "shared"),
                folder_scope("shared"),
                "fresh-email-shared",
                "shared",
            ),
        ];

        for (mail, scope, expected_state, account_id) in cases {
            let established = fresh_cursor_establishment(
                mail,
                scope.clone(),
                Arc::clone(&email_states),
                Arc::clone(&mailbox_states),
            )
            .await
            .expect("a seeded cursor is re-probed");
            let bifrost_types::CursorEstablishment::Ready(cursor) = established else {
                panic!("seeded cursor is Ready");
            };
            assert_eq!(cursor.scope, scope);
            let (_, state_string) = state::decode_cursor(&cursor).expect("cursor decodes");
            assert_eq!(state_string, expected_state);
            assert_eq!(cursor.advanced_through, None);
            assert_eq!(
                cursor.envelope_version,
                state::OUTER_CURSOR_ENVELOPE_VERSION
            );
            let cached = if matches!(scope, CursorScope::Type(ObjectType::Mailbox)) {
                super::super::state_cache::get(&mailbox_states, account_id).await
            } else {
                super::super::state_cache::get(&email_states, account_id).await
            };
            assert_eq!(cached.as_deref(), Some(expected_state));
        }

        let calls = transport.calls.lock().expect("calls").clone();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0][0], "Email/get");
        assert_eq!(calls[0][1]["ids"], serde_json::json!([]));
        assert_eq!(calls[1][0], "Mailbox/get");
        assert_eq!(calls[1][1]["ids"], serde_json::json!([]));
        assert_eq!(calls[2][0], "Email/get");
        assert_eq!(calls[2][1]["accountId"], "shared");
    }

    /// An unseeded scope is `Unsupported`, NOT a cursor over an empty
    /// state - the latter would present as a working cursor that returns
    /// nothing forever. The error must carry the cursor scope so the
    /// engine can route it.
    #[test]
    fn an_unseeded_scope_is_unsupported_and_names_its_scope() {
        let scope = CursorScope::Type(ObjectType::Mailbox);

        let error = unseeded_scope_error(scope.clone());
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::EstablishCursor)
        ));
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Cursor(scope)),
            "the engine routes the failure by cursor scope"
        );
    }

    /// The handle-selection boundary every thread- and message-keyed
    /// door delegates to. `JmapAccount` hardwires `ReqwestTransport`,
    /// so this is pinned over string handles: a regression that sends
    /// a foreign target down the primary route fails HERE, on the
    /// selected handle itself.
    #[test]
    fn object_and_thread_targets_route_to_the_owning_handle() {
        let primary = "PRIMARY".to_string();
        let foreign_map =
            std::collections::HashMap::from([("shared".to_string(), "SHARED".to_string())]);

        let foreign_thread = ThreadId(foreign::encode_object("shared", "T1"));
        let foreign_message = ObjectId(foreign::encode_object("shared", "M1"));
        let departed_thread = ThreadId(foreign::encode_object("gone", "T1"));

        // The raw selector, both namespaces.
        assert_eq!(
            route_object_id(&foreign_thread.0, &primary, &foreign_map),
            "SHARED"
        );
        assert_eq!(route_object_id("T1", &primary, &foreign_map), "PRIMARY");
        assert_eq!(
            route_object_id(&departed_thread.0, &primary, &foreign_map),
            "PRIMARY",
            "a departed share's id stays literal on the primary route"
        );

        // The MutationTarget dispatch the flag/read/importance doors use.
        assert_eq!(
            route_mutation_target(
                &MutationTarget::Thread(foreign_thread),
                &primary,
                &foreign_map
            ),
            "SHARED"
        );
        assert_eq!(
            route_mutation_target(
                &MutationTarget::Message(foreign_message),
                &primary,
                &foreign_map
            ),
            "SHARED"
        );
        assert_eq!(
            route_mutation_target(
                &MutationTarget::Thread(ThreadId("bare".into())),
                &primary,
                &foreign_map
            ),
            "PRIMARY"
        );
    }

    #[test]
    fn draft_lifecycle_uses_submission_namespace_when_available() {
        let mail = "MAIL".to_string();
        let submission = "SUBMISSION".to_string();

        assert_eq!(draft_route_account(&mail, Some(&submission)), &submission);
        assert_eq!(draft_route_account(&mail, None), &mail);
    }

    #[derive(Clone)]
    struct DraftUploadTransport {
        expected_account: &'static str,
    }

    impl crate::core::transport::HttpTransport for DraftUploadTransport {
        async fn api_request(
            &self,
            _url: &str,
            _body: Vec<u8>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            panic!("attachment upload needs no API request");
        }

        async fn upload(
            &self,
            url: &str,
            body: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            assert_eq!(
                url,
                format!("https://example.test/upload/{}", self.expected_account)
            );
            assert_eq!(body, b"attachment bytes");
            assert_eq!(content_type, Some("text/plain"));
            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "accountId": self.expected_account,
                    "blobId": "uploaded-blob",
                    "type": "text/plain",
                    "size": body.len()
                })
                .to_string(),
            ))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            panic!("attachment upload needs no download");
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            panic!("session is supplied in memory");
        }
    }

    #[tokio::test]
    async fn draft_attachment_upload_uses_draft_namespace_with_mail_fallback() {
        for (has_submission, expected_account) in [(true, "submission"), (false, "mail")] {
            let session = serde_json::from_value(serde_json::json!({
                "capabilities": {},
                "accounts": {},
                "primaryAccounts": {},
                "username": "user@example.test",
                "apiUrl": "https://example.test/api",
                "uploadUrl": "https://example.test/upload/{accountId}",
                "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
                "eventSourceUrl": "https://example.test/events",
                "state": "session-1"
            }))
            .expect("session parses");
            let client = crate::client::Client::with_transport(
                DraftUploadTransport { expected_account },
                session,
                "https://example.test/session",
            )
            .expect("client builds");
            let mail = crate::account::Account::new(client.clone(), "mail");
            let submission = crate::account::Account::new(client, "submission");
            let selected = has_submission.then_some(&submission);
            let stream = Box::pin(futures::stream::iter(vec![Ok(bytes::Bytes::from_static(
                b"attachment bytes",
            ))]));
            let handle =
                super::upload_draft_attachment(&mail, selected, stream, "text/plain".to_string())
                    .await
                    .expect("upload succeeds in draft account");
            assert!(!handle.0.is_empty());
            assert_eq!(
                draft_route_account(&mail, selected).id_str(),
                expected_account
            );
        }
    }

    fn as_shared() -> SendAs {
        SendAs::As(MailboxId("foreign-id".to_string()))
    }

    #[test]
    fn route_send_as_known_submission_capable_ok() {
        assert!(route_send_as(&as_shared(), true, false, true, true).is_ok());
    }

    #[test]
    fn route_send_as_scheduled_is_unsupported() {
        let err =
            route_send_as(&as_shared(), true, true, true, true).expect_err("scheduled rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    #[test]
    fn route_send_as_unknown_account_is_malformed() {
        let err = route_send_as(&as_shared(), true, false, false, false)
            .expect_err("unknown id rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn route_send_as_known_not_submission_capable_is_unsupported() {
        let err = route_send_as(&as_shared(), true, false, true, false)
            .expect_err("no submission rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    /// With the feature absent, an unknown id is `Unsupported`, NOT the
    /// `Malformed` the same inputs produce when it is present.
    ///
    /// This is the capability flag selecting the rejection class, which is the
    /// rule `SendRequest::send_as` states and which this function did not read
    /// until 2026-09-15: an account advertising `send_as == false` answered
    /// `Request(Malformed)`, telling a consumer to correct an argument when no
    /// argument could have worked. The contrast with
    /// `route_send_as_unknown_account_is_malformed` - identical but for this
    /// one bool - is the whole assertion.
    #[test]
    fn route_send_as_unknown_account_is_unsupported_when_the_feature_is_absent() {
        let err = route_send_as(&as_shared(), false, false, false, false)
            .expect_err("send-as is unavailable");
        assert!(
            matches!(
                err.kind(),
                AccountErrorKind::Unsupported(AccountOperation::Send)
            ),
            "got {:?}",
            err.kind()
        );
    }

    fn cursor(scope: CursorScope) -> ChangeCursor {
        state::cursor_for_scope(scope, "state-1").expect("scope encodes")
    }

    #[test]
    fn supported_scopes_describe_a_server_cursor() {
        for scope in [
            CursorScope::Type(ObjectType::Email),
            CursorScope::Type(ObjectType::Mailbox),
            CursorScope::Folder(foreign::encode_foreign("acct-9", "mbx-1")),
        ] {
            let descriptor = describe_cursor_support(&cursor(scope.clone()));
            assert_eq!(descriptor.cost_class, CostClass::Cheap, "{scope:?}");
            assert_eq!(descriptor.strategy, SyncStrategy::ServerCursor, "{scope:?}");
        }
    }

    #[test]
    fn legacy_thread_and_query_cursors_are_not_advertised_as_usable() {
        // Both decode (their scope tags are unchanged by the envelope
        // version bump, so old durable rows stay readable) but
        // `changes::stream` terminates them Unsupported(SyncChanges), so
        // the descriptor must not promise a strategy that dies on its
        // first poll. Strategy `None` routes the consumer back through
        // inventory instead.
        for scope in [
            CursorScope::Type(ObjectType::Thread),
            CursorScope::Query(QueryId("unread-in-inbox".to_string())),
        ] {
            let legacy = cursor(scope.clone());
            assert!(
                state::decode_cursor(&legacy).is_ok(),
                "premise: the legacy cursor still decodes ({scope:?})"
            );
            let descriptor = describe_cursor_support(&legacy);
            assert_eq!(descriptor.cost_class, CostClass::Expensive, "{scope:?}");
            assert_eq!(descriptor.strategy, SyncStrategy::None, "{scope:?}");
        }
    }

    #[test]
    fn an_undecodable_cursor_describes_no_strategy() {
        let mut broken = cursor(CursorScope::Type(ObjectType::Email));
        broken.server_state.bytes.pop();
        let descriptor = describe_cursor_support(&broken);
        assert_eq!(descriptor.cost_class, CostClass::Expensive);
        assert_eq!(descriptor.strategy, SyncStrategy::None);
    }

    #[test]
    fn discover_emits_foreign_account_owner_membership() {
        // The seeded shape is one account-level scope per share; a legacy
        // per-mailbox scope for the same account still contributes the
        // same (deduped) owner tag.
        let scopes = [
            CursorScope::Type(ObjectType::Email),
            CursorScope::Folder(foreign::encode_foreign_account("acct-9")),
            CursorScope::Folder(foreign::encode_foreign("acct-9", "mbx-2")),
            CursorScope::Folder(foreign::encode_foreign_account("acct-7")),
        ];
        let owners = foreign_owner_memberships_from_scopes(scopes.iter());
        assert!(owners.contains(&MembershipScope::Mailbox(MailboxId("acct-9".to_string()))));
        assert!(owners.contains(&MembershipScope::Mailbox(MailboxId("acct-7".to_string()))));
        // One owner tag per account, not per mailbox.
        assert_eq!(owners.len(), 2);
    }

    #[test]
    fn mail_for_scope_routes_foreign_account() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |id: &str| registered.contains(id);

        // The seeded account-level Folder scope routes foreign.
        let account_scope = CursorScope::Folder(foreign::encode_foreign_account("acct-9"));
        assert_eq!(
            resolve_foreign_account_id(&account_scope, is_registered),
            Some("acct-9".to_string())
        );

        // So does a legacy per-mailbox Folder scope.
        let foreign_scope = CursorScope::Folder(foreign::encode_foreign("acct-9", "mbx-3"));
        assert_eq!(
            resolve_foreign_account_id(&foreign_scope, is_registered),
            Some("acct-9".to_string())
        );

        // A foreign Folder scope for an UNregistered account resolves to
        // no handle (None). It is NOT silently routed to the primary
        // account - `is_unregistered_foreign` flags it so callers emit a
        // terminal error (see `unregistered_foreign_scope_is_flagged_not_misrouted`).
        let unknown_scope = CursorScope::Folder(foreign::encode_foreign("acct-other", "mbx-3"));
        assert_eq!(
            resolve_foreign_account_id(&unknown_scope, is_registered),
            None
        );

        // Primary scopes never route foreign.
        assert_eq!(
            resolve_foreign_account_id(&CursorScope::Type(ObjectType::Email), is_registered),
            None
        );
        assert_eq!(
            resolve_foreign_account_id(
                &CursorScope::Query(QueryId("q1".to_string())),
                is_registered
            ),
            None
        );
    }

    #[test]
    fn message_mutations_select_only_registered_foreign_accounts() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |id: &str| registered.contains(id);

        assert_eq!(
            foreign_account_id_for_object(
                &ObjectId(foreign::encode_object("acct-9", "M1")),
                is_registered,
            ),
            Some("acct-9".to_string())
        );
        assert_eq!(
            foreign_account_id_for_object(
                &ObjectId(foreign::encode_object("acct-gone", "M1")),
                is_registered,
            ),
            None,
            "an unreachable account stays on the primary route for a real server miss"
        );
        assert_eq!(
            foreign_account_id_for_object(&ObjectId("M1".to_string()), is_registered),
            None
        );
    }

    #[test]
    fn unregistered_foreign_scope_is_flagged_not_misrouted() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |id: &str| registered.contains(id);

        // A seeded-then-unregistered foreign scope: routing returns None
        // (would fall back to primary), but the misroute guard flags it
        // so the caller terminates instead of running it against the
        // primary mailbox.
        let gone = CursorScope::Folder(foreign::encode_foreign("acct-gone", "inbox"));
        assert_eq!(resolve_foreign_account_id(&gone, is_registered), None);
        assert!(is_unregistered_foreign(&gone, is_registered));

        // A registered foreign scope is NOT flagged (routes foreign).
        let live = CursorScope::Folder(foreign::encode_foreign("acct-9", "inbox"));
        assert!(!is_unregistered_foreign(&live, is_registered));

        // Primary scopes are never flagged.
        assert!(!is_unregistered_foreign(
            &CursorScope::Type(ObjectType::Email),
            is_registered
        ));
    }
}
