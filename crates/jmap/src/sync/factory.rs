use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bifrost_net::{StaticTokenSource, TokenSource};
use bifrost_types::{
    Account, AccountError, AccountFactory, AccountFuture, AccountId, CursorScope, ObjectType,
    OpenedAccount, SkippedScope,
};
use futures::stream::StreamExt;

use tokio_util::sync::CancellationToken;

use crate::account::Account as JmapMailAccount;
use crate::client::{Client, Credentials};
use crate::core::capability;
use crate::core::capability::Capability;
use crate::core::id::AccountId as JmapAccountId;
use crate::core::transport::HttpTransport;
use crate::email::{EmailGet, EmailId};
use crate::mailbox::{MailboxGet, Property as MailboxProperty};
use crate::principal::PrincipalGet;

use super::account::JmapAccount;
use super::capabilities;
use super::foreign;
use super::push::{PushRouting, ReconnectPolicy, WsState};
use super::state;

// pub: re-exported through crate::sync for engine AccountFactory registration.
#[derive(Debug, Clone)]
pub struct JmapAccountFactory {
    config: JmapAccountFactoryBuilder,
}

// pub: returned by JmapAccountFactory::builder for open-time configuration.
#[derive(Debug, Clone)]
pub struct JmapAccountFactoryBuilder {
    url: String,
    credentials: JmapCredentials,
    timeout: Option<Duration>,
    accept_invalid_certs: bool,
    reconnect_policy: ReconnectPolicy,
}

// pub: caller-supplied auth material for constructing a JMAP AccountFactory.
#[derive(Clone)]
#[non_exhaustive]
pub enum JmapCredentials {
    Basic { username: String, password: String },
    // The bearer source is shared: ratatoskr hands in one
    // `Arc<dyn TokenSource>` (typically an `OAuthRefresher` over its own
    // refresh-token store) and a token it refreshes and persists is read
    // live at every wire authentication without reopening the account.
    Bearer { token_source: Arc<dyn TokenSource> },
}

// `Arc<dyn TokenSource>` is not `Debug`; redact the bearer source so the
// derived `Debug` on the surrounding factory structs keeps working
// without leaking token material.
impl std::fmt::Debug for JmapCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Bearer { .. } => f
                .debug_struct("Bearer")
                .field("token_source", &"<token-source>")
                .finish(),
        }
    }
}

impl JmapAccountFactory {
    pub fn new(url: impl Into<String>, credentials: JmapCredentials) -> Self {
        Self {
            config: JmapAccountFactoryBuilder::new(url, credentials),
        }
    }

    pub fn builder(
        url: impl Into<String>,
        credentials: JmapCredentials,
    ) -> JmapAccountFactoryBuilder {
        JmapAccountFactoryBuilder::new(url, credentials)
    }
}

impl JmapAccountFactoryBuilder {
    pub fn new(url: impl Into<String>, credentials: JmapCredentials) -> Self {
        Self {
            url: url.into(),
            credentials,
            timeout: None,
            accept_invalid_certs: false,
            reconnect_policy: ReconnectPolicy::default(),
        }
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[must_use]
    pub fn accept_invalid_certs(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    #[must_use]
    pub fn reconnect_policy(mut self, reconnect_policy: ReconnectPolicy) -> Self {
        self.reconnect_policy = reconnect_policy;
        self
    }

    #[must_use]
    pub fn build(self) -> JmapAccountFactory {
        JmapAccountFactory { config: self }
    }
}

impl AccountFactory for JmapAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let config = self.config.clone();
        Box::pin(async move {
            let client = connect(config.clone(), account_id).await.map_err(|err| {
                super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Discover)
                        .with_scope(bifrost_types::ErrorScope::Account),
                )
            })?;
            let mail = client
                .primary_account::<capability::Mail>()
                .map_err(|err| {
                    super::error::into_account_error(
                        err,
                        super::error::JmapErrorContext::new(
                            bifrost_types::AccountOperation::Discover,
                        )
                        .with_scope(bifrost_types::ErrorScope::Account),
                    )
                })?;
            // Each optional family's handle is gated on the lane its own
            // capability block is in, so the handle and the derived
            // `PimSupport` flag cannot disagree.
            let OptionalFamilies {
                submission,
                vacation,
                quota,
                sieve,
                contacts,
                calendars,
                max_delayed_send,
            } = resolve_optional_families(&client);
            let session = client.session();
            let SeededOpenState {
                seed_states,
                email_states,
                mailbox_states,
                lifecycle_mailbox_states,
                mailbox_names,
                primary_id,
                foreign_mail,
                foreign_submission,
                skipped_scopes,
            } = validate_and_seed(&client, &mail).await?;
            let self_emails = fetch_self_emails(&client, &config.credentials).await;

            // This gate is derived from the successfully seeded routing set,
            // never merely from the session. A foreign account skipped during
            // probing must not make us advertise an unreachable send-as path.
            let support = capabilities::PimSupport {
                submission: submission.is_some(),
                max_delayed_send,
                foreign_submission: !foreign_submission.is_empty(),
                vacation: vacation.is_some(),
                quota: quota.is_some(),
                sieve: sieve.is_some(),
                contacts: contacts.is_some(),
                calendar: calendars.is_some(),
            };
            let (caps, limits) = capabilities::build(&session, support)?;

            let shutdown = CancellationToken::new();
            // Snapshot of who this session syncs: push notifications are
            // keyed by accountId (RFC 8620 s7.1) and the reader routes a
            // foreign account's Email changes onto its one seeded
            // account-level `Folder` scope instead of the primary type
            // scope.
            let push_routing = Arc::new(PushRouting::new(primary_id.clone(), seed_states.keys()));
            let ws = WsState::spawn(
                client.clone(),
                caps.push_in_process(),
                shutdown.clone(),
                config.reconnect_policy,
                push_routing,
            );

            let account = JmapAccount::new(
                client,
                mail,
                foreign_mail,
                foreign_submission,
                submission,
                max_delayed_send,
                vacation,
                quota,
                sieve,
                contacts,
                calendars,
                self_emails,
                caps,
                limits,
                seed_states,
                ws,
                shutdown,
                email_states,
                mailbox_states,
                lifecycle_mailbox_states,
                mailbox_names,
            );

            Ok(OpenedAccount {
                account: Arc::new(account) as Arc<dyn Account>,
                skipped_scopes,
            })
        })
    }
}

/// Everything the open sequence reads off the wire before it can build an
/// account: the primary account's two states, and one probe per foreign
/// (shared/delegate) mail account.
struct SeededOpenState<Tr: HttpTransport> {
    seed_states: HashMap<CursorScope, bifrost_types::OpaqueChangeState>,
    email_states: HashMap<String, Option<String>>,
    mailbox_states: HashMap<String, Option<String>>,
    lifecycle_mailbox_states: HashMap<String, Option<String>>,
    mailbox_names: HashMap<String, String>,
    primary_id: String,
    foreign_mail: HashMap<String, JmapMailAccount<Tr>>,
    foreign_submission: HashSet<String>,
    skipped_scopes: Vec<SkippedScope>,
}

/// Validate the session, THEN seed.
///
/// The order is the point, and it is why this stage is a function rather
/// than inline in `open`. `capabilities::build` used to be the only caller
/// of the session validation, and it runs after this whole stage - so an
/// open destined to be refused as a contract violation first issued the two
/// primary probes plus one per share, against a server already known to be
/// non-conformant, and threw every answer away. The session-only half of
/// that validation (core lane and limits, the required mail block's lane)
/// needs nothing from the wire, so it is decided first and the probes never
/// go out. The half that DOES need probe results - the `PimSupport` gates,
/// which depend on which shares actually seeded - stays in `build`.
async fn validate_and_seed<Tr: HttpTransport>(
    client: &Client<Tr>,
    mail: &JmapMailAccount<Tr>,
) -> Result<SeededOpenState<Tr>, AccountError> {
    let session = client.session();
    capabilities::validate_session(&session)?;

    let (email_state, mailbox_state, mailbox_names) =
        seed_account_state(mail).await.map_err(|err| {
            super::error::into_account_error(
                err,
                super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Discover)
                    .with_scope(bifrost_types::ErrorScope::Account),
            )
        })?;

    let mut seed_states = HashMap::new();
    seed_states.insert(
        CursorScope::Type(ObjectType::Email),
        state::encode_for_scope(&CursorScope::Type(ObjectType::Email), email_state.clone())
            .expect("static email cursor scope is supported"),
    );
    seed_states.insert(
        CursorScope::Type(ObjectType::Mailbox),
        state::encode_for_scope(
            &CursorScope::Type(ObjectType::Mailbox),
            mailbox_state.clone(),
        )
        .expect("static mailbox cursor scope is supported"),
    );

    // Per-accountId state caches. The primary account's id keys
    // the same maps every foreign account does - no primary-vs-
    // foreign branch in the cache itself.
    let primary_id = mail.id_str().to_string();
    let mut email_states: HashMap<String, Option<String>> = HashMap::new();
    let mut mailbox_states: HashMap<String, Option<String>> = HashMap::new();
    email_states.insert(primary_id.clone(), Some(email_state.clone()));
    mailbox_states.insert(primary_id.clone(), Some(mailbox_state.clone()));
    let mut lifecycle_mailbox_states = HashMap::new();
    lifecycle_mailbox_states.insert(primary_id.clone(), Some(mailbox_state.clone()));

    // Foreign (shared/delegate) accounts: the session lists every
    // non-personal mail account. For each, probe its two states
    // and seed ONE account-level `Folder` cursor scope. JMAP
    // `Email/changes` state is per (accountId, type) and cannot
    // be filtered by mailbox, so a per-mailbox topology would
    // stream the same account-wide change set once per mailbox
    // (the original B9 defect); the account-level scope streams
    // it exactly once, and per-mailbox membership is derived at
    // hydration from the qualified `mailboxIds` - the same model
    // the primary `Type(Email)` scope uses. A probe failure -
    // revoked grant and exhausted transient retry alike - skips
    // that foreign account and records the omission on
    // `OpenedAccount::skipped_scopes` with its classified error.
    // Open itself must not fail here: initial attach does not
    // retry `factory.open`, so failing would block the user's
    // own primary mail on someone else's shared mailbox being
    // down. And skipping silently would erase the share for the
    // session with no signal anywhere.
    let foreign_ids = foreign_mail_account_ids(&session, &primary_id);
    let mut foreign_mail: HashMap<String, JmapMailAccount<Tr>> = HashMap::new();
    let mut foreign_submission = HashSet::new();
    let mut skipped_scopes: Vec<SkippedScope> = Vec::new();
    // Probing the shares concurrently bounds open latency by the
    // slowest share instead of their sum, but the concurrency has
    // to be the server's number, not the share count: RFC 8620 s2
    // advertises `maxConcurrentRequests`, and a client that
    // exceeds it earns a request-level `limit` error - which here
    // would arrive as a spurious skip of a perfectly healthy
    // share. This is the only place in the crate that issues
    // overlapping API requests, so the bound lives here.
    let probe_concurrency = api_request_concurrency(&session);
    let foreign_results = futures::stream::iter(foreign_ids.into_iter().map(|foreign_id| {
        let foreign_account = JmapMailAccount::new(client.clone(), JmapAccountId::new(&foreign_id));
        async move {
            let result = seed_foreign_account_or_skip(&foreign_id, &foreign_account).await;
            (foreign_id, foreign_account, result)
        }
    }))
    .buffer_unordered(probe_concurrency)
    .collect::<Vec<_>>()
    .await;
    // Completion order is arrival order; installation order must
    // not be. Sorting by accountId keeps `foreign_mail`,
    // `seed_states`, and `skipped_scopes` identical across runs.
    let mut foreign_results = foreign_results;
    foreign_results.sort_by(|a, b| a.0.cmp(&b.0));
    for (foreign_id, foreign_account, result) in foreign_results {
        match result {
            Ok(seed) => {
                apply_foreign_seed(
                    &foreign_id,
                    seed,
                    &mut seed_states,
                    &mut email_states,
                    &mut mailbox_states,
                );
                if account_advertises_submission(&session, &foreign_id) {
                    foreign_submission.insert(foreign_id.clone());
                }
                foreign_mail.insert(foreign_id, foreign_account);
            }
            Err(skip) => {
                // Leave the share out of this session's surface
                // and report the omission. Reopen re-reads the
                // session, so a restored grant or a recovered
                // server brings the share back.
                skipped_scopes.push(skip);
            }
        }
    }

    Ok(SeededOpenState {
        seed_states,
        email_states,
        mailbox_states,
        lifecycle_mailbox_states,
        mailbox_names,
        primary_id,
        foreign_mail,
        foreign_submission,
        skipped_scopes,
    })
}

/// How wide one fan-out of API requests may go.
///
/// `Session::api_request_concurrency` is the ceiling - the same reading the
/// transport's client-wide gate uses, so an omitted or zero
/// `maxConcurrentRequests` is one here too. The upper clamp keeps a server
/// advertising an absurd number from having a fan-out go that wide.
///
/// Every concurrent lane in the crate shares this one reading - the
/// open-time foreign-account probes and the `filters_list` script-blob
/// downloads - so there is one answer to "how wide may we go", not one
/// per call site that could drift from the session.
pub(super) fn api_request_concurrency(session: &crate::core::session::Session) -> usize {
    const MAX_PROBE_CONCURRENCY: usize = 8;
    session.api_request_concurrency().min(MAX_PROBE_CONCURRENCY)
}

/// The `maxObjectsInGet` a session advertises, for the doors that hold a
/// bare `Account` rather than the `CoreLimits` built at open.
/// `capabilities::build` refuses a session that omits or zeroes this, so
/// the fallback is only ever reached by a caller running ahead of that
/// validation; batching one id at a time is the safe shape there.
pub(super) fn max_objects_in_get(session: &crate::core::session::Session) -> usize {
    session
        .core_capabilities()
        .and_then(crate::core::session::CoreCapabilities::max_objects_in_get)
        .unwrap_or(1)
        .max(1)
}

/// The optional PIM family handles a session yields, each already read
/// through the Absent / Malformed / Present lane rule.
///
/// A family's enable flag used to be "`primaryAccounts` names an account
/// for this URI" and nothing else, so a server that advertised, say,
/// `urn:ietf:params:jmap:sieve` with an object this crate refuses to parse
/// kept the whole filters family switched on and got one log line for it.
/// The block and the handle are one fact, so they are decided in one
/// place: on `Malformed` the handle is dropped here, which takes the
/// `PimSupport` flag derived from `is_some()` and the account door that
/// reads the same `Option` off with it. A live handle behind a `false`
/// capability - or a `true` capability in front of a handle whose
/// capability object is gibberish - is a guaranteed `Unsupported` (or
/// worse, a request built from limits nobody could read) one layer up.
struct OptionalFamilies<Tr: HttpTransport> {
    submission: Option<JmapMailAccount<Tr>>,
    vacation: Option<JmapMailAccount<Tr>>,
    quota: Option<JmapMailAccount<Tr>>,
    sieve: Option<JmapMailAccount<Tr>>,
    contacts: Option<JmapMailAccount<Tr>>,
    calendars: Option<JmapMailAccount<Tr>>,
    /// RFC 8621 `maxDelayedSend`, in seconds. Non-zero only when the
    /// selected account's Submission block is parseable and advertises a window.
    max_delayed_send: usize,
}

/// Whether an optional family may advertise, given the lane its own
/// capability block is in.
///
/// `Malformed` is the only refusal, and it is named: the server offered
/// the capability and then described it wrongly, and a family that
/// silently vanishes from a session which does advertise its URI leaves
/// nobody anything to debug. `Absent` is deliberately NOT refused here.
/// A missing block is kept distinct from a malformed one. Submission
/// without an account block has no delayed-send window to advertise.
fn family_block_is_usable<T>(
    state: crate::core::session::CapabilityState<'_, T>,
    uri: &str,
    family: &str,
) -> bool {
    match state {
        crate::core::session::CapabilityState::Present(_)
        | crate::core::session::CapabilityState::Absent => true,
        crate::core::session::CapabilityState::Malformed => {
            tracing::warn!(
                capability = uri,
                family,
                "JMAP account advertises this capability with an unparseable object; \
                 the family is not advertised and its methods refuse as unsupported"
            );
            false
        }
    }
}

fn resolve_optional_families<Tr: HttpTransport>(client: &Client<Tr>) -> OptionalFamilies<Tr> {
    let session = client.session();

    let submission_usable = family_block_is_usable(
        session.submission_capability_state(),
        <capability::Submission as Capability>::URI,
        "submission",
    );
    let submission = client
        .primary_account::<capability::Submission>()
        .ok()
        .filter(|_| submission_usable);
    // `maxDelayedSend` is a mandatory member of the Submission object, so
    // a malformed block has no window to read. This used to fall to 0,
    // which silently disabled `scheduled_send` while `send_message`,
    // `draft_send` and the identity doors stayed live off the same
    // unparseable block - a malformed optional block degrading as a
    // different VALUE rather than as a named "off", which is exactly what
    // the lane rule forbids. It now degrades with the family.
    let max_delayed_send = session
        .submission_capabilities()
        .map_or(0, crate::email::SubmissionCapabilities::max_delayed_send);

    // `urn:ietf:params:jmap:vacationresponse` carries no members this
    // crate models, so it has no typed block and therefore no malformed
    // lane to read: presence in `primaryAccounts` is the whole signal.
    let vacation = client
        .primary_account::<capability::VacationResponseCap>()
        .ok();

    let quota = client
        .primary_account::<capability::Quota>()
        .ok()
        .filter(|_| {
            family_block_is_usable(
                session.quota_capability_state(),
                <capability::Quota as Capability>::URI,
                "quota",
            )
        });
    let sieve = client
        .primary_account::<capability::Sieve>()
        .ok()
        .filter(|_| {
            family_block_is_usable(
                session.sieve_capability_state(),
                <capability::Sieve as Capability>::URI,
                "sieve",
            )
        });

    let contacts = client.primary_account::<capability::Contacts>().ok();
    #[cfg(feature = "contacts")]
    let contacts = contacts.filter(|_| {
        family_block_is_usable(
            session.contacts_capability_state(),
            <capability::Contacts as Capability>::URI,
            "contacts",
        )
    });

    let calendars = client.primary_account::<capability::Calendars>().ok();
    #[cfg(feature = "calendars")]
    let calendars = calendars.filter(|_| {
        family_block_is_usable(
            session.calendars_capability_state(),
            <capability::Calendars as Capability>::URI,
            "calendars",
        )
    });

    OptionalFamilies {
        submission,
        vacation,
        quota,
        sieve,
        contacts,
        calendars,
        max_delayed_send,
    }
}

fn account_advertises_submission(session: &crate::core::session::Session, id: &str) -> bool {
    session.account(id).is_some_and(|account| {
        // Foreign accounts describe their own submission limits too. A key
        // alone must not resurrect a family whose account object is invalid.
        family_block_is_usable(
            account.submission_capability_state(),
            <capability::Submission as Capability>::URI,
            "foreign submission",
        ) && account.submission_capabilities().is_some()
    })
}

async fn fetch_self_emails<Tr: HttpTransport>(
    client: &Client<Tr>,
    credentials: &JmapCredentials,
) -> Vec<String> {
    let mut emails = Vec::new();
    if let Some(email) = credentials.configured_email() {
        push_email_alias(&mut emails, email);
    }

    let Some((principals, principal_id)) = self_principal_lookup(client) else {
        return emails;
    };

    let Ok(response) = principals
        .call(PrincipalGet::new().ids([principal_id]))
        .await
    else {
        return emails;
    };
    for principal in response.into_list() {
        if let Some(email) = principal.email() {
            push_email_alias(&mut emails, email);
        }
        if let Some(aliases) = principal.aliases() {
            for alias in aliases {
                push_email_alias(&mut emails, alias);
            }
        }
    }
    emails
}

// The owner extension belongs to the account whose mail we opened; it
// need not (and normally does not) have a primaryAccounts entry of its own.
fn primary_mail_owner(
    session: &crate::core::session::Session,
) -> Option<&crate::core::session::PrincipalsOwnerCapabilities> {
    let id = session
        .primary_accounts()
        .find(|(uri, _)| uri.as_str() == <capability::Mail as Capability>::URI)?
        .1;
    session.account(id)?.principals_owner_capabilities()
}

fn self_principal_lookup<Tr: HttpTransport>(
    client: &Client<Tr>,
) -> Option<(crate::account::Account<Tr>, crate::principal::PrincipalId)> {
    let session = client.session();
    if let Some(id) = session
        .principals_capabilities()
        .and_then(|capabilities| capabilities.current_user_principal_id().cloned())
    {
        // The id and its containing account are one lookup identity. An
        // owner extension on the mail account may name a different account.
        return Some((client.primary_account::<capability::Principals>().ok()?, id));
    }
    let owner = primary_mail_owner(&session)?;
    let id = owner.principal_id()?.clone();
    let account = owner
        .account_id_for_principal()
        .map(|account_id| crate::account::Account::new(client.clone(), account_id.clone()))
        .or_else(|| client.primary_account::<capability::Principals>().ok())?;
    Some((account, id))
}

fn push_email_alias(emails: &mut Vec<String>, email: &str) {
    if !email.contains('@') {
        return;
    }
    let normalized = email.to_ascii_lowercase();
    if !emails.iter().any(|existing| existing == &normalized) {
        emails.push(normalized);
    }
}

/// The session's non-personal mail accounts (foreign / shared /
/// delegate), excluding the primary mail account. JMAP servers list
/// shared/delegated accounts with `isPersonal: false` and their own
/// `accountCapabilities`; A5a auto-discovers them from the session (no
/// config needed, unlike Graph). Reading the session document alone -
/// and every `open` fetches a fresh one - is what backs the advertised
/// `discovers_foreign_namespaces_on_rediscovery` capability: a share granted
/// after the last open appears here on the next open.
fn foreign_mail_account_ids(
    session: &crate::core::session::Session,
    primary_id: &str,
) -> Vec<String> {
    let mut ids: Vec<String> = session
        .accounts()
        .filter(|id| id.as_str() != primary_id)
        .filter(|id| {
            session.account(id).is_some_and(|account| {
                !account.is_personal()
                    && account
                        .capabilities()
                        .any(|uri| uri.as_str() == <capability::Mail as Capability>::URI)
            })
        })
        .cloned()
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

struct ForeignSeed {
    /// The foreign account's `Email/changes` state: the seed for its one
    /// account-level `Folder` cursor scope.
    email_state: String,
    mailbox_state: String,
}

/// Probe one foreign account's `Email` / `Mailbox` states. Mirrors the
/// primary probes (the `Mailbox/get` also proves the grant reaches the
/// account's mailboxes, not just its Email state).
async fn seed_foreign_account<T: HttpTransport>(
    mail: &JmapMailAccount<T>,
) -> crate::Result<ForeignSeed> {
    let (email_state, mailbox_state, _mailbox_names) = seed_account_state(mail).await?;
    Ok(ForeignSeed {
        email_state,
        mailbox_state,
    })
}

/// Register one successfully probed foreign account: its per-accountId
/// state-cache entries plus exactly ONE seeded cursor scope - the
/// account-level `Folder` scope, seeded from the account's Email state.
///
/// One scope per account, never one per mailbox: `Email/changes` is
/// account-wide, so per-mailbox cursors would each replay the identical
/// change set and a foreign push would fan out to every one of them.
/// Pure over its maps so the seeded topology is unit-pinnable.
fn apply_foreign_seed(
    foreign_id: &str,
    seed: ForeignSeed,
    seed_states: &mut HashMap<CursorScope, bifrost_types::OpaqueChangeState>,
    email_states: &mut HashMap<String, Option<String>>,
    mailbox_states: &mut HashMap<String, Option<String>>,
) {
    let scope = CursorScope::Folder(foreign::encode_foreign_account(foreign_id));
    let encoded = state::encode_for_scope(&scope, seed.email_state.clone())
        .expect("the account-level foreign scope always carries the codec separator");
    seed_states.insert(scope, encoded);
    email_states.insert(foreign_id.to_string(), Some(seed.email_state));
    mailbox_states.insert(foreign_id.to_string(), Some(seed.mailbox_state));
}

/// Seed one foreign account, or turn its probe failure into the
/// open-time skip entry the factory records on
/// `OpenedAccount::skipped_scopes`.
///
/// Every failure lane lands here: a revoked grant classifies terminal
/// (`NoPermission`) and an exhausted transient retry classifies
/// retryable. Neither is allowed to take either unacceptable shape: a
/// probe failure must not fail the whole open (initial attach does not
/// retry `factory.open`, so that blocks the user's own primary mail on
/// a delegate outage), and it must not skip silently (that erases the
/// share for the session, the original G8 defect). The skip's scope
/// names the foreign accountId so the consumer knows WHICH share is
/// degraded and the recovery class says whether a reopen can heal it.
async fn seed_foreign_account_or_skip<T: HttpTransport>(
    foreign_id: &str,
    mail: &JmapMailAccount<T>,
) -> Result<ForeignSeed, SkippedScope> {
    match seed_foreign_account(mail).await {
        Ok(seed) => Ok(seed),
        Err(err) => {
            let error = super::error::into_account_error(
                err,
                super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Discover)
                    .with_scope(bifrost_types::ErrorScope::Mailbox {
                        id: (foreign_id.to_string()).into(),
                    }),
            );
            Err(SkippedScope {
                scope: bifrost_types::ErrorScope::Mailbox {
                    id: (foreign_id.to_string()).into(),
                },
                error,
            })
        }
    }
}

/// The open-time probe set: `Email/get`, `Mailbox/get`.
const OPEN_PROBE_CALLS: usize = 2;

type OpenProbeResponses = (
    crate::core::get::GetResponse<crate::email::Email>,
    crate::core::get::GetResponse<crate::mailbox::Mailbox>,
);

/// Fresh copies of the two open-time probes, in wire order.
fn open_probe_methods() -> (EmailGet, MailboxGet) {
    (
        EmailGet::new().ids(Vec::<EmailId>::new()),
        MailboxGet::new().properties([MailboxProperty::Id, MailboxProperty::Name]),
    )
}

/// Issue the two open-time probes, batched into one request when the
/// session says one request can hold them and serially when it does not.
///
/// `maxCallsInRequest` and `maxSizeRequest` are both hard limits
/// (RFC 8620 §2): exceeding either makes the server reject the *entire*
/// request with a request-level `limit` error, not just the surplus
/// calls. The commonly quoted 16 is the minimum a server is recommended
/// to support, never a floor a client may assume, so batching without
/// checking would fail a primary account's open outright and silently
/// drop every foreign account against a conservative server.
async fn open_probes<T: HttpTransport>(
    mail: &JmapMailAccount<T>,
) -> crate::Result<OpenProbeResponses> {
    if let Some(responses) = batched_open_probes(mail).await? {
        return Ok(responses);
    }
    let (email, mailboxes) = open_probe_methods();
    Ok((mail.call(email).await?, mail.call(mailboxes).await?))
}

/// The batched leg. Returns `Ok(None)` without sending anything when
/// the advertised limits cannot hold both calls in one request.
async fn batched_open_probes<T: HttpTransport>(
    mail: &JmapMailAccount<T>,
) -> crate::Result<Option<OpenProbeResponses>> {
    let session = mail.client().session();
    // A session with no advertised core capability tells us nothing
    // about its limits, and `capabilities::build` rejects it later in
    // open anyway. Take the conservative shape rather than guessing.
    let Some(core) = session.core_capabilities() else {
        return Ok(None);
    };
    // An omitted call/size limit is not a licence to batch: the server
    // told us nothing, and the conservative shape is one call per request.
    let (Some(max_calls), Some(max_size)) = (core.max_calls_in_request(), core.max_size_request())
    else {
        return Ok(None);
    };
    if max_calls < OPEN_PROBE_CALLS {
        return Ok(None);
    }
    // The size limit is checked against the actual encoding inside
    // `send_methods_within`, because it can fall between the individual
    // probes and their batch.
    mail.build()
        .send_methods_within(open_probe_methods(), max_size)
        .await
}

/// Read the two states needed at open. Mailbox names ride the
/// Mailbox/get already needed for its state, so both the primary and
/// shared-account paths use exactly the same request shape.
async fn seed_account_state<T: HttpTransport>(
    mail: &JmapMailAccount<T>,
) -> crate::Result<(String, String, HashMap<String, String>)> {
    let (email, mailboxes) = open_probes(mail).await?;

    let email_state = email.into_state();
    let mailbox_state = mailboxes.state().to_string();
    let mut mailbox_names = HashMap::new();
    for mut mailbox in mailboxes.into_list() {
        let id = mailbox.take_id();
        if !id.as_str().is_empty() {
            mailbox_names.insert(id.into_string(), mailbox.name().unwrap_or("").to_string());
        }
    }

    Ok((email_state, mailbox_state, mailbox_names))
}

async fn connect(
    config: JmapAccountFactoryBuilder,
    account_id: AccountId,
) -> crate::Result<Client> {
    let mut builder = Client::new()
        .credentials(config.credentials.into_client_credentials())
        .net_account_id(account_id)
        .accept_invalid_certs(config.accept_invalid_certs);
    if let Some(timeout) = config.timeout {
        builder = builder.timeout(timeout);
    }
    builder.connect(&config.url).await
}

impl JmapCredentials {
    #[must_use]
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::Bearer {
            token_source: Arc::new(StaticTokenSource::new(token, None)),
        }
    }

    /// Construct bearer credentials from a shared token source.
    /// ratatoskr supplies one `Arc<dyn TokenSource>` it also drives
    /// rotation on, so a rotated token is visible to JMAP through this
    /// single object with no reopen.
    #[must_use]
    pub fn bearer_source(source: Arc<dyn TokenSource>) -> Self {
        Self::Bearer {
            token_source: source,
        }
    }

    fn configured_email(&self) -> Option<&str> {
        match self {
            Self::Basic { username, .. } if username.contains('@') => Some(username),
            _ => None,
        }
    }

    fn into_client_credentials(self) -> Credentials {
        match self {
            Self::Basic { username, password } => Credentials::basic(&username, &password),
            Self::Bearer { token_source } => Credentials::bearer_source(token_source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::{Arc, Mutex};

    use super::*;
    use bifrost_types::{
        ContainerId, FlagOp, IdempotencyKey, MembershipScope, MutationTarget, ObjectId,
        ProtocolSalt, RunId,
    };
    use bytes::Bytes;
    use futures::StreamExt;
    use serde_json::{Value, json};

    use crate::core::session::Session;
    use crate::core::transport::TransportError;

    /// Scripted JMAP HTTP boundary with the same contract as
    /// `ReqwestTransport`.
    ///
    /// Fidelity is the whole point: a fixture states a status and a body,
    /// and the seam derives the error shape from the same decision table
    /// `crates/net/src/request.rs` walks, so a test cannot pin behaviour
    /// against a response shape the production stack is incapable of
    /// producing. Once armed, the script is closed: an unplanned request
    /// reports its ordinal instead of ever reaching a real HTTP transport.
    struct ScriptedTransport {
        requests: Mutex<Vec<Value>>,
        replies: Mutex<VecDeque<ScriptedReply>>,
    }

    struct ScriptedReply {
        kind: ScriptedReplyKind,
        /// Opt-in routing guard. The transport answers positionally and
        /// never checks WHOM a request addresses, so a test that does
        /// not explicitly assert the recorded request can pass while
        /// the code under test talked to the wrong account. When set,
        /// every `methodCall` in the request this reply answers must
        /// carry exactly this `accountId`; a mismatch panics at the
        /// offending request instead of handing back a plausible
        /// answer.
        expect_account: Option<String>,
    }

    enum ScriptedReplyKind {
        /// A server response, classified on delivery.
        Response {
            status: reqwest::StatusCode,
            body: Bytes,
            headers: reqwest::header::HeaderMap,
        },
        /// A transport failure with no HTTP response behind it.
        Error(TransportError),
    }

    impl ScriptedReply {
        fn ok(body: impl Into<Bytes>) -> Self {
            Self::status(reqwest::StatusCode::OK, body)
        }

        fn status(status: reqwest::StatusCode, body: impl Into<Bytes>) -> Self {
            Self {
                kind: ScriptedReplyKind::Response {
                    status,
                    body: body.into(),
                    headers: reqwest::header::HeaderMap::new(),
                },
                expect_account: None,
            }
        }

        fn error(error: TransportError) -> Self {
            Self {
                kind: ScriptedReplyKind::Error(error),
                expect_account: None,
            }
        }

        fn header(mut self, name: &'static str, value: &str) -> Self {
            if let ScriptedReplyKind::Response { headers, .. } = &mut self.kind {
                headers.insert(
                    name,
                    reqwest::header::HeaderValue::from_str(value).expect("test header value"),
                );
            }
            self
        }

        /// Arm the routing guard: the request this reply answers must
        /// address `account_id` in every method call.
        fn for_account(mut self, account_id: &str) -> Self {
            self.expect_account = Some(account_id.to_string());
            self
        }

        /// Reproduce what the production stack hands the JMAP layer for
        /// this response. Follows `crates/net/src/request.rs`:
        ///
        /// - 2xx: the body reaches the protocol decoder.
        /// - 3xx: bifrost-net returns 304 / 305 / 306 and `Location`-less
        ///   redirects as `Ok`, and the redirect loop's `PassThrough` arm
        ///   hands the BODY up rather than discarding it, so
        ///   `ReqwestTransport::handle_response` turns them into a
        ///   `TransportError` carrying that body and no net evidence.
        ///   The fixture preserves the scripted body for the same
        ///   reason: production can deliver one. It used to blank the
        ///   body, matching an older `PassThrough` arm that dropped it -
        ///   a double that keeps mirroring behavior the transport has
        ///   stopped having makes every test built on it confidently
        ///   wrong.
        /// - 401: the forced refresh retries once, and the second 401 is
        ///   `AuthLost` with acknowledged transmission evidence.
        /// - Retryable per `RetryPolicy::default()` - 429 plus the whole
        ///   5xx family, read off the policy rather than restated: the
        ///   retry budget is spent and the loop surfaces `RateLimited`
        ///   for 429 and `RetryBudgetExhausted` for the rest, each with
        ///   the final response preserved.
        /// - Any other 4xx: terminal `Error::Status`.
        ///
        /// So a `Status` fixture for a retryable code is not something a
        /// test author can write by accident.
        fn into_transport_result(self) -> Result<Bytes, TransportError> {
            let (status, body, headers) = match self.kind {
                ScriptedReplyKind::Error(error) => return Err(error),
                ScriptedReplyKind::Response {
                    status,
                    body,
                    headers,
                } => (status, body, headers),
            };
            if status.is_success() {
                return Ok(body);
            }
            if status.is_redirection() {
                return Err(TransportError::with_body(format!("HTTP {status}"), body));
            }
            let final_response = bifrost_net::FinalResponse {
                status,
                headers: headers.clone(),
                body: body.clone(),
            };
            if status == reqwest::StatusCode::UNAUTHORIZED {
                return Err(TransportError::from_net(bifrost_net::Error::AuthLost {
                    transmission_state: Some(bifrost_types::TransmissionState::Acknowledged),
                    final_response: Some(final_response),
                }));
            }
            let policy = bifrost_net::RetryPolicy::default();
            if policy.statuses.contains(&status) || status.is_server_error() {
                let retry_after =
                    bifrost_net::parse_retry_after(headers.get(reqwest::header::RETRY_AFTER))
                        .map(|hint| hint.min(policy.honor_retry_after_cap));
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(TransportError::from_net(bifrost_net::Error::RateLimited {
                        retry_after,
                        final_response,
                    }));
                }
                return Err(TransportError::from_net(
                    bifrost_net::Error::RetryBudgetExhausted {
                        final_response: Some(final_response),
                        retry_after_history: retry_after.into_iter().collect(),
                    },
                ));
            }
            Err(TransportError::from_net(bifrost_net::Error::Status {
                code: status,
                body,
                headers,
            }))
        }
    }

    impl ScriptedTransport {
        fn new(replies: impl IntoIterator<Item = ScriptedReply>) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                replies: Mutex::new(replies.into_iter().collect()),
            }
        }

        fn requests(&self) -> Vec<Value> {
            self.requests.lock().expect("script requests lock").clone()
        }

        fn api_reply(&self, body: Vec<u8>) -> Result<Bytes, TransportError> {
            let request = serde_json::from_slice(&body).map_err(|error| {
                TransportError::with_source("JMAP client emitted a non-JSON API request", error)
            })?;
            let request_number = {
                let mut requests = self
                    .requests
                    .lock()
                    .map_err(|_| TransportError::new("script requests lock poisoned"))?;
                requests.push(request);
                requests.len()
            };
            let reply = self
                .replies
                .lock()
                .map_err(|_| TransportError::new("script replies lock poisoned"))?
                .pop_front()
                .ok_or_else(|| {
                    TransportError::new(format!(
                        "scripted JMAP transport exhausted at API request #{request_number}"
                    ))
                })?;
            if let Some(expected) = &reply.expect_account {
                let requests = self
                    .requests
                    .lock()
                    .map_err(|_| TransportError::new("script requests lock poisoned"))?;
                let request = &requests[request_number - 1];
                let calls = request["methodCalls"]
                    .as_array()
                    .unwrap_or_else(|| panic!("request #{request_number} has no methodCalls"));
                for call in calls {
                    // Panic rather than return an error: a routing
                    // mismatch answered with a TransportError could be
                    // swallowed by an error-tolerant code path, and the
                    // whole point of the guard is a loud failure.
                    assert_eq!(
                        call[1]["accountId"].as_str(),
                        Some(expected.as_str()),
                        "scripted reply #{request_number} is armed for account \
                         {expected:?} but the request addressed: {call}"
                    );
                }
            }
            reply.into_transport_result()
        }
    }

    impl HttpTransport for ScriptedTransport {
        async fn api_request(&self, _url: &str, body: Vec<u8>) -> Result<Bytes, TransportError> {
            self.api_reply(body)
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new(
                "scripted JMAP transport has no upload reply",
            ))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new(
                "scripted JMAP transport has no download reply",
            ))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new(
                "scripted JMAP transport has no session reply",
            ))
        }
    }

    fn session() -> Session {
        session_with_limits(8, 100_000)
    }

    fn session_with_limits(max_calls_in_request: usize, max_size_request: usize) -> Session {
        session_doc(
            max_calls_in_request,
            max_size_request,
            json!({
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                "shared": {"name": "Shared", "isPersonal": false, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            }),
        )
    }

    fn session_doc(
        max_calls_in_request: usize,
        max_size_request: usize,
        accounts: Value,
    ) -> Session {
        serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": max_size_request,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": max_calls_in_request,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": accounts,
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses")
    }

    fn email_result(account_id: &str, suffix: &str, call_id: &str) -> Value {
        json!(["Email/get", {"accountId": account_id, "state": format!("email-{suffix}"), "list": [], "notFound": []}, call_id])
    }

    fn mailbox_result(account_id: &str, suffix: &str, call_id: &str) -> Value {
        json!(["Mailbox/get", {"accountId": account_id, "state": format!("mailbox-{suffix}"), "list": [{"id": format!("inbox-{suffix}"), "name": "Inbox"}], "notFound": []}, call_id])
    }

    fn method_reply(results: Vec<Value>) -> ScriptedReply {
        ScriptedReply::ok(
            json!({"sessionState": "session-1", "methodResponses": results}).to_string(),
        )
    }

    fn method_error(account_id: &str, error_type: &str) -> ScriptedReply {
        method_reply(vec![json!([
            "error",
            {"type": error_type, "accountId": account_id},
            "s0"
        ])])
    }

    fn email_set_reply(
        account_id: &str,
        old_state: &str,
        new_state: &str,
        id: &str,
    ) -> ScriptedReply {
        email_set_reply_ids(account_id, old_state, new_state, &[id])
    }

    /// An `Email/set` answer acknowledging every submitted id.
    fn email_set_reply_ids(
        account_id: &str,
        old_state: &str,
        new_state: &str,
        ids: &[&str],
    ) -> ScriptedReply {
        let updated: serde_json::Map<String, Value> = ids
            .iter()
            .map(|id| ((*id).to_string(), Value::Null))
            .collect();
        method_reply(vec![json!([
            "Email/set",
            {
                "accountId": account_id,
                "oldState": old_state,
                "newState": new_state,
                "updated": updated,
                "notUpdated": {}
            },
            "s0"
        ])])
    }

    fn state_map(entries: &[(&str, &str)]) -> crate::sync::state_cache::StateMap {
        Arc::new(tokio::sync::Mutex::new(
            entries
                .iter()
                .map(|(account, state)| ((*account).to_string(), Some((*state).to_string())))
                .collect(),
        ))
    }

    fn idempotency_key() -> IdempotencyKey {
        IdempotencyKey {
            run_id: RunId("test-run".to_string()),
            sequence: 1,
            protocol_salt: ProtocolSalt::Jmap("test".to_string()),
        }
    }

    /// One reply answering both batched open probes.
    fn open_reply(account_id: &str, suffix: &str) -> ScriptedReply {
        method_reply(vec![
            email_result(account_id, suffix, "s0"),
            mailbox_result(account_id, suffix, "s1"),
        ])
    }

    /// Two replies, one per probe, for the serial fallback. Each
    /// single-call request numbers its one call `s0`.
    fn serial_open_replies(account_id: &str, suffix: &str) -> [ScriptedReply; 2] {
        [
            method_reply(vec![email_result(account_id, suffix, "s0")]),
            method_reply(vec![mailbox_result(account_id, suffix, "s0")]),
        ]
    }

    fn scripted_client(
        replies: impl IntoIterator<Item = ScriptedReply>,
    ) -> Client<ScriptedTransport> {
        scripted_client_with_session(session(), replies)
    }

    /// A session whose `primaryAccounts` names an account for every
    /// optional family, so the ONLY thing that can switch a family off is
    /// the shape of its capability block. `blocks` supplies those blocks.
    fn session_with_family_blocks(blocks: Value) -> Session {
        let mut capabilities = json!({
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
            "urn:ietf:params:jmap:mail": {}
        });
        let map = capabilities.as_object_mut().expect("object");
        for (key, value) in blocks.as_object().expect("blocks is an object") {
            map.insert(
                key.clone(),
                if key == "urn:ietf:params:jmap:quota" {
                    value.clone()
                } else {
                    json!({})
                },
            );
        }
        let mut account_capabilities = blocks;
        account_capabilities["urn:ietf:params:jmap:mail"] = json!({});
        serde_json::from_value(json!({
            "capabilities": capabilities,
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false, "accountCapabilities": account_capabilities}
            },
            "primaryAccounts": {
                "urn:ietf:params:jmap:mail": "primary",
                "urn:ietf:params:jmap:submission": "primary",
                "urn:ietf:params:jmap:vacationresponse": "primary",
                "urn:ietf:params:jmap:quota": "primary",
                "urn:ietf:params:jmap:sieve": "primary",
                "urn:ietf:params:jmap:contacts": "primary",
                "urn:ietf:params:jmap:calendars": "primary"
            },
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses")
    }

    #[tokio::test]
    async fn principal_aliases_come_from_account_capabilities() {
        for use_owner in [false, true] {
            let mut doc =
                serde_json::to_value(session_with_family_blocks(well_formed_family_blocks()))
                    .unwrap();
            let uri = "urn:ietf:params:jmap:principals";
            doc["capabilities"][uri] = json!({});
            doc["primaryAccounts"][uri] = json!("primary");
            doc["accounts"]["primary"]["accountCapabilities"][uri] = if use_owner {
                json!({})
            } else {
                json!({"currentUserPrincipalId": "self"})
            };
            doc["accounts"]["primary"]["accountCapabilities"]["urn:ietf:params:jmap:principals:owner"] =
                json!({"principalId": "owner", "accountIdForPrincipal": "owner-target"});
            doc["accounts"]["owner-target"] = json!({
                "name": "Principals", "isPersonal": true, "isReadOnly": false,
                "accountCapabilities": {uri: {}}
            });
            let account_id = if use_owner { "owner-target" } else { "primary" };
            let principal_id = if use_owner { "owner" } else { "self" };
            let response = method_reply(vec![json!(["Principal/get", {
                "accountId": account_id, "state": "p1",
                "list": [{"id": principal_id, "email": "Self@Example.Test", "aliases": ["alias@example.test"]}],
                "notFound": []
            }, "s0"])]).for_account(account_id);
            let client =
                scripted_client_with_session(serde_json::from_value(doc).unwrap(), [response]);
            let emails = fetch_self_emails(&client, &JmapCredentials::bearer("token")).await;
            assert_eq!(emails, ["self@example.test", "alias@example.test"]);
            let requests = client.transport().requests.lock().unwrap();
            assert_eq!(
                requests[0]["methodCalls"][0][1]["ids"],
                json!([principal_id])
            );
        }
    }

    #[tokio::test]
    async fn malformed_account_mail_limits_refuse_before_probes() {
        let mut doc = serde_json::to_value(session()).unwrap();
        doc["accounts"]["primary"]["accountCapabilities"]["urn:ietf:params:jmap:mail"] =
            json!("invalid");
        let client = scripted_client_with_session(serde_json::from_value(doc).unwrap(), []);
        let mail = JmapMailAccount::new(client.clone(), JmapAccountId::new("primary"));
        let error = validate_and_seed(&client, &mail)
            .await
            .err()
            .expect("invalid account block");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation,
            )
        );
        assert!(client.transport().requests.lock().unwrap().is_empty());
    }

    #[test]
    fn submission_window_is_selected_by_account_not_session() {
        let mut doc =
            serde_json::to_value(session_with_family_blocks(well_formed_family_blocks())).unwrap();
        let uri = "urn:ietf:params:jmap:submission";
        doc["capabilities"][uri] = json!({"maxDelayedSend": 9999});
        doc["accounts"]["other"] = json!({"name": "Other", "isPersonal": false, "isReadOnly": false, "accountCapabilities": {uri: {"maxDelayedSend": 17}}});
        doc["primaryAccounts"][uri] = json!("other");
        let client = scripted_client_with_session(serde_json::from_value(doc).unwrap(), []);
        let families = resolve_optional_families(&client);
        assert!(families.submission.is_some());
        assert_eq!(families.max_delayed_send, 17);
        assert_eq!(
            client
                .session()
                .account("primary")
                .unwrap()
                .submission_capabilities()
                .unwrap()
                .max_delayed_send(),
            3600
        );
    }

    /// Every optional family block in its well-formed shape.
    fn well_formed_family_blocks() -> Value {
        json!({
            "urn:ietf:params:jmap:submission": {"maxDelayedSend": 3600},
            "urn:ietf:params:jmap:quota": {},
            "urn:ietf:params:jmap:sieve": {},
            "urn:ietf:params:jmap:contacts": {},
            "urn:ietf:params:jmap:calendars": {}
        })
    }

    /// The control: a session that describes every optional family
    /// correctly hands back every handle, and the delayed-send window is
    /// the one the Submission block advertises.
    #[test]
    fn well_formed_optional_family_blocks_keep_every_handle() {
        let client = scripted_client_with_session(
            session_with_family_blocks(well_formed_family_blocks()),
            [],
        );
        let families = resolve_optional_families(&client);
        assert!(families.submission.is_some());
        assert!(families.vacation.is_some());
        assert!(families.quota.is_some());
        assert!(families.sieve.is_some());
        assert!(families.contacts.is_some());
        assert!(families.calendars.is_some());
        assert_eq!(families.max_delayed_send, 3600);
    }

    /// The defect: the family enable flags came from `primaryAccounts`
    /// alone, so a present-but-unparseable block left the family fully
    /// advertised with only a log line to show for it. Each family is
    /// varied on its own, so a gate that fires for the wrong URI is
    /// visible.
    #[test]
    fn a_malformed_optional_family_block_drops_its_own_handle_only() {
        for uri in [
            "urn:ietf:params:jmap:submission",
            "urn:ietf:params:jmap:quota",
            "urn:ietf:params:jmap:sieve",
            "urn:ietf:params:jmap:contacts",
            "urn:ietf:params:jmap:calendars",
        ] {
            let mut blocks = well_formed_family_blocks();
            blocks[uri] = json!("not an object");
            let client = scripted_client_with_session(session_with_family_blocks(blocks), []);
            let families = resolve_optional_families(&client);
            let present = [
                (
                    "urn:ietf:params:jmap:submission",
                    families.submission.is_some(),
                ),
                ("urn:ietf:params:jmap:quota", families.quota.is_some()),
                ("urn:ietf:params:jmap:sieve", families.sieve.is_some()),
                ("urn:ietf:params:jmap:contacts", families.contacts.is_some()),
                (
                    "urn:ietf:params:jmap:calendars",
                    families.calendars.is_some(),
                ),
            ];
            for (family_uri, is_present) in present {
                assert_eq!(
                    is_present,
                    family_uri != uri,
                    "malformed {uri}: {family_uri} availability"
                );
            }
            if uri == "urn:ietf:params:jmap:submission" {
                assert_eq!(
                    families.max_delayed_send, 0,
                    "a malformed Submission block advertises no window"
                );
            } else {
                assert_eq!(families.max_delayed_send, 3600, "malformed {uri}");
            }
        }
    }

    /// A Submission block that omits its mandatory `maxDelayedSend` is
    /// malformed, not "a server with a zero-second window". The old
    /// reader collapsed the two, so the whole submission family stayed
    /// live off a block nobody could parse and only `scheduled_send` went
    /// dark - a malformed optional block degrading as a silently
    /// different VALUE, which the lane rule forbids.
    #[test]
    fn an_account_submission_block_without_max_delayed_send_disables_the_family() {
        let mut blocks = well_formed_family_blocks();
        blocks["urn:ietf:params:jmap:submission"] = json!({});
        let client = scripted_client_with_session(session_with_family_blocks(blocks), []);
        let families = resolve_optional_families(&client);
        assert!(
            families.submission.is_none(),
            "an unparseable Submission block takes the whole family, not just the window"
        );
        assert_eq!(families.max_delayed_send, 0);
    }

    /// A well-formed Submission block advertising a zero window is a
    /// different server from a malformed one: the family stays live (send,
    /// drafts, identities all work) and only `scheduled_send` is off.
    #[test]
    fn a_zero_delayed_send_window_keeps_the_submission_family() {
        let mut blocks = well_formed_family_blocks();
        blocks["urn:ietf:params:jmap:submission"] = json!({"maxDelayedSend": 0});
        let client = scripted_client_with_session(session_with_family_blocks(blocks), []);
        let families = resolve_optional_families(&client);
        assert!(families.submission.is_some());
        assert_eq!(families.max_delayed_send, 0);

        let support = crate::sync::capabilities::PimSupport {
            submission: families.submission.is_some(),
            max_delayed_send: families.max_delayed_send,
            foreign_submission: false,
            vacation: families.vacation.is_some(),
            quota: families.quota.is_some(),
            sieve: families.sieve.is_some(),
            contacts: families.contacts.is_some(),
            calendar: families.calendars.is_some(),
        };
        let (caps, _) =
            crate::sync::capabilities::build(&client.session(), support).expect("session is legal");
        assert!(caps.pim_methods.send_message);
        assert!(!caps.pim_methods.scheduled_send);
    }

    /// The end-to-end shape of hole 2: the derived `PimMethodSupport`
    /// flags for a family whose block is malformed are all false, not just
    /// logged about.
    #[test]
    fn a_malformed_family_block_is_not_advertised_in_the_capability_snapshot() {
        let mut blocks = well_formed_family_blocks();
        blocks["urn:ietf:params:jmap:sieve"] = json!("not an object");
        blocks["urn:ietf:params:jmap:contacts"] = json!(7);
        blocks["urn:ietf:params:jmap:calendars"] = json!(false);
        let client = scripted_client_with_session(session_with_family_blocks(blocks), []);
        let families = resolve_optional_families(&client);
        let support = crate::sync::capabilities::PimSupport {
            submission: families.submission.is_some(),
            max_delayed_send: families.max_delayed_send,
            foreign_submission: false,
            vacation: families.vacation.is_some(),
            quota: families.quota.is_some(),
            sieve: families.sieve.is_some(),
            contacts: families.contacts.is_some(),
            calendar: families.calendars.is_some(),
        };
        let (caps, _) = crate::sync::capabilities::build(&client.session(), support)
            .expect("optional families degrade");
        let pim = &caps.pim_methods;
        assert!(!pim.filters_list);
        assert!(!pim.filter_validate);
        assert_eq!(caps.filter_rule_shape, bifrost_types::FilterRuleShape::None);
        assert!(!pim.contacts_list);
        assert!(!pim.contact_autocomplete);
        assert!(!pim.calendars_list);
        assert!(!pim.event_rsvp);
        // Quota and submission were described correctly, so they stay on.
        assert!(pim.quota_get);
        assert!(pim.send_message);
    }

    /// A session whose REQUIRED mail block is malformed is refused as a
    /// contract violation. That refusal is decided by the session alone, so
    /// it must land before the seeding stage puts anything on the wire:
    /// otherwise open pays two primary probes plus one per share - here
    /// three requests, all against a server already known non-conformant -
    /// and discards every answer with the error. The scripted transport is
    /// armed with NO replies, so any probe that does go out panics rather
    /// than merely being counted.
    #[tokio::test]
    async fn a_malformed_required_block_refuses_the_open_before_any_probe() {
        let session: Session = serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                // Present and unparseable: the malformed lane, not absent.
                "urn:ietf:params:jmap:mail": "not an object"
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
        .expect("test session parses");
        let client = scripted_client_with_session(session, []);
        let primary = JmapMailAccount::new(client.clone(), JmapAccountId::new("primary"));

        let error = validate_and_seed(&client, &primary)
            .await
            .err()
            .expect("a malformed required capability block refuses the open");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
        assert!(
            client.transport().requests().is_empty(),
            "a session the contract already condemns must cost zero requests, \
             not two primary probes and one per share"
        );
    }

    fn scripted_client_with_session(
        session: Session,
        replies: impl IntoIterator<Item = ScriptedReply>,
    ) -> Client<ScriptedTransport> {
        Client::with_transport(
            ScriptedTransport::new(replies),
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("scripted client builds")
    }

    fn assert_open_batch(request: &Value, account_id: &str) {
        assert_eq!(request["methodCalls"].as_array().map(Vec::len), Some(2));
        assert_eq!(request["methodCalls"][0][0], "Email/get");
        assert_eq!(request["methodCalls"][1][0], "Mailbox/get");
        for call in request["methodCalls"]
            .as_array()
            .expect("method calls array")
        {
            assert_eq!(call[1]["accountId"], account_id);
        }
    }

    /// The `discovers_foreign_namespaces_on_rediscovery` capability promises
    /// that a share granted after the last open surfaces at the next
    /// one. The mechanism is that foreign discovery reads only the
    /// session document and every `open` fetches a fresh one (the
    /// factory carries no session cache), so the predicate must track a
    /// before/after pair of session documents - and must select only
    /// non-personal accounts that actually advertise mail.
    #[test]
    fn foreign_discovery_is_session_driven_so_a_new_grant_surfaces_at_reopen() {
        let personal_mail = json!({
            "name": "Primary",
            "isPersonal": true,
            "isReadOnly": false,
            "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}
        });

        let before = session_doc(8, 100_000, json!({"primary": personal_mail.clone()}));
        assert_eq!(
            foreign_mail_account_ids(&before, "primary"),
            Vec::<String>::new(),
            "no grant yet: nothing foreign to discover"
        );

        let after = session_doc(
            8,
            100_000,
            json!({
                "primary": personal_mail,
                // The grant that arrived between opens.
                "delegate": {"name": "Delegate", "isPersonal": false, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                // Personal sibling account: never foreign.
                "archive": {"name": "Archive", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                // Non-personal but no mail capability: not a mail share.
                "files": {"name": "Files", "isPersonal": false, "isReadOnly": true, "accountCapabilities": {"urn:ietf:params:jmap:blob": {}}}
            }),
        );
        assert_eq!(
            foreign_mail_account_ids(&after, "primary"),
            vec!["delegate".to_string()],
            "the new grant, and only the new grant, is discovered"
        );
    }

    /// Build a session whose only interesting property is the advertised
    /// concurrency limit.
    fn session_with_concurrency(max_concurrent_requests: Value) -> Session {
        serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": max_concurrent_requests,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {"primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}},
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses")
    }

    /// Foreign probing is the one place the crate puts overlapping API
    /// requests on the wire, so it is the one place that has to honour
    /// `maxConcurrentRequests` (RFC 8620 s2). Exceeding it does not
    /// merely waste sockets: the server answers a request-level `limit`
    /// error, which `seed_foreign_account_or_skip` would classify as a
    /// probe failure and turn a healthy share into a skipped scope.
    #[test]
    fn api_request_concurrency_never_exceeds_the_advertised_limit() {
        assert_eq!(
            api_request_concurrency(&session_with_concurrency(json!(4))),
            4,
            "the server's own number is the ceiling"
        );
        assert_eq!(
            api_request_concurrency(&session_with_concurrency(json!(1))),
            1,
            "a strictly serial server must be probed serially"
        );
        assert_eq!(
            api_request_concurrency(&session_with_concurrency(json!(0))),
            1,
            "zero is not a usable buffer bound; serial is the safe reading"
        );
        assert_eq!(
            api_request_concurrency(&session_with_concurrency(json!(10_000))),
            8,
            "an absurd advertisement must not make open fan out that far"
        );
    }

    /// The routing guard's own bite: a reply armed for one account must
    /// panic when the request addresses another, because a positional
    /// answer to a misrouted request is exactly how a routing
    /// regression passes quietly.
    #[tokio::test]
    #[should_panic(expected = "armed for account \"shared\"")]
    async fn an_armed_reply_panics_when_the_request_addresses_another_account() {
        let client = scripted_client([open_reply("primary", "p").for_account("shared")]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let _ = seed_account_state(&primary).await;
    }

    #[tokio::test]
    async fn open_state_probes_are_one_recorded_request_per_account() {
        let client = scripted_client([open_reply("primary", "p"), open_reply("shared", "s")]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));

        let primary_seed = seed_account_state(&primary).await.expect("primary seed");
        let shared_seed = seed_foreign_account(&shared).await.expect("shared seed");

        assert_eq!(primary_seed.0, "email-p");
        assert_eq!(shared_seed.email_state, "email-s");
        let requests = client.transport().requests();
        assert_eq!(requests.len(), 2);
        assert_open_batch(&requests[0], "primary");
        assert_open_batch(&requests[1], "shared");
    }

    /// B9's regression guard: `Email/changes` is account-wide, so a share
    /// with M mailboxes must seed exactly ONE cursor scope - the
    /// account-level `Folder` scope, carrying the account's Email state -
    /// never one scope per mailbox each replaying the identical change
    /// set. This drives the real probe + seed application; only the
    /// `open()` shell around them is out of hermetic reach.
    #[tokio::test]
    async fn a_multi_mailbox_share_seeds_exactly_one_account_level_cursor_scope() {
        let client = scripted_client([method_reply(vec![
            email_result("shared", "s", "s0"),
            json!(["Mailbox/get", {"accountId": "shared", "state": "mailbox-s", "list": [
                {"id": "inbox", "name": "Inbox"},
                {"id": "archive", "name": "Archive"},
                {"id": "spam", "name": "Spam"}
            ], "notFound": []}, "s1"]),
        ])]);
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
        let seed = seed_foreign_account(&shared).await.expect("shared seed");

        let mut seed_states = HashMap::new();
        let mut email_states = HashMap::new();
        let mut mailbox_states = HashMap::new();
        apply_foreign_seed(
            "shared",
            seed,
            &mut seed_states,
            &mut email_states,
            &mut mailbox_states,
        );

        assert_eq!(
            seed_states.len(),
            1,
            "three mailboxes must not become three account-wide change streams"
        );
        let scope = CursorScope::Folder(foreign::encode_foreign_account("shared"));
        let encoded = seed_states
            .get(&scope)
            .expect("the one scope is the account-level Folder scope")
            .clone();
        let cursor = bifrost_types::ChangeCursor {
            scope: scope.clone(),
            server_state: encoded,
            advanced_through: None,
            envelope_version: state::OUTER_CURSOR_ENVELOPE_VERSION,
        };
        let (_, seeded_state) = state::decode_cursor(&cursor).expect("seed decodes");
        assert_eq!(
            seeded_state, "email-s",
            "the account scope is seeded from the account's Email state"
        );
        assert_eq!(
            email_states.get("shared"),
            Some(&Some("email-s".to_string()))
        );
        assert_eq!(
            mailbox_states.get("shared"),
            Some(&Some("mailbox-s".to_string()))
        );
    }

    /// The account-level foreign scope inventories the WHOLE account: one
    /// unfiltered `Email/query` walk against the foreign handle (the
    /// per-mailbox topology issued one `inMailbox`-filtered walk per
    /// mailbox), with every entry qualified into the owner's namespace so
    /// hydration, blob reads, and folder attribution stay self-routing.
    #[tokio::test]
    async fn foreign_account_inventory_walks_the_whole_account_without_a_mailbox_filter() {
        let client = scripted_client([
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "shared", "queryState": "q1", "position": 0, "ids": ["M1"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "shared", "state": "email-s", "list": [
                    {"id": "M1", "blobId": "B1", "threadId": "T1", "size": 10,
                     "mailboxIds": {"inbox": true}, "keywords": {}}
                ], "notFound": []},
                "s0"
            ])]),
            // The empty follow-up page ends the walk.
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "shared", "queryState": "q1", "position": 1, "ids": []},
                "s0"
            ])]),
        ]);
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
        let scope = CursorScope::Folder(foreign::encode_foreign_account("shared"));
        let owner = Some(bifrost_types::MailboxId("shared".to_string()));
        let mut stream = crate::sync::inventory::stream(
            shared,
            super::capabilities::CoreLimits {
                max_objects_in_get: 4,
                max_objects_in_set: 4,
            },
            scope,
            owner,
        );

        match stream.next().await {
            Some(bifrost_types::SyncEvent::Batch(batch)) => match &batch.items[..] {
                [entry] => {
                    assert_eq!(entry.id.0, foreign::encode_object("shared", "M1"));
                    assert_eq!(
                        entry.blob_id.as_ref().map(|blob| blob.0.as_str()),
                        Some(foreign::encode_object("shared", "B1").as_str())
                    );
                    assert!(entry.memberships.contains(&MembershipScope::Folder(
                        foreign::encode_foreign("shared", "inbox")
                    )));
                    assert!(entry.memberships.contains(&MembershipScope::Mailbox(
                        bifrost_types::MailboxId("shared".to_string())
                    )));
                }
                other => panic!("expected one qualified inventory entry, got {other:?}"),
            },
            other => panic!("expected the inventory batch, got {other:?}"),
        }
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::SyncEvent::Done(None))
        ));

        let requests = client.transport().requests();
        assert_eq!(requests.len(), 3);
        for request in [&requests[0], &requests[2]] {
            let query = &request["methodCalls"][0];
            assert_eq!(query[0], "Email/query");
            assert_eq!(query[1]["accountId"], "shared");
            assert!(
                query[1].get("filter").is_none(),
                "the account-level scope walks the whole account, unfiltered: {query}"
            );
        }
    }

    /// A full primary walk must not confuse a server-capped short query
    /// page with end-of-inventory. It continues from the consumed query
    /// position, and primary ids remain bare rather than being qualified.
    #[tokio::test]
    async fn primary_inventory_continues_after_a_short_page_with_bare_ids() {
        let client = scripted_client([
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q1", "position": 0, "ids": ["M1"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "primary", "state": "email-s", "list": [
                    {"id": "M1", "blobId": "B1", "threadId": "T1", "size": 10,
                     "mailboxIds": {"inbox": true}, "keywords": {}}
                ], "notFound": []},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q1", "position": 1, "ids": ["M2"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "primary", "state": "email-s", "list": [
                    {"id": "M2", "blobId": "B2", "threadId": "T2", "size": 11,
                     "mailboxIds": {"archive": true}, "keywords": {}}
                ], "notFound": []},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q1", "position": 2, "ids": []},
                "s0"
            ])]),
        ]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let mut stream = crate::sync::inventory::stream(
            primary,
            super::capabilities::CoreLimits {
                max_objects_in_get: 4,
                max_objects_in_set: 4,
            },
            CursorScope::Type(ObjectType::Email),
            None,
        );

        for expected in ["M1", "M2"] {
            match stream.next().await {
                Some(bifrost_types::SyncEvent::Batch(batch)) => {
                    assert_eq!(batch.items.len(), 1);
                    assert_eq!(batch.items[0].id.0, expected);
                }
                other => panic!("expected primary inventory batch, got {other:?}"),
            }
        }
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::SyncEvent::Done(None))
        ));
        let requests = client.transport().requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0]["methodCalls"][0][1]["position"], 0);
        assert_eq!(requests[2]["methodCalls"][0][1]["anchor"], "M1");
        assert_eq!(requests[2]["methodCalls"][0][1]["anchorOffset"], 1);
        assert!(requests[2]["methodCalls"][0][1].get("position").is_none());
        assert_eq!(requests[4]["methodCalls"][0][1]["anchor"], "M2");
    }

    /// Email inventory anchors each page on the last queried id. Its immutable
    /// sort key and the change cursor seeded before inventory make ordinary
    /// queryState movement safe to continue through.
    #[tokio::test]
    async fn email_inventory_continues_from_its_anchor_when_query_state_moves() {
        let client = scripted_client([
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q1", "position": 0, "ids": ["M1"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "primary", "state": "email-s", "list": [
                    {"id": "M1", "blobId": "B1", "threadId": "T1", "size": 10,
                     "mailboxIds": {"inbox": true}, "keywords": {}}
                ], "notFound": []},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q2", "position": 1, "ids": ["M2"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "primary", "state": "email-s", "list": [
                    {"id": "M2", "blobId": "B2", "threadId": "T2", "size": 12,
                     "mailboxIds": {"inbox": true}, "keywords": {}}
                ], "notFound": []},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q3", "position": 2, "ids": []},
                "s0"
            ])]),
        ]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let mut stream = crate::sync::inventory::stream(
            primary,
            super::capabilities::CoreLimits {
                max_objects_in_get: 4,
                max_objects_in_set: 4,
            },
            CursorScope::Type(ObjectType::Email),
            None,
        );

        for expected in ["M1", "M2"] {
            match stream.next().await {
                Some(bifrost_types::SyncEvent::Batch(batch)) => {
                    assert_eq!(batch.items.len(), 1);
                    assert_eq!(batch.items[0].id.0, expected);
                }
                other => panic!("expected an anchored inventory batch, got {other:?}"),
            }
        }
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::SyncEvent::Done(None))
        ));
        let requests = client.transport().requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0]["methodCalls"][0][1]["position"], 0);
        assert_eq!(requests[2]["methodCalls"][0][1]["anchor"], "M1");
        assert_eq!(requests[2]["methodCalls"][0][1]["anchorOffset"], 1);
    }

    /// A positional partition cannot inherit the preceding partition's
    /// anchor, so accepting it would reopen the silent-skip window.
    #[tokio::test]
    async fn positional_page_inventory_is_refused_without_sending() {
        let client = scripted_client([
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q1", "position": 2, "ids": ["M3"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "primary", "state": "email-s", "list": [
                    {"id": "M3", "size": 10, "mailboxIds": {}, "keywords": {}}
                ], "notFound": []},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q1", "position": 3, "ids": ["M4", "M5"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "primary", "state": "email-s", "list": [
                    {"id": "M4", "size": 10, "mailboxIds": {}, "keywords": {}},
                    {"id": "M5", "size": 10, "mailboxIds": {}, "keywords": {}}
                ], "notFound": []},
                "s0"
            ])]),
        ]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let mut stream = crate::sync::inventory::stream_partition(
            primary,
            super::capabilities::CoreLimits {
                max_objects_in_get: 99,
                max_objects_in_set: 4,
            },
            CursorScope::Type(ObjectType::Email),
            bifrost_types::InventoryPartition::Page { from: 2, to: 5 },
            None,
        );

        let mut ids = Vec::new();
        while let Some(event) = stream.next().await {
            match event {
                bifrost_types::SyncEvent::Batch(batch) => {
                    ids.extend(batch.items.into_iter().map(|entry| entry.id.0));
                }
                bifrost_types::SyncEvent::Terminated(err) => {
                    assert!(matches!(
                        err.kind(),
                        bifrost_types::AccountErrorKind::Unsupported(
                            bifrost_types::AccountOperation::SyncInventory
                        )
                    ));
                    break;
                }
                other => panic!("unexpected page inventory event: {other:?}"),
            }
        }
        assert!(ids.is_empty());
        assert!(client.transport().requests().is_empty());
    }

    /// Refusal is independent of the scripted response shape: no positional
    /// partition may reach the wire and then claim complete coverage.
    #[tokio::test]
    async fn a_vanished_positional_window_is_refused_without_sending() {
        let client = scripted_client([
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q1", "position": 0, "ids": ["M1", "M2"]},
                "s0"
            ])]),
            // Both ids were deleted between the query and the get.
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "primary", "state": "email-s", "list": [],
                 "notFound": ["M1", "M2"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "primary", "queryState": "q1", "position": 2, "ids": ["M3"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "primary", "state": "email-s", "list": [
                    {"id": "M3", "size": 10, "mailboxIds": {}, "keywords": {}}
                ], "notFound": []},
                "s0"
            ])]),
        ]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let mut stream = crate::sync::inventory::stream_partition(
            primary,
            super::capabilities::CoreLimits {
                max_objects_in_get: 99,
                max_objects_in_set: 4,
            },
            CursorScope::Type(ObjectType::Email),
            bifrost_types::InventoryPartition::Page { from: 0, to: 2 },
            None,
        );

        let mut ids = Vec::new();
        while let Some(event) = stream.next().await {
            match event {
                bifrost_types::SyncEvent::Batch(batch) => {
                    ids.extend(batch.items.into_iter().map(|entry| entry.id.0));
                }
                bifrost_types::SyncEvent::Terminated(err) => {
                    assert!(matches!(
                        err.kind(),
                        bifrost_types::AccountErrorKind::Unsupported(
                            bifrost_types::AccountOperation::SyncInventory
                        )
                    ));
                    break;
                }
                other => panic!("unexpected page inventory event: {other:?}"),
            }
        }
        assert!(ids.is_empty());
        assert!(client.transport().requests().is_empty());
    }

    /// The consolidated inventory loop must still qualify foreign ids,
    /// blob ids, thread ids, and memberships with the owning account, and
    /// must still route errors through the shared-scope mapping. This is
    /// the positive half of the primary walk's bare-id assertion.
    #[tokio::test]
    async fn foreign_inventory_qualifies_every_id_through_the_shared_loop() {
        let client = scripted_client([
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "shared", "queryState": "q1", "position": 0, "ids": ["F1"]},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/get",
                {"accountId": "shared", "state": "email-s", "list": [
                    {"id": "F1", "blobId": "FB1", "threadId": "FT1", "size": 12,
                     "mailboxIds": {"inbox": true}, "keywords": {}}
                ], "notFound": []},
                "s0"
            ])]),
            method_reply(vec![json!([
                "Email/query",
                {"accountId": "shared", "queryState": "q1", "position": 1, "ids": []},
                "s0"
            ])]),
        ]);
        let owner = bifrost_types::MailboxId("shared".to_string());
        let mut stream = crate::sync::inventory::stream(
            crate::account::Account::new(client.clone(), JmapAccountId::new("shared")),
            super::capabilities::CoreLimits {
                max_objects_in_get: 4,
                max_objects_in_set: 4,
            },
            CursorScope::Folder(foreign::encode_foreign_account("shared")),
            Some(owner.clone()),
        );

        let entry = match stream.next().await {
            Some(bifrost_types::SyncEvent::Batch(mut batch)) => {
                assert_eq!(batch.items.len(), 1);
                batch.items.remove(0)
            }
            other => panic!("expected a foreign inventory batch, got {other:?}"),
        };
        assert_eq!(entry.id.0, foreign::encode_object("shared", "F1"));
        assert_eq!(
            entry.blob_id.as_ref().map(|blob| blob.0.clone()),
            Some(foreign::encode_object("shared", "FB1"))
        );
        assert_eq!(
            entry.thread_id.as_ref().map(|thread| thread.0.clone()),
            Some(foreign::encode_object("shared", "FT1"))
        );
        assert!(
            entry
                .memberships
                .contains(&bifrost_types::MembershipScope::Folder(
                    foreign::encode_foreign("shared", "inbox")
                )),
            "native memberships are re-encoded into the foreign namespace: {:?}",
            entry.memberships
        );
        assert!(
            entry
                .memberships
                .contains(&bifrost_types::MembershipScope::Mailbox(owner)),
            "the owner tag is appended: {:?}",
            entry.memberships
        );
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::SyncEvent::Done(None))
        ));
    }

    /// Empty additive flag operations are caller errors. The shared guard must
    /// reject them before routing targets or touching the wire.
    #[tokio::test]
    async fn an_empty_flag_op_is_rejected_before_wire_or_target_routing() {
        for op in [
            FlagOp::Add(HashSet::new()),
            FlagOp::Remove(HashSet::new()),
            FlagOp::Patch {
                add: HashSet::new(),
                remove: HashSet::new(),
            },
        ] {
            let client = scripted_client([]);
            let primary = client
                .primary_account::<capability::Mail>()
                .expect("primary mail account");
            let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
            // Deliberately mixed routing: a bare primary id and a
            // foreign-qualified one, which `mutation_stream` sends to
            // two different owner routes.
            let ids = [
                ObjectId("M1".to_string()),
                ObjectId(foreign::encode_object("shared", "M9")),
                ObjectId("M2".to_string()),
            ];
            let targets = Box::pin(futures::stream::iter(ids.clone()));
            let stream = crate::sync::mutation::set_flags(
                primary,
                Arc::new(HashMap::from([("shared".to_string(), shared)])),
                // A batch bound of one forces the per-owner flush path
                // rather than a single end-of-input drain.
                super::capabilities::CoreLimits {
                    max_objects_in_get: 1,
                    max_objects_in_set: 1,
                },
                // Deliberately empty: a skip must not consult, probe, or
                // populate the Email-state cache.
                Arc::new(tokio::sync::Mutex::new(HashMap::new())),
                targets,
                op.clone(),
                idempotency_key(),
            );
            let events: Vec<_> = stream.collect().await;

            assert!(
                client.transport().requests().is_empty(),
                "{op:?}: an empty flag op must never touch the wire"
            );
            assert_eq!(events.len(), 1, "{op:?}");
            assert!(matches!(
                &events[0],
                bifrost_types::SyncEvent::Terminated(error)
                    if matches!(
                        error.kind(),
                        bifrost_types::AccountErrorKind::Request(
                            bifrost_types::RequestErrorKind::Malformed
                        )
                    )
            ));
        }
    }

    /// The production factory still calls `connect()` and therefore
    /// hardwires `ReqwestTransport`; this test deliberately exercises the
    /// generic sync request helper below that boundary. The scripted seam
    /// records the full Email/set request and returns the same response shape
    /// bifrost-net gives the JMAP decoder.
    #[tokio::test]
    async fn foreign_bulk_flags_use_the_foreign_account_native_id_and_state() {
        let client = scripted_client([email_set_reply(
            "shared",
            "shared-state",
            "shared-next",
            "M9",
        )]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
        let foreign_id = ObjectId(foreign::encode_object("shared", "M9"));
        let states = Arc::new(tokio::sync::Mutex::new(HashMap::from([(
            "shared".to_string(),
            Some("shared-state".to_string()),
        )])));
        let targets = Box::pin(futures::stream::iter([foreign_id.clone()]));
        let mut stream = crate::sync::mutation::set_flags(
            primary,
            Arc::new(HashMap::from([("shared".to_string(), shared)])),
            super::capabilities::CoreLimits {
                max_objects_in_get: 1,
                max_objects_in_set: 1,
            },
            states,
            targets,
            FlagOp::Add(HashSet::from(["$seen".to_string()])),
            idempotency_key(),
        );

        match stream.next().await {
            Some(bifrost_types::SyncEvent::Batch(batch)) => match &batch.items[..] {
                [bifrost_types::ItemOutcome::Succeeded(success)] => {
                    assert_eq!(success.item.0, foreign_id.0);
                }
                other => panic!("expected a successful foreign mutation, got {other:?}"),
            },
            other => panic!("expected the foreign mutation batch, got {other:?}"),
        }
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::SyncEvent::Done(None))
        ));

        let requests = client.transport().requests();
        assert_eq!(requests.len(), 1);
        let call = &requests[0]["methodCalls"][0];
        assert_eq!(call[0], "Email/set");
        assert_eq!(call[1]["accountId"], "shared");
        assert!(call[1].get("ifInState").is_none());
        assert_eq!(call[1]["update"]["M9"]["keywords/$seen"], true);
        assert!(call[1]["update"].get(&foreign_id.0).is_none());
    }

    /// A thread target is expanded by a `Thread/get` before the mutation
    /// runs. When that lookup comes back empty the failure still belongs
    /// to the operation the caller asked for - the expansion is an
    /// implementation detail, and reporting `HydrateThread` hands the sync
    /// engine an operation it never issued (and the recovery derived for
    /// it).
    #[tokio::test]
    async fn a_missing_thread_fails_under_the_mutation_operation_not_hydration() {
        let client = scripted_client([method_reply(vec![json!([
            "Thread/get",
            {"accountId": "primary", "state": "t-1", "list": [], "notFound": ["T9"]},
            "s0"
        ])])]);
        let primary = JmapMailAccount::new(client.clone(), JmapAccountId::new("primary"));

        let error = crate::sync::pim::set_keyword(
            primary,
            state_map(&[("primary", "primary-state")]),
            "primary".to_string(),
            MutationTarget::Thread(bifrost_types::ThreadId("T9".to_string())),
            "$seen".to_string(),
            true,
        )
        .await
        .expect_err("a thread the server does not know cannot be expanded");

        assert_eq!(
            error.operation(),
            Some(bifrost_types::AccountOperation::SetKeyword)
        );
    }

    #[tokio::test]
    async fn foreign_single_message_keyword_uses_native_id_on_its_selected_account() {
        let client = scripted_client([email_set_reply(
            "shared",
            "shared-state",
            "shared-next",
            "M9",
        )]);
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
        let foreign_id = ObjectId(foreign::encode_object("shared", "M9"));
        let states = Arc::new(tokio::sync::Mutex::new(HashMap::from([(
            "shared".to_string(),
            Some("shared-state".to_string()),
        )])));

        crate::sync::pim::set_keyword(
            shared,
            states,
            "shared".to_string(),
            MutationTarget::Message(foreign_id.clone()),
            "$seen".to_string(),
            true,
        )
        .await
        .expect("foreign keyword mutation succeeds");

        let requests = client.transport().requests();
        assert_eq!(requests.len(), 1);
        let call = &requests[0]["methodCalls"][0];
        assert_eq!(call[0], "Email/set");
        assert_eq!(call[1]["accountId"], "shared");
        assert!(call[1].get("ifInState").is_none());
        assert_eq!(call[1]["update"]["M9"]["keywords/$seen"], true);
        assert!(call[1]["update"].get(&foreign_id.0).is_none());
    }

    /// The outcome this pins is the one that SUCCEEDS.
    ///
    /// JMAP ids are account-scoped, so a bare primary mailbox id addressed to
    /// a shared account does not 404 - it resolves to whatever mailbox that
    /// account happens to hold under the same id. The second armed reply is
    /// exactly that server: a shared account with its own "inbox",
    /// acknowledging the update of M9. So if the foreign target's move is
    /// allowed on the wire, the caller is told the move APPLIED while the
    /// message was filed into a container it never named. The primary
    /// sibling is here to pin the other half: one inexpressible target must
    /// not take an expressible one down with it.
    #[tokio::test]
    async fn a_foreign_move_into_a_primary_mailbox_id_never_reaches_the_shared_account() {
        let client = scripted_client([
            email_set_reply("primary", "primary-state", "primary-next", "m1"),
            // Only reachable if the defect is present, and then it is the
            // wrong-container success the fix exists to prevent.
            email_set_reply("shared", "shared-state", "shared-next", "M9"),
        ]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
        let foreign_id = ObjectId(foreign::encode_object("shared", "M9"));
        let states = state_map(&[("primary", "primary-state"), ("shared", "shared-state")]);
        let targets = Box::pin(futures::stream::iter([
            ObjectId("m1".to_string()),
            foreign_id.clone(),
        ]));

        let mut stream = crate::sync::mutation::move_to(
            primary,
            Arc::new(HashMap::from([("shared".to_string(), shared)])),
            super::capabilities::CoreLimits {
                max_objects_in_get: 1,
                max_objects_in_set: 1,
            },
            states,
            targets,
            MembershipScope::Mailbox(bifrost_types::MailboxId("inbox".to_string())),
            idempotency_key(),
        );

        match stream.next().await {
            Some(bifrost_types::SyncEvent::Batch(batch)) => match &batch.items[..] {
                [bifrost_types::ItemOutcome::Succeeded(success)] => {
                    assert_eq!(success.item.0, "m1");
                }
                other => panic!("expected the primary move to apply, got {other:?}"),
            },
            other => panic!("expected the primary move batch, got {other:?}"),
        }
        // The cross-account target fails locally, on the item lane, so its
        // expressible sibling is not taken down with it.
        match stream.next().await {
            Some(bifrost_types::SyncEvent::Batch(batch)) => match &batch.items[..] {
                [bifrost_types::ItemOutcome::Failed(failure)] => {
                    assert_eq!(failure.item.0, foreign_id.0);
                    assert_eq!(
                        failure.error.kind(),
                        &bifrost_types::AccountErrorKind::Request(
                            bifrost_types::RequestErrorKind::Malformed
                        )
                    );
                }
                other => panic!("expected the cross-account move to fail, got {other:?}"),
            },
            other => panic!("expected the rejection batch, got {other:?}"),
        }
        assert!(matches!(
            stream.next().await,
            Some(bifrost_types::SyncEvent::Done(None))
        ));

        let requests = client.transport().requests();
        assert_eq!(
            requests.len(),
            1,
            "only the primary target is expressible; nothing may be sent for the foreign one"
        );
        let call = &requests[0]["methodCalls"][0];
        assert_eq!(call[1]["accountId"], "primary");
        assert_eq!(
            call[1]["update"]["m1"]["mailboxIds"],
            json!({"inbox": true})
        );
        for request in &requests {
            assert_ne!(
                request["methodCalls"][0][1]["accountId"], "shared",
                "a primary mailbox id must never be resolved in a shared account's namespace"
            );
        }
    }

    /// The same-owner move is still expressible and still strips both
    /// qualifications, so the rejection above is a routing check and not a
    /// blanket refusal of foreign moves.
    #[tokio::test]
    async fn a_foreign_move_into_that_accounts_own_mailbox_uses_native_ids() {
        let client = scripted_client([email_set_reply(
            "shared",
            "shared-state",
            "shared-next",
            "M9",
        )]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
        let foreign_id = ObjectId(foreign::encode_object("shared", "M9"));
        let destination = foreign::encode_foreign("shared", "mbx-2");
        let states = state_map(&[("shared", "shared-state")]);
        let targets = Box::pin(futures::stream::iter([foreign_id.clone()]));

        let mut stream = crate::sync::mutation::move_to(
            primary,
            Arc::new(HashMap::from([("shared".to_string(), shared)])),
            super::capabilities::CoreLimits {
                max_objects_in_get: 1,
                max_objects_in_set: 1,
            },
            states,
            targets,
            MembershipScope::Mailbox(bifrost_types::MailboxId(destination.0)),
            idempotency_key(),
        );

        match stream.next().await {
            Some(bifrost_types::SyncEvent::Batch(batch)) => match &batch.items[..] {
                [bifrost_types::ItemOutcome::Succeeded(success)] => {
                    assert_eq!(success.item.0, foreign_id.0);
                }
                other => panic!("expected the foreign move to apply, got {other:?}"),
            },
            other => panic!("expected the foreign move batch, got {other:?}"),
        }

        let requests = client.transport().requests();
        assert_eq!(requests.len(), 1);
        let call = &requests[0]["methodCalls"][0];
        assert_eq!(call[1]["accountId"], "shared");
        assert_eq!(call[1]["ifInState"], "shared-state");
        assert_eq!(
            call[1]["update"]["M9"]["mailboxIds"],
            json!({"mbx-2": true})
        );
    }

    /// Single-item container membership has the same silent-wrong-container
    /// exposure as the bulk lane: the target selects the account, the
    /// container id is taken as given, and both resolve inside that one
    /// account. The armed reply is what a shared server holding its own
    /// "inbox" would answer, so under the defect this call returns `Ok`.
    #[tokio::test]
    async fn a_foreign_message_cannot_be_filed_into_a_primary_container_id() {
        let client = scripted_client([email_set_reply(
            "shared",
            "shared-state",
            "shared-next",
            "M9",
        )]);
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
        let foreign_id = ObjectId(foreign::encode_object("shared", "M9"));
        let states = state_map(&[("shared", "shared-state")]);

        let error = crate::sync::pim::add_to_container(
            shared,
            states,
            "shared".to_string(),
            MutationTarget::Message(foreign_id),
            ContainerId("inbox".to_string()),
        )
        .await
        .expect_err("a primary container id is not addressable in a shared account");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert!(
            client.transport().requests().is_empty(),
            "the rejection has to happen before the Email/set is sent"
        );
    }

    /// A short batch for a quiet owner must not wait for the whole input to
    /// end. `shared` contributes one target and then goes silent; the fix
    /// flushes it once a full batch worth of other input has gone by, so its
    /// request precedes the busy owner's rather than trailing everything.
    #[tokio::test]
    async fn a_partial_batch_for_one_owner_flushes_before_the_input_ends() {
        let client = scripted_client([
            email_set_reply("shared", "shared-state", "shared-next", "M9"),
            email_set_reply_ids("primary", "primary-state", "primary-b", &["m1", "m2"]),
            email_set_reply_ids("primary", "primary-b", "primary-c", &["m3"]),
        ]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
        let states = state_map(&[("primary", "primary-state"), ("shared", "shared-state")]);
        let targets = Box::pin(futures::stream::iter([
            ObjectId(foreign::encode_object("shared", "M9")),
            ObjectId("m1".to_string()),
            ObjectId("m2".to_string()),
            ObjectId("m3".to_string()),
        ]));

        let mut stream = crate::sync::mutation::set_flags(
            primary,
            Arc::new(HashMap::from([("shared".to_string(), shared)])),
            super::capabilities::CoreLimits {
                max_objects_in_get: 1,
                max_objects_in_set: 2,
            },
            states,
            targets,
            FlagOp::Add(HashSet::from(["$seen".to_string()])),
            idempotency_key(),
        );
        while let Some(event) = stream.next().await {
            assert!(
                !matches!(event, bifrost_types::SyncEvent::Terminated(_)),
                "the mutation stream must not terminate: {event:?}"
            );
        }

        let requests = client.transport().requests();
        let accounts: Vec<Value> = requests
            .iter()
            .map(|request| request["methodCalls"][0][1]["accountId"].clone())
            .collect();
        assert_eq!(
            accounts,
            vec![json!("shared"), json!("primary"), json!("primary")],
            "the quiet owner's short batch must not be held until end of input"
        );
    }

    #[tokio::test]
    async fn armed_open_script_fails_at_the_unexpected_request() {
        let client = scripted_client([open_reply("primary", "p")]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        seed_account_state(&primary)
            .await
            .expect("first scripted request");
        let error = seed_account_state(&primary)
            .await
            .expect_err("second unscripted request must fail");

        assert!(
            error
                .to_string()
                .contains("scripted JMAP transport exhausted at API request #2")
        );
        assert_eq!(client.transport().requests().len(), 2);
    }

    /// `maxCallsInRequest` is a hard limit. RFC 8620 §2 recommends
    /// servers allow at least 16, but a client that treats the
    /// recommendation as a floor hands a conservative server a batch it
    /// rejects wholesale - which fails a primary open and silently drops
    /// every foreign account.
    #[tokio::test]
    async fn a_session_that_forbids_two_calls_gets_one_request_per_probe() {
        let client = scripted_client_with_session(
            session_with_limits(1, 100_000),
            serial_open_replies("primary", "p"),
        );
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let seed = seed_account_state(&primary).await.expect("serial seed");

        assert_eq!(seed.0, "email-p");
        assert_eq!(seed.1, "mailbox-p");
        let requests = client.transport().requests();
        assert_eq!(requests.len(), 2, "one request per probe");
        for request in &requests {
            assert_eq!(request["methodCalls"].as_array().map(Vec::len), Some(1));
        }
        assert_eq!(requests[0]["methodCalls"][0][0], "Email/get");
        assert_eq!(requests[1]["methodCalls"][0][0], "Mailbox/get");
    }

    /// A session document with no `urn:ietf:params:jmap:core` capability
    /// block at all.
    fn session_without_core_capability() -> Session {
        serde_json::from_value(json!({
            "capabilities": {"urn:ietf:params:jmap:mail": {}},
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses")
    }

    /// An absent core capability advertises no call limit, so the probes
    /// must still go out - serially, since nothing licenses batching.
    /// The request builder must NOT read "no advertised limit" as "the
    /// limit is zero": a zero enforced here would fail every probe as a
    /// client bug, so any caller probing such a session would see
    /// `Request(Malformed)` / `ClientBug` instead of the
    /// `SyncState(CapabilityChanged)` / `RestartAccount` that tells the
    /// engine to reopen. (`open` itself no longer probes at all here -
    /// `validate_and_seed` refuses this session first - but the builder's
    /// reading of an absent limit is a separate contract, and the other
    /// probing callers still depend on it.)
    #[tokio::test]
    async fn an_absent_core_capability_still_probes_and_classifies_as_a_capability_change() {
        let client = scripted_client_with_session(
            session_without_core_capability(),
            serial_open_replies("primary", "p"),
        );
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let seed = seed_account_state(&primary)
            .await
            .expect("probes must not be refused for want of an advertised limit");

        assert_eq!(seed.0, "email-p");
        assert_eq!(client.transport().requests().len(), 2, "one per probe");

        let error = crate::sync::capabilities::build(
            &client.session(),
            crate::sync::capabilities::PimSupport {
                submission: false,
                max_delayed_send: 0,
                foreign_submission: false,
                vacation: false,
                quota: false,
                sieve: false,
                contacts: false,
                calendar: false,
            },
        )
        .expect_err("a session with no core capability is refused");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::SyncState(
                bifrost_types::SyncStateErrorKind::CapabilityChanged
            )
        );
    }

    /// `maxCallsInRequest: 0` is an advertised limit that is not usable.
    /// It is a `Protocol(ContractViolation)` decided by the session
    /// validation, and enforcing it in the request builder would preempt
    /// that ruling with `Request(Malformed)` / `ClientBug` raised from the
    /// probe. (`open` refuses this session before probing; the builder's
    /// behaviour is pinned here for every other probing caller.)
    #[tokio::test]
    async fn a_zero_call_limit_still_probes_and_classifies_as_a_contract_violation() {
        let client = scripted_client_with_session(
            session_with_limits(0, 100_000),
            serial_open_replies("primary", "p"),
        );
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let seed = seed_account_state(&primary)
            .await
            .expect("a zero limit must not brick the probe");

        assert_eq!(seed.1, "mailbox-p");
        assert_eq!(client.transport().requests().len(), 2, "one per probe");

        let error = crate::sync::capabilities::build(
            &client.session(),
            crate::sync::capabilities::PimSupport {
                submission: false,
                max_delayed_send: 0,
                foreign_submission: false,
                vacation: false,
                quota: false,
                sieve: false,
                contacts: false,
                calendar: false,
            },
        )
        .expect_err("a zero-valued core limit is refused");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
    }

    /// `maxSizeRequest` is the other hard limit, and it can fall between
    /// the individual probes and their batch - so the call-count check
    /// alone is not enough.
    #[tokio::test]
    async fn a_size_limit_between_one_probe_and_the_batch_gets_one_request_per_probe() {
        // Big enough for either single probe, too small for both.
        let one_probe = {
            let client = scripted_client([open_reply("primary", "p")]);
            let primary = client
                .primary_account::<capability::Mail>()
                .expect("primary mail account");
            let mut request = primary.build();
            let (email, _) = open_probe_methods();
            request.call(email).expect("probe encodes");
            request.encoded_len().expect("probe encodes")
        };
        let client = scripted_client_with_session(
            session_with_limits(16, one_probe),
            serial_open_replies("primary", "p"),
        );
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let seed = seed_account_state(&primary).await.expect("serial seed");

        assert_eq!(seed.0, "email-p");
        let requests = client.transport().requests();
        assert_eq!(requests.len(), 2, "one request per probe");
    }

    /// A 429 never reaches a caller as `bifrost_net::Error::Status`:
    /// bifrost-net retries it and surfaces `RateLimited` with the final
    /// response preserved. The JMAP boundary has to lift the problem
    /// document back out of that evidence, or the provider's own
    /// explanation of the throttle is lost.
    #[tokio::test]
    async fn a_429_arrives_as_rate_limited_with_its_problem_document() {
        let client = scripted_client([ScriptedReply::status(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            Bytes::from_static(
                br#"{"type":"urn:ietf:params:jmap:error:limit","limit":"concurrentRequests"}"#,
            ),
        )
        .header("retry-after", "120")]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let error = seed_account_state(&primary)
            .await
            .expect_err("429 is a typed error, never a JMAP response");

        let crate::Error::Problem {
            transport: Some(transport),
            ..
        } = error
        else {
            panic!("a 429 carrying a problem document is a Problem error");
        };
        assert!(
            matches!(
                transport.net,
                Some(bifrost_net::Error::RateLimited {
                    retry_after: Some(_),
                    ..
                })
            ),
            "production never produces Error::Status for a 429"
        );
    }

    /// A retryable 5xx is `RetryBudgetExhausted`, not `Status`, for the
    /// same reason.
    #[tokio::test]
    async fn a_5xx_arrives_as_retry_budget_exhausted() {
        let client = scripted_client([ScriptedReply::status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from_static(br#"{"type":"about:blank","status":503}"#),
        )]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let error = seed_account_state(&primary)
            .await
            .expect_err("503 is a typed error, never a JMAP response");

        let crate::Error::Problem {
            transport: Some(transport),
            ..
        } = error
        else {
            panic!("a 503 carrying a problem document is a Problem error");
        };
        assert!(
            matches!(
                transport.net,
                Some(bifrost_net::Error::RetryBudgetExhausted { .. })
            ),
            "production never produces Error::Status for a 5xx"
        );
    }

    /// A status the retry policy does not cover is the one shape that
    /// really is `Error::Status`.
    #[tokio::test]
    async fn a_non_retryable_4xx_arrives_as_a_status_error() {
        let client = scripted_client([ScriptedReply::status(
            reqwest::StatusCode::BAD_REQUEST,
            Bytes::from_static(br#"{"type":"urn:ietf:params:jmap:error:notRequest"}"#),
        )]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let error = seed_account_state(&primary)
            .await
            .expect_err("400 is a typed error, never a JMAP response");

        let crate::Error::Problem {
            transport: Some(transport),
            ..
        } = error
        else {
            panic!("a 400 carrying a problem document is a Problem error");
        };
        assert!(matches!(
            transport.net,
            Some(bifrost_net::Error::Status {
                code: reqwest::StatusCode::BAD_REQUEST,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn scripted_typed_error_reaches_the_sync_request_unchanged() {
        let client = scripted_client([ScriptedReply::error(TransportError::new(
            "scripted connection reset",
        ))]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let error = seed_account_state(&primary)
            .await
            .expect_err("scripted transport error");
        assert!(matches!(error, crate::Error::Transport(_)));
    }

    #[tokio::test]
    async fn a_revoked_foreign_share_becomes_a_terminal_named_skip() {
        let client = scripted_client([method_error("shared", "forbidden")]);
        let shared = JmapMailAccount::new(client, JmapAccountId::new("shared"));

        let Err(skip) = seed_foreign_account_or_skip("shared", &shared).await else {
            panic!("a forbidden foreign probe must become a skip, not a seed");
        };

        assert!(
            matches!(&skip.scope, bifrost_types::ErrorScope::Mailbox { id } if id.0 == "shared"),
            "the skip names the degraded foreign account: {:?}",
            skip.scope
        );
        assert!(
            matches!(
                skip.error.recovery(),
                bifrost_types::RecoveryClass::NoPermission { .. }
            ),
            "a revoked grant classifies terminal NoPermission: {:?}",
            skip.error.recovery()
        );
    }

    #[tokio::test]
    async fn a_retry_exhausted_foreign_probe_becomes_a_retryable_skip() {
        let client = scripted_client([ScriptedReply::status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from_static(br#"{"type":"about:blank","status":503}"#),
        )]);
        let shared = JmapMailAccount::new(client, JmapAccountId::new("shared"));

        let Err(skip) = seed_foreign_account_or_skip("shared", &shared).await else {
            panic!("a transient foreign probe must not erase the share silently");
        };

        assert!(
            matches!(&skip.scope, bifrost_types::ErrorScope::Mailbox { id } if id.0 == "shared"),
            "the skip names the degraded foreign account: {:?}",
            skip.scope
        );
        assert!(
            skip.error.recovery().is_retryable(),
            "retry exhaustion from bifrost-net stays retryable on the skip \
             lane, so a consumer knows a reopen can heal it"
        );
    }

    #[test]
    fn configured_email_uses_basic_email_username_only() {
        let basic = JmapCredentials::Basic {
            username: "Ada@Example.Test".to_string(),
            password: "secret".to_string(),
        };
        assert_eq!(basic.configured_email(), Some("Ada@Example.Test"));

        let non_email = JmapCredentials::Basic {
            username: "ada".to_string(),
            password: "secret".to_string(),
        };
        assert_eq!(non_email.configured_email(), None);

        let bearer = JmapCredentials::bearer("token");
        assert_eq!(bearer.configured_email(), None);
    }

    #[tokio::test]
    async fn bearer_source_threads_token_source() {
        use crate::client::Authorization;
        use bifrost_net::AccessToken;

        let source = StaticTokenSource::new("old-token", None);
        let creds = JmapCredentials::bearer_source(Arc::new(source.clone()));
        // The shared source threads through to the client credential and
        // the account-net token source unchanged.
        let client_creds = creds.into_client_credentials();
        let authorization = Authorization::from_credentials_for_test(client_creds);
        let threaded = authorization.account_token_source();
        assert_eq!(
            threaded
                .current()
                .await
                .expect("static source infallible")
                .as_str(),
            "old-token"
        );
        // A token rotated on the original source is visible through the
        // threaded source with no reopen.
        source.set(AccessToken::new("new-token", None));
        assert_eq!(
            threaded
                .current()
                .await
                .expect("static source infallible")
                .as_str(),
            "new-token"
        );
    }

    #[test]
    fn push_email_alias_normalizes_and_deduplicates() {
        let mut emails = Vec::new();

        push_email_alias(&mut emails, "Ada@Example.Test");
        push_email_alias(&mut emails, "ada@example.test");
        push_email_alias(&mut emails, "not-an-email");

        assert_eq!(emails, vec!["ada@example.test"]);
    }

    // ---------------------------------------------------------------
    // Foreign thread-id qualification (nc-7).
    //
    // A foreign thread id is owner-qualified in the object namespace, the
    // same way its account's `Email` ids and `blobId`s already were. Every
    // thread-keyed door decodes that qualification and runs its
    // accountId-scoped `Thread/get` - plus the mutation that follows -
    // against the OWNING account. The doubles below arrange the loud
    // failure deliberately: the primary handle is always supplied
    // alongside the share, and every assertion names the accountId that
    // actually went on the wire, so a fallback to the primary shows up as
    // a failed assertion rather than a quietly passing test.
    // ---------------------------------------------------------------

    fn thread_get_reply(account_id: &str, thread_id: &str, email_ids: &[&str]) -> ScriptedReply {
        method_reply(vec![json!([
            "Thread/get",
            {"accountId": account_id, "state": "t-1", "list": [
                {"id": thread_id, "emailIds": email_ids}
            ], "notFound": []},
            "s0"
        ])])
    }

    /// One hydrated message inside a thread: enough properties for
    /// `email_to_message` to fill the id, thread id, and containers.
    fn thread_email_get_reply(account_id: &str, id: &str, thread_id: &str) -> ScriptedReply {
        method_reply(vec![json!([
            "Email/get",
            {"accountId": account_id, "state": "email-1", "list": [
                {"id": id, "threadId": thread_id, "blobId": "B1", "size": 10,
                 "mailboxIds": {"inbox": true}, "keywords": {}}
            ], "notFound": []},
            "s0"
        ])])
    }

    fn foreign_thread(account_id: &str, native: &str) -> bifrost_types::ThreadId {
        bifrost_types::ThreadId(foreign::encode_object(account_id, native))
    }

    fn foreign_map(
        account_id: &str,
        account: JmapMailAccount<ScriptedTransport>,
    ) -> Arc<HashMap<String, JmapMailAccount<ScriptedTransport>>> {
        Arc::new(HashMap::from([(account_id.to_string(), account)]))
    }

    /// A stale (v1) cursor must be refused at the `changes_stream` DOOR,
    /// not merely inside the cursor decoder: the door is what the engine
    /// calls, and what it yields is what drives the recovery. The v1
    /// encoding minted foreign thread ids bare, so resuming one would skip
    /// the inventory pass that re-mints them - reseeding is the migration.
    #[tokio::test]
    async fn a_v1_cursor_is_refused_at_the_changes_door_before_any_request() {
        // No replies armed: reaching the wire at all is a failure.
        let client = scripted_client([]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let scope = CursorScope::Type(ObjectType::Email);
        let mut cursor = state::cursor_for_scope(scope.clone(), "s1").expect("scope encodes");
        cursor.server_state.envelope_version = 1;

        let mut stream = crate::sync::changes::stream(
            primary,
            "primary".to_string(),
            super::capabilities::CoreLimits {
                max_objects_in_get: 4,
                max_objects_in_set: 4,
            },
            cursor,
            None,
            state_map(&[("primary", "s1")]),
            state_map(&[("primary", "s1")]),
        );

        match stream.next().await {
            Some(bifrost_types::SyncEvent::Terminated(err)) => {
                assert_eq!(
                    err.kind(),
                    &bifrost_types::AccountErrorKind::SyncState(
                        bifrost_types::SyncStateErrorKind::SchemaIncompatible
                    )
                );
                // The directive is the point: the engine drops every
                // durable cursor and re-establishes through inventory,
                // which re-mints the ids under the new encoding.
                assert_eq!(
                    err.recovery(),
                    &bifrost_types::RecoveryClass::Engine(
                        bifrost_types::EngineDirective::SchemaIncompatible
                    )
                );
            }
            other => panic!("expected a schema-incompatible termination, got {other:?}"),
        }
        assert!(
            client.transport().requests().is_empty(),
            "a stale cursor must not reach Email/changes at all"
        );
    }

    /// A mis-keyed cursor row - wrong protocol tag, or a payload whose
    /// scope disagrees with the `ChangeCursor` it rode in on - is a
    /// consumer/store bug, not schema drift. It must classify
    /// `CursorInvalid` and derive `Engine(RestartScope(scope))`: heal the
    /// one bogus row, not the account-wide schema clear that now also
    /// drops every backfill checkpoint and forces a full re-hydration.
    #[tokio::test]
    async fn a_mis_keyed_cursor_restarts_its_scope_instead_of_reseeding_the_account() {
        let limits = super::capabilities::CoreLimits {
            max_objects_in_get: 4,
            max_objects_in_set: 4,
        };
        let scope = CursorScope::Type(ObjectType::Email);

        // Wrong protocol tag on the envelope.
        let mut foreign_protocol =
            state::cursor_for_scope(scope.clone(), "s1").expect("scope encodes");
        foreign_protocol.server_state.protocol = bifrost_types::ProtocolKind::Gmail;

        // Payload scope disagreeing with the ChangeCursor scope.
        let mut scope_mismatch =
            state::cursor_for_scope(CursorScope::Type(ObjectType::Mailbox), "s1")
                .expect("scope encodes");
        scope_mismatch.scope = scope.clone();

        for cursor in [foreign_protocol, scope_mismatch] {
            // No replies armed: reaching the wire at all is a failure.
            let client = scripted_client([]);
            let primary = client
                .primary_account::<capability::Mail>()
                .expect("primary mail account");
            let mut stream = crate::sync::changes::stream(
                primary,
                "primary".to_string(),
                limits,
                cursor,
                None,
                state_map(&[("primary", "s1")]),
                state_map(&[("primary", "s1")]),
            );
            match stream.next().await {
                Some(bifrost_types::SyncEvent::Terminated(err)) => {
                    assert_eq!(
                        err.kind(),
                        &bifrost_types::AccountErrorKind::SyncState(
                            bifrost_types::SyncStateErrorKind::CursorInvalid
                        )
                    );
                    assert_eq!(
                        err.recovery(),
                        &bifrost_types::RecoveryClass::Engine(
                            bifrost_types::EngineDirective::RestartScope(scope.clone())
                        )
                    );
                }
                other => panic!("expected a cursor-invalid termination, got {other:?}"),
            }
            assert!(
                client.transport().requests().is_empty(),
                "a mis-keyed cursor must not reach Email/changes at all"
            );
        }
    }

    #[tokio::test]
    async fn a_foreign_thread_hydration_runs_in_the_share_and_requalifies_its_members() {
        let client = scripted_client([
            thread_get_reply("shared", "T9", &["M9"]),
            thread_email_get_reply("shared", "M9", "T9"),
        ]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));
        let thread = foreign_thread("shared", "T9");

        let hydration = crate::sync::pim::thread_hydrate(
            primary,
            foreign_map("shared", shared),
            thread.clone(),
        )
        .await
        .expect("the share answers its own thread");

        // The hydration echoes the caller's id verbatim, so a round trip
        // through this door is byte-stable.
        assert_eq!(hydration.id, thread);
        let message = match &hydration.messages[..] {
            [message] => message,
            other => panic!("expected the one thread member, got {other:?}"),
        };
        // Every member id comes back in the share's namespace so the
        // consumer's follow-on blob read / container join / per-message
        // mutation stays in the same account.
        assert_eq!(message.id.0, foreign::encode_object("shared", "M9"));
        assert_eq!(
            message.thread_id.as_ref().map(|id| id.0.as_str()),
            Some(foreign::encode_object("shared", "T9").as_str())
        );
        assert_eq!(
            message.containers.first().map(|id| id.0.as_str()),
            Some(foreign::encode_foreign("shared", "inbox").0.as_str())
        );

        let requests = client.transport().requests();
        assert_eq!(requests.len(), 2);
        let thread_get = &requests[0]["methodCalls"][0];
        assert_eq!(thread_get[0], "Thread/get");
        assert_eq!(
            thread_get[1]["accountId"], "shared",
            "a foreign thread must never be expanded against the primary account"
        );
        assert_eq!(thread_get[1]["ids"][0], "T9");
        assert_eq!(requests[1]["methodCalls"][0][1]["accountId"], "shared");
    }

    /// A bare thread id asserts primary ownership, and that assertion must
    /// keep holding: registering a share changes nothing for it.
    #[tokio::test]
    async fn a_bare_thread_id_still_hydrates_against_the_primary_account() {
        let client = scripted_client([
            thread_get_reply("primary", "T9", &["M9"]),
            thread_email_get_reply("primary", "M9", "T9"),
        ]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));

        let hydration = crate::sync::pim::thread_hydrate(
            primary,
            foreign_map("shared", shared),
            bifrost_types::ThreadId("T9".to_string()),
        )
        .await
        .expect("the primary answers its own thread");

        assert_eq!(hydration.id.0, "T9");
        let message = &hydration.messages[0];
        // Nothing is qualified on the primary route: one logical object,
        // one wire form.
        assert_eq!(message.id.0, "M9");
        assert_eq!(
            message.thread_id.as_ref().map(|id| id.0.as_str()),
            Some("T9")
        );
        assert_eq!(message.containers[0].0, "inbox");

        let requests = client.transport().requests();
        assert_eq!(requests[0]["methodCalls"][0][1]["accountId"], "primary");
        assert_eq!(requests[0]["methodCalls"][0][1]["ids"][0], "T9");
    }

    /// `set_keyword` / `set_is_read` / `set_importance` on a thread target
    /// all funnel through `resolve_target` + `send_email_set_with_retry`,
    /// so pinning one pins the expansion contract for all three: the
    /// `Thread/get` carries the NATIVE id inside the share, and the
    /// `Email/set` that follows names the same account.
    #[tokio::test]
    async fn a_foreign_thread_keyword_expands_and_writes_inside_the_share() {
        let client = scripted_client([
            thread_get_reply("shared", "T9", &["M9"]),
            email_set_reply("shared", "shared-state", "shared-next", "M9"),
        ]);
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));

        crate::sync::pim::set_keyword(
            shared,
            state_map(&[("primary", "primary-state"), ("shared", "shared-state")]),
            "shared".to_string(),
            MutationTarget::Thread(foreign_thread("shared", "T9")),
            "$seen".to_string(),
            true,
        )
        .await
        .expect("the share accepts a keyword on its own thread");

        let requests = client.transport().requests();
        assert_eq!(requests.len(), 2);
        let thread_get = &requests[0]["methodCalls"][0];
        assert_eq!(thread_get[0], "Thread/get");
        assert_eq!(thread_get[1]["accountId"], "shared");
        assert_eq!(
            thread_get[1]["ids"][0], "T9",
            "the qualification is stripped only for the owning account"
        );
        let set = &requests[1]["methodCalls"][0];
        assert_eq!(set[0], "Email/set");
        assert_eq!(set[1]["accountId"], "shared");
        assert!(set[1].get("ifInState").is_none());
        assert_eq!(set[1]["update"]["M9"]["keywords/$seen"], true);
    }

    /// The collision the whole change exists to prevent, pinned at the
    /// wire.
    ///
    /// A thread id qualified for an account this session no longer holds
    /// falls back to the primary route - and it must stay LITERAL there.
    /// Stripped to its bare native id it would be indistinguishable from a
    /// primary thread id, and the primary `Thread/get` would happily
    /// resolve an unrelated same-id thread whose messages the mutation
    /// then rewrites. What pins it is the id that actually goes on the
    /// wire: the primary account here HOLDS a thread called `T9` (with
    /// entirely different messages), so if the qualification is ever
    /// stripped on the primary route the recorded `ids[0]` becomes `T9`
    /// and this test fails - loudly - instead of quietly mutating a thread
    /// the caller never named. Only one reply is armed, so a mutation
    /// reaching the wire also fails rather than passing.
    #[tokio::test]
    async fn a_departed_shares_thread_id_can_never_resolve_a_primary_thread() {
        let client = scripted_client([method_reply(vec![json!([
            "Thread/get",
            {"accountId": "primary", "state": "t-1", "list": [],
             "notFound": [foreign::encode_object("revoked", "T9")]},
            "s0"
        ])])]);
        let primary = client
            .primary_account::<capability::Mail>()
            .expect("primary mail account");

        let error = crate::sync::pim::set_keyword(
            primary,
            state_map(&[("primary", "primary-state")]),
            "primary".to_string(),
            MutationTarget::Thread(foreign_thread("revoked", "T9")),
            "$seen".to_string(),
            true,
        )
        .await
        .expect_err("an unreachable thread must fail, not hit a same-id primary thread");
        assert_eq!(
            error.operation(),
            Some(bifrost_types::AccountOperation::SetKeyword)
        );

        let requests = client.transport().requests();
        assert_eq!(
            requests.len(),
            1,
            "the mutation must never be sent: {requests:?}"
        );
        assert_eq!(
            requests[0]["methodCalls"][0][1]["ids"][0],
            foreign::encode_object("revoked", "T9"),
            "the id stays literal on the primary route, so it names no real thread"
        );
    }

    #[tokio::test]
    async fn a_foreign_thread_move_files_into_the_shares_own_mailbox() {
        let client = scripted_client([
            thread_get_reply("shared", "T9", &["M9"]),
            email_set_reply("shared", "shared-state", "shared-next", "M9"),
        ]);
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));

        crate::sync::pim::move_thread(
            shared,
            state_map(&[("shared", "shared-state")]),
            "shared".to_string(),
            foreign_thread("shared", "T9"),
            ContainerId(foreign::encode_foreign("shared", "archive").0),
            None,
        )
        .await
        .expect("a share-local move is expressible");

        let requests = client.transport().requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["methodCalls"][0][1]["accountId"], "shared");
        assert_eq!(requests[0]["methodCalls"][0][1]["ids"][0], "T9");
        let set = &requests[1]["methodCalls"][0];
        assert_eq!(set[1]["accountId"], "shared");
        assert_eq!(
            set[1]["update"]["M9"]["mailboxIds/archive"], true,
            "the container qualification is stripped for the owning account"
        );
    }

    /// A foreign thread paired with a PRIMARY container id is
    /// inexpressible: `Email/set` names one accountId and resolves both
    /// operands inside it, so a bare "archive" addressed to the share
    /// files the thread into whatever mailbox the share holds under that
    /// id, with no error. Before thread ids were qualified this guard
    /// could not fire at all, because a thread always claimed primary
    /// ownership.
    #[tokio::test]
    async fn a_foreign_thread_move_into_a_primary_container_never_reaches_the_wire() {
        let client = scripted_client([
            thread_get_reply("shared", "T9", &["M9"]),
            email_set_reply("shared", "shared-state", "shared-next", "M9"),
        ]);
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));

        let error = crate::sync::pim::move_thread(
            shared,
            state_map(&[("shared", "shared-state")]),
            "shared".to_string(),
            foreign_thread("shared", "T9"),
            ContainerId("archive".to_string()),
            None,
        )
        .await
        .expect_err("a cross-account destination is not expressible");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert!(
            client.transport().requests().is_empty(),
            "the owner disagreement is caught before anything is sent"
        );
    }

    /// `delete_thread` is the sharp edge: routed to the wrong account it
    /// destroys an unrelated thread's messages. The Trash it resolves must
    /// also be the SHARE's Trash, minted in the share's namespace so the
    /// cross-account guard sees one account rather than a bare id that
    /// reads as primary.
    #[tokio::test]
    async fn a_foreign_thread_delete_resolves_and_trashes_inside_the_share() {
        let client = scripted_client([
            method_reply(vec![json!([
                "Mailbox/get",
                {"accountId": "shared", "state": "mailbox-s", "list": [
                    {"id": "shared-trash", "name": "Trash", "role": "trash"}
                ], "notFound": []},
                "s0"
            ])]),
            thread_get_reply("shared", "T9", &["M9"]),
            email_set_reply("shared", "shared-state", "shared-next", "M9"),
        ]);
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));

        crate::sync::pim::delete_thread(
            shared,
            state_map(&[("shared", "shared-state")]),
            "shared".to_string(),
            foreign_thread("shared", "T9"),
            None,
        )
        .await
        .expect("the share trashes its own thread");

        let requests = client.transport().requests();
        assert_eq!(requests.len(), 3);
        for request in &requests {
            assert_eq!(
                request["methodCalls"][0][1]["accountId"], "shared",
                "every leg of the delete stays in the share: {request}"
            );
        }
        assert_eq!(requests[1]["methodCalls"][0][1]["ids"][0], "T9");
        assert_eq!(
            requests[2]["methodCalls"][0][1]["update"]["M9"]["mailboxIds/shared-trash"],
            true
        );
    }

    /// The DESTRUCTIVE branch of the same door: a foreign thread whose
    /// `current` container already IS the share's Trash must compare
    /// equal against the owner-qualified Trash the delete resolves (one
    /// namespace, not a bare id that reads as primary) and then DESTROY
    /// inside the share - the sharpest consequence a routing regression
    /// has.
    #[tokio::test]
    async fn a_foreign_thread_already_in_the_shares_trash_is_destroyed_in_the_share() {
        let client = scripted_client([
            method_reply(vec![json!([
                "Mailbox/get",
                {"accountId": "shared", "state": "mailbox-s", "list": [
                    {"id": "shared-trash", "name": "Trash", "role": "trash"}
                ], "notFound": []},
                "s0"
            ])])
            .for_account("shared"),
            thread_get_reply("shared", "T9", &["M9"]).for_account("shared"),
            method_reply(vec![json!([
                "Email/set",
                {
                    "accountId": "shared",
                    "oldState": "shared-state",
                    "newState": "shared-next",
                    "destroyed": ["M9"],
                    "notDestroyed": {}
                },
                "s0"
            ])])
            .for_account("shared"),
        ]);
        let shared = JmapMailAccount::new(client.clone(), JmapAccountId::new("shared"));

        crate::sync::pim::delete_thread(
            shared,
            state_map(&[("shared", "shared-state")]),
            "shared".to_string(),
            foreign_thread("shared", "T9"),
            // The caller's container id arrives owner-qualified, the way
            // `containers_list` mints it for a share.
            Some(ContainerId(foreign::encode_object(
                "shared",
                "shared-trash",
            ))),
        )
        .await
        .expect("the share destroys its own trashed thread");

        let requests = client.transport().requests();
        assert_eq!(requests.len(), 3);
        for request in &requests {
            assert_eq!(
                request["methodCalls"][0][1]["accountId"], "shared",
                "every leg of the destroy stays in the share: {request}"
            );
        }
        assert_eq!(requests[1]["methodCalls"][0][1]["ids"][0], "T9");
        let set_args = &requests[2]["methodCalls"][0][1];
        assert_eq!(set_args["destroy"][0], "M9");
        assert!(
            set_args.get("update").is_none(),
            "already-in-Trash must destroy, not move: {set_args}"
        );
    }
}
