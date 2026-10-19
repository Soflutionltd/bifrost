//! Account configuration and open: pool construction, DAV attach, ID probe,
//! QRESYNC negotiation, folder LIST and shared-folder discovery.
//!
//! Audit boundary: not line-audited by the 2026-09-04 bug hunt, whose depth
//! was spent on the driver, framing, pool, push, auth, change strategies,
//! mutations, inventory, hydration and cursor envelope. An absence of
//! findings in this module is an absence of reading. Noted so a later
//! auditor knows where coverage stops.

use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{
    Account, AccountError, AccountFactory, AccountFuture, AccountId, OpenedAccount, SkippedScope,
};

use bifrost_caldav::{CalDavAccountFactory, CalDavConfig};
use bifrost_carddav::{CardDavAccountFactory, CardDavConfig};

use crate::connection::ImapConfig;
use crate::types::{
    AuthPolicy, Capability, Credentials, MailboxInfo, MailboxRights, ServerProfile,
};

use super::error::ImapErrorContext;
use super::sieve::ManageSieveConfig;
use super::submission::{SmtpSubmissionConfig, SubmissionTransport};
use super::{
    ImapAccount, ImapAccountParts, Pool, account_error_with, capabilities,
    folder_registry::FolderRegistry,
};
use bifrost_types::AccountOperation;

/// Account-boundary translation for every leg of `open` (connect,
/// AUTH, ID, QRESYNC, LIST). All four are tagged `Discover` because
/// they belong to a single discovery phase. The AUTH leg is genuinely
/// idempotent at the protocol level (re-authenticating produces the
/// same outcome), so an `InFlight` transport drop here is safe to
/// retry; the central recovery mapping picks `Retry::SameRequest` for
/// idempotent ops regardless. Splitting these into per-phase variants
/// would not change recovery for IMAP; it would only inflate the
/// `AccountOperation` enum surface.
fn discover_err(err: crate::Error) -> AccountError {
    account_error_with(err, ImapErrorContext::operation(AccountOperation::Discover))
}

/// Configuration used by `ImapAccountFactory`.
#[non_exhaustive]
#[derive(Clone)]
pub struct ImapAccountConfig {
    pub imap: ImapConfig,
    pub credentials: Credentials,
    pub auth_policy: AuthPolicy,
    pub pool_cap: usize,
    /// Maximum dedicated IDLE sessions used when the server lacks NOTIFY.
    pub idle_connection_budget: usize,
    pub idle_timeout: Duration,
    pub enable_qresync: bool,
    pub flag_sync_interval: Duration,
    pub deletion_check_interval: Duration,
    pub mutation_batch_size: usize,
    pub bandwidth_meter: Option<Arc<bifrost_net::BandwidthMeter>>,
    pub meter_sink: Option<Arc<dyn bifrost_net::MeterSink>>,
    pub sieve: Option<ManageSieveConfig>,
    pub carddav: Option<CardDavConfig>,
    pub caldav: Option<CalDavConfig>,
    pub submission: Option<SmtpSubmissionConfig>,
}

impl ImapAccountConfig {
    pub fn new(imap: ImapConfig, credentials: Credentials, auth_policy: AuthPolicy) -> Self {
        Self {
            imap,
            credentials,
            auth_policy,
            pool_cap: 4,
            idle_connection_budget: 4,
            idle_timeout: Duration::from_secs(29 * 60),
            enable_qresync: false,
            flag_sync_interval: Duration::from_secs(300),
            deletion_check_interval: Duration::from_secs(600),
            mutation_batch_size: 1024,
            bandwidth_meter: None,
            meter_sink: None,
            sieve: None,
            carddav: None,
            caldav: None,
            submission: None,
        }
    }

    pub fn with_bandwidth_meter(mut self, meter: Arc<bifrost_net::BandwidthMeter>) -> Self {
        self.bandwidth_meter = Some(meter);
        self
    }

    pub fn with_meter_sink(mut self, sink: Arc<dyn bifrost_net::MeterSink>) -> Self {
        self.meter_sink = Some(sink);
        self
    }

    pub fn with_manage_sieve(mut self, config: ManageSieveConfig) -> Self {
        self.sieve = Some(config);
        self
    }

    pub fn with_carddav(mut self, config: CardDavConfig) -> Self {
        self.carddav = Some(config);
        self
    }

    pub fn with_caldav(mut self, config: CalDavConfig) -> Self {
        self.caldav = Some(config);
        self
    }

    pub fn with_submission(mut self, config: SmtpSubmissionConfig) -> Self {
        self.submission = Some(config);
        self
    }
}

/// IMAP account factory sharing HTTP pools, host governors and account budgets.
///
/// Uses `Net::shared_default()` unless a transport is supplied with `with_net`.
/// Every account leg resolves its budget from that transport's registry.
pub struct ImapAccountFactory {
    cfg: Arc<ImapAccountConfig>,
    net: Option<bifrost_net::Net>,
}

impl ImapAccountFactory {
    pub fn new(config: ImapAccountConfig) -> Self {
        Self {
            cfg: Arc::new(config),
            net: None,
        }
    }

    /// Reuse this transport for composed DAV and its registry for every leg.
    /// Clones across opens retain the HTTP pool and per-host rate governor.
    #[must_use]
    pub fn with_net(mut self, net: bifrost_net::Net) -> Self {
        self.net = Some(net);
        self
    }

    fn resolve_net(&self, default: impl FnOnce() -> bifrost_net::Net) -> bifrost_net::Net {
        self.net.clone().unwrap_or_else(default)
    }

    // Both seams replace external I/O in tests; selection and account open
    // still run through the production path, including DAV discovery.
    fn open_using(
        &self,
        account_id: AccountId,
        default: impl FnOnce() -> bifrost_net::Net,
        connect: impl FnOnce(
            Arc<ImapAccountConfig>,
            Option<bifrost_net::MeterSinkHandle>,
            bifrost_net::BandwidthBudget,
        ) -> AccountFuture<Result<crate::ImapConnection, crate::Error>>,
    ) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let cfg = Arc::clone(&self.cfg);
        let net = Arc::new(self.resolve_net(default));
        let bandwidth_budget = net.bandwidth_registry().budget(account_id.clone());
        let meter = meter_handle(&cfg, account_id.clone());
        let connected = connect(Arc::clone(&cfg), meter.clone(), bandwidth_budget.clone());
        Box::pin(async move {
            let conn = connected.await.map_err(discover_err)?;

            let mut profile = conn.server_profile();
            let server_id = read_server_id(&conn, &cfg, &profile)
                .await
                .map_err(discover_err)?;
            let qresync = negotiate_qresync(&conn, &cfg, &profile, &server_id)
                .await
                .map_err(discover_err)?;
            profile = conn.server_profile();

            let folders = list_folders(&conn, &cfg, &profile)
                .await
                .map_err(discover_err)?;
            // Surface shared/other-user folders via NAMESPACE + ACL gating
            // (A5c). A NAMESPACE-less or personal-only server yields an
            // empty list; a single revoked prefix is skipped, not fatal.
            let shared = discover_shared_folders(&conn, &cfg, &profile).await;
            let foreign_namespaces_advertised = shared.foreign_namespaces_advertised;
            let shared = shared.entries;
            // Fail-soft: a DAV open failure degrades to IMAP-only for
            // this cycle instead of failing the whole IMAP account. The
            // degradation is recorded twice on purpose: as a `Warning`
            // surfaced through the first discovery stream (the live
            // signal), and as an open-time `SkippedScope` so the
            // engine's `open_skipped_scopes` lane reports the missing
            // sub-account with its classified error.
            let mut dav_degraded = Vec::new();
            let mut skipped_scopes = Vec::new();
            // Joined, not sequential: each DAV open is its own multi-round-trip
            // discovery against its own endpoint with its own client, so
            // awaiting them in turn made an IMAP open pay both latencies for no
            // reason. `into_attached` is applied afterwards in a fixed order so
            // the degradation and skipped-scope lanes stay deterministic - the
            // futures may finish in either order, the reporting may not.
            let (contacts, calendars) = futures::future::join(
                open_carddav(&cfg, account_id.clone(), Arc::clone(&net)),
                open_caldav(&cfg, account_id.clone(), Arc::clone(&net)),
            )
            .await;
            let contacts = contacts.into_attached(&mut dav_degraded, &mut skipped_scopes);
            let calendars = calendars.into_attached(&mut dav_degraded, &mut skipped_scopes);
            let contact_cursor_folders = dav_cursor_folders(contacts.as_ref()).await;
            let calendar_cursor_folders = dav_cursor_folders(calendars.as_ref()).await;
            let submission = open_submission(&cfg, meter.clone(), bandwidth_budget.clone())?;
            let caps = capabilities::build_capabilities(
                &profile,
                &folders,
                cfg.sieve.is_some(),
                contacts.as_ref().map(|c| c.capabilities()),
                calendars.as_ref().map(|c| c.capabilities()),
                submission.is_some(),
                foreign_namespaces_advertised,
            );
            let registry = Arc::new(FolderRegistry::from_lists(folders, shared));
            // Built AFTER the registry so the index can be made disjoint from
            // the real mailbox set: a DAV collection whose href happens to be
            // an IMAP mailbox name must not capture that mailbox's scope.
            let imap_mailboxes: std::collections::HashSet<String> = registry
                .entries()
                .into_iter()
                .map(|entry| entry.name.as_str().to_owned())
                .collect();
            let (dav_scopes, dav_scope_warnings) = super::DavScopeIndex::build(
                contact_cursor_folders,
                calendar_cursor_folders,
                &imap_mailboxes,
            );
            dav_degraded.extend(dav_scope_warnings);
            let data_cap = cfg.pool_cap.saturating_sub(1).max(1);
            let pool = Arc::new(Pool::new(
                Arc::clone(&cfg),
                conn,
                data_cap,
                meter,
                bandwidth_budget.clone(),
            ));
            let account = ImapAccount::new(ImapAccountParts {
                config: cfg,
                capabilities: caps,
                pool,
                folders: registry,
                qresync_enabled: qresync.enabled,
                qresync_negotiation_warning: qresync.warning,
                supports_notify: profile.supports_notify(),
                bandwidth_budget,
                contacts,
                calendars,
                dav_scopes,
                submission,
                dav_degraded,
            });
            Ok(OpenedAccount {
                account: Arc::new(account) as Arc<dyn Account>,
                skipped_scopes,
            })
        })
    }
}

impl AccountFactory for ImapAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        self.open_using(
            account_id,
            bifrost_net::Net::shared_default,
            |cfg, meter, budget| {
                Box::pin(async move {
                    cfg.imap
                        .connect_authenticated_metered(
                            &cfg.credentials,
                            &cfg.auth_policy,
                            meter,
                            Some(budget),
                        )
                        .await
                        .map(|(conn, _auth)| conn)
                })
            },
        )
    }
}

/// The `CursorScope::Folder` ids a composed sub-account discovers.
///
/// Kept as a `Vec` in discovery order so the ownership index's collision
/// warnings come out in a deterministic order.
async fn dav_cursor_folders(account: Option<&Arc<dyn Account>>) -> Vec<bifrost_types::FolderId> {
    use futures::StreamExt as _;

    let Some(account) = account else {
        return Vec::new();
    };
    let mut folders = Vec::new();
    let mut stream = account.discover_cursor_scopes();
    while let Some(event) = stream.next().await {
        if let bifrost_types::SyncEvent::Batch(batch) = event {
            folders.extend(batch.items.into_iter().filter_map(|scope| match scope {
                bifrost_types::CursorScope::Folder(folder) => Some(folder),
                _ => None,
            }));
        }
    }
    folders
}

/// Outcome of attempting to attach a composed DAV sub-account. Never an
/// `Err`: a configured-but-failed open degrades to IMAP-only for this
/// open cycle rather than failing the whole IMAP account (brick 5).
enum DavAttach {
    /// Opened and attached.
    Attached {
        account: Arc<dyn Account>,
        /// Skips the DAV factory itself reported on a SUCCESSFUL open.
        ///
        /// A DAV open can succeed and still leave part of its surface
        /// uncovered: both DAV crates sync exactly one collection, so every
        /// further calendar or address book is enumerated and reachable
        /// through the PIM primitives while producing no inventory or change
        /// events. Those entries have to be forwarded onto the composed
        /// account's own lane, or composing DAV into IMAP silently discards
        /// the only report of the gap.
        skipped: Vec<SkippedScope>,
    },
    /// Not configured.
    None,
    /// Configured but open failed; attach IMAP-only this cycle.
    Degraded {
        warning: bifrost_types::Warning,
        /// The open-time skip entry (classified error + collection
        /// scope) recorded on `OpenedAccount::skipped_scopes`.
        skip: SkippedScope,
        // Retried on the engine's next reopen by design. Carried for
        // telemetry/clarity; the engine's reopen re-runs the open either
        // way.
        #[allow(dead_code)]
        transient: bool,
    },
}

impl DavAttach {
    /// Resolve to the optional sub-account handle, pushing any degraded
    /// warning into `degraded` (surfaced by the first discovery) and the
    /// matching skip entry into `skipped` (surfaced by
    /// `OpenedAccount::skipped_scopes`).
    fn into_attached(
        self,
        degraded: &mut Vec<bifrost_types::Warning>,
        skipped: &mut Vec<SkippedScope>,
    ) -> Option<Arc<dyn Account>> {
        match self {
            DavAttach::Attached {
                account,
                skipped: sub_skipped,
            } => {
                skipped.extend(sub_skipped);
                Some(account)
            }
            DavAttach::None => None,
            DavAttach::Degraded { warning, skip, .. } => {
                degraded.push(warning);
                skipped.push(skip);
                None
            }
        }
    }
}

/// Classify a DAV `open` outcome into an attach decision. A success
/// attaches; a failure degrades, routing through the central
/// `RecoveryClass` (read `reference/error-model.md`) to decide whether
/// the degradation is transient (retried on the engine's next reopen) or
/// a terminal config-fix.
fn classify_dav_open(
    result: Result<OpenedAccount, AccountError>,
    label: &str,
    scope: bifrost_types::ErrorScope,
) -> DavAttach {
    match result {
        // A successful open can still report skips: both DAV crates cover one
        // collection, so any further collection comes back on
        // `OpenedAccount::skipped_scopes`. Forward them onto the composed
        // account's lane rather than dropping them - this arm used to assume
        // the DAV factories always answered an empty skip lane, which silently
        // hid every unsynced calendar and address book behind a composed
        // account.
        Ok(opened) => DavAttach::Attached {
            account: opened.account,
            skipped: opened.skipped_scopes,
        },
        Err(error) => {
            // A retryable recovery class means the failure is transient
            // (transport blip, server unavailable, throttle); anything
            // terminal (auth lost, no permission, not found) needs an
            // operator config fix. Either way IMAP stays online.
            let transient = error.recovery().is_retryable();
            let kind = if transient {
                bifrost_types::WarningKind::Other
            } else {
                bifrost_types::WarningKind::OperatorAttentionNeeded
            };
            let warning = bifrost_types::Warning::support_only(
                kind,
                format!(
                    "{label} sub-account did not open ({}); continuing IMAP-only{}",
                    error.message_key(),
                    if transient {
                        ", will retry on reopen"
                    } else {
                        "; check DAV configuration"
                    }
                ),
            )
            .with_protocol_detail(bifrost_types::DiagnosticText::support_only(
                label.to_string(),
            ));
            let skip = SkippedScope { scope, error };
            DavAttach::Degraded {
                warning,
                skip,
                transient,
            }
        }
    }
}

async fn open_carddav(
    cfg: &ImapAccountConfig,
    account_id: AccountId,
    net: Arc<bifrost_net::Net>,
) -> DavAttach {
    let Some(config) = cfg.carddav.clone() else {
        return DavAttach::None;
    };
    classify_dav_open(
        CardDavAccountFactory::new(config)
            .with_net(net)
            .open(account_id)
            .await,
        "CardDAV",
        bifrost_types::ErrorScope::ContactCollection,
    )
}

fn open_submission(
    cfg: &ImapAccountConfig,
    meter: Option<bifrost_net::MeterSinkHandle>,
    bandwidth_budget: bifrost_net::BandwidthBudget,
) -> Result<Option<Arc<SubmissionTransport>>, AccountError> {
    let Some(config) = &cfg.submission else {
        return Ok(None);
    };
    // SMTP pools and every other account leg share this live budget.
    let transport = SubmissionTransport::build(config, &cfg.credentials, meter, bandwidth_budget)
        .map_err(discover_err)?;
    Ok(Some(Arc::new(transport)))
}

async fn open_caldav(
    cfg: &ImapAccountConfig,
    account_id: AccountId,
    net: Arc<bifrost_net::Net>,
) -> DavAttach {
    let Some(config) = cfg.caldav.clone() else {
        return DavAttach::None;
    };
    classify_dav_open(
        CalDavAccountFactory::new(config)
            .with_net(net)
            .open(account_id)
            .await,
        "CalDAV",
        bifrost_types::ErrorScope::CalendarCollection,
    )
}

fn meter_handle(
    cfg: &ImapAccountConfig,
    account_id: AccountId,
) -> Option<bifrost_net::MeterSinkHandle> {
    if let Some(meter) = &cfg.bandwidth_meter {
        return Some(bifrost_net::MeterSinkHandle::from_meter(
            Arc::clone(meter),
            account_id,
        ));
    }
    cfg.meter_sink
        .as_ref()
        .map(|sink| bifrost_net::MeterSinkHandle::new(Arc::clone(sink), account_id))
}

struct QresyncNegotiation {
    enabled: bool,
    warning: Option<String>,
}

async fn negotiate_qresync(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
    server_id: &[(String, Option<String>)],
) -> Result<QresyncNegotiation, crate::Error> {
    if !cfg.enable_qresync || !profile.supports_qresync() {
        return Ok(QresyncNegotiation {
            enabled: false,
            warning: None,
        });
    }
    if server_id_disables_qresync(server_id) {
        return Ok(QresyncNegotiation {
            enabled: false,
            warning: Some(
                "server ID matches a known broken QRESYNC implementation; continuing with CONDSTORE"
                    .to_owned(),
            ),
        });
    }
    // The same initialization every pool dial runs, so the primed member and
    // the dialed ones follow one rule. The account policy is seeded from this
    // member's COMMITTED profile, not from ENABLE's return value: every member
    // talks to the same server, and one that will not confirm QRESYNC on the
    // first connection is reported once below and left on CONDSTORE.
    super::pool::enable_qresync(conn, cfg.imap.command_timeout).await?;
    let confirmed = conn.server_profile().enabled("QRESYNC");
    Ok(QresyncNegotiation {
        enabled: confirmed,
        warning: (!confirmed).then(|| {
            "server advertised QRESYNC but ENABLE did not confirm it; continuing with CONDSTORE"
                .to_owned()
        }),
    })
}

async fn read_server_id(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
) -> Result<Vec<(String, Option<String>)>, crate::Error> {
    if !profile.supports(Capability::Id) {
        return Ok(Vec::new());
    }
    match conn
        .id(&[("name", Some("bifrost-imap"))], cfg.imap.command_timeout)
        .await
    {
        Ok(id) => Ok(id),
        Err(_) => Ok(Vec::new()),
    }
}

fn server_id_disables_qresync(server_id: &[(String, Option<String>)]) -> bool {
    server_id.iter().any(|(key, value)| {
        let Some(value) = value.as_deref() else {
            return false;
        };
        let name = value.trim();
        key.eq_ignore_ascii_case("name")
            && (name.eq_ignore_ascii_case("icloud") || name.eq_ignore_ascii_case("icloud imap"))
    })
}

/// Owner identity for the *shared* namespace: the namespace root minus its
/// trailing delimiter. All folders under a shared root collapse to this one
/// owner (there is no per-principal segment in a shared namespace). Pure and
/// tested.
pub(crate) fn mailbox_owner_for(prefix: &str, delimiter: Option<char>) -> bifrost_types::MailboxId {
    // The owner is the whole namespace root minus its trailing delimiter,
    // NOT just the root's final segment. Collapsing to the final segment
    // merges distinct multi-segment shared trees: `#shared/dept/` and
    // `#other/dept/` would both yield `dept`, conflating two unrelated
    // shared mailboxes' membership scopes. The full (trailing-stripped)
    // prefix keeps them distinct.
    let owner = match delimiter {
        Some(d) => prefix.strip_suffix(d).unwrap_or(prefix),
        None => prefix,
    };
    bifrost_types::MailboxId(owner.to_owned())
}

/// Owner identity for the *other-user* namespace: the per-principal segment
/// that follows the namespace root in a folder's own path. RFC 2342 returns
/// a single `other` descriptor for the whole root (`#user/`), not one per
/// user, so the owning principal can only be read off each folder path -
/// `#user/alice/INBOX` under root `#user/` (delimiter `/`) -> `alice`;
/// `#user/bob` -> `bob`. Distinct users therefore get distinct owners.
/// Falls back to the root-derived owner when the folder path does not sit
/// under the prefix or has no segment after it (defensive; a well-formed
/// LIST under the prefix always does). Pure and tested.
pub(crate) fn mailbox_owner_from_other_user_path(
    folder_path: &str,
    prefix: &str,
    delimiter: Option<char>,
) -> bifrost_types::MailboxId {
    // The prefix only attributes a principal when the folder genuinely
    // sits *under* it: a bare textual `strip_prefix` would mis-read
    // `OtherTeam/INBOX` as belonging to the `Other` namespace and invent
    // owner `Team`. Require the prefix to terminate on a delimiter
    // boundary (the prefix already ends in the delimiter, or the path's
    // next character after the prefix is the delimiter). When it does
    // not, fall back to the root-derived owner rather than fabricating
    // one from a partial-segment match.
    let relative = match (folder_path.strip_prefix(prefix), delimiter) {
        (Some(rest), Some(d)) if prefix.ends_with(d) || rest.starts_with(d) => rest,
        (Some(rest), None) => rest,
        // Prefix matched textually but not on a delimiter boundary
        // (e.g. prefix `Other`, path `OtherTeam/INBOX`): not under the
        // namespace.
        _ => return mailbox_owner_for(prefix, delimiter),
    };
    let segment = match delimiter {
        Some(d) => relative.split(d).find(|s| !s.is_empty()),
        None => (!relative.is_empty()).then_some(relative),
    };
    match segment {
        Some(owner) => bifrost_types::MailboxId(owner.to_owned()),
        None => mailbox_owner_for(prefix, delimiter),
    }
}

/// Result of the open-time shared-folder discovery: the shared folders
/// themselves plus whether the server advertised any foreign (other-user
/// or shared) namespace root at all. The flag is deliberately independent
/// of `entries` being empty: a grantless viewer on a sharing-capable
/// server has zero entries today but a post-open ACL grant CAN surface
/// folders later, which is exactly what a consumer-side rediscovery
/// reattach exists to observe. On a personal-only server the flag is
/// `false` and such a reattach can never surface anything.
pub(crate) struct SharedDiscovery {
    pub(crate) entries: Vec<super::folder_registry::SharedFolderEntry>,
    pub(crate) foreign_namespaces_advertised: bool,
}

/// Pure decision: does a NAMESPACE response advertise any non-empty
/// foreign (other-user or shared) namespace root? Split out of
/// `discover_shared_folders` so the flag's semantics are pinned by unit
/// tests rather than living inline in wire-driven code.
pub(crate) fn namespaces_advertise_foreign(namespaces: &crate::types::NamespaceResponse) -> bool {
    namespaces
        .other
        .iter()
        .chain(namespaces.shared.iter())
        .any(|descriptor| !descriptor.prefix.is_empty())
}

/// Issue NAMESPACE (when advertised / rev2) and LIST each non-empty
/// `other` and `shared` namespace prefix. Returns the discovered shared
/// folders tagged with their owning mailbox. NAMESPACE absent or empty
/// other/shared lists -> empty entry Vec (a plain personal-only server). A LIST
/// under one prefix failing is non-fatal: log + skip that prefix, keep the
/// others (a revoked prefix must not fail the whole open). When the server
/// advertises ACL, each candidate folder is probed with MYRIGHTS and the
/// parsed set rides out on the entry. When ACL is not advertised, LIST
/// visibility is taken to imply at least lookup, and a later SELECT
/// surfaces any `NO`.
///
/// Every candidate under a non-personal prefix is RETURNED, including one
/// whose rights do not grant read. Dropping the unreadable ones here was a
/// silent demotion: RFC 2342 lets the personal `LIST "" "*"` echo the
/// non-personal namespaces (and servers do), so a dropped candidate stayed
/// in the registry as the bare personal entry that listing produced - no
/// owner, no `Shared` namespace, no rights - and a consumer reading
/// `containers_list` then treated a read-only share as a writable personal
/// folder. The read decision is still honored, one layer down:
/// `FolderEntry` marks such an entry UNSELECTABLE
/// (`shared_folder_is_selectable`), so it surfaces as a correctly-typed,
/// correctly-righted container without ever becoming a cursor scope.
///
/// The parsed rights set is RETAINED on the returned entry (not just used
/// as a gate) so `containers_list` can project it onto
/// `Container::rights`: without it a read-only shared folder is
/// indistinguishable from a writable one downstream.
pub(crate) async fn discover_shared_folders(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
) -> SharedDiscovery {
    if !profile.supports(Capability::Namespace) {
        return SharedDiscovery {
            entries: Vec::new(),
            foreign_namespaces_advertised: false,
        };
    }
    let namespaces = match conn.namespace(cfg.imap.command_timeout).await {
        Ok(ns) => ns,
        Err(err) => {
            tracing::debug!(error = %err, "NAMESPACE failed; treating as personal-only");
            return SharedDiscovery {
                entries: Vec::new(),
                foreign_namespaces_advertised: false,
            };
        }
    };
    let foreign_namespaces_advertised = namespaces_advertise_foreign(&namespaces);
    let acl = profile.supports(Capability::Acl);
    let mut out = Vec::new();
    // Other-user and shared namespaces are both non-personal, but their
    // owner derivation differs: an other-user root (`#user/`) carries one
    // descriptor for ALL users, so the owning principal is read per-folder
    // (the segment after the root). A shared root (`#shared.`) has no
    // per-principal segment, so every folder under it shares one owner.
    let descriptors = namespaces
        .other
        .iter()
        .map(|d| (d, true))
        .chain(namespaces.shared.iter().map(|d| (d, false)));
    for (descriptor, is_other_user) in descriptors {
        if descriptor.prefix.is_empty() {
            continue;
        }
        let shared_owner = mailbox_owner_for(&descriptor.prefix, descriptor.delimiter);
        // LIST everything under this prefix. The pattern - NOT the
        // reference - carries the namespace root: `LIST "" "<prefix>*"`.
        // RFC 3501 6.3.8 leaves reference/pattern concatenation
        // implementation-defined, and servers that ignore the reference
        // answer `LIST "<prefix>" "*"` with the PERSONAL namespace. That
        // silently re-registers personal folders as shared candidates and
        // yields zero real shared folders, so the interoperable form is
        // the one that names the prefix inside the pattern.
        let pattern = format!("{}*", descriptor.prefix);
        let listed = match conn.list("", &pattern, cfg.imap.command_timeout).await {
            Ok(folders) => folders,
            Err(err) => {
                tracing::debug!(
                    prefix = %descriptor.prefix,
                    error = %err,
                    "LIST under shared namespace prefix failed; skipping prefix"
                );
                continue;
            }
        };
        for info in listed {
            let owner = if is_other_user {
                mailbox_owner_from_other_user_path(
                    info.name.as_str(),
                    &descriptor.prefix,
                    descriptor.delimiter,
                )
            } else {
                shared_owner.clone()
            };
            let selectable = !info.attributes.iter().any(|attr| {
                matches!(
                    attr,
                    crate::types::MailboxAttribute::NoSelect
                        | crate::types::MailboxAttribute::NonExistent
                )
            });
            let mut rights = None;
            if acl && selectable {
                // Pre-flight ACL probe. Advisory: it records what the
                // server said, it does not decide membership of the shared
                // set - a folder we cannot read is still a folder in this
                // namespace, owned by this principal, and must reach the
                // consumer with that identity rather than falling back to
                // the personal listing's bare entry. A per-folder MYRIGHTS
                // failure is non-fatal (log + keep the folder; SELECT stays
                // authoritative).
                match conn
                    .my_rights(info.name.as_str(), cfg.imap.command_timeout)
                    .await
                {
                    Ok(wire) => {
                        rights = Some(MailboxRights::parse(&wire));
                    }
                    Err(err) => {
                        tracing::debug!(
                            mailbox = %info.name.as_str(),
                            error = %err,
                            "MYRIGHTS failed for shared folder; registering and deferring to SELECT"
                        );
                    }
                }
            }
            out.push(super::folder_registry::SharedFolderEntry {
                info,
                owner,
                rights,
                namespace_prefix: descriptor.prefix.clone(),
            });
        }
    }
    SharedDiscovery {
        entries: out,
        foreign_namespaces_advertised,
    }
}

pub(crate) async fn list_folders(
    conn: &crate::ImapConnection,
    cfg: &ImapAccountConfig,
    profile: &ServerProfile,
) -> Result<Vec<MailboxInfo>, crate::Error> {
    if profile.supports(Capability::ListExtended) {
        match conn
            .list_extended("", &["*"], &[], &["SPECIAL-USE"], cfg.imap.command_timeout)
            .await
        {
            Ok(folders) => return Ok(folders),
            Err(crate::Error::MissingCapability(_)) => {}
            Err(err) => return Err(err),
        }
    }
    conn.list("", "*", cfg.imap.command_timeout).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bandwidth_config(calendar: bool) -> ImapAccountConfig {
        let mut cfg = ImapAccountConfig::new(
            ImapConfig::plaintext("imap.invalid"),
            Credentials::password("user", "pass"),
            AuthPolicy::default(),
        );
        if calendar {
            cfg.caldav = Some(CalDavConfig::new(
                "https://dav.invalid",
                bifrost_caldav::CalDavCredentials::Basic {
                    username: "user".into(),
                    password: "pass".into(),
                },
            ));
        } else {
            cfg.carddav = Some(CardDavConfig::new(
                "https://dav.invalid",
                bifrost_carddav::CardDavCredentials::Basic {
                    username: "user".into(),
                    password: "pass".into(),
                },
            ));
        }
        cfg
    }

    #[test]
    fn bandwidth_factory_default_is_the_process_shared_net() {
        let factory = ImapAccountFactory::new(bandwidth_config(false));
        let first = factory.resolve_net(bifrost_net::Net::shared_default);
        let next = factory.resolve_net(bifrost_net::Net::shared_default);
        let shared = bifrost_net::Net::shared_default();
        assert!(std::ptr::eq(first.governor(), shared.governor()));
        assert!(std::ptr::eq(first.governor(), next.governor()));
        assert!(Arc::ptr_eq(
            first.bandwidth_registry(),
            shared.bandwidth_registry()
        ));
        assert!(Arc::ptr_eq(
            first.bandwidth_registry(),
            next.bandwidth_registry()
        ));
    }

    async fn bandwidth_connection(
        budget: bifrost_net::BandwidthBudget,
    ) -> Result<crate::ImapConnection, crate::Error> {
        use crate::connection::test_support::{
            driver_pair_metered, preauth_greeting, read_line, respond, tag_of,
        };
        let (conn, mut peer) = driver_pair_metered(&preauth_greeting("IMAP4rev1"), budget).await;
        tokio::spawn(async move {
            let command = read_line(&mut peer).await;
            assert!(command.contains(" LIST "));
            respond(
                &mut peer,
                &format!(
                    "* LIST () \"/\" \"INBOX\"\r\n{} OK listed\r\n",
                    tag_of(&command)
                ),
            )
            .await;
            let command = read_line(&mut peer).await;
            assert!(command.contains("EXAMINE") || command.contains("SELECT"));
            respond(&mut peer, &format!("* FLAGS (\\Seen)\r\n* 1 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 1] valid\r\n{} OK selected\r\n", tag_of(&command))).await;
            let command = read_line(&mut peer).await;
            assert!(command.contains("UID FETCH"));
            respond(
                &mut peer,
                &format!(
                    "* 1 FETCH (UID 1 BODY[] {{4}}\r\ndata)\r\n{} OK fetched\r\n",
                    tag_of(&command)
                ),
            )
            .await;
        });
        Ok(conn)
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_factory_opens_reuse_net_and_share_dav_with_fetch() {
        use bifrost_net::test_support::{ScriptedDispatch, canned, scripted_net};
        use bifrost_net::{BandwidthDirection, NetConfig};
        use futures::StreamExt;
        use tokio::time::{Duration, Instant};
        for injected in [false, true] {
            for calendar in [false, true] {
                let script = ScriptedDispatch::new(
                    (0..2).map(|_| canned(401_u16.try_into().unwrap(), &[b'x'; 4096])),
                );
                let net = scripted_net(&script, NetConfig::default());
                let factory = ImapAccountFactory::new(bandwidth_config(calendar));
                let factory = if injected {
                    factory.with_net(net.clone())
                } else {
                    factory
                };
                let id = AccountId(format!("composed-{injected}-{calendar}"));
                let budget = net.bandwidth_registry().budget(id.clone());
                budget.set_cap(Some(4096));
                let mut accounts = Vec::new();
                for incarnation in 0..2 {
                    let selected = factory.resolve_net(|| net.clone());
                    assert!(std::ptr::eq(selected.governor(), net.governor()));
                    // Substitute the default provider only to keep HTTP scripted.
                    // The separate default test pins the real shared singleton.
                    let opened = factory
                        .open_using(
                            id.clone(),
                            || {
                                assert!(
                                    !injected,
                                    "injected opens must not resolve a new default Net"
                                );
                                net.clone()
                            },
                            |_, _, budget| Box::pin(bandwidth_connection(budget)),
                        )
                        .await
                        .unwrap();
                    assert_eq!(script.requests().len(), incarnation + 1);
                    assert_eq!(opened.skipped_scopes.len(), 1);
                    opened.account.set_bandwidth_cap(Some(4096));
                    assert_eq!(budget.cap(), Some(4096));
                    let start = Instant::now();
                    let folder = crate::types::MailboxName::new("INBOX").unwrap();
                    let mut fetch = opened
                        .account
                        .open_raw_rfc822(super::super::encode_object_id(&folder, 1, 1));
                    let mut body = Vec::new();
                    let mut done = false;
                    while let Some(event) = fetch.next().await {
                        match event {
                            bifrost_types::SyncEvent::Batch(batch) => {
                                for bytes in batch.items {
                                    body.extend_from_slice(&bytes);
                                }
                            }
                            bifrost_types::SyncEvent::Done(_) => done = true,
                            other => panic!("unexpected fetch event: {other:?}"),
                        }
                    }
                    assert!(done);
                    assert_eq!(body, b"data");
                    assert!(
                        start.elapsed() >= Duration::from_secs(1),
                        "DAV's full spent slot must delay IMAP fetch by a second"
                    );
                    let next = budget.reserve(BandwidthDirection::Inbound, 4096);
                    assert!(
                        next.ready_at() > Instant::now(),
                        "FETCH must charge the same budget"
                    );
                    drop(next);
                    accounts.push(opened);
                }
                assert_eq!(script.remaining(), 0);
            }
        }
    }

    #[test]
    fn mailbox_owner_for_strips_delimiter() {
        // Shared-root helper: trailing-delimiter-stripped final segment.
        // Single-segment shared prefix: no interior delimiter, whole
        // (trailing-stripped) prefix is the owner.
        assert_eq!(
            mailbox_owner_for("#shared.", Some('.')),
            bifrost_types::MailboxId("#shared".to_owned())
        );
        // No delimiter: prefix is the owner verbatim.
        assert_eq!(
            mailbox_owner_for("Shared", None),
            bifrost_types::MailboxId("Shared".to_owned())
        );
        // Multi-segment shared root keeps its whole (trailing-stripped)
        // path as the owner so distinct trees do not collapse. A prior
        // version used only the final segment, which merged
        // `#shared/dept/` and `#other/dept/` into one `dept` owner.
        assert_eq!(
            mailbox_owner_for("#shared/dept/", Some('/')),
            bifrost_types::MailboxId("#shared/dept".to_owned())
        );
        assert_ne!(
            mailbox_owner_for("#shared/dept/", Some('/')),
            mailbox_owner_for("#other/dept/", Some('/')),
        );
    }

    #[test]
    fn mailbox_owner_from_other_user_path_reads_principal_per_folder() {
        // RFC 2342 returns one `other` descriptor (`#user/`) for every
        // user. The owning principal must be read off each folder path so
        // distinct users get distinct owners - not collapsed to the root.
        assert_eq!(
            mailbox_owner_from_other_user_path("#user/alice/INBOX", "#user/", Some('/')),
            bifrost_types::MailboxId("alice".to_owned())
        );
        assert_eq!(
            mailbox_owner_from_other_user_path("#user/bob/Sent", "#user/", Some('/')),
            bifrost_types::MailboxId("bob".to_owned())
        );
        // The user-root folder itself (no segment after the principal).
        assert_eq!(
            mailbox_owner_from_other_user_path("#user/alice", "#user/", Some('/')),
            bifrost_types::MailboxId("alice".to_owned())
        );
        // Non-default delimiter.
        assert_eq!(
            mailbox_owner_from_other_user_path(
                "Other Users.carol.INBOX",
                "Other Users.",
                Some('.')
            ),
            bifrost_types::MailboxId("carol".to_owned())
        );
        // Defensive fallback: a path not under the prefix degrades to the
        // root-derived owner rather than panicking.
        assert_eq!(
            mailbox_owner_from_other_user_path("#user/", "#user/", Some('/')),
            bifrost_types::MailboxId("#user".to_owned())
        );
        // Delimiter-boundary guard: a prefix that matches textually but
        // not on a delimiter boundary (`Other` vs `OtherTeam/INBOX`) must
        // NOT invent a bogus principal (`Team`) from the partial-segment
        // match; it falls back to the root-derived owner.
        assert_eq!(
            mailbox_owner_from_other_user_path("OtherTeam/INBOX", "Other", Some('/')),
            bifrost_types::MailboxId("Other".to_owned())
        );
        // A prefix without a trailing delimiter that DOES terminate on a
        // boundary (next char is the delimiter) still reads the principal.
        assert_eq!(
            mailbox_owner_from_other_user_path("Other/dave/INBOX", "Other", Some('/')),
            bifrost_types::MailboxId("dave".to_owned())
        );
    }

    #[test]
    fn foreign_namespace_advertisement_requires_a_non_empty_foreign_prefix() {
        use crate::types::{NamespaceDescriptor, NamespaceResponse};

        let personal_only = NamespaceResponse {
            personal: vec![NamespaceDescriptor {
                prefix: String::new(),
                delimiter: Some('/'),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(!namespaces_advertise_foreign(&personal_only));

        // An empty-prefix foreign descriptor is a degenerate advertisement
        // (nothing distinct to LIST under) and must not count.
        let empty_prefix_other = NamespaceResponse {
            other: vec![NamespaceDescriptor::default()],
            ..Default::default()
        };
        assert!(!namespaces_advertise_foreign(&empty_prefix_other));

        let other_user = NamespaceResponse {
            other: vec![NamespaceDescriptor {
                prefix: "#user/".to_string(),
                delimiter: Some('/'),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(namespaces_advertise_foreign(&other_user));

        let shared_root = NamespaceResponse {
            shared: vec![NamespaceDescriptor {
                prefix: "#shared.".to_string(),
                delimiter: Some('.'),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(namespaces_advertise_foreign(&shared_root));
    }

    #[test]
    fn qresync_runtime_gate_defaults_off() {
        let cfg = ImapAccountConfig::new(
            ImapConfig::tls("imap.example.test"),
            Credentials::password("user", "pass"),
            AuthPolicy::default(),
        );
        assert!(!cfg.enable_qresync);
    }

    #[test]
    fn server_id_preconfigures_known_broken_qresync() {
        let id = vec![("name".to_string(), Some("iCloud IMAP".to_string()))];
        assert!(server_id_disables_qresync(&id));

        let id = vec![("name".to_string(), Some("Dovecot".to_string()))];
        assert!(!server_id_disables_qresync(&id));

        let id = vec![("name".to_string(), Some("MyIcloudProxy".to_string()))];
        assert!(!server_id_disables_qresync(&id));
    }

    fn transport_error() -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
            bifrost_types::Cause::Transport(bifrost_types::TransportCause::new(
                bifrost_types::TransportKind::Network,
                None,
            )),
        )
        .operation(AccountOperation::Discover)
        .try_build()
        .expect("valid account error classification")
    }

    fn auth_lost_error() -> AccountError {
        bifrost_types::AccountErrorBuilder::new(
            bifrost_types::AccountErrorKind::Authentication(
                bifrost_types::AuthErrorKind::ReauthorizationRequired,
            ),
            bifrost_types::Cause::Auth(bifrost_types::AuthCause::ReauthorizationRequired),
        )
        .operation(AccountOperation::Discover)
        .try_build()
        .expect("valid account error classification")
    }

    #[test]
    fn dav_open_classify_maps_transient_and_terminal_and_success() {
        let contact_scope = || bifrost_types::ErrorScope::ContactCollection;
        // Transport failure -> degraded, transient, skip recorded with
        // the collection scope + retryable classification.
        match classify_dav_open(Err(transport_error()), "CardDAV", contact_scope()) {
            DavAttach::Degraded {
                transient, skip, ..
            } => {
                assert!(transient, "transport is transient");
                assert!(matches!(
                    skip.scope,
                    bifrost_types::ErrorScope::ContactCollection
                ));
                assert!(skip.error.recovery().is_retryable());
            }
            other => panic!("expected Degraded, got {}", attach_label(&other)),
        }

        // Auth-lost -> degraded, terminal (not transient).
        match classify_dav_open(Err(auth_lost_error()), "CardDAV", contact_scope()) {
            DavAttach::Degraded {
                transient, skip, ..
            } => {
                assert!(!transient, "auth-lost is a terminal config fix");
                assert!(skip.error.recovery().is_terminal());
            }
            other => panic!("expected Degraded, got {}", attach_label(&other)),
        }

        // Success -> attached.
        let stub = crate::account::test_support::stub_arc(
            crate::account::test_support::StubAccount::new(Vec::new()),
        );
        match classify_dav_open(
            Ok(OpenedAccount::complete(stub)),
            "CardDAV",
            contact_scope(),
        ) {
            DavAttach::Attached { skipped, .. } => assert!(skipped.is_empty()),
            other => panic!("expected Attached, got {}", attach_label(&other)),
        }
    }

    /// A DAV open that SUCCEEDS while reporting skips must forward them onto
    /// the composed account's lane. Both DAV crates cover one collection, so
    /// this is the path that carries an unsynced calendar or address book out
    /// of a composed IMAP account; dropping it hides the gap entirely.
    #[test]
    fn a_successful_dav_open_forwards_its_own_skipped_scopes() {
        let stub = crate::account::test_support::stub_arc(
            crate::account::test_support::StubAccount::new(Vec::new()),
        );
        let contact_scope = bifrost_types::ErrorScope::ContactCollection;
        let sub_skip = SkippedScope {
            scope: contact_scope.clone(),
            error: discover_err(crate::Error::Internal(
                "second address book is not synced".to_string(),
            )),
        };
        let opened = OpenedAccount {
            account: stub,
            skipped_scopes: vec![sub_skip],
        };

        let attach = classify_dav_open(Ok(opened), "CardDAV", contact_scope);
        let mut degraded = Vec::new();
        let mut skipped = Vec::new();
        let account = attach.into_attached(&mut degraded, &mut skipped);

        assert!(account.is_some(), "a successful open still attaches");
        assert!(
            degraded.is_empty(),
            "a skip on a successful open is not a degradation"
        );
        assert_eq!(
            skipped.len(),
            1,
            "the sub-account's own skip must reach the composed lane"
        );
    }

    fn attach_label(attach: &DavAttach) -> &'static str {
        match attach {
            DavAttach::Attached { .. } => "Attached",
            DavAttach::None => "None",
            DavAttach::Degraded { .. } => "Degraded",
        }
    }
}
