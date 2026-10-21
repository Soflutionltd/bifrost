#[cfg(feature = "mail")]
use crate::email::{MailCapabilities, SubmissionCapabilities};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Session {
    #[serde(rename = "capabilities")]
    #[serde(deserialize_with = "deserialize_session_capabilities_map")]
    capabilities: HashMap<String, Capabilities>,

    #[serde(rename = "accounts")]
    accounts: HashMap<String, Account>,

    #[serde(rename = "primaryAccounts")]
    primary_accounts: HashMap<String, String>,

    #[serde(rename = "username")]
    username: String,

    #[serde(rename = "apiUrl")]
    api_url: String,

    #[serde(rename = "downloadUrl")]
    download_url: String,

    #[serde(rename = "uploadUrl")]
    upload_url: String,

    #[serde(rename = "eventSourceUrl")]
    event_source_url: String,

    #[serde(rename = "state")]
    state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Account {
    #[serde(rename = "name")]
    name: String,

    #[serde(rename = "isPersonal")]
    is_personal: bool,

    #[serde(rename = "isReadOnly")]
    is_read_only: bool,

    #[serde(rename = "accountCapabilities")]
    #[serde(deserialize_with = "deserialize_capabilities_map")]
    account_capabilities: HashMap<String, Capabilities>,
}

/// Session/account capability value. The correct variant is selected by
/// the map key (capability URI), not by the value shape.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Capabilities {
    Core(CoreCapabilities),
    #[cfg(feature = "mail")]
    Mail(MailCapabilities),
    #[cfg(feature = "mail")]
    Submission(SubmissionCapabilities),
    /// A capability block this crate models by URI was PRESENT but did
    /// not parse into its typed struct (a core limit sent as a string, a
    /// websocket block with no `url`, say).
    ///
    /// Kept as its own variant rather than folded into `Other` so that a
    /// present-but-malformed block reads as the INVALID lane instead of
    /// the unadvertised one: the two map to different recovery classes
    /// (`Protocol(ContractViolation)` versus `SyncState(CapabilityChanged)`
    /// or a silent feature-off), and `Other` is indistinguishable from
    /// absent at every reader. `Other` holds unknown extension values and
    /// opaque session support markers; account limits are decoded only in
    /// the account map.
    Malformed(serde_json::Value),
    WebSocket(WebSocketCapabilities),
    #[cfg(feature = "mail")]
    Sieve(SieveCapabilities),
    #[cfg(feature = "quota")]
    Quota(QuotaCapabilities),
    #[cfg(feature = "blob")]
    Blob(BlobCapabilities),
    #[cfg(feature = "calendars")]
    Calendars(CalendarsCapabilities),
    #[cfg(feature = "contacts")]
    Contacts(ContactsCapabilities),
    Principals(PrincipalsCapabilities),
    PrincipalsOwner(PrincipalsOwnerCapabilities),
    Other(serde_json::Value),
}

// Session extension objects indicate support; their account-specific fields
// must never be decoded as limits or identities for an arbitrary account.
fn deserialize_session_capabilities_map<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, Capabilities>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: HashMap<String, JsonValue> = HashMap::deserialize(deserializer)?;
    raw.into_iter()
        .map(|(key, value)| {
            let cap = if matches!(
                key.as_str(),
                "urn:ietf:params:jmap:core"
                    | "urn:ietf:params:jmap:websocket"
                    | "urn:ietf:params:jmap:quota"
                    | "urn:ietf:params:jmap:blob"
            ) {
                let map = serde_json::json!({key.clone(): value});
                deserialize_capabilities_map(map)
                    .map_err(serde::de::Error::custom)?
                    .remove(&key)
                    .expect("single capability key")
            } else if value.is_object()
                || !matches!(
                    key.as_str(),
                    "urn:ietf:params:jmap:mail"
                        | "urn:ietf:params:jmap:submission"
                        | "urn:ietf:params:jmap:sieve"
                        | "urn:ietf:params:jmap:contacts"
                        | "urn:ietf:params:jmap:calendars"
                        | "urn:ietf:params:jmap:principals"
                        | "urn:ietf:params:jmap:principals:owner"
                )
            {
                Capabilities::Other(value)
            } else {
                Capabilities::Malformed(value)
            };
            Ok((key, cap))
        })
        .collect()
}

/// Custom deserializer for `HashMap<String, Capabilities>` that
/// dispatches to the correct `Capabilities` variant based on the URI
/// key, rather than relying on `#[serde(untagged)]` trial-and-error.
fn deserialize_capabilities_map<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, Capabilities>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: HashMap<String, JsonValue> = HashMap::deserialize(deserializer)?;
    let mut result = HashMap::with_capacity(raw.len());

    /// Deserialize a capability value as a typed struct, falling back to
    /// `Malformed(original_value)` on parse failure. Every URI this crate
    /// models goes through here, so the Absent / Malformed / Present
    /// discipline is one mechanism rather than a per-capability special
    /// case: a block whose URI we recognize and whose shape we reject is
    /// never reported as unadvertised. Serializes to a string first to
    /// avoid cloning the Value - `from_value` consumes on error, and the
    /// original has to survive into `Malformed`.
    ///
    /// The `is_object` guard is load-bearing and not redundant with the
    /// typed parse: serde's DERIVED struct deserializers also accept a
    /// JSON sequence in field-declaration order, so `[]` deserializes
    /// into any capability struct whose fields all default - which meant
    /// `"urn:ietf:params:jmap:calendars": []` read as a fully advertised,
    /// all-defaults PRESENT block. RFC 8620 s2 makes every capability
    /// value an object, so anything else is Malformed at the one door
    /// every modelled URI passes through.
    macro_rules! try_cap {
        ($value:expr, $variant:ident) => {{
            if !$value.is_object() {
                Capabilities::Malformed($value)
            } else {
                let s = serde_json::to_string(&$value).unwrap();
                match serde_json::from_str(&s) {
                    Ok(v) => Capabilities::$variant(v),
                    Err(_) => Capabilities::Malformed($value),
                }
            }
        }};
    }

    for (key, value) in raw {
        let cap = match key.as_str() {
            "urn:ietf:params:jmap:core" => try_cap!(value, Core),
            #[cfg(feature = "mail")]
            "urn:ietf:params:jmap:mail" => try_cap!(value, Mail),
            #[cfg(feature = "mail")]
            "urn:ietf:params:jmap:submission" => try_cap!(value, Submission),
            "urn:ietf:params:jmap:websocket" => try_cap!(value, WebSocket),
            #[cfg(feature = "mail")]
            "urn:ietf:params:jmap:sieve" => try_cap!(value, Sieve),
            #[cfg(feature = "quota")]
            "urn:ietf:params:jmap:quota" => try_cap!(value, Quota),
            #[cfg(feature = "blob")]
            "urn:ietf:params:jmap:blob" => try_cap!(value, Blob),
            #[cfg(feature = "calendars")]
            "urn:ietf:params:jmap:calendars" => try_cap!(value, Calendars),
            #[cfg(feature = "contacts")]
            "urn:ietf:params:jmap:contacts" => try_cap!(value, Contacts),
            "urn:ietf:params:jmap:principals" => try_cap!(value, Principals),
            "urn:ietf:params:jmap:principals:owner" => try_cap!(value, PrincipalsOwner),
            _ => Capabilities::Other(value),
        };
        result.insert(key, cap);
    }

    Ok(result)
}

/// `urn:ietf:params:jmap:core` limits and required properties.
///
/// Every numeric limit is `Option<usize>`, not a zero-filled `usize`. RFC
/// 8620 §2 makes all of them mandatory members of the core object, so an
/// omitted one and an advertised `0` are two different server bugs with two
/// different honest readings - "the server told us nothing" versus "the
/// server told us a limit that forbids every request" - and a blanket
/// `#[serde(default)]` merged them into the same zero. The readers that
/// enforce a bound (`CallLimit`, the WebSocket frame-size guard) and the
/// reader that validates the session (`sync::capabilities::build`) need the
/// distinction to land in the right lane. The required `collationAlgorithms`
/// array also preserves presence, while its existing accessor keeps returning
/// an empty slice for callers that only need the list.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct CoreCapabilities {
    #[serde(rename = "maxSizeUpload")]
    max_size_upload: Option<usize>,

    #[serde(rename = "maxConcurrentUpload")]
    max_concurrent_upload: Option<usize>,

    #[serde(rename = "maxSizeRequest")]
    max_size_request: Option<usize>,

    #[serde(rename = "maxConcurrentRequests")]
    max_concurrent_requests: Option<usize>,

    #[serde(rename = "maxCallsInRequest")]
    max_calls_in_request: Option<usize>,

    #[serde(rename = "maxObjectsInGet")]
    max_objects_in_get: Option<usize>,

    #[serde(rename = "maxObjectsInSet")]
    max_objects_in_set: Option<usize>,

    #[serde(rename = "collationAlgorithms")]
    #[serde(default, deserialize_with = "deserialize_collation_algorithms")]
    #[serde(skip_serializing_if = "Option::is_none")]
    collation_algorithms: Option<Vec<String>>,
}

/// An absent member defaults to `None`, while a present member must be an
/// array. Using `Option<Vec<_>>::deserialize` directly would also accept JSON
/// `null` as absence, even though RFC 8620 requires an array.
fn deserialize_collation_algorithms<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer).map(Some)
}

/// The three states a modelled capability block can be in, which the
/// two-state `Option<&T>` accessors cannot express.
///
/// Every typed capability has all three, not just core: a present block
/// the crate refuses to parse is a different fact from an absent one, and
/// collapsing them makes a server's malformed `websocket` block read as
/// "this server has no websockets".
#[derive(Debug, Clone, Copy)]
pub(crate) enum CapabilityState<'a, T> {
    /// No key for this capability URI in the selected map.
    Absent,
    /// Present, but not parseable as the typed capability object.
    Malformed,
    Present(&'a T),
}

/// The core block's three-state, spelled out for the readers that name it.
pub(crate) type CoreCapabilityState<'a> = CapabilityState<'a, CoreCapabilities>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WebSocketCapabilities {
    #[serde(rename = "url")]
    url: String,
    #[serde(rename = "supportsPush")]
    #[serde(default)]
    supports_push: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct SieveCapabilities {
    #[serde(rename = "implementation")]
    implementation: Option<String>,
    #[serde(rename = "maxSizeScriptName")]
    max_script_name: Option<usize>,
    #[serde(rename = "maxSizeScript")]
    max_script_size: Option<usize>,
    #[serde(rename = "maxNumberScripts")]
    max_scripts: Option<usize>,
    #[serde(rename = "maxNumberRedirects")]
    max_redirects: Option<usize>,
    #[serde(rename = "sieveExtensions")]
    extensions: Vec<String>,
    #[serde(rename = "notificationMethods")]
    notification_methods: Option<Vec<String>>,
    #[serde(rename = "externalLists")]
    ext_lists: Option<Vec<String>>,
}

/// Capabilities for `urn:ietf:params:jmap:quota` (RFC 9425).
///
/// Empty capability object per spec - presence indicates quota support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QuotaCapabilities {}

/// Capabilities for `urn:ietf:params:jmap:blob` (RFC 9404).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BlobCapabilities {
    #[serde(rename = "maxSizeBlobSet")]
    #[serde(default)]
    max_size_blob_set: Option<u64>,

    #[serde(rename = "supportedDigestAlgorithms")]
    #[serde(default)]
    supported_digest_algorithms: Vec<String>,

    #[serde(rename = "supportedTypeNames")]
    #[serde(default)]
    supported_type_names: Vec<String>,
}

/// Capabilities for `urn:ietf:params:jmap:calendars`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CalendarsCapabilities {
    #[serde(rename = "mayCreateCalendar")]
    #[serde(default)]
    may_create_calendar: bool,

    #[serde(rename = "maxCalendarsPerEvent")]
    #[serde(default)]
    max_calendars_per_event: Option<usize>,
}

/// Capabilities for `urn:ietf:params:jmap:contacts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ContactsCapabilities {
    #[serde(rename = "mayCreateAddressBook")]
    #[serde(default)]
    may_create_address_book: bool,

    #[serde(rename = "maxAddressBooksPerCard")]
    #[serde(default)]
    max_address_books_per_card: Option<usize>,
}

/// Capabilities for `urn:ietf:params:jmap:principals`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrincipalsCapabilities {
    #[serde(rename = "currentUserPrincipalId")]
    #[serde(default)]
    current_user_principal_id: Option<crate::principal::PrincipalId>,

    #[serde(rename = "accountIdForPrincipal")]
    #[serde(default)]
    account_id_for_principal: Option<crate::core::id::AccountId>,
}

/// Generates the pair of readers every modelled capability needs: the
/// two-state `Option` accessor for callers that only want to decline to
/// act, and the three-state `CapabilityState` accessor for callers that
/// have to CLASSIFY (refuse at the door, or degrade with a named
/// diagnostic) - because to those, malformed and absent are not the same
/// server.
macro_rules! session_cap_accessor {
    ($(#[$meta:meta])* $method:ident, $state_method:ident, $cap_marker:ty, $variant:ident, $return_type:ty, $map:ident) => {
        $(#[$meta])*
        pub(crate) fn $method(&self) -> Option<&$return_type> {
            match self.$state_method() {
                CapabilityState::Present(c) => Some(c),
                CapabilityState::Absent | CapabilityState::Malformed => None,
            }
        }

        $(#[$meta])*
        pub(crate) fn $state_method(&self) -> CapabilityState<'_, $return_type> {
            match self
                .$map
                .get(<$cap_marker as crate::core::capability::Capability>::URI)
            {
                None => CapabilityState::Absent,
                Some(Capabilities::$variant(c)) => CapabilityState::Present(c),
                Some(_) => CapabilityState::Malformed,
            }
        }
    };
}

impl Session {
    session_cap_accessor!(
        #[cfg(feature = "blob")]
        blob_capabilities,
        blob_capability_state,
        crate::core::capability::Blob,
        Blob,
        BlobCapabilities,
        capabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "quota")]
        quota_capabilities,
        quota_capability_state,
        crate::core::capability::Quota,
        Quota,
        QuotaCapabilities,
        capabilities
    );
    // Convenience readers select the primary account for this capability.
    #[cfg(feature = "mail")]
    pub(crate) fn mail_capabilities(&self) -> Option<&MailCapabilities> {
        match self.mail_capability_state() {
            CapabilityState::Present(value) => Some(value),
            _ => None,
        }
    }
    #[cfg(feature = "mail")]
    pub(crate) fn mail_capability_state(&self) -> CapabilityState<'_, MailCapabilities> {
        let uri = <crate::core::capability::Mail as super::capability::Capability>::URI;
        if matches!(self.capabilities.get(uri), Some(Capabilities::Malformed(_))) {
            return CapabilityState::Malformed;
        }
        self.primary_accounts
            .get(uri)
            .and_then(|id| self.account(id))
            .map_or(CapabilityState::Absent, Account::mail_capability_state)
    }
    #[cfg(feature = "mail")]
    pub(crate) fn submission_capabilities(&self) -> Option<&SubmissionCapabilities> {
        match self.submission_capability_state() {
            CapabilityState::Present(value) => Some(value),
            _ => None,
        }
    }
    #[cfg(feature = "mail")]
    pub(crate) fn submission_capability_state(
        &self,
    ) -> CapabilityState<'_, SubmissionCapabilities> {
        let uri = <crate::core::capability::Submission as super::capability::Capability>::URI;
        if matches!(self.capabilities.get(uri), Some(Capabilities::Malformed(_))) {
            return CapabilityState::Malformed;
        }
        self.primary_accounts
            .get(uri)
            .and_then(|id| self.account(id))
            .map_or(
                CapabilityState::Absent,
                Account::submission_capability_state,
            )
    }
    #[cfg(feature = "mail")]
    pub(crate) fn sieve_capabilities(&self) -> Option<&SieveCapabilities> {
        match self.sieve_capability_state() {
            CapabilityState::Present(value) => Some(value),
            _ => None,
        }
    }
    #[cfg(feature = "mail")]
    pub(crate) fn sieve_capability_state(&self) -> CapabilityState<'_, SieveCapabilities> {
        let uri = <crate::core::capability::Sieve as super::capability::Capability>::URI;
        if matches!(self.capabilities.get(uri), Some(Capabilities::Malformed(_))) {
            return CapabilityState::Malformed;
        }
        self.primary_accounts
            .get(uri)
            .and_then(|id| self.account(id))
            .map_or(CapabilityState::Absent, Account::sieve_capability_state)
    }

    #[cfg(feature = "calendars")]
    pub(crate) fn calendars_capabilities(&self) -> Option<&CalendarsCapabilities> {
        match self.calendars_capability_state() {
            CapabilityState::Present(value) => Some(value),
            _ => None,
        }
    }
    #[cfg(feature = "calendars")]
    pub(crate) fn calendars_capability_state(&self) -> CapabilityState<'_, CalendarsCapabilities> {
        let uri = <crate::core::capability::Calendars as super::capability::Capability>::URI;
        if matches!(self.capabilities.get(uri), Some(Capabilities::Malformed(_))) {
            return CapabilityState::Malformed;
        }
        self.primary_accounts
            .get(uri)
            .and_then(|id| self.account(id))
            .map_or(CapabilityState::Absent, Account::calendars_capability_state)
    }
    #[cfg(feature = "contacts")]
    pub(crate) fn contacts_capabilities(&self) -> Option<&ContactsCapabilities> {
        match self.contacts_capability_state() {
            CapabilityState::Present(value) => Some(value),
            _ => None,
        }
    }
    #[cfg(feature = "contacts")]
    pub(crate) fn contacts_capability_state(&self) -> CapabilityState<'_, ContactsCapabilities> {
        let uri = <crate::core::capability::Contacts as super::capability::Capability>::URI;
        if matches!(self.capabilities.get(uri), Some(Capabilities::Malformed(_))) {
            return CapabilityState::Malformed;
        }
        self.primary_accounts
            .get(uri)
            .and_then(|id| self.account(id))
            .map_or(CapabilityState::Absent, Account::contacts_capability_state)
    }

    pub(crate) fn principals_capabilities(&self) -> Option<&PrincipalsCapabilities> {
        match self.principals_capability_state() {
            CapabilityState::Present(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn principals_capability_state(
        &self,
    ) -> CapabilityState<'_, PrincipalsCapabilities> {
        let uri = <crate::core::capability::Principals as super::capability::Capability>::URI;
        if matches!(self.capabilities.get(uri), Some(Capabilities::Malformed(_))) {
            return CapabilityState::Malformed;
        }
        self.primary_accounts
            .get(uri)
            .and_then(|id| self.account(id))
            .map_or(
                CapabilityState::Absent,
                Account::principals_capability_state,
            )
    }

    pub(crate) fn principals_owner_capabilities(&self) -> Option<&PrincipalsOwnerCapabilities> {
        match self.principals_owner_capability_state() {
            CapabilityState::Present(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn principals_owner_capability_state(
        &self,
    ) -> CapabilityState<'_, PrincipalsOwnerCapabilities> {
        let uri = <crate::core::capability::PrincipalsOwner as super::capability::Capability>::URI;
        if matches!(self.capabilities.get(uri), Some(Capabilities::Malformed(_))) {
            return CapabilityState::Malformed;
        }
        self.primary_accounts
            .get(uri)
            .and_then(|id| self.account(id))
            .map_or(
                CapabilityState::Absent,
                Account::principals_owner_capability_state,
            )
    }

    pub(crate) fn capabilities(&self) -> impl Iterator<Item = &String> {
        self.capabilities.keys()
    }

    pub(crate) fn capability(&self, capability: impl AsRef<str>) -> Option<&Capabilities> {
        self.capabilities.get(capability.as_ref())
    }

    pub(crate) fn has_capability(&self, capability: impl AsRef<str>) -> bool {
        self.capabilities.contains_key(capability.as_ref())
    }

    /// Get a typed capability configuration by its capability marker type.
    ///
    /// Returns `None` if the server does not advertise the capability.
    ///
    /// ```ignore
    /// use crate::core::capability::{Capability, Mail};
    /// if let Some(mail) = session.capability_config::<Mail>() {
    ///     println!("max mailbox depth: {}", mail.max_mailbox_depth());
    /// }
    /// ```
    pub(crate) fn capability_config<C: super::capability::Capability>(&self) -> Option<C::Config> {
        let cap = if matches!(
            C::URI,
            "urn:ietf:params:jmap:core"
                | "urn:ietf:params:jmap:websocket"
                | "urn:ietf:params:jmap:quota"
                | "urn:ietf:params:jmap:blob"
        ) {
            self.capabilities.get(C::URI)?
        } else {
            self.account(self.primary_accounts.get(C::URI)?)?
                .capability(C::URI)?
        };
        // A typed reader must preserve the same three-state result as the
        // accessors. Re-parsing a malformed array through a defaulted struct
        // can succeed even though the session deserializer rejected it.
        if matches!(cap, Capabilities::Malformed(_)) {
            return None;
        }
        // Serialize the enum variant to Value, then deserialize into Config.
        // This is a one-time cost per access (session parsing is infrequent).
        let value = serde_json::to_value(cap).ok()?;
        serde_json::from_value(value).ok()
    }

    /// How many API requests this session admits at once: the advertised
    /// `maxConcurrentRequests`, with an omitted or zero value read as one.
    ///
    /// RFC 8620 s2 requires the field, so a session without it has told us
    /// nothing, and the only number that cannot earn a request-level `limit`
    /// error is one: serial work is slower but always legal. This is the
    /// crate's single reading of the field. The transport's client-wide gate
    /// and every fan-out width derive from it, so they cannot disagree about
    /// a session that leaves it out.
    pub(crate) fn api_request_concurrency(&self) -> usize {
        self.core_capabilities()
            .and_then(CoreCapabilities::max_concurrent_requests)
            .unwrap_or(1)
            .max(1)
    }

    /// How many upload requests this session admits at once. Missing or zero
    /// values use a serial upload lane, matching the API concurrency reader's
    /// safe minimum instead of creating a zero-permit gate that cannot make
    /// progress.
    pub(crate) fn upload_concurrency(&self) -> usize {
        self.core_capabilities()
            .and_then(CoreCapabilities::max_concurrent_upload)
            .unwrap_or(1)
            .max(1)
    }

    session_cap_accessor!(
        websocket_capabilities,
        websocket_capability_state,
        crate::core::capability::WebSocket,
        WebSocket,
        WebSocketCapabilities,
        capabilities
    );
    session_cap_accessor!(
        core_capabilities,
        core_capability_state,
        crate::core::capability::Core,
        Core,
        CoreCapabilities,
        capabilities
    );

    /// Every capability URI the session advertises whose block this crate
    /// models by URI and then failed to parse, including the selected
    /// primary account's object for that URI.
    ///
    /// The session validator uses this to refuse on a malformed block the
    /// account DEPENDS on and to name the merely-optional ones in a
    /// diagnostic, so that "the server advertised it and described it
    /// wrongly" never leaves the crate as silence.
    pub(crate) fn malformed_capabilities(&self) -> impl Iterator<Item = &str> {
        self.capabilities
            .iter()
            .filter(|(_, value)| matches!(value, Capabilities::Malformed(_)))
            .map(|(key, _)| key.as_str())
            .chain(self.primary_accounts.iter().filter_map(|(uri, id)| {
                let account = self.account(id)?;
                matches!(account.capability(uri), Some(Capabilities::Malformed(_)))
                    .then_some(uri.as_str())
            }))
    }

    pub(crate) fn accounts(&self) -> impl Iterator<Item = &String> {
        self.accounts.keys()
    }

    pub(crate) fn account(&self, account: &str) -> Option<&Account> {
        self.accounts.get(account)
    }

    pub(crate) fn primary_accounts(&self) -> impl Iterator<Item = (&String, &String)> {
        self.primary_accounts.iter()
    }

    /// Stable fallback account selection for generic request construction.
    /// Capability-specific callers should select their own primary account.
    ///
    /// The fallback is the lowest capability URI in `primaryAccounts`, so a
    /// session advertising only calendars serves a mail-shaped generic
    /// request off the calendar account. That is documented behaviour, not a
    /// defect: narrowing it (refusing a generic request whose implied
    /// capability the session does not name) is a product decision the
    /// 2026-09-04 bug hunt raised and left open. What IS refused is an
    /// EMPTY id - a session with no `primaryAccounts` at all - which
    /// `Request::call` reports as `NoPrimaryAccount` naming the method's
    /// own capability rather than shipping `"accountId": ""`. Methods
    /// without an accountId, such as PushSubscription, need no primary.
    pub(crate) fn default_account_id(&self) -> Option<&str> {
        self.primary_accounts()
            .min_by_key(|(capability, _)| *capability)
            .map(|(_, account_id)| account_id.as_str())
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn api_url(&self) -> &str {
        &self.api_url
    }

    pub(crate) fn download_url(&self) -> &str {
        &self.download_url
    }

    pub(crate) fn upload_url(&self) -> &str {
        &self.upload_url
    }

    pub(crate) fn event_source_url(&self) -> &str {
        &self.event_source_url
    }

    pub(crate) fn state(&self) -> &str {
        &self.state
    }
}

impl Account {
    session_cap_accessor!(
        #[cfg(feature = "mail")]
        mail_capabilities,
        mail_capability_state,
        crate::core::capability::Mail,
        Mail,
        MailCapabilities,
        account_capabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "mail")]
        submission_capabilities,
        submission_capability_state,
        crate::core::capability::Submission,
        Submission,
        SubmissionCapabilities,
        account_capabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "mail")]
        sieve_capabilities,
        sieve_capability_state,
        crate::core::capability::Sieve,
        Sieve,
        SieveCapabilities,
        account_capabilities
    );

    session_cap_accessor!(
        #[cfg(feature = "calendars")]
        calendars_capabilities,
        calendars_capability_state,
        crate::core::capability::Calendars,
        Calendars,
        CalendarsCapabilities,
        account_capabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "contacts")]
        contacts_capabilities,
        contacts_capability_state,
        crate::core::capability::Contacts,
        Contacts,
        ContactsCapabilities,
        account_capabilities
    );
    session_cap_accessor!(
        principals_capabilities,
        principals_capability_state,
        crate::core::capability::Principals,
        Principals,
        PrincipalsCapabilities,
        account_capabilities
    );
    session_cap_accessor!(
        principals_owner_capabilities,
        principals_owner_capability_state,
        crate::core::capability::PrincipalsOwner,
        PrincipalsOwner,
        PrincipalsOwnerCapabilities,
        account_capabilities
    );

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn is_personal(&self) -> bool {
        self.is_personal
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    pub(crate) fn capabilities(&self) -> impl Iterator<Item = &String> {
        self.account_capabilities.keys()
    }

    pub(crate) fn capability(&self, capability: &str) -> Option<&Capabilities> {
        self.account_capabilities.get(capability)
    }
}

impl CoreCapabilities {
    pub(crate) fn max_size_upload(&self) -> Option<usize> {
        self.max_size_upload
    }

    pub(crate) fn max_concurrent_upload(&self) -> Option<usize> {
        self.max_concurrent_upload
    }

    pub(crate) fn max_size_request(&self) -> Option<usize> {
        self.max_size_request
    }

    pub(crate) fn max_concurrent_requests(&self) -> Option<usize> {
        self.max_concurrent_requests
    }

    pub(crate) fn max_calls_in_request(&self) -> Option<usize> {
        self.max_calls_in_request
    }

    pub(crate) fn max_objects_in_get(&self) -> Option<usize> {
        self.max_objects_in_get
    }

    pub(crate) fn max_objects_in_set(&self) -> Option<usize> {
        self.max_objects_in_set
    }

    pub(crate) fn collation_algorithms(&self) -> &[String] {
        self.collation_algorithms.as_deref().unwrap_or_default()
    }

    pub(crate) fn has_collation_algorithms(&self) -> bool {
        self.collation_algorithms.is_some()
    }
}

impl WebSocketCapabilities {
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn supports_push(&self) -> bool {
        self.supports_push
    }
}

impl SieveCapabilities {
    pub(crate) fn max_script_name_size(&self) -> usize {
        self.max_script_name.unwrap_or(512)
    }

    pub(crate) fn max_script_size(&self) -> Option<usize> {
        self.max_script_size
    }

    pub(crate) fn max_number_scripts(&self) -> Option<usize> {
        self.max_scripts
    }

    pub(crate) fn max_number_redirects(&self) -> Option<usize> {
        self.max_redirects
    }

    pub(crate) fn sieve_extensions(&self) -> &[String] {
        &self.extensions
    }

    pub(crate) fn notification_methods(&self) -> Option<&[String]> {
        self.notification_methods.as_deref()
    }

    pub(crate) fn external_lists(&self) -> Option<&[String]> {
        self.ext_lists.as_deref()
    }
}

impl BlobCapabilities {
    pub(crate) fn max_size_blob_set(&self) -> Option<u64> {
        self.max_size_blob_set
    }

    pub(crate) fn supported_digest_algorithms(&self) -> &[String] {
        &self.supported_digest_algorithms
    }

    pub(crate) fn supported_type_names(&self) -> &[String] {
        &self.supported_type_names
    }
}

impl CalendarsCapabilities {
    pub(crate) fn may_create_calendar(&self) -> bool {
        self.may_create_calendar
    }

    pub(crate) fn max_calendars_per_event(&self) -> Option<usize> {
        self.max_calendars_per_event
    }
}

impl ContactsCapabilities {
    pub(crate) fn may_create_address_book(&self) -> bool {
        self.may_create_address_book
    }

    pub(crate) fn max_address_books_per_card(&self) -> Option<usize> {
        self.max_address_books_per_card
    }
}

/// Capabilities for `urn:ietf:params:jmap:principals:owner` (RFC 9670).
///
/// Account-level capability indicating the owner of the account's data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrincipalsOwnerCapabilities {
    #[serde(rename = "accountIdForPrincipal")]
    #[serde(default)]
    account_id_for_principal: Option<crate::core::id::AccountId>,

    #[serde(rename = "principalId")]
    #[serde(default)]
    principal_id: Option<crate::principal::PrincipalId>,
}

impl PrincipalsOwnerCapabilities {
    pub(crate) fn account_id_for_principal(&self) -> Option<&crate::core::id::AccountId> {
        self.account_id_for_principal.as_ref()
    }

    pub(crate) fn principal_id(&self) -> Option<&crate::principal::PrincipalId> {
        self.principal_id.as_ref()
    }
}

impl PrincipalsCapabilities {
    pub(crate) fn current_user_principal_id(&self) -> Option<&crate::principal::PrincipalId> {
        self.current_user_principal_id.as_ref()
    }

    pub(crate) fn account_id_for_principal(&self) -> Option<&crate::core::id::AccountId> {
        self.account_id_for_principal.as_ref()
    }
}

pub(crate) trait URLParser: Sized {
    fn parse(value: &str) -> Option<Self>;
}

/// The set to percent-encode when substituting a value into a URI
/// template variable.
///
/// RFC 6570 §3.2.2 (simple string expansion, the only form JMAP session
/// templates use) says a value is expanded by percent-encoding every
/// character that is not *unreserved* - ALPHA / DIGIT / `-` / `.` / `_`
/// / `~`. An ad-hoc deny list is the wrong shape here: it silently lets
/// through whatever reserved character nobody thought of, and each one
/// is a different injection. `+` in a query-position `{type}` is the
/// cheapest example - `application/ld+json` arrives at the server as
/// `application/ld json` - but `;`, `@`, `!`, `,` and `$` all carry
/// delimiter meaning somewhere in RFC 3986.
pub(crate) const TEMPLATE_VALUE: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent-encode `value` for substitution into a URI template variable.
pub(crate) fn encode_template_value(value: &str) -> percent_encoding::PercentEncode<'_> {
    percent_encoding::utf8_percent_encode(value, TEMPLATE_VALUE)
}

/// Expand a variable in the grammar at its actual substitution position.
/// Complete dot path segments must survive WHATWG URL normalization.
pub(crate) fn encode_template_value_at(value: &str, prefix: &str) -> String {
    if prefix.contains('?') || prefix.contains('#') {
        bifrost_net::url::encode_query_value(value)
    } else {
        bifrost_net::url::encode_path_component(value)
    }
}

#[non_exhaustive]
pub(crate) enum URLPart<T: URLParser> {
    Value(String),
    Parameter(T),
}

impl<T: URLParser> URLPart<T> {
    /// Split a URI template into literals and known parameters.
    ///
    /// The error is a bare description, not a `crate::Error`: this parser
    /// cannot know who wrote the template, and that decides the blame. Every
    /// production template comes from the server's session object, so the
    /// caller maps it to [`crate::Error::MalformedSessionUrl`] naming the
    /// property; filing it as the caller's `InvalidUrl` blamed the client's
    /// request for the provider's malformed session.
    pub(crate) fn parse(url: &str) -> Result<Vec<URLPart<T>>, String> {
        let mut parts = Vec::new();
        let mut buf = String::with_capacity(url.len());
        let mut in_parameter = false;

        for ch in url.chars() {
            match ch {
                '{' => {
                    if in_parameter {
                        return Err(format!("nested '{{' in URL template: {url}"));
                    }
                    if !buf.is_empty() {
                        parts.push(URLPart::Value(std::mem::take(&mut buf)));
                    }
                    in_parameter = true;
                }
                '}' => {
                    if in_parameter && !buf.is_empty() {
                        parts.push(URLPart::Parameter(T::parse(&buf).ok_or_else(|| {
                            format!("unknown parameter '{buf}' in URL template: {url}")
                        })?));
                        buf.clear();
                    } else {
                        return Err(format!("unmatched or empty '}}' in URL template: {url}"));
                    }
                    in_parameter = false;
                }
                _ => {
                    buf.push(ch);
                }
            }
        }

        if in_parameter {
            return Err(format!("unterminated parameter in URL template: {url}"));
        }

        if !buf.is_empty() {
            parts.push(URLPart::Value(std::mem::take(&mut buf)));
        }

        Ok(parts)
    }
}

#[cfg(test)]
mod tests {
    use super::{CapabilityState, Session};

    fn session_with_multiple_primaries() -> Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {
                "urn:ietf:params:jmap:mail": "mail-account",
                "urn:ietf:params:jmap:calendars": "calendar-account",
                "urn:ietf:params:jmap:contacts": "contacts-account"
            },
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/events",
            "state": "state-1"
        }))
        .expect("session fixture decodes")
    }

    #[test]
    fn template_values_preserve_dot_paths_and_query_values() {
        for (value, escaped) in [(".", "%252E"), ("..", "%252E%252E")] {
            let prefix = "https://example.test/download/account/blob/";
            let expanded = format!("{prefix}{}", super::encode_template_value_at(value, prefix));
            let parsed = reqwest::Url::parse(&expanded).unwrap();
            assert_eq!(parsed.path(), format!("/download/account/blob/{escaped}"));
            assert_eq!(
                super::encode_template_value_at(value, "https://example.test/?name="),
                value
            );
        }
        assert_eq!(
            super::encode_template_value_at("a/b?x#+", "https://example.test/"),
            "a%2Fb%3Fx%23%2B"
        );
    }

    #[test]
    fn default_account_id_is_stable_across_session_map_seeds() {
        for _ in 0..32 {
            let session = session_with_multiple_primaries();
            assert_eq!(session.default_account_id(), Some("calendar-account"));
        }
    }

    #[test]
    fn capability_config_does_not_reparse_malformed_sequence_as_present() {
        let session: Session = serde_json::from_value(serde_json::json!({
            "capabilities": {"urn:ietf:params:jmap:core": []},
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/events",
            "state": "state-1"
        }))
        .expect("the malformed capability is retained in the session");

        assert!(matches!(
            session.core_capability_state(),
            CapabilityState::Malformed
        ));
        assert!(
            session
                .capability_config::<crate::core::capability::Core>()
                .is_none()
        );
    }
}
