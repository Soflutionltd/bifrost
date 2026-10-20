use serde::de::DeserializeOwned;

/// A JMAP capability identified by its URN.
///
/// `Config` describes the session object for Core and WebSocket and the
/// account object for account-scoped extensions. Capabilities with no configuration
/// (empty JSON object) should use `()`.
pub(crate) trait Capability {
    const URI: &'static str;

    /// Capability configuration at its specified session or account level.
    type Config: DeserializeOwned + Send + Sync + 'static;
}

use super::session;

pub(crate) struct Core;
impl Capability for Core {
    const URI: &'static str = "urn:ietf:params:jmap:core";
    type Config = session::CoreCapabilities;
}

pub(crate) struct Mail;
impl Capability for Mail {
    const URI: &'static str = "urn:ietf:params:jmap:mail";
    #[cfg(feature = "mail")]
    type Config = crate::email::MailCapabilities;
    #[cfg(not(feature = "mail"))]
    type Config = serde_json::Value;
}

pub(crate) struct Submission;
impl Capability for Submission {
    const URI: &'static str = "urn:ietf:params:jmap:submission";
    #[cfg(feature = "mail")]
    type Config = crate::email::SubmissionCapabilities;
    #[cfg(not(feature = "mail"))]
    type Config = serde_json::Value;
}

pub(crate) struct VacationResponseCap;
impl Capability for VacationResponseCap {
    const URI: &'static str = "urn:ietf:params:jmap:vacationresponse";
    type Config = serde_json::Value;
}

pub(crate) struct Contacts;
impl Capability for Contacts {
    const URI: &'static str = "urn:ietf:params:jmap:contacts";
    #[cfg(feature = "contacts")]
    type Config = session::ContactsCapabilities;
    #[cfg(not(feature = "contacts"))]
    type Config = serde_json::Value;
}

pub(crate) struct ContactsParse;
impl Capability for ContactsParse {
    const URI: &'static str = "urn:ietf:params:jmap:contacts:parse";
    type Config = serde_json::Value;
}

pub(crate) struct Calendars;
impl Capability for Calendars {
    const URI: &'static str = "urn:ietf:params:jmap:calendars";
    #[cfg(feature = "calendars")]
    type Config = session::CalendarsCapabilities;
    #[cfg(not(feature = "calendars"))]
    type Config = serde_json::Value;
}

pub(crate) struct CalendarsParse;
impl Capability for CalendarsParse {
    const URI: &'static str = "urn:ietf:params:jmap:calendars:parse";
    type Config = serde_json::Value;
}

pub(crate) struct Blob;
impl Capability for Blob {
    const URI: &'static str = "urn:ietf:params:jmap:blob";
    #[cfg(feature = "blob")]
    type Config = session::BlobCapabilities;
    #[cfg(not(feature = "blob"))]
    type Config = serde_json::Value;
}

pub(crate) struct Quota;
impl Capability for Quota {
    const URI: &'static str = "urn:ietf:params:jmap:quota";
    #[cfg(feature = "quota")]
    type Config = session::QuotaCapabilities;
    #[cfg(not(feature = "quota"))]
    type Config = serde_json::Value;
}

pub(crate) struct WebSocket;
impl Capability for WebSocket {
    const URI: &'static str = "urn:ietf:params:jmap:websocket";
    type Config = session::WebSocketCapabilities;
}

pub(crate) struct Sieve;
impl Capability for Sieve {
    const URI: &'static str = "urn:ietf:params:jmap:sieve";
    #[cfg(feature = "mail")]
    type Config = session::SieveCapabilities;
    #[cfg(not(feature = "mail"))]
    type Config = serde_json::Value;
}

pub(crate) struct Principals;
impl Capability for Principals {
    const URI: &'static str = "urn:ietf:params:jmap:principals";
    type Config = session::PrincipalsCapabilities;
}

pub(crate) struct PrincipalsOwner;
impl Capability for PrincipalsOwner {
    const URI: &'static str = "urn:ietf:params:jmap:principals:owner";
    type Config = session::PrincipalsOwnerCapabilities;
}
