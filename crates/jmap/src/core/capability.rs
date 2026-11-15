use serde::de::DeserializeOwned;

/// A JMAP capability identified by its URN.
///
/// `Config` describes the capability object decoded by the typed reader.
/// [`super::session::Session::capability_config`] selects the session object
/// or the primary account's object according to the capability URI.
/// Capabilities with no configuration (empty JSON object) should use `()`.
pub(crate) trait Capability {
    const LEVEL: CapabilityLevel = CapabilityLevel::Account;
    const URI: &'static str;

    /// Capability configuration decoded from the object selected by the reader.
    type Config: DeserializeOwned + Send + Sync + 'static;

    fn config(value: &session::Capabilities) -> Option<Self::Config>;
}

use super::session;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CapabilityLevel {
    Session,
    Account,
}

pub(crate) struct Core;
impl Capability for Core {
    const URI: &'static str = "urn:ietf:params:jmap:core";
    type Config = session::CoreCapabilities;
    const LEVEL: CapabilityLevel = CapabilityLevel::Session;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        if let session::Capabilities::Core(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct Mail;
impl Capability for Mail {
    const URI: &'static str = "urn:ietf:params:jmap:mail";
    #[cfg(feature = "mail")]
    type Config = crate::email::MailCapabilities;
    #[cfg(not(feature = "mail"))]
    type Config = serde_json::Value;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        #[cfg(feature = "mail")]
        if let session::Capabilities::Mail(value) = value {
            return Some(value.clone());
        }
        #[cfg(not(feature = "mail"))]
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct Submission;
impl Capability for Submission {
    const URI: &'static str = "urn:ietf:params:jmap:submission";
    #[cfg(feature = "mail")]
    type Config = crate::email::SubmissionCapabilities;
    #[cfg(not(feature = "mail"))]
    type Config = serde_json::Value;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        #[cfg(feature = "mail")]
        if let session::Capabilities::Submission(value) = value {
            return Some(value.clone());
        }
        #[cfg(not(feature = "mail"))]
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct VacationResponseCap;
impl Capability for VacationResponseCap {
    const URI: &'static str = "urn:ietf:params:jmap:vacationresponse";
    type Config = serde_json::Value;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct Contacts;
impl Capability for Contacts {
    const URI: &'static str = "urn:ietf:params:jmap:contacts";
    #[cfg(feature = "contacts")]
    type Config = session::ContactsCapabilities;
    #[cfg(not(feature = "contacts"))]
    type Config = serde_json::Value;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        #[cfg(feature = "contacts")]
        if let session::Capabilities::Contacts(value) = value {
            return Some(value.clone());
        }
        #[cfg(not(feature = "contacts"))]
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct ContactsParse;
impl Capability for ContactsParse {
    const URI: &'static str = "urn:ietf:params:jmap:contacts:parse";
    type Config = serde_json::Value;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct Calendars;
impl Capability for Calendars {
    const URI: &'static str = "urn:ietf:params:jmap:calendars";
    #[cfg(feature = "calendars")]
    type Config = session::CalendarsCapabilities;
    #[cfg(not(feature = "calendars"))]
    type Config = serde_json::Value;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        #[cfg(feature = "calendars")]
        if let session::Capabilities::Calendars(value) = value {
            return Some(value.clone());
        }
        #[cfg(not(feature = "calendars"))]
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct CalendarsParse;
impl Capability for CalendarsParse {
    const URI: &'static str = "urn:ietf:params:jmap:calendars:parse";
    type Config = serde_json::Value;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct Blob;
impl Capability for Blob {
    const URI: &'static str = "urn:ietf:params:jmap:blob";
    #[cfg(feature = "blob")]
    type Config = session::BlobCapabilities;
    #[cfg(not(feature = "blob"))]
    type Config = serde_json::Value;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        #[cfg(feature = "blob")]
        if let session::Capabilities::Blob(value) = value {
            return Some(value.clone());
        }
        #[cfg(not(feature = "blob"))]
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct Quota;
impl Capability for Quota {
    const URI: &'static str = "urn:ietf:params:jmap:quota";
    #[cfg(feature = "quota")]
    type Config = session::QuotaCapabilities;
    #[cfg(not(feature = "quota"))]
    type Config = serde_json::Value;
    const LEVEL: CapabilityLevel = CapabilityLevel::Session;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        #[cfg(feature = "quota")]
        if let session::Capabilities::Quota(value) = value {
            return Some(value.clone());
        }
        #[cfg(not(feature = "quota"))]
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct WebSocket;
impl Capability for WebSocket {
    const URI: &'static str = "urn:ietf:params:jmap:websocket";
    type Config = session::WebSocketCapabilities;
    const LEVEL: CapabilityLevel = CapabilityLevel::Session;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        if let session::Capabilities::WebSocket(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct Sieve;
impl Capability for Sieve {
    const URI: &'static str = "urn:ietf:params:jmap:sieve";
    #[cfg(feature = "mail")]
    type Config = session::SieveCapabilities;
    #[cfg(not(feature = "mail"))]
    type Config = serde_json::Value;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        #[cfg(feature = "mail")]
        if let session::Capabilities::Sieve(value) = value {
            return Some(value.clone());
        }
        #[cfg(not(feature = "mail"))]
        if let session::Capabilities::Other(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct Principals;
impl Capability for Principals {
    const URI: &'static str = "urn:ietf:params:jmap:principals";
    type Config = session::PrincipalsCapabilities;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        if let session::Capabilities::Principals(value) = value {
            return Some(value.clone());
        }
        None
    }
}

pub(crate) struct PrincipalsOwner;
impl Capability for PrincipalsOwner {
    const URI: &'static str = "urn:ietf:params:jmap:principals:owner";
    type Config = session::PrincipalsOwnerCapabilities;
    fn config(value: &session::Capabilities) -> Option<Self::Config> {
        if let session::Capabilities::PrincipalsOwner(value) = value {
            return Some(value.clone());
        }
        None
    }
}

/// Placement is declared once by each marker, including disabled extensions.
pub(crate) fn level_for_uri(uri: &str) -> Option<CapabilityLevel> {
    if uri == Core::URI {
        return Some(Core::LEVEL);
    }
    if uri == Mail::URI {
        return Some(Mail::LEVEL);
    }
    if uri == Submission::URI {
        return Some(Submission::LEVEL);
    }
    if uri == VacationResponseCap::URI {
        return Some(VacationResponseCap::LEVEL);
    }
    if uri == Contacts::URI {
        return Some(Contacts::LEVEL);
    }
    if uri == ContactsParse::URI {
        return Some(ContactsParse::LEVEL);
    }
    if uri == Calendars::URI {
        return Some(Calendars::LEVEL);
    }
    if uri == CalendarsParse::URI {
        return Some(CalendarsParse::LEVEL);
    }
    if uri == Blob::URI {
        return Some(Blob::LEVEL);
    }
    if uri == Quota::URI {
        return Some(Quota::LEVEL);
    }
    if uri == WebSocket::URI {
        return Some(WebSocket::LEVEL);
    }
    if uri == Sieve::URI {
        return Some(Sieve::LEVEL);
    }
    if uri == Principals::URI {
        return Some(Principals::LEVEL);
    }
    if uri == PrincipalsOwner::URI {
        return Some(PrincipalsOwner::LEVEL);
    }
    None
}
