//! IMAP `ENVELOPE` type (RFC 3501 Section 7.4.2, RFC 9051 Section 7.5.2).
//!
//! All fields are owned strings. RFC 2047 encoded words are decoded at parse time
//! unless UTF8=ACCEPT (RFC 6855 Section 3) is active, in which case fields contain
//! raw UTF-8 per RFC 6532 Section 3. Non-UTF-8 charsets are lossy-converted to UTF-8.

/// A parsed IMAP ENVELOPE structure (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
///
/// Every field can be `None` because servers may return NIL for any of them.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct Envelope {
    /// Date from the Date header (RFC 5322 Section 3.3, RFC 3501 Section 7.4.2).
    pub date: Option<String>,
    /// Decoded subject (RFC 2047 Section 2, RFC 3501 Section 7.4.2).
    /// When UTF8=ACCEPT (RFC 6855 Section 3) is active, contains raw UTF-8
    /// per RFC 6532 Section 3.
    pub subject: Option<String>,
    /// From addresses (RFC 5322 Section 3.6.2, RFC 3501 Section 7.4.2).
    pub from: Vec<EnvelopeAddress>,
    /// Sender addresses (RFC 5322 Section 3.6.2, RFC 3501 Section 7.4.2).
    pub sender: Vec<EnvelopeAddress>,
    /// Reply-To addresses (RFC 5322 Section 3.6.2, RFC 3501 Section 7.4.2).
    pub reply_to: Vec<EnvelopeAddress>,
    /// To addresses (RFC 5322 Section 3.6.3, RFC 3501 Section 7.4.2).
    pub to: Vec<EnvelopeAddress>,
    /// CC addresses (RFC 5322 Section 3.6.3, RFC 3501 Section 7.4.2).
    pub cc: Vec<EnvelopeAddress>,
    /// BCC addresses (RFC 5322 Section 3.6.3, RFC 3501 Section 7.4.2).
    pub bcc: Vec<EnvelopeAddress>,
    /// In-Reply-To header (RFC 5322 Section 3.6.4, RFC 3501 Section 7.4.2).
    pub in_reply_to: Option<String>,
    /// Message-ID header (RFC 5322 Section 3.6.4, RFC 3501 Section 7.4.2).
    pub message_id: Option<String>,
}

impl Envelope {
    /// Extract the first message-id from `in_reply_to`, stripped of angle brackets.
    ///
    /// Convenience method  -  the raw value is preserved in the field.
    pub fn first_in_reply_to(&self) -> Option<&str> {
        let raw = self.in_reply_to.as_deref()?;
        bifrost_types::mime::first_message_id(raw)
    }

    /// Return the message-id stripped of angle brackets.
    ///
    /// Convenience method  -  the raw value is preserved in the field.
    pub fn bare_message_id(&self) -> Option<&str> {
        let raw = self.message_id.as_deref()?;
        bifrost_types::mime::first_message_id(raw)
    }
}

/// A single address entry from an ENVELOPE (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
///
/// Named `EnvelopeAddress` to distinguish from `crate::types::Address` (RFC 5322 Section 3.4),
/// which is a simpler name+email pair. This type carries the full IMAP 4-tuple.
///
/// Group syntax is represented as:
/// - **Group start**: `host` is `None`, `mailbox` holds the group name.
/// - **Group end**: both `host` and `mailbox` are `None`.
///
/// Inspect `host` and `mailbox` together to distinguish these markers.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct EnvelopeAddress {
    /// Display name, RFC 2047 decoded (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
    /// When UTF8=ACCEPT (RFC 6855 Section 3) is active, contains raw UTF-8
    /// per RFC 6532 Section 3.
    pub name: Option<String>,
    /// SMTP at-domain-list (source route)  -  almost always `None` in practice
    /// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
    pub adl: Option<String>,
    /// Mailbox (local-part), or group name for group-start markers
    /// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
    pub mailbox: Option<String>,
    /// Host (domain part), or `None` for group markers
    /// (RFC 3501 Section 7.4.2 / RFC 9051 Section 7.5.2).
    pub host: Option<String>,
}

impl EnvelopeAddress {
    /// Returns the full email address as `mailbox@host`, or `None` if either part is missing
    /// or empty (including for group markers).
    ///
    /// RFC 5322 Section 3.4.1: `addr-spec = local-part "@" domain`  -  both local-part
    /// and domain are required to be non-empty.
    pub fn email(&self) -> Option<String> {
        match (&self.mailbox, &self.host) {
            (Some(m), Some(h)) if !m.is_empty() && !h.is_empty() => Some(format!("{m}@{h}")),
            _ => None,
        }
    }

    /// Returns `true` if this is an RFC 5322 group start marker.
    ///
    /// Per RFC 3501 Section 7.4.2: host is NIL, mailbox holds the group name.
    #[cfg(test)]
    pub fn is_group_start(&self) -> bool {
        self.host.is_none() && self.mailbox.is_some()
    }

    /// Returns `true` if this is an RFC 5322 group end marker.
    ///
    /// Per RFC 3501 Section 7.4.2: both host and mailbox are NIL.
    #[cfg(test)]
    pub fn is_group_end(&self) -> bool {
        self.host.is_none() && self.mailbox.is_none()
    }

    /// Returns `true` if this is a real address (not a group marker).
    #[cfg(test)]
    pub fn is_address(&self) -> bool {
        self.host.is_some()
    }

    /// Converts this IMAP address to a `crate::types::Address`.
    ///
    /// Returns `None` for group markers (where both mailbox and host
    /// are not present). For real addresses, combines `mailbox@host`
    /// into the `email` field.
    ///
    /// This eliminates the boilerplate conversion that consumers otherwise
    /// need at every IMAP->message boundary.
    ///
    /// # References
    /// - RFC 3501 Section 7.4.2 (ENVELOPE address structure)
    /// - RFC 5322 Section 3.4 (address specification)
    pub fn to_message_address(&self) -> Option<crate::types::Address> {
        let email = self.email()?;
        Some(crate::types::Address::new_unchecked(
            self.name.clone(),
            email,
        ))
    }
}

/// Converts an IMAP ENVELOPE address to a `crate::types::Address`.
///
/// Group markers (where `email()` returns `None`) produce an `Address`
/// with an empty `email` field. Use [`EnvelopeAddress::to_message_address`] if
/// you need to filter those out.
///
/// # References
/// - RFC 3501 Section 7.4.2 (ENVELOPE address structure)
impl From<&EnvelopeAddress> for crate::types::Address {
    fn from(addr: &EnvelopeAddress) -> Self {
        Self::new_unchecked(addr.name.clone(), addr.email().unwrap_or_default())
    }
}

/// Owned conversion from IMAP envelope address to message address.
impl From<EnvelopeAddress> for crate::types::Address {
    fn from(addr: EnvelopeAddress) -> Self {
        let email = addr.email().unwrap_or_default();
        Self::new_unchecked(addr.name, email)
    }
}

#[cfg(test)]
#[path = "envelope_tests.rs"]
mod tests;
