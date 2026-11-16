//! Shared address type used by IMAP envelope conversion helpers.

/// A simple owned RFC 5322 address shape.
///
/// IMAP ENVELOPE data carries a four-field address tuple. This type is the
/// smaller name plus email representation callers usually want after converting
/// that tuple at the protocol boundary.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Address {
    /// Display name, if one was provided by the server.
    pub name: Option<String>,
    /// Email address in `local@domain` form.
    pub email: String,
}

impl Address {
    /// Creates an address without validating the email syntax.
    ///
    /// This is intended for parser and protocol conversion code that must
    /// preserve server-provided data.
    pub fn new_unchecked(name: Option<String>, email: String) -> Self {
        Self { name, email }
    }
}
