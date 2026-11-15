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
    /// Creates an address with only an email, validating the minimal syntax
    /// Bifrost needs at this layer.
    pub fn new(email: impl Into<String>) -> Result<Self, super::ValidationError> {
        let email = email.into();
        validate_email(&email)?;
        Ok(Self { name: None, email })
    }

    /// Creates an address with a display name and email.
    pub fn with_name(
        name: impl Into<String>,
        email: impl Into<String>,
    ) -> Result<Self, super::ValidationError> {
        let email = email.into();
        validate_email(&email)?;
        Ok(Self {
            name: Some(name.into()),
            email,
        })
    }

    /// Creates an address without validating the email syntax.
    ///
    /// This is intended for parser and protocol conversion code that must
    /// preserve server-provided data.
    pub fn new_unchecked(name: Option<String>, email: String) -> Self {
        Self { name, email }
    }
}

fn validate_email(email: &str) -> Result<(), super::ValidationError> {
    let Some((local, domain)) = email.split_once('@') else {
        return Err(super::ValidationError::new(
            super::validated::ValidationReason::MissingAddressSeparator,
            "email address must contain an @ separator",
        ));
    };
    if local.is_empty() || domain.is_empty() || domain.contains('@') {
        return Err(super::ValidationError::new(
            super::validated::ValidationReason::InvalidAddressParts,
            "email address must contain non-empty local and domain parts",
        ));
    }
    Ok(())
}
