//! Secret string storage for credentials and SASL payloads.

use std::fmt;
use std::ops::Deref;

use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

/// A string that zeroizes its allocation on drop and redacts under `Debug`.
///
/// `as_str`, `as_bytes`, and `Deref<Target = str>` intentionally expose the
/// raw secret to authentication code. Callers should treat those borrows like
/// any other credential material and avoid formatting or logging them.
#[repr(transparent)]
#[derive(Clone, Default, Eq)]
pub struct Secret(Zeroizing<String>);

impl Secret {
    /// Borrow the unredacted secret as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Borrow the unredacted secret as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Consume the wrapper and return the zeroizing string.
    pub fn into_zeroizing(self) -> Zeroizing<String> {
        self.0
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(Zeroizing::new(value))
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(Zeroizing::new(value.to_owned()))
    }
}

impl From<Zeroizing<String>> for Secret {
    fn from(value: Zeroizing<String>) -> Self {
        Self(value)
    }
}

impl Deref for Secret {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for Secret {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        // Constant-time over equal-length inputs; `subtle`'s slice ct_eq
        // short-circuits on a length mismatch, so the LENGTH of a secret can
        // leak through timing. Accepted: lengths here are not secret.
        bool::from(self.as_bytes().ct_eq(other.as_bytes()))
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn eq_compares_content_including_length_mismatch() {
        assert_eq!(Secret::from("hunter2"), Secret::from("hunter2"));
        assert_ne!(Secret::from("hunter2"), Secret::from("hunter3"));
        // A prefix compares unequal; `subtle` short-circuits on the length
        // mismatch, whose timing leak is accepted because secret lengths are
        // not sensitive here.
        assert_ne!(Secret::from("hunter2"), Secret::from("hunter2x"));
        assert_ne!(Secret::from("hunter2"), Secret::from(""));
        assert_eq!(Secret::default(), Secret::from(""));
    }

    #[test]
    fn debug_is_redacted() {
        let secret = Secret::from("hunter2");
        assert_eq!(format!("{secret:?}"), "<redacted>");
        // The redaction never leaks any part of the value.
        assert!(!format!("{secret:?}").contains("hunter"));
    }

    #[test]
    fn conversions_expose_the_inner_value_to_auth_code() {
        let secret = Secret::from(String::from("s3cret"));
        assert_eq!(secret.as_str(), "s3cret");
        assert_eq!(secret.as_bytes(), b"s3cret");
        assert_eq!(&*secret, "s3cret");
        assert_eq!(secret.as_ref(), "s3cret");
        assert_eq!(secret.clone().into_zeroizing().as_str(), "s3cret");
        let from_zeroizing = Secret::from(Zeroizing::new(String::from("s3cret")));
        assert_eq!(from_zeroizing, secret);
    }
}
