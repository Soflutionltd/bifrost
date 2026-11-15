//! One identity rule for provider-reported Gmail mailbox addresses.

/// Canonical comparison key, retaining Gmail's ASCII-insensitive identity rule.
/// Do not rewrite aliases, dots, plus suffixes or googlemail.com domains: users
/// expect distinct enterprise mailboxes to stay distinct unless Google identifies
/// them as the same mailbox in getProfile.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MailboxAddress(String);

impl MailboxAddress {
    // Callers pass the provider profile value or its persisted cursor copy.
    // This is an identity key, not an address validator or proof of provenance:
    // the cursor and upload helpers also consume stored strings. Keeping that
    // boundary explicit avoids treating caller-selected send-as aliases as the
    // owning mailbox, which would coordinate unrelated account watches.
    pub(super) fn from_provider_value(value: &str) -> Self {
        Self(value.to_ascii_lowercase())
    }

    pub(super) fn matches(&self, value: &str) -> bool {
        self.0.eq_ignore_ascii_case(value)
    }

    pub(super) fn domain(&self) -> Option<&str> {
        // Identity comparison accepts opaque profile strings, but sharing must
        // refuse malformed addresses before uploading: users expect an
        // organization share to stay inside the actual mailbox domain.
        let (local, domain) = self.0.split_once('@')?;
        let atom =
            |byte: u8| byte.is_ascii_alphanumeric() || b"!#$%&'*+-/=?^_`{|}~".contains(&byte);
        if local.is_empty()
            || local.len() > 64
            || self.0.len() > 254
            || !local
                .split('.')
                .all(|part| !part.is_empty() && part.bytes().all(atom))
            || domain.len() > 253
            || !domain.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && label.as_bytes()[0].is_ascii_alphanumeric()
                    && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                    && label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
        {
            return None;
        }
        Some(domain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sharing_refuses_malformed_profile_addresses() {
        for value in [
            "a@b@example.com",
            "@example.com",
            "a@",
            "a b@example.com",
            "a..b@example.com",
            "a@.example.com",
            "a@example..com",
            "a@-example.com",
            "a@example.com/path",
            "a@example.com\n",
        ] {
            assert_eq!(
                MailboxAddress::from_provider_value(value).domain(),
                None,
                "{value:?}"
            );
        }
    }

    #[test]
    fn identity_uses_case_but_never_guesses_aliases() {
        let address = MailboxAddress::from_provider_value("Person@Example.Test");
        assert!(address.matches("person@example.test"));
        assert_eq!(address.domain(), Some("example.test"));
        assert!(!address.matches("person+alias@example.test"));
        assert_ne!(
            MailboxAddress::from_provider_value("a@gmail.com"),
            MailboxAddress::from_provider_value("a@googlemail.com")
        );
    }
}
