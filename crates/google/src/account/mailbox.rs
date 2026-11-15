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
        self.0
            .rsplit_once('@')
            .map(|(_, domain)| domain)
            .filter(|domain| !domain.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
