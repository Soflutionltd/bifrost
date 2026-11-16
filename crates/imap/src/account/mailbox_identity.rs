//! Account mailbox ids and stable account ordering.

use crate::types::validated::{MailboxName, ValidationError, ValidationReason};

impl MailboxName {
    /// Stable account identity. Ordinary canonical names keep their existing ids;
    /// noncanonical wire addresses and reserved-prefix names use an escaped form.
    pub(crate) fn account_id(&self) -> String {
        use base64::Engine;
        const PREFIX: &str = "~imap-wire:";
        if self.can_reencode()
            && !self.as_str().starts_with(PREFIX)
            && Self::new(self.as_str()).is_ok()
        {
            return self.as_str().to_owned();
        }
        let (utf8, bytes) = self.identity();
        let mut payload = vec![u8::from(*utf8)];
        payload.extend_from_slice(bytes);
        format!(
            "{PREFIX}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
        )
    }

    /// Stable display ordering with canonical wire identity as the tie-breaker.
    pub(crate) fn cmp_account_order(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str()).then_with(|| {
            let (mode, bytes) = self.identity();
            let (other_mode, other_bytes) = other.identity();
            bytes.cmp(other_bytes).then(mode.cmp(other_mode))
        })
    }

    pub(crate) fn from_account_id(id: &str) -> Result<Self, ValidationError> {
        use base64::Engine;
        let Some(payload) = id.strip_prefix("~imap-wire:") else {
            return Self::new(id);
        };
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| {
                ValidationError::new(
                    ValidationReason::InvalidMailboxIdentity,
                    "invalid mailbox identity",
                )
            })?;
        let Some((&mode, bytes)) = payload.split_first() else {
            return Err(ValidationError::new(
                ValidationReason::InvalidMailboxIdentity,
                "empty mailbox identity",
            ));
        };
        if mode > 1 || bytes.contains(&0) {
            return Err(ValidationError::new(
                ValidationReason::InvalidMailboxIdentity,
                "invalid mailbox identity payload",
            ));
        }
        let (display, _) = crate::types::wire::WireMode::from(mode == 1).decode(bytes);
        Ok(Self::from_wire(display, bytes, mode == 1))
    }
}

#[cfg(test)]
mod account_identity_tests {
    use super::MailboxName;

    #[test]
    fn escaped_account_ids_keep_their_persisted_bytes() {
        // Fixed vectors pin the mode byte, wire octets and unpadded base64url
        // spelling independently of the encoder/decoder round-trip.
        let cases = [
            (
                MailboxName::from_wire("a".into(), b"&AGE-", false),
                "~imap-wire:ACZBR0Ut",
            ),
            (
                MailboxName::from_wire("\u{fffd}".into(), &[0xff], true),
                "~imap-wire:Af8",
            ),
            (
                MailboxName::new("~imap-wire:literal").unwrap(),
                "~imap-wire:AH5pbWFwLXdpcmU6bGl0ZXJhbA",
            ),
        ];
        for (name, expected) in cases {
            assert_eq!(name.account_id(), expected);
            assert_eq!(MailboxName::from_account_id(expected).unwrap(), name);
        }
    }

    #[test]
    fn equal_canonical_names_have_equal_escaped_ids_in_both_modes() {
        for display in ["~imap-wire:literal", "~imap-wire:旅行", "line\r\nbreak"] {
            let caller = MailboxName::from_decoded(display.into());
            let rev1_bytes = crate::types::wire::encode_utf7(display).into_bytes();
            let rev1 = MailboxName::from_wire(display.into(), &rev1_bytes, false);
            let utf8 = MailboxName::from_wire(display.into(), display.as_bytes(), true);
            for name in [caller.clone(), rev1, utf8] {
                assert_eq!(name, caller);
                assert_eq!(name.account_id(), caller.account_id());
                assert_eq!(
                    MailboxName::from_account_id(&name.account_id()).unwrap(),
                    caller
                );
            }
        }
    }
}
