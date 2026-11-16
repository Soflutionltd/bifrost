#![allow(clippy::unwrap_used)]

use super::*;

#[test]
fn address_email_both_parts() {
    let addr = EnvelopeAddress {
        name: Some("Alice".into()),
        adl: None,
        mailbox: Some("alice".into()),
        host: Some("example.com".into()),
    };
    assert_eq!(addr.email(), Some("alice@example.com".into()));
    assert!(addr.is_address());
    assert!(!addr.is_group_start());
    assert!(!addr.is_group_end());
}

#[test]
fn address_email_missing_host() {
    let addr = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: Some("alice".into()),
        host: None,
    };
    assert_eq!(addr.email(), None);
}

#[test]
fn address_email_missing_mailbox() {
    let addr = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: None,
        host: Some("example.com".into()),
    };
    assert_eq!(addr.email(), None);
}

#[test]
fn default_envelope_is_empty() {
    let env = Envelope::default();
    assert!(env.date.is_none());
    assert!(env.subject.is_none());
    assert!(env.from.is_empty());
    assert!(env.message_id.is_none());
}

// --- Group marker tests (RFC 3501 Section 7.4.2) ---

#[test]
fn group_start_marker() {
    let addr = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: Some("Friends".into()),
        host: None,
    };
    assert!(addr.is_group_start());
    assert!(!addr.is_group_end());
    assert!(!addr.is_address());
    assert_eq!(addr.email(), None);
}

#[test]
fn group_end_marker() {
    let addr = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: None,
        host: None,
    };
    assert!(addr.is_group_end());
    assert!(!addr.is_group_start());
    assert!(!addr.is_address());
}

// --- Envelope convenience methods ---

#[test]
fn bare_message_id_strips_brackets() {
    let env = Envelope {
        message_id: Some("<abc@example.com>".into()),
        ..Default::default()
    };
    assert_eq!(env.bare_message_id(), Some("abc@example.com"));
}

#[test]
fn bare_message_id_no_brackets() {
    let env = Envelope {
        message_id: Some("abc@example.com".into()),
        ..Default::default()
    };
    assert_eq!(env.bare_message_id(), Some("abc@example.com"));
}

#[test]
fn bare_message_id_nil() {
    let env = Envelope::default();
    assert_eq!(env.bare_message_id(), None);
}

#[test]
fn first_in_reply_to_single() {
    let env = Envelope {
        in_reply_to: Some("<parent@example.com>".into()),
        ..Default::default()
    };
    assert_eq!(env.first_in_reply_to(), Some("parent@example.com"));
}

#[test]
fn first_in_reply_to_multiple() {
    let env = Envelope {
        in_reply_to: Some("<first@a.com> <second@b.com>".into()),
        ..Default::default()
    };
    assert_eq!(env.first_in_reply_to(), Some("first@a.com"));
}

#[test]
fn first_in_reply_to_no_brackets() {
    let env = Envelope {
        in_reply_to: Some("bare-id@example.com".into()),
        ..Default::default()
    };
    assert_eq!(env.first_in_reply_to(), Some("bare-id@example.com"));
}

#[test]
fn first_in_reply_to_nil() {
    let env = Envelope::default();
    assert_eq!(env.first_in_reply_to(), None);
}

/// `first_in_reply_to()` must return `None` for empty angle brackets `<>`
/// (RFC 5322 Section 3.6.4: msg-id requires non-empty id-left "@" id-right).
#[test]
fn first_in_reply_to_empty_angle_brackets() {
    let env = Envelope {
        in_reply_to: Some("<>".into()),
        ..Default::default()
    };
    assert_eq!(
        env.first_in_reply_to(),
        None,
        "first_in_reply_to() must return None for empty angle brackets <> \
         (RFC 5322 Section 3.6.4)"
    );
}

/// `first_in_reply_to()` must return `None` for whitespace-only content
/// inside angle brackets `<  >` (RFC 5322 Section 3.6.4: msg-id requires
/// id-left "@" id-right, so whitespace alone is never a valid message-id).
#[test]
fn first_in_reply_to_whitespace_only_in_brackets() {
    let env = Envelope {
        in_reply_to: Some("<  >".into()),
        ..Default::default()
    };
    assert_eq!(
        env.first_in_reply_to(),
        None,
        "first_in_reply_to() must return None for whitespace-only angle brackets <  > \
         (RFC 5322 Section 3.6.4)"
    );
}

/// `first_in_reply_to()` must return `None` for a single space inside
/// angle brackets `< >` (RFC 5322 Section 3.6.4).
#[test]
fn first_in_reply_to_single_space_in_brackets() {
    let env = Envelope {
        in_reply_to: Some("< >".into()),
        ..Default::default()
    };
    assert_eq!(
        env.first_in_reply_to(),
        None,
        "first_in_reply_to() must return None for single-space angle brackets < > \
         (RFC 5322 Section 3.6.4)"
    );
}

/// `first_in_reply_to()` must trim surrounding whitespace from the
/// extracted message-id (RFC 5322 Section 3.6.4: CFWS around msg-id).
#[test]
fn first_in_reply_to_trims_whitespace() {
    let env = Envelope {
        in_reply_to: Some("< real@example.com >".into()),
        ..Default::default()
    };
    assert_eq!(
        env.first_in_reply_to(),
        Some("real@example.com"),
        "first_in_reply_to() must trim whitespace inside angle brackets \
         (RFC 5322 Section 3.6.4)"
    );
}

/// `first_in_reply_to()` returns the valid id for a normal bracketed input.
#[test]
fn first_in_reply_to_normal_bracketed() {
    let env = Envelope {
        in_reply_to: Some("<real@example.com>".into()),
        ..Default::default()
    };
    assert_eq!(env.first_in_reply_to(), Some("real@example.com"));
}

/// RFC 5322 Section 3.6.4: quoted explanatory text is not a msg-id.
/// `first_in_reply_to()` must ignore `<...>` that appears inside quotes
/// or comments and return the real identifier token.
#[test]
fn first_in_reply_to_ignores_quoted_and_commented_angle_brackets() {
    let env = Envelope {
        in_reply_to: Some(
            "\"quoted <quoted@example.com>\" (comment <comment@example.com>) <real@example.com>"
                .into(),
        ),
        ..Default::default()
    };
    assert_eq!(env.first_in_reply_to(), Some("real@example.com"));
}

/// `bare_message_id()` must return `None` for empty angle brackets `<>`.
#[test]
fn bare_message_id_empty_angle_brackets() {
    let env = Envelope {
        message_id: Some("<>".into()),
        ..Default::default()
    };
    assert_eq!(
        env.bare_message_id(),
        None,
        "bare_message_id() must return None for empty angle brackets <>"
    );
}

/// `bare_message_id()` must return `None` for whitespace-only content
/// inside angle brackets `< >` (RFC 5322 Section 3.6.4: msg-id requires
/// id-left "@" id-right, so whitespace alone is never a valid message-id).
#[test]
fn bare_message_id_whitespace_only_in_brackets() {
    let env = Envelope {
        message_id: Some("< >".into()),
        ..Default::default()
    };
    assert_eq!(
        env.bare_message_id(),
        None,
        "bare_message_id() must return None for whitespace-only angle brackets < > \
         (RFC 5322 Section 3.6.4)"
    );
}

/// `bare_message_id()` must return `None` for multi-space content
/// inside angle brackets `<   >` (RFC 5322 Section 3.6.4).
#[test]
fn bare_message_id_multi_space_in_brackets() {
    let env = Envelope {
        message_id: Some("<   >".into()),
        ..Default::default()
    };
    assert_eq!(
        env.bare_message_id(),
        None,
        "bare_message_id() must return None for multi-space angle brackets <   > \
         (RFC 5322 Section 3.6.4)"
    );
}

/// `email()` must return `None` when mailbox or host is an empty string,
/// because `@host` / `mailbox@` / `@` are not valid addr-spec values
/// (RFC 5322 Section 3.4.1).
#[test]
fn address_email_rejects_empty_strings() {
    let both_empty = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: Some(String::new()),
        host: Some(String::new()),
    };
    assert_eq!(
        both_empty.email(),
        None,
        "email() must return None when both mailbox and host are empty strings \
         (RFC 5322 Section 3.4.1: addr-spec = local-part '@' domain)"
    );

    let empty_mailbox = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: Some(String::new()),
        host: Some("example.com".into()),
    };
    assert_eq!(
        empty_mailbox.email(),
        None,
        "email() must return None when mailbox is empty \
         (RFC 5322 Section 3.4.1)"
    );

    let empty_host = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: Some("alice".into()),
        host: Some(String::new()),
    };
    assert_eq!(
        empty_host.email(),
        None,
        "email() must return None when host is empty \
         (RFC 5322 Section 3.4.1)"
    );
}

/// `bare_message_id()` must trim surrounding whitespace from the
/// extracted message-id (RFC 5322 Section 3.6.4: CFWS around msg-id).
#[test]
fn bare_message_id_trims_whitespace() {
    let env = Envelope {
        message_id: Some("< real@example.com >".into()),
        ..Default::default()
    };
    assert_eq!(
        env.bare_message_id(),
        Some("real@example.com"),
        "bare_message_id() must trim whitespace inside angle brackets \
         (RFC 5322 Section 3.6.4)"
    );
}

/// RFC 5322 Section 3.6.4: malformed leading quoted/commented `<...>` text
/// must not displace the actual Message-ID token for this convenience
/// accessor.
#[test]
fn bare_message_id_ignores_quoted_and_commented_angle_brackets() {
    let env = Envelope {
        message_id: Some(
            "\"quoted <quoted@example.com>\" (comment <comment@example.com>) <real@example.com>"
                .into(),
        ),
        ..Default::default()
    };
    assert_eq!(env.bare_message_id(), Some("real@example.com"));
}

// --- EnvelopeAddress is distinct from crate::types::Address ---

/// Verify that `EnvelopeAddress` exists as a distinct type from `crate::types::Address`
/// and has the correct IMAP envelope fields (name, adl, mailbox, host) per
/// RFC 3501 Section 7.4.2.
#[test]
fn envelope_address_is_distinct_from_message_address() {
    // EnvelopeAddress is the IMAP 4-tuple (RFC 3501 Section 7.4.2).
    let env_addr = EnvelopeAddress {
        name: Some("Alice".into()),
        adl: None,
        mailbox: Some("alice".into()),
        host: Some("example.com".into()),
    };
    assert_eq!(env_addr.name.as_deref(), Some("Alice"));
    assert!(env_addr.adl.is_none());
    assert_eq!(env_addr.mailbox.as_deref(), Some("alice"));
    assert_eq!(env_addr.host.as_deref(), Some("example.com"));

    // crate::types::Address is a simpler name+email pair (RFC 5322 Section 3.4).
    let msg_addr =
        crate::types::Address::new_unchecked(Some("Alice".into()), "alice@example.com".into());
    assert_eq!(msg_addr.name.as_deref(), Some("Alice"));
    assert_eq!(msg_addr.email, "alice@example.com");

    // The two types are fundamentally different  -  EnvelopeAddress has adl/mailbox/host,
    // while crate::types::Address has a single email field.
}

// --- Address -> crate::types::Address conversion  ---

#[test]
fn to_message_address_normal() {
    let addr = EnvelopeAddress {
        name: Some("Alice".into()),
        adl: None,
        mailbox: Some("alice".into()),
        host: Some("example.com".into()),
    };
    let msg_addr = addr.to_message_address().unwrap();
    assert_eq!(msg_addr.name.as_deref(), Some("Alice"));
    assert_eq!(msg_addr.email, "alice@example.com");
}

#[test]
fn to_message_address_group_marker_returns_none() {
    let group_start = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: Some("undisclosed".into()),
        host: None,
    };
    assert!(
        group_start.to_message_address().is_none(),
        "group start marker must return None"
    );
}

#[test]
fn from_ref_conversion() {
    let addr = EnvelopeAddress {
        name: Some("Bob".into()),
        adl: None,
        mailbox: Some("bob".into()),
        host: Some("test.com".into()),
    };
    let msg_addr: crate::types::Address = (&addr).into();
    assert_eq!(msg_addr.name.as_deref(), Some("Bob"));
    assert_eq!(msg_addr.email, "bob@test.com");
}

#[test]
fn from_owned_conversion() {
    let addr = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: Some("user".into()),
        host: Some("domain.org".into()),
    };
    let msg_addr: crate::types::Address = addr.into();
    assert!(msg_addr.name.is_none());
    assert_eq!(msg_addr.email, "user@domain.org");
}

#[test]
fn from_conversion_group_marker_empty_email() {
    let addr = EnvelopeAddress {
        name: None,
        adl: None,
        mailbox: None,
        host: None,
    };
    let msg_addr: crate::types::Address = (&addr).into();
    assert!(msg_addr.email.is_empty());
}

/// Both `From<&EnvelopeAddress>` and `From<EnvelopeAddress>` must produce identical results
/// for addresses with empty mailbox/host parts.
#[test]
fn from_ref_and_owned_match_for_empty_parts() {
    let cases = [
        EnvelopeAddress {
            name: Some("X".into()),
            adl: None,
            mailbox: Some(String::new()),
            host: Some("h.com".into()),
        },
        EnvelopeAddress {
            name: None,
            adl: None,
            mailbox: Some("u".into()),
            host: Some(String::new()),
        },
        EnvelopeAddress {
            name: None,
            adl: None,
            mailbox: Some(String::new()),
            host: Some(String::new()),
        },
        EnvelopeAddress {
            name: None,
            adl: None,
            mailbox: None,
            host: Some("h.com".into()),
        },
    ];
    for addr in &cases {
        let from_ref: crate::types::Address = addr.into();
        let from_owned: crate::types::Address = addr.clone().into();
        assert_eq!(
            from_ref.email, from_owned.email,
            "From<&EnvelopeAddress> and From<EnvelopeAddress> must produce the same email for {addr:?}"
        );
        assert_eq!(
            from_ref.name, from_owned.name,
            "From<&EnvelopeAddress> and From<EnvelopeAddress> must produce the same name for {addr:?}"
        );
    }
}
