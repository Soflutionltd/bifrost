// SetRequest::update starts from a default patch. A serialized null removes
// a property, so default patches must be empty across every object family.
mod patch_defaults {
    use serde_json::json;

    fn empty<T: serde::Serialize + Default>(label: &str) {
        assert_eq!(
            serde_json::to_value(T::default()).unwrap(),
            json!({}),
            "a default {label} must not remove any property"
        );
    }

    #[test]
    fn patches_that_are_correctly_empty() {
        #[cfg(feature = "mail")]
        {
            empty::<crate::email::EmailPatch>("EmailPatch");
            empty::<crate::email_submission::EmailSubmissionPatch>("EmailSubmissionPatch");
            empty::<crate::identity::IdentityPatch>("IdentityPatch");
            empty::<crate::mailbox::MailboxPatch>("MailboxPatch");
            empty::<crate::vacation_response::VacationResponsePatch>("VacationResponsePatch");
            empty::<crate::sieve::SieveScriptPatch>("SieveScriptPatch");
        }
        empty::<crate::principal::PrincipalPatch>("PrincipalPatch");
        empty::<crate::push_subscription::PushSubscriptionPatch>("PushSubscriptionPatch");
        #[cfg(feature = "calendars")]
        {
            empty::<crate::calendar::CalendarPatch>("CalendarPatch");
            empty::<crate::calendar_event::CalendarEventPatch>("CalendarEventPatch");
            empty::<crate::participant_identity::ParticipantIdentityPatch>(
                "ParticipantIdentityPatch",
            );
        }
        #[cfg(feature = "contacts")]
        {
            empty::<crate::address_book::AddressBookPatch>("AddressBookPatch");
            empty::<crate::contact_card::ContactCardPatch>("ContactCardPatch");
        }
    }

    #[cfg(feature = "mail")]
    #[test]
    fn explicit_empty_create_values_are_preserved() {
        use crate::core::SetCreate;
        let mut identity = crate::identity::IdentityCreate::new(None);
        identity.reply_to(Some(std::iter::empty::<crate::email::EmailAddress>()));
        assert_eq!(
            serde_json::to_value(identity).unwrap(),
            json!({"replyTo": []})
        );

        let mut mailbox = crate::mailbox::MailboxCreate::new(None);
        mailbox.acls(std::iter::empty::<(String, Vec<crate::mailbox::ACL>)>());
        assert_eq!(
            serde_json::to_value(mailbox).unwrap(),
            json!({"shareWith": {}})
        );

        let mut vacation = crate::vacation_response::VacationResponseCreate::new(None);
        vacation
            .from_date(Some(jiff::Timestamp::UNIX_EPOCH))
            .subject(Some(""));
        assert_eq!(
            serde_json::to_value(vacation).unwrap(),
            json!({"fromDate": "1970-01-01T00:00:00Z", "subject": ""})
        );
    }

    #[test]
    fn untouched_creates_omit_optional_fields() {
        use crate::core::SetCreate;
        #[cfg(feature = "contacts")]
        assert_eq!(
            serde_json::to_value(crate::address_book::AddressBookCreate::new(None)).unwrap(),
            json!({})
        );
        #[cfg(feature = "calendars")]
        assert_eq!(
            serde_json::to_value(crate::participant_identity::ParticipantIdentityCreate::new(
                Some(crate::core::creation::CreationId::named("c0"))
            ))
            .unwrap(),
            json!({"calendarAddress": ""})
        );
        #[cfg(feature = "mail")]
        {
            assert_eq!(
                serde_json::to_value(crate::mailbox::MailboxCreate::new(None)).unwrap(),
                json!({})
            );
            assert_eq!(
                serde_json::to_value(crate::identity::IdentityCreate::new(None)).unwrap(),
                json!({})
            );
            assert_eq!(
                serde_json::to_value(crate::vacation_response::VacationResponseCreate::new(None))
                    .unwrap(),
                json!({})
            );
        }
        assert_eq!(
            serde_json::to_value(crate::principal::PrincipalCreate::new(None)).unwrap(),
            json!({})
        );
    }
}
