mod data_type_wire {
    use crate::DataType;
    use serde_json::json;

    #[test]
    fn display_matches_the_serde_representation() {
        #[allow(unused_mut)]
        let mut types = vec![
            DataType::Core,
            DataType::PushSubscription,
            DataType::Principal,
            DataType::ShareNotification,
            DataType::FileNode,
        ];
        #[cfg(feature = "mail")]
        types.extend([
            DataType::Email,
            DataType::EmailDelivery,
            DataType::EmailSubmission,
            DataType::Mailbox,
            DataType::Thread,
            DataType::Identity,
            DataType::SearchSnippet,
            DataType::VacationResponse,
            DataType::Mdn,
            DataType::SieveScript,
        ]);
        #[cfg(feature = "calendars")]
        types.extend([
            DataType::Calendar,
            DataType::CalendarEvent,
            DataType::CalendarEventNotification,
            DataType::ParticipantIdentity,
            DataType::CalendarAlert,
        ]);
        #[cfg(feature = "contacts")]
        types.extend([DataType::AddressBook, DataType::ContactCard]);
        #[cfg(feature = "quota")]
        types.push(DataType::Quota);

        for data_type in types {
            let wire = serde_json::to_value(&data_type).unwrap();
            assert_eq!(
                wire,
                json!(data_type.to_string()),
                "Display and Serialize must agree for {data_type:?}"
            );
            assert_eq!(serde_json::from_value::<DataType>(wire).unwrap(), data_type);
        }
    }

    #[test]
    fn mdn_is_spelled_in_caps_on_the_wire() {
        #[cfg(feature = "mail")]
        assert_eq!(serde_json::to_value(DataType::Mdn).unwrap(), json!("MDN"));
    }

    #[test]
    fn other_preserves_its_wire_value() {
        let other = serde_json::from_str::<DataType>(r#""SomeFutureType""#).unwrap();
        assert_eq!(
            serde_json::to_value(other).unwrap(),
            json!("SomeFutureType")
        );
    }
}

mod unknown_wire_values {
    #[test]
    fn data_type_does_have_a_catch_all() {
        assert_eq!(
            serde_json::from_str::<crate::DataType>(r#""SomeFutureType""#).unwrap(),
            crate::DataType::Other("SomeFutureType".to_string())
        );
    }
}
