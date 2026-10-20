#![cfg(test)]

use serde_json::json;

// ---------------------------------------------------------------------------
// 1. CalendarEvent/ContactCard PatchObject null semantics
// ---------------------------------------------------------------------------

#[cfg(all(feature = "calendars", feature = "contacts"))]
mod patch_object_null_semantics {
    use super::*;
    use crate::calendar_event::CalendarEventCreate;
    use crate::contact_card::ContactCardCreate;
    use crate::core::SetCreate;

    #[test]
    fn calendar_event_calendar_id_false_produces_null() {
        let mut event = CalendarEventCreate::new(Some(0));
        event.calendar_id("cal-1", false);

        let value = serde_json::to_value(&event).unwrap();
        let calendar_ids = value.get("calendarIds").expect("calendarIds missing");
        let entry = calendar_ids.get("cal-1").expect("cal-1 missing");
        assert!(
            entry.is_null(),
            "calendar_id(id, false) must produce null, got {entry:?}"
        );
    }

    #[test]
    fn calendar_event_calendar_id_true_produces_true() {
        let mut event = CalendarEventCreate::new(Some(0));
        event.calendar_id("cal-1", true);

        let value = serde_json::to_value(&event).unwrap();
        let calendar_ids = value.get("calendarIds").unwrap();
        let entry = calendar_ids.get("cal-1").unwrap();
        assert_eq!(entry, &json!(true));
    }

    #[test]
    fn contact_card_address_book_id_false_produces_null() {
        let mut card = ContactCardCreate::new(Some(0));
        card.address_book_id("ab-1", false);

        let value = serde_json::to_value(&card).unwrap();
        let ab_ids = value.get("addressBookIds").expect("addressBookIds missing");
        let entry = ab_ids.get("ab-1").expect("ab-1 missing");
        assert!(
            entry.is_null(),
            "address_book_id(id, false) must produce null, got {entry:?}"
        );
    }

    #[test]
    fn contact_card_address_book_id_true_produces_true() {
        let mut card = ContactCardCreate::new(Some(0));
        card.address_book_id("ab-1", true);

        let value = serde_json::to_value(&card).unwrap();
        let ab_ids = value.get("addressBookIds").unwrap();
        let entry = ab_ids.get("ab-1").unwrap();
        assert_eq!(entry, &json!(true));
    }
}

// ---------------------------------------------------------------------------
// 2. CalendarEvent nullable getter semantics
// ---------------------------------------------------------------------------

#[cfg(feature = "calendars")]
mod calendar_event_nullable_getters {
    use crate::calendar_event::CalendarEvent;
    use crate::core::field::Field;

    /// Helper: deserialize a CalendarEvent from a JSON string.
    fn from_json(s: &str) -> CalendarEvent {
        serde_json::from_str(s).expect("failed to deserialize CalendarEvent")
    }

    // -- time_zone --

    #[test]
    fn time_zone_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.time_zone().is_omitted());
    }

    #[test]
    fn time_zone_null_returns_null() {
        let event = from_json(r#"{"timeZone":null}"#);
        assert!(event.time_zone().is_null());
    }

    #[test]
    fn time_zone_present_returns_value() {
        let event = from_json(r#"{"timeZone":"America/New_York"}"#);
        assert_eq!(event.time_zone(), Field::Value("America/New_York"));
    }

    // -- color --

    #[test]
    fn color_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.color().is_omitted());
    }

    #[test]
    fn color_null_returns_null() {
        let event = from_json(r#"{"color":null}"#);
        assert!(event.color().is_null());
    }

    #[test]
    fn color_present_returns_value() {
        let event = from_json(r##"{"color":"#ff0000"}"##);
        assert_eq!(event.color(), Field::Value("#ff0000"));
    }

    // -- locale --

    #[test]
    fn locale_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.locale().is_omitted());
    }

    #[test]
    fn locale_null_returns_null() {
        let event = from_json(r#"{"locale":null}"#);
        assert!(event.locale().is_null());
    }

    #[test]
    fn locale_present_returns_value() {
        let event = from_json(r#"{"locale":"en-US"}"#);
        assert_eq!(event.locale(), Field::Value("en-US"));
    }

    // -- alerts --

    #[test]
    fn alerts_absent_returns_omitted() {
        let event = from_json(r#"{"title":"test"}"#);
        assert!(event.alerts().is_omitted());
    }

    #[test]
    fn alerts_null_returns_null() {
        let event = from_json(r#"{"alerts":null}"#);
        assert!(
            event.alerts().is_null(),
            "null alerts should give Field::Null"
        );
    }

    #[test]
    fn alerts_present_returns_value() {
        let event = from_json(
            r#"{"alerts":{"a1":{"trigger":{"@type":"OffsetTrigger","offset":"-PT15M"},"action":"display"}}}"#,
        );
        let alerts = event.alerts();
        let map = alerts.as_value().expect("alerts should be Value(map)");
        assert!(map.contains_key("a1"));
    }
}

// ---------------------------------------------------------------------------
// 3. CalendarEvent/ContactCard round-trip with extension properties
// ---------------------------------------------------------------------------

#[cfg(all(feature = "calendars", feature = "contacts"))]
mod extension_property_round_trip {
    use super::*;
    use crate::calendar_event::CalendarEvent;
    use crate::contact_card::ContactCard;

    #[test]
    fn calendar_event_preserves_extension_properties() {
        let input = json!({
            "uid": "evt-1",
            "title": "Meeting",
            "example.com:custom-field": "custom-value",
            "vendor.io:priority": 42
        });

        let event: CalendarEvent = serde_json::from_value(input.clone()).expect("deser failed");

        // Verify typed accessors work
        assert_eq!(event.uid(), Some("evt-1"));
        assert_eq!(event.title(), Some("Meeting"));

        // Verify extension properties are accessible
        assert_eq!(
            event.property("example.com:custom-field"),
            Some(&json!("custom-value"))
        );
        assert_eq!(event.property("vendor.io:priority"), Some(&json!(42)));

        // Reserialize and verify extension properties survive
        let output = serde_json::to_value(&event).unwrap();
        assert_eq!(
            output.get("example.com:custom-field"),
            Some(&json!("custom-value"))
        );
        assert_eq!(output.get("vendor.io:priority"), Some(&json!(42)));
        assert_eq!(output.get("uid"), Some(&json!("evt-1")));
    }

    #[test]
    fn contact_card_preserves_extension_properties() {
        let input = json!({
            "uid": "card-1",
            "kind": "individual",
            "example.com:department": "Engineering"
        });

        let card: ContactCard = serde_json::from_value(input.clone()).expect("deser failed");

        assert_eq!(card.uid(), Some("card-1"));
        assert_eq!(card.kind(), Some("individual"));
        assert_eq!(
            card.property("example.com:department"),
            Some(&json!("Engineering"))
        );

        let output = serde_json::to_value(&card).unwrap();
        assert_eq!(
            output.get("example.com:department"),
            Some(&json!("Engineering"))
        );
        assert_eq!(output.get("uid"), Some(&json!("card-1")));
    }
}

// ---------------------------------------------------------------------------
// 4. Property enum round-trip
// ---------------------------------------------------------------------------

#[cfg(all(feature = "calendars", feature = "contacts"))]
mod property_enum_round_trip {
    use super::*;
    use crate::calendar_event::Property as CEProperty;
    use crate::contact_card::Property as CCProperty;

    /// All known CalendarEvent::Property variants and their wire names.
    fn ce_known_variants() -> Vec<(CEProperty, &'static str)> {
        vec![
            (CEProperty::Id, "id"),
            (CEProperty::Uid, "uid"),
            (CEProperty::CalendarIds, "calendarIds"),
            (CEProperty::IsDraft, "isDraft"),
            (CEProperty::Title, "title"),
            (CEProperty::Description, "description"),
            (CEProperty::DescriptionContentType, "descriptionContentType"),
            (CEProperty::Created, "created"),
            (CEProperty::Updated, "updated"),
            (CEProperty::Start, "start"),
            (CEProperty::Duration, "duration"),
            (CEProperty::TimeZone, "timeZone"),
            (CEProperty::ShowWithoutTime, "showWithoutTime"),
            (CEProperty::Status, "status"),
            (CEProperty::FreeBusyStatus, "freeBusyStatus"),
            (CEProperty::RecurrenceId, "recurrenceId"),
            (CEProperty::RecurrenceIdTimeZone, "recurrenceIdTimeZone"),
            (CEProperty::RecurrenceRules, "recurrenceRules"),
            (CEProperty::RecurrenceOverrides, "recurrenceOverrides"),
            (
                CEProperty::ExcludedRecurrenceRules,
                "excludedRecurrenceRules",
            ),
            (CEProperty::Priority, "priority"),
            (CEProperty::Color, "color"),
            (CEProperty::Locale, "locale"),
            (CEProperty::Keywords, "keywords"),
            (CEProperty::Categories, "categories"),
            (CEProperty::ProdId, "prodId"),
            (CEProperty::ReplyTo, "replyTo"),
            (CEProperty::Participants, "participants"),
            (CEProperty::UseDefaultAlerts, "useDefaultAlerts"),
            (CEProperty::Alerts, "alerts"),
            (CEProperty::Locations, "locations"),
            (CEProperty::VirtualLocations, "virtualLocations"),
            (CEProperty::Links, "links"),
            (CEProperty::RelatedTo, "relatedTo"),
            (CEProperty::ExcludedDates, "excludedDates"),
            (CEProperty::Localizations, "localizations"),
            (CEProperty::Method, "method"),
            (CEProperty::Sequence, "sequence"),
        ]
    }

    #[test]
    fn calendar_event_property_display_round_trip() {
        for (prop, wire_name) in ce_known_variants() {
            // Display -> &str -> From<&str>
            let displayed = prop.to_string();
            assert_eq!(displayed, wire_name, "Display mismatch for {prop:?}");
            let parsed = CEProperty::from(displayed.as_str());
            assert_eq!(parsed, prop, "From<&str> mismatch for {wire_name}");
        }
    }

    #[test]
    fn calendar_event_property_serde_round_trip() {
        for (prop, wire_name) in ce_known_variants() {
            let serialized = serde_json::to_value(&prop).unwrap();
            assert_eq!(serialized, json!(wire_name));
            let deserialized: CEProperty = serde_json::from_value(serialized).unwrap();
            assert_eq!(deserialized, prop);
        }
    }

    #[test]
    fn calendar_event_property_other_round_trip() {
        let prop = CEProperty::Other("example.com:custom".to_string());
        let displayed = prop.to_string();
        assert_eq!(displayed, "example.com:custom");
        let parsed = CEProperty::from("example.com:custom");
        assert_eq!(parsed, prop);

        let serialized = serde_json::to_value(&prop).unwrap();
        assert_eq!(serialized, json!("example.com:custom"));
        let deserialized: CEProperty = serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized, prop);
    }

    /// All known ContactCard::Property variants and their wire names.
    fn cc_known_variants() -> Vec<(CCProperty, &'static str)> {
        vec![
            (CCProperty::Id, "id"),
            (CCProperty::Uid, "uid"),
            (CCProperty::AddressBookIds, "addressBookIds"),
            (CCProperty::Kind, "kind"),
            (CCProperty::Name, "name"),
            (CCProperty::Nicknames, "nicknames"),
            (CCProperty::Emails, "emails"),
            (CCProperty::Phones, "phones"),
            (CCProperty::Addresses, "addresses"),
            (CCProperty::Organizations, "organizations"),
            (CCProperty::OnlineServices, "onlineServices"),
            (CCProperty::Notes, "notes"),
            (CCProperty::Media, "media"),
            (CCProperty::Created, "created"),
            (CCProperty::Updated, "updated"),
        ]
    }

    #[test]
    fn contact_card_property_display_round_trip() {
        for (prop, wire_name) in cc_known_variants() {
            let displayed = prop.to_string();
            assert_eq!(displayed, wire_name, "Display mismatch for {prop:?}");
            let parsed = CCProperty::from(displayed.as_str());
            assert_eq!(parsed, prop, "From<&str> mismatch for {wire_name}");
        }
    }

    #[test]
    fn contact_card_property_serde_round_trip() {
        for (prop, wire_name) in cc_known_variants() {
            let serialized = serde_json::to_value(&prop).unwrap();
            assert_eq!(serialized, json!(wire_name));
            let deserialized: CCProperty = serde_json::from_value(serialized).unwrap();
            assert_eq!(deserialized, prop);
        }
    }

    #[test]
    fn contact_card_property_other_round_trip() {
        let prop = CCProperty::Other("vendor:x-field".to_string());
        let displayed = prop.to_string();
        assert_eq!(displayed, "vendor:x-field");
        let parsed = CCProperty::from("vendor:x-field");
        assert_eq!(parsed, prop);

        let serialized = serde_json::to_value(&prop).unwrap();
        assert_eq!(serialized, json!("vendor:x-field"));
        let deserialized: CCProperty = serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized, prop);
    }
}

// ---------------------------------------------------------------------------
// 5. Query filter serialization
// ---------------------------------------------------------------------------

#[cfg(all(feature = "calendars", feature = "contacts", feature = "quota"))]
mod query_filter_serialization {
    use super::*;
    use crate::calendar_event::query::Filter as CEFilter;
    use crate::contact_card::query::Filter as CCFilter;
    use crate::quota::query::Filter as QFilter;

    // -- ContactCard filters with slashes in property names --

    #[test]
    fn contact_card_name_given_filter() {
        let filter = CCFilter::name_given("Alice");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name/given": "Alice"}));
    }

    #[test]
    fn contact_card_name_surname_filter() {
        let filter = CCFilter::name_surname("Smith");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name/surname": "Smith"}));
    }

    #[test]
    fn contact_card_name_surname2_filter() {
        let filter = CCFilter::name_surname2("Garcia");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name/surname2": "Garcia"}));
    }

    #[test]
    fn contact_card_in_address_book_filter() {
        let filter = CCFilter::in_address_book("ab-123");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"inAddressBook": "ab-123"}));
    }

    #[test]
    fn contact_card_nickname_filter() {
        let filter = CCFilter::nickname("Al");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"nickname": "Al"}));
    }

    // -- CalendarEvent filters: inCalendar (singular) vs inCalendars (plural) --

    #[test]
    fn calendar_event_in_calendar_singular() {
        let filter = CEFilter::in_calendar("cal-1");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"inCalendar": "cal-1"}));
    }

    #[test]
    fn calendar_event_in_calendars_plural() {
        let filter = CEFilter::in_calendars(["cal-1", "cal-2"]);
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"inCalendars": ["cal-1", "cal-2"]}));
    }

    #[test]
    fn calendar_event_text_filter() {
        let filter = CEFilter::text("meeting");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"text": "meeting"}));
    }

    #[test]
    fn calendar_event_uid_filter() {
        let filter = CEFilter::uid("uid-abc");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"uid": "uid-abc"}));
    }

    // -- Quota filters --

    #[test]
    fn quota_name_filter() {
        let filter = QFilter::name("Storage");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name": "Storage"}));
    }

    #[test]
    fn quota_scope_filter() {
        let filter = QFilter::scope("account");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"scope": "account"}));
    }

    #[test]
    fn quota_resource_type_filter() {
        let filter = QFilter::resource_type("octets");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"resourceType": "octets"}));
    }
}

// ---------------------------------------------------------------------------
// 6. Calendar Option<Option<T>> serialization
// ---------------------------------------------------------------------------

#[cfg(feature = "calendars")]
mod calendar_option_option_serialization {
    use super::*;
    use crate::calendar::CalendarCreate;
    use crate::core::SetCreate;

    #[test]
    fn calendar_description_none_serializes_as_null() {
        let mut cal = CalendarCreate::new(Some(0));
        cal.name("Test Calendar");
        cal.description(None::<String>);

        let value = serde_json::to_value(&cal).unwrap();
        // description is set to Some(None), so it serializes as null
        assert!(
            value.get("description").is_some(),
            "description should be present in JSON"
        );
        assert!(
            value.get("description").unwrap().is_null(),
            "description should be null, got {:?}",
            value.get("description")
        );
    }

    #[test]
    fn calendar_description_unset_is_absent() {
        let mut cal = CalendarCreate::new(Some(0));
        cal.name("Test Calendar");
        // Do NOT call cal.description() - leave it as outer None

        let value = serde_json::to_value(&cal).unwrap();
        assert!(
            value.get("description").is_none(),
            "unset description should be absent from JSON, but got {:?}",
            value.get("description")
        );
    }

    #[test]
    fn calendar_description_some_serializes_as_string() {
        let mut cal = CalendarCreate::new(Some(0));
        cal.name("Test Calendar");
        cal.description(Some("A nice calendar"));

        let value = serde_json::to_value(&cal).unwrap();
        assert_eq!(value.get("description"), Some(&json!("A nice calendar")));
    }
}

// ---------------------------------------------------------------------------
// 7. Blob DataSource serialization
// ---------------------------------------------------------------------------

#[cfg(feature = "blob")]
mod blob_data_source_serialization {
    use super::*;
    use crate::blob::manage::{DataSource, DataSourceBase64, DataSourceBlob, DataSourceText};

    #[test]
    fn data_source_text_serialization() {
        let src = DataSource::Text(DataSourceText {
            value: "Hello, world!".into(),
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(value, json!({"data:asText": "Hello, world!"}));
    }

    #[test]
    fn data_source_base64_serialization() {
        let src = DataSource::Base64(DataSourceBase64 {
            value: "SGVsbG8=".into(),
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(value, json!({"data:asBase64": "SGVsbG8="}));
    }

    #[test]
    fn data_source_blob_serialization() {
        let src = DataSource::Blob(DataSourceBlob {
            blob_id: "blob-123".into(),
            offset: Some(10),
            length: Some(100),
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(
            value,
            json!({"blobId": "blob-123", "offset": 10, "length": 100})
        );
    }

    #[test]
    fn data_source_blob_without_range() {
        let src = DataSource::Blob(DataSourceBlob {
            blob_id: "blob-456".into(),
            offset: None,
            length: None,
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(value, json!({"blobId": "blob-456"}));
        // offset and length should be absent
        assert!(value.get("offset").is_none());
        assert!(value.get("length").is_none());
    }
}

// ---------------------------------------------------------------------------
// 8. Session capabilities deserialization
// ---------------------------------------------------------------------------

mod session_capabilities_deserialization {
    use super::*;
    use crate::core::session::{Capabilities, Session};

    fn sample_session_json() -> serde_json::Value {
        let mut value = json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 50000000,
                    "maxConcurrentUpload": 4,
                    "maxSizeRequest": 10000000,
                    "maxConcurrentRequests": 8,
                    "maxCallsInRequest": 16,
                    "maxObjectsInGet": 500,
                    "maxObjectsInSet": 500,
                    "collationAlgorithms": ["i;ascii-casemap"]
                },
                "urn:ietf:params:jmap:mail": {
                    "maxMailboxesPerEmail": null,
                    "maxMailboxDepth": 10,
                    "maxSizeMailboxName": 200,
                    "maxSizeAttachmentsPerEmail": 50000000,
                    "emailQuerySortOptions": ["receivedAt", "from", "subject"],
                    "mayCreateTopLevelMailbox": true
                },
                "urn:ietf:params:jmap:quota": {},
                "urn:ietf:params:jmap:blob": {
                    "maxSizeBlobSet": 100000000,
                    "supportedDigestAlgorithms": ["sha", "sha-256"],
                    "supportedTypeNames": ["Email", "CalendarEvent"]
                },
                "urn:ietf:params:jmap:calendars": {
                    "mayCreateCalendar": true,
                    "maxCalendarsPerEvent": null
                },
                "urn:ietf:params:jmap:contacts": {
                    "mayCreateAddressBook": true,
                    "maxAddressBooksPerCard": null
                },
                "urn:ietf:params:jmap:principals": {
                    "currentUserPrincipalId": "user-1",
                    "accountIdForPrincipal": "acct-1"
                },
                "urn:vendor:custom": {
                    "someField": "someValue"
                }
            },
            "accounts": {
                "acct-1": {
                    "name": "John Doe",
                    "isPersonal": true,
                    "isReadOnly": false,
                    "accountCapabilities": {
                        "urn:ietf:params:jmap:core": {},
                        "urn:ietf:params:jmap:mail": {
                            "maxMailboxesPerEmail": null,
                            "maxMailboxDepth": 10,
                            "maxSizeMailboxName": 200,
                            "maxSizeAttachmentsPerEmail": 50000000,
                            "emailQuerySortOptions": [],
                            "mayCreateTopLevelMailbox": true
                        }
                    }
                }
            },
            "primaryAccounts": {
                "urn:ietf:params:jmap:mail": "acct-1"
            },
            "username": "john@example.org",
            "apiUrl": "https://jmap.example.org/api/",
            "downloadUrl": "https://jmap.example.org/download/{accountId}/{blobId}/{name}",
            "uploadUrl": "https://jmap.example.org/upload/{accountId}/",
            "eventSourceUrl": "https://jmap.example.org/eventsource/",
            "state": "s0"
        });
        for uri in [
            "urn:ietf:params:jmap:mail",
            "urn:ietf:params:jmap:calendars",
            "urn:ietf:params:jmap:contacts",
            "urn:ietf:params:jmap:principals",
        ] {
            let account_value = value["capabilities"][uri].take();
            value["capabilities"][uri] = json!({});
            value["accounts"]["acct-1"]["accountCapabilities"][uri] = account_value;
            value["primaryAccounts"][uri] = json!("acct-1");
        }
        value
    }

    #[test]
    fn session_deserializes_all_capability_types() {
        let session: Session =
            serde_json::from_value(sample_session_json()).expect("session deser failed");

        // core
        let core = session.core_capabilities().expect("core missing");
        assert_eq!(core.max_size_upload(), Some(50_000_000));
        assert_eq!(core.max_calls_in_request(), Some(16));
        assert_eq!(core.max_objects_in_get(), Some(500));
        assert_eq!(core.collation_algorithms(), &["i;ascii-casemap"]);

        #[cfg(feature = "mail")]
        {
            let mail = session.mail_capabilities().expect("mail missing");
            assert_eq!(mail.max_mailbox_depth(), 10);
        }

        #[cfg(feature = "quota")]
        assert!(session.quota_capabilities().is_some());

        #[cfg(feature = "blob")]
        {
            let blob = session.blob_capabilities().expect("blob missing");
            assert_eq!(blob.max_size_blob_set(), Some(100_000_000));
            assert_eq!(blob.supported_digest_algorithms(), &["sha", "sha-256"]);
        }

        #[cfg(feature = "calendars")]
        {
            let cals = session.calendars_capabilities().expect("calendars missing");
            assert!(cals.may_create_calendar());
            assert_eq!(cals.max_calendars_per_event(), None);
        }

        #[cfg(feature = "contacts")]
        {
            let contacts = session.contacts_capabilities().expect("contacts missing");
            assert!(contacts.may_create_address_book());
        }

        // principals
        let principals = session
            .principals_capabilities()
            .expect("principals missing");
        assert_eq!(
            principals
                .current_user_principal_id()
                .map(crate::core::id::Id::as_str),
            Some("user-1")
        );
        assert_eq!(
            principals
                .account_id_for_principal()
                .map(crate::core::id::Id::as_str),
            Some("acct-1")
        );

        // unknown vendor capability -> Other
        let vendor = session
            .capability("urn:vendor:custom")
            .expect("vendor cap missing");
        match vendor {
            Capabilities::Other(v) => {
                assert_eq!(v.get("someField"), Some(&json!("someValue")));
            }
            other => panic!("expected Capabilities::Other for vendor cap, got {other:?}"),
        }
    }

    #[test]
    fn session_basic_fields() {
        let session: Session =
            serde_json::from_value(sample_session_json()).expect("session deser failed");

        assert_eq!(session.username(), "john@example.org");
        assert_eq!(session.api_url(), "https://jmap.example.org/api/");
        assert_eq!(session.state(), "s0");

        let account = session.account("acct-1").expect("account missing");
        assert_eq!(account.name(), "John Doe");
        assert!(account.is_personal());
        assert!(!account.is_read_only());
    }
}

// ---------------------------------------------------------------------------
// 9. AlertTrigger deserialization
// ---------------------------------------------------------------------------
#[cfg(feature = "calendars")]
mod alert_trigger_deserialization {
    use super::*;
    use crate::calendar_event::{AlertTrigger, RelativeTo};

    #[test]
    fn offset_trigger_deserializes() {
        let json_str = r#"{"@type":"OffsetTrigger","offset":"-PT15M","relativeTo":"start"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        match trigger {
            AlertTrigger::OffsetTrigger {
                offset,
                relative_to,
            } => {
                assert_eq!(offset, "-PT15M");
                assert_eq!(relative_to, Some(RelativeTo::Start));
            }
            other => panic!("expected OffsetTrigger, got {other:?}"),
        }
    }

    #[test]
    fn offset_trigger_without_relative_to() {
        let json_str = r#"{"@type":"OffsetTrigger","offset":"PT0S"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        match trigger {
            AlertTrigger::OffsetTrigger {
                offset,
                relative_to,
            } => {
                assert_eq!(offset, "PT0S");
                assert_eq!(relative_to, None);
            }
            other => panic!("expected OffsetTrigger, got {other:?}"),
        }
    }

    #[test]
    fn absolute_trigger_deserializes() {
        let json_str = r#"{"@type":"AbsoluteTrigger","when":"2025-06-15T09:00:00Z"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        match trigger {
            AlertTrigger::AbsoluteTrigger { when } => {
                assert_eq!(when, "2025-06-15T09:00:00Z");
            }
            other => panic!("expected AbsoluteTrigger, got {other:?}"),
        }
    }

    #[test]
    fn unknown_trigger_type_deserializes_as_unknown() {
        // #[serde(other)] catches any unrecognized @type value.
        let json_str = r#"{"@type":"FutureTriggerType","foo":"bar"}"#;
        let trigger: AlertTrigger = serde_json::from_str(json_str).unwrap();
        assert!(
            matches!(trigger, AlertTrigger::Unknown),
            "expected Unknown, got {trigger:?}"
        );
    }

    #[test]
    fn offset_trigger_serializes() {
        let trigger = AlertTrigger::OffsetTrigger {
            offset: "-PT10M".to_string(),
            relative_to: Some(RelativeTo::End),
        };
        let value = serde_json::to_value(&trigger).unwrap();
        assert_eq!(value.get("@type"), Some(&json!("OffsetTrigger")));
        assert_eq!(value.get("offset"), Some(&json!("-PT10M")));
        assert_eq!(value.get("relativeTo"), Some(&json!("end")));
    }
}

// ---------------------------------------------------------------------------
// 10. Blob/get request serialization
// ---------------------------------------------------------------------------

#[cfg(feature = "blob")]
mod blob_get_request_serialization {
    use super::*;
    use crate::blob::manage::BlobGetRequest;

    #[test]
    fn blob_get_request_basic_serialization() {
        use crate::core::method::JmapMethod;
        let mut req = BlobGetRequest::new()
            .ids(["blob-1", "blob-2"])
            .properties(["data:asText", "size"]);
        req.set_account_id(&crate::core::id::AccountId::new("acct-1"));

        let value = serde_json::to_value(&req).unwrap();

        // accountId at top level
        assert_eq!(value.get("accountId"), Some(&json!("acct-1")));

        // ids as a flat array
        let ids = value.get("ids").expect("ids missing");
        assert!(ids.is_array());
        let ids_arr = ids.as_array().unwrap();
        assert_eq!(ids_arr.len(), 2);
        assert!(ids_arr.contains(&json!("blob-1")));
        assert!(ids_arr.contains(&json!("blob-2")));

        // properties at top level
        let props = value.get("properties").expect("properties missing");
        assert!(props.is_array());
        let props_arr = props.as_array().unwrap();
        assert!(props_arr.contains(&json!("data:asText")));
        assert!(props_arr.contains(&json!("size")));
    }

    #[test]
    fn blob_get_request_with_offset_and_length() {
        use crate::core::method::JmapMethod;
        let mut req = BlobGetRequest::new()
            .ids(["blob-1"])
            .offset(100)
            .length(500);
        req.set_account_id(&crate::core::id::AccountId::new("acct-1"));

        let value = serde_json::to_value(&req).unwrap();
        assert_eq!(value.get("offset"), Some(&json!(100)));
        assert_eq!(value.get("length"), Some(&json!(500)));
    }

    #[test]
    fn blob_get_request_without_optional_fields() {
        use crate::core::method::JmapMethod;
        let mut req = BlobGetRequest::new().ids(["blob-1"]);
        req.set_account_id(&crate::core::id::AccountId::new("acct-1"));

        let value = serde_json::to_value(&req).unwrap();
        // properties, offset, length should be absent
        assert!(
            value.get("properties").is_none(),
            "unset properties should be absent"
        );
        assert!(
            value.get("offset").is_none(),
            "unset offset should be absent"
        );
        assert!(
            value.get("length").is_none(),
            "unset length should be absent"
        );
    }
}

// ---------------------------------------------------------------------------
// ShareNotification (RFC 9670)
// ---------------------------------------------------------------------------

mod share_notification_serde {
    use super::*;
    use crate::share_notification::ShareNotification;

    fn from_json(s: &str) -> ShareNotification {
        serde_json::from_str(s).expect("failed to deserialize ShareNotification")
    }

    #[test]
    fn full_notification_round_trip() {
        let input = json!({
            "id": "notif-1",
            "created": "2024-11-15T10:30:00Z",
            "changedBy": {
                "name": "Alice",
                "email": "alice@example.com",
                "principalId": "p-alice"
            },
            "objectType": "Calendar",
            "objectAccountId": "acct-bob",
            "objectId": "cal-team",
            "oldRights": null,
            "newRights": {
                "mayReadFreeBusy": true,
                "mayReadItems": true,
                "mayWriteAll": false
            },
            "name": "Team Calendar"
        });

        let notif: ShareNotification = serde_json::from_value(input).unwrap();
        assert_eq!(notif.id().map(crate::core::id::Id::as_str), Some("notif-1"));
        assert_eq!(notif.created(), Some("2024-11-15T10:30:00Z"));
        assert_eq!(notif.object_type(), Some("Calendar"));
        assert_eq!(
            notif.object_account_id().map(crate::core::id::Id::as_str),
            Some("acct-bob")
        );
        assert_eq!(notif.object_id(), Some("cal-team"));
        assert_eq!(notif.name(), Some("Team Calendar"));
        assert!(notif.old_rights().is_none());
        let new_rights = notif.new_rights().unwrap();
        assert_eq!(new_rights.get("mayReadFreeBusy"), Some(&true));
        assert_eq!(new_rights.get("mayWriteAll"), Some(&false));

        let changed_by = notif.changed_by().unwrap();
        assert_eq!(changed_by.name(), Some("Alice"));
        assert_eq!(changed_by.email(), Some("alice@example.com"));
        assert_eq!(
            changed_by.principal_id().map(crate::core::id::Id::as_str),
            Some("p-alice")
        );
    }

    #[test]
    fn minimal_notification_deserializes() {
        let notif = from_json(r#"{"id":"n1"}"#);
        assert_eq!(notif.id().map(crate::core::id::Id::as_str), Some("n1"));
        assert!(notif.created().is_none());
        assert!(notif.changed_by().is_none());
        assert!(notif.object_type().is_none());
        assert!(notif.old_rights().is_none());
        assert!(notif.new_rights().is_none());
    }

    #[test]
    fn property_display() {
        use crate::share_notification::Property;
        assert_eq!(Property::ObjectType.to_string(), "objectType");
        assert_eq!(Property::ChangedBy.to_string(), "changedBy");
        assert_eq!(Property::OldRights.to_string(), "oldRights");
        assert_eq!(Property::NewRights.to_string(), "newRights");
        assert_eq!(Property::ObjectAccountId.to_string(), "objectAccountId");
    }

    #[test]
    fn filter_serialization() {
        use crate::share_notification::query::Filter;

        let f = Filter::after("2024-01-01T00:00:00Z");
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"after": "2024-01-01T00:00:00Z"}));

        let f = Filter::object_type("Calendar");
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"objectType": "Calendar"}));

        let f = Filter::object_account_id("acct-1");
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"objectAccountId": "acct-1"}));
    }
}

// ---------------------------------------------------------------------------
// Principal RFC 9670 additions
// ---------------------------------------------------------------------------

mod principal_rfc9670 {
    use super::*;
    use crate::principal::Principal;

    #[test]
    fn principal_with_accounts_deserializes() {
        let input = json!({
            "id": "p-alice",
            "type": "individual",
            "name": "Alice",
            "email": "alice@example.com",
            "capabilities": {
                "urn:ietf:params:jmap:mail": {},
                "urn:ietf:params:jmap:calendars": {"mayCreateCalendar": true}
            },
            "accounts": {
                "acct-1": {
                    "name": "alice@example.com",
                    "isPersonal": true,
                    "isReadOnly": false,
                    "accountCapabilities": {
                        "urn:ietf:params:jmap:mail": {}
                    }
                },
                "acct-2": {
                    "name": "shared",
                    "isPersonal": false,
                    "isReadOnly": true,
                    "accountCapabilities": {}
                }
            }
        });

        let principal: Principal = serde_json::from_value(input).unwrap();
        assert_eq!(
            principal.id().map(crate::core::id::Id::as_str),
            Some("p-alice")
        );
        assert_eq!(principal.name(), Some("Alice"));

        let caps = principal.capabilities().unwrap();
        assert!(caps.contains_key("urn:ietf:params:jmap:mail"));
        assert!(caps.contains_key("urn:ietf:params:jmap:calendars"));

        let accounts = principal.accounts().unwrap();
        assert_eq!(accounts.len(), 2);

        let acct1 = &accounts[&crate::core::id::AccountId::new("acct-1")];
        assert_eq!(acct1.name(), Some("alice@example.com"));
        assert!(acct1.is_personal());
        assert!(!acct1.is_read_only());
        assert!(
            acct1
                .account_capabilities()
                .contains_key("urn:ietf:params:jmap:mail")
        );

        let acct2 = &accounts[&crate::core::id::AccountId::new("acct-2")];
        assert_eq!(acct2.name(), Some("shared"));
        assert!(!acct2.is_personal());
        assert!(acct2.is_read_only());
    }

    #[test]
    fn principal_without_accounts_deserializes() {
        let input = json!({
            "id": "p-bob",
            "type": "individual",
            "name": "Bob"
        });

        let principal: Principal = serde_json::from_value(input).unwrap();
        assert_eq!(
            principal.id().map(crate::core::id::Id::as_str),
            Some("p-bob")
        );
        assert!(principal.accounts().is_none());
        assert!(principal.capabilities().is_none());
    }

    #[test]
    fn principal_account_ids_filter_serialization() {
        use crate::principal::query::Filter;
        let f = Filter::account_ids(["acct-1", "acct-2"]);
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({"accountIds": ["acct-1", "acct-2"]}));
    }
}

// ---------------------------------------------------------------------------
// PrincipalsOwner capability
// ---------------------------------------------------------------------------

mod principals_owner_capability {
    use super::*;

    #[test]
    fn session_with_principals_owner() {
        let session_json = json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 50000000,
                    "maxConcurrentUpload": 4,
                    "maxSizeRequest": 10000000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 16,
                    "maxObjectsInGet": 500,
                    "maxObjectsInSet": 500,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:principals": {},
            },
            "accounts": {
                "acct-1": {
                    "name": "user@example.com",
                    "isPersonal": true,
                    "isReadOnly": false,
                    "accountCapabilities": {
                        "urn:ietf:params:jmap:principals": {
                            "currentUserPrincipalId": "p-user"
                        },
                        "urn:ietf:params:jmap:principals:owner": {
                            "accountIdForPrincipal": "acct-principals",
                            "principalId": "p-user"
                        }
                    }
                }
            },
            "primaryAccounts": {
                "urn:ietf:params:jmap:principals": "acct-1"
            },
            "username": "user@example.com",
            "apiUrl": "https://example.com/jmap/",
            "downloadUrl": "https://example.com/jmap/download/{accountId}/{blobId}/{name}?accept={type}",
            "uploadUrl": "https://example.com/jmap/upload/{accountId}/",
            "eventSourceUrl": "https://example.com/jmap/eventsource/?types={types}&closeafter={closeafter}&ping={ping}",
            "state": "abc123"
        });

        let session: crate::core::session::Session = serde_json::from_value(session_json).unwrap();

        assert!(matches!(
            session.capability("urn:ietf:params:jmap:principals"),
            Some(crate::core::session::Capabilities::Other(_))
        ));

        // Account-level principals capability
        let account = session.account("acct-1").unwrap();
        let acct_principals = account
            .capability("urn:ietf:params:jmap:principals")
            .unwrap();
        match acct_principals {
            crate::core::session::Capabilities::Principals(c) => {
                assert_eq!(
                    c.current_user_principal_id()
                        .map(crate::core::id::Id::as_str),
                    Some("p-user")
                );
            }
            _ => panic!("expected Principals variant"),
        }

        // Account-level principals:owner capability
        let acct_owner = account
            .capability("urn:ietf:params:jmap:principals:owner")
            .unwrap();
        match acct_owner {
            crate::core::session::Capabilities::PrincipalsOwner(c) => {
                assert_eq!(
                    c.account_id_for_principal()
                        .map(crate::core::id::Id::as_str),
                    Some("acct-principals")
                );
                assert_eq!(
                    c.principal_id().map(crate::core::id::Id::as_str),
                    Some("p-user")
                );
            }
            _ => panic!("expected PrincipalsOwner variant"),
        }
    }
}

// ---------------------------------------------------------------------------
// Method name / capability table (RFC 8620 s3.2 "using")
// ---------------------------------------------------------------------------
//
// `Request::call` derives the `using` entry from `M::Cap::URI` and the
// wire method name from `M::NAME`. Neither is exercised by any other
// test, and a typo in either is a runtime `unknownMethod` /
// `unknownCapability` against a real server, never a compile error.

mod method_name_and_capability_table {
    use crate::core::capability::Capability;
    use crate::core::method::JmapMethod;

    const CORE: &str = "urn:ietf:params:jmap:core";
    #[cfg(feature = "mail")]
    const MAIL: &str = "urn:ietf:params:jmap:mail";
    #[cfg(feature = "mail")]
    const SUBMISSION: &str = "urn:ietf:params:jmap:submission";
    #[cfg(feature = "mail")]
    const VACATION: &str = "urn:ietf:params:jmap:vacationresponse";
    #[cfg(feature = "mail")]
    const SIEVE: &str = "urn:ietf:params:jmap:sieve";
    const PRINCIPALS: &str = "urn:ietf:params:jmap:principals";
    #[cfg(feature = "blob")]
    const BLOB: &str = "urn:ietf:params:jmap:blob";
    #[cfg(feature = "quota")]
    const QUOTA: &str = "urn:ietf:params:jmap:quota";
    #[cfg(feature = "calendars")]
    const CALENDARS: &str = "urn:ietf:params:jmap:calendars";
    #[cfg(feature = "contacts")]
    const CONTACTS: &str = "urn:ietf:params:jmap:contacts";

    fn check<M: JmapMethod>(name: &str, uri: &str) {
        assert_eq!(M::NAME, name, "wire method name");
        assert_eq!(
            <M::Cap as Capability>::URI,
            uri,
            "capability URI advertised in `using` for {name}"
        );
    }

    #[test]
    fn core_methods() {
        check::<crate::push_subscription::PushSubscriptionGet>("PushSubscription/get", CORE);
        check::<crate::push_subscription::PushSubscriptionSet>("PushSubscription/set", CORE);
        // RFC 8620 s6.3: Blob/copy is a core method, not a blob-extension
        // one.
        check::<crate::blob::copy::CopyBlobRequest>("Blob/copy", CORE);
    }

    #[cfg(feature = "mail")]
    #[test]
    fn mail_methods() {
        check::<crate::email::EmailGet>("Email/get", MAIL);
        check::<crate::email::EmailSet>("Email/set", MAIL);
        check::<crate::email::EmailChanges>("Email/changes", MAIL);
        check::<crate::email::EmailQuery>("Email/query", MAIL);
        check::<crate::email::EmailQueryChanges>("Email/queryChanges", MAIL);
        check::<crate::email::EmailCopy>("Email/copy", MAIL);
        check::<crate::email::import::EmailImportRequest>("Email/import", MAIL);
        check::<crate::email::parse::EmailParseRequest>("Email/parse", MAIL);
        check::<crate::email::search_snippet::SearchSnippetGetRequest>("SearchSnippet/get", MAIL);
        check::<crate::mailbox::MailboxGet>("Mailbox/get", MAIL);
        check::<crate::mailbox::MailboxSet>("Mailbox/set", MAIL);
        check::<crate::mailbox::MailboxChanges>("Mailbox/changes", MAIL);
        check::<crate::mailbox::MailboxQuery>("Mailbox/query", MAIL);
        check::<crate::mailbox::MailboxQueryChanges>("Mailbox/queryChanges", MAIL);
        check::<crate::thread::ThreadGet>("Thread/get", MAIL);
        check::<crate::thread::ThreadChanges>("Thread/changes", MAIL);
    }

    #[cfg(feature = "mail")]
    #[test]
    fn submission_methods() {
        // RFC 8621 s6: Identity lives in the submission capability, not
        // in mail.
        check::<crate::identity::IdentityGet>("Identity/get", SUBMISSION);
        check::<crate::identity::IdentitySet>("Identity/set", SUBMISSION);
        check::<crate::identity::IdentityChanges>("Identity/changes", SUBMISSION);
        check::<crate::email_submission::EmailSubmissionGet>("EmailSubmission/get", SUBMISSION);
        check::<crate::email_submission::EmailSubmissionSet>("EmailSubmission/set", SUBMISSION);
        check::<crate::email_submission::EmailSubmissionChanges>(
            "EmailSubmission/changes",
            SUBMISSION,
        );
        check::<crate::email_submission::EmailSubmissionQuery>("EmailSubmission/query", SUBMISSION);
        check::<crate::email_submission::EmailSubmissionQueryChanges>(
            "EmailSubmission/queryChanges",
            SUBMISSION,
        );
    }

    #[cfg(feature = "mail")]
    #[test]
    fn vacation_and_sieve_methods() {
        check::<crate::vacation_response::VacationResponseGet>("VacationResponse/get", VACATION);
        check::<crate::vacation_response::VacationResponseSet>("VacationResponse/set", VACATION);
        check::<crate::sieve::SieveScriptGet>("SieveScript/get", SIEVE);
        check::<crate::sieve::SieveScriptSet>("SieveScript/set", SIEVE);
        check::<crate::sieve::SieveScriptQuery>("SieveScript/query", SIEVE);
        check::<crate::sieve::validate::SieveScriptValidateRequest>("SieveScript/validate", SIEVE);
    }

    #[test]
    fn principal_and_sharing_methods() {
        check::<crate::principal::PrincipalGet>("Principal/get", PRINCIPALS);
        check::<crate::principal::PrincipalSet>("Principal/set", PRINCIPALS);
        check::<crate::principal::PrincipalChanges>("Principal/changes", PRINCIPALS);
        check::<crate::principal::PrincipalQuery>("Principal/query", PRINCIPALS);
        check::<crate::principal::PrincipalQueryChanges>("Principal/queryChanges", PRINCIPALS);
        // RFC 9670 s3: ShareNotification is defined by the principals
        // capability.
        check::<crate::share_notification::ShareNotificationGet>(
            "ShareNotification/get",
            PRINCIPALS,
        );
        check::<crate::share_notification::ShareNotificationSet>(
            "ShareNotification/set",
            PRINCIPALS,
        );
        check::<crate::share_notification::ShareNotificationChanges>(
            "ShareNotification/changes",
            PRINCIPALS,
        );
        check::<crate::share_notification::ShareNotificationQuery>(
            "ShareNotification/query",
            PRINCIPALS,
        );
        check::<crate::share_notification::ShareNotificationQueryChanges>(
            "ShareNotification/queryChanges",
            PRINCIPALS,
        );
    }

    #[cfg(feature = "blob")]
    #[test]
    fn blob_methods() {
        check::<crate::blob::manage::BlobUploadRequest>("Blob/upload", BLOB);
        check::<crate::blob::manage::BlobGetRequest>("Blob/get", BLOB);
        check::<crate::blob::manage::BlobLookupRequest>("Blob/lookup", BLOB);
    }

    #[cfg(feature = "quota")]
    #[test]
    fn quota_methods() {
        check::<crate::quota::QuotaGet>("Quota/get", QUOTA);
        check::<crate::quota::QuotaChanges>("Quota/changes", QUOTA);
        check::<crate::quota::QuotaQuery>("Quota/query", QUOTA);
        check::<crate::quota::QuotaQueryChanges>("Quota/queryChanges", QUOTA);
    }

    #[cfg(feature = "calendars")]
    #[test]
    fn calendar_methods() {
        check::<crate::calendar::CalendarGet>("Calendar/get", CALENDARS);
        check::<crate::calendar::CalendarSet>("Calendar/set", CALENDARS);
        check::<crate::calendar::CalendarChanges>("Calendar/changes", CALENDARS);
        check::<crate::calendar_event::CalendarEventGet>("CalendarEvent/get", CALENDARS);
        check::<crate::calendar_event::CalendarEventSet>("CalendarEvent/set", CALENDARS);
        check::<crate::calendar_event::CalendarEventChanges>("CalendarEvent/changes", CALENDARS);
        check::<crate::calendar_event::CalendarEventQuery>("CalendarEvent/query", CALENDARS);
        check::<crate::calendar_event::CalendarEventQueryChanges>(
            "CalendarEvent/queryChanges",
            CALENDARS,
        );
        check::<crate::calendar_event::CalendarEventCopy>("CalendarEvent/copy", CALENDARS);
        check::<crate::calendar_event_notification::CalendarEventNotificationGet>(
            "CalendarEventNotification/get",
            CALENDARS,
        );
        check::<crate::calendar_event_notification::CalendarEventNotificationSet>(
            "CalendarEventNotification/set",
            CALENDARS,
        );
        check::<crate::participant_identity::ParticipantIdentityGet>(
            "ParticipantIdentity/get",
            CALENDARS,
        );
        // The `parse` sub-capability is separate from `calendars`.
        check::<crate::calendar_event::parse::CalendarEventParseRequest>(
            "CalendarEvent/parse",
            "urn:ietf:params:jmap:calendars:parse",
        );
    }

    #[cfg(feature = "contacts")]
    #[test]
    fn contact_methods() {
        check::<crate::address_book::AddressBookGet>("AddressBook/get", CONTACTS);
        check::<crate::address_book::AddressBookSet>("AddressBook/set", CONTACTS);
        check::<crate::address_book::AddressBookChanges>("AddressBook/changes", CONTACTS);
        check::<crate::contact_card::ContactCardGet>("ContactCard/get", CONTACTS);
        check::<crate::contact_card::ContactCardSet>("ContactCard/set", CONTACTS);
        check::<crate::contact_card::ContactCardChanges>("ContactCard/changes", CONTACTS);
        check::<crate::contact_card::ContactCardQuery>("ContactCard/query", CONTACTS);
        check::<crate::contact_card::ContactCardQueryChanges>("ContactCard/queryChanges", CONTACTS);
        check::<crate::contact_card::ContactCardCopy>("ContactCard/copy", CONTACTS);
        check::<crate::contact_card::parse::ContactCardParseRequest>(
            "ContactCard/parse",
            "urn:ietf:params:jmap:contacts:parse",
        );
    }
}

// ---------------------------------------------------------------------------
// Email header-property grammar (RFC 8621 s4.1.2)
// ---------------------------------------------------------------------------

#[cfg(feature = "mail")]
mod email_header_property_grammar {
    use super::*;
    use crate::email::{Header, HeaderForm, Property};

    #[test]
    fn every_header_form_round_trips_through_display() {
        for (wire, form, all) in [
            ("header:Subject", HeaderForm::Raw, false),
            ("header:Subject:all", HeaderForm::Raw, true),
            ("header:Subject:asText", HeaderForm::Text, false),
            ("header:Subject:asText:all", HeaderForm::Text, true),
            ("header:To:asAddresses", HeaderForm::Addresses, false),
            (
                "header:To:asGroupedAddresses",
                HeaderForm::GroupedAddresses,
                false,
            ),
            (
                "header:References:asMessageIds",
                HeaderForm::MessageIds,
                false,
            ),
            ("header:Date:asDate", HeaderForm::Date, false),
            ("header:List-Post:asURLs", HeaderForm::URLs, false),
        ] {
            let header = Header::parse(wire).unwrap_or_else(|| panic!("{wire} must parse"));
            assert_eq!(header.form, form, "form for {wire}");
            assert_eq!(header.all, all, "`:all` suffix for {wire}");
            assert_eq!(header.to_string(), wire, "{wire} must render back verbatim");
        }
    }

    #[test]
    fn header_parse_rejects_malformed_names() {
        // Not prefixed with the `header:` literal.
        assert!(Header::parse("subject").is_none());
        assert!(Header::parse("Subject:asText").is_none());
        // Unknown form.
        assert!(Header::parse("header:X-Spam:asFloat").is_none());
        // Too many segments.
        assert!(Header::parse("header:X-Spam:asText:all:extra").is_none());
    }

    #[test]
    fn header_constructors_agree_with_the_parser() {
        assert_eq!(
            Header::as_message_ids("References", false),
            Header::parse("header:References:asMessageIds").unwrap()
        );
        assert_eq!(
            Header::as_raw("X-Vendor", true),
            Header::parse("header:X-Vendor:all").unwrap()
        );
    }

    #[test]
    fn header_format_width_applies_to_the_whole_property() {
        let header = Header::as_raw("Subject", false);
        assert_eq!(format!("{header:>20}"), "      header:Subject");
    }

    #[test]
    fn property_serde_covers_named_and_header_and_vendor_forms() {
        for (prop, wire) in [
            (Property::Id, "id"),
            (Property::BlobId, "blobId"),
            (Property::ThreadId, "threadId"),
            (Property::MailboxIds, "mailboxIds"),
            (Property::Keywords, "keywords"),
            (Property::Size, "size"),
            (Property::ReceivedAt, "receivedAt"),
            (Property::MessageId, "messageId"),
            (Property::InReplyTo, "inReplyTo"),
            (Property::References, "references"),
            (Property::Sender, "sender"),
            (Property::From, "from"),
            (Property::To, "to"),
            (Property::Cc, "cc"),
            (Property::Bcc, "bcc"),
            (Property::ReplyTo, "replyTo"),
            (Property::Subject, "subject"),
            (Property::SentAt, "sentAt"),
            (Property::BodyStructure, "bodyStructure"),
            (Property::BodyValues, "bodyValues"),
            (Property::TextBody, "textBody"),
            (Property::HtmlBody, "htmlBody"),
            (Property::Attachments, "attachments"),
            (Property::HasAttachment, "hasAttachment"),
            (Property::Preview, "preview"),
        ] {
            assert_eq!(serde_json::to_value(&prop).unwrap(), json!(wire));
            assert_eq!(
                serde_json::from_value::<Property>(json!(wire)).unwrap(),
                prop
            );
        }

        let header = Property::Header(Header::as_text("Subject", false));
        assert_eq!(
            serde_json::to_value(&header).unwrap(),
            json!("header:Subject:asText")
        );
        assert_eq!(
            serde_json::from_value::<Property>(json!("header:Subject:asText")).unwrap(),
            header
        );

        // Anything else is preserved verbatim rather than rejected.
        assert_eq!(
            serde_json::from_value::<Property>(json!("example.com:custom")).unwrap(),
            Property::Other("example.com:custom".to_string())
        );
    }

    #[test]
    fn property_rejects_an_unparseable_header_form() {
        // A `header:`-prefixed name with a bad form is the one input
        // `Property` refuses outright; everything else falls through to
        // `Other`.
        assert!(serde_json::from_value::<Property>(json!("header:X:asFloat")).is_err());
    }
}

// ---------------------------------------------------------------------------
// Email object decode
// ---------------------------------------------------------------------------

#[cfg(feature = "mail")]
mod email_object_decode {
    use super::*;
    use crate::email::{Email, Header, HeaderValue};

    #[test]
    fn known_properties_decode() {
        let email: Email = serde_json::from_value(json!({
            "id": "e1",
            "blobId": "b1",
            "threadId": "t1",
            "mailboxIds": {"mb1": true, "mb2": false},
            "keywords": {"$seen": true, "$flagged": false},
            "size": 1234,
            "receivedAt": "2026-01-02T03:04:05Z",
            "messageId": ["<a@b>"],
            "from": [{"name": "Alice", "email": "alice@example.com"}],
            "subject": "Hi",
            "hasAttachment": true
        }))
        .expect("email decodes");

        assert_eq!(email.id().map(crate::core::id::Id::as_str), Some("e1"));
        assert_eq!(email.size(), 1234);
        assert!(email.has_attachment());
        // `mailboxIds` / `keywords` getters filter on the boolean value,
        // so an explicit `false` membership is not reported.
        let mb1 = crate::mailbox::MailboxId::new("mb1");
        assert_eq!(email.mailbox_ids(), vec![&mb1]);
        assert_eq!(email.keywords(), vec!["$seen"]);
        assert_eq!(email.received_at(), Some(1_767_323_045));
    }

    // The header-form aliases on `Email` are gated behind
    // `cfg_attr(not(feature = "debug"), ...)`, so enabling `debug` removes
    // them and this decode stops working. That divergence is itself a bug
    // the gate here keeps the suite honest
    // under `--all-features` rather than endorsing it.
    #[cfg(not(feature = "debug"))]
    #[test]
    fn header_form_aliases_populate_the_typed_fields() {
        // The Account layer asks for `messageId` / `references`, but a
        // server may answer with the RFC 8621 header-form spellings.
        let email: Email = serde_json::from_value(json!({
            "id": "e1",
            "header:Message-ID:asMessageIds": ["<a@b>"],
            "header:References:asMessageIds": ["<c@d>"],
            "header:Subject:asText": "Hello"
        }))
        .expect("alias spellings decode");

        let message_id = ["<a@b>".to_string()];
        let references = ["<c@d>".to_string()];
        assert_eq!(email.message_id(), Some(&message_id[..]));
        assert_eq!(email.references(), Some(&references[..]));
        assert_eq!(email.subject(), Some("Hello"));
    }

    #[test]
    fn arbitrary_header_properties_land_in_the_flattened_map() {
        let email: Email = serde_json::from_value(json!({
            "id": "e1",
            "header:X-Spam-Score:asText": "0.1"
        }))
        .expect("header property decodes");

        let key = Header::as_text("X-Spam-Score", false);
        assert!(email.has_header(&key));
        match email.header(&key) {
            Some(HeaderValue::AsText(v)) => assert_eq!(v, "0.1"),
            other => panic!("expected AsText, got {other:?}"),
        }
    }

    #[test]
    fn unknown_properties_do_not_fail_the_email_decode() {
        let ok = serde_json::from_value::<Email>(json!({"id": "e1", "subject": "Hi"}));
        assert!(ok.is_ok(), "control: the same object without the extension");

        let email = serde_json::from_value::<Email>(json!({
            "id": "e1",
            "subject": "Hi",
            "example.com:snoozedUntil": "2026-02-01T00:00:00Z"
        }))
        .expect("an extension property is ignored");
        assert_eq!(email.subject(), Some("Hi"));
        assert!(email.headers.is_empty());
    }
}

// ---------------------------------------------------------------------------
// Email/query filter + comparator wire shapes (RFC 8621 s4.4)
// ---------------------------------------------------------------------------

#[cfg(feature = "mail")]
mod email_query_wire {
    use super::*;
    use crate::email::query::{Comparator, Filter};

    fn wire(filter: Filter) -> serde_json::Value {
        serde_json::to_value(&filter).unwrap()
    }

    #[test]
    fn filter_condition_property_names() {
        assert_eq!(wire(Filter::in_mailbox("mb1")), json!({"inMailbox": "mb1"}));
        assert_eq!(
            wire(Filter::in_mailbox_other_than(["a", "b"])),
            json!({"inMailboxOtherThan": ["a", "b"]})
        );
        assert_eq!(
            wire(Filter::has_keyword("$seen")),
            json!({"hasKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::not_keyword("$seen")),
            json!({"notKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::all_in_thread_have_keyword("$seen")),
            json!({"allInThreadHaveKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::some_in_thread_have_keyword("$seen")),
            json!({"someInThreadHaveKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::none_in_thread_have_keyword("$seen")),
            json!({"noneInThreadHaveKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::has_attachment(true)),
            json!({"hasAttachment": true})
        );
        assert_eq!(wire(Filter::min_size(10)), json!({"minSize": 10}));
        assert_eq!(wire(Filter::max_size(20)), json!({"maxSize": 20}));
        assert_eq!(wire(Filter::text("q")), json!({"text": "q"}));
        assert_eq!(
            wire(Filter::from("a@example.com")),
            json!({"from": "a@example.com"})
        );
        assert_eq!(
            wire(Filter::to("a@example.com")),
            json!({"to": "a@example.com"})
        );
        assert_eq!(
            wire(Filter::cc("a@example.com")),
            json!({"cc": "a@example.com"})
        );
        assert_eq!(
            wire(Filter::bcc("a@example.com")),
            json!({"bcc": "a@example.com"})
        );
        assert_eq!(wire(Filter::subject("s")), json!({"subject": "s"}));
        assert_eq!(wire(Filter::body("b")), json!({"body": "b"}));
    }

    #[test]
    fn header_filter_is_a_one_or_two_element_array() {
        assert_eq!(
            wire(Filter::header("X-Vendor", None::<String>)),
            json!({"header": ["X-Vendor"]})
        );
        assert_eq!(
            wire(Filter::header("X-Vendor", Some("v"))),
            json!({"header": ["X-Vendor", "v"]})
        );
    }

    #[test]
    fn date_filters_serialise_as_jmap_utcdate() {
        // RFC 8620 s1.4 UTCDate: "YYYY-MM-DDTHH:MM:SSZ", no offset, no
        // fractional seconds for a whole-second instant.
        assert_eq!(
            wire(Filter::before(0)),
            json!({"before": "1970-01-01T00:00:00Z"})
        );
        assert_eq!(
            wire(Filter::after(1_767_323_045)),
            json!({"after": "2026-01-02T03:04:05Z"})
        );
    }

    #[test]
    fn comparators_flatten_the_property_tag() {
        assert_eq!(
            serde_json::to_value(Comparator::received_at().descending()).unwrap(),
            json!({"isAscending": false, "property": "receivedAt"})
        );
        assert_eq!(
            serde_json::to_value(Comparator::has_keyword("$flagged")).unwrap(),
            json!({"isAscending": true, "property": "hasKeyword", "keyword": "$flagged"})
        );
        assert_eq!(
            serde_json::to_value(Comparator::size().collation("i;ascii-casemap".to_string()))
                .unwrap(),
            json!({"isAscending": true, "collation": "i;ascii-casemap", "property": "size"})
        );
    }

    #[test]
    fn query_request_carries_collapse_threads_and_paging() {
        let query = crate::email::EmailQuery::new()
            .filter(Filter::in_mailbox("mb1"))
            .sort([Comparator::received_at().descending()])
            .position(20)
            .limit(10)
            .calculate_total(true)
            .collapse_threads(true);
        let value = serde_json::to_value(&query).unwrap();

        assert_eq!(value.get("filter"), Some(&json!({"inMailbox": "mb1"})));
        assert_eq!(value.get("position"), Some(&json!(20)));
        assert_eq!(value.get("limit"), Some(&json!(10)));
        assert_eq!(value.get("calculateTotal"), Some(&json!(true)));
        assert_eq!(value.get("collapseThreads"), Some(&json!(true)));
        // Unset paging arguments must not appear at all.
        assert!(value.get("anchor").is_none());
        assert!(value.get("anchorOffset").is_none());
    }

    #[test]
    fn filter_operators_nest() {
        use crate::core::query::{Filter as CoreFilter, Operator};
        let in_mailbox: CoreFilter<Filter> = CoreFilter::FilterCondition(Filter::in_mailbox("mb1"));
        let has_keyword: CoreFilter<Filter> =
            CoreFilter::FilterCondition(Filter::has_keyword("$seen"));
        let negated: CoreFilter<Filter> = CoreFilter::not([has_keyword]);
        let combined: CoreFilter<Filter> = CoreFilter::and([in_mailbox, negated]);
        let value = serde_json::to_value(&combined).unwrap();
        assert_eq!(
            value,
            json!({
                "operator": "AND",
                "conditions": [
                    {"inMailbox": "mb1"},
                    {"operator": "NOT", "conditions": [{"hasKeyword": "$seen"}]}
                ]
            })
        );
        assert_eq!(
            serde_json::to_value(Operator::Or).unwrap(),
            json!("OR"),
            "operator names are upper-case in RFC 8620 s5.5"
        );
    }
}

// ---------------------------------------------------------------------------
// Email/set patch shapes (RFC 8620 s5.3 PatchObject)
// ---------------------------------------------------------------------------

#[cfg(feature = "mail")]
mod email_set_patch_shapes {
    use super::*;
    use crate::email::EmailPatch;
    use crate::mailbox::MailboxId;

    #[test]
    fn dotted_paths_use_null_to_remove_and_true_to_add() {
        let mut patch = EmailPatch::default();
        patch.keyword("$seen", true);
        patch.keyword("$flagged", false);
        patch.mailbox_id(&MailboxId::new("mb1"), true);
        patch.mailbox_id(&MailboxId::new("mb2"), false);
        let value = serde_json::to_value(&patch).unwrap();

        assert_eq!(
            value,
            json!({
                "keywords/$seen": true,
                "keywords/$flagged": null,
                "mailboxIds/mb1": true,
                "mailboxIds/mb2": null
            })
        );
    }

    #[test]
    fn a_path_setter_clears_the_wholesale_property() {
        // RFC 8620 s5.3 forbids sending both `keywords` and
        // `keywords/x` in one PatchObject.
        let mut patch = EmailPatch::default();
        patch.keywords(["$seen"]);
        patch.keyword("$flagged", true);
        let value = serde_json::to_value(&patch).unwrap();
        assert!(value.get("keywords").is_none());
        assert_eq!(value.get("keywords/$flagged"), Some(&json!(true)));
    }

    #[test]
    fn wholesale_setters_clear_previously_set_paths() {
        let mut patch = EmailPatch::default();
        patch.keyword("$flagged", true);
        patch.keywords(["$seen"]);
        patch.mailbox_id(&MailboxId::new("mb1"), true);
        patch.mailbox_ids([MailboxId::new("mb2")]);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("keywords"), Some(&json!({"$seen": true})));
        assert!(value.get("keywords/$flagged").is_none());
        assert_eq!(value.get("mailboxIds"), Some(&json!({"mb2": true})));
        assert!(value.get("mailboxIds/mb1").is_none());
    }

    // A raw entry for the exact property is not a child path, so the
    // wholesale setter has to remove it by name as well. Serialising is
    // checked on the JSON text, not on a `Value`: two writes of the same
    // key collapse in a `serde_json::Map` but both reach the wire.
    #[test]
    fn wholesale_setters_clear_a_raw_entry_for_the_same_property() {
        let mut patch = EmailPatch::default();
        patch.null_property("keywords");
        patch.keywords(["$seen"]);
        patch
            .raw_property("mailboxIds", &json!({"mb9": true}))
            .expect("serializable");
        patch.mailbox_ids([MailboxId::new("mb2")]);
        let text = serde_json::to_string(&patch).unwrap();
        assert_eq!(
            text.matches("\"keywords\"").count(),
            1,
            "duplicate `keywords` keys in {text}"
        );
        assert_eq!(
            text.matches("\"mailboxIds\"").count(),
            1,
            "duplicate `mailboxIds` keys in {text}"
        );
        assert!(!text.contains("\"keywords\":null"), "{text}");
        assert!(!text.contains("mb9"), "{text}");
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("keywords"), Some(&json!({"$seen": true})));
        assert_eq!(value.get("mailboxIds"), Some(&json!({"mb2": true})));
    }

    #[test]
    fn raw_and_null_property_escape_hatches() {
        let mut patch = EmailPatch::default();
        patch
            .raw_property("keywords/$vendor", &true)
            .expect("serializable");
        patch.null_property("preview");
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("keywords/$vendor"), Some(&json!(true)));
        assert_eq!(value.get("preview"), Some(&serde_json::Value::Null));
    }
}

// ---------------------------------------------------------------------------
// Mailbox wire shapes (RFC 8621 s2)
// ---------------------------------------------------------------------------

#[cfg(feature = "mail")]
mod mailbox_wire {
    use super::*;
    use crate::core::SetCreate;
    use crate::mailbox::{MailboxCreate, MailboxId, MailboxPatch, MailboxSet, Role};
    use crate::principal::ACL;

    #[test]
    fn role_wire_names() {
        for (role, wire) in [
            (Role::Inbox, "inbox"),
            (Role::Sent, "sent"),
            (Role::Trash, "trash"),
            (Role::Drafts, "drafts"),
            (Role::Junk, "junk"),
            (Role::Archive, "archive"),
            (Role::Important, "important"),
        ] {
            assert_eq!(serde_json::to_value(&role).unwrap(), json!(wire));
            assert_eq!(
                serde_json::from_str::<Role>(&format!("\"{wire}\"")).unwrap(),
                role
            );
        }
        // Role matching is case-insensitive on the way in.
        assert_eq!(
            serde_json::from_str::<Role>("\"INBOX\"").unwrap(),
            Role::Inbox
        );
    }

    #[test]
    fn unknown_roles_survive_as_other_but_are_lower_cased() {
        // Note the case fold: an `x-` role does NOT round-trip byte for
        // byte, which matters if the value is ever echoed back in a set.
        assert_eq!(
            serde_json::from_str::<Role>("\"x-MyRole\"").unwrap(),
            Role::Other("x-myrole".to_string())
        );
        assert_eq!(
            serde_json::to_value(Role::Other("x-myrole".to_string())).unwrap(),
            json!("x-myrole")
        );
    }

    #[test]
    fn role_decodes_when_the_string_is_not_borrowable() {
        assert_eq!(
            serde_json::from_value::<Role>(json!("inbox")).unwrap(),
            Role::Inbox
        );
        // 92 is the backslash byte, so the JSON below spells `inbox`
        // with a `u0069` escape for the leading `i`.
        let escaped = String::from_utf8(vec![
            b'"', 92, b'u', b'0', b'0', b'6', b'9', b'n', b'b', b'o', b'x', b'"',
        ])
        .expect("ascii");
        assert_eq!(serde_json::from_str::<Role>(&escaped).unwrap(), Role::Inbox);
    }

    #[test]
    fn a_fresh_create_serialises_to_an_empty_object() {
        // The sentinel defaults (`parentId: ""`, `role: None`, empty
        // `shareWith`) all have to be skipped, or a plain
        // `Mailbox/set create` would carry three properties the caller
        // never asked for.
        let create = MailboxCreate::new(Some(0));
        assert_eq!(serde_json::to_value(&create).unwrap(), json!({}));
    }

    #[test]
    fn create_with_an_explicit_none_parent_sends_null() {
        let mut create = MailboxCreate::new(Some(0));
        create.name("Top level");
        create.parent_id(None::<MailboxId>);
        let value = serde_json::to_value(&create).unwrap();
        assert_eq!(value.get("name"), Some(&json!("Top level")));
        assert_eq!(value.get("parentId"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn create_id_references_are_hash_prefixed() {
        let mut create = MailboxCreate::new(Some(0));
        create.parent_id_ref("c0");
        assert_eq!(
            serde_json::to_value(&create).unwrap().get("parentId"),
            Some(&json!("#c0"))
        );
    }

    #[test]
    fn an_empty_mailbox_patch_omits_role_and_share_with() {
        assert_eq!(
            serde_json::to_value(MailboxPatch::default()).unwrap(),
            json!({}),
            "an untouched patch must not remove any properties"
        );

        // The live rename path, verbatim.
        let mut set = MailboxSet::new();
        set.update(MailboxId::new("mb1")).name("Renamed");
        assert_eq!(
            serde_json::to_value(&set).unwrap().get("update"),
            Some(&json!({
                "mb1": {"name": "Renamed"}
            }))
        );
    }

    #[test]
    fn patching_parent_id_to_none_emits_a_null_parent_id() {
        let mut patch = MailboxPatch::default();
        patch.parent_id(None::<MailboxId>);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("parentId"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn patching_parent_id_to_some_moves_the_mailbox() {
        let mut patch = MailboxPatch::default();
        patch.parent_id(Some(MailboxId::new("mb-parent")));
        assert_eq!(
            serde_json::to_value(&patch).unwrap().get("parentId"),
            Some(&json!("mb-parent"))
        );
    }

    #[test]
    fn role_none_is_the_only_way_to_omit_role_from_a_patch() {
        // `role(Role::None)` sets the field to `None`, which is NOT the
        // sentinel `role_not_set` looks for, so it still emits null.
        // Only an explicit `Some(Role::None)` is skipped, and no public
        // setter produces one.
        let mut patch = MailboxPatch::default();
        patch.name("Renamed");
        patch.role(Role::None);
        assert_eq!(
            serde_json::to_value(&patch).unwrap().get("role"),
            Some(&serde_json::Value::Null)
        );

        let sentinel = MailboxPatch {
            role: Some(Role::None),
            ..MailboxPatch::default()
        };
        assert!(
            serde_json::to_value(&sentinel)
                .unwrap()
                .get("role")
                .is_none(),
            "the sentinel is the skip condition; the setter cannot reach it"
        );
    }

    #[test]
    fn share_with_replacement_uses_the_rfc_property_names() {
        let mut patch = MailboxPatch::default();
        patch.acls([("u1", [ACL::ReadItems, ACL::AddItems])]);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(
            value.get("shareWith").and_then(|v| v.get("u1")),
            Some(&json!({"mayReadItems": true, "mayAddItems": true}))
        );
    }

    #[test]
    fn acl_set_builds_a_patch_path_the_server_recognises() {
        let mut patch = MailboxPatch::default();
        patch.acl_set("u1", ACL::ReadItems, true);
        patch.acl_set("u1", ACL::Administer, false);
        let value = serde_json::to_value(&patch).unwrap();

        assert_eq!(value.get("shareWith/u1/mayReadItems"), Some(&json!(true)));
        assert_eq!(value.get("shareWith/u1/mayShare"), Some(&json!(false)));
    }

    #[test]
    fn on_destroy_remove_emails_flattens_into_the_set_request() {
        let set = MailboxSet::new()
            .destroy([MailboxId::new("mb1")])
            .on_destroy_remove_emails(false);
        let value = serde_json::to_value(&set).unwrap();
        assert_eq!(value.get("destroy"), Some(&json!(["mb1"])));
        assert_eq!(value.get("onDestroyRemoveEmails"), Some(&json!(false)));
    }

    #[test]
    fn mailbox_decodes_counts_and_rights() {
        let mailbox: crate::mailbox::Mailbox = serde_json::from_str(
            r#"{"id":"mb1","name":"Inbox","parentId":null,"role":"inbox","sortOrder":0,
                "totalEmails":10,"unreadEmails":3,"totalThreads":8,"unreadThreads":2,
                "isSubscribed":true,
                "myRights":{"mayReadItems":true,"mayAddItems":false}}"#,
        )
        .expect("mailbox decodes");

        assert_eq!(mailbox.name(), Some("Inbox"));
        assert_eq!(mailbox.role(), Some(&Role::Inbox));
        assert_eq!(mailbox.parent_id(), None, "a null parentId reads as absent");
        assert_eq!(mailbox.total_emails(), Some(10));
        assert_eq!(mailbox.unread_threads(), Some(2));
        assert_eq!(mailbox.is_subscribed(), Some(true));

        let rights = mailbox.my_rights().expect("myRights present");
        assert!(rights.may_read_items());
        // Rights the server omitted default to false rather than
        // failing the decode.
        assert!(!rights.may_submit());
        assert_eq!(rights.acl_list(), vec![ACL::ReadItems]);
    }
}

// ---------------------------------------------------------------------------
// EmailSubmission wire shapes (RFC 8621 s7)
// ---------------------------------------------------------------------------

#[cfg(feature = "mail")]
mod email_submission_wire {
    use super::*;
    use crate::email_submission::{Address, EmailSubmissionSet, UndoStatus};

    #[test]
    fn envelope_address_parameters() {
        // RFC 4865 FUTURERELEASE rides as a `mailFrom` parameter.
        let held = Address::new("me@example.com")
            .with_parameter("holduntil", Some("2026-01-02T03:04:05Z"));
        assert_eq!(
            serde_json::to_value(&held).unwrap(),
            json!({
                "email": "me@example.com",
                "parameters": {"holduntil": "2026-01-02T03:04:05Z"}
            })
        );

        // A valueless ESMTP parameter is a null value, not an empty
        // string.
        let flagged = Address::new("me@example.com").with_parameter("body", None::<String>);
        assert_eq!(
            serde_json::to_value(&flagged).unwrap(),
            json!({"email": "me@example.com", "parameters": {"body": null}})
        );

        // With no parameters at all the key is still emitted, as an
        // explicit null. RFC 8621 s7 types `parameters` as
        // `String[String|null]|null`: a nullable member, not an optional
        // one, and RFC 8620 s5.3 only permits omitting a create property
        // that has a defined default. Omitting it is therefore a
        // rejection risk on strict servers, so the null must be PRESENT.
        assert_eq!(
            serde_json::to_value(Address::new("me@example.com")).unwrap(),
            json!({"email": "me@example.com", "parameters": null})
        );
    }

    #[test]
    fn create_and_on_success_arguments() {
        let mut set = EmailSubmissionSet::new();
        {
            let create = set.create();
            create.identity_id("identity-1");
            create.email_id("e1");
            create.envelope("me@example.com", ["you@example.com"]);
            create.undo_status(UndoStatus::Pending);
        }
        set.on_success_update_email("c0")
            .keyword(crate::email::DRAFT_KEYWORD, false);
        let set = set.on_success_destroy_email("c0");

        let value = serde_json::to_value(&set).unwrap();

        let created = value
            .get("create")
            .and_then(|c| c.get("c0"))
            .expect("create entry c0");
        assert_eq!(created.get("identityId"), Some(&json!("identity-1")));
        assert_eq!(created.get("emailId"), Some(&json!("e1")));
        assert_eq!(created.get("undoStatus"), Some(&json!("pending")));
        assert_eq!(
            created.get("envelope"),
            Some(&json!({
                "mailFrom": {"email": "me@example.com", "parameters": null},
                "rcptTo": [{"email": "you@example.com", "parameters": null}]
            }))
        );

        // Both onSuccess arguments key off the create-id with a `#`
        // prefix (RFC 8621 s7.5).
        assert_eq!(
            value.get("onSuccessUpdateEmail"),
            Some(&json!({"#c0": {"keywords/$draft": null}}))
        );
        assert_eq!(value.get("onSuccessDestroyEmail"), Some(&json!(["#c0"])));
    }

    #[test]
    fn a_submission_id_reference_is_not_hash_prefixed() {
        let mut set = EmailSubmissionSet::new();
        set.on_success_update_email_id(crate::email_submission::EmailSubmissionId::new("sub-1"))
            .subject("ignored");
        let value = serde_json::to_value(&set).unwrap();
        assert_eq!(
            value.get("onSuccessUpdateEmail"),
            Some(&json!({"sub-1": {"subject": "ignored"}}))
        );
    }

    #[test]
    fn undo_status_patch() {
        let mut set = EmailSubmissionSet::new();
        set.update(crate::email_submission::EmailSubmissionId::new("sub-1"))
            .undo_status(UndoStatus::Canceled);
        assert_eq!(
            serde_json::to_value(&set).unwrap().get("update"),
            Some(&json!({"sub-1": {"undoStatus": "canceled"}}))
        );
    }

    #[test]
    fn submission_decodes_delivery_status() {
        let submission: crate::email_submission::EmailSubmission = serde_json::from_str(
            r#"{"id":"sub-1","identityId":"i1","emailId":"e1","threadId":"t1",
                "sendAt":"2026-01-02T03:04:05Z","undoStatus":"final",
                "deliveryStatus":{"you@example.com":{"smtpReply":"250 ok",
                    "delivered":"yes","displayed":"unknown"}}}"#,
        )
        .expect("submission decodes");

        assert_eq!(submission.send_at(), Some(1_767_323_045));
        assert_eq!(submission.undo_status(), Some(&UndoStatus::Final));
        let status = submission
            .delivery_status_email("you@example.com")
            .expect("delivery status");
        assert_eq!(status.smtp_reply(), "250 ok");
    }
}

// ---------------------------------------------------------------------------
// Identity / VacationResponse / SieveScript wire shapes
// ---------------------------------------------------------------------------

#[cfg(feature = "mail")]
mod settings_object_wire {
    use super::*;
    use crate::identity::IdentityPatch;
    use crate::sieve::SieveScriptSet;
    use crate::vacation_response::VacationResponsePatch;

    #[test]
    fn an_empty_identity_patch_omits_reply_to_and_bcc() {
        assert_eq!(
            serde_json::to_value(IdentityPatch::default()).unwrap(),
            json!({})
        );

        let mut patch = IdentityPatch::default();
        patch.name("Alice");
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"name": "Alice"}),
            "renaming an identity must not touch replyTo/bcc"
        );
    }

    #[test]
    fn identity_patch_clears_a_list_deliberately_with_none() {
        // The same `None`-means-null behaviour is what a caller who
        // *wants* to clear `replyTo` relies on (RFC 8621 s6 types it
        // `EmailAddress[]|null`), which is why the fix has to be a
        // three-state field rather than flipping the predicate.
        let mut patch = IdentityPatch::default();
        patch.reply_to(None::<std::iter::Empty<crate::email::EmailAddress>>);
        assert_eq!(
            serde_json::to_value(&patch).unwrap().get("replyTo"),
            Some(&serde_json::Value::Null)
        );
    }

    #[test]
    fn identity_patch_empty_list_clears_instead_of_omitting() {
        let mut patch = IdentityPatch::default();
        patch.reply_to(Some(std::iter::empty::<crate::email::EmailAddress>()));
        patch.bcc(Some(std::iter::empty::<crate::email::EmailAddress>()));
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"replyTo": null, "bcc": null})
        );
    }

    #[test]
    fn identity_patch_sets_a_reply_to_list() {
        let mut patch = IdentityPatch::default();
        patch.reply_to(Some(
            [crate::email::EmailAddress::new("r@example.com".to_string())].into_iter(),
        ));
        assert_eq!(
            serde_json::to_value(&patch).unwrap().get("replyTo"),
            Some(&json!([{"name": null, "email": "r@example.com"}]))
        );
    }

    #[test]
    fn vacation_patch_setters_clear_nullable_properties() {
        let mut patch = VacationResponsePatch::default();
        patch.is_enabled(false);
        patch.subject(None::<String>);
        patch.to_date(None);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"isEnabled": false, "subject": null, "toDate": null})
        );
    }

    #[test]
    fn vacation_response_decodes_dates() {
        let vacation: crate::vacation_response::VacationResponse = serde_json::from_str(
            r#"{"id":"singleton","isEnabled":true,"fromDate":"2026-01-02T03:04:05Z",
                "toDate":null,"subject":"Away","textBody":"back soon"}"#,
        )
        .expect("vacation decodes");
        assert!(vacation.is_enabled());
        assert_eq!(vacation.from_date(), Some(1_767_323_045));
        assert_eq!(vacation.to_date(), None);
        assert_eq!(vacation.subject(), Some("Away"));
    }

    #[test]
    fn sieve_activation_arguments() {
        // Activating a script created in the same request references the
        // create-id with a `#`; activating an existing one does not.
        let by_create_id = SieveScriptSet::new().on_success_activate_script("c0");
        assert_eq!(
            serde_json::to_value(&by_create_id)
                .unwrap()
                .get("onSuccessActivateScript"),
            Some(&json!("#c0"))
        );

        let by_id = SieveScriptSet::new()
            .on_success_activate_script_id(crate::sieve::SieveScriptId::new("s1"));
        assert_eq!(
            serde_json::to_value(&by_id)
                .unwrap()
                .get("onSuccessActivateScript"),
            Some(&json!("s1"))
        );

        let deactivate = SieveScriptSet::new().on_success_deactivate_script(true);
        assert_eq!(
            serde_json::to_value(&deactivate)
                .unwrap()
                .get("onSuccessDeactivateScript"),
            Some(&json!(true))
        );
    }

    #[test]
    fn sieve_validate_response_carries_a_set_error() {
        let response: crate::sieve::validate::SieveScriptValidateResponse = serde_json::from_str(
            r#"{"error":{"type":"invalidScript","description":"line 3: syntax error"}}"#,
        )
        .expect("validate response decodes");
        let error = response.into_error().expect("an error");
        assert_eq!(
            error.error_type(),
            &crate::core::set::SetErrorType::InvalidScript
        );
        assert_eq!(error.description(), Some("line 3: syntax error"));
    }
}

// ---------------------------------------------------------------------------
// Default-constructed PatchObjects must be empty
// ---------------------------------------------------------------------------
//
// `SetRequest::update` hands out `O::Patch::default()`. RFC 8620 s5.3
// makes a `null` value in a PatchObject a REMOVAL, so any property a
// default patch happens to serialise is a property every update in this
// crate silently deletes.
//
// Nullable Patch properties use `Field<T>` whenever they need to distinguish
// omission from an explicit property removal.

mod patch_defaults {
    use super::*;

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

    #[test]
    fn creates_install_empty_sentinels() {
        use crate::core::SetCreate;
        #[cfg(feature = "contacts")]
        assert_eq!(
            serde_json::to_value(crate::address_book::AddressBookCreate::new(Some(0))).unwrap(),
            json!({})
        );
        #[cfg(feature = "calendars")]
        assert_eq!(
            serde_json::to_value(crate::participant_identity::ParticipantIdentityCreate::new(
                Some(0)
            ))
            .unwrap(),
            json!({"calendarAddress": ""})
        );
        #[cfg(feature = "mail")]
        {
            assert_eq!(
                serde_json::to_value(crate::mailbox::MailboxCreate::new(Some(0))).unwrap(),
                json!({})
            );
            assert_eq!(
                serde_json::to_value(crate::identity::IdentityCreate::new(Some(0))).unwrap(),
                json!({})
            );
            assert_eq!(
                serde_json::to_value(crate::vacation_response::VacationResponseCreate::new(Some(
                    0
                )))
                .unwrap(),
                json!({})
            );
        }
        assert_eq!(
            serde_json::to_value(crate::principal::PrincipalCreate::new(Some(0))).unwrap(),
            json!({})
        );
    }
}

// ---------------------------------------------------------------------------
// SetError vocabulary (RFC 8620 s5.3 / RFC 8621 / sieve draft)
// ---------------------------------------------------------------------------

mod set_error_vocabulary {
    use crate::core::set::{SetError, SetErrorType};

    fn decode(code: &str) -> SetErrorType {
        let error: SetError<String> =
            serde_json::from_str(&format!(r#"{{"type":"{code}"}}"#)).expect("set error decodes");
        error.error_type().clone()
    }

    #[test]
    fn known_codes_map_to_typed_variants_and_render_back() {
        for (code, variant) in [
            ("forbidden", SetErrorType::Forbidden),
            ("overQuota", SetErrorType::OverQuota),
            ("tooLarge", SetErrorType::TooLarge),
            ("rateLimit", SetErrorType::RateLimit),
            ("notFound", SetErrorType::NotFound),
            ("invalidPatch", SetErrorType::InvalidPatch),
            ("willDestroy", SetErrorType::WillDestroy),
            ("invalidProperties", SetErrorType::InvalidProperties),
            ("singleton", SetErrorType::Singleton),
            ("mailboxHasChild", SetErrorType::MailboxHasChild),
            ("mailboxHasEmail", SetErrorType::MailboxHasEmail),
            ("blobNotFound", SetErrorType::BlobNotFound),
            ("tooManyKeywords", SetErrorType::TooManyKeywords),
            ("tooManyMailboxes", SetErrorType::TooManyMailboxes),
            ("forbiddenFrom", SetErrorType::ForbiddenFrom),
            ("invalidEmail", SetErrorType::InvalidEmail),
            ("tooManyRecipients", SetErrorType::TooManyRecipients),
            ("noRecipients", SetErrorType::NoRecipients),
            ("invalidRecipients", SetErrorType::InvalidRecipients),
            ("forbiddenMailFrom", SetErrorType::ForbiddenMailFrom),
            ("forbiddenToSend", SetErrorType::ForbiddenToSend),
            ("cannotUnsend", SetErrorType::CannotUnsend),
            ("alreadyExists", SetErrorType::AlreadyExists),
            ("invalidScript", SetErrorType::InvalidScript),
            ("scriptIsActive", SetErrorType::ScriptIsActive),
        ] {
            assert_eq!(decode(code), variant, "decoding {code}");
            assert_eq!(variant.to_string(), code, "rendering {code}");
        }
    }

    #[test]
    fn an_unknown_code_keeps_the_literal_the_server_sent() {
        // The gate-5 invariant: never synthesise an "other" literal; the
        // real wire code has to reach `WireCause::Jmap(Unknown { code })`.
        let decoded = decode("vendorSpecificFailure");
        assert_eq!(
            decoded,
            SetErrorType::Other("vendorSpecificFailure".to_string())
        );
        assert_eq!(decoded.to_string(), "vendorSpecificFailure");
    }

    #[test]
    fn set_error_display_includes_description_and_properties() {
        let error: SetError<String> = serde_json::from_str(
            r#"{"type":"invalidProperties","description":"bad","properties":["a","b"]}"#,
        )
        .expect("decodes");
        assert_eq!(
            error.to_string(),
            "invalidProperties: bad (properties: a, b)"
        );
    }
}

// ---------------------------------------------------------------------------
// `#[non_exhaustive]` wire enums with no catch-all
// ---------------------------------------------------------------------------
//
// These enums are marked `#[non_exhaustive]` and degrade unknown wire
// values rather than failing the containing response. `DataType`, `Role`,
// `AlertTrigger`, and `SetErrorType` follow the same policy.

mod wire_enums_without_a_catch_all {
    #[cfg(feature = "mail")]
    #[test]
    fn undo_status_degrades_an_unknown_value() {
        assert_eq!(
            serde_json::from_str::<crate::email_submission::UndoStatus>(r#""queued""#).unwrap(),
            crate::email_submission::UndoStatus::Unknown
        );
    }

    #[cfg(feature = "mail")]
    #[test]
    fn delivery_state_degrades_an_unknown_value() {
        assert_eq!(
            serde_json::from_str::<crate::email_submission::Delivered>(r#""bounced""#).unwrap(),
            crate::email_submission::Delivered::Other
        );
        assert_eq!(
            serde_json::from_str::<crate::email_submission::Displayed>(r#""no""#).unwrap(),
            crate::email_submission::Displayed::Other
        );
    }

    #[cfg(feature = "calendars")]
    #[test]
    fn calendar_enums_degrade_unknown_values() {
        assert_eq!(
            serde_json::from_str::<crate::calendar_event::AlertAction>(r#""audio""#).unwrap(),
            crate::calendar_event::AlertAction::Unknown
        );
        assert_eq!(
            serde_json::from_str::<crate::calendar_event::RelativeTo>(r#""alarm""#).unwrap(),
            crate::calendar_event::RelativeTo::Unknown
        );
        assert_eq!(
            serde_json::from_str::<crate::calendar_event_notification::NotificationType>(
                r#""sent""#
            )
            .unwrap(),
            crate::calendar_event_notification::NotificationType::Unknown
        );
    }

    #[cfg(feature = "calendars")]
    #[test]
    fn include_in_availability_degrades_an_unknown_value() {
        assert_eq!(
            serde_json::from_str::<crate::calendar::IncludeInAvailability>(r#""maybe""#).unwrap(),
            crate::calendar::IncludeInAvailability::Unknown
        );
    }

    #[test]
    fn principal_type_degrades_an_unknown_value() {
        // `Type::Other` is a real RFC value, distinct from the
        // deserialize-only catch-all.
        assert_eq!(
            serde_json::from_str::<crate::principal::Type>(r#""room""#).unwrap(),
            crate::principal::Type::Unknown
        );
        assert_eq!(
            serde_json::from_str::<crate::principal::Type>(r#""other""#).unwrap(),
            crate::principal::Type::Other
        );
    }

    #[test]
    fn data_type_does_have_a_catch_all() {
        assert_eq!(
            serde_json::from_str::<crate::DataType>(r#""SomeFutureType""#).unwrap(),
            crate::DataType::Other("SomeFutureType".to_string())
        );
    }
}

// ---------------------------------------------------------------------------
// DataType wire names (RFC 8620 s7 push / RFC 8887 dataTypes)
// ---------------------------------------------------------------------------

mod data_type_wire {
    use super::*;
    use crate::DataType;

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

// ---------------------------------------------------------------------------
// Session capability decoding fallbacks
// ---------------------------------------------------------------------------

mod session_capability_fallbacks {
    use super::*;
    use crate::core::session::{Capabilities, Session};

    fn session_with(capabilities: serde_json::Value) -> Session {
        serde_json::from_value(json!({
            "capabilities": capabilities,
            "accounts": {},
            "primaryAccounts": {},
            "username": "u@example.org",
            "apiUrl": "https://example.org/jmap/",
            "downloadUrl": "https://example.org/dl/{accountId}/{blobId}/{name}?accept={type}",
            "uploadUrl": "https://example.org/ul/{accountId}/",
            "eventSourceUrl": "https://example.org/es/?types={types}&closeafter={closeafter}&ping={ping}",
            "state": "s0"
        }))
        .expect("session decodes")
    }

    #[test]
    fn websocket_capability_decodes() {
        let session = session_with(json!({
            "urn:ietf:params:jmap:websocket": {
                "url": "wss://example.org/jmap/ws",
                "supportsPush": true
            }
        }));
        let ws = session.websocket_capabilities().expect("websocket cap");
        assert_eq!(ws.url(), "wss://example.org/jmap/ws");
        assert!(ws.supports_push());
    }

    #[test]
    fn websocket_capability_defaults_missing_supports_push_to_false() {
        let session = session_with(json!({
            "urn:ietf:params:jmap:websocket": {"url": "wss://example.org/jmap/ws"}
        }));
        let websocket = session
            .websocket_capabilities()
            .expect("websocket capability");
        assert_eq!(websocket.url(), "wss://example.org/jmap/ws");
        assert!(!websocket.supports_push());
    }

    #[test]
    fn core_capabilities_omitted_limits_are_none_not_zero() {
        // `CoreCapabilities` is `#[serde(default)]`, so an empty object
        // decodes rather than falling through to `Other`. Each omitted
        // limit reads as `None` - "the server advertised nothing" - which
        // the sync layer must not confuse with an advertised `0`.
        let session = session_with(json!({"urn:ietf:params:jmap:core": {}}));
        let core = session.core_capabilities().expect("core cap");
        assert_eq!(core.max_objects_in_get(), None);
        assert_eq!(core.max_objects_in_set(), None);
        assert!(core.collation_algorithms().is_empty());
    }

    /// A present-but-unparseable core block keeps its own state. `Other`
    /// would have made it indistinguishable from an absent capability at
    /// every reader, which is exactly how a string-typed limit came to be
    /// reported as "the server stopped advertising core".
    #[test]
    fn a_malformed_core_block_keeps_its_own_state_rather_than_reading_as_absent() {
        use crate::core::session::CoreCapabilityState;

        let session = session_with(json!({
            "urn:ietf:params:jmap:core": {"maxCallsInRequest": "16"}
        }));
        assert!(matches!(
            session.core_capability_state(),
            CoreCapabilityState::Malformed
        ));
        assert!(
            session.core_capabilities().is_none(),
            "the typed accessor still has nothing to hand back"
        );
        assert!(
            matches!(
                session.capability("urn:ietf:params:jmap:core"),
                Some(Capabilities::Malformed(v)) if v == &json!({"maxCallsInRequest": "16"})
            ),
            "and the offending value is preserved verbatim for diagnostics"
        );

        let absent = session_with(json!({}));
        assert!(matches!(
            absent.core_capability_state(),
            CoreCapabilityState::Absent
        ));
        let present = session_with(json!({"urn:ietf:params:jmap:core": {}}));
        assert!(matches!(
            present.core_capability_state(),
            CoreCapabilityState::Present(_)
        ));
    }

    /// The core block was the first capability to get the three-state
    /// treatment; it must not be the only one. A websocket block with no
    /// `url` is present-and-wrong, and reading it as absent is how a
    /// server bug turned into "this server has no push" with nothing said.
    #[test]
    fn a_malformed_non_core_block_is_malformed_and_not_absent() {
        use crate::core::session::CapabilityState;

        let session = session_with(json!({
            "urn:ietf:params:jmap:websocket": {"supportsPush": true}
        }));
        assert!(matches!(
            session.websocket_capability_state(),
            CapabilityState::Malformed
        ));
        assert!(session.websocket_capabilities().is_none());
        assert!(
            matches!(
                session.capability("urn:ietf:params:jmap:websocket"),
                Some(Capabilities::Malformed(v)) if v == &json!({"supportsPush": true})
            ),
            "the offending value is preserved verbatim for diagnostics"
        );
        assert_eq!(
            session.malformed_capabilities().collect::<Vec<_>>(),
            vec!["urn:ietf:params:jmap:websocket"]
        );

        let absent = session_with(json!({}));
        assert!(matches!(
            absent.websocket_capability_state(),
            CapabilityState::Absent
        ));
        assert_eq!(absent.malformed_capabilities().count(), 0);
    }

    /// The capability URIs this crate MODELS, which is feature-dependent by
    /// construction: `Capabilities::parse` gates its match arms, so a URI whose
    /// feature is off is genuinely not modelled and correctly falls back to
    /// `Other` rather than taking the malformed lane.
    ///
    /// This mirrors those arms, and the mirror is the point: both tests below
    /// used to carry their own unconditional copy of this list, which passed
    /// for as long as nothing ever built the crate with a capability feature
    /// off. The first run that did (calendars off) failed both.
    // `mut` is unused in a build with every capability feature off, since every
    // extend below is gated.
    #[allow(unused_mut)]
    fn modelled_capability_uris() -> Vec<&'static str> {
        let mut uris = vec![
            "urn:ietf:params:jmap:core",
            "urn:ietf:params:jmap:websocket",
            "urn:ietf:params:jmap:principals",
            "urn:ietf:params:jmap:principals:owner",
        ];
        #[cfg(feature = "mail")]
        uris.extend([
            "urn:ietf:params:jmap:mail",
            "urn:ietf:params:jmap:submission",
            "urn:ietf:params:jmap:sieve",
        ]);
        #[cfg(feature = "quota")]
        uris.push("urn:ietf:params:jmap:quota");
        #[cfg(feature = "blob")]
        uris.push("urn:ietf:params:jmap:blob");
        #[cfg(feature = "calendars")]
        uris.push("urn:ietf:params:jmap:calendars");
        #[cfg(feature = "contacts")]
        uris.push("urn:ietf:params:jmap:contacts");
        uris
    }

    /// Every typed capability the crate models, not a hand-picked few:
    /// a recognized URI carrying a value that cannot possibly be its
    /// object must land in `Malformed`, never in `Other`.
    #[test]
    fn every_modelled_capability_uri_has_a_malformed_lane() {
        for uri in modelled_capability_uris() {
            let session = session_with(json!({uri: "not an object at all"}));
            assert!(
                matches!(session.capability(uri), Some(Capabilities::Malformed(_))),
                "{uri} must take the malformed lane, not fall back to Other"
            );
            assert_eq!(
                session.malformed_capabilities().collect::<Vec<_>>(),
                vec![uri]
            );
        }
    }

    /// A JSON ARRAY is the malformed shape a string does not catch: serde's
    /// derived struct deserializers accept a SEQUENCE in field-declaration
    /// order, so `[]` parses cleanly into any capability struct whose
    /// fields all default - `"urn:ietf:params:jmap:calendars": []` read as
    /// a fully advertised block, and `["16"]` fed the core block a limit
    /// positionally. RFC 8620 s2 makes a capability value an object, so
    /// every modelled URI must refuse a non-object before the typed parse.
    #[test]
    fn a_capability_sent_as_an_array_is_malformed_and_not_present() {
        use crate::core::session::CoreCapabilityState;

        for uri in modelled_capability_uris() {
            let session = session_with(json!({uri: []}));
            assert!(
                matches!(session.capability(uri), Some(Capabilities::Malformed(v)) if v.is_array()),
                "{uri} sent as an array must be malformed, not an all-defaults present block"
            );
            assert_eq!(
                session.malformed_capabilities().collect::<Vec<_>>(),
                vec![uri]
            );
        }

        // Core, specifically: an array is not a present-with-limits block.
        let core = session_with(json!({"urn:ietf:params:jmap:core": []}));
        assert!(matches!(
            core.core_capability_state(),
            CoreCapabilityState::Malformed
        ));

        // And one optional family, whose every field defaults - the case
        // that parsed as Present before the guard.
        #[cfg(feature = "calendars")]
        {
            use crate::core::session::CapabilityState;
            let calendars = session_with(json!({"urn:ietf:params:jmap:calendars": []}));
            assert!(matches!(
                calendars.calendars_capability_state(),
                CapabilityState::Malformed
            ));
        }
    }

    /// `maxConcurrentUpload` is parsed and, by design, has no reader (see
    /// `reference/jmap.md`): every upload door in this crate awaits one
    /// upload at a time, so there is no concurrency for the limit to
    /// govern. It still has to survive the session decode, so that wiring
    /// it later is a reader change and not a parser change.
    #[test]
    fn core_max_concurrent_upload_is_parsed_even_though_nothing_reads_it() {
        let session = session_with(json!({
            "urn:ietf:params:jmap:core": {"maxConcurrentUpload": 4}
        }));
        let core = session.core_capabilities().expect("core cap");
        assert_eq!(core.max_concurrent_upload(), Some(4));

        let omitted = session_with(json!({"urn:ietf:params:jmap:core": {}}));
        assert_eq!(
            omitted
                .core_capabilities()
                .expect("core cap")
                .max_concurrent_upload(),
            None,
            "omitted must stay distinguishable from an advertised zero"
        );
    }

    #[test]
    fn an_unknown_capability_uri_is_preserved_verbatim() {
        let session = session_with(json!({"urn:vendor:thing": {"a": 1}}));
        match session.capability("urn:vendor:thing") {
            Some(Capabilities::Other(v)) => assert_eq!(v, &json!({"a": 1})),
            other => panic!("expected Other, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// URL template parsing (RFC 8620 s2 download/upload/eventSource URLs)
// ---------------------------------------------------------------------------

mod url_template_parsing {
    use crate::core::session::{URLParser, URLPart};

    #[derive(Debug, PartialEq, Eq)]
    enum P {
        A,
        B,
    }

    impl URLParser for P {
        fn parse(value: &str) -> Option<Self> {
            match value {
                "a" => Some(P::A),
                "b" => Some(P::B),
                _ => None,
            }
        }
    }

    fn parts(url: &str) -> Vec<String> {
        URLPart::<P>::parse(url)
            .expect("parses")
            .into_iter()
            .map(|part| match part {
                URLPart::Value(v) => format!("value:{v}"),
                URLPart::Parameter(p) => format!("param:{p:?}"),
            })
            .collect()
    }

    #[test]
    fn literals_and_parameters_alternate() {
        assert_eq!(
            parts("https://x/{a}/y/{b}"),
            vec![
                "value:https://x/".to_string(),
                "param:A".to_string(),
                "value:/y/".to_string(),
                "param:B".to_string(),
            ]
        );
    }

    #[test]
    fn adjacent_parameters_produce_no_empty_literal() {
        assert_eq!(
            parts("{a}{b}"),
            vec!["param:A".to_string(), "param:B".to_string()]
        );
    }

    #[test]
    fn malformed_templates_are_rejected() {
        // Unterminated parameter.
        assert!(URLPart::<P>::parse("https://x/{a").is_err());
        // Empty parameter.
        assert!(URLPart::<P>::parse("https://x/{}").is_err());
        // Closing brace with no opening one.
        assert!(URLPart::<P>::parse("https://x/a}").is_err());
        // Unknown parameter name.
        assert!(URLPart::<P>::parse("https://x/{zzz}").is_err());
        // An unterminated empty parameter.
        assert!(URLPart::<P>::parse("https://x/{").is_err());
        // A nested opening brace is not part of a parameter name.
        assert!(URLPart::<P>::parse("https://x/{{a}").is_err());
    }

    #[test]
    fn blob_url_parameters_cover_the_rfc_set() {
        use crate::blob::URLParameter;
        assert!(matches!(
            URLParameter::parse("accountId"),
            Some(URLParameter::AccountId)
        ));
        assert!(matches!(
            URLParameter::parse("blobId"),
            Some(URLParameter::BlobId)
        ));
        assert!(matches!(
            URLParameter::parse("name"),
            Some(URLParameter::Name)
        ));
        assert!(matches!(
            URLParameter::parse("type"),
            Some(URLParameter::Type)
        ));
        assert!(URLParameter::parse("size").is_none());
    }
}

// ---------------------------------------------------------------------------
// Blob management wire shapes (RFC 9404)
// ---------------------------------------------------------------------------

#[cfg(feature = "blob")]
mod blob_management_wire {
    use super::*;
    use crate::blob::manage::{
        BlobGetResponse, BlobLookupRequest, BlobLookupResponse, BlobUploadRequest, DataSource,
        DataSourceBlob,
    };

    #[test]
    fn upload_request_keys_creates_and_concatenates_sources() {
        let mut request = BlobUploadRequest::new();
        let first = request.create_from_text("hello", Some("text/plain"));
        let second = request.create_with_sources(
            vec![
                DataSource::Blob(DataSourceBlob {
                    blob_id: "b1".into(),
                    offset: Some(0),
                    length: Some(4),
                }),
                DataSource::Text(crate::blob::manage::DataSourceText {
                    value: "!".to_string(),
                }),
            ],
            None::<String>,
        );
        assert_eq!(first, "b0");
        assert_eq!(second, "b1");

        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            value.get("create").and_then(|c| c.get("b0")),
            Some(&json!({"data": [{"data:asText": "hello"}], "type": "text/plain"}))
        );
        assert_eq!(
            value.get("create").and_then(|c| c.get("b1")),
            Some(&json!({
                "data": [
                    {"blobId": "b1", "offset": 0, "length": 4},
                    {"data:asText": "!"}
                ]
            })),
            "an omitted content type must not appear as null"
        );
    }

    #[test]
    fn get_response_splits_named_fields_from_dynamic_ones() {
        let response: BlobGetResponse = serde_json::from_str(
            r#"{"accountId":"a1","list":[{"id":"b1","size":5,"isTruncated":false,
                "data:asText":"hello","digest:sha-256":"deadbeef"}],"notFound":["b2"]}"#,
        )
        .expect("blob get decodes");

        let entry = &response.list()[0];
        assert_eq!(entry.size, Some(5));
        assert_eq!(entry.is_truncated, Some(false));
        assert_eq!(entry.data_as_text(), Some("hello"));
        assert_eq!(entry.digest("sha-256"), Some("deadbeef"));
        // The named fields must not be duplicated into the flatten map.
        assert!(!entry.properties.contains_key("size"));
        assert!(!entry.properties.contains_key("id"));
        assert_eq!(
            response.not_found().map(<[crate::core::id::BlobId]>::len),
            Some(1),
            "notFound is optional but present here"
        );
    }

    #[test]
    fn lookup_round_trip() {
        let request = BlobLookupRequest::new()
            .type_names(["Email", "CalendarEvent"])
            .ids(["b1"]);
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            value.get("typeNames"),
            Some(&json!(["Email", "CalendarEvent"]))
        );
        assert_eq!(value.get("ids"), Some(&json!(["b1"])));

        let response: BlobLookupResponse = serde_json::from_str(
            r#"{"accountId":"a1","list":[{"id":"b1","matchedIds":{"Email":["e1","e2"]}}]}"#,
        )
        .expect("lookup decodes");
        assert_eq!(
            response.list()[0].matched_ids.get("Email"),
            Some(&vec!["e1".to_string(), "e2".to_string()])
        );
        assert!(response.not_found().is_none());
    }
}

// ---------------------------------------------------------------------------
// Three-state `Field<T>` on the typed-struct objects
// ---------------------------------------------------------------------------

#[cfg(feature = "quota")]
mod quota_field_three_state {
    use crate::core::field::Field;
    use crate::quota::Quota;

    #[test]
    fn omitted_null_and_present_are_distinguishable() {
        let omitted: Quota = serde_json::from_str(r#"{"id":"q1"}"#).unwrap();
        assert!(omitted.warn_limit_field().is_omitted());

        let null: Quota = serde_json::from_str(r#"{"id":"q1","warnLimit":null}"#).unwrap();
        assert!(null.warn_limit_field().is_null());

        let present: Quota = serde_json::from_str(r#"{"id":"q1","warnLimit":42}"#).unwrap();
        assert_eq!(present.warn_limit_field(), &Field::Value(42));
        assert_eq!(present.warn_limit(), Some(42));
    }

    #[test]
    fn a_null_field_reads_the_same_as_an_omitted_one_through_the_option_getter() {
        // The ergonomic getter collapses the two; the `_field()` sibling
        // is the only way to tell them apart when generating a patch.
        let null: Quota = serde_json::from_str(r#"{"id":"q1","description":null}"#).unwrap();
        let omitted: Quota = serde_json::from_str(r#"{"id":"q1"}"#).unwrap();
        assert_eq!(null.description(), omitted.description());
        assert_ne!(
            null.description_field().is_null(),
            omitted.description_field().is_null()
        );
    }

    #[test]
    fn quota_decodes_the_rfc_9425_shape() {
        let quota: Quota = serde_json::from_str(
            r#"{"id":"q1","resourceType":"octets","used":100,"hardLimit":1000,
                "scope":"account","name":"Storage","types":["Mail"],"softLimit":900}"#,
        )
        .expect("quota decodes");
        assert_eq!(quota.resource_type(), Some("octets"));
        assert_eq!(quota.used(), Some(100));
        assert_eq!(quota.hard_limit(), Some(1000));
        assert_eq!(quota.scope(), Some("account"));
        assert_eq!(quota.soft_limit(), Some(900));
    }
}

#[cfg(feature = "contacts")]
mod address_book_wire {
    use super::*;
    use crate::address_book::{AddressBook, AddressBookPatch};

    #[test]
    fn patching_a_nullable_field_to_null_reaches_the_wire() {
        // Like `MailboxPatch::parent_id` and `VacationResponsePatch`, this
        // uses `Field<T>` to preserve explicit property removal.
        let mut patch = AddressBookPatch::default();
        patch.name("Work");
        patch.description(None::<String>);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(value.get("name"), Some(&json!("Work")));
        assert_eq!(value.get("description"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn an_untouched_field_is_omitted_from_the_patch() {
        let mut patch = AddressBookPatch::default();
        patch.name("Work");
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"name": "Work"})
        );
    }

    #[test]
    fn address_book_decodes_rights() {
        let book: AddressBook = serde_json::from_str(
            r#"{"id":"ab1","name":"Personal","description":null,"sortOrder":0,
                "isDefault":true,"isSubscribed":true,
                "myRights":{"mayRead":true,"mayWrite":false}}"#,
        )
        .expect("address book decodes");
        assert_eq!(book.name(), Some("Personal"));
        assert!(book.description_field().is_null());
        assert_eq!(book.is_default(), Some(true));
        let rights = book.my_rights().expect("rights");
        assert_eq!(rights.may_read, Some(true));
        assert_eq!(rights.may_write, Some(false));
        assert_eq!(rights.may_share, None, "omitted rights stay unknown");
    }
}

#[cfg(feature = "calendars")]
mod calendar_wire {
    use super::*;
    use crate::calendar::{Calendar, CalendarPatch, IncludeInAvailability};

    #[test]
    fn calendar_decodes_the_full_draft_shape() {
        let calendar: Calendar = serde_json::from_str(
            r##"{"id":"cal1","name":"Work","description":null,"color":"#112233",
                "sortOrder":1,"isSubscribed":true,"isVisible":true,"isDefault":false,
                "includeInAvailability":"attending","timeZone":"Europe/Oslo",
                "myRights":{"mayReadItems":true,"mayWriteAll":false,"mayRSVP":true}}"##,
        )
        .expect("calendar decodes");

        assert_eq!(calendar.name(), Some("Work"));
        assert!(calendar.description_field().is_null());
        assert_eq!(calendar.color(), Some("#112233"));
        assert_eq!(calendar.time_zone(), Some("Europe/Oslo"));
        assert_eq!(
            calendar.include_in_availability(),
            Some(&IncludeInAvailability::Attending)
        );
        let rights = calendar.my_rights().expect("rights");
        assert_eq!(
            rights.may_rsvp,
            Some(true),
            "`mayRSVP` keeps the RFC casing"
        );
        assert_eq!(rights.may_read_items, Some(true));
    }

    #[test]
    fn calendar_patch_clears_and_sets_alerts() {
        let mut patch = CalendarPatch::default();
        patch.default_alerts_with_time(None);
        patch.is_visible(false);
        let value = serde_json::to_value(&patch).unwrap();
        assert_eq!(
            value.get("defaultAlertsWithTime"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(value.get("isVisible"), Some(&json!(false)));
        assert!(value.get("defaultAlertsWithoutTime").is_none());
    }

    #[test]
    fn calendar_set_arguments_flatten() {
        let set = crate::calendar::CalendarSet::new()
            .destroy([crate::calendar::CalendarId::new("cal1")])
            .on_destroy_remove_events(true)
            .on_success_set_is_default("cal2");
        let value = serde_json::to_value(&set).unwrap();
        assert_eq!(value.get("onDestroyRemoveEvents"), Some(&json!(true)));
        assert_eq!(value.get("onSuccessSetIsDefault"), Some(&json!("cal2")));
    }
}

// ---------------------------------------------------------------------------
// ParticipantIdentity wire shape (JMAP Calendars draft-26 s3)
// ---------------------------------------------------------------------------

#[cfg(feature = "calendars")]
mod participant_identity_wire {
    use super::*;
    use crate::core::SetCreate;
    use crate::participant_identity::{
        ParticipantIdentity, ParticipantIdentityCreate, ParticipantIdentityPatch, Property,
    };

    #[test]
    fn calendar_address_decodes_and_round_trips_through_create_and_patch() {
        let identity: ParticipantIdentity = serde_json::from_value(json!({
            "id": "pi-1",
            "name": "Ada",
            "calendarAddress": "mailto:ada@example.test",
            "isDefault": true
        }))
        .expect("a draft-26 identity decodes");
        assert_eq!(identity.calendar_address(), Some("mailto:ada@example.test"));
        assert_eq!(
            serde_json::to_value(Property::CalendarAddress).unwrap(),
            json!("calendarAddress")
        );

        let mut create = ParticipantIdentityCreate::new(Some(0));
        create.calendar_address("mailto:ada@example.test");
        assert_eq!(
            serde_json::to_value(create).unwrap(),
            json!({"calendarAddress": "mailto:ada@example.test"})
        );

        let mut patch = ParticipantIdentityPatch::default();
        patch.calendar_address("mailto:grace@example.test");
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({"calendarAddress": "mailto:grace@example.test"})
        );
    }

    /// draft-26 §3 makes `calendarAddress` required and non-nullable, so
    /// the patch is two-state: omitted, or a String. A name-only patch
    /// must not emit `"calendarAddress": null` - that is a property
    /// removal the server has to answer with `invalidProperties`.
    #[test]
    fn patch_omits_calendar_address_rather_than_nulling_it() {
        let mut patch = ParticipantIdentityPatch::default();
        patch.name("Grace");
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({"name": "Grace"})
        );
    }
}

// ---------------------------------------------------------------------------
// CalendarEvent / ContactCard patch-vs-create membership updates
// ---------------------------------------------------------------------------

#[cfg(feature = "calendars")]
mod calendar_event_patch_nesting {
    use super::*;
    use crate::calendar_event::{CalendarEventPatch, CalendarEventSet};

    #[test]
    fn patch_calendar_id_uses_dotted_paths() {
        let mut patch = CalendarEventPatch::default();
        patch.calendar_id("cal-1", false);
        patch.calendar_id("cal-2", true);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"calendarIds/cal-1": null, "calendarIds/cal-2": true})
        );
    }

    #[test]
    fn calendar_membership_setters_never_overlap_wholesale_and_dotted_forms() {
        let mut patch = CalendarEventPatch::default();
        patch.calendar_id("cal-1", true);
        patch.calendar_ids(["cal-2"]);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"calendarIds": {"cal-2": true}})
        );

        patch.calendar_id("cal-3", false);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"calendarIds/cal-3": null})
        );
    }

    #[test]
    fn dotted_paths_are_available_through_set_property() {
        let mut patch = CalendarEventPatch::default();
        patch.set_property("participants/p1/participationStatus", json!("accepted"));
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"participants/p1/participationStatus": "accepted"})
        );
    }

    #[test]
    fn set_arguments_flatten_into_the_request() {
        let set = CalendarEventSet::new().send_scheduling_messages(false);
        assert_eq!(
            serde_json::to_value(&set)
                .unwrap()
                .get("sendSchedulingMessages"),
            Some(&json!(false))
        );
    }

    #[test]
    fn get_arguments_flatten_into_the_request() {
        let get = crate::calendar_event::CalendarEventGet::new()
            .ids(["ev1"])
            .recurrence_overrides_after("2026-01-01T00:00:00")
            .reduce_participants(true)
            .time_zone("Europe/Oslo");
        let value = serde_json::to_value(&get).unwrap();
        assert_eq!(
            value.get("recurrenceOverridesAfter"),
            Some(&json!("2026-01-01T00:00:00"))
        );
        assert_eq!(value.get("reduceParticipants"), Some(&json!(true)));
        assert_eq!(value.get("timeZone"), Some(&json!("Europe/Oslo")));
        assert!(value.get("recurrenceOverridesBefore").is_none());
    }
}

#[cfg(feature = "contacts")]
mod contact_card_patch_membership {
    use super::*;
    use crate::contact_card::ContactCardPatch;

    #[test]
    fn patch_address_book_id_uses_dotted_paths_without_overlap() {
        let mut patch = ContactCardPatch::default();
        patch.address_book_ids(["ab-1"]);
        patch.address_book_id("ab-2", false);
        patch.address_book_id("ab-3", true);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"addressBookIds/ab-2": null, "addressBookIds/ab-3": true})
        );
    }
}

// ---------------------------------------------------------------------------
// Thread / SearchSnippet / PushSubscription decode
// ---------------------------------------------------------------------------

#[cfg(feature = "mail")]
mod misc_mail_object_decode {
    use super::*;

    #[test]
    fn thread_partial_projections_decode() {
        let thread: crate::thread::Thread =
            serde_json::from_str(r#"{"id":"t1","emailIds":["e1","e2"]}"#).expect("thread decodes");
        assert_eq!(thread.email_ids().expect("emailIds requested").len(), 2);

        let partial: crate::thread::Thread =
            serde_json::from_str(r#"{"id":"t1"}"#).expect("partial projection decodes");
        assert!(partial.email_ids().is_none());

        // JMAP always includes an object's id in a /get response, even
        // when the requested projection excludes it.
        assert!(serde_json::from_str::<crate::thread::Thread>(r#"{"emailIds":["e1"]}"#).is_err());
    }

    #[test]
    fn search_snippet_request_shape() {
        let request = crate::email::search_snippet::SearchSnippetGetRequest::new()
            .filter(crate::email::query::Filter::text("needle"))
            .email_ids(["e1", "e2"]);
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value.get("filter"), Some(&json!({"text": "needle"})));
        assert_eq!(value.get("emailIds"), Some(&json!(["e1", "e2"])));
        assert!(value.get("#emailIds").is_none());
    }

    #[test]
    fn email_import_entries_are_keyed_i0_i1() {
        let mut request = crate::email::import::EmailImportRequest::new().if_in_state("s1");
        {
            let entry = request.email("blob-1");
            entry.mailbox_ids([crate::mailbox::MailboxId::new("mb1")]);
            entry.keywords(["$seen"]);
            assert_eq!(entry.create_id(), "i0");
        }
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value.get("ifInState"), Some(&json!("s1")));
        assert_eq!(
            value.get("emails").and_then(|e| e.get("i0")),
            Some(&json!({
                "blobId": "blob-1",
                "mailboxIds": {"mb1": true},
                "keywords": {"$seen": true}
            }))
        );
    }
}

mod push_subscription_wire {
    use super::*;
    use crate::core::SetCreate;
    use crate::push_subscription::{PushSubscriptionCreate, PushSubscriptionGet};

    #[test]
    fn push_subscription_get_has_no_account_id() {
        // RFC 8620 s7.2: PushSubscription is not account-scoped, so the
        // request must not carry an `accountId` even though
        // `Request::call` injects one into every other method.
        use crate::core::method::JmapMethod;
        let mut request = PushSubscriptionGet::new();
        request.set_account_id(&crate::core::id::AccountId::new("a1"));
        let value = serde_json::to_value(&request).unwrap();
        assert!(
            value.get("accountId").is_none(),
            "accountId must stay absent for a non-account-scoped type"
        );
    }

    #[test]
    fn a_fresh_create_omits_the_empty_types_list() {
        let create = PushSubscriptionCreate::new(Some(0));
        assert_eq!(serde_json::to_value(&create).unwrap(), json!({}));
    }
}

// ---------------------------------------------------------------------------
// Principal ACL vocabulary (RFC 8621 s2 shareWith)
// ---------------------------------------------------------------------------

mod principal_property_wire_names {
    use super::*;
    use crate::principal::Property;

    #[test]
    fn property_display_and_serde_match_rfc_9670() {
        for (property, wire) in [
            (Property::Id, "id"),
            (Property::Type, "type"),
            (Property::Name, "name"),
            (Property::Description, "description"),
            (Property::Email, "email"),
            (Property::Timezone, "timezone"),
            (Property::Capabilities, "capabilities"),
            (Property::Aliases, "aliases"),
            (Property::Secret, "secret"),
            (Property::DKIM, "dkim"),
            (Property::Quota, "quota"),
            (Property::Picture, "picture"),
            (Property::Members, "members"),
            (Property::Accounts, "accounts"),
        ] {
            assert_eq!(property.to_string(), wire);
            assert_eq!(serde_json::to_value(property).unwrap(), json!(wire));
            assert_eq!(
                serde_json::from_value::<Property>(json!(wire)).unwrap(),
                property
            );
        }
    }
}

mod principal_acl_vocabulary {
    use super::*;
    use crate::principal::ACL;

    #[test]
    fn acl_serialises_with_the_share_with_property_names() {
        for (acl, wire) in [
            (ACL::Rename, "mayRename"),
            (ACL::Delete, "mayDelete"),
            (ACL::ReadItems, "mayReadItems"),
            (ACL::AddItems, "mayAddItems"),
            (ACL::SetKeywords, "maySetKeywords"),
            (ACL::RemoveItems, "mayRemoveItems"),
            (ACL::CreateChild, "mayCreateChild"),
            (ACL::Administer, "mayShare"),
            (ACL::Submit, "maySubmit"),
            (ACL::SetSeen, "maySetSeen"),
        ] {
            assert_eq!(serde_json::to_value(acl).unwrap(), json!(wire));
            assert_eq!(serde_json::from_value::<ACL>(json!(wire)).unwrap(), acl);
        }
    }

    #[test]
    fn acl_display_matches_the_wire_names() {
        assert_eq!(ACL::ReadItems.to_string(), "mayReadItems");
        assert_eq!(ACL::Administer.to_string(), "mayShare");
        for acl in [
            ACL::Rename,
            ACL::Delete,
            ACL::ReadItems,
            ACL::AddItems,
            ACL::SetKeywords,
            ACL::RemoveItems,
            ACL::CreateChild,
            ACL::Administer,
            ACL::Submit,
            ACL::SetSeen,
        ] {
            let wire = serde_json::to_value(acl).unwrap();
            assert_eq!(
                json!(acl.to_string()),
                wire,
                "Display and Serialize must agree for every ACL variant"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Thin query filter + comparator wire tables (jmap-T2 residual)
// ---------------------------------------------------------------------------
//
// The five small query modules were left untested on the argument that they
// are structurally identical to the pinned `email_query_wire` tables. What
// that argument does not cover is the part where they DIFFER from each other:
// every one of these enums is `#[serde(untagged)]` over single-field structs,
// so a `#[serde(rename)]` typo produces a syntactically valid request that the
// server answers with `unsupportedFilter` (or, worse, silently ignores) rather
// than anything this crate could notice. The property name IS the contract, so
// each is pinned against its RFC here. Comparators go through the shared
// `query::Comparator` envelope, which is why the tag lands flattened alongside
// `isAscending`.

mod thin_query_wire {
    use super::*;

    fn wire(value: &impl serde::Serialize) -> serde_json::Value {
        serde_json::to_value(value).expect("query fragments serialize")
    }

    /// RFC 9670 Section 2.4 (`Principal/query`).
    #[test]
    fn principal_filter_and_comparator_property_names() {
        use crate::principal::Type;
        use crate::principal::query::{Comparator, Filter};

        assert_eq!(
            wire(&Filter::account_ids(["acct-1", "acct-2"])),
            json!({"accountIds": ["acct-1", "acct-2"]})
        );
        assert_eq!(
            wire(&Filter::email("a@example.test")),
            json!({"email": "a@example.test"})
        );
        assert_eq!(wire(&Filter::name("Ops")), json!({"name": "Ops"}));
        assert_eq!(
            wire(&Filter::domain_name("example.test")),
            json!({"domainName": "example.test"})
        );
        assert_eq!(wire(&Filter::text("ops")), json!({"text": "ops"}));
        assert_eq!(
            wire(&Filter::ptype(Type::Group)),
            json!({"type": "group"}),
            "the principal type filter carries the wire token, not the Rust name"
        );
        assert_eq!(
            wire(&Filter::timezone("Europe/Oslo")),
            json!({"timezone": "Europe/Oslo"})
        );
        assert_eq!(wire(&Filter::members("p-1")), json!({"members": "p-1"}));
        assert_eq!(
            wire(&Filter::quota_lower_than(10)),
            json!({"quotaLowerThan": 10})
        );
        assert_eq!(
            wire(&Filter::quota_greater_than(20)),
            json!({"quotaGreaterThan": 20})
        );

        assert_eq!(
            wire(&Comparator::name()),
            json!({"isAscending": true, "property": "name"})
        );
        assert_eq!(
            wire(&Comparator::email()),
            json!({"isAscending": true, "property": "email"})
        );
        assert_eq!(
            wire(&Comparator::ptype().descending()),
            json!({"isAscending": false, "property": "type"})
        );
    }

    /// An untagged enum is decided by the SHAPE of the value, and
    /// `accountIds` is the only array-valued variant - which is why it is
    /// declared first. Pin that the array form still round-trips to the
    /// intended property rather than being claimed by a string variant.
    #[test]
    fn principal_account_ids_filter_survives_untagged_trial_order() {
        use crate::principal::query::Filter;

        let value = wire(&Filter::account_ids(["acct-1"]));
        assert_eq!(
            value.as_object().expect("an object").keys().next(),
            Some(&"accountIds".to_string())
        );
    }

    /// RFC 9670 Section 3.2 (`ShareNotification/query`).
    #[test]
    fn share_notification_filter_and_comparator_property_names() {
        use crate::share_notification::query::{Comparator, Filter};

        assert_eq!(
            wire(&Filter::after("2026-01-02T03:04:05Z")),
            json!({"after": "2026-01-02T03:04:05Z"})
        );
        assert_eq!(
            wire(&Filter::before("2026-01-02T03:04:05Z")),
            json!({"before": "2026-01-02T03:04:05Z"})
        );
        assert_eq!(
            wire(&Filter::object_type("Mailbox")),
            json!({"objectType": "Mailbox"})
        );
        assert_eq!(
            wire(&Filter::object_account_id("acct-9")),
            json!({"objectAccountId": "acct-9"})
        );

        assert_eq!(
            wire(&Comparator::created()),
            json!({"isAscending": true, "property": "created"})
        );
    }

    /// RFC 9404 Section 2.3 (`Quota/query`). The first three filters are
    /// pinned in `query_filter_serialization`; `type` and both comparators
    /// were the gap.
    #[cfg(feature = "quota")]
    #[test]
    fn quota_type_filter_and_comparator_property_names() {
        use crate::quota::query::{Comparator, Filter};

        assert_eq!(wire(&Filter::type_("Mail")), json!({"type": "Mail"}));

        assert_eq!(
            wire(&Comparator::name()),
            json!({"isAscending": true, "property": "name"})
        );
        assert_eq!(
            wire(&Comparator::used().descending()),
            json!({"isAscending": false, "property": "used"})
        );
    }

    /// draft-ietf-jmap-sieve Section 2.3 (`SieveScript/query`).
    #[cfg(feature = "mail")]
    #[test]
    fn sieve_script_filter_and_comparator_property_names() {
        use crate::sieve::query::{Comparator, Filter};

        assert_eq!(wire(&Filter::name("vacation")), json!({"name": "vacation"}));
        assert_eq!(wire(&Filter::is_active(true)), json!({"isActive": true}));

        assert_eq!(
            wire(&Comparator::name()),
            json!({"isAscending": true, "property": "name"})
        );
        assert_eq!(
            wire(&Comparator::is_active().descending()),
            json!({"isAscending": false, "property": "isActive"})
        );
    }

    /// draft-ietf-jmap-calendars (`CalendarEventNotification/query`).
    #[cfg(feature = "calendars")]
    #[test]
    fn calendar_event_notification_filter_and_comparator_property_names() {
        use crate::calendar_event_notification::query::{Comparator, Filter};

        assert_eq!(wire(&Filter::type_("created")), json!({"type": "created"}));
        assert_eq!(
            wire(&Filter::calendar_event_id("ev-1")),
            json!({"calendarEventId": "ev-1"})
        );

        assert_eq!(
            wire(&Comparator::created()),
            json!({"isAscending": true, "property": "created"})
        );
    }

    /// `Principal/getAvailability` (RFC 9670 Section 4.1): the request is a
    /// plain method object, so the only thing worth pinning is that every
    /// field carries its camelCase wire name and that `showDetails` is
    /// omitted rather than sent as null when unset.
    #[test]
    fn principal_get_availability_request_wire_shape() {
        use crate::principal::availability::PrincipalGetAvailabilityRequest;

        let bare = PrincipalGetAvailabilityRequest::new(
            "p-1",
            "2026-01-02T00:00:00Z",
            "2026-01-03T00:00:00Z",
        );
        assert_eq!(
            wire(&bare),
            json!({
                "accountId": "",
                "id": "p-1",
                "utcStart": "2026-01-02T00:00:00Z",
                "utcEnd": "2026-01-03T00:00:00Z",
            }),
            "showDetails must be absent, not null, when the caller did not set it"
        );

        let detailed = PrincipalGetAvailabilityRequest::new(
            "p-1",
            "2026-01-02T00:00:00Z",
            "2026-01-03T00:00:00Z",
        )
        .show_details(true);
        assert_eq!(
            wire(&detailed).get("showDetails"),
            Some(&json!(true)),
            "showDetails rides the request once set"
        );
    }
}
