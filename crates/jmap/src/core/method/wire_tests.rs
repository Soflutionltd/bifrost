mod specialized_method_contracts {
    use crate::core::capability::Capability;
    use crate::core::method::JmapMethod;

    const CORE: &str = "urn:ietf:params:jmap:core";
    #[cfg(feature = "mail")]
    const MAIL: &str = "urn:ietf:params:jmap:mail";
    #[cfg(feature = "mail")]
    const SIEVE: &str = "urn:ietf:params:jmap:sieve";
    #[cfg(feature = "blob")]
    const BLOB: &str = "urn:ietf:params:jmap:blob";

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
        // RFC 8620 s6.3: Blob/copy is a core method, not a blob-extension
        // one.
        check::<crate::blob::copy::CopyBlobRequest>("Blob/copy", CORE);
    }

    #[cfg(feature = "mail")]
    #[test]
    fn mail_methods() {
        check::<crate::email::import::EmailImportRequest>("Email/import", MAIL);
        check::<crate::email::parse::EmailParseRequest>("Email/parse", MAIL);
        check::<crate::email::search_snippet::SearchSnippetGetRequest>("SearchSnippet/get", MAIL);
    }

    #[cfg(feature = "mail")]
    #[test]
    fn sieve_validation_method() {
        check::<crate::sieve::validate::SieveScriptValidateRequest>("SieveScript/validate", SIEVE);
    }

    #[cfg(feature = "blob")]
    #[test]
    fn blob_methods() {
        check::<crate::blob::manage::BlobUploadRequest>("Blob/upload", BLOB);
        check::<crate::blob::manage::BlobGetRequest>("Blob/get", BLOB);
        check::<crate::blob::manage::BlobLookupRequest>("Blob/lookup", BLOB);
    }

    #[cfg(feature = "calendars")]
    #[test]
    fn calendar_methods() {
        // The `parse` sub-capability is separate from `calendars`.
        check::<crate::calendar_event::parse::CalendarEventParseRequest>(
            "CalendarEvent/parse",
            "urn:ietf:params:jmap:calendars:parse",
        );
    }

    #[cfg(feature = "contacts")]
    #[test]
    fn contact_methods() {
        check::<crate::contact_card::parse::ContactCardParseRequest>(
            "ContactCard/parse",
            "urn:ietf:params:jmap:contacts:parse",
        );
    }
}

mod property_enum_round_trip {
    use serde_json::json;

    crate::core::method::define_property_enum! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        #[non_exhaustive]
        enum ClosedProperty {
            First = 7 => "wireFirst",
            Second => "wireSecond",
        }
    }

    #[test]
    fn closed_property_macro_wire_contract() {
        for (property, wire) in [
            (ClosedProperty::First, "wireFirst"),
            (ClosedProperty::Second, "wireSecond"),
        ] {
            assert_eq!(property.to_string(), wire);
            assert_eq!(serde_json::to_value(property).unwrap(), json!(wire));
            assert_eq!(
                serde_json::from_value::<ClosedProperty>(json!(wire)).unwrap(),
                property
            );
        }
        assert_eq!(ClosedProperty::First as u8, 7);
        assert_eq!(ClosedProperty::Second as u8, 8);
        assert!(serde_json::from_value::<ClosedProperty>(json!("vendor:unknown")).is_err());
        assert!(serde_json::from_value::<ClosedProperty>(json!(7)).is_err());
    }

    #[cfg(any(feature = "calendars", feature = "contacts"))]
    crate::define_open_property_enum! {
        #[non_exhaustive]
        enum OpenProperty {
            First => "wireFirst",
            Second => "wireSecond",
        }
    }

    #[test]
    #[cfg(any(feature = "calendars", feature = "contacts"))]
    fn open_property_macro_wire_contract() {
        for (property, wire) in [
            (OpenProperty::First, "wireFirst"),
            (OpenProperty::Second, "wireSecond"),
            (
                OpenProperty::Other("vendor:unknown".to_string()),
                "vendor:unknown",
            ),
        ] {
            assert_eq!(property.as_str(), wire);
            assert_eq!(property.to_string(), wire);
            assert_eq!(OpenProperty::from(wire), property);
            assert_eq!(serde_json::to_value(&property).unwrap(), json!(wire));
            assert_eq!(
                serde_json::from_value::<OpenProperty>(json!(wire)).unwrap(),
                property
            );
        }
        assert!(serde_json::from_value::<OpenProperty>(json!(7)).is_err());
    }
}
