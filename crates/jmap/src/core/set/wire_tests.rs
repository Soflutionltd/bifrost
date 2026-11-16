use crate::core::SetCreate;
use crate::core::test_support::*;
use serde_json::json;

#[test]
fn set_response_deserializes() {
    use crate::core::set::SetResponse;

    let raw = json!({
        "accountId": "A1",
        "oldState": "s1",
        "newState": "s2",
        "created": {
            "c0": {"id": "new-1", "name": "created-obj"}
        },
        "updated": null,
        "destroyed": null,
        "notCreated": null,
        "notUpdated": null,
        "notDestroyed": null
    });

    let mut response: SetResponse<TestObj> = serde_json::from_value(raw).unwrap();
    assert_eq!(
        response
            .new_state()
            .map(crate::core::id::ObjectState::as_str),
        Some("s2")
    );
    let created = response.created("c0").unwrap();
    assert_eq!(created.name.as_deref(), Some("created-obj"));
}

/// RFC 8620 s5.3 makes `newState` mandatory, so a response without one is
/// a non-conforming server - and it must stay distinguishable from a
/// server that reported an empty state. The accessor used to fold both
/// into `""`, which handed the sync layer's state cache (where an
/// explicitly empty entry has its own meaning) a value no server sent.
#[test]
fn an_absent_set_new_state_is_not_an_empty_one() {
    use crate::core::set::SetResponse;

    let absent: SetResponse<TestObj> = serde_json::from_value(json!({
        "accountId": "A1",
        "oldState": "s1"
    }))
    .unwrap();
    assert_eq!(absent.new_state(), None);
    assert_eq!(absent.into_new_state(), None);

    let empty: SetResponse<TestObj> = serde_json::from_value(json!({
        "accountId": "A1",
        "oldState": "s1",
        "newState": ""
    }))
    .unwrap();
    assert_eq!(
        empty.new_state().map(crate::core::id::ObjectState::as_str),
        Some("")
    );
    assert_eq!(empty.into_new_state(), Some(String::new().into()));
}

#[test]
fn create_payload_identity_follows_its_owning_map() {
    let mut set = crate::core::set::SetRequest::<TestObj>::new();
    assert_eq!(
        set.create_with_id("chosen").create_id().as_deref(),
        Some("chosen")
    );
    let detached = TestObjCreate::new(Some(crate::core::creation::CreationId::named("old")));
    let inserted = set.create_item(detached);
    assert_eq!(
        set.create_with_id(inserted.clone()).create_id().as_deref(),
        Some(inserted.as_str())
    );
    assert_eq!(
        set.create_with_id("chosen").create_id().as_deref(),
        Some("chosen")
    );
}

#[test]
fn automatic_creation_ids_skip_explicit_collisions() {
    let mut set = crate::core::set::SetRequest::<TestObj>::new();
    set.create_with_id("c1");
    assert_eq!(set.create().create_id().as_deref(), Some("c2"));
    let wire = serde_json::to_value(set).unwrap();
    assert_eq!(wire["create"].as_object().unwrap().len(), 2);
    assert!(wire["create"].get("c1").is_some());
    assert!(wire["create"].get("c2").is_some());
}

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
