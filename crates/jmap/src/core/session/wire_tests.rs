mod session_capabilities_deserialization {
    use crate::core::session::{Capabilities, Session};
    use serde_json::json;

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
        // RFC 9404's Blob limits live on the account, like the others here;
        // the session object is only the empty support marker.
        for uri in [
            "urn:ietf:params:jmap:mail",
            "urn:ietf:params:jmap:blob",
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
            assert_eq!(mail.max_mailbox_depth(), Some(10));
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

mod principals_owner_capability {
    use serde_json::json;

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

mod session_capability_fallbacks {
    use crate::core::session::{Capabilities, Session};
    use serde_json::json;

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
        assert!(!core.has_collation_algorithms());
    }

    #[test]
    fn core_collation_algorithms_preserves_present_empty_array() {
        let session = session_with(json!({
            "urn:ietf:params:jmap:core": {"collationAlgorithms": []}
        }));
        let core = session.core_capabilities().expect("core cap");
        assert!(core.has_collation_algorithms());
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

    /// Upload calls from separate account operations can overlap, so the
    /// session's upload endpoint bound must survive decode and be read by the
    /// transport setup. Missing and zero values choose the serial minimum.
    #[test]
    fn core_max_concurrent_upload_is_read_with_a_serial_minimum() {
        let session = session_with(json!({
            "urn:ietf:params:jmap:core": {"maxConcurrentUpload": 4}
        }));
        let core = session.core_capabilities().expect("core cap");
        assert_eq!(core.max_concurrent_upload(), Some(4));
        assert_eq!(session.upload_concurrency(), 4);

        let omitted = session_with(json!({"urn:ietf:params:jmap:core": {}}));
        assert_eq!(
            omitted
                .core_capabilities()
                .expect("core cap")
                .max_concurrent_upload(),
            None,
            "omitted must stay distinguishable from an advertised zero"
        );
        assert_eq!(omitted.upload_concurrency(), 1);

        let zero = session_with(json!({
            "urn:ietf:params:jmap:core": {"maxConcurrentUpload": 0}
        }));
        assert_eq!(zero.upload_concurrency(), 1);
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
