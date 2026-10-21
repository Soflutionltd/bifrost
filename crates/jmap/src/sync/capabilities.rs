use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation,
    BatchingPolicy, BlobRangeSupport, Cause, ConvenienceShape, CursorFreshness, DiagnosticText,
    FilterRuleShape, MutationCapabilities, MutationConcurrency, MutationReplaySafety,
    PimMethodSupport, Protocol, ProtocolErrorKind, PushCapability, QuotaSignal, RateLimitClass,
    StarredFlagShape, StateCause, SyncStateErrorKind, WireCause,
};

/// Session document does not advertise the `urn:ietf:params:jmap:core`
/// capability at all. This is a server-side capability change relative
/// to whatever the session previously claimed: the engine must reopen.
fn missing_core_capability() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
        Cause::State(StateCause::CapabilityChanged { delta: None }),
    )
    .protocol(Protocol::Jmap)
    .operation(AccountOperation::Discover)
    .text(DiagnosticText::support_only(
        "JMAP session does not advertise the core capability",
    ))
    .try_build()
    .expect("valid account error classification")
}

/// A method response named a different session state. Account construction
/// derives routing, limits, and capabilities from the old session, so the
/// only safe consumer action is to reopen against a freshly fetched session.
pub(crate) fn session_state_changed() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
        Cause::State(StateCause::CapabilityChanged { delta: None }),
    )
    .protocol(Protocol::Jmap)
    .operation(AccountOperation::ScopeLifecycle)
    .text(DiagnosticText::support_only(
        "JMAP response sessionState diverged from the open session",
    ))
    .try_build()
    .expect("valid account error classification")
}

/// Server advertises the core capability but omits a required field or gives
/// zero for one of the request limits this account requires to be positive.
/// That is a `Protocol(ContractViolation)`: the server claimed conformance
/// and then withheld required information or advertised an unusable limit.
/// Zero is valid for `maxSizeUpload` and the concurrency limits because they
/// are `UnsignedInt`; an omitted `maxSizeUpload` takes the same error lane.
/// It is not a capability change.
fn unusable_core_limits() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(
                "JMAP core capability omits required fields or advertises unusable limits",
            )),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(AccountOperation::Discover)
    .try_build()
    .expect("valid account error classification")
}

/// The `urn:ietf:params:jmap:core` block is PRESENT but does not parse as
/// the RFC 8620 §2 core object (`"maxCallsInRequest": "16"` as a string is
/// the canonical shape). The server advertised the capability and then
/// described it wrongly, so this is the same `Protocol(ContractViolation)`
/// lane as a zero-valued limit, and deliberately NOT the
/// `SyncState(CapabilityChanged)` lane an absent block takes: nothing about
/// this session changes on a reopen, so classifying it as a capability
/// change would buy an endless reopen loop.
fn malformed_core_capability() -> AccountError {
    malformed_required_capability(
        <crate::core::capability::Core as crate::core::capability::Capability>::URI,
    )
}

/// A capability block the account DEPENDS on is present but does not
/// parse. Same lane and same reasoning as the core block: the server
/// advertised the capability and then described it wrongly, so a reopen
/// buys nothing and the honest classification is a contract violation.
/// The URI is named in the diagnostic - "some capability is malformed" is
/// not an actionable report when a session advertises a dozen.
fn malformed_required_capability(uri: &str) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(format!(
                "JMAP capability object {uri} is present but not parseable"
            ))),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(AccountOperation::Discover)
    .try_build()
    .expect("valid account error classification")
}

use crate::core::session::Session;

/// RFC 8620 section 2 requires maxSizeUpload. Zero permits only empty files;
/// the suggested minimum is not a mandatory lower bound.
pub(crate) fn upload_size_limit(session: &Session) -> Result<usize, AccountError> {
    match session.core_capability_state() {
        crate::core::session::CoreCapabilityState::Absent => Err(missing_core_capability()),
        crate::core::session::CoreCapabilityState::Malformed => Err(malformed_core_capability()),
        crate::core::session::CoreCapabilityState::Present(core) => {
            core.max_size_upload().ok_or_else(unusable_core_limits)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoreLimits {
    pub(crate) max_objects_in_get: usize,
    pub(crate) max_objects_in_set: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PimSupport {
    pub(crate) submission: bool,
    /// `urn:ietf:params:jmap:submission` `maxDelayedSend` (seconds).
    /// `> 0` means the server accepts FUTURERELEASE hold parameters and
    /// gates `scheduled_send`.
    pub(crate) max_delayed_send: usize,
    /// At least one successfully seeded foreign mail account also advertises
    /// JMAP Submission, so native send-as routing is reachable.
    pub(crate) foreign_submission: bool,
    pub(crate) vacation: bool,
    pub(crate) quota: bool,
    pub(crate) sieve: bool,
    pub(crate) contacts: bool,
    pub(crate) calendar: bool,
}

/// Everything `build` refuses an open for that is decided by the SESSION
/// alone - the core block's lane, its mandatory limits, and the required
/// `urn:ietf:params:jmap:mail` block's lane.
///
/// Split out of `build` so the open sequence can run it BEFORE the state
/// seeding and the foreign-account probe fan-out. Those probes are real
/// requests against a server we have already decided is non-conformant,
/// one per share, and their only possible outcome on this path is being
/// thrown away with the refusal. Validation that genuinely needs probe
/// results (the `PimSupport` gates, which depend on which shares seeded)
/// stays in `build`.
///
/// `build` calls this too, so the refusal does not depend on the caller
/// having run it first; running it twice is pure session reads.
pub(crate) fn validate_session(session: &Session) -> Result<CoreLimits, AccountError> {
    // Three lanes, not two. An ABSENT core block is a capability change
    // (the server dropped a capability the session previously claimed):
    // reopen. A block that is present but unparseable, or present with a
    // zero or missing mandatory limit, is a contract violation: the server
    // is answering `urn:ietf:params:jmap:core` with something that is not
    // one. Collapsing malformed into absent would send the engine into a
    // reopen loop against a server that will keep sending the same bad
    // session forever.
    let core = match session.core_capability_state() {
        crate::core::session::CoreCapabilityState::Absent => {
            return Err(missing_core_capability());
        }
        crate::core::session::CoreCapabilityState::Malformed => {
            return Err(malformed_core_capability());
        }
        crate::core::session::CoreCapabilityState::Present(core) => core,
    };

    upload_size_limit(session)?;
    // RFC 8620 §2 requires both concurrency fields. Their type is
    // `UnsignedInt`, so zero is a valid advertised value; only absence is
    // rejected here.
    if core.max_concurrent_upload().is_none() || core.max_concurrent_requests().is_none() {
        return Err(unusable_core_limits());
    }

    let usable = |limit: Option<usize>| limit.is_some_and(|value| value > 0);
    if !usable(core.max_calls_in_request())
        || !usable(core.max_objects_in_get())
        || !usable(core.max_objects_in_set())
        || !usable(core.max_size_request())
    {
        return Err(unusable_core_limits());
    }
    // Every one of the four is `Some(> 0)` past this point.
    let max_objects_in_get = core.max_objects_in_get().unwrap_or(1).max(1);
    let max_objects_in_set = core.max_objects_in_set().unwrap_or(1).max(1);

    // The same Absent / Malformed / Present split, applied to every other
    // capability this crate models. A block the account DEPENDS on -
    // `urn:ietf:params:jmap:mail`, which is the reason a JMAP account
    // exists here - is refused at open, in the contract-violation lane and
    // naming the URI. Every other modelled block is optional: the account
    // degrades to "that family is off", but it says so, because a silent
    // downgrade is indistinguishable from a server that never offered the
    // feature and leaves nobody anything to debug.
    #[cfg(feature = "mail")]
    {
        let mail_uri = <crate::core::capability::Mail as crate::core::capability::Capability>::URI;
        if session.malformed_capabilities().any(|uri| uri == mail_uri) {
            return Err(malformed_required_capability(mail_uri));
        }
    }

    Ok(CoreLimits {
        max_objects_in_get,
        max_objects_in_set,
    })
}

pub(crate) fn build(
    session: &Session,
    support: PimSupport,
) -> Result<(AccountCapabilities, CoreLimits), AccountError> {
    let CoreLimits {
        max_objects_in_get,
        max_objects_in_set,
    } = validate_session(session)?;

    for uri in session.malformed_capabilities() {
        tracing::warn!(
            capability = uri,
            "JMAP session advertises this capability with an unparseable object; \
             the features behind it are treated as unavailable"
        );
    }

    // Push specifically: a malformed websocket block used to derive
    // `PushCapability::None` with nothing said, which reads to the engine
    // as a server that does not do push at all. It still degrades to
    // polling - push is optional, and there is no url to connect to - but
    // the reason is on the record.
    let ws_push = match session.websocket_capability_state() {
        crate::core::session::CapabilityState::Present(ws) => ws.supports_push(),
        crate::core::session::CapabilityState::Absent => false,
        crate::core::session::CapabilityState::Malformed => {
            tracing::warn!(
                capability =
                    <crate::core::capability::WebSocket as crate::core::capability::Capability>::URI,
                "JMAP websocket capability object is present but not parseable; \
                 push is disabled and the account falls back to polling"
            );
            false
        }
    };

    let max_items = max_objects_in_set.clamp(1, 500);
    let caps = AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        // The existing JMAP transport exposes whole-blob downloads only.
        // Range support needs a request hook for the Range header.
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: if ws_push {
            PushCapability::InProcess
        } else {
            PushCapability::None
        },
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::ExpectedVersion {
                mail_set_flags: true,
                mail_move: true,
                mail_destroy: true,
                contact_update: false,
                event_update: false,
                contact_delete: false,
                event_delete: false,
                event_rsvp: false,
            },
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items,
            max_wait: Duration::from_millis(100),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Generous,
        quota_signal: QuotaSignal::None,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport {
            category_definitions: false,
            message_reactions: false,
            add_to_container: true,
            remove_from_container: true,
            set_keyword: true,
            set_label_membership: false,
            set_category: false,
            set_extended_property: false,
            set_importance: true,
            set_is_read: true,
            send_message: support.submission,
            attachment_upload: true,
            host_attachment: false,
            draft_create: true,
            draft_update: true,
            draft_discard: true,
            draft_send: support.submission,
            scheduled_send: support.submission && support.max_delayed_send > 0,
            send_as: support.foreign_submission,
            search: true,
            search_messages: true,
            containers_list: true,
            container_create: true,
            container_rename: true,
            container_move: true,
            container_delete: true,
            identities_list: support.submission,
            identity_update: support.submission,
            vacation_get: support.vacation,
            vacation_set: support.vacation,
            quota_get: support.quota,
            thread_hydrate: true,
            message_hydrate: true,
            open_raw_rfc822: true,
            filters_list: support.sieve,
            filter_create: support.sieve,
            filter_update: support.sieve,
            filter_delete: support.sieve,
            filter_validate: support.sieve,
            address_books_list: support.contacts,
            contacts_list: support.contacts,
            contact_get: support.contacts,
            contact_create: support.contacts,
            contact_update: support.contacts,
            contact_delete: support.contacts,
            contact_search: support.contacts,
            contact_autocomplete: support.contacts,
            directory_search: false,
            directory_enumerate: false,
            directory_groups_list: false,
            directory_group_expand: false,
            calendars_list: support.calendar,
            events_in_range: support.calendar,
            event_get: support.calendar,
            event_create: support.calendar,
            event_update: support.calendar,
            event_delete: support.calendar,
            event_rsvp: support.calendar,
            event_search: support.calendar,
            event_autocomplete: support.calendar,
        },
        filter_rule_shape: if support.sieve {
            FilterRuleShape::Scripts
        } else {
            FilterRuleShape::None
        },
        conveniences: ConvenienceShape {
            starred: StarredFlagShape::Keyword,
            replied_via_keyword: true,
            replied_via_extended_property: false,
            forwarded_via_keyword: true,
            forwarded_via_extended_property: false,
            mdn_sent_via_keyword: true,
        },
        // JMAP seeds foreign account scopes from the session during open
        // and emits no foreign scope lifecycle events, so a reopen is the
        // only way to discover a newly granted share. Unconditional: no
        // session signal can prove a server will never grant one (the
        // accounts list is only the current grants, and RFC 9670
        // principals support is sufficient but not necessary evidence),
        // and a false here tells the engine to never look.
        discovers_foreign_namespaces_on_rediscovery: true,
    };

    let limits = CoreLimits {
        max_objects_in_get,
        max_objects_in_set,
    };

    Ok((caps, limits))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(json: &str) -> Session {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn capability_builder_uses_core_limits_and_ws_push() {
        let session = session(
            r#"{
                "capabilities": {
                    "urn:ietf:params:jmap:core": {
                        "maxSizeUpload": 1000,
                        "maxConcurrentUpload": 2,
                        "maxSizeRequest": 100000,
                        "maxConcurrentRequests": 4,
                        "maxCallsInRequest": 8,
                        "maxObjectsInGet": 256,
                        "maxObjectsInSet": 700,
                        "collationAlgorithms": []
                    },
                    "urn:ietf:params:jmap:websocket": {
                        "url": "wss://example.test/jmap/ws",
                        "supportsPush": true
                    },
                    "urn:ietf:params:jmap:mail": {}
                },
                "accounts": {},
                "primaryAccounts": {},
                "username": "user",
                "apiUrl": "https://example.test/jmap/api",
                "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
                "uploadUrl": "https://example.test/upload/{accountId}",
                "eventSourceUrl": "https://example.test/eventsource",
                "state": "session-state"
            }"#,
        );

        let (caps, limits) = build(
            &session,
            PimSupport {
                submission: true,
                max_delayed_send: 0,
                foreign_submission: false,
                vacation: true,
                quota: true,
                sieve: true,
                contacts: true,
                calendar: false,
            },
        )
        .unwrap();
        assert_eq!(caps.cursor_freshness, CursorFreshness::ServerIssued);
        assert_eq!(caps.push, PushCapability::InProcess);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert_eq!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                mail_set_flags: true,
                mail_move: true,
                mail_destroy: true,
                contact_update: false,
                event_update: false,
                contact_delete: false,
                event_delete: false,
                event_rsvp: false,
            }
        );
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert_eq!(caps.batching_policy.max_items, 500);
        assert!(caps.pim_methods.add_to_container);
        assert!(caps.pim_methods.send_message);
        assert!(caps.pim_methods.open_raw_rfc822);
        assert!(!caps.pim_methods.set_label_membership);
        assert_eq!(caps.filter_rule_shape, FilterRuleShape::Scripts);
        assert!(caps.pim_methods.filters_list);
        assert!(caps.pim_methods.filter_validate);
        assert_eq!(caps.conveniences.starred, StarredFlagShape::Keyword);
        assert!(caps.conveniences.replied_via_keyword);
        assert!(caps.conveniences.forwarded_via_keyword);
        assert!(caps.conveniences.mdn_sent_via_keyword);
        assert!(caps.pim_methods.set_importance);
        assert!(caps.discovers_foreign_namespaces_on_rediscovery);
        assert_eq!(limits.max_objects_in_get, 256);
        assert_eq!(limits.max_objects_in_set, 700);
    }

    /// "Core limits zero" is a `Protocol(ContractViolation)`,
    /// not a `SyncState(CapabilityChanged)`. The server claimed
    /// conformance to the core capability and then advertised
    /// zero-valued limits, which the spec prohibits.
    #[test]
    fn zero_core_limit_classifies_as_contract_violation() {
        let session = session(
            r#"{
                "capabilities": {
                    "urn:ietf:params:jmap:core": {
                        "maxSizeUpload": 1000,
                        "maxConcurrentUpload": 2,
                        "maxSizeRequest": 100000,
                        "maxConcurrentRequests": 4,
                        "maxCallsInRequest": 0,
                        "maxObjectsInGet": 256,
                        "maxObjectsInSet": 700,
                        "collationAlgorithms": []
                    },
                    "urn:ietf:params:jmap:mail": {}
                },
                "accounts": {},
                "primaryAccounts": {},
                "username": "user",
                "apiUrl": "https://example.test/jmap/api",
                "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
                "uploadUrl": "https://example.test/upload/{accountId}",
                "eventSourceUrl": "https://example.test/eventsource",
                "state": "session-state"
            }"#,
        );

        let err = build(
            &session,
            PimSupport {
                submission: false,
                max_delayed_send: 0,
                foreign_submission: false,
                vacation: false,
                quota: false,
                sieve: false,
                contacts: false,
                calendar: false,
            },
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        );
        assert!(err.recovery().is_terminal());
    }

    /// A session document with a configurable `maxObjectsInSet` and an
    /// optional WebSocket capability block. The other core limits are
    /// held at legal non-zero values so each test varies exactly one
    /// input.
    fn session_with(max_objects_in_set: usize, websocket: bool) -> Session {
        let ws = if websocket {
            r#""urn:ietf:params:jmap:websocket": {"url": "wss://example.test/jmap/ws", "supportsPush": true},"#
        } else {
            ""
        };
        session(&format!(
            r#"{{
                "capabilities": {{
                    "urn:ietf:params:jmap:core": {{
                        "maxSizeUpload": 1000,
                        "maxConcurrentUpload": 2,
                        "maxSizeRequest": 100000,
                        "maxConcurrentRequests": 4,
                        "maxCallsInRequest": 8,
                        "maxObjectsInGet": 256,
                        "maxObjectsInSet": {max_objects_in_set},
                        "collationAlgorithms": []
                    }},
                    {ws}
                    "urn:ietf:params:jmap:mail": {{}}
                }},
                "accounts": {{}},
                "primaryAccounts": {{}},
                "username": "user",
                "apiUrl": "https://example.test/jmap/api",
                "downloadUrl": "https://example.test/download/{{accountId}}/{{blobId}}/{{name}}/{{type}}",
                "uploadUrl": "https://example.test/upload/{{accountId}}",
                "eventSourceUrl": "https://example.test/eventsource",
                "state": "session-state"
            }}"#
        ))
    }

    /// A server that advertises none of the optional PIM families.
    fn no_pim_support() -> PimSupport {
        PimSupport {
            submission: false,
            max_delayed_send: 0,
            foreign_submission: false,
            vacation: false,
            quota: false,
            sieve: false,
            contacts: false,
            calendar: false,
        }
    }

    /// Without `urn:ietf:params:jmap:websocket` (or with a block that
    /// does not support push) there is no in-process push channel, and
    /// the engine must be told so rather than subscribing into a
    /// transport that cannot deliver.
    #[test]
    fn a_session_without_websocket_push_advertises_no_push() {
        let (caps, _) = build(&session_with(256, false), no_pim_support()).unwrap();
        assert_eq!(caps.push, PushCapability::None);

        let (with_ws, _) = build(&session_with(256, true), no_pim_support()).unwrap();
        assert_eq!(with_ws.push, PushCapability::InProcess);
    }

    /// Every optional-family flag tracks its `PimSupport` input, and the
    /// unconditional flags stay on. This is the capability-shape half of
    /// the account contract: the engine calls exactly what is advertised,
    /// so a flag that turns on without its backing account handle is a
    /// guaranteed `Unsupported` at runtime.
    #[test]
    fn optional_pim_families_are_gated_by_their_session_support() {
        let (off, _) = build(&session_with(256, false), no_pim_support()).unwrap();
        let pim = &off.pim_methods;
        for (name, flag) in [
            ("send_message", pim.send_message),
            ("draft_send", pim.draft_send),
            ("scheduled_send", pim.scheduled_send),
            ("send_as", pim.send_as),
            ("identities_list", pim.identities_list),
            ("identity_update", pim.identity_update),
            ("vacation_get", pim.vacation_get),
            ("vacation_set", pim.vacation_set),
            ("quota_get", pim.quota_get),
            ("filters_list", pim.filters_list),
            ("filter_create", pim.filter_create),
            ("filter_update", pim.filter_update),
            ("filter_delete", pim.filter_delete),
            ("filter_validate", pim.filter_validate),
            ("address_books_list", pim.address_books_list),
            ("contacts_list", pim.contacts_list),
            ("contact_get", pim.contact_get),
            ("contact_create", pim.contact_create),
            ("contact_update", pim.contact_update),
            ("contact_delete", pim.contact_delete),
            ("contact_search", pim.contact_search),
            ("contact_autocomplete", pim.contact_autocomplete),
            ("calendars_list", pim.calendars_list),
            ("events_in_range", pim.events_in_range),
            ("event_get", pim.event_get),
            ("event_create", pim.event_create),
            ("event_update", pim.event_update),
            ("event_delete", pim.event_delete),
            ("event_rsvp", pim.event_rsvp),
            ("event_search", pim.event_search),
            ("event_autocomplete", pim.event_autocomplete),
        ] {
            assert!(!flag, "{name} must stay off without its session support");
        }
        assert_eq!(off.filter_rule_shape, FilterRuleShape::None);

        // Directory and the Graph/Gmail-shaped conveniences have no JMAP
        // backing at all: they are false regardless of session support.
        for (name, flag) in [
            ("directory_search", pim.directory_search),
            ("directory_groups_list", pim.directory_groups_list),
            ("directory_group_expand", pim.directory_group_expand),
            ("set_label_membership", pim.set_label_membership),
            ("set_category", pim.set_category),
            ("set_extended_property", pim.set_extended_property),
            ("category_definitions", pim.category_definitions),
            ("message_reactions", pim.message_reactions),
            ("host_attachment", pim.host_attachment),
        ] {
            assert!(!flag, "{name} has no JMAP implementation");
        }

        // The mail-core doors do not depend on any optional capability.
        for (name, flag) in [
            ("add_to_container", pim.add_to_container),
            ("remove_from_container", pim.remove_from_container),
            ("set_keyword", pim.set_keyword),
            ("set_is_read", pim.set_is_read),
            ("set_importance", pim.set_importance),
            ("attachment_upload", pim.attachment_upload),
            ("draft_create", pim.draft_create),
            ("draft_update", pim.draft_update),
            ("draft_discard", pim.draft_discard),
            ("search", pim.search),
            ("search_messages", pim.search_messages),
            ("containers_list", pim.containers_list),
            ("container_create", pim.container_create),
            ("container_rename", pim.container_rename),
            ("container_move", pim.container_move),
            ("container_delete", pim.container_delete),
            ("thread_hydrate", pim.thread_hydrate),
            ("message_hydrate", pim.message_hydrate),
            ("open_raw_rfc822", pim.open_raw_rfc822),
        ] {
            assert!(flag, "{name} is unconditional in JMAP");
        }

        let mut all = no_pim_support();
        all.submission = true;
        all.max_delayed_send = 3600;
        all.foreign_submission = true;
        all.vacation = true;
        all.quota = true;
        all.sieve = true;
        all.contacts = true;
        all.calendar = true;
        let (on, _) = build(&session_with(256, false), all).unwrap();
        assert!(on.pim_methods.send_message);
        assert!(on.pim_methods.draft_send);
        assert!(on.pim_methods.scheduled_send);
        assert!(on.pim_methods.send_as);
        assert!(on.pim_methods.vacation_set);
        assert!(on.pim_methods.quota_get);
        assert!(on.pim_methods.filter_validate);
        assert!(on.pim_methods.contact_autocomplete);
        assert!(on.pim_methods.event_rsvp);
        assert_eq!(on.filter_rule_shape, FilterRuleShape::Scripts);
    }

    /// The batch window is the server's `maxObjectsInSet`, clamped into
    /// `1..=500`. A server that advertises a tiny limit must not have the
    /// engine batch past it, and one that advertises a huge limit must
    /// not produce an unbounded in-memory batch.
    #[test]
    fn batching_window_clamps_max_objects_in_set() {
        for (advertised, expected) in [(1usize, 1usize), (10, 10), (500, 500), (5000, 500)] {
            let (caps, limits) = build(&session_with(advertised, false), no_pim_support()).unwrap();
            assert_eq!(
                caps.batching_policy.max_items, expected,
                "maxObjectsInSet {advertised}"
            );
            // The raw limit is passed through unclamped: it sizes the
            // per-request `/set` call, not the engine's batch window.
            assert_eq!(limits.max_objects_in_set, advertised);
        }
    }

    /// A session that does not advertise the core capability at all is a
    /// capability shift relative to whatever the last open saw, so it
    /// must derive a full account reopen - not a terminal failure and not
    /// a scope-level restart.
    #[test]
    fn a_session_without_the_core_capability_restarts_the_account() {
        let session = session(
            r#"{
                "capabilities": {"urn:ietf:params:jmap:mail": {}},
                "accounts": {},
                "primaryAccounts": {},
                "username": "user",
                "apiUrl": "https://example.test/jmap/api",
                "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
                "uploadUrl": "https://example.test/upload/{accountId}",
                "eventSourceUrl": "https://example.test/eventsource",
                "state": "session-state"
            }"#,
        );

        let err = build(&session, no_pim_support()).unwrap_err();
        assert_eq!(
            err.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged)
        );
        assert_eq!(
            err.recovery(),
            &bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::RestartAccount)
        );
        assert_eq!(err.protocol(), Some(Protocol::Jmap));
        assert_eq!(err.operation(), Some(AccountOperation::Discover));
    }

    /// Each of the four positive request limits is independently
    /// load-bearing: a zero in any one of them is a contract violation, not
    /// just in `maxCallsInRequest`.
    #[test]
    fn any_zero_core_limit_is_a_contract_violation() {
        for field in [
            "maxCallsInRequest",
            "maxObjectsInGet",
            "maxObjectsInSet",
            "maxSizeRequest",
        ] {
            let mut core = serde_json::json!({
                "maxSizeUpload": 1000,
                "maxConcurrentUpload": 2,
                "maxSizeRequest": 100_000,
                "maxConcurrentRequests": 4,
                "maxCallsInRequest": 8,
                "maxObjectsInGet": 256,
                "maxObjectsInSet": 256,
                "collationAlgorithms": []
            });
            core[field] = serde_json::json!(0);
            let doc = serde_json::json!({
                "capabilities": {
                    "urn:ietf:params:jmap:core": core,
                    "urn:ietf:params:jmap:mail": {}
                },
                "accounts": {},
                "primaryAccounts": {},
                "username": "user",
                "apiUrl": "https://example.test/jmap/api",
                "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
                "uploadUrl": "https://example.test/upload/{accountId}",
                "eventSourceUrl": "https://example.test/eventsource",
                "state": "session-state"
            });
            let session: Session = serde_json::from_value(doc).unwrap();
            let err = build(&session, no_pim_support())
                .err()
                .unwrap_or_else(|| panic!("zero {field} must be refused"));
            assert_eq!(
                err.kind(),
                &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                "zero {field}"
            );
        }
    }

    /// A session document carrying an arbitrary core capability block.
    fn session_with_core(core: serde_json::Value) -> Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": core,
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {},
            "primaryAccounts": {},
            "username": "user",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-state"
        }))
        .expect("session fixture parses")
    }

    /// The defect: a PRESENT core block that does not parse used to fall
    /// back to `Capabilities::Other`, which is indistinguishable from
    /// absent, so a server sending `"maxCallsInRequest": "16"` was
    /// classified `SyncState(CapabilityChanged)` and the engine reopened
    /// forever against a session that will never change. It is a contract
    /// violation: the server advertised the capability and then described
    /// it wrongly.
    #[test]
    fn a_present_but_malformed_core_block_is_a_contract_violation() {
        let session = session_with_core(serde_json::json!({
            "maxSizeUpload": 1000,
            "maxConcurrentUpload": 2,
            "maxSizeRequest": 100_000,
            "maxConcurrentRequests": 4,
            "maxCallsInRequest": "16",
            "maxObjectsInGet": 256,
            "maxObjectsInSet": 256,
            "collationAlgorithms": []
        }));

        let err =
            build(&session, no_pim_support()).expect_err("a malformed core block must be refused");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        );
        assert_ne!(
            err.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            "malformed is not absent"
        );
    }

    /// An OMITTED mandatory limit is not an advertised zero, but it is
    /// equally unusable, and blanket `#[serde(default)]` zero-filling used
    /// to erase the difference. Each required limit must be refused when it
    /// is missing entirely - and refused as a contract violation, since
    /// RFC 8620 §2 makes every one of them a mandatory member.
    #[test]
    fn any_omitted_required_core_limit_is_a_contract_violation() {
        for field in [
            "maxCallsInRequest",
            "maxObjectsInGet",
            "maxObjectsInSet",
            "maxSizeRequest",
            "maxSizeUpload",
            "maxConcurrentUpload",
            "maxConcurrentRequests",
        ] {
            let mut core = serde_json::json!({
                "maxSizeUpload": 1000,
                "maxConcurrentUpload": 2,
                "maxSizeRequest": 100_000,
                "maxConcurrentRequests": 4,
                "maxCallsInRequest": 8,
                "maxObjectsInGet": 256,
                "maxObjectsInSet": 256,
                "collationAlgorithms": []
            });
            core.as_object_mut().expect("object").remove(field);
            let err = build(&session_with_core(core), no_pim_support())
                .err()
                .unwrap_or_else(|| panic!("omitted {field} must be refused"));
            assert_eq!(
                err.kind(),
                &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                "omitted {field}"
            );
        }
    }

    #[test]
    fn zero_upload_size_is_a_valid_limit() {
        let session = session_with_core(serde_json::json!({
            "maxSizeUpload": 0,
            "maxConcurrentUpload": 2,
            "maxSizeRequest": 100_000,
            "maxConcurrentRequests": 4,
            "maxCallsInRequest": 8,
            "maxObjectsInGet": 256,
            "maxObjectsInSet": 256,
            "collationAlgorithms": []
        }));
        validate_session(&session).expect("zero only disallows nonempty uploads");
        assert_eq!(upload_size_limit(&session).expect("required limit"), 0);
    }

    #[test]
    fn zero_core_concurrency_limits_are_valid_unsigned_ints() {
        let session = session_with_core(serde_json::json!({
            "maxSizeUpload": 1000,
            "maxConcurrentUpload": 0,
            "maxSizeRequest": 100_000,
            "maxConcurrentRequests": 0,
            "maxCallsInRequest": 8,
            "maxObjectsInGet": 256,
            "maxObjectsInSet": 256,
            "collationAlgorithms": []
        }));

        validate_session(&session).expect("zero concurrency is still an advertised limit");
    }

    /// A session document with a legal core block plus whatever other
    /// capability blocks the test wants to vary.
    fn session_with_extra(extra: serde_json::Value) -> Session {
        let mut capabilities = serde_json::json!({
            "urn:ietf:params:jmap:core": {
                "maxSizeUpload": 1000,
                "maxConcurrentUpload": 2,
                "maxSizeRequest": 100_000,
                "maxConcurrentRequests": 4,
                "maxCallsInRequest": 8,
                "maxObjectsInGet": 256,
                "maxObjectsInSet": 256,
                "collationAlgorithms": []
            },
            "urn:ietf:params:jmap:mail": {}
        });
        let map = capabilities.as_object_mut().expect("object");
        for (key, value) in extra.as_object().expect("extra is an object") {
            map.insert(key.clone(), value.clone());
        }
        serde_json::from_value(serde_json::json!({
            "capabilities": capabilities,
            "accounts": {},
            "primaryAccounts": {},
            "username": "user",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-state"
        }))
        .expect("session fixture parses")
    }

    /// The lateral half of the malformed-core defect: the fallback to
    /// `Other` applied to every NON-core capability too, so a websocket
    /// block missing its mandatory `url` derived `PushCapability::None`
    /// exactly as an absent block would. Push is optional, so this still
    /// degrades rather than failing - but it degrades through the
    /// malformed lane, and the session reports the block by URI.
    #[test]
    fn a_malformed_websocket_block_degrades_push_without_reading_as_absent() {
        let session = session_with_extra(serde_json::json!({
            "urn:ietf:params:jmap:websocket": {"supportsPush": true}
        }));
        assert_eq!(
            session.malformed_capabilities().collect::<Vec<_>>(),
            vec!["urn:ietf:params:jmap:websocket"],
            "the malformed block must be nameable in a diagnostic"
        );
        let (caps, _) = build(&session, no_pim_support())
            .expect("an optional malformed capability must not fail open");
        assert_eq!(caps.push, PushCapability::None);
    }

    /// A malformed block for a capability the account DEPENDS on is not a
    /// degradation: `urn:ietf:params:jmap:mail` is the reason a JMAP
    /// account exists here, so a present-and-unparseable mail block takes
    /// the same contract-violation lane the core block takes, and names
    /// itself in the diagnostic.
    #[cfg(feature = "mail")]
    #[test]
    fn a_malformed_mail_block_is_a_named_contract_violation() {
        let session = session_with_extra(serde_json::json!({
            "urn:ietf:params:jmap:mail": "not an object"
        }));
        let err = build(&session, no_pim_support())
            .expect_err("a malformed required capability must be refused");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        );
        assert!(
            format!("{err:?}").contains("urn:ietf:params:jmap:mail"),
            "the diagnostic must name the malformed capability: {err:?}"
        );
    }

    /// An optional family whose block is malformed must not take the
    /// account down with it: the sieve/quota/calendar doors are gated on
    /// their own `PimSupport` flags, and open still succeeds.
    #[test]
    fn a_malformed_optional_block_does_not_fail_open() {
        let session = session_with_extra(serde_json::json!({
            "urn:ietf:params:jmap:sieve": "not an object",
            "urn:ietf:params:jmap:quota": 7
        }));
        let (caps, limits) =
            build(&session, no_pim_support()).expect("optional families degrade, they do not fail");
        assert_eq!(caps.push, PushCapability::None);
        assert_eq!(limits.max_objects_in_get, 256);
    }

    /// The `Error::MalformedCapability` the WebSocket door raises when the
    /// block is present-and-unparseable must classify as a contract
    /// violation naming the URI - not as the `Unsupported` that an ABSENT
    /// websocket block earns, which would file a server bug under
    /// "feature not offered".
    #[test]
    fn a_malformed_capability_error_classifies_as_a_named_contract_violation() {
        let err = crate::sync::error::into_account_error(
            crate::Error::MalformedCapability {
                capability: "urn:ietf:params:jmap:websocket",
            },
            crate::sync::error::JmapErrorContext::new(AccountOperation::PushSubscribe),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        );
        assert!(
            format!("{err:?}").contains("urn:ietf:params:jmap:websocket"),
            "the diagnostic must name the malformed capability: {err:?}"
        );
    }

    /// The invariants the engine reads before it reads anything else.
    /// JMAP states cursors server-side, has no UIDVALIDITY analogue, and
    /// its cursors do not expire on a clock.
    #[test]
    fn cursor_and_state_shape_is_server_issued_and_non_expiring() {
        let (caps, _) = build(&session_with(256, false), no_pim_support()).unwrap();
        assert_eq!(caps.cursor_freshness, CursorFreshness::ServerIssued);
        assert!(!caps.requires_uidvalidity_recheck);
        assert_eq!(caps.historyid_expires_after, None);
        assert_eq!(caps.delta_token_expires_after, None);
        assert_eq!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                mail_set_flags: true,
                mail_move: true,
                mail_destroy: true,
                contact_update: false,
                event_update: false,
                contact_delete: false,
                event_delete: false,
                event_rsvp: false,
            }
        );
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert!(!caps.blob_digest_pre_download);
        assert_eq!(caps.quota_signal, QuotaSignal::None);
        assert_eq!(caps.rate_limit_class, RateLimitClass::Generous);
        assert!(caps.discovers_foreign_namespaces_on_rediscovery);
    }

    fn scheduled_session() -> Session {
        session(
            r#"{
                "capabilities": {
                    "urn:ietf:params:jmap:core": {
                        "maxSizeUpload": 1000,
                        "maxConcurrentUpload": 2,
                        "maxSizeRequest": 100000,
                        "maxConcurrentRequests": 4,
                        "maxCallsInRequest": 8,
                        "maxObjectsInGet": 256,
                        "maxObjectsInSet": 700,
                        "collationAlgorithms": []
                    },
                    "urn:ietf:params:jmap:mail": {}
                },
                "accounts": {},
                "primaryAccounts": {},
                "username": "user",
                "apiUrl": "https://example.test/jmap/api",
                "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
                "uploadUrl": "https://example.test/upload/{accountId}",
                "eventSourceUrl": "https://example.test/eventsource",
                "state": "session-state"
            }"#,
        )
    }

    fn pim_support(max_delayed_send: usize) -> PimSupport {
        PimSupport {
            submission: true,
            max_delayed_send,
            foreign_submission: false,
            vacation: false,
            quota: false,
            sieve: false,
            contacts: false,
            calendar: false,
        }
    }

    #[test]
    fn scheduled_send_capability_tracks_max_delayed_send() {
        let (no_window, _) = build(&scheduled_session(), pim_support(0)).unwrap();
        assert!(
            !no_window.pim_methods.scheduled_send,
            "maxDelayedSend == 0 must disable scheduled_send"
        );

        let (with_window, _) = build(&scheduled_session(), pim_support(3600)).unwrap();
        assert!(
            with_window.pim_methods.scheduled_send,
            "maxDelayedSend > 0 must enable scheduled_send"
        );
    }

    #[test]
    fn send_as_capability_tracks_seeded_foreign_submission() {
        let (unavailable, _) = build(&scheduled_session(), pim_support(0)).unwrap();
        assert!(!unavailable.pim_methods.send_as);

        let mut support = pim_support(0);
        support.foreign_submission = true;
        let (available, _) = build(&scheduled_session(), support).unwrap();
        assert!(available.pim_methods.send_as);
    }

    #[test]
    fn scheduled_send_envelope_serializes_holduntil_on_mailfrom() {
        use crate::email_submission::{Address, EmailSubmissionSet};

        let mut set = EmailSubmissionSet::new();
        {
            let submit = set.create_with_id("submit0");
            let mail_from = Address::new("sender@example.test")
                .with_parameter("holduntil", Some("2026-06-16T10:00:00+00:00"));
            submit.envelope(mail_from, [Address::new("rcpt@example.test")]);
        }
        let json = serde_json::to_value(&set).unwrap();
        let body = json.to_string();
        assert!(
            json["create"]["submit0"].get("undoStatus").is_none(),
            "undoStatus is server-set and production scheduled creates omit it"
        );
        assert!(
            body.contains("holduntil"),
            "envelope mailFrom must carry the holduntil parameter: {body}"
        );
        assert!(
            body.contains("2026-06-16T10:00:00+00:00"),
            "holduntil value must serialize: {body}"
        );
    }

    #[test]
    fn scheduled_send_cancel_patch_serializes_canceled_undo_status() {
        use crate::email_submission::{EmailSubmissionId, EmailSubmissionSet, UndoStatus};

        let mut set = EmailSubmissionSet::new();
        set.update(EmailSubmissionId::new("submission-1"))
            .undo_status(UndoStatus::Canceled);
        let body = serde_json::to_value(&set).unwrap().to_string();
        assert!(
            body.contains("\"undoStatus\":\"canceled\""),
            "cancel patch must serialize undoStatus canceled: {body}"
        );
    }
}
