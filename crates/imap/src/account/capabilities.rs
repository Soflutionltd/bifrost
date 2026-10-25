use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, ConvenienceShape, CursorFreshness,
    FilterRuleShape, MutationCapabilities, MutationConcurrency, MutationReplaySafety,
    PimMethodSupport, PushCapability, QuotaSignal, RateLimitClass, StarredFlagShape,
};

use crate::types::{Capability, MailboxAttribute, MailboxInfo, ServerProfile};

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_capabilities(
    profile: &ServerProfile,
    folders: &[MailboxInfo],
    has_sieve: bool,
    contacts: Option<&AccountCapabilities>,
    calendars: Option<&AccountCapabilities>,
    submission_configured: bool,
    foreign_namespaces_advertised: bool,
    push_capacity: usize,
) -> AccountCapabilities {
    // Copy only the contact / calendar field subsets from the
    // sub-accounts' real capability snapshots; an absent sub-account
    // leaves those flags `false` (`PimMethodSupport::default()`). Every
    // other `pim_methods` field stays IMAP-owned below.
    let contact_pim = contacts.map(|c| &c.pim_methods);
    let calendar_pim = calendars.map(|c| &c.pim_methods);
    let has_drafts = folders.iter().any(|folder| {
        folder
            .attributes
            .iter()
            .any(|attr| matches!(attr, MailboxAttribute::Drafts))
            // Same leaf rule `folder_role` uses for its name fallback, so
            // the Drafts folder this flag promises is the one
            // `role_folder(Drafts)` later resolves.
            || super::folder_registry::leaf_name(folder.name.as_str(), folder.delimiter)
                .eq_ignore_ascii_case("drafts")
    });
    let mail_set_flags = profile.supports_condstore();
    let can_expunge_by_uid = profile.supports(Capability::UidPlus);
    let has_thread_references = profile.capabilities.iter().any(
        |cap| matches!(cap, Capability::Thread(alg) if alg.eq_ignore_ascii_case("REFERENCES")),
    );
    let contact_update = contacts.is_some_and(|caps| {
        matches!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                contact_update: true,
                ..
            }
        )
    });
    let event_update = calendars.is_some_and(|caps| {
        matches!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                event_update: true,
                ..
            }
        )
    });
    let contact_delete = contacts.is_some_and(|caps| {
        matches!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                contact_delete: true,
                ..
            }
        )
    });
    let event_delete = calendars.is_some_and(|caps| {
        matches!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                event_delete: true,
                ..
            }
        )
    });
    AccountCapabilities {
        cursor_freshness: CursorFreshness::Hybrid,
        // IMAP can fetch a BODY[] section by range, but this account does
        // not yet emit attachment BlobHandles backed by those sections.
        // Advertising range support before a caller can receive such a
        // handle is dishonest.
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        // Push capacity is carved out of `pool_cap` and is zero at a
        // single-session cap. Advertising push the account will refuse for
        // every scope would keep bifrost-sync's push forwarder open with
        // every subscribe refused.
        push: if profile.supports(Capability::Idle) && push_capacity > 0 {
            PushCapability::InProcess
        } else {
            PushCapability::None
        },
        mutation: MutationCapabilities {
            concurrency: if mail_set_flags
                || contact_update
                || event_update
                || contact_delete
                || event_delete
            {
                MutationConcurrency::ExpectedVersion {
                    mail_set_flags,
                    mail_move: false,
                    mail_destroy: false,
                    contact_update,
                    event_update,
                    contact_delete,
                    event_delete,
                    event_rsvp: false,
                }
            } else {
                MutationConcurrency::None
            },
            replay_safety: MutationReplaySafety::None,
            reassertion: bifrost_types::MutationReassertion {
                set_flags: true,
                destroy: true,
            },
        },
        batching_policy: BatchingPolicy {
            max_items: 1024,
            max_wait: Duration::from_millis(50),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::PerConnection,
        quota_signal: if profile.supports(Capability::Quota) {
            QuotaSignal::Implicit
        } else {
            QuotaSignal::None
        },
        requires_uidvalidity_recheck: true,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport {
            category_definitions: false,
            message_reactions: false,
            add_to_container: true,
            remove_from_container: can_expunge_by_uid,
            set_keyword: true,
            set_label_membership: false,
            set_category: false,
            set_extended_property: false,
            set_importance: true,
            set_is_read: true,
            send_message: submission_configured,
            attachment_upload: false,
            host_attachment: false,
            draft_create: has_drafts && can_expunge_by_uid,
            draft_update: false,
            draft_discard: can_expunge_by_uid,
            draft_send: submission_configured,
            // IMAP relay FUTURERELEASE is per-connection (advertised in
            // EHLO at send time), unknown at open. The honest snapshot
            // is `false`; an unsupporting relay surfaces a runtime
            // `Unsupported(Send)` from the smtp boundary.
            scheduled_send: false,
            // Graph-style shared-mailbox send routing is not modeled over
            // SMTP; a shared-mailbox send is `request.from` + relay
            // authorization. A `send_as` request is rejected at the
            // boundary.
            send_as: false,
            search: has_thread_references,
            search_messages: true,
            containers_list: true,
            container_create: true,
            container_rename: true,
            container_move: true,
            container_delete: true,
            identities_list: false,
            identity_update: false,
            vacation_get: false,
            vacation_set: false,
            quota_get: profile.supports(Capability::Quota),
            thread_hydrate: has_thread_references,
            message_hydrate: true,
            open_raw_rfc822: true,
            filters_list: has_sieve,
            filter_create: has_sieve,
            filter_update: has_sieve,
            filter_delete: has_sieve,
            filter_validate: has_sieve,
            address_books_list: contact_pim.is_some_and(|p| p.address_books_list),
            contacts_list: contact_pim.is_some_and(|p| p.contacts_list),
            contact_get: contact_pim.is_some_and(|p| p.contact_get),
            contact_create: contact_pim.is_some_and(|p| p.contact_create),
            contact_update: contact_pim.is_some_and(|p| p.contact_update),
            contact_delete: contact_pim.is_some_and(|p| p.contact_delete),
            contact_search: contact_pim.is_some_and(|p| p.contact_search),
            contact_autocomplete: contact_pim.is_some_and(|p| p.contact_autocomplete),
            directory_search: contact_pim.is_some_and(|p| p.directory_search),
            directory_enumerate: contact_pim.is_some_and(|p| p.directory_enumerate),
            directory_groups_list: false,
            directory_group_expand: false,
            calendars_list: calendar_pim.is_some_and(|p| p.calendars_list),
            events_in_range: calendar_pim.is_some_and(|p| p.events_in_range),
            event_get: calendar_pim.is_some_and(|p| p.event_get),
            event_create: calendar_pim.is_some_and(|p| p.event_create),
            event_update: calendar_pim.is_some_and(|p| p.event_update),
            event_delete: calendar_pim.is_some_and(|p| p.event_delete),
            event_rsvp: calendar_pim.is_some_and(|p| p.event_rsvp),
            event_search: calendar_pim.is_some_and(|p| p.event_search),
            event_autocomplete: calendar_pim.is_some_and(|p| p.event_autocomplete),
        },
        filter_rule_shape: if has_sieve {
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
        // From the open-time NAMESPACE response: IMAP discovers shared /
        // other-user folders only at open (no scope-lifecycle events), so
        // a reopen is the only door a post-open ACL grant can walk
        // through - and NAMESPACE is a truthful "never" signal, so a
        // personal-only server honestly reports false.
        discovers_foreign_namespaces_on_rediscovery: foreign_namespaces_advertised,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caller_delete_capabilities_follow_only_the_correct_composed_backend() {
        let profile = ServerProfile::new(Vec::new(), Vec::new());
        for (contact_delete, event_delete) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let mut contacts = crate::account::test_support::stub_capabilities();
            contacts.mutation.concurrency = MutationConcurrency::ExpectedVersion {
                mail_set_flags: false,
                mail_move: false,
                mail_destroy: false,
                contact_update: false,
                event_update: false,
                contact_delete,
                event_delete: true,
                event_rsvp: false,
            };
            let mut calendars = crate::account::test_support::stub_capabilities();
            calendars.mutation.concurrency = MutationConcurrency::ExpectedVersion {
                mail_set_flags: false,
                mail_move: false,
                mail_destroy: false,
                contact_update: false,
                event_update: false,
                contact_delete: true,
                event_delete,
                event_rsvp: false,
            };
            let caps = build_capabilities(
                &profile,
                &[],
                false,
                Some(&contacts),
                Some(&calendars),
                false,
                false,
                1,
            );
            let expected = if contact_delete || event_delete {
                MutationConcurrency::ExpectedVersion {
                    mail_set_flags: false,
                    mail_move: false,
                    mail_destroy: false,
                    contact_update: false,
                    event_update: false,
                    contact_delete,
                    event_delete,
                    event_rsvp: false,
                }
            } else {
                MutationConcurrency::None
            };
            assert_eq!(caps.mutation.concurrency, expected);
        }
    }

    #[test]
    fn caller_version_capabilities_follow_the_composed_update_doors() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        for (contact_update, event_update) in [(true, false), (false, true), (true, true)] {
            let mut contacts = crate::account::test_support::stub_capabilities();
            contacts.mutation.concurrency = MutationConcurrency::ExpectedVersion {
                mail_set_flags: false,
                mail_move: false,
                mail_destroy: false,
                contact_update,
                event_update: false,
                contact_delete: false,
                event_delete: false,
                event_rsvp: false,
            };
            let mut calendars = crate::account::test_support::stub_capabilities();
            calendars.mutation.concurrency = MutationConcurrency::ExpectedVersion {
                mail_set_flags: false,
                mail_move: false,
                mail_destroy: false,
                contact_update: false,
                event_update,
                contact_delete: false,
                event_delete: false,
                event_rsvp: false,
            };
            let caps = build_capabilities(
                &profile,
                &[],
                false,
                Some(&contacts),
                Some(&calendars),
                false,
                false,
                1,
            );
            assert_eq!(
                caps.mutation.concurrency,
                MutationConcurrency::ExpectedVersion {
                    mail_set_flags: false,
                    mail_move: false,
                    mail_destroy: false,
                    contact_update,
                    event_update,
                    contact_delete: false,
                    event_delete: false,
                    event_rsvp: false,
                }
            );
        }
    }

    #[test]
    fn capability_builder_sets_imap_tier_values() {
        let profile = ServerProfile::new(
            vec![Capability::Idle, Capability::Condstore, Capability::Quota],
            Vec::new(),
        );
        let caps = build_capabilities(&profile, &[], false, None, None, false, false, 1);
        assert_eq!(caps.cursor_freshness, CursorFreshness::Hybrid);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert_eq!(caps.push, PushCapability::InProcess);
        assert_eq!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                mail_set_flags: true,
                mail_move: false,
                mail_destroy: false,
                contact_update: false,
                event_update: false,
                contact_delete: false,
                event_delete: false,
                event_rsvp: false,
            }
        );
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert_eq!(
            caps.mutation.reassertion,
            bifrost_types::MutationReassertion {
                set_flags: true,
                destroy: true
            }
        );
        assert_eq!(caps.filter_rule_shape, FilterRuleShape::None);
        assert!(!caps.pim_methods.filters_list);
        assert!(!caps.pim_methods.address_books_list);
        assert!(!caps.pim_methods.contacts_list);
        assert!(!caps.pim_methods.contact_get);
        assert!(!caps.pim_methods.contact_create);
        assert!(!caps.pim_methods.contact_update);
        assert!(!caps.pim_methods.contact_delete);
        assert!(!caps.pim_methods.contact_search);
        assert!(!caps.pim_methods.contact_autocomplete);
        assert!(caps.pim_methods.open_raw_rfc822);
        assert!(caps.pim_methods.set_importance);
        assert!(caps.conveniences.mdn_sent_via_keyword);
        assert!(caps.requires_uidvalidity_recheck);
    }

    #[test]
    fn capability_builder_leaves_mutation_concurrency_none_without_condstore() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let caps = build_capabilities(&profile, &[], false, None, None, false, false, 1);
        assert_eq!(caps.mutation.concurrency, MutationConcurrency::None);
    }

    #[test]
    fn capability_builder_advertises_sieve_when_configured() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let caps = build_capabilities(&profile, &[], true, None, None, false, false, 1);
        assert_eq!(caps.filter_rule_shape, FilterRuleShape::Scripts);
        assert!(caps.pim_methods.filters_list);
        assert!(caps.pim_methods.filter_create);
        assert!(caps.pim_methods.filter_update);
        assert!(caps.pim_methods.filter_delete);
        assert!(caps.pim_methods.filter_validate);
    }

    #[test]
    fn directory_capabilities_copy_the_contact_backend_only() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        for (search, enumerate) in [(false, false), (true, false), (true, true)] {
            let mut sub = crate::account::test_support::stub_capabilities();
            sub.pim_methods.directory_search = search;
            sub.pim_methods.directory_enumerate = enumerate;
            let caps = build_capabilities(&profile, &[], false, Some(&sub), None, false, false, 1);
            assert_eq!(caps.pim_methods.directory_search, search);
            assert_eq!(caps.pim_methods.directory_enumerate, enumerate);
            let calendar_only =
                build_capabilities(&profile, &[], false, None, Some(&sub), false, false, 1);
            assert!(!calendar_only.pim_methods.directory_search);
            assert!(!calendar_only.pim_methods.directory_enumerate);
        }
    }

    /// Minimal capability snapshot for a composed sub-account, with all
    /// contact flags on and all calendar flags off (the carddav shape).
    fn contacts_snapshot() -> AccountCapabilities {
        let mut caps = crate::account::test_support::stub_capabilities();
        caps.pim_methods.address_books_list = true;
        caps.pim_methods.contacts_list = true;
        caps.pim_methods.contact_get = true;
        caps.pim_methods.contact_create = true;
        caps.pim_methods.contact_update = true;
        caps.pim_methods.contact_delete = true;
        caps.pim_methods.contact_search = true;
        caps.pim_methods.contact_autocomplete = true;
        caps
    }

    #[test]
    fn capability_builder_advertises_contacts_when_carddav_configured() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let sub = contacts_snapshot();
        let caps = build_capabilities(&profile, &[], false, Some(&sub), None, false, false, 1);
        assert!(caps.pim_methods.address_books_list);
        assert!(caps.pim_methods.contacts_list);
        assert!(caps.pim_methods.contact_get);
        assert!(caps.pim_methods.contact_create);
        assert!(caps.pim_methods.contact_update);
        assert!(caps.pim_methods.contact_delete);
        assert!(caps.pim_methods.contact_search);
        assert!(caps.pim_methods.contact_autocomplete);
        // Calendar flags stay false with no calendars sub.
        assert!(!caps.pim_methods.calendars_list);
        assert!(!caps.pim_methods.event_rsvp);
    }

    #[test]
    fn capability_builder_copies_real_sub_account_flags() {
        // A calendars sub that does NOT support event_rsvp must yield a
        // composed capability with event_rsvp = false, proving the
        // snapshot is copied field-by-field rather than blanket-true.
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let mut calendars = crate::account::test_support::stub_capabilities();
        calendars.pim_methods.calendars_list = true;
        calendars.pim_methods.events_in_range = true;
        calendars.pim_methods.event_get = true;
        calendars.pim_methods.event_rsvp = false; // unsupported by this provider

        let caps = build_capabilities(
            &profile,
            &[],
            false,
            None,
            Some(&calendars),
            false,
            false,
            1,
        );
        assert!(caps.pim_methods.calendars_list);
        assert!(caps.pim_methods.event_get);
        assert!(
            !caps.pim_methods.event_rsvp,
            "an unsupported sub-account flag must not be advertised true",
        );
        // IMAP-owned mail flags are untouched by the calendar merge.
        assert!(caps.pim_methods.message_hydrate);
        assert!(caps.pim_methods.search_messages);
    }

    #[test]
    fn capabilities_send_flag_tracks_submission() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());

        let without = build_capabilities(&profile, &[], false, None, None, false, false, 1);
        assert!(!without.pim_methods.send_message);
        assert!(!without.pim_methods.draft_send);

        let with = build_capabilities(&profile, &[], false, None, None, true, false, 1);
        assert!(with.pim_methods.send_message);
        assert!(with.pim_methods.draft_send);
        // Submission does not turn on uploaded-attachment support (A6).
        assert!(!with.pim_methods.attachment_upload);
    }

    #[test]
    fn foreign_namespace_flag_flows_through_verbatim() {
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let without = build_capabilities(&profile, &[], false, None, None, false, false, 1);
        assert!(!without.discovers_foreign_namespaces_on_rediscovery);
        let with = build_capabilities(&profile, &[], false, None, None, false, true, 1);
        assert!(with.discovers_foreign_namespaces_on_rediscovery);
    }

    fn mailbox(name: &str, attributes: Vec<MailboxAttribute>) -> MailboxInfo {
        MailboxInfo {
            name: crate::types::MailboxName::new(name).expect("valid mailbox"),
            delimiter: Some('/'),
            attributes,
            ..Default::default()
        }
    }

    // `draft_create` APPENDs to Drafts and needs the APPENDUID from
    // UIDPLUS to mint the returned handle, so both halves must be present
    // before the flag goes true.
    #[test]
    fn draft_create_requires_both_a_drafts_folder_and_uidplus() {
        let with_uidplus = ServerProfile::new(vec![Capability::UidPlus], Vec::new());
        let without_uidplus = ServerProfile::new(Vec::new(), Vec::new());
        let drafts = [mailbox("Drafts", Vec::new())];
        let special_use = [mailbox("Brouillons", vec![MailboxAttribute::Drafts])];

        let caps = build_capabilities(&with_uidplus, &drafts, false, None, None, false, false, 1);
        assert!(caps.pim_methods.draft_create, "name match plus UIDPLUS");

        let caps = build_capabilities(
            &with_uidplus,
            &special_use,
            false,
            None,
            None,
            false,
            false,
            1,
        );
        assert!(
            caps.pim_methods.draft_create,
            "a localized Drafts folder is found by its SPECIAL-USE attribute",
        );

        let caps = build_capabilities(
            &without_uidplus,
            &drafts,
            false,
            None,
            None,
            false,
            false,
            1,
        );
        assert!(!caps.pim_methods.draft_create, "no UIDPLUS, no APPENDUID");

        let caps = build_capabilities(&with_uidplus, &[], false, None, None, false, false, 1);
        assert!(!caps.pim_methods.draft_create, "nowhere to APPEND a draft");
    }

    #[test]
    fn drafts_name_fallback_uses_the_list_delimiter() {
        let profile = ServerProfile::new(vec![Capability::UidPlus], Vec::new());
        let mut drafts = mailbox("INBOX.Drafts", Vec::new());
        drafts.delimiter = Some('.');
        let caps = build_capabilities(&profile, &[drafts], false, None, None, false, false, 1);
        assert!(caps.pim_methods.draft_create);
    }

    // Thread search and thread hydration are implemented over the THREAD
    // command with the REFERENCES algorithm; a server offering only
    // ORDEREDSUBJECT must not have them advertised.
    #[test]
    fn thread_backed_methods_require_thread_references() {
        let references = ServerProfile::new(
            vec![Capability::Thread("REFERENCES".to_owned())],
            Vec::new(),
        );
        let caps = build_capabilities(&references, &[], false, None, None, false, false, 1);
        assert!(caps.pim_methods.search);
        assert!(caps.pim_methods.thread_hydrate);

        // The capability match is case-insensitive on the algorithm name.
        let lowercase = ServerProfile::new(
            vec![Capability::Thread("references".to_owned())],
            Vec::new(),
        );
        let caps = build_capabilities(&lowercase, &[], false, None, None, false, false, 1);
        assert!(caps.pim_methods.search);

        let ordered_subject = ServerProfile::new(
            vec![Capability::Thread("ORDEREDSUBJECT".to_owned())],
            Vec::new(),
        );
        let caps = build_capabilities(&ordered_subject, &[], false, None, None, false, false, 1);
        assert!(!caps.pim_methods.search);
        assert!(!caps.pim_methods.thread_hydrate);
        // Per-message search does not need THREAD at all.
        assert!(caps.pim_methods.search_messages);
    }

    #[test]
    fn push_and_quota_track_their_server_capabilities() {
        let bare = ServerProfile::new(Vec::new(), Vec::new());
        let caps = build_capabilities(&bare, &[], false, None, None, false, false, 1);
        assert_eq!(caps.push, PushCapability::None, "IDLE is what drives push");
        assert_eq!(caps.quota_signal, QuotaSignal::None);
        assert!(!caps.pim_methods.quota_get);

        let quota = ServerProfile::new(vec![Capability::Quota], Vec::new());
        let caps = build_capabilities(&quota, &[], false, None, None, false, false, 1);
        assert_eq!(caps.quota_signal, QuotaSignal::Implicit);
        assert!(caps.pim_methods.quota_get);
    }

    #[test]
    fn expunge_backed_methods_require_uidplus_or_rev2() {
        let bare = ServerProfile::new(Vec::new(), Vec::new());
        let caps = build_capabilities(&bare, &[], false, None, None, false, false, 1);
        assert!(!caps.pim_methods.remove_from_container);
        assert!(!caps.pim_methods.draft_discard);
        assert!(!caps.pim_methods.draft_create);

        let uidplus = ServerProfile::new(vec![Capability::UidPlus], Vec::new());
        let caps = build_capabilities(&uidplus, &[], false, None, None, false, false, 1);
        assert!(caps.pim_methods.remove_from_container);
        assert!(caps.pim_methods.draft_discard);

        let rev2 = ServerProfile::new(vec![Capability::Imap4Rev2], vec!["IMAP4REV2".to_owned()]);
        let drafts = [mailbox("Drafts", Vec::new())];
        let caps = build_capabilities(&rev2, &drafts, false, None, None, false, false, 1);
        assert!(caps.pim_methods.remove_from_container);
        assert!(caps.pim_methods.draft_discard);
        assert!(caps.pim_methods.draft_create);
    }

    #[test]
    fn scheduled_send_flag_stays_false_with_submission_configured() {
        // IMAP relay FUTURERELEASE is per-connection (EHLO at send
        // time), unknown at open. The capability snapshot is the honest
        // `false` even when submission is configured; support is decided
        // at send time by the smtp boundary.
        let profile = ServerProfile::new(vec![Capability::Idle], Vec::new());
        let with_submission = build_capabilities(&profile, &[], false, None, None, true, false, 1);
        assert!(!with_submission.pim_methods.scheduled_send);
    }

    #[test]
    fn push_is_not_advertised_without_push_capacity() {
        for (idle, capacity, expected) in [
            (true, 0, PushCapability::None),
            (true, 1, PushCapability::InProcess),
            (false, 1, PushCapability::None),
        ] {
            let profile = ServerProfile::new(
                if idle {
                    vec![Capability::Idle]
                } else {
                    Vec::new()
                },
                Vec::new(),
            );
            assert_eq!(
                build_capabilities(&profile, &[], false, None, None, false, false, capacity).push,
                expected
            );
        }
    }
}
