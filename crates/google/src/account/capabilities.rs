use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CursorFreshness, FilterRuleShape,
    MutationCapabilities, MutationConcurrency, MutationReplaySafety, PimMethodSupport,
    PushCapability, QuotaSignal, RateLimitClass, StarredFlagShape,
};

pub(crate) const GMAIL_BATCH_MODIFY_LIMIT: usize = 1000;

/// Gmail's per-request cap on the length of `addLabelIds` and
/// `removeLabelIds`.
///
/// The `users.messages.modify` reference states it for both lists
/// ("You can add up to 100 labels with each update", and the same
/// sentence for removal). The `batchModify` reference documents only
/// the 1000-id cap on `ids[]` and says nothing about the label lists,
/// but the two methods share one label-mutation path, so the crate
/// holds every request it emits under the documented figure rather
/// than assuming the silent method is unbounded.
///
/// This is NOT a bound the account's label vocabulary respects on its
/// own. `FlagOp::Set` names every user label in `removeLabelIds` by
/// construction (see `flags::patch_for_set`), and Gmail's own guidance
/// only *recommends* staying under 500 labels per account - the hard
/// ceiling is far above 100. A label-heavy account would therefore
/// turn every exact-set into a 400 that id-splitting cannot fix, which
/// is why the submitter chunks the label lists instead.
pub(crate) const GMAIL_MODIFY_LABEL_LIMIT: usize = 100;

pub(crate) fn gmail_capabilities() -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::OutOfProcessPubsub,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::ExpectedVersion {
                mail_set_flags: false,
                mail_move: false,
                mail_destroy: false,
                contact_update: false,
                event_update: true,
                contact_delete: false,
                event_delete: true,
                event_rsvp: true,
            },
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: GMAIL_BATCH_MODIFY_LIMIT,
            max_wait: Duration::from_millis(75),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Tiered,
        quota_signal: QuotaSignal::QuotaUnits,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport {
            category_definitions: false,
            message_reactions: false,
            add_to_container: true,
            remove_from_container: true,
            set_keyword: false,
            set_label_membership: true,
            set_category: false,
            set_extended_property: false,
            // Gmail has no message-importance field.
            set_importance: false,
            set_is_read: true,
            send_message: true,
            attachment_upload: false,
            host_attachment: true,
            draft_create: true,
            draft_update: true,
            draft_discard: true,
            draft_send: true,
            scheduled_send: false,
            send_as: false,
            search: true,
            search_messages: true,
            containers_list: true,
            container_create: true,
            container_rename: true,
            container_move: false,
            container_delete: true,
            identities_list: true,
            identity_update: true,
            vacation_get: true,
            vacation_set: true,
            quota_get: false,
            thread_hydrate: true,
            message_hydrate: true,
            open_raw_rfc822: true,
            filters_list: true,
            filter_create: true,
            filter_update: false,
            filter_delete: true,
            filter_validate: true,
            address_books_list: true,
            contacts_list: true,
            contact_get: true,
            contact_create: true,
            contact_update: true,
            contact_delete: true,
            contact_search: true,
            contact_autocomplete: true,
            directory_search: true,
            directory_enumerate: true,
            // Google People has no directory-group surface (the Workspace
            // Directory API is a separate consent/product scope, and People
            // "contact groups" are the user's personal labels, not
            // org-directory groups).
            directory_groups_list: false,
            directory_group_expand: false,
            calendars_list: true,
            events_in_range: true,
            event_get: true,
            event_create: true,
            event_update: true,
            event_delete: true,
            event_rsvp: true,
            event_search: true,
            event_autocomplete: true,
        },
        filter_rule_shape: FilterRuleShape::Rules,
        conveniences: bifrost_types::ConvenienceShape {
            starred: StarredFlagShape::LabelMembership,
            replied_via_keyword: false,
            replied_via_extended_property: false,
            forwarded_via_keyword: false,
            forwarded_via_extended_property: false,
            // Gmail's read-receipt model is read-only.
            mdn_sent_via_keyword: false,
        },
        // Gmail delegation rides separate authorization, not open-time
        // discovery: a reopen of this account can never surface another
        // user's mailbox.
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bifrost_types::{
        BlobRangeSupport, CursorFreshness, MutationConcurrency, MutationReplaySafety,
        PushCapability, QuotaSignal, RateLimitClass,
    };

    use super::*;

    #[test]
    fn capability_builder_matches_gmail_posture() {
        let caps = gmail_capabilities();
        assert_eq!(caps.cursor_freshness, CursorFreshness::ServerIssued);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert!(!caps.blob_digest_pre_download);
        assert_eq!(caps.push, PushCapability::OutOfProcessPubsub);
        assert!(!caps.push_in_process());
        assert_eq!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                mail_set_flags: false,
                mail_move: false,
                mail_destroy: false,
                contact_update: false,
                event_update: true,
                contact_delete: false,
                event_delete: true,
                event_rsvp: true,
            }
        );
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert_eq!(caps.batching_policy.max_items, 1000);
        assert!(caps.historyid_expires_after.is_none());
    }

    #[test]
    fn host_attachment_capability_true() {
        // Google Drive hosting is the bifrost replacement for ratatoskr's
        // consumer-side `supports_cloud_upload(Gmail) == true`.
        assert!(gmail_capabilities().pim_methods.host_attachment);
    }

    #[test]
    fn pim_method_support_matches_gmail_wire_shape() {
        let caps = gmail_capabilities();
        assert!(caps.pim_methods.add_to_container);
        assert!(caps.pim_methods.remove_from_container);
        assert!(!caps.pim_methods.set_keyword);
        assert!(caps.pim_methods.set_label_membership);
        assert!(!caps.pim_methods.set_category);
        assert!(!caps.pim_methods.set_extended_property);
        // Gmail has no message-importance field and a read-only
        // read-receipt model.
        assert!(!caps.pim_methods.set_importance);
        assert!(!caps.conveniences.mdn_sent_via_keyword);
        assert!(caps.pim_methods.set_is_read);
        assert!(caps.pim_methods.send_message);
        assert!(!caps.pim_methods.attachment_upload);
        assert!(caps.pim_methods.draft_create);
        assert!(caps.pim_methods.draft_update);
        assert!(caps.pim_methods.draft_discard);
        assert!(caps.pim_methods.draft_send);
        assert!(caps.pim_methods.search);
        assert!(caps.pim_methods.search_messages);
        assert!(caps.pim_methods.containers_list);
        assert!(caps.pim_methods.container_create);
        assert!(caps.pim_methods.container_rename);
        assert!(!caps.pim_methods.container_move);
        assert!(caps.pim_methods.container_delete);
        assert!(caps.pim_methods.identities_list);
        assert!(caps.pim_methods.identity_update);
        assert!(caps.pim_methods.vacation_get);
        assert!(caps.pim_methods.vacation_set);
        assert!(!caps.pim_methods.quota_get);
        assert!(caps.pim_methods.thread_hydrate);
        assert!(caps.pim_methods.message_hydrate);
        assert!(caps.pim_methods.open_raw_rfc822);
        assert_eq!(caps.filter_rule_shape, FilterRuleShape::Rules);
        assert!(caps.pim_methods.filters_list);
        assert!(caps.pim_methods.filter_create);
        assert!(!caps.pim_methods.filter_update);
        assert!(caps.pim_methods.filter_delete);
        assert!(caps.pim_methods.filter_validate);
        assert!(caps.pim_methods.address_books_list);
        assert!(caps.pim_methods.contacts_list);
        assert!(caps.pim_methods.contact_get);
        assert!(caps.pim_methods.contact_create);
        assert!(caps.pim_methods.contact_update);
        assert!(caps.pim_methods.contact_delete);
        assert!(caps.pim_methods.contact_search);
        assert!(caps.pim_methods.contact_autocomplete);
        assert!(caps.pim_methods.directory_search);
        assert!(caps.pim_methods.directory_enumerate);
    }

    #[test]
    fn convenience_shape_uses_gmail_starred_label() {
        let caps = gmail_capabilities();
        assert_eq!(caps.conveniences.starred, StarredFlagShape::LabelMembership);
        assert!(!caps.conveniences.replied_via_keyword);
        assert!(!caps.conveniences.replied_via_extended_property);
        assert!(!caps.conveniences.forwarded_via_keyword);
        assert!(!caps.conveniences.forwarded_via_extended_property);
    }

    #[test]
    fn batching_policy_matches_gmail_batch_modify_limit() {
        let caps = gmail_capabilities();
        assert_eq!(caps.batching_policy.max_items, GMAIL_BATCH_MODIFY_LIMIT);
        assert_eq!(caps.batching_policy.max_wait, Duration::from_millis(75));
        assert!(caps.batching_policy.flush_on_input_close);
    }

    #[test]
    fn rate_limit_and_quota_match_gmail_quota_units() {
        let caps = gmail_capabilities();
        assert_eq!(caps.rate_limit_class, RateLimitClass::Tiered);
        assert_eq!(caps.quota_signal, QuotaSignal::QuotaUnits);
    }

    #[test]
    fn no_uidvalidity_recheck_or_delta_token() {
        let caps = gmail_capabilities();
        assert!(!caps.requires_uidvalidity_recheck);
        assert!(caps.delta_token_expires_after.is_none());
    }
}
