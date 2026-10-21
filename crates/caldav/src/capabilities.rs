use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, ConvenienceShape, CursorFreshness,
    FilterRuleShape, MutationCapabilities, MutationConcurrency, MutationReplaySafety,
    PimMethodSupport, PushCapability, QuotaSignal, RateLimitClass,
};

/// True when the server advertises enough RFC 6638 scheduling for the
/// account's `event_rsvp` path to work at all.
///
/// Posting an iTIP REPLY needs two discovered facts and fails without
/// either:
///
/// - a `CALDAV:schedule-outbox-URL` on the current-user-principal, which
///   is the collection the REPLY is POSTed to, and
/// - a calendar-user-address for this user (from
///   `CALDAV:calendar-user-address-set`, or configured explicitly),
///   which is the `Originator` of that POST and the `ATTENDEE` the
///   REPLY is written as.
///
/// A server offering neither is a plain RFC 4791 store: it can hold
/// events but cannot transmit a response to the organizer. Reporting
/// `event_rsvp = true` there passes a consumer's capability gate and
/// then fails at the POST, which is a much worse failure mode than
/// telling the consumer up front that RSVP is unavailable.
///
/// Empty or whitespace-only values count as absent; a PROPFIND that
/// returns the element with no href carries no usable address.
pub(crate) fn scheduling_available(
    rsvp_email: Option<&str>,
    schedule_outbox_url: Option<&str>,
) -> bool {
    fn present(value: Option<&str>) -> bool {
        value.is_some_and(|v| !v.trim().is_empty())
    }
    present(rsvp_email) && present(schedule_outbox_url)
}

/// Build the CalDAV capability set. `event_rsvp` is discovery-derived
/// (see `scheduling_available`); every other PIM method is intrinsic to
/// RFC 4791 and always available.
pub(crate) fn caldav_capabilities(event_rsvp: bool) -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::Hybrid,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::None,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::ExpectedVersion {
                contact_update: false,
                event_update: true,
            },
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: 0,
            max_wait: Default::default(),
            flush_on_input_close: false,
        },
        rate_limit_class: RateLimitClass::Generous,
        quota_signal: QuotaSignal::Implicit,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport {
            calendars_list: true,
            events_in_range: true,
            event_get: true,
            event_create: true,
            event_update: true,
            event_delete: true,
            event_rsvp,
            event_search: true,
            event_autocomplete: true,
            ..PimMethodSupport::default()
        },
        filter_rule_shape: FilterRuleShape::None,
        conveniences: ConvenienceShape::default(),
        // A share arriving in the calendar home set is presented as an
        // ordinary own collection; this crate models no foreign
        // namespace, so there is nothing a rediscovery reopen could
        // surface under that label.
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

#[cfg(test)]
mod tests {
    use bifrost_types::{BlobRangeSupport, CursorFreshness, PushCapability};

    use super::*;

    #[test]
    fn capability_builder_matches_calendar_shape() {
        let caps = caldav_capabilities(true);
        assert_eq!(caps.cursor_freshness, CursorFreshness::Hybrid);
        assert_eq!(caps.batching_policy.max_items, 0);
        assert!(!caps.batching_policy.flush_on_input_close);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert_eq!(caps.push, PushCapability::None);
        assert!(caps.pim_methods.calendars_list);
        assert!(caps.pim_methods.events_in_range);
        assert!(caps.pim_methods.event_get);
        assert!(caps.pim_methods.event_create);
        assert!(caps.pim_methods.event_update);
        assert!(caps.pim_methods.event_delete);
        assert!(caps.pim_methods.event_rsvp);
        assert!(caps.pim_methods.event_search);
    }

    #[test]
    fn rsvp_capability_follows_discovery() {
        assert!(!caldav_capabilities(false).pim_methods.event_rsvp);
        // The rest of the calendar surface is RFC 4791 intrinsic and
        // must not move with the scheduling flag.
        let plain = caldav_capabilities(false);
        assert!(plain.pim_methods.calendars_list);
        assert!(plain.pim_methods.event_create);
        assert!(plain.pim_methods.event_update);
        assert!(plain.pim_methods.event_delete);
        assert!(plain.pim_methods.event_search);
    }

    #[test]
    fn scheduling_needs_both_an_outbox_and_a_user_address() {
        assert!(scheduling_available(
            Some("mailto:me@example.test"),
            Some("https://dav.example.test/outbox/")
        ));
        assert!(
            !scheduling_available(None, Some("https://dav.example.test/outbox/")),
            "no calendar-user-address means no Originator and no ATTENDEE"
        );
        assert!(
            !scheduling_available(Some("mailto:me@example.test"), None),
            "no schedule-outbox-URL means nowhere to POST the iTIP REPLY"
        );
        assert!(!scheduling_available(None, None));
    }

    #[test]
    fn blank_discovery_values_do_not_count_as_scheduling() {
        assert!(!scheduling_available(Some("  "), Some("https://x.test/o/")));
        assert!(!scheduling_available(Some("mailto:me@x.test"), Some("")));
    }
}
