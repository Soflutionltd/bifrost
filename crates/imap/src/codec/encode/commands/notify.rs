//! NOTIFY command encoder (RFC 5465).

use super::{
    CommandWriter, encode_mailbox_argument, validate_atom, validate_no_crlf,
    validate_single_fetch_att,
};
use crate::types::notify::{MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams};

/// Encode a `NOTIFY SET` command (RFC 5465 Section 3).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// notify     = "NOTIFY" SP (notify-set / notify-none)
/// notify-set = "SET" [status-indicator] SP event-groups
/// event-groups = event-group *(SP event-group)
/// event-group  = "(" filter-mailboxes SP events ")"
/// ```
pub(in crate::codec::encode) fn encode_notify_set(
    w: &mut CommandWriter,
    tag: &str,
    params: &NotifySetParams,
    utf8: bool,
) -> Result<(), crate::Error> {
    // RFC 5465 Section 8: event-groups = event-group *(SP event-group)
    // requires at least one event-group.
    if params.event_groups.is_empty() {
        return Err(crate::Error::InvalidInput(
            "NOTIFY SET requires at least one event group (RFC 5465 Section 8)".into(),
        ));
    }

    // RFC 5465 Section 3: "The command MUST NOT contain more than one
    // event group with a selected or selected-delayed filter."
    let selected_count = params
        .event_groups
        .iter()
        .filter(|g| {
            matches!(
                g.filter,
                MailboxFilter::Selected | MailboxFilter::SelectedDelayed
            )
        })
        .count();
    if selected_count > 1 {
        return Err(crate::Error::InvalidInput(
            "NOTIFY SET must not contain more than one event group with a \
             selected or selected-delayed filter (RFC 5465 Section 3)"
                .into(),
        ));
    }

    for group in &params.event_groups {
        validate_notify_event_group(group)?;
    }

    w.raw(tag.as_bytes());
    w.raw(b" NOTIFY SET");

    // RFC 5465 Section 4: optional STATUS indicator.
    if params.status {
        w.raw(b" STATUS");
    }

    for group in &params.event_groups {
        w.raw(b" (");
        encode_mailbox_filter(w, &group.filter, utf8)?;
        w.raw(b" ");
        encode_events(w, &group.events)?;
        w.raw(b")");
    }

    w.raw(b"\r\n");
    Ok(())
}

/// Validate a single NOTIFY event group's semantic constraints
/// (RFC 5465 Sections 5.1-5.4, 6.1).
fn validate_notify_event_group(group: &NotifyEventGroup) -> Result<(), crate::Error> {
    let is_selected_filter = matches!(
        group.filter,
        MailboxFilter::Selected | MailboxFilter::SelectedDelayed
    );

    // RFC 5465 Section 6.1: selected/selected-delayed filters only accept
    // message events (MessageNew, MessageExpunge, FlagChange, AnnotationChange).
    if is_selected_filter {
        for event in &group.events {
            if !is_message_event(event) {
                return Err(crate::Error::InvalidInput(format!(
                    "selected/selected-delayed filters only accept message events \
                     (MessageNew, MessageExpunge, FlagChange, AnnotationChange), \
                     got {event:?} (RFC 5465 Section 6.1)"
                )));
            }
        }
    }

    // RFC 5465 Section 8 ABNF: fetch attributes in MessageNew are only
    // valid with selected/selected-delayed filters per the message-event
    // production comment.
    if !is_selected_filter {
        for event in &group.events {
            if let NotifyEvent::MessageNew { fetch_attrs } = event
                && !fetch_attrs.is_empty()
            {
                return Err(crate::Error::InvalidInput(
                    "MessageNew fetch attributes are only valid with \
                     selected/selected-delayed filters (RFC 5465 Section 8)"
                        .into(),
                ));
            }
        }
    }

    // RFC 5465 Section 5: event dependency constraints:
    // - MessageExpunge requires MessageNew (and vice versa).
    // - FlagChange requires both MessageNew and MessageExpunge.
    // - AnnotationChange requires both MessageNew and MessageExpunge.
    let has_new = group
        .events
        .iter()
        .any(|e| matches!(e, NotifyEvent::MessageNew { .. }));
    let has_expunge = group
        .events
        .iter()
        .any(|e| matches!(e, NotifyEvent::MessageExpunge));
    let has_flag_change = group
        .events
        .iter()
        .any(|e| matches!(e, NotifyEvent::FlagChange));
    let has_annotation_change = group
        .events
        .iter()
        .any(|e| matches!(e, NotifyEvent::AnnotationChange));

    // RFC 5465 Section 5: "If one of MessageNew or MessageExpunge is
    // specified, then both events MUST be specified."
    if has_new && !has_expunge {
        return Err(crate::Error::InvalidInput(
            "MessageNew requires MessageExpunge to also be specified \
             (RFC 5465 Section 5)"
                .into(),
        ));
    }
    if has_expunge && !has_new {
        return Err(crate::Error::InvalidInput(
            "MessageExpunge requires MessageNew to also be specified \
             (RFC 5465 Section 5)"
                .into(),
        ));
    }
    // RFC 5465 Section 5: "If the FlagChange and/or AnnotationChange events
    // are specified, MessageNew and MessageExpunge MUST also be specified
    // by the client."
    if has_flag_change && (!has_new || !has_expunge) {
        return Err(crate::Error::InvalidInput(
            "FlagChange requires both MessageNew and MessageExpunge to also \
             be specified (RFC 5465 Section 5)"
                .into(),
        ));
    }
    if has_annotation_change && (!has_new || !has_expunge) {
        return Err(crate::Error::InvalidInput(
            "AnnotationChange requires both MessageNew and MessageExpunge to \
             also be specified (RFC 5465 Section 5)"
                .into(),
        ));
    }
    Ok(())
}

/// Returns `true` if the event is a message event (RFC 5465 Sections 5.1-5.3).
///
/// Message events are `FlagChange`/`AnnotationChange` (RFC 5465 Section 5.1),
/// `MessageNew` (RFC 5465 Section 5.2), and `MessageExpunge` (RFC 5465
/// Section 5.3). Extension events (`Other(...)`) are NOT message events;
/// RFC 5465 Section 8 defines `event-ext` as a separate ABNF production
/// from `message-event`, so they must be rejected for selected /
/// selected-delayed filters (Section 6.1). Mailbox events (`MailboxName`,
/// `SubscriptionChange`) and metadata events are also excluded.
fn is_message_event(event: &NotifyEvent) -> bool {
    // RFC 5465 Section 8 ABNF: `message-event` is MessageNew /
    // MessageExpunge / FlagChange / AnnotationChange. `event-ext`
    // (modelled as `Other(...)`) is a separate production and MUST NOT
    // be accepted under selected / selected-delayed filters (Section 6.1).
    matches!(
        event,
        NotifyEvent::MessageNew { .. }
            | NotifyEvent::MessageExpunge
            | NotifyEvent::FlagChange
            | NotifyEvent::AnnotationChange
    )
}

/// Encode a mailbox filter for a NOTIFY event group (RFC 5465 Section 6).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// filter-mailboxes-selected = "selected" / "selected-delayed"
/// filter-mailboxes-other    = "inboxes" / "personal" / "subscribed" /
///                             ("subtree" SP one-or-more-mailbox) /
///                             ("mailboxes" SP one-or-more-mailbox)
/// one-or-more-mailbox       = mailbox / many-mailboxes
/// many-mailboxes            = "(" mailbox *(SP mailbox) ")"
/// ```
fn encode_mailbox_filter(
    w: &mut CommandWriter,
    filter: &MailboxFilter,
    utf8: bool,
) -> Result<(), crate::Error> {
    match filter {
        MailboxFilter::Selected => w.raw(b"selected"),
        MailboxFilter::SelectedDelayed => w.raw(b"selected-delayed"),
        MailboxFilter::Inboxes => w.raw(b"inboxes"),
        MailboxFilter::Personal => w.raw(b"personal"),
        MailboxFilter::Subscribed => w.raw(b"subscribed"),
        MailboxFilter::Subtree(mailboxes) => {
            // RFC 5465 Section 8: one-or-more-mailbox requires at least one.
            if mailboxes.is_empty() {
                return Err(crate::Error::InvalidInput(
                    "subtree filter requires at least one mailbox (RFC 5465 Section 8)".into(),
                ));
            }
            w.raw(b"subtree");
            encode_one_or_more_mailbox(w, mailboxes, utf8)?;
        }
        MailboxFilter::Mailboxes(mailboxes) => {
            // RFC 5465 Section 8: one-or-more-mailbox requires at least one.
            if mailboxes.is_empty() {
                return Err(crate::Error::InvalidInput(
                    "mailboxes filter requires at least one mailbox (RFC 5465 Section 8)".into(),
                ));
            }
            w.raw(b"mailboxes");
            encode_one_or_more_mailbox(w, mailboxes, utf8)?;
        }
    }
    Ok(())
}

/// Encode `one-or-more-mailbox` (RFC 5465 Section 8).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// one-or-more-mailbox = mailbox / many-mailboxes
/// many-mailboxes      = "(" mailbox *(SP mailbox) ")"
/// ```
///
/// A single mailbox is encoded bare; two or more are parenthesized.
fn encode_one_or_more_mailbox(
    w: &mut CommandWriter,
    mailboxes: &[crate::types::MailboxArgument],
    utf8: bool,
) -> Result<(), crate::Error> {
    if mailboxes.len() == 1 {
        // RFC 5465 Section 8: one-or-more-mailbox = mailbox
        // RFC 3501 Section 5.1.3: encode with INBOX normalization and MUTF-7.
        let wire = encode_mailbox_argument(&mailboxes[0], utf8)?;
        w.raw(b" ");
        w.string(&wire, utf8);
    } else {
        // RFC 5465 Section 8: many-mailboxes = "(" mailbox *(SP mailbox) ")"
        w.raw(b" (");
        for (i, mbox) in mailboxes.iter().enumerate() {
            if i > 0 {
                w.raw(b" ");
            }
            // RFC 3501 Section 5.1.3: encode with INBOX normalization and MUTF-7.
            let wire = encode_mailbox_argument(mbox, utf8)?;
            w.string(&wire, utf8);
        }
        w.raw(b")");
    }
    Ok(())
}

/// Encode the events portion of an event group (RFC 5465 Section 8).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// events = ("(" event *(SP event) ")") / "NONE"
/// ```
fn encode_events(w: &mut CommandWriter, events: &[NotifyEvent]) -> Result<(), crate::Error> {
    if events.is_empty() {
        // RFC 5465 Section 8: events = "NONE"
        w.raw(b"NONE");
        return Ok(());
    }
    w.raw(b"(");
    for (i, event) in events.iter().enumerate() {
        if i > 0 {
            w.raw(b" ");
        }
        encode_single_event(w, event)?;
    }
    w.raw(b")");
    Ok(())
}

/// Encode a single NOTIFY event (RFC 5465 Section 5, Section 8).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// message-event = ("MessageNew" [SP "(" fetch-att *(SP fetch-att) ")"])
///               / "MessageExpunge" / "FlagChange" / "AnnotationChange"
/// ```
fn encode_single_event(w: &mut CommandWriter, event: &NotifyEvent) -> Result<(), crate::Error> {
    match event {
        NotifyEvent::MessageNew { fetch_attrs } => {
            w.raw(b"MessageNew");
            if !fetch_attrs.is_empty() {
                // RFC 5465 Section 5.2: optional fetch attributes for
                // selected/selected-delayed (per Section 8 ABNF).
                w.raw(b" (");
                for (i, attr) in fetch_attrs.iter().enumerate() {
                    if i > 0 {
                        w.raw(b" ");
                    }
                    // Reject CRLF in fetch attributes to prevent command injection
                    // (RFC 3501 Section 2.2).
                    validate_no_crlf(attr, "NOTIFY fetch-att")?;
                    // Reject empty attrs and unbalanced delimiters because they
                    // produce malformed wire output.
                    validate_single_fetch_att(attr)?;
                    w.raw(attr.as_bytes());
                }
                w.raw(b")");
            }
        }
        NotifyEvent::MessageExpunge => w.raw(b"MessageExpunge"),
        NotifyEvent::FlagChange => w.raw(b"FlagChange"),
        NotifyEvent::AnnotationChange => w.raw(b"AnnotationChange"),
        NotifyEvent::MailboxName => w.raw(b"MailboxName"),
        NotifyEvent::SubscriptionChange => w.raw(b"SubscriptionChange"),
        NotifyEvent::MailboxMetadataChange => w.raw(b"MailboxMetadataChange"),
        NotifyEvent::ServerMetadataChange => w.raw(b"ServerMetadataChange"),
        NotifyEvent::Other(name) => {
            // RFC 5465 Section 8: event-ext = atom.
            validate_atom(name, "NOTIFY event-ext")?;
            w.raw(name.as_bytes());
        }
    }
    Ok(())
}
