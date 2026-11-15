//! NOTIFY command encoder (RFC 5465).

use super::{
    CommandWriter, encode_mailbox_argument, validate_atom, validate_no_crlf,
    validate_single_fetch_att,
};
use crate::types::notify::{MailboxFilter, NotifyEvent, NotifySetParams};

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
    utf8: crate::codec::utf7::WireMode,
) -> Result<(), crate::Error> {
    // RFC 5465 Section 8: event-groups = event-group *(SP event-group)
    // requires at least one event-group.
    if params.event_groups.is_empty() {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                "NOTIFY SET requires at least one event group (RFC 5465 Section 8)".into(),
            ),
        ));
    }

    // RFC 5465 Section 3: "The command MUST NOT contain more than one
    // event group with a selected or selected-delayed filter."
    let selected_count = params
        .event_groups
        .iter()
        .filter(|g| {
            matches!(
                g.filter(),
                MailboxFilter::Selected | MailboxFilter::SelectedDelayed
            )
        })
        .count();
    if selected_count > 1 {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput(
                "NOTIFY SET must not contain more than one event group with a \
             selected or selected-delayed filter (RFC 5465 Section 3)"
                    .into(),
            ),
        ));
    }

    for group in &params.event_groups {
        group.validate()?;
    }

    w.raw(tag.as_bytes());
    w.raw(b" NOTIFY SET");

    // RFC 5465 Section 4: optional STATUS indicator.
    if params.status {
        w.raw(b" STATUS");
    }

    for group in &params.event_groups {
        w.raw(b" (");
        encode_mailbox_filter(w, group.filter(), utf8)?;
        w.raw(b" ");
        encode_events(w, group.events())?;
        w.raw(b")");
    }

    w.raw(b"\r\n");
    Ok(())
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
    utf8: crate::codec::utf7::WireMode,
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
                return Err(crate::Error::LocalRefusal(
                    crate::error::LocalRefusal::InvalidInput(
                        "subtree filter requires at least one mailbox (RFC 5465 Section 8)".into(),
                    ),
                ));
            }
            w.raw(b"subtree");
            encode_one_or_more_mailbox(w, mailboxes, utf8)?;
        }
        MailboxFilter::Mailboxes(mailboxes) => {
            // RFC 5465 Section 8: one-or-more-mailbox requires at least one.
            if mailboxes.is_empty() {
                return Err(crate::Error::LocalRefusal(
                    crate::error::LocalRefusal::InvalidInput(
                        "mailboxes filter requires at least one mailbox (RFC 5465 Section 8)"
                            .into(),
                    ),
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
    utf8: crate::codec::utf7::WireMode,
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
                    let wire = attr.wire();
                    let attr = wire.as_ref();
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
