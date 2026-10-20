//! IMAP NOTIFY extension types (RFC 5465).
//!
//! RFC 5465 allows clients to request notifications for mailbox and message
//! events without polling or blocking on IDLE. Event types are defined by
//! RFC 5423 (Internet Message Store Events).
//!
//! All types are fully owned  -  no lifetime parameters.

/// A mailbox filter that determines which mailboxes an event group applies to
/// (RFC 5465 Section 6).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// filter-mailboxes = filter-mailboxes-selected / filter-mailboxes-other
/// filter-mailboxes-selected = "selected" / "selected-delayed"
/// filter-mailboxes-other = "inboxes" / "personal" / "subscribed" /
///                          ("subtree" SP one-or-more-mailbox) /
///                          ("mailboxes" SP one-or-more-mailbox)
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MailboxFilter {
    /// `selected`  -  the currently selected mailbox; notifications delivered
    /// immediately, including expunges (RFC 5465 Section 6.1).
    ///
    /// Only message events are valid with this filter. When `MessageExpunge`
    /// is active, MSN values can change at any time so the client MUST use
    /// UID-based commands.
    Selected,

    /// `selected-delayed`  -  the currently selected mailbox with delayed
    /// `MessageExpunge` delivery (RFC 5465 Section 6.2).
    ///
    /// Only message events are valid with this filter. Expunge notifications
    /// are delayed until the client issues a command that permits returning
    /// expunge information (e.g. NOOP, IDLE).
    SelectedDelayed,

    /// `inboxes`  -  all selectable mailboxes where the MDA may deliver messages
    /// (RFC 5465 Section 6.3).
    ///
    /// If the server cannot compute this set, it MAY treat `inboxes` as
    /// equivalent to `personal`.
    Inboxes,

    /// `personal`  -  all selectable mailboxes in the user's personal
    /// namespace(s) per RFC 2342 (RFC 5465 Section 6.4).
    Personal,

    /// `subscribed`  -  all mailboxes the user has subscribed to
    /// (RFC 5465 Section 6.5).
    ///
    /// The server re-evaluates this set dynamically when subscriptions change.
    Subscribed,

    /// `subtree <mailbox> ...`  -  the specified mailbox plus all selectable
    /// subordinate mailboxes (RFC 5465 Section 6.6).
    ///
    /// Must contain at least one mailbox name per ABNF
    /// `one-or-more-mailbox` (RFC 5465 Section 8).
    Subtree(Vec<super::MailboxArgument>),

    /// `mailboxes <mailbox> ...`  -  one or more explicitly named mailboxes,
    /// no wildcard expansion (RFC 5465 Section 6.7).
    ///
    /// Must contain at least one mailbox name per ABNF
    /// `one-or-more-mailbox` (RFC 5465 Section 8).
    Mailboxes(Vec<super::MailboxArgument>),
}

/// An event type that can be requested in a NOTIFY command
/// (RFC 5465 Section 5, RFC 5423 Section 4).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// event         = message-event / mailbox-event / user-event / event-ext
/// message-event = ("MessageNew" [SP "(" fetch-att *(SP fetch-att) ")"])
///               / "MessageExpunge" / "FlagChange" / "AnnotationChange"
/// mailbox-event = "MailboxName" / "SubscriptionChange" / "MailboxMetadataChange"
/// user-event    = "ServerMetadataChange"
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NotifyEvent {
    // --- Message events (RFC 5465 Sections 5.1-5.3, RFC 5423 Section 4.1-4.2) ---
    /// `MessageNew`  -  new message delivered or appended
    /// (RFC 5465 Section 5.2, RFC 5423 Section 4.1).
    ///
    /// For the selected mailbox: server sends EXISTS then FETCH with the
    /// requested attributes. For other mailboxes: server sends STATUS with
    /// UIDNEXT and MESSAGES.
    ///
    /// The optional fetch attributes are only valid with `selected` or
    /// `selected-delayed` mailbox filters (RFC 5465 Section 5.2).
    MessageNew {
        /// Optional FETCH attributes to include with new-message notifications
        /// (RFC 5465 Section 5.2). Only valid for `selected`/`selected-delayed`.
        /// Each string is a raw FETCH attribute (e.g. `"UID"`, `"FLAGS"`,
        /// `"BODY.PEEK[HEADER.FIELDS (From To Subject)]"`).
        fetch_attrs: Vec<String>,
    },

    /// `MessageExpunge`  -  message expunged or expired
    /// (RFC 5465 Section 5.3, RFC 5423 Section 4.1).
    ///
    /// MUST always be specified together with `MessageNew` (RFC 5465 Section 5.2).
    MessageExpunge,

    /// `FlagChange`  -  message flags changed
    /// (RFC 5465 Section 5.1, RFC 5423 Section 4.2).
    ///
    /// Requires both `MessageNew` and `MessageExpunge` to also be specified
    /// (RFC 5465 Section 5.1).
    FlagChange,

    /// `AnnotationChange`  -  per-message annotation changed
    /// (RFC 5465 Section 5.1).
    ///
    /// Requires both `MessageNew` and `MessageExpunge` to also be specified
    /// (RFC 5465 Section 5.1).
    AnnotationChange,

    // --- Mailbox events (RFC 5465 Sections 5.4-5.6, RFC 5423 Section 4.4) ---
    /// `MailboxName`  -  mailbox created, deleted, or renamed
    /// (RFC 5465 Section 5.4, RFC 5423 Section 4.4).
    ///
    /// Server sends unsolicited LIST responses. On rename, the LIST includes
    /// an `OLDNAME` extended data item.
    MailboxName,

    /// `SubscriptionChange`  -  mailbox subscribed or unsubscribed
    /// (RFC 5465 Section 5.5, RFC 5423 Section 4.4).
    ///
    /// Server sends unsolicited LIST response with accurate `\Subscribed`
    /// attribute presence.
    SubscriptionChange,

    /// `MailboxMetadataChange`  -  per-mailbox metadata annotation changed
    /// (RFC 5465 Section 5.6).
    ///
    /// Server sends unsolicited METADATA response per RFC 5464 Section 4.4.2.
    /// Optional unless the server implements METADATA (RFC 5464).
    MailboxMetadataChange,

    // --- User events (RFC 5465 Section 5.7) ---
    /// `ServerMetadataChange`  -  server-level metadata annotation changed
    /// (RFC 5465 Section 5.7).
    ///
    /// Server sends unsolicited METADATA response with empty mailbox name.
    /// Optional unless the server implements METADATA or METADATA-SERVER.
    ServerMetadataChange,

    // --- Extension events (RFC 5465 Section 8) ---
    /// Unrecognized event type  -  preserved verbatim for forward compatibility
    /// (RFC 5465 Section 8: `event-ext = atom`).
    Other(String),
}

/// A group of events to monitor on a set of mailboxes
/// (RFC 5465 Section 8).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// event-group = "(" filter-mailboxes SP events ")"
/// events      = ("(" event *(SP event) ")") / "NONE"
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NotifyEventGroup {
    /// Which mailboxes this event group applies to (RFC 5465 Section 6).
    pub filter: MailboxFilter,

    /// Events to monitor. An empty Vec means `NONE`  -  no events for this
    /// filter (RFC 5465 Section 8).
    pub events: Vec<NotifyEvent>,
}

impl NotifyEventGroup {
    /// Create a new event group for the given mailbox filter and events
    /// (RFC 5465 Section 8).
    pub fn new(filter: MailboxFilter, events: Vec<NotifyEvent>) -> Self {
        Self { filter, events }
    }
}

/// Parameters for the `NOTIFY SET` command (RFC 5465 Section 3).
///
/// RFC 5465 Section 8 ABNF:
/// ```text
/// notify     = "NOTIFY" SP (notify-set / notify-none)
/// notify-set = "SET" [status-indicator] SP event-groups
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NotifySetParams {
    /// When `true`, the server sends an initial STATUS response for each
    /// non-selected mailbox with message events before the tagged OK
    /// (RFC 5465 Section 4, `status-indicator = SP "STATUS"`).
    pub status: bool,

    /// One or more event groups defining which events to monitor on which
    /// mailboxes (RFC 5465 Section 3).
    pub event_groups: Vec<NotifyEventGroup>,
}

impl NotifySetParams {
    /// Create NOTIFY SET parameters with the given event groups
    /// (RFC 5465 Section 3).
    ///
    /// `status` controls whether the server sends initial STATUS responses
    /// for non-selected mailboxes before the tagged OK (RFC 5465 Section 4).
    pub fn new(event_groups: Vec<NotifyEventGroup>, status: bool) -> Self {
        Self {
            status,
            event_groups,
        }
    }
}
