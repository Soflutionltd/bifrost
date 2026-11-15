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
        /// Typed attributes remain typed; raw extension attributes retain their
        /// own syntax validation at encoding.
        fetch_attrs: Vec<NotifyFetchAttribute>,
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
    filter: MailboxFilter,

    /// Events to monitor. An empty Vec means `NONE`  -  no events for this
    /// filter (RFC 5465 Section 8).
    events: Vec<NotifyEvent>,
    validation_error: Option<NotifyGroupFault>,
}

impl NotifyEventGroup {
    /// Create a new event group for the given mailbox filter and events
    /// (RFC 5465 Section 8).
    pub fn new(filter: MailboxFilter, events: Vec<NotifyEvent>) -> Self {
        let mut group = Self {
            filter,
            events,
            validation_error: None,
        };
        group.validation_error = validate_notify_event_group(&group).err();
        group
    }

    pub(crate) fn filter(&self) -> &MailboxFilter {
        &self.filter
    }

    pub(crate) fn events(&self) -> &[NotifyEvent] {
        &self.events
    }

    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        match &self.validation_error {
            Some(error) => Err(crate::Error::InvalidInput(error.message().into())),
            None => Ok(()),
        }
    }

    /// MessageNew and MessageExpunge are always registered together. Optional
    /// flag and annotation observations cannot omit that dependency pair.
    // The dependency bundle alone cannot guarantee filter legality: a mailbox
    // filter outside Selected/SelectedDelayed cannot request FETCH attributes,
    // and extension events cannot enter a selected group. Keep that admission
    // at the immutable group constructor rather than duplicating it in codec.
    pub(crate) fn messages(filter: MailboxFilter, messages: MessageEvents) -> Self {
        let mut events = vec![
            NotifyEvent::MessageNew {
                fetch_attrs: messages.new_fetch.into_iter().map(Into::into).collect(),
            },
            NotifyEvent::MessageExpunge,
        ];
        if messages.flag_change {
            events.push(NotifyEvent::FlagChange);
        }
        if messages.annotation_change {
            events.push(NotifyEvent::AnnotationChange);
        }
        Self::new(filter, events)
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

/// Per-type NOTIFY flags (RFC 5465 Sections 5.1-5.8).
///
/// Tracks which response types the current NOTIFY registration can produce
/// so that IDLE event classification and LIST filtering during LIST-family
/// commands know which types to expect.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct NotifyFlags {
    /// `MailboxName` or `SubscriptionChange` events registered
    /// (RFC 5465 Sections 5.4-5.5  -  delivered as LIST).
    pub(crate) list: bool,
    /// Message events on non-selected mailboxes
    /// (RFC 5465 Sections 5.1-5.3 - delivered as STATUS).
    pub(crate) status: bool,
    /// `MailboxMetadataChange` or `ServerMetadataChange` events registered
    /// (RFC 5465 Sections 5.6-5.7  -  delivered as METADATA).
    pub(crate) metadata: bool,
}

/// Compute which ongoing response types a NOTIFY registration can produce.
///
/// Derives named flags for *ongoing* notifications:
/// - `list`: any non-selected events are registered. `MailboxName`
///   (RFC 5465 Section 5.4) and `SubscriptionChange` (RFC 5465 Section 5.5)
///   produce explicit LIST responses, but ALL non-selected registrations
///   can trigger LIST responses for ACL changes  -  RFC 5465 Section 5.9
///   requires `LIST \NoAccess` / `LIST` when the logged-in user loses or
///   regains the `l` (lookup) ACL right on any monitored mailbox,
///   regardless of which event types were requested.
/// - `status`: message events on non-selected mailboxes  -  `FlagChange`
///   and `AnnotationChange` (RFC 5465 Section 5.1), `MessageNew`
///   (RFC 5465 Section 5.2), and `MessageExpunge` (RFC 5465 Section 5.3),
///   all delivered as STATUS responses.
/// - `metadata`: `MailboxMetadataChange` (RFC 5465 Section 5.6) or
///   `ServerMetadataChange` (RFC 5465 Section 5.7) events registered  -
///   delivered as METADATA.
///
/// The `params.status` indicator is NOT included here  -  it only triggers
/// an initial STATUS snapshot for non-selected mailboxes with message
/// events (RFC 5465 Section 4), not ongoing STATUS notifications. Those
/// initial responses are handled by the re-push logic in `notify_set()`.
impl From<&NotifySetParams> for NotifyFlags {
    fn from(params: &NotifySetParams) -> Self {
        let mut list = false;
        let mut status = false;
        let mut metadata = false;

        for group in &params.event_groups {
            let is_non_selected = !matches!(
                group.filter,
                MailboxFilter::Selected | MailboxFilter::SelectedDelayed
            );

            // RFC 5465 Section 5.9: any non-selected registration can trigger
            // LIST \NoAccess or LIST responses for ACL changes on monitored
            // mailboxes. Enable LIST buffering for all non-selected groups.
            if is_non_selected && !group.events.is_empty() {
                list = true;
            }

            for event in &group.events {
                match event {
                    NotifyEvent::MessageNew { .. }
                    | NotifyEvent::MessageExpunge
                    | NotifyEvent::FlagChange
                    | NotifyEvent::AnnotationChange => {
                        // RFC 5465 Sections 5.1-5.3: on non-selected mailboxes
                        // all message events are delivered as STATUS responses.
                        if is_non_selected {
                            status = true;
                        }
                    }
                    NotifyEvent::MailboxMetadataChange | NotifyEvent::ServerMetadataChange => {
                        // RFC 5465 Sections 5.6-5.7: delivered as METADATA.
                        metadata = true;
                    }
                    NotifyEvent::Other(_) => {
                        // Unknown extension event  -  we don't know which
                        // response type the server will use for delivery.
                        // Enable ALL known buffering flags defensively so
                        // the notification is not silently dropped regardless
                        // of delivery mechanism.  Note: Other(_) can only
                        // appear under non-selected filters  -  the encoder
                        // rejects it for selected / selected-delayed per
                        // RFC 5465 Section 6.1 / Section 8 (event-ext is a
                        // separate ABNF production from message-event).
                        // Truly extension-defined responses
                        // (`UntaggedResponse::Unknown`) are also routed
                        // to the unsolicited buffer by the dispatcher.
                        list = true;
                        status = true;
                        metadata = true;
                    }
                    _ => {}
                }
            }
        }

        Self {
            list,
            status,
            metadata,
        }
    }
}

/// One legal message-event dependency group. Filter-specific legality is checked
/// once by NotifyEventGroup construction, before the group enters a command.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MessageEvents {
    pub(crate) new_fetch: Vec<super::FetchAttr>,
    pub(crate) flag_change: bool,
    pub(crate) annotation_change: bool,
}

/// Validate a single NOTIFY event group's semantic constraints
/// (RFC 5465 Sections 5.1-5.4, 6.1).
fn validate_notify_event_group(group: &NotifyEventGroup) -> Result<(), NotifyGroupFault> {
    let selected = matches!(
        group.filter,
        MailboxFilter::Selected | MailboxFilter::SelectedDelayed
    );
    let mut new = false;
    let mut expunge = false;
    let mut flag = false;
    let mut annotation = false;
    for event in &group.events {
        if selected && !is_message_event(event) {
            return Err(NotifyGroupFault::SelectedEvent);
        }
        match event {
            NotifyEvent::MessageNew { fetch_attrs } => {
                if !selected && !fetch_attrs.is_empty() {
                    return Err(NotifyGroupFault::NonSelectedFetch);
                }
                new = true;
            }
            NotifyEvent::MessageExpunge => expunge = true,
            NotifyEvent::FlagChange => flag = true,
            NotifyEvent::AnnotationChange => annotation = true,
            _ => {}
        }
    }
    if new && !expunge {
        return Err(NotifyGroupFault::NewWithoutExpunge);
    }
    if expunge && !new {
        return Err(NotifyGroupFault::ExpungeWithoutNew);
    }
    if flag && (!new || !expunge) {
        return Err(NotifyGroupFault::FlagWithoutMessages);
    }
    if annotation && (!new || !expunge) {
        return Err(NotifyGroupFault::AnnotationWithoutMessages);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum NotifyGroupFault {
    SelectedEvent,
    NonSelectedFetch,
    NewWithoutExpunge,
    ExpungeWithoutNew,
    FlagWithoutMessages,
    AnnotationWithoutMessages,
}
impl NotifyGroupFault {
    fn message(&self) -> &'static str {
        match self {
            Self::SelectedEvent => {
                "selected/selected-delayed filters only accept message events (RFC 5465 Section 6.1)"
            }
            Self::NonSelectedFetch => {
                "MessageNew fetch attributes are only valid with selected/selected-delayed filters (RFC 5465 Section 8)"
            }
            Self::NewWithoutExpunge => {
                "MessageNew requires MessageExpunge to also be specified (RFC 5465 Section 5)"
            }
            Self::ExpungeWithoutNew => {
                "MessageExpunge requires MessageNew to also be specified (RFC 5465 Section 5)"
            }
            Self::FlagWithoutMessages => {
                "FlagChange requires both MessageNew and MessageExpunge to also be specified (RFC 5465 Section 5)"
            }
            Self::AnnotationWithoutMessages => {
                "AnnotationChange requires both MessageNew and MessageExpunge to also be specified (RFC 5465 Section 5)"
            }
        }
    }
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

/// A NOTIFY fetch operand keeps typed attributes intact. Raw extension operands
/// remain possible because servers can advertise attributes beyond FetchAttr.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NotifyFetchAttribute {
    Attribute(super::FetchAttr),
    Raw(String),
}
impl NotifyFetchAttribute {
    pub(crate) fn wire(&self) -> std::borrow::Cow<'_, str> {
        match self {
            Self::Attribute(attr) => attr.to_imap_string().into(),
            Self::Raw(text) => text.as_str().into(),
        }
    }
}
impl From<super::FetchAttr> for NotifyFetchAttribute {
    fn from(attr: super::FetchAttr) -> Self {
        Self::Attribute(attr)
    }
}
impl From<String> for NotifyFetchAttribute {
    fn from(text: String) -> Self {
        Self::Raw(text)
    }
}
impl From<&str> for NotifyFetchAttribute {
    fn from(text: &str) -> Self {
        Self::Raw(text.to_owned())
    }
}
